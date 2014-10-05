// A gen server behavior for dataport consumer.
//
// Daemon listens for new connections and spawns a reader routine
// for each new connection.
//
// concurrency model:
//
//                                   Application back channels,
//                               mutation channel and sideband channel
//                          -----------------------------------------------
//                                                      ^
//     NewServer() ----------*                          |
//             |             |                          | []*VbKeyVersions
//          (spawn)          |                          |
//             |             |    *---------------------*
//             |          (spawn) |                     |
//           listener()      |    | ConnectionError     |
//                 |         |    |                     |
//   serverCmdNewConnection  |    |                     |
//                 |         |    |                     |
//                 V         |    |                     |
//  Close() -------*------->gen-server()-----*---- doReceive()----*
//          serverCmdClose       ^           |                    |
//                               |           *---- doReceive()----*
//                serverCmdVbmap |           |                    |
//            serverCmdVbcontrol |           *---- doReceive()----*
//                serverCmdError |                                |
//                               *--------------------------------*
//                                          (control & faults)
//
// server behavior:
//
// 1. can handle more than one connection from same router.
// 2. whenever a connection with router
//    a. gets closed
//    b. or timeout
//    all connections with that router will be closed and same will
//    be intimated to application for catchup connection, using
//    ConnectionError message.

package dataport

import "errors"
import "fmt"
import "io"
import "net"
import "time"
import "runtime/debug"

import c "github.com/couchbase/indexing/secondary/common"
import "github.com/couchbase/indexing/secondary/protobuf"
import "github.com/couchbase/indexing/secondary/transport"

// Error codes

// ErrorPayload
var ErrorPayload = errors.New("dataport.daemonPayload")

// ErrorDuplicateStreamBegin
var ErrorDuplicateStreamBegin = errors.New("dataport.duplicateStreamBegin")

// ErrorMissingStreamBegin
var ErrorMissingStreamBegin = errors.New("dataport.missingStreamBegin")

// ErrorDaemonExit
var ErrorDaemonExit = errors.New("dataport.daemonExit")

// ErrorDuplicateClient
var ErrorDuplicateClient = errors.New("dataport.duplicateClient")

// ErrorWorkerKilled
var ErrorWorkerKilled = errors.New("dataport.workerKilled")

type activeVb struct {
	raddr  string // remote connection carrying this vbucket.
	bucket string
	vbno   uint16
}

type keeper map[string]*activeVb

func (avb *activeVb) id() string {
	return fmt.Sprintf("%v-%v-%v", avb.raddr, avb.bucket, avb.vbno)
}

func (hostUuids keeper) isActive(bucket string, vbno uint16) bool {
	for _, avb := range hostUuids {
		if avb.bucket == bucket && avb.vbno == vbno {
			return true
		}
	}
	return false
}

// messages to gen-server
type serverMessage struct {
	cmd   byte          // gen server command
	raddr string        // remote connection address, optional
	args  []interface{} // command arguments
	err   error         // in case the message is to notify error
}

// maintain information about each remote connection.
type netConn struct {
	conn   net.Conn
	worker chan interface{}
	active bool
}

// Server handles an active dataport server of mutation for all vbuckets.
type Server struct {
	laddr string // address to listen
	lis   net.Listener
	appch chan<- interface{} // backchannel to application

	// gen-server management
	conns     map[string]*netConn // resolve <host:port> to conn. obj
	reqch     chan []interface{}
	finch     chan bool
	logPrefix string
}

// NewServer creates a new dataport daemon.
func NewServer(laddr string, appch chan<- interface{}) (s *Server, err error) {
	s = &Server{
		laddr: laddr,
		appch: appch,

		// Managing vbuckets and connections for all routers
		reqch:     make(chan []interface{}, c.GenserverChannelSize),
		finch:     make(chan bool),
		conns:     make(map[string]*netConn),
		logPrefix: fmt.Sprintf("[dataport %q]", laddr),
	}
	if s.lis, err = net.Listen("tcp", laddr); err != nil {
		c.Errorf("%v failed starting ! %v", s.logPrefix, err)
		return nil, err
	}
	go listener(s.logPrefix, s.lis, s.reqch) // spawn daemon
	go s.genServer(s.reqch)                  // spawn gen-server
	c.Infof("%v started ...", s.logPrefix)
	return s, nil
}

func (s *Server) addUuids(started, hostUuids keeper) keeper {
	for x, newvb := range started {
		if hostUuids.isActive(newvb.bucket, newvb.vbno) {
			c.Errorf("%v duplicate vbucket %#v\n", s.logPrefix, newvb)
		}
		hostUuids[x] = newvb
		c.Infof("%v added vbucket %#v\n", s.logPrefix, newvb)
	}
	return hostUuids
}

func (s *Server) delUuids(finished, hostUuids keeper) keeper {
	for x := range finished {
		avb, ok := hostUuids[x]
		if !ok {
			c.Errorf("%v not active vbucket %#v\n", s.logPrefix, avb)
		}
		delete(hostUuids, x)
		c.Infof("%v deleted vbucket %#v\n", s.logPrefix, avb)
	}
	return hostUuids
}

// Close the daemon listening for new connections and shuts down all read
// routines for this dataport server. synchronous call.
func (s *Server) Close() (err error) {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{serverMessage{cmd: serverCmdClose}, respch}
	resp, err := c.FailsafeOp(s.reqch, respch, cmd, s.finch)
	return c.OpError(err, resp, 0)
}

// gen-server commands
const (
	serverCmdNewConnection byte = iota + 1
	serverCmdVbmap
	serverCmdVbcontrol
	serverCmdError
	serverCmdClose
)

// gen server routine for dataport server.
func (s *Server) genServer(reqch chan []interface{}) {
	var appmsg interface{}

	defer func() {
		if r := recover(); r != nil {
			c.Errorf("%v gen-server crashed: %v\n", s.logPrefix, r)
			c.StackTrace(string(debug.Stack()))
			s.handleClose()
		}
	}()

	hostUuids := make(keeper) // id() -> activeVb
loop:
	for {
		appmsg = nil
		select {
		case cmd := <-reqch:
			msg := cmd[0].(serverMessage)
			switch msg.cmd {
			case serverCmdNewConnection:
				conn, raddr := msg.args[0].(net.Conn), msg.raddr
				if _, ok := s.conns[raddr]; ok {
					c.Errorf("%v %q already active\n", s.logPrefix, raddr)
					conn.Close()
				} else { // connection accepted
					n := len(s.conns)
					c.Infof("%v new connection %q +%d\n", s.logPrefix, raddr, n)
					worker := make(chan interface{}, 10) // TODO: no magic num.
					s.conns[raddr] = &netConn{conn: conn, worker: worker}
					s.startWorker(raddr)
				}

			case serverCmdVbmap:
				vbmap := msg.args[0].(*protobuf.VbConnectionMap)
				b, raddr := vbmap.GetBucket(), msg.raddr
				for _, vbno := range vbmap.GetVbuckets() {
					avb := &activeVb{raddr, b, uint16(vbno)}
					hostUuids[avb.id()] = avb
				}
				s.startWorker(msg.raddr)

			case serverCmdVbcontrol:
				started := msg.args[0].(keeper)
				finished := msg.args[1].(keeper)
				if len(started) > 0 { // new vbucket stream(s) have started
					hostUuids = s.addUuids(started, hostUuids)
				}
				if len(finished) > 0 { // vbucket stream(s) have finished
					hostUuids = s.delUuids(finished, hostUuids)
				}
				s.startWorker(msg.raddr)

			case serverCmdClose:
				// This execution path never panics !!
				respch := cmd[1].(chan []interface{})
				s.handleClose()
				respch <- []interface{}{nil}
				break loop

			case serverCmdError:
				appmsg = s.jumboErrorHandler(msg.raddr, hostUuids, msg.err)
			}

			if appmsg != nil {
				s.appch <- appmsg
				c.Debugf("appmsg: %T:%+v\n", appmsg, appmsg)
			}
		}
	}
}

// handle new connection
func (s *Server) handleNewConnection(conn net.Conn, raddr string) error {
	c.Infof("%v connection request from %q\n", s.logPrefix, raddr)
	if _, ok := s.conns[raddr]; ok {
		c.Errorf("%v %q already active\n", s.logPrefix, raddr)
		return ErrorDuplicateClient
	}
	// connection accepted
	worker := make(chan interface{}, 10) // TODO: avoid magic numbers
	s.conns[raddr] = &netConn{conn: conn, worker: worker, active: false}
	c.Debugf("%v total active connections %v\n", s.logPrefix, len(s.conns))
	return nil
}

// shutdown this gen server and all its routines.
func (s *Server) handleClose() {
	defer func() {
		if r := recover(); r != nil {
			c.Errorf("%v handleClose() crashed: %v\n", s.logPrefix, r)
			c.StackTrace(string(debug.Stack()))
		}
	}()

	s.lis.Close() // close listener daemon

	for raddr, nc := range s.conns {
		closeConnection(s.logPrefix, raddr, nc)
	}
	s.lis, s.conns = nil, nil
	close(s.finch)

	c.Infof("%v ... stopped\n", s.logPrefix)
	return
}

// start a connection worker to read mutation message for a subset of vbuckets.
func (s *Server) startWorker(raddr string) {
	c.Infof("%v starting worker for connection %q\n", s.logPrefix, raddr)
	nc := s.conns[raddr]
	go doReceive(s.logPrefix, nc, s.appch, s.reqch)
	nc.active = true
}

// jumbo size error handler, it either closes all connections and shutdown the
// server or it closes all open connections with faulting remote-host and
// returns back a message for application.
func (s *Server) jumboErrorHandler(
	raddr string, hostUuids keeper, err error) (msg interface{}) {

	var whatJumbo string

	if _, ok := s.conns[raddr]; ok == false {
		c.Errorf("%v fatal remote %q already gone\n", s.logPrefix, raddr)
		return nil
	}

	if err == io.EOF {
		c.Errorf("%v remote %q closed\n", s.logPrefix, raddr)
		whatJumbo = "closeremote"

	} else if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
		c.Errorf("%v remote %q timeout: %v\n", s.logPrefix, raddr, err)
		whatJumbo = "closeremote"

	} else if err != nil {
		c.Errorf("%v remote %q unknown error: %v\n", s.logPrefix, raddr, err)
		whatJumbo = "closeall"

	} else {
		c.Errorf("%v no error why did you call jumbo !!!\n", s.logPrefix)
		return
	}

	switch whatJumbo {
	case "closeremote":
		ce := NewConnectionError()
		for _, raddr := range remoteConnections(raddr, s.conns) {
			ce.Append(hostUuids, raddr)
			closeConnection(s.logPrefix, raddr, s.conns[raddr])
			delete(s.conns, raddr)
		}
		msg = ce

	case "closeall":
		ce := NewConnectionError()
		for raddr := range s.conns {
			ce.Append(hostUuids, raddr)
		}
		msg = ce
		go s.Close()
	}
	return
}

func closeConnection(prefix, raddr string, nc *netConn) {
	defer func() {
		if r := recover(); r != nil {
			c.Errorf("%v closeConnection(%q) crashed: %v", prefix, raddr, r)
			c.StackTrace(string(debug.Stack()))
		}
	}()
	close(nc.worker)
	nc.conn.Close()
	c.Infof("%v connection %q closed !\n", prefix, raddr)
}

// get all remote connections for `host`
func remoteConnections(raddr string, conns map[string]*netConn) []string {
	host, _, _ := net.SplitHostPort(raddr)
	raddrs := make([]string, 0)
	for s := range conns {
		if h, _, _ := net.SplitHostPort(s); h == host {
			raddrs = append(raddrs, s)
		}
	}
	return raddrs
}

// go-routine to listen for new connections, if this routine goes down -
// server is shutdown and reason notified back to application.
func listener(prefix string, lis net.Listener, reqch chan []interface{}) {
	defer func() {
		if r := recover(); r != nil {
			c.Errorf("%v listener crashed: %v", prefix, r)
			c.StackTrace(string(debug.Stack()))
			msg := serverMessage{cmd: serverCmdError, err: ErrorDaemonExit}
			reqch <- []interface{}{msg}
		}
	}()

loop:
	for {
		// TODO: handle `err` for lis.Close() and avoid panic(err)
		if conn, err := lis.Accept(); err != nil {
			if e, ok := err.(*net.OpError); ok && e.Op == "accept" {
				c.Infof("%v ... stopped", prefix)
				break loop
			} else {
				panic(err)
			}

		} else {
			msg := serverMessage{
				cmd:   serverCmdNewConnection,
				raddr: conn.RemoteAddr().String(),
				args:  []interface{}{conn},
			}
			reqch <- []interface{}{msg}
		}
	}
}

// per connection go-routine to read []*VbKeyVersions.
func doReceive(
	prefix string,
	nc *netConn,
	appch chan<- interface{},
	reqch chan<- []interface{}) {

	conn, worker := nc.conn, nc.worker

	// TODO: make it configurable
	flags := transport.TransportFlag(0).SetProtobuf()
	pkt := transport.NewTransportPacket(c.MaxDataportPayload, flags)
	pkt.SetEncoder(transport.EncodingProtobuf, protobufEncode)
	pkt.SetDecoder(transport.EncodingProtobuf, protobufDecode)

	msg := serverMessage{raddr: conn.RemoteAddr().String()}

	// create it here to avoid repeated allocation.
	started := make(keeper)  // id() -> activeVb
	finished := make(keeper) // id() -> activeVb

	beginsAndEnds := func(vbs []*protobuf.VbKeyVersions) {
		for _, vb := range vbs { // for each vbucket
			bucket, vbno := vb.GetBucketname(), uint16(vb.GetVbucket())
			avb := &activeVb{msg.raddr, bucket, vbno}
			id := avb.id()
			kvs := vb.GetKvs() // mutations for each vbucket

			for _, kv := range kvs {
				if len(kv.GetCommands()) == 0 {
					continue
				}
				switch byte(kv.GetCommands()[0]) {
				case c.StreamBegin:
					started[id] = avb
				case c.StreamEnd:
					finished[id] = avb
				}
			}
			c.Tracef("%v {%v, %v}\n", prefix, bucket, vbno)
		}
	}

loop:
	for {
		timeoutMs := c.DataportReadDeadline * time.Millisecond
		conn.SetReadDeadline(time.Now().Add(timeoutMs))
		msg.cmd, msg.err, msg.args = 0, nil, nil
		if payload, err := pkt.Receive(conn); err != nil {
			msg.cmd, msg.err = serverCmdError, err
			reqch <- []interface{}{msg}
			c.Errorf("%v worker %q exit: %v\n", prefix, msg.raddr, err)
			break loop

		} else if vbmap, ok := payload.(*protobuf.VbConnectionMap); ok {
			msg.cmd, msg.args = serverCmdVbmap, []interface{}{vbmap}
			reqch <- []interface{}{msg}
			format := "%v worker %q exit: `serverCmdVbmap`\n"
			c.Infof(format, prefix, msg.raddr)
			break loop

		} else if vbs, ok := payload.([]*protobuf.VbKeyVersions); ok {
			beginsAndEnds(vbs)
			select {
			case appch <- vbs:
				if len(started) > 0 || len(finished) > 0 {
					msg.cmd = serverCmdVbcontrol
					msg.args = []interface{}{started, finished}
					reqch <- []interface{}{msg}
					format := "%v worker %q exit: serverCmdVbcontrol {%v,%v}\n"
					c.Infof(format, prefix, msg.raddr, len(started), len(finished))
					break loop
				}

			case <-worker:
				msg.cmd, msg.err = serverCmdError, ErrorWorkerKilled
				reqch <- []interface{}{msg}
				c.Errorf("%v worker %q exit: %v\n", prefix, msg.raddr, msg.err)
				break loop
			}

		} else {
			msg.cmd, msg.err = serverCmdError, ErrorPayload
			reqch <- []interface{}{msg}
			c.Errorf("%v worker %q exit: %v\n", prefix, msg.raddr, msg.err)
			break loop
		}
	}
	nc.active = false
}

func vbucketSchedule(vb *protobuf.VbKeyVersions) (s, e *protobuf.KeyVersions) {
	for _, kv := range vb.GetKvs() {
		commands := kv.GetCommands()
		if len(commands) == 1 {
			switch byte(commands[0]) {
			case c.StreamBegin:
				s, e = kv, nil
			case c.StreamEnd:
				s, e = nil, kv
			}
		}
	}
	return s, e
}

// ConnectionError to application
type ConnectionError map[string][]uint16 // bucket -> []vbuckets

// NewConnectionError makes a new connection-error map.
func NewConnectionError() ConnectionError {
	return make(ConnectionError)
}

// Append {buckets,vbuckets} for connection error.
func (ce ConnectionError) Append(hostUuids keeper, raddr string) {
	for _, avb := range hostUuids {
		if avb.raddr != raddr {
			continue
		}
		vbs, ok := ce[avb.bucket]
		if !ok {
			vbs = make([]uint16, 0, 4) // TODO: avoid magic numbers
		}
		vbs = append(vbs, avb.vbno)
		ce[avb.bucket] = vbs
	}
}
