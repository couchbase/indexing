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
//                                ^                      ^
//                             (sideband)            (mutation)
//     NewServer() ----------*    |                      |
//             |             |    |                      | []*VbKeyVersions
//          (spawn)          |    | ShutdownDataport     |
//             |             |    | RestartVbuckets      |
//             |          (spawn) | RepairVbuckets       |
//           listener()      |    | error                |
//                 |         |    |                      |
//  serverCmdNewConnection   |    |                      |
//                 |         |    |                      |
//  Close() -------*------->gen-server()------*---- doReceive()----*
//          serverCmdClose        ^           |                    |
//                                |           *---- doReceive()----*
//                serverCmdVbmap  |           |                    |
//            serverCmdVbcontrol  |           *---- doReceive()----*
//                serverCmdError  |                                |
//                                *--------------------------------*
//                                          (control & faults)
//
// server behavior:
//
// 1. can handle more than one connection from same router.
// 2. whenever a connection with router fails, all active connections with
//    that router will be closed and the same will be intimated to
//    application for catchup connection, using []*RestartVbuckets message.
// 3. side band information to application,
//    a. RestartVbuckets, vbucket streams have ended.
//    b. RepairVbuckets, connection with an upstream host is closed.
//    c. ShutdownDataport, all connections with an upstream host is closed.
//    d. error string, describing the cause of the error.
// 4. when ever dataport-instance shuts down it will signal application by
//    side-band channel.

package dataport

import (
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/protobuf"
)

// Error codes

// ErrorPayload
var ErrorPayload = errors.New("dataport.daemonPayload")

// ErrorVbmap
var ErrorVbmap = errors.New("dataport.vbmap")

type activeVb struct {
	bucket string
	vbno   uint16
}

func (avb *activeVb) id() string {
	return fmt.Sprintf("%v-%v", avb.bucket, avb.vbno)
}

// Side band information

// RestartVbuckets to restart a subset of vuckets that has ended.
type RestartVbuckets map[string][]uint16 // bucket -> []vbuckets

// ShutdownDataport tells daemon has shutdown, provides vbuckets to restart.
type ShutdownDataport map[string][]uint16 // bucket -> []vbuckets

// RepairVbuckets to restart vbuckets for closed connection.
type RepairVbuckets map[string][]uint16 // bucket -> []vbuckets

// messages to gen-server
type serverMessage struct {
	cmd   byte          // gen server command
	raddr string        // routine for remote connection, optional
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
	mutch chan<- []*protobuf.VbKeyVersions // backchannel to application
	sbch  chan<- interface{}               // sideband channel to application

	// gen-server management
	reqch     chan []interface{}
	finch     chan bool
	conns     map[string]netConn // resolve <host:port> to conn. obj
	logPrefix string
}

// NewServer creates a new dataport daemon.
func NewServer(
	laddr string,
	mutch chan []*protobuf.VbKeyVersions,
	sbch chan<- interface{}) (s *Server, err error) {

	s = &Server{
		laddr: laddr,
		mutch: mutch,
		sbch:  sbch,

		// Managing vbuckets and connections for all routers
		reqch:     make(chan []interface{}, c.GenserverChannelSize),
		finch:     make(chan bool),
		conns:     make(map[string]netConn),
		logPrefix: fmt.Sprintf("[dataport %q]", laddr),
	}
	if s.lis, err = net.Listen("tcp", laddr); err != nil {
		c.Errorf("%v failed starting ! %v", s.logPrefix, err)
		return nil, err
	}
	go listener(laddr, s.lis, s.reqch) // spawn daemon
	go s.genServer(s.reqch, sbch)      // spawn gen-server
	c.Infof("%v started ...", s.logPrefix)
	return s, nil
}

func (s *Server) addUuids(started, avbs map[string]*activeVb) (map[string]*activeVb, error) {
	for x, newvb := range started {
		if avb, ok := avbs[x]; ok {
			c.Errorf("%v duplicate vbucket %v\n", s.logPrefix, avb)
			return nil, ErrorDuplicateStreamBegin
		}
		c.Infof("%v added vbucket %#v\n", s.logPrefix, newvb)
	}
	for x, newvb := range started {
		avbs[x] = newvb
	}
	return avbs, nil
}

func (s *Server) delUuids(finished, avbs map[string]*activeVb) (map[string]*activeVb, error) {
	for x, _ := range finished {
		if avb, ok := avbs[x]; !ok {
			c.Errorf("%v non-existent vbucket %v\n", s.logPrefix, avb)
			return nil, ErrorMissingStreamBegin
		}
	}
	for x, _ := range finished {
		delete(avbs, x)
	}
	return avbs, nil
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
func (s *Server) genServer(reqch chan []interface{}, sbch chan<- interface{}) {
	var appmsg interface{}

	defer func() { // panic safe
		if r := recover(); r != nil {
			msg := serverMessage{cmd: serverCmdClose}
			s.handleClose(msg)
			c.Errorf("%v gen-server fatal panic: %v\n", s.logPrefix, r)
		}
	}()
	// raddr -> id() -> activeVb
	remoteUuids := make(map[string]map[string]*activeVb)
loop:
	for {
		appmsg = nil
		select {
		case cmd := <-reqch:
			msg := cmd[0].(serverMessage)
			switch msg.cmd {
			case serverCmdNewConnection:
				var err error
				conn := msg.args[0].(net.Conn)
				if appmsg, err = s.handleNewConnection(msg); err != nil {
					conn.Close()
				} else {
					remoteUuids[msg.raddr] = make(map[string]*activeVb)
					s.startWorker(msg.raddr)
				}

			case serverCmdVbmap:
				vbmap := msg.args[0].(*protobuf.VbConnectionMap)
				b, raddr := vbmap.GetBucket(), msg.raddr
				for _, vbno := range vbmap.GetVbuckets() {
					avb := &activeVb{b, uint16(vbno)}
					remoteUuids[raddr][avb.id()] = avb
				}
				s.startWorker(msg.raddr)

			case serverCmdVbcontrol:
				var err error
				started := msg.args[0].(map[string]*activeVb)
				finished := msg.args[1].(map[string]*activeVb)
				avbs := remoteUuids[msg.raddr]
				if len(started) > 0 { // new vbucket stream(s) have started
					if avbs, err = s.addUuids(started, avbs); err != nil {
						panic(err)
					}
				}
				if len(finished) > 0 { // vbucket stream(s) have finished
					if avbs, err = s.delUuids(finished, avbs); err != nil {
						panic(err)
					}
					appmsg = vbucketsForRemote(finished) // RestartVbuckets{}
				}
				remoteUuids[msg.raddr] = avbs
				s.startWorker(msg.raddr)

			case serverCmdClose:
				respch := cmd[1].(chan []interface{})
				appmsg = s.handleClose(msg)
				respch <- []interface{}{nil}
				break loop

			case serverCmdError:
				raddr, err := msg.raddr, msg.err
				appmsg = s.jumboErrorHandler(raddr, remoteUuids, err)
			}
			if appmsg != nil {
				s.sbch <- appmsg
				c.Debugf("appmsg: %T:%+v\n", appmsg, appmsg)
			}
		}
	}
}

/**** gen-server handlers ****/

// handle new connection
func (s *Server) handleNewConnection(msg serverMessage) (interface{}, error) {
	conn := msg.args[0].(net.Conn)
	c.Infof("%v connection request from %q\n", s.logPrefix, msg.raddr)
	if _, _, err := net.SplitHostPort(msg.raddr); err != nil {
		c.Errorf("%v unable to parse %q\n", s.logPrefix, msg.raddr)
		return nil, err
	}
	if _, ok := s.conns[msg.raddr]; ok {
		c.Errorf("%v %q already active\n", s.logPrefix, msg.raddr)
		return nil, ErrorDuplicateClient
	}
	// connection accepted
	worker := make(chan interface{}, 10) // TODO: avoid magic numbers
	s.conns[msg.raddr] = netConn{conn: conn, worker: worker, active: false}
	c.Infof("%v total active connections %v\n", s.logPrefix, len(s.conns))
	return nil, nil
}

// shutdown this gen server and all its routines.
func (s *Server) handleClose(msg serverMessage) (appmsg interface{}) {
	defer func() {
		if r := recover(); r != nil {
			c.Errorf("%v handleClose fatal panic: %v\n", s.logPrefix, r)
		}
	}()

	c.Infof("%v shutting down\n", s.logPrefix)
	s.lis.Close()                          // close listener daemon
	closeConnections(s.logPrefix, s.conns) // close workers
	s.lis, s.conns = nil, nil
	close(s.finch)
	return
}

// start a connection worker to read mutation message for a subset of vbuckets.
func (s *Server) startWorker(raddr string) {
	c.Infof("%v starting worker for connection %q\n", s.logPrefix, raddr)
	nc := s.conns[raddr]
	go doReceive(s.logPrefix, nc, s.mutch, s.reqch)
	nc.active = true
}

// jumbo size error handler, it either closes all connections and shutdown the
// server or it closes all open connections with faulting remote-host and
// returns back a message for application.
func (s *Server) jumboErrorHandler(
	raddr string,
	remoteUuids map[string]map[string]*activeVb,
	err error) (msg interface{}) {

	var whatJumbo string

	// connection is already gone. TODO: make the following error message as
	// fatal.
	if _, ok := s.conns[raddr]; ok == false {
		c.Errorf("%v fatal remote %q already gone\n", s.logPrefix, raddr)
		return nil
	}

	if err == io.EOF {
		c.Errorf("%v remote %q closed\n", s.logPrefix, raddr)
		whatJumbo = "closeconn"
	} else if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
		c.Errorf("%v remote %q timedout\n", s.logPrefix, raddr)
		whatJumbo = "closeconn"
	} else if err != nil {
		c.Errorf("%v `%v` from %q\n", s.logPrefix, err, raddr)
		whatJumbo = "closeall"
	} else {
		c.Errorf("%v no error why did you call jumbo !!!\n", s.logPrefix)
		return
	}

	switch whatJumbo {
	case "closeconn":
		m := vbucketsForRemote(remoteUuids[raddr]) // RepairVbuckets{}
		msg = RepairVbuckets(m)
		closeConnection(s.logPrefix, raddr, s.conns)

	case "closeall":
		msg = vbucketsForRemotes(remoteUuids)
		go s.Close()
	}
	return
}

// close all connections and worker routines
func closeConnections(prefix string, conns map[string]netConn) {
	var closed []string

	raddrs := make([]string, 0, len(conns))
	for raddr := range conns {
		raddrs = append(raddrs, raddr)
	}
	for len(raddrs) > 0 {
		closed, conns = closeRemoteHost(prefix, raddrs[0], conns)
		raddrs = c.ExcludeStrings(raddrs, closed)
	}
}

func closeConnection(prefix, raddr string, conns map[string]netConn) {
	recoverClose := func(conn net.Conn) {
		defer func() {
			if r := recover(); r != nil {
				c.Errorf("%v panic closing connection %q", prefix, raddr)
			}
			conn.Close()
		}()
	}
	nc := conns[raddr]
	recoverClose(nc.conn)
	close(nc.worker)
	delete(conns, raddr)
	c.Infof("%v closed connection %q\n", prefix, raddr)
}

// close all connections with remote host.
func closeRemoteHost(prefix, raddr string, conns map[string]netConn) ([]string, map[string]netConn) {
	clientRaddrs := make([]string, 0, len(conns))
	if _, ok := conns[raddr]; ok {
		host, _, _ := net.SplitHostPort(raddr)
		// close all connections and worker routines for this remote host
		for _, craddr := range remoteConnections(host, conns) {
			closeConnection(prefix, craddr, conns)
			clientRaddrs = append(clientRaddrs, craddr)
			c.Infof("%v closed connection %q\n", prefix, craddr)
		}
	} else {
		c.Errorf("%v fatal unknown connection %q\n", prefix, raddr)
	}
	return clientRaddrs, conns
}

// get all remote connections for `host`
func remoteConnections(host string, conns map[string]netConn) []string {
	raddrs := make([]string, 0)
	for s := range conns {
		if h, _, _ := net.SplitHostPort(s); h == host {
			raddrs = append(raddrs, s)
		}
	}
	return raddrs
}

// gather vbuckets to restart for all remote connections
func vbucketsForRemotes(remotes map[string]map[string]*activeVb) ShutdownDataport {
	m := make(ShutdownDataport)
	for _, avbs := range remotes {
		for _, avb := range avbs {
			vbs, ok := m[avb.bucket]
			if !ok {
				vbs = make([]uint16, 0, 4) // TODO: avoid magic numbers
			}
			vbs = append(vbs, avb.vbno)
			m[avb.bucket] = vbs
		}
	}
	return m
}

// gather vbuckets to restart for a single remote-connection.
func vbucketsForRemote(avbs map[string]*activeVb) RestartVbuckets {
	m := make(RestartVbuckets)
	for _, avb := range avbs {
		vbs, ok := m[avb.bucket]
		if !ok {
			vbs = make([]uint16, 0, 4) // TODO: avoid magic numbers
		}
		vbs = append(vbs, avb.vbno)
		m[avb.bucket] = vbs
	}
	return m
}

// go-routine to listen for new connections, if this routine goes down -
// server is shutdown and reason notified back to application.
func listener(laddr string, lis net.Listener, reqch chan []interface{}) {
	defer func() {
		c.Errorf("%v listener fatal panic: %v", laddr, recover())
		msg := serverMessage{cmd: serverCmdError, err: ErrorDaemonExit}
		reqch <- []interface{}{msg}
	}()
	for {
		// TODO: handle `err` for lis.Close() and avoid panic(err)
		if conn, err := lis.Accept(); err != nil {
			panic(err)
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
func doReceive(prefix string, nc netConn, mutch chan<- []*protobuf.VbKeyVersions, reqch chan<- []interface{}) {
	conn, worker := nc.conn, nc.worker
	flags := TransportFlag(0).SetProtobuf() // TODO: make it configurable
	pkt := NewTransportPacket(c.MaxDataportPayload, flags)
	msg := serverMessage{raddr: conn.RemoteAddr().String()}

	started := make(map[string]*activeVb)  // TODO: avoid magic numbers
	finished := make(map[string]*activeVb) // TODO: avoid magic numbers

	// detect StreamBegin and StreamEnd messages.
	// TODO: function uses 2 level of loops, figure out a smart way to identify
	//       presence of StreamBegin/StreamEnd so that we can avoid looping.
	updateActiveVbuckets := func(vbs []*protobuf.VbKeyVersions) {
		for _, vb := range vbs {
			bucket, vbno := vb.GetBucketname(), uint16(vb.GetVbucket())
			kvs := vb.GetKvs()

			commands := make([]uint32, 0, len(kvs))
			seqnos := make([]uint64, 0, len(kvs))

			// TODO: optimize this.
			for _, kv := range kvs {
				seqnos = append(seqnos, kv.GetSeqno())
				commands = append(commands, kv.GetCommands()...)
				commands = append(commands, 17)

				avb := &activeVb{bucket, vbno}
				if byte(kv.GetCommands()[0]) == c.StreamBegin {
					started[avb.id()] = avb
				} else if byte(kv.GetCommands()[0]) == c.StreamEnd {
					finished[avb.id()] = avb
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
			c.Errorf("%v worker %q exited %v\n", prefix, msg.raddr, err)
			break loop

		} else if vbmap, ok := payload.(*protobuf.VbConnectionMap); ok {
			msg.cmd, msg.args = serverCmdVbmap, []interface{}{vbmap}
			reqch <- []interface{}{msg}
			c.Infof(
				"%v worker %q exiting with `serverCmdVbmap`\n",
				prefix, msg.raddr)
			break loop

		} else if vbs, ok := payload.([]*protobuf.VbKeyVersions); ok {
			updateActiveVbuckets(vbs)
			select {
			case mutch <- vbs:
				if len(started) > 0 || len(finished) > 0 {
					msg.cmd = serverCmdVbcontrol
					msg.args = []interface{}{started, finished}
					reqch <- []interface{}{msg}
					c.Infof(
						"%v worker %q exit with %q {%v,%v}\n",
						prefix, msg.raddr, `serverCmdVbcontrol`,
						len(started), len(finished))
					break loop
				}

			case <-worker:
				msg.cmd, msg.err = serverCmdError, ErrorWorkerKilled
				reqch <- []interface{}{msg}
				c.Errorf("%v worker %q exited %v\n", prefix, msg.raddr, err)
				break loop
			}

		} else {
			msg.cmd, msg.err = serverCmdError, ErrorPayload
			reqch <- []interface{}{msg}
			c.Errorf("%v worker %q exited %v\n", prefix, msg.raddr, err)
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
