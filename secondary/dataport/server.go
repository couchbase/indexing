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
//        serverCmdVbKeyVersions |           *---- doReceive()----*
//                serverCmdError |                                |
//                               *--------------------------------*
//                                          (control & faults)
//
// server behavior:
//
// 1. can handle more than one connection from same router.
//
// 2. whenever a connection with router
//    a. gets closed
//    b. or timeout
//    all connections with that router will be closed and same will
//    be intimated to application for catchup connection, using
//    ConnectionError message.
//
// 3. StreamEnd, ConnectionError can be seen by serve due to,
//    a. rebalance
//    b. failover
//    c. projector crash
//    d. network partition
//    e. DCP dropping the connection
//    f. partial stream start
//    g. bucket delete
//    h. bucket flush
//    i. DCP feed error

package dataport

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	c "github.com/couchbase/indexing/secondary/common"

	"github.com/couchbase/cbauth"

	"github.com/couchbase/indexing/secondary/logging"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/data"
	"github.com/couchbase/indexing/secondary/security"
	"github.com/couchbase/indexing/secondary/transport"
)

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
	raddr      string // remote connection carrying this vbucket.
	keyspaceId string
	vbno       uint16
	kvers      uint64
	seqno      uint64
	opaque     uint64 // Indexer's sessionId for stream request
}

type keeper map[string]*activeVb

func (avb *activeVb) id() string {
	return fmt.Sprintf("%v#%v#%v", avb.raddr, avb.keyspaceId, avb.vbno)
}

func (hostUuids keeper) isActive(keyspaceId string, vbno uint16) bool {
	for _, avb := range hostUuids {
		if avb.keyspaceId == keyspaceId && avb.vbno == vbno {
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
	tpkt   *transport.TransportPacket
}

// Server handles an active dataport server of mutation for all vbuckets.
type Server struct {
	laddr string // address to listen
	lis   net.Listener
	appch chan<- interface{} // backchannel to application

	// gen-server management
	conns  map[string]*netConn // resolve <host:port> to conn. obj
	reqch  chan []interface{}
	datach chan []interface{}
	finch  chan bool

	// config parameters
	maxVbuckets  int
	genChSize    int           // channel size for genServer routine
	maxPayload   int           // maximum payload length from router
	readDeadline time.Duration // timeout, in millisecond, reading from socket
	logPrefix    string
	enableAuth   *uint32

	mu sync.Mutex
}

// NewServer creates a new dataport daemon.
func NewServer(
	laddr string,
	maxvbs int,
	config c.Config,
	appch chan<- interface{},
	enableAuth *uint32) (s *Server, err error) {

	genChSize := config["genServerChanSize"].Int()
	dataChSize := config["dataChanSize"].Int()

	s = &Server{
		laddr: laddr,
		appch: appch,
		// Managing vbuckets and connections for all routers
		reqch:  make(chan []interface{}, genChSize),
		datach: make(chan []interface{}, dataChSize),
		finch:  make(chan bool, 1),
		conns:  make(map[string]*netConn),
		// config parameters
		maxVbuckets:  maxvbs,
		genChSize:    genChSize,
		maxPayload:   config["maxPayload"].Int(),
		readDeadline: time.Duration(config["tcpReadDeadline"].Int()),
		enableAuth:   enableAuth,
	}
	s.logPrefix = fmt.Sprintf("DATP[->dataport %q]", laddr)

	if s.lis, err = security.MakeListener(laddr); err != nil {
		logging.Errorf("%v failed starting! %v\n", s.logPrefix, err)
		return nil, err
	}
	go s.listener()                   // spawn daemon
	go s.genServer(s.reqch, s.datach) // spawn gen-server
	logging.Infof("%v started ...", s.logPrefix)
	return s, nil
}

func (s *Server) addUuids(started, hostUuids keeper) keeper {
	for x, newvb := range started {
		if hostUuids.isActive(newvb.keyspaceId, newvb.vbno) {
			logging.Errorf("%v duplicate vbucket %v\n", s.logPrefix, newvb.id())
		}
		hostUuids[x] = newvb
		logging.Debugf("%v added vbucket %v\n", s.logPrefix, newvb.id())
	}
	return hostUuids
}

func (s *Server) delUuids(finished, hostUuids keeper) keeper {
	for x := range finished {
		avb, ok := hostUuids[x]
		if ok {
			delete(hostUuids, x)
			logging.Debugf("%v deleted vbucket %v\n", s.logPrefix, avb.id())
		} else {
			logging.Errorf("%v not active vbucket %v\n", s.logPrefix, x)
		}
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

func (s *Server) ResetConnections() (err error) {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{serverMessage{cmd: serverCmdResetConnections}, respch}
	resp, err := c.FailsafeOp(s.reqch, respch, cmd, s.finch)
	return c.OpError(err, resp, 0)
}

// gen-server commands
const (
	serverCmdNewConnection byte = iota + 1
	serverCmdVbmap
	serverCmdVbKeyVersions
	serverCmdError
	serverCmdClose
	serverCmdResetConnections
)

// gen server routine for dataport server.
func (s *Server) genServer(reqch, datach chan []interface{}) {
	defer func() {
		if r := recover(); r != nil {
			logging.Errorf("%v gen-server crashed: %v\n", s.logPrefix, r)
			logging.Errorf("%s", logging.StackTrace())
			s.handleClose()
		}
	}()

	hostUuids := make(keeper) // id() -> activeVb
	parseVbs := func(msg serverMessage) []*protobuf.VbKeyVersions {
		vbs := msg.args[0].([]*protobuf.VbKeyVersions)
		prune_off := 0
		for i := 0; i < len(vbs); i++ { //for each vbucket
			vb := vbs[i]
			keyspaceId, vbno := vb.GetKeyspaceId(), uint16(vb.GetVbucket())
			opaque := vb.GetOpaque2() //sessionId for the stream request
			id := (&activeVb{raddr: msg.raddr, keyspaceId: keyspaceId, vbno: vbno, opaque: opaque}).id()
			kvs := vb.GetKvs() // mutations for each vbucket

			// filter mutations for vbucket that is not from the same
			// source as its StreamBegin.
			avb, avbok := hostUuids[id]
			if avbok && (msg.raddr != avb.raddr) {
				fmsg := "%v filter %d mutations for %v\n"
				logging.Warnf(fmsg, s.logPrefix, len(kvs), id)
				continue
			}
			vbok := false
			for _, kv := range kvs {
				if len(kv.GetCommands()) == 0 {
					continue
				}
				switch byte(kv.GetCommands()[0]) {
				case c.StreamBegin: // new vbucket stream(s) have started
					avb = &activeVb{raddr: msg.raddr, keyspaceId: keyspaceId, vbno: vbno, opaque: opaque}
					hostUuids = s.addUuids(keeper{id: avb}, hostUuids)
					avbok, vbok = true, true

				case c.StreamEnd: // vbucket stream(s) have finished
					avb = &activeVb{raddr: msg.raddr, keyspaceId: keyspaceId, vbno: vbno, opaque: opaque}
					if curravb, ok := hostUuids[id]; ok {
						if avb.opaque == curravb.opaque {
							hostUuids = s.delUuids(keeper{id: avb}, hostUuids)
							vbok = true
						} else {
							//If the sessionId(opaque) doesn't match, do not update bookkeeping. If stream is
							//close/opened by Indexer in quick succession, StreamEnd from previous session can arrive
							//after StreamBegin from the later session.
							logging.Infof("%v skipped delete vbucket %v curr opaque %v, received opaque %v\n",
								s.logPrefix, avb.id(), curravb.opaque, avb.opaque)
						}
					} else {
						fmsg := "%v StreamEnd without StreamBegin for %v\n"
						logging.Warnf(fmsg, s.logPrefix, id)
					}
				case c.Upsert, c.Deletion, c.UpsertDeletion:
					if avbok && avb != nil {
						avb.seqno = kv.GetSeqno()
						avb.kvers++
					}
				}
			}
			// mutations received without a STREAM_BEGIN
			if _, ok := hostUuids[id]; ok || vbok {
				vbs[prune_off] = vb
				prune_off++
			} else {
				fmsg := "%v mutations filtered for %v\n"
				logging.Warnf(fmsg, s.logPrefix, id)
			}
			logging.Tracef("%v {%v, %v}\n", s.logPrefix, keyspaceId, vbno)
		}
		vbs = vbs[:prune_off]
		return vbs
	}

	handlereq := func(cmd []interface{}) {
		select {
		case <-s.finch:
			logging.Errorf("%v %q already closed\n", s.logPrefix, s.laddr)
			return
		default:
			msg := cmd[0].(serverMessage)
			switch msg.cmd {
			case serverCmdNewConnection:
				conn, raddr := msg.args[0].(net.Conn), msg.raddr
				reqMsg := msg.args[1]

				if _, ok := s.conns[raddr]; ok {
					logging.Errorf("%v %q already active\n", s.logPrefix, raddr)
					conn.Close()

				} else { // connection accepted
					worker := make(chan interface{}, s.maxVbuckets)
					s.conns[raddr] = &netConn{
						conn: conn, worker: worker,
						tpkt: newTransportPkt(s.maxPayload),
					}
					n := len(s.conns)
					fmsg := "%v new connection %q +%d\n"
					logging.Infof(fmsg, s.logPrefix, raddr, n)
					s.startWorker(raddr, reqMsg)
				}

			case serverCmdClose:
				// before closing the dataport-server log a consolidated
				// stats on the active-vbuckets.
				s.logStats(hostUuids)
				respch := cmd[1].(chan []interface{})
				s.handleClose()
				respch <- []interface{}{nil}

			case serverCmdResetConnections:
				logging.Errorf("%v %q reset connections ...\n", s.logPrefix, s.laddr)
				respch := cmd[1].(chan []interface{})
				err := s.handleResetConnections()
				respch <- []interface{}{err}
			}
		}
	}

	nicetoapp := func(msg interface{}) {
		for {
			select {
			case <-s.finch:
				return
			case s.appch <- msg:
				return
			case cmd := <-s.reqch:
				handlereq(cmd)
			}
		}
	}

loop:
	for {
		select {
		case cmd := <-reqch:
			handlereq(cmd)

		case datacmd := <-datach:
			msg := datacmd[0].(serverMessage)
			switch msg.cmd {
			case serverCmdVbmap:
				vbmap := msg.args[0].(*protobuf.VbConnectionMap)
				keyspaceId, raddr := vbmap.GetKeyspaceId(), msg.raddr
				for _, vbno := range vbmap.GetVbuckets() {
					avb := &activeVb{raddr: raddr, keyspaceId: keyspaceId, vbno: uint16(vbno)}
					hostUuids[avb.id()] = avb
				}
				s.startWorker(msg.raddr, nil)

			case serverCmdVbKeyVersions:
				nicetoapp(parseVbs(msg))

			case serverCmdError:
				var g interface{}
				hostUuids, g = s.jumboErrorHandler(msg.raddr, hostUuids, msg.err)
				if g != nil {
					nicetoapp(g)
					logging.Tracef("%v appmsg: %T:%+v\n", s.logPrefix, g, g)
				}
			}

		case <-s.finch:
			break loop
		}
	}
}

// shutdown this gen server and all its routines.
func (s *Server) handleClose() {
	defer func() {
		if r := recover(); r != nil {
			logging.Errorf("%v handleClose() crashed: %v\n", s.logPrefix, r)
			logging.Errorf("%s", logging.StackTrace())
		}
	}()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.lis != nil {
		s.lis.Close() // close listener daemon
	}

	for raddr, nc := range s.conns {
		closeConnection(s.logPrefix, raddr, nc)
	}
	s.lis, s.conns = nil, nil
	close(s.finch)

	logging.Infof("%v ... stopped\n", s.logPrefix)
	return
}

// restart
func (s *Server) handleResetConnections() error {

	func() {
		s.mu.Lock()
		defer s.mu.Unlock()

		// close listener daemon
		if s.lis != nil {
			s.lis.Close()
			s.lis = nil
		}
	}()

	// close connections (simulate network partition)
	for _, nc := range s.conns {
		nc.conn.Close()
	}

	// restart listener
	fn := func(r int, e error) error {
		var err error
		s.mu.Lock()
		defer s.mu.Unlock()

		if s.lis, err = security.MakeListener(s.laddr); err != nil {
			logging.Errorf("%v failed starting listener %v! %v\n", s.logPrefix, s.laddr, err)
			return err
		}
		go s.listener() // spawn daemon

		return nil
	}
	helper := c.NewRetryHelper(10, time.Second, 1, fn)
	if err := helper.Run(); err != nil {
		return err
	}

	return nil
}

// start a connection worker to read mutation message for a subset of vbuckets.
func (s *Server) startWorker(raddr string, reqMsg interface{}) {
	nc, ok := s.conns[raddr]
	if !ok {
		fmsg := "%v connection %q already gone stale !\n"
		logging.Infof(fmsg, s.logPrefix, raddr)
		return
	}
	logging.Tracef("%v starting worker for connection %q\n", s.logPrefix, raddr)
	go doReceive(s.logPrefix, nc, s.maxPayload, s.readDeadline, s.datach, reqMsg)
	nc.active = true
}

// jumbo size error handler, it either closes all connections and shutdown the
// server or it closes all open connections with faulting remote-host and
// returns back a message for application.
func (s *Server) jumboErrorHandler(
	raddr string, hostUuids keeper,
	err error) (actvUuids keeper, msg interface{}) {

	var whatJumbo string

	if _, ok := s.conns[raddr]; ok == false {
		logging.Errorf("%v fatal remote %q already gone\n", s.logPrefix, raddr)
		return hostUuids, nil
	}

	if err == io.EOF {
		logging.Errorf("%v remote %q closed\n", s.logPrefix, raddr)
		whatJumbo = "closeremote"

	} else if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
		logging.Errorf("%v remote %q timeout: %v\n", s.logPrefix, raddr, err)
		whatJumbo = "closeremote"

	} else if err != nil {
		fmsg := "%v remote %q unknown error: %v\n"
		logging.Errorf(fmsg, s.logPrefix, raddr, err)
		whatJumbo = "closeremote"

	} else {
		logging.Errorf("%v no error why did you call jumbo !!!\n", s.logPrefix)
		return hostUuids, nil
	}

	switch whatJumbo {
	case "closeremote":
		ce := NewConnectionError()
		finished := ce.Append(hostUuids, raddr)
		actvUuids = s.delUuids(finished, hostUuids)
		closeConnection(s.logPrefix, raddr, s.conns[raddr])
		delete(s.conns, raddr)
		msg = ce

	// NOTE: application does not expect dataport-server to be automatically
	// closed.
	case "closeall":
		ce := NewConnectionError()
		for raddr := range s.conns {
			finished := ce.Append(hostUuids, raddr)
			actvUuids = s.delUuids(finished, hostUuids)
		}
		msg = ce
		go s.Close()
	}
	return actvUuids, msg
}

func (s *Server) logStats(hostUuids keeper) {
	keyspaceIdkvs := make(map[string][]uint64)    // keyspaceId -> []count
	keyspaceIdseqnos := make(map[string][]uint64) // keyspaceId -> []seqno
	for _, avb := range hostUuids {
		counts, ok := keyspaceIdkvs[avb.keyspaceId]
		seqnos, ok := keyspaceIdseqnos[avb.keyspaceId]
		if !ok {
			counts = make([]uint64, s.maxVbuckets)
			seqnos = make([]uint64, s.maxVbuckets)
		}
		counts[avb.vbno] = avb.kvers
		seqnos[avb.vbno] = avb.seqno
		keyspaceIdkvs[avb.keyspaceId] = counts
		keyspaceIdseqnos[avb.keyspaceId] = seqnos
	}
	for keyspaceId, counts := range keyspaceIdkvs {
		seqnos := keyspaceIdseqnos[keyspaceId]
		fmsg := "%v keyspaceId: %v total received key-versions: %v\n"
		logging.Infof(fmsg, s.logPrefix, keyspaceId, counts)
		fmsg = "%v keyspaceId: %v latest sequence numbers: %v\n"
		logging.Infof(fmsg, s.logPrefix, keyspaceId, seqnos)
	}
}

func closeConnection(prefix, raddr string, nc *netConn) {
	defer func() {
		if r := recover(); r != nil {
			logging.Errorf("%v closeConnection(%q) crashed: %v\n", prefix, raddr, r)
			logging.Errorf("%s", logging.StackTrace())
		}
	}()
	close(nc.worker)
	nc.conn.Close()
	logging.Infof("%v connection %q closed !\n", prefix, raddr)
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
func (s *Server) listener() {

	lis := func() net.Listener {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.lis
	}()

	selfRestart := func() {
		var err error
		s.mu.Lock()
		defer s.mu.Unlock()

		if s.lis != nil && lis == s.lis { // if s.lis == nil, then Server.Close() was called
			s.lis.Close()
			if s.lis, err = security.MakeListener(s.laddr); err != nil {
				logging.Errorf("%v failed starting %v !!\n", s.logPrefix, err)
				panic(err)
			}
			logging.Errorf("%v Restarting listener\n", s.logPrefix)
			go s.listener()
		}
	}

	defer func() {
		if r := recover(); r != nil {
			logging.Errorf("%v listener() crashed: %v\n", s.logPrefix, r)
			logging.Errorf("%s", logging.StackTrace())
		}
		selfRestart()
	}()

	for {
		if conn, err := lis.Accept(); err != nil {
			e, ok := err.(*net.OpError)
			if ok && (e.Err == syscall.EMFILE || e.Err == syscall.ENFILE) {
				logging.Errorf("%v Accept() Error: %v. Retrying Accept.\n", s.logPrefix, err)
				continue
			}
			logging.Errorf("%v Accept() Error: %v\n", s.logPrefix, err)
			break //After loop breaks, selfRestart() is called in defer

		} else {
			go s.handleConnection(conn)
		}
	}
}

func (s *Server) handleConnection(conn net.Conn) {

	raddr := conn.RemoteAddr().String()

	reqMsg, err := s.doAuth(conn)
	if err != nil {
		logging.Errorf("%v %q error in authentication %v", s.logPrefix, raddr, err)
		conn.Close()
		return
	}

	msg := serverMessage{
		cmd:   serverCmdNewConnection,
		raddr: raddr,
	}

	args := make([]interface{}, 2)
	args[0] = conn
	args[1] = reqMsg

	msg.args = args
	s.reqch <- []interface{}{msg}
}

func (s *Server) doAuth(conn net.Conn) (interface{}, error) {

	raddr := conn.RemoteAddr()

	rpkt := newTransportPkt(s.maxPayload)
	logging.Infof("%v connection %q doAuth() ...", s.logPrefix, raddr)

	// Set read deadline for auth to 10 Seconds.
	if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		logging.Warnf("%v doAuth %q error %v in SetReadDeadline", s.logPrefix, raddr, err)
	}

	reqMsg, err := rpkt.Receive(conn)
	if err != nil {
		return nil, err
	}

	req, ok := reqMsg.(*protobuf.AuthRequest)
	if !ok {
		logging.Infof("%v connection %q doAuth() authentication is missing", s.logPrefix, raddr)

		if c.GetClusterVersion() < c.INDEXER_71_VERSION {
			logging.Infof("%v connection %q continue without auth", s.logPrefix, raddr)
			return reqMsg, nil
		}

		// When cluster is getting upgraded, client may lag behind in
		// receiving cluster upgrade notification as compared to the server.
		// In such a case, server will close the connection.
		//
		// Client can choose to either (1) handle this closed connection
		// intelligently or (2) take more disruptive code path where indexer has
		// to trigger the stream restart.
		//
		// To avoid disruptive stream restart, user can enable server auth only
		// when the "enableAuth" setting is enabled.

		if atomic.LoadUint32(s.enableAuth) == 0 {
			logging.Infof("%v connection %q continue without auth, as auth is disabled", s.logPrefix, raddr)
			return reqMsg, nil
		}

		return nil, c.ErrAuthMissing

	} else {
		// The upgraded server always accepts the auth request.
		_, err = cbauth.Auth(req.GetUser(), req.GetPass())
		if err != nil {
			logging.Errorf("%v connection %q doAuth() error %v", s.logPrefix, raddr, err)
			return nil, errors.New("Unauthenticated access. Authentication failure.")
		}

		logging.Infof("%v connection %q auth successful", s.logPrefix, raddr)

		// TODO: RBAC

		return nil, nil
	}

	return nil, nil
}

// per connection go-routine to read []*VbKeyVersions.
func doReceive(
	prefix string,
	nc *netConn,
	maxPayload int, readDeadline time.Duration,
	datach chan<- []interface{},
	reqMsg interface{}) {

	conn, worker := nc.conn, nc.worker

	pkt := nc.tpkt
	msg := serverMessage{raddr: conn.RemoteAddr().String()}

	var duration time.Duration
	var start time.Time
	var blocked bool

	epoc := time.Now()
	tick := time.NewTicker(time.Second * 5) // log every 5 second, if blocked
	defer func() {
		tick.Stop()
	}()

loop:
	for {
		timeoutMs := readDeadline * time.Millisecond
		conn.SetReadDeadline(time.Now().Add(timeoutMs))
		msg.cmd, msg.err, msg.args = 0, nil, nil

		var payload interface{}
		var err error

		if reqMsg != nil {
			payload = reqMsg
			reqMsg = nil
		} else {
			payload, err = pkt.Receive(conn)
			if err != nil {
				msg.cmd, msg.err = serverCmdError, err
				datach <- []interface{}{msg}
				logging.Errorf("%v worker %q exit: %v\n", prefix, msg.raddr, err)
				break loop
			}
		}

		if vbmap, ok := payload.(*protobuf.VbConnectionMap); ok {
			msg.cmd, msg.args = serverCmdVbmap, []interface{}{vbmap}
			datach <- []interface{}{msg}
			fmsg := "%v worker %q exit: `serverCmdVbmap`\n"
			logging.Tracef(fmsg, prefix, msg.raddr)
			break loop

		} else if vbs, ok := payload.([]*protobuf.VbKeyVersions); ok {
			msg.cmd, msg.args = serverCmdVbKeyVersions, []interface{}{vbs}
			if len(datach) == cap(datach) {
				start, blocked = time.Now(), true
			}
			select {
			case datach <- []interface{}{msg}:
			case <-worker:
				msg.cmd, msg.err = serverCmdError, ErrorWorkerKilled
				datach <- []interface{}{msg}
				fmsg := "%v worker %q exit: %v\n"
				logging.Errorf(fmsg, prefix, msg.raddr, msg.err)
				break loop
			}
			if blocked {
				duration += time.Since(start)
				blocked = false
				select {
				case <-tick.C:
					ratio := float64(duration) / float64(time.Since(epoc))
					fmsg := "%v DATP -> Indexer %f%% blocked"
					logging.Infof(fmsg, prefix, ratio*100)
				default:
				}
			}

		} else {
			msg.cmd, msg.err = serverCmdError, ErrorPayload
			datach <- []interface{}{msg}
			logging.Errorf("%v worker %q exit: %v\n", prefix, msg.raddr, msg.err)
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
type ConnectionError map[string][]uint16 // keyspaceId -> []vbuckets

// NewConnectionError makes a new connection-error map.
func NewConnectionError() ConnectionError {
	return make(ConnectionError)
}

// Append {keyspaceIds,vbuckets} for connection error.
func (ce ConnectionError) Append(hostUuids keeper, raddr string) keeper {
	finished := make(keeper)
	for uuid, avb := range hostUuids {
		if avb.raddr != raddr {
			continue
		}
		finished[uuid] = avb
		vbs, ok := ce[avb.keyspaceId]
		if !ok {
			vbs = make([]uint16, 0, 4)
		}
		vbs = append(vbs, avb.vbno)
		ce[avb.keyspaceId] = vbs
	}
	return finished
}

func newTransportPkt(maxPayload int) *transport.TransportPacket {
	flags := transport.TransportFlag(0).SetProtobuf()
	pkt := transport.NewTransportPacket(maxPayload, flags)
	pkt.SetEncoder(transport.EncodingProtobuf, protobufEncode)
	pkt.SetDecoder(transport.EncodingProtobuf, protobufDecode)
	return pkt
}
