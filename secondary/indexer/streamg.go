// A gen server behavior for stream consumer.
//
// concurrency model:
//
//                                     Application back channels,
//                                 mutation channel and sideband channel
//                             -----------------------------------------------
//                                ^                    ^
//                             (sideband)         (mutation)
//     NewMutationStream() --*    |                    |
//             |             |    |                    |
//          (spawn)          |    |                    |  (controls & faults)
//             |             |    |  *----------------------------------------*
//             |          (spawn) |  |                 |                      |
//       streamListener()    |    |  |        *---- doReceiveKeyVersions()----*
//                 |         |    |  |        |                               |
//              (Accept)     |    |  |        *---- doReceiveKeyVersions()----*
//                 |         |    |  V        |                               |
//  Stop() --------*------->gen-server()------*---- doReceiveKeyVersions()----*
//                           |           ^    |                               |
//                           |           |    *---- doReceiveKeyVersions()----*
//                        (spawn)        |    |                               |
//                           |           |    *---- doReceiveKeyVersions()----*
//                           V           |             (run in a loop)
//                          doReceiveVbmap()
//                            (not a loop)
//
//
// server behavior:
//
// 1. can handle more than one connection from same router.
// 2. receives a subset of [{vbseqno, vbuuid}, ...] over each connection.
// 3. whenever a connection with router fails, all active connections with
//    that router will be closed and the same will be intimated to
//    application for catchup connection.
// 4. STREAM_END and STREAM_BEGIN transitions for a vbucket will be
//    automatically handled.
// 5. in case STREAM_END or STREAM_BEGIN is lost, it will be considered as
//    error case, will close all the connection with the corresponding
//    router(s) and the same will be intimated to application for catchup
//    connection.
// 6. side band information to application,
//    a. DropVbucketData{vbnos: vbnos}, based on upstream notification
//    b. RestartVbuckets{vbnos: vbnos}, when all connections with an upstream
//       host is closed.
//    c. StreamCascadingFault, when there a fault within a fault.
//    d. error string, describing the cause of the error.
//
//    in case of c and d, all connections with all upstream host will be
//    closed and stream-instance will be shutdown.
// 7. when ever stream-instance shuts down it will signal application by
//    closing downstream's mutation channel and side-band channel.

package indexer

import (
	"fmt"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/protobuf"
	"io"
	"log"
	"net"
	"time"
)

// Error messages
var (
	StreamDaemonExit      = fmt.Errorf("streamDaemonExit")
	StreamWorkerKilled    = fmt.Errorf("streamWorkerKilled")
	StreamWorkerHarakiri  = fmt.Errorf("streamWorkerHarakiri")
	StreamConnectionError = fmt.Errorf("streamConnectionError")
	StreamPayloadError    = fmt.Errorf("streamPayloadError")
	StreamCascadingFault  = fmt.Errorf("streamCascadingFault")
	StreamUnexpectedError = fmt.Errorf("streamUnexpectedError")
)

// RestartVbuckets tells application to restart specified list of vbuckets.
type RestartVbuckets struct {
	vbnos []uint16
}

// DropVbucketData tells application that specified vbuckets have lost data.
type DropVbucketData struct {
	vbnos []uint16
}

// gen-server commands
const (
	streamgNewConnection byte = iota + 1
	streamgVbmap
	streamgVbcontrol
	streamgError
	streamgShutdown
)

// messages to gen-server
type streamServerMessage struct {
	cmd   byte          // gen server command
	args  []interface{} // command arguments
	raddr string        // routine for remote connection, optional
	err   error         // many times the message will be error
}

// structure that maintains information about each remote connection
type netConn struct {
	conn   net.Conn
	worker chan bool
	active bool
	ts     uint64
}

// MutationStream handles an active stream of mutation for all vbuckets.
type MutationStream struct {
	laddr string             // address to listen
	mutch chan<- interface{} // backchannel to send mutations
	sbch  chan<- interface{} // backchannel to send side-band info
	lis   net.Listener

	// gen-server management
	timeout  time.Duration
	reqch    chan streamServerMessage
	conns    map[string]netConn  // resolve <host:port> to con. obj
	vbuckets map[uint16][]string // map of vbno to <host:port>
	connVbs  map[string][]uint16 // map <host:port> to array of vbuckets
	workerq  []string            // list of workers waiting to be started
}

// NewMutationStream creates a new mutation stream, which have an instance of
// concurrent model, described elsewhere, to handle live streaming of
// mutations from mutiple remote nodes.
//
// Failure and corresponding error messages are due to tcp connection.
func NewMutationStream(
	laddr string,
	mutch chan<- interface{},
	sbch chan<- interface{},
	timeout time.Duration) (s *MutationStream, err error) {

	s = &MutationStream{
		laddr: laddr,
		mutch: mutch,
		sbch:  sbch,

		// Managing vbuckets and connections for all routers
		timeout:  timeout,
		reqch:    make(chan streamServerMessage, 10000), // TODO: magic number
		conns:    make(map[string]netConn),
		vbuckets: make(map[uint16][]string),
		connVbs:  make(map[string][]uint16),
		workerq:  make([]string, 0),
	}

	if s.lis, err = net.Listen("tcp", laddr); err != nil {
		return nil, err
	}

	// there should be alteast be one remote address supplying mutations for
	// each vbucket.
	for i := uint16(0); i < GetMaxVbuckets(); i++ {
		s.vbuckets[i] = make([]string, 0, 1)
	}

	go streamListen(laddr, s.lis, s.reqch) // spawn daemon
	go s.run()                             // spawn gen-server
	return s, nil
}

// Shutdown closes the daemon listening for new connections and shuts down
// all read routines for the stream. Call does not return until shutdown is
// compeleted.
func (s *MutationStream) Shutdown() {
	respch := make(chan interface{})
	msg := streamServerMessage{cmd: streamgShutdown, args: []interface{}{respch}}
	s.reqch <- msg
	<-respch
}

// run is the generic server routine that serializes side band information for
// upstream connections and application APIs.
func (s *MutationStream) run() {
	var appmsg interface{}
loop:
	for {
		appmsg = nil
		// timeout to restart worker routines in workerq.
		// TODO: make this timeout configurable
		after := time.After(10 * time.Millisecond)
		select {
		case msg := <-s.reqch:
			// handle message to gen-server
			switch msg.cmd {
			case streamgNewConnection:
				appmsg = s.handleNewConnection(msg)
			case streamgVbmap:
				appmsg = s.handleVbmap(msg)
			case streamgVbcontrol:
				appmsg = s.handleVbcontrol(msg)
			case streamgError:
				appmsg = s.handleError(msg)
			case streamgShutdown:
				appmsg = s.handleShutdown(msg)
				break loop
			}
			if appmsg != nil {
				s.sbch <- appmsg
				log.Printf("appmsg: %+v\n", appmsg)
			}
		case <-after:
		}
		// aggressively restart worker routines.
		workerq := s.workerq
		s.workerq = make([]string, 0, len(workerq))
		for _, raddr := range workerq {
			s.startWorker(raddr)
		}
	}
}

/**** gen-server handlers ****/

// handle new connection, start a go-routine to receive vbmap for this
// connection
func (s *MutationStream) handleNewConnection(msg streamServerMessage) interface{} {
	conn := msg.args[0].(net.Conn)
	worker := make(chan bool)
	if err := s.addConnection(conn, worker); err != nil {
		conn.Close()
		close(worker)
		return s.jumboErrorHandler(msg.raddr, err)
	}
	go doReceiveVbmap(conn, s.reqch, s.timeout)
	return nil
}

// handle payload containing vbucket map for connection, try to start a
// go-routine to read key-versions from upstream.
func (s *MutationStream) handleVbmap(msg streamServerMessage) interface{} {
	vbmap := msg.args[0].(*protobuf.VbConnectionMap)
	vbnos := make([]uint16, 0)
	for _, vbno := range vbmap.GetVbuckets() {
		vbnos = append(vbnos, uint16(vbno))
	}
	if err := s.addVbuckets(msg.raddr, vbnos); err != nil {
		return s.jumboErrorHandler(msg.raddr, err)
	}
	s.startWorker(msg.raddr)
	return nil
}

// handle control messages for vbucket, try to start a go-routine to read
// key-versions from upstream.
func (s *MutationStream) handleVbcontrol(msg streamServerMessage) interface{} {
	var err error
	var appmsg interface{}

	kv := msg.args[0].(*protobuf.KeyVersions)
	vbno := uint16(kv.GetVbucket())
	switch byte(kv.GetCommand()) {
	case common.DropData:
		vbnos := []uint16{uint16(vbno)}
		appmsg = DropVbucketData{vbnos: vbnos}
	case common.StreamBegin:
		err = s.addVbucket(msg.raddr, vbno)
	case common.StreamEnd:
		err = s.delVbucket(msg.raddr, vbno)
	}
	if err != nil {
		return s.jumboErrorHandler(msg.raddr, err)
	}
	s.startWorker(msg.raddr)
	return appmsg
}

// handle error messages for connection, connection is closed and worker
// routine is not restarted.
func (s *MutationStream) handleError(msg streamServerMessage) interface{} {
	return s.jumboErrorHandler(msg.raddr, msg.err)
}

// shutdown this gen server and all its routines.
func (s *MutationStream) handleShutdown(msg streamServerMessage) interface{} {
	respch := msg.args[0].(chan interface{})
	if s.lis == nil {
		log.Printf("repeated call to shutdown for server %q\n", s.laddr)
		return nil
	}
	log.Printf("stream server %q shutting down\n", s.laddr)
	s.lis.Close()        // close listener daemon
	s.closeConnections() // close workers
	// close downstream channels.
	close(s.mutch)
	close(s.sbch)
	s.lis = nil

	close(respch) // unblock the caller
	return nil
}

// start a connection worker to read mutation message for a subset of
// vbuckets.
func (s *MutationStream) startWorker(raddr string) {
	if s.isStaleConnection(raddr) {
		log.Printf("connection %q when stale\n", raddr)
		nc := s.conns[raddr]
		close(nc.worker)
		nc.conn.Close()
		s.delVbuckets(raddr)
	} else if s.isVbucketShiftingConnection(raddr) {
		log.Printf("vbuckets are shifting in connection %q\n", raddr)
		s.workerq = append(s.workerq, raddr)
	} else {
		log.Printf("restarting worker for connection %q\n", raddr)
		nc, _ := s.conns[raddr], true
		channels := []interface{}{s.mutch, s.reqch, nc.worker}
		go doReceiveKeyVersions(nc.conn, channels, s.timeout)
		nc.active = true
	}
}

// connection is considered stale if all vbuckets have migrated out.
func (s *MutationStream) isStaleConnection(raddr string) bool {
	return len(s.connVbs[raddr]) == 0
}

// vbuckets could be migrating from one connection to another.
func (s *MutationStream) isVbucketShiftingConnection(raddr string) bool {
	rbool := false
	for _, vbno := range s.connVbs[raddr] {
		if raddrs, ok := s.vbuckets[vbno]; ok {
			if len(raddrs) == 1 && raddrs[0] == raddr {
				continue
			}
		}
		rbool = true
		break
	}
	return rbool
}

// jumbo size error handler, it either closes all connections and shutdown the
// server or it closes all open connections with faulting remote-host and
// returns back a message for application.
func (s *MutationStream) jumboErrorHandler(raddr string, err error) (msg interface{}) {
	var whatJumbo string

	// connection is already gone.
	if _, ok := s.conns[raddr]; ok == false {
		log.Printf("stream %q says, remote %q already gone\n", s.laddr, raddr)
		return nil
	}

	if err == io.EOF {
		log.Printf("stream %q says, remote %q closed\n", s.laddr, raddr)
		whatJumbo = "closeremote"
	} else if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
		log.Printf("stream %q says, remote %q timedout\n", s.laddr, raddr)
		whatJumbo = "closeremote"
	} else if err != nil {
		log.Printf("stream %q says, error `%v` from %q\n", s.laddr, err, raddr)
		whatJumbo = "closeall"
	} else {
		log.Println("no error, why did you call jumbo !!!\n")
		return
	}

	switch whatJumbo {
	case "closeremote":
		if vbnos, err := s.closeRemoteHost(raddr); err != nil {
			msg = StreamCascadingFault
			go s.Shutdown()
		} else if len(vbnos) > 0 {
			msg = RestartVbuckets{vbnos: vbnos}
		} else {
			msg = nil
		}
	case "closeall":
		msg = err
		go s.Shutdown()
	}
	return
}

// add a new connection to gen-server state
func (s *MutationStream) addConnection(conn net.Conn, worker chan bool) (err error) {
	raddr := conn.RemoteAddr().String()
	log.Printf("connection request from %q\n", raddr)
	if _, _, err = net.SplitHostPort(raddr); err != nil {
		return
	}

	if _, ok := s.conns[raddr]; ok {
		log.Printf("Connection %q already active with %q\n", raddr, s.laddr)
		return StreamUnexpectedError
	}

	s.conns[raddr] = netConn{conn: conn, worker: worker, active: false}
	s.connVbs[raddr] = make([]uint16, 0, GetMaxVbuckets())
	log.Printf("total %v connections active with %s\n", len(s.conns), s.laddr)
	return
}

// close all connections and worker routines
func (s *MutationStream) closeConnections() {
	for raddr, nc := range s.conns {
		close(nc.worker)
		nc.conn.Close()
		if nc.active == false {
			delete(s.conns, raddr)
		}
	}
	// wait till all active workers have exited.
	for len(s.conns) > 0 {
		msg := <-s.reqch
		if msg.err != nil {
			log.Printf("connection %q closed `%v`\n", msg.raddr, msg.err)
			delete(s.conns, msg.raddr)
		}
	}
	s.conns, s.vbuckets, s.connVbs = nil, nil, nil
}

// close all connections with remote host.
func (s *MutationStream) closeRemoteHost(raddr string) ([]uint16, error) {
	vbnos := make([]uint16, 0)

	host, _, _ := net.SplitHostPort(raddr)
	raddrs := s.remoteConnections(host)

	log.Printf("closing connections %q\n", raddrs)
	// close all connections and worker routines for this remote host
	for _, raddr := range raddrs {
		nc := s.conns[raddr]
		close(nc.worker)
		nc.conn.Close()
		vbnos = append(vbnos, s.connVbs[raddr]...)
		if err := s.delVbuckets(raddr); err != nil {
			return nil, err
		}
	}
	return vbnos, nil
}

// add a bunch of vbuckets to remote connection.
func (s *MutationStream) addVbuckets(raddr string, vbnos []uint16) (err error) {
	if _, ok := s.conns[raddr]; ok == false {
		log.Printf("unknown connection %q\n", raddr)
		return StreamUnexpectedError
	}

	for _, vbno := range vbnos {
		if err = s.addVbucket(raddr, vbno); err != nil {
			return
		}
	}
	return
}

// add vbucket to remote connection.
func (s *MutationStream) addVbucket(raddr string, vbno uint16) (err error) {
	if _, ok := s.conns[raddr]; ok == false {
		log.Printf("unknown connection %q\n", raddr)
		return StreamUnexpectedError
	}

	s.vbuckets[vbno] = append(s.vbuckets[vbno], raddr)
	s.connVbs[raddr] = append(s.connVbs[raddr], vbno)
	log.Printf("vbucket %v mapped on connection %q\n", vbno, raddr)
	return
}

// delete vbuckets mapped to a remote connection
func (s *MutationStream) delVbuckets(raddr string) (err error) {
	if _, ok := s.conns[raddr]; ok == false {
		log.Printf("unknown connection %q\n", raddr)
		return StreamUnexpectedError
	}

	log.Printf("delete vbuckets %v for connection %q\n", s.connVbs[raddr], raddr)
	delete(s.conns, raddr)
	for _, vbno := range s.connVbs[raddr] {
		delete(s.vbuckets, vbno)
	}
	delete(s.connVbs, raddr)
	return
}

// delete a vbucket from remote connection.
func (s *MutationStream) delVbucket(raddr string, vbno uint16) (err error) {
	if _, ok := s.conns[raddr]; ok == false {
		log.Printf("unknown connection %q\n", raddr)
		return StreamUnexpectedError
	}

	vbnos := make([]uint16, 0, len(s.connVbs[raddr]))
	for _, vbnox := range s.connVbs[raddr] {
		if vbnox == vbno {
			continue
		}
		vbnos = append(vbnos, vbnox)
	}
	s.connVbs[raddr] = vbnos

	raddrs := make([]string, 0, len(s.vbuckets[vbno]))
	for _, raddrx := range s.vbuckets[vbno] {
		if raddrx == raddr {
			continue
		}
		raddrs = append(raddrs, raddrx)
	}
	s.vbuckets[vbno] = raddrs
	log.Printf("delete vbucket %v from connection %q\n", vbno, raddr)
	return
}

// get all remote connections for `host`
func (s *MutationStream) remoteConnections(host string) []string {
	raddrs := make([]string, 0)
	for raddrx, _ := range s.conns {
		if h, _, _ := net.SplitHostPort(raddrx); h == host {
			raddrs = append(raddrs, raddrx)
		}
	}
	return raddrs
}

// TODO: Remove this ???
func remoteAddrs(m map[string]interface{}) []string {
	raddrs := make([]string, 0)
	for raddr, _ := range m {
		raddrs = append(raddrs, raddr)
	}
	return raddrs
}
