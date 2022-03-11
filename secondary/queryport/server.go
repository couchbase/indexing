package queryport

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/security"

	c "github.com/couchbase/indexing/secondary/common"

	"github.com/couchbase/cbauth"

	protobuf "github.com/couchbase/indexing/secondary/protobuf/query"
	"github.com/couchbase/indexing/secondary/transport"
)

// RequestHandler shall interpret the request message
// from client and post response message(s) on `respch`
// channel, until `quitch` is closed. When there are
// no more response to post handler shall close `respch`.
type RequestHandler func(
	req interface{}, ctx interface{}, conn net.Conn, quitch <-chan bool)

type ConnectionHandler func() interface{}

type request struct {
	r      interface{}
	quitch chan bool
}

var Ping *request = &request{}

func newRequest(r interface{}) (req request) {
	req.r = r
	req.quitch = make(chan bool)
	return
}

// Server handles queryport connections.
type Server struct {
	laddr string         // address to listen
	callb RequestHandler // callback to application on incoming request.
	conb  ConnectionHandler
	// local fields
	mu  sync.Mutex
	lis net.Listener
	// config params
	maxPayload        int
	readDeadline      time.Duration
	writeDeadline     time.Duration
	keepAliveInterval time.Duration
	streamChanSize    int
	logPrefix         string
	nConnections      int64

	conns map[string]net.Conn
}

type ServerStats struct {
	Connections int64
}

// NewServer creates a new queryport daemon.
func NewServer(
	cluster, laddr string, callb RequestHandler, conb ConnectionHandler,
	config c.Config) (s *Server, err error) {

	s = &Server{
		laddr:          laddr,
		callb:          callb,
		conb:           conb,
		maxPayload:     config["maxPayload"].Int(),
		readDeadline:   time.Duration(config["readDeadline"].Int()),
		writeDeadline:  time.Duration(config["writeDeadline"].Int()),
		streamChanSize: config["streamChanSize"].Int(),
		logPrefix:      fmt.Sprintf("[Queryport %q]", laddr),
		nConnections:   0,
		conns:          make(map[string]net.Conn),
	}
	keepAliveInterval := config["keepAliveInterval"].Int()
	s.keepAliveInterval = time.Duration(keepAliveInterval) * time.Second
	if s.lis, err = security.MakeListener(laddr); err != nil {
		logging.Errorf("%v failed starting %v !!\n", s.logPrefix, err)
		return nil, err
	}

	go s.listener()
	logging.Infof("%v started ...\n", s.logPrefix)
	return s, nil
}

func (s *Server) Statistics() ServerStats {
	return ServerStats{
		Connections: atomic.LoadInt64(&s.nConnections),
	}
}

// Close queryport daemon.
// Close method only shuts down lister sockets
// Established connections will be only closed once client
// closes those connections.
func (s *Server) Close() (err error) {
	defer func() {
		if r := recover(); r != nil {
			logging.Errorf("%v Close() crashed: %v\n", s.logPrefix, r)
			err = fmt.Errorf("%v", r)
			logging.Errorf("%s", logging.StackTrace())
		}
	}()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.lis != nil {
		s.lis.Close() // close listener daemon
		s.lis = nil
		logging.Infof("%v ... stopped\n", s.logPrefix)
	}
	return
}

func (s *Server) ResetConnections() error {

	// close listener
	s.Close()

	// close all existing connections
	func() {
		s.mu.Lock()
		defer s.mu.Unlock()

		for _, conn := range s.conns {
			s.deregisterConnNoLock(conn)
			conn.Close()
		}
	}()

	// Existing connection will continue to run.  They need to
	// be closed by the clients by flushing their connection pools.
	logging.Infof("%v ... restarting listener\n", s.logPrefix)

	fn := func(r int, e error) error {
		s.mu.Lock()
		defer s.mu.Unlock()

		// Restart listener.
		var err error
		if s.lis, err = security.MakeListener(s.laddr); err != nil {
			logging.Errorf("%v failed starting listener %v %v !!\n", s.logPrefix, s.laddr, err)
			return err
		}
		go s.listener()

		return nil
	}
	helper := c.NewRetryHelper(10, time.Second, 1, fn)
	if err := helper.Run(); err != nil {
		return err
	}

	return nil
}

func (s *Server) registerConn(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.conns[conn.RemoteAddr().String()] = conn
}

func (s *Server) deregisterConn(conn net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deregisterConnNoLock(conn)
}

func (s *Server) deregisterConnNoLock(conn net.Conn) bool {
	_, ok := s.conns[conn.RemoteAddr().String()]
	if ok {
		delete(s.conns, conn.RemoteAddr().String())
	}
	return ok
}

// go-routine to listen for new connections, if this routine goes down -
// listener is restarted
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

		if s.lis != nil && s.lis == lis { // if s.lis == nil, then Server.Close() was called
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
		if conn, err := lis.Accept(); err == nil {
			go s.handleConnection(conn)
		} else {
			e, ok := err.(*net.OpError)
			if ok && (e.Err == syscall.EMFILE || e.Err == syscall.ENFILE) {
				logging.Errorf("%v Accept() Error: %v. Retrying Accept.\n", s.logPrefix, err)
				continue
			}
			logging.Errorf("%v Accept() Error: %v\n", s.logPrefix, err)
			break //After loop breaks, selfRestart() is called in defer
		}
	}
}

func (s *Server) enforceAuth(raddr string) bool {
	// When cluster is getting upgraded, client may lag behind in
	// receiving cluster upgrade notification as compared to the server.
	// In such a case, server will reject request due to AUTH_MISSING and
	// client should retry with a new connection. Server will close this
	// connection after responding to the client.
	//
	// Client is capable of understanding and handling AUTH_MISSING
	// response code.

	clustVer := c.GetClusterVersion()
	intVer := c.GetInternalVersion()

	if clustVer >= c.INDEXER_71_VERSION {
		return true
	}

	if intVer.Equals(c.MIN_VER_SRV_AUTH) || intVer.GreaterThan(c.MIN_VER_SRV_AUTH) {
		return true
	}

	logging.Infof("%v connection %q continue without auth %v:%v", s.logPrefix, raddr, clustVer, intVer)

	return false
}

func (s *Server) doAuth(conn net.Conn) (interface{}, error) {

	// TODO: Some code deduplication with doReveive can be done.
	raddr := conn.RemoteAddr().String()

	// transport buffer for receiving
	flags := transport.TransportFlag(0).SetProtobuf()
	rpkt := transport.NewTransportPacket(s.maxPayload, flags)
	rpkt.SetEncoder(transport.EncodingProtobuf, protobuf.ProtobufEncode)
	rpkt.SetDecoder(transport.EncodingProtobuf, protobuf.ProtobufDecode)

	logging.Infof("%v connection %q doAuth() ...", s.logPrefix, raddr)

	// Set read deadline for auth to 10 Seconds.
	if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		logging.Warnf("%v doAuth %q error %v in SetReadDeadline", s.logPrefix, raddr, err)
	}

	reqMsg, err := rpkt.Receive(conn)
	if err != nil {
		return nil, err
	}

	// Reset read deadline
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		logging.Warnf("%v doAuth %q error %v in SetReadDeadline during reset", s.logPrefix, raddr, err)
	}

	var authErr error
	var code uint32

	req, ok := reqMsg.(*protobuf.AuthRequest)
	if !ok {
		logging.Infof("%v connection %q doAuth() authentication is missing", s.logPrefix, raddr)

		if !s.enforceAuth(raddr) {
			return reqMsg, nil
		}

		code = transport.AUTH_MISSING
		authErr = c.ErrAuthMissing
	} else {
		// The upgraded server always responds to the AuthRequest.

		_, err = cbauth.Auth(req.GetUser(), req.GetPass())
		if err != nil {
			logging.Errorf("%v connection %q doAuth() error %v", s.logPrefix, raddr, err)
			code = transport.AUTH_FAILURE
			authErr = errors.New("Unauthenticated access. Authentication failure.")
		}

		// TODO: RBAC

		code = transport.AUTH_SUCCESS
	}

	resp := &protobuf.AuthResponse{
		Code: &code,
	}

	err = rpkt.Send(conn, resp)
	if err != nil {
		return nil, err
	}

	if authErr == nil {
		logging.Verbosef("%v connection %q auth successful", s.logPrefix, raddr)
	}

	return nil, authErr
}

// handle connection request. connection might be kept open in client's
// connection pool.
func (s *Server) handleConnection(conn net.Conn) {

	req, err := s.doAuth(conn)
	if err != nil {
		// On authentication error, just close the connection. Client
		// will try with a new connection by sending AuthRequest.
		logging.Errorf("%v Authentication failed for conn %v with error %v", s.logPrefix, conn.RemoteAddr(), err)
		conn.Close()
		return
	}

	s.registerConn(conn)

	atomic.AddInt64(&s.nConnections, 1)
	defer func() {
		atomic.AddInt64(&s.nConnections, -1)
	}()

	raddr := conn.RemoteAddr()
	defer func() {
		if s.deregisterConn(conn) {
			conn.Close()
		}
		logging.Infof("%v connection %v closed\n", s.logPrefix, raddr)
	}()

	// Set keep alive interval.
	if tcpconn, ok := conn.(*net.TCPConn); ok {
		tcpconn.SetKeepAlive(true)
		tcpconn.SetKeepAlivePeriod(s.keepAliveInterval)
	}

	// start a receive routine.
	killch := make(chan bool)
	rcvch := make(chan request, s.streamChanSize)

	go s.doReceive(conn, rcvch, killch, req)
	go s.doPing(rcvch, killch)

	var ctx interface{}
	if s.conb != nil {
		ctx = s.conb()
	}

	for req := range rcvch {
		s.callb(req.r, ctx, conn, req.quitch) // blocking call
		if req.r != Ping {
			transport.SendResponseEnd(conn)
		}
	}
}

// receive requests from remote, when this function returns
// the connection is expected to be closed.
func (s *Server) doReceive(conn net.Conn, rcvch chan<- request, killch chan bool, req interface{}) {
	raddr := conn.RemoteAddr()

	// transport buffer for receiving
	flags := transport.TransportFlag(0).SetProtobuf()
	rpkt := transport.NewTransportPacket(s.maxPayload, flags)
	rpkt.SetDecoder(transport.EncodingProtobuf, protobuf.ProtobufDecode)

	logging.Infof("%v connection %q doReceive() ...\n", s.logPrefix, raddr)

	var currRequest request

loop:
	for {
		// TODO: Fix read timeout correctly
		// timeoutMs := s.readDeadline * time.Millisecond
		// conn.SetReadDeadline(time.Now().Add(timeoutMs))

		var reqMsg interface{}
		var err error

		if req != nil {

			reqMsg = req
			req = nil

		} else {

			reqMsg, err = rpkt.Receive(conn)
			// TODO: handle close-connection and don't print error message.
			if err != nil {
				if err == io.EOF {
					logging.Tracef("%v connection %q exited %v\n", s.logPrefix, raddr, err)
				} else {
					logging.Errorf("%v connection %q exited %v\n", s.logPrefix, raddr, err)
				}

				// With this, connection close or error becomes ClientCancelErr.  But this
				// will ensure request will terminate if it is waiting for snapshot or
				// reader context.  This allows connection to be closed on the server side
				// without being blocked.
				if currRequest.quitch != nil {
					close(currRequest.quitch)
				}
				break loop
			}
		}

		// This message indicates graceful shutdown of a prior request.
		if _, yes := reqMsg.(*protobuf.EndStreamRequest); yes {
			format := "%v connection %s client requested quit"
			logging.Debugf(format, s.logPrefix, raddr)
			close(currRequest.quitch)

			// reset currRequest such that subsequent connection close will not try to
			// close the quitch channel twice.
			currRequest.quitch = nil
		} else {
			// Each queryport connection can only handle one client request at a time. Client must ensure that:
			// 1) If there is network error (e.g. timeout), connection is not going to be reused.
			// 2) If client cancel request, EndStreamRequest must be sent.
			// 3) Connection is not reused until current client request is successfully finished or canceled.
			currRequest = newRequest(reqMsg)
			rcvch <- currRequest
		}
	}

	close(killch)
}

func (s *Server) doPing(rcvch chan request, killch chan bool) {

	ticker := time.NewTicker(time.Minute * time.Duration(5))
	defer ticker.Stop()

loop2:
	for {
		select {
		case <-ticker.C:
			rcvch <- newRequest(Ping)
		case <-killch:
			break loop2
		}
	}

	// empty the receive channel
loop3:
	for {
		select {
		case <-rcvch:
		default:
			break loop3
		}
	}

	close(rcvch)
}
