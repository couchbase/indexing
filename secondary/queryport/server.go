package queryport

import (
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/indexing/secondary/logging"

	c "github.com/couchbase/indexing/secondary/common"

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
}

type ServerStats struct {
	Connections int64
}

// NewServer creates a new queryport daemon.
func NewServer(
	laddr string, callb RequestHandler, conb ConnectionHandler,
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
	}
	keepAliveInterval := config["keepAliveInterval"].Int()
	s.keepAliveInterval = time.Duration(keepAliveInterval) * time.Second
	if s.lis, err = net.Listen("tcp", laddr); err != nil {
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

// go-routine to listen for new connections, if this routine goes down -
// server is shutdown and reason notified back to application.
func (s *Server) listener() {
	defer func() {
		if r := recover(); r != nil {
			logging.Errorf("%v listener() crashed: %v\n", s.logPrefix, r)
			logging.Errorf("%s", logging.StackTrace())
		}
		go s.Close()
	}()

	for {
		if conn, err := s.lis.Accept(); err == nil {
			go s.handleConnection(conn)
		} else {
			if e, ok := err.(*net.OpError); ok && e.Op != "accept" {
				panic(err)
			}
			logging.Errorf("%v Accept(): %v\n", s.logPrefix, err)
			break
		}
	}
}

// handle connection request. connection might be kept open in client's
// connection pool.
func (s *Server) handleConnection(conn net.Conn) {
	atomic.AddInt64(&s.nConnections, 1)
	defer func() {
		atomic.AddInt64(&s.nConnections, -1)
	}()

	raddr := conn.RemoteAddr()
	defer func() {
		conn.Close()
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

	go s.doReceive(conn, rcvch, killch)
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
func (s *Server) doReceive(conn net.Conn, rcvch chan<- request, killch chan bool) {
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

		reqMsg, err := rpkt.Receive(conn)
		// TODO: handle close-connection and don't print error message.
		if err != nil {
			if err == io.EOF {
				logging.Tracef("%v connection %q exited %v\n", s.logPrefix, raddr, err)
			} else {
				logging.Errorf("%v connection %q exited %v\n", s.logPrefix, raddr, err)
			}
			break loop
		}

		// This message indicates graceful shutdown of a prior request.
		if _, yes := reqMsg.(*protobuf.EndStreamRequest); yes {
			format := "%v connection %s client requested quit"
			logging.Debugf(format, s.logPrefix, raddr)
			close(currRequest.quitch)
		} else {
			currRequest = newRequest(reqMsg)
			rcvch <- currRequest
		}
	}

	close(killch)
}

func (s *Server) doPing(rcvch chan<- request, killch chan bool) {

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

	close(rcvch)
}
