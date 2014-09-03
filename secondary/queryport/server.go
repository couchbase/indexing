package queryport

import (
	"fmt"
	"net"
	"sync"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/protobuf"
	"github.com/couchbase/indexing/secondary/transport"
)

// RequestHandler shall interpret the request message
// from client and post response message(s) on `respch`
// channel, until `quitch` is closed. When there are
// no more response to post handler shall close `respch`.
type RequestHandler func(
	req interface{}, respch chan<- interface{}, quitch <-chan interface{})

// Server handles queryport connections.
type Server struct {
	laddr string         // address to listen
	callb RequestHandler // callback to application on incoming request.
	// local fields
	mu        sync.Mutex
	lis       net.Listener
	killch    chan bool
	pkt       *transport.TransportPacket
	logPrefix string
}

// NewServer creates a new queryport daemon.
func NewServer(laddr string, callb RequestHandler) (s *Server, err error) {
	s = &Server{
		laddr:     laddr,
		callb:     callb,
		killch:    make(chan bool),
		logPrefix: fmt.Sprintf("[Queryport %q]", laddr),
	}
	if s.lis, err = net.Listen("tcp", laddr); err != nil {
		c.Errorf("%v failed starting %v !!", s.logPrefix, err)
		return nil, err
	}

	flags := transport.TransportFlag(0).SetProtobuf()
	s.pkt = transport.NewTransportPacket(c.MaxQueryportPayload, flags)
	s.pkt.SetEncoder(transport.EncodingProtobuf, protobufEncode)
	s.pkt.SetDecoder(transport.EncodingProtobuf, protobufDecode)

	go s.listener()
	c.Infof("%v started ...", s.logPrefix)
	return s, nil
}

// Close queryport daemon.
func (s *Server) Close() (err error) {
	defer func() {
		if r := recover(); r != nil {
			c.Errorf("%v Close fatal panic: %v\n", s.logPrefix, r)
			err = fmt.Errorf("%v", r)
		}
	}()

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lis != nil {
		s.lis.Close() // close listener daemon
		s.lis = nil
		close(s.killch)
		c.Infof("%v ... stopped\n", s.logPrefix)
	}
	return
}

// go-routine to listen for new connections, if this routine goes down -
// server is shutdown and reason notified back to application.
func (s *Server) listener() {
	defer func() {
		if r := recover(); r != nil {
			c.Errorf("%v listener fatal panic: %v", s.logPrefix, r)
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
			break
		}
	}
}

// handle connection request. connection might be kept open in client's
// connection pool.
func (s *Server) handleConnection(conn net.Conn) {
	raddr := conn.RemoteAddr()
	defer func() {
		conn.Close()
		c.Debugf("%v connection %v closed", s.logPrefix, raddr)
	}()

	// start a receive routine.
	rcvch := make(chan interface{}, 16) // TODO: avoid magic number
	go s.doReceive(conn, rcvch)

loop:
	for {
		select {
		case req, ok := <-rcvch:
			if _, yes := req.(*protobuf.EndStreamRequest); yes { // skip
				msg := "%v connection %q skip protobuf.EndStreamRequest"
				c.Debugf(msg, s.logPrefix, raddr)
				break
			} else if !ok {
				break loop
			}
			respch := make(chan interface{}, 16) // TODO: avoid magic number
			quitch := make(chan interface{}, 16) // TODO: avoid magic number
			go s.handleRequest(conn, respch, rcvch, quitch)
			s.callb(req, respch, quitch) // blocking call

		case <-s.killch:
			break loop
		}
	}
}

func (s *Server) handleRequest(
	conn net.Conn, respch, rcvch <-chan interface{}, quitch chan<- interface{}) {
	raddr := conn.RemoteAddr()

	timeoutMs := c.QueryportWriteDeadline * time.Millisecond
	transmit := func(resp interface{}) {
		conn.SetWriteDeadline(time.Now().Add(timeoutMs))
		if err := s.pkt.Send(conn, resp); err != nil {
			msg := "%v connection %v response transport failed `%v`\n"
			c.Debugf(msg, s.logPrefix, raddr, err)
		}
	}

	defer close(quitch)

loop:
	for { // response loop to stream query results back to client
		select {
		case resp, ok := <-respch:
			if !ok {
				transmit(&protobuf.StreamEndResponse{})
				msg := "%v connection %q transmitted protobuf.StreamEndResponse"
				c.Debugf(msg, s.logPrefix, raddr)
				break loop
			}
			transmit(resp)

		case req, ok := <-rcvch:
			if _, yes := req.(*protobuf.EndStreamRequest); ok && yes {
				transmit(&protobuf.StreamEndResponse{})
				msg := "%v connection %q transmitted protobuf.StreamEndResponse"
				c.Debugf(msg, s.logPrefix, raddr)
				break loop
			}

		case <-s.killch:
			break loop // close connection
		}
	}
}

// receive requests from remote, when this function returns
// the connection is expected to be closed.
func (s *Server) doReceive(conn net.Conn, rcvch chan<- interface{}) {
	raddr := conn.RemoteAddr()

	flags := transport.TransportFlag(0).SetProtobuf()
	pkt := transport.NewTransportPacket(c.MaxQueryportPayload, flags)
	pkt.SetDecoder(transport.EncodingProtobuf, protobufDecode)

	c.Debugf("%v connection %q doReceive() ...\n", s.logPrefix, raddr)

	timeoutMs := c.QueryportReadDeadline * time.Millisecond
loop:
	for {
		conn.SetReadDeadline(time.Now().Add(timeoutMs))
		req, err := pkt.Receive(conn)
		// TODO: handle close-connection and don't print error message.
		if err != nil {
			c.Errorf("%v connection %q exited %v\n", s.logPrefix, raddr, err)
			break loop
		}
		select {
		case rcvch <- req:
		case <-s.killch:
			break loop
		}
	}
	close(rcvch)
}
