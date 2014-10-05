// Example server {
//      name := "projector"
//      addr := "localhost:9999"
//      urlPrefix := "/adminport"
//      reqch := make(chan adminport.Request)
//      server := adminport.NewHTTPServer(name, addr, urlPrefix, reqch)
//
//      server.Register(&protobuf.RequestMessage{})
//      server.Start()
//
//      loop:
//      for {
//          select {
//          case req, ok := <-reqch:
//              if ok {
//                  msg := req.GetMessage()
//                  // interpret request and compose a response
//                  respMsg := &protobuf.ResponseMessage{}
//                  err := msg.Send(respMsg)
//              } else {
//                  break loop
//              }
//          }
//      }
//      server.Stop()
// }

package adminport

import "fmt"
import "runtime/debug"
import "io"
import "net"
import "net/http"
import "reflect"
import "sync"
import "time"

import c "github.com/couchbase/indexing/secondary/common"

// httpServer is a concrete type implementing adminport Server
// interface.
type httpServer struct {
	mu        sync.Mutex   // handle concurrent updates to this object
	lis       net.Listener // TCP listener
	srv       *http.Server // http server
	urlPrefix string       // URL path prefix for adminport
	messages  map[string]MessageMarshaller
	conns     []net.Conn
	reqch     chan<- Request // request channel back to application

	logPrefix     string
	statsInBytes  uint64
	statsOutBytes uint64
	statsMessages map[string][3]uint64 // msgname -> [3]uint64{in,out,err}
}

// NewHTTPServer creates an instance of admin-server.
// Start() will actually start the server.
func NewHTTPServer(name, addr, urlPrefix string, reqch chan<- Request) Server {
	s := &httpServer{
		urlPrefix:     urlPrefix,
		messages:      make(map[string]MessageMarshaller),
		conns:         make([]net.Conn, 0),
		reqch:         reqch,
		logPrefix:     fmt.Sprintf("[%s:%s]", name, addr),
		statsInBytes:  0.0,
		statsOutBytes: 0.0,
		statsMessages: make(map[string][3]uint64),
	}
	mux := http.NewServeMux()
	mux.HandleFunc(s.urlPrefix, s.systemHandler)
	s.srv = &http.Server{
		Addr:      addr,
		Handler:   mux,
		ConnState: s.connState,
		// TODO: supply this configuration via constrcutor.
		ReadTimeout:    c.AdminportReadTimeout * time.Millisecond,
		WriteTimeout:   c.AdminportWriteTimeout * time.Millisecond,
		MaxHeaderBytes: 1 << 20, // 1 MegaByte
	}
	return s
}

// Register is part of Server interface.
func (s *httpServer) Register(msg MessageMarshaller) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.lis != nil {
		c.Errorf("%v can't register, server already started\n", s.logPrefix)
		return ErrorRegisteringRequest
	}
	key := fmt.Sprintf("%v%v", s.urlPrefix, msg.Name())
	s.messages[key] = msg
	s.statsMessages[key] = [3]uint64{0, 0, 0}
	c.Infof("%s registered %s\n", s.logPrefix, s.getURL(msg))
	return
}

// Unregister is part of Server interface.
func (s *httpServer) Unregister(msg MessageMarshaller) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.lis != nil {
		c.Errorf("%v can't unregister, server already started\n", s.logPrefix)
		return ErrorRegisteringRequest
	}
	name := msg.Name()
	if _, ok := s.messages[name]; !ok {
		c.Errorf("%v message %q hasn't been registered\n", s.logPrefix, name)
		return ErrorMessageUnknown
	}
	delete(s.messages, name)
	c.Infof("%s unregistered %s\n", s.logPrefix, s.getURL(msg))
	return
}

// GetStatistics for adminport daemon
func (s *httpServer) GetStatistics() c.Statistics {
	s.mu.Lock()
	defer s.mu.Unlock()

	m := map[string]interface{}{
		"urlPrefix": s.urlPrefix,
		"payload":   [2]uint64{s.statsInBytes, s.statsOutBytes},
	}
	for name, ns := range s.statsMessages {
		m[name] = [3]uint64{ns[0] /*in*/, ns[1] /*out*/, ns[2] /*err*/}
	}
	stats, _ := c.NewStatistics(m)
	return stats
}

// Start is part of Server interface.
func (s *httpServer) Start() (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.lis != nil {
		c.Errorf("%v already started ...\n", s.logPrefix)
		return ErrorServerStarted
	}

	if s.lis, err = net.Listen("tcp", s.srv.Addr); err != nil {
		c.Errorf("%v listen failed %v\n", s.logPrefix, err)
		return err
	}

	// Server routine
	go func() {
		defer s.shutdown()

		c.Infof("%s starting ...\n", s.logPrefix)
		err := s.srv.Serve(s.lis) // serve until listener is closed.
		// TODO: look into error message and skip logging if Stop().
		if err != nil {
			c.Errorf("%s %v\n", s.logPrefix, err)
		}
	}()
	return
}

// Stop is part of Server interface. Returns only after all
// outstanding requests are serviced.
func (s *httpServer) Stop() {
	s.shutdown()
	c.Infof("%s ... stopped\n", s.logPrefix)
}

func (s *httpServer) shutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.lis != nil {
		s.lis.Close()
		for _, conn := range s.conns {
			conn.Close()
		}
		close(s.reqch)
		s.lis = nil
	}
}

// handle incoming request.
func (s *httpServer) systemHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	var dataIn, dataOut []byte

	c.Infof("%s Request %q\n", s.logPrefix, r.URL.Path)

	stats := s.statsMessages[r.URL.Path]

	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if recov := recover(); recov != nil {
			c.Errorf("%s systemHandler() crashed: %v\n", s.logPrefix, recov)
			c.StackTrace(string(debug.Stack()))
			stats[2]++ // error count
		}
		if err != nil {
			c.Errorf("%s %v\n", s.logPrefix, err)
			stats[2]++ // error count
		}
		stats[1]++ // response count
		if dataIn != nil {
			s.statsInBytes += uint64(len(dataIn))
		}
		if dataOut != nil {
			s.statsOutBytes += uint64(len(dataOut))
		}
		s.statsMessages[r.URL.Path] = stats
	}()

	s.mu.Lock()
	stats[0]++ // request count
	s.mu.Unlock()

	// get request message type.
	msg, ok := s.messages[r.URL.Path]
	if !ok {
		err = ErrorPathNotFound
		http.Error(w, "path not found", http.StatusNotFound)
		return
	}
	// read request
	dataIn = make([]byte, r.ContentLength)
	if err := requestRead(r.Body, dataIn); err != nil {
		err = fmt.Errorf("%v, %v", ErrorRequest, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Get an instance of request type and decode request into that.
	typeOfMsg := reflect.ValueOf(msg).Elem().Type()
	msg = reflect.New(typeOfMsg).Interface().(MessageMarshaller)
	if err = msg.Decode(dataIn); err != nil {
		err = fmt.Errorf("%v, %v", ErrorDecodeRequest, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	waitch := make(chan interface{}, 1)
	// send and wait
	s.reqch <- &httpAdminRequest{srv: s, msg: msg, waitch: waitch}
	val := <-waitch

	switch v := (val).(type) {
	case MessageMarshaller:
		if dataOut, err = v.Encode(); err == nil {
			header := w.Header()
			header["Content-Type"] = []string{v.ContentType()}
			w.Write(dataOut)

		} else {
			err = fmt.Errorf("%v, %v", ErrorEncodeResponse, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

	case error:
		err = fmt.Errorf("%v, %v", ErrorInternal, v)
		http.Error(w, v.Error(), http.StatusInternalServerError)
	}
}

func (s *httpServer) connState(conn net.Conn, state http.ConnState) {
	raddr := conn.RemoteAddr()
	c.Debugf("%s connState for %q : %v\n", s.logPrefix, raddr, state)

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.lis != nil && state == http.StateNew {
		s.conns = append(s.conns, conn)
	}
}

func (s *httpServer) getURL(msg MessageMarshaller) string {
	return s.urlPrefix + msg.Name()
}

func requestRead(r io.Reader, data []byte) (err error) {
	var c int

	n, start := len(data), 0
	for n > 0 && err == nil {
		// Per http://golang.org/pkg/io/#Reader, it is valid for Read to
		// return EOF with non-zero number of bytes at the end of the
		// input stream
		c, err = r.Read(data[start:])
		n -= c
		start += c
	}
	if n == 0 {
		return nil
	}
	return err
}

// concrete type implementing Request interface
type httpAdminRequest struct {
	srv    *httpServer
	msg    MessageMarshaller
	waitch chan interface{}
}

// GetMessage is part of Request interface.
func (r *httpAdminRequest) GetMessage() MessageMarshaller {
	return r.msg
}

// Send is part of Request interface.
func (r *httpAdminRequest) Send(msg MessageMarshaller) error {
	r.waitch <- msg
	close(r.waitch)
	return nil
}

// SendError is part of Request interface.
func (r *httpAdminRequest) SendError(err error) error {
	r.waitch <- err
	close(r.waitch)
	return nil
}
