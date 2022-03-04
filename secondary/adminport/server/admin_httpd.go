// Example server {
//      config := c.SystemConfig.SectionConfig("projector.adminport.")
//      config.Set("name", "projector").Set("listenAddr", "localhost:9999")
//      reqch := make(chan adminport.Request)
//      server := adminport.NewHTTPServer(config, reqch)
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

package server

import (
	"encoding/json"
	"expvar"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"sync"
	"time"

	"github.com/couchbase/indexing/secondary/audit"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/security"

	c "github.com/couchbase/indexing/secondary/common"

	apcommon "github.com/couchbase/indexing/secondary/adminport/common"
)

// httpServer is a concrete type implementing adminport Server
// interface.
type httpServer struct {
	mu       sync.Mutex   // handle concurrent updates to this object
	lis      net.Listener // TCP listener
	mux      *http.ServeMux
	srv      *http.Server // http server
	messages map[string]apcommon.MessageMarshaller
	conns    map[string]net.Conn
	reqch    chan<- apcommon.Request // request channel back to application

	// config params
	name      string // human readable name for this server
	laddr     string // address to bind and listen
	urlPrefix string // URL path prefix for adminport
	rtimeout  time.Duration
	wtimeout  time.Duration
	rhtimeout time.Duration
	maxHdrlen int

	// local
	logPrefix     string
	statsInBytes  uint64
	statsOutBytes uint64
	statsMessages map[string][3]uint64 // msgname -> [3]uint64{in,out,err}
}

// NewHTTPServer creates an instance of admin-server.
// Start() will actually start the server.
func NewHTTPServer(config c.Config, reqch chan<- apcommon.Request) (apcommon.Server, error) {
	s := &httpServer{
		messages:      make(map[string]apcommon.MessageMarshaller),
		conns:         make(map[string]net.Conn),
		reqch:         reqch,
		statsInBytes:  0.0,
		statsOutBytes: 0.0,
		statsMessages: make(map[string][3]uint64),

		name:      config["name"].String(),
		laddr:     config["listenAddr"].String(),
		urlPrefix: config["urlPrefix"].String(),
		rtimeout:  time.Duration(config["readTimeout"].Int()),
		wtimeout:  time.Duration(config["writeTimeout"].Int()),
		rhtimeout: time.Duration(config["readHeaderTimeout"].Int()),

		maxHdrlen: config["maxHeaderBytes"].Int(),
	}
	s.logPrefix = fmt.Sprintf("%s[%s]", s.name, s.laddr)

	s.mux = http.NewServeMux()
	s.mux.HandleFunc(s.urlPrefix, s.systemHandler)
	s.mux.HandleFunc("/debug/vars", s.expvarHandler)
	s.srv = &http.Server{
		Addr:              s.laddr,
		Handler:           s.mux,
		ConnState:         s.connState,
		ReadTimeout:       s.rtimeout * time.Millisecond,
		WriteTimeout:      s.wtimeout * time.Millisecond,
		ReadHeaderTimeout: s.rhtimeout * time.Millisecond,
		MaxHeaderBytes:    s.maxHdrlen,
	}

	if err := security.SecureServer(s.srv); err != nil {
		return nil, err
	}

	return s, nil
}

func validateAuth(w http.ResponseWriter, r *http.Request) bool {
	_, valid, err := c.IsAuthValid(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error() + "\n"))
	} else if valid == false {
		audit.Audit(c.AUDIT_UNAUTHORIZED, r, "admin_httpd::validateAuth", "")
		w.WriteHeader(http.StatusUnauthorized)
		w.Write(c.HTTP_STATUS_UNAUTHORIZED)
	}
	return valid
}

// Register is part of Server interface.
func (s *httpServer) Register(msg apcommon.MessageMarshaller) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.lis != nil {
		logging.Errorf("%v can't register, server already started\n", s.logPrefix)
		return apcommon.ErrorRegisteringRequest
	}
	key := fmt.Sprintf("%v%v", s.urlPrefix, msg.Name())
	s.messages[key] = msg
	s.statsMessages[key] = [3]uint64{0, 0, 0}
	logging.Infof("%s registered %s\n", s.logPrefix, s.getURL(msg))
	return
}

// RegisterHandler is part of Server interface.
func (s *httpServer) RegisterHTTPHandler(
	path string, handler interface{}) (err error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.lis != nil {
		logging.Errorf("%v can't register, server already started\n", s.logPrefix)
		return apcommon.ErrorRegisteringRequest
	}

	switch handl := handler.(type) {
	case http.Handler:
		s.mux.Handle(path, handl)
		logging.Infof("%s registered handler-obj %s\n", s.logPrefix, path)

	case func(http.ResponseWriter, *http.Request):
		s.mux.HandleFunc(path, handl)
		logging.Infof("%s registered handler-func %s\n", s.logPrefix, path)
	}
	return
}

// Unregister is part of Server interface.
func (s *httpServer) Unregister(msg apcommon.MessageMarshaller) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.lis != nil {
		logging.Errorf("%v can't unregister, server already started\n", s.logPrefix)
		return apcommon.ErrorRegisteringRequest
	}
	name := msg.Name()
	if _, ok := s.messages[name]; !ok {
		logging.Errorf("%v message %q hasn't been registered\n", s.logPrefix, name)
		return apcommon.ErrorMessageUnknown
	}
	delete(s.messages, name)
	logging.Infof("%s unregistered %s\n", s.logPrefix, s.getURL(msg))
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
		logging.Errorf("%v already started ...\n", s.logPrefix)
		return apcommon.ErrorServerStarted
	}

	if s.lis, err = security.MakeListener(s.srv.Addr); err != nil {
		logging.Fatalf("%v Unable to start server, LISTEN FAILED %v\n", s.logPrefix, err)
		return err
	}

	// Server routine
	go func() {
		defer s.shutdown()

		logging.Infof("%s starting ...\n", s.logPrefix)
		err := s.srv.Serve(s.lis) // serve until listener is closed.
		// TODO: look into error message and skip logging if Stop().
		if err != nil {
			logging.Errorf("%s %v\n", s.logPrefix, err)
		}
	}()

	logging.PeriodicProfile(logging.Debug, s.srv.Addr, "goroutine")
	return
}

// Stop is part of Server interface. Returns only after all
// outstanding requests are serviced.
func (s *httpServer) Stop() {
	s.shutdown()
	logging.Infof("%s ... stopped\n", s.logPrefix)
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

	logging.Infof("%s Request %q\n", s.logPrefix, r.URL.Path)

	s.mu.Lock()
	stats := s.statsMessages[r.URL.Path]
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if recov := recover(); recov != nil {
			logging.Errorf("%s systemHandler() crashed: %v\n", s.logPrefix, recov)
			logging.Errorf("%s", logging.StackTrace())
			stats[2]++ // error count
		}
		if err != nil {
			logging.Errorf("%s %v\n", s.logPrefix, err)
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
		err = apcommon.ErrorPathNotFound
		http.Error(w, "path not found", http.StatusNotFound)
		return
	}
	// read request
	dataIn = make([]byte, r.ContentLength)
	if err := requestRead(r.Body, dataIn); err != nil {
		err = fmt.Errorf("%v, %v", apcommon.ErrorRequest, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Get an instance of request type and decode request into that.
	typeOfMsg := reflect.ValueOf(msg).Elem().Type()
	msg = reflect.New(typeOfMsg).Interface().(apcommon.MessageMarshaller)
	if err = msg.Decode(dataIn); err != nil {
		err = fmt.Errorf("%v, %v", apcommon.ErrorDecodeRequest, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	waitch := make(chan interface{}, 1)
	// send and wait
	s.reqch <- &httpAdminRequest{srv: s, msg: msg, waitch: waitch}
	val := <-waitch

	switch v := (val).(type) {
	case apcommon.MessageMarshaller:
		if dataOut, err = v.Encode(); err == nil {
			header := w.Header()
			header["Content-Type"] = []string{v.ContentType()}
			w.Write(dataOut)

		} else {
			err = fmt.Errorf("%v, %v", apcommon.ErrorEncodeResponse, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

	case error:
		err = fmt.Errorf("%v, %v", apcommon.ErrorInternal, v)
		http.Error(w, v.Error(), http.StatusInternalServerError)
	}
}

// handle expvar request.
func (s *httpServer) expvarHandler(w http.ResponseWriter, r *http.Request) {
	valid := validateAuth(w, r)
	if !valid {
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	fmt.Fprintf(w, "{\n")
	first := true
	expvar.Do(func(kv expvar.KeyValue) {
		if !first {
			fmt.Fprintf(w, ",\n")
		}
		first = false
		data, err := json.Marshal(kv.Value)
		if err != nil {
			logging.Errorf("%v encoding statistics: %v\n", s.logPrefix, err)
		}
		fmt.Fprintf(w, "%q: %s", kv.Key, data)
	})
	fmt.Fprintf(w, "\n}\n")
}

func (s *httpServer) connState(conn net.Conn, state http.ConnState) {
	raddr := conn.RemoteAddr()
	logging.Tracef("%s connState for %q : %v\n", s.logPrefix, raddr, state)

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.lis != nil && state == http.StateNew {
		s.conns[raddr.String()] = conn
	}
	if state == http.StateClosed {
		delete(s.conns, raddr.String())
	}
}

func (s *httpServer) getURL(msg apcommon.MessageMarshaller) string {
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
	msg    apcommon.MessageMarshaller
	waitch chan interface{}
}

// GetMessage is part of Request interface.
func (r *httpAdminRequest) GetMessage() apcommon.MessageMarshaller {
	return r.msg
}

// Send is part of Request interface.
func (r *httpAdminRequest) Send(msg apcommon.MessageMarshaller) error {
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
