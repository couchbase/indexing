// admin server to handle admin and system messages.
//
// Example server {
//      reqch  := make(chan adminport.Request)
//      server := adminport.NewHTTPServer("projector", "localhost:9999", "/adminport", reqch)
//      server.Register(&protobuf.RequestMessage{})
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
// }

// TODO: IMPORTANT:
//  Go 1.3 is supposed to have graceful shutdown of http server.
//  Refer https://code.google.com/p/go/issues/detail?id=4674

package adminport

import (
	"fmt"
	c "github.com/couchbase/indexing/secondary/common"
	"log"
	"net"
	"net/http"
	"reflect"
	"sync"
	"time"
)

// httpServer is a concrete type implementing adminport Server interface.
type httpServer struct {
	mu        sync.Mutex   // handle concurrent updates to this object
	module    string       // module hosting the adminport daemon
	lis       net.Listener // TCP listener
	srv       *http.Server // http server
	urlPrefix string       // URL path prefix for adminport
	messages  map[string]MessageMarshaller
	reqch     chan<- Request // request channel back to application

	logPrefix string
}

// NewHTTPServer creates an instance of admin-server. Start() will actually
// start the server.
func NewHTTPServer(module, connAddr, urlPrefix string, reqch chan<- Request) Server {
	s := &httpServer{
		module:    module,
		reqch:     reqch,
		messages:  make(map[string]MessageMarshaller),
		urlPrefix: urlPrefix,
		logPrefix: fmt.Sprintf("%s's adminport(%s)", module, connAddr),
	}

	mux := http.NewServeMux()
	mux.HandleFunc(s.urlPrefix, s.systemHandler)
	s.srv = &http.Server{
		Addr:           connAddr,
		Handler:        mux,
		ReadTimeout:    c.AdminportReadTimeout * time.Millisecond,
		WriteTimeout:   c.AdminportWriteTimeout * time.Millisecond,
		MaxHeaderBytes: 1 << 20,
	}
	return s
}

// Register is part of Server interface.
func (s *httpServer) Register(msg MessageMarshaller) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.lis != nil {
		return ErrorRegisteringRequest
	}
	key := fmt.Sprintf("%v%v", s.urlPrefix, msg.Name())
	s.messages[key] = msg
	log.Printf("%s: registered %s\n", s.logPrefix, s.getURL(msg))
	return
}

// Unregister is part of Server interface.
func (s *httpServer) Unregister(msg MessageMarshaller) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.lis != nil {
		return ErrorRegisteringRequest
	}
	name := msg.Name()
	if s.messages[name] == nil {
		return ErrorMessageUnknown
	}
	delete(s.messages, name)
	log.Printf("%s: unregistered %s\n", s.logPrefix, s.getURL(msg))
	return
}

// Start is part of Server interface.
func (s *httpServer) Start() (err error) {
	if s.lis, err = net.Listen("tcp", s.srv.Addr); err != nil {
		return err
	}

	// Server routine
	go func() {
		defer s.shutdown()

		log.Printf("%s: starting ...\n", s.logPrefix)
		err := s.srv.Serve(s.lis) // serve until listener is closed.
		if err != nil {
			log.Printf("%s: error - %v\n", s.logPrefix, err)
		}
	}()
	return
}

// Stop is part of Server interface.
func (s *httpServer) Stop() {
	s.shutdown()
	log.Printf("%s: stopped\n", s.logPrefix)
}

func (s *httpServer) shutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.lis != nil {
		s.lis.Close()
		close(s.reqch)
		s.lis = nil
	}
}

func (s *httpServer) getURL(msg MessageMarshaller) string {
	return s.urlPrefix + msg.Name()
}

// handle incoming request.
func (s *httpServer) systemHandler(w http.ResponseWriter, r *http.Request) {
	logPrefix := fmt.Sprintf("%s: request %q", s.logPrefix, r.URL.Path)
	// Fault-tolerance. No need to crash the server in case of panic.
	defer func() {
		if r := recover(); r != nil {
			log.Printf("%s, error %v\n", logPrefix, r)
		}
	}()

	msg := s.messages[r.URL.Path]
	if msg == nil {
		log.Printf("%s, path not found\n", s.logPrefix)
		http.Error(w, "path not found", http.StatusNotFound)
		return
	}

	log.Printf("%s\n", logPrefix)

	data := make([]byte, r.ContentLength, r.ContentLength)
	r.Body.Read(data)

	// Get an instance of message type and decode request into that.
	typeOfMsg := reflect.ValueOf(msg).Elem().Type()
	m := reflect.New(typeOfMsg).Interface().(MessageMarshaller)
	if err := m.Decode(data); err != nil {
		log.Printf("%s, error decoding request %v\n", logPrefix, err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// send and wait
	waitch := make(chan interface{}, 1)
	s.reqch <- &httpAdminRequest{srv: s, msg: m, waitch: waitch}
	switch val := (<-waitch).(type) {
	case MessageMarshaller:
		if data, err := val.Encode(); err == nil {
			header := w.Header()
			header["Content-Type"] = []string{val.ContentType()}
			w.Write(data)
		} else {
			log.Printf("%s, error encoding response %v\n", logPrefix, err)
			http.Error(w, "internal error", http.StatusInternalServerError)
		}

	case error:
		http.Error(w, val.Error(), http.StatusInternalServerError)
	}
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
