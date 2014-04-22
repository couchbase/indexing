// admin server to handle admin and system messages.
//
// Example server {
//      reqch  := make(chan adminport.Request)
//      server := NewHttpServer("localhost:9999", 0, 0, reqch)
//      server.Register(&protobuf.RequestMessage{})
//
//      loop:
//      for {
//          select {
//          case req, ok := <-reqch:
//              if ok {
//                  msg := req.GetMessage().(protobuf.RequestMessage)
//                  // interpret request and compose a response
//                  respMsg := protobuf.ResponseMessage{}
//                  err := req.Send(&respMsg)
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
	"log"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"
)

// httpServer is a concrete type implementing adminport Server interface.
type httpServer struct {
	mu       sync.Mutex
	lis      net.Listener                 // TCP listener
	srv      *http.Server                 // http server
	messages map[string]MessageMarshaller // map of registered requests
	reqch    chan<- Request               // request channel back to application
}

// NewHttpServer creates an instance of admin-server. Start() will actually
// start the server.
//
// Arguments,
//  connAddr, <host:port> on which the server should listen
//  rt,       read timeout for the server
//  wt,       write timeout for the server
//  reqch,    back channel to application for handling Requests
func NewHttpServer(connAddr string, rt, wt time.Duration, reqch chan<- Request) Server {
	s := &httpServer{
		reqch:    reqch,
		messages: make(map[string]MessageMarshaller),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", s.systemHandler)
	s.srv = &http.Server{
		Addr:           connAddr,
		Handler:        mux,
		ReadTimeout:    rt * time.Second,
		WriteTimeout:   wt * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	return s
}

func (s *httpServer) Register(msg MessageMarshaller) (err error) {
	if s.lis != nil {
		return fmt.Errorf("Server is already running")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages[msg.Name()] = msg
	return
}

func (s *httpServer) Unregister(msg MessageMarshaller) (err error) {
	if s.lis != nil {
		return fmt.Errorf("server is already running")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	name := msg.Name()
	if s.messages[name] == nil {
		return fmt.Errorf("message name not registered")
	}
	delete(s.messages, name)
	return
}

func (s *httpServer) Start() (err error) {
	if s.lis, err = net.Listen("tcp", s.srv.Addr); err != nil {
		return err
	}

	// Server routine
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("adminport %q: %v\n", s.srv.Addr, r)
			}
			s.shutdown()
		}()

		err := s.srv.Serve(s.lis) // serve until listener is closed.
		if err != nil {
			panic(err)
		}
	}()
	return
}

func (s *httpServer) Stop() {
	log.Println("Stopping server", s.srv.Addr)
	s.shutdown()
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

// handle incoming request.
func (s *httpServer) systemHandler(w http.ResponseWriter, r *http.Request) {
	// Fault-tolerance. No need to crash the server in case of panic.
	defer func() {
		if r := recover(); r != nil {
			log.Printf("error handling http request: %v\n", r)
		}
	}()

	msg := s.messages[strings.Trim(r.URL.Path, "/")]
	if msg == nil {
		http.Error(w, "path not found", http.StatusNotFound)
		return
	}

	log.Printf("admin server (%v) %v\n", s.srv.Addr, r.URL.Path)

	data := make([]byte, r.ContentLength)
	r.Body.Read(data)

	// Get an instance of message type and decode request into that.
	typeOfMsg := reflect.ValueOf(msg).Elem().Type()
	m := reflect.New(typeOfMsg).Interface().(MessageMarshaller)
	if err := m.Decode(data); err != nil {
		log.Printf("error decoding request: %v\n", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	waitch := make(chan MessageMarshaller, 1)
	s.reqch <- &httpAdminRequest{srv: s, msg: m, waitch: waitch}

	// Wait for response message
	respMsg := <-waitch
	if data, err := respMsg.Encode(); err == nil {
		header := w.Header()
		header["Content-Type"] = []string{respMsg.ContentType()}
		w.Write(data)
	} else {
		log.Printf("error encoding response (%v) %v\n", r.URL.Path, err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
}

// httpAdminRequest is a concrete type implementing Request interface.
type httpAdminRequest struct {
	srv    *httpServer
	msg    MessageMarshaller
	waitch chan MessageMarshaller
}

func (r *httpAdminRequest) GetMessage() MessageMarshaller {
	return r.msg
}

func (r *httpAdminRequest) Send(msg MessageMarshaller) (err error) {
	r.waitch <- msg
	return
}
