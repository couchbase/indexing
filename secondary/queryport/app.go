package queryport

import "log"

import c "github.com/couchbase/indexing/secondary/common"
import "github.com/couchbase/indexing/secondary/protobuf"

// Application is example application logic that uses query-port server
func Application() {
	killch := make(chan bool)
	s, err := NewServer(
		"localhost:9990",
		func(req interface{},
			respch chan<- interface{}, quitch <-chan interface{}) {
			requestHandler(req, respch, quitch, killch)
		}, c.SystemConfig)

	if err != nil {
		log.Fatal(err)
	}
	<-killch
	s.Close()
}

// will be spawned as a go-routine by server's connection handler.
func requestHandler(
	req interface{},
	respch chan<- interface{}, // send reponse message back to client
	quitch <-chan interface{}, // client / connection might have quit (done)
	killch chan bool, // application is shutting down the server.
) {

	var responses []*protobuf.ResponseStream

	switch req.(type) {
	case *protobuf.StatisticsRequest:
		// responses = getStatistics()
	case *protobuf.ScanRequest:
		// responses = scanIndex()
	case *protobuf.ScanAllRequest:
		// responses = fullTableScan()
	}

loop:
	for _, resp := range responses {
		// query storage backend for request
		select {
		case respch <- resp:
		case <-quitch:
			close(killch)
			break loop
		}
	}
	close(respch)
	// Free resources.
}
