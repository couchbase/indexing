package queryport

import (
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"

	"net"

	protobuf "github.com/couchbase/indexing/secondary/protobuf/query"
)

// Application is example application logic that uses query-port server
func Application(config c.Config) {
	killch := make(chan bool)
	s, err := NewServer(
		"",
		"localhost:9990",
		func(req interface{}, ctx interface{},
			conn net.Conn, quitch <-chan bool, clientVersion uint32) {
			requestHandler(req, conn, quitch, killch)
		},
		nil,
		config)

	if err != nil {
		logging.Fatalf("Listen failed - %v", err)
	}
	<-killch
	s.Close()
}

// will be spawned as a go-routine by server's connection handler.
func requestHandler(
	req interface{},
	conn net.Conn, // Write handle to the tcp socket
	quitch <-chan bool, // client / connection might have quit (done)
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

	buf := make([]byte, 1024, 1024)
loop:
	for _, resp := range responses {
		// query storage backend for request
		protobuf.EncodeAndWrite(conn, buf, resp)
		select {
		case <-quitch:
			close(killch)
			break loop
		}
	}
	protobuf.EncodeAndWrite(conn, buf, &protobuf.StreamEndResponse{})
	// Free resources.
}
