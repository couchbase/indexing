package main

import "time"
import "log"
import "fmt"
import "reflect"
import "errors"

import c "github.com/couchbase/indexing/secondary/common"
import qclient "github.com/couchbase/indexing/secondary/queryport/client"
import "github.com/couchbase/indexing/secondary/queryport"
import protobuf "github.com/couchbase/indexing/secondary/protobuf/query"

//--------------------
// Benchmark queryport
//--------------------

func doBenchmark(cluster, addr string) {
	qconf := c.SystemConfig.SectionConfig("queryport.indexer.", true)
	s, err := queryport.NewServer(addr, serverCallb, qconf)
	if err != nil {
		log.Fatal(err)
	}
	loopback(cluster, addr)
	s.Close()
}

func loopback(cluster, raddr string) {
	qconf := c.SystemConfig.SectionConfig("queryport.client.", true)
	qconf.SetValue("poolSize", 10)
	qconf.SetValue("poolOverflow", mock_nclients)
	client, err := qclient.NewGsiClient(cluster, "querycmd", qconf)
	if err != nil {
		log.Fatal(err)
	}
	quitch := make(chan int)
	for i := 0; i < mock_nclients; i++ {
		t := time.After(time.Duration(mock_duration) * time.Second)
		go runClient(client, t, quitch)
	}

	count := 0
	for i := 0; i < mock_nclients; i++ {
		n := <-quitch
		count += n
	}

	client.Close()
	fmt.Printf("Completed %v queries in %v seconds\n", count, mock_duration)
}

func runClient(client *qclient.GsiClient, t <-chan time.Time, quitch chan<- int) {
	count := 0

loop:
	for {
		select {
		case <-t:
			quitch <- count
			break loop

		default:
			l, h := c.SecondaryKey{[]byte("aaaa")}, c.SecondaryKey{[]byte("zzzz")}
			err := client.Range(
				0xABBA /*defnID*/, l, h, 100, true, 1000,
				func(val qclient.ResponseReader) bool {
					switch v := val.(type) {
					case *protobuf.ResponseStream:
						count++
						if reflect.DeepEqual(v, testResponseStream) == false {
							log.Fatal("failed on testResponseStream")
						}
					case error:
						log.Println(v)
					}
					return true
				})
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

func serverCallb(
	req interface{}, respch chan<- interface{}, quitch <-chan interface{}) {

	switch req.(type) {
	case *protobuf.StatisticsRequest:
		resp := testStatisticsResponse
		select {
		case respch <- resp:
			close(respch)

		case <-quitch:
			log.Fatal("unexpected quit", req)
		}

	case *protobuf.ScanRequest:
		sendResponse(1, respch, quitch)
		close(respch)

	case *protobuf.ScanAllRequest:
		sendResponse(1, respch, quitch)
		close(respch)
	}
}

func sendResponse(
	count int, respch chan<- interface{}, quitch <-chan interface{}) {

	i := 0
loop:
	for ; i < count; i++ {
		select {
		case respch <- testResponseStream:
		case <-quitch:
			break loop
		}
	}
}

func waitUntilIndexState(
	client *qclient.GsiClient, defnIDs []uint64,
	state c.IndexState, period, timeout time.Duration) ([]c.IndexState, error) {

	expired := time.After(timeout * time.Millisecond)
	states := make([]c.IndexState, len(defnIDs))
	pending := len(defnIDs)
	for {
		select {
		case <-expired:
			return nil, errors.New("timeout")

		default:
		}
		for i, defnID := range defnIDs {
			if states[i] != state {
				if st, err := client.IndexState(defnID); err != nil {
					return nil, err
				} else if st == state {
					states[i] = state
					pending--
					continue
				}
			}
		}
		if pending == 0 {
			return states, nil
		}
		time.Sleep(period * time.Millisecond)
	}
}
