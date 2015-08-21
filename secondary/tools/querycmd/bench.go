package main

import "time"
import "log"
import "fmt"
import "net"
import "reflect"

import c "github.com/couchbase/indexing/secondary/common"
import qclient "github.com/couchbase/indexing/secondary/queryport/client"
import "github.com/couchbase/indexing/secondary/queryport"
import protobuf "github.com/couchbase/indexing/secondary/protobuf/query"
import "github.com/golang/protobuf/proto"

var mock_duration = 10

var testStatisticsResponse = &protobuf.StatisticsResponse{
	Stats: &protobuf.IndexStatistics{
		KeysCount:       proto.Uint64(100),
		UniqueKeysCount: proto.Uint64(100),
		KeyMin:          []byte(`"aaaaa"`),
		KeyMax:          []byte(`"zzzzz"`),
	},
}
var testResponseStream = &protobuf.ResponseStream{
	IndexEntries: []*protobuf.IndexEntry{
		&protobuf.IndexEntry{
			EntryKey: []byte(`["aaaaa"]`), PrimaryKey: []byte("key1"),
		},
		&protobuf.IndexEntry{
			EntryKey: []byte(`["aaaaa"]`), PrimaryKey: []byte("key2"),
		},
	},
}

//--------------------
// Benchmark queryport
//--------------------

func doBenchmark(cluster, addr string) {
	qconf := c.SystemConfig.SectionConfig("indexer.queryport.", true)
	s, err := queryport.NewServer(addr, serverCallb, qconf)
	if err != nil {
		log.Fatal(err)
	}
	loopback(cluster, addr, 1)
	loopback(cluster, addr, 2)
	loopback(cluster, addr, 4)
	loopback(cluster, addr, 8)
	loopback(cluster, addr, 16)
	s.Close()
}

func loopback(cluster, raddr string, mock_nclients int) {
	qconf := c.SystemConfig.SectionConfig("queryport.client.", true)
	qconf.SetValue("poolSize", 20)
	qconf.SetValue("poolOverflow", 20+mock_nclients)
	client := qclient.NewGsiScanClient(raddr, qconf)
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
	fmsg := "Completed %v queries in %v seconds with %v routines\n"
	fmt.Printf(fmsg, count, mock_duration, mock_nclients)
}

func runClient(client *qclient.GsiScanClient, t <-chan time.Time, quitch chan<- int) {
	count := 0

loop:
	for {
		select {
		case <-t:
			quitch <- count
			break loop

		default:
			l, h := c.SecondaryKey{[]byte("aaaa")}, c.SecondaryKey{[]byte("zzzz")}
			err, _ := client.Range(
				0xABBA /*defnID*/, l, h, 100, true, 1,
				c.AnyConsistency, nil,
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

// FIXME: this function is broken.
func serverCallb(
	req interface{}, conn net.Conn, quitch <-chan interface{}) {

	switch req.(type) {
	case *protobuf.StatisticsRequest:
	case *protobuf.ScanRequest:
	case *protobuf.ScanAllRequest:
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
