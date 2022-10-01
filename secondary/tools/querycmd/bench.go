package main

import (
	"fmt"
	"log"
	"net"
	"reflect"
	"time"

	c "github.com/couchbase/indexing/secondary/common"

	"github.com/couchbase/indexing/secondary/queryport"
	qclient "github.com/couchbase/indexing/secondary/queryport/client"

	protobuf "github.com/couchbase/indexing/secondary/protobuf/query"
	"github.com/golang/protobuf/proto"
)

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
	loopback(cluster, addr, 10, 1000)
	loopback(cluster, addr, 40, 1000)
	loopback(cluster, addr, 100, 1000)
	loopback(cluster, addr, 200, 1000)
	s.Close()
}

func loopback(cluster, raddr string, routines, pool_size int) {
	qconf := c.SystemConfig.SectionConfig("queryport.client.", true)
	qconf.SetValue("poolSize", pool_size)
	qconf.SetValue("poolOverflow", 20+routines)
	client := qclient.NewGsiScanClient(raddr, qconf)
	quitch := make(chan int)
	for i := 0; i < routines; i++ {
		t := time.After(time.Duration(mock_duration) * time.Second)
		go runClient(client, t, quitch)
	}

	count := 0
	for i := 0; i < routines; i++ {
		n := <-quitch
		count += n
	}

	client.Close()
	fmsg := "Completed %v queries in %v seconds with %v routines & %v pool_size\n"
	fmt.Printf(fmsg, count, mock_duration, routines, pool_size)
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
				0xABBA /*defnID*/, "requestId", l, h, 100, true, 1,
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
				}, false)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

// FIXME: this function is broken.
func serverCallb(req interface{}, conn net.Conn, quitch <-chan bool) {
	var buf [1024]byte

	switch req.(type) {
	case *protobuf.StatisticsRequest:
		protobuf.EncodeAndWrite(conn, buf[:], testStatisticsResponse)
	case *protobuf.ScanRequest:
		protobuf.EncodeAndWrite(conn, buf[:], testResponseStream)
	case *protobuf.ScanAllRequest:
		protobuf.EncodeAndWrite(conn, buf[:], testResponseStream)
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
