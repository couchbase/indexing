package main

import "flag"
import "fmt"
import "log"
import "os"
import "reflect"
import "time"

import c "github.com/couchbase/indexing/secondary/common"
import "github.com/couchbase/indexing/secondary/protobuf"
import "github.com/couchbase/indexing/secondary/queryport"
import "github.com/couchbaselabs/goprotobuf/proto"

var testStatisticsResponse = &protobuf.StatisticsResponse{
	Stats: &protobuf.IndexStatistics{
		Count:      proto.Uint64(100),
		UniqueKeys: proto.Uint64(100),
		Min:        []byte(`"aaaaa"`),
		Max:        []byte(`"zzzzz"`),
	},
}
var testResponseStream = &protobuf.ResponseStream{
	Entries: []*protobuf.IndexEntry{
		&protobuf.IndexEntry{
			EntryKey: []byte(`["aaaaa"]`), PrimaryKey: []byte("key1"),
		},
		&protobuf.IndexEntry{
			EntryKey: []byte(`["aaaaa"]`), PrimaryKey: []byte("key2"),
		},
	},
}

var options struct {
	server   string
	par      int
	seconds  int
	loopback bool
	mock     bool
	debug    bool
	trace    bool
}

func argParse() {
	flag.StringVar(&options.server, "server", "localhost:9998",
		"queryport server address")
	flag.IntVar(&options.par, "par", 1,
		"maximum number of vbuckets")
	flag.IntVar(&options.seconds, "seconds", 1,
		"seconds to run")
	flag.BoolVar(&options.loopback, "loopback", true,
		"run queryport in loopback")
	flag.BoolVar(&options.mock, "mock", false,
		"run queryport as mock scan coordinator")
	flag.BoolVar(&options.debug, "debug", false,
		"run in debug mode")
	flag.BoolVar(&options.trace, "trace", false,
		"run in trace mode")

	flag.Parse()

	if options.debug {
		c.SetLogLevel(c.LogLevelDebug)
	} else if options.trace {
		c.SetLogLevel(c.LogLevelTrace)
	} else {
		c.SetLogLevel(c.LogLevelInfo)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] <addr> \n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	argParse()
	s, err := queryport.NewServer(options.server, serverCallb, c.SystemConfig)
	if err != nil {
		log.Fatal(err)
	}

	if options.mock {
		blockForever := make(chan bool)
		<-blockForever

	} else if options.loopback {
		loopback()
	}
	s.Close()
}

func loopback() {
	config := c.SystemConfig.Clone().SetValue("queryport.client.poolSize", 10)
	config = config.SetValue("queryport.client.poolOverflow", options.par)
	client := queryport.NewClient(options.server, config)
	quitch := make(chan int)
	for i := 0; i < options.par; i++ {
		t := time.After(time.Duration(options.seconds) * time.Second)
		go runClient(client, t, quitch)
	}

	count := 0
	for i := 0; i < options.par; i++ {
		n := <-quitch
		count += n
	}

	client.Close()
	fmt.Printf("Completed %v queries in %v seconds\n", count, options.seconds)
}

func runClient(client *queryport.Client, t <-chan time.Time, quitch chan<- int) {
	count := 0

loop:
	for {
		select {
		case <-t:
			quitch <- count
			break loop

		default:
			client.Scan(
				"idx", "bkt", []byte("aaaa"), []byte("zzzz"), [][]byte{}, 0, 100, true, 1000,
				func(val interface{}) bool {
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
		}
	}
}

// Send back a single entry as response and close the channel.
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

	default:
		log.Fatal("unknown request", req)
	}
}

func sendResponse(count int, respch chan<- interface{}, quitch <-chan interface{}) {
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
