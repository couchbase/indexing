package main

import (
    "flag"
    "fmt"
    "log"
    "os"
    "reflect"
    "time"

    c "github.com/couchbase/indexing/secondary/common"
    "github.com/couchbase/indexing/secondary/protobuf"
    "github.com/couchbase/indexing/secondary/queryport"
)

var options struct {
    server  string
    par     int
    seconds int
    debug   bool
    trace   bool
}

func argParse() {
    flag.StringVar(&options.server, "server", "localhost:8888",
        "query server address")
    flag.IntVar(&options.par, "par", 1,
        "maximum number of vbuckets")
    flag.IntVar(&options.seconds, "seconds", 1,
        "seconds to run")
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
    time.Sleep(100 * time.Millisecond)

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
    s.Close()
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
                []byte("aaaa"), []byte("zzzz"), 0, 100, true, 1000,
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
    case *protobuf.ScanRequest:
    default:
        log.Fatalf("unknown request %v\n", req)
    }

    select {
    case respch <- testResponseStream:
    case <-quitch:
        log.Fatal("Unexpected quit")
    }
    close(respch)
}

var testResponseStream = &protobuf.ResponseStream{
    Entries: []*protobuf.IndexEntry{
        &protobuf.IndexEntry{
            EntryKey: []byte("aaaaa"), PrimaryKey: []byte("key"),
        },
        &protobuf.IndexEntry{
            EntryKey: []byte("aaaaa"), PrimaryKey: []byte("key"),
        },
    },
}
