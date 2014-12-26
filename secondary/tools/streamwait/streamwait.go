package main

import "flag"
import "os"
import "fmt"
import "strings"
import "log"
import "time"

import c "github.com/couchbase/indexing/secondary/common"

var options struct {
    maxVbs   int
    debug    bool
    trace    bool
    vbuckets []uint16
}

func argParse() []string {
    flag.IntVar(&options.maxVbs, "maxvbs", 1024,
        "configured number vbuckets")
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

    options.vbuckets = make([]uint16, 0, options.maxVbs)
    for i := 0; i < options.maxVbs; i++ {
        options.vbuckets = append(options.vbuckets, uint16(i))
    }

    args := flag.Args()
    if len(args) < 1 {
        os.Exit(1)
    }
    return strings.Split(args[0], ",")
}

func main() {
    cluster := argParse()[0]
    bucket, err := c.ConnectBucket(cluster, "default", "beer-sample")
    if err != nil {
        log.Fatal(err)
    }
    // get dcp feed for this bucket.
    suffix := uint32(time.Now().UnixNano() >> 24)
    name := fmt.Sprintf("streamwait-test-%v", suffix)
    uprFeed, err := bucket.StartUprFeed(name, uint32(0))
    if err != nil {
        log.Fatal(err)
    }
    go func() {
        // start vbucket streams
        for _, vbno := range options.vbuckets {
            flags, vbuuid := uint32(0), uint64(0)
            start, end := uint64(0), uint64(0xFFFFFFFFFFFFFFFF)
            snapStart, snapEnd := uint64(0), uint64(0)
            err := uprFeed.UprRequestStream(
                vbno, vbno /*opaque*/, flags, vbuuid, start, end,
                snapStart, snapEnd)
            if err != nil {
                log.Fatal(err)
            }
            // FIXME/TODO: the below sleep avoid back-to-back dispatch of
            // StreamRequest to DCP, which seem to cause some problems.
            time.Sleep(2 * time.Millisecond)
        }
    }()
    tick := time.Tick(time.Second)
    countEvents := 0
    for {
        select {
        case _, ok := <-uprFeed.C:
            if !ok {
                log.Fatal("uprFeed channel has closed")
            }
            countEvents++

        case <-tick:
            log.Println("events received countEvents", countEvents)
        }
    }
}
