//go:build nolint

package main

import (
	"flag"
	"log"
	"os"
	"strings"
	"time"

	"github.com/couchbase/cbauth"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"

	couchbase "github.com/couchbase/indexing/secondary/dcp"
)

var options struct {
	maxVbs   int
	vbuckets []uint16
	auth     string
	debug    bool
	trace    bool
}

func argParse() []string {
	flag.StringVar(&options.auth, "auth", "",
		"Auth user and password")
	flag.BoolVar(&options.debug, "debug", false,
		"run in debug mode")
	flag.BoolVar(&options.trace, "trace", false,
		"run in trace mode")

	flag.Parse()

	if options.debug {
		logging.SetLogLevel(logging.Debug)
	} else if options.trace {
		logging.SetLogLevel(logging.Trace)
	} else {
		logging.SetLogLevel(logging.Info)
	}

	args := flag.Args()
	if len(args) < 1 {
		os.Exit(1)
	}
	return strings.Split(args[0], ",")
}

func main() {
	cluster := argParse()[0]

	// setup cbauth
	if options.auth != "" {
		up := strings.Split(options.auth, ":")
		if _, err := cbauth.InternalRetryDefaultInit(cluster, up[0], up[1]); err != nil {
			logging.Fatalf("Failed to initialize cbauth: %s", err)
		}
	}
	go startFeed(cluster, "streamwait-feed1")
	go startFeed(cluster, "streamwait-feed2")
	time.Sleep(1000 * time.Second)
}

func startFeed(cluster, name string) {
	bucket, err := c.ConnectBucket(cluster, "default", "default")
	if err != nil {
		log.Fatal(err)
	}
	defer bucket.Close()
	options.maxVbs, err = c.MaxVbuckets(bucket)
	if err != nil {
		log.Fatal(err)
	}
	options.vbuckets = make([]uint16, 0, options.maxVbs)
	for i := 0; i < options.maxVbs; i++ {
		options.vbuckets = append(options.vbuckets, uint16(i))
	}

	// get dcp feed for this bucket.
	config := map[string]interface{}{
		"genChanSize":    10000,
		"dataChanSize":   10000,
		"numConnections": 4,
	}
	dcpFeed, err := bucket.StartDcpFeed(couchbase.NewDcpFeedName(name), uint32(0), 0xABCD, config)
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		// start vbucket streams
		for _, vbno := range options.vbuckets {
			flags, vbuuid := uint32(0), uint64(0)
			start, end := uint64(0), uint64(0xFFFFFFFFFFFFFFFF)
			snapStart, snapEnd := uint64(0), uint64(0)
			err := dcpFeed.DcpRequestStream(
				vbno, vbno /*opaque*/, flags, vbuuid, start, end,
				snapStart, snapEnd)
			if err != nil {
				log.Fatal(err)
			}
			// FIXME/TODO: the below sleep avoid back-to-back dispatch of
			// StreamRequest to DCP, which seem to cause some problems.
			time.Sleep(1 * time.Millisecond)
		}
	}()
	tick := time.Tick(time.Second)
	countEvents := 0
	commands := make(map[byte]int)
	for {
		select {
		case e, ok := <-dcpFeed.C:
			if !ok {
				log.Fatal("dcpFeed channel has closed")
			}
			if _, ok := commands[byte(e.Opcode)]; !ok {
				commands[byte(e.Opcode)] = 0
			}
			commands[byte(e.Opcode)]++
			countEvents++

		case <-tick:
			log.Println("events received countEvents", countEvents)
			log.Println("commands received", commands)
		}
	}
}
