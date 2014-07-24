package main

import (
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"time"

	mc "github.com/couchbase/gomemcached/client"
	"github.com/couchbaselabs/go-couchbase"
)

var options struct {
	buckets string
	maxVbno int
}

var done = make(chan bool, 16)
var rch = make(chan []interface{}, 10000)

func argParse() []string {
	buckets := "default"
	flag.StringVar(&options.buckets, "buckets", buckets, "buckets to listen")
	flag.IntVar(&options.maxVbno, "maxvb", 1024, "max number of vbuckets")
	flag.Parse()
	return flag.Args()
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] <addr> \n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	args := argParse()
	if len(args) < 1 {
		usage()
		os.Exit(1)
	}

	for _, bucket := range strings.Split(options.buckets, ",") {
		go startBucket(args[0], bucket)
	}
	receive()
}

func startBucket(addr, bucket string) int {
	u, err := url.Parse(addr)
	mf(err, "parse")

	c, err := couchbase.Connect(u.String())
	mf(err, "connect - "+u.String())

	p, err := c.GetPool("default")
	mf(err, "pool")

	b, err := p.GetBucket(bucket)
	mf(err, "bucket")

	uprFeed, err := b.StartUprFeed("rawupr", uint32(0))
	mf(err, "- upr")

	flogs := failoverLogs(b)

	go startUpr(uprFeed, flogs)
	for {
		e, ok := <-uprFeed.C
		if ok == false {
			fmt.Println("Closing for bucket", b.Name)
		}
		rch <- []interface{}{b.Name, e}
	}
}

func startUpr(uprFeed *couchbase.UprFeed, flogs couchbase.FailoverLog) {
	start, end := uint64(0), uint64(0xFFFFFFFFFFFFFFFF)
	snapStart, snapEnd := uint64(0), uint64(0)
	for vbno, flog := range flogs {
		x := flog[len(flog)-1]
		flags, vbuuid := uint32(0), x[0]
		err := uprFeed.UprRequestStream(
			vbno, flags, vbuuid, start, end, snapStart, snapEnd)
		mf(err, fmt.Sprintf("stream-req for %v failed", vbno))
	}
}

func failoverLogs(b *couchbase.Bucket) couchbase.FailoverLog {
	vbnos := make([]uint16, 0, options.maxVbno)
	for i := 0; i < options.maxVbno; i++ {
		vbnos = append(vbnos, uint16(i))
	}
	flogs, err := b.GetFailoverLogs(vbnos)
	mf(err, "- upr failoverlogs")
	return flogs
}

func mf(err error, msg string) {
	if err != nil {
		log.Fatalf("%v: %v", msg, err)
	}
}

func receive() {
	counts := make(map[string]map[mc.UprOpcode]int)
	tick := time.Tick(time.Second * 2)
	for {
		select {
		case msg, ok := <-rch:
			if ok == false {
				break
			}
			bucket, e := msg[0].(string), msg[1].(*mc.UprEvent)
			if _, ok := counts[bucket]; !ok {
				counts[bucket] = make(map[mc.UprOpcode]int)
			}
			if _, ok := counts[bucket][e.Opcode]; !ok {
				counts[bucket][e.Opcode] = 0
			}
			counts[bucket][e.Opcode]++
		case <-tick:
			for bucket, m := range counts {
				for opcode, n := range m {
					log.Printf("%q %v: %v \n", bucket, opcode, n)
				}
			}
			fmt.Println()
		}
	}
}
