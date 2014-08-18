// Tool receives raw events from go-couchbase UPR client.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	mc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbaselabs/go-couchbase"
)

var options struct {
	buckets    []string // buckets to connect with
	maxVbno    int      // maximum number of vbuckets
	stats      int      // periodic timeout(ms) to print stats, 0 will disable stats
	printflogs bool
}

var done = make(chan bool, 16)
var rch = make(chan []interface{}, 10000)

func argParse() string {
	var buckets string

	flag.StringVar(&buckets, "buckets", "default",
		"buckets to listen")
	flag.IntVar(&options.maxVbno, "maxvb", 1024,
		"maximum number of vbuckets")
	flag.IntVar(&options.stats, "stats", 1000,
		"periodic timeout in mS, to print statistics, `0` will disable stats")
	flag.BoolVar(&options.printflogs, "flogs", false,
		"display failover logs")

	options.buckets = strings.Split(buckets, ",")

	flag.Parse()
	args := flag.Args()
	if len(args) < 1 {
		usage()
		os.Exit(1)
	}
	return args[0]
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] <cluster-addr> \n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	cluster := argParse()
	for _, bucket := range options.buckets {
		go startBucket(cluster, bucket)
	}
	receive()
}

func startBucket(cluster, bucketn string) int {
	b, err := common.ConnectBucket(cluster, "default", bucketn)
	mf(err, "bucket")

	uprFeed, err := b.StartUprFeed("rawupr", uint32(0))
	mf(err, "- upr")

	flogs := failoverLogs(b)

	// list of vbuckets
	vbnos := make([]uint16, 0, options.maxVbno)
	for i := 0; i < options.maxVbno; i++ {
		vbnos = append(vbnos, uint16(i))
	}

	if options.printflogs {
		for i, vbno := range vbnos {
			fmt.Printf("Failover log for vbucket %v\n", vbno)
			fmt.Printf("   %#v\n", flogs[uint16(i)])
		}
		fmt.Println()
	}

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
		x := flog[len(flog)-1] // map[uint16][][2]uint64
		flags, vbuuid := uint32(0), x[0]
		err := uprFeed.UprRequestStream(
			vbno, flags, vbuuid, start, end, snapStart, snapEnd)
		mf(err, fmt.Sprintf("stream-req for %v failed", vbno))
	}
}

func failoverLogs(b *couchbase.Bucket) couchbase.FailoverLog {
	// list of vbuckets
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
	// bucket -> Opcode -> #count
	counts := make(map[string]map[mc.UprOpcode]int)

	var tick <-chan time.Time
	if options.stats > 0 {
		tick = time.Tick(time.Millisecond * time.Duration(options.stats))
	}

loop:
	for {
		select {
		case msg, ok := <-rch:
			if ok == false {
				break loop
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
				log.Printf("%q %s\n", bucket, sprintCounts(m))
			}
			fmt.Println()
		}
	}
}

func sprintCounts(counts map[mc.UprOpcode]int) string {
	line := ""
	for i := 0; i < 50; i++ {
		opcode := mc.UprOpcode(i)
		if n, ok := counts[opcode]; ok {
			line += fmt.Sprintf("%s:%v ", mc.UprOpcodeNames[opcode], n)
		}
	}
	return strings.TrimRight(line, " ")
}
