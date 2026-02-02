//go:build nolint

// Tool receives raw events from dcp client.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	couchbase "github.com/couchbase/indexing/secondary/dcp"
	mcd "github.com/couchbase/indexing/secondary/dcp/transport"
	mc "github.com/couchbase/indexing/secondary/dcp/transport/client"
	"github.com/couchbase/indexing/secondary/logging"
)

var options struct {
	buckets    []string // buckets to connect with
	maxVbno    int      // maximum number of vbuckets
	stats      int      // periodic timeout(ms) to print stats, 0 will disable
	duration   int
	printflogs bool
	info       bool
	debug      bool
	trace      bool
}

var done = make(chan bool, 16)

func argParse() string {
	var buckets string

	flag.StringVar(&buckets, "buckets", "default",
		"buckets to listen")
	flag.IntVar(&options.maxVbno, "maxvb", 1024,
		"maximum number of vbuckets")
	flag.IntVar(&options.stats, "stats", 1000,
		"periodic timeout in mS, to print statistics, `0` will disable stats")
	flag.IntVar(&options.duration, "duration", 3000,
		"receive mutations till duration milliseconds.")
	flag.BoolVar(&options.printflogs, "flogs", false,
		"display failover logs")
	flag.BoolVar(&options.info, "info", false,
		"display informational logs")
	flag.BoolVar(&options.debug, "debug", false,
		"display debug logs")
	flag.BoolVar(&options.trace, "trace", false,
		"display trace logs")

	flag.Parse()

	options.buckets = strings.Split(buckets, ",")
	if options.debug {
		logging.SetLogLevel(logging.Debug)
	} else if options.trace {
		logging.SetLogLevel(logging.Trace)
	} else {
		logging.SetLogLevel(logging.Info)
	}

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
	rch := make(chan []interface{}, 10000)
	for _, bucket := range options.buckets {
		go startBucket(cluster, bucket, rch)
	}
	receive(rch)
}

func startBucket(cluster, bucketn string, rch chan []interface{}) int {
	defer func() {
		if r := recover(); r != nil {
			logging.Errorf("Recovered from panic %v", r)
			logging.Errorf(logging.StackTrace())
		}
	}()

	logging.Infof("Connecting with %q\n", bucketn)
	b, err := common.ConnectBucket(cluster, "default", bucketn)
	mf(err, "bucket")

	dcpConfig := map[string]interface{}{
		"genChanSize":    10000,
		"dataChanSize":   10000,
		"numConnections": 4,
	}
	dcpFeed, err := b.StartDcpFeed(couchbase.NewDcpFeedName("rawupr"), uint32(0), 0xABCD, dcpConfig)
	mf(err, "- upr")

	vbnos := listOfVbnos(options.maxVbno)
	flogs, err := b.GetFailoverLogs(0xABCD, vbnos, dcpConfig)
	mf(err, "- dcp failoverlogs")
	if options.printflogs {
		printFlogs(vbnos, flogs)
	}
	go startDcp(dcpFeed, flogs)

	for {
		e, ok := <-dcpFeed.C
		if ok == false {
			logging.Infof("Closing for bucket %q\n", bucketn)
		}
		rch <- []interface{}{bucketn, e}
	}
}

func startDcp(dcpFeed *couchbase.DcpFeed, flogs couchbase.FailoverLog) {
	start, end := uint64(0), uint64(0xFFFFFFFFFFFFFFFF)
	snapStart, snapEnd := uint64(0), uint64(0)
	for vbno, flog := range flogs {
		x := flog[len(flog)-1] // map[uint16][][2]uint64
		opaque, flags, vbuuid := uint16(0), uint32(0), x[0]
		err := dcpFeed.DcpRequestStream(
			vbno, opaque, flags, vbuuid, start, end, snapStart, snapEnd)
		mf(err, fmt.Sprintf("stream-req for %v failed", vbno))
	}
}

func receive(rch chan []interface{}) {
	// bucket -> Opcode -> #count
	counts := make(map[string]map[mcd.CommandCode]int)

	var tick <-chan time.Time
	if options.stats > 0 {
		tick = time.Tick(time.Millisecond * time.Duration(options.stats))
	}

	finTimeout := time.After(time.Millisecond * time.Duration(options.duration))
loop:
	for {
		select {
		case msg, ok := <-rch:
			if ok == false {
				break loop
			}
			bucket, e := msg[0].(string), msg[1].(*mc.DcpEvent)
			if e.Opcode == mcd.DCP_MUTATION {
				logging.Tracef("DcpMutation KEY -- %v\n", string(e.Key))
				logging.Tracef("     %v\n", string(e.Value))
			}
			if _, ok := counts[bucket]; !ok {
				counts[bucket] = make(map[mcd.CommandCode]int)
			}
			if _, ok := counts[bucket][e.Opcode]; !ok {
				counts[bucket][e.Opcode] = 0
			}
			counts[bucket][e.Opcode]++

		case <-tick:
			for bucket, m := range counts {
				logging.Infof("%q %s\n", bucket, sprintCounts(m))
			}
			logging.Infof("\n")

		case <-finTimeout:
			break loop
		}
	}
}

func sprintCounts(counts map[mcd.CommandCode]int) string {
	line := ""
	for i := 0; i < 256; i++ {
		opcode := mcd.CommandCode(i)
		if n, ok := counts[opcode]; ok {
			line += fmt.Sprintf("%s:%v ", mcd.CommandNames[opcode], n)
		}
	}
	return strings.TrimRight(line, " ")
}

func listOfVbnos(maxVbno int) []uint16 {
	// list of vbuckets
	vbnos := make([]uint16, 0, maxVbno)
	for i := 0; i < maxVbno; i++ {
		vbnos = append(vbnos, uint16(i))
	}
	return vbnos
}

func printFlogs(vbnos []uint16, flogs couchbase.FailoverLog) {
	for i, vbno := range vbnos {
		logging.Infof("Failover log for vbucket %v\n", vbno)
		logging.Infof("   %#v\n", flogs[uint16(i)])
	}
	logging.Infof("\n")
}

func mf(err error, msg string) {
	if err != nil {
		logging.Fatalf("%v: %v", msg, err)
	}
}
