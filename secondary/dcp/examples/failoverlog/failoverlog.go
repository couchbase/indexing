//go:build nolint

package main

import (
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	couchbase "github.com/couchbase/indexing/secondary/dcp"
)

const clusterAddr = "http://localhost:9000"

var options struct {
	bucketn     string
	clusterAddr string
	maxVbs      int
	vbuckets    []uint16
	repeat      int
	repeatWait  int
}

func argParse() {
	var vbuckets string

	flag.StringVar(&options.bucketn, "bucket", "default",
		"bucket name")
	flag.IntVar(&options.maxVbs, "maxvbs", 1024,
		"max configured vbuckets")
	flag.StringVar(&vbuckets, "vbuckets", "",
		"list of vbuckets to fetch flogs")
	flag.IntVar(&options.repeat, "repeat", 1,
		"number of time to repeat fetching failover log")
	flag.IntVar(&options.repeatWait, "repeatWait", 1000,
		"time in mS, to wait before next repeat")

	flag.Parse()

	vbnos := make([]uint16, 0, options.maxVbs)
	if vbuckets == "" {
		for i := 0; i < options.maxVbs; i++ {
			vbnos = append(vbnos, uint16(i))
		}
	} else {
		for _, a := range strings.Split(vbuckets, ",") {
			vbno, err := strconv.Atoi(a)
			if err != nil {
				log.Fatal(err)
			}
			vbnos = append(vbnos, uint16(vbno))
		}
	}
	options.vbuckets = vbnos

	args := flag.Args()
	if len(args) < 1 {
		options.clusterAddr = clusterAddr
	} else {
		options.clusterAddr = args[0]
	}
}

func main() {
	argParse()
	// get a bucket and mc.Client connection
	bucket, err := getTestConnection(options.bucketn)
	if err != nil {
		panic(err)
	}

	// Get failover log for a vbucket
	dcpConfig := map[string]interface{}{
		"genChanSize":    10000,
		"dataChanSize":   10000,
		"numConnections": 4,
	}
	for options.repeat > 0 {
		opaque := uint16(options.repeat)
		flogs, err := bucket.GetFailoverLogs(opaque, options.vbuckets, dcpConfig)
		if err != nil {
			panic(err)
		}
		for vbno, flog := range flogs {
			log.Printf("Failover logs for vbucket %v: %v", vbno, flog)
		}
		options.repeat--
		if options.repeat > 0 {
			fmt.Println()
			time.Sleep(time.Duration(options.repeatWait) * time.Millisecond)
		}
	}
}

func getTestConnection(bucketname string) (*couchbase.Bucket, error) {
	couch, err := couchbase.Connect(options.clusterAddr)
	if err != nil {
		log.Println("Make sure that couchbase is at", options.clusterAddr)
		return nil, err
	}
	pool, err := couch.GetPool("default")
	if err != nil {
		return nil, err
	}
	bucket, err := pool.GetBucket(bucketname)
	return bucket, err
}
