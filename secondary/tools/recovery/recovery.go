package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	ap "github.com/couchbase/indexing/secondary/adminport"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/dataport"
	"github.com/couchbase/indexing/secondary/projector"
	"github.com/couchbase/indexing/secondary/protobuf"
)

var pooln = "default"
var cluster = "locahost:9000"

var options struct {
	buckets       []string
	endpoints     []string
	coordEndpoint string
	stat          string // periodic timeout to print dataport statistics
	timeout       string // timeout for dataport to exit
	maxVbno       int
}

func argParse() string {
	buckets := "default"
	endpoints := "localhost:9020"
	coordEndpoint := "localhost:9021"

	flag.StringVar(&buckets, "buckets", buckets,
		"buckets to project")
	flag.StringVar(&endpoints, "endpoints", endpoints,
		"endpoints for mutations stream")
	flag.StringVar(&options.coordEndpoint, "coorendp", coordEndpoint,
		"coordinator endpoint")
	flag.StringVar(&options.stat, "stat", "0",
		"periodic timeout to print dataport statistics")
	flag.StringVar(&options.timeout, "timeout", "0",
		"timeout for dataport to exit")
	flag.IntVar(&options.maxVbno, "maxvb", 1024,
		"max number of vbuckets")

	flag.Parse()

	options.buckets = strings.Split(buckets, ",")
	options.endpoints = strings.Split(endpoints, ",")

	args := flag.Args()
	if len(args) < 1 {
		usage()
		os.Exit(1)
	}
	return args[0]
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] <addr> \n", os.Args[0])
	flag.PrintDefaults()
}

var done = make(chan bool)
var projectors = make(map[string]ap.Client)

func main() {
	c.SetLogLevel(c.LogLevelInfo)

	cluster = argParse()

	// start dataport servers.
	for _, endpoint := range options.endpoints {
		stat, _ := strconv.Atoi(options.stat)
		timeout, _ := strconv.Atoi(options.timeout)
		go dataport.Application(endpoint, stat, timeout, appHandler)
	}
	go dataport.Application(options.coordEndpoint, 0, 0, nil)

	projector.SpawnProjectors(cluster, pooln, options.buckets, projectors)

	// start backfill stream on each projector
	for kvaddr, c := range projectors {
		startTopic(kvaddr, c, nil)
	}

	<-make(chan bool) // wait for ever
}

// bucket -> vbno -> [4]uint64{vbuuid, seqno, snapstart, snapend}
var activity = make(map[string]map[uint16][4]uint64)

func appHandler(endpoint string, msg interface{}) bool {
	switch v := msg.(type) {
	case []*protobuf.VbKeyVersions:
		for _, vb := range v {
			bucket := vb.GetBucketname()
			m, ok := activity[bucket]
			if !ok {
				m = make(map[uint16][4]uint64)
			}

			vbno, vbuuid := uint16(vb.GetVbucket()), vb.GetVbuuid()
			n, ok := m[vbno]
			if !ok {
				n = [4]uint64{vbuuid, 0, 0, 0}
			}
			for _, kv := range vb.GetKvs() {
				// fmt.Printf("vbucket %v %#v\n", vbno, kv)
				seqno := kv.GetSeqno()
				if seqno > n[1] {
					n[1] = seqno
				}
				typ, start, end := kv.Snapshot()
				if typ != 0 {
					n[2], n[3] = start, end
				}
			}

			m[vbno] = n
			activity[bucket] = m
		}

	case dataport.RestartVbuckets:
		tss := make(map[string]*c.Timestamp)
		fmt.Println(".....", activity)
		for bucket, vbnos := range v {
			ts := c.NewTimestamp(bucket, c.MaxVbuckets)
			for _, vbno := range vbnos {
				if m, ok := activity[bucket]; ok {
					if n, ok := m[vbno]; ok {
						if n[1] == n[3] || n[1] > n[2] {
							ts.Append(vbno, n[0], n[1], n[1], n[1])
						} else {
							ts.Append(vbno, n[0], n[1], n[2], n[3])
						}
					}
				}
			}
			fmt.Println(ts)
			tss[ts.Bucket] = ts
		}

		newkvaddrs := projector.SpawnProjectors(
			cluster, pooln, options.buckets, projectors)

		for kvaddr, c := range projectors {
			if _, ok := newkvaddrs[kvaddr]; ok {
				startTopic(kvaddr, c, tss)
				continue
			}
			instances := projector.ExampleIndexInstances(
				options.buckets, options.endpoints, options.coordEndpoint)
			projector.RestartMutationStream(
				c, "backfill", "default", tss, instances,
				func(res *protobuf.MutationStreamResponse, err error) bool {
					if err != nil {
						log.Println("restart error", err)
					} else {
						// log.Printf("%s\n", res.Repr())
					}
					return true
				})
		}

	case dataport.ShutdownDataport:
		log.Println(v)

	case dataport.RepairVbuckets:
		projectors = projector.ShutdownProjectors(
			cluster, pooln, options.buckets, projectors)
		if v != nil && len(v) > 0 {
			for _, c := range projectors {
				projector.RepairEndpoints(c, "backfill", []string{endpoint})
			}
		}

	case error:
		log.Println("recovery error", v)
		return false

	}
	return true
}

func mf(err error, msg string) {
	if err != nil {
		log.Fatalf("%v: %v", msg, err)
	}
}

func startTopic(kvaddr string, p ap.Client, tss map[string]*c.Timestamp) {
	// start backfill stream on each projector
	instances := projector.ExampleIndexInstances(
		options.buckets, options.endpoints, options.coordEndpoint)
	_, err := projector.InitialMutationStream(
		p, "backfill" /*topic*/, "default" /*pooln*/, options.buckets,
		[]string{kvaddr}, tss, instances)
	if err != nil {
		log.Fatal(err)
	}
}
