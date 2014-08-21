package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	ap "github.com/couchbase/indexing/secondary/adminport"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/dataport"
	"github.com/couchbase/indexing/secondary/projector"
)

var pooln = "default"

var options struct {
	endpoints     []string // list of endpoint daemon to start
	coordEndpoint string   // co-ordinator endpoint
	stat          string   // periodic timeout to print dataport statistics
	timeout       string   // timeout for dataport to exit
	maxVbno       int      // maximum number of vbuckets
}

func argParse() string {
	endpoints := "localhost:9020"

	flag.StringVar(&endpoints, "endpoints", endpoints,
		"list of endpoint daemon to start")
	flag.StringVar(&options.coordEndpoint, "coorendp", "localhost:9021",
		"co-ordinator endpoint")
	flag.StringVar(&options.stat, "stat", "0",
		"periodic timeout to print dataport statistics")
	flag.StringVar(&options.timeout, "timeout", "0",
		"timeout for dataport to exit")
	flag.IntVar(&options.maxVbno, "maxvb", 1024,
		"maximum number of vbuckets")

	flag.Parse()

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

var projectors = make(map[string]ap.Client)

func main() {
	c.SetLogLevel(c.LogLevelInfo)

	cluster := argParse()

	// start dataport servers.
	for _, endpoint := range options.endpoints {
		stat, _ := strconv.Atoi(options.stat)
		timeout, _ := strconv.Atoi(options.timeout)
		go dataport.Application(
			endpoint, stat, timeout,
			func(addr string, msg interface{}) bool { return true })
	}
	go dataport.Application(options.coordEndpoint, 0, 0, nil)

	// spawn initial set of projectors
	kvaddrs, err := projector.GetKVAddrs(cluster, pooln, "default")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("found %v nodes\n", kvaddrs)
	_, err = projector.SpawnProjectors(cluster, kvaddrs, projectors)
	if err != nil {
		log.Fatal(err)
	}

	// index instances for initial buckets.
	instances := projector.ExampleIndexInstances(
		[]string{"default"} /*buckets*/, options.endpoints,
		options.coordEndpoint)

	// start backfill stream on each projector
	for kvaddr, client := range projectors {
		// start backfill stream on each projector
		_, err := projector.InitialMutationStream(
			client, "backfill" /*topic*/, "default" /*pooln*/, kvaddr,
			[]string{"default"} /*buckets*/, instances)
		if err != nil {
			log.Fatal(err)
		}
	}

	buckets := []string{"projects", "beer-sample"}

	for {
		// add "beer-sample" bucket and its instances after few seconds
		<-time.After(10 * time.Second)
		instances = projector.ExampleIndexInstances(
			buckets, options.endpoints, options.coordEndpoint)
		for kvaddr, c := range projectors {
			err := projector.AddBuckets(
				c, "backfill" /*topic*/, kvaddr, "default" /*pooln*/, buckets,
				nil /*tss*/, instances)
			if err != nil {
				log.Fatal(err)
			}
		}

		// del "beer-sample" bucket and its instances after few seconds
		<-time.After(10 * time.Second)
		for _, c := range projectors {
			err := projector.DelBuckets(
				c, "backfill" /*topic*/, "default" /*pooln*/, buckets)
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	<-make(chan bool) // wait for ever
}

func mf(err error, msg string) {
	if err != nil {
		log.Fatalf("%v: %v", msg, err)
	}
}
