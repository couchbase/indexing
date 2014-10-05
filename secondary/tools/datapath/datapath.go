package main

import "flag"
import "fmt"
import "log"
import "os"
import "strconv"
import "strings"

import common "github.com/couchbase/indexing/secondary/common"
import "github.com/couchbase/indexing/secondary/dataport"
import "github.com/couchbase/indexing/secondary/projector"
import "github.com/couchbase/indexing/secondary/protobuf"

var pooln = "default"

var options struct {
	buckets       []string // buckets to connect
	endpoints     []string // list of endpoint daemon to start
	coordEndpoint string   // co-ordinator endpoint
	stat          string   // periodic timeout to print dataport statistics
	timeout       string   // timeout for dataport to exit
	maxVbno       int      // maximum number of vbuckets
	debug         bool
	trace         bool
}

func argParse() string {
	buckets := "default"
	endpoints := "localhost:9020"
	coordEndpoint := "localhost:9021"

	flag.StringVar(&buckets, "buckets", buckets,
		"buckets to connect")
	flag.StringVar(&endpoints, "endpoints", endpoints,
		"list of endpoint daemon to start")
	flag.StringVar(&options.coordEndpoint, "coorendp", coordEndpoint,
		"co-ordinator endpoint")
	flag.StringVar(&options.stat, "stat", "1000",
		"periodic timeout to print dataport statistics")
	flag.StringVar(&options.timeout, "timeout", "0",
		"timeout for dataport to exit")
	flag.IntVar(&options.maxVbno, "maxvb", 1024,
		"maximum number of vbuckets")
	flag.BoolVar(&options.debug, "debug", false,
		"run in debug mode")
	flag.BoolVar(&options.trace, "trace", false,
		"run in trace mode")

	flag.Parse()

	options.buckets = strings.Split(buckets, ",")
	options.endpoints = strings.Split(endpoints, ",")
	if options.debug {
		common.SetLogLevel(common.LogLevelDebug)
	} else if options.trace {
		common.SetLogLevel(common.LogLevelTrace)
	} else {
		common.SetLogLevel(common.LogLevelInfo)
	}

	args := flag.Args()
	if len(args) < 1 || len(options.buckets) < 1 {
		usage()
		os.Exit(1)
	}
	return args[0]
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] <addr> \n", os.Args[0])
	flag.PrintDefaults()
}

var projectors = make(map[string]*projector.Client)

func main() {
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

	kvaddrs, err := common.GetKVAddrs(cluster, pooln, options.buckets[0])
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("found %v nodes\n", kvaddrs)

	for _, kvaddr := range kvaddrs {
		adminport := kvaddr2adminport(kvaddr, 500)
		settings := map[string]interface{}{
			"cluster":   cluster,
			"adminport": adminport,
			"kvaddrs":   []string{kvaddr},
			"epfactory": common.RouterEndpointFactory(EndpointFactory),
		}
		projector.NewProjector(settings) // start projector daemon
		projectors[kvaddr] = projector.NewClient(adminport)
	}

	endpointSettings := map[string]interface{}{
		"type": "dataport",
	}

	// index instances for specified buckets.
	instances := protobuf.ExampleIndexInstances(
		options.buckets, options.endpoints, options.coordEndpoint)

	// start backfill stream on each projector
	for kvaddr, client := range projectors {
		// start backfill stream on each projector
		_, err := client.InitialTopicRequest(
			"backfill" /*topic*/, "default" /*pooln*/, kvaddr,
			endpointSettings, instances)
		if err != nil {
			log.Fatal(err)
		}
	}

	<-make(chan bool) // wait for ever
}

func kvaddr2adminport(kvaddr string, offset int) string {
	ss := strings.Split(kvaddr, ":")
	kport, err := strconv.Atoi(ss[1])
	if err != nil {
		log.Fatal(err)
	}
	return ss[0] + ":" + strconv.Itoa(kport+offset)
}

func mf(err error, msg string) {
	if err != nil {
		log.Fatalf("%v: %v", msg, err)
	}
}

// EndpointFactory to create endpoint instances based on settings.
func EndpointFactory(
	topic, addr string,
	settings map[string]interface{}) (common.RouterEndpoint, error) {

	switch v := settings["type"].(string); v {
	case "dataport":
		return dataport.NewRouterEndpoint(topic, addr, settings)
	default:
		log.Fatal("Unknown endpoint type")
	}
	return nil, nil
}
