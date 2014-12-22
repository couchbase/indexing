package main

import "flag"
import "fmt"
import "log"
import "os"
import "strconv"
import "strings"
import "time"

import c "github.com/couchbase/indexing/secondary/common"
import "github.com/couchbase/indexing/secondary/dataport"
import "github.com/couchbase/indexing/secondary/projector"
import projc "github.com/couchbase/indexing/secondary/projector/client"
import protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"

var pooln = "default"

var options struct {
	buckets       []string // buckets to connect
	endpoints     []string // list of endpoint daemon to start
	coordEndpoint string   // co-ordinator endpoint
	stat          string   // periodic timeout to print dataport statistics
	timeout       string   // timeout for dataport to exit
	projector     bool     // start projector, useful in debug mode.
	debug         bool
	trace         bool
}

func argParse() []string {
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
	flag.BoolVar(&options.projector, "projector", false,
		"start projector for debug mode")
	flag.BoolVar(&options.debug, "debug", false,
		"run in debug mode")
	flag.BoolVar(&options.trace, "trace", false,
		"run in trace mode")

	flag.Parse()

	options.buckets = strings.Split(buckets, ",")
	options.endpoints = strings.Split(endpoints, ",")
	if options.debug {
		c.SetLogLevel(c.LogLevelDebug)
	} else if options.trace {
		c.SetLogLevel(c.LogLevelTrace)
	} else {
		c.SetLogLevel(c.LogLevelInfo)
	}

	args := flag.Args()
	if len(args) < 1 || len(options.buckets) < 1 {
		usage()
		os.Exit(1)
	}
	return strings.Split(args[0], ",")
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] <addr> \n", os.Args[0])
	flag.PrintDefaults()
}

var projectors = make(map[string]*projc.Client)

func main() {
	clusters := argParse()

	maxvbs := c.SystemConfig["maxVbuckets"].Int()
	dconf := c.SystemConfig.SectionConfig("projector.dataport.indexer.", true)

	// start dataport servers.
	for _, endpoint := range options.endpoints {
		stat, _ := strconv.Atoi(options.stat)
		timeout, _ := strconv.Atoi(options.timeout)
		go dataport.Application(
			endpoint, stat, timeout, maxvbs, dconf,
			func(addr string, msg interface{}) bool { return true })
	}
	go dataport.Application(options.coordEndpoint, 0, 0, maxvbs, dconf, nil)

	for _, cluster := range clusters {
		adminport := getProjectorAdminport(cluster, "default")
		if options.projector {
			config := c.SystemConfig.SectionConfig("projector.", true)
			config.SetValue("clusterAddr", cluster)
			config.SetValue("adminport.listenAddr", adminport)
			econf := c.SystemConfig.SectionConfig("endpoint.dataport.", true)
			epfactory := NewEndpointFactory(cluster, maxvbs, econf)
			config.SetValue("routerEndpointFactory", epfactory)
			projector.NewProjector(maxvbs, config) // start projector daemon
		}

		// projector-client
		cconfig := c.SystemConfig.SectionConfig("projector.client.", true)
		projectors[cluster] = projc.NewClient(adminport, maxvbs, cconfig)
	}

	// index instances for specified buckets.
	instances := protobuf.ExampleIndexInstances(
		options.buckets, options.endpoints, options.coordEndpoint)

	// start backfill stream on each projector
	for _, client := range projectors {
		// start backfill stream on each projector
		_, err := client.InitialTopicRequest(
			"backfill" /*topic*/, "default", /*pooln*/
			"dataport" /*endpointType*/, instances)
		if err != nil {
			log.Fatal(err)
		}
	}

	time.Sleep(1000 * time.Second)
	//<-make(chan bool) // wait for ever
}

func getProjectorAdminport(cluster, pooln string) string {
	cinfo := c.NewClusterInfoCache(cluster, pooln)
	if err := cinfo.Fetch(); err != nil {
		log.Fatal(err)
	}
	nodeID := cinfo.GetCurrentNode()
	adminport, err := cinfo.GetServiceAddress(nodeID, "projector")
	if err != nil {
		log.Fatal(err)
	}
	return adminport
}

func mf(err error, msg string) {
	if err != nil {
		log.Fatalf("%v: %v", msg, err)
	}
}

// NewEndpointFactory to create endpoint instances based on config.
func NewEndpointFactory(
	cluster string, maxvbs int, econf c.Config) c.RouterEndpointFactory {

	return func(topic, endpointType, addr string) (c.RouterEndpoint, error) {
		switch endpointType {
		case "dataport":
			return dataport.NewRouterEndpoint(cluster, topic, addr, maxvbs, econf)
		default:
			log.Fatal("Unknown endpoint type")
		}
		return nil, nil
	}
}
