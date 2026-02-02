//go:build nolint

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/couchbase/indexing/secondary/logging"

	"github.com/couchbase/cbauth"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/dataport"
	"github.com/couchbase/indexing/secondary/projector"

	projc "github.com/couchbase/indexing/secondary/projector/client"

	protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
)

var pooln = "default"

var options struct {
	buckets       []string // buckets to connect
	endpoints     []string // list of endpoint daemon to start
	coordEndpoint string   // co-ordinator endpoint
	stat          int      // periodic timeout to print dataport statistics
	timeout       int      // timeout for dataport to exit
	auth          string
	projector     bool // start projector, useful in debug mode.
	debug         bool
	trace         bool
	certFile      string
	keyFile       string
}

func argParse() []string {
	buckets := "default"
	endpoints := "localhost:9020"
	coordEndpoint := "localhost:9000"

	flag.StringVar(&buckets, "buckets", buckets,
		"buckets to connect")
	flag.StringVar(&endpoints, "endpoints", endpoints,
		"list of endpoint daemon to start")
	flag.StringVar(&options.coordEndpoint, "coorendp", coordEndpoint,
		"co-ordinator endpoint")
	flag.IntVar(&options.stat, "stat", 1000,
		"periodic timeout to print dataport statistics")
	flag.IntVar(&options.timeout, "timeout", 0,
		"timeout for dataport to exit")
	flag.StringVar(&options.auth, "auth", "Administrator:asdasd",
		"Auth user and password")
	flag.BoolVar(&options.projector, "projector", false,
		"start projector for debug mode")
	flag.BoolVar(&options.debug, "debug", false,
		"run in debug mode")
	flag.BoolVar(&options.trace, "trace", false,
		"run in trace mode")
	flag.StringVar(&options.certFile, "certFile", "",
		"certFile for N2N encryption")
	flag.StringVar(&options.keyFile, "keyFile", "",
		"keyFile file for N2N encryption")
	flag.Parse()

	options.buckets = strings.Split(buckets, ",")
	options.endpoints = strings.Split(endpoints, ",")
	if options.debug {
		logging.SetLogLevel(logging.Debug)
	} else if options.trace {
		logging.SetLogLevel(logging.Trace)
	} else {
		logging.SetLogLevel(logging.Info)
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

	// setup cbauth
	up := strings.Split(options.auth, ":")
	_, err := cbauth.InternalRetryDefaultInit(clusters[0], up[0], up[1])
	if err != nil {
		log.Fatalf("Failed to initialize cbauth: %s", err)
	}

	maxvbs := c.SystemConfig["maxVbuckets"].Int()
	dconf := c.SystemConfig.SectionConfig("indexer.dataport.", true)
	dconf.SetValue("genServerChanSize", 1000000)

	// start dataport servers.
	for _, endpoint := range options.endpoints {
		go dataport.Application(
			endpoint, options.stat, options.timeout, maxvbs, dconf,
			func(addr string, msg interface{}) bool { return true })
	}
	//go dataport.Application(options.coordEndpoint, 0, 0, maxvbs, dconf, nil)

	for _, cluster := range clusters {
		adminport := getProjectorAdminport(cluster, "default")
		if options.projector {
			config := c.SystemConfig.Clone()
			config.SetValue("projector.clusterAddr", cluster)
			config.SetValue("projector.adminport.listenAddr", adminport)
			epfactory := NewEndpointFactory(cluster, maxvbs)
			config.SetValue("projector.routerEndpointFactory", epfactory)
			projector.NewProjector(maxvbs, config, options.certFile, options.keyFile) // start projector daemon
		}

		// projector-client
		cconfig := c.SystemConfig.SectionConfig("indexer.projectorclient.", true)
		projClient, err := projc.NewClient(adminport, maxvbs, cconfig)
		if err != nil {
			log.Fatalf("Could not open projector client for admin port: %v", adminport)
		} else {
			projectors[cluster] = projClient
		}
	}

	backFillStream()

	time.Sleep(1000 * time.Second)
	//<-make(chan bool) // wait for ever
}

func backFillStream() {
	// index instances for specified buckets.
	instances := protobuf.ScaleDefault4i(
		options.buckets, options.endpoints, options.coordEndpoint)

	// start backfill stream on each projector
	for _, client := range projectors {
		go func(client *projc.Client) {
			// start backfill stream on each projector
			_, err := client.InitialTopicRequest(
				"backfill" /*topic*/, "default", /*pooln*/
				"dataport" /*endpointType*/, instances,
				true /*async */, 0, /*opaque2*/
				false /*needsAuth*/, 0, /*numVBuckets*/
				0 /*numVbWorkers*/, 0, /*numDcpConns*/
			)
			if err != nil {
				log.Fatal(err)
			}
		}(client)
	}
}

func getProjectorAdminport(cluster, pooln string) string {
	url, err := c.ClusterAuthUrl(cluster)
	if err != nil {
		log.Fatal(err)
	}
	cinfo, err := c.NewClusterInfoCache(url, pooln)
	if err != nil {
		log.Fatal(err)
	}
	if err := cinfo.Fetch(); err != nil {
		log.Fatal(err)
	}
	nodeID := cinfo.GetCurrentNode()
	adminport, err := cinfo.GetServiceAddress(nodeID, "projector", false)
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
func NewEndpointFactory(cluster string, nvbs int) c.RouterEndpointFactory {

	return func(topic, endpointType, addr string, config c.Config) (c.RouterEndpoint, error) {
		switch endpointType {
		case "dataport":
			return dataport.NewRouterEndpoint(cluster, topic, addr, nvbs, config)
		default:
			log.Fatal("Unknown endpoint type")
		}
		return nil, nil
	}
}
