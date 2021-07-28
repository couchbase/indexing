package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/indexing/secondary/logging"

	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/dataport"
	"github.com/couchbase/indexing/secondary/projector"

	projc "github.com/couchbase/indexing/secondary/projector/client"

	protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"

	data "github.com/couchbase/indexing/secondary/protobuf/data"
)

var pooln = "default"

var options struct {
	buckets       []string
	endpoints     []string
	coordEndpoint string
	stat          int // periodic timeout to print dataport statistics
	timeout       int // timeout for dataport to exit
	maxVbnos      int
	auth          string
	projector     bool // start projector, useful in debug mode.
	debug         bool
	trace         bool
}

func argParse() string {
	buckets := "default"
	endpoints := "localhost:9020"

	flag.StringVar(&buckets, "buckets", buckets,
		"buckets to project")
	flag.StringVar(&endpoints, "endpoints", endpoints,
		"endpoints for mutations stream")
	flag.StringVar(&options.coordEndpoint, "coorendp", "localhost:9021",
		"coordinator endpoint")
	flag.IntVar(&options.stat, "stat", 1000,
		"periodic timeout to print dataport statistics")
	flag.IntVar(&options.timeout, "timeout", 0,
		"timeout for dataport to exit")
	flag.IntVar(&options.maxVbnos, "maxvb", 1024,
		"max number of vbuckets")
	flag.StringVar(&options.auth, "auth", "Administrator:asdasd",
		"Auth user and password")
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
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] <addr> \n", os.Args[0])
	flag.PrintDefaults()
}

var done = make(chan bool)
var projectors = make(map[string]*projc.Client) // cluster -> client

func main() {
	clusters := strings.Split(argParse(), ",")

	// setup cbauth
	up := strings.Split(options.auth, ":")
	_, err := cbauth.InternalRetryDefaultInit(clusters[0], up[0], up[1])
	if err != nil {
		log.Fatalf("Failed to initialize cbauth: %s", err)
	}

	// start dataport servers.
	maxvbs := c.SystemConfig["maxVbuckets"].Int()
	dconf := c.SystemConfig.SectionConfig("indexer.dataport.", true)
	for _, endpoint := range options.endpoints {
		go dataport.Application(
			endpoint, options.stat, options.timeout, maxvbs, dconf, appHandler)
	}
	go dataport.Application(options.coordEndpoint, 0, 0, maxvbs, dconf, nil)

	// spawn initial set of projectors
	for _, cluster := range clusters {
		adminport := getProjectorAdminport(cluster, "default")
		if options.projector {
			config := c.SystemConfig.Clone()
			config.SetValue("projector.clusterAddr", cluster)
			econf := c.SystemConfig.SectionConfig("projector.dataport.", true)
			epfactory := NewEndpointFactory(cluster, maxvbs, econf)
			config.SetValue("projector.routerEndpointFactory", epfactory)
			config.SetValue("projector.adminport.listenAddr", adminport)
			projector.NewProjector(maxvbs, config) // start projector daemon
		}
		// projector-client
		cconfig := c.SystemConfig.SectionConfig("indexer.projectorclient.", true)
		projectors[cluster] = projc.NewClient(adminport, maxvbs, cconfig)
	}

	// index instances for initial bucket []string{default}.
	instances := protobuf.ExampleIndexInstances(
		[]string{"beer-sample"}, options.endpoints, options.coordEndpoint)

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

	<-make(chan bool) // wait for ever
}

// bucket -> vbno -> [4]uint64{vbuuid, seqno, snapstart, snapend}
var activity = make(map[string]map[uint16][4]uint64)

func appHandler(endpoint string, msg interface{}) bool {
	switch v := msg.(type) {
	case []*data.VbKeyVersions:
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

	case dataport.ConnectionError:
		tss := make([]*protobuf.TsVbuuid, 0)
		for bucket, m := range activity {
			ts := protobuf.NewTsVbuuid("default", bucket, options.maxVbnos)
			for vbno, n := range m {
				if n[1] == n[3] || n[1] > n[2] {
					// seqno, vbuuid, start, end
					ts.Append(vbno, n[1], n[0], n[1], n[1])
				} else {
					// seqno, vbuuid, start, end
					ts.Append(vbno, n[1], n[0], n[2], n[3])
				}
			}
			tss = append(tss, ts)
		}

		// wait for one second and post repair endpoints and restart-vbuckets
		time.Sleep(1 * time.Second)
		endpoints := make([]string, 0)
		endpoints = append(endpoints, options.endpoints...)
		endpoints = append(endpoints, options.coordEndpoint)
		for _, client := range projectors {
			client.RepairEndpoints("backfill", endpoints)
			for _, ts := range tss {
				fmt.Println("RestartVbuckets ....", endpoint, ts.Repr())
			}
			client.RestartVbuckets("backfill", tss)
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

func getProjectorAdminport(cluster, pooln string) string {
	cinfo, err := c.NewClusterInfoCache(c.ClusterUrl(cluster), pooln)
	if err != nil {
		log.Fatal("error cluster-info: %v", err)
	}

	if err = cinfo.Fetch(); err != nil {
		log.Fatal("error cluster-info: %v", err)
	}
	nodeId := cinfo.GetCurrentNode()
	adminport, err := cinfo.GetServiceAddress(nodeId, "projector", false)
	if err != nil {
		log.Fatal(err)
	}
	return adminport
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
