package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
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
	endpoints     []string // list of endpoint daemon to start
	coordEndpoint string   // co-ordinator endpoint
	stat          int      // periodic timeout to print dataport statistics
	timeout       int      // timeout to loop
	maxVbno       int      // maximum number of vbuckets
	addBuckets    []string
	delBuckets    []string
	auth          string
	loop          int
	projector     bool // start projector, useful in debug mode.
	debug         bool
	trace         bool
}

func argParse() []string {
	addBuckets := "default"
	delBuckets := "default"
	endpoints := "localhost:9020"

	flag.StringVar(&endpoints, "endpoints", endpoints,
		"list of endpoint daemon to start")
	flag.StringVar(&options.coordEndpoint, "coorendp", "localhost:9021",
		"co-ordinator endpoint")
	flag.IntVar(&options.stat, "stat", 1000,
		"periodic timeout to print dataport statistics")
	flag.IntVar(&options.timeout, "timeout", 0,
		"timeout to loop")
	flag.IntVar(&options.maxVbno, "maxvb", 1024,
		"maximum number of vbuckets")
	flag.StringVar(&addBuckets, "addBuckets", addBuckets,
		"buckets to add")
	flag.StringVar(&delBuckets, "delBuckets", addBuckets,
		"buckets to del")
	flag.StringVar(&options.auth, "auth", "Administrator:asdasd",
		"Auth user and password")
	flag.IntVar(&options.loop, "loop", 10,
		"repeat bucket-add and bucket-del loop number of times")
	flag.BoolVar(&options.projector, "projector", false,
		"start projector for debug mode")
	flag.BoolVar(&options.debug, "debug", false,
		"run in debug mode")
	flag.BoolVar(&options.trace, "trace", false,
		"run in trace mode")

	flag.Parse()

	options.addBuckets = strings.Split(addBuckets, ",")
	options.delBuckets = strings.Split(delBuckets, ",")
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
	return strings.Split(args[0], ",")
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] <addr> \n", os.Args[0])
	flag.PrintDefaults()
}

var projectors = make(map[string]*projc.Client)

var mutations struct {
	mu        sync.Mutex
	seqnos    map[string][]uint64    // bucket -> vbno -> seqno
	snapshots map[string][][2]uint64 // bucket -> vbno -> snapshot
}

func main() {
	clusters := argParse()

	// setup cbauth
	up := strings.Split(options.auth, ":")
	_, err := cbauth.InternalRetryDefaultInit(clusters[0], up[0], up[1])
	if err != nil {
		log.Fatalf("Failed to initialize cbauth: %s", err)
	}

	mutations.seqnos = map[string][]uint64{
		"beer-sample": make([]uint64, options.maxVbno),
		"default":     make([]uint64, options.maxVbno),
		"users":       make([]uint64, options.maxVbno),
		"project":     make([]uint64, options.maxVbno),
	}
	mutations.snapshots = map[string][][2]uint64{
		"beer-sample": make([][2]uint64, options.maxVbno),
		"default":     make([][2]uint64, options.maxVbno),
		"users":       make([][2]uint64, options.maxVbno),
		"project":     make([][2]uint64, options.maxVbno),
	}

	// start dataport servers.
	maxvbs := c.SystemConfig["maxVbuckets"].Int()
	dconf := c.SystemConfig.SectionConfig("indexer.dataport.", true)
	for _, endpoint := range options.endpoints {
		go dataport.Application(endpoint, options.stat, 0, maxvbs, dconf, endpointCallback)
	}
	go dataport.Application(options.coordEndpoint, 0, 0, maxvbs, dconf, nil)

	// spawn initial set of projectors
	for _, cluster := range clusters {
		adminport := getProjectorAdminport(cluster, "default")
		if options.projector {
			config := c.SystemConfig.Clone()
			config.SetValue("projector.clusterAddr", cluster)
			config.SetValue("projector.adminport.listenAddr", adminport)
			econf := c.SystemConfig.SectionConfig("projector.dataport.", true)
			epfactory := NewEndpointFactory(cluster, maxvbs, econf)
			config.SetValue("projector.routerEndpointFactory", epfactory)
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
			"backfill", "default", "dataport" /*endpointType*/, instances)
		if err != nil {
			log.Fatal(err)
		}
	}

loop:
	if options.addBuckets != nil {
		// add `buckets` and its instances after few seconds
		<-time.After(time.Duration(options.timeout) * time.Millisecond)
		instances = protobuf.ExampleIndexInstances(
			options.addBuckets, options.endpoints, options.coordEndpoint)
		for _, client := range projectors {
			ts, err := client.InitialRestartTimestamp(pooln, "default")
			if err != nil {
				log.Fatal(err)
			}
			reqTss := []*protobuf.TsVbuuid{bucketTimestamp("default", ts)}
			res, err := client.AddBuckets("backfill", reqTss, instances)
			if err != nil {
				log.Fatal(err)
			}
			if err := res.GetErr(); err != nil {
				log.Fatal(err)
			}
		}
	}

	if options.delBuckets != nil {
		// del `buckets` and its instances after few seconds
		<-time.After(time.Duration(options.timeout) * time.Millisecond)
		for _, client := range projectors {
			err := client.DelBuckets("backfill", options.delBuckets)
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	options.loop--
	if options.loop > 0 {
		goto loop
	}

	<-make(chan bool) // wait for ever
}

func mf(err error, msg string) {
	if err != nil {
		log.Fatalf("%v: %v", msg, err)
	}
}

func getProjectorAdminport(cluster, pooln string) string {
	cinfo, err := c.NewClusterInfoCache(c.ClusterUrl(cluster), pooln)
	if err != nil {
		log.Fatal(err)
	}
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

func endpointCallback(addr string, msg interface{}) bool {
	mutations.mu.Lock()
	defer mutations.mu.Unlock()
	if vbs, ok := msg.([]*data.VbKeyVersions); ok {
		for _, vb := range vbs {
			bucket, kvs := vb.GetBucketname(), vb.GetKvs()
			vbno := vb.GetVbucket()
			for _, kv := range kvs {
				for _, command := range kv.GetCommands() {
					cmd := byte(command)
					switch cmd {
					case c.Snapshot:
						_, start, end := kv.Snapshot()
						mutations.snapshots[bucket][vbno] = [2]uint64{start, end}
					case c.Upsert, c.UpsertDeletion, c.Deletion:
						mutations.seqnos[bucket][vbno] = kv.GetSeqno()
					}
				}
			}
		}
	}
	return true
}

func bucketTimestamp(bucketn string, ts *protobuf.TsVbuuid) *protobuf.TsVbuuid {
	for i, vbno := range ts.GetVbnos() {
		seqno := uint64(0)
		if int(vbno) < len(mutations.seqnos[bucketn]) {
			seqno = mutations.seqnos[bucketn][vbno]
		}
		ts.Seqnos[i] = seqno
		if int(vbno) < len(mutations.snapshots[bucketn]) {
			ss := mutations.snapshots[bucketn][vbno]
			ts.Snapshots[i] = protobuf.NewSnapshot(ss[0], ss[1])
		} else {
			ts.Snapshots[i] = protobuf.NewSnapshot(seqno, seqno)
		}
	}
	return ts
}
