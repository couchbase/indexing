package main

import "flag"
import "fmt"
import "log"
import "os"
import "strconv"
import "strings"

import c "github.com/couchbase/indexing/secondary/common"
import "github.com/couchbase/indexing/secondary/dataport"
import "github.com/couchbase/indexing/secondary/projector"
import projc "github.com/couchbase/indexing/secondary/projector/client"
import "github.com/couchbase/indexing/secondary/protobuf"

var pooln = "default"

var options struct {
	buckets       []string
	endpoints     []string
	coordEndpoint string
	stat          string // periodic timeout to print dataport statistics
	timeout       string // timeout for dataport to exit
	maxVbnos      int
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
	flag.StringVar(&options.stat, "stat", "1000",
		"periodic timeout to print dataport statistics")
	flag.StringVar(&options.timeout, "timeout", "0",
		"timeout for dataport to exit")
	flag.IntVar(&options.maxVbnos, "maxvb", 1024,
		"max number of vbuckets")
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
var projectors = make(map[string]*projc.Client)

func main() {
	cluster := argParse()

	// start dataport servers.
	maxvbs := c.SystemConfig["maxVbuckets"].Int()
	dconf := c.SystemConfig.SectionConfig("projector.dataport.indexer.", true)
	for _, endpoint := range options.endpoints {
		stat, _ := strconv.Atoi(options.stat)
		timeout, _ := strconv.Atoi(options.timeout)
		go dataport.Application(endpoint, stat, timeout, maxvbs, dconf, appHandler)
	}
	go dataport.Application(options.coordEndpoint, 0, 0, maxvbs, dconf, nil)

	kvaddrs, err := c.GetKVAddrs(cluster, pooln, "default" /*bucket*/)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("found %v nodes\n", kvaddrs)

	// spawn initial set of projectors
	for _, kvaddr := range kvaddrs {
		adminport := kvaddr2adminport(kvaddr, 500)
		config := c.SystemConfig.SectionConfig("projector.", true)
		config.SetValue("clusterAddr", cluster)
		config.SetValue("kvAddrs", kvaddr)
		config.SetValue("adminport.listenAddr", adminport)
		epfactory := NewEndpointFactory(maxvbs, config)
		config.SetValue("routerEndpointFactory", epfactory)
		projector.NewProjector(maxvbs, config) // start projector daemon
		// projector-client
		cconfig := c.SystemConfig.SectionConfig("projector.client.", true)
		projectors[kvaddr] = projc.NewClient(adminport, maxvbs, cconfig)
	}

	// index instances for initial bucket []string{default}.
	instances := protobuf.ExampleIndexInstances(
		[]string{"default", "beer-sample", "users"}, options.endpoints,
		options.coordEndpoint)

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

	case dataport.ConnectionError:
		fmt.Println("RestartVbuckets ....", activity)
		tss := make([]*protobuf.TsVbuuid, 0)
		for bucket, vbnos := range v {
			ts := protobuf.NewTsVbuuid("default", bucket, options.maxVbnos)
			for _, vbno := range vbnos {
				if m, ok := activity[bucket]; ok {
					if n, ok := m[vbno]; ok {
						if n[1] == n[3] || n[1] > n[2] {
							// seqno, vbuuid, start, end
							ts.Append(vbno, n[1], n[0], n[1], n[1])
						} else {
							// seqno, vbuuid, start, end
							ts.Append(vbno, n[1], n[0], n[2], n[3])
						}
					}
				}
			}
			fmt.Println(ts)
			tss = append(tss, ts)
		}

		for _, client := range projectors {
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

func kvaddr2adminport(kvaddr string, offset int) string {
	ss := strings.Split(kvaddr, ":")
	kport, err := strconv.Atoi(ss[1])
	if err != nil {
		log.Fatal(err)
	}
	return ss[0] + ":" + strconv.Itoa(kport+offset)
}

// NewEndpointFactory to create endpoint instances based on config.
func NewEndpointFactory(maxvbs int, config c.Config) c.RouterEndpointFactory {
	econf := config.SectionConfig("dataport.client.", true)
	return func(topic, endpointType, addr string) (c.RouterEndpoint, error) {
		switch endpointType {
		case "dataport":
			return dataport.NewRouterEndpoint(topic, addr, maxvbs, econf)
		default:
			log.Fatal("Unknown endpoint type")
		}
		return nil, nil
	}
}
