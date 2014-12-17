package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/dataport"
	"github.com/couchbase/indexing/secondary/projector"
)

var done = make(chan bool)

var options struct {
	adminport string
	kvaddrs   string
	colocate  bool
	info      bool
	debug     bool
	trace     bool
}

func argParse() string {
	flag.StringVar(&options.adminport, "adminport", "localhost:9999",
		"adminport address")
	flag.StringVar(&options.kvaddrs, "kvaddrs", "127.0.0.1:12000",
		"comma separated list of kvaddrs")
	flag.BoolVar(&options.colocate, "colocate", true,
		"whether projector will be colocated with KV")
	flag.BoolVar(&options.info, "info", false,
		"enable info level logging")
	flag.BoolVar(&options.debug, "debug", false,
		"enable debug level logging")
	flag.BoolVar(&options.trace, "trace", false,
		"enable trace level logging")

	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
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
	cluster := argParse() // eg. "localhost:9000"
	if options.trace {
		c.SetLogLevel(c.LogLevelTrace)
	} else if options.debug {
		c.SetLogLevel(c.LogLevelDebug)
	} else if options.info {
		c.SetLogLevel(c.LogLevelInfo)
	}

	maxvbs := c.SystemConfig["maxVbuckets"].Int()
	config := c.SystemConfig.SectionConfig("projector.", true)
	config.SetValue("clusterAddr", cluster)
	econf := c.SystemConfig.SectionConfig("endpoint.dataport.", true)
	epfactory := NewEndpointFactory(cluster, maxvbs, econf)
	config.SetValue("routerEndpointFactory", epfactory)
	config.SetValue("colocate", options.colocate)
	config.SetValue("adminport.listenAddr", options.adminport)

	if !config["colocate"].Bool() {
		log.Fatal("Only colocation policy is supported for now!")
	}

	go c.ExitOnStdinClose()
	projector.NewProjector(maxvbs, config)
	<-done
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
