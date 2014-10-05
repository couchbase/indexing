package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/dataport"
	"github.com/couchbase/indexing/secondary/projector"
)

var done = make(chan bool)

var options struct {
	adminport string
	kvaddrs   []string
	info      bool
	debug     bool
	trace     bool
}

func argParse() string {
	var kvaddrs string

	flag.StringVar(&options.adminport, "adminport", "localhost:9999",
		"adminport address")
	flag.StringVar(&kvaddrs, "kvaddrs", "127.0.0.1:12000",
		"comma separated list of kvaddrs")
	flag.BoolVar(&options.info, "info", false,
		"enable info level logging")
	flag.BoolVar(&options.debug, "debug", false,
		"enable debug level logging")
	flag.BoolVar(&options.trace, "trace", false,
		"enable trace level logging")

	flag.Parse()

	options.kvaddrs = strings.Split(kvaddrs, ",")

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
	settings := map[string]interface{}{
		"cluster":   cluster,
		"adminport": options.adminport,
		"kvaddrs":   options.kvaddrs,
		"epfactory": c.RouterEndpointFactory(EndpointFactory),
	}
	projector.NewProjector(settings)
	<-done
}

// EndpointFactory to create endpoint instances based on settings.
func EndpointFactory(
	topic, addr string,
	settings map[string]interface{}) (c.RouterEndpoint, error) {

	switch v := settings["type"].(string); v {
	case "dataport":
		return dataport.NewRouterEndpoint(topic, addr, settings)
	default:
		log.Fatal("Unknown endpoint type")
	}
	return nil, nil
}
