package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/couchbase/cbauth"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/dataport"
	"github.com/couchbase/indexing/secondary/projector"
)

var done = make(chan bool)

var options struct {
	adminport string
	kvaddrs   string
	colocate  bool
	logFile   string
	auth      string
	info      bool
	debug     bool
	trace     bool
}

func argParse() string {
	flag.StringVar(&options.adminport, "adminport", "",
		"adminport address")
	flag.StringVar(&options.kvaddrs, "kvaddrs", "127.0.0.1:12000",
		"comma separated list of kvaddrs")
	flag.BoolVar(&options.colocate, "colocate", true,
		"whether projector will be colocated with KV")
	flag.StringVar(&options.logFile, "logFile", "",
		"output logs to file default is stdout")
	flag.StringVar(&options.auth, "auth", "",
		"Auth user and password")
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

	// setup cbauth
	if options.auth != "" {
		up := strings.Split(options.auth, ":")
		if _, err := cbauth.InternalRetryDefaultInit(cluster, up[0], up[1]); err != nil {
			log.Fatalf("Failed to initialize cbauth: %s", err)
		}
	}

	if f := getlogFile(); f != nil {
		log.Printf("Projector logging to %q\n", f.Name())
		c.SetLogWriter(f)
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

func getlogFile() *os.File {
	switch options.logFile {
	case "":
		return nil
	case "tempfile":
		f, err := ioutil.TempFile("", "projector")
		if err != nil {
			log.Fatal(err)
		}
		return f
	}
	f, err := os.Create(options.logFile)
	if err != nil {
		log.Fatal(err)
	}
	return f
}
