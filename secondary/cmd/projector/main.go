package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/couchbase/cbauth"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/dataport"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/projector"
)

var done = make(chan bool)

var options struct {
	adminport string
	kvaddrs   string
	colocate  bool
	logFile   string
	auth      string
	loglevel  string
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
	flag.StringVar(&options.loglevel, "logLevel", "Info",
		"Log Level - Silent, Fatal, Error, Info, Debug, Trace")
	flag.StringVar(&options.auth, "auth", "",
		"Auth user and password")

	// so we don't need to sync merge with ns_server. remove soon
	var unused string
	flag.StringVar(&unused, "debug", "", "Not Used")

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
	c.HideConsole(true)
	defer c.HideConsole(false)
	c.SeedProcess()

	cluster := argParse() // eg. "localhost:9000"

	config := c.SystemConfig.Clone()
	logging.SetLogLevel(logging.Level(options.loglevel))

	if f := getlogFile(); f != nil {
		fmt.Printf("Projector logging to %q\n", f.Name())
		logging.SetLogWriter(f)
		config.SetValue("log.file", f.Name())
	}
	config.SetValue("projector.clusterAddr", cluster)
	if options.colocate == false {
		logging.Fatalf("Only colocation policy is supported for now!\n")
	}
	config.SetValue("projector.colocate", options.colocate)
	config.SetValue("projector.adminport.listenAddr", options.adminport)

	// setup cbauth
	if options.auth != "" {
		up := strings.Split(options.auth, ":")
		if _, err := cbauth.InternalRetryDefaultInit(cluster, up[0], up[1]); err != nil {
			logging.Fatalf("Failed to initialize cbauth: %s", err)
		}
	}

	maxvbs := config["maxVbuckets"].Int()
	econf := c.SystemConfig.SectionConfig("endpoint.dataport.", true)
	epfactory := NewEndpointFactory(cluster, maxvbs, econf)
	config.SetValue("projector.routerEndpointFactory", epfactory)

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
			logging.Fatalf("Unknown endpoint type\n")
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
			logging.Fatalf("%v", err)
		}
		return f
	}
	f, err := os.Create(options.logFile)
	if err != nil {
		logging.Fatalf("%v", err)
	}
	return f
}
