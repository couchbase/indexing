package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/indexing/secondary/common"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/dataport"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/platform"
	"github.com/couchbase/indexing/secondary/projector"
)

var done = make(chan bool)

var options struct {
	adminport string
	kvaddrs   string
	logFile   string
	auth      string
	loglevel  string
	diagDir   string
	isIPv6    bool

	certFile string // PEM-format signed public key for accepting external TLS connections
	keyFile  string // PEM-format private key for accepting external TLS connections

	clientCertFile string // PEM-format signed public key for making internal TLS connections
	clientKeyFile  string // PEM-format private key for making internal TLS connections

	httpsPort       string
	caFile          string
	deploymentModel string

	logDir string
}

func argParse() string {
	fset := flag.NewFlagSet("projector", flag.ContinueOnError)
	fset.StringVar(&options.adminport, "adminport", "", "adminport address")

	// kvaddrs is passed in from ns-server.  For ipv6, it is expected that ns-server will pass in a proper address.
	fset.StringVar(&options.kvaddrs, "kvaddrs", "127.0.0.1:12000", "comma separated list of kvaddrs")
	fset.StringVar(&options.logFile, "logFile", "", "output logs to file default is stdout")
	fset.StringVar(&options.loglevel, "logLevel", "Info", "Log Level - Silent, Fatal, Error, Info, Debug, Trace")
	fset.StringVar(&options.auth, "auth", "", "Auth user and password")
	fset.StringVar(&options.diagDir, "diagDir", "./", "Directory for writing projector diagnostic information")

	// "Server" key pair for accepting external TLS connections to CB Server (PEM-format strings)
	fset.StringVar(&options.certFile, "certFile", "",
		"Projector X.509 certificate PEM string for accepting external TLS connections")
	fset.StringVar(&options.keyFile, "keyFile", "",
		"Projector X.509 private key PEM string for accepting external TLS connections")

	// "Client" key pair for making internal TLS connections to CB Server (PEM-format strings)
	fset.StringVar(&options.clientCertFile, "clientCertFile", "",
		"Projector X.509 certificate PEM string for making internal TLS connections")
	fset.StringVar(&options.clientKeyFile, "clientKeyFile", "",
		"Projector X.509 private key PEM string for making internal TLS connections")

	fset.StringVar(&options.httpsPort, "httpsPort", "", "projector https port")
	fset.StringVar(&options.caFile, "caFile", "", "Multiple Root/Client CAs")
	fset.StringVar(&options.deploymentModel, "deploymentModel", "default", "Specify the deployment model [provisioned|serverless|default]")
	fset.StringVar(&options.logDir, "logDir", "", "Dir to log projector stats into")

	ipv4 := fset.String("ipv4", "", "Specify if ipv4 is required|optional|off")
	ipv6 := fset.String("ipv6", "", "Specify if ipv6 is required|optional|off")

	logging.Infof("Parsing the args")

	for i := 1; i < len(os.Args); i++ {
		if err := fset.Parse(os.Args[i : i+1]); err != nil {
			if strings.Contains(err.Error(), "flag provided but not defined") {
				logging.Warnf("Ignoring the unspecified argument error: %v", err)
			} else {
				c.CrashOnError(err)
			}
		}
	}

	// Validate DeploymentModel
	if options.deploymentModel != "provisioned" &&
		options.deploymentModel != "serverless" &&
		options.deploymentModel != "default" {
		c.CrashOnError(fmt.Errorf("Deployment model should be [provisioned|serverless|default] but it is %v", options.deploymentModel))
	}

	// Set Deployment Model
	c.SetDeploymentModel(c.MakeDeploymentModel(options.deploymentModel))
	logging.Infof("Projector DeploymentModel is set to: %v", common.GetDeploymentModel())

	var isIPv6 bool
	var err error

	isIPv6, err = c.GetIPv6FromParam(*ipv4, *ipv6)
	if err != nil {
		logging.Fatalf("IsIPv6FromParam returns error: err=%v ipv4=%v ipv6=%v", err, *ipv4, *ipv6)
		c.CrashOnError(err)
	}

	options.isIPv6 = isIPv6

	args := fset.Args()
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
	platform.HideConsole(true)
	defer platform.HideConsole(false)
	c.SeedProcess()

	logging.Infof("Projector started with command line: %v\n", os.Args)

	cluster := argParse() // eg. "localhost:9000"

	config := c.SystemConfig.Clone()
	logging.SetLogLevel(logging.Level(options.loglevel))

	if f := getlogFile(); f != nil {
		fmt.Printf("Projector logging to %q\n", f.Name())
		logging.SetLogWriter(f)
		config.SetValue("log.file", f.Name())
	}
	config.SetValue("projector.clusterAddr", cluster)
	config.SetValue("projector.adminport.listenAddr", options.adminport)
	config.SetValue("projector.diagnostics_dir", options.diagDir)

	if len(options.logDir) != 0 {
		config.SetValue("projector.log_dir", options.logDir)
	}

	if err := os.MkdirAll(options.diagDir, 0755); err != nil {
		c.CrashOnError(err)
	}

	// setup cbauth
	if options.auth != "" {
		up := strings.Split(options.auth, ":")
		if _, err := cbauth.InternalRetryDefaultInit(cluster, up[0], up[1]); err != nil {
			logging.Fatalf("Failed to initialize cbauth: %s", err)
		}
	}

	epfactory := NewEndpointFactory(cluster)
	config.SetValue("projector.routerEndpointFactory", epfactory)

	logging.Infof("%v\n", c.LogOs())
	logging.Infof("%v\n", c.LogRuntime())

	logging.Infof("Setting ipv6=%v", options.isIPv6)

	c.SetIpv6(options.isIPv6)

	go c.ExitOnStdinClose()

	certFile := options.certFile
	/*
		if len(certFile) == 0 && len(options.diagDir) != 0 {
			if last := strings.LastIndex(options.diagDir, "/"); last != -1 {
				certFile = options.diagDir[:last]
				if len(certFile) != 0 {
					certFile = certFile + "/config/memcached-cert.pem"
				}
			}
		}
	*/

	keyFile := options.keyFile
	/*
		if len(keyFile) == 0 && len(options.diagDir) != 0 {
			if last := strings.LastIndex(options.diagDir, "/"); last != -1 {
				keyFile = options.diagDir[:last]
				if len(keyFile) != 0 {
					keyFile = keyFile + "/config/memcached-key.pem"
				}
			}
		}
	*/

	caFile := options.caFile

	projector.NewProjector(config, certFile, keyFile, caFile)

	if c.IsServerlessDeployment() {
		startRegulator()
	}

	<-done
}

// NewEndpointFactory to create endpoint instances based on config.
func NewEndpointFactory(cluster string) c.RouterEndpointFactory {

	return func(topic, endpointType, addr string, config c.Config, needsAuth bool) (c.RouterEndpoint, error) {
		switch endpointType {
		case "dataport":
			return dataport.NewRouterEndpoint(cluster, topic, addr, config, needsAuth)
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
