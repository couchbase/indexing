package main

import "flag"
import "fmt"
import "log"
import "os"
import "runtime"
import "runtime/pprof"

import "github.com/couchbase/indexing/secondary/logging"
import c "github.com/couchbase/indexing/secondary/common"
import "github.com/couchbase/indexing/secondary/querycmd"
import qclient "github.com/couchbase/indexing/secondary/queryport/client"

func usage(fset *flag.FlagSet) {
	fmt.Fprintf(os.Stderr, "Usage: %s (sanity|bench|...)\n", os.Args[0])
}

func main() {
	logging.SetLogLevel(logging.Warn)
	runtime.GOMAXPROCS(runtime.NumCPU())

	fd, err := os.Create("queryport.pprof")
	if err != nil {
		panic(err)
	}
	pprof.StartCPUProfile(fd)
	defer pprof.StopCPUProfile()

	cmdOptions, args, fset, err := querycmd.ParseArgs(os.Args[1:])
	if err != nil {
		logging.Fatalf("%v", err)
		os.Exit(0)
	} else if cmdOptions.Help {
		usage(fset)
		os.Exit(0)
	} else if len(args) < 1 {
		logging.Fatalf("%v", "specify a command")
	}

	b, err := c.ConnectBucket(cmdOptions.Server, "default", "default")
	if err != nil {
		log.Fatal(err)
	}
	defer b.Close()
	maxvb, err := c.MaxVbuckets(b)
	if err != nil {
		log.Fatal(err)
	}

	config := c.SystemConfig.SectionConfig("queryport.client.", true)
	client, err := qclient.NewGsiClient(cmdOptions.Server, config)
	if err != nil {
		log.Fatal(err)
	}

	switch args[0] {
	case "sanity":
		err = doSanityTests(cmdOptions.Server, client)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error occured %v\n", err)
		}

	case "mb14786":
		err = doMB14786(cmdOptions.Server, client)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error occured %v\n", err)
		}

	case "mb13339":
		err = doMB13339(cmdOptions.Server, client)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error occured %v\n", err)
		}

	case "mb16115":
		err = doMB16115(cmdOptions.Server, client)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error occured %v\n", err)
		}

	case "benchmark":
		doBenchmark(cmdOptions.Server, "localhost:8101")

	case "consistency":
		doConsistency(cmdOptions.Server, maxvb, client)
	}
	client.Close()
}
