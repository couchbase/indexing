package main

import "flag"
import "fmt"
import "os"

import c "github.com/couchbase/indexing/secondary/common"
import qclient "github.com/couchbase/indexing/secondary/queryport/client"
import "github.com/couchbase/indexing/secondary/logging"
import "github.com/couchbase/indexing/secondary/querycmd"

func usage(fset *flag.FlagSet) {
	fmt.Fprintf(os.Stderr, "Usage: %s [options]\n", os.Args[0])
	fset.PrintDefaults()
}

func main() {
	logging.SetLogLevel(logging.Warn)
	cmdOptions, _, fset, err := querycmd.ParseArgs(os.Args[1:])
	if err != nil {
		logging.Fatalf("%v\n", err)
	} else if cmdOptions.Help {
		usage(fset)
		os.Exit(0)
	}

	config := c.SystemConfig.SectionConfig("queryport.client.", true)
	client, err := qclient.NewGsiClient(
		cmdOptions.Server, config)
	if err != nil {
		logging.Fatalf("%v\n", err)
	}

	if err = querycmd.HandleCommand(client, cmdOptions, false, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "Error occured %v\n", err)
	}
	client.Close()
}
