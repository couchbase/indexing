//go:build nolint

package main

import (
	"fmt"
	"log"
	"os"

	"github.com/couchbase/indexing/secondary/querycmd"
	qclient "github.com/couchbase/indexing/secondary/queryport/client"
)

// test case to simulate
// https://issues.couchbase.com/browse/MB-13339

func doMB14786(
	cluster string, client *qclient.GsiClient) (err error) {

	for _, args := range mb14786Commands {
		cmd, _, _, err := querycmd.ParseArgs(args)
		if err != nil {
			log.Fatal(err)
		}
		err = querycmd.HandleCommand(client, cmd, true, os.Stdout)
		if err != nil {
			fmt.Printf("%#v\n", cmd)
			fmt.Printf("    %v\n", err)
		}
		fmt.Println()
	}
	return
}

var mb14786Commands = [][]string{
	[]string{"-type", "drop", "-bucket", "beer-sample", "-index", "index_abv1"},
	[]string{"-type", "drop", "-bucket", "beer-sample", "-index", "index_abv2"},

	[]string{
		"-type", "create", "-bucket", "beer-sample", "-index", "index_abv1",
		"-fields", "abv",
	},
	[]string{
		"-type", "create", "-bucket", "beer-sample", "-index", "index_abv2",
		"-fields", "abv",
	},

	[]string{
		"-type", "count", "-bucket", "beer-sample", "-index", "index_abv1",
		"-low", "[1.0]", "-high", "[100.0]",
	},
	[]string{
		"-type", "count", "-bucket", "beer-sample", "-index", "index_abv2",
		"-low", "[1.0]", "-high", "[100.0]",
	},
	[]string{
		"-type", "count", "-bucket", "beer-sample", "-index", "index_abv1",
		"-low", "[1.0]", "-high", "[100.0]",
	},
}
