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

func doMB13339(
	cluster string, client *qclient.GsiClient) (err error) {

	for _, args := range mb13339Commands {
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

var mb13339Commands = [][]string{
	[]string{
		"-type", "create", "-bucket", "beer-sample", "-index", "index-city",
		"-fields", "city",
	},
	[]string{
		"-type", "create", "-bucket", "beer-sample", "-index", "index-type",
		"-fields", "type",
	},
	[]string{
		"-type", "count", "-bucket", "beer-sample", "-index", "index-city",
		"-low", "[\"A\"]", "-high", "[\"s\"]",
	},
	[]string{"-type", "list", "-bucket", "beer-sample"},
	[]string{ // FIXME: drop and then retry ?
		"-type", "drop", "-indexes", "beer-sample:index-type",
	},
	[]string{ // retry
		"-type", "create", "-bucket", "beer-sample", "-index", "index-type",
		"-fields", "type",
	},
	[]string{
		"-type", "count", "-bucket", "beer-sample", "-index", "index-type",
		"-equal", "[\"brewery\"]",
	},
	[]string{"-type", "list", "-bucket", "beer-sample"},
	[]string{
		"-type", "drop", "-indexes", "beer-sample:index-city",
	},
	[]string{
		"-type", "drop", "-indexes", "beer-sample:index-type",
	},
}
