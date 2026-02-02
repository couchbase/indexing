package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/couchbase/indexing/secondary/querycmd"
	qclient "github.com/couchbase/indexing/secondary/queryport/client"
)

//------------------------
// memdb sanity test cases
//------------------------

func doMemdbTests(
	cluster string, client *qclient.GsiClient) (err error) {

	for _, args := range memdbCommands {
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

var memdbCommands = [][]string{
	[]string{
		"-type", "nodes",
	},
	[]string{
		"-type", "create", "-using", "memdb", "-bucket", "beer-sample",
		"-index", "index-city", "-fields", "city",
	},
	[]string{
		"-type", "create", "-using", "memdb", "-bucket", "beer-sample",
		"-index", "index-abv", "-fields", "abv",
		"-with", "{\"defer_build\": true}",
	},
	[]string{"-type", "list", "-bucket", "beer-sample"},

	// Query on index-city
	[]string{
		"-type", "scan", "-bucket", "beer-sample", "-index", "index-city",
		"-low", "[\"B\"]", "-high", "[\"D\"]", "-incl", "3", "-limit",
		"1000000000",
	},
	[]string{
		"-type", "scanAll", "-bucket", "beer-sample", "-index", "index-city",
		"-limit", "10000",
	},
	[]string{
		"-type", "count", "-bucket", "beer-sample", "-index", "index-city",
		"-equal", "[\"Beersel\"]",
	},
	[]string{
		"-type", "count", "-bucket", "beer-sample", "-index", "index-city",
		"-low", "[\"A\"]", "-high", "[\"s\"]",
	},
	[]string{
		"-type", "count", "-bucket", "beer-sample", "-index", "index-city",
	},
	[]string{
		"-type", "drop", "-indexes", "beer-sample:index-city",
	},

	// Deferred build
	[]string{
		"-type", "build", "-indexes", "beer-sample:index-abv",
	},

	// Query on index-abv
	[]string{
		"-type", "scan", "-bucket", "beer-sample", "-index", "index-abv",
		"-low", "[2]", "-high", "[20]", "-incl", "3", "-limit",
		"1000000000",
	},
	[]string{
		"-type", "scanAll", "-bucket", "beer-sample", "-index", "index-abv",
		"-limit", "10000",
	},
	[]string{
		"-type", "count", "-bucket", "beer-sample", "-index", "index-abv",
		"-equal", "[10]",
	},
	[]string{
		"-type", "count", "-bucket", "beer-sample", "-index", "index-abv",
		"-low", "[3]", "-high", "[50]",
	},
	[]string{
		"-type", "count", "-bucket", "beer-sample", "-index", "index-abv",
	},
	[]string{
		"-type", "drop", "-indexes", "beer-sample:index-abv",
	},
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
