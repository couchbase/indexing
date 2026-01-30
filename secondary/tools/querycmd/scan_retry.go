//go:build nolint

package main

import (
	"fmt"
	"log"
	"os"
	"time"

	c "github.com/couchbase/indexing/secondary/common"

	"github.com/couchbase/indexing/secondary/querycmd"
	qclient "github.com/couchbase/indexing/secondary/queryport/client"
)

func doScanRetry(cluster string, client *qclient.GsiClient) (err error) {

	crArgs := retryCommands[0]
	cmd, _, _, err := querycmd.ParseArgs(crArgs)
	if err != nil {
		log.Fatal(err)
	}
	err = querycmd.HandleCommand(client, cmd, true, os.Stdout)
	if err != nil {
		fmt.Printf("%#v\n", cmd)
		fmt.Printf("    %v\n", err)
	}
	fmt.Println()

	for {
		index, _ := querycmd.GetIndex(client, cmd.Bucket, cmd.IndexName)
		if index == nil {
			time.Sleep(5 * time.Second)
			continue
		}
		defnID := uint64(index.Definition.DefnId)

		crArgs := retryCommands[1]
		cmd, _, _, err := querycmd.ParseArgs(crArgs)
		if err != nil {
			log.Fatal(err)
		}
		low, high, incl := cmd.Low, cmd.High, cmd.Inclusion
		cons := c.SessionConsistency

		fmt.Printf("CountRange:\n")
		count, err := client.CountRange(
			uint64(defnID), "requestId", low, high, incl, cons, nil)
		if err == nil {
			fmt.Printf(
				"Index %q/%q has %v entries\n", cmd.Bucket, cmd.IndexName, count)
		} else {
			fmt.Printf("fail: %v %v\n", defnID, err)
		}
		fmt.Println()
		time.Sleep(1 * time.Second)
	}
	return
}

var retryCommands = [][]string{
	[]string{
		"-type", "create", "-bucket", "default", "-index", "iname",
		"-fields", "name",
	},
	[]string{
		"-type", "count", "-bucket", "default", "-index", "iname",
		"-low", `["A"]`, "-high", `["Z"]`, "-limit", "10000",
	},
}
