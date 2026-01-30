//go:build nolint

package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/couchbase/indexing/secondary/querycmd"
	qclient "github.com/couchbase/indexing/secondary/queryport/client"
)

// test case to simulate
// https://issues.couchbase.com/browse/MB-16115

func doMB16115(cluster string, client *qclient.GsiClient) (err error) {
	log.Println("====== Creating index ======")
	cr_cmd, _, _, err := querycmd.ParseArgs(mb16115commands[0])
	if err != nil {
		log.Fatal(err)
	}
	err = querycmd.HandleCommand(client, cr_cmd, true, os.Stdout)
	if err != nil {
		fmt.Printf("%#v\n", cr_cmd)
		fmt.Printf("    %v\n", err)
	}
	fmt.Println()

	log.Println("====== Querying index ======")
	cn_cmd, _, _, err := querycmd.ParseArgs(mb16115commands[1])
	if err != nil {
		log.Fatal(err)
	}
	wg := &sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			err = querycmd.HandleCommand(client, cn_cmd, true, os.Stdout)
			if err != nil {
				fmt.Printf("%#v\n", cn_cmd)
				fmt.Printf("    %v\n", err)
			}
			fmt.Println()
			wg.Done()
		}()
	}
	wg.Wait()

	log.Println("====== Killing indexer ======")
	cmd := exec.Command("pkill", "indexer")
	err = cmd.Run()
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(5 * time.Second)

	log.Println("====== Running the query with same client ======")
	err = querycmd.HandleCommand(client, cn_cmd, true, os.Stdout)
	if err != nil {
		fmt.Printf("%#v\n", cn_cmd)
		fmt.Printf("    %v\n", err)
	}
	fmt.Println()

	log.Println("====== Drop index ======")
	dl_cmd, _, _, err := querycmd.ParseArgs(mb16115commands[2])
	if err != nil {
		log.Fatal(err)
	}
	err = querycmd.HandleCommand(client, dl_cmd, true, os.Stdout)
	if err != nil {
		fmt.Printf("%#v\n", dl_cmd)
		fmt.Printf("    %v\n", err)
	}
	return
}

var mb16115commands = [][]string{
	[]string{
		"-type", "create", "-bucket", "beer-sample", "-index", "index_abv1",
		"-fields", "abv",
	},
	[]string{
		"-type", "count", "-bucket", "beer-sample", "-index", "index_abv1",
		"-low", "[1.0]", "-high", "[100.0]",
	},
	[]string{"-type", "drop", "-bucket", "beer-sample", "-index", "index_abv1"},
}
