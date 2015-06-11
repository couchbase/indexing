package main

import "fmt"
import "log"
import "os"
import "time"

import qclient "github.com/couchbase/indexing/secondary/queryport/client"
import "github.com/couchbase/indexing/secondary/querycmd"

func doGsiRR(cluster string, client *qclient.GsiClient) (err error) {
	for i := 0; i < 100; i++ {
		for _, args := range rr_cmds {
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
			time.Sleep(200 * time.Millisecond)
		}
	}
	return
}

var rr_cmds = [][]string{
	[]string{
		"-type", "scanAll", "-bucket", "default", "-index", "index1",
		"-limit", "1",
	},
	[]string{
		"-type", "scanAll", "-bucket", "default", "-index", "index2",
		"-limit", "1",
	},
}
