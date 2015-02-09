package main

import "fmt"

import c "github.com/couchbase/indexing/secondary/common"
import qclient "github.com/couchbase/indexing/secondary/queryport/client"

// test case to simulate
// https://issues.couchbase.com/browse/MB-13339

func doMB13339(cluster string, client *qclient.GsiClient) (err error) {
	cinfo, err :=
		c.NewClusterInfoCache(c.ClusterUrl(cluster), "default" /*pooln*/)
	if err != nil {
		return err
	}
	if err = cinfo.Fetch(); err != nil {
		return err
	}
	adminports, err := getIndexerAdminports(cinfo)
	if err != nil {
		return err
	}

	mb13339Commands = fixDeployments(adminports, mb13339Commands)
	for _, args := range mb13339Commands {
		cmd, _ := parseArgs(args)
		if err = handleCommand(client, cmd, true); err != nil {
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
