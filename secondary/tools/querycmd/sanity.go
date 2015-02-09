package main

import "fmt"
import "time"
import "math/rand"

import c "github.com/couchbase/indexing/secondary/common"
import qclient "github.com/couchbase/indexing/secondary/queryport/client"

//----------------------------------
// sanity check for queryport client
//----------------------------------

func doSanityTests(cluster string, client *qclient.GsiClient) (err error) {
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

	sanityCommands = fixDeployments(adminports, sanityCommands)
	for _, args := range sanityCommands {
		cmd, _ := parseArgs(args)
		if err = handleCommand(client, cmd, true); err != nil {
			fmt.Printf("%#v\n", cmd)
			fmt.Printf("    %v\n", err)
		}
		fmt.Println()
	}
	return
}

func fixDeployments(adminports []string, commands [][]string) [][]string {
	cmds := make([][]string, 0, len(commands))
	for i, cmd := range commands {
		if cmd[0] == "-type" && cmd[1] == "create" {
			rnd := rand.Intn(100000)
			n := (rnd / (10000 / len(adminports))) % len(adminports)
			switch cmd[5] {
			case "index-abv":
				f := "{\"nodes\": [%q], \"defer_build\": true}"
				with := fmt.Sprintf(f, adminports[n])
				cmd = append(cmd, "-with", with)

			default:
				with := fmt.Sprintf("{\"nodes\": [%q]}", adminports[n])
				cmd = append(cmd, "-with", with)
			}
		}
		cmds = append(cmds, cmd)
	}
	return cmds
}

func getIndexerAdminports(
	cinfo *c.ClusterInfoCache) ([]string, error) {

	iAdminports := make([]string, 0)
	for _, node := range cinfo.GetNodesByServiceType("indexAdmin") {
		adminport, err := cinfo.GetServiceAddress(node, "indexAdmin")
		if err != nil {
			return nil, err
		}
		iAdminports = append(iAdminports, adminport)
	}
	return iAdminports, nil
}

var sanityCommands = [][]string{
	[]string{
		"-type", "nodes",
	},
	[]string{
		"-type", "create", "-bucket", "beer-sample", "-index", "index-city",
		"-fields", "city",
	},
	[]string{
		"-type", "create", "-bucket", "beer-sample", "-index", "index-abv",
		"-fields", "abv",
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
