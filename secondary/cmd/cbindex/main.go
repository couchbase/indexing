package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"syscall"

	c "github.com/couchbase/indexing/secondary/common"

	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/querycmd"
	qclient "github.com/couchbase/indexing/secondary/queryport/client"
)

func usage(fset *flag.FlagSet) {
	fmt.Fprintln(os.Stderr, "Usage: cbindex [options]")
	fset.PrintDefaults()
	fmt.Fprintln(os.Stderr, `Examples:

- Scan
    cbindex -type=scanAll -bucket default -index abcd
    cbindex -type=scanAll -index abcd -limit 0
    cbindex -type=scanAll -index abcd -limit 0 -consistency true
    cbindex -type=scan -index state -low='["Ar"]' -high='["Co"]' -buffersz=300
    cbindex -type=scan -index name_state_age -low='["Ar"]' -high='["Arlette", "N"]'
    cbindex -type scan -index '#primary' -equal='["Adena_54605074"]'

- Create/Drop
    cbindex -type create -bucket default -using memdb -index first_name -fields=first_name,last_name
    cbindex -type create -bucket default -primary=true -index primary
    cbindex -type drop -instanceid 1234

- List
    cbindex -type list
    cbindex -type nodes

- Move
    Single Index:
    cbindex -type move -index 'def_airportname' -bucket default -with '{"nodes":"10.17.6.32:8091"}'

    Index And 1 Replica:
    ./cbindex -type move -index 'def_airportname' -bucket default -with '{"nodes":["10.17.6.32:8091","10.17.6.33:8091"]}'

    Move Index supports moving only 1 index(and its replicas) at a time.

- Misc
    cbindex -par 100 -duration 10 benchmark
    `)
}

func main() {
	logging.SetLogLevel(logging.Warn)
	runtime.GOMAXPROCS(runtime.NumCPU())

	cmdOptions, _, fset, err := querycmd.ParseArgs(os.Args[1:])
	if err != nil {
		logging.Fatalf("%v\n", err)
		os.Exit(1)
	} else if cmdOptions.Help || len(cmdOptions.OpType) < 1 {
		usage(fset)
		os.Exit(0)
	}

	if os.Getenv("CBAUTH_REVRPC_URL") == "" && cmdOptions.Auth != "" {
		// unfortunately, above is read at init, so we have to respawn
		revrpc := fmt.Sprintf("http://%v@%v/query2", cmdOptions.Auth, cmdOptions.Server)
		os.Setenv("CBAUTH_REVRPC_URL", revrpc)
		cmd := exec.Command(os.Args[0], os.Args[1:]...)
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		exitcode := 0
		if err := cmd.Run(); err != nil {
			if status, ok := err.(*exec.ExitError); ok {
				exitcode = status.Sys().(syscall.WaitStatus).ExitStatus()
			}
		}
		os.Exit(exitcode)
	}

	config := c.SystemConfig.SectionConfig("queryport.client.", true)
	client, err := qclient.NewGsiClient(cmdOptions.Server, config)
	if err != nil {
		logging.Fatalf("%v\n", err)
		os.Exit(1)
	}

	if err = querycmd.HandleCommand(client, cmdOptions, false, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "Error occured %v\n", err)
	}
	client.Close()
}
