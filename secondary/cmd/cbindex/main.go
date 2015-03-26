package main

import "flag"
import "fmt"
import "os"
import "runtime"

import c "github.com/couchbase/indexing/secondary/common"
import qclient "github.com/couchbase/indexing/secondary/queryport/client"
import "github.com/couchbase/indexing/secondary/logging"
import "github.com/couchbase/indexing/secondary/querycmd"

func usage(fset *flag.FlagSet) {
	fmt.Fprintln(os.Stderr, "Usage: cbindex [options]")
	fset.PrintDefaults()
	fmt.Fprintln(os.Stderr, `Examples:
- Scan
    cbindex -type=scanAll -bucket default -index abcd
    cbindex -type=scanAll -index abcd -limit 0
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
- Misc
    cbindex -type config -ckey=indexer.settings.log_override -cvalue="goforestdb/*=Trace"
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
