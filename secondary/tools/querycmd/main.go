package main

import "flag"
import "fmt"
import "log"
import "os"

import c "github.com/couchbase/indexing/secondary/common"
import "github.com/couchbase/indexing/secondary/querycmd"
import protobuf "github.com/couchbase/indexing/secondary/protobuf/query"
import qclient "github.com/couchbase/indexing/secondary/queryport/client"
import "github.com/couchbaselabs/goprotobuf/proto"

var testStatisticsResponse = &protobuf.StatisticsResponse{
	Stats: &protobuf.IndexStatistics{
		KeysCount:       proto.Uint64(100),
		UniqueKeysCount: proto.Uint64(100),
		KeyMin:          []byte(`"aaaaa"`),
		KeyMax:          []byte(`"zzzzz"`),
	},
}
var testResponseStream = &protobuf.ResponseStream{
	IndexEntries: []*protobuf.IndexEntry{
		&protobuf.IndexEntry{
			EntryKey: []byte(`["aaaaa"]`), PrimaryKey: []byte("key1"),
		},
		&protobuf.IndexEntry{
			EntryKey: []byte(`["aaaaa"]`), PrimaryKey: []byte("key2"),
		},
	},
}

func usage(fset *flag.FlagSet) {
	fmt.Fprintf(os.Stderr, "Usage: %s (sanity|bench|...)\n", os.Args[0])
}

func main() {
	cmdOptions, args, fset, err := querycmd.ParseArgs(os.Args[1:])
	if err != nil {
		log.Fatal(err)
	} else if cmdOptions.Help {
		usage(fset)
		os.Exit(0)
	} else if len(args) < 1 {
		log.Fatalf("specify a command")
	}

	config := c.SystemConfig.SectionConfig("queryport.client.", true)
	client, err := qclient.NewGsiClient(cmdOptions.Server, config)
	if err != nil {
		log.Fatal(err)
	}

	switch args[0] {
	case "sanity":
		err = doSanityTests(cmdOptions.Server, client)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error occured %v\n", err)
		}

	case "mb13339":
		err = doMB13339(cmdOptions.Server, client)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error occured %v\n", err)
		}

	case "benchmark":
		doBenchmark(cmdOptions.Server, "localhost:9101")
	}
	client.Close()
}
