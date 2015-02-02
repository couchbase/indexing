package main

import "encoding/json"
import "flag"
import "fmt"
import "log"
import "os"
import "strings"
import "errors"
import "time"

import "github.com/couchbase/cbauth"
import c "github.com/couchbase/indexing/secondary/common"
import mclient "github.com/couchbase/indexing/secondary/manager/client"
import protobuf "github.com/couchbase/indexing/secondary/protobuf/query"
import qclient "github.com/couchbase/indexing/secondary/queryport/client"
import "github.com/couchbaselabs/goprotobuf/proto"
import "github.com/couchbaselabs/query/expression"
import "github.com/couchbaselabs/query/parser/n1ql"

const (
	ExprType = "N1QL"
	where    = ""
)

var trace bool
var debug bool
var info bool
var mock_nclients int
var mock_duration int

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

type Command struct {
	opType string
	// basic options.
	server    string
	indexName string
	bucket    string
	adminPort string
	queryPort string
	auth      string
	// options for create-index.
	using     string
	exprType  string
	partnStr  string
	whereStr  string
	secStrs   []string
	isPrimary bool
	with      string
	withPlan  map[string]interface{}
	// options for build index
	bindexes []string
	// options for Range, Statistics, Count
	low       c.SecondaryKey
	high      c.SecondaryKey
	equal     c.SecondaryKey
	inclusion qclient.Inclusion
	limit     int64
}

func parseArgs(arguments []string) (*Command, []string) {
	var fields, bindexes string
	var inclusion uint
	var equal, low, high string

	cmdOptions := &Command{}
	fset := flag.NewFlagSet("cmd", flag.ExitOnError)

	// basic options
	fset.StringVar(&cmdOptions.server, "server", "127.0.0.1:9000", "Cluster server address")
	fset.StringVar(&cmdOptions.opType, "type", "scanAll", "Index command (scan|stats|scanAll|count|nodes|create|build|drop|list)")
	fset.StringVar(&cmdOptions.indexName, "index", "", "Index name")
	fset.StringVar(&cmdOptions.bucket, "bucket", "default", "Bucket name")
	fset.StringVar(&cmdOptions.auth, "auth", "", "Auth user and password")
	// options for create-index
	fset.StringVar(&cmdOptions.using, "using", "gsi", "using clause for create index")
	fset.StringVar(&cmdOptions.exprType, "exprType", "N1QL", "type of expression for create index")
	fset.StringVar(&cmdOptions.partnStr, "partn", "", "partition expression for create index")
	fset.StringVar(&cmdOptions.whereStr, "where", "", "where clause for create index")
	fset.StringVar(&fields, "fields", "", "Comma separated on-index fields") // secStrs
	fset.BoolVar(&cmdOptions.isPrimary, "primary", false, "Is primary index")
	fset.StringVar(&cmdOptions.with, "with", "", "index specific properties")
	// options for build-index
	fset.StringVar(&bindexes, "indexes", "", "csv list of bucket.index to build")
	// options for Range, Statistics, Count
	fset.StringVar(&low, "low", "[]", "Span.Range: [low]")
	fset.StringVar(&high, "high", "[]", "Span.Range: [high]")
	fset.StringVar(&equal, "equal", "", "Span.Lookup: [key]")
	fset.UintVar(&inclusion, "incl", 0, "Range: 0|1|2|3")
	fset.Int64Var(&cmdOptions.limit, "limit", 10, "Row limit")
	// options for logging
	fset.BoolVar(&debug, "debug", false, "run in debug mode")
	fset.BoolVar(&trace, "trace", false, "run in trace mode")
	fset.BoolVar(&info, "info", false, "run in info mode")
	// options for benchmark
	fset.IntVar(&mock_nclients, "par", 1, "number of parallel clients to use for benchmark")
	fset.IntVar(&mock_duration, "duration", 1, "seconds to profile")

	if err := fset.Parse(arguments); err != nil {
		log.Println(arguments)
		log.Fatal(err)
	}
	if len(bindexes) > 0 {
		cmdOptions.bindexes = strings.Split(bindexes, ",")
	}

	cmdOptions.inclusion = qclient.Inclusion(inclusion)
	cmdOptions.secStrs = make([]string, 0)
	if fields != "" {
		for _, field := range strings.Split(fields, ",") {
			expr, err := n1ql.ParseExpression(field)
			if err != nil {
				fmt.Printf("Error occured: Invalid field (%v) %v\n", field, err)
				os.Exit(1)
			}
			secStr := expression.NewStringer().Visit(expr)
			cmdOptions.secStrs = append(cmdOptions.secStrs, secStr)
		}
	}
	if equal != "" {
		cmdOptions.equal = c.SecondaryKey(arg2key([]byte(equal)))
	}
	cmdOptions.low = c.SecondaryKey(arg2key([]byte(low)))
	cmdOptions.high = c.SecondaryKey(arg2key([]byte(high)))

	if len(cmdOptions.with) > 0 {
		err := json.Unmarshal([]byte(cmdOptions.with), &cmdOptions.withPlan)
		if err != nil {
			log.Fatal(err)
		}
	}

	// setup cbauth
	if cmdOptions.auth != "" {
		up := strings.Split(cmdOptions.auth, ":")
		_, err := cbauth.InternalRetryDefaultInit(cmdOptions.server, up[0], up[1])
		if err != nil {
			log.Fatalf("Failed to initialize cbauth: %s", err)
		}
	}

	return cmdOptions, fset.Args()
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s -type scanAll -index idx1 -bucket default\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	cmdOptions, args := parseArgs(os.Args[1:])

	if debug {
		c.SetLogLevel(c.LogLevelDebug)
	} else if trace {
		c.SetLogLevel(c.LogLevelTrace)
	} else if info {
		c.SetLogLevel(c.LogLevelInfo)
	}

	config := c.SystemConfig.SectionConfig("queryport.client.", true)
	client, err := qclient.NewGsiClient(
		cmdOptions.server, "querycmd", config)
	if err != nil {
		log.Fatal(err)
	}

	if len(args) > 0 {
		switch args[0] {
		case "sanity":
			err = doSanityTests(cmdOptions.server, client)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error occured %v\n", err)
			}
		case "benchmark":
			doBenchmark(cmdOptions.server, "localhost:9101")
		}

	} else {
		err = handleCommand(client, cmdOptions, false)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error occured %v\n", err)
		}
	}
	client.Close()
}

func handleCommand(
	client *qclient.GsiClient, cmd *Command, sanity bool) (err error) {

	iname, bucket, limit := cmd.indexName, cmd.bucket, cmd.limit
	low, high, equal, incl := cmd.low, cmd.high, cmd.equal, cmd.inclusion

	indexes, err := client.Refresh()

	entries := 0
	callb := func(res qclient.ResponseReader) bool {
		if res.Error() != nil {
			fmt.Println("Error: ", res)
		} else if skeys, pkeys, err := res.GetEntries(); err != nil {
			fmt.Println("Error: ", err)
		} else {
			if sanity == false {
				for i, pkey := range pkeys {
					fmt.Printf("%v ... %v\n", skeys[i], string(pkey))
				}
			}
			entries += len(pkeys)
		}
		return true
	}

	switch cmd.opType {
	case "nodes":
		fmt.Println("List of nodes:")
		nodes, err := client.Nodes()
		if err != nil {
			log.Fatal(err)
		}
		for adminport, queryport := range nodes {
			fmt.Printf("    {%v, %v}\n", adminport, queryport)
		}

	case "list":
		if err != nil {
			return err
		}
		fmt.Println("List of indexes:")
		for _, index := range indexes {
			printIndexInfo(index)
		}

	case "create":
		var defnID uint64
		var states []c.IndexState
		if len(cmd.secStrs) == 0 && !cmd.isPrimary || cmd.indexName == "" {
			return fmt.Errorf("createIndex(): required fields missing")
		}
		defnID, err = client.CreateIndex(
			iname, bucket, cmd.using, cmd.exprType,
			cmd.partnStr, cmd.whereStr, cmd.secStrs, cmd.isPrimary,
			[]byte(cmd.with))
		if err == nil {
			fmt.Printf("Index created: %v with %q\n", defnID, cmd.with)
			if d, ok := cmd.withPlan["defer_build"]; !(ok && d.(bool)) {
				states, err = waitUntilIndexState(
					client, []uint64{defnID}, c.INDEX_STATE_ACTIVE,
					100 /*period*/, 20000 /*timeout*/)
				if err == nil {
					fmt.Println("Index state:", states[0])
				}
			}
		}

	case "build":
		var states []c.IndexState
		defnIDs := make([]uint64, 0, len(cmd.bindexes))
		for _, bindex := range cmd.bindexes {
			v := strings.Split(bindex, ":")
			if len(v) < 0 {
				return fmt.Errorf("Invalid index specified : %v", bindex)
			}
			bucket, iname = v[0], v[1]
			defnID, ok := getDefnID(client, bucket, iname)
			if ok {
				defnIDs = append(defnIDs, defnID)
			} else {
				err = fmt.Errorf("index %v/%v unknown", bucket, iname)
				break
			}
		}
		if err == nil {
			err = client.BuildIndexes(defnIDs)
			fmt.Printf("Index building for: %v\n", defnIDs)
			states, err = waitUntilIndexState(
				client, defnIDs, c.INDEX_STATE_ACTIVE,
				100 /*period*/, 20000 /*timeout*/)
			if err == nil {
				for i, defnID := range defnIDs {
					fmt.Printf("Index state for %v: %v", defnID, states[i])
				}
			}
		}

	case "drop":
		defnID, ok := getDefnID(client, bucket, iname)
		if ok {
			err = client.DropIndex(defnID)
			if err == nil {
				fmt.Println("Index dropped")
			}
		} else {
			err = fmt.Errorf("index %v/%v unknown", bucket, iname)
		}

	case "scan":
		defnID, _ := getDefnID(client, bucket, iname)
		fmt.Println("Scan index:")
		if cmd.equal != nil {
			equals := []c.SecondaryKey{cmd.equal}
			client.Lookup(uint64(defnID), equals, false, limit, callb)

		} else {
			err = client.Range(
				uint64(defnID), low, high, incl, false, limit, callb)
		}
		if err == nil {
			fmt.Println("Tota number of entries: ", entries)
		}

	case "scanAll":
		defnID, _ := getDefnID(client, bucket, iname)
		fmt.Println("ScanAll index:")
		err = client.ScanAll(uint64(defnID), limit, callb)
		if err == nil {
			fmt.Println("Tota number of entries: ", entries)
		}

	case "stats":
		var statsResp c.IndexStatistics
		defnID, _ := getDefnID(client, bucket, iname)
		if cmd.equal != nil {
			statsResp, err = client.LookupStatistics(uint64(defnID), equal)
		} else {
			statsResp, err = client.RangeStatistics(
				uint64(defnID), low, high, incl)
		}
		if err == nil {
			fmt.Println("Stats: ", statsResp)
		}

	case "count":
		var count int64

		defnID, _ := getDefnID(client, bucket, iname)
		if cmd.equal != nil {
			fmt.Println("CountLookup:")
			equals := []c.SecondaryKey{cmd.equal}
			count, err := client.CountLookup(uint64(defnID), equals)
			if err == nil {
				fmt.Printf("Index %q/%q has %v entries\n", bucket, iname, count)
			}

		} else {
			fmt.Println("CountRange:")
			count, err = client.CountRange(uint64(defnID), low, high, incl)
			if err == nil {
				fmt.Printf("Index %q/%q has %v entries\n", bucket, iname, count)
			}
		}

	}
	return err
}

func arg2key(arg []byte) []interface{} {
	var key []interface{}
	if err := json.Unmarshal(arg, &key); err != nil {
		log.Fatal(err)
	}
	return key
}

func printIndexInfo(index *mclient.IndexMetadata) {
	defn := index.Definition
	insts := index.Instances
	fmt.Printf("Index:%s/%s, Id:%v, State:%s, Using:%s, Exprs:%v, isPrimary:%v\n",
		defn.Name, defn.Bucket, defn.DefnId, insts[0].State, defn.Using, defn.SecExprs,
		defn.IsPrimary)
}

func getDefnID(
	client *qclient.GsiClient,
	bucket, indexName string) (defnID uint64, ok bool) {

	indexes, err := client.Refresh()
	if err != nil {
		log.Fatal(err)
	}
	for _, index := range indexes {
		defn := index.Definition
		if defn.Bucket == bucket && defn.Name == indexName {
			return uint64(index.Definition.DefnId), true
		}
	}
	return 0, false
}

func waitUntilIndexState(
	client *qclient.GsiClient, defnIDs []uint64,
	state c.IndexState, period, timeout time.Duration) ([]c.IndexState, error) {

	expired := time.After(timeout * time.Millisecond)
	states := make([]c.IndexState, len(defnIDs))
	pending := len(defnIDs)
	for {
		select {
		case <-expired:
			return nil, errors.New("timeout")

		default:
		}
		for i, defnID := range defnIDs {
			if states[i] != state {
				st, err := client.IndexState(defnID)
				if err != nil {
					return nil, err
				} else if st == state {
					states[i] = state
					pending--
					continue
				}
			}
		}
		if pending == 0 {
			return states, nil
		}
		time.Sleep(period * time.Millisecond)
	}
}
