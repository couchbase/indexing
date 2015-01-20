package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/couchbase/cbauth"
	c "github.com/couchbase/indexing/secondary/common"
	mclient "github.com/couchbase/indexing/secondary/manager/client"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/query"
	"github.com/couchbase/indexing/secondary/queryport"
	qclient "github.com/couchbase/indexing/secondary/queryport/client"
	"github.com/couchbaselabs/goprotobuf/proto"
	"github.com/couchbaselabs/query/expression"
	"github.com/couchbaselabs/query/parser/n1ql"
)

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
	fset.StringVar(&cmdOptions.server, "server", "localhost:9000", "Cluster server address")
	fset.StringVar(&cmdOptions.opType, "type", "scanAll", "Index command (scan|stats|scanAll|count|nodes|create|build|drop|list)")
	fset.StringVar(&cmdOptions.indexName, "index", "", "Index name")
	fset.StringVar(&cmdOptions.bucket, "bucket", "default", "Bucket name")
	fset.StringVar(&cmdOptions.auth, "auth", "Administrator:asdasd", "Auth user and password")
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

	// setup cbauth
	up := strings.Split(cmdOptions.auth, ":")
	authU, authP := up[0], up[1]
	authURL := fmt.Sprintf("http://%s/_cbauth", cmdOptions.server)
	rpcURL := fmt.Sprintf("http://%s/index", cmdOptions.server)
	c.MaybeSetEnv("NS_SERVER_CBAUTH_RPC_URL", rpcURL)
	c.MaybeSetEnv("NS_SERVER_CBAUTH_USER", authU)
	c.MaybeSetEnv("NS_SERVER_CBAUTH_PWD", authP)
	cbauth.Default = cbauth.NewDefaultAuthenticator(authURL, nil)

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
			err = runSanityTests(client)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error occured %v\n", err)
			}
		case "benchmark":
			benchmark(cmdOptions.server, "localhost:9101")
		}

	} else {
		err = handleCommand(client, cmdOptions, scanCallback)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error occured %v\n", err)
		}
	}
	client.Close()
}

func handleCommand(
	client *qclient.GsiClient, cmd *Command,
	callb qclient.ResponseHandler) (err error) {

	iname, bucket, limit := cmd.indexName, cmd.bucket, cmd.limit
	low, high, equal, incl := cmd.low, cmd.high, cmd.equal, cmd.inclusion

	indexes, err := client.Refresh()

	switch cmd.opType {
	case "nodes":
		fmt.Println("List of nodes:")
		nodes, err := client.Nodes()
		if err != nil {
			log.Fatal(err)
		}
		for adminport, queryport := range nodes {
			fmt.Println("    %v : scan %v", adminport, queryport)
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
		var defnID c.IndexDefnId
		if len(cmd.secStrs) == 0 && !cmd.isPrimary || cmd.indexName == "" {
			return fmt.Errorf("createIndex(): required fields missing")
		}
		defnID, err = client.CreateIndex(
			iname, bucket, cmd.using, cmd.exprType,
			cmd.partnStr, cmd.whereStr, cmd.secStrs, cmd.isPrimary,
			[]byte(cmd.with))
		if err == nil {
			fmt.Printf("Index created: %v\n", defnID)
		}

	case "build":
		defnIDs := make([]c.IndexDefnId, 0, len(cmd.bindexes))
		for _, bindex := range cmd.bindexes {
			v := strings.Split(bindex, ".")
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
		if cmd.equal != nil {
			equals := []c.SecondaryKey{cmd.equal}
			client.Lookup(uint64(defnID), equals, false, limit, callb)

		} else {
			err = client.Range(
				uint64(defnID), low, high, incl, false, limit, callb)
		}
		if err == nil {
			fmt.Println("Scan results:")
		}

	case "scanAll":
		defnID, _ := getDefnID(client, bucket, iname)
		err = client.ScanAll(uint64(defnID), limit, callb)
		if err == nil {
			fmt.Println("ScanAll results:")
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
			equals := []c.SecondaryKey{cmd.equal}
			count, err := client.CountLookup(uint64(defnID), equals)
			if err == nil {
				fmt.Printf("Index %q/%q has %v entries\n", bucket, iname, count)
				fmt.Println("CountLookup results:")
			}

		} else {
			count, err = client.CountRange(uint64(defnID), low, high, incl)
			if err == nil {
				fmt.Printf("Index %q/%q has %v entries\n", bucket, iname, count)
				fmt.Println("CountRange results:")
			}
		}

	}
	return err
}

func scanCallback(res qclient.ResponseReader) bool {
	if res.Error() != nil {
		fmt.Println("Error: ", res)
	} else if skeys, pkeys, err := res.GetEntries(); err != nil {
		fmt.Println("Error: ", err)
	} else {
		for i, pkey := range pkeys {
			fmt.Printf("%v ... %v\n", skeys[i], string(pkey))
		}
	}
	return true
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
	fmt.Printf("Index:%s/%s, Id:%v, Using:%s, Exprs:%v, isPrimary:%v\n",
		defn.Name, defn.Bucket, defn.DefnId, defn.Using, defn.SecExprs,
		defn.IsPrimary)
}

func getDefnID(
	client *qclient.GsiClient,
	bucket, indexName string) (defnID c.IndexDefnId, ok bool) {

	indexes, err := client.Refresh()
	if err != nil {
		log.Fatal(err)
	}
	for _, index := range indexes {
		defn := index.Definition
		if defn.Bucket == bucket && defn.Name == indexName {
			return index.Definition.DefnId, true
		}
	}
	return c.IndexDefnId(0), false
}

//----------------------------------
// sanity check for queryport client
//----------------------------------

func runSanityTests(client *qclient.GsiClient) (err error) {
	for _, args := range sanityCommands {
		entries := 0
		callb := func(res qclient.ResponseReader) bool {
			entries++
			return true
		}
		cmd, _ := parseArgs(args)
		if err = handleCommand(client, cmd, callb); err != nil {
			fmt.Printf("%#v\n", cmd)
			fmt.Printf("    %v\n", err)
		} else {
			fmt.Printf("    Success ... entries:%v\n", entries)
		}
		fmt.Println()
	}
	return
}

var sanityCommands = [][]string{
	[]string{
		"-type", "create", "-bucket", "beer-sample", "-index", "index-city",
		"-fields", "city",
	},
	[]string{"-type", "list", "-bucket", "beer-sample"},
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
		"-type", "drop", "-bucket", "beer-sample", "-index", "index-city",
	},
}

//--------------------
// benchmark queryport
//--------------------

func benchmark(cluster, addr string) {
	qconf := c.SystemConfig.SectionConfig("queryport.indexer.", true)
	s, err := queryport.NewServer(addr, serverCallb, qconf)
	if err != nil {
		log.Fatal(err)
	}
	loopback(cluster, addr)
	s.Close()
}

func loopback(cluster, raddr string) {
	qconf := c.SystemConfig.SectionConfig("queryport.client.", true)
	qconf.SetValue("poolSize", 10)
	qconf.SetValue("poolOverflow", mock_nclients)
	client, err := qclient.NewGsiClient(cluster, "querycmd", qconf)
	if err != nil {
		log.Fatal(err)
	}
	quitch := make(chan int)
	for i := 0; i < mock_nclients; i++ {
		t := time.After(time.Duration(mock_duration) * time.Second)
		go runClient(client, t, quitch)
	}

	count := 0
	for i := 0; i < mock_nclients; i++ {
		n := <-quitch
		count += n
	}

	client.Close()
	fmt.Printf("Completed %v queries in %v seconds\n", count, mock_duration)
}

func runClient(client *qclient.GsiClient, t <-chan time.Time, quitch chan<- int) {
	count := 0

loop:
	for {
		select {
		case <-t:
			quitch <- count
			break loop

		default:
			l, h := c.SecondaryKey{[]byte("aaaa")}, c.SecondaryKey{[]byte("zzzz")}
			err := client.Range(
				0xABBA /*defnID*/, l, h, 100, true, 1000,
				func(val qclient.ResponseReader) bool {
					switch v := val.(type) {
					case *protobuf.ResponseStream:
						count++
						if reflect.DeepEqual(v, testResponseStream) == false {
							log.Fatal("failed on testResponseStream")
						}
					case error:
						log.Println(v)
					}
					return true
				})
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

func serverCallb(
	req interface{}, respch chan<- interface{}, quitch <-chan interface{}) {

	switch req.(type) {
	case *protobuf.StatisticsRequest:
		resp := testStatisticsResponse
		select {
		case respch <- resp:
			close(respch)

		case <-quitch:
			log.Fatal("unexpected quit", req)
		}

	case *protobuf.ScanRequest:
		sendResponse(1, respch, quitch)
		close(respch)

	case *protobuf.ScanAllRequest:
		sendResponse(1, respch, quitch)
		close(respch)
	}
}

func sendResponse(
	count int, respch chan<- interface{}, quitch <-chan interface{}) {

	i := 0
loop:
	for ; i < count; i++ {
		select {
		case respch <- testResponseStream:
		case <-quitch:
			break loop
		}
	}
}
