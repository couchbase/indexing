package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	c "github.com/couchbase/indexing/secondary/common"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/query"
	queryclient "github.com/couchbase/indexing/secondary/queryport/client"
	"github.com/couchbaselabs/query/expression"
	"github.com/couchbaselabs/query/parser/n1ql"
)

var (
	server string
	opType string

	indexName string
	bucket    string

	low   string
	high  string
	equal string
	incl  uint

	limit    int64
	pageSize int64

	fields     string
	isPrimary  bool
	instanceId string
)

const (
	using    = "lsm"
	exprType = "N1QL"
	partnExp = ""
	where    = ""
)

func parseArgs() {
	flag.StringVar(&server, "server", "localhost:9101", "index server or scan server address")
	flag.StringVar(&opType, "type", "scanAll", "Index command (scan|stats|scanAll|create|drop|list)")
	flag.StringVar(&indexName, "index", "", "Index name")
	flag.StringVar(&bucket, "bucket", "default", "Bucket name")
	flag.StringVar(&low, "low", "[]", "Range: [low]")
	flag.StringVar(&high, "high", "[]", "Range: [high]")
	flag.StringVar(&equal, "equal", "", "Range: [key]")
	flag.UintVar(&incl, "incl", 0, "Range: 0|1|2|3")
	flag.Int64Var(&limit, "limit", 10, "Row limit")
	flag.Int64Var(&pageSize, "buffersz", 0, "Rows buffer size per internal message")
	flag.StringVar(&fields, "fields", "", "Comma separated on-index fields")
	flag.BoolVar(&isPrimary, "primary", false, "Is primary index")
	flag.StringVar(&instanceId, "instanceid", "", "Index instanceId")

	flag.Parse()
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s -type scanAll -index idx1 -bucket default\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	var err error
	var statsResp c.IndexStatistics
	var keys []interface{}

	parseArgs()

	switch opType {

	case "create":

		if !isPrimary && (fields == "" || indexName == "") {
			fmt.Println("Invalid fields or index name")
			usage()
			os.Exit(1)
		}

		client := queryclient.NewClusterClient(server)
		var secExprs []string

		if fields != "" {
			fields := strings.Split(fields, ",")
			for _, field := range fields {
				expr, err := n1ql.ParseExpression(field)
				if err != nil {
					fmt.Printf("Error occured: Invalid field (%v) %v\n", field, err)
					os.Exit(1)
				}

				secExprs = append(secExprs, expression.NewStringer().Visit(expr))
			}
		}

		info, err := client.CreateIndex(indexName, bucket, using, exprType, partnExp, where, secExprs, isPrimary)
		if err == nil {
			fmt.Println("Index created")
			printIndexInfo(*info)
		} else {
			fmt.Println("Error occured:", err)
		}

	case "drop":
		if instanceId == "" {
			fmt.Println("Invalid instanceId")
			usage()
			os.Exit(1)
		}

		client := queryclient.NewClusterClient(server)
		err := client.DropIndex(instanceId)
		if err == nil {
			fmt.Println("Index dropped")
		} else {
			fmt.Println("Error occured:", err)
		}
	case "list":
		client := queryclient.NewClusterClient(server)
		infos, err := client.List()
		if err != nil {
			fmt.Println("Error occured:", err)
		}

		fmt.Println("Indexes:")
		for _, info := range infos {
			printIndexInfo(info)
		}

	default:
		if indexName == "" {
			usage()
			os.Exit(1)
		}
		config := c.SystemConfig.SectionConfig("queryport.client.", true)
		client := queryclient.NewClient(queryclient.Remoteaddr(server), config)
		if equal != "" {
			keys = arg2key([]byte(equal))
		}

		inclusion := queryclient.Inclusion(incl)
		switch opType {
		case "scan":
			if keys == nil {
				l := c.SecondaryKey(arg2key([]byte(low)))
				h := c.SecondaryKey(arg2key([]byte(high)))
				err = client.Range(indexName, bucket, l, h, inclusion, false, limit, scanCallback)

			} else {
				err = client.Lookup(indexName, bucket, []c.SecondaryKey{keys}, false, limit, scanCallback)
			}
		case "scanAll":
			err = client.ScanAll(indexName, bucket, limit, scanCallback)
		case "stats":
			if keys == nil {
				l := c.SecondaryKey(arg2key([]byte(low)))
				h := c.SecondaryKey(arg2key([]byte(high)))
				statsResp, err = client.RangeStatistics(indexName, bucket, l, h, inclusion)
				if err == nil {
					fmt.Println("Stats: ", statsResp)
				}
			} else {
				statsResp, err = client.LookupStatistics(indexName, bucket, keys)
				if err == nil {
					fmt.Println("Stats: ", statsResp)
				}
			}
		}

		if err != nil {
			fmt.Fprintf(os.Stderr, "Error occured %v\n", err)
		}

		client.Close()
	}
}

func scanCallback(res queryclient.ResponseReader) bool {
	switch r := res.(type) {
	case *protobuf.ResponseStream:
		fmt.Println("StreamResponse: ", res.(*protobuf.ResponseStream).String())
	case error:
		fmt.Println("Error: ", r)
	}
	return true
}

func printIndexInfo(info queryclient.IndexInfo) {
	fmt.Printf("Index:%s/%s, Id:%s, Using:%s, Exprs:%v, isPrimary:%v\n",
		info.Name, info.Bucket, info.DefnID, info.Using, info.SecExprs, info.IsPrimary)
}

func arg2key(arg []byte) []interface{} {
	var key []interface{}
	if err := json.Unmarshal(arg, &key); err != nil {
		log.Fatal(err)
	}

	return key
}
