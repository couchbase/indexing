package main

import (
	"flag"
	"fmt"
	"os"

	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/protobuf"
	"github.com/couchbase/indexing/secondary/queryport"
)

var (
	server   string
	scanType string

	indexName string
	bucket    string

	low   string
	high  string
	equal string
	incl  uint

	limit    int64
	pageSize int64
)

func parseArgs() {
	flag.StringVar(&server, "server", "localhost:7000", "query server address")
	flag.StringVar(&scanType, "type", "scanAll", "Scan command")
	flag.StringVar(&indexName, "index", "", "Index name")
	flag.StringVar(&bucket, "bucket", "default", "Bucket name")
	flag.StringVar(&low, "low", "[]", "Range: []")
	flag.StringVar(&high, "high", "[]", "Range: []")
	flag.StringVar(&equal, "equal", "", "Range: \"\"")
	flag.UintVar(&incl, "incl", 0, "Range: 0|1|2|3")
	flag.Int64Var(&limit, "limit", 10, "Row limit")
	flag.Int64Var(&pageSize, "buffersz", 0, "Rows buffer size per internal message")

	flag.Parse()
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s -type scanAll -index idx1 -bucket default\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	var err error
	var statsResp *protobuf.IndexStatistics
	var keys [][]byte

	parseArgs()

	if indexName == "" {
		usage()
		os.Exit(1)
	}

	client := queryport.NewClient(server, c.SystemConfig)
	if equal != "" {
		keys = append(keys, []byte(equal))
	}

	switch scanType {
	case "scan":
		err = client.Scan(indexName, bucket, []byte(low), []byte(high), keys, uint32(incl), pageSize, false, limit, scanCallback)
	case "scanAll":
		err = client.ScanAll(indexName, bucket, pageSize, limit, scanCallback)
	case "stats":
		statsResp, err = client.Statistics(indexName, bucket, []byte(low), []byte(high), keys, uint32(incl))
		if err == nil {
			fmt.Println("Stats: ", statsResp)
		}
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error occured %v\n", err)
	}

	client.Close()
}

func scanCallback(res interface{}) bool {
	switch r := res.(type) {
	case *protobuf.ResponseStream:
		fmt.Println("StreamResponse: ", res.(*protobuf.ResponseStream).String())
	case error:
		fmt.Println("Error: ", r)
	}
	return true
}
