//go:build nolint

package main

import (
	"flag"
	"log"
	"strings"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/indexer"
)

var options struct {
	bucket  string
	cluster string
	trials  int
	auth    string
}

func argParse() {
	flag.StringVar(&options.bucket, "bucket", "default",
		"bucket to connect")
	flag.StringVar(&options.cluster, "cluster", "127.0.0.1:9000",
		"cluster to connect")
	flag.StringVar(&options.auth, "auth", "Administrator:asdasd",
		"Auth user and password")
	flag.IntVar(&options.trials, "trials", 1,
		"try stats call for `n` trials")

	flag.Parse()
}

func main() {
	argParse()

	// setup cbauth
	if options.auth != "" {
		up := strings.Split(options.auth, ":")
		if _, err := cbauth.InternalRetryDefaultInit(options.cluster, up[0], up[1]); err != nil {
			log.Fatalf("Failed to initialize cbauth: %s", err)
		}
	}

	numVb := numVbuckets(options.cluster, options.bucket)
	log.Printf("bucket %q has %v vbuckets\n", options.bucket, numVb)

	bucketTs(options.cluster, options.bucket, numVb)
	//getCurrentKVTs_old(options.cluster, options.bucket, numVb)
	getCurrentKVTs(options.cluster, options.bucket, numVb)
}

func bucketTs(cluster, bucketn string, numVb int) {
	b, err := common.ConnectBucket(cluster, "default" /*pooln*/, bucketn)
	if err != nil {
		log.Fatal(err)
	}
	defer b.Close()

	start := time.Now()
	for i := 0; i < options.trials; i++ {
		if _, _, err = common.BucketTs(b, numVb); err != nil {
			log.Fatal(err)
		}
	}
	durtn := time.Since(start) / time.Duration(options.trials)
	log.Printf("bucketTs: %v\n", durtn)
}

//func getCurrentKVTs_old(cluster, bucketn string, numVb int) {
//    b, err := common.ConnectBucket(cluster, "default" /*pooln*/, bucketn)
//    if err != nil {
//        log.Fatal(err)
//    }
//    defer b.Close()
//
//    start := time.Now()
//    for i := 0; i < options.trials; i++ {
//        if _, err := indexer.GetCurrentKVTs(b, numVb); err != nil {
//            log.Fatal(err)
//        }
//    }
//    durtn := time.Since(start) / time.Duration(options.trials)
//    log.Printf("getCurrentKVTs: %v\n", durtn)
//}

func getCurrentKVTs(cluster, bucketn string, numVb int) {
	start := time.Now()
	for i := 0; i < options.trials; i++ {
		_, err := indexer.GetCurrentKVTs(cluster, "default", bucketn, numVb)
		if err != nil {
			log.Fatal(err)
		}
	}
	durtn := time.Since(start) / time.Duration(options.trials)
	log.Printf("getCurrentKVTs: %v\n", durtn)
}

func numVbuckets(cluster, bucketn string) (numVb int) {
	b, err := common.ConnectBucket(cluster, "default" /*pooln*/, bucketn)
	if err != nil {
		log.Fatal(err)
	}
	defer b.Close()
	if numVb, err = common.MaxVbuckets(b); err != nil {
		log.Fatal(err)
	}
	return numVb
}
