//go:build nolint

package main

import (
	"fmt"
	"time"

	qclient "github.com/couchbase/indexing/secondary/queryport/client"
)

func doBenchtimeit(cluster string, client *qclient.GsiClient) (err error) {
	start := time.Now()
	for i := 0; i < 1000000; i++ {
		client.Bridge().Timeit(0x1111, 1)
	}
	fmt.Printf("time take by Timeit(): %v\n", time.Since(start)/1000000)
	return
}
