//go:build nolint

package main

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime/pprof"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/indexer"
)

func main() {

	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()

	f, _ := os.Create("cpu.prof")
	pprof.StartCPUProfile(f)
	do_test1()
	pprof.StopCPUProfile()

	f, _ = os.Create("mem.prof")
	pprof.WriteHeapProfile(f)
	f.Close()

}

func do_test1() {

	log.Println("***** TEST1 TWO KV STORE 5M INSERT EACH WITH GET *****")

	config := common.SystemConfig.SectionConfig(
		"indexer.", true /*trim*/)

	stats := &indexer.IndexStats{}
	stats.Init()

	slice, _ := indexer.NewForestDBSlice(".", 0, 1, 1, 1, false, 1, config, stats)

	log.Println("***** GENERATING *****")
	numItems := 5000000
	keys := make([][]byte, numItems)
	//lenkeys := make([]int, numItems)
	vals := make([][]byte, numItems)
	//lenvals := make([]int, numItems)
	for i := 0; i < numItems; i++ {
		keys[i] = []byte(fmt.Sprintf("perf%v", i))
		//lenkeys[i] = len(keys[i])
		vals[i] = []byte(fmt.Sprintf("body%v", i))
		//	lenvals[i] = len(vals[i])
	}

	time.Sleep(3 * time.Second)
	log.Println("***** LOADING*****")
	start := time.Now()
	for i := 0; i < numItems; i++ {
		slice.Insert(keys[i], vals[i], nil, nil)
	}
	//	dbfile.Commit(forestdb.COMMIT_MANUAL_WAL_FLUSH)
	slice.NewSnapshot(nil, false)

	elapsed := time.Since(start)
	log.Printf("***** RESULT : Docs Inserted %v Time Taken %v", numItems, elapsed)
}
