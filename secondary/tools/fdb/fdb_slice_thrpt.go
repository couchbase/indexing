package main

import (
	"fmt"
	"github.com/couchbase/indexing/secondary/indexer"
	"log"
	"os"
	"runtime/pprof"
	"time"
)

func main() {

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

	slice, _ := indexer.NewForestDBSlice(".", 0, 1, 1, false, nil)

	log.Println("***** GENERATING *****")
	numItems := 1000000
	keys := make([]*indexer.Key, numItems)
	//lenkeys := make([]int, numItems)
	vals := make([]*indexer.Value, numItems)
	//lenvals := make([]int, numItems)
	for i := 0; i < numItems; i++ {
		keys[i], _ = indexer.NewKey([]byte(fmt.Sprintf("perf%v", i)))
		//lenkeys[i] = len(keys[i])
		vals[i], _ = indexer.NewValue([]byte(fmt.Sprintf("body%v", i)))
		//	lenvals[i] = len(vals[i])
	}

	time.Sleep(3 * time.Second)
	log.Println("***** LOADING*****")
	start := time.Now()
	for i := 0; i < numItems; i++ {
		slice.Insert(keys[i], vals[i])
	}
	//	dbfile.Commit(forestdb.COMMIT_MANUAL_WAL_FLUSH)
	slice.NewSnapshot(nil, false)

	elapsed := time.Since(start)
	log.Printf("***** RESULT : Docs Inserted %v Time Taken %v", numItems, elapsed)
}
