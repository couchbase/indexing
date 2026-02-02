//go:build nolint

package main

import (
	"fmt"
	"log"
	"os"
	"time"

	forestdb "github.com/couchbase/indexing/secondary/fdb"
)

func main() {

	do_test1()
	do_test2()
	do_test3()

}

func do_test1() {

	log.Println("***** TEST1 SINGLE KV STORE 10M INSERT *****")

	config := forestdb.DefaultConfig()
	config.SetDurabilityOpt(forestdb.DRB_ASYNC)

	var dbfile *forestdb.File
	var err error
	file := "test1"
	if dbfile, err = forestdb.Open(file, config); err != nil {
		panic(err)
	}
	defer os.RemoveAll(file)
	defer forestdb.Destroy(file, config)

	var main *forestdb.KVStore
	kvconfig := forestdb.DefaultKVStoreConfig()
	if main, err = dbfile.OpenKVStore("main", kvconfig); err != nil {
		panic(err)
	}
	defer main.Close()

	log.Println("***** GENERATING *****")
	numItems := 10000000
	keys := make([][]byte, numItems)
	vals := make([][]byte, numItems)
	for i := 0; i < numItems; i++ {
		keys[i] = []byte(fmt.Sprintf("key%v", i))
		vals[i] = []byte(fmt.Sprintf("body%v", i))
	}

	time.Sleep(3 * time.Second)
	log.Println("***** LOADING*****")
	start := time.Now()
	for i := 0; i < numItems; i++ {
		main.SetKV(keys[i], vals[i])
	}
	dbfile.Commit(forestdb.COMMIT_MANUAL_WAL_FLUSH)
	elapsed := time.Since(start)

	log.Printf("***** RESULT : Docs Inserted %v Time Taken %v", numItems, elapsed)

}

func do_test2() {

	log.Println("***** TEST2 TWO KV STORES 5M INSERT EACH *****")

	config := forestdb.DefaultConfig()
	config.SetDurabilityOpt(forestdb.DRB_ASYNC)

	var dbfile *forestdb.File
	var err error
	file := "test2"
	if dbfile, err = forestdb.Open(file, config); err != nil {
		panic(err)
	}
	defer os.RemoveAll(file)
	defer forestdb.Destroy(file, config)

	var main, back *forestdb.KVStore
	kvconfig := forestdb.DefaultKVStoreConfig()
	if main, err = dbfile.OpenKVStore("main", kvconfig); err != nil {
		panic(err)
	}
	defer main.Close()

	if back, err = dbfile.OpenKVStore("back", kvconfig); err != nil {
		panic(err)
	}
	defer back.Close()

	log.Println("***** GENERATING *****")
	numItems := 5000000
	keys := make([][]byte, numItems)
	vals := make([][]byte, numItems)
	for i := 0; i < numItems; i++ {
		keys[i] = []byte(fmt.Sprintf("key%v", i))
		vals[i] = []byte(fmt.Sprintf("body%v", i))
	}

	time.Sleep(3 * time.Second)

	log.Println("***** LOADING*****")
	start := time.Now()
	for i := 0; i < numItems; i++ {
		back.SetKV(keys[i], vals[i])
		main.SetKV(keys[i], nil)
	}
	dbfile.Commit(forestdb.COMMIT_MANUAL_WAL_FLUSH)
	elapsed := time.Since(start)

	log.Printf("***** RESULT : Docs Inserted %v Time Taken %v", numItems, elapsed)

}

func do_test3() {

	log.Println("***** TEST3 TWO KV STORE 5M INSERT EACH WITH GET *****")

	config := forestdb.DefaultConfig()
	config.SetDurabilityOpt(forestdb.DRB_ASYNC)

	var dbfile *forestdb.File
	var err error
	file := "test3"
	if dbfile, err = forestdb.Open(file, config); err != nil {
		panic(err)
	}
	defer os.RemoveAll(file)
	defer forestdb.Destroy(file, config)

	var main, back *forestdb.KVStore
	kvconfig := forestdb.DefaultKVStoreConfig()
	if main, err = dbfile.OpenKVStore("main", kvconfig); err != nil {
		panic(err)
	}
	defer main.Close()

	if back, err = dbfile.OpenKVStore("back", kvconfig); err != nil {
		panic(err)
	}
	defer back.Close()

	log.Println("***** GENERATING *****")
	numItems := 5000000
	keys := make([][]byte, numItems)
	//lenkeys := make([]int, numItems)
	vals := make([][]byte, numItems)
	//lenvals := make([]int, numItems)
	for i := 0; i < numItems; i++ {
		keys[i] = []byte(fmt.Sprintf("key%v", i))
		//lenkeys[i] = len(keys[i])
		vals[i] = []byte(fmt.Sprintf("body%v", i))
		//	lenvals[i] = len(vals[i])
	}

	time.Sleep(3 * time.Second)
	log.Println("***** LOADING*****")
	start := time.Now()
	for i := 0; i < numItems; i++ {
		back.GetKV(keys[i])
		back.SetKV(keys[i], vals[i])
		//back.SetKV(keys[i], keys[i], lenkeys[i], lenkeys[i])
		main.SetKV(keys[i], nil)
	}
	dbfile.Commit(forestdb.COMMIT_MANUAL_WAL_FLUSH)
	elapsed := time.Since(start)
	log.Printf("***** RESULT : Docs Inserted %v Time Taken %v", numItems, elapsed)
}
