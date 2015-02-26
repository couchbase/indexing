package largedatatests

import (
	"fmt"
	"path/filepath"
	"time"
	"log"
	"sync"
	kv "github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	"testing"
)

func SkipTestPerfInitialIndexBuild_SimpleJson(t *testing.T) {
	fmt.Println("In TestPerfInitialIndexBuild()")
	secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	
	prodfile = filepath.Join(proddir, "test.prod")
	fmt.Println("Generating JSON docs")
	count := 1000000
	keyValues := GenerateJsons(count, 1, prodfile, bagdir)
	fmt.Println("Setting JSON docs in KV")
	kv.SetKeyValues(keyValues, "default", "", clusterconfig.KVAddress)
	
	var indexName = "index_company"
	var bucketName = "default"
	
	fmt.Println("Creating a 2i")
	start := time.Now()
	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"company"}, true)
	FailTestIfError(err, "Error in creating the index", t)
	elapsed := time.Since(start)
	fmt.Printf("Index build of %d user documents took %s\n", count, elapsed)
}

func SkipTestPerfInitialIndexBuild_ComplexJson(t *testing.T) {
	fmt.Println("In TestPerfInitialIndexBuild()")
	secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	
	prodfile := "../../../../../prataprc/monster/prods/users.prod"
	bagdir :=  "../../../../../prataprc/monster/bags/"
	fmt.Println("Generating JSON docs")
	count := 100000
	keyValues := GenerateJsons(count, 1, prodfile, bagdir)
	fmt.Println("Setting JSON docs in KV")
	kv.SetKeyValues(keyValues, "default", "", clusterconfig.KVAddress)
	
	var indexName = "index_company"
	var bucketName = "default"
	
	fmt.Println("Creating a 2i")
	start := time.Now()
	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"company"}, true)
	FailTestIfError(err, "Error in creating the index", t)
	elapsed := time.Since(start)
	fmt.Printf("Index build of %d user documents took %s\n", count, elapsed)
}

func SkipTestPerfRangeWithoutMutations_1M(t *testing.T) {
	fmt.Println("In TestPerfQueryWithoutMutations_1M()")
	secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	
	prodfile := "../../../../../prataprc/monster/prods/test.prod"
	bagdir :=  "../../../../../prataprc/monster/bags/"
	fmt.Println("Generating JSON docs")
	count := 1000000
	keyValues := GenerateJsons(count, 1, prodfile, bagdir)
	fmt.Println("Setting JSON docs in KV")
	kv.SetKeyValues(keyValues, "default", "", clusterconfig.KVAddress)
	
	var indexName = "index_company"
	var bucketName = "default"
	
	fmt.Println("Creating a 2i")
	
	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"company"}, true)
	FailTestIfError(err, "Error in creating the index", t)
	
	start := time.Now()
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"E"}, []interface{}{"N"}, 3, true, defaultlimit)
	elapsed := time.Since(start)
	log.Printf("RangeScan:: Len of scanResults is: %d", len(scanResults))
	fmt.Printf("Range Scan of %d user documents took %s\n", count, elapsed)
}

func SkipTestPerfLookupWithoutMutations_1M(t *testing.T) {
	fmt.Println("In TestPerfQueryWithoutMutations_1M()")
	secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	
	prodfile := "../../../../../prataprc/monster/prods/test.prod"
	bagdir :=  "../../../../../prataprc/monster/bags/"
	fmt.Println("Generating JSON docs")
	count := 1000000
	keyValues := GenerateJsons(count, 1, prodfile, bagdir)
	fmt.Println("Setting JSON docs in KV")
	kv.SetKeyValues(keyValues, "default", "", clusterconfig.KVAddress)
	
	var indexName = "index_company"
	var bucketName = "default"
	
	fmt.Println("Creating a 2i")
	
	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"company"}, true)
	FailTestIfError(err, "Error in creating the index", t)
	
	start := time.Now()
	scanResults, err := secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{"AQUACINE"}, true, defaultlimit)
	elapsed := time.Since(start)
	log.Printf("Lookup:: Len of scanResults is: %d", len(scanResults))
	fmt.Printf("Lookup of %d user documents took %s\n", count, elapsed)
}

func SkipTestPerfScanAllWithoutMutations_1M(t *testing.T) {
	fmt.Println("In TestPerfQueryWithoutMutations_1M()")
	secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	
	prodfile := "../../../../../prataprc/monster/prods/test.prod"
	bagdir :=  "../../../../../prataprc/monster/bags/"
	fmt.Println("Generating JSON docs")
	count := 1000000
	keyValues := GenerateJsons(count, 1, prodfile, bagdir)
	fmt.Println("Setting JSON docs in KV")
	kv.SetKeyValues(keyValues, "default", "", clusterconfig.KVAddress)
	
	var indexName = "index_company"
	var bucketName = "default"
	
	fmt.Println("Creating a 2i")
	
	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"company"}, true)
	FailTestIfError(err, "Error in creating the index", t)
	
	start := time.Now()
	scanResults, err := secondaryindex.ScanAll(indexName, bucketName, indexScanAddress, defaultlimit)
	elapsed := time.Since(start)
	log.Printf("ScalAll:: Len of scanResults is: %d", len(scanResults))
	fmt.Printf("ScanAll of %d user documents took %s\n", count, elapsed)
}

func SkipTestPerfRangeWithoutMutations_10M(t *testing.T) {
	fmt.Println("In TestPerfQueryWithoutMutations_10M()")
	secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	
	prodfile := "../../../../../prataprc/monster/prods/users.prod"
	bagdir :=  "../../../../../prataprc/monster/bags/"
	fmt.Println("Generating JSON docs")
	count := 10000000
	keyValues := GenerateJsons(count, 1, prodfile, bagdir)
	fmt.Println("Setting JSON docs in KV")
	kv.SetKeyValues(keyValues, "default", "", clusterconfig.KVAddress)
	
	var indexName = "index_company"
	var bucketName = "default"
	
	fmt.Println("Creating a 2i")
	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"company"}, true)
	FailTestIfError(err, "Error in creating the index", t)
	
	start := time.Now()
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"E"}, []interface{}{"N"}, 3, true, defaultlimit)
	elapsed := time.Since(start)
	log.Printf("RangeScan:: Len of scanResults is: %d", len(scanResults))
	fmt.Printf("Range Scan of %d user documents took %s\n", count, elapsed)
}

func SkipTestPerfRangeWithConcurrentKVMuts(t *testing.T) {
	fmt.Println("In TestPerfRangeWithConcurrentKVMuts()")
	var wg sync.WaitGroup
	prodfile = "../../../../../prataprc/monster/prods/test.prod"
	bagdir =  "../../../../../prataprc/monster/bags/"	
	secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	
	fmt.Println("Generating JSON docs")
	kvdocs = GenerateJsons(1000, seed, prodfile, bagdir)
	seed++	
	
	fmt.Println("Setting initial JSON docs in KV")
	kv.SetKeyValues(kvdocs, "default", "", clusterconfig.KVAddress)
	
	var indexName = "index_company"
	var bucketName = "default"
	
	fmt.Println("Creating a 2i")
	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"company"}, true)
	FailTestIfError(err, "Error in creating the index", t)
	
	wg.Add(2)
	go CreateDeleteDocsForDuration(&wg, 60)
	go RangeScanForDuration_ltr("Range Thread: ", &wg, 60, t, indexName, bucketName, indexScanAddress)
	wg.Wait()
}