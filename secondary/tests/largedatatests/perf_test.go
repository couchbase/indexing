package largedatatests

import (
	"errors"
	kv "github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	"log"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func SkipTestPerfInitialIndexBuild_SimpleJson(t *testing.T) {
	log.Printf("In TestPerfInitialIndexBuild()")
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	prodfile = filepath.Join(proddir, "test.prod")
	log.Printf("Generating JSON docs")
	count := 1000000
	keyValues := GenerateJsons(count, 1, prodfile, bagdir)
	log.Printf("Setting JSON docs in KV")
	kv.SetKeyValues(keyValues, "default", "", clusterconfig.KVAddress)

	var indexName = "index_company"
	var bucketName = "default"

	log.Printf("Creating a 2i")
	start := time.Now()
	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"company"}, true)
	FailTestIfError(err, "Error in creating the index", t)
	elapsed := time.Since(start)
	log.Printf("Index build of %d user documents took %s\n", count, elapsed)
}

func SkipTestPerfPrimaryIndexBuild_SimpleJson(t *testing.T) {
	log.Printf("In TestPerfInitialIndexBuild()")
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	prodfile = filepath.Join(proddir, "test.prod")
	log.Printf("Generating JSON docs")
	count := 100000000
	keyValues := GenerateJsons(count, 1, prodfile, bagdir)
	log.Printf("Setting JSON docs in KV")
	kv.SetKeyValues(keyValues, "default", "", clusterconfig.KVAddress)

	var indexName = "index_primary"
	var bucketName = "default"

	log.Printf("Creating a 2i")
	start := time.Now()
	err := secondaryindex.CreatePrimaryIndex(indexName, bucketName, indexManagementAddress, true)
	FailTestIfError(err, "Error in creating the index", t)
	elapsed := time.Since(start)
	log.Printf("Index build of %d user documents took %s\n", count, elapsed)
	scanResults, err := secondaryindex.ScanAll(indexName, bucketName, indexScanAddress, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	if len(scanResults) != len(keyValues) {
		log.Printf("Len of scanResults is incorrect. Expected and Actual are: %d and %d", len(keyValues), len(scanResults))
		err = errors.New("Len of scanResults is incorrect.")
	}
	FailTestIfError(err, "Len of scanResults is incorrect", t)
}

func SkipTestPerfInitialIndexBuild_ComplexJson(t *testing.T) {
	log.Printf("In TestPerfInitialIndexBuild()")
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	prodfile := "../../../../../prataprc/monster/prods/users.prod"
	bagdir := "../../../../../prataprc/monster/bags/"
	log.Printf("Generating JSON docs")
	count := 100000
	keyValues := GenerateJsons(count, 1, prodfile, bagdir)
	log.Printf("Setting JSON docs in KV")
	kv.SetKeyValues(keyValues, "default", "", clusterconfig.KVAddress)

	var indexName = "index_company"
	var bucketName = "default"

	log.Printf("Creating a 2i")
	start := time.Now()
	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"company"}, true)
	FailTestIfError(err, "Error in creating the index", t)
	elapsed := time.Since(start)
	log.Printf("Index build of %d user documents took %s\n", count, elapsed)
}

func SkipTestPerfRangeWithoutMutations_1M(t *testing.T) {
	log.Printf("In TestPerfQueryWithoutMutations_1M()")
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	prodfile := "../../../../../prataprc/monster/prods/test.prod"
	bagdir := "../../../../../prataprc/monster/bags/"
	log.Printf("Generating JSON docs")
	count := 1000000
	keyValues := GenerateJsons(count, 1, prodfile, bagdir)
	log.Printf("Setting JSON docs in KV")
	kv.SetKeyValues(keyValues, "default", "", clusterconfig.KVAddress)

	var indexName = "index_company"
	var bucketName = "default"

	log.Printf("Creating a 2i")

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"company"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	start := time.Now()
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"E"}, []interface{}{"N"}, 3, true, defaultlimit)
	elapsed := time.Since(start)
	log.Printf("RangeScan:: Len of scanResults is: %d", len(scanResults))
	log.Printf("Range Scan of %d user documents took %s\n", count, elapsed)
}

func SkipTestPerfLookupWithoutMutations_1M(t *testing.T) {
	log.Printf("In TestPerfQueryWithoutMutations_1M()")
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	prodfile := "../../../../../prataprc/monster/prods/test.prod"
	bagdir := "../../../../../prataprc/monster/bags/"
	log.Printf("Generating JSON docs")
	count := 1000000
	keyValues := GenerateJsons(count, 1, prodfile, bagdir)
	log.Printf("Setting JSON docs in KV")
	kv.SetKeyValues(keyValues, "default", "", clusterconfig.KVAddress)

	var indexName = "index_company"
	var bucketName = "default"

	log.Printf("Creating a 2i")

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"company"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	start := time.Now()
	scanResults, err := secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{"AQUACINE"}, true, defaultlimit)
	elapsed := time.Since(start)
	log.Printf("Lookup:: Len of scanResults is: %d", len(scanResults))
	log.Printf("Lookup of %d user documents took %s\n", count, elapsed)
}

func SkipTestPerfScanAllWithoutMutations_1M(t *testing.T) {
	log.Printf("In TestPerfQueryWithoutMutations_1M()")
	secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)

	prodfile := "../../../../../prataprc/monster/prods/test.prod"
	bagdir := "../../../../../prataprc/monster/bags/"
	log.Printf("Generating JSON docs")
	count := 1000000
	keyValues := GenerateJsons(count, 1, prodfile, bagdir)
	log.Printf("Setting JSON docs in KV")
	kv.SetKeyValues(keyValues, "default", "", clusterconfig.KVAddress)

	var indexName = "index_company"
	var bucketName = "default"

	log.Printf("Creating a 2i")

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"company"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	start := time.Now()
	scanResults, err := secondaryindex.ScanAll(indexName, bucketName, indexScanAddress, defaultlimit)
	elapsed := time.Since(start)
	log.Printf("ScalAll:: Len of scanResults is: %d", len(scanResults))
	log.Printf("ScanAll of %d user documents took %s\n", count, elapsed)
}

func SkipTestPerfRangeWithoutMutations_10M(t *testing.T) {
	log.Printf("In TestPerfQueryWithoutMutations_10M()")
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	prodfile := "../../../../../prataprc/monster/prods/users.prod"
	bagdir := "../../../../../prataprc/monster/bags/"
	log.Printf("Generating JSON docs")
	count := 10000000
	keyValues := GenerateJsons(count, 1, prodfile, bagdir)
	log.Printf("Setting JSON docs in KV")
	kv.SetKeyValues(keyValues, "default", "", clusterconfig.KVAddress)

	var indexName = "index_company"
	var bucketName = "default"

	log.Printf("Creating a 2i")
	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"company"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	start := time.Now()
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"E"}, []interface{}{"N"}, 3, true, defaultlimit)
	elapsed := time.Since(start)
	log.Printf("RangeScan:: Len of scanResults is: %d", len(scanResults))
	log.Printf("Range Scan of %d user documents took %s\n", count, elapsed)
}

func SkipTestPerfRangeWithConcurrentKVMuts(t *testing.T) {
	log.Printf("In TestPerfRangeWithConcurrentKVMuts()")
	var wg sync.WaitGroup
	prodfile = "../../../../../prataprc/monster/prods/test.prod"
	bagdir = "../../../../../prataprc/monster/bags/"
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	log.Printf("Generating JSON docs")
	kvdocs = GenerateJsons(1000, seed, prodfile, bagdir)
	seed++

	log.Printf("Setting initial JSON docs in KV")
	kv.SetKeyValues(kvdocs, "default", "", clusterconfig.KVAddress)

	var indexName = "index_company"
	var bucketName = "default"

	log.Printf("Creating a 2i")
	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"company"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	wg.Add(2)
	go CreateDeleteDocsForDuration(&wg, 60)
	go RangeScanForDuration_ltr("Range Thread: ", &wg, 60, t, indexName, bucketName, indexScanAddress)
	wg.Wait()
}
