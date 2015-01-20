package largedatatests

import (
	"fmt"
	"time"
	"log"
	"math/rand"
	"sync"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	kv "github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	"testing"
)

var kvdocs tc.KeyValues
var prodfile string
var bagdir string

func CreateDocsForDuration(wg *sync.WaitGroup, seconds float64) {
	fmt.Println("CreateDocs:: Creating mutations")
	defer wg.Done()
	start := time.Now()
	for {
		elapsed := time.Since(start)
		if elapsed.Seconds() >= seconds {
			break
		}
		
		docsToBeCreated := GenerateJsons(100, seed, prodfile, bagdir)
		seed++
		kv.SetKeyValues(docsToBeCreated, "default", "", clusterconfig.KVAddress)
		for key, value := range docsToBeCreated {
			kvdocs[key] = value	
		}
		fmt.Println("CreateDocs:: Len of kvdocs", len(kvdocs))
	}
}

func DeleteDocsForDuration(wg *sync.WaitGroup, seconds float64) {
	fmt.Println("CreateDocs:: Delete mutations")
	defer wg.Done()
	start := time.Now()
	for {
		elapsed := time.Since(start)
		if elapsed.Seconds() >= seconds {
			break
		}	
		i := 0
		keysToBeDeleted := make(tc.KeyValues)
		for key, value := range kvdocs {
			keysToBeDeleted[key] = value
			i++
			if i == 5 {
				break
			}
		}
		kv.DeleteKeys(keysToBeDeleted, "default", "", clusterconfig.KVAddress)
		// Update docs object with deleted keys
		for key, _ := range keysToBeDeleted {
			delete(kvdocs, key)
		}
		fmt.Println("DeleteDocs:: Len of kvdocs", len(kvdocs))
	}
}

func RangeScanForDuration(wg *sync.WaitGroup, seconds float64, t *testing.T, indexName, bucketName, server string, low, high []interface{}, inclusion uint32,
	distinct bool, limit int64) {
	fmt.Println("In Range Scan")	
	defer wg.Done()
	client := secondaryindex.CreateClient(clusterconfig.KVAddress, "RangeForDuration")
	defer client.Close()
	start := time.Now()
	i := 1
	for {
		elapsed := time.Since(start)
		if elapsed.Seconds() >= seconds {
			break
		}
		
		scanResults, err := secondaryindex.RangeWithClient(indexName, bucketName, server, low, high, inclusion, distinct, limit, client)
		log.Printf("%d  RangeScan:: Len of scanResults is: %d", i, len(scanResults))
		i++
		FailTestIfError(err, "Error in scan", t)
	}
}

func CreateDropIndexesForDuration(wg *sync.WaitGroup, seconds float64, t *testing.T) {
	fmt.Println("Create and Drop index operations")
	defer wg.Done()
	client := secondaryindex.CreateClient(clusterconfig.KVAddress, "CDIndex")
	start := time.Now()
	for {
		elapsed := time.Since(start)
		if elapsed.Seconds() >= seconds {
			break
		}
		
		var index1 = "index_age"
		var bucketName = "default"
		err := secondaryindex.CreateSecondaryIndexWithClient(index1, bucketName, indexManagementAddress, []string{"age"}, true, client)
		FailTestIfError(err, "Error in creating the index", t)
		time.Sleep(1 * time.Second)
		scanResults, err := secondaryindex.RangeWithClient(index1, bucketName, indexScanAddress, []interface{}{random(15, 80)}, []interface{}{random(15, 80)}, 3, true, defaultlimit, client)
		log.Printf("%v  RangeScan:: Len of scanResults is: %d", index1, len(scanResults))
		FailTestIfError(err, "Error in scan", t)
		
		var index2 = "index_firstname"
		err = secondaryindex.CreateSecondaryIndexWithClient(index2, bucketName, indexManagementAddress, []string{"`first-name`"}, true, client)
		FailTestIfError(err, "Error in creating the index", t)
		time.Sleep(1 * time.Second)
		scanResults, err = secondaryindex.RangeWithClient(index2, bucketName, indexScanAddress, []interface{}{"M"}, []interface{}{"Z"}, 3, true, defaultlimit, client)
		log.Printf("%v  RangeScan:: Len of scanResults is: %d", index2, len(scanResults))
		FailTestIfError(err, "Error in scan", t)
		
		err = secondaryindex.DropSecondaryIndexWithClient(index1, bucketName, indexManagementAddress, client)
		FailTestIfError(err, "Error in drop index", t)
		time.Sleep(1 * time.Second)
		err = secondaryindex.DropSecondaryIndexWithClient(index2, bucketName, indexManagementAddress, client)
		FailTestIfError(err, "Error in drop index", t)
		time.Sleep(1 * time.Second)
	}
	client.Close()
}

func SequentialRangeScanForDuration(indexName, bucketName string, seconds float64, t *testing.T) {
	fmt.Println("In Range Scan")	
	start := time.Now()
	i := 1
	for {
		elapsed := time.Since(start)
		if elapsed.Seconds() >= seconds {
			break
		}
		client := secondaryindex.CreateClient(clusterconfig.KVAddress, "SeqTest")
		scanResults, err := secondaryindex.RangeWithClient(indexName, bucketName, indexScanAddress, []interface{}{"a"}, []interface{}{"z"}, 3, true, defaultlimit, client)
		log.Printf("%d  RangeScan:: Len of scanResults is: %d", i, len(scanResults))
		i++
		FailTestIfError(err, "Error in scan", t)
		client.Close()
	}
}

func TestSequentialRangeScans(t *testing.T) {
	fmt.Println("In TestSequentialRangeScans()")
	prodfile = "../../../../../prataprc/monster/prods/test.prod"
	bagdir =  "../../../../../prataprc/monster/bags/"	
	// secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	
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
	SequentialRangeScanForDuration(indexName, bucketName, 60, t)	
}
	
func TestRangeWithConcurrentAddMuts(t *testing.T) {
	fmt.Println("In TestRangeWithConcurrentAddMuts()")
	var wg sync.WaitGroup
	prodfile = "../../../../../prataprc/monster/prods/test.prod"
	bagdir =  "../../../../../prataprc/monster/bags/"	
	// secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	
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
	go CreateDocsForDuration(&wg, 60)
	go RangeScanForDuration(&wg, 60, t, indexName, bucketName, indexScanAddress, []interface{}{"a"}, []interface{}{"z"}, 3, true, defaultlimit)	
	wg.Wait()
}

func TestRangeWithConcurrentDelMuts(t *testing.T) {
	fmt.Println("In TestRangeWithConcurrentDelMuts()")
	var wg sync.WaitGroup
	prodfile = "../../../../../prataprc/monster/prods/test.prod"
	bagdir =  "../../../../../prataprc/monster/bags/"	
	// secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	
	fmt.Println("Generating JSON docs")
	kvdocs = GenerateJsons(30000, seed, prodfile, bagdir)
	seed++	
	
	fmt.Println("Setting initial JSON docs in KV")
	kv.SetKeyValues(kvdocs, "default", "", clusterconfig.KVAddress)
	
	var indexName = "index_company"
	var bucketName = "default"
	
	fmt.Println("Creating a 2i")
	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"company"}, true)
	FailTestIfError(err, "Error in creating the index", t)
	
	wg.Add(2)
	go DeleteDocsForDuration(&wg, 60)
	go RangeScanForDuration(&wg, 60, t, indexName, bucketName, indexScanAddress, []interface{}{"a"}, []interface{}{"z"}, 3, true, defaultlimit)
	wg.Wait()
}

func TestScanWithConcurrentIndexOps(t *testing.T) {
	fmt.Println("In TestScanWithConcurrentIndexOps()")
	var wg sync.WaitGroup
	prodfile = "../../../../../prataprc/monster/prods/test.prod"
	bagdir =  "../../../../../prataprc/monster/bags/"	
	// secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	
	fmt.Println("Generating JSON docs")
	kvdocs = GenerateJsons(30000, seed, prodfile, bagdir)
	seed++	
	
	fmt.Println("Setting initial JSON docs in KV")
	kv.SetKeyValues(kvdocs, "default", "", clusterconfig.KVAddress)
	
	var indexName = "index_company"
	// var index1 = "index_age"
	// var index2 = "index_firstname"
	var bucketName = "default"
	
	fmt.Println("Creating a 2i")
	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"company"}, true)
	FailTestIfError(err, "Error in creating the index", t)
	
	wg.Add(2)
	go CreateDropIndexesForDuration(&wg, 180, t)
	go RangeScanForDuration(&wg, 180, t, indexName, bucketName, indexScanAddress, []interface{}{"a"}, []interface{}{"z"}, 3, true, defaultlimit)
	wg.Wait()
}

func random(min, max float64) float64 {
  return rand.Float64() * (max - min) + min
}