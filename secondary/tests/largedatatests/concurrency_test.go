package largedatatests

import (
	"fmt"
	"time"
	"log"
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

func CreateDropIndexesForDuration(wg *sync.WaitGroup, seconds float64) {
	fmt.Println("CreateDocs:: Creating mutations")
	defer wg.Done()
	start := time.Now()
	for {
		elapsed := time.Since(start)
		if elapsed.Seconds() >= seconds {
			break
		}
		
		var indexName = "index_company"
		var bucketName = "default"
		err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"company"}, true)
		FailTestIfError(err, "Error in creating the index", t)
		SequentialRangeScanForDuration(indexName, bucketName, 60, t)	
		fmt.Println("Creating a 2i")
	}
}

func RangeScanForDuration(wg *sync.WaitGroup, indexName, bucketName string, seconds float64, t *testing.T) {
	fmt.Println("In Range Scan")	
	defer wg.Done()
	client := secondaryindex.CreateClient(clusterconfig.KVAddress)
	defer client.Close()
	start := time.Now()
	i := 1
	for {
		elapsed := time.Since(start)
		if elapsed.Seconds() >= seconds {
			break
		}
		
		scanResults, err := secondaryindex.RangeWithClient(indexName, bucketName, indexScanAddress, []interface{}{"a"}, []interface{}{"z"}, 3, true, defaultlimit, client)
		log.Printf("%d  RangeScan:: Len of scanResults is: %d", i, len(scanResults))
		i++
		FailTestIfError(err, "Error in scan", t)
	}
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
		client := secondaryindex.CreateClient(clusterconfig.KVAddress)
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
	go RangeScanForDuration(&wg, indexName, bucketName, 60, t)	
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
	go RangeScanForDuration(&wg, indexName, bucketName, 60, t)	
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
	var bucketName = "default"
	
	fmt.Println("Creating a 2i")
	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"company"}, true)
	FailTestIfError(err, "Error in creating the index", t)
	
	wg.Add(2)
	go DeleteDocsForDuration(&wg, 60)
	go RangeScanForDuration(&wg, indexName, bucketName, 60, t)	
	wg.Wait()
}