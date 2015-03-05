package largedatatests

import (
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	kv "github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	"log"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

var kvdocs tc.KeyValues
var prodfile string

func CreateDocsForDuration(wg *sync.WaitGroup, seconds float64) {
	log.Printf("CreateDocs:: Creating mutations")
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
		// log.Printf("CreateDocs:: Len of kvdocs", len(kvdocs))
	}
}

func DeleteDocsForDuration(wg *sync.WaitGroup, seconds float64) {
	log.Printf("CreateDocs:: Delete mutations")
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
		// log.Printf("DeleteDocs:: Len of kvdocs", len(kvdocs))
	}
}

func CreateDeleteDocsForDuration(wg *sync.WaitGroup, seconds float64) {
	log.Printf("CreateDocs:: Creating mutations")
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
		log.Printf("CreateDocs:: Len of kvdocs: %v", len(kvdocs))

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
	}
}

func RangeScanForDuration_ltr(header string, wg *sync.WaitGroup, seconds float64, t *testing.T, indexName, bucketName, server string) {
	log.Printf("In Range Scan")
	defer wg.Done()
	client, e := secondaryindex.CreateClient(clusterconfig.KVAddress, "RangeForDuration")
	if e != nil {
		FailTestIfError(e, "RangeScanForDuration Thread:: Error in gsi client creation", t)
	}

	defer client.Close()
	start := time.Now()
	i := 1
	for {
		elapsed := time.Since(start)
		if elapsed.Seconds() >= seconds {
			break
		}
		low := random_letter()
		high := random_letter()

		rangeStart := time.Now()
		_, err := secondaryindex.RangeWithClient(indexName, bucketName, server, []interface{}{low}, []interface{}{high}, 3, true, defaultlimit, client)
		rangeElapsed := time.Since(rangeStart)
		FailTestIfError(err, "RangeScanForDuration Thread:: Error in scan", t)
		log.Printf("Range Scan of %d user documents took %s\n", len(kvdocs), rangeElapsed)
		i++
	}
}

func RangeScanForDuration_num(header string, wg *sync.WaitGroup, seconds float64, t *testing.T, indexName, bucketName, server string) {
	log.Printf("In Range Scan")
	defer wg.Done()
	client, e := secondaryindex.CreateClient(clusterconfig.KVAddress, "RangeForDuration")
	if e != nil {
		FailTestIfError(e, "RangeScanForDuration Thread:: Error in gsi client creation", t)
	}

	defer client.Close()
	start := time.Now()
	i := 1
	for {
		elapsed := time.Since(start)
		if elapsed.Seconds() >= seconds {
			break
		}
		low := random_num(15, 80)
		high := random_num(15, 80)
		_, err := secondaryindex.RangeWithClient(indexName, bucketName, server, []interface{}{low}, []interface{}{high}, 3, true, defaultlimit, client)
		i++
		FailTestIfError(err, "Error in scan", t)
	}
}

func CreateDropIndexesForDuration(wg *sync.WaitGroup, seconds float64, t *testing.T) {
	log.Printf("Create and Drop index operations")
	defer wg.Done()
	client, e := secondaryindex.CreateClient(clusterconfig.KVAddress, "CDIndex")
	if e != nil {
		FailTestIfError(e, "CreateDropIndexesForDuration Thread:: Error in gsi client creation", t)
	}

	defer client.Close()
	start := time.Now()
	for {
		elapsed := time.Since(start)
		if elapsed.Seconds() >= seconds {
			break
		}

		var index1 = "index_age"
		var bucketName = "default"
		err := secondaryindex.CreateSecondaryIndex(index1, bucketName, indexManagementAddress, "", []string{"age"}, false, nil, true, defaultIndexActiveTimeout, client)
		FailTestIfError(err, "Error in creating the index", t)
		time.Sleep(1 * time.Second)
		_, err = secondaryindex.RangeWithClient(index1, bucketName, indexScanAddress, []interface{}{random_num(15, 80)}, []interface{}{random_num(15, 80)}, 3, true, defaultlimit, client)
		FailTestIfError(err, "CreateDropIndexesForDuration:: Error in scan", t)

		var index2 = "index_firstname"
		err = secondaryindex.CreateSecondaryIndex(index2, bucketName, indexManagementAddress, "", []string{"`first-name`"}, false, nil, true, defaultIndexActiveTimeout, client)
		FailTestIfError(err, "Error in creating the index", t)
		time.Sleep(1 * time.Second)
		_, err = secondaryindex.RangeWithClient(index2, bucketName, indexScanAddress, []interface{}{"M"}, []interface{}{"Z"}, 3, true, defaultlimit, client)
		FailTestIfError(err, "CreateDropIndexesForDuration:: Error in scan", t)

		err = secondaryindex.DropSecondaryIndexWithClient(index1, bucketName, indexManagementAddress, client)
		FailTestIfError(err, "Error in drop index", t)
		time.Sleep(1 * time.Second)
		err = secondaryindex.DropSecondaryIndexWithClient(index2, bucketName, indexManagementAddress, client)
		FailTestIfError(err, "Error in drop index", t)
		time.Sleep(1 * time.Second)
	}
}

func SequentialRangeScanForDuration(indexName, bucketName string, seconds float64, t *testing.T) error {
	log.Printf("In Range Scan")
	start := time.Now()
	i := 1
	for {
		elapsed := time.Since(start)
		if elapsed.Seconds() >= seconds {
			break
		}
		client, e := secondaryindex.CreateClient(clusterconfig.KVAddress, "SeqTest")
		if e != nil {
			return e
		}

		_, err := secondaryindex.RangeWithClient(indexName, bucketName, indexScanAddress, []interface{}{"a"}, []interface{}{"z"}, 3, true, defaultlimit, client)
		// log.Printf("%d  RangeScan:: Len of scanResults is: %d", i, len(scanResults))
		i++
		FailTestIfError(err, "Error in scan", t)
		client.Close()
	}

	return nil
}

func SkipTestSequentialRangeScans(t *testing.T) {
	log.Printf("In TestSequentialRangeScans()")
	prodfile = filepath.Join(proddir, "test.prod")
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
	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"company"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)
	SequentialRangeScanForDuration(indexName, bucketName, 60, t)
}

func TestRangeWithConcurrentAddMuts(t *testing.T) {
	log.Printf("In TestRangeWithConcurrentAddMuts()")
	var wg sync.WaitGroup
	prodfile = filepath.Join(proddir, "test.prod")
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
	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"company"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	wg.Add(2)
	go CreateDocsForDuration(&wg, 120)
	go RangeScanForDuration_ltr("Thread 1: ", &wg, 120, t, indexName, bucketName, indexScanAddress)
	wg.Wait()
}

func TestRangeWithConcurrentDelMuts(t *testing.T) {
	log.Printf("In TestRangeWithConcurrentDelMuts()")
	var wg sync.WaitGroup
	prodfile = filepath.Join(proddir, "test.prod")
	// secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)

	log.Printf("Generating JSON docs")
	kvdocs = GenerateJsons(30000, seed, prodfile, bagdir)
	seed++

	log.Printf("Setting initial JSON docs in KV")
	kv.SetKeyValues(kvdocs, "default", "", clusterconfig.KVAddress)

	var indexName = "index_company"
	var bucketName = "default"

	log.Printf("Creating a 2i")
	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"company"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	wg.Add(2)
	go DeleteDocsForDuration(&wg, 120)
	go RangeScanForDuration_ltr("Thread 1: ", &wg, 120, t, indexName, bucketName, indexScanAddress)
	wg.Wait()
}

func TestScanWithConcurrentIndexOps(t *testing.T) {
	log.Printf("In TestScanWithConcurrentIndexOps()")
	var wg sync.WaitGroup
	prodfile = filepath.Join(proddir, "test.prod")
	// secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)

	log.Printf("Generating JSON docs")
	kvdocs = GenerateJsons(100000, seed, prodfile, bagdir)
	seed++

	log.Printf("Setting initial JSON docs in KV")
	kv.SetKeyValues(kvdocs, "default", "", clusterconfig.KVAddress)

	var indexName = "index_company"
	var bucketName = "default"

	log.Printf("Creating a 2i")
	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"company"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	wg.Add(2)
	go CreateDropIndexesForDuration(&wg, 120, t)
	go RangeScanForDuration_ltr("Thread 1: ", &wg, 120, t, indexName, bucketName, indexScanAddress)
	wg.Wait()
}

func TestConcurrentScans_SameIndex(t *testing.T) {
	log.Printf("In TestConcurrentScans_SameIndex()")
	var wg sync.WaitGroup
	prodfile = filepath.Join(proddir, "test.prod")
	// secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)

	log.Printf("Generating JSON docs")
	kvdocs = GenerateJsons(100000, seed, prodfile, bagdir)
	seed++

	log.Printf("Setting initial JSON docs in KV")
	kv.SetKeyValues(kvdocs, "default", "", clusterconfig.KVAddress)

	var indexName = "index_company"
	var bucketName = "default"

	log.Printf("Creating a 2i")
	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"company"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	wg.Add(6)
	go RangeScanForDuration_ltr("Thread 1: ", &wg, 120, t, indexName, bucketName, indexScanAddress)
	go RangeScanForDuration_ltr("Thread 2: ", &wg, 120, t, indexName, bucketName, indexScanAddress)
	go RangeScanForDuration_ltr("Thread 3: ", &wg, 120, t, indexName, bucketName, indexScanAddress)
	go RangeScanForDuration_ltr("Thread 4: ", &wg, 120, t, indexName, bucketName, indexScanAddress)
	go RangeScanForDuration_ltr("Thread 5: ", &wg, 120, t, indexName, bucketName, indexScanAddress)
	go RangeScanForDuration_ltr("Thread 6: ", &wg, 120, t, indexName, bucketName, indexScanAddress)
	wg.Wait()
}

func TestConcurrentScans_MultipleIndexes(t *testing.T) {
	log.Printf("In TestConcurrentScans_MultipleIndexes()")
	var wg sync.WaitGroup
	prodfile = filepath.Join(proddir, "test.prod")
	// secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)

	log.Printf("Generating JSON docs")
	kvdocs = GenerateJsons(100000, seed, prodfile, bagdir)
	seed++

	log.Printf("Setting initial JSON docs in KV")
	kv.SetKeyValues(kvdocs, "default", "", clusterconfig.KVAddress)

	var index1 = "index_company"
	var index2 = "index_age"
	var index3 = "index_firstname"
	var bucketName = "default"

	log.Printf("Creating multiple indexes")
	err := secondaryindex.CreateSecondaryIndex(index1, bucketName, indexManagementAddress, "", []string{"company"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.CreateSecondaryIndex(index2, bucketName, indexManagementAddress, "", []string{"age"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.CreateSecondaryIndex(index3, bucketName, indexManagementAddress, "", []string{"`first-name`"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	wg.Add(3)
	go RangeScanForDuration_ltr("Thread 1: ", &wg, 120, t, index1, bucketName, indexScanAddress)
	go RangeScanForDuration_num("Thread 2: ", &wg, 120, t, index2, bucketName, indexScanAddress)
	go RangeScanForDuration_ltr("Thread 3: ", &wg, 120, t, index3, bucketName, indexScanAddress)
	wg.Wait()
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func random_num(min, max float64) float64 {
	return rand.Float64()*(max-min) + min
}

func random_letter() string {
	letters := []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	return string(letters[rand.Intn(len(letters))])
}
