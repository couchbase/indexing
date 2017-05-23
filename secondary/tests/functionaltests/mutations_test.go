package functionaltests

import (
	c "github.com/couchbase/indexing/secondary/common"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/datautility"
	kv "github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	tv "github.com/couchbase/indexing/secondary/tests/framework/validation"
	"log"
	"testing"
	"time"
)

func CreateDocs(num int) {
	i := 0
	keysToBeSet := make(tc.KeyValues)
	for key, value := range mut_docs {
		keysToBeSet[key] = value
		i++
		if i == num {
			break
		}
	}
	kv.SetKeyValues(keysToBeSet, "default", "", clusterconfig.KVAddress)
	// Update docs object with newly added keys and remove those keys from mut_docs
	for key, value := range keysToBeSet {
		docs[key] = value
		delete(mut_docs, key)
	}
}

func DeleteDocs(num int) {
	i := 0
	keysToBeDeleted := make(tc.KeyValues)
	for key, value := range docs {
		keysToBeDeleted[key] = value
		i++
		if i == num {
			break
		}
	}
	kv.DeleteKeys(keysToBeDeleted, "default", "", clusterconfig.KVAddress)
	// Update docs object with deleted keys and add those keys from mut_docs
	for key, value := range keysToBeDeleted {
		delete(docs, key)
		mut_docs[key] = value
	}
}

// delete docs and do not update mut_docs object
func DeleteDocs2(num int) {
	i := 0
	keysToBeDeleted := make(tc.KeyValues)
	for key, value := range docs {
		keysToBeDeleted[key] = value
		i++
		if i == num {
			break
		}
	}
	kv.DeleteKeys(keysToBeDeleted, "default", "", clusterconfig.KVAddress)
	// Update docs object with deleted keys
	for key, _ := range keysToBeDeleted {
		delete(docs, key)
	}
}

func UpdateDocs(num int) {
	i := 0

	// Pick some docs from mut_docs
	keysFromMutDocs := make(tc.KeyValues)
	for key, value := range mut_docs {
		keysFromMutDocs[key] = value
		i++
		if i == num {
			break
		}
	}
	// and , Add them to docs
	keysToBeSet := make(tc.KeyValues)
	for _, value := range keysFromMutDocs {
		n := randomNum(0, float64(len(docs)-1))
		i = 0
		var k string
		for k, _ = range docs {
			if i == n {
				break
			}
			i++
		}
		docs[k] = value
		keysToBeSet[k] = value
	}
	log.Printf("Num of keysFromMutDocs: %d", len(keysFromMutDocs))
	log.Printf("Updating number of documents: %d", len(keysToBeSet))
	kv.SetKeyValues(keysToBeSet, "default", "", clusterconfig.KVAddress)
}

func TestRestartIndexer(t *testing.T) {
	log.Printf("In TestRestartIndexer()")

	tc.KillIndexer()
	time.Sleep(30 * time.Second)

	var indexName = "index_age"
	var bucketName = "default"

	docScanResults := datautility.ExpectedScanResponse_int64(docs, "age", 0, 90, 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{0}, []interface{}{90}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	log.Printf("Len of expected and actual scan results are :  %d and %d", len(docScanResults), len(scanResults))
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestCreateDocsMutation(t *testing.T) {
	log.Printf("In TestCreateDocsMutation()")
	var indexName = "index_age"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"age"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_int64(docs, "age", 0, 90, 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{0}, []interface{}{90}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	log.Printf("Len of expected and actual scan results are :  %d and %d", len(docScanResults), len(scanResults))
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	//Create docs mutations: Add new docs to KV
	CreateDocs(100)

	docScanResults = datautility.ExpectedScanResponse_int64(docs, "age", 0, 90, 1)
	start := time.Now()
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{0}, []interface{}{90}, 1, false, defaultlimit, c.SessionConsistency, nil)
	elapsed := time.Since(start)
	log.Printf("Index Scan after mutations took %s\n", elapsed)
	FailTestIfError(err, "Error in scan", t)
	log.Printf("Len of expected and actual scan results are :  %d and %d", len(docScanResults), len(scanResults))
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestRestartProjector(t *testing.T) {
	log.Printf("In TestRestartProjector()")

	tc.KillProjector()
	time.Sleep(30 * time.Second)

	var indexName = "index_age"
	var bucketName = "default"

	docScanResults := datautility.ExpectedScanResponse_int64(docs, "age", 0, 90, 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{0}, []interface{}{90}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	log.Printf("Len of expected and actual scan results are :  %d and %d", len(docScanResults), len(scanResults))
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

// Test with mutations delay wait of 15s
func TestDeleteDocsMutation(t *testing.T) {
	log.Printf("In TestDeleteDocsMutation()")
	var indexName = "index_age"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"age"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_int64(docs, "age", 0, 90, 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{0}, []interface{}{90}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	log.Printf("Len of expected and actual scan results are :  %d and %d", len(docScanResults), len(scanResults))
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	//Delete docs mutations:  Delete docs from KV
	DeleteDocs(200)

	docScanResults = datautility.ExpectedScanResponse_int64(docs, "age", 0, 90, 1)
	start := time.Now()
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{0}, []interface{}{90}, 1, false, defaultlimit, c.SessionConsistency, nil)
	elapsed := time.Since(start)
	log.Printf("Index Scan after mutations took %s\n", elapsed)
	FailTestIfError(err, "Error in scan", t)
	log.Printf("Len of expected and actual scan results are :  %d and %d", len(docScanResults), len(scanResults))
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

// Test with mutations delay wait of 15s
func TestUpdateDocsMutation(t *testing.T) {
	log.Printf("In TestUpdateDocsMutation()")
	var indexName = "index_age"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"age"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_int64(docs, "age", 20, 40, 2)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{20}, []interface{}{40}, 2, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	log.Printf("Len of expected and actual scan results are :  %d and %d", len(docScanResults), len(scanResults))
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	//Update docs mutations:  Update docs in KV
	UpdateDocs(100)

	docScanResults = datautility.ExpectedScanResponse_int64(docs, "age", 20, 40, 2)
	start := time.Now()
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{20}, []interface{}{40}, 2, false, defaultlimit, c.SessionConsistency, nil)
	elapsed := time.Since(start)
	log.Printf("Index Scan after mutations took %s\n", elapsed)
	FailTestIfError(err, "Error in scan", t)
	log.Printf("Len of expected and actual scan results are :  %d and %d", len(docScanResults), len(scanResults))
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

// Test with large number of mutations
func TestLargeMutations(t *testing.T) {
	log.Printf("In TestLargeMutations()")
	var index1 = "indexmut_1"
	var index2 = "indexmut_2"
	var bucketName = "default"
	var field1 = "company"
	var field2 = "gender"

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	docsToCreate := generateDocs(20000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	kv.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndex(index1, bucketName, indexManagementAddress, "", []string{field1}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanAllResponse(docs, field1)
	scanResults, err := secondaryindex.ScanAll(index1, bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
	log.Printf("Len of expected and actual scan results are :  %d and %d", len(docScanResults), len(scanResults))

	for i := 0; i <= 10; i++ {
		log.Printf("ITERATION %v\n", i)

		docsToCreate = generateDocs(10000, "users.prod")
		UpdateKVDocs(docsToCreate, docs)
		kv.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

		err := secondaryindex.CreateSecondaryIndex(index2, bucketName, indexManagementAddress, "", []string{field2}, false, nil, true, defaultIndexActiveTimeout, nil)
		FailTestIfError(err, "Error in creating the index", t)

		docScanResults = datautility.ExpectedScanAllResponse(docs, field1)
		scanResults, err = secondaryindex.ScanAll(index1, bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
		FailTestIfError(err, "Error in scan", t)
		err = tv.Validate(docScanResults, scanResults)
		FailTestIfError(err, "Error in scan result validation", t)
		log.Printf("Len of expected and actual scan results are :  %d and %d", len(docScanResults), len(scanResults))

		docScanResults = datautility.ExpectedScanAllResponse(docs, field2)
		scanResults, err = secondaryindex.ScanAll(index2, bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
		FailTestIfError(err, "Error in scan", t)
		err = tv.Validate(docScanResults, scanResults)
		FailTestIfError(err, "Error in scan result validation", t)
		log.Printf("Len of expected and actual scan results are :  %d and %d", len(docScanResults), len(scanResults))

		err = secondaryindex.DropSecondaryIndex(index2, bucketName, indexManagementAddress)
		FailTestIfError(err, "Error in drop index", t)
	}
}
