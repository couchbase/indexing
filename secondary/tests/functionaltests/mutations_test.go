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

// Test with mutations delay wait of 1s. Skipping currently because of failure
func SkipTestCreateDocsMutation_LessDelay(t *testing.T) {
	log.Printf("In TestCreateDocsMutation_LessDelay()")
	var indexName = "index_age"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"age"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_float64(docs, "age", 0, 90, 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{0}, []interface{}{90}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	log.Printf("Len of expected and actual scan results are : %d and %d", len(docScanResults), len(scanResults))
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	//Create docs mutations: Add new docs to KV
	CreateDocs(100)
	time.Sleep(5 * time.Second) // Wait for mutations to be updated in 2i

	docScanResults = datautility.ExpectedScanResponse_float64(docs, "age", 0, 90, 1)
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{0}, []interface{}{90}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	log.Printf("Len of expected and actual scan results are :  %d and %d", len(docScanResults), len(scanResults))
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

// Test with mutations delay wait of 1s. Skipping currently because of failure
func SkipTestDeleteDocsMutation_LessDelay(t *testing.T) {
	log.Printf("In TestDeleteDocsMutation_LessDelay()")
	var indexName = "index_age"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"age"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_float64(docs, "age", 0, 90, 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{0}, []interface{}{90}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	log.Printf("Len of expected and actual scan results are :  %d and %d", len(docScanResults), len(scanResults))
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	//Delete docs mutations:  Delete docs from KV
	DeleteDocs(200)
	time.Sleep(5 * time.Second) // Wait for mutations to be updated in 2i

	docScanResults = datautility.ExpectedScanResponse_float64(docs, "age", 0, 90, 1)
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{0}, []interface{}{90}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	log.Printf("Len of expected and actual scan results are :  %d and %d", len(docScanResults), len(scanResults))
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func SkipTestRestartIndexer(t *testing.T) {
	log.Printf("In TestRestartIndexer()")

	tc.KillIndexer()
	time.Sleep(60 * time.Second)

	var indexName = "index_age"
	var bucketName = "default"

	docScanResults := datautility.ExpectedScanResponse_float64(docs, "age", 0, 90, 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{0}, []interface{}{90}, 1, true, defaultlimit, c.SessionConsistency, nil)
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

	docScanResults := datautility.ExpectedScanResponse_float64(docs, "age", 0, 90, 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{0}, []interface{}{90}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	log.Printf("Len of expected and actual scan results are :  %d and %d", len(docScanResults), len(scanResults))
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	//Create docs mutations: Add new docs to KV
	CreateDocs(100)

	docScanResults = datautility.ExpectedScanResponse_float64(docs, "age", 0, 90, 1)
	start := time.Now()
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{0}, []interface{}{90}, 1, true, defaultlimit, c.SessionConsistency, nil)
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
	time.Sleep(60 * time.Second)

	var indexName = "index_age"
	var bucketName = "default"

	docScanResults := datautility.ExpectedScanResponse_float64(docs, "age", 0, 90, 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{0}, []interface{}{90}, 1, true, defaultlimit, c.SessionConsistency, nil)
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

	docScanResults := datautility.ExpectedScanResponse_float64(docs, "age", 0, 90, 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{0}, []interface{}{90}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	log.Printf("Len of expected and actual scan results are :  %d and %d", len(docScanResults), len(scanResults))
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	//Delete docs mutations:  Delete docs from KV
	DeleteDocs(200)

	docScanResults = datautility.ExpectedScanResponse_float64(docs, "age", 0, 90, 1)
	start := time.Now()
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{0}, []interface{}{90}, 1, true, defaultlimit, c.SessionConsistency, nil)
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

	docScanResults := datautility.ExpectedScanResponse_float64(docs, "age", 20, 40, 2)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{20}, []interface{}{40}, 2, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	log.Printf("Len of expected and actual scan results are :  %d and %d", len(docScanResults), len(scanResults))
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	//Update docs mutations:  Update docs in KV
	UpdateDocs(100)

	docScanResults = datautility.ExpectedScanResponse_float64(docs, "age", 20, 40, 2)
	start := time.Now()
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{20}, []interface{}{40}, 2, true, defaultlimit, c.SessionConsistency, nil)
	elapsed := time.Since(start)
	log.Printf("Index Scan after mutations took %s\n", elapsed)
	FailTestIfError(err, "Error in scan", t)
	log.Printf("Len of expected and actual scan results are :  %d and %d", len(docScanResults), len(scanResults))
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}
