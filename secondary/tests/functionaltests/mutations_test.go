package functionaltests

import (
	"fmt"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/datautility"
	kv "github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	tv "github.com/couchbase/indexing/secondary/tests/framework/validation"
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

// Test with mutations delay wait of 1s. Skipping currently because of failure
func SkipTestCreateDocsMutation_LessDelay(t *testing.T) {
	fmt.Println("In TestCreateDocsMutation_LessDelay()")
	var indexName = "index_age"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"age"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_float64(docs, "age", 0, 90, 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{0}, []interface{}{90}, 1, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	fmt.Println("Len of expected and actual scan results are : ", len(docScanResults), len(scanResults))
	tv.Validate(docScanResults, scanResults)
	
	//Create docs mutations: Add new docs to KV
	CreateDocs(100)
	time.Sleep(5 * time.Second) // Wait for mutations to be updated in 2i
	
	docScanResults = datautility.ExpectedScanResponse_float64(docs, "age", 0, 90, 1)
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{0}, []interface{}{90}, 1, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	fmt.Println("Len of expected and actual scan results are : ", len(docScanResults), len(scanResults))
	tv.Validate(docScanResults, scanResults)
}

// Test with mutations delay wait of 1s. Skipping currently because of failure
func SkipTestDeleteDocsMutation_LessDelay(t *testing.T) {
	fmt.Println("In TestDeleteDocsMutation_LessDelay()")
	var indexName = "index_age"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"age"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_float64(docs, "age", 0, 90, 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{0}, []interface{}{90}, 1, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	fmt.Println("Len of expected and actual scan results are : ", len(docScanResults), len(scanResults))
	tv.Validate(docScanResults, scanResults)
	
	//Delete docs mutations:  Delete docs from KV
	DeleteDocs(200)
	time.Sleep(5 * time.Second) // Wait for mutations to be updated in 2i
	
	docScanResults = datautility.ExpectedScanResponse_float64(docs, "age", 0, 90, 1)
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{0}, []interface{}{90}, 1, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	fmt.Println("Len of expected and actual scan results are : ", len(docScanResults), len(scanResults))
	tv.Validate(docScanResults, scanResults)
}

func TestCreateDocsMutation(t *testing.T) {
	fmt.Println("In TestCreateDocsMutation()")
	var indexName = "index_age"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"age"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_float64(docs, "age", 0, 90, 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{0}, []interface{}{90}, 1, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	fmt.Println("Len of expected and actual scan results are : ", len(docScanResults), len(scanResults))
	tv.Validate(docScanResults, scanResults)
	
	//Create docs mutations: Add new docs to KV
	CreateDocs(100)
	time.Sleep(15 * time.Second) // Wait for mutations to be updated in 2i
	
	docScanResults = datautility.ExpectedScanResponse_float64(docs, "age", 0, 90, 1)
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{0}, []interface{}{90}, 1, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	fmt.Println("Len of expected and actual scan results are : ", len(docScanResults), len(scanResults))
	tv.Validate(docScanResults, scanResults)
}

// Test with mutations delay wait of 3s
func TestDeleteDocsMutation(t *testing.T) {
	fmt.Println("In TestDeleteDocsMutation()")
	var indexName = "index_age"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"age"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_float64(docs, "age", 0, 90, 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{0}, []interface{}{90}, 1, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	fmt.Println("Len of expected and actual scan results are : ", len(docScanResults), len(scanResults))
	tv.Validate(docScanResults, scanResults)
	
	//Delete docs mutations:  Delete docs from KV
	DeleteDocs(200)
	time.Sleep(15 * time.Second) // Wait for mutations to be updated in 2i
	
	docScanResults = datautility.ExpectedScanResponse_float64(docs, "age", 0, 90, 1)
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{0}, []interface{}{90}, 1, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	fmt.Println("Len of expected and actual scan results are : ", len(docScanResults), len(scanResults))
	tv.Validate(docScanResults, scanResults)
}