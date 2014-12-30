package functionaltests

import (
	"fmt"
	"github.com/couchbase/indexing/secondary/tests/framework/datautility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	tv "github.com/couchbase/indexing/secondary/tests/framework/validation"
	"testing"
	"time"
)

func TestCreateDropScan(t *testing.T) {
	fmt.Println("In TestCreateDropScan()")
	var indexName = "index_cd"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"company"}, true)
	FailTestIfError(err, "Error in creating the index", t)
	time.Sleep(1 * time.Second)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "company", "FI", "SR", 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"FI"}, []interface{}{"SR"}, 1, true, defaultlimit)
	FailTestIfError(err, "Error in scan 1", t)
	tv.Validate(docScanResults, scanResults)

	err = secondaryindex.DropSecondaryIndex(indexName, bucketName, indexManagementAddress)

	docScanResults = datautility.ExpectedScanResponse_string(docs, "company", "BIOSPAN", "ZILLANET", 1)
	scanResults, e := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"BIOSPAN"}, []interface{}{"ZILLANET"}, 1, true, defaultlimit)
	if e == nil {
		t.Fatal("Error excpected when scanning for dropped index but scan didnt fail \n")
	} else {
		fmt.Printf("Scan failed as expected with error: %v\n", e)
	}
}

func TestCreateDropCreate(t *testing.T) {
	fmt.Println("In TestCreateDropCreate()")
	var indexName = "index_cdc"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"company"}, true)
	FailTestIfError(err, "Error in creating the index", t)
	time.Sleep(1 * time.Second)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "company", "FI", "SR", 2)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"FI"}, []interface{}{"SR"}, 2, true, defaultlimit)
	FailTestIfError(err, "Error in scan 1", t)
	tv.Validate(docScanResults, scanResults)

	err = secondaryindex.DropSecondaryIndex(indexName, bucketName, indexManagementAddress)
	time.Sleep(1 * time.Second)

	docScanResults = datautility.ExpectedScanResponse_string(docs, "company", "BIOSPAN", "ZILLANET", 0)
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"BIOSPAN"}, []interface{}{"ZILLANET"}, 0, true, defaultlimit)
	if err == nil {
		t.Fatal("Error excpected when scanning for dropped index but scan didnt fail \n")
	} else {
		fmt.Printf("Scan failed as expected with error: %v\n", err)
	}

	err = secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"company"}, true)
	FailTestIfError(err, "Error in creating the index", t)
	time.Sleep(1 * time.Second)

	docScanResults = datautility.ExpectedScanResponse_string(docs, "company", "FI", "SR", 1)
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"FI"}, []interface{}{"SR"}, 3, true, defaultlimit)
	FailTestIfError(err, "Error in scan 2", t)
	tv.Validate(docScanResults, scanResults)
}

func SkipTestCreate2Drop1Scan2(t *testing.T) {
	fmt.Println("In TestCreate2Drop1Scan2()")
	var index1 = "index_i1"
	var index2 = "index_i2"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(index1, bucketName, indexManagementAddress, []string{"company"}, true)
	FailTestIfError(err, "Error in creating the index", t)
	err = secondaryindex.CreateSecondaryIndex(index2, bucketName, indexManagementAddress, []string{"age"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	time.Sleep(2 * time.Second)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "company", "FI", "SR", 1)
	scanResults, err := secondaryindex.Range(index1, bucketName, indexScanAddress, []interface{}{"FI"}, []interface{}{"SR"}, 1, true, defaultlimit)
	FailTestIfError(err, "Error in scan 1", t)
	tv.Validate(docScanResults, scanResults)

	docScanResults = datautility.ExpectedScanResponse_float64(docs, "age", 30, 50, 1)
	scanResults, err = secondaryindex.Range(index2, bucketName, indexScanAddress, []interface{}{30}, []interface{}{50}, 1, true, defaultlimit)
	FailTestIfError(err, "Error in scan 2", t)
	tv.Validate(docScanResults, scanResults)

	err = secondaryindex.DropSecondaryIndex(index1, bucketName, indexManagementAddress)
	time.Sleep(2 * time.Second)

	docScanResults = datautility.ExpectedScanResponse_float64(docs, "age", 0, 60, 1)
	scanResults, err = secondaryindex.Range(index2, bucketName, indexScanAddress, []interface{}{0}, []interface{}{60}, 1, true, defaultlimit)
	FailTestIfError(err, "Error in scan 2", t)
	tv.Validate(docScanResults, scanResults)
}

func TestIndexNameCaseSensitivity(t *testing.T) {
	fmt.Println("In TestIndexNameCaseSensitivity()")
	var indexName = "index_age"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"age"}, true)
	FailTestIfError(err, "Error in creating the index", t)
	time.Sleep(1 * time.Second)

	docScanResults := datautility.ExpectedScanResponse_float64(docs, "age", 35, 40, 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{35}, []interface{}{40}, 1, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)

	scanResults, err = secondaryindex.Range("index_Age", bucketName, indexScanAddress, []interface{}{35}, []interface{}{40}, 1, true, defaultlimit)
	if err == nil {
		t.Fatal("Error excpected when scanning for non existent index but scan didnt fail \n")
	} else {
		fmt.Printf("Scan failed as expected with error: %v\n", err)
	}
}

func TestCreateDuplicateIndex(t *testing.T) {
	fmt.Println("In TestCreateDuplicateIndex()")
	var index1 = "index_di1"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(index1, bucketName, indexManagementAddress, []string{"age"}, true)
	FailTestIfError(err, "Error in creating the index", t)
	err = secondaryindex.CreateSecondaryIndex(index1, bucketName, indexManagementAddress, []string{"age"}, false)
	if err == nil {
		t.Fatal("Error excpected creating dupliate index but create didnt fail \n")
	} else {
		fmt.Printf("Create failed as expected with error: %v\n", err)
	}
}

// Negative test - Drop a secondary index that doesnt exist
func TestDropNonExistingIndex(t *testing.T) {
	fmt.Println("In TestDropNonExistingIndex()")
	var indexDefnId = "index_IDontExist"
	err := secondaryindex.DropSecondaryIndexByID(indexDefnId, indexManagementAddress)
	if err == nil {
		t.Fatal("Error excpected when deleting non existent index but index drop didnt fail \n")
	} else {
		fmt.Println("Index drop failed as expected with error: ", err)
	}
}

// Skipping right now as this causes further tests to fail
func SkipTestCreateIndexNonExistentBucket(t *testing.T) {
	fmt.Println("In TestCreateIndexNonExistentBucket()")
	var indexName = "index_BucketTest1"
	var bucketName = "test1"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"score"}, true)
	FailTestIfError(err, "Error in creating the index", t)
}
