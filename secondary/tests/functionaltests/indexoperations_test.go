package functionaltests

import (
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/tests/framework/datautility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	tv "github.com/couchbase/indexing/secondary/tests/framework/validation"
	"log"
	"testing"
)

func TestScanAfterBucketPopulate(t *testing.T) {
	log.Printf("In TestScanAfterBucketPopulate()")
	log.Printf("Create an index on empty bucket, populate the bucket and Run a scan on the index")
	var indexName = "index_eyeColor"
	var bucketName = "default"
	
	docScanResults := datautility.ExpectedScanResponse_string(docs, "eyeColor", "b", "c", 3)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"b"}, []interface{}{"c"}, 3, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation: ", t)
}

func TestThreeIndexCreates(t *testing.T) {
	log.Printf("In TestThreeIndexCreates()")
	var i1 = "index_balance"
	var i2 = "index_email"
	var i3 = "index_pin"
	var bucketName = "default"

	e := secondaryindex.CreateSecondaryIndex(i1, bucketName, indexManagementAddress, "", []string{"balance"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(e, "Error in creating the index", t)

	//Create docs mutations: Add new docs to KV
	log.Printf("Create docs mutations")
	CreateDocs(100)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "balance", "$1", "$2", 2)
	scanResults, err := secondaryindex.Range(i1, bucketName, indexScanAddress, []interface{}{"$1"}, []interface{}{"$2"}, 2, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	err = secondaryindex.CreateSecondaryIndex(i2, bucketName, indexManagementAddress, "", []string{"email"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	//Create docs mutations: Add new docs to KV
	log.Printf("Create docs mutations")
	CreateDocs(100)

	docScanResults = datautility.ExpectedScanResponse_string(docs, "email", "p", "w", 1)
	scanResults, err = secondaryindex.Range(i2, bucketName, indexScanAddress, []interface{}{"p"}, []interface{}{"w"}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	err = secondaryindex.CreateSecondaryIndex(i3, bucketName, indexManagementAddress, "", []string{"address.pin"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	//Delete docs mutations:  Delete docs from KV
	log.Printf("Delete docs mutations")
	DeleteDocs(150)

	docScanResults = datautility.ExpectedScanResponse_float64(docs, "address.pin", 2222, 5555, 3)
	scanResults, err = secondaryindex.Range(i3, bucketName, indexScanAddress, []interface{}{2222}, []interface{}{5555}, 3, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestMultipleIndexCreatesDropsWithMutations(t *testing.T) {
	log.Printf("In TestThreeIndexCreates()")
	var i1 = "index_state"
	var i2 = "index_registered"
	var i3 = "index_gender"
	var i4 = "index_longitude"
	var bucketName = "default"

	//Testcase Flow
	// Create i1, create mutations, scan i1
	// Create i2, create mutations, scan i2
	// Create i3, create mutations, scan i3
	// Drop i2, create/delete mutations, scan i1
	// Create i4, create mutations, scan i4

	e := secondaryindex.CreateSecondaryIndex(i1, bucketName, indexManagementAddress, "", []string{"address.street"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(e, "Error in creating the index", t)

	//Create docs mutations: Add new docs to KV
	log.Printf("Create docs mutations")
	CreateDocs(100)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "address.street", "F", "X", 2)
	scanResults, err := secondaryindex.Range(i1, bucketName, indexScanAddress, []interface{}{"F"}, []interface{}{"X"}, 2, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	err = secondaryindex.CreateSecondaryIndex(i2, bucketName, indexManagementAddress, "", []string{"registered"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	//Create docs mutations: Add new docs to KV
	log.Printf("Create docs mutations")
	CreateDocs(100)

	docScanResults = datautility.ExpectedScanResponse_string(docs, "registered", "2014-01", "2014-09", 1)
	scanResults, err = secondaryindex.Range(i2, bucketName, indexScanAddress, []interface{}{"2014-01"}, []interface{}{"2014-09"}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	err = secondaryindex.CreateSecondaryIndex(i3, bucketName, indexManagementAddress, "", []string{"gender"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	//Create docs mutations: Add new docs to KV
	log.Printf("Create docs mutations")
	CreateDocs(100)

	docScanResults = datautility.ExpectedScanResponse_string(docs, "gender", "male", "male", 3)
	scanResults, err = secondaryindex.Lookup(i3, bucketName, indexScanAddress, []interface{}{"male"}, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	err = secondaryindex.DropSecondaryIndex(i2, bucketName, indexManagementAddress)
	FailTestIfError(err, "Error dropping index", t)

	//Create docs mutations: Add new docs to KV
	log.Printf("Create docs mutations")
	CreateDocs(100)

	//Delete docs mutations:  Delete docs from KV
	log.Printf("Delete docs mutations")
	DeleteDocs(150)

	docScanResults = datautility.ExpectedScanResponse_string(docs, "address.street", "F", "X", 2)
	scanResults, err = secondaryindex.Range(i1, bucketName, indexScanAddress, []interface{}{"F"}, []interface{}{"X"}, 2, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	err = secondaryindex.CreateSecondaryIndex(i4, bucketName, indexManagementAddress, "", []string{"longitude"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	//Create docs mutations: Add new docs to KV
	log.Printf("Create docs mutations")
	CreateDocs(100)

	docScanResults = datautility.ExpectedScanResponse_float64(docs, "longitude", -50, 200, 3)
	scanResults, err = secondaryindex.Range(i4, bucketName, indexScanAddress, []interface{}{-50}, []interface{}{200}, 3, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestCreateDropScan(t *testing.T) {
	log.Printf("In TestCreateDropScan()")
	var indexName = "index_cd"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"company"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "company", "FI", "SR", 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"FI"}, []interface{}{"SR"}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan 1", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	err = secondaryindex.DropSecondaryIndex(indexName, bucketName, indexManagementAddress)

	docScanResults = datautility.ExpectedScanResponse_string(docs, "company", "BIOSPAN", "ZILLANET", 1)
	scanResults, e := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"BIOSPAN"}, []interface{}{"ZILLANET"}, 1, true, defaultlimit, c.SessionConsistency, nil)
	if e == nil {
		t.Fatal("Error excpected when scanning for dropped index but scan didnt fail \n")
	} else {
		log.Printf("Scan failed as expected with error: %v\n", e)
	}
}

func TestCreateDropCreate(t *testing.T) {
	log.Printf("In TestCreateDropCreate()")
	var indexName = "index_cdc"
	var bucketName = "default"

	// Create an index
	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"company"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	// Scan after index create
	docScanResults := datautility.ExpectedScanResponse_string(docs, "company", "FI", "SR", 2)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"FI"}, []interface{}{"SR"}, 2, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan 1: ", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan 1 result validation: ", t)

	// Drop the created index
	err = secondaryindex.DropSecondaryIndex(indexName, bucketName, indexManagementAddress)

	// Scan after dropping the index. Error expected
	docScanResults = datautility.ExpectedScanResponse_string(docs, "company", "BIOSPAN", "ZILLANET", 0)
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"BIOSPAN"}, []interface{}{"ZILLANET"}, 0, true, defaultlimit, c.SessionConsistency, nil)
	if err == nil {
		t.Fatal("Scan 2: Error excpected when scanning for dropped index but scan didnt fail \n")
	} else {
		log.Printf("Scan 2 failed as expected with error: %v\n", err)
	}

	// Create the same index again
	err = secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"company"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	// Scan the created index with inclusion 1
	docScanResults = datautility.ExpectedScanResponse_string(docs, "company", "FI", "SR", 1)
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"FI"}, []interface{}{"SR"}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan 3: ", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan 3 result validation: ", t)
	log.Printf("(Inclusion 1) Lengths of expected and actual scan results are %d and %d. Num of docs in bucket = %d", len(docScanResults), len(scanResults), len(docs))

	// Scan the created index with inclusion 3
	docScanResults = datautility.ExpectedScanResponse_string(docs, "company", "FI", "SR", 3)
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"FI"}, []interface{}{"SR"}, 3, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan 4: ", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan 4 result validation: ", t)
	log.Printf("(Inclusion 3) Lengths of expected and actual scan results are %d and %d. Num of docs in bucket = %d", len(docScanResults), len(scanResults), len(docs))
}

func TestCreate2Drop1Scan2(t *testing.T) {
	log.Printf("In TestCreate2Drop1Scan2()")
	var index1 = "index_i1"
	var index2 = "index_i2"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(index1, bucketName, indexManagementAddress, "", []string{"company"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)
	err = secondaryindex.CreateSecondaryIndex(index2, bucketName, indexManagementAddress, "", []string{"age"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "company", "FI", "SR", 1)
	scanResults, err := secondaryindex.Range(index1, bucketName, indexScanAddress, []interface{}{"FI"}, []interface{}{"SR"}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan 1", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docScanResults = datautility.ExpectedScanResponse_float64(docs, "age", 30, 50, 1)
	scanResults, err = secondaryindex.Range(index2, bucketName, indexScanAddress, []interface{}{30}, []interface{}{50}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan 2", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	err = secondaryindex.DropSecondaryIndex(index1, bucketName, indexManagementAddress)

	docScanResults = datautility.ExpectedScanResponse_float64(docs, "age", 0, 60, 1)
	scanResults, err = secondaryindex.Range(index2, bucketName, indexScanAddress, []interface{}{0}, []interface{}{60}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan 2", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestIndexNameCaseSensitivity(t *testing.T) {
	log.Printf("In TestIndexNameCaseSensitivity()")
	var indexName = "index_age"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"age"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_float64(docs, "age", 35, 40, 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{35}, []interface{}{40}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	scanResults, err = secondaryindex.Range("index_Age", bucketName, indexScanAddress, []interface{}{35}, []interface{}{40}, 1, true, defaultlimit, c.SessionConsistency, nil)
	if err == nil {
		t.Fatal("Error excpected when scanning for non existent index but scan didnt fail \n")
	} else {
		log.Printf("Scan failed as expected with error: %v\n", err)
	}
}

func TestCreateDuplicateIndex(t *testing.T) {
	log.Printf("In TestCreateDuplicateIndex()")
	var index1 = "index_di1"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(index1, bucketName, indexManagementAddress, "", []string{"age"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)
	err = secondaryindex.CreateSecondaryIndex(index1, bucketName, indexManagementAddress, "", []string{"age"}, false, nil, false, defaultIndexActiveTimeout, nil)
	if err == nil {
		t.Fatal("Error excpected creating dupliate index but create didnt fail \n")
	} else {
		log.Printf("Create failed as expected with error: %v\n", err)
	}
}

// Negative test - Drop a secondary index that doesnt exist
func TestDropNonExistingIndex(t *testing.T) {
	log.Printf("In TestDropNonExistingIndex()")
	err := secondaryindex.DropSecondaryIndexByID(123456, indexManagementAddress)
	if err == nil {
		t.Fatal("Error excpected when deleting non existent index but index drop didnt fail \n")
	} else {
		log.Printf("Index drop failed as expected with error: %v", err)
	}
}

func TestCreateIndexNonExistentBucket(t *testing.T) {
	log.Printf("In TestCreateIndexNonExistentBucket()")
	var indexName = "index_BlahBucket"
	var bucketName = "BlahBucket"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"score"}, false, nil, true, defaultIndexActiveTimeout, nil)
	if err == nil {
		t.Fatal("Error excpected when creating index on non-existent bucket but error didnt occur\n")
	} else {
		log.Printf("Index create failed as expected with error: %v", err)
	}
}

func TestScanWithNoTimeout(t *testing.T) {
	log.Printf("Create an index on empty bucket, populate the bucket and Run a scan on the index")
	var indexName = "index_eyeColor"
	var bucketName = "default"

	err := secondaryindex.ChangeIndexerSettings("indexer.settings.scan_timeout", float64(0), clusterconfig.Username, clusterconfig.Password, kvaddress)
	FailTestIfError(err, "Error in ChangeIndexerSettings", t)

	CreateDocs(100)
	docScanResults := datautility.ExpectedScanResponse_string(docs, "eyeColor", "b", "c", 3)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"b"}, []interface{}{"c"}, 3, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation: ", t)
}
