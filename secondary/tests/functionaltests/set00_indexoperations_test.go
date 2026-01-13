package functionaltests

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"testing"
	"time"

	"gopkg.in/couchbase/gocb.v1"

	"github.com/couchbase/gometa/repository"
	c "github.com/couchbase/indexing/secondary/common"
	json "github.com/couchbase/indexing/secondary/common/json"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/datautility"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	tv "github.com/couchbase/indexing/secondary/tests/framework/validation"
)

func TestScanAfterBucketPopulate(t *testing.T) {
	log.Printf("In TestScanAfterBucketPopulate()")
	log.Printf("Create an index on empty bucket, populate the bucket and Run a scan on the index")
	var indexName = "index_eyeColor"
	var bucketName = "default"

	docScanResults := datautility.ExpectedScanResponse_string(docs, "eyeColor", "b", "c", 3)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"b"}, []interface{}{"c"}, 3, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation: ", t)
}

func TestRestartNilSnapshot(t *testing.T) {
	log.Printf("In TestRestartNilSnapshot()")

	var err error

	err = secondaryindex.CreateSecondaryIndex("idx_age", "default", indexManagementAddress, "", []string{"age"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	// Restart indexer process and wait for some time.
	log.Printf("Restarting indexer process ...")
	tc.KillIndexer()
	time.Sleep(200 * time.Second)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "eyeColor", "b", "c", 3)
	scanResults, err1 := secondaryindex.Range("index_eyeColor", "default", indexScanAddress, []interface{}{"b"}, []interface{}{"c"}, 3, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err1, "Error in scan", t)
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
	scanResults, err := secondaryindex.Range(i1, bucketName, indexScanAddress, []interface{}{"$1"}, []interface{}{"$2"}, 2, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	err = secondaryindex.CreateSecondaryIndex(i2, bucketName, indexManagementAddress, "", []string{"email"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	//Create docs mutations: Add new docs to KV
	log.Printf("Create docs mutations")
	CreateDocs(100)

	docScanResults = datautility.ExpectedScanResponse_string(docs, "email", "p", "w", 1)
	scanResults, err = secondaryindex.Range(i2, bucketName, indexScanAddress, []interface{}{"p"}, []interface{}{"w"}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	err = secondaryindex.CreateSecondaryIndex(i3, bucketName, indexManagementAddress, "", []string{"address.pin"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	//Delete docs mutations:  Delete docs from KV
	log.Printf("Delete docs mutations")
	DeleteDocs(150)

	docScanResults = datautility.ExpectedScanResponse_int64(docs, "address.pin", 2222, 5555, 3)
	scanResults, err = secondaryindex.Range(i3, bucketName, indexScanAddress, []interface{}{2222}, []interface{}{5555}, 3, false, defaultlimit, c.SessionConsistency, nil)
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
	scanResults, err := secondaryindex.Range(i1, bucketName, indexScanAddress, []interface{}{"F"}, []interface{}{"X"}, 2, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	err = secondaryindex.CreateSecondaryIndex(i2, bucketName, indexManagementAddress, "", []string{"registered"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	//Create docs mutations: Add new docs to KV
	log.Printf("Create docs mutations")
	CreateDocs(100)

	docScanResults = datautility.ExpectedScanResponse_string(docs, "registered", "2014-01", "2014-09", 1)
	scanResults, err = secondaryindex.Range(i2, bucketName, indexScanAddress, []interface{}{"2014-01"}, []interface{}{"2014-09"}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	err = secondaryindex.CreateSecondaryIndex(i3, bucketName, indexManagementAddress, "", []string{"gender"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	//Create docs mutations: Add new docs to KV
	log.Printf("Create docs mutations")
	CreateDocs(100)

	docScanResults = datautility.ExpectedScanResponse_string(docs, "gender", "male", "male", 3)
	scanResults, err = secondaryindex.Lookup(i3, bucketName, indexScanAddress, []interface{}{"male"}, false, defaultlimit, c.SessionConsistency, nil)
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
	scanResults, err = secondaryindex.Range(i1, bucketName, indexScanAddress, []interface{}{"F"}, []interface{}{"X"}, 2, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	err = secondaryindex.CreateSecondaryIndex(i4, bucketName, indexManagementAddress, "", []string{"longitude"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	//Create docs mutations: Add new docs to KV
	log.Printf("Create docs mutations")
	CreateDocs(100)

	docScanResults = datautility.ExpectedScanResponse_float64(docs, "longitude", -50, 200, 3)
	scanResults, err = secondaryindex.Range(i4, bucketName, indexScanAddress, []interface{}{-50}, []interface{}{200}, 3, false, defaultlimit, c.SessionConsistency, nil)
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
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"FI"}, []interface{}{"SR"}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan 1", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	err = secondaryindex.DropSecondaryIndex(indexName, bucketName, indexManagementAddress)

	docScanResults = datautility.ExpectedScanResponse_string(docs, "company", "BIOSPAN", "ZILLANET", 1)
	scanResults, e := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"BIOSPAN"}, []interface{}{"ZILLANET"}, 1, false, defaultlimit, c.SessionConsistency, nil)
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
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"FI"}, []interface{}{"SR"}, 2, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan 1: ", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan 1 result validation: ", t)

	// Drop the created index
	err = secondaryindex.DropSecondaryIndex(indexName, bucketName, indexManagementAddress)

	// Scan after dropping the index. Error expected
	docScanResults = datautility.ExpectedScanResponse_string(docs, "company", "BIOSPAN", "ZILLANET", 0)
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"BIOSPAN"}, []interface{}{"ZILLANET"}, 0, false, defaultlimit, c.SessionConsistency, nil)
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
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"FI"}, []interface{}{"SR"}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan 3: ", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan 3 result validation: ", t)
	log.Printf("(Inclusion 1) Lengths of expected and actual scan results are %d and %d. Num of docs in bucket = %d", len(docScanResults), len(scanResults), len(docs))

	// Scan the created index with inclusion 3
	docScanResults = datautility.ExpectedScanResponse_string(docs, "company", "FI", "SR", 3)
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"FI"}, []interface{}{"SR"}, 3, false, defaultlimit, c.SessionConsistency, nil)
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
	scanResults, err := secondaryindex.Range(index1, bucketName, indexScanAddress, []interface{}{"FI"}, []interface{}{"SR"}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan 1", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docScanResults = datautility.ExpectedScanResponse_int64(docs, "age", 30, 50, 1)
	scanResults, err = secondaryindex.Range(index2, bucketName, indexScanAddress, []interface{}{30}, []interface{}{50}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan 2", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	err = secondaryindex.DropSecondaryIndex(index1, bucketName, indexManagementAddress)

	docScanResults = datautility.ExpectedScanResponse_int64(docs, "age", 0, 60, 1)
	scanResults, err = secondaryindex.Range(index2, bucketName, indexScanAddress, []interface{}{0}, []interface{}{60}, 1, false, defaultlimit, c.SessionConsistency, nil)
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

	docScanResults := datautility.ExpectedScanResponse_int64(docs, "age", 35, 40, 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{35}, []interface{}{40}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	scanResults, err = secondaryindex.Range("index_Age", bucketName, indexScanAddress, []interface{}{35}, []interface{}{40}, 1, false, defaultlimit, c.SessionConsistency, nil)
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
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"b"}, []interface{}{"c"}, 3, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation: ", t)
}

func TestIndexingOnBinaryBucketMeta(t *testing.T) {
	log.Printf("In TestIndexingOnBinaryBucketMeta()")
	log.Printf("\t 1. Populate a bucekt with binary docs and create indexs on the `id`, `cas` and `expiration` fields of Metadata")
	log.Printf("\t 2. Validate the test by comparing the items_count of indexes and the number of docs in the bucket for each of the fields")
	indexName_id := "index_binary_meta_id"
	indexName_cas := "index_binary_meta_cas"
	indexName_expiration := "index_binary_meta_expiration"
	bucket1 := "default"
	bucket2 := "binaryBucket"
	numDocs := 10
	kvutility.EditBucket(bucket1, "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256")
	kvutility.CreateBucket(bucket2, "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256", "")
	time.Sleep(bucketOpWaitDur * time.Second)

	docs := GenerateBinaryDocs(numDocs, bucket2, "", clusterconfig.KVAddress, clusterconfig.Username, clusterconfig.Password)

	err := secondaryindex.CreateSecondaryIndex(indexName_id, bucket2, indexManagementAddress, "", []string{"meta().id"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error creating"+indexName_id+" on binary docs", t)
	time.Sleep(5 * time.Second)

	stats := secondaryindex.GetIndexStats(indexName_id, bucket2, clusterconfig.Username, clusterconfig.Password, kvaddress)
	itemsCount := stats[bucket2+":"+indexName_id+":items_count"].(float64)
	log.Printf("items_count stat is %v for index %v", itemsCount, indexName_id)

	if itemsCount != float64(len(docs)) {
		log.Printf("Expected number items count = %v, actual items_count stat returned = %v", len(docs), itemsCount)
		err = errors.New("items_count is incorrect for index " + indexName_id)
		FailTestIfError(err, "Error in TestIndexingOnBinaryBucketMeta", t)
	}

	err = secondaryindex.DropSecondaryIndex(indexName_id, bucket2, indexManagementAddress)
	FailTestIfError(err, "Error dropping "+indexName_id, t)

	// Test for CAS
	err = secondaryindex.CreateSecondaryIndex(indexName_cas, bucket2, indexManagementAddress, "", []string{"meta().cas"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error creating"+indexName_cas+" on binary docs", t)
	time.Sleep(5 * time.Second)

	stats = secondaryindex.GetIndexStats(indexName_cas, bucket2, clusterconfig.Username, clusterconfig.Password, kvaddress)
	itemsCount = stats[bucket2+":"+indexName_cas+":items_count"].(float64)
	log.Printf("items_count stat is %v for index %v", itemsCount, indexName_cas)

	if itemsCount != float64(len(docs)) {
		log.Printf("Expected number items count = %v, actual items_count stat returned = %v", len(docs), itemsCount)
		err = errors.New("items_count is incorrect for index " + indexName_cas)
		FailTestIfError(err, "Error in TestIndexingOnBinaryBucketMeta", t)
	}

	err = secondaryindex.DropSecondaryIndex(indexName_cas, bucket2, indexManagementAddress)
	FailTestIfError(err, "Error dropping "+indexName_cas, t)

	// Test for expiration
	err = secondaryindex.CreateSecondaryIndex(indexName_expiration, bucket2, indexManagementAddress, "", []string{"meta().expiration"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error creating"+indexName_cas+" on binary docs", t)
	time.Sleep(5 * time.Second)

	stats = secondaryindex.GetIndexStats(indexName_expiration, bucket2, clusterconfig.Username, clusterconfig.Password, kvaddress)
	itemsCount = stats[bucket2+":"+indexName_expiration+":items_count"].(float64)
	log.Printf("items_count stat is %v for index %v", itemsCount, indexName_expiration)

	if itemsCount != float64(len(docs)) {
		log.Printf("Expected number items count = %v, actual items_count stat returned = %v", len(docs), itemsCount)
		err = errors.New("items_count is incorrect for index " + indexName_expiration)
		FailTestIfError(err, "Error in TestIndexingOnBinaryBucketMeta", t)
	}
	err = secondaryindex.DropSecondaryIndex(indexName_expiration, bucket2, indexManagementAddress)
	FailTestIfError(err, "Error dropping "+indexName_expiration, t)

	kvutility.DeleteBucket(bucket2, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	secondaryindex.RemoveClientForBucket(kvaddress, bucket2)
	kvutility.EditBucket(bucket1, "", clusterconfig.Username, clusterconfig.Password, kvaddress, "512")
	time.Sleep(bucketOpWaitDur * time.Second) // Sleep after bucket create or delete
}

func TestRetainDeleteXATTRBinaryDocs(t *testing.T) {
	log.Printf("In TestRetainDeleteXATTRBinaryDocs()")
	log.Printf("\t 1. Populate a bucket with binary docs having system XATTRS")
	log.Printf("\t 2. Create index on the system XATTRS with \"retain_deleted_xattr\" attribute set to true")
	log.Printf("\t 3. Delete the documents in the bucket")
	log.Printf("\t 4. Query for the meta() information in the source bucket. The total number of results should be equivalent to the number of documents in the bucket before deletion of documents")

	indexname_xattr := "index_system_xattr"
	bucket1 := "default"
	bucket2 := "binaryBucket"
	numDocs := 10
	kvutility.EditBucket(bucket1, "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256")
	kvutility.CreateBucket(bucket2, "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256", "")
	time.Sleep(bucketOpWaitDur * time.Second)

	docs := GenerateBinaryDocsWithXATTRS(numDocs, bucket2, "", "http://"+kvaddress, clusterconfig.Username, clusterconfig.Password)

	err := secondaryindex.CreateSecondaryIndex(indexname_xattr, bucket2, indexManagementAddress, "", []string{"meta().xattrs._sync1"}, false, []byte("{\"retain_deleted_xattr\":true}"), true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error creating"+indexname_xattr+" on binary docs", t)
	time.Sleep(5 * time.Second)

	stats := secondaryindex.GetIndexStats(indexname_xattr, bucket2, clusterconfig.Username, clusterconfig.Password, kvaddress)
	itemsCount := stats[bucket2+":"+indexname_xattr+":items_count"].(float64)

	if itemsCount != float64(len(docs)) {
		log.Printf("Expected number items count = %v, actual items_count stat returned = %v", len(docs), itemsCount)
		err = errors.New("items_count is incorrect for index " + indexname_xattr)
		FailTestIfError(err, "Error in TestRetainDeleteXATTRBinaryDocs", t)
	}

	// Delete the documents inside the bucket
	kvutility.DeleteKeys(docs, bucket2, "", kvaddress)
	log.Printf("Deleted all the documents in the bucket: " + bucket2 + " successfully")

	//Now execute a n1ql query on Source bucket
	n1qlstatement := "select meta() from " + bucket2 + " where META().xattrs._sync1 is not null"
	results, err := tc.ExecuteN1QLStatement(clusterconfig.KVAddress, clusterconfig.Username, clusterconfig.Password, bucket2, n1qlstatement, false, gocb.RequestPlus)
	FailTestIfError(err, "Error in creating primary index", t)

	if itemsCount != float64(len(results)) {
		log.Printf("Expected number of results = %v, actual items_count stat returned = %v", len(results), itemsCount)
		err = errors.New("resulsts are incorrect for index " + indexname_xattr)
		FailTestIfError(err, "Error in TestRetainDeleteXATTRBinaryDocs", t)
	}

	kvutility.DeleteBucket(bucket2, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	secondaryindex.RemoveClientForBucket(kvaddress, bucket2)
	kvutility.EditBucket(bucket1, "", clusterconfig.Username, clusterconfig.Password, kvaddress, "512")
	time.Sleep(bucketOpWaitDur * time.Second) // Sleep after bucket create or delete*/
}

// This test will create multiple xattr fields and indexes them
// Compares the index results with primary index scan.
// Run this test only for plasma & memdb as FDB does not support
// large keys
func TestIndexingOnXATTRs(t *testing.T) {
	log.Printf("In TestIndexingOnXATTRs()")
	if secondaryindex.IndexUsing == "forestdb" {
		log.Printf("Skipping test TestIndexingOnXATTRs() for forestdb")
		return
	}

	bucket1 := "default"
	bucket2 := "bucket_xattrs"
	numDocs := 100
	xattrs := map[string]string{"_sync.rev": randSpecialString(10),
		"_sync.channels": randSpecialString(10000),
		"_sync.test":     randSpecialString(1000),
		"_sync.history":  randSpecialString(100000)}

	index_sync_rev := "index_sync_rev"
	index_sync_channels := "index_sync_channels"
	index_sync_test := "index_sync_test"

	kvutility.EditBucket(bucket1, "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256")
	kvutility.CreateBucket(bucket2, "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256", "")
	time.Sleep(bucketOpWaitDur * time.Second)

	docs := GenerateDocsWithXATTRS(numDocs, bucket2, "", "http://"+kvaddress, clusterconfig.Username, clusterconfig.Password, xattrs)

	err := secondaryindex.CreateSecondaryIndex(index_sync_rev, bucket2, indexManagementAddress, "", []string{"meta().xattrs._sync.rev"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error creating"+index_sync_rev+" on xattr docs", t)

	err = secondaryindex.CreateSecondaryIndex(index_sync_channels, bucket2, indexManagementAddress, "", []string{"meta().xattrs._sync.channels"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error creating"+index_sync_channels+" on xattr docs", t)

	err = secondaryindex.CreateSecondaryIndex(index_sync_test, bucket2, indexManagementAddress, "", []string{"meta().xattrs._sync.test"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error creating"+index_sync_test+" on xattr docs", t)

	time.Sleep(5 * time.Second)

	validateItemsCount := func(indexName string) {
		stats := secondaryindex.GetIndexStats(indexName, bucket2, clusterconfig.Username, clusterconfig.Password, kvaddress)
		itemsCount := stats[bucket2+":"+indexName+":items_count"].(float64)
		log.Printf("items_count stat is %v for index %v", itemsCount, indexName)

		if itemsCount != float64(len(docs)) {
			log.Printf("Expected number items count = %v, actual items_count stat returned = %v", len(docs), itemsCount)
			err = errors.New("items_count is incorrect for index " + indexName)
			FailTestIfError(err, "Error in TestIndexingOnXATTRs", t)
		}
	}

	validateItemsCount(index_sync_rev)
	validateItemsCount(index_sync_channels)
	validateItemsCount(index_sync_test)

	validateScanResultsAndDropIndex := func(indexName string) {
		results, err := secondaryindex.ScanAll(indexName, bucket2, indexManagementAddress, defaultlimit, c.SessionConsistency, nil)
		FailTestIfError(err, "Error while scanning index_sync_rev", t)

		if len(results) != len(docs) {
			t.Fatalf("The number of entries in index do not match total docs. Index: %v, len(results): %v, len(docs): %v", indexName, len(results), len(docs))
		}

		for docId, _ := range docs {
			if val, ok := results[docId]; !ok {
				t.Fatalf("DocID: %v present in input docs but not in result. indexName: %v", docId, indexName)
			} else {
				var xattrVal string
				switch indexName {
				case index_sync_rev:
					xattrVal = xattrs["_sync.rev"]
				case index_sync_channels:
					xattrVal = xattrs["_sync.channels"]
				case index_sync_test:
					xattrVal = xattrs["_sync.test"]
				}

				if len(val) > 1 {
					t.Fatalf("Expected only one entry in results for index: %v, docId: %v, expected: %v, actual: %v", indexName, docId, xattrVal, val)
				}
				if val[0].ToString() != xattrVal {
					t.Fatalf("Incorrect xattrVal seen for index: %v, docId: %v, expected: %v, actual: %v", indexName, docId, xattrVal, val[0].ToString())
				}
			}
		}
		err = secondaryindex.DropSecondaryIndex(indexName, bucket2, kvaddress)
		FailTestIfError(err, fmt.Sprintf("Error dropping index: %v", indexName), t)
	}

	validateScanResultsAndDropIndex(index_sync_rev)
	validateScanResultsAndDropIndex(index_sync_channels)
	validateScanResultsAndDropIndex(index_sync_test)

	kvutility.DeleteBucket(bucket2, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	secondaryindex.RemoveClientForBucket(kvaddress, bucket2)
	kvutility.EditBucket(bucket1, "", clusterconfig.Username, clusterconfig.Password, kvaddress, "512")
	time.Sleep(bucketOpWaitDur * time.Second) // Sleep after bucket create or delete*/
}

func TestIndexMetadataStore(t *testing.T) {
	log.Printf("In %v()", t.Name())

	nodes, err := secondaryindex.GetIndexerNodes(kvaddress)
	tc.HandleError(err, "failed to read couchbase nodes")

	for _, node := range nodes {
		metastats := getMetastoreStats(node.Hostname, t)
		if metastats.Type != repository.MagmaStoreType {
			t.Errorf("Expected node %v to have %s store but has %s",
				node.Hostname, repository.MagmaStoreType, metastats.Type)
		}
	}
}

func getMetastoreStats(nodeAddr string, t *testing.T) *repository.MetastoreStats {
	idxAddr := secondaryindex.GetIndexHttpAddrOnNode(
		clusterconfig.Username,
		clusterconfig.Password,
		nodeAddr,
	)
	metaUrl := "http://" + idxAddr + "/stats/metadata"

	client := &http.Client{}
	defer client.CloseIdleConnections()
	log.Printf("for IndexMetadataStore %v", metaUrl)
	req, _ := http.NewRequest("GET", metaUrl, nil)
	req.SetBasicAuth(clusterconfig.Username, clusterconfig.Password)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")

	resp, err := client.Do(req)
	tc.HandleError(err, "failed to read indexer metadata")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		t.Fatalf("GET /stats/metadata failed")
	}

	metastats := new(repository.MetastoreStats)
	body, _ := ioutil.ReadAll(resp.Body)
	err = json.Unmarshal(body, metastats)
	tc.HandleError(err, "GET /stats/metadata :: Unmarshal of response body")

	return metastats
}
