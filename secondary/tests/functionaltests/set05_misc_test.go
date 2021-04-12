package functionaltests

import (
	"errors"
	"log"
	"path/filepath"
	"testing"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/datautility"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	tv "github.com/couchbase/indexing/secondary/tests/framework/validation"
)

// After bucket delete:-
// 1) query for old index before loading bucket
// 2) query for old index after loading bucket
// 3) create new indexes and query
// 4) list indexes: should list only new indexes
func TestBucketDefaultDelete(t *testing.T) {
	kvutility.DeleteBucket("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	secondaryindex.RemoveClientForBucket(kvaddress, "default")
	time.Sleep(bucketOpWaitDur * time.Second) // Sleep after bucket create or delete
	kvutility.CreateBucket("default", "none", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256", "11212")
	time.Sleep(bucketOpWaitDur * time.Second) // Sleep after bucket create or delete

	tc.ClearMap(docs)
	tc.ClearMap(mut_docs)
	docs = datautility.LoadJSONFromCompressedFile(dataFilePath, "docid")
	mut_docs = datautility.LoadJSONFromCompressedFile(mutationFilePath, "docid")
	log.Printf("Populating the default bucket")
	kvutility.SetKeyValues(docs, "default", "", clusterconfig.KVAddress)
	time.Sleep(2 * time.Second)

	var indexName = "index_isActive"
	var bucketName = "default"

	scanResults, err := secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{"BIOSPAN"}, false, defaultlimit, c.SessionConsistency, nil)
	if err == nil {
		log.Printf("Scan did not fail as expected. Got scanresults: %v\n", scanResults)
		e := errors.New("Scan did not fail as expected after bucket delete")
		FailTestIfError(e, "Error in TestBucketDefaultDelete", t)
	} else {
		log.Printf("Scan failed as expected with error: %v", err)
	}

	log.Printf("Populating the default bucket after it was deleted")
	kvutility.SetKeyValues(docs, "default", "", clusterconfig.KVAddress)

	err = secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"company"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "company", "BIOSPAN", "BIOSPAN", 3)
	scanResults, err = secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{"BIOSPAN"}, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
	// todo: list the index and confirm there is only index created
}

func TestMixedDatatypesScanAll(t *testing.T) {
	log.Printf("In TestMixedDatatypesScanAll()")
	log.Printf("Before test begin: Length of kv docs is %d", len(docs))
	field := "md_street"
	indexName := "index_mixeddt"
	bucketName := "default"

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	docsToCreate := generateJSONSMixedDatatype(1000, "md_street")
	log.Printf("After generate docs: Length of kv docs is %d", len(docs))
	seed++
	log.Printf("Setting mixed datatypes JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{field}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanAllResponse(docs, field)
	scanResults, err := secondaryindex.ScanAll(indexName, bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
	log.Printf("Lengths of expected and actual scan results are:  %d and %d", len(docScanResults), len(scanResults))
	log.Printf("End: Length of kv docs is %d", len(docs))
}

func TestMixedDatatypesRange_Float(t *testing.T) {
	log.Printf("In TestMixedDatatypesRange_Float()")

	field := "mixed_field"
	indexName := "index_mixeddt"
	bucketName := "default"

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	docsToCreate := generateJSONSMixedDatatype(1000, field)
	seed++
	log.Printf("Setting mixed datatypes JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{field}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_int64(docs, field, 100, 1000, 3)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{100}, []interface{}{1000}, 3, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
	log.Printf("Lengths of expected and actual scan results are:  %d and %d", len(docScanResults), len(scanResults))

	docScanResults = datautility.ExpectedScanResponse_int64(docs, field, 1, 100, 2)
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{1}, []interface{}{100}, 2, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
	log.Printf("Lengths of expected and actual scan results are:  %d and %d", len(docScanResults), len(scanResults))
	log.Printf("Length of kv docs is %d", len(docs))
}

func TestMixedDatatypesRange_String(t *testing.T) {
	log.Printf("In TestMixedDatatypesRange_String()")

	field := "mixed_field"
	indexName := "index_mixeddt"
	bucketName := "default"

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	docsToCreate := generateJSONSMixedDatatype(1000, field)
	seed++
	log.Printf("Setting mixed datatypes JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{field}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_string(docs, field, "A", "Z", 3)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"A"}, []interface{}{"Z"}, 3, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
	log.Printf("Lengths of expected and actual scan results are:  %d and %d", len(docScanResults), len(scanResults))
	log.Printf("Length of kv docs is %d", len(docs))
}

func TestMixedDatatypesRange_Json(t *testing.T) {
	log.Printf("In TestMixedDatatypesRange_Json()")

	field := "mixed_field"
	indexName := "index_mixeddt"
	bucketName := "default"

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	docsToCreate := generateJSONSMixedDatatype(1000, field)
	seed++
	log.Printf("Setting mixed datatypes JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{field}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	low := map[string]interface{}{
		"door":   0.0,
		"street": "#",
		"city":   "#"}
	high := map[string]interface{}{
		"door":   10000.0,
		"street": "zzzzzzzzz",
		"city":   "zzzzzzzzz"}

	docScanResults := datautility.ExpectedScanAllResponse_json(docs, field)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{low}, []interface{}{high}, 3, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
	log.Printf("Lengths of expected and actual scan results are:  %d and %d", len(docScanResults), len(scanResults))
	log.Printf("Length of kv docs is %d", len(docs))
}

func TestMixedDatatypesScan_Bool(t *testing.T) {
	log.Printf("In TestMixedDatatypesScan_Bool()")

	field := "mixed_field"
	indexName := "index_mixeddt"
	bucketName := "default"

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	docsToCreate := generateJSONSMixedDatatype(1000, field)
	seed++
	log.Printf("Setting mixed datatypes JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{field}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_bool(docs, field, true, 3)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{true}, []interface{}{true}, 3, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
	log.Printf("Lengths of expected and actual scan results are:  %d and %d", len(docScanResults), len(scanResults))

	docScanResults = datautility.ExpectedScanResponse_bool(docs, field, false, 3)
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{false}, []interface{}{false}, 3, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
	log.Printf("Lengths of expected and actual scan results are:  %d and %d", len(docScanResults), len(scanResults))
	log.Printf("Length of kv docs is %d", len(docs))
}

// Test case for testing secondary key field values as very huge
func TestLargeSecondaryKeyLength(t *testing.T) {
	log.Printf("In TestLargeSecondaryKeyLength()")

	field := "LongSecField"
	indexName := "index_LongSecField"
	bucketName := "default"

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	largeKeyDocs := generateLargeSecondayKeyDocs(1000, field)
	seed++
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(largeKeyDocs, "default", "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{field}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanAllResponse(docs, field)
	scanResults, err := secondaryindex.ScanAll(indexName, bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	log.Printf("ScanAll: Lengths of expected and actual scan results are:  %d and %d", len(docScanResults), len(scanResults))
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docScanResults = datautility.ExpectedScanResponse_string(docs, field, "A", "zzzz", 3)
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"A"}, []interface{}{"zzzz"}, 3, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	log.Printf("Range: Lengths of expected and actual scan results are:  %d and %d", len(docScanResults), len(scanResults))
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
	log.Printf("End: Length of kv docs is %d", len(docs))
}

// Test case for testing primary key values with longest length possible
func TestLargePrimaryKeyLength(t *testing.T) {
	log.Printf("In TestLargePrimaryKeyLength()")

	indexName := "index_LongPrimaryField"
	bucketName := "default"

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	largePrimaryKeyDocs := generateLargePrimaryKeyDocs(1000, "docid")
	seed++
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(largePrimaryKeyDocs, "default", "", clusterconfig.KVAddress)

	// Create a primary index
	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", nil, true, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	scanResults, err := secondaryindex.ScanAll(indexName, bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	if len(scanResults) != len(docs) {
		log.Printf("Len of scanResults is incorrect. Expected and Actual are:  %d and %d", len(docs), len(scanResults))
		err = errors.New("Len of scanResults is incorrect.")
		FailTestIfError(err, "Len of scanResults is incorrect", t)
	}

	log.Printf("Lengths of num of docs and scanResults are:  %d and %d", len(docs), len(scanResults))
	log.Printf("End: Length of kv docs is %d", len(docs))
}

// Backindexing
func TestUpdateMutations_DeleteField(t *testing.T) {
	log.Printf("In TestUpdateMutations_DeleteField()")

	var bucketName = "default"
	var indexName = "index_bal"
	var field = "balance"

	docsToCreate := generateDocs(1000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)

	seed++
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{field}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)
	docScanResults := datautility.ExpectedScanAllResponse(docs, field)
	scanResults, err := secondaryindex.ScanAll(indexName, bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	// Create mutations with delete fields
	DeleteFieldMutations(200, "balance") // Update 200 documents by deleting the indexed field

	docScanResults = datautility.ExpectedScanAllResponse(docs, field)
	scanResults, err = secondaryindex.ScanAll(indexName, bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestUpdateMutations_AddField(t *testing.T) {
	log.Printf("In TestUpdateMutations_AddField()")

	var bucketName = "default"
	var indexName = "index_newField"
	var field = "newField"

	docsToCreate := generateDocs(1000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)

	seed++
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{field}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanAllResponse(docs, field)
	scanResults, err := secondaryindex.ScanAll(indexName, bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	log.Printf("Count of scan results before add field mutations:  %d", len(scanResults))
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	// Create mutations with add fields
	AddFieldMutations(300, field) // Update documents by adding the indexed field

	docScanResults = datautility.ExpectedScanAllResponse(docs, field)
	scanResults, err = secondaryindex.ScanAll(indexName, bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	log.Printf("Count of scan results after add field mutations:  %d", len(scanResults))
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestUpdateMutations_DataTypeChange(t *testing.T) {
	log.Printf("In TestUpdateMutations_DataTypeChange()")

	var bucketName = "default"
	var indexName = "index_isUserActive"
	var field = "isActive"

	docsToCreate := generateDocs(1000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)

	seed++
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)
	time.Sleep(2 * time.Second)

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{field}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanAllResponse(docs, field)
	scanResults, err := secondaryindex.ScanAll(indexName, bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	// Create mutations with delete fields
	DataTypeChangeMutations_BoolToString(200, field) // Update documents by changing datatype of the indexed field

	docScanResults = datautility.ExpectedScanAllResponse(docs, field)
	scanResults, err = secondaryindex.ScanAll(indexName, bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docScanResults = datautility.ExpectedScanResponse_string(docs, field, "true", "true", 3)
	scanResults, err = secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{"true"}, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docScanResults = datautility.ExpectedScanResponse_string(docs, field, "false", "false", 3)
	scanResults, err = secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{"false"}, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestMultipleBuckets(t *testing.T) {
	log.Printf("In TestMultipleBuckets()")

	numOfBuckets := 4
	indexNames := [...]string{"bucket1_age", "bucket2_city", "bucket3_gender", "bucket4_balance"}
	indexFields := [...]string{"age", "address.city", "gender", "balance"}
	bucketNames := [...]string{"default", "testbucket2", "testbucket3", "testbucket4"}
	proxyPorts := [...]string{"11212", "11213", "11214", "11215"}
	var bucketDocs [4]tc.KeyValues

	// Update default bucket ram to 256
	// Create two more buckets with ram 256 each
	// Add different docs to all 3 buckets
	// Create index on each of them
	// Query the indexes
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)
	kvutility.FlushBucket(bucketNames[0], "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.EditBucket(bucketNames[0], "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256")

	for i := 1; i < numOfBuckets; i++ {
		kvutility.CreateBucket(bucketNames[i], "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256", proxyPorts[i])
	}
	time.Sleep(bucketOpWaitDur * time.Second)

	log.Printf("Generating docs and Populating all the buckets")
	for i := 0; i < numOfBuckets; i++ {
		bucketDocs[i] = generateDocs(1000, "users.prod")
		kvutility.SetKeyValues(bucketDocs[i], bucketNames[i], "", clusterconfig.KVAddress)
		err := secondaryindex.CreateSecondaryIndex(indexNames[i], bucketNames[i], indexManagementAddress, "", []string{indexFields[i]}, false, nil, true, defaultIndexActiveTimeout, nil)
		FailTestIfError(err, "Error in creating the index", t)
	}
	time.Sleep(3 * time.Second)

	// Scan index of first bucket
	docScanResults := datautility.ExpectedScanResponse_int64(bucketDocs[0], indexFields[0], 30, 50, 1)
	scanResults, err := secondaryindex.Range(indexNames[0], bucketNames[0], indexScanAddress, []interface{}{30}, []interface{}{50}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan 1", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	// Scan index of second bucket
	docScanResults = datautility.ExpectedScanResponse_string(bucketDocs[1], indexFields[1], "F", "Q", 2)
	scanResults, err = secondaryindex.Range(indexNames[1], bucketNames[1], indexScanAddress, []interface{}{"F"}, []interface{}{"Q"}, 2, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan 2", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	// Scan index of third bucket
	docScanResults = datautility.ExpectedScanResponse_string(bucketDocs[2], indexFields[2], "male", "male", 3)
	scanResults, err = secondaryindex.Range(indexNames[2], bucketNames[2], indexScanAddress, []interface{}{"male"}, []interface{}{"male"}, 3, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan 3", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	// Scan index of fourth bucket
	docScanResults = datautility.ExpectedScanResponse_string(bucketDocs[3], indexFields[3], "$30000", "$40000", 3)
	scanResults, err = secondaryindex.Range(indexNames[3], bucketNames[3], indexScanAddress, []interface{}{"$30000"}, []interface{}{"$40000"}, 3, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan 4", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	kvutility.DeleteBucket(bucketNames[1], "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	secondaryindex.RemoveClientForBucket(kvaddress, bucketNames[1])
	kvutility.DeleteBucket(bucketNames[2], "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	secondaryindex.RemoveClientForBucket(kvaddress, bucketNames[2])
	kvutility.DeleteBucket(bucketNames[3], "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	secondaryindex.RemoveClientForBucket(kvaddress, bucketNames[3])
	kvutility.EditBucket(bucketNames[0], "", clusterconfig.Username, clusterconfig.Password, kvaddress, "512")
	time.Sleep(bucketOpWaitDur * time.Second) // Sleep after bucket create or delete

	tc.ClearMap(docs)
	UpdateKVDocs(bucketDocs[0], docs)
}

func TestBucketFlush(t *testing.T) {
	log.Printf("In TestBucketFlush()")

	var bucketName = "default"
	indexNames := [...]string{"index_age", "index_gender", "index_city"}
	indexFields := [...]string{"age", "gender", "address.city"}

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)
	kvutility.FlushBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)

	kvdocs := generateDocs(1000, "users.prod")
	kvutility.SetKeyValues(kvdocs, bucketName, "", clusterconfig.KVAddress)

	for i := 0; i < 3; i++ {
		err := secondaryindex.CreateSecondaryIndex(indexNames[i], bucketName, indexManagementAddress, "", []string{indexFields[i]}, false, nil, true, defaultIndexActiveTimeout, nil)
		FailTestIfError(err, "Error in creating the index", t)
		docScanResults := datautility.ExpectedScanAllResponse(kvdocs, indexFields[i])
		scanResults, err := secondaryindex.ScanAll(indexNames[i], bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
		err = tv.Validate(docScanResults, scanResults)
		FailTestIfError(err, "Error in scan result validation", t)
	}

	kvutility.FlushBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	log.Printf("TestBucketFlush:: Flushed the bucket")

	for i := 0; i < 3; i++ {
		scanResults, err := secondaryindex.ScanAll(indexNames[i], bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
		FailTestIfError(err, "Error in scan ", t)
		if len(scanResults) != 0 {
			log.Printf("Scan results should be empty after bucket but it is : %v\n", scanResults)
			e := errors.New("Scan failed after bucket flush")
			FailTestIfError(e, "Error in TestBucketFlush", t)
		}
	}

	tc.ClearMap(docs)
}

func SkipTestSaslBucket(t *testing.T) {
	log.Printf("In TestSaslBucket()")
	var bucketName = "BucketA"
	var bucketPassword = "password1"
	var indexName = "i_age"
	var field = "age"

	kvutility.DeleteBucket(bucketName, "sasl", clusterconfig.Username, clusterconfig.Password, kvaddress)
	secondaryindex.RemoveClientForBucket(kvaddress, bucketName)
	time.Sleep(bucketOpWaitDur * time.Second) // Sleep after bucket create or delete

	kvutility.CreateBucket(bucketName, "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "300", "11212")
	time.Sleep(bucketOpWaitDur * time.Second) // Sleep after bucket create or delete
	kvdocs := generateDocs(1000, "users.prod")
	kvutility.SetKeyValues(kvdocs, bucketName, bucketPassword, clusterconfig.KVAddress)
	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{field}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_int64(kvdocs, field, 35, 40, 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{35}, []interface{}{40}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func generateJSONSMixedDatatype(numDocs int, fieldName string) tc.KeyValues {
	prodfile := filepath.Join(proddir, "test2.prod")
	docsToCreate := GenerateJsons(numDocs, seed, prodfile, bagdir)
	numberCount := 0
	stringCount := 0
	objCount := 0
	trueBoolCount := 0
	falseBoolCount := 0
	for k, v := range docsToCreate {
		json := v.(map[string]interface{})
		num := randomNum(1, 5)
		if num == 1 {
			numberCount++
			json[fieldName] = int64(randomNum(1, 10000))
		} else if num == 2 {
			stringCount++
			json[fieldName] = randString(randomNum(1, 20))
		} else if num == 3 {
			objCount++
			streetJson := make(map[string]interface{})
			streetJson["door"] = int64(randomNum(1, 1000))
			streetJson["street"] = randString(randomNum(1, 20))
			streetJson["city"] = randString(randomNum(1, 10))
			json[fieldName] = streetJson
		} else if num == 4 {
			boolVal := randomBool()
			if boolVal == true {
				trueBoolCount++
			} else {
				falseBoolCount++
			}
			json[fieldName] = boolVal
		}
		docsToCreate[k] = json
		docs[k] = json
	}
	log.Printf("Number of number fields is: %d", numberCount)
	log.Printf("Number of string fields is: %d", stringCount)
	log.Printf("Number of json fields is: %d", objCount)
	log.Printf("Number of true bool fields is: %d", trueBoolCount)
	log.Printf("Number of false bool fields is: %d", falseBoolCount)
	return docsToCreate
}

func generateLargeSecondayKeyDocs(numDocs int, fieldName string) tc.KeyValues {
	prodfile := filepath.Join(proddir, "test2.prod")
	docsToCreate := GenerateJsons(numDocs, seed, prodfile, bagdir)
	for k, v := range docsToCreate {
		json := v.(map[string]interface{})
		json[fieldName] = randString(randomNum(600, 2000))
		docsToCreate[k] = json
		docs[k] = json
	}
	return docsToCreate
}

func generateLargePrimaryKeyDocs(numDocs int, fieldName string) tc.KeyValues {
	prodfile := filepath.Join(proddir, "test2.prod")
	docsToCreate := GenerateJsons(numDocs, seed, prodfile, bagdir)
	largePrimaryKeyDocs := make(tc.KeyValues)
	for _, v := range docsToCreate {
		str := randString(randomNum(200, 210))
		largePrimaryKeyDocs[str] = v
		docs[str] = v
	}
	return largePrimaryKeyDocs
}

func DeleteFieldMutations(num int, field string) {
	i := 0
	keys := make(tc.KeyValues)
	for key, value := range docs {
		keys[key] = value
		i++
		if i == num {
			break
		}
	}

	keysToBeUpdated := make(tc.KeyValues)
	for key, value := range keys {
		json := value.(map[string]interface{})
		delete(json, field)
		docs[key] = json
		keysToBeUpdated[key] = json
	}

	kvutility.SetKeyValues(keysToBeUpdated, "default", "", clusterconfig.KVAddress)
}

func AddFieldMutations(num int, field string) {
	i := 0
	keys := make(tc.KeyValues)
	for key, value := range docs {
		keys[key] = value
		i++
		if i == num {
			break
		}
	}

	keysToBeUpdated := make(tc.KeyValues)
	for key, value := range keys {
		json := value.(map[string]interface{})
		strValue := randString(randomNum(5, 12))
		json[field] = strValue
		docs[key] = json
		keysToBeUpdated[key] = json
	}

	kvutility.SetKeyValues(keysToBeUpdated, "default", "", clusterconfig.KVAddress)
}

func DataTypeChangeMutations_BoolToString(num int, field string) {
	i := 0
	keys := make(tc.KeyValues)
	for key, value := range docs {
		keys[key] = value
		i++
		if i == num {
			break
		}
	}

	keysToBeUpdated := make(tc.KeyValues)
	for key, value := range keys {
		json := value.(map[string]interface{})
		if json[field] == true {
			json[field] = "true"
		} else {
			json[field] = "false"
		}
		docs[key] = json
		keysToBeUpdated[key] = json
	}

	kvutility.SetKeyValues(keysToBeUpdated, "default", "", clusterconfig.KVAddress)
}
