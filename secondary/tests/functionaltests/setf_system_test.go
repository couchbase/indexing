package functionaltests

import (
	"errors"
	c "github.com/couchbase/indexing/secondary/common"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/datautility"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	tv "github.com/couchbase/indexing/secondary/tests/framework/validation"
	"log"
	"strings"
	"testing"
	"time"
)

func TestBuildDeferredAnotherBuilding(t *testing.T) {
	log.Printf("In TestBuildDeferredAnotherBuilding()")

	var bucketName = "default"
	var index1 = "id_company"
	var index2 = "id_age"

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	docsToCreate := generateDocs(50000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndexAsync(index1, bucketName, indexManagementAddress, "", []string{"company"}, false, nil, true, nil)
	FailTestIfError(err, "Error in creating the index", t)
	time.Sleep(1 * time.Second)

	err = secondaryindex.CreateSecondaryIndexAsync(index2, bucketName, indexManagementAddress, "", []string{"age"}, false, []byte("{\"defer_build\": true}"), true, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.BuildIndex(index2, bucketName, indexManagementAddress, defaultIndexActiveTimeout)
	if err == nil {
		e := errors.New("Error excpected when build index while another build is in progress")
		FailTestIfError(e, "Error in TestBuildDeferredAnotherBuilding", t)
	} else {
		if strings.Contains(err.Error(), "Build Already In Progress") {
			log.Printf("Build index failed as expected: %v", err.Error())
		} else {
			log.Printf("Build index did not fail with expected error, instead failed with %v", err)
			e := errors.New("Build index did not fail")
			FailTestIfError(e, "Error in TestBuildDeferredAnotherBuilding", t)
		}
	}

	client, _ := secondaryindex.CreateClient(indexManagementAddress, "test1client")
	defer client.Close()
	defnID, _ := secondaryindex.GetDefnID(client, bucketName, index1)
	e = secondaryindex.WaitTillIndexActive(defnID, client, defaultIndexActiveTimeout)
	if e != nil {
		FailTestIfError(e, "Error in WaitTillIndexActive", t)
	}

	time.Sleep(1 * time.Second)

	err = secondaryindex.BuildIndex(index2, bucketName, indexManagementAddress, defaultIndexActiveTimeout)
	FailTestIfError(err, "Error in BuildIndex in TestBuildDeferredAnotherBuilding", t)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "company", "M", "V", 2)
	scanResults, err := secondaryindex.Range(index1, bucketName, indexScanAddress, []interface{}{"M"}, []interface{}{"V"}, 2, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docScanResults = datautility.ExpectedScanResponse_float64(docs, "age", 30, 50, 1)
	scanResults, err = secondaryindex.Range(index2, bucketName, indexScanAddress, []interface{}{30}, []interface{}{50}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestMultipleBucketsDeferredBuild(t *testing.T) {
	log.Printf("In TestMultipleBucketsDeferredBuild()")

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	index1 := "buck1_id1"
	index2 := "buck1_id2"
	index3 := "buck2_id3"

	bucket1 := "default"
	bucket2 := "defertest_buck2"

	kvutility.FlushBucket(bucket1, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.EditBucket(bucket1, "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256")
	kvutility.DeleteBucket(bucket2, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.CreateBucket(bucket2, "none", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256", "11213")
	tc.ClearMap(docs)
	time.Sleep(30 * time.Second)

	log.Printf("Setting JSON docs in KV")
	bucket1docs := generateDocs(50000, "users.prod")
	bucket2docs := generateDocs(50000, "users.prod")
	kvutility.SetKeyValues(bucket1docs, bucket1, "", clusterconfig.KVAddress)
	kvutility.SetKeyValues(bucket2docs, bucket2, "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndexAsync(index1, bucket1, indexManagementAddress, "", []string{"company"}, false, nil, true, nil)
	FailTestIfError(err, "Error in creating the index", t)
	time.Sleep(1 * time.Second)

	err = secondaryindex.CreateSecondaryIndexAsync(index2, bucket1, indexManagementAddress, "", []string{"email"}, false, []byte("{\"defer_build\": true}"), true, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.CreateSecondaryIndexAsync(index3, bucket2, indexManagementAddress, "", []string{"gender"}, false, []byte("{\"defer_build\": true}"), true, nil)
	FailTestIfError(err, "Error in creating the index", t)

	client, _ := secondaryindex.CreateClient(indexManagementAddress, "test1client")
	defer client.Close()
	defn1, _ := secondaryindex.GetDefnID(client, bucket1, index1)
	defn2, _ := secondaryindex.GetDefnID(client, bucket1, index2)
	defn3, _ := secondaryindex.GetDefnID(client, bucket2, index3)
	defnIds := []uint64{defn2, defn3}

	err = secondaryindex.BuildIndexesAsync(defnIds, indexManagementAddress, defaultIndexActiveTimeout)
	FailTestIfError(err, "Error from BuildIndexesAsync", t)

	// Get status of first : should fail
	// Get status of second: should be building. Wait for it to get active
	time.Sleep(5 * time.Second)
	state, e := client.IndexState(defn2)
	log.Printf("Index state of %v is %v and error is %v", defn2, state, e)
	if e == nil {
		e := errors.New("Error excpected when build index while another build is in progress")
		FailTestIfError(e, "Error in TestMultipleBucketsDeferredBuild", t)
	} else {
		log.Printf("Build index failed as expected: %v", e.Error())
	}

	state, e = client.IndexState(defn3)
	log.Printf("Index state of %v is %v", defn3, state)
	FailTestIfError(e, "Error in TestMultipleBucketsDeferredBuild. Build should complete and no error expected for index of second bucket", t)

	e = secondaryindex.WaitTillIndexActive(defn1, client, defaultIndexActiveTimeout)
	if e != nil {
		FailTestIfError(e, "Error in WaitTillIndexActive for first index first bucket", t)
	}
	time.Sleep(1 * time.Second)

	err = secondaryindex.BuildIndex(index2, bucket1, indexManagementAddress, defaultIndexActiveTimeout)
	FailTestIfError(err, "Error in BuildIndex in TestBuildDeferredAnotherBuilding", t)

	docScanResults := datautility.ExpectedScanResponse_string(bucket1docs, "company", "B", "H", 1)
	scanResults, err := secondaryindex.Range(index1, bucket1, indexScanAddress, []interface{}{"B"}, []interface{}{"H"}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docScanResults = datautility.ExpectedScanResponse_string(bucket1docs, "email", "f", "t", 3)
	scanResults, err = secondaryindex.Range(index2, bucket1, indexScanAddress, []interface{}{"f"}, []interface{}{"t"}, 3, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docScanResults = datautility.ExpectedScanResponse_string(bucket2docs, "gender", "female", "female", 3)
	scanResults, err = secondaryindex.Range(index3, bucket2, indexScanAddress, []interface{}{"female"}, []interface{}{"female"}, 3, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	kvutility.EditBucket(bucket1, "", clusterconfig.Username, clusterconfig.Password, kvaddress, "1024")
	kvutility.DeleteBucket(bucket2, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	UpdateKVDocs(bucket1docs, docs)
}

// Create/drop/create a deferred build index without actually building it.
// This should be done when there is already another index on the bucket.
// Do more mutations to see existing index is not affected.
func TestCreateDropCreateDeferredIndex(t *testing.T) {
	log.Printf("In TestCreateDropCreateDeferredIndex()")

	var bucketName = "default"
	var index1 = "id_company"
	var index2 = "id_age"

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	docsToCreate := generateDocs(10000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndex(index1, bucketName, indexManagementAddress, "", []string{"company"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)
	time.Sleep(1 * time.Second)

	err = secondaryindex.CreateSecondaryIndexAsync(index2, bucketName, indexManagementAddress, "", []string{"age"}, false, []byte("{\"defer_build\": true}"), true, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.DropSecondaryIndex(index2, bucketName, indexManagementAddress)
	FailTestIfError(err, "Error dropping index", t)

	docsToCreate = generateDocs(10000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "company", "M", "V", 2)
	scanResults, err := secondaryindex.Range(index1, bucketName, indexScanAddress, []interface{}{"M"}, []interface{}{"V"}, 2, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

// Multiple indexer nodes [create deferred build indexes and build together] [
func TestMultipleDeferredIndexes_BuildTogether(t *testing.T) {
	log.Printf("In TestMultipleDeferredIndexes_BuildTogether()")

	var bucketName = "default"
	var index1 = "id_company"
	var index2 = "id_age"
	var index3 = "id_gender"
	var index4 = "id_isActive"

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	docsToCreate := generateDocs(10000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndex(index1, bucketName, indexManagementAddress, "", []string{"company"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)
	time.Sleep(1 * time.Second)

	err = secondaryindex.CreateSecondaryIndexAsync(index2, bucketName, indexManagementAddress, "", []string{"age"}, false, []byte("{\"defer_build\": true}"), true, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.CreateSecondaryIndexAsync(index3, bucketName, indexManagementAddress, "", []string{"gender"}, false, []byte("{\"defer_build\": true}"), true, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.CreateSecondaryIndexAsync(index4, bucketName, indexManagementAddress, "", []string{"isActive"}, false, []byte("{\"defer_build\": true}"), true, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.BuildIndexes([]string{index2, index3, index4}, bucketName, indexManagementAddress, defaultIndexActiveTimeout)
	FailTestIfError(err, "Error in deferred index build", t)

	docScanResults := datautility.ExpectedScanResponse_float64(docs, "age", 30, 50, 1)
	scanResults, err := secondaryindex.Range(index2, bucketName, indexScanAddress, []interface{}{30}, []interface{}{50}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docsToCreate = generateDocs(10000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	docScanResults = datautility.ExpectedScanResponse_string(docs, "gender", "female", "female", 3)
	scanResults, err = secondaryindex.Range(index3, bucketName, indexScanAddress, []interface{}{"female"}, []interface{}{"female"}, 3, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docScanResults = datautility.ExpectedScanResponse_bool(docs, "isActive", true, 3)
	scanResults, err = secondaryindex.Range(index4, bucketName, indexScanAddress, []interface{}{true}, []interface{}{true}, 3, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

// Multiple indexer nodes [create deferred build indexes and build one by one]
func TestMultipleDeferredIndexes_BuildOneByOne(t *testing.T) {
	log.Printf("In TestMultipleDeferredIndexes_BuildOneByOne()")

	var bucketName = "default"
	var index1 = "id_company"
	var index2 = "id_age"
	var index3 = "id_gender"
	var index4 = "id_isActive"

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	docsToCreate := generateDocs(10000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndex(index1, bucketName, indexManagementAddress, "", []string{"company"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)
	time.Sleep(1 * time.Second)

	err = secondaryindex.CreateSecondaryIndexAsync(index2, bucketName, indexManagementAddress, "", []string{"age"}, false, []byte("{\"defer_build\": true}"), true, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.CreateSecondaryIndexAsync(index3, bucketName, indexManagementAddress, "", []string{"gender"}, false, []byte("{\"defer_build\": true}"), true, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.CreateSecondaryIndexAsync(index4, bucketName, indexManagementAddress, "", []string{"isActive"}, false, []byte("{\"defer_build\": true}"), true, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.BuildIndexes([]string{index2}, bucketName, indexManagementAddress, defaultIndexActiveTimeout)
	FailTestIfError(err, "Error in deferred index build", t)

	err = secondaryindex.BuildIndexes([]string{index3}, bucketName, indexManagementAddress, defaultIndexActiveTimeout)
	FailTestIfError(err, "Error in deferred index build", t)

	err = secondaryindex.BuildIndexes([]string{index4}, bucketName, indexManagementAddress, defaultIndexActiveTimeout)
	FailTestIfError(err, "Error in deferred index build", t)

	docScanResults := datautility.ExpectedScanResponse_float64(docs, "age", 30, 50, 1)
	scanResults, err := secondaryindex.Range(index2, bucketName, indexScanAddress, []interface{}{30}, []interface{}{50}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docsToCreate = generateDocs(10000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	docScanResults = datautility.ExpectedScanResponse_string(docs, "gender", "female", "female", 3)
	scanResults, err = secondaryindex.Range(index3, bucketName, indexScanAddress, []interface{}{"female"}, []interface{}{"female"}, 3, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docScanResults = datautility.ExpectedScanResponse_bool(docs, "isActive", true, 3)
	scanResults, err = secondaryindex.Range(index4, bucketName, indexScanAddress, []interface{}{true}, []interface{}{true}, 3, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

// Multiple Indexes, create 3 deferred indexes. Start Building two. Drop the index which is not building.
func TestDropDeferredIndexWhileOthersBuilding(t *testing.T) {
	log.Printf("In TestDropDeferredIndexWhileOthersBuilding()")

	var bucketName = "default"
	var index1 = "id_company"
	var index2 = "id_age"
	var index3 = "id_gender"
	var index4 = "id_isActive"

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	docsToCreate := generateDocs(10000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndex(index1, bucketName, indexManagementAddress, "", []string{"company"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)
	time.Sleep(1 * time.Second)

	err = secondaryindex.CreateSecondaryIndexAsync(index2, bucketName, indexManagementAddress, "", []string{"age"}, false, []byte("{\"defer_build\": true}"), true, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.CreateSecondaryIndexAsync(index3, bucketName, indexManagementAddress, "", []string{"gender"}, false, []byte("{\"defer_build\": true}"), true, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.CreateSecondaryIndexAsync(index4, bucketName, indexManagementAddress, "", []string{"isActive"}, false, []byte("{\"defer_build\": true}"), true, nil)
	FailTestIfError(err, "Error in creating the index", t)

	client, _ := secondaryindex.CreateClient(indexManagementAddress, "test2client")
	defer client.Close()
	defn2, _ := secondaryindex.GetDefnID(client, bucketName, index2)
	defn3, _ := secondaryindex.GetDefnID(client, bucketName, index3)
	defnIds := []uint64{defn2, defn3}

	err = secondaryindex.BuildIndexesAsync(defnIds, indexManagementAddress, defaultIndexActiveTimeout)
	FailTestIfError(err, "Error from BuildIndexesAsync", t)
	time.Sleep(2 * time.Second)

	err = secondaryindex.DropSecondaryIndex(index4, bucketName, indexManagementAddress)
	FailTestIfError(err, "Error dropping index4", t)

	e = secondaryindex.WaitTillIndexActive(defn2, client, defaultIndexActiveTimeout)
	if e != nil {
		FailTestIfError(e, "Error in WaitTillIndexActive for index2", t)
	}
	e = secondaryindex.WaitTillIndexActive(defn3, client, defaultIndexActiveTimeout)
	if e != nil {
		FailTestIfError(e, "Error in WaitTillIndexActive for index3", t)
	}
	time.Sleep(1 * time.Second)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "company", "G", "L", 2)
	scanResults, err := secondaryindex.Range(index1, bucketName, indexScanAddress, []interface{}{"G"}, []interface{}{"L"}, 2, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan index1", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docScanResults = datautility.ExpectedScanResponse_float64(docs, "age", 34, 35, 1)
	scanResults, err = secondaryindex.Range(index2, bucketName, indexScanAddress, []interface{}{34}, []interface{}{35}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan index2", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docsToCreate = generateDocs(10000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	docScanResults = datautility.ExpectedScanResponse_string(docs, "gender", "male", "male", 3)
	scanResults, err = secondaryindex.Range(index3, bucketName, indexScanAddress, []interface{}{"male"}, []interface{}{"male"}, 3, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

// 7. Multiple Indexes, create 3 deferred indexes. Start Building two. Drop the index which is building.
// Build the third index. Verify data.
func TestDropBuildingDeferredIndex(t *testing.T) {
	log.Printf("In TestDropBuildingDeferredIndex()")

	var bucketName = "default"
	var index1 = "id_company"
	var index2 = "id_age"
	var index3 = "id_gender"

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	docsToCreate := generateDocs(10000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndexAsync(index1, bucketName, indexManagementAddress, "", []string{"company"}, false, []byte("{\"defer_build\": true}"), true, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.CreateSecondaryIndexAsync(index2, bucketName, indexManagementAddress, "", []string{"age"}, false, []byte("{\"defer_build\": true}"), true, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.CreateSecondaryIndexAsync(index3, bucketName, indexManagementAddress, "", []string{"gender"}, false, []byte("{\"defer_build\": true}"), true, nil)
	FailTestIfError(err, "Error in creating the index", t)

	client, _ := secondaryindex.CreateClient(indexManagementAddress, "test2client")
	defer client.Close()
	defn1, _ := secondaryindex.GetDefnID(client, bucketName, index1)
	defn2, _ := secondaryindex.GetDefnID(client, bucketName, index2)
	defnIds := []uint64{defn1, defn2}

	err = secondaryindex.BuildIndexesAsync(defnIds, indexManagementAddress, defaultIndexActiveTimeout)
	FailTestIfError(err, "Error from BuildIndexesAsync", t)
	time.Sleep(1 * time.Second)

	err = secondaryindex.DropSecondaryIndex(index2, bucketName, indexManagementAddress)
	FailTestIfError(err, "Error dropping index2", t)

	e = secondaryindex.WaitTillIndexActive(defn1, client, defaultIndexActiveTimeout)
	if e != nil {
		FailTestIfError(e, "Error in WaitTillIndexActive for index1", t)
	}

	err = secondaryindex.BuildIndexes([]string{index3}, bucketName, indexManagementAddress, defaultIndexActiveTimeout)
	FailTestIfError(err, "Error in deferred index build index3", t)

	time.Sleep(1 * time.Second)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "company", "G", "L", 2)
	scanResults, err := secondaryindex.Range(index1, bucketName, indexScanAddress, []interface{}{"G"}, []interface{}{"L"}, 2, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan index1", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docScanResults = datautility.ExpectedScanResponse_string(docs, "gender", "male", "male", 3)
	scanResults, err = secondaryindex.Range(index3, bucketName, indexScanAddress, []interface{}{"male"}, []interface{}{"male"}, 3, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docsToCreate = generateDocs(10000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	docScanResults = datautility.ExpectedScanAllResponse(docs, "gender")
	scanResults, err = secondaryindex.ScanAll(index3, bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan index3", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

// 8. Multiple Indexes, create 3 deferred indexes. Start Building two. Drop both the indexes which are building.
// Build the third index. Verify data.
func TestDropMultipleBuildingDeferredIndexes(t *testing.T) {
	log.Printf("In TestDropMultipleBuildingDeferredIndexes()")

	var bucketName = "default"
	var index1 = "id_company"
	var index2 = "id_age"
	var index3 = "id_gender"
	var index4 = "id_isActive"

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	docsToCreate := generateDocs(30000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndex(index1, bucketName, indexManagementAddress, "", []string{"company"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)
	time.Sleep(1 * time.Second)

	err = secondaryindex.CreateSecondaryIndexAsync(index2, bucketName, indexManagementAddress, "", []string{"age"}, false, []byte("{\"defer_build\": true}"), true, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.CreateSecondaryIndexAsync(index3, bucketName, indexManagementAddress, "", []string{"gender"}, false, []byte("{\"defer_build\": true}"), true, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.CreateSecondaryIndexAsync(index4, bucketName, indexManagementAddress, "", []string{"isActive"}, false, []byte("{\"defer_build\": true}"), true, nil)
	FailTestIfError(err, "Error in creating the index", t)

	client, _ := secondaryindex.CreateClient(indexManagementAddress, "test2client")
	defer client.Close()
	defn2, _ := secondaryindex.GetDefnID(client, bucketName, index2)
	defn3, _ := secondaryindex.GetDefnID(client, bucketName, index3)
	defnIds := []uint64{defn2, defn3}

	err = secondaryindex.BuildIndexesAsync(defnIds, indexManagementAddress, defaultIndexActiveTimeout)
	FailTestIfError(err, "Error from BuildIndexesAsync", t)
	time.Sleep(1 * time.Second)

	err = secondaryindex.DropSecondaryIndex(index2, bucketName, indexManagementAddress)
	FailTestIfError(err, "Error dropping index2", t)

	err = secondaryindex.DropSecondaryIndex(index3, bucketName, indexManagementAddress)
	FailTestIfError(err, "Error dropping index2", t)

	err = secondaryindex.BuildIndexes([]string{index4}, bucketName, indexManagementAddress, defaultIndexActiveTimeout)
	FailTestIfError(err, "Error in deferred index build index4", t)

	time.Sleep(10 * time.Second)

	docScanResults := datautility.ExpectedScanAllResponse(docs, "isActive")
	scanResults, err := secondaryindex.ScanAll(index4, bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan index4", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation of index4", t)
	log.Printf("Number of docScanResults and scanResults = %v and %v", len(docScanResults), len(scanResults))

	docScanResults = datautility.ExpectedScanAllResponse(docs, "company")
	scanResults, err = secondaryindex.ScanAll(index1, bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan index1", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation of index1", t)
	log.Printf("Number of docScanResults and scanResults = %v and %v", len(docScanResults), len(scanResults))
}

// 9. Multiple Indexes, create 3 deferred indexes. Start Building one. Wait for it to complete. Start building second.
// Drop the first while second is building. Once second finishes, build the third one.
func TestDropOneIndexSecondDeferBuilding(t *testing.T) {
	log.Printf("In TestDropOneIndexSecondDeferBuilding()")

	var bucketName = "default"
	var index1 = "id_company"
	var index2 = "id_age"
	var index3 = "id_gender"

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	docsToCreate := generateDocs(10000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndexAsync(index1, bucketName, indexManagementAddress, "", []string{"company"}, false, []byte("{\"defer_build\": true}"), true, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.CreateSecondaryIndexAsync(index2, bucketName, indexManagementAddress, "", []string{"age"}, false, []byte("{\"defer_build\": true}"), true, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.CreateSecondaryIndexAsync(index3, bucketName, indexManagementAddress, "", []string{"gender"}, false, []byte("{\"defer_build\": true}"), true, nil)
	FailTestIfError(err, "Error in creating the index", t)

	client, _ := secondaryindex.CreateClient(indexManagementAddress, "test2client")
	defer client.Close()
	defn2, _ := secondaryindex.GetDefnID(client, bucketName, index2)

	err = secondaryindex.BuildIndexes([]string{index1}, bucketName, indexManagementAddress, defaultIndexActiveTimeout)
	FailTestIfError(err, "Error in deferred index build index1", t)

	err = secondaryindex.BuildIndexesAsync([]uint64{defn2}, indexManagementAddress, defaultIndexActiveTimeout)
	FailTestIfError(err, "Error from BuildIndexesAsync", t)
	time.Sleep(1 * time.Second)

	err = secondaryindex.DropSecondaryIndex(index1, bucketName, indexManagementAddress)
	FailTestIfError(err, "Error dropping index1", t)

	docsToCreate = generateDocs(10000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	e = secondaryindex.WaitTillIndexActive(defn2, client, defaultIndexActiveTimeout)
	if e != nil {
		FailTestIfError(e, "Error in WaitTillIndexActive for index2", t)
	}

	err = secondaryindex.BuildIndexes([]string{index3}, bucketName, indexManagementAddress, defaultIndexActiveTimeout)
	FailTestIfError(err, "Error in deferred index build index3", t)

	docScanResults := datautility.ExpectedScanResponse_float64(docs, "age", 34, 45, 1)
	scanResults, err := secondaryindex.Range(index2, bucketName, indexScanAddress, []interface{}{34}, []interface{}{45}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan index2", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docScanResults = datautility.ExpectedScanAllResponse(docs, "gender")
	scanResults, err = secondaryindex.ScanAll(index3, bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan index3", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

// 10. Multiple Indexes, create 3 deferred indexes. Start Building one. Wait for it to complete. Start building second.
// Drop the second while second is building. Build the third one
func TestDropSecondIndexSecondDeferBuilding(t *testing.T) {
	log.Printf("In TestDropSecondIndexSecondDeferBuilding()")

	var bucketName = "default"
	var index1 = "id_company"
	var index2 = "id_age"
	var index3 = "id_gender"

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	docsToCreate := generateDocs(10000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndexAsync(index1, bucketName, indexManagementAddress, "", []string{"company"}, false, []byte("{\"defer_build\": true}"), true, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.CreateSecondaryIndexAsync(index2, bucketName, indexManagementAddress, "", []string{"age"}, false, []byte("{\"defer_build\": true}"), true, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.CreateSecondaryIndexAsync(index3, bucketName, indexManagementAddress, "", []string{"gender"}, false, []byte("{\"defer_build\": true}"), true, nil)
	FailTestIfError(err, "Error in creating the index", t)

	client, _ := secondaryindex.CreateClient(indexManagementAddress, "test2client")
	defer client.Close()
	defn2, _ := secondaryindex.GetDefnID(client, bucketName, index2)

	err = secondaryindex.BuildIndexes([]string{index1}, bucketName, indexManagementAddress, defaultIndexActiveTimeout)
	FailTestIfError(err, "Error in deferred index build index1", t)

	err = secondaryindex.BuildIndexesAsync([]uint64{defn2}, indexManagementAddress, defaultIndexActiveTimeout)
	FailTestIfError(err, "Error from BuildIndexesAsync", t)
	time.Sleep(1 * time.Second)

	err = secondaryindex.DropSecondaryIndex(index2, bucketName, indexManagementAddress)
	FailTestIfError(err, "Error dropping index2", t)

	docsToCreate = generateDocs(10000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	err = secondaryindex.BuildIndexes([]string{index3}, bucketName, indexManagementAddress, defaultIndexActiveTimeout)
	FailTestIfError(err, "Error in deferred index build index3", t)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "company", "G", "L", 2)
	scanResults, err := secondaryindex.Range(index1, bucketName, indexScanAddress, []interface{}{"G"}, []interface{}{"L"}, 2, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan index1", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docScanResults = datautility.ExpectedScanAllResponse(docs, "gender")
	scanResults, err = secondaryindex.ScanAll(index3, bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan index3", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

// Drop Index tests

// 1. Create one index. Create another and while the index is in initial state, drop the second one.
// Create again. Verify both indexes.
func TestDropBuildingIndex1(t *testing.T) {
	log.Printf("In TestDropBuildingIndex1()")

	var bucketName = "default"
	var index1 = "id_company"
	var index2 = "id_age"

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	docsToCreate := generateDocs(20000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndex(index1, bucketName, indexManagementAddress, "", []string{"company"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index1", t)

	err = secondaryindex.CreateSecondaryIndexAsync(index2, bucketName, indexManagementAddress, "", []string{"age"}, false, nil, true, nil)
	FailTestIfError(err, "Error in creating the index2", t)
	time.Sleep(1 * time.Second)

	err = secondaryindex.DropSecondaryIndex(index2, bucketName, indexManagementAddress)
	FailTestIfError(err, "Error dropping index2", t)

	err = secondaryindex.CreateSecondaryIndex(index2, bucketName, indexManagementAddress, "", []string{"age"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index2", t)

	docsToCreate = generateDocs(10000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "company", "G", "L", 2)
	scanResults, err := secondaryindex.Range(index1, bucketName, indexScanAddress, []interface{}{"G"}, []interface{}{"L"}, 2, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan index1", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docScanResults = datautility.ExpectedScanResponse_float64(docs, "age", 30, 45, 1)
	scanResults, err = secondaryindex.Range(index2, bucketName, indexScanAddress, []interface{}{30}, []interface{}{45}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan index2", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

// 3. Create one index. Create another and while the index is in initial state, drop the first one.
// Create again. Verify both indexes.
func TestDropBuildingIndex2(t *testing.T) {
	log.Printf("In TestDropBuildingIndex2()")

	var bucketName = "default"
	var index1 = "id_company"
	var index2 = "id_age"

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	docsToCreate := generateDocs(20000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndex(index1, bucketName, indexManagementAddress, "", []string{"company"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index1", t)

	err = secondaryindex.CreateSecondaryIndexAsync(index2, bucketName, indexManagementAddress, "", []string{"age"}, false, nil, true, nil)
	FailTestIfError(err, "Error in creating the index2", t)
	time.Sleep(1 * time.Second)

	err = secondaryindex.DropSecondaryIndex(index1, bucketName, indexManagementAddress)
	FailTestIfError(err, "Error dropping index1", t)

	client, _ := secondaryindex.CreateClient(indexManagementAddress, "test3client")
	defer client.Close()
	defn2, _ := secondaryindex.GetDefnID(client, bucketName, index2)

	e = secondaryindex.WaitTillIndexActive(defn2, client, defaultIndexActiveTimeout)
	if e != nil {
		FailTestIfError(e, "Error in WaitTillIndexActive for index2", t)
	}

	err = secondaryindex.CreateSecondaryIndex(index1, bucketName, indexManagementAddress, "", []string{"company"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index1", t)

	docsToCreate = generateDocs(10000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "company", "G", "L", 2)
	scanResults, err := secondaryindex.Range(index1, bucketName, indexScanAddress, []interface{}{"G"}, []interface{}{"L"}, 2, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan index1", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docScanResults = datautility.ExpectedScanResponse_float64(docs, "age", 30, 45, 1)
	scanResults, err = secondaryindex.Range(index2, bucketName, indexScanAddress, []interface{}{30}, []interface{}{45}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan index2", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}
