package functionaltests

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	mclient "github.com/couchbase/indexing/secondary/manager/client"
	qc "github.com/couchbase/indexing/secondary/queryport/client"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/datautility"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	tv "github.com/couchbase/indexing/secondary/tests/framework/validation"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/parser/n1ql"
)

func TestBuildDeferredAnotherBuilding(t *testing.T) {
	log.Printf("In TestBuildDeferredAnotherBuilding()")

	var bucketName = "default"
	var index1 = "id_company"
	var index2 = "id_age"
	var index3 = "id_age1"

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	docsToCreate := generateDocs(200000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndexAsync(index1, bucketName, indexManagementAddress, "", []string{"company"}, false, []byte("{\"defer_build\": true}"), true, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.CreateSecondaryIndexAsync(index2, bucketName, indexManagementAddress, "", []string{"age"}, false, []byte("{\"defer_build\": true}"), true, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.CreateSecondaryIndexAsync(index3, bucketName, indexManagementAddress, "", []string{"age"}, false, []byte("{\"defer_build\": true}"), true, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.BuildIndex(index3, bucketName, indexManagementAddress, defaultIndexActiveTimeout)
	if err != nil {
		FailTestIfError(e, "Error in TestBuildDeferredAnotherBuilding", t)
	}

	client, err := secondaryindex.GetOrCreateClient(indexManagementAddress, "test7client")
	FailTestIfError(err, "Error in TestBuildDeferredAnotherBuilding while creating client", t)
	defn1, _ := secondaryindex.GetDefnID(client, bucketName, index1)

	err = secondaryindex.BuildIndexesAsync([]uint64{defn1}, indexManagementAddress, defaultIndexActiveTimeout)
	FailTestIfError(err, "Error from BuildIndexesAsync of index1", t)
	time.Sleep(100 * time.Millisecond)

	err = secondaryindex.BuildIndex(index2, bucketName, indexManagementAddress, defaultIndexActiveTimeout)
	if err == nil {
		e := errors.New("Error excpected when build index while another build is in progress")
		FailTestIfError(e, "Error in TestBuildDeferredAnotherBuilding", t)
	} else {
		if strings.Contains(err.Error(), "retry building in the background") {
			log.Printf("Build index failed as expected: %v", err.Error())
		} else {
			log.Printf("Build index did not fail with expected error, instead failed with %v", err)
			e := errors.New("Build index did not fail")
			FailTestIfError(e, "Error in TestBuildDeferredAnotherBuilding", t)
		}
	}

	defnID, _ := secondaryindex.GetDefnID(client, bucketName, index1)
	e = secondaryindex.WaitTillIndexActive(defnID, client, defaultIndexActiveTimeout)
	if e != nil {
		FailTestIfError(e, "Error in WaitTillIndexActive", t)
	}

	// comment out this test since it depends on timing on when indexer will retry rebuilding index
	//time.Sleep(1 * time.Second)
	//err = secondaryindex.BuildIndex(index2, bucketName, indexManagementAddress, defaultIndexActiveTimeout)
	//FailTestIfNoError(err, "Index2 is expected to build in background.   Expected failure when trying to build index2 explicitly, but no failure returned.", t)

	defnID2, _ := secondaryindex.GetDefnID(client, bucketName, index2)
	secondaryindex.WaitTillIndexActive(defnID2, client, defaultIndexActiveTimeout)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "company", "M", "V", 2)
	scanResults, err := secondaryindex.Range(index1, bucketName, indexScanAddress, []interface{}{"M"}, []interface{}{"V"}, 2, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docScanResults = datautility.ExpectedScanResponse_int64(docs, "age", 30, 50, 1)
	scanResults, err = secondaryindex.Range(index2, bucketName, indexScanAddress, []interface{}{30}, []interface{}{50}, 1, false, defaultlimit, c.SessionConsistency, nil)
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
	secondaryindex.RemoveClientForBucket(kvaddress, bucket2)
	kvutility.CreateBucket(bucket2, "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256", "11213")
	tc.ClearMap(docs)
	time.Sleep(bucketOpWaitDur * time.Second)

	log.Printf("Setting JSON docs in KV")
	bucket1docs := generateDocs(50000, "users.prod")
	bucket2docs := generateDocs(50000, "users.prod")
	kvutility.SetKeyValues(bucket1docs, bucket1, "", clusterconfig.KVAddress)
	kvutility.SetKeyValues(bucket2docs, bucket2, "", clusterconfig.KVAddress)
	UpdateKVDocs(bucket1docs, docs)

	err := secondaryindex.CreateSecondaryIndexAsync(index1, bucket1, indexManagementAddress, "", []string{"company"}, false, []byte("{\"defer_build\": true}"), true, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.CreateSecondaryIndexAsync(index2, bucket1, indexManagementAddress, "", []string{"email"}, false, []byte("{\"defer_build\": true}"), true, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.CreateSecondaryIndexAsync(index3, bucket2, indexManagementAddress, "", []string{"gender"}, false, []byte("{\"defer_build\": true}"), true, nil)
	FailTestIfError(err, "Error in creating the index", t)

	client, err := secondaryindex.GetOrCreateClient(indexManagementAddress, "test1client")
	FailTestIfError(err, "Error while creating client", t)

	defn1, _ := secondaryindex.GetDefnID(client, bucket1, index1)
	defn2, _ := secondaryindex.GetDefnID(client, bucket1, index2)
	defn3, _ := secondaryindex.GetDefnID(client, bucket2, index3)

	err = secondaryindex.BuildIndexesAsync([]uint64{defn1}, indexManagementAddress, defaultIndexActiveTimeout)
	FailTestIfError(err, "Error from BuildIndexesAsync of index1", t)

	err = secondaryindex.BuildIndexesAsync([]uint64{defn2, defn3}, indexManagementAddress, defaultIndexActiveTimeout)
	//FailTestIfNoError(err, "Error from BuildIndexesAsync", t)
	//if err != nil {
	//	log.Printf("Build index failed as expected for %v and %v.  Error = %v", defn2, defn3, err.Error())
	//}

	state, e := client.IndexState(defn3)
	log.Printf("Index state of %v is %v", defn3, state)
	FailTestIfError(e, "Error in TestMultipleBucketsDeferredBuild. Build should complete and no error expected for index of second bucket", t)

	e = secondaryindex.WaitTillIndexActive(defn1, client, defaultIndexActiveTimeout)
	if e != nil {
		FailTestIfError(e, "Error in WaitTillIndexActive for first index first bucket", t)
	}

	// comment out this test since it depends on timing on when indexer will retry rebuilding index
	//time.Sleep(1 * time.Second)
	//err = secondaryindex.BuildIndex(index2, bucket1, indexManagementAddress, defaultIndexActiveTimeout)
	//FailTestIfNoError(err, "Index2 is expected to build in background.   Expected failure when trying to build index2 explicitly, but no failure returned.", t)

	e = secondaryindex.WaitTillIndexActive(defn2, client, defaultIndexActiveTimeout)
	if e != nil {
		FailTestIfError(e, "Error in WaitTillIndexActive for second index first bucket", t)
	}

	docScanResults := datautility.ExpectedScanResponse_string(bucket1docs, "company", "B", "H", 1)
	scanResults, err := secondaryindex.Range(index1, bucket1, indexScanAddress, []interface{}{"B"}, []interface{}{"H"}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docScanResults = datautility.ExpectedScanResponse_string(bucket1docs, "email", "f", "t", 3)
	scanResults, err = secondaryindex.Range(index2, bucket1, indexScanAddress, []interface{}{"f"}, []interface{}{"t"}, 3, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docScanResults = datautility.ExpectedScanResponse_string(bucket2docs, "gender", "female", "female", 3)
	scanResults, err = secondaryindex.Range(index3, bucket2, indexScanAddress, []interface{}{"female"}, []interface{}{"female"}, 3, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	kvutility.EditBucket(bucket1, "", clusterconfig.Username, clusterconfig.Password, kvaddress, "1024")
	kvutility.DeleteBucket(bucket2, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	secondaryindex.RemoveClientForBucket(kvaddress, bucket2)
	time.Sleep(5 * time.Second) // Wait for bucket delete to complete
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
	scanResults, err := secondaryindex.Range(index1, bucketName, indexScanAddress, []interface{}{"M"}, []interface{}{"V"}, 2, false, defaultlimit, c.SessionConsistency, nil)
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

	docScanResults := datautility.ExpectedScanResponse_int64(docs, "age", 30, 50, 1)
	scanResults, err := secondaryindex.Range(index2, bucketName, indexScanAddress, []interface{}{30}, []interface{}{50}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docsToCreate = generateDocs(10000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	docScanResults = datautility.ExpectedScanResponse_string(docs, "gender", "female", "female", 3)
	scanResults, err = secondaryindex.Range(index3, bucketName, indexScanAddress, []interface{}{"female"}, []interface{}{"female"}, 3, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docScanResults = datautility.ExpectedScanResponse_bool(docs, "isActive", true, 3)
	scanResults, err = secondaryindex.Range(index4, bucketName, indexScanAddress, []interface{}{true}, []interface{}{true}, 3, false, defaultlimit, c.SessionConsistency, nil)
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

	docScanResults := datautility.ExpectedScanResponse_int64(docs, "age", 30, 50, 1)
	scanResults, err := secondaryindex.Range(index2, bucketName, indexScanAddress, []interface{}{30}, []interface{}{50}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docsToCreate = generateDocs(10000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	docScanResults = datautility.ExpectedScanResponse_string(docs, "gender", "female", "female", 3)
	scanResults, err = secondaryindex.Range(index3, bucketName, indexScanAddress, []interface{}{"female"}, []interface{}{"female"}, 3, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docScanResults = datautility.ExpectedScanResponse_bool(docs, "isActive", true, 3)
	scanResults, err = secondaryindex.Range(index4, bucketName, indexScanAddress, []interface{}{true}, []interface{}{true}, 3, false, defaultlimit, c.SessionConsistency, nil)
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

	client, err := secondaryindex.GetOrCreateClient(indexManagementAddress, "test2client")
	FailTestIfError(err, "Error while creating client", t)
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
	scanResults, err := secondaryindex.Range(index1, bucketName, indexScanAddress, []interface{}{"G"}, []interface{}{"L"}, 2, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan index1", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docScanResults = datautility.ExpectedScanResponse_int64(docs, "age", 34, 35, 1)
	scanResults, err = secondaryindex.Range(index2, bucketName, indexScanAddress, []interface{}{34}, []interface{}{35}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan index2", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docsToCreate = generateDocs(10000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	docScanResults = datautility.ExpectedScanResponse_string(docs, "gender", "male", "male", 3)
	scanResults, err = secondaryindex.Range(index3, bucketName, indexScanAddress, []interface{}{"male"}, []interface{}{"male"}, 3, false, defaultlimit, c.SessionConsistency, nil)
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

	client, err := secondaryindex.GetOrCreateClient(indexManagementAddress, "test2client")
	FailTestIfError(err, "Error while creating client", t)
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
	scanResults, err := secondaryindex.Range(index1, bucketName, indexScanAddress, []interface{}{"G"}, []interface{}{"L"}, 2, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan index1", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docScanResults = datautility.ExpectedScanResponse_string(docs, "gender", "male", "male", 3)
	scanResults, err = secondaryindex.Range(index3, bucketName, indexScanAddress, []interface{}{"male"}, []interface{}{"male"}, 3, false, defaultlimit, c.SessionConsistency, nil)
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

	client, err := secondaryindex.GetOrCreateClient(indexManagementAddress, "test2client")
	FailTestIfError(err, "Error while creating client", t)
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

	client, err := secondaryindex.GetOrCreateClient(indexManagementAddress, "test2client")
	FailTestIfError(err, "Error while creating client", t)
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

	docScanResults := datautility.ExpectedScanResponse_int64(docs, "age", 34, 45, 1)
	scanResults, err := secondaryindex.Range(index2, bucketName, indexScanAddress, []interface{}{34}, []interface{}{45}, 1, false, defaultlimit, c.SessionConsistency, nil)
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

	client, err := secondaryindex.GetOrCreateClient(indexManagementAddress, "test2client")
	FailTestIfError(err, "Error while creating client", t)
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
	scanResults, err := secondaryindex.Range(index1, bucketName, indexScanAddress, []interface{}{"G"}, []interface{}{"L"}, 2, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan index1", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docScanResults = datautility.ExpectedScanAllResponse(docs, "gender")
	scanResults, err = secondaryindex.ScanAll(index3, bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan index3", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

// 11. Multiple Indexes, create 2 deferred indexes. Start building one and wait for it to complete.
// Start building the second index. While the second index is building, drop the first index and
// then drop the second index. After both the indexes are dropped, start building third index
// The build of thrid index should finish successfully
// Note: This test depends on timing of operations. The first and second indexes have to be
// dropped in the same order while the second index is building
func TestCreateAfterDropWhileIndexBuilding(t *testing.T) {
	log.Printf("In TestCreateAfterDropWhileIndexBuilding()")

	var bucketName = "default"
	var index1 = "id_company"
	var index2 = "id_age"
	var index3 = "id_gender"

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	docsToCreate := generateDocs(100000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	client, err := secondaryindex.GetOrCreateClient(indexManagementAddress, "test2client")
	FailTestIfError(err, "Error while creating client", t)

	// Start building first index
	err = secondaryindex.CreateSecondaryIndexAsync(index1, bucketName, indexManagementAddress, "", []string{"company"}, false, []byte("{\"defer_build\": true}"), true, nil)
	FailTestIfError(err, "Error in creating the index", t)

	defn1, _ := secondaryindex.GetDefnID(client, bucketName, index1)
	defnIds := []uint64{defn1}

	err = secondaryindex.BuildIndexesAsync(defnIds, indexManagementAddress, defaultIndexActiveTimeout)
	FailTestIfError(err, "Error from BuildIndexesAsync", t)
	time.Sleep(1 * time.Second)

	// Wait for first index to become active
	e = secondaryindex.WaitTillIndexActive(defn1, client, defaultIndexActiveTimeout)
	if e != nil {
		FailTestIfError(e, "Error in WaitTillIndexActive for index1", t)
	}

	// Create index2
	err = secondaryindex.CreateSecondaryIndexAsync(index2, bucketName, indexManagementAddress, "", []string{"age"}, false, []byte("{\"defer_build\": true}"), true, nil)
	FailTestIfError(err, "Error in creating the index", t)

	defn2, _ := secondaryindex.GetDefnID(client, bucketName, index2)
	defnIds = []uint64{defn2}

	// Build index2
	err = secondaryindex.BuildIndexesAsync(defnIds, indexManagementAddress, defaultIndexActiveTimeout)
	FailTestIfError(err, "Error from BuildIndexesAsync", t)
	time.Sleep(1 * time.Second)

	// While index2 is building, drop index1
	err = secondaryindex.DropSecondaryIndex(index1, bucketName, indexManagementAddress)
	FailTestIfError(err, "Error dropping index1", t)

	// Drop index 2
	err = secondaryindex.DropSecondaryIndex(index2, bucketName, indexManagementAddress)
	FailTestIfError(err, "Error dropping index2", t)
	time.Sleep(5 * time.Second)

	// Create third index
	err = secondaryindex.CreateSecondaryIndexAsync(index3, bucketName, indexManagementAddress, "", []string{"gender"}, false, []byte("{\"defer_build\": true}"), true, nil)
	FailTestIfError(err, "Error in creating the index: index3", t)

	err = secondaryindex.BuildIndexes([]string{index3}, bucketName, indexManagementAddress, defaultIndexActiveTimeout)
	FailTestIfError(err, "Error in deferred index build index3", t)
	time.Sleep(1 * time.Second)

	defn3, _ := secondaryindex.GetDefnID(client, bucketName, index3)
	e = secondaryindex.WaitTillIndexActive(defn3, client, defaultIndexActiveTimeout)
	if e != nil {
		FailTestIfError(e, "Error in WaitTillIndexActive for index1", t)
	}

	docScanResults := datautility.ExpectedScanResponse_string(docs, "gender", "male", "male", 3)
	scanResults, err := secondaryindex.Range(index3, bucketName, indexScanAddress, []interface{}{"male"}, []interface{}{"male"}, 3, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
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
	scanResults, err := secondaryindex.Range(index1, bucketName, indexScanAddress, []interface{}{"G"}, []interface{}{"L"}, 2, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan index1", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docScanResults = datautility.ExpectedScanResponse_int64(docs, "age", 30, 45, 1)
	scanResults, err = secondaryindex.Range(index2, bucketName, indexScanAddress, []interface{}{30}, []interface{}{45}, 1, false, defaultlimit, c.SessionConsistency, nil)
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

	client, err := secondaryindex.GetOrCreateClient(indexManagementAddress, "test3client")
	FailTestIfError(err, "Error while creating client", t)
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
	scanResults, err := secondaryindex.Range(index1, bucketName, indexScanAddress, []interface{}{"G"}, []interface{}{"L"}, 2, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan index1", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docScanResults = datautility.ExpectedScanResponse_int64(docs, "age", 30, 45, 1)
	scanResults, err = secondaryindex.Range(index2, bucketName, indexScanAddress, []interface{}{30}, []interface{}{45}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan index2", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

// 5. Drop index when front end load is going and there are multiple indexes
func TestDropIndexWithDataLoad(t *testing.T) {
	log.Printf("In TestDropIndexWithDataLoad()")
	var wg sync.WaitGroup

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

	err = secondaryindex.CreateSecondaryIndex(index2, bucketName, indexManagementAddress, "", []string{"age"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)
	time.Sleep(1 * time.Second)

	err = secondaryindex.CreateSecondaryIndex(index3, bucketName, indexManagementAddress, "", []string{"gender"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)
	time.Sleep(1 * time.Second)

	err = secondaryindex.CreateSecondaryIndex(index4, bucketName, indexManagementAddress, "", []string{"isActive"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)
	time.Sleep(1 * time.Second)

	docsToCreate = generateDocs(30000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	log.Printf("Setting JSON docs in KV")

	wg.Add(2)
	go DropIndexThread(&wg, t, index1, bucketName)
	go LoadKVBucket(&wg, t, docsToCreate, bucketName, "")
	wg.Wait()

	docScanResults := datautility.ExpectedScanResponse_int64(docs, "age", 30, 45, 1)
	scanResults, err := secondaryindex.Range(index2, bucketName, indexScanAddress, []interface{}{30}, []interface{}{45}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan index2", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
	log.Printf("Number of docScanResults and scanResults = %v and %v", len(docScanResults), len(scanResults))

	docScanResults = datautility.ExpectedScanAllResponse(docs, "gender")
	scanResults, err = secondaryindex.ScanAll(index3, bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan index3", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
	log.Printf("Number of docScanResults and scanResults = %v and %v", len(docScanResults), len(scanResults))
}

// 7. Drop all indexes when frond end load is going on and there are multiple indexes.
func TestDropAllIndexesWithDataLoad(t *testing.T) {
	log.Printf("In TestDropAllIndexesWithDataLoad()")
	var wg sync.WaitGroup

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

	err = secondaryindex.CreateSecondaryIndex(index2, bucketName, indexManagementAddress, "", []string{"age"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)
	time.Sleep(1 * time.Second)

	err = secondaryindex.CreateSecondaryIndex(index3, bucketName, indexManagementAddress, "", []string{"gender"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)
	time.Sleep(1 * time.Second)

	err = secondaryindex.CreateSecondaryIndex(index4, bucketName, indexManagementAddress, "", []string{"isActive"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)
	time.Sleep(1 * time.Second)

	docsToCreate = generateDocs(30000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	log.Printf("Setting JSON docs in KV")

	wg.Add(5)
	go DropIndexThread(&wg, t, index1, bucketName)
	go DropIndexThread(&wg, t, index2, bucketName)
	go DropIndexThread(&wg, t, index3, bucketName)
	go DropIndexThread(&wg, t, index4, bucketName)
	go LoadKVBucket(&wg, t, docsToCreate, bucketName, "")
	wg.Wait()

	time.Sleep(time.Second)
	scanResults, e := secondaryindex.Range(index1, bucketName, indexScanAddress, []interface{}{"BIOSPAN"}, []interface{}{"ZILLANET"}, 1, false, defaultlimit, c.SessionConsistency, nil)
	if e == nil {
		t.Fatal("Error excpected when scanning for dropped index but scan didnt fail \n")
		log.Printf("Length of scanResults = %v", len(scanResults))
	} else {
		log.Printf("Scan failed as expected with error: %v\n", e)
	}
}

// Multiple Buckets
// 2. create bucket and an index on it when another index is building.
func TestCreateBucket_AnotherIndexBuilding(t *testing.T) {
	log.Printf("In TestCreateBucket_AnotherIndexBuilding()")

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	index1 := "buck1_idx"
	index2 := "buck2_idx"
	bucket1 := "default"
	bucket2 := "multi_buck2"

	kvutility.FlushBucket(bucket1, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.EditBucket(bucket1, "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256")
	kvutility.DeleteBucket(bucket2, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	secondaryindex.RemoveClientForBucket(kvaddress, bucket2)
	tc.ClearMap(docs)
	time.Sleep(bucketOpWaitDur * time.Second)

	log.Printf("Setting JSON docs in KV")
	bucket1docs := generateDocs(200000, "test.prod")
	bucket2docs := generateDocs(10000, "test.prod")
	kvutility.SetKeyValues(bucket1docs, bucket1, "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndexAsync(index1, bucket1, indexManagementAddress, "", []string{"company"}, false, nil, true, nil)
	FailTestIfError(err, "Error in creating the index1", t)

	kvutility.CreateBucket(bucket2, "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256", "11213")
	time.Sleep(bucketOpWaitDur * time.Second)
	kvutility.SetKeyValues(bucket2docs, bucket2, "", clusterconfig.KVAddress)
	err = secondaryindex.CreateSecondaryIndexAsync(index2, bucket2, indexManagementAddress, "", []string{"age"}, false, nil, true, nil)
	FailTestIfError(err, "Error in creating the index1", t)

	client, err := secondaryindex.GetOrCreateClient(indexManagementAddress, "test4client")
	FailTestIfError(err, "Error while creating client", t)
	defn1, _ := secondaryindex.GetDefnID(client, bucket1, index1)
	defn2, _ := secondaryindex.GetDefnID(client, bucket2, index2)

	e = secondaryindex.WaitTillIndexActive(defn1, client, defaultIndexActiveTimeout)
	if e != nil {
		FailTestIfError(e, "Error in WaitTillIndexActive for index1", t)
	}

	e = secondaryindex.WaitTillIndexActive(defn2, client, defaultIndexActiveTimeout)
	if e != nil {
		FailTestIfError(e, "Error in WaitTillIndexActive for index2", t)
	}

	docScanResults := datautility.ExpectedScanAllResponse(bucket2docs, "age")
	scanResults, err := secondaryindex.ScanAll(index2, bucket2, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan index2", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
	log.Printf("Number of docScanResults and scanResults = %v and %v", len(docScanResults), len(scanResults))

	docScanResults = datautility.ExpectedScanAllResponse(bucket1docs, "company")
	scanResults, err = secondaryindex.ScanAll(index1, bucket1, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan index1", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
	log.Printf("Number of docScanResults and scanResults = %v and %v", len(docScanResults), len(scanResults))

	kvutility.DeleteBucket(bucket2, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	secondaryindex.RemoveClientForBucket(kvaddress, bucket2)
	kvutility.FlushBucket(bucket1, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.ClearMap(docs)
}

// 3. create bucket and an index on it and drop another index in another bucket.
func TestDropBucket2Index_Bucket1IndexBuilding(t *testing.T) {
	log.Printf("In TestDropBucket2Index_Bucket1IndexBuilding()")

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	index1 := "buck1_idx"
	index2 := "buck2_idx"
	bucket1 := "default"
	bucket2 := "multibucket_test3"

	kvutility.FlushBucket(bucket1, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.EditBucket(bucket1, "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256")
	kvutility.DeleteBucket(bucket2, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	secondaryindex.RemoveClientForBucket(kvaddress, bucket2)
	kvutility.CreateBucket(bucket2, "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256", "11213")
	tc.ClearMap(docs)
	time.Sleep(bucketOpWaitDur * time.Second)

	log.Printf("Setting JSON docs in KV")
	bucket1docs := generateDocs(100000, "test.prod")
	bucket2docs := generateDocs(10000, "test.prod")
	kvutility.SetKeyValues(bucket1docs, bucket1, "", clusterconfig.KVAddress)
	kvutility.SetKeyValues(bucket2docs, bucket2, "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndex(index2, bucket2, indexManagementAddress, "", []string{"age"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index2", t)

	err = secondaryindex.CreateSecondaryIndexAsync(index1, bucket1, indexManagementAddress, "", []string{"company"}, false, nil, true, nil)
	FailTestIfError(err, "Error in creating the index1", t)
	time.Sleep(2 * time.Second)

	err = secondaryindex.DropSecondaryIndex(index2, bucket2, indexManagementAddress)
	FailTestIfError(err, "Error dropping index2", t)

	client, err := secondaryindex.GetOrCreateClient(indexManagementAddress, "test5client")
	FailTestIfError(err, "Error while creating client", t)
	defn1, _ := secondaryindex.GetDefnID(client, bucket1, index1)
	e = secondaryindex.WaitTillIndexActive(defn1, client, defaultIndexActiveTimeout)
	if e != nil {
		FailTestIfError(e, "Error in WaitTillIndexActive for index1", t)
	}

	docScanResults := datautility.ExpectedScanAllResponse(bucket1docs, "company")
	scanResults, err := secondaryindex.ScanAll(index1, bucket1, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan index1", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
	log.Printf("Number of docScanResults and scanResults = %v and %v", len(docScanResults), len(scanResults))

	kvutility.DeleteBucket(bucket2, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	secondaryindex.RemoveClientForBucket(kvaddress, bucket2)
	kvutility.FlushBucket(bucket1, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.ClearMap(docs)
}

//5. bucket delete when initial index build is in progress. there needs to be
//   multiple buckets with valid indexes.
func TestDeleteBucketWhileInitialIndexBuild(t *testing.T) {
	log.Printf("In TestDeleteBucketWhileInitialIndexBuild()")

	numOfBuckets := 4
	indexNames := [...]string{"bucket1_age", "bucket1_gender", "bucket2_city", "bucket2_company", "bucket3_gender", "bucket3_address", "bucket4_balance", "bucket4_isActive"}
	indexFields := [...]string{"age", "gender", "address.city", "company", "gender", "address", "balance", "isActive"}
	bucketNames := [...]string{"default", "testbucket2", "testbucket3", "testbucket4"}
	proxyPorts := [...]string{"11212", "11213", "11214", "11215"}
	var bucketDocs [4]tc.KeyValues

	// Update default bucket ram to 256
	// Create two more buckets with ram 256 each
	// Add different docs to all 3 buckets
	// Create two indexes on each of them
	// Query the indexes
	log.Printf("============== DBG: Drop all indexes in all buckets")
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)
	tc.ClearMap(docs)

	for i := 0; i < numOfBuckets; i++ {
		log.Printf("============== DBG: Delete bucket %v", bucketNames[i])
		kvutility.DeleteBucket(bucketNames[i], "", clusterconfig.Username, clusterconfig.Password, kvaddress)
		secondaryindex.RemoveClientForBucket(kvaddress, bucketNames[i])
		log.Printf("============== DBG: Create bucket %v", bucketNames[i])
		kvutility.CreateBucket(bucketNames[i], "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256", proxyPorts[i])
		kvutility.EnableBucketFlush(bucketNames[i], "", clusterconfig.Username, clusterconfig.Password, kvaddress)
		kvutility.FlushBucket(bucketNames[i], "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	}
	time.Sleep(bucketOpWaitDur * time.Second)

	log.Printf("Generating docs and Populating all the buckets")
	j := 0
	for i := 0; i < numOfBuckets-1; i++ {
		bucketDocs[i] = generateDocs(1000, "users.prod")
		log.Printf("============== DBG: Creating docs in bucket %v", bucketNames[i])
		kvutility.SetKeyValues(bucketDocs[i], bucketNames[i], "", clusterconfig.KVAddress)
		log.Printf("============== DBG: Creating index %v in bucket %v", indexNames[j], bucketNames[i])
		err := secondaryindex.CreateSecondaryIndex(indexNames[j], bucketNames[i], indexManagementAddress, "", []string{indexFields[j]}, false, nil, true, defaultIndexActiveTimeout, nil)
		FailTestIfError(err, "Error in creating the index", t)
		j++
		log.Printf("============== DBG: Creating index %v in bucket %v", indexNames[j], bucketNames[i])
		err = secondaryindex.CreateSecondaryIndex(indexNames[j], bucketNames[i], indexManagementAddress, "", []string{indexFields[j]}, false, nil, true, defaultIndexActiveTimeout, nil)
		FailTestIfError(err, "Error in creating the index", t)
		j++
	}

	// Scan index of first bucket
	log.Printf("============== DBG: First bucket scan:: Scanning index %v in bucket %v", indexNames[0], bucketNames[0])
	docScanResults := datautility.ExpectedScanResponse_int64(bucketDocs[0], indexFields[0], 30, 50, 1)
	scanResults, err := secondaryindex.Range(indexNames[0], bucketNames[0], indexScanAddress, []interface{}{30}, []interface{}{50}, 1, false, defaultlimit, c.SessionConsistency, nil)
	log.Printf("============== DBG: First bucket scan:: Expected results = %v Actual results = %v", len(docScanResults), len(scanResults))
	FailTestIfError(err, "Error in scan 1", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan 1 result validation", t)

	bucketDocs[3] = generateDocs(50000, "users.prod")
	log.Printf("============== DBG: Creating 50K docs in bucket %v", bucketNames[3])
	kvutility.SetKeyValues(bucketDocs[3], bucketNames[3], "", clusterconfig.KVAddress)
	log.Printf("============== DBG: Creating index %v asynchronously in bucket %v", indexNames[6], bucketNames[3])
	err = secondaryindex.CreateSecondaryIndexAsync(indexNames[6], bucketNames[3], indexManagementAddress, "", []string{indexFields[j]}, false, nil, true, nil)
	FailTestIfError(err, "Error in creating the index", t)
	time.Sleep(2 * time.Second)
	log.Printf("============== DBG: Deleting bucket %v", bucketNames[3])
	kvutility.DeleteBucket(bucketNames[3], "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	secondaryindex.RemoveClientForBucket(kvaddress, bucketNames[3])

	// Scan index of first bucket
	log.Printf("============== DBG: First bucket scan:: Scanning index %v in bucket %v", indexNames[0], bucketNames[0])
	docScanResults = datautility.ExpectedScanResponse_int64(bucketDocs[0], indexFields[0], 30, 50, 1)
	scanResults, err = secondaryindex.Range(indexNames[0], bucketNames[0], indexScanAddress, []interface{}{30}, []interface{}{50}, 1, false, defaultlimit, c.SessionConsistency, nil)
	log.Printf("============== DBG: First bucket scan:: Expected results = %v Actual results = %v", len(docScanResults), len(scanResults))
	FailTestIfError(err, "Error in scan 1", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan 1 result validation", t)

	// Scan index of second bucket
	log.Printf("============== DBG: Second bucket scan:: Scanning index %v in bucket %v", indexNames[2], bucketNames[1])
	docScanResults = datautility.ExpectedScanResponse_string(bucketDocs[1], indexFields[2], "F", "Q", 2)
	scanResults, err = secondaryindex.Range(indexNames[2], bucketNames[1], indexScanAddress, []interface{}{"F"}, []interface{}{"Q"}, 2, false, defaultlimit, c.SessionConsistency, nil)
	log.Printf("============== DBG: Second bucket scan:: Expected results = %v Actual results = %v", len(docScanResults), len(scanResults))
	FailTestIfError(err, "Error in scan 2", t)
	err = tv.Validate(docScanResults, scanResults)
	if err != nil {
		log.Printf("============== DBG: Scan of second bucket %v with index %v failed. Expected & actual results are below:", bucketNames[1], indexNames[2])
		tc.PrintScanResults(docScanResults, "docScanResults")
		tc.PrintScanResultsActual(scanResults, "scanResults")
	}
	FailTestIfError(err, "Error in scan 2 result validation", t)

	// Scan index of third bucket
	log.Printf("============== DBG: Third bucket scan:: Scanning index %v in bucket %v", indexNames[4], bucketNames[2])
	docScanResults = datautility.ExpectedScanResponse_string(bucketDocs[2], indexFields[4], "male", "male", 3)
	scanResults, err = secondaryindex.Range(indexNames[4], bucketNames[2], indexScanAddress, []interface{}{"male"}, []interface{}{"male"}, 3, false, defaultlimit, c.SessionConsistency, nil)
	log.Printf("============== DBG: Third bucket scan:: Expected results = %v Actual results = %v", len(docScanResults), len(scanResults))
	FailTestIfError(err, "Error in scan 3", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan 3 result validation", t)

	log.Printf("============== DBG: Deleting buckets %v %v %v", bucketNames[1], bucketNames[2], bucketNames[3])
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

// WHERE clause

// 1. Create Index with where clause. Update document so it moves out of index.
// Update document so it moves back again.
func TestWherClause_UpdateDocument(t *testing.T) {
	log.Printf("In TestWherClause_UpdateDocument()")

	var bucketName = "default"
	var index1 = "id_ageGreaterThan40"

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	kvutility.FlushBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.ClearMap(docs)

	docsToCreate := generateDocs(10000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, bucketName, "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndex(index1, bucketName, indexManagementAddress, `age>40`, []string{"age"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_int64(docs, "age", 40, 1000, 2)
	scanResults, err := secondaryindex.ScanAll(index1, bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan index1", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
	log.Printf("Number of docScanResults and scanResults = %v and %v", len(docScanResults), len(scanResults))

	i := 0
	keysToBeUpdated := make(tc.KeyValues)
	for key, value := range docs {
		if i >= 4000 {
			break
		}
		json := value.(map[string]interface{})
		oldAge := json["age"].(int64)
		if oldAge > 40 {
			json["age"] = 25
			docs[key] = json
			keysToBeUpdated[key] = json
			i++
		}
	}
	kvutility.SetKeyValues(keysToBeUpdated, "default", "", clusterconfig.KVAddress)

	docScanResults = datautility.ExpectedScanResponse_int64(docs, "age", 40, 1000, 2)
	scanResults, err = secondaryindex.ScanAll(index1, bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan index1", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
	log.Printf("Number of docScanResults and scanResults = %v and %v", len(docScanResults), len(scanResults))
}

// Sync-Async Tests

func TestDeferFalse(t *testing.T) {
	log.Printf("In TestDeferFalse()")

	docsToCreate := generateDocs(10000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	var indexName = "index_deferfalse1"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"address.city"}, false, []byte("{\"defer_build\": false}"), true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "address.city", "G", "M", 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"G"}, []interface{}{"M"}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan of index", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestDeferFalse_CloseClientConnection(t *testing.T) {
	log.Printf("In TestDeferFalse_CloseClientConnection()")

	var wg sync.WaitGroup
	indexName := "index_deferfalse2"
	bucketName := "default"
	server := indexManagementAddress
	whereExpr := ""
	indexFields := []string{"address.state"}
	isPrimary := false
	with := []byte("{\"defer_build\": false}")

	client, e := secondaryindex.CreateClient(server, "2itest")
	FailTestIfError(e, "Error in creation of client", t)

	wg.Add(2)
	go CreateIndexThread(&wg, t, indexName, bucketName, server, whereExpr, indexFields, isPrimary, with, client)
	go CloseClientThread(&wg, t, client)
	wg.Wait()

	client1, _ := secondaryindex.CreateClient(indexManagementAddress, "test6client")
	defer client1.Close()
	defn1, _ := secondaryindex.GetDefnID(client1, bucketName, indexName)
	e = secondaryindex.WaitTillIndexActive(defn1, client1, defaultIndexActiveTimeout)
	if e != nil {
		FailTestIfError(e, "Error in WaitTillIndexActive for index_deferfalse2", t)
	}

	docScanResults := datautility.ExpectedScanResponse_string(docs, "address.state", "C", "M", 2)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"C"}, []interface{}{"M"}, 2, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan of index", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func SkipTestDeferFalse_DropIndexWhileBuilding(t *testing.T) {
	log.Printf("In TestDeferFalse_DropIndexWhileBuilding()")

	var wg sync.WaitGroup
	indexName := "index_deferfalse3"
	bucketName := "default"
	server := indexManagementAddress
	whereExpr := ""
	indexFields := []string{"email"}
	isPrimary := false
	with := []byte("{\"defer_build\": false}")

	docsToCreate := generateDocs(500000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	client, e := secondaryindex.CreateClient(server, "2itest")
	FailTestIfError(e, "Error in creation of client", t)

	wg.Add(2)
	go CreateIndexThread(&wg, t, indexName, bucketName, server, whereExpr, indexFields, isPrimary, with, client)
	go DropIndexThread(&wg, t, indexName, bucketName)
	wg.Wait()

	scanResults, e := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"B"}, []interface{}{"T"}, 1, false, defaultlimit, c.SessionConsistency, nil)
	if e == nil {
		t.Fatal("Error excpected when scanning for dropped index but scan didnt fail \n")
		log.Printf("Length of scanResults = %v", len(scanResults))
	} else {
		log.Printf("Scan failed as expected with error: %v\n", e)
	}
}

func getIndexerStorageDir(t *testing.T) string {
	hosts, errHosts := secondaryindex.GetIndexerNodesHttpAddresses(indexManagementAddress)
	FailTestIfError(errHosts, "Error in GetIndexerNodesHttpAddresses", t)

	if len(hosts) > 1 {
		// Just return from here, don't fail the test
		log.Printf("Skipping TestOrphanIndexCleanup as number of hosts = %d\n", len(hosts))
		return ""
	}

	indexStorageDir, errGetSetting := tc.GetIndexerSetting(hosts[0], "indexer.storage_dir",
		clusterconfig.Username, clusterconfig.Password)
	FailTestIfError(errGetSetting, "Error in GetIndexerSetting", t)

	strIndexStorageDir := fmt.Sprintf("%v", indexStorageDir)
	absIndexStorageDir, err1 := filepath.Abs(strIndexStorageDir)
	FailTestIfError(err1, "Error while finding absolute path", t)

	exists, _ := verifyPathExists(absIndexStorageDir)

	if !exists {
		// Just return from here, don't fail the test
		log.Printf("Skipping TestOrphanIndexCleanup as indexStorageDir %v does not exists\n",
			indexStorageDir)
		return ""
	}

	return absIndexStorageDir
}

func TestOrphanIndexCleanup(t *testing.T) {
	// Explicitly create orphan index folder.
	// Restart the indexer
	// Check if orphan index folder gets deleted.

	absIndexStorageDir := getIndexerStorageDir(t)
	if absIndexStorageDir == "" {
		return
	}

	// Drop all existing secondary indexes.
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)
	time.Sleep(10 * time.Second)

	// Create a regular index - keep this index alive to verify that
	// non-orphan indexes are not deleted.
	err := secondaryindex.CreateSecondaryIndex("idx1_age_regular", "default", indexManagementAddress,
		"", []string{"age"}, false, nil, false, 60, nil)
	FailTestIfError(err, "Error in creating the index", t)

	// One more regular index
	err = secondaryindex.CreateSecondaryIndex("idx2_company_regular", "default", indexManagementAddress,
		"", []string{"company"}, false, nil, false, 60, nil)
	FailTestIfError(err, "Error in creating the index", t)

	// Let the persistent snapshot complete.
	time.Sleep(10 * time.Second)

	// Verify the index is queryable
	_, err = secondaryindex.Range("idx1_age_regular", "default", indexScanAddress, []interface{}{35},
		[]interface{}{40}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in range scan", t)
	log.Printf("Query on idx1_age_regular is successful\n")

	_, err = secondaryindex.Range("idx2_company_regular", "default", indexScanAddress, []interface{}{"G"},
		[]interface{}{"M"}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in range scan", t)
	log.Printf("Query on idx2_company_regular is successful\n")

	// This is an implementation of IndexPath from indexer package.
	idxPath := fmt.Sprintf("%s_%s_%d_%d.index", "dummy_bucket_name", "dummy_index_name",
		c.IndexInstId(12345), 4)
	idxFullPath := filepath.Join(absIndexStorageDir, idxPath)
	err = os.MkdirAll(idxFullPath, 0755)
	FailTestIfError(err, "Error creating dummy orphan index", t)

	// restart the indexer
	forceKillIndexer()

	// Verify that the idexer has come up - and query on non-orphan index succeeds.
	_, err = secondaryindex.Range("idx1_age_regular", "default", indexScanAddress, []interface{}{35},
		[]interface{}{40}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in range scan after indexer restart", t)
	log.Printf("Query on idx1_age_regular is successful - after indexer restart.\n")

	_, err = secondaryindex.Range("idx2_company_regular", "default", indexScanAddress, []interface{}{"G"},
		[]interface{}{"M"}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in range scan", t)
	log.Printf("Query on idx2_company_regular is successful - after indexer restart.\n")

	err = verifyDeletedPath(idxFullPath)
	FailTestIfError(err, "Cleanup of orphan index did not happen", t)
}

func TestOrphanPartitionCleanup(t *testing.T) {
	// Explicitly create orphan index folder.
	// Restart the indexer
	// Check if orphan index folder gets deleted.

	if clusterconfig.IndexUsing == "forestdb" {
		fmt.Println("Not running TestOrphanPartitionCleanup for forestdb")
		return
	}

	absIndexStorageDir := getIndexerStorageDir(t)
	if absIndexStorageDir == "" {
		return
	}

	var err error
	err = secondaryindex.CreateSecondaryIndex2("idx3_age_regular", "default", indexManagementAddress,
		"", []string{"age"}, []bool{false}, false, nil, c.KEY, []string{"age"}, false, 60, nil)
	FailTestIfError(err, "Error in creating the index", t)

	// Let the persistent snapshot complete.
	time.Sleep(10 * time.Second)

	// Verify the index is queryable
	_, err = secondaryindex.Range("idx3_age_regular", "default", indexScanAddress, []interface{}{35},
		[]interface{}{40}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in range scan", t)

	log.Printf("Query on idx3_age_regular is successful\n")

	slicePath, err := tc.GetIndexSlicePath("idx3_age_regular", "default", absIndexStorageDir, c.PartitionId(1))
	FailTestIfError(err, "Error in GetIndexSlicePath", t)

	comps := strings.Split(slicePath, "_")
	dummyPartnPath := strings.Join(comps[:len(comps)-1], "_")
	dummyPartnPath += fmt.Sprintf("_%d.index", c.PartitionId(404))

	err = os.MkdirAll(dummyPartnPath, 0755)
	FailTestIfError(err, "Error creating dummy orphan partition", t)

	// restart the indexer
	forceKillIndexer()

	// Verify that the indexer has come up - and query on non-orphan index succeeds.
	_, err = secondaryindex.Range("idx3_age_regular", "default", indexScanAddress, []interface{}{35},
		[]interface{}{40}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in range scan after indexer restart", t)

	log.Printf("Query on idx3_age_regular is successful - after indexer restart.\n")

	err = verifyDeletedPath(dummyPartnPath)
	FailTestIfError(err, "Cleanup of orphan partition did not happen", t)
}

func DropIndexThread(wg *sync.WaitGroup, t *testing.T, index, bucket string) {
	log.Printf("In DropIndexWhileKVLoad")
	defer wg.Done()

	time.Sleep(1 * time.Second)

	err := secondaryindex.DropSecondaryIndex(index, bucket, indexManagementAddress)
	FailTestIfError(err, "Error dropping index1", t)
}

func LoadKVBucket(wg *sync.WaitGroup, t *testing.T, docsToCreate tc.KeyValues, bucketName, bucketPassword string) {
	log.Printf("In LoadKVBucket")
	defer wg.Done()

	log.Printf("Bucket name = %v", bucketName)
	kvutility.SetKeyValues(docsToCreate, bucketName, bucketPassword, clusterconfig.KVAddress)
}

func CreateIndexThread(wg *sync.WaitGroup, t *testing.T, indexName, bucketName, server, whereExpr string, indexFields []string, isPrimary bool, with []byte, client *qc.GsiClient) {
	log.Printf("In CreateIndexThread")
	defer wg.Done()

	var secExprs []string
	if isPrimary == false {
		for _, indexField := range indexFields {
			expr, err := n1ql.ParseExpression(indexField)
			if err != nil {
				log.Printf("Creating index %v. Error while parsing the expression (%v) : %v", indexName, indexField, err)
			}

			secExprs = append(secExprs, expression.NewStringer().Visit(expr))
		}
	}
	exprType := "N1QL"
	partnExp := ""

	_, err := client.CreateIndex(indexName, bucketName, secondaryindex.IndexUsing, exprType, partnExp, whereExpr, secExprs, isPrimary, with)
	if err != nil {
		if strings.Contains(err.Error(), mclient.ErrClientTermination.Error()) ||
			strings.Contains(err.Error(), "Terminate Request during cleanup") {
			log.Printf("Create Index call failed as expected due to error : %v", err)
		} else {
			FailTestIfError(err, "Error in index create", t)
		}
	}
}

func CloseClientThread(wg *sync.WaitGroup, t *testing.T, client *qc.GsiClient) {
	log.Printf("In CloseClientThread")
	defer wg.Done()

	time.Sleep(2*time.Second + 200*time.Millisecond)
	client.Close()
}
