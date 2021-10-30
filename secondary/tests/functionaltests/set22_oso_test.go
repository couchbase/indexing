package functionaltests

import (
	"errors"
	"fmt"
	"log"
	"testing"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/datautility"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	kv "github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	tv "github.com/couchbase/indexing/secondary/tests/framework/validation"
)

func TestOSOSetup(t *testing.T) {

	log.Printf("In TestOSOSetup()")

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	kv.FlushBucket("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)

	docs = datautility.LoadJSONFromCompressedFile(dataFilePath, "docid")
	mut_docs = datautility.LoadJSONFromCompressedFile(mutationFilePath, "docid")

	// Populate the bucket now
	log.Printf("Populating the default bucket")
	kvutility.SetKeyValues(docs, "default", "", clusterconfig.KVAddress)

	//Enable OSO mode
	err := secondaryindex.ChangeIndexerSettings("indexer.build.enableOSO", true, clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")
}

func TestOSOInitBuildDeleteMutation(t *testing.T) {
	log.Printf("In TestOSOInitBuildDeleteMutation()")
	var indexName = "index_p_oso"
	var indexName1 = "index_p1_oso"
	var bucketName = "default"

	tmp := secondaryindex.UseClient
	secondaryindex.UseClient = "gsi"

	// Create a primary index
	err := secondaryindex.CreateSecondaryIndex(indexName1, bucketName, indexManagementAddress, "", nil, true, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	DeleteDocs(500)

	// Create a primary index
	err = secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", nil, true, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	scanResults, err := secondaryindex.ScanAll(indexName, bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	if len(scanResults) != len(docs) {
		log.Printf("Len of scanResults is incorrect. Expected and Actual are %d and %d", len(docs), len(scanResults))
		err = errors.New("Len of scanResults is incorrect.")
	}
	FailTestIfError(err, "Len of scanResults is incorrect", t)

	docScanResults := datautility.ExpectedScanResponse_RangePrimary(docs, "User2", "User5", 3)
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"User2"}, []interface{}{"User5"}, 3, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.ValidateActual(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation of Primary Range", t)

	// Count Range on primary index
	docScanResults = datautility.ExpectedScanResponse_RangePrimary(docs, "User2", "User5", 3)
	rangeCount, err := secondaryindex.CountRange(indexName, bucketName, indexScanAddress, []interface{}{"User2"}, []interface{}{"User5"}, 3, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in CountRange: ", t)
	log.Printf("CountRange() expected and actual is:  %d and %d", len(docScanResults), rangeCount)
	if int64(len(docScanResults)) != rangeCount {
		e := errors.New(fmt.Sprintf("Expected Range count %d does not match actual Range count %d: ", len(docScanResults), rangeCount))
		FailTestIfError(e, "Error in CountRange: ", t)
	}

	var lookupkey string
	for k := range docs {
		lookupkey = k
		break
	}
	log.Printf("lookupkey for CountLookup() = %v", lookupkey)

	// Count Lookup on primary index
	docScanResults = datautility.ExpectedScanResponse_RangePrimary(docs, lookupkey, lookupkey, 3)
	lookupCount, err := secondaryindex.CountLookup(indexName, bucketName, indexScanAddress, []interface{}{lookupkey}, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in CountRange: ", t)
	if int64(len(docScanResults)) != lookupCount {
		e := errors.New(fmt.Sprintf("Expected Lookup count %d does not match actual Range count %d: ", len(docScanResults), lookupCount))
		FailTestIfError(e, "Error in CountRange: ", t)
	}
	log.Printf("CountLookup() = %v", lookupCount)
	secondaryindex.UseClient = tmp
}

func TestOSOInitBuildIndexerRestart(t *testing.T) {
	log.Printf("In TestOSOInitBuildIndexerRestart()")
	var indexName = "index_p2_oso"
	var bucketName = "default"

	tmp := secondaryindex.UseClient
	secondaryindex.UseClient = "gsi"

	// Create a primary index
	err := secondaryindex.CreateSecondaryIndexAsync(indexName, bucketName, indexManagementAddress, "", nil, true, []byte("{\"defer_build\":   true}"), true, nil)
	FailTestIfError(err, "Error in creating the index", t)

	client, err := secondaryindex.GetOrCreateClient(indexManagementAddress, "test22client")
	FailTestIfError(err, "Error in TestOSOInitBuildIndexerRestart while creating client", t)
	defn1, _ := secondaryindex.GetDefnID(client, bucketName, indexName)

	err = secondaryindex.BuildIndexesAsync([]uint64{defn1}, indexManagementAddress, defaultIndexActiveTimeout)
	FailTestIfError(err, "Error from BuildIndexesAsync of index", t)
	time.Sleep(100 * time.Millisecond)

	tc.KillIndexer()
	time.Sleep(5 * time.Second)

	defnID, _ := secondaryindex.GetDefnID(client, bucketName, indexName)
	err = secondaryindex.WaitTillIndexActive(defnID, client, defaultIndexActiveTimeout)
	FailTestIfError(err, "Error in WaitTillIndexActive", t)

	scanResults, err := secondaryindex.ScanAll(indexName, bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	if len(scanResults) != len(docs) {
		log.Printf("Len of scanResults is incorrect. Expected and Actual are %d and %d", len(docs), len(scanResults))
		err = errors.New("Len of scanResults is incorrect.")
	}
	FailTestIfError(err, "Len of scanResults is incorrect", t)

	docScanResults := datautility.ExpectedScanResponse_RangePrimary(docs, "User2", "User5", 3)
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"User2"}, []interface{}{"User5"}, 3, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.ValidateActual(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation of Primary Range", t)

	// Count Range on primary index
	docScanResults = datautility.ExpectedScanResponse_RangePrimary(docs, "User2", "User5", 3)
	rangeCount, err := secondaryindex.CountRange(indexName, bucketName, indexScanAddress, []interface{}{"User2"}, []interface{}{"User5"}, 3, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in CountRange: ", t)
	log.Printf("CountRange() expected and actual is:  %d and %d", len(docScanResults), rangeCount)
	if int64(len(docScanResults)) != rangeCount {
		e := errors.New(fmt.Sprintf("Expected Range count %d does not match actual Range count %d: ", len(docScanResults), rangeCount))
		FailTestIfError(e, "Error in CountRange: ", t)
	}

	var lookupkey string
	for k := range docs {
		lookupkey = k
		break
	}
	log.Printf("lookupkey for CountLookup() = %v", lookupkey)

	// Count Lookup on primary index
	docScanResults = datautility.ExpectedScanResponse_RangePrimary(docs, lookupkey, lookupkey, 3)
	lookupCount, err := secondaryindex.CountLookup(indexName, bucketName, indexScanAddress, []interface{}{lookupkey}, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in CountRange: ", t)
	if int64(len(docScanResults)) != lookupCount {
		e := errors.New(fmt.Sprintf("Expected Lookup count %d does not match actual Range count %d: ", len(docScanResults), lookupCount))
		FailTestIfError(e, "Error in CountRange: ", t)
	}
	log.Printf("CountLookup() = %v", lookupCount)
	secondaryindex.UseClient = tmp
}
