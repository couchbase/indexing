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
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	tv "github.com/couchbase/indexing/secondary/tests/framework/validation"
)

func TestEphemeralBucketBasic(t *testing.T) {
	log.Printf("In TestEphemeralBuckets()")

	numOfBuckets := 4
	indexNames := [...]string{"bucket1_age", "bucket2_city", "bucket3_gender", "bucket4_balance"}
	indexFields := [...]string{"age", "address.city", "gender", "balance"}
	bucketNames := [...]string{"default", "ephemeral1", "ephemeral2", "ephemeral3"}

	var err error
	var bucketDocs [4]tc.KeyValues
	var docScanResults tc.ScanResponse
	var scanResults tc.ScanResponseActual

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
		kvutility.CreateBucketOfType(bucketNames[i], clusterconfig.Username, clusterconfig.Password, kvaddress, "256", "ephemeral")
	}
	time.Sleep(bucketOpWaitDur * time.Second)

	log.Printf("Generating docs and Populating all the buckets")
	for i := 0; i < numOfBuckets; i++ {
		bucketDocs[i] = generateDocs(1000, "users.prod")
		kvutility.SetKeyValues(bucketDocs[i], bucketNames[i], "", clusterconfig.KVAddress)
		err := secondaryindex.CreateSecondaryIndex(indexNames[i], bucketNames[i], indexManagementAddress, "", []string{indexFields[i]}, false, nil, true, defaultIndexActiveTimeout, nil)
		if i != 0 && clusterconfig.IndexUsing == "forestdb" {
			FailTestIfNoError(err, "Did not see Error when creating ForestDb index on Ephemeral Bucket", t)
			goto cleanup
		}
		FailTestIfError(err, "Error in creating the index", t)
	}
	time.Sleep(3 * time.Second)

	// Scan index of first bucket
	docScanResults = datautility.ExpectedScanResponse_int64(bucketDocs[0], indexFields[0], 30, 50, 1)
	scanResults, err = secondaryindex.Range(indexNames[0], bucketNames[0], indexScanAddress, []interface{}{30}, []interface{}{50}, 1, false, defaultlimit, c.SessionConsistency, nil)
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

cleanup:
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

func TestEphemeralBucketRecovery(t *testing.T) {
	if clusterconfig.IndexUsing == "forestdb" {
		fmt.Println("Not running TestEphemeralBucketRecovery for forestdb")
		return
	}

	log.Printf("In TestEphemeralBucketRecovery()")

	numOfBuckets := 2
	indexNames := [...]string{"bucket1_age", "bucket2_city"}
	indexFields := [...]string{"age", "address.city"}
	bucketNames := [...]string{"default", "ephemeral1"}

	var err error
	var bucketDocs [4]tc.KeyValues
	var docScanResults tc.ScanResponse
	var scanResults tc.ScanResponseActual

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)
	kvutility.FlushBucket(bucketNames[0], "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.EditBucket(bucketNames[0], "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256")

	// Have two buckets now. default is couchbase bucket and another ephemeral bucket
	for i := 1; i < numOfBuckets; i++ {
		kvutility.CreateBucketOfType(bucketNames[i], clusterconfig.Username, clusterconfig.Password, kvaddress, "256", "ephemeral")
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

	forceKillIndexer()

	// Scan index of default bucket which is of couchbase type
	docScanResults = datautility.ExpectedScanResponse_int64(bucketDocs[0], indexFields[0], 30, 50, 1)
	scanResults, err = secondaryindex.Range(indexNames[0], bucketNames[0], indexScanAddress, []interface{}{30}, []interface{}{50}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan 1", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docScanResults = datautility.ExpectedScanResponse_string(bucketDocs[1], indexFields[1], "F", "Q", 2)
	scanResults, err = secondaryindex.Range(indexNames[1], bucketNames[1], indexScanAddress, []interface{}{"F"}, []interface{}{"Q"}, 2, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan 2", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	kvutility.DeleteBucket(bucketNames[1], "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	secondaryindex.RemoveClientForBucket(kvaddress, bucketNames[1])

	kvutility.EditBucket(bucketNames[0], "", clusterconfig.Username, clusterconfig.Password, kvaddress, "512")
	time.Sleep(bucketOpWaitDur * time.Second) // Sleep after bucket create or delete

	tc.ClearMap(docs)
	UpdateKVDocs(bucketDocs[0], docs)
}

func TestEphemeralBucketFlush(t *testing.T) {
	if clusterconfig.IndexUsing == "forestdb" {
		fmt.Println("Not running TestEphemeralBucketFlush for forestdb")
		return
	}
	log.Printf("In TestEphemeralBucketFlush()")

	numOfBuckets := 2
	indexNames := [...]string{"bucket1_age", "bucket2_city"}
	indexFields := [...]string{"age", "address.city"}
	bucketNames := [...]string{"default", "ephemeral1"}

	var bucketDocs [4]tc.KeyValues

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)
	kvutility.FlushBucket(bucketNames[0], "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.EditBucket(bucketNames[0], "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256")

	// Have two buckets now. default is couchbase bucket and another ephemeral bucket
	for i := 1; i < numOfBuckets; i++ {
		kvutility.CreateBucketOfType(bucketNames[i], clusterconfig.Username, clusterconfig.Password, kvaddress, "256", "ephemeral")
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

	kvutility.FlushBucket(bucketNames[0], "", clusterconfig.Username, clusterconfig.Password, kvaddress)

	kvutility.EnableBucketFlush(bucketNames[1], "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.FlushBucket(bucketNames[1], "", clusterconfig.Username, clusterconfig.Password, kvaddress)

	// Scan index of default bucket which is of couchbase type
	for i := 0; i < 2; i++ {
		scanResults, err := secondaryindex.ScanAll(indexNames[i], bucketNames[i], indexScanAddress, defaultlimit, c.SessionConsistency, nil)
		FailTestIfError(err, "Error in scan ", t)
		if len(scanResults) != 0 {
			log.Printf("Scan results should be empty after bucket but it is : %v\n", scanResults)
			e := errors.New("Scan failed after bucket flush")
			FailTestIfError(e, "Error in TestEphemeralBucketFlush", t)
		}
	}

	kvutility.DeleteBucket(bucketNames[1], "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	secondaryindex.RemoveClientForBucket(kvaddress, bucketNames[1])

	kvutility.EditBucket(bucketNames[0], "", clusterconfig.Username, clusterconfig.Password, kvaddress, "512")
	time.Sleep(bucketOpWaitDur * time.Second) // Sleep after bucket create or delete

	tc.ClearMap(docs)
	UpdateKVDocs(bucketDocs[0], docs)
}

func TestEphemeralBucketMCDCrash(t *testing.T) {
	if clusterconfig.IndexUsing == "forestdb" {
		fmt.Println("Not running TestEphemeralBucketMCDCrash for forestdb")
		return
	}

	log.Printf("In TestEphemeralBucketMCDCrash()")

	numOfBuckets := 2
	indexNames := [...]string{"bucket1_age", "bucket2_city"}
	indexFields := [...]string{"age", "address.city"}
	bucketNames := [...]string{"default", "ephemeral1"}

	var err error
	var bucketDocs [4]tc.KeyValues
	var docScanResults tc.ScanResponse
	var scanResults tc.ScanResponseActual

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)
	kvutility.FlushBucket(bucketNames[0], "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.EditBucket(bucketNames[0], "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256")

	// Have two buckets now. default is couchbase bucket and another ephemeral bucket
	for i := 1; i < numOfBuckets; i++ {
		kvutility.CreateBucketOfType(bucketNames[i], clusterconfig.Username, clusterconfig.Password, kvaddress, "256", "ephemeral")
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

	forceKillMemcacheD()

	// Scan index of default bucket which is of couchbase type
	docScanResults = datautility.ExpectedScanResponse_int64(bucketDocs[0], indexFields[0], 30, 50, 1)
	scanResults, err = secondaryindex.Range(indexNames[0], bucketNames[0], indexScanAddress, []interface{}{30}, []interface{}{50}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan 1", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	// Scan index of ephemeral1 bucket which is of ephemeral type
	scanResults, err = secondaryindex.ScanAll(indexNames[1], bucketNames[1], indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan ", t)
	if len(scanResults) != 0 {
		log.Printf("Scan results should be empty after bucket but it is : %v\n", scanResults)
		e := errors.New("Scan failed MCD Crash")
		FailTestIfError(e, "Error in TestEphemeralBucketMCDCrash", t)
	}

	kvutility.DeleteBucket(bucketNames[1], "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	secondaryindex.RemoveClientForBucket(kvaddress, bucketNames[1])

	kvutility.EditBucket(bucketNames[0], "", clusterconfig.Username, clusterconfig.Password, kvaddress, "512")
	time.Sleep(bucketOpWaitDur * time.Second) // Sleep after bucket create or delete

	tc.ClearMap(docs)
	UpdateKVDocs(bucketDocs[0], docs)
}
