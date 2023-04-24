package serverlesstests

import (
	"encoding/json"
	"log"
	"strconv"
	"testing"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	br "github.com/couchbase/indexing/secondary/tests/framework/backuprestore"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
)

// This test will perform backup+restore operation on indexer node
// An index will be created on bucket.scope.collection and backup restore operation will be done on the same
func TestBackupRestore(t *testing.T) {
	log.Printf("In TestBackupRestore()")

	bucket := "b1"
	scope := "s1"
	coll := "c1"
	index := "index1"

	bucketOp := func() {
		log.Printf("Cleaning up & creating %v:%v:%v", bucket, scope, coll)
		kvutility.DeleteBucket(bucket, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
		kvutility.CreateBucket(bucket, "none", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "100", "")
		kvutility.WaitForBucketCreation(bucket, clusterconfig.Username, clusterconfig.Password, []string{clusterconfig.Nodes[0], clusterconfig.Nodes[1], clusterconfig.Nodes[2]})
		manifest := kvutility.CreateCollection(bucket, scope, coll, clusterconfig.Username, clusterconfig.Password, kvaddress)
		kvutility.WaitForCollectionCreation(bucket, scope, coll, clusterconfig.Username, clusterconfig.Password, []string{clusterconfig.Nodes[0], clusterconfig.Nodes[1], clusterconfig.Nodes[2]}, manifest) //TODO use cid & insert docs
		time.Sleep(bucketOpWaitDur * time.Second)
	}

	//Drop all indexes
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	tc.HandleError(e, "Error in DropAllSecondaryIndexes")

	// Create bucket
	bucketOp()

	// Insert docs
	cid := kvutility.GetCollectionID(bucket, scope, coll, clusterconfig.Username, clusterconfig.Password, kvaddress)
	CreateDocsForCollection(bucket, cid, 1000)
	time.Sleep(2 * time.Second)

	// Create Index
	withExpr := []byte("{\"num_replica\":1}") // This is 1 by default for serverless
	indexFields := []string{"age"}
	err := secondaryindex.CreateSecondaryIndex3(
		index, bucket, scope, coll, indexManagementAddress,
		"", indexFields, nil,
		false, withExpr, c.SINGLE, nil,
		true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error creating"+index+" on "+bucket, t)
	// Wait for indexes to be created
	status := "Ready"
	waitForIndexStatus(bucket, scope, coll, index, status, t)
	waitForIndexStatus(bucket, scope, coll, index+" (replica 1)", status, t)

	indexers, _ := secondaryindex.GetIndexerNodesHttpAddresses(indexManagementAddress)
	if len(indexers) == 0 {
		t.Fatalf("TestBackupRestore::Unable to find indexer nodes")
	}

	// Indexer Url with auth
	indexerUrl := "http://" + clusterconfig.Username + ":" + clusterconfig.Password + "@" + indexers[0]

	// Run backup operation
	backup_resp, err := br.Backup(indexerUrl, bucket, scope, coll, true)
	FailTestIfError(err, "Backup failed", t)

	// Delete the index
	err = secondaryindex.DropSecondaryIndex2(index, bucket, scope, coll, indexManagementAddress)
	FailTestIfError(err, "Error while dropping index", t)
	time.Sleep(2 * time.Second)

	// Run restore operation
	restore, err := br.Restore(indexerUrl, bucket, scope, coll, backup_resp, true)
	FailTestIfError(err, "Restore failed", t)

	log.Printf("Restore response: %v", string(restore[:]))
	time.Sleep(2 * time.Second)

	// Validate Restore Operation (Serverless mode will have only one index replica)
	// Wait for index created
	waitForIndexStatus(bucket, scope, coll, index, status, t)
	waitForIndexStatus(bucket, scope, coll, index+" (replica 1)", status, t)

	// Cleanup
	kvutility.DeleteBucket(bucket, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	secondaryindex.RemoveClientForBucket(kvaddress, bucket)
	time.Sleep(bucketOpWaitDur * time.Second)
}

// This test will perform backup+restore operation on indexer node
// A schedCreateToken will be created on bucket.scope.collection and backup restore operation will tested for it
func TestBackupRestoreSchedToken(t *testing.T) {
	log.Printf("In TestBackupRestoreSchedToken()")

	bucket := "b1"
	scope := "s1"
	coll := "c1"
	index := "index1"

	bucketOp := func() {
		log.Printf("Cleaning up & creating %v:%v:%v", bucket, scope, coll)
		kvutility.DeleteBucket(bucket, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
		kvutility.CreateBucket(bucket, "none", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "100", "")
		kvutility.WaitForBucketCreation(bucket, clusterconfig.Username, clusterconfig.Password, []string{clusterconfig.Nodes[0], clusterconfig.Nodes[1], clusterconfig.Nodes[2]})
		manifest := kvutility.CreateCollection(bucket, scope, coll, clusterconfig.Username, clusterconfig.Password, kvaddress)
		kvutility.WaitForCollectionCreation(bucket, scope, coll, clusterconfig.Username, clusterconfig.Password, []string{clusterconfig.Nodes[0], clusterconfig.Nodes[1], clusterconfig.Nodes[2]}, manifest) //TODO use cid & insert docs
		time.Sleep(bucketOpWaitDur * time.Second)
	}

	//Drop all indexes
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	tc.HandleError(e, "Error in DropAllSecondaryIndexes")

	// Create bucket
	bucketOp()

	// Insert docs
	cid := kvutility.GetCollectionID(bucket, scope, coll, clusterconfig.Username, clusterconfig.Password, kvaddress)
	CreateDocsForCollection(bucket, cid, 1000)
	time.Sleep(2 * time.Second)

	// Get indexer node addr & nodeuuid, bucketuuid
	indexers, err := secondaryindex.GetIndexerNodesHttpAddresses(indexManagementAddress)
	if len(indexers) == 0 {
		FailTestIfError(err, "Can't find indexer nodes", t)
	}
	addr := makeUrlForIndexNode(indexers[0], "")
	bucketUUID, err := br.GetBucketUUID(bucket, clusterconfig.Username, clusterconfig.Password, indexManagementAddress)
	if err != nil {
		log.Printf("GetNodeUUID Err: %v", err)
	}
	IndexerId, err := br.GetNodeUUID(addr)
	if err != nil {
		log.Printf("GetBucketUUID Err: %v", err)
	}

	// Post schedule create request for bucket.scope.collection
	defnId, err := c.NewIndexDefnId()
	FailTestIfError(err, "RestoreContext: fail to generate index definition id %v", t)

	idxDefn := &c.IndexDefn{
		DefnId:          defnId,
		Name:            index,
		Using:           "plasma",
		Bucket:          "b1",
		BucketUUID:      bucketUUID,
		SecExprs:        []string{"age"},
		ExprType:        "N1QL",
		PartitionScheme: "SINGLE",
		// Deferred:        true,
		NumReplica:   1,
		Scope:        scope,
		Collection:   coll,
		CollectionId: cid,
	}

	// Stop processing scheduleCreateToken
	err = secondaryindex.ChangeIndexerSettings("indexer.debug.enableBackgroundIndexCreation", false, clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	br.MakeScheduleCreateRequest(idxDefn, addr, IndexerId)

	// Indexer Url with auth
	indexerUrl := makeUrlForIndexNode(indexers[0], "")

	// Run backup operation
	backup_resp, err := br.Backup(indexerUrl, bucket, scope, coll, true)
	FailTestIfError(err, "Backup failed", t)

	status := "Ready"

	// Verify that scheduleCreateToken is captured during backup operation
	var f interface{}
	json.Unmarshal(backup_resp, &f)
	m := f.(map[string]interface{})
	schedTokens := m["schedTokens"].(map[string]interface{})
	log.Printf("Number of schedule create tokens captured in backup: %v", len(schedTokens))
	if len(schedTokens) == 0 {
		t.Fatalf("No scheduleCreateToken was captured during backup")
	}

	// Start processing scheduleCreateToken
	err = secondaryindex.ChangeIndexerSettings("indexer.debug.enableBackgroundIndexCreation", true, clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	// Delete the index if exists
	err = secondaryindex.DropSecondaryIndex2(index, bucket, scope, coll, indexManagementAddress)
	time.Sleep(3 * time.Second)

	// Run restore operation
	restore, err := br.Restore(indexerUrl, bucket, scope, coll, backup_resp, true)
	FailTestIfError(err, "Restore failed", t)

	log.Printf("Restore response: %v", string(restore[:]))
	time.Sleep(2 * time.Second)

	// Validate Restore Operation (Serverless mode will have only one index replica)
	// Wait for index created
	log.Printf("Wait for index status")
	status = "Ready"
	waitForIndexStatus(bucket, scope, coll, index, status, t)
	waitForIndexStatus(bucket, scope, coll, index+" (replica 1)", status, t)

	// Cleanup
	kvutility.DeleteBucket(bucket, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	secondaryindex.RemoveClientForBucket(kvaddress, bucket)
	time.Sleep(bucketOpWaitDur * time.Second)
}

// This test will perform backup+restore operation on indexer node
// Multiple indexes will be created on same keyspace and backup restore operation will be done
func TestBackupRestoreMultipleIndexes(t *testing.T) {
	log.Printf("In TestBackupRestoreMultipleIndexes()")

	bucket := "b1"
	scope := "s1"
	coll := "c1"
	index := "index"

	bucketOp := func() {
		log.Printf("Cleaning up & creating %v:%v:%v", bucket, scope, coll)
		kvutility.DeleteBucket(bucket, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
		kvutility.CreateBucket(bucket, "none", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "100", "")
		kvutility.WaitForBucketCreation(bucket, clusterconfig.Username, clusterconfig.Password, []string{clusterconfig.Nodes[0], clusterconfig.Nodes[1], clusterconfig.Nodes[2]})
		manifest := kvutility.CreateCollection(bucket, scope, coll, clusterconfig.Username, clusterconfig.Password, kvaddress)
		kvutility.WaitForCollectionCreation(bucket, scope, coll, clusterconfig.Username, clusterconfig.Password, []string{clusterconfig.Nodes[0], clusterconfig.Nodes[1], clusterconfig.Nodes[2]}, manifest) //TODO use cid & insert docs
		time.Sleep(bucketOpWaitDur * time.Second)
	}

	//Drop all indexes
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	tc.HandleError(e, "Error in DropAllSecondaryIndexes")

	// Create bucket
	bucketOp()

	// Insert docs
	cid := kvutility.GetCollectionID(bucket, scope, coll, clusterconfig.Username, clusterconfig.Password, kvaddress)
	CreateDocsForCollection(bucket, cid, 1000)
	time.Sleep(3 * time.Second)

	// Create Index
	withExpr := []byte("{\"num_replica\":1}") // This is 1 by default for serverless
	indexFields := []string{"age", "emailid", "eyeColor", "company", "balance", "gender", "phone", "picture", "registered", "type", "isActive", "friends", "address", "age", "emailid", "eyeColor", "company", "balance", "gender", "phone", "picture", "registered", "type", "isActive", "friends", "address"}
	lif := len(indexFields)
	for i := 0; i < lif; i++ {
		err := secondaryindex.CreateSecondaryIndex3(
			index+strconv.Itoa(i), bucket, scope, coll, indexManagementAddress,
			"", []string{indexFields[i]}, nil,
			false, withExpr, c.SINGLE, nil,
			true, defaultIndexActiveTimeout, nil)
		FailTestIfError(err, "Error creating"+index+strconv.Itoa(i)+" on "+bucket, t)
	}

	// Wait for indexes to be created
	status := "Ready"

	for i := 0; i < lif; i++ {
		waitForIndexStatus(bucket, scope, coll, index+strconv.Itoa(i), status, t)
		waitForIndexStatus(bucket, scope, coll, index+strconv.Itoa(i)+" (replica 1)", status, t)
	}

	indexers, _ := secondaryindex.GetIndexerNodesHttpAddresses(indexManagementAddress)
	if len(indexers) == 0 {
		t.Fatalf("TestSBackupRestore::Unable to find indexer nodes")
	}

	// Indexer Url with auth
	indexerUrl := "http://" + clusterconfig.Username + ":" + clusterconfig.Password + "@" + indexers[0]

	// Run backup operation
	backup_resp, err := br.Backup(indexerUrl, bucket, scope, coll, true)
	FailTestIfError(err, "Backup failed", t)

	// Delete the indexes
	for i := 0; i < lif; i++ {
		err = secondaryindex.DropSecondaryIndex2(index+strconv.Itoa(i), bucket, scope, coll, indexManagementAddress)
		FailTestIfError(err, "Error while dropping index", t)
	}
	time.Sleep(1 * time.Second)

	// Run restore operation
	restore, err := br.Restore(indexerUrl, bucket, scope, coll, backup_resp, true)
	FailTestIfError(err, "Restore failed", t)

	log.Printf("Restore response: %v", string(restore[:]))

	// Validate Restore Operation (Serverless mode will have only one index replica)
	// Wait for index created
	time.Sleep(2 * time.Minute)
	for i := 0; i < lif; i++ {
		waitForIndexStatus(bucket, scope, coll, index+strconv.Itoa(i), status, t)
		waitForIndexStatus(bucket, scope, coll, index+strconv.Itoa(i)+" (replica 1)", status, t)
	}

	// Cleanup
	kvutility.DeleteBucket(bucket, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	secondaryindex.RemoveClientForBucket(kvaddress, bucket)
	time.Sleep(bucketOpWaitDur * time.Second)
}
