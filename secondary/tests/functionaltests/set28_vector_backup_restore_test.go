package functionaltests

import (
	"fmt"
	"log"
	"testing"
	"time"

	br "github.com/couchbase/indexing/secondary/tests/framework/backuprestore"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
)

var idxFieldsDedicated = "idx_dedicated_partn"
var idxFieldsShared = "idx_shared_partn"

var idxDedicatedBhive = "idxBHIVE_dedicated_partn"
var idxSharedBhive = "idxBHIVE_shared_partn"

// This test will perform backup+restore operation on indexer node
// An index will be created on bucket.scope.collection and backup restore operation will be done on the same
// The test creates a mix of Scalar, Composite and Bhive Vector Indexes
func TestVectorBackupRestore(t *testing.T) {
	skipIfNotPlasma(t)

	log.Printf("In TestVectorBackupRestore()")
	setupCluster(t)
	// Any index created on default scope and collection will be a dedicated instance
	vectorSetup(t, BUCKET, "", "", 20000)

	log.Printf("******** Create vector docs on scope and collection **********")

	manifest := kvutility.CreateCollection(BUCKET, scope, coll, clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.GetCollectionID(BUCKET, scope, coll, clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.WaitForCollectionCreation(BUCKET, scope, coll, clusterconfig.Username, clusterconfig.Password, []string{kvaddress}, manifest)

	e := loadVectorData(t, BUCKET, scope, coll, 20000)
	FailTestIfError(e, "Error in loading vector data", t)

	//Drop all indexes
	e = secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes (1)", t)

	// Create standard GSI
	stmt := fmt.Sprintf("CREATE INDEX %v"+
		" ON `%v`(fields)"+
		" PARTITION BY HASH(meta().id)"+
		" WITH {\"num_partition\":4, \"defer_build\":true};",
		idxFieldsDedicated, BUCKET)
	err := createWithDeferAndBuild(idxFieldsDedicated, BUCKET, "", "", stmt, defaultIndexActiveTimeout)
	FailTestIfError(err, "Error in creating "+idxFieldsDedicated, t)

	stmt = fmt.Sprintf("CREATE INDEX %v"+
		" ON `%v`.`%v`.`%v`(fields)"+
		" PARTITION BY HASH(meta().id)"+
		" WITH { \"num_partition\":4, \"defer_build\":true};",
		idxFieldsShared, BUCKET, scope, coll)
	err = createWithDeferAndBuild(idxFieldsShared, BUCKET, scope, coll, stmt, defaultIndexActiveTimeout)
	FailTestIfError(err, "Error in creating "+idxFieldsShared, t)

	// Create Composite Vector Indexes
	stmt = fmt.Sprintf("CREATE INDEX %v"+
		" ON `%v`(sift VECTOR)"+
		" PARTITION BY HASH(meta().id)"+
		" WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\", \"num_partition\":4, \"defer_build\":true};",
		idxDedicated, BUCKET)
	err = createWithDeferAndBuild(idxDedicated, BUCKET, "", "", stmt, defaultIndexActiveTimeout)
	FailTestIfError(err, "Error in creating "+idxDedicated, t)

	stmt = fmt.Sprintf("CREATE INDEX %v"+
		" ON `%v`.`%v`.`%v`(sift VECTOR)"+
		" PARTITION BY HASH(meta().id)"+
		" WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\", \"num_partition\":4, \"defer_build\":true};",
		idxShared, BUCKET, scope, coll)
	err = createWithDeferAndBuild(idxShared, BUCKET, scope, coll, stmt, defaultIndexActiveTimeout)
	FailTestIfError(err, "Error in creating "+idxShared, t)

	// Create Bhive Vector Indexes
	stmt = fmt.Sprintf("CREATE VECTOR INDEX %v"+
		" ON `%v`(sift VECTOR)"+
		" PARTITION BY HASH(meta().id)"+
		" WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\", \"num_partition\":4, \"defer_build\":true};",
		idxDedicatedBhive, BUCKET)
	err = createWithDeferAndBuild(idxDedicatedBhive, BUCKET, "", "", stmt, defaultIndexActiveTimeout)
	FailTestIfError(err, "Error in creating "+idxDedicatedBhive, t)

	stmt = fmt.Sprintf("CREATE VECTOR INDEX %v"+
		" ON `%v`.`%v`.`%v`(sift VECTOR)"+
		" PARTITION BY HASH(meta().id)"+
		" WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\", \"num_partition\":4, \"defer_build\":true};",
		idxSharedBhive, BUCKET, scope, coll)
	err = createWithDeferAndBuild(idxSharedBhive, BUCKET, scope, coll, stmt, defaultIndexActiveTimeout)
	FailTestIfError(err, "Error in creating "+idxSharedBhive, t)

	indexers, _ := secondaryindex.GetIndexerNodesHttpAddresses(indexManagementAddress)
	if len(indexers) == 0 {
		t.Fatalf("TestBackupRestore::Unable to find indexer nodes")
	}

	// Indexer Url with auth
	indexerUrl := "http://" + clusterconfig.Username + ":" + clusterconfig.Password + "@" + indexers[0]

	// Run backup operation
	backup_resp_def, err := br.Backup(indexerUrl, BUCKET, "_default", "_default", true)
	FailTestIfError(err, "Backup failed for default keyspace", t)

	backup_resp, err := br.Backup(indexerUrl, BUCKET, scope, coll, true)
	FailTestIfError(err, "Backup failed", t)

	// Delete all indexes
	e = secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes (2)", t)
	time.Sleep(5 * time.Second)

	// Run restore operations
	restore_def, err := br.Restore(indexerUrl, BUCKET, "_default", "_default", backup_resp_def, true)
	FailTestIfError(err, "Restore failed for default keyspace", t)
	log.Printf("Restore response for default keyspace: %v", string(restore_def[:]))
	time.Sleep(2 * time.Second)

	restore, err := br.Restore(indexerUrl, BUCKET, scope, coll, backup_resp, true)
	FailTestIfError(err, "Restore failed", t)
	log.Printf("Restore response: %v", string(restore[:]))
	time.Sleep(2 * time.Second)

}

func TestBuildRestoredVectorIndexes(t *testing.T) {
	skipIfNotPlasma(t)
	log.Printf("In TestBuildRestoredVectorIndexes()")

	defer func() {
		// Cleanup
		err := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
		FailTestIfError(err, "Error in DropAllSecondaryIndexes (2)", t)
		time.Sleep(5 * time.Second)

		kvutility.FlushBucket(BUCKET, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
		secondaryindex.RemoveClientForBucket(kvaddress, BUCKET)
		time.Sleep(bucketOpWaitDur * time.Second)
	}()

	sharedInsts := []string{idxShared, idxFieldsShared, idxSharedBhive}
	dedicatedInsts := []string{idxDedicated, idxFieldsDedicated, idxBhiveDedicated}

	// Validate Restore Operation
	// During Restore, the indexes are not built, there have defer_build = true
	// 1. Check All the Indexes are in expected INDEX_STATE_READY
	checkForIndexStatus(BUCKET, scope, coll, sharedInsts, "INDEX_STATE_READY", t)
	checkForIndexStatus(BUCKET, "", "", dedicatedInsts, "INDEX_STATE_READY", t)

	// 2. Issue build request per keyspace
	err := issueBuildStatement(BUCKET, scope, coll, sharedInsts)
	FailTestIfError(err, "execution of build statement failed", t)

	err = issueBuildStatement(BUCKET, "", "", dedicatedInsts)
	FailTestIfError(err, "execution of build statement failed", t)

	// 3. Wait for the Indexes to go online
	client, err := secondaryindex.GetOrCreateClient(indexManagementAddress, "2itest")
	FailTestIfError(err, "execution of build statement failed", t)

	waitForIndexesToGoActive := func(indexNames []string, bucket, scope, coll string) {
		defnIds := make([]uint64, len(indexNames))
		for i := range indexNames {
			defnIds[i], _ = secondaryindex.GetDefnID2(client, bucket, scope, coll, indexNames[i])
		}
		for i := range defnIds {
			err = secondaryindex.WaitTillIndexActive(defnIds[i], client, defaultIndexActiveTimeout)
			FailTestIfError(err, fmt.Sprintf("Index:%v ", indexNames[i]), t)
		}
	}

	waitForIndexesToGoActive(sharedInsts, BUCKET, scope, coll)
	waitForIndexesToGoActive(dedicatedInsts, BUCKET, "_default", "_default")

}

func checkForIndexStatus(bucket, scope, coll string, indexes []string, expectedState string, t *testing.T) {
	for _, indexName := range indexes {
		state, err := secondaryindex.IndexState2(indexName, bucket, scope, coll, indexManagementAddress)
		FailTestIfError(err, "Error in retrieving state", t)
		if state != expectedState {
			t.Fatalf("Index:%v, not found in the expected state:%v, found:%v",
				indexName, expectedState, state)
		}
	}
}
