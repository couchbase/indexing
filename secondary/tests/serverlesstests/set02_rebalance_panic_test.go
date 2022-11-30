package serverlesstests

import (
	"fmt"
	"log"
	"testing"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/testcode"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
)

func TestRebalancePanicTestsSetup(t *testing.T) {
	//a. Drop all secondary indexes
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	tc.HandleError(e, "Error in DropAllSecondaryIndexes")
	for _, bucket := range buckets {
		kvutility.DeleteBucket(bucket, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	}
	time.Sleep(bucketOpWaitDur * time.Second)

	if absRebalStorageDirPath == "" {
		makeStorageDir(t)
	}

	//b. Remove all nodes from the cluster & keep only nodes[1], nodes[2]
	resetCluster(t)

	// c. For each bucket, create indexes on c1 collection -> Just one collection
	// is sufficient for these tests as the goal is to validate rebalance failure
	// scenarios. Use partitioned index
	collection := "c1"
	for _, bucket := range buckets {
		kvutility.CreateBucket(bucket, "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "100", "11213")
		kvutility.WaitForBucketCreation(bucket, clusterconfig.Username, clusterconfig.Password, []string{clusterconfig.Nodes[0], clusterconfig.Nodes[1], clusterconfig.Nodes[2]})

		manifest := kvutility.CreateCollection(bucket, scope, collection, clusterconfig.Username, clusterconfig.Password, clusterconfig.KVAddress)
		log.Printf("TestIndexPlacement: Manifest for bucket: %v, scope: %v, collection: %v is: %v", bucket, scope, collection, manifest)
		cid := kvutility.WaitForCollectionCreation(bucket, scope, collection, clusterconfig.Username, clusterconfig.Password, []string{clusterconfig.Nodes[0], clusterconfig.Nodes[1], clusterconfig.Nodes[2]}, manifest)

		CreateDocsForCollection(bucket, cid, numDocs)

		n1qlStatement := fmt.Sprintf("create index %v on `%v`.`%v`.`%v`(age)", indexes[0], bucket, scope, collection)
		execN1qlAndWaitForStatus(n1qlStatement, bucket, scope, collection, indexes[0], "Ready", t)

		// Create a partitioned index
		n1qlStatement = fmt.Sprintf("create index %v on `%v`.`%v`.`%v`(emalid) partition by hash(meta().id)", indexes[4], bucket, scope, collection)
		execN1qlAndWaitForStatus(n1qlStatement, bucket, scope, collection, indexes[4], "Ready", t)

		// Create a partitioned index with defer_build:true
		n1qlStatement = fmt.Sprintf("create index %v on `%v`.`%v`.`%v`(balance) partition by hash(meta().id)  with {\"defer_build\":true}", indexes[5], bucket, scope, collection)
		execN1qlAndWaitForStatus(n1qlStatement, bucket, scope, collection, indexes[5], "Created", t)
	}
}

// Prior to this test, indexes existed on nodes[1] & nodes[2].
// This test will try to swap rebalance by adding nodes[3] & nodes[4],
// removing nodes[1], nodes[2]. A panic is invoked in the code
// after transfer token move to state "ScheduleAck". Post rebalance
// failure, indexes should remain on nodes[1] & nodes[2]. The storage
// directory for rebalance should remain empty
func TestRebalancePanicAtMasterShardTokenScheduleAck(t *testing.T) {
	log.Printf("In TestRebalancePanicAtMasterShardTokenScheduleAck")

	err := secondaryindex.ChangeIndexerSettings("indexer.shardRebalance.cancelOrPanic", "panic", clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	tag := testcode.MASTER_SHARDTOKEN_SCHEDULEACK
	err = secondaryindex.ChangeIndexerSettings("indexer.shardRebalance.cancelOrPanicTag", tag, clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	performSwapRebalance([]string{clusterconfig.Nodes[3], clusterconfig.Nodes[4]}, []string{clusterconfig.Nodes[1], clusterconfig.Nodes[2]}, true, t)
	waitForRebalanceCleanup(clusterconfig.Nodes[1], t)
	waitForRebalanceCleanup(clusterconfig.Nodes[2], t)
	waitForRebalanceCleanup(clusterconfig.Nodes[3], t)
	waitForRebalanceCleanup(clusterconfig.Nodes[4], t)

	secondaryindex.ResetAllIndexerStats(clusterconfig.Username, clusterconfig.Password, kvaddress)
	waitForStatsUpdate()

	validateIndexPlacement([]string{clusterconfig.Nodes[1], clusterconfig.Nodes[2]}, t)
	validateShardIdMapping(clusterconfig.Nodes[1], t)
	validateShardIdMapping(clusterconfig.Nodes[2], t)

	validateShardFiles(clusterconfig.Nodes[3], t)
	validateShardFiles(clusterconfig.Nodes[4], t)

	collection := "c1"
	// Scan indexes
	for _, bucket := range buckets {
		scanIndexReplicas(indexes[0], bucket, scope, collection, []int{0, 1}, numScans, numDocs, len(indexPartnIds[0]), t)
		scanIndexReplicas(indexes[4], bucket, scope, collection, []int{0, 1}, numScans, numDocs, len(indexPartnIds[4]), t)

		scanResults, e := secondaryindex.ScanAll2(indexes[5], bucket, scope, collection, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
		if e == nil {
			t.Fatalf("Error excpected when scanning for dropped index but scan didnt fail. index: %v, bucket: %v, scope: %v, collection: %v\n", indexes[5], bucket, scope, collection)
			log.Printf("Length of scanResults = %v", len(scanResults))
		} else {
			log.Printf("Scan failed as expected with error: %v, index: %v, bucket: %v, scope: %v, collection: %v\n", e, indexes[5], bucket, scope, collection)
		}
	}

	verifyStorageDirContents(t)

	testDDLAfterRebalance(t)

}
