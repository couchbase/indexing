package serverlesstests

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	cluster "github.com/couchbase/indexing/secondary/tests/framework/clusterutility"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
)

var rebalanceTmpDir string

const SHARD_REBALANCE_DIR = "shard_rebalance_storage_dir"

var absRebalStorageDirPath string

func TestShardRebalanceSetup(t *testing.T) {
	log.Printf("In TestShardRebalanceSetup")
	// Create a tmp dir in the current working directory
	err := os.Mkdir(SHARD_REBALANCE_DIR, 0755)
	if err != nil {
		t.Fatalf("Error while creating rebalance dir: %v", err)
	}

	cwd, err := filepath.Abs(".")
	FailTestIfError(err, "Error while finding absolute path", t)
	log.Printf("TestShardRebalanceSetup: Using %v as storage dir for rebalance", absRebalStorageDirPath)

	err = secondaryindex.ChangeIndexerSettings("indexer.settings.rebalance.blob_storage_bucket", cwd, clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in change setting for indexer.settings.rebalance.blob_storage_bucket")

	err = secondaryindex.ChangeIndexerSettings("indexer.settings.rebalance.blob_storage_prefix", SHARD_REBALANCE_DIR, clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in change setting for indexer.settings.rebalance.blob_storage_bucket")

	absRebalStorageDirPath = cwd + "/" + SHARD_REBALANCE_DIR
}

// At this point, there are 2 index nodes in the cluster: Nodes[1], Nodes[2]
// Indexes are created during the TestShardIdMapping test.
//
// This test removes Nodes[1], Nodes[2] from the cluster, adds
// Nodes[3], Nodes[4] into the cluster and initiates a rebalance
// All the indexes are expected to be moved to Nodes[3] & Nodes[4]
func TestTwoNodeSwapRebalance(t *testing.T) {
	log.Printf("In TestTwoNodeSwapRebalance")
	performSwapRebalance([]string{clusterconfig.Nodes[3], clusterconfig.Nodes[4]}, []string{clusterconfig.Nodes[1], clusterconfig.Nodes[2]}, t)

	for _, bucket := range buckets {
		for _, collection := range collections {
			for i, index := range indexes {
				partns := indexPartnIds[i]
				if i%2 == 0 {
					scanIndexReplicas(index, bucket, scope, collection, []int{0, 1}, numScans, numDocs, len(partns), t)
				}
			}
		}
	}
}

func getServerGroupForNode(node string) string {
	group_1 := "Group 1"
	group_2 := "Group 2"
	switch node {
	case clusterconfig.Nodes[0], clusterconfig.Nodes[2], clusterconfig.Nodes[4]:
		return group_1
	case clusterconfig.Nodes[1], clusterconfig.Nodes[3], clusterconfig.Nodes[5]:
		return group_2
	default:
		return ""
	}
}

func performSwapRebalance(addNodes []string, removeNodes []string, t *testing.T) {

	for _, node := range addNodes {
		serverGroup := getServerGroupForNode(node)
		if err := cluster.AddNodeWithServerGroup(kvaddress, clusterconfig.Username, clusterconfig.Password, node, "index", serverGroup); err != nil {
			FailTestIfError(err, fmt.Sprintf("Error while adding node %v cluster in server group: %v", node, serverGroup), t)
		}
	}

	if err := cluster.RemoveNodes(kvaddress, clusterconfig.Username, clusterconfig.Password, removeNodes); err != nil {
		FailTestIfError(err, fmt.Sprintf("Error while removing nodes: %v from cluster", removeNodes), t)
	}

	secondaryindex.ResetAllIndexerStats(clusterconfig.Username, clusterconfig.Password, kvaddress)

	// This sleep will ensure that the stats are propagated to client
	// Also, any pending rebalance cleanup is expected to be done during
	// this time - so that validateShardFiles can see cleaned up directories
	waitForStatsUpdate()

	validateIndexPlacement(addNodes, t)
	// Validate the data files on nodes that have been rebalanced out
	for _, removedNode := range removeNodes {
		validateShardFiles(removedNode, t)
	}

	// All indexes are created now. Get the shardId's for each node
	for _, addedNode := range addNodes {
		validateShardIdMapping(addedNode, t)
	}

	verifyStorageDirContents(t)
}

// Prior to this test, indexes are only on Nodes[3], Nodes[4]
// Nodes[3] is in server group "Group 2" & Nodes [4] is in
// server group "Group 1". Indexer will remove Nodes[4], add
// Nodes[2] (which will be in "Group 1") and initiate rebalance.
// After rebalance, all indexes should exist on Nodes[2] & Nodes[3]
func TestSingleNodeSwapRebalance(t *testing.T) {
	log.Printf("In TestSingleNodeSwapRebalance")

	performSwapRebalance([]string{clusterconfig.Nodes[2]}, []string{clusterconfig.Nodes[4]}, t)

	for _, bucket := range buckets {
		for _, collection := range collections {
			for i, index := range indexes {
				partns := indexPartnIds[i]
				if i%2 == 0 { // scan all non-deferred indexes
					scanIndexReplicas(index, bucket, scope, collection, []int{0, 1}, numScans, numDocs, len(partns), t)
				}
			}
		}
	}
}

// Prior to this, the indexes existed on Nodes[2] & Nodes[3].
// In this test, the indexer on Nodes[3] will be failed over
// initiating a replica repair code path. Nodes[1] will be
// added to the cluster & the indexes should be re-built on
// Nodes[1]. Final index placement would be on Nodes[1] & Nodes[2]
func TestReplicaRepair(t *testing.T) {
	log.Printf("In TestReplicaRepair")

	// Failover Nodes[3]
	if err := cluster.FailoverNode(kvaddress, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[3]); err != nil {
		FailTestIfError(err, fmt.Sprintf("Error while failing over nodes: %v from cluster", clusterconfig.Nodes[3]), t)
	}

	rebalance(t)

	// Now, add Nodes[1] to the cluster
	if err := cluster.AddNodeWithServerGroup(kvaddress, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[1], "index", "Group 2"); err != nil {
		FailTestIfError(err, fmt.Sprintf("Error while adding node %v cluster in server group: Group 2", clusterconfig.Nodes[1]), t)
	}
	rebalance(t)

	// Reset all indexer stats
	secondaryindex.ResetAllIndexerStats(clusterconfig.Username, clusterconfig.Password, kvaddress)

	// This sleep will ensure that the stats are propagated to client
	// Also, any pending rebalance cleanup is expected to be done during
	// this time - so that validateShardFiles can see cleaned up directories
	waitForStatsUpdate()

	validateIndexPlacement([]string{clusterconfig.Nodes[1], clusterconfig.Nodes[2]}, t)
	validateShardIdMapping(clusterconfig.Nodes[1], t)
	validateShardIdMapping(clusterconfig.Nodes[2], t)

	for _, bucket := range buckets {
		for _, collection := range collections {
			for i, index := range indexes {
				partns := indexPartnIds[i]
				if i%2 == 0 { // Scan all non-deferred indexes
					scanIndexReplicas(index, bucket, scope, collection, []int{0, 1}, numScans, numDocs, len(partns), t)
				}
			}
		}
	}

	verifyStorageDirContents(t)
}

// Prior to this, the indexes existed on Nodes[1] & Nodes[2].
// In this test, the indexer on Nodes[2] will be failed over
// and Nodes[1] will be swap rebalanced out initiating both
// replica repair & swap rebalance at same time. Final index
// placement would be on Nodes[3] & Nodes[4]
func TestReplicaRepairAndSwapRebalance(t *testing.T) {
	log.Printf("In TestReplicaRepairAndSwapRebalance")

	// Failover Nodes[2]
	if err := cluster.FailoverNode(kvaddress, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[2]); err != nil {
		FailTestIfError(err, fmt.Sprintf("Error while failing over nodes: %v from cluster", clusterconfig.Nodes[2]), t)
	}

	// Now, add Nodes[3] to the cluster
	if err := cluster.AddNodeWithServerGroup(kvaddress, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[3], "index", "Group 2"); err != nil {
		FailTestIfError(err, fmt.Sprintf("Error while adding node %v cluster in server group: Group 2", clusterconfig.Nodes[3]), t)
	}

	// Now, add Nodes[4] to the cluster
	if err := cluster.AddNodeWithServerGroup(kvaddress, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[4], "index", "Group 1"); err != nil {
		FailTestIfError(err, fmt.Sprintf("Error while adding node %v cluster in server group: Group 2", clusterconfig.Nodes[4]), t)
	}

	// Remove nodes also performs rebalance
	if err := cluster.RemoveNodes(kvaddress, clusterconfig.Username, clusterconfig.Password, []string{clusterconfig.Nodes[1]}); err != nil {
		FailTestIfError(err, fmt.Sprintf("Error while removing nodes: %v from cluster", clusterconfig.Nodes[1]), t)
	}

	// Reset all indexer stats
	secondaryindex.ResetAllIndexerStats(clusterconfig.Username, clusterconfig.Password, kvaddress)

	// This sleep will ensure that the stats are propagated to client
	// Also, any pending rebalance cleanup is expected to be done during
	// this time - so that validateShardFiles can see cleaned up directories
	waitForStatsUpdate()

	validateIndexPlacement([]string{clusterconfig.Nodes[3], clusterconfig.Nodes[4]}, t)
	validateShardIdMapping(clusterconfig.Nodes[3], t)
	validateShardIdMapping(clusterconfig.Nodes[4], t)

	for _, bucket := range buckets {
		for _, collection := range collections {
			for i, index := range indexes {
				partns := indexPartnIds[i]
				if i%2 == 0 { // Scan all non-deferred indexes
					scanIndexReplicas(index, bucket, scope, collection, []int{0, 1}, numScans, numDocs, len(partns), t)
				}
			}
		}
	}

	verifyStorageDirContents(t)
}

func TestBuildDeferredIndexesAfterRebalance(t *testing.T) {
	log.Printf("In TestBuildDeferredIndexesAfterRebalance")
	index := indexes[1]
	for _, bucket := range buckets {
		for _, collection := range collections {
			err := secondaryindex.BuildIndexes2([]string{index}, bucket, scope, collection, indexManagementAddress, defaultIndexActiveTimeout)
			if err != nil {
				t.Fatalf("Error observed while building indexes: %v", index)
			}
		}
	}
	// Reset all indexer stats
	secondaryindex.ResetAllIndexerStats(clusterconfig.Username, clusterconfig.Password, kvaddress)

	waitForStatsUpdate()
	validateShardIdMapping(clusterconfig.Nodes[3], t)
	validateShardIdMapping(clusterconfig.Nodes[4], t)

	partns := indexPartnIds[1]
	for _, bucket := range buckets {
		for _, collection := range collections {
			scanIndexReplicas(index, bucket, scope, collection, []int{0, 1}, numScans, numDocs, len(partns), t)
		}
	}

}

// Prior to this test, indexes exist on nodes[3] & nodes[4]
// Indexer tries to drop an index and verifies if the index
// is properly dropped or not
func TestDropIndexAfterRebalance(t *testing.T) {
	log.Printf("In TestDropIndexAfterRebalance")
	for _, bucket := range buckets {
		for _, collection := range collections {
			for i, index := range indexes {
				if i == 0 || i == 1 { // Drop only 0th and 1st index in the list
					err := secondaryindex.DropSecondaryIndex2(index, bucket, scope, collection, indexManagementAddress)
					if err != nil {
						t.Fatalf("Error while dropping index: %v, err: %v", index, err)
					}
				}
			}
		}
	}
	for _, bucket := range buckets {
		for _, collection := range collections {
			for i, index := range indexes {
				if i == 0 || i == 1 { // Drop only 0th and 1st index in the list
					waitForReplicaDrop(index, bucket, scope, collection, 0, t) // wait for replica drop-0
					waitForReplicaDrop(index, bucket, scope, collection, 1, t) // wait for replica drop-1
				}
			}
		}
	}
	// Reset all indexer stats
	secondaryindex.ResetAllIndexerStats(clusterconfig.Username, clusterconfig.Password, kvaddress)

	waitForStatsUpdate()

	validateShardIdMapping(clusterconfig.Nodes[3], t)
	validateShardIdMapping(clusterconfig.Nodes[4], t)

	for _, bucket := range buckets {
		for _, collection := range collections {
			for i, index := range indexes {
				if i == 0 || i == 1 { // Scan only 0th and 1st index in the list
					scanResults, e := secondaryindex.ScanAll2(index, bucket, scope, collection, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
					if e == nil {
						t.Fatalf("Error excpected when scanning for dropped index but scan didnt fail. index: %v, bucket: %v, scope: %v, collection: %v\n", index, bucket, scope, collection)
						log.Printf("Length of scanResults = %v", len(scanResults))
					} else {
						log.Printf("Scan failed as expected with error: %v, index: %v, bucket: %v, scope: %v, collection: %v\n", e, index, bucket, scope, collection)
					}
				}
			}
		}
	}
}

func TestCreateIndexsAfterRebalance(t *testing.T) {
	log.Printf("In TestCreateIndexesAfterRebalance")

	for _, bucket := range buckets {
		for _, collection := range collections {
			// Create a normal index
			n1qlStatement := fmt.Sprintf("create index %v on `%v`.`%v`.`%v`(age)", indexes[0], bucket, scope, collection)
			execN1qlAndWaitForStatus(n1qlStatement, bucket, scope, collection, indexes[0], "Ready", t)
			// Create an index with defer_build
			n1qlStatement = fmt.Sprintf("create index %v on `%v`.`%v`.`%v`(age) with {\"defer_build\":true}", indexes[1], bucket, scope, collection)
			execN1qlAndWaitForStatus(n1qlStatement, bucket, scope, collection, indexes[1], "Created", t)
		}
	}
	// Reset all indexer stats
	secondaryindex.ResetAllIndexerStats(clusterconfig.Username, clusterconfig.Password, kvaddress)

	waitForStatsUpdate()
	validateShardIdMapping(clusterconfig.Nodes[3], t)
	validateShardIdMapping(clusterconfig.Nodes[4], t)

	index := indexes[0]
	partns := indexPartnIds[0]
	for _, bucket := range buckets {
		for _, collection := range collections {
			// Scan only the newly created index
			scanIndexReplicas(index, bucket, scope, collection, []int{0, 1}, numScans, numDocs, len(partns), t)
		}
	}
}

// Helper function that can be used to verify whether an index is dropped or not
// for alter index decrement replica count, alter index drop
func waitForReplicaDrop(index, bucket, scope, collection string, replicaId int, t *testing.T) {
	ticker := time.NewTicker(5 * time.Second)
	indexName := index
	if replicaId != 0 {
		indexName = fmt.Sprintf("%v (replica %v)", index, replicaId)
	}
	// Wait for 2 minutes max for the replica to get dropped
	for {
		select {
		case <-ticker.C:
			stats := secondaryindex.GetIndexStats2(indexName, bucket, scope, collection, clusterconfig.Username, clusterconfig.Password, kvaddress)
			if stats != nil {
				if collection != "_default" {
					if _, ok := stats[bucket+":"+scope+":"+collection+":"+indexName+":items_count"]; ok {
						continue
					}
				} else {
					if _, ok := stats[bucket+":"+indexName+":items_count"]; ok {
						continue
					}
				}
				return
			} else {
				log.Printf("waitForReplicaDrop:: Unable to retrieve index stats for index: %v", indexName)
				return
			}

		case <-time.After(2 * time.Minute):
			t.Fatalf("waitForReplicaDrop:: Index replica %v exists even after 2 minutes", indexName)
			return
		}
	}
	return
}
