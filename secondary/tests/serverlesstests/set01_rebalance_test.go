package serverlesstests

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"

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
