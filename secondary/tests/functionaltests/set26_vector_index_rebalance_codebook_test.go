package functionaltests

import (
	"fmt"
	"github.com/couchbase/indexing/secondary/tests/framework/clusterutility"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	"log"
	"strings"
	"testing"
)

var VECTOR_INDEX_INDEXER_QUOTA = "512"

func TestVectorIndexShardRebalance(t *testing.T) {
	skipIfNotPlasma(t)

	scope, coll := "s1", "c1"
	idxDedicated := "idxCVI_dedicated_partn"
	idxShared := "idxCVI_shared_partn"

	t.Run("RebalanceSetupCluster", func(subt *testing.T) {
		TestRebalanceSetupCluster(subt)
		vectorsLoaded = false

		err := clusterutility.SetDataAndIndexQuota(clusterconfig.Nodes[0], clusterconfig.Username, clusterconfig.Password, "1500", VECTOR_INDEX_INDEXER_QUOTA)
		tc.HandleError(err, "Failed to set memory quota in cluster")

		// wait for indexer to come up as the above step will cause a restart
		err = secondaryindex.WaitTillAllIndexNodesActive(kvaddress, defaultIndexActiveTimeout)
		tc.HandleError(err, fmt.Sprintf("The indexer nodes didnt become active, err:%v", err))

		// For Shard and codebook movement enable shard affinity
		configChanges := map[string]interface{}{
			"indexer.settings.enable_shard_affinity": true,
			"indexer.planner.honourNodesInDefn":      true,
		}
		err = secondaryindex.ChangeMultipleIndexerSettings(configChanges, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[1])
		tc.HandleError(err, fmt.Sprintf("Failed to change config %v", configChanges))
	})

	defer t.Run("RebalanceResetCluster", func(subt *testing.T) {
		TestRebalanceResetCluster(subt)

		configChanges := map[string]interface{}{
			"indexer.settings.enable_shard_affinity": false,
			"indexer.planner.honourNodesInDefn":      false,
		}
		err := secondaryindex.ChangeMultipleIndexerSettings(configChanges, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[1])
		tc.HandleError(err, fmt.Sprintf("Failed to change config %v", configChanges))
	})

	t.Run("LoadVectorDocsBeforeRebalance", func(subt *testing.T) {

		// Any index created on default scope and collection will be a dedicated instance
		vectorSetup(subt, BUCKET, "", "", 10000)

		log.Printf("******** Create vector docs on scope and collection **********")

		manifest := kvutility.CreateCollection(BUCKET, scope, coll, clusterconfig.Username, clusterconfig.Password, kvaddress)
		kvutility.GetCollectionID(BUCKET, scope, coll, clusterconfig.Username, clusterconfig.Password, kvaddress)
		kvutility.WaitForCollectionCreation(BUCKET, scope, coll, clusterconfig.Username, clusterconfig.Password, []string{kvaddress}, manifest)

		e := loadVectorData(subt, BUCKET, scope, coll, 10000)
		FailTestIfError(e, "Error in loading vector data", t)
	})

	t.Run("CreateCompositeVectorIndexesBeforeRebalance", func(subt *testing.T) {

		stmt := fmt.Sprintf("CREATE INDEX %v"+
			" ON `%v`(sift VECTOR)"+
			" PARTITION BY HASH(meta().id)"+
			" WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\", \"num_partition\":3, \"defer_build\":true};",
			idxDedicated, BUCKET)
		err := createWithDeferAndBuild(idxDedicated, BUCKET, "", "", stmt, defaultIndexActiveTimeout)
		FailTestIfError(err, "Error in creating "+idxDedicated, t)

		// Any index created on non-default scope and coll will be a shared instance
		log.Printf("********Create composite indices on scope and collection**********")

		stmt = fmt.Sprintf("CREATE INDEX %v"+
			" ON `%v`.`%v`.`%v`(sift VECTOR)"+
			" PARTITION BY HASH(meta().id)"+
			" WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\", \"num_partition\":3, \"defer_build\":true};",
			idxShared, BUCKET, scope, coll)
		err = createWithDeferAndBuild(idxShared, BUCKET, scope, coll, stmt, defaultIndexActiveTimeout)
		FailTestIfError(err, "Error in creating "+idxShared, t)
	})

	// entry cluster config - [0: kv n1ql] [1: index]
	// exit cluster config - [0: kv n1ql] [1: index] [2: index] [3: index]
	t.Run("AddTwoNodesAndRebalanceIn", func(subt *testing.T) {
		addTwoNodesAndRebalance("AddTwoNodesAndRebalanceIn", t)
		waitForRebalanceCleanup()

		report, err := getLastRebalanceReport(kvaddress, clusterconfig.Username,
			clusterconfig.Password)
		tc.HandleError(err, "Failed to get last rebalance report")

		if completionMsg, exists := report["completionMessage"]; exists &&
			!strings.Contains(completionMsg.(string), "Rebalance completed successfully.") {
			subt.Fatalf("Expected rebalance to be successful- %v",
				report)
		} else if !exists {
			subt.Fatalf("Rebalance report does not have any completion message - %v",
				report)
		}

	})

}

