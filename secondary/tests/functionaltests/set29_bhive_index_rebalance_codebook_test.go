package functionaltests

import (
	"fmt"
	"log"
	"strings"

	"testing"

	"github.com/couchbase/indexing/secondary/testcode"
	"github.com/couchbase/indexing/secondary/tests/framework/clusterutility"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
)

var BHIVE_INDEX_INDEXER_QUOTA = "1024"

var idxBhiveDedicated = "idxBHIVE_dedicated_partn"
var idxBhiveShared = "idxBHIVE_shared_partn"
var idxBhiveDedicatedReplica = "idxBHIVE_dedicated_replica"
var idxBhiveSharedReplica = "idxBHIVE_dedicated_replica"

var bhiveIndexActiveTimeout = int64(float64(defaultIndexActiveTimeout) * 1)

func TestBhiveIndexDCPRebalance(t *testing.T) {
	skipIfNotPlasma(t)
	t.Run("RebalanceSetupCluster", func(subt *testing.T) {
		TestRebalanceSetupCluster(subt)
		vectorsLoaded = false

		err := clusterutility.SetDataAndIndexQuota(clusterconfig.Nodes[0], clusterconfig.Username, clusterconfig.Password, "1500", "1024")
		tc.HandleError(err, "Failed to set memory quota in cluster")

		// wait for indexer to come up as the above step will cause a restart
		err = secondaryindex.WaitTillAllIndexNodesActive(kvaddress, defaultIndexActiveTimeout)
		tc.HandleError(err, fmt.Sprintf("The indexer nodes didnt become active, err:%v", err))

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

	t.Run("CreateBhiveVectorIndexesBeforeRebalance", func(subt *testing.T) {

		stmt := fmt.Sprintf("CREATE VECTOR INDEX %v"+
			" ON `%v`(sift VECTOR)"+
			" PARTITION BY HASH(meta().id)"+
			" WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\", \"num_partition\":3, \"defer_build\":true};",
			idxBhiveDedicated, BUCKET)
		err := createWithDeferAndBuild(idxBhiveDedicated, BUCKET, "", "", stmt, bhiveIndexActiveTimeout)
		FailTestIfError(err, "Error in creating "+idxBhiveDedicated, t)

		// Any index created on non-default scope and coll will be a shared instance
		log.Printf("********Create Bhive indices on scope and collection**********")

		stmt = fmt.Sprintf("CREATE VECTOR INDEX %v"+
			" ON `%v`.`%v`.`%v`(sift VECTOR)"+
			" PARTITION BY HASH(meta().id)"+
			" WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\", \"num_partition\":3, \"defer_build\":true};",
			idxBhiveShared, BUCKET, scope, coll)
		err = createWithDeferAndBuild(idxBhiveShared, BUCKET, scope, coll, stmt, bhiveIndexActiveTimeout)
		FailTestIfError(err, "Error in creating "+idxBhiveShared, t)
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

	// entry cluster config - [0: kv n1ql] [1: index]
	// exit cluster config - [0: kv n1ql] [1: index] [2: index] [3: index]
	t.Run("TestAddTwoNodesAndRebalanceIn", func(subt *testing.T) {
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

		indexStatusResp := performCodebookTransferValidation(subt, []string{idxBhiveDedicated, idxBhiveShared})
		validateBhiveScan(subt, indexStatusResp)
	})

	t.Run("TestIndexNodeRebalanceOut", func(subt *testing.T) {
		TestIndexNodeRebalanceOut(subt)

		indexStatusResp := performCodebookTransferValidation(subt, []string{idxBhiveDedicated, idxBhiveShared})
		validateBhiveScan(subt, indexStatusResp)
	})

	t.Run("TestFailoverAndRebalance", func(subt *testing.T) {
		TestFailoverAndRebalance(subt)

		indexStatusResp := performCodebookTransferValidation(subt, []string{idxBhiveDedicated, idxBhiveShared})
		validateBhiveScan(subt, indexStatusResp)
	})

	t.Run("TestSwapRebalance", func(subt *testing.T) {
		TestSwapRebalance(t)

		indexStatusResp := performCodebookTransferValidation(subt, []string{idxBhiveDedicated, idxBhiveShared})
		validateBhiveScan(subt, indexStatusResp)
	})

	t.Run("TestFailedTraining", func(subt *testing.T) {
		log.Println("*********Setup cluster*********")
		setupCluster(t)
		var err error

		err = clusterutility.SetDataAndIndexQuota(clusterconfig.Nodes[0], clusterconfig.Username, clusterconfig.Password, "1500", BHIVE_INDEX_INDEXER_QUOTA)
		tc.HandleError(err, "Failed to set memory quota in cluster")
		// wait for indexer to come up as the above step will cause a restart
		secondaryindex.WaitTillAllIndexNodesActive(kvaddress, defaultIndexActiveTimeout)

		err = secondaryindex.WaitForSystemIndices(kvaddress, 0)
		tc.HandleError(err, "Waiting for indices in system scope")

		vectorSetup(subt, BUCKET, "", "", 10000)

		log.Printf("******** Create vector docs on scope and collection **********")

		manifest := kvutility.CreateCollection(BUCKET, scope, coll, clusterconfig.Username, clusterconfig.Password, kvaddress)
		kvutility.GetCollectionID(BUCKET, scope, coll, clusterconfig.Username, clusterconfig.Password, kvaddress)
		kvutility.WaitForCollectionCreation(BUCKET, scope, coll, clusterconfig.Username, clusterconfig.Password, []string{kvaddress}, manifest)

		e := loadVectorData(subt, BUCKET, scope, coll, 10000)
		FailTestIfError(e, "Error in loading vector data", t)

		log.Printf("********Create bhive indices on scope and collection**********")

		stmt := fmt.Sprintf("CREATE VECTOR INDEX %v"+
			" ON `%v`.`%v`.`%v`(sift VECTOR)"+
			" PARTITION BY HASH(meta().id)"+
			" WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\", \"num_partition\":3, \"defer_build\":true};",
			idxBhiveShared, BUCKET, scope, coll)
		err = createWithDeferAndBuild(idxBhiveShared, BUCKET, scope, coll, stmt, bhiveIndexActiveTimeout)
		FailTestIfError(err, "Error in creating "+idxBhiveShared, t)

		err = secondaryindex.ChangeMultipleIndexerSettings(map[string]interface{}{"indexer.shardRebalance.execTestAction": true},
			clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[1])
		tc.HandleError(err, "Failed to activate testactions")

		defer func() {
			err = secondaryindex.ChangeMultipleIndexerSettings(map[string]interface{}{"indexer.shardRebalance.execTestAction": false},
				clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[2])
			tc.HandleError(err, "Failed to de-activate testactions")
		}()

		tag := testcode.BUILD_INDEX_TRAINING
		err = testcode.PostOptionsRequestToMetaKV2(clusterconfig.Nodes[2], clusterconfig.Username,
			clusterconfig.Password, tag, testcode.INJECT_ERROR, "", 0, "Test Induced Training Error")
		FailTestIfError(err, "Error while posting request to metaKV", subt)

		addNode(clusterconfig.Nodes[2], "index", subt)
		if err := clusterutility.RemoveNode(kvaddress, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[1]); err == nil {
			subt.Fatalf("%v expected rebalance to fail due to failed training but rebalance completed successfully", subt.Name())
		}

	})
}

func validateBhiveScan(t *testing.T, statuses *tc.IndexStatusResponse) {

	dedicatedNames := []string{idxBhiveDedicatedReplica, idxBhiveDedicated}
	sharedNames := []string{idxBhiveSharedReplica, idxBhiveShared}

	var foundDedicatedIdx []string
	var foundSharedIdx []string

OUTER:
	for _, status := range statuses.Status {
		for _, idxName := range dedicatedNames {
			if status.Name == idxName {
				foundDedicatedIdx = append(foundDedicatedIdx, idxName)
				continue OUTER
			}
		}
		for _, idxName := range sharedNames {
			if status.Name == idxName {
				foundSharedIdx = append(foundSharedIdx, idxName)
				continue OUTER
			}
		}
	}

	for _, idxName := range foundDedicatedIdx {
		performVectorScanOnIndex(t, idxName, "", "")
	}
	for _, idxName := range foundSharedIdx {
		performVectorScanOnIndex(t, idxName, scope, coll)
	}
}
