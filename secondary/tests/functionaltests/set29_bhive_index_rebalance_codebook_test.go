package functionaltests

import (
	"fmt"
	"log"
	"strings"

	"testing"
	"time"

	"github.com/couchbase/indexing/secondary/testcode"
	"github.com/couchbase/indexing/secondary/tests/framework/clusterutility"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
)

var BHIVE_INDEX_INDEXER_QUOTA = "750"

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

		err := clusterutility.SetDataAndIndexQuota(clusterconfig.Nodes[0], clusterconfig.Username, clusterconfig.Password, "1500", BHIVE_INDEX_INDEXER_QUOTA)
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
		FailTestIfError(e, "Error in loading vector data", subt)
	})

	t.Run("CreateBhiveVectorIndexesBeforeRebalance", func(subt *testing.T) {

		stmt := fmt.Sprintf("CREATE VECTOR INDEX %v"+
			" ON `%v`(sift VECTOR)"+
			" PARTITION BY HASH(meta().id)"+
			" WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\", \"num_partition\":3, \"defer_build\":true};",
			idxBhiveDedicated, BUCKET)
		err := createWithDeferAndBuild(idxBhiveDedicated, BUCKET, "", "", stmt, bhiveIndexActiveTimeout)
		FailTestIfError(err, "Error in creating "+idxBhiveDedicated, subt)

		// Any index created on non-default scope and coll will be a shared instance
		log.Printf("********Create Bhive indices on scope and collection**********")

		stmt = fmt.Sprintf("CREATE VECTOR INDEX %v"+
			" ON `%v`.`%v`.`%v`(sift VECTOR)"+
			" PARTITION BY HASH(meta().id)"+
			" WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\", \"num_partition\":3, \"defer_build\":true};",
			idxBhiveShared, BUCKET, scope, coll)
		err = createWithDeferAndBuild(idxBhiveShared, BUCKET, scope, coll, stmt, bhiveIndexActiveTimeout)
		FailTestIfError(err, "Error in creating "+idxBhiveShared, subt)
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
		addTwoNodesAndRebalance("AddTwoNodesAndRebalanceIn", subt)
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
		TestSwapRebalance(subt)

		indexStatusResp := performCodebookTransferValidation(subt, []string{idxBhiveDedicated, idxBhiveShared})
		validateBhiveScan(subt, indexStatusResp)
	})

	t.Run("TestFailedTraining", func(subt *testing.T) {
		t.Skipf("Skipping TestFailedTraining as training error should not fail rebalances")
		log.Println("*********Setup cluster*********")
		setupCluster(subt)
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
		FailTestIfError(e, "Error in loading vector data", subt)

		log.Printf("********Create bhive indices on scope and collection**********")

		stmt := fmt.Sprintf("CREATE VECTOR INDEX %v"+
			" ON `%v`.`%v`.`%v`(sift VECTOR)"+
			" PARTITION BY HASH(meta().id)"+
			" WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\", \"num_partition\":3, \"defer_build\":true};",
			idxBhiveShared, BUCKET, scope, coll)
		err = createWithDeferAndBuild(idxBhiveShared, BUCKET, scope, coll, stmt, bhiveIndexActiveTimeout)
		FailTestIfError(err, "Error in creating "+idxBhiveShared, subt)

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

func TestBhiveIndexShardRebalance(t *testing.T) {
	skipIfNotPlasma(t)

	t.Run("RebalanceSetupCluster", func(subt *testing.T) {
		TestRebalanceSetupCluster(subt)
		vectorsLoaded = false

		err := clusterutility.SetDataAndIndexQuota(clusterconfig.Nodes[0], clusterconfig.Username, clusterconfig.Password, "1500", BHIVE_INDEX_INDEXER_QUOTA)
		tc.HandleError(err, "Failed to set memory quota in cluster")

		// wait for indexer to come up as the above step will cause a restart
		err = secondaryindex.WaitTillAllIndexNodesActive(kvaddress, defaultIndexActiveTimeout)
		tc.HandleError(err, fmt.Sprintf("The indexer nodes didnt become active, err:%v", err))

		configChanges := map[string]interface{}{
			"indexer.settings.enable_shard_affinity": true,
			"indexer.planner.honourNodesInDefn":      true,
		}
		err = secondaryindex.ChangeMultipleIndexerSettings(configChanges, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[1])
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
		FailTestIfError(e, "Error in loading vector data", subt)
	})

	t.Run("CreateBhiveVectorIndexesBeforeRebalance", func(subt *testing.T) {

		stmt := fmt.Sprintf("CREATE VECTOR INDEX %v"+
			" ON `%v`(sift VECTOR)"+
			" PARTITION BY HASH(meta().id)"+
			" WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\", \"num_partition\":3, \"defer_build\":true};",
			idxBhiveDedicated, BUCKET)
		err := createWithDeferAndBuild(idxBhiveDedicated, BUCKET, "", "", stmt, bhiveIndexActiveTimeout)
		FailTestIfError(err, "Error in creating "+idxBhiveDedicated, subt)

		// Any index created on non-default scope and coll will be a shared instance
		log.Printf("********Create bhive indices on scope and collection**********")

		stmt = fmt.Sprintf("CREATE VECTOR INDEX %v"+
			" ON `%v`.`%v`.`%v`(sift VECTOR)"+
			" PARTITION BY HASH(meta().id)"+
			" WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\", \"num_partition\":3, \"defer_build\":true};",
			idxBhiveShared, BUCKET, scope, coll)
		err = createWithDeferAndBuild(idxBhiveShared, BUCKET, scope, coll, stmt, bhiveIndexActiveTimeout)
		FailTestIfError(err, "Error in creating "+idxBhiveShared, subt)
	})

	defer t.Run("RebalanceResetCluster", func(subt *testing.T) {
		TestRebalanceResetCluster(subt)

		configChanges := map[string]interface{}{
			"indexer.settings.enable_shard_affinity": false,
			"indexer.planner.honourNodesInDefn":      false,
			"indexer.shardRebalance.execTestAction":  false,
		}
		err := secondaryindex.ChangeMultipleIndexerSettings(configChanges, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[1])
		tc.HandleError(err, fmt.Sprintf("Failed to change config %v", configChanges))
	})

	// entry cluster config - [0: kv n1ql] [1: index]
	// exit cluster config - [0: kv n1ql] [1: index] [2: index] [3: index]
	t.Run("TestAddTwoNodesAndRebalanceIn", func(subt *testing.T) {
		addTwoNodesAndRebalance("AddTwoNodesAndRebalanceIn", subt)
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
		performStagingCleanupValidation(subt)
		validateBhiveScan(subt, indexStatusResp)
	})

	t.Run("TestIndexNodeRebalanceOut", func(subt *testing.T) {
		TestIndexNodeRebalanceOut(subt)

		indexStatusResp := performCodebookTransferValidation(subt, []string{idxBhiveDedicated, idxBhiveShared})
		performStagingCleanupValidation(subt)
		validateBhiveScan(subt, indexStatusResp)
	})

	t.Run("TestFailoverAndRebalance", func(subt *testing.T) {
		TestFailoverAndRebalance(subt)

		indexStatusResp := performCodebookTransferValidation(subt, []string{idxBhiveDedicated, idxBhiveShared})
		performStagingCleanupValidation(subt)
		validateBhiveScan(subt, indexStatusResp)
	})

	t.Run("TestSwapRebalance", func(subt *testing.T) {
		TestSwapRebalance(subt)

		indexStatusResp := performCodebookTransferValidation(subt, []string{idxBhiveDedicated, idxBhiveShared})
		performStagingCleanupValidation(subt)
		validateBhiveScan(subt, indexStatusResp)
	})

	// entry and exit cluster config - [0: kv n1ql] [1: index] [2: index]
	t.Run("TestRebalanceReplicaRepair", func(subt *testing.T) {
		log.Print("In TestRebalanceReplicaRepair")
		// Due to previous cluster config, node[3] is still part of the cluster
		swapRebalance(subt, 2, 3)

		status := getClusterStatus()
		if len(status) != 3 || !isNodeIndex(status, clusterconfig.Nodes[1]) ||
			!isNodeIndex(status, clusterconfig.Nodes[2]) {
			subt.Fatalf("%v Unexpected cluster configuration: %v", subt.Name(), status)
		}

		printClusterConfig(subt.Name(), "entry")

		// Create Replicated Vector Instances - shared and dedicated
		// NOTE: don't drop these indexes as next test will use these
		stmt := fmt.Sprintf("CREATE VECTOR INDEX %v"+
			" ON `%v`(sift VECTOR)"+
			" WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\", \"num_replica\":1, \"defer_build\":true};",
			idxBhiveDedicatedReplica, BUCKET)
		err := createWithDeferAndBuild(idxBhiveDedicatedReplica, BUCKET, "", "", stmt, bhiveIndexActiveTimeout)
		FailTestIfError(err, "Error in creating "+idxBhiveDedicatedReplica, subt)

		startTime := time.Now()
		stmt = fmt.Sprintf("CREATE VECTOR INDEX %v"+
			" ON `%v`.`%v`.`%v`(sift VECTOR)"+
			" WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\", \"num_replica\":1, \"defer_build\":true};",
			idxBhiveSharedReplica, BUCKET, scope, coll)
		err = createWithDeferAndBuild(idxBhiveSharedReplica, BUCKET, scope, coll, stmt, bhiveIndexActiveTimeout)
		FailTestIfError(err, "Error in creating "+idxBhiveSharedReplica, subt)
		timeTaken := time.Since(startTime)

		time.Sleep(timeTaken / 2)

		// Failover Node 2 - Replica is lost
		log.Printf("%v: Failing over index node %v", subt.Name(), clusterconfig.Nodes[2])
		failoverNode(clusterconfig.Nodes[2], subt)
		log.Printf("%v: Rebalancing", subt.Name())
		rebalance(subt)

		// Add Node 2 and Rebalance - Replica is repaired
		log.Printf("%v: Add Node %v to the cluster for replica repair", subt.Name(), clusterconfig.Nodes[2])
		addNodeAndRebalance(clusterconfig.Nodes[2], "index", subt)

		waitForRebalanceCleanup()

		indexStatusResp := performCodebookTransferValidation(subt, []string{idxBhiveDedicated, idxBhiveShared})
		validateBhiveScan(subt, indexStatusResp)
	})

	// NOTE: The replicated index from the previous test ensure that atleast one Index is
	// present on the NodeOut
	// entry and exit cluster config - [0: kv n1ql] [1: index] [2: index]
	t.Run("TestRebalanceCancelIndexerAfterTransfer", func(subt *testing.T) {

		log.Print("In TestRebalanceCancelIndexerAfterTransfer")
		status := getClusterStatus()
		if len(status) != 3 || !isNodeIndex(status, clusterconfig.Nodes[1]) ||
			!isNodeIndex(status, clusterconfig.Nodes[2]) {
			subt.Fatalf("%v Unexpected cluster configuration: %v", subt.Name(), status)
		}

		printClusterConfig(subt.Name(), "entry")

		log.Print("** Setting TestAction REBALANCE_CANCEL for SOURCE_SHARDTOKEN_AFTER_TRANSFER")
		configChanges := map[string]interface{}{
			"indexer.shardRebalance.execTestAction": true,
		}
		err := secondaryindex.ChangeMultipleIndexerSettings(configChanges, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[1])
		tc.HandleError(err, "Failed to activate testactions")

		defer func() {
			waitForRebalanceCleanup()
			configChanges := map[string]interface{}{
				"indexer.shardRebalance.execTestAction": false,
			}
			err := secondaryindex.ChangeMultipleIndexerSettings(configChanges, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[1])
			tc.HandleError(err, "Failed to activate testactions")

			removeNode(clusterconfig.Nodes[3], subt)
			printClusterConfig(subt.Name(), "exit")
		}()

		tag := testcode.SOURCE_SHARDTOKEN_AFTER_TRANSFER
		err = testcode.PostOptionsRequestToMetaKV(clusterconfig.Nodes[2], clusterconfig.Username,
			clusterconfig.Password, tag, testcode.REBALANCE_CANCEL, "", 0)
		FailTestIfError(err, "Error while posting request to metaKV", subt)

		swapAndCheckForCleanup(subt, 3, 2)
	})

	// entry and exit cluster config - [0: kv n1ql] [1: index] [2: index]
	t.Run("TestRebalanceCancelIndexerAfterRestore", func(subt *testing.T) {

		log.Print("In TestRebalanceCancelIndexerAfterRestore")
		status := getClusterStatus()
		if len(status) != 3 || !isNodeIndex(status, clusterconfig.Nodes[1]) ||
			!isNodeIndex(status, clusterconfig.Nodes[2]) {
			subt.Fatalf("%v Unexpected cluster configuration: %v", subt.Name(), status)
		}

		printClusterConfig(subt.Name(), "entry")

		log.Print("** Setting TestAction REBALANCE_CANCEL for DEST_SHARDTOKEN_AFTER_RESTORE")
		configChanges := map[string]interface{}{
			"indexer.shardRebalance.execTestAction": true,
		}
		err := secondaryindex.ChangeMultipleIndexerSettings(configChanges, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[1])
		tc.HandleError(err, "Failed to activate testactions")

		defer func() {
			waitForRebalanceCleanup()
			configChanges := map[string]interface{}{
				"indexer.shardRebalance.execTestAction": false,
			}
			err := secondaryindex.ChangeMultipleIndexerSettings(configChanges, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[1])
			tc.HandleError(err, "Failed to activate testactions")

			removeNode(clusterconfig.Nodes[3], subt)
			printClusterConfig(subt.Name(), "exit")
		}()

		tag := testcode.DEST_SHARDTOKEN_AFTER_RESTORE
		err = testcode.PostOptionsRequestToMetaKV(clusterconfig.Nodes[3], clusterconfig.Username,
			clusterconfig.Password, tag, testcode.REBALANCE_CANCEL, "", 0)
		FailTestIfError(err, "Error while posting request to metaKV", subt)

		swapAndCheckForCleanup(subt, 3, 2)
	})

	// entry and exit cluster config - [0: kv n1ql] [1: index] [2: index]
	t.Run("TestRebalanceCancelIndexerBeforeRecovery", func(subt *testing.T) {

		log.Print("In TestRebalanceCancelIndexerBeforeRecovery")
		status := getClusterStatus()
		if len(status) != 3 || !isNodeIndex(status, clusterconfig.Nodes[1]) ||
			!isNodeIndex(status, clusterconfig.Nodes[2]) {
			subt.Fatalf("%v Unexpected cluster configuration: %v", subt.Name(), status)
		}

		printClusterConfig(subt.Name(), "entry")

		log.Print("** Setting TestAction REBALANCE_CANCEL for DEST_INDEXER_BEFORE_INDEX_RECOVERY")
		configChanges := map[string]interface{}{
			"indexer.shardRebalance.execTestAction": true,
		}
		err := secondaryindex.ChangeMultipleIndexerSettings(configChanges, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[1])
		tc.HandleError(err, "Failed to activate testactions")

		defer func() {
			waitForRebalanceCleanup()
			configChanges := map[string]interface{}{
				"indexer.shardRebalance.execTestAction": false,
			}
			err := secondaryindex.ChangeMultipleIndexerSettings(configChanges, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[1])
			tc.HandleError(err, "Failed to activate testactions")

			removeNode(clusterconfig.Nodes[3], subt)
			printClusterConfig(subt.Name(), "exit")
		}()

		tag := testcode.DEST_INDEXER_BEFORE_INDEX_RECOVERY
		err = testcode.PostOptionsRequestToMetaKV(clusterconfig.Nodes[3], clusterconfig.Username,
			clusterconfig.Password, tag, testcode.REBALANCE_CANCEL, "", 0)
		FailTestIfError(err, "Error while posting request to metaKV", subt)

		swapAndCheckForCleanup(subt, 3, 2)
	})

	// entry and exit cluster config - [0: kv n1ql] [1: index] [2: index]
	t.Run("TestRebalanceCancelIndexerAfterRecovery", func(subt *testing.T) {
		log.Print("In TestRebalanceCancelIndexerAfterRecovery")
		status := getClusterStatus()
		if len(status) != 3 || !isNodeIndex(status, clusterconfig.Nodes[1]) ||
			!isNodeIndex(status, clusterconfig.Nodes[2]) {
			subt.Fatalf("%v Unexpected cluster configuration: %v", subt.Name(), status)
		}

		printClusterConfig(subt.Name(), "entry")

		log.Print("** Setting TestAction REBALANCE_CANCEL for DEST_INDEXER_AFTER_INDEX_RECOVERY")
		configChanges := map[string]interface{}{
			"indexer.shardRebalance.execTestAction": true,
		}
		err := secondaryindex.ChangeMultipleIndexerSettings(configChanges, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[1])
		tc.HandleError(err, "Failed to activate testactions")

		defer func() {
			waitForRebalanceCleanup()
			configChanges := map[string]interface{}{
				"indexer.shardRebalance.execTestAction": false,
			}
			err := secondaryindex.ChangeMultipleIndexerSettings(configChanges, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[1])
			tc.HandleError(err, "Failed to activate testactions")

			removeNode(clusterconfig.Nodes[3], subt)
			printClusterConfig(subt.Name(), "exit")
		}()

		tag := testcode.DEST_INDEXER_AFTER_INDEX_RECOVERY
		err = testcode.PostOptionsRequestToMetaKV(clusterconfig.Nodes[3], clusterconfig.Username,
			clusterconfig.Password, tag, testcode.REBALANCE_CANCEL, "", 0)
		FailTestIfError(err, "Error while posting request to metaKV", subt)

		swapAndCheckForCleanup(subt, 3, 2)
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
