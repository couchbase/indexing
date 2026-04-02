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

const sparseVecField = "sparse_dim"
const sparseNprobes = 256

type sparseRebalanceConfig struct {
	createStmtPrefix                 string
	indexerQuota                     string
	indexActiveTimeout               int64
	dedicated                        string
	shared                           string
	dedicatedReplica                 string
	sharedReplica                    string
	hasTrainingFailNotEnoughDocsTest bool
}

var sparseCompositeRebalanceConfig = sparseRebalanceConfig{
	createStmtPrefix:                 "CREATE INDEX",
	indexerQuota:                     VECTOR_INDEX_INDEXER_QUOTA,
	indexActiveTimeout:               defaultIndexActiveTimeout,
	dedicated:                        "idxSparseComposite_dedicated_partn",
	shared:                           "idxSparseComposite_shared_partn",
	dedicatedReplica:                 "idxSparseComposite_dedicated_replica",
	sharedReplica:                    "idxSparseComposite_shared_replica",
	hasTrainingFailNotEnoughDocsTest: true,
}

func TestSparseIndexDCPRebalance(t *testing.T) {
	runSparseIndexDCPRebalance(t, sparseCompositeRebalanceConfig)
}

func TestSparseIndexShardRebalance(t *testing.T) {
	runSparseIndexShardRebalance(t, sparseCompositeRebalanceConfig)
}

func runSparseIndexDCPRebalance(t *testing.T, cfg sparseRebalanceConfig) {
	skipIfNotPlasma(t)

	t.Run("RebalanceSetupCluster", func(subt *testing.T) {
		TestRebalanceSetupCluster(subt)
		resetVectorDataSetupFlags()

		err := clusterutility.SetDataAndIndexQuota(clusterconfig.Nodes[0], clusterconfig.Username, clusterconfig.Password, "1500", cfg.indexerQuota)
		tc.HandleError(err, "Failed to set memory quota in cluster")

		err = secondaryindex.WaitTillAllIndexNodesActive(kvaddress, defaultIndexActiveTimeout)
		tc.HandleError(err, fmt.Sprintf("The indexer nodes didnt become active, err:%v", err))
	})

	t.Run("LoadSparseDocsBeforeRebalance", func(subt *testing.T) {
		loadSparseDocsBeforeRebalance(subt)
	})

	t.Run("CreateSparseIndexesBeforeRebalance", func(subt *testing.T) {
		createSparseIndexesBeforeRebalance(subt, cfg)
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

	t.Run("TestAddTwoNodesAndRebalanceIn", func(subt *testing.T) {
		addTwoNodesAndRebalance("AddTwoNodesAndRebalanceIn", subt)
		waitForRebalanceCleanup()

		report, err := getLastRebalanceReport(kvaddress, clusterconfig.Username, clusterconfig.Password)
		tc.HandleError(err, "Failed to get last rebalance report")
		if completionMsg, exists := report["completionMessage"]; exists &&
			!strings.Contains(completionMsg.(string), "Rebalance completed successfully.") {
			subt.Fatalf("Expected rebalance to be successful- %v", report)
		} else if !exists {
			subt.Fatalf("Rebalance report does not have any completion message - %v", report)
		}

		indexStatusResp := performCodebookTransferValidation(subt, []string{cfg.dedicated, cfg.shared})
		validateSparseScan(subt, indexStatusResp, cfg)
	})

	t.Run("TestIndexNodeRebalanceOut", func(subt *testing.T) {
		TestIndexNodeRebalanceOut(subt)

		indexStatusResp := performCodebookTransferValidation(subt, []string{cfg.dedicated, cfg.shared})
		validateSparseScan(subt, indexStatusResp, cfg)
	})

	t.Run("TestFailoverAndRebalance", func(subt *testing.T) {
		TestFailoverAndRebalance(subt)

		indexStatusResp := performCodebookTransferValidation(subt, []string{cfg.dedicated, cfg.shared})
		validateSparseScan(subt, indexStatusResp, cfg)
	})

	t.Run("TestSwapRebalance", func(subt *testing.T) {
		TestSwapRebalance(subt)

		indexStatusResp := performCodebookTransferValidation(subt, []string{cfg.dedicated, cfg.shared})
		validateSparseScan(subt, indexStatusResp, cfg)
	})

	t.Run("TestFailedTraining", func(subt *testing.T) {
		runSparseFailedTraining(subt, cfg)
	})

	if cfg.hasTrainingFailNotEnoughDocsTest {
		t.Run("TestTrainingFailNotEnoughDocument", func(subt *testing.T) {
			runSparseTrainingFailNotEnoughDocument(subt)
		})
	}
}

func runSparseIndexShardRebalance(t *testing.T, cfg sparseRebalanceConfig) {
	skipIfNotPlasma(t)

	t.Run("RebalanceSetupCluster", func(subt *testing.T) {
		TestRebalanceSetupCluster(subt)
		resetVectorDataSetupFlags()

		err := clusterutility.SetDataAndIndexQuota(clusterconfig.Nodes[0], clusterconfig.Username, clusterconfig.Password, "1500", cfg.indexerQuota)
		tc.HandleError(err, "Failed to set memory quota in cluster")

		err = secondaryindex.WaitTillAllIndexNodesActive(kvaddress, defaultIndexActiveTimeout)
		tc.HandleError(err, fmt.Sprintf("The indexer nodes didnt become active, err:%v", err))

		configChanges := map[string]interface{}{
			"indexer.settings.enable_shard_affinity": true,
			"indexer.planner.honourNodesInDefn":      true,
		}
		err = secondaryindex.ChangeMultipleIndexerSettings(configChanges, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[1])
		tc.HandleError(err, fmt.Sprintf("Failed to change config %v", configChanges))
	})

	t.Run("LoadSparseDocsBeforeRebalance", func(subt *testing.T) {
		loadSparseDocsBeforeRebalance(subt)
	})

	t.Run("CreateSparseIndexesBeforeRebalance", func(subt *testing.T) {
		createSparseIndexesBeforeRebalance(subt, cfg)
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

	t.Run("TestAddTwoNodesAndRebalanceIn", func(subt *testing.T) {
		addTwoNodesAndRebalance("AddTwoNodesAndRebalanceIn", subt)
		waitForRebalanceCleanup()

		report, err := getLastRebalanceReport(kvaddress, clusterconfig.Username, clusterconfig.Password)
		tc.HandleError(err, "Failed to get last rebalance report")
		if completionMsg, exists := report["completionMessage"]; exists &&
			!strings.Contains(completionMsg.(string), "Rebalance completed successfully.") {
			subt.Fatalf("Expected rebalance to be successful- %v", report)
		} else if !exists {
			subt.Fatalf("Rebalance report does not have any completion message - %v", report)
		}

		indexStatusResp := performCodebookTransferValidation(subt, []string{cfg.dedicated, cfg.shared})
		performStagingCleanupValidation(subt)
		validateSparseScan(subt, indexStatusResp, cfg)
	})

	t.Run("TestIndexNodeRebalanceOut", func(subt *testing.T) {
		TestIndexNodeRebalanceOut(subt)

		indexStatusResp := performCodebookTransferValidation(subt, []string{cfg.dedicated, cfg.shared})
		performStagingCleanupValidation(subt)
		validateSparseScan(subt, indexStatusResp, cfg)
	})

	t.Run("TestFailoverAndRebalance", func(subt *testing.T) {
		TestFailoverAndRebalance(subt)

		indexStatusResp := performCodebookTransferValidation(subt, []string{cfg.dedicated, cfg.shared})
		performStagingCleanupValidation(subt)
		validateSparseScan(subt, indexStatusResp, cfg)
	})

	t.Run("TestSwapRebalance", func(subt *testing.T) {
		TestSwapRebalance(subt)

		indexStatusResp := performCodebookTransferValidation(subt, []string{cfg.dedicated, cfg.shared})
		performStagingCleanupValidation(subt)
		validateSparseScan(subt, indexStatusResp, cfg)
	})

	t.Run("TestRebalanceReplicaRepair", func(subt *testing.T) {
		log.Print("In TestRebalanceReplicaRepair")
		swapRebalance(subt, 2, 3)

		status := getClusterStatus()
		if len(status) != 3 || !isNodeIndex(status, clusterconfig.Nodes[1]) ||
			!isNodeIndex(status, clusterconfig.Nodes[2]) {
			subt.Fatalf("%v Unexpected cluster configuration: %v", subt.Name(), status)
		}

		printClusterConfig(subt.Name(), "entry")

		stmt := fmt.Sprintf("%v %v ON `%v`(%v SPARSE VECTOR) WITH { \"description\": \"IVF256\", \"num_replica\":1, \"defer_build\":true};",
			cfg.createStmtPrefix, cfg.dedicatedReplica, BUCKET, sparseVecField)
		err := createWithDeferAndBuild(cfg.dedicatedReplica, BUCKET, "", "", stmt, cfg.indexActiveTimeout)
		FailTestIfError(err, "Error in creating "+cfg.dedicatedReplica, subt)

		startTime := time.Now()
		stmt = fmt.Sprintf("%v %v ON `%v`.`%v`.`%v`(%v SPARSE VECTOR) WITH { \"description\": \"IVF256\", \"num_replica\":1, \"defer_build\":true};",
			cfg.createStmtPrefix, cfg.sharedReplica, BUCKET, scope, coll, sparseVecField)
		err = createWithDeferAndBuild(cfg.sharedReplica, BUCKET, scope, coll, stmt, cfg.indexActiveTimeout)
		FailTestIfError(err, "Error in creating "+cfg.sharedReplica, subt)
		timeTaken := time.Since(startTime)

		time.Sleep(timeTaken / 2)

		log.Printf("%v: Failing over index node %v", subt.Name(), clusterconfig.Nodes[2])
		failoverNode(clusterconfig.Nodes[2], subt)
		log.Printf("%v: Rebalancing", subt.Name())
		rebalance(subt)

		log.Printf("%v: Add Node %v to the cluster for replica repair", subt.Name(), clusterconfig.Nodes[2])
		addNodeAndRebalance(clusterconfig.Nodes[2], "index", subt)

		waitForRebalanceCleanup()

		indexStatusResp := performCodebookTransferValidation(subt, []string{cfg.dedicated, cfg.shared})
		validateSparseScan(subt, indexStatusResp, cfg)
	})

	t.Run("TestRebalanceCancelIndexerAfterTransfer", func(subt *testing.T) {
		runSparseRebalanceCancelTest(subt, cfg, testcode.SOURCE_SHARDTOKEN_AFTER_TRANSFER)
	})

	t.Run("TestRebalanceCancelIndexerAfterRestore", func(subt *testing.T) {
		runSparseRebalanceCancelTest(subt, cfg, testcode.DEST_SHARDTOKEN_AFTER_RESTORE)
	})

	t.Run("TestRebalanceCancelIndexerBeforeRecovery", func(subt *testing.T) {
		runSparseRebalanceCancelTest(subt, cfg, testcode.DEST_INDEXER_BEFORE_INDEX_RECOVERY)
	})

	t.Run("TestRebalanceCancelIndexerAfterRecovery", func(subt *testing.T) {
		runSparseRebalanceCancelTest(subt, cfg, testcode.DEST_INDEXER_AFTER_INDEX_RECOVERY)
	})
}

// Shared sparse rebalance helpers

func loadSparseDocsBeforeRebalance(t *testing.T) {
	vectorSetupSparse(t, BUCKET, "", "", 10000, false)

	log.Printf("******** Create sparse vector docs on scope and collection **********")

	manifest := kvutility.CreateCollection(BUCKET, scope, coll, clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.GetCollectionID(BUCKET, scope, coll, clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.WaitForCollectionCreation(BUCKET, scope, coll, clusterconfig.Username, clusterconfig.Password, []string{kvaddress}, manifest)

	e := loadSparseVectorData(t, BUCKET, scope, coll, 10000, false)
	FailTestIfError(e, "Error in loading sparse vector data", t)
}

func runSparseFailedTraining(t *testing.T, cfg sparseRebalanceConfig) {
	t.Skipf("Skipping TestFailedTraining as training error should not fail rebalances")
	log.Println("*********Setup cluster*********")
	setupCluster(t)
	resetVectorDataSetupFlags()

	err := clusterutility.SetDataAndIndexQuota(clusterconfig.Nodes[0], clusterconfig.Username, clusterconfig.Password, "1500", cfg.indexerQuota)
	tc.HandleError(err, "Failed to set memory quota in cluster")

	secondaryindex.WaitTillAllIndexNodesActive(kvaddress, defaultIndexActiveTimeout)

	err = secondaryindex.WaitForSystemIndices(kvaddress, 0)
	tc.HandleError(err, "Waiting for indices in system scope")

	loadSparseDocsBeforeRebalance(t)

	log.Printf("********Create sparse indices on scope and collection**********")

	stmt := fmt.Sprintf("%v %v ON `%v`.`%v`.`%v`(%v SPARSE VECTOR) PARTITION BY HASH(meta().id) WITH { \"description\": \"IVF256\", \"num_partition\":3, \"defer_build\":true};",
		cfg.createStmtPrefix, cfg.shared, BUCKET, scope, coll, sparseVecField)
	err = createWithDeferAndBuild(cfg.shared, BUCKET, scope, coll, stmt, cfg.indexActiveTimeout)
	FailTestIfError(err, "Error in creating "+cfg.shared, t)

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
	FailTestIfError(err, "Error while posting request to metaKV", t)

	addNode(clusterconfig.Nodes[2], "index", t)
	if err := clusterutility.RemoveNode(kvaddress, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[1]); err == nil {
		t.Fatalf("%v expected rebalance to fail due to failed training but rebalance completed successfully", t.Name())
	}
}

func runSparseTrainingFailNotEnoughDocument(t *testing.T) {
	printClusterConfig(t.Name(), "entry")

	removeNode(clusterconfig.Nodes[2], t)
	printClusterConfig(t.Name(), "after setup")

	kvutility.EnableBucketFlush(BUCKET, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.FlushBucket(BUCKET, "", clusterconfig.Username, clusterconfig.Password, kvaddress)

	addNode(clusterconfig.Nodes[3], "index", t)
	if err := clusterutility.RemoveNode(clusterconfig.KVAddress, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[1]); err != nil {
		t.Fatalf("%v expected rebalance to pass since index is kept in error state but rebalance fail", t.Name())
	}
}

func createSparseIndexesBeforeRebalance(t *testing.T, cfg sparseRebalanceConfig) {
	stmt := fmt.Sprintf("%v %v ON `%v`(%v SPARSE VECTOR) PARTITION BY HASH(meta().id) WITH { \"description\": \"IVF256\", \"num_partition\":3, \"defer_build\":true};",
		cfg.createStmtPrefix, cfg.dedicated, BUCKET, sparseVecField)
	err := createWithDeferAndBuild(cfg.dedicated, BUCKET, "", "", stmt, cfg.indexActiveTimeout)
	FailTestIfError(err, "Error in creating "+cfg.dedicated, t)

	log.Printf("******** Create sparse indices on scope and collection **********")

	stmt = fmt.Sprintf("%v %v ON `%v`.`%v`.`%v`(%v SPARSE VECTOR) PARTITION BY HASH(meta().id) WITH { \"description\": \"IVF256\", \"num_partition\":3, \"defer_build\":true};",
		cfg.createStmtPrefix, cfg.shared, BUCKET, scope, coll, sparseVecField)
	err = createWithDeferAndBuild(cfg.shared, BUCKET, scope, coll, stmt, cfg.indexActiveTimeout)
	FailTestIfError(err, "Error in creating "+cfg.shared, t)
}

func validateSparseScan(t *testing.T, statuses *tc.IndexStatusResponse, cfg sparseRebalanceConfig) {
	found := make(map[string]bool)
	for _, status := range statuses.Status {
		found[status.Name] = true
	}

	for _, idxName := range []string{cfg.dedicatedReplica, cfg.dedicated} {
		if found[idxName] {
			runSparseScan(t, idxName, sparseVecField, "", "", sparseNprobes, limit, -1)
		}
	}

	for _, idxName := range []string{cfg.sharedReplica, cfg.shared} {
		if found[idxName] {
			runSparseScan(t, idxName, sparseVecField, scope, coll, sparseNprobes, limit, -1)
		}
	}
}

func runSparseRebalanceCancelTest(t *testing.T, cfg sparseRebalanceConfig, tag testcode.TestActionTag) {
	log.Printf("In %v", t.Name())
	status := getClusterStatus()
	if len(status) != 3 || !isNodeIndex(status, clusterconfig.Nodes[1]) ||
		!isNodeIndex(status, clusterconfig.Nodes[2]) {
		t.Fatalf("%v Unexpected cluster configuration: %v", t.Name(), status)
	}

	printClusterConfig(t.Name(), "entry")

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

		removeNode(clusterconfig.Nodes[3], t)
		printClusterConfig(t.Name(), "exit")
	}()

	targetNode := clusterconfig.Nodes[3]
	if tag == testcode.SOURCE_SHARDTOKEN_AFTER_TRANSFER {
		targetNode = clusterconfig.Nodes[2]
	}

	err = testcode.PostOptionsRequestToMetaKV(targetNode, clusterconfig.Username,
		clusterconfig.Password, tag, testcode.REBALANCE_CANCEL, "", 0)
	FailTestIfError(err, "Error while posting request to metaKV", t)

	swapAndCheckForSparseCleanup(t, cfg, 3, 2)
}

func swapAndCheckForSparseCleanup(t *testing.T, cfg sparseRebalanceConfig, nidIn, nidOut int) {
	log.Printf("** Starting Shard Rebalance (node n%v <=> n%v)", nidOut, nidIn)
	swapRebalance(t, nidIn, nidOut)

	report, err := getLastRebalanceReport(kvaddress, clusterconfig.Username, clusterconfig.Password)
	tc.HandleError(err, "Failed to get last rebalance report")
	if completionMsg, exists := report["completionMessage"]; exists &&
		!strings.Contains(completionMsg.(string), "stopped by user") {
		t.Fatalf("Expected rebalance to be cancelled but it did not cancel. Report - %v", report)
	} else if !exists {
		t.Fatalf("Rebalance report does not have any completion message - %v", report)
	}

	waitForRebalanceCleanup()

	indexStatusResp := performCodebookTransferValidation(t, []string{cfg.dedicated, cfg.shared})
	validateSparseScan(t, indexStatusResp, cfg)

	time.Sleep(10 * time.Second)
}
