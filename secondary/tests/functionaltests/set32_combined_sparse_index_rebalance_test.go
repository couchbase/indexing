package functionaltests

import (
	"fmt"
	"strings"
	"testing"

	"github.com/couchbase/indexing/secondary/tests/framework/clusterutility"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
)

func TestCombinedSparseIndexDCPRebalance(t *testing.T) {
	skipIfNotPlasma(t)

	t.Run("RebalanceSetupCluster", func(subt *testing.T) {
		TestRebalanceSetupCluster(subt)
		resetVectorDataSetupFlags()

		err := clusterutility.SetDataAndIndexQuota(clusterconfig.Nodes[0], clusterconfig.Username, clusterconfig.Password, "1500", BHIVE_INDEX_INDEXER_QUOTA)
		tc.HandleError(err, "Failed to set memory quota in cluster")

		err = secondaryindex.WaitTillAllIndexNodesActive(kvaddress, defaultIndexActiveTimeout)
		tc.HandleError(err, fmt.Sprintf("The indexer nodes didnt become active, err:%v", err))
	})

	t.Run("LoadSparseDocsBeforeRebalance", func(subt *testing.T) {
		loadSparseDocsBeforeRebalance(subt)
	})

	t.Run("CreateBothSparseIndexesBeforeRebalance", func(subt *testing.T) {
		createSparseIndexesBeforeRebalance(subt, sparseCompositeRebalanceConfig)
		createSparseIndexesBeforeRebalance(subt, sparseBhiveRebalanceConfig)
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

	allIdxNames := []string{sparseCompositeRebalanceConfig.dedicated, sparseCompositeRebalanceConfig.shared, sparseBhiveRebalanceConfig.dedicated, sparseBhiveRebalanceConfig.shared}

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

		indexStatusResp := performCodebookTransferValidation(subt, allIdxNames)
		validateSparseScan(subt, indexStatusResp, sparseCompositeRebalanceConfig)
		validateSparseScan(subt, indexStatusResp, sparseBhiveRebalanceConfig)
	})

	t.Run("TestIndexNodeRebalanceOut", func(subt *testing.T) {
		TestIndexNodeRebalanceOut(subt)

		indexStatusResp := performCodebookTransferValidation(subt, allIdxNames)
		validateSparseScan(subt, indexStatusResp, sparseCompositeRebalanceConfig)
		validateSparseScan(subt, indexStatusResp, sparseBhiveRebalanceConfig)
	})

	t.Run("TestFailoverAndRebalance", func(subt *testing.T) {
		TestFailoverAndRebalance(subt)

		indexStatusResp := performCodebookTransferValidation(subt, allIdxNames)
		validateSparseScan(subt, indexStatusResp, sparseCompositeRebalanceConfig)
		validateSparseScan(subt, indexStatusResp, sparseBhiveRebalanceConfig)
	})

	t.Run("TestSwapRebalance", func(subt *testing.T) {
		TestSwapRebalance(subt)

		indexStatusResp := performCodebookTransferValidation(subt, allIdxNames)
		validateSparseScan(subt, indexStatusResp, sparseCompositeRebalanceConfig)
		validateSparseScan(subt, indexStatusResp, sparseBhiveRebalanceConfig)
	})
}

func TestCombinedSparseIndexShardRebalance(t *testing.T) {
	skipIfNotPlasma(t)

	t.Run("RebalanceSetupCluster", func(subt *testing.T) {
		TestRebalanceSetupCluster(subt)
		resetVectorDataSetupFlags()

		err := clusterutility.SetDataAndIndexQuota(clusterconfig.Nodes[0], clusterconfig.Username, clusterconfig.Password, "1500", BHIVE_INDEX_INDEXER_QUOTA)
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

	t.Run("CreateBothSparseIndexesBeforeRebalance", func(subt *testing.T) {
		createSparseIndexesBeforeRebalance(subt, sparseCompositeRebalanceConfig)
		createSparseIndexesBeforeRebalance(subt, sparseBhiveRebalanceConfig)
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

	allIdxNames := []string{sparseCompositeRebalanceConfig.dedicated, sparseCompositeRebalanceConfig.shared, sparseBhiveRebalanceConfig.dedicated, sparseBhiveRebalanceConfig.shared}

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

		indexStatusResp := performCodebookTransferValidation(subt, allIdxNames)
		performStagingCleanupValidation(subt)
		validateSparseScan(subt, indexStatusResp, sparseCompositeRebalanceConfig)
		validateSparseScan(subt, indexStatusResp, sparseBhiveRebalanceConfig)
	})

	t.Run("TestIndexNodeRebalanceOut", func(subt *testing.T) {
		TestIndexNodeRebalanceOut(subt)

		indexStatusResp := performCodebookTransferValidation(subt, allIdxNames)
		performStagingCleanupValidation(subt)
		validateSparseScan(subt, indexStatusResp, sparseCompositeRebalanceConfig)
		validateSparseScan(subt, indexStatusResp, sparseBhiveRebalanceConfig)
	})

	t.Run("TestFailoverAndRebalance", func(subt *testing.T) {
		TestFailoverAndRebalance(subt)

		indexStatusResp := performCodebookTransferValidation(subt, allIdxNames)
		performStagingCleanupValidation(subt)
		validateSparseScan(subt, indexStatusResp, sparseCompositeRebalanceConfig)
		validateSparseScan(subt, indexStatusResp, sparseBhiveRebalanceConfig)
	})

	t.Run("TestSwapRebalance", func(subt *testing.T) {
		TestSwapRebalance(subt)

		indexStatusResp := performCodebookTransferValidation(subt, allIdxNames)
		performStagingCleanupValidation(subt)
		validateSparseScan(subt, indexStatusResp, sparseCompositeRebalanceConfig)
		validateSparseScan(subt, indexStatusResp, sparseBhiveRebalanceConfig)
	})
}
