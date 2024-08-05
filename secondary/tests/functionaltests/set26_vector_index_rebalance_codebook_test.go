package functionaltests

import (
	"fmt"
	c "github.com/couchbase/indexing/secondary/common"
	qc "github.com/couchbase/indexing/secondary/queryport/client"
	"github.com/couchbase/indexing/secondary/testcode"
	"github.com/couchbase/indexing/secondary/tests/framework/clusterutility"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

var VECTOR_INDEX_INDEXER_QUOTA = "512"

var scope, coll = "s1", "c1"
var idxDedicated = "idxCVI_dedicated_partn"
var idxShared = "idxCVI_shared_partn"

var limit int64 = 5

func TestVectorIndexDCPRebalance(t *testing.T) {
	skipIfNotPlasma(t)
	t.Run("RebalanceSetupCluster", func(subt *testing.T) {
		TestRebalanceSetupCluster(subt)
		vectorsLoaded = false

		err := clusterutility.SetDataAndIndexQuota(clusterconfig.Nodes[0], clusterconfig.Username, clusterconfig.Password, "1500", VECTOR_INDEX_INDEXER_QUOTA)
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

		performCodebookTransferValidation(subt, []string{idxDedicated, idxShared})
		validateVectorScan(subt)
	})

	t.Run("TestIndexNodeRebalanceOut", func(subt *testing.T) {
		TestIndexNodeRebalanceOut(subt)

		performCodebookTransferValidation(subt, []string{idxDedicated, idxShared})
		validateVectorScan(subt)
	})

	t.Run("TestFailoverAndRebalance", func(subt *testing.T) {
		TestFailoverAndRebalance(subt)

		performCodebookTransferValidation(subt, []string{idxDedicated, idxShared})
		validateVectorScan(subt)
	})

	t.Run("TestSwapRebalance", func(subt *testing.T) {
		TestSwapRebalance(t)

		performCodebookTransferValidation(subt, []string{idxDedicated, idxShared})
		validateVectorScan(subt)
	})
}

func TestVectorIndexShardRebalance(t *testing.T) {
	skipIfNotPlasma(t)

	t.Run("RebalanceSetupCluster", func(subt *testing.T) {
		TestRebalanceSetupCluster(subt)
		vectorsLoaded = false

		err := clusterutility.SetDataAndIndexQuota(clusterconfig.Nodes[0], clusterconfig.Username, clusterconfig.Password, "1500", VECTOR_INDEX_INDEXER_QUOTA)
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

		performCodebookTransferValidation(subt, []string{idxDedicated, idxShared})
		performStagingCleanupValidation(subt)
		validateVectorScan(subt)
	})

	t.Run("TestIndexNodeRebalanceOut", func(subt *testing.T) {
		TestIndexNodeRebalanceOut(subt)

		performCodebookTransferValidation(subt, []string{idxDedicated, idxShared})
		performStagingCleanupValidation(subt)
		validateVectorScan(subt)
	})

	t.Run("TestFailoverAndRebalance", func(subt *testing.T) {
		TestFailoverAndRebalance(subt)

		performCodebookTransferValidation(subt, []string{idxDedicated, idxShared})
		performStagingCleanupValidation(subt)
		validateVectorScan(subt)
	})

	t.Run("TestSwapRebalance", func(subt *testing.T) {
		TestSwapRebalance(t)

		performCodebookTransferValidation(subt, []string{idxDedicated, idxShared})
		performStagingCleanupValidation(subt)
		validateVectorScan(subt)
	})

	// entry and exit cluster config - [0: kv n1ql] [1: index] [2: index]
	t.Run("TestRebalanceCancelIndexerAfterTransfer", func(subt *testing.T) {
		// Due to previous cluster config, node[3] is still part of the cluster
		swapRebalance(t, 2, 3)

		log.Print("In TestRebalanceCancelIndexerAfterTransfer")
		status := getClusterStatus()
		if len(status) != 3 || !isNodeIndex(status, clusterconfig.Nodes[1]) ||
			!isNodeIndex(status, clusterconfig.Nodes[2]) {
			subt.Fatalf("%v Unexpected cluster configuration: %v", subt.Name(), status)
		}

		printClusterConfig(subt.Name(), "entry")

		log.Print("** Setting TestAction REBALANCE_CANCEL for SOURCE_SHARDTOKEN_AFTER_TRANSFER")
		err := secondaryindex.ChangeIndexerSettings("indexer.shardRebalance.execTestAction", true,
			clusterconfig.Username, clusterconfig.Password, kvaddress)
		tc.HandleError(err, "Failed to activate testactions")

		defer func() {
			err = secondaryindex.ChangeIndexerSettings("indexer.shardRebalance.execTestAction", false,
				clusterconfig.Username, clusterconfig.Password, kvaddress)
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
		err := secondaryindex.ChangeIndexerSettings("indexer.shardRebalance.execTestAction", true,
			clusterconfig.Username, clusterconfig.Password, kvaddress)
		tc.HandleError(err, "Failed to activate testactions")

		defer func() {
			err = secondaryindex.ChangeIndexerSettings("indexer.shardRebalance.execTestAction", false,
				clusterconfig.Username, clusterconfig.Password, kvaddress)
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
		err := secondaryindex.ChangeIndexerSettings("indexer.shardRebalance.execTestAction", true,
			clusterconfig.Username, clusterconfig.Password, kvaddress)
		tc.HandleError(err, "Failed to activate testactions")

		defer func() {
			err = secondaryindex.ChangeIndexerSettings("indexer.shardRebalance.execTestAction", false,
				clusterconfig.Username, clusterconfig.Password, kvaddress)
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
		err := secondaryindex.ChangeIndexerSettings("indexer.shardRebalance.execTestAction", true,
			clusterconfig.Username, clusterconfig.Password, kvaddress)
		tc.HandleError(err, "Failed to activate testactions")

		defer func() {
			err = secondaryindex.ChangeIndexerSettings("indexer.shardRebalance.execTestAction", false,
				clusterconfig.Username, clusterconfig.Password, kvaddress)
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

func swapAndCheckForCleanup(t *testing.T, nidIn, nidOut int) {
	log.Printf("** Starting Shard Rebalance (node n%v <=> n%v)", nidOut, nidIn)
	swapRebalance(t, nidIn, nidOut)

	report, err := getLastRebalanceReport(kvaddress, clusterconfig.Username,
		clusterconfig.Password)
	tc.HandleError(err, "Failed to get last rebalance report")
	if completionMsg, exists := report["completionMessage"]; exists &&
		!strings.Contains(completionMsg.(string), "stopped by user") {
		t.Fatalf("Expected rebalance to be cancelled but it did not cancel. Report - %v",
			report)
	} else if !exists {
		t.Fatalf("Rebalance report does not have any completion message - %v",
			report)
	}

	waitForRebalanceCleanup()

	performCodebookTransferValidation(t, []string{idxDedicated, idxShared})
	validateVectorScan(t)
}

func validateVectorScan(t *testing.T) {
	performVectorScanOnIndex(t, idxDedicated, "", "")
	performVectorScanOnIndex(t, idxShared, scope, coll)
}

func performVectorScanOnIndex(t *testing.T, idxName, scope, coll string) {
	scans := qc.Scans{
		&qc.Scan{
			Filter: []*qc.CompositeElementFilter{
				&qc.CompositeElementFilter{},
			},
		},
	}

	scanResults, err := secondaryindex.Scan6(idxName, bucket, scope, coll, kvaddress, scans, false, false,
		nil, 0, limit, nil, c.AnyConsistency, nil, indexVector)
	FailTestIfError(err, "Error during secondary index scan", t)

	vectorPosReturned := make([]uint32, 0)
	for k, _ := range scanResults {
		s := strings.Split(k, "_")
		vps := s[1]
		vp, err := strconv.Atoi(vps)
		if err != nil {
			t.Fatal(err)
		}
		vectorPosReturned = append(vectorPosReturned, uint32(vp))
	}

	recall := recallAtR(expectedVectorPosTop100[0:int(limit)], vectorPosReturned, int(limit))
	log.Printf("For idx:%v Recall: %v expected: %v result: %v %+v", idxName, recall,
		expectedVectorPosTop100[0:int(limit)], vectorPosReturned, scanResults)
}

func getAllStorageDirs(t *testing.T) map[string]string {
	storageDirs := make(map[string]string)
	hosts, errHosts := secondaryindex.GetIndexerNodesHttpAddresses(indexManagementAddress)
	FailTestIfError(errHosts, "Error in GetIndexerNodesHttpAddresses", t)

	for _, host := range hosts {
		indexStorageDir, errGetSetting := tc.GetIndexerSetting(host, "indexer.storage_dir",
			clusterconfig.Username, clusterconfig.Password)
		FailTestIfError(errGetSetting, "Error in GetIndexerSetting", t)

		strIndexStorageDir := fmt.Sprintf("%v", indexStorageDir)
		absIndexStorageDir, err1 := filepath.Abs(strIndexStorageDir)
		FailTestIfError(err1, "Error while finding absolute path", t)

		exists, _ := verifyPathExists(absIndexStorageDir)

		if exists {
			storageDirs[host] = absIndexStorageDir
		} else {
			FailTestIfError(fmt.Errorf("Storage path should exist"), "Error while verifying if path exists", t)
		}
	}
	return storageDirs
}

func performCodebookTransferValidation(subt *testing.T, idxNames []string) {

	var statuses *tc.IndexStatusResponse
	err := c.NewRetryHelper(10, 10*time.Millisecond, 5, func(attempts int, lastErr error) error {
		if attempts > 0 {
			log.Printf("WARN - failed getting live indexer info from getIndexStatus for %v times. Last err - %v", attempts, lastErr)
		}
		var err error
		statuses, err = getIndexStatusFromIndexer()
		return err
	}).RunWithConditionalError(func(err error) bool {
		return !(strings.Contains(err.Error(), syscall.ECONNREFUSED.Error()))
	})

	if err != nil {
		FailTestIfError(err, "Error while getIndexStatusFromIndexer", subt)
	}

	storageDirMap := getAllStorageDirs(subt)

	for _, status := range statuses.Status {
		found := false
		for _, idxName := range idxNames {
			if status.Name == idxName {
				found = true
			}
		}
		if !found {
			continue
		}

		for _, host := range status.Hosts {
			indexerAddr := secondaryindex.GetIndexHttpAddrOnNode(clusterconfig.Username, clusterconfig.Password, host)

			for _, partnId := range status.PartitionMap[host] {
				slicePath, err := tc.GetIndexSlicePath(status.Name, status.Bucket, storageDirMap[indexerAddr], c.PartitionId(partnId))
				FailTestIfError(err, "Error while GetIndexSlicePath", subt)

				codebookName := tc.GetCodebookName(status.Name, status.Bucket, status.InstId, c.PartitionId(partnId))
				codebookPath := filepath.Join(slicePath, tc.CODEBOOK_DIR, codebookName)

				if exist, err1 := verifyPathExists(codebookPath); err1 != nil {
					FailTestIfError(err1, "Error while verifying codebook path", subt)
				} else if !exist {
					FailTestIfError(fmt.Errorf("Expected codebook to exist for idx:%v, inst:%v, partnId:%v",
						status.Name, status.InstId, c.PartitionId(partnId)), "Error while verifying codebook path", subt)
				}

			}
		}
	}
}

func performStagingCleanupValidation(subt *testing.T) {
	storageDirMap := getAllStorageDirs(subt)

	for _, path := range storageDirMap {
		rebalStagingDir := filepath.Join(path, tc.REBALANCE_STAGING_DIR)
		if exist, err := verifyPathExists(rebalStagingDir); err != nil {
			FailTestIfError(err, "Error while verifying staging dir", subt)
		} else if !exist {
			// the entire staging dir was cleaned up; proceed to next host
			continue
		}

		// if the staging dir exist, check for residual in the codebook folder
		codebookStaging := filepath.Join(rebalStagingDir, tc.CODEBOOK_COPY_PREFIX)
		if exist, err := verifyPathExists(rebalStagingDir); err != nil {
			FailTestIfError(err, "Error while verifying staging dir", subt)
		} else if !exist {
			// the entire codebook dir was cleaned up; proceed to next host
			continue
		}

		err := checkForAnyResidualCodebooks(codebookStaging)
		FailTestIfError(err, "Found residual codebook in the staging folder", subt)
	}
}

func checkForAnyResidualCodebooks(dirPath string) error {

	dirWalkFn := func(pth string, finfo os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if finfo == nil {
			return fmt.Errorf("Nil finifo for path %v", pth)
		}

		if finfo.IsDir() {
			if finfo.Name() == ".tmp" {
				return filepath.SkipDir
			}
			return nil
		}

		if strings.Contains(strings.ToLower(finfo.Name()), "codebook") {
			return fmt.Errorf("Found a codebook at path: %v", pth)
		}
		log.Printf("WARN - found non-empty codebook folder at path:%v, file: %v", pth, finfo.Name())
		return nil
	}

	return filepath.WalkDir(dirPath, dirWalkFn)
}
