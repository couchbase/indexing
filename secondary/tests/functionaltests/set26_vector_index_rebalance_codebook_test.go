package functionaltests

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	qc "github.com/couchbase/indexing/secondary/queryport/client"
	"github.com/couchbase/indexing/secondary/testcode"
	"github.com/couchbase/indexing/secondary/tests/framework/clusterutility"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	"github.com/couchbase/plasma"
)

var VECTOR_INDEX_INDEXER_QUOTA = "512"

var scope, coll = "s1", "c1"
var idxDedicated = "idxCVI_dedicated_partn"
var idxShared = "idxCVI_shared_partn"
var idxDedicatedReplica = "idxCVI_dedicated_replica"
var idxSharedReplica = "idxCVI_dedicated_replica"

var limit int64 = 5

func Test_SaveMProf2(t *testing.T) {
	Test_SaveMProf(t)
}

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

		indexStatusResp := performCodebookTransferValidation(subt, []string{idxDedicated, idxShared})
		validateVectorScan(subt, indexStatusResp)
	})

	t.Run("TestIndexNodeRebalanceOut", func(subt *testing.T) {
		TestIndexNodeRebalanceOut(subt)

		indexStatusResp := performCodebookTransferValidation(subt, []string{idxDedicated, idxShared})
		validateVectorScan(subt, indexStatusResp)
	})

	t.Run("TestFailoverAndRebalance", func(subt *testing.T) {
		TestFailoverAndRebalance(subt)

		indexStatusResp := performCodebookTransferValidation(subt, []string{idxDedicated, idxShared})
		validateVectorScan(subt, indexStatusResp)
	})

	t.Run("TestSwapRebalance", func(subt *testing.T) {
		TestSwapRebalance(subt)

		indexStatusResp := performCodebookTransferValidation(subt, []string{idxDedicated, idxShared})
		validateVectorScan(subt, indexStatusResp)
	})

	t.Run("TestFailedTraining", func(subt *testing.T) {
		t.Skipf("Skipping TestFailedTraining as training error should not fail rebalances")
		log.Println("*********Setup cluster*********")
		setupCluster(subt)
		var err error

		err = clusterutility.SetDataAndIndexQuota(clusterconfig.Nodes[0], clusterconfig.Username, clusterconfig.Password, "1500", VECTOR_INDEX_INDEXER_QUOTA)
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

		log.Printf("********Create composite indices on scope and collection**********")

		stmt := fmt.Sprintf("CREATE INDEX %v"+
			" ON `%v`.`%v`.`%v`(sift VECTOR)"+
			" PARTITION BY HASH(meta().id)"+
			" WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\", \"num_partition\":3, \"defer_build\":true};",
			idxShared, BUCKET, scope, coll)
		err = createWithDeferAndBuild(idxShared, BUCKET, scope, coll, stmt, defaultIndexActiveTimeout)
		FailTestIfError(err, "Error in creating "+idxShared, t)

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

	// entry cluster config - [0: kv n1ql] [1: index] [2: index]
	// index should be present on Node1
	t.Run("TestTrainingFailNotEnoughDocument", func(subt *testing.T) {
		printClusterConfig(subt.Name(), "entry")

		removeNode(clusterconfig.Nodes[2], subt)
		printClusterConfig(subt.Name(), "after setup")

		// flush all the docs in the bucket
		kvutility.EnableBucketFlush(BUCKET, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
		kvutility.FlushBucket(BUCKET, "", clusterconfig.Username, clusterconfig.Password, kvaddress)

		addNode(clusterconfig.Nodes[3], "index", subt)
		if err := clusterutility.RemoveNode(clusterconfig.KVAddress, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[1]); err != nil {
			subt.Fatalf("%v expected rebalance to pass since index is kept in error state but rebalance fail", subt.Name())
		}

	})
}

func Test_SaveMProf(t *testing.T) {
	skipIfNotPlasma(t)

	runtime.GC()

	var m runtime.MemStats

	runtime.ReadMemStats(&m)

	stats, err := plasma.GetProcessRSS()
	tc.HandleError(err, "Failed to get process stats")
	log.Printf("Memstats - InUse: %v, Alloc: %v, Sys: %v, Idle: %v, Released: %v, GCSys: %v;\n\tRSS: %v",
		m.HeapInuse, m.HeapAlloc, m.Sys, m.HeapIdle, m.HeapReleased, m.GCSys, stats.ProcMemRSS)

	workspace := os.Getenv("WORKSPACE")
	str := strings.ReplaceAll(t.Name(), "/", "_")
	dir := filepath.Join(workspace, "ns_server", "logs", "n_1")
	os.MkdirAll(dir, 0755)
	fileName := filepath.Join(dir, str+"_functionaltests_mprof.log")

	f, err := os.Create(fileName)
	tc.HandleError(err, "Failed to create mprof file")

	err = pprof.WriteHeapProfile(f)
	tc.HandleError(err, "Failed to write mprof file")

	log.Printf("mprof file written to %v", fileName)
	f.Close()

	if stats.ProcMemRSS > 4*1024*1024*1024 {
		fileName = filepath.Join(dir, str+"_heap_dump.log")
		f, err = os.Create(fileName)
		tc.HandleError(err, "Failed to create heap dump file")

		// costly af - will write all heap info to file
		debug.WriteHeapDump(f.Fd())

		log.Printf("heap dump file written to %v", fileName)
		f.Close()
	}
}

func TestVectorIndexShardRebalance(t *testing.T) {
	skipIfNotPlasma(t)

	closeCh := make(chan struct{})

	// Setup signal handler for graceful shutdown
	var signalCh = make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM, syscall.SIGABRT, syscall.SIGKILL)

	defer close(closeCh)

	go func() {
		select {
		case sig := <-signalCh:
			log.Printf("Received signal: %v. Initiating graceful shutdown...", sig)
			Test_SaveMProf(t)

			os.Exit(0)
		case <-closeCh:
			log.Printf("Received close signal. Initiating graceful shutdown...")
		}
	}()

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

		indexStatusResp := performCodebookTransferValidation(subt, []string{idxDedicated, idxShared})
		performStagingCleanupValidation(subt)
		validateVectorScan(subt, indexStatusResp)
	})

	t.Run("TestIndexNodeRebalanceOut", func(subt *testing.T) {
		TestIndexNodeRebalanceOut(subt)

		indexStatusResp := performCodebookTransferValidation(subt, []string{idxDedicated, idxShared})
		performStagingCleanupValidation(subt)
		validateVectorScan(subt, indexStatusResp)
	})

	t.Run("TestFailoverAndRebalance", func(subt *testing.T) {
		TestFailoverAndRebalance(subt)

		indexStatusResp := performCodebookTransferValidation(subt, []string{idxDedicated, idxShared})
		performStagingCleanupValidation(subt)
		validateVectorScan(subt, indexStatusResp)
	})

	t.Run("TestSwapRebalance", func(subt *testing.T) {
		TestSwapRebalance(subt)

		indexStatusResp := performCodebookTransferValidation(subt, []string{idxDedicated, idxShared})
		performStagingCleanupValidation(subt)
		validateVectorScan(subt, indexStatusResp)
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
		stmt := fmt.Sprintf("CREATE INDEX %v"+
			" ON `%v`(sift VECTOR)"+
			" WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\", \"num_replica\":1, \"defer_build\":true};",
			idxDedicatedReplica, BUCKET)
		err := createWithDeferAndBuild(idxDedicatedReplica, BUCKET, "", "", stmt, defaultIndexActiveTimeout)
		FailTestIfError(err, "Error in creating "+idxDedicatedReplica, subt)

		startTime := time.Now()
		stmt = fmt.Sprintf("CREATE INDEX %v"+
			" ON `%v`.`%v`.`%v`(sift VECTOR)"+
			" WITH { \"dimension\":128, \"description\": \"IVF256,PQ32x8\", \"similarity\":\"L2_SQUARED\", \"num_replica\":1, \"defer_build\":true};",
			idxSharedReplica, BUCKET, scope, coll)
		err = createWithDeferAndBuild(idxSharedReplica, BUCKET, scope, coll, stmt, defaultIndexActiveTimeout)
		FailTestIfError(err, "Error in creating "+idxSharedReplica, subt)
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

		indexStatusResp := performCodebookTransferValidation(subt, []string{idxDedicated, idxShared})
		validateVectorScan(subt, indexStatusResp)
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

	indexStatusResp := performCodebookTransferValidation(t, []string{idxDedicated, idxShared})
	validateVectorScan(t, indexStatusResp)

	time.Sleep(10 * time.Second)
}

func validateVectorScan(t *testing.T, statuses *tc.IndexStatusResponse) {

	dedicatedNames := []string{idxDedicatedReplica, idxDedicated}
	sharedNames := []string{idxSharedReplica, idxShared}

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

func performCodebookTransferValidation(subt *testing.T, idxNames []string) *tc.IndexStatusResponse {

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
				cvsSlicePath, err1 := tc.GetIndexSlicePath(status.Name, status.Bucket, storageDirMap[indexerAddr], c.PartitionId(partnId))
				bhiveDir := filepath.Join(storageDirMap[indexerAddr], c.BHIVE_DIR_PREFIX)
				bhiveSlicePath, err2 := tc.GetIndexSlicePath(status.Name, status.Bucket, bhiveDir, c.PartitionId(partnId))
				if err1 != nil && err2 != nil {
					FailTestIfError(err1, "Error while GetIndexSlicePath", subt)
					FailTestIfError(err2, "Error while GetIndexSlicePath", subt)
				} else if len(cvsSlicePath) == 0 && len(bhiveSlicePath) == 0 {
					FailTestIfError(fmt.Errorf("slice path empty for all storages"), "Error while GetIndexSlicePath", subt)
				}
				slicePath := cvsSlicePath
				if len(cvsSlicePath) == 0 {
					slicePath = bhiveSlicePath
				}

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
	return statuses
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
		if exist, err := verifyPathExists(codebookStaging); err != nil {
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
