package functionaltests

import (
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	json "github.com/couchbase/indexing/secondary/common/json"
	"github.com/couchbase/indexing/secondary/manager"
	mc "github.com/couchbase/indexing/secondary/manager/common"
	"github.com/couchbase/indexing/secondary/testcode"
	"github.com/couchbase/indexing/secondary/tests/framework/clusterutility"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
)

func getIndexStatusFromIndexer() (*tc.IndexStatusResponse, error) {
	url, err := makeurl("/getIndexStatus?useETag=false")
	if err != nil {
		return nil, err
	}

	var resp *http.Response
	resp, err = http.Get(url)
	if resp != nil {
		defer resp.Body.Close()
	}

	if err != nil {
		return nil, err
	}

	var respbody []byte
	respbody, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var st tc.IndexStatusResponse
	err = json.Unmarshal(respbody, &st)
	if err != nil {
		return nil, err
	}

	return &st, nil
}

func getShardGroupingFromLiveCluster() (tc.AlternateShardMap, *tc.IndexStatusResponse, error) {
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
		return nil, nil, err
	}

	shardGrouping := make(tc.AlternateShardMap)
	for _, status := range statuses.Status {
		var replicaMap map[int]map[c.PartitionId][]string
		var partnMap map[c.PartitionId][]string

		var ok bool

		if status.Scope == "_system" && status.Collection == "_query" {
			continue
		}

		if defnStruct, ok := shardGrouping[status.DefnId]; !ok {
			replicaMap = make(map[int]map[c.PartitionId][]string)
			shardGrouping[status.DefnId] = &struct {
				Name         string
				NumReplica   int
				NumPartition int
				IsPrimary    bool
				Status       string
				ReplicaMap   map[int]map[c.PartitionId][]string
			}{
				Name:         status.Name,
				NumReplica:   status.NumReplica,
				NumPartition: status.NumPartition,
				IsPrimary:    status.IsPrimary,
				ReplicaMap:   replicaMap,
				Status:       status.Status,
			}
		} else {
			replicaMap = defnStruct.ReplicaMap
		}

		if partnMap, ok = replicaMap[status.ReplicaId]; !ok {
			partnMap = make(map[c.PartitionId][]string)
			replicaMap[status.ReplicaId] = partnMap
		}

		for _, partShardMap := range status.AlternateShardIds {
			for partnId, shards := range partShardMap {
				partnMap[c.PartitionId(partnId)] = append(partnMap[c.PartitionId(partnId)], shards...)
			}
		}

	}

	return shardGrouping, statuses, nil
}

func getIndexerStorageDirForNode(nodeAdd string, t *testing.T) string {
	host, errHosts := secondaryindex.GetIndexerNodesHttpAddressForNode(nodeAdd)
	FailTestIfError(errHosts, "Error in GetIndexerNodesHttpAddressForNode", t)

	if len(host) == 0 {
		// Just return from here, don't fail the test
		log.Printf("%v::getIndexerStorageDirForNode: Failed to get indexer for %v", t.Name(), nodeAdd)
		return ""
	}

	indexStorageDir, errGetSetting := tc.GetIndexerSetting(host, "indexer.storage_dir",
		clusterconfig.Username, clusterconfig.Password)
	FailTestIfError(errGetSetting, "Error in GetIndexerSetting", t)

	strIndexStorageDir := fmt.Sprintf("%v", indexStorageDir)
	absIndexStorageDir, err1 := filepath.Abs(strIndexStorageDir)
	FailTestIfError(err1, "Error while finding absolute path", t)

	exists, _ := verifyPathExists(absIndexStorageDir)

	if !exists {
		// Just return from here, don't fail the test
		log.Printf("Skipping TestOrphanIndexCleanup as indexStorageDir %v does not exists\n",
			indexStorageDir)
		return ""
	}

	return absIndexStorageDir
}

func prettyErrors(errs []string) string {
	var prettyErrs strings.Builder
	for _, err := range errs {
		prettyErrs.WriteString(fmt.Sprintf("*\t%v\n", err))
	}
	return prettyErrs.String()
}

func performClusterStateValidation(t *testing.T, negTests bool, validations ...tc.InvalidClusterState) {
	var retryCount = 0

retry:
	shardGrouping, statuses, err := getShardGroupingFromLiveCluster()
	tc.HandleError(err, "Err in getting Index Status from live cluster")
	statusStr, _ := json.MarshalIndent(statuses, "", "  ")

	errMap := tc.ValidateClusterState(shardGrouping, len(validations) != 0)
	errStr := strings.Builder{}
	if len(validations) == 0 && len(errMap) != 0 {
		for violation, errs := range errMap {
			errStr.WriteString(fmt.Sprintf("\t%v violation in live cluster: \n%v", violation, prettyErrors(errs)))
		}
	} else if len(validations) > 0 && len(errMap) > 0 {
		for _, validation := range validations {
			if errs, ok := errMap[validation]; ok {
				errStr.WriteString(fmt.Sprintf("\t%v violation in live cluster: \n%v", validation, prettyErrors(errs)))
			}
		}
	}
	if errStr.Len() > 0 && !negTests {
		if retryCount < 5 {
			retryCount++
			time.Sleep(time.Duration(100*retryCount) * time.Millisecond)
			goto retry
		}
		t.Fatalf("%v:performClusterStateValidation validations failed - \n%v\nLive cluster state - \n%v",
			t.Name(), errStr.String(), string(statusStr))
	} else if errStr.Len() == 0 && negTests {
		if len(validations) == 0 {
			if retryCount < 5 {
				retryCount++
				time.Sleep(time.Duration(100*retryCount) * time.Millisecond)
				goto retry
			}
			t.Fatalf("%v:performClusterStateValidation expected atleast one validation to fail but none failed. Live cluster state - \n%v",
				t.Name(), string(statusStr))
		} else {
			unfaildValidations := make([]tc.InvalidClusterState, 0, len(errMap))
			for _, toFailValidation := range validations {
				if _, ok := errMap[toFailValidation]; !ok {
					unfaildValidations = append(unfaildValidations, toFailValidation)
					delete(errMap, toFailValidation)
				}
			}
			if len(unfaildValidations) > 0 || len(errMap) > 0 {
				failedValidations := func() []string {
					res := make([]string, 0)
					for err := range errMap {
						res = append(res, err.String())
					}
					return res
				}()
				if retryCount < 5 {
					retryCount++
					time.Sleep(time.Duration(100*retryCount) * time.Millisecond)
					goto retry
				}
				statusStr, _ := json.MarshalIndent(statuses, "", "  ")
				t.Fatalf("%v:performClusterState\n* expected validations(%v) to fail but did not fail\n* expetecd validations (%v) to pass but failed\nLive cluster state - \n%v",
					t.Name(), unfaildValidations, failedValidations, string(statusStr))
			}
		}
	}

	for _, status := range statuses.Status {
		if status.Status == "Active" {
			replicaIds := make([]int, 1, status.NumReplica+1)
			for i := 1; i < status.NumReplica+1; i++ {
				replicaIds = append(replicaIds, i)
			}
			scanIndexReplicas2(
				status.Name,
				status.Bucket, status.Scope, status.Collection,
				replicaIds, 100, -1, status.NumPartition, t)
		}
	}
}

func skipShardAffinityTests(t *testing.T) {
	if clusterconfig.IndexUsing != "plasma" {
		t.Skipf("Shard affinity tests only valid with plasma storage")
		return
	}
}

const SHARD_AFFINITY_INDEXER_QUOTA = "384"

// shouldTestWithShardDealer is a global variable that is used to determine if the shard affinity
// tests should be run with the shard dealer;
// This is used to ensure we have not regressed from the basic 7.6 shard affinity functionality;
var shouldTestWithShardDealer = true

func TestWithShardAffinity(t *testing.T) {
	skipShardAffinityTests(t)

	scope, coll := "s1", "c1"

	t.Run("RebalanceSetupCluster", func(subt *testing.T) {
		TestRebalanceSetupCluster(subt)

		err := clusterutility.SetDataAndIndexQuota(clusterconfig.Nodes[0], clusterconfig.Username, clusterconfig.Password, "1500", SHARD_AFFINITY_INDEXER_QUOTA)
		tc.HandleError(err, "Failed to set memory quota in cluster")

		// wait for indexer to come up as the above step will cause a restart
		secondaryindex.WaitTillAllIndexNodesActive(kvaddress, defaultIndexActiveTimeout)

		configChanges := map[string]interface{}{
			"indexer.settings.enable_shard_affinity": true,
			"indexer.planner.honourNodesInDefn":      true,
		}

		if !shouldTestWithShardDealer {
			configChanges["indexer.planner.use_shard_dealer"] = false
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

	t.Run("TestCreateDocsBeforeRebalance", func(subt *testing.T) {
		TestCreateDocsBeforeRebalance(subt)

		log.Printf("********Create docs on scope and collection**********")
		manifest := kvutility.CreateCollection(BUCKET, scope, coll, clusterconfig.Username, clusterconfig.Password, kvaddress)
		cid := kvutility.GetCollectionID(BUCKET, scope, coll, clusterconfig.Username, clusterconfig.Password, kvaddress)

		kvutility.WaitForCollectionCreation(BUCKET, scope, coll, clusterconfig.Username, clusterconfig.Password, []string{kvaddress}, manifest)
		masterDocs_c1 = CreateDocsForCollection(BUCKET, cid, 2000)
	})

	t.Run("TestCreateIndexesBeforeRebalance", func(subt *testing.T) {
		TestCreateIndexesBeforeRebalance(subt)
	})

	t.Run("TestShardAffinityInInitialCluster", func(subt *testing.T) {
		performClusterStateValidation(subt, false)
	})

	t.Run("TestIndexNodeRebalanceIn", func(subt *testing.T) {
		TestIndexNodeRebalanceIn(subt)

		performClusterStateValidation(subt, false)
	})

	t.Run("TestCreateReplicatedIndexesBeforeRebalance", func(subt *testing.T) {
		TestCreateReplicatedIndexesBeforeRebalance(subt)

		// this is to create shared instances on a shard
		log.Printf("********Create indices on scope and collection**********")
		idx1 := t.Name() + "_age"
		idx1 = strings.ReplaceAll(idx1, "/", "_")
		stmt := fmt.Sprintf("create index %v on %v.%v.%v(%v) with {\"num_replica\": 1}", idx1, BUCKET, scope, coll, "age")
		executeN1qlStmt(stmt, BUCKET, subt.Name(), subt)

		idx2 := t.Name() + "_gender"
		idx2 = strings.ReplaceAll(idx2, "/", "_")
		stmt = fmt.Sprintf("create index %v on %v.%v.%v(%v) with {\"num_replica\": 1}", idx2, BUCKET, scope, coll, "gender")
		executeN1qlStmt(stmt, BUCKET, subt.Name(), subt)

		performClusterStateValidation(subt, false)
	})

	t.Run("TestIndexNodeRebalanceOut", func(subt *testing.T) {
		TestIndexNodeRebalanceOut(subt)

		// cluster will have missing replicas as we have inidces with 3 replicas but only 2 nodes
		performClusterStateValidation(subt, true,
			tc.MISSING_REPLICA_INVALID_CLUSTER_STATE)
	})

	t.Run("TestFailoverAndRebalance", func(subt *testing.T) {
		TestFailoverAndRebalance(subt)

		performClusterStateValidation(subt, true,
			tc.MISSING_REPLICA_INVALID_CLUSTER_STATE)
	})

	t.Run("TestSwapRebalance", func(subt *testing.T) {
		TestSwapRebalance(subt)

		performClusterStateValidation(subt, true,
			tc.MISSING_REPLICA_INVALID_CLUSTER_STATE)
	})

	t.Run("TestRebalanceReplicaRepair", func(subt *testing.T) {
		TestRebalanceReplicaRepair(subt)

		performClusterStateValidation(subt, false)
	})

	t.Run("TestCorruptIndexDuringRecovery", func(t *testing.T) {
		// entry and exit config -
		// [0: kv n1ql] [1: index] [2: index] [3: index]

		status := getClusterStatus()
		if len(status) != 4 || !isNodeIndex(status, clusterconfig.Nodes[1]) ||
			!isNodeIndex(status, clusterconfig.Nodes[2]) || !isNodeIndex(status, clusterconfig.Nodes[3]) {
			t.Fatalf("%v Unexpected cluster configuration: %v", t.Name(), status)
		}

		printClusterConfig(t.Name(), "entry")

		log.Printf("********Updating `indexer.shardRebalance.corruptIndexOnRecovery`=true**********")

		configChanges := map[string]interface{}{
			"indexer.shardRebalance.corruptIndexOnRecovery": true,
		}
		err := secondaryindex.ChangeMultipleIndexerSettings(configChanges, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[1])
		tc.HandleError(err, fmt.Sprintf("Failed to change config %v", configChanges))

		defer func() {
			configChanges := map[string]interface{}{
				"indexer.shardRebalance.corruptIndexOnRecovery": false,
			}
			err := secondaryindex.ChangeMultipleIndexerSettings(configChanges, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[1])
			tc.HandleError(err, fmt.Sprintf("Failed to change config %v", configChanges))
		}()

		if err := clusterutility.RemoveNode(kvaddress, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[2]); err == nil {
			t.Fatalf("%v expected rebalance to fail due to corrupt shards on recovery but rebalance completed successfully", t.Name())
		}

		performClusterStateValidation(t, false)

		log.Printf("********Test for corrupt data backups**********")
		// verify corrupt index dir exists on n3 and n1
		storageDirs := []string{
			getIndexerStorageDirForNode(clusterconfig.Nodes[1], t),
			getIndexerStorageDirForNode(clusterconfig.Nodes[3], t),
		}
		corruptDirs := make([]string, 0, len(storageDirs))

		paths := strings.Builder{}

		files := make([]fs.DirEntry, 0)
		for _, storageDir := range storageDirs {
			corruptDir := filepath.Join(storageDir, CORRUPT_DATA_SUBDIR)
			corruptDirs = append(corruptDirs, corruptDir)

			fileObjs, err := os.ReadDir(corruptDir)
			if err != nil {
				t.Logf("WARN failed to read corrupt dir %v with err %v", corruptDir, err)
				continue
			}
			files = append(files, fileObjs...)

			for _, i := range fileObjs {
				paths.WriteString(fmt.Sprintf("\t->%v\n", i.Name()))
				if i.IsDir() {
					if strings.Contains(i.Name(), "shards") {
						shards, _ := os.ReadDir(filepath.Join(corruptDir, i.Name()))
						for _, j := range shards {
							paths.WriteString(fmt.Sprintf("\t\t->%v\n", j.Name()))
						}
					}
				}
			}
		}

		if len(files) == 0 {
			t.Fatalf("%v expected corrupt data to be backed up but none were backed in indexer dir %v",
				t.Name(), corruptDirs)
		} else {
			log.Printf("Backed up shards/indices at %v\n%v", corruptDirs, paths.String())
		}

		waitForRebalanceCleanup()
	})

	t.Run("TestFailureAndRebalanceDuringInitialIndexBuild", func(subt *testing.T) {
		TestFailureAndRebalanceDuringInitialIndexBuild(subt)

		performClusterStateValidation(subt, false)
	})

	t.Run("TestRedistributWhenNodeIsAddedForFalse", func(subt *testing.T) {
		TestRedistributeWhenNodeIsAddedForFalse(subt)

		performClusterStateValidation(subt, false)
	})

	t.Run("TestRedistributeWhenNodeInAddedForTrue", func(subt *testing.T) {
		TestRedistributeWhenNodeIsAddedForTrue(subt)

		performClusterStateValidation(subt, false)
	})

	// entry cluster config - [0: kv n1ql] [1: index] [2: index]
	// exit cluster config - [0: kv n1ql] [1: index] [2: index] [3: index]
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

		log.Print("** Starting Shard Rebalance (node n2 <=> n3)")
		swapRebalance(subt, 3, 2)

		report, err := getLastRebalanceReport(kvaddress, clusterconfig.Username,
			clusterconfig.Password)
		tc.HandleError(err, "Failed to get last rebalance report")
		if completionMsg, exists := report["completionMessage"]; exists &&
			!strings.Contains(completionMsg.(string), "stopped by user") {
			subt.Fatalf("Expected rebalance to be cancelled but it did not cancel. Report - %v",
				report)
		} else if !exists {
			subt.Fatalf("Rebalance report does not have any completion message - %v",
				report)
		}

		waitForRebalanceCleanup()

		performClusterStateValidation(subt, false)
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

		log.Print("** Starting Shard Rebalance (node n2 <=> n3)")
		swapRebalance(subt, 3, 2)

		report, err := getLastRebalanceReport(kvaddress, clusterconfig.Username,
			clusterconfig.Password)
		tc.HandleError(err, "Failed to get last rebalance report")
		if completionMsg, exists := report["completionMessage"]; exists &&
			!strings.Contains(completionMsg.(string), "stopped by user") {
			subt.Fatalf("Expected rebalance to be cancelled but it did not cancel. Report - %v",
				report)
		} else if !exists {
			subt.Fatalf("Rebalance report does not have any completion message - %v",
				report)
		}

		waitForRebalanceCleanup()

		performClusterStateValidation(subt, false)
	})

	// entry config - [0: kv n1ql] [1: index] [2: index]
	// exit config - [0: kv n1ql] [1: index]            [3: index]
	t.Run("TestShardRebalanceWithCreateCommandToken", func(subt *testing.T) {
		TestRebalanceWithCreateCommandToken(subt)

		performClusterStateValidation(subt, false)
	})

	t.Run("TestResetMetakvActions", func(subt *testing.T) {
		subt.Log("In TestResetMetakvActions")

		tc.HandleError(testcode.ResetMetaKV(), "Failed to reset metakv testactions")
	})
}

// In an existing cluster with indices, we enable the shard affinity feature
// then swap rebalance all nodes; after the last rebalance, all indices should have
// alternate shard ids assigned to them
func TestRebalancePseudoOfflineUgradeWithShardAffinity(t *testing.T) {
	skipShardAffinityTests(t)

	log.Println("*********Setup cluster*********")
	setupCluster(t)
	var err error

	err = clusterutility.SetDataAndIndexQuota(clusterconfig.Nodes[0], clusterconfig.Username, clusterconfig.Password, "1500", SHARD_AFFINITY_INDEXER_QUOTA)
	tc.HandleError(err, "Failed to set memory quota in cluster")
	// wait for indexer to come up as the above step will cause a restart
	secondaryindex.WaitTillAllIndexNodesActive(kvaddress, defaultIndexActiveTimeout)

	err = secondaryindex.WaitForSystemIndices(kvaddress, 0)
	tc.HandleError(err, "Waiting for indices in system scope")

	addNodeAndRebalance(clusterconfig.Nodes[2], "index", t)
	addNodeAndRebalance(clusterconfig.Nodes[3], "index", t)

	status := getClusterStatus()
	if len(status) != 4 || !isNodeIndex(status, clusterconfig.Nodes[1]) ||
		!isNodeIndex(status, clusterconfig.Nodes[2]) ||
		!isNodeIndex(status, clusterconfig.Nodes[3]) {
		t.Fatalf("%v Unexpected cluster configuration: %v", t.Name(), status)
	}

	printClusterConfig(t.Name(), "entry")

	log.Printf("********Create Docs and Indices**********")
	err = secondaryindex.DropAllNonSystemIndexes(clusterconfig.Nodes[1])
	tc.HandleError(err, "Failed to drop all indices")

	numDocs := 1000
	CreateDocs(numDocs)

	var indexPrefix = strings.ReplaceAll(t.Name(), "/", "_")

	// create primary index
	indexName := indexPrefix + "_idx_primary"
	n1qlStmt := fmt.Sprintf("create primary index %v on `%v`", indexName, BUCKET)
	executeN1qlStmt(n1qlStmt, BUCKET, t.Name(), t)
	log.Printf("%v %v index is now active.", t.Name(), indexName)

	indices := []string{}
	// create deffered indices
	for field1, fieldName1 := range fieldNames {
		fieldName2 := fieldNames[(field1+1)%len(fieldNames)]
		indexName := indexPrefix + "_DFRD_" + fieldName1 + "_" + fieldName2
		n1qlStmt := fmt.Sprintf("create index %v on `%v`(%v, %v) with {\"defer_build\":true}",
			indexName, BUCKET, fieldName1, fieldName2)

		executeN1qlStmt(n1qlStmt, BUCKET, t.Name(), t)
		indices = append(indices, indexName)
	}
	log.Printf("%v %v indices are now created with defer build", t.Name(), indices)

	indices = []string{}
	// create non-deffered partitioned indices
	for field1 := 0; field1 < 2; field1++ {
		fieldName1 := fieldNames[field1%len(fieldNames)]
		fieldName2 := fieldNames[(field1+4)%len(fieldNames)]
		indexName := indexPrefix + "_5PTN_1RP_" + fieldName1 + "_" + fieldName2
		n1qlStmt := fmt.Sprintf(
			"create index %v on `%v`(%v, %v) partition by hash(Meta().id) with {\"num_partition\":5, \"num_replica\":1}",
			indexName, BUCKET, fieldName1, fieldName2)
		executeN1qlStmt(n1qlStmt, BUCKET, t.Name(), t)
		indices = append(indices, indexName)
	}
	log.Printf("%v %v indices are now active.", t.Name(), indices)

	// this validation is supposed to fail as our validations are driven by alternate shard ids
	performClusterStateValidation(t, true)

	log.Printf("********Updating `indexer.settings.enable_shard_affinity`=true**********")

	var configChanges = map[string]interface{}{
		"indexer.settings.enable_shard_affinity": true,
	}
	if !shouldTestWithShardDealer {
		configChanges["indexer.planner.use_shard_dealer"] = false
	}

	// config - [0: kv n1ql] [1: index] - old [2: index] - old [3: index]
	err = secondaryindex.ChangeMultipleIndexerSettings(configChanges, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[1])
	tc.HandleError(err, "Unable to change indexer setting `indexer.settings.enable_shard_affinity`")

	defer func() {
		err := secondaryindex.ChangeIndexerSettings("indexer.settings.enable_shard_affinity", false, clusterconfig.Username, clusterconfig.Password, kvaddress)
		tc.HandleError(err, "Unable to change indexer setting `indexer.settings.enable_shard_affinity`")
	}()

	log.Printf("********Swap rebalance all nodes**********")

	swapRebalance(t, 4, 3) // config - [0: kv n1ql] [1: index] [2: index] .......... [4: index] - movements via DCP
	swapRebalance(t, 3, 2) // config - [0: kv n1ql] [1: index] .......... [3: index] [4: index] - movements via DCP
	swapRebalance(t, 2, 1) // config - [0: kv n1ql] .......... [2: index] [3: index] [4: index] - movements via DCP

	log.Printf("%v all nodes swap rebalanced. All indices should have under gone movement and we should have shard affinity in cluster. Validating the same...",
		t.Name())
	performClusterStateValidation(t, false)

	log.Printf("%v swap rebalancing a node to test shard rebalance and validate cluster affinity..",
		t.Name())

	swapRebalance(t, 1, 4) // config - [0: kv n1ql] [1: index] [2: index] [3: index] - movements via shard
	performClusterStateValidation(t, false)

	printClusterConfig(t.Name(), "exit")
}

// expected cluster state in entry
// - [0: kv n1ql] [1: index] [2: index] [3: index]
func TestCreateInSimulatedMixedMode(t *testing.T) {
	skipShardAffinityTests(t)

	status := getClusterStatus()
	if len(status) != 4 || !isNodeIndex(status, clusterconfig.Nodes[1]) ||
		!isNodeIndex(status, clusterconfig.Nodes[2]) ||
		!isNodeIndex(status, clusterconfig.Nodes[3]) {
		t.Fatalf("%v Unexpected cluster configuration: %v", t.Name(), status)
	}

	var indexPrefix = strings.ReplaceAll(t.Name(), "/", "_")

	log.Println("*********Setup cluster*********")
	err := secondaryindex.DropAllNonSystemIndexes(clusterconfig.Nodes[1])
	tc.HandleError(err, "failed to drop all secondary indices")

	// config - [0: kv n1ql] [1: index] [2: index] [3: index]
	printClusterConfig(t.Name(), "entry")

	log.Printf("********Updating `indexer.settings.enable_shard_affinity`=true**********")

	configChanges := map[string]interface{}{
		"indexer.settings.enable_shard_affinity":       true,
		"indexer.planner.honourNodesInDefn":            true,
		"indexer.thisNodeOnly.ignoreAlternateShardIds": true,
	}
	err = secondaryindex.ChangeMultipleIndexerSettings(configChanges, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[1])
	tc.HandleError(err, fmt.Sprintf("Failed to change config %v", configChanges))

	defer func() {
		configChanges := map[string]interface{}{
			"indexer.settings.enable_shard_affinity":       false,
			"indexer.planner.honourNodesInDefn":            false,
			"indexer.thisNodeOnly.ignoreAlternateShardIds": false,
		}
		err := secondaryindex.ChangeMultipleIndexerSettings(configChanges, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[1])
		tc.HandleError(err, fmt.Sprintf("Failed to change config %v", configChanges))
	}()

	log.Printf("********Create indices**********")
	indices := []string{}
	// create non-deffered partitioned indices
	for field1 := 0; field1 < 2; field1++ {
		fieldName1 := fieldNames[field1%len(fieldNames)]
		fieldName2 := fieldNames[(field1+4)%len(fieldNames)]
		indexName := indexPrefix + "_5PTN_1RP_" + fieldName1 + "_" + fieldName2
		n1qlStmt := fmt.Sprintf(
			"create index %v on `%v`(%v, %v) partition by hash(Meta().id) with {\"num_partition\":5, \"num_replica\":1}",
			indexName, BUCKET, fieldName1, fieldName2)
		executeN1qlStmt(n1qlStmt, BUCKET, t.Name(), t)
		indices = append(indices, indexName)
	}
	log.Printf("%v %v indices are now active.", t.Name(), indices)

	// cluster validations are expected to fail as a node does not have alternate shard ids
	performClusterStateValidation(t, true)
}

// expected cluster state in entry
// [0: kv n1ql] [1: index] [2: index] [3: index]
// indices on node[1] do not have Alternate Shard IDs
func TestSwapRebalanceMixedMode(t *testing.T) {
	skipShardAffinityTests(t)

	status := getClusterStatus()
	if len(status) != 4 || !isNodeIndex(status, clusterconfig.Nodes[1]) ||
		!isNodeIndex(status, clusterconfig.Nodes[2]) ||
		!isNodeIndex(status, clusterconfig.Nodes[3]) {
		t.Fatalf("%v Unexpected cluster configuration: %v", t.Name(), status)
	}

	log.Printf("********Updating `indexer.settings.enable_shard_affinity`=true**********")

	configChanges := map[string]interface{}{
		"indexer.settings.enable_shard_affinity":       true,
		"indexer.planner.honourNodesInDefn":            true,
		"indexer.thisNodeOnly.ignoreAlternateShardIds": false,
	}
	if !shouldTestWithShardDealer {
		configChanges["indexer.planner.use_shard_dealer"] = false
	}

	err := secondaryindex.ChangeMultipleIndexerSettings(configChanges, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[1])
	tc.HandleError(err, fmt.Sprintf("Failed to change config %v", configChanges))

	defer func() {
		configChanges := map[string]interface{}{
			"indexer.settings.enable_shard_affinity":       false,
			"indexer.planner.honourNodesInDefn":            false,
			"indexer.thisNodeOnly.ignoreAlternateShardIds": false, // may not be necessary as node was removed from cluster
		}
		err := secondaryindex.ChangeMultipleIndexerSettings(configChanges, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[1])
		tc.HandleError(err, fmt.Sprintf("Failed to change config %v", configChanges))
	}()

	swapRebalance(t, 4, 3) // config - [0: kv n1ql] [1: index] [2: index] .......... [4: index] - swap rebalance via Shard
	// cluster state still not valid wrt Alternate Shard IDs
	performClusterStateValidation(t, true)

	swapRebalance(t, 3, 2) // config - [0: kv n1ql] [1: index] .......... [3: index] [4: index] - swap rebalance via Shard
	performClusterStateValidation(t, true)

	swapRebalance(t, 2, 1) // config - [0: kv n1ql]            [2: index] [3: index] [4: index] - swap rebalance via DCP
	performClusterStateValidation(t, false)

	// indexer.thisNodeOnly.ignoreAlternateShardIds no longer valid for node[1]
	swapRebalance(t, 1, 4) // config - [0: kv n1ql] [1: index] [2: index] [3: index] - swap rebalance via Shard
	performClusterStateValidation(t, false)
}

// expected cluster state in entry
// [0: kv n1ql] [1: index] [2: index] [3: index]
func TestFailoverAndRebalanceMixedMode(t *testing.T) {
	skipShardAffinityTests(t)

	status := getClusterStatus()
	if len(status) != 4 || !isNodeIndex(status, clusterconfig.Nodes[1]) ||
		!isNodeIndex(status, clusterconfig.Nodes[2]) ||
		!isNodeIndex(status, clusterconfig.Nodes[3]) {
		t.Fatalf("%v Unexpected cluster configuration: %v", t.Name(), status)
	}

	log.Println("*********Setup cluster*********")
	err := secondaryindex.DropAllNonSystemIndexes(clusterconfig.Nodes[1])
	tc.HandleError(err, "failed to drop all secondary indices")

	addNodeAndRebalance(clusterconfig.Nodes[4], "index", t)
	status = getClusterStatus()
	if len(status) != 5 || !isNodeIndex(status, clusterconfig.Nodes[1]) ||
		!isNodeIndex(status, clusterconfig.Nodes[2]) ||
		!isNodeIndex(status, clusterconfig.Nodes[3]) ||
		!isNodeIndex(status, clusterconfig.Nodes[4]) {
		t.Fatalf("%v Unexpected cluster configuration: %v", t.Name(), status)
	}

	// config - [0: kv n1ql] [1: index] [2: index] [3: index] [4: index]
	printClusterConfig(t.Name(), "entry")

	log.Printf("********Updating `indexer.settings.enable_shard_affinity`=true**********")

	configChanges := map[string]interface{}{
		"indexer.settings.enable_shard_affinity":       true,
		"indexer.planner.honourNodesInDefn":            true,
		"indexer.thisNodeOnly.ignoreAlternateShardIds": true,
	}
	if !shouldTestWithShardDealer {
		configChanges["indexer.planner.use_shard_dealer"] = false
	}

	err = secondaryindex.ChangeMultipleIndexerSettings(configChanges, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[4])
	tc.HandleError(err, fmt.Sprintf("Failed to change config %v", configChanges))

	defer func() {
		configChanges := map[string]interface{}{
			"indexer.planner.honourNodesInDefn": false,
		}
		err := secondaryindex.ChangeMultipleIndexerSettings(configChanges, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[1])
		tc.HandleError(err, fmt.Sprintf("Failed to change config %v", configChanges))
	}()

	var indexPrefix = strings.ReplaceAll(t.Name(), "/", "_")

	log.Printf("********Create indices with nodes clause**********")
	indices := []string{}
	// create non-deffered partitioned indices
	for field1 := 0; field1 < 2; field1++ {
		fieldName1 := fieldNames[field1%len(fieldNames)]
		fieldName2 := fieldNames[(field1+4)%len(fieldNames)]
		indexName := indexPrefix + "_5PTN_1RP_" + fieldName1 + "_" + fieldName2
		n1qlStmt := fmt.Sprintf(
			"create index %v on `%v`(%v, %v) partition by hash(Meta().id) with {\"num_partition\":5, \"num_replica\":1, \"nodes\": [\"%v\", \"%v\"]}",
			indexName, BUCKET, fieldName1, fieldName2, clusterconfig.Nodes[4], clusterconfig.Nodes[randomNum(1, 4)])
		executeN1qlStmt(n1qlStmt, BUCKET, t.Name(), t)
		indices = append(indices, indexName)
	}
	log.Printf("%v %v indices are now active.", t.Name(), indices)

	// cluster state still not valid wrt Alternate Shard IDs
	performClusterStateValidation(t, true)

	err = secondaryindex.ChangeMultipleIndexerSettings(map[string]interface{}{"indexer.thisNodeOnly.ignoreAlternateShardIds": false}, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[4])
	tc.HandleError(err, fmt.Sprintf("failed to reset `indexer.thisNodeOnly.ignoreAlternateShardIds` on %v", clusterconfig.Nodes[4]))

	failoverNode(clusterconfig.Nodes[2], t)
	rebalance(t) // config - [0: kv n1ql] [1: index]            [3: index] [4: index] - replica/partn repair needs to happen
	// cluster state will still not be valid as some indices on node 3 will not have Alternate Shard IDs
	performClusterStateValidation(t, true)

	swapRebalance(t, 2, 4) // config - [0: kv n1ql] [1: index] [2: index] [3: index] - shard + DCP rebalance
	performClusterStateValidation(t, false)
}

// expected cluster state in entry
// [0: kv n1ql] [1: index] [2: index] [3: index]
// exit cluster config
// [0: kv n1ql] [1: index]            [3: index] [4: index]
func TestRebalanceOutNewerNodeInMixedMode(t *testing.T) {
	// t.Skipf("Unstable test")
	skipShardAffinityTests(t)

	status := getClusterStatus()
	if len(status) != 4 || !isNodeIndex(status, clusterconfig.Nodes[1]) ||
		!isNodeIndex(status, clusterconfig.Nodes[2]) ||
		!isNodeIndex(status, clusterconfig.Nodes[3]) {
		t.Fatalf("%v Unexpected cluster configuration: %v", t.Name(), status)
	}

	log.Println("*********Setup cluster*********")
	err := secondaryindex.DropAllNonSystemIndexes(clusterconfig.Nodes[1])
	tc.HandleError(err, "failed to drop all secondary indices")

	addNodeAndRebalance(clusterconfig.Nodes[4], "index", t)

	err = secondaryindex.WaitForSystemIndices(kvaddress, 0)
	tc.HandleError(err, "Waiting for indices in system scope")

	status = getClusterStatus()
	if len(status) != 5 || !isNodeIndex(status, clusterconfig.Nodes[1]) ||
		!isNodeIndex(status, clusterconfig.Nodes[2]) || !isNodeIndex(status, clusterconfig.Nodes[3]) ||
		!isNodeIndex(status, clusterconfig.Nodes[4]) {
		t.Fatalf("%v Unexpected cluster configuration: %v", t.Name(), status)
	}

	// config - [0: kv n1ql] [1: index] [2: index] [3: index] [4: index]
	printClusterConfig(t.Name(), "entry")

	log.Printf("********Updating `indexer.settings.enable_shard_affinity`=true**********")

	configChanges := map[string]interface{}{
		"indexer.settings.enable_shard_affinity":       true,
		"indexer.planner.honourNodesInDefn":            true,
		"indexer.thisNodeOnly.ignoreAlternateShardIds": true,
	}
	if !shouldTestWithShardDealer {
		configChanges["indexer.planner.use_shard_dealer"] = false
	}

	err = secondaryindex.ChangeMultipleIndexerSettings(configChanges, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[4])
	tc.HandleError(err, fmt.Sprintf("Failed to change config %v", configChanges))

	defer func() {
		configChanges := map[string]interface{}{
			"indexer.settings.enable_shard_affinity": false,
			"indexer.planner.honourNodesInDefn":      false,
		}
		err := secondaryindex.ChangeMultipleIndexerSettings(configChanges, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[4])
		tc.HandleError(err, fmt.Sprintf("Failed to change config %v", configChanges))
	}()

	var indexPrefix = strings.ReplaceAll(t.Name(), "/", "_")

	log.Printf("********Create indices with nodes clause**********")
	indices := []string{}
	// create non-deffered partitioned indices
	for field1 := 0; field1 < 2; field1++ {
		fieldName1 := fieldNames[field1%len(fieldNames)]
		fieldName2 := fieldNames[(field1+4)%len(fieldNames)]
		indexName := indexPrefix + "_5PTN_1RP_" + fieldName1 + "_" + fieldName2
		n1qlStmt := fmt.Sprintf(
			"create index %v on `%v`(%v, %v) partition by hash(Meta().id) with {\"num_partition\":5, \"num_replica\":1, \"nodes\": [\"%v\", \"%v\"]}",
			indexName, BUCKET, fieldName1, fieldName2, clusterconfig.Nodes[4], clusterconfig.Nodes[randomNum(1, 4)])
		executeN1qlStmt(n1qlStmt, BUCKET, t.Name(), t)
		indices = append(indices, indexName)
	}
	log.Printf("%v %v indices are now active.", t.Name(), indices)

	// cluster state still not valid wrt Alternate Shard IDs
	performClusterStateValidation(t, true)

	log.Printf("********Remove Node 2(latest node)**********")
	removeNode(clusterconfig.Nodes[2], t)

	log.Printf("********Validating Mixed Mode State**********")
	node1Meta, err := getLocalMetaWithRetry(clusterconfig.Nodes[1])
	tc.HandleError(err, "Failed to getLocalMetadata from node 1")
	node3Meta, err := getLocalMetaWithRetry(clusterconfig.Nodes[3])
	tc.HandleError(err, "Failed to getLocalMetadata from node 3")

	node4Meta, err := getLocalMetaWithRetry(clusterconfig.Nodes[4])
	tc.HandleError(err, "Failed to getLocalMetdata from node 4")

	indicesInCluster := make(map[c.IndexDefnId][]string)

	// node 1 - all indies should have Alternate Shard ID
	for _, defn := range node1Meta.IndexDefinitions {
		if strings.Contains(defn.Scope, "system") {
			continue
		}
		for partn, asis := range defn.AlternateShardIds {
			if defn.IsPrimary && len(asis) != 1 {
				t.Fatalf("%v Expected to have 1 Alternate Shard ID but found %v for index %v partn %v on node %v",
					t.Name(), asis, defn.Name, partn, clusterconfig.Nodes[1])
			} else if !defn.IsPrimary && len(asis) != 2 {
				t.Fatalf("%v Expected to have 2 Alternate Shard ID but found %v for index %v partn %v on node %v",
					t.Name(), asis, defn.Name, partn, clusterconfig.Nodes[1])
			}
		}
		indicesInCluster[defn.DefnId] = append(indicesInCluster[defn.DefnId], defn.Name)
	}

	// node 3 - all indies should have Alternate Shard ID
	for _, defn := range node3Meta.IndexDefinitions {
		if strings.Contains(defn.Scope, "system") {
			continue
		}
		for partn, asis := range defn.AlternateShardIds {
			if defn.IsPrimary && len(asis) != 1 {
				t.Fatalf("%v Expected to have 1 Alternate Shard ID but found %v for index %v partn %v on node %v",
					t.Name(), asis, defn.Name, partn, clusterconfig.Nodes[3])
			} else if !defn.IsPrimary && len(asis) != 2 {
				t.Fatalf("%v Expected to have 2 Alternate Shard ID but found %v for index %v partn %v on node %v",
					t.Name(), asis, defn.Name, partn, clusterconfig.Nodes[3])
			}
		}
		indicesInCluster[defn.DefnId] = append(indicesInCluster[defn.DefnId], defn.Name)
	}

	// node 4 - no indices should have Alternate Shard ID
	for _, defn := range node4Meta.IndexDefinitions {
		if strings.Contains(defn.Scope, "system") {
			continue
		}
		for partn, asis := range defn.AlternateShardIds {
			if len(asis) > 0 {
				t.Fatalf("%v index %v partn %v should not have any Alternate Shard ID but found %v",
					t.Name(), defn.Name, partn, asis)
			}
		}
		indicesInCluster[defn.DefnId] = append(indicesInCluster[defn.DefnId], defn.Name)
	}

	if len(indicesInCluster) != len(indices) {
		t.Fatalf("Expected %v indices to be in cluster but found %v", indices, indicesInCluster)
	}

	printClusterConfig(t.Name(), "exit")
}

// cluster in mixed mode; node 1,3 - new node, node 4 - old node
// drop indices on all nodes for replica repair
// add node 2 (new node) and run rebalance
// entry cluster config -
// [0: kv n1ql] [1: index]            [3: index] [4: index]
// cluster in mixed mode
// exit cluster config -
// [0: kv n1ql] [1: index] [2: index] [3: index]
func TestReplicaRepairInMixedModeRebalance(t *testing.T) {
	// t.Skipf("Disabled until MB-60242 is fixed")
	skipShardAffinityTests(t)

	var err error
	// resetCluster(t)
	// addNodeAndRebalance(clusterconfig.Nodes[3], "index", t)
	// err = clusterutility.SetDataAndIndexQuota(clusterconfig.Nodes[0], clusterconfig.Username, clusterconfig.Password, "1500", SHARD_AFFINITY_INDEXER_QUOTA)
	// tc.HandleError(err, "Failed to set memory quota in cluster")

	status := getClusterStatus()
	if len(status) != 4 || !isNodeIndex(status, clusterconfig.Nodes[1]) ||
		!isNodeIndex(status, clusterconfig.Nodes[3]) ||
		!isNodeIndex(status, clusterconfig.Nodes[4]) {
		t.Fatalf("%v Unexpected cluster configuration: %v", t.Name(), status)
	}

	// config - [0: kv n1ql] [1: index]            [3: index] [4: index]
	printClusterConfig(t.Name(), "entry")

	log.Println("*********Setup cluster*********")
	err = secondaryindex.DropAllNonSystemIndexes(clusterconfig.Nodes[1])
	tc.HandleError(err, "Failed to drop all non-system indices")

	log.Printf("********Updating `indexer.settings.enable_shard_affinity`=true with node 4 in simulated mixed mode**********")
	configChanges := map[string]interface{}{
		"indexer.settings.enable_shard_affinity":          true,
		"indexer.planner.honourNodesInDefn":               true,
		"indexer.thisNodeOnly.ignoreAlternateShardIds":    true,
		"indexer.settings.rebalance.redistribute_indexes": true,
	}
	if !shouldTestWithShardDealer {
		configChanges["indexer.planner.use_shard_dealer"] = false
	}

	err = secondaryindex.ChangeMultipleIndexerSettings(configChanges, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[4])
	tc.HandleError(err, fmt.Sprintf("Failed to change config %v", configChanges))

	defer func() {
		configChanges := map[string]interface{}{
			"indexer.settings.enable_shard_affinity":          false,
			"indexer.planner.honourNodesInDefn":               false,
			"indexer.settings.rebalance.redistribute_indexes": false,
		}
		err := secondaryindex.ChangeMultipleIndexerSettings(configChanges, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[1])
		tc.HandleError(err, fmt.Sprintf("Failed to change config %v", configChanges))
	}()

	var indexPrefix = strings.ReplaceAll(t.Name(), "/", "_")

	log.Printf("********Create indices**********")
	indices := []string{}
	// create non-deffered partitioned indices
	for field1 := 0; field1 < 6; field1++ {
		fieldName1 := fieldNames[field1%len(fieldNames)]
		fieldName2 := fieldNames[(field1+4)%len(fieldNames)]
		indexName := indexPrefix + "_5PTN_1RP_" + fieldName1 + "_" + fieldName2
		n1qlStmt := fmt.Sprintf(
			"create index %v on `%v`(%v, %v) partition by hash(Meta().id) with {\"num_partition\":5, \"num_replica\":1}",
			indexName, BUCKET, fieldName1, fieldName2)
		executeN1qlStmt(n1qlStmt, BUCKET, t.Name(), t)
		indices = append(indices, indexName)
	}
	log.Printf("%v %v indices are now active.", t.Name(), indices)

	performClusterStateValidation(t, true)

	dropIndicesMap := make(map[string]int)

	node1meta, err := getLocalMetaWithRetry(clusterconfig.Nodes[1])
	tc.HandleError(err, "Failed to getLocalMetadata from node 1")

	for _, defn := range node1meta.IndexTopologies[0].Definitions {
		if len(dropIndicesMap) == 3 {
			break
		}
		if _, exists := dropIndicesMap[defn.Name]; !exists {
			if strings.Contains(defn.Name, "#primary") {
				continue
			}
			// pick the replica ID of the first instance
			dropIndicesMap[defn.Name] = int(defn.Instances[0].ReplicaId)
		}
	}

	node4meta, err := getLocalMetaWithRetry(clusterconfig.Nodes[4])
	tc.HandleError(err, "Failed to getLocalMetadata from node 4")

	for _, defn := range node4meta.IndexTopologies[0].Definitions {
		if len(dropIndicesMap) == 6 {
			break
		}
		if _, exists := dropIndicesMap[defn.Name]; !exists {
			if strings.Contains(defn.Name, "#primary") {
				continue
			}
			// pick the replica ID of the first instance
			dropIndicesMap[defn.Name] = int(defn.Instances[0].ReplicaId)
		}
	}

	log.Printf("********Drop replicas on node 1 and 4**********")

	for idxName, replicaId := range dropIndicesMap {
		stmt := fmt.Sprintf("alter index %v on %v with {\"action\": \"drop_replica\", \"replicaId\": %v}",
			idxName, BUCKET, replicaId)
		executeN1qlStmt(stmt, BUCKET, t.Name(), t)
		if waitForReplicaDrop(idxName, fmt.Sprintf("%v:%v:%v", BUCKET, "_default", "_default"), replicaId) ||
			waitForReplicaDrop(idxName, BUCKET, replicaId) {
			t.Fatalf("%v couldn't drop index %v replica %v", t.Name(), idxName, replicaId)
		}
	}

	log.Printf("%v dropped the following index:replica %v", t.Name(), dropIndicesMap)

	performClusterStateValidation(t, true)

	// safety! remove ignoreAlternateShardIds from node 4
	err = secondaryindex.ChangeMultipleIndexerSettings(map[string]interface{}{"indexer.thisNodeOnly.ignoreAlternateShardIds": false}, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[4])
	tc.HandleError(err, fmt.Sprintf("failed to reset `indexer.thisNodeOnly.ignoreAlternateShardIds` on %v", clusterconfig.Nodes[4]))

	log.Printf("********Swap Rebalance node 4 <=> 2**********")

	swapRebalance(t, 2, 4)

	performClusterStateValidation(t, false)
}

func preparePrimaryIndexDefn(defnID c.IndexDefnId,
	name, bucket, bucketUUID, scope, collection string,
	scopeId, collectionId string,
	indexerIDs []string,
	numReplica uint32,
	partitionScheme c.PartitionScheme,
	deferred bool) *c.IndexDefn {
	defn := &c.IndexDefn{
		DefnId:          defnID,
		Name:            name,
		Bucket:          bucket,
		BucketUUID:      bucketUUID,
		Scope:           scope,
		Collection:      collection,
		Versions:        []int{0},
		Using:           "plasma",
		Deferred:        deferred,
		ScopeId:         scopeId,
		CollectionId:    collectionId,
		NumPartitions:   1,
		NumReplica:      numReplica,
		PartitionScheme: partitionScheme,
		ExprType:        c.N1QL,
		IsPrimary:       true,
		Partitions:      []c.PartitionId{0},
		Nodes:           indexerIDs,
	}
	defn.NumReplica2.InitializeCounter(defn.NumReplica)
	return defn
}

// TestShardRebalance_DropDuplicateIndexes - create duplicate indexes on node 1 and node 2.
// swap rebalance node 2 with node 3. rebalance should drop the duplicate indexes on node 2.
func TestShardRebalance_DropDuplicateIndexes(t *testing.T) {
	skipShardAffinityTests(t)

	clearCreateComandTokens := func() {
		err := c.MetakvRecurciveDel(mc.CreateDDLCommandTokenPath)
		tc.HandleError(err, "failed to delete all create command token")
	}

	status := getClusterStatus()
	if len(status) != 4 || !isNodeIndex(status, clusterconfig.Nodes[1]) ||
		!isNodeIndex(status, clusterconfig.Nodes[2]) ||
		!isNodeIndex(status, clusterconfig.Nodes[3]) {
		t.Fatalf("%v Unexpected cluster configuration: %v", t.Name(), status)
	}
	removeNode(clusterconfig.Nodes[3], t)

	var retry = 0

init:

	clearCreateComandTokens()

	setupRetry := func(errMsg string) {
		if retry > 5 {
			t.Fatalf("WARN - %v. failed after %v retries", errMsg, retry)
			return
		}
		log.Printf("WARN - %v. retrying test...", errMsg)
		time.Sleep(10 * time.Second)
		resetCluster(t)
		addNodeAndRebalance(clusterconfig.Nodes[2], "index", t)
		retry++
	}

	status = getClusterStatus()
	if len(status) != 3 || !isNodeIndex(status, clusterconfig.Nodes[1]) ||
		!isNodeIndex(status, clusterconfig.Nodes[2]) {
		setupRetry(fmt.Sprintf("unexpected cluster configuration - %v", status))
		goto init
	}

	// config - [0: kv n1ql] [1: index] [2: index]
	printClusterConfig(t.Name(), "entry")

	log.Println("*********Setup cluster*********")
	err := secondaryindex.DropAllNonSystemIndexes(clusterconfig.Nodes[1])
	tc.HandleError(err, "Failed to drop all non-system indices")

	log.Printf("********Updating `indexer.settings.enable_shard_affinity`=true**********")
	configChanges := map[string]interface{}{
		"indexer.settings.enable_shard_affinity":          true,
		"indexer.planner.honourNodesInDefn":               true,
		"indexer.settings.rebalance.redistribute_indexes": true,
	}
	if !shouldTestWithShardDealer {
		configChanges["indexer.planner.use_shard_dealer"] = false
	}

	err = secondaryindex.ChangeMultipleIndexerSettings(configChanges, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[1])
	tc.HandleError(err, fmt.Sprintf("Failed to change config %v", configChanges))

	defer func() {
		configChanges := map[string]interface{}{
			"indexer.settings.enable_shard_affinity":          false,
			"indexer.planner.honourNodesInDefn":               false,
			"indexer.settings.rebalance.redistribute_indexes": false,
		}
		err := secondaryindex.ChangeMultipleIndexerSettings(configChanges, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[1])
		tc.HandleError(err, fmt.Sprintf("Failed to change config %v", configChanges))
	}()

	log.Printf("********Create indices**********")

	client, err := secondaryindex.GetOrCreateClient(indexManagementAddress, "2itest")
	tc.HandleError(err, "failed to get client")

	bucketUUID, err := c.GetBucketUUID(kvaddress, bucket)
	tc.HandleError(err, "failed to get bucket UUID")
	scopeID, collectionID, err := c.GetScopeAndCollectionID(kvaddress, bucket, "_default", "_default")
	tc.HandleError(err, "failed to get scope and collection ID")

	indexerIDs := []string{}
	nodes, err := secondaryindex.GetIndexerNodes(indexManagementAddress)
	tc.HandleError(err, "failed to get indexer nodes")
	for _, node := range nodes {
		indexerIDs = append(indexerIDs, node.NodeUUID)
	}

	defer clearCreateComandTokens()

	var indexPrefix = strings.ReplaceAll(t.Name(), "/", "_")

	// create equivalent replicated indexes on both nodes
	var defnIDs = make([]uint64, 0, 5)
	var baseDefnID c.IndexDefnId = 100
	for i := 0; i < 3; i++ {
		defns := make(map[c.IndexerId][]c.IndexDefn)
		defnID := baseDefnID
		defn := preparePrimaryIndexDefn(defnID,
			indexPrefix+"_index_"+strconv.Itoa(i),
			bucket, bucketUUID, "_default", "_default",
			scopeID, collectionID, indexerIDs, uint32(len(indexerIDs)-1), c.SINGLE, false)
		for j, indexerID := range indexerIDs {
			repID := (j + 1) % 2
			clone := defn.Clone()
			clone.ReplicaId = repID
			clone.InstId = c.IndexInstId(time.Now().UnixNano())
			clone.Partitions = []c.PartitionId{0}
			clone.AlternateShardIds = map[c.PartitionId][]string{
				c.PartitionId(0): {fmt.Sprintf("1-%v-0", repID)},
			}
			defns[c.IndexerId(indexerID)] = []c.IndexDefn{*clone}
		}
		defnIDs = append(defnIDs, uint64(defnID))
		err = mc.PostCreateCommandToken(defnID, bucketUUID, scopeID, collectionID, 0, defns)
		tc.HandleError(err, "failed to post create command token")

		err = secondaryindex.WaitTillIndexActive(uint64(defnID), client, 60)
		if err != nil {
			errMsg := fmt.Sprintf("failed to wait for index %v to be active. err - %v", defnID, err)
			setupRetry(errMsg)
			goto init
		}
		baseDefnID++
	}

	// create duplicate indexes
	defnID1 := baseDefnID
	defn1 := preparePrimaryIndexDefn(defnID1,
		indexPrefix+"_index",
		bucket, bucketUUID, "_default", "_default",
		scopeID, collectionID, indexerIDs, 0, c.SINGLE, false)
	defn1.Nodes = []string{indexerIDs[0]}
	defn1.AlternateShardIds = map[c.PartitionId][]string{
		c.PartitionId(0): {"2-0-0"},
	}
	defn1.InstId = c.IndexInstId(time.Now().UnixNano())

	defns1 := map[c.IndexerId][]c.IndexDefn{
		c.IndexerId(indexerIDs[0]): {*defn1},
	}
	defnIDs = append(defnIDs, uint64(defnID1))

	baseDefnID++
	defnID2 := baseDefnID
	defn2 := defn1.Clone()
	defn2.DefnId = defnID2
	defn2.Partitions = []c.PartitionId{0}
	defn2.InstId = c.IndexInstId(time.Now().UnixNano())
	defn2.Nodes = []string{indexerIDs[1]}
	defn2.AlternateShardIds = map[c.PartitionId][]string{
		c.PartitionId(0): {"1-0-0"},
	}
	defns2 := map[c.IndexerId][]c.IndexDefn{
		c.IndexerId(indexerIDs[1]): {*defn2},
	}
	defnIDs = append(defnIDs, uint64(defnID2))

	err = mc.PostCreateCommandToken(defnID1, bucketUUID, scopeID, collectionID, 0, defns1)
	tc.HandleError(err, "failed to post create command token")

	err = mc.PostCreateCommandToken(defnID2, bucketUUID, scopeID, collectionID, 0, defns2)
	tc.HandleError(err, "failed to post create command token")

	log.Printf("waiting for all indexes %v to be active", defnIDs)
	err = secondaryindex.WaitTillAllIndexesActive(defnIDs, client, 60)
	if err != nil {
		errMsg := fmt.Sprintf("failed to wait for all indexes to be active. err - %v", err)
		setupRetry(errMsg)
		goto init
	}

	var numIndexesBeforeRebal = 0
	indexNamesBeforeRebal := []string{}
	statuses, err := getIndexStatusFromIndexer()
	tc.HandleError(err, "failed to get index status from indexer")
	for _, status := range statuses.Status {
		if status.Scope == "_system" && status.Collection == "_default" {
			continue
		}
		numIndexesBeforeRebal++
		indexNamesBeforeRebal = append(indexNamesBeforeRebal, status.IndexName)
	}

	performClusterStateValidation(t, false)

	swapRebalance(t, 3, 2)

	var numIndexesAfterRebal = 0
	indexNamesAfterRebal := []string{}
	statuses, err = getIndexStatusFromIndexer()
	tc.HandleError(err, "failed to get index status from indexer")
	for _, status := range statuses.Status {
		if status.Scope == "_system" && status.Collection == "_default" {
			continue
		}
		if status.DefnId == defnID2 {
			// we have failed to drop the moving index
			if retry > 5 {
				t.Skipf("skipping test as we could not deterministically drop the moving index in 5 tries")
				return
			}
			errMsg := fmt.Sprintf("duplicate index with defnID %v on moving shard ['1-0-0'] was not dropped. %v would have been dropped. retrying test...", defnID2, defnID1)
			setupRetry(errMsg)
			goto init
		}
		numIndexesAfterRebal++
		indexNamesAfterRebal = append(indexNamesAfterRebal, status.IndexName)
	}

	performClusterStateValidation(t, false)

	if numIndexesAfterRebal+1 != numIndexesBeforeRebal {
		t.Fatalf("%v expected %v indexes to be active but found %v. Before rebalance - %v, after rebalance - %v",
			t.Name(), numIndexesBeforeRebal, numIndexesAfterRebal, indexNamesBeforeRebal, indexNamesAfterRebal)
	}
}

func TestShardRebalanceSetupCluster(t *testing.T) {
	resetCluster(t)

	tc.HandleError(secondaryindex.ChangeIndexerSettings("indexer.settings.enable_shard_affinity", false, clusterconfig.Username, clusterconfig.Password, kvaddress), "Failed to reset shard affinity")
}

func Test_SaveMProf3(t *testing.T) {
	Test_SaveMProf(t)
}

func swapRebalance(t *testing.T, nidIn, nidOut int) {
	addNode(clusterconfig.Nodes[nidIn], "index", t)
	removeNode(clusterconfig.Nodes[nidOut], t)
}

func getLocalMetaWithRetry(nodeAddress string) (*manager.LocalIndexMetadata, error) {
	meta := (*manager.LocalIndexMetadata)(nil)
	err := c.NewRetryHelper(5, 1*time.Millisecond, 5,
		func(attempts int, lastErr error) error {
			if attempts > 0 {
				log.Printf("WARN - failed to get local meta from %v for %v times. Last err - %v",
					nodeAddress, attempts, lastErr)
			}
			var err error
			meta, err = secondaryindex.GetIndexLocalMetadata(clusterconfig.Username, clusterconfig.Password, nodeAddress)
			return err
		}).RunWithConditionalError(
		func(err error) bool {
			return !strings.Contains(err.Error(), syscall.ECONNREFUSED.Error())
		})

	return meta, err
}

func getLastRebalanceReport(kvaddress, username, password string) (map[string]interface{}, error) {
	var res map[string]interface{}
	err := c.NewRetryHelper(5, 1*time.Millisecond, 5, func(attemp int, lastErr error) error {
		resp, err := http.Get(fmt.Sprintf("http://%v:%v@%v/logs/rebalanceReport", username, password, kvaddress))
		if resp.Body != nil {
			defer resp.Body.Close()
		}

		if err != nil {
			return err
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		return json.Unmarshal(body, &res)
	}).Run()

	return res, err
}
