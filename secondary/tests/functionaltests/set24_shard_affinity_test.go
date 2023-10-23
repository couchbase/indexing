package functionaltests

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	json "github.com/couchbase/indexing/secondary/common/json"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	"github.com/couchbase/tools-common/functional/maps"
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

func getShardGroupingFromLiveCluster() (tc.AlternateShardMap, error) {
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
		return nil, err
	}

	shardGrouping := make(tc.AlternateShardMap)
	for _, status := range statuses.Status {
		var replicaMap map[int]map[c.PartitionId][]string
		var partnMap map[c.PartitionId][]string

		var ok bool

		if defnStruct, ok := shardGrouping[status.DefnId]; !ok {
			replicaMap = make(map[int]map[c.PartitionId][]string)
			shardGrouping[status.DefnId] = &struct {
				Name         string
				NumReplica   int
				NumPartition int
				IsPrimary    bool
				ReplicaMap   map[int]map[c.PartitionId][]string
			}{
				Name:         status.Name,
				NumReplica:   status.NumReplica,
				NumPartition: status.NumPartition,
				IsPrimary:    status.IsPrimary,
				ReplicaMap:   replicaMap,
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
				partnMap[c.PartitionId(partnId)] = shards
			}
		}

	}

	// adjust partition count for replica indices (NumPartn will be actual value * index count)
	// for _, defnStruct := range shardGrouping {
	// 	defnStruct.NumPartition /= 1 + defnStruct.NumReplica
	// }
	return shardGrouping, nil
}

func performClusterStateValidation(t *testing.T, negTests bool, validations ...tc.InvalidClusterState) {
	shardGrouping, err := getShardGroupingFromLiveCluster()
	tc.HandleError(err, "Err in getting Index Status from live cluster")

	errMap := tc.ValidateClusterState(shardGrouping, len(validations) != 0)
	errStr := strings.Builder{}
	if len(validations) == 0 && len(errMap) != 0 {
		for violation, errs := range errMap {
			errStr.WriteString(fmt.Sprintf("\t%v violation in live cluster: %v\n", violation, errs))
		}
	} else if len(validations) > 0 && len(errMap) > 0 {
		for _, validation := range validations {
			if errs, ok := errMap[validation]; ok {
				errStr.WriteString(fmt.Sprintf("\t%v violation in live cluster: %v\n", validation, errs))
			}
		}
	}
	if errStr.Len() > 0 && !negTests {
		t.Fatalf("%v:performClusterStateValidation validations failed - \n%v", t.Name(), errStr.String())
	} else if errStr.Len() == 0 && negTests {
		if len(validations) == 0 {
			t.Fatalf("%v:performClusterStateValidation expected atleat one validation to fail but none failed", t.Name())
		} else {
			unfaildValidations := make([]tc.InvalidClusterState, 0, len(errMap))
			for _, toFailValidation := range validations {
				if _, ok := errMap[toFailValidation]; !ok {
					unfaildValidations = append(unfaildValidations, toFailValidation)
					delete(errMap, toFailValidation)
				}
			}
			if len(unfaildValidations) > 0 || len(errMap) > 0 {
				failedValidations := maps.Keys[map[tc.InvalidClusterState][]string](errMap)
				t.Fatalf("%v:performClusterState\n* expected validations(%v) to fail but did not fail\n* expetecd validations (%v) to pass but failed",
					t.Name(), unfaildValidations, failedValidations)
			}
		}
	}
}

func skipShardAffinityTests(t *testing.T) {
	if clusterconfig.IndexUsing != "plasma" {
		t.Skipf("Shard affinity tests only valid with plasma storage")
		return
	}
}

func TestWithShardAffinity(t *testing.T) {

	skipShardAffinityTests(t)

	configChanges := map[string]interface{}{
		"indexer.settings.enable_shard_affinity": true,
		"indexer.planner.honourNodesInDefn":      true,
	}
	err := secondaryindex.ChangeMultipleIndexerSettings(configChanges, clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, fmt.Sprintf("Failed to change config %v", configChanges))

	defer func() {
		configChanges := map[string]interface{}{
			"indexer.settings.enable_shard_affinity": false,
			"indexer.planner.honourNodesInDefn":      false,
		}
		err := secondaryindex.ChangeMultipleIndexerSettings(configChanges, clusterconfig.Username, clusterconfig.Password, kvaddress)
		tc.HandleError(err, fmt.Sprintf("Failed to change config %v", configChanges))
	}()

	t.Run("RebalanceSetupCluster", func(subt *testing.T) {
		TestRebalanceSetupCluster(subt)
	})
	defer t.Run("RebalanceResetCluster", func(subt *testing.T) {
		TestRebalanceResetCluster(subt)
	})

	t.Run("TestCreateDocsBeforeRebalance", func(subt *testing.T) {
		TestCreateDocsBeforeRebalance(subt)
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
		TestSwapRebalance(t)

		performClusterStateValidation(subt, true,
			tc.MISSING_REPLICA_INVALID_CLUSTER_STATE)
	})

	t.Run("TestRebalanceReplicaRepair", func(subt *testing.T) {
		TestRebalanceReplicaRepair(subt)

		performClusterStateValidation(subt, false)
	})

	t.Run("TestFailureAndRebalanceDuringInitialIndexBuild", func(subt *testing.T) {
		subt.Skipf("Unstable test")
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

}

// In an existing cluster with indices, we enable the shard affinity feature
// then swap rebalance all nodes; after the last rebalance, all indices should have
// alternate shard ids assigned to them
func TestRebalancePseudoOfflineUgradeWithShardAffinity(t *testing.T) {
	t.Skipf("Unstable test")
	skipShardAffinityTests(t)

	log.Println("*********Setup cluster*********")
	setupCluster(t)

	addNodeAndRebalance(clusterconfig.Nodes[2], "index", t)
	waitForRebalanceCleanup()

	status := getClusterStatus()
	if len(status) != 3 || !isNodeIndex(status, clusterconfig.Nodes[1]) ||
		!isNodeIndex(status, clusterconfig.Nodes[2]) {
		t.Fatalf("%v Unexpected cluster configuration: %v", t.Name(), status)
	}

	printClusterConfig(t.Name(), "entry")

	log.Printf("********Create Docs and Indices**********")
	numDocs := 1000
	CreateDocs(numDocs)

	// create primary index
	indexName := "idx_primary"
	n1qlStmt := fmt.Sprintf("create primary index %v on `%v`", indexName, BUCKET)
	executeN1qlStmt(n1qlStmt, BUCKET, t.Name(), t)
	log.Printf("%v %v index is now active.", t.Name(), indexName)

	indices := []string{}
	// create deffered indices
	for field1, fieldName1 := range fieldNames {
		fieldName2 := fieldNames[(field1+1)%len(fieldNames)]
		indexName := indexNamePrefix + "DFRD_" + fieldName1 + "_" + fieldName2
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
		indexName := indexNamePrefix + "5PTN_1RP_" + fieldName1 + "_" + fieldName2
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

	// config - [0: kv n1ql] [1: index] [2: index]
	err := secondaryindex.ChangeIndexerSettings("indexer.settings.enable_shard_affinity", true, clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Unable to change indexer setting `indexer.settings.enable_shard_affinity`")

	defer func() {
		err := secondaryindex.ChangeIndexerSettings("indexer.settings.enable_shard_affinity", false, clusterconfig.Username, clusterconfig.Password, kvaddress)
		tc.HandleError(err, "Unable to change indexer setting `indexer.settings.enable_shard_affinity`")
	}()

	log.Printf("********Swap rebalance all nodes**********")

	swapRebalance(t, 3, 2) // config - [0: kv n1ql] [1: index] .......... [3 index] - movements via DCP
	swapRebalance(t, 2, 1) // config - [0: kv n1ql] .......... [2: index] [3 index] - movements via DCP
	log.Printf("%v all nodes swap rebalanced. All indices should have under gone movement and we should have shard affinity in cluster. Validating the same...",
		t.Name())
	performClusterStateValidation(t, false)

	log.Printf("%v swap rebalancing a node to test shard rebalance and validate cluster affinity..",
		t.Name())
	swapRebalance(t, 1, 3) // config - [0: kv n1ql] [1: index] [2: index] - movements via Shards
	performClusterStateValidation(t, false)

	printClusterConfig(t.Name(), "exit")
}

// expected cluster state in entry
// - [0: kv n1ql] [1: index] [2: index]
func TestCreateInSimulatedMixedMode(t *testing.T) {
	t.Skipf("Unstable test")
	skipShardAffinityTests(t)

	status := getClusterStatus()
	if len(status) != 3 || !isNodeIndex(status, clusterconfig.Nodes[1]) ||
		!isNodeIndex(status, clusterconfig.Nodes[2]) {
		t.Fatalf("%v Unexpected cluster configuration: %v", t.Name(), status)
	}

	log.Println("*********Setup cluster*********")
	err := secondaryindex.DropAllSecondaryIndexes(clusterconfig.Nodes[1])
	tc.HandleError(err, "failed to drop all secondary indices")

	// config - [0: kv n1ql] [1: index] [2: index]
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
		indexName := indexNamePrefix + "5PTN_1RP_" + fieldName1 + "_" + fieldName2
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
// [0: kv n1ql] [1: index] [2: index]
// indices on node[1] do not have Alternate Shard IDs
func TestSwapRebalanceMixedMode(t *testing.T) {
	t.Skipf("Unstable test")
	skipShardAffinityTests(t)

	log.Printf("********Updating `indexer.settings.enable_shard_affinity`=true**********")

	configChanges := map[string]interface{}{
		"indexer.settings.enable_shard_affinity":       true,
		"indexer.planner.honourNodesInDefn":            true,
		"indexer.thisNodeOnly.ignoreAlternateShardIds": true,
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

	swapRebalance(t, 3, 2) // config - [0: kv n1ql] [1: index]            [3: index] - swap rebalance via Shard
	// cluster state still not valid wrt Alternate Shard IDs
	performClusterStateValidation(t, true)

	swapRebalance(t, 2, 1) // config - [0: kv n1ql]            [2: index] [3: index] - swap rebalance via DCP
	performClusterStateValidation(t, false)

	// indexer.thisNodeOnly.ignoreAlternateShardIds no longer valid for node[1]
	swapRebalance(t, 3, 1) // config - [0: kv n1ql] [1: index] [2: index] - swap rebalance via Shard
	performClusterStateValidation(t, false)

}

// expected cluster state in entry
// [0: kv n1ql] [1: index] [2: index]
func TestFailoverAndRebalanceMixedMode(t *testing.T) {
	t.Skipf("Unstable test")
	skipShardAffinityTests(t)

	status := getClusterStatus()
	if len(status) != 3 || !isNodeIndex(status, clusterconfig.Nodes[1]) ||
		!isNodeIndex(status, clusterconfig.Nodes[2]) {
		t.Fatalf("%v Unexpected cluster configuration: %v", t.Name(), status)
	}

	log.Println("*********Setup cluster*********")
	addNodeAndRebalance(clusterconfig.Nodes[3], "index", t)
	status = getClusterStatus()
	if len(status) != 3 || !isNodeIndex(status, clusterconfig.Nodes[1]) ||
		!isNodeIndex(status, clusterconfig.Nodes[2]) || !isNodeIndex(status, clusterconfig.Nodes[3]) {
		t.Fatalf("%v Unexpected cluster configuration: %v", t.Name(), status)
	}
	err := secondaryindex.DropAllSecondaryIndexes(clusterconfig.Nodes[1])
	tc.HandleError(err, "failed to drop all secondary indices")

	// config - [0: kv n1ql] [1: index] [2: index] [3: index]
	printClusterConfig(t.Name(), "entry")

	log.Printf("********Updating `indexer.settings.enable_shard_affinity`=true**********")

	configChanges := map[string]interface{}{
		"indexer.settings.enable_shard_affinity":       true,
		"indexer.planner.honourNodesInDefn":            true,
		"indexer.thisNodeOnly.ignoreAlternateShardIds": true,
	}
	err = secondaryindex.ChangeMultipleIndexerSettings(configChanges, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[3])
	tc.HandleError(err, fmt.Sprintf("Failed to change config %v", configChanges))

	defer func() {
		configChanges := map[string]interface{}{
			"indexer.settings.enable_shard_affinity": false,
			"indexer.planner.honourNodesInDefn":      false,
		}
		err := secondaryindex.ChangeMultipleIndexerSettings(configChanges, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[3])
		tc.HandleError(err, fmt.Sprintf("Failed to change config %v", configChanges))
	}()

	log.Printf("********Create indices with nodes clause**********")
	indices := []string{}
	// create non-deffered partitioned indices
	for field1 := 0; field1 < 2; field1++ {
		fieldName1 := fieldNames[field1%len(fieldNames)]
		fieldName2 := fieldNames[(field1+4)%len(fieldNames)]
		indexName := indexNamePrefix + "5PTN_1RP_" + fieldName1 + "_" + fieldName2
		n1qlStmt := fmt.Sprintf(
			"create index %v on `%v`(%v, %v) partition by hash(Meta().id) with {\"num_partition\":5, \"num_replica\":1, \"nodes\": [\"%v\", \"%v\"]}",
			indexName, BUCKET, fieldName1, fieldName2, clusterconfig.Nodes[3], clusterconfig.Nodes[randomNum(0, 3)])
		executeN1qlStmt(n1qlStmt, BUCKET, t.Name(), t)
		indices = append(indices, indexName)
	}
	log.Printf("%v %v indices are now active.", t.Name(), indices)

	// cluster state still not valid wrt Alternate Shard IDs
	performClusterStateValidation(t, true)

	err = secondaryindex.ChangeIndexerSettings("indexer.thisNodeOnly.ignoreAlternateShardIds", false, clusterconfig.Username, clusterconfig.Password, clusterconfig.Nodes[3])
	tc.HandleError(err, fmt.Sprintf("failed to reset `indexer.thisNodeOnly.ignoreAlternateShardIds` on %v", clusterconfig.Nodes[3]))

	failoverNode(clusterconfig.Nodes[2], t)
	rebalance(t) // config - [0: kv n1ql] [1: index]            [3: index] - replica/partn repair needs to happen
	// cluster state will still not be valid as some indices on node 3 will not have Alternate Shard IDs
	performClusterStateValidation(t, true)

	swapRebalance(t, 2, 3) // config - [0: kv n1ql] [1: index] [2: index] - shard + DCP rebalance
	performClusterStateValidation(t, false)

}

func swapRebalance(t *testing.T, nidIn, nidOut int) {
	addNode(clusterconfig.Nodes[nidIn], "index", t)
	removeNode(clusterconfig.Nodes[nidOut], t)
}
