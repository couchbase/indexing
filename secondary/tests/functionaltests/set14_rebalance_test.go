package functionaltests

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"testing"
	"time"

	"github.com/couchbase/cbauth/service"
	c "github.com/couchbase/indexing/secondary/common"
	cluster "github.com/couchbase/indexing/secondary/tests/framework/clusterutility"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
)

///////////////////////////////////////////////////////////////////////////////
// GLOBAL VARIABLES
///////////////////////////////////////////////////////////////////////////////

var bucketName string = "default" // bucket to create indexes in

// fieldNames is names of most of the small-value fields in the test docs
// (from testdata/Users_mut.txt.gz).
var fieldNames []string = []string{
	"_id",
	"docid",
	"guid",
	"isActive",
	"balance",
	"picture",
	"age",
	"eyeColor",
	"name",
	"gender",
	"company",
	"email",
	"phone",
	"registered",
	"latitude",
	"longitude",
	"favoriteFruit",
}

const indexNamePrefix = "set14_idx_" // prefix of all index names created

///////////////////////////////////////////////////////////////////////////////
// UTILITY FUNCTIONS
///////////////////////////////////////////////////////////////////////////////

// executeN1qlStmt executes a N1QL statement on the given bucket and fails the
// calling test if the N1QL fails. bucketName is a convenience to avoid having to
// parse n1qlStmt to find it. caller gives the caller's name for logging.
func executeN1qlStmt(n1qlStmt, bucketName, caller string, t *testing.T) {
	log.Printf("%v: Executing N1QL: %v", caller, n1qlStmt)
	_, err := tc.ExecuteN1QLStatement(indexManagementAddress, clusterconfig.Username,
		clusterconfig.Password, bucketName, n1qlStmt, false, nil)
	FailTestIfError(err, fmt.Sprintf("%v: Error executing N1QL %v", caller, n1qlStmt), t)
}

// printClusterConfig prints the current cluster configuration, which is a map from
// node addresses to a slice of the service names they have, such as
//    172.31.5.112:9000:[kv n1ql] map[127.0.0.1:9001:[index kv] 127.0.0.1:9002:[index]
// caller tells the calling function and location gives a tag like "entry" or "exit".
func printClusterConfig(caller string, location string) {
	log.Printf("%v %v: Current cluster configuration: %v", caller, location, getClusterStatus())
}

// setupCluster sets the starting cluster configuration expected for the rest of these tests.
// It is also called to reset the cluster to the same state at the end. Since all indexer
// nodes (1, 2, 3) are dropped, no indexes will remain.
// Goal config: [0: kv n1ql] [1: index]
// Node 0 is assumed to be correct already and not actually set here. Any existing nodes 1-3
// are dropped and readded as index-only nodes.
func setupCluster(t *testing.T) {
	const _setupCluster = "set14_rebalance_test.go::setupCluster:"
	resetCluster(t)
	status := getClusterStatus()
	if len(status) != 2 || !isNodeIndex(status, clusterconfig.Nodes[1]) {
		t.Fatalf("%v Unexpected cluster configuration: %v", _setupCluster, status)
	}
}

// skipTest determines whether the calling test should be run based on whether
// MultipleIndexerTests is enabled and the original cluster config file has at
// least 4 nodes in it, for backward compability with legacy test runs with
// fewer nodes defined (so these tests get skipped instead of failing).
func skipTest() bool {
	if !clusterconfig.MultipleIndexerTests || len(clusterconfig.Nodes) < 4 {
		return true
	}
	return false
}

// cancelTask calls the generic_sercvice_manager.go CancelTaskList cbauth RPC API on each index node
// with the provided arguments.
func cancelTask(id string, rev uint64, t *testing.T) {
	const _cancelTask = "set14_rebalance_test.go::cancelTask:"

	indexerNodeAddrs := getIndexerNodeAddrs(t)
	for _, nodeAddr := range indexerNodeAddrs {
		restPath := fmt.Sprintf(
			"/test/CancelTask?id=%v&rev=%v", id, rev)
		url := makeUrlForIndexNode(nodeAddr, restPath)
		resp, err := http.Get(url)
		FailTestIfError(err, fmt.Sprintf("%v http.Get %v returned error", _cancelTask, url), t)

		_, err = ioutil.ReadAll(resp.Body)
		FailTestIfError(err, fmt.Sprintf("%v ioutil.ReadAll %v returned error", _cancelTask,
			url), t)
	}
}

// getIndexerNodeAddrs returns ipAddr:port of each index node.
func getIndexerNodeAddrs(t *testing.T) []string {
	const _getIndexerNodeAddrs = "set14_rebalance_test.go::getIndexerNodeAddrs:"

	indexerNodeAddrs, err := secondaryindex.GetIndexerNodesHttpAddresses(indexManagementAddress)
	FailTestIfError(err,
		fmt.Sprintf("%v GetIndexerNodesHttpAddresses returned error", _getIndexerNodeAddrs), t)
	if len(indexerNodeAddrs) == 0 {
		t.Fatalf("%v No Index nodes found", _getIndexerNodeAddrs)
	}
	return indexerNodeAddrs
}

// getTaskList calls the generic generic_service_manager.go GetTaskList cbauth RPC API on each
// index node and returns the TaskList responses for each. It omits nil responses so as not to mask
// mysterious failures by making it look like a non-trivial response was received.
func getTaskList(t *testing.T) (taskLists []*service.TaskList) {
	const _getTaskList = "set14_rebalance_test.go::getTaskList:"

	indexerNodeAddrs := getIndexerNodeAddrs(t)
	for _, nodeAddr := range indexerNodeAddrs {
		url := makeUrlForIndexNode(nodeAddr, "/test/GetTaskList")
		resp, err := http.Get(url)
		FailTestIfError(err, fmt.Sprintf("%v http.Get %v returned error", _getTaskList, url), t)

		respBody, err := ioutil.ReadAll(resp.Body)
		FailTestIfError(err, fmt.Sprintf("%v ioutil.ReadAll %v returned error", _getTaskList,
			url), t)

		var getTaskListResponse tc.GetTaskListResponse
		err = json.Unmarshal(respBody, &getTaskListResponse)
		FailTestIfError(err, fmt.Sprintf("%v json.Unmarshal %v returned error", _getTaskList,
			url), t)

		// Return value
		taskList := getTaskListResponse.TaskList
		if taskList != nil {
			taskLists = append(taskLists, taskList)
		}

		// For debugging: Print the response
		log.Printf("%v getTaskListResponse: %+v, TaskList %+v", _getTaskList,
			getTaskListResponse, taskList)
	}

	return taskLists
}

// preparePause calls the pause_service_manager.go PreparePause cbauth RPC API on each
// index node with the provided arguments.
func preparePause(id, bucket, bucketUuid, remotePath string, t *testing.T) {
	const _preparePause = "set14_rebalance_test.go::preparePause:"

	indexerNodeAddrs := getIndexerNodeAddrs(t)
	for _, nodeAddr := range indexerNodeAddrs {
		restPath := fmt.Sprintf(
			"/test/PreparePause?id=%v&bucket=%v&bucketUuid=%v&remotePath=%v",
			id, bucket, bucketUuid, remotePath)
		url := makeUrlForIndexNode(nodeAddr, restPath)
		resp, err := http.Get(url)
		FailTestIfError(err, fmt.Sprintf("%v http.Get %v returned error", _preparePause, url), t)

		_, err = ioutil.ReadAll(resp.Body)
		FailTestIfError(err, fmt.Sprintf("%v ioutil.ReadAll %v returned error", _preparePause,
			url), t)
	}
}

// pause performs the Elixir Pause action (hibernate a bucket) using local disk instead of S3 by
// calling the pause_service_manager.go Pause cbauth RPC API on all Index nodes.
func pause() {
	// kjc_implement
}

// prepareResume calls the pause_service_manager.go PrepareResume cbauth RPC API on each
// index node with the provided arguments.
func prepareResume(id, bucket, remotePath string, dryRun bool, t *testing.T) {
	const _prepareResume = "set14_rebalance_test.go::prepareResume:"

	indexerNodeAddrs := getIndexerNodeAddrs(t)
	for _, nodeAddr := range indexerNodeAddrs {
		restPath := fmt.Sprintf(
			"/test/PrepareResume?id=%v&bucket=%v&remotePath=%v&dryRun=%v",
			id, bucket, remotePath, dryRun)
		url := makeUrlForIndexNode(nodeAddr, restPath)
		resp, err := http.Get(url)
		FailTestIfError(err, fmt.Sprintf("%v http.Get %v returned error", _prepareResume, url), t)

		_, err = ioutil.ReadAll(resp.Body)
		FailTestIfError(err, fmt.Sprintf("%v ioutil.ReadAll %v returned error", _prepareResume,
			url), t)
	}
}

// resume performs the Elixir Resume action (unhibernate a bucket) using local disk instead of S3 by
// calling the pause_service_manager.go Resume cbauth RPC API on all Index nodes.
func resume() {
	// kjc_implement
}

///////////////////////////////////////////////////////////////////////////////
// TEST FUNCTIONS
///////////////////////////////////////////////////////////////////////////////

// TestRebalanceSetupCluster sets the starting cluster configuration expected for the rest of these tests:
//   [0: kv n1ql] -- assumed to be correct already (not actually set here)
//   [1: index]
// It also sets indexer.settings.rebalance.redistribute_indexes = true so rebalance will move
// indexes to empty nodes.
func TestRebalanceSetupCluster(t *testing.T) {
	const _TestRebalanceSetupCluster = "set14_rebalance_test.go::TestRebalanceSetupCluster:"
	printClusterConfig(_TestRebalanceSetupCluster, "entry")
	if skipTest() {
		log.Printf("%v Test skipped", _TestRebalanceSetupCluster)
		return
	}

	log.Printf("%v 1. Setting up initial cluster configuration", _TestRebalanceSetupCluster)
	setupCluster(t)
	status := getClusterStatus()
	if len(status) != 2 || !isNodeIndex(status, clusterconfig.Nodes[1]) {
		t.Fatalf("%v Unexpected cluster configuration: %v", _TestRebalanceSetupCluster, status)
	}

	// Enable Rebalance to move indexes onto empty nodes
	log.Printf("%v 2. Changing indexer.settings.rebalance.redistribute_indexes to true", _TestRebalanceSetupCluster)
	err := secondaryindex.ChangeIndexerSettings("indexer.settings.rebalance.redistribute_indexes",
		true, clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error setting indexer.settings.rebalance.redistribute_indexes to true")

	printClusterConfig(_TestRebalanceSetupCluster, "exit")
}

// TestCreateDocsBeforeRebalance creates some documents (from testdata/Users_mut.txt.gz)
// to be indexed.
//   Starting config: [0: kv n1ql] [1: index]
//   Ending config:   same
func TestCreateDocsBeforeRebalance(t *testing.T) {
	const _TestCreateDocsBeforeRebalance = "set14_rebalance_test.go::TestCreateDocsBeforeRebalance:"
	printClusterConfig(_TestCreateDocsBeforeRebalance, "entry")
	if skipTest() {
		log.Printf("%v Test skipped", _TestCreateDocsBeforeRebalance)
		return
	}

	docs := 100 // # docs to create
	log.Printf("%v 1. Creating %v documents", _TestCreateDocsBeforeRebalance, docs)
	CreateDocs(docs)
	log.Printf("%v %v documents created", _TestCreateDocsBeforeRebalance, docs)

	printClusterConfig(_TestCreateDocsBeforeRebalance, "exit")
}

// TestCreateIndexesBeforeRebalance creates some indexes so Rebalance will do
// index movements. Otherwise this test suite would depend on other tests having
// run before it to properly exercise Rebalance. (Cf set00_indexoperations_test.go.)
//   Starting config: [0: kv n1ql] [1: index]
//   Ending config:   same
func TestCreateIndexesBeforeRebalance(t *testing.T) {
	const _TestCreateIndexesBeforeRebalance = "set14_rebalance_test.go::TestCreateIndexesBeforeRebalance:"
	printClusterConfig(_TestCreateIndexesBeforeRebalance, "entry")
	if skipTest() {
		log.Printf("%v Test skipped", _TestCreateIndexesBeforeRebalance)
		return
	}

	// Create non-partitioned, 0-replica, non-deferred indexes
	log.Printf("%v 1. Creating %v indexes: non-partitioned, 0-replica, non-deferred",
		_TestCreateIndexesBeforeRebalance, len(fieldNames))
	for _, fieldName := range fieldNames {
		indexName := indexNamePrefix + "PLAIN_" + fieldName
		n1qlStmt := fmt.Sprintf("create index %v on `%v`(%v)", indexName, bucketName, fieldName)
		executeN1qlStmt(n1qlStmt, bucketName, _TestCreateIndexesBeforeRebalance, t)
		log.Printf("%v %v index is now active.", _TestCreateIndexesBeforeRebalance, indexName)
	}

	// Create non-partitioned, 0-replica, DEFERRED indexes
	log.Printf("%v 2. Creating %v indexes: non-partitioned, 0-replica, DEFERRED",
		_TestCreateIndexesBeforeRebalance, len(fieldNames))
	for field1, fieldName1 := range fieldNames {
		fieldName2 := fieldNames[(field1+1)%len(fieldNames)]
		indexName := indexNamePrefix + "DEFERRED_" + fieldName1 + "_" + fieldName2
		n1qlStmt := fmt.Sprintf("create index %v on `%v`(%v, %v) with {\"defer_build\":true}",
			indexName, bucketName, fieldName1, fieldName2)
		if clusterconfig.IndexUsing == "memory_optimized" {
			time.Sleep(100 * time.Millisecond) // need to slow these a bit on memory_optimized
		}
		executeN1qlStmt(n1qlStmt, bucketName, _TestCreateIndexesBeforeRebalance, t)
		log.Printf("%v %v index is now deferred.", _TestCreateIndexesBeforeRebalance, indexName)
	}

	// Create 7-PARTITION, 0-replica, non-deferred indexes. Skip on FDB to avoid error:
	// [5000] GSI CreateIndex() - cause: Create Index fails. Reason = Cannot create partitioned index using forestdb
	if clusterconfig.IndexUsing != "forestdb" {
		numToCreate := 3
		log.Printf("%v 3. Creating %v indexes: 7-PARTITION, 0-replica, non-deferred",
			_TestCreateIndexesBeforeRebalance, numToCreate)
		for field1 := 0; field1 < numToCreate; field1++ {
			fieldName1 := fieldNames[field1%len(fieldNames)]
			fieldName2 := fieldNames[(field1+2)%len(fieldNames)]
			indexName := indexNamePrefix + "7PARTITIONS_" + fieldName1 + "_" + fieldName2
			n1qlStmt := fmt.Sprintf("create index %v on `%v`(%v, %v) partition by hash(Meta().id) with {\"num_partition\":7}",
				indexName, bucketName, fieldName1, fieldName2)
			executeN1qlStmt(n1qlStmt, bucketName, _TestCreateIndexesBeforeRebalance, t)
			log.Printf("%v %v index is now active.", _TestCreateIndexesBeforeRebalance, indexName)
		}
	} else {
		log.Printf("%v Skipped unsupported creation of partitioned indexes on ForestDB.", _TestCreateIndexesBeforeRebalance)
	}

	printClusterConfig(_TestCreateIndexesBeforeRebalance, "exit")
}

// TestIndexNodeRebalanceIn adds nodes [2: index] and [3: index], then rebalances. The actions performed are
// the same as TestRebalanceReplicaRepair except that at this point in the flow there are NO replicas to repair.
//   Starting config: [0: kv n1ql] [1: index]
//   Ending config:   [0: kv n1ql] [1: index] [2: index] [3: index]
func TestIndexNodeRebalanceIn(t *testing.T) {
	addTwoNodesAndRebalance("TestIndexNodeRebalanceIn", t)
	waitForRebalanceCleanup()
}

// addTwoNodesAndRebalance is the delegate of two tests that perform the same actions at different points
// in the Rebalance testing flow:
//   1. TestIndexNodeRebalanceIn -- no replicas to repair
//   2. TestRebalanceReplicaRepair -- some replicas need to be repaired
// Both of these expect:
//   Starting config: [0: kv n1ql] [1: index]
//   Ending config:   [0: kv n1ql] [1: index] [2: index] [3: index]
// caller gives the name of the calling function for logging.
func addTwoNodesAndRebalance(caller string, t *testing.T) {
	printClusterConfig(caller, "entry")
	if skipTest() {
		log.Printf("%v: Test skipped", caller)
		return
	}

	node := 2
	log.Printf("%v: 1. Adding index node %v to the cluster", caller, clusterconfig.Nodes[node])
	addNode(clusterconfig.Nodes[node], "index", t)

	node = 3
	log.Printf("%v: 2. Adding index node %v to the cluster", caller, clusterconfig.Nodes[node])
	addNode(clusterconfig.Nodes[node], "index", t)

	log.Printf("%v: 3. Rebalancing", caller)
	rebalance(t)

	status := getClusterStatus()
	if len(status) != 4 || !isNodeIndex(status, clusterconfig.Nodes[1]) ||
		!isNodeIndex(status, clusterconfig.Nodes[2]) || !isNodeIndex(status, clusterconfig.Nodes[3]) {

		t.Fatalf("%v: Unexpected cluster configuration: %v", caller, status)
	}

	printClusterConfig(caller, "exit")
}

// TestCreateReplicatedIndexesBeforeRebalance creates indexes with replicas (now that
// the cluster has three index nodes). A later test will do Rebalance with replica repair.
//   Starting config: [0: kv n1ql] [1: index] [2: index] [3: index]
//   Ending config:   same
func TestCreateReplicatedIndexesBeforeRebalance(t *testing.T) {
	const _TestCreateReplicatedIndexesBeforeRebalance = "set14_rebalance_test.go::TestCreateReplicatedIndexesBeforeRebalance:"
	printClusterConfig(_TestCreateReplicatedIndexesBeforeRebalance, "entry")
	if skipTest() {
		log.Printf("%v Test skipped", _TestCreateReplicatedIndexesBeforeRebalance)
		return
	}

	// Create non-partitioned, 2-REPLICA, non-deferred indexes
	numToCreate := 5
	log.Printf("%v 1. Creating %v indexes: non-partitioned, 2-REPLICA, non-deferred",
		_TestCreateReplicatedIndexesBeforeRebalance, numToCreate)
	for field1 := 0; field1 < numToCreate; field1++ {
		fieldName1 := fieldNames[field1%len(fieldNames)]
		fieldName2 := fieldNames[(field1+3)%len(fieldNames)]
		indexName := indexNamePrefix + "2REPLICAS_" + fieldName1 + "_" + fieldName2
		n1qlStmt := fmt.Sprintf("create index %v on `%v`(%v, %v) with {\"num_replica\":2}",
			indexName, bucketName, fieldName1, fieldName2)
		executeN1qlStmt(n1qlStmt, bucketName, _TestCreateReplicatedIndexesBeforeRebalance, t)
		log.Printf("%v %v index is now active.", _TestCreateReplicatedIndexesBeforeRebalance, indexName)
	}

	// Create 5-PARTITION, 1-REPLICA, non-deferred indexes. Skip on FDB to avoid error:
	// [5000] GSI CreateIndex() - cause: Create Index fails. Reason = Cannot create partitioned index using forestdb
	if clusterconfig.IndexUsing != "forestdb" {
		numToCreate = 2
		log.Printf("%v 2. Creating %v indexes: 5-PARTITION, 1-REPLICA, non-deferred",
			_TestCreateReplicatedIndexesBeforeRebalance, numToCreate)
		for field1 := 0; field1 < numToCreate; field1++ {
			fieldName1 := fieldNames[field1%len(fieldNames)]
			fieldName2 := fieldNames[(field1+4)%len(fieldNames)]
			indexName := indexNamePrefix + "5PARTITIONS_1REPLICAS_" + fieldName1 + "_" + fieldName2
			n1qlStmt := fmt.Sprintf(
				"create index %v on `%v`(%v, %v) partition by hash(Meta().id) with {\"num_partition\":5, \"num_replica\":1}",
				indexName, bucketName, fieldName1, fieldName2)
			executeN1qlStmt(n1qlStmt, bucketName, _TestCreateReplicatedIndexesBeforeRebalance, t)
			log.Printf("%v %v index is now active.", _TestCreateReplicatedIndexesBeforeRebalance, indexName)
		}
	} else {
		log.Printf("%v Skipped unsupported creation of partitioned indexes on ForestDB.", _TestCreateReplicatedIndexesBeforeRebalance)
	}

	printClusterConfig(_TestCreateReplicatedIndexesBeforeRebalance, "exit")
}

// TestIndexNodeRebalanceOut removes node [1: index] from the cluster, which includes an implicit
// Rebalance. Since there are indexes on node 1, this will force Planner to generate transfer tokens
// to move the indexes off of that node. (On entry there are not necessarily any indexes on nodes
// 2-3 -- see TestIndexNodeRebalanceIn comment header.)
//   Starting config: [0: kv n1ql] [1: index] [2: index] [3: index]
//   Ending config:   [0: kv n1ql]            [2: index] [3: index]
func TestIndexNodeRebalanceOut(t *testing.T) {
	const _TestIndexNodeRebalanceOut = "set14_rebalance_test.go::TestIndexNodeRebalanceOut:"
	printClusterConfig(_TestIndexNodeRebalanceOut, "entry")
	if skipTest() {
		log.Printf("%v Test skipped", _TestIndexNodeRebalanceOut)
		return
	}

	node := 1
	log.Printf("%v 1. Rebalancing index node %v out of the cluster", _TestIndexNodeRebalanceOut, clusterconfig.Nodes[node])
	removeNode(clusterconfig.Nodes[node], t)

	status := getClusterStatus()
	if len(status) != 3 || !isNodeIndex(status, clusterconfig.Nodes[2]) && !isNodeIndex(status, clusterconfig.Nodes[3]) {
		t.Fatalf("%v Unexpected cluster configuration: %v", _TestIndexNodeRebalanceOut, status)
	}

	printClusterConfig(_TestIndexNodeRebalanceOut, "exit")
	waitForRebalanceCleanup()
}

// TestFailoverAndRebalance fails over node [2: index] from the cluster and rebalances.
//   Starting config: [0: kv n1ql]            [2: index] [3: index]
//   Ending config:   [0: kv n1ql]                       [3: index]
func TestFailoverAndRebalance(t *testing.T) {
	const _TestFailoverAndRebalance = "set14_rebalance_test.go::TestFailoverAndRebalance:"
	printClusterConfig(_TestFailoverAndRebalance, "entry")
	if skipTest() {
		log.Printf("%v Test skipped", _TestFailoverAndRebalance)
		return
	}

	node := 2
	log.Printf("%v 1. Failing over index node %v", _TestFailoverAndRebalance, clusterconfig.Nodes[node])
	failoverNode(clusterconfig.Nodes[node], t)

	log.Printf("%v 2. Rebalancing", _TestFailoverAndRebalance)
	rebalance(t)

	status := getClusterStatus()
	if len(status) != 2 || !isNodeIndex(status, clusterconfig.Nodes[3]) {
		t.Fatalf("%v Unexpected cluster configuration: %v", _TestFailoverAndRebalance, status)
	}

	printClusterConfig(_TestFailoverAndRebalance, "exit")
	waitForRebalanceCleanup()
}

// TestSwapRebalance adds node [1: index] to the cluster (without rebalancing), then rebalances out
// node [3: index], thus performing a "swap rebalance" of node 1 replacing node 3.
//   Starting config: [0: kv n1ql]                       [3: index]
//   Ending config:   [0: kv n1ql] [1: index]
func TestSwapRebalance(t *testing.T) {
	const _TestSwapRebalance = "set14_rebalance_test.go::TestSwapRebalance:"
	printClusterConfig(_TestSwapRebalance, "entry")
	if skipTest() {
		log.Printf("%v Test skipped", _TestSwapRebalance)
		return
	}

	node := 1
	log.Printf("%v 1. Adding index node %v to the cluster", _TestSwapRebalance, clusterconfig.Nodes[node])
	addNode(clusterconfig.Nodes[node], "index", t)

	node = 3
	log.Printf("%v 2. Swap rebalancing index node %v out of the cluster", _TestSwapRebalance, clusterconfig.Nodes[node])
	removeNode(clusterconfig.Nodes[node], t)

	status := getClusterStatus()
	if len(status) != 2 || !isNodeIndex(status, clusterconfig.Nodes[1]) {
		t.Fatalf("%v Unexpected cluster configuration: %v", _TestSwapRebalance, status)
	}

	printClusterConfig(_TestSwapRebalance, "exit")
	waitForRebalanceCleanup()
}

// TestRebalanceReplicaRepair adds nodes [2: index] and [3: index], then rebalances. The actions performed are
// the same as TestIndexNodeRebalanceIn except that at this point in the flow there ARE replicas to repair.
//   Starting config: [0: kv n1ql] [1: index]
//   Ending config:   [0: kv n1ql] [1: index] [2: index] [3: index]
// (Same as TestIndexNodeRebalanceIn.)
func TestRebalanceReplicaRepair(t *testing.T) {
	addTwoNodesAndRebalance("TestRebalanceReplicaRepair", t)
	waitForRebalanceCleanup()
}

// TestPauseAndResume tests Elixir Pause and Resume features using local disk instead of S3.
//   Starting config: [0: kv n1ql] [1: index] [2: index] [3: index]
//   Ending config:   [0: kv n1ql] [1: index] [2: index] [3: index]
func TestPauseAndResume(t *testing.T) {
	const _TestPauseAndResume = "set14_rebalance_test.go::TestPauseAndResume:"
	printClusterConfig(_TestPauseAndResume, "entry")
	if skipTest() {
		log.Printf("%v Test skipped", _TestPauseAndResume)
		return
	}
	defer printClusterConfig(_TestPauseAndResume, "exit")

	log.Printf("%v Before start, calling GetTaskList", _TestPauseAndResume)
	const numIndexNodes = 3 // number of index nodes throughout this test
	taskLists := getTaskList(t)
	if len(taskLists) != numIndexNodes {
		t.Fatalf("%v Before Pause-Resume expected %v getTaskList replies, got %v", _TestPauseAndResume, numIndexNodes, len(taskLists))
	}

	// Arguments for PreparePause and PrepareResume create and cancel tests
	const (
		taskId     = "fakeTaskId"
		bucket     = "fakeBucket"
		bucketUuid = "fakeBucketUuid"
		remotePath = "file:///fakeRemotePath"
		archiveDir = "/fakeRemotePath/" // tweaked version of remotePath stored in the tasks
		dryRun     = true
		rev        = uint64(0) // rev for CancelTask; so far does not matter for Pause-Resume
	)

	// Proxies for some constants from pause_service_manager.go as tests cannot import the indexer
	// package as that pulls in sigar which won't build here. (Thus these are also not exported.)
	const (
		archive_FILE        = "File"
		task_PREPARE_PAUSE  = "task-prepare-pause"  // ns_server version of the string name
		task_PREPARE_RESUME = "task-prepare-resume" // ditto
	)

	// PreparePause --------------------------------------------------------------------------------
	log.Printf("%v Calling PreparePause", _TestPauseAndResume)
	preparePause(taskId, bucket, bucketUuid, remotePath, t)
	log.Printf("%v Calling GetTaskList(PreparePause)", _TestPauseAndResume)
	taskLists = getTaskList(t)
	if len(taskLists) != numIndexNodes {
		t.Fatalf("%v After PreparePause expected %v getTaskList replies, got %v", _TestPauseAndResume,
			numIndexNodes, len(taskLists))
	}
	numNodeTasks := 1 // number of tasks expected in each node's task list
	for _, taskList := range taskLists {
		if len(taskList.Tasks) != numNodeTasks {
			t.Fatalf("%v PreparePause expected len(taskList.Tasks) = %v, got %v", _TestPauseAndResume,
				numNodeTasks, len(taskList.Tasks))
		}
		task := taskList.Tasks[0]
		if task.ID != taskId {
			t.Fatalf("%v PreparePause expected task.ID '%v', got '%v'", _TestPauseAndResume,
				taskId, task.ID)
		}
		if string(task.Type) != task_PREPARE_PAUSE {
			t.Fatalf("%v PreparePause expected task.Type '%v', got '%v'", _TestPauseAndResume,
				task_PREPARE_PAUSE, string(task.Type))
		}
		val := task.Extra["bucket"]
		if val != bucket {
			t.Fatalf("%v PreparePause expected bucket '%v', got '%v'", _TestPauseAndResume,
				bucket, val)
		}
		val = task.Extra["bucketUuid"]
		if val != bucketUuid {
			t.Fatalf("%v PreparePause expected bucketUuid '%v', got '%v'", _TestPauseAndResume,
				bucketUuid, val)
		}
		val = task.Extra["archiveDir"]
		if val != archiveDir {
			t.Fatalf("%v PreparePause expected archiveDir '%v', got '%v'", _TestPauseAndResume,
				archiveDir, val)
		}
		val = task.Extra["archiveType"]
		if val != archive_FILE {
			t.Fatalf("%v PreparePause expected archiveType '%v', got '%v'", _TestPauseAndResume,
				archive_FILE, val)
		}
	}

	// CancelTask(PreparePause) --------------------------------------------------------------------
	log.Printf("%v Calling CancelTask(PreparePause)", _TestPauseAndResume)
	cancelTask(taskId, rev, t)
	log.Printf("%v Calling GetTaskList()", _TestPauseAndResume)
	taskLists = getTaskList(t)
	if len(taskLists) != numIndexNodes {
		t.Fatalf("%v After CancelTask(PreparePause) expected %v getTaskList replies, got %v", _TestPauseAndResume,
			numIndexNodes, len(taskLists))
	}
	numNodeTasks = 0 // number of tasks expected in each node's task list
	for _, taskList := range taskLists {
		if len(taskList.Tasks) != numNodeTasks {
			t.Fatalf("%v CancelTask(PreparePause) expected len(taskList.Tasks) = %v, got %v", _TestPauseAndResume,
				numNodeTasks, len(taskList.Tasks))
		}
	}

	// PrepareResume -------------------------------------------------------------------------------
	log.Printf("%v Calling PrepareResume", _TestPauseAndResume)
	prepareResume(taskId, bucket, remotePath, dryRun, t)
	log.Printf("%v Calling GetTaskList(PrepareResume)", _TestPauseAndResume)
	taskLists = getTaskList(t)
	if len(taskLists) != numIndexNodes {
		t.Fatalf("%v After PrepareResume expected %v getTaskList replies, got %v", _TestPauseAndResume,
			numIndexNodes, len(taskLists))
	}
	numNodeTasks = 1 // number of tasks expected in each node's task list
	for _, taskList := range taskLists {
		if len(taskList.Tasks) != numNodeTasks {
			t.Fatalf("%v PrepareResume expected len(taskList.Tasks) = %v, got %v", _TestPauseAndResume,
				numNodeTasks, len(taskList.Tasks))
		}
		task := taskList.Tasks[0]
		if task.ID != taskId {
			t.Fatalf("%v PrepareResume expected task.ID '%v', got '%v'", _TestPauseAndResume,
				taskId, task.ID)
		}
		if string(task.Type) != task_PREPARE_RESUME {
			t.Fatalf("%v PrepareResume expected task.Type '%v', got '%v'", _TestPauseAndResume,
				task_PREPARE_RESUME, string(task.Type))
		}
		val := task.Extra["bucket"]
		if val != bucket {
			t.Fatalf("%v PrepareResume expected bucket '%v', got '%v'", _TestPauseAndResume,
				bucket, val)
		}
		val = task.Extra["dryRun"]
		if val != dryRun {
			t.Fatalf("%v PrepareResume expected dryRun '%v', got '%v'", _TestPauseAndResume,
				dryRun, val)
		}
		val = task.Extra["archiveDir"]
		if val != archiveDir {
			t.Fatalf("%v PreparePause expected archiveDir '%v', got '%v'", _TestPauseAndResume,
				archiveDir, val)
		}
		val = task.Extra["archiveType"]
		if val != archive_FILE {
			t.Fatalf("%v PreparePause expected archiveType '%v', got '%v'", _TestPauseAndResume,
				archive_FILE, val)
		}
	}

	// CancelTask(PrepareResume) -------------------------------------------------------------------
	log.Printf("%v Calling CancelTask(PrepareResume)", _TestPauseAndResume)
	cancelTask(taskId, rev, t)
	log.Printf("%v Calling GetTaskList()", _TestPauseAndResume)
	taskLists = getTaskList(t)
	if len(taskLists) != numIndexNodes {
		t.Fatalf("%v After CancelTask(PrepareResume) expected %v getTaskList replies, got %v", _TestPauseAndResume,
			numIndexNodes, len(taskLists))
	}
	numNodeTasks = 0 // number of tasks expected in each node's task list
	for _, taskList := range taskLists {
		if len(taskList.Tasks) != numNodeTasks {
			t.Fatalf("%v CancelTask(PrepareResume) expected len(taskList.Tasks) = %v, got %v", _TestPauseAndResume,
				numNodeTasks, len(taskList.Tasks))
		}
	}
}

// This test failsover an indexer while index build is going on
// and will trigger a rebalance. Due to the skewed index distribution
// indexer is expected to move indexes during rebalance and this
// rebalance should fail as DDL is in progress on the failed over node
//   Starting Config: [0: kv n1ql] [1: index] [2: index] [3: index]
//   Ending config:   [0: kv n1ql] [1: index] [2: index] [3: index]
func TestFailureAndRebalanceDuringInitialIndexBuild(t *testing.T) {
	const _TestFailureAndRebalanceDuringInitialIndexBuild = "set14_rebalance_test.go::TestPauseAndResume:"
	printClusterConfig(_TestFailureAndRebalanceDuringInitialIndexBuild, "entry")
	if skipTest() {
		log.Printf("%v Test skipped", _TestFailureAndRebalanceDuringInitialIndexBuild)
		return
	}

	bucket := "default"
	scope := "_default"
	coll := "_default"

	// Create 3 indexes on node-1
	for i := 0; i < 10; i++ {
		index := fmt.Sprintf("index_%v", i)
		err := secondaryindex.CreateSecondaryIndex3(index, bucket, scope, coll, indexManagementAddress,
			"", []string{"abcdefgh"}, []bool{false}, false, []byte("{\"nodes\": [\"127.0.0.1:9001\"]}"), c.SINGLE, nil, true,
			defaultIndexActiveTimeout, nil)
		FailTestIfError(err, fmt.Sprintf("Failed while creating index: %v", index), t)
	}

	docs := 100000
	CreateDocs(docs)

	err := secondaryindex.CreateSecondaryIndex3("index_11", bucket, scope, coll, indexManagementAddress,
		"", []string{"name"}, []bool{false}, false, []byte("{\"nodes\": [\"127.0.0.1:9002\"], \"defer_build\":true}"), c.SINGLE, nil, true,
		0, nil)
	FailTestIfError(err, fmt.Sprintf("Failed while creating index_11"), t)
	go func() {
		err = secondaryindex.BuildIndex("index_11", bucketName, indexManagementAddress, defaultIndexActiveTimeout)
	}()
	time.Sleep(1 * time.Second)
	// Failover Node-2
	failoverNode(clusterconfig.Nodes[2], t)

	// Add back node-2
	log.Printf("TestFailureAndRebalanceDuringInitialIndexBuild: 1. Adding index node %v to the cluster", clusterconfig.Nodes[2])
	recoverNode(clusterconfig.Nodes[2], "full", t)
	time.Sleep(1 * time.Second)
	if err := cluster.Rebalance(clusterconfig.KVAddress, clusterconfig.Username, clusterconfig.Password); err == nil {
		t.Fatalf("TestFailureAndRebalanceDuringInitialIndexBuild: Rebalance is expected to fail, but it succeded")
	}

	printClusterConfig(_TestFailureAndRebalanceDuringInitialIndexBuild, "exit")
}

// TestRebalanceResetCluster restores indexer.settings.rebalance.redistribute_indexes = false
// to avoid affecting other tests, including those that call addNodeAndRebalance/AddNodeAndRebalance,
// amd then resets the expected starting cluster configuration, as some later tests depend on it.
func TestRebalanceResetCluster(t *testing.T) {
	const _TestRebalanceResetCluster = "set14_rebalance_test.go::TestRebalanceResetCluster:"
	printClusterConfig(_TestRebalanceResetCluster, "entry")
	if skipTest() {
		log.Printf("%v Test skipped", _TestRebalanceResetCluster)
		return
	}

	// Disable Rebalance from moving indexes onto empty nodes (its normal setting from ns_server)
	log.Printf("%v 1. Restoring indexer.settings.rebalance.redistribute_indexes to false", _TestRebalanceResetCluster)
	err := secondaryindex.ChangeIndexerSettings("indexer.settings.rebalance.redistribute_indexes",
		false, clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error setting indexer.settings.rebalance.redistribute_indexes to false")

	log.Printf("%v 2. Resetting cluster to initial configuration", _TestRebalanceResetCluster)
	setupCluster(t)
	status := getClusterStatus()
	if len(status) != 2 || !isNodeIndex(status, clusterconfig.Nodes[1]) {
		t.Fatalf("%v Unexpected cluster configuration: %v", _TestRebalanceResetCluster, status)
	}
	printClusterConfig(_TestRebalanceResetCluster, "exit")
	waitForRebalanceCleanup()
}

func waitForRebalanceCleanup() {
	// This time is to prevent the next test to go ahead and create
	// indexes while rebalance cleanup is in progress (Rebalance cleanup
	// can take upto 1 seconds on source or destination nodes after the
	// master decides that rebalance is done. This is because,
	// waitForIndexBuild sleeps for upto 1 second before it can read the
	// closure of r.done channel)
	time.Sleep(2 * time.Second)
}
