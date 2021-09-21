package functionaltests

import (
	"fmt"
	"log"
	"testing"
	"time"

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

var indexNamePrefix string = "set14_idx_" // prefix of all index names created

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
	const method string = "setupCluster" // for logging
	resetCluster(t)
	status := getClusterStatus()
	if len(status) != 2 || !isNodeIndex(status, clusterconfig.Nodes[1]) {
		t.Fatalf("%v: Unexpected cluster configuration: %v", method, status)
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

///////////////////////////////////////////////////////////////////////////////
// TEST FUNCTIONS
///////////////////////////////////////////////////////////////////////////////

// TestRebalanceSetupCluster sets the starting cluster configuration expected for the rest of these tests:
//   [0: kv n1ql] -- assumed to be correct already (not actually set here)
//   [1: index]
// It also sets indexer.settings.rebalance.redistribute_indexes = true so rebalance will move
// indexes to empty nodes.
func TestRebalanceSetupCluster(t *testing.T) {
	const method string = "TestRebalanceSetupCluster" // for logging
	printClusterConfig(method, "entry")
	if skipTest() {
		log.Printf("%v: Test skipped", method)
		return
	}

	log.Printf("%v: 1. Setting up initial cluster configuration", method)
	setupCluster(t)
	status := getClusterStatus()
	if len(status) != 2 || !isNodeIndex(status, clusterconfig.Nodes[1]) {
		t.Fatalf("%v: Unexpected cluster configuration: %v", method, status)
	}

	// Enable Rebalance to move indexes onto empty nodes
	log.Printf("%v: 2. Changing indexer.settings.rebalance.redistribute_indexes to true", method)
	err := secondaryindex.ChangeIndexerSettings("indexer.settings.rebalance.redistribute_indexes",
		true, clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error setting indexer.settings.rebalance.redistribute_indexes to true")

	printClusterConfig(method, "exit")
}

// TestCreateDocsBeforeRebalance creates some documents (from testdata/Users_mut.txt.gz)
// to be indexed.
//   Starting config: [0: kv n1ql] [1: index]
//   Ending config:   same
func TestCreateDocsBeforeRebalance(t *testing.T) {
	const method string = "TestCreateDocsBeforeRebalance" // for logging
	printClusterConfig(method, "entry")
	if skipTest() {
		log.Printf("%v: Test skipped", method)
		return
	}

	docs := 100 // # docs to create
	log.Printf("%v: 1. Creating %v documents", method, docs)
	CreateDocs(docs)
	log.Printf("%v: %v documents created", method, docs)

	printClusterConfig(method, "exit")
}

// TestCreateIndexesBeforeRebalance creates some indexes so Rebalance will do
// index movements. Otherwise this test suite would depend on other tests having
// run before it to properly exercise Rebalance. (Cf set00_indexoperations_test.go.)
//   Starting config: [0: kv n1ql] [1: index]
//   Ending config:   same
func TestCreateIndexesBeforeRebalance(t *testing.T) {
	const method string = "TestCreateIndexesBeforeRebalance" // for logging
	printClusterConfig(method, "entry")
	if skipTest() {
		log.Printf("%v: Test skipped", method)
		return
	}

	// Create non-partitioned, 0-replica, non-deferred indexes
	log.Printf("%v: 1. Creating %v indexes: non-partitioned, 0-replica, non-deferred",
		method, len(fieldNames))
	for _, fieldName := range fieldNames {
		indexName := indexNamePrefix + fieldName
		n1qlStmt := fmt.Sprintf("create index %v on `%v`(%v)", indexName, bucketName, fieldName)
		executeN1qlStmt(n1qlStmt, bucketName, method, t)
		log.Printf("%v: %v index is now active.", method, indexName)
	}

	// Create non-partitioned, 0-replica, DEFERRED indexes
	log.Printf("%v: 2. Creating %v indexes: non-partitioned, 0-replica, DEFERRED",
		method, len(fieldNames))
	for field1, fieldName1 := range fieldNames {
		fieldName2 := fieldNames[(field1+1)%len(fieldNames)]
		indexName := indexNamePrefix + "DEFERRED_" + fieldName1 + "_" + fieldName2
		n1qlStmt := fmt.Sprintf("create index %v on `%v`(%v, %v) with {\"defer_build\":true}",
			indexName, bucketName, fieldName1, fieldName2)
		if clusterconfig.IndexUsing == "memory_optimized" {
			time.Sleep(100 * time.Millisecond) // need to slow these a bit on memory_optimized
		}
		executeN1qlStmt(n1qlStmt, bucketName, method, t)
		log.Printf("%v: %v index is now deferred.", method, indexName)
	}

	// Create 7-PARTITION, 0-replica, non-deferred indexes. Skip on FDB to avoid error:
	// [5000] GSI CreateIndex() - cause: Create Index fails. Reason = Cannot create partitioned index using forestdb
	if clusterconfig.IndexUsing != "forestdb" {
		numToCreate := 3
		log.Printf("%v: 3. Creating %v indexes: 7-PARTITION, 0-replica, non-deferred",
			method, numToCreate)
		for field1 := 0; field1 < numToCreate; field1++ {
			fieldName1 := fieldNames[field1%len(fieldNames)]
			fieldName2 := fieldNames[(field1+2)%len(fieldNames)]
			indexName := indexNamePrefix + "7PARTITIONS_" + fieldName1 + "_" + fieldName2
			n1qlStmt := fmt.Sprintf("create index %v on `%v`(%v, %v) partition by hash(Meta().id) with {\"num_partition\":7}",
				indexName, bucketName, fieldName1, fieldName2)
			executeN1qlStmt(n1qlStmt, bucketName, method, t)
			log.Printf("%v: %v index is now active.", method, indexName)
		}
	} else {
		log.Printf("%v: Skipped unsupported creation of partitioned indexes on ForestDB.", method)
	}

	printClusterConfig(method, "exit")
}

// TestIndexNodeRebalanceIn adds nodes [2: index] and [3: index], then rebalances. The actions performed are
// the same as TestRebalanceReplicaRepair except that at this point in the flow there are NO replicas to repair.
//   Starting config: [0: kv n1ql] [1: index]
//   Ending config:   [0: kv n1ql] [1: index] [2: index] [3: index]
func TestIndexNodeRebalanceIn(t *testing.T) {
	addTwoNodesAndRebalance("TestIndexNodeRebalanceIn", t)
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
	time.Sleep(5 * time.Second) // even tho rebalance fn waited, Rebalance may not really be finished yet

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
	const method string = "TestCreateReplicatedIndexesBeforeRebalance" // for logging
	printClusterConfig(method, "entry")
	if skipTest() {
		log.Printf("%v: Test skipped", method)
		return
	}

	// Create non-partitioned, 2-REPLICA, non-deferred indexes
	numToCreate := 5
	log.Printf("%v: 1. Creating %v indexes: non-partitioned, 2-REPLICA, non-deferred",
		method, numToCreate)
	for field1 := 0; field1 < numToCreate; field1++ {
		fieldName1 := fieldNames[field1%len(fieldNames)]
		fieldName2 := fieldNames[(field1+3)%len(fieldNames)]
		indexName := indexNamePrefix + "2REPLICAS_" + fieldName1 + "_" + fieldName2
		n1qlStmt := fmt.Sprintf("create index %v on `%v`(%v, %v) with {\"num_replica\":2}",
			indexName, bucketName, fieldName1, fieldName2)
		executeN1qlStmt(n1qlStmt, bucketName, method, t)
		//time.Sleep(6 * time.Second) // these take some time to build
		log.Printf("%v: %v index is now active.", method, indexName)
	}

	// Create 5-PARTITION, 1-REPLICA, non-deferred indexes. Skip on FDB to avoid error:
	// [5000] GSI CreateIndex() - cause: Create Index fails. Reason = Cannot create partitioned index using forestdb
	if clusterconfig.IndexUsing != "forestdb" {
		numToCreate = 2
		log.Printf("%v: 2. Creating %v indexes: 5-PARTITION, 1-REPLICA, non-deferred",
			method, numToCreate)
		for field1 := 0; field1 < numToCreate; field1++ {
			fieldName1 := fieldNames[field1%len(fieldNames)]
			fieldName2 := fieldNames[(field1+4)%len(fieldNames)]
			indexName := indexNamePrefix + "5PARTITIONS_1REPLICAS_" + fieldName1 + "_" + fieldName2
			n1qlStmt := fmt.Sprintf(
				"create index %v on `%v`(%v, %v) partition by hash(Meta().id) with {\"num_partition\":5, \"num_replica\":1}",
				indexName, bucketName, fieldName1, fieldName2)
			executeN1qlStmt(n1qlStmt, bucketName, method, t)
			//time.Sleep(15 * time.Second) // these take some time to build
			log.Printf("%v: %v index is now active.", method, indexName)
		}
	} else {
		log.Printf("%v: Skipped unsupported creation of partitioned indexes on ForestDB.", method)
	}

	printClusterConfig(method, "exit")
}

// TestIndexNodeRebalanceOut removes node [1: index] from the cluster, which includes an implicit
// Rebalance. Since there are indexes on node 1, this will force Planner to generate transfer tokens
// to move the indexes off of that node. (On entry there are not necessarily any indexes on nodes
// 2-3 -- see TestIndexNodeRebalanceIn comment header.)
//   Starting config: [0: kv n1ql] [1: index] [2: index] [3: index]
//   Ending config:   [0: kv n1ql]            [2: index] [3: index]
func TestIndexNodeRebalanceOut(t *testing.T) {
	const method string = "TestIndexNodeRebalanceOut" // for logging
	printClusterConfig(method, "entry")
	if skipTest() {
		log.Printf("%v: Test skipped", method)
		return
	}

	node := 1
	log.Printf("%v: 1. Rebalancing index node %v out of the cluster", method, clusterconfig.Nodes[node])
	removeNode(clusterconfig.Nodes[node], t)
	time.Sleep(5 * time.Second) // even tho removeNode fn waited, Rebalance may not really be finished yet

	status := getClusterStatus()
	if len(status) != 3 || !isNodeIndex(status, clusterconfig.Nodes[2]) && !isNodeIndex(status, clusterconfig.Nodes[3]) {
		t.Fatalf("%v: Unexpected cluster configuration: %v", method, status)
	}

	printClusterConfig(method, "exit")
}

// TestFailoverAndRebalance fails over node [2: index] from the cluster and rebalances.
//   Starting config: [0: kv n1ql]            [2: index] [3: index]
//   Ending config:   [0: kv n1ql]                       [3: index]
func TestFailoverAndRebalance(t *testing.T) {
	const method string = "TestFailoverAndRebalance" // for logging
	printClusterConfig(method, "entry")
	if skipTest() {
		log.Printf("%v: Test skipped", method)
		return
	}

	node := 2
	log.Printf("%v: 1. Failing over index node %v", method, clusterconfig.Nodes[node])
	failoverNode(clusterconfig.Nodes[node], t)

	log.Printf("%v: 2. Rebalancing", method)
	rebalance(t)

	status := getClusterStatus()
	if len(status) != 2 || !isNodeIndex(status, clusterconfig.Nodes[3]) {
		t.Fatalf("%v: Unexpected cluster configuration: %v", method, status)
	}

	printClusterConfig(method, "exit")
}

// TestSwapRebalance adds node [1: index] to the cluster (without rebalancing), then rebalances out
// node [3: index], thus performing a "swap rebalance" of node 1 replacing node 3.
//   Starting config: [0: kv n1ql]                       [3: index]
//   Ending config:   [0: kv n1ql] [1: index]
func TestSwapRebalance(t *testing.T) {
	const method string = "TestSwapRebalance" // for logging
	printClusterConfig(method, "entry")
	if skipTest() {
		log.Printf("%v: Test skipped", method)
		return
	}

	node := 1
	log.Printf("%v: 1. Adding index node %v to the cluster", method, clusterconfig.Nodes[node])
	addNode(clusterconfig.Nodes[node], "index", t)

	node = 3
	log.Printf("%v: 2. Swap rebalancing index node %v out of the cluster", method, clusterconfig.Nodes[node])
	removeNode(clusterconfig.Nodes[node], t)

	status := getClusterStatus()
	if len(status) != 2 || !isNodeIndex(status, clusterconfig.Nodes[1]) {
		t.Fatalf("%v: Unexpected cluster configuration: %v", method, status)
	}

	printClusterConfig(method, "exit")
}

// TestRebalanceReplicaRepair adds nodes [2: index] and [3: index], then rebalances. The actions performed are
// the same as TestIndexNodeRebalanceIn except that at this point in the flow there ARE replicas to repair.
//   Starting config: [0: kv n1ql] [1: index]
//   Ending config:   [0: kv n1ql] [1: index] [2: index] [3: index]
// (Same as TestIndexNodeRebalanceIn.)
func TestRebalanceReplicaRepair(t *testing.T) {
	addTwoNodesAndRebalance("TestRebalanceReplicaRepair", t)
}

// TestRebalanceResetCluster restores indexer.settings.rebalance.redistribute_indexes = false
// to avoid affecting other tests, including those that call addNodeAndRebalance/AddNodeAndRebalance,
// amd then resets the expected starting cluster configuration, as some later tests depend on it.
func TestRebalanceResetCluster(t *testing.T) {
	const method string = "TestRebalanceResetCluster" // for logging
	printClusterConfig(method, "entry")
	if skipTest() {
		log.Printf("%v: Test skipped", method)
		return
	}

	// Disable Rebalance from moving indexes onto empty nodes (its normal setting from ns_server)
	log.Printf("%v: 1. Restoring indexer.settings.rebalance.redistribute_indexes to false", method)
	err := secondaryindex.ChangeIndexerSettings("indexer.settings.rebalance.redistribute_indexes",
		false, clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error setting indexer.settings.rebalance.redistribute_indexes to false")

	log.Printf("%v: 2. Resetting cluster to initial configuration", method)
	setupCluster(t)
	status := getClusterStatus()
	if len(status) != 2 || !isNodeIndex(status, clusterconfig.Nodes[1]) {
		t.Fatalf("%v: Unexpected cluster configuration: %v", method, status)
	}
	printClusterConfig(method, "exit")
}

// NOTE: Make sure that the last test in this file resets the cluster configuration
// by calling setupCluster so that it doesn't break any other tests that run after
// the tests in this file
