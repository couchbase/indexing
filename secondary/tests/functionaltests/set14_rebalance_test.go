package functionaltests

import (
	"log"
	"testing"
	"time"
)

func TestIndexNodeRebalanceIn(t *testing.T) {
	if !clusterconfig.MultipleIndexerTests || len(clusterconfig.Nodes) < 4 {
		return
	}

	log.Printf("In TestIndexNodeRebalanceIn()")
	log.Printf("This test will rebalance in an indexer node into the cluster")

	if err := validateServers(clusterconfig.Nodes); err != nil {
		log.Printf("Error while validating cluster, err: %v", err)
		log.Printf("Considering the test successful")
		return
	}
	status := getClusterStatus()
	//more than 2 nodes in the cluster or second node is not an index node
	if len(status) != 2 || !isNodeIndex(status, clusterconfig.Nodes[1]) {
		log.Printf("Unexpected cluster status, status: %v", status)
		resetCluster(t)
	}

	addNode(clusterconfig.Nodes[2], "index", t)

	status = getClusterStatus()
	if len(status) != 3 || !isNodeIndex(status, clusterconfig.Nodes[1]) || !isNodeIndex(status, clusterconfig.Nodes[2]) {
		t.Fatalf("Unexpected cluster status: %v", status)
	}
}

// This test assmues that the cluster has kv+n1ql on clusterconfig.Nodes[0],
// index on clusterconfig.Nodes[1] and clusterconfig.Nodes[2]. This test will
// rebalance out clusterconfig.Nodes[1] from cluster
func TestIndexNodeRebalanceOut(t *testing.T) {
	if !clusterconfig.MultipleIndexerTests || len(clusterconfig.Nodes) < 4 {
		return
	}

	log.Printf("In TestIndexNodeRebalanceOut()")
	log.Printf("This test will rebalance out an indexer node (%v) out of the cluster", clusterconfig.Nodes[1])

	if err := validateServers(clusterconfig.Nodes); err != nil {
		log.Printf("Error while validating cluster, err: %v", err)
		log.Printf("Considering the test successful")
		return
	}

	status := getClusterStatus()
	if len(status) != 3 || !isNodeIndex(status, clusterconfig.Nodes[1]) || !isNodeIndex(status, clusterconfig.Nodes[2]) {
		t.Fatalf("Unexpected cluster configuration: %v. Expecting 1kv+n1ql, 2 index nodes in the cluster", status)
	}

	removeNode(clusterconfig.Nodes[1], t)

	status = getClusterStatus()
	if len(status) != 2 || !isNodeIndex(status, clusterconfig.Nodes[2]) && !isNodeIndex(status, clusterconfig.Nodes[1]) {
		t.Fatalf("Unexpected cluster status: %v", status)
	}
	time.Sleep(3 * time.Second)
}

// This test assmues that the cluster has kv+n1ql on clusterconfig.Nodes[0],
// index on clusterconfig.Nodes[2]. This test will failover clusterconfig.Nodes[2],
// rebalace the cluster and reset's the cluster
func TestFailoverAndRebalance(t *testing.T) {
	if !clusterconfig.MultipleIndexerTests || len(clusterconfig.Nodes) < 4 {
		return
	}

	log.Printf("In TestFailoverAndRebalance()")
	log.Printf("This test will failover and rebalance-out an indexer node (%v) out of the cluster", clusterconfig.Nodes[2])

	if err := validateServers(clusterconfig.Nodes); err != nil {
		log.Printf("Error while validating cluster, err: %v", err)
		log.Printf("Considering the test successful")
		return
	}

	status := getClusterStatus()
	if len(status) != 2 || !isNodeIndex(status, clusterconfig.Nodes[2]) {
		t.Fatalf("Unexpected cluster status: %v", status)
	}

	failoverNode(clusterconfig.Nodes[2], t)
	rebalance(t)

	status = getClusterStatus()
	if len(status) != 1 {
		t.Fatalf("Unexpected cluster status: %v", status)
	}
	time.Sleep(3 * time.Second)
}

func TestResetCluster(t *testing.T) {
	if !clusterconfig.MultipleIndexerTests || len(clusterconfig.Nodes) < 4 {
		return
	}
	resetCluster(t)
}

// NOTE: Make sure that the last test in this file resets the cluster configuration
// to 1 kv+n1ql, 1 index nodes so that it doesn't break any other tests that run after
// the tests in this file
