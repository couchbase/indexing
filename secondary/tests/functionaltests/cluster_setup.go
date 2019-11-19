package functionaltests

import (
	cluster "github.com/couchbase/indexing/secondary/tests/framework/clusterutility"
	"testing"
)

func addNode(hostname, role string, t *testing.T) {
	serverAddr := clusterconfig.KVAddress
	username := clusterconfig.Username
	password := clusterconfig.Password

	if err := cluster.AddNode(serverAddr, username, password, hostname, role); err != nil {
		t.Fatalf(err.Error())
	}
}

func removeNode(hostname string, t *testing.T) {
	serverAddr := clusterconfig.KVAddress
	username := clusterconfig.Username
	password := clusterconfig.Password

	if err := cluster.RemoveNode(serverAddr, username, password, hostname); err != nil {
		t.Fatalf(err.Error())
	}
}

func failoverNode(hostname string, t *testing.T) {
	serverAddr := clusterconfig.KVAddress
	username := clusterconfig.Username
	password := clusterconfig.Password

	if err := cluster.FailoverNode(serverAddr, username, password, hostname); err != nil {
		t.Fatalf(err.Error())
	}
}

func rebalance(t *testing.T) {
	serverAddr := clusterconfig.KVAddress
	username := clusterconfig.Username
	password := clusterconfig.Password

	if err := cluster.Rebalance(serverAddr, username, password); err != nil {
		t.Fatalf(err.Error())
	}
}

// resetCluster will drop the nodes: Nodes[1], Nodes[2], Nodes[3] from
// the cluster and adds back Nodes[1] ("index") into the cluster. We assume
// that Nodes[0] is present in always present in the cluster
func resetCluster(t *testing.T) {
	serverAddr := clusterconfig.KVAddress
	username := clusterconfig.Username
	password := clusterconfig.Password

	dropNodes := []string{clusterconfig.Nodes[1], clusterconfig.Nodes[2], clusterconfig.Nodes[3]}
	keepNodes := make(map[string]string)
	keepNodes[clusterconfig.Nodes[1]] = "index"

	if err := cluster.ResetCluster(serverAddr, username, password, dropNodes, keepNodes); err != nil {
		t.Fatalf(err.Error())
	}

	// Verify that the cluster was reset successfully
	status := getClusterStatus()
	if len(status) != 2 || !isNodeIndex(status, clusterconfig.Nodes[1]) {
		t.Fatalf("Unable to resest the cluster, current cluster state: %v", status)
	}
}

func getClusterStatus() map[string][]string {
	serverAddr := clusterconfig.KVAddress
	username := clusterconfig.Username
	password := clusterconfig.Password

	return cluster.GetClusterStatus(serverAddr, username, password)
}

func isNodeIndex(status map[string][]string, hostname string) bool {
	return cluster.IsNodeIndex(status, hostname)
}

func validateServers(nodes []string) error {
	serverAddr := clusterconfig.KVAddress
	username := clusterconfig.Username
	password := clusterconfig.Password

	return cluster.ValidateServers(serverAddr, username, password, nodes)
}

// A 4 node cluster should have index services on Nodes[1], Nodes[2], Nodes[3]
func is4NodeCluster() bool {
	status := getClusterStatus()
	if isNodeIndex(status, clusterconfig.Nodes[1]) && isNodeIndex(status, clusterconfig.Nodes[2]) && isNodeIndex(status, clusterconfig.Nodes[3]) {
		return true
	}
	return false
}

// A 4 node cluster will have index services on Nodes[1], Nodes[2], Nodes[3]
func init4NodeCluster(t *testing.T) {

	resetCluster(t)
	addNode(clusterconfig.Nodes[2], "index", t)
	addNode(clusterconfig.Nodes[3], "index", t)

	if !is4NodeCluster() {
		t.Fatalf("Unable to initialize 4 node cluster. Cluster status: %v", getClusterStatus())
	}
}
