package serverlesstests

import (
	"fmt"
	"log"
	"testing"
	"time"

	cluster "github.com/couchbase/indexing/secondary/tests/framework/clusterutility"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
)

// addNode just adds a node to the cluster but does NOT perform rebalance.
func addNode(hostname, role string, t *testing.T) {
	serverAddr := clusterconfig.KVAddress
	username := clusterconfig.Username
	password := clusterconfig.Password

	if err := cluster.AddNode(serverAddr, username, password, hostname, role); err != nil {
		t.Fatalf(err.Error())
	}
}

func recoverNode(hostname, recoveryType string, t *testing.T) {
	serverAddr := clusterconfig.KVAddress
	username := clusterconfig.Username
	password := clusterconfig.Password

	if err := cluster.RecoverNode(serverAddr, username, password, hostname, recoveryType); err != nil {
		t.Fatalf(err.Error())
	}
}

// addNodeAndRebalance adds a node to the cluster and then does a rebalance.
func addNodeAndRebalance(hostname, role string, t *testing.T) {
	serverAddr := clusterconfig.KVAddress
	username := clusterconfig.Username
	password := clusterconfig.Password

	if err := cluster.AddNodeAndRebalance(serverAddr, username, password, hostname, role); err != nil {
		t.Fatalf(err.Error())
	}
}

// removeNode performs a rebalance out (ejection) of the specified node.
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

	dropNodes := []string{clusterconfig.Nodes[1], clusterconfig.Nodes[2], clusterconfig.Nodes[3], clusterconfig.Nodes[4]}

	if err := cluster.ResetCluster(serverAddr, username, password, dropNodes, nil); err != nil {
		t.Fatalf(err.Error())
	}

	if err := cluster.AddNodeWithServerGroup(serverAddr, username, password, clusterconfig.Nodes[1], "index", "Group 2"); err != nil {
		t.Fatalf(err.Error())
	}

	if err := cluster.AddNodeWithServerGroup(serverAddr, username, password, clusterconfig.Nodes[2], "index", "Group 1"); err != nil {
		t.Fatalf(err.Error())
	}
	// Rebalance the cluster
	if err := cluster.Rebalance(serverAddr, username, password); err != nil {
		t.Fatalf(err.Error())
	}

	// Verify that the cluster was reset successfully
	status := getClusterStatus()
	if len(status) != 3 || !isNodeIndex(status, clusterconfig.Nodes[1]) || !isNodeIndex(status, clusterconfig.Nodes[2]) {
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
	addNodeAndRebalance(clusterconfig.Nodes[2], "index", t)
	addNodeAndRebalance(clusterconfig.Nodes[3], "index", t)

	if !is4NodeCluster() {
		t.Fatalf("Unable to initialize 4 node cluster. Cluster status: %v", getClusterStatus())
	}
}

func initClusterFromREST() error {
	status := getClusterStatus()
	serverAddr := clusterconfig.Nodes[0]
	username := clusterconfig.Username
	password := clusterconfig.Password

	// Initialise an un-initialised cluster. If cluster is already initialised,
	// then do not touch it
	if len(status) == 0 {
		if err := cluster.InitClusterServices(serverAddr, username, password, "kv,n1ql"); err != nil {
			return err
		}
		time.Sleep(1 * time.Second)
		if err := cluster.InitWebCreds(serverAddr, username, password); err != nil {
			return err
		}

		time.Sleep(100 * time.Millisecond)
		if err := cluster.SetDataAndIndexQuota(serverAddr, username, password, "1500", "1500"); err != nil {
			return err
		}
		time.Sleep(100 * time.Millisecond)

		// ServerGroup "Group 1" already exits by default. Create "Group 2"
		if err := cluster.AddServerGroup(serverAddr, username, password, "Group 2"); err != nil {
			return err
		}
		time.Sleep(100 * time.Millisecond)

		if err := cluster.AddNodeWithServerGroup(serverAddr, username, password, clusterconfig.Nodes[1], "index", "Group 2"); err != nil {
			return err
		}

		time.Sleep(100 * time.Millisecond)
		if err := cluster.AddNodeWithServerGroup(serverAddr, username, password, clusterconfig.Nodes[2], "index", "Group 1"); err != nil {
			return err
		}

		time.Sleep(100 * time.Millisecond)
		// Rebalance the cluster
		if err := cluster.Rebalance(serverAddr, username, password); err != nil {
			return err
		}

		time.Sleep(5 * time.Second)
		kvutility.CreateBucket("default", "sasl", "", username, password, serverAddr, "1500", "11213")
		time.Sleep(5 * time.Second)
		status = getClusterStatus()
		log.Printf("Cluster status: %v", status)
		if cluster.IsNodeIndex(status, clusterconfig.Nodes[1]) &&
			cluster.IsNodeIndex(status, clusterconfig.Nodes[2]) {
			// The IP address of Nodes[0] can get renamed. Until we figure out
			// how to disable the re-naming (like --dont-rename in cluster_connect)
			// ignore the hostname for Nodes[0] and only check for services
			delete(status, clusterconfig.Nodes[1])
			delete(status, clusterconfig.Nodes[2])
			if len(status) == 1 {
				kv := false
				n1ql := false
				for _, services := range status {
					for _, service := range services {
						if service == "kv" {
							kv = true
						} else if service == "n1ql" {
							n1ql = true
						}
					}
				}
				if kv == false || n1ql == false {
					return fmt.Errorf("Error while initialising cluster as KV & n1ql services are not found. Check cluster status")
				}
			} else {
				return fmt.Errorf("Error while initialising cluster as there are more nodes in status than expected. Check cluster status")
			}
			log.Printf("Successfully initialised cluster")
		} else {
			return fmt.Errorf("Error while initialising cluster as one of Nodes[1], Nodes[2] is not index. Check cluster status")
		}
	}
	status = getClusterStatus()
	log.Printf("Cluster status: %v", status)
	return nil
}
