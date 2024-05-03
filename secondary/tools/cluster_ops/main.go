package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	cluster "github.com/couchbase/indexing/secondary/tests/framework/clusterutility"
)

var Nodes = []string{
	"127.0.0.1:9000",
	"127.0.0.1:9001",
	"127.0.0.1:9002",
	"127.0.0.1:9003",
	"127.0.0.1:9004",
	"127.0.0.1:9005",
	"127.0.0.1:9006",
	"127.0.0.1:9007",
	"127.0.0.1:9008",
	"127.0.0.1:9009"}

type strSlice []string

func (slice *strSlice) String() string {
	var str string
	for _, val := range *slice {
		str += val + " "
	}
	return str
}

func (slice *strSlice) Set(value string) error {
	*slice = append(*slice, value)
	return nil
}

var options struct {
	username string
	password string

	// Number of nodes to initialise the cluster with. Default is 3 nodes
	numNodes int

	// Initial set of nodes in the cluster. Currently only Nodes[0] hosts kv+n1ql services
	// All other nodes in the cluster host index service
	Nodes strSlice

	// Options to add, eject or remove nodes
	// Currently, addNodes supports addition of only index service
	addNodes    strSlice // Does addition of nodes to the cluster. Does not trigger rebalance
	ejectNodes  strSlice // Does failover of nodes in the cluster. Does not trigger rebalance
	removeNodes strSlice // Does removal of nodes in the cluster. Does not trigger rebalance

	rebalance bool
}

func argParse() {

	flag.StringVar(&options.username, "username", "Administrator",
		"Cluster username")
	flag.StringVar(&options.password, "password", "asdasd",
		"Cluster password")
	flag.IntVar(&options.numNodes, "numNodes", 3,
		"Number of nodes to initialise the cluster with. Defaults to 3 (1 KV + N1QL, 2 index nodes")
	flag.Var(&options.Nodes, "Nodes",
		"Initial set of nodes to be present in the cluster. Only Nodes[0] contains kv+n1ql "+
			"services. All other nodes are initialised for index service")
	flag.Var(&options.addNodes, "addNodes",
		"Nodes that should be added to the cluster (No rebalance is performed)")
	flag.Var(&options.ejectNodes, "ejectNodes",
		"Nodes that should be failed over from the cluster (No rebalance is performed)")
	flag.Var(&options.removeNodes, "removeNodes",
		"Nodes that should be removed out of the cluster (No rebalance is performed)")
	flag.BoolVar(&options.rebalance, "rebalance", true,
		"Performs rebalance on the cluster")
	flag.Parse()

	// Update options.Nodes according to numNodes if Nodes is not set explicitly
	if len(options.Nodes) == 0 {
		for i := 0; i < options.numNodes; i++ {
			options.Nodes.Set(Nodes[i])
		}
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] <addr> \n", os.Args[0])
	// TODO: Add some examples
	flag.PrintDefaults()
}

func main() {
	argParse()
	fmt.Printf("%+v", options)

	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	if err := InitClusterFromREST(); err != nil {
		log.Fatalf("Error while initialising cluster, err: %v", err)
		return
	}

	if len(options.addNodes) > 0 {
		if err := addNodesIfNotPresent(options.addNodes); err != nil {
			return
		}
	} else if len(options.ejectNodes) > 0 {
		if err := ejectNodesIfPresent(options.ejectNodes); err != nil {
			return
		}
	} else if len(options.removeNodes) > 0 {
		// TODO: Remove nodes performs rebalance irrespective of options.rebalance
		// Need to fix this
		if err := removeNodesIfPresent(options.removeNodes); err != nil {
			return
		}
	} else if options.rebalance {
		if err := cluster.Rebalance(options.Nodes[0], options.username, options.password); err != nil {
			log.Fatalf("Error while rebalancing the cluster, err: %v", err)
			return
		}
	}
}

func addNodesIfNotPresent(addNodes strSlice) error {
	status := getClusterStatus()
	nodeAdded := false
	for i := 0; i < len(addNodes); i++ {
		if _, ok := status[addNodes[i]]; !ok {
			serverGroup := "Group 1"
			if i%2 == 1 { //All odd nodes go to "Group 2"
				serverGroup = "Group 2"
			}
			if err := cluster.AddNodeWithServerGroup(options.Nodes[0], options.username, options.password, addNodes[i], "index", serverGroup); err != nil {
				log.Fatalf("Error while adding node: %v (serverGroup: %v) to cluster, err: %v", addNodes[i], serverGroup, err)
				return err
			}
			nodeAdded = true
		} else {
			log.Printf("Node: %v already a part of cluster", addNodes[i])
			continue
		}
	}

	time.Sleep(100 * time.Millisecond)
	if options.rebalance && nodeAdded {
		// Rebalance the cluster
		if err := cluster.Rebalance(options.Nodes[0], options.username, options.password); err != nil {
			log.Fatalf("Error while rebalancing  after adding nodes: %v to cluster, err: %v", addNodes, err)
			return err
		}
	}
	return nil
}

func ejectNodesIfPresent(ejectNodes strSlice) error {
	status := getClusterStatus()
	nodeFailedOver := false
	for i := 0; i < len(ejectNodes); i++ {
		if _, ok := status[ejectNodes[i]]; ok {
			if err := cluster.FailoverNode(options.Nodes[0], options.username, options.password, ejectNodes[i]); err != nil {
				log.Fatalf("Error while failing over node: %v to cluster, err: %v", ejectNodes[i], err)
				return err
			}
			nodeFailedOver = true
		} else {
			log.Printf("Node: %v not a part of cluster", ejectNodes[i])
			continue
		}
	}

	time.Sleep(100 * time.Millisecond)
	if options.rebalance && nodeFailedOver {
		// Rebalance the cluster
		if err := cluster.Rebalance(options.Nodes[0], options.username, options.password); err != nil {
			log.Fatalf("Error while rebalancing  after ejecting nodes: %v to cluster, err: %v", ejectNodes, err)
			return err
		}
	}
	return nil
}

func removeNodesIfPresent(removeNodes strSlice) error {
	status := getClusterStatus()
	nodeRemoved := false
	for i := 0; i < len(removeNodes); i++ {
		if _, ok := status[removeNodes[i]]; ok {
			if err := cluster.RemoveNode(options.Nodes[0], options.username, options.password, removeNodes[i]); err != nil {
				log.Fatalf("Error while removing over node: %v to cluster, err: %v", removeNodes[i], err)
				return err
			}
			nodeRemoved = true
		} else {
			log.Printf("Node: %v not a part of cluster", removeNodes[i])
			continue
		}
	}

	time.Sleep(100 * time.Millisecond)
	if options.rebalance && nodeRemoved {
		// Rebalance the cluster
		if err := cluster.Rebalance(options.Nodes[0], options.username, options.password); err != nil {
			log.Fatalf("Error while rebalancing  after removing nodes: %v to cluster, err: %v", removeNodes, err)
			return err
		}
	}
	return nil
}
