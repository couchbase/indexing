package main

import (
	"fmt"
	"log"
	"time"

	cluster "github.com/couchbase/indexing/secondary/tests/framework/clusterutility"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
)

func getClusterStatus() map[string][]string {
	serverAddr := options.Nodes[0]
	username := options.username
	password := options.password

	return cluster.GetClusterStatus(serverAddr, username, password)
}

func InitClusterFromREST() error {
	status := getClusterStatus()
	serverAddr := options.Nodes[0]
	username := options.username
	password := options.password

	// Initialise an un-initialised cluster. If cluster is already initialised,
	// then do not touch it
	if len(status) == 0 {
		if err := cluster.InitClusterServices(serverAddr, username, password, "kv,n1ql"); err != nil {
			return err
		}
		time.Sleep(100 * time.Millisecond)
		if err := cluster.InitWebCreds(serverAddr, username, password); err != nil {
			return err
		}

		time.Sleep(100 * time.Millisecond)
		if err := cluster.InitDataAndIndexQuota(serverAddr, username, password); err != nil {
			return err
		}

		time.Sleep(100 * time.Millisecond)
		if err := cluster.AddNode(serverAddr, username, password, options.Nodes[1], "index"); err != nil {
			return err
		}
		time.Sleep(100 * time.Millisecond)
		if err := cluster.AddNode(serverAddr, username, password, options.Nodes[2], "index"); err != nil {
			return err
		}
		time.Sleep(100 * time.Millisecond)
		// Rebalance the cluster
		if err := cluster.Rebalance(serverAddr, username, password); err != nil {
			return err
		}
		kvutility.CreateBucket("default", "sasl", "", username, password, serverAddr, "1500", "11213")
		time.Sleep(1 * time.Second)
		status = getClusterStatus()
		log.Printf("Cluster status: %v", status)
		if cluster.IsNodeIndex(status, options.Nodes[1]) && cluster.IsNodeIndex(status, options.Nodes[2]) {
			// The IP address of Nodes[0] can get renamed. Until we figure out
			// how to disable the re-naming (like --dont-rename in cluster_connect)
			delete(status, options.Nodes[1])
			delete(status, options.Nodes[2])
			// ignore the hostname for Nodes[0] and only check for services
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
					return fmt.Errorf("Error while initialising cluster. Check cluster status")
				}
			} else {
				return fmt.Errorf("Error while initialising cluster. Check cluster status")
			}
			log.Printf("Successfully initialised cluster")
		} else {
			return fmt.Errorf("Error while initialising cluster. Check cluster status")
		}
	}
	status = getClusterStatus()
	log.Printf("Cluster status: %v", status)
	return nil
}
