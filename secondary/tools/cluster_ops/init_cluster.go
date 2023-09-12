package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	cluster "github.com/couchbase/indexing/secondary/tests/framework/clusterutility"
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
		if err := cluster.InitDataAndIndexQuota(serverAddr, username, password, "1500", "256"); err != nil {
			return err
		}
		time.Sleep(100 * time.Millisecond)

		// ServerGroup "Group 1" already exits by default. Create "Group 2"
		if err := cluster.AddServerGroup(serverAddr, username, password, "Group 2"); err != nil {
			return err
		}
		time.Sleep(100 * time.Millisecond)

		if err := cluster.AddNodeWithServerGroup(serverAddr, username, password, options.Nodes[1], "index", "Group 2"); err != nil {
			return err
		}
		time.Sleep(100 * time.Millisecond)
		if err := cluster.AddNodeWithServerGroup(serverAddr, username, password, options.Nodes[2], "index", "Group 1"); err != nil {
			return err
		}
		time.Sleep(100 * time.Millisecond)

		// Rebalance the cluster
		if err := cluster.Rebalance(serverAddr, username, password); err != nil {
			return err
		}
		CreateBucket("default", "sasl", "", username, password, serverAddr, "1500", "11213")
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

		err := setStorageModeAndShardAwareRebalance("plasma", username, password)
		if err != nil {
			log.Fatalf("Error in setting storage mode to plasma")
		}
	}
	status = getClusterStatus()
	log.Printf("Cluster status: %v", status)

	return nil
}

func setStorageModeAndShardAwareRebalance(storageMode, username, password string) error {
	client := http.Client{}
	// TODO: This is a bad way of doing things. Use Nodes value and extract
	// correct indexer address to set storage mode. Current logic assumes node
	// with server port 9001 is always available when initialising cluster
	url := "http://127.0.0.1:9108/internal/settings"

	log.Printf("Changing storage mode value %v\n", storageMode)
	jbody := make(map[string]interface{})
	jbody["indexer.settings.storage_mode"] = storageMode
	jbody["indexer.rebalance.shard_aware_rebalance"] = true
	jbody["indexer.settings.rebalance.blob_storage_bucket"] = "/tmp/"
	jbody["indexer.settings.enableShardAffinity"] = true
	pbody, err := json.Marshal(jbody)
	if err != nil {
		return err
	}
	preq, err := http.NewRequest("POST", url, bytes.NewBuffer(pbody))
	preq.SetBasicAuth(username, password)

	resp, err := client.Do(preq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	ioutil.ReadAll(resp.Body)

	return nil
}

func CreateBucket(bucketName, authenticationType, saslBucketPassword, serverUserName, serverPassword, hostaddress, bucketRamQuota, proxyPort string) {
	client := &http.Client{}
	address := "http://" + hostaddress + "/pools/default/buckets"
	data := url.Values{"name": {bucketName}, "ramQuotaMB": {bucketRamQuota}, "flushEnabled": {"1"}, "replicaNumber": {"1"}}
	req, _ := http.NewRequest("POST", address, strings.NewReader(data.Encode()))
	req.SetBasicAuth(serverUserName, serverPassword)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	resp, err := client.Do(req)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		log.Printf(address)
		log.Printf("%v", req)
		log.Printf("%v", resp)
		log.Printf("CreateBucket failed for bucket %v \n", bucketName)
	}
	// todo : error out if response is error
	if err != nil {
		log.Fatalf("Error while creating bucket, err: %v", err)
	}
	defer resp.Body.Close()
	responseBody, _ := ioutil.ReadAll(resp.Body)
	log.Printf("Created bucket %v, responseBody: %s", bucketName, responseBody)
}
