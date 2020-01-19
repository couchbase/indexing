package functionaltests

import (
	"fmt"
	"log"
	"testing"
	"time"

	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
)

func TestPartitionDistributionWithReplica(t *testing.T) {
	if clusterconfig.IndexUsing == "forestdb" {
		log.Printf("Not running TestPartitionDistributionWithReplica for forestdb as" +
			" partition indexes are not supported with forestdb storage mode")
		return
	}

	if !clusterconfig.MultipleIndexerTests || len(clusterconfig.Nodes) < 4 {
		return
	}

	log.Printf("In TestPartitionDistributionWithReplica()")
	log.Printf("This test will create a paritioned index with replica and checks the parition distribution")
	log.Printf("Parititions with same ID beloning to both replica and source index should not be on the same node")

	if err := validateServers(clusterconfig.Nodes); err != nil {
		log.Printf("Error while validating cluster, err: %v", err)
		log.Printf("Considering the test successful")
		return
	}

	if !is4NodeCluster() {
		init4NodeCluster(t)
	}

	// Drop all seconday indexes so that the test can be validated
	// by comparing the scan related stats
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)
	time.Sleep(10 * time.Second)

	var bucketName = "default"
	indexName := "idx_partn"
	num_partition := 8
	num_docs := 100
	num_scans := 100

	kvutility.FlushBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)

	kvdocs := generateDocs(num_docs, "users_simplearray.prod")
	kvutility.SetKeyValues(kvdocs, bucketName, "", clusterconfig.KVAddress)

	// Create a paritioned index on age
	n1qlstatement := fmt.Sprintf("create index `%v` on `%v`(age) partition by hash(meta().id) "+
		"with {\"num_partition\":%v, \"num_replica\":1}", indexName, bucketName, num_partition)

	log.Printf("Executing create partition index command on: %v", n1qlstatement)
	_, err := tc.ExecuteN1QLStatement(kvaddress, clusterconfig.Username, clusterconfig.Password, bucketName, n1qlstatement, false, nil)
	FailTestIfError(err, "Error in executing n1ql statement", t)

	// Wait for index and its replica build to finish
	waitForIndexActive(bucketName, indexName, t)
	waitForIndexActive(bucketName, indexName+" (replica 1)", t)

	partnMapRepl_0 := getPartnDist(indexName, bucketName)
	validatePartnCount(partnMapRepl_0, num_partition, t)

	partnMapRepl_1 := getPartnDist(indexName+" (replica 1)", bucketName)
	validatePartnCount(partnMapRepl_1, num_partition, t)

	for node, partnPerNodeRepl_0 := range partnMapRepl_0 {
		// Get the partition map for repl_1 for this node
		if partnPerNodeRepl_1, ok := partnMapRepl_1[node]; ok {
			for partn, _ := range partnPerNodeRepl_0 {
				if _, ok := partnPerNodeRepl_1[partn]; ok {
					t.Fatalf("Unexpected partition placement. The partition: %v is on the same node"+
						" for both replica_0 and replica_1.\nIndex name: %v\nPartion placement for replica_0: %v\n, for replica_1: %v\n",
						partn, indexName, partnMapRepl_0, partnMapRepl_1)
				}
			}
		}
	}

	// Scan the partitioned index and its replica
	scanIndexReplicas(indexName, bucketName, []int{0, 1}, num_scans, num_docs, num_partition, t)
}

func getPartnDist(index, bucket string) map[string]map[int]bool {
	partnMap := make(map[string]map[int]bool)
	status, err := secondaryindex.GetIndexStatus(clusterconfig.Username, clusterconfig.Password, kvaddress)
	if status != nil && err == nil {
		indexes := status["indexes"].([]interface{})
		for _, indexEntry := range indexes {
			entry := indexEntry.(map[string]interface{})
			if index == entry["index"].(string) && bucket == entry["bucket"].(string) {
				pMap := entry["partitionMap"].(map[string]interface{})
				for node, partns := range pMap {
					dist := partns.([]interface{})
					partnMap[node] = make(map[int]bool, 0)
					for _, partnId := range dist {
						partnMap[node][(int)(partnId.(float64))] = true
					}

				}
			}
		}
	}
	if err != nil {
		log.Printf("getPartnDist:: Error while retrieving IndexStatus, err: %v", err)
	}
	return partnMap
}

func validatePartnCount(partnMap map[string]map[int]bool, numPartitions int, t *testing.T) {
	actualPartns := 0
	for _, partns := range partnMap {
		actualPartns += len(partns)
	}

	if actualPartns != numPartitions {
		t.Fatalf("Unexpected number of partitions. Expected %v partitions across all nodes."+
			" Actual: %v, Partition map: %+v", numPartitions, actualPartns, partnMap)
	}
}

func TestResetCluster_2(t *testing.T) {
	if !clusterconfig.MultipleIndexerTests || len(clusterconfig.Nodes) < 4 {
		return
	}
	resetCluster(t)
}
