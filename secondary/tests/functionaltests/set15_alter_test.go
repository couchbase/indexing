package functionaltests

import (
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"

	"fmt"
	"log"
	"testing"
	"time"
)

func TestAlterIndexIncrReplica(t *testing.T) {
	if !clusterconfig.MultipleIndexerTests || len(clusterconfig.Nodes) < 4 {
		return
	}

	log.Printf("In TestAlterIndexIncrReplica()")
	log.Printf("This test creates an index with one replica and then increments replica count to 2")

	if err := validateServers(clusterconfig.Nodes[0:3]); err != nil {
		t.Fatalf("Error while validating cluster, err: %v", err)
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
	indexName := "idx_1"
	num_docs := 100
	num_scans := 100

	kvutility.FlushBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)

	kvdocs := generateDocs(num_docs, "users_simplearray.prod")
	kvutility.SetKeyValues(kvdocs, bucketName, "", clusterconfig.KVAddress)

	// Create index on age
	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"age"}, false, []byte("{\"num_replica\":1}"), true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	// Now, increment the replica count
	n1qlstatement := "alter index `" + bucketName + "`." + indexName + " with {\"action\":\"replica_count\", \"num_replica\":2}"
	log.Printf("Executing alter index command: %v", n1qlstatement)
	_, err = tc.ExecuteN1QLStatement(kvaddress, clusterconfig.Username, clusterconfig.Password, bucketName, n1qlstatement, false, nil)
	FailTestIfError(err, "Error in executing n1ql statement", t)

	// Wait for alter index to finish execution
	waitForIndexActive(bucketName, indexName+" (replica 2)", t)

	waitForStatsUpdate()

	scanIndexReplicas(indexName, bucketName, []int{0, 1, 2}, num_scans, num_docs, 1, t)
}

func TestAlterIndexDecrReplica(t *testing.T) {
	if !clusterconfig.MultipleIndexerTests || len(clusterconfig.Nodes) < 4 {
		return
	}

	log.Printf("In TestAlterIndexDecrReplica()")
	log.Printf("This test creates an index with two replicas and then decrements replica count to 1")

	if err := validateServers(clusterconfig.Nodes[0:3]); err != nil {
		t.Fatalf("Error while validating cluster, err: %v", err)
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
	indexName := "idx_2"
	num_docs := 100
	num_scans := 100

	// Create index on age
	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"age"}, false, []byte("{\"num_replica\":2}"), true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	// Decrement the replica count
	n1qlstatement := "alter index `" + bucketName + "`." + indexName + " with {\"action\":\"replica_count\", \"num_replica\":1}"
	log.Printf("Executing alter index command: %v", n1qlstatement)
	_, err = tc.ExecuteN1QLStatement(kvaddress, clusterconfig.Username, clusterconfig.Password, bucketName, n1qlstatement, false, nil)
	FailTestIfError(err, "Error in executing n1ql statement", t)

	// Wait for the replica index to get dropped
	if waitForReplicaDrop(indexName, bucketName, 2) {
		t.Fatalf("Replica: 2 is expected to be dropped. But it still exists")
	}

	waitForStatsUpdate()

	scanIndexReplicas(indexName, bucketName, []int{0, 1}, num_scans, num_docs, 1, t)
}

func TestAlterIndexDropReplica(t *testing.T) {
	if !clusterconfig.MultipleIndexerTests || len(clusterconfig.Nodes) < 4 {
		return
	}

	log.Printf("In TestAlterIndexDropReplica()")
	log.Printf("This test creates an index with two replicas and then drops one replica from cluster")

	if err := validateServers(clusterconfig.Nodes[0:3]); err != nil {
		t.Fatalf("Error while validating cluster, err: %v", err)
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
	indexName := "idx_3"
	num_docs := 100
	num_scans := 100

	// Create index on age
	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"age"}, false, []byte("{\"num_replica\":2}"), true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	// Drop the index 'idx_3'
	n1qlstatement := "alter index `" + bucketName + "`." + indexName + " with {\"action\":\"drop_replica\", \"replicaId\":0}"
	log.Printf("Executing alter index command: %v", n1qlstatement)
	_, err = tc.ExecuteN1QLStatement(kvaddress, clusterconfig.Username, clusterconfig.Password, bucketName, n1qlstatement, false, nil)
	FailTestIfError(err, "Error in executing n1ql statement", t)

	// Wait for the replica to get dropped
	if waitForReplicaDrop(indexName, bucketName, 0) {
		t.Fatalf("Replica: 0 is expected to be dropped. But it still exists")
	}

	waitForStatsUpdate()

	scanIndexReplicas(indexName, bucketName, []int{1, 2}, num_scans, num_docs, 1, t)
}

// Make sure that we reset cluster at the end of all the tests in this file
func TestResetCluster_1(t *testing.T) {
	if !clusterconfig.MultipleIndexerTests || len(clusterconfig.Nodes) < 4 {
		return
	}
	resetCluster(t)
}

// Helper function that can be used to verify whether an index is dropped or not
// for alter index decrement replica count, alter index drop
func waitForReplicaDrop(index, bucket string, replicaId int) bool {
	ticker := time.NewTicker(5 * time.Second)
	indexName := index
	if replicaId != 0 {
		indexName = fmt.Sprintf("%v (replica %v)", index, replicaId)
	}
	// Wait for 2 minutes max for the replica to get dropped
	for {
		select {
		case <-ticker.C:
			stats := secondaryindex.GetIndexStats(indexName, bucket, clusterconfig.Username, clusterconfig.Password, kvaddress)
			if stats != nil {
				if _, ok := stats[bucket+":"+indexName+":items_count"]; ok {
					continue
				}
				return false
			} else {
				log.Printf("waitForReplicaDrop:: Unable to retrieve index stats for index: %v", indexName)
				return true
			}

		case <-time.After(2 * time.Minute):
			log.Printf("waitForReplicaDrop:: Index replica %v exists even after 2 minutes", indexName)
			return true
		}
	}
	return false
}
