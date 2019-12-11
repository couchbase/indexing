package functionaltests

import (
	c "github.com/couchbase/indexing/secondary/common"
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

	if err := validateServers(clusterconfig.Nodes); err != nil {
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
	FailTestIfError(err, "Error in creating primary index", t)

	// Wait for alter index to finish execution
	waitForIndexActive(bucketName, indexName+" (replica 2)", t)

	scanIndexReplicas(indexName, bucketName, []int{0, 1, 2}, num_scans, num_docs, t)
}

func TestAlterIndexDecrReplica(t *testing.T) {
	if !clusterconfig.MultipleIndexerTests || len(clusterconfig.Nodes) < 4 {
		return
	}

	log.Printf("In TestAlterIndexDecrReplica()")
	log.Printf("This test creates an index with two replicas and then decrements replica count to 1")

	if err := validateServers(clusterconfig.Nodes); err != nil {
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
	FailTestIfError(err, "Error in creating primary index", t)
	// Wait for the index to get dropped
	time.Sleep(10 * time.Second)

	// Check if index exists after altering
	if checkIfReplicaExists(indexName, bucketName, 2) {
		t.Fatalf("Replica: 2 is expected to be dropped. But it still exists")
	}

	scanIndexReplicas(indexName, bucketName, []int{0, 1}, num_scans, num_docs, t)
}

// Make sure that we reset cluster at the end of all the tests in this file
func TestResetCluster_1(t *testing.T) {
	if !clusterconfig.MultipleIndexerTests || len(clusterconfig.Nodes) < 4 {
		return
	}
	resetCluster(t)
}

func waitForIndexActive(bucket, index string, t *testing.T) {
	for {
		select {
		case <-time.After(time.Duration(3 * time.Minute)):
			t.Fatalf("Index did not become active after 3 minutes")
			break
		default:
			status, err := secondaryindex.GetIndexStatus(clusterconfig.Username, clusterconfig.Password, kvaddress)
			if status != nil && err == nil {
				indexes := status["indexes"].([]interface{})
				for _, indexEntry := range indexes {
					entry := indexEntry.(map[string]interface{})

					if index == entry["index"].(string) {
						if bucket == entry["bucket"].(string) {
							if "Ready" == entry["status"].(string) {
								return
							}
						}
					}
				}
			}
			if err != nil {
				log.Printf("waitForIndexActive:: Error while retrieving GetIndexStatus, err: %v", err)
			}
			time.Sleep(1 * time.Second)
		}
	}
}

// Helper function that can be used to verify whether an index is dropped or not
// for alter index decrement replica count, alter index drop
func checkIfReplicaExists(index, bucket string, replicaId int) bool {
	indexName := fmt.Sprintf("%v (replica %v)", index, replicaId)
	status, err := secondaryindex.GetIndexStatus(clusterconfig.Username, clusterconfig.Password, kvaddress)
	if status != nil && err == nil {
		indexes := status["indexes"].([]interface{})
		for _, indexEntry := range indexes {
			entry := indexEntry.(map[string]interface{})
			if indexName == entry["index"].(string) {
				if bucket == entry["bucket"].(string) {
					return true
				}
			}
		}
	}
	if err != nil {
		log.Printf("checkIfReplicaExists:: Error while retrieving GetIndexStatus, err: %v", err)
	}
	return false
}

// scanIndexReplicas scan's the index and validates if all the replica's of the index are retruning
// valid results
func scanIndexReplicas(index, bucket string, replicaIds []int, numScans, numDocs int, t *testing.T) {
	// Scan the index num_scans times
	for i := 0; i < numScans; i++ {
		scanResults, err := secondaryindex.ScanAll(index, bucket, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
		if len(scanResults) != numDocs {
			errStr := fmt.Sprintf("Error in ScanAll. Expected len(scanResults): %v, actual: %v", numDocs, len(scanResults))
			FailTestIfError(err, errStr, t)
		}
	}

	stats := secondaryindex.GetStats(clusterconfig.Username, clusterconfig.Password, kvaddress)

	// construct corresponding replica strings's from replicaIds
	replicas := make([]string, len(replicaIds))
	for i, replicaId := range replicaIds {
		if replicaId == 0 {
			replicas[i] = ""
		} else {
			replicas[i] = fmt.Sprintf(" (replica %v)", replicaId)
		}
	}

	// For each index, get num_requests, num_scan_errors, num_scan_timeouts
	num_requests := make([]float64, len(replicas))
	num_scan_errors := 0.0
	num_scan_timeouts := 0.0

	for i := 0; i < len(replicas); i++ {
		num_requests[i] = stats[bucket+":"+index+replicas[i]+":num_requests"].(float64)
		num_scan_errors += stats[bucket+":"+index+replicas[i]+":num_scan_errors"].(float64)
		num_scan_timeouts += stats[bucket+":"+index+replicas[i]+":num_scan_timeouts"].(float64)
	}

	if num_scan_errors > 0 {
		t.Fatalf("Expected '0' scan errors. Found: %v scan errors", num_scan_errors)
	}

	if num_scan_timeouts > 0 {
		t.Fatalf("Expected '0' scan timeouts. Found: %v scan timeouts", num_scan_errors)
	}

	total_scan_requests := 0.0
	for i := 0; i < len(replicas); i++ {
		if num_requests[i] == 0 {
			t.Fatalf("Zero scan requests seen for index: %v", index+replicas[i])
		}
		total_scan_requests += num_requests[i]
	}
	if total_scan_requests != (float64)(numScans) {
		t.Fatalf("Total scan requests for all indexes does not match the total scans. Expected: %v, actual: %v", numScans, total_scan_requests)
	}
}
