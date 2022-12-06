package serverlesstests

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
)

// TODO:Add special characters to bucket names
var buckets []string = []string{"bucket_1", "bucket_2"}
var collections []string = []string{"_default", "c1", "c2"}
var scope string = "_default"

// TODO: Add "#" character to primary index name
var indexes []string = []string{"idx_secondary", "idx_secondary_defer", "primary", "primary_defer", "idx_partitioned", "idx_partitioned_defer"}
var indexPartnIds [][]int = [][]int{[]int{0}, []int{0}, []int{0}, []int{0}, []int{1, 2, 3, 4, 5, 6, 7, 8}, []int{1, 2, 3, 4, 5, 6, 7, 8}}
var numDocs int = 100
var numScans int = 100

// When creating an index through N1QL, the index is expected
// to be created with a replica
func TestIndexPlacement(t *testing.T) {
	bucket := "bucket_1"
	scope := "_default"
	collection := "c1"
	index := "idx_1"

	// Create Bucket
	kvutility.CreateBucket(bucket, "sasl", "", clusterconfig.Username, clusterconfig.Password, clusterconfig.KVAddress, "256", "11213")
	kvutility.WaitForBucketCreation(bucket, clusterconfig.Username, clusterconfig.Password, []string{clusterconfig.Nodes[0], clusterconfig.Nodes[1], clusterconfig.Nodes[2]})

	manifest := kvutility.CreateCollection(bucket, scope, collection, clusterconfig.Username, clusterconfig.Password, clusterconfig.KVAddress)
	log.Printf("TestIndexPlacement: Manifest for bucket: %v, scope: %v, collection: %v is: %v", bucket, scope, collection, manifest)
	cid := kvutility.WaitForCollectionCreation(bucket, scope, collection, clusterconfig.Username, clusterconfig.Password, []string{clusterconfig.Nodes[0], clusterconfig.Nodes[1], clusterconfig.Nodes[2]}, manifest)

	CreateDocsForCollection(bucket, cid, numDocs)

	n1qlStatement := fmt.Sprintf("create index %v on `%v`.`%v`.`%v`(company)", index, bucket, scope, collection)
	execN1qlAndWaitForStatus(n1qlStatement, bucket, scope, collection, index, "Ready", t)

	waitForStatsUpdate()

	// Scan the index
	scanIndexReplicas(index, bucket, scope, collection, []int{0, 1}, numScans, numDocs, 1, t)
	kvutility.DeleteBucket(bucket, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	time.Sleep(bucketOpWaitDur * time.Second)

	cleanupShardDir(t)
}

// This test creates a variety of indexes on 2 buckets, 3 collections per bucket
// and checks if all indexes are mapped to same shards
func TestShardIdMapping(t *testing.T) {

	for _, bucket := range buckets {
		kvutility.CreateBucket(bucket, "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "100", "11213")
		kvutility.WaitForBucketCreation(bucket, clusterconfig.Username, clusterconfig.Password, []string{clusterconfig.Nodes[0], clusterconfig.Nodes[1], clusterconfig.Nodes[2]})

		for _, collection := range collections {
			var cid string
			if collection != "_default" { // default collection always exists
				manifest := kvutility.CreateCollection(bucket, scope, collection, clusterconfig.Username, clusterconfig.Password, clusterconfig.KVAddress)
				log.Printf("TestIndexPlacement: Manifest for bucket: %v, scope: %v, collection: %v is: %v", bucket, scope, collection, manifest)
				cid = kvutility.WaitForCollectionCreation(bucket, scope, collection, clusterconfig.Username, clusterconfig.Password, []string{clusterconfig.Nodes[0], clusterconfig.Nodes[1], clusterconfig.Nodes[2]}, manifest)
			} else {
				cid = "0" // CID of default collection
			}
			CreateDocsForCollection(bucket, cid, numDocs)

			createAllIndexes(bucket, scope, collection, t)
		}
	}

	validateShardIdMapping(clusterconfig.Nodes[1], t)
	validateShardIdMapping(clusterconfig.Nodes[2], t)
	waitForStatsUpdate()

	for _, bucket := range buckets {
		for _, collection := range collections {
			for i, index := range indexes {
				partns := indexPartnIds[i]
				if i%2 == 0 { // Scan all non-deferred indexes
					scanIndexReplicas(index, bucket, scope, collection, []int{0, 1}, numScans, numDocs, len(partns), t)
				}
			}
		}
	}
}

func createAllIndexes(bucket, scope, collection string, t *testing.T) {
	// Create a variety of indexes on each bucket

	// For local testing, if only a few indexes are required in the test (say only indexes[0:2]),
	// then these "if" conditions based on length checks will help to filter the unused indexes
	// from creation. Note that the order of "indexes" should not change for this code to work
	// Otherwise, indexes will be created with wrong names
	if len(indexes) > 0 {
		// Create a normal index
		n1qlStatement := fmt.Sprintf("create index %v on `%v`.`%v`.`%v`(age)", indexes[0], bucket, scope, collection)
		execN1qlAndWaitForStatus(n1qlStatement, bucket, scope, collection, indexes[0], "Ready", t)
	}

	if len(indexes) > 1 {
		// Create an index with defer_build
		n1qlStatement := fmt.Sprintf("create index %v on `%v`.`%v`.`%v`(company) with {\"defer_build\":true}", indexes[1], bucket, scope, collection)
		execN1qlAndWaitForStatus(n1qlStatement, bucket, scope, collection, indexes[1], "Created", t)
	}

	if len(indexes) > 2 {
		// Create a primary index
		n1qlStatement := fmt.Sprintf("create primary index `%v` on `%v`.`%v`.`%v`", indexes[2], bucket, scope, collection)
		execN1qlAndWaitForStatus(n1qlStatement, bucket, scope, collection, indexes[2], "Ready", t)
	}

	if len(indexes) > 3 {
		// Create a primary index with defer_build:true
		n1qlStatement := fmt.Sprintf("create primary index `%v` on `%v`.`%v`.`%v` with {\"defer_build\":true}", indexes[3], bucket, scope, collection)
		execN1qlAndWaitForStatus(n1qlStatement, bucket, scope, collection, indexes[3], "Created", t)
	}

	if len(indexes) > 4 {
		// Create a partitioned index
		n1qlStatement := fmt.Sprintf("create index %v on `%v`.`%v`.`%v`(emalid) partition by hash(meta().id)", indexes[4], bucket, scope, collection)
		execN1qlAndWaitForStatus(n1qlStatement, bucket, scope, collection, indexes[4], "Ready", t)
	}

	if len(indexes) > 5 {
		// Create a partitioned index with defer_build:true
		n1qlStatement := fmt.Sprintf("create index %v on `%v`.`%v`.`%v`(balance) partition by hash(meta().id)  with {\"defer_build\":true}", indexes[5], bucket, scope, collection)
		execN1qlAndWaitForStatus(n1qlStatement, bucket, scope, collection, indexes[5], "Created", t)
	}
}

// Helper function that can be used to verify whether an index is dropped or not
// for alter index decrement replica count, alter index drop
func waitForReplicaDrop(index, bucket, scope, collection string, replicaId int, t *testing.T) {
	ticker := time.NewTicker(5 * time.Second)
	indexName := index
	if replicaId != 0 {
		indexName = fmt.Sprintf("%v (replica %v)", index, replicaId)
	}
	// Wait for 2 minutes max for the replica to get dropped
	for {
		select {
		case <-ticker.C:
			stats := secondaryindex.GetIndexStats2(indexName, bucket, scope, collection, clusterconfig.Username, clusterconfig.Password, kvaddress)
			if stats != nil {
				if collection != "_default" {
					if _, ok := stats[bucket+":"+scope+":"+collection+":"+indexName+":items_count"]; ok {
						continue
					}
				} else {
					if _, ok := stats[bucket+":"+indexName+":items_count"]; ok {
						continue
					}
				}
				return
			} else {
				log.Printf("waitForReplicaDrop:: Unable to retrieve index stats for index: %v", indexName)
				return
			}

		case <-time.After(2 * time.Minute):
			t.Fatalf("waitForReplicaDrop:: Index replica %v exists even after 2 minutes", indexName)
			return
		}
	}
	return
}
