package functionaltests

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"testing"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
)

func TestPartitionDistributionWithReplica(t *testing.T) {
	t.Skipf("Skipping TestPartitionDistributionWithReplica(). Please enable this in future...")
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

	if err := validateServers(clusterconfig.Nodes[0:3]); err != nil {
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

	waitForStatsUpdate()

	// Scan the partitioned index and its replica
	scanIndexReplicas(indexName, bucketName, []int{0, 1}, num_scans, num_docs, num_partition, t)
}

func TestPartitionedPartialIndex(t *testing.T) {

	if clusterconfig.IndexUsing == "forestdb" {
		log.Printf("Not running TestPartitionedPartialIndex for forestdb as" +
			" partition indexes are not supported with forestdb storage mode")
		return
	}

	var bucketName = "default"
	idx1, idx2, idx3, idx4 := "idx_regular", "idx_partial", "idx_partitioned", "idx_partitioned_partial"
	field_name, field_age := "partn_name", "partn_age"

	docids := make(map[string]bool, 0)
	count := 100
	docsToUpload := make(tc.KeyValues)
	for i := 0; i < count; i++ {
		key := "key_partn_partial_" + strconv.Itoa(i)
		docids[key] = true
		json := make(map[string]interface{})
		json[field_name] = "username" + strconv.Itoa(i)
		json[field_age] = i
		docsToUpload[key] = json
	}
	kvutility.SetKeyValues(docsToUpload, bucketName, "", clusterconfig.KVAddress)

	mutateDoc := func(docid int, nameVal string, ageVal int) {
		muts := make(tc.KeyValues)
		key := "key_partn_partial_" + strconv.Itoa(docid)
		json := make(map[string]interface{})
		json[field_name] = nameVal
		json[field_age] = ageVal
		muts[key] = json
		docsToUpload[key] = json
		kvutility.SetKeyValues(muts, bucketName, "", clusterconfig.KVAddress)
	}

	deleteDoc := func(docid int) {
		key := "key_partn_partial_" + strconv.Itoa(docid)
		kvutility.Delete(key, bucketName, "", clusterconfig.KVAddress)
		delete(docsToUpload, key)
		delete(docids, key)
	}

	scanAllAndVerify := func(indexName string, expectedCount int) {
		scanResults, err := secondaryindex.ScanAll(indexName, bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
		FailTestIfError(err, "Error in ScanAll of TestPartitionedPartialIndex", t)
		if len(scanResults) != expectedCount {
			e := errors.New(fmt.Sprintf("Expected scanAll for index %v count %d "+
				"does not match actual scanAll count %d: ", indexName, expectedCount, len(scanResults)))
			FailTestIfError(e, "Error in TestPartitionedPartialIndex", t)
		}
	}

	createIndex := func(n1qlstatement string) {
		log.Printf("Executing create index command: %v", n1qlstatement)
		_, err := tc.ExecuteN1QLStatement(kvaddress, clusterconfig.Username, clusterconfig.Password, bucketName, n1qlstatement, false, nil)
		FailTestIfError(err, fmt.Sprintf("Error in executing n1ql statement %v", n1qlstatement), t)
	}

	// TEST 1: Regular index: inserts, updates, deletes (immutable flag from user true and false cannot be tested as it is disabled)
	n1qlstatement := fmt.Sprintf("CREATE INDEX `%v` ON `%v`(%v)", idx1, bucketName, field_name)
	createIndex(n1qlstatement)
	scanAllAndVerify(idx1, count)
	err := secondaryindex.DropSecondaryIndex(idx1, bucketName, indexManagementAddress)
	FailTestIfError(err, "Error dropping index idx1", t)

	// TEST 2: Partial index: inserts, updates, deletes. where clause evaluating to true and false
	// (immutable flag from user true and false cannot be tested as it is disabled)
	n1qlstatement = fmt.Sprintf("CREATE INDEX `%v` ON `%v`(%v) WHERE %v >= 0", idx2, bucketName, field_name, field_age)
	createIndex(n1qlstatement)

	scanAllAndVerify(idx2, count)

	mutateDoc(0, "username"+strconv.Itoa(0)+"_00", 0)
	scanAllAndVerify(idx2, count)

	mutateDoc(1, "username"+strconv.Itoa(1)+"_01", -5) // WHERE clause evaluates to false
	scanAllAndVerify(idx2, count-1)

	err = secondaryindex.DropSecondaryIndex(idx2, bucketName, indexManagementAddress)
	FailTestIfError(err, "Error dropping index idx2", t)
	kvutility.SetKeyValues(docsToUpload, "default", "", clusterconfig.KVAddress)

	// TEST 3: Partitioned index: inserts, updates, deletes
	// (immutable flag from user true and false cannot be tested as it is disabled)
	n1qlstatement = fmt.Sprintf("CREATE INDEX `%v` ON `%v`(%v) PARTITION BY HASH(meta().id) ", idx3, bucketName, field_name)
	createIndex(n1qlstatement)
	scanAllAndVerify(idx3, count)

	mutateDoc(1, "username"+strconv.Itoa(1)+"_01", 1)
	scanAllAndVerify(idx3, count)

	err = secondaryindex.DropSecondaryIndex(idx3, bucketName, indexManagementAddress)
	FailTestIfError(err, "Error dropping index idx3", t)
	kvutility.SetKeyValues(docsToUpload, "default", "", clusterconfig.KVAddress)

	// TEST 4: Partitioned + Partial index: inserts, updates, deletes. where clause evaluating to true and false
	// (immutable flag from user true and false cannot be tested as it is disabled)
	n1qlstatement = fmt.Sprintf("CREATE INDEX `%v` ON `%v`(%v) PARTITION BY HASH(meta().id) "+
		"WHERE %v >= 0", idx4, bucketName, field_name, field_age)
	createIndex(n1qlstatement)
	scanAllAndVerify(idx4, count)

	mutateDoc(1, "username"+strconv.Itoa(1)+"_01", 1)
	scanAllAndVerify(idx4, count)

	mutateDoc(0, "username"+strconv.Itoa(0)+"_00", -5) // WHERE clause evaluates to false
	scanAllAndVerify(idx4, count-1)

	deleteDoc(10)
	scanAllAndVerify(idx4, count-2)

	err = secondaryindex.DropSecondaryIndex(idx4, bucketName, indexManagementAddress)
	FailTestIfError(err, "Error dropping index idx4", t)
	for key, _ := range docids {
		kvutility.Delete(key, bucketName, "", clusterconfig.KVAddress)
	}
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
