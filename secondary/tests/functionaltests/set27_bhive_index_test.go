package functionaltests

import (
	"fmt"
	"log"
	"testing"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	qvalue "github.com/couchbase/query/value"
)

const numDocs int = 10000

func validateScanStats(bucket, index string, expectedNumRequests, expectedNumRowsReturned int64, t *testing.T) {
	// Get parititioned stats for the index from all nodes
	stats := secondaryindex.GetPerPartnStats(clusterconfig.Username, clusterconfig.Password, kvaddress)

	indexName := fmt.Sprintf("%s:%s", bucket, index)
	num_requests := stats[indexName+":num_requests"].(float64)
	num_scan_errors := stats[indexName+":num_scan_errors"].(float64)
	num_scan_timeouts := stats[indexName+":num_scan_timeouts"].(float64)
	num_rows_returned := stats[indexName+":num_rows_returned"].(float64)

	logStats := func() {
		log.Printf("ScanAllReplicas: Indexer stats are: %v", stats)
	}
	if num_scan_errors > 0 {
		logStats()
		t.Fatalf("Expected '0' scan errors. Found: %v scan errors", num_scan_errors)
	}

	if num_scan_timeouts > 0 {
		logStats()
		t.Fatalf("Expected '0' scan timeouts. Found: %v scan timeouts", num_scan_errors)
	}

	if num_requests != float64(expectedNumRequests) {
		logStats()
		t.Fatalf("Expected %v requests to the index. Found %v requests", expectedNumRequests, num_requests)
	}

	if num_rows_returned != float64(expectedNumRowsReturned) {
		logStats()
		t.Fatalf("Expected %v rows to be returned to the index. Found %v rows being returned", expectedNumRowsReturned, num_rows_returned)
	}
}

// This method validates if the scan stats are changed only for the "partns" in the input
// For other partitions, stats are not expected to change
func validateScanStatsForPartns(bucket, index string, expectedNumRequests map[int]int64, numPartition int, t *testing.T) {
	// Get parititioned stats for the index from all nodes
	stats := secondaryindex.GetPerPartnStats(clusterconfig.Username, clusterconfig.Password, kvaddress)

	for i := 1; i <= numPartition; i++ {

		indexName := fmt.Sprintf("%s:%s %v", bucket, index, i) // include partitionId as well
		num_requests := stats[indexName+":num_requests"].(float64)
		num_scan_errors := stats[indexName+":num_scan_errors"].(float64)
		num_scan_timeouts := stats[indexName+":num_scan_timeouts"].(float64)

		logStats := func() {
			log.Printf("ScanAllReplicas: Indexer stats are: %v", stats)
		}
		if num_scan_errors > 0 {
			logStats()
			t.Fatalf("Expected '0' scan errors. Found: %v scan errors", num_scan_errors)
		}

		if num_scan_timeouts > 0 {
			logStats()
			t.Fatalf("Expected '0' scan timeouts. Found: %v scan timeouts", num_scan_errors)
		}

		if num_requests != float64(expectedNumRequests[i]) {
			logStats()
			t.Fatalf("Expected %v requests to the index. Found %v requests", expectedNumRequests, num_requests)
		}
	}
}

func computeRecallWithKNNResults(annResults []interface{}, knnResults []interface{}, expectedProjItems int, t *testing.T) float32 {
	if len(annResults) != len(knnResults) {
		t.Fatalf("Mismatch in annResults and knnResults. annResults: %v, knnResults: %v", annResults, knnResults)
	}

	knnMap := make(map[string]interface{})
	for i := range knnResults {
		if val, ok := knnResults[i].(map[string]interface{})["id"]; ok {
			knnMap[val.(string)] = knnResults[i]
		}
	}

	numAnn := 0
	for i := range annResults {
		if val, ok := annResults[i].(map[string]interface{})["id"]; ok {
			if knnVal, ok1 := knnMap[val.(string)]; ok1 {
				numAnn++
				knnValMap := knnVal.(map[string]interface{})
				annValMap := annResults[i].(map[string]interface{})
				if len(knnValMap) != len(annValMap) {
					t.Fatalf("Mismatch in map lengths for id: %v, knnValMap: %v, annValMap: %v", val, knnValMap, annValMap)
				}

				if len(annValMap) != expectedProjItems || len(knnValMap) != expectedProjItems {
					t.Fatalf("Mismatch in expected projection items for id: %v, knnValMap: %v, annValMap: %v, expectedProj: %v", val, knnValMap, annValMap, expectedProjItems)
				}

				for knnKey, knnVal := range knnValMap {
					if annVal, ok2 := annValMap[knnKey]; !ok2 {
						t.Fatalf("Mismatch in map keys for id: %v, knnValMap: %v, annValMap: %v", val, knnValMap, annValMap)
					} else if annVal != knnVal {
						t.Fatalf("Mismatch in map values for id: %v, knnValMap: %v, annValMap: %v", val, knnValMap, annValMap)
					}
				}
			}
		}
	}

	return float32(numAnn) / float32(len(knnMap))
}

func TestBhiveVectorIndex(t *testing.T) {
	skipIfNotPlasma(t)

	if !vectorsLoaded {
		vectorSetup(t, bucket, "", "", numDocs)
	}

	idx_bhive := "idx_bhive"

	// Drop all indexes from earlier tests
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	// Create Index
	stmt := "CREATE VECTOR INDEX " + idx_bhive +
		" ON default(sift VECTOR)" +
		" WITH { \"dimension\":128, \"description\": \"IVF,SQ8\", \"similarity\":\"L2_SQUARED\", \"defer_build\":true};"
	err := createWithDeferAndBuild(idx_bhive, bucket, "", "", stmt, defaultIndexActiveTimeout*2)
	FailTestIfError(err, "Error in creating idx_sift10k", t)

	limit := int64(5)

	queryVectorStr := "["
	for _, val := range indexVector.QueryVector {
		queryVectorStr += fmt.Sprintf("%v,", val)
	}
	queryVectorStr = queryVectorStr[:len(queryVectorStr)-1]
	queryVectorStr += "]"

	annScanStmt := fmt.Sprintf("with qvec as (%v) select meta().id, APPROX_VECTOR_DISTANCE(sift, qvec, \"L2_SQUARED\", %v, true) as distance "+
		"from %v ORDER BY distance limit %v", queryVectorStr, indexVector.Probes, bucket, limit)

	annScanResults, err := execN1QL(bucket, annScanStmt)
	FailTestIfError(err, "Error during secondary index scan", t)
	validateScanStats(bucket, idx_bhive, 1, limit, t)

	// validate the results using KNN
	knnScanStmt := fmt.Sprintf("with qvec as (%v) select meta().id, VECTOR_DISTANCE(sift, qvec, \"L2_SQUARED\") as distance "+
		"from %v ORDER BY distance limit %v",
		queryVectorStr, bucket, limit)

	knnScanResults, err := execN1QL(bucket, knnScanStmt)
	FailTestIfError(err, "Error during secondary index scan", t)
	validateScanStats(bucket, idx_bhive, 1, limit, t) // The num-request should not change because of KNN scan

	recall := computeRecallWithKNNResults(annScanResults, knnScanResults, 2, t)
	log.Printf("TestBhiveVectorIndex recall observed is: %v", recall)
	if recall < 0.5 {
		log.Printf("ANN scan results: %v, KNN scan results: %v", annScanResults, knnScanResults)
	}
}

func TestBhiveIndexWithIncludeColumns(t *testing.T) {
	skipIfNotPlasma(t)

	if !vectorsLoaded {
		vectorSetup(t, bucket, "", "", numDocs)
	}

	// Drop all indexes from earlier tests so that scan stats can validate this index alone
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)
	limit := int64(5)

	idx_bhive_include := "idx_bhive_include"
	// Create Index
	stmt := "CREATE VECTOR INDEX " + idx_bhive_include +
		" ON default(sift VECTOR) INCLUDE(`count`, `direction`, `docnum`, `vectornum`, `gender`) " +
		" WITH { \"dimension\":128, \"description\": \"IVF,SQ8\", \"similarity\":\"L2_SQUARED\", \"defer_build\":true};"
	err := createWithDeferAndBuild(idx_bhive_include, bucket, "", "", stmt, defaultIndexActiveTimeout*2)
	FailTestIfError(err, "Error in creating idx_sift10k", t)

	queryVectorStr := "["
	for _, val := range indexVector.QueryVector {
		queryVectorStr += fmt.Sprintf("%v,", val)
	}
	queryVectorStr = queryVectorStr[:len(queryVectorStr)-1]
	queryVectorStr += "]"

	// Case-1: Scan only the vector field with meta().id and distance in projection
	annScanStmt := fmt.Sprintf("with qvec as (%v) select meta().id, APPROX_VECTOR_DISTANCE(sift, qvec, \"L2_SQUARED\", %v, true) as distance "+
		"from %v ORDER BY distance limit %v", queryVectorStr, indexVector.Probes, bucket, limit)
	annScanResults, err := execN1QL(bucket, annScanStmt)
	FailTestIfError(err, "Error during secondary index scan", t)
	validateScanStats(bucket, idx_bhive_include, 1, limit, t)

	// validate the results using KNN
	knnScanStmt := fmt.Sprintf("with qvec as (%v) select meta().id, VECTOR_DISTANCE(sift, qvec, \"L2_SQUARED\") as distance "+
		"from %v ORDER BY distance limit %v", queryVectorStr, bucket, limit)

	knnScanResults, err := execN1QL(bucket, knnScanStmt)
	FailTestIfError(err, "Error during secondary index scan", t)
	validateScanStats(bucket, idx_bhive_include, 1, limit, t) // The num-request should not change because of KNN scan

	recall := computeRecallWithKNNResults(annScanResults, knnScanResults, 2, t)
	log.Printf("TestBhiveIndexWithIncludeColumns:Case-1 recall observed is: %v", recall)
	if recall < 0.5 {
		log.Printf("ANN scan results: %v, KNN scan results: %v", annScanResults, knnScanResults)
	}

	// Case-2: Let projection contain all the include fields with out filtering on include fields
	annScanStmt = fmt.Sprintf("with qvec as (%v) select meta().id, APPROX_VECTOR_DISTANCE(sift, qvec, \"L2_SQUARED\", %v, true) as distance, "+
		"count, direction, docnum, vectornum, gender from %v ORDER BY distance limit %v", queryVectorStr, indexVector.Probes, bucket, limit)
	annScanResults, err = execN1QL(bucket, annScanStmt)
	FailTestIfError(err, "Error during secondary index scan", t)
	validateScanStats(bucket, idx_bhive_include, 2, 2*limit, t)

	// validate the results using KNN
	knnScanStmt = fmt.Sprintf("with qvec as (%v) select meta().id, VECTOR_DISTANCE(sift, qvec, \"L2_SQUARED\") as distance, "+
		"count, direction, docnum, vectornum, gender from %v ORDER BY distance limit %v", queryVectorStr, bucket, limit)

	knnScanResults, err = execN1QL(bucket, knnScanStmt)
	FailTestIfError(err, "Error during secondary index scan", t)
	validateScanStats(bucket, idx_bhive_include, 2, 2*limit, t) // The num-request should not change because of KNN scan

	recall = computeRecallWithKNNResults(annScanResults, knnScanResults, 7, t)
	log.Printf("TestBhiveIndexWithIncludeColumns:Case-2 recall observed is: %v", recall)
	if recall < 0.5 {
		log.Printf("ANN scan results: %v, KNN scan results: %v", annScanResults, knnScanResults)
	}

	// Case-3: Let projection contain few the include fields with out filtering on include fields
	annScanStmt = fmt.Sprintf("with qvec as (%v) select meta().id, APPROX_VECTOR_DISTANCE(sift, qvec, \"L2_SQUARED\", %v, true) as distance, "+
		" count, direction, docnum from %v ORDER BY distance limit %v", queryVectorStr, indexVector.Probes, bucket, limit)
	annScanResults, err = execN1QL(bucket, annScanStmt)
	FailTestIfError(err, "Error during secondary index scan", t)
	validateScanStats(bucket, idx_bhive_include, 3, 3*limit, t)

	// validate the results using KNN
	knnScanStmt = fmt.Sprintf("with qvec as (%v) select meta().id, VECTOR_DISTANCE(sift, qvec, \"L2_SQUARED\") as distance, "+
		"count, direction, docnum from %v ORDER BY distance limit %v", queryVectorStr, bucket, limit)

	knnScanResults, err = execN1QL(bucket, knnScanStmt)
	FailTestIfError(err, "Error during secondary index scan", t)
	validateScanStats(bucket, idx_bhive_include, 3, 3*limit, t) // The num-request should not change because of KNN scan

	recall = computeRecallWithKNNResults(annScanResults, knnScanResults, 5, t)
	log.Printf("TestBhiveIndexWithIncludeColumns:Case-3 recall observed is: %v", recall)
	if recall < 0.5 {
		log.Printf("ANN scan results: %v, KNN scan results: %v", annScanResults, knnScanResults)
	}

	// Case-4: Let projection contain few the include fields with filtering on one include fields
	annScanStmt = fmt.Sprintf("with qvec as (%v) select meta().id, APPROX_VECTOR_DISTANCE(sift, qvec, \"L2_SQUARED\", %v, true) as distance, "+
		"count, direction, docnum from %v where direction = \"east\" ORDER BY distance limit %v", queryVectorStr, indexVector.Probes, bucket, limit)
	annScanResults, err = execN1QL(bucket, annScanStmt)
	FailTestIfError(err, "Error during secondary index scan", t)
	validateScanStats(bucket, idx_bhive_include, 4, 4*limit, t)

	// validate the results using KNN
	knnScanStmt = fmt.Sprintf("with qvec as (%v) select meta().id, VECTOR_DISTANCE(sift, qvec, \"L2_SQUARED\") as distance, "+
		"count, direction, docnum from %v where direction = \"east\" ORDER BY distance limit %v",
		queryVectorStr, bucket, limit)

	knnScanResults, err = execN1QL(bucket, knnScanStmt)
	FailTestIfError(err, "Error during secondary index scan", t)
	validateScanStats(bucket, idx_bhive_include, 4, 4*limit, t) // The num-request should not change because of KNN scan

	recall = computeRecallWithKNNResults(annScanResults, knnScanResults, 5, t)
	log.Printf("TestBhiveIndexWithIncludeColumns:Case-4 recall observed is: %v", recall)
	if recall < 0.5 {
		log.Printf("ANN scan results: %v, KNN scan results: %v", annScanResults, knnScanResults)
	}

	// Case-5: Let projection contain few the include fields with filtering on multiple include fields
	annScanStmt = fmt.Sprintf("with qvec as (%v) select meta().id, APPROX_VECTOR_DISTANCE(sift, qvec, \"L2_SQUARED\", %v, true) as distance, "+
		"direction, docnum from %v where direction = \"east\" and docnum > 1000 ORDER BY distance limit %v",
		queryVectorStr, indexVector.Probes, bucket, limit)
	annScanResults, err = execN1QL(bucket, annScanStmt)
	FailTestIfError(err, "Error during secondary index scan", t)
	validateScanStats(bucket, idx_bhive_include, 5, 5*limit, t)

	// validate the results using KNN
	knnScanStmt = fmt.Sprintf("with qvec as (%v) select meta().id, VECTOR_DISTANCE(sift, qvec, \"L2_SQUARED\") as distance, "+
		"direction, docnum from %v where direction = \"east\" and docnum > 1000 ORDER BY distance limit %v",
		queryVectorStr, bucket, limit)

	knnScanResults, err = execN1QL(bucket, knnScanStmt)
	FailTestIfError(err, "Error during secondary index scan", t)
	validateScanStats(bucket, idx_bhive_include, 5, 5*limit, t) // The num-request should not change because of KNN scan

	recall = computeRecallWithKNNResults(annScanResults, knnScanResults, 4, t)
	log.Printf("TestBhiveIndexWithIncludeColumns:Case-5 recall observed is: %v", recall)
	if recall < 0.5 {
		log.Printf("ANN scan results: %v, KNN scan results: %v", annScanResults, knnScanResults)
	}

	// Case-6: Include column fields are present in projection but not in filter
	annScanStmt = fmt.Sprintf("with qvec as (%v) select meta().id, direction, docnum, "+
		"APPROX_VECTOR_DISTANCE(sift, qvec, \"L2_SQUARED\", %v, true) as distance from %v ORDER BY distance limit %v",
		queryVectorStr, indexVector.Probes, bucket, limit)
	annScanResults, err = execN1QL(bucket, annScanStmt)
	FailTestIfError(err, "Error during secondary index scan", t)
	validateScanStats(bucket, idx_bhive_include, 6, 6*limit, t)

	// validate the results using KNN
	knnScanStmt = fmt.Sprintf("with qvec as (%v) select meta().id, direction, docnum, VECTOR_DISTANCE(sift, qvec, \"L2_SQUARED\") as distance"+
		" from %v ORDER BY distance limit %v", queryVectorStr, bucket, limit)

	knnScanResults, err = execN1QL(bucket, knnScanStmt)
	FailTestIfError(err, "Error during secondary index scan", t)
	validateScanStats(bucket, idx_bhive_include, 6, 6*limit, t) // The num-request should not change because of KNN scan

	recall = computeRecallWithKNNResults(annScanResults, knnScanResults, 4, t)
	log.Printf("TestBhiveIndexWithIncludeColumns:Case-6 recall observed is: %v", recall)
}

func TestPartitionSetsWithBhiveIndex(t *testing.T) {
	skipIfNotPlasma(t)

	if !vectorsLoaded {
		vectorSetup(t, bucket, "", "", numDocs)
	}

	bucket := "default"
	// Drop all indexes from earlier tests so that scan stats can validate this index alone
	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	limit := int64(5)

	idx_bhive_partnsets := "idx_bhive_partnsets"
	// Create Index
	stmt := "CREATE VECTOR INDEX " + idx_bhive_partnsets +
		" ON default(sift VECTOR) INCLUDE(`count`, `docnum`, `vectornum`, `gender`) partition by hash(`direction`)" +
		" WITH { \"dimension\":128, \"description\": \"IVF,SQ8\", \"similarity\":\"L2_SQUARED\", \"defer_build\":true};"
	err := createWithDeferAndBuild(idx_bhive_partnsets, bucket, "", "", stmt, defaultIndexActiveTimeout*2)
	FailTestIfError(err, "Error in creating idx_bhive_partnsets", t)

	queryVectorStr := "["
	for _, val := range indexVector.QueryVector {
		queryVectorStr += fmt.Sprintf("%v,", val)
	}
	queryVectorStr = queryVectorStr[:len(queryVectorStr)-1]
	queryVectorStr += "]"

	// Case-1: Scan the vector fields where direction = "east"
	annScanStmt := fmt.Sprintf("with qvec as (%v) select meta().id, APPROX_VECTOR_DISTANCE(sift, qvec, \"L2_SQUARED\", %v, true) as distance "+
		"from %v where direction = \"east\" ORDER BY distance limit %v", queryVectorStr, indexVector.Probes, bucket, limit)
	annScanResults, err := execN1QL(bucket, annScanStmt)
	FailTestIfError(err, "Error during secondary index scan", t)

	value := qvalue.NewValue("east")
	v, e := qvalue.NewValue([]qvalue.Value{value}).MarshalJSON()
	if e != nil {
		t.Fatalf("Error observed while marshalling direction. err: %v", e)
	}

	numPartition := 8
	partnId := common.HashKeyPartition(v, int(numPartition), common.CRC32)
	expectedRequests := make(map[int]int64)
	for i := 1; i <= numPartition; i++ {
		if i == int(partnId) {
			expectedRequests[i] = 1
		} else {
			expectedRequests[i] = 0
		}
	}

	validateScanStatsForPartns(bucket, idx_bhive_partnsets, expectedRequests, numPartition, t)

	// validate the results using KNN
	knnScanStmt := fmt.Sprintf("with qvec as (%v) select meta().id, VECTOR_DISTANCE(sift, qvec, \"L2_SQUARED\") as distance "+
		"from %v where direction = \"east\" ORDER BY distance limit %v",
		queryVectorStr, bucket, limit)

	knnScanResults, err := execN1QL(bucket, knnScanStmt)
	FailTestIfError(err, "Error during secondary index scan", t)
	validateScanStatsForPartns(bucket, idx_bhive_partnsets, expectedRequests, numPartition, t) // The num-request should not change because of KNN scan

	recall := computeRecallWithKNNResults(annScanResults, knnScanResults, 2, t)
	log.Printf("TestPartitionSetsWithBhiveIndex recall observed is: %v", recall)
	if recall < 0.5 {
		log.Printf("ANN scan results: %v, KNN scan results: %v", annScanResults, knnScanResults)
	}
}
