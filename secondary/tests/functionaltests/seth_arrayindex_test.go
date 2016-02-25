package functionaltests

import (
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/tests/framework/datautility"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	tv "github.com/couchbase/indexing/secondary/tests/framework/validation"
	"log"
	"testing"
)

// Simple array with string array items
func TestRangeArrayIndex_Distinct(t *testing.T) {
	log.Printf("In TestRangeArrayIndex_Distinct()")
	
	var bucketName = "default"
	indexName := "arridx_friends"
	indexExpr := "ALL DISTINCT friends"

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)
	kvutility.FlushBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)

	kvdocs := generateDocs(1000, "users_simplearray.prod")
	kvutility.SetKeyValues(kvdocs, bucketName, "", clusterconfig.KVAddress)
	
	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{indexExpr}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)
	
	docScanResults := datautility.ExpectedArrayScanResponse_string(kvdocs, "friends", "A", "zzz", 3, true)
	scanResults, err := secondaryindex.ArrayIndex_Range(indexName, bucketName, indexScanAddress, []interface{}{"A"}, []interface{}{"zzz"}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.ValidateArrayResult(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestUpdateArrayIndex_Distinct(t *testing.T) {
	log.Printf("In TestUpdateArrayIndex_Distinct()")
	
	var bucketName = "default"
	indexName := "arridx_friends"
	indexExpr := "ALL DISTINCT friends"

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)
	kvutility.FlushBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)

	kvdocs := generateDocs(1000, "users_simplearray.prod")
	kvutility.SetKeyValues(kvdocs, bucketName, "", clusterconfig.KVAddress)
	
	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{indexExpr}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)
	
	docScanResults := datautility.ExpectedArrayScanResponse_string(kvdocs, "friends", "A", "zzz", 3, true)
	scanResults, err := secondaryindex.ArrayIndex_Range(indexName, bucketName, indexScanAddress, []interface{}{"A"}, []interface{}{"zzz"}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.ValidateArrayResult(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
	
	// log.Printf("kvdocs = \n")
	// tc.PrintDocs(kvdocs)
	updatedDocs := generateDocs(1000, "users_simplearray.prod")
	keys := []string{}
	for k := range kvdocs {
	    keys = append(keys, k)
	}
	i := 0
	for _, v := range updatedDocs {
	    kvdocs[keys[i]] = v
		i++
	}
	
	//log.Printf("After updating kvdocs = \n")
	//tc.PrintDocs(kvdocs)
	
	kvutility.SetKeyValues(kvdocs, bucketName, "", clusterconfig.KVAddress)
	docScanResults = datautility.ExpectedArrayScanResponse_string(kvdocs, "friends", "A", "zzz", 3, true)
	scanResults, err = secondaryindex.ArrayIndex_Range(indexName, bucketName, indexScanAddress, []interface{}{"A"}, []interface{}{"zzz"}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.ValidateArrayResult(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

// Simple array with string array items
func TestRangeArrayIndex_Duplicate(t *testing.T) {
	log.Printf("In TestRangeArrayIndex_Duplicate()")
	
	var bucketName = "default"
	indexName := "arridx_friends"
	indexExpr := "ALL friends"

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)
	kvutility.FlushBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)

	kvdocs := generateDocs(1000, "users_simplearray.prod")
	kvutility.SetKeyValues(kvdocs, bucketName, "", clusterconfig.KVAddress)
	
	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{indexExpr}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)
	
	docScanResults := datautility.ExpectedArrayScanResponse_string(kvdocs, "friends", "A", "zzz", 3, false)
	scanResults, err := secondaryindex.ArrayIndex_Range(indexName, bucketName, indexScanAddress, []interface{}{"A"}, []interface{}{"zzz"}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.ValidateArrayResult(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestUpdateArrayIndex_Duplicate(t *testing.T) {
	log.Printf("In TestUpdateArrayIndex_Duplicate()")
	
	var bucketName = "default"
	indexName := "arridx_friends"
	indexExpr := "ALL friends"

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)
	kvutility.FlushBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)

	kvdocs := generateDocs(1000, "users_simplearray.prod")
	kvutility.SetKeyValues(kvdocs, bucketName, "", clusterconfig.KVAddress)
	
	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{indexExpr}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)
	
	docScanResults := datautility.ExpectedArrayScanResponse_string(kvdocs, "friends", "A", "zzz", 3, false)
	scanResults, err := secondaryindex.ArrayIndex_Range(indexName, bucketName, indexScanAddress, []interface{}{"A"}, []interface{}{"zzz"}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.ValidateArrayResult(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
	
	updatedDocs := generateDocs(1000, "users_simplearray.prod")
	keys := []string{}
	for k := range kvdocs {
	    keys = append(keys, k)
	}
	i := 0
	for _, v := range updatedDocs {
	    kvdocs[keys[i]] = v
		i++
	}
	kvutility.SetKeyValues(kvdocs, bucketName, "", clusterconfig.KVAddress)
	docScanResults = datautility.ExpectedArrayScanResponse_string(kvdocs, "friends", "A", "zzz", 3, false)
	scanResults, err = secondaryindex.ArrayIndex_Range(indexName, bucketName, indexScanAddress, []interface{}{"A"}, []interface{}{"zzz"}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.ValidateArrayResult(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}