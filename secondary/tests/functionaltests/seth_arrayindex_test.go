package functionaltests

import (
	"errors"
	"fmt"
	c "github.com/couchbase/indexing/secondary/common"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/datautility"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	tv "github.com/couchbase/indexing/secondary/tests/framework/validation"
	"log"
	"strconv"
	"testing"
	"time"
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

func TestArraySizeIncreaseDecrease(t *testing.T) {
	log.Printf("In TestArraySizeIncreaseDecrease()")

	var bucketName = "default"
	indexName := "arr1"
	indexExpr := "ALL friends"

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)
	kvutility.FlushBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)

	err := secondaryindex.ChangeIndexerSettings("indexer.settings.max_array_seckey_size", float64(5120), clusterconfig.Username, clusterconfig.Password, kvaddress)
	FailTestIfError(err, "Error in ChangeIndexerSettings", t)
	time.Sleep(2 * time.Second)

	kvdocs := createArrayDocs(100, 6000)
	kvutility.SetKeyValues(kvdocs, bucketName, "", clusterconfig.KVAddress)

	err = secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{indexExpr}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	scanResults, err := secondaryindex.ArrayIndex_Range(indexName, bucketName, indexScanAddress, []interface{}{"#"}, []interface{}{"zzz"}, 1, true, defaultlimit, c.SessionConsistency, nil)
	if len(scanResults) != 0 {
		FailTestIfError(errors.New("Expected 0 results due to size limit"), "Error in scan result validation", t)
	}

	// Change setting to higher value
	err = secondaryindex.ChangeIndexerSettings("indexer.settings.max_array_seckey_size", float64(51200), clusterconfig.Username, clusterconfig.Password, kvaddress)
	FailTestIfError(err, "Error in ChangeIndexerSettings", t)
	time.Sleep(5 * time.Second) // Wait for restart after this setting change

	// Update docs
	kvdocs = updateDocsArrayField(kvdocs, bucketName)
	docScanResults := datautility.ExpectedArrayScanResponse_string(kvdocs, "friends", "a", "g", 1, false)
	scanResults, err = secondaryindex.ArrayIndex_Range(indexName, bucketName, indexScanAddress, []interface{}{"a"}, []interface{}{"g"}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.ValidateArrayResult(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	// Change setting to low value
	err = secondaryindex.ChangeIndexerSettings("indexer.settings.max_array_seckey_size", float64(4096), clusterconfig.Username, clusterconfig.Password, kvaddress)
	FailTestIfError(err, "Error in ChangeIndexerSettings", t)
	time.Sleep(5 * time.Second) // Wait for restart after this setting change

	// Update docs
	kvdocs = updateDocsArrayField(kvdocs, bucketName)
	scanResults, err = secondaryindex.ArrayIndex_Range(indexName, bucketName, indexScanAddress, []interface{}{"#"}, []interface{}{"zzz"}, 1, true, defaultlimit, c.SessionConsistency, nil)
	if len(scanResults) != 0 {
		FailTestIfError(errors.New("Expected 0 results due to size limit"), "Error in scan result validation", t)
	}
}

func updateDocsArrayField(kvdocs tc.KeyValues, bucketName string) tc.KeyValues {
	// Update docs
	keysToBeUpdated := make(tc.KeyValues)
	for k, v := range kvdocs {
		json := v.(map[string]interface{})
		arr := json["friends"].([]string)
		arr[0] = fmt.Sprintf("%s_%v", arr[0], randomNum(0, 10000))
		json["friends"] = arr
		keysToBeUpdated[k] = json
	}
	kvdocs = keysToBeUpdated
	kvutility.SetKeyValues(kvdocs, bucketName, "", clusterconfig.KVAddress)
	time.Sleep(2 * time.Second)
	return kvdocs
}

// create docs with arrays with size atleast 5 times numArrayItems
func createArrayDocs(numDocs, numArrayItems int) tc.KeyValues {
	log.Printf("Start of createArrayDocs()")
	arrDocs := make(tc.KeyValues)
	for i := 0; i < numDocs; i++ {
		uuid, _ := c.NewUUID()
		key := strconv.Itoa(int(uuid.Uint64()))
		value := make(map[string]interface{})
		value["age"] = randomNum(0, 100)
		// value["friends"]
		arr := make([]string, 0)
		for j := 0; j < numArrayItems; j++ {
			arr = append(arr, randString(randomNum(5, 7)))
		}
		value["friends"] = arr
		arrDocs[key] = value
	}
	log.Printf("End of createArrayDocs()")
	return arrDocs
}
