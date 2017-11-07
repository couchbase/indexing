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
	scanResults, err := secondaryindex.ArrayIndex_Range(indexName, bucketName, indexScanAddress, []interface{}{"A"}, []interface{}{"zzz"}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.ValidateArrayResult(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	// Delete some docs
	kvdocs = deleteArrayDocs(100, kvdocs)
	docScanResults = datautility.ExpectedArrayScanResponse_string(kvdocs, "friends", "#haw", "6h25", 1, true)
	scanResults, err = secondaryindex.ArrayIndex_Range(indexName, bucketName, indexScanAddress, []interface{}{"#haw"}, []interface{}{"6h25"}, 1, false, defaultlimit, c.SessionConsistency, nil)
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
	scanResults, err := secondaryindex.ArrayIndex_Range(indexName, bucketName, indexScanAddress, []interface{}{"A"}, []interface{}{"zzz"}, 1, false, defaultlimit, c.SessionConsistency, nil)
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
	scanResults, err = secondaryindex.ArrayIndex_Range(indexName, bucketName, indexScanAddress, []interface{}{"A"}, []interface{}{"zzz"}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.ValidateArrayResult(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	// Delete some docs
	kvdocs = deleteArrayDocs(100, kvdocs)
	docScanResults = datautility.ExpectedArrayScanResponse_string(kvdocs, "friends", "sutq", "xq25", 2, true)
	scanResults, err = secondaryindex.ArrayIndex_Range(indexName, bucketName, indexScanAddress, []interface{}{"sutq"}, []interface{}{"xq25"}, 2, false, defaultlimit, c.SessionConsistency, nil)
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
	scanResults, err := secondaryindex.ArrayIndex_Range(indexName, bucketName, indexScanAddress, []interface{}{"A"}, []interface{}{"zzz"}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.ValidateArrayResult(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	// Delete some docs
	kvdocs = deleteArrayDocs(100, kvdocs)
	docScanResults = datautility.ExpectedArrayScanResponse_string(kvdocs, "friends", "dsfsdf", "kluilh", 0, false)
	scanResults, err = secondaryindex.ArrayIndex_Range(indexName, bucketName, indexScanAddress, []interface{}{"dsfsdf"}, []interface{}{"kluilh"}, 0, false, defaultlimit, c.SessionConsistency, nil)
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
	scanResults, err := secondaryindex.ArrayIndex_Range(indexName, bucketName, indexScanAddress, []interface{}{"A"}, []interface{}{"zzz"}, 1, false, defaultlimit, c.SessionConsistency, nil)
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
	scanResults, err = secondaryindex.ArrayIndex_Range(indexName, bucketName, indexScanAddress, []interface{}{"A"}, []interface{}{"zzz"}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.ValidateArrayResult(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	// Delete some docs
	kvdocs = deleteArrayDocs(100, kvdocs)
	docScanResults = datautility.ExpectedArrayScanResponse_string(kvdocs, "friends", "454fds", "ghgsd", 3, false)
	scanResults, err = secondaryindex.ArrayIndex_Range(indexName, bucketName, indexScanAddress, []interface{}{"454fds"}, []interface{}{"ghgsd"}, 3, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.ValidateArrayResult(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

// Array index with array being empty, missing, null and scalar
// in cases of leading and non-leading key
func TestArrayIndexCornerCases(t *testing.T) {
	log.Printf("In TestArrayIndexCornerCases()")

	tmp := secondaryindex.UseClient
	secondaryindex.UseClient = "gsi"

	bucketName, field_name, field_tags := "default", "arr_name", "arr_tags"
	missing := "~[]{}falsenilNA~"
	indexName1, indexName2, indexName3 := "arr_single", "arr_leading", "arr_nonleading"
	indexExpressions := [][]string{}
	indexExpressions = append(indexExpressions, []string{"ALL arr_tags"}, []string{"ALL arr_tags", "arr_name"}, []string{"arr_name", "ALL arr_tags"})

	createIndexes(bucketName, []string{indexName1, indexName2, indexName3}, indexExpressions, t)

	log.Printf("\n\n--------ScanAll for EMPTY array--------")
	key := getRandomDocId()
	createSpecialArrayDoc(EMPTY, key, field_name, field_tags, bucketName)
	scanAllAndValidate(indexName1, bucketName, key, nil, t)
	scanAllAndValidate(indexName2, bucketName, key, nil, t)
	scanAllAndValidate(indexName3, bucketName, key, []interface{}{docs[key].(map[string]interface{})[field_name], missing}, t)

	log.Printf("\n\n--------ScanAll for MISSING array--------")
	createSpecialArrayDoc(MISSING, key, field_name, field_tags, bucketName)
	scanAllAndValidate(indexName1, bucketName, key, nil, t)
	scanAllAndValidate(indexName2, bucketName, key, nil, t)
	scanAllAndValidate(indexName3, bucketName, key, []interface{}{docs[key].(map[string]interface{})[field_name], missing}, t)

	log.Printf("\n\n--------ScanAll for NULL array--------")
	createSpecialArrayDoc(NULL, key, field_name, field_tags, bucketName)
	scanAllAndValidate(indexName1, bucketName, key, []interface{}{nil}, t)
	scanAllAndValidate(indexName2, bucketName, key, []interface{}{nil, docs[key].(map[string]interface{})[field_name]}, t)
	scanAllAndValidate(indexName3, bucketName, key, []interface{}{docs[key].(map[string]interface{})[field_name], nil}, t)

	log.Printf("\n\n--------ScanAll for SCALARVALUE array--------")
	createSpecialArrayDoc(SCALARVALUE, key, field_name, field_tags, bucketName)
	scanAllAndValidate(indexName1, bucketName, key, []interface{}{docs[key].(map[string]interface{})[field_tags]}, t)
	scanAllAndValidate(indexName2, bucketName, key, []interface{}{docs[key].(map[string]interface{})[field_tags], docs[key].(map[string]interface{})[field_name]}, t)
	scanAllAndValidate(indexName3, bucketName, key, []interface{}{docs[key].(map[string]interface{})[field_name], docs[key].(map[string]interface{})[field_tags]}, t)

	log.Printf("\n\n--------ScanAll for SCALAROBJECT array--------\n")
	createSpecialArrayDoc(SCALAROBJECT, key, field_name, field_tags, bucketName)
	scanAllAndValidate(indexName1, bucketName, key, []interface{}{docs[key].(map[string]interface{})[field_tags]}, t)
	scanAllAndValidate(indexName2, bucketName, key, []interface{}{docs[key].(map[string]interface{})[field_tags], docs[key].(map[string]interface{})[field_name]}, t)
	scanAllAndValidate(indexName3, bucketName, key, []interface{}{docs[key].(map[string]interface{})[field_name], docs[key].(map[string]interface{})[field_tags]}, t)
	secondaryindex.UseClient = tmp
}

func TestArraySizeIncreaseDecrease(t *testing.T) {
	log.Printf("In TestArraySizeIncreaseDecrease()")

	var bucketName = "default"
	indexName := "arr1"
	indexExpr := "ALL friends"

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)
	kvutility.FlushBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)

	err := secondaryindex.ChangeIndexerSettings("indexer.settings.allow_large_keys", false, clusterconfig.Username, clusterconfig.Password, kvaddress)
	FailTestIfError(err, "Error in ChangeIndexerSettings", t)
	time.Sleep(5 * time.Second) // Wait for restart after this setting change

	err = secondaryindex.ChangeIndexerSettings("indexer.settings.max_array_seckey_size", float64(5120), clusterconfig.Username, clusterconfig.Password, kvaddress)
	FailTestIfError(err, "Error in ChangeIndexerSettings", t)
	time.Sleep(2 * time.Second)

	kvdocs := createArrayDocs(100, 6000)
	kvutility.SetKeyValues(kvdocs, bucketName, "", clusterconfig.KVAddress)

	err = secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{indexExpr}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	scanResults, err := secondaryindex.ArrayIndex_Range(indexName, bucketName, indexScanAddress, []interface{}{"#"}, []interface{}{"zzz"}, 1, false, defaultlimit, c.SessionConsistency, nil)
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
	scanResults, err = secondaryindex.ArrayIndex_Range(indexName, bucketName, indexScanAddress, []interface{}{"a"}, []interface{}{"g"}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.ValidateArrayResult(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	// Change setting to low value
	err = secondaryindex.ChangeIndexerSettings("indexer.settings.max_array_seckey_size", float64(4096), clusterconfig.Username, clusterconfig.Password, kvaddress)
	FailTestIfError(err, "Error in ChangeIndexerSettings", t)
	time.Sleep(5 * time.Second) // Wait for restart after this setting change

	// Update docs
	kvdocs = updateDocsArrayField(kvdocs, bucketName)
	scanResults, err = secondaryindex.ArrayIndex_Range(indexName, bucketName, indexScanAddress, []interface{}{"#"}, []interface{}{"zzz"}, 1, false, defaultlimit, c.SessionConsistency, nil)
	if len(scanResults) != 0 {
		FailTestIfError(errors.New("Expected 0 results due to size limit"), "Error in scan result validation", t)
	}

	err = secondaryindex.ChangeIndexerSettings("indexer.settings.allow_large_keys", true, clusterconfig.Username, clusterconfig.Password, kvaddress)
	FailTestIfError(err, "Error in ChangeIndexerSettings", t)
	time.Sleep(5 * time.Second) // Wait for restart after this setting change
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
		key := getRandomDocId()
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

func deleteArrayDocs(numDocs int, kvdocs tc.KeyValues) tc.KeyValues {
	i := 0
	keysToBeDeleted := make(tc.KeyValues)
	for key, value := range kvdocs {
		keysToBeDeleted[key] = value
		i++
		if i == numDocs {
			break
		}
	}
	kvutility.DeleteKeys(keysToBeDeleted, "default", "", clusterconfig.KVAddress)
	// Update docs object with deleted keys
	for key, _ := range keysToBeDeleted {
		delete(kvdocs, key)
	}
	return kvdocs
}

type ArrayType int

const (
	EMPTY ArrayType = iota
	MISSING
	NULL
	SCALARVALUE
	SCALAROBJECT
)

// create docs with array being empty, missing, null or scalar
func createSpecialArrayDoc(at ArrayType, key, nonArrayFieldName, arrayFieldName, bucketName string) tc.KeyValues {
	arrDocs := make(tc.KeyValues)
	value := make(map[string]interface{})
	value[nonArrayFieldName] = randString(randomNum(5, 7))
	switch at {
	case EMPTY:
		value[arrayFieldName] = []string{} //EMPTY
	case MISSING:
		// value[arrayFieldName] = []string{}    //Comment it out to have missing effect
	case NULL:
		value[arrayFieldName] = nil //JSON null
	case SCALARVALUE:
		value[arrayFieldName] = "IamScalar" //Scalar value
	case SCALAROBJECT:
		tags := make(map[string]interface{})
		tags["1"] = "abc"
		tags["2"] = "def"
		value[arrayFieldName] = tags //Scalar object
	}
	arrDocs[key] = value
	kvutility.SetKeyValues(arrDocs, bucketName, "", clusterconfig.KVAddress)
	UpdateKVDocs(arrDocs, docs)
	return arrDocs
}

func scanAllAndValidate(indexName, bucketName, docID string, expectedScanResult []interface{}, t *testing.T) {
	scanResults, err := secondaryindex.ScanAll(indexName, bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	tc.PrintScanResults(scanResults, "scanResults")
	if expectedScanResult == nil { // Expecting 0 results
		if len(scanResults) != 0 {
			FailTestIfError(errors.New("Expected 0 results"), "Error in scan result validation", t)
		}
	} else {
		docScanResults := make(tc.ScanResponse)
		docScanResults[docID] = expectedScanResult
		err = tv.Validate(docScanResults, scanResults)
		FailTestIfError(err, "Error in scan result validation", t)
	}
}

func getRandomDocId() string {
	uuid, _ := c.NewUUID()
	return strconv.Itoa(int(uuid.Uint64()))
}

func createIndexes(bucketName string, indexNames []string, indexExpressions [][]string, t *testing.T) {
	for i, indexName := range indexNames {
		err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", indexExpressions[i], false, nil, true, defaultIndexActiveTimeout, nil)
		FailTestIfError(err, "Error in creating the index", t)
	}
}
