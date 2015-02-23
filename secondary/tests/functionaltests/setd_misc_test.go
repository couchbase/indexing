package functionaltests

import (
	"errors"
	"fmt"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/datautility"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	tv "github.com/couchbase/indexing/secondary/tests/framework/validation"
	"math/rand"
	"path/filepath"
	"testing"
	"time"
)

var expectedJsonDocs tc.KeyValues
var mixeddtdocs tc.KeyValues
var seed int
var proddir, bagdir string

// After bucket delete:-
// 1) query for old index before loading bucket
// 2) query for old index after loading bucket
// 3) create new indexes and query
// 4) list indexes: should list only new indexes
func SkipTestBucketDefaultDelete(t *testing.T) {
	kvutility.DeleteBucket("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	time.Sleep(1 * time.Second)
	kvutility.CreateBucket("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress, 256)
	docs = datautility.LoadJSONFromCompressedFile(dataFilePath, "docid")
	mut_docs = datautility.LoadJSONFromCompressedFile(mutationFilePath, "docid")
	fmt.Println("Populating the default bucket")
	kvutility.SetKeyValues(docs, "default", "", clusterconfig.KVAddress)
	time.Sleep(10 * time.Second)

	var indexName = "index_isActive"
	var bucketName = "default"

	scanResults, err := secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{"BIOSPAN"}, true, 10000000)
	if err == nil {
		fmt.Println("Scan did not fail as expected. Got scanresults: \n", scanResults)
		e := errors.New("Scan did not fail as expected after bucket delete")
		FailTestIfError(e, "Error in TestBucketDefaultDelete", t)
	} else {
		fmt.Println("Scan failed as expected with error: ", err)
	}

	fmt.Println("Populating the default bucket after it was deleted")
	kvutility.SetKeyValues(docs, "default", "", clusterconfig.KVAddress)

	err = secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"company"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "company", "BIOSPAN", "BIOSPAN", 3)
	scanResults, err = secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{"BIOSPAN"}, true, 10000000)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)
	// todo: list the index and confirm there is only index created
}

func TestMixedDatatypesScanAll(t *testing.T) {
	fmt.Println("In TestMixedDatatypesScanAll()")

	field := "md_street"
	indexName := "index_mixeddt"
	bucketName := "default"

	secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	kvutility.FlushBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	time.Sleep(1 * time.Second)

	mixeddtdocs = generateJSONSMixedDatatype(1000, "md_street")
	seed++
	fmt.Println("Setting mixed datatypes JSON docs in KV")
	kvutility.SetKeyValues(mixeddtdocs, "default", "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{field}, true)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanAllResponse(mixeddtdocs, field)
	scanResults, err := secondaryindex.ScanAll(indexName, bucketName, indexScanAddress, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)
	fmt.Println("Lengths of expected and actual scan results are: ", len(docScanResults), len(scanResults))
}

func TestMixedDatatypesRange_Float(t *testing.T) {
	fmt.Println("In TestMixedDatatypesRange_Float()")

	field := "mixed_field"
	indexName := "index_mixeddt"
	bucketName := "default"

	secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	kvutility.FlushBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	time.Sleep(1 * time.Second)

	mixeddtdocs = generateJSONSMixedDatatype(1000, field)
	seed++
	fmt.Println("Setting mixed datatypes JSON docs in KV")
	kvutility.SetKeyValues(mixeddtdocs, "default", "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{field}, true)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_float64(mixeddtdocs, field, 100, 1000, 3)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{100}, []interface{}{1000}, 3, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)
	fmt.Println("Lengths of expected and actual scan results are: ", len(docScanResults), len(scanResults))

	docScanResults = datautility.ExpectedScanResponse_float64(mixeddtdocs, field, 1, 100, 2)
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{1}, []interface{}{100}, 2, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)
	fmt.Println("Lengths of expected and actual scan results are: ", len(docScanResults), len(scanResults))
}

func TestMixedDatatypesRange_String(t *testing.T) {
	fmt.Println("In TestMixedDatatypesRange_String()")

	field := "mixed_field"
	indexName := "index_mixeddt"
	bucketName := "default"

	secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	kvutility.FlushBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	time.Sleep(1 * time.Second)

	mixeddtdocs = generateJSONSMixedDatatype(1000, field)
	seed++
	fmt.Println("Setting mixed datatypes JSON docs in KV")
	kvutility.SetKeyValues(mixeddtdocs, "default", "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{field}, true)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_string(mixeddtdocs, field, "A", "Z", 3)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"A"}, []interface{}{"Z"}, 3, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)
	fmt.Println("Lengths of expected and actual scan results are: ", len(docScanResults), len(scanResults))
}

func TestMixedDatatypesRange_Json(t *testing.T) {
	fmt.Println("In TestMixedDatatypesRange_Json()")

	field := "mixed_field"
	indexName := "index_mixeddt"
	bucketName := "default"

	secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	kvutility.FlushBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	time.Sleep(1 * time.Second)

	mixeddtdocs = generateJSONSMixedDatatype(1000, field)
	seed++
	fmt.Println("Setting mixed datatypes JSON docs in KV")
	kvutility.SetKeyValues(mixeddtdocs, "default", "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{field}, true)
	FailTestIfError(err, "Error in creating the index", t)

	low := map[string]interface{}{
		"door":   0.0,
		"street": "#",
		"city":   "#"}
	high := map[string]interface{}{
		"door":   10000.0,
		"street": "zzzzzzzzz",
		"city":   "zzzzzzzzz"}

	docScanResults := datautility.ExpectedScanAllResponse_json(mixeddtdocs, field)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{low}, []interface{}{high}, 3, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)
	fmt.Println("Lengths of expected and actual scan results are: ", len(docScanResults), len(scanResults))
}

func TestMixedDatatypesScan_Bool(t *testing.T) {
	fmt.Println("In TestMixedDatatypesScan_Bool()")

	field := "mixed_field"
	indexName := "index_mixeddt"
	bucketName := "default"

	secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	kvutility.FlushBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	time.Sleep(1 * time.Second)

	mixeddtdocs = generateJSONSMixedDatatype(1000, field)
	seed++
	fmt.Println("Setting mixed datatypes JSON docs in KV")
	kvutility.SetKeyValues(mixeddtdocs, "default", "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{field}, true)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_bool(mixeddtdocs, field, true, 3)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{true}, []interface{}{true}, 3, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)
	fmt.Println("Lengths of expected and actual scan results are: ", len(docScanResults), len(scanResults))

	docScanResults = datautility.ExpectedScanResponse_bool(mixeddtdocs, field, false, 3)
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{false}, []interface{}{false}, 3, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)
	fmt.Println("Lengths of expected and actual scan results are: ", len(docScanResults), len(scanResults))
}

// Test case for testing secondary key field values as very huge
func TestLargeSecondaryKeyLength(t *testing.T) {
	fmt.Println("In TestLargeSecondaryKeyLength()")

	field := "LongSecField"
	indexName := "index_LongSecField"
	bucketName := "default"

	secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	kvutility.FlushBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	time.Sleep(1 * time.Second)

	largeKeyDocs := generateLargeSecondayKeyDocs(1000, field)
	seed++
	fmt.Println("Setting JSON docs in KV")
	kvutility.SetKeyValues(largeKeyDocs, "default", "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{field}, true)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanAllResponse(largeKeyDocs, field)
	scanResults, err := secondaryindex.ScanAll(indexName, bucketName, indexScanAddress, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	fmt.Println("ScanAll: Lengths of expected and actual scan results are: ", len(docScanResults), len(scanResults))
	tv.Validate(docScanResults, scanResults)

	docScanResults = datautility.ExpectedScanResponse_string(largeKeyDocs, field, "A", "zzzz", 3)
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"A"}, []interface{}{"zzzz"}, 3, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	fmt.Println("Range: Lengths of expected and actual scan results are: ", len(docScanResults), len(scanResults))
	tv.Validate(docScanResults, scanResults)
}

// Test case for testing primary key values with longest length possible
func TestLargePrimaryKeyLength(t *testing.T) {
	fmt.Println("In TestLargePrimaryKeyLength()")
	
	indexName := "index_LongPrimaryField"
	bucketName := "default"
	
	secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	kvutility.FlushBucket(bucketName, "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	time.Sleep(1 * time.Second)
	
	largePrimaryKeyDocs := generateLargePrimaryKeyDocs(1000, "docid")
	seed++
	fmt.Println("Setting JSON docs in KV")
	kvutility.SetKeyValues(largePrimaryKeyDocs, "default", "", clusterconfig.KVAddress)	
	
	err := secondaryindex.CreatePrimaryIndex(indexName, bucketName, indexManagementAddress, true)
	FailTestIfError(err, "Error in creating the index", t)

	scanResults, err := secondaryindex.ScanAll(indexName, bucketName, indexScanAddress, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	if len(scanResults) != len(largePrimaryKeyDocs) {
		fmt.Println("Len of scanResults is incorrect. Expected and Actual are", len(largePrimaryKeyDocs), len(scanResults))
		err = errors.New("Len of scanResults is incorrect.")
	}
	FailTestIfError(err, "Len of scanResults is incorrect", t)
	fmt.Println("Lengths of scanReuslts and num of docs are: ", len(scanResults), len(largePrimaryKeyDocs))
}

func generateJSONSMixedDatatype(numDocs int, fieldName string) tc.KeyValues {
	prodfile := filepath.Join(proddir, "test2.prod")
	docs := GenerateJsons(numDocs, seed, prodfile, bagdir)
	numberCount := 0
	stringCount := 0
	objCount := 0
	trueBoolCount := 0
	falseBoolCount := 0
	for k, v := range docs {
		json := v.(map[string]interface{})
		num := randomNum(1, 5)
		if num == 1 {
			numberCount++
			json[fieldName] = float64(randomNum(1, 10000))
		} else if num == 2 {
			stringCount++
			json[fieldName] = randString(randomNum(1, 20))
		} else if num == 3 {
			objCount++
			streetJson := make(map[string]interface{})
			streetJson["door"] = float64(randomNum(1, 1000))
			streetJson["street"] = randString(randomNum(1, 20))
			streetJson["city"] = randString(randomNum(1, 10))
			json[fieldName] = streetJson
		} else if num == 4 {
			boolVal := randomBool()
			if boolVal == true {
				trueBoolCount++
			} else {
				falseBoolCount++
			}
			json[fieldName] = boolVal
		}
		docs[k] = json
	}
	fmt.Println("Number of number fields is: ", numberCount)
	fmt.Println("Number of string fields is: ", stringCount)
	fmt.Println("Number of json fields is: ", objCount)
	fmt.Println("Number of true bool fields is: ", trueBoolCount)
	fmt.Println("Number of false bool fields is: ", falseBoolCount)
	return docs
}

func generateLargeSecondayKeyDocs(numDocs int, fieldName string) tc.KeyValues {
	prodfile := filepath.Join(proddir, "test2.prod")
	docs := GenerateJsons(numDocs, seed, prodfile, bagdir)
	for k, v := range docs {
		json := v.(map[string]interface{})
		json[fieldName] = randString(randomNum(600, 2000))
		docs[k] = json
	}
	return docs
}

func generateLargePrimaryKeyDocs(numDocs int,fieldName string) tc.KeyValues {
	prodfile := filepath.Join(proddir, "test2.prod")
	docs := GenerateJsons(numDocs, seed, prodfile, bagdir)
	largePrimaryKeyDocs := make(tc.KeyValues)
	for _, v := range docs {
		str := randString(randomNum(200, 210))
		largePrimaryKeyDocs[str] = v
	}
	return largePrimaryKeyDocs
}

func randomBool() bool {
	rand.Seed(time.Now().UnixNano())
	switch randomNum(0, 2) {
	case 0:
		return false
	case 1:
		return true
	}
	return true
}

func randomNum(min, max float64) int {
	rand.Seed(time.Now().UnixNano())
	return int(rand.Float64()*(max-min) + min)
}

func randString(n int) string {
	chars := []rune("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ,#")
	rand.Seed(time.Now().UnixNano())
	b := make([]rune, n)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return string(b)
}

func random_char() string {
	chars := []rune("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	return string(chars[rand.Intn(len(chars))])
}
