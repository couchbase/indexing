package functionaltests

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os/user"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/datautility"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	tv "github.com/couchbase/indexing/secondary/tests/framework/validation"
)

func TestLargeDocumentSize(t *testing.T) {
	log.Printf("In TestLargeDocumentSize()")

	u, _ := user.Current()
	datapath := filepath.Join(u.HomeDir, "testdata/TwitterFeed1.txt.gz")
	tc.DownloadDataFile(tc.IndexTypesTwitterFeed1JSONDataS3, datapath, true)
	largeDocs := datautility.LoadJSONFromCompressedFile(datapath, "id_str")
	UpdateKVDocs(largeDocs, docs)

	log.Printf("Length of docs and largeDocs = %d and %d", len(docs), len(largeDocs))

	bucketName := "default"
	index1 := "index_userscreenname"

	kvutility.SetKeyValues(largeDocs, bucketName, "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndex(index1, bucketName, indexManagementAddress, "", []string{"`user`.screen_name"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanAllResponse(docs, "user.screen_name")
	scanResults, err := secondaryindex.ScanAll(index1, bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan 1: ", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan 1:  result validation", t)
}

func TestFieldsWithSpecialCharacters(t *testing.T) {
	log.Printf("In TestFieldsWithSpecialCharacters()")

	var bucketName = "default"
	var indexName = "index_specialchar"
	var field = "splfield"

	docsToCreate := generateDocsWithSpecialCharacters(1000, "users.prod", field)
	UpdateKVDocs(docsToCreate, docs)
	var valueToLookup string
	for _, v := range docsToCreate {
		json := v.(map[string]interface{})
		valueToLookup = json[field].(string)
		break
	}

	kvutility.SetKeyValues(docsToCreate, bucketName, "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{field}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	log.Printf("Looking up for value %v", valueToLookup)
	docScanResults := datautility.ExpectedScanResponse_string(docs, field, valueToLookup, valueToLookup, 3)
	scanResults, err := secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{valueToLookup}, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestLargeKeyLookup(t *testing.T) {
	if secondaryindex.IndexUsing == "forestdb" {
		log.Printf("Skipping test TestLargeKeyLookup() for forestdb")
		return
	}
	log.Printf("In TestLargeKeyLookup()")

	var bucketName = "default"
	var indexName = "index_largeKeyLookup"
	var field = "str"

	docsToCreate := generateDocsWithSpecialCharacters(1000, "users.prod", field)
	chars := []string{"\t", "&", "<", ">", "a", "1"}
	for k, v := range docsToCreate {
		json := v.(map[string]interface{})
		json["str"] = splstr(randomNum(4000, 5000), chars)
		docsToCreate[k] = json
	}
	UpdateKVDocs(docsToCreate, docs)

	var valueToLookup string
	for _, v := range docsToCreate {
		json := v.(map[string]interface{})
		valueToLookup = json[field].(string)
		break
	}

	kvutility.SetKeyValues(docsToCreate, bucketName, "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{field}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	log.Printf("Looking up for a large key")
	docScanResults := datautility.ExpectedScanResponse_string(docs, field, valueToLookup, valueToLookup, 3)
	scanResults, err := secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{valueToLookup}, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestIndexNameValidation(t *testing.T) {
	log.Printf("In TestIndexNameValidation()")

	var bucketName = "default"
	var validIndexName = "#primary-Index_test"
	var invalidIndexName = "ÌñÐÉx&(abc_%"
	var field = "balance"

	docsToCreate := generateDocs(1000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)

	seed++
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndex(invalidIndexName, bucketName, indexManagementAddress, "", []string{field}, false, nil, true, defaultIndexActiveTimeout, nil)
	errMsg := "Expected index name validation error for index " + invalidIndexName
	FailTestIfNoError(err, errMsg, t)
	log.Printf("Creation of index with invalid name %v failed as expected", invalidIndexName)

	err = secondaryindex.CreateSecondaryIndex(validIndexName, bucketName, indexManagementAddress, "", []string{field}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_string(docs, field, "$4", "$7", 3)
	scanResults, err := secondaryindex.Range(validIndexName, bucketName, indexScanAddress, []interface{}{"$4"}, []interface{}{"$7"}, 3, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestSameFieldNameAtDifferentLevels(t *testing.T) {
	log.Printf("In TestSameFieldNameAtDifferentLevels()")

	var bucketName = "default"
	var indexName = "cityindex"
	var field = "city"

	docsToCreate := generateDocs(1000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	docsToUpload := make(tc.KeyValues)

	for k, v := range docsToCreate {
		json := v.(map[string]interface{})
		address := json["address"].(map[string]interface{})
		city := address["city"].(string)
		json[field] = city
		address["city"] = "ThisIsNestedCity " + city
		docsToUpload[k] = json
	}

	seed++
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(docsToUpload, "default", "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{field}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_string(docs, field, "A", "K", 3)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"A"}, []interface{}{"K"}, 3, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestSameIndexNameInTwoBuckets(t *testing.T) {
	log.Printf("In TestSameIndexNameInTwoBuckets()")

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	numOfBuckets := 2
	indexName := "b_idx"
	indexFields := [...]string{"age", "address.city"}
	bucketNames := [...]string{"default", "buck2"}
	proxyPorts := [...]string{"11212", "11213"}
	var bucketDocs [2]tc.KeyValues

	// Update default bucket ram to 256
	// Create two more buckets with ram 256 each
	// Add different docs to all 3 buckets
	// Create index on each of them
	// Query the indexes

	kvutility.FlushBucket(bucketNames[0], "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.EditBucket(bucketNames[0], "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256")

	for i := 1; i < numOfBuckets; i++ {
		kvutility.CreateBucket(bucketNames[i], "sasl", "", clusterconfig.Username, clusterconfig.Password, kvaddress, "256", proxyPorts[i])
	}
	time.Sleep(bucketOpWaitDur * time.Second)

	log.Printf("Generating docs and Populating all the buckets")
	for i := 0; i < numOfBuckets; i++ {
		bucketDocs[i] = generateDocs(1000, "users.prod")
		kvutility.SetKeyValues(bucketDocs[i], bucketNames[i], "", clusterconfig.KVAddress)
		err := secondaryindex.CreateSecondaryIndex(indexName, bucketNames[i], indexManagementAddress, "", []string{indexFields[i]}, false, nil, true, defaultIndexActiveTimeout, nil)
		FailTestIfError(err, "Error in creating the index", t)
	}
	time.Sleep(3 * time.Second)

	// Scan index of first bucket
	docScanResults := datautility.ExpectedScanResponse_int64(bucketDocs[0], indexFields[0], 30, 50, 1)
	scanResults, err := secondaryindex.Range(indexName, bucketNames[0], indexScanAddress, []interface{}{30}, []interface{}{50}, 1, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan 1", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	// Scan index of second bucket
	docScanResults = datautility.ExpectedScanResponse_string(bucketDocs[1], indexFields[1], "F", "Q", 2)
	scanResults, err = secondaryindex.Range(indexName, bucketNames[1], indexScanAddress, []interface{}{"F"}, []interface{}{"Q"}, 2, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan 2", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	kvutility.EditBucket(bucketNames[0], "", clusterconfig.Username, clusterconfig.Password, kvaddress, "512")
	kvutility.DeleteBucket(bucketNames[1], "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	secondaryindex.RemoveClientForBucket(kvaddress, bucketNames[1])
	time.Sleep(bucketOpWaitDur * time.Second) // Sleep after bucket create or delete

	tc.ClearMap(docs)
	UpdateKVDocs(bucketDocs[0], docs)
}

//Test for large keys with special characters like \t & < >
// MB-30861
func TestLargeKeysSplChars(t *testing.T) {
	if secondaryindex.IndexUsing == "forestdb" {
		log.Printf("Skipping test TestLargeKeysSplChars() for forestdb")
		return
	}

	log.Printf("In TestLargeKeysSplChars()")

	chars := []string{"\t", "&", "<", ">", "a", "1"}
	var bucketName = "default"
	numDocs := 100
	prodfile := filepath.Join(proddir, "test2.prod")
	docsToCreate := GenerateJsons(numDocs, seed, prodfile, bagdir)
	for k, v := range docsToCreate {
		json := v.(map[string]interface{})
		json["str"] = splstr(randomNum(4000, 5000), chars)
		json["strarray"] = splstrArray(randomNum(50, 100), randomNum(4000, 5000))
		json["nestedobj"] = nestedobj(randomNum(5, 10), randomNum(4000, 5000))
		docsToCreate[k] = json
	}
	UpdateKVDocs(docsToCreate, docs)

	kvutility.SetKeyValues(docsToCreate, bucketName, "", clusterconfig.KVAddress)
	err := secondaryindex.CreateSecondaryIndex("idspl1", bucketName, indexManagementAddress, "", []string{"str"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index idspl1", t)
	err = secondaryindex.CreateSecondaryIndex("idspl2", bucketName, indexManagementAddress, "", []string{"ALL strarray"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index idspl2", t)
	err = secondaryindex.CreateSecondaryIndex("idspl3", bucketName, indexManagementAddress, "", []string{"nestedobj"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index idspl3", t)

	docScanResults := datautility.ExpectedScanAllResponse(docs, "str")
	scanResults, err := secondaryindex.ScanAll("idspl1", bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan 1: ", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan 1:  result validation", t)

	docScanResults1 := datautility.ExpectedArrayScanResponse_string(docs, "strarray", " ", "z", 3, false)
	scanResults1, err := secondaryindex.ArrayIndex_Range("idspl2", bucketName, indexScanAddress, []interface{}{" "}, []interface{}{"z"}, 3, false, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan 2: ", t)
	err = tv.ValidateArrayResult(docScanResults1, scanResults1)
	FailTestIfError(err, "Error in scan 2:  result validation", t)

	docScanResults = datautility.ExpectedScanAllResponse(docs, "nestedobj")
	scanResults, err = secondaryindex.ScanAll("idspl3", bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan 3: ", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan 3:  result validation", t)
}

func TestVeryLargeIndexKey(t *testing.T) {
	if secondaryindex.IndexUsing == "forestdb" {
		log.Printf("Skipping test TestVeryLargeIndexKey() for forestdb")
		return
	}

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	tc.ClearMap(docs)
	kvutility.FlushBucket("default", "", clusterconfig.Username,
		clusterconfig.Password, clusterconfig.KVAddress)

	log.Printf("TestVeryLargeIndexKey:: Flushed the bucket")

	expResponse := make(tc.ScanResponse)
	expResponse2 := make(tc.ScanResponse)

	key := "VeryLargeIndexKey1"
	doc := make(map[string]interface{})
	val := make(map[string]interface{})
	v1 := make(map[string]interface{})
	v2 := make(map[string]interface{})
	chars := []string{"\t", "&", "<", ">"}
	v3 := splstr(3*1024*1024, chars)
	v2["b"] = v3
	v1["a"] = v2
	val["v"] = v1
	doc[key] = val

	expResponse[key] = make([]interface{}, 1)
	expResponse[key][0] = v1
	expResponse2[key] = make([]interface{}, 2)
	expResponse2[key][0] = v1
	expResponse2[key][1] = v2

	key1 := "VeryLargeIndexKey2"
	val = make(map[string]interface{})
	v1 = make(map[string]interface{})
	v2 = make(map[string]interface{})
	chars = []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	v3 = splstr(19*1024*1024, chars)
	v2["b"] = v3
	v1["a"] = v2
	val["v"] = v1
	doc[key1] = val

	expResponse[key1] = make([]interface{}, 1)
	expResponse[key1][0] = v1
	expResponse2[key1] = make([]interface{}, 2)
	expResponse2[key1][0] = v1
	expResponse2[key1][1] = v2

	log.Printf("clusterconfig.KVAddress = %v", clusterconfig.KVAddress)
	kvutility.SetKeyValues(doc, "default", "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndex("i1", "default", indexManagementAddress, "",
		[]string{"v"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	scanResults, err := secondaryindex.ScanAll("i1", "default", indexScanAddress,
		defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan 1: ", t)
	err = tv.Validate(expResponse, scanResults)
	FailTestIfError(err, "Error in scan 1:  result validation", t)

	err = secondaryindex.CreateSecondaryIndex("i2", "default", indexManagementAddress, "",
		[]string{"v", "v.a"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	scanResults, err = secondaryindex.ScanAll("i2", "default", indexScanAddress,
		defaultlimit, c.SessionConsistency, nil)
	err = tv.Validate(expResponse2, scanResults)
	FailTestIfError(err, "Error in scan 1:  result validation", t)

	e = secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	kvutility.FlushBucket("default", "", clusterconfig.Username,
		clusterconfig.Password, clusterconfig.KVAddress)
}

// Index key size = c.TEMP_BUF_SIZE +/- delta
// delta = 10%
// Index key data types
// 1. Simple data types like strings and numbers (of type int and float)
// 2. Composite data types like arrays and nested objects
//    where the arrays and nested objects can contain numbers and strings
// Here c.SECKEY_TEMP_BUF_PADDING will be ignored on purpose.
func TestTempBufScanResult(t *testing.T) {
	if secondaryindex.IndexUsing == "forestdb" {
		log.Printf("Skipping test TestTempBufScanResult() for forestdb")
		return
	}

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	tc.ClearMap(docs)
	kvutility.FlushBucket("default", "", clusterconfig.Username,
		clusterconfig.Password, clusterconfig.KVAddress)

	log.Printf("TestTempBufScanResult:: Flushed the bucket")

	strlen := c.TEMP_BUF_SIZE
	newdocs := make(map[string]interface{})
	expResponse := make(tc.ScanResponse)

	// Array of floats
	var floats interface{}
	err := json.Unmarshal([]byte(generateFloatArrayJson(strlen, float64(10.0))), &floats)
	FailTestIfError(err, "Error in json.Unmarshal generateFloatArrayJson", t)
	m := make(map[string]interface{})
	m["idxKey"] = floats
	newdocs["k1"] = m
	expResponse["k1"] = make([]interface{}, 1)
	expResponse["k1"][0] = floats

	// Array of strings
	var strings interface{}
	strArr := generateStringArrayJson(strlen, float64(10.0))
	err = json.Unmarshal([]byte(strArr), &strings)
	FailTestIfError(err, "Error in json.Unmarshal generateStringArrayJson", t)
	m = make(map[string]interface{})
	m["idxKey"] = strings
	newdocs["k2"] = m
	expResponse["k2"] = make([]interface{}, 1)
	expResponse["k2"][0] = strings

	// A long string
	chars := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	newdocs["k3"] = make(map[string]interface{})
	l := recalcStrlen(strlen, float64(0.0))
	s := splstr(l, chars)
	m = make(map[string]interface{})
	m["idxKey"] = s
	newdocs["k3"] = m
	expResponse["k3"] = make([]interface{}, 1)
	expResponse["k3"][0] = s

	// Another long string
	newdocs["k4"] = make(map[string]interface{})
	l = recalcStrlen(strlen, float64(10.0))
	s = splstr(l, chars)
	m = make(map[string]interface{})
	m["idxKey"] = s
	newdocs["k4"] = m
	expResponse["k4"] = make([]interface{}, 1)
	expResponse["k4"][0] = s

	// Nested object of strings
	newdocs["k5"] = make(map[string]interface{})
	o := nestedobj(4, strlen/4)
	m = make(map[string]interface{})
	m["idxKey"] = o
	newdocs["k5"] = m
	expResponse["k5"] = make([]interface{}, 1)
	expResponse["k5"][0] = o

	// Array of a string and a float
	l = recalcStrlen(strlen, float64(10.0))
	a := make([]byte, 0, l)
	s = "[\"" + splstr(l, chars) + "\"," + "0.12" + "]"
	a = append(a, []byte(s)...)
	var arr interface{}
	err = json.Unmarshal(a, &arr)
	FailTestIfError(err, "Error in json.Unmarshal: array of a string and a float", t)
	newdocs["k6"] = make(map[string]interface{})
	m = make(map[string]interface{})
	m["idxKey"] = arr
	newdocs["k6"] = m
	expResponse["k6"] = make([]interface{}, 1)
	expResponse["k6"][0] = arr

	// Nested object of floats
	newdocs["k7"] = make(map[string]interface{})
	o = nestedobjFloat(256)
	m = make(map[string]interface{})
	m["idxKey"] = o
	newdocs["k7"] = m
	expResponse["k7"] = make([]interface{}, 1)
	expResponse["k7"][0] = o

	// Set docs in kv
	kvutility.SetKeyValues(newdocs, "default", "", clusterconfig.KVAddress)

	// Create index
	err = secondaryindex.CreateSecondaryIndex("index_idxKey", "default", indexManagementAddress, "",
		[]string{"idxKey"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	// Scan results
	scanResults, err := secondaryindex.ScanAll("index_idxKey", "default", indexScanAddress,
		defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan 1: ", t)
	err = tv.Validate(expResponse, scanResults)
	FailTestIfError(err, "Error in scan 1:  result validation", t)

	// Cleanup
	e = secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	kvutility.FlushBucket("default", "", clusterconfig.Username,
		clusterconfig.Password, clusterconfig.KVAddress)
}

func recalcStrlen(strlen int, deltaPercent float64) int {
	delta := float64(strlen) * rand.Float64() * deltaPercent / 100.0
	if rand.Intn(100) > 50 {
		delta *= -1
	}
	return strlen + int(delta)
}

func generateFloatArrayJson(strlen int, deltaPercent float64) string {
	strlen = recalcStrlen(strlen, deltaPercent)

	sampleFloats := []string{"0.12", "1.23", "123.456", "123456.123456",
		"12345.48732468362846238674", "-0.34", "-1.67", "-456.456",
		"-98765.98765", "-12345.3984573498759784395749545"}

	l := len(sampleFloats)
	str := make([]byte, 0, strlen)
	str = append(str, "["...)
	for i := 0; i < strlen; {
		v := sampleFloats[rand.Intn(l)] + ","
		str = append(str, v...)
		i += len(v)
	}
	// Remove last comma.
	str = str[:len(str)-1]
	str = append(str, "]"...)
	return string(str)
}

func generateStringArrayJson(strlen int, deltaPercent float64) string {
	strlen = recalcStrlen(strlen, deltaPercent)
	str := make([]byte, 0, strlen)
	str = append(str, "["...)

	chars := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	for i := 0; i < strlen; {
		v := "\"" + splstr(rand.Intn(int(strlen/4)), chars) + "\","
		str = append(str, v...)
		i += len(v)
	}
	// Remove last comma.
	str = str[:len(str)-1]
	str = append(str, "]"...)
	return string(str)
}

func splstr(strlen int, chars []string) string {
	newstr := make([]byte, strlen)
	for i := 0; i < strlen; i++ {
		newstr[i] = chars[rand.Intn(len(chars))][0]
	}
	return fmt.Sprintf("%s", newstr)
}

func splstrArray(size, strlen int) []string {
	chars := []string{"\t", "&", "<", ">", "a", "1"}
	strArray := make([]string, 0, size)
	for i := 0; i < size; i++ {
		strArray = append(strArray, splstr(strlen, chars))
	}
	return strArray
}

func nestedobj(depth, strlen int) interface{} {
	chars := []string{"\t", "&", "<", ">", "a", "1"}
	if depth == 0 {
		return nil
	}
	obj := make(map[string]interface{})
	obj[strconv.Itoa(depth)] = splstr(strlen, chars)
	obj["f"+strconv.Itoa(depth)] = nestedobj(strlen, depth-1)
	return obj
}

func nestedobjFloat(depth int) interface{} {
	if depth == 0 {
		return nil
	}
	sampleFloats := []float64{0.12, 1.23, 123.456, 123456.123456, 0.000000124,
		12345.48732468362846238674, -0.34, -1.67, -456.456, -0.00000000789,
		-98765.98765, -12345.3984573498759784395749545}
	l := len(sampleFloats)
	obj := make(map[string]interface{})
	obj[strconv.Itoa(depth)] = sampleFloats[rand.Intn(l)]
	obj["f"+strconv.Itoa(depth)] = nestedobjFloat(depth - 1)
	return obj
}

func generateDocsWithSpecialCharacters(numDocs int, prodFileName, field string) tc.KeyValues {
	prodfile := filepath.Join(proddir, prodFileName)
	docsToCreate := GenerateJsons(numDocs, seed, prodfile, bagdir)
	for k, v := range docsToCreate {
		json := v.(map[string]interface{})
		json[field] = randSpecialString(7)
		docsToCreate[k] = json
	}
	return docsToCreate
}
