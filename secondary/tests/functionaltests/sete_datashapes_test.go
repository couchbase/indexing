package functionaltests

import (
	c "github.com/couchbase/indexing/secondary/common"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/datautility"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	tv "github.com/couchbase/indexing/secondary/tests/framework/validation"
	"log"
	"math/rand"
	"os/user"
	"path/filepath"
	"strconv"
	"testing"
	"time"
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
	time.Sleep(30 * time.Second)

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
	time.Sleep(30 * time.Second) // Sleep after bucket create or delete

	tc.ClearMap(docs)
	UpdateKVDocs(bucketDocs[0], docs)
}

//Test for large keys with special characters like \t & < >
// MB-30861
func TestLargeKeysSplChars(t *testing.T) {
	log.Printf("In TestLargeKeysSplChars()")

	var bucketName = "default"
	numDocs := 100
	prodfile := filepath.Join(proddir, "test2.prod")
	docsToCreate := GenerateJsons(numDocs, seed, prodfile, bagdir)
	for k, v := range docsToCreate {
		json := v.(map[string]interface{})
		json["str"] = splstr(randomNum(4000, 5000))
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

func splstr(strlen int) string {
	chars := []string{"\t", "&", "<", ">", "a", "1"}
	var newstr string
	for i := 0; i < strlen; i++ {
		newstr += chars[rand.Intn(len(chars))]
	}
	return newstr
}

func splstrArray(size, strlen int) []string {
	strArray := make([]string, 0, size)
	for i := 0; i < size; i++ {
		strArray = append(strArray, splstr(strlen))
	}
	return strArray
}

func nestedobj(depth, strlen int) interface{} {
	if depth == 0 {
		return nil
	}
	obj := make(map[string]interface{})
	obj[strconv.Itoa(depth)] = splstr(strlen)
	obj["f"+strconv.Itoa(depth)] = nestedobj(strlen, depth-1)
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
