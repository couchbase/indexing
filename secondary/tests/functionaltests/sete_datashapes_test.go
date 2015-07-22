package functionaltests

import (
	c "github.com/couchbase/indexing/secondary/common"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/datautility"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	tv "github.com/couchbase/indexing/secondary/tests/framework/validation"
	"log"
	"os/user"
	"path/filepath"
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
	scanResults, err := secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{valueToLookup}, true, defaultlimit, c.SessionConsistency, nil)
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
	scanResults, err := secondaryindex.Range(validIndexName, bucketName, indexScanAddress, []interface{}{"$4"}, []interface{}{"$7"}, 3, true, defaultlimit, c.SessionConsistency, nil)
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
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"A"}, []interface{}{"K"}, 3, true, defaultlimit, c.SessionConsistency, nil)
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
	docScanResults := datautility.ExpectedScanResponse_float64(bucketDocs[0], indexFields[0], 30, 50, 1)
	scanResults, err := secondaryindex.Range(indexName, bucketNames[0], indexScanAddress, []interface{}{30}, []interface{}{50}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan 1", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	// Scan index of second bucket
	docScanResults = datautility.ExpectedScanResponse_string(bucketDocs[1], indexFields[1], "F", "Q", 2)
	scanResults, err = secondaryindex.Range(indexName, bucketNames[1], indexScanAddress, []interface{}{"F"}, []interface{}{"Q"}, 2, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan 2", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	kvutility.EditBucket(bucketNames[0], "", clusterconfig.Username, clusterconfig.Password, kvaddress, "512")
	kvutility.DeleteBucket(bucketNames[1], "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	time.Sleep(30 * time.Second) // Sleep after bucket create or delete

	tc.ClearMap(docs)
	UpdateKVDocs(bucketDocs[0], docs)
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
