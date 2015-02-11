package functionaltests

import (
	"errors"
	"flag"
	"fmt"
	"github.com/couchbase/cbauth"
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

var docs, mut_docs tc.KeyValues
var defaultlimit int64 = 10000000
var kvaddress, indexManagementAddress, indexScanAddress string
var clusterconfig tc.ClusterConfiguration

func init() {
	fmt.Println("In init()")
	var configpath string
	flag.StringVar(&configpath, "cbconfig", "../config/clusterrun_conf.json", "Path of the configuration file with data about Couchbase Cluster")
	flag.Parse()
	clusterconfig = tc.GetClusterConfFromFile(configpath)
	kvaddress = clusterconfig.KVAddress
	indexManagementAddress = clusterconfig.KVAddress
	indexScanAddress = clusterconfig.KVAddress

	// setup cbauth
	if _, err := cbauth.InternalRetryDefaultInit(kvaddress, clusterconfig.Username, clusterconfig.Password); err != nil {
		log.Fatalf("Failed to initialize cbauth: %s", err)
	}
	secondaryindex.CheckCollation = true
	secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	time.Sleep(5 * time.Second)
	// Working with Users10k and Users_mut dataset.
	u, _ := user.Current()
	dataFilePath := filepath.Join(u.HomeDir, "testdata/Users10k.txt.gz")
	mutationFilePath := filepath.Join(u.HomeDir, "testdata/Users_mut.txt.gz")
	tc.DownloadDataFile(tc.IndexTypesStaticJSONDataS3, dataFilePath, false)
	tc.DownloadDataFile(tc.IndexTypesMutationJSONDataS3, mutationFilePath, false)
	docs = datautility.LoadJSONFromCompressedFile(dataFilePath, "docid")
	mut_docs = datautility.LoadJSONFromCompressedFile(mutationFilePath, "docid")
	fmt.Println("Emptying the default bucket")
	kvutility.DeleteKeys(docs, "default", "", clusterconfig.KVAddress)
	kvutility.DeleteKeys(mut_docs, "default", "", clusterconfig.KVAddress)
	time.Sleep(5 * time.Second)

	fmt.Println("In TestCreateIndexOnEmptyBucket()")
	var indexName = "index_eyeColor"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"eyeColor"}, true)
	tc.HandleError(err, "Error in creating the index")

	// Populate the bucket now
	fmt.Println("Populating the default bucket")
	kvutility.SetKeyValues(docs, "default", "", clusterconfig.KVAddress)
	time.Sleep(10 * time.Second) // Sleep for mutations to catch up
	docScanResults := datautility.ExpectedScanResponse_string(docs, "eyeColor", "b", "c", 3)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"b"}, []interface{}{"c"}, 3, true, defaultlimit)
	tc.HandleError(err, "Error in scan")
	tv.Validate(docScanResults, scanResults)
}

// Test for single index field of data type float64
func TestSimpleIndex_FloatDataType(t *testing.T) {
	fmt.Println("In TestSimpleIndex_FloatDataType()")
	var indexName = "index_age"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"age"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_float64(docs, "age", 35, 40, 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{35}, []interface{}{40}, 1, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)
}

// Test for single index field of data type string
func TestSimpleIndex_StringDataType(t *testing.T) {
	fmt.Println("In TestSimpleIndex_StringDataType()")
	var indexName = "index_company"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"company"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "company", "G", "M", 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"G"}, []interface{}{"M"}, 1, true, defaultlimit)
	FailTestIfError(err, "Error in scan 1", t)
	tv.Validate(docScanResults, scanResults)

	docScanResults = datautility.ExpectedScanResponse_string(docs, "company", "BIOSPAN", "ZILLANET", 1)
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"BIOSPAN"}, []interface{}{"ZILLANET"}, 1, true, defaultlimit)
	FailTestIfError(err, "Error in scan 2", t)
	tv.Validate(docScanResults, scanResults)
}

// Test for case sensitivity of index field values
func TestSimpleIndex_FieldValueCaseSensitivity(t *testing.T) {
	fmt.Println("In TestSimpleIndex_StringCaseSensitivity()")
	var indexName = "index_company"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"company"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "company", "B", "C", 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"B"}, []interface{}{"C"}, 1, true, defaultlimit)
	FailTestIfError(err, "Error in scan 1", t)
	tv.Validate(docScanResults, scanResults)

	docScanResults = datautility.ExpectedScanResponse_string(docs, "company", "B", "c", 1)
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"B"}, []interface{}{"c"}, 1, true, defaultlimit)
	FailTestIfError(err, "Error in scan 2", t)
	tv.Validate(docScanResults, scanResults)
}

// Test for single index field of data type bool
func TestSimpleIndex_BoolDataType(t *testing.T) {
	fmt.Println("In TestSimpleIndex_BoolDataType()")
	var indexName = "index_isActive"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"isActive"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_bool(docs, "isActive", true, 3)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{true}, []interface{}{true}, 3, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)
}

func TestBasicLookup(t *testing.T) {
	fmt.Println("In TestBasicLookup()")
	var indexName = "index_company"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"company"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "company", "BIOSPAN", "BIOSPAN", 3)
	scanResults, err := secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{"BIOSPAN"}, true, 10000000)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)
}

func TestIndexOnNonExistentField(t *testing.T) {
	fmt.Println("In TestIndexOnNonExistentField()")
	var indexName = "index_height"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"height"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_float64(docs, "height", 6.0, 6.5, 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{6.0}, []interface{}{6.5}, 1, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)
}

func TestIndexPartiallyMissingField(t *testing.T) {
	fmt.Println("In TestIndexPartiallyMissingField()")
	var indexName = "index_nationality"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"nationality"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "nationality", "A", "z", 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"A"}, []interface{}{"z"}, 1, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)
}

// Index field is float but scan for string
func TestScanNonMatchingDatatype(t *testing.T) {
	fmt.Println("In TestScanNonMatchingDatatype()")
	var indexName = "index_age"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"age"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "age", "35", "40", 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"35"}, []interface{}{"40"}, 1, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)
}

// Inclusion tests

// Inclusion 0
func TestInclusionNeither(t *testing.T) {
	fmt.Println("In TestInclusionNeither()")
	var indexName = "index_age"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"age"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_float64(docs, "age", 32, 36, 0)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{32}, []interface{}{36}, 0, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)
}

// Inclusion 1
func TestInclusionLow(t *testing.T) {
	fmt.Println("In TestInclusionLow()")
	var indexName = "index_age"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"age"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_float64(docs, "age", 32, 36, 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{32}, []interface{}{36}, 1, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)
}

// Inclusion 2
func TestInclusionHigh(t *testing.T) {
	fmt.Println("In TestInclusionHigh()")
	var indexName = "index_age"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"age"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_float64(docs, "age", 32, 36, 2)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{32}, []interface{}{36}, 2, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)
}

// Inclusion 3
func TestInclusionBoth(t *testing.T) {
	fmt.Println("In TestInclusionBoth()")
	var indexName = "index_age"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"age"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_float64(docs, "age", 32, 36, 3)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{32}, []interface{}{36}, 3, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)
}

func TestNestedIndex_String(t *testing.T) {
	fmt.Println("In TestNestedIndex_String()")
	var indexName = "index_streetname"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"address.streetaddress.streetname"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "address.streetaddress.streetname", "A", "z", 3)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"A"}, []interface{}{"z"}, 3, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)
}

func TestNestedIndex_Float(t *testing.T) {
	fmt.Println("In TestNestedIndex_Float()")
	var indexName = "index_floor"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"address.streetaddress.floor"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_float64(docs, "address.streetaddress.floor", 3, 6, 3)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{3}, []interface{}{6}, 3, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)
}

func TestNestedIndex_Bool(t *testing.T) {
	fmt.Println("In TestNestedIndex_Bool()")
	var indexName = "index_isresidential"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"address.isresidential"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_bool(docs, "address.isresidential", false, 3)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{false}, []interface{}{false}, 3, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)
}

func TestLookupJsonObject(t *testing.T) {
	fmt.Println("In TestLookupJsonObject()")
	var indexName = "index_streetaddress"
	var bucketName = "default"

	addDocIfNotPresentInKV("User3bf51f08-0bac-4c03-bcec-5c255cbdde2c")
	addDocIfNotPresentInKV("Userbb48952f-f8d1-4e04-a0e1-96b9019706fb")
	time.Sleep(2 * time.Second)

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"address.streetaddress"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	value := map[string]interface{}{
		"doornumber":   "12B",
		"floor":        5.0,
		"buildingname": "Sterling Heights",
		"streetname":   "Hill Street"}
	docScanResults := datautility.ExpectedLookupResponse_json(docs, "address.streetaddress", value)
	scanResults, err := secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{value}, true, defaultlimit)
	tc.PrintScanResults(docScanResults, "docScanResults")
	tc.PrintScanResults(scanResults, "scanResults")
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)
}

func TestLookupObjDifferentOrdering(t *testing.T) {
	fmt.Println("In TestLookupObjDifferentOrdering()")
	var indexName = "index_streetaddress"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"address.streetaddress"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	value := map[string]interface{}{
		"floor":        5.0,
		"streetname":   "Hill Street",
		"buildingname": "Sterling Heights",
		"doornumber":   "12B"}
	docScanResults := datautility.ExpectedLookupResponse_json(docs, "address.streetaddress", value)
	scanResults, err := secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{value}, true, defaultlimit)
	tc.PrintScanResults(docScanResults, "docScanResults")
	tc.PrintScanResults(scanResults, "scanResults")
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)
}

func TestRangeJsonObject(t *testing.T) {
	fmt.Println("In TestRangeJsonObject()")
	var indexName = "index_streetaddress"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"address.streetaddress"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	low := map[string]interface{}{
		"floor":        1.0,
		"streetname":   "AAA",
		"buildingname": "AA",
		"doornumber":   "AAAA"}
	high := map[string]interface{}{
		"floor":        9.0,
		"streetname":   "zzz",
		"buildingname": "zz",
		"doornumber":   "zzzz"}

	value1 := map[string]interface{}{
		"floor":        5.0,
		"streetname":   "Hill Street",
		"buildingname": "Sterling Heights",
		"doornumber":   "12B"}
	value2 := map[string]interface{}{
		"floor":        2.0,
		"streetname":   "Karweg Place",
		"buildingname": "Rosewood Gardens",
		"doornumber":   "514"}
	docScanResults := make(tc.ScanResponse)
	docScanResults["User3bf51f08-0bac-4c03-bcec-5c255cbdde2c"] = []interface{}{value1}
	docScanResults["Userbb48952f-f8d1-4e04-a0e1-96b9019706fb"] = []interface{}{value2}
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{low}, []interface{}{high}, 3, true, defaultlimit)
	tc.PrintScanResults(scanResults, "scanResults")
	tc.PrintScanResults(docScanResults, "docScanResults")
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)
}

func TestLookupFloatDiffForms(t *testing.T) {
	fmt.Println("In TestLookupFloatDiffForms()")
	var indexName = "index_latitude"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"latitude"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	// Scan 1
	fmt.Println("Scan 1")
	docScanResults := datautility.ExpectedScanResponse_float64(docs, "latitude", -13, 70, 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{-13}, []interface{}{70}, 1, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)

	// Scan 2
	fmt.Println("Scan 2")
	docScanResults = datautility.ExpectedScanResponse_float64(docs, "latitude", 4.112783, 4.112783, 3)
	scanResults, err = secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{4.112783}, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)

	// Scan 3
	fmt.Println("Scan 3")
	scanResults, err = secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{20.563915 / 5}, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)

	// Scan 4
	fmt.Println("Scan 4")
	scanResults, err = secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{2.0563915 * 2}, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)

	// Scan 5
	fmt.Println("Scan 5")
	docScanResults = datautility.ExpectedScanResponse_float64(docs, "latitude", 4.112783000, 4.112783000, 3)
	scanResults, err = secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{4.112783000}, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)

	// Scan 6
	fmt.Println("Scan 6")
	docScanResults = datautility.ExpectedScanResponse_float64(docs, "latitude", 4.112783333, 4.112783333, 3)
	scanResults, err = secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{4.112783333}, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)
}

func TestRangeFloatInclVariations(t *testing.T) {
	fmt.Println("In TestRangeFloatInclVariations()")
	var indexName = "index_latitude"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"latitude"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	// Scan 1. Value close to  -67.373265, Inclusion 0
	fmt.Println("Scan 1")
	docScanResults := datautility.ExpectedScanResponse_float64(docs, "latitude", -67.373365, -67.373165, 0)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{-67.373365}, []interface{}{-67.373165}, 0, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)

	// Scan 2. Value close to  -67.373265, Inclusion 1 ( >= low && < high) (val < low && val < high : Expected 0 result)
	fmt.Println("Scan 2")
	docScanResults = datautility.ExpectedScanResponse_float64(docs, "latitude", -67.3732649999, -67.373264, 1)
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{-67.3732649999}, []interface{}{-67.373264}, 1, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)

	// Scan 3. Value close to  -67.373265, Inclusion 2 ( > low && <= high) (val > low && val > high: Expect 0 result)
	fmt.Println("Scan 3")
	docScanResults = datautility.ExpectedScanResponse_float64(docs, "latitude", -67.373265999, -67.37326500001, 2)
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{-67.373265999}, []interface{}{-67.37326500001}, 2, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)

	// Scan 4. Value close to  -67.373265, Inclusion 2 ( > low && <= high) ( val > low && val < high: Expect 1 result)
	fmt.Println("Scan 4")
	docScanResults = datautility.ExpectedScanResponse_float64(docs, "latitude", -67.37326500001, -67.3732649999, 2)
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{-67.37326500001}, []interface{}{-67.3732649999}, 2, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)

	// Scan 5. Value close to  -67.373265, Inclusion 3 ( val == low && val < high : Expect 1 result)
	fmt.Println("Scan 5")
	docScanResults = datautility.ExpectedScanResponse_float64(docs, "latitude", -67.373265, -67.3732649999, 3)
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{-67.373265}, []interface{}{-67.3732649999}, 3, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)

	// Scan 6. Value close to  -67.373265, Inclusion 3 ( val == low && val > high : Expect 0 results)
	fmt.Println("Scan 6")
	docScanResults = datautility.ExpectedScanResponse_float64(docs, "latitude", -67.373265, -67.37326500001, 3)
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{-67.373265}, []interface{}{-67.37326500001}, 3, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)
}

func TestScanAll(t *testing.T) {
	fmt.Println("In TestScanAll()")
	var index1 = "index_name"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(index1, bucketName, indexManagementAddress, []string{"name"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "name", "A", "z", 3)
	fmt.Println("Length of docScanResults = ", len(docScanResults))
	scanResults, err := secondaryindex.ScanAll(index1, bucketName, indexScanAddress, defaultlimit)
	fmt.Println("Length of scanResults = ", len(scanResults))
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)
}

func TestScanAllNestedField(t *testing.T) {
	fmt.Println("In TestScanAllNestedField()")
	var index1 = "index_streetname"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(index1, bucketName, indexManagementAddress, []string{"address.streetaddress.streetname"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "address.streetaddress.streetname", "A", "z", 3)
	fmt.Println("Length of docScanResults = ", len(docScanResults))
	scanResults, err := secondaryindex.ScanAll(index1, bucketName, indexScanAddress, defaultlimit)
	fmt.Println("Length of scanResults = ", len(scanResults))
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)
}

func TestBasicPrimaryIndex(t *testing.T) {
	fmt.Println("In TestBasicPrimaryIndex()")
	var indexName = "index_p1"
	var bucketName = "default"

	err := secondaryindex.CreatePrimaryIndex(indexName, bucketName, indexManagementAddress, true)
	FailTestIfError(err, "Error in creating the index", t)

	// docScanResults := datautility.ExpectedScanResponse_float64(docs, "latitude", -67.373365, -67.373165, 0)
	scanResults, err := secondaryindex.ScanAll(indexName, bucketName, indexScanAddress, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	if len(scanResults) != len(docs) {
		fmt.Println("Len of scanResults is incorrect. Expected and Actual are", len(docs), len(scanResults))
		err = errors.New("Len of scanResults is incorrect.")
	}
	FailTestIfError(err, "Len of scanResults is incorrect", t)
}

func TestBasicNullDataType(t *testing.T) {
	fmt.Println("In TestBasicNullDataType()")
	var indexName = "index_email"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"email"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedLookupResponse_nil(docs, "email")
	scanResults, err := secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{nil}, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)

	// Scan all should include null : todo
}

func TestBasicArrayDataType_ScanAll(t *testing.T) {
	fmt.Println("In TestBasicArrayDataType_ScanAll()")
	var indexName = "index_tags"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"tags"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanAllResponse(docs, "tags")
	scanResults, err := secondaryindex.ScanAll(indexName, bucketName, indexScanAddress, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)
}

func TestBasicArrayDataType_Lookup(t *testing.T) {
	fmt.Println("In TestBasicArrayDataType_Lookup()")
	var indexName = "index_tags"
	var bucketName = "default"

	addDocIfNotPresentInKV("Usere46cea01-38f6-4e7b-92e5-69d64668ae75")
	time.Sleep(2 * time.Second)

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"tags"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	// Document has array: "reprehenderit", "tempor", "officia", "exercitation", "labore", "sunt", "tempor"
	arrayValue := []string{"reprehenderit", "tempor", "officia", "exercitation", "labore", "sunt", "tempor"}

	scanResults, err := secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{arrayValue}, true, defaultlimit)
	tc.PrintScanResults(scanResults, "scanResults")
	FailTestIfError(err, "Error in scan", t)
	if len(scanResults) != 1 {
		e := errors.New("Lookup should return exactly one result")
		FailTestIfError(e, "Error in array Lookup: ", t)
	}
	for k := range scanResults {
		if k != "Usere46cea01-38f6-4e7b-92e5-69d64668ae75" {
			e := errors.New("Lookup returned a wrong key")
			FailTestIfError(e, "Error in array Lookup: ", t)
		}
	}
}

func TestArrayDataType_LookupMissingArrayValue(t *testing.T) {
	fmt.Println("In TestArrayDataType_LookupMissingArrayValue()")
	var indexName = "index_tags"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"tags"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	// Document has array: "reprehenderit", "tempor", "officia", "exercitation", "labore", "sunt", "tempor"
	arrayValue := []string{"A", "B", "C", "D", "E", "F", "G"} // an array that doesnt exist in  tags field
	scanResults, err := secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{arrayValue}, true, defaultlimit)
	tc.PrintScanResults(scanResults, "scanResults")
	FailTestIfError(err, "Error in scan", t)
	if len(scanResults) != 0 {
		e := errors.New("Lookup should not return any doc key as array looked is missing in docs")
		FailTestIfError(e, "Error in array Lookup", t)
	}
}

func SkipTestArrayDataType_LookupWrongOrder(t *testing.T) {
	fmt.Println("In TestArrayDataType_LookupWrongOrder()")
	var indexName = "index_tags"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"tags"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	// Document has array: "reprehenderit", "tempor", "officia", "exercitation", "labore", "sunt", "tempor"
	arrayValue := []string{"reprehenderit", "tempor", "officia", "labore", "sunt", "tempor", "exercitation"} // Re-ordered the array elements
	scanResults, err := secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{arrayValue}, true, defaultlimit)
	tc.PrintScanResults(scanResults, "scanResults")
	FailTestIfError(err, "Error in scan", t)
	if len(scanResults) != 0 {
		e := errors.New("Lookup should not return any doc key as array was looked up in wrong order")
		FailTestIfError(e, "Error in array Lookup", t)
	}
}

func SkipTestArrayDataType_LookupSubset(t *testing.T) {
	fmt.Println("In TestArrayDataType_LookupSubset()")
	var indexName = "index_tags"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"tags"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	// Document has array: "reprehenderit", "tempor", "officia", "exercitation", "labore", "sunt", "tempor"
	arrayValue1 := []string{"reprehenderit", "exercitation", "labore", "sunt"} // Subset of the array elements
	// todo: arrayValue2 is a todo
	// arrayValue2 := []string{ "reprehenderit", "officia", "tempor", "exercitation", "labore", "sunt", "tempor"}    // Removed a repeating element from original arrayValue

	scanResults, err := secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{arrayValue1}, true, defaultlimit)
	tc.PrintScanResults(scanResults, "scanResults")
	FailTestIfError(err, "Error in scan", t)
	if len(scanResults) != 0 {
		e := errors.New("Lookup should not return any doc key as lookup key was array's subset")
		FailTestIfError(e, "Error in array Lookup", t)
	}
}

func TestScanLimitParameter(t *testing.T) {
	fmt.Println("In TestScanLimitParameter()")
	var indexName = "index_age"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"age"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	limit := 500
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{35}, []interface{}{40}, 1, true, int64(limit))
	FailTestIfError(err, "Error in scan", t)
	if len(scanResults) != limit {
		e := errors.New(fmt.Sprintf("Expected %d number of results but got %d results", limit, len(scanResults)))
		FailTestIfError(e, "Error in scan limit: ", t)
	}

	limit = 1000
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{35}, []interface{}{40}, 1, true, int64(limit))
	FailTestIfError(err, "Error in scan", t)
	if len(scanResults) != limit {
		e := errors.New(fmt.Sprintf("Expected %d number of results but got %d results", limit, len(scanResults)))
		FailTestIfError(e, "Error in scan limit: ", t)
	}

	// Todo: Verify the results in scanresults
}

func SkipTestScanDistinctParameter(t *testing.T) {
	fmt.Println("In TestScanDistinctParameter()")
	var indexName = "index_age"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"age"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	// var limit int
	// limit = 10
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{35}, []interface{}{40}, 1, true, defaultlimit)
	tc.PrintScanResults(scanResults, "scanResults")
	FailTestIfError(err, "Error in scan", t)
	// Todo: Verify the results in scanresults
}

func TestCountRange(t *testing.T) {
	fmt.Println("In TestRangeCount()")
	var indexName = "index_age"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"age"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_float64(docs, "age", 35, 40, 1)
	rangeCount, err := secondaryindex.CountRange(indexName, bucketName, indexScanAddress, []interface{}{35}, []interface{}{40}, 1)
	FailTestIfError(err, "Error in CountRange: ", t)
	fmt.Println("Count of expected and actual Range is: ", len(docScanResults), rangeCount)
	if int64(len(docScanResults)) != rangeCount {
		e := errors.New(fmt.Sprintf("Expected Range count %d does not match actual Range count %d: ", len(docScanResults), rangeCount))
		FailTestIfError(e, "Error in CountRange: ", t)
	}

	docScanResults = datautility.ExpectedScanResponse_float64(docs, "age", -10, 50, 2)
	rangeCount, err = secondaryindex.CountRange(indexName, bucketName, indexScanAddress, []interface{}{-10}, []interface{}{50}, 2)
	FailTestIfError(err, "Error in CountRange: ", t)
	fmt.Println("Count of expected and actual Range is: ", len(docScanResults), rangeCount)
	if int64(len(docScanResults)) != rangeCount {
		e := errors.New(fmt.Sprintf("Expected Range count %d does not match actual Range count %d: ", len(docScanResults), rangeCount))
		FailTestIfError(e, "Error in CountRange: ", t)
	}

	docScanResults = datautility.ExpectedScanResponse_float64(docs, "age", 45, 46, 2)
	rangeCount, err = secondaryindex.CountRange(indexName, bucketName, indexScanAddress, []interface{}{45}, []interface{}{46}, 2)
	FailTestIfError(err, "Error in CountRange: ", t)
	fmt.Println("Count of expected and actual Range is: ", len(docScanResults), rangeCount)
	if int64(len(docScanResults)) != rangeCount {
		e := errors.New(fmt.Sprintf("Expected Range count %d does not match actual Range count %d: ", len(docScanResults), rangeCount))
		FailTestIfError(e, "Error in CountRange: ", t)
	}

	docScanResults = datautility.ExpectedScanResponse_float64(docs, "age", 40, 50, 3)
	rangeCount, err = secondaryindex.CountRange(indexName, bucketName, indexScanAddress, []interface{}{40}, []interface{}{50}, 3)
	FailTestIfError(err, "Error in CountRange: ", t)
	fmt.Println("Count of expected and actual Range is: ", len(docScanResults), rangeCount)
	if int64(len(docScanResults)) != rangeCount {
		e := errors.New(fmt.Sprintf("Expected Range count %d does not match actual Range count %d: ", len(docScanResults), rangeCount))
		FailTestIfError(e, "Error in CountRange: ", t)
	}

	docScanResults = datautility.ExpectedScanResponse_float64(docs, "age", 55, 45, 3)
	rangeCount, err = secondaryindex.CountRange(indexName, bucketName, indexScanAddress, []interface{}{55}, []interface{}{45}, 3)
	FailTestIfError(err, "Error in CountRange: ", t)
	fmt.Println("Count of expected and actual Range is: ", len(docScanResults), rangeCount)
	if int64(len(docScanResults)) != rangeCount {
		e := errors.New(fmt.Sprintf("Expected Range count %d does not match actual Range count %d: ", len(docScanResults), rangeCount))
		FailTestIfError(e, "Error in CountRange: ", t)
	}
}

func TestCountLookup(t *testing.T) {
	fmt.Println("In TestCountLookup()")
	var indexName = "index_age"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"age"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_float64(docs, "age", 25, 25, 3)
	rangeCount, err := secondaryindex.CountLookup(indexName, bucketName, indexScanAddress, []interface{}{25})
	FailTestIfError(err, "Error in CountRange: ", t)
	fmt.Println("Count of expected and actual Range is: ", len(docScanResults), rangeCount)
	if int64(len(docScanResults)) != rangeCount {
		e := errors.New(fmt.Sprintf("Expected Range count %d does not match actual Range count %d: ", len(docScanResults), rangeCount))
		FailTestIfError(e, "Error in CountRange: ", t)
	}

	docScanResults = datautility.ExpectedScanResponse_float64(docs, "age", 75, 75, 3)
	rangeCount, err = secondaryindex.CountLookup(indexName, bucketName, indexScanAddress, []interface{}{75})
	FailTestIfError(err, "Error in CountRange: ", t)
	fmt.Println("Count of expected and actual Range is: ", len(docScanResults), rangeCount)
	if int64(len(docScanResults)) != rangeCount {
		e := errors.New(fmt.Sprintf("Expected Range count %d does not match actual Range count %d: ", len(docScanResults), rangeCount))
		FailTestIfError(e, "Error in CountRange: ", t)
	}
}

func FailTestIfError(err error, msg string, t *testing.T) {
	if err != nil {
		t.Fatal("%v: %v\n", msg, err)
	}
}

func addDocIfNotPresentInKV(docKey string) {
	if _, present := docs[docKey]; present == false {
		keysToBeSet := make(tc.KeyValues)
		keysToBeSet[docKey] = mut_docs[docKey]
		kvutility.SetKeyValues(keysToBeSet, "default", "", clusterconfig.KVAddress)
		// Update docs object with newly added keys and remove those keys from mut_docs
		docs[docKey] = mut_docs[docKey]
		delete(mut_docs, docKey)
	}
}
