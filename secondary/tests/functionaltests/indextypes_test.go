package functionaltests

import (
	"flag"
	"fmt"
	"github.com/couchbase/cbauth"
	"github.com/couchbase/indexing/secondary/common"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/datautility"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	tv "github.com/couchbase/indexing/secondary/tests/framework/validation"
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
	authURL := fmt.Sprintf("http://%s/_cbauth", kvaddress)
	// FIXME: for now we will say this client is goxdcr, but eventually
	// ns_server should accomodate test clients.
	rpcURL := fmt.Sprintf("http://%s/goxdcr", kvaddress)
	common.MaybeSetEnv("NS_SERVER_CBAUTH_RPC_URL", rpcURL)
	common.MaybeSetEnv("NS_SERVER_CBAUTH_USER", clusterconfig.Username)
	common.MaybeSetEnv("NS_SERVER_CBAUTH_PWD", clusterconfig.Password)
	cbauth.Default = cbauth.NewDefaultAuthenticator(authURL, nil)

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
	fmt.Println("Loading the default bucket")
	kvutility.SetKeyValues(docs, "default", "", clusterconfig.KVAddress)
}

// Test for single index field of data type float64
func TestSimpleIndex_FloatDataType(t *testing.T) {
	fmt.Println("In TestSimpleIndex_FloatDataType()")
	var indexName = "index_age"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"age"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	// Wait, else results in "Index not ready"
	time.Sleep(1 * time.Second)

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
	time.Sleep(1 * time.Second)

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
	time.Sleep(1 * time.Second)

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

	time.Sleep(1 * time.Second)

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
	time.Sleep(1 * time.Second)

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
	time.Sleep(1 * time.Second)

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
	time.Sleep(5 * time.Second)

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

	// Wait, else results in "Index not ready"
	time.Sleep(5 * time.Second)

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

	// Wait, else results in "Index not ready"
	time.Sleep(3 * time.Second)

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

	// Wait, else results in "Index not ready"
	time.Sleep(3 * time.Second)

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

	// Wait, else results in "Index not ready"
	time.Sleep(3 * time.Second)

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

	// Wait, else results in "Index not ready"
	time.Sleep(3 * time.Second)

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

	// Wait, else results in "Index not ready"
	time.Sleep(3 * time.Second)

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

	// Wait, else results in "Index not ready"
	time.Sleep(3 * time.Second)

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

	// Wait, else results in "Index not ready"
	time.Sleep(3 * time.Second)

	docScanResults := datautility.ExpectedScanResponse_bool(docs, "address.isresidential", false, 3)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{false}, []interface{}{false}, 3, true, defaultlimit)
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)
}

func TestLookupJsonObject(t *testing.T) {
	fmt.Println("In TestLookupJsonObject()")
	var indexName = "index_streetaddress"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"address.streetaddress"}, true)
	FailTestIfError(err, "Error in creating the index", t)
	time.Sleep(3 * time.Second)

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
	time.Sleep(3 * time.Second)

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
	time.Sleep(3 * time.Second)

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
	FailTestIfError(err, "Error in scan", t)
	tv.Validate(docScanResults, scanResults)
}

func TestLookupFloatDiffForms(t *testing.T) {
	fmt.Println("In TestLookupFloatDiffForms()")
	var indexName = "index_latitude"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, []string{"latitude"}, true)
	FailTestIfError(err, "Error in creating the index", t)

	// Wait, else results in "Index not ready"
	time.Sleep(1 * time.Second)

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
	time.Sleep(1 * time.Second) // Wait, else results in "Index not ready"

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

func FailTestIfError(err error, msg string, t *testing.T) {
	if err != nil {
		t.Fatal("%v: %v\n", msg, err)
	}
}
