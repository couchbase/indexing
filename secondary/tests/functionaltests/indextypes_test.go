package functionaltests

import (
	"errors"
	"fmt"
	c "github.com/couchbase/indexing/secondary/common"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/datautility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	tv "github.com/couchbase/indexing/secondary/tests/framework/validation"
	"log"
	"testing"
	"time"
)

// Test for single index field of data type float64
func TestSimpleIndex_FloatDataType(t *testing.T) {
	log.Printf("In TestSimpleIndex_FloatDataType()")
	var indexName = "index_age"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"age"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_float64(docs, "age", 35, 40, 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{35}, []interface{}{40}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

// Test for single index field of data type string
func TestSimpleIndex_StringDataType(t *testing.T) {
	log.Printf("In TestSimpleIndex_StringDataType()")
	var indexName = "index_company"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"company"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "company", "G", "M", 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"G"}, []interface{}{"M"}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan 1", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docScanResults = datautility.ExpectedScanResponse_string(docs, "company", "BIOSPAN", "ZILLANET", 1)
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"BIOSPAN"}, []interface{}{"ZILLANET"}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan 2", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

// Test for case sensitivity of index field values
func TestSimpleIndex_FieldValueCaseSensitivity(t *testing.T) {
	log.Printf("In TestSimpleIndex_StringCaseSensitivity()")
	var indexName = "index_company"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"company"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "company", "B", "C", 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"B"}, []interface{}{"C"}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan 1", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docScanResults = datautility.ExpectedScanResponse_string(docs, "company", "B", "c", 1)
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"B"}, []interface{}{"c"}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan 2", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

// Test for single index field of data type bool
func TestSimpleIndex_BoolDataType(t *testing.T) {
	log.Printf("In TestSimpleIndex_BoolDataType()")
	var indexName = "index_isActive"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"isActive"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_bool(docs, "isActive", true, 3)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{true}, []interface{}{true}, 3, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestBasicLookup(t *testing.T) {
	log.Printf("In TestBasicLookup()")
	var indexName = "index_company"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"company"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "company", "BIOSPAN", "BIOSPAN", 3)
	scanResults, err := secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{"BIOSPAN"}, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestIndexOnNonExistentField(t *testing.T) {
	log.Printf("In TestIndexOnNonExistentField()")
	var indexName = "index_height"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"height"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_float64(docs, "height", 6.0, 6.5, 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{6.0}, []interface{}{6.5}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestIndexPartiallyMissingField(t *testing.T) {
	log.Printf("In TestIndexPartiallyMissingField()")
	var indexName = "index_nationality"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"nationality"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "nationality", "A", "z", 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"A"}, []interface{}{"z"}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

// Index field is float but scan for string
func TestScanNonMatchingDatatype(t *testing.T) {
	log.Printf("In TestScanNonMatchingDatatype()")
	var indexName = "index_age"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"age"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "age", "35", "40", 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"35"}, []interface{}{"40"}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

// Inclusion tests

// Inclusion 0
func TestInclusionNeither(t *testing.T) {
	log.Printf("In TestInclusionNeither()")
	var indexName = "index_age"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"age"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_float64(docs, "age", 32, 36, 0)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{32}, []interface{}{36}, 0, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

// Inclusion 1
func TestInclusionLow(t *testing.T) {
	log.Printf("In TestInclusionLow()")
	var indexName = "index_age"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"age"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_float64(docs, "age", 32, 36, 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{32}, []interface{}{36}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

// Inclusion 2
func TestInclusionHigh(t *testing.T) {
	log.Printf("In TestInclusionHigh()")
	var indexName = "index_age"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"age"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_float64(docs, "age", 32, 36, 2)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{32}, []interface{}{36}, 2, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

// Inclusion 3
func TestInclusionBoth(t *testing.T) {
	log.Printf("In TestInclusionBoth()")
	var indexName = "index_age"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"age"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_float64(docs, "age", 32, 36, 3)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{32}, []interface{}{36}, 3, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestNestedIndex_String(t *testing.T) {
	log.Printf("In TestNestedIndex_String()")
	var indexName = "index_streetname"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"address.streetaddress.streetname"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "address.streetaddress.streetname", "A", "z", 3)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"A"}, []interface{}{"z"}, 3, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestNestedIndex_Float(t *testing.T) {
	log.Printf("In TestNestedIndex_Float()")
	var indexName = "index_floor"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"address.streetaddress.floor"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_float64(docs, "address.streetaddress.floor", 3, 6, 3)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{3}, []interface{}{6}, 3, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestNestedIndex_Bool(t *testing.T) {
	log.Printf("In TestNestedIndex_Bool()")
	var indexName = "index_isresidential"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"address.isresidential"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_bool(docs, "address.isresidential", false, 3)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{false}, []interface{}{false}, 3, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestLookupJsonObject(t *testing.T) {
	log.Printf("In TestLookupJsonObject()")
	var indexName = "index_streetaddress"
	var bucketName = "default"

	addDocIfNotPresentInKV("User3bf51f08-0bac-4c03-bcec-5c255cbdde2c")
	addDocIfNotPresentInKV("Userbb48952f-f8d1-4e04-a0e1-96b9019706fb")
	time.Sleep(2 * time.Second)

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"address.streetaddress"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	value := map[string]interface{}{
		"doornumber":   "12B",
		"floor":        5.0,
		"buildingname": "Sterling Heights",
		"streetname":   "Hill Street"}
	docScanResults := datautility.ExpectedLookupResponse_json(docs, "address.streetaddress", value)
	scanResults, err := secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{value}, true, defaultlimit, c.SessionConsistency, nil)
	tc.PrintScanResults(docScanResults, "docScanResults")
	tc.PrintScanResults(scanResults, "scanResults")
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestLookupObjDifferentOrdering(t *testing.T) {
	log.Printf("In TestLookupObjDifferentOrdering()")
	var indexName = "index_streetaddress"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"address.streetaddress"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	value := map[string]interface{}{
		"floor":        5.0,
		"streetname":   "Hill Street",
		"buildingname": "Sterling Heights",
		"doornumber":   "12B"}
	docScanResults := datautility.ExpectedLookupResponse_json(docs, "address.streetaddress", value)
	scanResults, err := secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{value}, true, defaultlimit, c.SessionConsistency, nil)
	tc.PrintScanResults(docScanResults, "docScanResults")
	tc.PrintScanResults(scanResults, "scanResults")
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestRangeJsonObject(t *testing.T) {
	log.Printf("In TestRangeJsonObject()")
	var indexName = "index_streetaddress"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"address.streetaddress"}, false, nil, true, defaultIndexActiveTimeout, nil)
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
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{low}, []interface{}{high}, 3, true, defaultlimit, c.SessionConsistency, nil)
	tc.PrintScanResults(scanResults, "scanResults")
	tc.PrintScanResults(docScanResults, "docScanResults")
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestLookupFloatDiffForms(t *testing.T) {
	log.Printf("In TestLookupFloatDiffForms()")
	var indexName = "index_latitude"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"latitude"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	// Scan 1
	log.Printf("Scan 1")
	docScanResults := datautility.ExpectedScanResponse_float64(docs, "latitude", -13, 70, 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{-13}, []interface{}{70}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	// Scan 2
	log.Printf("Scan 2")
	docScanResults = datautility.ExpectedScanResponse_float64(docs, "latitude", 4.112783, 4.112783, 3)
	scanResults, err = secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{4.112783}, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	// Scan 3
	log.Printf("Scan 3")
	scanResults, err = secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{20.563915 / 5}, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	// Scan 4
	log.Printf("Scan 4")
	scanResults, err = secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{2.0563915 * 2}, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	// Scan 5
	log.Printf("Scan 5")
	docScanResults = datautility.ExpectedScanResponse_float64(docs, "latitude", 4.112783000, 4.112783000, 3)
	scanResults, err = secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{4.112783000}, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	// Scan 6
	log.Printf("Scan 6")
	docScanResults = datautility.ExpectedScanResponse_float64(docs, "latitude", 4.112783333, 4.112783333, 3)
	scanResults, err = secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{4.112783333}, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestRangeFloatInclVariations(t *testing.T) {
	log.Printf("In TestRangeFloatInclVariations()")
	var indexName = "index_latitude"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"latitude"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	// Scan 1. Value close to  -67.373265, Inclusion 0
	log.Printf("Scan 1")
	docScanResults := datautility.ExpectedScanResponse_float64(docs, "latitude", -67.373365, -67.373165, 0)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{-67.373365}, []interface{}{-67.373165}, 0, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	// Scan 2. Value close to  -67.373265, Inclusion 1 ( >= low && < high) (val < low && val < high : Expected 0 result)
	log.Printf("Scan 2")
	docScanResults = datautility.ExpectedScanResponse_float64(docs, "latitude", -67.3732649999, -67.373264, 1)
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{-67.3732649999}, []interface{}{-67.373264}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	// Scan 3. Value close to  -67.373265, Inclusion 2 ( > low && <= high) (val > low && val > high: Expect 0 result)
	log.Printf("Scan 3")
	docScanResults = datautility.ExpectedScanResponse_float64(docs, "latitude", -67.373265999, -67.37326500001, 2)
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{-67.373265999}, []interface{}{-67.37326500001}, 2, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	// Scan 4. Value close to  -67.373265, Inclusion 2 ( > low && <= high) ( val > low && val < high: Expect 1 result)
	log.Printf("Scan 4")
	docScanResults = datautility.ExpectedScanResponse_float64(docs, "latitude", -67.37326500001, -67.3732649999, 2)
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{-67.37326500001}, []interface{}{-67.3732649999}, 2, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	// Scan 5. Value close to  -67.373265, Inclusion 3 ( val == low && val < high : Expect 1 result)
	log.Printf("Scan 5")
	docScanResults = datautility.ExpectedScanResponse_float64(docs, "latitude", -67.373265, -67.3732649999, 3)
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{-67.373265}, []interface{}{-67.3732649999}, 3, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	// Scan 6. Value close to  -67.373265, Inclusion 3 ( val == low && val > high : Expect 0 results)
	log.Printf("Scan 6")
	docScanResults = datautility.ExpectedScanResponse_float64(docs, "latitude", -67.373265, -67.37326500001, 3)
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{-67.373265}, []interface{}{-67.37326500001}, 3, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestScanAll(t *testing.T) {
	log.Printf("In TestScanAll()")
	var index1 = "index_name"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(index1, bucketName, indexManagementAddress, "", []string{"name"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "name", "A", "z", 3)
	log.Printf("Length of docScanResults = %d", len(docScanResults))
	scanResults, err := secondaryindex.ScanAll(index1, bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	log.Printf("Length of scanResults = %d", len(scanResults))
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestScanAllNestedField(t *testing.T) {
	log.Printf("In TestScanAllNestedField()")
	var index1 = "index_streetname"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(index1, bucketName, indexManagementAddress, "", []string{"address.streetaddress.streetname"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "address.streetaddress.streetname", "A", "z", 3)
	log.Printf("Length of docScanResults = %d", len(docScanResults))
	scanResults, err := secondaryindex.ScanAll(index1, bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	log.Printf("Length of scanResults = %d", len(scanResults))
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestBasicPrimaryIndex(t *testing.T) {
	log.Printf("In TestBasicPrimaryIndex()")
	var indexName = "index_p1"
	var bucketName = "default"

	// Create a primary index
	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", nil, true, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	// docScanResults := datautility.ExpectedScanResponse_float64(docs, "latitude", -67.373365, -67.373165, 0)
	scanResults, err := secondaryindex.ScanAll(indexName, bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	if len(scanResults) != len(docs) {
		log.Printf("Len of scanResults is incorrect. Expected and Actual are %d and %d", len(docs), len(scanResults))
		err = errors.New("Len of scanResults is incorrect.")
	}
	FailTestIfError(err, "Len of scanResults is incorrect", t)

	docScanResults := datautility.ExpectedScanResponse_RangePrimary(docs, "User2", "User5", 3)
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"User2"}, []interface{}{"User5"}, 3, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation of Primary Range", t)

	// Count Range on primary index
	docScanResults = datautility.ExpectedScanResponse_RangePrimary(docs, "User2", "User5", 3)
	rangeCount, err := secondaryindex.CountRange(indexName, bucketName, indexScanAddress, []interface{}{"User2"}, []interface{}{"User5"}, 3, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in CountRange: ", t)
	log.Printf("CountRange() expected and actual is:  %d and %d", len(docScanResults), rangeCount)
	if int64(len(docScanResults)) != rangeCount {
		e := errors.New(fmt.Sprintf("Expected Range count %d does not match actual Range count %d: ", len(docScanResults), rangeCount))
		FailTestIfError(e, "Error in CountRange: ", t)
	}

	var lookupkey string
	for k := range docs {
		lookupkey = k
		break
	}
	log.Printf("lookupkey for CountLookup() = %v", lookupkey)

	// Count Lookup on primary index
	docScanResults = datautility.ExpectedScanResponse_RangePrimary(docs, lookupkey, lookupkey, 3)
	lookupCount, err := secondaryindex.CountLookup(indexName, bucketName, indexScanAddress, []interface{}{lookupkey}, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in CountRange: ", t)
	if int64(len(docScanResults)) != lookupCount {
		e := errors.New(fmt.Sprintf("Expected Lookup count %d does not match actual Range count %d: ", len(docScanResults), lookupCount))
		FailTestIfError(e, "Error in CountRange: ", t)
	}
	log.Printf("CountLookup() = %v", lookupCount)
}

func TestBasicNullDataType(t *testing.T) {
	log.Printf("In TestBasicNullDataType()")
	var indexName = "index_email"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"email"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedLookupResponse_nil(docs, "email")
	scanResults, err := secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{nil}, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	// Scan all should include null : todo
}

func TestBasicArrayDataType_ScanAll(t *testing.T) {
	log.Printf("In TestBasicArrayDataType_ScanAll()")
	var indexName = "index_tags"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"tags"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanAllResponse(docs, "tags")
	scanResults, err := secondaryindex.ScanAll(indexName, bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestBasicArrayDataType_Lookup(t *testing.T) {
	log.Printf("In TestBasicArrayDataType_Lookup()")
	var indexName = "index_tags"
	var bucketName = "default"

	addDocIfNotPresentInKV("Usere46cea01-38f6-4e7b-92e5-69d64668ae75")
	time.Sleep(2 * time.Second)

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"tags"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	// Document has array: "reprehenderit", "tempor", "officia", "exercitation", "labore", "sunt", "tempor"
	arrayValue := []string{"reprehenderit", "tempor", "officia", "exercitation", "labore", "sunt", "tempor"}

	scanResults, err := secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{arrayValue}, true, defaultlimit, c.SessionConsistency, nil)
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
	log.Printf("In TestArrayDataType_LookupMissingArrayValue()")
	var indexName = "index_tags"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"tags"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	// Document has array: "reprehenderit", "tempor", "officia", "exercitation", "labore", "sunt", "tempor"
	arrayValue := []string{"A", "B", "C", "D", "E", "F", "G"} // an array that doesnt exist in  tags field
	scanResults, err := secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{arrayValue}, true, defaultlimit, c.SessionConsistency, nil)
	tc.PrintScanResults(scanResults, "scanResults")
	FailTestIfError(err, "Error in scan", t)
	if len(scanResults) != 0 {
		e := errors.New("Lookup should not return any doc key as array looked is missing in docs")
		FailTestIfError(e, "Error in array Lookup", t)
	}
}

func TestArrayDataType_LookupWrongOrder(t *testing.T) {
	log.Printf("In TestArrayDataType_LookupWrongOrder()")
	var indexName = "index_tags"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"tags"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	// Document has array: "reprehenderit", "tempor", "officia", "exercitation", "labore", "sunt", "tempor"
	arrayValue := []string{"reprehenderit", "tempor", "officia", "labore", "sunt", "tempor", "exercitation"} // Re-ordered the array elements
	scanResults, err := secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{arrayValue}, true, defaultlimit, c.SessionConsistency, nil)
	tc.PrintScanResults(scanResults, "scanResults")
	FailTestIfError(err, "Error in scan", t)
	if len(scanResults) != 0 {
		e := errors.New("Lookup should not return any doc key as array was looked up in wrong order")
		FailTestIfError(e, "Error in array Lookup", t)
	}
}

func TestArrayDataType_LookupSubset(t *testing.T) {
	log.Printf("In TestArrayDataType_LookupSubset()")
	var indexName = "index_tags"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"tags"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	// Document has array: "reprehenderit", "tempor", "officia", "exercitation", "labore", "sunt", "tempor"
	arrayValue1 := []string{"reprehenderit", "exercitation", "labore", "sunt"} // Subset of the array elements
	// todo: arrayValue2 is a todo
	// arrayValue2 := []string{ "reprehenderit", "officia", "tempor", "exercitation", "labore", "sunt", "tempor"}    // Removed a repeating element from original arrayValue

	scanResults, err := secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{arrayValue1}, true, defaultlimit, c.SessionConsistency, nil)
	tc.PrintScanResults(scanResults, "scanResults")
	FailTestIfError(err, "Error in scan", t)
	if len(scanResults) != 0 {
		e := errors.New("Lookup should not return any doc key as lookup key was array's subset")
		FailTestIfError(e, "Error in array Lookup", t)
	}
}

func TestScanLimitParameter(t *testing.T) {
	log.Printf("In TestScanLimitParameter()")
	var indexName = "index_age"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"age"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	limit := 500
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{35}, []interface{}{40}, 1, true, int64(limit), c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	if len(scanResults) != limit {
		e := errors.New(fmt.Sprintf("Expected %d number of results but got %d results", limit, len(scanResults)))
		FailTestIfError(e, "Error in scan limit: ", t)
	}

	limit = 1000
	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{35}, []interface{}{40}, 1, true, int64(limit), c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	if len(scanResults) != limit {
		e := errors.New(fmt.Sprintf("Expected %d number of results but got %d results", limit, len(scanResults)))
		FailTestIfError(e, "Error in scan limit: ", t)
	}

	// Todo: Verify the results in scanresults
}

func SkipTestScanDistinctParameter(t *testing.T) {
	log.Printf("In TestScanDistinctParameter()")
	var indexName = "index_age"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"age"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	// var limit int
	// limit = 10
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{35}, []interface{}{40}, 1, true, defaultlimit, c.SessionConsistency, nil)
	tc.PrintScanResults(scanResults, "scanResults")
	FailTestIfError(err, "Error in scan", t)
	// Todo: Verify the results in scanresults
}

func TestCountRange(t *testing.T) {
	log.Printf("In TestRangeCount()")
	var indexName = "index_age"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"age"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_float64(docs, "age", 35, 40, 1)
	rangeCount, err := secondaryindex.CountRange(indexName, bucketName, indexScanAddress, []interface{}{35}, []interface{}{40}, 1, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in CountRange: ", t)
	log.Printf("Count of expected and actual Range is:  %d and %d", len(docScanResults), rangeCount)
	if int64(len(docScanResults)) != rangeCount {
		e := errors.New(fmt.Sprintf("Expected Range count %d does not match actual Range count %d: ", len(docScanResults), rangeCount))
		FailTestIfError(e, "Error in CountRange: ", t)
	}

	docScanResults = datautility.ExpectedScanResponse_float64(docs, "age", -10, 50, 2)
	rangeCount, err = secondaryindex.CountRange(indexName, bucketName, indexScanAddress, []interface{}{-10}, []interface{}{50}, 2, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in CountRange: ", t)
	log.Printf("Count of expected and actual Range is: %d and %d", len(docScanResults), rangeCount)
	if int64(len(docScanResults)) != rangeCount {
		e := errors.New(fmt.Sprintf("Expected Range count %d does not match actual Range count %d: ", len(docScanResults), rangeCount))
		FailTestIfError(e, "Error in CountRange: ", t)
	}

	docScanResults = datautility.ExpectedScanResponse_float64(docs, "age", 45, 46, 2)
	rangeCount, err = secondaryindex.CountRange(indexName, bucketName, indexScanAddress, []interface{}{45}, []interface{}{46}, 2, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in CountRange: ", t)
	log.Printf("Count of expected and actual Range are: %d and %d", len(docScanResults), rangeCount)
	if int64(len(docScanResults)) != rangeCount {
		e := errors.New(fmt.Sprintf("Expected Range count %d does not match actual Range count %d: ", len(docScanResults), rangeCount))
		FailTestIfError(e, "Error in CountRange: ", t)
	}

	docScanResults = datautility.ExpectedScanResponse_float64(docs, "age", 40, 50, 3)
	rangeCount, err = secondaryindex.CountRange(indexName, bucketName, indexScanAddress, []interface{}{40}, []interface{}{50}, 3, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in CountRange: ", t)
	log.Printf("Count of expected and actual Range are: %d and %d", len(docScanResults), rangeCount)
	if int64(len(docScanResults)) != rangeCount {
		e := errors.New(fmt.Sprintf("Expected Range count %d does not match actual Range count %d: ", len(docScanResults), rangeCount))
		FailTestIfError(e, "Error in CountRange: ", t)
	}

	docScanResults = datautility.ExpectedScanResponse_float64(docs, "age", 55, 45, 3)
	rangeCount, err = secondaryindex.CountRange(indexName, bucketName, indexScanAddress, []interface{}{55}, []interface{}{45}, 3, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in CountRange: ", t)
	log.Printf("Count of expected and actual Range are: %d and %d", len(docScanResults), rangeCount)
	if int64(len(docScanResults)) != rangeCount {
		e := errors.New(fmt.Sprintf("Expected Range count %d does not match actual Range count %d: ", len(docScanResults), rangeCount))
		FailTestIfError(e, "Error in CountRange: ", t)
	}
}

func TestCountLookup(t *testing.T) {
	log.Printf("In TestCountLookup()")
	var indexName = "index_age"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"age"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_float64(docs, "age", 25, 25, 3)
	rangeCount, err := secondaryindex.CountLookup(indexName, bucketName, indexScanAddress, []interface{}{25}, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in CountRange: ", t)
	log.Printf("Count of expected and actual Range are: %d and %d", len(docScanResults), rangeCount)
	if int64(len(docScanResults)) != rangeCount {
		e := errors.New(fmt.Sprintf("Expected Range count %d does not match actual Range count %d: ", len(docScanResults), rangeCount))
		FailTestIfError(e, "Error in CountRange: ", t)
	}

	docScanResults = datautility.ExpectedScanResponse_float64(docs, "age", 75, 75, 3)
	rangeCount, err = secondaryindex.CountLookup(indexName, bucketName, indexScanAddress, []interface{}{75}, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in CountRange: ", t)
	log.Printf("Count of expected and actual Range are: %d and %d", len(docScanResults), rangeCount)
	if int64(len(docScanResults)) != rangeCount {
		e := errors.New(fmt.Sprintf("Expected Range count %d does not match actual Range count %d: ", len(docScanResults), rangeCount))
		FailTestIfError(e, "Error in CountRange: ", t)
	}
}

func TestRangeStatistics(t *testing.T) {
	log.Printf("In TestRangeCount()")
	var indexName = "index_age"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"age"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)
}

func TestIndexCreateWithWhere(t *testing.T) {
	log.Printf("In TestIndexCreateWithWhere()")
	var index1 = "index_ageabove30"
	var index2 = "index_ageteens"
	var index3 = "index_age35to45"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(index1, bucketName, indexManagementAddress, `age>30`, []string{"age"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults := datautility.ExpectedScanResponse_float64(docs, "age", 31, 40, 1)
	scanResults, err := secondaryindex.Range(index1, bucketName, indexScanAddress, []interface{}{31}, []interface{}{40}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
	log.Printf("Lengths of expected and actual scanReuslts are:  %d and %d", len(scanResults), len(docScanResults))

	err = secondaryindex.CreateSecondaryIndex(index2, bucketName, indexManagementAddress, `age > 12 AND age < 20`, []string{"age"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults = datautility.ExpectedScanResponse_float64(docs, "age", 12, 20, 0)
	scanResults, err = secondaryindex.Range(index2, bucketName, indexScanAddress, []interface{}{12}, []interface{}{20}, 0, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
	log.Printf("Lengths of expected and actual scanReuslts are:  %d and %d", len(scanResults), len(docScanResults))

	err = secondaryindex.CreateSecondaryIndex(index3, bucketName, indexManagementAddress, `age >= 35 AND age <= 45`, []string{"age"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	docScanResults = datautility.ExpectedScanResponse_float64(docs, "age", 35, 45, 3)
	scanResults, err = secondaryindex.ScanAll(index3, bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
	log.Printf("Lengths of expected and actual scanReuslts are:  %d and %d", len(scanResults), len(docScanResults))
}

func TestDeferredIndexCreate(t *testing.T) {
	log.Printf("In TestDeferredIndexCreate()")

	var indexName = "index_deferred"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndexAsync(indexName, bucketName, indexManagementAddress, "", []string{"company"}, false, []byte("{\"defer_build\": true}"), true, nil)
	FailTestIfError(err, "Error in creating the index", t)

	state, err := secondaryindex.IndexState(indexName, bucketName, indexManagementAddress)
	FailTestIfError(err, "Error in getting index state for index", t)
	log.Printf("Created the index %v in deferred mode. Index state is %v", indexName, state)

	err = secondaryindex.BuildIndex(indexName, bucketName, indexManagementAddress, defaultIndexActiveTimeout)
	FailTestIfError(err, "Error in deferred index build", t)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "company", "G", "M", 1)
	scanResults, err := secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{"G"}, []interface{}{"M"}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan of deferred index", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestCompositeIndex_NumAndString(t *testing.T) {
	log.Printf("In TestCompositeIndex()")

	var bucketName = "default"
	var indexName = "index_composite1"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"age", "company"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)
	scanResults, err := secondaryindex.ScanAll(indexName, bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	if len(scanResults) != len(docs) {
		log.Printf("ScanAll of composite index is wrong. Expected and actual num of results:  %d and %d", len(docs), len(scanResults))
		e := errors.New("ScanAll of composite index is wrong")
		FailTestIfError(e, "Error in TestCompositeIndex", t)

	}

	scanResults, err = secondaryindex.Range(indexName, bucketName, indexScanAddress, []interface{}{25, "F"}, []interface{}{30, "M"}, 3, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	// todo: validate the results

	addDocIfNotPresentInKV("User22a44f1c-3f15-4ada-9cf5-6c24a7690a37")

	docScanResults := make(tc.ScanResponse)
	docScanResults["User22a44f1c-3f15-4ada-9cf5-6c24a7690a37"] = []interface{}{25.0, "ZIGGLES"}
	scanResults, err = secondaryindex.Lookup(indexName, bucketName, indexScanAddress, []interface{}{25, "ZIGGLES"}, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestCompositeIndex_TwoNumberFields(t *testing.T) {
	log.Printf("In TestCompositeIndex()")

	var bucketName = "default"
	var indexName = "index_composite2"

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"latitude", "longitude"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	scanResults, err := secondaryindex.ScanAll(indexName, bucketName, indexScanAddress, defaultlimit, c.SessionConsistency, nil)
	if len(scanResults) != len(docs) {
		log.Printf("ScanAll of composite index is wrong. Expected and actual num of results:  %d and %d", len(docs), len(scanResults))
		e := errors.New("ScanAll of composite index is wrong")
		FailTestIfError(e, "Error in TestCompositeIndex", t)

	}
}
