package functionaltests

import (
	"fmt"
	"testing"
	"time"
	"github.com/couchbase/indexing/secondary/tests/framework/datautility"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	tv "github.com/couchbase/indexing/secondary/tests/framework/validation"
)

var docs []kvutility.KeyValue

func init() {
	fmt.Println("In init()")
	secondaryindex.DropAllSecondaryIndexes()
	
	// Working with Users100 dataset.
	keyValues := datautility.LoadJSONFromCompressedFile("../testdata/Users100.txt.gz", "docid")
	kvutility.SetKeyValues(keyValues, "default", "", "127.0.0.1")
	docs = keyValues
}

// Test for single index field of data type float64
func TestSimpleIndex_FloatDataType(t *testing.T) {
	fmt.Println("In TestSimpleIndex_FloatDataType()")
	var indexName = "index_age"
	var bucketName = "default"
	
	secondaryindex.CreateSecondaryIndex(indexName, bucketName, []string {"age"})
	
	// Wait, else results in "Index not ready"
	time.Sleep(1 * time.Second)
	
	docScanResults := datautility.ExpectedScanResponse_float64(docs, "age", 35, 40, 1)
	scanResults := secondaryindex.Range(indexName, bucketName, []interface{} {35}, []interface{} {40}, 1, true, 10000000)
	tv.Validate(docScanResults, scanResults)
}

// Test for single index field of data type string
func TestSimpleIndex_StringDataType(t *testing.T) {
	fmt.Println("In TestSimpleIndex_StringDataType()")
	var indexName = "index_company"
	var bucketName = "default"
	
	secondaryindex.CreateSecondaryIndex(indexName, bucketName, []string {"company"})
	time.Sleep(1 * time.Second)
	
	docScanResults := datautility.ExpectedScanResponse_string(docs, "company", "G", "M", 1)
	scanResults := secondaryindex.Range(indexName, bucketName, []interface{} {"G"}, []interface{} {"M"}, 1, true, 10000000)
	tv.Validate(docScanResults, scanResults)
	
	docScanResults = datautility.ExpectedScanResponse_string(docs, "company", "BIOSPAN", "ZILLANET", 1)
	scanResults = secondaryindex.Range(indexName, bucketName, []interface{} {"BIOSPAN"}, []interface{} {"ZILLANET"}, 1, true, 10000000)
	tv.Validate(docScanResults, scanResults)
}

// Test for single index field of data type bool
func TestSimpleIndex_BoolDataType(t *testing.T) {
	fmt.Println("In TestSimpleIndex_BoolDataType()")
	var indexName = "index_isActive"
	var bucketName = "default"
	
	secondaryindex.CreateSecondaryIndex(indexName, bucketName, []string {"isActive"})
	
	time.Sleep(1 * time.Second)
	
	docScanResults := datautility.ExpectedScanResponse_bool(docs, "isActive", true, 3)
	scanResults := secondaryindex.Range(indexName, bucketName, []interface{} { true }, []interface{} { true }, 3, true, 10000000)
	tv.Validate(docScanResults, scanResults)
}

