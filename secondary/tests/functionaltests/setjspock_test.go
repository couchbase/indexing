package functionaltests

import (
	"errors"
	"fmt"
	c "github.com/couchbase/indexing/secondary/common"
	qc "github.com/couchbase/indexing/secondary/queryport/client"
	//tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/datautility"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	tv "github.com/couchbase/indexing/secondary/tests/framework/validation"
	"log"
	"testing"
	"time"
)

var offset int64

func TestMultiScanSetup(t *testing.T) {
	log.Printf("In TestMultiScanSetup()")

	docs = nil
	mut_docs = nil
	docs = datautility.LoadJSONFromCompressedFile(dataFilePath, "docid")
	mut_docs = datautility.LoadJSONFromCompressedFile(mutationFilePath, "docid")
	log.Printf("Emptying the default bucket")
	kvutility.EnableBucketFlush("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.FlushBucket("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	time.Sleep(5 * time.Second)

	// Populate the bucket now
	log.Printf("Populating the default bucket")
	kvutility.SetKeyValues(docs, "default", "", clusterconfig.KVAddress)

	var index1 = "index_companyname"
	var index2 = "index_company"
	var index3 = "index_company_name_age"
	var index4 = "index_primary"
	var index_4field = "index_company_name_age_address"
	var index_5field = "index_company_name_age_address_friends"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(index1, bucketName, indexManagementAddress, "", []string{"company", "name"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.CreateSecondaryIndex(index2, bucketName, indexManagementAddress, "", []string{"company"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.CreateSecondaryIndex(index3, bucketName, indexManagementAddress, "", []string{"company", "name", "age"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	// Create a primary index
	err = secondaryindex.CreateSecondaryIndex(index4, bucketName, indexManagementAddress, "", nil, true, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.CreateSecondaryIndex(index_4field, bucketName, indexManagementAddress, "", []string{"company", "name", "age", "address"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.CreateSecondaryIndex(index_5field, bucketName, indexManagementAddress, "", []string{"company", "name", "age", "address", "friends"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)
}

func SkipTestMultiScanBugMB22541(t *testing.T) {
	log.Printf("In TestMultiScanBugMB22541()")

	json := make(map[string]interface{})
	json["k0"] = float64(10)
	json["k1"] = float64(10)
	docs["k001"] = json
	kvutility.SetKeyValues(docs, "default", "", clusterconfig.KVAddress)

	index := "ix1"
	fields := []string{"k0"}
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex(index, bucketName, indexManagementAddress, "", fields, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	scans := make(qc.Scans, 2)
	filter1 := make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: c.MinUnbounded, High: float64(10), Inclusion: qc.Inclusion(uint32(0))}
	scans[0] = &qc.Scan{Filter: filter1}

	filter2 := make([]*qc.CompositeElementFilter, 1)
	filter2[0] = &qc.CompositeElementFilter{Low: float64(10), High: float64(10), Inclusion: qc.Inclusion(uint32(3))}
	scans[1] = &qc.Scan{Filter: filter2}
	runMultiScanWithIndex(index, fields, scans, false, false, nil, 0, defaultlimit, false, false, "SitaramScenario", t)
}

func TestMultiScanCount(t *testing.T) {
	log.Printf("In TestMultiScanCount()")
	var index = "index_companyname"
	fields := []string{"company", "name"}

	log.Printf("\n\n--------- Composite Index with 2 fields ---------")
	runMultiScanCountWithIndex(index, fields, getScanAllNoFilter(), false, false, nil, 0, defaultlimit, true, false, "ScanAllNoFilter", t)
	runMultiScanCountWithIndex(index, fields, getScanAllFilterNil(), false, false, nil, 0, defaultlimit, true, false, "ScanAllFilterNil", t)
	runMultiScanCountWithIndex(index, fields, getScanAll_AllFiltersNil(), false, false, nil, 0, defaultlimit, true, false, "ScanAll_AllFiltersNil", t)

	runMultiScanCountWithIndex(index, fields, getSingleSeek(), false, false, nil, 0, defaultlimit, false, false, "SingleSeek", t)
	runMultiScanCountWithIndex(index, fields, getMultipleSeek(), false, false, nil, 0, defaultlimit, false, false, "MultipleSeek", t)

	runMultiScanCountWithIndex(index, fields, getSimpleRange(), false, false, nil, 0, defaultlimit, false, false, "SimpleRange", t)
	runMultiScanCountWithIndex(index, fields, getNonOverlappingRanges(), false, false, nil, 0, defaultlimit, false, false, "NonOverlappingRanges", t)
	runMultiScanCountWithIndex(index, fields, getOverlappingRanges(), false, false, nil, 0, defaultlimit, false, false, "OverlappingRanges", t)

	runMultiScanCountWithIndex(index, fields, getNonOverlappingFilters(), false, false, nil, 0, defaultlimit, false, false, "NonOverlappingFilters", t)
	runMultiScanCountWithIndex(index, fields, getOverlappingFilters(), false, false, nil, 0, defaultlimit, false, false, "OverlappingFilters", t)
	runMultiScanCountWithIndex(index, fields, getBoundaryFilters(), false, false, nil, 0, defaultlimit, false, false, "BoundaryFilters", t)

	runMultiScanCountWithIndex(index, fields, getSeekAndFilters_NonOverlapping(), false, false, nil, 0, defaultlimit, false, false, "SeekAndFilters_NonOverlapping", t)
	runMultiScanCountWithIndex(index, fields, getSeekAndFilters_Overlapping(), false, false, nil, 0, defaultlimit, false, false, "SeekAndFilters_Overlapping", t)

	// low-high unbounded , low>high
	runMultiScanCountWithIndex(index, fields, getSimpleRangeLowUnbounded(), false, false, nil, 0, defaultlimit, false, false, "SimpleRangeLowUnbounded", t)
	runMultiScanCountWithIndex(index, fields, getSimpleRangeHighUnbounded(), false, false, nil, 0, defaultlimit, false, false, "SimpleRangeHighUnbounded", t)
	runMultiScanCountWithIndex(index, fields, getSimpleRangeMultipleUnbounded(), false, false, nil, 0, defaultlimit, false, false, "SimpleRangeMultipleUnbounded", t)
	runMultiScanCountWithIndex(index, fields, getFiltersWithUnbounded(), false, false, nil, 0, defaultlimit, false, false, "FiltersWithUnbounded", t)
	runMultiScanCountWithIndex(index, fields, getFiltersLowGreaterThanHigh(), false, false, nil, 0, defaultlimit, false, false, "FiltersLowGreaterThanHigh", t)

	log.Printf("\n\n--------- Simple Index with 1 field ---------")
	var index2 = "index_company"
	fields = []string{"company"}
	runMultiScanCountWithIndex(index2, fields, getSingleIndexSimpleRange(), false, false, nil, 0, defaultlimit, false, false, "SingleIndexSimpleRange", t)
	runMultiScanCountWithIndex(index2, fields, getSingleIndex_SimpleRanges_NonOverlapping(), false, false, nil, 0, defaultlimit, false, false, "SingleIndex_SimpleRanges_NonOverlapping", t)
	runMultiScanCountWithIndex(index2, fields, getSingleIndex_SimpleRanges_Overlapping(), false, false, nil, 0, defaultlimit, false, false, "SingleIndex_SimpleRanges_Overlapping", t)

	log.Printf("\n\n--------- Composite Index with 3 fields ---------")
	var index3 = "index_company_name_age"
	fields = []string{"company", "name", "age"}
	runMultiScanCountWithIndex(index3, fields, getScanAllNoFilter(), false, false, nil, 0, defaultlimit, true, false, "ScanAllNoFilter", t)
	runMultiScanCountWithIndex(index3, fields, getScanAllFilterNil(), false, false, nil, 0, defaultlimit, true, false, "ScanAllFilterNil", t)
	runMultiScanCountWithIndex(index3, fields, getScanAll_AllFiltersNil(), false, false, nil, 0, defaultlimit, true, false, "ScanAll_AllFiltersNil", t)

	runMultiScanCountWithIndex(index3, fields, get3FieldsSingleSeek(), false, false, nil, 0, defaultlimit, false, false, "3FieldsSingleSeek", t)
	runMultiScanCountWithIndex(index3, fields, get3FieldsMultipleSeeks(), false, false, nil, 0, defaultlimit, false, false, "3FieldsMultipleSeeks", t)
	runMultiScanCountWithIndex(index3, fields, get3FieldsMultipleSeeks_Identical(), false, false, nil, 0, defaultlimit, false, false, "3FieldsMultipleSeeks_Identical", t)

	//new scenarios - Feb 2
	log.Printf("\n\n--------- New scenarios ---------")

	index2 = "index_companyname"
	fields2 := []string{"company", "name"}
	runManyMultiScanCountWithIndex(index2, fields2, getCompIndexHighUnbounded1(), false, false, nil, 0, defaultlimit, false, false, "CompIndexHighUnbounded1", t)
	runManyMultiScanCountWithIndex(index2, fields2, getCompIndexHighUnbounded2(), false, false, nil, 0, defaultlimit, false, false, "CompIndexHighUnbounded2", t)
	runManyMultiScanCountWithIndex(index2, fields2, getCompIndexHighUnbounded3(), false, false, nil, 0, defaultlimit, false, false, "CompIndexHighUnbounded3", t)
	runManyMultiScanCountWithIndex(index2, fields2, getCompIndexHighUnbounded4(), false, false, nil, 0, defaultlimit, false, false, "CompIndexHighUnbounded4", t)
	runManyMultiScanCountWithIndex(index2, fields2, getCompIndexHighUnbounded5(), false, false, nil, 0, defaultlimit, false, false, "CompIndexHighUnbounded5", t)
	runManyMultiScanCountWithIndex(index2, fields2, getSeekBoundaries(), false, false, nil, 0, defaultlimit, false, false, "SeekBoundaries", t)

	log.Printf("\n\n--------- With DISTINCT True ---------")

	index = "index_companyname"
	fields = []string{"company", "name"}
	runMultiScanCountWithIndex(index, fields, getScanAllNoFilter(), false, true, nil, 0, defaultlimit, true, false, "ScanAllNoFilter", t)
	runMultiScanCountWithIndex(index, fields, getScanAllFilterNil(), false, true, nil, 0, defaultlimit, true, false, "ScanAllFilterNil", t)
	runMultiScanCountWithIndex(index, fields, getScanAll_AllFiltersNil(), false, true, nil, 0, defaultlimit, true, false, "ScanAll_AllFiltersNil", t)

	runMultiScanCountWithIndex(index, fields, getSingleSeek(), false, true, nil, 0, defaultlimit, false, false, "SingleSeek", t)
	runMultiScanCountWithIndex(index, fields, getMultipleSeek(), false, true, nil, 0, defaultlimit, false, false, "MultipleSeek", t)

	runMultiScanCountWithIndex(index, fields, getSimpleRange(), false, true, nil, 0, defaultlimit, false, false, "SimpleRange", t)
	runMultiScanCountWithIndex(index, fields, getNonOverlappingRanges(), false, true, nil, 0, defaultlimit, false, false, "NonOverlappingRanges", t)
	runMultiScanCountWithIndex(index, fields, getOverlappingRanges(), false, true, nil, 0, defaultlimit, false, false, "OverlappingRanges", t)

	runMultiScanCountWithIndex(index, fields, getNonOverlappingFilters(), false, true, nil, 0, defaultlimit, false, false, "NonOverlappingFilters", t)
	runMultiScanCountWithIndex(index, fields, getOverlappingFilters(), false, true, nil, 0, defaultlimit, false, false, "OverlappingFilters", t)
	runMultiScanCountWithIndex(index, fields, getBoundaryFilters(), false, true, nil, 0, defaultlimit, false, false, "BoundaryFilters", t)

	runMultiScanCountWithIndex(index, fields, getSeekAndFilters_NonOverlapping(), false, true, nil, 0, defaultlimit, false, false, "SeekAndFilters_NonOverlapping", t)
	runMultiScanCountWithIndex(index, fields, getSeekAndFilters_Overlapping(), false, true, nil, 0, defaultlimit, false, false, "SeekAndFilters_Overlapping", t)

	// low-high unbounded , low>high
	runMultiScanCountWithIndex(index, fields, getSimpleRangeLowUnbounded(), false, false, nil, 0, defaultlimit, false, false, "SimpleRangeLowUnbounded", t)
	runMultiScanCountWithIndex(index, fields, getSimpleRangeHighUnbounded(), false, false, nil, 0, defaultlimit, false, false, "SimpleRangeHighUnbounded", t)
	runMultiScanCountWithIndex(index, fields, getSimpleRangeMultipleUnbounded(), false, false, nil, 0, defaultlimit, false, false, "SimpleRangeMultipleUnbounded", t)
	runMultiScanCountWithIndex(index, fields, getFiltersWithUnbounded(), false, false, nil, 0, defaultlimit, false, false, "FiltersWithUnbounded", t)
	runMultiScanCountWithIndex(index, fields, getFiltersLowGreaterThanHigh(), false, false, nil, 0, defaultlimit, false, false, "FiltersLowGreaterThanHigh", t)

	log.Printf("\n\n--------- Simple Index with 1 field ---------")
	index2 = "index_company"
	fields = []string{"company"}
	runMultiScanCountWithIndex(index2, fields, getSingleIndexSimpleRange(), false, true, nil, 0, defaultlimit, false, false, "SingleIndexSimpleRange", t)
	runMultiScanCountWithIndex(index2, fields, getSingleIndex_SimpleRanges_NonOverlapping(), false, true, nil, 0, defaultlimit, false, false, "SingleIndex_SimpleRanges_NonOverlapping", t)
	runMultiScanCountWithIndex(index2, fields, getSingleIndex_SimpleRanges_Overlapping(), false, true, nil, 0, defaultlimit, false, false, "SingleIndex_SimpleRanges_Overlapping", t)

	log.Printf("\n\n--------- Composite Index with 3 fields ---------")
	index3 = "index_company_name_age"
	fields = []string{"company", "name", "age"}
	runMultiScanCountWithIndex(index3, fields, getScanAllNoFilter(), false, true, nil, 0, defaultlimit, true, false, "ScanAllNoFilter", t)
	runMultiScanCountWithIndex(index3, fields, getScanAllFilterNil(), false, true, nil, 0, defaultlimit, true, false, "ScanAllFilterNil", t)
	runMultiScanCountWithIndex(index3, fields, getScanAll_AllFiltersNil(), false, true, nil, 0, defaultlimit, true, false, "ScanAll_AllFiltersNil", t)

	runMultiScanCountWithIndex(index3, fields, get3FieldsSingleSeek(), false, true, nil, 0, defaultlimit, false, false, "3FieldsSingleSeek", t)
	runMultiScanCountWithIndex(index3, fields, get3FieldsMultipleSeeks(), false, true, nil, 0, defaultlimit, false, false, "3FieldsMultipleSeeks", t)
	runMultiScanCountWithIndex(index3, fields, get3FieldsMultipleSeeks_Identical(), false, true, nil, 0, defaultlimit, false, false, "3FieldsMultipleSeeks_Identical", t)
}

func TestMultiScanScenarios(t *testing.T) {
	log.Printf("In TestMultiScanScenarios()")

	log.Printf("\n\n--------- Composite Index with 2 fields ---------")
	runMultiScan(getScanAllNoFilter(), false, false, nil, 0, defaultlimit, true, false, "ScanAllNoFilter", t)
	runMultiScan(getScanAllFilterNil(), false, false, nil, 0, defaultlimit, true, false, "ScanAllFilterNil", t)
	runMultiScan(getScanAll_AllFiltersNil(), false, false, nil, 0, defaultlimit, true, false, "ScanAll_AllFiltersNil", t)

	runMultiScan(getSingleSeek(), false, false, nil, 0, defaultlimit, false, false, "SingleSeek", t)
	runMultiScan(getMultipleSeek(), false, false, nil, 0, defaultlimit, false, false, "MultipleSeek", t)

	runMultiScan(getSimpleRange(), false, false, nil, 0, defaultlimit, false, false, "SimpleRange", t)
	runMultiScan(getNonOverlappingRanges(), false, false, nil, 0, defaultlimit, false, false, "NonOverlappingRanges", t)
	runMultiScan(getOverlappingRanges(), false, false, nil, 0, defaultlimit, false, false, "OverlappingRanges", t)

	runMultiScan(getNonOverlappingFilters(), false, false, nil, 0, defaultlimit, false, false, "NonOverlappingFilters", t)
	runMultiScan(getOverlappingFilters(), false, false, nil, 0, defaultlimit, false, false, "OverlappingFilters", t)
	runMultiScan(getBoundaryFilters(), false, false, nil, 0, defaultlimit, false, false, "BoundaryFilters", t)

	runMultiScan(getSeekAndFilters_NonOverlapping(), false, false, nil, 0, defaultlimit, false, false, "SeekAndFilters_NonOverlapping", t)
	runMultiScan(getSeekAndFilters_Overlapping(), false, false, nil, 0, defaultlimit, false, false, "SeekAndFilters_Overlapping", t)

	// low-high unbounded , low>high
	runMultiScan(getSimpleRangeLowUnbounded(), false, false, nil, 0, defaultlimit, false, false, "SimpleRangeLowUnbounded", t)
	runMultiScan(getSimpleRangeHighUnbounded(), false, false, nil, 0, defaultlimit, false, false, "SimpleRangeHighUnbounded", t)
	runMultiScan(getSimpleRangeMultipleUnbounded(), false, false, nil, 0, defaultlimit, false, false, "SimpleRangeMultipleUnbounded", t)
	runMultiScan(getFiltersWithUnbounded(), false, false, nil, 0, defaultlimit, false, false, "FiltersWithUnbounded", t)
	runMultiScan(getFiltersLowGreaterThanHigh(), false, false, nil, 0, defaultlimit, false, false, "FiltersLowGreaterThanHigh", t)

	log.Printf("\n\n--------- Simple Index with 1 field ---------")
	var index2 = "index_company"
	fields := []string{"company"}
	runMultiScanWithIndex(index2, fields, getSingleIndexSimpleRange(), false, false, nil, 0, defaultlimit, false, false, "SingleIndexSimpleRange", t)
	runMultiScanWithIndex(index2, fields, getSingleIndex_SimpleRanges_NonOverlapping(), false, false, nil, 0, defaultlimit, false, false, "SingleIndex_SimpleRanges_NonOverlapping", t)
	runMultiScanWithIndex(index2, fields, getSingleIndex_SimpleRanges_Overlapping(), false, false, nil, 0, defaultlimit, false, false, "SingleIndex_SimpleRanges_Overlapping", t)

	log.Printf("\n\n--------- Composite Index with 3 fields ---------")
	var index3 = "index_company_name_age"
	fields = []string{"company", "name", "age"}
	runMultiScanWithIndex(index3, fields, getScanAllNoFilter(), false, false, nil, 0, defaultlimit, true, false, "ScanAllNoFilter", t)
	runMultiScanWithIndex(index3, fields, getScanAllFilterNil(), false, false, nil, 0, defaultlimit, true, false, "ScanAllFilterNil", t)
	runMultiScanWithIndex(index3, fields, getScanAll_AllFiltersNil(), false, false, nil, 0, defaultlimit, true, false, "ScanAll_AllFiltersNil", t)

	runMultiScanWithIndex(index3, fields, get3FieldsSingleSeek(), false, false, nil, 0, defaultlimit, false, false, "3FieldsSingleSeek", t)
	runMultiScanWithIndex(index3, fields, get3FieldsMultipleSeeks(), false, false, nil, 0, defaultlimit, false, false, "3FieldsMultipleSeeks", t)
	runMultiScanWithIndex(index3, fields, get3FieldsMultipleSeeks_Identical(), false, false, nil, 0, defaultlimit, false, false, "3FieldsMultipleSeeks_Identical", t)

	//new scenarios - Feb 2
	log.Printf("\n\n--------- New scenarios ---------")

	index2 = "index_companyname"
	fields2 := []string{"company", "name"}
	runManyMultiScanWithIndex(index2, fields2, getCompIndexHighUnbounded1(), false, false, nil, 0, defaultlimit, false, false, "CompIndexHighUnbounded1", t)
	runManyMultiScanWithIndex(index2, fields2, getCompIndexHighUnbounded2(), false, false, nil, 0, defaultlimit, false, false, "CompIndexHighUnbounded2", t)
	runManyMultiScanWithIndex(index2, fields2, getCompIndexHighUnbounded3(), false, false, nil, 0, defaultlimit, false, false, "CompIndexHighUnbounded3", t)
	runManyMultiScanWithIndex(index2, fields2, getCompIndexHighUnbounded4(), false, false, nil, 0, defaultlimit, false, false, "CompIndexHighUnbounded4", t)
	runManyMultiScanWithIndex(index2, fields2, getCompIndexHighUnbounded5(), false, false, nil, 0, defaultlimit, false, false, "CompIndexHighUnbounded5", t)
	runManyMultiScanWithIndex(index2, fields2, getSeekBoundaries(), false, false, nil, 0, defaultlimit, false, false, "SeekBoundaries", t)
}

func TestMultiScanOffset(t *testing.T) {
	log.Printf("In TestMultiScanOffset()")

	log.Printf("\n\n--------- Composite Index with 2 fields ---------")

	runMultiScan(getScanAllNoFilter(), false, false, nil, 8453, defaultlimit, true, true, "ScanAllNoFilter", t)
	runMultiScan(getScanAllFilterNil(), false, false, nil, 1, defaultlimit, true, true, "ScanAllFilterNil", t)
	runMultiScan(getScanAll_AllFiltersNil(), false, false, nil, 10002, defaultlimit, true, true, "ScanAll_AllFiltersNil", t)

	runMultiScan(getSingleSeek(), false, false, nil, 1, defaultlimit, false, true, "SingleSeek", t)
	runMultiScan(getMultipleSeek(), false, false, nil, 1, defaultlimit, false, true, "MultipleSeek", t)

	runMultiScan(getSimpleRange(), false, false, nil, 2273, defaultlimit, false, true, "SimpleRange", t)
	runMultiScan(getNonOverlappingRanges(), false, false, nil, 1111, defaultlimit, false, true, "NonOverlappingRanges", t)
	runMultiScan(getOverlappingRanges(), false, false, nil, 100, defaultlimit, false, true, "OverlappingRanges", t)

	runMultiScan(getNonOverlappingFilters(), false, false, nil, 340, defaultlimit, false, true, "NonOverlappingFilters", t)
	runMultiScan(getOverlappingFilters(), false, false, nil, 1213, defaultlimit, false, true, "OverlappingFilters", t)
	runMultiScan(getBoundaryFilters(), false, false, nil, 399, defaultlimit, false, true, "BoundaryFilters", t)

	runMultiScan(getSeekAndFilters_NonOverlapping(), false, false, nil, 121, defaultlimit, false, true, "SeekAndFilters_NonOverlapping", t)
	runMultiScan(getSeekAndFilters_Overlapping(), false, false, nil, 254, defaultlimit, false, true, "SeekAndFilters_Overlapping", t)

	// low-high unbounded , low>high
	runMultiScan(getSimpleRangeLowUnbounded(), false, false, nil, 0, defaultlimit, false, false, "SimpleRangeLowUnbounded", t)
	runMultiScan(getSimpleRangeHighUnbounded(), false, false, nil, 0, defaultlimit, false, false, "SimpleRangeHighUnbounded", t)
	runMultiScan(getSimpleRangeMultipleUnbounded(), false, false, nil, 0, defaultlimit, false, false, "SimpleRangeMultipleUnbounded", t)
	runMultiScan(getFiltersWithUnbounded(), false, false, nil, 0, defaultlimit, false, false, "FiltersWithUnbounded", t)
	runMultiScan(getFiltersLowGreaterThanHigh(), false, false, nil, 0, defaultlimit, false, false, "FiltersLowGreaterThanHigh", t)

	log.Printf("\n\n--------- Simple Index with 1 field ---------")
	var index2 = "index_company"
	fields := []string{"company"}
	runMultiScanWithIndex(index2, fields, getSingleIndexSimpleRange(), false, false, nil, 1273, defaultlimit, false, true, "SingleIndexSimpleRange", t)
	runMultiScanWithIndex(index2, fields, getSingleIndex_SimpleRanges_NonOverlapping(), false, false, nil, 140, defaultlimit, false, true, "SingleIndex_SimpleRanges_NonOverlapping", t)
	runMultiScanWithIndex(index2, fields, getSingleIndex_SimpleRanges_Overlapping(), false, false, nil, 6000, defaultlimit, false, true, "SingleIndex_SimpleRanges_Overlapping", t)

	log.Printf("\n\n--------- Composite Index with 3 fields ---------")
	var index3 = "index_company_name_age"
	fields = []string{"company", "name", "age"}
	runMultiScanWithIndex(index3, fields, getScanAllNoFilter(), false, false, nil, 100000, defaultlimit, true, true, "ScanAllNoFilter", t)
	runMultiScanWithIndex(index3, fields, getScanAllFilterNil(), false, false, nil, 0, defaultlimit, true, true, "ScanAllFilterNil", t)
	runMultiScanWithIndex(index3, fields, getScanAll_AllFiltersNil(), false, false, nil, 1002, defaultlimit, true, true, "ScanAll_AllFiltersNil", t)

	runMultiScanWithIndex(index3, fields, get3FieldsSingleSeek(), false, false, nil, 0, defaultlimit, false, true, "3FieldsSingleSeek", t)
	runMultiScanWithIndex(index3, fields, get3FieldsMultipleSeeks(), false, false, nil, 1, defaultlimit, false, true, "3FieldsMultipleSeeks", t)
	runMultiScanWithIndex(index3, fields, get3FieldsMultipleSeeks_Identical(), false, false, nil, 1, defaultlimit, false, true, "3FieldsMultipleSeeks_Identical", t)
}

func TestMultiScanPrimaryIndex(t *testing.T) {
	log.Printf("In TestMultiScanPrimaryIndex()")

	var index4 = "index_primary"
	runMultiScanForPrimaryIndex(index4, getPrimaryRange(), false, false, nil, 0, defaultlimit, true, false, "PrimaryRange", t)
	runMultiScanForPrimaryIndex(index4, getScanAllNoFilter(), false, false, nil, 0, defaultlimit, true, false, "PrimaryScanAllNoFilter", t)
}

func TestMultiScanDistinct(t *testing.T) {
	log.Printf("In TestScansDistinct()")

	log.Printf("\n\n--------- Composite Index with 2 fields ---------")

	runMultiScan(getScanAllNoFilter(), false, true, nil, 0, defaultlimit, true, false, "ScanAllNoFilter", t)
	runMultiScan(getScanAllFilterNil(), false, true, nil, 0, defaultlimit, true, false, "ScanAllFilterNil", t)
	runMultiScan(getScanAll_AllFiltersNil(), false, true, nil, 0, defaultlimit, true, false, "ScanAll_AllFiltersNil", t)

	runMultiScan(getSingleSeek(), false, true, nil, 0, defaultlimit, false, false, "SingleSeek", t)
	runMultiScan(getMultipleSeek(), false, true, nil, 0, defaultlimit, false, false, "MultipleSeek", t)

	runMultiScan(getSimpleRange(), false, true, nil, 0, defaultlimit, false, false, "SimpleRange", t)
	runMultiScan(getNonOverlappingRanges(), false, true, nil, 0, defaultlimit, false, false, "NonOverlappingRanges", t)
	runMultiScan(getOverlappingRanges(), false, true, nil, 0, defaultlimit, false, false, "OverlappingRanges", t)

	runMultiScan(getNonOverlappingFilters(), false, true, nil, 0, defaultlimit, false, false, "NonOverlappingFilters", t)
	runMultiScan(getOverlappingFilters(), false, true, nil, 0, defaultlimit, false, false, "OverlappingFilters", t)
	runMultiScan(getBoundaryFilters(), false, true, nil, 0, defaultlimit, false, false, "BoundaryFilters", t)

	runMultiScan(getSeekAndFilters_NonOverlapping(), false, true, nil, 0, defaultlimit, false, false, "SeekAndFilters_NonOverlapping", t)
	runMultiScan(getSeekAndFilters_Overlapping(), false, true, nil, 0, defaultlimit, false, false, "SeekAndFilters_Overlapping", t)

	// low-high unbounded , low>high
	runMultiScan(getSimpleRangeLowUnbounded(), false, false, nil, 0, defaultlimit, false, false, "SimpleRangeLowUnbounded", t)
	runMultiScan(getSimpleRangeHighUnbounded(), false, false, nil, 0, defaultlimit, false, false, "SimpleRangeHighUnbounded", t)
	runMultiScan(getSimpleRangeMultipleUnbounded(), false, false, nil, 0, defaultlimit, false, false, "SimpleRangeMultipleUnbounded", t)
	runMultiScan(getFiltersWithUnbounded(), false, false, nil, 0, defaultlimit, false, false, "FiltersWithUnbounded", t)
	runMultiScan(getFiltersLowGreaterThanHigh(), false, false, nil, 0, defaultlimit, false, false, "FiltersLowGreaterThanHigh", t)

	log.Printf("\n\n--------- Simple Index with 1 field ---------")
	var index2 = "index_company"
	fields := []string{"company"}
	runMultiScanWithIndex(index2, fields, getSingleIndexSimpleRange(), false, true, nil, 0, defaultlimit, false, false, "SingleIndexSimpleRange", t)
	runMultiScanWithIndex(index2, fields, getSingleIndex_SimpleRanges_NonOverlapping(), false, true, nil, 0, defaultlimit, false, false, "SingleIndex_SimpleRanges_NonOverlapping", t)
	runMultiScanWithIndex(index2, fields, getSingleIndex_SimpleRanges_Overlapping(), false, true, nil, 0, defaultlimit, false, false, "SingleIndex_SimpleRanges_Overlapping", t)

	log.Printf("\n\n--------- Composite Index with 3 fields ---------")
	var index3 = "index_company_name_age"
	fields = []string{"company", "name", "age"}
	runMultiScanWithIndex(index3, fields, getScanAllNoFilter(), false, true, nil, 0, defaultlimit, true, false, "ScanAllNoFilter", t)
	runMultiScanWithIndex(index3, fields, getScanAllFilterNil(), false, true, nil, 0, defaultlimit, true, false, "ScanAllFilterNil", t)
	runMultiScanWithIndex(index3, fields, getScanAll_AllFiltersNil(), false, true, nil, 0, defaultlimit, true, false, "ScanAll_AllFiltersNil", t)

	runMultiScanWithIndex(index3, fields, get3FieldsSingleSeek(), false, true, nil, 0, defaultlimit, false, false, "3FieldsSingleSeek", t)
	runMultiScanWithIndex(index3, fields, get3FieldsMultipleSeeks(), false, true, nil, 0, defaultlimit, false, false, "3FieldsMultipleSeeks", t)
	runMultiScanWithIndex(index3, fields, get3FieldsMultipleSeeks_Identical(), false, true, nil, 0, defaultlimit, false, false, "3FieldsMultipleSeeks_Identical", t)
}

func TestMultiScanProjection(t *testing.T) {
	log.Printf("In TestMultiScanProjection()")

	// Old scenarios
	secondaryindex.CheckCollation = false
	log.Printf("\n\n--------- Composite Index with 2 fields ---------")

	runMultiScan(getScanAllNoFilter(), false, true, projpktrue([]int64{0, 1}), 0, defaultlimit, true, false, "ScanAllNoFilter", t)
	runMultiScan(getScanAllFilterNil(), false, true, projpktrue([]int64{0}), 0, defaultlimit, true, false, "ScanAllFilterNil", t)
	runMultiScan(getScanAll_AllFiltersNil(), false, true, projpktrue([]int64{1}), 0, defaultlimit, true, false, "ScanAll_AllFiltersNil", t)

	runMultiScan(getSingleSeek(), false, true, projpktrue([]int64{1}), 0, defaultlimit, false, false, "SingleSeek", t)
	runMultiScan(getMultipleSeek(), false, true, projpktrue([]int64{0, 1}), 0, defaultlimit, false, false, "MultipleSeek", t)

	runMultiScan(getSimpleRange(), false, true, projpktrue([]int64{1}), 0, defaultlimit, false, false, "SimpleRange", t)
	runMultiScan(getNonOverlappingRanges(), false, true, projpktrue([]int64{0}), 0, defaultlimit, false, false, "NonOverlappingRanges", t)
	runMultiScan(getOverlappingRanges(), false, true, projpktrue([]int64{0, 1}), 0, defaultlimit, false, false, "OverlappingRanges", t)

	runMultiScan(getNonOverlappingFilters(), false, true, projpktrue([]int64{1}), 0, defaultlimit, false, false, "NonOverlappingFilters", t)
	runMultiScan(getOverlappingFilters(), false, true, projpktrue([]int64{0}), 0, defaultlimit, false, false, "OverlappingFilters", t)
	runMultiScan(getBoundaryFilters(), false, true, projpktrue([]int64{0, 1}), 0, defaultlimit, false, false, "BoundaryFilters", t)

	runMultiScan(getSeekAndFilters_NonOverlapping(), false, true, projpktrue([]int64{0, 1}), 0, defaultlimit, false, false, "SeekAndFilters_NonOverlapping", t)
	runMultiScan(getSeekAndFilters_Overlapping(), false, true, projpktrue([]int64{1}), 0, defaultlimit, false, false, "SeekAndFilters_Overlapping", t)

	// low-high unbounded , low>high
	runMultiScan(getSimpleRangeLowUnbounded(), false, false, projpktrue([]int64{0}), 0, defaultlimit, false, false, "SimpleRangeLowUnbounded", t)
	runMultiScan(getSimpleRangeHighUnbounded(), false, false, projpktrue([]int64{1}), 0, defaultlimit, false, false, "SimpleRangeHighUnbounded", t)
	runMultiScan(getSimpleRangeMultipleUnbounded(), false, false, projpktrue([]int64{0, 1}), 0, defaultlimit, false, false, "SimpleRangeMultipleUnbounded", t)
	runMultiScan(getFiltersWithUnbounded(), false, false, projpktrue([]int64{0, 1}), 0, defaultlimit, false, false, "FiltersWithUnbounded", t)
	runMultiScan(getFiltersLowGreaterThanHigh(), false, false, nil, 0, defaultlimit, false, false, "FiltersLowGreaterThanHigh", t)

	log.Printf("\n\n--------- Simple Index with 1 field ---------")
	var index2 = "index_company"
	fields := []string{"company"}
	runMultiScanWithIndex(index2, fields, getSingleIndexSimpleRange(), false, true, projpktrue([]int64{0}), 0, defaultlimit, false, false, "SingleIndexSimpleRange", t)
	runMultiScanWithIndex(index2, fields, getSingleIndex_SimpleRanges_NonOverlapping(), false, true, projpktrue([]int64{0}), 0, defaultlimit, false, false, "SingleIndex_SimpleRanges_NonOverlapping", t)
	runMultiScanWithIndex(index2, fields, getSingleIndex_SimpleRanges_Overlapping(), false, true, projpktrue([]int64{0}), 0, defaultlimit, false, false, "SingleIndex_SimpleRanges_Overlapping", t)

	log.Printf("\n\n--------- Composite Index with 3 fields ---------")
	var index3 = "index_company_name_age"
	fields = []string{"company", "name", "age"}
	runMultiScanWithIndex(index3, fields, getScanAllNoFilter(), false, true, projpktrue([]int64{0, 1, 2}), 0, defaultlimit, true, false, "ScanAllNoFilter", t)
	runMultiScanWithIndex(index3, fields, getScanAllFilterNil(), false, true, projpktrue([]int64{0, 1}), 0, defaultlimit, true, false, "ScanAllFilterNil", t)
	runMultiScanWithIndex(index3, fields, getScanAll_AllFiltersNil(), false, true, projpktrue([]int64{1, 2}), 0, defaultlimit, true, false, "ScanAll_AllFiltersNil", t)

	runMultiScanWithIndex(index3, fields, get3FieldsSingleSeek(), false, true, projpktrue([]int64{0, 2}), 0, defaultlimit, false, false, "3FieldsSingleSeek", t)
	runMultiScanWithIndex(index3, fields, get3FieldsMultipleSeeks(), false, true, projpktrue([]int64{1}), 0, defaultlimit, false, false, "3FieldsMultipleSeeks", t)
	runMultiScanWithIndex(index3, fields, get3FieldsMultipleSeeks_Identical(), false, true, projpktrue([]int64{2}), 0, defaultlimit, false, false, "3FieldsMultipleSeeks_Identical", t)

	// New projection specific scenarios
	//var index_primary = "index_primary"
	var index_1field = "index_company"
	var index_2field = "index_companyname"
	var index_3field = "index_company_name_age"
	var index_4field = "index_company_name_age_address"
	var index_5field = "index_company_name_age_address_friends"
	fields_1 := []string{"company"}
	fields_2 := []string{"company", "name"}
	fields_3 := []string{"company", "name", "age"}
	fields_4 := []string{"company", "name", "age", "address"}
	fields_5 := []string{"company", "name", "age", "address", "friends"}

	log.Printf("indexes are: ", index_1field, index_2field, index_3field, index_4field, index_5field)
	log.Printf("fields are: ", fields_1, fields_2, fields_3, fields_4, fields_5)

	// Project first, second, third, etc. One at a time.
	runMultiScanWithIndex(index_1field, fields_1, getSingleIndexSimpleRange(), false, true,
		projpktrue([]int64{0}), 0, defaultlimit, false, false, "SingleIndexProjectFirst", t)

	runMultiScanWithIndex(index_2field, fields_2, getSingleIndexSimpleRange(), false, true,
		projpktrue([]int64{1}), 0, defaultlimit, false, false, "2FieldIndexProjectSecond", t)

	runMultiScanWithIndex(index_3field, fields_3, getSingleIndexSimpleRange(), false, true,
		projpktrue([]int64{2}), 0, defaultlimit, false, false, "3FieldIndexProjectThird", t)

	runMultiScanWithIndex(index_4field, fields_4, getSingleIndexSimpleRange(), false, true,
		projpktrue([]int64{3}), 0, defaultlimit, false, false, "4FieldIndexProjectFourth", t)

	runMultiScanWithIndex(index_5field, fields_5, getSingleIndexSimpleRange(), false, true,
		projpktrue([]int64{4}), 0, defaultlimit, false, false, "5FieldIndexProjectFifth", t)

	// Project two a time - all combinations
	runMultiScanWithIndex(index_2field, fields_2, getSingleIndexSimpleRange(), false, true,
		projpktrue([]int64{0, 1}), 0, defaultlimit, false, false, "2FieldIndexProjectTwo", t)

	runMultiScanWithIndex(index_3field, fields_3, getSingleIndexSimpleRange(), false, true,
		projpktrue([]int64{0, 1}), 0, defaultlimit, false, false, "3FieldIndexProjectTwo", t)

	runMultiScanWithIndex(index_3field, fields_3, getSingleIndexSimpleRange(), false, true,
		projpktrue([]int64{0, 2}), 0, defaultlimit, false, false, "3FieldIndexProjectTwo", t)

	runMultiScanWithIndex(index_3field, fields_3, getSingleIndexSimpleRange(), false, true,
		projpktrue([]int64{1, 2}), 0, defaultlimit, false, false, "3FieldIndexProjectTwo", t)

	runMultiScanWithIndex(index_4field, fields_4, getSingleIndexSimpleRange(), false, true,
		projpktrue([]int64{0, 2}), 0, defaultlimit, false, false, "4FieldIndexProjectTwo", t)

	runMultiScanWithIndex(index_4field, fields_4, getSingleIndexSimpleRange(), false, true,
		projpktrue([]int64{0, 3}), 0, defaultlimit, false, false, "4FieldIndexProjectTwo", t)

	runMultiScanWithIndex(index_4field, fields_4, getSingleIndexSimpleRange(), false, true,
		projpktrue([]int64{1, 2}), 0, defaultlimit, false, false, "4FieldIndexProjectTwo", t)

	runMultiScanWithIndex(index_4field, fields_4, getSingleIndexSimpleRange(), false, true,
		projpktrue([]int64{1, 3}), 0, defaultlimit, false, false, "4FieldIndexProjectTwo", t)

	runMultiScanWithIndex(index_4field, fields_4, getSingleIndexSimpleRange(), false, true,
		projpktrue([]int64{2, 3}), 0, defaultlimit, false, false, "4FieldIndexProjectTwo", t)

	runMultiScanWithIndex(index_5field, fields_5, getSingleIndexSimpleRange(), false, true,
		projpktrue([]int64{0, 4}), 0, defaultlimit, false, false, "5FieldIndexProjectTwo", t)

	runMultiScanWithIndex(index_5field, fields_5, getSingleIndexSimpleRange(), false, true,
		projpktrue([]int64{1, 3}), 0, defaultlimit, false, false, "5FieldIndexProjectTwo", t)

	runMultiScanWithIndex(index_5field, fields_5, getSingleIndexSimpleRange(), false, true,
		projpktrue([]int64{2, 4}), 0, defaultlimit, false, false, "5FieldIndexProjectTwo", t)

	runMultiScanWithIndex(index_5field, fields_5, getSingleIndexSimpleRange(), false, true,
		projpktrue([]int64{3, 4}), 0, defaultlimit, false, false, "5FieldIndexProjectTwo", t)

	runMultiScanWithIndex(index_5field, fields_5, getSingleIndexSimpleRange(), false, true,
		projpktrue([]int64{1, 2, 4}), 0, defaultlimit, false, false, "5FieldIndexProjectThree", t)

	runMultiScanWithIndex(index_5field, fields_5, getSingleIndexSimpleRange(), false, true,
		projpktrue([]int64{0, 2, 3, 4}), 0, defaultlimit, false, false, "5FieldIndexProjectFour", t)

	runMultiScanWithIndex(index_5field, fields_5, getSingleIndexSimpleRange(), false, true,
		projpktrue([]int64{0, 1, 2, 3, 4}), 0, defaultlimit, false, false, "5FieldIndexProjectAll", t)

	runMultiScanWithIndex(index_5field, fields_5, getSingleIndexSimpleRange(), false, true,
		projpktrue([]int64{0, 2, 4}), 0, defaultlimit, false, false, "5FieldIndexProjectAlternate", t)
	runMultiScanWithIndex(index_5field, fields_5, getSingleIndexSimpleRange(), false, true,
		projpktrue([]int64{}), 0, defaultlimit, false, false, "5FieldIndexProjectEmptyEntryKeys", t)
}

func TestMultiScanRestAPI(t *testing.T) {
	log.Printf("In TestMultiScanRestAPI()")

	var indexName = "index_companyname"
	var bucketName = "default"

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	err := secondaryindex.CreateSecondaryIndex(indexName, bucketName, indexManagementAddress, "", []string{"company", "name"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	// get indexes
	indexes, err := restful_getall()
	FailTestIfError(err, "Error in restful_getall()", t)
	ids := make([]string, 0)
	for id := range indexes {
		ids = append(ids, id)
	}

	scans := `[{"Seek":null,"Filter":[{"Low":"D","High":"F","Inclusion":3},{"Low":"A","High":"C","Inclusion":3}]},{"Seek":null,"Filter":[{"Low":"S","High":"V","Inclusion":3},{"Low":"A","High":"C","Inclusion":3}]}]`
	projection := `{"EntryKeys":[1],"PrimaryKey":false}`
	reqbody := restful_clonebody(reqscans)
	reqbody["scans"] = scans
	reqbody["projection"] = projection
	reqbody["distinct"] = false
	reqbody["limit"] = 100000000
	reqbody["stale"] = "ok"
	reqbody["reverse"] = false
	reqbody["offset"] = int64(0)
	entries, err := getscans(ids[0], reqbody)
	FailTestIfError(err, "Error in getscans()", t)
	log.Printf("number of entries %v\n", len(entries))
}

func runMultiScan(scans qc.Scans, reverse, distinct bool,
	projection *qc.IndexProjection, offset, limit int64,
	isScanAll bool, validateOnlyCount bool, scenario string, t *testing.T) {
	var indexName = "index_companyname"
	var bucketName = "default"
	log.Printf("\n--- %v ---", scenario)

	docScanResults := datautility.ExpectedMultiScanResponse(docs, []string{"company", "name"}, scans, reverse, distinct, projection, offset, limit, isScanAll)
	scanResults, err := secondaryindex.Scans(indexName, bucketName, indexScanAddress, scans, reverse, distinct, projection, offset, limit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)

	if validateOnlyCount {
		if len(scanResults) != len(docScanResults) {
			msg := fmt.Sprintf("Length of expected results %v is not equal to length of scan results", len(docScanResults), len(scanResults))
			FailTestIfError(errors.New(msg), "Error in scan result validation", t)
		}
	} else {
		err = tv.Validate(docScanResults, scanResults)
		FailTestIfError(err, "Error in scan result validation", t)
	}
}

func runMultiScanWithIndex(indexName string, fields []string, scans qc.Scans,
	reverse, distinct bool, projection *qc.IndexProjection, offset, limit int64,
	isScanAll bool, validateOnlyCount bool, scenario string, t *testing.T) {
	var bucketName = "default"
	log.Printf("\n--- %v ---", scenario)

	docScanResults := datautility.ExpectedMultiScanResponse(docs, fields, scans, reverse, distinct, projection, offset, limit, isScanAll)
	scanResults, err := secondaryindex.Scans(indexName, bucketName, indexScanAddress, scans, reverse, distinct, projection, offset, limit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)

	if validateOnlyCount {
		if len(scanResults) != len(docScanResults) {
			msg := fmt.Sprintf("Length of expected results %v is not equal to length of scan results", len(docScanResults), len(scanResults))
			FailTestIfError(errors.New(msg), "Error in scan result validation", t)
		}
	} else {
		err = tv.Validate(docScanResults, scanResults)
		FailTestIfError(err, "Error in scan result validation", t)
	}
}

func runMultiScanCountWithIndex(indexName string, fields []string, scans qc.Scans,
	reverse, distinct bool, projection *qc.IndexProjection, offset, limit int64,
	isScanAll bool, validateOnlyCount bool, scenario string, t *testing.T) {
	var bucketName = "default"
	log.Printf("\n--- %v ---", scenario)

	docScanResults := datautility.ExpectedMultiScanResponse(docs, fields, scans, reverse, distinct, projection, offset, limit, isScanAll)
	scanResults, err := secondaryindex.Scans(indexName, bucketName, indexScanAddress, scans, reverse, distinct, projection, offset, limit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)

	multiScanCount, err := secondaryindex.MultiScanCount(indexName, bucketName, indexScanAddress, scans, distinct, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan count", t)

	log.Printf("len(scanResults) = %v MultiScanCount = %v", len(scanResults), multiScanCount)
	if len(scanResults) != int(multiScanCount) {
		msg := fmt.Sprintf("MultiScanCount not same as results from MultiScan")
		FailTestIfError(errors.New(msg), "Error in scan result validation", t)
	}

	if validateOnlyCount {
		if len(scanResults) != len(docScanResults) {
			msg := fmt.Sprintf("Length of expected results %v is not equal to length of scan results", len(docScanResults), len(scanResults))
			FailTestIfError(errors.New(msg), "Error in scan result validation", t)
		}
	} else {
		err = tv.Validate(docScanResults, scanResults)
		FailTestIfError(err, "Error in scan result validation", t)
	}
}

func runManyMultiScanWithIndex(indexName string, fields []string, manyscans []qc.Scans,
	reverse, distinct bool, projection *qc.IndexProjection, offset, limit int64,
	isScanAll bool, validateOnlyCount bool, scenario string, t *testing.T) {
	var bucketName = "default"
	log.Printf("\n--- %v ---", scenario)
	for i, scans := range manyscans {
		log.Printf("\n--- Multi Scan %v ---", i)
		docScanResults := datautility.ExpectedMultiScanResponse(docs, fields, scans, reverse, distinct, projection, offset, limit, isScanAll)
		scanResults, err := secondaryindex.Scans(indexName, bucketName, indexScanAddress, scans, reverse, distinct, projection, offset, limit, c.SessionConsistency, nil)
		FailTestIfError(err, "Error in scan", t)

		if validateOnlyCount {
			if len(scanResults) != len(docScanResults) {
				msg := fmt.Sprintf("Length of expected results %v is not equal to length of scan results", len(docScanResults), len(scanResults))
				FailTestIfError(errors.New(msg), "Error in scan result validation", t)
			}
		} else {
			err = tv.Validate(docScanResults, scanResults)
			FailTestIfError(err, "Error in scan result validation", t)
		}
	}
}

func runManyMultiScanCountWithIndex(indexName string, fields []string, manyscans []qc.Scans,
	reverse, distinct bool, projection *qc.IndexProjection, offset, limit int64,
	isScanAll bool, validateOnlyCount bool, scenario string, t *testing.T) {
	var bucketName = "default"
	log.Printf("\n--- %v ---", scenario)
	for i, scans := range manyscans {
		log.Printf("\n--- Multi Scan %v ---", i)
		docScanResults := datautility.ExpectedMultiScanResponse(docs, fields, scans, reverse, distinct, projection, offset, limit, isScanAll)
		scanResults, err := secondaryindex.Scans(indexName, bucketName, indexScanAddress, scans, reverse, distinct, projection, offset, limit, c.SessionConsistency, nil)
		FailTestIfError(err, "Error in scan", t)
		multiScanCount, err := secondaryindex.MultiScanCount(indexName, bucketName, indexScanAddress, scans, distinct, c.SessionConsistency, nil)
		FailTestIfError(err, "Error in scan count", t)

		log.Printf("len(scanResults) = %v MultiScanCount = %v", len(scanResults), multiScanCount)
		if len(scanResults) != int(multiScanCount) {
			msg := fmt.Sprintf("MultiScanCount not same as results from MultiScan")
			FailTestIfError(errors.New(msg), "Error in scan result validation", t)
		}
		if validateOnlyCount {
			if len(scanResults) != len(docScanResults) {
				msg := fmt.Sprintf("Length of expected results %v is not equal to length of scan results", len(docScanResults), len(scanResults))
				FailTestIfError(errors.New(msg), "Error in scan result validation", t)
			}
		} else {
			err = tv.Validate(docScanResults, scanResults)
			FailTestIfError(err, "Error in scan result validation", t)
		}
	}
}

func runMultiScanForPrimaryIndex(indexName string, scans qc.Scans,
	reverse, distinct bool, projection *qc.IndexProjection, offset, limit int64,
	isScanAll bool, validateOnlyCount bool, scenario string, t *testing.T) {
	var bucketName = "default"
	log.Printf("\n--- %v ---", scenario)

	docScanResults := datautility.ExpectedMultiScanResponse_Primary(docs, scans, reverse, distinct, offset, limit)
	scanResults, err := secondaryindex.Scans(indexName, bucketName, indexScanAddress, scans, reverse, distinct, projection, offset, limit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	if validateOnlyCount {
		if len(scanResults) != len(docScanResults) {
			msg := fmt.Sprintf("Length of expected results %v is not equal to length of scan results", len(docScanResults), len(scanResults))
			FailTestIfError(errors.New(msg), "Error in scan result validation", t)
		}
	} else {
		err = tv.Validate(docScanResults, scanResults)
		FailTestIfError(err, "Error in scan result validation", t)
	}
}

func projpktrue(entrykeys []int64) *qc.IndexProjection {
	return &qc.IndexProjection{
		EntryKeys:  entrykeys,
		PrimaryKey: true,
	}
}

func projpkfalse(entrykeys []int64) *qc.IndexProjection {
	return &qc.IndexProjection{
		EntryKeys:  entrykeys,
		PrimaryKey: false,
	}
}

func getScanAllNoFilter() qc.Scans {
	scans := make(qc.Scans, 1)
	scans[0] = &qc.Scan{Filter: nil}
	return scans
}

func getScanAllFilterNil() qc.Scans {
	scans := make(qc.Scans, 2)
	filter2 := make([]*qc.CompositeElementFilter, 2)
	filter2[0] = &qc.CompositeElementFilter{Low: "S", High: "V", Inclusion: qc.Inclusion(uint32(0))}
	filter2[1] = &qc.CompositeElementFilter{Low: "H", High: "J", Inclusion: qc.Inclusion(uint32(3))}
	scans[0] = &qc.Scan{Filter: filter2}
	scans[1] = &qc.Scan{Filter: nil}
	return scans
}

func getScanAll_AllFiltersNil() qc.Scans {
	scans := make(qc.Scans, 2)

	filter1 := make([]*qc.CompositeElementFilter, 2)
	filter1[0] = &qc.CompositeElementFilter{Low: c.MinUnbounded, High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(3))}
	filter1[1] = &qc.CompositeElementFilter{Low: c.MinUnbounded, High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(3))}
	scans[0] = &qc.Scan{Filter: filter1}

	filter2 := make([]*qc.CompositeElementFilter, 2)
	filter2[0] = &qc.CompositeElementFilter{Low: "S", High: "V", Inclusion: qc.Inclusion(uint32(2))}
	filter2[1] = &qc.CompositeElementFilter{Low: "H", High: "J", Inclusion: qc.Inclusion(uint32(1))}
	scans[1] = &qc.Scan{Filter: filter2}
	return scans
}

func getSingleSeek() qc.Scans {
	scans := make(qc.Scans, 1)
	eq := c.SecondaryKey([]interface{}{"UTARIAN", "Michelle Mckay"})
	scans[0] = &qc.Scan{Seek: eq}
	return scans
}

func getMultipleSeek() qc.Scans {
	scans := make(qc.Scans, 2)
	eq := c.SecondaryKey([]interface{}{"UTARIAN", "Michelle Mckay"})
	scans[0] = &qc.Scan{Seek: eq}
	eq = c.SecondaryKey([]interface{}{"JUMPSTACK", "Loretta Wilkerson"})
	scans[1] = &qc.Scan{Seek: eq}
	return scans
}

func getSimpleRange() qc.Scans {
	scans := make(qc.Scans, 1)
	filter1 := make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "G", High: "N", Inclusion: qc.Inclusion(uint32(1))}
	scans[0] = &qc.Scan{Filter: filter1}
	return scans
}

func getNonOverlappingRanges() qc.Scans {
	scans := make(qc.Scans, 3)
	filter1 := make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "G", High: "K", Inclusion: qc.Inclusion(uint32(1))}
	scans[0] = &qc.Scan{Filter: filter1}

	filter2 := make([]*qc.CompositeElementFilter, 1)
	filter2[0] = &qc.CompositeElementFilter{Low: "M", High: "R", Inclusion: qc.Inclusion(uint32(2))}
	scans[1] = &qc.Scan{Filter: filter2}

	filter3 := make([]*qc.CompositeElementFilter, 1)
	filter3[0] = &qc.CompositeElementFilter{Low: "T", High: "X", Inclusion: qc.Inclusion(uint32(0))}
	scans[2] = &qc.Scan{Filter: filter3}

	return scans
}

func getOverlappingRanges() qc.Scans {
	scans := make(qc.Scans, 3)
	filter1 := make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "G", High: "K", Inclusion: qc.Inclusion(uint32(1))}
	scans[0] = &qc.Scan{Filter: filter1}

	filter2 := make([]*qc.CompositeElementFilter, 1)
	filter2[0] = &qc.CompositeElementFilter{Low: "I", High: "Q", Inclusion: qc.Inclusion(uint32(2))}
	scans[1] = &qc.Scan{Filter: filter2}

	filter3 := make([]*qc.CompositeElementFilter, 1)
	filter3[0] = &qc.CompositeElementFilter{Low: "M", High: "X", Inclusion: qc.Inclusion(uint32(0))}
	scans[2] = &qc.Scan{Filter: filter3}

	return scans
}

func getNonOverlappingFilters() qc.Scans {
	scans := make(qc.Scans, 2)
	filter1 := make([]*qc.CompositeElementFilter, 2)
	filter1[0] = &qc.CompositeElementFilter{Low: "D", High: "F", Inclusion: qc.Inclusion(uint32(0))}
	filter1[1] = &qc.CompositeElementFilter{Low: "A", High: "C", Inclusion: qc.Inclusion(uint32(1))}

	scans[0] = &qc.Scan{Filter: filter1}

	filter2 := make([]*qc.CompositeElementFilter, 2)
	filter2[0] = &qc.CompositeElementFilter{Low: "S", High: "V", Inclusion: qc.Inclusion(uint32(2))}
	filter2[1] = &qc.CompositeElementFilter{Low: "A", High: "C", Inclusion: qc.Inclusion(uint32(3))}
	scans[1] = &qc.Scan{Filter: filter2}
	return scans
}

func getOverlappingFilters() qc.Scans {
	scans := make(qc.Scans, 3)
	filter1 := make([]*qc.CompositeElementFilter, 2)
	filter1[0] = &qc.CompositeElementFilter{Low: "B", High: "H", Inclusion: qc.Inclusion(uint32(3))}
	filter1[1] = &qc.CompositeElementFilter{Low: "T", High: "X", Inclusion: qc.Inclusion(uint32(3))}
	scans[0] = &qc.Scan{Filter: filter1}

	filter2 := make([]*qc.CompositeElementFilter, 2)
	filter2[0] = &qc.CompositeElementFilter{Low: "E", High: "M", Inclusion: qc.Inclusion(uint32(3))}
	filter2[1] = &qc.CompositeElementFilter{Low: "C", High: "R", Inclusion: qc.Inclusion(uint32(3))}
	scans[1] = &qc.Scan{Filter: filter2}

	filter3 := make([]*qc.CompositeElementFilter, 2)
	filter3[0] = &qc.CompositeElementFilter{Low: "S", High: "X", Inclusion: qc.Inclusion(uint32(3))}
	filter3[1] = &qc.CompositeElementFilter{Low: "A", High: "D", Inclusion: qc.Inclusion(uint32(3))}
	scans[2] = &qc.Scan{Filter: filter3}

	return scans
}

func getBoundaryFilters() qc.Scans {
	scans := make(qc.Scans, 2)
	filter1 := make([]*qc.CompositeElementFilter, 2)
	filter1[0] = &qc.CompositeElementFilter{Low: "GEEKWAGON", High: "INJOY", Inclusion: qc.Inclusion(uint32(3))}
	filter1[1] = &qc.CompositeElementFilter{Low: "Hendrix Orr", High: "Trina Mcfadden", Inclusion: qc.Inclusion(uint32(3))}
	scans[0] = &qc.Scan{Filter: filter1}

	filter2 := make([]*qc.CompositeElementFilter, 2)
	filter2[0] = &qc.CompositeElementFilter{Low: "INJOY", High: "ORBIN", Inclusion: qc.Inclusion(uint32(3))}
	filter2[1] = &qc.CompositeElementFilter{Low: "Trina Mcfadden", High: "ZZZZZ", Inclusion: qc.Inclusion(uint32(3))}
	scans[1] = &qc.Scan{Filter: filter2}

	return scans
}

func getSeekAndFilters_NonOverlapping() qc.Scans {
	scans := make(qc.Scans, 2)

	eq := c.SecondaryKey([]interface{}{"UTARIAN", "Michelle Mckay"})
	scans[0] = &qc.Scan{Seek: eq}

	filter2 := make([]*qc.CompositeElementFilter, 2)
	filter2[0] = &qc.CompositeElementFilter{Low: "F", High: "K", Inclusion: qc.Inclusion(uint32(2))}
	filter2[1] = &qc.CompositeElementFilter{Low: "H", High: "L", Inclusion: qc.Inclusion(uint32(1))}
	scans[1] = &qc.Scan{Filter: filter2}

	return scans
}

func getSeekAndFilters_Overlapping() qc.Scans {
	scans := make(qc.Scans, 2)

	filter2 := make([]*qc.CompositeElementFilter, 2)
	filter2[0] = &qc.CompositeElementFilter{Low: "F", High: "K", Inclusion: qc.Inclusion(uint32(2))}
	filter2[1] = &qc.CompositeElementFilter{Low: "H", High: "L", Inclusion: qc.Inclusion(uint32(1))}
	scans[0] = &qc.Scan{Filter: filter2}

	eq := c.SecondaryKey([]interface{}{"IMAGINART", "Janell Hyde"})
	scans[1] = &qc.Scan{Seek: eq}

	return scans
}

func getSimpleRangeLowUnbounded() qc.Scans {
	scans := make(qc.Scans, 1)
	filter1 := make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: c.MinUnbounded, High: "N", Inclusion: qc.Inclusion(uint32(1))}
	scans[0] = &qc.Scan{Filter: filter1}
	return scans
}

func getSimpleRangeHighUnbounded() qc.Scans {
	scans := make(qc.Scans, 1)
	filter1 := make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "P", High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(1))}
	scans[0] = &qc.Scan{Filter: filter1}
	return scans
}

func getSimpleRangeMultipleUnbounded() qc.Scans {
	scans := make(qc.Scans, 2)
	filter1 := make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: c.MinUnbounded, High: "N", Inclusion: qc.Inclusion(uint32(1))}
	scans[0] = &qc.Scan{Filter: filter1}

	filter2 := make([]*qc.CompositeElementFilter, 1)
	filter2[0] = &qc.CompositeElementFilter{Low: "D", High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(1))}
	scans[1] = &qc.Scan{Filter: filter2}
	return scans
}

func getFiltersWithUnbounded() qc.Scans {
	scans := make(qc.Scans, 2)
	filter1 := make([]*qc.CompositeElementFilter, 2)
	filter1[0] = &qc.CompositeElementFilter{Low: "E", High: "L", Inclusion: qc.Inclusion(uint32(0))}
	filter1[1] = &qc.CompositeElementFilter{Low: c.MinUnbounded, High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(1))}

	scans[0] = &qc.Scan{Filter: filter1}

	filter2 := make([]*qc.CompositeElementFilter, 2)
	filter2[0] = &qc.CompositeElementFilter{Low: "P", High: "T", Inclusion: qc.Inclusion(uint32(2))}
	filter2[1] = &qc.CompositeElementFilter{Low: "Q", High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(3))}
	scans[1] = &qc.Scan{Filter: filter2}
	return scans

	return scans
}

func getFiltersLowGreaterThanHigh() qc.Scans {
	scans := make(qc.Scans, 2)
	filter1 := make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "L", High: "E", Inclusion: qc.Inclusion(uint32(0))}
	scans[0] = &qc.Scan{Filter: filter1}

	filter2 := make([]*qc.CompositeElementFilter, 2)
	filter2[0] = &qc.CompositeElementFilter{Low: "P", High: "T", Inclusion: qc.Inclusion(uint32(2))}
	filter2[1] = &qc.CompositeElementFilter{Low: "Q", High: "Z", Inclusion: qc.Inclusion(uint32(3))}
	scans[1] = &qc.Scan{Filter: filter2}

	return scans
}

func getSingleIndexSimpleRange() qc.Scans {
	scans := make(qc.Scans, 1)
	filter1 := make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "G", High: "N", Inclusion: qc.Inclusion(uint32(2))}
	scans[0] = &qc.Scan{Filter: filter1}
	return scans
}

func getSingleIndex_SimpleRanges_NonOverlapping() qc.Scans {
	scans := make(qc.Scans, 3)
	filter1 := make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "B", High: "GZZZZZ", Inclusion: qc.Inclusion(uint32(0))}
	scans[0] = &qc.Scan{Filter: filter1}

	filter2 := make([]*qc.CompositeElementFilter, 1)
	filter2[0] = &qc.CompositeElementFilter{Low: "J", High: "OZZZZZ", Inclusion: qc.Inclusion(uint32(3))}
	scans[1] = &qc.Scan{Filter: filter2}

	filter3 := make([]*qc.CompositeElementFilter, 1)
	filter3[0] = &qc.CompositeElementFilter{Low: "R", High: "XZZZZZ", Inclusion: qc.Inclusion(uint32(1))}
	scans[2] = &qc.Scan{Filter: filter3}
	return scans
}

func getSingleIndex_SimpleRanges_Overlapping() qc.Scans {
	scans := make(qc.Scans, 4)
	filter1 := make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "B", High: "OZZZZZ", Inclusion: qc.Inclusion(uint32(0))}
	scans[0] = &qc.Scan{Filter: filter1}

	filter2 := make([]*qc.CompositeElementFilter, 1)
	filter2[0] = &qc.CompositeElementFilter{Low: "E", High: "GZZZZZ", Inclusion: qc.Inclusion(uint32(3))}
	scans[1] = &qc.Scan{Filter: filter2}

	filter3 := make([]*qc.CompositeElementFilter, 1)
	filter3[0] = &qc.CompositeElementFilter{Low: "J", High: "RZZZZZ", Inclusion: qc.Inclusion(uint32(1))}
	scans[2] = &qc.Scan{Filter: filter3}

	filter4 := make([]*qc.CompositeElementFilter, 1)
	filter4[0] = &qc.CompositeElementFilter{Low: "S", High: "XZZZZZ", Inclusion: qc.Inclusion(uint32(1))}
	scans[3] = &qc.Scan{Filter: filter4}
	return scans
}

func get3FieldsSingleSeek() qc.Scans {
	scans := make(qc.Scans, 1)
	eq := c.SecondaryKey([]interface{}{"SOLAREN", "Michele Yang", float64(25)})
	scans[0] = &qc.Scan{Seek: eq}
	return scans
}

func get3FieldsMultipleSeeks() qc.Scans {
	scans := make(qc.Scans, 3)
	eq := c.SecondaryKey([]interface{}{"RODEOLOGY", "Tasha Dodson", float64(23)})
	scans[0] = &qc.Scan{Seek: eq}
	eq = c.SecondaryKey([]interface{}{"NETROPIC", "Lillian Mcneil", float64(24)})
	scans[1] = &qc.Scan{Seek: eq}
	eq = c.SecondaryKey([]interface{}{"ZYTREX", "Olga Patton", float64(29)})
	scans[2] = &qc.Scan{Seek: eq}
	return scans
}

func get3FieldsMultipleSeeks_Identical() qc.Scans {
	scans := make(qc.Scans, 3)
	eq := c.SecondaryKey([]interface{}{"RODEOLOGY", "Tasha Dodson", float64(23)})
	scans[0] = &qc.Scan{Seek: eq}
	eq = c.SecondaryKey([]interface{}{"NETROPIC", "Lillian Mcneil", float64(24)})
	scans[1] = &qc.Scan{Seek: eq}
	eq = c.SecondaryKey([]interface{}{"RODEOLOGY", "Tasha Dodson", float64(23)})
	scans[2] = &qc.Scan{Seek: eq}
	return scans
}

func getPrimaryRange() qc.Scans {
	scans := make(qc.Scans, 1)
	filter1 := make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "A", High: "zzzzz", Inclusion: qc.Inclusion(uint32(1))}
	scans[0] = &qc.Scan{Filter: filter1}
	return scans
}

// Trailing high key unbounded
func getCompIndexHighUnbounded1() []qc.Scans {
	manyscans := make([]qc.Scans, 0, 2)

	scans1 := make(qc.Scans, 1)
	filter1 := make([]*qc.CompositeElementFilter, 2)
	filter1[0] = &qc.CompositeElementFilter{Low: "KANGLE", High: "KANGLE", Inclusion: qc.Inclusion(uint32(3))}
	filter1[1] = &qc.CompositeElementFilter{Low: "Daniel Beach", High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(0))}
	scans1[0] = &qc.Scan{Filter: filter1}
	manyscans = append(manyscans, scans1)

	scans2 := make(qc.Scans, 1)
	filter2 := make([]*qc.CompositeElementFilter, 2)
	filter2[0] = &qc.CompositeElementFilter{Low: "KANGLE", High: "KANGLE", Inclusion: qc.Inclusion(uint32(0))}
	filter2[1] = &qc.CompositeElementFilter{Low: "Daniel Beach", High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(0))}
	scans2[0] = &qc.Scan{Filter: filter2}
	manyscans = append(manyscans, scans2)

	scans3 := make(qc.Scans, 1)
	filter3 := make([]*qc.CompositeElementFilter, 2)
	filter3[0] = &qc.CompositeElementFilter{Low: "KANGLE", High: "KANGLE", Inclusion: qc.Inclusion(uint32(3))}
	filter3[1] = &qc.CompositeElementFilter{Low: "C", High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(3))}
	scans3[0] = &qc.Scan{Filter: filter3}
	manyscans = append(manyscans, scans3)
	return manyscans
}

// Leading high key unbounded
func getCompIndexHighUnbounded2() []qc.Scans {
	manyscans := make([]qc.Scans, 0, 2)

	scans1 := make(qc.Scans, 1)
	filter1 := make([]*qc.CompositeElementFilter, 2)
	filter1[0] = &qc.CompositeElementFilter{Low: "KANGLE", High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(3))}
	filter1[1] = &qc.CompositeElementFilter{Low: "Daniel Beach", High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(0))}
	scans1[0] = &qc.Scan{Filter: filter1}
	manyscans = append(manyscans, scans1)

	scans2 := make(qc.Scans, 1)
	filter2 := make([]*qc.CompositeElementFilter, 2)
	filter2[0] = &qc.CompositeElementFilter{Low: "KANGLE", High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(0))}
	filter2[1] = &qc.CompositeElementFilter{Low: "Daniel Beach", High: "P", Inclusion: qc.Inclusion(uint32(0))}
	scans2[0] = &qc.Scan{Filter: filter2}
	manyscans = append(manyscans, scans2)

	scans3 := make(qc.Scans, 1)
	filter3 := make([]*qc.CompositeElementFilter, 2)
	filter3[0] = &qc.CompositeElementFilter{Low: "KANGLE", High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(3))}
	filter3[1] = &qc.CompositeElementFilter{Low: "C", High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(3))}
	scans3[0] = &qc.Scan{Filter: filter3}
	manyscans = append(manyscans, scans3)
	return manyscans
}

// Trailing low key unbounded, Pratap's scenario
func getCompIndexHighUnbounded3() []qc.Scans {
	manyscans := make([]qc.Scans, 0, 1)

	scans1 := make(qc.Scans, 2)
	filter1 := make([]*qc.CompositeElementFilter, 2)
	filter1[0] = &qc.CompositeElementFilter{Low: "KANGLE", High: "MEDIFAX", Inclusion: qc.Inclusion(uint32(3))}
	filter1[1] = &qc.CompositeElementFilter{Low: "Snow Beach", High: "Travis Wilson", Inclusion: qc.Inclusion(uint32(0))}
	scans1[0] = &qc.Scan{Filter: filter1}

	filter2 := make([]*qc.CompositeElementFilter, 2)
	filter2[0] = &qc.CompositeElementFilter{Low: "KANGLE", High: "OTHERWAY", Inclusion: qc.Inclusion(uint32(3))}
	filter2[1] = &qc.CompositeElementFilter{Low: c.MinUnbounded, High: "Robert Horne", Inclusion: qc.Inclusion(uint32(0))}
	scans1[1] = &qc.Scan{Filter: filter2}

	manyscans = append(manyscans, scans1)
	return manyscans
}

func getCompIndexHighUnbounded4() []qc.Scans {
	manyscans := make([]qc.Scans, 0, 1)

	scans1 := make(qc.Scans, 2)
	filter1 := make([]*qc.CompositeElementFilter, 2)
	filter1[0] = &qc.CompositeElementFilter{Low: c.MinUnbounded, High: "P", Inclusion: qc.Inclusion(uint32(3))}
	filter1[1] = &qc.CompositeElementFilter{Low: "A", High: "M", Inclusion: qc.Inclusion(uint32(0))}
	scans1[0] = &qc.Scan{Filter: filter1}

	filter2 := make([]*qc.CompositeElementFilter, 2)
	filter2[0] = &qc.CompositeElementFilter{Low: c.MinUnbounded, High: "R", Inclusion: qc.Inclusion(uint32(3))}
	filter2[1] = &qc.CompositeElementFilter{Low: "Q", High: "Z", Inclusion: qc.Inclusion(uint32(0))}
	scans1[1] = &qc.Scan{Filter: filter2}

	manyscans = append(manyscans, scans1)
	return manyscans
}

func getCompIndexHighUnbounded5() []qc.Scans {
	manyscans := make([]qc.Scans, 0, 1)

	scans1 := make(qc.Scans, 2)
	filter1 := make([]*qc.CompositeElementFilter, 2)
	filter1[0] = &qc.CompositeElementFilter{Low: c.MinUnbounded, High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(3))}
	filter1[1] = &qc.CompositeElementFilter{Low: "A", High: "M", Inclusion: qc.Inclusion(uint32(0))}
	scans1[0] = &qc.Scan{Filter: filter1}

	filter2 := make([]*qc.CompositeElementFilter, 2)
	filter2[0] = &qc.CompositeElementFilter{Low: c.MinUnbounded, High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(3))}
	filter2[1] = &qc.CompositeElementFilter{Low: "Q", High: "Z", Inclusion: qc.Inclusion(uint32(0))}
	scans1[1] = &qc.Scan{Filter: filter2}

	manyscans = append(manyscans, scans1)
	return manyscans
}

func getSeekBoundaries() []qc.Scans {
	manyscans := make([]qc.Scans, 0, 1)

	//1: Scan encompassing the seek
	scans1 := make(qc.Scans, 2)
	eq := c.SecondaryKey([]interface{}{"IMAGINART", "Janell Hyde"})
	scans1[0] = &qc.Scan{Seek: eq}

	filter2 := make([]*qc.CompositeElementFilter, 2)
	filter2[0] = &qc.CompositeElementFilter{Low: "F", High: "K", Inclusion: qc.Inclusion(uint32(2))}
	filter2[1] = &qc.CompositeElementFilter{Low: "S", High: "V", Inclusion: qc.Inclusion(uint32(0))}
	scans1[1] = &qc.Scan{Filter: filter2}
	manyscans = append(manyscans, scans1)

	//2: Two seeks at same point
	scans2 := make(qc.Scans, 2)
	scans2[0] = &qc.Scan{Seek: c.SecondaryKey([]interface{}{"FORTEAN", "Marshall Chavez"})}
	scans2[1] = &qc.Scan{Seek: c.SecondaryKey([]interface{}{"FORTEAN", "Marshall Chavez"})}
	manyscans = append(manyscans, scans2)

	//3: Scan1 high is same as Seek
	scans3 := make(qc.Scans, 2)
	filter3 := make([]*qc.CompositeElementFilter, 2)
	filter3[0] = &qc.CompositeElementFilter{Low: "C", High: "FORTEAN", Inclusion: qc.Inclusion(uint32(3))}
	filter3[1] = &qc.CompositeElementFilter{Low: "H", High: "Marshall Chavez", Inclusion: qc.Inclusion(uint32(3))}
	scans3[0] = &qc.Scan{Filter: filter3}

	eq = c.SecondaryKey([]interface{}{"FORTEAN", "Marshall Chavez"})
	scans3[1] = &qc.Scan{Seek: eq}
	manyscans = append(manyscans, scans3)

	//3.1: Scan1 ends in Seek and Scan2 begins at the Seek
	scans4 := make(qc.Scans, 3)
	filter4 := make([]*qc.CompositeElementFilter, 2)
	filter4[0] = &qc.CompositeElementFilter{Low: "C", High: "FORTEAN", Inclusion: qc.Inclusion(uint32(3))}
	filter4[1] = &qc.CompositeElementFilter{Low: "H", High: "Marshall Chavez", Inclusion: qc.Inclusion(uint32(3))}
	scans4[0] = &qc.Scan{Filter: filter4}

	eq = c.SecondaryKey([]interface{}{"FORTEAN", "Marshall Chavez"})
	scans4[1] = &qc.Scan{Seek: eq}

	filter5 := make([]*qc.CompositeElementFilter, 2)
	filter5[0] = &qc.CompositeElementFilter{Low: "FORTEAN", High: "O", Inclusion: qc.Inclusion(uint32(3))}
	filter5[1] = &qc.CompositeElementFilter{Low: "Marshall Chavez", High: "P", Inclusion: qc.Inclusion(uint32(3))}
	scans4[2] = &qc.Scan{Filter: filter5}

	manyscans = append(manyscans, scans4)

	//4: Scan1 bordered by Seek1 and Seek2 on either sides
	scans5 := make(qc.Scans, 3)
	scans5[0] = &qc.Scan{Seek: c.SecondaryKey([]interface{}{"FORTEAN", "Marshall Chavez"})}

	filter6 := make([]*qc.CompositeElementFilter, 2)
	filter6[0] = &qc.CompositeElementFilter{Low: "FORTEAN", High: "IDEALIS", Inclusion: qc.Inclusion(uint32(3))}
	filter6[1] = &qc.CompositeElementFilter{Low: "Marshall Chavez", High: "Watts Calderon", Inclusion: qc.Inclusion(uint32(3))}
	scans5[1] = &qc.Scan{Filter: filter6}

	scans5[2] = &qc.Scan{Seek: c.SecondaryKey([]interface{}{"IDEALIS", "Watts Calderon"})}

	manyscans = append(manyscans, scans5)

	//5: S1 - single CEF  S2 (single CEf) overlaps with S3 (Multiple CEFs)
	scans6 := make(qc.Scans, 3)
	filter7 := make([]*qc.CompositeElementFilter, 1)
	filter7[0] = &qc.CompositeElementFilter{Low: "E", High: "J", Inclusion: qc.Inclusion(uint32(1))}
	scans6[0] = &qc.Scan{Filter: filter7}

	filter8 := make([]*qc.CompositeElementFilter, 1)
	filter8[0] = &qc.CompositeElementFilter{Low: "M", High: "S", Inclusion: qc.Inclusion(uint32(2))}
	scans6[1] = &qc.Scan{Filter: filter8}

	filter9 := make([]*qc.CompositeElementFilter, 2)
	filter9[0] = &qc.CompositeElementFilter{Low: "P", High: "X", Inclusion: qc.Inclusion(uint32(0))}
	filter9[1] = &qc.CompositeElementFilter{Low: "N", High: "Z", Inclusion: qc.Inclusion(uint32(3))}
	scans6[2] = &qc.Scan{Filter: filter9}

	manyscans = append(manyscans, scans6)

	//6
	scans7 := make(qc.Scans, 2)
	filter10 := make([]*qc.CompositeElementFilter, 2)
	filter10[0] = &qc.CompositeElementFilter{Low: c.MinUnbounded, High: "L", Inclusion: qc.Inclusion(uint32(1))}
	filter10[1] = &qc.CompositeElementFilter{Low: c.MinUnbounded, High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(1))}
	scans7[0] = &qc.Scan{Filter: filter10}

	filter11 := make([]*qc.CompositeElementFilter, 2)
	filter11[0] = &qc.CompositeElementFilter{Low: "A", High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(2))}
	filter11[1] = &qc.CompositeElementFilter{Low: c.MinUnbounded, High: "C", Inclusion: qc.Inclusion(uint32(2))}
	scans7[1] = &qc.Scan{Filter: filter11}
	manyscans = append(manyscans, scans7)

	//7
	scans8 := make(qc.Scans, 2)
	scans8[0] = &qc.Scan{Seek: c.SecondaryKey([]interface{}{"GEEKETRON", "Reese Fletcher"})}

	filter12 := make([]*qc.CompositeElementFilter, 2)
	filter12[0] = &qc.CompositeElementFilter{Low: "GEEKETRON", High: "GEEKETRON", Inclusion: qc.Inclusion(uint32(3))}
	filter12[1] = &qc.CompositeElementFilter{Low: "Hahn Fletcher", High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(3))}
	scans8[1] = &qc.Scan{Filter: filter12}
	manyscans = append(manyscans, scans8)

	return manyscans
}

// Test Cases -

// Primary index - Seek with nil

// Nil Span: Caused error

// Filter Range - simple index
/*
	scans := make(qc.Scans, 1)
	filter1 := make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "CYTRAK", High: "SPRINGBEE", Inclusion: qc.Inclusion(uint32(1))}
	scans[0] = &qc.Scan{Filter: filter1}
*/

// Scan a simple index (non-composite)

// Overlapping regions on the boundary

// Mix of composite filters and non-composite filters

// Inclusions variations

// Low > high scenarios

// Out of order non-overlapping
// out of order overlapping

// Primary overlapping ranges with different inclusions

// Projection tests
// Project first, second, third, etc. One at a time.
// Project two a time - all combinations
// Project three a time - all combinations
// Project all
// Alternate keys to be projected
// Project with array index distinct (no count) - TODO
// Project with array index duplicate (count encoded) - TODO
// Entry keys empty, Primary true or false
// Entry keys nil, Primary true or false
// Projection nil
// EntryKeys nil
// Wrong positions in EntryKeys (> len of comp keys, negative)
// More entrykeys than len of ck's
// Project out of order keys
