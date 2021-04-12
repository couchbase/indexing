package functionaltests

import (
	"log"
	"strconv"
	"testing"

	c "github.com/couchbase/indexing/secondary/common"
	qc "github.com/couchbase/indexing/secondary/queryport/client"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/datautility"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	tv "github.com/couchbase/indexing/secondary/tests/framework/validation"
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
	runMultiScanCountWithIndex(index, fields, getNonOverlappingFilters2(), false, true, nil, 0, defaultlimit, false, false, "NonOverlappingFilters2", t)
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

	// Prefix sort scenarios
	runManyMultiScanWithIndex(index2, fields2, getPrefixSortVariations(), false, false, nil, 0, defaultlimit, false, false, "PrefixSortVariations", t)
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

	log.Printf("indexes are: %v, %v, %v, %v, %v", index_1field, index_2field, index_3field, index_4field, index_5field)
	log.Printf("fields are: %v, %v, %v, %v, %v", fields_1, fields_2, fields_3, fields_4, fields_5)

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

	reqbody = restful_clonebody(reqscanscount)
	count, err := getscanscount(ids[0], reqbody)
	FailTestIfError(err, "Error in getscanscout()", t)
	log.Printf("Result from multiscancount API = %v\n", count)
}

func TestMultiScanPrimaryIndexVariations(t *testing.T) {
	log.Printf("In TestMultiScanPrimaryIndexVariations()")

	var primaryindex = "index_pi"
	var bucketName = "default"
	primaryIndexDocs := make(tc.KeyValues)
	for i := 1; i <= 100; i++ {
		key := "doc" + strconv.Itoa(i)
		value := make(map[string]interface{})
		value["temp1"] = randomNum(0, 100)
		value["temp2"] = randString(10)
		primaryIndexDocs[key] = value
	}
	kvutility.SetKeyValues(primaryIndexDocs, bucketName, "", clusterconfig.KVAddress)
	UpdateKVDocs(primaryIndexDocs, docs)
	// Create a primary index
	err := secondaryindex.CreateSecondaryIndex(primaryindex, bucketName, indexManagementAddress, "", nil, true, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	// Scenario1: No Overlap
	scans := make(qc.Scans, 4)
	scans[0] = &qc.Scan{Filter: getPrimaryFilter("doc20", "doc30", 0)}
	scans[1] = &qc.Scan{Filter: getPrimaryFilter("doc40", "doc50", 1)}
	scans[2] = &qc.Scan{Filter: getPrimaryFilter("doc60", "doc70", 2)}
	scans[3] = &qc.Scan{Filter: getPrimaryFilter("doc80", "doc90", 3)}
	runMultiScanForPrimaryIndex(primaryindex, scans, false, false, nil, 0, defaultlimit, true, false, "No Overlap", t)

	// Scenario2: Proper Overlap
	scans = make(qc.Scans, 4)
	scans[0] = &qc.Scan{Filter: getPrimaryFilter("doc20", "doc40", 0)}
	scans[1] = &qc.Scan{Filter: getPrimaryFilter("doc30", "doc50", 3)}
	scans[2] = &qc.Scan{Filter: getPrimaryFilter("doc60", "doc80", 1)}
	scans[3] = &qc.Scan{Filter: getPrimaryFilter("doc70", "doc90", 1)}
	runMultiScanForPrimaryIndex(primaryindex, scans, false, false, nil, 0, defaultlimit, true, false, "Proper Overlap", t)

	// Scenario3: Low Boundary Overlap
	scans = make(qc.Scans, 2)
	scans[0] = &qc.Scan{Filter: getPrimaryFilter("doc11", "doc21", 0)}
	scans[1] = &qc.Scan{Filter: getPrimaryFilter("doc11", "doc25", 2)}
	runMultiScanForPrimaryIndex(primaryindex, scans, false, false, nil, 0, defaultlimit, true, false, "Low Boundary Overlap", t)

	// Scenario4: Complex Overlaps
	scans = make(qc.Scans, 6)
	scans[0] = &qc.Scan{Filter: getPrimaryFilter("doc50", "doc70", 0)}
	scans[1] = &qc.Scan{Filter: getPrimaryFilter("doc32", "doc40", 2)}
	scans[2] = &qc.Scan{Filter: getPrimaryFilter("doc59", "doc70", 1)}
	scans[3] = &qc.Scan{Filter: getPrimaryFilter("doc20", "doc40", 0)}
	scans[4] = &qc.Scan{Filter: getPrimaryFilter("doc66", "doc70", 1)}
	scans[5] = &qc.Scan{Filter: getPrimaryFilter("doc20", "doc28", 1)}
	runMultiScanForPrimaryIndex(primaryindex, scans, false, false, nil, 0, defaultlimit, true, false, "Complex Overlaps", t)

	// Scenario5: Multiple Boundary Equal Overlaps
	scans = make(qc.Scans, 6)
	scans[0] = &qc.Scan{Filter: getPrimaryFilter("doc20", "doc30", 0)}
	scans[1] = &qc.Scan{Filter: getPrimaryFilter("doc20", "doc30", 0)}
	scans[2] = &qc.Scan{Filter: getPrimaryFilter("doc20", "doc30", 0)}
	scans[3] = &qc.Scan{Filter: getPrimaryFilter("doc30", "doc40", 0)}
	scans[4] = &qc.Scan{Filter: getPrimaryFilter("doc30", "doc40", 0)}
	scans[5] = &qc.Scan{Filter: getPrimaryFilter("doc30", "doc40", 0)}
	runMultiScanForPrimaryIndex(primaryindex, scans, false, false, nil, 0, defaultlimit, true, false, "Multiple Equal Overlaps", t)

	// Scenario6: Boundary and Subset Overlaps
	scans = make(qc.Scans, 5)
	scans[0] = &qc.Scan{Filter: getPrimaryFilter("doc66", "doc72", 1)}
	scans[1] = &qc.Scan{Filter: getPrimaryFilter("doc30", "doc40", 0)}
	scans[2] = &qc.Scan{Filter: getPrimaryFilter("doc20", "doc30", 0)}
	scans[3] = &qc.Scan{Filter: getPrimaryFilter("doc60", "doc80", 0)}
	scans[4] = &qc.Scan{Filter: getPrimaryFilter("doc60", "doc80", 2)}
	runMultiScanForPrimaryIndex(primaryindex, scans, false, false, nil, 0, defaultlimit, true, false, "Boundary and Subset Overlaps", t)

	// Scenario7: Point Overlaps
	scans = make(qc.Scans, 2)
	scans[0] = &qc.Scan{Filter: getPrimaryFilter("doc25", "doc25", 0)}
	scans[1] = &qc.Scan{Filter: getPrimaryFilter("doc25", "doc25", 1)}
	runMultiScanForPrimaryIndex(primaryindex, scans, false, false, nil, 0, defaultlimit, true, false, "Point Overlaps", t)

	// Scenario8: Boundary and Point Overlaps
	scans = make(qc.Scans, 2)
	scans[0] = &qc.Scan{Filter: getPrimaryFilter("doc25", "doc30", 0)}
	scans[1] = &qc.Scan{Filter: getPrimaryFilter("doc30", "doc30", 3)}
	runMultiScanForPrimaryIndex(primaryindex, scans, false, false, nil, 0, defaultlimit, true, false, "Boundary and Point Overlaps", t)

	log.Printf("\n--- %v ---", "Primary index range null")
	scans = make(qc.Scans, 1)
	scans[0] = &qc.Scan{Filter: getPrimaryFilter(nil, nil, 3)}
	scans2 := make(qc.Scans, 1)
	scans2[0] = &qc.Scan{Filter: getPrimaryFilter("z", "a", 0)}
	docScanResults := datautility.ExpectedMultiScanResponse_Primary(docs, scans2, false, false, 0, defaultlimit)
	scanResults, err := secondaryindex.Scans(primaryindex, bucketName, indexScanAddress, scans, false, false, nil, 0, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.ValidateActual(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	kvutility.DeleteKeys(primaryIndexDocs, bucketName, "", clusterconfig.KVAddress)
	for key, _ := range primaryIndexDocs {
		delete(docs, key) // Update docs object with deleted keys
	}
	err = secondaryindex.DropSecondaryIndex(primaryindex, bucketName, clusterconfig.KVAddress)
	FailTestIfError(err, "Error in index drop", t)
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

	validateMultiScanResults(docScanResults, scanResults, validateOnlyCount, projection, t)
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

func getPrefixSortVariations() []qc.Scans {
	manyscans := make([]qc.Scans, 0, 2)

	//1
	scans := make(qc.Scans, 2)
	filter := make([]*qc.CompositeElementFilter, 2)
	filter[0] = &qc.CompositeElementFilter{Low: "QUALITEX", High: "QUALITEX", Inclusion: qc.Inclusion(uint32(3))}
	filter[1] = &qc.CompositeElementFilter{Low: "Laurel Kirkland", High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(1))}
	scans[0] = &qc.Scan{Filter: filter}

	filter = make([]*qc.CompositeElementFilter, 1)
	filter[0] = &qc.CompositeElementFilter{Low: "QUALITEX", High: "QUALITEX", Inclusion: qc.Inclusion(uint32(3))}
	scans[1] = &qc.Scan{Filter: filter}
	manyscans = append(manyscans, scans)

	//2
	scans = make(qc.Scans, 3)
	filter = make([]*qc.CompositeElementFilter, 2)
	filter[0] = &qc.CompositeElementFilter{Low: c.MinUnbounded, High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(3))}
	filter[1] = &qc.CompositeElementFilter{Low: "Laurel Kirkland", High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(1))}
	scans[0] = &qc.Scan{Filter: filter}

	filter = make([]*qc.CompositeElementFilter, 2)
	filter[0] = &qc.CompositeElementFilter{Low: c.MinUnbounded, High: "M", Inclusion: qc.Inclusion(uint32(3))}
	filter[1] = &qc.CompositeElementFilter{Low: c.MinUnbounded, High: "Pat Sharpe", Inclusion: qc.Inclusion(uint32(3))}
	scans[1] = &qc.Scan{Filter: filter}

	filter = make([]*qc.CompositeElementFilter, 2)
	filter[0] = &qc.CompositeElementFilter{Low: "B", High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(3))}
	filter[1] = &qc.CompositeElementFilter{Low: c.MinUnbounded, High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(3))}
	scans[2] = &qc.Scan{Filter: filter}

	manyscans = append(manyscans, scans)

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
