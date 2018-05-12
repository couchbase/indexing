package functionaltests

import (
	"errors"
	"fmt"
	c "github.com/couchbase/indexing/secondary/common"
	qc "github.com/couchbase/indexing/secondary/queryport/client"
	"github.com/couchbase/indexing/secondary/tests/framework/datautility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	tv "github.com/couchbase/indexing/secondary/tests/framework/validation"
	"log"
	"testing"
)

func TestMultiScanDescSetup(t *testing.T) {
	log.Printf("In TestMultiScanDescSetup()")

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)

	var index1 = "index_companyname_desc"
	var index2 = "index_company_desc"
	var index3 = "index_company_name_age_desc"
	var bucketName = "default"

	err := secondaryindex.CreateSecondaryIndex2(index1, bucketName, indexManagementAddress, "", []string{"company", "name"}, []bool{true, true}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.CreateSecondaryIndex2(index2, bucketName, indexManagementAddress, "", []string{"company"}, []bool{true}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.CreateSecondaryIndex2(index3, bucketName, indexManagementAddress, "", []string{"company", "name", "age"}, []bool{false, true, false}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

}

func TestMultiScanDescScenarios(t *testing.T) {
	log.Printf("In TestMultiScanDescScenarios()")

	secondaryindex.CheckCollation = false
	secondaryindex.DescCollation = true

	log.Printf("\n\n--------- Composite Index with 2 fields ---------")
	runMultiScanDesc(getScanAllNoFilter(), false, false, nil, 0, defaultlimit, true, false, "ScanAllNoFilter", t)
	runMultiScanDesc(getScanAllFilterNil(), false, false, nil, 0, defaultlimit, true, false, "ScanAllFilterNil", t)
	runMultiScanDesc(getScanAll_AllFiltersNil(), false, false, nil, 0, defaultlimit, true, false, "ScanAll_AllFiltersNil", t)

	runMultiScanDesc(getSingleSeek(), false, false, nil, 0, defaultlimit, false, false, "SingleSeek", t)
	runMultiScanDesc(getMultipleSeek(), false, false, nil, 0, defaultlimit, false, false, "MultipleSeek", t)

	runMultiScanDesc(getSimpleRange(), false, false, nil, 0, defaultlimit, false, false, "SimpleRange", t)
	runMultiScanDesc(getNonOverlappingRanges(), false, false, nil, 0, defaultlimit, false, false, "NonOverlappingRanges", t)
	runMultiScanDesc(getOverlappingRanges(), false, false, nil, 0, defaultlimit, false, false, "OverlappingRanges", t)

	runMultiScanDesc(getNonOverlappingFilters(), false, false, nil, 0, defaultlimit, false, false, "NonOverlappingFilters", t)
	runMultiScanDesc(getOverlappingFilters(), false, false, nil, 0, defaultlimit, false, false, "OverlappingFilters", t)
	runMultiScanDesc(getBoundaryFilters(), false, false, nil, 0, defaultlimit, false, false, "BoundaryFilters", t)

	runMultiScanDesc(getSeekAndFilters_NonOverlapping(), false, false, nil, 0, defaultlimit, false, false, "SeekAndFilters_NonOverlapping", t)
	runMultiScanDesc(getSeekAndFilters_Overlapping(), false, false, nil, 0, defaultlimit, false, false, "SeekAndFilters_Overlapping", t)

	// low-high unbounded , low>high
	runMultiScanDesc(getSimpleRangeLowUnbounded(), false, false, nil, 0, defaultlimit, false, false, "SimpleRangeLowUnbounded", t)
	runMultiScanDesc(getSimpleRangeHighUnbounded(), false, false, nil, 0, defaultlimit, false, false, "SimpleRangeHighUnbounded", t)
	runMultiScanDesc(getSimpleRangeMultipleUnbounded(), false, false, nil, 0, defaultlimit, false, false, "SimpleRangeMultipleUnbounded", t)
	runMultiScanDesc(getFiltersWithUnbounded(), false, false, nil, 0, defaultlimit, false, false, "FiltersWithUnbounded", t)
	runMultiScanDesc(getFiltersLowGreaterThanHigh(), false, false, nil, 0, defaultlimit, false, false, "FiltersLowGreaterThanHigh", t)

	log.Printf("\n\n--------- Simple Index with 1 field ---------")
	var index2 = "index_company_desc"
	fields := []string{"company"}
	runMultiScanWithIndex(index2, fields, getSingleIndexSimpleRange(), false, false, nil, 0, defaultlimit, false, false, "SingleIndexSimpleRange", t)
	runMultiScanWithIndex(index2, fields, getSingleIndex_SimpleRanges_NonOverlapping(), false, false, nil, 0, defaultlimit, false, false, "SingleIndex_SimpleRanges_NonOverlapping", t)
	runMultiScanWithIndex(index2, fields, getSingleIndex_SimpleRanges_Overlapping(), false, false, nil, 0, defaultlimit, false, false, "SingleIndex_SimpleRanges_Overlapping", t)

	log.Printf("\n\n--------- Composite Index with 3 fields ---------")
	var index3 = "index_company_name_age_desc"
	fields = []string{"company", "name", "age"}
	runMultiScanWithIndex(index3, fields, getScanAllNoFilter(), false, false, nil, 0, defaultlimit, true, false, "ScanAllNoFilter", t)
	runMultiScanWithIndex(index3, fields, getScanAllFilterNil(), false, false, nil, 0, defaultlimit, true, false, "ScanAllFilterNil", t)
	runMultiScanWithIndex(index3, fields, getScanAll_AllFiltersNil(), false, false, nil, 0, defaultlimit, true, false, "ScanAll_AllFiltersNil", t)

	runMultiScanWithIndex(index3, fields, get3FieldsSingleSeek(), false, false, nil, 0, defaultlimit, false, false, "3FieldsSingleSeek", t)
	runMultiScanWithIndex(index3, fields, get3FieldsMultipleSeeks(), false, false, nil, 0, defaultlimit, false, false, "3FieldsMultipleSeeks", t)
	runMultiScanWithIndex(index3, fields, get3FieldsMultipleSeeks_Identical(), false, false, nil, 0, defaultlimit, false, false, "3FieldsMultipleSeeks_Identical", t)

	log.Printf("\n\n--------- New scenarios ---------")

	index2 = "index_companyname_desc"
	fields2 := []string{"company", "name"}
	runManyMultiScanWithIndex(index2, fields2, getCompIndexHighUnbounded1(), false, false, nil, 0, defaultlimit, false, false, "CompIndexHighUnbounded1", t)
	runManyMultiScanWithIndex(index2, fields2, getCompIndexHighUnbounded2(), false, false, nil, 0, defaultlimit, false, false, "CompIndexHighUnbounded2", t)
	runManyMultiScanWithIndex(index2, fields2, getCompIndexHighUnbounded3(), false, false, nil, 0, defaultlimit, false, false, "CompIndexHighUnbounded3", t)
	runManyMultiScanWithIndex(index2, fields2, getCompIndexHighUnbounded4(), false, false, nil, 0, defaultlimit, false, false, "CompIndexHighUnbounded4", t)
	runManyMultiScanWithIndex(index2, fields2, getCompIndexHighUnbounded5(), false, false, nil, 0, defaultlimit, false, false, "CompIndexHighUnbounded5", t)
	runManyMultiScanWithIndex(index2, fields2, getSeekBoundaries(), false, false, nil, 0, defaultlimit, false, false, "SeekBoundaries", t)

	secondaryindex.DescCollation = false

}

func TestMultiScanDescCount(t *testing.T) {
	log.Printf("In TestMultiScanDescCount()")
	var index = "index_companyname_desc"
	fields := []string{"company", "name"}

	secondaryindex.CheckCollation = false
	secondaryindex.DescCollation = true

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
	var index2 = "index_company_desc"
	fields = []string{"company"}
	runMultiScanCountWithIndex(index2, fields, getSingleIndexSimpleRange(), false, false, nil, 0, defaultlimit, false, false, "SingleIndexSimpleRange", t)
	runMultiScanCountWithIndex(index2, fields, getSingleIndex_SimpleRanges_NonOverlapping(), false, false, nil, 0, defaultlimit, false, false, "SingleIndex_SimpleRanges_NonOverlapping", t)
	runMultiScanCountWithIndex(index2, fields, getSingleIndex_SimpleRanges_Overlapping(), false, false, nil, 0, defaultlimit, false, false, "SingleIndex_SimpleRanges_Overlapping", t)

	log.Printf("\n\n--------- Composite Index with 3 fields ---------")
	var index3 = "index_company_name_age_desc"
	fields = []string{"company", "name", "age"}
	runMultiScanCountWithIndex(index3, fields, getScanAllNoFilter(), false, false, nil, 0, defaultlimit, true, false, "ScanAllNoFilter", t)
	runMultiScanCountWithIndex(index3, fields, getScanAllFilterNil(), false, false, nil, 0, defaultlimit, true, false, "ScanAllFilterNil", t)
	runMultiScanCountWithIndex(index3, fields, getScanAll_AllFiltersNil(), false, false, nil, 0, defaultlimit, true, false, "ScanAll_AllFiltersNil", t)

	runMultiScanCountWithIndex(index3, fields, get3FieldsSingleSeek(), false, false, nil, 0, defaultlimit, false, false, "3FieldsSingleSeek", t)
	runMultiScanCountWithIndex(index3, fields, get3FieldsMultipleSeeks(), false, false, nil, 0, defaultlimit, false, false, "3FieldsMultipleSeeks", t)
	runMultiScanCountWithIndex(index3, fields, get3FieldsMultipleSeeks_Identical(), false, false, nil, 0, defaultlimit, false, false, "3FieldsMultipleSeeks_Identical", t)

	log.Printf("\n\n--------- New scenarios ---------")

	index2 = "index_companyname_desc"
	fields2 := []string{"company", "name"}
	runManyMultiScanCountWithIndex(index2, fields2, getCompIndexHighUnbounded1(), false, false, nil, 0, defaultlimit, false, false, "CompIndexHighUnbounded1", t)
	runManyMultiScanCountWithIndex(index2, fields2, getCompIndexHighUnbounded2(), false, false, nil, 0, defaultlimit, false, false, "CompIndexHighUnbounded2", t)
	runManyMultiScanCountWithIndex(index2, fields2, getCompIndexHighUnbounded3(), false, false, nil, 0, defaultlimit, false, false, "CompIndexHighUnbounded3", t)
	runManyMultiScanCountWithIndex(index2, fields2, getCompIndexHighUnbounded4(), false, false, nil, 0, defaultlimit, false, false, "CompIndexHighUnbounded4", t)
	runManyMultiScanCountWithIndex(index2, fields2, getCompIndexHighUnbounded5(), false, false, nil, 0, defaultlimit, false, false, "CompIndexHighUnbounded5", t)
	runManyMultiScanCountWithIndex(index2, fields2, getSeekBoundaries(), false, false, nil, 0, defaultlimit, false, false, "SeekBoundaries", t)

	log.Printf("\n\n--------- With DISTINCT True ---------")

	index = "index_companyname_desc"
	fields = []string{"company", "name"}
	runMultiScanCountWithIndex(index, fields, getScanAllNoFilter(), false, true, nil, 0, defaultlimit, true, false, "ScanAllNoFilter", t)
	runMultiScanCountWithIndex(index, fields, getScanAllFilterNil(), false, true, nil, 0, defaultlimit, true, false, "ScanAllFilterNil", t)
	runMultiScanCountWithIndex(index, fields, getScanAll_AllFiltersNil(), false, true, nil, 0, defaultlimit, true, false, "ScanAll_AllFiltersNil", t)

	runMultiScanCountWithIndex(index, fields, getSingleSeek(), false, true, nil, 0, defaultlimit, false, false, "SingleSeek", t)
	runMultiScanCountWithIndex(index, fields, getMultipleSeek(), false, true, nil, 0, defaultlimit, false, false, "MultipleSeek", t)

	runMultiScanCountWithIndex(index, fields, getSimpleRange(), false, true, nil, 0, defaultlimit, false, false, "SimpleRange", t)
	runMultiScanCountWithIndex(index, fields, getNonOverlappingRanges(), false, true, nil, 0, defaultlimit, false, false, "NonOverlappingRanges", t)
	runMultiScanCountWithIndex(index, fields, getNonOverlappingFilters2(), false, true, nil, 0, defaultlimit, false, false, "NonOverlappingFilters2", t)
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
	index2 = "index_company_desc"
	fields = []string{"company"}
	runMultiScanCountWithIndex(index2, fields, getSingleIndexSimpleRange(), false, true, nil, 0, defaultlimit, false, false, "SingleIndexSimpleRange", t)
	runMultiScanCountWithIndex(index2, fields, getSingleIndex_SimpleRanges_NonOverlapping(), false, true, nil, 0, defaultlimit, false, false, "SingleIndex_SimpleRanges_NonOverlapping", t)
	runMultiScanCountWithIndex(index2, fields, getSingleIndex_SimpleRanges_Overlapping(), false, true, nil, 0, defaultlimit, false, false, "SingleIndex_SimpleRanges_Overlapping", t)

	log.Printf("\n\n--------- Composite Index with 3 fields ---------")
	index3 = "index_company_name_age_desc"
	fields = []string{"company", "name", "age"}
	runMultiScanCountWithIndex(index3, fields, getScanAllNoFilter(), false, true, nil, 0, defaultlimit, true, false, "ScanAllNoFilter", t)
	runMultiScanCountWithIndex(index3, fields, getScanAllFilterNil(), false, true, nil, 0, defaultlimit, true, false, "ScanAllFilterNil", t)
	runMultiScanCountWithIndex(index3, fields, getScanAll_AllFiltersNil(), false, true, nil, 0, defaultlimit, true, false, "ScanAll_AllFiltersNil", t)

	runMultiScanCountWithIndex(index3, fields, get3FieldsSingleSeek(), false, true, nil, 0, defaultlimit, false, false, "3FieldsSingleSeek", t)
	runMultiScanCountWithIndex(index3, fields, get3FieldsMultipleSeeks(), false, true, nil, 0, defaultlimit, false, false, "3FieldsMultipleSeeks", t)
	runMultiScanCountWithIndex(index3, fields, get3FieldsMultipleSeeks_Identical(), false, true, nil, 0, defaultlimit, false, false, "3FieldsMultipleSeeks_Identical", t)

	secondaryindex.DescCollation = false

}

func TestMultiScanDescOffset(t *testing.T) {
	log.Printf("In SkipTestMultiScanDescOffset()")

	log.Printf("\n\n--------- Composite Index with 2 fields ---------")

	secondaryindex.CheckCollation = false
	secondaryindex.DescCollation = true

	runMultiScanDesc(getScanAllNoFilter(), false, false, nil, 8453, defaultlimit, true, true, "ScanAllNoFilter", t)
	runMultiScanDesc(getScanAllFilterNil(), false, false, nil, 1, defaultlimit, true, true, "ScanAllFilterNil", t)
	runMultiScanDesc(getScanAll_AllFiltersNil(), false, false, nil, 10002, defaultlimit, true, true, "ScanAll_AllFiltersNil", t)

	runMultiScanDesc(getSingleSeek(), false, false, nil, 1, defaultlimit, false, true, "SingleSeek", t)
	runMultiScanDesc(getMultipleSeek(), false, false, nil, 1, defaultlimit, false, true, "MultipleSeek", t)

	runMultiScanDesc(getSimpleRange(), false, false, nil, 2273, defaultlimit, false, true, "SimpleRange", t)
	runMultiScanDesc(getNonOverlappingRanges(), false, false, nil, 1111, defaultlimit, false, true, "NonOverlappingRanges", t)
	runMultiScanDesc(getOverlappingRanges(), false, false, nil, 100, defaultlimit, false, true, "OverlappingRanges", t)

	runMultiScanDesc(getNonOverlappingFilters(), false, false, nil, 340, defaultlimit, false, true, "NonOverlappingFilters", t)
	runMultiScanDesc(getOverlappingFilters(), false, false, nil, 1213, defaultlimit, false, true, "OverlappingFilters", t)
	runMultiScanDesc(getBoundaryFilters(), false, false, nil, 399, defaultlimit, false, true, "BoundaryFilters", t)

	runMultiScanDesc(getSeekAndFilters_NonOverlapping(), false, false, nil, 121, defaultlimit, false, true, "SeekAndFilters_NonOverlapping", t)
	runMultiScanDesc(getSeekAndFilters_Overlapping(), false, false, nil, 254, defaultlimit, false, true, "SeekAndFilters_Overlapping", t)

	// low-high unbounded , low>high
	runMultiScanDesc(getSimpleRangeLowUnbounded(), false, false, nil, 0, defaultlimit, false, false, "SimpleRangeLowUnbounded", t)
	runMultiScanDesc(getSimpleRangeHighUnbounded(), false, false, nil, 0, defaultlimit, false, false, "SimpleRangeHighUnbounded", t)
	runMultiScanDesc(getSimpleRangeMultipleUnbounded(), false, false, nil, 0, defaultlimit, false, false, "SimpleRangeMultipleUnbounded", t)
	runMultiScanDesc(getFiltersWithUnbounded(), false, false, nil, 0, defaultlimit, false, false, "FiltersWithUnbounded", t)
	runMultiScanDesc(getFiltersLowGreaterThanHigh(), false, false, nil, 0, defaultlimit, false, false, "FiltersLowGreaterThanHigh", t)

	log.Printf("\n\n--------- Simple Index with 1 field ---------")
	var index2 = "index_company_desc"
	fields := []string{"company"}
	runMultiScanWithIndex(index2, fields, getSingleIndexSimpleRange(), false, false, nil, 1273, defaultlimit, false, true, "SingleIndexSimpleRange", t)
	runMultiScanWithIndex(index2, fields, getSingleIndex_SimpleRanges_NonOverlapping(), false, false, nil, 140, defaultlimit, false, true, "SingleIndex_SimpleRanges_NonOverlapping", t)
	runMultiScanWithIndex(index2, fields, getSingleIndex_SimpleRanges_Overlapping(), false, false, nil, 6000, defaultlimit, false, true, "SingleIndex_SimpleRanges_Overlapping", t)

	log.Printf("\n\n--------- Composite Index with 3 fields ---------")
	var index3 = "index_company_name_age_desc"
	fields = []string{"company", "name", "age"}
	runMultiScanWithIndex(index3, fields, getScanAllNoFilter(), false, false, nil, 100000, defaultlimit, true, true, "ScanAllNoFilter", t)
	runMultiScanWithIndex(index3, fields, getScanAllFilterNil(), false, false, nil, 0, defaultlimit, true, true, "ScanAllFilterNil", t)
	runMultiScanWithIndex(index3, fields, getScanAll_AllFiltersNil(), false, false, nil, 1002, defaultlimit, true, true, "ScanAll_AllFiltersNil", t)

	runMultiScanWithIndex(index3, fields, get3FieldsSingleSeek(), false, false, nil, 0, defaultlimit, false, true, "3FieldsSingleSeek", t)
	runMultiScanWithIndex(index3, fields, get3FieldsMultipleSeeks(), false, false, nil, 1, defaultlimit, false, true, "3FieldsMultipleSeeks", t)
	runMultiScanWithIndex(index3, fields, get3FieldsMultipleSeeks_Identical(), false, false, nil, 1, defaultlimit, false, true, "3FieldsMultipleSeeks_Identical", t)

	secondaryindex.DescCollation = false

}

func TestMultiScanDescDistinct(t *testing.T) {
	log.Printf("In SkipTestMultiScanDescDistinct()")

	log.Printf("\n\n--------- Composite Index with 2 fields ---------")

	secondaryindex.CheckCollation = false
	secondaryindex.DescCollation = true

	runMultiScanDesc(getScanAllNoFilter(), false, true, nil, 0, defaultlimit, true, false, "ScanAllNoFilter", t)
	runMultiScanDesc(getScanAllFilterNil(), false, true, nil, 0, defaultlimit, true, false, "ScanAllFilterNil", t)
	runMultiScanDesc(getScanAll_AllFiltersNil(), false, true, nil, 0, defaultlimit, true, false, "ScanAll_AllFiltersNil", t)

	runMultiScanDesc(getSingleSeek(), false, true, nil, 0, defaultlimit, false, false, "SingleSeek", t)
	runMultiScanDesc(getMultipleSeek(), false, true, nil, 0, defaultlimit, false, false, "MultipleSeek", t)

	runMultiScanDesc(getSimpleRange(), false, true, nil, 0, defaultlimit, false, false, "SimpleRange", t)
	runMultiScanDesc(getNonOverlappingRanges(), false, true, nil, 0, defaultlimit, false, false, "NonOverlappingRanges", t)
	runMultiScanDesc(getOverlappingRanges(), false, true, nil, 0, defaultlimit, false, false, "OverlappingRanges", t)

	runMultiScanDesc(getNonOverlappingFilters(), false, true, nil, 0, defaultlimit, false, false, "NonOverlappingFilters", t)
	runMultiScanDesc(getOverlappingFilters(), false, true, nil, 0, defaultlimit, false, false, "OverlappingFilters", t)
	runMultiScanDesc(getBoundaryFilters(), false, true, nil, 0, defaultlimit, false, false, "BoundaryFilters", t)

	runMultiScanDesc(getSeekAndFilters_NonOverlapping(), false, true, nil, 0, defaultlimit, false, false, "SeekAndFilters_NonOverlapping", t)
	runMultiScanDesc(getSeekAndFilters_Overlapping(), false, true, nil, 0, defaultlimit, false, false, "SeekAndFilters_Overlapping", t)

	// low-high unbounded , low>high
	runMultiScanDesc(getSimpleRangeLowUnbounded(), false, false, nil, 0, defaultlimit, false, false, "SimpleRangeLowUnbounded", t)
	runMultiScanDesc(getSimpleRangeHighUnbounded(), false, false, nil, 0, defaultlimit, false, false, "SimpleRangeHighUnbounded", t)
	runMultiScanDesc(getSimpleRangeMultipleUnbounded(), false, false, nil, 0, defaultlimit, false, false, "SimpleRangeMultipleUnbounded", t)
	runMultiScanDesc(getFiltersWithUnbounded(), false, false, nil, 0, defaultlimit, false, false, "FiltersWithUnbounded", t)
	runMultiScanDesc(getFiltersLowGreaterThanHigh(), false, false, nil, 0, defaultlimit, false, false, "FiltersLowGreaterThanHigh", t)

	log.Printf("\n\n--------- Simple Index with 1 field ---------")
	var index2 = "index_company_desc"
	fields := []string{"company"}
	runMultiScanWithIndex(index2, fields, getSingleIndexSimpleRange(), false, true, nil, 0, defaultlimit, false, false, "SingleIndexSimpleRange", t)
	runMultiScanWithIndex(index2, fields, getSingleIndex_SimpleRanges_NonOverlapping(), false, true, nil, 0, defaultlimit, false, false, "SingleIndex_SimpleRanges_NonOverlapping", t)
	runMultiScanWithIndex(index2, fields, getSingleIndex_SimpleRanges_Overlapping(), false, true, nil, 0, defaultlimit, false, false, "SingleIndex_SimpleRanges_Overlapping", t)

	log.Printf("\n\n--------- Composite Index with 3 fields ---------")
	var index3 = "index_company_name_age_desc"
	fields = []string{"company", "name", "age"}
	runMultiScanWithIndex(index3, fields, getScanAllNoFilter(), false, true, nil, 0, defaultlimit, true, false, "ScanAllNoFilter", t)
	runMultiScanWithIndex(index3, fields, getScanAllFilterNil(), false, true, nil, 0, defaultlimit, true, false, "ScanAllFilterNil", t)
	runMultiScanWithIndex(index3, fields, getScanAll_AllFiltersNil(), false, true, nil, 0, defaultlimit, true, false, "ScanAll_AllFiltersNil", t)

	runMultiScanWithIndex(index3, fields, get3FieldsSingleSeek(), false, true, nil, 0, defaultlimit, false, false, "3FieldsSingleSeek", t)
	runMultiScanWithIndex(index3, fields, get3FieldsMultipleSeeks(), false, true, nil, 0, defaultlimit, false, false, "3FieldsMultipleSeeks", t)
	runMultiScanWithIndex(index3, fields, get3FieldsMultipleSeeks_Identical(), false, true, nil, 0, defaultlimit, false, false, "3FieldsMultipleSeeks_Identical", t)

	secondaryindex.DescCollation = false

}

func runMultiScanDesc(scans qc.Scans, reverse, distinct bool,
	projection *qc.IndexProjection, offset, limit int64,
	isScanAll bool, validateOnlyCount bool, scenario string, t *testing.T) {
	var indexName = "index_companyname_desc"
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
