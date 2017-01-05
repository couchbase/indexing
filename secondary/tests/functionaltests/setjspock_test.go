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
}

func TestMultiScanScenarios(t *testing.T) {
	log.Printf("In TestMultiScanScenarios()")

	log.Printf("\n\n--------- Composite Index with 2 fields ---------")

	runMultiScan(getScanAllNoFilter(), false, false, 0, defaultlimit, true, false, "ScanAllNoFilter", t)
	runMultiScan(getScanAllFilterNil(), false, false, 0, defaultlimit, true, false, "ScanAllFilterNil", t)
	runMultiScan(getScanAll_AllFiltersNil(), false, false, 0, defaultlimit, true, false, "ScanAll_AllFiltersNil", t)

	runMultiScan(getSingleSeek(), false, false, 0, defaultlimit, false, false, "SingleSeek", t)
	runMultiScan(getMultipleSeek(), false, false, 0, defaultlimit, false, false, "MultipleSeek", t)

	runMultiScan(getSimpleRange(), false, false, 0, defaultlimit, false, false, "SimpleRange", t)
	runMultiScan(getNonOverlappingRanges(), false, false, 0, defaultlimit, false, false, "NonOverlappingRanges", t)
	runMultiScan(getOverlappingRanges(), false, false, 0, defaultlimit, false, false, "OverlappingRanges", t)

	runMultiScan(getNonOverlappingFilters(), false, false, 0, defaultlimit, false, false, "NonOverlappingFilters", t)
	runMultiScan(getOverlappingFilters(), false, false, 0, defaultlimit, false, false, "OverlappingFilters", t)
	runMultiScan(getBoundaryFilters(), false, false, 0, defaultlimit, false, false, "BoundaryFilters", t)

	runMultiScan(getSeekAndFilters_NonOverlapping(), false, false, 0, defaultlimit, false, false, "SeekAndFilters_NonOverlapping", t)
	runMultiScan(getSeekAndFilters_Overlapping(), false, false, 0, defaultlimit, false, false, "SeekAndFilters_Overlapping", t)

	// low-high unbounded , low>high
	runMultiScan(getSimpleRangeLowUnbounded(), false, false, 0, defaultlimit, false, false, "SimpleRangeLowUnbounded", t)
	runMultiScan(getSimpleRangeHighUnbounded(), false, false, 0, defaultlimit, false, false, "SimpleRangeHighUnbounded", t)
	runMultiScan(getSimpleRangeMultipleUnbounded(), false, false, 0, defaultlimit, false, false, "SimpleRangeMultipleUnbounded", t)
	runMultiScan(getFiltersWithUnbounded(), false, false, 0, defaultlimit, false, false, "FiltersWithUnbounded", t)
	//runMultiScan(getFiltersLowGreaterThanHigh(), false, false, 0, defaultlimit, false, false, "FiltersLowGreaterThanHigh", t)

	log.Printf("\n\n--------- Simple Index with 1 field ---------")
	var index2 = "index_company"
	fields := []string{"company"}
	runMultiScanWithIndex(index2, fields, getSingleIndexSimpleRange(), false, false, 0, defaultlimit, false, false, "SingleIndexSimpleRange", t)
	runMultiScanWithIndex(index2, fields, getSingleIndex_SimpleRanges_NonOverlapping(), false, false, 0, defaultlimit, false, false, "SingleIndex_SimpleRanges_NonOverlapping", t)
	runMultiScanWithIndex(index2, fields, getSingleIndex_SimpleRanges_Overlapping(), false, false, 0, defaultlimit, false, false, "SingleIndex_SimpleRanges_Overlapping", t)

	log.Printf("\n\n--------- Composite Index with 3 fields ---------")
	var index3 = "index_company_name_age"
	fields = []string{"company", "name", "age"}
	runMultiScanWithIndex(index3, fields, getScanAllNoFilter(), false, false, 0, defaultlimit, true, false, "ScanAllNoFilter", t)
	runMultiScanWithIndex(index3, fields, getScanAllFilterNil(), false, false, 0, defaultlimit, true, false, "ScanAllFilterNil", t)
	runMultiScanWithIndex(index3, fields, getScanAll_AllFiltersNil(), false, false, 0, defaultlimit, true, false, "ScanAll_AllFiltersNil", t)

	runMultiScanWithIndex(index3, fields, get3FieldsSingleSeek(), false, false, 0, defaultlimit, false, false, "3FieldsSingleSeek", t)
	runMultiScanWithIndex(index3, fields, get3FieldsMultipleSeeks(), false, false, 0, defaultlimit, false, false, "3FieldsMultipleSeeks", t)
	runMultiScanWithIndex(index3, fields, get3FieldsMultipleSeeks_Identical(), false, false, 0, defaultlimit, false, false, "3FieldsMultipleSeeks_Identical", t)

}

func TestMultiScanOffset(t *testing.T) {
	log.Printf("In TestMultiScanOffset()")

	log.Printf("\n\n--------- Composite Index with 2 fields ---------")

	runMultiScan(getScanAllNoFilter(), false, false, 8453, defaultlimit, true, true, "ScanAllNoFilter", t)
	runMultiScan(getScanAllFilterNil(), false, false, 1, defaultlimit, true, true, "ScanAllFilterNil", t)
	runMultiScan(getScanAll_AllFiltersNil(), false, false, 10002, defaultlimit, true, true, "ScanAll_AllFiltersNil", t)

	runMultiScan(getSingleSeek(), false, false, 1, defaultlimit, false, true, "SingleSeek", t)
	runMultiScan(getMultipleSeek(), false, false, 1, defaultlimit, false, true, "MultipleSeek", t)

	runMultiScan(getSimpleRange(), false, false, 2273, defaultlimit, false, true, "SimpleRange", t)
	runMultiScan(getNonOverlappingRanges(), false, false, 1111, defaultlimit, false, true, "NonOverlappingRanges", t)
	runMultiScan(getOverlappingRanges(), false, false, 100, defaultlimit, false, true, "OverlappingRanges", t)

	runMultiScan(getNonOverlappingFilters(), false, false, 340, defaultlimit, false, true, "NonOverlappingFilters", t)
	runMultiScan(getOverlappingFilters(), false, false, 1213, defaultlimit, false, true, "OverlappingFilters", t)
	runMultiScan(getBoundaryFilters(), false, false, 399, defaultlimit, false, true, "BoundaryFilters", t)

	runMultiScan(getSeekAndFilters_NonOverlapping(), false, false, 121, defaultlimit, false, true, "SeekAndFilters_NonOverlapping", t)
	runMultiScan(getSeekAndFilters_Overlapping(), false, false, 254, defaultlimit, false, true, "SeekAndFilters_Overlapping", t)

	// low-high unbounded , low>high
	runMultiScan(getSimpleRangeLowUnbounded(), false, false, 0, defaultlimit, false, false, "SimpleRangeLowUnbounded", t)
	runMultiScan(getSimpleRangeHighUnbounded(), false, false, 0, defaultlimit, false, false, "SimpleRangeHighUnbounded", t)
	runMultiScan(getSimpleRangeMultipleUnbounded(), false, false, 0, defaultlimit, false, false, "SimpleRangeMultipleUnbounded", t)
	runMultiScan(getFiltersWithUnbounded(), false, false, 0, defaultlimit, false, false, "FiltersWithUnbounded", t)
	//runMultiScan(getFiltersLowGreaterThanHigh(), false, false, 0, defaultlimit, false, false, "FiltersLowGreaterThanHigh", t)

	log.Printf("\n\n--------- Simple Index with 1 field ---------")
	var index2 = "index_company"
	fields := []string{"company"}
	runMultiScanWithIndex(index2, fields, getSingleIndexSimpleRange(), false, false, 1273, defaultlimit, false, true, "SingleIndexSimpleRange", t)
	runMultiScanWithIndex(index2, fields, getSingleIndex_SimpleRanges_NonOverlapping(), false, false, 140, defaultlimit, false, true, "SingleIndex_SimpleRanges_NonOverlapping", t)
	runMultiScanWithIndex(index2, fields, getSingleIndex_SimpleRanges_Overlapping(), false, false, 6000, defaultlimit, false, true, "SingleIndex_SimpleRanges_Overlapping", t)

	log.Printf("\n\n--------- Composite Index with 3 fields ---------")
	var index3 = "index_company_name_age"
	fields = []string{"company", "name", "age"}
	runMultiScanWithIndex(index3, fields, getScanAllNoFilter(), false, false, 100000, defaultlimit, true, true, "ScanAllNoFilter", t)
	runMultiScanWithIndex(index3, fields, getScanAllFilterNil(), false, false, 0, defaultlimit, true, true, "ScanAllFilterNil", t)
	runMultiScanWithIndex(index3, fields, getScanAll_AllFiltersNil(), false, false, 1002, defaultlimit, true, true, "ScanAll_AllFiltersNil", t)

	runMultiScanWithIndex(index3, fields, get3FieldsSingleSeek(), false, false, 0, defaultlimit, false, true, "3FieldsSingleSeek", t)
	runMultiScanWithIndex(index3, fields, get3FieldsMultipleSeeks(), false, false, 1, defaultlimit, false, true, "3FieldsMultipleSeeks", t)
	runMultiScanWithIndex(index3, fields, get3FieldsMultipleSeeks_Identical(), false, false, 1, defaultlimit, false, true, "3FieldsMultipleSeeks_Identical", t)
}

func TestMultiScanPrimaryIndex(t *testing.T) {
	log.Printf("In TestMultiScanPrimaryIndex()")

	var index4 = "index_primary"
	runMultiScanForPrimaryIndex(index4, getPrimaryRange(), false, false, 0, defaultlimit, true, false, "PrimaryRange", t)
	runMultiScanForPrimaryIndex(index4, getScanAllNoFilter(), false, false, 0, defaultlimit, true, false, "PrimaryScanAllNoFilter", t)
}

func SkipTestScansDistinct(t *testing.T) {
	log.Printf("In TestScansDistinct()")

	log.Printf("\n\n--------- Composite Index with 2 fields ---------")

	runMultiScan(getScanAllNoFilter(), false, true, 0, defaultlimit, true, false, "ScanAllNoFilter", t)
	runMultiScan(getScanAllFilterNil(), false, true, 0, defaultlimit, true, false, "ScanAllFilterNil", t)
	runMultiScan(getScanAll_AllFiltersNil(), false, true, 0, defaultlimit, true, false, "ScanAll_AllFiltersNil", t)

	runMultiScan(getSingleSeek(), false, true, 0, defaultlimit, false, false, "SingleSeek", t)
	runMultiScan(getMultipleSeek(), false, true, 0, defaultlimit, false, false, "MultipleSeek", t)

	runMultiScan(getSimpleRange(), false, true, 0, defaultlimit, false, false, "SimpleRange", t)
	runMultiScan(getNonOverlappingRanges(), false, true, 0, defaultlimit, false, false, "NonOverlappingRanges", t)
	runMultiScan(getOverlappingRanges(), false, true, 0, defaultlimit, false, false, "OverlappingRanges", t)

	runMultiScan(getNonOverlappingFilters(), false, true, 0, defaultlimit, false, false, "NonOverlappingFilters", t)
	runMultiScan(getOverlappingFilters(), false, true, 0, defaultlimit, false, false, "OverlappingFilters", t)
	runMultiScan(getBoundaryFilters(), false, true, 0, defaultlimit, false, false, "BoundaryFilters", t)

	runMultiScan(getSeekAndFilters_NonOverlapping(), false, true, 0, defaultlimit, false, false, "SeekAndFilters_NonOverlapping", t)
	runMultiScan(getSeekAndFilters_Overlapping(), false, true, 0, defaultlimit, false, false, "SeekAndFilters_Overlapping", t)

	// low-high unbounded , low>high
	runMultiScan(getSimpleRangeLowUnbounded(), false, false, 0, defaultlimit, false, false, "SimpleRangeLowUnbounded", t)
	runMultiScan(getSimpleRangeHighUnbounded(), false, false, 0, defaultlimit, false, false, "SimpleRangeHighUnbounded", t)
	runMultiScan(getSimpleRangeMultipleUnbounded(), false, false, 0, defaultlimit, false, false, "SimpleRangeMultipleUnbounded", t)
	runMultiScan(getFiltersWithUnbounded(), false, false, 0, defaultlimit, false, false, "FiltersWithUnbounded", t)
	//runMultiScan(getFiltersLowGreaterThanHigh(), false, false, 0, defaultlimit, false, false, "FiltersLowGreaterThanHigh", t)

	log.Printf("\n\n--------- Simple Index with 1 field ---------")
	var index2 = "index_company"
	fields := []string{"company"}
	runMultiScanWithIndex(index2, fields, getSingleIndexSimpleRange(), false, true, 0, defaultlimit, false, false, "SingleIndexSimpleRange", t)
	runMultiScanWithIndex(index2, fields, getSingleIndex_SimpleRanges_NonOverlapping(), false, true, 0, defaultlimit, false, false, "SingleIndex_SimpleRanges_NonOverlapping", t)
	runMultiScanWithIndex(index2, fields, getSingleIndex_SimpleRanges_Overlapping(), false, true, 0, defaultlimit, false, false, "SingleIndex_SimpleRanges_Overlapping", t)

	log.Printf("\n\n--------- Composite Index with 3 fields ---------")
	var index3 = "index_company_name_age"
	fields = []string{"company", "name", "age"}
	runMultiScanWithIndex(index3, fields, getScanAllNoFilter(), false, true, 0, defaultlimit, true, false, "ScanAllNoFilter", t)
	runMultiScanWithIndex(index3, fields, getScanAllFilterNil(), false, true, 0, defaultlimit, true, false, "ScanAllFilterNil", t)
	runMultiScanWithIndex(index3, fields, getScanAll_AllFiltersNil(), false, true, 0, defaultlimit, true, false, "ScanAll_AllFiltersNil", t)

	runMultiScanWithIndex(index3, fields, get3FieldsSingleSeek(), false, true, 0, defaultlimit, false, false, "3FieldsSingleSeek", t)
	runMultiScanWithIndex(index3, fields, get3FieldsMultipleSeeks(), false, true, 0, defaultlimit, false, false, "3FieldsMultipleSeeks", t)
	runMultiScanWithIndex(index3, fields, get3FieldsMultipleSeeks_Identical(), false, true, 0, defaultlimit, false, false, "3FieldsMultipleSeeks_Identical", t)
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

func runMultiScan(scans qc.Scans, reverse, distinct bool, offset, limit int64,
	isScanAll bool, validateOnlyCount bool, scenario string, t *testing.T) {
	var indexName = "index_companyname"
	var bucketName = "default"
	log.Printf("\n--- %v ---", scenario)

	docScanResults := datautility.ExpectedMultiScanResponse(docs, []string{"company", "name"}, scans, reverse, distinct, offset, limit, isScanAll)
	scanResults, err := secondaryindex.Scans(indexName, bucketName, "127.0.0.1:9000", scans, reverse, distinct, offset, limit, c.SessionConsistency, nil)
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
	reverse, distinct bool, offset, limit int64,
	isScanAll bool, validateOnlyCount bool, scenario string, t *testing.T) {
	var bucketName = "default"
	log.Printf("\n--- %v ---", scenario)

	docScanResults := datautility.ExpectedMultiScanResponse(docs, fields, scans, reverse, distinct, offset, limit, isScanAll)
	scanResults, err := secondaryindex.Scans(indexName, bucketName, "127.0.0.1:9000", scans, reverse, distinct, offset, limit, c.SessionConsistency, nil)
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

func runMultiScanForPrimaryIndex(indexName string, scans qc.Scans,
	reverse, distinct bool, offset, limit int64,
	isScanAll bool, validateOnlyCount bool, scenario string, t *testing.T) {
	var bucketName = "default"
	log.Printf("\n--- %v ---", scenario)

	docScanResults := datautility.ExpectedMultiScanResponse_Primary(docs, scans, reverse, distinct, offset, limit)
	scanResults, err := secondaryindex.Scans(indexName, bucketName, "127.0.0.1:9000", scans, reverse, distinct, offset, limit, c.SessionConsistency, nil)
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
