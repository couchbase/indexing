package functionaltests

import (
	c "github.com/couchbase/indexing/secondary/common"
	qc "github.com/couchbase/indexing/secondary/queryport/client"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"gopkg.in/couchbase/gocb.v1"

	//"github.com/couchbase/indexing/secondary/tests/framework/datautility"
	"log"
	"testing"
	"time"

	"github.com/couchbase/query/value"

	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	tv "github.com/couchbase/indexing/secondary/tests/framework/validation"
)

func TestGroupAggrSetup(t *testing.T) {
	log.Printf("In TestGroupAggrSetup()")

	tmpclient = secondaryindex.UseClient
	secondaryindex.UseClient = "gsi"

	var index = "index_agg"
	var bucket = "default"

	log.Printf("Emptying the default bucket")
	kvutility.EnableBucketFlush("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.FlushBucket("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)

	secondaryindex.DropSecondaryIndex(index, bucket, indexManagementAddress)

	// Populate the bucket now
	log.Printf("Populating the default bucket")
	docs := makeGroupAggDocs()
	kvutility.SetKeyValues(docs, "default", "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndex(index, bucket, indexManagementAddress, "", []string{"Year", "Month", "Sale"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	n1qlstatement := "create primary index on default"
	_, err = tc.ExecuteN1QLStatement(kvaddress, clusterconfig.Username, clusterconfig.Password, bucket, n1qlstatement, false, gocb.NotBounded)
	FailTestIfError(err, "Error in creating primary index", t)
}

type Aggdoc struct {
	Year  string
	Month interface{}
	Sale  interface{}
}

type Aggdoc1 struct {
	Year string
	Sale interface{}
}

type Aggdoc2 struct {
	Year  string
	Month interface{}
}

type Aggdoc3 struct {
	Year string
}

func makeGroupAggDocs() tc.KeyValues {

	docs := make(tc.KeyValues)

	docs["doc1"] = Aggdoc{Year: "2016", Month: 1, Sale: 10}
	docs["doc2"] = Aggdoc{Year: "2016", Month: 1, Sale: 20}
	docs["doc3"] = Aggdoc{Year: "2016", Month: 2, Sale: 30}
	docs["doc4"] = Aggdoc{Year: "2016", Month: 2, Sale: 40}
	docs["doc5"] = Aggdoc{Year: "2016", Month: 3, Sale: 50}
	docs["doc6"] = Aggdoc{Year: "2016", Month: 3, Sale: 60}
	docs["doc7"] = Aggdoc{Year: "2017", Month: 1, Sale: 10}
	docs["doc8"] = Aggdoc{Year: "2017", Month: 1, Sale: 20}
	docs["doc9"] = Aggdoc{Year: "2017", Month: 2, Sale: 30}
	docs["doc10"] = Aggdoc{Year: "2017", Month: 2, Sale: 40}
	docs["doc11"] = Aggdoc{Year: "2017", Month: 3, Sale: 50}
	docs["doc12"] = Aggdoc{Year: "2017", Month: 3, Sale: 60}

	docs["doc13"] = Aggdoc{Year: "2017", Month: 4, Sale: "salestr"}
	docs["doc14"] = Aggdoc{Year: "2017", Month: 4, Sale: nil}
	docs["doc15"] = Aggdoc{Year: "2017", Month: 4, Sale: 10}
	docs["doc32"] = Aggdoc{Year: "2017", Month: 4, Sale: true}
	docs["doc33"] = Aggdoc{Year: "2017", Month: 4, Sale: false}
	docs["doc34"] = Aggdoc{Year: "2017", Month: 4, Sale: []int{1, 2, 3}}
	docs["doc35"] = Aggdoc{Year: "2017", Month: 4, Sale: Aggdoc3{Year: "2019"}}
	docs["doc36"] = Aggdoc2{Year: "2017", Month: 4}
	docs["doc37"] = Aggdoc{Year: "2017", Month: 4, Sale: 5.5}

	docs["doc16"] = Aggdoc{Year: "2019", Month: nil, Sale: 10}
	docs["doc17"] = Aggdoc{Year: "2019", Month: 0, Sale: 10}
	docs["doc18"] = Aggdoc1{Year: "2019", Sale: 10}
	docs["doc19"] = Aggdoc{Year: "2019", Month: "strmonth", Sale: 10}
	docs["doc20"] = Aggdoc{Year: "2019", Month: true, Sale: 10}
	docs["doc21"] = Aggdoc{Year: "2019", Month: false, Sale: 10}
	docs["doc22"] = Aggdoc{Year: "2019", Month: []int{1, 2, 3}, Sale: 10}
	docs["doc23"] = Aggdoc{Year: "2019", Month: Aggdoc3{Year: "2019"}, Sale: 10}
	docs["doc24"] = Aggdoc{Year: "2019", Month: nil, Sale: 5}
	docs["doc25"] = Aggdoc{Year: "2019", Month: 0, Sale: 5}
	docs["doc26"] = Aggdoc1{Year: "2019", Sale: 5}
	docs["doc27"] = Aggdoc{Year: "2019", Month: "strmonth", Sale: 5}
	docs["doc28"] = Aggdoc{Year: "2019", Month: true, Sale: 5}
	docs["doc29"] = Aggdoc{Year: "2019", Month: false, Sale: 5}
	docs["doc30"] = Aggdoc{Year: "2019", Month: []int{1, 2, 3}, Sale: 5}
	docs["doc31"] = Aggdoc{Year: "2019", Month: Aggdoc3{Year: "2019"}, Sale: 5}

	docs["doc38"] = Aggdoc{Year: "2020", Month: 4, Sale: 0}
	docs["doc39"] = Aggdoc{Year: "2020", Month: 4, Sale: 0}
	docs["doc40"] = Aggdoc{Year: "2020", Month: 4, Sale: -20}
	docs["doc41"] = Aggdoc{Year: "2020", Month: 4, Sale: 0}
	docs["doc42"] = Aggdoc{Year: "2020", Month: 4, Sale: -20}
	docs["doc43"] = Aggdoc{Year: "2020", Month: 4, Sale: "str"}
	docs["doc44"] = Aggdoc{Year: "2020", Month: 5}
	docs["doc45"] = Aggdoc2{Year: "2020", Month: 5}

	return docs

}

func basicGroupAggr() (*qc.GroupAggr, *qc.IndexProjection) {

	groups := make([]*qc.GroupKey, 2)

	g1 := &qc.GroupKey{
		EntryKeyId: 3,
		KeyPos:     0,
	}

	g2 := &qc.GroupKey{
		EntryKeyId: 4,
		KeyPos:     1,
	}
	groups[0] = g1
	groups[1] = g2

	//Aggrs
	aggregates := make([]*qc.Aggregate, 2)
	a1 := &qc.Aggregate{
		AggrFunc:   c.AGG_SUM,
		EntryKeyId: 5,
		KeyPos:     2,
	}

	a2 := &qc.Aggregate{
		AggrFunc:   c.AGG_COUNT,
		EntryKeyId: 6,
		KeyPos:     2,
	}
	aggregates[0] = a1
	aggregates[1] = a2

	dependsOnIndexKeys := make([]int32, 1)
	dependsOnIndexKeys[0] = int32(0)

	ga := &qc.GroupAggr{
		Name:               "testGrpAggr2",
		Group:              groups,
		Aggrs:              aggregates,
		DependsOnIndexKeys: dependsOnIndexKeys,
	}

	entry := make([]int64, 4)
	entry[0] = 3
	entry[1] = 4
	entry[2] = 5
	entry[3] = 6

	proj := &qc.IndexProjection{
		EntryKeys: entry,
	}

	return ga, proj
}

func TestGroupAggrLeading(t *testing.T) {
	log.Printf("In TestGroupAggrLeading()")

	var index1 = "index_agg"
	var bucketName = "default"

	ga, proj := basicGroupAggr()

	expectedResults := make(tc.GroupAggrScanResponse, 7)
	expectedResults[0] = []interface{}{"2016", int64(1), int64(30), int64(2)}
	expectedResults[1] = []interface{}{"2016", int64(2), int64(70), int64(2)}
	expectedResults[2] = []interface{}{"2016", int64(3), int64(110), int64(2)}
	expectedResults[3] = []interface{}{"2017", int64(1), int64(30), int64(2)}
	expectedResults[4] = []interface{}{"2017", int64(2), int64(70), int64(2)}
	expectedResults[5] = []interface{}{"2017", int64(3), int64(110), int64(2)}
	expectedResults[6] = []interface{}{"2017", int64(4), float64(15.5), int64(7)}

	_, scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getPartialMatchFilter1(), false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.ValidateGroupAggrResult(expectedResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	expectedResults = expectedResults[:3]
	expectedResults[0] = []interface{}{"2016", int64(1), int64(30), int64(2)}
	expectedResults[1] = []interface{}{"2017", int64(2), int64(70), int64(2)}
	expectedResults[2] = []interface{}{"2017", int64(3), int64(110), int64(2)}

	_, scanResults, err = secondaryindex.Scan3(index1, bucketName, indexScanAddress, getNonOverlappingFilters3(), false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.ValidateGroupAggrResult(expectedResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestGroupAggrNonLeading(t *testing.T) {
	log.Printf("In TestGroupAggrNonLeading()")

	var index1 = "index_agg"
	var bucketName = "default"

	ga, proj := basicGroupAggr()

	ga.Group = ga.Group[1:]
	proj.EntryKeys = proj.EntryKeys[1:]

	ga.AllowPartialAggr = true

	expectedResults := make(tc.GroupAggrScanResponse, 4)
	expectedResults[0] = []interface{}{int64(1), int64(60), int64(4)}
	expectedResults[1] = []interface{}{int64(2), int64(140), int64(4)}
	expectedResults[2] = []interface{}{int64(3), int64(220), int64(4)}
	expectedResults[3] = []interface{}{int64(4), float64(15.5), int64(7)}

	_, scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getPartialMatchFilter1(), false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.ValidateGroupAggrResult(expectedResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestGroupAggrNoGroup(t *testing.T) {
	log.Printf("In TestGroupAggrNoGroup()")

	var index1 = "index_agg"
	var bucketName = "default"

	ga, proj := basicGroupAggr()
	ga.Group = nil
	proj.EntryKeys = proj.EntryKeys[2:]

	expectedResults := make(tc.GroupAggrScanResponse, 1)
	expectedResults[0] = []interface{}{515.5, int64(41)}

	_, scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getScanAllNoFilter(), false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.ValidateGroupAggrResult(expectedResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestGroupAggrMinMax(t *testing.T) {
	log.Printf("In TestGroupAggrMinMax()")

	var index1 = "index_agg"
	var bucketName = "default"

	ga, proj := basicGroupAggr()

	ga.Aggrs[0].AggrFunc = c.AGG_MIN
	ga.Aggrs[1].AggrFunc = c.AGG_MAX

	expectedResults := make(tc.GroupAggrScanResponse, 4)
	expectedResults[0] = []interface{}{"2017", int64(1), int64(10), int64(20)}
	expectedResults[1] = []interface{}{"2017", int64(2), int64(30), int64(40)}
	expectedResults[2] = []interface{}{"2017", int64(3), int64(50), int64(60)}
	expectedResults[3] = []interface{}{"2017", int64(4), false, map[string]interface{}{"Year": "2019"}}

	_, scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getPartialMatchFilter2(), false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)

	err = tv.ValidateGroupAggrResult(expectedResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestGroupAggrMinMax2(t *testing.T) {
	log.Printf("In TestGroupAggrMinMax()")

	var index1 = "index_agg"
	var bucketName = "default"

	ga, proj := basicGroupAggr()
	ga.Group = nil
	proj.EntryKeys = proj.EntryKeys[2:]

	ga.Aggrs[0].AggrFunc = c.AGG_MIN
	ga.Aggrs[1].AggrFunc = c.AGG_MAX

	expectedResults := make(tc.GroupAggrScanResponse, 1)
	expectedResults[0] = []interface{}{false, map[string]interface{}{"Year": "2019"}}

	_, scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getScanAllNoFilter(), false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.ValidateGroupAggrResult(expectedResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestGroupAggrLeading_N1QLExprs(t *testing.T) {
	log.Printf("In TestGroupAggrLeading_N1QLExprs()")

	var index1 = "index_agg"
	var bucketName = "default"

	ga, proj, n1qlEquivalent := basicGroupAggrN1QLExprs1()

	_, scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getScanAllNoFilter(), false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.ValidateGroupAggrWithN1QL(kvaddress, clusterconfig.Username,
		clusterconfig.Password, bucketName, n1qlEquivalent, ga, proj, scanResults)
	if err == nil {
		log.Printf("basicGroupAggrN1QLExprs1: Scan validation passed")
	}
	FailTestIfError(err, "Error in scan result validation", t)

	ga, proj, n1qlEquivalent = basicGroupAggrN1QLExprs2()
	_, scanResults, err = secondaryindex.Scan3(index1, bucketName, indexScanAddress, getScanAllNoFilter(), false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.ValidateGroupAggrWithN1QL(kvaddress, clusterconfig.Username,
		clusterconfig.Password, bucketName, n1qlEquivalent, ga, proj, scanResults)
	if err == nil {
		log.Printf("basicGroupAggrN1QLExprs2: Scan validation passed")
	}
	FailTestIfError(err, "Error in scan result validation", t)
}

func basicGroupAggrN1QLExprs1() (*qc.GroupAggr, *qc.IndexProjection, string) {

	n1qlEquivalent := "SELECT 'Year' || Year as a, Month as b, SUM(Sale * 2) as c, " +
		"COUNT(Sale) as d from default USE INDEX(`#primary`) GROUP BY 'Year' || Year, Month"
	g1 := &qc.GroupKey{EntryKeyId: 3, KeyPos: -1, Expr: "\"Year\" || cover ((`default`.`Year`))"}
	g2 := &qc.GroupKey{EntryKeyId: 4, KeyPos: -1, Expr: "cover ((`default`.`Month`))"}
	groups := []*qc.GroupKey{g1, g2}

	a1 := &qc.Aggregate{AggrFunc: c.AGG_SUM, EntryKeyId: 5, KeyPos: -1,
		Expr: "cover ((`default`.`Sale`)) * 2"}

	a2 := &qc.Aggregate{AggrFunc: c.AGG_COUNT, EntryKeyId: 6, KeyPos: 2, Expr: ""}
	aggregates := []*qc.Aggregate{a1, a2}

	ga := &qc.GroupAggr{
		Name:               "testGrpAggr1",
		Group:              groups,
		Aggrs:              aggregates,
		DependsOnIndexKeys: []int32{0, 1, 2, 3},
		IndexKeyNames: []string{
			"(`default`.`Year`)",
			"(`default`.`Month`)",
			"(`default`.`Sale`)",
			"(meta(`default`).`id`)"},
		AllowPartialAggr: true,
	}

	proj := &qc.IndexProjection{
		EntryKeys: []int64{3, 4, 5, 6},
	}

	return ga, proj, n1qlEquivalent
}

func basicGroupAggrN1QLExprs2() (*qc.GroupAggr, *qc.IndexProjection, string) {

	n1qlEquivalent := "SELECT SUBSTR(meta().id, 0, 4) as a, SUM(Sale) as b, COUNT(Sale) as c, " +
		"SUM(10+5) as d from default USE INDEX(`#primary`) GROUP BY SUBSTR(meta().id, 0, 4)"
	g1 := &qc.GroupKey{EntryKeyId: 3, KeyPos: -1, Expr: "SUBSTR(cover ((meta(`default`).`id`)), 0, 4)"}
	groups := []*qc.GroupKey{g1}

	a1 := &qc.Aggregate{AggrFunc: c.AGG_SUM, EntryKeyId: 4, KeyPos: -1,
		Expr: "cover ((`default`.`Sale`))"}

	a2 := &qc.Aggregate{AggrFunc: c.AGG_COUNT, EntryKeyId: 5, KeyPos: 2, Expr: ""}

	a3 := &qc.Aggregate{AggrFunc: c.AGG_SUM, EntryKeyId: 6, KeyPos: -1, Expr: "10 + 5"} // Const Expr
	aggregates := []*qc.Aggregate{a1, a2, a3}

	ga := &qc.GroupAggr{
		Name:               "testGrpAggr2",
		Group:              groups,
		Aggrs:              aggregates,
		DependsOnIndexKeys: []int32{0, 1, 2, 3},
		IndexKeyNames: []string{
			"(`default`.`Year`)",
			"(`default`.`Month`)",
			"(`default`.`Sale`)",
			"(meta(`default`).`id`)"},
		AllowPartialAggr: true,
	}

	proj := &qc.IndexProjection{
		EntryKeys: []int64{3, 4, 5, 6},
	}

	return ga, proj, n1qlEquivalent
}

func getNonOverlappingFilters3() qc.Scans {
	scans := make(qc.Scans, 2)

	filter1 := make([]*qc.CompositeElementFilter, 2)
	filter1[0] = &qc.CompositeElementFilter{Low: "2016", High: "2016", Inclusion: qc.Inclusion(uint32(3))}
	filter1[1] = &qc.CompositeElementFilter{Low: int64(1), High: int64(1), Inclusion: qc.Inclusion(uint32(3))}
	scans[0] = &qc.Scan{Filter: filter1}

	filter2 := make([]*qc.CompositeElementFilter, 2)
	filter2[0] = &qc.CompositeElementFilter{Low: "2017", High: "2017", Inclusion: qc.Inclusion(uint32(3))}
	filter2[1] = &qc.CompositeElementFilter{Low: int64(2), High: int64(3), Inclusion: qc.Inclusion(uint32(3))}
	scans[1] = &qc.Scan{Filter: filter2}
	return scans
}

func TestGroupAggrLimit(t *testing.T) {

	var index1 = "index_agg"
	var bucketName = "default"

	log.Printf("In TestGroupAggrLimit()")
	//leading
	{
		ga, proj := basicGroupAggr()

		expectedResults := make(tc.GroupAggrScanResponse, 3)
		expectedResults[0] = []interface{}{"2016", int64(1), int64(30), int64(2)}
		expectedResults[1] = []interface{}{"2016", int64(2), int64(70), int64(2)}
		expectedResults[2] = []interface{}{"2016", int64(3), int64(110), int64(2)}

		_, scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getScanAllNoFilter(), false, false, proj, 0, 3, ga, c.SessionConsistency, nil)
		FailTestIfError(err, "Error in scan", t)
		err = tv.ValidateGroupAggrResult(expectedResults, scanResults)
		FailTestIfError(err, "Error in scan result validation", t)
	}

	//non-leading
	{
		ga, proj := basicGroupAggr()

		ga.Group = ga.Group[1:]
		proj.EntryKeys = proj.EntryKeys[1:]
		ga.AllowPartialAggr = true

		expectedResults := make(tc.GroupAggrScanResponse, 2)
		expectedResults[0] = []interface{}{int64(1), int64(60), int64(4)}
		expectedResults[1] = []interface{}{int64(2), int64(140), int64(4)}

		_, scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getScanAllNoFilter(), false, false, proj, 0, 2, ga, c.SessionConsistency, nil)
		FailTestIfError(err, "Error in scan", t)
		err = tv.ValidateGroupAggrResult(expectedResults, scanResults)
		FailTestIfError(err, "Error in scan result validation", t)
	}

}

func TestGroupAggrOffset(t *testing.T) {
	log.Printf("In TestGroupAggrOffset()")

	var index1 = "index_agg"
	var bucketName = "default"

	//leading
	{
		ga, proj := basicGroupAggr()

		expectedResults := make(tc.GroupAggrScanResponse, 3)
		expectedResults[0] = []interface{}{"2016", int64(2), int64(70), int64(2)}
		expectedResults[1] = []interface{}{"2016", int64(3), int64(110), int64(2)}
		expectedResults[2] = []interface{}{"2017", int64(1), int64(30), int64(2)}

		_, scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getScanAllNoFilter(), false, false, proj, 1, 3, ga, c.SessionConsistency, nil)
		FailTestIfError(err, "Error in scan", t)
		err = tv.ValidateGroupAggrResult(expectedResults, scanResults)
		FailTestIfError(err, "Error in scan result validation", t)

	}

	//non-leading
	{
		ga, proj := basicGroupAggr()

		ga.Group = ga.Group[1:]
		proj.EntryKeys = proj.EntryKeys[1:]
		ga.AllowPartialAggr = true

		expectedResults := make(tc.GroupAggrScanResponse, 2)
		expectedResults[0] = []interface{}{int64(2), int64(140), int64(4)}
		expectedResults[1] = []interface{}{int64(3), int64(220), int64(4)}

		_, scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getScanAllNoFilter(), false, false, proj, 1, 2, ga, c.SessionConsistency, nil)
		FailTestIfError(err, "Error in scan", t)
		err = tv.ValidateGroupAggrResult(expectedResults, scanResults)
		FailTestIfError(err, "Error in scan result validation", t)
	}

}

func TestGroupAggrCountN(t *testing.T) {
	log.Printf("In TestGroupAggrCountN()")

	var index1 = "index_agg"
	var bucketName = "default"

	ga, proj := basicGroupAggr()

	expectedResults := make(tc.GroupAggrScanResponse, 4)
	expectedResults[0] = []interface{}{"2017", int64(1), int64(30), int64(2)}
	expectedResults[1] = []interface{}{"2017", int64(2), int64(70), int64(2)}
	expectedResults[2] = []interface{}{"2017", int64(3), int64(110), int64(2)}
	expectedResults[3] = []interface{}{"2017", int64(4), float64(15.5), int64(7)}

	_, scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getPartialMatchFilter2(), false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)

	err = tv.ValidateGroupAggrResult(expectedResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	a2 := &qc.Aggregate{
		AggrFunc:   c.AGG_COUNTN,
		EntryKeyId: 6,
		KeyPos:     2,
	}

	ga.Aggrs[1] = a2

	expectedResults[0] = []interface{}{"2017", int64(1), int64(30), int64(2)}
	expectedResults[1] = []interface{}{"2017", int64(2), int64(70), int64(2)}
	expectedResults[2] = []interface{}{"2017", int64(3), int64(110), int64(2)}
	expectedResults[3] = []interface{}{"2017", int64(4), float64(15.5), int64(2)}

	_, scanResults, err = secondaryindex.Scan3(index1, bucketName, indexScanAddress, getPartialMatchFilter2(), false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)

	err = tv.ValidateGroupAggrResult(expectedResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestGroupAggrNoGroupNoMatch(t *testing.T) {
	log.Printf("In TestGroupAggrNoGroupNoMatch()")

	var index1 = "index_agg"
	var bucketName = "default"

	ga, proj := basicGroupAggr()
	ga.Group = nil
	proj.EntryKeys = proj.EntryKeys[2:]

	expectedResults := make(tc.GroupAggrScanResponse, 1)
	expectedResults[0] = []interface{}{nil, int64(0)}

	_, scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getNoMatchFilter(), false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)

	err = tv.ValidateGroupAggrResult(expectedResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func TestGroupAggrGroupNoMatch(t *testing.T) {
	log.Printf("In TestGroupAggrGroupNoMatch()")

	var index1 = "index_agg"
	var bucketName = "default"

	ga, proj := basicGroupAggr()

	_, scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getNoMatchFilter(), false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)

	err = tv.ValidateGroupAggrResult(nil, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}

func getNoMatchFilter() qc.Scans {
	scans := make(qc.Scans, 1)

	filter1 := make([]*qc.CompositeElementFilter, 2)
	filter1[0] = &qc.CompositeElementFilter{Low: "2018", High: "2018", Inclusion: qc.Inclusion(uint32(3))}
	filter1[1] = &qc.CompositeElementFilter{Low: int64(1), High: int64(1), Inclusion: qc.Inclusion(uint32(3))}
	scans[0] = &qc.Scan{Filter: filter1}

	return scans
}

func TestGroupAggrMultDataTypes(t *testing.T) {
	log.Printf("In TestGroupAggrMultDataTypes()")

	var index1 = "index_agg"
	var bucketName = "default"

	ga, proj := basicGroupAggr()

	expectedResults := make(tc.GroupAggrScanResponse, 8)
	expectedResults[0] = []interface{}{"2019", value.MISSING_VALUE, int64(15), int64(2)}
	expectedResults[1] = []interface{}{"2019", nil, int64(15), int64(2)}
	expectedResults[2] = []interface{}{"2019", false, int64(15), int64(2)}
	expectedResults[3] = []interface{}{"2019", true, int64(15), int64(2)}
	expectedResults[4] = []interface{}{"2019", int64(0), int64(15), int64(2)}
	expectedResults[5] = []interface{}{"2019", "strmonth", int64(15), int64(2)}
	expectedResults[6] = []interface{}{"2019", []interface{}{int64(1), int64(2), int64(3)}, int64(15), int64(2)}
	expectedResults[7] = []interface{}{"2019", map[string]interface{}{"Year": "2019"}, int64(15), int64(2)}

	_, scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getPartialMatchFilter(), false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)

	err = tv.ValidateGroupAggrResult(expectedResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

}

func getPartialMatchFilter() qc.Scans {
	scans := make(qc.Scans, 1)

	filter1 := make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "2019", High: "2019", Inclusion: qc.Inclusion(uint32(3))}
	scans[0] = &qc.Scan{Filter: filter1}

	return scans
}

func getPartialMatchFilter1() qc.Scans {
	scans := make(qc.Scans, 1)

	filter1 := make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "2016", High: "2017", Inclusion: qc.Inclusion(uint32(3))}
	scans[0] = &qc.Scan{Filter: filter1}

	return scans
}

func getPartialMatchFilter2() qc.Scans {
	scans := make(qc.Scans, 1)

	filter1 := make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "2017", High: "2017", Inclusion: qc.Inclusion(uint32(3))}
	scans[0] = &qc.Scan{Filter: filter1}

	return scans
}

func TestGroupAggrDistinct(t *testing.T) {
	log.Printf("In TestGroupAggrDistinct()")

	var index1 = "index_agg"
	var bucketName = "default"

	//sum/count
	{
		ga, proj := basicGroupAggr()

		ga.Aggrs[0].Distinct = true
		ga.Aggrs[1].Distinct = true

		expectedResults := make(tc.GroupAggrScanResponse, 2)
		expectedResults[0] = []interface{}{"2020", int64(4), int64(-20), int64(3)}
		expectedResults[1] = []interface{}{"2020", int64(5), nil, int64(0)}

		_, scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getDistinctFilter(), false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
		FailTestIfError(err, "Error in scan", t)
		err = tv.ValidateGroupAggrResult(expectedResults, scanResults)
		FailTestIfError(err, "Error in scan result validation", t)
	}

	//min/max/countn
	{
		ga, proj := basicGroupAggr()

		ga.Aggrs[0].Distinct = true
		ga.Aggrs[1].Distinct = true

		ga.Aggrs[0].AggrFunc = c.AGG_MIN
		ga.Aggrs[1].AggrFunc = c.AGG_MAX

		a2 := &qc.Aggregate{
			AggrFunc:   c.AGG_COUNTN,
			EntryKeyId: 7,
			KeyPos:     2,
			Distinct:   true,
		}

		ga.Aggrs = append(ga.Aggrs, a2)

		proj.EntryKeys = append(proj.EntryKeys, int64(7))

		expectedResults := make(tc.GroupAggrScanResponse, 2)
		expectedResults[0] = []interface{}{"2020", int64(4), int64(-20), "str", int64(2)}
		expectedResults[1] = []interface{}{"2020", int64(5), nil, nil, int64(0)}

		_, scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getDistinctFilter(), false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
		FailTestIfError(err, "Error in scan", t)
		err = tv.ValidateGroupAggrResult(expectedResults, scanResults)
		FailTestIfError(err, "Error in scan result validation", t)
	}
}

func TestGroupAggrDistinct2(t *testing.T) {
	log.Printf("In TestGroupAggrDistinct2()")

	var index1 = "index_agg"
	var bucketName = "default"

	//distinct aggregate on first key without group by
	{
		ga, proj := basicGroupAggr()

		ga.Aggrs[0].Distinct = true
		ga.Aggrs[0].KeyPos = 0
		ga.Aggrs[0].AggrFunc = c.AGG_COUNTN

		ga.Aggrs[1].Distinct = true
		ga.Aggrs[1].KeyPos = 0

		ga.Group = nil
		proj.EntryKeys = proj.EntryKeys[2:]

		expectedResults := make(tc.GroupAggrScanResponse, 1)
		expectedResults[0] = []interface{}{int64(0), int64(4)}

		_, scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getScanAllNoFilter(), false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
		FailTestIfError(err, "Error in scan", t)
		err = tv.ValidateGroupAggrResult(expectedResults, scanResults)
		FailTestIfError(err, "Error in scan result validation", t)
	}

	//distinct aggregate leading group by keys
	{
		ga, proj := basicGroupAggr()

		ga.Aggrs[0].Distinct = true
		ga.Aggrs[0].KeyPos = 0
		ga.Aggrs[0].AggrFunc = c.AGG_COUNT

		ga.Aggrs[1].Distinct = true
		ga.Aggrs[1].KeyPos = 1
		ga.Aggrs[1].AggrFunc = c.AGG_COUNTN

		expectedResults := make(tc.GroupAggrScanResponse, 4)
		expectedResults[0] = []interface{}{"2017", int64(1), int64(1), int64(1)}
		expectedResults[1] = []interface{}{"2017", int64(2), int64(1), int64(1)}
		expectedResults[2] = []interface{}{"2017", int64(3), int64(1), int64(1)}
		expectedResults[3] = []interface{}{"2017", int64(4), int64(1), int64(1)}

		_, scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getPartialMatchFilter2(), false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
		FailTestIfError(err, "Error in scan", t)
		err = tv.ValidateGroupAggrResult(expectedResults, scanResults)
		FailTestIfError(err, "Error in scan result validation", t)
	}

	//mix distinct and non distinct
	{
		ga, proj := basicGroupAggr()

		ga.Aggrs[0].KeyPos = 0
		ga.Aggrs[0].AggrFunc = c.AGG_COUNT

		ga.Aggrs[1].Distinct = true
		ga.Aggrs[1].KeyPos = 1
		ga.Aggrs[1].AggrFunc = c.AGG_COUNTN

		expectedResults := make(tc.GroupAggrScanResponse, 4)
		expectedResults[0] = []interface{}{"2017", int64(1), int64(2), int64(1)}
		expectedResults[1] = []interface{}{"2017", int64(2), int64(2), int64(1)}
		expectedResults[2] = []interface{}{"2017", int64(3), int64(2), int64(1)}
		expectedResults[3] = []interface{}{"2017", int64(4), int64(9), int64(1)}

		_, scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getPartialMatchFilter2(), false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
		FailTestIfError(err, "Error in scan", t)
		err = tv.ValidateGroupAggrResult(expectedResults, scanResults)
		FailTestIfError(err, "Error in scan result validation", t)
	}

}

func getDistinctFilter() qc.Scans {
	scans := make(qc.Scans, 1)

	filter1 := make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "2020", High: "2020", Inclusion: qc.Inclusion(uint32(3))}
	scans[0] = &qc.Scan{Filter: filter1}

	return scans
}

func TestGroupAggrNull(t *testing.T) {
	log.Printf("In TestGroupAggrNull()")

	var index1 = "index_agg"
	var bucketName = "default"

	//sum/count
	{
		ga, proj := basicGroupAggr()

		expectedResults := make(tc.GroupAggrScanResponse, 2)
		expectedResults[0] = []interface{}{"2020", int64(4), int64(-40), int64(6)}
		expectedResults[1] = []interface{}{"2020", int64(5), nil, int64(0)}

		_, scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getDistinctFilter(), false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
		FailTestIfError(err, "Error in scan", t)
		err = tv.ValidateGroupAggrResult(expectedResults, scanResults)
		FailTestIfError(err, "Error in scan result validation", t)
	}

	//min/max/countn
	{
		ga, proj := basicGroupAggr()

		ga.Aggrs[0].AggrFunc = c.AGG_MIN
		ga.Aggrs[1].AggrFunc = c.AGG_MAX

		a2 := &qc.Aggregate{
			AggrFunc:   c.AGG_COUNTN,
			EntryKeyId: 7,
			KeyPos:     2,
		}

		ga.Aggrs = append(ga.Aggrs, a2)

		proj.EntryKeys = append(proj.EntryKeys, int64(7))

		expectedResults := make(tc.GroupAggrScanResponse, 2)
		expectedResults[0] = []interface{}{"2020", int64(4), int64(-20), "str", int64(5)}
		expectedResults[1] = []interface{}{"2020", int64(5), nil, nil, int64(0)}

		_, scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getDistinctFilter(), false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
		FailTestIfError(err, "Error in scan", t)
		err = tv.ValidateGroupAggrResult(expectedResults, scanResults)
		FailTestIfError(err, "Error in scan result validation", t)
	}
}

func updateGroupAggDocs() tc.KeyValues {

	docs := make(tc.KeyValues)

	docs["doc7"] = Aggdoc{Year: "2017", Month: 1, Sale: 10}
	docs["doc8"] = Aggdoc{Year: "2017", Month: 1, Sale: 10}
	docs["doc9"] = Aggdoc{Year: "2017", Month: 1, Sale: 10.5}
	docs["doc10"] = Aggdoc{Year: "2017", Month: 1, Sale: 10.5}
	docs["doc11"] = Aggdoc{Year: "2017", Month: 1, Sale: 20.7}
	docs["doc12"] = Aggdoc{Year: "2017", Month: 1, Sale: 20}
	docs["doc13"] = Aggdoc{Year: "2017", Month: 1, Sale: 20}
	docs["doc14"] = Aggdoc{Year: "2017", Month: 1, Sale: 20.7}
	docs["doc15"] = Aggdoc{Year: "2017", Month: 1, Sale: 20.0}

	docs["doc16"] = Aggdoc{Year: "2017", Month: 4, Sale: 10.5}
	docs["doc32"] = Aggdoc{Year: "2017", Month: 4, Sale: 10.5}
	docs["doc33"] = Aggdoc{Year: "2017", Month: 4, Sale: 10}
	docs["doc34"] = Aggdoc{Year: "2017", Month: 4, Sale: 20}
	docs["doc35"] = Aggdoc{Year: "2017", Month: 4, Sale: 20}
	docs["doc36"] = Aggdoc{Year: "2017", Month: 4, Sale: 20.7}
	docs["doc37"] = Aggdoc{Year: "2017", Month: 4, Sale: 20.7}
	docs["doc38"] = Aggdoc{Year: "2017", Month: 4, Sale: 15}

	docs["doc39"] = Aggdoc{Year: "2020", Month: 1, Sale: 411168601842738790}
	docs["doc40"] = Aggdoc{Year: "2020", Month: 1, Sale: 411168601842738790}
	docs["doc41"] = Aggdoc{Year: "2020", Month: 1, Sale: 922337203685477580}
	docs["doc42"] = Aggdoc{Year: "2020", Month: 1, Sale: 922337203685477580}
	docs["doc43"] = Aggdoc{Year: "2020", Month: 4, Sale: -822337203685477580}
	docs["doc44"] = Aggdoc{Year: "2020", Month: 4, Sale: -822337203685477580}
	docs["doc45"] = Aggdoc{Year: "2020", Month: 4, Sale: 10}

	return docs

}

func TestGroupAggrInt64(t *testing.T) {
	log.Printf("In TestGroupAggrInt64()")

	log.Printf("Updating the default bucket")
	docs := updateGroupAggDocs()
	kvutility.SetKeyValues(docs, "default", "", clusterconfig.KVAddress)

	var index1 = "index_agg"
	var bucketName = "default"

	{
		ga, proj := basicGroupAggr()

		expectedResults := make(tc.GroupAggrScanResponse, 2)
		expectedResults[0] = []interface{}{"2017", int64(1), float64(142.4), int64(9)}
		expectedResults[1] = []interface{}{"2017", int64(4), float64(127.4), int64(8)}

		_, scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getPartialMatchFilter2(), false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
		FailTestIfError(err, "Error in scan", t)
		err = tv.ValidateGroupAggrResult(expectedResults, scanResults)
		FailTestIfError(err, "Error in scan result validation", t)
	}

	{
		ga, proj := basicGroupAggr()

		ga.Aggrs[0].Distinct = true
		ga.Aggrs[1].Distinct = true

		expectedResults := make(tc.GroupAggrScanResponse, 2)
		expectedResults[0] = []interface{}{"2017", int64(1), float64(61.2), int64(4)}
		expectedResults[1] = []interface{}{"2017", int64(4), float64(76.2), int64(5)}

		_, scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getPartialMatchFilter2(), false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
		FailTestIfError(err, "Error in scan", t)
		err = tv.ValidateGroupAggrResult(expectedResults, scanResults)
		FailTestIfError(err, "Error in scan result validation", t)
	}

	{
		ga, proj := basicGroupAggr()

		expectedResults := make(tc.GroupAggrScanResponse, 2)
		expectedResults[0] = []interface{}{"2020", int64(1), int64(2667011611056432740), int64(4)}
		expectedResults[1] = []interface{}{"2020", int64(4), int64(-1644674407370955150), int64(3)}

		_, scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getDistinctFilter(), false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
		FailTestIfError(err, "Error in scan", t)
		err = tv.ValidateGroupAggrResult(expectedResults, scanResults)
		FailTestIfError(err, "Error in scan result validation", t)
	}

	{
		ga, proj := basicGroupAggr()

		ga.Aggrs[0].Distinct = true
		ga.Aggrs[1].Distinct = true

		expectedResults := make(tc.GroupAggrScanResponse, 2)
		expectedResults[0] = []interface{}{"2020", int64(1), int64(1333505805528216370), int64(2)}
		expectedResults[1] = []interface{}{"2020", int64(4), int64(-822337203685477570), int64(2)}

		_, scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getDistinctFilter(), false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
		FailTestIfError(err, "Error in scan", t)
		err = tv.ValidateGroupAggrResult(expectedResults, scanResults)
		FailTestIfError(err, "Error in scan result validation", t)
	}

	docs = make(tc.KeyValues)
	docs["doc39"] = Aggdoc{Year: "2020", Month: 1, Sale: 4111686018427387900}
	docs["doc40"] = Aggdoc{Year: "2020", Month: 1, Sale: 4111686018427387900}
	docs["doc43"] = Aggdoc{Year: "2020", Month: 4, Sale: -8223372036854775808}
	docs["doc44"] = Aggdoc{Year: "2020", Month: 4, Sale: -8223372036854775808}

	kvutility.SetKeyValues(docs, "default", "", clusterconfig.KVAddress)

	{
		ga, proj := basicGroupAggr()

		expectedResults := make(tc.GroupAggrScanResponse, 2)
		expectedResults[0] = []interface{}{"2020", int64(1), float64(10068046444225730000), int64(4)}
		expectedResults[1] = []interface{}{"2020", int64(4), float64(-16446744073709552000), int64(3)}

		_, scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getDistinctFilter(), false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
		FailTestIfError(err, "Error in scan", t)
		err = tv.ValidateGroupAggrResult(expectedResults, scanResults)
		FailTestIfError(err, "Error in scan result validation", t)
	}

	{
		ga, proj := basicGroupAggr()

		ga.Aggrs[0].Distinct = true
		ga.Aggrs[1].Distinct = true

		expectedResults := make(tc.GroupAggrScanResponse, 2)
		expectedResults[0] = []interface{}{"2020", int64(1), int64(5034023222112865480), int64(2)}
		expectedResults[1] = []interface{}{"2020", int64(4), int64(-8223372036854775798), int64(2)}

		_, scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getDistinctFilter(), false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
		FailTestIfError(err, "Error in scan", t)
		err = tv.ValidateGroupAggrResult(expectedResults, scanResults)
		FailTestIfError(err, "Error in scan result validation", t)
	}
}

///////////////////////////////////////////////////////////////////////
// New Tests with N1QL query based validation and with vaster dataset
///////////////////////////////////////////////////////////////////////
func TestGroupAggr1(t *testing.T) {
	log.Printf("In TestGroupAggr1()")

	var bucket = "default"
	var index = "idx_aggrs"
	var arr_field = "friends"
	err := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(err, "Error in DropAllSecondaryIndexes", t)
	kvutility.FlushBucket(bucket, "", clusterconfig.Username, clusterconfig.Password, kvaddress)

	kvdocs := generateDocs(1000, "users_simplearray.prod")

	// Modify array docs for better distibution of duplicate array elements
	fn := func(arr []string, item string, num int) []string {
		for i := 0; i < num; i++ {
			arr = append(arr, item)
		}
		return arr
	}
	for _, v := range kvdocs {
		document := v.(map[string]interface{})
		array := document[arr_field].([]interface{})
		newArray := []string{}
		for _, item := range array {
			val := item.(string)
			newArray = fn(newArray, val, randomNum(1, 5))
		}
		document[arr_field] = newArray
	}
	kvutility.SetKeyValues(kvdocs, bucket, "", clusterconfig.KVAddress)

	indexExpr := []string{"company", "age", "eyeColor", "`address`.`number`", "age", "`first-name`"}

	stmt := "create primary index on default"
	_, err = tc.ExecuteN1QLStatement(kvaddress, clusterconfig.Username, clusterconfig.Password, bucket, stmt, false, gocb.NotBounded)
	FailTestIfError(err, "Error in creating primary index", t)

	err = secondaryindex.CreateSecondaryIndex(index, bucket, indexManagementAddress, "", indexExpr, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	//Scenario 1
	n1qlEquivalent := "SELECT  company as a, MIN(age) as b FROM default USE INDEX(`#primary`) " +
		" GROUP BY company"
	g1 := &qc.GroupKey{EntryKeyId: 6, KeyPos: 0}
	groups := []*qc.GroupKey{g1}

	a1 := &qc.Aggregate{AggrFunc: c.AGG_MIN, EntryKeyId: 7, KeyPos: 1}
	aggregates := []*qc.Aggregate{a1}

	ga := &qc.GroupAggr{
		Name:  "S1",
		Group: groups,
		Aggrs: aggregates,
	}
	proj := &qc.IndexProjection{
		EntryKeys: []int64{6, 7},
	}
	executeGroupAggrTest(ga, proj, n1qlEquivalent, index, t)

	//Scenario 2

	n1qlEquivalent = "SELECT  MIN(age) as a, MAX(`address`.`number`) as b, " +
		"COUNT(eyeColor) as c, age as d, eyeColor as e " +
		"FROM default USE INDEX(`#primary`) GROUP BY age, eyeColor"
	g1 = &qc.GroupKey{EntryKeyId: 9, KeyPos: 1}
	g2 := &qc.GroupKey{EntryKeyId: 10, KeyPos: 2}
	groups = []*qc.GroupKey{g1, g2}

	a1 = &qc.Aggregate{AggrFunc: c.AGG_MIN, EntryKeyId: 6, KeyPos: 1}
	a2 := &qc.Aggregate{AggrFunc: c.AGG_MAX, EntryKeyId: 7, KeyPos: 3}
	a3 := &qc.Aggregate{AggrFunc: c.AGG_COUNT, EntryKeyId: 8, KeyPos: 2}
	aggregates = []*qc.Aggregate{a1, a2, a3}

	ga = &qc.GroupAggr{
		Name:             "S1",
		Group:            groups,
		Aggrs:            aggregates,
		AllowPartialAggr: true,
	}
	proj = &qc.IndexProjection{
		EntryKeys: []int64{6, 7, 8, 9, 10},
	}
	executeGroupAggrTest(ga, proj, n1qlEquivalent, index, t)

}

func TestGroupAggrArrayIndex(t *testing.T) {
	log.Printf("In TestGroupAggrArrayIndex()")

	var i1 = "ga_arr1"
	var i2 = "ga_arr2"
	var bucket = "default"
	var err error
	var n1qlEquivalent string

	err = secondaryindex.CreateSecondaryIndex(i1, bucket, indexManagementAddress, "",
		[]string{"company", "ALL friends", "age"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.CreateSecondaryIndex(i2, bucket, indexManagementAddress, "",
		[]string{"ALL friends", "company", "age"}, false, nil, true, defaultIndexActiveTimeout, nil)

	// S1: Array aggregate on non-array field, group by non-array field
	log.Printf("Scenario 1")
	n1qlEquivalent = "SELECT  company as a, MIN(age) as b FROM default USE INDEX(`#primary`) " +
		" GROUP BY company"
	g1 := &qc.GroupKey{EntryKeyId: 6, KeyPos: 0}
	groups := []*qc.GroupKey{g1}

	a1 := &qc.Aggregate{AggrFunc: c.AGG_MIN, EntryKeyId: 7, KeyPos: 2}
	aggregates := []*qc.Aggregate{a1}

	ga := &qc.GroupAggr{
		Name:  "S1",
		Group: groups,
		Aggrs: aggregates,
	}
	proj := &qc.IndexProjection{
		EntryKeys: []int64{6, 7},
	}
	executeGroupAggrTest(ga, proj, n1qlEquivalent, i1, t)

	// S2: Array aggregate on non-array field, group by array field
	log.Printf("Scenario 2")
	n1qlEquivalent = "SELECT x as a, MIN(d.company) as b, MAX(d.age) as c from default d " +
		" USE INDEX(`#primary`) UNNEST d.friends AS x GROUP BY x"
	g1 = &qc.GroupKey{EntryKeyId: 6, KeyPos: 1}
	groups = []*qc.GroupKey{g1}

	a1 = &qc.Aggregate{AggrFunc: c.AGG_MIN, EntryKeyId: 7, KeyPos: 0}
	a2 := &qc.Aggregate{AggrFunc: c.AGG_MAX, EntryKeyId: 8, KeyPos: 2}
	aggregates = []*qc.Aggregate{a1, a2}

	ga = &qc.GroupAggr{
		Name:             "S2",
		Group:            groups,
		Aggrs:            aggregates,
		AllowPartialAggr: true,
	}
	proj = &qc.IndexProjection{
		EntryKeys: []int64{6, 7, 8},
	}
	executeGroupAggrTest(ga, proj, n1qlEquivalent, i1, t)

	// S3: Array aggregate on non-array field, no group by
	log.Printf("Scenario 3")
	n1qlEquivalent = "SELECT MIN(d.company) as a, MAX(d.age) as b from default d " +
		" USE INDEX(`#primary`)"

	a1 = &qc.Aggregate{AggrFunc: c.AGG_MIN, EntryKeyId: 7, KeyPos: 0}
	a2 = &qc.Aggregate{AggrFunc: c.AGG_MAX, EntryKeyId: 8, KeyPos: 2}
	aggregates = []*qc.Aggregate{a1, a2}

	ga = &qc.GroupAggr{
		Name:  "S3",
		Group: nil,
		Aggrs: aggregates,
	}

	proj = &qc.IndexProjection{
		EntryKeys: []int64{7, 8},
	}
	executeGroupAggrTest(ga, proj, n1qlEquivalent, i1, t)

	// S4: Array aggregate on array field, group by non-array field
	log.Printf("Scenario 4")
	n1qlEquivalent = "SELECT d.age as a, d.company as b, MIN(x) as c from default d " +
		"USE INDEX(`#primary`)  UNNEST d.friends AS x  GROUP BY d.age, d.company"
	g1 = &qc.GroupKey{EntryKeyId: 6, KeyPos: 2}
	g2 := &qc.GroupKey{EntryKeyId: 7, KeyPos: 0}
	groups = []*qc.GroupKey{g1, g2}

	a1 = &qc.Aggregate{AggrFunc: c.AGG_MIN, EntryKeyId: 8, KeyPos: 1}
	aggregates = []*qc.Aggregate{a1}

	ga = &qc.GroupAggr{
		Name:             "S4",
		Group:            groups,
		Aggrs:            aggregates,
		AllowPartialAggr: true,
	}
	proj = &qc.IndexProjection{
		EntryKeys: []int64{6, 7, 8},
	}
	executeGroupAggrTest(ga, proj, n1qlEquivalent, i1, t)

	// S5: Array aggregate on array field, group by array field
	log.Printf("Scenario 5")
	n1qlEquivalent = "SELECT x as a, COUNT(x) as b from default d " +
		"USE INDEX(`#primary`)  UNNEST d.friends AS x  GROUP BY x"
	g1 = &qc.GroupKey{EntryKeyId: 6, KeyPos: 1}
	groups = []*qc.GroupKey{g1}

	a1 = &qc.Aggregate{AggrFunc: c.AGG_COUNT, EntryKeyId: 7, KeyPos: 1}
	aggregates = []*qc.Aggregate{a1}

	ga = &qc.GroupAggr{
		Name:             "S5",
		Group:            groups,
		Aggrs:            aggregates,
		AllowPartialAggr: true,
	}
	proj = &qc.IndexProjection{
		EntryKeys: []int64{6, 7},
	}
	executeGroupAggrTest(ga, proj, n1qlEquivalent, i1, t)

	// S6: Array aggregate on array field, no group by
	log.Printf("Scenario 6")
	n1qlEquivalent = "SELECT COUNT(x) as a, MIN(x) as b, MAX(x) as c, COUNTN(x) as d, " +
		" SUM(x) as e from default d USE INDEX(`#primary`)  UNNEST d.friends AS x"

	a1 = &qc.Aggregate{AggrFunc: c.AGG_COUNT, EntryKeyId: 4, KeyPos: 1}
	a2 = &qc.Aggregate{AggrFunc: c.AGG_MIN, EntryKeyId: 5, KeyPos: 1}
	a3 := &qc.Aggregate{AggrFunc: c.AGG_MAX, EntryKeyId: 6, KeyPos: 1}
	a4 := &qc.Aggregate{AggrFunc: c.AGG_COUNTN, EntryKeyId: 7, KeyPos: 1}
	a5 := &qc.Aggregate{AggrFunc: c.AGG_SUM, EntryKeyId: 8, KeyPos: 1}

	aggregates = []*qc.Aggregate{a1, a2, a3, a4, a5}

	ga = &qc.GroupAggr{
		Name:  "S6",
		Group: nil,
		Aggrs: aggregates,
	}
	proj = &qc.IndexProjection{
		EntryKeys: []int64{4, 5, 6, 7, 8},
	}
	executeGroupAggrTest(ga, proj, n1qlEquivalent, i1, t)

	// S7: Distinct aggregate on non-array field, group by non-array field
	log.Printf("Scenario 7")
	n1qlEquivalent = "SELECT SUM(DISTINCT d.age) as a, d.company as b, x as c " +
		"from default d USE INDEX(`#primary`)  UNNEST d.friends AS x GROUP BY d.company, x"
	g1 = &qc.GroupKey{EntryKeyId: 7, KeyPos: 0}
	g2 = &qc.GroupKey{EntryKeyId: 8, KeyPos: 1}
	groups = []*qc.GroupKey{g1, g2}

	a1 = &qc.Aggregate{AggrFunc: c.AGG_SUM, EntryKeyId: 6, KeyPos: 2, Distinct: true}
	aggregates = []*qc.Aggregate{a1}

	ga = &qc.GroupAggr{
		Name:  "S7",
		Group: groups,
		Aggrs: aggregates,
	}
	proj = &qc.IndexProjection{
		EntryKeys: []int64{6, 7, 8},
	}
	executeGroupAggrTest(ga, proj, n1qlEquivalent, i1, t)

	// S8: Distinct aggregate on non-array field, group by array field
	log.Printf("Scenario 8")
	n1qlEquivalent = "SELECT x as a, COUNT(DISTINCT d.company) as b from default d " +
		" USE INDEX(`#primary`)  UNNEST d.friends AS x GROUP BY x"
	g1 = &qc.GroupKey{EntryKeyId: 6, KeyPos: 0}
	groups = []*qc.GroupKey{g1}

	a1 = &qc.Aggregate{AggrFunc: c.AGG_COUNT, EntryKeyId: 8, KeyPos: 1, Distinct: true}
	aggregates = []*qc.Aggregate{a1}

	ga = &qc.GroupAggr{
		Name:  "S8",
		Group: groups,
		Aggrs: aggregates,
	}
	proj = &qc.IndexProjection{
		EntryKeys: []int64{6, 8},
	}
	executeGroupAggrTest(ga, proj, n1qlEquivalent, i2, t)

	// S9: Distinct aggregate on non-array field, no group by
	log.Printf("Scenario 9")
	n1qlEquivalent = "SELECT COUNT(DISTINCT company) as a from default " +
		" USE INDEX(`#primary`)"

	a1 = &qc.Aggregate{AggrFunc: c.AGG_COUNT, EntryKeyId: 7, KeyPos: 0, Distinct: true}
	aggregates = []*qc.Aggregate{a1}

	ga = &qc.GroupAggr{
		Name:  "S9",
		Group: nil,
		Aggrs: aggregates,
	}
	proj = &qc.IndexProjection{
		EntryKeys: []int64{7},
	}
	executeGroupAggrTest(ga, proj, n1qlEquivalent, i1, t)

	// S10: Distinct aggregate on array field, group by non-array field
	log.Printf("Scenario 10")
	n1qlEquivalent = "SELECT d.company as a, COUNT(DISTINCT x) as b from default d " +
		"USE INDEX(`#primary`)  UNNEST d.friends AS x GROUP BY d.company"
	g1 = &qc.GroupKey{EntryKeyId: 6, KeyPos: 0}
	groups = []*qc.GroupKey{g1}

	a1 = &qc.Aggregate{AggrFunc: c.AGG_COUNT, EntryKeyId: 7, KeyPos: 1, Distinct: true}
	aggregates = []*qc.Aggregate{a1}

	ga = &qc.GroupAggr{
		Name:  "S10",
		Group: groups,
		Aggrs: aggregates,
	}
	proj = &qc.IndexProjection{
		EntryKeys: []int64{6, 7},
	}
	executeGroupAggrTest(ga, proj, n1qlEquivalent, i1, t)

	// S11: Distinct aggregate on array field, group by array field
	log.Printf("Scenario 11")
	n1qlEquivalent = "SELECT x as a, COUNT(DISTINCT x) as b from default d " +
		"USE INDEX(`#primary`)  UNNEST d.friends AS x GROUP BY x"
	g1 = &qc.GroupKey{EntryKeyId: 6, KeyPos: 0}
	groups = []*qc.GroupKey{g1}

	a1 = &qc.Aggregate{AggrFunc: c.AGG_COUNT, EntryKeyId: 7, KeyPos: 0, Distinct: true}
	aggregates = []*qc.Aggregate{a1}

	ga = &qc.GroupAggr{
		Name:  "S11",
		Group: groups,
		Aggrs: aggregates,
	}
	proj = &qc.IndexProjection{
		EntryKeys: []int64{6, 7},
	}
	executeGroupAggrTest(ga, proj, n1qlEquivalent, i2, t)

	// S12: Distinct aggregate on array field, no group
	log.Printf("Scenario 12")
	n1qlEquivalent = "SELECT COUNT(DISTINCT x) as a from default d " +
		"USE INDEX(`#primary`)  UNNEST d.friends AS x"

	a1 = &qc.Aggregate{AggrFunc: c.AGG_COUNT, EntryKeyId: 7, KeyPos: 0, Distinct: true}
	aggregates = []*qc.Aggregate{a1}

	ga = &qc.GroupAggr{
		Name:  "S12",
		Group: nil,
		Aggrs: aggregates,
	}
	proj = &qc.IndexProjection{
		EntryKeys: []int64{7},
	}
	executeGroupAggrTest(ga, proj, n1qlEquivalent, i2, t)

	//S13: Test for OnePerPrimaryKey
	log.Printf("Scenario 13")

	stmt := "CREATE INDEX test_oneperprimarykey ON default(ALL ARRAY v1 FOR v1 IN friends END, company, age)"
	_, err = tc.ExecuteN1QLStatement(kvaddress, clusterconfig.Username, clusterconfig.Password, bucket, stmt, false, gocb.NotBounded)
	FailTestIfError(err, "Error in index test_oneperprimarykey", t)

	n1qlEquivalent = "SELECT COUNT(company) as a, SUM(age) as b FROM default USE INDEX(`#primary`) " +
		" WHERE ANY v1 IN friends SATISFIES v1 == \"Aaron\" END "

	a1 = &qc.Aggregate{AggrFunc: c.AGG_COUNT, EntryKeyId: 7, KeyPos: 1}
	a2 = &qc.Aggregate{AggrFunc: c.AGG_SUM, EntryKeyId: 8, KeyPos: 2}
	aggregates = []*qc.Aggregate{a1, a2}

	ga = &qc.GroupAggr{
		Name:             "S13",
		Group:            nil,
		Aggrs:            aggregates,
		OnePerPrimaryKey: true,
	}
	proj = &qc.IndexProjection{
		EntryKeys: []int64{7, 8},
	}

	scans := make(qc.Scans, 1)
	filter1 := make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "Aaron", High: "Aaron", Inclusion: qc.Inclusion(uint32(3))}
	scans[0] = &qc.Scan{Filter: filter1}

	executeGroupAggrTest2(scans, ga, proj, n1qlEquivalent, i2, t)
}

//Test MIN/MAX/COUNT optimization MB-27861
func TestGroupAggr_FirstValidAggrOnly(t *testing.T) {
	log.Printf("In TestGroupAggr_FirstValidAggrOnly()")

	var i1 = "idx_asc_3field"
	var i2 = "idx_desc_3field"

	indexExpr1 := []string{"company", "age", "`first-name`"}
	indexExpr2 := []string{"company", "age", "`first-name`"}
	var bucket = "default"
	err := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(err, "Error in DropAllSecondaryIndexes", t)

	time.Sleep(5 * time.Second)

	stmt := "create primary index on default"
	_, err = tc.ExecuteN1QLStatement(kvaddress, clusterconfig.Username, clusterconfig.Password, bucket, stmt, false, gocb.NotBounded)
	FailTestIfError(err, "Error in creating primary index", t)

	err = secondaryindex.CreateSecondaryIndex(i1, bucket, indexManagementAddress, "", indexExpr1, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.CreateSecondaryIndex2(i2, bucket, indexManagementAddress, "", indexExpr2,
		[]bool{true, false, false}, false, nil, c.SINGLE, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	log.Printf("=== MIN no group by ===") // flag true
	n1qlEquivalent := "SELECT MIN(company) AS a FROM default USE INDEX(`#primary`) " +
		" WHERE company > \"A\" "
	a1 := &qc.Aggregate{AggrFunc: c.AGG_MIN, EntryKeyId: 7, KeyPos: 0}
	aggregates := []*qc.Aggregate{a1}
	ga := &qc.GroupAggr{Name: "S1", Group: nil, Aggrs: aggregates}

	proj := &qc.IndexProjection{EntryKeys: []int64{7}}
	scans := make(qc.Scans, 1)
	filter1 := make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "A", High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(3))}
	scans[0] = &qc.Scan{Filter: filter1}
	executeGroupAggrTest2(scans, ga, proj, n1qlEquivalent, i1, t)

	log.Printf("=== MIN no group by, no row match ===") // flag true
	n1qlEquivalent = "SELECT MIN(company) AS a FROM default USE INDEX(`#primary`) " +
		" WHERE company = \"blah\" "
	a1 = &qc.Aggregate{AggrFunc: c.AGG_MIN, EntryKeyId: 7, KeyPos: 0}
	aggregates = []*qc.Aggregate{a1}
	ga = &qc.GroupAggr{Group: nil, Aggrs: aggregates}

	proj = &qc.IndexProjection{EntryKeys: []int64{7}}
	scans = make(qc.Scans, 1)
	filter1 = make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "blah", High: "blah", Inclusion: qc.Inclusion(uint32(3))}
	scans[0] = &qc.Scan{Filter: filter1}
	executeGroupAggrTest2(scans, ga, proj, n1qlEquivalent, i1, t)

	log.Printf("=== MIN with group by ===") // flag false
	n1qlEquivalent = "SELECT MIN(company) AS a, company as b FROM default USE INDEX(`#primary`) " +
		" WHERE company > \"A\" GROUP BY company"
	a1 = &qc.Aggregate{AggrFunc: c.AGG_MIN, EntryKeyId: 7, KeyPos: 0}
	aggregates = []*qc.Aggregate{a1}
	g1 := &qc.GroupKey{EntryKeyId: 8, KeyPos: 0}
	groups := []*qc.GroupKey{g1}
	ga = &qc.GroupAggr{Group: groups, Aggrs: aggregates}

	proj = &qc.IndexProjection{EntryKeys: []int64{7, 8}}
	scans = make(qc.Scans, 1)
	filter1 = make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "A", High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(3))}
	scans[0] = &qc.Scan{Filter: filter1}
	executeGroupAggrTest2(scans, ga, proj, n1qlEquivalent, i1, t)

	log.Printf("=== MIN with group by, no row match ===") // flag false
	n1qlEquivalent = "SELECT MIN(company) AS a, company as b FROM default USE INDEX(`#primary`) " +
		" WHERE company = \"blah\" GROUP BY company"
	a1 = &qc.Aggregate{AggrFunc: c.AGG_MIN, EntryKeyId: 7, KeyPos: 0}
	aggregates = []*qc.Aggregate{a1}
	g1 = &qc.GroupKey{EntryKeyId: 8, KeyPos: 0}
	groups = []*qc.GroupKey{g1}
	ga = &qc.GroupAggr{Group: groups, Aggrs: aggregates}

	proj = &qc.IndexProjection{EntryKeys: []int64{7, 8}}
	scans = make(qc.Scans, 1)
	filter1 = make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "blah", High: "blah", Inclusion: qc.Inclusion(uint32(3))}
	scans[0] = &qc.Scan{Filter: filter1}
	executeGroupAggrTest2(scans, ga, proj, n1qlEquivalent, i1, t)

	log.Printf("=== One Aggr, no group by ===") // flag true
	n1qlEquivalent = "SELECT MIN(company) AS a FROM default USE INDEX(`#primary`) " +
		" WHERE company BETWEEN \"F\" and \"P\" "
	a1 = &qc.Aggregate{AggrFunc: c.AGG_MIN, EntryKeyId: 7, KeyPos: 0}
	aggregates = []*qc.Aggregate{a1}
	ga = &qc.GroupAggr{Group: nil, Aggrs: aggregates}

	proj = &qc.IndexProjection{EntryKeys: []int64{7}}
	scans = make(qc.Scans, 1)
	filter1 = make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "F", High: "P", Inclusion: qc.Inclusion(uint32(3))}
	scans[0] = &qc.Scan{Filter: filter1}
	executeGroupAggrTest2(scans, ga, proj, n1qlEquivalent, i1, t)

	log.Printf("=== One Aggr, no group by, no row match ===") // flag true
	n1qlEquivalent = "SELECT MIN(company) AS a FROM default USE INDEX(`#primary`) " +
		" WHERE company = \"blah\" "
	a1 = &qc.Aggregate{AggrFunc: c.AGG_MIN, EntryKeyId: 7, KeyPos: 0}
	aggregates = []*qc.Aggregate{a1}
	ga = &qc.GroupAggr{Group: nil, Aggrs: aggregates}

	proj = &qc.IndexProjection{EntryKeys: []int64{7}}
	scans = make(qc.Scans, 1)
	filter1 = make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "blah", High: "blah", Inclusion: qc.Inclusion(uint32(3))}
	scans[0] = &qc.Scan{Filter: filter1}
	executeGroupAggrTest2(scans, ga, proj, n1qlEquivalent, i1, t)

	log.Printf("=== Multiple Aggr, no group by ===") // flag false
	n1qlEquivalent = "SELECT MIN(company) AS a, MIN(age) as b FROM default USE INDEX(`#primary`) " +
		" WHERE company BETWEEN \"F\" and \"P\" "
	a1 = &qc.Aggregate{AggrFunc: c.AGG_MIN, EntryKeyId: 7, KeyPos: 0}
	a2 := &qc.Aggregate{AggrFunc: c.AGG_MIN, EntryKeyId: 8, KeyPos: 1}
	aggregates = []*qc.Aggregate{a1, a2}
	ga = &qc.GroupAggr{Group: nil, Aggrs: aggregates}

	proj = &qc.IndexProjection{EntryKeys: []int64{7, 8}}
	scans = make(qc.Scans, 1)
	filter1 = make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "F", High: "P", Inclusion: qc.Inclusion(uint32(3))}
	scans[0] = &qc.Scan{Filter: filter1}
	executeGroupAggrTest2(scans, ga, proj, n1qlEquivalent, i1, t)

	log.Printf("=== Multiple Aggr, no group by, no row match ===") // flag false
	n1qlEquivalent = "SELECT MIN(company) AS a, MIN(age) as b FROM default USE INDEX(`#primary`) " +
		" WHERE company = \"blah\" "
	a1 = &qc.Aggregate{AggrFunc: c.AGG_MIN, EntryKeyId: 7, KeyPos: 0}
	a2 = &qc.Aggregate{AggrFunc: c.AGG_MIN, EntryKeyId: 8, KeyPos: 1}
	aggregates = []*qc.Aggregate{a1, a2}
	ga = &qc.GroupAggr{Group: nil, Aggrs: aggregates}

	proj = &qc.IndexProjection{EntryKeys: []int64{7, 8}}
	scans = make(qc.Scans, 1)
	filter1 = make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "blah", High: "blah", Inclusion: qc.Inclusion(uint32(3))}
	scans[0] = &qc.Scan{Filter: filter1}
	executeGroupAggrTest2(scans, ga, proj, n1qlEquivalent, i1, t)

	log.Printf("=== No Aggr, 1 group by ===") // flag false
	n1qlEquivalent = "SELECT company AS a FROM default USE INDEX(`#primary`) " +
		" WHERE company BETWEEN \"F\" and \"P\" GROUP BY company "
	g1 = &qc.GroupKey{EntryKeyId: 8, KeyPos: 0}
	groups = []*qc.GroupKey{g1}
	ga = &qc.GroupAggr{Group: groups, Aggrs: nil}

	proj = &qc.IndexProjection{EntryKeys: []int64{8}}
	scans = make(qc.Scans, 1)
	filter1 = make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "F", High: "P", Inclusion: qc.Inclusion(uint32(3))}
	scans[0] = &qc.Scan{Filter: filter1}
	executeGroupAggrTest2(scans, ga, proj, n1qlEquivalent, i1, t)

	log.Printf("=== Aggr on non-leading key, previous equality filter, no group ===") // flag true
	n1qlEquivalent = "SELECT MIN(age) AS a FROM default USE INDEX(`#primary`) " +
		" WHERE company = \"ISODRIVE\"  "
	a1 = &qc.Aggregate{AggrFunc: c.AGG_MIN, EntryKeyId: 7, KeyPos: 1}
	aggregates = []*qc.Aggregate{a1}
	ga = &qc.GroupAggr{Group: nil, Aggrs: aggregates}

	proj = &qc.IndexProjection{EntryKeys: []int64{7}}
	scans = make(qc.Scans, 1)
	filter1 = make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "ISODRIVE", High: "ISODRIVE", Inclusion: qc.Inclusion(uint32(3))}
	scans[0] = &qc.Scan{Filter: filter1}
	executeGroupAggrTest2(scans, ga, proj, n1qlEquivalent, i1, t)

	log.Printf("=== Aggr on non-leading key, previous equality filters, no group ===") // flag true
	n1qlEquivalent = "SELECT MIN(`first-name`) AS a FROM default USE INDEX(`#primary`) " +
		" WHERE company = \"ISODRIVE\" and age = 39 "
	a1 = &qc.Aggregate{AggrFunc: c.AGG_MIN, EntryKeyId: 7, KeyPos: 2}
	aggregates = []*qc.Aggregate{a1}
	ga = &qc.GroupAggr{Group: nil, Aggrs: aggregates}

	proj = &qc.IndexProjection{EntryKeys: []int64{7}}
	scans = make(qc.Scans, 1)
	filter1 = make([]*qc.CompositeElementFilter, 2)
	filter1[0] = &qc.CompositeElementFilter{Low: "ISODRIVE", High: "ISODRIVE", Inclusion: qc.Inclusion(uint32(3))}
	filter1[1] = &qc.CompositeElementFilter{Low: 39, High: 39, Inclusion: qc.Inclusion(uint32(3))}
	scans[0] = &qc.Scan{Filter: filter1}
	executeGroupAggrTest2(scans, ga, proj, n1qlEquivalent, i1, t)

	log.Printf("=== Aggr on non-leading key, previous non-equality filters, no group ===") // flag false
	n1qlEquivalent = "SELECT MIN(`first-name`) AS a FROM default USE INDEX(`#primary`) " +
		" WHERE company BETWEEN \"F\" and \"P\"  "
	a1 = &qc.Aggregate{AggrFunc: c.AGG_MIN, EntryKeyId: 7, KeyPos: 2}
	aggregates = []*qc.Aggregate{a1}
	ga = &qc.GroupAggr{Group: nil, Aggrs: aggregates}

	proj = &qc.IndexProjection{EntryKeys: []int64{7}}
	scans = make(qc.Scans, 1)
	filter1 = make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "F", High: "P", Inclusion: qc.Inclusion(uint32(3))}
	scans[0] = &qc.Scan{Filter: filter1}
	executeGroupAggrTest2(scans, ga, proj, n1qlEquivalent, i1, t)

	log.Printf("=== MIN on desc, no group ===") // flag false
	n1qlEquivalent = "SELECT MIN(company) AS a FROM default USE INDEX(`#primary`) " +
		" WHERE company BETWEEN \"F\" and \"P\"  "
	a1 = &qc.Aggregate{AggrFunc: c.AGG_MIN, EntryKeyId: 7, KeyPos: 0}
	aggregates = []*qc.Aggregate{a1}
	ga = &qc.GroupAggr{Group: nil, Aggrs: aggregates}

	proj = &qc.IndexProjection{EntryKeys: []int64{7}}
	scans = make(qc.Scans, 1)
	filter1 = make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "F", High: "P", Inclusion: qc.Inclusion(uint32(3))}
	scans[0] = &qc.Scan{Filter: filter1}
	executeGroupAggrTest2(scans, ga, proj, n1qlEquivalent, i2, t)

	log.Printf("=== MAX on asc, no group ===") // flag false
	n1qlEquivalent = "SELECT MAX(company) AS a FROM default USE INDEX(`#primary`) " +
		" WHERE company BETWEEN \"F\" and \"P\"  "
	a1 = &qc.Aggregate{AggrFunc: c.AGG_MAX, EntryKeyId: 7, KeyPos: 0}
	aggregates = []*qc.Aggregate{a1}
	ga = &qc.GroupAggr{Group: nil, Aggrs: aggregates}

	proj = &qc.IndexProjection{EntryKeys: []int64{7}}
	scans = make(qc.Scans, 1)
	filter1 = make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "F", High: "P", Inclusion: qc.Inclusion(uint32(3))}
	scans[0] = &qc.Scan{Filter: filter1}
	executeGroupAggrTest2(scans, ga, proj, n1qlEquivalent, i1, t)

	log.Printf("=== MAX on desc, no group ===") // flag true
	n1qlEquivalent = "SELECT MAX(company) AS a FROM default USE INDEX(`#primary`) " +
		" WHERE company BETWEEN \"F\" and \"P\"  "
	a1 = &qc.Aggregate{AggrFunc: c.AGG_MAX, EntryKeyId: 7, KeyPos: 0}
	aggregates = []*qc.Aggregate{a1}
	ga = &qc.GroupAggr{Group: nil, Aggrs: aggregates}

	proj = &qc.IndexProjection{EntryKeys: []int64{7}}
	scans = make(qc.Scans, 1)
	filter1 = make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "F", High: "P", Inclusion: qc.Inclusion(uint32(3))}
	scans[0] = &qc.Scan{Filter: filter1}
	executeGroupAggrTest2(scans, ga, proj, n1qlEquivalent, i2, t)

	log.Printf("=== COUNT(DISTINCT const_expr, no group ===") // flag true
	n1qlEquivalent = "SELECT COUNT(DISTINCT 2+3) AS a FROM default USE INDEX(`#primary`) " +
		" WHERE company BETWEEN \"F\" and \"P\"  "
	a1 = &qc.Aggregate{AggrFunc: c.AGG_COUNT, EntryKeyId: 7, KeyPos: -1, Expr: "2+3", Distinct: true}
	aggregates = []*qc.Aggregate{a1}
	ga = &qc.GroupAggr{Group: nil, Aggrs: aggregates}

	proj = &qc.IndexProjection{EntryKeys: []int64{7}}
	scans = make(qc.Scans, 1)
	filter1 = make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "F", High: "P", Inclusion: qc.Inclusion(uint32(3))}
	scans[0] = &qc.Scan{Filter: filter1}
	executeGroupAggrTest2(scans, ga, proj, n1qlEquivalent, i2, t)

	log.Printf("=== COUNT(DISTINCT const_expr, no group, no row match ===") // flag true
	n1qlEquivalent = "SELECT COUNT(DISTINCT 2+3) AS a FROM default USE INDEX(`#primary`) " +
		" WHERE company = \"blah\"  "
	a1 = &qc.Aggregate{AggrFunc: c.AGG_COUNT, EntryKeyId: 7, KeyPos: -1, Expr: "2+3", Distinct: true}
	aggregates = []*qc.Aggregate{a1}
	ga = &qc.GroupAggr{Group: nil, Aggrs: aggregates}

	proj = &qc.IndexProjection{EntryKeys: []int64{7}}
	scans = make(qc.Scans, 1)
	filter1 = make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "blah", High: "blah", Inclusion: qc.Inclusion(uint32(3))}
	scans[0] = &qc.Scan{Filter: filter1}
	executeGroupAggrTest2(scans, ga, proj, n1qlEquivalent, i2, t)

	log.Printf("=== COUNT(const_expr, no group ===") // flag false
	n1qlEquivalent = "SELECT COUNT(2+3) AS a FROM default USE INDEX(`#primary`) " +
		" WHERE company BETWEEN \"F\" and \"P\"  "
	a1 = &qc.Aggregate{AggrFunc: c.AGG_COUNT, EntryKeyId: 7, KeyPos: -1, Expr: "2+3"}
	aggregates = []*qc.Aggregate{a1}
	ga = &qc.GroupAggr{Group: nil, Aggrs: aggregates}

	proj = &qc.IndexProjection{EntryKeys: []int64{7}}
	scans = make(qc.Scans, 1)
	filter1 = make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "F", High: "P", Inclusion: qc.Inclusion(uint32(3))}
	scans[0] = &qc.Scan{Filter: filter1}
	executeGroupAggrTest2(scans, ga, proj, n1qlEquivalent, i2, t)

	MinAggrOptimizationTests(t)
}

func MinAggrOptimizationTests(t *testing.T) {
	setup := func() {
		makeDocs := func() {
			docs := make(tc.KeyValues)
			doc1 := make(map[string]interface{})
			doc1["a"] = "aa"
			doc1["b"] = "ba"
			doc1["c"] = 10
			docs["doc1"] = doc1

			doc2 := make(map[string]interface{})
			doc2["a"] = "aa"
			doc2["b"] = "bb"
			doc2["c"] = 5
			docs["doc2"] = doc2
			kvutility.SetKeyValues(docs, "default", "", clusterconfig.KVAddress)
		}
		makeDocs()

		err := secondaryindex.CreateSecondaryIndex("indexMinAggr", "default",
			indexManagementAddress, "", []string{"a", "b", "c"}, false, nil,
			true, defaultIndexActiveTimeout, nil)
		FailTestIfError(err, "Error in creating the index", t)
	}

	test1 := func() {
		log.Printf("=== Equality filter check: Equality for a, nil filter for b - inclusion 0, no filter for c ===")
		n1qlEquivalent := "SELECT MIN(c) AS a FROM default USE INDEX(`#primary`) " +
			" WHERE a = \"aa\" "
		a1 := &qc.Aggregate{AggrFunc: c.AGG_MIN, EntryKeyId: 4, KeyPos: 2}
		aggregates := []*qc.Aggregate{a1}
		ga := &qc.GroupAggr{
			Group:              nil,
			Aggrs:              aggregates,
			DependsOnIndexKeys: []int32{2},
			IndexKeyNames:      []string{"(`default`.`a`)", "(`default`.`b`)", "(`default`.`c`)", "(meta(`default`).`id`)"},
		}

		proj := &qc.IndexProjection{EntryKeys: []int64{4}}
		scans := make(qc.Scans, 1)
		filter1 := make([]*qc.CompositeElementFilter, 2)
		filter1[0] = &qc.CompositeElementFilter{Low: "aa", High: "aa", Inclusion: qc.Inclusion(uint32(3))}
		filter1[1] = &qc.CompositeElementFilter{Low: c.MinUnbounded, High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(0))}
		scans[0] = &qc.Scan{Filter: filter1}
		executeGroupAggrTest2(scans, ga, proj, n1qlEquivalent, "indexMinAggr", t)
	}

	test2 := func() {
		log.Printf("=== Equality filter check: Equality for a, nil filter for b - inclusion 3, no filter for c ===")
		n1qlEquivalent := "SELECT MIN(c) AS a FROM default USE INDEX(`#primary`) " +
			" WHERE a = \"aa\" "
		a1 := &qc.Aggregate{AggrFunc: c.AGG_MIN, EntryKeyId: 4, KeyPos: 2}
		aggregates := []*qc.Aggregate{a1}
		ga := &qc.GroupAggr{
			Group:              nil,
			Aggrs:              aggregates,
			DependsOnIndexKeys: []int32{2},
			IndexKeyNames:      []string{"(`default`.`a`)", "(`default`.`b`)", "(`default`.`c`)", "(meta(`default`).`id`)"},
		}

		proj := &qc.IndexProjection{EntryKeys: []int64{4}}
		scans := make(qc.Scans, 1)
		filter1 := make([]*qc.CompositeElementFilter, 2)
		filter1[0] = &qc.CompositeElementFilter{Low: "aa", High: "aa", Inclusion: qc.Inclusion(uint32(3))}
		filter1[1] = &qc.CompositeElementFilter{Low: c.MinUnbounded, High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(3))}
		scans[0] = &qc.Scan{Filter: filter1}
		executeGroupAggrTest2(scans, ga, proj, n1qlEquivalent, "indexMinAggr", t)
	}

	test3 := func() {
		log.Printf("=== Equality filter check: Equality for a, nil filter for b - inclusion 3, nil filter for c ===")
		n1qlEquivalent := "SELECT MIN(c) AS a FROM default USE INDEX(`#primary`) " +
			" WHERE a = \"aa\" "
		a1 := &qc.Aggregate{AggrFunc: c.AGG_MIN, EntryKeyId: 4, KeyPos: 2}
		aggregates := []*qc.Aggregate{a1}
		ga := &qc.GroupAggr{
			Group:              nil,
			Aggrs:              aggregates,
			DependsOnIndexKeys: []int32{2},
			IndexKeyNames:      []string{"(`default`.`a`)", "(`default`.`b`)", "(`default`.`c`)", "(meta(`default`).`id`)"},
		}

		proj := &qc.IndexProjection{EntryKeys: []int64{4}}
		scans := make(qc.Scans, 1)
		filter1 := make([]*qc.CompositeElementFilter, 2)
		filter1[0] = &qc.CompositeElementFilter{Low: "aa", High: "aa", Inclusion: qc.Inclusion(uint32(3))}
		filter1[1] = &qc.CompositeElementFilter{Low: c.MinUnbounded, High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(3))}
		filter1[1] = &qc.CompositeElementFilter{Low: c.MinUnbounded, High: c.MaxUnbounded, Inclusion: qc.Inclusion(uint32(0))}
		scans[0] = &qc.Scan{Filter: filter1}
		executeGroupAggrTest2(scans, ga, proj, n1qlEquivalent, "indexMinAggr", t)
	}

	setup()
	test1()
	test2()
	test3()
}

func TestGroupAggrPrimary(t *testing.T) {
	log.Printf("In TestGroupAggrPrimary()")

	var index = "#primary"

	//select sum(meta().id) from default
	n1qlEquivalent := "select sum(meta().id) as a from default"
	a1 := &qc.Aggregate{AggrFunc: c.AGG_SUM, EntryKeyId: 1, KeyPos: 0}
	aggregates := []*qc.Aggregate{a1}

	dependsOnIndexKeys := []int32{0}

	ga := &qc.GroupAggr{
		Name:               "Primary",
		Group:              nil,
		Aggrs:              aggregates,
		DependsOnIndexKeys: dependsOnIndexKeys,
	}
	proj := &qc.IndexProjection{
		EntryKeys: []int64{1},
	}

	executeGroupAggrTest(ga, proj, n1qlEquivalent, index, t)

	//select min(meta().id) from default
	n1qlEquivalent = "select min(meta().id) as a from default"
	ga.Aggrs[0].AggrFunc = c.AGG_MIN
	executeGroupAggrTest(ga, proj, n1qlEquivalent, index, t)

	//select count(meta().id) from default
	n1qlEquivalent = "select count(meta().id) as a from default"
	ga.Aggrs[0].AggrFunc = c.AGG_COUNT
	executeGroupAggrTest(ga, proj, n1qlEquivalent, index, t)

	{
		//select meta().id, count(meta().id) from default group by meta().id
		n1qlEquivalent = "select meta().id as a, count(meta().id) as b from default group by meta().id"
		g1 := &qc.GroupKey{EntryKeyId: 0, KeyPos: 0}
		groups := []*qc.GroupKey{g1}

		a1 := &qc.Aggregate{AggrFunc: c.AGG_COUNT, EntryKeyId: 1, KeyPos: 0}
		aggregates := []*qc.Aggregate{a1}

		ga := &qc.GroupAggr{
			Name:               "primary",
			Group:              groups,
			Aggrs:              aggregates,
			DependsOnIndexKeys: []int32{0},
			IndexKeyNames: []string{
				"(meta(`default`).`id`)"},
		}

		proj := &qc.IndexProjection{
			EntryKeys: []int64{0, 1},
		}
		executeGroupAggrTest(ga, proj, n1qlEquivalent, index, t)
	}

	{
		//select meta().id, count(meta().id) from default group by meta().id, substr(meta().id, 0, 4)
		n1qlEquivalent = "select meta().id a, count(meta().id) as b from default group by meta().id, substr(meta().id, 0, 4)"
		g1 := &qc.GroupKey{EntryKeyId: 0, KeyPos: 0}
		g2 := &qc.GroupKey{EntryKeyId: 1, KeyPos: -1, Expr: "SUBSTR(cover ((meta(`default`).`id`)), 0, 4)"}
		groups := []*qc.GroupKey{g1, g2}

		a1 := &qc.Aggregate{AggrFunc: c.AGG_COUNT, EntryKeyId: 2, KeyPos: 0}
		aggregates := []*qc.Aggregate{a1}

		ga := &qc.GroupAggr{
			Name:               "primary",
			Group:              groups,
			Aggrs:              aggregates,
			DependsOnIndexKeys: []int32{0},
			IndexKeyNames: []string{
				"(meta(`default`).`id`)"},
		}

		proj := &qc.IndexProjection{
			EntryKeys: []int64{0, 2},
		}
		executeGroupAggrTest(ga, proj, n1qlEquivalent, index, t)
	}

	{

		//select meta().id, count(meta().id) from default group by 1)
		n1qlEquivalent = "select count(meta().id) as a from default group by 1"
		g1 := &qc.GroupKey{EntryKeyId: 0, KeyPos: -1, Expr: "1"}
		groups := []*qc.GroupKey{g1}

		a1 := &qc.Aggregate{AggrFunc: c.AGG_COUNT, EntryKeyId: 1, KeyPos: 0}
		aggregates := []*qc.Aggregate{a1}

		ga := &qc.GroupAggr{
			Name:               "primary",
			Group:              groups,
			Aggrs:              aggregates,
			DependsOnIndexKeys: []int32{0},
			IndexKeyNames: []string{
				"(meta(`default`).`id`)"},
		}

		proj := &qc.IndexProjection{
			EntryKeys: []int64{1},
		}
		executeGroupAggrTest(ga, proj, n1qlEquivalent, index, t)

	}

	{
		//select sum(substr(meta().id,0, 4)), count(1) from default
		n1qlEquivalent := "SELECT SUM(SUBSTR(meta().id, 0, 4)) as a, COUNT(1) as b from default"

		a1 := &qc.Aggregate{AggrFunc: c.AGG_SUM, EntryKeyId: 0, KeyPos: -1,
			Expr: "SUBSTR(cover ((meta(`default`).`id`)), 0, 4)"}

		a2 := &qc.Aggregate{AggrFunc: c.AGG_COUNT, EntryKeyId: 1, KeyPos: -1, Expr: "1"}

		aggregates := []*qc.Aggregate{a1, a2}

		ga := &qc.GroupAggr{
			Name:               "primary",
			Group:              nil,
			Aggrs:              aggregates,
			DependsOnIndexKeys: []int32{0},
			IndexKeyNames: []string{
				"(meta(`default`).`id`)"},
		}

		proj := &qc.IndexProjection{
			EntryKeys: []int64{0, 1},
		}
		executeGroupAggrTest(ga, proj, n1qlEquivalent, index, t)
	}

	{
		//select sum(1) from default
		n1qlEquivalent := "SELECT SUM(1) as a from default"

		a1 := &qc.Aggregate{AggrFunc: c.AGG_SUM, EntryKeyId: 0, KeyPos: -1, Expr: "1"}
		aggregates := []*qc.Aggregate{a1}

		ga := &qc.GroupAggr{
			Name:               "primary",
			Group:              nil,
			Aggrs:              aggregates,
			DependsOnIndexKeys: []int32{0},
			IndexKeyNames: []string{
				"(meta(`default`).`id`)"},
		}

		proj := &qc.IndexProjection{
			EntryKeys: []int64{0},
		}
		executeGroupAggrTest(ga, proj, n1qlEquivalent, index, t)
	}

	{
		//select count(distinct 1) from default
		n1qlEquivalent := "SELECT COUNT(DISTINCT 1) as a from default"

		a1 := &qc.Aggregate{AggrFunc: c.AGG_COUNT, EntryKeyId: 0, KeyPos: -1, Expr: "1", Distinct: true}
		aggregates := []*qc.Aggregate{a1}

		ga := &qc.GroupAggr{
			Name:               "primary",
			Group:              nil,
			Aggrs:              aggregates,
			DependsOnIndexKeys: []int32{0},
			IndexKeyNames: []string{
				"(meta(`default`).`id`)"},
		}

		proj := &qc.IndexProjection{
			EntryKeys: []int64{0},
		}
		executeGroupAggrTest(ga, proj, n1qlEquivalent, index, t)
	}

	{

		//select count(null) from default
		n1qlEquivalent = "select count(null) as a from default"

		a1 := &qc.Aggregate{AggrFunc: c.AGG_COUNT, EntryKeyId: 1, KeyPos: -1, Expr: "NULL"}
		aggregates := []*qc.Aggregate{a1}

		ga := &qc.GroupAggr{
			Name:               "primary",
			Group:              nil,
			Aggrs:              aggregates,
			DependsOnIndexKeys: []int32{0},
			IndexKeyNames: []string{
				"(meta(`default`).`id`)"},
		}

		proj := &qc.IndexProjection{
			EntryKeys: []int64{1},
		}
		executeGroupAggrTest(ga, proj, n1qlEquivalent, index, t)

		//select min(null) from default
		n1qlEquivalent = "select min(null) as a from default"

		ga.Aggrs[0].AggrFunc = c.AGG_MIN
		executeGroupAggrTest(ga, proj, n1qlEquivalent, index, t)

	}

	{
		log.Printf("--- MB-28305 Scenario 1 ---")
		n1qlEquivalent = "select COUNT(meta().id) as a from default where meta().id IS NULL"

		a1 := &qc.Aggregate{AggrFunc: c.AGG_COUNT, EntryKeyId: 1, KeyPos: 1}
		aggregates := []*qc.Aggregate{a1}

		ga := &qc.GroupAggr{
			Name:               "primary",
			Group:              nil,
			Aggrs:              aggregates,
			DependsOnIndexKeys: []int32{0},
			IndexKeyNames:      []string{"(meta(`default`).`id`)"},
		}

		proj := &qc.IndexProjection{
			EntryKeys: []int64{1},
		}
		scans := make(qc.Scans, 1)
		scans[0] = &qc.Scan{Filter: getPrimaryFilter(nil, nil, 3)}
		executeGroupAggrTest2(scans, ga, proj, n1qlEquivalent, index, t)
	}

	{
		log.Printf("--- MB-28305 Scenario 2 ---")
		n1qlEquivalent = "select COUNT(meta().id) as a from default where meta().id > 'z' and meta().id < 'a' "

		a1 := &qc.Aggregate{AggrFunc: c.AGG_COUNT, EntryKeyId: 1, KeyPos: 1}
		aggregates := []*qc.Aggregate{a1}

		ga := &qc.GroupAggr{
			Name:               "primary",
			Group:              nil,
			Aggrs:              aggregates,
			DependsOnIndexKeys: []int32{0},
			IndexKeyNames:      []string{"(meta(`default`).`id`)"},
		}

		proj := &qc.IndexProjection{
			EntryKeys: []int64{1},
		}
		scans := make(qc.Scans, 1)
		scans[0] = &qc.Scan{Filter: getPrimaryFilter("z", "a", 0)}
		executeGroupAggrTest2(scans, ga, proj, n1qlEquivalent, index, t)
	}

	{
		log.Printf("--- MB-28305 Scenario 3 ---")
		n1qlEquivalent = "select COUNT(meta().id) as a from default where meta().id IS NULL " +
			"OR meta().id > 'doc32' and meta().id < 'doc37' "

		a1 := &qc.Aggregate{AggrFunc: c.AGG_COUNT, EntryKeyId: 1, KeyPos: 0}
		aggregates := []*qc.Aggregate{a1}

		ga := &qc.GroupAggr{
			Name:               "primary",
			Group:              nil,
			Aggrs:              aggregates,
			DependsOnIndexKeys: []int32{0},
			IndexKeyNames:      []string{"(meta(`default`).`id`)"},
		}

		proj := &qc.IndexProjection{
			EntryKeys: []int64{1},
		}
		scans := make(qc.Scans, 2)
		scans[0] = &qc.Scan{Filter: getPrimaryFilter(nil, nil, 3)}
		scans[1] = &qc.Scan{Filter: getPrimaryFilter("doc32", "doc37", 0)}
		executeGroupAggrTest2(scans, ga, proj, n1qlEquivalent, index, t)
	}

	secondaryindex.UseClient = tmpclient
}

type GroupAggrDocumentKeyDoc1 struct {
	Attr1 string `json:"Attr1,omitempty"`
	Id    string `json:"Id,omitempty"`
}

type GroupAggrDocumentKeyDoc2 struct {
	Attr1 string `json:"Attr1,omitempty"`
	Id    string `json:"Id,omitempty"`
	Attr2 string `json:"Attr2,omitempty"`
	Attr3 string `json:"Attr3,omitempty"`
}

func makeGroupAggrDocumentKeyDocs1() tc.KeyValues {

	docs := make(tc.KeyValues)

	docs["e::g::1"] = GroupAggrDocumentKeyDoc1{Attr1: "doct", Id: "e::g::1"}
	docs["e::g::2"] = GroupAggrDocumentKeyDoc1{Attr1: "doct", Id: "e::g::2"}
	docs["e::g::3"] = GroupAggrDocumentKeyDoc1{Attr1: "doct", Id: "e::g::3"}

	return docs
}

func makeGroupAggrDocumentKeyDocs2() tc.KeyValues {

	docs := make(tc.KeyValues)

	docs["e::z::1"] = GroupAggrDocumentKeyDoc2{Attr1: "docz", Id: "e::z::1", Attr2: "abc", Attr3: "male"}
	docs["e::z::2"] = GroupAggrDocumentKeyDoc2{Attr1: "docz", Id: "e::z::2", Attr2: "abc", Attr3: "male"}
	docs["e::z::3"] = GroupAggrDocumentKeyDoc2{Attr1: "docz", Id: "e::z::3", Attr2: "abc", Attr3: "male"}

	return docs
}

func getGroupAggrDocumentKeyFilter1() qc.Scans {
	scans := make(qc.Scans, 1)

	filter1 := make([]*qc.CompositeElementFilter, 1)
	filter1[0] = &qc.CompositeElementFilter{Low: "doct", High: "doct", Inclusion: qc.Inclusion(uint32(3))}
	scans[0] = &qc.Scan{Filter: filter1}

	return scans
}

func getGroupAggrDocumentKeyFilter2() qc.Scans {
	scans := make(qc.Scans, 1)
	filter1 := make([]*qc.CompositeElementFilter, 3)
	filter1[0] = &qc.CompositeElementFilter{Low: "docz", High: "docz", Inclusion: qc.Inclusion(uint32(3))}
	filter1[1] = &qc.CompositeElementFilter{Low: "abc", High: "abc", Inclusion: qc.Inclusion(uint32(3))}
	filter1[2] = &qc.CompositeElementFilter{Low: "male", High: "male", Inclusion: qc.Inclusion(uint32(3))}
	scans[0] = &qc.Scan{Filter: filter1}
	return scans
}

func getGroupAggrDocumentKey1() (*qc.GroupAggr, *qc.IndexProjection) {

	groups := make([]*qc.GroupKey, 1)

	g1 := &qc.GroupKey{
		EntryKeyId: 2,
		KeyPos:     -1,
		Expr:       "(split(cover ((meta(`d`).`id`)), \"::\")[2])",
	}

	groups[0] = g1

	dependsOnIndexKeys := make([]int32, 1)
	dependsOnIndexKeys[0] = int32(1)
	indexKeyNames := []string{"(`d`.`Attr1`)", "(meta(`d`).`id`)"}
	ga := &qc.GroupAggr{
		Group:              groups,
		DependsOnIndexKeys: dependsOnIndexKeys,
		IndexKeyNames:      indexKeyNames,
		AllowPartialAggr:   true,
		OnePerPrimaryKey:   false,
	}

	entry := make([]int64, 1)
	entry[0] = 2
	proj := &qc.IndexProjection{
		EntryKeys: entry,
	}

	return ga, proj
}

func getGroupAggrDocumentKey2() (*qc.GroupAggr, *qc.IndexProjection) {

	groups := make([]*qc.GroupKey, 1)

	g1 := &qc.GroupKey{
		EntryKeyId: 4,
		KeyPos:     -1,
		Expr:       "(split(cover ((meta(`d`).`id`)), \"::\")[2])",
	}

	groups[0] = g1

	dependsOnIndexKeys := make([]int32, 1)
	dependsOnIndexKeys[0] = int32(3)
	indexKeyNames := []string{"(`d`.`Attr1`)", "(`d`.`Attr2`)", "(`d`.`Attr3`)", "(meta(`d`).`id`)"}
	ga := &qc.GroupAggr{
		Group:              groups,
		DependsOnIndexKeys: dependsOnIndexKeys,
		IndexKeyNames:      indexKeyNames,
		AllowPartialAggr:   true,
		OnePerPrimaryKey:   false,
	}

	entry := make([]int64, 1)
	entry[0] = 4
	proj := &qc.IndexProjection{
		EntryKeys: entry,
	}

	return ga, proj
}

// test group aggrgate where group by clause uses document key i.e META().id
func TestGroupAggrDocumentKey(t *testing.T) {

	log.Printf("In TestGroupAggrDocumentKey()")
	var bucket = "default"
	//test 1 single key index
	var index1 = "documentkey_idx1"
	secondaryindex.DropSecondaryIndex(index1, bucket, indexManagementAddress)
	//test 2 composite key index
	var index2 = "documentkey_idx2"
	secondaryindex.DropSecondaryIndex(index2, bucket, indexManagementAddress)

	log.Printf("Populating the default bucket for TestGroupAggrDocumentKey single key index")
	docs1 := makeGroupAggrDocumentKeyDocs1()
	kvutility.SetKeyValues(docs1, "default", "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndex(index1, bucket, indexManagementAddress,
		"Attr1=\"doct\"", []string{"Attr1"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index1 for TestGroupAggrDocumentKey", t)

	scans := getGroupAggrDocumentKeyFilter1()
	ga, proj := getGroupAggrDocumentKey1()
	_, scanResults, err := secondaryindex.Scan3(index1, bucket, indexScanAddress, scans, false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)

	n1qlEquivalent := "SELECT RAW g FROM default AS d  USE INDEX (`#primary`) WHERE d.Attr1 = \"doct\" GROUP BY (SPLIT(META(d).id, \"::\")[2]) AS g ORDER BY g ASC;"
	results, err := tc.ExecuteN1QLStatement(kvaddress, clusterconfig.Username, clusterconfig.Password, bucket, n1qlEquivalent, false, gocb.NotBounded)
	if err != nil {
		FailTestIfError(err, "Error in", t)
	}
	// Generate expectedResponse based on results
	expectedResponse := make(tc.GroupAggrScanResponse, 0)
	for _, result := range results {
		rows := make([]interface{}, 0)
		res := result.(string)
		if res != "" {
			rows = append(rows, value.NewValue(res))
		} else {
			rows = append(rows, value.NewMissingValue())
		}
		expectedResponse = append(expectedResponse, rows)
	}
	err = tv.ValidateGroupAggrResult(expectedResponse, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	// test 2 composite key index
	log.Printf("Populating the default bucket for TestGroupAggrDocumentKey composite key index")
	docs2 := makeGroupAggrDocumentKeyDocs2()
	kvutility.SetKeyValues(docs2, "default", "", clusterconfig.KVAddress)

	err = secondaryindex.CreateSecondaryIndex(index2, bucket, indexManagementAddress,
		"Attr1=\"docz\" AND Attr2=\"abc\" AND Attr3=\"male\"", []string{"Attr1", "Attr2", "Attr3"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index2 for TestGroupAggrDocumentKey ", t)

	scans = getGroupAggrDocumentKeyFilter2()
	ga, proj = getGroupAggrDocumentKey2()
	_, scanResults, err = secondaryindex.Scan3(index2, bucket, indexScanAddress, scans, false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)

	n1qlEquivalent = "SELECT RAW g FROM default AS d  USE INDEX (`#primary`) WHERE d.Attr1 = \"docz\" AND d.Attr2 = \"abc\" AND d.Attr3 = \"male\" GROUP BY (SPLIT(META(d).id, \"::\")[2]) AS g ORDER BY g ASC;"
	results, err = tc.ExecuteN1QLStatement(kvaddress, clusterconfig.Username, clusterconfig.Password, bucket, n1qlEquivalent, false, gocb.NotBounded)
	if err != nil {
		FailTestIfError(err, "Error in", t)
	}
	// Generate expectedResponse based on results
	expectedResponse = make(tc.GroupAggrScanResponse, 0)
	for _, result := range results {
		rows := make([]interface{}, 0)
		res := result.(string)
		if res != "" {
			rows = append(rows, value.NewValue(res))
		} else {
			rows = append(rows, value.NewMissingValue())
		}
		expectedResponse = append(expectedResponse, rows)
	}
	err = tv.ValidateGroupAggrResult(expectedResponse, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

}
