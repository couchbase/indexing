package functionaltests

import (
	c "github.com/couchbase/indexing/secondary/common"
	qc "github.com/couchbase/indexing/secondary/queryport/client"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	//"github.com/couchbase/indexing/secondary/tests/framework/datautility"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	//tv "github.com/couchbase/indexing/secondary/tests/framework/validation"
	"log"
	"testing"
	"time"
)

var tmpclient string

func TestGroupAggrSetup(t *testing.T) {
	log.Printf("In TestGroupAggrSetup()")

	tmpclient = secondaryindex.UseClient
	secondaryindex.UseClient = "gsi"

	var index = "index_agg"
	var bucket = "default"

	log.Printf("Emptying the default bucket")
	kvutility.EnableBucketFlush("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	kvutility.FlushBucket("default", "", clusterconfig.Username, clusterconfig.Password, kvaddress)
	time.Sleep(5 * time.Second)

	secondaryindex.DropSecondaryIndex(index, bucket, indexManagementAddress)

	// Populate the bucket now
	log.Printf("Populating the default bucket")
	docs := makeGroupAggDocs()
	kvutility.SetKeyValues(docs, "default", "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndex(index, bucket, indexManagementAddress, "", []string{"Year", "Month", "Sale"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)
}

type Aggdoc struct {
	Year  string
	Month int64
	Sale  int64
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

	scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getScanAllNoFilter(), false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	tc.PrintScanResults(scanResults, "scanResults")
}

func TestGroupAggrNonLeading(t *testing.T) {
	log.Printf("In TestGroupAggrNonLeading()")

	var index1 = "index_agg"
	var bucketName = "default"

	ga, proj := basicGroupAggr()

	ga.Group = ga.Group[1:]
	proj.EntryKeys = proj.EntryKeys[1:]

	scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getScanAllNoFilter(), false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	tc.PrintScanResults(scanResults, "scanResults")
}

func TestGroupAggrNoGroup(t *testing.T) {
	log.Printf("In TestGroupAggrNoGroup()")

	var index1 = "index_agg"
	var bucketName = "default"

	ga, proj := basicGroupAggr()
	ga.Group = nil
	proj.EntryKeys = proj.EntryKeys[2:]

	scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getScanAllNoFilter(), false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	tc.PrintScanResults(scanResults, "scanResults")
}

func TestGroupAggrMinMax(t *testing.T) {
	log.Printf("In TestGroupAggrMinMax()")

	var index1 = "index_agg"
	var bucketName = "default"

	ga, proj := basicGroupAggr()

	ga.Aggrs[0].AggrFunc = c.AGG_MIN
	ga.Aggrs[1].AggrFunc = c.AGG_MAX

	scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getScanAllNoFilter(), false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	tc.PrintScanResults(scanResults, "scanResults")
}

func TestGroupAggrLeading_N1QLExprs(t *testing.T) {
	log.Printf("In TestGroupAggrLeading_N1QLExprs()")

	var index1 = "index_agg"
	var bucketName = "default"

	ga, proj := basicGroupAggrN1QLExprs1()

	scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getScanAllNoFilter(), false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	tc.PrintScanResults(scanResults, "scanResults")

	ga, proj = basicGroupAggrN1QLExprs2()

	scanResults, err = secondaryindex.Scan3(index1, bucketName, indexScanAddress, getScanAllNoFilter(), false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	tc.PrintScanResults(scanResults, "scanResults")
	secondaryindex.UseClient = tmpclient
}

func basicGroupAggrN1QLExprs1() (*qc.GroupAggr, *qc.IndexProjection) {

	g1 := &qc.GroupKey{EntryKeyId: 3, KeyPos: -1, Expr: "\"Year \" || cover ((`default`.`Year`))"}
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
	}

	proj := &qc.IndexProjection{
		EntryKeys: []int64{3, 4, 5, 6},
	}

	return ga, proj
}

func basicGroupAggrN1QLExprs2() (*qc.GroupAggr, *qc.IndexProjection) {

	g1 := &qc.GroupKey{EntryKeyId: 3, KeyPos: -1, Expr: "SUBSTR(cover ((meta(`default`).`id`)), 0, 4)"}
	groups := []*qc.GroupKey{g1}

	a1 := &qc.Aggregate{AggrFunc: c.AGG_SUM, EntryKeyId: 4, KeyPos: -1,
		Expr: "cover ((`default`.`Sale`))"}

	a2 := &qc.Aggregate{AggrFunc: c.AGG_COUNT, EntryKeyId: 5, KeyPos: 2, Expr: ""}
	aggregates := []*qc.Aggregate{a1, a2}

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
	}

	proj := &qc.IndexProjection{
		EntryKeys: []int64{3, 4, 5},
	}

	return ga, proj
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

		scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getScanAllNoFilter(), false, false, proj, 0, 3, ga, c.SessionConsistency, nil)
		FailTestIfError(err, "Error in scan", t)
		tc.PrintScanResults(scanResults, "scanResults")
	}

	//non-leading
	{
		ga, proj := basicGroupAggr()

		ga.Group = ga.Group[1:]
		proj.EntryKeys = proj.EntryKeys[1:]

		scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getScanAllNoFilter(), false, false, proj, 0, 2, ga, c.SessionConsistency, nil)
		FailTestIfError(err, "Error in scan", t)
		tc.PrintScanResults(scanResults, "scanResults")
	}

}

func TestGroupAggrOffset(t *testing.T) {
	log.Printf("In TestGroupAggrOffset()")

	var index1 = "index_agg"
	var bucketName = "default"

	//leading
	{
		ga, proj := basicGroupAggr()

		scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getScanAllNoFilter(), false, false, proj, 1, 3, ga, c.SessionConsistency, nil)
		FailTestIfError(err, "Error in scan", t)
		tc.PrintScanResults(scanResults, "scanResults")
	}

	//non-leading
	{
		ga, proj := basicGroupAggr()

		ga.Group = ga.Group[1:]
		proj.EntryKeys = proj.EntryKeys[1:]

		scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getScanAllNoFilter(), false, false, proj, 1, 2, ga, c.SessionConsistency, nil)
		FailTestIfError(err, "Error in scan", t)
		tc.PrintScanResults(scanResults, "scanResults")
	}

}
