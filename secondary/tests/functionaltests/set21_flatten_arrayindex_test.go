package functionaltests

import (
	"log"
	"testing"

	c "github.com/couchbase/indexing/secondary/common"
	qc "github.com/couchbase/indexing/secondary/queryport/client"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	"gopkg.in/couchbase/gocb.v1"
)

func TestFlattenArrayIndexTestSetup(t *testing.T) {
	var bucket = "default"
	var arr_field = "friends"

	err := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(err, "Error in DropAllSecondaryIndexes", t)
	kvutility.FlushBucket(bucket, "", clusterconfig.Username, clusterconfig.Password, kvaddress)

	kvdocs := generateDocs(1000, "users_simplearray.prod")

	// Modify array docs for better distibution of duplicate array elements
	fn := func(item string, id int) map[string]interface{} {
		arr := make(map[string]interface{})
		arr["name"] = item
		arr["id"] = id
		arr["age"] = randomNum(20, 100)
		arr["email"] = randString(randomNum(10, 20)) + "@abcdefg.com"
		return arr
	}
	id := 0
	for _, v := range kvdocs {
		document := v.(map[string]interface{})
		array := document[arr_field].([]interface{})
		newArray := make([]interface{}, 0)
		for _, item := range array {
			val := item.(string)
			newArray = append(newArray, fn(val, id))
			id++
		}
		document[arr_field] = newArray
	}

	kvutility.SetKeyValues(kvdocs, bucket, "", clusterconfig.KVAddress)

	// Create primary index for scan validation
	n1qlstatement := "create primary index on default"
	_, err = tc.ExecuteN1QLStatement(kvaddress, clusterconfig.Username, clusterconfig.Password, bucket, n1qlstatement, false, gocb.RequestPlus)
	FailTestIfError(err, "Error in creating primary index", t)
}

func TestScanOnFlattenedAraryIndex(t *testing.T) {
	idx1 := "idx_flatten"
	bucket := "default"
	err := secondaryindex.CreateSecondaryIndex(idx1, bucket, indexManagementAddress, "",
		[]string{"company", "DISTINCT ARRAY FLATTEN_KEYS(v.name, v.age) for v in friends END", "balance"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	scans := make(qc.Scans, 1)

	filter1 := make([]*qc.CompositeElementFilter, 4)
	filter1[0] = &qc.CompositeElementFilter{Low: "A", High: "M", Inclusion: qc.Inclusion(uint32(3))}
	filter1[1] = &qc.CompositeElementFilter{Low: "M", High: "Z", Inclusion: qc.Inclusion(uint32(3))}
	filter1[2] = &qc.CompositeElementFilter{Low: int64(50), High: int64(100), Inclusion: qc.Inclusion(uint32(3))}
	filter1[3] = &qc.CompositeElementFilter{Low: "$1000", High: "$99999", Inclusion: qc.Inclusion(uint32(3))}
	scans[0] = &qc.Scan{Filter: filter1}

	scanResults, _, err := secondaryindex.Scan3(idx1, bucket, kvaddress, scans, false, false, nil, 0, defaultlimit, nil, c.SessionConsistency, nil)
	FailTestIfError(err, "Error during secondary index scan", t)

	// Scan using primary index
	n1qlEquivalent := "select meta().id from default USE INDEX(`#primary`) where default.company >= \"A\" AND default.company <= \"M\" AND " +
		"ANY v in default.friends SATISFIES v.name >= \"M\" and v.name <= \"Z\" AND v.age >= 50 AND v.age <= 100 END AND " +
		"default.balance >= \"$1000\" AND default.balance <= \"$99999\""

	scanResultsPrimary, err := tc.ExecuteN1QLStatement(kvaddress, clusterconfig.Username, clusterconfig.Password, bucket, n1qlEquivalent, true, gocb.RequestPlus)
	FailTestIfError(err, "Error while creating primary index", t)

	if len(scanResultsPrimary) != len(scanResults) {
		log.Printf("ScanResultsPrimary: %v", scanResultsPrimary)
		log.Printf("ScanResultsSecondary: %v", scanResults)
		t.Fatalf("Mismatch in scan results")
	}
	for _, doc := range scanResultsPrimary {
		docId := doc.(map[string]interface{})["id"].(string)
		if _, ok := scanResults[docId]; !ok {
			log.Printf("ScanResultsPrimary: %v", scanResultsPrimary)
			log.Printf("ScanResultsSecondary: %v", scanResults)
			t.Fatalf("Mismatch in scan results")
		}
	}
}

func TestGroupAggrFlattenArrayIndex(t *testing.T) {
	log.Printf("In TestGroupAggrArrayIndex()")

	tmpclient = secondaryindex.UseClient
	secondaryindex.UseClient = "gsi"

	defer func() {
		secondaryindex.UseClient = tmpclient
	}()

	var i1 = "ga_flatten_arr1"
	var i2 = "ga_flatten_arr2"
	var bucket = "default"
	var err error
	var n1qlEquivalent string

	err = secondaryindex.CreateSecondaryIndex(i1, bucket, indexManagementAddress, "",
		[]string{"company", "DISTINCT ARRAY FLATTEN_KEYS(v.name, v.age, v.email) for v in friends END", "age"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.CreateSecondaryIndex(i2, bucket, indexManagementAddress, "",
		[]string{"DISTINCT ARRAY FLATTEN_KEYS(v.name, v.age, v.email) for v in friends END", "company", "age"}, false, nil, true, defaultIndexActiveTimeout, nil)

	// S1: Array aggregate on non-array field, group by non-array field
	log.Printf("Scenario 1")
	n1qlEquivalent = "SELECT company as a, MIN(age) as b FROM default USE INDEX(`#primary`) " +
		" GROUP BY company"
	g1 := &qc.GroupKey{EntryKeyId: 6, KeyPos: 0}
	groups := []*qc.GroupKey{g1}

	a1 := &qc.Aggregate{AggrFunc: c.AGG_MIN, EntryKeyId: 7, KeyPos: 4}
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
	n1qlEquivalent = "SELECT x.name as a, MIN(d.company) as b, MAX(d.age) as c from default d " +
		" USE INDEX(`#primary`) UNNEST d.friends AS x GROUP BY x.name"
	g1 = &qc.GroupKey{EntryKeyId: 6, KeyPos: 1}
	groups = []*qc.GroupKey{g1}

	a1 = &qc.Aggregate{AggrFunc: c.AGG_MIN, EntryKeyId: 7, KeyPos: 0}
	a2 := &qc.Aggregate{AggrFunc: c.AGG_MAX, EntryKeyId: 8, KeyPos: 4}
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
	a2 = &qc.Aggregate{AggrFunc: c.AGG_MAX, EntryKeyId: 8, KeyPos: 4}
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
	n1qlEquivalent = "SELECT d.age as a, d.company as b, MIN(x.age) as c from default d " +
		"USE INDEX(`#primary`)  UNNEST d.friends AS x GROUP BY d.age, d.company"
	g1 = &qc.GroupKey{EntryKeyId: 6, KeyPos: 4}
	g2 := &qc.GroupKey{EntryKeyId: 7, KeyPos: 0}
	groups = []*qc.GroupKey{g1, g2}

	a1 = &qc.Aggregate{AggrFunc: c.AGG_MIN, EntryKeyId: 8, KeyPos: 2}
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
	n1qlEquivalent = "SELECT x.name as a, COUNT(x.age) as b from default d " +
		"USE INDEX(`#primary`)  UNNEST d.friends AS x  GROUP BY x.name"
	g1 = &qc.GroupKey{EntryKeyId: 6, KeyPos: 1}
	groups = []*qc.GroupKey{g1}

	a1 = &qc.Aggregate{AggrFunc: c.AGG_COUNT, EntryKeyId: 7, KeyPos: 2}
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
	n1qlEquivalent = "SELECT COUNT(x.name) as a, MIN(x.age) as b, MAX(x.age) as c, COUNTN(x.age) as d, " +
		" SUM(x.age) as e from default d USE INDEX(`#primary`)  UNNEST d.friends AS x"

	a1 = &qc.Aggregate{AggrFunc: c.AGG_COUNT, EntryKeyId: 4, KeyPos: 1}
	a2 = &qc.Aggregate{AggrFunc: c.AGG_MIN, EntryKeyId: 5, KeyPos: 2}
	a3 := &qc.Aggregate{AggrFunc: c.AGG_MAX, EntryKeyId: 6, KeyPos: 2}
	a4 := &qc.Aggregate{AggrFunc: c.AGG_COUNTN, EntryKeyId: 7, KeyPos: 2}
	a5 := &qc.Aggregate{AggrFunc: c.AGG_SUM, EntryKeyId: 8, KeyPos: 2}

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
	n1qlEquivalent = "SELECT SUM(DISTINCT d.age) as a, d.company as b, x.email as c " +
		"from default d USE INDEX(`#primary`)  UNNEST d.friends AS x GROUP BY d.company, x.email"
	g1 = &qc.GroupKey{EntryKeyId: 7, KeyPos: 0}
	g2 = &qc.GroupKey{EntryKeyId: 8, KeyPos: 3}
	groups = []*qc.GroupKey{g1, g2}

	a1 = &qc.Aggregate{AggrFunc: c.AGG_SUM, EntryKeyId: 6, KeyPos: 4, Distinct: true}
	aggregates = []*qc.Aggregate{a1}

	ga = &qc.GroupAggr{
		Name:             "S7",
		Group:            groups,
		Aggrs:            aggregates,
		AllowPartialAggr: true,
	}
	proj = &qc.IndexProjection{
		EntryKeys: []int64{6, 7, 8},
	}
	executeGroupAggrTest(ga, proj, n1qlEquivalent, i1, t)

	// S8: Distinct aggregate on non-array field, group by array field
	log.Printf("Scenario 8")
	n1qlEquivalent = "SELECT x.name as a, COUNT(DISTINCT d.company) as b from default d " +
		" USE INDEX(`#primary`)  UNNEST d.friends AS x GROUP BY x.name"
	g1 = &qc.GroupKey{EntryKeyId: 6, KeyPos: 1}
	groups = []*qc.GroupKey{g1}

	a1 = &qc.Aggregate{AggrFunc: c.AGG_COUNT, EntryKeyId: 8, KeyPos: 0, Distinct: true}
	aggregates = []*qc.Aggregate{a1}

	ga = &qc.GroupAggr{
		Name:             "S8",
		Group:            groups,
		Aggrs:            aggregates,
		AllowPartialAggr: true,
	}
	proj = &qc.IndexProjection{
		EntryKeys: []int64{6, 8},
	}
	executeGroupAggrTest(ga, proj, n1qlEquivalent, i1, t)

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
	n1qlEquivalent = "SELECT d.company as a, COUNT(DISTINCT x.name) as b from default d " +
		"USE INDEX(`#primary`) UNNEST d.friends AS x GROUP BY d.company"
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
	n1qlEquivalent = "SELECT  x.name as a, COUNT(DISTINCT x.age) as b from default d " +
		"USE INDEX(`#primary`)  UNNEST d.friends AS x GROUP BY x.name"
	g1 = &qc.GroupKey{EntryKeyId: 6, KeyPos: 0}
	groups = []*qc.GroupKey{g1}

	a1 = &qc.Aggregate{AggrFunc: c.AGG_COUNT, EntryKeyId: 7, KeyPos: 1, Distinct: true}
	aggregates = []*qc.Aggregate{a1}

	ga = &qc.GroupAggr{
		Name:             "S11",
		Group:            groups,
		Aggrs:            aggregates,
		AllowPartialAggr: true,
	}
	proj = &qc.IndexProjection{
		EntryKeys: []int64{6, 7},
	}
	executeGroupAggrTest(ga, proj, n1qlEquivalent, i2, t)

	// S12: Distinct aggregate on array field, no group
	log.Printf("Scenario 12")
	n1qlEquivalent = "SELECT COUNT(DISTINCT x.name) as a from default d " +
		"USE INDEX(`#primary`)  UNNEST d.friends AS x"

	a1 = &qc.Aggregate{AggrFunc: c.AGG_COUNT, EntryKeyId: 7, KeyPos: 0, Distinct: true}
	aggregates = []*qc.Aggregate{a1}

	ga = &qc.GroupAggr{
		Name:             "S12",
		Group:            nil,
		Aggrs:            aggregates,
		AllowPartialAggr: true,
	}
	proj = &qc.IndexProjection{
		EntryKeys: []int64{7},
	}
	executeGroupAggrTest(ga, proj, n1qlEquivalent, i2, t)

	//S13: Test for OnePerPrimaryKey
	log.Printf("Scenario 13")

	stmt := "CREATE INDEX test_oneperprimarykey ON default(ALL ARRAY FLATTEN_KEYS(v1.name, v1.age, v1.email) FOR v1 IN friends END, company, age)"
	_, err = tc.ExecuteN1QLStatement(kvaddress, clusterconfig.Username, clusterconfig.Password, bucket, stmt, false, gocb.NotBounded)
	FailTestIfError(err, "Error in index test_oneperprimarykey", t)

	n1qlEquivalent = "SELECT COUNT(company) as a, SUM(age) as b FROM default USE INDEX(`#primary`) " +
		" WHERE ANY v1 IN friends SATISFIES v1.name == \"Aaron\" END "

	a1 = &qc.Aggregate{AggrFunc: c.AGG_COUNT, EntryKeyId: 7, KeyPos: 3}
	a2 = &qc.Aggregate{AggrFunc: c.AGG_SUM, EntryKeyId: 8, KeyPos: 4}
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

	executeGroupAggrTest2(scans, ga, proj, n1qlEquivalent, "test_oneperprimarykey", t)
}
