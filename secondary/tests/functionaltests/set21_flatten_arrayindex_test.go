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

	kvdocs := generateDocs(100, "users_simplearray.prod")

	// Modify array docs for better distibution of duplicate array elements
	fn := func(item string, id int) map[string]interface{} {
		arr := make(map[string]interface{})
		arr["name"] = item
		arr["id"] = id
		arr["age"] = randomNum(20, 100)
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
	_, err = tc.ExecuteN1QLStatement(kvaddress, clusterconfig.Username, clusterconfig.Password, bucket, n1qlstatement, false, gocb.NotBounded)
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
	filter1[3] = &qc.CompositeElementFilter{Low: "$1000", High: "$99999", Inclusion: qc.Inclusion(uint32(1))}
	scans[0] = &qc.Scan{Filter: filter1}

	scanResults, _, err := secondaryindex.Scan3(idx1, bucket, kvaddress, scans, false, false, nil, 0, defaultlimit, nil, c.SessionConsistency, nil)
	FailTestIfError(err, "Error during secondary index scan", t)

	// Scan using primary index
	n1qlEquivalent := "select meta().id from default USE INDEX(`#primary`) where default.company >= \"A\" AND default.company <= \"M\" AND " +
		"ANY v in default.friends SATISFIES v.name >= \"M\" and v.name <= \"Z\" AND v.age >= 50 AND v.age <= 100 END AND " +
		"default.balance >= \"$1000\""

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
