package functionaltests

import (
	c "github.com/couchbase/indexing/secondary/common"
	qc "github.com/couchbase/indexing/secondary/queryport/client"
	//tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/datautility"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	//tv "github.com/couchbase/indexing/secondary/tests/framework/validation"
	"log"
	"testing"
	"time"
)

func TestMultiScanGroupsAggrs1(t *testing.T) {
	log.Printf("In TestMultiScanGroupsAggrs1()")

	var index1 = "index_company"
	var bucketName = "default"

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

	err := secondaryindex.CreateSecondaryIndex(index1, bucketName, indexManagementAddress, "", []string{"company"}, false, nil, true, defaultIndexActiveTimeout, nil)
	FailTestIfError(err, "Error in creating the index", t)

	_, err = secondaryindex.Scan3(index1, bucketName, indexScanAddress, getScanAllNoFilter(), false, false, nil, 0, defaultlimit, basicGroupAggr(),
		c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	//tc.PrintScanResults(scanResults, "scanResults")
}

func TestMultiScanGroupsAggrs2(t *testing.T) {
	log.Printf("In TestMultiScanGroupsAggrs2()")

	var index1 = "index_company"
	var bucketName = "default"

	_, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getScanAllNoFilter(), false, false, nil, 0, defaultlimit, basicGroupAggr(),
		c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	//tc.PrintScanResults(scanResults, "scanResults")
}

func basicGroupAggr() *qc.GroupAggr {
	groups := make([]*qc.GroupKey, 1)
	g := &qc.GroupKey{
		EntryKeyId: 0,
		KeyPos:     0,
		Expr:       "company",
	}
	groups[0] = g

	//Aggrs
	aggregates := make([]*qc.Aggregate, 1)
	a := &qc.Aggregate{
		AggrFunc:   c.AGG_COUNT,
		EntryKeyId: int32(0),
		KeyPos:     int32(0),
		Expr:       "company",
		Distinct:   false,
	}
	aggregates[0] = a

	dependsOnIndexKeys := make([]int32, 1)
	dependsOnIndexKeys[0] = int32(0)

	ga := &qc.GroupAggr{
		Name:                "testGrpAggr2",
		Group:               groups,
		Aggrs:               aggregates,
		DependsOnIndexKeys:  dependsOnIndexKeys,
		DependsOnPrimaryKey: false,
	}
	return ga
}
