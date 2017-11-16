package functionaltests

import (
	c "github.com/couchbase/indexing/secondary/common"
	qc "github.com/couchbase/indexing/secondary/queryport/client"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
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

	ga, proj := basicGroupAggr()

	scanResults, err1 := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getScanAllNoFilter(), false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
	FailTestIfError(err1, "Error in scan", t)
	tc.PrintScanResults(scanResults, "scanResults")
}

func TestMultiScanGroupsAggrs2(t *testing.T) {
	log.Printf("In TestMultiScanGroupsAggrs2()")

	var index1 = "index_company"
	var bucketName = "default"

	ga, proj := basicGroupAggr()

	scanResults, err := secondaryindex.Scan3(index1, bucketName, indexScanAddress, getScanAllNoFilter(), false, false, proj, 0, defaultlimit, ga, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	tc.PrintScanResults(scanResults, "scanResults")
}

func basicGroupAggr() (*qc.GroupAggr, *qc.IndexProjection) {
	groups := make([]*qc.GroupKey, 1)
	g := &qc.GroupKey{
		EntryKeyId: 1,
		KeyPos:     0,
	}
	groups[0] = g

	//Aggrs
	aggregates := make([]*qc.Aggregate, 1)
	a := &qc.Aggregate{
		AggrFunc:   c.AGG_COUNT,
		EntryKeyId: 2,
		KeyPos:     0,
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

	entry := make([]int64, 2)
	entry[0] = 1
	entry[1] = 2

	proj := &qc.IndexProjection{
		EntryKeys: entry,
	}

	return ga, proj
}
