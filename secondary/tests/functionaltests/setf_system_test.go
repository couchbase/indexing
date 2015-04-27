package functionaltests

import (
	"errors"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/tests/framework/datautility"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
	tv "github.com/couchbase/indexing/secondary/tests/framework/validation"
	"log"
	"strings"
	"testing"
	"time"
)

func TestBuildDeferredAnotherBuilding(t *testing.T) {
	log.Printf("In TestBuildDeferredAnotherBuilding()")

	var bucketName = "default"
	var index1 = "id_company"
	var index2 = "id_age"

	e := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(e, "Error in DropAllSecondaryIndexes", t)
	docsToCreate := generateDocs(50000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	err := secondaryindex.CreateSecondaryIndexAsync(index1, bucketName, indexManagementAddress, "", []string{"company"}, false, nil, true, nil)
	FailTestIfError(err, "Error in creating the index", t)
	time.Sleep(1 * time.Second)

	err = secondaryindex.CreateSecondaryIndexAsync(index2, bucketName, indexManagementAddress, "", []string{"age"}, false, []byte("{\"defer_build\": true}"), true, nil)
	FailTestIfError(err, "Error in creating the index", t)

	err = secondaryindex.BuildIndex(index2, bucketName, indexManagementAddress, defaultIndexActiveTimeout)
	if err == nil {
		e := errors.New("Error excpected when build index while another build is in progress")
		FailTestIfError(e, "Error in TestBuildDeferredAnotherBuilding", t)
	} else {
		if strings.Contains(err.Error(), "Build Already In Progress") {
			log.Printf("Build index failed as expected: %v", err.Error())
		} else {
			log.Printf("Build index did not fail with expected error, instead failed with %v", err)
			e := errors.New("Build index did not fail")
			FailTestIfError(e, "Error in TestBuildDeferredAnotherBuilding", t)
		}
	}

	client, _ := secondaryindex.CreateClient(indexManagementAddress, "test1client")
	defnID, _ := secondaryindex.GetDefnID(client, bucketName, index1)
	e = secondaryindex.WaitTillIndexActive(defnID, client, defaultIndexActiveTimeout)
	if e != nil {
		FailTestIfError(e, "Error in WaitTillIndexActive", t)
	}

	time.Sleep(1 * time.Second)

	err = secondaryindex.BuildIndex(index2, bucketName, indexManagementAddress, defaultIndexActiveTimeout)
	FailTestIfError(err, "Error in BuildIndex in TestBuildDeferredAnotherBuilding", t)

	docScanResults := datautility.ExpectedScanResponse_string(docs, "company", "M", "V", 2)
	scanResults, err := secondaryindex.Range(index1, bucketName, indexScanAddress, []interface{}{"M"}, []interface{}{"V"}, 2, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)

	docScanResults = datautility.ExpectedScanResponse_float64(docs, "age", 30, 50, 1)
	scanResults, err = secondaryindex.Range(index2, bucketName, indexScanAddress, []interface{}{30}, []interface{}{50}, 1, true, defaultlimit, c.SessionConsistency, nil)
	FailTestIfError(err, "Error in scan", t)
	err = tv.Validate(docScanResults, scanResults)
	FailTestIfError(err, "Error in scan result validation", t)
}
