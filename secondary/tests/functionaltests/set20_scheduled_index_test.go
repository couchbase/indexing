package functionaltests

import (
	"log"
	"testing"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/queryport/client"
	tc "github.com/couchbase/indexing/secondary/tests/framework/common"
	"github.com/couchbase/indexing/secondary/tests/framework/kvutility"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
)

// Create multiple indexes Asynchronously atleast one of them will be scheduled and wait till it is created.
func TestScheduleIndexBasic(t *testing.T) {
	log.Printf("In TestMultipleDeferredIndexes_BuildTogether()")

	var client *client.GsiClient
	var bucketName = "default"
	var index1 = "id_company"
	var index2 = "id_age"
	var index3 = "id_gender"
	var index4 = "id_isActive"

	err := secondaryindex.DropAllSecondaryIndexes(indexManagementAddress)
	FailTestIfError(err, "Error in DropAllSecondaryIndexes", t)

	// Load data
	docsToCreate := generateDocs(50000, "users.prod")
	UpdateKVDocs(docsToCreate, docs)
	log.Printf("Setting JSON docs in KV")
	kvutility.SetKeyValues(docsToCreate, "default", "", clusterconfig.KVAddress)

	// Disable background index creation to verify if some indexes are in SCHEDULED state
	err = secondaryindex.ChangeIndexerSettings("indexer.debug.enableBackgroundIndexCreation", false, clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	// Launch in another go routine as waitForScheduledIndex will block the goroutines creating scheduled index
	log.Printf("Creating indexes Asynchronously")
	go func() {
		indexNameToFieldMap := make(map[string][]string, 4)
		indexNameToFieldMap[index1] = []string{"company"}
		indexNameToFieldMap[index2] = []string{"age"}
		indexNameToFieldMap[index3] = []string{"gender"}
		indexNameToFieldMap[index4] = []string{"isActive"}

		errList := secondaryindex.CreateIndexesConcurrently(bucketName, indexManagementAddress, indexNameToFieldMap, nil, nil, nil, true, nil)
		for _, err := range errList {
			log.Printf("Error %v Observed when creating index", err)
		}
	}()

	// Get all definition IDs
	log.Printf("Finding definition IDs for all indexes")
	client, err = secondaryindex.GetOrCreateClient(indexManagementAddress, "")
	FailTestIfError(err, "Unable to get or create client", t)
	retryCount := 0
retry:
	defnIDMap := secondaryindex.GetDefnIdsDefault(client, bucketName, []string{index1, index2, index3, index4})
	defnIDs := make([]uint64, 0)
	for index, defnID := range defnIDMap {
		if defnID == 0 {
			if retryCount >= 5 {
				t.Fatalf("Index Definition not available for %v", index)
			}
			retryCount++
			time.Sleep(3 * time.Second)
			goto retry
		}
		defnIDs = append(defnIDs, defnID)
	}

	// Print status of indexes.
	log.Printf("Status of all indexes")
	indexStateMap, err := secondaryindex.GetStatusOfAllIndexes(indexManagementAddress)
	tc.HandleError(err, "Error while fetching status of indexes")

	// Throw error if no index is in scheduled state
	anyScheduledIndex := false
	for idxName, idxState := range indexStateMap {
		log.Printf("Index %s is in state %v", idxName, idxState)
		if idxState == c.INDEX_STATE_SCHEDULED {
			anyScheduledIndex = true
			break
		}
	}

	if !anyScheduledIndex {
		t.Fatalf("Found no indexes in scheduled state")
	}

	// Enable Creation of indexes for scheduled indexes to proceed with creation.
	err = secondaryindex.ChangeIndexerSettings("indexer.debug.enableBackgroundIndexCreation", true, clusterconfig.Username, clusterconfig.Password, kvaddress)
	tc.HandleError(err, "Error in ChangeIndexerSettings")

	// Wait till all indexes are created.
	log.Printf("Waiting for all indexes to become active")
	err = secondaryindex.WaitTillAllIndexesActive(defnIDs, client, defaultIndexActiveTimeout)
	if err != nil {
		FailTestIfError(err, "Error in WaitTillIndexActive", t)
	}
}
