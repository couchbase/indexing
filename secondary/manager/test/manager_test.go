// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package test

import (
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/manager"
	"github.com/couchbase/indexing/secondary/manager/client"
	util "github.com/couchbase/indexing/secondary/manager/test/util"
	"os"
	"testing"
	"time"
)

// For this test, use Index Defn Id from 100 - 110
func TestIndexManager(t *testing.T) {

	logging.SetLogLevel(logging.Trace)
	os.MkdirAll("./data/", os.ModePerm)

	cfg := common.SystemConfig.SectionConfig("indexer", true /*trim*/)
	cfg.Set("storage_dir", common.ConfigValue{"./data/", "metadata file path", "./"})

	logging.Infof("Start Index Manager *********************************************************")

	var msgAddr = "localhost:9884"
	factory := new(util.TestDefaultClientFactory)
	env := new(util.TestDefaultClientEnv)
	admin := manager.NewProjectorAdmin(factory, env, nil)
	mgr, err := manager.NewIndexManagerInternal(msgAddr, "localhost:"+manager.COORD_MAINT_STREAM_PORT, admin, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	logging.Infof("Cleanup Test *********************************************************")

	cleanupTest(mgr, t)

	logging.Infof("Setup Initial Data *********************************************************")

	//setupInitialData(mgr, t)

	logging.Infof("Start Provider *********************************************************")

	var providerId = "TestMetadataProvider"
	provider, err := client.NewMetadataProvider(providerId)
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	provider.WatchMetadata(msgAddr)

	logging.Infof("Test Iterator *********************************************************")

	runIterator(mgr, t, 0)

	plan := make(map[string]interface{})
	plan["nodes"] = []string{msgAddr}
	plan["defer_build"] = true
	newDefnId101, err := provider.CreateIndexWithPlan("manager_test_101", "Default", common.ForestDB,
		common.N1QL, "Testing", "TestingWhereExpr", []string{"Testing"}, false, plan)
	if err != nil {
		t.Fatal("Cannot create Index Defn 101 through MetadataProvider")
	}
	runIterator(mgr, t, 1)

	newDefnId102, err := provider.CreateIndexWithPlan("manager_test_102", "Default", common.ForestDB,
		common.N1QL, "Testing", "TestingWhereExpr", []string{"Testing"}, false, plan)
	if err != nil {
		t.Fatal("Cannot create Index Defn 102 through MetadataProvider")
	}
	runIterator(mgr, t, 2)

	logging.Infof("Cleanup Test *********************************************************")

	provider.UnwatchMetadata(msgAddr)
	cleanSingleIndex_managerTest(mgr, t, newDefnId101)
	cleanSingleIndex_managerTest(mgr, t, newDefnId102)
	time.Sleep(time.Duration(1000) * time.Millisecond)
}

func setupInitialData_managerTest(mgr *manager.IndexManager, t *testing.T) {

	// Add a new index definition : 100
	idxDefn := &common.IndexDefn{
		DefnId:          common.IndexDefnId(100),
		Name:            "index_manager_test_100",
		Using:           common.ForestDB,
		Bucket:          "Default",
		IsPrimary:       false,
		SecExprs:        []string{"Testing"},
		ExprType:        common.N1QL,
		PartitionScheme: common.HASH,
		PartitionKey:    "Testing"}

	err := mgr.HandleCreateIndexDDL(idxDefn)
	if err != nil {
		t.Fatal(err)
	}

	// Add a new index definition : 101
	idxDefn = &common.IndexDefn{
		DefnId:          common.IndexDefnId(101),
		Name:            "index_manager_test_101",
		Using:           common.ForestDB,
		Bucket:          "Default",
		IsPrimary:       false,
		SecExprs:        []string{"Testing"},
		ExprType:        common.N1QL,
		PartitionScheme: common.HASH,
		PartitionKey:    "Testing"}

	err = mgr.HandleCreateIndexDDL(idxDefn)
	if err != nil {
		t.Fatal(err)
	}

	err = mgr.UpdateIndexInstance("Default", common.IndexDefnId(101), common.INDEX_STATE_ACTIVE, common.StreamId(0), "")
	if err != nil {
		util.TT.Fatal(err)
	}

	err = mgr.UpdateIndexInstance("Default", common.IndexDefnId(102), common.INDEX_STATE_ACTIVE, common.StreamId(0), "")
	if err != nil {
		util.TT.Fatal(err)
	}
}

func cleanSingleIndex_managerTest(mgr *manager.IndexManager, t *testing.T, id common.IndexDefnId) {

	_, err := mgr.GetIndexDefnById(id)
	if err != nil {
		logging.Infof("cleanupTest() :  cannot find index defn %d.  No cleanup ...", id)
	} else {
		logging.Infof("cleanupTest.cleanupTest() :  found index defn %d.  Cleaning up ...", id)

		mgr.HandleDeleteIndexDDL(id)

		_, err := mgr.GetIndexDefnById(id)
		if err == nil {
			logging.Infof("cleanupTest() :  cannot cleanup index defn %d.  ...", id)
		}
	}
}

func runIterator(mgr *manager.IndexManager, t *testing.T, expectedCount int) {

	metaIter, err := mgr.NewIndexDefnIterator()
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	for key, _, err := metaIter.Next(); err == nil; key, _, err = metaIter.Next() {
		logging.Infof("key during iteration %s", key)
		count++
	}

	if expectedCount != -1 && expectedCount != count {
		t.Fatal("ExpectedCount does not match with number of index defn in repository")
	}
}
