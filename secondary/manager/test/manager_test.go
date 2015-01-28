// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package test

import (
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/manager"
	"github.com/couchbase/indexing/secondary/manager/client"
	util "github.com/couchbase/indexing/secondary/manager/test/util"
	"os"
	"testing"
	"time"
)

// For this test, use Index Defn Id from 100 - 110
func TestIndexManager(t *testing.T) {

	common.LogEnable()
	common.SetLogLevel(common.LogLevelTrace)
	os.MkdirAll("./data/", os.ModePerm)

	cfg := common.SystemConfig.SectionConfig("indexer", true /*trim*/)
	cfg.Set("storage_dir", common.ConfigValue{"./data/", "metadata file path", "./"})

	common.Infof("Start Index Manager *********************************************************")

	var msgAddr = "localhost:9884"
	factory := new(util.TestDefaultClientFactory)
	env := new(util.TestDefaultClientEnv)
	admin := manager.NewProjectorAdmin(factory, env, nil)
	mgr, err := manager.NewIndexManagerInternal(msgAddr, "localhost:"+manager.COORD_MAINT_STREAM_PORT, admin, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	common.Infof("Cleanup Test *********************************************************")

	cleanupTest(mgr, t)

	common.Infof("Setup Initial Data *********************************************************")

	//setupInitialData(mgr, t)

	common.Infof("Start Provider *********************************************************")

	var providerId = "TestMetadataProvider"
	provider, err := client.NewMetadataProvider(providerId)
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	provider.WatchMetadata(msgAddr)

	common.Infof("Test Iterator *********************************************************")

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

	common.Infof("Cleanup Test *********************************************************")

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
		common.Infof("cleanupTest() :  cannot find index defn %d.  No cleanup ...", id)
	} else {
		common.Infof("cleanupTest.cleanupTest() :  found index defn %d.  Cleaning up ...", id)

		mgr.HandleDeleteIndexDDL(id)

		_, err := mgr.GetIndexDefnById(id)
		if err == nil {
			common.Infof("cleanupTest() :  cannot cleanup index defn %d.  ...", id)
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
		common.Infof("key during iteration %s", key)
		count++
	}

	if expectedCount != -1 && expectedCount != count {
		t.Fatal("ExpectedCount does not match with number of index defn in repository")
	}
}
