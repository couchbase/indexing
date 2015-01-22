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
	"fmt"
	c "github.com/couchbase/gometa/common"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/manager"
	"github.com/couchbase/indexing/secondary/manager/client"
	util "github.com/couchbase/indexing/secondary/manager/test/util"
	"testing"
	"time"
)

var newDefnId, newDefnId2 common.IndexDefnId
var gMgr *manager.IndexManager = nil

type notifier struct {
	hasCreated bool
	hasDeleted bool
}

// For this test, use Index Defn Id from 100 - 110
func TestMetadataProvider(t *testing.T) {

	common.LogEnable()
	common.SetLogLevel(common.LogLevelTrace)

	common.Infof("Start Index Manager *********************************************************")

	var msgAddr = "localhost:9884"
	factory := new(util.TestDefaultClientFactory)
	env := new(util.TestDefaultClientEnv)
	admin := manager.NewProjectorAdmin(factory, env, nil)
	mgr, err := manager.NewIndexManagerInternal(msgAddr, "localhost:"+manager.COORD_MAINT_STREAM_PORT, admin)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()
	gMgr = mgr

	common.Infof("Cleanup Test *********************************************************")

	cleanupTest(mgr, t)

	common.Infof("Setup Initial Data *********************************************************")

	setupInitialData(mgr, t)

	common.Infof("Start Provider *********************************************************")

	var providerId = "TestMetadataProvider"
	provider, err := client.NewMetadataProvider(providerId)
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	provider.WatchMetadata(msgAddr)

	// the gometa server is running in the same process as MetadataProvider (client).  So sleep to
	// make sure that the server has a chance to finish off initialization, since the client may
	// be ready, but the server is not.
	time.Sleep(time.Duration(1000) * time.Millisecond)

	common.Infof("Verify Initial Data *********************************************************")

	meta := lookup(provider, common.IndexDefnId(100))
	if meta == nil {
		t.Fatal("Cannot find Index Defn 100 from MetadataProvider")
	}
	common.Infof("found Index Defn 100")
	if len(meta.Instances) == 0 || meta.Instances[0].State != common.INDEX_STATE_READY {
		t.Fatal("Index Defn 100 state is not ready")
	}
	if meta.Instances[0].Endpts[0] != "localhost:"+manager.COORD_MAINT_STREAM_PORT {
		t.Fatal("Index Defn 100 state is not ready")
	}

	meta = lookup(provider, common.IndexDefnId(101))
	if meta == nil {
		t.Fatal("Cannot find Index Defn 101 from MetadataProvider")
	}
	common.Infof("found Index Defn 101")
	if len(meta.Instances) == 0 || meta.Instances[0].State != common.INDEX_STATE_READY {
		t.Fatal("Index Defn 101 state is not ready")
	}
	if meta.Instances[0].Endpts[0] != "localhost:"+manager.COORD_MAINT_STREAM_PORT {
		t.Fatal("Index Defn 100 state is not ready")
	}

	common.Infof("Change Data *********************************************************")

	notifier := &notifier{hasCreated: false, hasDeleted: false}
	mgr.RegisterNotifier(notifier)

	// Create Index with deployment plan (deferred)
	plan := make(map[string]interface{})
	plan["nodes"] = []string{msgAddr}
	plan["defer_build"] = true
	newDefnId, err := provider.CreateIndexWithPlan("metadata_provider_test_102", "Default", common.ForestDB,
		common.N1QL, "Testing", "TestingWhereExpr", []string{"Testing"}, false, plan)
	if err != nil {
		t.Fatal("Cannot create Index Defn 102 through MetadataProvider" + err.Error())
	}
	input := make([]common.IndexDefnId, 1)
	input[0] = newDefnId
	if err := provider.BuildIndexes(msgAddr, input); err != nil {
		t.Fatal("Cannot build Index Defn : %v", err)
	}
	common.Infof("done creating index 102")

	// Drop a seeded index (created during setup step)
	if err := provider.DropIndex(common.IndexDefnId(101), msgAddr); err != nil {
		t.Fatal("Cannot drop Index Defn 101 through MetadataProvider")
	}
	common.Infof("done dropping index 101")

	// Create Index (immediate).
	newDefnId2, err := provider.CreateIndex("metadata_provider_test_103", "Default", common.ForestDB,
		common.N1QL, "Testing", "TestingWhereExpr", msgAddr, []string{"Testing"}, false)
	if err != nil {
		t.Fatal("Cannot create Index Defn 103 through MetadataProvider")
	}
	common.Infof("done creating index 103")

	// Update instance (set state to ACTIVE) 
	if err := mgr.UpdateIndexInstance("Default", newDefnId2, common.INDEX_STATE_ACTIVE, common.StreamId(100), ""); err != nil {
		t.Fatal("Fail to update index instance")
	}
	common.Infof("done updating index 103")

	// Update instance (set error string)
	if err := mgr.UpdateIndexInstance("Default", newDefnId2, common.INDEX_STATE_NIL, common.NIL_STREAM, "testing"); err != nil {
		t.Fatal("Fail to update index instance")
	}
	common.Infof("done updating index 103")
	
	// Create Index (immediate).  This index is supposed to fail by OnIndexBuild()
	if _, err := provider.CreateIndex("metadata_provider_test_104", "Default", common.ForestDB,
		common.N1QL, "Testing", "Testing", msgAddr, []string{"Testing"}, false); err == nil {
		t.Fatal("Error does not propage for create Index Defn 104 through MetadataProvider")
	}
	common.Infof("done creating index 104")

	common.Infof("Verify Changed Data *********************************************************")

	if lookup(provider, common.IndexDefnId(100)) == nil {
		t.Fatal("Cannot find Index Defn 100 from MetadataProvider")
	}
	common.Infof("found Index Defn 100")

	if lookup(provider, common.IndexDefnId(101)) != nil {
		t.Fatal("Found Deleted Index Defn 101 from MetadataProvider")
	}
	common.Infof("cannot found deleted Index Defn 101")

	if meta = lookup(provider, newDefnId); meta == nil {
		t.Fatal(fmt.Sprintf("Cannot Found Index Defn %d from MetadataProvider", newDefnId))
	} else {
		common.Infof("Found Index Defn %d", newDefnId)
		common.Infof("meta.Instance %v", meta.Instances)
		if meta.Instances[0].Endpts[0] != "localhost:"+manager.COORD_MAINT_STREAM_PORT {
			t.Fatal(fmt.Sprintf("Index Defn %v has incorrect endpoint", newDefnId))
		}
		if meta.Definition.WhereExpr != "TestingWhereExpr" {
			t.Fatal(fmt.Sprintf("WhereExpr is missing in Index Defn %v", newDefnId))
		}
		if meta.Instances[0].State != common.INDEX_STATE_INITIAL {
			t.Fatal(fmt.Sprintf("Index Defn %v has incorrect state", newDefnId))
		}
	}

	if meta = lookup(provider, newDefnId2); meta == nil {
		t.Fatal(fmt.Sprintf("Cannot Found Index Defn %d from MetadataProvider", newDefnId2))
	} else {
		common.Infof("Found Index Defn %d", newDefnId2)
		common.Infof("meta.Instance %v", meta.Instances)
		if meta.Instances[0].Endpts[0] != "localhost:"+manager.COORD_MAINT_STREAM_PORT {
			t.Fatal(fmt.Sprintf("Index Defn %v has incorrect endpoint", newDefnId2))
		}
		if meta.Definition.WhereExpr != "TestingWhereExpr" {
			t.Fatal(fmt.Sprintf("WhereExpr is missing in Index Defn %v", newDefnId2))
		}
		if meta.Instances[0].State != common.INDEX_STATE_ACTIVE {
			t.Fatal(fmt.Sprintf("Index Defn %v has incorrect state", newDefnId2))
		}
		if meta.Instances[0].Error != "testing" {
			t.Fatal(fmt.Sprintf("Index Defn %v has incorrect error string", newDefnId2))
		}
	}

	if !notifier.hasCreated {
		t.Fatal(fmt.Sprintf("Does not recieve notification for creating index %s", newDefnId))
	}
	common.Infof(fmt.Sprintf("Recieve notification for creating index %v", newDefnId))

	if !notifier.hasDeleted {
		t.Fatal("Does not recieve notification for deleting index 101")
	}
	common.Infof("Recieve notification for deleting index 101")

	time.Sleep(time.Duration(1000) * time.Millisecond)

	common.Infof("Cleanup Test *********************************************************")

	provider.UnwatchMetadata(msgAddr)
	cleanupTest(mgr, t)
	cleanSingleIndex(mgr, t, newDefnId)
	cleanSingleIndex(mgr, t, newDefnId2)
	time.Sleep(time.Duration(1000) * time.Millisecond)
}

func lookup(provider *client.MetadataProvider, id common.IndexDefnId) *client.IndexMetadata {

	metas := provider.ListIndex()

	for _, meta := range metas {
		if meta.Definition.DefnId == id {
			return meta
		}
	}

	return nil
}

func setupInitialData(mgr *manager.IndexManager, t *testing.T) {

	// Add a new index definition : 100
	idxDefn := &common.IndexDefn{
		DefnId:          common.IndexDefnId(100),
		Name:            "metadata_provider_test_100",
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
		Name:            "metadata_provider_test_101",
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
}

// clean up
func cleanupTest(mgr *manager.IndexManager, t *testing.T) {

	cleanSingleIndex(mgr, t, common.IndexDefnId(100))
	cleanSingleIndex(mgr, t, common.IndexDefnId(101))
}

func cleanSingleIndex(mgr *manager.IndexManager, t *testing.T, id common.IndexDefnId) {

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

func (n *notifier) OnIndexCreate(defn *common.IndexDefn) error {

	if defn.Name == "metadata_provider_test_104" {
		return &c.RecoverableError{Reason: "do not allow creating metadata_provider_test_104"}
	}

	n.hasCreated = true
	return nil
}

func (n *notifier) OnIndexDelete(common.IndexDefnId) error {
	n.hasDeleted = true
	return nil
}

func (n *notifier) OnIndexBuild(id []common.IndexDefnId) error {
	err := gMgr.UpdateIndexInstance("Default", id[0], common.INDEX_STATE_INITIAL, common.StreamId(100), "")
	return err
}
