// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package test

import (
	"fmt"
	c "github.com/couchbase/gometa/common"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/manager"
	"github.com/couchbase/indexing/secondary/manager/client"
	util "github.com/couchbase/indexing/secondary/manager/test/util"
	"os"
	"testing"
	"time"
)

var newDefnId, newDefnId2 common.IndexDefnId
var gMgr *manager.IndexManager = nil
var metadata_provider_test_done = make(chan bool)

type notifier struct {
	hasCreated bool
	hasDeleted bool
}

// For this test, use Index Defn Id from 100 - 110
func TestMetadataProvider(t *testing.T) {

	logging.SetLogLevel(logging.Trace)

	cfg := common.SystemConfig.SectionConfig("indexer", true /*trim*/)
	cfg.Set("storage_dir", common.ConfigValue{"./data/", "metadata file path", "./"})
	os.MkdirAll("./data/", os.ModePerm)

	logging.Infof("Start Index Manager *********************************************************")

	var msgAddr = "localhost:9884"
	var httpAddr = "localhost:9885"
	factory := new(util.TestDefaultClientFactory)
	env := new(util.TestDefaultClientEnv)
	admin := manager.NewProjectorAdmin(factory, env, nil)
	addrPrv := util.NewFakeAddressProvider(msgAddr, httpAddr)
	mgr, err := manager.NewIndexManagerInternal(addrPrv, admin, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()
	gMgr = mgr

	logging.Infof("Cleanup Test *********************************************************")

	cleanupTest(mgr, t)

	logging.Infof("Setup Initial Data *********************************************************")

	setupInitialData(mgr, t)

	logging.Infof("Start Provider *********************************************************")

	var providerId = "TestMetadataProvider"
	provider, err := client.NewMetadataProvider(providerId)
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	provider.SetTimeout(int64(time.Second) * 15)
	indexerId, err := provider.WatchMetadata(msgAddr)
	if err != nil {
		t.Fatal(err)
	}

	// the gometa server is running in the same process as MetadataProvider (client).  So sleep to
	// make sure that the server has a chance to finish off initialization, since the client may
	// be ready, but the server is not.
	time.Sleep(time.Duration(1000) * time.Millisecond)

	logging.Infof("Verify Initial Data *********************************************************")

	meta := lookup(provider, common.IndexDefnId(100))
	if meta == nil {
		t.Fatal("Cannot find Index Defn 100 from MetadataProvider")
	}
	logging.Infof("found Index Defn 100")
	if len(meta.Instances) == 0 || meta.Instances[0].State != common.INDEX_STATE_READY {
		t.Fatal("Index Defn 100 state is not ready")
	}
	if meta.Instances[0].IndexerId != indexerId {
		t.Fatal("Index Defn 100 state is not ready")
	}

	meta = lookup(provider, common.IndexDefnId(101))
	if meta == nil {
		t.Fatal("Cannot find Index Defn 101 from MetadataProvider")
	}
	logging.Infof("found Index Defn 101")
	if len(meta.Instances) == 0 || meta.Instances[0].State != common.INDEX_STATE_READY {
		t.Fatal("Index Defn 101 state is not ready")
	}
	if meta.Instances[0].IndexerId != indexerId {
		t.Fatal("Index Defn 100 state is not ready")
	}

	logging.Infof("Change Data *********************************************************")

	notifier := &notifier{hasCreated: false, hasDeleted: false}
	mgr.RegisterNotifier(notifier)

	// Create Index with deployment plan (deferred)
	plan := make(map[string]interface{})
	plan["nodes"] = []interface{}{msgAddr}
	plan["defer_build"] = true
	newDefnId, err := provider.CreateIndexWithPlan("metadata_provider_test_102", "Default", common.ForestDB,
		common.N1QL, "Testing", "TestingWhereExpr", []string{"Testing"}, false, plan)
	if err != nil {
		t.Fatal("Cannot create Index Defn 102 through MetadataProvider" + err.Error())
	}
	input := make([]common.IndexDefnId, 1)
	input[0] = newDefnId
	if err := provider.BuildIndexes(input); err != nil {
		t.Fatal("Cannot build Index Defn : %v", err)
	}
	logging.Infof("done creating index 102")

	// Drop a seeded index (created during setup step)
	if err := provider.DropIndex(common.IndexDefnId(101)); err != nil {
		t.Fatal("Cannot drop Index Defn 101 through MetadataProvider")
	}
	logging.Infof("done dropping index 101")

	// Create Index (immediate).
	newDefnId2, err := provider.CreateIndexWithPlan("metadata_provider_test_103", "Default", common.ForestDB,
		common.N1QL, "Testing", "TestingWhereExpr", []string{"Testing"}, false, nil)
	if err != nil {
		t.Fatal("Cannot create Index Defn 103 through MetadataProvider")
	}
	logging.Infof("done creating index 103")

	// Update instance (set state to ACTIVE)
	if err := mgr.UpdateIndexInstance("Default", newDefnId2, common.INDEX_STATE_ACTIVE, common.StreamId(100), "", nil); err != nil {
		t.Fatal("Fail to update index instance")
	}
	logging.Infof("done updating index 103")

	// Update instance (set error string)
	if err := mgr.UpdateIndexInstance("Default", newDefnId2, common.INDEX_STATE_NIL, common.NIL_STREAM, "testing", nil); err != nil {
		t.Fatal("Fail to update index instance")
	}
	logging.Infof("done updating index 103")

	// Create Index (immediate).  This index is supposed to fail by OnIndexBuild()
	if _, err := provider.CreateIndexWithPlan("metadata_provider_test_104", "Default", common.ForestDB,
		common.N1QL, "Testing", "Testing", []string{"Testing"}, false, nil); err == nil {
		t.Fatal("Error does not propage for create Index Defn 104 through MetadataProvider")
	}
	logging.Infof("done creating index 104")

	logging.Infof("Verify Changed Data *********************************************************")

	if lookup(provider, common.IndexDefnId(100)) == nil {
		t.Fatal("Cannot find Index Defn 100 from MetadataProvider")
	}
	logging.Infof("found Index Defn 100")

	if lookup(provider, common.IndexDefnId(101)) != nil {
		t.Fatal("Found Deleted Index Defn 101 from MetadataProvider")
	}
	logging.Infof("cannot found deleted Index Defn 101")

	if meta = lookup(provider, newDefnId); meta == nil {
		t.Fatal(fmt.Sprintf("Cannot Found Index Defn %d from MetadataProvider", newDefnId))
	} else {
		logging.Infof("Found Index Defn %d", newDefnId)
		logging.Infof("meta.Instance %v", meta.Instances)
		if meta.Instances[0].IndexerId != indexerId {
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
		logging.Infof("Found Index Defn %d", newDefnId2)
		logging.Infof("meta.Instance %v", meta.Instances)
		if meta.Instances[0].IndexerId != indexerId {
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
		if meta.Instances[0].BuildTime[10] != 33 {
			t.Fatal(fmt.Sprintf("Index Defn %v has incorrect buildtime", newDefnId2))
		}
	}

	if !notifier.hasCreated {
		t.Fatal(fmt.Sprintf("Does not recieve notification for creating index %s", newDefnId))
	}
	logging.Infof(fmt.Sprintf("Recieve notification for creating index %v", newDefnId))

	if !notifier.hasDeleted {
		t.Fatal("Does not recieve notification for deleting index 101")
	}
	logging.Infof("Recieve notification for deleting index 101")

	time.Sleep(time.Duration(1000) * time.Millisecond)

	logging.Infof("Verify Cleanup / Timeout *********************************************************")

	// Create Index (immediate).

	newDefnId105, err := provider.CreateIndexWithPlan("metadata_provider_test_105", "Default", common.ForestDB,
		common.N1QL, "Testing", "TestingWhereExpr", []string{"Testing"}, false, nil)
	if err == nil {
		t.Fatal("Does not receive timeout error for create Index Defn 105 through MetadataProvider")
	}
	logging.Infof("recieve expected timeout error when creating index 105")
	close(metadata_provider_test_done)

	logging.Infof("Cleanup Test *********************************************************")

	provider.UnwatchMetadata(indexerId)
	cleanupTest(mgr, t)
	cleanSingleIndex(mgr, t, newDefnId)
	cleanSingleIndex(mgr, t, newDefnId2)
	cleanSingleIndex(mgr, t, newDefnId105)
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

	uuid, err := common.NewUUID()
	if err != nil {
		t.Fatal(err)
	}
	mgr.SetLocalValue("IndexerId", uuid.Str())

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

	if err := mgr.HandleCreateIndexDDL(idxDefn); err != nil {
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

	if err := mgr.HandleCreateIndexDDL(idxDefn); err != nil {
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

func (n *notifier) OnIndexCreate(defn *common.IndexDefn) error {

	if defn.Name == "metadata_provider_test_104" {
		return &c.RecoverableError{Reason: "do not allow creating metadata_provider_test_104"}
	}

	if defn.Name == "metadata_provider_test_105" {
		<-metadata_provider_test_done
	}

	n.hasCreated = true
	return nil
}

func (n *notifier) OnIndexDelete(common.IndexDefnId) error {
	n.hasDeleted = true
	return nil
}

func (n *notifier) OnIndexBuild(id []common.IndexDefnId) error {
	buildTime := make([]uint64, 1024)
	buildTime[10] = 33
	err := gMgr.UpdateIndexInstance("Default", id[0], common.INDEX_STATE_INITIAL, common.StreamId(100), "", buildTime)

	// change the value to test copy works
	buildTime[10] = 34
	return err
}
