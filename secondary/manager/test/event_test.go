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
	util "github.com/couchbase/indexing/secondary/manager/test/util"
	"os"
	"testing"
	"time"
)

// For this test, use Index Defn Id from 300 - 310

func TestEventMgr(t *testing.T) {

	logging.SetLogLevel(logging.Trace)

	logging.Infof("Start TestEventMgr *********************************************************")

	cfg := common.SystemConfig.SectionConfig("indexer", true /*trim*/)
	cfg.Set("storage_dir", common.ConfigValue{"./data/", "metadata file path", "./"})
	os.MkdirAll("./data/", os.ModePerm)

	/*
		var requestAddr = "localhost:9885"
		var leaderAddr = "localhost:9884"
		var config = "./config.json"
	*/

	logging.Infof("Start Index Manager")
	factory := new(util.TestDefaultClientFactory)
	env := new(util.TestDefaultClientEnv)
	admin := manager.NewProjectorAdmin(factory, env, nil)
	//mgr, err := manager.NewIndexManagerInternal(requestAddr, leaderAddr, config, admin)
	mgr, err := manager.NewIndexManagerInternal("localhost:9886", "localhost:"+manager.COORD_MAINT_STREAM_PORT, admin, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	cleanupEvtMgrTest(mgr, t)
	time.Sleep(time.Duration(1000) * time.Millisecond)

	logging.Infof("Start Listening to event")
	notifications, err := mgr.StartListenIndexCreate("TestEventMgr")
	if err != nil {
		t.Fatal(err)
	}

	// Add a new index definition : 300
	idxDefn := &common.IndexDefn{
		DefnId:          common.IndexDefnId(300),
		Name:            "event_mgr_test",
		Using:           common.ForestDB,
		Bucket:          "Default",
		IsPrimary:       false,
		SecExprs:        []string{"Testing"},
		ExprType:        common.N1QL,
		PartitionScheme: common.HASH,
		PartitionKey:    "Testing"}

	logging.Infof("Before DDL")
	err = mgr.HandleCreateIndexDDL(idxDefn)
	if err != nil {
		t.Fatal(err)
	}

	data := listen(notifications)
	if data == nil {
		t.Fatal("Does not receive notification from watcher")
	}

	idxDefn, err = common.UnmarshallIndexDefn(([]byte)(data))
	if err != nil {
		t.Fatal(err)
	}

	if idxDefn == nil {
		t.Fatal("Cannot unmarshall index definition")
	}

	if idxDefn.Name != "event_mgr_test" {
		t.Fatal("Index Definition Name mismatch")
	}

	cleanupEvtMgrTest(mgr, t)
	mgr.CleanupTopology()
	mgr.CleanupStabilityTimestamp()
	time.Sleep(time.Duration(1000) * time.Millisecond)

	logging.Infof("Stop TestEventMgr. Tearing down *********************************************************")

	mgr.Close()
	time.Sleep(time.Duration(1000) * time.Millisecond)
}

// clean up
func cleanupEvtMgrTest(mgr *manager.IndexManager, t *testing.T) {

	_, err := mgr.GetIndexDefnById(common.IndexDefnId(300))
	if err != nil {
		logging.Infof("EventMgrTest.cleanupEvtMgrTest() :  cannot find index defn event_mgr_test.  No cleanup ...")
	} else {
		logging.Infof("EventMgrTest.cleanupEvtMgrTest() :  found index defn event_mgr_test.  Cleaning up ...")

		err = mgr.HandleDeleteIndexDDL(common.IndexDefnId(300))
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Duration(1000) * time.Millisecond)

		// double check if we have really cleaned up
		_, err := mgr.GetIndexDefnById(common.IndexDefnId(300))
		if err == nil {
			t.Fatal("EventMgrTest.cleanupEvtMgrTest(): Cannot clean up index defn event_mgr_test")
		}
	}
}

// listen to an event
func listen(notifications <-chan interface{}) []byte {

	timer := time.After(time.Duration(20000) * time.Millisecond)
	select {
	case data, ok := <-notifications:
		if !ok {
			return nil
		}

		return data.([]byte)
	case <-timer:
		return nil
	}

	return nil
}
