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
	util "github.com/couchbase/indexing/secondary/manager/test/util"
	"testing"
	"time"
)

// For this test, use Index Defn Id from 300 - 310

func TestEventMgr(t *testing.T) {

	common.LogEnable()
	common.SetLogLevel(common.LogLevelDebug)

	common.Infof("Start TestEventMgr *********************************************************")

	/*
		var requestAddr = "localhost:9885"
		var leaderAddr = "localhost:9884"
		var config = "./config.json"
	*/

	common.Infof("Start Index Manager")
	factory := new(util.TestDefaultClientFactory)
	env := new(util.TestDefaultClientEnv)
	admin := manager.NewProjectorAdmin(factory, env, nil)
	//mgr, err := manager.NewIndexManagerInternal(requestAddr, leaderAddr, config, admin)
	mgr, err := manager.NewIndexManagerInternal("localhost:9886", "localhost:"+manager.COORD_MAINT_STREAM_PORT, admin)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	cleanupEvtMgrTest(mgr, t)
	time.Sleep(time.Duration(1000) * time.Millisecond)

	common.Infof("Start Listening to event")
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

	common.Infof("Before DDL")
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

	common.Infof("Stop TestEventMgr. Tearing down *********************************************************")

	mgr.Close()
	time.Sleep(time.Duration(1000) * time.Millisecond)
}

// clean up
func cleanupEvtMgrTest(mgr *manager.IndexManager, t *testing.T) {

	_, err := mgr.GetIndexDefnById(common.IndexDefnId(300))
	if err != nil {
		common.Infof("EventMgrTest.cleanupEvtMgrTest() :  cannot find index defn event_mgr_test.  No cleanup ...")
	} else {
		common.Infof("EventMgrTest.cleanupEvtMgrTest() :  found index defn event_mgr_test.  Cleaning up ...")

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
