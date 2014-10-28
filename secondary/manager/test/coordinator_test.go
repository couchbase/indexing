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
	"testing"
	"time"
)

// For this test, use index definition id from 200 - 210

func TestCoordinator(t *testing.T) {

	common.LogEnable()
	common.SetLogLevel(common.LogLevelDebug)

	common.Infof("Start TestCoordinator *********************************************************")

	var requestAddr = "localhost:9885"
	var leaderAddr = "localhost:9884"
	var config = "./config.json"

	mgr, err := manager.NewIndexManager(requestAddr, leaderAddr, config)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	cleanup(mgr, t)
	time.Sleep(time.Duration(1000) * time.Millisecond)

	// Add a new index definition : 100
	idxDefn := &common.IndexDefn{
		DefnId:          common.IndexDefnId(200),
		Name:            "coordinator_test",
		Using:           common.ForestDB,
		Bucket:          "Default",
		IsPrimary:       false,
		OnExprList:      []string{"Testing"},
		ExprType:        common.N1QL,
		PartitionScheme: common.HASH,
		PartitionKey:    "Testing"}

	err = mgr.HandleCreateIndexDDL(idxDefn)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Duration(1000) * time.Millisecond)

	idxDefn, err = mgr.GetIndexDefnByName("coordinator_test")
	if err != nil {
		t.Fatal(err)
	}

	if idxDefn == nil {
		t.Fatal("Cannot find index definition")
	}
	
	topology, err := mgr.GetTopologyByBucket("Default") 
	if err != nil {
		t.Fatal(err)
	}
	content, err := manager.MarshallIndexTopology(topology) 
	if err != nil {
		t.Fatal(err)
	}
	common.Infof("Topology after index creation : %s", string(content))

	cleanup(mgr, t)
	time.Sleep(time.Duration(1000) * time.Millisecond)

	common.Infof("Done TestCoordinator. Tearing down *********************************************************")
	mgr.Close()
	time.Sleep(time.Duration(1000) * time.Millisecond)
}

// clean up
func cleanup(mgr *manager.IndexManager, t *testing.T) {

	err := mgr.HandleDeleteIndexDDL("coordinator_test")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Duration(1000) * time.Millisecond)
}
