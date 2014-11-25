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
	"github.com/couchbase/indexing/secondary/dataport"
	"github.com/couchbase/indexing/secondary/manager"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
	"github.com/couchbase/indexing/secondary/transport"
	"net"
	"testing"
	"time"
	//"runtime/pprof"
	//"os"
)

// For this test, use Index Defn Id from 400 - 410

type testStreamAdmin struct {
}

func TestStreamMgr(t *testing.T) {

	common.LogEnable()
	common.SetLogLevel(common.LogLevelDebug)

	common.Infof("Start TestStreamMgr *********************************************************")

	var requestAddr = "localhost:9885"
	var leaderAddr = "localhost:9884"
	var config = "./config.json"

	common.Infof("Start Index Manager")
	mgr, err := manager.NewIndexManager(requestAddr, leaderAddr, config)
	if err != nil {
		t.Fatal(err)
	}

	// Test cleanup and setup
	common.Infof("Setup test data")
	cleanupStreamMgrTest(mgr, t)
	setupStreamMgrTest(mgr, t)

	// Start Stream Manager
	common.Infof("Start Stream Manager for testing")
	admin := new(testStreamAdmin)
	handler := manager.NewMgrMutHandler(mgr, admin)
	streamMgr, err := manager.NewStreamManager(mgr, handler, admin)
	if err != nil {
		t.Fatal(err)
	}

	if err := streamMgr.StartStream(common.MAINT_STREAM, "6579"); err != nil {
		t.Fatal(err)
	}

	if err := streamMgr.OpenStreamForBucket(common.MAINT_STREAM, "Default", "6579"); err != nil {
		t.Fatal(err)
	}

	common.Infof("Run Test")
	ch := mgr.GetStabilityTimestampChannel(common.MAINT_STREAM)
	donech := make(chan bool)
	go runTestSender("6579", donech, t)
	runTestReceiver(ch, donech, t)
	time.Sleep(time.Duration(3000) * time.Millisecond)

	////////////////////////////////////////////////////
	common.Infof("Stop TestStreamMgr. Tearing down *********************************************************")
	cleanupStreamMgrTest(mgr, t)
	time.Sleep(time.Duration(1000) * time.Millisecond)
	mgr.Close()
	time.Sleep(time.Duration(1000) * time.Millisecond)
	streamMgr.Close()
	time.Sleep(time.Duration(1000) * time.Millisecond)
}

// run test
func runTestSender(port string, donech chan bool, t *testing.T) {

	// start client
	addr := net.JoinHostPort("127.0.0.1", port)
	prefix := "projector.dataport.client."
	config := c.SystemConfig.SectionConfig(prefix, true /*trim*/)
	maxvbs := c.SystemConfig["maxVbuckets"].Int()
	flag := transport.TransportFlag(0).SetProtobuf()
	client, err := dataport.NewClient(addr, flag, maxvbs, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	payloads := make([]*common.VbKeyVersions, 0, 200)

	payload := common.NewVbKeyVersions("Default", 10, 1, 10)
	kv := common.NewKeyVersions(100, []byte("document-name"), 1)
	kv.AddStreamBegin()
	payload.AddKeyVersions(kv)
	payloads = append(payloads, payload)

	for i := 0; i < 1; i++ {
		payload := common.NewVbKeyVersions("Default", 10, 1, 10)
		kv := common.NewKeyVersions(uint64(100+i), []byte("document-name"), 1)
		kv.AddSync()
		payload.AddKeyVersions(kv)
		payloads = append(payloads, payload)
	}

	// send payload
	err = client.SendKeyVersions(payloads, true)
	if err != nil {
		t.Fatal(err)
	}

	<-donech
	common.Infof("runTestSender() done")
}

// run test
func runTestReceiver(ch chan *common.TsVbuuid, donech chan bool, t *testing.T) {

	defer func() {
		common.Infof("runTestReceiver() done")
		close(donech)
	}()

	// wait for the sync message to arrive
	ticker := time.NewTicker(time.Duration(30) * time.Second)
	for {
		select {
		case ts := <-ch:
			if ts.Seqnos[10] >= 100 {
				return
			}
		case <-ticker.C:
			t.Fatal("Timeout waiting to receive timestamp to arrive")
		}
	}
}

// start up
func setupStreamMgrTest(mgr *manager.IndexManager, t *testing.T) {

	// Add a new index definition : 400
	idxDefn := &common.IndexDefn{
		DefnId:          common.IndexDefnId(400),
		Name:            "stream_mgr_test",
		Using:           common.ForestDB,
		Bucket:          "Default",
		IsPrimary:       false,
		SecExprs:        []string{"Testing"},
		ExprType:        common.N1QL,
		PartitionScheme: common.HASH,
		PartitionKey:    "Testing"}

	common.Infof("Before DDL")
	if err := mgr.HandleCreateIndexDDL(idxDefn); err != nil {
		t.Fatal(err)
	}
}

// clean up
func cleanupStreamMgrTest(mgr *manager.IndexManager, t *testing.T) {

	_, err := mgr.GetIndexDefnByName("stream_mgr_test")
	if err != nil {
		common.Infof("StreamMgrTest.cleanupStreamMgrTest() :  cannot find index defn stream_mgr_test.  No cleanup ...")
	} else {
		common.Infof("StreamMgrTest.cleanupStreamMgrTest() :  found index defn stream_mgr_test.  Cleaning up ...")

		err = mgr.HandleDeleteIndexDDL("stream_mgr_test")
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Duration(1000) * time.Millisecond)

		// double check if we have really cleaned up
		_, err := mgr.GetIndexDefnByName("stream_mgr_test")
		if err == nil {
			t.Fatal("StreamMgrTest.cleanupStreamMgrTest(): Cannot clean up index defn stream_mgr_test")
		}
	}

	time.Sleep(time.Duration(1000) * time.Millisecond)
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// testStreamAdmin
////////////////////////////////////////////////////////////////////////////////////////////////////

func (a *testStreamAdmin) OpenStreamForBucket(streamId common.StreamId, bucket string,
	topology []*protobuf.Instance, requestTs *common.TsVbuuid) error {
	return nil
}

func (a *testStreamAdmin) RepairStreamForEndpoint(streamId common.StreamId, bucketVbnosMap map[string][]uint16, endpoint string) error {
	return nil
}

func (a *testStreamAdmin) AddIndexToStream(streamId common.StreamId, bucket string, instances []*protobuf.Instance) error {
	return nil
}

func (a *testStreamAdmin) DeleteIndexFromStream(streamId common.StreamId, bucket string, instances []uint64) error {
	return nil
}
