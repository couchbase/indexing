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
	protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
	"testing"
	"time"
)

// For this test, use Index Defn Id from 400 - 410

// implement ProjectorStreamClientFactory
type syncTestProjectorClientFactory struct {
	donech chan bool
}

// implement ProjectorClientEnv
type syncTestProjectorClientEnv struct {
}

// projector client specific for SYNC_TEST
// implement ProjectorStreamClient
type syncTestProjectorClient struct {
	donech chan bool
}


////////////////////////////////////////////////////////////////////////////////////////////////////
// Test Driver
////////////////////////////////////////////////////////////////////////////////////////////////////

func TestStreamMgr_Sync(t *testing.T) {

	common.LogEnable()
	common.SetLogLevel(common.LogLevelDebug)
	util.TT = t

	old_value := manager.NUM_VB	
	manager.NUM_VB = 16 
	defer func() {manager.NUM_VB = old_value}()

	// Running test
	runSyncTest()
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Sync Test
// This test creates a new index definition and updates topology.  Topology update would send new
// instances to a fake projector.  The fake projector, in turn, will send a sync message to the
// timer.  The timer will then broadcast a new stability timestamp through the coordinator.  A test
// receiver will be listening to the update on the stability timestamp.
//
// This test covers the following functions:
// 1) Coordinator Master creates new index and updates topology.
// 2) Topology change can be received by the stream manager.
// 3) Stream manager can diff between old and new topology.
// 4) Stream manager sends new instances to (fake) projector
// 5) Stream Handler can receive sync messages from (fake) projector
// 6) Timer updates stability timestamp upon sync messages
// 7) Stability timestamp is broadcasted by coordinator
////////////////////////////////////////////////////////////////////////////////////////////////////

func runSyncTest() {

	common.Infof("**** Run Sync Test ******************************************")
	
	common.Infof("***** Start TestStreamMgr ") 
	var requestAddr = "localhost:9885"
	var leaderAddr = "localhost:9884"
	var config = "./config.json"

	common.Infof("Start Index Manager")
	donech := make(chan bool)
	factory := new(syncTestProjectorClientFactory)
	factory.donech = donech
	env := new(syncTestProjectorClientEnv)
	admin := manager.NewProjectorAdmin(factory, env, nil)
	mgr, err := manager.NewIndexManagerInternal(requestAddr, leaderAddr, config, admin)
	if err != nil {
		util.TT.Fatal(err)
	}
	time.Sleep(time.Duration(3000) * time.Millisecond)
	
	common.Infof("Sync Test Cleanup ...")
	cleanupStreamMgrSyncTest(mgr)

	common.Infof("***** Run Sync Test ...")
	ch := mgr.GetStabilityTimestampChannel(common.MAINT_STREAM)
	go runSyncTestReceiver(ch, donech)

	common.Infof("Setup data for Sync Test")
	changeTopologyForSyncTest(mgr)
	<-donech

	common.Infof("**** Sync Test Cleanup ...")
	cleanupStreamMgrSyncTest(mgr)
	mgr.CleanupTopology()
	mgr.CleanupStabilityTimestamp()
	time.Sleep(time.Duration(1000) * time.Millisecond)

	common.Infof("**** Stop TestStreamMgr. Tearing down ") 
	mgr.Close()
	time.Sleep(time.Duration(1000) * time.Millisecond)
	
	common.Infof("**** Finish Sync Test ************************************************")
}

// run test
func runSyncTestReceiver(ch chan *common.TsVbuuid, donech chan bool) {

	common.Infof("Run Sync Test Receiver")
	defer close(donech)

	// wait for the sync message to arrive
	ticker := time.NewTicker(time.Duration(60) * time.Second)
	for {
		select {
		case ts := <-ch:
			common.Infof("****** runSyncTestReceiver() receive stability timestamp seq[10] = %d", ts.Seqnos[10])
			if ts.Seqnos[10] == 400 {
				common.Infof("****** runSyncTestReceiver() receive correct stability timestamp")
				return
			}
		case <-ticker.C:
			common.Infof("****** runSyncTestReceiver() : timeout")
			util.TT.Fatal("runSyncTestReceiver(): Timeout waiting to receive timestamp to arrive")
		}
	}

	common.Infof("runSyncTestReceiver() done")
}

// start up
func changeTopologyForSyncTest(mgr *manager.IndexManager) {

	// Add a new index definition : 400
	idxDefn := &common.IndexDefn{
		DefnId:          common.IndexDefnId(400),
		Name:            "stream_mgr_sync_test",
		Using:           common.ForestDB,
		Bucket:          "Default",
		IsPrimary:       false,
		SecExprs:        []string{"Testing"},
		ExprType:        common.N1QL,
		PartitionScheme: common.HASH,
		PartitionKey:    "Testing"}

	common.Infof("Run Sync Test : Create Index Defn 400")
	if err := mgr.HandleCreateIndexDDL(idxDefn); err != nil {
		util.TT.Fatal(err)
	}
	// Wait so there is no race condition.
	time.Sleep(time.Duration(1000) * time.Millisecond)

	// Update the index definition to ready
	common.Infof("Run Sync Test : Update Index Defn 400 to READY")
	topology, err := mgr.GetTopologyByBucket("Default")
	if err != nil {
		util.TT.Fatal(err)
	}
	topology.ChangeStateForIndexInstByDefn(common.IndexDefnId(400), common.INDEX_STATE_CREATED, common.INDEX_STATE_READY)
	if err := mgr.SetTopologyByBucket("Default", topology); err != nil {
		util.TT.Fatal(err)
	}

}

// clean up
func cleanupStreamMgrSyncTest(mgr *manager.IndexManager) {

	_, err := mgr.GetIndexDefnByName("Default", "stream_mgr_sync_test")
	if err != nil {
		common.Infof("StreamMgrTest.cleanupStreamMgrSyncTest() :  cannot find index defn stream_mgr_sync_test.  No cleanup ...")
	} else {
		common.Infof("StreamMgrTest.cleanupStreamMgrSyncTest() :  found index defn stream_mgr_sync_test.  Cleaning up ...")

		err = mgr.HandleDeleteIndexDDL("Default", "stream_mgr_sync_test")
		if err != nil {
			util.TT.Fatal(err)
		}
		time.Sleep(time.Duration(1000) * time.Millisecond)

		// double check if we have really cleaned up
		_, err := mgr.GetIndexDefnByName("Default", "stream_mgr_sync_test")
		if err == nil {
			util.TT.Fatal("StreamMgrTest.cleanupStreamMgrSyncTest(): Cannot clean up index defn stream_mgr_sync_test")
		}
	}
	
	time.Sleep(time.Duration(1000) * time.Millisecond)
}

func (c *syncTestProjectorClient) sendSync(instances []*protobuf.Instance) {

	common.Infof("syncTestProjectorClient.sendSync() ")

	if len(instances) != 1 {
		util.TT.Fatal("syncTestProjectorClient.sendSync(): More than one index instance sent to fake projector")
	}

	for _, inst := range instances {
		if inst.GetIndexInstance().GetDefinition().GetDefnID() == uint64(400) {

			p := util.NewFakeProjector(manager.COORD_MAINT_STREAM_PORT)
			go p.Run(c.donech)

			payloads := make([]*common.VbKeyVersions, 0, 2000)

			// send StreamBegin for all vbuckets
			for i := 0; i < manager.NUM_VB; i++ {
				payload := common.NewVbKeyVersions("Default", uint16(i) /* vb */, 1, 10)
				kv := common.NewKeyVersions(1, []byte("document-name"), 1)
				kv.AddStreamBegin()
				kv.AddSync()
				payload.AddKeyVersions(kv)
				payloads = append(payloads, payload)
			}

			payload := common.NewVbKeyVersions("Default", 10, 1, 10)
			kv := common.NewKeyVersions(uint64(400), []byte("document-name"), 1)
			kv.AddSync()
			payload.AddKeyVersions(kv)
			payloads = append(payloads, payload)

			// send payload
			err := p.Client.SendKeyVersions(payloads, true)
			if err != nil {
				util.TT.Fatal(err)
			}
		}
	}
}

func (c *syncTestProjectorClient) MutationTopicRequest(topic, endpointType string,
	reqTimestamps []*protobuf.TsVbuuid, instances []*protobuf.Instance) (*protobuf.TopicResponse, error) {

	if len(reqTimestamps) == 0 {
		util.TT.Fatal("testProjectorClient.MutationTopicRequest(): reqTimestamps is nil")
	}

	c.sendSync(instances)

	response := new(protobuf.TopicResponse)
	response.Topic = &topic
	response.InstanceIds = make([]uint64, len(instances))
	for i, inst := range instances {
		response.InstanceIds[i] = inst.GetIndexInstance().GetInstId()
	}
	response.ActiveTimestamps = make([]*protobuf.TsVbuuid, 1)
	response.ActiveTimestamps[0] = reqTimestamps[0]
	response.RollbackTimestamps = nil
	response.Err = nil

	return response, nil
}

func (c *syncTestProjectorClient) DelInstances(topic string, uuids []uint64) error {
	return nil
}

func (c *syncTestProjectorClient) RepairEndpoints(topic string, endpoints []string) error {
	return nil
}

func (c *syncTestProjectorClient) InitialRestartTimestamp(pooln, bucketn string) (*protobuf.TsVbuuid, error) {

	newTs := protobuf.NewTsVbuuid("default", "Default", manager.NUM_VB)
	for i := 0; i < manager.NUM_VB; i++ {
		newTs.Append(uint16(i), uint64(i), uint64(1234), uint64(0), uint64(0))
	}
	return newTs, nil
}

func (c *syncTestProjectorClient) RestartVbuckets(topic string, 
	restartTimestamps []*protobuf.TsVbuuid) (*protobuf.TopicResponse, error) {
	return nil, nil
}
	
////////////////////////////////////////////////////////////////////////////////////////////////////
// testProjectorClientFactory
////////////////////////////////////////////////////////////////////////////////////////////////////

func (p *syncTestProjectorClientFactory) GetClientForNode(server string) manager.ProjectorStreamClient {

	c := new(syncTestProjectorClient)
	c.donech = p.donech
	return c
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// syncTestProjectorClientFactory
////////////////////////////////////////////////////////////////////////////////////////////////////

func (p *syncTestProjectorClientEnv) GetNodeListForBuckets(buckets []string) (map[string]string, error) {

	common.Infof("syncTestProjectorClientEnv.GetNodeListForBuckets() ")
	nodes := make(map[string]string)
	nodes["127.0.0.1"] = "127.0.0.1"
	return nodes, nil
}

func (p *syncTestProjectorClientEnv) GetNodeListForTimestamps(timestamps []*common.TsVbuuid) (map[string][]*protobuf.TsVbuuid, error) {

	common.Infof("syncTestProjectorClientEnv.GetNodeListForTimestamps() ")
	return nil, nil
}

func (p *syncTestProjectorClientEnv) FilterTimestampsForNode(timestamps []*protobuf.TsVbuuid, node string) ([]*protobuf.TsVbuuid, error) {
	return timestamps, nil
}