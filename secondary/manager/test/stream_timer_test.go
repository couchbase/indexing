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
	protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
	"testing"
	"time"
	util "github.com/couchbase/indexing/secondary/manager/test/util"
)

// For this test, use Index Defn Id from 400 - 410

// implement ProjectorStreamClientFactory
type timerTestProjectorClientFactory struct {
	donech chan bool
}

// implement ProjectorClientEnv
type timerTestProjectorClientEnv struct {
}

// projector client specific for TIMER_TEST
// implement ProjectorStreamClient
type timerTestProjectorClient struct {
	donech chan bool
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Test Driver
////////////////////////////////////////////////////////////////////////////////////////////////////

func TestStreamMgr_Timer(t *testing.T) {

	common.LogEnable()
	common.SetLogLevel(common.LogLevelDebug)
	util.TT = t

	old_value := manager.NUM_VB	
	manager.NUM_VB = 16 
	defer func() {manager.NUM_VB = old_value}()

	// Running test
	runTimerTest()
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Timer Test
// 1) This test will create a new index.  
// 2) Upon notifying of the new index, the fake projector will push a sync message to the stream manager.  
// 3) The fake projector will send another sync message after TIMESTAMP_PERSIST_INTERVAL
// 4) read the timestamp from the respository and check to see if matches with the timer in memory
////////////////////////////////////////////////////////////////////////////////////////////////////

func runTimerTest() {

	common.Infof("**** Run Timer Test **********************************************")

	common.Infof("***** Start TestStreamMgr ") 
	var requestAddr = "localhost:9885"
	var leaderAddr = "localhost:9884"
	var config = "./config.json"

	common.Infof("Start Index Manager")
	donech := make(chan bool)
	factory := new(timerTestProjectorClientFactory)
	factory.donech = donech
	env := new(timerTestProjectorClientEnv)
	admin := manager.NewProjectorAdmin(factory, env, nil)
	mgr, err := manager.NewIndexManagerInternal(requestAddr, leaderAddr, config, admin)
	if err != nil {
		util.TT.Fatal(err)
	}
	time.Sleep(time.Duration(3000) * time.Millisecond)
	
	mgr.SetTimestampPersistenceInterval(1)
	defer mgr.SetTimestampPersistenceInterval(manager.TIMESTAMP_PERSIST_INTERVAL)

	common.Infof("Timer Test Cleanup ...")
	cleanupStreamMgrTimerTest(mgr)

	common.Infof("***** Run timer Test ...")
	ch := mgr.GetStabilityTimestampChannel(common.MAINT_STREAM)
	go runTimerTestReceiver(mgr, ch, donech)

	common.Infof("Setup data for Timer Test")
	changeTopologyForTimerTest(mgr)
	<-donech

	common.Infof("**** Timer Test Cleanup ...")
	cleanupStreamMgrTimerTest(mgr)
	mgr.CleanupTopology()
	mgr.CleanupStabilityTimestamp()
	time.Sleep(time.Duration(1000) * time.Millisecond)

	common.Infof("**** Stop TestStreamMgr. Tearing down ") 
	mgr.Close()
	time.Sleep(time.Duration(1000) * time.Millisecond)
	
	common.Infof("**** Finish Timer Test *************************************************")
}

// clean up
func cleanupStreamMgrTimerTest(mgr *manager.IndexManager) {

	_, err := mgr.GetIndexDefnByName("Default", "stream_mgr_timer_test")
	if err != nil {
		common.Infof("StreamMgrTest.cleanupStreamMgrTimerTest() :  cannot find index defn stream_mgr_timer_test.  No cleanup ...")
	} else {
		common.Infof("StreamMgrTest.cleanupStreamMgrTimerTest() :  found index defn stream_mgr_timer_test.  Cleaning up ...")

		err = mgr.HandleDeleteIndexDDL("Default", "stream_mgr_timer_test")
		if err != nil {
			util.TT.Fatal(err)
		}
		time.Sleep(time.Duration(1000) * time.Millisecond)

		// double check if we have really cleaned up
		_, err := mgr.GetIndexDefnByName("Default", "stream_mgr_timer_test")
		if err == nil {
			util.TT.Fatal("StreamMgrTest.cleanupStreamMgrTimerTest(): Cannot clean up index defn stream_mgr_timer_test")
		}
	}

	time.Sleep(time.Duration(1000) * time.Millisecond)
}

// run test
func runTimerTestReceiver(mgr *manager.IndexManager, ch chan *common.TsVbuuid, donech chan bool) {

	common.Infof("Run Timer Test Receiver")
	defer close(donech)

	// wait for the sync message to arrive
	ticker := time.NewTicker(time.Duration(60) * time.Second)
	for {
		select {
		case ts := <-ch:
			if ts.Seqnos[10] == 406 {
			
				// wait to avoid race condition -- this is timing dependent.  
				time.Sleep(time.Duration(2000) * time.Millisecond)
				
			 	seqno, ok := mgr.GetStabilityTimestampForVb(common.MAINT_STREAM, "Default", uint16(10)) 
			 	if !ok || seqno != ts.Seqnos[10] {
					util.TT.Fatal("runTimerTestReceiver(): timestamp seqno does not match with repo.  %d != %d",
						ts.Seqnos[10], seqno)
			 	} else {
					common.Infof("****** runTimerTestReceiver() receive correct stability timestamp")
			 		return
			 	}
			}
		case <-ticker.C:
			common.Infof("****** runTimerTestReceiver() : timeout")
			util.TT.Fatal("runTimerTestReceiver(): Timeout waiting to receive timestamp to arrive")
		}
	}

	common.Infof("runTimerTestReceiver() done")
}

// start up
func changeTopologyForTimerTest(mgr *manager.IndexManager) {

	// Add a new index definition : 406
	idxDefn := &common.IndexDefn{
		DefnId:          common.IndexDefnId(406),
		Name:            "stream_mgr_timer_test",
		Using:           common.ForestDB,
		Bucket:          "Default",
		IsPrimary:       false,
		SecExprs:        []string{"Testing"},
		ExprType:        common.N1QL,
		PartitionScheme: common.HASH,
		PartitionKey:    "Testing"}

	common.Infof("Run Timer Test : Create Index Defn 406")
	if err := mgr.HandleCreateIndexDDL(idxDefn); err != nil {
		util.TT.Fatal(err)
	}
	// Wait so there is no race condition.
	time.Sleep(time.Duration(1000) * time.Millisecond)

	// Update the index definition to ready
	common.Infof("Run Timer Test : Update Index Defn 406 to READY")
	topology, err := mgr.GetTopologyByBucket("Default")
	if err != nil {
		util.TT.Fatal(err)
	}
	topology.ChangeStateForIndexInstByDefn(common.IndexDefnId(406), common.INDEX_STATE_CREATED, common.INDEX_STATE_READY)
	if err := mgr.SetTopologyByBucket("Default", topology); err != nil {
		util.TT.Fatal(err)
	}
}

func (c *timerTestProjectorClient) sendSync(instances []*protobuf.Instance) {

	common.Infof("timerTestProjectorClient.sendSync() ")

	if len(instances) != 1 {
		util.TT.Fatal("timerTestProjectorClient.sendSync(): More than one index instance sent to fake projector")
	}

	for _, inst := range instances {
		if inst.GetIndexInstance().GetDefinition().GetDefnID() == uint64(406) {

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
			kv := common.NewKeyVersions(100, []byte("document-name"), 1)
			kv.AddSync()
			payload.AddKeyVersions(kv)
			payloads = append(payloads, payload)

			// send payload
			common.Infof("****** runTimerTestReceiver() sending the first sync message")
			if err := p.Client.SendKeyVersions(payloads, true); err != nil {
				util.TT.Fatal(err)
			}

			payloads = make([]*common.VbKeyVersions, 0, 200)

			payload = common.NewVbKeyVersions("Default", 10, 1, 10)
			kv = common.NewKeyVersions(406, []byte("document-name"), 1)
			kv.AddSync()
			payload.AddKeyVersions(kv)
			payloads = append(payloads, payload)

			// send payload
			common.Infof("****** runTimerTestReceiver() sending the second sync message")
			if err := p.Client.SendKeyVersions(payloads, true); err != nil {
				util.TT.Fatal(err)
			}
		}
	}
}

func (c *timerTestProjectorClient) MutationTopicRequest(topic, endpointType string,
	reqTimestamps []*protobuf.TsVbuuid, instances []*protobuf.Instance) (*protobuf.TopicResponse, error) {

	if len(reqTimestamps) == 0 {
		util.TT.Fatal("timerTestProjectorClient.MutationTopicRequest(): reqTimestamps is nil")
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

func (c *timerTestProjectorClient) DelInstances(topic string, uuids []uint64) error {
	return nil
}

func (c *timerTestProjectorClient) RepairEndpoints(topic string, endpoints []string) error {
	return nil
}

func (c *timerTestProjectorClient) InitialRestartTimestamp(pooln, bucketn string) (*protobuf.TsVbuuid, error) {

	newTs := protobuf.NewTsVbuuid("default", "Default", manager.NUM_VB)
	for i := 0; i < manager.NUM_VB; i++ {
		newTs.Append(uint16(i), uint64(i), uint64(1234), uint64(0), uint64(0))
	}
	return newTs, nil
}

func (c *timerTestProjectorClient) RestartVbuckets(topic string, 
	restartTimestamps []*protobuf.TsVbuuid) (*protobuf.TopicResponse, error) {
	return nil, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// testProjectorClientFactory
////////////////////////////////////////////////////////////////////////////////////////////////////

func (p *timerTestProjectorClientFactory) GetClientForNode(server string) manager.ProjectorStreamClient {

	c := new(timerTestProjectorClient)
	c.donech = p.donech
	return c
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// testProjectorClientFactory
////////////////////////////////////////////////////////////////////////////////////////////////////

func (p *timerTestProjectorClientEnv) GetNodeListForBuckets(buckets []string) (map[string]string, error) {

	common.Infof("timerTestProjectorClientEnv.GetNodeListForBuckets() ")
	nodes := make(map[string]string)
	nodes["127.0.0.1"] = "127.0.0.1"
	return nodes, nil
}

func (p *timerTestProjectorClientEnv) GetNodeListForTimestamps(timestamps []*common.TsVbuuid) (map[string][]*protobuf.TsVbuuid, error) {

	common.Infof("timerTestProjectorClientEnv.GetNodeListForTimestamps() ")
	return nil, nil
}

func (p *timerTestProjectorClientEnv) FilterTimestampsForNode(timestamps []*protobuf.TsVbuuid, node string) ([]*protobuf.TsVbuuid, error) {
	return timestamps, nil
}
