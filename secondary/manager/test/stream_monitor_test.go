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
	"github.com/couchbase/indexing/secondary/projector"
	"testing"
	"time"
	util "github.com/couchbase/indexing/secondary/manager/test/util"
)

// For this test, use Index Defn Id from 400 - 410

// implement ProjectorStreamClientFactory
type monitorTestProjectorClientFactory struct {
	donech chan bool
}

// implement ProjectorClientEnv
type monitorTestProjectorClientEnv struct {
}

// projector client specific for STREAM_END_TEST
// implement ProjectorStreamClient
type monitorTestProjectorClient struct {
	donech chan bool
}


////////////////////////////////////////////////////////////////////////////////////////////////////
// Test Driver
////////////////////////////////////////////////////////////////////////////////////////////////////

func TestStreamMgr_Monitor(t *testing.T) {

	common.LogEnable()
	common.SetLogLevel(common.LogLevelDebug)
	util.TT = t

	old_value := manager.NUM_VB	
	manager.NUM_VB = 16 
	defer func() {manager.NUM_VB = old_value}()
	
	old_interval := manager.MONITOR_INTERVAL
	manager.MONITOR_INTERVAL = time.Duration(1000) * time.Millisecond	
	defer func() {manager.MONITOR_INTERVAL = old_interval}()

	// Running test
	runMonitorTest()
}
	
////////////////////////////////////////////////////////////////////////////////////////////////////
// Monitor Test
// 1) This test will create a new index.  
// 2) Upon notifying of the new index, MutationTopicRequest will return an error with the active timestamp as well
//    as the rollback timestamp.
// 3) StreamAdmin will retry MutationTopicRequest by merging the request timestamps and rollback timestamps.
// 4) If the new request timestamp has the right seqno (from rollback timestamp), MutationTopicRequest will not return error. 
//    But no streamBegin and sync message will be sent.
// 5) Stream Monitior will timeout without receving streamBegin message. 
// 6) The monitor call back to the fake projector to restart the vubcket with a restart timestamp.  This restart
//	  timestamp is the same as the one returned from MutationTopicRequest.
// 7) During restart vbucket, the fake projector will send BeginStream message as well as sync messagse.   It will use
//    the seqno from the restart timestamp when sending sync message. 
// 8) The dataport will update the stability timestamp upon receving the sync message.  The test will end if the
//    timestamp contains the expected seqno
////////////////////////////////////////////////////////////////////////////////////////////////////

func runMonitorTest() {

	common.Infof("**** Run Monitor Test ******************************************")

	common.Infof("***** Start TestStreamMgr ") 
	/*
	var requestAddr = "localhost:9885"
	var leaderAddr = "localhost:9884"
	*/
	var config = "./config.json"

	common.Infof("Start Index Manager")
	donech := make(chan bool)
	factory := new(monitorTestProjectorClientFactory)
	factory.donech = donech
	env := new(monitorTestProjectorClientEnv)
	admin := manager.NewProjectorAdmin(factory, env, nil)
	//mgr, err := manager.NewIndexManagerInternal(requestAddr, leaderAddr, config, admin)
	mgr, err := manager.NewIndexManagerInternal("localhost:9886", "localhost:" + manager.COORD_MAINT_STREAM_PORT, admin)
	if err != nil {
		util.TT.Fatal(err)
	}
	mgr.StartCoordinator(config)
	time.Sleep(time.Duration(3000) * time.Millisecond)
	
	common.Infof("Monitor Test Cleanup ...")
	cleanupStreamMgrMonitorTest(mgr)

	common.Infof("***** Run Monitor Test ...")
	ch := mgr.GetStabilityTimestampChannel(common.MAINT_STREAM)
	go runMonitorTestReceiver(ch, donech)

	common.Infof("Setup data for Monitor Test")
	changeTopologyForMonitorTest(mgr)
	<-donech

	common.Infof("**** Monitor Test Cleanup ...")
	cleanupStreamMgrMonitorTest(mgr)
	mgr.CleanupTopology()
	mgr.CleanupStabilityTimestamp()
	time.Sleep(time.Duration(1000) * time.Millisecond)

	common.Infof("**** Stop TestStreamMgr. Tearing down ") 
	mgr.Close()
	time.Sleep(time.Duration(1000) * time.Millisecond)
	
	common.Infof("**** Finish Monitor Test ****************************************")
}

// clean up
func cleanupStreamMgrMonitorTest(mgr *manager.IndexManager) {

	_, err := mgr.GetIndexDefnByName("Default", "stream_mgr_monitor_test")
	if err != nil {
		common.Infof("StreamMgrTest.cleanupStreamMgrMonitorTest() :  cannot find index defn stream_mgr_monitor_test.  No cleanup ...")
	} else {
		common.Infof("StreamMgrTest.cleanupStreamMgrMonitorTest() :  found index defn stream_mgr_monitor_test.  Cleaning up ...")

		err = mgr.HandleDeleteIndexDDL("Default", "stream_mgr_monitor_test")
		if err != nil {
			util.TT.Fatal(err)
		}
		time.Sleep(time.Duration(1000) * time.Millisecond)

		// double check if we have really cleaned up
		_, err := mgr.GetIndexDefnByName("Default", "stream_mgr_monitor_test")
		if err == nil {
			util.TT.Fatal("StreamMgrTest.cleanupStreamMgrMonitorTest(): Cannot clean up index defn stream_mgr_monitor_test")
		}
	}

	time.Sleep(time.Duration(1000) * time.Millisecond)
}

// run test
func runMonitorTestReceiver(ch chan *common.TsVbuuid, donech chan bool) {

	common.Infof("Run Monitor Test Receiver")
	defer close(donech)

	// wait for the sync message to arrive
	ticker := time.NewTicker(time.Duration(60) * time.Second)
	for {
		select {
		case ts := <-ch:
			if ts.Seqnos[10] == 406 {
				common.Infof("****** runMonitorTestReceiver() receive correct stability timestamp")
				return
			}
		case <-ticker.C:
			common.Infof("****** runMonitorTestReceiver() : timeout")
			util.TT.Fatal("runMonitorTestReceiver(): Timeout waiting to receive timestamp to arrive")
		}
	}

	common.Infof("runMonitorTestReceiver() done")
}

// start up
func changeTopologyForMonitorTest(mgr *manager.IndexManager) {

	// Add a new index definition : 406
	idxDefn := &common.IndexDefn{
		DefnId:          common.IndexDefnId(406),
		Name:            "stream_mgr_monitor_test",
		Using:           common.ForestDB,
		Bucket:          "Default",
		IsPrimary:       false,
		SecExprs:        []string{"Testing"},
		ExprType:        common.N1QL,
		PartitionScheme: common.HASH,
		PartitionKey:    "Testing"}

	common.Infof("Run Monitor Test : Create Index Defn 406")
	if err := mgr.HandleCreateIndexDDL(idxDefn); err != nil {
		util.TT.Fatal(err)
	}
	// Wait so there is no race condition.
	time.Sleep(time.Duration(1000) * time.Millisecond)

	// Update the index definition to ready
	common.Infof("Run Monitor Test : Update Index Defn 406 to READY")
	topology, err := mgr.GetTopologyByBucket("Default")
	if err != nil {
		util.TT.Fatal(err)
	}
	topology.ChangeStateForIndexInstByDefn(common.IndexDefnId(406), common.INDEX_STATE_CREATED, common.INDEX_STATE_READY)
	if err := mgr.SetTopologyByBucket("Default", topology); err != nil {
		util.TT.Fatal(err)
	}
}

func (c *monitorTestProjectorClient) sendSync(timestamps []*protobuf.TsVbuuid) {

	common.Infof("monitorTestProjectorClient.sendSync() ")

	if len(timestamps) != 1 {
		util.TT.Fatal("monitorTestProjectorClient.sendSync(): More than one timestamp sent to fake projector. Num = %v", len(timestamps))
	}

	p := util.NewFakeProjector(manager.COORD_MAINT_STREAM_PORT)
	go p.Run(c.donech)

	payloads := make([]*common.VbKeyVersions, 0, 2000)
	
	// send StreamBegin for all vbuckets
	for i := 0; i < manager.NUM_VB; i++ {
		seqno, vbuuid, _, _, err := timestamps[0].Get(uint16(i))
		if err != nil {
			seqno = 1
			vbuuid = 1
		} 
		if seqno == 0 {
			seqno = 1
		}
		
		payload := common.NewVbKeyVersions("Default", uint16(i) /* vb */, vbuuid, 10)
		kv := common.NewKeyVersions(seqno, []byte("document-name"), 1)
		kv.AddStreamBegin()
		kv.AddSync()
		payload.AddKeyVersions(kv)
		payloads = append(payloads, payload)
	}

	// send payload
	if err := p.Client.SendKeyVersions(payloads, true); err != nil {
		util.TT.Fatal(err)
	}
}

func (c *monitorTestProjectorClient) MutationTopicRequest(topic, endpointType string,
	reqTimestamps []*protobuf.TsVbuuid, instances []*protobuf.Instance) (*protobuf.TopicResponse, error) {

	common.Infof("monitorTestProjectorClient.MutationTopicRequest(): start")
	
	if len(reqTimestamps) == 0 {
		util.TT.Fatal("testProjectorClient.MutationTopicRequest(): reqTimestamps is nil")
		
	}

	response := new(protobuf.TopicResponse)
	response.Topic = &topic
	response.InstanceIds = make([]uint64, len(instances))
	for i, inst := range instances {
		response.InstanceIds[i] = inst.GetIndexInstance().GetInstId()
	}
	response.ActiveTimestamps = reqTimestamps

	if reqTimestamps[0].GetSeqnos()[10] != 406 {	
		response.RollbackTimestamps = make([]*protobuf.TsVbuuid, 1)
		response.RollbackTimestamps[0] = protobuf.NewTsVbuuid(manager.DEFAULT_POOL_NAME, reqTimestamps[0].GetBucket(), manager.NUM_VB)
		response.RollbackTimestamps[0].Append(uint16(10), uint64(406), reqTimestamps[0].Vbuuids[10], 0, 0)

		response.Err = protobuf.NewError(projector.ErrorStreamRequest)
		return response, projector.ErrorStreamRequest 
	} else {
		response.RollbackTimestamps = nil 
		response.Err = nil
		return response, nil 
	}
}

func (c *monitorTestProjectorClient) DelInstances(topic string, uuids []uint64) error {
	return nil
}

func (c *monitorTestProjectorClient) RepairEndpoints(topic string, endpoints []string) error {
	return nil
}

func (c *monitorTestProjectorClient) InitialRestartTimestamp(pooln, bucketn string) (*protobuf.TsVbuuid, error) {

	common.Infof("monitorTestProjectorClient. InitialRestartTimestamp(): start")
	newTs := protobuf.NewTsVbuuid("default", bucketn, manager.NUM_VB)
	for i := 0; i < manager.NUM_VB; i++ {
		newTs.Append(uint16(i), uint64(i), uint64(1234), uint64(0), uint64(0))
	}
	return newTs, nil
}

func (c *monitorTestProjectorClient) RestartVbuckets(topic string, 
	restartTimestamps []*protobuf.TsVbuuid) (*protobuf.TopicResponse, error) {
	
	c.sendSync(restartTimestamps)
	
	response := new(protobuf.TopicResponse)
	response.Topic = &topic
	response.InstanceIds = nil 
	response.ActiveTimestamps = make([]*protobuf.TsVbuuid, 1)
	response.ActiveTimestamps[0] = restartTimestamps[0]
	response.RollbackTimestamps = nil
	response.Err = nil

	return response, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// testProjectorClientFactory
////////////////////////////////////////////////////////////////////////////////////////////////////

func (p *monitorTestProjectorClientFactory) GetClientForNode(server string) manager.ProjectorStreamClient {

	c := new(monitorTestProjectorClient)
	c.donech = p.donech
	return c
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// testProjectorClientFactory
////////////////////////////////////////////////////////////////////////////////////////////////////

func (p *monitorTestProjectorClientEnv) GetNodeListForBuckets(buckets []string) (map[string]string, error) {

	common.Infof("monitorTestProjectorClientEnv.GetNodeListForBuckets() ")
	nodes := make(map[string]string)
	nodes["127.0.0.1"] = "127.0.0.1"
	return nodes, nil
}

func (p *monitorTestProjectorClientEnv) GetNodeListForTimestamps(timestamps []*common.TsVbuuid) (map[string][]*protobuf.TsVbuuid, error) {

	common.Infof("monitorTestProjectorClientEnv.GetNodeListForTimestamps() ")
	nodes := make(map[string][]*protobuf.TsVbuuid)
	nodes["127.0.0.1"] = nil 
		
	newTs := protobuf.NewTsVbuuid("default", "Default", 1)
	for i, _ := range timestamps[0].Seqnos {
		newTs.Append(uint16(i), timestamps[0].Seqnos[i], timestamps[0].Vbuuids[i],
			timestamps[0].Snapshots[i][0], timestamps[0].Snapshots[i][1])
	}
		
	nodes["127.0.0.1"] = append(nodes["127.0.0.1"], newTs)	
	return nodes, nil
}

func (p *monitorTestProjectorClientEnv) FilterTimestampsForNode(timestamps []*protobuf.TsVbuuid, node string) ([]*protobuf.TsVbuuid, error) {
	return timestamps, nil
}