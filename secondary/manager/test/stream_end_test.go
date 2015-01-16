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
type streamEndTestProjectorClientFactory struct {
	donech chan bool
}

// implement ProjectorClientEnv
type streamEndTestProjectorClientEnv struct {
}

// projector client specific for STREAM_END_TEST
// implement ProjectorStreamClient
type streamEndTestProjectorClient struct {
	donech chan bool
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Test Driver
////////////////////////////////////////////////////////////////////////////////////////////////////

func TestStreamMgr_StreamEnd(t *testing.T) {

	common.LogEnable()
	common.SetLogLevel(common.LogLevelTrace)
	util.TT = t

	old_value := manager.NUM_VB
	manager.NUM_VB = 16
	defer func() { manager.NUM_VB = old_value }()

	// Running test
	runStreamEndTest()
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// StreamEnd Test
// 1) This test will create a new index.
// 2) Upon notifying of the new index, the fake projector will push a StreamEnd message to the stream manager.
// 3) The stream manager will then call back to the fake projector to repair the vubcket.  If the correct vbucket
//    is received, the fake projector will return without error.
// 4) The fake projector will also push a sync message to the data port for the coordinator to broadcast a timestamp.
////////////////////////////////////////////////////////////////////////////////////////////////////

func runStreamEndTest() {

	common.Infof("**** Run StreamEnd Test ******************************************")

	common.Infof("***** Start TestStreamMgr ")
	/*
		var requestAddr = "localhost:9885"
		var leaderAddr = "localhost:9884"
	*/
	var config = "./config.json"

	common.Infof("Start Index Manager")
	donech := make(chan bool)
	factory := new(streamEndTestProjectorClientFactory)
	factory.donech = donech
	env := new(streamEndTestProjectorClientEnv)
	admin := manager.NewProjectorAdmin(factory, env, nil)
	//mgr, err := manager.NewIndexManagerInternal(requestAddr, leaderAddr, config, admin)
	mgr, err := manager.NewIndexManagerInternal("localhost:9886", "localhost:"+manager.COORD_MAINT_STREAM_PORT, admin)
	if err != nil {
		util.TT.Fatal(err)
	}
	mgr.StartCoordinator(config)
	time.Sleep(time.Duration(3000) * time.Millisecond)

	common.Infof("StreamEnd Test Cleanup ...")
	cleanupStreamMgrStreamEndTest(mgr)

	common.Infof("***** Run StreamEnd Test ...")
	ch := mgr.GetStabilityTimestampChannel(common.MAINT_STREAM)
	go runStreamEndTestReceiver(ch, donech)

	common.Infof("Setup data for StreamEnd Test")
	changeTopologyForStreamEndTest(mgr)
	<-donech

	common.Infof("**** StreamEnd Test Cleanup ...")
	cleanupStreamMgrStreamEndTest(mgr)
	mgr.CleanupTopology()
	mgr.CleanupStabilityTimestamp()
	time.Sleep(time.Duration(1000) * time.Millisecond)

	common.Infof("**** Stop TestStreamMgr. Tearing down ")
	mgr.Close()
	time.Sleep(time.Duration(1000) * time.Millisecond)

	common.Infof("**** Finish StreamEnd Test ****************************************")
}

// clean up
func cleanupStreamMgrStreamEndTest(mgr *manager.IndexManager) {

	_, err := mgr.GetIndexDefnById(common.IndexDefnId(405))
	if err != nil {
		common.Infof("StreamMgrTest.cleanupStreamMgrStreamEndTest() :  cannot find index defn stream_mgr_stream_end_test.  No cleanup ...")
	} else {
		common.Infof("StreamMgrTest.cleanupStreamMgrStreamEndTest() :  found index defn stream_mgr_stream_end_test.  Cleaning up ...")

		err = mgr.HandleDeleteIndexDDL(common.IndexDefnId(405))
		if err != nil {
			util.TT.Fatal(err)
		}
		time.Sleep(time.Duration(1000) * time.Millisecond)

		// double check if we have really cleaned up
		_, err := mgr.GetIndexDefnById(common.IndexDefnId(405))
		if err == nil {
			util.TT.Fatal("StreamMgrTest.cleanupStreamMgrStreamEndTest(): Cannot clean up index defn stream_mgr_stream_end_test")
		}
	}

	time.Sleep(time.Duration(1000) * time.Millisecond)
}

// run test
func runStreamEndTestReceiver(ch chan *common.TsVbuuid, donech chan bool) {

	common.Infof("Run StreamEnd Test Receiver")
	defer close(donech)

	// wait for the sync message to arrive
	ticker := time.NewTicker(time.Duration(60) * time.Second)
	for {
		select {
		case ts := <-ch:
			if ts.Seqnos[10] == 405 {
				common.Infof("****** runStreamEndTestReceiver() receive correct stability timestamp")
				return
			}
		case <-ticker.C:
			common.Infof("****** runStreamEndTestReceiver() : timeout")
			util.TT.Fatal("runSyncTestReceiver(): Timeout waiting to receive timestamp to arrive")
		}
	}

	common.Infof("runStreamEndTestReceiver() done")
}

// start up
func changeTopologyForStreamEndTest(mgr *manager.IndexManager) {

	// Add a new index definition : 405
	idxDefn := &common.IndexDefn{
		DefnId:          common.IndexDefnId(405),
		Name:            "stream_mgr_stream_end_test",
		Using:           common.ForestDB,
		Bucket:          "Default",
		IsPrimary:       false,
		SecExprs:        []string{"Testing"},
		ExprType:        common.N1QL,
		PartitionScheme: common.HASH,
		PartitionKey:    "Testing"}

	common.Infof("Run Sync Test : Create Index Defn 405")
	if err := mgr.HandleCreateIndexDDL(idxDefn); err != nil {
		util.TT.Fatal(err)
	}
	// Wait so there is no race condition.
	time.Sleep(time.Duration(1000) * time.Millisecond)

	// Update the index definition to ready
	common.Infof("Run Sync Test : Update Index Defn 405 to READY")
	topology, err := mgr.GetTopologyByBucket("Default")
	if err != nil {
		util.TT.Fatal(err)
	}
	topology.ChangeStateForIndexInstByDefn(common.IndexDefnId(405), common.INDEX_STATE_CREATED, common.INDEX_STATE_READY)
	if err := mgr.SetTopologyByBucket("Default", topology); err != nil {
		util.TT.Fatal(err)
	}
}

func (c *streamEndTestProjectorClient) sendSync(timestamps []*protobuf.TsVbuuid) {

	common.Infof("streamEndTestProjectorClient.sendSync() ")

	if len(timestamps) != 1 {
		util.TT.Fatal("streamEndTestProjectorClient.sendSync(): More than one timestamp sent to fake projector. Num = %v", len(timestamps))
	}

	seqno, _, _, _, err := timestamps[0].Get(uint16(10))
	if err != nil {
		util.TT.Fatal(err)
	}

	p := util.NewFakeProjector(manager.COORD_MAINT_STREAM_PORT)
	go p.Run(c.donech)

	payloads := make([]*common.VbKeyVersions, 0, 2000)

	payload := common.NewVbKeyVersions("Default", 10 /* vb */, 1, 10)
	kv := common.NewKeyVersions(seqno, []byte("document-name"), 1)
	kv.AddStreamBegin()
	kv.AddSync()
	payload.AddKeyVersions(kv)
	payloads = append(payloads, payload)

	// send payload
	if err := p.Client.SendKeyVersions(payloads, true); err != nil {
		util.TT.Fatal(err)
	}
}

func (c *streamEndTestProjectorClient) sendStreamEnd(instances []*protobuf.Instance) {

	common.Infof("streamEndTestProjectorClient.sendStreamEnd() ")

	if len(instances) != 1 {
		util.TT.Fatal("streamEndTestProjectorClient.sendStreamEnd(): More than one index instance sent to fake projector")
	}

	for _, inst := range instances {
		if inst.GetIndexInstance().GetDefinition().GetDefnID() == uint64(405) {

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
			kv := common.NewKeyVersions(405, []byte("document-name"), 1)
			kv.AddSync()
			kv.AddStreamEnd()
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

func (c *streamEndTestProjectorClient) MutationTopicRequest(topic, endpointType string,
	reqTimestamps []*protobuf.TsVbuuid, instances []*protobuf.Instance) (*protobuf.TopicResponse, error) {

	if len(reqTimestamps) == 0 {
		util.TT.Fatal("testProjectorClient.MutationTopicRequest(): reqTimestamps is nil")
	}

	c.sendStreamEnd(instances)

	response := new(protobuf.TopicResponse)
	response.Topic = &topic
	response.InstanceIds = make([]uint64, len(instances))
	for i, inst := range instances {
		response.InstanceIds[i] = inst.GetIndexInstance().GetInstId()
	}
	response.ActiveTimestamps = reqTimestamps
	response.RollbackTimestamps = nil
	response.Err = nil

	return response, nil
}

func (c *streamEndTestProjectorClient) DelInstances(topic string, uuids []uint64) error {
	return nil
}

func (c *streamEndTestProjectorClient) RepairEndpoints(topic string, endpoints []string) error {
	return nil
}

func (c *streamEndTestProjectorClient) InitialRestartTimestamp(pooln, bucketn string) (*protobuf.TsVbuuid, error) {

	newTs := protobuf.NewTsVbuuid("default", bucketn, manager.NUM_VB)
	for i := 0; i < manager.NUM_VB; i++ {
		newTs.Append(uint16(i), uint64(i), uint64(1234), uint64(0), uint64(0))
	}
	return newTs, nil
}

func (c *streamEndTestProjectorClient) RestartVbuckets(topic string,
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

func (p *streamEndTestProjectorClientFactory) GetClientForNode(server string) manager.ProjectorStreamClient {

	c := new(streamEndTestProjectorClient)
	c.donech = p.donech
	return c
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// testProjectorClientFactory
////////////////////////////////////////////////////////////////////////////////////////////////////

func (p *streamEndTestProjectorClientEnv) GetNodeListForBuckets(buckets []string) (map[string]string, error) {

	common.Infof("streamEndTestProjectorClientEnv.GetNodeListForBuckets() ")
	nodes := make(map[string]string)
	nodes["127.0.0.1"] = "127.0.0.1"
	return nodes, nil
}

func (p *streamEndTestProjectorClientEnv) GetNodeListForTimestamps(timestamps []*common.TsVbuuid) (map[string][]*protobuf.TsVbuuid, error) {

	common.Infof("streamEndTestProjectorClientEnv.GetNodeListForTimestamps() ")
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

func (p *streamEndTestProjectorClientEnv) FilterTimestampsForNode(timestamps []*protobuf.TsVbuuid, node string) ([]*protobuf.TsVbuuid, error) {
	return timestamps, nil
}
