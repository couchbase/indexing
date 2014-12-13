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
	"sync"
)

// For this test, use Index Defn Id from 400 - 410

// implement ProjectorStreamClientFactory
type deleteTestProjectorClientFactory struct {
	donech chan bool
}

// implement ProjectorClientEnv
type deleteTestProjectorClientEnv struct {
}

// projector client specific for DELETE_TEST
// implement ProjectorStreamClient
type deleteTestProjectorClient struct {
	server string
	donech chan bool
}

var delete_test_status map[uint64]*protobuf.Instance
var delete_test_once sync.Once	

////////////////////////////////////////////////////////////////////////////////////////////////////
// Test Driver
////////////////////////////////////////////////////////////////////////////////////////////////////

func TestStreamMgr_Delete(t *testing.T) {

	common.LogEnable()
	common.SetLogLevel(common.LogLevelDebug)
	util.TT = t

	old_value := manager.NUM_VB	
	manager.NUM_VB = 16 
	defer func() {manager.NUM_VB = old_value}()

	// Running test
	runDeleteTest()
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Delete Test
// This test deletes creates 3 index on 2 different buckets across 2 different (fake) projectors.
// One index will then be deleted.  A sync message is sent after the index is deleted.  The
// stabiltiy timestamp should have the seqno for both buckets and from the two remaining indexes.
// This test covers the following functions:
// 1) Coordinator Master deletes index and updates topology
// 2) Stream Manager receives the topoogy update and determine the index to delete
// 3) StreamAdmin process delete instances
// 4) Stream Manager handle requests to more than one projectors
////////////////////////////////////////////////////////////////////////////////////////////////////

func runDeleteTest() {

	common.Infof("**** Run Delete Test *******************************************")
	delete_test_status = make(map[uint64]*protobuf.Instance)

	common.Infof("***** Start TestStreamMgr ") 
	var requestAddr = "localhost:9885"
	var leaderAddr = "localhost:9884"
	var config = "./config.json"

	common.Infof("Start Index Manager")
	donech := make(chan bool)
	factory := new(deleteTestProjectorClientFactory)
	factory.donech = donech
	env := new(deleteTestProjectorClientEnv)
	admin := manager.NewProjectorAdmin(factory, env, nil)
	mgr, err := manager.NewIndexManagerInternal(requestAddr, leaderAddr, config, admin)
	if err != nil {
		util.TT.Fatal(err)
	}
	time.Sleep(time.Duration(3000) * time.Millisecond)
	
	common.Infof("Delete Test Cleanup ...")
	cleanupStreamMgrDeleteTest(mgr)

	common.Infof("***** Run Delete Test :  setup")
	ch := mgr.GetStabilityTimestampChannel(common.MAINT_STREAM)
	go runDeleteTestReceiver(ch, donech)

	common.Infof("***** Run Delete Test :  add 3 indexes in 2 buckets to trigger topology change")
	addIndexForDeleteTest(mgr)

	common.Infof("***** Run Delete Test :  delete index in 'Default' buckets to trigger topology change")
	deleteIndexForDeleteTest(mgr)

	common.Infof("***** Run Delete Test :  wait for stability timestamp to arrive")
	<-donech

	common.Infof("**** Delete Test Cleanup ...")
	cleanupStreamMgrDeleteTest(mgr)
	mgr.CleanupTopology()
	mgr.CleanupStabilityTimestamp()
	time.Sleep(time.Duration(1000) * time.Millisecond)

	common.Infof("**** Stop TestStreamMgr. Tearing down ") 
	mgr.Close()
	time.Sleep(time.Duration(1000) * time.Millisecond)
	
	common.Infof("**** Finish Delete Test ***********************************************************")
}

func addIndexForDeleteTest(mgr *manager.IndexManager) {

	//
	// Add a new index definition : 401 (Default)
	//
	idxDefn := &common.IndexDefn{
		DefnId:          common.IndexDefnId(401),
		Name:            "stream_mgr_delete_test_1",
		Using:           common.ForestDB,
		Bucket:          "Default",
		IsPrimary:       false,
		SecExprs:        []string{"Testing"},
		ExprType:        common.N1QL,
		PartitionScheme: common.HASH,
		PartitionKey:    "Testing"}

	common.Infof("Run Delete Test : Create Index Defn 401")
	if err := mgr.HandleCreateIndexDDL(idxDefn); err != nil {
		util.TT.Fatal(err)
	}
	// Wait so there is no race condition.
	time.Sleep(time.Duration(1000) * time.Millisecond)

	// Update the index definition to ready
	common.Infof("Run Delete Test : Update Index Defn 401 to READY")
	topology, err := mgr.GetTopologyByBucket("Default")
	if err != nil {
		util.TT.Fatal(err)
	}
	topology.ChangeStateForIndexInstByDefn(common.IndexDefnId(401), common.INDEX_STATE_CREATED, common.INDEX_STATE_READY)
	if err := mgr.SetTopologyByBucket("Default", topology); err != nil {
		util.TT.Fatal(err)
	}

	//
	// Add a new index definition : 402 (Default)
	//
	idxDefn = &common.IndexDefn{
		DefnId:          common.IndexDefnId(402),
		Name:            "stream_mgr_delete_test_2",
		Using:           common.ForestDB,
		Bucket:          "Default",
		IsPrimary:       false,
		SecExprs:        []string{"Testing"},
		ExprType:        common.N1QL,
		PartitionScheme: common.HASH,
		PartitionKey:    "Testing"}

	common.Infof("Run Delete Test : Create Index Defn 402")
	if err := mgr.HandleCreateIndexDDL(idxDefn); err != nil {
		util.TT.Fatal(err)
	}
	// Wait so there is no race condition.
	time.Sleep(time.Duration(1000) * time.Millisecond)

	// Update the index definition to ready
	common.Infof("Run Delete Test : Update Index Defn 402 to READY")
	topology, err = mgr.GetTopologyByBucket("Default")
	if err != nil {
		util.TT.Fatal(err)
	}
	topology.ChangeStateForIndexInstByDefn(common.IndexDefnId(402), common.INDEX_STATE_CREATED, common.INDEX_STATE_READY)
	if err := mgr.SetTopologyByBucket("Default", topology); err != nil {
		util.TT.Fatal(err)
	}

	//
	// Add a new index definition : 403 (Defaultxx)
	//
	idxDefn = &common.IndexDefn{
		DefnId:          common.IndexDefnId(403),
		Name:            "stream_mgr_delete_test_3",
		Using:           common.ForestDB,
		Bucket:          "Defaultxx",
		IsPrimary:       false,
		SecExprs:        []string{"Testing"},
		ExprType:        common.N1QL,
		PartitionScheme: common.HASH,
		PartitionKey:    "Testing"}

	common.Infof("Run Delete Test : Create Index Defn 403")
	if err := mgr.HandleCreateIndexDDL(idxDefn); err != nil {
		util.TT.Fatal(err)
	}
	// Wait so there is no race condition.
	time.Sleep(time.Duration(1000) * time.Millisecond)

	// Update the index definition to ready
	common.Infof("Run Delete Test : Update Index Defn 403 to READY")
	topology, err = mgr.GetTopologyByBucket("Defaultxx")
	if err != nil {
		util.TT.Fatal(err)
	}
	topology.ChangeStateForIndexInstByDefn(common.IndexDefnId(403), common.INDEX_STATE_CREATED, common.INDEX_STATE_READY)
	if err := mgr.SetTopologyByBucket("Defaultxx", topology); err != nil {
		util.TT.Fatal(err)
	}
}

func deleteIndexForDeleteTest(mgr *manager.IndexManager) {

	//
	// Delete a new index definition : 401 (Default)
	//
	common.Infof("Run Delete Test : Delete Index Defn 401")
	if err := mgr.HandleDeleteIndexDDL("Default", "stream_mgr_delete_test_1"); err != nil {
		util.TT.Fatal(err)
	}
	// Wait so there is no race condition.
	//time.Sleep(time.Duration(1000) * time.Millisecond)
}

// clean up
func cleanupStreamMgrDeleteTest(mgr *manager.IndexManager) {

	_, err := mgr.GetIndexDefnByName("Default", "stream_mgr_delete_test_1")
	if err != nil {
		common.Infof("StreamMgrTest.cleanupStreamMgrSyncTest() :  cannot find index defn stream_mgr_delete_test_1.  No cleanup ...")
	} else {
		common.Infof("StreamMgrTest.cleanupStreamMgrSyncTest() :  found index defn stream_mgr_delete_test_1.  Cleaning up ...")

		err = mgr.HandleDeleteIndexDDL("Default", "stream_mgr_delete_test_1")
		if err != nil {
			util.TT.Fatal(err)
		}
		time.Sleep(time.Duration(1000) * time.Millisecond)

		// double check if we have really cleaned up
		_, err = mgr.GetIndexDefnByName("Default", "stream_mgr_delete_test_1")
		if err == nil {
			util.TT.Fatal("StreamMgrTest.cleanupStreamMgrSyncTest(): Cannot clean up index defn stream_mgr_delete_test_1")
		}
	}

	_, err = mgr.GetIndexDefnByName("Default", "stream_mgr_delete_test_2")
	if err != nil {
		common.Infof("StreamMgrTest.cleanupStreamMgrSyncTest() :  cannot find index defn stream_mgr_delete_test_2.  No cleanup ...")
	} else {
		common.Infof("StreamMgrTest.cleanupStreamMgrSyncTest() :  found index defn stream_mgr_delete_test_2.  Cleaning up ...")

		err = mgr.HandleDeleteIndexDDL("Default", "stream_mgr_delete_test_2")
		if err != nil {
			util.TT.Fatal(err)
		}
		time.Sleep(time.Duration(1000) * time.Millisecond)

		// double check if we have really cleaned up
		_, err = mgr.GetIndexDefnByName("Default", "stream_mgr_delete_test_2")
		if err == nil {
			util.TT.Fatal("StreamMgrTest.cleanupStreamMgrSyncTest(): Cannot clean up index defn stream_mgr_delete_test_2")
		}
	}

	_, err = mgr.GetIndexDefnByName("Defaultxx", "stream_mgr_delete_test_3")
	if err != nil {
		common.Infof("StreamMgrTest.cleanupStreamMgrSyncTest() :  cannot find index defn stream_mgr_delete_test_3.  No cleanup ...")
	} else {
		common.Infof("StreamMgrTest.cleanupStreamMgrSyncTest() :  found index defn stream_mgr_delete_test_3.  Cleaning up ...")

		err = mgr.HandleDeleteIndexDDL("Defaultxx", "stream_mgr_delete_test_3")
		if err != nil {
			util.TT.Fatal(err)
		}
		time.Sleep(time.Duration(1000) * time.Millisecond)

		// double check if we have really cleaned up
		_, err = mgr.GetIndexDefnByName("Defaultxx", "stream_mgr_delete_test_3")
		if err == nil {
			util.TT.Fatal("StreamMgrTest.cleanupStreamMgrSyncTest(): Cannot clean up index defn stream_mgr_delete_test_3")
		}
	}

	time.Sleep(time.Duration(1000) * time.Millisecond)
}

// run test receiver
func runDeleteTestReceiver(ch chan *common.TsVbuuid, donech chan bool) {

	common.Infof("Run Delete Test Receiver")
	defer close(donech)

	// wait for the delete message to arrive
	bucket1 := false
	bucket2 := false
	ticker := time.NewTicker(time.Duration(60) * time.Second)
	for {
		select {
		case ts := <-ch:
			common.Infof("****** runDeleteTestReceiver() : receive timestamp. bucket %s seqno[10] %v seqno[11] %v",
				ts.Bucket, ts.Seqnos[10], ts.Seqnos[11])
			common.Infof("****** runDeleteTestReceiver() : receive timestamp. bucket %s seqno[12] %v seqno[13] %v",
				ts.Bucket, ts.Seqnos[12], ts.Seqnos[13])

			if ts.Bucket == "Default" && ts.Seqnos[10] == 401 && ts.Seqnos[11] == 402 {
				common.Infof("****** runDeleteTestReceiver() receive correct stability timestamp for bucket Default")
				bucket1 = true
			}

			if ts.Bucket == "Defaultxx" && ts.Seqnos[12] >= 403 && ts.Seqnos[13] >= 404 {
				common.Infof("****** runDeleteTestReceiver() receive correct stability timestamp for bucket Defaultxx")
				bucket2 = true
			}

			if bucket1 && bucket2 {
				return
			}

		case <-ticker.C:
			common.Infof("****** runDeleteTestReceiver() : timeout")
			util.TT.Fatal("runDeleteTestReceiver1(): Timeout waiting to receive timestamp to arrive")
		}
	}

	common.Infof("runDeleteTestReceiver1() done")
}

func (c *deleteTestProjectorClient) sendSync() {

	common.Infof("deleteTestProjectorClient.sendSync() server %v", c.server)

	p := util.NewFakeProjector(manager.COORD_MAINT_STREAM_PORT)
	go p.Run(c.donech)

	// create an array of KeyVersions
	payloads := make([]*common.VbKeyVersions, 0, 4000)
	
	delete_test_once.Do(func() {	
		common.Infof("deleteTestProjectorClient.sendSync() sending streamBegin %v", c.server)
		
		// send StreamBegin for all vbuckets
		for i := 0; i < manager.NUM_VB; i++ {
			if i != 10 && i != 11 {	
				payload := common.NewVbKeyVersions("Default", uint16(i) /* vb */, 1, 10)
				kv := common.NewKeyVersions(1, []byte("document-name"), 1)
				kv.AddStreamBegin()
				kv.AddSync()
				payload.AddKeyVersions(kv)
				payloads = append(payloads, payload)
			}
		}

		for i := 0; i < manager.NUM_VB; i++ {
			if i != 12 && i != 13 {
				payload := common.NewVbKeyVersions("Defaultxx", uint16(i) /* vb */, 1, 10)
				kv := common.NewKeyVersions(1, []byte("document-name"), 1)
				kv.AddStreamBegin()
				kv.AddSync()
				payload.AddKeyVersions(kv)
				payloads = append(payloads, payload)
			}
		}
	})

	// bucket <Default>, node <127.0.0.1>  -> vb 10, seqno 401
	// bucket <Default>, node <127.0.0.2>  -> vb 11, seqno 402
	// bucket <Defaultxx>, node <127.0.0.1> -> vb 12, seqno 403
	// bucket <Defaultxx>, node <127.0.0.2> -> vb 13, seqno 404
	for _, inst := range delete_test_status {
		seqno := 0
		vb := 0
		bucket := inst.GetIndexInstance().GetDefinition().GetBucket()
		if bucket == "Default" {
			if c.server == "127.0.0.1" {
				seqno = 401 
				vb = 10
			} else if c.server == "127.0.0.2" {
				seqno = 402 
				vb = 11 
			}
		} else if bucket == "Defaultxx" {
			if c.server == "127.0.0.1" {
				seqno = 403 
				vb = 12 
			} else if c.server == "127.0.0.2" {
				seqno = 404
				vb = 13 
			}
		}

		common.Infof("deleteTestProjectorClient.sendSync() for node %v and bucket %v vbucket %v seqno %d", 
			c.server, bucket, vb, seqno)

		// Create Sync Message
		payload := common.NewVbKeyVersions(bucket, uint16(vb), 1, 10)
		kv := common.NewKeyVersions(uint64(seqno), []byte("document-name"), 1)
		kv.AddStreamBegin()
		kv.AddSync()
		payload.AddKeyVersions(kv)
		payloads = append(payloads, payload)
	}

	// Send payload
	if len(payloads) != 0 {
		common.Infof("deleteTestProjectorClient.sendSync() sending payloads to stream manager for %s", c.server)
		err := p.Client.SendKeyVersions(payloads, true)
		if err != nil {
			util.TT.Fatal(err)
		}
	}
}

func (c *deleteTestProjectorClient) MutationTopicRequest(topic, endpointType string,
	reqTimestamps []*protobuf.TsVbuuid, instances []*protobuf.Instance) (*protobuf.TopicResponse, error) {

	if len(reqTimestamps) == 0 {
		util.TT.Fatal("deleteTestProjectorClient.MutationTopicRequest(): reqTimestamps is nil")
	}

	for _, inst := range instances {
		delete_test_status[inst.GetIndexInstance().GetInstId()] = inst
	}

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

func (c *deleteTestProjectorClient) DelInstances(topic string, uuids []uint64) error {

	common.Infof("deleteTestProjectorClient.DelInstances() for server %v", c.server)

	for _, uuid := range uuids {
		delete(delete_test_status, uuid)
	}
	c.sendSync()

	return nil
}

func (c *deleteTestProjectorClient) RepairEndpoints(topic string, endpoints []string) error {
	return nil
}

func (c *deleteTestProjectorClient) InitialRestartTimestamp(pooln, bucketn string) (*protobuf.TsVbuuid, error) {

	newTs := protobuf.NewTsVbuuid("default", bucketn, manager.NUM_VB)
	for i := 0; i < manager.NUM_VB; i++ {
		newTs.Append(uint16(i), uint64(i), uint64(1234), uint64(0), uint64(0))
	}
	return newTs, nil
}

func (c *deleteTestProjectorClient) RestartVbuckets(topic string, 
	restartTimestamps []*protobuf.TsVbuuid) (*protobuf.TopicResponse, error) {
	return nil, nil
}
	
////////////////////////////////////////////////////////////////////////////////////////////////////
// testProjectorClientFactory
////////////////////////////////////////////////////////////////////////////////////////////////////

func (p *deleteTestProjectorClientFactory) GetClientForNode(server string) manager.ProjectorStreamClient {

	client := new(deleteTestProjectorClient)
	client.server = server
	client.donech = p.donech
	return client
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// testProjectorClientFactory
////////////////////////////////////////////////////////////////////////////////////////////////////

func (p *deleteTestProjectorClientEnv) GetNodeListForBuckets(buckets []string) (map[string]string, error) {

	common.Infof("deleteTestProjectorClientEnv.GetNodeListForBuckets() ")
	nodes := make(map[string]string)
	nodes["127.0.0.1"] = "127.0.0.1"
	nodes["127.0.0.2"] = "127.0.0.2"
	return nodes, nil
}

func (p *deleteTestProjectorClientEnv) GetNodeListForTimestamps(timestamps []*common.TsVbuuid) (map[string][]*protobuf.TsVbuuid, error) {
	return nil, nil
}

func (p *deleteTestProjectorClientEnv) FilterTimestampsForNode(timestamps []*protobuf.TsVbuuid, node string) ([]*protobuf.TsVbuuid, error) {
	return timestamps, nil
}
