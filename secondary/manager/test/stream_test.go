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
	"sync"
)

// For this test, use Index Defn Id from 400 - 410

type fakeProjector struct {
	client *dataport.Client
}

// implement ProjectorStreamClientFactory
type testProjectorClientFactory struct {
}

// implement ProjectorClientEnv
type testProjectorClientEnv struct {
}

// projector client specific for SYNC_TEST
// implement ProjectorStreamClient
type syncTestProjectorClient struct {
}

// projector client specific for DELETE_TEST
// implement ProjectorStreamClient
type deleteTestProjectorClient struct {
	server string
}

// projector client specific for STREAM_END_TEST
// implement ProjectorStreamClient
type streamEndTestProjectorClient struct {
}

// projector client specific for TIMER_TEST
// implement ProjectorStreamClient
type timerTestProjectorClient struct {
}

const (
	NO_TEST     uint8 = 0
	SYNC_TEST         = 1
	DELETE_TEST       = 2
	STREAM_END_TEST   = 3
	TIMER_TEST        = 4
)

var TT *testing.T
var donech chan bool
var test uint8
var delete_test_status map[uint64]*protobuf.Instance
var delete_test_once sync.Once	

////////////////////////////////////////////////////////////////////////////////////////////////////
// Test Driver
////////////////////////////////////////////////////////////////////////////////////////////////////

func TestStreamMgr(t *testing.T) {

	common.LogEnable()
	common.SetLogLevel(common.LogLevelDebug)
	test = NO_TEST
	TT = t

	old_value := manager.NUM_VB	
	manager.NUM_VB = 16 
	defer func() {manager.NUM_VB = old_value}()

	// Running test
	runSyncTest()
	runDeleteTest()
	runStreamEndTest()
	runTimerTest()
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
	defer func() { test = NO_TEST }()

	common.Infof("**** Run Sync Test ******************************************")
	test = SYNC_TEST
	
	common.Infof("***** Start TestStreamMgr ") 
	var requestAddr = "localhost:9885"
	var leaderAddr = "localhost:9884"
	var config = "./config.json"

	common.Infof("Start Index Manager")
	factory := new(testProjectorClientFactory)
	env := new(testProjectorClientEnv)
	admin := manager.NewProjectorAdmin(factory, env)
	mgr, err := manager.NewIndexManagerInternal(requestAddr, leaderAddr, config, admin)
	if err != nil {
		TT.Fatal(err)
	}
	time.Sleep(time.Duration(3000) * time.Millisecond)
	
	common.Infof("Sync Test Cleanup ...")
	cleanupStreamMgrSyncTest(mgr)

	common.Infof("***** Run Sync Test ...")
	donech = make(chan bool)
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
			TT.Fatal("runSyncTestReceiver(): Timeout waiting to receive timestamp to arrive")
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
		TT.Fatal(err)
	}
	// Wait so there is no race condition.
	time.Sleep(time.Duration(1000) * time.Millisecond)

	// Update the index definition to ready
	common.Infof("Run Sync Test : Update Index Defn 400 to READY")
	topology, err := mgr.GetTopologyByBucket("Default")
	if err != nil {
		TT.Fatal(err)
	}
	topology.ChangeStateForIndexInstByDefn(common.IndexDefnId(400), common.INDEX_STATE_CREATED, common.INDEX_STATE_READY)
	if err := mgr.SetTopologyByBucket("Default", topology); err != nil {
		TT.Fatal(err)
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
			TT.Fatal(err)
		}
		time.Sleep(time.Duration(1000) * time.Millisecond)

		// double check if we have really cleaned up
		_, err := mgr.GetIndexDefnByName("Default", "stream_mgr_sync_test")
		if err == nil {
			TT.Fatal("StreamMgrTest.cleanupStreamMgrSyncTest(): Cannot clean up index defn stream_mgr_sync_test")
		}
	}
	
	time.Sleep(time.Duration(1000) * time.Millisecond)
}

func (c *syncTestProjectorClient) sendSync(instances []*protobuf.Instance) {

	common.Infof("syncTestProjectorClient.sendSync() ")

	if len(instances) != 1 {
		TT.Fatal("syncTestProjectorClient.sendSync(): More than one index instance sent to fake projector")
	}

	for _, inst := range instances {
		if inst.GetIndexInstance().GetDefinition().GetDefnID() == uint64(400) {

			p := newFakeProjector(manager.COORD_MAINT_STREAM_PORT)
			go p.run(donech)

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
			err := p.client.SendKeyVersions(payloads, true)
			if err != nil {
				TT.Fatal(err)
			}
		}
	}
}

func (c *syncTestProjectorClient) MutationTopicRequest(topic, endpointType string,
	reqTimestamps []*protobuf.TsVbuuid, instances []*protobuf.Instance) (*protobuf.TopicResponse, error) {

	if len(reqTimestamps) == 0 {
		TT.Fatal("testProjectorClient.MutationTopicRequest(): reqTimestamps is nil")
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
	defer func() { test = NO_TEST }()

	common.Infof("**** Run Delete Test *******************************************")
	test = DELETE_TEST
	delete_test_status = make(map[uint64]*protobuf.Instance)

	common.Infof("***** Start TestStreamMgr ") 
	var requestAddr = "localhost:9885"
	var leaderAddr = "localhost:9884"
	var config = "./config.json"

	common.Infof("Start Index Manager")
	factory := new(testProjectorClientFactory)
	env := new(testProjectorClientEnv)
	admin := manager.NewProjectorAdmin(factory, env)
	mgr, err := manager.NewIndexManagerInternal(requestAddr, leaderAddr, config, admin)
	if err != nil {
		TT.Fatal(err)
	}
	time.Sleep(time.Duration(3000) * time.Millisecond)
	
	common.Infof("Delete Test Cleanup ...")
	cleanupStreamMgrDeleteTest(mgr)

	common.Infof("***** Run Delete Test :  setup")
	donech = make(chan bool)
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
		TT.Fatal(err)
	}
	// Wait so there is no race condition.
	time.Sleep(time.Duration(1000) * time.Millisecond)

	// Update the index definition to ready
	common.Infof("Run Delete Test : Update Index Defn 401 to READY")
	topology, err := mgr.GetTopologyByBucket("Default")
	if err != nil {
		TT.Fatal(err)
	}
	topology.ChangeStateForIndexInstByDefn(common.IndexDefnId(401), common.INDEX_STATE_CREATED, common.INDEX_STATE_READY)
	if err := mgr.SetTopologyByBucket("Default", topology); err != nil {
		TT.Fatal(err)
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
		TT.Fatal(err)
	}
	// Wait so there is no race condition.
	time.Sleep(time.Duration(1000) * time.Millisecond)

	// Update the index definition to ready
	common.Infof("Run Delete Test : Update Index Defn 402 to READY")
	topology, err = mgr.GetTopologyByBucket("Default")
	if err != nil {
		TT.Fatal(err)
	}
	topology.ChangeStateForIndexInstByDefn(common.IndexDefnId(402), common.INDEX_STATE_CREATED, common.INDEX_STATE_READY)
	if err := mgr.SetTopologyByBucket("Default", topology); err != nil {
		TT.Fatal(err)
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
		TT.Fatal(err)
	}
	// Wait so there is no race condition.
	time.Sleep(time.Duration(1000) * time.Millisecond)

	// Update the index definition to ready
	common.Infof("Run Delete Test : Update Index Defn 403 to READY")
	topology, err = mgr.GetTopologyByBucket("Defaultxx")
	if err != nil {
		TT.Fatal(err)
	}
	topology.ChangeStateForIndexInstByDefn(common.IndexDefnId(403), common.INDEX_STATE_CREATED, common.INDEX_STATE_READY)
	if err := mgr.SetTopologyByBucket("Defaultxx", topology); err != nil {
		TT.Fatal(err)
	}
}

func deleteIndexForDeleteTest(mgr *manager.IndexManager) {

	//
	// Delete a new index definition : 401 (Default)
	//
	common.Infof("Run Delete Test : Delete Index Defn 401")
	if err := mgr.HandleDeleteIndexDDL("Default", "stream_mgr_delete_test_1"); err != nil {
		TT.Fatal(err)
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
			TT.Fatal(err)
		}
		time.Sleep(time.Duration(1000) * time.Millisecond)

		// double check if we have really cleaned up
		_, err = mgr.GetIndexDefnByName("Default", "stream_mgr_delete_test_1")
		if err == nil {
			TT.Fatal("StreamMgrTest.cleanupStreamMgrSyncTest(): Cannot clean up index defn stream_mgr_delete_test_1")
		}
	}

	_, err = mgr.GetIndexDefnByName("Default", "stream_mgr_delete_test_2")
	if err != nil {
		common.Infof("StreamMgrTest.cleanupStreamMgrSyncTest() :  cannot find index defn stream_mgr_delete_test_2.  No cleanup ...")
	} else {
		common.Infof("StreamMgrTest.cleanupStreamMgrSyncTest() :  found index defn stream_mgr_delete_test_2.  Cleaning up ...")

		err = mgr.HandleDeleteIndexDDL("Default", "stream_mgr_delete_test_2")
		if err != nil {
			TT.Fatal(err)
		}
		time.Sleep(time.Duration(1000) * time.Millisecond)

		// double check if we have really cleaned up
		_, err = mgr.GetIndexDefnByName("Default", "stream_mgr_delete_test_2")
		if err == nil {
			TT.Fatal("StreamMgrTest.cleanupStreamMgrSyncTest(): Cannot clean up index defn stream_mgr_delete_test_2")
		}
	}

	_, err = mgr.GetIndexDefnByName("Defaultxx", "stream_mgr_delete_test_3")
	if err != nil {
		common.Infof("StreamMgrTest.cleanupStreamMgrSyncTest() :  cannot find index defn stream_mgr_delete_test_3.  No cleanup ...")
	} else {
		common.Infof("StreamMgrTest.cleanupStreamMgrSyncTest() :  found index defn stream_mgr_delete_test_3.  Cleaning up ...")

		err = mgr.HandleDeleteIndexDDL("Defaultxx", "stream_mgr_delete_test_3")
		if err != nil {
			TT.Fatal(err)
		}
		time.Sleep(time.Duration(1000) * time.Millisecond)

		// double check if we have really cleaned up
		_, err = mgr.GetIndexDefnByName("Defaultxx", "stream_mgr_delete_test_3")
		if err == nil {
			TT.Fatal("StreamMgrTest.cleanupStreamMgrSyncTest(): Cannot clean up index defn stream_mgr_delete_test_3")
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
			TT.Fatal("runDeleteTestReceiver1(): Timeout waiting to receive timestamp to arrive")
		}
	}

	common.Infof("runDeleteTestReceiver1() done")
}

func (c *deleteTestProjectorClient) sendSync() {

	common.Infof("deleteTestProjectorClient.sendSync() server %v", c.server)

	p := newFakeProjector(manager.COORD_MAINT_STREAM_PORT)
	go p.run(donech)

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
		err := p.client.SendKeyVersions(payloads, true)
		if err != nil {
			TT.Fatal(err)
		}
	}
}

func (c *deleteTestProjectorClient) MutationTopicRequest(topic, endpointType string,
	reqTimestamps []*protobuf.TsVbuuid, instances []*protobuf.Instance) (*protobuf.TopicResponse, error) {

	if len(reqTimestamps) == 0 {
		TT.Fatal("deleteTestProjectorClient.MutationTopicRequest(): reqTimestamps is nil")
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
	response.ActiveTimestamps = make([]*protobuf.TsVbuuid, 1)
	response.ActiveTimestamps[0] = reqTimestamps[0]
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

	newTs := protobuf.NewTsVbuuid("default", "Default", manager.NUM_VB)
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
// StreamEnd Test
// 1) This test will create a new index.  
// 2) Upon notifying of the new index, the fake projector will push a StreamEnd message to the stream manager.  
// 3) The stream manager will then call back to the fake projector to repair the vubcket.  If the correct vbucket 
//    is received, the fake projector will return without error.   
// 4) The fake projector will also push a sync message to the data port for the coordinator to broadcast a timestamp. 
////////////////////////////////////////////////////////////////////////////////////////////////////

func runStreamEndTest() {
	defer func() { test = NO_TEST }()

	common.Infof("**** Run StreamEnd Test ******************************************")
	test = STREAM_END_TEST

	common.Infof("***** Start TestStreamMgr ") 
	var requestAddr = "localhost:9885"
	var leaderAddr = "localhost:9884"
	var config = "./config.json"

	common.Infof("Start Index Manager")
	factory := new(testProjectorClientFactory)
	env := new(testProjectorClientEnv)
	admin := manager.NewProjectorAdmin(factory, env)
	mgr, err := manager.NewIndexManagerInternal(requestAddr, leaderAddr, config, admin)
	if err != nil {
		TT.Fatal(err)
	}
	time.Sleep(time.Duration(3000) * time.Millisecond)
	
	common.Infof("StreamEnd Test Cleanup ...")
	cleanupStreamMgrStreamEndTest(mgr)

	common.Infof("***** Run StreamEnd Test ...")
	donech = make(chan bool)
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

	_, err := mgr.GetIndexDefnByName("Default", "stream_mgr_stream_end_test")
	if err != nil {
		common.Infof("StreamMgrTest.cleanupStreamMgrStreamEndTest() :  cannot find index defn stream_mgr_stream_end_test.  No cleanup ...")
	} else {
		common.Infof("StreamMgrTest.cleanupStreamMgrStreamEndTest() :  found index defn stream_mgr_stream_end_test.  Cleaning up ...")

		err = mgr.HandleDeleteIndexDDL("Default", "stream_mgr_stream_end_test")
		if err != nil {
			TT.Fatal(err)
		}
		time.Sleep(time.Duration(1000) * time.Millisecond)

		// double check if we have really cleaned up
		_, err := mgr.GetIndexDefnByName("Default", "stream_mgr_stream_end_test")
		if err == nil {
			TT.Fatal("StreamMgrTest.cleanupStreamMgrStreamEndTest(): Cannot clean up index defn stream_mgr_stream_end_test")
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
			TT.Fatal("runSyncTestReceiver(): Timeout waiting to receive timestamp to arrive")
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
		TT.Fatal(err)
	}
	// Wait so there is no race condition.
	time.Sleep(time.Duration(1000) * time.Millisecond)

	// Update the index definition to ready
	common.Infof("Run Sync Test : Update Index Defn 405 to READY")
	topology, err := mgr.GetTopologyByBucket("Default")
	if err != nil {
		TT.Fatal(err)
	}
	topology.ChangeStateForIndexInstByDefn(common.IndexDefnId(405), common.INDEX_STATE_CREATED, common.INDEX_STATE_READY)
	if err := mgr.SetTopologyByBucket("Default", topology); err != nil {
		TT.Fatal(err)
	}
}

func (c *streamEndTestProjectorClient) sendSync(timestamps []*protobuf.TsVbuuid) {

	common.Infof("streamEndTestProjectorClient.sendSync() ")

	if len(timestamps) != 1 {
		TT.Fatal("streamEndTestProjectorClient.sendSync(): More than one timestamp sent to fake projector. Num = %v", len(timestamps))
	}

	seqno, _, _, _, err := timestamps[0].Get(uint16(10))
	if err != nil {
		TT.Fatal(err)
	}
	
	p := newFakeProjector(manager.COORD_MAINT_STREAM_PORT)
	go p.run(donech)

	payloads := make([]*common.VbKeyVersions, 0, 2000)
	
	payload := common.NewVbKeyVersions("Default", 10 /* vb */, 1, 10)
	kv := common.NewKeyVersions(seqno, []byte("document-name"), 1)
	kv.AddStreamBegin()
	kv.AddSync()
	payload.AddKeyVersions(kv)
	payloads = append(payloads, payload)
	
	// send payload
	if err := p.client.SendKeyVersions(payloads, true); err != nil {
		TT.Fatal(err)
	}
}

func (c *streamEndTestProjectorClient) sendStreamEnd(instances []*protobuf.Instance) {

	common.Infof("streamEndTestProjectorClient.sendStreamEnd() ")

	if len(instances) != 1 {
		TT.Fatal("streamEndTestProjectorClient.sendStreamEnd(): More than one index instance sent to fake projector")
	}

	for _, inst := range instances {
		if inst.GetIndexInstance().GetDefinition().GetDefnID() == uint64(405) {

			p := newFakeProjector(manager.COORD_MAINT_STREAM_PORT)
			go p.run(donech)

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
			err := p.client.SendKeyVersions(payloads, true)
			if err != nil {
				TT.Fatal(err)
			}
		}
	}
}

func (c *streamEndTestProjectorClient) MutationTopicRequest(topic, endpointType string,
	reqTimestamps []*protobuf.TsVbuuid, instances []*protobuf.Instance) (*protobuf.TopicResponse, error) {

	if len(reqTimestamps) == 0 {
		TT.Fatal("testProjectorClient.MutationTopicRequest(): reqTimestamps is nil")
	}

	c.sendStreamEnd(instances)

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

func (c *streamEndTestProjectorClient) DelInstances(topic string, uuids []uint64) error {
	return nil
}

func (c *streamEndTestProjectorClient) RepairEndpoints(topic string, endpoints []string) error {
	return nil
}

func (c *streamEndTestProjectorClient) InitialRestartTimestamp(pooln, bucketn string) (*protobuf.TsVbuuid, error) {

	newTs := protobuf.NewTsVbuuid("default", "Default", manager.NUM_VB)
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
// Timer Test
// 1) This test will create a new index.  
// 2) Upon notifying of the new index, the fake projector will push a sync message to the stream manager.  
// 3) The fake projector will send another sync message after TIMESTAMP_PERSIST_INTERVAL
// 4) read the timestamp from the respository and check to see if matches with the timer in memory
////////////////////////////////////////////////////////////////////////////////////////////////////

func runTimerTest() {
	defer func() { test = NO_TEST }()

	common.Infof("**** Run Timer Test **********************************************")
	test = TIMER_TEST

	common.Infof("***** Start TestStreamMgr ") 
	var requestAddr = "localhost:9885"
	var leaderAddr = "localhost:9884"
	var config = "./config.json"

	common.Infof("Start Index Manager")
	factory := new(testProjectorClientFactory)
	env := new(testProjectorClientEnv)
	admin := manager.NewProjectorAdmin(factory, env)
	mgr, err := manager.NewIndexManagerInternal(requestAddr, leaderAddr, config, admin)
	if err != nil {
		TT.Fatal(err)
	}
	time.Sleep(time.Duration(3000) * time.Millisecond)
	
	mgr.SetTimestampPersistenceInterval(1)
	defer mgr.SetTimestampPersistenceInterval(manager.TIMESTAMP_PERSIST_INTERVAL)

	common.Infof("Timer Test Cleanup ...")
	cleanupStreamMgrTimerTest(mgr)

	common.Infof("***** Run timer Test ...")
	donech = make(chan bool)
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
			TT.Fatal(err)
		}
		time.Sleep(time.Duration(1000) * time.Millisecond)

		// double check if we have really cleaned up
		_, err := mgr.GetIndexDefnByName("Default", "stream_mgr_timer_test")
		if err == nil {
			TT.Fatal("StreamMgrTest.cleanupStreamMgrTimerTest(): Cannot clean up index defn stream_mgr_timer_test")
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
					TT.Fatal("runTimerTestReceiver(): timestamp seqno does not match with repo.  %d != %d",
						ts.Seqnos[10], seqno)
			 	} else {
					common.Infof("****** runTimerTestReceiver() receive correct stability timestamp")
			 		return
			 	}
			}
		case <-ticker.C:
			common.Infof("****** runTimerTestReceiver() : timeout")
			TT.Fatal("runTimerTestReceiver(): Timeout waiting to receive timestamp to arrive")
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
		TT.Fatal(err)
	}
	// Wait so there is no race condition.
	time.Sleep(time.Duration(1000) * time.Millisecond)

	// Update the index definition to ready
	common.Infof("Run Timer Test : Update Index Defn 406 to READY")
	topology, err := mgr.GetTopologyByBucket("Default")
	if err != nil {
		TT.Fatal(err)
	}
	topology.ChangeStateForIndexInstByDefn(common.IndexDefnId(406), common.INDEX_STATE_CREATED, common.INDEX_STATE_READY)
	if err := mgr.SetTopologyByBucket("Default", topology); err != nil {
		TT.Fatal(err)
	}
}

func (c *timerTestProjectorClient) sendSync(instances []*protobuf.Instance) {

	common.Infof("timerTestProjectorClient.sendSync() ")

	if len(instances) != 1 {
		TT.Fatal("timerTestProjectorClient.sendSync(): More than one index instance sent to fake projector")
	}

	for _, inst := range instances {
		if inst.GetIndexInstance().GetDefinition().GetDefnID() == uint64(406) {

			p := newFakeProjector(manager.COORD_MAINT_STREAM_PORT)
			go p.run(donech)

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
			if err := p.client.SendKeyVersions(payloads, true); err != nil {
				TT.Fatal(err)
			}

			payloads = make([]*common.VbKeyVersions, 0, 200)

			payload = common.NewVbKeyVersions("Default", 10, 1, 10)
			kv = common.NewKeyVersions(406, []byte("document-name"), 1)
			kv.AddSync()
			payload.AddKeyVersions(kv)
			payloads = append(payloads, payload)

			// send payload
			common.Infof("****** runTimerTestReceiver() sending the second sync message")
			if err := p.client.SendKeyVersions(payloads, true); err != nil {
				TT.Fatal(err)
			}
		}
	}
}

func (c *timerTestProjectorClient) MutationTopicRequest(topic, endpointType string,
	reqTimestamps []*protobuf.TsVbuuid, instances []*protobuf.Instance) (*protobuf.TopicResponse, error) {

	if len(reqTimestamps) == 0 {
		TT.Fatal("timerTestProjectorClient.MutationTopicRequest(): reqTimestamps is nil")
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
// Retry Test
// This test exercise the following error handling functions:
// 1) Projector returns rollbackTimestamp upon MutationTopicRequest
// 2) Projector returns recoverable error
// 3) Projector returns a non-recoverable error
// 4) Stream Admin timeouts on projector retry
// 5) Stream handler gets ConnectionError
////////////////////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////////////////////
// fakeProjector
////////////////////////////////////////////////////////////////////////////////////////////////////

func newFakeProjector(port string) *fakeProjector {

	p := new(fakeProjector)

	addr := net.JoinHostPort("127.0.0.1", port)
	prefix := "projector.dataport.client."
    config := common.SystemConfig.SectionConfig(prefix, true /*trim*/)
    maxvbs := common.SystemConfig["maxVbuckets"].Int()
    flag := transport.TransportFlag(0).SetProtobuf()
    
	var err error
	p.client, err = dataport.NewClient(addr, flag, maxvbs, config)
	if err != nil {
		TT.Fatal(err)
	}

	return p
}

func (p *fakeProjector) run(donech chan bool) {

	<-donech
	p.client.Close()

	common.Infof("fakeProjector: done")
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// testProjectorClientFactory
////////////////////////////////////////////////////////////////////////////////////////////////////

func (p *testProjectorClientFactory) GetClientForNode(server string) manager.ProjectorStreamClient {

	switch test {
	case SYNC_TEST:
		return new(syncTestProjectorClient)
	case DELETE_TEST:
		client := new(deleteTestProjectorClient)
		client.server = server
		return client
	case STREAM_END_TEST:
		return new(streamEndTestProjectorClient)
	case TIMER_TEST:
		return new(timerTestProjectorClient)
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// testProjectorClientFactory
////////////////////////////////////////////////////////////////////////////////////////////////////

func (p *testProjectorClientEnv) GetNodeListForBuckets(buckets []string) (map[string]string, error) {

	common.Infof("testProjectorClientEnv.GetNodeListForBuckets() ")
	switch test {
	case SYNC_TEST:
		nodes := make(map[string]string)
		nodes["127.0.0.1"] = "127.0.0.1"
		return nodes, nil
	case DELETE_TEST:
		nodes := make(map[string]string)
		nodes["127.0.0.1"] = "127.0.0.1"
		nodes["127.0.0.2"] = "127.0.0.2"
		return nodes, nil
	case STREAM_END_TEST:
		nodes := make(map[string]string)
		nodes["127.0.0.1"] = "127.0.0.1"
		return nodes, nil
	case TIMER_TEST:
		nodes := make(map[string]string)
		nodes["127.0.0.1"] = "127.0.0.1"
		return nodes, nil
	}

	return nil, nil
}

func (p *testProjectorClientEnv) GetNodeListForTimestamps(timestamps []*common.TsVbuuid) (map[string][]*protobuf.TsVbuuid, error) {

	common.Infof("testProjectorClientEnv.GetNodeListForTimestamps() ")
	switch test {
	case STREAM_END_TEST:
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

	return nil, nil
}
