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

const (
	NO_TEST     uint8 = 0
	SYNC_TEST         = 1
	DELETE_TEST       = 2
)

var TT *testing.T
var donech chan bool
var test uint8
var delete_test_status map[uint64]*protobuf.Instance

////////////////////////////////////////////////////////////////////////////////////////////////////
// Test Driver
////////////////////////////////////////////////////////////////////////////////////////////////////

func TestStreamMgr(t *testing.T) {

	common.LogEnable()
	common.SetLogLevel(common.LogLevelDebug)
	test = NO_TEST
	TT = t

	common.Infof("Start TestStreamMgr *********************************************************")

	var requestAddr = "localhost:9885"
	var leaderAddr = "localhost:9884"
	var config = "./config.json"

	common.Infof("Start Index Manager")
	factory := new(testProjectorClientFactory)
	env := new(testProjectorClientEnv)
	admin := manager.NewProjectorAdmin(factory, env)
	mgr, err := manager.NewIndexManagerInternal(requestAddr, leaderAddr, config, admin)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Duration(3000) * time.Millisecond)

	// Running test
	runSyncTest(mgr)
	runDeleteTest(mgr)

	////////////////////////////////////////////////////
	common.Infof("Stop TestStreamMgr. Tearing down *********************************************************")
	mgr.Close()
	time.Sleep(time.Duration(1000) * time.Millisecond)
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

func runSyncTest(mgr *manager.IndexManager) {
	defer func() { test = NO_TEST }()

	common.Infof("**** Run Sync Test *****")
	common.Infof("Sync Test Cleanup ...")
	test = SYNC_TEST

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
	time.Sleep(time.Duration(1000) * time.Millisecond)

	common.Infof("**** Finish Sync Test *****")
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
			common.Infof("****** runSyncTestReceiver() receive correct stability timestamp")
			if ts.Seqnos[10] >= 100 {
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

	newTs := protobuf.NewTsVbuuid("default", "Default", 1024)
	for i := 0; i < 1024; i++ {
		newTs.Append(uint16(i), uint64(i), uint64(1234), uint64(0), uint64(0))
	}
	return newTs, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Delete Test
// This test deletes creates 3 index on 2 different buckets across 2 different (fake) projectors.j
// One index will then be deleted.  A sync message is sent after the index is deleted.  The
// stabiltiy timestamp should have the seqno for both buckets and from the two remaining indexes.
// This test covers the following functions:
// 1) Coordinator Master deletes index and updates topology
// 2) Stream Manager receives the topoogy update and determine the index to delete
// 3) StreamAdmin process delete instances
// 4) Stream Manager handle requests to more than one projectors
////////////////////////////////////////////////////////////////////////////////////////////////////

func runDeleteTest(mgr *manager.IndexManager) {
	defer func() { test = NO_TEST }()

	common.Infof("**** Run Delete Test *****")
	test = DELETE_TEST

	common.Infof("Delete Test Cleanup ...")
	cleanupStreamMgrDeleteTest(mgr)

	common.Infof("***** Run Delete Test :  setup")
	delete_test_status = make(map[uint64]*protobuf.Instance)
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
	time.Sleep(time.Duration(1000) * time.Millisecond)

	common.Infof("**** Finish Delete Test *****")
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
	// Add a new index definition : 403 (Default1)
	//
	idxDefn = &common.IndexDefn{
		DefnId:          common.IndexDefnId(403),
		Name:            "stream_mgr_delete_test_3",
		Using:           common.ForestDB,
		Bucket:          "Default1",
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
	topology, err = mgr.GetTopologyByBucket("Default1")
	if err != nil {
		TT.Fatal(err)
	}
	topology.ChangeStateForIndexInstByDefn(common.IndexDefnId(403), common.INDEX_STATE_CREATED, common.INDEX_STATE_READY)
	if err := mgr.SetTopologyByBucket("Default1", topology); err != nil {
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

	_, err = mgr.GetIndexDefnByName("Default1", "stream_mgr_delete_test_3")
	if err != nil {
		common.Infof("StreamMgrTest.cleanupStreamMgrSyncTest() :  cannot find index defn stream_mgr_delete_test_3.  No cleanup ...")
	} else {
		common.Infof("StreamMgrTest.cleanupStreamMgrSyncTest() :  found index defn stream_mgr_delete_test_3.  Cleaning up ...")

		err = mgr.HandleDeleteIndexDDL("Default1", "stream_mgr_delete_test_3")
		if err != nil {
			TT.Fatal(err)
		}
		time.Sleep(time.Duration(1000) * time.Millisecond)

		// double check if we have really cleaned up
		_, err = mgr.GetIndexDefnByName("Default1", "stream_mgr_delete_test_3")
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
			common.Infof("****** runDeleteTestReceiver() : receive timestamp. bucket %s seqno[10] %v seqno[20] %v",
				ts.Bucket, ts.Seqnos[10], ts.Seqnos[20])
			common.Infof("****** runDeleteTestReceiver() : receive timestamp. bucket %s seqno[30] %v seqno[40] %v",
				ts.Bucket, ts.Seqnos[30], ts.Seqnos[40])

			if ts.Bucket == "Default" && ts.Seqnos[10] >= 100 && ts.Seqnos[20] >= 200 {
				common.Infof("****** runDeleteTestReceiver() receive correct stability timestamp for bucket Default")
				bucket1 = true
			}

			if ts.Bucket == "Default1" && ts.Seqnos[30] >= 300 && ts.Seqnos[40] >= 400 {
				common.Infof("****** runDeleteTestReceiver() receive correct stability timestamp for bucket Default1")
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
	payloads := make([]*common.VbKeyVersions, 0, 200)

	// bucket <Default>, node <127.0.0.1>  -> vb 10, seqno 101
	// bucket <Default>, node <127.0.0.2>  -> vb 20, seqno 201
	// bucket <Default1>, node <127.0.0.1> -> vb 30, seqno 301
	// bucket <Default1>, node <127.0.0.2> -> vb 40, seqno 401
	for _, inst := range delete_test_status {
		seqno := 0
		vb := 0
		bucket := inst.GetIndexInstance().GetDefinition().GetBucket()
		if bucket == "Default" {
			if c.server == "127.0.0.1" {
				seqno = 101
				vb = 10
			} else if c.server == "127.0.0.2" {
				seqno = 201
				vb = 20
			}
		} else if bucket == "Default1" {
			if c.server == "127.0.0.1" {
				seqno = 301
				vb = 30
			} else if c.server == "127.0.0.2" {
				seqno = 401
				vb = 40
			}
		}

		common.Infof("deleteTestProjectorClient.sendSync() for node %v and bucket %v", c.server, bucket)

		// create StreamBegin
		payload := common.NewVbKeyVersions(bucket, uint16(vb), 1, 10)
		kv := common.NewKeyVersions(uint64(seqno), []byte("document-name"), 1)
		kv.AddStreamBegin()
		payload.AddKeyVersions(kv)
		payloads = append(payloads, payload)

		// Create Sync Message
		payload = common.NewVbKeyVersions(bucket, uint16(vb), 1, 10)
		kv = common.NewKeyVersions(uint64(seqno), []byte("document-name"), 1)
		kv.AddSync()
		payload.AddKeyVersions(kv)
		payloads = append(payloads, payload)
	}

	// Send payload
	if len(payloads) != 0 {
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

	newTs := protobuf.NewTsVbuuid("default", "Default", 1024)
	for i := 0; i < 1024; i++ {
		newTs.Append(uint16(i), uint64(i), uint64(1234), uint64(0), uint64(0))
	}
	return newTs, nil
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
	var err error
	p.client, err = dataport.NewClient(addr, transport.TransportFlag(0).SetProtobuf(), common.SystemConfig)
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
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// testProjectorClientFactory
////////////////////////////////////////////////////////////////////////////////////////////////////

func (p *testProjectorClientEnv) GetNodeList(buckets []string) (map[string]string, error) {

	common.Infof("testProjectorClientEnv.GetNodeList() ")
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
	}

	return nil, nil
}
