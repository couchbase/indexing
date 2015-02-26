// Copyright (c) 2014 Couchbase, Inc.

// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.
package manager

import (
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/dataport"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/data"
)

type mgrMutHandler struct {
	indexMgr *IndexManager
	admin    StreamAdmin
	monitor  *StreamMonitor
}

func NewMgrMutHandler(indexMgr *IndexManager, admin StreamAdmin, monitor *StreamMonitor) *mgrMutHandler {
	return &mgrMutHandler{indexMgr: indexMgr,
		admin:   admin,
		monitor: monitor}
}

func (m *mgrMutHandler) HandleSync(streamId common.StreamId,
	bucket string,
	vbucket uint32,
	vbuuid uint64,
	kv *protobuf.KeyVersions,
	offset int) {

	logging.Debugf("mgrMutHandler.HandleSync: bucket %s, vbucket %d, vbuuid %d, seqno %d",
		bucket, vbucket, vbuuid, kv.GetSeqno())

	// update the timer
	m.indexMgr.getTimer().increment(streamId, bucket, vbucket, vbuuid, kv.GetSeqno())
}

func (m *mgrMutHandler) HandleStreamBegin(streamId common.StreamId,
	bucket string,
	vbucket uint32,
	vbuuid uint64,
	kv *protobuf.KeyVersions,
	offset int) {

	logging.Debugf("mgrMutHandler.StreamBegin : stream %d bucket %s vb %d", streamId, bucket, vbucket)
	if m.monitor != nil {
		m.monitor.Activate(streamId, bucket, uint16(vbucket))
	}
}

func (m *mgrMutHandler) HandleStreamEnd(streamId common.StreamId,
	bucket string,
	vbucket uint32,
	vbuuid uint64,
	kv *protobuf.KeyVersions,
	offset int) {

	logging.Debugf("mgrMutHandler.StreamEnd : stream %d bucket %s vb %d", streamId, bucket, vbucket)
	if m.monitor != nil {
		m.monitor.Deactivate(streamId, bucket, uint16(vbucket))
	}

	lastTs := m.indexMgr.getTimer().getLatest(streamId, bucket)
	if lastTs != nil {
		ts := common.NewTsVbuuid(bucket, NUM_VB)
		ts.Seqnos[vbucket] = lastTs.Seqnos[vbucket]
		ts.Vbuuids[vbucket] = lastTs.Vbuuids[vbucket]
		ts.Snapshots[vbucket][0] = lastTs.Snapshots[vbucket][0]
		ts.Snapshots[vbucket][1] = lastTs.Snapshots[vbucket][1]

		err := m.indexMgr.streamMgr.RestartStreamIfNecessary(streamId, []*common.TsVbuuid{ts})
		if err != nil {
			// TODO: What if the bucket is deleted?
			logging.Errorf("mgrMutHandler.HandleStreamEnd(): error encounterd %v", err)
		}
	} else {
		// TODO: Handle race condition - get stream end before the first sync message is received.
		logging.Errorf("mgrMutHandler.HandleStreamEnd(): stability timestamp is not available")
	}
}

func (m *mgrMutHandler) HandleUpsert(streamId common.StreamId,
	bucket string,
	vbucket uint32,
	vbuuid uint64,
	kv *protobuf.KeyVersions,
	offset int) {

	// Ignore any mutation
	logging.Debugf("mgrMutHandler.HandleUpsert")
}

func (m *mgrMutHandler) HandleDeletion(streamId common.StreamId,
	bucket string,
	vbucket uint32,
	vbuuid uint64,
	kv *protobuf.KeyVersions,
	offset int) {

	// Ignore any mutation
	logging.Debugf("mgrMutHandler.HandleDeletion")
}

func (m *mgrMutHandler) HandleUpsertDeletion(streamId common.StreamId,
	bucket string,
	vbucket uint32,
	vbuuid uint64,
	kv *protobuf.KeyVersions,
	offset int) {

	// Ignore any mutation
	logging.Debugf("mgrMutHandler.HandleUpsertDeletion")
}

func (m *mgrMutHandler) HandleDropData(streamId common.StreamId,
	bucket string,
	vbucket uint32,
	vbuuid uint64,
	kv *protobuf.KeyVersions,
	offset int) {

	// TODO
	logging.Debugf("mgrMutHandler.HandleDropData")
}

func (m *mgrMutHandler) HandleSnapshot(streamId common.StreamId,
	bucket string,
	vbucket uint32,
	vbuuid uint64,
	kv *protobuf.KeyVersions,
	offset int) {

	logging.Debugf("mgrMutHandler.Snapshot")

	snapshotType, _, end := kv.Snapshot()

	// Update the timer based on the snapshot marker (ondisk and inmemory snapshot).
	if snapshotType&(0x1|0x2) != 0 {
		m.indexMgr.getTimer().increment(streamId, bucket, vbucket, vbuuid, end)
	}
}

func (m *mgrMutHandler) HandleConnectionError(streamId common.StreamId, err dataport.ConnectionError) {

	logging.Debugf("mgrMutHandler.ConnectionError")

	// ConnectionError happens in 3 cases:
	//  1) Projector goes down for a specific node
	//  2) Network error that forces the connection to terminate for the coordinator
	//  3) Projector buffer is full and forces projector to terminate the connection
	//
	// For (2) and (3),  the coordinator can call projector to repairEndpoints, without
	// interrupting the shared stream. If we get an error from projector on repair endpoints,
	// it will need to restart the stream.
	// TODO: Need to see if repair endpiont needs to return TopicNotExist
	//
	stream := m.indexMgr.streamMgr.getStream(streamId)
	if stream != nil {
		endpoint := stream.getEndpoint()
		// TODO : handle error
		if err := m.admin.RepairEndpointForStream(streamId, (map[string][]uint16)(err), endpoint); err != nil {
			// TODO: differentiate the error for "stream not exist"
			if err := m.indexMgr.streamMgr.AddIndexForAllBuckets(streamId); err != nil {
				// TODO : return error
			}
		}
	}
}
