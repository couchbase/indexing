package manager

import (
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/dataport"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/data"
)

type mgrMutHandler struct {
	indexMgr *IndexManager
	admin    StreamAdmin
}

func NewMgrMutHandler(indexMgr *IndexManager, admin StreamAdmin) *mgrMutHandler {
	return &mgrMutHandler{indexMgr: indexMgr,
		admin: admin}
}

func (m *mgrMutHandler) HandleSync(streamId common.StreamId,
	bucket string,
	vbucket uint32,
	vbuuid uint64,
	kv *protobuf.KeyVersions,
	offset int) {

	common.Debugf("mgrMutHandler.HandleSync: bucket %s, vbucket %d, vbuuid %d, seqno %d",
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

	common.Debugf("mgrMutHandler.StreamBegin : stream %d bucket %s vb %d", streamId, bucket, vbucket)
}

func (m *mgrMutHandler) HandleStreamEnd(streamId common.StreamId,
	bucket string,
	vbucket uint32,
	vbuuid uint64,
	kv *protobuf.KeyVersions,
	offset int) {

	common.Debugf("mgrMutHandler.StreamEnd : stream %d bucket %s vb %d", streamId, bucket, vbucket)

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
			common.Errorf("mgrMutHandler.HandleStreamEnd(): error encounterd %v", err)
		}
	} else {
		// TODO: Handle race condition - get stream end before the first sync message is received.
		common.Errorf("mgrMutHandler.HandleStreamEnd(): stability timestamp is not available")
	}
}

func (m *mgrMutHandler) HandleUpsert(streamId common.StreamId,
	bucket string,
	vbucket uint32,
	vbuuid uint64,
	kv *protobuf.KeyVersions,
	offset int) {

	// Ignore any mutation
	common.Debugf("mgrMutHandler.HandleUpsert")
}

func (m *mgrMutHandler) HandleDeletion(streamId common.StreamId,
	bucket string,
	vbucket uint32,
	vbuuid uint64,
	kv *protobuf.KeyVersions,
	offset int) {

	// Ignore any mutation
	common.Debugf("mgrMutHandler.HandleDeletion")
}

func (m *mgrMutHandler) HandleUpsertDeletion(streamId common.StreamId,
	bucket string,
	vbucket uint32,
	vbuuid uint64,
	kv *protobuf.KeyVersions,
	offset int) {

	// Ignore any mutation
	common.Debugf("mgrMutHandler.HandleUpsertDeletion")
}

func (m *mgrMutHandler) HandleDropData(streamId common.StreamId,
	bucket string,
	vbucket uint32,
	vbuuid uint64,
	kv *protobuf.KeyVersions,
	offset int) {

	// TODO
	common.Debugf("mgrMutHandler.HandleDropData")
}

func (m *mgrMutHandler) HandleSnapshot(streamId common.StreamId,
	bucket string,
	vbucket uint32,
	vbuuid uint64,
	kv *protobuf.KeyVersions,
	offset int) {

	common.Debugf("mgrMutHandler.Snapshot")

	snapshotType, _, end := kv.Snapshot()

	// Update the timer based on the snapshot marker (ondisk and inmemory snapshot).
	if snapshotType&(0x1|0x2) != 0 {
		m.indexMgr.getTimer().increment(streamId, bucket, vbucket, vbuuid, end)
	}
}

func (m *mgrMutHandler) HandleConnectionError(streamId common.StreamId, err dataport.ConnectionError) {

	common.Debugf("mgrMutHandler.ConnectionError")

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
