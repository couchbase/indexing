package manager

import (
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/dataport"
	"github.com/couchbase/indexing/secondary/protobuf"
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

	common.Debugf("mgrMutHandler.StreamBegin")

}

func (m *mgrMutHandler) HandleStreamEnd(streamId common.StreamId,
	bucket string,
	vbucket uint32,
	vbuuid uint64,
	kv *protobuf.KeyVersions,
	offset int) {

	common.Debugf("mgrMutHandler.StreamEnd")
	
	// A stream has terminated.  Need to restart. 
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

	// Update the timer based on the snapshot marker.
	if snapshotType == 0 || snapshotType == 1 || snapshotType == 2 {
		m.indexMgr.getTimer().increment(streamId, bucket, vbucket, vbuuid, end)
	}
}

func (m *mgrMutHandler) HandleConnectionError(streamId common.StreamId, err dataport.ConnectionError) {

	common.Debugf("mgrMutHandler.ConnectionError")

	// ConnectionError happens in 3 cases:
	// 	1) Projector goes down for a specific node
	// 	2) Network error that forces the connection to terminate for the coordinator
	// 	3) Projector buffer is full and forces projector to terminate the connection
	//
	// For (2) and (3),  the coordinator can call projector to repairEndpoints, without
	// interrupting the shared stream. If we get an error from projector on repair endpoints,
	// it will need to restart the stream.
	//
	stream := m.indexMgr.streamMgr.getStream(streamId)
	if stream != nil {
		endpoint := stream.getEndpoint()
		// TODO : handle error
		if err := m.admin.RepairStreamForEndpoint(streamId, (map[string][]uint16)(err), endpoint); err != nil {
			// TODO: differentiate the error for "stream not exist"
			if err := m.indexMgr.streamMgr.OpenStreamForAllBuckets(streamId); err != nil {
				// TODO : return error
			}
		}
	}
}
