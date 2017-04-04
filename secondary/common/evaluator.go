package common

import mc "github.com/couchbase/indexing/secondary/dcp/transport/client"

// Evaluator interface for projector, to be implemented by
// secondary-index or other entities.
type Evaluator interface {
	// Return the bucket name for which this evaluator is applicable.
	Bucket() string

	// StreamBeginData is generated for downstream.
	StreamBeginData(vbno uint16, vbuuid, seqno uint64) (data interface{})

	// Sync is generated for downstream.
	SyncData(vbno uint16, vbuuid, seqno uint64) (data interface{})

	// SnapshotData is generated for downstream.
	SnapshotData(m *mc.DcpEvent, vbno uint16, vbuuid, seqno uint64) interface{}

	// StreamEnd is generated for downstream.
	StreamEndData(vbno uint16, vbuuid, seqno uint64) (data interface{})

	// TransformRoute will transform document consumable by
	// downstream, returns data to be published to endpoints.
	TransformRoute(vbuuid uint64, m *mc.DcpEvent, data map[string]interface{}, encodeBuf []byte) ([]byte, error)
}
