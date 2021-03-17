package common

import (
	mc "github.com/couchbase/indexing/secondary/dcp/transport/client"
	qexpr "github.com/couchbase/query/expression"
	qvalue "github.com/couchbase/query/value"
)

// Evaluator interface for projector, to be implemented by
// secondary-index or other entities.
type Evaluator interface {
	// Return the bucket name for which this evaluator is applicable.
	Bucket() string

	// Return the scope name for which this evaluator is applicable.
	Scope() string

	// Return the collection name for which this evaluator is applicable.
	Collection() string

	// StreamBeginData is generated for downstream.
	StreamBeginData(vbno uint16, vbuuid, seqno uint64, nodeUUID string,
		status, code byte, opaque2 uint64, oso bool) (data interface{})

	// Sync is generated for downstream.
	SyncData(vbno uint16, vbuuid, seqno, opaque2 uint64) (data interface{})

	// SnapshotData is generated for downstream.
	SnapshotData(m *mc.DcpEvent, vbno uint16, vbuuid, seqno, opaque2 uint64, oso bool) interface{}

	// SystemEventData is generated for downstream.
	SystemEventData(m *mc.DcpEvent, vbno uint16, vbuuid, seqno, opaque2 uint64) interface{}

	// UpdateSeqnoData is generated for downstream.
	UpdateSeqnoData(m *mc.DcpEvent, vbno uint16, vbuuid, seqno, opaque2 uint64) interface{}

	// SeqnoAdvancedData is generated for downstream.
	SeqnoAdvancedData(m *mc.DcpEvent, vbno uint16, vbuuid, seqno, opaque2 uint64) interface{}

	// OSOSnapshotData is generated for downstream.
	OSOSnapshotData(m *mc.DcpEvent, vbno uint16, vbuuid, opaque2 uint64) interface{}

	// StreamEnd is generated for downstream.
	StreamEndData(vbno uint16, vbuuid, seqno, opaque2 uint64, oso bool) (data interface{})

	// TransformRoute will transform document consumable by
	// downstream, returns data to be published to endpoints.
	TransformRoute(
		vbuuid uint64, m *mc.DcpEvent, data map[string]interface{}, encodeBuf []byte,
		docval qvalue.AnnotatedValue, context qexpr.Context, numIndexes int,
		opaque2 uint64, oso bool) ([]byte, int, error)

	Stats() interface{}

	// Get the name of the index
	GetIndexName() string

	// Get ID of the collection to which this engine belongs
	GetCollectionID() string
}
