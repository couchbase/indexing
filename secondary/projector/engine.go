// engine library handles document evaluation for secondary key and routing
// them to endpoints.

package projector

import (
	qexpr "github.com/couchbase/query/expression"
	qvalue "github.com/couchbase/query/value"

	mc "github.com/couchbase/indexing/secondary/dcp/transport/client"

	c "github.com/couchbase/indexing/secondary/common"
)

// IMPORTANT: concurrent access to be expected for Engine object.

// Engine is immutable structure defined for each index,
// or any other entity that wants projection and routing
// over kv-mutations.
type Engine struct {
	uuid      uint64
	evaluator c.Evaluator // do document projection
	router    c.Router    // route projected values to zero or more end-points
}

// NewEngine creates a new engine instance for `uuid`.
func NewEngine(uuid uint64, evaluator c.Evaluator, router c.Router) *Engine {
	engine := &Engine{
		uuid:      uuid,
		evaluator: evaluator,
		router:    router,
	}
	return engine
}

// Endpoints hosting this engine.
func (engine *Engine) Endpoints() []string {
	return engine.router.Endpoints()
}

// StreamBeginData from this engine.
func (engine *Engine) StreamBeginData(
	vbno uint16, vbuuid, seqno uint64, status byte,
	code byte, opaque2 uint64, oso bool) interface{} {

	return engine.evaluator.StreamBeginData(vbno, vbuuid, seqno,
		GetNodeUUID(), status, code, opaque2, oso)
}

// SyncData from this engine.
func (engine *Engine) SyncData(
	vbno uint16, vbuuid, seqno uint64, opaque2 uint64) interface{} {

	return engine.evaluator.SyncData(vbno, vbuuid, seqno, opaque2)
}

// SnapshotData from this engine.
func (engine *Engine) SnapshotData(
	m *mc.DcpEvent, vbno uint16, vbuuid,
	seqno uint64, opaque2 uint64, oso bool) interface{} {

	return engine.evaluator.SnapshotData(m, vbno, vbuuid, seqno,
		opaque2, oso)
}

// SystemEventData from this engine.
func (engine *Engine) SystemEventData(
	m *mc.DcpEvent, vbno uint16, vbuuid,
	seqno uint64, opaque2 uint64) interface{} {

	return engine.evaluator.SystemEventData(m, vbno, vbuuid, seqno, opaque2)
}

// UpdateSeqnoData from this engine.
func (engine *Engine) UpdateSeqnoData(
	m *mc.DcpEvent, vbno uint16, vbuuid,
	seqno uint64, opaque2 uint64) interface{} {

	return engine.evaluator.UpdateSeqnoData(m, vbno, vbuuid, seqno, opaque2)
}

// SeqnoAdvancedData from this engine.
func (engine *Engine) SeqnoAdvancedData(
	m *mc.DcpEvent, vbno uint16, vbuuid,
	seqno uint64, opaque2 uint64) interface{} {

	return engine.evaluator.SeqnoAdvancedData(m, vbno, vbuuid, seqno, opaque2)
}

// OSOSnapshot from this engine.
func (engine *Engine) OSOSnapshotData(
	m *mc.DcpEvent, vbno uint16, vbuuid, opaque2 uint64) interface{} {

	return engine.evaluator.OSOSnapshotData(m, vbno, vbuuid, opaque2)
}

// StreamEndData from this engine.
func (engine *Engine) StreamEndData(
	vbno uint16, vbuuid, seqno uint64, opaque2 uint64, oso bool) interface{} {

	return engine.evaluator.StreamEndData(vbno, vbuuid, seqno, opaque2, oso)
}

// TransformRoute data to endpoints.
func (engine *Engine) TransformRoute(
	vbuuid uint64, m *mc.DcpEvent, data map[string]interface{}, encodeBuf []byte,
	docval qvalue.AnnotatedValue, context qexpr.Context,
	numIndexes int, opaque2 uint64, oso bool) ([]byte, int, error) {

	return engine.evaluator.TransformRoute(
		vbuuid, m, data, encodeBuf, docval, context, numIndexes, opaque2, oso,
	)
}

// GetEvaluatorStats returns the pointer to the stats object for this engine
func (engine *Engine) GetEvaluatorStats() interface{} {
	return engine.evaluator.Stats()
}

// Get name of the index
func (engine *Engine) GetIndexName() string {
	return engine.evaluator.GetIndexName()
}

// Get name of the bucket
func (engine *Engine) Bucket() string {
	return engine.evaluator.Bucket()
}

// Get name of the scope
func (engine *Engine) Scope() string {
	return engine.evaluator.Scope()
}

// Get name of the collection
func (engine *Engine) Collection() string {
	return engine.evaluator.Collection()
}

func (engine *Engine) GetCollectionID() string {
	return engine.evaluator.GetCollectionID()
}
