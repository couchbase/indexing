// engine library handles document evaluation for secondary key and routing
// them to endpoints.

package projector

import mc "github.com/couchbase/gomemcached/client"
import c "github.com/couchbase/indexing/secondary/common"

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
	vbno uint16, vbuuid, seqno uint64) interface{} {

	return engine.evaluator.StreamBeginData(vbno, vbuuid, seqno)
}

// SyncData from this engine.
func (engine *Engine) SyncData(
	vbno uint16, vbuuid, seqno uint64) interface{} {

	return engine.evaluator.SyncData(vbno, vbuuid, seqno)
}

// SnapshotData from this engine.
func (engine *Engine) SnapshotData(
	m *mc.UprEvent, vbno uint16, vbuuid, seqno uint64) interface{} {

	return engine.evaluator.SnapshotData(m, vbno, vbuuid, seqno)
}

// StreamEndData from this engine.
func (engine *Engine) StreamEndData(
	vbno uint16, vbuuid, seqno uint64) interface{} {

	return engine.evaluator.StreamEndData(vbno, vbuuid, seqno)
}

// TransformRoute data to endpoints.
func (engine *Engine) TransformRoute(
	vbuuid uint64, m *mc.UprEvent) (map[string]interface{}, error) {

	return engine.evaluator.TransformRoute(vbuuid, m)
}
