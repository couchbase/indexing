// engine library handles document evaluation for secondary key and routing
// them to endpoints.

package projector

import (
	"fmt"
	mc "github.com/couchbase/gomemcached/client"
	c "github.com/couchbase/indexing/secondary/common"
)

// IMPORTANT: concurrent access to be expected for Engine object.

// Engine is immutable structure defined for each index, or any other entity
// that wants projection and routing over kv-mutations.
type Engine struct {
	uuid      uint64
	evaluator c.Evaluator // do document projection
	router    c.Router    // route projected values to zero or more end-points
}

// NewEngine creates a new engine instance for `uuid`.
func NewEngine(feed *Feed, uuid uint64, evaluator c.Evaluator, router c.Router) *Engine {
	engine := &Engine{
		uuid:      uuid,
		evaluator: evaluator,
		router:    router,
	}
	return engine
}

// AddToEndpoints create KeyVersions for single `uuid`.
func (engine *Engine) AddToEndpoints(
	m *mc.UprEvent,
	kvForEndpoints map[string]*c.KeyVersions) error {

	uuid := engine.uuid
	evaluator, router := engine.evaluator, engine.router

	vbno, seqno, docid := m.VBucket, m.Seqno, m.Key // Key is Docid
	switch m.Opcode {
	case mc.UprMutation:
		pkey, nkey, okey, err := doEvaluate(m, uuid, evaluator)
		if err != nil {
			return err
		}
		// Upsert
		raddrs := router.UpsertEndpoints(vbno, seqno, docid, pkey, nkey, okey)
		for _, raddr := range raddrs {
			kvForEndpoints[raddr].AddUpsert(uuid, nkey, okey)
		}
		// UpsertDeletion
		raddrs =
			router.UpsertDeletionEndpoints(vbno, seqno, docid, pkey, nkey, okey)
		for _, raddr := range raddrs {
			kvForEndpoints[raddr].AddUpsertDeletion(uuid, okey)
		}

	case mc.UprDeletion:
		pkey, _, okey, err := doEvaluate(m, uuid, evaluator)
		if err != nil {
			return err
		}
		// Deletion
		raddrs := router.DeletionEndpoints(vbno, seqno, docid, pkey, okey)
		for _, raddr := range raddrs {
			kvForEndpoints[raddr].AddDeletion(uuid, okey)
		}
	}
	return nil
}

func doEvaluate(
	m *mc.UprEvent,
	uuid uint64,
	evaluator c.Evaluator) (pkey, nkey, okey []byte, err error) {

	defer func() { // panic safe
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()

	if len(m.Value) > 0 { // project new secondary key
		if pkey, err = evaluator.PartitionKey(m.Key, m.Value); err != nil {
			return
		}
		if nkey, err = evaluator.Transform(m.Key, m.Value); err != nil {
			return
		}
	}
	if len(m.OldValue) > 0 { // project old secondary key
		if okey, err = evaluator.Transform(m.Key, m.OldValue); err != nil {
			return
		}
	}
	return pkey, nkey, okey, nil
}
