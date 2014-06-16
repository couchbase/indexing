// engine library handles document evaluation for secondary key and routing
// them to endpoints.

package projector

import (
	"fmt"
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
func (engine *Engine) AddToEndpoints(m *MutationEvent, kvForEndpoints map[string]*c.KeyVersions) error {
	uuid := engine.uuid
	evaluator := engine.evaluator
	router := engine.router

	vbno, seqno, k := m.Vbucket, m.Seqno, m.Key // Key is Docid
	switch m.Opcode {
	case OpMutation:
		sk, skold, err := doEvaluate(m, uuid, evaluator)
		if err != nil {
			return err
		}
		// Upsert
		raddrs := router.UpsertEndpoints(vbno, seqno, k, sk, skold)
		for _, raddr := range raddrs {
			kvForEndpoints[raddr].AddUpsert(uuid, sk, skold)
		}
		// UpsertDeletion
		raddrs = router.UpsertDeletionEndpoints(vbno, seqno, k, sk, skold)
		for _, raddr := range raddrs {
			kvForEndpoints[raddr].AddUpsertDeletion(uuid, skold)
		}

	case OpDeletion:
		_, skold, err := doEvaluate(m, uuid, evaluator)
		if err != nil {
			return err
		}
		// Deletion
		raddrs := router.DeletionEndpoints(vbno, seqno, k, skold)
		for _, raddr := range raddrs {
			kvForEndpoints[raddr].AddDeletion(uuid, skold)
		}
	}
	return nil
}

func doEvaluate(m *MutationEvent, uuid uint64, evaluator c.Evaluator) (seckeyN, seckeyO []byte, err error) {
	defer func() { // panic safe
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()

	if len(m.OldValue) > 0 { // project old secondary key
		if seckeyO, err = evaluator.Evaluate(m.Key, m.OldValue); err != nil {
			return nil, nil, err
		}
	}
	if len(m.Value) > 0 { // project new secondary key
		if seckeyN, err = evaluator.Evaluate(m.Key, m.Value); err != nil {
			return nil, nil, err
		}
	}
	return seckeyO, seckeyN, nil
}
