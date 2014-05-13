// engine library handles document evaluation for secondary key and routing
// them to endpoints.

package projector

import (
	"fmt"
	c "github.com/couchbase/indexing/secondary/common"
	"log"
)

// Engine is immutable structure defined for each index, or any other entity
// that wants projection and routing over kv-mutations.
type Engine struct {
	uuid      uint64
	evaluator c.Evaluator // do document projection
	router    c.Router    // route projected values to zero or more end-points
	logPrefix string
}

// NewEngine creates a new engine instance for `uuid`.
func NewEngine(feed *Feed, uuid uint64, evaluator c.Evaluator, router c.Router) *Engine {
	engine := &Engine{
		uuid:      uuid,
		evaluator: evaluator,
		router:    router,
		logPrefix: fmt.Sprintf("engine %v:%v", feed.topic, uuid),
	}
	return engine
}

// AddToEndpoints create KeyVersions for single `uuid`.
func (engine *Engine) AddToEndpoints(m *MutationEvent, kvForEndpoints map[string]*c.KeyVersions) {
	uuid := engine.uuid
	evaluator := engine.evaluator
	router := engine.router

	vbno, seqno, k := m.Vbucket, m.Seqno, m.Key
	switch m.Opcode {
	case OpMutation:
		if sk, skold, err := doEvaluate(m, uuid, evaluator); err == nil {
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
		}

	case OpDeletion:
		if _, skold, err := doEvaluate(m, uuid, evaluator); err == nil {
			// Deletion
			raddrs := router.DeletionEndpoints(vbno, seqno, k, skold)
			for _, raddr := range raddrs {
				kvForEndpoints[raddr].AddDeletion(uuid, skold)
			}
		}
	}
}

func doEvaluate(m *MutationEvent, uuid uint64, evaluator c.Evaluator) (seckeyN []byte, seckeyO []byte, err error) {
	defer func() { // panic safe
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
		if err != nil {
			s := string(m.Key) // docid
			log.Printf("error %v, evaluating %v with %v\n", err, s, uuid)
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
