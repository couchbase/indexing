package projector

import c "github.com/couchbase/indexing/secondary/common"

// Subscriber interface abstracts engines (aka instances)
// that can supply `evaluators`, to transform mutations into
// custom-messages, and `routers`, to supply distribution topology
// for custom-messages.
type Subscriber interface {
	// GetEvaluators will return a map of uuid to Evaluator interface.
	// - return ErrorInconsistentFeed for malformed tables.
	GetEvaluators() (map[uint64]c.Evaluator, error)

	// GetRouters will return a map of uuid to Router interface.
	// - return ErrorInconsistentFeed for malformed tables.
	GetRouters() (map[uint64]c.Router, error)
}
