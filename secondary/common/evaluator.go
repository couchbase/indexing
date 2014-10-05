package common

import mc "github.com/couchbase/gomemcached/client"
import qexpr "github.com/couchbaselabs/query/expression"
import qparser "github.com/couchbaselabs/query/expression/parser"
import qvalue "github.com/couchbaselabs/query/value"

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
	SnapshotData(m *mc.UprEvent, vbno uint16, vbuuid, seqno uint64) interface{}

	// StreamEnd is generated for downstream.
	StreamEndData(vbno uint16, vbuuid, seqno uint64) (data interface{})

	// TransformRoute will transform document consumable by
	// downstream, returns data to be published to endpoints.
	TransformRoute(vbuuid uint64, m *mc.UprEvent) (endpoints map[string]interface{}, err error)
}

// CompileN1QLExpression will take expressions defined in N1QL's DDL statement
// and compile them for evaluation.
func CompileN1QLExpression(expressions []string) ([]interface{}, error) {
	cExprs := make([]interface{}, 0, len(expressions))
	for _, expr := range expressions {
		cExpr, err := qparser.Parse(expr)
		if err != nil {
			Errorf("CompileN1QLExpression() %v: %v", expr, err)
			return nil, err
		}
		cExprs = append(cExprs, cExpr)
	}
	return cExprs, nil
}

// N1QLTransform will use compile list of expression from N1QL's DDL
// statement and evaluate a document using them to return a secondary
// key as JSON object.
func N1QLTransform(document []byte, cExprs []interface{}) ([]byte, error) {
	arrValue := make([]qvalue.Value, 0, len(cExprs))
	context := qexpr.NewIndexContext()
	for _, cExpr := range cExprs {
		expr := cExpr.(qexpr.Expression)
		// TODO: CBIDXT-133: needs to send nil secondary keys to indexer
		key, err := expr.Evaluate(qvalue.NewValueFromBytes(document), context)
		if err != nil {
			return nil, nil
		}
		arrValue = append(arrValue, key)
	}

	if len(arrValue) > 1 {
		secKey := qvalue.NewValue(make([]interface{}, len(cExprs)))
		for i, key := range arrValue {
			secKey.SetIndex(i, key)
		}
		return secKey.Bytes(), nil // [ seckey1, seckey2, ... ]

	} else if len(arrValue) == 1 {
		return arrValue[0].Bytes(), nil // seckey1

	}
	return nil, ErrorEmptyN1QLExpression
}
