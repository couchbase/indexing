package common

import "github.com/couchbase/indexing/secondary/collatejson"
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

var missing = qvalue.NewValue(string(collatejson.MissingLiteral))

// N1QLTransform will use compile list of expression from N1QL's DDL
// statement and evaluate a document using them to return a secondary
// key as JSON object.
func N1QLTransform(docid, doc []byte, cExprs []interface{}) ([]byte, error) {
	arrValue := make([]qvalue.Value, 0, len(cExprs))
	context := qexpr.NewIndexContext()
	skip := true
	docval := qvalue.NewValueFromBytes(doc)
	for _, cExpr := range cExprs {
		expr := cExpr.(qexpr.Expression)
		key, err := expr.Evaluate(docval, context)
		if err != nil {
			return nil, err

		} else if key.Type() == qvalue.MISSING && skip {
			return nil, nil

		} else if key.Type() == qvalue.MISSING {
			arrValue = append(arrValue, missing)
			continue
		}
		skip = false
		arrValue = append(arrValue, key)
	}

	if len(cExprs) == 1 && len(arrValue) == 1 && docid == nil {
		// used for partition-key evaluation and where predicate.
		return arrValue[0].Bytes(), nil

	} else if len(arrValue) > 0 {
		// A strictly internal hack to make secondary keys unique by
		// appending the docid. The shape of the secondary key looks like,
		//     [expr1, docid] - for simple key
		//     [expr1, expr2, ..., docid] - for composite key
		//
		// above hack is applicable only when docid is not `nil`
		if docid != nil {
			arrValue = append(arrValue, qvalue.NewValue(string(docid)))
		}
		secKey := qvalue.NewValue(make([]interface{}, len(arrValue)))
		for i, key := range arrValue {
			secKey.SetIndex(i, key)
		}
		return secKey.Bytes(), nil // return as JSON array
	}
	return nil, nil
}
