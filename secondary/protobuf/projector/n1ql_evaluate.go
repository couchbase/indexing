package protobuf

import c "github.com/couchbase/indexing/secondary/common"
import "github.com/couchbase/indexing/secondary/collatejson"
import qexpr "github.com/couchbase/query/expression"
import qparser "github.com/couchbase/query/expression/parser"
import qvalue "github.com/couchbase/query/value"

// CompileN1QLExpression will take expressions defined in N1QL's DDL statement
// and compile them for evaluation.
func CompileN1QLExpression(expressions []string) ([]interface{}, error) {
	cExprs := make([]interface{}, 0, len(expressions))
	for _, expr := range expressions {
		cExpr, err := qparser.Parse(expr)
		if err != nil {
			c.Errorf("CompileN1QLExpression() %v: %v\n", expr, err)
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
	docval := qvalue.NewValue(doc)
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
		return arrValue[0].MarshalJSON()

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
		return secKey.MarshalJSON() // return as JSON array
	}
	return nil, nil
}
