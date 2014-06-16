package common

import (
	"github.com/couchbaselabs/dparval"
	"github.com/couchbaselabs/tuqtng/ast"
)

// Evaluator interface for projector, to be implemented by secondary-index or
// other entities.
type Evaluator interface {
	// Return the bucket name for which this evaluator is applicable
	Bucket() string

	// Compile expressions defined for an index. Will be called when a feed
	// (aka mutation-stream) is being setup.
	Compile() error

	// Transform for document using DDL expressions for seconday-key. Will
	// be called for every KV-mutation.
	Transform(docid []byte, document []byte) (secKey []byte, err error)

	// PartitionKey for document using DDL expressions for seconday-key. Will
	// be called for every KV-mutation.
	PartitionKey(docid []byte, document []byte) (secKey []byte, err error)
}

// CompileN1QLExpression will take expressions defined in N1QL's DDL statement
// and compile them for evaluation.
func CompileN1QLExpression(expressions []string) ([]interface{}, error) {
	cExprs := make([]interface{}, 0, len(expressions))
	for _, expr := range expressions {
		cExpr, err := ast.UnmarshalExpression([]byte(expr))
		if err != nil {
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
	arrValue := make([]*dparval.Value, 0, len(cExprs))
	for _, cExpr := range cExprs {
		expr := cExpr.(ast.Expression)
		key, err := expr.Evaluate(dparval.NewValueFromBytes(document))
		if err != nil {
			return nil, err
		}
		arrValue = append(arrValue, key)
	}
	if len(arrValue) > 1 {
		secKey := dparval.NewValue(make([]interface{}, len(cExprs)))
		for i, key := range arrValue {
			secKey.SetIndex(i, key)
		}
		return secKey.Bytes(), nil
	} else if len(arrValue) == 1 {
		return arrValue[0].Bytes(), nil
	} else {
		return nil, ErrorEmptyN1QLExpression
	}
}

// CompositeKeysToArray convert list of composite keys to JSON array of
// values.
func CompositeKeysToArray(keys []*dparval.Value) []byte {
	secKey := dparval.NewValue(make([]interface{}, len(keys)))
	for i, key := range keys {
		secKey.SetIndex(i, key)
	}
	return secKey.Bytes()
}
