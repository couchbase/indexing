// support functions for index message

// TODO: Evaluator and Router interface are not really dependant on protobuf
// definition of an index. At later point we might want to abstract index
// definitions, partition definitions into native structures, at which point
// we shall move Evaluator and Router implementation to native structures as
// well.

package protobuf

import (
	c "github.com/couchbase/indexing/secondary/common"
)

// IndexEvaluator implements `Evaluator` interface for protobuf definition of
// an index.
type IndexEvaluator struct {
	c_exprs []interface{} // compiled expression
	index   *Index
}

// NewIndexEvaluator returns a reference to a new instance of IndexEvaluator.
func NewIndexEvaluator(index *Index) *IndexEvaluator {
	return &IndexEvaluator{index: index}
}

// Evaluator interface methods

func (ie *IndexEvaluator) Bucket() string {
	return ie.index.GetIndexinfo().GetBucket()
}

func (ie *IndexEvaluator) Compile() (err error) {
	info := ie.index.GetIndexinfo()
	switch info.GetExprType() {
	case ExprType_Simple:
	case ExprType_JavaScript:
	case ExprType_N1QL:
		ie.c_exprs, err = c.CompileN1QLExpression(info.GetExpressions())
	}
	return
}

func (ie *IndexEvaluator) Evaluate(docid []byte, document []byte) ([]byte, error) {
	info := ie.index.GetIndexinfo()
	if info.GetIsPrimary() {
		return docid, nil
	}

	var secKey []byte
	var err error

	exprType := ie.index.GetIndexinfo().GetExprType()
	switch exprType {
	case ExprType_Simple:
	case ExprType_JavaScript:
	case ExprType_N1QL:
		secKey, err = c.EvaluateWithN1QL(document, ie.c_exprs)
	}
	return secKey, err
}

// Router interface methods

func (index *Index) Bucket() string {
	return index.GetIndexinfo().GetBucket()
}

func (index *Index) UuidEndpoints() []string {
	return []string{}
}

func (index *Index) CoordinatorEndpoint() string {
	return ""
}

func (index *Index) UpsertEndpoints(vbno uint16, seqno uint64, docid, key, oldkey []byte) []string {
	return []string{}
}

func (index *Index) UpsertDeletionEndpoints(vbno uint16, seqno uint64, docid, key, oldkey []byte) []string {
	return []string{}
}

func (index *Index) DeletionEndpoints(vbno uint16, seqno uint64, docid, oldkey []byte) []string {
	return []string{}
}
