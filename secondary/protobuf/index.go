// support functions for index message

// TODO: Evaluator and Router interface are not really dependant on protobuf
// definition of an index instance. At later point we might want to abstract
// index definitions, partition definitions into native structures, at which
// point we shall move Evaluator and Router implementation to native structures
// as well.

package protobuf

import (
	c "github.com/couchbase/indexing/secondary/common"
)

// IndexEvaluator implements `Evaluator` interface for protobuf definition of
// an index instance.
type IndexEvaluator struct {
	skExprs  []interface{} // compiled expression
	pkExpr   interface{}   // compiled expression
	instance *IndexInst
}

// NewIndexEvaluator returns a reference to a new instance of IndexEvaluator.
func NewIndexEvaluator(instance *IndexInst) *IndexEvaluator {
	return &IndexEvaluator{instance: instance}
}

// Evaluator interface methods

func (ie *IndexEvaluator) Bucket() string {
	return ie.instance.GetDefinition().GetBucket()
}

func (ie *IndexEvaluator) Compile() (err error) {
	defn := ie.instance.GetDefinition()
	switch defn.GetExprType() {
	case ExprType_JavaScript:
	case ExprType_N1QL:
		ie.skExprs, err = c.CompileN1QLExpression(defn.GetSecExpressions())
		if err != nil {
			return err
		}
		expr := defn.GetPartnExpression()
		cExprs, err := c.CompileN1QLExpression([]string{expr})
		if err != nil {
			return err
		}
		ie.pkExpr = cExprs[0]
	}
	return
}

func (ie *IndexEvaluator) PartitionKey(docid []byte, document []byte) (partKey []byte, err error) {
	defn := ie.instance.GetDefinition()
	if defn.GetIsPrimary() {
		return nil, nil
	}

	exprType := ie.instance.GetDefinition().GetExprType() // ???
	switch exprType {
	case ExprType_JavaScript:
	case ExprType_N1QL:
		partKey, err = c.N1QLTransform(document, []interface{}{ie.pkExpr})
	}
	return partKey, err
}

func (ie *IndexEvaluator) Transform(docid []byte, document []byte) (secKey []byte, err error) {
	defn := ie.instance.GetDefinition()
	if defn.GetIsPrimary() {
		return docid, nil
	}

	exprType := ie.instance.GetDefinition().GetExprType()
	switch exprType {
	case ExprType_JavaScript:
	case ExprType_N1QL:
		secKey, err = c.N1QLTransform(document, ie.skExprs)
	}
	return secKey, err
}

// Router interface methods

func (instance *IndexInst) Bucket() string {
	return instance.GetDefinition().GetBucket()
}

func (instance *IndexInst) UuidEndpoints() []string {
	if tp := instance.getTestPartitionScheme(); tp != nil {
		return tp.GetEndpoints()
	}
	return []string{}
}

func (instance *IndexInst) CoordinatorEndpoint() string {
	if tp := instance.getTestPartitionScheme(); tp != nil {
		return tp.GetCoordEndpoint()
	}
	return ""
}

func (instance *IndexInst) UpsertEndpoints(vbno uint16, seqno uint64, docid, partKey, key, oldKey []byte) []string {
	if tp := instance.getTestPartitionScheme(); tp != nil {
		return tp.GetEndpoints()
	}
	return []string{}
}

func (instance *IndexInst) UpsertDeletionEndpoints(vbno uint16, seqno uint64, docid, partKey, key, oldKey []byte) []string {
	if tp := instance.getTestPartitionScheme(); tp != nil {
		return tp.GetEndpoints()
	}
	return []string{}
}

func (instance *IndexInst) DeletionEndpoints(vbno uint16, seqno uint64, docid, partKey, oldKey []byte) []string {
	if tp := instance.getTestPartitionScheme(); tp != nil {
		return tp.GetEndpoints()
	}
	return []string{}
}

// -----

func (instance *IndexInst) getTestPartitionScheme() *TestPartition {
	defn := instance.GetDefinition()
	if defn.GetPartitionScheme() == PartitionScheme_TEST {
		return instance.GetTp()
	}
	return nil
}
