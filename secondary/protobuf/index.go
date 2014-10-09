package protobuf

import (
	"fmt"

	mcd "github.com/couchbase/gomemcached"
	mc "github.com/couchbase/gomemcached/client"
	c "github.com/couchbase/indexing/secondary/common"
)

// IndexEvaluator implements `Evaluator` interface for protobuf
// definition of an index instance.
type IndexEvaluator struct {
	skExprs  []interface{} // compiled expression
	pkExpr   interface{}   // compiled expression
	whExpr   interface{}   // compiled expression
	instance *IndexInst
}

// NewIndexEvaluator returns a reference to a new instance
// of IndexEvaluator.
func NewIndexEvaluator(instance *IndexInst) (*IndexEvaluator, error) {
	var err error

	ie := &IndexEvaluator{instance: instance}
	// compile expressions once and reuse it many times.
	defn := ie.instance.GetDefinition()
	switch defn.GetExprType() {
	case ExprType_JavaScript:
	case ExprType_N1QL:
		ie.skExprs, err = c.CompileN1QLExpression(defn.GetSecExpressions())
		if err != nil {
			return nil, err
		}
		expr := defn.GetPartnExpression()
		if len(expr) > 0 {
			cExprs, err := c.CompileN1QLExpression([]string{expr})
			if err != nil {
				return nil, err
			} else if len(cExprs) > 0 {
				ie.pkExpr = cExprs[0]
			}
		}
		expr = defn.GetWhereExpression()
		if len(expr) > 0 {
			cExprs, err := c.CompileN1QLExpression([]string{expr})
			if err != nil {
				return nil, err
			} else if len(cExprs) > 0 {
				ie.whExpr = cExprs[0]
			}
		}
	}
	return ie, nil
}

// Bucket implements Evaluator{} interface.
func (ie *IndexEvaluator) Bucket() string {
	return ie.instance.GetDefinition().GetBucket()
}

// StreamBeginData implement Evaluator{} interface.
func (ie *IndexEvaluator) StreamBeginData(
	vbno uint16, vbuuid, seqno uint64) (data interface{}) {

	bucket := ie.Bucket()
	kv := c.NewKeyVersions(seqno, nil, 1)
	kv.AddStreamBegin()
	return &c.DataportKeyVersions{bucket, vbno, vbuuid, kv}
}

// SyncData implement Evaluator{} interface.
func (ie *IndexEvaluator) SyncData(
	vbno uint16, vbuuid, seqno uint64) (data interface{}) {

	bucket := ie.Bucket()
	kv := c.NewKeyVersions(seqno, nil, 1)
	kv.AddSync()
	return &c.DataportKeyVersions{bucket, vbno, vbuuid, kv}
}

// SnapshotData implement Evaluator{} interface.
func (ie *IndexEvaluator) SnapshotData(
	m *mc.UprEvent, vbno uint16, vbuuid, seqno uint64) (data interface{}) {

	bucket := ie.Bucket()
	kv := c.NewKeyVersions(seqno, nil, 1)
	kv.AddSnapshot(m.SnapshotType, m.SnapstartSeq, m.SnapendSeq)
	return &c.DataportKeyVersions{bucket, vbno, vbuuid, kv}
}

// StreamEndData implement Evaluator{} interface.
func (ie *IndexEvaluator) StreamEndData(
	vbno uint16, vbuuid, seqno uint64) (data interface{}) {

	bucket := ie.Bucket()
	kv := c.NewKeyVersions(seqno, nil, 1)
	kv.AddStreamEnd()
	return &c.DataportKeyVersions{bucket, vbno, vbuuid, kv}
}

// TransformRoute implement Evaluator{} interface.
func (ie *IndexEvaluator) TransformRoute(
	vbuuid uint64,
	m *mc.UprEvent) (endpoints map[string]interface{}, err error) {

	defer func() { // panic safe
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()

	var npkey /*new-partition*/, opkey /*old-partition*/, nkey, okey []byte
	instn := ie.instance

	where, err := ie.wherePredicate(m.Value)
	if err != nil {
		return nil, err
	}

	if where && len(m.Value) > 0 { // project new secondary key
		if npkey, err = ie.partitionKey(m.Value); err != nil {
			return nil, err
		}
		if nkey, err = ie.evaluate(m.Key, m.Value); err != nil {
			return nil, err
		}
	}
	if len(m.OldValue) > 0 { // project old secondary key
		if opkey, err = ie.partitionKey(m.OldValue); err != nil {
			return nil, err
		}
		if okey, err = ie.evaluate(m.Key, m.OldValue); err != nil {
			return nil, err
		}
	}

	vbno, seqno := m.VBucket, m.Seqno
	uuid := instn.GetInstId()

	var kv *c.KeyVersions

	endpoints = make(map[string]interface{})
	bucket := ie.Bucket()

	c.Tracef("inst: %v where: %v (pkey: %v) key: %v\n",
		uuid, where, string(npkey), string(nkey))
	switch m.Opcode {
	case mcd.UPR_MUTATION:
		if where { // WHERE predicate
			// NOTE: Upsert shall be targeted to indexer node hosting the key.
			raddrs := instn.UpsertEndpoints(m, npkey, nkey, okey)
			for _, raddr := range raddrs {
				if _, ok := endpoints[raddr]; !ok {
					kv = c.NewKeyVersions(seqno, m.Key, 4)
				}
				kv.AddUpsert(uuid, nkey, okey)
				endpoints[raddr] =
					&c.DataportKeyVersions{bucket, vbno, vbuuid, kv}
			}
		}
		// NOTE: UpsertDeletion shall be broadcasted if old-key is not
		// available.
		raddrs := instn.UpsertDeletionEndpoints(m, opkey, nkey, okey)
		for _, raddr := range raddrs {
			if _, ok := endpoints[raddr]; !ok {
				kv = c.NewKeyVersions(seqno, m.Key, 4)
			}
			kv.AddUpsertDeletion(uuid, okey)
			endpoints[raddr] = &c.DataportKeyVersions{bucket, vbno, vbuuid, kv}
		}

	case mcd.UPR_DELETION, mcd.UPR_EXPIRATION:
		// Delete shall be broadcasted if old-key is not available.
		raddrs := instn.DeletionEndpoints(m, opkey, okey)
		for _, raddr := range raddrs {
			if _, ok := endpoints[raddr]; !ok {
				kv = c.NewKeyVersions(seqno, m.Key, 4)
			}
			kv.AddDeletion(uuid, okey)
			endpoints[raddr] = &c.DataportKeyVersions{bucket, vbno, vbuuid, kv}
		}
	}
	return endpoints, nil
}

func (ie *IndexEvaluator) evaluate(docid, doc []byte) ([]byte, error) {
	defn := ie.instance.GetDefinition()
	if defn.GetIsPrimary() { // primary index supported !!
		return docid, nil // by saying primary-key is secondary-key
	}

	exprType := defn.GetExprType()
	switch exprType {
	case ExprType_JavaScript:
	case ExprType_N1QL:
		return c.N1QLTransform(docid, doc, ie.skExprs)
	}
	return nil, nil
}

func (ie *IndexEvaluator) partitionKey(doc []byte) ([]byte, error) {
	defn := ie.instance.GetDefinition()
	if defn.GetIsPrimary() { // TODO: strategy for primary index ???
		return nil, nil
	} else if ie.pkExpr == nil { // no partition key
		return nil, nil
	}

	exprType := defn.GetExprType()
	switch exprType {
	case ExprType_JavaScript:
	case ExprType_N1QL:
		return c.N1QLTransform(nil, doc, []interface{}{ie.pkExpr})
	}
	return nil, nil
}

func (ie *IndexEvaluator) wherePredicate(doc []byte) (bool, error) {
	// if where predicate is not supplied - always evaluate to `true`
	if ie.whExpr == nil {
		return true, nil
	}
	defn := ie.instance.GetDefinition()
	exprType := defn.GetExprType()
	switch exprType {
	case ExprType_JavaScript:
	case ExprType_N1QL:
		// TODO: can be optimized by using a custom N1QL-evaluator.
		out, err := c.N1QLTransform(nil, doc, []interface{}{ie.whExpr})
		if out == nil { // missing is treated as false
			return false, err
		} else if err != nil { // errors are treated as false
			return false, err
		} else if string(out) == "true" {
			return true, nil
		}
		return false, nil // predicate is false
	}
	return true, nil
}

// Bucket implements Router{} interface.
func (instance *IndexInst) Bucket() string {
	return instance.GetDefinition().GetBucket()
}

// Endpoints implements Router{} interface.
func (instance *IndexInst) Endpoints() []string {
	if tp := instance.getTestPartitionScheme(); tp != nil {
		return tp.GetEndpoints()
	}
	return []string{}
}

// UpsertEndpoints implements Router{} interface.
func (instance *IndexInst) UpsertEndpoints(
	m *mc.UprEvent, partKey, key, oldKey []byte) []string {

	if tp := instance.getTestPartitionScheme(); tp != nil {
		return tp.GetEndpoints()
	}
	return []string{}
}

// UpsertDeletionEndpoints implements Router{} interface.
func (instance *IndexInst) UpsertDeletionEndpoints(
	m *mc.UprEvent, partKey, key, oldKey []byte) []string {

	if tp := instance.getTestPartitionScheme(); tp != nil {
		return tp.GetEndpoints()
	}
	return []string{}
}

// DeletionEndpoints implements Router{} interface.
func (instance *IndexInst) DeletionEndpoints(
	m *mc.UprEvent, partKey, oldKey []byte) []string {

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
