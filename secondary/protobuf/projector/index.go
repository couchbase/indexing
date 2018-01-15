package protobuf

import "fmt"

import "github.com/couchbase/indexing/secondary/logging"
import c "github.com/couchbase/indexing/secondary/common"
import mcd "github.com/couchbase/indexing/secondary/dcp/transport"
import mc "github.com/couchbase/indexing/secondary/dcp/transport/client"

type Partition interface {
	// Hosts return full list of endpoints <host:port>
	// that are listening for this instance.
	Hosts(*IndexInst) []string

	// UpsertEndpoints return a list of endpoints <host:port>
	// to which Upsert message will be published.
	UpsertEndpoints(i *IndexInst, m *mc.DcpEvent, partKey, key, oldKey []byte) []string

	// UpsertDeletionEndpoints return a list of endpoints
	// <host:port> to which UpsertDeletion message will be
	// published.
	UpsertDeletionEndpoints(i *IndexInst, m *mc.DcpEvent, partKey, key, oldKey []byte) []string

	// DeletionEndpoints return a list of endpoints
	// <host:port> to which Deletion message will be published.
	DeletionEndpoints(i *IndexInst, m *mc.DcpEvent, partKey, oldKey []byte) []string
}

// Bucket implements Router{} interface.
func (instance *IndexInst) Bucket() string {
	return instance.GetDefinition().GetBucket()
}

// Endpoints implements Router{} interface.
func (instance *IndexInst) Endpoints() []string {
	p := instance.GetPartitionObject()
	if p == nil {
		return nil
	}
	return p.Hosts(instance)
}

// UpsertEndpoints implements Router{} interface.
func (instance *IndexInst) UpsertEndpoints(
	m *mc.DcpEvent, partKey, key, oldKey []byte) []string {

	p := instance.GetPartitionObject()
	if p == nil {
		return nil
	}
	return p.UpsertEndpoints(instance, m, partKey, key, oldKey)
}

// UpsertDeletionEndpoints implements Router{} interface.
func (instance *IndexInst) UpsertDeletionEndpoints(
	m *mc.DcpEvent, partKey, key, oldKey []byte) []string {

	p := instance.GetPartitionObject()
	if p == nil {
		return nil
	}
	return p.UpsertDeletionEndpoints(instance, m, partKey, key, oldKey)
}

// DeletionEndpoints implements Router{} interface.
func (instance *IndexInst) DeletionEndpoints(
	m *mc.DcpEvent, partKey, oldKey []byte) []string {

	p := instance.GetPartitionObject()
	if p == nil {
		return nil
	}
	return p.DeletionEndpoints(instance, m, partKey, oldKey)
}

func (instance *IndexInst) GetPartitionObject() Partition {
	switch instance.GetDefinition().GetPartitionScheme() {
	case PartitionScheme_TEST:
		return instance.GetTp()
	case PartitionScheme_SINGLE:
		return instance.GetSinglePartn()
	case PartitionScheme_KEY:
		// return instance.GetKeyPartn()
	case PartitionScheme_HASH:
		// return instance.GetHashPartn()
	case PartitionScheme_RANGE:
		// return instance.GetRangePartn()
	}
	return nil
}

// IndexEvaluator implements `Evaluator` interface for protobuf
// definition of an index instance.
type IndexEvaluator struct {
	skExprs  []interface{} // compiled expression
	pkExprs  []interface{} // compiled expression
	whExpr   interface{}   // compiled expression
	instance *IndexInst
	version  FeedVersion
}

// NewIndexEvaluator returns a reference to a new instance
// of IndexEvaluator.
func NewIndexEvaluator(instance *IndexInst,
	version FeedVersion) (*IndexEvaluator, error) {

	var err error

	ie := &IndexEvaluator{instance: instance, version: version}
	// compile expressions once and reuse it many times.
	defn := ie.instance.GetDefinition()
	exprtype := defn.GetExprType()
	switch exprtype {
	case ExprType_N1QL:
		// expressions to evaluate secondary-key
		exprs := defn.GetSecExpressions()
		ie.skExprs, err = CompileN1QLExpression(exprs)
		if err != nil {
			return nil, err
		}
		// expression to evaluate partition key
		exprs = defn.GetPartnExpressions()
		if len(exprs) > 0 {
			cExprs, err := CompileN1QLExpression(exprs)
			if err != nil {
				return nil, err
			} else if len(cExprs) > 0 {
				ie.pkExprs = cExprs
			}
		}
		// expression to evaluate where clause
		expr := defn.GetWhereExpression()
		if len(expr) > 0 {
			cExprs, err := CompileN1QLExpression([]string{expr})
			if err != nil {
				return nil, err
			} else if len(cExprs) > 0 {
				ie.whExpr = cExprs[0]
			}
		}

	default:
		logging.Errorf("invalid expression type %v\n", exprtype)
		return nil, fmt.Errorf("invalid expression type %v", exprtype)
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
	kv := c.NewKeyVersions(seqno, nil, 1, 0 /*ctime*/)
	kv.AddStreamBegin()
	return &c.DataportKeyVersions{bucket, vbno, vbuuid, kv}
}

// SyncData implement Evaluator{} interface.
func (ie *IndexEvaluator) SyncData(
	vbno uint16, vbuuid, seqno uint64) (data interface{}) {

	bucket := ie.Bucket()
	kv := c.NewKeyVersions(seqno, nil, 1, 0 /*ctime*/)
	kv.AddSync()
	return &c.DataportKeyVersions{bucket, vbno, vbuuid, kv}
}

// SnapshotData implement Evaluator{} interface.
func (ie *IndexEvaluator) SnapshotData(
	m *mc.DcpEvent, vbno uint16, vbuuid, seqno uint64) (data interface{}) {

	bucket := ie.Bucket()
	kv := c.NewKeyVersions(seqno, nil, 1, m.Ctime)
	kv.AddSnapshot(m.SnapshotType, m.SnapstartSeq, m.SnapendSeq)
	return &c.DataportKeyVersions{bucket, vbno, vbuuid, kv}
}

// StreamEndData implement Evaluator{} interface.
func (ie *IndexEvaluator) StreamEndData(
	vbno uint16, vbuuid, seqno uint64) (data interface{}) {

	bucket := ie.Bucket()
	kv := c.NewKeyVersions(seqno, nil, 1, 0 /*ctime*/)
	kv.AddStreamEnd()
	return &c.DataportKeyVersions{bucket, vbno, vbuuid, kv}
}

// TransformRoute implement Evaluator{} interface.
func (ie *IndexEvaluator) TransformRoute(
	vbuuid uint64, m *mc.DcpEvent, data map[string]interface{},
	encodeBuf []byte) ([]byte, error) {
	var err error
	defer func() { // panic safe
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()

	if ie.version < FeedVersion_watson {
		encodeBuf = nil
	}

	var npkey /*new-partition*/, opkey /*old-partition*/, nkey, okey []byte
	var newBuf []byte
	instn := ie.instance

	meta := dcpEvent2Meta(m)
	where, err := ie.wherePredicate(m, m.Value, meta, encodeBuf)
	if err != nil {
		return nil, err
	}

	if where && len(m.Value) > 0 { // project new secondary key
		if npkey, err = ie.partitionKey(m, m.Key, m.Value, meta, encodeBuf); err != nil {
			return nil, err
		}
		if nkey, newBuf, err = ie.evaluate(m, m.Key, m.Value, meta, encodeBuf); err != nil {
			return nil, err
		}
	}
	if len(m.OldValue) > 0 { // project old secondary key
		if opkey, err = ie.partitionKey(m, m.Key, m.OldValue, meta, encodeBuf); err != nil {
			return nil, err
		}
		if okey, newBuf, err = ie.evaluate(m, m.Key, m.OldValue, meta, encodeBuf); err != nil {
			return nil, err
		}
	}

	vbno, seqno := m.VBucket, m.Seqno
	uuid := instn.GetInstId()

	bucket := ie.Bucket()

	arg1 := logging.TagUD(string(npkey))
	arg2 := logging.TagUD(string(nkey))
	logging.Tracef("inst: %v where: %v (pkey: %v) key: %v\n",
		uuid, where, arg1, arg2)
	switch m.Opcode {
	case mcd.DCP_MUTATION:
		// FIXME: TODO: where clause is not used to for optimizing out messages
		// not passing the where clause. For this we need a gaurantee that
		// where clause will be defined only on immutable fields.

		if where { // WHERE predicate, sent upsert only if where is true.
			raddrs := instn.UpsertEndpoints(m, npkey, nkey, okey)
			for _, raddr := range raddrs {
				dkv, ok := data[raddr].(*c.DataportKeyVersions)
				if !ok {
					kv := c.NewKeyVersions(seqno, m.Key, 4, m.Ctime)
					kv.AddUpsert(uuid, nkey, okey, npkey)
					dkv = &c.DataportKeyVersions{bucket, vbno, vbuuid, kv}
				} else {
					dkv.Kv.AddUpsert(uuid, nkey, okey, npkey)
				}
				data[raddr] = dkv
			}
		} else { // if WHERE is false, broadcast upsertdelete.
			// NOTE: downstream can use upsertdelete and immutable flag
			// to optimize out back-index lookup.
			raddrs := instn.UpsertDeletionEndpoints(m, opkey, nkey, okey)
			for _, raddr := range raddrs {
				dkv, ok := data[raddr].(*c.DataportKeyVersions)
				if !ok {
					kv := c.NewKeyVersions(seqno, m.Key, 4, m.Ctime)
					kv.AddUpsertDeletion(uuid, okey, npkey)
					dkv = &c.DataportKeyVersions{bucket, vbno, vbuuid, kv}
				} else {
					dkv.Kv.AddUpsertDeletion(uuid, okey, npkey)
				}
				data[raddr] = dkv
			}
		}

	case mcd.DCP_DELETION, mcd.DCP_EXPIRATION:

		// Delete shall be broadcasted if old-key is not available.
		raddrs := instn.DeletionEndpoints(m, opkey, okey)
		for _, raddr := range raddrs {
			dkv, ok := data[raddr].(*c.DataportKeyVersions)
			if !ok {
				kv := c.NewKeyVersions(seqno, m.Key, 4, m.Ctime)
				kv.AddDeletion(uuid, okey, npkey)
				dkv = &c.DataportKeyVersions{bucket, vbno, vbuuid, kv}
			} else {
				dkv.Kv.AddDeletion(uuid, okey, npkey)
			}
			data[raddr] = dkv
		}
	}
	return newBuf, nil
}

func (ie *IndexEvaluator) evaluate(
	m *mc.DcpEvent, docid, doc []byte,
	meta map[string]interface{}, encodeBuf []byte) ([]byte, []byte, error) {

	defn := ie.instance.GetDefinition()
	if defn.GetIsPrimary() { // primary index supported !!
		return []byte(`["` + string(docid) + `"]`), nil, nil
	}

	if m.IsJSON() == false {
		return nil, nil, nil
	}

	exprType := defn.GetExprType()
	switch exprType {
	case ExprType_N1QL:
		return N1QLTransform(docid, doc, ie.skExprs, meta, encodeBuf)
	}
	return nil, nil, nil
}

func (ie *IndexEvaluator) partitionKey(
	m *mc.DcpEvent, docid, doc []byte,
	meta map[string]interface{}, encodeBuf []byte) ([]byte, error) {

	defn := ie.instance.GetDefinition()
	if ie.pkExprs == nil { // no partition key
		return nil, nil
	}
	if m.IsJSON() == false {
		return nil, nil
	}

	exprType := defn.GetExprType()
	switch exprType {
	case ExprType_N1QL:
		out, _, err := N1QLTransform(docid, doc, ie.pkExprs, meta, nil)
		return out, err
	}
	return nil, nil
}

func (ie *IndexEvaluator) wherePredicate(
	m *mc.DcpEvent, doc []byte,
	meta map[string]interface{}, encodeBuf []byte) (bool, error) {

	// if where predicate is not supplied - always evaluate to `true`
	if ie.whExpr == nil {
		return true, nil
	}

	if m.IsJSON() == false {
		return false, nil
	}

	defn := ie.instance.GetDefinition()
	exprType := defn.GetExprType()
	switch exprType {
	case ExprType_N1QL:
		// TODO: can be optimized by using a custom N1QL-evaluator.
		out, _, err := N1QLTransform(nil, doc, []interface{}{ie.whExpr}, meta, encodeBuf)
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

// helper functions
func dcpEvent2Meta(m *mc.DcpEvent) map[string]interface{} {
	return map[string]interface{}{
		"id":         string(m.Key),
		"byseqno":    m.Seqno,
		"revseqno":   m.RevSeqno,
		"flags":      m.Flags,
		"expiration": m.Expiry,
		"locktime":   m.LockTime,
		"nru":        m.Nru,
		"cas":        m.Cas,
	}
}
