package protoProjector

import (
	"fmt"
	"strconv"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/stats"

	c "github.com/couchbase/indexing/secondary/common"
	qu "github.com/couchbase/indexing/secondary/common/queryutil"
	mcd "github.com/couchbase/indexing/secondary/dcp/transport"
	mc "github.com/couchbase/indexing/secondary/dcp/transport/client"
	"github.com/couchbase/indexing/secondary/logging"
	qexpr "github.com/couchbase/query/expression"
	qvalue "github.com/couchbase/query/value"
)

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
		return instance.GetKeyPartn()
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
	keyspaceId string
	skExprs    []interface{} // compiled expression
	pkExprs    []interface{} // compiled expression
	whExpr     interface{}   // compiled expression
	instance   *IndexInst
	version    FeedVersion
	xattrs     []string
	stats      *IndexEvaluatorStats

	// For flattened array index, this variable represents
	// the number of keys in the flatten_keys expression
	numFlattenKeys int

	// Position of the array expression in the list of secondary expressions
	arrayPos int

	// Set to true if array is flattened
	isArrayFlattened bool

	indexMissingLeadingKey bool

	// Vector meta related information

	// A boolean shortcut which represents if any of `hasVectorAttr`
	// is set to true. `hasVectorAttr` is read from index definition
	isVectorIndex bool

	// For a vector index, captures the position of the vector in
	// the list of skExprs. This position is relative to "skExprs"

	// E.g., for the index
	// create index idx on default(name, FLATTEN_ARRAY(v.brand, v.cost) for v in brands END, color VECTOR)
	// vectorPos will be "2" as it is at 2nd position in the list of secondary expressions (0-indexed)
	// In this case, the hasVectorAttr will be [false false false true] as it is derived for exploded
	// version of secondary keys
	//
	// If the vector attribute is inside FLATTEN_ARRAY, then vectorPos == flattenArrayPos
	// and the entry at vectorPosInFlattenedArray will be considered for processing the
	// vector
	// create index idx on default(name, FLATTEN_ARRAY(v.brand, v.cost VECTOR) for v in brands END, color)
	// vectorPos == arrayPos == 1 and isFlattenedArray == true and vectorPosInFlattenedArr = 1
	vectorPos int

	// If `vectorPos == arrayPos` and array is flattened, then `vectorPosInFlattenedArray`
	// captures the relative position of the VECTOR field inside the flattened
	// array
	vectorPosInFlattenedArray int

	// `dimension` represents the vector dimension with which the index has been
	// created
	dimension int

	// The expresses to be evaluated for include columns
	includeExprs []interface{}

	// coineSimilarity
	isCosine bool
}

// NewIndexEvaluator returns a reference to a new instance
// of IndexEvaluator.
func NewIndexEvaluator(
	instance *IndexInst,
	version FeedVersion,
	keyspaceId string) (*IndexEvaluator, error) {

	var err error

	ie := &IndexEvaluator{
		keyspaceId:                keyspaceId,
		instance:                  instance,
		version:                   version,
		arrayPos:                  -1,
		isArrayFlattened:          false,
		isVectorIndex:             false,
		vectorPos:                 -1,
		vectorPosInFlattenedArray: -1,
	}

	// compile expressions once and reuse it many times.
	defn := ie.instance.GetDefinition()

	ie.indexMissingLeadingKey = defn.GetIndexMissingLeadingKey()

	exprtype := defn.GetExprType()
	switch exprtype {
	case ExprType_N1QL:
		xattrExprs := make([]string, 0)
		// expressions to evaluate secondary-key
		exprs := defn.GetSecExpressions()
		xattrExprs = append(xattrExprs, exprs...)
		ie.skExprs, err = CompileN1QLExpression(exprs)
		if err != nil {
			return nil, err
		}

		flattenedArrayPos := 0
		for pos, skExpr := range ie.skExprs {
			expr := skExpr.(qexpr.Expression)
			isArray, _, isFlattened := expr.IsArrayIndexKey()
			if isArray {
				ie.arrayPos = pos
			}
			if isArray && isFlattened {
				// Populate numFlattenKeys and break
				ie.isArrayFlattened = isFlattened
				ie.numFlattenKeys = expr.(*qexpr.All).FlattenSize()
				break
			}
		}

		// expression to evaluate partition key
		exprs = defn.GetPartnExpressions()
		xattrExprs = append(xattrExprs, exprs...)
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
			xattrExprs = append(xattrExprs, expr)
			cExprs, err := CompileN1QLExpression([]string{expr})
			if err != nil {
				return nil, err
			} else if len(cExprs) > 0 {
				ie.whExpr = cExprs[0]
			}
		}
		_, xattrNames, _ := qu.GetXATTRNames(xattrExprs)
		ie.xattrs = xattrNames

		// Vector index related information
		hasVectorAttr := defn.GetHasVectorAttr()

		// Check if any secondary field or expression has VECTOR attribute
		for i := range hasVectorAttr {
			if hasVectorAttr[i] {
				ie.isVectorIndex = true

				if ie.isArrayFlattened {
					flattenedArrayPos = ie.arrayPos
					if i < flattenedArrayPos {
						ie.vectorPos = i
					} else if i >= flattenedArrayPos && i < flattenedArrayPos+ie.numFlattenKeys {
						ie.vectorPos = flattenedArrayPos
						ie.vectorPosInFlattenedArray = i - flattenedArrayPos
					} else {
						ie.vectorPos = i - ie.numFlattenKeys + 1
					}
				} else {
					ie.vectorPos = i
				}
				break
			}
		}

		// Expected dimension of vectors in incoming document
		ie.dimension = int(defn.GetDimension())
		ie.isCosine = defn.GetIsCosine()

		// Secondary fields or expressions for include columns
		include := defn.GetInclude()
		if len(include) > 0 {
			includeExprs, err := CompileN1QLExpression(include)
			if err != nil {
				return nil, err
			} else if len(includeExprs) > 0 {
				ie.includeExprs = includeExprs
			}
		}

		logging.Infof("NewIndexEvaluator: InstId: %v, isVectorIndex: %v, vectorPos: %v, arrayPos: %v, "+
			"vectorPosInFlattenedArray: %v, dimension: %v, include: %v", ie.instance.GetInstId(), ie.isVectorIndex,
			ie.vectorPos, ie.arrayPos, ie.vectorPosInFlattenedArray, ie.dimension, ie.includeExprs)

	default:
		logging.Errorf("invalid expression type %v\n", exprtype)
		return nil, fmt.Errorf("invalid expression type %v", exprtype)
	}

	ie.stats = &IndexEvaluatorStats{}
	ie.stats.Init()
	return ie, nil
}

// Bucket implements Evaluator{} interface.
func (ie *IndexEvaluator) Bucket() string {
	return ie.instance.GetDefinition().GetBucket()
}

func (ie *IndexEvaluator) Scope() string {
	return ie.instance.GetDefinition().GetScope()
}

func (ie *IndexEvaluator) Collection() string {
	return ie.instance.GetDefinition().GetCollection()
}

func (ie *IndexEvaluator) GetKeyspaceId() string {
	return ie.keyspaceId
}

// StreamBeginData implement Evaluator{} interface.
func (ie *IndexEvaluator) StreamBeginData(
	vbno uint16, vbuuid, seqno uint64, nodeUUID string,
	status byte, code byte, opaque2 uint64, oso bool) (data interface{}) {

	keyspaceId := ie.GetKeyspaceId()
	kv := c.NewKeyVersions(seqno, []byte(nodeUUID), 1, 0 /*ctime*/)
	kv.AddStreamBegin(status, code)
	return &c.DataportKeyVersions{keyspaceId, vbno, vbuuid, kv, opaque2, oso}
}

// SyncData implement Evaluator{} interface.
func (ie *IndexEvaluator) SyncData(
	vbno uint16, vbuuid, seqno uint64,
	opaque2 uint64, oso bool) (data interface{}) {

	keyspaceId := ie.GetKeyspaceId()
	kv := c.NewKeyVersions(seqno, nil, 1, 0 /*ctime*/)
	kv.AddSync()
	return &c.DataportKeyVersions{keyspaceId, vbno, vbuuid, kv, opaque2, oso}
}

// SnapshotData implement Evaluator{} interface.
func (ie *IndexEvaluator) SnapshotData(
	m *mc.DcpEvent, vbno uint16, vbuuid, seqno uint64,
	opaque2 uint64, oso bool) (data interface{}) {

	keyspaceId := ie.GetKeyspaceId()
	kv := c.NewKeyVersions(seqno, nil, 1, m.Ctime)
	kv.AddSnapshot(m.SnapshotType, m.SnapstartSeq, m.SnapendSeq)
	return &c.DataportKeyVersions{keyspaceId, vbno, vbuuid, kv, opaque2, oso}
}

// SystemEventData implement Evaluator{} interface.
func (ie *IndexEvaluator) SystemEventData(
	m *mc.DcpEvent, vbno uint16, vbuuid, seqno uint64,
	opaque2 uint64, oso bool) (data interface{}) {

	keyspaceId := ie.GetKeyspaceId()
	kv := c.NewKeyVersions(seqno, nil, 1, m.Ctime)
	cid := strconv.FormatUint(uint64(m.CollectionID), 16) //transmit as base-16 string
	kv.AddSystemEvent(m.EventType, m.ManifestUID, m.ScopeID, []byte(cid))
	return &c.DataportKeyVersions{keyspaceId, vbno, vbuuid, kv, opaque2, oso}
}

// UpdateSeqnoData implement Evaluator{} interface.
func (ie *IndexEvaluator) UpdateSeqnoData(
	m *mc.DcpEvent, vbno uint16, vbuuid, seqno uint64,
	opaque2 uint64, oso bool) (data interface{}) {

	keyspaceId := ie.GetKeyspaceId()
	kv := c.NewKeyVersions(seqno, m.Key, 1, 0)
	kv.AddUpdateSeqno()
	return &c.DataportKeyVersions{keyspaceId, vbno, vbuuid, kv, opaque2, oso}
}

// SeqnoAdvancedData implement Evaluator{} interface.
func (ie *IndexEvaluator) SeqnoAdvancedData(
	m *mc.DcpEvent, vbno uint16, vbuuid, seqno uint64,
	opaque2 uint64, oso bool) (data interface{}) {

	keyspaceId := ie.GetKeyspaceId()
	kv := c.NewKeyVersions(seqno, nil, 1, 0)
	kv.AddSeqnoAdvanced()
	return &c.DataportKeyVersions{keyspaceId, vbno, vbuuid, kv, opaque2, oso}
}

// SeqnoAdvancedData implement Evaluator{} interface.
func (ie *IndexEvaluator) OSOSnapshotData(
	m *mc.DcpEvent, vbno uint16, vbuuid,
	opaque2 uint64, oso bool) (data interface{}) {

	keyspaceId := ie.GetKeyspaceId()
	kv := c.NewKeyVersions(0 /*seqno*/, nil, 1, 0)
	kv.AddOSOSnapshot(m.EventType)
	return &c.DataportKeyVersions{keyspaceId, vbno, vbuuid, kv, opaque2, oso}
}

// StreamEndData implement Evaluator{} interface.
func (ie *IndexEvaluator) StreamEndData(
	vbno uint16, vbuuid, seqno uint64, opaque2 uint64,
	oso bool) (data interface{}) {

	keyspaceId := ie.GetKeyspaceId()
	kv := c.NewKeyVersions(seqno, nil, 1, 0 /*ctime*/)
	kv.AddStreamEnd()
	return &c.DataportKeyVersions{keyspaceId, vbno, vbuuid, kv, opaque2, oso}
}

func (ie *IndexEvaluator) ProcessEvent(m *mc.DcpEvent, encodeBuf []byte,
	docval qvalue.AnnotatedValue, context qexpr.Context,
	pl *logging.TimedNStackTraces) (npkey, opkey, nkey, okey, includeColumn, newBuf []byte,
	where bool, opcode mcd.CommandCode, nVectors [][]float32, centroidPos []int32, err error) {

	defer func() { // panic safe
		if r := recover(); r != nil {
			errStr := fmt.Sprintf("ProcessEvent: %v", r)
			err = fmt.Errorf(errStr)
			if pl != nil {
				pl.TryLogStackTrace(logging.Error, errStr)
			}
		}
	}()

	if ie.version < FeedVersion_watson {
		encodeBuf = nil
	}

	instn := ie.instance

	defn := instn.Definition
	retainDelete := m.HasXATTR() && defn.GetRetainDeletedXATTR() &&
		(m.Opcode == mcd.DCP_DELETION || m.Opcode == mcd.DCP_EXPIRATION)

	opcode = m.Opcode
	if retainDelete {
		// TODO: Replace with isMetaIndex()
		m.TreatAsJSON()
		opcode = mcd.DCP_MUTATION
		nvalue := qvalue.NewParsedValueWithOptions(c.NULL, true, true)
		docval = qvalue.NewAnnotatedValue(nvalue)
	}

	ie.dcpEvent2Meta(m, docval)
	where, err = ie.wherePredicate(m, docval, context, encodeBuf)
	if err != nil {
		return npkey, opkey, nkey, okey, includeColumn, newBuf, where, opcode, nil, nil, err
	}

	npkey, err = ie.partitionKey(m, m.Key, docval, context, encodeBuf)
	if err != nil {
		return npkey, opkey, nkey, okey, includeColumn, newBuf, where, opcode, nil, nil, err
	}

	if where && (len(m.Value) > 0 || retainDelete) { // project new secondary key
		nkey, newBuf, nVectors, centroidPos, err = ie.evaluate(m, m.Key, docval, context, encodeBuf)
		if err != nil {
			return npkey, opkey, nkey, okey, includeColumn, newBuf, where, opcode, nVectors, centroidPos, err
		}

		if cap(newBuf) > cap(encodeBuf) {
			encodeBuf = newBuf
		}

		// evaluate() will allocate new memory for the final collate JSON encoded value
		// Hence, re-use the encodeBuf for include column evaluation
		includeColumn, newBuf, err = ie.includeColumns(m, m.Key, docval, context, encodeBuf[:0])
		if err != nil {
			return npkey, opkey, nkey, okey, includeColumn, newBuf, where, opcode, nil, nil, err
		}
	}
	if len(m.OldValue) > 0 { // project old secondary key
		nvalue := qvalue.NewParsedValueWithOptions(m.OldValue, true, true)
		oldval := qvalue.NewAnnotatedValue(nvalue)
		oldval.ShareAnnotations(docval)
		opkey, err = ie.partitionKey(m, m.Key, oldval, context, encodeBuf)
		if err != nil {
			return npkey, opkey, nkey, okey, includeColumn, newBuf, where, opcode, nil, nil, err
		}
		okey, newBuf, _, _, err = ie.evaluate(m, m.Key, oldval, context, encodeBuf)
		if err != nil {
			return npkey, opkey, nkey, okey, includeColumn, newBuf, where, opcode, nil, nil, err
		}
	}

	return npkey, opkey, nkey, okey, includeColumn, newBuf, where, opcode, nVectors, centroidPos, nil
}

// TransformRoute implement Evaluator{} interface.
func (ie *IndexEvaluator) TransformRoute(
	vbuuid uint64, m *mc.DcpEvent, data map[string]interface{}, encodeBuf []byte,
	docval qvalue.AnnotatedValue, context qexpr.Context,
	numIndexes int, opaque2 uint64, oso bool, pl *logging.TimedNStackTraces) ([]byte, int, error) {

	var err error
	var npkey /*new-partition*/, opkey /*old-partition*/, nkey, okey, includeColumn []byte
	var newBuf []byte
	var where bool
	var opcode mcd.CommandCode
	var nVectors [][]float32
	var centroidPos []int32

	forceUpsertDeletion := false
	npkey, opkey, nkey, okey, includeColumn, newBuf, where, opcode, nVectors, centroidPos, err = ie.ProcessEvent(m,
		encodeBuf, docval, context, pl)
	if err != nil {
		forceUpsertDeletion = true
	}

	err1 := ie.populateData(vbuuid, m, data, numIndexes, npkey, opkey, nkey, okey, includeColumn,
		where, opcode, nVectors, centroidPos, opaque2, forceUpsertDeletion, oso, pl)

	if err == nil && err1 != nil {
		err = err1
	}

	if err != nil {
		// The decision to perform UpsertDel on error is made in this function.
		// So, this function will record the count of Error Skip mutations.
		ie.stats.ErrSkip.Add(1)
		ie.stats.ErrSkipAll.Add(1)
	}

	return newBuf, len(nkey), err
}

func (ie *IndexEvaluator) populateData(vbuuid uint64, m *mc.DcpEvent,
	data map[string]interface{}, numIndexes int, npkey, opkey []byte,
	nkey, okey, includeColumn []byte, where bool, opcode mcd.CommandCode, nVectors [][]float32,
	centroidPos []int32, opaque2 uint64, forceUpsertDeletion bool, oso bool, pl *logging.TimedNStackTraces) (err error) {

	defer func() { // panic safe
		if r := recover(); r != nil {
			errStr := fmt.Sprintf("populateData: %v", r)
			err = fmt.Errorf(errStr)
			if pl != nil {
				pl.TryLogStackTrace(logging.Error, errStr)
			}
		}
	}()

	vbno, seqno := m.VBucket, m.Seqno
	instn := ie.instance
	uuid := instn.GetInstId()

	keyspaceId := ie.GetKeyspaceId()

	logging.LazyTrace(func() string {
		return fmt.Sprintf("inst: %v where: %v (pkey: %v) key: %v\n", uuid, where,
			logging.TagUD(string(npkey)), logging.TagUD(string(nkey)))
	})

	processUpsert := func(raddrs []string) {
		for _, raddr := range raddrs {
			dkv, ok := data[raddr].(*c.DataportKeyVersions)
			if !ok {
				kv := c.NewKeyVersions(seqno, m.Key, numIndexes, m.Ctime)
				if len(nVectors) > 0 {
					kv.AddUpsertWithVectors(uuid, nkey, okey, npkey, includeColumn, nVectors, centroidPos)
				} else {
					kv.AddUpsert(uuid, nkey, okey, npkey, includeColumn)
				}
				dkv = &c.DataportKeyVersions{keyspaceId, vbno, vbuuid,
					kv, opaque2, oso}
			} else {
				if len(nVectors) > 0 {
					dkv.Kv.AddUpsertWithVectors(uuid, nkey, okey, npkey, includeColumn, nVectors, centroidPos)
				} else {
					dkv.Kv.AddUpsert(uuid, nkey, okey, npkey, includeColumn)
				}
			}
			data[raddr] = dkv
		}
	}

	processUpsertDel := func() {
		raddrs := instn.UpsertDeletionEndpoints(m, npkey, nkey, okey)
		for _, raddr := range raddrs {
			dkv, ok := data[raddr].(*c.DataportKeyVersions)
			if !ok {
				kv := c.NewKeyVersions(seqno, m.Key, numIndexes, m.Ctime)
				if len(nVectors) > 0 {
					kv.AddUpsertDeletionWithVectors(uuid, okey, npkey, includeColumn, nVectors, centroidPos)
				} else {
					kv.AddUpsertDeletion(uuid, okey, npkey, includeColumn)
				}
				dkv = &c.DataportKeyVersions{keyspaceId, vbno, vbuuid,
					kv, opaque2, oso}
			} else {
				if len(nVectors) > 0 {
					dkv.Kv.AddUpsertDeletionWithVectors(uuid, okey, npkey, includeColumn, nVectors, centroidPos)
				} else {
					dkv.Kv.AddUpsertDeletion(uuid, okey, npkey, includeColumn)
				}
			}
			data[raddr] = dkv
		}
	}

	processDeletion := func() {
		raddrs := instn.DeletionEndpoints(m, opkey, okey)
		for _, raddr := range raddrs {
			dkv, ok := data[raddr].(*c.DataportKeyVersions)
			if !ok {
				kv := c.NewKeyVersions(seqno, m.Key, numIndexes, m.Ctime)
				kv.AddDeletion(uuid, okey, npkey, includeColumn)
				dkv = &c.DataportKeyVersions{keyspaceId, vbno, vbuuid,
					kv, opaque2, oso}
			} else {
				dkv.Kv.AddDeletion(uuid, okey, npkey, includeColumn)
			}
			data[raddr] = dkv
		}
	}

	if forceUpsertDeletion {
		processUpsertDel()
		return
	}

	switch opcode {
	case mcd.DCP_MUTATION:
		// FIXME: TODO: where clause is not used to for optimizing out messages
		// not passing the where clause. For this we need a gaurantee that
		// where clause will be defined only on immutable fields.
		if where { // WHERE predicate, sent upsert only if where is true.
			raddrs := instn.UpsertEndpoints(m, npkey, nkey, okey)
			if len(raddrs) != 0 {
				processUpsert(raddrs)
			} else {
				// send upsertDeletion if cannot find an endpoint that can accept this mutation
				// for the given feed
				processUpsertDel()
			}
		} else { // if WHERE is false, broadcast upsertdelete.
			// NOTE: downstream can use upsertdelete and immutable flag
			// to optimize out back-index lookup.
			processUpsertDel()
		}

	case mcd.DCP_DELETION, mcd.DCP_EXPIRATION:

		// Delete shall be broadcasted if old-key is not available.
		processDeletion()
	}

	return nil
}

func (ie *IndexEvaluator) Stats() interface{} {
	return ie.stats
}

func (ie *IndexEvaluator) evaluate(
	m *mc.DcpEvent, docid []byte, docval qvalue.AnnotatedValue,
	context qexpr.Context, encodeBuf []byte) ([]byte, []byte, [][]float32, []int32, error) {

	defn := ie.instance.GetDefinition()
	if defn.GetIsPrimary() { // primary index supported !!
		return []byte(`["` + string(docid) + `"]`), nil, nil, nil, nil
	}

	exprType := defn.GetExprType()
	switch exprType {
	case ExprType_N1QL:
		if ie.isVectorIndex {
			return N1QLTransformForVectorIndex(docid, docval, context, encodeBuf, ie)
		} else {
			out, newBuf, err := N1QLTransform(docid, docval, context, ie.skExprs,
				ie.numFlattenKeys, encodeBuf, ie.stats,
				ie.indexMissingLeadingKey)
			return out, newBuf, nil, nil, err
		}
	}
	return nil, nil, nil, nil, nil
}

func (ie *IndexEvaluator) partitionKey(
	m *mc.DcpEvent, docid []byte, docval qvalue.AnnotatedValue,
	context qexpr.Context, encodeBuf []byte) ([]byte, error) {

	defn := ie.instance.GetDefinition()
	if ie.pkExprs == nil { // no partition key
		return nil, nil
	}

	exprType := defn.GetExprType()
	switch exprType {
	case ExprType_N1QL:
		out, _, err := N1QLTransform(docid, docval, context, ie.pkExprs,
			0, nil, ie.stats, ie.indexMissingLeadingKey)
		return out, err
	}
	return nil, nil
}

func (ie *IndexEvaluator) wherePredicate(
	m *mc.DcpEvent, docval qvalue.AnnotatedValue,
	context qexpr.Context, encodeBuf []byte) (bool, error) {

	// if where predicate is not supplied - always evaluate to `true`
	if ie.whExpr == nil {
		return true, nil
	}

	defn := ie.instance.GetDefinition()
	exprType := defn.GetExprType()
	switch exprType {
	case ExprType_N1QL:
		// TODO: can be optimized by using a custom N1QL-evaluator.
		out, _, err := N1QLTransform(nil, docval, context,
			[]interface{}{ie.whExpr}, 0, encodeBuf, ie.stats, false)
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

func (ie *IndexEvaluator) includeColumns(
	m *mc.DcpEvent, docid []byte, docval qvalue.AnnotatedValue,
	context qexpr.Context, encodeBuf []byte) ([]byte, []byte, error) {

	defn := ie.instance.GetDefinition()
	if ie.includeExprs == nil { // no include expressions
		return nil, encodeBuf, nil
	}

	exprType := defn.GetExprType()
	switch exprType {
	case ExprType_N1QL:
		out, newBuf, err := N1QLTransform(docid, docval, context, ie.includeExprs,
			0, encodeBuf, ie.stats, false)
		return out, newBuf, err
	}
	return nil, nil, nil
}

// helper functions
func (ie *IndexEvaluator) dcpEvent2Meta(m *mc.DcpEvent, docval qvalue.AnnotatedValue) {
	// If index is defined on xattr (either where-expression, part-expression
	// or secondary-expression) then unmarshall XATTR, and only one for this
	// event, and only used XATTRs. Cache the results for reuse.
	if len(ie.xattrs) > 0 && m.ParsedXATTR == nil {
		m.ParsedXATTR = make(map[string]interface{})
	}
	for _, xattr := range ie.xattrs {
		if _, ok := m.ParsedXATTR[xattr]; !ok {
			if len(m.RawXATTR[xattr]) != 0 {
				m.ParsedXATTR[xattr] = qvalue.NewParsedValueWithOptions(m.RawXATTR[xattr], true, true)
			}
		}
	}

	docval.SetMetaField(qvalue.META_BYSEQNO, m.Seqno)
	docval.SetMetaField(qvalue.META_REVSEQNO, m.RevSeqno)
	docval.SetMetaField(qvalue.META_FLAGS, m.Flags)
	docval.SetMetaField(qvalue.META_EXPIRATION, m.Expiry)
	docval.SetMetaField(qvalue.META_LOCKTIME, m.LockTime)
	docval.SetMetaField(qvalue.META_NRU, m.Nru)
	docval.SetMetaField(qvalue.META_CAS, m.Cas)
	docval.SetMetaField(qvalue.META_XATTRS, m.ParsedXATTR)
	docval.SetId(string(m.Key))
}

// GetIndexName implements Evaluator{} interface.
func (ie *IndexEvaluator) GetIndexName() string {
	return ie.instance.GetDefinition().GetName()
}

func (ie *IndexEvaluator) GetCollectionID() string {
	return ie.instance.GetDefinition().GetCollectionID()
}

type IndexEvaluatorStats struct {
	Count     stats.Int64Val
	TotalDur  stats.Int64Val
	PrevCount stats.Int64Val
	PrevDur   stats.Int64Val
	SMA       stats.Int64Val // Simple moving average

	// ErrSkip represents number of mutations skipped since the
	// last call to GetAndResetErrorSkip
	ErrSkip stats.Int64Val

	// Total number of mutations skipped since this stat object was initialized.
	ErrSkipAll stats.Int64Val

	ErrInvalidVectorDimension stats.Int64Val
	ErrHeterogenousVectorData stats.Int64Val
	ErrDataOutOfBounds        stats.Int64Val
	ErrInvalidVectorType      stats.Int64Val
	ErrZeroVectorForCosine    stats.Int64Val

	InstId     common.IndexInstId
	Topic      string
	KeyspaceId string
}

func (ie *IndexEvaluatorStats) Init() {
	ie.Count.Init()
	ie.TotalDur.Init()
	ie.PrevCount.Init()
	ie.PrevDur.Init()
	ie.SMA.Init()
	ie.ErrSkip.Init()
	ie.ErrSkipAll.Init()

	ie.ErrInvalidVectorType.Init()
	ie.ErrDataOutOfBounds.Init()
	ie.ErrInvalidVectorDimension.Init()
	ie.ErrHeterogenousVectorData.Init()
	ie.ErrZeroVectorForCosine.Init()
}

func (ies *IndexEvaluatorStats) add(duration time.Duration) {
	ies.Count.Add(1)
	ies.TotalDur.Add(duration.Nanoseconds())
}

// Implements simple moving average. Returns the moving average value
func (ies *IndexEvaluatorStats) MovingAvg() int64 {
	count := ies.Count.Value()
	prevCount := ies.PrevCount.Value()
	totalDur := ies.TotalDur.Value()
	prevDur := ies.PrevDur.Value()
	prevSMA := ies.SMA.Value()

	if count-prevCount > 0 {
		newAvg := (totalDur - prevDur) / (count - prevCount)
		if prevCount > 0 {
			newAvg = (prevSMA + newAvg) / 2
		}
		ies.SMA.Set(newAvg)
		ies.PrevCount.Set(count)
		ies.PrevDur.Set(totalDur)
	}

	return ies.SMA.Value()
}

func (ies *IndexEvaluatorStats) GetAndResetErrorSkip() int64 {
	val := ies.ErrSkip.Value()
	ies.ErrSkip.Add(-val)
	return val
}

func (ies *IndexEvaluatorStats) GetErrorSkipAll() int64 {
	return ies.ErrSkipAll.Value()
}

func (ies *IndexEvaluatorStats) GetVectorErrs() map[string]int64 {
	out := make(map[string]int64)
	if ies.ErrDataOutOfBounds.Value() > 0 {
		out[getVectorStatStr(ErrDataOutOfBounds)] = ies.ErrDataOutOfBounds.Value()
	}

	if ies.ErrInvalidVectorDimension.Value() > 0 {
		out[getVectorStatStr(ErrInvalidVectorDimension)] = ies.ErrInvalidVectorDimension.Value()
	}

	if ies.ErrHeterogenousVectorData.Value() > 0 {
		out[getVectorStatStr(ErrHeterogenousVectorData)] = ies.ErrHeterogenousVectorData.Value()
	}

	if ies.ErrInvalidVectorType.Value() > 0 {
		out[getVectorStatStr(ErrInvalidVectorType)] = ies.ErrInvalidVectorType.Value()
	}

	if ies.ErrZeroVectorForCosine.Value() > 0 {
		out[getVectorStatStr(ErrZeroVectorForCosine)] = ies.ErrZeroVectorForCosine.Value()
	}

	return out

}

func (ies *IndexEvaluatorStats) updateErrCount(err error) {
	switch err {
	case ErrDataOutOfBounds:
		ies.ErrDataOutOfBounds.Add(1)
		return
	case ErrInvalidVectorDimension:
		ies.ErrInvalidVectorDimension.Add(1)
		return
	case ErrInvalidVectorType:
		ies.ErrInvalidVectorType.Add(1)
		return
	case ErrHeterogenousVectorData:
		ies.ErrHeterogenousVectorData.Add(1)
		return
	case ErrZeroVectorForCosine:
		ies.ErrZeroVectorForCosine.Add(1)
		return
	}
}
