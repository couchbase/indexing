// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/indexing/secondary/collatejson"
	"github.com/couchbase/indexing/secondary/common"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/query"
	"github.com/couchbase/indexing/secondary/vector/codebook"
	"github.com/golang/protobuf/proto"

	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/expression/parser"
	"github.com/couchbase/query/value"
)

type ScanReqType string

const (
	StatsReq          ScanReqType = "stats"
	CountReq                      = "count"
	ScanReq                       = "scan"
	ScanAllReq                    = "scanAll"
	HeloReq                       = "helo"
	MultiScanCountReq             = "multiscancount"
	FastCountReq                  = "fastcountreq" //generated internally
	VectorScanReq                 = "vectorscanreq"

	// VECTOR_TODO: Check if this is needed. ScanAllReq with distances substituted
	VectorScanAllReq = "vectorscanallreq"
)

type ScanRequest struct {
	ScanType     ScanReqType
	DefnID       uint64
	IndexInstId  common.IndexInstId
	IndexName    string
	Bucket       string
	CollectionId string
	PartitionIds []common.PartitionId
	Ts           *common.TsVbuuid
	Low          IndexKey
	High         IndexKey
	Keys         []IndexKey
	Consistency  *common.Consistency
	Stats        *IndexStats
	IndexInst    common.IndexInst

	Ctxs []IndexReaderContext

	// user supplied
	LowBytes, HighBytes []byte
	KeysBytes           [][]byte

	Incl      Inclusion
	Limit     int64
	isPrimary bool

	// New parameters for API2 pushdowns
	Scans             []Scan
	Indexprojection   *Projection
	Reverse           bool
	Distinct          bool
	Offset            int64
	projectPrimaryKey bool

	//groupby/aggregate

	GroupAggr *GroupAggr

	//below two arrays indicate what parts of composite keys
	//need to be exploded and decoded. explodeUpto indicates
	//maximum position of explode or decode
	explodePositions []bool
	decodePositions  []bool
	explodeUpto      int

	maxCompositeFilters int

	// New parameters for partitioned index
	Sorted bool

	// Rollback Time
	rollbackTime int64

	ScanId      uint64
	ExpiredTime time.Time
	Timeout     *time.Timer
	CancelCh    <-chan bool

	RequestId string
	LogPrefix string

	keyBufList      []*[]byte
	indexKeyBuffer  []byte
	sharedBuffer    *[]byte
	sharedBufferLen int

	hasRollback *atomic.Value

	sco *scanCoordinator

	connCtx *ConnectionContext

	dataEncFmt common.DataEncodingFormat
	keySzCfg   keySizeConfig

	User             string // For read metering
	SkipReadMetering bool

	nprobes               int
	vectorPos             int
	isVectorScan          bool
	isBhiveScan           bool
	queryVector           []float32
	codebookMap           map[common.PartitionId]codebook.Codebook
	centroidMap           map[common.PartitionId][]int64
	protoScans            []*protobuf.Scan
	vectorScans           map[common.PartitionId]map[int64][]Scan
	parallelCentroidScans int
	indexOrder            *IndexKeyOrder
	projectVectorDist     bool // set to true if vector distance has to be projected. false otherwise

	// re-ranking support for BHIVE vector indexes
	enableReranking bool // set to 'true' if re-ranking is enabled

	// For re-ranking, the number of rows scanned will be typically "R" times
	// greater than the actual limit in the scan. E.g., if "limit 10" is issued
	// in the scan, then R * 10 will be the actual rows scanned. Re-ranking
	// will be perfomed on these "R * 10" rows. The variable "rlimit" captures
	// this limit
	rlimit int64

	perPartnSnaps       map[common.PartitionId]SliceSnapshot
	perPartnReaderCtx   map[common.PartitionId][]IndexReaderContext
	readersPerPartition int

	indexKeyNames    []string
	inlineFilter     string
	inlineFilterExpr expression.Expression
}

type IndexKeyOrder struct {
	vectorDistOnly  bool
	vectorDistDesc  bool
	inIndexOrder    bool
	posInIndexOrder int
	keyPos          []int32
	desc            []bool
}

func (io *IndexKeyOrder) IsSortKeyNeeded() bool {
	if io.vectorDistOnly {
		return false
	}
	// if io.inIndexOrder {
	// 	return false
	// }
	return true
}

func (io *IndexKeyOrder) IsDistAdditionNeeded() bool {
	if io.vectorDistOnly {
		return false
	}
	if io.inIndexOrder {
		return true
	}
	return false
}

func (io *IndexKeyOrder) IsOrderAscending() bool {
	if io == nil {
		return true
	}
	if io.vectorDistOnly {
		return !io.vectorDistDesc
	}
	return true
}

type Projection struct {
	projectSecKeys   bool
	projectionKeys   []bool
	entryKeysEmpty   bool
	projectGroupKeys []projGroup

	projectInclude     bool
	projectAllInclude  bool
	projectIncludeKeys []bool
}

type projGroup struct {
	pos    int
	grpKey bool
}

type Scan struct {
	Low      IndexKey  // Overall Low for a Span. Computed from composite filters (Ranges)
	High     IndexKey  // Overall High for a Span. Computed from composite filters (Ranges)
	Incl     Inclusion // Overall Inclusion for a Span
	ScanType ScanFilterType
	Filters  []Filter // A collection qualifying filters
	Equals   IndexKey // TODO: Remove Equals
}

type Filter struct {
	// If composite index has n keys,
	// it will have <= n CompositeElementFilters
	CompositeFilters []CompositeElementFilter
	Low              IndexKey
	High             IndexKey
	Inclusion        Inclusion
	ScanType         ScanFilterType
}

type ScanFilterType string

// RangeReq is a span which is Range on the entire index
// without composite index filtering
// FilterRangeReq is a span request which needs composite
// index filtering
const (
	AllReq         ScanFilterType = "scanAll"
	LookupReq                     = "lookup"
	RangeReq                      = "range"       // Range with no filtering
	FilterRangeReq                = "filterRange" // Range with filtering
)

// Range for a single field in composite index
type CompositeElementFilter struct {
	Low       IndexKey
	High      IndexKey
	Inclusion Inclusion
}

func (c CompositeElementFilter) String() string {
	return fmt.Sprintf("(Low: %s High: %s Inclusion: %v)", c.Low.Bytes(), c.High.Bytes(), c.Inclusion)
}

// A point in index and the corresponding filter
// the point belongs to either as high or low
type IndexPoint struct {
	Value    IndexKey
	FilterId int
	Type     string
}

// Implements sort Interface
type IndexPoints []IndexPoint

// Implements sort Interface
type Filters []Filter

//Groupby/Aggregate pushdown

type GroupKey struct {
	EntryKeyId int32                 // Id that can be used in IndexProjection
	KeyPos     int32                 // >=0 means use expr at index key position otherwise use Expr
	Expr       expression.Expression // group expression
	ExprValue  value.Value           // Is non-nil if expression is constant
}

type Aggregate struct {
	AggrFunc   common.AggrFuncType   // Aggregate operation
	EntryKeyId int32                 // Id that can be used in IndexProjection
	KeyPos     int32                 // >=0 means use expr at index key position otherwise use Expr
	Expr       expression.Expression // Aggregate expression
	ExprValue  value.Value           // Is non-nil if expression is constant
	Distinct   bool                  // Aggregate only on Distinct values with in the group
}

type GroupAggr struct {
	Name                string       // name of the index aggregate
	Group               []*GroupKey  // group keys, nil means no group by
	Aggrs               []*Aggregate // aggregates with in the group, nil means no aggregates
	DependsOnIndexKeys  []int32      // GROUP and Aggregates Depends on List of index keys positions
	IndexKeyNames       []string     // Index key names used in expressions
	DependsOnPrimaryKey bool
	AllowPartialAggr    bool // Partial aggregates are allowed
	OnePerPrimaryKey    bool // Leading Key is ALL & equality span consider one per docid

	IsLeadingGroup     bool // Group by key(s) are leading subset
	IsPrimary          bool
	NeedDecode         bool // Need decode values for SUM or N1QLExpr evaluation
	NeedExplode        bool // If only constant expression
	HasExpr            bool // Has a non constant expression
	FirstValidAggrOnly bool // Scan storage entries upto first valid value - MB-27861

	//For caching values
	cv          *value.ScopeValue
	av          value.AnnotatedValue
	exprContext expression.Context
	aggrs       []*aggrVal
	groups      []*groupKey
}

func (ga GroupAggr) String() string {
	str := "Groups: "
	for _, g := range ga.Group {
		str += fmt.Sprintf(" %+v ", g)
	}

	str += "Aggregates: "
	for _, a := range ga.Aggrs {
		str += fmt.Sprintf(" %+v ", a)
	}

	str += fmt.Sprintf(" DependsOnIndexKeys %v", ga.DependsOnIndexKeys)
	str += fmt.Sprintf(" IndexKeyNames %v", ga.IndexKeyNames)
	str += fmt.Sprintf(" NeedDecode %v", ga.NeedDecode)
	str += fmt.Sprintf(" NeedExplode %v", ga.NeedExplode)
	str += fmt.Sprintf(" IsLeadingGroup %v", ga.IsLeadingGroup)
	return str
}

func (g GroupKey) String() string {
	str := "Group: "
	str += fmt.Sprintf(" EntryKeyId %v", g.EntryKeyId)
	str += fmt.Sprintf(" KeyPos %v", g.KeyPos)
	str += fmt.Sprintf(" Expr %v", logging.TagUD(g.Expr))
	str += fmt.Sprintf(" ExprValue %v", logging.TagUD(g.ExprValue))
	return str
}

func (a Aggregate) String() string {
	str := "Aggregate: "
	str += fmt.Sprintf(" AggrFunc %v", a.AggrFunc)
	str += fmt.Sprintf(" EntryKeyId %v", a.EntryKeyId)
	str += fmt.Sprintf(" KeyPos %v", a.KeyPos)
	str += fmt.Sprintf(" Expr %v", logging.TagUD(a.Expr))
	str += fmt.Sprintf(" ExprValue %v", logging.TagUD(a.ExprValue))
	str += fmt.Sprintf(" Distinct %v", a.Distinct)
	return str
}

var (
	ErrInvalidAggrFunc = errors.New("Invalid Aggregate Function")
)

var inclusionMatrix = [][]Inclusion{
	{Neither, High},
	{Low, Both},
}

/////////////////////////////////////////////////////////////////////////
//
//  scan request implementation
//
/////////////////////////////////////////////////////////////////////////

func NewScanRequest(protoReq interface{}, ctx interface{},
	cancelCh <-chan bool, s *scanCoordinator) (r *ScanRequest, err error) {

	r = new(ScanRequest)
	r.ScanId = atomic.AddUint64(&s.reqCounter, 1)
	r.LogPrefix = fmt.Sprintf("SCAN##%d", r.ScanId)
	r.sco = s

	defer func() {
		if rv := recover(); rv != nil {
			logging.Errorf("%v Panic while creating new scan request - %v", r.LogPrefix, r)
			logging.Errorf("%v %v\n%s", r.LogPrefix, rv, logging.StackTrace())
			err = fmt.Errorf("Internal error while creating new scan request")
		}
	}()

	cfg := s.config.Load()
	timeout := time.Millisecond * time.Duration(cfg["settings.scan_timeout"].Int())
	r.CancelCh = cancelCh

	r.projectPrimaryKey = true

	if ctx == nil {
		r.connCtx = createConnectionContext().(*ConnectionContext)
	} else {
		r.connCtx = ctx.(*ConnectionContext)
	}

	r.keySzCfg = getKeySizeConfig(cfg)

	r.parallelCentroidScans = cfg["scan.vector.parallel_centroid_scans"].Int()

	switch req := protoReq.(type) {
	case *protobuf.HeloRequest:
		setTimeoutTimer(timeout, r)
		r.ScanType = HeloReq
	case *protobuf.StatisticsRequest:
		setTimeoutTimer(timeout, r)
		r.DefnID = req.GetDefnID()
		r.RequestId = req.GetRequestId()
		r.ScanType = StatsReq
		r.Incl = Inclusion(req.GetSpan().GetRange().GetInclusion())
		r.Sorted = true
		if err = r.setIndexParams(); err != nil {
			return
		}

		err = r.fillRanges(
			req.GetSpan().GetRange().GetLow(),
			req.GetSpan().GetRange().GetHigh(),
			req.GetSpan().GetEquals())
		if err != nil {
			return
		}

	case *protobuf.CountRequest:
		if req.GetReqTimeout() != 0 {
			timeout = time.Millisecond * time.Duration(req.GetReqTimeout())
		}
		setTimeoutTimer(timeout, r)

		r.DefnID = req.GetDefnID()
		r.RequestId = req.GetRequestId()
		r.User = req.GetUser()
		r.rollbackTime = req.GetRollbackTime()
		r.PartitionIds = makePartitionIds(req.GetPartitionIds())
		cons := common.Consistency(req.GetCons())
		tsvector := req.GetTsVector()
		r.ScanType = CountReq
		r.Incl = Inclusion(req.GetSpan().GetRange().GetInclusion())
		r.Sorted = true
		r.SkipReadMetering = req.GetSkipReadMetering()

		if err = r.setIndexParams(); err != nil {
			return
		}

		if err = r.setConsistency(cons, tsvector); err != nil {
			return
		}

		err = r.fillRanges(
			req.GetSpan().GetRange().GetLow(),
			req.GetSpan().GetRange().GetHigh(),
			req.GetSpan().GetEquals())
		if err != nil {
			return
		}

		sc := req.GetScans()
		if len(sc) != 0 {
			r.Scans, err = r.makeScans(sc)
			r.ScanType = MultiScanCountReq
			r.Distinct = req.GetDistinct()
		}
		if err != nil {
			return
		}

	case *protobuf.ScanRequest:
		if req.GetReqTimeout() != 0 {
			timeout = time.Millisecond * time.Duration(req.GetReqTimeout())
		}
		setTimeoutTimer(timeout, r)

		r.isVectorScan = (req.GetIndexVector() != nil)
		if r.isVectorScan {
			ivec := req.GetIndexVector()
			r.setVectorIndexParams(ivec)
		}

		r.DefnID = req.GetDefnID()
		r.RequestId = req.GetRequestId()
		r.User = req.GetUser()
		r.rollbackTime = req.GetRollbackTime()
		r.PartitionIds = makePartitionIds(req.GetPartitionIds())
		cons := common.Consistency(req.GetCons())
		tsvector := req.GetTsVector()
		r.ScanType = ScanReq
		r.Incl = Inclusion(req.GetSpan().GetRange().GetInclusion())
		r.Limit = req.GetLimit()
		r.Sorted = req.GetSorted()
		r.Reverse = req.GetReverse()
		proj := req.GetIndexprojection()
		r.dataEncFmt = common.DataEncodingFormat(req.GetDataEncFmt())
		if proj == nil {
			r.Distinct = req.GetDistinct()
		}
		r.Offset = req.GetOffset()
		r.SkipReadMetering = req.GetSkipReadMetering()
		r.indexKeyNames = req.GetIndexKeyNames()
		r.inlineFilter = req.GetInlineFilter()

		if err = r.setIndexParams(); err != nil {
			return
		}

		if err = r.setConsistency(cons, tsvector); err != nil {
			return
		}

		if err = r.validateIncludeColumns(); err != nil {
			return
		}

		if proj != nil {
			var localerr error
			if req.GetGroupAggr() == nil {
				cklen := len(r.IndexInst.Defn.SecExprs)
				includelen := len(r.IndexInst.Defn.Include)
				if r.Indexprojection, localerr = validateIndexProjection(proj, cklen, includelen); localerr != nil {
					err = localerr
					return
				}
				r.projectPrimaryKey = *proj.PrimaryKey
			} else {
				if r.Indexprojection, localerr = validateIndexProjectionGroupAggr(proj, req.GetGroupAggr()); localerr != nil {
					err = localerr
					return
				}
				r.projectPrimaryKey = false
			}
		}
		err = r.fillRanges(
			req.GetSpan().GetRange().GetLow(),
			req.GetSpan().GetRange().GetHigh(),
			req.GetSpan().GetEquals())
		if err != nil {
			return
		}

		if r.isVectorScan {
			r.ScanType = VectorScanReq
			r.protoScans = req.GetScans() // Save ref to protoScans to generate vector scans later
			if err = r.setVectorIndexParamsFromDefn(); err != nil {
				return
			}

			r.projectVectorDist = r.ProjectVectorDist()
			r.setRerankLimits()
			protoIndexOrder := req.GetIndexOrder()
			if r.indexOrder, err = validateIndexOrder(protoIndexOrder,
				r.IndexInst.Defn.Desc, r.vectorPos); err != nil {
				return
			}
		} else {
			if r.Scans, err = r.makeScans(req.GetScans()); err != nil {
				return
			}
		}

		if err = r.fillGroupAggr(req.GetGroupAggr(), req.GetScans()); err != nil {
			return
		}
		r.setExplodePositions()

	case *protobuf.ScanAllRequest:
		if req.GetReqTimeout() != 0 {
			timeout = time.Millisecond * time.Duration(req.GetReqTimeout())
		}
		setTimeoutTimer(timeout, r)

		r.isVectorScan = (req.GetIndexVector() != nil)
		if r.isVectorScan {
			ivec := req.GetIndexVector()
			r.setVectorIndexParams(ivec)
		}

		r.DefnID = req.GetDefnID()
		r.RequestId = req.GetRequestId()
		r.User = req.GetUser()
		r.rollbackTime = req.GetRollbackTime()
		r.PartitionIds = makePartitionIds(req.GetPartitionIds())
		cons := common.Consistency(req.GetCons())
		tsvector := req.GetTsVector()
		r.ScanType = ScanAllReq
		r.Limit = req.GetLimit()
		r.Scans = make([]Scan, 1)
		r.Scans[0].ScanType = AllReq
		r.Sorted = true
		r.dataEncFmt = common.DataEncodingFormat(req.GetDataEncFmt())
		r.SkipReadMetering = req.GetSkipReadMetering()

		if err = r.setIndexParams(); err != nil {
			return
		}

		if err = r.setConsistency(cons, tsvector); err != nil {
			return
		}

		// VECTOR_TODO: Check if this is needed
		if r.isVectorScan {
			r.ScanType = VectorScanAllReq
			if err = r.setVectorIndexParamsFromDefn(); err != nil {
				return
			}
		}
	default:
		err = ErrUnsupportedRequest
	}

	return
}

// setVectorIndexParams sets the params in ScanRequest that are derived from protobuf.IndexVector
// these values are passed from query client and this function must be called before findIndexInstance
// and nprobes is used to fetch index reader contexts
func (r *ScanRequest) setVectorIndexParams(ivec *protobuf.IndexVector) {
	// Populate query vector in req
	qvec := ivec.GetQueryVector()
	r.queryVector = append(r.queryVector, qvec...)

	// Set Scan type to VectorScanReq so that we can process vector req differently
	// If r.nprobes is 0 fallback to value from index creation time after getting the definition
	// Currently set to value from query and can be 0 its reset in setVectorIndexParamsFromDefn
	r.nprobes = int(ivec.GetProbes())
}

// setVectorIndexParamsFromDefn will set vectorPos in ScanRequest and should be called after getting indexn instance
// as this uses IndexInst in ScanRequest. Call this function after findIndexInstance
func (r *ScanRequest) setVectorIndexParamsFromDefn() (err error) {
	// Set vector position
	r.vectorPos = r.IndexInst.Defn.GetVectorKeyPosExploded()
	if r.vectorPos < 0 {
		return ErrNotVectorIndex
	}

	// Validate nprobes and r.nprobes is 0 fallback to value from index creation time
	scanTimeNProbes := r.nprobes
	if scanTimeNProbes == 0 {
		r.nprobes = r.IndexInst.Defn.VectorMeta.Nprobes
	}
	if r.nprobes == 0 {
		return fmt.Errorf("nprobes value is zero. scan_probes: %v from index creation"+
			" probes: %v from index scan", r.IndexInst.Defn.VectorMeta.Nprobes, scanTimeNProbes)
	}

	return nil
}

func (r *ScanRequest) setRerankLimits() {
	// Re-ranking is supported only for BHIVE indexes
	if !r.isBhiveScan {
		r.enableReranking = false
		return
	}

	// Disable re-ranking if limit is not specified or all the
	// rows of index are getting scanned
	if r.Limit == 0 || r.Limit == math.MaxInt64 {
		r.enableReranking = false
		return
	}

	cfg := r.sco.config.Load()
	rfactor := cfg["scan.vector.rerank_factor"].Int()
	if rfactor <= 1 {
		r.enableReranking = false
		return
	}

	// For all other cases, enable re-ranking for BHIVE indexes
	r.enableReranking = true
	r.rlimit = r.Limit * int64(rfactor)
}

func (r *ScanRequest) getLimit() int64 {
	if r.enableReranking {
		return r.rlimit
	}
	return r.Limit
}

func (r *ScanRequest) ProjectVectorDist() bool {
	// If index projection is not pushed down
	if r.Indexprojection == nil {
		logging.Verbosef("%v ProjectVectorDist projecting vector distance as projection is not pushed down", r.LogPrefix)
		return true
	}

	// If we are projecting all keys or entry keys projector vector distance in place of vector field
	if !r.Indexprojection.projectSecKeys {
		logging.Verbosef("%v ProjectVectorDist projecting vector distance as all keys are being projected", r.LogPrefix)
		return true
	}

	// If vectorKeyPos is in the list of keys being project project vector distance
	if r.Indexprojection.projectionKeys[r.vectorPos] {
		logging.Verbosef("%v ProjectVectorDist projecting vector distance as vector keys is being projected", r.LogPrefix)
		return true
	}

	return false
}

func (r *ScanRequest) useHeapForVectorIndex() bool {
	return r.Limit != 0 && r.Limit != math.MaxInt64
}

func (r *ScanRequest) getNearestCentroids() error {
	r.centroidMap = make(map[common.PartitionId][]int64)

	for pid, cb := range r.codebookMap {
		t0 := time.Now()
		centroids, err := cb.FindNearestCentroids(r.queryVector, int64(r.nprobes))
		if err != nil {
			return err
		}
		if r.Stats != nil {
			r.Stats.Timings.vtAssign.Put(time.Now().Sub(t0))
		}
		r.centroidMap[pid] = centroids
	}
	return nil
}

// fillVectorScans must be called after getNearestCentroids as this function uses centroidIDs
func (r *ScanRequest) fillVectorScans() (localErr error) {

	if r.isBhiveScan {
		scansForPartns := make(map[common.PartitionId]map[int64][]Scan)
		for partnId, centroidIdList := range r.centroidMap {
			scansForCentroids := make(map[int64][]Scan)
			for _, cid := range centroidIdList {
				bcid := NewBhiveCentroidId(uint64(cid))

				qv := bhiveCentroidId(Float32ToByteSlice(r.queryVector))
				scan := Scan{Low: bcid, High: qv, Incl: Both, ScanType: LookupReq}
				scansForCentroids[cid] = append(scansForCentroids[cid], scan)
			}
			scansForPartns[partnId] = scansForCentroids
		}
		r.vectorScans = scansForPartns
		return nil
	}

	// Scans for composite vector index will be processed here
	substituteCentroidID := func(centroidId int64) error {
		for _, scan := range r.protoScans {
			for filterPos, compFilter := range scan.Filters {
				if filterPos == r.vectorPos {
					centroidStr := common.GetCentroidIdStr(centroidId)
					centroidIdBytes, err := json.Marshal(centroidStr)
					if err != nil {
						return err
					}

					compFilter.Low = centroidIdBytes
					compFilter.High = centroidIdBytes
					compFilter.Inclusion = proto.Uint32(uint32(Both))
				}
			}
		}
		return nil
	}

	scansForPartns := make(map[common.PartitionId]map[int64][]Scan)
	for partnId, centroidIdList := range r.centroidMap {
		scansForCentroids := make(map[int64][]Scan)
		for _, cid := range centroidIdList {
			localErr = substituteCentroidID(cid)
			if localErr != nil {
				return localErr
			}
			logging.Verbosef("Susbstituted ProtoScans: %v", r.protoScans)
			scansForCentroids[cid], localErr = r.makeScans(r.protoScans)
			if localErr != nil {
				return localErr
			}
		}
		scansForPartns[partnId] = scansForCentroids
	}
	r.vectorScans = scansForPartns
	return nil
}

func (r *ScanRequest) getTimeoutCh() <-chan time.Time {
	if r.Timeout != nil {
		return r.Timeout.C
	}

	return nil
}

func (r *ScanRequest) Done() {
	// If the requested DefnID in invalid, stats object will not be populated
	if r.Stats != nil {
		r.Stats.numCompletedRequests.Add(1)
		if r.GroupAggr != nil {
			r.Stats.numCompletedRequestsAggr.Add(1)
		} else {
			r.Stats.numCompletedRequestsRange.Add(1)
		}

		for _, partitionId := range r.PartitionIds {
			r.Stats.updatePartitionStats(partitionId,
				func(stats *IndexStats) {
					stats.numCompletedRequests.Add(1)
				})
		}
	}

	for _, buf := range r.keyBufList {
		secKeyBufPool.Put(buf)
	}

	r.keyBufList = nil

	if r.Timeout != nil {
		r.Timeout.Stop()
	}
}

func (r *ScanRequest) isNil(k []byte) bool {
	if k == nil || (!r.isPrimary && string(k) == "[]") {
		return true
	}
	return false
}

func (r *ScanRequest) newKey(k []byte) (IndexKey, error) {
	if k == nil {
		return nil, fmt.Errorf("Key is null")
	}

	if r.isPrimary {
		return NewPrimaryKey(k)
	} else {
		return NewSecondaryKey(k, r.getKeyBuffer(3*len(k)), r.keySzCfg.allowLargeKeys, r.keySzCfg.maxSecKeyLen)
	}
}

func (r *ScanRequest) newLowKey(k []byte) (IndexKey, error) {
	if r.isNil(k) {
		return MinIndexKey, nil
	}

	return r.newKey(k)
}

func (r *ScanRequest) newHighKey(k []byte) (IndexKey, error) {
	if r.isNil(k) {
		return MaxIndexKey, nil
	}

	return r.newKey(k)
}

func (r *ScanRequest) fillRanges(low, high []byte, keys [][]byte) (localErr error) {
	var key IndexKey

	// range
	r.LowBytes = low
	r.HighBytes = high

	if r.Low, localErr = r.newLowKey(low); localErr != nil {
		localErr = fmt.Errorf("Invalid low key %s (%s)", string(low), localErr)
		return
	}

	if r.High, localErr = r.newHighKey(high); localErr != nil {
		localErr = fmt.Errorf("Invalid high key %s (%s)", string(high), localErr)
		return
	}

	// point query for keys
	for _, k := range keys {
		r.KeysBytes = append(r.KeysBytes, k)
		if key, localErr = r.newKey(k); localErr != nil {
			localErr = fmt.Errorf("Invalid equal key %s (%s)", string(k), localErr)
			return
		}
		r.Keys = append(r.Keys, key)
	}
	return
}

func (r *ScanRequest) joinKeys(keys [][]byte) ([]byte, error) {
	buf1 := r.getSharedBuffer(len(keys) * 3)
	joined, e := jsonEncoder.JoinArray(keys, buf1)
	if e != nil {
		e = fmt.Errorf("Error in joining keys: %s", e)
		return nil, e
	}
	r.sharedBufferLen += len(joined)
	return joined, nil
}

func (r *ScanRequest) areFiltersNil(protoScan *protobuf.Scan) bool {
	areFiltersNil := true
	for _, filter := range protoScan.Filters {
		if !r.isNil(filter.Low) || !r.isNil(filter.High) {
			areFiltersNil = false
			break
		}
	}
	return areFiltersNil
}

func (r *ScanRequest) getEmptyScan() Scan {
	key, _ := r.newKey([]byte(""))
	return Scan{Low: key, High: key, Incl: Neither, ScanType: RangeReq}
}

// Compute the overall low, high for a Filter
// based on composite filter ranges
func (r *ScanRequest) fillFilterLowHigh(compFilters []CompositeElementFilter, filter *Filter) error {
	if !r.IndexInst.Defn.HasDescending() {
		var lows, highs [][]byte
		var e error
		joinLowKey, joinHighKey := true, true

		if compFilters[0].Low == MinIndexKey {
			filter.Low = MinIndexKey
			joinLowKey = false
		}
		if compFilters[0].High == MaxIndexKey {
			filter.High = MaxIndexKey
			joinHighKey = false
		}

		var l, h []byte
		codec := collatejson.NewCodec(16)
		if joinLowKey {
			for _, f := range compFilters {
				if f.Low == MinIndexKey {
					break
				}
				lows = append(lows, f.Low.Bytes())
			}

			buf1 := r.getSharedBuffer(len(lows) * 3)
			if l, e = codec.JoinArray(lows, buf1); e != nil {
				e = fmt.Errorf("Error in forming low key %s", e)
				return e
			}
			r.sharedBufferLen += len(l)
			lowKey := secondaryKey(l)
			filter.Low = &lowKey
		}
		if joinHighKey {
			for _, f := range compFilters {
				if f.High == MaxIndexKey {
					break
				}
				highs = append(highs, f.High.Bytes())
			}

			buf2 := r.getSharedBuffer(len(highs) * 3)
			if h, e = codec.JoinArray(highs, buf2); e != nil {
				e = fmt.Errorf("Error in forming high key %s", e)
				return e
			}
			r.sharedBufferLen += len(h)
			highKey := secondaryKey(h)
			filter.High = &highKey
		}
		return nil
	}

	//********** Reverse Collation fix **********//

	// Step 1: Form lows and highs keys
	var lows, highs []IndexKey
	for _, f := range compFilters {
		lows = append(lows, f.Low)
		highs = append(highs, f.High)
	}

	// Step 2: Exchange lows and highs depending on []desc
	var lows2, highs2 []IndexKey
	for i, _ := range compFilters {
		if r.IndexInst.Defn.Desc[i] {
			lows2 = append(lows2, highs[i])
			highs2 = append(highs2, lows[i])
		} else {
			lows2 = append(lows2, lows[i])
			highs2 = append(highs2, highs[i])
		}
	}

	// Step 3: Prune lows2 and highs2 if Min/Max present
	for i, l := range lows2 {
		if l == MinIndexKey || l == MaxIndexKey {
			lows2 = lows2[:i]
			break
		}
	}
	for i, h := range highs2 {
		if h == MinIndexKey || h == MaxIndexKey {
			highs2 = highs2[:i]
			break
		}
	}

	// Step 4: Join lows2 and highs2
	var joinedLow, joinedHigh []byte
	var e error
	var lowKey, highKey IndexKey
	if len(lows2) > 0 {
		var lows2bytes [][]byte
		for _, l := range lows2 {
			lows2bytes = append(lows2bytes, l.Bytes())
		}
		if joinedLow, e = r.joinKeys(lows2bytes); e != nil {
			return e
		}
		lowKey, e = getReverseCollatedIndexKey(joinedLow, r.IndexInst.Defn.Desc[:len(lows2)])
		if e != nil {
			return e
		}
	} else {
		lowKey = MinIndexKey
	}

	if len(highs2) > 0 {
		var highs2bytes [][]byte
		for _, l := range highs2 {
			highs2bytes = append(highs2bytes, l.Bytes())
		}
		if joinedHigh, e = r.joinKeys(highs2bytes); e != nil {
			return e
		}
		highKey, e = getReverseCollatedIndexKey(joinedHigh, r.IndexInst.Defn.Desc[:len(highs2)])
		if e != nil {
			return e
		}
	} else {
		highKey = MaxIndexKey
	}
	filter.Low = lowKey
	filter.High = highKey
	//********** End of Reverse Collation fix **********//

	// TODO: Calculate the right inclusion
	// Right now using Both inclusion
	return nil
}

func (r *ScanRequest) fillFilterEquals(protoScan *protobuf.Scan, filter *Filter) error {
	var e error
	var equals [][]byte
	for _, k := range protoScan.Equals {
		var key IndexKey
		if key, e = r.newKey(k); e != nil {
			e = fmt.Errorf("Invalid equal key %s (%s)", string(k), e)
			return e
		}
		equals = append(equals, key.Bytes())
	}

	codec := collatejson.NewCodec(16)
	buf1 := r.getSharedBuffer(len(equals) * 3)
	var equalsKey, eqReverse []byte
	if equalsKey, e = codec.JoinArray(equals, buf1); e != nil {
		e = fmt.Errorf("Error in forming equals key %s", e)
		return e
	}
	r.sharedBufferLen += len(equalsKey)
	if !r.IndexInst.Defn.HasDescending() {
		eqReverse = equalsKey
	} else {
		eqReverse, e = jsonEncoder.ReverseCollate(equalsKey, r.IndexInst.Defn.Desc[:len(equals)])
		if e != nil {
			return e
		}
	}
	eqKey := secondaryKey(eqReverse)

	var compFilters []CompositeElementFilter
	for _, k := range equals {
		ek := secondaryKey(k)
		fl := CompositeElementFilter{
			Low:       &ek,
			High:      &ek,
			Inclusion: Both,
		}
		compFilters = append(compFilters, fl)
	}

	filter.Low = &eqKey
	filter.High = &eqKey
	filter.Inclusion = Both
	filter.CompositeFilters = compFilters
	filter.ScanType = LookupReq
	return nil
}

// /// Compose Scans for Secondary Index
// Create scans from sorted Index Points
// Iterate over sorted points and keep track of applicable filters
// between overlapped regions
func (r *ScanRequest) composeScans(points []IndexPoint, filters []Filter) []Scan {

	var scans []Scan
	filtersMap := make(map[int]bool)
	var filtersList []int
	var low IndexKey
	for _, p := range points {
		if len(filtersMap) == 0 {
			low = p.Value
		}
		filterid := p.FilterId
		if _, present := filtersMap[filterid]; present {
			delete(filtersMap, filterid)
			if len(filtersMap) == 0 { // Empty filtersMap indicates end of overlapping region
				if len(scans) > 0 &&
					scans[len(scans)-1].High.ComparePrefixIndexKey(low) == 0 {
					// If high of previous scan == low of next scan, then
					// merge the filters instead of creating a new scan
					for _, fl := range filtersList {
						scans[len(scans)-1].Filters = append(scans[len(scans)-1].Filters, filters[fl])
					}
					scans[len(scans)-1].High = p.Value
					filtersList = nil
				} else {
					scan := Scan{
						Low:      low,
						High:     p.Value,
						Incl:     Both,
						ScanType: FilterRangeReq,
					}
					for _, fl := range filtersList {
						scan.Filters = append(scan.Filters, filters[fl])
					}

					scans = append(scans, scan)
					filtersList = nil
				}
			}
		} else {
			filtersMap[filterid] = true
			filtersList = append(filtersList, filterid)
		}
	}
	for i, _ := range scans {
		if len(scans[i].Filters) == 1 && scans[i].Filters[0].ScanType == LookupReq {
			scans[i].Equals = scans[i].Low
			scans[i].ScanType = LookupReq
		}

		if scans[i].ScanType == FilterRangeReq && len(scans[i].Filters) == 1 &&
			len(scans[i].Filters[0].CompositeFilters) == 1 {
			// Flip inclusion if first element is descending
			scans[i].Incl = flipInclusion(scans[i].Filters[0].CompositeFilters[0].Inclusion, r.IndexInst.Defn.Desc)
			scans[i].ScanType = RangeReq
		}
		// TODO: Optimzation if single CEF in all filters (for both primary and secondary)
	}

	return scans
}

// /// Compose Scans for Primary Index
func lowInclude(lowInclusions []Inclusion) int {
	for _, incl := range lowInclusions {
		if incl == Low || incl == Both {
			return 1
		}
	}
	return 0
}

func highInclude(highInclusions []Inclusion) int {
	for _, incl := range highInclusions {
		if incl == High || incl == Both {
			return 1
		}
	}
	return 0
}

func MergeFiltersForPrimary(scans []Scan, f2 Filter) []Scan {

	getNewScans := func(scans []Scan, f Filter) []Scan {
		sc := Scan{Low: f.Low, High: f.High, Incl: f.Inclusion, ScanType: RangeReq}
		scans = append(scans, sc)
		return scans
	}

	if len(scans) > 0 {
		f1 := scans[len(scans)-1]
		l1, h1, i1 := f1.Low, f1.High, f1.Incl
		l2, h2, i2 := f2.Low, f2.High, f2.Inclusion

		//No Merge casess
		if l2.ComparePrefixIndexKey(h1) > 0 {
			return getNewScans(scans, f2)
		}
		if (h1.ComparePrefixIndexKey(l2) == 0) &&
			!(i1 == High || i1 == Both || i2 == Low || i2 == Both) {
			return getNewScans(scans, f2)
		}

		// Merge cases
		var low, high IndexKey
		inclLow, inclHigh := 0, 0
		if l1.ComparePrefixIndexKey(l2) == 0 {
			low = l1
			inclLow = lowInclude([]Inclusion{i1, i2})
		}
		if h1.ComparePrefixIndexKey(h2) == 0 {
			high = h1
			inclHigh = highInclude([]Inclusion{i1, i2})
		}
		if low == nil {
			if l1.ComparePrefixIndexKey(l2) < 0 {
				low = l1
				inclLow = lowInclude([]Inclusion{i1})
			} else {
				low = l2
				inclLow = lowInclude([]Inclusion{i2})
			}
		}
		if high == nil {
			if h1.ComparePrefixIndexKey(h2) > 0 {
				high = h1
				inclHigh = highInclude([]Inclusion{i1})
			} else {
				high = h2
				inclHigh = highInclude([]Inclusion{i2})
			}
		}
		f1.Low, f1.High = low, high
		f1.Incl = inclusionMatrix[inclLow][inclHigh]
		scans[len(scans)-1] = f1
		return scans
	}
	return getNewScans(scans, f2)
}

///// END - Compose Scans for Primary Index

func (r *ScanRequest) makeScans(protoScans []*protobuf.Scan) (s []Scan, localErr error) {
	var l, h IndexKey

	// For Upgrade
	if len(protoScans) == 0 {
		scans := make([]Scan, 1)
		if len(r.Keys) > 0 {
			scans[0].Equals = r.Keys[0] //TODO fix for multiple Keys needed?
			scans[0].ScanType = LookupReq
		} else {
			scans[0].Low = r.Low
			scans[0].High = r.High
			scans[0].Incl = r.Incl
			scans[0].ScanType = RangeReq
		}
		return scans, nil
	}

	// Array of Filters
	var filters []Filter
	var points []IndexPoint

	if r.isPrimary {
		var scans []Scan
		for _, protoScan := range protoScans {
			if len(protoScan.Equals) != 0 {
				var filter Filter
				var key IndexKey
				if key, localErr = r.newKey(protoScan.Equals[0]); localErr != nil {
					localErr = fmt.Errorf("Invalid equal key %s (%s)", string(protoScan.Equals[0]), localErr)
					return nil, localErr
				}
				filter.Low = key
				filter.High = key
				filter.Inclusion = Both
				filters = append(filters, filter)

				p1 := IndexPoint{Value: filter.Low, FilterId: len(filters) - 1, Type: "low"}
				p2 := IndexPoint{Value: filter.High, FilterId: len(filters) - 1, Type: "high"}
				points = append(points, p1, p2)
				continue
			}

			// If there are no filters in scan, it is ScanAll
			if len(protoScan.Filters) == 0 {
				scans := make([]Scan, 1)
				scans[0] = getScanAll()
				return scans, nil
			}

			// if all scan filters are (nil, nil), it is ScanAll
			if r.areFiltersNil(protoScan) {
				scans := make([]Scan, 1)
				scans[0] = getScanAll()
				return scans, nil
			}

			fl := protoScan.Filters[0]
			if l, localErr = r.newLowKey(fl.Low); localErr != nil {
				localErr = fmt.Errorf("Invalid low key %s (%s)", logging.TagStrUD(fl.Low), localErr)
				return nil, localErr
			}

			if h, localErr = r.newHighKey(fl.High); localErr != nil {
				localErr = fmt.Errorf("Invalid high key %s (%s)", logging.TagStrUD(fl.High), localErr)
				return nil, localErr
			}

			if IndexKeyLessThan(h, l) {
				scans = append(scans, r.getEmptyScan())
				continue
			}

			// When l == h, only valid case is: meta().id >= l && meta().id <= h
			if l.CompareIndexKey(h) == 0 && Inclusion(fl.GetInclusion()) != Both {
				scans = append(scans, r.getEmptyScan())
				continue
			}

			compfil := CompositeElementFilter{
				Low:       l,
				High:      h,
				Inclusion: Inclusion(fl.GetInclusion()),
			}

			filter := Filter{
				CompositeFilters: []CompositeElementFilter{compfil},
				Inclusion:        compfil.Inclusion,
				Low:              l,
				High:             h,
			}
			filters = append(filters, filter)
		}
		// Sort Filters based only on low value
		sort.Sort(Filters(filters))
		for _, filter := range filters {
			scans = MergeFiltersForPrimary(scans, filter)
		}

		return scans, nil
	} else {
		for _, protoScan := range protoScans {
			skipScan := false
			if len(protoScan.Equals) != 0 {
				//Encode the equals keys
				var filter Filter
				if localErr = r.fillFilterEquals(protoScan, &filter); localErr != nil {
					return nil, localErr
				}
				filters = append(filters, filter)

				p1 := IndexPoint{Value: filter.Low, FilterId: len(filters) - 1, Type: "low"}
				p2 := IndexPoint{Value: filter.High, FilterId: len(filters) - 1, Type: "high"}
				points = append(points, p1, p2)
				continue
			}

			// If there are no filters in scan, it is ScanAll
			if len(protoScan.Filters) == 0 {
				scans := make([]Scan, 1)
				scans[0] = getScanAll()
				return scans, nil
			}

			// if all scan filters are (nil, nil), it is ScanAll
			if r.areFiltersNil(protoScan) {
				scans := make([]Scan, 1)
				scans[0] = getScanAll()
				return scans, nil
			}

			var compFilters []CompositeElementFilter
			// Encode Filters
			for _, fl := range protoScan.Filters {
				if l, localErr = r.newLowKey(fl.Low); localErr != nil {
					localErr = fmt.Errorf("Invalid low key %s (%s)", logging.TagStrUD(fl.Low), localErr)
					return nil, localErr
				}

				if h, localErr = r.newHighKey(fl.High); localErr != nil {
					localErr = fmt.Errorf("Invalid high key %s (%s)", logging.TagStrUD(fl.High), localErr)
					return nil, localErr
				}

				if IndexKeyLessThan(h, l) {
					skipScan = true
					break
				}

				compfil := CompositeElementFilter{
					Low:       l,
					High:      h,
					Inclusion: Inclusion(fl.GetInclusion()),
				}
				compFilters = append(compFilters, compfil)
			}

			if skipScan {
				continue
			}

			filter := Filter{
				CompositeFilters: compFilters,
				Inclusion:        Both,
			}

			if localErr = r.fillFilterLowHigh(compFilters, &filter); localErr != nil {
				return nil, localErr
			}

			filters = append(filters, filter)

			p1 := IndexPoint{Value: filter.Low, FilterId: len(filters) - 1, Type: "low"}
			p2 := IndexPoint{Value: filter.High, FilterId: len(filters) - 1, Type: "high"}
			points = append(points, p1, p2)

			// TODO: Does single Composite Element Filter
			// mean no filtering? Revisit single CEF
		}
	}

	// Sort Index Points
	sort.Sort(IndexPoints(points))
	scans := r.composeScans(points, filters)

	r.maxCompositeFilters = 0
	for _, sc := range scans {
		if sc.ScanType != FilterRangeReq {
			continue
		}

		for _, fl := range sc.Filters {
			num := len(fl.CompositeFilters)
			if num > r.maxCompositeFilters {
				r.maxCompositeFilters = num
			}
		}
	}

	// Adding extra logs for MB-52355
	if r.maxCompositeFilters > len(r.IndexInst.Defn.SecExprs) {
		fmsg := "%v ReqID: %v Defn: %v \n\tSecExprs: %v \n\tScans: %v \n\tMaxCompositeFilters: %v \n\tProtoScans: %v"
		var sb strings.Builder
		for i, s := range protoScans {
			sb.WriteString(fmt.Sprintf("\n\t\t protobuf scan %v %v", i, s.String()))
		}
		logging.Errorf(fmsg, r.LogPrefix, r.RequestId, r.DefnID, r.IndexInst.Defn.SecExprs,
			logging.TagUD(r.Scans), r.maxCompositeFilters, sb.String())
		return nil, fmt.Errorf("invalid length of composite element filters in scan request")
	}

	return scans, nil
}

// Populate list of positions of keys which need to be
// exploded for composite filtering and index projection
func (r *ScanRequest) setExplodePositions() {

	if r.isPrimary {
		return
	}

	if r.explodePositions == nil {
		r.explodePositions = make([]bool, len(r.IndexInst.Defn.SecExprs))
		r.decodePositions = make([]bool, len(r.IndexInst.Defn.SecExprs))
	}

	for i := 0; i < r.maxCompositeFilters; i++ {
		r.explodePositions[i] = true
	}

	if r.Indexprojection != nil && r.Indexprojection.projectSecKeys {
		for i, project := range r.Indexprojection.projectionKeys {
			if project {
				r.explodePositions[i] = true
			}
		}
	}

	// Set max position until which we need explode or decode
	for i := 0; i < len(r.explodePositions); i++ {
		if r.explodePositions[i] || r.decodePositions[i] {
			r.explodeUpto = i
		}
	}
}

func (r *ScanRequest) setConsistency(cons common.Consistency, vector *protobuf.TsConsistency) (localErr error) {

	r.Consistency = &cons
	cfg := r.sco.config.Load()
	if cons == common.QueryConsistency && vector != nil {
		bucketNameNumVBucketsMap := r.sco.bucketNameNumVBucketsMapHolder.Get()
		numVBuckets := bucketNameNumVBucketsMap[r.IndexInst.Defn.Bucket]
		r.Ts = common.NewTsVbuuid(r.Bucket, numVBuckets)
		// if vector == nil, it is similar to AnyConsistency
		for i, vbno := range vector.Vbnos {
			r.Ts.Seqnos[vbno] = vector.Seqnos[i]
			r.Ts.Vbuuids[vbno] = vector.Vbuuids[i]
		}
	} else if cons == common.SessionConsistency {
		cluster := cfg["clusterAddr"].String()
		r.Ts = &common.TsVbuuid{}
		t0 := time.Now()
		r.Ts.Seqnos, localErr = bucketSeqsWithRetry(cfg["settings.scan_getseqnos_retries"].Int(),
			r.LogPrefix, cluster, r.Bucket, r.CollectionId,
			cfg["use_bucket_seqnos"].Bool())
		if localErr == nil && r.Stats != nil {
			r.Stats.Timings.dcpSeqs.Put(time.Since(t0))
		}
		r.Ts.Crc64 = 0
		r.Ts.Bucket = r.Bucket
	}
	return
}

func computeNprobesAndCtxs(indexInst *common.IndexInst, scanTimeNProbes, parallelCentroidScans int) (int, int, error) {

	// If scanTimeNprobes is 0 fallback to value from index creation time
	nprobes := scanTimeNProbes
	if nprobes <= 0 {
		nprobes = indexInst.Defn.VectorMeta.Nprobes
	}

	if nprobes <= 0 {
		return 0, 0, fmt.Errorf("nprobes value is zero. scan_probes: %v from index creation"+
			" probes: %v from index scan", indexInst.Defn.VectorMeta.Nprobes, scanTimeNProbes)
	}

	// nprobes is a positive integer (i.e. a valid value)
	// check if nprobes is more than the centroids in index instance
	// Use min(nprobes, centroids) for scanning the index
	for partnId, centroids := range indexInst.Nlist {
		if centroids <= 0 {
			return 0, 0, fmt.Errorf("Cannot scan the index as the centroids available for index: %v partnId: %v is zero", indexInst.InstId, partnId)
		}
		nprobes = min(nprobes, centroids)
	}

	// Compute the minimum of nprobes and parallelCentroidScans as we can not scan
	// more centroids than that are required i.e. we should not scan 3 centroids
	// if only 2 centroid scans are required
	ctxsPerPartition := min(nprobes, parallelCentroidScans)

	return nprobes, ctxsPerPartition, nil
}

func (r *ScanRequest) setIndexParams() (localErr error) {
	r.sco.mu.RLock()
	defer r.sco.mu.RUnlock()

	var indexInst *common.IndexInst

	ctxsPerPartition := 1
	stats := r.sco.stats.Get()

	if r.isVectorScan {
		indexInst, _, localErr = r.sco.findIndexInstance(r.DefnID, r.PartitionIds,
			r.User, r.SkipReadMetering, 0, false)
		if localErr != nil {
			return
		}

		r.nprobes, ctxsPerPartition, localErr = computeNprobesAndCtxs(indexInst, r.nprobes, r.parallelCentroidScans)
		if localErr != nil {
			return
		}
	}
	indexInst, r.Ctxs, localErr = r.sco.findIndexInstance(r.DefnID, r.PartitionIds,
		r.User, r.SkipReadMetering, ctxsPerPartition, true)

	if localErr == nil {
		r.isPrimary = indexInst.Defn.IsPrimary
		r.IndexName, r.Bucket = indexInst.Defn.Name, indexInst.Defn.Bucket
		r.CollectionId = indexInst.Defn.CollectionId
		r.IndexInstId = indexInst.InstId
		r.IndexInst = *indexInst

		if indexInst.State != common.INDEX_STATE_ACTIVE {
			localErr = common.ErrIndexNotReady
		}
		r.Stats = stats.indexes[r.IndexInstId]
		rbMap := *r.sco.getRollbackInProgress()
		r.hasRollback = rbMap[indexInst.Defn.Bucket]

		// VECTOR_TODO: Replace this with IsBhive() method
		r.isBhiveScan = r.isVectorScan && indexInst.Defn.IsVectorIndex && indexInst.Defn.VectorMeta.IsBhive
	}
	return
}

func validateIndexOrder(protoIndexOrder *protobuf.IndexKeyOrder, indexDesc []bool,
	vectorKeyPos int) (*IndexKeyOrder, error) {
	if protoIndexOrder == nil {
		return nil, nil
	}

	// VECTOR_TODO: Add validations on keypos and desc..
	// VECTOR_TODO: Check these positions for flattened array indexes
	indexOrder := &IndexKeyOrder{
		keyPos: make([]int32, len(protoIndexOrder.KeyPos)),
		desc:   make([]bool, len(protoIndexOrder.Desc)),
	}
	copy(indexOrder.keyPos, protoIndexOrder.GetKeyPos())
	copy(indexOrder.desc, protoIndexOrder.GetDesc())

	if logging.IsEnabled(logging.Verbose) {
		defer logging.Infof("validateIndexOrder: %+v", indexOrder)
	}

	// If there is orderby on vectorkey only use vector distance in heap and sort based on desc
	if len(protoIndexOrder.KeyPos) == 1 && vectorKeyPos == int(protoIndexOrder.KeyPos[0]) {
		indexOrder.vectorDistOnly = true
		indexOrder.vectorDistDesc = protoIndexOrder.Desc[0]
		return indexOrder, nil
	} else {
		for i, keyPos := range indexOrder.keyPos {
			if keyPos == int32(vectorKeyPos) {
				indexOrder.vectorDistDesc = indexOrder.desc[i]
			}
		}
	}

	// For orderby to be in index order it should start from 0th key in index and
	// should be strictly increasing till the vector pos
	outOfIndexOrder := false
	// There should be ordering on all keys till vector key possition
	if len(indexOrder.keyPos) < vectorKeyPos+1 {
		outOfIndexOrder = true
	} else if indexOrder.keyPos[0] == 0 && indexOrder.desc[0] == indexDesc[0] {
		prevKp := int32(0)
		for i := 1; i < len(indexOrder.keyPos); i++ {
			currkp := indexOrder.keyPos[i]
			if currkp != prevKp+1 || indexOrder.desc[i] != indexDesc[currkp] {
				outOfIndexOrder = true
				break
			}
			prevKp = currkp
		}
	} else {
		outOfIndexOrder = true
	}
	indexOrder.inIndexOrder = !outOfIndexOrder
	return indexOrder, nil
}

func validateIndexProjection(projection *protobuf.IndexProjection, cklen int, includelen int) (*Projection, error) {
	if len(projection.EntryKeys) > cklen+includelen {
		e := errors.New(fmt.Sprintf("Invalid number of Entry Keys %v in IndexProjection. cklen: %v, includelen: %v",
			len(projection.EntryKeys), cklen, includelen))
		return nil, e
	}

	projectInclude := false
	projectIncludeKeys := make([]bool, includelen)
	projectionKeys := make([]bool, cklen)
	for _, position := range projection.EntryKeys {
		if position < 0 {
			e := errors.New(fmt.Sprintf("Invalid Entry Key %v in IndexProjection", position))
			return nil, e
		} else if position < int64(cklen) {
			projectionKeys[position] = true
		} else if position >= int64(cklen) && position < int64(cklen+includelen) {
			projectInclude = true
			projectIncludeKeys[position-int64(cklen)] = true
		} else if position > int64(cklen+includelen) {
			// position == int64(cklen + includelen) is when meta().id is included in projection
			// It is a valid case. Hence, only check for position > int64(cklen + includelen)
			e := errors.New(fmt.Sprintf("Invalid Entry Key %v in IndexProjection", position))
			return nil, e
		}

	}

	projectAllSecKeys := true
	for _, sp := range projectionKeys {
		if sp == false {
			projectAllSecKeys = false
		}
	}

	projectAllIncludeKeys := true
	for _, ip := range projectIncludeKeys {
		if ip == false {
			projectAllIncludeKeys = false
		}
	}

	indexProjection := &Projection{}
	indexProjection.projectSecKeys = !projectAllSecKeys
	indexProjection.projectionKeys = projectionKeys
	indexProjection.entryKeysEmpty = len(projection.EntryKeys) == 0

	indexProjection.projectInclude = projectInclude
	indexProjection.projectAllInclude = projectAllIncludeKeys
	indexProjection.projectIncludeKeys = projectIncludeKeys

	return indexProjection, nil
}

func validateIndexProjectionGroupAggr(projection *protobuf.IndexProjection, protoGroupAggr *protobuf.GroupAggr) (*Projection, error) {

	nproj := len(projection.GetEntryKeys())

	if nproj <= 0 {
		return nil, errors.New("Grouping without projection is not supported")
	}

	projGrp := make([]projGroup, nproj)
	var found bool
	for i, entryId := range projection.GetEntryKeys() {

		found = false
		for j, g := range protoGroupAggr.GetGroupKeys() {
			if entryId == int64(g.GetEntryKeyId()) {
				projGrp[i] = projGroup{pos: j, grpKey: true}
				found = true
				break
			}
		}

		if found {
			continue
		}

		for j, a := range protoGroupAggr.GetAggrs() {
			if entryId == int64(a.GetEntryKeyId()) {
				projGrp[i] = projGroup{pos: j, grpKey: false}
				found = true
				break
			}
		}

		if !found {
			return nil, errors.New(fmt.Sprintf("Projection EntryId %v not found in any Group/Aggregate %v", entryId, protoGroupAggr))
		}

	}

	indexProjection := &Projection{}
	indexProjection.projectGroupKeys = projGrp
	indexProjection.projectSecKeys = true

	return indexProjection, nil
}

// indexKeyNames and inlineFilter go together with include columns
// i.e. if len(indexKeyNames) > 0 then inlineFilter != "" is true
// if len(indexKeyNames) == 0 then inlineFilter == 0 is true
func (r *ScanRequest) validateIncludeColumns() error {
	if len(r.indexKeyNames) == 0 && len(r.inlineFilter) == 0 {
		return nil
	}

	if len(r.indexKeyNames) > 0 && len(r.inlineFilter) == 0 {
		return fmt.Errorf("indexKeyNames: %v is non-empty but inline filter: %v is empty", r.indexKeyNames, r.inlineFilter)
	}

	if len(r.indexKeyNames) == 0 && len(r.inlineFilter) > 0 {
		return fmt.Errorf("indexKeyNames: %v is empty but inline filter: %v is non-empty", r.indexKeyNames, r.inlineFilter)
	}

	inlineFilterExpr, err := compileN1QLExpression(r.inlineFilter)
	if err != nil {
		return err
	}
	r.inlineFilterExpr = inlineFilterExpr
	return nil
}

func (r *ScanRequest) fillGroupAggr(protoGroupAggr *protobuf.GroupAggr, protoScans []*protobuf.Scan) (err error) {

	if protoGroupAggr == nil {
		return nil
	}

	if r.explodePositions == nil {
		r.explodePositions = make([]bool, len(r.IndexInst.Defn.SecExprs))
		r.decodePositions = make([]bool, len(r.IndexInst.Defn.SecExprs))
	}

	r.GroupAggr = &GroupAggr{}

	if err = r.unmarshallGroupKeys(protoGroupAggr); err != nil {
		return
	}

	if err = r.unmarshallAggrs(protoGroupAggr); err != nil {
		return
	}

	if r.isPrimary {
		r.GroupAggr.IsPrimary = true
	}

	for _, d := range protoGroupAggr.GetDependsOnIndexKeys() {
		r.GroupAggr.DependsOnIndexKeys = append(r.GroupAggr.DependsOnIndexKeys, d)
		if !r.isPrimary && int(d) == len(r.IndexInst.Defn.SecExprs) {
			r.GroupAggr.DependsOnPrimaryKey = true
		}
	}

	for _, d := range protoGroupAggr.GetIndexKeyNames() {
		r.GroupAggr.IndexKeyNames = append(r.GroupAggr.IndexKeyNames, string(d))
	}

	r.GroupAggr.AllowPartialAggr = protoGroupAggr.GetAllowPartialAggr()
	r.GroupAggr.OnePerPrimaryKey = protoGroupAggr.GetOnePerPrimaryKey()

	if err = r.validateGroupAggr(); err != nil {
		return
	}

	// Look at groupAggr.DependsOnIndexKeys to figure out
	// explode and decode positions for N1QL expression dependencies
	if !r.isPrimary && r.GroupAggr.HasExpr {
		for _, depends := range r.GroupAggr.DependsOnIndexKeys {
			if int(depends) == len(r.IndexInst.Defn.SecExprs) {
				continue //Expr depends on meta().id, so ignore
			}
			r.explodePositions[depends] = true
			r.decodePositions[depends] = true
		}
	}

	cfg := r.sco.config.Load()
	if cfg["scan.enable_fast_count"].Bool() {
		if r.canUseFastCount(protoScans) {
			r.ScanType = FastCountReq
		}
	}

	return
}

func (r *ScanRequest) unmarshallGroupKeys(protoGroupAggr *protobuf.GroupAggr) error {

	for _, g := range protoGroupAggr.GetGroupKeys() {

		var groupKey GroupKey

		groupKey.EntryKeyId = g.GetEntryKeyId()
		groupKey.KeyPos = g.GetKeyPos()

		if groupKey.KeyPos < 0 {
			if string(g.GetExpr()) == "" {
				return errors.New("Group expression is empty")
			}
			expr, err := compileN1QLExpression(string(g.GetExpr()))
			if err != nil {
				return err
			}
			groupKey.Expr = expr
			groupKey.ExprValue = expr.Value() // value will be nil if it is not constant expr
			if groupKey.ExprValue == nil {
				r.GroupAggr.HasExpr = true
				r.GroupAggr.NeedDecode = true
				r.GroupAggr.NeedExplode = true
			}
			if r.GroupAggr.cv == nil {
				r.GroupAggr.cv = value.NewScopeValue(make(map[string]interface{}), nil)
				r.GroupAggr.av = value.NewAnnotatedValue(r.GroupAggr.cv)
				r.GroupAggr.exprContext = expression.NewIndexContext()
			}
		} else {
			r.GroupAggr.NeedExplode = true
			if !r.isPrimary {
				r.explodePositions[groupKey.KeyPos] = true
			}
		}

		r.GroupAggr.Group = append(r.GroupAggr.Group, &groupKey)
	}

	return nil

}

func (r *ScanRequest) unmarshallAggrs(protoGroupAggr *protobuf.GroupAggr) error {

	for _, a := range protoGroupAggr.GetAggrs() {

		var aggr Aggregate

		aggr.AggrFunc = common.AggrFuncType(a.GetAggrFunc())
		aggr.EntryKeyId = a.GetEntryKeyId()
		aggr.KeyPos = a.GetKeyPos()
		aggr.Distinct = a.GetDistinct()

		if aggr.KeyPos < 0 {
			if string(a.GetExpr()) == "" {
				return errors.New("Aggregate expression is empty")
			}
			expr, err := compileN1QLExpression(string(a.GetExpr()))
			if err != nil {
				return err
			}
			aggr.Expr = expr
			aggr.ExprValue = expr.Value() // value will be nil if it is not constant expr
			if aggr.ExprValue == nil {
				r.GroupAggr.HasExpr = true
				r.GroupAggr.NeedDecode = true
				r.GroupAggr.NeedExplode = true
			}
			if r.GroupAggr.cv == nil {
				r.GroupAggr.cv = value.NewScopeValue(make(map[string]interface{}), nil)
				r.GroupAggr.av = value.NewAnnotatedValue(r.GroupAggr.cv)
				r.GroupAggr.exprContext = expression.NewIndexContext()
			}
		} else {
			if aggr.AggrFunc == common.AGG_SUM {
				r.GroupAggr.NeedDecode = true
				if !r.isPrimary {
					r.decodePositions[aggr.KeyPos] = true
				}
			}
			r.GroupAggr.NeedExplode = true
			if !r.isPrimary {
				r.explodePositions[aggr.KeyPos] = true
			}
		}

		r.GroupAggr.Aggrs = append(r.GroupAggr.Aggrs, &aggr)
	}

	return nil

}

func (r *ScanRequest) validateGroupAggr() error {

	if r.isPrimary {
		return nil
	}

	//identify leading/non-leading
	var prevPos int32 = -1
	r.GroupAggr.IsLeadingGroup = true

outerloop:
	for _, g := range r.GroupAggr.Group {
		if g.KeyPos < 0 {
			r.GroupAggr.IsLeadingGroup = false
			break
		} else if g.KeyPos == 0 {
			prevPos = 0
		} else {
			if g.KeyPos != prevPos+1 {
				for prevPos < g.KeyPos-1 {
					prevPos++
					if !r.hasAllEqualFilters(int(prevPos)) {
						prevPos--
						break
					}
				}
				if g.KeyPos != prevPos+1 {
					r.GroupAggr.IsLeadingGroup = false
					break outerloop
				}
			}
		}
		prevPos = g.KeyPos
	}

	var err error

	if !r.GroupAggr.AllowPartialAggr && !r.GroupAggr.IsLeadingGroup {
		err = fmt.Errorf("Requested Partial Aggr %v Not Supported For Given Scan", r.GroupAggr.AllowPartialAggr)
		logging.Errorf("ScanRequest::validateGroupAggr %v ", err)
		return err
	}

	//validate aggregates
	for _, a := range r.GroupAggr.Aggrs {
		if a.AggrFunc >= common.AGG_INVALID {
			logging.Errorf("ScanRequest::validateGroupAggr %v %v", ErrInvalidAggrFunc, a.AggrFunc)
			return ErrInvalidAggrFunc
		}
		if int(a.KeyPos) >= len(r.IndexInst.Defn.SecExprs) {
			err = fmt.Errorf("Invalid KeyPos In Aggr %v", a)
			logging.Errorf("ScanRequest::validateGroupAggr %v", err)
			return err
		}
	}

	//validate group by
	for _, g := range r.GroupAggr.Group {
		if int(g.KeyPos) >= len(r.IndexInst.Defn.SecExprs) {
			err = fmt.Errorf("Invalid KeyPos In GroupKey %v", g)
			logging.Errorf("ScanRequest::validateGroupAggr %v", err)
			return err
		}
	}

	//validate DependsOnIndexKeys
	for _, k := range r.GroupAggr.DependsOnIndexKeys {
		if int(k) > len(r.IndexInst.Defn.SecExprs) {
			err = fmt.Errorf("Invalid KeyPos In DependsOnIndexKeys %v", k)
			logging.Errorf("ScanRequest::validateGroupAggr %v", err)
			return err
		}
	}

	r.GroupAggr.FirstValidAggrOnly = r.processFirstValidAggrOnly()
	return nil
}

// Scan needs to process only first valid aggregate value
// if below rules are satisfied. It is an optimization added for MB-27861
func (r *ScanRequest) processFirstValidAggrOnly() bool {

	if len(r.GroupAggr.Group) != 0 {
		return false
	}

	if len(r.GroupAggr.Aggrs) != 1 {
		return false
	}

	aggr := r.GroupAggr.Aggrs[0]

	if aggr.AggrFunc != common.AGG_MIN &&
		aggr.AggrFunc != common.AGG_MAX &&
		aggr.AggrFunc != common.AGG_COUNT {
		return false
	}

	checkEqualityFilters := func(keyPos int32) bool {
		if keyPos < 0 {
			return false
		}
		if keyPos == 0 {
			return true
		}

		// If keyPos > 0, check if there is more than 1 span
		// In case of multiple spans, do not apply the optimization
		if len(r.Scans) > 1 {
			return false
		}

		return r.hasAllEqualFiltersUpto(int(keyPos) - 1)
	}

	isAscKey := func(keyPos int32) bool {
		if !r.IndexInst.Defn.HasDescending() {
			return true
		}
		if r.IndexInst.Defn.Desc[keyPos] {
			return false
		}
		return true
	}

	if aggr.AggrFunc == common.AGG_MIN {
		if !checkEqualityFilters(aggr.KeyPos) {
			return false
		}

		return isAscKey(aggr.KeyPos)
	}

	if aggr.AggrFunc == common.AGG_MAX {
		if !checkEqualityFilters(aggr.KeyPos) {
			return false
		}

		return !isAscKey(aggr.KeyPos)
	}

	// Rule applies for COUNT(DISTINCT const_expr)
	if aggr.AggrFunc == common.AGG_COUNT {
		if aggr.ExprValue != nil && aggr.Distinct {
			return true
		}
		return false
	}

	return false
}

func (r *ScanRequest) canUseFastCount(protoScans []*protobuf.Scan) bool {

	//only one aggregate
	if len(r.GroupAggr.Aggrs) != 1 {
		return false
	}

	//no group by
	if len(r.GroupAggr.Group) != 0 {
		return false
	}

	//ignore array index
	if r.IndexInst.Defn.IsArrayIndex {
		return false
	}

	//ignore primary index
	if r.IndexInst.Defn.IsPrimary {
		return false
	}

	aggr := r.GroupAggr.Aggrs[0]

	//only non distinct count
	if aggr.AggrFunc != common.AGG_COUNT || aggr.Distinct {
		return false
	}

	if r.canUseFastCountNoWhere() {
		return true
	}
	if r.canUseFastCountWhere(protoScans) {
		return true
	}
	return false

}

func (r *ScanRequest) canUseFastCountWhere(protoScans []*protobuf.Scan) bool {

	aggr := r.GroupAggr.Aggrs[0]
	//only the first leading key or constant expression
	if aggr.KeyPos == 0 || aggr.ExprValue != nil {
		//if index has where clause
		if r.IndexInst.Defn.WhereExpr != "" {

			for _, scan := range protoScans {
				//compute filter covers
				wExpr, err := parser.Parse(r.IndexInst.Defn.WhereExpr)
				if err != nil {
					logging.Errorf("%v Error parsing where expr %v", r.LogPrefix, err)
				}

				fc := make(map[string]value.Value)
				fc = wExpr.FilterCovers(fc)

				for i, fl := range scan.Filters {

					//only equal filter is supported
					if !checkEqualFilter(fl) {
						return false
					}

					var cv *value.ScopeValue
					var av value.AnnotatedValue

					cv = value.NewScopeValue(make(map[string]interface{}), nil)
					av = value.NewAnnotatedValue(cv)

					av.SetCover(r.IndexInst.Defn.SecExprs[i], value.NewValue(fl.Low))

					cv1 := av.Covers()
					fields := cv1.Fields()

					for f, v := range fields {
						if v1, ok := fc[f]; ok {
							if v != v1 {
								return false
							}
						} else {
							return false
						}
					}
				}
				return true
			}
		}
	}
	return false
}

func (r *ScanRequest) canUseFastCountNoWhere() bool {

	aggr := r.GroupAggr.Aggrs[0]

	//only the first leading key or constant expression
	if aggr.KeyPos == 0 || aggr.ExprValue != nil {

		//full index scan
		if len(r.Scans) == 1 {
			scan := r.Scans[0]
			if len(scan.Filters) == 1 {
				filter := scan.Filters[0]
				if len(filter.CompositeFilters) == 1 {
					if isEncodedNull(filter.CompositeFilters[0].Low.Bytes()) &&
						filter.CompositeFilters[0].High.Bytes() == nil &&
						(filter.CompositeFilters[0].Inclusion == Low ||
							filter.CompositeFilters[0].Inclusion == Neither) {
						return true
					}
				}
			}
		}
	}

	return false
}

func checkEqualFilter(fl *protobuf.CompositeElementFilter) bool {

	if (fl.Low != nil && fl.High != nil && bytes.Equal(fl.Low, fl.High)) && Inclusion(fl.GetInclusion()) == Both {
		return true
	}
	return false

}

func (r *ScanRequest) hasAllEqualFiltersUpto(keyPos int) bool {
	for i := 0; i <= keyPos; i++ {
		if !r.hasAllEqualFilters(i) {
			return false
		}
	}
	return true
}

// Returns true if all filters for the given keyPos(index field) are equal
// and atleast one equal filter exists.
//
// (1) "nil" value for high or low means the filter is unbounded on one end
//
//	or the both ends. So, it cannot be an equality filter.
//
// (2) If Low == High AND
//
//	(2.1) If Inclusion is Low or High, then the filter is contradictory.
//	(2.2) If Inclusion is Neither, then everything will be filtered out,
//	      which is an unexpected behavior.
//
// (3) If there are multiple filters, and at least one filter has less number
//
//	of composite filters as compared to the input keyPos, then for that
//	filter the equality is unknown and hence return false.
//
// So, for these cases, hasAllEqualFilters returns false.
func (r *ScanRequest) hasAllEqualFilters(keyPos int) bool {

	found := false
	for _, scan := range r.Scans {
		for _, filter := range scan.Filters {
			if len(filter.CompositeFilters) > keyPos {
				lowBytes := filter.CompositeFilters[keyPos].Low.Bytes()
				highBytes := filter.CompositeFilters[keyPos].High.Bytes()
				if lowBytes == nil || highBytes == nil {
					return false
				}

				if !bytes.Equal(lowBytes, highBytes) {
					return false
				} else {
					if filter.CompositeFilters[keyPos].Inclusion != Both {
						return false
					}

					found = true
				}
			} else {
				return false
			}
		}
	}
	return found
}

func compileN1QLExpression(expr string) (expression.Expression, error) {

	cExpr, err := parser.Parse(expr)
	if err != nil {
		logging.Errorf("ScanRequest::compileN1QLExpression() %v: %v\n", logging.TagUD(expr), err)
		return nil, err
	}
	return cExpr, nil

}

func (req *ScanRequest) GetReadUnits() (ru uint64) {
	if len(req.Ctxs) != 0 {
		for _, ctx := range req.Ctxs {
			ru += ctx.ReadUnits()
		}
	}
	return
}

/////////////////////////////////////////////////////////////////////////
//
// Helpers
//
/////////////////////////////////////////////////////////////////////////

func getReverseCollatedIndexKey(input []byte, desc []bool) (IndexKey, error) {
	reversed, err := jsonEncoder.ReverseCollate(input, desc)
	if err != nil {
		return nil, err
	}
	key := secondaryKey(reversed)
	return &key, nil
}

func flipInclusion(incl Inclusion, desc []bool) Inclusion {
	if len(desc) != 0 && desc[0] {
		if incl == Low {
			return High
		} else if incl == High {
			return Low
		}
	}
	return incl
}

func getScanAll() Scan {
	s := Scan{
		ScanType: AllReq,
	}
	return s
}

// Return true if a < b
func IndexKeyLessThan(a, b IndexKey) bool {
	if a == MinIndexKey {
		return true
	} else if a == MaxIndexKey {
		return false
	} else if b == MinIndexKey {
		return false
	} else if b == MaxIndexKey {
		return true
	}
	return (bytes.Compare(a.Bytes(), b.Bytes()) < 0)
}

func setTimeoutTimer(timeout time.Duration, r *ScanRequest) {
	if timeout != 0 {
		r.ExpiredTime = time.Now().Add(timeout)
		r.Timeout = time.NewTimer(timeout)
	}
}

func (r ScanRequest) String() string {
	str := fmt.Sprintf("defnId:%v, instId:%v, index:%v/%v, type:%v, partitions:%v user:%v",
		r.DefnID, r.IndexInstId, r.Bucket, r.IndexName, r.ScanType, r.PartitionIds, r.User)

	if len(r.Scans) == 0 {
		var incl, span string

		switch r.Incl {
		case Low:
			incl = "incl:low"
		case High:
			incl = "incl:high"
		case Both:
			incl = "incl:both"
		default:
			incl = "incl:none"
		}

		if len(r.Keys) == 0 {
			if r.ScanType == StatsReq || r.ScanType == ScanReq || r.ScanType == CountReq || r.ScanType == VectorScanReq {
				span = fmt.Sprintf("range (%s,%s %s)", r.Low, r.High, incl)
			} else {
				span = "all"
			}
		} else {
			span = "keys ( "
			for _, k := range r.Keys {
				span = span + k.String() + " "
			}
			span = span + ")"
		}

		str += fmt.Sprintf(", span:%s", logging.TagUD(span))
	} else {
		str += fmt.Sprintf(", scans: %+v", logging.TagUD(r.Scans))
	}

	if r.Limit > 0 {
		str += fmt.Sprintf(", limit:%d", r.Limit)
	}

	if r.Consistency != nil {
		str += fmt.Sprintf(", consistency:%s", strings.ToLower(r.Consistency.String()))
	}

	if r.RequestId != "" {
		str += fmt.Sprintf(", requestId:%v", r.RequestId)
	}

	if r.GroupAggr != nil {
		str += fmt.Sprintf(", groupaggr: %v", r.GroupAggr)
	}

	return str
}

func (r *ScanRequest) getKeyBuffer(minSize int) []byte {
	if r.indexKeyBuffer == nil {
		buf := secKeyBufPool.Get()
		if minSize != 0 {
			newBuf := resizeEncodeBuf(*buf, minSize, true)
			r.keyBufList = append(r.keyBufList, &newBuf)
			r.indexKeyBuffer = newBuf
		} else {
			r.keyBufList = append(r.keyBufList, buf)
			r.indexKeyBuffer = *buf
		}
	}
	return r.indexKeyBuffer
}

// Reuses buffer from buffer pool. When current buffer is insufficient
// get new buffer from the pool, reset sharedBuffer & sharedBufferLen
func (r *ScanRequest) getSharedBuffer(length int) []byte {
	if r.sharedBuffer == nil || (cap(*r.sharedBuffer)-r.sharedBufferLen) < length {
		buf := secKeyBufPool.Get()
		r.keyBufList = append(r.keyBufList, buf)
		r.sharedBuffer = buf
		r.sharedBufferLen = 0
		return (*r.sharedBuffer)[:0]
	}
	return (*r.sharedBuffer)[r.sharedBufferLen:r.sharedBufferLen]
}

func (r *ScanRequest) hasDesc() bool {
	hasDesc := r.IndexInst.Defn.HasDescending()
	return hasDesc
}

func (r *ScanRequest) getFromSecKeyBufPool() *[]byte {
	buf := secKeyBufPool.Get()
	r.keyBufList = append(r.keyBufList, buf)
	return buf
}

func (r *ScanRequest) getVectorDim() int {
	return r.IndexInst.Defn.VectorMeta.Dimension
}

func (r *ScanRequest) getVectorKeyPos() int {
	return r.vectorPos
}

// return the codeSize for vector index. 0 indicates unknown.
func (r *ScanRequest) getVectorCodeSize() int {

	if r.codebookMap != nil {
		for _, cb := range r.codebookMap {
			if cb != nil {
				cs, err := cb.CodeSize()
				if err == nil {
					return cs
				}
			}
		}
	}
	return 0
}

func (r *ScanRequest) getRowCompare() RowsCompareLessFn {
	if r.indexOrder == nil {
		logging.Verbosef("getRowCompare: No OderBy pushdown is seen")
		return nil
	}

	if r.indexOrder.vectorDistOnly {
		logging.Verbosef("getRowCompare: Ordering by Vector Distance only..")
		return nil
	}

	// For this we always need ordering based on distance and hence we need
	// substituion of dist and which is not possible when we are not copying
	// row as we will modifying the storage copy. So make sort key and use
	// that in this case too
	// if r.indexOrder.inIndexOrder {
	// 	logging.Infof("Odering in the Index Order")
	// 	return func(i, j *Row) bool {
	// 		return bytes.Compare(i.key, j.key) < 0
	// 	}
	// }

	logging.Verbosef("getRowCompare: Odering using sortkey")
	return func(i, j *Row) bool {
		return bytes.Compare(i.sortKey, j.sortKey) < 0
	}
}

// return the codeSize for vector index. 0 indicates unknown.
func (r *ScanRequest) getVectorCoarseSize() int {

	if r.codebookMap != nil {
		for _, cb := range r.codebookMap {
			if cb != nil {
				cs, err := cb.CoarseSize()
				if err == nil {
					return cs
				}
			}
		}
	}
	return 0
}

/////////////////////////////////////////////////////////////////////////
//
// IndexPoints Implementation
//
/////////////////////////////////////////////////////////////////////////

func (ip IndexPoints) Len() int {
	return len(ip)
}

func (ip IndexPoints) Swap(i, j int) {
	ip[i], ip[j] = ip[j], ip[i]
}

func (ip IndexPoints) Less(i, j int) bool {
	return IndexPointLessThan(ip[i], ip[j])
}

// Return true if x < y
func IndexPointLessThan(x, y IndexPoint) bool {
	a := x.Value
	b := y.Value
	if a == MinIndexKey {
		return true
	} else if a == MaxIndexKey {
		return false
	} else if b == MinIndexKey {
		return false
	} else if b == MaxIndexKey {
		return true
	}

	if a.ComparePrefixIndexKey(b) < 0 {
		return true
	} else if a.ComparePrefixIndexKey(b) == 0 {
		if len(a.Bytes()) == len(b.Bytes()) {
			if x.Type == "low" && y.Type == "high" {
				return true
			}
			return false
		}
		inclusiveKey := minlen(x, y)
		switch inclusiveKey.Type {
		case "low":
			if inclusiveKey == x {
				return true
			}
		case "high":
			if inclusiveKey == y {
				return true
			}
		}
		return false
	}
	return false
}

func minlen(x, y IndexPoint) IndexPoint {
	if len(x.Value.Bytes()) < len(y.Value.Bytes()) {
		return x
	}
	return y
}

/////////////////////////////////////////////////////////////////////////
//
// Filters Implementation
//
/////////////////////////////////////////////////////////////////////////

func (fl Filters) Len() int {
	return len(fl)
}

func (fl Filters) Swap(i, j int) {
	fl[i], fl[j] = fl[j], fl[i]
}

func (fl Filters) Less(i, j int) bool {
	return FilterLessThan(fl[i], fl[j])
}

// Return true if x < y
func FilterLessThan(x, y Filter) bool {
	a := x.Low
	b := y.Low
	if a == MinIndexKey {
		return true
	} else if a == MaxIndexKey {
		return false
	} else if b == MinIndexKey {
		return false
	} else if b == MaxIndexKey {
		return true
	}

	if a.ComparePrefixIndexKey(b) < 0 {
		return true
	}
	return false
}

/////////////////////////////////////////////////////////////////////////
//
// Connection Handler
//
/////////////////////////////////////////////////////////////////////////

const (
	ScanBufPoolSize = DEFAULT_MAX_SEC_KEY_LEN_SCAN + MAX_DOCID_LEN + 2
)

const (
	ScanQueue        = "ScanQueue"
	VectorScanWorker = "VectorScanWorker"
)

type ConCacheObj interface {
	Free() bool
}

type ConnectionContext struct {
	bufPool    map[common.PartitionId]*common.BytesBufPool
	vecBufPool map[int]*common.BytesBufPool
	cache      map[string]ConCacheObj
	mutex      sync.RWMutex
}

func createConnectionContext() interface{} {
	return &ConnectionContext{
		bufPool:    make(map[common.PartitionId]*common.BytesBufPool),
		vecBufPool: make(map[int]*common.BytesBufPool),
		cache:      make(map[string]ConCacheObj),
	}
}

func (c *ConnectionContext) GetBufPool(partitionId common.PartitionId) *common.BytesBufPool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if _, ok := c.bufPool[partitionId]; !ok {
		c.bufPool[partitionId] = common.NewByteBufferPool(ScanBufPoolSize)
	}

	return c.bufPool[partitionId]
}

func (c *ConnectionContext) GetVectorBufPool(workerId int) *common.BytesBufPool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if _, ok := c.vecBufPool[workerId]; !ok {
		c.vecBufPool[workerId] = common.NewByteBufferPool(ScanBufPoolSize)
	}

	return c.vecBufPool[workerId]
}

func (c *ConnectionContext) Get(id string) ConCacheObj {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.cache[id]
}

func (c *ConnectionContext) Put(id string, obj ConCacheObj) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.cache[id] = obj
}

func (c *ConnectionContext) ResetCache() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for key, obj := range c.cache {
		if obj.Free() {
			delete(c.cache, key)
		}
	}
}
