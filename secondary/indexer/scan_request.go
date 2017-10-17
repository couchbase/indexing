// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package indexer

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/couchbase/indexing/secondary/collatejson"
	"github.com/couchbase/indexing/secondary/common"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/query"
)

type ScanReqType string

const (
	StatsReq          ScanReqType = "stats"
	CountReq                      = "count"
	ScanReq                       = "scan"
	ScanAllReq                    = "scanAll"
	HeloReq                       = "helo"
	MultiScanCountReq             = "multiscancount"
)

type ScanRequest struct {
	ScanType     ScanReqType
	DefnID       uint64
	IndexInstId  common.IndexInstId
	IndexName    string
	Bucket       string
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

	// New parameters for spock
	Scans             []Scan
	Indexprojection   *Projection
	Reverse           bool
	Distinct          bool
	Offset            int64
	projectPrimaryKey bool

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
}

type Projection struct {
	projectSecKeys bool
	projectionKeys []bool
	entryKeysEmpty bool
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

// A point in index and the corresponding filter
// the point belongs to either as high or low
type IndexPoint struct {
	Value    IndexKey
	FilterId int
	Type     string
}

// Implements sort Interface
type IndexPoints []IndexPoint

/////////////////////////////////////////////////////////////////////////
//
//  scan request implementation
//
/////////////////////////////////////////////////////////////////////////

func NewScanRequest(protoReq interface{},
	cancelCh <-chan bool, s *scanCoordinator) (r *ScanRequest, err error) {

	r = new(ScanRequest)
	r.ScanId = atomic.AddUint64(&s.reqCounter, 1)
	r.LogPrefix = fmt.Sprintf("SCAN##%d", r.ScanId)
	r.sco = s

	cfg := s.config.Load()
	timeout := time.Millisecond * time.Duration(cfg["settings.scan_timeout"].Int())

	if timeout != 0 {
		r.ExpiredTime = time.Now().Add(timeout)
		r.Timeout = time.NewTimer(timeout)
	}

	r.CancelCh = cancelCh

	isBootstrapMode := s.isBootstrapMode()
	r.projectPrimaryKey = true

	switch req := protoReq.(type) {
	case *protobuf.HeloRequest:
		r.ScanType = HeloReq
	case *protobuf.StatisticsRequest:
		r.DefnID = req.GetDefnID()
		r.RequestId = req.GetRequestId()
		r.ScanType = StatsReq
		r.Incl = Inclusion(req.GetSpan().GetRange().GetInclusion())
		if isBootstrapMode {
			err = common.ErrIndexerInBootstrap
			return
		}
		err = r.setIndexParams()
		err = r.fillRanges(
			req.GetSpan().GetRange().GetLow(),
			req.GetSpan().GetRange().GetHigh(),
			req.GetSpan().GetEquals())

	case *protobuf.CountRequest:
		r.DefnID = req.GetDefnID()
		r.RequestId = req.GetRequestId()
		r.rollbackTime = req.GetRollbackTime()
		r.PartitionIds = makePartitionIds(req.GetPartitionIds())
		cons := common.Consistency(req.GetCons())
		vector := req.GetVector()
		r.ScanType = CountReq
		r.Incl = Inclusion(req.GetSpan().GetRange().GetInclusion())

		if isBootstrapMode {
			err = common.ErrIndexerInBootstrap
			return
		}

		err = r.setIndexParams()
		err = r.setConsistency(cons, vector)
		err = r.fillRanges(
			req.GetSpan().GetRange().GetLow(),
			req.GetSpan().GetRange().GetHigh(),
			req.GetSpan().GetEquals())
		sc := req.GetScans()
		if len(sc) != 0 {
			err = r.fillScans(sc)
			r.ScanType = MultiScanCountReq
			r.Distinct = req.GetDistinct()
		}

	case *protobuf.ScanRequest:
		r.DefnID = req.GetDefnID()
		r.RequestId = req.GetRequestId()
		r.rollbackTime = req.GetRollbackTime()
		r.PartitionIds = makePartitionIds(req.GetPartitionIds())
		cons := common.Consistency(req.GetCons())
		vector := req.GetVector()
		r.ScanType = ScanReq
		r.Incl = Inclusion(req.GetSpan().GetRange().GetInclusion())
		r.Limit = req.GetLimit()
		r.Reverse = req.GetReverse()
		proj := req.GetIndexprojection()
		if proj == nil {
			r.Distinct = req.GetDistinct()
		}
		r.Offset = req.GetOffset()
		if isBootstrapMode {
			err = common.ErrIndexerInBootstrap
			return
		}
		err = r.setIndexParams()
		err = r.setConsistency(cons, vector)
		if proj != nil {
			var localerr error
			if r.Indexprojection, localerr = validateIndexProjection(proj, len(r.IndexInst.Defn.SecExprs)); localerr != nil {
				err = localerr
				return
			}
			r.projectPrimaryKey = *proj.PrimaryKey
		}
		err = r.fillRanges(
			req.GetSpan().GetRange().GetLow(),
			req.GetSpan().GetRange().GetHigh(),
			req.GetSpan().GetEquals())
		err = r.fillScans(req.GetScans())

	case *protobuf.ScanAllRequest:
		r.DefnID = req.GetDefnID()
		r.RequestId = req.GetRequestId()
		r.rollbackTime = req.GetRollbackTime()
		r.PartitionIds = makePartitionIds(req.GetPartitionIds())
		cons := common.Consistency(req.GetCons())
		vector := req.GetVector()
		r.ScanType = ScanAllReq
		r.Limit = req.GetLimit()
		r.Scans = make([]Scan, 1)
		r.Scans[0].ScanType = AllReq

		if isBootstrapMode {
			err = common.ErrIndexerInBootstrap
			return
		}

		err = r.setIndexParams()
		err = r.setConsistency(cons, vector)
	default:
		err = ErrUnsupportedRequest
	}

	return
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
		return NewSecondaryKey(k, r.getKeyBuffer())
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
		lowKey = getReverseCollatedIndexKey(joinedLow, r.IndexInst.Defn.Desc[:len(lows2)])
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
		highKey = getReverseCollatedIndexKey(joinedHigh, r.IndexInst.Defn.Desc[:len(highs2)])
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
		eqReverse = jsonEncoder.ReverseCollate(equalsKey, r.IndexInst.Defn.Desc[:len(equals)])
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

					if r.isPrimary {
						scan.ScanType = RangeReq
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

func (r *ScanRequest) fillScans(protoScans []*protobuf.Scan) (localErr error) {
	var l, h IndexKey

	// For Upgrade
	if len(protoScans) == 0 {
		r.Scans = make([]Scan, 1)
		if len(r.Keys) > 0 {
			r.Scans[0].Equals = r.Keys[0] //TODO fix for multiple Keys needed?
			r.Scans[0].ScanType = LookupReq
		} else {
			r.Scans[0].Low = r.Low
			r.Scans[0].High = r.High
			r.Scans[0].Incl = r.Incl
			r.Scans[0].ScanType = RangeReq
		}
		return
	}

	// Array of Filters
	var filters []Filter
	var points []IndexPoint

	if r.isPrimary {
		for _, protoScan := range protoScans {
			if len(protoScan.Equals) != 0 {
				var filter Filter
				var key IndexKey
				if key, localErr = r.newKey(protoScan.Equals[0]); localErr != nil {
					localErr = fmt.Errorf("Invalid equal key %s (%s)", string(protoScan.Equals[0]), localErr)
					return
				}
				filter.Low = key
				filter.High = key
				filters = append(filters, filter)

				p1 := IndexPoint{Value: filter.Low, FilterId: len(filters) - 1, Type: "low"}
				p2 := IndexPoint{Value: filter.High, FilterId: len(filters) - 1, Type: "high"}
				points = append(points, p1, p2)
				continue
			}

			// If there are no filters in scan, it is ScanAll
			if len(protoScan.Filters) == 0 {
				r.Scans = make([]Scan, 1)
				r.Scans[0] = getScanAll()
				return
			}

			// if all scan filters are (nil, nil), it is ScanAll
			if r.areFiltersNil(protoScan) {
				r.Scans = make([]Scan, 1)
				r.Scans[0] = getScanAll()
				return
			}

			fl := protoScan.Filters[0]
			if l, localErr = r.newLowKey(fl.Low); localErr != nil {
				localErr = fmt.Errorf("Invalid low key %s (%s)", string(fl.Low), localErr)
				return
			}

			if h, localErr = r.newHighKey(fl.High); localErr != nil {
				localErr = fmt.Errorf("Invalid high key %s (%s)", string(fl.High), localErr)
				return
			}

			if IndexKeyLessThan(h, l) {
				continue
			}

			filter := Filter{
				CompositeFilters: nil,
				Inclusion:        Inclusion(fl.GetInclusion()),
				Low:              l,
				High:             h,
			}
			filters = append(filters, filter)
			p1 := IndexPoint{Value: filter.Low, FilterId: len(filters) - 1, Type: "low"}
			p2 := IndexPoint{Value: filter.High, FilterId: len(filters) - 1, Type: "high"}
			points = append(points, p1, p2)
		}
	} else {
		for _, protoScan := range protoScans {
			skipScan := false
			if len(protoScan.Equals) != 0 {
				//Encode the equals keys
				var filter Filter
				if localErr = r.fillFilterEquals(protoScan, &filter); localErr != nil {
					return
				}
				filters = append(filters, filter)

				p1 := IndexPoint{Value: filter.Low, FilterId: len(filters) - 1, Type: "low"}
				p2 := IndexPoint{Value: filter.High, FilterId: len(filters) - 1, Type: "high"}
				points = append(points, p1, p2)
				continue
			}

			// If there are no filters in scan, it is ScanAll
			if len(protoScan.Filters) == 0 {
				r.Scans = make([]Scan, 1)
				r.Scans[0] = getScanAll()
				return
			}

			// if all scan filters are (nil, nil), it is ScanAll
			if r.areFiltersNil(protoScan) {
				r.Scans = make([]Scan, 1)
				r.Scans[0] = getScanAll()
				return
			}

			var compFilters []CompositeElementFilter
			// Encode Filters
			for _, fl := range protoScan.Filters {
				if l, localErr = r.newLowKey(fl.Low); localErr != nil {
					localErr = fmt.Errorf("Invalid low key %s (%s)", string(fl.Low), localErr)
					return
				}

				if h, localErr = r.newHighKey(fl.High); localErr != nil {
					localErr = fmt.Errorf("Invalid high key %s (%s)", string(fl.High), localErr)
					return
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
				return
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
	r.Scans = r.composeScans(points, filters)
	return
}

func (r *ScanRequest) setConsistency(cons common.Consistency, vector *protobuf.TsConsistency) (localErr error) {

	r.Consistency = &cons
	cfg := r.sco.config.Load()
	if cons == common.QueryConsistency && vector != nil {
		r.Ts = common.NewTsVbuuid(r.Bucket, cfg["numVbuckets"].Int())
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
			r.LogPrefix, cluster, r.Bucket, cfg["numVbuckets"].Int())
		if localErr == nil && r.Stats != nil {
			r.Stats.Timings.dcpSeqs.Put(time.Since(t0))
		}
		r.Ts.Crc64 = 0
		r.Ts.Bucket = r.Bucket
	}
	return
}

func (r *ScanRequest) setIndexParams() (localErr error) {
	r.sco.mu.RLock()
	defer r.sco.mu.RUnlock()

	var indexInst *common.IndexInst

	stats := r.sco.stats.Get()
	indexInst, r.Ctxs, localErr = r.sco.findIndexInstance(r.DefnID, r.PartitionIds)
	if localErr == nil {
		r.isPrimary = indexInst.Defn.IsPrimary
		r.IndexName, r.Bucket = indexInst.Defn.Name, indexInst.Defn.Bucket
		r.IndexInstId = indexInst.InstId
		r.IndexInst = *indexInst

		if indexInst.State != common.INDEX_STATE_ACTIVE {
			localErr = common.ErrIndexNotReady
		}
		r.Stats = stats.indexes[r.IndexInstId]
		rbMap := *r.sco.getRollbackInProgress()
		r.hasRollback = rbMap[indexInst.Defn.Bucket]
	}
	return
}

func validateIndexProjection(projection *protobuf.IndexProjection, cklen int) (*Projection, error) {
	if len(projection.EntryKeys) > cklen {
		e := errors.New(fmt.Sprintf("Invalid number of Entry Keys %v in IndexProjection", len(projection.EntryKeys)))
		return nil, e
	}

	projectionKeys := make([]bool, cklen)
	for _, position := range projection.EntryKeys {
		if position >= int64(cklen) || position < 0 {
			e := errors.New(fmt.Sprintf("Invalid Entry Key %v in IndexProjection", position))
			return nil, e
		}
		projectionKeys[position] = true
	}

	projectAllSecKeys := true
	for _, sp := range projectionKeys {
		if sp == false {
			projectAllSecKeys = false
		}
	}

	indexProjection := &Projection{}
	indexProjection.projectSecKeys = !projectAllSecKeys
	indexProjection.projectionKeys = projectionKeys
	indexProjection.entryKeysEmpty = len(projection.EntryKeys) == 0

	return indexProjection, nil
}

func getReverseCollatedIndexKey(input []byte, desc []bool) IndexKey {
	reversed := jsonEncoder.ReverseCollate(input, desc)
	key := secondaryKey(reversed)
	return &key
}

func flipInclusion(incl Inclusion, desc []bool) Inclusion {
	if desc != nil && desc[0] {
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

func (r ScanRequest) String() string {
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
		if r.ScanType == StatsReq || r.ScanType == ScanReq || r.ScanType == CountReq {
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

	str := fmt.Sprintf("defnId:%v, index:%v/%v, type:%v, span:%s",
		r.DefnID, r.Bucket, r.IndexName, r.ScanType, span)

	if r.Limit > 0 {
		str += fmt.Sprintf(", limit:%d", r.Limit)
	}

	if r.Consistency != nil {
		str += fmt.Sprintf(", consistency:%s", strings.ToLower(r.Consistency.String()))
	}

	if r.RequestId != "" {
		str += fmt.Sprintf(", requestId:%v", r.RequestId)
	}

	return str
}

func (r *ScanRequest) getKeyBuffer() []byte {
	if r.indexKeyBuffer == nil {
		buf := secKeyBufPool.Get()
		r.keyBufList = append(r.keyBufList, buf)
		r.indexKeyBuffer = *buf
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
