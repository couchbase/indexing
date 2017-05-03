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
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/indexing/secondary/collatejson"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	p "github.com/couchbase/indexing/secondary/pipeline"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/query"
	"github.com/couchbase/indexing/secondary/queryport"
	"github.com/golang/protobuf/proto"
)

// Errors
var (
	ErrNotMyIndex         = errors.New("Not my index")
	ErrInternal           = errors.New("Internal server error occured")
	ErrSnapNotAvailable   = errors.New("No snapshot available for scan")
	ErrUnsupportedRequest = errors.New("Unsupported query request")
	ErrVbuuidMismatch     = errors.New("Mismatch in session vbuuids")
)

var secKeyBufPool *common.BytesBufPool

func init() {
	secKeyBufPool = common.NewByteBufferPool(maxSecKeyBufferLen + ENCODE_BUF_SAFE_PAD)
}

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
	ScanType    ScanReqType
	DefnID      uint64
	IndexInstId common.IndexInstId
	IndexName   string
	Bucket      string
	Ts          *common.TsVbuuid
	Low         IndexKey
	High        IndexKey
	Keys        []IndexKey
	Consistency *common.Consistency
	Stats       *IndexStats
	IndexInst   common.IndexInst

	ctx IndexReaderContext

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

	ScanId      uint64
	ExpiredTime time.Time
	Timeout     *time.Timer
	CancelCh    <-chan bool

	RequestId string
	LogPrefix string

	keyBufList []*[]byte
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

type CancelCb struct {
	done    chan struct{}
	timeout <-chan time.Time
	cancel  <-chan bool
	callb   func(error)
}

func (c *CancelCb) Run() {
	go func() {
		select {
		case <-c.done:
		case <-c.cancel:
			c.callb(common.ErrClientCancel)
		case <-c.timeout:
			c.callb(common.ErrScanTimedOut)
		}
	}()
}

func (c *CancelCb) Done() {
	close(c.done)
}

func NewCancelCallback(req *ScanRequest, callb func(error)) *CancelCb {
	cb := &CancelCb{
		done:    make(chan struct{}),
		cancel:  req.CancelCh,
		timeout: req.getTimeoutCh(),
		callb:   callb,
	}

	return cb
}

type ScanCoordinator interface {
}

type scanCoordinator struct {
	supvCmdch        MsgChannel //supervisor sends commands on this channel
	supvMsgch        MsgChannel //channel to send any async message to supervisor
	snapshotNotifych chan IndexSnapshot
	lastSnapshot     map[common.IndexInstId]IndexSnapshot

	serv      *queryport.Server
	logPrefix string

	mu            sync.RWMutex
	indexInstMap  common.IndexInstMap
	indexPartnMap IndexPartnMap

	reqCounter uint64
	config     common.ConfigHolder

	stats IndexerStatsHolder

	indexerState atomic.Value
}

func (s *scanCoordinator) getIndexerState() common.IndexerState {
	return s.indexerState.Load().(common.IndexerState)
}

func (s *scanCoordinator) setIndexerState(state common.IndexerState) {
	s.indexerState.Store(state)
}

// NewScanCoordinator returns an instance of scanCoordinator or err message
// It listens on supvCmdch for command and every command is followed
// by a synchronous response on the supvCmdch.
// Any async message to supervisor is sent to supvMsgch.
// If supvCmdch get closed, ScanCoordinator will shut itself down.
func NewScanCoordinator(supvCmdch MsgChannel, supvMsgch MsgChannel,
	config common.Config, snapshotNotifych chan IndexSnapshot) (ScanCoordinator, Message) {
	var err error

	s := &scanCoordinator{
		supvCmdch:        supvCmdch,
		supvMsgch:        supvMsgch,
		lastSnapshot:     make(map[common.IndexInstId]IndexSnapshot),
		snapshotNotifych: snapshotNotifych,
		logPrefix:        "ScanCoordinator",
		reqCounter:       0,
	}

	s.config.Store(config)

	addr := net.JoinHostPort("", config["scanPort"].String())
	queryportCfg := config.SectionConfig("queryport.", true)
	s.serv, err = queryport.NewServer(addr, s.serverCallback, queryportCfg)

	if err != nil {
		errMsg := &MsgError{err: Error{code: ERROR_SCAN_COORD_QUERYPORT_FAIL,
			severity: FATAL,
			category: SCAN_COORD,
			cause:    err,
		},
		}
		return nil, errMsg
	}

	s.setIndexerState(common.INDEXER_BOOTSTRAP)

	// main loop
	go s.run()
	go s.listenSnapshot()

	return s, &MsgSuccess{}

}

func (s *scanCoordinator) listenSnapshot() {
	for snapshot := range s.snapshotNotifych {
		func(ss IndexSnapshot) {
			s.mu.Lock()
			defer s.mu.Unlock()

			if oldSnap, ok := s.lastSnapshot[ss.IndexInstId()]; ok {
				delete(s.lastSnapshot, ss.IndexInstId())
				if oldSnap != nil {
					DestroyIndexSnapshot(oldSnap)
				}
			}

			if ss.Timestamp() != nil {
				s.lastSnapshot[ss.IndexInstId()] = ss
			}

		}(snapshot)
	}
}

func (s *scanCoordinator) handleStats(cmd Message) {
	s.supvCmdch <- &MsgSuccess{}

	req := cmd.(*MsgStatsRequest)
	replych := req.GetReplyChannel()
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := s.stats.Get()
	st := s.serv.Statistics()
	stats.numConnections.Set(st.Connections)

	// Compute counts asynchronously and reply to stats request
	go func() {
		for id, idxStats := range stats.indexes {
			c, err := s.getItemsCount(id)
			if err == nil {
				idxStats.itemsCount.Set(int64(c))
			} else {
				logging.Errorf("%v: Unable compute index count for %v/%v (%v)", s.logPrefix,
					idxStats.bucket, idxStats.name, err)
			}

			// compute scan rate
			now := time.Now().UnixNano()
			elapsed := float64(now-idxStats.lastScanGatherTime.Value()) / float64(time.Second)
			if elapsed > 0 {
				numRowsReturned := idxStats.numRowsReturned.Value()
				scanRate := float64(numRowsReturned-idxStats.lastNumRowsReturned.Value()) / elapsed
				idxStats.avgScanRate.Set(int64((scanRate + float64(idxStats.avgScanRate.Value())) / 2))
				idxStats.lastScanGatherTime.Set(now)
				idxStats.lastNumRowsReturned.Set(numRowsReturned)
			}
		}
		replych <- true
	}()
}

func (s *scanCoordinator) run() {
loop:
	for {
		select {
		case cmd, ok := <-s.supvCmdch:
			if ok {
				if cmd.GetMsgType() == SCAN_COORD_SHUTDOWN {
					logging.Infof("ScanCoordinator: Shutting Down")
					s.serv.Close()
					s.supvCmdch <- &MsgSuccess{}
					break loop
				}
				s.handleSupvervisorCommands(cmd)
			} else {
				//supervisor channel closed. exit
				break loop
			}
		}
	}
}

func (s *scanCoordinator) handleSupvervisorCommands(cmd Message) {
	switch cmd.GetMsgType() {
	case UPDATE_INDEX_INSTANCE_MAP:
		s.handleUpdateIndexInstMap(cmd)

	case UPDATE_INDEX_PARTITION_MAP:
		s.handleUpdateIndexPartnMap(cmd)

	case SCAN_STATS:
		s.handleStats(cmd)

	case CONFIG_SETTINGS_UPDATE:
		s.handleConfigUpdate(cmd)

	case INDEXER_PAUSE:
		s.handleIndexerPause(cmd)

	case INDEXER_RESUME:
		s.handleIndexerResume(cmd)

	case INDEXER_BOOTSTRAP:
		s.handleIndexerBootstrap(cmd)

	default:
		logging.Errorf("ScanCoordinator: Received Unknown Command %v", cmd)
		s.supvCmdch <- &MsgError{
			err: Error{code: ERROR_SCAN_COORD_UNKNOWN_COMMAND,
				severity: NORMAL,
				category: SCAN_COORD}}
	}

}

func (s *scanCoordinator) newRequest(protoReq interface{},
	cancelCh <-chan bool) (r *ScanRequest, err error) {

	var indexInst *common.IndexInst
	r = new(ScanRequest)
	r.ScanId = atomic.AddUint64(&s.reqCounter, 1)
	r.LogPrefix = fmt.Sprintf("SCAN##%d", r.ScanId)

	cfg := s.config.Load()
	timeout := time.Millisecond * time.Duration(cfg["settings.scan_timeout"].Int())
	getseqsRetries := cfg["settings.scan_getseqnos_retries"].Int()

	if timeout != 0 {
		r.ExpiredTime = time.Now().Add(timeout)
		r.Timeout = time.NewTimer(timeout)
	}

	r.CancelCh = cancelCh

	isBootstrapMode := s.isBootstrapMode()
	r.projectPrimaryKey = true

	isNil := func(k []byte) bool {
		if k == nil || (!r.isPrimary && string(k) == "[]") {
			return true
		}
		return false
	}

	newKey := func(k []byte) (IndexKey, error) {
		if k == nil {
			return nil, fmt.Errorf("Key is null")
		}

		if r.isPrimary {
			return NewPrimaryKey(k)
		} else {
			buf := secKeyBufPool.Get()
			r.keyBufList = append(r.keyBufList, buf)
			return NewSecondaryKey(k, *buf)
		}
	}

	newLowKey := func(k []byte) (IndexKey, error) {
		if isNil(k) {
			return MinIndexKey, nil
		}

		return newKey(k)
	}

	newHighKey := func(k []byte) (IndexKey, error) {
		if isNil(k) {
			return MaxIndexKey, nil
		}

		return newKey(k)
	}

	fillRanges := func(low, high []byte, keys [][]byte) {
		var key IndexKey
		var localErr error
		defer func() {
			if err == nil {
				err = localErr
			}
		}()

		// range
		r.LowBytes = low
		r.HighBytes = high

		if r.Low, localErr = newLowKey(low); localErr != nil {
			localErr = fmt.Errorf("Invalid low key %s (%s)", string(low), localErr)
			return
		}

		if r.High, localErr = newHighKey(high); localErr != nil {
			localErr = fmt.Errorf("Invalid high key %s (%s)", string(high), localErr)
			return
		}

		// point query for keys
		for _, k := range keys {
			r.KeysBytes = append(r.KeysBytes, k)
			if key, localErr = newKey(k); localErr != nil {
				localErr = fmt.Errorf("Invalid equal key %s (%s)", string(k), localErr)
				return
			}
			r.Keys = append(r.Keys, key)
		}
	}

	joinKeys := func(keys [][]byte) ([]byte, error) {
		buf1 := secKeyBufPool.Get()
		r.keyBufList = append(r.keyBufList, buf1)

		joined, e := jsonEncoder.JoinArray(keys, (*buf1)[:0])
		if e != nil {
			e = fmt.Errorf("Error in joining keys: %s", e)
			return nil, e
		}
		return joined, nil
	}

	getReverseCollatedIndexKey := func(input []byte, desc []bool) IndexKey {
		reversed := jsonEncoder.ReverseCollate(input, desc)
		key := secondaryKey(reversed)
		return &key
	}

	flipInclusion := func(incl Inclusion, desc []bool) Inclusion {
		if desc != nil && desc[0] {
			if incl == Low {
				return High
			} else if incl == High {
				return Low
			}
		}
		return incl
	}

	getScanAll := func() Scan {
		s := Scan{
			ScanType: AllReq,
		}
		return s
	}

	areFiltersNil := func(protoScan *protobuf.Scan) bool {
		areFiltersNil := true
		for _, filter := range protoScan.Filters {
			if !isNil(filter.Low) || !isNil(filter.High) {
				areFiltersNil = false
				break
			}
		}
		return areFiltersNil
	}

	// Compute the overall low, high for a Filter
	// based on composite filter ranges
	fillFilterLowHigh := func(compFilters []CompositeElementFilter, filter *Filter) error {
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
				buf1 := secKeyBufPool.Get()
				r.keyBufList = append(r.keyBufList, buf1)

				if l, e = codec.JoinArray(lows, (*buf1)[:0]); e != nil {
					e = fmt.Errorf("Error in forming low key %s", e)
					return e
				}
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
				buf2 := secKeyBufPool.Get()
				r.keyBufList = append(r.keyBufList, buf2)

				if h, e = codec.JoinArray(highs, (*buf2)[:0]); e != nil {
					e = fmt.Errorf("Error in forming high key %s", e)
					return e
				}

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
			if joinedLow, e = joinKeys(lows2bytes); e != nil {
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
			if joinedHigh, e = joinKeys(highs2bytes); e != nil {
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

	fillFilterEquals := func(protoScan *protobuf.Scan, filter *Filter) error {
		var e error
		var equals [][]byte
		for _, k := range protoScan.Equals {
			var key IndexKey
			if key, e = newKey(k); e != nil {
				e = fmt.Errorf("Invalid equal key %s (%s)", string(k), e)
				return e
			}
			equals = append(equals, key.Bytes())
		}

		codec := collatejson.NewCodec(16)
		buf1 := secKeyBufPool.Get()
		r.keyBufList = append(r.keyBufList, buf1)
		var equalsKey, eqReverse []byte
		if equalsKey, e = codec.JoinArray(equals, (*buf1)[:0]); e != nil {
			e = fmt.Errorf("Error in forming equals key %s", e)
			return e
		}
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
	composeScans := func(points []IndexPoint, filters []Filter) []Scan {
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

	fillScans := func(protoScans []*protobuf.Scan) {
		var l, h IndexKey
		var localErr error
		defer func() {
			if err == nil {
				err = localErr
			}
		}()

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
					if key, localErr = newKey(protoScan.Equals[0]); localErr != nil {
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
				if areFiltersNil(protoScan) {
					r.Scans = make([]Scan, 1)
					r.Scans[0] = getScanAll()
					return
				}

				fl := protoScan.Filters[0]
				if l, localErr = newLowKey(fl.Low); localErr != nil {
					localErr = fmt.Errorf("Invalid low key %s (%s)", string(fl.Low), localErr)
					return
				}

				if h, localErr = newHighKey(fl.High); localErr != nil {
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
					if localErr = fillFilterEquals(protoScan, &filter); localErr != nil {
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
				if areFiltersNil(protoScan) {
					r.Scans = make([]Scan, 1)
					r.Scans[0] = getScanAll()
					return
				}

				var compFilters []CompositeElementFilter
				// Encode Filters
				for _, fl := range protoScan.Filters {
					if l, localErr = newLowKey(fl.Low); localErr != nil {
						localErr = fmt.Errorf("Invalid low key %s (%s)", string(fl.Low), localErr)
						return
					}

					if h, localErr = newHighKey(fl.High); localErr != nil {
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

				if localErr = fillFilterLowHigh(compFilters, &filter); localErr != nil {
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
		r.Scans = composeScans(points, filters)
	}

	setConsistency := func(
		cons common.Consistency, vector *protobuf.TsConsistency) {

		var localErr error
		defer func() {
			if err == nil {
				err = localErr
			}
		}()
		r.Consistency = &cons
		cfg := s.config.Load()
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
			r.Ts.Seqnos, localErr = bucketSeqsWithRetry(getseqsRetries, r.LogPrefix, cluster, r.Bucket)
			if localErr == nil && r.Stats != nil {
				r.Stats.Timings.dcpSeqs.Put(time.Since(t0))
			}
			r.Ts.Crc64 = 0
			r.Ts.Bucket = r.Bucket
		}
	}

	setIndexParams := func() {
		var localErr error
		defer func() {
			if err == nil {
				err = localErr
			}
		}()
		s.mu.RLock()
		defer s.mu.RUnlock()

		stats := s.stats.Get()
		indexInst, r.ctx, localErr = s.findIndexInstance(r.DefnID)
		if localErr == nil {
			r.isPrimary = indexInst.Defn.IsPrimary
			r.IndexName, r.Bucket = indexInst.Defn.Name, indexInst.Defn.Bucket
			r.IndexInstId = indexInst.InstId
			r.IndexInst = *indexInst

			if indexInst.State != common.INDEX_STATE_ACTIVE {
				localErr = common.ErrIndexNotReady
			}
			r.Stats = stats.indexes[r.IndexInstId]
		}
	}

	defer func() {
		if r.ctx != nil {
			r.ctx.Init()
		}
	}()

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
		setIndexParams()
		fillRanges(
			req.GetSpan().GetRange().GetLow(),
			req.GetSpan().GetRange().GetHigh(),
			req.GetSpan().GetEquals())

	case *protobuf.CountRequest:
		r.DefnID = req.GetDefnID()
		r.RequestId = req.GetRequestId()
		cons := common.Consistency(req.GetCons())
		vector := req.GetVector()
		r.ScanType = CountReq
		r.Incl = Inclusion(req.GetSpan().GetRange().GetInclusion())

		if isBootstrapMode {
			err = common.ErrIndexerInBootstrap
			return
		}

		setIndexParams()
		setConsistency(cons, vector)
		fillRanges(
			req.GetSpan().GetRange().GetLow(),
			req.GetSpan().GetRange().GetHigh(),
			req.GetSpan().GetEquals())
		sc := req.GetScans()
		if len(sc) != 0 {
			fillScans(sc)
			r.ScanType = MultiScanCountReq
			r.Distinct = req.GetDistinct()
		}

	case *protobuf.ScanRequest:
		r.DefnID = req.GetDefnID()
		r.RequestId = req.GetRequestId()
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
		setIndexParams()
		setConsistency(cons, vector)
		if proj != nil {
			if r.Indexprojection, err = validateIndexProjection(proj, len(r.IndexInst.Defn.SecExprs)); err != nil {
				return
			}
			r.projectPrimaryKey = *proj.PrimaryKey
		}
		fillRanges(
			req.GetSpan().GetRange().GetLow(),
			req.GetSpan().GetRange().GetHigh(),
			req.GetSpan().GetEquals())
		fillScans(req.GetScans())

	case *protobuf.ScanAllRequest:
		r.DefnID = req.GetDefnID()
		r.RequestId = req.GetRequestId()
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

		setIndexParams()
		setConsistency(cons, vector)
	default:
		err = ErrUnsupportedRequest
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

// Before starting the index scan, we have to find out the snapshot timestamp
// that can fullfil this query by considering atleast-timestamp provided in
// the query request. A timestamp request message is sent to the storage
// manager. The storage manager will respond immediately if a snapshot
// is available, otherwise it will wait until a matching snapshot is
// available and return the timestamp. Util then, the query processor
// will block wait.
// This mechanism can be used to implement RYOW.
func (s *scanCoordinator) getRequestedIndexSnapshot(r *ScanRequest) (snap IndexSnapshot, err error) {

	snapshot, err := func() (IndexSnapshot, error) {
		s.mu.RLock()
		defer s.mu.RUnlock()

		ss, ok := s.lastSnapshot[r.IndexInstId]
		cons := *r.Consistency
		if ok && ss != nil && isSnapshotConsistent(ss, cons, r.Ts) {
			return CloneIndexSnapshot(ss), nil
		}
		return nil, nil
	}()

	if err != nil {
		return nil, err
	} else if snapshot != nil {
		return snapshot, nil
	}

	snapResch := make(chan interface{}, 1)
	snapReqMsg := &MsgIndexSnapRequest{
		ts:          r.Ts,
		cons:        *r.Consistency,
		respch:      snapResch,
		idxInstId:   r.IndexInstId,
		expiredTime: r.ExpiredTime,
	}

	// Block wait until a ts is available for fullfilling the request
	s.supvMsgch <- snapReqMsg
	var msg interface{}
	select {
	case msg = <-snapResch:
	case <-r.getTimeoutCh():
		go readDeallocSnapshot(snapResch)
		msg = common.ErrScanTimedOut
	}

	switch msg.(type) {
	case IndexSnapshot:
		snap = msg.(IndexSnapshot)
	case error:
		err = msg.(error)
	}

	return
}

func isSnapshotConsistent(
	ss IndexSnapshot, cons common.Consistency, reqTs *common.TsVbuuid) bool {

	if snapTs := ss.Timestamp(); snapTs != nil {
		if cons == common.QueryConsistency && snapTs.AsRecent(reqTs) {
			return true
		} else if cons == common.SessionConsistency {
			if ss.IsEpoch() && reqTs.IsEpoch() {
				return true
			}
			if snapTs.CheckCrc64(reqTs) && snapTs.AsRecentTs(reqTs) {
				return true
			}
			// don't return error because client might be ahead of
			// in receiving a rollback.
			// return nil, ErrVbuuidMismatch
			return false
		} else if cons == common.AnyConsistency {
			return true
		}
	}
	return false
}

func (s *scanCoordinator) respondWithError(conn net.Conn, req *ScanRequest, err error) {
	var res interface{}

	buf := p.GetBlock()
	defer p.PutBlock(buf)

	protoErr := &protobuf.Error{Error: proto.String(err.Error())}

	switch req.ScanType {
	case StatsReq:
		res = &protobuf.StatisticsResponse{
			Err: protoErr,
		}
	case CountReq:
		res = &protobuf.CountResponse{
			Count: proto.Int64(0), Err: protoErr,
		}
	case ScanAllReq, ScanReq:
		res = &protobuf.ResponseStream{
			Err: protoErr,
		}
	}

	err2 := protobuf.EncodeAndWrite(conn, *buf, res)
	if err2 != nil {
		err = fmt.Errorf("%s, %s", err, err2)
		goto finish
	}
	err2 = protobuf.EncodeAndWrite(conn, *buf, &protobuf.StreamEndResponse{})
	if err2 != nil {
		err = fmt.Errorf("%s, %s", err, err2)
	}

finish:
	logging.Errorf("%s RESPONSE Failed with error (%s), requestId: %v", req.LogPrefix, err, req.RequestId)
}

func (s *scanCoordinator) handleError(prefix string, err error) {
	if err != nil {
		logging.Errorf("%s Error occured %s", prefix, err)
	}
}

func (s *scanCoordinator) tryRespondWithError(w ScanResponseWriter, req *ScanRequest, err error) bool {
	if err != nil {
		if err == common.ErrIndexNotReady && req.Stats != nil {
			req.Stats.notReadyError.Add(1)
		} else if err == common.ErrIndexNotFound {
			stats := s.stats.Get()
			stats.notFoundError.Add(1)
		} else if err == common.ErrIndexerInBootstrap {
			logging.Verbosef("%s REQUEST %s", req.LogPrefix, req)
			logging.Verbosef("%s RESPONSE status:(error = %s), requestId: %v", req.LogPrefix, err, req.RequestId)
		} else {
			logging.Infof("%s REQUEST %s", req.LogPrefix, req)
			logging.Infof("%s RESPONSE status:(error = %s), requestId: %v", req.LogPrefix, err, req.RequestId)
		}
		s.handleError(req.LogPrefix, w.Error(err))
		return true
	}

	return false
}

func (s *scanCoordinator) serverCallback(protoReq interface{}, conn net.Conn,
	cancelCh <-chan bool) {

	ttime := time.Now()

	req, err := s.newRequest(protoReq, cancelCh)
	defer func() {
		if req.ctx != nil {
			req.ctx.Done()
		}
	}()

	atime := time.Now()
	w := NewProtoWriter(req.ScanType, conn)
	defer func() {
		s.handleError(req.LogPrefix, w.Done())
		req.Done()
	}()

	if req.ScanType == HeloReq {
		s.handleHeloRequest(req, w)
		return
	}

	logging.Verbosef("%s REQUEST %s", req.LogPrefix, req)

	if req.Consistency != nil {
		logging.LazyVerbose(func() string {
			return fmt.Sprintf("%s requested timestamp: %s => %s Crc64 => %v", req.LogPrefix,
				strings.ToLower(req.Consistency.String()), ScanTStoString(req.Ts), req.Ts.GetCrc64())
		})
	}

	if s.tryRespondWithError(w, req, err) {
		return
	}

	req.Stats.scanReqAllocDuration.Add(time.Now().Sub(atime).Nanoseconds())

	if err := s.isScanAllowed(*req.Consistency); err != nil {
		s.tryRespondWithError(w, req, err)
		return
	}

	req.Stats.numRequests.Add(1)

	req.Stats.scanReqInitDuration.Add(time.Now().Sub(ttime).Nanoseconds())

	t0 := time.Now()
	is, err := s.getRequestedIndexSnapshot(req)
	if s.tryRespondWithError(w, req, err) {
		return
	}

	defer DestroyIndexSnapshot(is)

	logging.LazyVerbose(func() string {
		return fmt.Sprintf("%s snapshot timestamp: %s",
			req.LogPrefix, ScanTStoString(is.Timestamp()))
	})

	defer func() {
		req.Stats.scanReqDuration.Add(time.Now().Sub(ttime).Nanoseconds())
	}()

	s.processRequest(req, w, is, t0)
}

func (s *scanCoordinator) processRequest(req *ScanRequest, w ScanResponseWriter,
	is IndexSnapshot, t0 time.Time) {

	switch req.ScanType {
	case ScanReq, ScanAllReq:
		s.handleScanRequest(req, w, is, t0)
	case CountReq:
		s.handleCountRequest(req, w, is, t0)
	case MultiScanCountReq:
		s.handleMultiScanCountRequest(req, w, is, t0)
	case StatsReq:
		s.handleStatsRequest(req, w, is)
	}
}

func (s *scanCoordinator) handleHeloRequest(req *ScanRequest, w ScanResponseWriter) {
	err := w.Helo()
	s.handleError(req.LogPrefix, err)
}

func (s *scanCoordinator) handleScanRequest(req *ScanRequest, w ScanResponseWriter,
	is IndexSnapshot, t0 time.Time) {
	waitTime := time.Now().Sub(t0)

	scanPipeline := NewScanPipeline(req, w, is)
	cancelCb := NewCancelCallback(req, func(e error) {
		scanPipeline.Cancel(e)
	})
	cancelCb.Run()
	defer cancelCb.Done()

	err := scanPipeline.Execute()
	scanTime := time.Now().Sub(t0)

	req.Stats.numRowsReturned.Add(int64(scanPipeline.RowsReturned()))
	req.Stats.scanBytesRead.Add(int64(scanPipeline.BytesRead()))
	req.Stats.scanDuration.Add(scanTime.Nanoseconds())
	req.Stats.scanWaitDuration.Add(waitTime.Nanoseconds())

	if err != nil {
		status := fmt.Sprintf("(error = %s)", err)
		logging.LazyVerbose(func() string {
			return fmt.Sprintf("%s RESPONSE rows:%d, waitTime:%v, totalTime:%v, status:%s, requestId:%s",
				req.LogPrefix, scanPipeline.RowsReturned(), waitTime, scanTime, status, req.RequestId)
		})

		if err == common.ErrClientCancel {
			req.Stats.clientCancelError.Add(1)
		}
	} else {
		status := "ok"
		logging.LazyVerbose(func() string {
			return fmt.Sprintf("%s RESPONSE rows:%d, waitTime:%v, totalTime:%v, status:%s",
				req.LogPrefix, scanPipeline.RowsReturned(), waitTime, scanTime, status)
		})
	}
}

func (s *scanCoordinator) handleCountRequest(req *ScanRequest, w ScanResponseWriter,
	is IndexSnapshot, t0 time.Time) {
	var rows uint64
	var err error

	stopch := make(StopChannel)
	cancelCb := NewCancelCallback(req, func(e error) {
		err = e
		close(stopch)
	})
	cancelCb.Run()
	defer cancelCb.Done()

	for _, s := range GetSliceSnapshots(is) {
		var r uint64
		snap := s.Snapshot()
		if len(req.Keys) > 0 {
			r, err = snap.CountLookup(req.ctx, req.Keys, stopch)
		} else if req.Low.Bytes() == nil && req.High.Bytes() == nil {
			r, err = snap.CountTotal(req.ctx, stopch)
		} else {
			r, err = snap.CountRange(req.ctx, req.Low, req.High, req.Incl, stopch)
		}

		if err != nil {
			break
		}

		rows += r
	}

	if s.tryRespondWithError(w, req, err) {
		return
	}

	logging.Verbosef("%s RESPONSE count:%d status:ok", req.LogPrefix, rows)
	err = w.Count(rows)
	s.handleError(req.LogPrefix, err)
}

func (s *scanCoordinator) handleMultiScanCountRequest(req *ScanRequest, w ScanResponseWriter,
	is IndexSnapshot, t0 time.Time) {
	var rows uint64
	var err error
	stopch := make(StopChannel)
	cancelCb := NewCancelCallback(req, func(e error) {
		err = e
		close(stopch)
	})
	cancelCb.Run()
	defer cancelCb.Done()

	for _, scan := range req.Scans {
		for _, s := range GetSliceSnapshots(is) {
			var r uint64
			snap := s.Snapshot()
			if scan.ScanType == AllReq {
				r, err = snap.MultiScanCount(req.ctx, MinIndexKey, MaxIndexKey, Both, scan, req.Distinct, stopch)
			} else if scan.ScanType == LookupReq || scan.ScanType == RangeReq ||
				scan.ScanType == FilterRangeReq {
				r, err = snap.MultiScanCount(req.ctx, scan.Low, scan.High, scan.Incl, scan, req.Distinct, stopch)
			}

			if err != nil {
				break
			}

			rows += r
		}
	}

	if s.tryRespondWithError(w, req, err) {
		return
	}

	logging.Verbosef("%s RESPONSE count:%d status:ok", req.LogPrefix, rows)
	err = w.Count(rows)
	s.handleError(req.LogPrefix, err)
}

func (s *scanCoordinator) handleStatsRequest(req *ScanRequest, w ScanResponseWriter,
	is IndexSnapshot) {
	var rows uint64
	var err error

	stopch := make(StopChannel)
	cancelCb := NewCancelCallback(req, func(e error) {
		err = e
		close(stopch)
	})
	cancelCb.Run()
	defer cancelCb.Done()

	for _, s := range GetSliceSnapshots(is) {
		var r uint64
		snap := s.Snapshot()
		if req.Low.Bytes() == nil && req.Low.Bytes() == nil {
			r, err = snap.StatCountTotal()
		} else {
			r, err = snap.CountRange(req.ctx, req.Low, req.High, req.Incl, stopch)
		}

		if err != nil {
			break
		}

		rows += r
	}

	if s.tryRespondWithError(w, req, err) {
		return
	}

	logging.Verbosef("%s RESPONSE status:ok", req.LogPrefix)
	err = w.Stats(rows, 0, nil, nil)
	s.handleError(req.LogPrefix, err)
}

// Find and return data structures for the specified index
func (s *scanCoordinator) findIndexInstance(
	defnID uint64) (*common.IndexInst, IndexReaderContext, error) {

	for _, inst := range s.indexInstMap {
		if inst.Defn.DefnId == common.IndexDefnId(defnID) {
			if pmap, ok := s.indexPartnMap[inst.InstId]; ok {
				ctx := pmap[0].Sc.GetSliceById(0).GetReaderContext()

				return &inst, ctx, nil
			}
			return nil, nil, ErrNotMyIndex
		}
	}
	return nil, nil, common.ErrIndexNotFound
}

func (s *scanCoordinator) handleUpdateIndexInstMap(cmd Message) {
	s.mu.Lock()
	defer s.mu.Unlock()

	req := cmd.(*MsgUpdateInstMap)
	logging.Tracef("ScanCoordinator::handleUpdateIndexInstMap %v", cmd)
	indexInstMap := req.GetIndexInstMap()
	s.stats.Set(req.GetStatsObject())
	s.indexInstMap = common.CopyIndexInstMap(indexInstMap)

	s.supvCmdch <- &MsgSuccess{}
}

func (s *scanCoordinator) handleUpdateIndexPartnMap(cmd Message) {
	s.mu.Lock()
	defer s.mu.Unlock()

	logging.Tracef("ScanCoordinator::handleUpdateIndexPartnMap %v", cmd)
	indexPartnMap := cmd.(*MsgUpdatePartnMap).GetIndexPartnMap()
	s.indexPartnMap = CopyIndexPartnMap(indexPartnMap)

	s.supvCmdch <- &MsgSuccess{}
}

func (s *scanCoordinator) handleConfigUpdate(cmd Message) {
	cfgUpdate := cmd.(*MsgConfigUpdate)
	s.config.Store(cfgUpdate.GetConfig())
	s.supvCmdch <- &MsgSuccess{}
}

func (s *scanCoordinator) handleIndexerPause(cmd Message) {
	s.setIndexerState(common.INDEXER_PAUSED)
	s.supvCmdch <- &MsgSuccess{}

}

func (s *scanCoordinator) handleIndexerResume(cmd Message) {
	s.setIndexerState(common.INDEXER_ACTIVE)

	s.supvCmdch <- &MsgSuccess{}
}

func (s *scanCoordinator) handleIndexerBootstrap(cmd Message) {
	s.setIndexerState(common.INDEXER_BOOTSTRAP)
	s.supvCmdch <- &MsgSuccess{}
}

func (s *scanCoordinator) getItemsCount(instId common.IndexInstId) (uint64, error) {
	var count uint64

	snapResch := make(chan interface{}, 1)
	snapReqMsg := &MsgIndexSnapRequest{
		cons:      common.AnyConsistency,
		respch:    snapResch,
		idxInstId: instId,
	}

	s.supvMsgch <- snapReqMsg
	msg := <-snapResch

	// Index snapshot is not available yet (non-active index or empty index)
	if msg == nil {
		return 0, nil
	}

	var is IndexSnapshot

	switch msg.(type) {
	case IndexSnapshot:
		is = msg.(IndexSnapshot)
		if is == nil {
			return 0, nil
		}
		defer DestroyIndexSnapshot(is)
	case error:
		return 0, msg.(error)
	}

	for _, ps := range is.Partitions() {
		for _, ss := range ps.Slices() {
			snap := ss.Snapshot()
			c, err := snap.StatCountTotal()
			if err != nil {
				return 0, err
			}
			count += c
		}
	}

	return count, nil
}

// Helper method to pretty print timestamp
func ScanTStoString(ts *common.TsVbuuid) string {
	var seqsStr string = "["

	if ts != nil {
		for i, s := range ts.Seqnos {
			if i > 0 {
				seqsStr += ","
			}
			seqsStr += fmt.Sprintf("%d=%d", i, s)
		}
	}

	seqsStr += "]"

	return seqsStr
}

func readDeallocSnapshot(ch chan interface{}) {
	msg := <-ch
	if msg == nil {
		return
	}

	var is IndexSnapshot
	switch msg.(type) {
	case IndexSnapshot:
		is = msg.(IndexSnapshot)
		if is == nil {
			return
		}

		DestroyIndexSnapshot(is)
	}
}

func (s *scanCoordinator) isScanAllowed(c common.Consistency) error {
	if s.getIndexerState() == common.INDEXER_PAUSED {
		cfg := s.config.Load()
		allow_scan_when_paused := cfg["allow_scan_when_paused"].Bool()

		if c != common.AnyConsistency {
			return errors.New(fmt.Sprintf("Indexer Cannot Service %v Scan In Paused State", c.String()))
		} else if !allow_scan_when_paused {
			return errors.New(fmt.Sprintf("Indexer Cannot Service Scan In Paused State"))
		} else {
			return nil
		}
	}

	return nil
}

func (s *scanCoordinator) isBootstrapMode() bool {
	return s.getIndexerState() == common.INDEXER_BOOTSTRAP
}

func bucketSeqsWithRetry(retries int, logPrefix, cluster, bucket string) (seqnos []uint64, err error) {
	fn := func(r int, err error) error {
		if r > 0 {
			logging.Errorf("%s BucketSeqnos(%s): failed with error (%v)...Retrying (%d)",
				logPrefix, bucket, err, r)
		}
		seqnos, err = common.BucketSeqnos(cluster, "default", bucket)
		return err
	}

	rh := common.NewRetryHelper(retries, time.Second, 1, fn)
	err = rh.Run()
	return
}
