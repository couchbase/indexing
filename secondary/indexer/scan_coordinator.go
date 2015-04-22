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
	"code.google.com/p/goprotobuf/proto"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/query"
	"github.com/couchbase/indexing/secondary/queryport"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// TODO:
// 1. Add distinct unsupported error
// 2. Add unique count unsupported error

// Errors
var (
	// Error strings for ErrIndexNotFound and ErrIndexNotReady need to be
	// in sync with errors defined in queryport/n1ql/secondary_index.go
	ErrIndexNotFound      = errors.New("Index not found")
	ErrIndexNotReady      = errors.New("Index not ready for serving queries")
	ErrNotMyIndex         = errors.New("Not my index")
	ErrInternal           = errors.New("Internal server error occured")
	ErrSnapNotAvailable   = errors.New("No snapshot available for scan")
	ErrScanTimedOut       = errors.New("Index scan timed out")
	ErrUnsupportedRequest = errors.New("Unsupported query request")
)

type scanType string

const (
	queryStats   scanType = "stats"
	queryCount   scanType = "count"
	queryScan    scanType = "scan"
	queryScanAll scanType = "scanall"
)

// Internal scan handle for a request
type scanDescriptor struct {
	scanId    uint64
	p         *scanParams
	isPrimary bool
	stopch    StopChannel
	timeoutch <-chan time.Time

	respch chan interface{}
}

func (sd *scanDescriptor) ParseIndexInstance(indexInst *common.IndexInst) {
	sd.isPrimary = indexInst.Defn.IsPrimary
	sd.p.indexName, sd.p.bucket = indexInst.Defn.Name, indexInst.Defn.Bucket
	if sd.p.ts != nil {
		sd.p.ts.Bucket = sd.p.bucket
	}
}

func (sd scanDescriptor) String() string {
	var incl, span string

	switch sd.p.incl {
	case Low:
		incl = "incl:low"
	case High:
		incl = "incl:high"
	case Both:
		incl = "incl:both"
	default:
		incl = "incl:none"
	}

	if len(sd.p.keys) == 0 {
		if sd.p.scanType == queryStats || sd.p.scanType == queryScan {
			span = fmt.Sprintf("range (%s,%s %s)", string(sd.p.low.Raw()),
				string(sd.p.high.Raw()), incl)
		} else {
			span = "all"
		}
	} else {
		span = "keys ( "
		for _, k := range sd.p.keys {
			span = span + string(k.Raw()) + " "
		}
		span = span + ")"
	}

	str := fmt.Sprintf("scan id: %v, defnId: %v, index: %v/%v, type: %v, span: %s", sd.scanId,
		sd.p.defnID, sd.p.bucket, sd.p.indexName, sd.p.scanType, span)

	if sd.p.pageSize > 0 {
		str += fmt.Sprintf(" pagesize: %d", sd.p.pageSize)
	}

	if sd.p.limit > 0 {
		str += fmt.Sprintf(" limit: %d", sd.p.limit)
	}

	return str
}

type scanParams struct {
	scanType  scanType
	defnID    uint64
	indexName string
	bucket    string
	ts        *common.TsVbuuid
	low       Key
	high      Key
	keys      []Key
	partnKey  []byte
	incl      Inclusion
	limit     int64
	pageSize  int64
}

type statsResponse struct {
	min, max Key
	unique   uint64
	count    uint64
}

type countResponse struct {
	count int64
}

// Streaming scan results reader helper
// Used for:
// - Reading batched entries of page size from scan res stream
// - To apply limit clause on streaming scan results
// - To perform graceful termination of stream scanning
type scanStreamReader struct {
	sd        *scanDescriptor
	keysBuf   *[]Key
	bufSize   int64
	count     int64
	bytesRead int64
	hasNext   bool
}

func newResponseReader(sd *scanDescriptor) *scanStreamReader {
	r := new(scanStreamReader)
	r.sd = sd
	r.keysBuf = new([]Key)
	r.hasNext = true
	r.bufSize = 0
	return r
}

// Read a chunk of keys from scan results with a maximum batch size equals page size
func (r *scanStreamReader) ReadKeyBatch() (keys *[]Key, done bool, err error) {
	var resp interface{}
	done = false

loop:
	for r.hasNext {
		select {
		case resp, r.hasNext = <-r.sd.respch:
		case <-r.sd.timeoutch:
			// Since error is set as response, cleanup happens in the following
			// code block by calling r.Done()
			resp = ErrScanTimedOut
		}
		if r.hasNext {
			switch resp.(type) {
			case Key:
				// Limit constraint
				if r.sd.p.limit > 0 && r.sd.p.limit == r.count {
					r.Done()
					break loop
				}

				k := resp.(Key)
				sz := int64(len(k.Raw()))
				r.bytesRead += sz
				// Page size constraint
				if r.bufSize > 0 && r.bufSize+sz > r.sd.p.pageSize {
					keys = r.keysBuf
					r.bufSize = sz
					r.keysBuf = new([]Key)
					*r.keysBuf = append(*r.keysBuf, k)
					r.count++
					return
				}

				r.bufSize += sz
				*r.keysBuf = append(*r.keysBuf, k)
				r.count++
			case error:
				err = resp.(error)
				r.Done()
				return
			}
		}
	}

	// No more item left to be read from buffer
	if len(*r.keysBuf) == 0 {
		done = true
	}

	keys = r.keysBuf
	r.keysBuf = new([]Key)

	return
}

func (r *scanStreamReader) ReadStat() (stat statsResponse, err error) {
	resp := <-r.sd.respch
	switch resp.(type) {
	case statsResponse:
		stat = resp.(statsResponse)
	case error:
		err = resp.(error)
	}
	return
}

func (r *scanStreamReader) ReadCount() (count countResponse, err error) {
	resp := <-r.sd.respch
	switch val := resp.(type) {
	case countResponse:
		return val, nil
	case error:
		return count, val
	}
	return count, err
}

func (r *scanStreamReader) Done() {
	r.hasNext = false
	if r.sd.stopch != nil {
		close(r.sd.stopch)
		r.sd.stopch = nil
	}

	// Drain any leftover responses when client requests for graceful
	// termination
	go func() {
		for {
			_, ok := <-r.sd.respch
			if !ok {
				break
			}
		}
	}()
}

func (r *scanStreamReader) ReturnedRows() uint64 {
	return uint64(r.count)
}

func (r *scanStreamReader) ReturnedBytes() uint64 {
	return uint64(r.bytesRead)
}

//TODO
//For any query request, check if the replica is available. And use replica in case
//its more recent or serving less queries.

//ScanCoordinator handles scanning for an incoming index query. It will figure out
//the partitions/slices required to be scanned as per query parameters.

type ScanCoordinator interface {
}

type indexScanStats struct {
	Requests  *uint64
	Rows      *uint64
	BytesRead *uint64
	ScanTime  *uint64
	WaitTime  *uint64
}

type scanCoordinator struct {
	supvCmdch  MsgChannel //supervisor sends commands on this channel
	supvMsgch  MsgChannel //channel to send any async message to supervisor
	serv       *queryport.Server
	logPrefix  string
	reqCounter uint64

	mu            sync.RWMutex
	indexInstMap  common.IndexInstMap
	indexPartnMap IndexPartnMap

	config common.ConfigHolder

	scanStatsMap map[common.IndexInstId]indexScanStats
}

// NewScanCoordinator returns an instance of scanCoordinator or err message
// It listens on supvCmdch for command and every command is followed
// by a synchronous response on the supvCmdch.
// Any async message to supervisor is sent to supvMsgch.
// If supvCmdch get closed, ScanCoordinator will shut itself down.
func NewScanCoordinator(supvCmdch MsgChannel, supvMsgch MsgChannel,
	config common.Config) (ScanCoordinator, Message) {
	var err error

	s := &scanCoordinator{
		supvCmdch:    supvCmdch,
		supvMsgch:    supvMsgch,
		logPrefix:    "ScanCoordinator",
		scanStatsMap: make(map[common.IndexInstId]indexScanStats),
	}

	s.config.Store(config)

	addr := net.JoinHostPort("", config["scanPort"].String())
	queryportCfg := config.SectionConfig("queryport.", true)
	s.serv, err = queryport.NewServer(addr, s.requestHandler, queryportCfg)

	if err != nil {
		errMsg := &MsgError{err: Error{code: ERROR_SCAN_COORD_QUERYPORT_FAIL,
			severity: FATAL,
			category: SCAN_COORD,
			cause:    err,
		},
		}
		return nil, errMsg
	}

	// main loop
	go s.run()

	return s, &MsgSuccess{}

}

func (s *scanCoordinator) handleStats(cmd Message) {
	s.supvCmdch <- &MsgSuccess{}

	statsMap := make(map[string]interface{})
	req := cmd.(*MsgStatsRequest)
	replych := req.GetReplyChannel()
	var instList []common.IndexInst
	s.mu.RLock()
	defer s.mu.RUnlock()

	for instId, stat := range s.scanStatsMap {
		inst := s.indexInstMap[instId]
		//skip deleted indexes
		if inst.State == common.INDEX_STATE_DELETED {
			continue
		}

		instList = append(instList, inst)

		k := fmt.Sprintf("%s:%s:num_requests", inst.Defn.Bucket, inst.Defn.Name)
		v := atomic.LoadUint64(stat.Requests)
		statsMap[k] = v
		k = fmt.Sprintf("%s:%s:num_rows_returned", inst.Defn.Bucket, inst.Defn.Name)
		v = atomic.LoadUint64(stat.Rows)
		statsMap[k] = v
		k = fmt.Sprintf("%s:%s:scan_bytes_read", inst.Defn.Bucket, inst.Defn.Name)
		v = atomic.LoadUint64(stat.BytesRead)
		statsMap[k] = v
		k = fmt.Sprintf("%s:%s:total_scan_duration", inst.Defn.Bucket, inst.Defn.Name)
		v = atomic.LoadUint64(stat.ScanTime)
		statsMap[k] = v
		k = fmt.Sprintf("%s:%s:scan_wait_duration", inst.Defn.Bucket, inst.Defn.Name)
		v = atomic.LoadUint64(stat.WaitTime)
		statsMap[k] = v

		st := s.serv.Statistics()
		statsMap["num_connections"] = st.Connections
	}

	// Compute counts asynchronously and reply to stats request
	go func() {
		for _, inst := range instList {
			c, err := s.getItemsCount(inst.InstId)
			if err == nil {
				k := fmt.Sprintf("%s:%s:items_count", inst.Defn.Bucket, inst.Defn.Name)
				statsMap[k] = c
			} else {
				logging.Errorf("%v: Unable compute index count for %v/%v (%v)", s.logPrefix,
					inst.Defn.Bucket, inst.Defn.Name, err)
			}
		}
		replych <- statsMap
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

	default:
		logging.Errorf("ScanCoordinator: Received Unknown Command %v", cmd)
		s.supvCmdch <- &MsgError{
			err: Error{code: ERROR_SCAN_COORD_UNKNOWN_COMMAND,
				severity: NORMAL,
				category: SCAN_COORD}}
	}

}

// Parse scan params from queryport request
func (s *scanCoordinator) parseScanParams(
	req interface{}) (p *scanParams, err error) {

	p = new(scanParams)
	p.partnKey = []byte("default")

	fillRanges := func(low, high []byte, keys [][]byte) error {
		var err error
		var key Key

		// range
		if p.low, err = NewKey(low); err != nil {
			msg := fmt.Sprintf("Invalid low key %s (%s)", string(low), err.Error())
			return errors.New(msg)
		}

		if p.high, err = NewKey(high); err != nil {
			msg := fmt.Sprintf("Invalid high key %s (%s)", string(high), err.Error())
			return errors.New(msg)
		}

		// point query for keys
		for _, k := range keys {
			if key, err = NewKey(k); err != nil {
				msg := fmt.Sprintf("Invalid equal key %s (%s)", string(k), err.Error())
				return errors.New(msg)
			}
			p.keys = append(p.keys, key)
		}

		return nil
	}

	setConsistency := func(cons common.Consistency, vector *protobuf.TsConsistency) {
		checkVector :=
			cons == common.QueryConsistency || cons == common.SessionConsistency
		if checkVector && vector != nil {
			cfg := s.config.Load()
			p.ts = common.NewTsVbuuid("", cfg["numVbuckets"].Int())
			for i, vbno := range vector.Vbnos {
				p.ts.Seqnos[vbno] = vector.Seqnos[i]
				p.ts.Vbuuids[vbno] = vector.Vbuuids[i]
			}
		}
	}

	switch r := req.(type) {
	case *protobuf.StatisticsRequest:
		p.scanType = queryStats
		p.incl = Inclusion(r.GetSpan().GetRange().GetInclusion())
		err = fillRanges(
			r.GetSpan().GetRange().GetLow(),
			r.GetSpan().GetRange().GetHigh(),
			r.GetSpan().GetEquals())
		p.defnID = r.GetDefnID()
	case *protobuf.CountRequest:
		p.scanType = queryCount
		p.incl = Inclusion(r.GetSpan().GetRange().GetInclusion())
		p.defnID = r.GetDefnID()
		err = fillRanges(
			r.GetSpan().GetRange().GetLow(),
			r.GetSpan().GetRange().GetHigh(),
			r.GetSpan().GetEquals())
		cons := common.Consistency(r.GetCons())
		vector := r.GetVector()
		setConsistency(cons, vector)
	case *protobuf.ScanRequest:
		p.scanType = queryScan
		p.incl = Inclusion(r.GetSpan().GetRange().GetInclusion())
		err = fillRanges(
			r.GetSpan().GetRange().GetLow(),
			r.GetSpan().GetRange().GetHigh(),
			r.GetSpan().GetEquals())
		p.limit = r.GetLimit()
		p.defnID = r.GetDefnID()
		p.pageSize = r.GetPageSize()
		cons := common.Consistency(r.GetCons())
		vector := r.GetVector()
		setConsistency(cons, vector)
	case *protobuf.ScanAllRequest:
		p.scanType = queryScanAll
		p.limit = r.GetLimit()
		p.defnID = r.GetDefnID()
		p.pageSize = r.GetPageSize()
		cons := common.Consistency(r.GetCons())
		vector := r.GetVector()
		setConsistency(cons, vector)
	default:
		err = ErrUnsupportedRequest
	}

	return
}

// Handle query requests arriving through queryport
func (s *scanCoordinator) requestHandler(
	req interface{},
	respch chan<- interface{},
	quitch <-chan interface{}) {

	var indexInst *common.IndexInst

	p, err := s.parseScanParams(req)
	if err == ErrUnsupportedRequest {
		// TODO: Add error response for invalid queryport reqs
		panic(err)
	}

	scanId := atomic.AddUint64(&s.reqCounter, 1)
	cfg := s.config.Load()
	timeout := time.Millisecond * time.Duration(cfg["settings.scan_timeout"].Int())
	startTime := time.Now()
	sd := &scanDescriptor{
		scanId:    scanId,
		p:         p,
		stopch:    make(StopChannel),
		respch:    make(chan interface{}),
		timeoutch: time.After(timeout),
	}

	if err == nil {
		if indexInst, err = s.findIndexInstance(p.defnID); err == nil {
			sd.ParseIndexInstance(indexInst)
		}
	}

	if err == nil && indexInst.State != common.INDEX_STATE_ACTIVE {
		err = ErrIndexNotReady
	}

	if err != nil {
		logging.Infof("%v: SCAN_REQ: %v, Error (%v)", s.logPrefix, sd, err)
		respch <- s.makeResponseMessage(sd, err)
		close(respch)
		return
	}

	// Update statistics
	s.mu.RLock()
	atomic.AddUint64(s.scanStatsMap[indexInst.InstId].Requests, 1)
	s.mu.RUnlock()

	logging.Infof("%v: SCAN_REQ %v", s.logPrefix, sd)
	// Before starting the index scan, we have to find out the snapshot timestamp
	// that can fullfil this query by considering atleast-timestamp provided in
	// the query request. A timestamp request message is sent to the storage
	// manager. The storage manager will respond immediately if a snapshot
	// is available, otherwise it will wait until a matching snapshot is
	// available and return the timestamp. Util then, the query processor
	// will block wait.
	// This mechanism can be used to implement RYOW.

	snapResch := make(chan interface{}, 1)
	snapReqMsg := &MsgIndexSnapRequest{
		ts:        sd.p.ts,
		respch:    snapResch,
		idxInstId: indexInst.InstId,
	}

	logging.Debugf("%v: SCAN_ID: %v requested timestamp: %v",
		s.logPrefix, sd.scanId, ScanTStoString(sd.p.ts))
	// Block wait until a ts is available for fullfilling the request
	s.supvMsgch <- snapReqMsg
	var msg interface{}
	select {
	case msg = <-snapResch:
	case <-sd.timeoutch:
		go readDeallocSnapshot(snapResch)
		msg = ErrScanTimedOut
	}

	var snap IndexSnapshot
	var ts *common.TsVbuuid

	switch msg.(type) {
	case IndexSnapshot:
		snap = msg.(IndexSnapshot)
		if snap != nil {
			ts = snap.Timestamp()
		}
	case error:
		err := msg.(error)
		respch <- s.makeResponseMessage(sd, err)
		logging.Infof("%v: SCAN_REQ: %v, Error (%v)", s.logPrefix, sd, err)
		close(respch)
		return
	}

	waitDuration := time.Now().Sub(startTime)

	logging.Infof("%v: SCAN_ID: %v scan timestamp: %v",
		s.logPrefix, sd.scanId, ScanTStoString(ts))
	// Index has no scannable snapshot available
	if snap == nil {
		close(respch)
		return
	}

	go s.scanIndexSnapshot(sd, snap)

	rdr := newResponseReader(sd)
	switch sd.p.scanType {
	case queryStats:
		var msg interface{}
		stat, err := rdr.ReadStat()
		if err != nil {
			msg = s.makeResponseMessage(sd, err)
		} else {
			msg = s.makeResponseMessage(sd, stat)
		}

		respch <- msg
		close(respch)

	case queryCount:
		var msg interface{}
		count, err := rdr.ReadCount()
		if err != nil {
			msg = s.makeResponseMessage(sd, err)
		} else {
			msg = s.makeResponseMessage(sd, count)
		}

		respch <- msg
		close(respch)

	case queryScan:
		fallthrough
	case queryScanAll:
		var keys *[]Key
		var msg interface{}
		var done bool
		var reqquit bool = false
		var status string

		// Read scan entries and send it to the client
		// Closing respch indicates that we have no more messages to be sent
	loop:
		for {
			keys, done, err = rdr.ReadKeyBatch()
			// We have already finished reading from response stream
			if done {
				break loop
			}

			if err != nil {
				if err == ErrScanTimedOut {
					logging.Warnf("%v: SCAN_ID: %v scan request timed out in index db reads",
						s.logPrefix, sd.scanId)
				}
				msg = s.makeResponseMessage(sd, err)
			} else {
				msg = s.makeResponseMessage(sd, keys)
			}

			// Send protobuf message response to queryport
			select {
			case _, ok := <-quitch:
				if !ok {
					reqquit = true
					rdr.Done()
					break loop
				}
			case respch <- msg:
			}

			if err != nil {
				break loop
			}
		}
		close(respch)
		if reqquit {
			status = "client requested quit"
		} else if err != nil {
			status = "error occured " + err.Error()
		} else {
			status = "successful"
		}

		s.mu.RLock()
		atomic.AddUint64(s.scanStatsMap[indexInst.InstId].Rows, rdr.ReturnedRows())
		atomic.AddUint64(s.scanStatsMap[indexInst.InstId].BytesRead, rdr.ReturnedBytes())
		atomic.AddUint64(s.scanStatsMap[indexInst.InstId].ScanTime, uint64(time.Now().Sub(startTime).Nanoseconds()))
		atomic.AddUint64(s.scanStatsMap[indexInst.InstId].WaitTime, uint64(waitDuration.Nanoseconds()))
		s.mu.RUnlock()
		logging.Infof("%v: SCAN_RESULT scan id: %v rows: %v finished scan (%s)", s.logPrefix, sd.scanId, rdr.ReturnedRows(), status)
	}
}

func ProtoIndexEntryFromKey(k Key, isPrimary bool) *protobuf.IndexEntry {
	// TODO: Return error instead of panic
	var tmp []interface{}
	var err error
	var secKeyBytes, pKeyBytes []byte

	kbytes := k.Raw()
	err = json.Unmarshal(kbytes, &tmp)
	if err != nil {
		panic("corruption detected " + string(kbytes) + " " + err.Error())
	}

	l := len(tmp)
	if l == 0 || (isPrimary == false && l == 1) {
		panic("corruption detected")
	}

	if isPrimary == true {
		secKeyBytes = []byte{}
	} else {
		secKey := tmp[:l-1]
		secKeyBytes, err = json.Marshal(secKey)
		if err != nil {
			panic("corruption detected " + err.Error())
		}
	}

	// Primary key should be in raw bytes
	pKeyBytes = []byte(tmp[l-1].(string))
	entry := &protobuf.IndexEntry{
		EntryKey: secKeyBytes, PrimaryKey: pKeyBytes,
	}

	return entry
}

// Create a queryport response message
// Response message can be StreamResponse or StatisticsResponse
func (s *scanCoordinator) makeResponseMessage(sd *scanDescriptor,
	payload interface{}) (r interface{}) {

	switch payload.(type) {
	case error:
		err := payload.(error)
		protoErr := &protobuf.Error{Error: proto.String(err.Error())}
		switch sd.p.scanType {
		case queryStats:
			r = &protobuf.StatisticsResponse{
				Stats: &protobuf.IndexStatistics{
					KeysCount:       proto.Uint64(0),
					UniqueKeysCount: proto.Uint64(0),
					KeyMin:          []byte{},
					KeyMax:          []byte{},
				},
				Err: protoErr,
			}
		case queryCount:
			r = &protobuf.CountResponse{
				Count: proto.Int64(0), Err: protoErr,
			}
		case queryScan:
			fallthrough
		case queryScanAll:
			r = &protobuf.ResponseStream{
				Err: protoErr,
			}
		}
	case *[]Key:
		var entries []*protobuf.IndexEntry
		keys := *payload.(*[]Key)
		for _, k := range keys {
			entry := ProtoIndexEntryFromKey(k, sd.isPrimary)
			entries = append(entries, entry)
		}
		r = &protobuf.ResponseStream{IndexEntries: entries}
	case statsResponse:
		stats := payload.(statsResponse)
		r = &protobuf.StatisticsResponse{
			Stats: &protobuf.IndexStatistics{
				KeysCount:       proto.Uint64(stats.count),
				UniqueKeysCount: proto.Uint64(stats.unique),
				KeyMin:          stats.min.Raw(),
				KeyMax:          stats.max.Raw(),
			},
		}
	case countResponse:
		counts := payload.(countResponse)
		r = &protobuf.CountResponse{Count: proto.Int64(counts.count)}
	}
	return
}

// Find and return data structures for the specified index
func (s *scanCoordinator) findIndexInstance(
	defnID uint64) (*common.IndexInst, error) {

	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, inst := range s.indexInstMap {
		if inst.Defn.DefnId == common.IndexDefnId(defnID) {
			if _, ok := s.indexPartnMap[inst.InstId]; ok {
				return &inst, nil
			}
			return nil, ErrNotMyIndex
		}
	}
	return nil, ErrIndexNotFound
}

// Scan entries from the target partitions from index snapshot
// Scan entries/errors are written back into sd.respch channel
func (s *scanCoordinator) scanIndexSnapshot(sd *scanDescriptor, snap IndexSnapshot) {
	// TODO: Multiple partition scanner needs a stream merger/stats reducer to
	// work with multiple partitions and slices.
	logging.Debugf("%v: scanIndexSnapshot: SCAN_ID: %v instance_id: %v",
		s.logPrefix, sd.scanId, snap.IndexInstId())

	var wg sync.WaitGroup
	var workerStopChannels []StopChannel

	for _, ps := range snap.Partitions() {
		wg.Add(1)
		stopch := make(StopChannel)
		workerStopChannels = append(workerStopChannels, stopch)
		go s.scanPartitionSnapshot(sd, ps, stopch, &wg)
	}

	s.monitorWorkers(&wg, sd.stopch, workerStopChannels, "scanPartitions")
	// We have no more responses to be sent
	close(sd.respch)
}

// Waits for the provided workers to finish and return
// It also listens to the stop channel and if that gets closed, all workers
// are stopped using workerStopChannels. Once all workers stop, the
// method retuns.
func (s *scanCoordinator) monitorWorkers(wg *sync.WaitGroup,
	stopch StopChannel, workerStopChannels []StopChannel, debugStr string) {

	allWorkersDoneCh := make(DoneChannel)

	//wait for all workers to finish
	go func() {
		logging.Tracef("ScanCoordinator: %s: Waiting for workers to finish.", debugStr)
		wg.Wait()
		//send signal on channel to indicate all workers have finished
		logging.Tracef("ScanCoordinator: %s: All workers finished", debugStr)
		close(allWorkersDoneCh)
	}()

	//wait for upstream to signal stop or for all workers to signal done
	select {
	case <-stopch:
		logging.Debugf("ScanCoordinator: %s: Stopping All Workers.", debugStr)
		//stop all workers
		for _, ch := range workerStopChannels {
			close(ch)
		}
		//wait for all workers to stop
		<-allWorkersDoneCh
		logging.Debugf("ScanCoordinator: %s: Stopped All Workers.", debugStr)

		//wait for notification of all workers finishing
	case <-allWorkersDoneCh:

	}

}

func (s *scanCoordinator) scanPartitionSnapshot(sd *scanDescriptor,
	snap PartitionSnapshot, stopch StopChannel, wg *sync.WaitGroup) {

	defer wg.Done()
	logging.Debugf("%v: scanPartitionSnapshot: SCAN_ID: %v partition: %v",
		s.logPrefix, sd.scanId, snap.PartitionId())

	var workerWg sync.WaitGroup
	var workerStopChannels []StopChannel

	for _, sliceSnap := range snap.Slices() {
		workerWg.Add(1)
		workerStopCh := make(StopChannel)
		workerStopChannels = append(workerStopChannels, workerStopCh)
		go s.scanSliceSnapshot(sd, sliceSnap, workerStopCh, &workerWg)
	}

	s.monitorWorkers(&workerWg, stopch, workerStopChannels, "scanPartitionSnapshot")
}

func (s *scanCoordinator) scanSliceSnapshot(sd *scanDescriptor,
	ss SliceSnapshot, stopch StopChannel, wg *sync.WaitGroup) {

	defer wg.Done()
	logging.Debugf("%v: scanLocalSlice: SCAN_ID: %v Slice : %v",
		s.logPrefix, sd.scanId, ss.SliceId())

	switch sd.p.scanType {
	case queryStats:
		s.queryStats(sd, ss.Snapshot(), stopch)
	case queryCount:
		s.queryCount(sd, ss.Snapshot(), stopch)
	case queryScan:
		s.queryScan(sd, ss.Snapshot(), stopch)
	case queryScanAll:
		s.queryScanAll(sd, ss.Snapshot(), stopch)

	}

	// Top level request handler may go away even before scan worker go-routine is died.
	// Hence snapshot cleanup should be done at scan worker level
	ss.Snapshot().Close()
}

func (s *scanCoordinator) queryStats(sd *scanDescriptor, snap Snapshot, stopch StopChannel) {
	var totalRows uint64
	var err error

	if sd.p.low.IsNull() && sd.p.high.IsNull() && sd.p.incl == Both {
		totalRows, err = snap.StatCountTotal()
	} else {
		totalRows, err = snap.CountRange(sd.p.low, sd.p.high, sd.p.incl, stopch)
	}

	// TODO: Implement min, max, unique (maybe)
	if err != nil {
		sd.respch <- err
	} else {
		min, _ := NewKey([]byte("min"))
		max, _ := NewKey([]byte("max"))
		sd.respch <- statsResponse{count: totalRows, min: min, max: max}
	}
}

func (s *scanCoordinator) queryCount(sd *scanDescriptor, snap Snapshot, stopch StopChannel) {
	p := sd.p
	lowkey, highkey := p.low.Encoded(), p.high.Encoded()
	if p.keys != nil && len(p.keys) > 0 { // handle lookup counts
		allCounts := uint64(0)
		for _, key := range p.keys {
			count, err := snap.CountRange(key, key, Both, stopch)
			if err != nil {
				sd.respch <- err
				break
			}
			allCounts += count
		}
		sd.respch <- countResponse{count: int64(allCounts)}

	} else if lowkey != nil || highkey != nil { // handle range counts
		count, err := snap.CountRange(p.low, p.high, p.incl, stopch)
		if err != nil {
			sd.respch <- err
		}
		sd.respch <- countResponse{count: int64(count)}

	} else { // handle full total
		count, err := snap.CountTotal(stopch)
		if err != nil {
			sd.respch <- err
		} else {
			sd.respch <- countResponse{count: int64(count)}
		}
	}
}

func (s *scanCoordinator) queryScan(sd *scanDescriptor, snap Snapshot, stopch StopChannel) {
	// TODO: Decide whether a missing response should be provided point query for keys
	if len(sd.p.keys) != 0 {
		for _, k := range sd.p.keys {
			ch, cherr, _ := snap.KeyRange(k, k, Both, stopch)
			s.receiveKeys(sd, ch, cherr)
		}
	} else {
		ch, cherr, _ := snap.KeyRange(sd.p.low, sd.p.high, sd.p.incl, stopch)
		s.receiveKeys(sd, ch, cherr)
	}

}

func (s *scanCoordinator) queryScanAll(sd *scanDescriptor, snap Snapshot, stopch StopChannel) {
	ch, cherr := snap.KeySet(stopch)
	s.receiveKeys(sd, ch, cherr)
}

// receiveKeys receives results/errors from snapshot reader and forwards it to
// the caller till the result channel is closed by the snapshot reader
func (s *scanCoordinator) receiveKeys(sd *scanDescriptor, chkey chan Key, cherr chan error) {
	ok := true
	var key Key
	var err error

	for ok {
		select {
		case key, ok = <-chkey:
			if ok {
				logging.Tracef("%v: SCAN_ID: %v Received key: %v)",
					s.logPrefix, sd.scanId, string(key.Raw()))
				sd.respch <- key
			}
		case err, _ = <-cherr:
			if err != nil {
				sd.respch <- err
			}
		}
	}
}

func (s *scanCoordinator) handleUpdateIndexInstMap(cmd Message) {
	s.mu.Lock()
	defer s.mu.Unlock()

	logging.Tracef("ScanCoordinator::handleUpdateIndexInstMap %v", cmd)
	indexInstMap := cmd.(*MsgUpdateInstMap).GetIndexInstMap()
	s.indexInstMap = common.CopyIndexInstMap(indexInstMap)

	// Remove invalid indexes
	for instId, _ := range s.scanStatsMap {
		if _, ok := s.indexInstMap[instId]; !ok {
			delete(s.scanStatsMap, instId)
		}
	}

	// Add newly added indexes
	for instId, _ := range s.indexInstMap {
		if _, ok := s.scanStatsMap[instId]; !ok {
			s.scanStatsMap[instId] = indexScanStats{
				Requests:  new(uint64),
				Rows:      new(uint64),
				BytesRead: new(uint64),
				ScanTime:  new(uint64),
				WaitTime:  new(uint64),
			}
		}
	}

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

func (s *scanCoordinator) getItemsCount(instId common.IndexInstId) (uint64, error) {
	var count uint64

	snapResch := make(chan interface{}, 1)
	snapReqMsg := &MsgIndexSnapRequest{
		ts:        nil,
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
