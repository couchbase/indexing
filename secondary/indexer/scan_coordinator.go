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
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/protobuf"
	"github.com/couchbase/indexing/secondary/queryport"
	"github.com/couchbaselabs/goprotobuf/proto"
	"sync"
	"sync/atomic"
)

// TODO:
// 1. Add distinct unsupported error
// 2. Add unique count unsupported error

// Errors
var (
	ErrUnsupportedRequest = errors.New("Unsupported query request")
	ErrIndexNotFound      = errors.New("Index not found")
	ErrNotMyIndex         = errors.New("Not my index")
	ErrIndexNotReady      = errors.New("Index not ready")
	ErrInternal           = errors.New("Internal server error occured")
)

type scanType string

const (
	queryStats   scanType = "stats"
	queryScan    scanType = "scan"
	queryScanAll scanType = "scanall"
)

// Internal scan handle for a request
type scanDescriptor struct {
	scanId    uint64
	p         *scanParams
	isPrimary bool
	stopch    StopChannel

	respch chan interface{}
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

	str := fmt.Sprintf("scan id: %v, index: %v/%v, type: %v, span: %s", sd.scanId,
		sd.p.bucket, sd.p.indexName, sd.p.scanType, span)

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

// Streaming scan results reader helper
// Used for:
// - Reading batched entries of page size from scan res stream
// - To apply limit clause on streaming scan results
// - To perform graceful termination of stream scanning
type scanStreamReader struct {
	sd      *scanDescriptor
	keysBuf *[]Key
	bufSize int64
	count   int64
	hasNext bool
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
		resp, r.hasNext = <-r.sd.respch
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

func (r *scanStreamReader) Done() {
	r.hasNext = false
	close(r.sd.stopch)

	// Drain any leftover responses when client requests for graceful
	// termination
	go func() {
		for {
			_, closed := <-r.sd.respch
			if closed {
				break
			}
		}
	}()
}

//TODO
//For any query request, check if the replica is available. And use replica in case
//its more recent or serving less queries.

//ScanCoordinator handles scanning for an incoming index query. It will figure out
//the partitions/slices required to be scanned as per query parameters.

type ScanCoordinator interface {
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
}

// NewScanCoordinator returns an instance of scanCoordinator or err message
// It listens on supvCmdch for command and every command is followed
// by a synchronous response on the supvCmdch.
// Any async message to supervisor is sent to supvMsgch.
// If supvCmdch get closed, ScanCoordinator will shut itself down.
func NewScanCoordinator(supvCmdch MsgChannel, supvMsgch MsgChannel) (
	ScanCoordinator, Message) {
	var err error

	s := &scanCoordinator{
		supvCmdch: supvCmdch,
		supvMsgch: supvMsgch,
		logPrefix: "ScanCoordinator",
	}

	s.serv, err = queryport.NewServer(QUERY_PORT_ADDR, s.requestHandler,
		common.SystemConfig)

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

func (s *scanCoordinator) run() {
loop:
	for {
		select {
		case cmd, ok := <-s.supvCmdch:
			if ok {
				if cmd.GetMsgType() == SCAN_COORD_SHUTDOWN {
					common.Infof("ScanCoordinator: Shutting Down")
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

	default:
		common.Errorf("ScanCoordinator: Received Unknown Command %v", cmd)
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

	switch req.(type) {
	case *protobuf.StatisticsRequest:
		r := req.(*protobuf.StatisticsRequest)
		p.scanType = queryStats
		p.incl = Inclusion(r.GetSpan().GetRange().GetInclusion())
		err = fillRanges(
			r.GetSpan().GetRange().GetLow(),
			r.GetSpan().GetRange().GetHigh(),
			r.GetSpan().GetEqual())
		p.indexName = r.GetIndexName()
		p.bucket = r.GetBucket()
	case *protobuf.ScanRequest:
		r := req.(*protobuf.ScanRequest)
		p.scanType = queryScan
		p.incl = Inclusion(r.GetSpan().GetRange().GetInclusion())
		err = fillRanges(
			r.GetSpan().GetRange().GetLow(),
			r.GetSpan().GetRange().GetHigh(),
			r.GetSpan().GetEqual())
		p.limit = r.GetLimit()
		p.indexName = r.GetIndexName()
		p.bucket = r.GetBucket()
		p.pageSize = r.GetPageSize()
	case *protobuf.ScanAllRequest:
		p.scanType = queryScanAll
		r := req.(*protobuf.ScanAllRequest)
		p.limit = r.GetLimit()
		p.indexName = r.GetIndexName()
		p.bucket = r.GetBucket()
		p.pageSize = r.GetPageSize()
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
	var partnInstMap *PartitionInstMap

	p, err := s.parseScanParams(req)
	if err == ErrUnsupportedRequest {
		// TODO: Add error response for invalid queryport reqs
		panic(err)
	}

	scanId := atomic.AddUint64(&s.reqCounter, 1)
	sd := &scanDescriptor{
		scanId: scanId,
		p:      p,
		stopch: make(StopChannel),
		respch: make(chan interface{}),
	}

	if err == nil {
		indexInst, partnInstMap, err = s.getIndexDS(p.indexName, p.bucket)
	}

	if err == nil && indexInst.State != common.INDEX_STATE_ACTIVE {
		err = ErrIndexNotReady
	}

	if err != nil {
		common.Infof("%v: SCAN_REQ: %v, Error (%v)", s.logPrefix, sd, err)
		respch <- s.makeResponseMessage(sd, err)
		close(respch)
		return
	}

	// Its a primary index scan
	sd.isPrimary = indexInst.Defn.IsPrimary

	common.Infof("%v: SCAN_REQ %v", s.logPrefix, sd)
	// Before starting the index scan, we have to find out the snapshot timestamp
	// that can fullfil this query by considering atleast-timestamp provided in
	// the query request. A timestamp request message is sent to the storage
	// manager. The storage manager will respond immediately if a snapshot
	// is available, otherwise it will wait until a matching snapshot is
	// available and return the timestamp. Util then, the query processor
	// will block wait.
	// This mechanism can be used to implement RYOW.

	tsch := make(chan interface{}, 1)
	tsReqMsg := &MsgTSRequest{
		ts:        sd.p.ts,
		respch:    tsch,
		idxInstId: indexInst.InstId,
	}

	// Block wait until a ts is available for fullfilling the request
	s.supvMsgch <- tsReqMsg
	msg := <-tsch

	switch msg.(type) {
	case *common.TsVbuuid:
		sd.p.ts = msg.(*common.TsVbuuid)
	case error:
		respch <- s.makeResponseMessage(sd, msg.(error))
		close(respch)
		return
	}

	common.Infof("%v: SCAN_ID: %v scan timestamp: %v", s.logPrefix, sd.scanId, ScanTStoString(sd.p.ts))
	// Index has no scannable snapshot available
	if sd.p.ts == nil {
		close(respch)
		return
	}

	partnDefs := s.findPartitionDefsForScan(sd, indexInst)
	go s.scanPartitions(sd, partnDefs, partnInstMap)

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

	case queryScan:
		fallthrough
	case queryScanAll:
		var keys *[]Key
		var msg interface{}
		var done bool

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
				msg = s.makeResponseMessage(sd, err)
			} else {
				msg = s.makeResponseMessage(sd, keys)
			}

			// Send protobuf message response to queryport
			select {
			case _, ok := <-quitch:
				if !ok {
					rdr.Done()
					break loop
				}
			case respch <- msg:
			}
		}
		close(respch)
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

	pKey := tmp[l-1]
	pKeyBytes, err = json.Marshal(pKey)
	if err != nil {
		panic("corruption detected " + err.Error())
	}

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
					Count:      proto.Uint64(0),
					UniqueKeys: proto.Uint64(0),
					Min:        []byte{},
					Max:        []byte{},
				},
				Err: protoErr,
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
		r = &protobuf.ResponseStream{Entries: entries}
	case statsResponse:
		stats := payload.(statsResponse)
		r = &protobuf.StatisticsResponse{
			Stats: &protobuf.IndexStatistics{
				Count:      proto.Uint64(stats.count),
				UniqueKeys: proto.Uint64(stats.unique),
				Min:        stats.min.Raw(),
				Max:        stats.max.Raw(),
			},
		}
	}
	return
}

// Find and return data structures for the specified index
func (s *scanCoordinator) getIndexDS(indexName, bucket string) (indexInst *common.IndexInst,
	partnInstMap *PartitionInstMap, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, inst := range s.indexInstMap {
		if inst.Defn.Name == indexName && inst.Defn.Bucket == bucket {
			indexInst = &inst
			if pmap, ok := s.indexPartnMap[inst.InstId]; ok {
				partnInstMap = &pmap
				return
			}

			err = ErrNotMyIndex
			return
		}
	}

	err = ErrIndexNotFound
	return
}

// Get defs of necessary partitions required for serving the scan request
func (s *scanCoordinator) findPartitionDefsForScan(sd *scanDescriptor,
	indexInst *common.IndexInst) []common.PartitionDefn {

	var partnDefs []common.PartitionDefn

	// Scan request has specified the specific partition to be scanned
	if string(sd.p.partnKey) != "" {
		id := indexInst.Pc.GetPartitionIdByPartitionKey(sd.p.partnKey)
		partnDefs = []common.PartitionDefn{indexInst.Pc.GetPartitionById(id)}
	} else {
		partnDefs = indexInst.Pc.GetAllPartitions()
	}

	return partnDefs
}

func (s *scanCoordinator) isLocalEndpoint(endpoint common.Endpoint) bool {
	// TODO: Detect local endpoint correctly
	// Since the current indexer supports only single partition, this assumption
	// holds true
	return true
}

// Scan entries from the target partitions for index query
// Scan will be distributed across all the endpoints of the target partitions
// Scan entries/errors are written back into sd.respch channel
func (s *scanCoordinator) scanPartitions(sd *scanDescriptor,
	partDefs []common.PartitionDefn, partnInstMap *PartitionInstMap) {
	// TODO: Multiple partition scanner needs a stream merger/stats reducer to
	// work with multiple partitions and slices.
	common.Debugf("%v: scanParitions: SCAN_ID: %v partitions: %v", s.logPrefix,
		sd.scanId, partDefs)

	var wg sync.WaitGroup
	var workerStopChannels []StopChannel

	for _, partnDefn := range partDefs {
		for _, endpoint := range partnDefn.Endpoints() {
			wg.Add(1)
			stopch := make(StopChannel)
			workerStopChannels = append(workerStopChannels, stopch)
			id := partnDefn.GetPartitionId()
			if s.isLocalEndpoint(endpoint) {
				// run local scan for local partition
				go s.scanLocalPartitionEndpoint(sd, id, partnInstMap, stopch, &wg)
			} else {
				go s.scanRemotePartitionEndpoint(sd, endpoint, id, stopch, &wg)
			}
		}
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
		common.Tracef("ScanCoordinator: %s: Waiting for workers to finish.", debugStr)
		wg.Wait()
		//send signal on channel to indicate all workers have finished
		common.Tracef("ScanCoordinator: %s: All workers finished", debugStr)
		close(allWorkersDoneCh)
	}()

	//wait for upstream to signal stop or for all workers to signal done
	select {
	case <-stopch:
		common.Debugf("ScanCoordinator: %s: Stopping All Workers.", debugStr)
		//stop all workers
		for _, ch := range workerStopChannels {
			close(ch)
		}
		//wait for all workers to stop
		<-allWorkersDoneCh
		common.Debugf("ScanCoordinator: %s: Stopped All Workers.", debugStr)

		//wait for notification of all workers finishing
	case <-allWorkersDoneCh:

	}

}

// Locate the slices for the local partition endpoint and scan them
func (s *scanCoordinator) scanLocalPartitionEndpoint(sd *scanDescriptor,
	partnId common.PartitionId, partnInstMap *PartitionInstMap, stopch StopChannel,
	wg *sync.WaitGroup) {

	var partnInst PartitionInst
	var ok bool

	defer wg.Done()
	common.Debugf("%v: scanLocalParitionEndpoint: SCAN_ID: %v partition: %v",
		s.logPrefix, sd.scanId, partnId)

	// TODO: Crash or return error to the client ?
	if partnInst, ok = (*partnInstMap)[partnId]; !ok {
		panic("Partition cannot be found in partition instance map")
	}

	var workerWg sync.WaitGroup
	var workerStopChannels []StopChannel

	sliceList := partnInst.Sc.GetAllSlices()

	for _, slice := range sliceList {
		workerWg.Add(1)
		workerStopCh := make(StopChannel)
		workerStopChannels = append(workerStopChannels, workerStopCh)
		go s.scanLocalSlice(sd, slice, workerStopCh, &workerWg)
	}

	s.monitorWorkers(&workerWg, stopch, workerStopChannels, "scanLocalPartition")
}

func (s *scanCoordinator) scanRemotePartitionEndpoint(sd *scanDescriptor,
	endpoint common.Endpoint,
	partnId common.PartitionId, stopch StopChannel,
	wg *sync.WaitGroup) {

	defer wg.Done()
	panic("not implemented")
}

// Scan a snapshot from a local slice
// Snapshot to be scanned is determined by query parameters
func (s *scanCoordinator) scanLocalSlice(sd *scanDescriptor,
	slice Slice, stopch StopChannel, wg *sync.WaitGroup) {
	var snap Snapshot

	defer wg.Done()
	common.Debugf("%v: scanLocalSlice: SCAN_ID: %v Slice : %v",
		s.logPrefix, sd.scanId, slice.Id())

	snapContainer := slice.GetSnapshotContainer()
	if sd.p.ts == nil {
		snap = snapContainer.GetLatestSnapshot()
	} else {
		snap = snapContainer.GetSnapshotEqualToTS(sd.p.ts)
	}

	if snap != nil {
		s.executeLocalScan(sd, snap, stopch)
	} else {
		common.Infof("%v: SCAN_ID: %v Slice: %v Error (No snapshot available for scan)",
			s.logPrefix, sd.scanId, slice.Id())
	}
}

// Executes the actual scan of the snapshot
// Scan can be stopped anytime by closing the stop channel
func (s *scanCoordinator) executeLocalScan(sd *scanDescriptor, snap Snapshot, stopch StopChannel) {
	switch sd.p.scanType {
	case queryStats:
		s.statsQuery(sd, snap, stopch)
	case queryScan:
		s.scanQuery(sd, snap, stopch)
	case queryScanAll:
		s.scanAllQuery(sd, snap, stopch)
	}
}

func (s *scanCoordinator) statsQuery(sd *scanDescriptor, snap Snapshot, stopch StopChannel) {
	totalRows, err := snap.CountRange(sd.p.low, sd.p.high, sd.p.incl, stopch)
	// TODO: Implement min, max, unique (maybe)
	if err != nil {
		sd.respch <- err
	} else {
		min, _ := NewKey([]byte("min"))
		max, _ := NewKey([]byte("max"))
		sd.respch <- statsResponse{count: totalRows, min: min, max: max}
	}
}

func (s *scanCoordinator) scanQuery(sd *scanDescriptor, snap Snapshot, stopch StopChannel) {
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

func (s *scanCoordinator) scanAllQuery(sd *scanDescriptor, snap Snapshot, stopch StopChannel) {
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
				common.Tracef("%v: SCAN_ID: %v Received key: %v)",
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

	common.Infof("ScanCoordinator::handleUpdateIndexInstMap %v", cmd)
	indexInstMap := cmd.(*MsgUpdateInstMap).GetIndexInstMap()
	s.indexInstMap = common.CopyIndexInstMap(indexInstMap)

	s.supvCmdch <- &MsgSuccess{}
}

func (s *scanCoordinator) handleUpdateIndexPartnMap(cmd Message) {
	s.mu.Lock()
	defer s.mu.Unlock()

	common.Infof("ScanCoordinator::handleUpdateIndexPartnMap %v", cmd)
	indexPartnMap := cmd.(*MsgUpdatePartnMap).GetIndexPartnMap()
	s.indexPartnMap = CopyIndexPartnMap(indexPartnMap)

	s.supvCmdch <- &MsgSuccess{}
}

// Helper method to pretty print timestamp
func ScanTStoString(ts *common.TsVbuuid) string {
	var seqsStr string = "["

	if ts != nil {
		for i, s := range ts.Snapshots {
			if i > 0 {
				seqsStr += ","
			}
			seqsStr += fmt.Sprintf("%d=%d", i, s[1])
		}
	}

	seqsStr += "]"

	return seqsStr
}
