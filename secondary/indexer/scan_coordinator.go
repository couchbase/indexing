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
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

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
	ErrNotMyPartition     = errors.New("Not my partition")
)

var secKeyBufPool *common.BytesBufPool

func init() {
	secKeyBufPool = common.NewByteBufferPool(maxSecKeyBufferLen + ENCODE_BUF_SAFE_PAD)
}

type ScanCoordinator interface {
}

type scanCoordinator struct {
	supvCmdch        MsgChannel //supervisor sends commands on this channel
	supvMsgch        MsgChannel //channel to send any async message to supervisor
	snapshotNotifych chan IndexSnapshot
	lastSnapshot     map[common.IndexInstId]IndexSnapshot
	rollbackTimes    unsafe.Pointer

	rollbackInProgress unsafe.Pointer

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
	s.initRollbackInProgress()

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

	case INDEXER_ROLLBACK:
		s.handleIndexerRollback(cmd)

	default:
		logging.Errorf("ScanCoordinator: Received Unknown Command %v", cmd)
		s.supvCmdch <- &MsgError{
			err: Error{code: ERROR_SCAN_COORD_UNKNOWN_COMMAND,
				severity: NORMAL,
				category: SCAN_COORD}}
	}

}

/////////////////////////////////////////////////////////////////////////
//
//  scan handler
//
/////////////////////////////////////////////////////////////////////////

func (s *scanCoordinator) serverCallback(protoReq interface{}, conn net.Conn,
	cancelCh <-chan bool) {

	ttime := time.Now()

	req, err := NewScanRequest(protoReq, cancelCh, s)
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

	logging.LazyVerbose(func() string {
		return fmt.Sprintf("%s REQUEST %s", req.LogPrefix, logging.TagStrUD(req))
	})

	if req.Consistency != nil {
		logging.LazyVerbose(func() string {
			return fmt.Sprintf("%s requested timestamp: %s => %s Crc64 => %v", req.LogPrefix,
				strings.ToLower(req.Consistency.String()), ScanTStoString(req.Ts), req.Ts.GetCrc64())
		})
	}

	if s.tryRespondWithError(w, req, err) {
		return
	}

	if req.Stats != nil {
		req.Stats.scanReqAllocDuration.Add(time.Now().Sub(atime).Nanoseconds())
	}

	if err := s.isScanAllowed(*req.Consistency, req); err != nil {
		s.tryRespondWithError(w, req, err)
		return
	}

	if req.Stats != nil {
		req.Stats.scanReqInitDuration.Add(time.Now().Sub(ttime).Nanoseconds())

		for _, partitionId := range req.PartitionIds {
			req.Stats.updatePartitionStats(partitionId,
				func(stats *IndexStats) {
					stats.numRequests.Add(1)
					if req.GroupAggr != nil {
						stats.numRequestsAggr.Add(1)
					} else {
						stats.numRequestsRange.Add(1)
					}
				})
		}
	}

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
		if req.Stats != nil {
			req.Stats.scanReqDuration.Add(time.Now().Sub(ttime).Nanoseconds())
		}
	}()

	if len(req.Ctxs) != 0 {
		for _, ctx := range req.Ctxs {
			ctx.Init()
		}
	}

	s.processRequest(req, w, is, t0)

	if len(req.Ctxs) != 0 {
		for _, ctx := range req.Ctxs {
			ctx.Done()
		}
	}

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

	scanPipeline := NewScanPipeline(req, w, is, s.config.Load())
	cancelCb := NewCancelCallback(req, func(e error) {
		scanPipeline.Cancel(e)
	})
	cancelCb.Run()
	defer cancelCb.Done()

	err := scanPipeline.Execute()
	scanTime := time.Now().Sub(t0)

	if req.Stats != nil {
		req.Stats.numRowsReturned.Add(int64(scanPipeline.RowsReturned()))
		req.Stats.scanBytesRead.Add(int64(scanPipeline.BytesRead()))
		req.Stats.scanDuration.Add(scanTime.Nanoseconds())
		req.Stats.scanWaitDuration.Add(waitTime.Nanoseconds())

		if req.GroupAggr != nil {
			req.Stats.numRowsReturnedAggr.Add(int64(scanPipeline.RowsReturned()))
			req.Stats.numRowsScannedAggr.Add(int64(scanPipeline.RowsScanned()))
			req.Stats.scanCacheHitAggr.Add(int64(scanPipeline.CacheHitRatio()))
			req.Stats.Timings.n1qlExpr.Put(scanPipeline.AvgExprEvalDur())
		} else {
			req.Stats.numRowsReturnedRange.Add(int64(scanPipeline.RowsReturned()))
			req.Stats.numRowsScannedRange.Add(int64(scanPipeline.RowsScanned()))
			req.Stats.scanCacheHitRange.Add(int64(scanPipeline.CacheHitRatio()))
		}
	}

	if err != nil {
		status := fmt.Sprintf("(error = %s)", err)
		logging.LazyVerbose(func() string {
			return fmt.Sprintf("%s RESPONSE rows:%d, scanned:%d, waitTime:%v, totalTime:%v, status:%s, requestId:%s",
				req.LogPrefix, scanPipeline.RowsReturned(), scanPipeline.RowsScanned(), waitTime, scanTime, status, req.RequestId)
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
	var snapshots []SliceSnapshot

	stopch := make(StopChannel)
	cancelCb := NewCancelCallback(req, func(e error) {
		err = e
		close(stopch)
	})
	cancelCb.Run()
	defer cancelCb.Done()

	if snapshots, err = GetSliceSnapshots(is, req.PartitionIds); err == nil {
		rows, err = scatterCount(req, snapshots, stopch)
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
	var snapshots []SliceSnapshot

	stopch := make(StopChannel)
	cancelCb := NewCancelCallback(req, func(e error) {
		err = e
		close(stopch)
	})
	cancelCb.Run()
	defer cancelCb.Done()

	if snapshots, err = GetSliceSnapshots(is, req.PartitionIds); err == nil {
		previousRows := make([][]byte, len(snapshots))
		for i := 0; i < len(previousRows); i++ {
			buf := secKeyBufPool.Get()
			req.keyBufList = append(req.keyBufList, buf)
			previousRows[i] = (*buf)[:0]
			req.Ctxs[i].SetCursorKey(&previousRows[i])
		}
		for _, scan := range req.Scans {
			r, err1 := scatterMultiCount(req, scan, snapshots, previousRows, stopch)
			if err1 != nil {
				err = err1
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
	var snapshots []SliceSnapshot

	stopch := make(StopChannel)
	cancelCb := NewCancelCallback(req, func(e error) {
		err = e
		close(stopch)
	})
	cancelCb.Run()
	defer cancelCb.Done()

	if snapshots, err = GetSliceSnapshots(is, req.PartitionIds); err == nil {
		rows, err = scatterStats(req, snapshots, stopch)
	}

	if s.tryRespondWithError(w, req, err) {
		return
	}

	logging.Verbosef("%s RESPONSE status:ok", req.LogPrefix)
	err = w.Stats(rows, 0, nil, nil)
	s.handleError(req.LogPrefix, err)
}

/////////////////////////////////////////////////////////////////////////
//
//  scan helpers
//
/////////////////////////////////////////////////////////////////////////

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

func (s *scanCoordinator) isScanAllowed(c common.Consistency, scan *ScanRequest) error {
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

	if scan.rollbackTime == 0 {
		return nil
	}

	rollbackTimes := (*map[string]int64)(atomic.LoadPointer(&s.rollbackTimes))
	if rollbackTimes == nil {
		logging.Errorf("ScanCoordinator.isScanAllowed: rollback time not initialized")
		return ErrIndexRollbackOrBootstrap
	}

	rollbackTime, ok := (*rollbackTimes)[scan.Bucket]
	if !ok {
		logging.Errorf("ScanCoordinator.isScanAllowed: missing rollback time for bucket %v", scan.Bucket)
		return ErrIndexRollbackOrBootstrap
	}

	if scan.rollbackTime != rollbackTime {
		logging.Errorf("ScanCoordinator.isScanAllowed: rollback time mismatch. Req %v indexer %v", scan.rollbackTime, rollbackTime)
		return ErrIndexRollbackOrBootstrap
	}

	return nil
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

/////////////////////////////////////////////////////////////////////////
//
//  supervisor message handlers
//
/////////////////////////////////////////////////////////////////////////

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
			err := s.updateItemsCount(id, idxStats)
			if err != nil {
				logging.Errorf("%v: Unable compute index count for %v/%v (%v)", s.logPrefix,
					idxStats.bucket, idxStats.name, err)
			}

			// compute scan rate
			now := time.Now().UnixNano()
			elapsed := float64(now-idxStats.lastScanGatherTime.Value()) / float64(time.Second)
			if elapsed > 60 {
				partitions := idxStats.getPartitions()
				for _, pid := range partitions {
					partnStats := idxStats.getPartitionStats(pid)
					numRowsScanned := partnStats.numRowsScanned.Value()
					scanRate := float64(numRowsScanned-partnStats.lastNumRowsScanned.Value()) / elapsed
					partnStats.avgScanRate.Set(int64((scanRate + float64(partnStats.avgScanRate.Value())) / 2))
					partnStats.lastNumRowsScanned.Set(numRowsScanned)

					logging.Debugf("scanCoordinator.handleStats: index %v partition %v numRowsScanned %v scan rate %v avg scan rate %v",
						id, pid, numRowsScanned, scanRate, partnStats.avgScanRate.Value())

					idxStats.lastScanGatherTime.Set(now)
				}
			}
		}
		replych <- true
	}()
}

func (s *scanCoordinator) handleUpdateIndexInstMap(cmd Message) {
	s.mu.Lock()
	defer s.mu.Unlock()

	req := cmd.(*MsgUpdateInstMap)
	logging.Tracef("ScanCoordinator::handleUpdateIndexInstMap %v", cmd)
	indexInstMap := req.GetIndexInstMap()
	s.stats.Set(req.GetStatsObject())
	s.indexInstMap = common.CopyIndexInstMap(indexInstMap)

	if len(req.GetRollbackTimes()) != 0 {
		logging.Infof("ScanCoordinator::initialize rollback times on new index inst map: %v", req.GetRollbackTimes())
		s.initRollbackTimes(req.GetRollbackTimes())
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

func (s *scanCoordinator) handleIndexerPause(cmd Message) {
	s.setIndexerState(common.INDEXER_PAUSED)
	s.supvCmdch <- &MsgSuccess{}

}

func (s *scanCoordinator) handleIndexerResume(cmd Message) {

	msg := cmd.(*MsgIndexerState)
	rollbackTimes := msg.GetRollbackTimes()
	if len(rollbackTimes) != 0 {
		logging.Infof("ScanCoordinator::initialize rollback times on indexer resume: %v", rollbackTimes)
		s.initRollbackTimes(rollbackTimes)
	}

	s.setIndexerState(common.INDEXER_ACTIVE)

	s.supvCmdch <- &MsgSuccess{}
}

func (s *scanCoordinator) handleIndexerBootstrap(cmd Message) {
	s.setIndexerState(common.INDEXER_BOOTSTRAP)
	s.supvCmdch <- &MsgSuccess{}
}

func (s *scanCoordinator) handleIndexerRollback(cmd Message) {

	msg := cmd.(*MsgRollback)

	if msg.rollbackTime != 0 {
		s.saveRollbackTime(msg.bucket, msg.rollbackTime)
		s.setRollbackInProgress(msg.bucket, true)
	} else {
		s.setRollbackInProgress(msg.bucket, false)
	}

	s.supvCmdch <- &MsgSuccess{}
}

/////////////////////////////////////////////////////////////////////////
//
//  supervisor message handler helpers
//
/////////////////////////////////////////////////////////////////////////

func (s *scanCoordinator) getIndexerState() common.IndexerState {
	return s.indexerState.Load().(common.IndexerState)
}

func (s *scanCoordinator) setIndexerState(state common.IndexerState) {
	s.indexerState.Store(state)
}

func (s *scanCoordinator) cloneRollbackTimes() map[string]int64 {

	newTime := make(map[string]int64)
	oldTime := (*map[string]int64)(atomic.LoadPointer(&s.rollbackTimes))
	if oldTime != nil {
		for bucket, rollbackTime := range *oldTime {
			newTime[bucket] = rollbackTime
		}
	}

	return newTime
}

func (s *scanCoordinator) saveRollbackTime(bucket string, rollbackTime int64) {

	logging.Infof("ScanCoordinator::saveRollbackTime: bucket %v time %v", bucket, rollbackTime)
	newTime := s.cloneRollbackTimes()
	newTime[bucket] = rollbackTime
	atomic.StorePointer(&s.rollbackTimes, unsafe.Pointer(&newTime))
}

func (s *scanCoordinator) initRollbackTimes(rollbackTimes map[string]int64) {

	newTimes := make(map[string]int64)
	for bucket, rollbackTime := range rollbackTimes {
		newTimes[bucket] = rollbackTime
	}
	atomic.StorePointer(&s.rollbackTimes, unsafe.Pointer(&newTimes))
}

func (s *scanCoordinator) initRollbackInProgress() {

	rollbackInProgress := make(map[string]*bool)
	atomic.StorePointer(&s.rollbackInProgress, unsafe.Pointer(&rollbackInProgress))
}

func (s *scanCoordinator) setRollbackInProgress(bucket string, rollback bool) {

	logging.Infof("ScanCoordinator::setRollbackInProgress bucket %v rollback %v", bucket, rollback)
	newRollbackInProgress := s.cloneRollbackInProgress()
	rbMap := *s.getRollbackInProgress()
	if v, ok := rbMap[bucket]; ok {
		v.Store(rollback)
	} else {
		var v atomic.Value
		v.Store(rollback)
		newRollbackInProgress[bucket] = &v
		atomic.StorePointer(&s.rollbackInProgress, unsafe.Pointer(&newRollbackInProgress))
	}
}

func (s *scanCoordinator) getRollbackInProgress() *map[string]*atomic.Value {

	return (*map[string]*atomic.Value)(atomic.LoadPointer(&s.rollbackInProgress))

}

func (s *scanCoordinator) cloneRollbackInProgress() map[string]*atomic.Value {

	newRollbackInProgress := make(map[string]*atomic.Value)
	oldRollbackInProgress := s.getRollbackInProgress()
	if oldRollbackInProgress != nil {
		for bucket, rollbackInProgress := range *oldRollbackInProgress {
			newRollbackInProgress[bucket] = rollbackInProgress
		}
	}

	return newRollbackInProgress
}

func (s *scanCoordinator) updateItemsCount(instId common.IndexInstId, idxStats *IndexStats) error {

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
		return nil
	}

	var is IndexSnapshot

	switch msg.(type) {
	case IndexSnapshot:
		is = msg.(IndexSnapshot)
		if is == nil {
			return nil
		}
		defer DestroyIndexSnapshot(is)
	case error:
		return msg.(error)
	}

	for pid, ps := range is.Partitions() {
		for _, ss := range ps.Slices() {
			snap := ss.Snapshot()
			c, err := snap.StatCountTotal()
			if err != nil {
				return err
			}
			idxStats.updatePartitionStats(pid, func(ps *IndexStats) {
				ps.itemsCount.Set(int64(c))
			})
		}
	}

	return nil
}

/////////////////////////////////////////////////////////////////////////
//
// utility methods
//
/////////////////////////////////////////////////////////////////////////

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

// Find and return data structures for the specified index
// This will also return the IndexReaderContext for each partition.  IndexReaderContext must
// be returned in the same order as partitionIds.
func (s *scanCoordinator) findIndexInstance(
	defnID uint64, partitionIds []common.PartitionId) (*common.IndexInst, []IndexReaderContext, error) {

	hasIndex := false
	isPartition := false

	ctx := make([]IndexReaderContext, len(partitionIds))
	missing := make(map[common.IndexInstId][]common.PartitionId)

	indexInstMap := s.indexInstMap
	indexPartnMap := s.indexPartnMap

	for _, inst := range indexInstMap {
		// Allow REBAL_PENDING to be scanned.  During merge partition, the metadata is updated before inst map is broadcasted.  So
		// there is a chance that cbq is aware of the metadata change ahead of scan coorindator.
		if inst.State != common.INDEX_STATE_ACTIVE || (inst.RState != common.REBAL_ACTIVE && inst.RState != common.REBAL_PENDING) {
			continue
		}
		if inst.Defn.DefnId == common.IndexDefnId(defnID) {
			hasIndex = true
			isPartition = common.IsPartitioned(inst.Defn.PartitionScheme)
			if pmap, ok := indexPartnMap[inst.InstId]; ok {
				found := true
				for i, partnId := range partitionIds {
					if partition, ok := pmap[partnId]; ok {
						ctx[i] = partition.Sc.GetSliceById(0).GetReaderContext()
					} else {
						found = false
						missing[inst.InstId] = append(missing[inst.InstId], partnId)
					}
				}

				if found {
					return &inst, ctx, nil
				}
			}
		}
	}

	if hasIndex {
		if isPartition {
			if content, err := json.Marshal(&missing); err == nil {
				return nil, nil, fmt.Errorf("%v:%v", ErrNotMyPartition, string(content))
			}
			return nil, nil, ErrNotMyPartition
		} else {
			return nil, nil, ErrNotMyIndex
		}
	}
	return nil, nil, common.ErrIndexNotFound
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

func (s *scanCoordinator) isBootstrapMode() bool {
	return s.getIndexerState() == common.INDEXER_BOOTSTRAP
}

func bucketSeqsWithRetry(retries int, logPrefix, cluster, bucket string, numVbs int) (seqnos []uint64, err error) {
	fn := func(r int, err error) error {
		if r > 0 {
			logging.Errorf("%s BucketSeqnos(%s): failed with error (%v)...Retrying (%d)",
				logPrefix, bucket, err, r)
		}
		seqnos, err = common.BucketSeqnos(cluster, "default", bucket)

		if err == nil && len(seqnos) < numVbs {
			return fmt.Errorf("Mismatch of number of vbuckets in DCP seqnos (%v).  Expected (%v).", len(seqnos), numVbs)
		}

		return err
	}

	rh := common.NewRetryHelper(retries, time.Second, 1, fn)
	err = rh.Run()
	return
}

func makePartitionIds(ids []uint64) []common.PartitionId {

	if len(ids) == 0 {
		return []common.PartitionId{common.NON_PARTITION_ID}
	}

	result := make([]common.PartitionId, len(ids))
	for i := 0; i < len(ids); i++ {
		min := i
		for j := i + 1; j < len(ids); j++ {
			if ids[min] > ids[j] {
				min = j
			}
		}

		tmp := ids[i]
		ids[i] = ids[min]
		ids[min] = tmp

		result[i] = common.PartitionId(ids[i])
	}

	return result
}
