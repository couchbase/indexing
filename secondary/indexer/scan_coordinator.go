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
	"os"
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

const DECODE_ERR_THRESHOLD = 100

var secKeyBufPool *common.BytesBufPool

type ScanCoordinator interface {
	SetMeteringMgr(mtMgr *MeteringThrottlingMgr)
}

type scanCoordinator struct {
	supvCmdch        MsgChannel //supervisor sends commands on this channel
	supvMsgch        MsgChannel //channel to send any async message to supervisor
	snapshotNotifych []chan IndexSnapshot
	snapshotReqCh    []MsgChannel
	lastSnapshot     IndexSnapMapHolder
	rollbackTimes    unsafe.Pointer

	rollbackInProgress unsafe.Pointer

	serv      *queryport.Server
	logPrefix string

	mu            sync.RWMutex
	indexInstMap  common.IndexInstMap
	indexPartnMap IndexPartnMap
	indexDefnMap  map[common.IndexDefnId][]common.IndexInstId

	reqCounter      uint64
	config          common.ConfigHolder
	stats           IndexerStatsHolder
	indexerState    atomic.Value
	numDecodeErrors uint32       // Number of errors in collatejson decode.
	cpuThrottle     *CpuThrottle // for Autofailover CPU throttling
	meteringMgr     *MeteringThrottlingMgr

	bucketNameNumVBucketsMapHolder common.BucketNameNumVBucketsMapHolder

	totalMaintDocsQueued int64
	numKeyspaces         int64

	//maintains bucket->bucketStateEnum mapping for pause state
	bucketPauseState map[string]bucketStateEnum
}

// NewScanCoordinator returns an instance of scanCoordinator or err message
// It listens on supvCmdch for command and every command is followed
// by a synchronous response on the supvCmdch.
// Any async message to supervisor is sent to supvMsgch.
// If supvCmdch get closed, ScanCoordinator will shut itself down.
func NewScanCoordinator(supvCmdch MsgChannel, supvMsgch MsgChannel,
	config common.Config, snapshotNotifych []chan IndexSnapshot,
	snapshotReqCh []MsgChannel, stats *IndexerStats,
	cpuThrottle *CpuThrottle) (ScanCoordinator, Message) {
	var err error

	s := &scanCoordinator{
		supvCmdch:        supvCmdch,
		supvMsgch:        supvMsgch,
		snapshotNotifych: snapshotNotifych,
		snapshotReqCh:    snapshotReqCh,
		logPrefix:        "ScanCoordinator",
		reqCounter:       0,
		cpuThrottle:      cpuThrottle,
		indexInstMap:     make(common.IndexInstMap),
		indexPartnMap:    make(IndexPartnMap),
		indexDefnMap:     make(map[common.IndexDefnId][]common.IndexInstId),
		bucketPauseState: make(map[string]bucketStateEnum),
	}

	s.config.Store(config)
	s.initRollbackInProgress()
	s.lastSnapshot.Init()
	s.bucketNameNumVBucketsMapHolder.Init()

	addr := net.JoinHostPort("", config["scanPort"].String())
	queryportCfg := config.SectionConfig("queryport.", true)
	s.serv, err = queryport.NewServer(config["clusterAddr"].String(), addr,
		s.serverCallback, createConnectionContext, queryportCfg)

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
	s.stats.Set(stats)

	for i := 0; i < len(s.snapshotNotifych); i++ {
		go s.listenSnapshot(i)
	}

	// main loop
	go s.run()

	return s, &MsgSuccess{}

}

func (s *scanCoordinator) SetMeteringMgr(mtMgr *MeteringThrottlingMgr) {
	if common.GetBuildMode() == common.ENTERPRISE && common.GetDeploymentModel() == common.SERVERLESS_DEPLOYMENT {
		s.meteringMgr = mtMgr
	}
}

func (s *scanCoordinator) run() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

loop:
	for {
		select {
		case cmd, ok := <-s.supvCmdch:
			if ok {
				if cmd.GetMsgType() == SCAN_COORD_SHUTDOWN {
					logging.Infof("ScanCoordinator: Shutting Down")
					s.serv.Close()
					for i := 0; i < len(s.snapshotReqCh); i++ {
						close(s.snapshotReqCh[i])
					}
					s.supvCmdch <- &MsgSuccess{}
					break loop
				}
				s.handleSupvervisorCommands(cmd)
			} else {
				//supervisor channel closed. exit
				break loop
			}
		case <-ticker.C:
			s.gatherStats()
		}
	}
}

func (s *scanCoordinator) listenSnapshot(index int) {
	for snapshot := range s.snapshotNotifych[index] {
		func(ss IndexSnapshot) {

			lastSnapshot := s.lastSnapshot.Get()

			if snapContainer, ok := lastSnapshot[ss.IndexInstId()]; ok {
				snapContainer.Lock()
				defer snapContainer.Unlock()
				oldSnap := snapContainer.snap
				if oldSnap != nil {
					DestroyIndexSnapshot(oldSnap)
				}

				if !snapContainer.deleted {
					if ss.Timestamp() != nil {
						snapContainer.snap = ss
					} else {
						snapContainer.snap = nil
					}
				} else {
					//If the snap container has been marked deleted,
					//it indicates the index was deleted concurrently,
					//while this method had read lastSnapshot already.
					//At this point, the snapshot coming from storage
					//doesn't need to be stored in the snapshot container
					//and can be destroyed.
					DestroyIndexSnapshot(ss)
				}
			} else {
				// If an instance does not exist in indexInstMap, then
				// it will not exist in lastSnapshot map as well as both
				// the map's get updated at the same time.
				// Scan will not proceed if an index does not exist in
				// indexInstMap. Destroy the snapshot coming from the
				// storage manager.
				DestroyIndexSnapshot(ss)
			}
		}(snapshot)
	}
}

func (s *scanCoordinator) handleSupvervisorCommands(cmd Message) {
	switch cmd.GetMsgType() {
	case ADD_INDEX_INSTANCE:
		s.handleAddIndexInstance(cmd)

	case UPDATE_INDEX_INSTANCE_MAP:
		s.handleUpdateIndexInstMap(cmd)

	case UPDATE_INDEX_PARTITION_MAP:
		s.handleUpdateIndexPartnMap(cmd)

	case SCAN_STATS:
		s.handleStats(cmd)

	case CONFIG_SETTINGS_UPDATE:
		s.handleConfigUpdate(cmd)

	case INDEXER_PAUSE_MOI:
		s.handleIndexerPauseMOI(cmd)

	case INDEXER_RESUME_MOI:
		s.handleIndexerResumeMOI(cmd)

	case INDEXER_BOOTSTRAP:
		s.handleIndexerBootstrap(cmd)

	case INDEXER_ROLLBACK:
		s.handleIndexerRollback(cmd)

	case INDEXER_SECURITY_CHANGE:
		s.handleSecurityChange(cmd)

	case UPDATE_NUMVBUCKETS:
		s.handleUpdateNumVBuckets(cmd)

	case PAUSE_UPDATE_BUCKET_STATE:
		s.handleUpdateBucketPauseState(cmd)

	default:
		logging.Errorf("ScanCoordinator: Received Unknown Command %v", cmd)
		s.supvCmdch <- &MsgError{
			err: Error{code: ERROR_SCAN_COORD_UNKNOWN_COMMAND,
				severity: NORMAL,
				category: SCAN_COORD}}
	}

}

func (s *scanCoordinator) gatherStats() {

	stats := s.stats.Get()

	// update number of mutations queued in maint stream
	if stats != nil {
		totalQueued := int64(0)
		numKeyspaces := int64(0)
		for streamId, statsMap := range stats.GetKeyspaceStatsMap() {
			if streamId == common.MAINT_STREAM {
				for _, keyspaceStats := range statsMap {
					if keyspaceStats != nil {
						totalQueued += keyspaceStats.numMaintDocsQueued.Value()
					}
					numKeyspaces++
				}
			}
		}
		atomic.StoreInt64(&s.totalMaintDocsQueued, totalQueued)
		atomic.StoreInt64(&s.numKeyspaces, numKeyspaces)
	}
}

/////////////////////////////////////////////////////////////////////////
//
//  scan handler
//
/////////////////////////////////////////////////////////////////////////

// serverCallback is the single routine that starts each index scan.
func (s *scanCoordinator) serverCallback(protoReq interface{}, ctx interface{},
	conn net.Conn, cancelCh <-chan bool, clientVersion uint32) {

	if protoReq == queryport.Ping {
		if ctx != nil {
			if conCtx := ctx.(*ConnectionContext); conCtx != nil {
				conCtx.ResetCache()
			}
		}
		return
	}

	ttime := time.Now()

	stats := s.stats.Get()

	req, err := NewScanRequest(protoReq, ctx, cancelCh, s)
	atime := time.Now()
	w := NewProtoWriter(req.ScanType, conn)
	var readUnits uint64 = 0

	if req.ScanType == HeloReq {
		s.handleHeloRequest(req, w)
		s.handleError(req.LogPrefix, w.Done(readUnits, clientVersion))

		if req.Timeout != nil {
			req.Timeout.Stop()
		}
		return
	}

	defer func() {
		s.handleError(req.LogPrefix, w.Done(readUnits, clientVersion))
		req.Done()
	}()

	if req.Stats != nil {
		req.Stats.numRequests.Add(1)
		stats.TotalRequests.Add(1)
		if req.GroupAggr != nil {
			req.Stats.numRequestsAggr.Add(1)
		} else {
			req.Stats.numRequestsRange.Add(1)
		}
		for _, partitionId := range req.PartitionIds {
			req.Stats.updatePartitionStats(partitionId,
				func(stats *IndexStats) {
					stats.numRequests.Add(1)
				})
		}
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

	if req.hasRollback != nil && req.hasRollback.Load() == true {
		err = w.Error(ErrIndexRollback)
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
		elapsed := time.Now().Sub(ttime).Nanoseconds()
		req.Stats.scanReqInitDuration.Add(elapsed)
		req.Stats.scanReqInitLatDist.Add(elapsed)

		now := time.Now().UnixNano()
		req.Stats.lastScanTime.Set(now)

		for _, partitionId := range req.PartitionIds {
			req.Stats.updatePartitionStats(partitionId,
				func(stats *IndexStats) {
					stats.lastScanTime.Set(now)
				})
		}
	}

	// If Autofailover is enabled, do any needed CPU throttling
	cpuThrottleDelayMs := s.cpuThrottle.GetActiveThrottleDelayMs()
	if cpuThrottleDelayMs > 0 {
		time.Sleep(time.Duration(cpuThrottleDelayMs) * time.Millisecond)
	}

	// Pause the scan request if there are too many pending mutation
	if common.IsServerlessDeployment() {
		if cfg := s.config.Load(); cfg != nil {
			threshold := int64(cfg["serverless.scan.throttle.queued_threshold"].Int()) * atomic.LoadInt64(&s.numKeyspaces)
			totalQueued := atomic.LoadInt64(&s.totalMaintDocsQueued)
			if threshold > 0 && totalQueued > threshold {
				time.Sleep(time.Millisecond * time.Duration(cfg["serverless.scan.throttle.pause_duration"].Int()))
			}
		}
	}

	// Pre-scan checks passed, so get a snapshot for the scan
	t0 := time.Now()
	is, err := s.getRequestedIndexSnapshot(req)
	if err != nil {
		logging.Errorf("%s Error in getRequestedIndexSnapshot, instId: %v, partnIds: %v, err: %v", req.LogPrefix, req.IndexInstId, req.PartitionIds, err)

		if err == common.ErrScanTimedOut {
			getSnapTs := func() *common.TsVbuuid {
				lastSnapshot := s.lastSnapshot.Get()

				sc, ok := lastSnapshot[req.IndexInstId]
				if ok && sc != nil {
					sc.Lock()
					defer sc.Unlock()

					ss := sc.snap
					if ss == nil {
						return nil
					}
					return sc.snap.Timestamp()
				}
				return nil
			}

			logSnapInfoAtTimeout(getSnapTs(), req.Ts, req.IndexInstId, req.LogPrefix, req.Stats.lastTsTime.Value())
		}

		if s.tryRespondWithError(w, req, err) {
			return
		}
	}
	defer DestroyIndexSnapshot(is)

	logging.LazyVerbose(func() string {
		return fmt.Sprintf("%s snapshot timestamp: %s",
			req.LogPrefix, ScanTStoString(is.Timestamp()))
	})

	defer func() {
		if req.Stats != nil {
			elapsed := time.Now().Sub(ttime).Nanoseconds()
			req.Stats.scanReqDuration.Add(elapsed)
			req.Stats.scanReqLatDist.Add(elapsed)
		}
	}()

	if len(req.Ctxs) != 0 {
		var err error
		donech := make(chan bool)
		var mutex sync.Mutex

		go func() {
			select {
			case <-req.getTimeoutCh():
				mutex.Lock()
				defer mutex.Unlock()

				select {
				case <-donech:
				default:
					err = common.ErrScanTimedOut
					close(donech)
				}
			case <-req.CancelCh:
				mutex.Lock()
				defer mutex.Unlock()

				select {
				case <-donech:
				default:
					err = common.ErrClientCancel
					close(donech)
				}
			case <-donech:
			}
		}()

		numCtxs := 0
		for _, ctx := range req.Ctxs {
			if !ctx.Init(donech) {
				break
			}
			numCtxs++
		}

		cont := func() bool {
			mutex.Lock()
			defer mutex.Unlock()

			if s.tryRespondWithError(w, req, err) {
				for i := 0; i < numCtxs; i++ {
					req.Ctxs[i].Done()
				}
				return false
			}

			close(donech)
			return true
		}()

		if !cont {
			return
		}
	}

	// Do the scan
	s.processRequest(req, w, is, t0)
	readUnits = req.GetReadUnits()

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
	case FastCountReq:
		s.handleFastCountRequest(req, w, is, t0)
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

	stats := s.stats.Get()

	if req.Stats != nil {
		stats.TotalRowsReturned.Add(int64(scanPipeline.RowsReturned()))
		stats.TotalRowsScanned.Add(int64(scanPipeline.RowsScanned()))

		req.Stats.numRowsReturned.Add(int64(scanPipeline.RowsReturned()))
		req.Stats.scanBytesRead.Add(int64(scanPipeline.BytesRead()))
		req.Stats.scanDuration.Add(scanTime.Nanoseconds())
		req.Stats.scanWaitDuration.Add(waitTime.Nanoseconds())
		req.Stats.scanReqWaitLatDist.Add(waitTime.Nanoseconds())

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
		logging.Errorf("%s RESPONSE rows:%d, scanned:%d, waitTime:%v, totalTime:%v, status:%s, requestId:%s, instId: %v, partnIds: %v",
			req.LogPrefix, scanPipeline.RowsReturned(), scanPipeline.RowsScanned(), waitTime, scanTime, status, req.RequestId, req.IndexInstId, req.PartitionIds)

		s.updateErrStats(req, err)
		if strings.Contains(err.Error(), "Collatejson decode error") {
			errCount := atomic.AddUint32(&s.numDecodeErrors, 1)
			if errCount > DECODE_ERR_THRESHOLD {
				// Not sure if this is in-memory data corruption.
				// It is safe to start afresh.
				logging.Fatalf("Too many unexpected errors in scan decode. "+
					"Error count = %v. Indexer exiting ...", errCount)
				os.Exit(1)
			}
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

func (s *scanCoordinator) handleFastCountRequest(req *ScanRequest, w ScanResponseWriter,
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

	for _, scan := range req.Scans {
		if snapshots, err = GetSliceSnapshots(is, req.PartitionIds); err == nil {
			r, err1 := scatterFastCount(req, scan, snapshots, stopch)
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

	var sk []byte
	if req.dataEncFmt == common.DATA_ENC_COLLATEJSON {
		result := []uint64{rows}
		sk, err = encodeValue(result)
		if s.tryRespondWithError(w, req, err) {
			return
		}
	} else if req.dataEncFmt == common.DATA_ENC_JSON {
		result := []uint64{rows}
		sk, err = json.Marshal(result)
		if s.tryRespondWithError(w, req, err) {
			return
		}
	}

	err = w.Row(nil, sk)
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

		lastSnapshot := s.lastSnapshot.Get()

		sc, ok := lastSnapshot[r.IndexInstId]
		cons := *r.Consistency
		if ok && sc != nil {
			sc.Lock()
			defer sc.Unlock()

			ss := sc.snap
			if ss == nil {
				return nil, nil
			}

			cfg := s.config.Load()
			if !cfg["enable_session_consistency_strict"].Bool() || cons != common.SessionConsistency {
				if isSnapshotConsistent(ss, cons, r.Ts) {
					return CloneIndexSnapshot(ss), nil
				}
				return nil, nil
			}

			strict_chk_threshold := cfg["strict_consistency_check_threshold"].Int()
			isConsistent, isAhead := isSnapshotConsistentOrAhead(ss, r.Ts, cons, strict_chk_threshold)

			if !isConsistent {
				return nil, nil
			}
			if !isAhead {
				return CloneIndexSnapshot(ss), nil
			}

			// snapshot TS is way ahead of Bucket TS. This indicates a possible data loss in KV
			// and indexer has not processed the rollback yet. Switch to SessionConsistencyStrict
			cluster, retries := cfg["clusterAddr"].String(), cfg["settings.scan_getseqnos_retries"].Int()
			newCons := common.SessionConsistencyStrict
			r.Consistency = &newCons

			if r.Stats != nil {
				r.Stats.numStrictConsReqs.Add(1)
			}

			seqnos, vbuuids, e := bucketSeqVbuuidsWithRetry(retries, s.logPrefix,
				cluster, r.Bucket)
			if e != nil {
				return nil, e
			}
			r.Ts = common.NewTsVbuuid2(r.Bucket, seqnos, vbuuids)

			if isSnapshotConsistent(ss, *r.Consistency, r.Ts) {
				return CloneIndexSnapshot(ss), nil
			}

			return nil, nil
		}
		return nil, nil
	}()

	if err != nil {
		return nil, err
	} else if snapshot != nil {
		return snapshot, nil
	}

	// We have not found a consistent snapshot at this point. If indexer is in
	// bootstrap and reqeuest is for consistent scan, we dont want to wait further
	// for consistent snapshots as streams will not be started yet. It would be
	// better to return an error right away instead of scan time so that replica
	// can be retried by the client.
	if s.isBootstrapMode() && *r.Consistency != common.AnyConsistency {
		return nil, common.ErrIndexNotReady
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
	index := uint64(r.IndexInstId) % uint64(len(s.snapshotReqCh))
	s.snapshotReqCh[int(index)] <- snapReqMsg
	var msg interface{}
	select {
	case msg = <-snapResch:
	case <-r.getTimeoutCh():
		go readDeallocSnapshot(snapResch)
		msg = common.ErrScanTimedOut
	case <-r.CancelCh:
		go readDeallocSnapshot(snapResch)
		msg = common.ErrClientCancel
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
		if (cons == common.QueryConsistency || cons == common.SessionConsistencyStrict) &&
			snapTs.AsRecent(reqTs) {
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

// Check whether snapshotTS is consistent with request TS
// Also return if snapshotTS is ahead of request TS for any vb
func isSnapshotConsistentOrAhead(ss IndexSnapshot, reqTs *common.TsVbuuid,
	cons common.Consistency, strict_chk_threshold int) (bool, bool) {

	snapTsConsistent, snapTsAhead := false, false

	if snapTs := ss.Timestamp(); snapTs != nil {
		if ss.IsEpoch() && reqTs.IsEpoch() {
			return true, false
		}

		snapTsConsistent, snapTsAhead = isSnapTsConsistentOrAhead(snapTs, reqTs, strict_chk_threshold)
		if snapTs.CheckCrc64(reqTs) && snapTsConsistent {
			return true, snapTsAhead
		}

		// don't return error because client might be ahead of
		// in receiving a rollback.
		// return nil, ErrVbuuidMismatch
		return false, false
	}
	return false, false
}

// First return param: true if snapTs is as recent as reqTs
// Second return param: true if snapTs is signifantly ahead of reqTs
func isSnapTsConsistentOrAhead(snapTs, reqTs *common.TsVbuuid, strict_chk_threshold int) (bool, bool) {

	isSnapAhead := false

	lag := uint64(0) // Number by which KV Timestamp is behind snapshot Timestamp
	if snapTs == nil || reqTs == nil {
		return false, isSnapAhead
	}
	if snapTs.Bucket != reqTs.Bucket {
		return false, isSnapAhead
	}
	if len(snapTs.Seqnos) > len(reqTs.Seqnos) {
		return false, isSnapAhead
	}
	for i, seqno := range snapTs.Seqnos {
		if seqno < reqTs.Seqnos[i] {
			return false, isSnapAhead
		}

		if seqno > reqTs.Seqnos[i] {
			lag += (seqno - reqTs.Seqnos[i])
		}
	}

	if lag > uint64(strict_chk_threshold) {
		// snapTs is significantly ahead of reqTS, this indicates that
		// there is a possible KV data loss. Switch to slow path that checks
		// for vbuuids along with vbseqnos so that stale results will not be returned
		isSnapAhead = true
	}

	return true, isSnapAhead
}

// isScanAllowed checks several conditions for whether the scan request is allowed to proceed,
// including whether the Indexer is "paused" due to MOI running out of memory, or the bucket is
// being Elixir Paused or Resumed (hibernate/unhibernate tenant bucket).
func (s *scanCoordinator) isScanAllowed(c common.Consistency, scan *ScanRequest) error {
	const _isScanAllowed = "ScanCoordinator::isScanAllowed:"

	if s.getIndexerState() == common.INDEXER_PAUSED_MOI {
		cfg := s.config.Load()
		allow_scan_when_paused := cfg["allow_scan_when_paused"].Bool()

		if c != common.AnyConsistency {
			return fmt.Errorf("%v Indexer Cannot Service %v Scan In Paused State", _isScanAllowed,
				c.String())
		} else if !allow_scan_when_paused {
			return fmt.Errorf("%v Indexer Cannot Service Scan In Paused State", _isScanAllowed)
		} else {
			return nil
		}
	}

	if common.IsServerlessDeployment() {
		if bucketState := s.getBucketPauseState(scan.Bucket); bucketState.IsHibernating() {
			return fmt.Errorf("%v Bucket '%v' scans blocked while in Pause/Resume state %v", _isScanAllowed,
				scan.Bucket, bucketState)
		}
	}

	if scan.rollbackTime == 0 {
		return nil
	}

	rollbackTimes := (*map[string]int64)(atomic.LoadPointer(&s.rollbackTimes))
	if rollbackTimes == nil {
		logging.Errorf("ScanCoordinator.isScanAllowed: rollback time not initialized for instId: %v, partnIds: %v",
			scan.IndexInstId, scan.PartitionIds)
		return ErrIndexRollbackOrBootstrap
	}

	rollbackTime, ok := (*rollbackTimes)[scan.Bucket]
	if !ok {
		logging.Errorf("ScanCoordinator.isScanAllowed: missing rollback time for bucket %v, instId: %v",
			scan.Bucket, scan.IndexInstId)
		return ErrIndexRollbackOrBootstrap
	}

	if scan.rollbackTime != rollbackTime {
		logging.Errorf("ScanCoordinator.isScanAllowed: rollback time mismatch. Req %v indexer %v, instId: %v, partnIds: %v",
			scan.rollbackTime, rollbackTime, scan.IndexInstId, scan.PartitionIds)
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
		if err == common.ErrIndexNotFound {
			stats := s.stats.Get()
			stats.notFoundError.Add(1)
		} else if err == common.ErrIndexerInBootstrap {
			logging.Verbosef("%s REQUEST %s", req.LogPrefix, req)
			logging.Verbosef("%s RESPONSE status:(error = %s), requestId: %v", req.LogPrefix, err, req.RequestId)
		} else {
			logging.Infof("%s REQUEST %s", req.LogPrefix, req)
			logging.Infof("%s RESPONSE status:(error = %s), requestId: %v", req.LogPrefix, err, req.RequestId)
		}
		s.updateErrStats(req, err)
		s.handleError(req.LogPrefix, w.Error(err))
		return true
	}

	return false
}

func (s *scanCoordinator) updateErrStats(req *ScanRequest, err error) {
	if req.Stats != nil {
		switch err {
		case common.ErrClientCancel:
			req.Stats.clientCancelError.Add(1)
		case common.ErrScanTimedOut:
			req.Stats.numScanTimeouts.Add(1)
		case common.ErrIndexNotReady:
			req.Stats.notReadyError.Add(1)
		default:
			req.Stats.numScanErrors.Add(1)
		}
	}
}

/////////////////////////////////////////////////////////////////////////
//
//  supervisor message handlers
//
/////////////////////////////////////////////////////////////////////////

func (s *scanCoordinator) handleUpdateNumVBuckets(cmd Message) {
	logging.Tracef("scanCoordinator::handleUpdateNumVBuckets %v", cmd)

	req := cmd.(*MsgUpdateNumVbuckets)
	bucketNameNumVBucketsMap := req.GetBucketNameNumVBucketsMap()

	s.bucketNameNumVBucketsMapHolder.Set(bucketNameNumVBucketsMap)

	s.supvCmdch <- &MsgSuccess{}
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
		var netScanned int64
		var totalCompletedReqs int64
		for id, idxStats := range stats.indexes {

			err := s.updateItemsCount(id, idxStats)
			if err != nil {
				logging.Errorf("%v: Unable to compute index items_count for %v/%v/%v state %v (%v)", s.logPrefix,
					idxStats.bucket, idxStats.name, id, idxStats.indexState.Value(), err)
			}

			// compute scan rate
			now := time.Now().UnixNano()
			elapsed := float64(now-idxStats.lastScanGatherTime.Value()) / float64(time.Second)
			if elapsed > 60 {
				partitions := idxStats.getPartitions()
				for _, pid := range partitions {
					partnStats := idxStats.getPartitionStats(pid)
					numRowsScanned := partnStats.numRowsScanned.Value()
					if idxStats.lastScanGatherTime.Value() != int64(0) {
						scanned := numRowsScanned - partnStats.lastNumRowsScanned.Value()
						netScanned += scanned
						scanRate := float64(scanned) / elapsed
						partnStats.avgScanRate.Set(int64((scanRate + float64(partnStats.avgScanRate.Value())) / 2))
						logging.Debugf("scanCoordinator.handleStats: index %v partition %v numRowsScanned %v scan rate %v avg scan rate %v",
							id, pid, numRowsScanned, scanRate, partnStats.avgScanRate.Value())
					}
					partnStats.lastNumRowsScanned.Set(numRowsScanned)
					idxStats.lastScanGatherTime.Set(now)
				}
			}
			totalCompletedReqs += idxStats.numCompletedRequests.Value()
		}

		pendingScans := stats.TotalRequests.Value() - totalCompletedReqs
		stats.totalPendingScans.Set(pendingScans)

		now := time.Now().UnixNano()
		netElapsed := float64(now-stats.lastScanGatherTime.Value()) / float64(time.Second)
		if netElapsed > 60 {
			if stats.lastScanGatherTime.Value() != int64(0) {
				netScanRate := float64(netScanned) / netElapsed
				stats.netAvgScanRate.Set(int64((netScanRate + float64(stats.netAvgScanRate.Value())) / 2))
			}
			stats.lastScanGatherTime.Set(now)
		}
		replych <- true
	}()
}

// The updates to indexInstMap and indexPartnMap are mutex protected. Readers
// will acquire RLock before accessing them. Also, the inst and partnMap are
// cloned at source. Hence, it is safe to update indexInstMap and indexPartnMap
// by acquiring lock
func (s *scanCoordinator) handleAddIndexInstance(cmd Message) {
	logging.Infof("ScanCoordinator::handleAddIndexInstance %v", cmd)
	s.mu.Lock()
	defer s.mu.Unlock()

	req := cmd.(*MsgAddIndexInst)
	inst := req.GetIndexInst()
	partnMap := req.GetInstPartnMap()
	instStats := req.GetIndexInstStats()

	s.indexInstMap[inst.InstId] = inst
	s.indexPartnMap[inst.InstId] = partnMap

	defnId := inst.Defn.DefnId
	s.indexDefnMap[defnId] = append(s.indexDefnMap[defnId], inst.InstId)

	stats := s.stats.Get()
	stats.setIndexStats(inst.InstId, instStats)
	s.stats.Set(stats)

	s.supvCmdch <- &MsgSuccess{}
}

func (s *scanCoordinator) handleUpdateIndexInstMap(cmd Message) {
	s.mu.Lock()
	defer s.mu.Unlock()

	req := cmd.(*MsgUpdateInstMap)
	logging.Tracef("ScanCoordinator::handleUpdateIndexInstMap %v", cmd)
	indexInstMap := req.GetIndexInstMap()
	s.stats.Set(req.GetStatsObject())
	s.indexInstMap = common.CopyIndexInstMap(indexInstMap)

	// Re-initialize indexDefnMap
	s.indexDefnMap = make(map[common.IndexDefnId][]common.IndexInstId)
	for instId, inst := range s.indexInstMap {
		s.indexDefnMap[inst.Defn.DefnId] = append(s.indexDefnMap[inst.Defn.DefnId], instId)
	}

	s.updateLastSnapshotMap()

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

func (s *scanCoordinator) handleIndexerPauseMOI(cmd Message) {
	s.setIndexerState(common.INDEXER_PAUSED_MOI)
	s.supvCmdch <- &MsgSuccess{}

}

func (s *scanCoordinator) handleIndexerResumeMOI(cmd Message) {

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

	//TODO Collections are these changes required at collection level
	bucket, _, _ := SplitKeyspaceId(msg.keyspaceId)

	if msg.rollbackTime != 0 {
		s.saveRollbackTime(bucket, msg.rollbackTime)
		s.setRollbackInProgress(bucket, true)
	} else {
		s.setRollbackInProgress(bucket, false)
	}

	s.supvCmdch <- &MsgSuccess{}
}

func (s *scanCoordinator) handleSecurityChange(cmd Message) {

	err := s.serv.ResetConnections()
	if err != nil {
		idxErr := Error{
			code:     ERROR_INDEXER_INTERNAL_ERROR,
			severity: FATAL,
			cause:    err,
			category: INDEXER,
		}
		s.supvCmdch <- &MsgError{err: idxErr}
		return
	}

	s.supvCmdch <- &MsgSuccess{}
}

func (s *scanCoordinator) handleUpdateBucketPauseState(cmd Message) {

	req := cmd.(*MsgPauseUpdateBucketState)
	bucket := req.GetBucket()
	bucketState := req.GetBucketPauseState()

	if common.IsServerlessDeployment() {
		s.mu.Lock()
		defer s.mu.Unlock()

		//update indexer book-keeping
		s.bucketPauseState[bucket] = bucketState
	}

	s.supvCmdch <- &MsgSuccess{}

}

func (s *scanCoordinator) getBucketPauseState(bucket string) bucketStateEnum {

	s.mu.RLock()
	defer s.mu.RUnlock()

	if state, ok := s.bucketPauseState[bucket]; ok {
		return state
	} else {
		return bst_NIL
	}
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

	index := uint64(instId) % uint64(len(s.snapshotReqCh))
	s.snapshotReqCh[int(index)] <- snapReqMsg
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
func (s *scanCoordinator) findIndexInstance(defnID uint64,
	partitionIds []common.PartitionId, user string, skipReadMetering bool) (
	*common.IndexInst, []IndexReaderContext, error) {

	hasIndex := false
	isPartition := false

	ctx := make([]IndexReaderContext, len(partitionIds))
	missing := make(map[common.IndexInstId][]common.PartitionId)

	indexInstMap := s.indexInstMap
	indexPartnMap := s.indexPartnMap

	// Get all instanceId's of interest
	instIdList := s.indexDefnMap[common.IndexDefnId(defnID)]

	for _, instId := range instIdList {
		inst := indexInstMap[instId]
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
						ctx[i] = partition.Sc.GetSliceById(0).GetReaderContext(user, skipReadMetering)
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

	if s.isBootstrapMode() {
		// Since madhatter, scanning of warmed up index during indexer bootstrap
		// is allowed. If indexer is in bootstrap and index is not found, it implies
		// index is not warmed up yet. Return IndexNotReady error.
		return nil, nil, common.ErrIndexNotReady
	}

	return nil, nil, common.ErrIndexNotFound
}

func (s *scanCoordinator) updateLastSnapshotMap() {
	lastSnapshot := s.lastSnapshot.Clone()

	// Add any new indexes that got added
	for instId, _ := range s.indexInstMap {
		if _, ok := lastSnapshot[instId]; !ok {
			sc := &IndexSnapshotContainer{}
			lastSnapshot[instId] = sc
		}
	}

	// Delete instances that got deleted
	for instId, sc := range lastSnapshot {
		if inst, ok := s.indexInstMap[instId]; !ok || inst.State == common.INDEX_STATE_DELETED {

			func() {
				sc.Lock()
				defer sc.Unlock()
				if sc.snap != nil {
					DestroyIndexSnapshot(sc.snap)
					sc.snap = nil
				}

				//set sc.deleted to true to indicate to concurrent readers
				//that this snap container should no longer be used to
				//add new snapshots.
				sc.deleted = true
				delete(lastSnapshot, instId)

			}()
		}
	}
	s.lastSnapshot.Set(lastSnapshot)
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

func bucketSeqsWithRetry(retries int, logPrefix, cluster, bucket string, cid string, useBucketSeqnos bool) (seqnos []uint64, err error) {
	fn := func(r int, err error) error {
		if r > 0 {
			logging.Errorf("%s BucketSeqnos(%s): failed with error (%v)...Retrying (%d)",
				logPrefix, bucket, err, r)
		}
		if useBucketSeqnos {
			seqnos, err = common.BucketSeqnos(cluster, "default", bucket)
		} else {
			seqnos, err = common.GetSeqnos(cluster, "default", bucket, cid)
		}

		// Commenting this check as this is already done in BucketSeqnos and GetSeqnos
		// if err == nil && len(seqnos) < numVbs {
		// 	return fmt.Errorf("Mismatch of number of vbuckets in DCP seqnos (%v).  Expected (%v).", len(seqnos), numVbs)
		// }

		return err
	}

	rh := common.NewRetryHelper(retries, time.Second, 1, fn)
	err = rh.Run()
	return
}

func bucketSeqVbuuidsWithRetry(retries int, logPrefix, cluster,
	bucket string) (seqnos, vbuuids []uint64, err error) {
	fn := func(r int, err error) error {
		if r > 0 {
			logging.Errorf("%s BucketTs(%s): failed with error (%v)...Retrying (%d)",
				logPrefix, bucket, err, r)
		}

		b, err := common.ConnectBucket(cluster, "default", bucket)
		if err != nil {
			return err
		}
		defer b.Close()

		numVBuckets := b.NumVBuckets

		seqnos, vbuuids, err = common.BucketTs(b, numVBuckets)
		if err != nil {
			return err
		}

		if err == nil && len(seqnos) < numVBuckets {
			return fmt.Errorf("Mismatch of number of vbuckets in DCP seqnos (%v).  Expected (%v).", len(seqnos), numVBuckets)
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
