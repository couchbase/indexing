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
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	couchbase "github.com/couchbase/indexing/secondary/dcp"
	"github.com/couchbase/indexing/secondary/logging"
)

const (
	maxStatsRetries = 5
)

//Timekeeper manages the Stability Timestamp Generation and also
//keeps track of the HWTimestamp for each bucket
type Timekeeper interface {
}

type timekeeper struct {
	supvCmdch  MsgChannel //supervisor sends commands on this channel
	supvRespch MsgChannel //channel to send any async message to supervisor

	ss *StreamState

	//map of indexInstId to its Initial Build Info
	indexBuildInfo map[common.IndexInstId]*InitialBuildInfo

	config common.Config

	indexInstMap  common.IndexInstMap
	indexPartnMap IndexPartnMap

	statsLock  sync.Mutex
	bucketConn map[string]*couchbase.Bucket

	stats           IndexerStatsHolder
	vbCheckerStopCh chan bool

	lock sync.RWMutex //lock to protect this structure

	indexerState common.IndexerState
}

type InitialBuildInfo struct {
	indexInst            common.IndexInst
	buildTs              Timestamp
	buildDoneAckReceived bool
	minMergeTs           *common.TsVbuuid //minimum merge ts for init stream
	addInstPending       bool
	waitForRecovery      bool
}

//timeout in milliseconds to batch the vbuckets
//together for repair message
const REPAIR_BATCH_TIMEOUT = 1000
const KV_RETRY_INTERVAL = 5000

//const REPAIR_RETRY_INTERVAL = 5000
const REPAIR_RETRY_BEFORE_SHUTDOWN = 5

//NewTimekeeper returns an instance of timekeeper or err message.
//It listens on supvCmdch for command and every command is followed
//by a synchronous response of the supvCmdch.
//Any async response to supervisor is sent to supvRespch.
//If supvCmdch get closed, storageMgr will shut itself down.
func NewTimekeeper(supvCmdch MsgChannel, supvRespch MsgChannel,
	config common.Config) (Timekeeper, Message) {

	//Init the timekeeper struct
	tk := &timekeeper{
		supvCmdch:      supvCmdch,
		supvRespch:     supvRespch,
		ss:             InitStreamState(config),
		config:         config,
		indexInstMap:   make(common.IndexInstMap),
		indexPartnMap:  make(IndexPartnMap),
		indexBuildInfo: make(map[common.IndexInstId]*InitialBuildInfo),
		bucketConn:     make(map[string]*couchbase.Bucket),
	}

	//start timekeeper loop which listens to commands from its supervisor
	go tk.run()

	return tk, &MsgSuccess{}

}

//run starts the timekeeper loop which listens to messages
//from it supervisor(indexer)
func (tk *timekeeper) run() {

	//main timekeeper loop
loop:
	for {
		select {

		case cmd, ok := <-tk.supvCmdch:
			if ok {
				if cmd.GetMsgType() == TK_SHUTDOWN {
					logging.Infof("Timekeeper::run Shutting Down")
					tk.supvCmdch <- &MsgSuccess{}
					close(tk.vbCheckerStopCh)
					break loop
				}
				tk.handleSupervisorCommands(cmd)
			} else {
				//supervisor channel closed. exit
				break loop
			}

		}
	}
}

func (tk *timekeeper) handleSupervisorCommands(cmd Message) {

	switch cmd.GetMsgType() {

	case OPEN_STREAM:
		tk.handleStreamOpen(cmd)

	case ADD_INDEX_LIST_TO_STREAM:
		tk.handleAddIndextoStream(cmd)

	case INDEXER_UPDATE_BUILD_TS:
		tk.handleUpdateBuildTs(cmd)

	case REMOVE_INDEX_LIST_FROM_STREAM:
		tk.handleRemoveIndexFromStream(cmd)

	case REMOVE_BUCKET_FROM_STREAM:
		tk.handleRemoveBucketFromStream(cmd)

	case CLOSE_STREAM:
		tk.handleStreamClose(cmd)

	case CLEANUP_STREAM:
		tk.handleStreamCleanup(cmd)

	case STREAM_READER_STREAM_BEGIN:
		tk.handleStreamBegin(cmd)

	case STREAM_READER_STREAM_END:
		tk.handleStreamEnd(cmd)

	case STREAM_READER_HWT:
		tk.handleSync(cmd)

	case STREAM_READER_CONN_ERROR:
		tk.handleStreamConnError(cmd)

	case POOL_CHANGE:
		tk.handlePoolChange(cmd)

	case TK_ENABLE_FLUSH:
		tk.handleFlushStateChange(cmd)

	case TK_DISABLE_FLUSH:
		tk.handleFlushStateChange(cmd)

	case STORAGE_SNAP_DONE:
		tk.handleFlushDone(cmd)

	case MUT_MGR_ABORT_DONE:
		tk.handleFlushAbortDone(cmd)

	case TK_GET_BUCKET_HWT:
		tk.handleGetBucketHWT(cmd)

	case INDEXER_INIT_PREP_RECOVERY:
		tk.handleInitPrepRecovery(cmd)

	case INDEXER_PREPARE_DONE:
		tk.handlePrepareDone(cmd)

	case TK_INIT_BUILD_DONE_ACK:
		tk.handleInitBuildDoneAck(cmd)

	case TK_ADD_INSTANCE_FAIL:
		tk.handleAddInstanceFail(cmd)

	case TK_MERGE_STREAM_ACK:
		tk.handleMergeStreamAck(cmd)

	case STREAM_REQUEST_DONE:
		tk.handleStreamRequestDone(cmd)

	case INDEXER_RECOVERY_DONE:
		tk.handleRecoveryDone(cmd)

	case CONFIG_SETTINGS_UPDATE:
		tk.handleConfigUpdate(cmd)

	case UPDATE_INDEX_INSTANCE_MAP:
		tk.handleUpdateIndexInstMap(cmd)

	case UPDATE_INDEX_PARTITION_MAP:
		tk.handleUpdateIndexPartnMap(cmd)

	case INDEX_PROGRESS_STATS:
		tk.handleStats(cmd)

	case INDEXER_PAUSE:
		tk.handleIndexerPause(cmd)

	case INDEXER_PREPARE_UNPAUSE:
		tk.handlePrepareUnpause(cmd)

	case INDEXER_RESUME:
		tk.handleIndexerResume(cmd)

	case INDEXER_ABORT_RECOVERY:
		tk.handleAbortRecovery(cmd)

	default:
		logging.Errorf("Timekeeper::handleSupvervisorCommands "+
			"Received Unknown Command %v", cmd)
		common.CrashOnError(errors.New("Unknown Command On Supervisor Channel"))

	}

}

func (tk *timekeeper) handleStreamOpen(cmd Message) {

	logging.Debugf("Timekeeper::handleStreamOpen %v", cmd)

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()
	bucket := cmd.(*MsgStreamUpdate).GetBucket()
	restartTs := cmd.(*MsgStreamUpdate).GetRestartTs()
	rollbackTime := cmd.(*MsgStreamUpdate).GetRollbackTime()
	async := cmd.(*MsgStreamUpdate).GetAsync()
	sessionId := cmd.(*MsgStreamUpdate).GetSessionId()

	tk.lock.Lock()
	defer tk.lock.Unlock()

	if tk.ss.streamStatus[streamId] != STREAM_ACTIVE {
		tk.ss.initNewStream(streamId)
		logging.Infof("Timekeeper::handleStreamOpen Stream %v "+
			"State Changed to ACTIVE", streamId)

	}

	status := tk.ss.streamBucketStatus[streamId][bucket]
	switch status {

	//fresh start or recovery
	case STREAM_INACTIVE, STREAM_PREPARE_DONE:
		tk.ss.initBucketInStream(streamId, bucket)
		if restartTs != nil {
			tk.ss.streamBucketRestartTsMap[streamId][bucket] = restartTs.Copy()
			tk.ss.setHWTFromRestartTs(streamId, bucket)
		}
		tk.ss.setRollbackTime(bucket, rollbackTime)
		tk.ss.streamBucketAsyncMap[streamId][bucket] = async
		tk.ss.streamBucketSessionId[streamId][bucket] = sessionId
		tk.addIndextoStream(cmd)
		tk.startTimer(streamId, bucket)

	//repair
	default:
		//no-op
		//reset timer for open stream
		tk.ss.streamBucketOpenTsMap[streamId][bucket] = nil
		tk.ss.streamBucketStartTimeMap[streamId][bucket] = uint64(0)

		logging.Infof("Timekeeper::handleStreamOpen %v %v Status %v. "+
			"Nothing to do.", streamId, bucket, status)

	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleStreamClose(cmd Message) {

	logging.Debugf("Timekeeper::handleStreamClose %v", cmd)

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()

	tk.lock.Lock()
	defer tk.lock.Unlock()

	//cleanup all buckets from stream
	for bucket, _ := range tk.ss.streamBucketStatus[streamId] {
		tk.removeBucketFromStream(streamId, bucket, false)
	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleInitPrepRecovery(msg Message) {

	bucket := msg.(*MsgRecovery).GetBucket()
	streamId := msg.(*MsgRecovery).GetStreamId()

	logging.Infof("Timekeeper::handleInitPrepRecovery %v %v",
		streamId, bucket)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	tk.prepareRecovery(streamId, bucket)

	tk.supvCmdch <- &MsgSuccess{}

}

func (tk *timekeeper) handlePrepareDone(cmd Message) {

	logging.Infof("Timekeeper::handlePrepareDone %v", cmd)

	streamId := cmd.(*MsgRecovery).GetStreamId()
	bucket := cmd.(*MsgRecovery).GetBucket()

	tk.lock.Lock()
	defer tk.lock.Unlock()

	//if stream is in PREPARE_RECOVERY, check and init RECOVERY
	if tk.ss.streamBucketStatus[streamId][bucket] == STREAM_PREPARE_RECOVERY {

		logging.Infof("Timekeeper::handlePrepareDone Stream %v "+
			"Bucket %v State Changed to PREPARE_DONE", streamId, bucket)
		tk.ss.streamBucketStatus[streamId][bucket] = STREAM_PREPARE_DONE

		switch streamId {
		case common.MAINT_STREAM, common.INIT_STREAM:
			if tk.checkBucketReadyForRecovery(streamId, bucket) {
				tk.initiateRecovery(streamId, bucket)
			}
		}
	} else {
		logging.Infof("Timekeeper::handlePrepareDone Unexpected PREPARE_DONE "+
			"for StreamId %v Bucket %v State %v", streamId, bucket,
			tk.ss.streamBucketStatus[streamId][bucket])
	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleAddIndextoStream(cmd Message) {

	logging.Verbosef("Timekeeper::handleAddIndextoStream %v", cmd)

	tk.lock.Lock()
	defer tk.lock.Unlock()
	tk.addIndextoStream(cmd)

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) addIndextoStream(cmd Message) {

	indexInstList := cmd.(*MsgStreamUpdate).GetIndexList()
	buildTs := cmd.(*MsgStreamUpdate).GetTimestamp()
	streamId := cmd.(*MsgStreamUpdate).GetStreamId()
	bucketInRecovery := cmd.(*MsgStreamUpdate).BucketInRecovery()

	//If the index is in INITIAL state, store it in initialbuild map
	for _, idx := range indexInstList {

		tk.ss.streamBucketIndexCountMap[streamId][idx.Defn.Bucket] += 1
		logging.Infof("Timekeeper::addIndextoStream IndexCount %v", tk.ss.streamBucketIndexCountMap)

		// buildInfo can be reomved when the corresponding is closed.   A stream can be closed for
		// various condition, such as recovery.   When the stream is re-opened, index will be added back
		// to the stream.  It is necesasry to ensure that buildInfo is added back accordingly to ensure
		// that build index will converge.
		if _, ok := tk.indexBuildInfo[idx.InstId]; !ok {

			if idx.State == common.INDEX_STATE_INITIAL ||
				(streamId == common.INIT_STREAM && idx.State == common.INDEX_STATE_CATCHUP) {

				logging.Infof("Timekeeper::addIndextoStream add BuildInfo index %v "+
					"stream %v bucket %v state %v waitForRecovery %v", idx.InstId, streamId,
					idx.Defn.Bucket, idx.State, bucketInRecovery)

				tk.indexBuildInfo[idx.InstId] = &InitialBuildInfo{
					indexInst:       idx,
					buildTs:         buildTs,
					waitForRecovery: bucketInRecovery,
				}
			}
		}
	}

}

func (tk *timekeeper) handleUpdateBuildTs(cmd Message) {
	logging.Infof("Timekeeper::handleUpdateBuildTs %v", cmd)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()
	bucket := cmd.(*MsgStreamUpdate).GetBucket()
	state := tk.ss.streamBucketStatus[streamId][bucket]

	// Ignore UPDATE_BUILD_TS msg for inactive and recovery phase. For recovery,
	// stream will get re-opened and build done will get re-computed.
	if state == STREAM_INACTIVE || state == STREAM_PREPARE_DONE ||
		state == STREAM_PREPARE_RECOVERY {
		logging.Infof("Timekeeper::handleUpdateBuildTs Ignore updateBuildTs "+
			"for Bucket: %v StreamId: %v State: %v", bucket, streamId, state)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	tk.updateBuildTs(cmd)

	//Check for possiblity of build done after buildTs is updated.
	//In case of crash recovery, if there are no mutations, there is
	//no flush happening, which can cause index to be in initial state.
	if !tk.ss.checkAnyFlushPending(streamId, bucket) &&
		!tk.ss.checkAnyAbortPending(streamId, bucket) {
		lastFlushedTs := tk.ss.streamBucketLastFlushedTsMap[streamId][bucket]
		tk.checkInitialBuildDone(streamId, bucket, lastFlushedTs)
	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) updateBuildTs(cmd Message) {
	buildTs := cmd.(*MsgStreamUpdate).GetTimestamp()
	streamId := cmd.(*MsgStreamUpdate).GetStreamId()
	bucket := cmd.(*MsgStreamUpdate).GetBucket()

	tk.setBuildTs(streamId, bucket, buildTs)
}

func (tk *timekeeper) handleRemoveIndexFromStream(cmd Message) {

	logging.Infof("Timekeeper::handleRemoveIndexFromStream %v", cmd)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	tk.removeIndexFromStream(cmd)

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleRemoveBucketFromStream(cmd Message) {

	logging.Infof("Timekeeper::handleRemoveBucketFromStream %v", cmd)

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()
	bucket := cmd.(*MsgStreamUpdate).GetBucket()
	abort := cmd.(*MsgStreamUpdate).AbortRecovery()

	tk.lock.Lock()
	defer tk.lock.Unlock()

	tk.removeBucketFromStream(streamId, bucket, abort)

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) removeIndexFromStream(cmd Message) {

	indexInstList := cmd.(*MsgStreamUpdate).GetIndexList()
	streamId := cmd.(*MsgStreamUpdate).GetStreamId()

	for _, idx := range indexInstList {
		// This is only called for DROP INDEX.   Therefore, it is OK to remove buildInfo.
		// If this is used for other purpose, it is necessary to ensure there is no side effect.
		if _, ok := tk.indexBuildInfo[idx.InstId]; ok {
			logging.Infof("Timekeeper::removeIndexFromStream remove index %v from stream %v bucket %v",
				idx.InstId, streamId, idx.Defn.Bucket)
			delete(tk.indexBuildInfo, idx.InstId)
		}
		if tk.ss.streamBucketIndexCountMap[streamId][idx.Defn.Bucket] == 0 {
			logging.Fatalf("Timekeeper::removeIndexFromStream Invalid Internal "+
				"State Detected. Index Count Underflow. Stream %s. Bucket %s.", streamId,
				idx.Defn.Bucket)
		} else {
			tk.ss.streamBucketIndexCountMap[streamId][idx.Defn.Bucket] -= 1
			logging.Infof("Timekeeper::removeIndexFromStream IndexCount %v", tk.ss.streamBucketIndexCountMap)
		}
	}
}

func (tk *timekeeper) removeBucketFromStream(streamId common.StreamId,
	bucket string, abort bool) {

	status := tk.ss.streamBucketStatus[streamId][bucket]

	// This is called when closing a stream or remove a bucket from a stream.  If this
	// is called during recovery,  it is necessary to ensure that the buildInfo is added
	// back to timekeeper accordingly.
	for instId, idx := range tk.indexBuildInfo {
		// remove buildInfo only for the given bucket AND stream
		if idx.indexInst.Defn.Bucket == bucket && idx.indexInst.Stream == streamId {
			logging.Infof("Timekeeper::removeBucketFromStream remove index %v from stream %v bucket %v",
				instId, streamId, bucket)
			delete(tk.indexBuildInfo, instId)
		}
	}

	//for a bucket in prepareRecovery, only change the status
	//actual cleanup happens in initRecovery
	if status == STREAM_PREPARE_RECOVERY && !abort {
		tk.ss.streamBucketStatus[streamId][bucket] = STREAM_PREPARE_RECOVERY
		tk.ss.clearAllRepairState(streamId, bucket)
	} else {
		tk.stopTimer(streamId, bucket)
		tk.ss.cleanupBucketFromStream(streamId, bucket)
	}

}

func (tk *timekeeper) handleSync(cmd Message) {

	logging.LazyTrace(func() string {
		return fmt.Sprintf("Timekeeper::handleSync %v", cmd)
	})

	streamId := cmd.(*MsgBucketHWT).GetStreamId()
	bucket := cmd.(*MsgBucketHWT).GetBucket()
	hwt := cmd.(*MsgBucketHWT).GetHWT()
	prevSnap := cmd.(*MsgBucketHWT).GetPrevSnap()
	sessionId := cmd.(*MsgBucketHWT).GetSessionId()

	tk.lock.Lock()
	defer tk.lock.Unlock()

	//check if bucket is active in stream
	if tk.checkBucketActiveInStream(streamId, bucket) == false {
		logging.Tracef("Timekeeper::handleSync Received Sync for "+
			"Inactive Bucket %v Stream %v. Ignored.", bucket, streamId)
		return
	}

	//if there are no indexes for this bucket and stream, ignore
	if c, ok := tk.ss.streamBucketIndexCountMap[streamId][bucket]; !ok || c <= 0 {
		logging.Tracef("Timekeeper::handleSync Ignore Sync for StreamId %v "+
			"Bucket %v. IndexCount %v. ", streamId, bucket, c)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	//if the session doesn't match, ignore
	currSessionId := tk.ss.getSessionId(streamId, bucket)
	if sessionId != 0 && sessionId != currSessionId {
		logging.Warnf("Timekeeper::handleSync Ignore Sync for StreamId %v "+
			"Bucket %v. SessionId %v. Current Session %v ", streamId, bucket,
			sessionId, currSessionId)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	if _, ok := tk.ss.streamBucketHWTMap[streamId][bucket]; !ok {
		logging.Debugf("Timekeeper::handleSync Ignoring Sync Marker "+
			"for StreamId %v Bucket %v. Bucket Not Found.", streamId, bucket)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	//update HWT for the bucket
	tk.ss.updateHWT(streamId, bucket, hwt, prevSnap)
	hwt.Free()
	prevSnap.Free()

	tk.supvCmdch <- &MsgSuccess{}

}

func (tk *timekeeper) handleFlushDone(cmd Message) {

	streamId := cmd.(*MsgMutMgrFlushDone).GetStreamId()
	bucket := cmd.(*MsgMutMgrFlushDone).GetBucket()
	flushWasAborted := cmd.(*MsgMutMgrFlushDone).GetAborted()

	tk.lock.Lock()
	defer tk.lock.Unlock()

	bucketLastFlushedTsMap := tk.ss.streamBucketLastFlushedTsMap[streamId]
	bucketFlushInProgressTsMap := tk.ss.streamBucketFlushInProgressTsMap[streamId]

	doneCh := tk.ss.streamBucketFlushDone[streamId][bucket]
	if doneCh != nil {
		close(doneCh)
		tk.ss.streamBucketFlushDone[streamId][bucket] = nil
	}

	if flushWasAborted {

		tk.processFlushAbort(streamId, bucket)
		return
	}

	if _, ok := bucketFlushInProgressTsMap[bucket]; ok {
		//store the last flushed TS
		fts := bucketFlushInProgressTsMap[bucket]

		bucketLastFlushedTsMap[bucket] = fts

		if fts != nil {
			tk.ss.updateLastMutationVbuuid(streamId, bucket, fts)
		}

		// check if each flush time is snap aligned. If so, make a copy.
		if fts != nil && fts.IsSnapAligned() {
			tk.ss.streamBucketLastSnapAlignFlushedTsMap[streamId][bucket] = fts.Copy()
		}

		//update internal map to reflect flush is done
		bucketFlushInProgressTsMap[bucket] = nil
	} else {
		//this bucket is already gone from this stream, may be because
		//the index were dropped. Log and ignore.
		logging.Warnf("Timekeeper::handleFlushDone Ignore Flush Done for Stream %v "+
			"Bucket %v. Bucket Info Not Found", streamId, bucket)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	switch streamId {

	case common.MAINT_STREAM:
		tk.handleFlushDoneMaintStream(cmd)

	case common.INIT_STREAM:
		tk.handleFlushDoneInitStream(cmd)

	default:
		logging.Errorf("Timekeeper::handleFlushDone \n\tInvalid StreamId %v ", streamId)
	}

}

func (tk *timekeeper) processFlushAbort(streamId common.StreamId, bucket string) {

	bucketLastFlushedTsMap := tk.ss.streamBucketLastFlushedTsMap[streamId]
	bucketFlushInProgressTsMap := tk.ss.streamBucketFlushInProgressTsMap[streamId]

	logging.Infof("Timekeeper::processFlushAbort Flush Abort Received %v %v"+
		"\nFlushTs %v \nLastFlushTs %v", streamId, bucket, bucketFlushInProgressTsMap[bucket],
		bucketLastFlushedTsMap[bucket])

	bucketFlushInProgressTsMap[bucket] = nil

	//disable further processing for this stream
	tk.ss.streamBucketFlushEnabledMap[streamId][bucket] = false

	//after this point, a recovery must happen
	tk.ss.streamBucketForceRecovery[streamId][bucket] = true

	tsList := tk.ss.streamBucketTsListMap[streamId][bucket]
	tsList.Init()

	state := tk.ss.streamBucketStatus[streamId][bucket]

	sessionId := tk.ss.getSessionId(streamId, bucket)

	switch state {

	case STREAM_ACTIVE, STREAM_RECOVERY:

		logging.Infof("Timekeeper::processFlushAbort %v %v %v Generate InitPrepRecovery",
			streamId, bucket, sessionId)
		tk.supvRespch <- &MsgRecovery{mType: INDEXER_INIT_PREP_RECOVERY,
			streamId:  streamId,
			bucket:    bucket,
			restartTs: tk.ss.computeRestartTs(streamId, bucket),
			sessionId: sessionId}

	case STREAM_PREPARE_RECOVERY:

		//send message to stop running stream
		logging.Infof("Timekeeper::processFlushAbort %v %v %v Generate PrepareRecovery",
			streamId, bucket, sessionId)
		tk.supvRespch <- &MsgRecovery{mType: INDEXER_PREPARE_RECOVERY,
			streamId:  streamId,
			bucket:    bucket,
			sessionId: sessionId}

	case STREAM_INACTIVE:
		logging.Errorf("Timekeeper::processFlushAbort Unexpected Flush Abort "+
			"Received for Inactive StreamId %v bucket %v sessionId %v ",
			streamId, bucket, sessionId)

	default:
		logging.Errorf("Timekeeper::processFlushAbort %v %v Invalid Stream State %v.",
			streamId, bucket, state)

	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleFlushDoneMaintStream(cmd Message) {

	logging.LazyTrace(func() string {
		return fmt.Sprintf("Timekeeper::handleFlushDoneMaintStream %v", cmd)
	})

	streamId := cmd.(*MsgMutMgrFlushDone).GetStreamId()
	bucket := cmd.(*MsgMutMgrFlushDone).GetBucket()

	state := tk.ss.streamBucketStatus[streamId][bucket]

	switch state {

	case STREAM_ACTIVE, STREAM_RECOVERY:

		//check if any of the initial build index is past its Build TS.
		//Generate msg for Build Done and change the state of the index.
		flushTs := tk.ss.streamBucketLastFlushedTsMap[streamId][bucket]

		tk.checkInitialBuildDone(streamId, bucket, flushTs)
		tk.checkPendingStreamMerge(streamId, bucket)

		//check if there is any pending TS for this bucket/stream.
		//It can be processed now.
		tk.processPendingTS(streamId, bucket)

	case STREAM_PREPARE_RECOVERY:

		//check if there is any pending TS for this bucket/stream.
		//It can be processed now.
		found := tk.processPendingTS(streamId, bucket)

		if !found && !tk.ss.checkAnyAbortPending(streamId, bucket) {
			sessionId := tk.ss.getSessionId(streamId, bucket)
			//send message to stop running stream
			tk.supvRespch <- &MsgRecovery{mType: INDEXER_PREPARE_RECOVERY,
				streamId:  streamId,
				bucket:    bucket,
				sessionId: sessionId}
		}

	case STREAM_INACTIVE:
		logging.Errorf("Timekeeper::handleFlushDoneMaintStream Unexpected Flush Done "+
			"Received for Inactive StreamId %v.", streamId)

	default:
		logging.Errorf("Timekeeper::handleFlushDoneMaintStream Invalid Stream State %v.", state)

	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleFlushDoneCatchupStream(cmd Message) {

	logging.Tracef("Timekeeper::handleFlushDoneCatchupStream %v", cmd)

	streamId := cmd.(*MsgMutMgrFlushDone).GetStreamId()
	bucket := cmd.(*MsgMutMgrFlushDone).GetBucket()

	state := tk.ss.streamBucketStatus[streamId][bucket]

	switch state {

	case STREAM_ACTIVE:

		if tk.checkCatchupStreamReadyToMerge(cmd) {
			//if stream is ready to merge, further processing is
			//not required, return from here.
			return
		}

		//check if there is any pending TS for this bucket/stream.
		//It can be processed now.
		tk.processPendingTS(streamId, bucket)

	case STREAM_PREPARE_RECOVERY:

		//check if there is any pending TS for this bucket/stream.
		//It can be processed now.
		found := tk.processPendingTS(streamId, bucket)

		//if no more pending TS were found and there is no abort or flush
		//in progress recovery can be initiated
		if !found {
			if tk.checkBucketReadyForRecovery(streamId, bucket) {
				tk.initiateRecovery(streamId, bucket)
			}
		}
	default:

		logging.Errorf("Timekeeper::handleFlushDoneCatchupStream Invalid State Detected. "+
			"CATCHUP_STREAM Can Only Be Flushed in ACTIVE or PREPARE_RECOVERY state. "+
			"Current State %v.", state)
	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleFlushDoneInitStream(cmd Message) {

	logging.LazyTrace(func() string {
		return fmt.Sprintf("Timekeeper::handleFlushDoneInitStream %v", cmd)
	})

	streamId := cmd.(*MsgMutMgrFlushDone).GetStreamId()
	bucket := cmd.(*MsgMutMgrFlushDone).GetBucket()

	state := tk.ss.streamBucketStatus[streamId][bucket]

	switch state {

	case STREAM_ACTIVE:

		//check if any of the initial build index is past its Build TS.
		//Generate msg for Build Done and change the state of the index.
		if tk.checkAnyInitialStateIndex(bucket) {
			flushTs := tk.ss.streamBucketLastFlushedTsMap[streamId][bucket]
			tk.checkInitialBuildDone(streamId, bucket, flushTs)
		} else {

			//if flush is for INIT_STREAM, check if any index in CATCHUP has reached
			//past the last flushed TS of the MAINT_STREAM for this bucket.
			//In such case, all indexes of the bucket can merged to MAINT_STREAM.
			lastFlushedTs := tk.ss.streamBucketLastFlushedTsMap[streamId][bucket]
			if tk.checkInitStreamReadyToMerge(streamId, bucket, lastFlushedTs) {
				//if stream is ready to merge, further STREAM_ACTIVE processing is
				//not required, return from here.
				break
			}
		}

		//check if there is any pending TS for this bucket/stream.
		//It can be processed now.
		tk.processPendingTS(streamId, bucket)

	case STREAM_PREPARE_RECOVERY:

		//check if there is any pending TS for this bucket/stream.
		//It can be processed now.
		found := tk.processPendingTS(streamId, bucket)
		sessionId := tk.ss.getSessionId(streamId, bucket)

		if !found && !tk.ss.checkAnyAbortPending(streamId, bucket) {
			//send message to stop running stream
			tk.supvRespch <- &MsgRecovery{mType: INDEXER_PREPARE_RECOVERY,
				streamId:  streamId,
				bucket:    bucket,
				sessionId: sessionId}
		}

	default:

		logging.Errorf("Timekeeper::handleFlushDoneInitStream Invalid State Detected. "+
			"INIT_STREAM Can Only Be Flushed in ACTIVE or PREPARE_RECOVERY state. "+
			"Current State %v.", state)
	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleFlushAbortDone(cmd Message) {

	logging.Tracef("Timekeeper::handleFlushAbortDone %v", cmd)

	streamId := cmd.(*MsgMutMgrFlushDone).GetStreamId()
	bucket := cmd.(*MsgMutMgrFlushDone).GetBucket()

	tk.lock.Lock()
	defer tk.lock.Unlock()

	state := tk.ss.streamBucketStatus[streamId][bucket]

	switch state {

	case STREAM_ACTIVE:
		logging.Errorf("Timekeeper::handleFlushAbortDone Unexpected Flush Abort "+
			"Received for Active StreamId %v.", streamId)

	case STREAM_PREPARE_RECOVERY:
		sessionId := tk.ss.getSessionId(streamId, bucket)
		bucketFlushInProgressTsMap := tk.ss.streamBucketFlushInProgressTsMap[streamId]
		if _, ok := bucketFlushInProgressTsMap[bucket]; ok {
			//if there is flush in progress, mark it as done
			if bucketFlushInProgressTsMap[bucket] != nil {
				bucketFlushInProgressTsMap[bucket] = nil
				doneCh := tk.ss.streamBucketFlushDone[streamId][bucket]
				if doneCh != nil {
					close(doneCh)
					tk.ss.streamBucketFlushDone[streamId][bucket] = nil
				}
			}
		} else {
			//this bucket is already gone from this stream, may be because
			//the index were dropped. Log and ignore.
			logging.Warnf("Timekeeper::handleFlushDone Ignore Flush Abort for Stream %v "+
				"Bucket %v SessionId %v. Bucket Info Not Found", streamId, bucket, sessionId)
			tk.supvCmdch <- &MsgSuccess{}
			return
		}

		//update abort status and initiate recovery
		bucketAbortInProgressMap := tk.ss.streamBucketAbortInProgressMap[streamId]
		bucketAbortInProgressMap[bucket] = false

		//send message to stop running stream
		tk.supvRespch <- &MsgRecovery{mType: INDEXER_PREPARE_RECOVERY,
			streamId:  streamId,
			bucket:    bucket,
			sessionId: sessionId}

	case STREAM_RECOVERY, STREAM_INACTIVE:
		logging.Errorf("Timekeeper::handleFlushAbortDone Unexpected Flush Abort "+
			"Received for StreamId %v Bucket %v State %v", streamId, bucket, state)

	default:
		logging.Errorf("Timekeeper::handleFlushAbortDone Invalid Stream State %v "+
			"for StreamId %v Bucket %v", state, streamId, bucket)

	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleFlushStateChange(cmd Message) {

	t := cmd.(*MsgTKToggleFlush).GetMsgType()
	streamId := cmd.(*MsgTKToggleFlush).GetStreamId()
	bucket := cmd.(*MsgTKToggleFlush).GetBucket()

	logging.Infof("Timekeeper::handleFlushStateChange Received Flush State Change "+
		"for Bucket: %v StreamId: %v Type: %v", bucket, streamId, t)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	state := tk.ss.streamBucketStatus[streamId][bucket]

	if state == STREAM_INACTIVE {
		logging.Infof("Timekeeper::handleFlushStateChange Ignore Flush State Change "+
			"for Bucket: %v StreamId: %v Type: %v State: %v", bucket, streamId, t, state)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	switch t {

	case TK_ENABLE_FLUSH:

		//if bucket is empty, enable for all buckets in the stream
		bucketFlushEnabledMap := tk.ss.streamBucketFlushEnabledMap[streamId]
		if bucket == "" {
			for bucket, _ := range bucketFlushEnabledMap {
				bucketFlushEnabledMap[bucket] = true
				//if there are any pending TS, send that
				tk.processPendingTS(streamId, bucket)
			}
		} else {
			bucketFlushEnabledMap[bucket] = true
			//if there are any pending TS, send that
			tk.processPendingTS(streamId, bucket)
		}

	case TK_DISABLE_FLUSH:

		//TODO What about the flush which is already in progress
		//if bucket is empty, disable for all buckets in the stream
		bucketFlushEnabledMap := tk.ss.streamBucketFlushEnabledMap[streamId]
		if bucket == "" {
			for bucket, _ := range bucketFlushEnabledMap {
				bucketFlushEnabledMap[bucket] = false
			}
		} else {
			bucketFlushEnabledMap[bucket] = false
		}
	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleGetBucketHWT(cmd Message) {

	logging.Debugf("Timekeeper::handleGetBucketHWT %v", cmd)

	streamId := cmd.(*MsgBucketHWT).GetStreamId()
	bucket := cmd.(*MsgBucketHWT).GetBucket()

	tk.lock.Lock()
	defer tk.lock.Unlock()

	//set the return ts to nil
	msg := cmd.(*MsgBucketHWT)
	msg.ts = nil

	if bucketHWTMap, ok := tk.ss.streamBucketHWTMap[streamId]; ok {
		if ts, ok := bucketHWTMap[bucket]; ok {
			newTs := ts.Copy()
			msg.ts = newTs
		}
	}
	tk.supvCmdch <- msg
}

func (tk *timekeeper) handleStreamBegin(cmd Message) {

	streamId := cmd.(*MsgStream).GetStreamId()
	meta := cmd.(*MsgStream).GetMutationMeta()

	defer meta.Free()

	logging.Infof("TK StreamBegin %v %v %v %v %v %v", streamId, meta.bucket,
		meta.vbucket, meta.vbuuid, meta.seqno, meta.opaque)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	if tk.indexerState == INDEXER_PREPARE_UNPAUSE {
		logging.Warnf("Timekeeper::handleStreamBegin Received StreamBegin In "+
			"Prepare Unpause State. Bucket %v Stream %v. Ignored.", meta.bucket, streamId)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	//check if bucket is active in stream
	if tk.checkBucketActiveInStream(streamId, meta.bucket) == false {
		logging.Warnf("Timekeeper::handleStreamBegin Received StreamBegin for "+
			"Inactive Bucket %v Stream %v. Ignored.", meta.bucket, streamId)
		return
	}

	//if there are no indexes for this bucket and stream, ignore
	if c, ok := tk.ss.streamBucketIndexCountMap[streamId][meta.bucket]; !ok || c <= 0 {
		logging.Warnf("Timekeeper::handleStreamBegin Ignore StreamBegin for StreamId %v "+
			"Bucket %v. IndexCount %v. ", streamId, meta.bucket, c)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	//if the session doesn't match, ignore
	sessionId := tk.ss.getSessionId(streamId, meta.bucket)
	if meta.opaque != 0 && sessionId != meta.opaque {
		logging.Warnf("Timekeeper::handleStreamBegin Ignore StreamBegin for StreamId %v "+
			"Bucket %v. SessionId %v. Current Session %v ", streamId, meta.bucket,
			meta.opaque, sessionId)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	state := tk.ss.streamBucketStatus[streamId][meta.bucket]

	switch state {

	case STREAM_ACTIVE:

		needRepair := false

		// For bookkeeping, timekeeper needs to make sure that it will be eventually correct:
		// 1) There is only one KV node claims ownership for each vb
		// 2) Each KV node accepts the restart seqno for each vb
		//
		// Expected projector behavior (for async mode):
		// 1) stream request only apply to master vb
		// 2) projector ensures all control message are processed in sequence.
		// 3) projector keeps track of pending vb and active vb.
		//    - Pending vb = vb being requested that has not yet acknowledged by kv.
		//    - Active vb = vb acknowledged AND accepted by kv.  The projector has claimed ownership of this vb.
		// 4) projector does not process start/restart for pending vb or active vb.
		// 5) projector does not process vb shutdown for pending vb
		// 6) projector does not claim vb ownerhip if vb is rolled back (it is not an active vb).
		//
		// For bookkeeping,
		// 1) Each vb has a status
		//    VBS_STREAM_BEGIN                  - one node has claimed ownership of this vb
		//                                      - A node CLAIMS ownership even if it sends a Stream Begin with rollack status.
		//    VBS_STERAM_END                    - current owner has released ownership of this vb.
		//                                        Will figure out new ownership by restarting vb.
		//    VBS_INIT (init)                   - ownership unknown -- no projector has claimed ownerhsip yet.
		//    VBS_CONN_ERROR (connection error) - ownership unknown  -- projector may have claimed ownership, but we don't know for certain.
		//                                      - Will figure out ownership by shutdown/restart vb sequence.
		// 2) If there is more than 1 node claims ownership (StreamBegin), it has to raise connection error on the vb (VBS_CONN_ERROR).
		// 3) If there is more than 1 node release ownership (StreamEnd), it will also raise connection error on the vb.
		//    - It is an no-op in receiving StreamEnd until ownership is known
		// 4) After a node release ownership (StreamEnd), it will try to figure out new ownership by restarting vb.
		//    If no node claims ownership after max retry, it will raise connection error.
		// 5) If there is a physical network error, it will raise connection error.
		// 6) If there is missing vb during MTR, flag the error and restart vb.
		// 7) Once all vb has started, it has to start recovery if any vb has rolled back.
		//
		// Raising connection error helps to identify the vb owner:
		// 1) clear book keeping for the vb
		//    - clear the ref count
		//    - clear rollbackTs
		// 2) broadcast vb shutdown to all projectors
		// 3) broadcast vb restart to all projectors
		// 4) start reference counting until receving the first streamBegin
		//    - If there is only 1 vb owner, there would eventually be only single stream-begin (ref count set to 1)
		//
		// Timekeeper only asserts that there is 1 vb owner, but it does not verify the vb owner with ns-server vbmap.  If
		// timekeeper receives multiple Stream Begin, special care must be taken to make sure rollbackTs is coming from the
		// actual vb owner:
		// 1) If multiple Stream Begin causes ref count > 1, connection error is rasied.  This reset rollback Ts and seek
		//    the actual vb owner to re-identify itself (through shutdown/restart).
		// 2) If Stream End is received, clear the rollback Ts.   Stream repair will seek the actual vb owner to re-identify itself.
		//
		// Note:
		// 1) retry (shutdown/restart) due to connection error (VBS_CONN_ERROR) does not have effect on pending vb.
		//    - projector cannot cancel pending vb even if shutting down vb
		// 2) projector can send repeated StreamBegin with ROLLBACK on repeated MTR or RestartVb.
		// 3) Do not assume that projector must send StreamBegin before StreamEnd (depending on dcp)
		//    - a pending vb get rebalanced.  DCP can send StreamEnd on rebalanced.
		//

		// If status is STREAM_SUCCESS, proceed as usual.  Pre-6.5, status is always STREAM_SUCCESS.
		if cmd.(*MsgStream).GetStatus() == common.STREAM_SUCCESS {

			// Update the mapping between a vbucket to KV node UUID
			tk.updateStreamBucketVbMap(cmd)

			// When receivng a StreamBegin, it means that the projector claims ownership
			// of a vbucket.   Keep track of how many projectors are claiming ownership.
			tk.ss.incVbRefCount(streamId, meta.bucket, meta.vbucket)

			//update the HWT of this stream and bucket with the vbuuid
			bucketHWTMap := tk.ss.streamBucketHWTMap[streamId]

			// Always use the vbuuid of the last StreamBegin
			ts := bucketHWTMap[meta.bucket]
			ts.Vbuuids[meta.vbucket] = uint64(meta.vbuuid)

			// New TS needs to be generated for vbuuid change as stale=false scans
			// can only succeed if all vbuuids are latest. Also if there are no docs
			// in a vbucket and its stream begin arrives later than all mutations,
			// there will be no TS with that vbuuid.
			tk.ss.streamBucketNewTsReqdMap[streamId][meta.bucket] = true

			tk.ss.updateVbStatus(streamId, meta.bucket, []Vbucket{meta.vbucket}, VBS_STREAM_BEGIN)

			// record the time when receiving the last StreamBegin
			tk.ss.streamBucketLastBeginTime[streamId][meta.bucket] = uint64(time.Now().UnixNano())

			//  record ActiveTs
			tk.ss.addKVActiveTs(streamId, meta.bucket, meta.vbucket, uint64(meta.seqno), uint64(meta.vbuuid))
			tk.ss.clearKVRollbackTs(streamId, meta.bucket, meta.vbucket)
			tk.ss.clearKVPendingTs(streamId, meta.bucket, meta.vbucket)
			tk.ss.clearLastRepairTime(streamId, meta.bucket, meta.vbucket)
			tk.ss.clearRepairState(streamId, meta.bucket, meta.vbucket)

			if tk.ss.getVbRefCount(streamId, meta.bucket, meta.vbucket) > 1 {
				logging.Infof("Timekeeper::handleStreamBegin \n\tOwner count > 1. Treat as CONN_ERR. "+
					"StreamId %v MutationMeta %v", streamId, meta)

				// This will trigger repairStream, as well as replying to supervisor channel
				tk.handleStreamConnErrorInternal(streamId, meta.bucket, []Vbucket{meta.vbucket})
				return
			}
		}

		// If status is STREAM_ROLLBACK, set rollbackTs in stream state.  Update vb status to
		// STREAM_BEGIN since at least one projector has claimed this vb (even though it needs
		// rollback).  Trigger stream repair.
		// Do not update ref count since projector repeatedly send STREAM_ROLLBACK to indexer.
		if cmd.(*MsgStream).GetStatus() == common.STREAM_ROLLBACK {
			logging.Warnf("Timekeeper::handleStreamBegin StreamBegin rollback for StreamId %v "+
				"Bucket %v vbucket %v. Rollback (%v, %16x). ", streamId, meta.bucket, meta.vbucket, meta.seqno, meta.vbuuid)

			tk.ss.updateVbStatus(streamId, meta.bucket, []Vbucket{meta.vbucket}, VBS_STREAM_BEGIN)

			// record the time when receiving the first rollback for this vb.  Note that projector
			// can keep sending rollback streamBegin on repeated MTR or restartVB.  So only set
			// begin time on the first time we see rollback.
			if _, vbuuid := tk.ss.getKVRollbackTs(streamId, meta.bucket, meta.vbucket); vbuuid == 0 {
				tk.ss.streamBucketLastBeginTime[streamId][meta.bucket] = uint64(time.Now().UnixNano())
			}

			// record the rollback Ts
			tk.ss.addKVRollbackTs(streamId, meta.bucket, meta.vbucket, uint64(meta.seqno), uint64(meta.vbuuid))
			tk.ss.clearKVActiveTs(streamId, meta.bucket, meta.vbucket)
			tk.ss.clearKVPendingTs(streamId, meta.bucket, meta.vbucket)
			tk.ss.clearLastRepairTime(streamId, meta.bucket, meta.vbucket)
			tk.ss.clearRepairState(streamId, meta.bucket, meta.vbucket)

			// Trigger repair.   If this is the last vb, then we will need to rollback.
			needRepair = true
		}

		// Trigger stream repair.
		if needRepair {
			if stopCh, ok := tk.ss.streamBucketRepairStopCh[streamId][meta.bucket]; !ok || stopCh == nil {
				tk.ss.streamBucketRepairStopCh[streamId][meta.bucket] = make(StopChannel)
				logging.Infof("Timekeeper::handleStreamBegin start repairStream. StreamId %v MutationMeta %v", streamId, meta)
				go tk.repairStream(streamId, meta.bucket)
			}
		}

	case STREAM_PREPARE_RECOVERY, STREAM_PREPARE_DONE, STREAM_INACTIVE:
		//ignore stream begin in prepare_recovery
		logging.Verbosef("Timekeeper::handleStreamBegin Ignore StreamBegin "+
			"for StreamId %v State %v MutationMeta %v", streamId, state, meta)

	default:
		logging.Errorf("Timekeeper::handleStreamBegin Invalid Stream State "+
			"StreamId %v Bucket %v State %v", streamId, meta.bucket, state)
	}

	tk.supvCmdch <- &MsgSuccess{}

}

func (tk *timekeeper) handleStreamEnd(cmd Message) {

	streamId := cmd.(*MsgStream).GetStreamId()
	meta := cmd.(*MsgStream).GetMutationMeta()

	defer meta.Free()

	logging.Infof("TK StreamEnd %v %v %v %v %v", streamId, meta.bucket,
		meta.vbucket, meta.vbuuid, meta.seqno)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	if tk.indexerState == INDEXER_PREPARE_UNPAUSE {
		logging.Warnf("Timekeeper::handleStreamEnd Received StreamEnd In "+
			"Prepare Unpause State. Bucket %v Stream %v. Ignored.", meta.bucket, streamId)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	//check if bucket is active in stream
	if tk.checkBucketActiveInStream(streamId, meta.bucket) == false {
		logging.Warnf("Timekeeper::handleStreamEnd Received StreamEnd for "+
			"Inactive Bucket %v Stream %v. Ignored.", meta.bucket, streamId)
		return
	}

	//if there are no indexes for this bucket and stream, ignore
	if c, ok := tk.ss.streamBucketIndexCountMap[streamId][meta.bucket]; !ok || c <= 0 {
		logging.Warnf("Timekeeper::handleStreamEnd Ignore StreamEnd for StreamId %v "+
			"Bucket %v. IndexCount %v. ", streamId, meta.bucket, c)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	//if the session doesn't match, ignore
	sessionId := tk.ss.getSessionId(streamId, meta.bucket)
	if meta.opaque != 0 && sessionId != meta.opaque {
		logging.Warnf("Timekeeper::handleStreamEnd Ignore StreamEnd for StreamId %v "+
			"Bucket %v. SessionId %v. Current Session %v ", streamId, meta.bucket,
			meta.opaque, sessionId)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	state := tk.ss.streamBucketStatus[streamId][meta.bucket]
	switch state {

	case STREAM_ACTIVE:

		// When receivng a StreamEnd, it means that the projector releases ownership of vb.
		// Update the vb count since there is one less owner.  Ignore any StreamEnd if
		// (1) vb has not received a STREAM_BEGIN, (2) recovering from CONN_ERROR
		//
		vbState := tk.ss.getVbStatus(streamId, meta.bucket, meta.vbucket)
		if vbState != VBS_CONN_ERROR && vbState != VBS_INIT {

			tk.ss.decVbRefCount(streamId, meta.bucket, meta.vbucket)
			count := tk.ss.getVbRefCount(streamId, meta.bucket, meta.vbucket)

			if count < 0 {
				// If count < 0, after CONN_ERROR, count is reset to 0.   During rebalancing,
				// residue StreamEnd from old master can still arrive.   Therefore, after
				// CONN_ERROR, the total number of StreamEnd > total number of StreamBegin,
				// and count will be negative in this case.
				logging.Infof("Timekeeper::handleStreamEnd \n\tOwner count < 0. Treat as CONN_ERR. "+
					"StreamId %v MutationMeta %v", streamId, meta)

				tk.handleStreamConnErrorInternal(streamId, meta.bucket, []Vbucket{meta.vbucket})
				return

			} else {

				tk.ss.updateVbStatus(streamId, meta.bucket, []Vbucket{meta.vbucket}, VBS_STREAM_END)
				tk.ss.setLastRepairTime(streamId, meta.bucket, meta.vbucket)

				// clear all Ts for this vbucket
				tk.ss.clearKVActiveTs(streamId, meta.bucket, meta.vbucket)
				tk.ss.clearKVPendingTs(streamId, meta.bucket, meta.vbucket)
				tk.ss.clearKVRollbackTs(streamId, meta.bucket, meta.vbucket)
				tk.ss.clearRepairState(streamId, meta.bucket, meta.vbucket)

				// If Count => 0.  This could be just normal vb take-over during rebalancing.
				if stopCh, ok := tk.ss.streamBucketRepairStopCh[streamId][meta.bucket]; !ok || stopCh == nil {
					tk.ss.streamBucketRepairStopCh[streamId][meta.bucket] = make(StopChannel)
					logging.Infof("Timekeeper::handleStreamEnd RepairStream due to StreamEnd. "+
						"StreamId %v MutationMeta %v", streamId, meta)
					go tk.repairStream(streamId, meta.bucket)
				}
			}
		}

	case STREAM_PREPARE_RECOVERY, STREAM_PREPARE_DONE, STREAM_INACTIVE:
		//ignore stream end in prepare_recovery
		logging.Verbosef("Timekeeper::handleStreamEnd Ignore StreamEnd for "+
			"StreamId %v State %v MutationMeta %v", streamId, state, meta)

	default:
		logging.Errorf("Timekeeper::handleStreamEnd Invalid Stream State "+
			"StreamId %v Bucket %v State %v", streamId, meta.bucket, state)
	}

	tk.supvCmdch <- &MsgSuccess{}

}

func (tk *timekeeper) handleStreamConnError(cmd Message) {

	logging.Debugf("Timekeeper::handleStreamConnError %v", cmd)

	streamId := cmd.(*MsgStreamInfo).GetStreamId()
	bucket := cmd.(*MsgStreamInfo).GetBucket()
	vbList := cmd.(*MsgStreamInfo).GetVbList()

	logging.Infof("TK ConnError %v %v %v", streamId, bucket, vbList)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	if len(bucket) != 0 && len(vbList) != 0 {
		tk.handleStreamConnErrorInternal(streamId, bucket, vbList)
	} else {
		tk.supvCmdch <- &MsgSuccess{}
	}

	if tk.vbCheckerStopCh == nil {
		logging.Infof("Timekeeper::handleStreamConnError \n\t Call RepairMissingStreamBegin to check for vbucket for repair. "+
			"StreamId %v bucket %v", streamId, bucket)
		tk.vbCheckerStopCh = make(chan bool)
		go tk.repairMissingStreamBegin(streamId)
	}
}

func (tk *timekeeper) computeVbWithMissingStreamBegin(streamId common.StreamId) BucketVbStatusMap {

	result := make(BucketVbStatusMap)

	for bucket, ts := range tk.ss.streamBucketVbStatusMap[streamId] {

		// only check for stream in ACTIVE or RECOVERY state
		bucketStatus := tk.ss.streamBucketStatus[streamId][bucket]
		if bucketStatus == STREAM_ACTIVE || bucketStatus == STREAM_RECOVERY {
			hasMissing := false
			for _, status := range ts {
				if status == VBS_INIT {
					hasMissing = true
				}
			}

			if hasMissing {
				result[bucket] = CopyTimestamp(ts)
			}
		}
	}

	return result
}

func (tk *timekeeper) repairMissingStreamBegin(streamId common.StreamId) {

	logging.Infof("timekeeper.repairMissingStreamBegin stream %v", streamId)

	defer func() {
		tk.lock.Lock()
		defer tk.lock.Unlock()
		tk.vbCheckerStopCh = nil
	}()

	// compute any vb that is missing StreamBegin
	missing := func() BucketVbStatusMap {
		tk.lock.Lock()
		defer tk.lock.Unlock()
		return tk.computeVbWithMissingStreamBegin(streamId)
	}()

	for len(missing) != 0 {

		// Let's sleep and check for vb at a later time.  If a vb has missing StreamBegin,
		// it could mean that the projector is still sending StreamBegin to indexer.  So
		// let's wait a little longer for TK to recieve those in-flight streamBegin.
		ticker := time.After(2 * time.Second)
		select {
		case <-tk.vbCheckerStopCh:
			return
		case <-ticker:
		}

		missing = func() BucketVbStatusMap {

			tk.lock.Lock()
			defer tk.lock.Unlock()

			now := uint64(time.Now().UnixNano())

			// After sleep, find the vb that has not yet received StreamBegin.  If the same set
			// of vb are still missing before and after sleep, then we can assume those vb will
			// need to repair.   If the set of vb are not identical, then retry again.
			newMissing := tk.computeVbWithMissingStreamBegin(streamId)

			toFix := make(map[string][]Vbucket)
			for bucket, oldTs := range missing {

				vbList := []Vbucket(nil)

				// if stream request is not done (we have not yet got
				// the activeTs), then do not process this. Also wait for
				// 30s after stream request before checking.
				if tk.ss.streamBucketOpenTsMap[streamId][bucket] == nil ||
					tk.ss.streamBucketStartTimeMap[streamId][bucket] == 0 ||
					now-tk.ss.streamBucketStartTimeMap[streamId][bucket] < uint64(30*time.Second) {
					// if the bucket still exist
					if newTs, ok := newMissing[bucket]; ok && newTs != nil {
						delete(newMissing, bucket)
						newMissing[bucket] = oldTs
					}
				} else {
					// compare the vb status before and after sleep
					for i, oldStatus := range oldTs {
						if oldStatus == VBS_INIT {
							if newStatus, ok := newMissing[bucket]; ok && newStatus[i] == oldStatus {
								vbList = append(vbList, Vbucket(i))
							}
						}
					}
				}

				// if the vb are still missing StreamBegin, then we need to repair this bucket.
				if len(vbList) != 0 {
					toFix[bucket] = vbList
				}
			}

			// Repair the vb in the bucket
			maxInterval := uint64(tk.config["timekeeper.escalate.StreamBeginWaitTime"].Int()) * uint64(time.Second)

			for bucket, vbList := range toFix {

				bucketStatus := tk.ss.streamBucketStatus[streamId][bucket]
				if len(vbList) != 0 && (bucketStatus == STREAM_ACTIVE || bucketStatus == STREAM_RECOVERY) {

					// If there is missing vb and timekeeper has not received StreamBegin for any vb for over the threshold,
					// then raise an error on those vbs.
					if now-uint64(tk.ss.streamBucketLastBeginTime[streamId][bucket]) > uint64(maxInterval) {

						//flag the missing vb as error and repair stream.  Do not raise connection error.
						for _, vb := range vbList {
							tk.ss.updateVbStatus(streamId, bucket, []Vbucket{vb}, VBS_STREAM_END)
							tk.ss.clearVbRefCount(streamId, bucket, vb)

							// clear all Ts for this vbucket
							tk.ss.clearKVActiveTs(streamId, bucket, vb)
							tk.ss.clearKVPendingTs(streamId, bucket, vb)
							tk.ss.clearKVRollbackTs(streamId, bucket, vb)
							tk.ss.clearRepairState(streamId, bucket, vb)
							tk.ss.setLastRepairTime(streamId, bucket, vb)
						}

						if stopCh, ok := tk.ss.streamBucketRepairStopCh[streamId][bucket]; !ok || stopCh == nil {
							tk.ss.streamBucketRepairStopCh[streamId][bucket] = make(StopChannel)
							logging.Infof("Timekeeper::repairWithMissingStreamBegin. Repair StreamId %v bucket %v vbuckets %v", streamId, bucket, vbList)
							go tk.repairStream(streamId, bucket)
						}
					}
				}
			}

			// Return any vb that still has missing streamBegin
			return newMissing
		}()
	}

	logging.Infof("timekeeper.repairMissingStreamBegin stream %v done", streamId)
}

func (tk *timekeeper) handleStreamConnErrorInternal(streamId common.StreamId, bucket string, vbList []Vbucket) {

	if tk.indexerState == INDEXER_PREPARE_UNPAUSE {
		logging.Warnf("Timekeeper::handleStreamConnError Received ConnError In "+
			"Prepare Unpause State. Bucket %v Stream %v. Ignored.", bucket, streamId)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	//check if bucket is active in stream
	if tk.checkBucketActiveInStream(streamId, bucket) == false {
		logging.Warnf("Timekeeper::handleStreamConnError \n\tReceived ConnError for "+
			"Inactive Bucket %v Stream %v. Ignored.", bucket, streamId)
		return
	}

	state := tk.ss.streamBucketStatus[streamId][bucket]
	switch state {

	case STREAM_ACTIVE:

		// ConnErr is generated by dataport when the physical connection is disconnected.
		// Dataport keeps track of the active vb for each connection/projector (by
		// book-keeping StreamBegin and StreamEnd).  So we know that for that vb, the last
		// control msg from the connection is a StreamBegin.  Dataport is sensitive to order
		// of messages arriving at indexer.   As such, bookkeeping at dataport is only
		// advisory and provides the following semantics:
		// 1) Any StreamBegin and StreamEnd will pass through to the indexer. Each
		//    StreamBegin and StreamEnd will carry corresponding vb information.
		// 2) For connection error, dataport provides a list of "potential" vb that
		//    require repair, but this list is not an exhaustive list.
		//    a) Dataport could recieve a StreamEnd from old projector owner, while
		//       getting a connection error from new vb owner, therefore missing a
		//       StreamBegin.  Therefore, dataport will not know that projector owns a vb.
		//    b) Dataport could receive StreamBegin from the new owner, before getting
		//       StreamBegin from the old owner, if the vb owner can change hands
		//       multiple times in a short span.   Theforefore, if the connection to
		//       the old owner dies, dataport may think that the vb needs repair.

		// Upon receiving connection error, the state of the projecto is unclear, so it is
		// required for the projector to relinquish its vb ownership.  This is done by
		// explicitly asking the projector to shutdown/restart vb.  By shutdown/restart,
		// the current vb master (according to ns-server vbmap) will send a new pair
		// of StreamEnd/StreamBegin sequence.  Note that the proejctor may not send
		// StreamEnd if vb has not started yet at the current vb master.  Any message
		// after CONN_ERR is dropped until the shutdown/restart sequence kick off.
		// Messages from a single projector is ordered, but indexer can no longer
		// guarantee that the vb ref count is accurate upon connection error.

		// In case of rebalancing, the vb ownership can change during CONN_ERROR.
		// Mutliple projectors can send StreamBegin/StreamEnd to indexer,  and
		// there is no certainty if any StreamBegin/StreamEnd message may be lost
		// due to 1 or more CONN_ERROR.   Messages from different projectors can
		// also be out-of-order.
		// 1) For each projector, StreamBegin must happen before StreamEnd.
		// 2) For CONN_ERROR on a projector, indexer may miss both StreamBegin/StreamEnd,
		//    or missing StreamEnd.   But it cannot miss a StreamBegin and not missing
		//    StreamEnd.
		// 3) Base on (1) and (2), during CONN_ERROR, num of StreamBegin can be
		//    greater than StreamEnd, even if there is no projector owning that vb (restart
		//    sequence has not started).

		// As such, once the indexer receives connection error, it will do the following:
		// 1) Reset the vb ownership counter to 0.
		// 2) Vb state would remain to be CONN_ERR until a new STREAM_BEGIN arrives.
		// 3) While vb state in CONN_ERROR, indexer will not decrement counter when it
		//    receives a StreamEnd.  Once it receives a new StreamBegin, it would restart
		//    the counting sequence.
		// 4) If counter < 0, it indicates StreamEnd and StreamBegin are out-of-order,
		//    possibly because count is reset at CONN_ERROR during rebalancing.
		//    Vb state will set to CONN_ERROR before repair stream.
		// 5) If counter == 0, it could be normal rebalancing, or out-of-order
		//    StreamBegin and StreamEnd.   Vb state will be set to STREAM_END.   If
		//    repair is not successfully after N tries, vb state will be set to CONN_ERROR.
		// 6) After the stream is repaired, if there is any vb with count != 1, then the
		//    vb needs to be shutdown/retart.

		// By resetting count on CONN_ERROR, indexer ignores any counting based on previous
		// StreamBegin and StreamEnd.   Any residual StreamBegin or StreamEnd (arrive after
		// CONN_ERROR) can bring the count <= 0 or count > 1.   In either case, a new
		// shutdown/restart sequence will be triggered with count being reset at each try,
		// until there is no residual message and there is one projector acknowledges as
		// the vb master.   Acknowledgement means that indexer is assured that at least
		// one projector does not return an error during restart, without relying on
		// arrival of StreamBegin as indication of successful restart.

		for _, vb := range vbList {
			tk.ss.makeConnectionError(streamId, bucket, vb)
		}

		if stopCh, ok := tk.ss.streamBucketRepairStopCh[streamId][bucket]; !ok || stopCh == nil {
			tk.ss.streamBucketRepairStopCh[streamId][bucket] = make(StopChannel)
			logging.Infof("Timekeeper::handleStreamConnError RepairStream due to ConnError. "+
				"StreamId %v Bucket %v VbList %v", streamId, bucket, vbList)
			go tk.repairStream(streamId, bucket)
		} else {
			logging.Infof("Timekeeper::handleStreamConnErr Stream repair is already in progress "+
				"for stream: %v, bucket: %v", streamId, bucket)
		}

	case STREAM_PREPARE_RECOVERY, STREAM_PREPARE_DONE, STREAM_INACTIVE:
		logging.Verbosef("Timekeeper::handleStreamConnError Ignore Connection Error "+
			"for StreamId %v Bucket %v State %v", streamId, bucket, state)

	default:
		logging.Errorf("Timekeeper::handleStreamConnError Invalid Stream State "+
			"StreamId %v Bucket %v State %v", streamId, bucket, state)
	}

	tk.supvCmdch <- &MsgSuccess{}

}

func (tk *timekeeper) handleInitBuildDoneAck(cmd Message) {

	streamId := cmd.(*MsgTKInitBuildDone).GetStreamId()
	bucket := cmd.(*MsgTKInitBuildDone).GetBucket()
	mergeTs := cmd.(*MsgTKInitBuildDone).GetMergeTs()

	logging.Infof("Timekeeper::handleInitBuildDoneAck StreamId %v Bucket %v",
		streamId, bucket)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	state := tk.ss.streamBucketStatus[streamId][bucket]

	//ignore build done ack for Inactive and Recovery phase. For recovery, stream
	//will get reopen and build done will get recomputed.
	if state == STREAM_INACTIVE || state == STREAM_PREPARE_DONE ||
		state == STREAM_PREPARE_RECOVERY {
		logging.Infof("Timekeeper::handleInitBuildDoneAck Ignore BuildDoneAck "+
			"for Bucket: %v StreamId: %v State: %v", bucket, streamId, state)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	//if buildDoneAck is received for INIT_STREAM, this means the index got
	//successfully added to MAINT_STREAM. Set the buildDoneAck flag and the
	//mergeTs for the Catchup state indexes.
	if streamId == common.INIT_STREAM {
		tk.setAddInstPending(streamId, bucket, false)
		if mergeTs != nil {
			tk.setMergeTs(streamId, bucket, mergeTs)
		} else {
			//BuildDoneAck should always have a valid mergeTs. This comes from projector when
			//the index gets added to stream. It cannot be nil otherwise merge will get stuck.
			logging.Fatalf("Timekeeper::handleInitBuildDoneAck %v %v. Received unexpected nil mergeTs.",
				streamId, bucket)
			common.CrashOnError(errors.New("Nil MergeTs Received"))
		}
	}

	if !tk.ss.checkAnyFlushPending(streamId, bucket) &&
		!tk.ss.checkAnyAbortPending(streamId, bucket) {

		lastFlushedTs := tk.ss.streamBucketLastFlushedTsMap[streamId][bucket]
		if tk.checkInitStreamReadyToMerge(streamId, bucket, lastFlushedTs) {
			//if stream is ready to merge, further STREAM_ACTIVE processing is
			//not required, return from here.
			tk.supvCmdch <- &MsgSuccess{}
			return
		}

		//check if there is any pending TS for this bucket/stream.
		//It can be processed now.
		tk.processPendingTS(streamId, bucket)

	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleAddInstanceFail(cmd Message) {

	streamId := cmd.(*MsgTKInitBuildDone).GetStreamId()
	bucket := cmd.(*MsgTKInitBuildDone).GetBucket()

	logging.Infof("Timekeeper::handleAddInstanceFail StreamId %v Bucket %v",
		streamId, bucket)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	state := tk.ss.streamBucketStatus[streamId][bucket]

	//ignore build done ack for Inactive and Recovery phase. For recovery, stream
	//will get reopen and build done will get recomputed.
	if state == STREAM_INACTIVE || state == STREAM_PREPARE_DONE ||
		state == STREAM_PREPARE_RECOVERY {
		logging.Infof("Timekeeper::handleAddInstanceFail Ignore AddInstanceFail "+
			"for Bucket: %v StreamId: %v State: %v", bucket, streamId, state)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	//if AddInstance fails, reset the AddInstPending flag. Recovery will
	//add these instances back to projector bookkeeping.
	if streamId == common.INIT_STREAM {
		tk.setAddInstPending(streamId, bucket, false)
	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleMergeStreamAck(cmd Message) {

	streamId := cmd.(*MsgTKMergeStream).GetStreamId()
	bucket := cmd.(*MsgTKMergeStream).GetBucket()

	logging.Infof("Timekeeper::handleMergeStreamAck StreamId %v Bucket %v",
		streamId, bucket)

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleStreamRequestDone(cmd Message) {

	streamId := cmd.(*MsgStreamInfo).GetStreamId()
	bucket := cmd.(*MsgStreamInfo).GetBucket()
	activeTs := cmd.(*MsgStreamInfo).GetActiveTs()
	pendingTs := cmd.(*MsgStreamInfo).GetPendingTs()

	logging.Infof("Timekeeper::handleStreamRequestDone StreamId %v Bucket %v",
		streamId, bucket)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	tk.ss.streamBucketKVActiveTsMap[streamId][bucket] = activeTs
	openTs := activeTs

	tk.ss.streamBucketKVPendingTsMap[streamId][bucket] = pendingTs
	if pendingTs != nil {
		openTs = pendingTs.Union(openTs)
	}

	if openTs == nil {
		openTs = common.NewTsVbuuid(bucket, tk.config["numVbuckets"].Int())
	}

	tk.ss.streamBucketOpenTsMap[streamId][bucket] = openTs
	tk.ss.streamBucketStartTimeMap[streamId][bucket] = uint64(time.Now().UnixNano())

	//Check for possiblity of build done after stream request done.
	//In case of crash recovery, if there are no mutations, there is
	//no flush happening, which can cause index to be in initial state.
	if !tk.ss.checkAnyFlushPending(streamId, bucket) &&
		!tk.ss.checkAnyAbortPending(streamId, bucket) {
		lastFlushedTs := tk.ss.streamBucketLastFlushedTsMap[streamId][bucket]
		tk.checkInitialBuildDone(streamId, bucket, lastFlushedTs)
	}

	//If the indexer crashed after processing all mutations but before
	//merge of an index in Catchup state, the merge needs to happen here,
	//as no flush would happen in case there are no more mutations.
	tk.checkPendingStreamMerge(streamId, bucket)

	if tk.indexerState == INDEXER_PREPARE_UNPAUSE {
		logging.Warnf("Timekeeper::handleStreamRequestDone Skip Repair Check In "+
			"Prepare Unpause State. Bucket %v Stream %v.", bucket, streamId)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	// Check if the stream needs repair for streamEnd and ConnErr
	if stopCh, ok := tk.ss.streamBucketRepairStopCh[streamId][bucket]; !ok || stopCh == nil {
		tk.ss.streamBucketRepairStopCh[streamId][bucket] = make(StopChannel)
		logging.Infof("Timekeeper::handleStreamRequestDone Call RepairStream to check for vbucket for repair. "+
			"StreamId %v bucket %v", streamId, bucket)
		go tk.repairStream(streamId, bucket)
	}

	// Check if the stream needs repair for missing streamBegin
	if tk.vbCheckerStopCh == nil {
		logging.Infof("Timekeeper::handleStreamRequestDone Call RepairMissingStreamBegin to check for vbucket for repair. "+
			"StreamId %v bucket %v", streamId, bucket)
		tk.vbCheckerStopCh = make(chan bool)
		go tk.repairMissingStreamBegin(streamId)
	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleRecoveryDone(cmd Message) {

	streamId := cmd.(*MsgRecovery).GetStreamId()
	bucket := cmd.(*MsgRecovery).GetBucket()
	mergeTs := cmd.(*MsgRecovery).GetRestartTs()
	activeTs := cmd.(*MsgRecovery).GetActiveTs()
	pendingTs := cmd.(*MsgRecovery).GetPendingTs()

	logging.Infof("Timekeeper::handleRecoveryDone StreamId %v Bucket %v",
		streamId, bucket)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	if tk.ss.streamBucketForceRecovery[streamId][bucket] {

		sessionId := tk.ss.getSessionId(streamId, bucket)

		logging.Infof("Timekeeper::handleRecoveryDone %v %v. Initiate Recovery "+
			"due to Force Recovery Flag. SessionId %v.", streamId, bucket, sessionId)

		tk.supvRespch <- &MsgRecovery{mType: INDEXER_INIT_PREP_RECOVERY,
			streamId:  streamId,
			bucket:    bucket,
			restartTs: tk.ss.computeRestartTs(streamId, bucket),
			sessionId: sessionId}
		tk.supvCmdch <- &MsgSuccess{}
		return

	}

	tk.ss.streamBucketKVActiveTsMap[streamId][bucket] = activeTs
	openTs := activeTs

	tk.ss.streamBucketKVPendingTsMap[streamId][bucket] = pendingTs
	if pendingTs != nil {
		openTs = pendingTs.Union(openTs)
	}

	if openTs == nil {
		openTs = common.NewTsVbuuid(bucket, tk.config["numVbuckets"].Int())
	}

	tk.resetWaitForRecovery(streamId, bucket)
	tk.ss.streamBucketOpenTsMap[streamId][bucket] = openTs
	tk.ss.streamBucketStartTimeMap[streamId][bucket] = uint64(time.Now().UnixNano())

	//once MAINT_STREAM gets successfully restarted in recovery, use the restartTs
	//as the mergeTs for Catchup indexes. As part of the MTR, Catchup state indexes
	//get added to MAINT_STREAM.
	if streamId == common.MAINT_STREAM {
		if mergeTs == nil {
			logging.Infof("Timekeeper::handleRecoveryDone %v %v. Received nil mergeTs. "+
				"Considering it as rollback to 0", streamId, bucket)
			numVbuckets := tk.config["numVbuckets"].Int()
			mergeTs = common.NewTsVbuuid(bucket, numVbuckets)
		}
		tk.setMergeTs(streamId, bucket, mergeTs)
	}

	//Check for possiblity of build done after recovery is done.
	//In case of crash recovery, if there are no mutations, there is
	//no flush happening, which can cause index to be in initial state.
	if !tk.ss.checkAnyFlushPending(streamId, bucket) &&
		!tk.ss.checkAnyAbortPending(streamId, bucket) {
		lastFlushedTs := tk.ss.streamBucketLastFlushedTsMap[streamId][bucket]
		tk.checkInitialBuildDone(streamId, bucket, lastFlushedTs)
	}

	//If the indexer crashed after processing all mutations but before
	//merge of an index in Catchup state, the merge needs to happen here,
	//as no flush would happen in case there are no more mutations.
	tk.checkPendingStreamMerge(streamId, bucket)

	if tk.indexerState == INDEXER_PREPARE_UNPAUSE {
		logging.Warnf("Timekeeper::handleRecoveryDone Skip Repair Check In "+
			"Prepare Unpause State. Bucket %v Stream %v.", bucket, streamId)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	// Check if the stream needs repair
	if stopCh, ok := tk.ss.streamBucketRepairStopCh[streamId][bucket]; !ok || stopCh == nil {
		tk.ss.streamBucketRepairStopCh[streamId][bucket] = make(StopChannel)
		logging.Infof("Timekeeper::handleRecoveryDone Call RepairStream to check for vbucket for repair. "+
			"StreamId %v bucket %v", streamId, bucket)
		go tk.repairStream(streamId, bucket)
	}

	// Check if the stream needs repair for missing streamBegin
	if tk.vbCheckerStopCh == nil {
		logging.Infof("Timekeeper::handleRecoveryDone Call RepairMissingStreamBegin to check for vbucket for repair. "+
			"StreamId %v bucket %v", streamId, bucket)
		tk.vbCheckerStopCh = make(chan bool)
		go tk.repairMissingStreamBegin(streamId)
	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleAbortRecovery(cmd Message) {

	logging.Infof("Timekeeper::handleAbortRecovery %v", cmd)

	streamId := cmd.(*MsgRecovery).GetStreamId()
	bucket := cmd.(*MsgRecovery).GetBucket()

	tk.lock.Lock()
	defer tk.lock.Unlock()

	tk.removeBucketFromStream(streamId, bucket, true)

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleConfigUpdate(cmd Message) {
	cfgUpdate := cmd.(*MsgConfigUpdate)
	tk.config = cfgUpdate.GetConfig()
	tk.ss.UpdateConfig(tk.config)

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) prepareRecovery(streamId common.StreamId,
	bucket string) bool {

	logging.Infof("Timekeeper::prepareRecovery StreamId %v Bucket %v",
		streamId, bucket)

	//change to PREPARE_RECOVERY so that
	//no more TS generation and flush happen.
	if tk.ss.streamBucketStatus[streamId][bucket] == STREAM_ACTIVE {

		logging.Infof("Timekeeper::prepareRecovery Stream %v "+
			"Bucket %v State Changed to PREPARE_RECOVERY", streamId, bucket)

		tk.ss.streamBucketStatus[streamId][bucket] = STREAM_PREPARE_RECOVERY

		//The existing mutations for which stability TS has already been generated
		//can be safely flushed before initiating recovery. This can reduce the duration
		//of recovery.
		tk.flushOrAbortInProgressTS(streamId, bucket)

	} else {

		logging.Errorf("Timekeeper::prepareRecovery Invalid Prepare Recovery Request")
		return false

	}

	return true

}

func (tk *timekeeper) flushOrAbortInProgressTS(streamId common.StreamId,
	bucket string) {

	bucketFlushInProgressTsMap := tk.ss.streamBucketFlushInProgressTsMap[streamId]
	if ts := bucketFlushInProgressTsMap[bucket]; ts != nil {

		//If Flush is in progress for the bucket, check if all the mutations
		//required for this flush have already been received. If not, this flush
		//needs to be aborted. Pending TS for the bucket can be discarded and
		//all remaining mutations in queue can be drained.
		//Stream can then be moved to RECOVERY

		//TODO this check is best effort right now as there may be more
		//mutations in the queue than what we have received the SYNC messages
		//for. We may end up discarding one TS worth of mutations unnecessarily,
		//but that is fine for now.
		tsVbuuidHWT := tk.ss.streamBucketHWTMap[streamId][bucket]
		tsHWT := getSeqTsFromTsVbuuid(tsVbuuidHWT)

		flushTs := getSeqTsFromTsVbuuid(ts)

		//if HWT is greater than flush in progress TS, this means the flush
		//in progress will finish.
		if tsHWT.GreaterThanEqual(flushTs) {
			logging.Infof("Timekeeper::flushOrAbortInProgressTS Processing Flush TS %v "+
				"before recovery for bucket %v streamId %v", ts, bucket, streamId)
		} else {
			//else this flush needs to be aborted. Though some mutations may
			//arrive and flush may get completed before this abort message
			//reaches.
			logging.Infof("Timekeeper::flushOrAbortInProgressTS Aborting Flush TS %v "+
				"before recovery for bucket %v streamId %v", ts, bucket, streamId)

			tk.supvRespch <- &MsgMutMgrFlushMutationQueue{mType: MUT_MGR_ABORT_PERSIST,
				bucket:   bucket,
				streamId: streamId}

			bucketAbortInProgressMap := tk.ss.streamBucketAbortInProgressMap[streamId]
			bucketAbortInProgressMap[bucket] = true

		}
		//TODO As prepareRecovery is now done only if rollback is received,
		//no point flushing more mutations. Its going to be rolled back anyways.
		//Once Catchup Stream is back, move this logic back.
		//empty the TSList for this bucket and stream so
		//there is no further processing for this bucket.
		tsList := tk.ss.streamBucketTsListMap[streamId][bucket]
		tsList.Init()

	} else {

		//for the buckets where no flush is in progress, it implies
		//there are no pending TS. So nothing needs to be done and
		//recovery can be initiated.
		sessionId := tk.ss.getSessionId(streamId, bucket)
		logging.Infof("Timekeeper::flushOrAbortInProgressTS Recovery can be initiated for "+
			"Bucket %v Stream %v SessionId %v", bucket, streamId, sessionId)

		//send message to stop running stream
		tk.supvRespch <- &MsgRecovery{mType: INDEXER_PREPARE_RECOVERY,
			streamId:  streamId,
			bucket:    bucket,
			sessionId: sessionId}

	}

}

//checkInitialBuildDone checks if any of the index in Initial State is past its
//Build TS based on the Flush Done Message. It generates msg for Build Done
//and changes the state of the index.
func (tk *timekeeper) checkInitialBuildDone(streamId common.StreamId,
	bucket string, flushTs *common.TsVbuuid) bool {

	for _, buildInfo := range tk.indexBuildInfo {

		if buildInfo.waitForRecovery {
			continue
		}

		//if index belongs to the flushed bucket and in INITIAL state
		initBuildDone := false
		idx := buildInfo.indexInst
		if idx.Defn.Bucket == bucket &&
			idx.Stream == streamId &&
			idx.State == common.INDEX_STATE_INITIAL {

			if buildInfo.buildTs == nil {
				initBuildDone = false
			} else if buildInfo.buildTs.IsZeroTs() { //if buildTs is zero, initial build is done
				initBuildDone = true
			} else if flushTs == nil {
				initBuildDone = false
			} else if !flushTs.IsSnapAligned() {
				initBuildDone = false
			} else {
				//check if the flushTS is greater than buildTS
				ts := getSeqTsFromTsVbuuid(flushTs)
				if ts.GreaterThanEqual(buildInfo.buildTs) {
					initBuildDone = true
				}
			}

			if initBuildDone {
				//change all indexes of this bucket to Catchup state if the flush
				//is for INIT_STREAM
				if streamId == common.INIT_STREAM {
					tk.changeIndexStateForBucket(bucket, common.INDEX_STATE_CATCHUP)
				} else {
					//cleanup all indexes for bucket as build is done
					for _, buildInfo := range tk.indexBuildInfo {
						if buildInfo.indexInst.Defn.Bucket == bucket {
							logging.Infof("Timekeeper::checkInitialBuildDone remove "+
								"index %v from stream %v bucket %v", buildInfo.indexInst.InstId,
								streamId, bucket)
							delete(tk.indexBuildInfo, buildInfo.indexInst.InstId)
						}
					}
				}

				sessionId := tk.ss.getSessionId(streamId, bucket)
				logging.Infof("Timekeeper::checkInitialBuildDone Initial Build Done Index: %v "+
					"Stream: %v Bucket: %v Session: %v BuildTS: %v", idx.InstId, streamId,
					bucket, sessionId, buildInfo.buildTs)

				tk.setAddInstPending(streamId, bucket, true)

				//generate init build done msg
				tk.supvRespch <- &MsgTKInitBuildDone{
					mType:     TK_INIT_BUILD_DONE,
					streamId:  streamId,
					buildTs:   buildInfo.buildTs,
					bucket:    bucket,
					sessionId: sessionId}

				return true

			}
		}
	}
	return false
}

//checkInitStreamReadyToMerge checks if any index in Catchup State in INIT_STREAM
//has reached past the last flushed TS of the MAINT_STREAM for this bucket.
//In such case, all indexes of the bucket can merged to MAINT_STREAM.
func (tk *timekeeper) checkInitStreamReadyToMerge(streamId common.StreamId,
	bucket string, initFlushTs *common.TsVbuuid) bool {

	logging.LazyTrace(func() string {
		return fmt.Sprintf("Timekeeper::checkInitStreamReadyToMerge \n\t Stream %v Bucket %v len(buildInfo) %v "+
			"FlushTs %v", streamId, bucket, len(tk.indexBuildInfo), initFlushTs)
	})

	if streamId != common.INIT_STREAM {
		return false
	}

	//if flushTs is not on snap boundary, merge cannot be done
	if !initFlushTs.IsSnapAligned() {
		hwt := tk.ss.streamBucketHWTMap[streamId][bucket]
		logging.Infof("Timekeeper::checkInitStreamReadyToMerge FlushTs Not Snapshot "+
			"Aligned. Continue both streams for bucket %v.", bucket)
		logging.LazyVerbose(func() string {
			return fmt.Sprintf("Timekeeper::checkInitStreamReadyToMerge FlushTs %v\n HWT %v", initFlushTs, hwt)
		})
		return false
	}

	//INIT_STREAM cannot be merged to MAINT_STREAM if its not ACTIVE
	if tk.ss.streamBucketStatus[common.MAINT_STREAM][bucket] != STREAM_ACTIVE {
		logging.Infof("Timekeeper::checkInitStreamReadyToMerge MAINT_STREAM in %v. "+
			"INIT_STREAM cannot be merged. Continue both streams for bucket %v.",
			tk.ss.streamBucketStatus[common.MAINT_STREAM][bucket], bucket)
		return false
	}

	//If any repair is going on, merge cannot happen
	if stopCh, ok := tk.ss.streamBucketRepairStopCh[common.MAINT_STREAM][bucket]; ok && stopCh != nil {

		logging.Infof("Timekeeper::checkInitStreamReadyToMerge MAINT_STREAM In Repair."+
			"INIT_STREAM cannot be merged. Continue both streams for bucket %v.", bucket)
		return false
	}

	if stopCh, ok := tk.ss.streamBucketRepairStopCh[common.INIT_STREAM][bucket]; ok && stopCh != nil {

		logging.Infof("Timekeeper::checkInitStreamReadyToMerge INIT_STREAM In Repair."+
			"INIT_STREAM cannot be merged. Continue both streams for bucket %v.", bucket)
		return false
	}

	if ts, ok := tk.ss.streamBucketOpenTsMap[common.INIT_STREAM][bucket]; !ok || ts == nil {

		logging.Infof("Timekeeper::checkInitStreamReadyToMerge INIT_STREAM MTR In Progress."+
			"INIT_STREAM cannot be merged. Continue both streams for bucket %v.", bucket)
		return false
	}

	for _, buildInfo := range tk.indexBuildInfo {

		//if index belongs to the flushed bucket and in CATCHUP state and
		//buildDoneAck has been received
		idx := buildInfo.indexInst
		if idx.Defn.Bucket == bucket &&
			idx.State == common.INDEX_STATE_CATCHUP &&
			buildInfo.buildDoneAckReceived == true &&
			buildInfo.minMergeTs != nil {

			readyToMerge, initTsSeq := tk.checkFlushTsValidForMerge(streamId, bucket, initFlushTs, buildInfo.minMergeTs)

			if readyToMerge {

				//disable flush for MAINT_STREAM for this bucket, so it doesn't
				//move ahead till merge is complete
				tk.ss.streamBucketFlushEnabledMap[common.MAINT_STREAM][bucket] = false

				//if bucket in INIT_STREAM is going to merge, disable flush. No need to waste
				//resources on flush as these mutations will be flushed from MAINT_STREAM anyway.
				tk.ss.streamBucketFlushEnabledMap[common.INIT_STREAM][bucket] = false

				//change state of all indexes of this bucket to ACTIVE
				//these indexes get removed later as part of merge message
				//from indexer
				tk.changeIndexStateForBucket(bucket, common.INDEX_STATE_ACTIVE)

				sessionId := tk.ss.getSessionId(streamId, bucket)

				logging.Infof("Timekeeper::checkInitStreamReadyToMerge Index Ready To Merge. "+
					"Index: %v Stream: %v Bucket: %v SessionId: %v LastFlushTS: %v", idx.InstId,
					streamId, bucket, sessionId, initTsSeq)

				tk.supvRespch <- &MsgTKMergeStream{
					mType:     TK_MERGE_STREAM,
					streamId:  streamId,
					bucket:    bucket,
					mergeTs:   initTsSeq,
					sessionId: sessionId}

				logging.Infof("Timekeeper::checkInitStreamReadyToMerge \n\t Stream %v "+
					"Bucket %v State Changed to INACTIVE", streamId, bucket)
				tk.stopTimer(streamId, bucket)
				tk.ss.cleanupBucketFromStream(streamId, bucket)
				return true

			} else {
				//check for one index is sufficient as all indexes in a bucket
				//move together
				return false
			}
		}
	}

	return false
}

func (tk *timekeeper) checkFlushTsValidForMerge(streamId common.StreamId, bucket string,
	initFlushTs *common.TsVbuuid, minMergeTs *common.TsVbuuid) (bool, Timestamp) {

	var maintFlushTs *common.TsVbuuid

	//if the initFlushTs is past the lastFlushTs of this bucket in MAINT_STREAM,
	//this index can be merged to MAINT_STREAM. If there is a flush in progress,
	//it is important to use that for comparison as after merge MAINT_STREAM will
	//include merged indexes after the in progress flush finishes.
	if lts, ok := tk.ss.streamBucketFlushInProgressTsMap[common.MAINT_STREAM][bucket]; ok && lts != nil {
		maintFlushTs = lts
	} else {
		maintFlushTs = tk.ss.streamBucketLastFlushedTsMap[common.MAINT_STREAM][bucket]
	}

	var maintTsSeq, initTsSeq Timestamp

	//if no flush has happened yet from MAINT_STREAM, its good to merge
	//this can happen if MAINT_STREAM gets a rollback to 0 and is yet to
	//start flushing, while INIT_STREAM is done flushing till buildTs
	if maintFlushTs == nil {
		return true, nil
	}

	//if initFlushTs is nil for INIT_STREAM and non-nil for MAINT_STREAM
	//merge cannot happen
	if maintFlushTs != nil && initFlushTs == nil {
		return false, nil
	}

	maintTsSeq = getSeqTsFromTsVbuuid(maintFlushTs)
	initTsSeq = getSeqTsFromTsVbuuid(initFlushTs)
	minMergeTsSeq := getSeqTsFromTsVbuuid(minMergeTs)
	if initTsSeq.GreaterThanEqual(maintTsSeq) &&
		initTsSeq.GreaterThanEqual(minMergeTsSeq) {

		//vbuuids need to match for index merge
		if initFlushTs.CompareVbuuids(maintFlushTs) {
			return true, initTsSeq
		}

	}
	return false, nil
}

func (tk *timekeeper) checkCatchupStreamReadyToMerge(cmd Message) bool {

	streamId := cmd.(*MsgMutMgrFlushDone).GetStreamId()
	bucket := cmd.(*MsgMutMgrFlushDone).GetBucket()

	if streamId == common.CATCHUP_STREAM {

		//get the lowest TS for this bucket in MAINT_STREAM
		bucketTsListMap := tk.ss.streamBucketTsListMap[common.MAINT_STREAM]
		tsList := bucketTsListMap[bucket]
		var maintTs *common.TsVbuuid
		if tsList.Len() > 0 {
			e := tsList.Front()
			maintTs = e.Value.(*common.TsVbuuid)
		} else {
			return false
		}

		//get the lastFlushedTs of CATCHUP_STREAM
		bucketLastFlushedTsMap := tk.ss.streamBucketLastFlushedTsMap[common.CATCHUP_STREAM]
		lastFlushedTs := bucketLastFlushedTsMap[bucket]
		if lastFlushedTs == nil {
			return false
		}

		if compareTsSnapshot(lastFlushedTs, maintTs) {
			//disable drain for MAINT_STREAM for this bucket, so it doesn't
			//drop mutations till merge is complete
			tk.ss.streamBucketDrainEnabledMap[common.MAINT_STREAM][bucket] = false

			logging.Infof("Timekeeper::checkCatchupStreamReadyToMerge Bucket Ready To Merge. "+
				"Stream: %v Bucket: %v ", streamId, bucket)

			tk.supvRespch <- &MsgTKMergeStream{
				streamId: streamId,
				bucket:   bucket}

			tk.supvCmdch <- &MsgSuccess{}
			return true
		}
	}
	return false
}

//generates a new StabilityTS
func (tk *timekeeper) generateNewStabilityTS(streamId common.StreamId,
	bucket string) {

	tk.lock.Lock()
	defer tk.lock.Unlock()

	if tk.indexerState != common.INDEXER_ACTIVE {
		return
	}

	if status, ok := tk.ss.streamBucketStatus[streamId][bucket]; !ok || status != STREAM_ACTIVE {
		return
	}

	if tk.ss.checkNewTSDue(streamId, bucket) {
		tsVbuuid := tk.ss.getNextStabilityTS(streamId, bucket)

		//persist TS which completes the build
		if tk.isBuildCompletionTs(streamId, bucket, tsVbuuid) {

			if hasTS, ok := tk.ss.streamBucketHasBuildCompTSMap[streamId][bucket]; !ok || !hasTS {
				logging.Infof("timekeeper::generateNewStability: setting snapshot type as DISK_SNAP due to BuildCompletionTS")
				tsVbuuid.SetSnapType(common.DISK_SNAP)
				tk.ss.streamBucketHasBuildCompTSMap[streamId][bucket] = true
			}
		}

		if tk.ss.canFlushNewTS(streamId, bucket) {
			tk.sendNewStabilityTS(tsVbuuid, bucket, streamId)
		} else {
			//store the ts in list
			logging.LazyTrace(func() string {
				return fmt.Sprintf(
					"Timekeeper::generateNewStabilityTS %v %v Added TS to Pending List "+
						"%v ", bucket, streamId, tsVbuuid)
			})
			tsList := tk.ss.streamBucketTsListMap[streamId][bucket]
			tsList.PushBack(tsVbuuid)
			stats := tk.stats.Get()
			if stat, ok := stats.buckets[bucket]; ok {
				stat.tsQueueSize.Set(int64(tsList.Len()))
			}

		}
	} else if tk.processPendingTS(streamId, bucket) {
		//nothing to do
	} else {
		if !tk.hasInitStateIndex(streamId, bucket) &&
			tk.ss.checkCommitOverdue(streamId, bucket) {

			tsVbuuid := tk.ss.streamBucketLastFlushedTsMap[streamId][bucket].Copy()

			if tsVbuuid.IsSnapAligned() {
				logging.Infof("Timekeeper:: %v %v Forcing Overdue Commit", streamId, bucket)
				tsVbuuid.SetSnapType(common.FORCE_COMMIT)
				tk.ss.streamBucketLastPersistTime[streamId][bucket] = time.Now()
				tk.sendNewStabilityTS(tsVbuuid, bucket, streamId)
			}
		}
	}

}

//merge a new Ts with one already pending for the stream-bucket,
//if large snapshots are being processed
func (tk *timekeeper) maybeMergeTs(streamId common.StreamId,
	bucket string, newTs *common.TsVbuuid) {

	tsList := tk.ss.streamBucketTsListMap[streamId][bucket]
	var lts *common.TsVbuuid
	merge := false

	//get the last generated but not yet processed timestamp
	if tsList.Len() > 0 {
		e := tsList.Back()
		lts = e.Value.(*common.TsVbuuid)
	}

	//if either of the last generated or newTs has a large snapshot,
	//merging is required
	if lts != nil {
		if lts.HasLargeSnapshot() || newTs.HasLargeSnapshot() {
			merge = true
		}
	}

	//when TS merge happens, all the pending TS in list are merged such that
	//there is only a single TS in list which has all the latest snapshots seen
	//by indexer.
	if merge {
		if lts.HasLargeSnapshot() {
			newTs.SetLargeSnapshot(true)
		}

		tsList.Init()
	}

	tsList.PushBack(newTs)

}

//processPendingTS checks if there is any pending TS for the given stream and
//bucket. If any TS is found, it is sent to supervisor.
func (tk *timekeeper) processPendingTS(streamId common.StreamId, bucket string) bool {

	//if there is a flush already in progress for this stream and bucket
	//or flush is disabled, nothing to be done
	bucketFlushInProgressTsMap := tk.ss.streamBucketFlushInProgressTsMap[streamId]
	bucketFlushEnabledMap := tk.ss.streamBucketFlushEnabledMap[streamId]

	if bucketFlushInProgressTsMap[bucket] != nil ||
		bucketFlushEnabledMap[bucket] == false {
		return false
	}

	//if there are pending TS for this bucket, send New TS
	bucketTsListMap := tk.ss.streamBucketTsListMap[streamId]
	tsList := bucketTsListMap[bucket]

	if tsList.Len() > 0 {
		e := tsList.Front()
		tsVbuuid := e.Value.(*common.TsVbuuid)
		tsList.Remove(e)
		ts := getSeqTsFromTsVbuuid(tsVbuuid)

		if tk.ss.streamBucketStatus[streamId][bucket] == STREAM_PREPARE_RECOVERY ||
			tk.ss.streamBucketStatus[streamId][bucket] == STREAM_PREPARE_DONE {

			tsVbuuidHWT := tk.ss.streamBucketHWTMap[streamId][bucket]
			tsHWT := getSeqTsFromTsVbuuid(tsVbuuidHWT)

			//if HWT is greater than flush TS, this TS can be flush
			if tsHWT.GreaterThanEqual(ts) {
				logging.LazyDebug(func() string {
					return fmt.Sprintf(
						"Timekeeper::processPendingTS Processing Flush TS %v "+
							"before recovery for bucket %v streamId %v", ts, bucket, streamId)
				})
			} else {
				//empty the TSList for this bucket and stream so
				//there is no further processing for this bucket.
				logging.LazyDebug(func() string {
					return fmt.Sprintf(
						"Timekeeper::processPendingTS Cannot Flush TS %v "+
							"before recovery for bucket %v streamId %v. Clearing Ts List", ts,
						bucket, streamId)
				})
				tsList.Init()
				return false
			}

		}
		tk.sendNewStabilityTS(tsVbuuid, bucket, streamId)
		//update tsQueueSize when processing queued TS
		stats := tk.stats.Get()
		if stat, ok := stats.buckets[bucket]; ok {
			stat.tsQueueSize.Set(int64(tsList.Len()))
		}
		return true
	}

	return false
}

//sendNewStabilityTS sends the given TS to supervisor
func (tk *timekeeper) sendNewStabilityTS(flushTs *common.TsVbuuid, bucket string,
	streamId common.StreamId) {

	logging.LazyTrace(func() string {
		return fmt.Sprintf("Timekeeper::sendNewStabilityTS Bucket: %v "+
			"Stream: %v TS: %v", bucket, streamId, flushTs)
	})

	tk.mayBeMakeSnapAligned(streamId, bucket, flushTs)
	tk.ensureMonotonicTs(streamId, bucket, flushTs)

	var changeVec []bool
	if flushTs.GetSnapType() != common.FORCE_COMMIT {
		var noChange bool
		changeVec, noChange = tk.ss.computeTsChangeVec(streamId, bucket, flushTs)
		if noChange {
			return
		}
		tk.setSnapshotType(streamId, bucket, flushTs)
	}

	tk.setNeedsCommit(streamId, bucket, flushTs)

	tk.ss.streamBucketFlushInProgressTsMap[streamId][bucket] = flushTs

	var stopCh StopChannel
	var doneCh DoneChannel

	monitor_ts := tk.config["timekeeper.monitor_flush"].Bool()

	if monitor_ts {
		stopCh = tk.ss.streamBucketTimerStopCh[streamId][bucket]
		doneCh = make(DoneChannel)
		tk.ss.streamBucketFlushDone[streamId][bucket] = doneCh
	}

	hasAllSB := false
	vbmap := tk.ss.streamBucketVBMap[streamId][bucket]
	//if all stream begins have been seen atleast once after stream start
	if len(vbmap) == len(flushTs.Vbuuids) {
		hasAllSB = true
	}

	go func() {
		tk.supvRespch <- &MsgTKStabilityTS{ts: flushTs,
			bucket:    bucket,
			streamId:  streamId,
			changeVec: changeVec,
			hasAllSB:  hasAllSB,
		}

		if monitor_ts {

			var totalWait int
			ticker := time.NewTicker(time.Second * 10)
			defer ticker.Stop()

			for {

				select {

				case <-stopCh:
					return
				case <-doneCh:
					return

				case <-ticker.C:

					tk.lock.Lock()
					flushInProgress := tk.ss.streamBucketFlushInProgressTsMap[streamId][bucket]
					if flushTs.Equal(flushInProgress) {
						totalWait += 10

						if totalWait > 60 {
							lastFlushedTs := tk.ss.streamBucketLastFlushedTsMap[streamId][bucket]
							hwt := tk.ss.streamBucketHWTMap[streamId][bucket]
							logging.Warnf("Timekeeper::flushMonitor Waiting for flush "+
								"to finish for %v seconds. Stream %v Bucket %v.", totalWait, streamId, bucket)
							logging.Verbosef("Timekeeper::flushMonitor FlushTs %v \n LastFlushTs %v \n HWT %v", flushTs,
								lastFlushedTs, hwt)
						}
					} else {
						tk.lock.Unlock()
						return
					}
					tk.lock.Unlock()
				}

			}
		}

	}()
}

//set the snapshot type
func (tk *timekeeper) setSnapshotType(streamId common.StreamId, bucket string,
	flushTs *common.TsVbuuid) {

	lastPersistTime := tk.ss.streamBucketLastPersistTime[streamId][bucket]

	//for init build, if there is no snapshot option set
	if tk.hasInitStateIndex(streamId, bucket) {
		if flushTs.GetSnapType() == common.NO_SNAP {
			isMergeCandidate := false

			//if this TS is a merge candidate, generate in-mem snapshot
			if tk.checkMergeCandidateTs(streamId, bucket, flushTs) {
				flushTs.SetSnapType(common.INMEM_SNAP)
				isMergeCandidate = true
			}

			// if storage type is MOI, then also generate snapshot during initial build.
			if common.GetStorageMode() == common.MOI {
				flushTs.SetSnapType(common.INMEM_SNAP)
			}

			//during catchup phase, in-memory snapshots are generated for merging.
			//it is better to use smaller persist duration during this phase.
			//Otherwise use the persist interval for initial build.
			var snapPersistInterval uint64
			var persistDuration time.Duration
			if flushTs.GetSnapType() == common.INMEM_SNAP && isMergeCandidate {
				snapPersistInterval = tk.getPersistInterval()
				persistDuration = time.Duration(snapPersistInterval) * time.Millisecond
			} else {
				snapPersistInterval = tk.getPersistIntervalInitBuild()
				persistDuration = time.Duration(snapPersistInterval) * time.Millisecond
			}

			//create disk snapshot based on wall clock time
			if time.Since(lastPersistTime) > persistDuration {
				flushTs.SetSnapType(common.DISK_SNAP)
				tk.ss.streamBucketLastPersistTime[streamId][bucket] = time.Now()
			}
		}
	} else if flushTs.IsSnapAligned() {
		//for incremental build, snapshot only if ts is snap aligned
		//set either in-mem or persist snapshot based on wall clock time
		snapPersistInterval := tk.getPersistInterval()
		persistDuration := time.Duration(snapPersistInterval) * time.Millisecond

		if time.Since(lastPersistTime) > persistDuration {
			flushTs.SetSnapType(common.DISK_SNAP)
			tk.ss.streamBucketLastPersistTime[streamId][bucket] = time.Now()
			tk.ss.streamBucketSkippedInMemTs[streamId][bucket] = 0
		} else {
			fastFlush := tk.config["settings.fast_flush_mode"].Bool()
			hasInMemSnap := tk.ss.streamBucketHasInMemSnap[streamId][bucket]
			if fastFlush && hasInMemSnap {
				//if fast flush mode is enabled, skip in-mem snapshots based
				//on number of pending ts to be processed.
				//skip in-mem snapshots only if there is atleast one in-mem snapshot
				//merge of a partitioned index expects a storage snapshot to be available
				skipFactor := tk.calcSkipFactorForFastFlush(streamId, bucket)
				if skipFactor != 0 && (tk.ss.streamBucketSkippedInMemTs[streamId][bucket] < skipFactor) {
					tk.ss.streamBucketSkippedInMemTs[streamId][bucket]++
					flushTs.SetSnapType(common.NO_SNAP)
				} else {
					flushTs.SetSnapType(common.INMEM_SNAP)
					tk.ss.streamBucketSkippedInMemTs[streamId][bucket] = 0
				}
			} else {
				flushTs.SetSnapType(common.INMEM_SNAP)
				tk.ss.streamBucketSkippedInMemTs[streamId][bucket] = 0
				tk.ss.streamBucketHasInMemSnap[streamId][bucket] = true
			}
		}
	} else {
		stats := tk.stats.Get()
		if stat, ok := stats.buckets[bucket]; ok {
			stat.numNonAlignTS.Add(1)
		}
	}

}

//checkMergeCandidateTs check if a TS is a candidate for merge with
//MAINT_STREAM
func (tk *timekeeper) checkMergeCandidateTs(streamId common.StreamId,
	bucket string, flushTs *common.TsVbuuid) bool {

	//if stream is not INIT_STREAM, merge is not required
	if streamId != common.INIT_STREAM {
		return false
	}

	//if flushTs is not on snap boundary, it is not merge candidate
	if !flushTs.IsSnapAligned() {
		return false
	}

	//if the flushTs is past the lastFlushTs of this bucket in MAINT_STREAM,
	//this index can be merged to MAINT_STREAM. If there is a flush in progress,
	//it is important to use that for comparison as after merge MAINT_STREAM will
	//include merged indexes after the in progress flush finishes.
	var lastFlushedTsVbuuid *common.TsVbuuid
	if lts, ok := tk.ss.streamBucketFlushInProgressTsMap[common.MAINT_STREAM][bucket]; ok && lts != nil {
		lastFlushedTsVbuuid = lts
	} else {
		lastFlushedTsVbuuid = tk.ss.streamBucketLastFlushedTsMap[common.MAINT_STREAM][bucket]
	}

	mergeCandidate := false
	//if no flush has happened yet, its a merge candidate
	if lastFlushedTsVbuuid == nil {
		mergeCandidate = true
	} else {
		lastFlushedTs := getSeqTsFromTsVbuuid(lastFlushedTsVbuuid)
		ts := getSeqTsFromTsVbuuid(flushTs)
		if ts.GreaterThanEqual(lastFlushedTs) {
			mergeCandidate = true
		}
	}

	return mergeCandidate
}

//mayBeMakeSnapAligned makes a Ts snap aligned if all seqnos
//have been received till Snapshot End and the difference is not
//greater than largeSnapThreshold
func (tk *timekeeper) mayBeMakeSnapAligned(streamId common.StreamId,
	bucket string, flushTs *common.TsVbuuid) {

	if tk.indexerState != common.INDEXER_ACTIVE {
		return
	}

	if tk.hasInitStateIndex(streamId, bucket) {
		return
	}

	if flushTs.HasDisableAlign() {
		return
	}

	hwt := tk.ss.streamBucketHWTMap[streamId][bucket]

	largeSnap := tk.ss.config["settings.largeSnapshotThreshold"].Uint64()

	for i, s := range flushTs.Snapshots {

		//if diff between snapEnd and seqno is with largeSnap limit
		//and all mutations have been received till snapEnd
		if s[1]-flushTs.Seqnos[i] < largeSnap &&
			hwt.Seqnos[i] >= s[1] {

			if flushTs.Seqnos[i] != s[1] {
				logging.Debugf("Timekeeper::mayBeMakeSnapAligned.  Align Seqno to Snap End for large snapshot. "+
					"Bucket %v StreamId %v vbucket %v Snapshot %v-%v old Seqno %v new Seqno %v Vbuuid %v current HWT seqno %v",
					bucket, streamId, i, flushTs.Snapshots[i][0], flushTs.Snapshots[i][1], flushTs.Seqnos[i], s[1],
					flushTs.Vbuuids[i], hwt.Seqnos[i])
			}

			flushTs.Seqnos[i] = s[1]
		}
	}

	if flushTs.CheckSnapAligned() {
		flushTs.SetSnapAligned(true)
	} else {
		flushTs.SetSnapAligned(false)
	}

}

func (tk *timekeeper) ensureMonotonicTs(streamId common.StreamId, bucket string,
	flushTs *common.TsVbuuid) {

	// Seqno should be monotonically increasing when it comes to mutation queue.
	// For pre-caution, if we detect a flushTS that is smaller than LastFlushTS,
	// we should make it align with lastFlushTS to make sure indexer does not hang
	// because it may be waiting for a seqno that never exist in mutation queue.
	if lts, ok := tk.ss.streamBucketLastFlushedTsMap[streamId][bucket]; ok && lts != nil {

		for i, s := range flushTs.Seqnos {
			//if flushTs has a smaller seqno than lastFlushTs
			if s < lts.Seqnos[i] {

				logging.Infof("Timekeeper::ensureMonotonicTs  Align seqno smaller than lastFlushTs. "+
					"Bucket %v StreamId %v vbucket %v. CurrentTS: Snapshot %v-%v Seqno %v Vbuuid %v. "+
					"LastFlushTS: Snapshot %v-%v Seqno %v Vbuuid %v.",
					bucket, streamId, i,
					flushTs.Snapshots[i][0], flushTs.Snapshots[i][1], flushTs.Seqnos[i], flushTs.Vbuuids[i],
					lts.Snapshots[i][0], lts.Snapshots[i][1], lts.Seqnos[i], lts.Vbuuids[i])

				flushTs.Seqnos[i] = lts.Seqnos[i]
				flushTs.Vbuuids[i] = lts.Vbuuids[i]
				flushTs.Snapshots[i][0] = lts.Snapshots[i][0]
				flushTs.Snapshots[i][1] = lts.Snapshots[i][1]
			}
		}
	}

}

//splits a Ts if current HWT is less than Snapshot End for the vbucket.
//It is important to send TS to flusher only upto the HWT as that's the
//only guaranteed seqno that can be flushed.
func (tk *timekeeper) maybeSplitTs(ts *common.TsVbuuid, bucket string,
	streamId common.StreamId) *common.TsVbuuid {

	hwt := tk.ss.streamBucketHWTMap[streamId][bucket]

	var newTs *common.TsVbuuid
	for i, s := range ts.Snapshots {

		//process upto hwt or snapEnd, whichever is lower
		if hwt.Seqnos[i] < s[1] {
			if newTs == nil {
				newTs = ts.Copy()
			}
			newTs.Seqnos[i] = hwt.Seqnos[i]
		}
	}

	if newTs != nil {
		tsList := tk.ss.streamBucketTsListMap[streamId][bucket]
		tsList.PushFront(ts)
		return newTs
	} else {
		return ts
	}
}

//changeIndexStateForBucket changes the state of all indexes in the given bucket
//to the one provided
func (tk *timekeeper) changeIndexStateForBucket(bucket string, state common.IndexState) {

	//for all indexes in this bucket, change the state
	for _, buildInfo := range tk.indexBuildInfo {
		if buildInfo.indexInst.Defn.Bucket == bucket {
			buildInfo.indexInst.State = state
		}
	}

}

func (tk *timekeeper) setAddInstPending(streamId common.StreamId, bucket string, val bool) {

	if streamId != common.INIT_STREAM {
		return
	}

	for _, buildInfo := range tk.indexBuildInfo {
		idx := buildInfo.indexInst
		if idx.Defn.Bucket == bucket &&
			idx.Stream == streamId &&
			idx.State == common.INDEX_STATE_CATCHUP {
			buildInfo.addInstPending = val

		}
	}
}

//check if any index for the given bucket is in initial state
func (tk *timekeeper) checkAnyInitialStateIndex(bucket string) bool {

	for _, buildInfo := range tk.indexBuildInfo {
		//if index belongs to the flushed bucket and in INITIAL state
		idx := buildInfo.indexInst
		if idx.Defn.Bucket == bucket &&
			idx.State == common.INDEX_STATE_INITIAL {
			return true
		}
	}

	return false

}

//checkBucketActiveInStream checks if the given bucket has Active status
//in stream
func (tk *timekeeper) checkBucketActiveInStream(streamId common.StreamId,
	bucket string) bool {

	if bucketStatus, ok := tk.ss.streamBucketStatus[streamId]; !ok {
		logging.Tracef("Timekeeper::checkBucketActiveInStream "+
			"Unknown Stream: %v", streamId)
		tk.supvCmdch <- &MsgError{
			err: Error{code: ERROR_TK_UNKNOWN_STREAM,
				severity: NORMAL,
				category: TIMEKEEPER}}
		return false
	} else if status, ok := bucketStatus[bucket]; !ok || status != STREAM_ACTIVE {
		logging.Tracef("Timekeeper::checkBucketActiveInStream "+
			"Unknown Bucket %v In Stream %v", bucket, streamId)
		tk.supvCmdch <- &MsgError{
			err: Error{code: ERROR_TK_UNKNOWN_STREAM,
				severity: NORMAL,
				category: TIMEKEEPER}}
		return false
	}
	return true
}

//helper function to extract Stability Timestamp from TsVbuuid
func getStabilityTSFromTsVbuuid(tsVbuuid *common.TsVbuuid) Timestamp {
	numVbuckets := len(tsVbuuid.Snapshots)
	ts := NewTimestamp(numVbuckets)
	for i, s := range tsVbuuid.Snapshots {
		ts[i] = Seqno(s[1]) //high seq num in snapshot marker
	}
	return ts
}

//helper function to extract Seqnum Timestamp from TsVbuuid
func getSeqTsFromTsVbuuid(tsVbuuid *common.TsVbuuid) Timestamp {
	numVbuckets := len(tsVbuuid.Snapshots)
	ts := NewTimestamp(numVbuckets)
	for i, s := range tsVbuuid.Seqnos {
		ts[i] = Seqno(s)
	}
	return ts
}

func (tk *timekeeper) initiateRecovery(streamId common.StreamId,
	bucket string) {

	logging.Debugf("Timekeeper::initiateRecovery Started")

	if tk.ss.streamBucketStatus[streamId][bucket] == STREAM_PREPARE_DONE {

		var retryTs *common.TsVbuuid
		restartTs := tk.ss.computeRestartTs(streamId, bucket)

		//adjust for non-snap aligned ts
		if restartTs != nil {
			tk.ss.adjustNonSnapAlignedVbs(restartTs, streamId, bucket, nil, false)
			retryTs = tk.ss.computeRetryTs(streamId, bucket, restartTs)
		}

		sessionId := tk.ss.getSessionId(streamId, bucket)

		tk.stopTimer(streamId, bucket)
		tk.ss.cleanupBucketFromStream(streamId, bucket)

		//send message for recovery
		tk.supvRespch <- &MsgRecovery{mType: INDEXER_INITIATE_RECOVERY,
			streamId:  streamId,
			bucket:    bucket,
			restartTs: restartTs,
			retryTs:   retryTs,
			sessionId: sessionId}
		logging.Infof("Timekeeper::initiateRecovery StreamId %v Bucket %v "+
			"SessionId %v RestartTs %v", streamId, bucket, sessionId, restartTs)
	} else {
		logging.Errorf("Timekeeper::initiateRecovery Invalid State For %v "+
			"Stream Detected. State %v", streamId,
			tk.ss.streamBucketStatus[streamId][bucket])
	}

}

//if End Snapshot Seqnum of each vbucket in sourceTs is greater than or equal
//to Start Snapshot Seqnum in targetTs, return true
func compareTsSnapshot(sourceTs, targetTs *common.TsVbuuid) bool {

	for i, snap := range sourceTs.Snapshots {
		if snap[1] < targetTs.Snapshots[i][0] {
			return false
		}
	}
	return true
}

func (tk *timekeeper) checkBucketReadyForRecovery(streamId common.StreamId,
	bucket string) bool {

	logging.Infof("Timekeeper::checkBucketReadyForRecovery StreamId %v "+
		"Bucket %v", streamId, bucket)

	if tk.ss.checkAnyFlushPending(streamId, bucket) ||
		tk.ss.checkAnyAbortPending(streamId, bucket) ||
		tk.ss.streamBucketStatus[streamId][bucket] != STREAM_PREPARE_DONE {
		return false
	}

	logging.Infof("Timekeeper::checkBucketReadyForRecovery StreamId %v "+
		"Bucket %v Ready for Recovery", streamId, bucket)
	return true
}

func (tk *timekeeper) handleStreamCleanup(cmd Message) {

	logging.Infof("Timekeeper::handleStreamCleanup %v", cmd)

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()

	logging.Infof("Timekeeper::handleStreamCleanup Stream %v "+
		"State Changed to INACTIVE", streamId)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	tk.ss.streamStatus[streamId] = STREAM_INACTIVE

	tk.supvCmdch <- &MsgSuccess{}

}

//
// RepairStream decides on the repair action for each vb:
// 1) StreamEnd
// 2) ConnErr due to network failure
// 3) ConnErr due to incorrect ref count
// 4) Failed StreamBegin (Missing StreamBegin)
// 5) Rollback
//
// Repair action can be:
// 1) RestartVb
//    - symptoms: StreamEnd, Failed StreamBegin
//    - effective when projector's vb is not pending or active
// 2) ShutdownVb/RestartVb
//    - symptoms: ConnErr + symptoms of RestartVb
//    - effective when projector's vb is not pending
// 3) Rollabck
//    - symptoms: KV rollback
//    - effective when projector's vb rollback
// 4) MTR
//    - symptoms: TopicMissing, InvalidBucket
//    - effective when projector loses its bookkeeping
// 5) Recovery (no rollback)
//    - symptoms: All other repair actions ineffective
//    - effective when projector's vb is pending
// 6) Recovery (rollback)
//    - symptoms: StreamBegin with rollback
//    - effective when streamBegin with rollback
//
// escalation policy:
// 1) RestartVb -> ShutdownVb/RestartVb
//    - Indexer vb: StreamEnd
//    - projector vb: Active
//      - escalate if no new StreamBegin in indexer.timekeeper.escalate.StreamBeginWaitTime
//    - projector vb: Pending
//      - escalate if no new StreamBegin in (indexer.timekeeper.escalate.StreamBeginWaitTime * 2)
// 2) ShutdownVb/RestartVb -> MTR
//    - Indexer vb: ConnErr
//    - projector vb: Active
//      - escalate if no new StreamBegin in indexer.timekeeper.escalate.StreamBeginWaitTime
//    - projector vb: Pending
//      - escalate if no new StreamBegin in (indexer.timekeeper.escalate.StreamBeginWaitTime * 2)
// 3) MTR -> Recovery
//    - Indexer vb: ConnErr
//    - projector vb: Active
//      - escalate if no new StreamBegin in indexer.timekeeper.escalate.StreamBeginWaitTime
//    - projector vb: Pending
//      - escalate if no new StreamBegin in (indexer.timekeeper.escalate.StreamBeginWaitTime * 2)
//
func (tk *timekeeper) repairStream(streamId common.StreamId,
	bucket string) {

	//wait for REPAIR_BATCH_TIMEOUT to batch more error msgs
	//and send request to KV for a batch
	time.Sleep(REPAIR_BATCH_TIMEOUT * time.Millisecond)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	if status := tk.ss.streamBucketStatus[streamId][bucket]; status != STREAM_ACTIVE {

		logging.Infof("Timekeeper::repairStream Found Stream %v Bucket %v In "+
			"State %v. Skipping Repair.", streamId, bucket, status)
		return
	}

	// Start rollback if necessary:
	needsRollback, canRollback := tk.ss.canRollbackNow(streamId, bucket)
	if canRollback {

		sessionId := tk.ss.getSessionId(streamId, bucket)
		logging.Infof("Timekeeper::repairStream need rollback for %v %v %v. "+
			"Sending Init Prepare.", streamId, bucket, sessionId)

		// Initiate recovery.   It will reset bucket book keeping upon prepare recovery.
		tk.supvRespch <- &MsgRecovery{
			mType:     INDEXER_INIT_PREP_RECOVERY,
			streamId:  streamId,
			bucket:    bucket,
			restartTs: tk.ss.computeRollbackTs(streamId, bucket),
			sessionId: sessionId,
		}

		delete(tk.ss.streamBucketRepairStopCh[streamId], bucket)
		return
	}

	// Start MTR if necessary.  This happens if indexer has not received any new StreamBegin over max wait time, and there
	// are still ConnErr.   tk.lock must be hold without releasing, while calling startMTROnRetry and repairStreamWithMTR.
	if tk.ss.startMTROnRetry(streamId, bucket) {

		logging.Infof("Timekeeper::repairStream need MTR for %v %v. Sending StreamRepair.", streamId, bucket)

		sessionId := tk.ss.getSessionId(streamId, bucket)
		resp := &MsgKVStreamRepair{
			streamId:  streamId,
			bucket:    bucket,
			sessionId: sessionId,
		}

		tk.repairStreamWithMTR(streamId, bucket, resp)
		return
	}

	//prepare repairTs with all vbs in STREAM_END, REPAIR status and
	//send that to KVSender to repair
	if repairTs, needRepair, connErrVbs, repairVbs := tk.ss.getRepairTsForBucket(streamId, bucket); needRepair {

		respCh := make(MsgChannel)
		stopCh := tk.ss.streamBucketRepairStopCh[streamId][bucket]
		sessionId := tk.ss.getSessionId(streamId, bucket)

		restartMsg := &MsgRestartVbuckets{streamId: streamId,
			bucket:     bucket,
			restartTs:  repairTs,
			respCh:     respCh,
			stopCh:     stopCh,
			connErrVbs: connErrVbs,
			repairVbs:  repairVbs,
			sessionId:  sessionId}

		go tk.sendRestartMsg(restartMsg)

	} else if needsRollback {
		go tk.repairStream(streamId, bucket)

	} else {
		delete(tk.ss.streamBucketRepairStopCh[streamId], bucket)
		logging.Infof("Timekeeper::repairStream Nothing to repair for "+
			"Stream %v and Bucket %v", streamId, bucket)

		//process any merge that was missed due to stream repair
		tk.checkPendingStreamMerge(streamId, bucket)
	}

}

func (tk *timekeeper) sendRestartMsg(restartMsg Message) {

	tk.supvRespch <- restartMsg

	//wait on respCh
	kvresp := <-restartMsg.(*MsgRestartVbuckets).GetResponseCh()

	streamId := restartMsg.(*MsgRestartVbuckets).GetStreamId()
	bucket := restartMsg.(*MsgRestartVbuckets).GetBucket()
	repairVbs := restartMsg.(*MsgRestartVbuckets).RepairVbs()
	shutdownVbs := restartMsg.(*MsgRestartVbuckets).ConnErrVbs()

	//if timekeeper has moved to prepare unpause state, ignore
	//the response message as all streams are going to be
	//restarted anyways
	if tk.checkIndexerState(common.INDEXER_PREPARE_UNPAUSE) {
		tk.lock.Lock()
		delete(tk.ss.streamBucketRepairStopCh[streamId], bucket)
		tk.lock.Unlock()
		return
	}

	validateStreamStatusAndSessionId := func(streamId common.StreamId,
		bucket string, sessionId uint64, logMsg string) bool {

		status := tk.ss.streamBucketStatus[streamId][bucket]

		//response can be skipped for inactive stream or under recovery stream(stream
		//will be restarted as part of recovery)
		if status != STREAM_ACTIVE {
			logging.Infof("Timekeeper::sendRestartMsg Found Stream %v Bucket %v In "+
				"State %v. Skipping %v.", streamId, bucket, status, logMsg)
			return false
		}

		currSessionId := tk.ss.getSessionId(streamId, bucket)
		if sessionId != currSessionId {
			logging.Infof("Timekeeper::sendRestartMsg Stream %v Bucket %v Curr Session "+
				"%v. Skipping %v Session %v.", streamId, bucket,
				currSessionId, logMsg, sessionId)
			return false
		}
		return true
	}

	switch kvresp.GetMsgType() {

	case REPAIR_ABORT:
		//nothing to do
		logging.Infof("Timekeeper::sendRestartMsg Repair Aborted %v %v", streamId, bucket)

	case KV_SENDER_RESTART_VBUCKETS_RESPONSE:

		needsRollback := false

		restartRespMsg := kvresp.(*MsgRestartVbucketsResponse)
		activeTs := restartRespMsg.GetActiveTs()
		pendingTs := restartRespMsg.GetPendingTs()
		sessionId := restartRespMsg.GetSessionId()

		abort := func() bool {
			tk.lock.RLock()
			defer tk.lock.RUnlock()

			if !validateStreamStatusAndSessionId(streamId, bucket,
				sessionId, "Restart Vbuckets Response") {
				return true
			} else {
				needsRollback = tk.ss.needsRollback(streamId, bucket)
			}
			return false
		}()

		if abort {
			return
		}

		if needsRollback {
			//if a rollback is required, proceed without waiting
		} else {
			//allow sufficient time for control messages to come in
			//after projector has confirmed success
			waitTime := tk.config["timekeeper.streamRepairWaitTime"].Int()
			time.Sleep(time.Duration(waitTime) * time.Second)
		}

		abort = func() bool {

			tk.lock.Lock()
			defer tk.lock.Unlock()

			if !validateStreamStatusAndSessionId(streamId, bucket, sessionId, "Rollback") {
				return true
			}

			tk.ss.streamBucketKVActiveTsMap[streamId][bucket] = activeTs
			tk.ss.streamBucketKVPendingTsMap[streamId][bucket] = pendingTs
			tk.ss.updateRepairState(streamId, bucket, repairVbs, shutdownVbs)
			return false

		}()

		if abort {
			return
		}

		//check for more vbuckets in repair state
		tk.repairStream(streamId, bucket)

	case INDEXER_ROLLBACK:

		contRepair := false
		func() {

			tk.lock.Lock()
			defer tk.lock.Unlock()

			sessionId := kvresp.(*MsgRollback).GetSessionId()

			if !validateStreamStatusAndSessionId(streamId, bucket, sessionId, "Rollback") {
				return
			}

			//if rollback msg, call initPrepareRecovery
			logging.Infof("Timekeeper::sendRestartMsg Received Rollback Msg For "+
				"%v %v. Update RollbackTs.", streamId, bucket)

			currSessionId := tk.ss.getSessionId(streamId, bucket)

			// There is rollback while repairing stream.  RollbackTs could be coming from
			// 1) Unrelated to the vb under repair
			// 2) Due to vb under repair
			// In either case, indexer expects getting StreamBegin even if vb is rolled back.  This will
			// set the Vb back to STREAM_BEGIN status.  Once all vb are in STREAM_BEGIN status, repairStream
			// will trigger recovery.
			rollbackTs := kvresp.(*MsgRollback).GetRollbackTs()
			if rollbackTs != nil && rollbackTs.Len() > 0 {

				ts := rollbackTs.Union(tk.ss.streamBucketKVRollbackTsMap[streamId][bucket])
				if ts != nil {
					tk.ss.streamBucketKVRollbackTsMap[streamId][bucket] = ts
				}

				if ts == nil {
					logging.Errorf("Timekeeper::sendRestartMsg Received Rollback Msg For "+
						"%v %v %v. Fail to merge rollbackTs in timekeeper. Send InitPrepRecovery "+
						"right away. RollbackTs %v", streamId, bucket, sessionId, rollbackTs)

					tk.supvRespch <- &MsgRecovery{
						mType:     INDEXER_INIT_PREP_RECOVERY,
						streamId:  streamId,
						bucket:    bucket,
						restartTs: tk.ss.computeRollbackTs(streamId, bucket),
						sessionId: currSessionId,
					}
					delete(tk.ss.streamBucketRepairStopCh[streamId], bucket)
					return
				}
				// update repair state even if there is rollback
				tk.ss.updateRepairState(streamId, bucket, repairVbs, shutdownVbs)
				contRepair = true

			} else {
				logging.Errorf("Timekeeper::sendRestartMsg Received Rollback Msg For "+
					"%v %v %v. RollbackTs is empty.  Send InitPrepRecovery right away.",
					streamId, bucket, sessionId)

				tk.supvRespch <- &MsgRecovery{
					mType:     INDEXER_INIT_PREP_RECOVERY,
					streamId:  streamId,
					bucket:    bucket,
					restartTs: tk.ss.computeRollbackTs(streamId, bucket),
					sessionId: currSessionId,
				}

				delete(tk.ss.streamBucketRepairStopCh[streamId], bucket)
			}
		}()

		if contRepair {
			tk.repairStream(streamId, bucket)
		}

	case KV_STREAM_REPAIR:

		// KV Sender has requested a MTR.  Do not clear vb status since
		// this will make timekeeper diverge from projector.   vb status
		// should only be reset on recovery or vb shutdown.

		tk.lock.Lock()
		defer tk.lock.Unlock()

		sessionId := kvresp.(*MsgKVStreamRepair).GetSessionId()

		if !validateStreamStatusAndSessionId(streamId, bucket, sessionId, "Stream Repair") {
			return
		}

		currSessionId := tk.ss.getSessionId(streamId, bucket)

		// If we need recovery, then trigger recovery right away.
		if tk.ss.needsRollback(streamId, bucket) {

			logging.Infof("Timekeeper::sendRestartMsg Received KV Repair Msg For "+
				"Stream %v Bucket %v SessionId %v. Attempting Rollback.",
				streamId, bucket, sessionId)

			tk.supvRespch <- &MsgRecovery{
				mType:     INDEXER_INIT_PREP_RECOVERY,
				streamId:  streamId,
				bucket:    bucket,
				restartTs: tk.ss.computeRollbackTs(streamId, bucket),
				sessionId: currSessionId,
			}

			delete(tk.ss.streamBucketRepairStopCh[streamId], bucket)
			return
		}

		logging.Infof("Timekeeper::sendRestartMsg Received KV Repair Msg For "+
			"Stream %v Bucket %v. Attempting Stream Repair.", streamId, bucket)

		repairMsg := kvresp.(*MsgKVStreamRepair)
		repairMsg.sessionId = currSessionId

		tk.repairStreamWithMTR(streamId, bucket, repairMsg)

	default: //MSG_ERROR

		sessionId := kvresp.(*MsgError).GetSessionId()

		abort := func() bool {
			tk.lock.RLock()
			defer tk.lock.RUnlock()

			if !validateStreamStatusAndSessionId(streamId, bucket, sessionId, "Error") {
				return true
			}
			return false
		}()

		if abort {
			return
		}

		var bucketUUIDList []string
		for _, indexInst := range tk.indexInstMap {
			if indexInst.Defn.Bucket == bucket && indexInst.Stream == streamId &&
				indexInst.State != common.INDEX_STATE_DELETED {
				bucketUUIDList = append(bucketUUIDList, indexInst.Defn.BucketUUID)
			}
		}

		if !ValidateBucket(tk.config["clusterAddr"].String(), bucket, bucketUUIDList) {
			logging.Errorf("Timekeeper::sendRestartMsg Bucket Not Found "+
				"For Stream %v Bucket %v", streamId, bucket)

			tk.lock.Lock()
			defer tk.lock.Unlock()

			if !validateStreamStatusAndSessionId(streamId, bucket, sessionId, "Bucket Not Found") {
				return
			}

			currSessionId := tk.ss.getSessionId(streamId, bucket)
			delete(tk.ss.streamBucketRepairStopCh[streamId], bucket)
			tk.ss.streamBucketStatus[streamId][bucket] = STREAM_INACTIVE

			tk.supvRespch <- &MsgRecovery{mType: INDEXER_BUCKET_NOT_FOUND,
				streamId:  streamId,
				bucket:    bucket,
				sessionId: currSessionId}
		} else {
			logging.Errorf("Timekeeper::sendRestartMsg Error Response "+
				"from KV %v For Request %v. Retrying RestartVbucket.", kvresp, restartMsg)

			tk.repairStream(streamId, bucket)
		}
	}

}

func (tk *timekeeper) repairStreamWithMTR(streamId common.StreamId, bucket string, resp *MsgKVStreamRepair) {

	//for stream repair, use the HWT. If there has been a rollback, it will
	//be detected as response of the MTR. Without rollback, even if a vbucket
	//has moved to a new node, using HWT is sufficient.
	resp.restartTs = tk.ss.streamBucketHWTMap[streamId][bucket].Copy()
	resp.async = tk.ss.streamBucketAsyncMap[streamId][bucket]

	//adjust for non-snap aligned ts
	tk.ss.adjustNonSnapAlignedVbs(resp.restartTs, streamId, bucket, nil, false)
	tk.ss.adjustVbuuids(resp.restartTs, streamId, bucket)

	//reset timer for open stream
	tk.ss.streamBucketOpenTsMap[streamId][bucket] = nil
	tk.ss.streamBucketStartTimeMap[streamId][bucket] = uint64(0)

	// stop repair
	delete(tk.ss.streamBucketRepairStopCh[streamId], bucket)

	// Update repair state of each vb now, even though MTR has not completed yet, since
	// repairStream will terminate after this function.
	for i, _ := range tk.ss.streamBucketRepairStateMap[streamId][bucket] {
		if tk.ss.streamBucketRepairStateMap[streamId][bucket][i] == REPAIR_SHUTDOWN_VB {
			logging.Infof("timekeeper::repairStreamWithMTR - set repair state to REPAIR_MTR for %v bucket %v vb %v", streamId, bucket, i)
			tk.ss.streamBucketRepairStateMap[streamId][bucket][i] = REPAIR_MTR
			tk.ss.setLastRepairTime(streamId, bucket, Vbucket(i))
		}
	}

	tk.supvRespch <- resp
}

func (tk *timekeeper) handleUpdateIndexInstMap(cmd Message) {
	tk.lock.Lock()
	defer tk.lock.Unlock()

	req := cmd.(*MsgUpdateInstMap)
	logging.Tracef("Timekeeper::handleUpdateIndexInstMap %v", cmd)
	indexInstMap := req.GetIndexInstMap()

	// Cleanup bucket conn cache for unused buckets
	validBuckets := make(map[string]bool)
	for _, inst := range indexInstMap {
		validBuckets[inst.Defn.Bucket] = true
	}

	for _, inst := range tk.indexInstMap {
		if _, ok := validBuckets[inst.Defn.Bucket]; !ok {
			tk.removeBucketConn(inst.Defn.Bucket)
		}
	}

	tk.stats.Set(req.GetStatsObject())
	tk.indexInstMap = common.CopyIndexInstMap(indexInstMap)
	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleUpdateIndexPartnMap(cmd Message) {
	tk.lock.Lock()
	defer tk.lock.Unlock()

	logging.Tracef("Timekeeper::handleUpdateIndexPartnMap %v", cmd)
	indexPartnMap := cmd.(*MsgUpdatePartnMap).GetIndexPartnMap()
	tk.indexPartnMap = CopyIndexPartnMap(indexPartnMap)
	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) removeBucketConn(name string) {
	tk.statsLock.Lock()
	defer tk.statsLock.Unlock()
	tk.removeBucketConnUnlocked(name)
}

func (tk *timekeeper) removeBucketConnUnlocked(name string) {
	if b, ok := tk.bucketConn[name]; ok {
		b.Close()
		delete(tk.bucketConn, name)
	}
}

func (tk *timekeeper) getBucketConn(name string, refresh bool) (*couchbase.Bucket, error) {
	var ok bool
	var b *couchbase.Bucket
	var err error

	tk.statsLock.Lock()
	defer tk.statsLock.Unlock()

	b, ok = tk.bucketConn[name]
	if ok && !refresh {
		return b, nil
	} else {
		// Close the old bucket instance
		tk.removeBucketConnUnlocked(name)
		logging.Infof("Timekeeper::getBucketConn Creating new conn for bucket: %v", name)
		b, err = common.ConnectBucket(tk.config["clusterAddr"].String(), "default", name)
		if err != nil {
			return nil, err
		}
		tk.bucketConn[name] = b
		return b, nil
	}
}

func (tk *timekeeper) handleStats(cmd Message) {
	tk.supvCmdch <- &MsgSuccess{}

	req := cmd.(*MsgStatsRequest)
	replych := req.GetReplyChannel()

	tk.lock.Lock()
	indexInstMap := common.CopyIndexInstMap(tk.indexInstMap)
	tk.lock.Unlock()

	go func() {

		if !req.FetchDcp() {
			tk.updateTimestampStats()
			replych <- true
			return
		}

		// Populate current KV timestamps for all buckets
		bucketTsMap := make(map[string]Timestamp)
		for _, inst := range indexInstMap {
			//skip deleted indexes
			if inst.State == common.INDEX_STATE_DELETED {
				continue
			}

			var kvTs Timestamp
			var err error

			if _, ok := bucketTsMap[inst.Defn.Bucket]; !ok {
				rh := common.NewRetryHelper(maxStatsRetries, time.Second, 1, func(a int, err error) error {
					cluster := tk.config["clusterAddr"].String()
					numVbuckets := tk.config["numVbuckets"].Int()
					kvTs, err = GetCurrentKVTs(cluster, "default", inst.Defn.Bucket, numVbuckets)
					return err
				})
				if err = rh.Run(); err != nil {
					logging.Errorf("Timekeeper::handleStats Error occured while obtaining KV seqnos - %v", err)
					replych <- true
					return
				}

				bucketTsMap[inst.Defn.Bucket] = kvTs
			}
		}

		tk.lock.Lock()
		defer tk.lock.Unlock()

		stats := tk.stats.Get()
		for _, inst := range tk.indexInstMap {
			//skip deleted indexes
			if inst.State == common.INDEX_STATE_DELETED {
				continue
			}

			idxStats := stats.indexes[inst.InstId]
			flushedCount := uint64(0)
			flushedTs := tk.ss.streamBucketLastFlushedTsMap[inst.Stream][inst.Defn.Bucket]
			if flushedTs != nil {
				for _, seqno := range flushedTs.Seqnos {
					flushedCount += seqno
				}
			}

			v := float64(flushedCount)
			receivedTs := tk.ss.streamBucketHWTMap[inst.Stream][inst.Defn.Bucket]
			queued := uint64(0)
			if receivedTs != nil {
				for i, seqno := range receivedTs.Seqnos {
					flushSeqno := uint64(0)
					if flushedTs != nil {
						flushSeqno = flushedTs.Seqnos[i]
					}

					queued += seqno - flushSeqno
				}
			}

			pending := uint64(0)
			kvTs := bucketTsMap[inst.Defn.Bucket]
			for i, seqno := range kvTs {
				recvdSeqno := uint64(0)
				if receivedTs != nil {
					recvdSeqno = receivedTs.Seqnos[i]
				}

				// By the time we compute index stats, kv timestamp would have
				// become old.
				if uint64(seqno) > recvdSeqno {
					pending += uint64(seqno) - recvdSeqno
				}
			}

			switch inst.State {
			default:
				v = 0.00
			case common.INDEX_STATE_ACTIVE:
				v = 100.00
			case common.INDEX_STATE_INITIAL, common.INDEX_STATE_CATCHUP:
				totalToBeflushed := uint64(0)
				for _, seqno := range kvTs {
					totalToBeflushed += uint64(seqno)
				}

				if totalToBeflushed > flushedCount {
					v = float64(flushedCount) * 100.00 / float64(totalToBeflushed)
				} else {
					v = 100.00
				}
			}

			if idxStats != nil {
				idxStats.numDocsProcessed.Set(int64(flushedCount))
				idxStats.numDocsQueued.Set(int64(queued))
				idxStats.numDocsPending.Set(int64(pending))
				idxStats.buildProgress.Set(int64(v))
				idxStats.completionProgress.Set(int64(math.Float64bits(v)))
				idxStats.lastRollbackTime.Set(tk.ss.bucketRollbackTime[inst.Defn.Bucket])
				idxStats.progressStatTime.Set(time.Now().UnixNano())
			}
		}

		replych <- true
	}()
}

func (tk *timekeeper) updateTimestampStats() {

	tk.lock.Lock()
	defer tk.lock.Unlock()

	stats := tk.stats.Get()
	for _, inst := range tk.indexInstMap {
		//skip deleted indexes
		if inst.State == common.INDEX_STATE_DELETED {
			continue
		}

		idxStats := stats.indexes[inst.InstId]
		if idxStats != nil {
			idxStats.lastRollbackTime.Set(tk.ss.bucketRollbackTime[inst.Defn.Bucket])
			idxStats.progressStatTime.Set(time.Now().UnixNano())
		}
	}
}

func (tk *timekeeper) isBuildCompletionTs(streamId common.StreamId,
	bucket string, flushTs *common.TsVbuuid) bool {

	for _, buildInfo := range tk.indexBuildInfo {
		//if index belongs to the flushed bucket and in INITIAL state
		idx := buildInfo.indexInst
		if idx.Defn.Bucket == bucket &&
			idx.Stream == streamId &&
			idx.State == common.INDEX_STATE_INITIAL {

			if buildInfo.buildTs != nil {
				//if flushTs is greater than or equal to buildTs
				//and is snap aligned(except if buildTs is 0)
				ts := getSeqTsFromTsVbuuid(flushTs)
				if buildInfo.buildTs.IsZeroTs() ||
					(ts.GreaterThanEqual(buildInfo.buildTs) &&
						flushTs.IsSnapAligned()) {
					return true
				}
			}
			return false
		}
	}

	return false
}

//check any stream merge that was missed due to stream repair
func (tk *timekeeper) checkPendingStreamMerge(streamId common.StreamId,
	bucket string) {

	logging.Debugf("Timekeeper::checkPendingStreamMerge Stream: %v Bucket: %v", streamId, bucket)

	//for repair done of MAINT_STREAM, if there is any corresponding INIT_STREAM,
	//check the possibility of merge
	if streamId == common.MAINT_STREAM {
		if tk.ss.streamBucketStatus[common.INIT_STREAM][bucket] == STREAM_ACTIVE {
			streamId = common.INIT_STREAM
		} else {
			return
		}
	}

	if !tk.ss.checkAnyFlushPending(streamId, bucket) &&
		!tk.ss.checkAnyAbortPending(streamId, bucket) {

		lastFlushedTs := tk.ss.streamBucketLastFlushedTsMap[streamId][bucket]
		tk.checkInitStreamReadyToMerge(streamId, bucket, lastFlushedTs)

	}
}

//startTimer starts a per stream/bucket timer to periodically check and
//generate a new stability timestamp
func (tk *timekeeper) startTimer(streamId common.StreamId,
	bucket string) {

	logging.Infof("Timekeeper::startTimer %v %v", streamId, bucket)

	snapInterval := tk.getInMemSnapInterval()
	ticker := time.NewTicker(time.Millisecond * time.Duration(snapInterval))
	stopCh := tk.ss.streamBucketTimerStopCh[streamId][bucket]

	go func() {
		for {
			select {
			case <-ticker.C:
				tk.generateNewStabilityTS(streamId, bucket)

			case <-stopCh:
				ticker.Stop()
				return
			}
		}
	}()

}

//stopTimer stops the stream/bucket timer started by startTimer
func (tk *timekeeper) stopTimer(streamId common.StreamId, bucket string) {

	logging.Infof("Timekeeper::stopTimer %v %v", streamId, bucket)

	stopCh := tk.ss.streamBucketTimerStopCh[streamId][bucket]
	if stopCh != nil {
		close(stopCh)
	}

}

func (tk *timekeeper) setBuildTs(streamId common.StreamId, bucket string,
	buildTs Timestamp) {

	for _, buildInfo := range tk.indexBuildInfo {
		//if index belongs to the given bucket/stream and in INITIAL state
		idx := buildInfo.indexInst
		if idx.Defn.Bucket == bucket &&
			idx.Stream == streamId &&
			idx.State == common.INDEX_STATE_INITIAL {
			buildInfo.buildTs = buildTs
		}
	}
}

//setMergeTs sets the mergeTs for catchup state indexes in case of recovery.
func (tk *timekeeper) setMergeTs(streamId common.StreamId, bucket string,
	mergeTs *common.TsVbuuid) {

	for _, buildInfo := range tk.indexBuildInfo {
		if buildInfo.indexInst.Defn.Bucket == bucket &&
			buildInfo.indexInst.State == common.INDEX_STATE_CATCHUP &&
			buildInfo.addInstPending == false {
			buildInfo.buildDoneAckReceived = true
			//set minMergeTs. stream merge can only happen at or above this
			//TS as projector guarantees new index definitions have been applied
			//to the MAINT_STREAM only at or above this.
			buildInfo.minMergeTs = mergeTs.Copy()
		}
	}
}

func (tk *timekeeper) resetWaitForRecovery(streamId common.StreamId, bucket string) {

	logging.Infof("Timekeeper::resetWaitForRecovery Stream %v Bucket %v", streamId, bucket)

	for _, buildInfo := range tk.indexBuildInfo {
		idx := buildInfo.indexInst
		if idx.Defn.Bucket == bucket &&
			idx.Stream == streamId {
			buildInfo.waitForRecovery = false
		}
	}

}

func (tk *timekeeper) hasInitStateIndex(streamId common.StreamId,
	bucket string) bool {

	if streamId == common.INIT_STREAM {
		return true
	}

	for _, buildInfo := range tk.indexBuildInfo {
		//if index belongs to the flushed bucket and in INITIAL state
		idx := buildInfo.indexInst
		if idx.Defn.Bucket == bucket &&
			idx.Stream == streamId &&
			idx.State == common.INDEX_STATE_INITIAL {
			return true
		}
	}
	return false
}

//calc skip factor for in-mem snapshots based on the
//number of pending TS to be flushed
func (tk *timekeeper) calcSkipFactorForFastFlush(streamId common.StreamId,
	bucket string) uint64 {
	tsList := tk.ss.streamBucketTsListMap[streamId][bucket]

	numPendingTs := tsList.Len()

	if numPendingTs > 150 {
		return 5
	} else if numPendingTs > 50 {
		return 3
	} else if numPendingTs > 10 {
		return 2
	} else if numPendingTs > 5 {
		return 1
	} else {
		return 0
	}
}

func (tk *timekeeper) handleIndexerPause(cmd Message) {

	logging.Infof("Timekeeper::handleIndexerPause")

	tk.lock.Lock()
	defer tk.lock.Unlock()

	tk.indexerState = common.INDEXER_PAUSED

	tk.supvCmdch <- &MsgSuccess{}

}

func (tk *timekeeper) handlePrepareUnpause(cmd Message) {

	logging.Infof("Timekeeper::handlePrepareUnpause")

	tk.lock.Lock()
	defer tk.lock.Unlock()

	tk.indexerState = common.INDEXER_PREPARE_UNPAUSE

	go tk.doUnpause()

	tk.supvCmdch <- &MsgSuccess{}

}

func (tk *timekeeper) handleIndexerResume(cmd Message) {

	logging.Infof("Timekeeper::handleIndexerResume")

	tk.lock.Lock()
	defer tk.lock.Unlock()

	tk.indexerState = common.INDEXER_ACTIVE

	//generate prepare init recovery for all stream/buckets
	for s, bs := range tk.ss.streamBucketStatus {
		for b, status := range bs {
			if status != STREAM_INACTIVE {
				tk.ss.streamBucketFlushEnabledMap[s][b] = false
				sessionId := tk.ss.getSessionId(s, b)
				tk.supvRespch <- &MsgRecovery{mType: INDEXER_INIT_PREP_RECOVERY,
					streamId:  s,
					bucket:    b,
					sessionId: sessionId,
				}
			}
		}
	}

	tk.supvCmdch <- &MsgSuccess{}

}

func (tk *timekeeper) doUnpause() {

	//send unpause to indexer once there is no repair in progress
	ticker := time.NewTicker(time.Second * 1)
	defer ticker.Stop()

	for _ = range ticker.C {

		if tk.checkAnyRepairPending() {
			logging.Infof("Timekeeper::doUnpause Dropping Request to Unpause. " +
				"Next Try In 1 Second...")
			continue
		}
		break
	}

	tk.supvRespch <- &MsgIndexerState{mType: INDEXER_UNPAUSE}
}

func (tk *timekeeper) checkAnyRepairPending() bool {

	tk.lock.Lock()
	defer tk.lock.Unlock()
	for s, bs := range tk.ss.streamBucketStatus {

		for b, _ := range bs {
			if tk.ss.streamBucketRepairStopCh[s][b] != nil {
				return true
			}
		}
	}

	return false

}

func (tk *timekeeper) checkIndexerState(state common.IndexerState) bool {

	tk.lock.Lock()
	defer tk.lock.Unlock()
	if tk.indexerState == state {
		return true
	}
	return false

}

func (tk *timekeeper) getPersistInterval() uint64 {

	if common.GetStorageMode() == common.FORESTDB {
		return tk.config["settings.persisted_snapshot.fdb.interval"].Uint64()
	} else {
		return tk.config["settings.persisted_snapshot.moi.interval"].Uint64()
	}

}
func (tk *timekeeper) getPersistIntervalInitBuild() uint64 {

	if common.GetStorageMode() == common.FORESTDB {
		return tk.config["settings.persisted_snapshot_init_build.fdb.interval"].Uint64()
	} else {
		return tk.config["settings.persisted_snapshot_init_build.moi.interval"].Uint64()
	}

}
func (tk *timekeeper) getInMemSnapInterval() uint64 {

	if common.GetStorageMode() == common.FORESTDB {
		return tk.config["settings.inmemory_snapshot.fdb.interval"].Uint64()
	} else {
		return tk.config["settings.inmemory_snapshot.moi.interval"].Uint64()
	}

}

func (tk *timekeeper) setNeedsCommit(streamId common.StreamId,
	bucket string, flushTs *common.TsVbuuid) {

	switch flushTs.GetSnapType() {

	case common.INMEM_SNAP, common.NO_SNAP:
		tk.ss.streamBucketNeedsCommitMap[streamId][bucket] = true
	case common.DISK_SNAP, common.FORCE_COMMIT:
		tk.ss.streamBucketNeedsCommitMap[streamId][bucket] = false
	}

}

// This method has to be called while timepeeker has aquired tk.lock
func (tk *timekeeper) updateStreamBucketVbMap(cmd Message) {

	streamId := cmd.(*MsgStream).GetStreamId()
	meta := cmd.(*MsgStream).GetMutationMeta()
	node := cmd.(*MsgStream).GetNode()
	if node != nil {
		nodeUUID := fmt.Sprintf("%s", node)
		bucket := meta.bucket
		tk.ss.streamBucketVBMap[streamId][bucket][meta.vbucket] = nodeUUID
	}
}

func (tk *timekeeper) handlePoolChange(cmd Message) {

	kvNodes := cmd.(*MsgPoolChange).GetNodes()
	streamId := cmd.(*MsgPoolChange).GetStreamId()
	bucket := cmd.(*MsgPoolChange).GetBucket()
	tk.lock.Lock()
	defer tk.lock.Unlock()

	state := tk.ss.streamBucketStatus[streamId][bucket]
	if state != STREAM_ACTIVE {
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	vbMap := tk.ss.streamBucketVBMap[streamId][bucket]
	vbList := make(Vbuckets, 0)

	for vb, nodeuuid := range vbMap {
		if _, ok := kvNodes[nodeuuid]; !ok {
			// Node UUID not a part of active KV nodes. Get the vb's belonging to this KV node
			if vbState := tk.ss.streamBucketVbStatusMap[streamId][bucket][vb]; vbState == VBS_STREAM_BEGIN {
				vbList = append(vbList, vb)
			}
		}
	}

	if len(vbList) > 0 {
		sort.Sort(vbList)
		logging.Infof("Timekeeper::handlePoolChange streamId: %v, bucket:%v, vbList: %v", streamId, bucket, vbList)
		tk.handleStreamConnErrorInternal(streamId, bucket, vbList)
	} else {
		tk.supvCmdch <- &MsgSuccess{}
	}
}
