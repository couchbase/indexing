// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
)

const (
	maxStatsRetries = 5
)

//Timekeeper manages the Stability Timestamp Generation and also
//keeps track of the HWTimestamp for each keyspaceId
type Timekeeper interface {
}

type timekeeper struct {
	supvCmdch  MsgChannel //supervisor sends commands on this channel
	supvRespch MsgChannel //channel to send any async message to supervisor

	ss *StreamState

	//map of indexInstId to its Initial Build Info
	indexBuildInfo map[common.IndexInstId]*InitialBuildInfo

	config common.Config

	indexInstMap  IndexInstMapHolder
	indexPartnMap IndexPartnMapHolder

	// Lock to protect simultaneous update of stats by multiple go-routines
	statsLock sync.Mutex

	stats           IndexerStatsHolder
	vbCheckerStopCh map[common.StreamId]chan bool

	lock sync.RWMutex //lock to protect this structure

	indexerState common.IndexerState

	clusterInfoClient *common.ClusterInfoClient
}

type InitialBuildInfo struct {
	indexInst             common.IndexInst
	buildTs               Timestamp
	buildDoneAckReceived  bool
	minMergeTs            *common.TsVbuuid //minimum merge ts for init stream
	addInstPending        bool
	waitForRecovery       bool
	flushedUptoMinMergeTs bool //indicates flushTs is past minMergeTs
}

//timeout in milliseconds to batch the vbuckets
//together for repair message
const REPAIR_BATCH_TIMEOUT = 1000
const KV_RETRY_INTERVAL = 5000

//const REPAIR_RETRY_INTERVAL = 5000
//const REPAIR_RETRY_BEFORE_SHUTDOWN = 5

//NewTimekeeper returns an instance of timekeeper or err message.
//It listens on supvCmdch for command and every command is followed
//by a synchronous response of the supvCmdch.
//Any async response to supervisor is sent to supvRespch.
//If supvCmdch get closed, storageMgr will shut itself down.
func NewTimekeeper(supvCmdch MsgChannel, supvRespch MsgChannel,
	config common.Config, c *common.ClusterInfoClient) (Timekeeper, Message) {

	//Init the timekeeper struct
	tk := &timekeeper{
		supvCmdch:         supvCmdch,
		supvRespch:        supvRespch,
		ss:                InitStreamState(config),
		config:            config,
		indexBuildInfo:    make(map[common.IndexInstId]*InitialBuildInfo),
		vbCheckerStopCh:   make(map[common.StreamId]chan bool),
		clusterInfoClient: c,
	}

	tk.indexInstMap.Init()
	tk.indexPartnMap.Init()

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
					for _, stopCh := range tk.vbCheckerStopCh {
						if stopCh != nil {
							close(stopCh)
						}
					}
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

	case REMOVE_KEYSPACE_FROM_STREAM:
		tk.handleRemoveKeyspaceFromStream(cmd)

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

	case STREAM_READER_SYSTEM_EVENT:
		tk.handleDcpSystemEvent(cmd)

	case STREAM_READER_OSO_SNAPSHOT_MARKER:
		tk.handleOSOSnapshotMarker(cmd)

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

	case TK_GET_KEYSPACE_HWT:
		tk.handleGetKeyspaceHWT(cmd)

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

	case UPDATE_KEYSPACE_STATS_MAP:
		tk.handleUpdateKeyspaceStatsMap(cmd)

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
	keyspaceId := cmd.(*MsgStreamUpdate).GetKeyspaceId()
	restartTs := cmd.(*MsgStreamUpdate).GetRestartTs()
	rollbackTime := cmd.(*MsgStreamUpdate).GetRollbackTime()
	async := cmd.(*MsgStreamUpdate).GetAsync()
	sessionId := cmd.(*MsgStreamUpdate).GetSessionId()
	collectionId := cmd.(*MsgStreamUpdate).GetCollectionId()
	enableOSO := cmd.(*MsgStreamUpdate).EnableOSO()

	tk.lock.Lock()
	defer tk.lock.Unlock()

	if tk.ss.streamStatus[streamId] != STREAM_ACTIVE {
		tk.ss.initNewStream(streamId)
		logging.Infof("Timekeeper::handleStreamOpen Stream %v "+
			"State Changed to ACTIVE", streamId)

	}

	status := tk.ss.streamKeyspaceIdStatus[streamId][keyspaceId]
	switch status {

	//fresh start or recovery
	case STREAM_INACTIVE, STREAM_PREPARE_DONE:
		tk.ss.initKeyspaceIdInStream(streamId, keyspaceId)
		if restartTs != nil {
			tk.ss.streamKeyspaceIdRestartTsMap[streamId][keyspaceId] = restartTs.Copy()
			tk.ss.setHWTFromRestartTs(streamId, keyspaceId)
		}
		tk.ss.setRollbackTime(keyspaceId, rollbackTime)
		tk.ss.streamKeyspaceIdAsyncMap[streamId][keyspaceId] = async
		tk.ss.streamKeyspaceIdSessionId[streamId][keyspaceId] = sessionId
		tk.ss.streamKeyspaceIdCollectionId[streamId][keyspaceId] = collectionId
		tk.ss.streamKeyspaceIdEnableOSO[streamId][keyspaceId] = enableOSO
		tk.addIndextoStream(cmd)
		tk.startTimer(streamId, keyspaceId)

	//repair
	default:
		//no-op
		//reset timer for open stream
		tk.ss.streamKeyspaceIdOpenTsMap[streamId][keyspaceId] = nil
		tk.ss.streamKeyspaceIdStartTimeMap[streamId][keyspaceId] = uint64(0)

		logging.Infof("Timekeeper::handleStreamOpen %v %v Status %v. "+
			"Nothing to do.", streamId, keyspaceId, status)

	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleStreamClose(cmd Message) {

	logging.Debugf("Timekeeper::handleStreamClose %v", cmd)

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()

	tk.lock.Lock()
	defer tk.lock.Unlock()

	//cleanup all keyspaceIds from stream
	for keyspaceId := range tk.ss.streamKeyspaceIdStatus[streamId] {
		tk.removeKeyspaceFromStream(streamId, keyspaceId, false)
	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleInitPrepRecovery(msg Message) {

	keyspaceId := msg.(*MsgRecovery).GetKeyspaceId()
	streamId := msg.(*MsgRecovery).GetStreamId()

	logging.Infof("Timekeeper::handleInitPrepRecovery %v %v",
		streamId, keyspaceId)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	tk.prepareRecovery(streamId, keyspaceId)

	tk.supvCmdch <- &MsgSuccess{}

}

func (tk *timekeeper) handlePrepareDone(cmd Message) {

	logging.Infof("Timekeeper::handlePrepareDone %v", cmd)

	streamId := cmd.(*MsgRecovery).GetStreamId()
	keyspaceId := cmd.(*MsgRecovery).GetKeyspaceId()

	tk.lock.Lock()
	defer tk.lock.Unlock()

	//if stream is in PREPARE_RECOVERY, check and init RECOVERY
	if tk.ss.streamKeyspaceIdStatus[streamId][keyspaceId] == STREAM_PREPARE_RECOVERY {

		logging.Infof("Timekeeper::handlePrepareDone Stream %v "+
			"KeyspaceId %v State Changed to PREPARE_DONE", streamId, keyspaceId)
		tk.ss.streamKeyspaceIdStatus[streamId][keyspaceId] = STREAM_PREPARE_DONE

		switch streamId {
		case common.MAINT_STREAM, common.INIT_STREAM:
			if tk.checkKeyspaceReadyForRecovery(streamId, keyspaceId) {
				tk.initiateRecovery(streamId, keyspaceId)
			}
		}
	} else {
		logging.Infof("Timekeeper::handlePrepareDone Unexpected PREPARE_DONE "+
			"for StreamId %v KeyspaceId %v State %v", streamId, keyspaceId,
			tk.ss.streamKeyspaceIdStatus[streamId][keyspaceId])
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
	keyspaceInRecovery := cmd.(*MsgStreamUpdate).KeyspaceInRecovery()

	//If the index is in INITIAL state, store it in initialbuild map
	for _, idx := range indexInstList {

		tk.ss.streamKeyspaceIdIndexCountMap[streamId][idx.Defn.KeyspaceId(streamId)] += 1
		logging.Infof("Timekeeper::addIndextoStream IndexCount %v", tk.ss.streamKeyspaceIdIndexCountMap)

		// buildInfo can be reomved when the corresponding is closed.   A stream can be closed for
		// various condition, such as recovery.   When the stream is re-opened, index will be added back
		// to the stream.  It is necesasry to ensure that buildInfo is added back accordingly to ensure
		// that build index will converge.
		if _, ok := tk.indexBuildInfo[idx.InstId]; !ok {

			if idx.State == common.INDEX_STATE_INITIAL ||
				(streamId == common.INIT_STREAM && idx.State == common.INDEX_STATE_CATCHUP) {

				logging.Infof("Timekeeper::addIndextoStream add BuildInfo index %v "+
					"stream %v keyspaceId %v state %v waitForRecovery %v", idx.InstId, streamId,
					idx.Defn.KeyspaceId(streamId), idx.State, keyspaceInRecovery)

				tk.indexBuildInfo[idx.InstId] = &InitialBuildInfo{
					indexInst:       idx,
					buildTs:         buildTs,
					waitForRecovery: keyspaceInRecovery,
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
	keyspaceId := cmd.(*MsgStreamUpdate).GetKeyspaceId()
	state := tk.ss.streamKeyspaceIdStatus[streamId][keyspaceId]

	// Ignore UPDATE_BUILD_TS msg for inactive and recovery phase. For recovery,
	// stream will get re-opened and build done will get re-computed.
	if state == STREAM_INACTIVE || state == STREAM_PREPARE_DONE ||
		state == STREAM_PREPARE_RECOVERY {
		logging.Infof("Timekeeper::handleUpdateBuildTs Ignore updateBuildTs "+
			"for KeyspaceId: %v StreamId: %v State: %v", keyspaceId, streamId, state)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	tk.updateBuildTs(cmd)

	//Check for possiblity of build done after buildTs is updated.
	//In case of crash recovery, if there are no mutations, there is
	//no flush happening, which can cause index to be in initial state.
	if !tk.ss.checkAnyFlushPending(streamId, keyspaceId) &&
		!tk.ss.checkAnyAbortPending(streamId, keyspaceId) {
		lastFlushedTs := tk.ss.streamKeyspaceIdLastFlushedTsMap[streamId][keyspaceId]
		tk.checkInitialBuildDone(streamId, keyspaceId, lastFlushedTs)
	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) updateBuildTs(cmd Message) {
	buildTs := cmd.(*MsgStreamUpdate).GetTimestamp()
	streamId := cmd.(*MsgStreamUpdate).GetStreamId()
	keyspaceId := cmd.(*MsgStreamUpdate).GetKeyspaceId()

	tk.setBuildTs(streamId, keyspaceId, buildTs)
}

func (tk *timekeeper) handleRemoveIndexFromStream(cmd Message) {

	logging.Infof("Timekeeper::handleRemoveIndexFromStream %v", cmd)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	tk.removeIndexFromStream(cmd)

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleRemoveKeyspaceFromStream(cmd Message) {

	logging.Infof("Timekeeper::handleRemoveKeyspaceFromStream %v", cmd)

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()
	keyspaceId := cmd.(*MsgStreamUpdate).GetKeyspaceId()
	abort := cmd.(*MsgStreamUpdate).AbortRecovery()

	tk.lock.Lock()
	defer tk.lock.Unlock()

	tk.removeKeyspaceFromStream(streamId, keyspaceId, abort)

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) removeIndexFromStream(cmd Message) {

	indexInstList := cmd.(*MsgStreamUpdate).GetIndexList()
	streamId := cmd.(*MsgStreamUpdate).GetStreamId()

	for _, idx := range indexInstList {
		// This is only called for DROP INDEX.   Therefore, it is OK to remove buildInfo.
		// If this is used for other purpose, it is necessary to ensure there is no side effect.
		if _, ok := tk.indexBuildInfo[idx.InstId]; ok {
			logging.Infof("Timekeeper::removeIndexFromStream remove index %v from stream %v keyspaceId %v",
				idx.InstId, streamId, idx.Defn.KeyspaceId(streamId))
			delete(tk.indexBuildInfo, idx.InstId)
		}
		if tk.ss.streamKeyspaceIdIndexCountMap[streamId][idx.Defn.KeyspaceId(streamId)] == 0 {
			logging.Fatalf("Timekeeper::removeIndexFromStream Invalid Internal "+
				"State Detected. Index Count Underflow. Stream %s. KeyspaceId %s.", streamId,
				idx.Defn.KeyspaceId(streamId))
		} else {
			tk.ss.streamKeyspaceIdIndexCountMap[streamId][idx.Defn.KeyspaceId(streamId)] -= 1
			logging.Infof("Timekeeper::removeIndexFromStream IndexCount %v", tk.ss.streamKeyspaceIdIndexCountMap)
		}
	}
}

func (tk *timekeeper) removeKeyspaceFromStream(streamId common.StreamId,
	keyspaceId string, abort bool) {

	status := tk.ss.streamKeyspaceIdStatus[streamId][keyspaceId]

	// This is called when closing a stream or remove a keyspace from a stream.  If this
	// is called during recovery,  it is necessary to ensure that the buildInfo is added
	// back to timekeeper accordingly.
	for instId, idx := range tk.indexBuildInfo {
		// remove buildInfo only for the given keyspaceId AND stream
		if idx.indexInst.Defn.KeyspaceId(idx.indexInst.Stream) == keyspaceId &&
			idx.indexInst.Stream == streamId {
			logging.Infof("Timekeeper::removeKeyspaceFromStream remove index %v from stream %v keyspaceId %v",
				instId, streamId, keyspaceId)
			delete(tk.indexBuildInfo, instId)
		}
	}

	//for a keyspaceId in prepareRecovery, only change the status
	//actual cleanup happens in initRecovery
	if status == STREAM_PREPARE_RECOVERY && !abort {
		tk.ss.streamKeyspaceIdStatus[streamId][keyspaceId] = STREAM_PREPARE_RECOVERY
		tk.ss.clearAllRepairState(streamId, keyspaceId)
	} else {
		tk.stopTimer(streamId, keyspaceId)
		tk.ss.cleanupKeyspaceIdFromStream(streamId, keyspaceId)
	}

}

func (tk *timekeeper) handleSync(cmd Message) {

	logging.LazyTrace(func() string {
		return fmt.Sprintf("Timekeeper::handleSync %v", cmd)
	})

	streamId := cmd.(*MsgKeyspaceHWT).GetStreamId()
	keyspaceId := cmd.(*MsgKeyspaceHWT).GetKeyspaceId()
	hwt := cmd.(*MsgKeyspaceHWT).GetHWT()
	hwtOSO := cmd.(*MsgKeyspaceHWT).GetHWTOSO()
	prevSnap := cmd.(*MsgKeyspaceHWT).GetPrevSnap()
	sessionId := cmd.(*MsgKeyspaceHWT).GetSessionId()

	tk.lock.Lock()
	defer tk.lock.Unlock()

	//check if keyspaceId is active in stream
	if tk.checkKeyspaceActiveInStream(streamId, keyspaceId) == false {
		logging.Tracef("Timekeeper::handleSync Received Sync for "+
			"Inactive KeyspaceId %v Stream %v. Ignored.", keyspaceId, streamId)
		return
	}

	//if there are no indexes for this keyspaceId and stream, ignore
	if c, ok := tk.ss.streamKeyspaceIdIndexCountMap[streamId][keyspaceId]; !ok || c <= 0 {
		logging.Tracef("Timekeeper::handleSync Ignore Sync for StreamId %v "+
			"KeyspaceId %v. IndexCount %v. ", streamId, keyspaceId, c)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	//if the session doesn't match, ignore
	currSessionId := tk.ss.getSessionId(streamId, keyspaceId)
	if sessionId != 0 && sessionId != currSessionId {
		logging.Warnf("Timekeeper::handleSync Ignore Sync for StreamId %v "+
			"KeyspaceId %v. SessionId %v. Current Session %v ", streamId, keyspaceId,
			sessionId, currSessionId)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	if _, ok := tk.ss.streamKeyspaceIdHWTMap[streamId][keyspaceId]; !ok {
		logging.Debugf("Timekeeper::handleSync Ignoring Sync Marker "+
			"for StreamId %v KeyspaceId %v. KeyspaceId Not Found.", streamId, keyspaceId)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	//update HWT for the keyspaceId
	tk.ss.updateHWT(streamId, keyspaceId, hwt, hwtOSO, prevSnap)
	hwt.Free()
	prevSnap.Free()
	if hwtOSO != nil {
		hwtOSO.Free()
	}

	tk.supvCmdch <- &MsgSuccess{}

}

func (tk *timekeeper) handleFlushDone(cmd Message) {

	streamId := cmd.(*MsgMutMgrFlushDone).GetStreamId()
	keyspaceId := cmd.(*MsgMutMgrFlushDone).GetKeyspaceId()
	flushWasAborted := cmd.(*MsgMutMgrFlushDone).GetAborted()

	tk.lock.Lock()
	defer tk.lock.Unlock()

	keyspaceIdLastFlushedTsMap := tk.ss.streamKeyspaceIdLastFlushedTsMap[streamId]
	keyspaceIdFlushInProgressTsMap := tk.ss.streamKeyspaceIdFlushInProgressTsMap[streamId]

	doneCh := tk.ss.streamKeyspaceIdFlushDone[streamId][keyspaceId]
	if doneCh != nil {
		close(doneCh)
		tk.ss.streamKeyspaceIdFlushDone[streamId][keyspaceId] = nil
	}

	if flushWasAborted {

		tk.processFlushAbort(streamId, keyspaceId)
		return
	}

	if _, ok := keyspaceIdFlushInProgressTsMap[keyspaceId]; ok {
		//store the last flushed TS
		fts := keyspaceIdFlushInProgressTsMap[keyspaceId]

		keyspaceIdLastFlushedTsMap[keyspaceId] = fts

		if fts != nil {
			tk.ss.updateLastMutationVbuuid(streamId, keyspaceId, fts)
		}

		// check if each flush time is snap aligned. If so, make a copy.
		if fts != nil && fts.IsSnapAligned() {
			tk.ss.streamKeyspaceIdLastSnapAlignFlushedTsMap[streamId][keyspaceId] = fts.Copy()
		}

		//update internal map to reflect flush is done
		keyspaceIdFlushInProgressTsMap[keyspaceId] = nil
	} else {
		//this keyspaceId is already gone from this stream, may be because
		//the index were dropped. Log and ignore.
		logging.Warnf("Timekeeper::handleFlushDone Ignore Flush Done for Stream %v "+
			"KeyspaceId %v. KeyspaceId Info Not Found", streamId, keyspaceId)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	switch streamId {

	case common.MAINT_STREAM:
		tk.handleFlushDoneMaintStream(cmd)

	case common.INIT_STREAM:
		tk.handleFlushDoneInitStream(cmd)

	default:
		logging.Errorf("Timekeeper::handleFlushDone Invalid StreamId %v ", streamId)
	}

}

func (tk *timekeeper) processFlushAbort(streamId common.StreamId, keyspaceId string) {

	keyspaceIdLastFlushedTsMap := tk.ss.streamKeyspaceIdLastFlushedTsMap[streamId]
	keyspaceIdFlushInProgressTsMap := tk.ss.streamKeyspaceIdFlushInProgressTsMap[streamId]

	logging.Infof("Timekeeper::processFlushAbort Flush Abort Received %v %v"+
		"\nFlushTs %v \nLastFlushTs %v", streamId, keyspaceId, keyspaceIdFlushInProgressTsMap[keyspaceId],
		keyspaceIdLastFlushedTsMap[keyspaceId])

	keyspaceIdFlushInProgressTsMap[keyspaceId] = nil

	//disable further processing for this stream
	tk.ss.streamKeyspaceIdFlushEnabledMap[streamId][keyspaceId] = false

	//after this point, a recovery must happen.
	tk.ss.streamKeyspaceIdForceRecovery[streamId][keyspaceId] = true

	tsList := tk.ss.streamKeyspaceIdTsListMap[streamId][keyspaceId]
	tsList.Init()

	state := tk.ss.streamKeyspaceIdStatus[streamId][keyspaceId]

	sessionId := tk.ss.getSessionId(streamId, keyspaceId)

	switch state {

	case STREAM_ACTIVE, STREAM_RECOVERY:

		if tk.resetStreamIfOSOEnabled(streamId, keyspaceId, sessionId, false) {
			break
		}

		logging.Infof("Timekeeper::processFlushAbort %v %v %v Generate InitPrepRecovery",
			streamId, keyspaceId, sessionId)
		tk.supvRespch <- &MsgRecovery{mType: INDEXER_INIT_PREP_RECOVERY,
			streamId:   streamId,
			keyspaceId: keyspaceId,
			restartTs:  tk.ss.computeRestartTs(streamId, keyspaceId),
			sessionId:  sessionId}

	case STREAM_PREPARE_RECOVERY:

		//send message to stop running stream
		logging.Infof("Timekeeper::processFlushAbort %v %v %v Generate PrepareRecovery",
			streamId, keyspaceId, sessionId)
		tk.supvRespch <- &MsgRecovery{mType: INDEXER_PREPARE_RECOVERY,
			streamId:   streamId,
			keyspaceId: keyspaceId,
			sessionId:  sessionId}

	case STREAM_INACTIVE:
		logging.Errorf("Timekeeper::processFlushAbort Unexpected Flush Abort "+
			"Received for Inactive StreamId %v keyspaceId %v sessionId %v ",
			streamId, keyspaceId, sessionId)

	default:
		logging.Errorf("Timekeeper::processFlushAbort %v %v Invalid Stream State %v.",
			streamId, keyspaceId, state)

	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleFlushDoneMaintStream(cmd Message) {

	logging.LazyTrace(func() string {
		return fmt.Sprintf("Timekeeper::handleFlushDoneMaintStream %v", cmd)
	})

	streamId := cmd.(*MsgMutMgrFlushDone).GetStreamId()
	keyspaceId := cmd.(*MsgMutMgrFlushDone).GetKeyspaceId()

	state := tk.ss.streamKeyspaceIdStatus[streamId][keyspaceId]

	switch state {

	case STREAM_ACTIVE, STREAM_RECOVERY:

		//check if any of the initial build index is past its Build TS.
		//Generate msg for Build Done and change the state of the index.
		flushTs := tk.ss.streamKeyspaceIdLastFlushedTsMap[streamId][keyspaceId]

		tk.checkInitialBuildDone(streamId, keyspaceId, flushTs)
		tk.checkPendingStreamMerge(streamId, keyspaceId)

		//check if there is any pending TS for this keyspaceId/stream.
		//It can be processed now.
		tk.processPendingTS(streamId, keyspaceId)

	case STREAM_PREPARE_RECOVERY:

		//check if there is any pending TS for this keyspaceId/stream.
		//It can be processed now.
		found := tk.processPendingTS(streamId, keyspaceId)

		if !found && !tk.ss.checkAnyAbortPending(streamId, keyspaceId) {
			sessionId := tk.ss.getSessionId(streamId, keyspaceId)
			//send message to stop running stream
			tk.supvRespch <- &MsgRecovery{mType: INDEXER_PREPARE_RECOVERY,
				streamId:   streamId,
				keyspaceId: keyspaceId,
				sessionId:  sessionId}
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
	keyspaceId := cmd.(*MsgMutMgrFlushDone).GetKeyspaceId()

	state := tk.ss.streamKeyspaceIdStatus[streamId][keyspaceId]

	switch state {

	case STREAM_ACTIVE:

		if tk.checkCatchupStreamReadyToMerge(cmd) {
			//if stream is ready to merge, further processing is
			//not required, return from here.
			return
		}

		//check if there is any pending TS for this keyspaceId/stream.
		//It can be processed now.
		tk.processPendingTS(streamId, keyspaceId)

	case STREAM_PREPARE_RECOVERY:

		//check if there is any pending TS for this keyspaceId/stream.
		//It can be processed now.
		found := tk.processPendingTS(streamId, keyspaceId)

		//if no more pending TS were found and there is no abort or flush
		//in progress recovery can be initiated
		if !found {
			if tk.checkKeyspaceReadyForRecovery(streamId, keyspaceId) {
				tk.initiateRecovery(streamId, keyspaceId)
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
	keyspaceId := cmd.(*MsgMutMgrFlushDone).GetKeyspaceId()

	state := tk.ss.streamKeyspaceIdStatus[streamId][keyspaceId]

	switch state {

	case STREAM_ACTIVE:

		//check if any of the initial build index is past its Build TS.
		//Generate msg for Build Done and change the state of the index.
		if tk.checkAnyInitialStateIndex(keyspaceId) {
			flushTs := tk.ss.streamKeyspaceIdLastFlushedTsMap[streamId][keyspaceId]
			tk.checkInitialBuildDone(streamId, keyspaceId, flushTs)
		} else {

			//if flush is for INIT_STREAM, check if any index in CATCHUP has reached
			//past the last flushed TS of the MAINT_STREAM for this keyspace.
			//In such case, all indexes of the keyspace can merged to MAINT_STREAM.
			lastFlushedTs := tk.ss.streamKeyspaceIdLastFlushedTsMap[streamId][keyspaceId]
			if tk.checkInitStreamReadyToMerge(streamId, keyspaceId, lastFlushedTs, false) {
				//if stream is ready to merge, further STREAM_ACTIVE processing is
				//not required, return from here.
				break
			}
		}

		//check if there is any pending TS for this keyspaceId/stream.
		//It can be processed now.
		tk.processPendingTS(streamId, keyspaceId)

	case STREAM_PREPARE_RECOVERY:

		//check if there is any pending TS for this keyspaceId/stream.
		//It can be processed now.
		found := tk.processPendingTS(streamId, keyspaceId)
		sessionId := tk.ss.getSessionId(streamId, keyspaceId)

		if !found && !tk.ss.checkAnyAbortPending(streamId, keyspaceId) {
			//send message to stop running stream
			tk.supvRespch <- &MsgRecovery{mType: INDEXER_PREPARE_RECOVERY,
				streamId:   streamId,
				keyspaceId: keyspaceId,
				sessionId:  sessionId}
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
	keyspaceId := cmd.(*MsgMutMgrFlushDone).GetKeyspaceId()

	tk.lock.Lock()
	defer tk.lock.Unlock()

	state := tk.ss.streamKeyspaceIdStatus[streamId][keyspaceId]

	switch state {

	case STREAM_ACTIVE:
		logging.Errorf("Timekeeper::handleFlushAbortDone Unexpected Flush Abort "+
			"Received for Active StreamId %v.", streamId)

	case STREAM_PREPARE_RECOVERY:
		sessionId := tk.ss.getSessionId(streamId, keyspaceId)
		keyspaceIdFlushInProgressTsMap := tk.ss.streamKeyspaceIdFlushInProgressTsMap[streamId]
		if _, ok := keyspaceIdFlushInProgressTsMap[keyspaceId]; ok {
			//if there is flush in progress, mark it as done
			if keyspaceIdFlushInProgressTsMap[keyspaceId] != nil {
				keyspaceIdFlushInProgressTsMap[keyspaceId] = nil
				doneCh := tk.ss.streamKeyspaceIdFlushDone[streamId][keyspaceId]
				if doneCh != nil {
					close(doneCh)
					tk.ss.streamKeyspaceIdFlushDone[streamId][keyspaceId] = nil
				}
			}
		} else {
			//this keyspace is already gone from this stream, may be because
			//the index were dropped. Log and ignore.
			logging.Warnf("Timekeeper::handleFlushDone Ignore Flush Abort for Stream %v "+
				"KeyspaceId %v SessionId %v. KeyspaceId Info Not Found", streamId, keyspaceId, sessionId)
			tk.supvCmdch <- &MsgSuccess{}
			return
		}

		//update abort status and initiate recovery
		keyspaceIdAbortInProgressMap := tk.ss.streamKeyspaceIdAbortInProgressMap[streamId]
		keyspaceIdAbortInProgressMap[keyspaceId] = false

		//send message to stop running stream
		tk.supvRespch <- &MsgRecovery{mType: INDEXER_PREPARE_RECOVERY,
			streamId:   streamId,
			keyspaceId: keyspaceId,
			sessionId:  sessionId}

	case STREAM_RECOVERY, STREAM_INACTIVE:
		logging.Errorf("Timekeeper::handleFlushAbortDone Unexpected Flush Abort "+
			"Received for StreamId %v KeyspaceId %v State %v", streamId, keyspaceId, state)

	default:
		logging.Errorf("Timekeeper::handleFlushAbortDone Invalid Stream State %v "+
			"for StreamId %v KeyspaceId %v", state, streamId, keyspaceId)

	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleFlushStateChange(cmd Message) {

	t := cmd.(*MsgTKToggleFlush).GetMsgType()
	streamId := cmd.(*MsgTKToggleFlush).GetStreamId()
	keyspaceId := cmd.(*MsgTKToggleFlush).GetKeyspaceId()

	logging.Infof("Timekeeper::handleFlushStateChange Received Flush State Change "+
		"for KeyspaceId: %v StreamId: %v Type: %v", keyspaceId, streamId, t)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	state := tk.ss.streamKeyspaceIdStatus[streamId][keyspaceId]

	if state == STREAM_INACTIVE {
		logging.Infof("Timekeeper::handleFlushStateChange Ignore Flush State Change "+
			"for KeyspaceId: %v StreamId: %v Type: %v State: %v", keyspaceId, streamId, t, state)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	switch t {

	case TK_ENABLE_FLUSH:

		//if keyspaceId is empty, enable for all keyspaceIds in the stream
		keyspaceIdFlushEnabledMap := tk.ss.streamKeyspaceIdFlushEnabledMap[streamId]
		if keyspaceId == "" {
			for keyspaceId := range keyspaceIdFlushEnabledMap {
				keyspaceIdFlushEnabledMap[keyspaceId] = true
				//if there are any pending TS, send that
				tk.processPendingTS(streamId, keyspaceId)
			}
		} else {
			keyspaceIdFlushEnabledMap[keyspaceId] = true
			//if there are any pending TS, send that
			tk.processPendingTS(streamId, keyspaceId)
		}

	case TK_DISABLE_FLUSH:

		//TODO What about the flush which is already in progress
		//if keyspaceId is empty, disable for all keyspaceIds in the stream
		keyspaceIdFlushEnabledMap := tk.ss.streamKeyspaceIdFlushEnabledMap[streamId]
		if keyspaceId == "" {
			for keyspaceId := range keyspaceIdFlushEnabledMap {
				keyspaceIdFlushEnabledMap[keyspaceId] = false
			}
		} else {
			keyspaceIdFlushEnabledMap[keyspaceId] = false
		}
	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleGetKeyspaceHWT(cmd Message) {

	logging.Debugf("Timekeeper::handleGetKeyspaceHWT %v", cmd)

	streamId := cmd.(*MsgKeyspaceHWT).GetStreamId()
	keyspaceId := cmd.(*MsgKeyspaceHWT).GetKeyspaceId()

	tk.lock.Lock()
	defer tk.lock.Unlock()

	//set the return ts to nil
	msg := cmd.(*MsgKeyspaceHWT)
	msg.hwt = nil

	if keyspaceIdHWTMap, ok := tk.ss.streamKeyspaceIdHWTMap[streamId]; ok {
		if ts, ok := keyspaceIdHWTMap[keyspaceId]; ok {
			newTs := ts.Copy()
			msg.hwt = newTs
		}
	}
	tk.supvCmdch <- msg
}

func (tk *timekeeper) handleStreamBegin(cmd Message) {

	streamId := cmd.(*MsgStream).GetStreamId()
	meta := cmd.(*MsgStream).GetMutationMeta()
	node := cmd.(*MsgStream).GetNode()

	defer meta.Free()

	var host string
	if node != nil {
		nodeUUID := fmt.Sprintf("%s", node)
		stats := tk.stats.Get()
		if stats != nil {
			nodeToHostMap := stats.nodeToHostMap.Get()
			if nodeToHostMap != nil {
				host = nodeToHostMap[nodeUUID]
			}
		}
	}

	var ss, se, seq uint64
	hwt := tk.ss.streamKeyspaceIdHWTMap[streamId][meta.keyspaceId]
	if hwt != nil {
		ss = hwt.Snapshots[meta.vbucket][0]
		se = hwt.Snapshots[meta.vbucket][1]
		seq = hwt.Seqnos[meta.vbucket]
	}

	logging.Infof("TK StreamBegin %v %v %v %v %v %v %v. HWT [%v-%v,%v].", streamId, meta.keyspaceId,
		meta.vbucket, meta.vbuuid, meta.seqno, meta.opaque, host, ss, se, seq)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	if tk.indexerState == INDEXER_PREPARE_UNPAUSE {
		logging.Warnf("Timekeeper::handleStreamBegin Received StreamBegin In "+
			"Prepare Unpause State. KeyspaceId %v Stream %v. Ignored.", meta.keyspaceId, streamId)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	//check if keyspace is active in stream
	if tk.checkKeyspaceActiveInStream(streamId, meta.keyspaceId) == false {
		logging.Warnf("Timekeeper::handleStreamBegin Received StreamBegin for "+
			"Inactive KeyspaceId %v Stream %v. Ignored.", meta.keyspaceId, streamId)
		return
	}

	//if there are no indexes for this keyspace and stream, ignore
	if c, ok := tk.ss.streamKeyspaceIdIndexCountMap[streamId][meta.keyspaceId]; !ok || c <= 0 {
		logging.Warnf("Timekeeper::handleStreamBegin Ignore StreamBegin for StreamId %v "+
			"KeyspaceId %v. IndexCount %v. ", streamId, meta.keyspaceId, c)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	//if the session doesn't match, ignore
	sessionId := tk.ss.getSessionId(streamId, meta.keyspaceId)
	if meta.opaque != 0 && sessionId != meta.opaque {
		logging.Warnf("Timekeeper::handleStreamBegin Ignore StreamBegin for StreamId %v "+
			"KeyspaceId %v. SessionId %v. Current Session %v ", streamId, meta.keyspaceId,
			meta.opaque, sessionId)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	state := tk.ss.streamKeyspaceIdStatus[streamId][meta.keyspaceId]

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
			tk.updateStreamKeyspaceIdVbMap(cmd)

			// When receivng a StreamBegin, it means that the projector claims ownership
			// of a vbucket.   Keep track of how many projectors are claiming ownership.
			tk.ss.incVbRefCount(streamId, meta.keyspaceId, meta.vbucket)

			//update the HWT of this stream and keyspaceId with the vbuuid
			keyspaceIdHWTMap := tk.ss.streamKeyspaceIdHWTMap[streamId]

			// Always use the vbuuid of the last StreamBegin
			ts := keyspaceIdHWTMap[meta.keyspaceId]
			ts.Vbuuids[meta.vbucket] = uint64(meta.vbuuid)

			// New TS needs to be generated for vbuuid change as stale=false scans
			// can only succeed if all vbuuids are latest. Also if there are no docs
			// in a vbucket and its stream begin arrives later than all mutations,
			// there will be no TS with that vbuuid.
			tk.ss.streamKeyspaceIdNewTsReqdMap[streamId][meta.keyspaceId] = true

			tk.ss.updateVbStatus(streamId, meta.keyspaceId, []Vbucket{meta.vbucket}, VBS_STREAM_BEGIN)

			// record the time when receiving the last StreamBegin
			tk.ss.streamKeyspaceIdLastBeginTime[streamId][meta.keyspaceId] = uint64(time.Now().UnixNano())

			//  record ActiveTs
			tk.ss.addKVActiveTs(streamId, meta.keyspaceId, meta.vbucket, meta.seqno, uint64(meta.vbuuid))
			tk.ss.clearKVRollbackTs(streamId, meta.keyspaceId, meta.vbucket)
			tk.ss.clearKVPendingTs(streamId, meta.keyspaceId, meta.vbucket)
			tk.ss.clearLastRepairTime(streamId, meta.keyspaceId, meta.vbucket)
			tk.ss.clearRepairState(streamId, meta.keyspaceId, meta.vbucket)

			if tk.ss.getVbRefCount(streamId, meta.keyspaceId, meta.vbucket) > 1 {
				logging.Infof("Timekeeper::handleStreamBegin Owner count > 1. Treat as CONN_ERR. "+
					"StreamId %v MutationMeta %v", streamId, meta)

				// This will trigger repairStream, as well as replying to supervisor channel
				tk.handleStreamConnErrorInternal(streamId, meta.keyspaceId, []Vbucket{meta.vbucket})
				return
			}
		}

		// If status is STREAM_ROLLBACK, set rollbackTs in stream state.  Update vb status to
		// STREAM_BEGIN since at least one projector has claimed this vb (even though it needs
		// rollback).  Trigger stream repair.
		// Do not update ref count since projector repeatedly send STREAM_ROLLBACK to indexer.
		if cmd.(*MsgStream).GetStatus() == common.STREAM_ROLLBACK {
			logging.Warnf("Timekeeper::handleStreamBegin StreamBegin rollback for StreamId %v "+
				"KeyspaceId %v vbucket %v. Rollback (%v, %16x). ", streamId, meta.keyspaceId, meta.vbucket, meta.seqno, meta.vbuuid)

			tk.ss.updateVbStatus(streamId, meta.keyspaceId, []Vbucket{meta.vbucket}, VBS_STREAM_BEGIN)

			// record the time when receiving the first rollback for this vb.  Note that projector
			// can keep sending rollback streamBegin on repeated MTR or restartVB.  So only set
			// begin time on the first time we see rollback.
			if _, vbuuid := tk.ss.getKVRollbackTs(streamId, meta.keyspaceId, meta.vbucket); vbuuid == 0 {
				tk.ss.streamKeyspaceIdLastBeginTime[streamId][meta.keyspaceId] = uint64(time.Now().UnixNano())
			}

			// record the rollback Ts
			tk.ss.addKVRollbackTs(streamId, meta.keyspaceId, meta.vbucket, meta.seqno, uint64(meta.vbuuid))
			tk.ss.clearKVActiveTs(streamId, meta.keyspaceId, meta.vbucket)
			tk.ss.clearKVPendingTs(streamId, meta.keyspaceId, meta.vbucket)
			tk.ss.clearLastRepairTime(streamId, meta.keyspaceId, meta.vbucket)
			tk.ss.clearRepairState(streamId, meta.keyspaceId, meta.vbucket)

			// Trigger repair.   If this is the last vb, then we will need to rollback.
			needRepair = true
		}

		cmdStatus := cmd.(*MsgStream).GetStatus()
		if cmdStatus == common.STREAM_UNKNOWN_COLLECTION || cmdStatus == common.STREAM_UNKNOWN_SCOPE {
			logging.Warnf("Timekeeper::handleStreamBegin StreamBegin %v for StreamId %v "+
				"KeyspaceId %v vbucket %v", cmdStatus, streamId, meta.keyspaceId, meta.vbucket)

			delete(tk.ss.streamKeyspaceIdRepairStopCh[streamId], meta.keyspaceId)
			tk.ss.streamKeyspaceIdStatus[streamId][meta.keyspaceId] = STREAM_INACTIVE

			tk.supvRespch <- &MsgRecovery{mType: INDEXER_KEYSPACE_NOT_FOUND,
				streamId:   streamId,
				keyspaceId: meta.keyspaceId,
				sessionId:  sessionId}
			logging.Infof("Timekeeper::handleStreamBegin Sent Keyspace not found message to indexer for StreamId %v "+
				"KeyspaceId %v vbucket %v", streamId, meta.keyspaceId, meta.vbucket)
		}

		// Trigger stream repair.
		if needRepair {
			if stopCh, ok := tk.ss.streamKeyspaceIdRepairStopCh[streamId][meta.keyspaceId]; !ok || stopCh == nil {
				tk.ss.streamKeyspaceIdRepairStopCh[streamId][meta.keyspaceId] = make(StopChannel)
				logging.Infof("Timekeeper::handleStreamBegin start repairStream. StreamId %v MutationMeta %v", streamId, meta)
				go tk.repairStream(streamId, meta.keyspaceId)
			}
		}

	case STREAM_PREPARE_RECOVERY, STREAM_PREPARE_DONE, STREAM_INACTIVE:
		//ignore stream begin in prepare_recovery
		logging.Verbosef("Timekeeper::handleStreamBegin Ignore StreamBegin "+
			"for StreamId %v State %v MutationMeta %v", streamId, state, meta)

	default:
		logging.Errorf("Timekeeper::handleStreamBegin Invalid Stream State "+
			"StreamId %v KeyspaceId %v State %v", streamId, meta.keyspaceId, state)
	}

	tk.supvCmdch <- &MsgSuccess{}

}

func (tk *timekeeper) handleStreamEnd(cmd Message) {

	streamId := cmd.(*MsgStream).GetStreamId()
	meta := cmd.(*MsgStream).GetMutationMeta()

	defer meta.Free()

	logging.Infof("TK StreamEnd %v %v %v %v %v", streamId, meta.keyspaceId,
		meta.vbucket, meta.vbuuid, meta.seqno)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	if tk.indexerState == INDEXER_PREPARE_UNPAUSE {
		logging.Warnf("Timekeeper::handleStreamEnd Received StreamEnd In "+
			"Prepare Unpause State. KeyspaceId %v Stream %v. Ignored.", meta.keyspaceId, streamId)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	//check if keyspaceId is active in stream
	if tk.checkKeyspaceActiveInStream(streamId, meta.keyspaceId) == false {
		logging.Warnf("Timekeeper::handleStreamEnd Received StreamEnd for "+
			"Inactive KeyspaceId %v Stream %v. Ignored.", meta.keyspaceId, streamId)
		return
	}

	//if there are no indexes for this keyspaceId and stream, ignore
	if c, ok := tk.ss.streamKeyspaceIdIndexCountMap[streamId][meta.keyspaceId]; !ok || c <= 0 {
		logging.Warnf("Timekeeper::handleStreamEnd Ignore StreamEnd for StreamId %v "+
			"KeyspaceId %v. IndexCount %v. ", streamId, meta.keyspaceId, c)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	//if the session doesn't match, ignore
	sessionId := tk.ss.getSessionId(streamId, meta.keyspaceId)
	if meta.opaque != 0 && sessionId != meta.opaque {
		logging.Warnf("Timekeeper::handleStreamEnd Ignore StreamEnd for StreamId %v "+
			"KeyspaceId %v. SessionId %v. Current Session %v ", streamId, meta.keyspaceId,
			meta.opaque, sessionId)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	state := tk.ss.streamKeyspaceIdStatus[streamId][meta.keyspaceId]
	switch state {

	case STREAM_ACTIVE:

		// When receivng a StreamEnd, it means that the projector releases ownership of vb.
		// Update the vb count since there is one less owner.  Ignore any StreamEnd if
		// (1) vb has not received a STREAM_BEGIN, (2) recovering from CONN_ERROR
		//
		vbState := tk.ss.getVbStatus(streamId, meta.keyspaceId, meta.vbucket)
		if vbState != VBS_CONN_ERROR && vbState != VBS_INIT {

			tk.ss.decVbRefCount(streamId, meta.keyspaceId, meta.vbucket)
			count := tk.ss.getVbRefCount(streamId, meta.keyspaceId, meta.vbucket)

			if count < 0 {
				// If count < 0, after CONN_ERROR, count is reset to 0.   During rebalancing,
				// residue StreamEnd from old master can still arrive.   Therefore, after
				// CONN_ERROR, the total number of StreamEnd > total number of StreamBegin,
				// and count will be negative in this case.
				logging.Infof("Timekeeper::handleStreamEnd Owner count < 0. Treat as CONN_ERR. "+
					"StreamId %v MutationMeta %v", streamId, meta)

				tk.handleStreamConnErrorInternal(streamId, meta.keyspaceId, []Vbucket{meta.vbucket})
				return

			} else {

				tk.ss.updateVbStatus(streamId, meta.keyspaceId, []Vbucket{meta.vbucket}, VBS_STREAM_END)
				tk.ss.setLastRepairTime(streamId, meta.keyspaceId, meta.vbucket)

				// clear all Ts for this vbucket
				tk.ss.clearKVActiveTs(streamId, meta.keyspaceId, meta.vbucket)
				tk.ss.clearKVPendingTs(streamId, meta.keyspaceId, meta.vbucket)
				tk.ss.clearKVRollbackTs(streamId, meta.keyspaceId, meta.vbucket)
				tk.ss.clearRepairState(streamId, meta.keyspaceId, meta.vbucket)

				// If Count => 0.  This could be just normal vb take-over during rebalancing.
				if stopCh, ok := tk.ss.streamKeyspaceIdRepairStopCh[streamId][meta.keyspaceId]; !ok || stopCh == nil {
					tk.ss.streamKeyspaceIdRepairStopCh[streamId][meta.keyspaceId] = make(StopChannel)
					logging.Infof("Timekeeper::handleStreamEnd RepairStream due to StreamEnd. "+
						"StreamId %v MutationMeta %v", streamId, meta)
					go tk.repairStream(streamId, meta.keyspaceId)
				}
			}
		}

	case STREAM_PREPARE_RECOVERY, STREAM_PREPARE_DONE, STREAM_INACTIVE:
		//ignore stream end in prepare_recovery
		logging.Verbosef("Timekeeper::handleStreamEnd Ignore StreamEnd for "+
			"StreamId %v State %v MutationMeta %v", streamId, state, meta)

	default:
		logging.Errorf("Timekeeper::handleStreamEnd Invalid Stream State "+
			"StreamId %v KeyspaceId %v State %v", streamId, meta.keyspaceId, state)
	}

	tk.supvCmdch <- &MsgSuccess{}

}

func (tk *timekeeper) handleStreamConnError(cmd Message) {

	logging.Debugf("Timekeeper::handleStreamConnError %v", cmd)

	streamId := cmd.(*MsgStreamInfo).GetStreamId()
	keyspaceId := cmd.(*MsgStreamInfo).GetKeyspaceId()
	vbList := cmd.(*MsgStreamInfo).GetVbList()

	logging.Infof("TK ConnError %v %v %v", streamId, keyspaceId, vbList)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	if len(keyspaceId) != 0 && len(vbList) != 0 {
		tk.handleStreamConnErrorInternal(streamId, keyspaceId, vbList)
	} else {
		tk.supvCmdch <- &MsgSuccess{}
	}

	if stopCh, ok := tk.vbCheckerStopCh[streamId]; stopCh == nil || !ok {
		logging.Infof("Timekeeper::handleStreamConnError Call RepairMissingStreamBegin to check for vbucket for repair. "+
			"StreamId %v KeyspaceId %v", streamId, keyspaceId)
		tk.vbCheckerStopCh[streamId] = make(chan bool)
		go tk.repairMissingStreamBegin(streamId)
	}
}

func (tk *timekeeper) computeVbWithMissingStreamBegin(streamId common.StreamId) KeyspaceIdVbStatusMap {

	result := make(KeyspaceIdVbStatusMap)

	for keyspaceId, ts := range tk.ss.streamKeyspaceIdVbStatusMap[streamId] {

		// only check for stream in ACTIVE or RECOVERY state
		keyspaceIdStatus := tk.ss.streamKeyspaceIdStatus[streamId][keyspaceId]
		if keyspaceIdStatus == STREAM_ACTIVE || keyspaceIdStatus == STREAM_RECOVERY {
			hasMissing := false
			for _, status := range ts {
				if status == VBS_INIT {
					hasMissing = true
				}
			}

			if hasMissing {
				result[keyspaceId] = CopyTimestamp(ts)
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
		tk.vbCheckerStopCh[streamId] = nil
	}()

	// compute any vb that is missing StreamBegin
	missing := func() KeyspaceIdVbStatusMap {
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
		case <-tk.vbCheckerStopCh[streamId]:
			return
		case <-ticker:
		}

		missing = func() KeyspaceIdVbStatusMap {

			tk.lock.Lock()
			defer tk.lock.Unlock()

			now := uint64(time.Now().UnixNano())

			// After sleep, find the vb that has not yet received StreamBegin.  If the same set
			// of vb are still missing before and after sleep, then we can assume those vb will
			// need to repair.   If the set of vb are not identical, then retry again.
			newMissing := tk.computeVbWithMissingStreamBegin(streamId)

			toFix := make(map[string][]Vbucket)
			for keyspaceId, oldTs := range missing {

				vbList := []Vbucket(nil)

				// if stream request is not done (we have not yet got
				// the activeTs), then do not process this. Also wait for
				// 30s after stream request before checking.
				if tk.ss.streamKeyspaceIdOpenTsMap[streamId][keyspaceId] == nil ||
					tk.ss.streamKeyspaceIdStartTimeMap[streamId][keyspaceId] == 0 ||
					now-tk.ss.streamKeyspaceIdStartTimeMap[streamId][keyspaceId] < uint64(30*time.Second) {
					// if the keyspaceId still exist
					if newTs, ok := newMissing[keyspaceId]; ok && newTs != nil {
						delete(newMissing, keyspaceId)
						newMissing[keyspaceId] = oldTs
					}
				} else {
					// compare the vb status before and after sleep
					for i, oldStatus := range oldTs {
						if oldStatus == VBS_INIT {
							if newStatus, ok := newMissing[keyspaceId]; ok && newStatus[i] == oldStatus {
								vbList = append(vbList, Vbucket(i))
							}
						}
					}
				}

				// if the vb are still missing StreamBegin, then we need to repair this keyspaceId.
				if len(vbList) != 0 {
					toFix[keyspaceId] = vbList
				}
			}

			// Repair the vb in the keyspaceId
			maxInterval := uint64(tk.config["timekeeper.escalate.StreamBeginWaitTime"].Int()) * uint64(time.Second)

			for keyspaceId, vbList := range toFix {

				keyspaceIdStatus := tk.ss.streamKeyspaceIdStatus[streamId][keyspaceId]
				if len(vbList) != 0 && (keyspaceIdStatus == STREAM_ACTIVE || keyspaceIdStatus == STREAM_RECOVERY) {

					// If there is missing vb and timekeeper has not received StreamBegin for any vb for over the threshold,
					// then raise an error on those vbs.
					if now-tk.ss.streamKeyspaceIdLastBeginTime[streamId][keyspaceId] > maxInterval {

						//flag the missing vb as error and repair stream.  Do not raise connection error.
						for _, vb := range vbList {
							tk.ss.updateVbStatus(streamId, keyspaceId, []Vbucket{vb}, VBS_STREAM_END)
							tk.ss.clearVbRefCount(streamId, keyspaceId, vb)

							// clear all Ts for this vbucket
							tk.ss.clearKVActiveTs(streamId, keyspaceId, vb)
							tk.ss.clearKVPendingTs(streamId, keyspaceId, vb)
							tk.ss.clearKVRollbackTs(streamId, keyspaceId, vb)
							tk.ss.clearRepairState(streamId, keyspaceId, vb)
							tk.ss.setLastRepairTime(streamId, keyspaceId, vb)
						}

						if stopCh, ok := tk.ss.streamKeyspaceIdRepairStopCh[streamId][keyspaceId]; !ok || stopCh == nil {
							tk.ss.streamKeyspaceIdRepairStopCh[streamId][keyspaceId] = make(StopChannel)
							logging.Infof("Timekeeper::repairWithMissingStreamBegin. Repair StreamId %v keyspaceId %v vbuckets %v", streamId, keyspaceId, vbList)
							go tk.repairStream(streamId, keyspaceId)
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

func (tk *timekeeper) handleStreamConnErrorInternal(streamId common.StreamId, keyspaceId string, vbList []Vbucket) {

	if tk.indexerState == INDEXER_PREPARE_UNPAUSE {
		logging.Warnf("Timekeeper::handleStreamConnError Received ConnError In "+
			"Prepare Unpause State. KeyspaceId %v Stream %v. Ignored.", keyspaceId, streamId)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	//check if keyspaceId is active in stream
	if tk.checkKeyspaceActiveInStream(streamId, keyspaceId) == false {
		logging.Warnf("Timekeeper::handleStreamConnError Received ConnError for "+
			"Inactive KeyspaceId %v Stream %v. Ignored.", keyspaceId, streamId)
		return
	}

	state := tk.ss.streamKeyspaceIdStatus[streamId][keyspaceId]
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
			tk.ss.makeConnectionError(streamId, keyspaceId, vb)
		}

		if stopCh, ok := tk.ss.streamKeyspaceIdRepairStopCh[streamId][keyspaceId]; !ok || stopCh == nil {
			tk.ss.streamKeyspaceIdRepairStopCh[streamId][keyspaceId] = make(StopChannel)
			logging.Infof("Timekeeper::handleStreamConnError RepairStream due to ConnError. "+
				"StreamId %v KeyspaceId %v VbList %v", streamId, keyspaceId, vbList)
			go tk.repairStream(streamId, keyspaceId)
		} else {
			logging.Infof("Timekeeper::handleStreamConnErr Stream repair is already in progress "+
				"for stream: %v, keyspaceId: %v", streamId, keyspaceId)
		}

	case STREAM_PREPARE_RECOVERY, STREAM_PREPARE_DONE, STREAM_INACTIVE:
		logging.Verbosef("Timekeeper::handleStreamConnError Ignore Connection Error "+
			"for StreamId %v KeyspaceId %v State %v", streamId, keyspaceId, state)

	default:
		logging.Errorf("Timekeeper::handleStreamConnError Invalid Stream State "+
			"StreamId %v KeyspaceId %v State %v", streamId, keyspaceId, state)
	}

	tk.supvCmdch <- &MsgSuccess{}

}

func (tk *timekeeper) handleDcpSystemEvent(cmd Message) {

	streamId := cmd.(*MsgStream).GetStreamId()
	meta := cmd.(*MsgStream).GetMutationMeta()
	eventType := cmd.(*MsgStream).GetEventType()
	manifestuid := cmd.(*MsgStream).GetManifestUID()

	defer meta.Free()

	logging.Infof("TK SystemEvent %v %v %v %v %v %v %v %v", streamId, meta.keyspaceId,
		meta.vbucket, meta.vbuuid, meta.seqno, meta.opaque, eventType, manifestuid)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	if tk.indexerState == INDEXER_PREPARE_UNPAUSE {
		logging.Warnf("Timekeeper::handleDcpSystemEvent Received SystemEvent In "+
			"Prepare Unpause State. KeyspaceId %v Stream %v. Ignored.", meta.keyspaceId, streamId)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	//check if keyspace is active in stream
	if tk.checkKeyspaceActiveInStream(streamId, meta.keyspaceId) == false {
		logging.Warnf("Timekeeper::handleDcpSystemEvent Received SystemEvent for "+
			"Inactive KeyspaceId %v Stream %v. Ignored.", meta.keyspaceId, streamId)
		return
	}

	//if there are no indexes for this keyspace and stream, ignore
	if c, ok := tk.ss.streamKeyspaceIdIndexCountMap[streamId][meta.keyspaceId]; !ok || c <= 0 {
		logging.Warnf("Timekeeper::handleDcpSystemEvent Ignore SystemEvent for StreamId %v "+
			"KeyspaceId %v. IndexCount %v. ", streamId, meta.keyspaceId, c)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	//if the session doesn't match, ignore
	sessionId := tk.ss.getSessionId(streamId, meta.keyspaceId)
	if meta.opaque != 0 && sessionId != meta.opaque {
		logging.Warnf("Timekeeper::handleDcpSystemEvent Ignore SystemEvent for StreamId %v "+
			"KeyspaceId %v. SessionId %v. Current Session %v ", streamId, meta.keyspaceId,
			meta.opaque, sessionId)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	state := tk.ss.streamKeyspaceIdStatus[streamId][meta.keyspaceId]

	switch state {

	case STREAM_ACTIVE:
		//update the HWT of this stream and keyspaceId with the manifestuid
		keyspaceIdHWTMap := tk.ss.streamKeyspaceIdHWTMap[streamId]
		ts := keyspaceIdHWTMap[meta.keyspaceId]
		ts.ManifestUIDs[meta.vbucket] = manifestuid

	case STREAM_PREPARE_RECOVERY, STREAM_PREPARE_DONE, STREAM_INACTIVE:
		logging.Verbosef("Timekeeper::handleDcpSystemEvent Ignore SystemEvent "+
			"for StreamId %v KeyspaceId %v State %v", streamId, meta.keyspaceId, state)

	default:
		logging.Errorf("Timekeeper::handleDcpSystemEvent Invalid Stream State "+
			"StreamId %v KeyspaceId %v State %v", streamId, meta.keyspaceId, state)
	}

	tk.supvCmdch <- &MsgSuccess{}

}

func (tk *timekeeper) handleOSOSnapshotMarker(cmd Message) {

	streamId := cmd.(*MsgStream).GetStreamId()
	meta := cmd.(*MsgStream).GetMutationMeta()
	eventType := cmd.(*MsgStream).GetEventType()

	defer meta.Free()

	logging.Infof("TK OSOSnapshot %v %v %v %v %v %v %v", streamId, meta.keyspaceId,
		meta.vbucket, meta.vbuuid, meta.seqno, meta.opaque, eventType)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	if tk.indexerState == INDEXER_PREPARE_UNPAUSE {
		logging.Warnf("Timekeeper::handleOSOSnapshotMarker Received OSO Snapshot In "+
			"Prepare Unpause State. KeyspaceId %v Stream %v. Ignored.", meta.keyspaceId, streamId)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	//check if keyspace is active in stream
	if tk.checkKeyspaceActiveInStream(streamId, meta.keyspaceId) == false {
		logging.Warnf("Timekeeper::handleOSOSnapshotMarker Received OSO Snapshot for "+
			"Inactive KeyspaceId %v Stream %v. Ignored.", meta.keyspaceId, streamId)
		return
	}

	//if there are no indexes for this keyspace and stream, ignore
	if c, ok := tk.ss.streamKeyspaceIdIndexCountMap[streamId][meta.keyspaceId]; !ok || c <= 0 {
		logging.Warnf("Timekeeper::handleOSOSnapshotMarker Ignore OSO Snapshot for StreamId %v "+
			"KeyspaceId %v. IndexCount %v. ", streamId, meta.keyspaceId, c)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	//if the session doesn't match, ignore
	sessionId := tk.ss.getSessionId(streamId, meta.keyspaceId)
	if meta.opaque != 0 && sessionId != meta.opaque {
		logging.Warnf("Timekeeper::handleOSOSnapshotMarker Ignore OSO Snapshot for StreamId %v "+
			"KeyspaceId %v. SessionId %v. Current Session %v ", streamId, meta.keyspaceId,
			meta.opaque, sessionId)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	state := tk.ss.streamKeyspaceIdStatus[streamId][meta.keyspaceId]

	switch state {

	case STREAM_ACTIVE:
		if tk.checkAnyInitialStateIndex(meta.keyspaceId) {
			//TODO Collections - generate stream reset message

		}

	case STREAM_PREPARE_RECOVERY, STREAM_PREPARE_DONE, STREAM_INACTIVE:
		logging.Verbosef("Timekeeper::handleOSOSnapshotMarker Ignore OSO Snapshot "+
			"for StreamId %v KeyspaceId %v State %v", streamId, meta.keyspaceId, state)

	default:
		logging.Errorf("Timekeeper::handleOSOSnapshotMarker Invalid Stream State "+
			"StreamId %v KeyspaceId %v State %v", streamId, meta.keyspaceId, state)
	}

	tk.supvCmdch <- &MsgSuccess{}

}

func (tk *timekeeper) handleInitBuildDoneAck(cmd Message) {

	streamId := cmd.(*MsgTKInitBuildDone).GetStreamId()
	keyspaceId := cmd.(*MsgTKInitBuildDone).GetKeyspaceId()
	mergeTs := cmd.(*MsgTKInitBuildDone).GetMergeTs()

	logging.Infof("Timekeeper::handleInitBuildDoneAck StreamId %v KeyspaceId %v",
		streamId, keyspaceId)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	state := tk.ss.streamKeyspaceIdStatus[streamId][keyspaceId]

	//ignore build done ack for Inactive and Recovery phase. For recovery, stream
	//will get reopen and build done will get recomputed.
	if state == STREAM_INACTIVE || state == STREAM_PREPARE_DONE ||
		state == STREAM_PREPARE_RECOVERY {
		logging.Infof("Timekeeper::handleInitBuildDoneAck Ignore BuildDoneAck "+
			"for KeyspaceId: %v StreamId: %v State: %v", keyspaceId, streamId, state)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	//if buildDoneAck is received for INIT_STREAM, this means the index got
	//successfully added to MAINT_STREAM. Set the buildDoneAck flag and the
	//mergeTs for the Catchup state indexes.
	if streamId == common.INIT_STREAM {
		tk.setAddInstPending(streamId, keyspaceId, false)
		if mergeTs != nil {
			tk.setMergeTs(streamId, keyspaceId, mergeTs)
		} else {
			//BuildDoneAck should always have a valid mergeTs. This comes from projector when
			//the index gets added to stream. It cannot be nil otherwise merge will get stuck.
			logging.Fatalf("Timekeeper::handleInitBuildDoneAck %v %v. Received unexpected nil mergeTs.",
				streamId, keyspaceId)
			common.CrashOnError(errors.New("Nil MergeTs Received"))
		}
	}

	if !tk.ss.checkAnyFlushPending(streamId, keyspaceId) &&
		!tk.ss.checkAnyAbortPending(streamId, keyspaceId) {

		lastFlushedTs := tk.ss.streamKeyspaceIdLastFlushedTsMap[streamId][keyspaceId]
		if tk.checkInitStreamReadyToMerge(streamId, keyspaceId, lastFlushedTs, false) {
			//if stream is ready to merge, further STREAM_ACTIVE processing is
			//not required, return from here.
			tk.supvCmdch <- &MsgSuccess{}
			return
		}

		//check if there is any pending TS for this keyspaceId/stream.
		//It can be processed now.
		tk.processPendingTS(streamId, keyspaceId)

	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleAddInstanceFail(cmd Message) {

	streamId := cmd.(*MsgTKInitBuildDone).GetStreamId()
	keyspaceId := cmd.(*MsgTKInitBuildDone).GetKeyspaceId()

	logging.Infof("Timekeeper::handleAddInstanceFail StreamId %v KeyspaceId %v",
		streamId, keyspaceId)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	state := tk.ss.streamKeyspaceIdStatus[streamId][keyspaceId]

	//ignore build done ack for Inactive and Recovery phase. For recovery, stream
	//will get reopen and build done will get recomputed.
	if state == STREAM_INACTIVE || state == STREAM_PREPARE_DONE ||
		state == STREAM_PREPARE_RECOVERY {
		logging.Infof("Timekeeper::handleAddInstanceFail Ignore AddInstanceFail "+
			"for KeyspaceId: %v StreamId: %v State: %v", keyspaceId, streamId, state)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	//if AddInstance fails, reset the AddInstPending flag. Recovery will
	//add these instances back to projector bookkeeping.
	if streamId == common.INIT_STREAM {
		tk.setAddInstPending(streamId, keyspaceId, false)
	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleMergeStreamAck(cmd Message) {

	streamId := cmd.(*MsgTKMergeStream).GetStreamId()
	keyspaceId := cmd.(*MsgTKMergeStream).GetKeyspaceId()

	logging.Infof("Timekeeper::handleMergeStreamAck StreamId %v KeyspaceId %v",
		streamId, keyspaceId)

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleStreamRequestDone(cmd Message) {

	streamId := cmd.(*MsgStreamInfo).GetStreamId()
	keyspaceId := cmd.(*MsgStreamInfo).GetKeyspaceId()
	activeTs := cmd.(*MsgStreamInfo).GetActiveTs()
	pendingTs := cmd.(*MsgStreamInfo).GetPendingTs()
	sessionId := cmd.(*MsgStreamInfo).GetSessionId()

	logging.Infof("Timekeeper::handleStreamRequestDone StreamId %v KeyspaceId %v",
		streamId, keyspaceId)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	//check if keyspace is active in stream
	if tk.checkKeyspaceActiveInStream(streamId, keyspaceId) == false {
		logging.Warnf("Timekeeper::handleStreamRequestDone Received StreamRequestDone for "+
			"Inactive KeyspaceId %v Stream %v. Ignored.", keyspaceId, streamId)
		return
	}

	//if there are no indexes for this keyspace and stream, ignore
	if c, ok := tk.ss.streamKeyspaceIdIndexCountMap[streamId][keyspaceId]; !ok || c <= 0 {
		logging.Warnf("Timekeeper::handleStreamRequestDone Ignore StreamRequestDone for StreamId %v "+
			"KeyspaceId %v. IndexCount %v. ", streamId, keyspaceId, c)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	//if the session doesn't match, ignore
	currSessionId := tk.ss.getSessionId(streamId, keyspaceId)
	if sessionId != 0 && sessionId != currSessionId {
		logging.Warnf("Timekeeper::handleStreamRequestDone Ignore StreamRequestDone for StreamId %v "+
			"KeyspaceId %v. SessionId %v. Current Session %v ", streamId, keyspaceId,
			sessionId, currSessionId)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	if tk.ss.streamKeyspaceIdForceRecovery[streamId][keyspaceId] {

		sessionId := tk.ss.getSessionId(streamId, keyspaceId)

		logging.Infof("Timekeeper::handleStreamRequestDone %v %v. Initiate Recovery "+
			"due to Force Recovery Flag. SessionId %v.", streamId, keyspaceId, sessionId)

		if tk.resetStreamIfOSOEnabled(streamId, keyspaceId, sessionId, true) {
			tk.supvCmdch <- &MsgSuccess{}
			return
		} else {
			tk.supvRespch <- &MsgRecovery{mType: INDEXER_INIT_PREP_RECOVERY,
				streamId:   streamId,
				keyspaceId: keyspaceId,
				restartTs:  tk.ss.computeRestartTs(streamId, keyspaceId),
				sessionId:  sessionId}
			tk.supvCmdch <- &MsgSuccess{}
			return
		}
	}

	tk.ss.streamKeyspaceIdKVActiveTsMap[streamId][keyspaceId] = activeTs
	openTs := activeTs

	tk.ss.streamKeyspaceIdKVPendingTsMap[streamId][keyspaceId] = pendingTs
	if pendingTs != nil {
		openTs = pendingTs.Union(openTs)
	}

	if openTs == nil {
		openTs = common.NewTsVbuuid(GetBucketFromKeyspaceId(keyspaceId), tk.config["numVbuckets"].Int())
	}

	tk.ss.streamKeyspaceIdOpenTsMap[streamId][keyspaceId] = openTs
	tk.ss.streamKeyspaceIdStartTimeMap[streamId][keyspaceId] = uint64(time.Now().UnixNano())

	//Check for possiblity of build done after stream request done.
	//In case of crash recovery, if there are no mutations, there is
	//no flush happening, which can cause index to be in initial state.
	if !tk.ss.checkAnyFlushPending(streamId, keyspaceId) &&
		!tk.ss.checkAnyAbortPending(streamId, keyspaceId) {
		lastFlushedTs := tk.ss.streamKeyspaceIdLastFlushedTsMap[streamId][keyspaceId]
		tk.checkInitialBuildDone(streamId, keyspaceId, lastFlushedTs)
	}

	//If the indexer crashed after processing all mutations but before
	//merge of an index in Catchup state, the merge needs to happen here,
	//as no flush would happen in case there are no more mutations.
	tk.checkPendingStreamMerge(streamId, keyspaceId)

	if tk.indexerState == INDEXER_PREPARE_UNPAUSE {
		logging.Warnf("Timekeeper::handleStreamRequestDone Skip Repair Check In "+
			"Prepare Unpause State. KeyspaceId %v Stream %v.", keyspaceId, streamId)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	// Check if the stream needs repair for streamEnd and ConnErr
	if stopCh, ok := tk.ss.streamKeyspaceIdRepairStopCh[streamId][keyspaceId]; !ok || stopCh == nil {
		tk.ss.streamKeyspaceIdRepairStopCh[streamId][keyspaceId] = make(StopChannel)
		logging.Infof("Timekeeper::handleStreamRequestDone Call RepairStream to check for vbucket for repair. "+
			"StreamId %v keyspaceId %v", streamId, keyspaceId)
		go tk.repairStream(streamId, keyspaceId)
	}

	// Check if the stream needs repair for missing streamBegin
	if stopCh, ok := tk.vbCheckerStopCh[streamId]; stopCh == nil || !ok {
		logging.Infof("Timekeeper::handleStreamRequestDone Call RepairMissingStreamBegin to check for vbucket for repair. "+
			"StreamId %v keyspaceId %v", streamId, keyspaceId)
		tk.vbCheckerStopCh[streamId] = make(chan bool)
		go tk.repairMissingStreamBegin(streamId)
	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleRecoveryDone(cmd Message) {

	streamId := cmd.(*MsgRecovery).GetStreamId()
	keyspaceId := cmd.(*MsgRecovery).GetKeyspaceId()
	mergeTs := cmd.(*MsgRecovery).GetRestartTs()
	activeTs := cmd.(*MsgRecovery).GetActiveTs()
	pendingTs := cmd.(*MsgRecovery).GetPendingTs()
	sessionId := cmd.(*MsgRecovery).GetSessionId()

	logging.Infof("Timekeeper::handleRecoveryDone StreamId %v KeyspaceId %v",
		streamId, keyspaceId)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	//check if keyspace is active in stream
	if tk.checkKeyspaceActiveInStream(streamId, keyspaceId) == false {
		logging.Warnf("Timekeeper::handleRecoveryDone Received RecoveryDone for "+
			"Inactive KeyspaceId %v Stream %v. Ignored.", keyspaceId, streamId)
		return
	}

	//if there are no indexes for this keyspace and stream, ignore
	if c, ok := tk.ss.streamKeyspaceIdIndexCountMap[streamId][keyspaceId]; !ok || c <= 0 {
		logging.Warnf("Timekeeper::handleRecoveryDone Ignore RecoveryDone for StreamId %v "+
			"KeyspaceId %v. IndexCount %v. ", streamId, keyspaceId, c)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	//if the session doesn't match, ignore
	currSessionId := tk.ss.getSessionId(streamId, keyspaceId)
	if sessionId != 0 && sessionId != currSessionId {
		logging.Warnf("Timekeeper::handleRecoveryDone Ignore RecoveryDone for StreamId %v "+
			"KeyspaceId %v. SessionId %v. Current Session %v ", streamId, keyspaceId,
			sessionId, currSessionId)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	if tk.ss.streamKeyspaceIdForceRecovery[streamId][keyspaceId] {

		sessionId := tk.ss.getSessionId(streamId, keyspaceId)

		logging.Infof("Timekeeper::handleRecoveryDone %v %v. Initiate Recovery "+
			"due to Force Recovery Flag. SessionId %v.", streamId, keyspaceId, sessionId)

		if tk.resetStreamIfOSOEnabled(streamId, keyspaceId, sessionId, true) {
			tk.supvCmdch <- &MsgSuccess{}
			return
		} else {
			tk.supvRespch <- &MsgRecovery{mType: INDEXER_INIT_PREP_RECOVERY,
				streamId:   streamId,
				keyspaceId: keyspaceId,
				restartTs:  tk.ss.computeRestartTs(streamId, keyspaceId),
				sessionId:  sessionId}
			tk.supvCmdch <- &MsgSuccess{}
			return

		}
	}

	tk.ss.streamKeyspaceIdKVActiveTsMap[streamId][keyspaceId] = activeTs
	openTs := activeTs

	tk.ss.streamKeyspaceIdKVPendingTsMap[streamId][keyspaceId] = pendingTs
	if pendingTs != nil {
		openTs = pendingTs.Union(openTs)
	}

	if openTs == nil {
		openTs = common.NewTsVbuuid(GetBucketFromKeyspaceId(keyspaceId), tk.config["numVbuckets"].Int())
	}

	tk.resetWaitForRecovery(streamId, keyspaceId)
	tk.ss.streamKeyspaceIdOpenTsMap[streamId][keyspaceId] = openTs
	tk.ss.streamKeyspaceIdStartTimeMap[streamId][keyspaceId] = uint64(time.Now().UnixNano())

	//once MAINT_STREAM gets successfully restarted in recovery, use the restartTs
	//as the mergeTs for Catchup indexes. As part of the MTR, Catchup state indexes
	//get added to MAINT_STREAM.
	if streamId == common.MAINT_STREAM {
		if mergeTs == nil {
			logging.Infof("Timekeeper::handleRecoveryDone %v %v. Received nil mergeTs. "+
				"Considering it as rollback to 0", streamId, keyspaceId)
			numVbuckets := tk.config["numVbuckets"].Int()
			mergeTs = common.NewTsVbuuid(GetBucketFromKeyspaceId(keyspaceId), numVbuckets)
		}
		tk.setMergeTs(streamId, keyspaceId, mergeTs)
	}

	//Check for possiblity of build done after recovery is done.
	//In case of crash recovery, if there are no mutations, there is
	//no flush happening, which can cause index to be in initial state.
	if !tk.ss.checkAnyFlushPending(streamId, keyspaceId) &&
		!tk.ss.checkAnyAbortPending(streamId, keyspaceId) {
		lastFlushedTs := tk.ss.streamKeyspaceIdLastFlushedTsMap[streamId][keyspaceId]
		tk.checkInitialBuildDone(streamId, keyspaceId, lastFlushedTs)
	}

	//If the indexer crashed after processing all mutations but before
	//merge of an index in Catchup state, the merge needs to happen here,
	//as no flush would happen in case there are no more mutations.
	tk.checkPendingStreamMerge(streamId, keyspaceId)

	if tk.indexerState == INDEXER_PREPARE_UNPAUSE {
		logging.Warnf("Timekeeper::handleRecoveryDone Skip Repair Check In "+
			"Prepare Unpause State. KeyspaceId %v Stream %v.", keyspaceId, streamId)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	// Check if the stream needs repair
	if stopCh, ok := tk.ss.streamKeyspaceIdRepairStopCh[streamId][keyspaceId]; !ok || stopCh == nil {
		tk.ss.streamKeyspaceIdRepairStopCh[streamId][keyspaceId] = make(StopChannel)
		logging.Infof("Timekeeper::handleRecoveryDone Call RepairStream to check for vbucket for repair. "+
			"StreamId %v keyspaceId %v", streamId, keyspaceId)
		go tk.repairStream(streamId, keyspaceId)
	}

	// Check if the stream needs repair for missing streamBegin
	if stopCh, ok := tk.vbCheckerStopCh[streamId]; stopCh == nil || !ok {
		logging.Infof("Timekeeper::handleRecoveryDone Call RepairMissingStreamBegin to check for vbucket for repair. "+
			"StreamId %v keyspaceId %v", streamId, keyspaceId)
		tk.vbCheckerStopCh[streamId] = make(chan bool)
		go tk.repairMissingStreamBegin(streamId)
	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleAbortRecovery(cmd Message) {

	logging.Infof("Timekeeper::handleAbortRecovery %v", cmd)

	streamId := cmd.(*MsgRecovery).GetStreamId()
	keyspaceId := cmd.(*MsgRecovery).GetKeyspaceId()

	tk.lock.Lock()
	defer tk.lock.Unlock()

	tk.removeKeyspaceFromStream(streamId, keyspaceId, true)

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleConfigUpdate(cmd Message) {
	cfgUpdate := cmd.(*MsgConfigUpdate)
	tk.config = cfgUpdate.GetConfig()
	tk.ss.UpdateConfig(tk.config)

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) prepareRecovery(streamId common.StreamId,
	keyspaceId string) bool {

	logging.Infof("Timekeeper::prepareRecovery StreamId %v KeyspaceId %v",
		streamId, keyspaceId)

	//change to PREPARE_RECOVERY so that
	//no more TS generation and flush happen.
	if tk.ss.streamKeyspaceIdStatus[streamId][keyspaceId] == STREAM_ACTIVE {

		logging.Infof("Timekeeper::prepareRecovery Stream %v "+
			"KeyspaceId %v State Changed to PREPARE_RECOVERY", streamId, keyspaceId)

		tk.ss.streamKeyspaceIdStatus[streamId][keyspaceId] = STREAM_PREPARE_RECOVERY

		//The existing mutations for which stability TS has already been generated
		//can be safely flushed before initiating recovery. This can reduce the duration
		//of recovery.
		tk.flushOrAbortInProgressTS(streamId, keyspaceId)

	} else {

		logging.Errorf("Timekeeper::prepareRecovery Invalid Prepare Recovery Request")
		return false

	}

	return true

}

func (tk *timekeeper) flushOrAbortInProgressTS(streamId common.StreamId,
	keyspaceId string) {

	keyspaceIdFlushInProgressTsMap := tk.ss.streamKeyspaceIdFlushInProgressTsMap[streamId]
	if ts := keyspaceIdFlushInProgressTsMap[keyspaceId]; ts != nil {

		//If Flush is in progress for the keyspace, check if all the mutations
		//required for this flush have already been received. If not, this flush
		//needs to be aborted. Pending TS for the keyspace can be discarded and
		//all remaining mutations in queue can be drained.
		//Stream can then be moved to RECOVERY

		//TODO this check is best effort right now as there may be more
		//mutations in the queue than what we have received the SYNC messages
		//for. We may end up discarding one TS worth of mutations unnecessarily,
		//but that is fine for now.
		tsVbuuidHWT := tk.ss.streamKeyspaceIdHWTMap[streamId][keyspaceId]
		tsHWT := getSeqTsFromTsVbuuid(tsVbuuidHWT)

		flushTs := getSeqTsFromTsVbuuid(ts)

		//if HWT is greater than flush in progress TS, this means the flush
		//in progress will finish.
		if tsHWT.GreaterThanEqual(flushTs) {
			logging.Infof("Timekeeper::flushOrAbortInProgressTS Processing Flush TS %v "+
				"before recovery for keyspaceId %v streamId %v", ts, keyspaceId, streamId)
		} else {
			//else this flush needs to be aborted. Though some mutations may
			//arrive and flush may get completed before this abort message
			//reaches.
			logging.Infof("Timekeeper::flushOrAbortInProgressTS Aborting Flush TS %v "+
				"before recovery for keyspaceId %v streamId %v", ts, keyspaceId, streamId)

			tk.supvRespch <- &MsgMutMgrFlushMutationQueue{mType: MUT_MGR_ABORT_PERSIST,
				keyspaceId: keyspaceId,
				streamId:   streamId}

			keyspaceIdAbortInProgressMap := tk.ss.streamKeyspaceIdAbortInProgressMap[streamId]
			keyspaceIdAbortInProgressMap[keyspaceId] = true

		}
		//TODO As prepareRecovery is now done only if rollback is received,
		//no point flushing more mutations. Its going to be rolled back anyways.
		//Once Catchup Stream is back, move this logic back.
		//empty the TSList for this keyspace and stream so
		//there is no further processing for this keyspace.
		tsList := tk.ss.streamKeyspaceIdTsListMap[streamId][keyspaceId]
		tsList.Init()

	} else {

		//for the buckets where no flush is in progress, it implies
		//there are no pending TS. So nothing needs to be done and
		//recovery can be initiated.
		sessionId := tk.ss.getSessionId(streamId, keyspaceId)
		logging.Infof("Timekeeper::flushOrAbortInProgressTS Recovery can be initiated for "+
			"KeyspaceId %v Stream %v SessionId %v", keyspaceId, streamId, sessionId)

		//send message to stop running stream
		tk.supvRespch <- &MsgRecovery{mType: INDEXER_PREPARE_RECOVERY,
			streamId:   streamId,
			keyspaceId: keyspaceId,
			sessionId:  sessionId}

	}

}

//checkInitialBuildDone checks if any of the index in Initial State is past its
//Build TS based on the Flush Done Message. It generates msg for Build Done
//and changes the state of the index.
func (tk *timekeeper) checkInitialBuildDone(streamId common.StreamId,
	keyspaceId string, flushTs *common.TsVbuuid) bool {

	//TODO Collections - verify this. wait for stream request to be done
	if ts, ok := tk.ss.streamKeyspaceIdOpenTsMap[streamId][keyspaceId]; !ok || ts == nil {
		return false
	}
	enableOSO := tk.ss.streamKeyspaceIdEnableOSO[streamId][keyspaceId]
	for _, buildInfo := range tk.indexBuildInfo {

		if buildInfo.waitForRecovery {
			continue
		}

		//if index belongs to the flushed keyspaceId and in INITIAL state
		initBuildDone := false
		idx := buildInfo.indexInst
		if idx.Defn.KeyspaceId(idx.Stream) == keyspaceId &&
			idx.Stream == streamId &&
			idx.State == common.INDEX_STATE_INITIAL {

			if buildInfo.buildTs == nil {
				initBuildDone = false
			} else if buildInfo.buildTs.IsZeroTs() { //if buildTs is zero, initial build is done
				initBuildDone = true
			} else if flushTs == nil {
				initBuildDone = false
			} else if enableOSO && (flushTs.HasOpenOSOSnap() || !flushTs.IsSnapAligned()) {
				//build is not complete till all OSO Snap Ends have been received
				initBuildDone = false
			} else if !enableOSO && !flushTs.IsSnapAligned() {
				initBuildDone = false
			} else {
				//check if the flushTS is greater than buildTS
				ts := getSeqTsFromTsVbuuid(flushTs)
				if ts.GreaterThanEqual(buildInfo.buildTs) {
					initBuildDone = true
				}
			}

			if initBuildDone {

				sessionId := tk.ss.getSessionId(streamId, keyspaceId)

				//change all indexes of this keyspaceId to Catchup state.
				tk.changeIndexStateForKeyspaceId(keyspaceId, common.INDEX_STATE_CATCHUP)
				tk.setAddInstPending(streamId, keyspaceId, true)
				tk.ss.streamKeyspaceIdFlushEnabledMap[streamId][keyspaceId] = false

				//reset numNonAlignTS for catchup phase
				keyspaceStats := tk.stats.GetKeyspaceStats(streamId, keyspaceId)
				if keyspaceStats != nil {
					keyspaceStats.numNonAlignTS.Set(0)
				}

				logging.Infof("Timekeeper::checkInitialBuildDone Initial Build Done Index: %v "+
					"Stream: %v KeyspaceId: %v Session: %v BuildTS: %v", idx.InstId, streamId,
					keyspaceId, sessionId, buildInfo.buildTs)

				//generate init build done msg
				tk.supvRespch <- &MsgTKInitBuildDone{
					mType:      TK_INIT_BUILD_DONE,
					streamId:   streamId,
					buildTs:    buildInfo.buildTs,
					keyspaceId: keyspaceId,
					flushTs:    flushTs,
					sessionId:  sessionId}

				return true
			} else {
				var lenInitTs int
				tsList := tk.ss.streamKeyspaceIdTsListMap[streamId][keyspaceId]
				if tsList != nil {
					lenInitTs = tsList.Len()
				}

				forceLog := false
				now := uint64(time.Now().UnixNano())
				sinceLastLog := now - tk.ss.keyspaceIdPendBuildDebugLogTime[keyspaceId]

				//log more debug information if build is not able to complete
				//but doesn't have pending mutations
				if lenInitTs == 0 && sinceLastLog > uint64(300*time.Second) {
					forceLog = true
					tk.ss.keyspaceIdPendBuildDebugLogTime[keyspaceId] = now
				}

				if forceLog || logging.IsEnabled(logging.Verbose) {
					tk.ss.keyspaceIdPendBuildDebugLogTime[keyspaceId] = now
					hwt := tk.ss.streamKeyspaceIdHWTMap[streamId][keyspaceId]
					logging.Infof("Timekeeper::checkInitialBuildDone Index: %v Stream: %v KeyspaceId: %v"+
						" FlushTs %v\n HWT %v", idx.InstId, streamId, keyspaceId, flushTs, hwt)
				}
			}
		}
	}
	return false
}

//checkInitStreamReadyToMerge checks if any index in Catchup State in INIT_STREAM
//has reached past the last flushed TS of the MAINT_STREAM for this keyspaceId.
//In such case, all indexes of the keyspaceId can merged to MAINT_STREAM.
//If fetchKVSeq is true, this function will fetch latest collection/bucket seqnos
//from KV for stream merge check(if required). It should only be used if there is
//no flush activity for INIT_STREAM, as in that case the only way to know about
//latest KV seq nums is to fetch those.
func (tk *timekeeper) checkInitStreamReadyToMerge(streamId common.StreamId,
	keyspaceId string, initFlushTs *common.TsVbuuid, fetchKVSeq bool) bool {

	logging.LazyTrace(func() string {
		return fmt.Sprintf("Timekeeper::checkInitStreamReadyToMerge Stream %v KeyspaceId %v len(buildInfo) %v "+
			"FlushTs %v", streamId, keyspaceId, len(tk.indexBuildInfo), initFlushTs)
	})

	if streamId != common.INIT_STREAM {
		return false
	}

	enableOSO := tk.ss.streamKeyspaceIdEnableOSO[streamId][keyspaceId]
	if enableOSO && initFlushTs.HasOpenOSOSnap() {

		logging.Infof("Timekeeper::checkInitStreamReadyToMerge INIT_STREAM has open OSO Snapshot."+
			"INIT_STREAM cannot be merged. Continue both streams for keyspaceId %v.", keyspaceId)

		logging.LazyVerbose(func() string {
			hwt := tk.ss.streamKeyspaceIdHWTMap[streamId][keyspaceId]
			return fmt.Sprintf("Timekeeper::checkInitStreamReadyToMerge FlushTs %v\n HWT %v", initFlushTs, hwt)
		})
		return false
	}

	bucket, _, _ := SplitKeyspaceId(keyspaceId)

	//if flushTs is not on snap boundary, merge cannot be done
	if !initFlushTs.IsSnapAligned() {
		hwt := tk.ss.streamKeyspaceIdHWTMap[streamId][keyspaceId]

		var lenInitTs, lenMaintTs int
		tsList := tk.ss.streamKeyspaceIdTsListMap[streamId][keyspaceId]
		if tsList != nil {
			lenInitTs = tsList.Len()
		}
		tsListMaint := tk.ss.streamKeyspaceIdTsListMap[common.MAINT_STREAM][bucket]
		if tsListMaint != nil {
			lenMaintTs = tsListMaint.Len()
		}

		logging.Infof("Timekeeper::checkInitStreamReadyToMerge FlushTs Not Snapshot "+
			"Aligned. Continue both streams for keyspaceId %v. INIT PendTsCount %v. "+
			"MAINT PendTsCount %v.", keyspaceId, lenInitTs, lenMaintTs)

		forceLog := false
		now := uint64(time.Now().UnixNano())
		sinceLastLog := now - tk.ss.keyspaceIdPendBuildDebugLogTime[keyspaceId]

		//log more debug information if INIT_STREAM is waiting for long time to merge
		//but doesn't have pending mutations
		if lenInitTs == 0 && sinceLastLog > uint64(300*time.Second) {
			forceLog = true
			tk.ss.keyspaceIdPendBuildDebugLogTime[keyspaceId] = now
		}

		if forceLog || logging.IsEnabled(logging.Verbose) {
			logging.Infof("Timekeeper::checkInitStreamReadyToMerge FlushTs %v\n HWT %v", initFlushTs, hwt)
		}
		return false
	}

	//INIT_STREAM cannot be merged to MAINT_STREAM if it is in Recovery
	mStatus := tk.ss.streamKeyspaceIdStatus[common.MAINT_STREAM][bucket]
	if mStatus == STREAM_PREPARE_RECOVERY ||
		mStatus == STREAM_PREPARE_DONE ||
		mStatus == STREAM_RECOVERY {
		logging.Infof("Timekeeper::checkInitStreamReadyToMerge MAINT_STREAM in %v. "+
			"INIT_STREAM cannot be merged. Continue both streams for keyspaceId %v.",
			tk.ss.streamKeyspaceIdStatus[common.MAINT_STREAM][bucket], keyspaceId)
		return false
	}

	//If any repair is going on, merge cannot happen
	if stopCh, ok := tk.ss.streamKeyspaceIdRepairStopCh[common.MAINT_STREAM][bucket]; ok && stopCh != nil {

		logging.Infof("Timekeeper::checkInitStreamReadyToMerge MAINT_STREAM In Repair."+
			"INIT_STREAM cannot be merged. Continue both streams for keyspaceId %v.", keyspaceId)
		return false
	}

	if stopCh, ok := tk.ss.streamKeyspaceIdRepairStopCh[common.INIT_STREAM][keyspaceId]; ok && stopCh != nil {

		logging.Infof("Timekeeper::checkInitStreamReadyToMerge INIT_STREAM In Repair."+
			"INIT_STREAM cannot be merged. Continue both streams for keyspaceId %v.", keyspaceId)
		return false
	}

	if ts, ok := tk.ss.streamKeyspaceIdOpenTsMap[common.INIT_STREAM][keyspaceId]; !ok || ts == nil {

		logging.Infof("Timekeeper::checkInitStreamReadyToMerge INIT_STREAM MTR In Progress."+
			"INIT_STREAM cannot be merged. Continue both streams for keyspaceId %v.", keyspaceId)
		return false
	}

	for _, buildInfo := range tk.indexBuildInfo {

		//if index belongs to the flushed keyspace and in CATCHUP state and
		//buildDoneAck has been received
		idx := buildInfo.indexInst
		if idx.Defn.KeyspaceId(idx.Stream) == keyspaceId &&
			idx.State == common.INDEX_STATE_CATCHUP &&
			buildInfo.buildDoneAckReceived == true &&
			buildInfo.minMergeTs != nil {

			readyToMerge, initTsSeq := tk.checkFlushTsValidForMerge(streamId, keyspaceId,
				initFlushTs, buildInfo.minMergeTs, fetchKVSeq)

			if readyToMerge {

				//disable flush for MAINT_STREAM for this keyspace, so it doesn't
				//move ahead till merge is complete
				if mStatus == STREAM_ACTIVE {
					if flushEnabled, ok := tk.ss.streamKeyspaceIdFlushEnabledMap[common.MAINT_STREAM][bucket]; ok && flushEnabled {
						tk.ss.streamKeyspaceIdFlushEnabledMap[common.MAINT_STREAM][bucket] = false
					}
				}

				//if keyspace in INIT_STREAM is going to merge, disable flush. No need to waste
				//resources on flush as these mutations will be flushed from MAINT_STREAM anyway.
				tk.ss.streamKeyspaceIdFlushEnabledMap[common.INIT_STREAM][keyspaceId] = false

				//change state of all indexes of this keyspaceId to ACTIVE
				//these indexes get removed later as part of merge message
				//from indexer
				tk.changeIndexStateForKeyspaceId(keyspaceId, common.INDEX_STATE_ACTIVE)

				sessionId := tk.ss.getSessionId(streamId, keyspaceId)

				logging.Infof("Timekeeper::checkInitStreamReadyToMerge Index Ready To Merge. "+
					"Index: %v Stream: %v KeyspaceId: %v SessionId: %v LastFlushTS: %v", idx.InstId,
					streamId, keyspaceId, sessionId, initTsSeq)

				tk.supvRespch <- &MsgTKMergeStream{
					mType:      TK_MERGE_STREAM,
					streamId:   streamId,
					keyspaceId: keyspaceId,
					mergeTs:    initTsSeq,
					sessionId:  sessionId}

				logging.Infof("Timekeeper::checkInitStreamReadyToMerge Stream %v "+
					"KeyspaceId %v State Changed to INACTIVE", streamId, keyspaceId)
				tk.stopTimer(streamId, keyspaceId)
				tk.ss.cleanupKeyspaceIdFromStream(streamId, keyspaceId)
				return true

			} else {
				//check for one index is sufficient as all indexes in a keyspaceId
				//move together
				return false
			}
		}
	}

	return false
}

func (tk *timekeeper) checkFlushTsValidForMerge(streamId common.StreamId, keyspaceId string,
	initFlushTs *common.TsVbuuid, minMergeTs *common.TsVbuuid, fetchKVSeq bool) (bool, *common.TsVbuuid) {

	var maintFlushTs *common.TsVbuuid

	//if the initFlushTs is past the lastFlushTs of this keyspace in MAINT_STREAM,
	//this index can be merged to MAINT_STREAM. If there is a flush in progress,
	//it is important to use that for comparison as after merge MAINT_STREAM will
	//include merged indexes after the in progress flush finishes.
	bucket, _, _ := SplitKeyspaceId(keyspaceId)
	if lts, ok := tk.ss.streamKeyspaceIdFlushInProgressTsMap[common.MAINT_STREAM][bucket]; ok && lts != nil {
		maintFlushTs = lts
	} else {
		maintFlushTs = tk.ss.streamKeyspaceIdLastFlushedTsMap[common.MAINT_STREAM][bucket]
	}

	var maintTsSeq, initTsSeq Timestamp

	//if no flush has happened from MAINT_STREAM, it means it is not running.
	//MAINT_STREAM needs to be started with lastFlushTs of INIT_STREAM
	if maintFlushTs == nil {
		return true, initFlushTs
	}

	//if initFlushTs is nil for INIT_STREAM and non-nil for MAINT_STREAM
	//merge cannot happen
	if maintFlushTs != nil && initFlushTs == nil {
		return false, nil
	}

	cluster := tk.config["clusterAddr"].String()

	initTsSeq = getSeqTsFromTsVbuuid(initFlushTs)
	minMergeTsSeq := getSeqTsFromTsVbuuid(minMergeTs)

	//if INIT_STREAM has not caught upto minMergeTs
	flushedPastMinMergeTs := tk.ss.streamKeyspaceIdPastMinMergeTs[streamId][keyspaceId]

	if !flushedPastMinMergeTs {
		//check and set the flag
		if initTsSeq.GreaterThanEqual(minMergeTsSeq) {
			tk.ss.streamKeyspaceIdPastMinMergeTs[streamId][keyspaceId] = true
		} else {
			cid := tk.ss.streamKeyspaceIdCollectionId[streamId][keyspaceId]
			if cid != "" {
				//vbuuids need to match for index merge
				if !initFlushTs.CompareVbuuids(maintFlushTs) {
					return false, nil
				}
				currCTs, err := common.CollectionSeqnos(cluster, "default", bucket, cid)
				if err != nil {
					logging.Infof("Timekeeper::checkFlushTsValidForMerge CollectionSeqnos err %v. Skipping stream merge.", err)
					return false, nil
				}
				//if the collection high seqno has been reached
				if initTsSeq.GreaterThanEqual(Timestamp(currCTs)) {
					return true, initFlushTs
				}
			}
			return false, nil
		}
	}

	//vbuuids need to match for index merge
	if !initFlushTs.CompareVbuuids(maintFlushTs) {
		return false, nil
	}

	tsList := tk.ss.streamKeyspaceIdTsListMap[streamId][keyspaceId]
	lenInitTs := tsList.Len()

	maintTsSeq = getSeqTsFromTsVbuuid(maintFlushTs)
	if initTsSeq.GreaterThanEqual(maintTsSeq) {
		return true, initFlushTs
	} else if lenInitTs == 0 && fetchKVSeq {
		//If there are mutations pending for the INIT_STREAM, avoid the expensive
		//call to get KV seqnum. Instead, check for stream merge based on
		//the seqnum of received mutations.

		//if this stream is on a collection
		cid := tk.ss.streamKeyspaceIdCollectionId[streamId][keyspaceId]
		if cid != "" {

			//TODO Collections compute the seqnos asynchronously
			currBTs, err := common.BucketSeqnos(cluster, "default", bucket)
			if err != nil {
				logging.Infof("Timekeeper::checkFlushTsValidForMerge BucketSeqnos err %v. Skipping stream merge.", err)
				return false, nil
			}

			currCTs, err := common.CollectionSeqnos(cluster, "default", bucket, cid)
			if err != nil {
				logging.Infof("Timekeeper::checkFlushTsValidForMerge CollectionSeqnos err %v. Skipping stream merge.", err)
				return false, nil
			}

			bucketTsSeq := Timestamp(currBTs)
			if initTsSeq.GreaterThanEqual(Timestamp(currCTs)) &&
				bucketTsSeq.GreaterThanEqual(maintTsSeq) {
				return true, initFlushTs
			}
		}
	}
	return false, nil
}

func (tk *timekeeper) checkCatchupStreamReadyToMerge(cmd Message) bool {

	streamId := cmd.(*MsgMutMgrFlushDone).GetStreamId()
	keyspaceId := cmd.(*MsgMutMgrFlushDone).GetKeyspaceId()

	if streamId == common.CATCHUP_STREAM {

		//get the lowest TS for this keyspace in MAINT_STREAM
		keyspaceIdTsListMap := tk.ss.streamKeyspaceIdTsListMap[common.MAINT_STREAM]
		tsList := keyspaceIdTsListMap[keyspaceId]
		var maintTs *common.TsVbuuid
		if tsList.Len() > 0 {
			e := tsList.Front()
			tsElem := e.Value.(*TsListElem)
			maintTs = tsElem.ts
		} else {
			return false
		}

		//get the lastFlushedTs of CATCHUP_STREAM
		keyspaceIdLastFlushedTsMap := tk.ss.streamKeyspaceIdLastFlushedTsMap[common.CATCHUP_STREAM]
		lastFlushedTs := keyspaceIdLastFlushedTsMap[keyspaceId]
		if lastFlushedTs == nil {
			return false
		}

		if compareTsSnapshot(lastFlushedTs, maintTs) {
			//disable drain for MAINT_STREAM for this keyspace, so it doesn't
			//drop mutations till merge is complete
			tk.ss.streamKeyspaceIdDrainEnabledMap[common.MAINT_STREAM][keyspaceId] = false

			logging.Infof("Timekeeper::checkCatchupStreamReadyToMerge Keyspace Ready To Merge. "+
				"Stream: %v KeyspaceId: %v ", streamId, keyspaceId)

			tk.supvRespch <- &MsgTKMergeStream{
				streamId:   streamId,
				keyspaceId: keyspaceId}

			tk.supvCmdch <- &MsgSuccess{}
			return true
		}
	}
	return false
}

//generates a new StabilityTS
func (tk *timekeeper) generateNewStabilityTS(streamId common.StreamId,
	keyspaceId string) {

	tk.lock.Lock()
	defer tk.lock.Unlock()

	if tk.indexerState != common.INDEXER_ACTIVE {
		return
	}

	if status, ok := tk.ss.streamKeyspaceIdStatus[streamId][keyspaceId]; !ok || status != STREAM_ACTIVE {
		return
	}

	if tk.ss.checkNewTSDue(streamId, keyspaceId) {
		tsElem := tk.ss.getNextStabilityTS(streamId, keyspaceId)

		//persist TS which completes the build
		if tk.isBuildCompletionTs(streamId, keyspaceId, tsElem.ts) {

			if hasTS, ok := tk.ss.streamKeyspaceIdHasBuildCompTSMap[streamId][keyspaceId]; !ok || !hasTS {

				//NOTE For OSO mode, it is fine to create snap as DISK type, as there is no
				//open OSO snapshot at this stage. This snapshot is eligible for recovery.
				logging.Infof("Timekeeper::generateNewStability %v %v setting snapshot "+
					"type as DISK_SNAP due to BuildCompletionTS", streamId, keyspaceId)
				tsElem.ts.SetSnapType(common.DISK_SNAP)
				tk.ss.streamKeyspaceIdHasBuildCompTSMap[streamId][keyspaceId] = true
			}
		}

		if tk.ss.canFlushNewTS(streamId, keyspaceId) {
			tk.sendNewStabilityTS(tsElem, keyspaceId, streamId)
		} else {
			//store the ts in list
			logging.LazyTrace(func() string {
				return fmt.Sprintf(
					"Timekeeper::generateNewStabilityTS %v %v Added TS to Pending List "+
						"%v ", keyspaceId, streamId, tsElem.ts)
			})
			tsList := tk.ss.streamKeyspaceIdTsListMap[streamId][keyspaceId]
			tsList.PushBack(tsElem)
			keyspaceStats := tk.stats.GetKeyspaceStats(streamId, keyspaceId)
			if keyspaceStats != nil {
				keyspaceStats.tsQueueSize.Set(int64(tsList.Len()))
			}

		}
	} else if tk.processPendingTS(streamId, keyspaceId) {
		//nothing to do
	} else {
		if !tk.hasInitStateIndex(streamId, keyspaceId) &&
			tk.ss.checkCommitOverdue(streamId, keyspaceId) {

			tsVbuuid := tk.ss.streamKeyspaceIdLastFlushedTsMap[streamId][keyspaceId].Copy()

			if tsVbuuid.IsSnapAligned() {
				logging.Infof("Timekeeper:: %v %v Forcing Overdue Commit", streamId, keyspaceId)
				tsVbuuid.SetSnapType(common.FORCE_COMMIT)
				tk.ss.streamKeyspaceIdLastPersistTime[streamId][keyspaceId] = time.Now()
				tk.sendNewStabilityTS(&TsListElem{ts: tsVbuuid}, keyspaceId, streamId)
			}
		}

		//For INIT_STREAM, if there is no new/pending TS, check for stream merge.
		//fetchKVSeq is set to true here to allow merge to fetch latest bucket/collection seqnos
		//for the check. This is an expensive operation and is only done if there is no
		//flush activity.
		if streamId == common.INIT_STREAM {
			if !tk.ss.checkAnyFlushPending(streamId, keyspaceId) &&
				!tk.ss.checkAnyAbortPending(streamId, keyspaceId) {

				lastKVSeqFetchTime := tk.ss.streamKeyspaceIdLastKVSeqFetch[streamId][keyspaceId]

				//if there is no flush activity, check for stream merge every 5 seconds by
				//fetching the KV Seqnums(expensive check)
				if time.Since(lastKVSeqFetchTime) > time.Duration(5*time.Second) {
					lastFlushedTs := tk.ss.streamKeyspaceIdLastFlushedTsMap[streamId][keyspaceId]
					logging.Infof("Timekeeper::generateNewStabilityTS %v %v Check pending stream merge.", streamId, keyspaceId)
					tk.checkInitStreamReadyToMerge(streamId, keyspaceId, lastFlushedTs, true /*fetchKVSeq*/)
					tk.ss.streamKeyspaceIdLastKVSeqFetch[streamId][keyspaceId] = time.Now()
				}
			}
		}
	}
}

//merge a new Ts with one already pending for the stream-bucket,
//if large snapshots are being processed
func (tk *timekeeper) maybeMergeTs(streamId common.StreamId,
	keyspaceId string, newTs *common.TsVbuuid) {

	tsList := tk.ss.streamKeyspaceIdTsListMap[streamId][keyspaceId]
	var lts *common.TsVbuuid
	merge := false

	//get the last generated but not yet processed timestamp
	if tsList.Len() > 0 {
		e := tsList.Back()
		tsElem := e.Value.(*TsListElem)
		lts = tsElem.ts
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

	tsList.PushBack(&TsListElem{ts: newTs})

}

//processPendingTS checks if there is any pending TS for the given stream and
//keyspace. If any TS is found, it is sent to supervisor.
func (tk *timekeeper) processPendingTS(streamId common.StreamId, keyspaceId string) bool {

	//if there is a flush already in progress for this stream and keyspaceId
	//or flush is disabled, nothing to be done
	keyspaceIdFlushInProgressTsMap := tk.ss.streamKeyspaceIdFlushInProgressTsMap[streamId]
	keyspaceIdFlushEnabledMap := tk.ss.streamKeyspaceIdFlushEnabledMap[streamId]

	if keyspaceIdFlushInProgressTsMap[keyspaceId] != nil ||
		keyspaceIdFlushEnabledMap[keyspaceId] == false {
		return false
	}

	//if there are pending TS for this keyspaceId, send New TS
	keyspaceIdTsListMap := tk.ss.streamKeyspaceIdTsListMap[streamId]
	tsList := keyspaceIdTsListMap[keyspaceId]

	if tsList.Len() > 0 {
		e := tsList.Front()
		tsElem := e.Value.(*TsListElem)
		tsVbuuid := tsElem.ts
		tsList.Remove(e)
		ts := getSeqTsFromTsVbuuid(tsVbuuid)

		if tk.ss.streamKeyspaceIdStatus[streamId][keyspaceId] == STREAM_PREPARE_RECOVERY ||
			tk.ss.streamKeyspaceIdStatus[streamId][keyspaceId] == STREAM_PREPARE_DONE {

			tsVbuuidHWT := tk.ss.streamKeyspaceIdHWTMap[streamId][keyspaceId]
			tsHWT := getSeqTsFromTsVbuuid(tsVbuuidHWT)

			//TODO Collections Handle OSO HWT
			//if HWT is greater than flush TS, this TS can be flush
			if tsHWT.GreaterThanEqual(ts) {
				logging.LazyDebug(func() string {
					return fmt.Sprintf(
						"Timekeeper::processPendingTS Processing Flush TS %v "+
							"before recovery for keyspaceId %v streamId %v", ts, keyspaceId, streamId)
				})
			} else {
				//empty the TSList for this keyspaceId and stream so
				//there is no further processing for this keyspaceId.
				logging.LazyDebug(func() string {
					return fmt.Sprintf(
						"Timekeeper::processPendingTS Cannot Flush TS %v "+
							"before recovery for keyspaceId %v streamId %v. Clearing Ts List", ts,
						keyspaceId, streamId)
				})
				tsList.Init()
				return false
			}

		}
		tk.sendNewStabilityTS(tsElem, keyspaceId, streamId)
		//update tsQueueSize when processing queued TS
		keyspaceStats := tk.stats.GetKeyspaceStats(streamId, keyspaceId)
		if keyspaceStats != nil {
			keyspaceStats.tsQueueSize.Set(int64(tsList.Len()))
		}
		return true
	}
	return false
}

//sendNewStabilityTS sends the given TS to supervisor
func (tk *timekeeper) sendNewStabilityTS(tsElem *TsListElem, keyspaceId string,
	streamId common.StreamId) {

	flushTs := tsElem.ts
	logging.LazyTrace(func() string {
		return fmt.Sprintf("Timekeeper::sendNewStabilityTS KeyspaceId: %v "+
			"Stream: %v TS: %v", keyspaceId, streamId, flushTs)
	})

	tk.mayBeMakeSnapAligned(streamId, keyspaceId, flushTs)
	tk.ensureMonotonicTs(streamId, keyspaceId, tsElem)

	var changeVec []bool
	var countVec []uint64
	if flushTs.GetSnapType() != common.FORCE_COMMIT {
		var noChange bool
		changeVec, noChange, countVec = tk.ss.computeTsChangeVec(streamId, keyspaceId, tsElem)
		if noChange {
			return
		}
		tk.setSnapshotType(streamId, keyspaceId, flushTs)
	}

	tk.setNeedsCommit(streamId, keyspaceId, flushTs)

	tk.ss.streamKeyspaceIdFlushInProgressTsMap[streamId][keyspaceId] = flushTs

	var stopCh StopChannel
	var doneCh DoneChannel

	monitor_ts := tk.config["timekeeper.monitor_flush"].Bool()

	if monitor_ts {
		stopCh = tk.ss.streamKeyspaceIdTimerStopCh[streamId][keyspaceId]
		doneCh = make(DoneChannel)
		tk.ss.streamKeyspaceIdFlushDone[streamId][keyspaceId] = doneCh
	}

	hasAllSB := false
	needsReset := tk.ss.streamKeyspaceIdNeedsLastRollbackReset[streamId][keyspaceId]
	vbmap := tk.ss.streamKeyspaceIdVBMap[streamId][keyspaceId]
	//if all stream begins have been seen atleast once after stream start
	if needsReset && len(vbmap) == len(flushTs.Vbuuids) {
		hasAllSB = true
		tk.ss.streamKeyspaceIdNeedsLastRollbackReset[streamId][keyspaceId] = false
	}

	go func() {
		tk.supvRespch <- &MsgTKStabilityTS{ts: flushTs,
			keyspaceId: keyspaceId,
			streamId:   streamId,
			changeVec:  changeVec,
			hasAllSB:   hasAllSB,
			countVec:   countVec,
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
					flushInProgress := tk.ss.streamKeyspaceIdFlushInProgressTsMap[streamId][keyspaceId]
					if flushTs.Equal(flushInProgress) {
						totalWait += 10

						if totalWait > 60 {
							lastFlushedTs := tk.ss.streamKeyspaceIdLastFlushedTsMap[streamId][keyspaceId]
							hwt := tk.ss.streamKeyspaceIdHWTMap[streamId][keyspaceId]
							logging.Warnf("Timekeeper::flushMonitor Waiting for flush "+
								"to finish for %v seconds. Stream %v KeyspaceId %v.", totalWait, streamId, keyspaceId)
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
func (tk *timekeeper) setSnapshotType(streamId common.StreamId, keyspaceId string,
	flushTs *common.TsVbuuid) {

	lastPersistTime := tk.ss.streamKeyspaceIdLastPersistTime[streamId][keyspaceId]

	//for init build, if there is no snapshot option set
	if tk.hasInitStateIndexNoCatchup(streamId, keyspaceId) {
		if flushTs.GetSnapType() == common.NO_SNAP {
			isMergeCandidate := false

			//if this TS is a merge candidate, generate in-mem snapshot
			if tk.checkMergeCandidateTs(streamId, keyspaceId, flushTs) {
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
				tk.ss.streamKeyspaceIdLastPersistTime[streamId][keyspaceId] = time.Now()
			}
		} else if flushTs.GetSnapType() == common.NO_SNAP_OSO {
			// if storage type is MOI, generate snapshot during initial build.
			if common.GetStorageMode() == common.MOI {
				flushTs.SetSnapType(common.INMEM_SNAP_OSO)
			}

			snapPersistInterval := tk.getPersistIntervalInitBuild()
			persistDuration := time.Duration(snapPersistInterval) * time.Millisecond
			//create disk snapshot based on wall clock time
			if time.Since(lastPersistTime) > persistDuration {
				if flushTs.HasOpenOSOSnap() {
					flushTs.SetSnapType(common.DISK_SNAP_OSO)
				} else {
					//if there is no open OSO snapshot, it is eligible for recovery
					flushTs.SetSnapType(common.DISK_SNAP)
				}
				tk.ss.streamKeyspaceIdLastPersistTime[streamId][keyspaceId] = time.Now()
			}
		}
	} else if flushTs.IsSnapAligned() {
		//for incremental build, snapshot only if ts is snap aligned
		//set either in-mem or persist snapshot based on wall clock time
		snapPersistInterval := tk.getPersistInterval()
		persistDuration := time.Duration(snapPersistInterval) * time.Millisecond

		if time.Since(lastPersistTime) > persistDuration {
			flushTs.SetSnapType(common.DISK_SNAP)
			tk.ss.streamKeyspaceIdLastPersistTime[streamId][keyspaceId] = time.Now()
			tk.ss.streamKeyspaceIdSkippedInMemTs[streamId][keyspaceId] = 0
		} else {
			fastFlush := tk.config["settings.fast_flush_mode"].Bool()
			hasInMemSnap := tk.ss.streamKeyspaceIdHasInMemSnap[streamId][keyspaceId]

			if fastFlush && hasInMemSnap {
				//if fast flush mode is enabled, skip in-mem snapshots based
				//on number of pending ts to be processed.
				//skip in-mem snapshots only if there is atleast one in-mem snapshot
				//merge of a partitioned index expects a storage snapshot to be available
				skipFactor := tk.calcSkipFactorForFastFlush(streamId, keyspaceId)
				if skipFactor != 0 && (tk.ss.streamKeyspaceIdSkippedInMemTs[streamId][keyspaceId] < skipFactor) {
					tk.ss.streamKeyspaceIdSkippedInMemTs[streamId][keyspaceId]++
					flushTs.SetSnapType(common.NO_SNAP)
				} else {
					flushTs.SetSnapType(common.INMEM_SNAP)
					tk.ss.streamKeyspaceIdSkippedInMemTs[streamId][keyspaceId] = 0
				}
			} else {
				flushTs.SetSnapType(common.INMEM_SNAP)
				tk.ss.streamKeyspaceIdSkippedInMemTs[streamId][keyspaceId] = 0
				tk.ss.streamKeyspaceIdHasInMemSnap[streamId][keyspaceId] = true
			}
		}
	}

	if !flushTs.IsSnapAligned() {
		keyspaceStats := tk.stats.GetKeyspaceStats(streamId, keyspaceId)
		if keyspaceStats != nil {
			keyspaceStats.numNonAlignTS.Add(1)
		}
	}

}

//checkMergeCandidateTs check if a TS is a candidate for merge with
//MAINT_STREAM
func (tk *timekeeper) checkMergeCandidateTs(streamId common.StreamId,
	keyspaceId string, flushTs *common.TsVbuuid) bool {

	//if stream is not INIT_STREAM, merge is not required
	if streamId != common.INIT_STREAM {
		return false
	}

	//if flushTs is not on snap boundary, it is not merge candidate
	if !flushTs.IsSnapAligned() {
		return false
	}

	//merge candidate cannot be evaluated by comparing
	//the seqno of collection vs bucket. It is better to generate in-mem
	//snapshot for all snap aligned timestamps.

	return true
}

//mayBeMakeSnapAligned makes a Ts snap aligned if all seqnos
//have been received till Snapshot End and the difference is not
//greater than largeSnapThreshold
func (tk *timekeeper) mayBeMakeSnapAligned(streamId common.StreamId,
	keyspaceId string, flushTs *common.TsVbuuid) {

	if tk.indexerState != common.INDEXER_ACTIVE {
		return
	}

	if tk.hasInitStateIndexNoCatchup(streamId, keyspaceId) {
		return
	}

	if flushTs.HasDisableAlign() {
		return
	}

	hwt := tk.ss.streamKeyspaceIdHWTMap[streamId][keyspaceId]

	largeSnap := tk.ss.config["settings.largeSnapshotThreshold"].Uint64()

	for i, s := range flushTs.Snapshots {

		//if diff between snapEnd and seqno is with largeSnap limit
		//and all mutations have been received till snapEnd
		if s[1]-flushTs.Seqnos[i] < largeSnap &&
			hwt.Seqnos[i] >= s[1] {

			if flushTs.Seqnos[i] != s[1] {
				logging.Debugf("Timekeeper::mayBeMakeSnapAligned.  Align Seqno to Snap End for large snapshot. "+
					"KeyspaceId %v StreamId %v vbucket %v Snapshot %v-%v old Seqno %v new Seqno %v Vbuuid %v current HWT seqno %v",
					keyspaceId, streamId, i, flushTs.Snapshots[i][0], flushTs.Snapshots[i][1], flushTs.Seqnos[i], s[1],
					flushTs.Vbuuids[i], hwt.Seqnos[i])
			}

			flushTs.Seqnos[i] = s[1]
		}
	}

	if flushTs.CheckSnapAligned() {
		flushTs.SetSnapAligned(true)
	}

}

func (tk *timekeeper) ensureMonotonicTs(streamId common.StreamId, keyspaceId string,
	tsElem *TsListElem) {

	flushTs := tsElem.ts

	// Seqno should be monotonically increasing when it comes to mutation queue.
	// For pre-caution, if we detect a flushTS that is smaller than LastFlushTS,
	// we should make it align with lastFlushTS to make sure indexer does not hang
	// because it may be waiting for a seqno that never exist in mutation queue.
	if lts, ok := tk.ss.streamKeyspaceIdLastFlushedTsMap[streamId][keyspaceId]; ok && lts != nil {

		var sumDcpSnapSize uint64
		for i, s := range flushTs.Seqnos {

			enableOSO := tk.ss.streamKeyspaceIdEnableOSO[streamId][keyspaceId]
			//oso can be non-monotonic
			if enableOSO &&
				flushTs.OSOCount != nil &&
				flushTs.OSOCount[i] != 0 {
				continue
			}

			//if flushTs has a smaller seqno than lastFlushTs
			if s < lts.Seqnos[i] {

				needsLog := true
				//if disableAlign is set for the TS, it is possible that lastFlushTs has flushed upto
				//snapEnd but the current TS seqno is smaller as snap alignment was disabled.
				//skip logging this information as this can flood the logs when partial snapshots
				//are getting processed.
				if flushTs.HasDisableAlign() &&
					flushTs.Snapshots[i][0] == lts.Snapshots[i][0] && //same snapshot start/end
					flushTs.Snapshots[i][1] == lts.Snapshots[i][1] {
					needsLog = false
				}

				if needsLog {
					logging.Infof("Timekeeper::ensureMonotonicTs  Align seqno smaller than lastFlushTs. "+
						"KeyspaceId %v StreamId %v vbucket %v. CurrentTS: Snapshot %v-%v Seqno %v Vbuuid %v. "+
						"LastFlushTS: Snapshot %v-%v Seqno %v Vbuuid %v.",
						keyspaceId, streamId, i,
						flushTs.Snapshots[i][0], flushTs.Snapshots[i][1], flushTs.Seqnos[i], flushTs.Vbuuids[i],
						lts.Snapshots[i][0], lts.Snapshots[i][1], lts.Seqnos[i], lts.Vbuuids[i])
				}

				flushTs.Seqnos[i] = lts.Seqnos[i]
				flushTs.Vbuuids[i] = lts.Vbuuids[i]
				flushTs.Snapshots[i][0] = lts.Snapshots[i][0]
				flushTs.Snapshots[i][1] = lts.Snapshots[i][1]
			}

			sumDcpSnapSize += flushTs.Snapshots[i][1] - flushTs.Snapshots[i][0]
		}

		//avgDcpSnapSize is a point in time stat computed for each flushTs per stream/keyspace
		//average is computed by sum(snapEnd - snapStart)/numVbuckets
		keyspaceStats := tk.stats.GetKeyspaceStats(streamId, keyspaceId)
		if keyspaceStats != nil {
			numVb := uint64(len(flushTs.Seqnos))
			keyspaceStats.avgDcpSnapSize.Set(sumDcpSnapSize / numVb)
		}
	}

}

//splits a Ts if current HWT is less than Snapshot End for the vbucket.
//It is important to send TS to flusher only upto the HWT as that's the
//only guaranteed seqno that can be flushed.
func (tk *timekeeper) maybeSplitTs(ts *common.TsVbuuid, keyspaceId string,
	streamId common.StreamId) *common.TsVbuuid {

	hwt := tk.ss.streamKeyspaceIdHWTMap[streamId][keyspaceId]

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
		tsList := tk.ss.streamKeyspaceIdTsListMap[streamId][keyspaceId]
		tsList.PushFront(&TsListElem{ts: newTs})
		return newTs
	} else {
		return ts
	}
}

//changeIndexStateForKeyspaceId changes the state of all indexes in the given keyspaceId
//to the one provided
func (tk *timekeeper) changeIndexStateForKeyspaceId(keyspaceId string, state common.IndexState) {

	//for all indexes in this keyspaceId, change the state
	for _, bi := range tk.indexBuildInfo {
		if bi.indexInst.Defn.KeyspaceId(bi.indexInst.Stream) == keyspaceId {
			logging.Infof("Timekeeper::changeIndexStateForKeyspaceId %v %v %v %v", bi.indexInst.Stream,
				keyspaceId, bi.indexInst.InstId, state)
			bi.indexInst.State = state
		}
	}

}

func (tk *timekeeper) setAddInstPending(streamId common.StreamId, keyspaceId string, val bool) {

	if streamId != common.INIT_STREAM {
		return
	}

	for _, buildInfo := range tk.indexBuildInfo {
		idx := buildInfo.indexInst
		if idx.Defn.KeyspaceId(idx.Stream) == keyspaceId &&
			idx.Stream == streamId &&
			idx.State == common.INDEX_STATE_CATCHUP {
			logging.Infof("Timekeeper::setAddInstPending %v %v %v %v", streamId, keyspaceId,
				idx.InstId, val)
			buildInfo.addInstPending = val
		}
	}
}

//check if any index for the given keyspaceId is in initial state
func (tk *timekeeper) checkAnyInitialStateIndex(keyspaceId string) bool {

	for _, buildInfo := range tk.indexBuildInfo {
		//if index belongs to the flushed keyspaceId and in INITIAL state
		idx := buildInfo.indexInst
		if idx.Defn.KeyspaceId(idx.Stream) == keyspaceId &&
			idx.State == common.INDEX_STATE_INITIAL {
			return true
		}
	}

	return false

}

//checkKeyspaceActiveInStream checks if the given keyspaceId has Active status
//in stream
func (tk *timekeeper) checkKeyspaceActiveInStream(streamId common.StreamId,
	keyspaceId string) bool {

	if keyspaceIdStatus, ok := tk.ss.streamKeyspaceIdStatus[streamId]; !ok {
		logging.Tracef("Timekeeper::checkKeyspaceActiveInStream "+
			"Unknown Stream: %v", streamId)
		tk.supvCmdch <- &MsgError{
			err: Error{code: ERROR_TK_UNKNOWN_STREAM,
				severity: NORMAL,
				category: TIMEKEEPER}}
		return false
	} else if status, ok := keyspaceIdStatus[keyspaceId]; !ok || status != STREAM_ACTIVE {
		logging.Tracef("Timekeeper::checkKeyspaceActiveInStream "+
			"Unknown KeyspaceId %v In Stream %v", keyspaceId, streamId)
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
		ts[i] = s[1] //high seq num in snapshot marker
	}
	return ts
}

//helper function to extract Seqnum Timestamp from TsVbuuid
func getSeqTsFromTsVbuuid(tsVbuuid *common.TsVbuuid) Timestamp {
	numVbuckets := len(tsVbuuid.Snapshots)
	ts := NewTimestamp(numVbuckets)
	for i, s := range tsVbuuid.Seqnos {
		ts[i] = s
	}
	return ts
}

func (tk *timekeeper) initiateRecovery(streamId common.StreamId,
	keyspaceId string) {

	logging.Debugf("Timekeeper::initiateRecovery Started")

	if tk.ss.streamKeyspaceIdStatus[streamId][keyspaceId] == STREAM_PREPARE_DONE {

		var retryTs *common.TsVbuuid
		restartTs := tk.ss.computeRestartTs(streamId, keyspaceId)

		//adjust for non-snap aligned ts
		if restartTs != nil {
			tk.ss.adjustNonSnapAlignedVbs(restartTs, streamId, keyspaceId, nil, false)
			retryTs = tk.ss.computeRetryTs(streamId, keyspaceId, restartTs)
		}

		sessionId := tk.ss.getSessionId(streamId, keyspaceId)

		tk.stopTimer(streamId, keyspaceId)
		tk.ss.cleanupKeyspaceIdFromStream(streamId, keyspaceId)

		//send message for recovery
		tk.supvRespch <- &MsgRecovery{mType: INDEXER_INITIATE_RECOVERY,
			streamId:   streamId,
			keyspaceId: keyspaceId,
			restartTs:  restartTs,
			retryTs:    retryTs,
			sessionId:  sessionId}
		logging.Infof("Timekeeper::initiateRecovery StreamId %v KeyspaceId %v "+
			"SessionId %v RestartTs %v", streamId, keyspaceId, sessionId, restartTs)
	} else {
		logging.Errorf("Timekeeper::initiateRecovery Invalid State For %v "+
			"Stream Detected. State %v", streamId,
			tk.ss.streamKeyspaceIdStatus[streamId][keyspaceId])
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

func (tk *timekeeper) checkKeyspaceReadyForRecovery(streamId common.StreamId,
	keyspaceId string) bool {

	logging.Infof("Timekeeper::checkKeyspaceReadyForRecovery StreamId %v "+
		"KeyspaceId %v", streamId, keyspaceId)

	if tk.ss.checkAnyFlushPending(streamId, keyspaceId) ||
		tk.ss.checkAnyAbortPending(streamId, keyspaceId) ||
		tk.ss.streamKeyspaceIdStatus[streamId][keyspaceId] != STREAM_PREPARE_DONE {
		return false
	}

	logging.Infof("Timekeeper::checkKeyspaceReadyForRecovery StreamId %v "+
		"KeyspaceId %v Ready for Recovery", streamId, keyspaceId)
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
	keyspaceId string) {

	//wait for REPAIR_BATCH_TIMEOUT to batch more error msgs
	//and send request to KV for a batch
	time.Sleep(REPAIR_BATCH_TIMEOUT * time.Millisecond)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	if status := tk.ss.streamKeyspaceIdStatus[streamId][keyspaceId]; status != STREAM_ACTIVE {

		logging.Infof("Timekeeper::repairStream Found Stream %v KeyspaceId %v In "+
			"State %v. Skipping Repair.", streamId, keyspaceId, status)
		return
	}

	// Start rollback if necessary:
	needsRollback, canRollback := tk.ss.canRollbackNow(streamId, keyspaceId)
	if canRollback {

		sessionId := tk.ss.getSessionId(streamId, keyspaceId)
		if tk.resetStreamIfOSOEnabled(streamId, keyspaceId, sessionId, false) {
			delete(tk.ss.streamKeyspaceIdRepairStopCh[streamId], keyspaceId)
			return

		} else {
			logging.Infof("Timekeeper::repairStream need rollback for %v %v %v. "+
				"Sending Init Prepare.", streamId, keyspaceId, sessionId)

			// Initiate recovery.   It will reset keyspaceId book keeping upon prepare recovery.
			tk.supvRespch <- &MsgRecovery{
				mType:      INDEXER_INIT_PREP_RECOVERY,
				streamId:   streamId,
				keyspaceId: keyspaceId,
				restartTs:  tk.ss.computeRollbackTs(streamId, keyspaceId),
				sessionId:  sessionId,
			}
			delete(tk.ss.streamKeyspaceIdRepairStopCh[streamId], keyspaceId)
			return
		}

	}

	// Start MTR if necessary.  This happens if indexer has not received any new StreamBegin over max wait time, and there
	// are still ConnErr.   tk.lock must be hold without releasing, while calling startMTROnRetry and repairStreamWithMTR.
	if tk.ss.startMTROnRetry(streamId, keyspaceId) {

		logging.Infof("Timekeeper::repairStream need MTR for %v %v. Sending StreamRepair.", streamId, keyspaceId)

		sessionId := tk.ss.getSessionId(streamId, keyspaceId)

		if tk.resetStreamIfOSOEnabled(streamId, keyspaceId, sessionId, false) {
			delete(tk.ss.streamKeyspaceIdRepairStopCh[streamId], keyspaceId)
			return
		} else {
			resp := &MsgKVStreamRepair{
				streamId:   streamId,
				keyspaceId: keyspaceId,
				sessionId:  sessionId,
			}

			tk.repairStreamWithMTR(streamId, keyspaceId, resp)
			return
		}
	}

	//prepare repairTs with all vbs in STREAM_END, REPAIR status and
	//send that to KVSender to repair
	if repairTs, needRepair, connErrVbs, repairVbs := tk.ss.getRepairTsForKeyspaceId(streamId, keyspaceId); needRepair {

		sessionId := tk.ss.getSessionId(streamId, keyspaceId)

		if tk.resetStreamIfOSOEnabled(streamId, keyspaceId, sessionId, false) {
			delete(tk.ss.streamKeyspaceIdRepairStopCh[streamId], keyspaceId)
			return
		} else {
			respCh := make(MsgChannel)
			stopCh := tk.ss.streamKeyspaceIdRepairStopCh[streamId][keyspaceId]
			cid := tk.ss.streamKeyspaceIdCollectionId[streamId][keyspaceId]

			restartMsg := &MsgRestartVbuckets{streamId: streamId,
				keyspaceId:   keyspaceId,
				restartTs:    repairTs,
				respCh:       respCh,
				stopCh:       stopCh,
				connErrVbs:   connErrVbs,
				repairVbs:    repairVbs,
				sessionId:    sessionId,
				collectionId: cid}

			go tk.sendRestartMsg(restartMsg)
		}

	} else if needsRollback {
		go tk.repairStream(streamId, keyspaceId)

	} else {
		delete(tk.ss.streamKeyspaceIdRepairStopCh[streamId], keyspaceId)
		logging.Infof("Timekeeper::repairStream Nothing to repair for "+
			"Stream %v and KeyspaceId %v", streamId, keyspaceId)

		//process any merge that was missed due to stream repair
		tk.checkPendingStreamMerge(streamId, keyspaceId)
	}

}

func (tk *timekeeper) sendRestartMsg(restartMsg Message) {

	tk.supvRespch <- restartMsg

	//wait on respCh
	kvresp := <-restartMsg.(*MsgRestartVbuckets).GetResponseCh()

	streamId := restartMsg.(*MsgRestartVbuckets).GetStreamId()
	keyspaceId := restartMsg.(*MsgRestartVbuckets).GetKeyspaceId()
	repairVbs := restartMsg.(*MsgRestartVbuckets).RepairVbs()
	shutdownVbs := restartMsg.(*MsgRestartVbuckets).ConnErrVbs()

	//if timekeeper has moved to prepare unpause state, ignore
	//the response message as all streams are going to be
	//restarted anyways
	if tk.checkIndexerState(common.INDEXER_PREPARE_UNPAUSE) {
		tk.lock.Lock()
		delete(tk.ss.streamKeyspaceIdRepairStopCh[streamId], keyspaceId)
		tk.lock.Unlock()
		return
	}

	validateStreamStatusAndSessionId := func(streamId common.StreamId,
		keyspaceId string, sessionId uint64, logMsg string) bool {

		status := tk.ss.streamKeyspaceIdStatus[streamId][keyspaceId]

		//response can be skipped for inactive stream or under recovery stream(stream
		//will be restarted as part of recovery)
		if status != STREAM_ACTIVE {
			logging.Infof("Timekeeper::sendRestartMsg Found Stream %v KeyspaceId %v In "+
				"State %v. Skipping %v.", streamId, keyspaceId, status, logMsg)
			return false
		}

		currSessionId := tk.ss.getSessionId(streamId, keyspaceId)
		if sessionId != currSessionId {
			logging.Infof("Timekeeper::sendRestartMsg Stream %v KeyspaceId %v Curr Session "+
				"%v. Skipping %v Session %v.", streamId, keyspaceId,
				currSessionId, logMsg, sessionId)
			return false
		}
		return true
	}

	switch kvresp.GetMsgType() {

	case REPAIR_ABORT:
		//nothing to do
		logging.Infof("Timekeeper::sendRestartMsg Repair Aborted %v %v", streamId, keyspaceId)

	case KV_SENDER_RESTART_VBUCKETS_RESPONSE:

		needsRollback := false

		restartRespMsg := kvresp.(*MsgRestartVbucketsResponse)
		activeTs := restartRespMsg.GetActiveTs()
		pendingTs := restartRespMsg.GetPendingTs()
		sessionId := restartRespMsg.GetSessionId()

		abort := func() bool {
			tk.lock.RLock()
			defer tk.lock.RUnlock()

			if !validateStreamStatusAndSessionId(streamId, keyspaceId,
				sessionId, "Restart Vbuckets Response") {
				return true
			} else {
				needsRollback = tk.ss.needsRollback(streamId, keyspaceId)
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

			if !validateStreamStatusAndSessionId(streamId, keyspaceId, sessionId, "Rollback") {
				return true
			}

			tk.ss.streamKeyspaceIdKVActiveTsMap[streamId][keyspaceId] = activeTs
			tk.ss.streamKeyspaceIdKVPendingTsMap[streamId][keyspaceId] = pendingTs
			tk.ss.updateRepairState(streamId, keyspaceId, repairVbs, shutdownVbs)
			return false

		}()

		if abort {
			return
		}

		//check for more vbuckets in repair state
		tk.repairStream(streamId, keyspaceId)

	case INDEXER_ROLLBACK:

		contRepair := false
		func() {

			tk.lock.Lock()
			defer tk.lock.Unlock()

			sessionId := kvresp.(*MsgRollback).GetSessionId()

			if !validateStreamStatusAndSessionId(streamId, keyspaceId, sessionId, "Rollback") {
				return
			}

			//if rollback msg, call initPrepareRecovery
			logging.Infof("Timekeeper::sendRestartMsg Received Rollback Msg For "+
				"%v %v. Update RollbackTs.", streamId, keyspaceId)

			currSessionId := tk.ss.getSessionId(streamId, keyspaceId)

			// There is rollback while repairing stream.  RollbackTs could be coming from
			// 1) Unrelated to the vb under repair
			// 2) Due to vb under repair
			// In either case, indexer expects getting StreamBegin even if vb is rolled back.  This will
			// set the Vb back to STREAM_BEGIN status.  Once all vb are in STREAM_BEGIN status, repairStream
			// will trigger recovery.
			rollbackTs := kvresp.(*MsgRollback).GetRollbackTs()
			if rollbackTs != nil && rollbackTs.Len() > 0 {

				ts := rollbackTs.Union(tk.ss.streamKeyspaceIdKVRollbackTsMap[streamId][keyspaceId])
				if ts != nil {
					tk.ss.streamKeyspaceIdKVRollbackTsMap[streamId][keyspaceId] = ts
				}

				if ts == nil {
					logging.Errorf("Timekeeper::sendRestartMsg Received Rollback Msg For "+
						"%v %v %v. Fail to merge rollbackTs in timekeeper. Send InitPrepRecovery "+
						"right away. RollbackTs %v", streamId, keyspaceId, sessionId, rollbackTs)

					tk.supvRespch <- &MsgRecovery{
						mType:      INDEXER_INIT_PREP_RECOVERY,
						streamId:   streamId,
						keyspaceId: keyspaceId,
						restartTs:  tk.ss.computeRollbackTs(streamId, keyspaceId),
						sessionId:  currSessionId,
					}
					delete(tk.ss.streamKeyspaceIdRepairStopCh[streamId], keyspaceId)
					return
				}
				// update repair state even if there is rollback
				tk.ss.updateRepairState(streamId, keyspaceId, repairVbs, shutdownVbs)
				contRepair = true

			} else {
				logging.Errorf("Timekeeper::sendRestartMsg Received Rollback Msg For "+
					"%v %v %v. RollbackTs is empty.  Send InitPrepRecovery right away.",
					streamId, keyspaceId, sessionId)

				tk.supvRespch <- &MsgRecovery{
					mType:      INDEXER_INIT_PREP_RECOVERY,
					streamId:   streamId,
					keyspaceId: keyspaceId,
					restartTs:  tk.ss.computeRollbackTs(streamId, keyspaceId),
					sessionId:  currSessionId,
				}

				delete(tk.ss.streamKeyspaceIdRepairStopCh[streamId], keyspaceId)
			}
		}()

		if contRepair {
			tk.repairStream(streamId, keyspaceId)
		}

	case KV_STREAM_REPAIR:

		// KV Sender has requested a MTR.  Do not clear vb status since
		// this will make timekeeper diverge from projector.   vb status
		// should only be reset on recovery or vb shutdown.

		tk.lock.Lock()
		defer tk.lock.Unlock()

		sessionId := kvresp.(*MsgKVStreamRepair).GetSessionId()

		if !validateStreamStatusAndSessionId(streamId, keyspaceId, sessionId, "Stream Repair") {
			return
		}

		currSessionId := tk.ss.getSessionId(streamId, keyspaceId)

		// If we need recovery, then trigger recovery right away.
		if tk.ss.needsRollback(streamId, keyspaceId) {

			logging.Infof("Timekeeper::sendRestartMsg Received KV Repair Msg For "+
				"Stream %v KeyspaceId %v SessionId %v. Attempting Rollback.",
				streamId, keyspaceId, sessionId)

			tk.supvRespch <- &MsgRecovery{
				mType:      INDEXER_INIT_PREP_RECOVERY,
				streamId:   streamId,
				keyspaceId: keyspaceId,
				restartTs:  tk.ss.computeRollbackTs(streamId, keyspaceId),
				sessionId:  currSessionId,
			}

			delete(tk.ss.streamKeyspaceIdRepairStopCh[streamId], keyspaceId)
			return
		}

		logging.Infof("Timekeeper::sendRestartMsg Received KV Repair Msg For "+
			"Stream %v KeyspaceId %v. Attempting Stream Repair.", streamId, keyspaceId)

		repairMsg := kvresp.(*MsgKVStreamRepair)
		repairMsg.sessionId = currSessionId

		tk.repairStreamWithMTR(streamId, keyspaceId, repairMsg)

	default: //MSG_ERROR

		sessionId := kvresp.(*MsgError).GetSessionId()

		abort := func() bool {
			tk.lock.RLock()
			defer tk.lock.RUnlock()

			if !validateStreamStatusAndSessionId(streamId, keyspaceId, sessionId, "Error") {
				return true
			}
			return false
		}()

		if abort {
			return
		}

		indexInstMap := tk.indexInstMap.Get()
		var bucketUUIDList []string
		for _, indexInst := range indexInstMap {
			if indexInst.Defn.KeyspaceId(indexInst.Stream) == keyspaceId && indexInst.Stream == streamId &&
				indexInst.State != common.INDEX_STATE_DELETED {
				bucketUUIDList = append(bucketUUIDList, indexInst.Defn.BucketUUID)
			}
		}

		bucket, _, _ := SplitKeyspaceId(keyspaceId)
		if !tk.ValidateKeyspace(streamId, keyspaceId, bucketUUIDList) {
			logging.Errorf("Timekeeper::sendRestartMsg Keyspace Not Found "+
				"For Stream %v KeyspaceId %v Bucket %v", streamId, keyspaceId, bucket)

			tk.lock.Lock()
			defer tk.lock.Unlock()

			if !validateStreamStatusAndSessionId(streamId, keyspaceId, sessionId, "Keyspace Not Found") {
				return
			}

			currSessionId := tk.ss.getSessionId(streamId, keyspaceId)
			delete(tk.ss.streamKeyspaceIdRepairStopCh[streamId], keyspaceId)
			tk.ss.streamKeyspaceIdStatus[streamId][keyspaceId] = STREAM_INACTIVE

			tk.supvRespch <- &MsgRecovery{mType: INDEXER_KEYSPACE_NOT_FOUND,
				streamId:   streamId,
				keyspaceId: keyspaceId,
				sessionId:  currSessionId}
		} else {
			logging.Errorf("Timekeeper::sendRestartMsg Error Response "+
				"from KV %v For Request %v. Retrying RestartVbucket.", kvresp, restartMsg)

			tk.repairStream(streamId, keyspaceId)
		}
	}

}

func (tk *timekeeper) repairStreamWithMTR(streamId common.StreamId, keyspaceId string, resp *MsgKVStreamRepair) {

	//for stream repair, use the HWT. If there has been a rollback, it will
	//be detected as response of the MTR. Without rollback, even if a vbucket
	//has moved to a new node, using HWT is sufficient.
	resp.restartTs = tk.ss.streamKeyspaceIdHWTMap[streamId][keyspaceId].Copy()
	resp.async = tk.ss.streamKeyspaceIdAsyncMap[streamId][keyspaceId]

	//adjust for non-snap aligned ts
	tk.ss.adjustNonSnapAlignedVbs(resp.restartTs, streamId, keyspaceId, nil, false)
	tk.ss.adjustVbuuids(resp.restartTs, streamId, keyspaceId)

	//reset timer for open stream
	tk.ss.streamKeyspaceIdOpenTsMap[streamId][keyspaceId] = nil
	tk.ss.streamKeyspaceIdStartTimeMap[streamId][keyspaceId] = uint64(0)

	// stop repair
	delete(tk.ss.streamKeyspaceIdRepairStopCh[streamId], keyspaceId)

	// Update repair state of each vb now, even though MTR has not completed yet, since
	// repairStream will terminate after this function.
	for i := range tk.ss.streamKeyspaceIdRepairStateMap[streamId][keyspaceId] {
		if tk.ss.streamKeyspaceIdRepairStateMap[streamId][keyspaceId][i] == REPAIR_SHUTDOWN_VB {
			logging.Infof("timekeeper::repairStreamWithMTR - set repair state to REPAIR_MTR for %v keyspaceId %v vb %v", streamId, keyspaceId, i)
			tk.ss.streamKeyspaceIdRepairStateMap[streamId][keyspaceId][i] = REPAIR_MTR
			tk.ss.setLastRepairTime(streamId, keyspaceId, Vbucket(i))
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

	tk.stats.Set(req.GetStatsObject())
	tk.indexInstMap.Set(common.CopyIndexInstMap(indexInstMap))
	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleUpdateIndexPartnMap(cmd Message) {
	tk.lock.Lock()
	defer tk.lock.Unlock()

	logging.Tracef("Timekeeper::handleUpdateIndexPartnMap %v", cmd)
	indexPartnMap := cmd.(*MsgUpdatePartnMap).GetIndexPartnMap()
	tk.indexPartnMap.Set(CopyIndexPartnMap(indexPartnMap))
	tk.supvCmdch <- &MsgSuccess{}
}

// handleUpdateKeyspaceStatsMap atomically swaps in the pointer to a new KeyspaceStatsMap.
func (tk *timekeeper) handleUpdateKeyspaceStatsMap(cmd Message) {
	logging.Tracef("Timekeeper::handleUpdateKeyspaceStatsMap %v", cmd)
	req := cmd.(*MsgUpdateKeyspaceStatsMap)
	stats := tk.stats.Get()
	if stats != nil {
		stats.keyspaceStatsMap.Set(req.GetStatsObject())
	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleStats(cmd Message) {
	tk.supvCmdch <- &MsgSuccess{}

	req := cmd.(*MsgStatsRequest)
	replych := req.GetReplyChannel()

	tk.lock.Lock()
	keyspaceIdCollectionId := tk.ss.CloneCollectionIdMap(common.INIT_STREAM)
	tk.lock.Unlock()

	indexInstMap := tk.indexInstMap.Get()

	go func() {

		if !req.FetchDcp() {
			tk.updateTimestampStats()
			replych <- true
			return
		}

		tk.statsLock.Lock()
		defer tk.statsLock.Unlock()
		// Populate current KV timestamps for all keyspaces
		keyspaceIdTsMap := make(map[common.StreamId]map[string]Timestamp)
		for _, inst := range indexInstMap {
			//skip deleted indexes
			if inst.State == common.INDEX_STATE_DELETED {
				continue
			}

			var kvTs Timestamp
			var err error

			keyspaceId := inst.Defn.KeyspaceId(inst.Stream)
			stream := inst.Stream
			if _, ok := keyspaceIdTsMap[stream]; !ok {
				keyspaceIdTsMap[stream] = make(map[string]Timestamp)
			}
			if _, ok := keyspaceIdTsMap[stream][keyspaceId]; !ok {
				rh := common.NewRetryHelper(maxStatsRetries, time.Second, 1, func(a int, err error) error {
					cluster := tk.config["clusterAddr"].String()
					numVbuckets := tk.config["numVbuckets"].Int()
					cid := ""
					if inst.Stream == common.INIT_STREAM && inst.Defn.KeyspaceId(inst.Stream) != inst.Defn.Bucket {
						cid = keyspaceIdCollectionId[keyspaceId]
					}
					kvTs, err = GetCurrentKVTs(cluster, "default", keyspaceId, cid, numVbuckets)
					return err
				})
				if err = rh.Run(); err != nil {
					logging.Errorf("Timekeeper::handleStats Error occured while obtaining KV seqnos - %v", err)
					replych <- true
					return
				}

				keyspaceIdTsMap[stream][keyspaceId] = kvTs
			}
		}

		progressStatTime := time.Now().UnixNano()
		var rollbackTimeMap map[string]int64
		flushedCountMap := make(map[common.StreamId]map[string]uint64)
		queuedMap := make(map[common.StreamId]map[string]uint64)
		pendingMap := make(map[common.StreamId]map[string]uint64)
		totalTobeFlushedMap := make(map[common.StreamId]map[string]uint64)

		func() {
			tk.lock.Lock()
			defer tk.lock.Unlock()
			flushedTsMap := tk.ss.streamKeyspaceIdLastFlushedTsMap
			receivedTsMap := tk.ss.streamKeyspaceIdHWTMap
			rollbackTimeMap = tk.ss.CloneKeyspaceIdRollbackTime()

			// Pre-compute flushedCount for all streams and keyspaceId's
			for stream, keyspaceIdMap := range flushedTsMap {
				if _, ok := flushedCountMap[stream]; !ok {
					flushedCountMap[stream] = make(map[string]uint64)
				}

				for keyspaceId, flushedTs := range keyspaceIdMap {
					flushedCount := uint64(0)
					if flushedTs != nil {
						for _, seqno := range flushedTs.Seqnos {
							flushedCount += seqno
						}
					}
					flushedCountMap[stream][keyspaceId] = flushedCount
				}
			}

			// Pre-compute queuedCount for all streams and keyspaceId's
			for stream, keyspaceIdMap := range receivedTsMap {
				if _, ok := queuedMap[stream]; !ok {
					queuedMap[stream] = make(map[string]uint64)
				}

				for keyspaceId, receivedTs := range keyspaceIdMap {
					queued := uint64(0)
					if receivedTs != nil {
						for _, seqno := range receivedTs.Seqnos {
							queued += seqno
						}
					}

					flushedCount := flushedCountMap[stream][keyspaceId]
					queuedMap[stream][keyspaceId] = queued - flushedCount
				}
			}

			// Pre-compute pending count for all streams and keyspaceIds
			for stream, keyspaceIdMap := range keyspaceIdTsMap {
				if _, ok := pendingMap[stream]; !ok {
					pendingMap[stream] = make(map[string]uint64)
					totalTobeFlushedMap[stream] = make(map[string]uint64)
				}

				for keyspaceId, kvTs := range keyspaceIdMap {
					pending := uint64(0)
					sum := uint64(0)
					if kvTs != nil {

						// Get receivedTs for this keyspaceId
						receivedTs := receivedTsMap[stream][keyspaceId]

						for i, seqno := range kvTs {
							sum += seqno
							receivedSeqno := uint64(0)
							if receivedTs != nil {
								receivedSeqno = receivedTs.Seqnos[i]
							}
							// By the time we compute index stats, kv timestamp would have
							// become old.
							if seqno > receivedSeqno {
								pending += seqno - receivedSeqno
							}
						}
					}
					pendingMap[stream][keyspaceId] = pending
					totalTobeFlushedMap[stream][keyspaceId] = sum
				}
			}
		}()

		stats := tk.stats.Get()
		for instId, inst := range indexInstMap {
			//skip deleted indexes
			if inst.State == common.INDEX_STATE_DELETED {
				continue
			}

			stream := inst.Stream
			keyspaceId := inst.Defn.KeyspaceId(inst.Stream)
			idxStats := stats.indexes[instId]
			v := float64(0)
			switch inst.State {
			default:
				v = 0.00
			case common.INDEX_STATE_ACTIVE:
				v = 100.00
			case common.INDEX_STATE_INITIAL, common.INDEX_STATE_CATCHUP:
				totalToBeflushed := totalTobeFlushedMap[stream][keyspaceId]
				flushedCount := flushedCountMap[stream][keyspaceId]
				if totalToBeflushed > flushedCount {
					v = float64(flushedCount) * 100.00 / float64(totalToBeflushed)
				} else {
					v = 100.00
				}
			}

			if idxStats != nil {
				idxStats.indexState.Set((uint64)(inst.State))
				idxStats.numDocsProcessed.Set(int64(flushedCountMap[stream][keyspaceId]))
				idxStats.numDocsQueued.Set(int64(queuedMap[stream][keyspaceId]))
				idxStats.numDocsPending.Set(int64(pendingMap[stream][keyspaceId]))
				idxStats.buildProgress.Set(int64(v))
				idxStats.completionProgress.Set(int64(math.Float64bits(v)))
				idxStats.lastRollbackTime.Set(rollbackTimeMap[keyspaceId])
				idxStats.progressStatTime.Set(progressStatTime)
			}
		}

		replych <- true
	}()
}

func (tk *timekeeper) updateTimestampStats() {

	var rollbackTimeMap map[string]int64

	func() {
		tk.lock.Lock()
		defer tk.lock.Unlock()
		rollbackTimeMap = tk.ss.CloneKeyspaceIdRollbackTime()
	}()
	indexInstMap := tk.indexInstMap.Get()

	stats := tk.stats.Get()
	for _, inst := range indexInstMap {
		//skip deleted indexes
		if inst.State == common.INDEX_STATE_DELETED {
			continue
		}

		idxStats := stats.indexes[inst.InstId]
		if idxStats != nil {
			idxStats.lastRollbackTime.Set(rollbackTimeMap[inst.Defn.KeyspaceId(inst.Stream)])
			idxStats.progressStatTime.Set(time.Now().UnixNano())
		}
	}
}

func (tk *timekeeper) isBuildCompletionTs(streamId common.StreamId,
	keyspaceId string, flushTs *common.TsVbuuid) bool {

	if streamId != common.INIT_STREAM {
		return false
	}

	for _, buildInfo := range tk.indexBuildInfo {
		//if index belongs to the flushed keyspaceId and in INITIAL state
		idx := buildInfo.indexInst
		if idx.Defn.KeyspaceId(idx.Stream) == keyspaceId &&
			idx.Stream == streamId &&
			idx.State == common.INDEX_STATE_INITIAL {

			enableOSO := tk.ss.streamKeyspaceIdEnableOSO[streamId][keyspaceId]
			//build is not complete till all OSO Snap Ends have been received
			if enableOSO && flushTs.HasOpenOSOSnap() {
				return false
			}

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
	keyspaceId string) {

	logging.Debugf("Timekeeper::checkPendingStreamMerge Stream: %v KeyspaceId: %v", streamId, keyspaceId)

	checkPendingMerge := func(streamId common.StreamId, keyspaceId string, fetchKVSeq bool) {

		if !tk.ss.checkAnyFlushPending(streamId, keyspaceId) &&
			!tk.ss.checkAnyAbortPending(streamId, keyspaceId) {

			lastFlushedTs := tk.ss.streamKeyspaceIdLastFlushedTsMap[streamId][keyspaceId]
			tk.checkInitStreamReadyToMerge(streamId, keyspaceId, lastFlushedTs, fetchKVSeq)
		}
	}

	//for repair done of MAINT_STREAM, if there is any corresponding INIT_STREAM,
	//check the possibility of merge
	if streamId == common.MAINT_STREAM {

		keyspaceIdStatus := tk.ss.streamKeyspaceIdStatus[common.INIT_STREAM]
		for ks, status := range keyspaceIdStatus {
			bucket, _, _ := SplitKeyspaceId(ks)
			if bucket == keyspaceId && status == STREAM_ACTIVE {
				checkPendingMerge(common.INIT_STREAM, ks, false)
			}
		}
	} else {
		checkPendingMerge(streamId, keyspaceId, false)
	}

}

//startTimer starts a per stream/keyspaceId timer to periodically check and
//generate a new stability timestamp
func (tk *timekeeper) startTimer(streamId common.StreamId,
	keyspaceId string) {

	logging.Infof("Timekeeper::startTimer %v %v", streamId, keyspaceId)

	snapInterval := tk.getInMemSnapInterval()
	ticker := time.NewTicker(time.Millisecond * time.Duration(snapInterval))
	stopCh := tk.ss.streamKeyspaceIdTimerStopCh[streamId][keyspaceId]

	go func() {
		for {
			select {
			case <-ticker.C:
				tk.generateNewStabilityTS(streamId, keyspaceId)

			case <-stopCh:
				ticker.Stop()
				return
			}
		}
	}()

}

//stopTimer stops the stream/keyspaceId timer started by startTimer
func (tk *timekeeper) stopTimer(streamId common.StreamId, keyspaceId string) {

	logging.Infof("Timekeeper::stopTimer %v %v", streamId, keyspaceId)

	stopCh := tk.ss.streamKeyspaceIdTimerStopCh[streamId][keyspaceId]
	if stopCh != nil {
		close(stopCh)
	}

}

func (tk *timekeeper) setBuildTs(streamId common.StreamId, keyspaceId string,
	buildTs Timestamp) {

	for _, buildInfo := range tk.indexBuildInfo {
		//if index belongs to the given keyspaceId/stream and in INITIAL state
		idx := buildInfo.indexInst
		if idx.Defn.KeyspaceId(idx.Stream) == keyspaceId &&
			idx.Stream == streamId &&
			idx.State == common.INDEX_STATE_INITIAL {
			buildInfo.buildTs = buildTs
		}
	}
}

//setMergeTs sets the mergeTs for catchup state indexes in case of recovery.
func (tk *timekeeper) setMergeTs(streamId common.StreamId, keyspaceId string,
	mergeTs *common.TsVbuuid) {

	for _, buildInfo := range tk.indexBuildInfo {
		if buildInfo.indexInst.Defn.KeyspaceId(streamId) == keyspaceId &&
			buildInfo.indexInst.State == common.INDEX_STATE_CATCHUP &&
			buildInfo.addInstPending == false {

			logging.Infof("Timekeeper::setMergeTs %v %v %v", streamId, keyspaceId, buildInfo.indexInst.InstId)

			buildInfo.buildDoneAckReceived = true
			//set minMergeTs. stream merge can only happen at or above this
			//TS as projector guarantees new index definitions have been applied
			//to the MAINT_STREAM only at or above this.
			buildInfo.minMergeTs = mergeTs.Copy()
		}
	}
}

func (tk *timekeeper) resetWaitForRecovery(streamId common.StreamId, keyspaceId string) {

	logging.Infof("Timekeeper::resetWaitForRecovery Stream %v KeyspaceId %v", streamId, keyspaceId)

	for _, buildInfo := range tk.indexBuildInfo {
		idx := buildInfo.indexInst
		if idx.Defn.KeyspaceId(idx.Stream) == keyspaceId &&
			idx.Stream == streamId {
			buildInfo.waitForRecovery = false
		}
	}

}

func (tk *timekeeper) hasInitStateIndex(streamId common.StreamId,
	keyspaceId string) bool {

	//7.0 and above, init build can only happen in init stream
	if streamId == common.INIT_STREAM {
		return true
	} else {
		return false
	}

}

//hasInitStateIndexNoCatchup returns true if the stream/keyspace has
//index in initial build except for catchup phase
func (tk *timekeeper) hasInitStateIndexNoCatchup(streamId common.StreamId,
	keyspaceId string) bool {

	//index build doesn't happen in MAINT_STREAM
	if streamId == common.MAINT_STREAM {
		return false
	}

	for _, buildInfo := range tk.indexBuildInfo {
		//if index belongs to the flushed keyspaceId and in INITIAL state
		idx := buildInfo.indexInst
		if idx.Defn.KeyspaceId(idx.Stream) == keyspaceId &&
			idx.Stream == streamId {
			//all indexes in a stream/keyspace have the same state
			//first qualfying check is sufficient to determine the answer
			if idx.State == common.INDEX_STATE_CATCHUP {
				return false
			} else if idx.State == common.INDEX_STATE_INITIAL {
				return true
			}
		}
	}
	return false
}

//calc skip factor for in-mem snapshots based on the
//number of pending TS to be flushed
func (tk *timekeeper) calcSkipFactorForFastFlush(streamId common.StreamId,
	keyspaceId string) uint64 {
	tsList := tk.ss.streamKeyspaceIdTsListMap[streamId][keyspaceId]

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

	//generate prepare init recovery for all stream/keyspaceIds
	for s, bs := range tk.ss.streamKeyspaceIdStatus {
		for b, status := range bs {
			if status != STREAM_INACTIVE {
				tk.ss.streamKeyspaceIdFlushEnabledMap[s][b] = false
				sessionId := tk.ss.getSessionId(s, b)
				if !tk.resetStreamIfOSOEnabled(s, b, sessionId, false) {
					tk.supvRespch <- &MsgRecovery{mType: INDEXER_INIT_PREP_RECOVERY,
						streamId:   s,
						keyspaceId: b,
						sessionId:  sessionId,
					}
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

	for range ticker.C {

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
	for s, bs := range tk.ss.streamKeyspaceIdStatus {

		for b := range bs {
			if tk.ss.streamKeyspaceIdRepairStopCh[s][b] != nil {
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
	keyspaceId string, flushTs *common.TsVbuuid) {

	switch flushTs.GetSnapType() {

	case common.INMEM_SNAP, common.NO_SNAP:
		tk.ss.streamKeyspaceIdNeedsCommitMap[streamId][keyspaceId] = true
	case common.DISK_SNAP, common.FORCE_COMMIT:
		tk.ss.streamKeyspaceIdNeedsCommitMap[streamId][keyspaceId] = false
	}

}

// This method has to be called while timepeeker has aquired tk.lock
func (tk *timekeeper) updateStreamKeyspaceIdVbMap(cmd Message) {

	streamId := cmd.(*MsgStream).GetStreamId()
	meta := cmd.(*MsgStream).GetMutationMeta()
	node := cmd.(*MsgStream).GetNode()
	if node != nil {
		nodeUUID := fmt.Sprintf("%s", node)
		keyspaceId := meta.keyspaceId
		tk.ss.streamKeyspaceIdVBMap[streamId][keyspaceId][meta.vbucket] = nodeUUID
	}
}

func (tk *timekeeper) handlePoolChange(cmd Message) {

	kvNodes := cmd.(*MsgPoolChange).GetNodes()
	streamId := cmd.(*MsgPoolChange).GetStreamId()
	keyspaceId := cmd.(*MsgPoolChange).GetKeyspaceId()
	tk.lock.Lock()
	defer tk.lock.Unlock()

	state := tk.ss.streamKeyspaceIdStatus[streamId][keyspaceId]
	if state != STREAM_ACTIVE {
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	vbMap := tk.ss.streamKeyspaceIdVBMap[streamId][keyspaceId]
	vbList := make(Vbuckets, 0)

	for vb, nodeuuid := range vbMap {
		if _, ok := kvNodes[nodeuuid]; !ok {
			// Node UUID not a part of active KV nodes. Get the vb's belonging to this KV node
			if vbState := tk.ss.streamKeyspaceIdVbStatusMap[streamId][keyspaceId][vb]; vbState == VBS_STREAM_BEGIN {
				vbList = append(vbList, vb)
			}
		}
	}

	if len(vbList) > 0 {
		sort.Sort(vbList)
		logging.Infof("Timekeeper::handlePoolChange streamId: %v, keyspaceId:%v, vbList: %v", streamId, keyspaceId, vbList)
		tk.handleStreamConnErrorInternal(streamId, keyspaceId, vbList)
	} else {
		tk.supvCmdch <- &MsgSuccess{}
	}
}

func (tk *timekeeper) ValidateKeyspace(streamId common.StreamId, keyspaceId string,
	bucketUUIDs []string) bool {

	collectionId := tk.ss.streamKeyspaceIdCollectionId[streamId][keyspaceId]

	bucket, scope, collection := SplitKeyspaceId(keyspaceId)

	//if the stream is using a cid, validate collection.
	//otherwise only validate the bucket
	if collectionId == "" {
		if !tk.clusterInfoClient.ValidateBucket(bucket, bucketUUIDs) {
			return false
		}
	} else {

		if scope == "" && collection == "" {
			scope = common.DEFAULT_SCOPE
			collection = common.DEFAULT_COLLECTION
		}

		if !tk.clusterInfoClient.ValidateCollectionID(bucket, scope,
			collection, collectionId) {
			return false
		}
	}
	return true

}

//ignoreException flag can be used by callers to instruct indexer to ignore
//any already recorded exception and always initiate recovery.
//This is useful in cases where the stream request is going to terminate, and
//there is no further trigger to initiate recovery which got skipped due to
//OSO exception.
func (tk *timekeeper) resetStreamIfOSOEnabled(streamId common.StreamId,
	keyspaceId string, sessionId uint64, ignoreException bool) bool {

	if tk.ss.streamKeyspaceIdEnableOSO[streamId][keyspaceId] {
		logging.Infof("Timekeeper::resetStreamIfOSOEnabled %v %v %v %v. Reset Stream. ",
			streamId, keyspaceId, sessionId, ignoreException)

		tk.ss.streamKeyspaceIdForceRecovery[streamId][keyspaceId] = true

		tk.supvRespch <- &MsgStreamUpdate{
			mType:              RESET_STREAM,
			streamId:           streamId,
			keyspaceId:         keyspaceId,
			sessionId:          sessionId,
			ignoreOSOException: ignoreException,
		}
		return true
	}
	return false
}
