// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

//TODO Does timekeeper need to take into account all the indexes for a bucket getting dropped?
//Right now it assumes for such a case there will be no SYNC message for that bucket, but doesn't
//clean up its internal maps

package indexer

import (
	"errors"
	"fmt"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/dcp"
	"github.com/couchbase/indexing/secondary/logging"
	"sync"
	"time"
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

	stats IndexerStatsHolder

	lock sync.Mutex //lock to protect this structure
}

type InitialBuildInfo struct {
	indexInst            common.IndexInst
	buildTs              Timestamp
	buildDoneAckReceived bool
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

	tk.lock.Lock()
	defer tk.lock.Unlock()

	if tk.ss.streamStatus[streamId] != STREAM_ACTIVE {
		tk.ss.initNewStream(streamId)
		logging.Debugf("Timekeeper::handleStreamOpen \n\t Stream %v "+
			"State Changed to ACTIVE", streamId)

	}

	status := tk.ss.streamBucketStatus[streamId][bucket]
	switch status {

	//fresh start or recovery
	case STREAM_INACTIVE, STREAM_PREPARE_DONE:
		tk.ss.initBucketInStream(streamId, bucket)
		if restartTs != nil {
			tk.ss.streamBucketRestartTsMap[streamId][bucket] = restartTs
			tk.ss.setHWTFromRestartTs(streamId, bucket)
		}
		tk.addIndextoStream(cmd)
		tk.startTimer(streamId, bucket)

	//repair
	default:
		//no-op
		logging.Debugf("Timekeeper::handleStreamOpen %v %v Status %v. "+
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
		tk.removeBucketFromStream(streamId, bucket)
	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleInitPrepRecovery(msg Message) {

	bucket := msg.(*MsgRecovery).GetBucket()
	streamId := msg.(*MsgRecovery).GetStreamId()

	logging.Debugf("Timekeeper::handleInitPrepRecovery %v %v",
		streamId, bucket)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	tk.prepareRecovery(streamId, bucket)

	tk.supvCmdch <- &MsgSuccess{}

}

func (tk *timekeeper) handlePrepareDone(cmd Message) {

	logging.Debugf("Timekeeper::handlePrepareDone %v", cmd)

	streamId := cmd.(*MsgRecovery).GetStreamId()
	bucket := cmd.(*MsgRecovery).GetBucket()

	tk.lock.Lock()
	defer tk.lock.Unlock()

	//if stream is in PREPARE_RECOVERY, check and init RECOVERY
	if tk.ss.streamBucketStatus[streamId][bucket] == STREAM_PREPARE_RECOVERY {

		logging.Debugf("Timekeeper::handlePrepareDone\n\t Stream %v "+
			"Bucket %v State Changed to PREPARE_DONE", streamId, bucket)
		tk.ss.streamBucketStatus[streamId][bucket] = STREAM_PREPARE_DONE

		switch streamId {
		case common.MAINT_STREAM, common.INIT_STREAM:
			if tk.checkBucketReadyForRecovery(streamId, bucket) {
				tk.initiateRecovery(streamId, bucket)
			}
		}
	} else {
		logging.Debugf("Timekeeper::handlePrepareDone\n\t Unexpected PREPARE_DONE "+
			"for StreamId %v Bucket %v State %v", streamId, bucket,
			tk.ss.streamBucketStatus[streamId][bucket])
	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleAddIndextoStream(cmd Message) {

	logging.Infof("Timekeeper::handleAddIndextoStream %v", cmd)

	tk.lock.Lock()
	defer tk.lock.Unlock()
	tk.addIndextoStream(cmd)

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) addIndextoStream(cmd Message) {

	indexInstList := cmd.(*MsgStreamUpdate).GetIndexList()
	buildTs := cmd.(*MsgStreamUpdate).GetTimestamp()
	streamId := cmd.(*MsgStreamUpdate).GetStreamId()

	//If the index is in INITIAL state, store it in initialbuild map
	for _, idx := range indexInstList {

		tk.ss.streamBucketIndexCountMap[streamId][idx.Defn.Bucket] += 1
		logging.Debugf("Timekeeper::addIndextoStream IndexCount %v", tk.ss.streamBucketIndexCountMap)

		// buildInfo can be reomved when the corresponding is closed.   A stream can be closed for
		// various condition, such as recovery.   When the stream is re-opened, index will be added back
		// to the stream.  It is necesasry to ensure that buildInfo is added back accordingly to ensure
		// that build index will converge.
		if _, ok := tk.indexBuildInfo[idx.InstId]; !ok {

			if idx.State == common.INDEX_STATE_INITIAL ||
				(streamId == common.INIT_STREAM && idx.State == common.INDEX_STATE_CATCHUP) {

				logging.Debugf("Timekeeper::addIndextoStream add BuildInfo index %v stream %v bucket %v",
					idx.InstId, streamId, idx.Defn.Bucket)
				tk.indexBuildInfo[idx.InstId] = &InitialBuildInfo{
					indexInst: idx,
					buildTs:   buildTs,
				}
			}
		}
	}

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

	tk.lock.Lock()
	defer tk.lock.Unlock()

	tk.removeBucketFromStream(streamId, bucket)

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) removeIndexFromStream(cmd Message) {

	indexInstList := cmd.(*MsgStreamUpdate).GetIndexList()
	streamId := cmd.(*MsgStreamUpdate).GetStreamId()

	for _, idx := range indexInstList {
		// This is only called for DROP INDEX.   Therefore, it is OK to remove buildInfo.
		// If this is used for other purpose, it is necessary to ensure there is no side effect.
		if _, ok := tk.indexBuildInfo[idx.InstId]; ok {
			logging.Debugf("Timekeeper::removeIndexFromStream remove index %v from stream %v bucket %v",
				idx.InstId, streamId, idx.Defn.Bucket)
			delete(tk.indexBuildInfo, idx.InstId)
		}
		if tk.ss.streamBucketIndexCountMap[streamId][idx.Defn.Bucket] == 0 {
			logging.Fatalf("Timekeeper::removeIndexFromStream Invalid Internal "+
				"State Detected. Index Count Underflow. Stream %s. Bucket %s.", streamId,
				idx.Defn.Bucket)
		} else {
			tk.ss.streamBucketIndexCountMap[streamId][idx.Defn.Bucket] -= 1
			logging.Debugf("Timekeeper::removeIndexFromStream IndexCount %v", tk.ss.streamBucketIndexCountMap)
		}
	}
}

func (tk *timekeeper) removeBucketFromStream(streamId common.StreamId,
	bucket string) {

	status := tk.ss.streamBucketStatus[streamId][bucket]

	// This is called when closing a stream or remove a bucket from a stream.  If this
	// is called during recovery,  it is necessary to ensure that the buildInfo is added
	// back to timekeeper accordingly.
	for instId, idx := range tk.indexBuildInfo {
		// remove buildInfo only for the given bucket AND stream
		if idx.indexInst.Defn.Bucket == bucket && idx.indexInst.Stream == streamId {
			logging.Debugf("Timekeeper::removeBucketFromStream remove index %v from stream %v bucket %v",
				instId, streamId, bucket)
			delete(tk.indexBuildInfo, instId)
		}
	}

	//for a bucket in prepareRecovery, only change the status
	//actual cleanup happens in initRecovery
	if status == STREAM_PREPARE_RECOVERY {
		tk.ss.streamBucketStatus[streamId][bucket] = STREAM_PREPARE_RECOVERY
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

	tk.lock.Lock()
	defer tk.lock.Unlock()

	//check if bucket is active in stream
	if tk.checkBucketActiveInStream(streamId, bucket) == false {
		logging.Tracef("Timekeeper::handleSync \n\tReceived Sync for "+
			"Inactive Bucket %v Stream %v. Ignored.", bucket, streamId)
		return
	}

	//if there are no indexes for this bucket and stream, ignore
	if c, ok := tk.ss.streamBucketIndexCountMap[streamId][bucket]; !ok || c <= 0 {
		logging.Tracef("Timekeeper::handleSync \n\tIgnore Sync for StreamId %v "+
			"Bucket %v. IndexCount %v. ", streamId, bucket, c)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	if _, ok := tk.ss.streamBucketHWTMap[streamId][bucket]; !ok {
		logging.Debugf("Timekeeper::handleSync \n\tIgnoring Sync Marker "+
			"for StreamId %v Bucket %v. Bucket Not Found.", streamId, bucket)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	//update HWT for the bucket
	tk.ss.updateHWT(streamId, bucket, hwt)
	hwt.Free()

	tk.supvCmdch <- &MsgSuccess{}

}

func (tk *timekeeper) handleFlushDone(cmd Message) {

	streamId := cmd.(*MsgMutMgrFlushDone).GetStreamId()
	bucket := cmd.(*MsgMutMgrFlushDone).GetBucket()

	tk.lock.Lock()
	defer tk.lock.Unlock()

	bucketLastFlushedTsMap := tk.ss.streamBucketLastFlushedTsMap[streamId]
	bucketFlushInProgressTsMap := tk.ss.streamBucketFlushInProgressTsMap[streamId]

	if _, ok := bucketFlushInProgressTsMap[bucket]; ok {
		//store the last flushed TS
		fts := bucketFlushInProgressTsMap[bucket]
		bucketLastFlushedTsMap[bucket] = fts

		// check if each flush time is snap aligned. If so, make a copy.
		if fts != nil && fts.IsSnapAligned() {
			tk.ss.streamBucketLastSnapAlignFlushedTsMap[streamId][bucket] = fts.Copy()
		}

		//update internal map to reflect flush is done
		bucketFlushInProgressTsMap[bucket] = nil
	} else {
		//this bucket is already gone from this stream, may be because
		//the index were dropped. Log and ignore.
		logging.Debugf("Timekeeper::handleFlushDone Ignore Flush Done for Stream %v "+
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

	case STREAM_PREPARE_RECOVERY, STREAM_PREPARE_DONE:

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

	case STREAM_PREPARE_RECOVERY, STREAM_PREPARE_DONE:

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
		bucketFlushInProgressTsMap := tk.ss.streamBucketFlushInProgressTsMap[streamId]
		if _, ok := bucketFlushInProgressTsMap[bucket]; ok {
			//if there is flush in progress, mark it as done
			if bucketFlushInProgressTsMap[bucket] != nil {
				bucketFlushInProgressTsMap[bucket] = nil
			}
		} else {
			//this bucket is already gone from this stream, may be because
			//the index were dropped. Log and ignore.
			logging.Debugf("Timekeeper::handleFlushDone Ignore Flush Abort for Stream %v "+
				"Bucket %v. Bucket Info Not Found", streamId, bucket)
			tk.supvCmdch <- &MsgSuccess{}
			return
		}

		//update abort status and initiate recovery
		bucketAbortInProgressMap := tk.ss.streamBucketAbortInProgressMap[streamId]
		bucketAbortInProgressMap[bucket] = false

		//if flush for all buckets is done and no abort is in progress,
		//recovery can be initiated
		if tk.checkBucketReadyForRecovery(streamId, bucket) {
			tk.initiateRecovery(streamId, bucket)
		}

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

	logging.Debugf("Timekeeper::handleFlushStateChange \n\t Received Flush State Change "+
		"for Bucket: %v StreamId: %v Type: %v", bucket, streamId, t)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	state := tk.ss.streamBucketStatus[streamId][bucket]

	if state == STREAM_INACTIVE {
		logging.Debugf("Timekeeper::handleFlushStateChange Ignore Flush State Change "+
			"for Bucket: %v StreamId: %v Type: %v State: %v", bucket, streamId, t, state)
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

	logging.Debugf("TK StreamBegin %v %v %v %v %v", streamId, meta.bucket,
		meta.vbucket, meta.vbuuid, meta.seqno)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	//check if bucket is active in stream
	if tk.checkBucketActiveInStream(streamId, meta.bucket) == false {
		logging.Warnf("Timekeeper::handleStreamBegin \n\tReceived StreamBegin for "+
			"Inactive Bucket %v Stream %v. Ignored.", meta.bucket, streamId)
		return
	}

	//if there are no indexes for this bucket and stream, ignore
	if c, ok := tk.ss.streamBucketIndexCountMap[streamId][meta.bucket]; !ok || c <= 0 {
		logging.Warnf("Timekeeper::handleStreamBegin \n\tIgnore StreamBegin for StreamId %v "+
			"Bucket %v. IndexCount %v. ", streamId, meta.bucket, c)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	state := tk.ss.streamBucketStatus[streamId][meta.bucket]

	switch state {

	case STREAM_ACTIVE:

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

		count := tk.ss.getVbRefCount(streamId, meta.bucket, meta.vbucket)
		if count > 1 {
			if stopCh, ok := tk.ss.streamBucketRepairStopCh[streamId][meta.bucket]; !ok || stopCh == nil {
				tk.ss.clearRestartVbRetry(streamId, meta.bucket, meta.vbucket)
				tk.ss.streamBucketRepairStopCh[streamId][meta.bucket] = make(StopChannel)
				logging.Debugf("Timekeeper::handleStreamBegin \n\tRepairStream due to vb ref count > 1. "+
					"StreamId %v MutationMeta %v", streamId, meta)
				go tk.repairStream(streamId, meta.bucket)
			}
		}

	case STREAM_PREPARE_RECOVERY, STREAM_PREPARE_DONE, STREAM_INACTIVE:
		//ignore stream begin in prepare_recovery
		logging.Debugf("Timekeeper::handleStreamBegin \n\tIgnore StreamBegin "+
			"for StreamId %v State %v MutationMeta %v", streamId, state, meta)

	default:
		logging.Errorf("Timekeeper::handleStreamBegin \n\tInvalid Stream State "+
			"StreamId %v Bucket %v State %v", streamId, meta.bucket, state)
	}

	tk.supvCmdch <- &MsgSuccess{}

}

func (tk *timekeeper) handleStreamEnd(cmd Message) {

	streamId := cmd.(*MsgStream).GetStreamId()
	meta := cmd.(*MsgStream).GetMutationMeta()

	defer meta.Free()

	logging.Debugf("TK StreamEnd %v %v %v %v %v", streamId, meta.bucket,
		meta.vbucket, meta.vbuuid, meta.seqno)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	//check if bucket is active in stream
	if tk.checkBucketActiveInStream(streamId, meta.bucket) == false {
		logging.Warnf("Timekeeper::handleStreamEnd \n\tReceived StreamEnd for "+
			"Inactive Bucket %v Stream %v. Ignored.", meta.bucket, streamId)
		return
	}

	//if there are no indexes for this bucket and stream, ignore
	if c, ok := tk.ss.streamBucketIndexCountMap[streamId][meta.bucket]; !ok || c <= 0 {
		logging.Warnf("Timekeeper::handleStreamEnd \n\tIgnore StreamEnd for StreamId %v "+
			"Bucket %v. IndexCount %v. ", streamId, meta.bucket, c)
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
		if vbState != VBS_CONN_ERROR {

			tk.ss.decVbRefCount(streamId, meta.bucket, meta.vbucket)
			count := tk.ss.getVbRefCount(streamId, meta.bucket, meta.vbucket)

			if count < 0 {
				// If count < 0, after CONN_ERROR, count is reset to 0.   During rebalancing,
				// residue StreamEnd from old master can still arrive.   Therefore, after
				// CONN_ERROR, the total number of StreamEnd > total number of StreamBegin,
				// and count will be negative in this case.
				logging.Debugf("Timekeeper::handleStreamEnd \n\tOwner count < 0. Treat as CONN_ERR. "+
					"StreamId %v MutationMeta %v", streamId, meta)

				tk.handleStreamConnErrorInternal(streamId, meta.bucket, []Vbucket{meta.vbucket})
				return

			} else {

				// If Count => 0.  This could be just normal vb take-over during rebalancing.
				tk.ss.updateVbStatus(streamId, meta.bucket, []Vbucket{meta.vbucket}, VBS_STREAM_END)
				if stopCh, ok := tk.ss.streamBucketRepairStopCh[streamId][meta.bucket]; !ok || stopCh == nil {
					tk.ss.clearRestartVbRetry(streamId, meta.bucket, meta.vbucket)
					tk.ss.streamBucketRepairStopCh[streamId][meta.bucket] = make(StopChannel)
					logging.Debugf("Timekeeper::handleStreamEnd \n\tRepairStream due to StreamEnd. "+
						"StreamId %v MutationMeta %v", streamId, meta)
					go tk.repairStream(streamId, meta.bucket)
				}
			}
		}

	case STREAM_PREPARE_RECOVERY, STREAM_PREPARE_DONE, STREAM_INACTIVE:
		//ignore stream end in prepare_recovery
		logging.Debugf("Timekeeper::handleStreamEnd \n\tIgnore StreamEnd for "+
			"StreamId %v State %v MutationMeta %v", streamId, state, meta)

	default:
		logging.Errorf("Timekeeper::handleStreamEnd \n\tInvalid Stream State "+
			"StreamId %v Bucket %v State %v", streamId, meta.bucket, state)
	}

	tk.supvCmdch <- &MsgSuccess{}

}

func (tk *timekeeper) handleStreamConnError(cmd Message) {

	logging.Debugf("Timekeeper::handleStreamConnError %v", cmd)

	streamId := cmd.(*MsgStreamInfo).GetStreamId()
	bucket := cmd.(*MsgStreamInfo).GetBucket()
	vbList := cmd.(*MsgStreamInfo).GetVbList()

	logging.Debugf("TK ConnError %v %v %v", streamId, bucket, vbList)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	tk.handleStreamConnErrorInternal(streamId, bucket, vbList)
}

func (tk *timekeeper) handleStreamConnErrorInternal(streamId common.StreamId, bucket string, vbList []Vbucket) {

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
			logging.Debugf("Timekeeper::handleStreamConnError \n\tRepairStream due to ConnError. "+
				"StreamId %v Bucket %v VbList %v", streamId, bucket, vbList)
			go tk.repairStream(streamId, bucket)
		}

	case STREAM_PREPARE_RECOVERY, STREAM_PREPARE_DONE, STREAM_INACTIVE:
		logging.Debugf("Timekeeper::handleStreamConnError \n\tIgnore Connection Error "+
			"for StreamId %v Bucket %v State %v", streamId, bucket, state)

	default:
		logging.Errorf("Timekeeper::handleStreamConnError \n\tInvalid Stream State "+
			"StreamId %v Bucket %v State %v", streamId, bucket, state)
	}

	tk.supvCmdch <- &MsgSuccess{}

}

func (tk *timekeeper) handleInitBuildDoneAck(cmd Message) {

	streamId := cmd.(*MsgTKInitBuildDone).GetStreamId()
	bucket := cmd.(*MsgTKInitBuildDone).GetBucket()

	logging.Debugf("Timekeeper::handleInitBuildDoneAck StreamId %v Bucket %v",
		streamId, bucket)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	state := tk.ss.streamBucketStatus[streamId][bucket]

	//ignore build done ack for Inactive and Recovery phase. For recovery, stream
	//will get reopen and build done will get recomputed.
	if state == STREAM_INACTIVE || state == STREAM_PREPARE_DONE ||
		state == STREAM_PREPARE_RECOVERY {
		logging.Debugf("Timekeeper::handleInitBuildDoneAck Ignore BuildDoneAck "+
			"for Bucket: %v StreamId: %v State: %v", bucket, streamId, state)
		return
	}

	if streamId == common.INIT_STREAM {
		for _, buildInfo := range tk.indexBuildInfo {
			if buildInfo.indexInst.Defn.Bucket == bucket {
				buildInfo.buildDoneAckReceived = true
			}
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

func (tk *timekeeper) handleMergeStreamAck(cmd Message) {

	streamId := cmd.(*MsgTKMergeStream).GetStreamId()
	bucket := cmd.(*MsgTKMergeStream).GetBucket()

	logging.Debugf("Timekeeper::handleMergeStreamAck StreamId %v Bucket %v",
		streamId, bucket)

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleStreamRequestDone(cmd Message) {

	streamId := cmd.(*MsgStreamInfo).GetStreamId()
	bucket := cmd.(*MsgStreamInfo).GetBucket()
	buildTs := cmd.(*MsgStreamInfo).GetBuildTs()

	logging.Debugf("Timekeeper::handleStreamRequestDone StreamId %v Bucket %v",
		streamId, bucket)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	tk.setBuildTs(streamId, bucket, buildTs)

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

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleRecoveryDone(cmd Message) {

	streamId := cmd.(*MsgRecovery).GetStreamId()
	bucket := cmd.(*MsgRecovery).GetBucket()
	buildTs := cmd.(*MsgRecovery).GetBuildTs()

	logging.Debugf("Timekeeper::handleRecoveryDone StreamId %v Bucket %v",
		streamId, bucket)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	tk.setBuildTs(streamId, bucket, buildTs)

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

	logging.Debugf("Timekeeper::prepareRecovery StreamId %v Bucket %v",
		streamId, bucket)

	//change to PREPARE_RECOVERY so that
	//no more TS generation and flush happen.
	if tk.ss.streamBucketStatus[streamId][bucket] == STREAM_ACTIVE {

		logging.Debugf("Timekeeper::prepareRecovery \n\t Stream %v "+
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

	//send message to stop running stream
	tk.supvRespch <- &MsgRecovery{mType: INDEXER_PREPARE_RECOVERY,
		streamId: streamId,
		bucket:   bucket}

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
			logging.Debugf("Timekeeper::flushOrAbortInProgressTS \n\tProcessing Flush TS %v "+
				"before recovery for bucket %v streamId %v", ts, bucket, streamId)
		} else {
			//else this flush needs to be aborted. Though some mutations may
			//arrive and flush may get completed before this abort message
			//reaches.
			logging.Debugf("Timekeeper::flushOrAbortInProgressTS \n\tAborting Flush TS %v "+
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
		logging.Debugf("Timekeeper::flushOrAbortInProgressTS \n\tRecovery can be initiated for "+
			"Bucket %v Stream %v", bucket, streamId)
	}

}

//checkInitialBuildDone checks if any of the index in Initial State is past its
//Build TS based on the Flush Done Message. It generates msg for Build Done
//and changes the state of the index.
func (tk *timekeeper) checkInitialBuildDone(streamId common.StreamId,
	bucket string, flushTs *common.TsVbuuid) bool {

	for _, buildInfo := range tk.indexBuildInfo {
		//if index belongs to the flushed bucket and in INITIAL state
		initBuildDone := false
		idx := buildInfo.indexInst
		if idx.Defn.Bucket == bucket &&
			idx.Stream == streamId &&
			idx.State == common.INDEX_STATE_INITIAL {

			//if buildTs is zero, initial build is done
			if buildInfo.buildTs.IsZeroTs() {
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
							logging.Debugf("Timekeeper::checkInitialBuildDone remove index %v from stream %v bucket %v",
								buildInfo.indexInst.InstId, streamId, bucket)
							delete(tk.indexBuildInfo, buildInfo.indexInst.InstId)
						}
					}
				}

				logging.Debugf("Timekeeper::checkInitialBuildDone \n\tInitial Build Done Index: %v "+
					"Stream: %v Bucket: %v BuildTS: %v", idx.InstId, streamId, bucket, buildInfo.buildTs)

				//generate init build done msg
				tk.supvRespch <- &MsgTKInitBuildDone{
					mType:    TK_INIT_BUILD_DONE,
					streamId: streamId,
					buildTs:  buildInfo.buildTs,
					bucket:   bucket}

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
	bucket string, flushTs *common.TsVbuuid) bool {

	logging.LazyTrace(func() string {
		return fmt.Sprintf("Timekeeper::checkInitStreamReadyToMerge \n\t Stream %v Bucket %v len(buildInfo) %v "+
			"FlushTs %v", streamId, bucket, len(tk.indexBuildInfo), flushTs)
	})

	if streamId != common.INIT_STREAM {
		return false
	}

	//if flushTs is not on snap boundary, merge cannot be done
	if !flushTs.IsSnapAligned() {
		logging.Debugf("Timekeeper::checkInitStreamReadyToMerge \n\tFlushTs Not Snapshot " +
			"Snapshot Aligned. Continue both streams.")
		return false
	}

	//INIT_STREAM cannot be merged to MAINT_STREAM if its not ACTIVE
	if tk.ss.streamBucketStatus[common.MAINT_STREAM][bucket] != STREAM_ACTIVE {
		logging.Debugf("Timekeeper::checkInitStreamReadyToMerge \n\tMAINT_STREAM in %v. "+
			"INIT_STREAM cannot be merged. Continue both streams.",
			tk.ss.streamBucketStatus[common.MAINT_STREAM][bucket])
		return false
	}

	//If any repair is going on, merge cannot happen
	if stopCh, ok := tk.ss.streamBucketRepairStopCh[common.MAINT_STREAM][bucket]; ok && stopCh != nil {

		logging.Debugf("Timekeeper::checkInitStreamReadyToMerge \n\t MAINT_STREAM In Repair." +
			"INIT_STREAM cannot be merged. Continue both streams.")
		return false
	}

	if stopCh, ok := tk.ss.streamBucketRepairStopCh[common.INIT_STREAM][bucket]; ok && stopCh != nil {

		logging.Debugf("Timekeeper::checkInitStreamReadyToMerge \n\t INIT_STREAM In Repair." +
			"INIT_STREAM cannot be merged. Continue both streams.")
		return false
	}

	for _, buildInfo := range tk.indexBuildInfo {

		//if index belongs to the flushed bucket and in CATCHUP state and
		//buildDoneAck has been received
		idx := buildInfo.indexInst
		if idx.Defn.Bucket == bucket &&
			idx.State == common.INDEX_STATE_CATCHUP &&
			buildInfo.buildDoneAckReceived == true {

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

			//if no flush has happened yet, its good to merge
			readyToMerge := false
			var ts, lastFlushedTs Timestamp
			if lastFlushedTsVbuuid == nil {
				readyToMerge = true
			} else if flushTs == nil {
				//if flushTs is nil for INIT_STREAM and non-nil for MAINT_STREAM
				//merge cannot happen
				readyToMerge = false
			} else {
				lastFlushedTs = getSeqTsFromTsVbuuid(lastFlushedTsVbuuid)
				ts = getSeqTsFromTsVbuuid(flushTs)
				if ts.GreaterThanEqual(lastFlushedTs) {
					readyToMerge = true
				}
			}

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

				logging.Debugf("Timekeeper::checkInitStreamReadyToMerge \n\tIndex Ready To Merge. "+
					"Index: %v Stream: %v Bucket: %v LastFlushTS: %v", idx.InstId, streamId,
					bucket, lastFlushedTs)

				tk.supvRespch <- &MsgTKMergeStream{
					mType:    TK_MERGE_STREAM,
					streamId: streamId,
					bucket:   bucket,
					mergeTs:  ts}

				logging.Debugf("Timekeeper::checkInitStreamReadyToMerge \n\t Stream %v "+
					"Bucket %v State Changed to INACTIVE", streamId, bucket)
				tk.stopTimer(streamId, bucket)
				tk.ss.cleanupBucketFromStream(streamId, bucket)
				return true

			}
		}
	}

	return false
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

			logging.Debugf("Timekeeper::checkCatchupStreamReadyToMerge \n\tBucket Ready To Merge. "+
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

	if status, ok := tk.ss.streamBucketStatus[streamId][bucket]; !ok || status != STREAM_ACTIVE {
		return
	}

	if tk.ss.checkNewTSDue(streamId, bucket) {
		tsVbuuid := tk.ss.getNextStabilityTS(streamId, bucket)

		//persist TS which completes the build
		if tk.isBuildCompletionTs(streamId, bucket, tsVbuuid) {
			tsVbuuid.SetSnapType(common.DISK_SNAP)
		}

		if tk.ss.canFlushNewTS(streamId, bucket) {
			tk.sendNewStabilityTS(tsVbuuid, bucket, streamId)
		} else {
			//store the ts in list
			logging.LazyDebug(func() string {
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
	} else {
		tk.processPendingTS(streamId, bucket)
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
						"Timekeeper::processPendingTS \n\tProcessing Flush TS %v "+
							"before recovery for bucket %v streamId %v", ts, bucket, streamId)
				})
			} else {
				//empty the TSList for this bucket and stream so
				//there is no further processing for this bucket.
				logging.LazyDebug(func() string {
					return fmt.Sprintf(
						"Timekeeper::processPendingTS \n\tCannot Flush TS %v "+
							"before recovery for bucket %v streamId %v. Clearing Ts List", ts,
						bucket, streamId)
				})
				tsList.Init()
				return false
			}

		}
		tk.sendNewStabilityTS(tsVbuuid, bucket, streamId)
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

	changeVec, noChange := tk.ss.computeTsChangeVec(streamId, bucket, flushTs)
	if noChange {
		return
	}

	tk.setSnapshotType(streamId, bucket, flushTs)

	tk.ss.streamBucketFlushInProgressTsMap[streamId][bucket] = flushTs

	go func() {
		tk.supvRespch <- &MsgTKStabilityTS{ts: flushTs,
			bucket:    bucket,
			streamId:  streamId,
			changeVec: changeVec}
	}()
}

//set the snapshot type
func (tk *timekeeper) setSnapshotType(streamId common.StreamId, bucket string,
	flushTs *common.TsVbuuid) {

	snapPersistInterval := tk.config["settings.persisted_snapshot.interval"].Uint64()

	persistDuration := time.Duration(snapPersistInterval) * time.Millisecond
	lastPersistTime := tk.ss.streamBucketLastPersistTime[streamId][bucket]

	//for init build, if there is no snapshot option set
	if tk.hasInitStateIndex(streamId, bucket) {
		if flushTs.GetSnapType() == common.NO_SNAP {
			//if this TS is a merge candidate, generate in-mem snapshot
			if tk.checkMergeCandidateTs(streamId, bucket, flushTs) {
				flushTs.SetSnapType(common.INMEM_SNAP)
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
		if time.Since(lastPersistTime) > persistDuration {
			flushTs.SetSnapType(common.DISK_SNAP)
			tk.ss.streamBucketLastPersistTime[streamId][bucket] = time.Now()
		} else {
			flushTs.SetSnapType(common.INMEM_SNAP)
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

	if tk.hasInitStateIndex(streamId, bucket) {
		return
	}

	hwt := tk.ss.streamBucketHWTMap[streamId][bucket]

	largeSnap := tk.ss.config["settings.largeSnapshotThreshold"].Uint64()

	for i, s := range flushTs.Snapshots {

		//if diff between snapEnd and seqno is with largeSnap limit
		//and all mutations have been received till snapEnd
		if s[1]-flushTs.Seqnos[i] < largeSnap &&
			hwt.Seqnos[i] >= s[1] {
			flushTs.Seqnos[i] = s[1]
		}
	}

	if flushTs.CheckSnapAligned() {
		flushTs.SetSnapAligned(true)
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
			"\n\tUnknown Stream: %v", streamId)
		tk.supvCmdch <- &MsgError{
			err: Error{code: ERROR_TK_UNKNOWN_STREAM,
				severity: NORMAL,
				category: TIMEKEEPER}}
		return false
	} else if status, ok := bucketStatus[bucket]; !ok || status != STREAM_ACTIVE {
		logging.Tracef("Timekeeper::checkBucketActiveInStream "+
			"\n\tUnknown Bucket %v In Stream %v", bucket, streamId)
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

		restartTs := tk.ss.computeRestartTs(streamId, bucket)
		//adjust for non-snap aligned ts
		tk.ss.adjustNonSnapAlignedVbs(restartTs, streamId, bucket)

		tk.stopTimer(streamId, bucket)
		tk.ss.cleanupBucketFromStream(streamId, bucket)

		//send message for recovery
		tk.supvRespch <- &MsgRecovery{mType: INDEXER_INITIATE_RECOVERY,
			streamId:  streamId,
			bucket:    bucket,
			restartTs: restartTs}
		logging.Debugf("Timekeeper::initiateRecovery StreamId %v "+
			"RestartTs %v", streamId, restartTs)
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

	logging.Debugf("Timekeeper::checkBucketReadyForRecovery StreamId %v "+
		"Bucket %v", streamId, bucket)

	if tk.ss.checkAnyFlushPending(streamId, bucket) ||
		tk.ss.checkAnyAbortPending(streamId, bucket) ||
		tk.ss.streamBucketStatus[streamId][bucket] != STREAM_PREPARE_DONE {
		return false
	}

	logging.Debugf("Timekeeper::checkBucketReadyForRecovery StreamId %v "+
		"Bucket %v Ready for Recovery", streamId, bucket)
	return true
}

func (tk *timekeeper) handleStreamCleanup(cmd Message) {

	logging.Debugf("Timekeeper::handleStreamCleanup %v", cmd)

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()

	logging.Debugf("Timekeeper::handleStreamCleanup \n\t Stream %v "+
		"State Changed to INACTIVE", streamId)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	tk.ss.streamStatus[streamId] = STREAM_INACTIVE

	tk.supvCmdch <- &MsgSuccess{}

}

func (tk *timekeeper) repairStream(streamId common.StreamId,
	bucket string) {

	//wait for REPAIR_BATCH_TIMEOUT to batch more error msgs
	//and send request to KV for a batch
	time.Sleep(REPAIR_BATCH_TIMEOUT * time.Millisecond)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	//prepare repairTs with all vbs in STREAM_END, REPAIR status and
	//send that to KVSender to repair
	if repairTs, needRepair, connErrVbs := tk.ss.getRepairTsForBucket(streamId, bucket); needRepair {

		respCh := make(MsgChannel)
		stopCh := tk.ss.streamBucketRepairStopCh[streamId][bucket]

		restartMsg := &MsgRestartVbuckets{streamId: streamId,
			bucket:     bucket,
			restartTs:  repairTs,
			respCh:     respCh,
			stopCh:     stopCh,
			connErrVbs: connErrVbs}

		// before retrying, clear up the previous marker on retry error.
		tk.ss.clearRestartVbError(streamId, bucket)

		go tk.sendRestartMsg(restartMsg)

	} else {
		delete(tk.ss.streamBucketRepairStopCh[streamId], bucket)
		logging.Debugf("Timekeeper::repairStream Nothing to repair for "+
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

	switch kvresp.GetMsgType() {

	case MSG_SUCCESS:
		//allow sufficient time for control messages to come in
		//after projector has confirmed success
		time.Sleep(KV_RETRY_INTERVAL * time.Millisecond)

		//check for more vbuckets in repair state
		tk.repairStream(streamId, bucket)

	case INDEXER_ROLLBACK:
		//if rollback msg, call initPrepareRecovery
		logging.Infof("Timekeeper::sendRestartMsg Received Rollback Msg For "+
			"%v %v. Sending Init Prepare.", streamId, bucket)

		tk.supvRespch <- &MsgRecovery{mType: INDEXER_INIT_PREP_RECOVERY,
			streamId: streamId,
			bucket:   bucket}

	case KV_STREAM_REPAIR:

		logging.Infof("Timekeeper::sendRestartMsg Received KV Repair Msg For "+
			"Stream %v Bucket %v. Attempting Stream Repair.", streamId, bucket)

		tk.lock.Lock()
		defer tk.lock.Unlock()

		resp := kvresp.(*MsgKVStreamRepair)

		//for stream repair, use the HWT. If there has been a rollback, it will
		//be detected as response of the MTR. Without rollback, even if a vbucket
		//has moved to a new node, using HWT is sufficient.
		resp.restartTs = tk.ss.streamBucketHWTMap[streamId][bucket].Copy()

		//adjust for non-snap aligned ts
		tk.ss.adjustNonSnapAlignedVbs(resp.restartTs, streamId, bucket)

		delete(tk.ss.streamBucketRepairStopCh[streamId], bucket)

		tk.supvRespch <- resp

	default:

		var bucketUUIDList []string
		for _, indexInst := range tk.indexInstMap {
			if indexInst.Defn.Bucket == bucket && indexInst.Stream == streamId &&
				indexInst.State != common.INDEX_STATE_DELETED {
				bucketUUIDList = append(bucketUUIDList, indexInst.Defn.BucketUUID)
			}
		}

		if !ValidateBucket(tk.config["clusterAddr"].String(), bucket, bucketUUIDList) {
			logging.Errorf("Timekeeper::sendRestartMsg \n\tBucket Not Found "+
				"For Stream %v Bucket %v", streamId, bucket)

			delete(tk.ss.streamBucketRepairStopCh[streamId], bucket)

			tk.ss.streamBucketStatus[streamId][bucket] = STREAM_INACTIVE

			tk.supvRespch <- &MsgRecovery{mType: INDEXER_BUCKET_NOT_FOUND,
				streamId: streamId,
				bucket:   bucket}
		} else {
			logging.Fatalf("Timekeeper::sendRestartMsg Error Response "+
				"from KV %v For Request %v. Retrying RestartVbucket.", kvresp, restartMsg)
			tk.ss.markRestartVbError(streamId, bucket)
			tk.repairStream(streamId, bucket)
		}
	}

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

			v := flushedCount
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
				v = 0
			case common.INDEX_STATE_ACTIVE:
				v = 100
			case common.INDEX_STATE_INITIAL, common.INDEX_STATE_CATCHUP:
				totalToBeflushed := uint64(0)
				for _, seqno := range kvTs {
					totalToBeflushed += uint64(seqno)
				}

				if totalToBeflushed > flushedCount {
					v = flushedCount * 100 / totalToBeflushed
				} else {
					v = 100
				}
			}

			if idxStats != nil {
				idxStats.numDocsIndexed.Set(int64(flushedCount))
				idxStats.numDocsQueued.Set(int64(queued))
				idxStats.numDocsPending.Set(int64(pending))
				idxStats.buildProgress.Set(int64(v))
			}
		}

		replych <- true
	}()
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
				ts := getSeqTsFromTsVbuuid(flushTs)
				if ts.GreaterThanEqual(buildInfo.buildTs) {
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

	logging.Debugf("Timekeeper::startTimer %v %v", streamId, bucket)

	snapInterval := tk.config["settings.inmemory_snapshot.interval"].Uint64()
	ticker := time.NewTicker(time.Millisecond * time.Duration(snapInterval))
	stopCh := tk.ss.streamBucketTimerStopCh[streamId][bucket]

	go func() {
		for {
			select {
			case <-ticker.C:
				tk.generateNewStabilityTS(streamId, bucket)

			case <-stopCh:
				return
			}
		}
	}()

}

//stopTimer stops the stream/bucket timer started by startTimer
func (tk *timekeeper) stopTimer(streamId common.StreamId, bucket string) {

	logging.Debugf("Timekeeper::stopTimer %v %v", streamId, bucket)

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
