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
	"fmt"
	"github.com/couchbase/indexing/secondary/common"
	"sync"
	"time"
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

	lock sync.Mutex //lock to protect this structure
}

type InitialBuildInfo struct {
	indexInst            common.IndexInst
	buildTs              Timestamp
	buildDoneAckReceived bool
}

//timeout in milliseconds to batch the vbuckets
//together for repair message
const REPAIR_BATCH_TIMEOUT = 100

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
					common.Infof("Timekeeper::run Shutting Down")
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

	case STREAM_READER_SNAPSHOT_MARKER:
		tk.handleSnapshotMarker(cmd)

	case STREAM_READER_SYNC:
		tk.handleSync(cmd)

	case STREAM_READER_CONN_ERROR:
		tk.handleStreamConnError(cmd)

	case TK_ENABLE_FLUSH:
		tk.handleFlushStateChange(cmd)

	case TK_DISABLE_FLUSH:
		tk.handleFlushStateChange(cmd)

	case MUT_MGR_FLUSH_DONE:
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

	case CONFIG_SETTINGS_UPDATE:
		tk.handleConfigUpdate(cmd)

	case UPDATE_INDEX_INSTANCE_MAP:
		tk.handleUpdateIndexInstMap(cmd)

	case UPDATE_INDEX_PARTITION_MAP:
		tk.handleUpdateIndexPartnMap(cmd)

	case INDEX_PROGRESS_STATS:
		tk.handleStats(cmd)

	default:
		common.Errorf("Timekeeper::handleSupvervisorCommands "+
			"Received Unknown Command %v", cmd)

	}

}

func (tk *timekeeper) handleStreamOpen(cmd Message) {

	common.Debugf("Timekeeper::handleStreamOpen %v", cmd)

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()
	bucket := cmd.(*MsgStreamUpdate).GetBucket()
	restartTs := cmd.(*MsgStreamUpdate).GetRestartTs()

	tk.lock.Lock()
	defer tk.lock.Unlock()

	if tk.ss.streamStatus[streamId] != STREAM_ACTIVE {
		tk.ss.initNewStream(streamId)
		common.Debugf("Timekeeper::handleStreamOpen \n\t Stream %v "+
			"State Changed to ACTIVE", streamId)

	}

	status := tk.ss.streamBucketStatus[streamId][bucket]
	switch status {

	//fresh start or recovery
	case STREAM_INACTIVE, STREAM_PREPARE_DONE:
		if restartTs != nil {
			tk.ss.streamBucketRestartTsMap[streamId][bucket] = restartTs
		}
		tk.ss.initBucketInStream(streamId, bucket)
		tk.ss.setHWTFromRestartTs(streamId, bucket)
		tk.addIndextoStream(cmd)

	//repair
	default:
		//no-op
		common.Debugf("Timekeeper::handleStreamOpen %v %v Status %v. "+
			"Nothing to do.", streamId, bucket, status)

	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleStreamClose(cmd Message) {

	common.Debugf("Timekeeper::handleStreamClose %v", cmd)

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

	common.Debugf("Timekeeper::handleInitPrepRecovery %v %v",
		streamId, bucket)

	tk.prepareRecovery(streamId, bucket)

	tk.supvCmdch <- &MsgSuccess{}

}

func (tk *timekeeper) handlePrepareDone(cmd Message) {

	common.Debugf("Timekeeper::handlePrepareDone %v", cmd)

	streamId := cmd.(*MsgRecovery).GetStreamId()
	bucket := cmd.(*MsgRecovery).GetBucket()

	tk.lock.Lock()
	defer tk.lock.Unlock()

	//if stream is in PREPARE_RECOVERY, check and init RECOVERY
	if tk.ss.streamBucketStatus[streamId][bucket] == STREAM_PREPARE_RECOVERY {

		common.Debugf("Timekeeper::handlePrepareDone\n\t Stream %v "+
			"Bucket %v State Changed to PREPARE_DONE", streamId, bucket)
		tk.ss.streamBucketStatus[streamId][bucket] = STREAM_PREPARE_DONE

		switch streamId {
		case common.MAINT_STREAM, common.INIT_STREAM:
			if tk.checkBucketReadyForRecovery(streamId, bucket) {
				tk.initiateRecovery(streamId, bucket)
			}
		}
	} else {
		common.Debugf("Timekeeper::handlePrepareDone\n\t Unexpected PREPARE_DONE "+
			"for StreamId %v Bucket %v State %v", streamId, bucket,
			tk.ss.streamBucketStatus[streamId][bucket])
	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleAddIndextoStream(cmd Message) {

	common.Infof("Timekeeper::handleAddIndextoStream %v", cmd)

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
		common.Debugf("Timekeeper::addIndextoStream IndexCount %v", tk.ss.streamBucketIndexCountMap)

		if idx.State == common.INDEX_STATE_INITIAL {
			tk.indexBuildInfo[idx.InstId] = &InitialBuildInfo{
				indexInst: idx,
				buildTs:   buildTs}
		}
	}

}

func (tk *timekeeper) handleRemoveIndexFromStream(cmd Message) {

	common.Infof("Timekeeper::handleRemoveIndexFromStream %v", cmd)

	tk.lock.Lock()
	defer tk.lock.Unlock()

	tk.removeIndexFromStream(cmd)

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleRemoveBucketFromStream(cmd Message) {

	common.Infof("Timekeeper::handleRemoveBucketFromStream %v", cmd)

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
		//if the index in internal map, delete it
		if _, ok := tk.indexBuildInfo[idx.InstId]; ok {
			delete(tk.indexBuildInfo, idx.InstId)
		}
		if tk.ss.streamBucketIndexCountMap[streamId][idx.Defn.Bucket] == 0 {
			common.Fatalf("Timekeeper::removeIndexFromStream Invalid Internal "+
				"State Detected. Index Count Underflow. Stream %s. Bucket %s.", streamId,
				idx.Defn.Bucket)
		} else {
			tk.ss.streamBucketIndexCountMap[streamId][idx.Defn.Bucket] -= 1
			common.Debugf("Timekeeper::removeIndexFromStream IndexCount %v", tk.ss.streamBucketIndexCountMap)
		}
	}
}

func (tk *timekeeper) removeBucketFromStream(streamId common.StreamId,
	bucket string) {

	status := tk.ss.streamBucketStatus[streamId][bucket]

	//delete all indexes for this bucket
	for instId, idx := range tk.indexBuildInfo {
		if idx.indexInst.Defn.Bucket == bucket {
			delete(tk.indexBuildInfo, instId)
		}
	}

	//for a bucket in prepareRecovery, only change the status
	//actual cleanup happens in initRecovery
	if status == STREAM_PREPARE_RECOVERY {
		tk.ss.streamBucketStatus[streamId][bucket] = STREAM_PREPARE_RECOVERY
	} else {
		tk.ss.cleanupBucketFromStream(streamId, bucket)
	}

}

func (tk *timekeeper) handleSync(cmd Message) {

	common.Tracef("Timekeeper::handleSync %v", cmd)

	streamId := cmd.(*MsgStream).GetStreamId()
	meta := cmd.(*MsgStream).GetMutationMeta()

	tk.lock.Lock()
	defer tk.lock.Unlock()

	//check if bucket is active in stream
	if tk.checkBucketActiveInStream(streamId, meta.bucket) == false {
		common.Warnf("Timekeeper::handleSync \n\tReceived Sync for "+
			"Inactive Bucket %v Stream %v. Ignored.", meta.bucket, streamId)
		return
	}

	//if there are no indexes for this bucket and stream, ignore
	if c, ok := tk.ss.streamBucketIndexCountMap[streamId][meta.bucket]; !ok || c <= 0 {
		common.Tracef("Timekeeper::handleSync \n\tIgnore Sync for StreamId %v "+
			"Bucket %v. IndexCount %v. ", streamId, meta.bucket, c)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	if _, ok := tk.ss.streamBucketHWTMap[streamId][meta.bucket]; !ok {
		common.Debugf("Timekeeper::handleSync \n\tIgnoring Sync Marker "+
			"for StreamId %v Bucket %v. Bucket Not Found.", streamId, meta.bucket)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	//update HWT for the bucket
	tk.ss.updateHWT(streamId, meta)

	//update Sync Count for the bucket
	tk.ss.incrSyncCount(streamId, meta.bucket)

	tk.generateNewStabilityTS(streamId, meta.bucket)

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
		bucketLastFlushedTsMap[bucket] = bucketFlushInProgressTsMap[bucket]
		//update internal map to reflect flush is done
		bucketFlushInProgressTsMap[bucket] = nil
	} else {
		//this bucket is already gone from this stream, may be because
		//the index were dropped. Log and ignore.
		common.Debugf("Timekeeper::handleFlushDone Ignore Flush Done for Stream %v "+
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
		common.Errorf("Timekeeper::handleFlushDone \n\tInvalid StreamId %v ", streamId)
	}

}

func (tk *timekeeper) handleFlushDoneMaintStream(cmd Message) {

	common.Tracef("Timekeeper::handleFlushDoneMaintStream %v", cmd)

	streamId := cmd.(*MsgMutMgrFlushDone).GetStreamId()
	bucket := cmd.(*MsgMutMgrFlushDone).GetBucket()

	state := tk.ss.streamBucketStatus[streamId][bucket]

	switch state {

	case STREAM_ACTIVE, STREAM_RECOVERY:

		//check if any of the initial build index is past its Build TS.
		//Generate msg for Build Done and change the state of the index.
		flushTs := tk.ss.streamBucketLastFlushedTsMap[streamId][bucket]
		tk.checkInitialBuildDone(streamId, bucket, flushTs)

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
		common.Errorf("Timekeeper::handleFlushDoneMaintStream Unexpected Flush Done "+
			"Received for Inactive StreamId %v.", streamId)

	default:
		common.Errorf("Timekeeper::handleFlushDoneMaintStream Invalid Stream State %v.", state)

	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleFlushDoneCatchupStream(cmd Message) {

	common.Tracef("Timekeeper::handleFlushDoneCatchupStream %v", cmd)

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

		common.Errorf("Timekeeper::handleFlushDoneCatchupStream Invalid State Detected. "+
			"CATCHUP_STREAM Can Only Be Flushed in ACTIVE or PREPARE_RECOVERY state. "+
			"Current State %v.", state)
	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleFlushDoneInitStream(cmd Message) {

	common.Tracef("Timekeeper::handleFlushDoneInitStream %v", cmd)

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

		common.Errorf("Timekeeper::handleFlushDoneInitStream Invalid State Detected. "+
			"INIT_STREAM Can Only Be Flushed in ACTIVE or PREPARE_RECOVERY state. "+
			"Current State %v.", state)
	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleFlushAbortDone(cmd Message) {

	common.Tracef("Timekeeper::handleFlushAbortDone %v", cmd)

	streamId := cmd.(*MsgMutMgrFlushDone).GetStreamId()
	bucket := cmd.(*MsgMutMgrFlushDone).GetBucket()

	tk.lock.Lock()
	defer tk.lock.Unlock()

	state := tk.ss.streamBucketStatus[streamId][bucket]

	switch state {

	case STREAM_ACTIVE:
		common.Errorf("Timekeeper::handleFlushAbortDone Unexpected Flush Abort "+
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
			common.Debugf("Timekeeper::handleFlushDone Ignore Flush Abort for Stream %v "+
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
		common.Errorf("Timekeeper::handleFlushAbortDone Unexpected Flush Abort "+
			"Received for StreamId %v Bucket %v State %v", streamId, bucket, state)

	default:
		common.Errorf("Timekeeper::handleFlushAbortDone Invalid Stream State %v "+
			"for StreamId %v Bucket %v", state, streamId, bucket)

	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleFlushStateChange(cmd Message) {

	t := cmd.(*MsgTKToggleFlush).GetMsgType()
	streamId := cmd.(*MsgTKToggleFlush).GetStreamId()
	bucket := cmd.(*MsgTKToggleFlush).GetBucket()

	common.Debugf("Timekeeper::handleFlushStateChange \n\t Received Flush State Change "+
		"for Bucket: %v StreamId: %v Type: %v", bucket, streamId, t)

	//TODO Check if stream is valid

	tk.lock.Lock()
	defer tk.lock.Unlock()

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

func (tk *timekeeper) handleSnapshotMarker(cmd Message) {

	common.Tracef("Timekeeper::handleSnapshotMarker %v", cmd)

	streamId := cmd.(*MsgStream).GetStreamId()
	meta := cmd.(*MsgStream).GetMutationMeta()

	tk.lock.Lock()
	defer tk.lock.Unlock()

	//check if bucket is active in stream
	if tk.checkBucketActiveInStream(streamId, meta.bucket) == false {
		common.Warnf("Timekeeper::handleSnapshotMarker \n\tReceived Snapshot Marker for "+
			"Inactive Bucket %v Stream %v. Ignored.", meta.bucket, streamId)
		return
	}

	//if there are no indexes for this bucket and stream, ignore
	if c, ok := tk.ss.streamBucketIndexCountMap[streamId][meta.bucket]; !ok || c <= 0 {
		common.Warnf("Timekeeper::handleSnapshotMarker \n\tIgnore Snapshot for StreamId %v "+
			"Bucket %v. IndexCount %v. ", streamId, meta.bucket, c)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	snapshot := cmd.(*MsgStream).GetSnapshot()
	if snapshot.CanProcess() == true {
		//update the snapshot seqno in internal map
		ts := tk.ss.streamBucketHWTMap[streamId][meta.bucket]
		ts.Snapshots[meta.vbucket][0] = snapshot.start
		ts.Snapshots[meta.vbucket][1] = snapshot.end

		tk.ss.streamBucketNewTsReqdMap[streamId][meta.bucket] = true
		common.Tracef("Timekeeper::handleSnapshotMarker \n\tUpdated TS %v", ts)
	} else {
		common.Debugf("Timekeeper::handleSnapshotMarker \n\tIgnoring Snapshot Marker. "+
			"Unknown Type %v. Bucket %v. StreamId %v", snapshot.snapType, meta.bucket,
			streamId)
	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleGetBucketHWT(cmd Message) {

	common.Debugf("Timekeeper::handleGetBucketHWT %v", cmd)

	streamId := cmd.(*MsgTKGetBucketHWT).GetStreamId()
	bucket := cmd.(*MsgTKGetBucketHWT).GetBucket()

	tk.lock.Lock()
	defer tk.lock.Unlock()

	//set the return ts to nil
	msg := cmd.(*MsgTKGetBucketHWT)
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

	common.Debugf("Timekeeper::handleStreamBegin %v", cmd)

	streamId := cmd.(*MsgStream).GetStreamId()
	meta := cmd.(*MsgStream).GetMutationMeta()

	tk.lock.Lock()
	defer tk.lock.Unlock()

	//check if bucket is active in stream
	if tk.checkBucketActiveInStream(streamId, meta.bucket) == false {
		common.Warnf("Timekeeper::handleStreamBegin \n\tReceived StreamBegin for "+
			"Inactive Bucket %v Stream %v. Ignored.", meta.bucket, streamId)
		return
	}

	//if there are no indexes for this bucket and stream, ignore
	if c, ok := tk.ss.streamBucketIndexCountMap[streamId][meta.bucket]; !ok || c <= 0 {
		common.Warnf("Timekeeper::handleStreamBegin \n\tIgnore StreamBegin for StreamId %v "+
			"Bucket %v. IndexCount %v. ", streamId, meta.bucket, c)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	state := tk.ss.streamBucketStatus[streamId][meta.bucket]

	switch state {

	case STREAM_ACTIVE:

		//update the HWT of this stream and bucket with the vbuuid
		bucketHWTMap := tk.ss.streamBucketHWTMap[streamId]

		//TODO: Check if this is duplicate StreamBegin. Treat it as StreamEnd.
		ts := bucketHWTMap[meta.bucket]
		ts.Vbuuids[meta.vbucket] = uint64(meta.vbuuid)
		tk.ss.updateVbStatus(streamId, meta.bucket, []Vbucket{meta.vbucket}, VBS_STREAM_BEGIN)

	case STREAM_PREPARE_RECOVERY, STREAM_PREPARE_DONE, STREAM_INACTIVE:
		//ignore stream begin in prepare_recovery
		common.Debugf("Timekeeper::handleStreamBegin \n\tIgnore StreamBegin "+
			"for StreamId %v State %v MutationMeta %v", streamId, state, meta)

	default:
		common.Errorf("Timekeeper::handleStreamBegin \n\tInvalid Stream State "+
			"StreamId %v Bucket %v State %v", streamId, meta.bucket, state)
	}

	tk.supvCmdch <- &MsgSuccess{}

}

func (tk *timekeeper) handleStreamEnd(cmd Message) {

	common.Debugf("Timekeeper::handleStreamEnd %v", cmd)

	streamId := cmd.(*MsgStream).GetStreamId()
	meta := cmd.(*MsgStream).GetMutationMeta()

	tk.lock.Lock()
	defer tk.lock.Unlock()

	//check if bucket is active in stream
	if tk.checkBucketActiveInStream(streamId, meta.bucket) == false {
		common.Warnf("Timekeeper::handleStreamEnd \n\tReceived StreamEnd for "+
			"Inactive Bucket %v Stream %v. Ignored.", meta.bucket, streamId)
		return
	}

	//if there are no indexes for this bucket and stream, ignore
	if c, ok := tk.ss.streamBucketIndexCountMap[streamId][meta.bucket]; !ok || c <= 0 {
		common.Warnf("Timekeeper::handleStreamEnd \n\tIgnore StreamEnd for StreamId %v "+
			"Bucket %v. IndexCount %v. ", streamId, meta.bucket, c)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	state := tk.ss.streamBucketStatus[streamId][meta.bucket]
	switch state {

	case STREAM_ACTIVE:
		tk.ss.updateVbStatus(streamId, meta.bucket, []Vbucket{meta.vbucket}, VBS_STREAM_END)
		if stopCh, ok := tk.ss.streamBucketRepairStopCh[streamId][meta.bucket]; !ok || stopCh == nil {
			tk.ss.streamBucketRepairStopCh[streamId][meta.bucket] = make(StopChannel)
			common.Debugf("Timekeeper::handleStreamEnd \n\tRepairStream due to StreamEnd. "+
				"StreamId %v MutationMeta %v", streamId, meta)
			go tk.repairStream(streamId, meta.bucket)
		}

	case STREAM_PREPARE_RECOVERY, STREAM_PREPARE_DONE, STREAM_INACTIVE:
		//ignore stream end in prepare_recovery
		common.Debugf("Timekeeper::handleStreamEnd \n\tIgnore StreamEnd for "+
			"StreamId %v State %v MutationMeta %v", streamId, state, meta)

	default:
		common.Errorf("Timekeeper::handleStreamEnd \n\tInvalid Stream State "+
			"StreamId %v Bucket %v State %v", streamId, meta.bucket, state)
	}

	tk.supvCmdch <- &MsgSuccess{}

}
func (tk *timekeeper) handleStreamConnError(cmd Message) {

	common.Debugf("Timekeeper::handleStreamConnError %v", cmd)

	streamId := cmd.(*MsgStreamInfo).GetStreamId()
	bucket := cmd.(*MsgStreamInfo).GetBucket()

	tk.lock.Lock()
	defer tk.lock.Unlock()

	//check if bucket is active in stream
	if tk.checkBucketActiveInStream(streamId, bucket) == false {
		common.Warnf("Timekeeper::handleStreamConnError \n\tReceived ConnError for "+
			"Inactive Bucket %v Stream %v. Ignored.", bucket, streamId)
		return
	}

	state := tk.ss.streamBucketStatus[streamId][bucket]
	switch state {

	case STREAM_ACTIVE:
		vbList := cmd.(*MsgStreamInfo).GetVbList()
		bucket := cmd.(*MsgStreamInfo).GetBucket()
		tk.ss.updateVbStatus(streamId, bucket, vbList, VBS_CONN_ERROR)
		if stopCh, ok := tk.ss.streamBucketRepairStopCh[streamId][bucket]; !ok || stopCh == nil {
			tk.ss.streamBucketRepairStopCh[streamId][bucket] = make(StopChannel)
			common.Debugf("Timekeeper::handleStreamConnError \n\tRepairStream due to ConnError. "+
				"StreamId %v Bucket %v VbList %v", streamId, bucket, vbList)
			go tk.repairStream(streamId, bucket)
		}

	case STREAM_PREPARE_RECOVERY, STREAM_PREPARE_DONE, STREAM_INACTIVE:
		common.Debugf("Timekeeper::handleStreamConnError \n\tIgnore Connection Error "+
			"for StreamId %v Bucket %v State %v", streamId, bucket, state)

	default:
		common.Errorf("Timekeeper::handleStreamConnError \n\tInvalid Stream State "+
			"StreamId %v Bucket %v State %v", streamId, bucket, state)
	}

	tk.supvCmdch <- &MsgSuccess{}

}

func (tk *timekeeper) handleInitBuildDoneAck(cmd Message) {

	streamId := cmd.(*MsgTKInitBuildDone).GetStreamId()
	bucket := cmd.(*MsgTKInitBuildDone).GetBucket()

	common.Debugf("Timekeeper::handleInitBuildDoneAck StreamId %v Bucket %v",
		streamId, bucket)

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

	common.Debugf("Timekeeper::handleMergeStreamAck StreamId %v Bucket %v",
		streamId, bucket)

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleStreamRequestDone(cmd Message) {

	streamId := cmd.(*MsgStreamInfo).GetStreamId()
	bucket := cmd.(*MsgStreamInfo).GetBucket()

	common.Debugf("Timekeeper::handleStreamRequestDone StreamId %v Bucket %v",
		streamId, bucket)

	if streamId == common.INIT_STREAM {

		if !tk.ss.checkAnyFlushPending(streamId, bucket) &&
			!tk.ss.checkAnyAbortPending(streamId, bucket) {
			lastFlushedTs := tk.ss.streamBucketLastFlushedTsMap[streamId][bucket]
			tk.checkInitialBuildDone(streamId, bucket, lastFlushedTs)
		}
	}

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

	common.Debugf("Timekeeper::prepareRecovery StreamId %v Bucket %v",
		streamId, bucket)

	//change to PREPARE_RECOVERY so that
	//no more TS generation and flush happen.
	if tk.ss.streamBucketStatus[streamId][bucket] == STREAM_ACTIVE {

		common.Debugf("Timekeeper::prepareRecovery \n\t Stream %v "+
			"Bucket %v State Changed to PREPARE_RECOVERY", streamId, bucket)

		tk.ss.streamBucketStatus[streamId][bucket] = STREAM_PREPARE_RECOVERY
		//The existing mutations for which stability TS has already been generated
		//can be safely flushed before initiating recovery. This can reduce the duration
		//of recovery.
		tk.flushOrAbortInProgressTS(streamId, bucket)

	} else {

		common.Errorf("Timekeeper::prepareRecovery Invalid Prepare Recovery Request")
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
		tsHWT := getStabilityTSFromTsVbuuid(tsVbuuidHWT)

		flushTs := getStabilityTSFromTsVbuuid(ts)

		//if HWT is greater than flush in progress TS, this means the flush
		//in progress will finish.
		if tsHWT.GreaterThanEqual(flushTs) {
			common.Debugf("Timekeeper::flushOrAbortInProgressTS \n\tProcessing Flush TS %v "+
				"before recovery for bucket %v streamId %v", ts, bucket, streamId)
		} else {
			//else this flush needs to be aborted. Though some mutations may
			//arrive and flush may get completed before this abort message
			//reaches.
			common.Debugf("Timekeeper::flushOrAbortInProgressTS \n\tAborting Flush TS %v "+
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
		common.Debugf("Timekeeper::flushOrAbortInProgressTS \n\tRecovery can be initiated for "+
			"Bucket %v Stream %v", bucket, streamId)
	}

}

//checkInitialBuildDone checks if any of the index in Initial State is past its
//Build TS based on the Flush Done Message. It generates msg for Build Done
//and changes the state of the index.
func (tk *timekeeper) checkInitialBuildDone(streamId common.StreamId,
	bucket string, flushTs *common.TsVbuuid) bool {

	status := false

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
			} else {
				//check if the flushTS is greater than buildTS
				ts := getStabilityTSFromTsVbuuid(flushTs)
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
					//cleanup the index as build is done
					if _, ok := tk.indexBuildInfo[idx.InstId]; ok {
						delete(tk.indexBuildInfo, idx.InstId)
					}
				}

				common.Debugf("Timekeeper::checkInitialBuildDone \n\tInitial Build Done Index: %v "+
					"Stream: %v Bucket: %v BuildTS: %v", idx.InstId, streamId, bucket, buildInfo.buildTs)

				status = true

				//generate init build done msg
				tk.supvRespch <- &MsgTKInitBuildDone{
					mType:    TK_INIT_BUILD_DONE,
					streamId: streamId,
					buildTs:  buildInfo.buildTs,
					bucket:   bucket}

			}
		}
	}
	return status
}

//checkInitStreamReadyToMerge checks if any index in Catchup State in INIT_STREAM
//has reached past the last flushed TS of the MAINT_STREAM for this bucket.
//In such case, all indexes of the bucket can merged to MAINT_STREAM.
func (tk *timekeeper) checkInitStreamReadyToMerge(streamId common.StreamId,
	bucket string, flushTs *common.TsVbuuid) bool {

	common.Debugf("Timekeeper::checkInitStreamReadyToMerge \n\t Stream %v Bucket %v "+
		"FlushTs %v", streamId, bucket, flushTs)

	if streamId != common.INIT_STREAM {
		return false
	}

	//INIT_STREAM cannot be merged to MAINT_STREAM if its not ACTIVE
	if tk.ss.streamBucketStatus[common.MAINT_STREAM][bucket] != STREAM_ACTIVE {
		common.Debugf("Timekeeper::checkInitStreamReadyToMerge \n\tMAINT_STREAM in %v. "+
			"INIT_STREAM cannot be merged. Continue both streams.",
			tk.ss.streamBucketStatus[common.MAINT_STREAM][bucket])
		return false
	}

	//If any repair is going on, merge cannot happen
	if stopCh, ok := tk.ss.streamBucketRepairStopCh[common.MAINT_STREAM][bucket]; ok && stopCh != nil {

		common.Debugf("Timekeeper::checkInitStreamReadyToMerge \n\t MAINT_STREAM In Repair." +
			"INIT_STREAM cannot be merged. Continue both streams.")
		return false
	}

	if stopCh, ok := tk.ss.streamBucketRepairStopCh[common.INIT_STREAM][bucket]; ok && stopCh != nil {

		common.Debugf("Timekeeper::checkInitStreamReadyToMerge \n\t INIT_STREAM In Repair." +
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
			//this index can be merged to MAINT_STREAM
			bucketLastFlushedTsMap := tk.ss.streamBucketLastFlushedTsMap[common.MAINT_STREAM]
			lastFlushedTsVbuuid := bucketLastFlushedTsMap[idx.Defn.Bucket]

			//if no flush has happened yet, its good to merge
			readyToMerge := false
			var ts, lastFlushedTs Timestamp
			if lastFlushedTsVbuuid == nil {
				readyToMerge = true
			} else {
				lastFlushedTs := getStabilityTSFromTsVbuuid(lastFlushedTsVbuuid)
				ts := getStabilityTSFromTsVbuuid(flushTs)
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

				common.Debugf("Timekeeper::checkInitStreamReadyToMerge \n\tIndex Ready To Merge. "+
					"Index: %v Stream: %v Bucket: %v LastFlushTS: %v", idx.InstId, streamId,
					bucket, lastFlushedTs)

				tk.supvRespch <- &MsgTKMergeStream{
					mType:    TK_MERGE_STREAM,
					streamId: streamId,
					bucket:   bucket,
					mergeTs:  ts}

				common.Debugf("Timekeeper::checkInitStreamReadyToMerge \n\t Stream %v "+
					"Bucket %v State Changed to INACTIVE", streamId, bucket)
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

			common.Debugf("Timekeeper::checkCatchupStreamReadyToMerge \n\tBucket Ready To Merge. "+
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

	if tk.ss.checkNewTSDue(streamId, bucket) {

		tsVbuuid := tk.ss.getNextStabilityTS(streamId, bucket)

		if tk.ss.canFlushNewTS(streamId, bucket) {
			common.Debugf("Timekeeper::generateNewStabilityTS \n\tFlushing new "+
				"TS: %v Bucket: %v Stream: %v.", tsVbuuid, bucket, streamId)
			tk.ss.streamBucketFlushInProgressTsMap[streamId][bucket] = tsVbuuid
			go tk.sendNewStabilityTS(tsVbuuid, bucket, streamId)
		} else {
			//store the ts in list
			common.Debugf("Timekeeper::generateNewStabilityTS \n\tAdding TS: %v to Pending "+
				"List for Bucket: %v Stream: %v.", tsVbuuid, bucket, streamId)
			tsList := tk.ss.streamBucketTsListMap[streamId][bucket]
			tsList.PushBack(tsVbuuid)
			tk.drainQueueIfOverflow(streamId, bucket)
		}
	}
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

	tsVbuuidHWT := tk.ss.streamBucketHWTMap[streamId][bucket]
	tsHWT := getStabilityTSFromTsVbuuid(tsVbuuidHWT)

	//if there are pending TS for this bucket, send New TS
	bucketTsListMap := tk.ss.streamBucketTsListMap[streamId]
	tsList := bucketTsListMap[bucket]
	if tsList.Len() > 0 {
		e := tsList.Front()
		tsVbuuid := e.Value.(*common.TsVbuuid)
		tsList.Remove(e)
		ts := getStabilityTSFromTsVbuuid(tsVbuuid)

		if tk.ss.streamBucketStatus[streamId][bucket] == STREAM_PREPARE_RECOVERY ||
			tk.ss.streamBucketStatus[streamId][bucket] == STREAM_PREPARE_DONE {
			//if HWT is greater than flush TS, this TS can be flush
			if tsHWT.GreaterThanEqual(ts) {
				common.Debugf("Timekeeper::processPendingTS \n\tProcessing Flush TS %v "+
					"before recovery for bucket %v streamId %v", ts, bucket, streamId)
			} else {
				//empty the TSList for this bucket and stream so
				//there is no further processing for this bucket.
				common.Debugf("Timekeeper::processPendingTS \n\tCannot Flush TS %v "+
					"before recovery for bucket %v streamId %v. Clearing Ts List", ts,
					bucket, streamId)
				tsList.Init()
				return false
			}

		}
		common.Debugf("Timekeeper::processPendingTS \n\tFound Pending Stability TS Bucket: %v "+
			"Stream: %v TS: %v", bucket, streamId, ts)

		tk.ss.streamBucketFlushInProgressTsMap[streamId][bucket] = tsVbuuid
		go tk.sendNewStabilityTS(tsVbuuid, bucket, streamId)
		return true
	}

	return false
}

//sendNewStabilityTS sends the given TS to supervisor
func (tk *timekeeper) sendNewStabilityTS(ts *common.TsVbuuid, bucket string,
	streamId common.StreamId) {

	common.Debugf("Timekeeper::sendNewStabilityTS \n\tBucket: %v "+
		"Stream: %v TS: %v", bucket, streamId, ts)

	tk.supvRespch <- &MsgTKStabilityTS{ts: ts,
		bucket:   bucket,
		streamId: streamId}
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
		common.Errorf("Timekeeper::checkBucketActiveInStream "+
			"\n\tUnknown Stream: %v", streamId)
		tk.supvCmdch <- &MsgError{
			err: Error{code: ERROR_TK_UNKNOWN_STREAM,
				severity: NORMAL,
				category: TIMEKEEPER}}
		return false
	} else if status, ok := bucketStatus[bucket]; !ok || status != STREAM_ACTIVE {
		common.Errorf("Timekeeper::checkBucketActiveInStream "+
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
func getTSFromTsVbuuid(tsVbuuid *common.TsVbuuid) Timestamp {
	numVbuckets := len(tsVbuuid.Snapshots)
	ts := NewTimestamp(numVbuckets)
	for i, s := range tsVbuuid.Seqnos {
		ts[i] = Seqno(s)
	}
	return ts
}

//if there are more mutations in this queue than configured,
//drain the queue
func (tk *timekeeper) drainQueueIfOverflow(streamId common.StreamId, bucket string) {

	if !tk.ss.streamBucketDrainEnabledMap[streamId][bucket] {
		return
	}

	switch streamId {

	case common.MAINT_STREAM:
		//TODO

		//if the number of mutation are more than configured

		//if stream is in PREPARE_RECOVERY, nothing to do

		//if RECOVERY, drain on TS from queue

		//if stream is in ACTIVE state, flush the queue
	}
}

func (tk *timekeeper) initiateRecovery(streamId common.StreamId,
	bucket string) {

	common.Debugf("Timekeeper::initiateRecovery Started")

	if tk.ss.streamBucketStatus[streamId][bucket] == STREAM_PREPARE_DONE {

		restartTs := tk.ss.computeRestartTs(streamId, bucket)
		tk.ss.cleanupBucketFromStream(streamId, bucket)

		//send message for recovery
		tk.supvRespch <- &MsgRecovery{mType: INDEXER_INITIATE_RECOVERY,
			streamId:  streamId,
			bucket:    bucket,
			restartTs: restartTs}
		tk.ss.streamBucketRestartTsMap[streamId][bucket] = restartTs
		common.Debugf("Timekeeper::initiateRecovery StreamId %v "+
			"RestartTs %v", streamId, restartTs)
	} else {
		common.Errorf("Timekeeper::initiateRecovery Invalid State For %v "+
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

	common.Debugf("Timekeeper::checkBucketReadyForRecovery StreamId %v "+
		"Bucket %v", streamId, bucket)

	if tk.ss.checkAnyFlushPending(streamId, bucket) ||
		tk.ss.checkAnyAbortPending(streamId, bucket) ||
		tk.ss.streamBucketStatus[streamId][bucket] != STREAM_PREPARE_DONE {
		return false
	}

	common.Debugf("Timekeeper::checkBucketReadyForRecovery StreamId %v "+
		"Bucket %v Ready for Recovery", streamId, bucket)
	return true
}

func (tk *timekeeper) handleStreamCleanup(cmd Message) {

	common.Debugf("Timekeeper::handleStreamCleanup %v", cmd)

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()

	common.Debugf("Timekeeper::handleStreamCleanup \n\t Stream %v "+
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
	if repairTs, ok := tk.ss.getRepairTsForBucket(streamId, bucket); ok {

		respCh := make(MsgChannel)
		stopCh := tk.ss.streamBucketRepairStopCh[streamId][bucket]

		restartMsg := &MsgRestartVbuckets{streamId: streamId,
			bucket:    bucket,
			restartTs: repairTs,
			respCh:    respCh,
			stopCh:    stopCh}

		go tk.sendRestartMsg(restartMsg)

	} else {
		delete(tk.ss.streamBucketRepairStopCh[streamId], bucket)
		common.Debugf("Timekeeper::repairStream Nothing to repair for "+
			"Stream %v and Bucket %v", streamId, bucket)
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
		//success, check for more vbuckets in repair state
		tk.repairStream(streamId, bucket)

	case INDEXER_ROLLBACK:
		//if rollback msg, call initPrepareRecovery
		common.Infof("Timekeeper::sendRestartMsg Received Rollback Msg For "+
			"%v %v. Sending Init Prepare.", streamId, bucket)

		tk.supvRespch <- &MsgRecovery{mType: INDEXER_INIT_PREP_RECOVERY,
			streamId: streamId,
			bucket:   bucket}

	case KV_STREAM_REPAIR:

		common.Infof("Timekeeper::sendRestartMsg Received KV Repair Msg For "+
			"Stream %v Bucket %v. Attempting Stream Repair.", streamId, bucket)

		tk.lock.Lock()
		defer tk.lock.Unlock()

		resp := kvresp.(*MsgKVStreamRepair)

		resp.restartTs = tk.ss.computeRestartTs(streamId, bucket)
		delete(tk.ss.streamBucketRepairStopCh[streamId], bucket)

		tk.supvRespch <- resp

	default:
		if !ValidateBucket(tk.config["clusterAddr"].String(), bucket) {
			common.Errorf("Timekeeper::sendRestartMsg \n\tBucket Not Found "+
				"For Stream %v Bucket %v", streamId, bucket)

			delete(tk.ss.streamBucketRepairStopCh[streamId], bucket)

			tk.ss.streamBucketStatus[streamId][bucket] = STREAM_INACTIVE

			tk.supvRespch <- &MsgRecovery{mType: INDEXER_BUCKET_NOT_FOUND,
				streamId: streamId,
				bucket:   bucket}
		} else {
			common.Fatalf("Timekeeper::sendRestartMsg Error Response "+
				"from KV %v For Request %v. Retrying RestartVbucket.", kvresp, restartMsg)
			tk.repairStream(streamId, bucket)
		}
	}

}

func (tk *timekeeper) handleUpdateIndexInstMap(cmd Message) {
	common.Infof("Timekeeper::handleUpdateIndexInstMap %v", cmd)
	indexInstMap := cmd.(*MsgUpdateInstMap).GetIndexInstMap()
	tk.indexInstMap = common.CopyIndexInstMap(indexInstMap)
	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleUpdateIndexPartnMap(cmd Message) {
	common.Infof("Timekeeper::handleUpdateIndexPartnMap %v", cmd)
	indexPartnMap := cmd.(*MsgUpdatePartnMap).GetIndexPartnMap()
	tk.indexPartnMap = CopyIndexPartnMap(indexPartnMap)
	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleStats(cmd Message) {
	tk.supvCmdch <- &MsgSuccess{}

	statsMap := make(map[string]string)
	req := cmd.(*MsgStatsRequest)
	replych := req.GetReplyChannel()

	// Populate current KV timestamps for all buckets
	bucketTsMap := make(map[string]Timestamp)
	for _, inst := range tk.indexInstMap {
		if _, ok := bucketTsMap[inst.Defn.Bucket]; !ok {
			kvTs, err := GetCurrentKVTs(tk.config["clusterAddr"].String(),
				inst.Defn.Bucket, tk.config["numVbuckets"].Int())
			if err != nil {
				common.Errorf("Timekeeper::handleStats Error occured while obtaining KV seqnos - %v", err)
				replych <- statsMap
				return
			}

			bucketTsMap[inst.Defn.Bucket] = kvTs
		}
	}

	for _, inst := range tk.indexInstMap {
		k := fmt.Sprintf("%s:%s:num_docs_indexed", inst.Defn.Bucket, inst.Defn.Name)
		sum := uint64(0)
		flushedTs := tk.ss.streamBucketLastFlushedTsMap[inst.Stream][inst.Defn.Bucket]
		if flushedTs != nil {
			for _, seqno := range flushedTs.Seqnos {
				sum += seqno
			}
		}
		v := fmt.Sprint(sum)
		statsMap[k] = v

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
		k = fmt.Sprintf("%s:%s:num_docs_queued", inst.Defn.Bucket, inst.Defn.Name)
		v = fmt.Sprint(queued)
		statsMap[k] = v

		pending := uint64(0)
		kvTs := bucketTsMap[inst.Defn.Bucket]
		for i, seqno := range kvTs {
			recvdSeqno := uint64(0)
			if receivedTs != nil {
				recvdSeqno = receivedTs.Seqnos[i]
			}
			pending += uint64(seqno) - recvdSeqno
		}
		k = fmt.Sprintf("%s:%s:num_docs_pending", inst.Defn.Bucket, inst.Defn.Name)
		v = fmt.Sprint(pending)
		statsMap[k] = v
	}

	replych <- statsMap
}
