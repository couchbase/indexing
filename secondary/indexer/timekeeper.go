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
	"container/list"
	"github.com/couchbase/indexing/secondary/common"
)

//Timekeeper manages the Stability Timestamp Generation and also
//keeps track of the HWTimestamp for each bucket
type Timekeeper interface {
}

type timekeeper struct {
	supvCmdch  MsgChannel //supervisor sends commands on this channel
	supvRespch MsgChannel //channel to send any async message to supervisor

	streamBucketHWTMap       map[common.StreamId]BucketHWTMap
	streamBucketSyncCountMap map[common.StreamId]BucketSyncCountMap
	streamBucketNewTsReqdMap map[common.StreamId]BucketNewTsReqdMap

	streamBucketTsListMap            map[common.StreamId]BucketTsListMap
	streamBucketFlushInProgressTsMap map[common.StreamId]BucketFlushInProgressTsMap
	streamBucketAbortInProgressMap   map[common.StreamId]BucketAbortInProgressMap

	streamBucketLastFlushedTsMap map[common.StreamId]BucketLastFlushedTsMap
	streamBucketRestartTsMap     map[common.StreamId]BucketRestartTsMap
	streamBucketFlushEnabledMap  map[common.StreamId]BucketFlushEnabledMap
	streamBucketDrainEnabledMap  map[common.StreamId]BucketDrainEnabledMap

	streamBucketStreamBeginMap map[common.StreamId]BucketStreamBeginMap
	streamState                map[common.StreamId]StreamState

	//map of indexInstId to its Initial Build Info
	indexBuildInfo map[common.IndexInstId]*InitialBuildInfo
}

type BucketHWTMap map[string]*common.TsVbuuid
type BucketLastFlushedTsMap map[string]*common.TsVbuuid
type BucketRestartTsMap map[string]*common.TsVbuuid
type BucketSyncCountMap map[string]uint64
type BucketNewTsReqdMap map[string]bool

type BucketTsListMap map[string]*list.List
type BucketFlushInProgressTsMap map[string]*common.TsVbuuid
type BucketAbortInProgressMap map[string]bool
type BucketFlushEnabledMap map[string]bool
type BucketDrainEnabledMap map[string]bool

//Each location in TS represents the state of StreamBegin.
//0 denotes no StreamBegin. 1 denotes StreamBegin.
//2 denotes StreamEnd has been received.
type BucketStreamBeginMap map[string]Timestamp

type InitialBuildInfo struct {
	indexInst common.IndexInst
	buildTs   Timestamp
	respCh    MsgChannel
}

//NewTimekeeper returns an instance of timekeeper or err message.
//It listens on supvCmdch for command and every command is followed
//by a synchronous response of the supvCmdch.
//Any async response to supervisor is sent to supvRespch.
//If supvCmdch get closed, storageMgr will shut itself down.
func NewTimekeeper(supvCmdch MsgChannel, supvRespch MsgChannel) (
	Timekeeper, Message) {

	//Init the timekeeper struct
	tk := &timekeeper{
		supvCmdch:                        supvCmdch,
		supvRespch:                       supvRespch,
		streamBucketHWTMap:               make(map[common.StreamId]BucketHWTMap),
		streamBucketSyncCountMap:         make(map[common.StreamId]BucketSyncCountMap),
		streamBucketNewTsReqdMap:         make(map[common.StreamId]BucketNewTsReqdMap),
		streamBucketTsListMap:            make(map[common.StreamId]BucketTsListMap),
		streamBucketFlushInProgressTsMap: make(map[common.StreamId]BucketFlushInProgressTsMap),
		streamBucketAbortInProgressMap:   make(map[common.StreamId]BucketAbortInProgressMap),
		streamBucketLastFlushedTsMap:     make(map[common.StreamId]BucketLastFlushedTsMap),
		streamBucketRestartTsMap:         make(map[common.StreamId]BucketRestartTsMap),
		streamBucketFlushEnabledMap:      make(map[common.StreamId]BucketFlushEnabledMap),
		streamBucketDrainEnabledMap:      make(map[common.StreamId]BucketDrainEnabledMap),
		streamBucketStreamBeginMap:       make(map[common.StreamId]BucketStreamBeginMap),
		streamState:                      make(map[common.StreamId]StreamState),
		indexBuildInfo:                   make(map[common.IndexInstId]*InitialBuildInfo),
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

	case STREAM_READER_SYNC:
		tk.handleSync(cmd)

	case OPEN_STREAM:
		tk.handleStreamOpen(cmd)

	case ADD_INDEX_LIST_TO_STREAM:
		tk.handleAddIndextoStream(cmd)

	case REMOVE_INDEX_LIST_FROM_STREAM:
		tk.handleRemoveIndexFromStream(cmd)

	case CLOSE_STREAM:
		tk.handleStreamClose(cmd)

	case CLEANUP_STREAM:
		tk.handleStreamCleanup(cmd)

	case MUT_MGR_FLUSH_DONE:
		tk.handleFlushDone(cmd)

	case MUT_MGR_ABORT_DONE:
		tk.handleFlushAbortDone(cmd)

	case TK_ENABLE_FLUSH:
		tk.handleFlushStateChange(cmd)

	case TK_DISABLE_FLUSH:
		tk.handleFlushStateChange(cmd)

	case STREAM_READER_SNAPSHOT_MARKER:
		tk.handleSnapshotMarker(cmd)

	case TK_GET_BUCKET_HWT:
		tk.handleGetBucketHWT(cmd)

	case STREAM_READER_STREAM_BEGIN:
		tk.handleStreamBegin(cmd)

	case STREAM_READER_STREAM_END:
		tk.handleStreamEnd(cmd)

	case STREAM_READER_CONN_ERROR:
		tk.handleStreamConnError(cmd)

	default:
		common.Errorf("Timekeeper::handleSupvervisorCommands "+
			"Received Unknown Command %v", cmd)

	}

}

func (tk *timekeeper) handleStreamOpen(cmd Message) {

	common.Infof("Timekeeper::handleStreamOpen %v", cmd)

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()

	//if stream is INACTIVE, move it to RECOVERY
	if tk.streamState[streamId] == STREAM_INACTIVE {

		switch streamId {
		case common.MAINT_STREAM:
			common.Debugf("Timekeeper::handleStreamOpen \n\t Stream %v "+
				"State Changed to RECOVERY", streamId)
			tk.streamState[streamId] = STREAM_RECOVERY

		case common.CATCHUP_STREAM, common.INIT_STREAM:
			common.Debugf("Timekeeper::handleStreamOpen \n\t Stream %v "+
				"State Changed to ACTIVE", streamId)
			tk.streamState[streamId] = STREAM_ACTIVE
		}

	} else {

		//init all internal maps for this stream
		bucketHWTMap := make(BucketHWTMap)
		tk.streamBucketHWTMap[streamId] = bucketHWTMap

		bucketSyncCountMap := make(BucketSyncCountMap)
		tk.streamBucketSyncCountMap[streamId] = bucketSyncCountMap

		bucketNewTsReqdMap := make(BucketNewTsReqdMap)
		tk.streamBucketNewTsReqdMap[streamId] = bucketNewTsReqdMap

		bucketTsListMap := make(BucketTsListMap)
		tk.streamBucketTsListMap[streamId] = bucketTsListMap

		bucketFlushInProgressTsMap := make(BucketFlushInProgressTsMap)
		tk.streamBucketFlushInProgressTsMap[streamId] = bucketFlushInProgressTsMap

		bucketAbortInProgressMap := make(BucketAbortInProgressMap)
		tk.streamBucketAbortInProgressMap[streamId] = bucketAbortInProgressMap

		bucketLastFlushedTsMap := make(BucketLastFlushedTsMap)
		tk.streamBucketLastFlushedTsMap[streamId] = bucketLastFlushedTsMap

		bucketFlushEnabledMap := make(BucketFlushEnabledMap)
		tk.streamBucketFlushEnabledMap[streamId] = bucketFlushEnabledMap

		bucketDrainEnabledMap := make(BucketDrainEnabledMap)
		tk.streamBucketDrainEnabledMap[streamId] = bucketDrainEnabledMap

		bucketStreamBeginMap := make(BucketStreamBeginMap)
		tk.streamBucketStreamBeginMap[streamId] = bucketStreamBeginMap

		common.Debugf("Timekeeper::handleStreamOpen \n\t Stream %v "+
			"State Changed to ACTIVE", streamId)
		tk.streamState[streamId] = STREAM_ACTIVE

		//add the new indexes to internal maps
		tk.addIndextoStream(cmd)
	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleStreamClose(cmd Message) {

	common.Debugf("Timekeeper::handleStreamClose %v", cmd)

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()

	//if stream is in PREPARE_RECOVERY, check and init RECOVERY
	if tk.streamState[streamId] == STREAM_PREPARE_RECOVERY {

		common.Debugf("Timekeeper::handleStreamClose \n\t Stream %v "+
			"State Changed to INACTIVE", streamId)
		tk.streamState[streamId] = STREAM_INACTIVE

		switch streamId {
		case common.MAINT_STREAM, common.INIT_STREAM:
			if tk.checkStreamReadyForRecovery(streamId) {
				tk.initiateRecovery(streamId)
			}
		}
	} else {

		//delete this stream from internal maps
		delete(tk.streamBucketHWTMap, streamId)
		delete(tk.streamBucketSyncCountMap, streamId)
		delete(tk.streamBucketNewTsReqdMap, streamId)
		delete(tk.streamBucketTsListMap, streamId)
		delete(tk.streamBucketFlushInProgressTsMap, streamId)
		delete(tk.streamBucketAbortInProgressMap, streamId)
		delete(tk.streamBucketLastFlushedTsMap, streamId)
		delete(tk.streamBucketFlushEnabledMap, streamId)
		delete(tk.streamBucketDrainEnabledMap, streamId)
		delete(tk.streamBucketStreamBeginMap, streamId)
		delete(tk.streamState, streamId)

		//delete indexes from stream
		tk.removeIndexFromStream(cmd)
	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleAddIndextoStream(cmd Message) {

	common.Infof("Timekeeper::handleAddIndextoStream %v", cmd)

	tk.addIndextoStream(cmd)

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) addIndextoStream(cmd Message) {

	respCh := cmd.(*MsgStreamUpdate).GetResponseChannel()
	indexInstList := cmd.(*MsgStreamUpdate).GetIndexList()
	buildTs := cmd.(*MsgStreamUpdate).GetTimestamp()

	//If the index is in INITIAL state, store it in initialbuild map
	for _, idx := range indexInstList {
		if idx.State == common.INDEX_STATE_INITIAL {
			tk.indexBuildInfo[idx.InstId] = &InitialBuildInfo{
				indexInst: idx,
				buildTs:   buildTs,
				respCh:    respCh}
		}
	}

}

func (tk *timekeeper) handleRemoveIndexFromStream(cmd Message) {

	common.Infof("Timekeeper::handleRemoveIndexFromStream %v", cmd)

	tk.removeIndexFromStream(cmd)

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) removeIndexFromStream(cmd Message) {

	indexInstList := cmd.(*MsgStreamUpdate).GetIndexList()

	for _, idx := range indexInstList {
		//if the index in internal map, delete it
		if _, ok := tk.indexBuildInfo[idx.InstId]; ok {
			delete(tk.indexBuildInfo, idx.InstId)
		}
	}
}

func (tk *timekeeper) handleSync(cmd Message) {

	common.Tracef("Timekeeper::handleSync %v", cmd)

	streamId := cmd.(*MsgStream).GetStreamId()
	meta := cmd.(*MsgStream).GetMutationMeta()

	//check if stream is valid
	if tk.checkStreamValid(streamId) == false {
		common.Warnf("Timekeeper::handleSync \n\tReceived Sync for "+
			"Invalid Stream %v. Ignored.", streamId)
		return
	}

	if tk.streamState[streamId] == STREAM_PREPARE_RECOVERY ||
		tk.streamState[streamId] == STREAM_INACTIVE {
		common.Debugf("Timekeeper::handleSync \n\tIgnoring Sync Marker "+
			"for StreamId %v. Current State %v", streamId, tk.streamState[streamId])
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	if _, ok := tk.streamBucketHWTMap[streamId][meta.bucket]; !ok {
		common.Debugf("Timekeeper::handleSync \n\tIgnoring Sync Marker "+
			"for StreamId %v Bucket %v. Bucket Not Found.", streamId, meta.bucket)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	//update HWT for the bucket
	tk.updateHWT(cmd)

	//update Sync Count for the bucket
	tk.incrSyncCount(streamId, meta.bucket)

	tk.generateNewStabilityTS(streamId, meta.bucket)

	tk.supvCmdch <- &MsgSuccess{}

}

func (tk *timekeeper) handleFlushDone(cmd Message) {

	streamId := cmd.(*MsgMutMgrFlushDone).GetStreamId()
	bucket := cmd.(*MsgMutMgrFlushDone).GetBucket()

	bucketLastFlushedTsMap := tk.streamBucketLastFlushedTsMap[streamId]
	bucketFlushInProgressTsMap := tk.streamBucketFlushInProgressTsMap[streamId]

	//store the last flushed TS
	bucketLastFlushedTsMap[bucket] = bucketFlushInProgressTsMap[bucket]
	//update internal map to reflect flush is done
	bucketFlushInProgressTsMap[bucket] = nil

	switch streamId {

	case common.MAINT_STREAM:
		tk.handleFlushDoneMaintStream(cmd)

	case common.INIT_STREAM:
		tk.handleFlushDoneInitStream(cmd)

	case common.CATCHUP_STREAM:
		tk.handleFlushDoneCatchupStream(cmd)

	default:
		common.Errorf("Timekeeper::handleFlushDone \n\tInvalid StreamId %v ", streamId)
	}

}

func (tk *timekeeper) handleFlushDoneMaintStream(cmd Message) {

	common.Debugf("Timekeeper::handleFlushDoneMaintStream %v", cmd)

	streamId := cmd.(*MsgMutMgrFlushDone).GetStreamId()
	bucket := cmd.(*MsgMutMgrFlushDone).GetBucket()

	state := tk.streamState[streamId]

	switch state {

	case STREAM_ACTIVE, STREAM_RECOVERY:

		//check if any of the initial build index is past its Build TS.
		//Generate msg for Build Done and change the state of the index.
		tk.checkInitialBuildDone(cmd)

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
			if tk.checkStreamReadyForRecovery(streamId) {
				tk.initiateRecovery(streamId)
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

	common.Debugf("Timekeeper::handleFlushDoneCatchupStream %v", cmd)

	streamId := cmd.(*MsgMutMgrFlushDone).GetStreamId()
	bucket := cmd.(*MsgMutMgrFlushDone).GetBucket()

	state := tk.streamState[streamId]

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
			if tk.checkStreamReadyForRecovery(streamId) {
				tk.initiateRecovery(streamId)
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

	common.Debugf("Timekeeper::handleFlushDoneInitStream %v", cmd)

	streamId := cmd.(*MsgMutMgrFlushDone).GetStreamId()
	bucket := cmd.(*MsgMutMgrFlushDone).GetBucket()

	state := tk.streamState[streamId]

	switch state {

	case STREAM_ACTIVE:

		//check if any of the initial build index is past its Build TS.
		//Generate msg for Build Done and change the state of the index.
		tk.checkInitialBuildDone(cmd)

		//if flush is for INIT_STREAM, check if any index in CATCHUP has reached
		//past the last flushed TS of the MAINT_STREAM for this bucket.
		//In such case, all indexes of the bucket can merged to MAINT_STREAM.
		if tk.checkInitStreamReadyToMerge(cmd) {
			//if stream is ready to merge, further STREAM_ACTIVE processing is
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
			if tk.checkStreamReadyForRecovery(streamId) {
				tk.initiateRecovery(streamId)
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

	common.Debugf("Timekeeper::handleFlushAbortDone %v", cmd)

	streamId := cmd.(*MsgMutMgrFlushDone).GetStreamId()
	bucket := cmd.(*MsgMutMgrFlushDone).GetBucket()

	state := tk.streamState[streamId]

	switch state {

	case STREAM_ACTIVE:
		common.Errorf("Timekeeper::handleFlushAbortDone Unexpected Flush Abort "+
			"Received for Active StreamId %v.", streamId)

	case STREAM_PREPARE_RECOVERY:
		bucketFlushInProgressTsMap := tk.streamBucketFlushInProgressTsMap[streamId]
		//if there is flush in progress, mark it as done
		if bucketFlushInProgressTsMap[bucket] != nil {
			bucketFlushInProgressTsMap[bucket] = nil
		}

		//update abort status and initiate recovery
		bucketAbortInProgressMap := tk.streamBucketAbortInProgressMap[streamId]
		bucketAbortInProgressMap[bucket] = false

		//if flush for all buckets is done and no abort is in progress,
		//recovery can be initiated
		if tk.checkStreamReadyForRecovery(streamId) {
			tk.initiateRecovery(streamId)
		}

	case STREAM_RECOVERY:
		common.Errorf("Timekeeper::handleFlushAbortDone Unexpected Flush Abort "+
			"Received for StreamId %v. State %v.", streamId, state)

	case STREAM_INACTIVE:
		common.Errorf("Timekeeper::handleFlushAbortDone Unexpected Flush Abort "+
			"Received for Inactive StreamId %v.", streamId)

	default:
		common.Errorf("Timekeeper::handleFlushAbortDone Invalid Stream State %v "+
			"for StreamId %v.", state, streamId)

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

	switch t {

	case TK_ENABLE_FLUSH:

		//if bucket is empty, enable for all buckets in the stream
		bucketFlushEnabledMap := tk.streamBucketFlushEnabledMap[streamId]
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

		if tk.streamState[streamId] == STREAM_RECOVERY {
			common.Debugf("Timekeeper::handleFlushStateChange \n\t Stream %v "+
				"State Changed to ACTIVE", streamId)
			tk.streamState[streamId] = STREAM_ACTIVE
		}

	case TK_DISABLE_FLUSH:

		//TODO What about the flush which is already in progress
		//if bucket is empty, disable for all buckets in the stream
		bucketFlushEnabledMap := tk.streamBucketFlushEnabledMap[streamId]
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

	common.Debugf("Timekeeper::handleSnapshotMarker %v", cmd)

	streamId := cmd.(*MsgStream).GetStreamId()
	meta := cmd.(*MsgStream).GetMutationMeta()

	//check if stream is valid
	if tk.checkStreamValid(streamId) == false {
		common.Warnf("Timekeeper::handleSnapshotMarker \n\tReceived Snapshot Marker for "+
			"Invalid Stream %v. Ignored.", streamId)
		return
	}

	if tk.streamState[streamId] == STREAM_PREPARE_RECOVERY ||
		tk.streamState[streamId] == STREAM_INACTIVE {
		common.Debugf("Timekeeper::handleSnapshotMarker \n\tIgnoring Snapshot Marker "+
			"for StreamId %v State %v", streamId, tk.streamState[streamId])
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	if _, ok := tk.streamBucketHWTMap[streamId][meta.bucket]; !ok {
		common.Debugf("Timekeeper::handleSnapshotMarker \n\tIgnoring Snapshot Marker "+
			"for StreamId %v Bucket %v", streamId, meta.bucket)
		tk.supvCmdch <- &MsgSuccess{}
		return
	}

	//only SnapshotType 0 and 1 are processed for now,
	//UPR can send other special snapshot markers, which
	//need to be ignored.
	//TODO: Use the same logic for processing the snapshot types
	//as used by view-engine
	snapshot := cmd.(*MsgStream).GetSnapshot()
	if snapshot.snapType == 0 || snapshot.snapType == 1 ||
		snapshot.snapType == 2 {

		//update the snapshot seqno in internal map
		ts := tk.streamBucketHWTMap[streamId][meta.bucket]
		ts.Snapshots[meta.vbucket][0] = snapshot.start
		ts.Snapshots[meta.vbucket][1] = snapshot.end

		tk.streamBucketNewTsReqdMap[streamId][meta.bucket] = true
		common.Tracef("Timekeeper::handleSnapshotMarker \n\tUpdated TS %v", ts)
	} else {
		common.Debugf("Timekeeper::handleSnapshotMarker \n\tIgnoring Snapshot Marker. "+
			"Unknown Type %v.", snapshot.snapType)
	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleGetBucketHWT(cmd Message) {

	common.Debugf("Timekeeper::handleGetBucketHWT %v", cmd)

	streamId := cmd.(*MsgTKGetBucketHWT).GetStreamId()
	bucket := cmd.(*MsgTKGetBucketHWT).GetBucket()

	//set the return ts to nil
	msg := cmd.(*MsgTKGetBucketHWT)
	msg.ts = nil

	if bucketHWTMap, ok := tk.streamBucketHWTMap[streamId]; ok {
		if ts, ok := bucketHWTMap[bucket]; ok {
			newTs := copyTsVbuuid(bucket, ts)
			msg.ts = newTs
		}
	}
	tk.supvCmdch <- msg
}

func (tk *timekeeper) handleStreamBegin(cmd Message) {

	common.Debugf("Timekeeper::handleStreamBegin %v", cmd)

	streamId := cmd.(*MsgStream).GetStreamId()
	meta := cmd.(*MsgStream).GetMutationMeta()

	//check if stream is valid
	if tk.checkStreamValid(streamId) == false {
		common.Warnf("Timekeeper::handleStreamBegin \n\tReceived Stream Begin for "+
			"Invalid Stream %v. Ignored.", streamId)
		return
	}

	switch streamId {

	case common.MAINT_STREAM:

		switch tk.streamState[streamId] {

		case STREAM_ACTIVE, STREAM_RECOVERY:
			//update the HWT of this stream and bucket with the vbuuid
			bucketHWTMap := tk.streamBucketHWTMap[streamId]

			if _, ok := bucketHWTMap[meta.bucket]; !ok {
				tk.initInternalStreamState(streamId, meta.bucket)
			}

			//TODO: Check if this is duplicate StreamBegin. Treat it as StreamEnd.
			ts := bucketHWTMap[meta.bucket]
			ts.Vbuuids[meta.vbucket] = uint64(meta.vbuuid)

			sb := tk.streamBucketStreamBeginMap[streamId][meta.bucket]
			sb[meta.vbucket] = 1

			//disable flush for MAINT_STREAM in Recovery
			if tk.streamState[streamId] == STREAM_RECOVERY {
				tk.setHWTFromRestartTs(streamId, meta.bucket)
				tk.disableStreamFlush(streamId)
			}

		case STREAM_PREPARE_RECOVERY, STREAM_INACTIVE:
			//ignore stream end in prepare_recovery
			common.Debugf("Timekeeper::handleStreamBegin \n\tIgnore StreamBegin for StreamId %v "+
				"State %v MutationMeta %v", streamId, tk.streamState[streamId], meta)

		default:
			common.Errorf("Timekeeper::handleStreamBegin \n\tInvalid Stream State StreamId %v "+
				"State %v", streamId, tk.streamState[streamId])
		}

	case common.INIT_STREAM:

		switch tk.streamState[streamId] {

		case STREAM_ACTIVE:
			//update the HWT of this stream and bucket with the vbuuid
			bucketHWTMap := tk.streamBucketHWTMap[streamId]

			if _, ok := bucketHWTMap[meta.bucket]; !ok {
				tk.initInternalStreamState(streamId, meta.bucket)
			}

			//TODO: Check if this is duplicate StreamBegin. Treat it as StreamEnd.
			ts := bucketHWTMap[meta.bucket]
			ts.Vbuuids[meta.vbucket] = uint64(meta.vbuuid)

			sb := tk.streamBucketStreamBeginMap[streamId][meta.bucket]
			sb[meta.vbucket] = 1

		default:
			common.Errorf("Timekeeper::handleStreamBegin \n\tInvalid Stream State StreamId %v "+
				"State %v", streamId, tk.streamState[streamId])
		}

	case common.CATCHUP_STREAM:

		switch tk.streamState[streamId] {

		case STREAM_ACTIVE:
			//update the HWT of this stream and bucket with the vbuuid
			bucketHWTMap := tk.streamBucketHWTMap[streamId]

			if _, ok := bucketHWTMap[meta.bucket]; !ok {
				tk.initInternalStreamState(streamId, meta.bucket)
			}

			//TODO: Check if this is duplicate StreamBegin. Treat it as StreamEnd.
			ts := bucketHWTMap[meta.bucket]
			ts.Vbuuids[meta.vbucket] = uint64(meta.vbuuid)

			sb := tk.streamBucketStreamBeginMap[streamId][meta.bucket]
			sb[meta.vbucket] = 1
			tk.setHWTFromRestartTs(streamId, meta.bucket)

		case STREAM_PREPARE_RECOVERY, STREAM_INACTIVE, STREAM_RECOVERY:
			//ignore stream end in prepare_recovery
			common.Debugf("Timekeeper::handleStreamBegin \n\tIgnore StreamBegin for StreamId %v "+
				"State %v MutationMeta %v", streamId, tk.streamState[streamId], meta)

		default:
			common.Errorf("Timekeeper::handleStreamBegin \n\tInvalid Stream State StreamId %v "+
				"State %v", streamId, tk.streamState[streamId])
		}

	default:
		common.Errorf("Timekeeper::handleStreamBegin \n\tInvalid StreamId %v ", streamId)

	}

	tk.supvCmdch <- &MsgSuccess{}

}

func (tk *timekeeper) handleStreamEnd(cmd Message) {

	common.Debugf("Timekeeper::handleStreamEnd %v", cmd)

	streamId := cmd.(*MsgStream).GetStreamId()
	meta := cmd.(*MsgStream).GetMutationMeta()

	//check if stream is valid
	if tk.checkStreamValid(streamId) == false {
		common.Warnf("Timekeeper::handleStreamEnd \n\tReceived Stream End for "+
			"Invalid Stream %v. Ignored.", streamId)
		return
	}

	switch streamId {

	case common.MAINT_STREAM:

		switch tk.streamState[streamId] {

		case STREAM_ACTIVE, STREAM_RECOVERY:
			ts := tk.streamBucketStreamBeginMap[streamId][meta.bucket]
			ts[meta.vbucket] = 2

			tk.prepareRecovery(streamId)

		case STREAM_PREPARE_RECOVERY, STREAM_INACTIVE:
			//ignore stream end in prepare_recovery
			common.Debugf("Timekeeper::handleStreamEnd \n\tIgnore StreamEnd for StreamId %v "+
				"State %v MutationMeta %v", streamId, tk.streamState[streamId], meta)

		default:
			common.Errorf("Timekeeper::handleStreamEnd \n\tInvalid Stream State StreamId %v "+
				"State %v", streamId, tk.streamState[streamId])
		}

	case common.INIT_STREAM, common.CATCHUP_STREAM:

		switch tk.streamState[streamId] {

		case STREAM_ACTIVE:
			ts := tk.streamBucketStreamBeginMap[streamId][meta.bucket]
			ts[meta.vbucket] = 2

			tk.prepareRecovery(streamId)

		case STREAM_PREPARE_RECOVERY, STREAM_INACTIVE:
			//ignore stream end in prepare_recovery
			common.Debugf("Timekeeper::handleStreamEnd \n\tIgnore StreamEnd for StreamId %v "+
				"State %v MutationMeta %v", streamId, tk.streamState[streamId], meta)

		default:
			common.Errorf("Timekeeper::handleStreamEnd \n\tInvalid Stream State StreamId %v "+
				"State %v", streamId, tk.streamState[streamId])
		}

	default:
		common.Errorf("Timekeeper::handleStreamEnd \n\tInvalid StreamId %v ", streamId)

	}

	tk.supvCmdch <- &MsgSuccess{}

}
func (tk *timekeeper) handleStreamConnError(cmd Message) {

	common.Debugf("Timekeeper::handleStreamConnError %v", cmd)

	streamId := cmd.(*MsgStreamInfo).GetStreamId()

	//check if stream is valid
	if tk.checkStreamValid(streamId) == false {
		common.Warnf("Timekeeper::handleStreamConnError \n\tReceived ConnError for "+
			"Invalid Stream %v. Ignored.", streamId)
		return
	}

	switch streamId {

	case common.MAINT_STREAM:

		switch tk.streamState[streamId] {

		case STREAM_ACTIVE, STREAM_RECOVERY:
			tk.prepareRecovery(streamId)

		case STREAM_PREPARE_RECOVERY, STREAM_INACTIVE:
			//ignore stream end in prepare_recovery
			common.Debugf("Timekeeper::handleStreamConnError \n\tIgnore Connection Error "+
				"for StreamId %v State %v", streamId, tk.streamState[streamId])

		default:
			common.Errorf("Timekeeper::handleStreamConnError \n\tInvalid Stream State StreamId %v "+
				"State %v", streamId, tk.streamState[streamId])
		}

	case common.INIT_STREAM, common.CATCHUP_STREAM:

		switch tk.streamState[streamId] {

		case STREAM_ACTIVE:
			tk.prepareRecovery(streamId)

		case STREAM_PREPARE_RECOVERY, STREAM_INACTIVE:
			//ignore stream end in prepare_recovery
			common.Debugf("Timekeeper::handleStreamConnError \n\tIgnore Connection Error "+
				"for StreamId %v State %v", streamId, tk.streamState[streamId])

		default:
			common.Errorf("Timekeeper::handleStreamConnError \n\tInvalid Stream State StreamId %v "+
				"State %v", streamId, tk.streamState[streamId])
		}

	default:
		common.Errorf("Timekeeper::handleStreamConnError \n\tInvalid StreamId %v ", streamId)

	}

	tk.supvCmdch <- &MsgSuccess{}

}

func (tk *timekeeper) prepareRecovery(streamId common.StreamId) bool {

	common.Debugf("Timekeeper::prepareRecovery StreamId %v", streamId)

	//change to PREPARE_RECOVERY so that
	//no more TS generation and flush happen.
	switch streamId {

	case common.MAINT_STREAM, common.CATCHUP_STREAM:

		if tk.streamState[common.MAINT_STREAM] == STREAM_RECOVERY {

			common.Debugf("Timekeeper::prepareRecovery \n\t Stream %v and %v "+
				"State Changed to PREPARE_RECOVERY", common.MAINT_STREAM,
				common.CATCHUP_STREAM)
			tk.streamState[common.MAINT_STREAM] = STREAM_PREPARE_RECOVERY
			tk.streamState[common.CATCHUP_STREAM] = STREAM_PREPARE_RECOVERY
			//The existing mutations for which stability TS has already been generated
			//can be safely flushed before initiating recovery. This can reduce the duration
			//of recovery.
			tk.flushOrAbortInProgressTS(common.MAINT_STREAM)
			tk.flushOrAbortInProgressTS(common.CATCHUP_STREAM)

		} else if tk.streamState[common.MAINT_STREAM] == STREAM_ACTIVE {

			common.Debugf("Timekeeper::prepareRecovery \n\t Stream %v "+
				"State Changed to PREPARE_RECOVERY", common.MAINT_STREAM)
			tk.streamState[common.MAINT_STREAM] = STREAM_PREPARE_RECOVERY
			//The existing mutations for which stability TS has already been generated
			//can be safely flushed before initiating recovery. This can reduce the duration
			//of recovery.
			tk.flushOrAbortInProgressTS(common.MAINT_STREAM)

		} else {

			common.Errorf("Timekeeper::prepareRecovery Invalid Prepare Recovery Request")
			return false

		}

		//send message to stop running stream
		tk.supvRespch <- &MsgRecovery{mType: INDEXER_PREPARE_RECOVERY,
			streamId: common.MAINT_STREAM}

	case common.INIT_STREAM:

		common.Debugf("Timekeeper::prepareRecovery \n\t Stream %v "+
			"State Changed to PREPARE_RECOVERY", streamId)
		tk.streamState[streamId] = STREAM_PREPARE_RECOVERY

		//The existing mutations for which stability TS has already been generated
		//can be safely flushed before initiating recovery. This can reduce the duration
		//of recovery.
		tk.flushOrAbortInProgressTS(common.MAINT_STREAM)

		//send message to stop running stream
		tk.supvRespch <- &MsgRecovery{mType: INDEXER_PREPARE_RECOVERY,
			streamId: streamId}
	}

	return true

}

func (tk *timekeeper) flushOrAbortInProgressTS(streamId common.StreamId) {

	bucketFlushInProgressTsMap := tk.streamBucketFlushInProgressTsMap[streamId]
	for bucket, ts := range bucketFlushInProgressTsMap {

		if ts != nil {
			//If Flush is in progress for the bucket, check if all the mutations
			//required for this flush have already been received. If not, this flush
			//needs to be aborted. Pending TS for the bucket can be discarded and
			//all remaining mutations in queue can be drained.
			//Stream can then be moved to RECOVERY

			//TODO this check is best effort right now as there may be more
			//mutations in the queue than what we have received the SYNC messages
			//for. We may end up discarding one TS worth of mutations unnecessarily,
			//but that is fine for now.
			tsVbuuidHWT := tk.streamBucketHWTMap[streamId][bucket]
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

				//empty the TSList for this bucket and stream so
				//there is no further processing for this bucket.
				tsList := tk.streamBucketTsListMap[streamId][bucket]
				tsList.Init()
			}

		} else {

			//for the buckets where no flush is in progress, it implies
			//there are no pending TS. So nothing needs to be done and
			//recovery can be initiated.
			common.Debugf("Timekeeper::flushOrAbortInProgressTS \n\tRecovery can be initiated for "+
				"Bucket %v Stream %v", bucket, streamId)
		}

	}
}

//checkInitialBuildDone checks if any of the index in Initial State is past its
//Build TS based on the Flush Done Message. It generates msg for Build Done
//and changes the state of the index.
func (tk *timekeeper) checkInitialBuildDone(cmd Message) {

	streamId := cmd.(*MsgMutMgrFlushDone).GetStreamId()
	bucket := cmd.(*MsgMutMgrFlushDone).GetBucket()
	flushTs := cmd.(*MsgMutMgrFlushDone).GetTS()

	for _, buildInfo := range tk.indexBuildInfo {
		//if index belongs to the flushed bucket and in INITIAL state
		idx := buildInfo.indexInst
		if idx.Defn.Bucket == bucket &&
			idx.Stream == streamId &&
			idx.State == common.INDEX_STATE_INITIAL {
			//check if the flushTS is greater than buildTS
			ts := getStabilityTSFromTsVbuuid(flushTs)
			if ts.GreaterThanEqual(buildInfo.buildTs) {

				//change all indexes of this bucket to Catchup state if the flush
				//is for INIT_STREAM
				if streamId == common.INIT_STREAM {
					tk.changeIndexStateForBucket(bucket, common.INDEX_STATE_CATCHUP)
				}

				common.Debugf("Timekeeper::checkInitialBuildDone \n\tInitial Build Done Index: %v "+
					"Stream: %v Bucket: %v BuildTS: %v", idx.InstId, streamId, bucket, buildInfo.buildTs)

				//generate init build done msg
				tk.supvRespch <- &MsgTKInitBuildDone{
					streamId: streamId,
					buildTs:  buildInfo.buildTs,
					bucket:   bucket,
					respCh:   buildInfo.respCh}

			}
		}
	}
}

//checkInitStreamReadyToMerge checks if any index in Catchup State in INIT_STREAM
//has reached past the last flushed TS of the MAINT_STREAM for this bucket.
//In such case, all indexes of the bucket can merged to MAINT_STREAM.
func (tk *timekeeper) checkInitStreamReadyToMerge(cmd Message) bool {

	streamId := cmd.(*MsgMutMgrFlushDone).GetStreamId()
	bucket := cmd.(*MsgMutMgrFlushDone).GetBucket()
	flushTs := cmd.(*MsgMutMgrFlushDone).GetTS()

	//INIT_STREAM cannot be merged to MAINT_STREAM in recovery
	if tk.streamState[common.MAINT_STREAM] == STREAM_RECOVERY ||
		tk.streamState[common.MAINT_STREAM] == STREAM_PREPARE_RECOVERY {
		common.Debugf("Timekeeper::checkInitStreamReadyToMerge \n\tMAINT_STREAM in Recovery. " +
			"INIT_STREAM cannot be merged. Continue both streams.")
		return false
	}

	if streamId == common.INIT_STREAM {

		for _, buildInfo := range tk.indexBuildInfo {
			//if index belongs to the flushed bucket and in CATCHUP state
			idx := buildInfo.indexInst
			if idx.Defn.Bucket == bucket &&
				idx.State == common.INDEX_STATE_CATCHUP {

				//if the flushTs is past the lastFlushTs of this bucket in MAINT_STREAM,
				//this index can be merged to MAINT_STREAM
				bucketLastFlushedTsMap := tk.streamBucketLastFlushedTsMap[common.MAINT_STREAM]
				lastFlushedTsVbuuid := bucketLastFlushedTsMap[idx.Defn.Bucket]
				lastFlushedTs := getStabilityTSFromTsVbuuid(lastFlushedTsVbuuid)

				ts := getStabilityTSFromTsVbuuid(flushTs)
				if ts.GreaterThanEqual(lastFlushedTs) {
					//disable flush for MAINT_STREAM for this bucket, so it doesn't
					//move ahead till merge is complete
					bucketFlushEnabledMap := tk.streamBucketFlushEnabledMap[common.MAINT_STREAM]
					bucketFlushEnabledMap[idx.Defn.Bucket] = false

					//change state of all indexes of this bucket to ACTIVE
					//these indexes get removed later as part of merge message
					//from indexer
					tk.changeIndexStateForBucket(bucket, common.INDEX_STATE_ACTIVE)

					common.Debugf("Timekeeper::checkInitStreamReadyToMerge \n\tIndex Ready To Merge. "+
						"Index: %v Stream: %v Bucket: %v LastFlushTS: %v", idx.InstId, streamId,
						bucket, lastFlushedTs)

					tk.supvRespch <- &MsgTKMergeStream{
						streamId: streamId,
						bucket:   bucket,
						mergeTs:  ts}

					common.Debugf("Timekeeper::checkInitStreamReadyToMerge \n\t Stream %v "+
						"State Changed to INACTIVE", streamId)
					tk.streamState[streamId] = STREAM_INACTIVE
					tk.supvCmdch <- &MsgSuccess{}
					return true
				}
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
		bucketTsListMap := tk.streamBucketTsListMap[common.MAINT_STREAM]
		tsList := bucketTsListMap[bucket]
		var maintTs *common.TsVbuuid
		if tsList.Len() > 0 {
			e := tsList.Front()
			maintTs = e.Value.(*common.TsVbuuid)
		} else {
			return false
		}

		//get the lastFlushedTs of CATCHUP_STREAM
		bucketLastFlushedTsMap := tk.streamBucketLastFlushedTsMap[common.CATCHUP_STREAM]
		lastFlushedTs := bucketLastFlushedTsMap[bucket]
		if lastFlushedTs == nil {
			return false
		}

		if compareTsSnapshot(lastFlushedTs, maintTs) {
			//disable drain for MAINT_STREAM for this bucket, so it doesn't
			//drop mutations till merge is complete
			tk.streamBucketDrainEnabledMap[common.MAINT_STREAM][bucket] = false

			common.Debugf("Timekeeper::checkCatchupStreamReadyToMerge \n\tBucket Ready To Merge. "+
				"Stream: %v Bucket: %v ", streamId, bucket)

			tk.supvRespch <- &MsgTKMergeStream{
				streamId: streamId,
				bucket:   bucket}

			common.Debugf("Timekeeper::checkCatchupStreamReadyToMerge \n\t Stream %v "+
				"State Changed to INACTIVE", streamId)

			tk.streamState[streamId] = STREAM_INACTIVE
			tk.supvCmdch <- &MsgSuccess{}
			return true
		}
	}
	return false
}

//updateHWT will update the HW Timestamp for a bucket in the stream
//based on the Sync message received.
func (tk *timekeeper) updateHWT(cmd Message) {

	streamId := cmd.(*MsgStream).GetStreamId()
	meta := cmd.(*MsgStream).GetMutationMeta()

	//if seqno has incremented, update it
	ts := tk.streamBucketHWTMap[streamId][meta.bucket]
	if uint64(meta.seqno) > ts.Seqnos[meta.vbucket] {
		ts.Seqnos[meta.vbucket] = uint64(meta.seqno)
		ts.Vbuuids[meta.vbucket] = uint64(meta.vbuuid)
		common.Tracef("Timekeeper::updateHWT \n\tHWT Updated : %v", ts)
	}

}

//incrSyncCount increment the sync count for a bucket in the stream
func (tk *timekeeper) incrSyncCount(streamId common.StreamId, bucket string) {

	bucketSyncCountMap := tk.streamBucketSyncCountMap[streamId]

	//update sync count for this bucket
	if syncCount, ok := bucketSyncCountMap[bucket]; ok {
		syncCount++

		common.Tracef("Timekeeper::incrSyncCount \n\tUpdating Sync Count for Bucket: %v "+
			"Stream: %v. SyncCount: %v.", bucket, streamId, syncCount)
		//update only if its less than trigger count, otherwise it makes no
		//difference. On long running systems, syncCount may overflow otherwise
		if syncCount <= uint64(SYNC_COUNT_TS_TRIGGER*NUM_VBUCKETS) {
			bucketSyncCountMap[bucket] = syncCount
		}

	} else {
		//add a new counter for this bucket
		common.Debugf("Timekeeper::incrSyncCount \n\tAdding new Sync Count for Bucket: %v "+
			"Stream: %v. SyncCount: %v.", bucket, streamId, syncCount)
		bucketSyncCountMap[bucket] = 1
	}

}

//generates a new StabilityTS
func (tk *timekeeper) generateNewStabilityTS(streamId common.StreamId,
	bucket string) {

	bucketNewTsReqd := tk.streamBucketNewTsReqdMap[streamId]
	bucketFlushInProgressTsMap := tk.streamBucketFlushInProgressTsMap[streamId]
	bucketTsListMap := tk.streamBucketTsListMap[streamId]
	bucketFlushEnabledMap := tk.streamBucketFlushEnabledMap[streamId]
	bucketSyncCountMap := tk.streamBucketSyncCountMap[streamId]

	//new timestamp can be generated if all stream begins have been received
	if bucketSyncCountMap[bucket] >= uint64(SYNC_COUNT_TS_TRIGGER*NUM_VBUCKETS) &&
		bucketNewTsReqd[bucket] == true &&
		tk.allStreamBeginsReceived(streamId, bucket) == true {

		//generate new stability timestamp
		tsVbuuid := copyTsVbuuid(bucket, tk.streamBucketHWTMap[streamId][bucket])

		//HWT may have less Seqno than Snapshot marker as mutation come later than
		//snapshot markers. Once a TS is generated, update the Seqnos with the
		//snapshot high seq num as that persistence will happen at these seqnums.
		updateTsSeqNumToSnapshot(tsVbuuid)

		common.Debugf("Timekeeper::generateNewStabilityTS \n\tGenerating new Stability "+
			"TS: %v Bucket: %v Stream: %v. SyncCount: %v", tsVbuuid,
			bucket, streamId, bucketSyncCountMap[bucket])

		//if there is no flush already in progress for this bucket
		//no pending TS in list and flush is not disabled, send new TS
		tsList := bucketTsListMap[bucket]
		if bucketFlushInProgressTsMap[bucket] == nil &&
			bucketFlushEnabledMap[bucket] == true &&
			tsList.Len() == 0 {
			tk.streamBucketFlushInProgressTsMap[streamId][bucket] = tsVbuuid
			go tk.sendNewStabilityTS(tsVbuuid, bucket, streamId)
		} else {
			//store the ts in list
			common.Debugf("Timekeeper::generateNewStabilityTS \n\tAdding TS: %v to Pending "+
				"List for Bucket: %v Stream: %v.", tsVbuuid, bucket, streamId)
			tsList.PushBack(tsVbuuid)
			tk.drainQueueIfOverflow(streamId, bucket)
		}
		bucketSyncCountMap[bucket] = 0
		bucketNewTsReqd[bucket] = false
	}
}

//processPendingTS checks if there is any pending TS for the given stream and
//bucket. If any TS is found, it is sent to supervisor.
func (tk *timekeeper) processPendingTS(streamId common.StreamId, bucket string) bool {

	//if there is a flush already in progress for this stream and bucket
	//or flush is disabled, nothing to be done
	bucketFlushInProgressTsMap := tk.streamBucketFlushInProgressTsMap[streamId]
	bucketFlushEnabledMap := tk.streamBucketFlushEnabledMap[streamId]

	if bucketFlushInProgressTsMap[bucket] != nil ||
		bucketFlushEnabledMap[bucket] == false {
		return false
	}

	tsVbuuidHWT := tk.streamBucketHWTMap[streamId][bucket]
	tsHWT := getStabilityTSFromTsVbuuid(tsVbuuidHWT)

	//if there are pending TS for this bucket, send New TS
	bucketTsListMap := tk.streamBucketTsListMap[streamId]
	tsList := bucketTsListMap[bucket]
	if tsList.Len() > 0 {
		e := tsList.Front()
		tsVbuuid := e.Value.(*common.TsVbuuid)
		tsList.Remove(e)
		ts := getStabilityTSFromTsVbuuid(tsVbuuid)

		if tk.streamState[streamId] == STREAM_PREPARE_RECOVERY {
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

		tk.streamBucketFlushInProgressTsMap[streamId][bucket] = tsVbuuid
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

//checkStreamValid checks if the given streamId is available in
//internal map
func (tk *timekeeper) checkStreamValid(streamId common.StreamId) bool {

	if _, ok := tk.streamBucketHWTMap[streamId]; !ok {
		common.Errorf("Timekeeper::checkStreamValid \n\tUnknown Stream: %v", streamId)
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

	ts := NewTimestamp()
	for i, s := range tsVbuuid.Snapshots {
		ts[i] = Seqno(s[1]) //high seq num in snapshot marker
	}
	return ts
}

//helper function to extract Seqnum Timestamp from TsVbuuid
func getTSFromTsVbuuid(tsVbuuid *common.TsVbuuid) Timestamp {

	ts := NewTimestamp()
	for i, s := range tsVbuuid.Seqnos {
		ts[i] = Seqno(s)
	}
	return ts
}

//helper function to copy TsVbuuid
func copyTsVbuuid(bucket string, tsVbuuid *common.TsVbuuid) *common.TsVbuuid {

	newTs := common.NewTsVbuuid(bucket, int(NUM_VBUCKETS))

	for i := 0; i < int(NUM_VBUCKETS); i++ {
		newTs.Seqnos[i] = tsVbuuid.Seqnos[i]
		newTs.Vbuuids[i] = tsVbuuid.Vbuuids[i]
		newTs.Snapshots[i] = tsVbuuid.Snapshots[i]
	}

	return newTs

}

//helper function to update Seqnos in TsVbuuid to
//high seqnum of snapshot markers
func updateTsSeqNumToSnapshot(ts *common.TsVbuuid) {

	for i, s := range ts.Snapshots {
		ts.Seqnos[i] = s[1]
	}

}

//if there are more mutations in this queue than configured,
//drain the queue
func (tk *timekeeper) drainQueueIfOverflow(streamId common.StreamId, bucket string) {

	if !tk.streamBucketDrainEnabledMap[streamId][bucket] {
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

func (tk *timekeeper) initiateRecovery(streamId common.StreamId) {

	common.Debugf("Timekeeper::initiateRecovery Started")

	switch streamId {

	case common.CATCHUP_STREAM, common.MAINT_STREAM:

		//check if Catchup is running, reset state for both maint and catchup
		if state, ok := tk.streamState[common.CATCHUP_STREAM]; ok {

			if state == STREAM_INACTIVE {
				bucketRestartTs := tk.computeRestartTs(common.CATCHUP_STREAM)
				tk.resetInternalStreamState(common.CATCHUP_STREAM)
				tk.resetInternalStreamState(common.MAINT_STREAM)
				//send message for recovery
				tk.supvRespch <- &MsgRecovery{mType: INDEXER_INITIATE_RECOVERY,
					streamId:  common.MAINT_STREAM,
					restartTs: bucketRestartTs}
				tk.streamBucketRestartTsMap[common.CATCHUP_STREAM] = bucketRestartTs
				tk.streamBucketRestartTsMap[common.MAINT_STREAM] = bucketRestartTs
				common.Debugf("Timekeeper::initiateRecovery StreamId %v "+
					"BucketRestartTs %v", streamId, bucketRestartTs)
				return
			} else {
				common.Errorf("Timekeeper::initiateRecovery Invalid State For CATCHUP "+
					"Stream Detected. State %v", state)
				return
			}
		}

		if tk.streamState[common.MAINT_STREAM] == STREAM_INACTIVE {

			bucketRestartTs := tk.computeRestartTs(common.MAINT_STREAM)
			tk.resetInternalStreamState(common.MAINT_STREAM)

			//send message for recovery
			tk.supvRespch <- &MsgRecovery{mType: INDEXER_INITIATE_RECOVERY,
				streamId:  common.MAINT_STREAM,
				restartTs: bucketRestartTs}
			tk.streamBucketRestartTsMap[common.MAINT_STREAM] = bucketRestartTs
			common.Debugf("Timekeeper::initiateRecovery StreamId %v "+
				"BucketRestartTs %v", streamId, bucketRestartTs)
		} else {
			common.Errorf("Timekeeper::initiateRecovery Invalid State For MAINT "+
				"Stream Detected. State %v", tk.streamState[common.MAINT_STREAM])
		}

	case common.INIT_STREAM:

		if tk.streamState[streamId] == STREAM_PREPARE_RECOVERY {

			bucketRestartTs := tk.computeRestartTs(streamId)
			tk.resetInternalStreamState(streamId)

			//send message for recovery
			tk.supvRespch <- &MsgRecovery{mType: INDEXER_INITIATE_RECOVERY,
				streamId:  streamId,
				restartTs: bucketRestartTs}
			tk.streamBucketRestartTsMap[streamId] = bucketRestartTs
			common.Debugf("Timekeeper::initiateRecovery StreamId %v "+
				"BucketRestartTs %v", streamId, bucketRestartTs)
		} else {
			common.Errorf("Timekeeper::initiateRecovery Invalid State For INIT "+
				"Stream Detected. State %v", tk.streamState[streamId])
		}
	}

}

//computes the restart Ts for all the buckets in a given stream
func (tk *timekeeper) computeRestartTs(streamId common.StreamId) map[string]*common.TsVbuuid {

	bucketRestartTs := make(map[string]*common.TsVbuuid)

	for bucket, _ := range tk.streamBucketLastFlushedTsMap[streamId] {

		bucketRestartTs[bucket] = copyTsVbuuid(bucket, tk.streamBucketLastFlushedTsMap[streamId][bucket])
	}

	return bucketRestartTs

}

func (tk *timekeeper) initInternalStreamState(streamId common.StreamId,
	bucket string) {

	tk.streamBucketHWTMap[streamId][bucket] = common.NewTsVbuuid(bucket, int(NUM_VBUCKETS))
	tk.streamBucketNewTsReqdMap[streamId][bucket] = false
	tk.streamBucketFlushInProgressTsMap[streamId][bucket] = nil
	tk.streamBucketTsListMap[streamId][bucket] = list.New()
	tk.streamBucketFlushEnabledMap[streamId][bucket] = true
	tk.streamBucketDrainEnabledMap[streamId][bucket] = true
	tk.streamBucketStreamBeginMap[streamId][bucket] = NewTimestamp()

	common.Debugf("Timekeeper::initInternalStreamState \n\tNew TS Allocated for Bucket %v "+
		"Stream %v", bucket, streamId)
}

func (tk *timekeeper) resetInternalStreamState(streamId common.StreamId) {

	common.Debugf("Timekeeper::resetInternalStreamState \n\tReset Stream %v State", streamId)

	for bucket, _ := range tk.streamBucketHWTMap[streamId] {
		//reset all the internal structs
		delete(tk.streamBucketHWTMap[streamId], bucket)
		delete(tk.streamBucketSyncCountMap[streamId], bucket)
		delete(tk.streamBucketNewTsReqdMap[streamId], bucket)
		delete(tk.streamBucketTsListMap[streamId], bucket)
		delete(tk.streamBucketStreamBeginMap[streamId], bucket)
		delete(tk.streamBucketFlushEnabledMap[streamId], bucket)
		delete(tk.streamBucketDrainEnabledMap[streamId], bucket)
		delete(tk.streamBucketLastFlushedTsMap[streamId], bucket)
	}

}

//checks if none of the buckets in this stream has any flush in progress
func (tk *timekeeper) checkAnyFlushPending(streamId common.StreamId) bool {

	bucketFlushInProgressTsMap := tk.streamBucketFlushInProgressTsMap[streamId]

	//check if there is any flush in progress. No flush in progress implies
	//there is no flush pending as well bcoz every flush done triggers
	//the next flush.
	for _, ts := range bucketFlushInProgressTsMap {
		if ts != nil {
			return true
		}
	}
	return false
}

//checks if none of the buckets in this stream has any flush abort in progress
func (tk *timekeeper) checkAnyAbortPending(streamId common.StreamId) bool {

	bucketAbortInProgressMap := tk.streamBucketAbortInProgressMap[streamId]

	//check if there is any flush in progress. No flush in progress implies
	//there is no flush pending as well bcoz every flush done triggers
	//the next flush.
	for _, running := range bucketAbortInProgressMap {
		if running {
			return true
		}
	}
	return false
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

func (tk *timekeeper) checkStreamReadyForRecovery(streamId common.StreamId) bool {

	common.Debugf("Timekeeper::checkStreamReadyForRecovery StreamId %v", streamId)

	switch streamId {

	case common.MAINT_STREAM, common.CATCHUP_STREAM:

		//check if there is no flush or abort pending for MAINT_STREAM and
		//stream has been closed
		if tk.checkAnyFlushPending(common.MAINT_STREAM) ||
			tk.checkAnyAbortPending(common.MAINT_STREAM) ||
			tk.streamState[common.MAINT_STREAM] != STREAM_INACTIVE {
			return false
		}

		//if CATCHUP exists, check for catchup stream as well
		if _, ok := tk.streamState[common.CATCHUP_STREAM]; ok {
			if tk.checkAnyFlushPending(common.CATCHUP_STREAM) ||
				tk.checkAnyAbortPending(common.CATCHUP_STREAM) ||
				tk.streamState[common.CATCHUP_STREAM] != STREAM_INACTIVE {
				return false
			}
		}

	case common.INIT_STREAM:

		if tk.checkAnyFlushPending(streamId) ||
			tk.checkAnyAbortPending(streamId) ||
			tk.streamState[streamId] != STREAM_INACTIVE {
			return false
		}
	}
	common.Debugf("Timekeeper::checkStreamReadyForRecovery StreamId %v Ready for Recovery", streamId)
	return true
}

func (tk *timekeeper) disableStreamFlush(streamId common.StreamId) {

	bucketFlushEnabledMap := tk.streamBucketFlushEnabledMap[streamId]
	for bucket, _ := range bucketFlushEnabledMap {
		bucketFlushEnabledMap[bucket] = false
	}
}

func (tk *timekeeper) allStreamBeginsReceived(streamId common.StreamId, bucket string) bool {

	if bucketStreamBeginMap, ok := tk.streamBucketStreamBeginMap[streamId]; ok {
		if ts, ok := bucketStreamBeginMap[bucket]; ok {
			for _, v := range ts {
				if v != 1 { //1 denotes StreamBegin
					return false
				}
			}
		} else {
			return false
		}
	} else {
		return false
	}
	return true
}

func (tk *timekeeper) setHWTFromRestartTs(streamId common.StreamId, bucket string) {

	common.Debugf("Timekeeper::setHWTFromRestartTs Stream %v Bucket %v", streamId, bucket)

	//if no flush has been done for Catchup Stream yet, use RestartTs from Maint Stream
	fromStream := streamId
	if streamId == common.CATCHUP_STREAM {
		if bucketRestartTsMap, ok := tk.streamBucketRestartTsMap[streamId]; ok {
			if _, ok := bucketRestartTsMap[bucket]; ok {
			} else {
				fromStream = common.MAINT_STREAM
			}
		} else {
			fromStream = common.MAINT_STREAM
		}
	}

	if bucketRestartTs, ok := tk.streamBucketRestartTsMap[fromStream]; ok {

		if restartTs, ok := bucketRestartTs[bucket]; ok {

			//update HWT
			tk.streamBucketHWTMap[streamId][bucket] = copyTsVbuuid(bucket, restartTs)

			//update Last Flushed Ts
			tk.streamBucketLastFlushedTsMap[streamId][bucket] = copyTsVbuuid(bucket, restartTs)
			common.Debugf("Timekeeper::setHWTFromRestartTs \n\tHWT Set For "+
				"Bucket %v StreamId %v. TS %v.", bucket, streamId, restartTs)

		} else {
			common.Warnf("Timekeeper::setHWTFromRestartTs \n\tRestartTs Not Found For "+
				"Bucket %v StreamId %v. No Value Set.", bucket, streamId)
		}
	}
}

func (tk *timekeeper) handleStreamCleanup(cmd Message) {

	common.Debugf("Timekeeper::handleStreamCleanup %v", cmd)

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()

	common.Debugf("Timekeeper::handleStreamCleanup \n\t Stream %v "+
		"State Changed to INACTIVE", streamId)
	tk.streamState[streamId] = STREAM_INACTIVE

	tk.supvCmdch <- &MsgSuccess{}

}
