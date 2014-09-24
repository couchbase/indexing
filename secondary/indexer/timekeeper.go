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

	streamBucketTsListMap          map[common.StreamId]BucketTsListMap
	streamBucketFlushInProgressMap map[common.StreamId]BucketFlushInProgressMap

	streamBucketLastTsFlushedMap map[common.StreamId]BucketLastTsFlushedMap
	streamBucketFlushEnabledMap  map[common.StreamId]BucketFlushEnabledMap

	//map of indexInstId to its Initial Build Info
	indexBuildInfo map[common.IndexInstId]*InitialBuildInfo
}

type BucketHWTMap map[string]*common.TsVbuuid
type BucketLastTsFlushedMap map[string]Timestamp
type BucketSyncCountMap map[string]uint64
type BucketNewTsReqdMap map[string]bool

type BucketTsListMap map[string]*list.List
type BucketFlushInProgressMap map[string]bool
type BucketFlushEnabledMap map[string]bool

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
		supvCmdch:                      supvCmdch,
		supvRespch:                     supvRespch,
		streamBucketHWTMap:             make(map[common.StreamId]BucketHWTMap),
		streamBucketSyncCountMap:       make(map[common.StreamId]BucketSyncCountMap),
		streamBucketNewTsReqdMap:       make(map[common.StreamId]BucketNewTsReqdMap),
		streamBucketTsListMap:          make(map[common.StreamId]BucketTsListMap),
		streamBucketFlushInProgressMap: make(map[common.StreamId]BucketFlushInProgressMap),
		streamBucketLastTsFlushedMap:   make(map[common.StreamId]BucketLastTsFlushedMap),
		streamBucketFlushEnabledMap:    make(map[common.StreamId]BucketFlushEnabledMap),
		indexBuildInfo:                 make(map[common.IndexInstId]*InitialBuildInfo),
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
				tk.handleSupvervisorCommands(cmd)
			} else {
				//supervisor channel closed. exit
				break loop
			}

		}
	}
}

func (tk *timekeeper) handleSupvervisorCommands(cmd Message) {

	switch cmd.GetMsgType() {

	case STREAM_READER_SYNC:
		tk.handleSync(cmd)

	case OPEN_STREAM:
		tk.handleStreamStart(cmd)

	case ADD_INDEX_LIST_TO_STREAM:
		tk.handleAddIndextoStream(cmd)

	case REMOVE_INDEX_LIST_FROM_STREAM:
		tk.handleRemoveIndexFromStream(cmd)

	case CLOSE_STREAM:
		tk.handleStreamStop(cmd)

	case MUT_MGR_FLUSH_DONE:
		tk.handleFlushDone(cmd)

	case TK_ENABLE_FLUSH:
		tk.handleEnableFlush(cmd)

	case STREAM_READER_SNAPSHOT_MARKER:
		tk.handleSnapshotMarker(cmd)

	case TK_GET_BUCKET_HWT:
		tk.handleGetBucketHWT(cmd)

	default:
		common.Errorf("Timekeeper::handleSupvervisorCommands "+
			"Received Unknown Command %v", cmd)

	}

}

func (tk *timekeeper) handleStreamStart(cmd Message) {

	common.Infof("Timekeeper::handleStreamStart %v", cmd)

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()

	//init all internal maps for this stream
	bucketHWTMap := make(BucketHWTMap)
	tk.streamBucketHWTMap[streamId] = bucketHWTMap

	bucketSyncCountMap := make(BucketSyncCountMap)
	tk.streamBucketSyncCountMap[streamId] = bucketSyncCountMap

	bucketNewTsReqdMap := make(BucketNewTsReqdMap)
	tk.streamBucketNewTsReqdMap[streamId] = bucketNewTsReqdMap

	bucketTsListMap := make(BucketTsListMap)
	tk.streamBucketTsListMap[streamId] = bucketTsListMap

	bucketFlushInProgressMap := make(BucketFlushInProgressMap)
	tk.streamBucketFlushInProgressMap[streamId] = bucketFlushInProgressMap

	bucketLastTsFlushedMap := make(BucketLastTsFlushedMap)
	tk.streamBucketLastTsFlushedMap[streamId] = bucketLastTsFlushedMap

	bucketFlushEnabledMap := make(BucketFlushEnabledMap)
	tk.streamBucketFlushEnabledMap[streamId] = bucketFlushEnabledMap

	//add the new indexes to internal maps
	tk.addIndextoStream(cmd)

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleStreamStop(cmd Message) {

	common.Infof("Timekeeper::handleStreamStop %v", cmd)

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()

	//delete this stream from internal maps
	delete(tk.streamBucketHWTMap, streamId)
	delete(tk.streamBucketSyncCountMap, streamId)
	delete(tk.streamBucketNewTsReqdMap, streamId)
	delete(tk.streamBucketTsListMap, streamId)
	delete(tk.streamBucketFlushInProgressMap, streamId)
	delete(tk.streamBucketLastTsFlushedMap, streamId)
	delete(tk.streamBucketFlushEnabledMap, streamId)

	//delete indexes from stream
	tk.removeIndexFromStream(cmd)

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

	//check if stream is valid
	if tk.checkStreamValid(streamId) == false {
		return
	}

	//update HWT for the bucket
	tk.updateHWT(cmd)

	meta := cmd.(*MsgStream).GetMutationMeta()

	//update Sync Count for the bucket
	tk.incrSyncCount(streamId, meta.bucket)

	//generate new StabilityTS if required
	tk.generateNewStabilityTS(streamId, meta.bucket)

	tk.supvCmdch <- &MsgSuccess{}

}

func (tk *timekeeper) handleFlushDone(cmd Message) {

	common.Debugf("Timekeeper:handleFlushDone %v", cmd)

	streamId := cmd.(*MsgMutMgrFlushDone).GetStreamId()
	bucket := cmd.(*MsgMutMgrFlushDone).GetBucket()

	//update internal map to reflect flush is done
	bucketFlushInProgressMap := tk.streamBucketFlushInProgressMap[streamId]
	bucketFlushInProgressMap[bucket] = false

	//check if any of the initial build index is past its Build TS.
	//Generate msg for Build Done and change the state of the index.
	tk.checkInitialBuildDone(cmd)

	//if flush is for INIT_STREAM, check if any index in CATCHUP has reached
	//past the last flushed TS of the MAINT_STREAM for this bucket.
	//In such case, all indexes of the bucket can merged to MAINT_STREAM.
	if tk.checkStreamReadyToMerge(cmd) {
		//if stream is ready to merge, further processing is
		//not required, return from here.
		return
	}

	//check if there is any pending TS for this bucket/stream.
	//It can be processed now.
	tk.checkPendingTS(streamId, bucket)

	tk.supvCmdch <- &MsgSuccess{}

}

func (tk *timekeeper) handleEnableFlush(cmd Message) {

	streamId := cmd.(*MsgTKEnableFlush).GetStreamId()
	bucket := cmd.(*MsgTKEnableFlush).GetBucket()

	common.Debugf("Timekeeper::handleEnableFlush \n\t Received Enable Flush for "+
		"Bucket: %v StreamId: %v", bucket, streamId)

	bucketFlushEnabledMap := tk.streamBucketFlushEnabledMap[streamId]
	bucketFlushEnabledMap[bucket] = true

	//if there are any pending TS, send that
	tk.checkPendingTS(streamId, bucket)

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleSnapshotMarker(cmd Message) {

	common.Debugf("Timekeeper::handleSnapshotMarker %v", cmd)

	streamId := cmd.(*MsgStream).GetStreamId()
	meta := cmd.(*MsgStream).GetMutationMeta()

	//check if stream is valid
	if tk.checkStreamValid(streamId) == false {
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

		bucketHWTMap := tk.streamBucketHWTMap[streamId]

		//allocate a new timestamp for this bucket, if not
		//already there
		if _, ok := bucketHWTMap[meta.bucket]; !ok {
			bucketHWTMap[meta.bucket] = common.NewTsVbuuid(meta.bucket, int(NUM_VBUCKETS))
			tk.streamBucketNewTsReqdMap[streamId][meta.bucket] = false
			tk.streamBucketTsListMap[streamId][meta.bucket] = list.New()
			tk.streamBucketFlushInProgressMap[streamId][meta.bucket] = false
			tk.streamBucketFlushEnabledMap[streamId][meta.bucket] = true
		}

		//update the snapshot seqno in internal map
		ts := bucketHWTMap[meta.bucket]
		ts.Snapshots[meta.vbucket][0] = snapshot.start
		ts.Snapshots[meta.vbucket][1] = snapshot.end
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
			if flushTs.GreaterThanEqual(buildInfo.buildTs) {

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

//checkStreamReadyToMerge checks if any index in Catchup State in INIT_STREAM
//has reached past the last flushed TS of the MAINT_STREAM for this bucket.
//In such case, all indexes of the bucket can merged to MAINT_STREAM.
func (tk *timekeeper) checkStreamReadyToMerge(cmd Message) bool {

	streamId := cmd.(*MsgMutMgrFlushDone).GetStreamId()
	bucket := cmd.(*MsgMutMgrFlushDone).GetBucket()
	flushTs := cmd.(*MsgMutMgrFlushDone).GetTS()

	if streamId == common.INIT_STREAM {

		for _, buildInfo := range tk.indexBuildInfo {
			//if index belongs to the flushed bucket and in CATCHUP state
			idx := buildInfo.indexInst
			if idx.Defn.Bucket == bucket &&
				idx.State == common.INDEX_STATE_CATCHUP {

				//if the flushTs is past the lastFlushTs of this bucket in MAINT_STREAM,
				//this index can be merged to MAINT_STREAM
				bucketLastTsFlushedMap := tk.streamBucketLastTsFlushedMap[common.MAINT_STREAM]
				lastFlushedTs := bucketLastTsFlushedMap[idx.Defn.Bucket]

				if flushTs.GreaterThanEqual(lastFlushedTs) {
					//disable flush for MAINT_STREAM for this bucket, so it doesn't
					//move ahead till merge is complete
					bucketFlushEnabledMap := tk.streamBucketFlushEnabledMap[common.MAINT_STREAM]
					bucketFlushEnabledMap[idx.Defn.Bucket] = false

					//change state of all indexes of this bucket to ACTIVE
					//these indexes get removed later as part of merge message
					//from indexer
					tk.changeIndexStateForBucket(bucket, common.INDEX_STATE_ACTIVE)

					common.Debugf("Timekeeper::checkStreamReadyToMerge \n\tIndex Ready To Merge. "+
						"Index: %v Stream: %v Bucket: %v LastFlushTS: %v", idx.InstId, streamId,
						bucket, lastFlushedTs)

					tk.supvRespch <- &MsgTKMergeStream{
						streamId: streamId,
						bucket:   bucket,
						mergeTs:  flushTs}

					tk.supvCmdch <- &MsgSuccess{}
					return true
				}
			}
		}
	}
	return false
}

//updateHWT will update the HW Timestamp for a bucket in the stream
//based on the Sync message received.
func (tk *timekeeper) updateHWT(cmd Message) {

	streamId := cmd.(*MsgStream).GetStreamId()
	meta := cmd.(*MsgStream).GetMutationMeta()

	bucketHWTMap := tk.streamBucketHWTMap[streamId]

	//allocate a new timestamp for this bucket, if not
	//already there(on the first sync message for this bucket)
	if _, ok := bucketHWTMap[meta.bucket]; !ok {
		bucketHWTMap[meta.bucket] = common.NewTsVbuuid(meta.bucket, int(NUM_VBUCKETS))
		tk.streamBucketNewTsReqdMap[streamId][meta.bucket] = false
		tk.streamBucketFlushInProgressMap[streamId][meta.bucket] = false
		tk.streamBucketTsListMap[streamId][meta.bucket] = list.New()
		tk.streamBucketFlushEnabledMap[streamId][meta.bucket] = true
	}

	//if seqno has incremented, update it
	ts := bucketHWTMap[meta.bucket]
	if uint64(meta.seqno) > ts.Seqnos[meta.vbucket] {
		tk.streamBucketNewTsReqdMap[streamId][meta.bucket] = true
		ts.Seqnos[meta.vbucket] = uint64(meta.seqno)
		ts.Vbuuids[meta.vbucket] = uint64(meta.vbuuid)
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
		if syncCount <= SYNC_COUNT_TS_TRIGGER {
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
	bucketFlushInProgressMap := tk.streamBucketFlushInProgressMap[streamId]
	bucketTsListMap := tk.streamBucketTsListMap[streamId]
	bucketFlushEnabledMap := tk.streamBucketFlushEnabledMap[streamId]
	bucketSyncCountMap := tk.streamBucketSyncCountMap[streamId]

	if bucketSyncCountMap[bucket] >= SYNC_COUNT_TS_TRIGGER &&
		bucketNewTsReqd[bucket] == true {
		//generate new stability timestamp

		tsVbuuid := tk.streamBucketHWTMap[streamId][bucket]
		newTs := getTSFromTsVbuuid(tsVbuuid)

		common.Debugf("Timekeeper::generateNewStabilityTS \n\tGenerating new Stability "+
			"TS: %v Bucket: %v Stream: %v. SyncCount: %v", newTs,
			bucket, streamId, bucketSyncCountMap[bucket])

		//if there is no flush already in progress for this bucket
		//no pending TS in list and flush is not disabled, send new TS
		tsList := bucketTsListMap[bucket]
		if bucketFlushInProgressMap[bucket] == false &&
			bucketFlushEnabledMap[bucket] == true &&
			tsList.Len() == 0 {
			go tk.sendNewStabilityTS(newTs, bucket, streamId)
		} else {
			//store the ts in list
			common.Debugf("Timekeeper::generateNewStabilityTS \n\tAdding TS: %v to Pending "+
				"List for Bucket: %v Stream: %v.", newTs, bucket, streamId)
			tsList.PushBack(newTs)
		}
		bucketSyncCountMap[bucket] = 0
		bucketNewTsReqd[bucket] = false
	}
}

//checkPendingTS checks if there is any pending TS for the given stream and
//bucket. If any TS is found, it is sent to supervisor.
func (tk *timekeeper) checkPendingTS(streamId common.StreamId, bucket string) {

	//if there is a flush already in progress for this stream and bucket
	//or flush is disabled, nothing to be done
	bucketFlushInProgressMap := tk.streamBucketFlushInProgressMap[streamId]
	bucketFlushEnabledMap := tk.streamBucketFlushEnabledMap[streamId]

	if bucketFlushInProgressMap[bucket] == true ||
		bucketFlushEnabledMap[bucket] == false {
		return
	}

	//if there are pending TS for this bucket, send New TS
	bucketTsListMap := tk.streamBucketTsListMap[streamId]
	tsList := bucketTsListMap[bucket]
	if tsList.Len() > 0 {
		e := tsList.Front()
		ts := e.Value.(Timestamp)
		tsList.Remove(e)
		common.Debugf("Timekeeper::checkPendingTS \n\tFound Pending Stability TS Bucket: %v "+
			"Stream: %v TS: %v", bucket, streamId, ts)
		go tk.sendNewStabilityTS(ts, bucket, streamId)
	}
}

//sendNewStabilityTS sends the given TS to supervisor
func (tk *timekeeper) sendNewStabilityTS(ts Timestamp, bucket string,
	streamId common.StreamId) {

	common.Debugf("Timekeeper::sendNewStabilityTS \n\tBucket: %v "+
		"Stream: %v TS: %v", bucket, streamId, ts)

	//store the last flushed TS
	bucketLastTsFlushedMap := tk.streamBucketLastTsFlushedMap[streamId]
	bucketLastTsFlushedMap[bucket] = ts

	bucketFlushInProgressMap := tk.streamBucketFlushInProgressMap[streamId]
	bucketFlushInProgressMap[bucket] = true

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
		common.Fatalf("Timekeeper::checkStreamValid \n\tGot STREAM_READER_SYNC "+
			"For Unknown Stream: %v", streamId)
		tk.supvCmdch <- &MsgError{
			err: Error{code: ERROR_TK_UNKNOWN_STREAM,
				severity: FATAL,
				category: TIMEKEEPER}}
		return false
	}
	return true
}

//helper function to extract Timestamp from TsVbuuid
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
