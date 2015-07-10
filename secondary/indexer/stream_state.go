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
	"container/list"
	"errors"
	"fmt"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"time"
)

type StreamState struct {
	config common.Config

	streamStatus              map[common.StreamId]StreamStatus
	streamBucketStatus        map[common.StreamId]BucketStatus
	streamBucketVbStatusMap   map[common.StreamId]BucketVbStatusMap
	streamBucketVbRefCountMap map[common.StreamId]BucketVbRefCountMap

	streamBucketHWTMap           map[common.StreamId]BucketHWTMap
	streamBucketInMemTsCountMap  map[common.StreamId]BucketInMemTsCountMap
	streamBucketNewTsReqdMap     map[common.StreamId]BucketNewTsReqdMap
	streamBucketTsListMap        map[common.StreamId]BucketTsListMap
	streamBucketLastFlushedTsMap map[common.StreamId]BucketLastFlushedTsMap
	streamBucketRestartTsMap     map[common.StreamId]BucketRestartTsMap

	streamBucketLastSnapAlignFlushedTsMap map[common.StreamId]BucketLastFlushedTsMap

	streamBucketRestartVbErrMap   map[common.StreamId]BucketRestartVbErrMap
	streamBucketRestartVbTsMap    map[common.StreamId]BucketRestartVbTsMap
	streamBucketRestartVbRetryMap map[common.StreamId]BucketRestartVbRetryMap

	streamBucketFlushInProgressTsMap map[common.StreamId]BucketFlushInProgressTsMap
	streamBucketAbortInProgressMap   map[common.StreamId]BucketAbortInProgressMap
	streamBucketFlushEnabledMap      map[common.StreamId]BucketFlushEnabledMap
	streamBucketDrainEnabledMap      map[common.StreamId]BucketDrainEnabledMap

	streamBucketIndexCountMap   map[common.StreamId]BucketIndexCountMap
	streamBucketRepairStopCh    map[common.StreamId]BucketRepairStopCh
	streamBucketTimerStopCh     map[common.StreamId]BucketTimerStopCh
	streamBucketLastPersistTime map[common.StreamId]BucketLastPersistTime
}

type BucketHWTMap map[string]*common.TsVbuuid
type BucketLastFlushedTsMap map[string]*common.TsVbuuid
type BucketRestartTsMap map[string]*common.TsVbuuid
type BucketInMemTsCountMap map[string]uint64
type BucketNewTsReqdMap map[string]bool

type BucketTsListMap map[string]*list.List
type BucketFlushInProgressTsMap map[string]*common.TsVbuuid
type BucketAbortInProgressMap map[string]bool
type BucketFlushEnabledMap map[string]bool
type BucketDrainEnabledMap map[string]bool

type BucketRestartVbErrMap map[string]bool
type BucketRestartVbTsMap map[string]*common.TsVbuuid
type BucketRestartVbRetryMap map[string]Timestamp

type BucketVbStatusMap map[string]Timestamp
type BucketVbRefCountMap map[string]Timestamp
type BucketRepairStopCh map[string]StopChannel
type BucketTimerStopCh map[string]StopChannel
type BucketLastPersistTime map[string]time.Time

type BucketStatus map[string]StreamStatus

func InitStreamState(config common.Config) *StreamState {

	ss := &StreamState{
		config:                                config,
		streamBucketHWTMap:                    make(map[common.StreamId]BucketHWTMap),
		streamBucketInMemTsCountMap:           make(map[common.StreamId]BucketInMemTsCountMap),
		streamBucketNewTsReqdMap:              make(map[common.StreamId]BucketNewTsReqdMap),
		streamBucketTsListMap:                 make(map[common.StreamId]BucketTsListMap),
		streamBucketFlushInProgressTsMap:      make(map[common.StreamId]BucketFlushInProgressTsMap),
		streamBucketAbortInProgressMap:        make(map[common.StreamId]BucketAbortInProgressMap),
		streamBucketLastFlushedTsMap:          make(map[common.StreamId]BucketLastFlushedTsMap),
		streamBucketLastSnapAlignFlushedTsMap: make(map[common.StreamId]BucketLastFlushedTsMap),
		streamBucketRestartTsMap:              make(map[common.StreamId]BucketRestartTsMap),
		streamBucketFlushEnabledMap:           make(map[common.StreamId]BucketFlushEnabledMap),
		streamBucketDrainEnabledMap:           make(map[common.StreamId]BucketDrainEnabledMap),
		streamBucketVbStatusMap:               make(map[common.StreamId]BucketVbStatusMap),
		streamBucketVbRefCountMap:             make(map[common.StreamId]BucketVbRefCountMap),
		streamBucketRestartVbErrMap:           make(map[common.StreamId]BucketRestartVbErrMap),
		streamBucketRestartVbTsMap:            make(map[common.StreamId]BucketRestartVbTsMap),
		streamBucketRestartVbRetryMap:         make(map[common.StreamId]BucketRestartVbRetryMap),
		streamStatus:                          make(map[common.StreamId]StreamStatus),
		streamBucketStatus:                    make(map[common.StreamId]BucketStatus),
		streamBucketIndexCountMap:             make(map[common.StreamId]BucketIndexCountMap),
		streamBucketRepairStopCh:              make(map[common.StreamId]BucketRepairStopCh),
		streamBucketTimerStopCh:               make(map[common.StreamId]BucketTimerStopCh),
		streamBucketLastPersistTime:           make(map[common.StreamId]BucketLastPersistTime),
	}

	return ss

}

func (ss *StreamState) initNewStream(streamId common.StreamId) {

	//init all internal maps for this stream
	bucketHWTMap := make(BucketHWTMap)
	ss.streamBucketHWTMap[streamId] = bucketHWTMap

	bucketInMemTsCountMap := make(BucketInMemTsCountMap)
	ss.streamBucketInMemTsCountMap[streamId] = bucketInMemTsCountMap

	bucketNewTsReqdMap := make(BucketNewTsReqdMap)
	ss.streamBucketNewTsReqdMap[streamId] = bucketNewTsReqdMap

	bucketRestartTsMap := make(BucketRestartTsMap)
	ss.streamBucketRestartTsMap[streamId] = bucketRestartTsMap

	bucketTsListMap := make(BucketTsListMap)
	ss.streamBucketTsListMap[streamId] = bucketTsListMap

	bucketFlushInProgressTsMap := make(BucketFlushInProgressTsMap)
	ss.streamBucketFlushInProgressTsMap[streamId] = bucketFlushInProgressTsMap

	bucketAbortInProgressMap := make(BucketAbortInProgressMap)
	ss.streamBucketAbortInProgressMap[streamId] = bucketAbortInProgressMap

	bucketLastFlushedTsMap := make(BucketLastFlushedTsMap)
	ss.streamBucketLastFlushedTsMap[streamId] = bucketLastFlushedTsMap

	bucketLastSnapAlignFlushedTsMap := make(BucketLastFlushedTsMap)
	ss.streamBucketLastSnapAlignFlushedTsMap[streamId] = bucketLastSnapAlignFlushedTsMap

	bucketFlushEnabledMap := make(BucketFlushEnabledMap)
	ss.streamBucketFlushEnabledMap[streamId] = bucketFlushEnabledMap

	bucketDrainEnabledMap := make(BucketDrainEnabledMap)
	ss.streamBucketDrainEnabledMap[streamId] = bucketDrainEnabledMap

	bucketVbStatusMap := make(BucketVbStatusMap)
	ss.streamBucketVbStatusMap[streamId] = bucketVbStatusMap

	bucketVbRefCountMap := make(BucketVbRefCountMap)
	ss.streamBucketVbRefCountMap[streamId] = bucketVbRefCountMap

	bucketRestartVbRetryMap := make(BucketRestartVbRetryMap)
	ss.streamBucketRestartVbRetryMap[streamId] = bucketRestartVbRetryMap

	bucketRestartVbErrMap := make(BucketRestartVbErrMap)
	ss.streamBucketRestartVbErrMap[streamId] = bucketRestartVbErrMap

	bucketRestartVbTsMap := make(BucketRestartVbTsMap)
	ss.streamBucketRestartVbTsMap[streamId] = bucketRestartVbTsMap

	bucketIndexCountMap := make(BucketIndexCountMap)
	ss.streamBucketIndexCountMap[streamId] = bucketIndexCountMap

	bucketRepairStopChMap := make(BucketRepairStopCh)
	ss.streamBucketRepairStopCh[streamId] = bucketRepairStopChMap

	bucketTimerStopChMap := make(BucketTimerStopCh)
	ss.streamBucketTimerStopCh[streamId] = bucketTimerStopChMap

	bucketLastPersistTime := make(BucketLastPersistTime)
	ss.streamBucketLastPersistTime[streamId] = bucketLastPersistTime

	bucketStatus := make(BucketStatus)
	ss.streamBucketStatus[streamId] = bucketStatus

	ss.streamStatus[streamId] = STREAM_ACTIVE

}

func (ss *StreamState) initBucketInStream(streamId common.StreamId,
	bucket string) {

	numVbuckets := ss.config["numVbuckets"].Int()
	ss.streamBucketHWTMap[streamId][bucket] = common.NewTsVbuuid(bucket, numVbuckets)
	ss.streamBucketInMemTsCountMap[streamId][bucket] = 0
	ss.streamBucketNewTsReqdMap[streamId][bucket] = false
	ss.streamBucketFlushInProgressTsMap[streamId][bucket] = nil
	ss.streamBucketAbortInProgressMap[streamId][bucket] = false
	ss.streamBucketTsListMap[streamId][bucket] = list.New()
	ss.streamBucketLastFlushedTsMap[streamId][bucket] = nil
	ss.streamBucketLastSnapAlignFlushedTsMap[streamId][bucket] = nil
	ss.streamBucketFlushEnabledMap[streamId][bucket] = true
	ss.streamBucketDrainEnabledMap[streamId][bucket] = true
	ss.streamBucketVbStatusMap[streamId][bucket] = NewTimestamp(numVbuckets)
	ss.streamBucketVbRefCountMap[streamId][bucket] = NewTimestamp(numVbuckets)
	ss.streamBucketRestartVbRetryMap[streamId][bucket] = NewTimestamp(numVbuckets)
	ss.streamBucketRestartVbTsMap[streamId][bucket] = nil
	ss.streamBucketRestartVbErrMap[streamId][bucket] = false
	ss.streamBucketIndexCountMap[streamId][bucket] = 0
	ss.streamBucketRepairStopCh[streamId][bucket] = nil
	ss.streamBucketTimerStopCh[streamId][bucket] = make(StopChannel)
	ss.streamBucketLastPersistTime[streamId][bucket] = time.Now()
	ss.streamBucketRestartTsMap[streamId][bucket] = nil

	ss.streamBucketStatus[streamId][bucket] = STREAM_ACTIVE

	logging.Debugf("StreamState::initBucketInStream \n\tNew Bucket %v Added for "+
		"Stream %v", bucket, streamId)
}

func (ss *StreamState) cleanupBucketFromStream(streamId common.StreamId,
	bucket string) {

	//if there is any ongoing repair for this bucket, abort that
	if stopCh, ok := ss.streamBucketRepairStopCh[streamId][bucket]; ok && stopCh != nil {
		close(stopCh)
	}

	delete(ss.streamBucketHWTMap[streamId], bucket)
	delete(ss.streamBucketInMemTsCountMap[streamId], bucket)
	delete(ss.streamBucketNewTsReqdMap[streamId], bucket)
	delete(ss.streamBucketTsListMap[streamId], bucket)
	delete(ss.streamBucketFlushInProgressTsMap[streamId], bucket)
	delete(ss.streamBucketAbortInProgressMap[streamId], bucket)
	delete(ss.streamBucketLastFlushedTsMap[streamId], bucket)
	delete(ss.streamBucketLastSnapAlignFlushedTsMap[streamId], bucket)
	delete(ss.streamBucketFlushEnabledMap[streamId], bucket)
	delete(ss.streamBucketDrainEnabledMap[streamId], bucket)
	delete(ss.streamBucketVbStatusMap[streamId], bucket)
	delete(ss.streamBucketVbRefCountMap[streamId], bucket)
	delete(ss.streamBucketRestartVbRetryMap[streamId], bucket)
	delete(ss.streamBucketRestartVbErrMap[streamId], bucket)
	delete(ss.streamBucketRestartVbTsMap[streamId], bucket)
	delete(ss.streamBucketIndexCountMap[streamId], bucket)
	delete(ss.streamBucketRepairStopCh[streamId], bucket)
	delete(ss.streamBucketTimerStopCh[streamId], bucket)
	delete(ss.streamBucketLastPersistTime[streamId], bucket)
	delete(ss.streamBucketRestartTsMap[streamId], bucket)

	ss.streamBucketStatus[streamId][bucket] = STREAM_INACTIVE

	logging.Debugf("StreamState::cleanupBucketFromStream \n\tBucket %v Deleted from "+
		"Stream %v", bucket, streamId)

}

func (ss *StreamState) resetStreamState(streamId common.StreamId) {

	//delete this stream from internal maps
	delete(ss.streamBucketHWTMap, streamId)
	delete(ss.streamBucketInMemTsCountMap, streamId)
	delete(ss.streamBucketNewTsReqdMap, streamId)
	delete(ss.streamBucketTsListMap, streamId)
	delete(ss.streamBucketFlushInProgressTsMap, streamId)
	delete(ss.streamBucketAbortInProgressMap, streamId)
	delete(ss.streamBucketLastFlushedTsMap, streamId)
	delete(ss.streamBucketLastSnapAlignFlushedTsMap, streamId)
	delete(ss.streamBucketFlushEnabledMap, streamId)
	delete(ss.streamBucketDrainEnabledMap, streamId)
	delete(ss.streamBucketVbStatusMap, streamId)
	delete(ss.streamBucketVbRefCountMap, streamId)
	delete(ss.streamBucketRestartVbRetryMap, streamId)
	delete(ss.streamBucketRestartVbErrMap, streamId)
	delete(ss.streamBucketRestartVbTsMap, streamId)
	delete(ss.streamBucketIndexCountMap, streamId)
	delete(ss.streamBucketLastPersistTime, streamId)
	delete(ss.streamBucketStatus, streamId)
	delete(ss.streamBucketRestartTsMap, streamId)

	ss.streamStatus[streamId] = STREAM_INACTIVE

	logging.Debugf("StreamState::resetStreamState \n\tReset Stream %v State", streamId)
}

func (ss *StreamState) getVbStatus(streamId common.StreamId, bucket string, vb Vbucket) VbStatus {

	return VbStatus(ss.streamBucketVbStatusMap[streamId][bucket][vb])
}

func (ss *StreamState) updateVbStatus(streamId common.StreamId, bucket string,
	vbList []Vbucket, status VbStatus) {

	for _, vb := range vbList {
		vbs := ss.streamBucketVbStatusMap[streamId][bucket]
		vbs[vb] = Seqno(status)
	}

}

func (ss *StreamState) clearRestartVbRetry(streamId common.StreamId, bucket string, vb Vbucket) {

	ss.streamBucketRestartVbRetryMap[streamId][bucket][vb] = Seqno(0)
}

func (ss *StreamState) markRestartVbError(streamId common.StreamId, bucket string) {

	ss.streamBucketRestartVbErrMap[streamId][bucket] = true
}

func (ss *StreamState) clearRestartVbError(streamId common.StreamId, bucket string) {

	ss.streamBucketRestartVbErrMap[streamId][bucket] = false
}

func (ss *StreamState) getVbRefCount(streamId common.StreamId, bucket string, vb Vbucket) int {

	return int(ss.streamBucketVbRefCountMap[streamId][bucket][vb])
}

func (ss *StreamState) clearVbRefCount(streamId common.StreamId, bucket string, vb Vbucket) {

	ss.streamBucketVbRefCountMap[streamId][bucket][vb] = Seqno(0)
}

func (ss *StreamState) incVbRefCount(streamId common.StreamId, bucket string, vb Vbucket) {

	vbs := ss.streamBucketVbRefCountMap[streamId][bucket]
	vbs[vb] = Seqno(int(vbs[vb]) + 1)
}

func (ss *StreamState) decVbRefCount(streamId common.StreamId, bucket string, vb Vbucket) {

	vbs := ss.streamBucketVbRefCountMap[streamId][bucket]
	vbs[vb] = Seqno(int(vbs[vb]) - 1)
}

//computes the restart Ts for the given bucket and stream
func (ss *StreamState) computeRestartTs(streamId common.StreamId,
	bucket string) *common.TsVbuuid {

	var restartTs *common.TsVbuuid

	if fts, ok := ss.streamBucketLastFlushedTsMap[streamId][bucket]; ok && fts != nil {
		restartTs = ss.streamBucketLastFlushedTsMap[streamId][bucket].Copy()
	} else if ts, ok := ss.streamBucketRestartTsMap[streamId][bucket]; ok && ts != nil {
		//if no flush has been done yet, use restart TS
		restartTs = ss.streamBucketRestartTsMap[streamId][bucket].Copy()
	}
	return restartTs
}

func (ss *StreamState) setHWTFromRestartTs(streamId common.StreamId,
	bucket string) {

	logging.Debugf("StreamState::setHWTFromRestartTs Stream %v "+
		"Bucket %v", streamId, bucket)

	if bucketRestartTs, ok := ss.streamBucketRestartTsMap[streamId]; ok {

		if restartTs, ok := bucketRestartTs[bucket]; ok && restartTs != nil {

			//update HWT
			ss.streamBucketHWTMap[streamId][bucket] = restartTs.Copy()

			//update Last Flushed Ts
			ss.streamBucketLastFlushedTsMap[streamId][bucket] = restartTs.Copy()
			logging.Debugf("StreamState::setHWTFromRestartTs \n\tHWT Set For "+
				"Bucket %v StreamId %v. TS %v.", bucket, streamId, restartTs)

		} else {
			logging.Warnf("StreamState::setHWTFromRestartTs \n\tRestartTs Not Found For "+
				"Bucket %v StreamId %v. No Value Set.", bucket, streamId)
		}
	}
}

// This function gets the list of vb and seqno to repair stream.
// Termination condition for stream repair:
// 1) All vb are in StreamBegin state
// 2) All vb have ref count == 1
// 3) There is no error in stream repair
func (ss *StreamState) getRepairTsForBucket(streamId common.StreamId,
	bucket string) (*common.TsVbuuid, bool, []Vbucket) {

	// always repair if the last repair is not successful
	anythingToRepair := ss.streamBucketRestartVbErrMap[streamId][bucket]

	numVbuckets := ss.config["numVbuckets"].Int()
	repairTs := common.NewTsVbuuid(bucket, numVbuckets)
	var shutdownVbs []Vbucket = nil
	var count = 0

	hwtTs := ss.streamBucketHWTMap[streamId][bucket]
	hasConnError := ss.hasConnectionError(streamId, bucket)

	// First step : Find out if there is any StreamEnd or ConnError on any vb.
	for i, s := range ss.streamBucketVbStatusMap[streamId][bucket] {
		if s == VBS_STREAM_END || s == VBS_CONN_ERROR {

			ss.addRepairTs(repairTs, hwtTs, Vbucket(i))
			count++
			anythingToRepair = true
			if hasConnError {
				// Make sure that we shutdown vb for BOTH StreamEnd and
				// ConnErr.  This is to ensure to cover the case where
				// indexer may miss a StreamBegin from the new owner
				// due to connection error. Dataport will not be able
				// to tell indexer that vb needs to start since
				// StreamBegin never arrives.
				shutdownVbs = append(shutdownVbs, Vbucket(i))
			}
		}
	}

	// Second step: Find out if any StreamEnd over max retry limit.  If so,
	// add it to ShutdownVbs (for shutdown/restart).  Only need to do this
	// if there is no vb marked with conn error because vb with StreamEnd
	// would already be in shutdownVbs, if there is connErr.
	if !hasConnError {
		for i, s := range ss.streamBucketVbStatusMap[streamId][bucket] {
			if s == VBS_STREAM_END {
				vbs := ss.streamBucketRestartVbRetryMap[streamId][bucket]
				vbs[i] = Seqno(int(vbs[i]) + 1)

				if int(vbs[i]) > REPAIR_RETRY_BEFORE_SHUTDOWN {
					logging.Debugf("StreamState::getRepairTsForBucket\n\t"+
						"Bucket %v StreamId %v Vbucket %v repair is being retried for %v times.",
						bucket, streamId, i, vbs[i])
					ss.clearRestartVbRetry(streamId, bucket, Vbucket(i))
					shutdownVbs = append(shutdownVbs, Vbucket(i))
				}
			}
		}
	}

	// Third step: If there is nothing to repair, then double check if every vb has
	// exactly one vb owner.  If not, then the accounting is wrong (most likely due
	// to connection error).  Make the vb as ConnErr and continue to repair.
	// Note: This will also take care of vb in VBS_INIT state.
	if !anythingToRepair {
		for i, _ := range ss.streamBucketVbStatusMap[streamId][bucket] {
			count := ss.streamBucketVbRefCountMap[streamId][bucket][i]
			if count != 1 {
				logging.Debugf("StreamState::getRepairTsForBucket\n\t"+
					"Bucket %v StreamId %v Vbucket %v have ref count (%v != 1). Convert to CONN_ERROR.",
					bucket, streamId, i, count)
				// Make it a ConnErr such that subsequent retry will
				// force a shutdown/restart sequence.
				ss.makeConnectionError(streamId, bucket, Vbucket(i))
				ss.addRepairTs(repairTs, hwtTs, Vbucket(i))
				count++
				shutdownVbs = append(shutdownVbs, Vbucket(i))
				anythingToRepair = true
			}
		}
	}

	// Forth Step: If there is something to repair, but indexer has received StreamBegin for
	// all vb, then retry with the last timestamp.
	if anythingToRepair && count == 0 {
		logging.Debugf("StreamState::getRepairTsForBucket\n\t"+
			"Bucket %v StreamId %v previous repair fails. Retry using previous repairTs",
			bucket, streamId)

		ts := ss.streamBucketRestartVbTsMap[streamId][bucket]
		if ts != nil {
			repairTs = ts.Copy()
		} else {
			repairTs = hwtTs.Copy()
		}

		shutdownVbs = nil
		vbnos := repairTs.GetVbnos()
		for _, vbno := range vbnos {
			shutdownVbs = append(shutdownVbs, Vbucket(vbno))
		}
	}

	if !anythingToRepair {
		ss.streamBucketRestartVbTsMap[streamId][bucket] = nil
		ss.clearRestartVbError(streamId, bucket)
	} else {
		ss.streamBucketRestartVbTsMap[streamId][bucket] = repairTs.Copy()
	}

	ss.adjustNonSnapAlignedVbs(repairTs, streamId, bucket)

	return repairTs, anythingToRepair, shutdownVbs
}

func (ss *StreamState) hasConnectionError(streamId common.StreamId, bucket string) bool {

	for _, s := range ss.streamBucketVbStatusMap[streamId][bucket] {
		if s == VBS_CONN_ERROR {
			return true
		}
	}
	return false
}

//
// When making a vb to have connection error, the ref count must also set to 0.  No counting
// will start until it gets its first StreamBegin message.
//
func (ss *StreamState) makeConnectionError(streamId common.StreamId, bucket string, vbno Vbucket) {

	ss.clearVbRefCount(streamId, bucket, vbno)
	ss.streamBucketVbStatusMap[streamId][bucket][vbno] = VBS_CONN_ERROR
	ss.clearRestartVbRetry(streamId, bucket, vbno)
}

func (ss *StreamState) addRepairTs(repairTs *common.TsVbuuid, hwtTs *common.TsVbuuid, vbno Vbucket) {

	repairTs.Seqnos[vbno] = hwtTs.Seqnos[vbno]
	repairTs.Vbuuids[vbno] = hwtTs.Vbuuids[vbno]
	repairTs.Snapshots[vbno][0] = hwtTs.Snapshots[vbno][0]
	repairTs.Snapshots[vbno][1] = hwtTs.Snapshots[vbno][1]
}

//If a snapshot marker has been received but no mutation for that snapshot,
//the repairTs seqno will be outside the snapshot marker range and
//DCP will refuse to accept such seqno for restart. Such VBs need to
//use lastFlushTs or restartTs.
func (ss *StreamState) adjustNonSnapAlignedVbs(repairTs *common.TsVbuuid,
	streamId common.StreamId, bucket string) {

	for _, vbno := range repairTs.GetVbnos() {
		seqno := repairTs.Seqnos[vbno]
		snapBegin := repairTs.Snapshots[vbno][0]
		snapEnd := repairTs.Snapshots[vbno][1]
		if !(seqno >= snapBegin && seqno <= snapEnd) {
			// First, use the last flush TS seqno if avaliable
			if fts, ok := ss.streamBucketLastFlushedTsMap[streamId][bucket]; ok && fts != nil {
				repairTs.Snapshots[vbno][0] = fts.Snapshots[vbno][0]
				repairTs.Snapshots[vbno][1] = fts.Snapshots[vbno][1]
				repairTs.Seqnos[vbno] = fts.Seqnos[vbno]
				snapBegin = repairTs.Snapshots[vbno][0]
				snapEnd = repairTs.Snapshots[vbno][1]
			}

			// If last flush TS is still out-of-bound, use last Snap-aligned flushed TS if available
			if !(repairTs.Seqnos[vbno] >= snapBegin && repairTs.Seqnos[vbno] <= snapEnd) {
				if fts, ok := ss.streamBucketLastSnapAlignFlushedTsMap[streamId][bucket]; ok && fts != nil {
					repairTs.Snapshots[vbno][0] = fts.Snapshots[vbno][0]
					repairTs.Snapshots[vbno][1] = fts.Snapshots[vbno][1]
					repairTs.Seqnos[vbno] = fts.Seqnos[vbno]
					snapBegin = repairTs.Snapshots[vbno][0]
					snapEnd = repairTs.Snapshots[vbno][1]
				}
			}

			// If last snap-aligned flushed TS is not avail, then use restartTS
			if !(repairTs.Seqnos[vbno] >= snapBegin && repairTs.Seqnos[vbno] <= snapEnd) {
				if rts, ok := ss.streamBucketRestartTsMap[streamId][bucket]; ok && rts != nil {
					//if no flush has been done yet, use restart TS
					repairTs.Snapshots[vbno][0] = rts.Snapshots[vbno][0]
					repairTs.Snapshots[vbno][1] = rts.Snapshots[vbno][1]
					repairTs.Seqnos[vbno] = rts.Seqnos[vbno]
					snapBegin = repairTs.Snapshots[vbno][0]
					snapEnd = repairTs.Snapshots[vbno][1]
				}
			}

			//if seqno is still not with snapshot range, then it is likely a bug in state management
			if !(repairTs.Seqnos[vbno] >= snapBegin && repairTs.Seqnos[vbno] <= snapEnd) {
				common.CrashOnError(errors.New("No Valid Restart Seqno Found"))
			}
		}
	}

}

func (ss *StreamState) checkAllStreamBeginsReceived(streamId common.StreamId,
	bucket string) bool {

	if bucketVbStatusMap, ok := ss.streamBucketVbStatusMap[streamId]; ok {
		if vbs, ok := bucketVbStatusMap[bucket]; ok {
			for _, v := range vbs {
				if v != VBS_STREAM_BEGIN {
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

//checks snapshot markers have been received for all vbuckets where
//the buildTs is non-nil.
func (ss *StreamState) checkAllSnapMarkersReceived(streamId common.StreamId,
	bucket string, buildTs Timestamp) bool {

	ts := ss.streamBucketHWTMap[streamId][bucket]

	//all snapshot markers should be present, except if buildTs has 0
	//seqnum for the vbucket
	for i, s := range ts.Snapshots {
		if s[1] == 0 && buildTs[i] != 0 {
			return false
		}
	}
	return true
}

//checks if the given bucket in this stream has any flush in progress
func (ss *StreamState) checkAnyFlushPending(streamId common.StreamId,
	bucket string) bool {

	bucketFlushInProgressTsMap := ss.streamBucketFlushInProgressTsMap[streamId]

	//check if there is any flush in progress. No flush in progress implies
	//there is no flush pending as well bcoz every flush done triggers
	//the next flush.
	if ts := bucketFlushInProgressTsMap[bucket]; ts != nil {
		return true
	}
	return false
}

//checks if none of the buckets in this stream has any flush abort in progress
func (ss *StreamState) checkAnyAbortPending(streamId common.StreamId,
	bucket string) bool {

	bucketAbortInProgressMap := ss.streamBucketAbortInProgressMap[streamId]

	if running := bucketAbortInProgressMap[bucket]; running {
		return true
	}
	return false
}

//updateHWT will update the HW Timestamp for a bucket in the stream
//based on the Sync message received.
func (ss *StreamState) updateHWT(streamId common.StreamId,
	bucket string, hwt *common.TsVbuuid) {

	ts := ss.streamBucketHWTMap[streamId][bucket]

	for i, seq := range hwt.Seqnos {
		//if seqno has incremented, update it
		if seq > ts.Seqnos[i] {
			ts.Seqnos[i] = seq
			ss.streamBucketNewTsReqdMap[streamId][bucket] = true
		}
		//if snapEnd is greater than current hwt snapEnd
		if hwt.Snapshots[i][1] > ts.Snapshots[i][1] {
			ts.Snapshots[i][0] = hwt.Snapshots[i][0]
			ts.Snapshots[i][1] = hwt.Snapshots[i][1]
			ss.streamBucketNewTsReqdMap[streamId][bucket] = true
		}
	}

	logging.LazyTrace(func() string {
		return fmt.Sprintf("StreamState::updateHWT HWT Updated : %v", ts)
	})
}

func (ss *StreamState) checkNewTSDue(streamId common.StreamId, bucket string) bool {
	newTsReqd := ss.streamBucketNewTsReqdMap[streamId][bucket]
	return newTsReqd
}

//gets the stability timestamp based on the current HWT
func (ss *StreamState) getNextStabilityTS(streamId common.StreamId,
	bucket string) *common.TsVbuuid {

	//generate new stability timestamp
	tsVbuuid := ss.streamBucketHWTMap[streamId][bucket].Copy()

	//reset state for next TS
	ss.streamBucketNewTsReqdMap[streamId][bucket] = false

	if tsVbuuid.CheckSnapAligned() {
		tsVbuuid.SetSnapAligned(true)
	}

	return tsVbuuid
}

//check for presence of large snapshot in a TS and set the flag
func (ss *StreamState) checkLargeSnapshot(ts *common.TsVbuuid) {

	largeSnapshotThreshold := ss.config["settings.largeSnapshotThreshold"].Uint64()
	for _, s := range ts.Snapshots {
		if s[1]-s[0] > largeSnapshotThreshold {
			ts.SetLargeSnapshot(true)
			return
		}
	}

}

func (ss *StreamState) canFlushNewTS(streamId common.StreamId,
	bucket string) bool {

	bucketFlushInProgressTsMap := ss.streamBucketFlushInProgressTsMap[streamId]
	bucketTsListMap := ss.streamBucketTsListMap[streamId]
	bucketFlushEnabledMap := ss.streamBucketFlushEnabledMap[streamId]

	//if there is no flush already in progress for this bucket
	//no pending TS in list and flush is not disabled, send new TS
	tsList := bucketTsListMap[bucket]
	if bucketFlushInProgressTsMap[bucket] == nil &&
		bucketFlushEnabledMap[bucket] == true &&
		tsList.Len() == 0 {
		return true
	}

	return false

}

//computes which vbuckets have mutations compared to last flush
func (ss *StreamState) computeTsChangeVec(streamId common.StreamId,
	bucket string, ts *common.TsVbuuid) ([]bool, bool) {

	numVbuckets := len(ts.Snapshots)
	changeVec := make([]bool, numVbuckets)
	noChange := true

	//if there is a lastFlushedTs, compare with that
	if lts, ok := ss.streamBucketLastFlushedTsMap[streamId][bucket]; ok && lts != nil {

		for i, s := range ts.Seqnos {
			//if currentTs has seqno greater than last flushed
			if s > lts.Seqnos[i] {
				changeVec[i] = true
				noChange = false
			}
			//if vbuuid has changed, consider that as a change as well.
			//new snapshot is required for stale=false
			if ts.Vbuuids[i] != lts.Vbuuids[i] {
				noChange = false
			}
		}

	} else {

		//if this is the first ts, check seqno > 0
		for i, s := range ts.Seqnos {
			if s > 0 {
				changeVec[i] = true
				noChange = false
			}
		}
	}

	return changeVec, noChange
}

func (ss *StreamState) UpdateConfig(cfg common.Config) {
	ss.config = cfg
}

//helper function to update Seqnos in TsVbuuid to
//high seqnum of snapshot markers
func updateTsSeqNumToSnapshot(ts *common.TsVbuuid) {

	for i, s := range ts.Snapshots {
		ts.Seqnos[i] = s[1]
	}

}
