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
	"fmt"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
)

type StreamState struct {
	config common.Config

	streamStatus              map[common.StreamId]StreamStatus
	streamBucketStatus        map[common.StreamId]BucketStatus
	streamBucketVbStatusMap   map[common.StreamId]BucketVbStatusMap
	streamBucketVbRefCountMap map[common.StreamId]BucketVbRefCountMap

	streamBucketHWTMap            map[common.StreamId]BucketHWTMap
	streamBucketNeedsCommitMap    map[common.StreamId]BucketNeedsCommitMap
	streamBucketHasBuildCompTSMap map[common.StreamId]BucketHasBuildCompTSMap
	streamBucketNewTsReqdMap      map[common.StreamId]BucketNewTsReqdMap
	streamBucketTsListMap         map[common.StreamId]BucketTsListMap
	streamBucketLastFlushedTsMap  map[common.StreamId]BucketLastFlushedTsMap
	streamBucketRestartTsMap      map[common.StreamId]BucketRestartTsMap
	streamBucketOpenTsMap         map[common.StreamId]BucketOpenTsMap
	streamBucketStartTimeMap      map[common.StreamId]BucketStartTimeMap
	streamBucketLastSnapMarker    map[common.StreamId]BucketLastSnapMarker

	streamBucketLastSnapAlignFlushedTsMap map[common.StreamId]BucketLastFlushedTsMap
	streamBucketLastMutationVbuuid        map[common.StreamId]BucketLastMutationVbuuid

	streamBucketRestartVbTsMap map[common.StreamId]BucketRestartVbTsMap

	streamBucketFlushInProgressTsMap map[common.StreamId]BucketFlushInProgressTsMap
	streamBucketAbortInProgressMap   map[common.StreamId]BucketAbortInProgressMap
	streamBucketFlushEnabledMap      map[common.StreamId]BucketFlushEnabledMap
	streamBucketDrainEnabledMap      map[common.StreamId]BucketDrainEnabledMap
	streamBucketFlushDone            map[common.StreamId]BucketFlushDone
	streamBucketForceRecovery        map[common.StreamId]BucketForceRecovery

	streamBucketIndexCountMap   map[common.StreamId]BucketIndexCountMap
	streamBucketRepairStopCh    map[common.StreamId]BucketRepairStopCh
	streamBucketTimerStopCh     map[common.StreamId]BucketTimerStopCh
	streamBucketLastPersistTime map[common.StreamId]BucketLastPersistTime
	streamBucketSkippedInMemTs  map[common.StreamId]BucketSkippedInMemTs
	streamBucketHasInMemSnap    map[common.StreamId]BucketHasInMemSnap
	streamBucketSessionId       map[common.StreamId]BucketSessionId

	streamBucketAsyncMap map[common.StreamId]BucketStreamAsyncMap

	streamBucketKVRollbackTsMap map[common.StreamId]BucketKVRollbackTsMap
	streamBucketKVActiveTsMap   map[common.StreamId]BucketKVActiveTsMap
	streamBucketKVPendingTsMap  map[common.StreamId]BucketKVPendingTsMap

	// state for managing vb repair
	// 1) When a vb needs a repair, RepairTime is updated.
	// 2) A repair action is performed.  Once the action is completed, RepairState is updated.
	// 3) If RepairTime exceeds waitTime (action not effective), escalate to the next action by updating repairTime.
	streamBucketLastBeginTime     map[common.StreamId]BucketStreamLastBeginTime
	streamBucketLastRepairTimeMap map[common.StreamId]BucketStreamLastRepairTimeMap
	streamBucketRepairStateMap    map[common.StreamId]BucketStreamRepairStateMap

	// Maintains the mapping between vbucket to kv node UUID
	// for each bucket, for each stream
	streamBucketVBMap map[common.StreamId]BucketVBMap

	bucketRollbackTime map[string]int64
}

type BucketHWTMap map[string]*common.TsVbuuid
type BucketLastFlushedTsMap map[string]*common.TsVbuuid
type BucketRestartTsMap map[string]*common.TsVbuuid
type BucketOpenTsMap map[string]*common.TsVbuuid
type BucketStartTimeMap map[string]uint64
type BucketNeedsCommitMap map[string]bool
type BucketHasBuildCompTSMap map[string]bool
type BucketNewTsReqdMap map[string]bool
type BucketLastSnapMarker map[string]*common.TsVbuuid
type BucketLastMutationVbuuid map[string]*common.TsVbuuid

type BucketTsListMap map[string]*list.List
type BucketFlushInProgressTsMap map[string]*common.TsVbuuid
type BucketAbortInProgressMap map[string]bool
type BucketFlushEnabledMap map[string]bool
type BucketDrainEnabledMap map[string]bool
type BucketFlushDone map[string]DoneChannel
type BucketForceRecovery map[string]bool

type BucketRestartVbTsMap map[string]*common.TsVbuuid

type BucketVbStatusMap map[string]Timestamp
type BucketVbRefCountMap map[string]Timestamp
type BucketRepairStopCh map[string]StopChannel
type BucketTimerStopCh map[string]StopChannel
type BucketLastPersistTime map[string]time.Time
type BucketSkippedInMemTs map[string]uint64
type BucketHasInMemSnap map[string]bool
type BucketSessionId map[string]uint64

type BucketStreamAsyncMap map[string]bool
type BucketStreamLastBeginTime map[string]uint64
type BucketStreamLastRepairTimeMap map[string]Timestamp
type BucketKVRollbackTsMap map[string]*common.TsVbuuid
type BucketKVActiveTsMap map[string]*common.TsVbuuid
type BucketKVPendingTsMap map[string]*common.TsVbuuid
type BucketStreamRepairStateMap map[string][]RepairState

type BucketStatus map[string]StreamStatus

type BucketVBMap map[string]map[Vbucket]string

type RepairState byte

const (
	REPAIR_NONE RepairState = iota
	REPAIR_RESTART_VB
	REPAIR_SHUTDOWN_VB
	REPAIR_MTR
	REPAIR_RECOVERY
)

func InitStreamState(config common.Config) *StreamState {

	ss := &StreamState{
		config:                                config,
		streamBucketHWTMap:                    make(map[common.StreamId]BucketHWTMap),
		streamBucketNeedsCommitMap:            make(map[common.StreamId]BucketNeedsCommitMap),
		streamBucketHasBuildCompTSMap:         make(map[common.StreamId]BucketHasBuildCompTSMap),
		streamBucketNewTsReqdMap:              make(map[common.StreamId]BucketNewTsReqdMap),
		streamBucketTsListMap:                 make(map[common.StreamId]BucketTsListMap),
		streamBucketFlushInProgressTsMap:      make(map[common.StreamId]BucketFlushInProgressTsMap),
		streamBucketAbortInProgressMap:        make(map[common.StreamId]BucketAbortInProgressMap),
		streamBucketLastFlushedTsMap:          make(map[common.StreamId]BucketLastFlushedTsMap),
		streamBucketLastSnapAlignFlushedTsMap: make(map[common.StreamId]BucketLastFlushedTsMap),
		streamBucketRestartTsMap:              make(map[common.StreamId]BucketRestartTsMap),

		streamBucketOpenTsMap:          make(map[common.StreamId]BucketOpenTsMap),
		streamBucketStartTimeMap:       make(map[common.StreamId]BucketStartTimeMap),
		streamBucketFlushEnabledMap:    make(map[common.StreamId]BucketFlushEnabledMap),
		streamBucketDrainEnabledMap:    make(map[common.StreamId]BucketDrainEnabledMap),
		streamBucketFlushDone:          make(map[common.StreamId]BucketFlushDone),
		streamBucketForceRecovery:      make(map[common.StreamId]BucketForceRecovery),
		streamBucketVbStatusMap:        make(map[common.StreamId]BucketVbStatusMap),
		streamBucketVbRefCountMap:      make(map[common.StreamId]BucketVbRefCountMap),
		streamBucketRestartVbTsMap:     make(map[common.StreamId]BucketRestartVbTsMap),
		streamStatus:                   make(map[common.StreamId]StreamStatus),
		streamBucketStatus:             make(map[common.StreamId]BucketStatus),
		streamBucketIndexCountMap:      make(map[common.StreamId]BucketIndexCountMap),
		streamBucketRepairStopCh:       make(map[common.StreamId]BucketRepairStopCh),
		streamBucketTimerStopCh:        make(map[common.StreamId]BucketTimerStopCh),
		streamBucketLastPersistTime:    make(map[common.StreamId]BucketLastPersistTime),
		streamBucketSkippedInMemTs:     make(map[common.StreamId]BucketSkippedInMemTs),
		streamBucketHasInMemSnap:       make(map[common.StreamId]BucketHasInMemSnap),
		streamBucketLastSnapMarker:     make(map[common.StreamId]BucketLastSnapMarker),
		streamBucketLastMutationVbuuid: make(map[common.StreamId]BucketLastMutationVbuuid),
		bucketRollbackTime:             make(map[string]int64),
		streamBucketAsyncMap:           make(map[common.StreamId]BucketStreamAsyncMap),
		streamBucketLastBeginTime:      make(map[common.StreamId]BucketStreamLastBeginTime),
		streamBucketLastRepairTimeMap:  make(map[common.StreamId]BucketStreamLastRepairTimeMap),
		streamBucketKVRollbackTsMap:    make(map[common.StreamId]BucketKVRollbackTsMap),
		streamBucketKVActiveTsMap:      make(map[common.StreamId]BucketKVActiveTsMap),
		streamBucketKVPendingTsMap:     make(map[common.StreamId]BucketKVPendingTsMap),
		streamBucketRepairStateMap:     make(map[common.StreamId]BucketStreamRepairStateMap),
		streamBucketSessionId:          make(map[common.StreamId]BucketSessionId),
		streamBucketVBMap:              make(map[common.StreamId]BucketVBMap),
	}

	return ss

}

func (ss *StreamState) initNewStream(streamId common.StreamId) {

	//init all internal maps for this stream
	bucketHWTMap := make(BucketHWTMap)
	ss.streamBucketHWTMap[streamId] = bucketHWTMap

	bucketNeedsCommitMap := make(BucketNeedsCommitMap)
	ss.streamBucketNeedsCommitMap[streamId] = bucketNeedsCommitMap

	bucketHasBuildCompTSMap := make(BucketHasBuildCompTSMap)
	ss.streamBucketHasBuildCompTSMap[streamId] = bucketHasBuildCompTSMap

	bucketNewTsReqdMap := make(BucketNewTsReqdMap)
	ss.streamBucketNewTsReqdMap[streamId] = bucketNewTsReqdMap

	bucketRestartTsMap := make(BucketRestartTsMap)
	ss.streamBucketRestartTsMap[streamId] = bucketRestartTsMap

	bucketOpenTsMap := make(BucketOpenTsMap)
	ss.streamBucketOpenTsMap[streamId] = bucketOpenTsMap

	bucketStartTimeMap := make(BucketStartTimeMap)
	ss.streamBucketStartTimeMap[streamId] = bucketStartTimeMap

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

	bucketFlushDone := make(BucketFlushDone)
	ss.streamBucketFlushDone[streamId] = bucketFlushDone

	bucketForceRecovery := make(BucketForceRecovery)
	ss.streamBucketForceRecovery[streamId] = bucketForceRecovery

	bucketVbStatusMap := make(BucketVbStatusMap)
	ss.streamBucketVbStatusMap[streamId] = bucketVbStatusMap

	bucketVbRefCountMap := make(BucketVbRefCountMap)
	ss.streamBucketVbRefCountMap[streamId] = bucketVbRefCountMap

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

	bucketSkippedInMemTs := make(BucketSkippedInMemTs)
	ss.streamBucketSkippedInMemTs[streamId] = bucketSkippedInMemTs

	bucketHasInMemSnap := make(BucketHasInMemSnap)
	ss.streamBucketHasInMemSnap[streamId] = bucketHasInMemSnap

	bucketSessionId := make(BucketSessionId)
	ss.streamBucketSessionId[streamId] = bucketSessionId

	bucketStatus := make(BucketStatus)
	ss.streamBucketStatus[streamId] = bucketStatus

	bucketLastSnapMarker := make(BucketLastSnapMarker)
	ss.streamBucketLastSnapMarker[streamId] = bucketLastSnapMarker

	bucketLastMutationVbuuid := make(BucketLastMutationVbuuid)
	ss.streamBucketLastMutationVbuuid[streamId] = bucketLastMutationVbuuid

	bucketStreamAsyncMap := make(BucketStreamAsyncMap)
	ss.streamBucketAsyncMap[streamId] = bucketStreamAsyncMap

	bucketStreamLastBeginTime := make(BucketStreamLastBeginTime)
	ss.streamBucketLastBeginTime[streamId] = bucketStreamLastBeginTime

	bucketStreamLastRepairTimeMap := make(BucketStreamLastRepairTimeMap)
	ss.streamBucketLastRepairTimeMap[streamId] = bucketStreamLastRepairTimeMap

	bucketKVRollbackTsMap := make(BucketKVRollbackTsMap)
	ss.streamBucketKVRollbackTsMap[streamId] = bucketKVRollbackTsMap

	bucketKVActiveTsMap := make(BucketKVActiveTsMap)
	ss.streamBucketKVActiveTsMap[streamId] = bucketKVActiveTsMap

	bucketKVPendingTsMap := make(BucketKVPendingTsMap)
	ss.streamBucketKVPendingTsMap[streamId] = bucketKVPendingTsMap

	bucketStreamRepairStateMap := make(BucketStreamRepairStateMap)
	ss.streamBucketRepairStateMap[streamId] = bucketStreamRepairStateMap

	bucketVBMap := make(BucketVBMap)
	ss.streamBucketVBMap[streamId] = bucketVBMap

	ss.streamStatus[streamId] = STREAM_ACTIVE

}

func (ss *StreamState) initBucketInStream(streamId common.StreamId,
	bucket string) {

	numVbuckets := ss.config["numVbuckets"].Int()
	ss.streamBucketHWTMap[streamId][bucket] = common.NewTsVbuuid(bucket, numVbuckets)
	ss.streamBucketNeedsCommitMap[streamId][bucket] = false
	ss.streamBucketHasBuildCompTSMap[streamId][bucket] = false
	ss.streamBucketNewTsReqdMap[streamId][bucket] = false
	ss.streamBucketFlushInProgressTsMap[streamId][bucket] = nil
	ss.streamBucketAbortInProgressMap[streamId][bucket] = false
	ss.streamBucketTsListMap[streamId][bucket] = list.New()
	ss.streamBucketLastFlushedTsMap[streamId][bucket] = nil
	ss.streamBucketLastSnapAlignFlushedTsMap[streamId][bucket] = nil
	ss.streamBucketFlushEnabledMap[streamId][bucket] = true
	ss.streamBucketDrainEnabledMap[streamId][bucket] = true
	ss.streamBucketFlushDone[streamId][bucket] = nil
	ss.streamBucketForceRecovery[streamId][bucket] = false
	ss.streamBucketVbStatusMap[streamId][bucket] = NewTimestamp(numVbuckets)
	ss.streamBucketVbRefCountMap[streamId][bucket] = NewTimestamp(numVbuckets)
	ss.streamBucketRestartVbTsMap[streamId][bucket] = nil
	ss.streamBucketIndexCountMap[streamId][bucket] = 0
	ss.streamBucketRepairStopCh[streamId][bucket] = nil
	ss.streamBucketTimerStopCh[streamId][bucket] = make(StopChannel)
	ss.streamBucketLastPersistTime[streamId][bucket] = time.Now()
	ss.streamBucketRestartTsMap[streamId][bucket] = nil
	ss.streamBucketOpenTsMap[streamId][bucket] = nil
	ss.streamBucketStartTimeMap[streamId][bucket] = uint64(0)
	ss.streamBucketSkippedInMemTs[streamId][bucket] = 0
	ss.streamBucketHasInMemSnap[streamId][bucket] = false
	ss.streamBucketSessionId[streamId][bucket] = 0
	ss.streamBucketLastSnapMarker[streamId][bucket] = common.NewTsVbuuid(bucket, numVbuckets)
	ss.streamBucketLastMutationVbuuid[streamId][bucket] = common.NewTsVbuuid(bucket, numVbuckets)
	ss.streamBucketAsyncMap[streamId][bucket] = false
	ss.streamBucketLastBeginTime[streamId][bucket] = 0
	ss.streamBucketLastRepairTimeMap[streamId][bucket] = NewTimestamp(numVbuckets)
	ss.streamBucketKVRollbackTsMap[streamId][bucket] = common.NewTsVbuuid(bucket, numVbuckets)
	ss.streamBucketKVActiveTsMap[streamId][bucket] = common.NewTsVbuuid(bucket, numVbuckets)
	ss.streamBucketKVPendingTsMap[streamId][bucket] = common.NewTsVbuuid(bucket, numVbuckets)
	ss.streamBucketRepairStateMap[streamId][bucket] = make([]RepairState, numVbuckets)
	ss.streamBucketVBMap[streamId][bucket] = make(map[Vbucket]string)

	ss.streamBucketStatus[streamId][bucket] = STREAM_ACTIVE

	logging.Infof("StreamState::initBucketInStream New Bucket %v Added for "+
		"Stream %v", bucket, streamId)
}

func (ss *StreamState) cleanupBucketFromStream(streamId common.StreamId,
	bucket string) {

	//if there is any ongoing repair for this bucket, abort that
	if stopCh, ok := ss.streamBucketRepairStopCh[streamId][bucket]; ok && stopCh != nil {
		close(stopCh)
	}

	delete(ss.streamBucketHWTMap[streamId], bucket)
	delete(ss.streamBucketNeedsCommitMap[streamId], bucket)
	delete(ss.streamBucketHasBuildCompTSMap[streamId], bucket)
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
	delete(ss.streamBucketRestartVbTsMap[streamId], bucket)
	delete(ss.streamBucketIndexCountMap[streamId], bucket)
	delete(ss.streamBucketRepairStopCh[streamId], bucket)
	delete(ss.streamBucketTimerStopCh[streamId], bucket)
	delete(ss.streamBucketLastPersistTime[streamId], bucket)
	delete(ss.streamBucketRestartTsMap[streamId], bucket)
	delete(ss.streamBucketOpenTsMap[streamId], bucket)
	delete(ss.streamBucketStartTimeMap[streamId], bucket)
	delete(ss.streamBucketLastSnapMarker[streamId], bucket)
	delete(ss.streamBucketLastMutationVbuuid[streamId], bucket)
	delete(ss.streamBucketSkippedInMemTs[streamId], bucket)
	delete(ss.streamBucketHasInMemSnap[streamId], bucket)
	delete(ss.streamBucketSessionId[streamId], bucket)
	delete(ss.streamBucketAsyncMap[streamId], bucket)
	delete(ss.streamBucketLastBeginTime[streamId], bucket)
	delete(ss.streamBucketLastRepairTimeMap[streamId], bucket)
	delete(ss.streamBucketKVRollbackTsMap[streamId], bucket)
	delete(ss.streamBucketKVActiveTsMap[streamId], bucket)
	delete(ss.streamBucketKVPendingTsMap[streamId], bucket)
	delete(ss.streamBucketRepairStateMap[streamId], bucket)
	delete(ss.streamBucketVBMap[streamId], bucket)

	if donech, ok := ss.streamBucketFlushDone[streamId][bucket]; ok && donech != nil {
		close(donech)
	}
	delete(ss.streamBucketFlushDone[streamId], bucket)
	delete(ss.streamBucketForceRecovery[streamId], bucket)

	if bs, ok := ss.streamBucketStatus[streamId]; ok && bs != nil {
		bs[bucket] = STREAM_INACTIVE
	}

	logging.Infof("StreamState::cleanupBucketFromStream Bucket %v Deleted from "+
		"Stream %v", bucket, streamId)

}

func (ss *StreamState) resetStreamState(streamId common.StreamId) {

	//delete this stream from internal maps
	delete(ss.streamBucketHWTMap, streamId)
	delete(ss.streamBucketNeedsCommitMap, streamId)
	delete(ss.streamBucketHasBuildCompTSMap, streamId)
	delete(ss.streamBucketNewTsReqdMap, streamId)
	delete(ss.streamBucketTsListMap, streamId)
	delete(ss.streamBucketFlushInProgressTsMap, streamId)
	delete(ss.streamBucketAbortInProgressMap, streamId)
	delete(ss.streamBucketLastFlushedTsMap, streamId)
	delete(ss.streamBucketLastSnapAlignFlushedTsMap, streamId)
	delete(ss.streamBucketFlushEnabledMap, streamId)
	delete(ss.streamBucketDrainEnabledMap, streamId)
	delete(ss.streamBucketFlushDone, streamId)
	delete(ss.streamBucketForceRecovery, streamId)
	delete(ss.streamBucketVbStatusMap, streamId)
	delete(ss.streamBucketVbRefCountMap, streamId)
	delete(ss.streamBucketRestartVbTsMap, streamId)
	delete(ss.streamBucketIndexCountMap, streamId)
	delete(ss.streamBucketLastPersistTime, streamId)
	delete(ss.streamBucketStatus, streamId)
	delete(ss.streamBucketRestartTsMap, streamId)
	delete(ss.streamBucketOpenTsMap, streamId)
	delete(ss.streamBucketStartTimeMap, streamId)
	delete(ss.streamBucketSkippedInMemTs, streamId)
	delete(ss.streamBucketHasInMemSnap, streamId)
	delete(ss.streamBucketSessionId, streamId)
	delete(ss.streamBucketLastSnapMarker, streamId)
	delete(ss.streamBucketLastMutationVbuuid, streamId)
	delete(ss.streamBucketAsyncMap, streamId)
	delete(ss.streamBucketLastBeginTime, streamId)
	delete(ss.streamBucketLastRepairTimeMap, streamId)
	delete(ss.streamBucketKVRollbackTsMap, streamId)
	delete(ss.streamBucketKVActiveTsMap, streamId)
	delete(ss.streamBucketKVPendingTsMap, streamId)
	delete(ss.streamBucketRepairStateMap, streamId)
	delete(ss.streamBucketVBMap, streamId)

	ss.streamStatus[streamId] = STREAM_INACTIVE

	logging.Infof("StreamState::resetStreamState Reset Stream %v State", streamId)
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

func (ss *StreamState) setRollbackTime(bucket string, rollbackTime int64) {
	if rollbackTime != 0 {
		ss.bucketRollbackTime[bucket] = rollbackTime
	}
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

//computes the rollback Ts for the given bucket and stream
func (ss *StreamState) computeRollbackTs(streamId common.StreamId, bucket string) *common.TsVbuuid {

	if rollTs, ok := ss.streamBucketKVRollbackTsMap[streamId][bucket]; ok && rollTs.Len() > 0 {
		// Get restart Ts
		restartTs := ss.computeRestartTs(streamId, bucket)
		if restartTs == nil {
			numVbuckets := ss.config["numVbuckets"].Int()
			restartTs = common.NewTsVbuuid(bucket, numVbuckets)
		} else {
			ss.adjustNonSnapAlignedVbs(restartTs, streamId, bucket, nil, false)
			ss.adjustVbuuids(restartTs, streamId, bucket)
		}

		// overlay KV rollback Ts on top of restart Ts
		return restartTs.Union(ss.streamBucketKVRollbackTsMap[streamId][bucket])
	}

	return nil
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
			logging.Verbosef("StreamState::setHWTFromRestartTs HWT Set For "+
				"Bucket %v StreamId %v. TS %v.", bucket, streamId, restartTs)

			ss.streamBucketLastMutationVbuuid[streamId][bucket] = restartTs.Copy()

		} else {
			logging.Warnf("StreamState::setHWTFromRestartTs RestartTs Not Found For "+
				"Bucket %v StreamId %v. No Value Set.", bucket, streamId)
		}
	}
}

func (ss *StreamState) updateRepairState(streamId common.StreamId,
	bucket string, repairVbs []Vbucket, shutdownVbs []Vbucket) {

	for _, vbno := range repairVbs {
		if ss.streamBucketVbStatusMap[streamId][bucket][vbno] == VBS_STREAM_END &&
			ss.streamBucketRepairStateMap[streamId][bucket][int(vbno)] == REPAIR_NONE {
			logging.Infof("StreamState::set repair state to RESTART_VB for %v bucket %v vb %v", streamId, bucket, vbno)
			ss.streamBucketRepairStateMap[streamId][bucket][int(vbno)] = REPAIR_RESTART_VB
		}
	}

	for _, vbno := range shutdownVbs {
		if ss.streamBucketVbStatusMap[streamId][bucket][vbno] == VBS_CONN_ERROR &&
			ss.streamBucketRepairStateMap[streamId][bucket][int(vbno)] == REPAIR_RESTART_VB {
			logging.Infof("StreamState::set repair state to SHUTDOWN_VB for %v bucket %v vb %v", streamId, bucket, vbno)
			ss.streamBucketRepairStateMap[streamId][bucket][int(vbno)] = REPAIR_SHUTDOWN_VB
		}
	}
}

// This function gets the list of vb and seqno to repair stream.
// Termination condition for stream repair:
// 1) All vb are in StreamBegin state
// 2) All vb have ref count == 1
// 3) There is no error in stream repair
func (ss *StreamState) getRepairTsForBucket(streamId common.StreamId,
	bucket string) (*common.TsVbuuid, bool, []Vbucket, []Vbucket) {

	anythingToRepair := false

	numVbuckets := ss.config["numVbuckets"].Int()
	repairTs := common.NewTsVbuuid(bucket, numVbuckets)
	var shutdownVbs []Vbucket = nil
	var repairVbs []Vbucket = nil
	var count = 0

	hwtTs := ss.streamBucketHWTMap[streamId][bucket]

	// First step: Find out if any StreamEnd needs to be escalated to ConnErr.
	// If so, add it to ShutdownVbs (for shutdown/restart).
	for i, s := range ss.streamBucketVbStatusMap[streamId][bucket] {
		if s == VBS_STREAM_END &&
			ss.canShutdownOnRetry(streamId, bucket, Vbucket(i)) {

			logging.Infof("StreamState::getRepairTsForBucket\n\t"+
				"Bucket %v StreamId %v Vbucket %v exceeds repair wait time. Convert to CONN_ERROR.",
				bucket, streamId, i)

			ss.makeConnectionError(streamId, bucket, Vbucket(i))
		}
	}

	// Second step : Repair any StreamEnd, ConnError on any vb.
	for i, s := range ss.streamBucketVbStatusMap[streamId][bucket] {
		if s == VBS_STREAM_END || s == VBS_CONN_ERROR {

			count++
			anythingToRepair = true
			repairVbs = ss.addRepairTs(repairTs, hwtTs, Vbucket(i), repairVbs)

			if s == VBS_CONN_ERROR &&
				ss.streamBucketRepairStateMap[streamId][bucket][i] < REPAIR_SHUTDOWN_VB {
				logging.Infof("StreamState::getRepairTsForBucket\n\t"+
					"Bucket %v StreamId %v Vbucket %v. Shutdown/Restart Vb.",
					bucket, streamId, i)

				shutdownVbs = append(shutdownVbs, Vbucket(i))
			}
		}
	}

	// Third step: If there is nothing to repair, then double check if every vb has
	// exactly one vb owner.  If not, then the accounting is wrong (most likely due
	// to connection error).  Make the vb as ConnErr and continue to repair.
	// Note: Do not check for VBS_INIT.  RepairMissingStreamBegin will ensure that
	// indexer is getting all StreamBegin.
	if !anythingToRepair && !ss.needsRollback(streamId, bucket) {
		for i, s := range ss.streamBucketVbStatusMap[streamId][bucket] {
			refCount := ss.streamBucketVbRefCountMap[streamId][bucket][i]
			if refCount != 1 && s != VBS_INIT {
				logging.Infof("StreamState::getRepairTsForBucket\n\t"+
					"Bucket %v StreamId %v Vbucket %v have ref count (%v != 1). Convert to CONN_ERROR.",
					bucket, streamId, i, refCount)
				// Make it a ConnErr such that subsequent retry will
				// force a shutdown/restart sequence.
				ss.makeConnectionError(streamId, bucket, Vbucket(i))
				repairVbs = ss.addRepairTs(repairTs, hwtTs, Vbucket(i), repairVbs)
				count++
				shutdownVbs = append(shutdownVbs, Vbucket(i))
				anythingToRepair = true
			}
		}
	}

	if !anythingToRepair {
		ss.streamBucketRestartVbTsMap[streamId][bucket] = nil
	} else {
		ss.streamBucketRestartVbTsMap[streamId][bucket] = repairTs.Copy()
	}

	ss.adjustNonSnapAlignedVbs(repairTs, streamId, bucket, repairVbs, true)
	ss.adjustVbuuids(repairTs, streamId, bucket)

	logging.Verbosef("StreamState::getRepairTsForBucket\n\t"+
		"Bucket %v StreamId %v repairTS %v",
		bucket, streamId, repairTs)

	return repairTs, anythingToRepair, shutdownVbs, repairVbs
}

func (ss *StreamState) canShutdownOnRetry(streamId common.StreamId, bucket string, vbno Vbucket) bool {

	if status := ss.streamBucketVbStatusMap[streamId][bucket][int(vbno)]; status != VBS_STREAM_END {
		return false
	}

	if ss.streamBucketRepairStateMap[streamId][bucket][int(vbno)] != REPAIR_RESTART_VB {
		return false
	}

	waitTime := int64(ss.config["timekeeper.escalate.StreamBeginWaitTime"].Int()) * int64(time.Second)

	if _, pendingVbuuid := ss.getKVPendingTs(streamId, bucket, vbno); pendingVbuuid != 0 {
		// acknowledged by projector but not yet active
		waitTime = waitTime * 2
	}

	if waitTime > 0 {
		exceedBeginTime := time.Now().UnixNano()-int64(ss.streamBucketLastBeginTime[streamId][bucket]) > waitTime
		exceedRepairTime := time.Now().UnixNano()-int64(ss.streamBucketLastRepairTimeMap[streamId][bucket][vbno]) > waitTime

		return exceedBeginTime && exceedRepairTime
	}

	return false
}

func (ss *StreamState) startMTROnRetry(streamId common.StreamId, bucket string) bool {

	startMTR := false

	for vbno, status := range ss.streamBucketVbStatusMap[streamId][bucket] {

		if status == VBS_STREAM_END {
			return false
		}

		if status != VBS_CONN_ERROR {
			continue
		}

		if ss.streamBucketRepairStateMap[streamId][bucket][vbno] != REPAIR_SHUTDOWN_VB {
			continue
		}

		waitTime := int64(ss.config["timekeeper.escalate.StreamBeginWaitTime"].Int()) * int64(time.Second)

		if _, pendingVbuuid := ss.getKVPendingTs(streamId, bucket, Vbucket(vbno)); pendingVbuuid != 0 {
			// acknowledged by projector but not yet active
			waitTime = waitTime * 2
		}

		if waitTime > 0 {
			exceedBeginTime := time.Now().UnixNano()-int64(ss.streamBucketLastBeginTime[streamId][bucket]) > waitTime
			exceedRepairTime := time.Now().UnixNano()-int64(ss.streamBucketLastRepairTimeMap[streamId][bucket][vbno]) > waitTime

			if exceedBeginTime && exceedRepairTime {
				startMTR = true
			}
		}
	}

	return startMTR
}

func (ss *StreamState) startRecoveryOnRetry(streamId common.StreamId, bucket string) bool {

	//FIXME: Currently, timekeeper can hang on initial index build if
	// 1) it is the first index
	// 2) there is recovery (no rollback) got triggered during initial build
	// There is race condition:
	// 1) During recovery, timekeeper execute checkInitialBuildDone.
	// 2) If build is now done due to recovery, timekeeper can delete the build info.
	// 3) Timekeeper now call indexer to checkInitialBuildDone
	// 4) Indexer will skip
	// 5) Later on when recoveryDone, timekeeper will call checkInitialBuildDone again.
	// 6) But since build info is deleted, it will not trigger another buildDone action.
	// 7) Indexer, therfore, cannot move the index to ACTIVE state.
	return false

	startRecovery := false

	for vbno, status := range ss.streamBucketVbStatusMap[streamId][bucket] {

		if status == VBS_STREAM_END {
			return false
		}

		if status != VBS_CONN_ERROR {
			continue
		}

		if ss.streamBucketRepairStateMap[streamId][bucket][vbno] != REPAIR_MTR {
			continue
		}

		waitTime := int64(ss.config["timekeeper.escalate.StreamBeginWaitTime"].Int()) * int64(time.Second)

		if _, pendingVbuuid := ss.getKVPendingTs(streamId, bucket, Vbucket(vbno)); pendingVbuuid != 0 {
			// acknowledged by projector but not yet active
			waitTime = waitTime * 2
		}

		if waitTime > 0 {
			exceedBeginTime := time.Now().UnixNano()-int64(ss.streamBucketLastBeginTime[streamId][bucket]) > waitTime
			exceedRepairTime := time.Now().UnixNano()-int64(ss.streamBucketLastRepairTimeMap[streamId][bucket][vbno]) > waitTime

			if exceedBeginTime && exceedRepairTime {
				startRecovery = true
			}
		}
	}

	return startRecovery
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

	ss.streamBucketVbStatusMap[streamId][bucket][vbno] = VBS_CONN_ERROR
	ss.clearVbRefCount(streamId, bucket, vbno)

	// clear rollbackTs and failTs for this vbucket
	ss.clearKVActiveTs(streamId, bucket, vbno)
	ss.clearKVPendingTs(streamId, bucket, vbno)
	ss.clearKVRollbackTs(streamId, bucket, vbno)

	ss.streamBucketRepairStateMap[streamId][bucket][vbno] = REPAIR_RESTART_VB
	ss.setLastRepairTime(streamId, bucket, vbno)

	logging.Infof("StreamState::connection error - set repair state to RESTART_VB for %v bucket %v vb %v", streamId, bucket, vbno)
}

func (ss *StreamState) addRepairTs(repairTs *common.TsVbuuid, hwtTs *common.TsVbuuid, vbno Vbucket, repairSeqno []Vbucket) []Vbucket {

	repairTs.Seqnos[vbno] = hwtTs.Seqnos[vbno]
	repairTs.Vbuuids[vbno] = hwtTs.Vbuuids[vbno]
	repairTs.Snapshots[vbno][0] = hwtTs.Snapshots[vbno][0]
	repairTs.Snapshots[vbno][1] = hwtTs.Snapshots[vbno][1]

	return append(repairSeqno, vbno)
}

func (ss *StreamState) addKVRollbackTs(streamId common.StreamId, bucket string, vbno Vbucket, seqno uint64, vbuuid uint64) {

	rollbackTs := ss.streamBucketKVRollbackTsMap[streamId][bucket]
	if rollbackTs == nil {
		numVbuckets := ss.config["numVbuckets"].Int()
		rollbackTs = common.NewTsVbuuid(bucket, numVbuckets)
		ss.streamBucketKVRollbackTsMap[streamId][bucket] = rollbackTs
	}

	rollbackTs.Seqnos[vbno] = seqno
	rollbackTs.Vbuuids[vbno] = vbuuid
	rollbackTs.Snapshots[vbno][0] = 0
	rollbackTs.Snapshots[vbno][1] = 0
}

func (ss *StreamState) clearKVRollbackTs(streamId common.StreamId, bucket string, vbno Vbucket) {

	rollbackTs := ss.streamBucketKVRollbackTsMap[streamId][bucket]
	if rollbackTs != nil {
		rollbackTs.Seqnos[vbno] = 0
		rollbackTs.Vbuuids[vbno] = 0
		rollbackTs.Snapshots[vbno][0] = 0
		rollbackTs.Snapshots[vbno][1] = 0
	}
}

func (ss *StreamState) getKVRollbackTs(streamId common.StreamId, bucket string, vbno Vbucket) (uint64, uint64) {

	rollbackTs := ss.streamBucketKVRollbackTsMap[streamId][bucket]
	if rollbackTs == nil {
		return 0, 0
	}
	return rollbackTs.Seqnos[vbno], rollbackTs.Vbuuids[vbno]
}

func (ss *StreamState) needsRollback(streamId common.StreamId, bucket string) bool {

	rollbackTs := ss.streamBucketKVRollbackTsMap[streamId][bucket]
	for _, vbuuid := range rollbackTs.Vbuuids {
		if vbuuid != 0 {
			return true
		}
	}

	return false
}

func (ss *StreamState) needsRollbackToZero(streamId common.StreamId, bucket string) bool {

	rollbackTs := ss.streamBucketKVRollbackTsMap[streamId][bucket]
	for i, vbuuid := range rollbackTs.Vbuuids {
		if vbuuid != 0 && rollbackTs.Seqnos[i] == 0 {
			return true
		}
	}

	return false
}

func (ss *StreamState) numKVRollbackTs(streamId common.StreamId, bucket string) int {

	count := 0
	rollbackTs := ss.streamBucketKVRollbackTsMap[streamId][bucket]
	for _, vbuuid := range rollbackTs.Vbuuids {
		if vbuuid != 0 {
			count++
		}
	}

	return count
}

func (ss *StreamState) addKVActiveTs(streamId common.StreamId, bucket string, vbno Vbucket, seqno uint64, vbuuid uint64) {

	activeTs := ss.streamBucketKVActiveTsMap[streamId][bucket]
	if activeTs == nil {
		numVbuckets := ss.config["numVbuckets"].Int()
		activeTs = common.NewTsVbuuid(bucket, numVbuckets)
		ss.streamBucketKVActiveTsMap[streamId][bucket] = activeTs
	}

	activeTs.Seqnos[vbno] = seqno
	activeTs.Vbuuids[vbno] = vbuuid
	activeTs.Snapshots[vbno][0] = 0
	activeTs.Snapshots[vbno][1] = 0
}

func (ss *StreamState) clearKVActiveTs(streamId common.StreamId, bucket string, vbno Vbucket) {

	activeTs := ss.streamBucketKVActiveTsMap[streamId][bucket]
	if activeTs != nil {
		activeTs.Seqnos[vbno] = 0
		activeTs.Vbuuids[vbno] = 0
		activeTs.Snapshots[vbno][0] = 0
		activeTs.Snapshots[vbno][1] = 0
	}
}

func (ss *StreamState) getKVActiveTs(streamId common.StreamId, bucket string, vbno Vbucket) (uint64, uint64) {

	activeTs := ss.streamBucketKVActiveTsMap[streamId][bucket]
	if activeTs == nil {
		return 0, 0
	}

	return activeTs.Seqnos[vbno], activeTs.Vbuuids[vbno]
}

func (ss *StreamState) addKVPendingTs(streamId common.StreamId, bucket string, vbno Vbucket, seqno uint64, vbuuid uint64) {

	pendingTs := ss.streamBucketKVPendingTsMap[streamId][bucket]
	if pendingTs == nil {
		numVbuckets := ss.config["numVbuckets"].Int()
		pendingTs = common.NewTsVbuuid(bucket, numVbuckets)
		ss.streamBucketKVPendingTsMap[streamId][bucket] = pendingTs
	}

	pendingTs.Seqnos[vbno] = seqno
	pendingTs.Vbuuids[vbno] = vbuuid
	pendingTs.Snapshots[vbno][0] = 0
	pendingTs.Snapshots[vbno][1] = 0
}

func (ss *StreamState) clearKVPendingTs(streamId common.StreamId, bucket string, vbno Vbucket) {

	pendingTs := ss.streamBucketKVPendingTsMap[streamId][bucket]
	if pendingTs != nil {
		pendingTs.Seqnos[vbno] = 0
		pendingTs.Vbuuids[vbno] = 0
		pendingTs.Snapshots[vbno][0] = 0
		pendingTs.Snapshots[vbno][1] = 0
	}
}

func (ss *StreamState) getKVPendingTs(streamId common.StreamId, bucket string, vbno Vbucket) (uint64, uint64) {

	pendingTs := ss.streamBucketKVPendingTsMap[streamId][bucket]
	if pendingTs == nil {
		return 0, 0
	}

	return pendingTs.Seqnos[vbno], pendingTs.Vbuuids[vbno]
}

func (ss *StreamState) setLastRepairTime(streamId common.StreamId, bucket string, vbno Vbucket) {

	repairTimeMap := ss.streamBucketLastRepairTimeMap[streamId][bucket]
	if repairTimeMap == nil {
		numVbuckets := ss.config["numVbuckets"].Int()
		repairTimeMap = NewTimestamp(numVbuckets)
		ss.streamBucketLastRepairTimeMap[streamId][bucket] = repairTimeMap
	}
	repairTimeMap[vbno] = Seqno(time.Now().UnixNano())
}

func (ss *StreamState) getLastRepairTime(streamId common.StreamId, bucket string, vbno Vbucket) int64 {

	repairTimeMap := ss.streamBucketLastRepairTimeMap[streamId][bucket]
	if repairTimeMap == nil {
		return 0
	}
	return int64(repairTimeMap[vbno])
}

func (ss *StreamState) clearLastRepairTime(streamId common.StreamId, bucket string, vbno Vbucket) {

	repairTimeMap := ss.streamBucketLastRepairTimeMap[streamId][bucket]
	if repairTimeMap != nil {
		repairTimeMap[vbno] = Seqno(0)
	}
}

func (ss *StreamState) resetAllLastRepairTime(streamId common.StreamId, bucket string) {

	now := Seqno(time.Now().UnixNano())
	for i, t := range ss.streamBucketLastRepairTimeMap[streamId][bucket] {
		if t != 0 {
			ss.streamBucketLastRepairTimeMap[streamId][bucket][i] = now
		}
	}
}

func (ss *StreamState) clearRepairState(streamId common.StreamId, bucket string, vbno Vbucket) {

	state := ss.streamBucketRepairStateMap[streamId][bucket]
	if state != nil {
		ss.streamBucketRepairStateMap[streamId][bucket][vbno] = REPAIR_NONE
	}
}

func (ss *StreamState) clearAllRepairState(streamId common.StreamId, bucket string) {

	logging.Infof("StreamState::clear all repair state for %v bucket %v", streamId, bucket)

	numVbuckets := ss.config["numVbuckets"].Int()
	ss.streamBucketRepairStateMap[streamId][bucket] = make([]RepairState, numVbuckets)
	ss.streamBucketLastBeginTime[streamId][bucket] = 0
	ss.streamBucketLastRepairTimeMap[streamId][bucket] = NewTimestamp(numVbuckets)
	ss.streamBucketKVActiveTsMap[streamId][bucket] = common.NewTsVbuuid(bucket, numVbuckets)
	ss.streamBucketKVPendingTsMap[streamId][bucket] = common.NewTsVbuuid(bucket, numVbuckets)
	ss.streamBucketKVRollbackTsMap[streamId][bucket] = common.NewTsVbuuid(bucket, numVbuckets)
}

//
// Return true if all vb has received streamBegin regardless of rollbackTs.
// If indexer has seen StreamBegin for a vb, but the vb has later received
// StreamEnd or ConnErr, this function will still return false.
//
func (ss *StreamState) seenAllVbs(streamId common.StreamId, bucket string) bool {

	numVbuckets := ss.config["numVbuckets"].Int()
	vbs := ss.streamBucketVbStatusMap[streamId][bucket]

	for i := 0; i < numVbuckets; i++ {
		status := vbs[i]
		if status != VBS_STREAM_BEGIN {
			return false
		}
	}

	return true
}

func (ss *StreamState) canRollbackNow(streamId common.StreamId, bucket string) (bool, bool) {

	canRollback := false
	needsRollback := false

	if ss.needsRollback(streamId, bucket) {
		needsRollback = true

		numRollback := ss.numKVRollbackTs(streamId, bucket)
		numVbuckets := ss.config["numVbuckets"].Int()

		waitTime := int64(ss.config["timekeeper.rollback.StreamBeginWaitTime"].Int()) * int64(time.Second)
		exceedWaitTime := time.Now().UnixNano()-int64(ss.streamBucketLastBeginTime[streamId][bucket]) > waitTime

		//if any vb needs a rollback to zero, there is no need to wait
		rollbackToZero := ss.needsRollbackToZero(streamId, bucket)

		canRollback = !ss.streamBucketAsyncMap[streamId][bucket] ||
			exceedWaitTime ||
			numRollback == numVbuckets ||
			rollbackToZero
	}

	if !needsRollback {
		needsRollback = ss.startRecoveryOnRetry(streamId, bucket)
		canRollback = needsRollback
	}

	return needsRollback, canRollback
}

//If a snapshot marker has been received but no mutation for that snapshot,
//the repairTs seqno will be outside the snapshot marker range and
//DCP will refuse to accept such seqno for restart. Such VBs need to
//use lastFlushTs or restartTs.
//
func (ss *StreamState) adjustNonSnapAlignedVbs(repairTs *common.TsVbuuid,
	streamId common.StreamId, bucket string, repairVbs []Vbucket, forStreamRepair bool) {

	// The caller either provide a vector of vb to repair, or this function will
	// look for any vb in the repairTS that has a non-zero vbuuid.
	if repairVbs == nil {
		for _, vbno := range repairTs.GetVbnos() {
			repairVbs = append(repairVbs, Vbucket(vbno))
		}
	}

	logging.Infof("StreamState::adjustNonSnapAlignedVbs\n\t"+
		"Bucket %v StreamId %v Vbuckets %v.",
		bucket, streamId, repairVbs)

	for _, vbno := range repairVbs {

		if !(repairTs.Seqnos[vbno] >= repairTs.Snapshots[vbno][0] &&
			repairTs.Seqnos[vbno] <= repairTs.Snapshots[vbno][1]) ||
			repairTs.Vbuuids[vbno] == 0 {

			// First, use the last flush TS seqno if avaliable
			if fts, ok := ss.streamBucketLastFlushedTsMap[streamId][bucket]; ok && fts != nil {
				repairTs.Snapshots[vbno][0] = fts.Snapshots[vbno][0]
				repairTs.Snapshots[vbno][1] = fts.Snapshots[vbno][1]
				repairTs.Seqnos[vbno] = fts.Seqnos[vbno]
				repairTs.Vbuuids[vbno] = fts.Vbuuids[vbno]
			}

			// If last flush TS is still out-of-bound, use last Snap-aligned flushed TS if available
			if !(repairTs.Seqnos[vbno] >= repairTs.Snapshots[vbno][0] &&
				repairTs.Seqnos[vbno] <= repairTs.Snapshots[vbno][1]) ||
				repairTs.Vbuuids[vbno] == 0 {
				if fts, ok := ss.streamBucketLastSnapAlignFlushedTsMap[streamId][bucket]; ok && fts != nil {
					repairTs.Snapshots[vbno][0] = fts.Snapshots[vbno][0]
					repairTs.Snapshots[vbno][1] = fts.Snapshots[vbno][1]
					repairTs.Seqnos[vbno] = fts.Seqnos[vbno]
					repairTs.Vbuuids[vbno] = fts.Vbuuids[vbno]
				}
			}

			// If timestamp used for open stream is available, use it
			if !(repairTs.Seqnos[vbno] >= repairTs.Snapshots[vbno][0] &&
				repairTs.Seqnos[vbno] <= repairTs.Snapshots[vbno][1]) ||
				repairTs.Vbuuids[vbno] == 0 {
				if rts, ok := ss.streamBucketOpenTsMap[streamId][bucket]; ok && rts != nil {
					repairTs.Snapshots[vbno][0] = rts.Snapshots[vbno][0]
					repairTs.Snapshots[vbno][1] = rts.Snapshots[vbno][1]
					repairTs.Seqnos[vbno] = rts.Seqnos[vbno]
					repairTs.Vbuuids[vbno] = rts.Vbuuids[vbno]
				}
			}

			// If open stream TS is not avail, then use restartTS
			if !(repairTs.Seqnos[vbno] >= repairTs.Snapshots[vbno][0] &&
				repairTs.Seqnos[vbno] <= repairTs.Snapshots[vbno][1]) ||
				repairTs.Vbuuids[vbno] == 0 {
				if rts, ok := ss.streamBucketRestartTsMap[streamId][bucket]; ok && rts != nil {
					//if no flush has been done yet, use restart TS
					repairTs.Snapshots[vbno][0] = rts.Snapshots[vbno][0]
					repairTs.Snapshots[vbno][1] = rts.Snapshots[vbno][1]
					repairTs.Seqnos[vbno] = rts.Seqnos[vbno]
					repairTs.Vbuuids[vbno] = rts.Vbuuids[vbno]
				}
			}

			//if seqno is still not with snapshot range or invalid vbuuid, then create an 0 timestamp
			if !(repairTs.Seqnos[vbno] >= repairTs.Snapshots[vbno][0] &&
				repairTs.Seqnos[vbno] <= repairTs.Snapshots[vbno][1]) ||
				repairTs.Vbuuids[vbno] == 0 {

				repairTs.Snapshots[vbno][0] = 0
				repairTs.Snapshots[vbno][1] = 0
				repairTs.Seqnos[vbno] = 0
				repairTs.Vbuuids[vbno] = 0
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
	bucket string, hwt *common.TsVbuuid, prevSnap *common.TsVbuuid) {

	ts := ss.streamBucketHWTMap[streamId][bucket]
	partialSnap := false

	for i, seq := range hwt.Seqnos {
		//if seqno has incremented, update it
		if seq > ts.Seqnos[i] {
			ts.Seqnos[i] = seq
			ss.streamBucketNewTsReqdMap[streamId][bucket] = true
		}
		//if snapEnd is greater than current hwt snapEnd
		if hwt.Snapshots[i][1] > ts.Snapshots[i][1] {
			lastSnap := ss.streamBucketLastSnapMarker[streamId][bucket]
			//store the prev snap marker in the lastSnapMarker map
			lastSnap.Snapshots[i][0] = prevSnap.Snapshots[i][0]
			lastSnap.Snapshots[i][1] = prevSnap.Snapshots[i][1]
			lastSnap.Vbuuids[i] = prevSnap.Vbuuids[i]
			lastSnap.Seqnos[i] = prevSnap.Seqnos[i]

			//store the new snap marker in hwt
			ts.Snapshots[i][0] = hwt.Snapshots[i][0]
			ts.Snapshots[i][1] = hwt.Snapshots[i][1]
			//	ts.Vbuuids[i] = hwt.Vbuuids[i]
			ss.streamBucketNewTsReqdMap[streamId][bucket] = true
			if prevSnap.Seqnos[i] != prevSnap.Snapshots[i][1] {
				logging.Warnf("StreamState::updateHWT Received Partial Last Snapshot in HWT "+
					"Bucket %v StreamId %v vbucket %v Snapshot %v-%v Seqno %v Vbuuid %v lastSnap %v-%v lastSnapSeqno %v",
					bucket, streamId, i, hwt.Snapshots[i][0], hwt.Snapshots[i][1], hwt.Seqnos[i], ts.Vbuuids[i],
					prevSnap.Snapshots[i][0], prevSnap.Snapshots[i][1], prevSnap.Seqnos[i])
				partialSnap = true

			}

		} else if hwt.Snapshots[i][1] < ts.Snapshots[i][1] {
			// Catch any out of order Snapshot.   StreamReader should make sure that Snapshot is monotonic increasing
			logging.Debugf("StreamState::updateHWT.  Recieved a snapshot marker older than current hwt snapshot. "+
				"Bucket %v StreamId %v vbucket %v Current Snapshot %v-%v New Snapshot %v-%v",
				bucket, streamId, i, ts.Snapshots[i][0], ts.Snapshots[i][1], hwt.Snapshots[i][0], hwt.Snapshots[i][1])
		}
	}

	if hwt.DisableAlign {
		logging.Warnf("StreamState::updateHWT Received Partial Snapshot Message in HWT "+
			"StreamId %v Bucket %v", streamId, bucket)
	}

	if partialSnap || hwt.DisableAlign {
		ss.disableSnapAlignForPendingTs(streamId, bucket)
	}

	logging.LazyTrace(func() string {
		return fmt.Sprintf("StreamState::updateHWT HWT Updated : %v", ts)
	})
}

func (ss *StreamState) checkNewTSDue(streamId common.StreamId, bucket string) bool {
	newTsReqd := ss.streamBucketNewTsReqdMap[streamId][bucket]
	return newTsReqd
}

func (ss *StreamState) checkCommitOverdue(streamId common.StreamId, bucket string) bool {

	snapPersistInterval := ss.getPersistInterval()
	persistDuration := time.Duration(snapPersistInterval) * time.Millisecond

	lastPersistTime := ss.streamBucketLastPersistTime[streamId][bucket]

	if time.Since(lastPersistTime) > persistDuration {

		bucketFlushInProgressTsMap := ss.streamBucketFlushInProgressTsMap[streamId]
		bucketTsListMap := ss.streamBucketTsListMap[streamId]
		bucketFlushEnabledMap := ss.streamBucketFlushEnabledMap[streamId]
		bucketNeedsCommit := ss.streamBucketNeedsCommitMap[streamId]

		//if there is no flush already in progress for this bucket
		//no pending TS in list and flush is not disabled
		tsList := bucketTsListMap[bucket]
		if bucketFlushInProgressTsMap[bucket] == nil &&
			bucketFlushEnabledMap[bucket] == true &&
			tsList.Len() == 0 &&
			bucketNeedsCommit[bucket] == true {
			return true
		}
	}

	return false
}

//gets the stability timestamp based on the current HWT
func (ss *StreamState) getNextStabilityTS(streamId common.StreamId,
	bucket string) *common.TsVbuuid {

	//generate new stability timestamp
	tsVbuuid := ss.streamBucketHWTMap[streamId][bucket].Copy()

	//Explicitly set the snapAligned flag to false as the initial state.
	//HWT can be set from an earlier restartTs which is snap aligned
	//and the flag needs to be reset for new timestamp.
	tsVbuuid.SetSnapAligned(false)

	tsVbuuid.SetSnapType(common.NO_SNAP)

	ss.alignSnapBoundary(streamId, bucket, tsVbuuid)

	//reset state for next TS
	ss.streamBucketNewTsReqdMap[streamId][bucket] = false

	if tsVbuuid.CheckSnapAligned() {
		tsVbuuid.SetSnapAligned(true)
	} else {
		tsVbuuid.SetSnapAligned(false)
	}

	return tsVbuuid
}

//align the snap boundary of TS if the seqno of the TS falls within the range of
//last snap marker
func (ss *StreamState) alignSnapBoundary(streamId common.StreamId,
	bucket string, ts *common.TsVbuuid) {

	smallSnap := ss.config["settings.smallSnapshotThreshold"].Uint64()
	lastSnap := ss.streamBucketLastSnapMarker[streamId][bucket]

	for i, s := range ts.Snapshots {
		//if seqno is not between snap boundary
		if !(ts.Seqnos[i] >= s[0] && ts.Seqnos[i] <= s[1]) {

			//if seqno matches with the SnapEnd of the lastSnapMarker
			//use that to align the TS
			if ts.Seqnos[i] == lastSnap.Snapshots[i][1] {
				ts.Snapshots[i][0] = lastSnap.Snapshots[i][0]
				ts.Snapshots[i][1] = lastSnap.Snapshots[i][1]
				ts.Vbuuids[i] = lastSnap.Vbuuids[i]
			} else {
				logging.Warnf("StreamState::alignSnapBoundary Received New Snapshot While Processing Incomplete "+
					"Snapshot. Bucket %v StreamId %v vbucket %v Snapshot %v-%v Seqno %v Vbuuid %v lastSnap %v-%v",
					bucket, streamId, i, ts.Snapshots[i][0], ts.Snapshots[i][1], ts.Seqnos[i], ts.Vbuuids[i],
					lastSnap.Snapshots[i][0], lastSnap.Snapshots[i][1])
			}

		} else if ts.Seqnos[i] != s[1] && (s[1]-s[0]) <= smallSnap {
			//for small snapshots, if all the mutations have not been received for the
			//snapshot, use the last snapshot marker to make it snap aligned.
			//this snapshot will get picked up in the next TS.

			//Use lastSnap only if it is from the same branch(vbuuid matches) and
			//HWT is already past the snapEnd of lastSnap and its not partial
			if ts.Seqnos[i] > lastSnap.Snapshots[i][1] && ts.Vbuuids[i] == lastSnap.Vbuuids[i] && (lastSnap.Seqnos[i] == lastSnap.Snapshots[i][1]) {
				ts.Snapshots[i][0] = lastSnap.Snapshots[i][0]
				ts.Snapshots[i][1] = lastSnap.Snapshots[i][1]
				ts.Seqnos[i] = lastSnap.Snapshots[i][1]
				ts.Vbuuids[i] = lastSnap.Vbuuids[i]
			}
			if lastSnap.Seqnos[i] != lastSnap.Snapshots[i][1] {
				logging.Warnf("StreamState::alignSnapBoundary Received Partial Last Snapshot in HWT "+
					"Bucket %v StreamId %v vbucket %v Snapshot %v-%v Seqno %v Vbuuid %v lastSnap %v-%v lastSnapSeqno %v",
					bucket, streamId, i, ts.Snapshots[i][0], ts.Snapshots[i][1], ts.Seqnos[i], ts.Vbuuids[i],
					lastSnap.Snapshots[i][0], lastSnap.Snapshots[i][1], lastSnap.Seqnos[i])
			}

		}

		if !(ts.Seqnos[i] >= ts.Snapshots[i][0] && ts.Seqnos[i] <= ts.Snapshots[i][1]) {
			logging.Warnf("StreamState::alignSnapBoundary.  TS falls out of snapshot boundary. "+
				"Bucket %v StreamId %v vbucket %v Snapshot %v-%v Seqno %v Vbuuid %v lastSnap %v-%v",
				bucket, streamId, i, ts.Snapshots[i][0], ts.Snapshots[i][1], ts.Seqnos[i], ts.Vbuuids[i],
				lastSnap.Snapshots[i][0], lastSnap.Snapshots[i][1])
		}
	}
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
func (ss *StreamState) getPersistInterval() uint64 {

	if common.GetStorageMode() == common.FORESTDB {
		return ss.config["settings.persisted_snapshot.interval"].Uint64()
	} else {
		return ss.config["settings.persisted_snapshot.moi.interval"].Uint64()
	}

}

func (ss *StreamState) disableSnapAlignForPendingTs(streamId common.StreamId, bucket string) {

	tsList := ss.streamBucketTsListMap[streamId][bucket]
	disableCount := 0
	for e := tsList.Front(); e != nil; e = e.Next() {
		ts := e.Value.(*common.TsVbuuid)
		ts.SetDisableAlign(true)
		disableCount++
	}
	if disableCount > 0 {
		logging.Infof("StreamState::disableSnapAlignForPendingTs Stream %v Bucket %v Disabled Snap Align for %v TS", streamId, bucket, disableCount)

	}
}

func (ss *StreamState) updateLastMutationVbuuid(streamId common.StreamId,
	bucket string, fts *common.TsVbuuid) {

	pts := ss.streamBucketLastMutationVbuuid[streamId][bucket]
	if pts == nil {
		ss.streamBucketLastMutationVbuuid[streamId][bucket] = fts.Copy()
		return
	}

	//LastMutationVbuuid has the vbuuid/seqno for the last valid mutation.
	//if there is a vbuuid change without a change is seqno,
	//it needs to be ignored
	for i, _ := range pts.Vbuuids {
		if fts.Seqnos[i] != pts.Seqnos[i] {
			pts.Vbuuids[i] = fts.Vbuuids[i]
			pts.Seqnos[i] = fts.Seqnos[i]
		}

	}
}

func (ss *StreamState) adjustVbuuids(restartTs *common.TsVbuuid,
	streamId common.StreamId, bucket string) {

	if restartTs == nil {
		return
	}

	//use the vbuuids with last known mutation
	if pts, ok := ss.streamBucketLastMutationVbuuid[streamId][bucket]; ok && pts != nil {
		for i, _ := range pts.Vbuuids {
			if restartTs.Seqnos[i] == pts.Seqnos[i] &&
				restartTs.Seqnos[i] != 0 &&
				restartTs.Vbuuids[i] != pts.Vbuuids[i] {
				logging.Infof("StreamState::adjustVbuuids %v %v Vb %v Seqno %v "+
					"From %v To %v", streamId, bucket, i, pts.Seqnos[i], restartTs.Vbuuids[i],
					pts.Vbuuids[i])
				restartTs.Vbuuids[i] = pts.Vbuuids[i]
			}
		}
	}
	return
}

//if there is a different vbuuid for the same seqno, use that to
//retry dcp stream request in case of rollback. this scenario could
//happen if a node fails over and we have more recent vbuuid than
//kv replica
func (ss *StreamState) computeRetryTs(streamId common.StreamId,
	bucket string, restartTs *common.TsVbuuid) *common.TsVbuuid {

	var retryTs *common.TsVbuuid

	if pts, ok := ss.streamBucketLastMutationVbuuid[streamId][bucket]; ok && pts != nil {

		retryTs = restartTs.Copy()
		valid := false

		for i, _ := range pts.Vbuuids {
			if retryTs.Seqnos[i] == pts.Seqnos[i] &&
				retryTs.Vbuuids[i] != pts.Vbuuids[i] {
				retryTs.Vbuuids[i] = pts.Vbuuids[i]
				valid = true
			}
		}
		if !valid {
			retryTs = nil
		}
	}

	return retryTs
}

func (ss *StreamState) getSessionId(streamId common.StreamId, bucket string) uint64 {
	return ss.streamBucketSessionId[streamId][bucket]
}
