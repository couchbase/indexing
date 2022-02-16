// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

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

	streamStatus                  map[common.StreamId]StreamStatus
	streamKeyspaceIdStatus        map[common.StreamId]KeyspaceIdStatus
	streamKeyspaceIdVbStatusMap   map[common.StreamId]KeyspaceIdVbStatusMap
	streamKeyspaceIdVbRefCountMap map[common.StreamId]KeyspaceIdVbRefCountMap

	streamKeyspaceIdHWTMap            map[common.StreamId]KeyspaceIdHWTMap
	streamKeyspaceIdNeedsCommitMap    map[common.StreamId]KeyspaceIdNeedsCommitMap
	streamKeyspaceIdHasBuildCompTSMap map[common.StreamId]KeyspaceIdHasBuildCompTSMap
	streamKeyspaceIdNewTsReqdMap      map[common.StreamId]KeyspaceIdNewTsReqdMap
	streamKeyspaceIdTsListMap         map[common.StreamId]KeyspaceIdTsListMap
	streamKeyspaceIdLastFlushedTsMap  map[common.StreamId]KeyspaceIdLastFlushedTsMap
	streamKeyspaceIdRestartTsMap      map[common.StreamId]KeyspaceIdRestartTsMap
	streamKeyspaceIdOpenTsMap         map[common.StreamId]KeyspaceIdOpenTsMap
	streamKeyspaceIdStartTimeMap      map[common.StreamId]KeyspaceIdStartTimeMap
	streamKeyspaceIdLastSnapMarker    map[common.StreamId]KeyspaceIdLastSnapMarker

	streamKeyspaceIdLastSnapAlignFlushedTsMap map[common.StreamId]KeyspaceIdLastFlushedTsMap
	streamKeyspaceIdLastMutationVbuuid        map[common.StreamId]KeyspaceIdLastMutationVbuuid
	streamKeyspaceIdNeedsLastRollbackReset    map[common.StreamId]KeyspaceIdNeedsLastRollbackReset

	streamKeyspaceIdRestartVbTsMap map[common.StreamId]KeyspaceIdRestartVbTsMap

	streamKeyspaceIdFlushInProgressTsMap map[common.StreamId]KeyspaceIdFlushInProgressTsMap
	streamKeyspaceIdAbortInProgressMap   map[common.StreamId]KeyspaceIdAbortInProgressMap
	streamKeyspaceIdFlushEnabledMap      map[common.StreamId]KeyspaceIdFlushEnabledMap
	streamKeyspaceIdDrainEnabledMap      map[common.StreamId]KeyspaceIdDrainEnabledMap
	streamKeyspaceIdFlushDone            map[common.StreamId]KeyspaceIdFlushDone
	streamKeyspaceIdForceRecovery        map[common.StreamId]KeyspaceIdForceRecovery

	streamKeyspaceIdIndexCountMap   map[common.StreamId]KeyspaceIdIndexCountMap
	streamKeyspaceIdRepairStopCh    map[common.StreamId]KeyspaceIdRepairStopCh
	streamKeyspaceIdTimerStopCh     map[common.StreamId]KeyspaceIdTimerStopCh
	streamKeyspaceIdLastPersistTime map[common.StreamId]KeyspaceIdLastPersistTime
	streamKeyspaceIdSkippedInMemTs  map[common.StreamId]KeyspaceIdSkippedInMemTs
	streamKeyspaceIdHasInMemSnap    map[common.StreamId]KeyspaceIdHasInMemSnap
	streamKeyspaceIdSessionId       map[common.StreamId]KeyspaceIdSessionId
	streamKeyspaceIdCollectionId    map[common.StreamId]KeyspaceIdCollectionId
	streamKeyspaceIdPastMinMergeTs  map[common.StreamId]KeyspaceIdPastMinMergeTs

	streamKeyspaceIdAsyncMap     map[common.StreamId]KeyspaceIdStreamAsyncMap
	streamKeyspaceIdPendingMerge map[common.StreamId]KeyspaceIdPendingMerge

	streamKeyspaceIdKVRollbackTsMap map[common.StreamId]KeyspaceIdKVRollbackTsMap
	streamKeyspaceIdKVActiveTsMap   map[common.StreamId]KeyspaceIdKVActiveTsMap
	streamKeyspaceIdKVPendingTsMap  map[common.StreamId]KeyspaceIdKVPendingTsMap

	// state for managing vb repair
	// 1) When a vb needs a repair, RepairTime is updated.
	// 2) A repair action is performed.  Once the action is completed, RepairState is updated.
	// 3) If RepairTime exceeds waitTime (action not effective), escalate to the next action by updating repairTime.
	streamKeyspaceIdLastBeginTime     map[common.StreamId]KeyspaceIdStreamLastBeginTime
	streamKeyspaceIdLastRepairTimeMap map[common.StreamId]KeyspaceIdStreamLastRepairTimeMap
	streamKeyspaceIdRepairStateMap    map[common.StreamId]KeyspaceIdStreamRepairStateMap

	// Maintains the mapping between vbucket to kv node UUID
	// for each keyspaceId, for each stream
	streamKeyspaceIdVBMap map[common.StreamId]KeyspaceIdVBMap

	keyspaceIdRollbackTime map[string]int64

	streamKeyspaceIdEnableOSO map[common.StreamId]KeyspaceIdEnableOSO

	streamKeyspaceIdHWTOSO map[common.StreamId]KeyspaceIdHWTOSO

	//only used to log debug information for pending builds(INIT_STREAM only)
	keyspaceIdPendBuildDebugLogTime map[string]uint64

	//only used to log debug information for when flushTs check for merge fails(INIT_STREAM only)
	keyspaceIdFlushCheckDebugLogTime map[string]uint64

	//last time of KV seqnum fetch for stream merge check
	streamKeyspaceIdLastKVSeqFetch map[common.StreamId]KeyspaceIdLastKVSeqFetch
}

type KeyspaceIdHWTMap map[string]*common.TsVbuuid
type KeyspaceIdLastFlushedTsMap map[string]*common.TsVbuuid
type KeyspaceIdRestartTsMap map[string]*common.TsVbuuid
type KeyspaceIdOpenTsMap map[string]*common.TsVbuuid
type KeyspaceIdStartTimeMap map[string]uint64
type KeyspaceIdNeedsCommitMap map[string]bool
type KeyspaceIdHasBuildCompTSMap map[string]bool
type KeyspaceIdNewTsReqdMap map[string]bool
type KeyspaceIdLastSnapMarker map[string]*common.TsVbuuid
type KeyspaceIdLastMutationVbuuid map[string]*common.TsVbuuid
type KeyspaceIdNeedsLastRollbackReset map[string]bool

type KeyspaceIdTsListMap map[string]*list.List
type KeyspaceIdFlushInProgressTsMap map[string]*common.TsVbuuid
type KeyspaceIdAbortInProgressMap map[string]bool
type KeyspaceIdFlushEnabledMap map[string]bool
type KeyspaceIdDrainEnabledMap map[string]bool
type KeyspaceIdFlushDone map[string]DoneChannel
type KeyspaceIdForceRecovery map[string]bool

type KeyspaceIdRestartVbTsMap map[string]*common.TsVbuuid

type KeyspaceIdVbStatusMap map[string]Timestamp
type KeyspaceIdVbRefCountMap map[string]Timestamp
type KeyspaceIdRepairStopCh map[string]StopChannel
type KeyspaceIdTimerStopCh map[string]StopChannel
type KeyspaceIdLastPersistTime map[string]time.Time
type KeyspaceIdSkippedInMemTs map[string]uint64
type KeyspaceIdHasInMemSnap map[string]bool
type KeyspaceIdSessionId map[string]uint64
type KeyspaceIdEnableOSO map[string]bool
type KeyspaceIdCollectionId map[string]string
type KeyspaceIdPastMinMergeTs map[string]bool

type KeyspaceIdStreamAsyncMap map[string]bool
type KeyspaceIdPendingMerge map[string]string //map[bucket][pending_merge_keyspaceId]
type KeyspaceIdStreamLastBeginTime map[string]uint64
type KeyspaceIdStreamLastRepairTimeMap map[string]Timestamp
type KeyspaceIdKVRollbackTsMap map[string]*common.TsVbuuid
type KeyspaceIdKVActiveTsMap map[string]*common.TsVbuuid
type KeyspaceIdKVPendingTsMap map[string]*common.TsVbuuid
type KeyspaceIdStreamRepairStateMap map[string][]RepairState

type KeyspaceIdStatus map[string]StreamStatus

type KeyspaceIdVBMap map[string]map[Vbucket]string

type KeyspaceIdHWTOSO map[string]*common.TsVbuuid
type KeyspaceIdLastKVSeqFetch map[string]time.Time

type TsListElem struct {
	ts       *common.TsVbuuid
	osoCount []uint64
}

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
		config:                                    config,
		streamKeyspaceIdHWTMap:                    make(map[common.StreamId]KeyspaceIdHWTMap),
		streamKeyspaceIdNeedsCommitMap:            make(map[common.StreamId]KeyspaceIdNeedsCommitMap),
		streamKeyspaceIdHasBuildCompTSMap:         make(map[common.StreamId]KeyspaceIdHasBuildCompTSMap),
		streamKeyspaceIdNewTsReqdMap:              make(map[common.StreamId]KeyspaceIdNewTsReqdMap),
		streamKeyspaceIdTsListMap:                 make(map[common.StreamId]KeyspaceIdTsListMap),
		streamKeyspaceIdFlushInProgressTsMap:      make(map[common.StreamId]KeyspaceIdFlushInProgressTsMap),
		streamKeyspaceIdAbortInProgressMap:        make(map[common.StreamId]KeyspaceIdAbortInProgressMap),
		streamKeyspaceIdLastFlushedTsMap:          make(map[common.StreamId]KeyspaceIdLastFlushedTsMap),
		streamKeyspaceIdLastSnapAlignFlushedTsMap: make(map[common.StreamId]KeyspaceIdLastFlushedTsMap),
		streamKeyspaceIdRestartTsMap:              make(map[common.StreamId]KeyspaceIdRestartTsMap),

		streamKeyspaceIdOpenTsMap:              make(map[common.StreamId]KeyspaceIdOpenTsMap),
		streamKeyspaceIdStartTimeMap:           make(map[common.StreamId]KeyspaceIdStartTimeMap),
		streamKeyspaceIdFlushEnabledMap:        make(map[common.StreamId]KeyspaceIdFlushEnabledMap),
		streamKeyspaceIdDrainEnabledMap:        make(map[common.StreamId]KeyspaceIdDrainEnabledMap),
		streamKeyspaceIdFlushDone:              make(map[common.StreamId]KeyspaceIdFlushDone),
		streamKeyspaceIdForceRecovery:          make(map[common.StreamId]KeyspaceIdForceRecovery),
		streamKeyspaceIdVbStatusMap:            make(map[common.StreamId]KeyspaceIdVbStatusMap),
		streamKeyspaceIdVbRefCountMap:          make(map[common.StreamId]KeyspaceIdVbRefCountMap),
		streamKeyspaceIdRestartVbTsMap:         make(map[common.StreamId]KeyspaceIdRestartVbTsMap),
		streamStatus:                           make(map[common.StreamId]StreamStatus),
		streamKeyspaceIdStatus:                 make(map[common.StreamId]KeyspaceIdStatus),
		streamKeyspaceIdIndexCountMap:          make(map[common.StreamId]KeyspaceIdIndexCountMap),
		streamKeyspaceIdRepairStopCh:           make(map[common.StreamId]KeyspaceIdRepairStopCh),
		streamKeyspaceIdTimerStopCh:            make(map[common.StreamId]KeyspaceIdTimerStopCh),
		streamKeyspaceIdLastPersistTime:        make(map[common.StreamId]KeyspaceIdLastPersistTime),
		streamKeyspaceIdSkippedInMemTs:         make(map[common.StreamId]KeyspaceIdSkippedInMemTs),
		streamKeyspaceIdHasInMemSnap:           make(map[common.StreamId]KeyspaceIdHasInMemSnap),
		streamKeyspaceIdLastSnapMarker:         make(map[common.StreamId]KeyspaceIdLastSnapMarker),
		streamKeyspaceIdLastMutationVbuuid:     make(map[common.StreamId]KeyspaceIdLastMutationVbuuid),
		streamKeyspaceIdNeedsLastRollbackReset: make(map[common.StreamId]KeyspaceIdNeedsLastRollbackReset),
		keyspaceIdRollbackTime:                 make(map[string]int64),
		streamKeyspaceIdAsyncMap:               make(map[common.StreamId]KeyspaceIdStreamAsyncMap),
		streamKeyspaceIdPendingMerge:           make(map[common.StreamId]KeyspaceIdPendingMerge),
		streamKeyspaceIdLastBeginTime:          make(map[common.StreamId]KeyspaceIdStreamLastBeginTime),
		streamKeyspaceIdLastRepairTimeMap:      make(map[common.StreamId]KeyspaceIdStreamLastRepairTimeMap),
		streamKeyspaceIdKVRollbackTsMap:        make(map[common.StreamId]KeyspaceIdKVRollbackTsMap),
		streamKeyspaceIdKVActiveTsMap:          make(map[common.StreamId]KeyspaceIdKVActiveTsMap),
		streamKeyspaceIdKVPendingTsMap:         make(map[common.StreamId]KeyspaceIdKVPendingTsMap),
		streamKeyspaceIdRepairStateMap:         make(map[common.StreamId]KeyspaceIdStreamRepairStateMap),
		streamKeyspaceIdSessionId:              make(map[common.StreamId]KeyspaceIdSessionId),
		streamKeyspaceIdCollectionId:           make(map[common.StreamId]KeyspaceIdCollectionId),
		streamKeyspaceIdPastMinMergeTs:         make(map[common.StreamId]KeyspaceIdPastMinMergeTs),
		streamKeyspaceIdVBMap:                  make(map[common.StreamId]KeyspaceIdVBMap),
		streamKeyspaceIdEnableOSO:              make(map[common.StreamId]KeyspaceIdEnableOSO),
		streamKeyspaceIdHWTOSO:                 make(map[common.StreamId]KeyspaceIdHWTOSO),
		keyspaceIdPendBuildDebugLogTime:        make(map[string]uint64),
		keyspaceIdFlushCheckDebugLogTime:       make(map[string]uint64),
		streamKeyspaceIdLastKVSeqFetch:         make(map[common.StreamId]KeyspaceIdLastKVSeqFetch),
	}

	return ss

}

func (ss *StreamState) initNewStream(streamId common.StreamId) {

	//init all internal maps for this stream
	keyspaceIdHWTMap := make(KeyspaceIdHWTMap)
	ss.streamKeyspaceIdHWTMap[streamId] = keyspaceIdHWTMap

	keyspaceIdNeedsCommitMap := make(KeyspaceIdNeedsCommitMap)
	ss.streamKeyspaceIdNeedsCommitMap[streamId] = keyspaceIdNeedsCommitMap

	keyspaceIdHasBuildCompTSMap := make(KeyspaceIdHasBuildCompTSMap)
	ss.streamKeyspaceIdHasBuildCompTSMap[streamId] = keyspaceIdHasBuildCompTSMap

	keyspaceIdNewTsReqdMap := make(KeyspaceIdNewTsReqdMap)
	ss.streamKeyspaceIdNewTsReqdMap[streamId] = keyspaceIdNewTsReqdMap

	keyspaceIdRestartTsMap := make(KeyspaceIdRestartTsMap)
	ss.streamKeyspaceIdRestartTsMap[streamId] = keyspaceIdRestartTsMap

	keyspaceIdOpenTsMap := make(KeyspaceIdOpenTsMap)
	ss.streamKeyspaceIdOpenTsMap[streamId] = keyspaceIdOpenTsMap

	keyspaceIdStartTimeMap := make(KeyspaceIdStartTimeMap)
	ss.streamKeyspaceIdStartTimeMap[streamId] = keyspaceIdStartTimeMap

	keyspaceIdTsListMap := make(KeyspaceIdTsListMap)
	ss.streamKeyspaceIdTsListMap[streamId] = keyspaceIdTsListMap

	keyspaceIdFlushInProgressTsMap := make(KeyspaceIdFlushInProgressTsMap)
	ss.streamKeyspaceIdFlushInProgressTsMap[streamId] = keyspaceIdFlushInProgressTsMap

	keyspaceIdAbortInProgressMap := make(KeyspaceIdAbortInProgressMap)
	ss.streamKeyspaceIdAbortInProgressMap[streamId] = keyspaceIdAbortInProgressMap

	keyspaceIdLastFlushedTsMap := make(KeyspaceIdLastFlushedTsMap)
	ss.streamKeyspaceIdLastFlushedTsMap[streamId] = keyspaceIdLastFlushedTsMap

	keyspaceIdLastSnapAlignFlushedTsMap := make(KeyspaceIdLastFlushedTsMap)
	ss.streamKeyspaceIdLastSnapAlignFlushedTsMap[streamId] = keyspaceIdLastSnapAlignFlushedTsMap

	keyspaceIdFlushEnabledMap := make(KeyspaceIdFlushEnabledMap)
	ss.streamKeyspaceIdFlushEnabledMap[streamId] = keyspaceIdFlushEnabledMap

	keyspaceIdDrainEnabledMap := make(KeyspaceIdDrainEnabledMap)
	ss.streamKeyspaceIdDrainEnabledMap[streamId] = keyspaceIdDrainEnabledMap

	keyspaceIdFlushDone := make(KeyspaceIdFlushDone)
	ss.streamKeyspaceIdFlushDone[streamId] = keyspaceIdFlushDone

	keyspaceIdForceRecovery := make(KeyspaceIdForceRecovery)
	ss.streamKeyspaceIdForceRecovery[streamId] = keyspaceIdForceRecovery

	keyspaceIdVbStatusMap := make(KeyspaceIdVbStatusMap)
	ss.streamKeyspaceIdVbStatusMap[streamId] = keyspaceIdVbStatusMap

	keyspaceIdVbRefCountMap := make(KeyspaceIdVbRefCountMap)
	ss.streamKeyspaceIdVbRefCountMap[streamId] = keyspaceIdVbRefCountMap

	keyspaceIdRestartVbTsMap := make(KeyspaceIdRestartVbTsMap)
	ss.streamKeyspaceIdRestartVbTsMap[streamId] = keyspaceIdRestartVbTsMap

	keyspaceIdIndexCountMap := make(KeyspaceIdIndexCountMap)
	ss.streamKeyspaceIdIndexCountMap[streamId] = keyspaceIdIndexCountMap

	keyspaceIdRepairStopChMap := make(KeyspaceIdRepairStopCh)
	ss.streamKeyspaceIdRepairStopCh[streamId] = keyspaceIdRepairStopChMap

	keyspaceIdTimerStopChMap := make(KeyspaceIdTimerStopCh)
	ss.streamKeyspaceIdTimerStopCh[streamId] = keyspaceIdTimerStopChMap

	keyspaceIdLastPersistTime := make(KeyspaceIdLastPersistTime)
	ss.streamKeyspaceIdLastPersistTime[streamId] = keyspaceIdLastPersistTime

	keyspaceIdSkippedInMemTs := make(KeyspaceIdSkippedInMemTs)
	ss.streamKeyspaceIdSkippedInMemTs[streamId] = keyspaceIdSkippedInMemTs

	keyspaceIdHasInMemSnap := make(KeyspaceIdHasInMemSnap)
	ss.streamKeyspaceIdHasInMemSnap[streamId] = keyspaceIdHasInMemSnap

	keyspaceIdSessionId := make(KeyspaceIdSessionId)
	ss.streamKeyspaceIdSessionId[streamId] = keyspaceIdSessionId

	keyspaceIdCollectionId := make(KeyspaceIdCollectionId)
	ss.streamKeyspaceIdCollectionId[streamId] = keyspaceIdCollectionId

	keyspaceIdPastMinMergeTs := make(KeyspaceIdPastMinMergeTs)
	ss.streamKeyspaceIdPastMinMergeTs[streamId] = keyspaceIdPastMinMergeTs

	keyspaceIdStatus := make(KeyspaceIdStatus)
	ss.streamKeyspaceIdStatus[streamId] = keyspaceIdStatus

	keyspaceIdLastSnapMarker := make(KeyspaceIdLastSnapMarker)
	ss.streamKeyspaceIdLastSnapMarker[streamId] = keyspaceIdLastSnapMarker

	keyspaceIdLastMutationVbuuid := make(KeyspaceIdLastMutationVbuuid)
	ss.streamKeyspaceIdLastMutationVbuuid[streamId] = keyspaceIdLastMutationVbuuid

	keyspaceIdNeedsLastRollbackReset := make(KeyspaceIdNeedsLastRollbackReset)
	ss.streamKeyspaceIdNeedsLastRollbackReset[streamId] = keyspaceIdNeedsLastRollbackReset

	keyspaceIdStreamAsyncMap := make(KeyspaceIdStreamAsyncMap)
	ss.streamKeyspaceIdAsyncMap[streamId] = keyspaceIdStreamAsyncMap

	keyspaceIdPendingMerge := make(KeyspaceIdPendingMerge)
	ss.streamKeyspaceIdPendingMerge[streamId] = keyspaceIdPendingMerge

	keyspaceIdStreamLastBeginTime := make(KeyspaceIdStreamLastBeginTime)
	ss.streamKeyspaceIdLastBeginTime[streamId] = keyspaceIdStreamLastBeginTime

	keyspaceIdStreamLastRepairTimeMap := make(KeyspaceIdStreamLastRepairTimeMap)
	ss.streamKeyspaceIdLastRepairTimeMap[streamId] = keyspaceIdStreamLastRepairTimeMap

	keyspaceIdKVRollbackTsMap := make(KeyspaceIdKVRollbackTsMap)
	ss.streamKeyspaceIdKVRollbackTsMap[streamId] = keyspaceIdKVRollbackTsMap

	keyspaceIdKVActiveTsMap := make(KeyspaceIdKVActiveTsMap)
	ss.streamKeyspaceIdKVActiveTsMap[streamId] = keyspaceIdKVActiveTsMap

	keyspaceIdKVPendingTsMap := make(KeyspaceIdKVPendingTsMap)
	ss.streamKeyspaceIdKVPendingTsMap[streamId] = keyspaceIdKVPendingTsMap

	keyspaceIdStreamRepairStateMap := make(KeyspaceIdStreamRepairStateMap)
	ss.streamKeyspaceIdRepairStateMap[streamId] = keyspaceIdStreamRepairStateMap

	keyspaceIdVBMap := make(KeyspaceIdVBMap)
	ss.streamKeyspaceIdVBMap[streamId] = keyspaceIdVBMap

	keyspaceIdEnableOSO := make(KeyspaceIdEnableOSO)
	ss.streamKeyspaceIdEnableOSO[streamId] = keyspaceIdEnableOSO

	keyspaceIdHWTOSO := make(KeyspaceIdHWTOSO)
	ss.streamKeyspaceIdHWTOSO[streamId] = keyspaceIdHWTOSO

	keyspaceIdLastKVSeqFetch := make(KeyspaceIdLastKVSeqFetch)
	ss.streamKeyspaceIdLastKVSeqFetch[streamId] = keyspaceIdLastKVSeqFetch

	ss.streamStatus[streamId] = STREAM_ACTIVE

}

func (ss *StreamState) initKeyspaceIdInStream(streamId common.StreamId,
	keyspaceId string) {

	numVbuckets := ss.config["numVbuckets"].Int()

	bucket := GetBucketFromKeyspaceId(keyspaceId)
	ss.streamKeyspaceIdHWTMap[streamId][keyspaceId] = common.NewTsVbuuid(bucket, numVbuckets)
	ss.streamKeyspaceIdNeedsCommitMap[streamId][keyspaceId] = false
	ss.streamKeyspaceIdHasBuildCompTSMap[streamId][keyspaceId] = false
	ss.streamKeyspaceIdNewTsReqdMap[streamId][keyspaceId] = false
	ss.streamKeyspaceIdFlushInProgressTsMap[streamId][keyspaceId] = nil
	ss.streamKeyspaceIdAbortInProgressMap[streamId][keyspaceId] = false
	ss.streamKeyspaceIdTsListMap[streamId][keyspaceId] = list.New()
	ss.streamKeyspaceIdLastFlushedTsMap[streamId][keyspaceId] = nil
	ss.streamKeyspaceIdLastSnapAlignFlushedTsMap[streamId][keyspaceId] = nil
	ss.streamKeyspaceIdFlushEnabledMap[streamId][keyspaceId] = true
	ss.streamKeyspaceIdDrainEnabledMap[streamId][keyspaceId] = true
	ss.streamKeyspaceIdFlushDone[streamId][keyspaceId] = nil
	ss.streamKeyspaceIdForceRecovery[streamId][keyspaceId] = false
	ss.streamKeyspaceIdVbStatusMap[streamId][keyspaceId] = NewTimestamp(numVbuckets)
	ss.streamKeyspaceIdVbRefCountMap[streamId][keyspaceId] = NewTimestamp(numVbuckets)
	ss.streamKeyspaceIdRestartVbTsMap[streamId][keyspaceId] = nil
	ss.streamKeyspaceIdIndexCountMap[streamId][keyspaceId] = 0
	ss.streamKeyspaceIdRepairStopCh[streamId][keyspaceId] = nil
	ss.streamKeyspaceIdTimerStopCh[streamId][keyspaceId] = make(StopChannel)
	ss.streamKeyspaceIdLastPersistTime[streamId][keyspaceId] = time.Now()
	ss.streamKeyspaceIdRestartTsMap[streamId][keyspaceId] = nil
	ss.streamKeyspaceIdOpenTsMap[streamId][keyspaceId] = nil
	ss.streamKeyspaceIdStartTimeMap[streamId][keyspaceId] = uint64(0)
	ss.streamKeyspaceIdSkippedInMemTs[streamId][keyspaceId] = 0
	ss.streamKeyspaceIdHasInMemSnap[streamId][keyspaceId] = false
	ss.streamKeyspaceIdSessionId[streamId][keyspaceId] = 0
	ss.streamKeyspaceIdCollectionId[streamId][keyspaceId] = ""
	ss.streamKeyspaceIdPastMinMergeTs[streamId][keyspaceId] = false
	ss.streamKeyspaceIdLastSnapMarker[streamId][keyspaceId] = common.NewTsVbuuid(bucket, numVbuckets)
	ss.streamKeyspaceIdLastMutationVbuuid[streamId][keyspaceId] = common.NewTsVbuuid(bucket, numVbuckets)
	ss.streamKeyspaceIdNeedsLastRollbackReset[streamId][keyspaceId] = true
	ss.streamKeyspaceIdAsyncMap[streamId][keyspaceId] = false
	ss.streamKeyspaceIdPendingMerge[streamId][keyspaceId] = ""
	ss.streamKeyspaceIdLastBeginTime[streamId][keyspaceId] = 0
	ss.streamKeyspaceIdLastRepairTimeMap[streamId][keyspaceId] = NewTimestamp(numVbuckets)
	ss.streamKeyspaceIdKVRollbackTsMap[streamId][keyspaceId] = common.NewTsVbuuid(bucket, numVbuckets)
	ss.streamKeyspaceIdKVActiveTsMap[streamId][keyspaceId] = common.NewTsVbuuid(bucket, numVbuckets)
	ss.streamKeyspaceIdKVPendingTsMap[streamId][keyspaceId] = common.NewTsVbuuid(bucket, numVbuckets)
	ss.streamKeyspaceIdRepairStateMap[streamId][keyspaceId] = make([]RepairState, numVbuckets)
	ss.streamKeyspaceIdVBMap[streamId][keyspaceId] = make(map[Vbucket]string)
	ss.streamKeyspaceIdEnableOSO[streamId][keyspaceId] = false
	ss.streamKeyspaceIdHWTOSO[streamId][keyspaceId] = common.NewTsVbuuid(bucket, numVbuckets)
	ss.streamKeyspaceIdLastKVSeqFetch[streamId][keyspaceId] = time.Time{}

	if streamId == common.INIT_STREAM {
		ss.keyspaceIdPendBuildDebugLogTime[keyspaceId] = uint64(time.Now().UnixNano())
		ss.keyspaceIdFlushCheckDebugLogTime[keyspaceId] = uint64(time.Now().UnixNano())
	}

	ss.streamKeyspaceIdStatus[streamId][keyspaceId] = STREAM_ACTIVE

	logging.Infof("StreamState::initKeyspaceIdInStream New KeyspaceId %v Added for "+
		"Stream %v", keyspaceId, streamId)
}

func (ss *StreamState) cleanupKeyspaceIdFromStream(streamId common.StreamId,
	keyspaceId string) {

	//if there is any ongoing repair for this keyspaceId, abort that
	if stopCh, ok := ss.streamKeyspaceIdRepairStopCh[streamId][keyspaceId]; ok && stopCh != nil {
		close(stopCh)
	}

	delete(ss.streamKeyspaceIdHWTMap[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdNeedsCommitMap[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdHasBuildCompTSMap[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdNewTsReqdMap[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdTsListMap[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdFlushInProgressTsMap[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdAbortInProgressMap[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdLastFlushedTsMap[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdLastSnapAlignFlushedTsMap[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdFlushEnabledMap[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdDrainEnabledMap[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdVbStatusMap[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdVbRefCountMap[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdRestartVbTsMap[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdIndexCountMap[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdRepairStopCh[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdTimerStopCh[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdLastPersistTime[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdRestartTsMap[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdOpenTsMap[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdStartTimeMap[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdLastSnapMarker[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdLastMutationVbuuid[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdNeedsLastRollbackReset[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdSkippedInMemTs[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdHasInMemSnap[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdSessionId[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdCollectionId[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdPastMinMergeTs[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdAsyncMap[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdPendingMerge[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdLastBeginTime[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdLastRepairTimeMap[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdKVRollbackTsMap[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdKVActiveTsMap[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdKVPendingTsMap[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdRepairStateMap[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdVBMap[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdEnableOSO[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdHWTOSO[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdLastKVSeqFetch[streamId], keyspaceId)

	if streamId == common.INIT_STREAM {
		delete(ss.keyspaceIdPendBuildDebugLogTime, keyspaceId)
		delete(ss.keyspaceIdFlushCheckDebugLogTime, keyspaceId)
	}

	if donech, ok := ss.streamKeyspaceIdFlushDone[streamId][keyspaceId]; ok && donech != nil {
		close(donech)
	}
	delete(ss.streamKeyspaceIdFlushDone[streamId], keyspaceId)
	delete(ss.streamKeyspaceIdForceRecovery[streamId], keyspaceId)

	if bs, ok := ss.streamKeyspaceIdStatus[streamId]; ok && bs != nil {
		bs[keyspaceId] = STREAM_INACTIVE
	}

	logging.Infof("StreamState::cleanupKeyspaceIdFromStream KeyspaceId %v Deleted from "+
		"Stream %v", keyspaceId, streamId)

}

func (ss *StreamState) resetStreamState(streamId common.StreamId) {

	//delete this stream from internal maps
	delete(ss.streamKeyspaceIdHWTMap, streamId)
	delete(ss.streamKeyspaceIdNeedsCommitMap, streamId)
	delete(ss.streamKeyspaceIdHasBuildCompTSMap, streamId)
	delete(ss.streamKeyspaceIdNewTsReqdMap, streamId)
	delete(ss.streamKeyspaceIdTsListMap, streamId)
	delete(ss.streamKeyspaceIdFlushInProgressTsMap, streamId)
	delete(ss.streamKeyspaceIdAbortInProgressMap, streamId)
	delete(ss.streamKeyspaceIdLastFlushedTsMap, streamId)
	delete(ss.streamKeyspaceIdLastSnapAlignFlushedTsMap, streamId)
	delete(ss.streamKeyspaceIdFlushEnabledMap, streamId)
	delete(ss.streamKeyspaceIdDrainEnabledMap, streamId)
	delete(ss.streamKeyspaceIdFlushDone, streamId)
	delete(ss.streamKeyspaceIdForceRecovery, streamId)
	delete(ss.streamKeyspaceIdVbStatusMap, streamId)
	delete(ss.streamKeyspaceIdVbRefCountMap, streamId)
	delete(ss.streamKeyspaceIdRestartVbTsMap, streamId)
	delete(ss.streamKeyspaceIdIndexCountMap, streamId)
	delete(ss.streamKeyspaceIdLastPersistTime, streamId)
	delete(ss.streamKeyspaceIdStatus, streamId)
	delete(ss.streamKeyspaceIdRestartTsMap, streamId)
	delete(ss.streamKeyspaceIdOpenTsMap, streamId)
	delete(ss.streamKeyspaceIdStartTimeMap, streamId)
	delete(ss.streamKeyspaceIdSkippedInMemTs, streamId)
	delete(ss.streamKeyspaceIdHasInMemSnap, streamId)
	delete(ss.streamKeyspaceIdSessionId, streamId)
	delete(ss.streamKeyspaceIdCollectionId, streamId)
	delete(ss.streamKeyspaceIdPastMinMergeTs, streamId)
	delete(ss.streamKeyspaceIdLastSnapMarker, streamId)
	delete(ss.streamKeyspaceIdLastMutationVbuuid, streamId)
	delete(ss.streamKeyspaceIdNeedsLastRollbackReset, streamId)
	delete(ss.streamKeyspaceIdAsyncMap, streamId)
	delete(ss.streamKeyspaceIdPendingMerge, streamId)
	delete(ss.streamKeyspaceIdLastBeginTime, streamId)
	delete(ss.streamKeyspaceIdLastRepairTimeMap, streamId)
	delete(ss.streamKeyspaceIdKVRollbackTsMap, streamId)
	delete(ss.streamKeyspaceIdKVActiveTsMap, streamId)
	delete(ss.streamKeyspaceIdKVPendingTsMap, streamId)
	delete(ss.streamKeyspaceIdRepairStateMap, streamId)
	delete(ss.streamKeyspaceIdVBMap, streamId)
	delete(ss.streamKeyspaceIdEnableOSO, streamId)
	delete(ss.streamKeyspaceIdHWTOSO, streamId)
	delete(ss.streamKeyspaceIdLastKVSeqFetch, streamId)

	ss.streamStatus[streamId] = STREAM_INACTIVE

	logging.Infof("StreamState::resetStreamState Reset Stream %v State", streamId)
}

func (ss *StreamState) getVbStatus(streamId common.StreamId, keyspaceId string, vb Vbucket) VbStatus {

	return VbStatus(ss.streamKeyspaceIdVbStatusMap[streamId][keyspaceId][vb])
}

func (ss *StreamState) updateVbStatus(streamId common.StreamId, keyspaceId string,
	vbList []Vbucket, status VbStatus) {

	for _, vb := range vbList {
		vbs := ss.streamKeyspaceIdVbStatusMap[streamId][keyspaceId]
		vbs[vb] = uint64(status)
	}

}

func (ss *StreamState) setRollbackTime(keyspaceId string, rollbackTime int64) {
	if rollbackTime != 0 {
		ss.keyspaceIdRollbackTime[keyspaceId] = rollbackTime
	}
}

func (ss *StreamState) getVbRefCount(streamId common.StreamId, keyspaceId string, vb Vbucket) int {

	return int(ss.streamKeyspaceIdVbRefCountMap[streamId][keyspaceId][vb])
}

func (ss *StreamState) clearVbRefCount(streamId common.StreamId, keyspaceId string, vb Vbucket) {

	ss.streamKeyspaceIdVbRefCountMap[streamId][keyspaceId][vb] = 0
}

func (ss *StreamState) incVbRefCount(streamId common.StreamId, keyspaceId string, vb Vbucket) {

	vbs := ss.streamKeyspaceIdVbRefCountMap[streamId][keyspaceId]
	vbs[vb] = vbs[vb] + 1
}

func (ss *StreamState) decVbRefCount(streamId common.StreamId, keyspaceId string, vb Vbucket) {

	vbs := ss.streamKeyspaceIdVbRefCountMap[streamId][keyspaceId]
	vbs[vb] = vbs[vb] - 1
}

//computes the restart Ts for the given keyspaceId and stream
func (ss *StreamState) computeRestartTs(streamId common.StreamId,
	keyspaceId string) *common.TsVbuuid {

	var restartTs *common.TsVbuuid

	if fts, ok := ss.streamKeyspaceIdLastFlushedTsMap[streamId][keyspaceId]; ok && fts != nil {
		restartTs = ss.streamKeyspaceIdLastFlushedTsMap[streamId][keyspaceId].Copy()
	} else if ts, ok := ss.streamKeyspaceIdRestartTsMap[streamId][keyspaceId]; ok && ts != nil {
		//if no flush has been done yet, use restart TS
		restartTs = ss.streamKeyspaceIdRestartTsMap[streamId][keyspaceId].Copy()
	}
	return restartTs
}

//computes the rollback Ts for the given keyspaceId and stream
func (ss *StreamState) computeRollbackTs(streamId common.StreamId, keyspaceId string) *common.TsVbuuid {

	if rollTs, ok := ss.streamKeyspaceIdKVRollbackTsMap[streamId][keyspaceId]; ok && rollTs.Len() > 0 {
		// Get restart Ts
		restartTs := ss.computeRestartTs(streamId, keyspaceId)
		if restartTs == nil {
			numVbuckets := ss.config["numVbuckets"].Int()
			restartTs = common.NewTsVbuuid(GetBucketFromKeyspaceId(keyspaceId), numVbuckets)
		} else {
			ss.adjustNonSnapAlignedVbs(restartTs, streamId, keyspaceId, nil, false)
			ss.adjustVbuuids(restartTs, streamId, keyspaceId)
		}

		// overlay KV rollback Ts on top of restart Ts
		return restartTs.Union(ss.streamKeyspaceIdKVRollbackTsMap[streamId][keyspaceId])
	}

	return nil
}

func (ss *StreamState) setHWTFromRestartTs(streamId common.StreamId,
	keyspaceId string) {

	logging.Debugf("StreamState::setHWTFromRestartTs Stream %v "+
		"KeyspaceId %v", streamId, keyspaceId)

	if keyspaceIdRestartTs, ok := ss.streamKeyspaceIdRestartTsMap[streamId]; ok {

		if restartTs, ok := keyspaceIdRestartTs[keyspaceId]; ok && restartTs != nil {

			//update HWT
			ss.streamKeyspaceIdHWTMap[streamId][keyspaceId] = restartTs.Copy()

			//update Last Flushed Ts
			ss.streamKeyspaceIdLastFlushedTsMap[streamId][keyspaceId] = restartTs.Copy()
			logging.Verbosef("StreamState::setHWTFromRestartTs HWT Set For "+
				"KeyspaceId %v StreamId %v. TS %v.", keyspaceId, streamId, restartTs)

			ss.streamKeyspaceIdLastMutationVbuuid[streamId][keyspaceId] = restartTs.Copy()

		} else {
			logging.Warnf("StreamState::setHWTFromRestartTs RestartTs Not Found For "+
				"KeyspaceId %v StreamId %v. No Value Set.", keyspaceId, streamId)
		}
	}
}

func (ss *StreamState) updateRepairState(streamId common.StreamId,
	keyspaceId string, repairVbs []Vbucket, shutdownVbs []Vbucket) {

	for _, vbno := range repairVbs {
		if ss.streamKeyspaceIdVbStatusMap[streamId][keyspaceId][vbno] == VBS_STREAM_END &&
			ss.streamKeyspaceIdRepairStateMap[streamId][keyspaceId][int(vbno)] == REPAIR_NONE {
			logging.Infof("StreamState::set repair state to RESTART_VB for %v keyspaceId %v vb %v", streamId, keyspaceId, vbno)
			ss.streamKeyspaceIdRepairStateMap[streamId][keyspaceId][int(vbno)] = REPAIR_RESTART_VB
		}
	}

	for _, vbno := range shutdownVbs {
		if ss.streamKeyspaceIdVbStatusMap[streamId][keyspaceId][vbno] == VBS_CONN_ERROR &&
			ss.streamKeyspaceIdRepairStateMap[streamId][keyspaceId][int(vbno)] == REPAIR_RESTART_VB {
			logging.Infof("StreamState::set repair state to SHUTDOWN_VB for %v keyspaceId %v vb %v", streamId, keyspaceId, vbno)
			ss.streamKeyspaceIdRepairStateMap[streamId][keyspaceId][int(vbno)] = REPAIR_SHUTDOWN_VB
		}
	}
}

// This function gets the list of vb and seqno to repair stream.
// Termination condition for stream repair:
// 1) All vb are in StreamBegin state
// 2) All vb have ref count == 1
// 3) There is no error in stream repair
func (ss *StreamState) getRepairTsForKeyspaceId(streamId common.StreamId,
	keyspaceId string) (*common.TsVbuuid, bool, []Vbucket, []Vbucket) {

	anythingToRepair := false

	numVbuckets := ss.config["numVbuckets"].Int()
	repairTs := common.NewTsVbuuid(GetBucketFromKeyspaceId(keyspaceId), numVbuckets)

	var shutdownVbs []Vbucket = nil
	var repairVbs []Vbucket = nil
	var count = 0

	hwtTs := ss.streamKeyspaceIdHWTMap[streamId][keyspaceId]

	// First step: Find out if any StreamEnd needs to be escalated to ConnErr.
	// If so, add it to ShutdownVbs (for shutdown/restart).
	for i, s := range ss.streamKeyspaceIdVbStatusMap[streamId][keyspaceId] {
		if s == VBS_STREAM_END &&
			ss.canShutdownOnRetry(streamId, keyspaceId, Vbucket(i)) {

			logging.Infof("StreamState::getRepairTsForKeyspaceId\n\t"+
				"KeyspaceId %v StreamId %v Vbucket %v exceeds repair wait time. Convert to CONN_ERROR.",
				keyspaceId, streamId, i)

			ss.makeConnectionError(streamId, keyspaceId, Vbucket(i))
		}
	}

	// Second step : Repair any StreamEnd, ConnError on any vb.
	for i, s := range ss.streamKeyspaceIdVbStatusMap[streamId][keyspaceId] {
		if s == VBS_STREAM_END || s == VBS_CONN_ERROR {

			count++
			anythingToRepair = true
			repairVbs = ss.addRepairTs(repairTs, hwtTs, Vbucket(i), repairVbs)

			if s == VBS_CONN_ERROR &&
				ss.streamKeyspaceIdRepairStateMap[streamId][keyspaceId][i] < REPAIR_SHUTDOWN_VB {
				logging.Infof("StreamState::getRepairTsForKeyspaceId\n\t"+
					"KeyspaceId %v StreamId %v Vbucket %v. Shutdown/Restart Vb.",
					keyspaceId, streamId, i)

				shutdownVbs = append(shutdownVbs, Vbucket(i))
			}
		}
	}

	// Third step: If there is nothing to repair, then double check if every vb has
	// exactly one vb owner.  If not, then the accounting is wrong (most likely due
	// to connection error).  Make the vb as ConnErr and continue to repair.
	// Note: Do not check for VBS_INIT.  RepairMissingStreamBegin will ensure that
	// indexer is getting all StreamBegin.
	if !anythingToRepair && !ss.needsRollback(streamId, keyspaceId) {
		for i, s := range ss.streamKeyspaceIdVbStatusMap[streamId][keyspaceId] {
			refCount := ss.streamKeyspaceIdVbRefCountMap[streamId][keyspaceId][i]
			if refCount != 1 && s != VBS_INIT {
				logging.Infof("StreamState::getRepairTsForKeyspaceId\n\t"+
					"KeyspaceId %v StreamId %v Vbucket %v have ref count (%v != 1). Convert to CONN_ERROR.",
					keyspaceId, streamId, i, refCount)
				// Make it a ConnErr such that subsequent retry will
				// force a shutdown/restart sequence.
				ss.makeConnectionError(streamId, keyspaceId, Vbucket(i))
				repairVbs = ss.addRepairTs(repairTs, hwtTs, Vbucket(i), repairVbs)
				count++
				shutdownVbs = append(shutdownVbs, Vbucket(i))
				anythingToRepair = true
			}
		}
	}

	if !anythingToRepair {
		ss.streamKeyspaceIdRestartVbTsMap[streamId][keyspaceId] = nil
	} else {
		ss.streamKeyspaceIdRestartVbTsMap[streamId][keyspaceId] = repairTs.Copy()
	}

	ss.adjustNonSnapAlignedVbs(repairTs, streamId, keyspaceId, repairVbs, true)
	ss.adjustVbuuids(repairTs, streamId, keyspaceId)

	logging.Verbosef("StreamState::getRepairTsForKeyspaceId\n\t"+
		"KeyspaceId %v StreamId %v repairTS %v",
		keyspaceId, streamId, repairTs)

	return repairTs, anythingToRepair, shutdownVbs, repairVbs
}

func (ss *StreamState) canShutdownOnRetry(streamId common.StreamId, keyspaceId string, vbno Vbucket) bool {

	if status := ss.streamKeyspaceIdVbStatusMap[streamId][keyspaceId][int(vbno)]; status != VBS_STREAM_END {
		return false
	}

	if ss.streamKeyspaceIdRepairStateMap[streamId][keyspaceId][int(vbno)] != REPAIR_RESTART_VB {
		return false
	}

	waitTime := int64(ss.config["timekeeper.escalate.StreamBeginWaitTime"].Int()) * int64(time.Second)

	if _, pendingVbuuid := ss.getKVPendingTs(streamId, keyspaceId, vbno); pendingVbuuid != 0 {
		// acknowledged by projector but not yet active
		waitTime = waitTime * 2
	}

	if waitTime > 0 {
		exceedBeginTime := time.Now().UnixNano()-int64(ss.streamKeyspaceIdLastBeginTime[streamId][keyspaceId]) > waitTime
		exceedRepairTime := time.Now().UnixNano()-int64(ss.streamKeyspaceIdLastRepairTimeMap[streamId][keyspaceId][vbno]) > waitTime

		return exceedBeginTime && exceedRepairTime
	}

	return false
}

func (ss *StreamState) startMTROnRetry(streamId common.StreamId, keyspaceId string) bool {

	startMTR := false

	for vbno, status := range ss.streamKeyspaceIdVbStatusMap[streamId][keyspaceId] {

		if status == VBS_STREAM_END {
			return false
		}

		if status != VBS_CONN_ERROR {
			continue
		}

		if ss.streamKeyspaceIdRepairStateMap[streamId][keyspaceId][vbno] != REPAIR_SHUTDOWN_VB {
			continue
		}

		waitTime := int64(ss.config["timekeeper.escalate.StreamBeginWaitTime"].Int()) * int64(time.Second)

		if _, pendingVbuuid := ss.getKVPendingTs(streamId, keyspaceId, Vbucket(vbno)); pendingVbuuid != 0 {
			// acknowledged by projector but not yet active
			waitTime = waitTime * 2
		}

		if waitTime > 0 {
			exceedBeginTime := time.Now().UnixNano()-int64(ss.streamKeyspaceIdLastBeginTime[streamId][keyspaceId]) > waitTime
			exceedRepairTime := time.Now().UnixNano()-int64(ss.streamKeyspaceIdLastRepairTimeMap[streamId][keyspaceId][vbno]) > waitTime

			if exceedBeginTime && exceedRepairTime {
				startMTR = true
			}
		}
	}

	return startMTR
}

func (ss *StreamState) startRecoveryOnRetry(streamId common.StreamId, keyspaceId string) bool {

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

	for vbno, status := range ss.streamKeyspaceIdVbStatusMap[streamId][keyspaceId] {

		if status == VBS_STREAM_END {
			return false
		}

		if status != VBS_CONN_ERROR {
			continue
		}

		if ss.streamKeyspaceIdRepairStateMap[streamId][keyspaceId][vbno] != REPAIR_MTR {
			continue
		}

		waitTime := int64(ss.config["timekeeper.escalate.StreamBeginWaitTime"].Int()) * int64(time.Second)

		if _, pendingVbuuid := ss.getKVPendingTs(streamId, keyspaceId, Vbucket(vbno)); pendingVbuuid != 0 {
			// acknowledged by projector but not yet active
			waitTime = waitTime * 2
		}

		if waitTime > 0 {
			exceedBeginTime := time.Now().UnixNano()-int64(ss.streamKeyspaceIdLastBeginTime[streamId][keyspaceId]) > waitTime
			exceedRepairTime := time.Now().UnixNano()-int64(ss.streamKeyspaceIdLastRepairTimeMap[streamId][keyspaceId][vbno]) > waitTime

			if exceedBeginTime && exceedRepairTime {
				startRecovery = true
			}
		}
	}

	return startRecovery
}

func (ss *StreamState) hasConnectionError(streamId common.StreamId, keyspaceId string) bool {

	for _, s := range ss.streamKeyspaceIdVbStatusMap[streamId][keyspaceId] {
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
func (ss *StreamState) makeConnectionError(streamId common.StreamId, keyspaceId string, vbno Vbucket) {

	ss.streamKeyspaceIdVbStatusMap[streamId][keyspaceId][vbno] = VBS_CONN_ERROR
	ss.clearVbRefCount(streamId, keyspaceId, vbno)

	// clear rollbackTs and failTs for this vbucket
	ss.clearKVActiveTs(streamId, keyspaceId, vbno)
	ss.clearKVPendingTs(streamId, keyspaceId, vbno)
	ss.clearKVRollbackTs(streamId, keyspaceId, vbno)

	ss.streamKeyspaceIdRepairStateMap[streamId][keyspaceId][vbno] = REPAIR_RESTART_VB
	ss.setLastRepairTime(streamId, keyspaceId, vbno)

	logging.Infof("StreamState::connection error - set repair state to RESTART_VB for %v keyspaceId %v vb %v", streamId, keyspaceId, vbno)
}

func (ss *StreamState) addRepairTs(repairTs *common.TsVbuuid, hwtTs *common.TsVbuuid, vbno Vbucket, repairSeqno []Vbucket) []Vbucket {

	repairTs.Seqnos[vbno] = hwtTs.Seqnos[vbno]
	repairTs.Vbuuids[vbno] = hwtTs.Vbuuids[vbno]
	repairTs.Snapshots[vbno][0] = hwtTs.Snapshots[vbno][0]
	repairTs.Snapshots[vbno][1] = hwtTs.Snapshots[vbno][1]

	return append(repairSeqno, vbno)
}

func (ss *StreamState) addKVRollbackTs(streamId common.StreamId, keyspaceId string, vbno Vbucket, seqno uint64, vbuuid uint64) {

	rollbackTs := ss.streamKeyspaceIdKVRollbackTsMap[streamId][keyspaceId]
	if rollbackTs == nil {
		numVbuckets := ss.config["numVbuckets"].Int()
		rollbackTs = common.NewTsVbuuid(GetBucketFromKeyspaceId(keyspaceId), numVbuckets)
		ss.streamKeyspaceIdKVRollbackTsMap[streamId][keyspaceId] = rollbackTs
	}

	rollbackTs.Seqnos[vbno] = seqno
	rollbackTs.Vbuuids[vbno] = vbuuid
	rollbackTs.Snapshots[vbno][0] = 0
	rollbackTs.Snapshots[vbno][1] = 0
}

func (ss *StreamState) clearKVRollbackTs(streamId common.StreamId, keyspaceId string, vbno Vbucket) {

	rollbackTs := ss.streamKeyspaceIdKVRollbackTsMap[streamId][keyspaceId]
	if rollbackTs != nil {
		rollbackTs.Seqnos[vbno] = 0
		rollbackTs.Vbuuids[vbno] = 0
		rollbackTs.Snapshots[vbno][0] = 0
		rollbackTs.Snapshots[vbno][1] = 0
	}
}

func (ss *StreamState) getKVRollbackTs(streamId common.StreamId, keyspaceId string, vbno Vbucket) (uint64, uint64) {

	rollbackTs := ss.streamKeyspaceIdKVRollbackTsMap[streamId][keyspaceId]
	if rollbackTs == nil {
		return 0, 0
	}
	return rollbackTs.Seqnos[vbno], rollbackTs.Vbuuids[vbno]
}

func (ss *StreamState) needsRollback(streamId common.StreamId, keyspaceId string) bool {

	rollbackTs := ss.streamKeyspaceIdKVRollbackTsMap[streamId][keyspaceId]
	for _, vbuuid := range rollbackTs.Vbuuids {
		if vbuuid != 0 {
			return true
		}
	}

	return false
}

func (ss *StreamState) needsRollbackToZero(streamId common.StreamId, keyspaceId string) bool {

	rollbackTs := ss.streamKeyspaceIdKVRollbackTsMap[streamId][keyspaceId]
	for i, vbuuid := range rollbackTs.Vbuuids {
		if vbuuid != 0 && rollbackTs.Seqnos[i] == 0 {
			return true
		}
	}

	return false
}

func (ss *StreamState) numKVRollbackTs(streamId common.StreamId, keyspaceId string) int {

	count := 0
	rollbackTs := ss.streamKeyspaceIdKVRollbackTsMap[streamId][keyspaceId]
	for _, vbuuid := range rollbackTs.Vbuuids {
		if vbuuid != 0 {
			count++
		}
	}

	return count
}

func (ss *StreamState) addKVActiveTs(streamId common.StreamId, keyspaceId string, vbno Vbucket, seqno uint64, vbuuid uint64) {

	activeTs := ss.streamKeyspaceIdKVActiveTsMap[streamId][keyspaceId]
	if activeTs == nil {
		numVbuckets := ss.config["numVbuckets"].Int()
		activeTs = common.NewTsVbuuid(GetBucketFromKeyspaceId(keyspaceId), numVbuckets)
		ss.streamKeyspaceIdKVActiveTsMap[streamId][keyspaceId] = activeTs
	}

	activeTs.Seqnos[vbno] = seqno
	activeTs.Vbuuids[vbno] = vbuuid
	activeTs.Snapshots[vbno][0] = 0
	activeTs.Snapshots[vbno][1] = 0
}

func (ss *StreamState) clearKVActiveTs(streamId common.StreamId, keyspaceId string, vbno Vbucket) {

	activeTs := ss.streamKeyspaceIdKVActiveTsMap[streamId][keyspaceId]
	if activeTs != nil {
		activeTs.Seqnos[vbno] = 0
		activeTs.Vbuuids[vbno] = 0
		activeTs.Snapshots[vbno][0] = 0
		activeTs.Snapshots[vbno][1] = 0
	}
}

func (ss *StreamState) getKVActiveTs(streamId common.StreamId, keyspaceId string, vbno Vbucket) (uint64, uint64) {

	activeTs := ss.streamKeyspaceIdKVActiveTsMap[streamId][keyspaceId]
	if activeTs == nil {
		return 0, 0
	}

	return activeTs.Seqnos[vbno], activeTs.Vbuuids[vbno]
}

func (ss *StreamState) addKVPendingTs(streamId common.StreamId, keyspaceId string, vbno Vbucket, seqno uint64, vbuuid uint64) {

	pendingTs := ss.streamKeyspaceIdKVPendingTsMap[streamId][keyspaceId]
	if pendingTs == nil {
		numVbuckets := ss.config["numVbuckets"].Int()
		pendingTs = common.NewTsVbuuid(GetBucketFromKeyspaceId(keyspaceId), numVbuckets)
		ss.streamKeyspaceIdKVPendingTsMap[streamId][keyspaceId] = pendingTs
	}

	pendingTs.Seqnos[vbno] = seqno
	pendingTs.Vbuuids[vbno] = vbuuid
	pendingTs.Snapshots[vbno][0] = 0
	pendingTs.Snapshots[vbno][1] = 0
}

func (ss *StreamState) clearKVPendingTs(streamId common.StreamId, keyspaceId string, vbno Vbucket) {

	pendingTs := ss.streamKeyspaceIdKVPendingTsMap[streamId][keyspaceId]
	if pendingTs != nil {
		pendingTs.Seqnos[vbno] = 0
		pendingTs.Vbuuids[vbno] = 0
		pendingTs.Snapshots[vbno][0] = 0
		pendingTs.Snapshots[vbno][1] = 0
	}
}

func (ss *StreamState) getKVPendingTs(streamId common.StreamId, keyspaceId string, vbno Vbucket) (uint64, uint64) {

	pendingTs := ss.streamKeyspaceIdKVPendingTsMap[streamId][keyspaceId]
	if pendingTs == nil {
		return 0, 0
	}

	return pendingTs.Seqnos[vbno], pendingTs.Vbuuids[vbno]
}

func (ss *StreamState) setLastRepairTime(streamId common.StreamId, keyspaceId string, vbno Vbucket) {

	repairTimeMap := ss.streamKeyspaceIdLastRepairTimeMap[streamId][keyspaceId]
	if repairTimeMap == nil {
		numVbuckets := ss.config["numVbuckets"].Int()
		repairTimeMap = NewTimestamp(numVbuckets)
		ss.streamKeyspaceIdLastRepairTimeMap[streamId][keyspaceId] = repairTimeMap
	}
	repairTimeMap[vbno] = uint64(time.Now().UnixNano())
}

func (ss *StreamState) getLastRepairTime(streamId common.StreamId, keyspaceId string, vbno Vbucket) int64 {

	repairTimeMap := ss.streamKeyspaceIdLastRepairTimeMap[streamId][keyspaceId]
	if repairTimeMap == nil {
		return 0
	}
	return int64(repairTimeMap[vbno])
}

func (ss *StreamState) clearLastRepairTime(streamId common.StreamId, keyspaceId string, vbno Vbucket) {

	repairTimeMap := ss.streamKeyspaceIdLastRepairTimeMap[streamId][keyspaceId]
	if repairTimeMap != nil {
		repairTimeMap[vbno] = 0
	}
}

func (ss *StreamState) resetAllLastRepairTime(streamId common.StreamId, keyspaceId string) {

	now := time.Now().UnixNano()
	for i, t := range ss.streamKeyspaceIdLastRepairTimeMap[streamId][keyspaceId] {
		if t != 0 {
			ss.streamKeyspaceIdLastRepairTimeMap[streamId][keyspaceId][i] = uint64(now)
		}
	}
}

func (ss *StreamState) clearRepairState(streamId common.StreamId, keyspaceId string, vbno Vbucket) {

	state := ss.streamKeyspaceIdRepairStateMap[streamId][keyspaceId]
	if state != nil {
		ss.streamKeyspaceIdRepairStateMap[streamId][keyspaceId][vbno] = REPAIR_NONE
	}
}

func (ss *StreamState) clearAllRepairState(streamId common.StreamId, keyspaceId string) {

	logging.Infof("StreamState::clear all repair state for %v keyspaceId %v", streamId, keyspaceId)

	numVbuckets := ss.config["numVbuckets"].Int()
	bucket := GetBucketFromKeyspaceId(keyspaceId)
	ss.streamKeyspaceIdRepairStateMap[streamId][keyspaceId] = make([]RepairState, numVbuckets)
	ss.streamKeyspaceIdLastBeginTime[streamId][keyspaceId] = 0
	ss.streamKeyspaceIdLastRepairTimeMap[streamId][keyspaceId] = NewTimestamp(numVbuckets)
	ss.streamKeyspaceIdKVActiveTsMap[streamId][keyspaceId] = common.NewTsVbuuid(bucket, numVbuckets)
	ss.streamKeyspaceIdKVPendingTsMap[streamId][keyspaceId] = common.NewTsVbuuid(bucket, numVbuckets)
	ss.streamKeyspaceIdKVRollbackTsMap[streamId][keyspaceId] = common.NewTsVbuuid(bucket, numVbuckets)
}

//
// Return true if all vb has received streamBegin regardless of rollbackTs.
// If indexer has seen StreamBegin for a vb, but the vb has later received
// StreamEnd or ConnErr, this function will still return false.
//
func (ss *StreamState) seenAllVbs(streamId common.StreamId, keyspaceId string) bool {

	numVbuckets := ss.config["numVbuckets"].Int()
	vbs := ss.streamKeyspaceIdVbStatusMap[streamId][keyspaceId]

	for i := 0; i < numVbuckets; i++ {
		status := vbs[i]
		if status != VBS_STREAM_BEGIN {
			return false
		}
	}

	return true
}

func (ss *StreamState) canRollbackNow(streamId common.StreamId, keyspaceId string) (bool, bool) {

	canRollback := false
	needsRollback := false

	if ss.needsRollback(streamId, keyspaceId) {
		needsRollback = true

		numRollback := ss.numKVRollbackTs(streamId, keyspaceId)
		numVbuckets := ss.config["numVbuckets"].Int()

		waitTime := int64(ss.config["timekeeper.rollback.StreamBeginWaitTime"].Int()) * int64(time.Second)
		exceedWaitTime := time.Now().UnixNano()-int64(ss.streamKeyspaceIdLastBeginTime[streamId][keyspaceId]) > waitTime

		//if any vb needs a rollback to zero, there is no need to wait
		rollbackToZero := ss.needsRollbackToZero(streamId, keyspaceId)

		canRollback = !ss.streamKeyspaceIdAsyncMap[streamId][keyspaceId] ||
			exceedWaitTime ||
			numRollback == numVbuckets ||
			rollbackToZero
	}

	if !needsRollback {
		needsRollback = ss.startRecoveryOnRetry(streamId, keyspaceId)
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
	streamId common.StreamId, keyspaceId string, repairVbs []Vbucket, forStreamRepair bool) {

	// The caller either provide a vector of vb to repair, or this function will
	// look for any vb in the repairTS that has a non-zero vbuuid.
	if repairVbs == nil {
		for _, vbno := range repairTs.GetVbnos() {
			repairVbs = append(repairVbs, Vbucket(vbno))
		}
	}

	logging.Infof("StreamState::adjustNonSnapAlignedVbs\n\t"+
		"KeyspaceId %v StreamId %v Vbuckets %v.",
		keyspaceId, streamId, repairVbs)

	for _, vbno := range repairVbs {

		if !(repairTs.Seqnos[vbno] >= repairTs.Snapshots[vbno][0] &&
			repairTs.Seqnos[vbno] <= repairTs.Snapshots[vbno][1]) ||
			repairTs.Vbuuids[vbno] == 0 {

			// First, use the last flush TS seqno if avaliable
			if fts, ok := ss.streamKeyspaceIdLastFlushedTsMap[streamId][keyspaceId]; ok && fts != nil {
				repairTs.Snapshots[vbno][0] = fts.Snapshots[vbno][0]
				repairTs.Snapshots[vbno][1] = fts.Snapshots[vbno][1]
				repairTs.Seqnos[vbno] = fts.Seqnos[vbno]
				repairTs.Vbuuids[vbno] = fts.Vbuuids[vbno]
			}

			// If last flush TS is still out-of-bound, use last Snap-aligned flushed TS if available
			if !(repairTs.Seqnos[vbno] >= repairTs.Snapshots[vbno][0] &&
				repairTs.Seqnos[vbno] <= repairTs.Snapshots[vbno][1]) ||
				repairTs.Vbuuids[vbno] == 0 {
				if fts, ok := ss.streamKeyspaceIdLastSnapAlignFlushedTsMap[streamId][keyspaceId]; ok && fts != nil {
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
				if rts, ok := ss.streamKeyspaceIdOpenTsMap[streamId][keyspaceId]; ok && rts != nil {
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
				if rts, ok := ss.streamKeyspaceIdRestartTsMap[streamId][keyspaceId]; ok && rts != nil {
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
	keyspaceId string) bool {

	if keyspaceIdVbStatusMap, ok := ss.streamKeyspaceIdVbStatusMap[streamId]; ok {
		if vbs, ok := keyspaceIdVbStatusMap[keyspaceId]; ok {
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
	keyspaceId string, buildTs Timestamp) bool {

	ts := ss.streamKeyspaceIdHWTMap[streamId][keyspaceId]

	//all snapshot markers should be present, except if buildTs has 0
	//seqnum for the vbucket
	for i, s := range ts.Snapshots {
		if s[1] == 0 && buildTs[i] != 0 {
			return false
		}
	}
	return true
}

//checks if the given keyspaceId in this stream has any flush in progress
func (ss *StreamState) checkAnyFlushPending(streamId common.StreamId,
	keyspaceId string) bool {

	keyspaceIdFlushInProgressTsMap := ss.streamKeyspaceIdFlushInProgressTsMap[streamId]

	//check if there is any flush in progress. No flush in progress implies
	//there is no flush pending as well bcoz every flush done triggers
	//the next flush.
	if ts := keyspaceIdFlushInProgressTsMap[keyspaceId]; ts != nil {
		return true
	}
	return false
}

//checks if none of the keyspaceIds in this stream has any flush abort in progress
func (ss *StreamState) checkAnyAbortPending(streamId common.StreamId,
	keyspaceId string) bool {

	keyspaceIdAbortInProgressMap := ss.streamKeyspaceIdAbortInProgressMap[streamId]

	if running := keyspaceIdAbortInProgressMap[keyspaceId]; running {
		return true
	}
	return false
}

//updateHWT will update the HW Timestamp for a keyspaceId in the stream
//based on the Sync message received.
func (ss *StreamState) updateHWT(streamId common.StreamId,
	keyspaceId string, hwt *common.TsVbuuid, hwtOSO *common.TsVbuuid, prevSnap *common.TsVbuuid) {

	ts := ss.streamKeyspaceIdHWTMap[streamId][keyspaceId]
	partialSnap := false

	for i, seq := range hwt.Seqnos {

		//update OSO bookkeeping
		if hwtOSO != nil {

			tsOSO := ss.streamKeyspaceIdHWTOSO[streamId][keyspaceId]

			//if mutation count has incremented
			if hwtOSO.Vbuuids[i] > tsOSO.Vbuuids[i] {
				tsOSO.Seqnos[i] = hwtOSO.Seqnos[i]   //high seqno
				tsOSO.Vbuuids[i] = hwtOSO.Vbuuids[i] //Vbuuid stores count for OSO
				ss.streamKeyspaceIdNewTsReqdMap[streamId][keyspaceId] = true
			}

			//OSO Snap Start
			tsOSO.Snapshots[i][0] = hwtOSO.Snapshots[i][0]

			//OSO Snap End
			if hwtOSO.Snapshots[i][1] > tsOSO.Snapshots[i][1] {
				ss.streamKeyspaceIdNewTsReqdMap[streamId][keyspaceId] = true
			}
			tsOSO.Snapshots[i][1] = hwtOSO.Snapshots[i][1]
		}

		if seq > ts.Seqnos[i] { //if seqno has incremented, update it
			ts.Seqnos[i] = seq
			ss.streamKeyspaceIdNewTsReqdMap[streamId][keyspaceId] = true
		}
		//if snapEnd is greater than current hwt snapEnd
		if hwt.Snapshots[i][1] > ts.Snapshots[i][1] {
			lastSnap := ss.streamKeyspaceIdLastSnapMarker[streamId][keyspaceId]
			//store the prev snap marker in the lastSnapMarker map
			lastSnap.Snapshots[i][0] = prevSnap.Snapshots[i][0]
			lastSnap.Snapshots[i][1] = prevSnap.Snapshots[i][1]
			lastSnap.Vbuuids[i] = prevSnap.Vbuuids[i]
			lastSnap.Seqnos[i] = prevSnap.Seqnos[i]

			//store the new snap marker in hwt
			ts.Snapshots[i][0] = hwt.Snapshots[i][0]
			ts.Snapshots[i][1] = hwt.Snapshots[i][1]
			//	ts.Vbuuids[i] = hwt.Vbuuids[i]
			ss.streamKeyspaceIdNewTsReqdMap[streamId][keyspaceId] = true
			if prevSnap.Seqnos[i] != prevSnap.Snapshots[i][1] &&
				prevSnap.Snapshots[i][0] != 0 {
				logging.Warnf("StreamState::updateHWT Received Partial Last Snapshot in HWT "+
					"KeyspaceId %v StreamId %v vbucket %v Snapshot %v-%v Seqno %v Vbuuid %v lastSnap %v-%v lastSnapSeqno %v",
					keyspaceId, streamId, i, hwt.Snapshots[i][0], hwt.Snapshots[i][1], hwt.Seqnos[i], ts.Vbuuids[i],
					prevSnap.Snapshots[i][0], prevSnap.Snapshots[i][1], prevSnap.Seqnos[i])
				partialSnap = true

			}

		} else if hwt.Snapshots[i][1] < ts.Snapshots[i][1] {
			// Catch any out of order Snapshot.   StreamReader should make sure that Snapshot is monotonic increasing
			logging.Debugf("StreamState::updateHWT.  Recieved a snapshot marker older than current hwt snapshot. "+
				"KeyspaceId %v StreamId %v vbucket %v Current Snapshot %v-%v New Snapshot %v-%v",
				keyspaceId, streamId, i, ts.Snapshots[i][0], ts.Snapshots[i][1], hwt.Snapshots[i][0], hwt.Snapshots[i][1])
		}
	}

	if hwt.DisableAlign {
		logging.Warnf("StreamState::updateHWT Received Partial Snapshot Message in HWT "+
			"StreamId %v KeyspaceId %v", streamId, keyspaceId)
	}

	if partialSnap || hwt.DisableAlign {
		ss.disableSnapAlignForPendingTs(streamId, keyspaceId)
	}

	logging.LazyTrace(func() string {
		return fmt.Sprintf("StreamState::updateHWT HWT Updated : %v", ts)
	})
}

func (ss *StreamState) checkNewTSDue(streamId common.StreamId, keyspaceId string) bool {
	newTsReqd := ss.streamKeyspaceIdNewTsReqdMap[streamId][keyspaceId]
	return newTsReqd
}

func (ss *StreamState) checkCommitOverdue(streamId common.StreamId, keyspaceId string) bool {

	snapPersistInterval := ss.getPersistInterval()
	persistDuration := time.Duration(snapPersistInterval) * time.Millisecond

	lastPersistTime := ss.streamKeyspaceIdLastPersistTime[streamId][keyspaceId]

	if time.Since(lastPersistTime) > persistDuration {

		keyspaceIdFlushInProgressTsMap := ss.streamKeyspaceIdFlushInProgressTsMap[streamId]
		keyspaceIdTsListMap := ss.streamKeyspaceIdTsListMap[streamId]
		keyspaceIdFlushEnabledMap := ss.streamKeyspaceIdFlushEnabledMap[streamId]
		keyspaceIdNeedsCommit := ss.streamKeyspaceIdNeedsCommitMap[streamId]

		//if there is no flush already in progress for this keyspaceId
		//no pending TS in list and flush is not disabled
		tsList := keyspaceIdTsListMap[keyspaceId]
		if keyspaceIdFlushInProgressTsMap[keyspaceId] == nil &&
			keyspaceIdFlushEnabledMap[keyspaceId] == true &&
			tsList.Len() == 0 &&
			keyspaceIdNeedsCommit[keyspaceId] == true {
			return true
		}
	}

	return false
}

//gets the stability timestamp based on the current HWT
func (ss *StreamState) getNextStabilityTS(streamId common.StreamId,
	keyspaceId string) *TsListElem {

	//generate new stability timestamp
	tsVbuuid := ss.streamKeyspaceIdHWTMap[streamId][keyspaceId].Copy()

	tsElem := &TsListElem{
		ts: tsVbuuid,
	}

	enableOSO := ss.streamKeyspaceIdEnableOSO[streamId][keyspaceId]
	if enableOSO {
		tsElem.ts.SetSnapType(common.NO_SNAP_OSO)
		hwtOSO := ss.streamKeyspaceIdHWTOSO[streamId][keyspaceId]
		ss.setCountForOSOTs(streamId, keyspaceId, tsElem, hwtOSO)
	} else {
		tsElem.ts.SetSnapType(common.NO_SNAP)
	}

	ss.alignSnapBoundary(streamId, keyspaceId, tsElem, enableOSO)

	//reset state for next TS
	ss.streamKeyspaceIdNewTsReqdMap[streamId][keyspaceId] = false

	if tsElem.ts.CheckSnapAligned() {
		tsElem.ts.SetSnapAligned(true)
	}

	return tsElem
}

//align the snap boundary of TS if the seqno of the TS falls within the range of
//last snap marker
func (ss *StreamState) alignSnapBoundary(streamId common.StreamId,
	keyspaceId string, tsElem *TsListElem, enableOSO bool) {

	var smallSnap uint64
	if streamId == common.INIT_STREAM {
		smallSnap = ss.config["init_stream.smallSnapshotThreshold"].Uint64()
	} else {
		smallSnap = ss.config["settings.smallSnapshotThreshold"].Uint64()
	}

	lastSnap := ss.streamKeyspaceIdLastSnapMarker[streamId][keyspaceId]

	ts := tsElem.ts
	for i, s := range ts.Snapshots {

		if enableOSO {

			//if ts has OSO snapshot, skip
			if ts.OSOCount != nil &&
				ts.OSOCount[i] != 0 &&
				s[0] == 1 {
				continue
			}

			//if lastSnap OSO, skip
			if lastSnap.Snapshots[i][0] == 1 &&
				lastSnap.Snapshots[i][1] == 1 &&
				lastSnap.Seqnos[i] > lastSnap.Snapshots[i][1] {
				continue
			}
		}

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
					"Snapshot. KeyspaceId %v StreamId %v vbucket %v Snapshot %v-%v Seqno %v Vbuuid %v lastSnap %v-%v",
					keyspaceId, streamId, i, ts.Snapshots[i][0], ts.Snapshots[i][1], ts.Seqnos[i], ts.Vbuuids[i],
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
					"KeyspaceId %v StreamId %v vbucket %v Snapshot %v-%v Seqno %v Vbuuid %v lastSnap %v-%v lastSnapSeqno %v",
					keyspaceId, streamId, i, ts.Snapshots[i][0], ts.Snapshots[i][1], ts.Seqnos[i], ts.Vbuuids[i],
					lastSnap.Snapshots[i][0], lastSnap.Snapshots[i][1], lastSnap.Seqnos[i])
			}

		}

		if !(ts.Seqnos[i] >= ts.Snapshots[i][0] && ts.Seqnos[i] <= ts.Snapshots[i][1]) {
			logging.Warnf("StreamState::alignSnapBoundary.  TS falls out of snapshot boundary. "+
				"KeyspaceId %v StreamId %v vbucket %v Snapshot %v-%v Seqno %v Vbuuid %v lastSnap %v-%v",
				keyspaceId, streamId, i, ts.Snapshots[i][0], ts.Snapshots[i][1], ts.Seqnos[i], ts.Vbuuids[i],
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
	keyspaceId string) bool {

	keyspaceIdFlushInProgressTsMap := ss.streamKeyspaceIdFlushInProgressTsMap[streamId]
	keyspaceIdTsListMap := ss.streamKeyspaceIdTsListMap[streamId]
	keyspaceIdFlushEnabledMap := ss.streamKeyspaceIdFlushEnabledMap[streamId]

	//if there is no flush already in progress for this keyspaceId
	//no pending TS in list and flush is not disabled, send new TS
	tsList := keyspaceIdTsListMap[keyspaceId]
	if keyspaceIdFlushInProgressTsMap[keyspaceId] == nil &&
		keyspaceIdFlushEnabledMap[keyspaceId] == true &&
		tsList.Len() == 0 {
		return true
	}

	return false

}

//computes which vbuckets have mutations compared to last flush
func (ss *StreamState) computeTsChangeVec(streamId common.StreamId,
	keyspaceId string, tsElem *TsListElem) ([]bool, bool, []uint64) {

	ts := tsElem.ts

	numVbuckets := len(ts.Snapshots)
	changeVec := make([]bool, numVbuckets)
	noChange := true

	var countVec []uint64
	enableOSO := ss.streamKeyspaceIdEnableOSO[streamId][keyspaceId]
	if enableOSO {
		countVec = make([]uint64, numVbuckets)
	}

	//if there is a lastFlushedTs, compare with that
	if lts, ok := ss.streamKeyspaceIdLastFlushedTsMap[streamId][keyspaceId]; ok && lts != nil {

		for i, s := range ts.Seqnos {

			//if OSO snapshot
			if enableOSO &&
				ts.OSOCount != nil &&
				ts.OSOCount[i] != 0 &&
				ts.Snapshots[i][0] == 1 &&
				s != 0 {

				var ltsCount uint64
				if lts.OSOCount != nil {
					ltsCount = lts.OSOCount[i]
				}

				if ts.OSOCount[i] > ltsCount {
					changeVec[i] = true
					noChange = false
					countVec[i] = ts.OSOCount[i] - ltsCount //count to be flushed

				}
				if ts.Snapshots[i][1] == 1 {
					//once the OSO snapshot is complete
					//update the snapshot, seqno to high seqno
					ts.Snapshots[i][0] = ts.Seqnos[i]
					ts.Snapshots[i][1] = ts.Seqnos[i]

					//consider OSO End as change. Build is only
					//considered done when all OSO End markers have arrived.
					if lts.Snapshots[i][1] == 0 {
						noChange = false
					}
				} else {
					ts.Seqnos[i] = ts.OSOCount[i]
				}
				continue
			}
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
			//if OSO snapshot
			if enableOSO &&
				ts.OSOCount != nil &&
				ts.OSOCount[i] != 0 &&
				ts.Snapshots[i][0] == 1 &&
				s != 0 {

				changeVec[i] = true
				noChange = false
				countVec[i] = ts.OSOCount[i] //count to be flushed

				//once the OSO snapshot is complete
				//update the snapshot, seqno to high seqno
				if ts.Snapshots[i][1] == 1 {
					ts.Snapshots[i][0] = ts.Seqnos[i]
					ts.Snapshots[i][1] = ts.Seqnos[i]
				} else {
					ts.Seqnos[i] = ts.OSOCount[i]
				}

			} else if s > 0 {
				changeVec[i] = true
				noChange = false
			}
		}
	}

	if enableOSO {
		if ts.CheckSnapAligned() {
			ts.SetSnapAligned(true)
		}
	}

	return changeVec, noChange, countVec
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

func (ss *StreamState) disableSnapAlignForPendingTs(streamId common.StreamId, keyspaceId string) {

	tsList := ss.streamKeyspaceIdTsListMap[streamId][keyspaceId]
	disableCount := 0
	for e := tsList.Front(); e != nil; e = e.Next() {
		tsElem := e.Value.(*TsListElem)
		ts := tsElem.ts
		ts.SetDisableAlign(true)
		disableCount++
	}
	if disableCount > 0 {
		logging.Infof("StreamState::disableSnapAlignForPendingTs Stream %v KeyspaceId %v Disabled Snap Align for %v TS", streamId, keyspaceId, disableCount)

	}
}

func (ss *StreamState) updateLastMutationVbuuid(streamId common.StreamId,
	keyspaceId string, fts *common.TsVbuuid) {

	pts := ss.streamKeyspaceIdLastMutationVbuuid[streamId][keyspaceId]
	if pts == nil {
		ss.streamKeyspaceIdLastMutationVbuuid[streamId][keyspaceId] = fts.Copy()
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
	streamId common.StreamId, keyspaceId string) {

	if restartTs == nil {
		return
	}

	//use the vbuuids with last known mutation
	if pts, ok := ss.streamKeyspaceIdLastMutationVbuuid[streamId][keyspaceId]; ok && pts != nil {
		for i, _ := range pts.Vbuuids {
			if restartTs.Seqnos[i] == pts.Seqnos[i] &&
				restartTs.Seqnos[i] != 0 &&
				restartTs.Vbuuids[i] != pts.Vbuuids[i] {
				logging.Infof("StreamState::adjustVbuuids %v %v Vb %v Seqno %v "+
					"From %v To %v", streamId, keyspaceId, i, pts.Seqnos[i], restartTs.Vbuuids[i],
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
	keyspaceId string, restartTs *common.TsVbuuid) *common.TsVbuuid {

	var retryTs *common.TsVbuuid

	if pts, ok := ss.streamKeyspaceIdLastMutationVbuuid[streamId][keyspaceId]; ok && pts != nil {

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

func (ss *StreamState) getSessionId(streamId common.StreamId, keyspaceId string) uint64 {
	return ss.streamKeyspaceIdSessionId[streamId][keyspaceId]
}

func (ss *StreamState) CloneCollectionIdMap(streamId common.StreamId) KeyspaceIdCollectionId {

	outMap := make(KeyspaceIdCollectionId)
	for k, v := range ss.streamKeyspaceIdCollectionId[streamId] {
		outMap[k] = v
	}
	return outMap

}

func (ss *StreamState) CloneKeyspaceIdRollbackTime() map[string]int64 {

	outMap := make(map[string]int64)
	for k, v := range ss.keyspaceIdRollbackTime {
		outMap[k] = v
	}
	return outMap
}

func (ss *StreamState) setCountForOSOTs(streamId common.StreamId,
	keyspaceId string, tsElem *TsListElem, hwtOSO *common.TsVbuuid) {

	ts := tsElem.ts
	for i, sn := range ts.Snapshots {

		//if vb got an OSO Snapshot
		if hwtOSO.Snapshots[i][0] == 1 &&
			hwtOSO.Seqnos[i] != 0 {
			//if there is a regular snapshot with mutations
			if sn[1] != 0 && ts.Seqnos[i] != 0 {
				//use latest snapshot information already in ts
			} else {
				//NOTE if OSO has ended and the next snapshot marker has come in
				//but no new mutations, then that TS still needs to go out with count

				hwtOSO := ss.streamKeyspaceIdHWTOSO[streamId][keyspaceId]
				ts.Seqnos[i] = hwtOSO.Seqnos[i] //high seqno
				ts.Snapshots[i][0] = hwtOSO.Snapshots[i][0]
				ts.Snapshots[i][1] = hwtOSO.Snapshots[i][1]

				if ts.OSOCount == nil {
					ts.OSOCount = make([]uint64, len(hwtOSO.Seqnos))
				}
				ts.OSOCount[i] = hwtOSO.Vbuuids[i] //count

				//if this is an open OSO snap(i.e. OSO End has not been received)
				if ts.Snapshots[i][1] != 1 {
					ts.SetOpenOSOSnap(true)
				}
			}
		}
	}
}
