// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

import (
	"fmt"
	"strings"
	"time"

	"github.com/couchbase/indexing/secondary/common"
)

type MsgType int16

const (

	//General Messages
	MSG_SUCCESS = iota
	MSG_ERROR
	MSG_TIMESTAMP

	//Component specific messages

	//STREAM_READER
	STREAM_READER_STREAM_DROP_DATA
	STREAM_READER_STREAM_BEGIN
	STREAM_READER_STREAM_END
	STREAM_READER_SYNC
	STREAM_READER_SNAPSHOT_MARKER
	STREAM_READER_UPDATE_QUEUE_MAP
	STREAM_READER_ERROR
	STREAM_READER_SHUTDOWN
	STREAM_READER_CONN_ERROR
	STREAM_READER_HWT
	STREAM_READER_SYSTEM_EVENT
	STREAM_READER_OSO_SNAPSHOT_MARKER

	//MUTATION_MANAGER
	MUT_MGR_PERSIST_MUTATION_QUEUE
	MUT_MGR_ABORT_PERSIST
	MUT_MGR_DRAIN_MUTATION_QUEUE
	MUT_MGR_GET_MUTATION_QUEUE_HWT
	MUT_MGR_GET_MUTATION_QUEUE_LWT
	MUT_MGR_SHUTDOWN
	MUT_MGR_FLUSH_DONE
	MUT_MGR_ABORT_DONE
	MUT_MGR_STREAM_CLOSE

	//TIMEKEEPER
	TK_SHUTDOWN
	TK_STABILITY_TIMESTAMP
	TK_INIT_BUILD_DONE
	TK_INIT_BUILD_DONE_ACK
	TK_INIT_BUILD_DONE_NO_CATCHUP_ACK
	TK_ADD_INSTANCE_FAIL
	TK_ENABLE_FLUSH
	TK_DISABLE_FLUSH
	TK_MERGE_STREAM
	TK_MERGE_STREAM_ACK
	TK_GET_KEYSPACE_HWT

	//STORAGE_MANAGER
	STORAGE_MGR_SHUTDOWN
	STORAGE_INDEX_SNAP_REQUEST
	STORAGE_INDEX_STORAGE_STATS
	STORAGE_INDEX_COMPACT
	STORAGE_SNAP_DONE
	STORAGE_INDEX_MERGE_SNAPSHOT
	STORAGE_INDEX_PRUNE_SNAPSHOT
	STORAGE_UPDATE_SNAP_MAP

	//KVSender
	KV_SENDER_SHUTDOWN
	KV_SENDER_GET_CURR_KV_TS
	KV_SENDER_RESTART_VBUCKETS
	KV_SENDER_RESTART_VBUCKETS_RESPONSE
	KV_SENDER_REPAIR_ENDPOINTS
	KV_STREAM_REPAIR
	MSG_SUCCESS_OPEN_STREAM

	//ADMIN_MGR
	ADMIN_MGR_SHUTDOWN
	MSG_SUCCESS_DROP

	//CLUSTER_MGR
	CLUST_MGR_AGENT_SHUTDOWN
	CLUST_MGR_CREATE_INDEX_DDL
	CLUST_MGR_BUILD_INDEX_DDL
	CLUST_MGR_BUILD_INDEX_DDL_RESPONSE
	CLUST_MGR_DROP_INDEX_DDL
	CLUST_MGR_UPDATE_TOPOLOGY_FOR_INDEX
	CLUST_MGR_RESET_INDEX_ON_UPGRADE
	CLUST_MGR_RESET_INDEX_ON_ROLLBACK
	CLUST_MGR_GET_GLOBAL_TOPOLOGY
	CLUST_MGR_GET_LOCAL
	CLUST_MGR_SET_LOCAL
	CLUST_MGR_DEL_LOCAL
	CLUST_MGR_DEL_KEYSPACE
	CLUST_MGR_INDEXER_READY
	CLUST_MGR_REBALANCE_RUNNING
	CLUST_MGR_CLEANUP_INDEX
	CLUST_MGR_CLEANUP_PARTITION
	CLUST_MGR_MERGE_PARTITION
	CLUST_MGR_PRUNE_PARTITION
	CLUST_MGR_RECOVER_INDEX
	CLUST_MGR_BUILD_RECOVERED_INDEXES

	//CBQ_BRIDGE_SHUTDOWN
	CBQ_BRIDGE_SHUTDOWN
	CBQ_CREATE_INDEX_DDL
	CBQ_DROP_INDEX_DDL

	//INDEXER
	INDEXER_INIT_PREP_RECOVERY
	INDEXER_PREPARE_RECOVERY
	INDEXER_PREPARE_DONE
	INDEXER_INITIATE_RECOVERY
	INDEXER_RECOVERY_DONE
	INDEXER_KEYSPACE_NOT_FOUND
	INDEXER_ROLLBACK
	STORAGE_ROLLBACK_DONE
	STREAM_REQUEST_DONE
	INDEXER_PAUSE
	INDEXER_RESUME
	INDEXER_PREPARE_UNPAUSE
	INDEXER_UNPAUSE
	INDEXER_BOOTSTRAP
	INDEXER_SET_LOCAL_META
	INDEXER_GET_LOCAL_META
	INDEXER_DEL_LOCAL_META
	INDEXER_CHECK_DDL_IN_PROGRESS
	INDEXER_UPDATE_RSTATE
	INDEXER_MERGE_PARTITION
	INDEXER_CANCEL_MERGE_PARTITION
	INDEXER_MTR_FAIL
	INDEXER_ABORT_RECOVERY
	INDEXER_STORAGE_WARMUP_DONE
	INDEXER_SECURITY_CHANGE
	INDEXER_RESET_INDEX_DONE
	INDEXER_ACTIVE
	INDEXER_INST_RECOVERY_RESPONSE

	//SCAN COORDINATOR
	SCAN_COORD_SHUTDOWN

	COMPACTION_MGR_SHUTDOWN

	//COMMON
	UPDATE_NUMVBUCKETS
	UPDATE_INDEX_INSTANCE_MAP
	UPDATE_INDEX_PARTITION_MAP
	UPDATE_KEYSPACE_STATS_MAP
	UPDATE_MAP_WORKER
	ADD_INDEX_INSTANCE
	UPDATE_SHARDID_MAP

	OPEN_STREAM
	ADD_INDEX_LIST_TO_STREAM
	REMOVE_INDEX_LIST_FROM_STREAM
	REMOVE_KEYSPACE_FROM_STREAM // stop sending mutations for anything in keyspace
	CLOSE_STREAM
	CLEANUP_STREAM
	CLEANUP_PRJ_STATS
	INDEXER_UPDATE_BUILD_TS
	RESET_STREAM

	CONFIG_SETTINGS_UPDATE

	STORAGE_STATS
	SCAN_STATS
	INDEX_PROGRESS_STATS
	INDEXER_STATS
	INDEX_STATS_DONE
	INDEX_STATS_BROADCAST
	INDEX_BOOTSTRAP_STATS_UPDATE

	STATS_RESET
	STATS_PERSISTER_START
	STATS_PERSISTER_STOP
	STATS_PERSISTER_FORCE_PERSIST
	STATS_PERSISTER_CONFIG_UPDATE
	STATS_READ_PERSISTED_STATS
	STATS_LOG_AT_EXIT

	REPAIR_ABORT

	POOL_CHANGE

	INDEXER_DDL_IN_PROGRESS_RESPONSE
	INDEXER_DROP_COLLECTION

	START_SHARD_TRANSFER
	SHARD_TRANSFER_RESPONSE
	SHARD_TRANSFER_CLEANUP
	START_SHARD_RESTORE
	DESTROY_LOCAL_SHARD
)

type Message interface {
	GetMsgType() MsgType
}

// Generic Message
type MsgGeneral struct {
	mType MsgType
}

func (m *MsgGeneral) GetMsgType() MsgType {
	return m.mType
}

// Error Message
type MsgError struct {
	err       Error
	sessionId uint64
}

func (m *MsgError) GetMsgType() MsgType {
	return MSG_ERROR
}

func (m *MsgError) GetError() Error {
	return m.err
}

func (m *MsgError) GetSessionId() uint64 {
	return m.sessionId
}

func (m *MsgError) String() string {
	return fmt.Sprintf("%v", m.err)
}

// Success Message
type MsgSuccess struct {
}

func (m *MsgSuccess) GetMsgType() MsgType {
	return MSG_SUCCESS
}

// Success Message
type MsgSuccessOpenStream struct {
	activeTs  *common.TsVbuuid
	pendingTs *common.TsVbuuid
}

func (m *MsgSuccessOpenStream) GetMsgType() MsgType {
	return MSG_SUCCESS_OPEN_STREAM
}

func (m *MsgSuccessOpenStream) GetActiveTs() *common.TsVbuuid {
	return m.activeTs
}

func (m *MsgSuccessOpenStream) GetPendingTs() *common.TsVbuuid {
	return m.pendingTs
}

// Success Message
type MsgSuccessDrop struct {
	streamId   common.StreamId
	keyspaceId string
}

func (m *MsgSuccessDrop) GetMsgType() MsgType {
	return MSG_SUCCESS_DROP
}

func (m *MsgSuccessDrop) GetStreamId() common.StreamId {
	return m.streamId
}

func (m *MsgSuccessDrop) GetKeyspaceId() string {
	return m.keyspaceId
}

//MUT_MGR_STREAM_CLOSE
type MsgSuccessMutMgr struct {
	mType MsgType
}

func (m *MsgSuccessMutMgr) GetMsgType() MsgType {
	return m.mType
}

// Timestamp Message
type MsgTimestamp struct {
	mType MsgType
	ts    Timestamp
}

func (m *MsgTimestamp) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgTimestamp) GetTimestamp() Timestamp {
	return m.ts
}

// Stream Reader Message
type MsgStream struct {
	mType        MsgType
	streamId     common.StreamId
	node         []byte
	meta         *MutationMeta
	snapshot     *MutationSnapshot
	status       common.StreamStatus
	errCode      byte
	eventType    byte
	manifestuid  string
	scopeId      string
	collectionId string
}

func (m *MsgStream) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgStream) GetMutationMeta() *MutationMeta {
	return m.meta
}

func (m *MsgStream) GetStreamId() common.StreamId {
	return m.streamId
}

func (m *MsgStream) GetNode() []byte {
	return m.node
}

func (m *MsgStream) GetSnapshot() *MutationSnapshot {
	return m.snapshot
}

func (m *MsgStream) GetStatus() common.StreamStatus {
	return m.status
}

func (m *MsgStream) GetErrorCode() byte {
	return m.errCode
}

func (m *MsgStream) GetEventType() byte {
	return m.eventType
}

func (m *MsgStream) GetManifestUID() string {
	return m.manifestuid
}

func (m *MsgStream) GetScopeId() string {
	return m.scopeId
}

func (m *MsgStream) GetCollectionId() string {
	return m.collectionId
}

func (m *MsgStream) String() string {

	str := "\n\tMessage: MsgStream"
	str += fmt.Sprintf("\n\tType: %v", m.mType)
	str += fmt.Sprintf("\n\tStreamId: %v", m.streamId)
	str += fmt.Sprintf("\n\tMeta: %v", m.meta)
	str += fmt.Sprintf("\n\tSnapshot: %v", m.snapshot)
	str += fmt.Sprintf("\n\tStatus: %v", m.status)
	str += fmt.Sprintf("\n\tErrorCode: %v", m.errCode)
	return str

}

// Stream Error Message
type MsgStreamError struct {
	streamId common.StreamId
	err      Error
}

func (m *MsgStreamError) GetMsgType() MsgType {
	return STREAM_READER_ERROR
}

func (m *MsgStreamError) GetStreamId() common.StreamId {
	return m.streamId
}

func (m *MsgStreamError) GetError() Error {
	return m.err
}

// STREAM_READER_CONN_ERROR
// STREAM_REQUEST_DONE
type MsgStreamInfo struct {
	mType      MsgType
	streamId   common.StreamId
	keyspaceId string
	vbList     []Vbucket
	buildTs    Timestamp
	activeTs   *common.TsVbuuid
	pendingTs  *common.TsVbuuid
	reqCh      StopChannel
	sessionId  uint64
}

func (m *MsgStreamInfo) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgStreamInfo) GetStreamId() common.StreamId {
	return m.streamId
}

func (m *MsgStreamInfo) GetKeyspaceId() string {
	return m.keyspaceId
}

func (m *MsgStreamInfo) GetVbList() []Vbucket {
	return m.vbList
}

func (m *MsgStreamInfo) GetBuildTs() Timestamp {
	return m.buildTs
}

func (m *MsgStreamInfo) GetActiveTs() *common.TsVbuuid {
	return m.activeTs
}

func (m *MsgStreamInfo) GetPendingTs() *common.TsVbuuid {
	return m.pendingTs
}

func (m *MsgStreamInfo) GetRequestCh() StopChannel {
	return m.reqCh
}

func (m *MsgStreamInfo) GetSessionId() uint64 {
	return m.sessionId
}

func (m *MsgStreamInfo) String() string {

	str := "\n\tMessage: MsgStreamInfo"
	str += fmt.Sprintf("\n\tType: %v", m.mType)
	str += fmt.Sprintf("\n\tStream: %v", m.streamId)
	str += fmt.Sprintf("\n\tKeyspaceId: %v", m.keyspaceId)
	str += fmt.Sprintf("\n\tSessionId: %v", m.sessionId)
	str += fmt.Sprintf("\n\tVbList: %v", m.vbList)
	return str
}

// STREAM_READER_UPDATE_QUEUE_MAP
type MsgUpdateKeyspaceIdQueue struct {
	keyspaceIdQueueMap  KeyspaceIdQueueMap
	stats               *IndexerStats
	keyspaceIdFilter    map[string]*common.TsVbuuid
	keyspaceIdSessionId KeyspaceIdSessionId
	keyspaceIdEnableOSO KeyspaceIdEnableOSO
}

func (m *MsgUpdateKeyspaceIdQueue) GetMsgType() MsgType {
	return STREAM_READER_UPDATE_QUEUE_MAP
}

func (m *MsgUpdateKeyspaceIdQueue) GetKeyspaceIdQueueMap() KeyspaceIdQueueMap {
	return m.keyspaceIdQueueMap
}

func (m *MsgUpdateKeyspaceIdQueue) GetStatsObject() *IndexerStats {
	return m.stats
}

func (m *MsgUpdateKeyspaceIdQueue) GetKeyspaceIdFilter() map[string]*common.TsVbuuid {
	return m.keyspaceIdFilter
}

func (m *MsgUpdateKeyspaceIdQueue) GetKeyspaceIdSessionId() KeyspaceIdSessionId {
	return m.keyspaceIdSessionId
}

func (m *MsgUpdateKeyspaceIdQueue) GetKeyspaceIdEnableOSO() KeyspaceIdEnableOSO {
	return m.keyspaceIdEnableOSO
}

func (m *MsgUpdateKeyspaceIdQueue) String() string {

	str := "\n\tMessage: MsgUpdateKeyspaceIdQueue"
	str += fmt.Sprintf("\n\tKeyspaceIdQueueMap: %v", m.keyspaceIdQueueMap)
	return str

}

// OPEN_STREAM
// ADD_INDEX_LIST_TO_STREAM
// REMOVE_KEYSPACE_FROM_STREAM
// REMOVE_INDEX_LIST_FROM_STREAM
// CLOSE_STREAM
// CLEANUP_STREAM
// CLEANUP_PRJ_STATS
// INDEXER_UPDATE_BUILD_TS
// RESET_STREAM
type MsgStreamUpdate struct {
	mType        MsgType
	streamId     common.StreamId
	indexList    []common.IndexInst
	buildTs      Timestamp
	respCh       MsgChannel
	stopCh       StopChannel
	keyspaceId   string
	restartTs    *common.TsVbuuid
	rollbackTime int64
	async        bool
	sessionId    uint64
	collectionId string
	mergeTs      *common.TsVbuuid
	numVBuckets  int

	allowMarkFirstSnap bool
	keyspaceInRecovery bool
	abortRecovery      bool
	collectionAware    bool
	enableOSO          bool
	ignoreOSOException bool

	timeBarrier time.Time
}

func (m *MsgStreamUpdate) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgStreamUpdate) GetStreamId() common.StreamId {
	return m.streamId
}

func (m *MsgStreamUpdate) GetIndexList() []common.IndexInst {
	return m.indexList
}

func (m *MsgStreamUpdate) GetTimestamp() Timestamp {
	return m.buildTs
}

func (m *MsgStreamUpdate) GetResponseChannel() MsgChannel {
	return m.respCh
}

func (m *MsgStreamUpdate) GetStopChannel() StopChannel {
	return m.stopCh
}

func (m *MsgStreamUpdate) GetKeyspaceId() string {
	return m.keyspaceId
}

func (m *MsgStreamUpdate) GetRestartTs() *common.TsVbuuid {
	return m.restartTs
}

func (m *MsgStreamUpdate) GetRollbackTime() int64 {
	return m.rollbackTime
}

func (m *MsgStreamUpdate) AllowMarkFirstSnap() bool {
	return m.allowMarkFirstSnap
}

func (m *MsgStreamUpdate) KeyspaceInRecovery() bool {
	return m.keyspaceInRecovery
}

func (m *MsgStreamUpdate) GetAsync() bool {
	return m.async
}

func (m *MsgStreamUpdate) GetSessionId() uint64 {
	return m.sessionId
}

func (m *MsgStreamUpdate) AbortRecovery() bool {
	return m.abortRecovery
}

func (m *MsgStreamUpdate) GetCollectionId() string {
	return m.collectionId
}

func (m *MsgStreamUpdate) CollectionAware() bool {
	return m.collectionAware
}

func (m *MsgStreamUpdate) EnableOSO() bool {
	return m.enableOSO
}

func (m *MsgStreamUpdate) IgnoreOSOException() bool {
	return m.ignoreOSOException
}

func (m *MsgStreamUpdate) GetMergeTs() *common.TsVbuuid {
	return m.mergeTs
}

func (m *MsgStreamUpdate) GetNumVBuckets() int {
	return m.numVBuckets
}

func (m *MsgStreamUpdate) GetTimeBarrier() time.Time {
	return m.timeBarrier
}

func (m *MsgStreamUpdate) String() string {

	str := "\n\tMessage: MsgStreamUpdate"
	str += fmt.Sprintf("\n\tType: %v", m.mType)
	str += fmt.Sprintf("\n\tStream: %v", m.streamId)
	str += fmt.Sprintf("\n\tKeyspaceId: %v", m.keyspaceId)
	str += fmt.Sprintf("\n\tBuildTS: %v", m.buildTs)
	str += fmt.Sprintf("\n\tIndexList: %v", m.indexList)
	str += fmt.Sprintf("\n\tAsync: %v", m.async)
	str += fmt.Sprintf("\n\tSessionId: %v", m.sessionId)
	str += fmt.Sprintf("\n\tCollectionId: %v", m.collectionId)
	str += fmt.Sprintf("\n\tCollectionAware: %v", m.collectionAware)
	str += fmt.Sprintf("\n\tEnableOSO: %v", m.enableOSO)
	str += fmt.Sprintf("\n\tRestartTs: %v", m.restartTs)
	if m.numVBuckets != 0 {
		str += fmt.Sprintf("\n\tNumVBuckets: %v", m.numVBuckets)
	}
	return str

}

// MUT_MGR_PERSIST_MUTATION_QUEUE
// MUT_MGR_ABORT_PERSIST
// MUT_MGR_DRAIN_MUTATION_QUEUE
type MsgMutMgrFlushMutationQueue struct {
	mType      MsgType
	keyspaceId string
	streamId   common.StreamId
	ts         *common.TsVbuuid
	changeVec  []bool
	hasAllSB   bool
	countVec   []uint64
}

func (m *MsgMutMgrFlushMutationQueue) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgMutMgrFlushMutationQueue) GetKeyspaceId() string {
	return m.keyspaceId
}

func (m *MsgMutMgrFlushMutationQueue) GetStreamId() common.StreamId {
	return m.streamId
}

func (m *MsgMutMgrFlushMutationQueue) GetTimestamp() *common.TsVbuuid {
	return m.ts
}

func (m *MsgMutMgrFlushMutationQueue) GetChangeVector() []bool {
	return m.changeVec
}

func (m *MsgMutMgrFlushMutationQueue) HasAllSB() bool {
	return m.hasAllSB
}

func (m *MsgMutMgrFlushMutationQueue) GetCountVector() []uint64 {
	return m.countVec
}

func (m *MsgMutMgrFlushMutationQueue) String() string {

	str := "\n\tMessage: MsgMutMgrFlushMutationQueue"
	str += fmt.Sprintf("\n\tType: %v", m.mType)
	str += fmt.Sprintf("\n\tKeyspaceId: %v", m.keyspaceId)
	str += fmt.Sprintf("\n\tStream: %v", m.streamId)
	return str

}

// MUT_MGR_GET_MUTATION_QUEUE_HWT
// MUT_MGR_GET_MUTATION_QUEUE_LWT
type MsgMutMgrGetTimestamp struct {
	mType      MsgType
	keyspaceId string
	streamId   common.StreamId
}

func (m *MsgMutMgrGetTimestamp) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgMutMgrGetTimestamp) GetKeyspaceId() string {
	return m.keyspaceId
}

func (m *MsgMutMgrGetTimestamp) GetStreamId() common.StreamId {
	return m.streamId
}

// UPDATE_INDEX_INSTANCE_MAP
type MsgUpdateInstMap struct {
	indexInstMap   common.IndexInstMap
	stats          *IndexerStats
	rollbackTimes  map[string]int64
	updatedInsts   common.IndexInstList
	deletedInstIds []common.IndexInstId // used only for logging
}

func (m *MsgUpdateInstMap) GetMsgType() MsgType {
	return UPDATE_INDEX_INSTANCE_MAP
}

func (m *MsgUpdateInstMap) GetIndexInstMap() common.IndexInstMap {
	return m.indexInstMap
}

func (m *MsgUpdateInstMap) GetStatsObject() *IndexerStats {
	return m.stats
}

func (m *MsgUpdateInstMap) GetRollbackTimes() map[string]int64 {
	return m.rollbackTimes
}

func (m *MsgUpdateInstMap) GetUpdatedInsts() common.IndexInstList {
	return m.updatedInsts
}

func (m *MsgUpdateInstMap) AppendUpdatedInsts(insts common.IndexInstList) {
	m.updatedInsts = append(m.updatedInsts, insts...)
}

func (m *MsgUpdateInstMap) GetDeletedInstIds() []common.IndexInstId {
	return m.deletedInstIds
}

func (m *MsgUpdateInstMap) AppendDeletedInstIds(instIds []common.IndexInstId) {
	m.deletedInstIds = append(m.deletedInstIds, instIds...)
}

func (m *MsgUpdateInstMap) String() string {

	str := "\n\tMessage: MsgUpdateInstMap"
	str += fmt.Sprintf("%v", m.indexInstMap)
	return str
}

// UPDATE_INDEX_PARTITION_MAP
type MsgUpdatePartnMap struct {
	indexPartnMap   IndexPartnMap
	updatedPartnMap PartitionInstMap
	deletedInstIds  []common.IndexInstId // used only for logging
}

func (m *MsgUpdatePartnMap) GetMsgType() MsgType {
	return UPDATE_INDEX_PARTITION_MAP
}

func (m *MsgUpdatePartnMap) GetIndexPartnMap() IndexPartnMap {
	return m.indexPartnMap
}

func (m *MsgUpdatePartnMap) GetUpdatedPartnMap() PartitionInstMap {
	return m.updatedPartnMap
}

func (m *MsgUpdatePartnMap) SetUpdatedPartnMap(partnMap PartitionInstMap) {
	m.updatedPartnMap = partnMap
}

func (m *MsgUpdatePartnMap) GetDeletedInstIds() []common.IndexInstId {
	return m.deletedInstIds
}

func (m *MsgUpdatePartnMap) AppendDeletedInstIds(instIds []common.IndexInstId) {
	m.deletedInstIds = append(m.deletedInstIds, instIds...)
}

func (m *MsgUpdatePartnMap) String() string {

	str := "\n\tMessage: MsgUpdatePartnMap"
	str += fmt.Sprintf("%v", m.indexPartnMap)
	return str
}

// UPDATE_KEYSPACE_STATS_MAP
type MsgUpdateKeyspaceStatsMap struct {
	keyspaceStatsMap KeyspaceStatsMap
}

func (m *MsgUpdateKeyspaceStatsMap) GetMsgType() MsgType {
	return UPDATE_KEYSPACE_STATS_MAP
}

func (m *MsgUpdateKeyspaceStatsMap) GetStatsObject() KeyspaceStatsMap {
	return m.keyspaceStatsMap
}

func (m *MsgUpdateKeyspaceStatsMap) String() string {

	str := "\n\tMessage: MsgUpdateKeyspaceStatsMap"
	str += fmt.Sprintf("%v", m.keyspaceStatsMap)
	return str
}

// UPDATE_MAP_WORKER
type MsgUpdateWorker struct {
	workerCh      MsgChannel
	workerStr     string
	indexInstMap  common.IndexInstMap
	indexPartnMap IndexPartnMap
	respCh        chan error
}

func (m *MsgUpdateWorker) GetMsgType() MsgType {
	return UPDATE_MAP_WORKER
}

func (m *MsgUpdateWorker) GetWorkerCh() MsgChannel {
	return m.workerCh
}

func (m *MsgUpdateWorker) GetWorkerStr() string {
	return m.workerStr
}

func (m *MsgUpdateWorker) GetIndexInstMap() common.IndexInstMap {
	return m.indexInstMap
}

func (m *MsgUpdateWorker) GetIndexPartnMap() IndexPartnMap {
	return m.indexPartnMap
}

func (m *MsgUpdateWorker) GetRespCh() chan error {
	return m.respCh
}

// ADD_INDEX_INSTANCE
type MsgAddIndexInst struct {
	workerCh   MsgChannel
	workerStr  string
	indexInst  common.IndexInst
	instPartns PartitionInstMap
	stats      *IndexStats
	respCh     chan error
}

func (m *MsgAddIndexInst) GetMsgType() MsgType {
	return ADD_INDEX_INSTANCE
}

func (m *MsgAddIndexInst) GetWorkerCh() MsgChannel {
	return m.workerCh
}

func (m *MsgAddIndexInst) GetWorkerStr() string {
	return m.workerStr
}

func (m *MsgAddIndexInst) GetIndexInst() common.IndexInst {
	return m.indexInst
}

func (m *MsgAddIndexInst) GetIndexInstStats() *IndexStats {
	return m.stats
}

func (m *MsgAddIndexInst) GetInstPartnMap() PartitionInstMap {
	return m.instPartns
}

func (m *MsgAddIndexInst) GetRespCh() chan error {
	return m.respCh
}

// MUT_MGR_FLUSH_DONE
// MUT_MGR_ABORT_DONE
// STORAGE_SNAP_DONE
type MsgMutMgrFlushDone struct {
	mType      MsgType
	ts         *common.TsVbuuid
	streamId   common.StreamId
	keyspaceId string
	aborted    bool
	hasAllSB   bool
}

func (m *MsgMutMgrFlushDone) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgMutMgrFlushDone) GetTS() *common.TsVbuuid {
	return m.ts
}

func (m *MsgMutMgrFlushDone) GetStreamId() common.StreamId {
	return m.streamId
}

func (m *MsgMutMgrFlushDone) GetKeyspaceId() string {
	return m.keyspaceId
}

func (m *MsgMutMgrFlushDone) GetAborted() bool {
	return m.aborted
}

func (m *MsgMutMgrFlushDone) HasAllSB() bool {
	return m.hasAllSB
}

func (m *MsgMutMgrFlushDone) String() string {

	str := "\n\tMessage: MsgMutMgrFlushDone"
	str += fmt.Sprintf("\n\tStream: %v", m.streamId)
	str += fmt.Sprintf("\n\tKeyspaceId: %v", m.keyspaceId)
	str += fmt.Sprintf("\n\tTS: %v", m.ts)
	str += fmt.Sprintf("\n\tAborted: %v", m.aborted)
	return str

}

// TK_STABILITY_TIMESTAMP
type MsgTKStabilityTS struct {
	ts         *common.TsVbuuid
	streamId   common.StreamId
	keyspaceId string
	changeVec  []bool
	hasAllSB   bool
	countVec   []uint64
}

func (m *MsgTKStabilityTS) GetMsgType() MsgType {
	return TK_STABILITY_TIMESTAMP
}

func (m *MsgTKStabilityTS) GetStreamId() common.StreamId {
	return m.streamId
}

func (m *MsgTKStabilityTS) GetKeyspaceId() string {
	return m.keyspaceId
}

func (m *MsgTKStabilityTS) GetTimestamp() *common.TsVbuuid {
	return m.ts
}

func (m *MsgTKStabilityTS) GetChangeVector() []bool {
	return m.changeVec
}

func (m *MsgTKStabilityTS) HasAllSB() bool {
	return m.hasAllSB
}

func (m *MsgTKStabilityTS) GetCountVector() []uint64 {
	return m.countVec
}

func (m *MsgTKStabilityTS) String() string {

	str := "\n\tMessage: MsgTKStabilityTS"
	str += fmt.Sprintf("\n\tStream: %v", m.streamId)
	str += fmt.Sprintf("\n\tKeyspaceId: %v", m.keyspaceId)
	str += fmt.Sprintf("\n\tTS: %v", m.ts)
	return str

}

// TK_MERGE_STREAM
// TK_MERGE_STREAM_ACK
type MsgTKMergeStream struct {
	mType      MsgType
	streamId   common.StreamId
	keyspaceId string
	mergeTs    *common.TsVbuuid
	mergeList  []common.IndexInst
	reqCh      StopChannel
	sessionId  uint64
}

func (m *MsgTKMergeStream) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgTKMergeStream) GetStreamId() common.StreamId {
	return m.streamId
}

func (m *MsgTKMergeStream) GetKeyspaceId() string {
	return m.keyspaceId
}

func (m *MsgTKMergeStream) GetMergeTS() *common.TsVbuuid {
	return m.mergeTs
}

func (m *MsgTKMergeStream) GetMergeList() []common.IndexInst {
	return m.mergeList
}

func (m *MsgTKMergeStream) GetRequestCh() StopChannel {
	return m.reqCh
}

func (m *MsgTKMergeStream) GetSessionId() uint64 {
	return m.sessionId
}

// TK_ENABLE_FLUSH
// TK_DISABLE_FLUSH
type MsgTKToggleFlush struct {
	mType             MsgType
	streamId          common.StreamId
	keyspaceId        string
	resetPendingMerge bool
}

func (m *MsgTKToggleFlush) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgTKToggleFlush) GetStreamId() common.StreamId {
	return m.streamId
}

func (m *MsgTKToggleFlush) GetKeyspaceId() string {
	return m.keyspaceId
}

func (m *MsgTKToggleFlush) GetResetPendingMerge() bool {
	return m.resetPendingMerge
}

// CBQ_CREATE_INDEX_DDL
// CLUST_MGR_CREATE_INDEX_DDL
type MsgCreateIndex struct {
	mType     MsgType
	indexInst common.IndexInst
	respCh    MsgChannel
	reqCtx    *common.MetadataRequestContext
}

func (m *MsgCreateIndex) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgCreateIndex) GetIndexInst() common.IndexInst {
	return m.indexInst
}

func (m *MsgCreateIndex) GetResponseChannel() MsgChannel {
	return m.respCh
}

func (m *MsgCreateIndex) GetRequestCtx() *common.MetadataRequestContext {
	return m.reqCtx
}

func (m *MsgCreateIndex) GetString() string {

	str := "\n\tMessage: MsgCreateIndex"
	str += fmt.Sprintf("\n\tType: %v", m.mType)
	str += fmt.Sprintf("\n\tIndex: %v", m.indexInst)
	return str
}

type MsgRecoverIndex struct {
	mType     MsgType
	indexInst common.IndexInst
	respCh    MsgChannel
	reqCtx    *common.MetadataRequestContext
}

func (m *MsgRecoverIndex) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgRecoverIndex) GetIndexInst() common.IndexInst {
	return m.indexInst
}

func (m *MsgRecoverIndex) GetResponseChannel() MsgChannel {
	return m.respCh
}

func (m *MsgRecoverIndex) GetRequestCtx() *common.MetadataRequestContext {
	return m.reqCtx
}

func (m *MsgRecoverIndex) GetString() string {

	str := "\n\tMessage: MsgRecoverIndex"
	str += fmt.Sprintf("\n\tType: %v", m.mType)
	str += fmt.Sprintf("\n\tIndex: %v", m.indexInst)
	return str
}

type MsgRecoverIndexResp struct {
	mType           MsgType
	indexInst       common.IndexInst
	partnInstMap    PartitionInstMap
	partnShardIdMap common.PartnShardIdMap
	err             error
}

func (m *MsgRecoverIndexResp) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgRecoverIndexResp) GetIndexInst() common.IndexInst {
	return m.indexInst
}

func (m *MsgRecoverIndexResp) GetPartnInstMap() PartitionInstMap {
	return m.partnInstMap
}

func (m *MsgRecoverIndexResp) GetPartnShardIdMap() common.PartnShardIdMap {
	return m.partnShardIdMap
}

func (m *MsgRecoverIndexResp) GetError() error {
	return m.err
}

func (m *MsgRecoverIndexResp) GetString() string {

	str := "\n\tMessage: MsgRecoverIndex"
	str += fmt.Sprintf("\n\tType: %v", m.mType)
	str += fmt.Sprintf("\n\tIndex: %v", m.indexInst)
	return str
}

// INDEXER_MERGE_PARTITION
type MsgMergePartition struct {
	srcInstId  common.IndexInstId
	tgtInstId  common.IndexInstId
	rebalState common.RebalanceState
	respCh     chan error
}

func (m *MsgMergePartition) GetMsgType() MsgType {
	return INDEXER_MERGE_PARTITION
}

func (m *MsgMergePartition) GetSourceInstId() common.IndexInstId {
	return m.srcInstId
}

func (m *MsgMergePartition) GetTargetInstId() common.IndexInstId {
	return m.tgtInstId
}

func (m *MsgMergePartition) GetRebalanceState() common.RebalanceState {
	return m.rebalState
}

func (m *MsgMergePartition) GetResponseChannel() chan error {
	return m.respCh
}

func (m *MsgMergePartition) GetString() string {

	str := "\n\tMessage: MsgMergePartition"
	str += fmt.Sprintf("\n\tType: %v", INDEXER_MERGE_PARTITION)
	str += fmt.Sprintf("\n\tSurrogate inst Id: %v", m.srcInstId)
	str += fmt.Sprintf("\n\tReal inst Id: %v", m.tgtInstId)
	return str
}

// INDEXER_CANCEL_MERGE_PARTITION
type MsgCancelMergePartition struct {
	indexStateMap map[common.IndexInstId]common.RebalanceState
	respCh        chan error
}

func (m *MsgCancelMergePartition) GetMsgType() MsgType {
	return INDEXER_CANCEL_MERGE_PARTITION
}

func (m *MsgCancelMergePartition) GetIndexStateMap() map[common.IndexInstId]common.RebalanceState {
	return m.indexStateMap
}

func (m *MsgCancelMergePartition) GetResponseChannel() chan error {
	return m.respCh
}

func (m *MsgCancelMergePartition) GetString() string {

	str := "\n\tMessage: MsgCancelMergePartition"
	str += fmt.Sprintf("\n\tType: %v", INDEXER_CANCEL_MERGE_PARTITION)
	return str
}

// CLUST_MGR_CLEANUP_PARTITION
type MsgClustMgrCleanupPartition struct {
	defn             common.IndexDefn
	instId           common.IndexInstId
	partnId          common.PartitionId
	replicaId        int
	updateStatusOnly bool
}

func (m *MsgClustMgrCleanupPartition) GetMsgType() MsgType {
	return CLUST_MGR_CLEANUP_PARTITION
}

func (m *MsgClustMgrCleanupPartition) GetDefn() common.IndexDefn {
	return m.defn
}

func (m *MsgClustMgrCleanupPartition) GetInstId() common.IndexInstId {
	return m.instId
}

func (m *MsgClustMgrCleanupPartition) GetPartitionId() common.PartitionId {
	return m.partnId
}

func (m *MsgClustMgrCleanupPartition) GetReplicaId() int {
	return m.replicaId
}

func (m *MsgClustMgrCleanupPartition) UpdateStatusOnly() bool {
	return m.updateStatusOnly
}

func (m *MsgClustMgrCleanupPartition) GetString() string {
	var b strings.Builder
	b.WriteString("\n\tMessage: MsgCleanupPartition")
	fmt.Fprintf(&b, "\n\tType: %v", CLUST_MGR_CLEANUP_PARTITION)
	fmt.Fprintf(&b, "\n\tIndex defn Id: %v", m.defn.DefnId)
	fmt.Fprintf(&b, "\n\tIndex inst Id: %v", m.instId)
	fmt.Fprintf(&b, "\n\tIndex partition Id: %v", m.partnId)
	fmt.Fprintf(&b, "\n\tIndex replica Id: %v", m.replicaId)
	fmt.Fprintf(&b, "\n\tUpdate Status Only: %v", m.updateStatusOnly)
	return b.String()
}

func (m *MsgClustMgrCleanupPartition) String() string {
	return m.GetString()
}

// CLUST_MGR_MERGE_PARTITION
type MsgClustMgrMergePartition struct {
	defnId         common.IndexDefnId
	srcInstId      common.IndexInstId
	srcRState      common.RebalanceState
	tgtInstId      common.IndexInstId
	tgtPartitions  []common.PartitionId
	tgtVersions    []int
	tgtInstVersion uint64
	respch         chan error
}

func (m *MsgClustMgrMergePartition) GetMsgType() MsgType {
	return CLUST_MGR_MERGE_PARTITION
}

func (m *MsgClustMgrMergePartition) GetDefnId() common.IndexDefnId {
	return m.defnId
}

func (m *MsgClustMgrMergePartition) GetSrcInstId() common.IndexInstId {
	return m.srcInstId
}

func (m *MsgClustMgrMergePartition) GetSrcRState() common.RebalanceState {
	return m.srcRState
}

func (m *MsgClustMgrMergePartition) GetTgtInstId() common.IndexInstId {
	return m.tgtInstId
}

func (m *MsgClustMgrMergePartition) GetTgtPartitions() []common.PartitionId {
	return m.tgtPartitions
}

func (m *MsgClustMgrMergePartition) GetTgtVersions() []int {
	return m.tgtVersions
}

func (m *MsgClustMgrMergePartition) GetTgtInstVersion() uint64 {
	return m.tgtInstVersion
}

func (m *MsgClustMgrMergePartition) GetRespch() chan error {
	return m.respch
}

func (m *MsgClustMgrMergePartition) GetString() string {

	str := "\n\tMessage: MsgMergePartition"
	str += fmt.Sprintf("\n\tType: %v", CLUST_MGR_MERGE_PARTITION)
	str += fmt.Sprintf("\n\tIndex defn Id: %v", m.defnId)
	str += fmt.Sprintf("\n\tIndex src inst Id: %v", m.srcInstId)
	str += fmt.Sprintf("\n\tIndex src rebal state: %v", m.srcRState)
	str += fmt.Sprintf("\n\tIndex tgt inst Id: %v", m.tgtInstId)
	str += fmt.Sprintf("\n\tIndex tgt partitions: %v", m.tgtPartitions)
	str += fmt.Sprintf("\n\tIndex tgt versions: %v", m.tgtVersions)
	str += fmt.Sprintf("\n\tIndex tgt version: %v", m.tgtInstVersion)
	return str
}

// CLUST_MGR_PRUNE_PARTITION
type MsgClustMgrPrunePartition struct {
	instId     common.IndexInstId
	partitions []common.PartitionId
	respCh     MsgChannel
}

func (m *MsgClustMgrPrunePartition) GetMsgType() MsgType {
	return CLUST_MGR_PRUNE_PARTITION
}

func (m *MsgClustMgrPrunePartition) GetInstId() common.IndexInstId {
	return m.instId
}

func (m *MsgClustMgrPrunePartition) GetPartitions() []common.PartitionId {
	return m.partitions
}

func (m *MsgClustMgrPrunePartition) GetRespCh() MsgChannel {
	return m.respCh
}

func (m *MsgClustMgrPrunePartition) GetString() string {

	str := "\n\tMessage: MsgClustMgrPrunePartition"
	str += fmt.Sprintf("\n\tType: %v", CLUST_MGR_PRUNE_PARTITION)
	str += fmt.Sprintf("\n\tinst Id: %v", m.instId)
	str += fmt.Sprintf("\n\tpartitions: %v", m.partitions)
	return str
}

// INDEXER_CANCEL_MERGE_PARTITION
// CLUST_MGR_BUILD_INDEX_DDL
// CLUST_MGR_BUILD_RECOVERED_INDEXES
type MsgBuildIndex struct {
	mType         MsgType
	indexInstList []common.IndexInstId
	bucketList    []string
	respCh        MsgChannel
	reqCtx        *common.MetadataRequestContext
}

func (m *MsgBuildIndex) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgBuildIndex) GetIndexList() []common.IndexInstId {
	return m.indexInstList
}

func (m *MsgBuildIndex) GetBucketList() []string {
	return m.bucketList
}

func (m *MsgBuildIndex) GetRespCh() MsgChannel {
	return m.respCh
}

func (m *MsgBuildIndex) GetRequestCtx() *common.MetadataRequestContext {
	return m.reqCtx
}

func (m *MsgBuildIndex) GetString() string {

	str := "\n\tMessage: MsgBuildIndex"
	str += fmt.Sprintf("\n\tType: %v", CLUST_MGR_BUILD_INDEX_DDL)
	str += fmt.Sprintf("\n\tIndex: %v", m.indexInstList)
	return str
}

// CLUST_MGR_BUILD_INDEX_DDL_RESPONSE
type MsgBuildIndexResponse struct {
	errMap map[common.IndexInstId]error
}

func (m *MsgBuildIndexResponse) GetMsgType() MsgType {
	return CLUST_MGR_BUILD_INDEX_DDL_RESPONSE
}

func (m *MsgBuildIndexResponse) GetErrorMap() map[common.IndexInstId]error {
	return m.errMap
}

func (m *MsgBuildIndexResponse) GetString() string {

	str := "\n\tMessage: MsgBuildIndexResponse"
	str += fmt.Sprintf("\n\tType: %v", CLUST_MGR_BUILD_INDEX_DDL_RESPONSE)
	str += fmt.Sprintf("\n\terrMap: %v", m.errMap)
	return str
}

// CBQ_DROP_INDEX_DDL
// CLUST_MGR_DROP_INDEX_DDL
type MsgDropIndex struct {
	mType       MsgType
	indexInstId common.IndexInstId
	keyspaceId  string
	respCh      MsgChannel
	reqCtx      *common.MetadataRequestContext
}

func (m *MsgDropIndex) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgDropIndex) GetIndexInstId() common.IndexInstId {
	return m.indexInstId
}

func (m *MsgDropIndex) GetKeyspaceId() string {
	return m.keyspaceId
}

func (m *MsgDropIndex) GetResponseChannel() MsgChannel {
	return m.respCh
}

func (m *MsgDropIndex) GetRequestCtx() *common.MetadataRequestContext {
	return m.reqCtx
}

func (m *MsgDropIndex) GetString() string {

	str := "\n\tMessage: MsgDropIndex"
	str += fmt.Sprintf("\n\tType: %v", m.mType)
	str += fmt.Sprintf("\n\tIndex: %v", m.indexInstId)
	return str
}

// TK_GET_KEYSPACE_HWT
// STREAM_READER_HWT
type MsgKeyspaceHWT struct {
	mType      MsgType
	streamId   common.StreamId
	keyspaceId string
	hwt        *common.TsVbuuid
	hwtOSO     *common.TsVbuuid
	prevSnap   *common.TsVbuuid
	sessionId  uint64
}

func (m *MsgKeyspaceHWT) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgKeyspaceHWT) GetStreamId() common.StreamId {
	return m.streamId
}

func (m *MsgKeyspaceHWT) GetKeyspaceId() string {
	return m.keyspaceId
}

func (m *MsgKeyspaceHWT) GetHWT() *common.TsVbuuid {
	return m.hwt
}

func (m *MsgKeyspaceHWT) GetHWTOSO() *common.TsVbuuid {
	return m.hwtOSO
}

func (m *MsgKeyspaceHWT) GetPrevSnap() *common.TsVbuuid {
	return m.prevSnap
}

func (m *MsgKeyspaceHWT) GetSessionId() uint64 {
	return m.sessionId
}

func (m *MsgKeyspaceHWT) String() string {

	str := "\n\tMessage: MsgKeyspaceHWT "
	str += fmt.Sprintf("StreamId: %v ", m.streamId)
	str += fmt.Sprintf("KeyspaceId: %v ", m.keyspaceId)
	str += fmt.Sprintf("SessionId: %v ", m.sessionId)
	return str

}

// KV_SENDER_RESTART_VBUCKETS
type MsgRestartVbuckets struct {
	streamId     common.StreamId
	keyspaceId   string
	restartTs    *common.TsVbuuid
	connErrVbs   []Vbucket
	repairVbs    []Vbucket
	respCh       MsgChannel
	stopCh       StopChannel
	sessionId    uint64
	collectionId string
}

func (m *MsgRestartVbuckets) GetMsgType() MsgType {
	return KV_SENDER_RESTART_VBUCKETS
}

func (m *MsgRestartVbuckets) GetStreamId() common.StreamId {
	return m.streamId
}

func (m *MsgRestartVbuckets) GetKeyspaceId() string {
	return m.keyspaceId
}

func (m *MsgRestartVbuckets) GetRestartTs() *common.TsVbuuid {
	return m.restartTs
}

func (m *MsgRestartVbuckets) ConnErrVbs() []Vbucket {
	return m.connErrVbs
}

func (m *MsgRestartVbuckets) RepairVbs() []Vbucket {
	return m.repairVbs
}

func (m *MsgRestartVbuckets) GetResponseCh() MsgChannel {
	return m.respCh
}

func (m *MsgRestartVbuckets) GetStopChannel() StopChannel {
	return m.stopCh
}

func (m *MsgRestartVbuckets) GetSessionId() uint64 {
	return m.sessionId
}

func (m *MsgRestartVbuckets) GetCollectionId() string {
	return m.collectionId
}

func (m *MsgRestartVbuckets) String() string {
	str := "\n\tMessage: MsgRestartVbuckets"
	str += fmt.Sprintf("\n\tStreamId: %v", m.streamId)
	str += fmt.Sprintf("\n\tKeyspaceId: %v", m.keyspaceId)
	str += fmt.Sprintf("\n\tSessionId: %v", m.sessionId)
	str += fmt.Sprintf("\n\tCollectionId: %v", m.collectionId)
	str += fmt.Sprintf("\n\tRestartTS: %v", m.restartTs)
	return str
}

// KV_SENDER_RESTART_VBUCKETS_RESPONSE
type MsgRestartVbucketsResponse struct {
	streamId   common.StreamId
	keyspaceId string
	activeTs   *common.TsVbuuid
	pendingTs  *common.TsVbuuid
	sessionId  uint64
}

func (m *MsgRestartVbucketsResponse) GetMsgType() MsgType {
	return KV_SENDER_RESTART_VBUCKETS_RESPONSE
}

func (m *MsgRestartVbucketsResponse) GetStreamId() common.StreamId {
	return m.streamId
}

func (m *MsgRestartVbucketsResponse) GetKeyspaceId() string {
	return m.keyspaceId
}

func (m *MsgRestartVbucketsResponse) GetActiveTs() *common.TsVbuuid {
	return m.activeTs
}

func (m *MsgRestartVbucketsResponse) GetPendingTs() *common.TsVbuuid {
	return m.pendingTs
}

func (m *MsgRestartVbucketsResponse) GetSessionId() uint64 {
	return m.sessionId
}

func (m *MsgRestartVbucketsResponse) String() string {
	str := "\n\tMessage: MsgRestartVbucketsResponse"
	str += fmt.Sprintf("\n\tStreamId: %v", m.streamId)
	str += fmt.Sprintf("\n\tKeyspaceId: %v", m.keyspaceId)
	str += fmt.Sprintf("\n\tSessionId: %v", m.sessionId)
	str += fmt.Sprintf("\n\tActiveTS: %v", m.activeTs)
	str += fmt.Sprintf("\n\tPendingTS: %v", m.pendingTs)
	return str
}

// KV_SENDER_REPAIR_ENDPOINTS
type MsgRepairEndpoints struct {
	streamId  common.StreamId
	endpoints []string
}

func (m *MsgRepairEndpoints) GetMsgType() MsgType {
	return KV_SENDER_REPAIR_ENDPOINTS
}

func (m *MsgRepairEndpoints) GetStreamId() common.StreamId {
	return m.streamId
}

func (m *MsgRepairEndpoints) GetEndpoints() []string {
	return m.endpoints
}

func (m *MsgRepairEndpoints) String() string {
	str := "\n\tMessage: MsgRepairEndpoints"
	str += fmt.Sprintf("\n\tStreamId: %v", m.streamId)
	str += fmt.Sprintf("\n\tEndpoints: %v", m.endpoints)
	return str
}

// INDEXER_INIT_PREP_RECOVERY
// INDEXER_PREPARE_RECOVERY
// INDEXER_PREPARE_DONE
// INDEXER_INITIATE_RECOVERY
// INDEXER_RECOVERY_DONE
// INDEXER_KEYSPACE_NOT_FOUND
// INDEXER_MTR_FAIL
// INDEXER_ABORT_RECOVERY
type MsgRecovery struct {
	mType      MsgType
	streamId   common.StreamId
	keyspaceId string
	restartTs  *common.TsVbuuid
	buildTs    Timestamp
	activeTs   *common.TsVbuuid
	inMTR      bool
	retryTs    *common.TsVbuuid
	pendingTs  *common.TsVbuuid
	requestCh  StopChannel
	sessionId  uint64
}

func (m *MsgRecovery) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgRecovery) GetStreamId() common.StreamId {
	return m.streamId
}

func (m *MsgRecovery) GetKeyspaceId() string {
	return m.keyspaceId
}

func (m *MsgRecovery) GetRestartTs() *common.TsVbuuid {
	return m.restartTs
}

func (m *MsgRecovery) GetActiveTs() *common.TsVbuuid {
	return m.activeTs
}

func (m *MsgRecovery) GetPendingTs() *common.TsVbuuid {
	return m.pendingTs
}

func (m *MsgRecovery) GetBuildTs() Timestamp {
	return m.buildTs
}

func (m *MsgRecovery) InMTR() bool {
	return m.inMTR
}

func (m *MsgRecovery) GetRetryTs() *common.TsVbuuid {
	return m.retryTs
}

func (m *MsgRecovery) GetRequestCh() StopChannel {
	return m.requestCh
}

func (m *MsgRecovery) GetSessionId() uint64 {
	return m.sessionId
}

type MsgRollback struct {
	streamId     common.StreamId
	keyspaceId   string
	rollbackTs   *common.TsVbuuid
	rollbackTime int64
	sessionId    uint64
}

func (m *MsgRollback) GetMsgType() MsgType {
	return INDEXER_ROLLBACK
}

func (m *MsgRollback) GetStreamId() common.StreamId {
	return m.streamId
}

func (m *MsgRollback) GetKeyspaceId() string {
	return m.keyspaceId
}

func (m *MsgRollback) GetRollbackTs() *common.TsVbuuid {
	return m.rollbackTs
}

func (m *MsgRollback) GetRollbackTime() int64 {
	return m.rollbackTime
}

func (m *MsgRollback) GetSessionId() uint64 {
	return m.sessionId
}

type MsgRollbackDone struct {
	streamId   common.StreamId
	keyspaceId string
	restartTs  *common.TsVbuuid
	err        error
	sessionId  uint64
}

func (m *MsgRollbackDone) GetMsgType() MsgType {
	return STORAGE_ROLLBACK_DONE
}

func (m *MsgRollbackDone) GetStreamId() common.StreamId {
	return m.streamId
}

func (m *MsgRollbackDone) GetKeyspaceId() string {
	return m.keyspaceId
}

func (m *MsgRollbackDone) GetRestartTs() *common.TsVbuuid {
	return m.restartTs
}

func (m *MsgRollbackDone) GetError() error {
	return m.err
}

func (m *MsgRollbackDone) GetSessionId() uint64 {
	return m.sessionId
}

type MsgRepairAbort struct {
	streamId   common.StreamId
	keyspaceId string
}

func (m *MsgRepairAbort) GetMsgType() MsgType {
	return REPAIR_ABORT
}

func (m *MsgRepairAbort) GetStreamId() common.StreamId {
	return m.streamId
}

func (m *MsgRepairAbort) GetKeyspaceId() string {
	return m.keyspaceId
}

// POOL_CHANGE
type MsgPoolChange struct {
	mType      MsgType
	nodes      map[string]bool
	streamId   common.StreamId
	keyspaceId string
}

func (m *MsgPoolChange) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgPoolChange) GetNodes() map[string]bool {
	return m.nodes
}

func (m *MsgPoolChange) GetStreamId() common.StreamId {
	return m.streamId
}

func (m *MsgPoolChange) GetKeyspaceId() string {
	return m.keyspaceId
}

// TK_INIT_BUILD_DONE
// TK_INIT_BUILD_DONE_ACK
// TK_INIT_BUILD_DONE_NO_CATCHUP_ACK
// TK_ADD_INSTANCE_FAIL
type MsgTKInitBuildDone struct {
	mType      MsgType
	streamId   common.StreamId
	buildTs    Timestamp
	keyspaceId string
	mergeTs    *common.TsVbuuid
	flushTs    *common.TsVbuuid
	sessionId  uint64
}

func (m *MsgTKInitBuildDone) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgTKInitBuildDone) GetKeyspaceId() string {
	return m.keyspaceId
}

func (m *MsgTKInitBuildDone) GetTimestamp() Timestamp {
	return m.buildTs
}

func (m *MsgTKInitBuildDone) GetStreamId() common.StreamId {
	return m.streamId
}

func (m *MsgTKInitBuildDone) GetMergeTs() *common.TsVbuuid {
	return m.mergeTs
}

func (m *MsgTKInitBuildDone) GetFlushTs() *common.TsVbuuid {
	return m.flushTs
}

func (m *MsgTKInitBuildDone) GetSessionId() uint64 {
	return m.sessionId
}

type MsgUpdateNumVbuckets struct {
	bucketNameNumVBucketsMap map[string]int
}

func (m *MsgUpdateNumVbuckets) GetMsgType() MsgType {
	return UPDATE_NUMVBUCKETS
}

func (m *MsgUpdateNumVbuckets) GetBucketNameNumVBucketsMap() map[string]int {
	return m.bucketNameNumVBucketsMap
}

type MsgIndexSnapRequest struct {
	ts          *common.TsVbuuid
	cons        common.Consistency
	idxInstId   common.IndexInstId
	expiredTime time.Time

	// Send error or index snapshot
	respch chan interface{}
}

func (m *MsgIndexSnapRequest) GetMsgType() MsgType {
	return STORAGE_INDEX_SNAP_REQUEST
}

func (m *MsgIndexSnapRequest) GetTS() *common.TsVbuuid {
	return m.ts
}

func (m *MsgIndexSnapRequest) GetConsistency() common.Consistency {
	return m.cons
}

func (m *MsgIndexSnapRequest) GetExpiredTime() time.Time {
	return m.expiredTime
}

func (m *MsgIndexSnapRequest) GetReplyChannel() chan interface{} {
	return m.respch
}

func (m *MsgIndexSnapRequest) GetIndexId() common.IndexInstId {
	return m.idxInstId
}

type MsgIndexMergeSnapshot struct {
	srcInstId  common.IndexInstId
	tgtInstId  common.IndexInstId
	partitions []common.PartitionId
}

func (m *MsgIndexMergeSnapshot) GetMsgType() MsgType {
	return STORAGE_INDEX_MERGE_SNAPSHOT
}

func (m *MsgIndexMergeSnapshot) GetSourceInstId() common.IndexInstId {
	return m.srcInstId
}

func (m *MsgIndexMergeSnapshot) GetTargetInstId() common.IndexInstId {
	return m.tgtInstId
}

func (m *MsgIndexMergeSnapshot) GetPartitions() []common.PartitionId {
	return m.partitions
}

type MsgIndexPruneSnapshot struct {
	instId     common.IndexInstId
	partitions []common.PartitionId
}

func (m *MsgIndexPruneSnapshot) GetMsgType() MsgType {
	return STORAGE_INDEX_PRUNE_SNAPSHOT
}

func (m *MsgIndexPruneSnapshot) GetInstId() common.IndexInstId {
	return m.instId
}

func (m *MsgIndexPruneSnapshot) GetPartitions() []common.PartitionId {
	return m.partitions
}

// STORAGE_UPDATE_SNAP_MAP
type MsgUpdateSnapMap struct {
	idxInstId  common.IndexInstId
	idxInst    common.IndexInst
	partnMap   PartitionInstMap
	streamId   common.StreamId
	keyspaceId string
	respch     chan bool
}

func (m *MsgUpdateSnapMap) GetMsgType() MsgType {
	return STORAGE_UPDATE_SNAP_MAP
}

func (m *MsgUpdateSnapMap) GetInstId() common.IndexInstId {
	return m.idxInstId
}

func (m *MsgUpdateSnapMap) GetInst() common.IndexInst {
	return m.idxInst
}
func (m *MsgUpdateSnapMap) GetPartnMap() PartitionInstMap {
	return m.partnMap
}
func (m *MsgUpdateSnapMap) GetStreamId() common.StreamId {
	return m.streamId
}
func (m *MsgUpdateSnapMap) GetKeyspaceId() string {
	return m.keyspaceId
}
func (m *MsgUpdateSnapMap) GetReplyChannel() chan bool {
	return m.respch
}

type MsgIndexStorageStats struct {
	respch chan []IndexStorageStats
	spec   *statsSpec
}

func (m *MsgIndexStorageStats) GetMsgType() MsgType {
	return STORAGE_INDEX_STORAGE_STATS
}

func (m *MsgIndexStorageStats) GetReplyChannel() chan []IndexStorageStats {
	return m.respch
}

func (m *MsgIndexStorageStats) GetStatsSpec() *statsSpec {
	return m.spec
}

type MsgStatsRequest struct {
	mType    MsgType
	respch   chan bool
	fetchDcp bool
	stats    common.Statistics
}

func (m *MsgStatsRequest) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgStatsRequest) GetReplyChannel() chan bool {
	return m.respch
}

func (m *MsgStatsRequest) FetchDcp() bool {
	return m.fetchDcp
}

func (m *MsgStatsRequest) GetStats() common.Statistics {
	return m.stats
}

type MsgIndexCompact struct {
	instId    common.IndexInstId
	partnId   common.PartitionId
	errch     chan error
	abortTime time.Time
	minFrag   int
}

func (m *MsgIndexCompact) GetMsgType() MsgType {
	return STORAGE_INDEX_COMPACT
}

func (m *MsgIndexCompact) GetInstId() common.IndexInstId {
	return m.instId
}

func (m *MsgIndexCompact) GetPartitionId() common.PartitionId {
	return m.partnId
}

func (m *MsgIndexCompact) GetErrorChannel() chan error {
	return m.errch
}

func (m *MsgIndexCompact) GetAbortTime() time.Time {
	return m.abortTime
}

func (m *MsgIndexCompact) GetMinFrag() int {
	return m.minFrag
}

// KV_STREAM_REPAIR
type MsgKVStreamRepair struct {
	streamId   common.StreamId
	keyspaceId string
	restartTs  *common.TsVbuuid
	async      bool
	sessionId  uint64
}

func (m *MsgKVStreamRepair) GetMsgType() MsgType {
	return KV_STREAM_REPAIR
}

func (m *MsgKVStreamRepair) GetStreamId() common.StreamId {
	return m.streamId
}

func (m *MsgKVStreamRepair) GetKeyspaceId() string {
	return m.keyspaceId
}

func (m *MsgKVStreamRepair) GetRestartTs() *common.TsVbuuid {
	return m.restartTs
}

func (m *MsgKVStreamRepair) GetAsync() bool {
	return m.async
}

func (m *MsgKVStreamRepair) GetSessionId() uint64 {
	return m.sessionId
}

// CLUST_MGR_RESET_INDEX_ON_UPGRADE
type MsgClustMgrResetIndexOnUpgrade struct {
	inst common.IndexInst
}

func (m *MsgClustMgrResetIndexOnUpgrade) GetMsgType() MsgType {
	return CLUST_MGR_RESET_INDEX_ON_UPGRADE
}

func (m *MsgClustMgrResetIndexOnUpgrade) GetIndex() common.IndexInst {
	return m.inst
}

func (m *MsgClustMgrResetIndexOnUpgrade) String() string {
	return fmt.Sprintf("inst : %v", m.inst)
}

// CLUST_MGR_RESET_INDEX_ON_ROLLBACK
type MsgClustMgrResetIndexOnRollback struct {
	inst   common.IndexInst
	respch chan error
}

func (m *MsgClustMgrResetIndexOnRollback) GetMsgType() MsgType {
	return CLUST_MGR_RESET_INDEX_ON_ROLLBACK
}

func (m *MsgClustMgrResetIndexOnRollback) GetIndex() common.IndexInst {
	return m.inst
}

func (m *MsgClustMgrResetIndexOnRollback) GetRespch() chan error {
	return m.respch
}

func (m *MsgClustMgrResetIndexOnRollback) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "inst : %v ", m.inst)
	fmt.Fprintf(&b, "respch : %v", m.respch)
	return b.String()
}

// INDEXER_RESET_INDEX_DONE
type MsgResetIndexDone struct {
	streamId   common.StreamId
	keyspaceId string
	sessionId  uint64
}

func (m *MsgResetIndexDone) GetMsgType() MsgType {
	return INDEXER_RESET_INDEX_DONE
}

func (m *MsgResetIndexDone) GetStreamId() common.StreamId {
	return m.streamId
}

func (m *MsgResetIndexDone) GetKeyspaceId() string {
	return m.keyspaceId
}

func (m *MsgResetIndexDone) GetSessionId() uint64 {
	return m.sessionId
}

// CLUST_MGR_UPDATE_TOPOLOGY_FOR_INDEX
type MsgClustMgrUpdate struct {
	mType         MsgType
	indexList     []common.IndexInst
	updatedFields MetaUpdateFields
	bucket        string
	scope         string
	collection    string
	streamId      common.StreamId
	syncUpdate    bool
	respCh        chan error
}

func (m *MsgClustMgrUpdate) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "mType: %v ", m.mType)
	fmt.Fprintf(&b, "indexList: [")
	for _, i := range m.indexList {
		fmt.Fprintf(&b, "%v\n", i)
	}
	fmt.Fprintf(&b, "] ")
	fmt.Fprintf(&b, "bucket: %v ", m.bucket)
	fmt.Fprintf(&b, "scope: %v ", m.scope)
	fmt.Fprintf(&b, "collection: %v ", m.collection)
	fmt.Fprintf(&b, "streamId: %v ", m.streamId)
	fmt.Fprintf(&b, "syncUpdate: %v ", m.syncUpdate)
	fmt.Fprintf(&b, "respCh: %v ", m.respCh)
	return b.String()
}

func (m *MsgClustMgrUpdate) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgClustMgrUpdate) GetIndexList() []common.IndexInst {
	return m.indexList
}

func (m *MsgClustMgrUpdate) GetUpdatedFields() MetaUpdateFields {
	return m.updatedFields
}

func (m *MsgClustMgrUpdate) GetBucket() string {
	return m.bucket
}

func (m *MsgClustMgrUpdate) GetScope() string {
	return m.scope
}

func (m *MsgClustMgrUpdate) GetCollection() string {
	return m.collection
}

func (m *MsgClustMgrUpdate) GetStreamId() common.StreamId {
	return m.streamId
}

func (m *MsgClustMgrUpdate) GetIsSyncUpdate() bool {
	return m.syncUpdate
}

func (m *MsgClustMgrUpdate) GetRespCh() chan error {
	return m.respCh
}

// CLUST_MGR_GET_GLOBAL_TOPOLOGY
type MsgClustMgrTopology struct {
	indexInstMap common.IndexInstMap
}

func (m *MsgClustMgrTopology) GetMsgType() MsgType {
	return CLUST_MGR_GET_GLOBAL_TOPOLOGY
}

func (m *MsgClustMgrTopology) GetInstMap() common.IndexInstMap {
	return m.indexInstMap
}

func (m *MsgClustMgrTopology) String() string {
	var b strings.Builder
	b.WriteString("indexInstMap : \n")
	for k, v := range m.indexInstMap {
		fmt.Fprintf(&b, "%v:%v\n", k, v)
	}
	return b.String()
}

// CLUST_MGR_GET_LOCAL
// CLUST_MGR_SET_LOCAL
// CLUST_MGR_DEL_LOCAL
type MsgClustMgrLocal struct {
	mType             MsgType
	key               string
	value             string
	err               error
	respch            MsgChannel
	checkDDL          bool
	inProgressIndexes []string
}

func (m *MsgClustMgrLocal) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgClustMgrLocal) GetKey() string {
	return m.key
}

func (m *MsgClustMgrLocal) GetValue() string {
	return m.value
}

func (m *MsgClustMgrLocal) GetError() error {
	return m.err
}

func (m *MsgClustMgrLocal) GetRespCh() MsgChannel {
	return m.respch
}

func (m *MsgClustMgrLocal) GetCheckDDL() bool {
	return m.checkDDL
}

func (m *MsgClustMgrLocal) GetInProgressIndexes() []string {
	return m.inProgressIndexes
}

type MsgConfigUpdate struct {
	cfg common.Config
}

func (m *MsgConfigUpdate) GetMsgType() MsgType {
	return CONFIG_SETTINGS_UPDATE
}

func (m *MsgConfigUpdate) GetConfig() common.Config {
	return m.cfg
}

type MsgResetStats struct {
}

func (m *MsgResetStats) GetMsgType() MsgType {
	return STATS_RESET
}

type MsgSecurityChange struct {
	refreshCert    bool
	refreshEncrypt bool
}

func (m *MsgSecurityChange) GetMsgType() MsgType {
	return INDEXER_SECURITY_CHANGE
}

func (m *MsgSecurityChange) RefreshCert() bool {
	return m.refreshCert
}

func (m *MsgSecurityChange) RefreshEncrypt() bool {
	return m.refreshEncrypt
}

// STATS_PERSISTER_START
// STATS_PERSISTER_STOP
// STATS_PERSISTER_FORCE_PERSIST
// STATS_PERSISTER_CONFIG_UPDATE
// STATS_READ_PERSISTED_STATS
// STATS_LOG_AT_EXIT
type MsgStatsPersister struct {
	mType  MsgType
	stats  *IndexerStats
	respCh chan bool
}

func (m *MsgStatsPersister) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgStatsPersister) GetStats() *IndexerStats {
	return m.stats
}

func (m *MsgStatsPersister) GetResponseChannel() chan bool {
	return m.respCh
}

// INDEXER_PAUSE
// INDEXER_RESUME
// INDEXER_PREPARE_UNPAUSE
// INDEXER_UNPAUSE
// INDEXER_BOOTSTRAP
type MsgIndexerState struct {
	mType         MsgType
	rollbackTimes map[string]int64
}

func (m *MsgIndexerState) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgIndexerState) GetRollbackTimes() map[string]int64 {
	return m.rollbackTimes
}

type MsgCheckDDLInProgress struct {
	respCh MsgChannel
}

func (m *MsgCheckDDLInProgress) GetMsgType() MsgType {
	return INDEXER_CHECK_DDL_IN_PROGRESS
}

func (m *MsgCheckDDLInProgress) GetRespCh() MsgChannel {
	return m.respCh
}

type MsgDDLInProgressResponse struct {
	ddlInProgress        bool
	inProgressIndexNames []string
}

func (m *MsgDDLInProgressResponse) GetMsgType() MsgType {
	return INDEXER_DDL_IN_PROGRESS_RESPONSE
}

func (m *MsgDDLInProgressResponse) GetInProgressIndexNames() []string {
	return m.inProgressIndexNames
}

func (m *MsgDDLInProgressResponse) GetDDLInProgress() bool {
	return m.ddlInProgress
}

type MsgUpdateIndexRState struct {
	instId common.IndexInstId
	respch chan error
	rstate common.RebalanceState
}

func (m *MsgUpdateIndexRState) GetMsgType() MsgType {
	return INDEXER_UPDATE_RSTATE
}

func (m *MsgUpdateIndexRState) GetInstId() common.IndexInstId {
	return m.instId
}

func (m *MsgUpdateIndexRState) GetRespCh() chan error {
	return m.respch
}

func (m *MsgUpdateIndexRState) GetRState() common.RebalanceState {
	return m.rstate
}

type MsgStorageWarmupDone struct {
	err          error
	needsRestart bool
}

func (m *MsgStorageWarmupDone) GetMsgType() MsgType {
	return INDEXER_STORAGE_WARMUP_DONE
}

func (m *MsgStorageWarmupDone) GetError() error {
	return m.err
}

func (m *MsgStorageWarmupDone) NeedsRestart() bool {
	return m.needsRestart
}

type MsgIndexerDropCollection struct {
	streamId     common.StreamId
	keyspaceId   string
	scopeId      string
	collectionId string
}

func (m *MsgIndexerDropCollection) GetMsgType() MsgType {
	return INDEXER_DROP_COLLECTION
}

func (m *MsgIndexerDropCollection) GetStreamId() common.StreamId {
	return m.streamId
}

func (m *MsgIndexerDropCollection) GetKeyspaceId() string {
	return m.keyspaceId
}

func (m *MsgIndexerDropCollection) GetScopeId() string {
	return m.scopeId
}

func (m *MsgIndexerDropCollection) GetCollectionId() string {
	return m.collectionId
}

type MsgStartShardTransfer struct {
	shardIds        []common.ShardId
	rebalanceId     string
	transferTokenId string
	destination     string

	// The rebalance cancelCh shared with indexer to indicate
	// any upstream cancellation of rebalance
	cancelCh chan struct{}

	// If rebalance on this node fails due to any error, doneCh
	// is closed first. In that case, abort the transfer
	doneCh chan struct{}

	progressCh chan *ShardTransferStatistics
	respCh     chan Message
}

func (m *MsgStartShardTransfer) GetMsgType() MsgType {
	return START_SHARD_TRANSFER
}

func (m *MsgStartShardTransfer) GetShardIds() []common.ShardId {
	return m.shardIds
}

func (m *MsgStartShardTransfer) GetRebalanceId() string {
	return m.rebalanceId
}

func (m *MsgStartShardTransfer) GetTransferTokenId() string {
	return m.transferTokenId
}

func (m *MsgStartShardTransfer) GetDestination() string {
	return m.destination
}

func (m *MsgStartShardTransfer) GetCancelCh() chan struct{} {
	return m.cancelCh
}

func (m *MsgStartShardTransfer) GetDoneCh() chan struct{} {
	return m.doneCh
}

func (m *MsgStartShardTransfer) GetProgressCh() chan *ShardTransferStatistics {
	return m.progressCh
}

func (m *MsgStartShardTransfer) GetRespCh() chan Message {
	return m.respCh
}

func (m *MsgStartShardTransfer) String() string {
	var sb strings.Builder
	sbp := &sb

	fmt.Fprintf(sbp, " ShardIds: %v ", m.shardIds)
	fmt.Fprintf(sbp, " RebalanceId: %v ", m.rebalanceId)
	fmt.Fprintf(sbp, " TransferTokenId: %v ", m.transferTokenId)
	fmt.Fprintf(sbp, " Destination: %v ", m.destination)

	return sbp.String()
}

type MsgShardTransferResp struct {
	errMap     map[common.ShardId]error
	shardPaths map[common.ShardId]string //ShardId -> Location where shard is uploaded
}

func (m *MsgShardTransferResp) GetMsgType() MsgType {
	return SHARD_TRANSFER_RESPONSE
}

func (m *MsgShardTransferResp) GetErrorMap() map[common.ShardId]error {
	return m.errMap
}

func (m *MsgShardTransferResp) GetShardPaths() map[common.ShardId]string {
	return m.shardPaths
}

type MsgShardTransferCleanup struct {
	shardPaths      map[common.ShardId]string // shardId -> Location of shard transfer
	destination     string
	rebalanceId     string
	transferTokenId string
	respCh          chan bool
}

func (m *MsgShardTransferCleanup) GetMsgType() MsgType {
	return SHARD_TRANSFER_CLEANUP
}

func (m *MsgShardTransferCleanup) GetShardPaths() map[common.ShardId]string {
	return m.shardPaths
}

func (m *MsgShardTransferCleanup) GetDestination() string {
	return m.destination
}

func (m *MsgShardTransferCleanup) GetRebalanceId() string {
	return m.rebalanceId
}

func (m *MsgShardTransferCleanup) GetTransferTokenId() string {
	return m.transferTokenId
}

func (m *MsgShardTransferCleanup) GetRespCh() chan bool {
	return m.respCh
}

func (m *MsgShardTransferCleanup) String() string {
	var sb strings.Builder
	sbp := &sb

	fmt.Fprintf(sbp, " ShardPaths: %v ", m.shardPaths)
	fmt.Fprintf(sbp, " RebalanceId: %v ", m.rebalanceId)
	fmt.Fprintf(sbp, " TransferTokenId: %v ", m.transferTokenId)

	return sbp.String()
}

type MsgStartShardRestore struct {
	shardPaths      map[common.ShardId]string
	rebalanceId     string
	transferTokenId string
	destination     string

	// The rebalance cancelCh shared with indexer to indicate
	// any upstream cancellation of rebalance
	cancelCh chan struct{}

	// If rebalance on this node fails due to any error, doneCh
	// is closed first. In that case, abort the restore process
	doneCh chan struct{}

	// used by shard rebalancer during replica repair
	instRenameMap map[common.ShardId]map[string]string

	progressCh chan *ShardTransferStatistics
	respCh     chan Message
}

func (m *MsgStartShardRestore) GetMsgType() MsgType {
	return START_SHARD_RESTORE
}

func (m *MsgStartShardRestore) GetShardPaths() map[common.ShardId]string {
	return m.shardPaths
}

func (m *MsgStartShardRestore) GetRebalanceId() string {
	return m.rebalanceId
}

func (m *MsgStartShardRestore) GetTransferTokenId() string {
	return m.transferTokenId
}

func (m *MsgStartShardRestore) GetDestination() string {
	return m.destination
}

func (m *MsgStartShardRestore) GetInstRenameMap() map[common.ShardId]map[string]string {
	return m.instRenameMap
}

func (m *MsgStartShardRestore) GetCancelCh() chan struct{} {
	return m.cancelCh
}

func (m *MsgStartShardRestore) GetDoneCh() chan struct{} {
	return m.doneCh
}

func (m *MsgStartShardRestore) GetProgressCh() chan *ShardTransferStatistics {
	return m.progressCh
}

func (m *MsgStartShardRestore) GetRespCh() chan Message {
	return m.respCh
}

type MsgDestroyLocalShardData struct {
	shardIds []common.ShardId // shardId -> Location of shard transfer
	respCh   chan bool
}

func (m *MsgDestroyLocalShardData) GetMsgType() MsgType {
	return DESTROY_LOCAL_SHARD
}

func (m *MsgDestroyLocalShardData) GetShardIds() []common.ShardId {
	return m.shardIds
}

func (m *MsgDestroyLocalShardData) GetRespCh() chan bool {
	return m.respCh
}

func (m *MsgDestroyLocalShardData) String() string {
	str := "\n\tMessage: MsgDestroyLocalShardData"
	str += fmt.Sprintf("\nShardIds: %v", m.shardIds)
	return str
}

type MsgUpdateShardIds struct {
	partnShardIdMap common.PartnShardIdMap
}

func (m *MsgUpdateShardIds) GetMsgType() MsgType {
	return UPDATE_SHARDID_MAP
}

func (m *MsgUpdateShardIds) GetShardIds() common.PartnShardIdMap {
	return m.partnShardIdMap
}

// MsgType.String is a helper function to return string for message type.
func (m MsgType) String() string {

	switch m {
	case MSG_SUCCESS:
		return "MSG_SUCCESS"
	case MSG_ERROR:
		return "MSG_ERROR"
	case MSG_TIMESTAMP:
		return "MSG_TIMESTAMP"
	case STREAM_READER_STREAM_DROP_DATA:
		return "STREAM_READER_STREAM_DROP_DATA"
	case STREAM_READER_STREAM_BEGIN:
		return "STREAM_READER_STREAM_BEGIN"
	case STREAM_READER_STREAM_END:
		return "STREAM_READER_STREAM_END"
	case STREAM_READER_SYNC:
		return "STREAM_READER_SYNC"
	case STREAM_READER_SNAPSHOT_MARKER:
		return "STREAM_READER_SNAPSHOT_MARKER"
	case STREAM_READER_UPDATE_QUEUE_MAP:
		return "STREAM_READER_UPDATE_QUEUE_MAP"
	case STREAM_READER_ERROR:
		return "STREAM_READER_ERROR"
	case STREAM_READER_SHUTDOWN:
		return "STREAM_READER_SHUTDOWN"
	case STREAM_READER_CONN_ERROR:
		return "STREAM_READER_CONN_ERROR"
	case STREAM_READER_HWT:
		return "STREAM_READER_HWT"
	case STREAM_READER_SYSTEM_EVENT:
		return "STREAM_READER_SYSTEM_EVENT"
	case STREAM_READER_OSO_SNAPSHOT_MARKER:
		return "STREAM_READER_OSO_SNAPSHOT_MARKER"

	case MUT_MGR_PERSIST_MUTATION_QUEUE:
		return "MUT_MGR_PERSIST_MUTATION_QUEUE"
	case MUT_MGR_ABORT_PERSIST:
		return "MUT_MGR_ABORT_PERSIST"
	case MUT_MGR_DRAIN_MUTATION_QUEUE:
		return "MUT_MGR_DRAIN_MUTATION_QUEUE"
	case MUT_MGR_GET_MUTATION_QUEUE_HWT:
		return "MUT_MGR_GET_MUTATION_QUEUE_HWT"
	case MUT_MGR_GET_MUTATION_QUEUE_LWT:
		return "MUT_MGR_GET_MUTATION_QUEUE_LWT"
	case MUT_MGR_SHUTDOWN:
		return "MUT_MGR_SHUTDOWN"
	case MUT_MGR_FLUSH_DONE:
		return "MUT_MGR_FLUSH_DONE"
	case MUT_MGR_ABORT_DONE:
		return "MUT_MGR_ABORT_DONE"
	case MUT_MGR_STREAM_CLOSE:
		return "MUT_MGR_STREAM_CLOSE"

	case TK_SHUTDOWN:
		return "TK_SHUTDOWN"

	case TK_STABILITY_TIMESTAMP:
		return "TK_STABILITY_TIMESTAMP"
	case TK_INIT_BUILD_DONE:
		return "TK_INIT_BUILD_DONE"
	case TK_INIT_BUILD_DONE_ACK:
		return "TK_INIT_BUILD_DONE_ACK"
	case TK_INIT_BUILD_DONE_NO_CATCHUP_ACK:
		return "TK_INIT_BUILD_DONE_NO_CATCHUP_ACK"
	case TK_ADD_INSTANCE_FAIL:
		return "TK_ADD_INSTANCE_FAIL"
	case TK_ENABLE_FLUSH:
		return "TK_ENABLE_FLUSH"
	case TK_DISABLE_FLUSH:
		return "TK_DISABLE_FLUSH"
	case TK_MERGE_STREAM:
		return "TK_MERGE_STREAM"
	case TK_MERGE_STREAM_ACK:
		return "TK_MERGE_STREAM_ACK"
	case TK_GET_KEYSPACE_HWT:
		return "TK_GET_KEYSPACE_HWT"
	case REPAIR_ABORT:
		return "REPAIR_ABORT"
	case POOL_CHANGE:
		return "POOL_CHANGE"

	case STORAGE_MGR_SHUTDOWN:
		return "STORAGE_MGR_SHUTDOWN"

	case KV_SENDER_SHUTDOWN:
		return "KV_SENDER_SHUTDOWN"
	case KV_SENDER_GET_CURR_KV_TS:
		return "KV_SENDER_GET_CURR_KV_TS"

	case ADMIN_MGR_SHUTDOWN:
		return "ADMIN_MGR_SHUTDOWN"
	case MSG_SUCCESS_DROP:
		return "MSG_SUCCESS_DROP"
	case CLUST_MGR_AGENT_SHUTDOWN:
		return "CLUST_MGR_AGENT_SHUTDOWN"
	case CBQ_BRIDGE_SHUTDOWN:
		return "CBQ_BRIDGE_SHUTDOWN"

	case INDEXER_INIT_PREP_RECOVERY:
		return "INDEXER_INIT_PREP_RECOVERY"
	case INDEXER_PREPARE_RECOVERY:
		return "INDEXER_PREPARE_RECOVERY"
	case INDEXER_PREPARE_DONE:
		return "INDEXER_PREPARE_DONE"
	case INDEXER_INITIATE_RECOVERY:
		return "INDEXER_INITIATE_RECOVERY"
	case INDEXER_RECOVERY_DONE:
		return "INDEXER_RECOVERY_DONE"
	case INDEXER_KEYSPACE_NOT_FOUND:
		return "INDEXER_KEYSPACE_NOT_FOUND"
	case INDEXER_ABORT_RECOVERY:
		return "INDEXER_ABORT_RECOVERY"
	case INDEXER_ROLLBACK:
		return "INDEXER_ROLLBACK"
	case STORAGE_ROLLBACK_DONE:
		return "STORAGE_ROLLBACK_DONE"
	case STREAM_REQUEST_DONE:
		return "STREAM_REQUEST_DONE"
	case INDEXER_PAUSE:
		return "INDEXER_PAUSE"
	case INDEXER_RESUME:
		return "INDEXER_RESUME"
	case INDEXER_PREPARE_UNPAUSE:
		return "INDEXER_PREPARE_UNPAUSE"
	case INDEXER_UNPAUSE:
		return "INDEXER_UNPAUSE"
	case INDEXER_BOOTSTRAP:
		return "INDEXER_BOOTSTRAP"
	case INDEXER_SET_LOCAL_META:
		return "INDEXER_SET_LOCAL_META"
	case INDEXER_GET_LOCAL_META:
		return "INDEXER_GET_LOCAL_META"
	case INDEXER_DEL_LOCAL_META:
		return "INDEXER_DEL_LOCAL_META"
	case INDEXER_CHECK_DDL_IN_PROGRESS:
		return "INDEXER_CHECK_DDL_IN_PROGRESS"
	case INDEXER_UPDATE_RSTATE:
		return "INDEXER_UPDATE_RSTATE"
	case INDEXER_MERGE_PARTITION:
		return "INDEXER_MERGE_PARTITION"
	case INDEXER_CANCEL_MERGE_PARTITION:
		return "INDEXER_CANCEL_MERGE_PARTITION"
	case INDEXER_MTR_FAIL:
		return "INDEXER_MTR_FAIL"
	case INDEXER_STORAGE_WARMUP_DONE:
		return "INDEXER_STORAGE_WARMUP_DONE"
	case INDEXER_SECURITY_CHANGE:
		return "INDEXER_SECURITY_CHANGE"
	case INDEXER_RESET_INDEX_DONE:
		return "INDEXER_RESET_INDEX_DONE"
	case INDEXER_ACTIVE:
		return "INDEXER_ACTIVE"

	case SCAN_COORD_SHUTDOWN:
		return "SCAN_COORD_SHUTDOWN"

	case COMPACTION_MGR_SHUTDOWN:
		return "COMPACTION_MGR_SHUTDOWN"

	case UPDATE_INDEX_INSTANCE_MAP:
		return "UPDATE_INDEX_INSTANCE_MAP"
	case UPDATE_INDEX_PARTITION_MAP:
		return "UPDATE_INDEX_PARTITION_MAP"
	case UPDATE_KEYSPACE_STATS_MAP:
		return "UPDATE_KEYSPACE_STATS_MAP"
	case UPDATE_MAP_WORKER:
		return "UPDATE_MAP_WORKER"

	case OPEN_STREAM:
		return "OPEN_STREAM"
	case ADD_INDEX_LIST_TO_STREAM:
		return "ADD_INDEX_LIST_TO_STREAM"
	case REMOVE_INDEX_LIST_FROM_STREAM:
		return "REMOVE_INDEX_LIST_FROM_STREAM"
	case REMOVE_KEYSPACE_FROM_STREAM:
		return "REMOVE_KEYSPACE_FROM_STREAM"
	case CLOSE_STREAM:
		return "CLOSE_STREAM"
	case CLEANUP_STREAM:
		return "CLEANUP_STREAM"
	case CLEANUP_PRJ_STATS:
		return "CLEANUP_PRJ_STATS"
	case INDEXER_UPDATE_BUILD_TS:
		return "INDEXER_UPDATE_BUILD_TS"
	case RESET_STREAM:
		return "RESET_STREAM"

	case KV_SENDER_RESTART_VBUCKETS:
		return "KV_SENDER_RESTART_VBUCKETS"
	case KV_SENDER_RESTART_VBUCKETS_RESPONSE:
		return "KV_SENDER_RESTART_VBUCKETS_RESPONSE"
	case KV_SENDER_REPAIR_ENDPOINTS:
		return "KV_SENDER_REPAIR_ENDPOINTS"
	case KV_STREAM_REPAIR:
		return "KV_STREAM_REPAIR"
	case MSG_SUCCESS_OPEN_STREAM:
		return "MSG_SUCCESS_OPEN_STREAM"

	case CLUST_MGR_CREATE_INDEX_DDL:
		return "CLUST_MGR_CREATE_INDEX_DDL"
	case CLUST_MGR_BUILD_INDEX_DDL:
		return "CLUST_MGR_BUILD_INDEX_DDL"
	case CLUST_MGR_BUILD_INDEX_DDL_RESPONSE:
		return "CLUST_MGR_BUILD_INDEX_DDL_RESPONSE"
	case CLUST_MGR_DROP_INDEX_DDL:
		return "CLUST_MGR_DROP_INDEX_DDL"
	case CLUST_MGR_UPDATE_TOPOLOGY_FOR_INDEX:
		return "CLUST_MGR_UPDATE_TOPOLOGY_FOR_INDEX"
	case CLUST_MGR_GET_GLOBAL_TOPOLOGY:
		return "CLUST_MGR_GET_GLOBAL_TOPOLOGY"
	case CLUST_MGR_GET_LOCAL:
		return "CLUST_MGR_GET_LOCAL"
	case CLUST_MGR_SET_LOCAL:
		return "CLUST_MGR_SET_LOCAL"
	case CLUST_MGR_DEL_LOCAL:
		return "CLUST_MGR_DEL_LOCAL"
	case CLUST_MGR_DEL_KEYSPACE:
		return "CLUST_MGR_DEL_KEYSPACE"
	case CLUST_MGR_INDEXER_READY:
		return "CLUST_MGR_INDEXER_READY"
	case CLUST_MGR_REBALANCE_RUNNING:
		return "CLUST_MGR_REBALANCE_RUNNING"
	case CLUST_MGR_CLEANUP_INDEX:
		return "CLUST_MGR_CLEANUP_INDEX"
	case CLUST_MGR_CLEANUP_PARTITION:
		return "CLUST_MGR_CLEANUP_PARTITION"
	case CLUST_MGR_MERGE_PARTITION:
		return "CLUST_MGR_MERGE_PARTITION"
	case CLUST_MGR_PRUNE_PARTITION:
		return "CLUST_MGR_PRUNE_PARTITION"
	case CLUST_MGR_RESET_INDEX_ON_UPGRADE:
		return "CLUST_MGR_RESET_INDEX_ON_UPGRADE"
	case CLUST_MGR_RESET_INDEX_ON_ROLLBACK:
		return "CLUST_MGR_RESET_INDEX_ON_ROLLBACK"
	case CLUST_MGR_RECOVER_INDEX:
		return "CLUST_MGR_RECOVER_INDEX"
	case CLUST_MGR_BUILD_RECOVERED_INDEXES:
		return "CLUST_MGR_BUILD_RECOVERED_INDEXES"

	case CBQ_CREATE_INDEX_DDL:
		return "CBQ_CREATE_INDEX_DDL"
	case CBQ_DROP_INDEX_DDL:
		return "CBQ_DROP_INDEX_DDL"

	case STORAGE_INDEX_SNAP_REQUEST:
		return "STORAGE_INDEX_SNAP_REQUEST"
	case STORAGE_INDEX_STORAGE_STATS:
		return "STORAGE_INDEX_STORAGE_STATS"
	case STORAGE_INDEX_COMPACT:
		return "STORAGE_INDEX_COMPACT"
	case STORAGE_SNAP_DONE:
		return "STORAGE_SNAP_DONE"
	case STORAGE_INDEX_MERGE_SNAPSHOT:
		return "STORAGE_INDEX_MERGE_SNAPSHOT"
	case STORAGE_INDEX_PRUNE_SNAPSHOT:
		return "STORAGE_INDEX_PRUNE_SNAPSHOT"
	case STORAGE_UPDATE_SNAP_MAP:
		return "STORAGE_UPDATE_SNAP_MAP"

	case CONFIG_SETTINGS_UPDATE:
		return "CONFIG_SETTINGS_UPDATE"

	case STORAGE_STATS:
		return "STORAGE_STATS"
	case SCAN_STATS:
		return "SCAN_STATS"
	case INDEX_PROGRESS_STATS:
		return "INDEX_PROGRESS_STATS"
	case INDEXER_STATS:
		return "INDEXER_STATS"
	case INDEX_STATS_DONE:
		return "INDEX_STATS_DONE"
	case INDEX_STATS_BROADCAST:
		return "INDEX_STATS_BROADCAST"
	case INDEX_BOOTSTRAP_STATS_UPDATE:
		return "INDEX_BOOTSTRAP_STATS_UPDATE"

	case STATS_RESET:
		return "STATS_RESET"
	case STATS_PERSISTER_START:
		return "STATS_PERSISTER_START"
	case STATS_PERSISTER_STOP:
		return "STATS_PERSISTER_STOP"
	case STATS_PERSISTER_FORCE_PERSIST:
		return "STATS_PERSISTER_FORCE_PERSIST"
	case STATS_PERSISTER_CONFIG_UPDATE:
		return "STATS_PERSISTER_CONFIG_UPDATE"
	case STATS_READ_PERSISTED_STATS:
		return "STATS_READ_PERSISTED_STATS"
	case STATS_LOG_AT_EXIT:
		return "STATS_LOG_AT_EXIT"

	case INDEXER_DDL_IN_PROGRESS_RESPONSE:
		return "INDEXER_DDL_IN_PROGRESS_RESPONSE"
	case INDEXER_DROP_COLLECTION:
		return "INDEXER_DROP_COLLECTION"

	case START_SHARD_TRANSFER:
		return "START_SHARD_TRANSFER"
	case SHARD_TRANSFER_RESPONSE:
		return "SHARD_TRANSFER_RESPONSE"
	case SHARD_TRANSFER_CLEANUP:
		return "SHARD_TRANSFER_CLEANUP"
	case START_SHARD_RESTORE:
		return "START_SHARD_RESTORE"
	case DESTROY_LOCAL_SHARD:
		return "DESTROY_LOCAL_SHARD"

	default:
		return "UNKNOWN_MSG_TYPE"
	}

}
