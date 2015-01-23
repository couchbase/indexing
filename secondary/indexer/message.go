// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package indexer

import (
	"fmt"
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

	//MUTATION_MANAGER
	MUT_MGR_PERSIST_MUTATION_QUEUE
	MUT_MGR_ABORT_PERSIST
	MUT_MGR_DRAIN_MUTATION_QUEUE
	MUT_MGR_GET_MUTATION_QUEUE_HWT
	MUT_MGR_GET_MUTATION_QUEUE_LWT
	MUT_MGR_SHUTDOWN
	MUT_MGR_FLUSH_DONE
	MUT_MGR_ABORT_DONE

	//TIMEKEEPER
	TK_SHUTDOWN
	TK_STABILITY_TIMESTAMP
	TK_INIT_BUILD_DONE
	TK_INIT_BUILD_DONE_ACK
	TK_ENABLE_FLUSH
	TK_DISABLE_FLUSH
	TK_MERGE_STREAM
	TK_MERGE_STREAM_ACK
	TK_GET_BUCKET_HWT

	//STORAGE_MANAGER
	STORAGE_MGR_SHUTDOWN
	STORAGE_INDEX_SNAP_REQUEST
	STORAGE_INDEX_STORAGE_STATS
	STORAGE_INDEX_COMPACT

	//KVSender
	KV_SENDER_SHUTDOWN
	KV_SENDER_GET_CURR_KV_TS
	KV_SENDER_RESTART_VBUCKETS
	KV_SENDER_REPAIR_ENDPOINTS
	KV_STREAM_REPAIR

	//ADMIN_MGR
	ADMIN_MGR_SHUTDOWN

	//CLUSTER_MGR
	CLUST_MGR_AGENT_SHUTDOWN
	CLUST_MGR_CREATE_INDEX_DDL
	CLUST_MGR_BUILD_INDEX_DDL
	CLUST_MGR_DROP_INDEX_DDL
	CLUST_MGR_UPDATE_TOPOLOGY_FOR_INDEX
	CLUST_MGR_GET_GLOBAL_TOPOLOGY
	CLUST_MGR_GET_LOCAL
	CLUST_MGR_SET_LOCAL

	//CBQ_BRIDGE_SHUTDOWN
	CBQ_BRIDGE_SHUTDOWN
	CBQ_CREATE_INDEX_DDL
	CBQ_DROP_INDEX_DDL

	//INDEXER
	INDEXER_PREPARE_RECOVERY
	INDEXER_PREPARE_DONE
	INDEXER_INITIATE_RECOVERY
	INDEXER_RECOVERY_DONE
	INDEXER_ROLLBACK
	STREAM_REQUEST_DONE

	//SCAN COORDINATOR
	SCAN_COORD_SHUTDOWN

	COMPACTION_MGR_SHUTDOWN

	//COMMON
	UPDATE_INDEX_INSTANCE_MAP
	UPDATE_INDEX_PARTITION_MAP

	OPEN_STREAM
	ADD_INDEX_LIST_TO_STREAM
	REMOVE_INDEX_LIST_FROM_STREAM
	REMOVE_BUCKET_FROM_STREAM
	CLOSE_STREAM
	CLEANUP_STREAM

	CONFIG_SETTINGS_UPDATE

	STORAGE_STATS
	SCAN_STATS
	INDEX_PROGRESS_STATS
)

type Message interface {
	GetMsgType() MsgType
}

//Generic Message
type MsgGeneral struct {
	mType MsgType
}

func (m *MsgGeneral) GetMsgType() MsgType {
	return m.mType
}

//Error Message
type MsgError struct {
	err Error
}

func (m *MsgError) GetMsgType() MsgType {
	return MSG_ERROR
}

func (m *MsgError) GetError() Error {
	return m.err
}

//Success Message
type MsgSuccess struct {
}

func (m *MsgSuccess) GetMsgType() MsgType {
	return MSG_SUCCESS
}

//Timestamp Message
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

//Stream Reader Message
type MsgStream struct {
	mType    MsgType
	streamId common.StreamId
	meta     *MutationMeta
	snapshot *MutationSnapshot
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

func (m *MsgStream) GetSnapshot() *MutationSnapshot {
	return m.snapshot
}

func (m *MsgStream) String() string {

	str := "\n\tMessage: MsgStream"
	str += fmt.Sprintf("\n\tType: %v", m.mType)
	str += fmt.Sprintf("\n\tStreamId: %v", m.streamId)
	str += fmt.Sprintf("\n\tMeta: %v", m.meta)
	str += fmt.Sprintf("\n\tSnapshot: %v", m.snapshot)
	return str

}

//Stream Error Message
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

//STREAM_READER_CONN_ERROR
//STREAM_REQUEST_DONE
type MsgStreamInfo struct {
	mType    MsgType
	streamId common.StreamId
	bucket   string
	vbList   []Vbucket
}

func (m *MsgStreamInfo) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgStreamInfo) GetStreamId() common.StreamId {
	return m.streamId
}

func (m *MsgStreamInfo) GetBucket() string {
	return m.bucket
}

func (m *MsgStreamInfo) GetVbList() []Vbucket {
	return m.vbList
}

func (m *MsgStreamInfo) String() string {

	str := "\n\tMessage: MsgStreamInfo"
	str += fmt.Sprintf("\n\tType: %v", m.mType)
	str += fmt.Sprintf("\n\tStream: %v", m.streamId)
	str += fmt.Sprintf("\n\tBucket: %v", m.bucket)
	str += fmt.Sprintf("\n\tVbList: %v", m.vbList)
	return str
}

//STREAM_READER_UPDATE_QUEUE_MAP
type MsgUpdateBucketQueue struct {
	bucketQueueMap BucketQueueMap
}

func (m *MsgUpdateBucketQueue) GetMsgType() MsgType {
	return STREAM_READER_UPDATE_QUEUE_MAP
}

func (m *MsgUpdateBucketQueue) GetBucketQueueMap() BucketQueueMap {
	return m.bucketQueueMap
}

func (m *MsgUpdateBucketQueue) String() string {

	str := "\n\tMessage: MsgUpdateBucketQueue"
	str += fmt.Sprintf("\n\tBucketQueueMap: %v", m.bucketQueueMap)
	return str

}

//OPEN_STREAM
//ADD_INDEX_LIST_TO_STREAM
//REMOVE_BUCKET_FROM_STREAM
//REMOVE_INDEX_LIST_FROM_STREAM
//CLOSE_STREAM
//CLEANUP_STREAM
type MsgStreamUpdate struct {
	mType     MsgType
	streamId  common.StreamId
	indexList []common.IndexInst
	buildTs   Timestamp
	respCh    MsgChannel
	stopCh    StopChannel
	bucket    string
	restartTs *common.TsVbuuid
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

func (m *MsgStreamUpdate) GetBucket() string {
	return m.bucket
}

func (m *MsgStreamUpdate) GetRestartTs() *common.TsVbuuid {
	return m.restartTs
}

func (m *MsgStreamUpdate) String() string {

	str := "\n\tMessage: MsgStreamUpdate"
	str += fmt.Sprintf("\n\tType: %v", m.mType)
	str += fmt.Sprintf("\n\tStream: %v", m.streamId)
	str += fmt.Sprintf("\n\tBuildTS: %v", m.buildTs)
	str += fmt.Sprintf("\n\tIndexList: %v", m.indexList)
	str += fmt.Sprintf("\n\tRestartTs: %v", m.restartTs)
	return str

}

//MUT_MGR_PERSIST_MUTATION_QUEUE
//MUT_MGR_ABORT_PERSIST
//MUT_MGR_DRAIN_MUTATION_QUEUE
type MsgMutMgrFlushMutationQueue struct {
	mType    MsgType
	bucket   string
	streamId common.StreamId
	ts       *common.TsVbuuid
}

func (m *MsgMutMgrFlushMutationQueue) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgMutMgrFlushMutationQueue) GetBucket() string {
	return m.bucket
}

func (m *MsgMutMgrFlushMutationQueue) GetStreamId() common.StreamId {
	return m.streamId
}

func (m *MsgMutMgrFlushMutationQueue) GetTimestamp() *common.TsVbuuid {
	return m.ts
}

func (m *MsgMutMgrFlushMutationQueue) String() string {

	str := "\n\tMessage: MsgMutMgrFlushMutationQueue"
	str += fmt.Sprintf("\n\tType: %v", m.mType)
	str += fmt.Sprintf("\n\tBucket: %v", m.bucket)
	str += fmt.Sprintf("\n\tStream: %v", m.streamId)
	str += fmt.Sprintf("\n\tTS: %v", m.ts)
	return str

}

//MUT_MGR_GET_MUTATION_QUEUE_HWT
//MUT_MGR_GET_MUTATION_QUEUE_LWT
type MsgMutMgrGetTimestamp struct {
	mType    MsgType
	bucket   string
	streamId common.StreamId
}

func (m *MsgMutMgrGetTimestamp) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgMutMgrGetTimestamp) GetBucket() string {
	return m.bucket
}

func (m *MsgMutMgrGetTimestamp) GetStreamId() common.StreamId {
	return m.streamId
}

//UPDATE_INSTANCE_MAP
type MsgUpdateInstMap struct {
	indexInstMap common.IndexInstMap
}

func (m *MsgUpdateInstMap) GetMsgType() MsgType {
	return UPDATE_INDEX_INSTANCE_MAP
}

func (m *MsgUpdateInstMap) GetIndexInstMap() common.IndexInstMap {
	return m.indexInstMap
}

func (m *MsgUpdateInstMap) String() string {

	str := "\n\tMessage: MsgUpdateInstMap"
	str += fmt.Sprintf("%v", m.indexInstMap)
	return str
}

//UPDATE_PARTITION_MAP
type MsgUpdatePartnMap struct {
	indexPartnMap IndexPartnMap
}

func (m *MsgUpdatePartnMap) GetMsgType() MsgType {
	return UPDATE_INDEX_PARTITION_MAP
}

func (m *MsgUpdatePartnMap) GetIndexPartnMap() IndexPartnMap {
	return m.indexPartnMap
}

func (m *MsgUpdatePartnMap) String() string {

	str := "\n\tMessage: MsgUpdatePartnMap"
	str += fmt.Sprintf("%v", m.indexPartnMap)
	return str
}

//MUT_MGR_FLUSH_DONE
//MUT_MGR_ABORT_DONE
type MsgMutMgrFlushDone struct {
	mType    MsgType
	ts       *common.TsVbuuid
	streamId common.StreamId
	bucket   string
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

func (m *MsgMutMgrFlushDone) GetBucket() string {
	return m.bucket
}

func (m *MsgMutMgrFlushDone) String() string {

	str := "\n\tMessage: MsgMutMgrFlushDone"
	str += fmt.Sprintf("\n\tStream: %v", m.streamId)
	str += fmt.Sprintf("\n\tBucket: %v", m.bucket)
	str += fmt.Sprintf("\n\tTS: %v", m.ts)
	return str

}

//TK_STABILITY_TIMESTAMP
type MsgTKStabilityTS struct {
	ts       *common.TsVbuuid
	streamId common.StreamId
	bucket   string
}

func (m *MsgTKStabilityTS) GetMsgType() MsgType {
	return TK_STABILITY_TIMESTAMP
}

func (m *MsgTKStabilityTS) GetStreamId() common.StreamId {
	return m.streamId
}

func (m *MsgTKStabilityTS) GetBucket() string {
	return m.bucket
}

func (m *MsgTKStabilityTS) GetTimestamp() *common.TsVbuuid {
	return m.ts
}

func (m *MsgTKStabilityTS) String() string {

	str := "\n\tMessage: MsgTKStabilityTS"
	str += fmt.Sprintf("\n\tStream: %v", m.streamId)
	str += fmt.Sprintf("\n\tBucket: %v", m.bucket)
	str += fmt.Sprintf("\n\tTS: %v", m.ts)
	return str

}

//TK_INIT_BUILD_DONE
//TK_INIT_BUILD_DONE_ACK
type MsgTKInitBuildDone struct {
	mType    MsgType
	streamId common.StreamId
	buildTs  Timestamp
	bucket   string
}

func (m *MsgTKInitBuildDone) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgTKInitBuildDone) GetBucket() string {
	return m.bucket
}

func (m *MsgTKInitBuildDone) GetTimestamp() Timestamp {
	return m.buildTs
}

func (m *MsgTKInitBuildDone) GetStreamId() common.StreamId {
	return m.streamId
}

//TK_MERGE_STREAM
//TK_MERGE_STREAM_ACK
type MsgTKMergeStream struct {
	mType    MsgType
	streamId common.StreamId
	bucket   string
	mergeTs  Timestamp
}

func (m *MsgTKMergeStream) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgTKMergeStream) GetStreamId() common.StreamId {
	return m.streamId
}

func (m *MsgTKMergeStream) GetBucket() string {
	return m.bucket
}

func (m *MsgTKMergeStream) GetMergeTS() Timestamp {
	return m.mergeTs
}

//TK_ENABLE_FLUSH
//TK_DISABLE_FLUSH
type MsgTKToggleFlush struct {
	mType    MsgType
	streamId common.StreamId
	bucket   string
}

func (m *MsgTKToggleFlush) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgTKToggleFlush) GetStreamId() common.StreamId {
	return m.streamId
}

func (m *MsgTKToggleFlush) GetBucket() string {
	return m.bucket
}

//CBQ_CREATE_INDEX_DDL
//CLUST_MGR_CREATE_INDEX_DDL
type MsgCreateIndex struct {
	mType     MsgType
	indexInst common.IndexInst
	respCh    MsgChannel
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

func (m *MsgCreateIndex) GetString() string {

	str := "\n\tMessage: MsgCreateIndex"
	str += fmt.Sprintf("\n\tType: %v", m.mType)
	str += fmt.Sprintf("\n\tIndex: %v", m.indexInst)
	return str
}

//CLUST_MGR_BUILD_INDEX_DDL
type MsgBuildIndex struct {
	indexInstList []common.IndexInstId
	respCh        MsgChannel
}

func (m *MsgBuildIndex) GetMsgType() MsgType {
	return CLUST_MGR_BUILD_INDEX_DDL
}

func (m *MsgBuildIndex) GetIndexList() []common.IndexInstId {
	return m.indexInstList
}

func (m *MsgBuildIndex) GetRespCh() MsgChannel {
	return m.respCh
}

func (m *MsgBuildIndex) GetString() string {

	str := "\n\tMessage: MsgBuildIndex"
	str += fmt.Sprintf("\n\tType: %v", CLUST_MGR_BUILD_INDEX_DDL)
	str += fmt.Sprintf("\n\tIndex: %v", m.indexInstList)
	return str
}

//CBQ_DROP_INDEX_DDL
//CLUST_MGR_DROP_INDEX_DDL
type MsgDropIndex struct {
	mType       MsgType
	indexInstId common.IndexInstId
	respCh      MsgChannel
}

func (m *MsgDropIndex) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgDropIndex) GetIndexInstId() common.IndexInstId {
	return m.indexInstId
}

func (m *MsgDropIndex) GetResponseChannel() MsgChannel {
	return m.respCh
}

func (m *MsgDropIndex) GetString() string {

	str := "\n\tMessage: MsgDropIndex"
	str += fmt.Sprintf("\n\tType: %v", m.mType)
	str += fmt.Sprintf("\n\tIndex: %v", m.indexInstId)
	return str
}

//TK_GET_BUCKET_HWT
type MsgTKGetBucketHWT struct {
	streamId common.StreamId
	bucket   string
	ts       *common.TsVbuuid
}

func (m *MsgTKGetBucketHWT) GetMsgType() MsgType {
	return TK_GET_BUCKET_HWT
}

func (m *MsgTKGetBucketHWT) GetStreamId() common.StreamId {
	return m.streamId
}

func (m *MsgTKGetBucketHWT) GetBucket() string {
	return m.bucket
}

func (m *MsgTKGetBucketHWT) GetHWT() *common.TsVbuuid {
	return m.ts
}

func (m *MsgTKGetBucketHWT) String() string {

	str := "\n\tMessage: MsgTKGetBucketHWT"
	str += fmt.Sprintf("\n\tStreamId: %v", m.streamId)
	str += fmt.Sprintf("\n\tBucket: %v", m.bucket)
	if m.ts != nil {
		str += fmt.Sprintf("\n\tTS Seqnos: %v", m.ts.Seqnos)
		str += fmt.Sprintf("\n\tTS Vbuuids: %v", m.ts.Vbuuids)
		str += fmt.Sprintf("\n\tTS Snapshots: %v", m.ts.Snapshots)
	}
	return str

}

//KV_SENDER_RESTART_VBUCKETS
type MsgRestartVbuckets struct {
	streamId  common.StreamId
	bucket    string
	restartTs *common.TsVbuuid
	respCh    MsgChannel
	stopCh    StopChannel
}

func (m *MsgRestartVbuckets) GetMsgType() MsgType {
	return KV_SENDER_RESTART_VBUCKETS
}

func (m *MsgRestartVbuckets) GetStreamId() common.StreamId {
	return m.streamId
}

func (m *MsgRestartVbuckets) GetBucket() string {
	return m.bucket
}

func (m *MsgRestartVbuckets) GetRestartTs() *common.TsVbuuid {
	return m.restartTs
}

func (m *MsgRestartVbuckets) GetResponseCh() MsgChannel {
	return m.respCh
}

func (m *MsgRestartVbuckets) GetStopChannel() StopChannel {
	return m.stopCh
}

func (m *MsgRestartVbuckets) String() string {
	str := "\n\tMessage: MsgRestartVbuckets"
	str += fmt.Sprintf("\n\tStreamId: %v", m.streamId)
	str += fmt.Sprintf("\n\tRestartTS: %v", m.restartTs)
	return str
}

//KV_SENDER_REPAIR_ENDPOINTS
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

//INDEXER_PREPARE_RECOVERY
//INDEXER_PREPARE_DONE
//INDEXER_INITIATE_RECOVERY
//INDEXER_RECOVERY_DONE
type MsgRecovery struct {
	mType     MsgType
	streamId  common.StreamId
	bucket    string
	restartTs *common.TsVbuuid
}

func (m *MsgRecovery) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgRecovery) GetStreamId() common.StreamId {
	return m.streamId
}

func (m *MsgRecovery) GetBucket() string {
	return m.bucket
}

func (m *MsgRecovery) GetRestartTs() *common.TsVbuuid {
	return m.restartTs
}

type MsgRollback struct {
	streamId   common.StreamId
	bucket     string
	rollbackTs *common.TsVbuuid
}

func (m *MsgRollback) GetMsgType() MsgType {
	return INDEXER_ROLLBACK
}

func (m *MsgRollback) GetStreamId() common.StreamId {
	return m.streamId
}

func (m *MsgRollback) GetBucket() string {
	return m.bucket
}

func (m *MsgRollback) GetRollbackTs() *common.TsVbuuid {
	return m.rollbackTs
}

type MsgIndexSnapRequest struct {
	ts        *common.TsVbuuid
	idxInstId common.IndexInstId

	// Send error or index snapshot
	respch chan interface{}
}

func (m *MsgIndexSnapRequest) GetMsgType() MsgType {
	return STORAGE_INDEX_SNAP_REQUEST
}

func (m *MsgIndexSnapRequest) GetTS() *common.TsVbuuid {
	return m.ts
}

func (m *MsgIndexSnapRequest) GetReplyChannel() chan interface{} {
	return m.respch
}

func (m *MsgIndexSnapRequest) GetIndexId() common.IndexInstId {
	return m.idxInstId
}

type MsgIndexStorageStats struct {
	respch chan []IndexStorageStats
}

func (m *MsgIndexStorageStats) GetMsgType() MsgType {
	return STORAGE_INDEX_STORAGE_STATS
}

func (m *MsgIndexStorageStats) GetReplyChannel() chan []IndexStorageStats {
	return m.respch
}

type MsgStatsRequest struct {
	mType  MsgType
	respch chan map[string]string
}

func (m *MsgStatsRequest) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgStatsRequest) GetReplyChannel() chan map[string]string {
	return m.respch
}

type MsgIndexCompact struct {
	instId common.IndexInstId
	errch  chan error
}

func (m *MsgIndexCompact) GetMsgType() MsgType {
	return STORAGE_INDEX_COMPACT
}

func (m *MsgIndexCompact) GetInstId() common.IndexInstId {
	return m.instId
}

func (m *MsgIndexCompact) GetErrorChannel() chan error {
	return m.errch
}

//KV_STREAM_REPAIR
type MsgKVStreamRepair struct {
	streamId  common.StreamId
	bucket    string
	restartTs *common.TsVbuuid
}

func (m *MsgKVStreamRepair) GetMsgType() MsgType {
	return KV_STREAM_REPAIR
}

func (m *MsgKVStreamRepair) GetStreamId() common.StreamId {
	return m.streamId
}

func (m *MsgKVStreamRepair) GetBucket() string {
	return m.bucket
}

func (m *MsgKVStreamRepair) GetRestartTs() *common.TsVbuuid {
	return m.restartTs
}

//CLUST_MGR_UPDATE_TOPOLOGY_FOR_INDEX
type MsgClustMgrUpdate struct {
	mType         MsgType
	indexList     []common.IndexInst
	updatedFields MetaUpdateFields
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

//CLUST_MGR_GET_GLOBAL_TOPOLOGY
type MsgClustMgrTopology struct {
	indexInstMap common.IndexInstMap
}

func (m *MsgClustMgrTopology) GetMsgType() MsgType {
	return CLUST_MGR_GET_GLOBAL_TOPOLOGY
}

func (m *MsgClustMgrTopology) GetInstMap() common.IndexInstMap {
	return m.indexInstMap
}

//CLUST_MGR_GET_LOCAL
//CLUST_MGR_SET_LOCAL
type MsgClustMgrLocal struct {
	mType MsgType
	key   string
	value string
	err   error
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

type MsgConfigUpdate struct {
	cfg common.Config
}

func (m *MsgConfigUpdate) GetMsgType() MsgType {
	return CONFIG_SETTINGS_UPDATE
}

func (m *MsgConfigUpdate) GetConfig() common.Config {
	return m.cfg
}

//Helper function to return string for message type

func (m MsgType) String() string {

	switch m {

	case MSG_SUCCESS:
		return "MSG_SUCCESS"
	case MSG_ERROR:
		return "MSG_SUCCESS"
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

	case TK_SHUTDOWN:
		return "TK_SHUTDOWN"

	case TK_STABILITY_TIMESTAMP:
		return "TK_STABILITY_TIMESTAMP"
	case TK_INIT_BUILD_DONE:
		return "TK_INIT_BUILD_DONE"
	case TK_INIT_BUILD_DONE_ACK:
		return "TK_INIT_BUILD_DONE_ACK"
	case TK_ENABLE_FLUSH:
		return "TK_ENABLE_FLUSH"
	case TK_DISABLE_FLUSH:
		return "TK_DISABLE_FLUSH"
	case TK_MERGE_STREAM:
		return "TK_MERGE_STREAM"
	case TK_MERGE_STREAM_ACK:
		return "TK_MERGE_STREAM_ACK"
	case TK_GET_BUCKET_HWT:
		return "TK_GET_BUCKET_HWT"

	case STORAGE_MGR_SHUTDOWN:
		return "STORAGE_MGR_SHUTDOWN"

	case KV_SENDER_SHUTDOWN:
		return "KV_SENDER_SHUTDOWN"
	case KV_SENDER_GET_CURR_KV_TS:
		return "KV_SENDER_GET_CURR_KV_TS"

	case ADMIN_MGR_SHUTDOWN:
		return "ADMIN_MGR_SHUTDOWN"
	case CLUST_MGR_AGENT_SHUTDOWN:
		return "CLUST_MGR_AGENT_SHUTDOWN"
	case CBQ_BRIDGE_SHUTDOWN:
		return "CBQ_BRIDGE_SHUTDOWN"

	case INDEXER_PREPARE_RECOVERY:
		return "INDEXER_PREPARE_RECOVERY"
	case INDEXER_PREPARE_DONE:
		return "INDEXER_PREPARE_DONE"
	case INDEXER_INITIATE_RECOVERY:
		return "INDEXER_INITIATE_RECOVERY"
	case INDEXER_RECOVERY_DONE:
		return "INDEXER_RECOVERY_DONE"
	case INDEXER_ROLLBACK:
		return "INDEXER_ROLLBACK"
	case STREAM_REQUEST_DONE:
		return "STREAM_REQUEST_DONE"

	case SCAN_COORD_SHUTDOWN:
		return "SCAN_COORD_SHUTDOWN"

	case UPDATE_INDEX_INSTANCE_MAP:
		return "UPDATE_INDEX_INSTANCE_MAP"
	case UPDATE_INDEX_PARTITION_MAP:
		return "UPDATE_INDEX_PARTITION_MAP"

	case OPEN_STREAM:
		return "OPEN_STREAM"
	case ADD_INDEX_LIST_TO_STREAM:
		return "ADD_INDEX_LIST_TO_STREAM"
	case REMOVE_INDEX_LIST_FROM_STREAM:
		return "REMOVE_INDEX_LIST_FROM_STREAM"
	case REMOVE_BUCKET_FROM_STREAM:
		return "REMOVE_BUCKET_FROM_STREAM"
	case CLOSE_STREAM:
		return "CLOSE_STREAM"
	case CLEANUP_STREAM:
		return "CLEANUP_STREAM"

	case KV_SENDER_RESTART_VBUCKETS:
		return "KV_SENDER_RESTART_VBUCKETS"
	case KV_SENDER_REPAIR_ENDPOINTS:
		return "KV_SENDER_REPAIR_ENDPOINTS"
	case KV_STREAM_REPAIR:
		return "KV_STREAM_REPAIR"

	case CLUST_MGR_CREATE_INDEX_DDL:
		return "CLUST_MGR_CREATE_INDEX_DDL"
	case CLUST_MGR_BUILD_INDEX_DDL:
		return "CLUST_MGR_BUILD_INDEX_DDL"
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

	case CONFIG_SETTINGS_UPDATE:
		return "CONFIG_SETTINGS_UPDATE"

	default:
		return "UNKNOWN_MSG_TYPE"
	}

}
