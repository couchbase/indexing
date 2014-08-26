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
	STREAM_READER_UPDATE_QUEUE_MAP
	STREAM_READER_ERROR
	STREAM_READER_SHUTDOWN

	//MUTATION_MANAGER
	MUT_MGR_PERSIST_MUTATION_QUEUE
	MUT_MGR_DRAIN_MUTATION_QUEUE
	MUT_MGR_GET_MUTATION_QUEUE_HWT
	MUT_MGR_GET_MUTATION_QUEUE_LWT
	MUT_MGR_SHUTDOWN
	MUT_MGR_FLUSH_DONE

	//TIMEKEEPER
	TK_SHUTDOWN
	TK_STABILITY_TIMESTAMP
	TK_INIT_BUILD_DONE
	TK_ENABLE_FLUSH
	TK_MERGE_STREAM

	//STORAGE_MANAGER
	STORAGE_MGR_SHUTDOWN

	//KVSender
	KV_SENDER_SHUTDOWN
	KV_SENDER_GET_CURR_KV_TS

	//ADMIN_MGR
	ADMIN_MGR_SHUTDOWN

	//CLUSTER_MGR
	CLUST_MGR_SENDER_SHUTDOWN

	//CBQ_BRIDGE_SHUTDOWN
	CBQ_BRIDGE_SHUTDOWN

	//INDEXER
	INDEXER_CREATE_INDEX_DDL
	INDEXER_DROP_INDEX_DDL

	//SCAN COORDINATOR
	SCAN_COORD_SCAN_INDEX
	SCAN_COORD_SCAN_PARTITION
	SCAN_COORD_SCAN_SLICE
	SCAN_COORD_SHUTDOWN

	//COMMON
	UPDATE_INDEX_INSTANCE_MAP
	UPDATE_INDEX_PARTITION_MAP

	OPEN_STREAM
	ADD_INDEX_LIST_TO_STREAM
	REMOVE_INDEX_LIST_FROM_STREAM
	CLOSE_STREAM
	CLEANUP_STREAM
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
//REMOVE_INDEX_LIST_FROM_STREAM
//CLOSE_STREAM
//CLEANUP_STREAM
type MsgStreamUpdate struct {
	mType     MsgType
	streamId  common.StreamId
	indexList []common.IndexInst
	buildTs   Timestamp
	respCh    MsgChannel
	bucket    string
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

func (m *MsgStreamUpdate) GetBucket() string {
	return m.bucket
}

func (m *MsgStreamUpdate) String() string {

	str := "\n\tMessage: MsgStreamUpdate"
	str += fmt.Sprintf("\n\tType: %v", m.mType)
	str += fmt.Sprintf("\n\tStream: %v", m.streamId)
	str += fmt.Sprintf("\n\tBuildTS: %v", m.buildTs)
	str += fmt.Sprintf("\n\tIndexList: %v", m.indexList)
	return str

}

//MUT_MGR_PERSIST_MUTATION_QUEUE
//MUT_MGR_DISCARD_MUTATION_QUEUE
type MsgMutMgrFlushMutationQueue struct {
	mType    MsgType
	bucket   string
	streamId common.StreamId
	ts       Timestamp
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

func (m *MsgMutMgrFlushMutationQueue) GetTimestamp() Timestamp {
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
type MsgMutMgrFlushDone struct {
	ts       Timestamp
	streamId common.StreamId
	bucket   string
}

func (m *MsgMutMgrFlushDone) GetMsgType() MsgType {
	return MUT_MGR_FLUSH_DONE
}

func (m *MsgMutMgrFlushDone) GetTS() Timestamp {
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
	ts       Timestamp
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

func (m *MsgTKStabilityTS) GetTimestamp() Timestamp {
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
type MsgTKInitBuildDone struct {
	streamId common.StreamId
	buildTs  Timestamp
	bucket   string
	respCh   MsgChannel
}

func (m *MsgTKInitBuildDone) GetMsgType() MsgType {
	return TK_INIT_BUILD_DONE
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

func (m *MsgTKInitBuildDone) GetResponseChannel() MsgChannel {
	return m.respCh
}

//TK_MERGE_STREAM
type MsgTKMergeStream struct {
	streamId common.StreamId
	bucket   string
	mergeTs  Timestamp
}

func (m *MsgTKMergeStream) GetMsgType() MsgType {
	return TK_MERGE_STREAM
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
type MsgTKEnableFlush struct {
	streamId common.StreamId
	bucket   string
}

func (m *MsgTKEnableFlush) GetMsgType() MsgType {
	return TK_ENABLE_FLUSH
}

func (m *MsgTKEnableFlush) GetStreamId() common.StreamId {
	return m.streamId
}

func (m *MsgTKEnableFlush) GetBucket() string {
	return m.bucket
}

//INDEXER_CREATE_INDEX_DDL
type MsgCreateIndex struct {
	indexInst common.IndexInst
	respCh    MsgChannel
}

func (m *MsgCreateIndex) GetMsgType() MsgType {
	return INDEXER_CREATE_INDEX_DDL
}

func (m *MsgCreateIndex) GetIndexInst() common.IndexInst {
	return m.indexInst
}

func (m *MsgCreateIndex) GetResponseChannel() MsgChannel {
	return m.respCh
}

func (m *MsgCreateIndex) GetString() string {

	str := "\n\tMessage: MsgCreateIndex"
	str += fmt.Sprintf("\n\tIndex: %v", m.indexInst)
	return str
}

//INDEXER_DROP_INDEX_DDL
type MsgDropIndex struct {
	indexInstId common.IndexInstId
	respCh      MsgChannel
}

func (m *MsgDropIndex) GetMsgType() MsgType {
	return INDEXER_DROP_INDEX_DDL
}

func (m *MsgDropIndex) GetIndexInstId() common.IndexInstId {
	return m.indexInstId
}

func (m *MsgDropIndex) GetResponseChannel() MsgChannel {
	return m.respCh
}

func (m *MsgDropIndex) GetString() string {

	str := "\n\tMessage: MsgDropIndex"
	str += fmt.Sprintf("\n\tIndex: %v", m.indexInstId)
	return str
}

//SCAN_COORD_SCAN_INDEX
type MsgScanIndex struct {
	scanId      int64
	indexInstId common.IndexInstId
	stopch      StopChannel
	p           ScanParams
	resCh       chan Value
	errCh       chan Message
	countCh     chan uint64
}

func (m *MsgScanIndex) GetMsgType() MsgType {
	return SCAN_COORD_SCAN_INDEX
}

func (m *MsgScanIndex) GetIndexInstId() common.IndexInstId {
	return m.indexInstId
}

func (m *MsgScanIndex) GetScanId() int64 {
	return m.scanId
}

func (m *MsgScanIndex) GetResultChannel() chan Value {
	return m.resCh
}

func (m *MsgScanIndex) GetErrorChannel() chan Message {
	return m.errCh
}

func (m *MsgScanIndex) GetCountChannel() chan uint64 {
	return m.countCh
}

func (m *MsgScanIndex) GetStopChannel() StopChannel {
	return m.stopch
}

func (m *MsgScanIndex) GetParams() ScanParams {
	return m.p
}
