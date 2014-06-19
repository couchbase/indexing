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
	"github.com/couchbase/indexing/secondary/common"
)

type MsgType int16

const (

	//General Messages
	SUCCESS = iota
	ERROR
	TIMESTAMP

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
	MUT_MGR_OPEN_STREAM
	MUT_MGR_ADD_INDEX_LIST_TO_STREAM
	MUT_MGR_REMOVE_INDEX_LIST_FROM_STREAM
	MUT_MGR_CLOSE_STREAM
	MUT_MGR_CLEANUP_STREAM
	MUT_MGR_PERSIST_MUTATION_QUEUE
	MUT_MGR_DRAIN_MUTATION_QUEUE
	MUT_MGR_GET_MUTATION_QUEUE_HWT
	MUT_MGR_GET_MUTATION_QUEUE_LWT
	MUT_MGR_SHUTDOWN
	MUT_MGR_FLUSH_DONE

	//TIMEKEEPER
	TK_STREAM_START
	TK_STREAM_SHOP
	TK_SHUTDOWN
	TK_STABILITY_TIMESTAMP

	//STORAGE_MANAGER
	STORAGE_MGR_SHUTDOWN

	UPDATE_INDEX_INSTANCE_MAP
	UPDATE_INDEX_PARTITION_MAP
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
	mType MsgType
	err   Error
}

func (m *MsgError) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgError) GetError() Error {
	return m.err
}

//Success Message
type MsgSuccess struct {
}

func (m *MsgSuccess) GetMsgType() MsgType {
	return SUCCESS
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
	streamId StreamId
	meta     *MutationMeta
}

func (m *MsgStream) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgStream) GetMutationMeta() *MutationMeta {
	return m.meta
}

func (m *MsgStream) GetStreamId() StreamId {
	return m.streamId
}

//Stream Error Message
type MsgStreamError struct {
	streamId StreamId
	err      Error
}

func (m *MsgStreamError) GetMsgType() MsgType {
	return STREAM_READER_ERROR
}

func (m *MsgStreamError) GetStreamId() StreamId {
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

//MUT_MGR_CREATE_STREAM
//MUT_MGR_ADD_INDEX_LIST_TO_STREAM
//MUT_MGR_REMOVE_INDEX_LIST_FROM_STREAM
//MUT_MGR_CLOSE_STREAM
//MUT_MGR_CLEANUP_STREAM
type MsgMutMgrStreamUpdate struct {
	mType     MsgType
	streamId  StreamId
	indexList []common.IndexInst
}

func (m *MsgMutMgrStreamUpdate) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgMutMgrStreamUpdate) GetStreamId() StreamId {
	return m.streamId
}

func (m *MsgMutMgrStreamUpdate) GetIndexList() []common.IndexInst {
	return m.indexList
}

//MUT_MGR_PERSIST_MUTATION_QUEUE
//MUT_MGR_DISCARD_MUTATION_QUEUE
type MsgMutMgrFlushMutationQueue struct {
	mType    MsgType
	bucket   string
	streamId StreamId
	ts       Timestamp
}

func (m *MsgMutMgrFlushMutationQueue) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgMutMgrFlushMutationQueue) GetBucket() string {
	return m.bucket
}

func (m *MsgMutMgrFlushMutationQueue) GetStreamId() StreamId {
	return m.streamId
}

func (m *MsgMutMgrFlushMutationQueue) GetTimestamp() Timestamp {
	return m.ts
}

//MUT_MGR_GET_MUTATION_QUEUE_HWT
//MUT_MGR_GET_MUTATION_QUEUE_LWT
type MsgMutMgrGetTimestamp struct {
	mType    MsgType
	bucket   string
	streamId StreamId
}

func (m *MsgMutMgrGetTimestamp) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgMutMgrGetTimestamp) GetBucket() string {
	return m.bucket
}

func (m *MsgMutMgrGetTimestamp) GetStreamId() StreamId {
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

//MUT_MGR_FLUSH_DONE
type MsgMutMgrFlushDone struct {
	ts       Timestamp
	streamId StreamId
	bucket   string
}

func (m *MsgMutMgrFlushDone) GetMsgType() MsgType {
	return MUT_MGR_FLUSH_DONE
}

func (m *MsgMutMgrFlushDone) GetTS() Timestamp {
	return m.ts
}

func (m *MsgMutMgrFlushDone) GetStreamId() StreamId {
	return m.streamId
}

func (m *MsgMutMgrFlushDone) GetBucket() string {
	return m.bucket
}

//TK_STREAM_START
//TK_STREAM_SHOP
type MsgTKStreamUpdate struct {
	mType         MsgType
	streamId      StreamId
	indexInstList []common.IndexInst
}

func (m *MsgTKStreamUpdate) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgTKStreamUpdate) GetStreamId() StreamId {
	return m.streamId
}

func (m *MsgTKStreamUpdate) GetIndexList() []common.IndexInst {
	return m.indexInstList
}

//TK_STABILITY_TIMESTAMP
type MsgTKStabilityTS struct {
	ts       Timestamp
	streamId StreamId
	bucket   string
}

func (m *MsgTKStabilityTS) GetMsgType() MsgType {
	return TK_STABILITY_TIMESTAMP
}

func (m *MsgTKStabilityTS) GetStreamId() StreamId {
	return m.streamId
}

func (m *MsgTKStabilityTS) GetBucket() string {
	return m.bucket
}

func (m *MsgTKStabilityTS) GetTimestamp() Timestamp {
	return m.ts
}
