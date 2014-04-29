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

	//Component specific messages

	//STREAM_READER
	STREAM_READER_STREAM_SHUTDOWN
	STREAM_READER_STREAM_DROP_DATA
	STREAM_READER_STREAM_BEGIN
	STREAM_READER_STREAM_END
	STREAM_READER_PANIC
	STREAM_READER_UPDATE_QUEUE_MAP
)

type Message interface {
	GetMsgType() MsgType
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

//Stream Reader Message
type MsgStream struct {
	mType    MsgType
	streamId StreamId
	mutation *common.Mutation
}

func (m *MsgStream) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgStream) GetMutationMsg() *common.Mutation {
	return m.mutation
}

func (m *MsgStream) GetStreamId() StreamId {
	return m.streamId
}

//Stream Panic Message
type MsgStreamPanic struct {
	streamId StreamId
	err      Error
}

func (m *MsgStreamPanic) GetMsgType() MsgType {
	return STREAM_READER_PANIC
}

func (m *MsgStreamPanic) GetStreamId() StreamId {
	return m.streamId
}

func (m *MsgStreamPanic) GetError() Error {
	return m.err
}

//Stream Update Index Queue Message
type MsgUpdateIndexQueue struct {
	indexQueueMap IndexQueueMap
}

func (m *MsgUpdateIndexQueue) GetMsgType() MsgType {
	return STREAM_READER_UPDATE_QUEUE_MAP
}

func (m *MsgUpdateIndexQueue) GetIndexQueueMap() IndexQueueMap {
	return m.indexQueueMap
}
