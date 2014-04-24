// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package indexer

type MsgType int16

const (

	//General Messages
	SUCCESS = iota
	ERROR

	//Component specific messages
	//TODO
)

type Message interface {
	GetMsgType() MsgType
}

//Error Message
type MsgError struct {
	mType MsgType
	err   error
}

func (m *MsgError) GetMsgType() MsgType {
	return m.mType
}

func (m *MsgError) GetError() error {
	return m.err
}

//Success Message
type MsgSuccess struct {
}

func (m *MsgSuccess) GetMsgType() MsgType {
	return SUCCESS
}
