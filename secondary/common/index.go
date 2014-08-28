// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package common

import (
	"fmt"
)

type IndexKey [][]byte

type IndexDefnId uint64
type IndexInstId uint64

type ExprType string

const (
	JavaScript ExprType = "JavaScript"
	N1QL                = "N1QL"
)

type PartitionScheme string

const (
	KEY   PartitionScheme = "KEY"
	HASH                  = "HASH"
	RANGE                 = "RANGE"
	TEST                  = "TEST"
)

type IndexType string

const (
	View     IndexType = "View"
	Llrb               = "Llrb"
	LevelDB            = "LevelDB"
	ForestDB           = "ForestDB"
)

type IndexState int

const (
	//Create Index Processed
	INDEX_STATE_CREATED IndexState = 0
	//Initial Build In Progress
	INDEX_STATE_INITIAL = 1
	//Catchup In Progress
	INDEX_STATE_CATCHUP = 2
	//Maitenance Stream
	INDEX_STATE_ACTIVE = 3
	//Drop Index Processed
	INDEX_STATE_DELETED = 4
)

//IndexDefn represents the index definition as specified
//during CREATE INDEX
type IndexDefn struct {
	DefnId          IndexDefnId
	Name            string    // Name of the index
	Using           IndexType // indexing algorithm
	Bucket          string    // bucket name
	IsPrimary       bool
	OnExprList      []string // expression list
	ExprType        ExprType
	PartitionScheme PartitionScheme
	PartitionKey    string
}

//IndexInst is an instance of an Index(aka replica)
type IndexInst struct {
	InstId IndexInstId
	Defn   IndexDefn
	State  IndexState
	Stream StreamId
	Pc     PartitionContainer
}

//IndexInstMap is a map from IndexInstanceId to IndexInstance
type IndexInstMap map[IndexInstId]IndexInst

func (idx IndexDefn) String() string {

	str := fmt.Sprintf("DefnId: %v ", idx.DefnId)
	str += fmt.Sprintf("Name: %v ", idx.Name)
	str += fmt.Sprintf("Using: %v ", idx.Using)
	str += fmt.Sprintf("Bucket: %v ", idx.Bucket)
	str += fmt.Sprintf("IsPrimary: %v ", idx.IsPrimary)
	str += fmt.Sprintf("\n\t\tOnExprList: %v ", idx.OnExprList)
	str += fmt.Sprintf("\n\t\tPartitionScheme: %v ", idx.PartitionScheme)
	str += fmt.Sprintf("PartitionKey: %v ", idx.PartitionKey)
	return str

}
func (idx IndexInst) String() string {

	str := "\n"
	str += fmt.Sprintf("\tInstId: %v\n", idx.InstId)
	str += fmt.Sprintf("\tDefn: %v\n", idx.Defn)
	str += fmt.Sprintf("\tState: %v\n", idx.State)
	str += fmt.Sprintf("\tStream: %v\n", idx.Stream)
	str += fmt.Sprintf("\tPartitionContainer: %v", idx.Pc)
	return str

}

//StreamId represents the possible mutation streams
type StreamId uint16

const (
	MAINT_STREAM StreamId = iota
	MAINT_CATCHUP_STREAM
	INIT_STREAM
	INIT_CATCHUP_STREAM
	MAX_STREAMS
)

func (idx IndexInstMap) String() string {

	str := "\n"
	for i, index := range idx {
		str += fmt.Sprintf("\tInstanceId: %v ", i)
		str += fmt.Sprintf("Name: %v ", index.Defn.Name)
		str += fmt.Sprintf("Bucket: %v ", index.Defn.Bucket)
		str += fmt.Sprintf("State: %v ", index.State)
		str += fmt.Sprintf("Stream: %v ", index.Stream)
		str += "\n"
	}
	return str

}

func CopyIndexInstMap(inMap IndexInstMap) IndexInstMap {

	outMap := make(IndexInstMap)
	for k, v := range inMap {
		outMap[k] = v
	}
	return outMap
}
