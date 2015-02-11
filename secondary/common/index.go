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
	"encoding/json"
	"fmt"
)

type IndexKey []byte

type IndexerId string

const INDEXER_ID_NIL = IndexerId("")

// SecondaryKey is secondary-key in the shape of - [ val1, val2, ..., valN ]
// where value can be any golang data-type that can be serialized into JSON.
// simple-key shall be shaped as [ val ]
type SecondaryKey []interface{}

// IndexStatistics captures statistics for a range or a single key.
type IndexStatistics interface {
	Count() (int64, error)
	MinKey() (SecondaryKey, error)
	MaxKey() (SecondaryKey, error)
	DistinctCount() (int64, error)
	Bins() ([]IndexStatistics, error)
}

type IndexDefnId uint64
type IndexInstId uint64

type ExprType string

const (
	JavaScript ExprType = "JavaScript"
	N1QL                = "N1QL"
)

type PartitionScheme string

const (
	KEY    PartitionScheme = "KEY"
	HASH                   = "HASH"
	RANGE                  = "RANGE"
	TEST                   = "TEST"
	SINGLE                 = "SINGLE"
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
	INDEX_STATE_CREATED IndexState = iota
	// Index is stream is ready
	INDEX_STATE_READY
	//Initial Build In Progress
	INDEX_STATE_INITIAL
	//Catchup In Progress
	INDEX_STATE_CATCHUP
	//Maitenance Stream
	INDEX_STATE_ACTIVE
	//Drop Index Processed
	INDEX_STATE_DELETED
	//Error State
	INDEX_STATE_ERROR
	// Nil State (used for no-op / invalid)
	INDEX_STATE_NIL
)

func (s IndexState) String() string {

	switch s {
	case INDEX_STATE_CREATED:
		return "INDEX_STATE_CREATED"
	case INDEX_STATE_READY:
		return "INDEX_STATE_READY"
	case INDEX_STATE_INITIAL:
		return "INDEX_STATE_INITIAL"
	case INDEX_STATE_CATCHUP:
		return "INDEX_STATE_CATCHUP"
	case INDEX_STATE_ACTIVE:
		return "INDEX_STATE_ACTIVE"
	case INDEX_STATE_DELETED:
		return "INDEX_STATE_DELETED"
	case INDEX_STATE_ERROR:
		return "INDEX_STATE_ERROR"
	default:
		return "INDEX_STATE_UNKNOWN"
	}
}

// Consistency definition for index-scan queries.
type Consistency byte

const (
	// AnyConsistency indexer would return the most current
	// data available at the moment.
	AnyConsistency Consistency = iota + 1

	// SessionConsistency indexer would query the latest timestamp
	// from each KV node. It will ensure that the scan result is at
	// least as recent as the KV timestamp. In other words, this
	// option ensures the query result is at least as recent as what
	// the user session has observed so far.
	SessionConsistency

	// QueryConsistency indexer would accept a timestamp vector,
	// and make sure to return a stable data-set that is atleast as
	// recent as the timestamp-vector.
	QueryConsistency
)

func (cons Consistency) String() string {
	switch cons {
	case AnyConsistency:
		return "ANY_CONSISTENCY"
	case SessionConsistency:
		return "SESSION_CONSISTENCY"
	case QueryConsistency:
		return "QUERY_CONSISTENCY"
	default:
		return "UNKNOWN_CONSISTENCY"
	}
}

//IndexDefn represents the index definition as specified
//during CREATE INDEX
type IndexDefn struct {
	DefnId          IndexDefnId     `json:"defnId,omitempty"`
	Name            string          `json:"name,omitempty"`
	Using           IndexType       `json:"using,omitempty"`
	Bucket          string          `json:"bucket,omitempty"`
	IsPrimary       bool            `json:"isPrimary,omitempty"`
	SecExprs        []string        `json:"secExprs,omitempty"`
	ExprType        ExprType        `json:"exprType,omitempty"`
	PartitionScheme PartitionScheme `json:"partitionScheme,omitempty"`
	PartitionKey    string          `json:"partitionKey,omitempty"`
	WhereExpr       string          `json:"where,omitempty"`
	Deferred        bool            `json:"deferred,omitempty"`
	Nodes           []string        `json:"nodes,omitempty"`
}

//IndexInst is an instance of an Index(aka replica)
type IndexInst struct {
	InstId IndexInstId
	Defn   IndexDefn
	State  IndexState
	Stream StreamId
	Pc     PartitionContainer
	Error  string
}

//IndexInstMap is a map from IndexInstanceId to IndexInstance
type IndexInstMap map[IndexInstId]IndexInst

func (idx IndexDefn) String() string {

	str := fmt.Sprintf("DefnId: %v ", idx.DefnId)
	str += fmt.Sprintf("Name: %v ", idx.Name)
	str += fmt.Sprintf("Using: %v ", idx.Using)
	str += fmt.Sprintf("Bucket: %v ", idx.Bucket)
	str += fmt.Sprintf("IsPrimary: %v ", idx.IsPrimary)
	str += fmt.Sprintf("\n\t\tSecExprs: %v ", idx.SecExprs)
	str += fmt.Sprintf("\n\t\tPartitionScheme: %v ", idx.PartitionScheme)
	str += fmt.Sprintf("PartitionKey: %v ", idx.PartitionKey)
	str += fmt.Sprintf("WhereExpr: %v ", idx.WhereExpr)
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
	NIL_STREAM StreamId = iota
	MAINT_STREAM
	CATCHUP_STREAM
	INIT_STREAM
	ALL_STREAMS
)

func (s StreamId) String() string {

	switch s {
	case MAINT_STREAM:
		return "MAINT_STREAM"
	case CATCHUP_STREAM:
		return "CATCHUP_STREAM"
	case INIT_STREAM:
		return "INIT_STREAM"
	default:
		return "INVALID_STREAM"
	}
}

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

func MarshallIndexDefn(defn *IndexDefn) ([]byte, error) {

	buf, err := json.Marshal(&defn)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func UnmarshallIndexDefn(data []byte) (*IndexDefn, error) {

	defn := new(IndexDefn)
	if err := json.Unmarshal(data, defn); err != nil {
		return nil, err
	}

	return defn, nil
}

func NewIndexDefnId() (IndexDefnId, error) {
	uuid, err := NewUUID()
	if err != nil {
		return IndexDefnId(0), err
	}

	return IndexDefnId(uuid.Uint64()), nil
}
