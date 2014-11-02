// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package indexer

import (
	"fmt"
	"github.com/couchbase/indexing/secondary/common"
)

//Protocol struct definitions for Cbq Bridge

// Every index ever created and maintained by this package will have an
// associated index-info structure.
type IndexInfo struct {
	Name       string           `json:"name,omitempty"`       // Name of the index
	Uuid       string           `json:"uuid,omitempty"`       // unique id for every index
	Using      common.IndexType `json:"using,omitempty"`      // indexing algorithm
	OnExprList []string         `json:"onExprList,omitempty"` // expression list
	Bucket     string           `json:"bucket,omitempty"`     // bucket name
	IsPrimary  bool             `json:"isPrimary,omitempty"`
	Exprtype   common.ExprType  `json:"exprType,omitempty"`
}

type RequestType string

const (
	CREATE RequestType = "create"
	DROP   RequestType = "drop"
	LIST   RequestType = "list"
	NOTIFY RequestType = "notify"
	NODES  RequestType = "nodes"
)

// All API accept IndexRequest structure and returns IndexResponse structure.
// If application is written in Go, and compiled with `indexing` package then
// they can choose the access the underlying interfaces directly.
type IndexRequest struct {
	Type       RequestType `json:"type,omitempty"`
	Index      IndexInfo   `json:"index,omitempty"`
	ServerUuid string      `json:"serverUuid,omitempty"`
}

//RESPONSE DATA FORMATS
type ResponseStatus string

const (
	RESP_SUCCESS       ResponseStatus = "success"
	RESP_ERROR         ResponseStatus = "error"
	RESP_INVALID_CACHE ResponseStatus = "invalid_cache"
)

type IndexRow struct {
	Key   []byte `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

type IndexError struct {
	Code string `json:"code,omitempty"`
	Msg  string `json:"msg,omitempty"`
}

type IndexMetaResponse struct {
	Status     ResponseStatus `json:"status,omitempty"`
	Indexes    []IndexInfo    `json:"indexes,omitempty"`
	ServerUuid string         `json:"serverUuid,omitempty"`
	Nodes      []NodeInfo     `json:"nodes,omitempty"`
	Errors     []IndexError   `json:"errors,omitempty"`
}

type IndexScanResponse struct {
	Status    ResponseStatus `json:"status,omitempty"`
	TotalRows uint64         `json:"totalrows,omitempty"`
	Rows      []IndexRow     `json:"rows,omitempty"`
	Errors    []IndexError   `json:"errors,omitempty"`
}

//Indexer Node Info
type NodeInfo struct {
	IndexerURL string `json:"indexerURL,omitempty"`
}

func (idx IndexInfo) String() string {

	str := "\n"
	str += fmt.Sprintf("\tName : %v\n", idx.Name)
	str += fmt.Sprintf("\tUuid : %v\n", idx.Uuid)
	str += fmt.Sprintf("\tUsing: %v\n", idx.Using)
	str += fmt.Sprintf("\tOnExprListName : %v\n", idx.OnExprList)
	str += fmt.Sprintf("\tBucket: %v\n", idx.Bucket)
	str += fmt.Sprintf("\tIsPrimary: %v\n", idx.IsPrimary)
	str += fmt.Sprintf("\tExprtype: %v\n", idx.Exprtype)
	return str

}
