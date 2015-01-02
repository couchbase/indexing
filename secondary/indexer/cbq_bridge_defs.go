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
)

type RequestType string

const (
	CREATE RequestType = "create"
	DROP   RequestType = "drop"
	LIST   RequestType = "list"
)

//RESPONSE DATA FORMATS
type ResponseStatus string

const (
	RESP_SUCCESS       ResponseStatus = "success"
	RESP_ERROR         ResponseStatus = "error"
	RESP_INVALID_CACHE ResponseStatus = "invalid_cache"
)

type IndexError struct {
	Code string `json:"code,omitempty"`
	Msg  string `json:"msg,omitempty"`
}

type IndexRequest struct {
	Version uint64      `json:"version,omitempty"`
	Type    RequestType `json:"type,omitempty"`
	Index   IndexInfo   `json:"index,omitempty"`
}

// Every index ever created and maintained by this package will have an
// associated index-info structure.
type IndexInfo struct {
	Name      string   `json:"name,omitempty"`
	Bucket    string   `json:"bucket,omitempty"`
	DefnID    uint64   `json:"defnID, omitempty"`
	Using     string   `json:"using,omitempty"`
	Exprtype  string   `json:"exprType,omitempty"`
	PartnExpr string   `json:"partnExpr,omitempty"`
	SecExprs  []string `json:"secExprs,omitempty"`
	WhereExpr string   `json:"whereExpr,omitempty"`
	IsPrimary bool     `json:"isPrimary,omitempty"`
}

type IndexMetaResponse struct {
	Version uint64         `json:"version,omitempty"`
	Status  ResponseStatus `json:"status,omitempty"`
	Indexes []IndexInfo    `json:"indexes,omitempty"`
	Errors  []IndexError   `json:"errors,omitempty"`
}

type ScanType string

const (
	COUNT      ScanType = "count"
	EXISTS     ScanType = "exists"
	LOOKUP     ScanType = "lookup"
	RANGESCAN  ScanType = "rangeScan"
	FULLSCAN   ScanType = "fullScan"
	RANGECOUNT ScanType = "rangeCount"
)

func (idx IndexInfo) String() string {
	str := "\n"
	str += fmt.Sprintf("\tName: %v\n", idx.Name)
	str += fmt.Sprintf("\tDefnID: %v\n", idx.DefnID)
	str += fmt.Sprintf("\tUsing: %v\n", idx.Using)
	str += fmt.Sprintf("\tSecExprs: %v\n", idx.SecExprs)
	str += fmt.Sprintf("\tBucket: %v\n", idx.Bucket)
	str += fmt.Sprintf("\tIsPrimary: %v\n", idx.IsPrimary)
	str += fmt.Sprintf("\tExprtype: %v\n", idx.Exprtype)
	return str

}
