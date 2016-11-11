package client

//TODO move all these defs to common

import common "github.com/couchbase/indexing/secondary/common"

type RequestType string

const (
	CREATE RequestType = "create"
	DROP   RequestType = "drop"
	BUILD  RequestType = "build"
)

type IndexRequest struct {
	Version  uint64                 `json:"version,omitempty"`
	Type     RequestType            `json:"type,omitempty"`
	Index    common.IndexDefn       `json:"index,omitempty"`
	IndexIds IndexIdList            `json:indexIds,omitempty"`
	Plan     map[string]interface{} `json:plan,omitempty"`
}

type IndexResponse struct {
	Version uint64 `json:"version,omitempty"`
	Code    string `json:"code,omitempty"`
	Error   string `json:"error,omitempty"`
}

type IndexIdList struct {
	DefnIds []uint64 `json:"defnIds,omitempty"`
}

//
// Response
//

const (
	RESP_SUCCESS string = "success"
	RESP_ERROR   string = "error"
)
