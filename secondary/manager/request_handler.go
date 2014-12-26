// Copyright (c) 2014 Couchbase, Inc.

// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.
package manager

import (
	"encoding/json"
	"github.com/couchbase/indexing/secondary/common"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
	"github.com/couchbaselabs/goprotobuf/proto"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"sync"
)

///////////////////////////////////////////////////////
// Type Definition
// -- This is the same as cbq-bridge-defs.  Moving
// to index manager package.  Will need to prune these
// to narrow down only for DDL.
///////////////////////////////////////////////////////

type IndexInfo struct {
	Name      string   `json:"name,omitempty"`
	Bucket    string   `json:"bucket,omitempty"`
	DefnID    string   `json:"defnID, omitempty"`
	Using     string   `json:"using,omitempty"`
	Exprtype  string   `json:"exprType,omitempty"`
	PartnExpr string   `json:"partnExpr,omitempty"`
	SecExprs  []string `json:"secExprs,omitempty"`
	WhereExpr string   `json:"whereExpr,omitempty"`
	IsPrimary bool     `json:"isPrimary,omitempty"`
}

type RequestType string

const (
	CREATE RequestType = "create"
	DROP   RequestType = "drop"
	GET    RequestType = "get"
)

type IndexRequest struct {
	Version uint64      `json:"version,omitempty"`
	Type    RequestType `json:"type,omitempty"`
	Index   IndexInfo   `json:"index,omitempty"`
}

type IndexResponse struct {
	Version uint64         `json:"version,omitempty"`
	Status  ResponseStatus `json:"status,omitempty"`
	Indexes []IndexInfo    `json:"indexes,omitempty"`
	Errors  []IndexError   `json:"errors,omitempty"`
}

type IndexError struct {
	Code string `json:"code,omitempty"`
	Msg  string `json:"msg,omitempty"`
}

type TopologyRequest struct {
	Version uint64      `json:"version,omitempty"`
	Type    RequestType `json:"type,omitempty"`
	Bucket  string      `json:"bucket,omitempty"`
}

type TopologyResponse struct {
	Version   uint64         `json:"version,omitempty"`
	Status    ResponseStatus `json:"status,omitempty"`
	Instances []byte         `json:"instance,omitempty"`
	Errors    []IndexError   `json:"errors,omitempty"`
}

type ResponseStatus string

const (
	RESP_SUCCESS ResponseStatus = "success"
	RESP_ERROR   ResponseStatus = "error"
)

type requestHandler struct {
	mgr      *IndexManager
	listener net.Listener
	mutex    sync.Mutex
	isClosed bool
}

type httpHandler struct {
	initializer sync.Once
	mgr         *IndexManager
}

var handler httpHandler

///////////////////////////////////////////////////////
// Package Local Function
///////////////////////////////////////////////////////

func NewRequestHandler(mgr *IndexManager) (*requestHandler, error) {

	r := &requestHandler{mgr: mgr,
		isClosed: false,
		listener: nil}
	go r.run()
	return r, nil
}

func (m *requestHandler) close() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.isClosed {
		return
	}

	m.isClosed = true

	if m.listener != nil {
		m.listener.Close()
	}
}

func (m *httpHandler) createIndexRequest(w http.ResponseWriter, r *http.Request) {

	// convert request
	request := convertIndexRequest(r)
	if request == nil {
		ierr := IndexError{Code: string(RESP_ERROR),
			Msg: "RequestHandler::createIndexRequest: Unable to convert request"}

		res := IndexResponse{
			Status: RESP_ERROR,
			Errors: []IndexError{ierr},
		}

		sendResponse(w, res)
		return
	}

	indexinfo := request.Index

	defnID := uint64(rand.Int())
	if len(indexinfo.DefnID) != 0 {
		if num, err := strconv.ParseUint(indexinfo.DefnID, 10, 64); err == nil {
			defnID = num
		}
	}

	// create an in-memory index definition
	// TODO : WhereExpr
	idxDefn := &common.IndexDefn{
		DefnId:          common.IndexDefnId(defnID),
		Name:            indexinfo.Name,
		Using:           common.ForestDB,
		Bucket:          indexinfo.Bucket,
		IsPrimary:       indexinfo.IsPrimary,
		SecExprs:        indexinfo.SecExprs,
		ExprType:        common.N1QL,
		PartitionScheme: common.SINGLE,
		PartitionKey:    indexinfo.PartnExpr}

	// call the index manager to handle the DDL
	common.Debugf("RequestHandler::createIndexRequest: invoke IndexManager for create index bucket %s name %s",
		indexinfo.Bucket, indexinfo.Name)

	err := m.mgr.HandleCreateIndexDDL(idxDefn)
	if err == nil {
		// No error, return success
		res := IndexResponse{
			Status:  RESP_SUCCESS,
			Indexes: []IndexInfo{indexinfo},
		}
		sendResponse(w, res)
	} else {
		// report failure
		ierr := IndexError{Code: string(RESP_ERROR),
			Msg: err.Error()}

		res := IndexResponse{
			Status: RESP_ERROR,
			Errors: []IndexError{ierr},
		}
		sendResponse(w, res)
	}
}

func (m *httpHandler) dropIndexRequest(w http.ResponseWriter, r *http.Request) {

	// convert request
	request := convertIndexRequest(r)
	if request == nil {
		ierr := IndexError{Code: string(RESP_ERROR),
			Msg: "RequestHandler::dropIndexRequest: Unable to convert request"}

		res := IndexResponse{
			Status: RESP_ERROR,
			Errors: []IndexError{ierr},
		}

		sendResponse(w, res)
		return
	}

	// call the index manager to handle the DDL
	indexinfo := request.Index
	id, err := indexDefnId(indexinfo.DefnID)
	if err == nil {
		err = m.mgr.HandleDeleteIndexDDL(id)
	}
	
	if err == nil {
		// No error, return success
		res := IndexResponse{
			Status: RESP_SUCCESS,
		}
		sendResponse(w, res)
	} else {
		// report failure
		ierr := IndexError{Code: string(RESP_ERROR),
			Msg: err.Error()}

		res := IndexResponse{
			Status: RESP_ERROR,
			Errors: []IndexError{ierr},
		}
		sendResponse(w, res)
	}
}

func (m *httpHandler) getTopologyRequest(w http.ResponseWriter, r *http.Request) {
	// convert request
	request := convertTopologyRequest(r)
	if request == nil {
		ierr := IndexError{Code: string(RESP_ERROR),
			Msg: "RequestHandler::getTopologyRequest: Unable to convert request"}

		res := TopologyResponse{
			Status: RESP_ERROR,
			Errors: []IndexError{ierr},
		}

		sendResponse(w, res)
		return
	}

	// call the index manager to handle the DDL
	instances, _, err := GetTopologyAsInstanceProtoMsg(m.mgr, request.Bucket, SCAN_REQUEST_PORT)
	protoInsts := &protobuf.Instances{Instances: instances}
	data, err := proto.Marshal(protoInsts)
	if err == nil {

		// No error, return success
		res := TopologyResponse{
			Status:    RESP_SUCCESS,
			Instances: data,
		}
		sendResponse(w, res)
	} else {
		// report failure
		ierr := IndexError{Code: string(RESP_ERROR),
			Msg: err.Error()}

		res := TopologyResponse{
			Status: RESP_ERROR,
			Errors: []IndexError{ierr},
		}
		sendResponse(w, res)
	}
}

///////////////////////////////////////////////////////
// Private Function
///////////////////////////////////////////////////////

func convertIndexRequest(r *http.Request) *IndexRequest {
	req := IndexRequest{}
	buf := make([]byte, r.ContentLength)
	common.Debugf("RequestHandler::convertIndexRequest: request content length %d", len(buf))

	// Body will be non-null but can return EOF if being empty
	if n, err := r.Body.Read(buf); err != nil && int64(n) != r.ContentLength {
		common.Debugf("RequestHandler::convertIndexRequest: unable to read request body, err %v", err)
		return nil
	}

	if err := json.Unmarshal(buf, &req); err != nil {
		common.Debugf("RequestHandler::convertIndexRequest: unable to unmarshall request body. Buf = %s, err %v", buf, err)
		return nil
	}

	return &req
}

func convertTopologyRequest(r *http.Request) *TopologyRequest {
	req := TopologyRequest{}
	buf := make([]byte, r.ContentLength)
	common.Debugf("RequestHandler::convertIndexRequest: request content length %d", len(buf))

	// Body will be non-null but can return EOF if being empty
	if n, err := r.Body.Read(buf); err != nil && int64(n) != r.ContentLength {
		common.Debugf("RequestHandler::convertIndexRequest: unable to read request body, err %v", err)
		return nil
	}

	if err := json.Unmarshal(buf, &req); err != nil {
		common.Debugf("RequestHandler::convertIndexRequest: unable to unmarshall request body. Buf = %s, err %v", buf, err)
		return nil
	}

	return &req
}

func sendResponse(w http.ResponseWriter, res interface{}) {
	common.Debugf("RequestHandler::sendResponse: sending response back to caller")

	header := w.Header()
	header["Content-Type"] = []string{"application/json"}

	if buf, err := json.Marshal(&res); err == nil {
		w.Write(buf)
	} else {
		// note : buf is nil if err != nil
		sendHttpError(w, "RequestHandler::sendResponse: Unable to marshall response", http.StatusInternalServerError)
	}
}

func sendHttpError(w http.ResponseWriter, reason string, code int) {
	http.Error(w, reason, code)
}

func (r *requestHandler) run() {

	handler.initializer.Do(func() {
		defer func() {
			if r := recover(); r != nil {
				common.Warnf("error encountered when registering http createIndex handler : %v.  Ignored.\n", r)
			}
		}()

		http.HandleFunc("/createIndex", handler.createIndexRequest)
		http.HandleFunc("/dropIndex", handler.dropIndexRequest)
		http.HandleFunc("/getTopology", handler.getTopologyRequest)
	})

	handler.mgr = r.mgr

	li, err := net.Listen("tcp", INDEX_DDL_HTTP_ADDR)
	if err != nil {
		// TODO: abort
		common.Warnf("requestHandler.run() : HTTP Server Listen fails, err %v", err)
	}
	r.listener = li

	if err := http.Serve(li, nil); err != nil {
		// TODO: abort
		common.Warnf("requestHandler.run() : HTTP Server Serve fails, err %v", err)
	}

	common.Debugf("requestHandler.run() : Request Handler HTTP server running")
}
