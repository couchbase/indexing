package manager

import (
	"encoding/json"
	gometa "github.com/couchbase/gometa/common"
	"github.com/couchbase/indexing/secondary/common"
	"math/rand"
	"net"
	"net/http"
	"sync"
)

///////////////////////////////////////////////////////
// Type Definition
// -- This is the same as cbq-bridge-defs.  Moving
// to index manager package.  Will need to prune these
// to narrow down only for DDL.
///////////////////////////////////////////////////////

// Every index ever created and maintained by this package will have an
// associated index-info structure.
type IndexInfo struct {
	Name      string           `json:"name,omitempty"`       // Name of the index
	Uuid      string           `json:"uuid,omitempty"`       // unique id for every index
	Using     common.IndexType `json:"using,omitempty"`      // indexing algorithm
	SecExprs  []string         `json:"onExprList,omitempty"` // expression list
	Bucket    string           `json:"bucket,omitempty"`     // bucket name
	IsPrimary bool             `json:"isPrimary,omitempty"`
	Exprtype  common.ExprType  `json:"exprType,omitempty"`
}

type RequestType string

const (
	CREATE RequestType = "create"
	DROP   RequestType = "drop"
	LIST   RequestType = "list"
	NOTIFY RequestType = "notify"
	NODES  RequestType = "nodes"
	SCAN   RequestType = "scan"
	STATS  RequestType = "stats"
)

// All API accept IndexRequest structure and returns IndexResponse structure.
// If application is written in Go, and compiled with `indexing` package then
// they can choose the access the underlying interfaces directly.
type IndexRequest struct {
	Type       RequestType `json:"type,omitempty"`
	Index      IndexInfo   `json:"index,omitempty"`
	ServerUuid string      `json:"serverUuid,omitempty"`
	Params     QueryParams `json:"params,omitempty"`
}

// URL encoded query params
type QueryParams struct {
	ScanType  ScanType  `json:"scanType,omitempty"`
	Low       [][]byte  `json:"low,omitempty"`
	High      [][]byte  `json:"high,omitempty"`
	Inclusion Inclusion `json:"inclusion,omitempty"`
	Limit     int64     `json:"limit,omitempty"`
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

//RESPONSE DATA FORMATS
type ResponseStatus string

const (
	RESP_SUCCESS       ResponseStatus = "success"
	RESP_ERROR         ResponseStatus = "error"
	RESP_INVALID_CACHE ResponseStatus = "invalid_cache"
)

type IndexRow struct {
	Key   [][]byte `json:"key,omitempty"`
	Value string   `json:"value,omitempty"`
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

// Inclusion
type Inclusion int

const (
	Neither Inclusion = iota
	Low
	High
	Both
)

type requestHandler struct {
	mgr      *IndexManager
	listener net.Listener
	mutex    sync.Mutex
	isClosed bool
}

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

func (m *requestHandler) createIndexRequest(w http.ResponseWriter, r *http.Request) {

	// convert request
	idxRequest := convertRequest(r)
	if idxRequest == nil {
		ierr := IndexError{Code: string(RESP_ERROR),
			Msg: "RequestHandler::createIndexRequest: Unable to convert request"}

		res := IndexMetaResponse{
			Status: RESP_ERROR,
			Errors: []IndexError{ierr},
		}

		sendResponse(w, res)
		return
	}

	// create an in-memory index definition
	indexinfo := idxRequest.Index
	idxDefn := &common.IndexDefn{
		DefnId:          common.IndexDefnId(rand.Int()),
		Name:            indexinfo.Name,
		Using:           common.ForestDB,
		Bucket:          indexinfo.Bucket,
		IsPrimary:       indexinfo.IsPrimary,
		SecExprs:        indexinfo.SecExprs,
		ExprType:        common.N1QL,
		PartitionScheme: common.TEST,
		PartitionKey:    ""}

	// call the index manager to handle the DDL
	err := m.mgr.HandleCreateIndexDDL(idxDefn)
	if err == nil {
		// No error, return success
		res := IndexMetaResponse{
			Status:     RESP_SUCCESS,
			Indexes:    []IndexInfo{indexinfo},
			ServerUuid: "",
		}
		sendResponse(w, res)
	} else {
		// report failure
		ierr := IndexError{Code: string(RESP_ERROR),
			Msg: err.Error()}

		res := IndexMetaResponse{
			Status: RESP_ERROR,
			Errors: []IndexError{ierr},
		}
		sendResponse(w, res)
	}
}

///////////////////////////////////////////////////////
// Private Function
///////////////////////////////////////////////////////

func convertRequest(r *http.Request) *IndexRequest {
	indexreq := IndexRequest{}
	buf := make([]byte, r.ContentLength, r.ContentLength)

	// Body will be non-null but can return EOF if being empty
	if _, err := r.Body.Read(buf); err != nil {
		common.Debugf("RequestHandler::convertRequest: unable to read request body")
		return nil
	}

	if err := json.Unmarshal(buf, &indexreq); err != nil {
		common.Debugf("RequestHandler::convertRequest: unable to unmarshall request body. Buf = %s", buf)
		return nil
	}

	return &indexreq
}

func sendResponse(w http.ResponseWriter, res interface{}) {
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

	gometa.SafeRun("requestHandler.run()",
		func() {
			http.HandleFunc("/createIndex", r.createIndexRequest)
		})

	li, err := net.Listen("tcp", INDEX_DDL_HTTP_ADDR)
	if err != nil {
		// TODO: abort
		common.Warnf("EventManager.run() : HTTP Server fails")
	}
	r.listener = li

	if err := http.Serve(li, nil); err != nil {
		// TODO: abort
		common.Warnf("EventManager.run() : HTTP Server fails")
	}
}
