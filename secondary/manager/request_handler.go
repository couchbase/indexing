// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package manager

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	u "net/url"
	"path"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/indexing/secondary/audit"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/common/collections"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/manager/client"
	mc "github.com/couchbase/indexing/secondary/manager/common"
	"github.com/couchbase/indexing/secondary/planner"
	"github.com/couchbase/indexing/secondary/security"
)

///////////////////////////////////////////////////////
// Type Definition
///////////////////////////////////////////////////////

//
// Index create / drop
//

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
	IndexIds client.IndexIdList     `json:indexIds,omitempty"`
	Plan     map[string]interface{} `json:plan,omitempty"`
}

type IndexResponse struct {
	Version uint64 `json:"version,omitempty"`
	Code    string `json:"code,omitempty"`
	Error   string `json:"error,omitempty"`
	Message string `json:"message,omitempty"`
}

//
// Index Backup / Restore
//

// LocalIndexMetadata is the metadata returned by getIndexStatus
// for all indexes on a single indexer node.
type LocalIndexMetadata struct {
	IndexerId        string             `json:"indexerId,omitempty"`
	NodeUUID         string             `json:"nodeUUID,omitempty"`
	StorageMode      string             `json:"storageMode,omitempty"`
	LocalSettings    map[string]string  `json:"localSettings,omitempty"`
	IndexTopologies  []IndexTopology    `json:"topologies,omitempty"`
	IndexDefinitions []common.IndexDefn `json:"definitions,omitempty"`

	// Pseudofields that should not be checksummed for ETags. These are stored in
	// requestHandlerCache but NOT in MetadataRepo.
	Timestamp        int64  `json:"timestamp,omitempty"`        // UnixNano meta repo retrieval time; not stored therein
	ETag             uint64 `json:"eTag,omitempty"`             // checksum (HTTP entity tag); 0 is HTTP_VAL_ETAG_INVALID
	ETagExpiry       int64  `json:"eTagExpiry,omitempty"`       // ETag expiration UnixNano time
	AllIndexesActive bool   `json:"allIndexesActive,omitempty"` // all indexes *included in this object* are active as described by the info contained in this object
}

type ClusterIndexMetadata struct {
	Metadata    []LocalIndexMetadata                           `json:"metadata,omitempty"`
	SchedTokens map[common.IndexDefnId]*mc.ScheduleCreateToken `json:"schedTokens,omitempty"`
}

type BackupResponse struct {
	Version uint64               `json:"version,omitempty"`
	Code    string               `json:"code,omitempty"`
	Error   string               `json:"error,omitempty"`
	Result  ClusterIndexMetadata `json:"result,omitempty"`
}

type RestoreResponse struct {
	Version uint64 `json:"version,omitempty"`
	Code    string `json:"code,omitempty"`
	Error   string `json:"error,omitempty"`
}

//
// Index Status
//

type IndexStatusResponse struct {
	Version     uint64        `json:"version,omitempty"`
	Code        string        `json:"code,omitempty"`
	Error       string        `json:"error,omitempty"`
	FailedNodes []string      `json:"failedNodes,omitempty"`
	Status      []IndexStatus `json:"status,omitempty"`
}

type IndexStatus struct {
	DefnId common.IndexDefnId `json:"defnId,omitempty"`
	InstId common.IndexInstId `json:"instId,omitempty"`

	// Name is the IndexName decorated with replica and partition info for the UI (i.e. display
	// name). See index.go FormatIndexPartnDisplayName.
	Name string `json:"name,omitempty"`

	Bucket     string   `json:"bucket,omitempty"`
	Scope      string   `json:"scope,omitempty"`
	Collection string   `json:"collection,omitempty"`
	IsPrimary  bool     `json:"isPrimary,omitempty"`
	SecExprs   []string `json:"secExprs,omitempty"`
	WhereExpr  string   `json:"where,omitempty"`
	IndexType  string   `json:"indexType,omitempty"`
	Status     string   `json:"status,omitempty"`
	Definition string   `json:"definition"`

	// Hosts in non-consolidated results has only one entry giving the host:index_http_port of the
	// node this IndexStatus is about. In consolidated results it lists all nodes holding any
	// partition of the instance.
	Hosts []string `json:"hosts,omitempty"`

	Error       string  `json:"error,omitempty"`
	Completion  int     `json:"completion"`
	Progress    float64 `json:"progress"`
	Scheduled   bool    `json:"scheduled"`
	Partitioned bool    `json:"partitioned"`

	// NumPartition in non-consolidated results gives the number of partitions of the index on the
	// node this IndexStatus is about. In consolidated results it gives the total number of
	// partitions the index has. A non-partitioned index is treated as having 1 partition.
	NumPartition int `json:"numPartition"`

	// PartitionMap is a map from host:index_http_port to partitionIds, telling which partition(s)
	// are on which node(s). In non-consolidated results this will have only one entry whose key
	// is the node this IndexStatus is about and whose value lists the (1-based) partition numbers
	// on that node. In consolidated results it will have a key for every host holding any
	// partitions of the instance, each of whose values lists the partition numbers on that node.
	// If an index is not partitioned, it will have a single partition with ID 0. (Partitioned
	// indexes do not have a partion 0.)
	PartitionMap map[string][]int `json:"partitionMap"`

	// NodeUUID in non-consolidated results holds the UUID (32-digit random hex assigned by
	// ns_server) of the node this IndexStatus is about. In consolidated results NodeUUID is always
	// set to "", even for non-partitioned indexes.
	NodeUUID string `json:"nodeUUID,omitempty"`

	NumReplica int `json:"numReplica"`

	// IndexName is the N1QL name of the index.
	IndexName string `json:"indexName"`

	ReplicaId    int    `json:"replicaId"` // 0-based
	Stale        bool   `json:"stale"`
	LastScanTime string `json:"lastScanTime,omitempty"`
}

type indexStatusSorter []IndexStatus

//
// Response
//

const (
	RESP_SUCCESS string = "success"
	RESP_ERROR   string = "error"
)

// XXX_LEVEL constants are used in the target.level field to indicate at what level of the hierarchy
// index stats are requested. They are listed here from coarse to fine (e.g. INDEX_LEVEL means stats
// for all indexes on the Index node, whereas lower levels get only indexes in the subtree). Those
// other than INDEXER_LEVEL are also the keys used to specify these parameters in /getIndexStatus
// and related REST calls.
const (
	INDEXER_LEVEL    string = "indexer"
	BUCKET_LEVEL     string = "bucket"
	SCOPE_LEVEL      string = "scope"
	COLLECTION_LEVEL string = "collection"
	INDEX_LEVEL      string = "index"
)

// target represents a filter for /getIndexStatus and related REST calls to restrict the set of
// indexes for which stats are returned. level is one of the XXX_LEVEL constants above, while the
// other strings are optional filter parameters. All coarser-grained filters must be specified in
// order to specify a finer-grained filter.
type target struct {
	bucket     string
	scope      string
	collection string
	index      string
	level      string
}

//
// requestHandlerContext contains state for the HTTP(S) server created by
// RegisterRequestHandler.
//
type requestHandlerContext struct {
	initializer sync.Once     // HTTP(S) custom initialization at startup
	finalizer   sync.Once     // cleanup at HTTP(S) server shutdown
	mgr         *IndexManager // parent
	config      common.Config // config settings map
	clusterUrl  string        // this node's full URL
	hostname    string        // this node's host:httpPort
	hostKey     string        // this node's mem+disk cache key
	eTagPeriod  time.Duration // for computing ETag expiries on rounded time boundaries

	// Current ETag info for /getIndexStatus global results
	getIndexStatusETag       uint64       // current valid checksum of cached getIndexStatus full results
	getIndexStatusETagExpiry int64        // getIndexStatusETag expiration UnixNano time
	getIndexStatusMutex      sync.RWMutex // sync for the above ETag info

	// requestHandlerCache is the mem + disk cache for LocalIndexMetadata, and subset of IndexStats
	rhc *requestHandlerCache

	schedTokenMon    *schedTokenMonitor
	stReqFailCount   uint64
	stReqRecCount    uint64
	useGreedyPlanner bool
}

var handlerContext requestHandlerContext // state for the HTTP(S) server

///////////////////////////////////////////////////////
// Registration
///////////////////////////////////////////////////////

// RegisterRequestHandler is the main entry point of the request handler. It creates
// an HTTP(S) server by registering REST endpoints and their handlers with Go's
// HTTP server infrastructure http.ServeMux (Go's HTTP request multiplexer), which
// will receive requests and call their handler functions.
func RegisterRequestHandler(mgr *IndexManager, mux *http.ServeMux, config common.Config) {
	handlerContext.initializer.Do(func() {
		defer func() {
			if r := recover(); r != nil {
				logging.Warnf("error encountered when registering http createIndex handler : %v.  Ignored.\n", r)
			}
		}()

		handlerContext.mgr = mgr
		handlerContext.config = config
		handlerContext.clusterUrl = config["clusterAddr"].String()
		handlerContext.hostname = getHostname(handlerContext.clusterUrl)
		handlerContext.hostKey = Host2key(handlerContext.hostname)
		handlerContext.eTagPeriod = time.Duration(config["settings.eTagPeriod"].Int()) * time.Second

		// Scatter-gather endpoints. These are the entry points to a single indexer that will
		// scatter the request to all indexers and gather the results to return to the caller.
		mux.HandleFunc("/getIndexMetadata", handlerContext.handleIndexMetadataRequest)
		mux.HandleFunc("/getIndexStatus", handlerContext.handleIndexStatusRequest)
		mux.HandleFunc("/getIndexStatement", handlerContext.handleIndexStatementRequest) // delegates to getIndexStatus

		// Single-indexer endpoints (non-scatter-gather).
		mux.HandleFunc("/createIndex", handlerContext.createIndexRequest)
		mux.HandleFunc("/createIndexRebalance", handlerContext.createIndexRequestRebalance)
		mux.HandleFunc("/dropIndex", handlerContext.dropIndexRequest)
		mux.HandleFunc("/buildIndexRebalance", handlerContext.buildIndexRequestRebalance)
		mux.HandleFunc("/getLocalIndexMetadata", handlerContext.handleLocalIndexMetadataRequest)
		mux.HandleFunc( // stripped-down version of getIndexStatus
			"/getCachedIndexTopology", handlerContext.handleCachedIndexTopologyRequest)
		mux.HandleFunc("/restoreIndexMetadata", handlerContext.handleRestoreIndexMetadataRequest)
		mux.HandleFunc("/planIndex", handlerContext.handleIndexPlanRequest)
		mux.HandleFunc("/settings/storageMode", handlerContext.handleIndexStorageModeRequest)
		mux.HandleFunc("/settings/planner", handlerContext.handlePlannerRequest)
		mux.HandleFunc("/listReplicaCount", handlerContext.handleListLocalReplicaCountRequest)
		mux.HandleFunc("/getCachedLocalIndexMetadata", handlerContext.handleCachedLocalIndexMetadataRequest)
		mux.HandleFunc("/getCachedStats", handlerContext.handleCachedStats)
		mux.HandleFunc("/postScheduleCreateRequest", handlerContext.handleScheduleCreateRequest)
		mux.HandleFunc("/getInternalVersion", handlerContext.handleInternalVersionRequest)

		cacheDir := path.Join(config["storage_dir"].String(), "cache")
		handlerContext.rhc = NewRequestHandlerCache(cacheDir)

		handlerContext.schedTokenMon = newSchedTokenMonitor(mgr)
		handlerContext.useGreedyPlanner = config["planner.useGreedyPlanner"].Bool()
	})
}

// Close permanently shuts down the HTTP(S) server created by RegisterRequestHandler.
func (m *requestHandlerContext) Close() {
	m.finalizer.Do(func() {
		m.rhc.Shutdown()
		m.schedTokenMon.Close()
	})
}

// getHostname returns the properly IPv4 or IPv6 formatted host:httpPort from a URL.
func getHostname(url string) string {
	host, _, _ := net.SplitHostPort(url)
	port := handlerContext.config["httpPort"].String()
	return net.JoinHostPort(host, port)
}

///////////////////////////////////////////////////////
// Create / Drop Index
///////////////////////////////////////////////////////

// createIndexRequest handles /createIndex REST endpoint. Called for non-rebalance index creations.
func (m *requestHandlerContext) createIndexRequest(w http.ResponseWriter, r *http.Request) {

	m.doCreateIndex(w, r, false)

}

// createIndexRequestRebalance handles /createIndexRebalance REST endpoint. Called by Rebalancer to
// create index on destination node.
func (m *requestHandlerContext) createIndexRequestRebalance(w http.ResponseWriter, r *http.Request) {

	m.doCreateIndex(w, r, true)

}

// doCreateIndex creates an index via REST. isRebalReq arg is true if called from Rebalancer, else false.
// It delegates to IndexManager to do the real work.
func (m *requestHandlerContext) doCreateIndex(w http.ResponseWriter, r *http.Request, isRebalReq bool) {
	const method string = "RequestHandler::doCreateIndex" // for logging

	creds, ok := doAuth(r, w, method)
	if !ok {
		return
	}

	// convert request
	request := m.convertIndexRequest(r)
	if request == nil {
		sendIndexResponseWithError(http.StatusBadRequest, w, "Unable to convert request for create index")
		return
	}

	permission := fmt.Sprintf("cluster.collection[%s:%s:%s].n1ql.index!create", request.Index.Bucket, request.Index.Scope, request.Index.Collection)
	if !isAllowed(creds, []string{permission}, r, w, method) {
		return
	}

	indexDefn := request.Index

	if indexDefn.DefnId == 0 {
		defnId, err := common.NewIndexDefnId()
		if err != nil {
			sendIndexResponseWithError(http.StatusInternalServerError, w, fmt.Sprintf("Fail to generate index definition id %v", err))
			return
		}
		indexDefn.DefnId = defnId
	}

	if len(indexDefn.Using) != 0 && strings.ToLower(string(indexDefn.Using)) != "gsi" {
		if common.IndexTypeToStorageMode(indexDefn.Using) != common.GetStorageMode() {
			sendIndexResponseWithError(http.StatusInternalServerError, w, fmt.Sprintf("Storage Mode Mismatch %v", indexDefn.Using))
			return
		}
	}

	// call the index manager to handle the DDL
	if logging.IsEnabled(logging.Debug) {
		logging.Debugf(
			"%v: calling IndexManager to create index %v:%v:%v:%v",
			method, indexDefn.Bucket, indexDefn.Scope, indexDefn.Collection, indexDefn.Name)
	}
	if err := m.mgr.HandleCreateIndexDDL(&indexDefn, isRebalReq); err == nil {
		// No error, return success
		sendIndexResponse(w)
	} else {
		// report failure
		sendIndexResponseWithError(http.StatusInternalServerError, w, fmt.Sprintf("%v", err))
	}
}

// dropIndexRequest handles the /dropIndex REST endpoint.
func (m *requestHandlerContext) dropIndexRequest(w http.ResponseWriter, r *http.Request) {
	const method string = "RequestHandler::dropIndexRequest" // for logging

	creds, ok := doAuth(r, w, method)
	if !ok {
		return
	}

	// convert request
	request := m.convertIndexRequest(r)
	if request == nil {
		sendIndexResponseWithError(http.StatusBadRequest, w, "Unable to convert request for drop index")
		return
	}

	permission := fmt.Sprintf("cluster.collection[%s:%s:%s].n1ql.index!drop", request.Index.Bucket, request.Index.Scope, request.Index.Collection)
	if !isAllowed(creds, []string{permission}, r, w, method) {
		return
	}

	// call the index manager to handle the DDL
	indexDefn := request.Index

	if indexDefn.RealInstId == 0 {
		if err := m.mgr.HandleDeleteIndexDDL(indexDefn.DefnId); err == nil {
			// No error, return success
			sendIndexResponse(w)
		} else {
			// report failure
			sendIndexResponseWithError(http.StatusInternalServerError, w, fmt.Sprintf("%v", err))
		}
	} else if indexDefn.InstId != 0 {
		if err := m.mgr.DropOrPruneInstance(indexDefn, true); err == nil {
			// No error, return success
			sendIndexResponse(w)
		} else {
			// report failure
			sendIndexResponseWithError(http.StatusInternalServerError, w, fmt.Sprintf("%v", err))
		}
	} else {
		// report failure
		sendIndexResponseWithError(http.StatusInternalServerError, w, fmt.Sprintf("Missing index inst id for defn %v", indexDefn.DefnId))
	}
}

//
// buildIndexRequestRebalance handles the /buildIndexRebalance REST endpoint used only by Rebalancer via
// local REST call, which can specify multiple indexes to build. The builds happen asynchronously under
// Indexer.handleBuildIndex, which returns per-instance error information in the form of an error map
// that is then lightly edited by LifecycleMgr.buildIndexesLifecycleMgr and received and processed by
// Rebalancer.buildAcceptedIndexes.
//
// The full call chain of the request processing is long and hard to deduce from code. At time of writing it was:
//   rebalancer.go
//     processTokenAsDest
//     go buildAcceptedIndexes
//       REST call POST to /buildIndexRebalance
//       waitForIndexBuild
//   request_handler.go
//     buildIndexRequestRebalance -- this function
//   manager.go
//     HandleBuildIndexRebalDDL
//   gometa/server/embeddedServer.go
//     MakeRequest -- OPCODE_BUILD_INDEX_REBAL (part of RequestServer interface defined by manager.go that
//       gometa/server/embeddedServer.go implements)
//   lifecycle.go
//     OnNewRequest (impl of a method of gometa/protocol/common.go CustomRequestHandler iface) -- add to incomings queue
//     go processRequest -- pulls from incomings queue
//     dispatchRequest -- eventually puts the return message into outgoings queue
//     handleBuildIndexes
//     buildIndexesLifecycleMgr
//   cluster_manager_agent.go
//     OnIndexBuild -- sends MsgBuildIndex to Indexer
//   indexer.go
//     handleAdminMsgs -- receives MsgBuildIndex
//     handleBuildIndex -- sends MsgBuildIndexResponse containing the error map back to cluster_manager_agent OnIndexBuild
//
func (m *requestHandlerContext) buildIndexRequestRebalance(w http.ResponseWriter, r *http.Request) {
	const method string = "RequestHandler::buildIndexRequestRebalance" // for logging

	creds, ok := doAuth(r, w, method)
	if !ok {
		return
	}

	// convert request
	request := m.convertIndexRequest(r)
	if request == nil {
		sendIndexResponseWithError(http.StatusBadRequest, w, "Unable to convert request for build index")
		return
	}

	permission := fmt.Sprintf("cluster.collection[%s:%s:%s].n1ql.index!build", request.Index.Bucket, request.Index.Scope, request.Index.Collection)
	if !isAllowed(creds, []string{permission}, r, w, method) {
		return
	}

	// call the index manager to handle the DDL
	indexIds := request.IndexIds
	if err := m.mgr.HandleBuildIndexRebalDDL(indexIds); err == nil {
		// No error, return success
		sendIndexResponse(w)
	} else {
		// report failure
		sendIndexResponseWithError(http.StatusInternalServerError, w, fmt.Sprintf("%v", err))
	}
}

func (m *requestHandlerContext) convertIndexRequest(r *http.Request) *IndexRequest {

	req := &IndexRequest{}

	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(r.Body); err != nil {
		logging.Debugf("RequestHandler::convertIndexRequest: unable to read request body, err %v", err)
		return nil
	}

	if err := json.Unmarshal(buf.Bytes(), req); err != nil {
		logging.Debugf("RequestHandler::convertIndexRequest: unable to unmarshall request body. Buf = %s, err %v", logging.TagStrUD(buf), err)
		return nil
	}

	// Set default scope and collection name if incoming request dont have them
	req.Index.SetCollectionDefaults()

	return req
}

//////////////////////////////////////////////////////
// Index Status
///////////////////////////////////////////////////////

// handleIndexStatusRequest services the /getIndexStatus REST endpoint and returns statuses
// for (possibly a subset of) indexes on every indexer node. The caller can request status
// of only a subset of indexes; additionally any indexes they do not have permissions for
// will be filtered out. (ns_server calls this enpoint every 5 sec on every indexer node to
// get statues for all indexes, but it is not the only caller.)
func (m *requestHandlerContext) handleIndexStatusRequest(w http.ResponseWriter, r *http.Request) {
	const method string = "RequestHandler::handleIndexStatusRequest" // for logging

	defer func() {
		if r := recover(); r != nil {
			count := atomic.AddUint64(&m.stReqRecCount, 1)
			if count%40 == 1 {
				logging.Fatalf("%v: Recovered from panic %v. Stacktrace %v",
					method, count, string(debug.Stack()))
			}
		}
	}()

	creds, ok := doAuth(r, w, method)
	if !ok {
		return
	}

	// Request can be for a subset of indexes. Construct target t describing that subset.
	bucket := m.getBucket(r)
	scope := m.getScope(r)
	collection := m.getCollection(r)
	index := m.getIndex(r)
	t, err := validateRequest(bucket, scope, collection, index)
	if err != nil {
		logging.Debugf("%v: Error %v", method, err)
		resp := &IndexStatusResponse{Code: RESP_ERROR, Error: err.Error()}
		send(http.StatusInternalServerError, w, resp)
		return
	}

	// getAll=true means keep details of partitioned and replicated indexes separate by host;
	// otherwise they are consolidated into one entry per index.
	getAll := false
	val := r.FormValue("getAll")
	if len(val) != 0 && val == "true" {
		getAll = true
	}

	// omitScheduled=true means omit information about scheduled indexes
	omitScheduled := false
	val = r.FormValue("omitScheduled")
	if len(val) != 0 && val == "true" {
		omitScheduled = true
	}

	indexStatuses, failedNodes, eTagResponse, err := m.getIndexStatus(creds, t, getAll, omitScheduled)
	if err == nil && len(failedNodes) == 0 {
		sort.Sort(indexStatusSorter(indexStatuses))
		eTagRequest := getETagFromHttpHeader(r)
		if eTagRequest == eTagResponse && eTagResponse != common.HTTP_VAL_ETAG_INVALID {
			// Valid ETag; respond 304 Not Modified with the same ETag
			sendNotModified(w, eTagRequest)
		} else {
			resp := &IndexStatusResponse{Code: RESP_SUCCESS, Status: indexStatuses}
			sendWithETag(http.StatusOK, w, resp, eTagResponse)
		}
		atomic.StoreUint64(&m.stReqFailCount, 0)
	} else {
		atomic.AddUint64(&m.stReqFailCount, 1)
		logging.Debugf("%v: failed nodes %v", method, failedNodes)
		sort.Sort(indexStatusSorter(indexStatuses))
		resp := &IndexStatusResponse{Code: RESP_ERROR, Error: "Fail to retrieve cluster-wide metadata from index service",
			Status: indexStatuses, FailedNodes: failedNodes}
		send(http.StatusInternalServerError, w, resp)
	}
}

// handleCachedIndexTopologyRequest handles "/getCachedIndexTopology" which is a stripped-down
// version of "/getIndexStatus" that reads entirely from the local node's memory cache and avoids
// ClusterInfoCache. It returns only the subset of information needed by Autofailover.
func (m *requestHandlerContext) handleCachedIndexTopologyRequest(
	w http.ResponseWriter, r *http.Request) {
	const method string = "RequestHandler::handleCachedIndexTopologyRequest" // for logging

	creds, ok := doAuth(r, w, method)
	if !ok {
		return
	}

	indexStatuses := m.getCachedIndexTopology(creds)
	resp := &IndexStatusResponse{Code: RESP_SUCCESS, Status: indexStatuses}
	send(http.StatusOK, w, resp)
}

func (m *requestHandlerContext) getBucket(r *http.Request) string {

	return r.FormValue(BUCKET_LEVEL)
}

func (m *requestHandlerContext) getScope(r *http.Request) string {

	return r.FormValue(SCOPE_LEVEL)
}

func (m *requestHandlerContext) getCollection(r *http.Request) string {

	return r.FormValue(COLLECTION_LEVEL)
}

func (m *requestHandlerContext) getIndex(r *http.Request) string {

	return r.FormValue(INDEX_LEVEL)
}

// buildTopologyMapPerCollection is a helper for getIndexStatus. It creates
// a map of index topology pointers keyed by [bucket][scope][collection].
func buildTopologyMapPerCollection(topologies []IndexTopology) map[string]map[string]map[string]*IndexTopology {

	topoMap := make(map[string]map[string]map[string]*IndexTopology)
	for i, _ := range topologies {
		t := &topologies[i]
		t.SetCollectionDefaults()
		if _, ok := topoMap[t.Bucket]; !ok {
			topoMap[t.Bucket] = make(map[string]map[string]*IndexTopology)
		}
		if _, ok := topoMap[t.Bucket][t.Scope]; !ok {
			topoMap[t.Bucket][t.Scope] = make(map[string]*IndexTopology)
		}
		topoMap[t.Bucket][t.Scope][t.Collection] = t
	}
	return topoMap
}

// mergeCounter is a helper for getIndexStatus and getCachedIndexTopology.
func mergeCounter(defnId common.IndexDefnId, counter common.Counter,
	numReplicas map[common.IndexDefnId]common.Counter) {

	if current, ok := numReplicas[defnId]; ok {
		newValue, merged, err := current.MergeWith(counter)
		if err != nil {
			logging.Errorf("Fail to merge replica count. Error: %v", err)
			return
		}

		if merged {
			numReplicas[defnId] = newValue
		}
		return
	}

	if counter.IsValid() {
		numReplicas[defnId] = counter
	}
}

// addHost is a helper for getIndexStatus.
func addHost(defnId common.IndexDefnId, hostAddr string, defnToHostMap map[common.IndexDefnId][]string) {
	if hostList, ok := defnToHostMap[defnId]; ok {
		for _, host := range hostList {
			if strings.Compare(hostAddr, host) == 0 {
				return
			}
		}
	}
	defnToHostMap[defnId] = append(defnToHostMap[defnId], hostAddr)
}

// getScheduledIndexes is a helper for getIndexStatus that returns statuses for scheduled but not
// yet created indexes.
func (m *requestHandlerContext) getScheduledIndexes(
	defns map[common.IndexDefnId]common.IndexDefn) []IndexStatus {
	schedIndexes := m.schedTokenMon.getIndexes()
	schedIndexList := make([]IndexStatus, 0, len(schedIndexes)) // return value

	for _, idx := range schedIndexes {
		if _, ok := defns[idx.DefnId]; ok {
			continue
		}
		schedIndexList = append(schedIndexList, *idx)
	}
	return schedIndexList
}

func (m *requestHandlerContext) IndexStatusErrorf(msg string) {
	failCount := atomic.LoadUint64(&m.stReqFailCount)
	msgWithCount := fmt.Sprintf("%v Error Count after pass: %v", msg, failCount+1)
	if (failCount % 40) == 0 {
		logging.Errorf(msgWithCount)
	} else {
		logging.Debugf(msgWithCount)
	}
}

// getIndexStatus returns statuses of all indexer nodes. An unresponsive node's status is served
// from the cache of whichever responsive node has the newest data cached. Only if no responsive
// node has a cached value will an unresponsive node's status be omitted. (The caches were only
// added in common.INDEXER_65_VERSION.) Status consists of two parts:
//   1. LocalIndexMetadata
//   2. A subset of IndexStats (currently: buildProgress, completionProgress, lastScanTime)
//
// getAll true means preserve per-node information about partitioned indexes; false means
//   consolidate the statuses of all nodes hosting some partitions/replicas of an instance into a
//   single entry (whose NodeUUID will be set to "").
// omitScheduled true means do not include information about scheduled indexes.
func (m *requestHandlerContext) getIndexStatus(creds cbauth.Creds, target *target, getAll bool, omitScheduled bool) (
	indexStatuses []IndexStatus, failedNodes []string, eTagResponse uint64, err error) {

	m.mgr.cinfoProviderLockReqHandler.RLock()
	defer m.mgr.cinfoProviderLockReqHandler.RUnlock()

	ninfo, err := m.mgr.cinfoProviderReqHandler.GetNodesInfoProvider()
	if err != nil || ninfo == nil {
		errMsg := "RequestHandler::getIndexStatus ClusterInfoCache unavailable in IndexManager"
		logging.Errorf(errMsg)
		err = errors.New(errMsg)
		return nil, nil, common.HTTP_VAL_ETAG_INVALID, err
	}

	ninfo.RLock()
	defer ninfo.RUnlock()

	// find all nodes that has a index http service
	nids := ninfo.GetNodesByServiceType(common.INDEX_HTTP_SERVICE)

	numReplicas := make(map[common.IndexDefnId]common.Counter)
	defns := make(map[common.IndexDefnId]common.IndexDefn) // map from defnId to defn
	indexStatuses = make([]IndexStatus, 0)                 // return 1: flat list of statuses
	failedNodes = make([]string, 0)                        // return 2: flat list of unreachable indexer nodes

	// IndexStatus pieces to compute ETags from (full sets across hosts)
	metaAcrossHosts := make(map[string]*LocalIndexMetadata)
	statsAcrossHosts := make(map[string]*common.Statistics)
	fullSet := true           // do the results contain all index defns?
	allFromLocalCache := true // are all results from local cache?

	defnToHostMap := make(map[common.IndexDefnId][]string)
	isInstanceDeferred := make(map[common.IndexInstId]bool)
	permissionCache := common.NewSessionPermissionsCache(creds)

	keepKeys := make([]string, 0, len(nids)) // memory cache keys of current indexer nodes
	for _, nid := range nids {
		nodeMetaFromLocalCache := true  // is localMeta for current node from local cache?
		nodeStatsFromLocalCache := true // is stats for current node from local cache?

		// mgmtAddr is this node's "cluster" address (host:uiPort), NOT a key for caches
		// address here will be with encrypted port if encryption is enabled.
		mgmtAddr, err := ninfo.GetServiceAddress(nid, "mgmt", true)
		if err != nil {
			msg := fmt.Sprintf("RequestHandler::getIndexStatus: Error from GetServiceAddress (mgmt) for node id %v. Error = %v", nid, err)
			m.IndexStatusErrorf(msg)
			continue
		}

		addr, err := ninfo.GetServiceAddress(nid, common.INDEX_HTTP_SERVICE, true)
		if err != nil {
			msg := fmt.Sprintf("RequestHandler::getIndexStatus: Error from GetServiceAddress(indexHttp, true) for node id %v. Error = %v", nid, err)
			m.IndexStatusErrorf(msg)
			failedNodes = append(failedNodes, mgmtAddr)
			continue
		}

		// Use same un-encrypted port number as key, even if the encryption level changes in between
		hostname, err := ninfo.GetServiceAddress(nid, common.INDEX_HTTP_SERVICE, false)
		if err != nil {
			msg := fmt.Sprintf("RequestHandler::getIndexStatus: Error from GetServiceAddress(indexHttp, false) for node id %v. Error = %v", nid, err)
			m.IndexStatusErrorf(msg)
			failedNodes = append(failedNodes, mgmtAddr)
			continue
		}

		hostKey := Host2key(hostname) // key to caches
		keepKeys = append(keepKeys, hostKey)
		stale := false

		//
		// Get metadata for all indexes of current node
		//
		// TODO: It is not required to fetch metadata for entire node when target is for a specific
		// bucket or collection
		metaAcrossHosts[hostKey] = nil
		localMeta, latest, localMetaIsFromCache, err := m.getLocalIndexMetadataForNode(
			addr, hostname, ninfo)
		if localMeta == nil || err != nil {
			msg := fmt.Sprintf("RequestHandler::getIndexStatus: Error while retrieving %v with auth %v", addr+"/getLocalIndexMetadata", err)
			m.IndexStatusErrorf(msg)
			failedNodes = append(failedNodes, mgmtAddr)
			continue
		}
		if !latest {
			stale = true
		}
		if !localMetaIsFromCache {
			allFromLocalCache = false
			nodeMetaFromLocalCache = false
		}
		metaAcrossHosts[hostKey] = localMeta
		topoMap := buildTopologyMapPerCollection(localMeta.IndexTopologies)

		//
		// Get stats subset for all indexes of current node
		//
		statsAcrossHosts[hostKey] = nil
		var stats *common.Statistics = nil
		if localMetaIsFromCache && localMeta.AllIndexesActive { // try cache
			stats = m.rhc.GetStatsFromCache(hostKey)
		}
		if stats == nil { // full-bore stats retrieval needed
			stats, latest, err = m.getStatsForNode(addr, hostname, ninfo)
			if stats == nil || err != nil {
				msg := fmt.Sprintf("RequestHandler::getIndexStatus: Error while retrieving %v with auth %v", addr+"/stats?async=true", err)
				m.IndexStatusErrorf(msg)
				failedNodes = append(failedNodes, mgmtAddr)
				continue
			}
			if !latest {
				stale = true
			}
			allFromLocalCache = false
			nodeStatsFromLocalCache = false
		}
		statsAcrossHosts[hostKey] = stats

		//
		// Process all the data for current host
		//
		if !localMetaIsFromCache { // localMeta.AllIndexesActive is not already set correctly
			localMeta.AllIndexesActive = true // will change to false below if any non-active found
		}
		for _, defn := range localMeta.IndexDefinitions {
			defn.SetCollectionDefaults()
			if !shouldProcess(target, defn.Bucket, defn.Scope, defn.Collection, defn.Name) {
				// Do not cache partial results
				metaAcrossHosts[hostKey] = nil
				statsAcrossHosts[hostKey] = nil
				fullSet = false
				continue
			}
			accessAllowed := permissionCache.IsAllowed(defn.Bucket, defn.Scope, defn.Collection, "list")
			if !accessAllowed {
				// Do not cache partial results
				metaAcrossHosts[hostKey] = nil
				statsAcrossHosts[hostKey] = nil
				fullSet = false
				continue
			}
			mergeCounter(defn.DefnId, defn.NumReplica2, numReplicas)
			if topology, ok := topoMap[defn.Bucket][defn.Scope][defn.Collection]; ok && topology != nil {
				instances := topology.GetIndexInstancesByDefn(defn.DefnId)
				for _, instance := range instances {
					state, errStr := topology.GetStatusByInst(defn.DefnId, common.IndexInstId(instance.InstId))
					if state != common.INDEX_STATE_ACTIVE {
						localMeta.AllIndexesActive = false
					}
					if state != common.INDEX_STATE_CREATED &&
						state != common.INDEX_STATE_DELETED &&
						state != common.INDEX_STATE_NIL {

						stateStr := getStateStr(&instance, state, len(errStr) != 0, stats)

						name := common.FormatIndexInstDisplayName(defn.Name, int(instance.ReplicaId))
						prefix := common.GetStatsPrefix(defn.Bucket, defn.Scope, defn.Collection,
							defn.Name, int(instance.ReplicaId), 0, false)

						completion := int(0)
						key := common.GetIndexStatKey(prefix, "build_progress")
						if progress, ok := stats.ToMap()[key]; ok {
							completion = int(progress.(float64))
						}

						progress := float64(0)
						key = fmt.Sprintf("%v:completion_progress", instance.InstId)
						if stat, ok := stats.ToMap()[key]; ok {
							progress = math.Float64frombits(uint64(stat.(float64)))
						}

						lastScanTime := "NA"
						key = common.GetIndexStatKey(prefix, "last_known_scan_time")
						if scanTime, ok := stats.ToMap()[key]; ok {
							nsecs := int64(scanTime.(float64))
							if nsecs != 0 {
								lastScanTime = time.Unix(0, nsecs).Format(time.UnixDate)
							}
						}

						partitionMap := make(map[string][]int)
						for _, partnDef := range instance.Partitions {
							partitionMap[mgmtAddr] = append(partitionMap[mgmtAddr], int(partnDef.PartId))
						}

						addHost(defn.DefnId, mgmtAddr, defnToHostMap)
						isInstanceDeferred[common.IndexInstId(instance.InstId)] = defn.Deferred
						defn.NumPartitions = instance.NumPartitions

						secExprs, _, _ := common.GetUnexplodedExprs(defn.SecExprs, defn.Desc)

						status := IndexStatus{
							DefnId:       defn.DefnId,
							InstId:       common.IndexInstId(instance.InstId),
							Name:         name, // display name only
							Bucket:       defn.Bucket,
							Scope:        defn.Scope,
							Collection:   defn.Collection,
							IndexName:    defn.Name, // real index name
							IsPrimary:    defn.IsPrimary,
							SecExprs:     secExprs,
							WhereExpr:    defn.WhereExpr,
							IndexType:    string(defn.Using),
							Status:       stateStr,
							Error:        errStr,
							Hosts:        []string{mgmtAddr},
							Definition:   common.IndexStatement(defn, int(instance.NumPartitions), -1, true),
							Completion:   completion,
							Progress:     progress,
							Scheduled:    instance.Scheduled,
							Partitioned:  common.IsPartitioned(defn.PartitionScheme),
							NumPartition: len(instance.Partitions),
							PartitionMap: partitionMap,
							NodeUUID:     localMeta.NodeUUID,
							NumReplica:   int(defn.GetNumReplica()),
							ReplicaId:    int(instance.ReplicaId),
							Stale:        stale,
							LastScanTime: lastScanTime,
						}

						indexStatuses = append(indexStatuses, status)
					}
				} // for each instance
			}
			defns[defn.DefnId] = defn
		}

		// Cache any updated data if it is a full set for nid
		metaToCacheForHost := metaAcrossHosts[hostKey]
		if metaToCacheForHost != nil && !nodeMetaFromLocalCache {
			if hostKey == m.hostKey { // fresh info being cached is for *current host*
				// Ideally clearing the dirty flag would have been done in setETagLocalIndexMetadata,
				// but we don't know what code path entered it so its data might never be cached.
				m.mgr.getMetadataRepo().ClearMetaDirty()
			}
			// If LocalIndexMetadata came from a pre-Cheshire-Cat node, it won't have computed the
			// ETag and ETagExpiry fields, so do it here. This can still prevent having to send full
			// result back to external caller, though not the re-getting of the LIM each time.
			if metaToCacheForHost.ETag == common.HTTP_VAL_ETAG_INVALID {
				m.setETagLocalIndexMetadata(metaToCacheForHost)
			}
			m.rhc.CacheLocalIndexMetadata(hostKey, metaToCacheForHost)
		}
		statsToCacheForHost := statsAcrossHosts[hostKey]
		if statsToCacheForHost != nil && !nodeStatsFromLocalCache {
			m.rhc.CacheStats(hostKey, statsToCacheForHost)
		}
	} // for each nid

	// Delete cache entries of former Index nodes that are no longer in the cluster
	m.rhc.DeleteObsoleteCacheEntries(keepKeys)

	//Fix replica count
	for i, index := range indexStatuses {
		if counter, ok := numReplicas[index.DefnId]; ok {
			numReplica, exist := counter.Value()
			if exist {
				indexStatuses[i].NumReplica = int(numReplica)
			}
		}
	}

	// Fix index definition so that the "nodes" field inside
	// "with" clause show the current set of nodes on which
	// the index resides.
	//
	// If the index resides on different nodes, the "nodes" clause
	// is populated on UI irrespective of whether the index is
	// explicitly defined with "nodes" clause or not
	//
	// If the index resides only on one node, the "nodes" clause is
	// populated on UI only if the index definition is explicitly
	// defined with "nodes" clause
	for i, index := range indexStatuses {
		defnId := index.DefnId
		defn := defns[defnId]
		if len(defnToHostMap[defnId]) > 1 || defn.Nodes != nil {
			defn.Nodes = defnToHostMap[defnId]
			// The deferred field will be set to true by default for a rebalanced index
			// For the non-rebalanced index, it can either be true or false depending on
			// how it was created
			defn.Deferred = isInstanceDeferred[index.InstId]
			indexStatuses[i].Definition = common.IndexStatement(defn, int(defn.NumPartitions), index.NumReplica, true)
		}
	}

	if !getAll {
		indexStatuses = m.consolidateIndexStatus(indexStatuses)
	}

	if !omitScheduled {
		// Add statuses of scheduled indexes that are not built yet
		schedIndexList := m.getScheduledIndexes(defns)
		indexStatuses = append(indexStatuses, schedIndexList...)
	} else {
		fullSet = false
	}

	// Compute eTagResponse
	// 1. If got full set of index statuses (the only thing we compute getIndexStatus ETags for):
	//    a. If got all results from local cache AND current expiry time not passed, use current ETag
	//    b. Else compute and save new global getIndexStatus ETag and expiry
	// 2. Else partial set: leave eTagResponse as original zero value == common.HTTP_VAL_ETAG_INVALID
	if fullSet {
		eTagCurr, eTagExpiry := m.getGetIndexStatusETagInfo()
		if allFromLocalCache && time.Now().UnixNano() < eTagExpiry {
			eTagResponse = eTagCurr
		} else {
			eTagResponse, err = m.setETagGetIndexStatus(metaAcrossHosts, statsAcrossHosts)
			if err != nil {
				return nil, nil, common.HTTP_VAL_ETAG_INVALID, err
			}
		}
	}

	return indexStatuses, failedNodes, eTagResponse, nil
}

// getCachedIndexTopology is a stripped-down version of getIndexStatus that reads entirely from the
// local node's memory cache and avoids ClusterInfoCache. It returns only the subset of information
// needed by Autofailover. It does not include scheduled indexes as these do not exist yet so will
// not be lost by failing over any subset of nodes. Failover will migrate any scheduled tokens that
// need new homes to surviving nodes.
func (m *requestHandlerContext) getCachedIndexTopology(creds cbauth.Creds) (indexStatuses []IndexStatus) {
	const _getCachedIndexTopology = "requestHandlerContext::getCachedIndexTopology:"

	indexStatuses = make([]IndexStatus, 0)                      // return value
	permissionCache := common.NewSessionPermissionsCache(creds) // access permissions
	defns := make(map[common.IndexDefnId]common.IndexDefn)      // map from defnId to defn
	numReplicas := make(map[common.IndexDefnId]common.Counter)

	metaAcrossHosts := m.rhc.GetAllCachedLocalIndexMetadata()
	for hostKey, localMeta := range metaAcrossHosts {
		topoMap := buildTopologyMapPerCollection(localMeta.IndexTopologies)

		for _, defn := range localMeta.IndexDefinitions {
			defn.SetCollectionDefaults()
			accessAllowed := permissionCache.IsAllowed(
				defn.Bucket, defn.Scope, defn.Collection, "list")
			if !accessAllowed {
				continue
			}
			mergeCounter(defn.DefnId, defn.NumReplica2, numReplicas)
			if topology, ok := topoMap[defn.Bucket][defn.Scope][defn.Collection]; ok && topology != nil {
				instances := topology.GetIndexInstancesByDefn(defn.DefnId)
				for _, instance := range instances {
					state, _ := topology.GetStatusByInst(defn.DefnId, common.IndexInstId(instance.InstId))
					if state != common.INDEX_STATE_CREATED &&
						state != common.INDEX_STATE_DELETED &&
						state != common.INDEX_STATE_NIL {

						// hostKey is substituted in this routine for the host:httpPort hostnames
						// getIndexStatus gets from ClusterInfoCache
						partitionMap := make(map[string][]int)
						for _, partnDef := range instance.Partitions {
							partitionMap[hostKey] = append(partitionMap[hostKey], int(partnDef.PartId))
						}

						// Only populate the fields needed for IsAutofailoverSafe method
						status := IndexStatus{
							Bucket:     defn.Bucket,
							Scope:      defn.Scope,
							Collection: defn.Collection,
							IndexName:  defn.Name, // real index name

							Hosts:        []string{hostKey},
							PartitionMap: partitionMap,
							NodeUUID:     localMeta.NodeUUID,
						}
						indexStatuses = append(indexStatuses, status)
					}
				} // for each instance
			}
			defns[defn.DefnId] = defn
		} // for each defn
	} // for each hostKey (node)

	logging.Infof("%v Returning %v IndexStatuses", _getCachedIndexTopology, len(indexStatuses))
	return indexStatuses
}

// getStateStr is a helper for getIndexStatus that returns the IndexStatus.Status string for the
// given instance given its state, stateError, and stats.
func getStateStr(instance *IndexInstDistribution, state common.IndexState, stateError bool,
	stats *common.Statistics) string {

	if stateError {
		return "Error"
	}

	stateStr := "Not Available"
	switch state {
	case common.INDEX_STATE_READY:
		stateStr = "Created"
	case common.INDEX_STATE_INITIAL:
		stateStr = "Building"
	case common.INDEX_STATE_CATCHUP:
		stateStr = "Building"
	case common.INDEX_STATE_ACTIVE:
		stateStr = "Ready"
	}
	if instance.RState == uint32(common.REBAL_PENDING) && state != common.INDEX_STATE_READY {
		stateStr = "Moving"
	}
	if state == common.INDEX_STATE_INITIAL || state == common.INDEX_STATE_CATCHUP {
		if len(instance.OldStorageMode) != 0 {
			if instance.OldStorageMode == common.ForestDB && instance.StorageMode == common.PlasmaDB {
				stateStr = "Building (Upgrading)"
			}
			if instance.StorageMode == common.ForestDB && instance.OldStorageMode == common.PlasmaDB {
				stateStr = "Building (Downgrading)"
			}
		}
	}
	if state == common.INDEX_STATE_READY {
		if len(instance.OldStorageMode) != 0 {
			if instance.OldStorageMode == common.ForestDB && instance.StorageMode == common.PlasmaDB {
				stateStr = "Created (Upgrading)"
			}
			if instance.StorageMode == common.ForestDB && instance.OldStorageMode == common.PlasmaDB {
				stateStr = "Created (Downgrading)"
			}
		}
	}
	if indexerState, ok := stats.ToMap()["indexer_state"]; ok {
		if indexerState == "Paused" {
			stateStr = "Paused"
		} else if indexerState == "Bootstrap" || indexerState == "Warmup" {
			stateStr = "Warmup"
		}
	}
	return stateStr
}

// generateETagExpiry returns the next expiration UnixNano time of ETags based on
// the current time. The expiry will be the next future rounded-to-S-seconds time
// that is at least S/2 seconds away, where S is specified by config variable
// indexer.settings.eTagPeriod (currently 240). The expiry will thus average S
// seconds in the future but can be anything between S/2 and 3S/2. (Most of the
// time it will be very close to S because ns_server calls getIndexStatus much more
// frequently, triggering new ETag creations soon after the prior expiry.) The rounding
// is done to try to keep expiry times aligned across all nodes (unfortunately
// jittered by any internode clock differences), so getIndexStatus for many nodes
// will likely have either all or none of its individual LocalIndexMetadata ETags
// unexpired, as if even one's ETag is expired we must send full results to caller.
func (m *requestHandlerContext) generateETagExpiry() int64 {
	return time.Now().Add(m.eTagPeriod).Round(m.eTagPeriod).UnixNano()
}

// getGetIndexStatusETag returns the current ETag and expiry for global getIndexStatus results.
func (m *requestHandlerContext) getGetIndexStatusETagInfo() (eTag uint64, expiry int64) {
	m.getIndexStatusMutex.RLock()
	defer m.getIndexStatusMutex.RUnlock()
	return m.getIndexStatusETag, m.getIndexStatusETagExpiry
}

// setGetIndexStatusETag sets a new ETag and expiry for global getIndexStatus results.
func (m *requestHandlerContext) setGetIndexStatusETag(eTag uint64) {
	m.getIndexStatusMutex.Lock()
	m.getIndexStatusETag = eTag
	m.getIndexStatusETagExpiry = m.generateETagExpiry()
	m.getIndexStatusMutex.Unlock()
}

// setETagGetIndexStatus computes and sets a new ETag and expiry for global getIndexStatus
// results and returns the new ETag.
func (m *requestHandlerContext) setETagGetIndexStatus(
	metaToCache map[string]*LocalIndexMetadata, statsToCache map[string]*common.Statistics) (uint64, error) {

	// 1. Checksum (into eTag) the LocalIndexMetadata checksums
	var sb strings.Builder
	sbPtr := &sb // for reuse efficiency
	for _, localMeta := range metaToCache {
		if localMeta != nil {
			if localMeta.ETag == common.HTTP_VAL_ETAG_INVALID { // should never happen
				errMsg := fmt.Sprintf("RequestHandler::setETagGetIndexStatus invalid localMeta ETag: %x for IndexerId: %v",
					localMeta.ETag, localMeta.IndexerId)
				logging.Errorf(errMsg)
				err := errors.New(errMsg)
				return common.HTTP_VAL_ETAG_INVALID, err
			}
			fmt.Fprintf(sbPtr, "%016x", localMeta.ETag)
		}
	}
	bytes := []byte(sb.String())
	eTag := common.Crc64Checksum(bytes)

	// Add the stats to the eTag checksum
	bytes, err := json.Marshal(statsToCache)
	if err != nil {
		logging.Errorf("RequestHandler::setETagGetIndexStatus json.Marshal failed: %v", err.Error())
		return common.HTTP_VAL_ETAG_INVALID, err
	}
	eTag = common.Crc64Update(eTag, bytes)

	// Set the new global getIndexStatus ETag and expiry
	m.setGetIndexStatusETag(eTag)
	return eTag, nil
}

// consolidateIndexStatus consolidates the status entries of partitioned instances, which might be
// hosted on more than one node, into a single entry (with NodeUUID == "").
func (m *requestHandlerContext) consolidateIndexStatus(statuses []IndexStatus) []IndexStatus {

	statusMap := make(map[common.IndexInstId]IndexStatus)

	for _, status := range statuses {
		if s2, ok := statusMap[status.InstId]; !ok {
			status.NodeUUID = ""
			statusMap[status.InstId] = status
		} else {
			s2.Status = m.consolideStateStr(s2.Status, status.Status)
			s2.Hosts = append(s2.Hosts, status.Hosts...)
			s2.Completion = (s2.Completion + status.Completion) / 2
			s2.Progress = (s2.Progress + status.Progress) / 2.0
			s2.NumPartition += status.NumPartition
			s2.NodeUUID = ""
			if len(status.Error) != 0 {
				s2.Error = fmt.Sprintf("%v %v", s2.Error, status.Error)
			}

			for host, partitions := range status.PartitionMap {
				s2.PartitionMap[host] = partitions
			}
			s2.Stale = s2.Stale || status.Stale

			statusMap[status.InstId] = s2
		}
	}

	result := make([]IndexStatus, 0, len(statuses))
	for _, status := range statusMap {
		result = append(result, status)
	}

	return result
}

func (m *requestHandlerContext) consolideStateStr(str1 string, str2 string) string {

	if str1 == "Paused" || str2 == "Paused" {
		return "Paused"
	}

	if str1 == "Warmup" || str2 == "Warmup" {
		return "Warmup"
	}

	if strings.HasPrefix(str1, "Created") || strings.HasPrefix(str2, "Created") {
		if str1 == str2 {
			return str1
		}
		return "Created"
	}

	if strings.HasPrefix(str1, "Building") || strings.HasPrefix(str2, "Building") {
		if str1 == str2 {
			return str1
		}
		return "Building"
	}

	if str1 == "Moving" || str2 == "Moving" {
		return "Moving"
	}

	// must be ready
	return str1
}

//////////////////////////////////////////////////////
// Index Statement
///////////////////////////////////////////////////////

// handleIndexStatementRequest services the /getIndexStatement REST endpoint and returns
// the "create index" statements for (possibly a subset of) indexes on every indexer node.
// The caller can request statements for only a subset of indexes; additionally any indexes
// they do not have permissions for will be filtered out.
func (m *requestHandlerContext) handleIndexStatementRequest(w http.ResponseWriter, r *http.Request) {
	const method string = "RequestHandler::handleIndexStatementRequest" // for logging

	creds, ok := doAuth(r, w, method)
	if !ok {
		return
	}

	bucket := m.getBucket(r)
	scope := m.getScope(r)
	collection := m.getCollection(r)
	index := m.getIndex(r)

	t, err := validateRequest(bucket, scope, collection, index)
	if err != nil {
		logging.Debugf("%v: err %v", method, err)
		resp := &BackupResponse{Code: RESP_ERROR, Error: err.Error()}
		send(http.StatusInternalServerError, w, resp)
		return
	}

	statements, eTagResponse, err := m.getIndexStatement(creds, t)
	if err == nil {
		sort.Strings(statements)
		eTagRequest := getETagFromHttpHeader(r)
		if eTagRequest == eTagResponse { // eTagResponse is always fresh
			// Valid ETag; respond 304 Not Modified with the same ETag
			sendNotModified(w, eTagRequest)
		} else {
			sendWithETag(http.StatusOK, w, statements, eTagResponse)
		}
	} else {
		send(http.StatusInternalServerError, w, err.Error())
	}
}

// getIndexStatement is a helper for handleIndexStatementRequest. It delegates to getIndexStatus,
// then cherry picks the "create index" statements out of the results.
func (m *requestHandlerContext) getIndexStatement(creds cbauth.Creds, target *target) (
	statements []string, eTagResponse uint64, err error) {

	indexStatuses, failedNodes, eTagResponse, err := m.getIndexStatus(creds, target, false, false)
	if err != nil {
		return nil, common.HTTP_VAL_ETAG_INVALID, err
	}
	if len(failedNodes) != 0 {
		return nil, common.HTTP_VAL_ETAG_INVALID,
			errors.New(fmt.Sprintf("Failed to connect to indexer nodes %v", failedNodes))
	}

	defnMap := make(map[common.IndexDefnId]bool)
	statements = ([]string)(nil)
	for _, index := range indexStatuses {
		if _, ok := defnMap[index.DefnId]; !ok {
			defnMap[index.DefnId] = true
			statements = append(statements, index.Definition)
		}
	}

	return statements, eTagResponse, nil
}

///////////////////////////////////////////////////////
// ClusterIndexMetadata
///////////////////////////////////////////////////////

func (m *requestHandlerContext) handleIndexMetadataRequest(w http.ResponseWriter, r *http.Request) {
	const method string = "RequestHandler::handleIndexMetadataRequest" // for logging

	creds, ok := doAuth(r, w, method)
	if !ok {
		return
	}

	bucket := m.getBucket(r)
	scope := m.getScope(r)
	collection := m.getCollection(r)

	index := m.getIndex(r)
	if len(index) != 0 {
		err := fmt.Errorf("%v: err: Index level metadata requests are not supported", method)
		resp := &BackupResponse{Code: RESP_ERROR, Error: err.Error()}
		send(http.StatusInternalServerError, w, resp)
		return
	}

	t, err := validateRequest(bucket, scope, collection, index)
	if err != nil {
		logging.Debugf("%v: err %v", method, err)
		resp := &BackupResponse{Code: RESP_ERROR, Error: err.Error()}
		send(http.StatusInternalServerError, w, resp)
		return
	}

	meta, err := m.getIndexMetadata(creds, t)
	if err == nil {
		resp := &BackupResponse{Code: RESP_SUCCESS, Result: *meta}
		send(http.StatusOK, w, resp)
	} else {
		logging.Debugf("%v: err %v", method, err)
		resp := &BackupResponse{Code: RESP_ERROR, Error: err.Error()}
		send(http.StatusInternalServerError, w, resp)
	}
}

func (m *requestHandlerContext) getIndexMetadata(creds cbauth.Creds, target *target) (*ClusterIndexMetadata, error) {

	cinfo, err := m.mgr.FetchNewClusterInfoCache()
	if err != nil {
		return nil, err
	}

	permissionsCache := common.NewSessionPermissionsCache(creds)

	// find all nodes that has a index http service
	nids := cinfo.GetNodesByServiceType(common.INDEX_HTTP_SERVICE)

	clusterMeta := &ClusterIndexMetadata{Metadata: make([]LocalIndexMetadata, len(nids))}

	for i, nid := range nids {

		addr, err := cinfo.GetServiceAddress(nid, common.INDEX_HTTP_SERVICE, true)
		if err == nil {

			url := "/getLocalIndexMetadata"
			if len(target.bucket) != 0 {
				url += "?bucket=" + u.QueryEscape(target.bucket)
			}
			if len(target.scope) != 0 {
				url += "&scope=" + u.QueryEscape(target.scope)
			}
			if len(target.collection) != 0 {
				url += "&collection=" + u.QueryEscape(target.collection)
			}
			if len(target.index) != 0 {
				url += "&index=" + u.QueryEscape(target.index)
			}

			resp, err := getWithAuth(addr + url)
			if err != nil {
				logging.Debugf("RequestHandler::getIndexMetadata: Error while retrieving %v with auth %v", addr+"/getLocalIndexMetadata", err)
				return nil, errors.New(fmt.Sprintf("Fail to retrieve index definition from url %s", addr))
			}
			defer resp.Body.Close()

			localMeta := new(LocalIndexMetadata)
			status := convertResponse(resp, localMeta)
			if status == RESP_ERROR {
				return nil, errors.New(fmt.Sprintf("Fail to retrieve local metadata from url %s.", addr))
			}

			newLocalMeta := LocalIndexMetadata{
				IndexerId:   localMeta.IndexerId,
				NodeUUID:    localMeta.NodeUUID,
				StorageMode: localMeta.StorageMode,
			}

			for _, topology := range localMeta.IndexTopologies {
				if permissionsCache.IsAllowed(topology.Bucket, topology.Scope, topology.Collection, "list") {
					newLocalMeta.IndexTopologies = append(newLocalMeta.IndexTopologies, topology)
				}
			}

			for _, defn := range localMeta.IndexDefinitions {
				if permissionsCache.IsAllowed(defn.Bucket, defn.Scope, defn.Collection, "list") {
					newLocalMeta.IndexDefinitions = append(newLocalMeta.IndexDefinitions, defn)
				}
			}

			clusterMeta.Metadata[i] = newLocalMeta

		} else {
			return nil, errors.New(fmt.Sprintf("Fail to retrieve http endpoint for index node"))
		}
	}

	return clusterMeta, nil
}

func (m *requestHandlerContext) convertIndexMetadataRequest(r *http.Request) *ClusterIndexMetadata {
	var check map[string]interface{}

	meta := &ClusterIndexMetadata{}

	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(r.Body); err != nil {
		logging.Debugf("RequestHandler::convertIndexRequest: unable to read request body, err %v", err)
		return nil
	}

	logging.Debugf("requestHandler.convertIndexMetadataRequest(): input %v", string(buf.Bytes()))

	if err := json.Unmarshal(buf.Bytes(), &check); err != nil {
		logging.Debugf("RequestHandler::convertIndexMetadataRequest: unable to unmarshall request body. Buf = %s, err %v", buf, err)
		return nil
	} else if _, ok := check["metadata"]; !ok {
		logging.Debugf("RequestHandler::convertIndexMetadataRequest: invalid shape of request body. Buf = %s, err %v", buf, err)
		return nil
	}

	if err := json.Unmarshal(buf.Bytes(), meta); err != nil {
		logging.Debugf("RequestHandler::convertIndexMetadataRequest: unable to unmarshall request body. Buf = %s, err %v", buf, err)
		return nil
	}

	return meta
}

func validateRequest(bucket, scope, collection, index string) (*target, error) {
	// When bucket is not specified, return indexer level stats
	if len(bucket) == 0 {
		if len(scope) == 0 && len(collection) == 0 && len(index) == 0 {
			return &target{level: INDEXER_LEVEL}, nil
		}
		return nil, errors.New("Missing bucket parameter as scope/collection/index are specified")
	} else {
		// When bucket is specified and scope, collection are empty, return results
		// from all indexes belonging to that bucket
		if len(scope) == 0 && len(collection) == 0 {
			if len(index) != 0 {
				return nil, errors.New("Missing scope and collection parameters as index parameter is specified")
			}
			return &target{bucket: bucket, level: BUCKET_LEVEL}, nil
		} else if len(scope) != 0 && len(collection) == 0 {
			if len(index) != 0 {
				return nil, errors.New("Missing collection parameter as index parameter is specified")
			}

			return &target{bucket: bucket, scope: scope, level: SCOPE_LEVEL}, nil
		} else if len(scope) == 0 && len(collection) != 0 {
			return nil, errors.New("Missing scope parameter as collection paramter is specified")
		} else { // Both collection and scope are specified
			if len(index) != 0 {
				return &target{bucket: bucket, scope: scope, collection: collection, index: index, level: INDEX_LEVEL}, nil
			}
			return &target{bucket: bucket, scope: scope, collection: collection, level: COLLECTION_LEVEL}, nil
		}
		return nil, errors.New("Missing scope or collection parameters")
	}
	return nil, nil
}

func getFilters(r *http.Request, bucket string) (map[string]bool, string, error) {
	// Validation rules:
	//
	// 1. When include or exclude filter is specified, scope and collection
	//    parameters should NOT be specified.
	// 2. When include or exclude filter is specified, bucket parameter
	//    SHOULD be specified.
	// 3. Either include or exclude should be specified. Not both.

	include := r.FormValue("include")
	exclude := r.FormValue("exclude")
	scope := r.FormValue("scope")
	collection := r.FormValue("collection")

	if len(include) != 0 || len(exclude) != 0 {
		if len(bucket) == 0 {
			return nil, "", fmt.Errorf("Malformed input: include/exclude parameters are specified without bucket.")
		}

		if len(scope) != 0 || len(collection) != 0 {
			return nil, "", fmt.Errorf("Malformed input: include/exclude parameters are specified with scope/collection.")
		}
	}

	if len(include) != 0 && len(exclude) != 0 {
		return nil, "", fmt.Errorf("Malformed input: include and exclude both parameters are specified.")
	}

	getFilter := func(s string) string {
		comp := strings.Split(s, ".")
		if len(comp) == 1 || len(comp) == 2 {
			return s
		}

		return ""
	}

	filterType := ""
	filters := make(map[string]bool)

	if len(include) != 0 {
		filterType = "include"
		incl := strings.Split(include, ",")
		for _, inc := range incl {
			filter := getFilter(inc)
			if filter == "" {
				return nil, "", fmt.Errorf("Malformed input: include filter is malformed (%v) (%v)", incl, inc)
			}

			filters[filter] = true
		}
	}

	if len(exclude) != 0 {
		filterType = "exclude"
		excl := strings.Split(exclude, ",")
		for _, exc := range excl {
			filter := getFilter(exc)
			if filter == "" {
				return nil, "", fmt.Errorf("Malformed input: exclude filter is malformed (%v) (%v)", excl, exc)
			}

			filters[filter] = true
		}
	}

	// TODO: Do we need any more validations?
	return filters, filterType, nil
}

// applyFilters returns true iff an IndexDefn's (idxBucket, scope, collection, name)
// are selected by (bucket, filters, filterType).
func applyFilters(bucket, idxBucket, scope, collection, name string,
	filters map[string]bool, filterType string) bool {

	if bucket == "" {
		return true
	}

	if idxBucket != bucket {
		return false
	}

	if filterType == "" {
		return true
	}

	if _, ok := filters[scope]; ok {
		if filterType == "include" {
			return true
		} else {
			return false
		}
	}

	if _, ok := filters[fmt.Sprintf("%v.%v", scope, collection)]; ok {
		if filterType == "include" {
			return true
		} else {
			return false
		}
	}

	if name != "" {
		if _, ok := filters[fmt.Sprintf("%v.%v.%v", scope, collection, name)]; ok {
			if filterType == "include" {
				return true
			} else {
				return false
			}
		}
	}

	if filterType == "include" {
		return false
	}

	return true
}

func getRestoreRemapParam(r *http.Request) (map[string]string, error) {

	remap := make(map[string]string)

	remapStr := r.FormValue("remap")
	if remapStr == "" {
		return remap, nil
	}

	remaps := strings.Split(remapStr, ",")

	// Cache the collection level remaps for verification
	collRemap := make(map[string]string)

	for _, rm := range remaps {

		rmp := strings.Split(rm, ":")
		if len(rmp) > 2 || len(rmp) < 2 {
			return nil, fmt.Errorf("Malformed input. Missing source/target in remap %v", remapStr)
		}

		source := rmp[0]
		target := rmp[1]

		src := strings.Split(source, ".")
		tgt := strings.Split(target, ".")

		if len(src) != len(tgt) {
			return nil, fmt.Errorf("Malformed input. source and target in remap should be at same level %v", remapStr)
		}

		switch len(src) {

		case 2:
			// This is collection level remap
			// Search for overlapping scope level remap
			// Allow overlapping at the target, but not source
			if _, ok := remap[src[0]]; ok {
				return nil, fmt.Errorf("Malformed input. Overlapping remaps %v", remapStr)
			}

			remap[source] = target
			collRemap[src[0]] = src[1]

		case 1:
			// This is scope level remap.
			// Search for overlapping collection level remap
			// Allow overlapping at the target, but not source
			if _, ok := collRemap[source]; ok {
				return nil, fmt.Errorf("Malformed input. Overlapping remaps %v", remapStr)
			}

			remap[source] = target

		default:
			return nil, fmt.Errorf("Malformed input remap %v", remapStr)
		}
	}

	return remap, nil
}

///////////////////////////////////////////////////////
// LocalIndexMetadata
///////////////////////////////////////////////////////

// getETagFromHttpHeader returns the ETag from an HTTP request header, if present. It is
// expected to be a uint64 represented in hex. If it is missing or garbage, the parse
// returns 0, which (intentionally) is HTTP_KEY_ETAG_INVALID, so errors are ignored.
func getETagFromHttpHeader(req *http.Request) uint64 {
	eTagRequest, _ := strconv.ParseUint(req.Header.Get(common.HTTP_KEY_ETAG_REQUEST),
		common.HTTP_VAL_ETAG_BASE, 64)
	return eTagRequest
}

// eTagValid returns whether an ETag from an HTTP request is still valid by comparing it
// to the current ETag and the current time to the expiration time. (Order of first two
// arguments is actually arbitrary.)
func eTagValid(eTagRequest uint64, eTagCurrent uint64, eTagExpiry int64) bool {
	return eTagRequest == eTagCurrent && time.Now().UnixNano() < eTagExpiry
}

// handleLocalIndexMetadataRequest handles incoming requests for the /getLocalIndexMetadata
// REST endpoint. If the optional ETag request header field is set, this is the checksum of
// the previously returned results to the caller. If this checksum is still valid, return
// it with a 304 Not Modified response, else return the full metadata with latest checksum.
func (m *requestHandlerContext) handleLocalIndexMetadataRequest(w http.ResponseWriter, r *http.Request) {
	const method string = "RequestHandler::handleLocalIndexMetadataRequest" // for logging

	creds, ok := doAuth(r, w, method)
	if !ok {
		return
	}

	// By default use ETag.
	useETag := true
	if r.FormValue("useETag") == "false" {
		useETag = false
	}

	if useETag {
		// If metadata not dirty, can avoid collection and marshaling if caller passed a still-valid ETag
		if !m.mgr.getMetadataRepo().IsMetaDirty() {
			eTagRequest := getETagFromHttpHeader(r)
			if eTagRequest != common.HTTP_VAL_ETAG_INVALID { // also ...INVALID if missing or garbage
				cachedMeta := m.rhc.GetLocalIndexMetadataFromCache(m.hostKey)
				if cachedMeta != nil && eTagValid(eTagRequest, cachedMeta.ETag, cachedMeta.ETagExpiry) {
					// Valid ETag; respond 304 Not Modified with the same ETag
					sendNotModified(w, eTagRequest)
					return
				}
			}
		}
	}

	// Need to respond with the full local index metadata
	bucket := m.getBucket(r)
	scope := m.getScope(r)
	collection := m.getCollection(r)
	index := m.getIndex(r)
	if len(index) != 0 {
		err := fmt.Errorf("%v: err: Index level metadata requests are not supported", method)
		resp := &BackupResponse{Code: RESP_ERROR, Error: err.Error()}
		send(http.StatusBadRequest, w, resp)
		return
	}

	t, err := validateRequest(bucket, scope, collection, index)
	if err != nil {
		logging.Debugf("%v: err %v", method, err)
		errStr := fmt.Sprintf(" Unable to retrieve local index metadata due to: %v", err.Error())
		sendHttpError(w, errStr, http.StatusBadRequest)
		return
	}

	var filters map[string]bool
	var filterType string
	filters, filterType, err = getFilters(r, bucket)
	if err != nil {
		logging.Infof("%v: err %v", method, err)
		errStr := fmt.Sprintf(" Unable to retrieve local index metadata due to: %v", err.Error())
		sendHttpError(w, errStr, http.StatusBadRequest)
		return
	}

	if len(filters) == 0 {
		if t.level == SCOPE_LEVEL {
			filterType = "include"
			filters[t.scope] = true
		} else if t.level == COLLECTION_LEVEL {
			filterType = "include"
			filters[fmt.Sprintf("%v.%v", t.scope, t.collection)] = true
		} else if t.level == INDEX_LEVEL {
			filterType = "include"
			filters[fmt.Sprintf("%v.%v.%v", t.scope, t.collection, t.index)] = true
		}
	}

	meta, timingStr, err := m.getLocalIndexMetadata(creds, bucket, filters, filterType, useETag)
	if err == nil {
		if timingStr != "" {
			logging.Warnf("%v: req %v, %v", method, common.GetHTTPReqInfo(r), timingStr)
		}

		sendWithETag(http.StatusOK, w, meta, meta.ETag)
	} else {
		logging.Debugf("%v: err %v", method, err)
		sendHttpError(w, " Unable to retrieve index metadata", http.StatusInternalServerError)
	}
}

// getLocalIndexMetadata gets index metadata from the local metadata repo and,
// iff it is a full set AND caller specified useETag, sets its ETag info.
func (m *requestHandlerContext) getLocalIndexMetadata(creds cbauth.Creds,
	bucket string, filters map[string]bool, filterType string,
	useETag bool) (meta *LocalIndexMetadata, timingStr string, err error) {

	t0 := time.Now()

	repo := m.mgr.getMetadataRepo()
	permissionsCache := common.NewSessionPermissionsCache(creds)

	meta = &LocalIndexMetadata{IndexTopologies: nil, IndexDefinitions: nil}
	indexerId, err := repo.GetLocalIndexerId()
	if err != nil {
		return nil, "", err
	}
	meta.IndexerId = string(indexerId)

	nodeUUID, err := repo.GetLocalNodeUUID()
	if err != nil {
		return nil, "", err
	}
	meta.NodeUUID = string(nodeUUID)

	meta.StorageMode = string(common.StorageModeToIndexType(common.GetStorageMode()))
	meta.LocalSettings = make(map[string]string)

	retrievalTs := time.Now().UnixNano() // don't set in meta yet; breaks ETag checksum

	if exclude, err := m.mgr.GetLocalValue("excludeNode"); err == nil {
		meta.LocalSettings["excludeNode"] = exclude
	}

	t1 := time.Now()

	iter, err := repo.NewIterator()
	if err != nil {
		return nil, "", err
	}
	defer iter.Close()

	var defn *common.IndexDefn
	fullSet := true // do final results include all defns?
	_, defn, err = iter.Next()
	for err == nil {
		if applyFilters(bucket, defn.Bucket, defn.Scope, defn.Collection, defn.Name, filters, filterType) &&
			permissionsCache.IsAllowed(defn.Bucket, defn.Scope, defn.Collection, "list") {
			meta.IndexDefinitions = append(meta.IndexDefinitions, *defn)
		} else {
			fullSet = false
		}
		_, defn, err = iter.Next()
	}

	t2 := time.Now()

	iter1, err := repo.NewTopologyIterator()
	if err != nil {
		return nil, "", err
	}
	defer iter1.Close()

	var topology *IndexTopology
	topology, err = iter1.Next()
	for err == nil {
		if applyFilters(bucket, topology.Bucket, topology.Scope, topology.Collection, "", filters, filterType) &&
			permissionsCache.IsAllowed(topology.Bucket, topology.Scope, topology.Collection, "list") {
			meta.IndexTopologies = append(meta.IndexTopologies, *topology)
		} else {
			fullSet = false
		}
		topology, err = iter1.Next()
	}

	// meta.ETag and meta.ETagExpiry will stay 0 from object creation unless set here
	if fullSet && useETag {
		m.setETagLocalIndexMetadata(meta)
	}

	d0 := time.Since(t0)
	d1 := time.Since(t1)
	d2 := time.Since(t2)
	if int64(d2) > int64(20*time.Second) || int64(d1) > int64(20*time.Second) || int64(d0) > int64(20*time.Second) {
		timingStr = fmt.Sprintf("timings total %v, Iter %v, topoIter %v", d0, d1, d2)
	}

	meta.Timestamp = retrievalTs
	return meta, timingStr, nil
}

// setETagLocalIndexMetadata should only be called on a LocalIndexMetadata
// that inludes info about all IndexDefns of this host, as we never cache partial
// sets. It computes the ETag checksum and sets that ETag and its expiry in the
// object. It sets invalid ETag HTTP_VAL_ETAG_INVALID if the input could not be
// marshaled.
func (m *requestHandlerContext) setETagLocalIndexMetadata(meta *LocalIndexMetadata) {

	var bytes []byte // data to checksum
	var err error

	// If-else intentionally avoids creating metaCopy variable when not
	// needed (normal case) to avoid Go overhead of zeroing it out.
	if meta.Timestamp == 0 && meta.ETag == common.HTTP_VAL_ETAG_INVALID &&
		meta.ETagExpiry == 0 && !meta.AllIndexesActive {
		// Pseudo-fields are zero; checksum the original
		bytes, err = json.Marshal(meta)
	} else {
		// Pseudo-field(s) are set; shallow copy to zero them so they don't affect checksum
		metaCopy := *meta
		metaCopy.Timestamp = 0
		metaCopy.ETag = common.HTTP_VAL_ETAG_INVALID
		metaCopy.ETagExpiry = 0
		metaCopy.AllIndexesActive = false
		bytes, err = json.Marshal(metaCopy)
	}
	if err != nil {
		logging.Errorf("setETagLocalIndexMetadata: marshal metadata failed. Error: %v", err)
		meta.ETag = common.HTTP_VAL_ETAG_INVALID
		return
	}
	meta.ETag = common.Crc64Checksum(bytes)
	meta.ETagExpiry = m.generateETagExpiry()
}

// shouldProcess is a helper for getIndexStatus that determines whether a given index
// matches the target filter parameters of the request.
func shouldProcess(target *target, defnBucket, defnScope, defnColl, defnName string) bool {
	if target.level == INDEXER_LEVEL {
		return true
	}
	if target.level == BUCKET_LEVEL && (target.bucket == defnBucket) {
		return true
	}
	if target.level == SCOPE_LEVEL && (target.bucket == defnBucket) && target.scope == defnScope {
		return true
	}
	if target.level == COLLECTION_LEVEL && (target.bucket == defnBucket) && target.scope == defnScope && target.collection == defnColl {
		return true
	}
	if target.level == INDEX_LEVEL && (target.bucket == defnBucket) && target.scope == defnScope && target.collection == defnColl && target.index == defnName {
		return true
	}
	return false
}

///////////////////////////////////////////////////////
// Cached LocalIndexMetadata and Stats
///////////////////////////////////////////////////////

func (m *requestHandlerContext) handleCachedLocalIndexMetadataRequest(w http.ResponseWriter, r *http.Request) {
	const method string = "RequestHandler::handleCachedLocalIndexMetadataRequest:" // for logging

	creds, ok := doAuth(r, w, method)
	if !ok {
		return
	}

	permissionsCache := common.NewSessionPermissionsCache(creds)
	host := r.FormValue("host")
	host = strings.Trim(host, "\"")

	meta := m.rhc.GetLocalIndexMetadataFromCache(Host2key(host))
	if meta != nil {
		newMeta := *meta
		newMeta.IndexDefinitions = make([]common.IndexDefn, 0, len(meta.IndexDefinitions))
		newMeta.IndexTopologies = make([]IndexTopology, 0, len(meta.IndexTopologies))

		for _, defn := range meta.IndexDefinitions {
			if permissionsCache.IsAllowed(defn.Bucket, defn.Scope, defn.Collection, "list") {
				newMeta.IndexDefinitions = append(newMeta.IndexDefinitions, defn)
			}
		}

		for _, topology := range meta.IndexTopologies {
			if permissionsCache.IsAllowed(topology.Bucket, topology.Scope, topology.Collection, "list") {
				newMeta.IndexTopologies = append(newMeta.IndexTopologies, topology)
			}
		}

		send(http.StatusOK, w, newMeta)

	} else {
		logging.Debugf("%v host %v", method, host)
		msg := fmt.Sprintf("No cached LocalIndexMetadata available for %v", host)
		sendHttpError(w, msg, http.StatusNotFound)
	}
}

func (m *requestHandlerContext) handleCachedStats(w http.ResponseWriter, r *http.Request) {
	const method string = "RequestHandler::handleCachedStats:" // for logging

	_, ok := doAuth(r, w, method)
	if !ok {
		return
	}

	host := r.FormValue("host")
	host = strings.Trim(host, "\"")

	stats := m.rhc.GetStatsFromCache(Host2key(host))
	if stats != nil {
		send(http.StatusOK, w, stats)
	} else {
		logging.Debugf("%v host %v", method, host)
		msg := fmt.Sprintf("No cached stats available for %v", host)
		sendHttpError(w, msg, http.StatusNotFound)
	}
}

///////////////////////////////////////////////////////
// Restore
///////////////////////////////////////////////////////

//
// Restore semantic:
// 1) Each index is associated with the <IndexDefnId, IndexerId>.  IndexDefnId is unique for each index defnition,
//    and IndexerId is unique among the index nodes.  Note that IndexDefnId cannot be reused.
// 2) Index defn exists for the given <IndexDefnId, IndexerId> in current repository.  No action will be applied during restore.
// 3) Index defn is deleted or missing in current repository.  Index Defn restored from backup if bucket exists.
//    - Index defn of the same <bucket, name> exists.   It will rename the index to <index name>_restore_<seqNo>
//    - Bucket does not exist.   It will restore an index defn with a non-existent bucket.
//
// TODO (Collections): Any changes necessary will be handled as part of Backup-Restore task
func (m *requestHandlerContext) handleRestoreIndexMetadataRequest(w http.ResponseWriter, r *http.Request) {
	const method string = "RequestHandler::handleRestoreIndexMetadataRequest" // for logging

	creds, ok := doAuth(r, w, method)
	if !ok {
		return
	}

	permissionsCache := common.NewSessionPermissionsCache(creds)
	// convert backup image into runtime data structure
	image := m.convertIndexMetadataRequest(r)
	if image == nil {
		send(http.StatusBadRequest, w, &RestoreResponse{Code: RESP_ERROR, Error: "Unable to process request input"})
		return
	}

	for _, localMeta := range image.Metadata {
		for _, topology := range localMeta.IndexTopologies {
			if !permissionsCache.IsAllowed(topology.Bucket, topology.Scope, topology.Collection, "write") {
				return
			}
		}

		for _, defn := range localMeta.IndexDefinitions {
			if !permissionsCache.IsAllowed(defn.Bucket, defn.Scope, defn.Collection, "write") {
				return
			}
		}
	}

	// Restore
	bucket := m.getBucket(r)
	logging.Infof("restore to target bucket %v", bucket)

	context := createRestoreContext(image, m.clusterUrl, bucket, nil, "", nil)
	hostIndexMap, err := context.computeIndexLayout()
	if err != nil {
		send(http.StatusInternalServerError, w, &RestoreResponse{Code: RESP_ERROR, Error: fmt.Sprintf("Unable to restore metadata.  Error=%v", err)})
	}

	if err := m.restoreIndexMetadataToNodes(hostIndexMap); err == nil {
		send(http.StatusOK, w, &RestoreResponse{Code: RESP_SUCCESS})
	} else {
		send(http.StatusInternalServerError, w, &RestoreResponse{Code: RESP_ERROR, Error: fmt.Sprintf("%v", err)})
	}
}

func (m *requestHandlerContext) restoreIndexMetadataToNodes(hostIndexMap map[string][]*common.IndexDefn) error {

	var mu sync.Mutex
	var wg sync.WaitGroup

	errMap := make(map[string]error)

	restoreIndexes := func(host string, indexes []*common.IndexDefn) {
		defer wg.Done()

		for _, index := range indexes {
			if err := m.makeCreateIndexRequest(*index, host); err != nil {
				mu.Lock()
				defer mu.Unlock()

				errMap[host] = err
				return
			}
		}
	}

	for host, indexes := range hostIndexMap {
		wg.Add(1)
		go restoreIndexes(host, indexes)
	}

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	for _, err := range errMap {
		return err
	}

	return nil
}

func (m *requestHandlerContext) makeCreateIndexRequest(defn common.IndexDefn, host string) error {

	// deferred build for restore
	defn.Deferred = true

	req := IndexRequest{Version: uint64(1), Type: CREATE, Index: defn}
	body, err := json.Marshal(&req)
	if err != nil {
		logging.Errorf("requestHandler.makeCreateIndexRequest(): cannot marshall create index request %v", err)
		return err
	}

	bodybuf := bytes.NewBuffer(body)

	resp, err := postWithAuth(host+"/createIndex", "application/json", bodybuf)
	if err != nil {
		logging.Errorf("requestHandler.makeCreateIndexRequest(): create index request fails for %v/createIndex. Error=%v", host, err)
		return err
	}
	defer resp.Body.Close()

	response := new(IndexResponse)
	status := convertResponse(resp, response)
	if status == RESP_ERROR || response.Code == RESP_ERROR {
		logging.Errorf("requestHandler.makeCreateIndexRequest(): create index request fails. Error=%v", response.Error)
		return fmt.Errorf("%v: %v", response.Error, response.Message)
	}

	return nil
}

//////////////////////////////////////////////////////
// Planner
///////////////////////////////////////////////////////

func (m *requestHandlerContext) handleIndexPlanRequest(w http.ResponseWriter, r *http.Request) {
	const method string = "RequestHandler::handleIndexPlanRequest" // for logging

	_, ok := doAuth(r, w, method)
	if !ok {
		return
	}

	stmts, err := m.getIndexPlan(r)
	if err == nil {
		send(http.StatusOK, w, stmts)
	} else {
		sendHttpError(w, err.Error(), http.StatusInternalServerError)
	}
}

func (m *requestHandlerContext) getIndexPlan(r *http.Request) (string, error) {
	const method string = "RequestHandler::getIndexPlan" // for logging

	plan, err := planner.RetrievePlanFromCluster(m.clusterUrl, nil, false)
	if err != nil {
		return "", fmt.Errorf("%v: Fail to retrieve index information from cluster. err: %v", method, err)
	}

	specs, err := m.convertIndexPlanRequest(r)
	if err != nil {
		return "", fmt.Errorf("%v: Fail to read index spec from request. err: %v", method, err)
	}

	solution, err := planner.ExecutePlanWithOptions(plan, specs, true, "", "", 0, -1, -1, false, true, m.useGreedyPlanner)
	if err != nil {
		return "", fmt.Errorf("%v: Fail to plan index. err: %v", method, err)
	}

	return planner.CreateIndexDDL(solution), nil
}

func (m *requestHandlerContext) convertIndexPlanRequest(r *http.Request) ([]*planner.IndexSpec, error) {

	var specs []*planner.IndexSpec

	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(r.Body); err != nil {
		logging.Debugf("RequestHandler::convertIndexPlanRequest: unable to read request body, err %v", err)
		return nil, err
	}

	logging.Debugf("requestHandler.convertIndexPlanRequest(): input %v", string(buf.Bytes()))

	if err := json.Unmarshal(buf.Bytes(), &specs); err != nil {
		logging.Debugf("RequestHandler::convertIndexPlanRequest: unable to unmarshall request body. Buf = %s, err %v", buf, err)
		return nil, err
	}

	return specs, nil
}

//////////////////////////////////////////////////////
// Storage Mode
///////////////////////////////////////////////////////

func (m *requestHandlerContext) handleIndexStorageModeRequest(w http.ResponseWriter, r *http.Request) {
	const method string = "RequestHandler::handleIndexStorageModeRequest" // for logging

	creds, ok := doAuth(r, w, method)
	if !ok {
		return
	}

	if !isAllowed(creds, []string{"cluster.settings!write"}, r, w, method) {
		return
	}

	// Override the storage mode for the local indexer.  Override will not take into effect until
	// indexer has restarted manually by administrator.   During indexer bootstrap, it will upgrade/downgrade
	// individual index to the override storage mode.
	value := r.FormValue("downgrade")
	if len(value) != 0 {
		downgrade, err := strconv.ParseBool(value)
		if err == nil {
			if downgrade {
				if common.GetStorageMode() == common.StorageMode(common.PLASMA) {

					nodeUUID, err := m.mgr.getMetadataRepo().GetLocalNodeUUID()
					if err != nil {
						logging.Infof("%v: Unable to identify nodeUUID.  Cannot downgrade.", method)
						send(http.StatusOK, w, "Unable to identify nodeUUID.  Cannot downgrade.")
						return
					}

					mc.PostIndexerStorageModeOverride(string(nodeUUID), common.ForestDB)
					logging.Infof("%v: Set override storage mode to forestdb", method)
					send(http.StatusOK, w, "Downgrade storage mode to forestdb after indexer restart.")
				} else {
					logging.Infof("%v: Local storage mode is not plasma.  Cannot downgrade.", method)
					send(http.StatusOK, w, "Indexer storage mode is not plasma.  Cannot downgrade.")
				}
			} else {
				nodeUUID, err := m.mgr.getMetadataRepo().GetLocalNodeUUID()
				if err != nil {
					logging.Infof("%v: Unable to identify nodeUUID.  Cannot disable storage mode downgrade.",
						method)
					send(http.StatusOK, w, "Unable to identify nodeUUID.  Cannot disable storage mode downgrade.")
					return
				}

				mc.PostIndexerStorageModeOverride(string(nodeUUID), "")
				logging.Infof("RequestHandler::handleIndexStorageModeRequst: unset storage mode override")
				send(http.StatusOK, w, "storage mode downgrade is disabled")
			}
		} else {
			sendHttpError(w, err.Error(), http.StatusBadRequest)
		}
	} else {
		sendHttpError(w, "missing argument `override`", http.StatusBadRequest)
	}
}

//////////////////////////////////////////////////////
// Planner
///////////////////////////////////////////////////////

func (m *requestHandlerContext) handlePlannerRequest(w http.ResponseWriter, r *http.Request) {
	const method string = "RequestHandler::handlePlannerRequest" // for logging

	creds, ok := doAuth(r, w, method)
	if !ok {
		return
	}

	if !isAllowed(creds, []string{"cluster.settings!write"}, r, w, method) {
		return
	}

	value := r.FormValue("excludeNode")
	if value == "in" || value == "out" || value == "inout" || len(value) == 0 {
		m.mgr.SetLocalValue("excludeNode", value)
		send(http.StatusOK, w, "OK")
	} else {
		sendHttpError(w, "value must be in, out or inout", http.StatusBadRequest)
	}
}

//////////////////////////////////////////////////////
// Alter Index
///////////////////////////////////////////////////////

func (m *requestHandlerContext) handleListLocalReplicaCountRequest(w http.ResponseWriter, r *http.Request) {
	const method string = "RequestHandler::handleListLocalReplicaCountRequest" // for logging

	creds, ok := doAuth(r, w, method)
	if !ok {
		return
	}

	result, err := m.getLocalReplicaCount(creds)
	if err == nil {
		send(http.StatusOK, w, result)
	} else {
		logging.Debugf("%v: err %v", method, err)
		sendHttpError(w, " Unable to retrieve index metadata", http.StatusInternalServerError)
	}
}

func (m *requestHandlerContext) getLocalReplicaCount(creds cbauth.Creds) (map[common.IndexDefnId]common.Counter, error) {

	createCommandTokenMap, err := mc.FetchIndexDefnToCreateCommandTokensMap()
	if err != nil {
		return nil, err
	}

	dropInstanceCommandTokenMap, err := mc.FetchIndexDefnToDropInstanceCommandTokenMap()
	if err != nil {
		return nil, err
	}

	result := make(map[common.IndexDefnId]common.Counter)

	repo := m.mgr.getMetadataRepo()
	iter, err := repo.NewIterator()
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var defn *common.IndexDefn
	permissionsCache := common.NewSessionPermissionsCache(creds)

	_, defn, err = iter.Next()
	for err == nil {
		if !permissionsCache.IsAllowed(defn.Bucket, defn.Scope, defn.Collection, "list") {
			return nil, fmt.Errorf("Permission denied on reading metadata for keyspace %v:%v:%v", defn.Bucket, defn.Scope, defn.Collection)
		}

		createTokenList := createCommandTokenMap[defn.DefnId]
		dropInstTokenList := dropInstanceCommandTokenMap[defn.DefnId]

		var numReplica *common.Counter
		numReplica, err = GetLatestReplicaCountFromTokens(defn, createTokenList, dropInstTokenList)
		if err != nil {
			return nil, fmt.Errorf("Fail to retreive replica count.  Error: %v", err)
		}

		result[defn.DefnId] = *numReplica
		_, defn, err = iter.Next()
	}

	return result, nil
}

///////////////////////////////////////////////////////
// Utility
///////////////////////////////////////////////////////

func sendIndexResponseWithError(status int, w http.ResponseWriter, msg string) {
	res := &IndexResponse{Code: RESP_ERROR, Error: msg}
	send(status, w, res)
}

func sendIndexResponse(w http.ResponseWriter) {
	result := &IndexResponse{Code: RESP_SUCCESS}
	send(http.StatusOK, w, result)
}

// send sends an HTTP(S) response of the specified status on success.
// res is the response payload.
func send(status int, w http.ResponseWriter, res interface{}) {
	sendWithETag(status, w, res, common.HTTP_VAL_ETAG_INVALID)
}

// sendWithETag sends an HTTP(S) response of the specified status on success.
// res is the response payload. If non-zero, eTag is the most recent checksum
// and is also sent; if zero it is not sent.
func sendWithETag(status int, w http.ResponseWriter, res interface{}, eTag uint64) {
	header := w.Header()
	header[common.HTTP_KEY_CONTENT_TYPE] = []string{common.HTTP_VAL_APPLICATION_JSON}
	if eTag != common.HTTP_VAL_ETAG_INVALID {
		header[common.HTTP_KEY_ETAG_RESPONSE] = []string{strconv.FormatUint(eTag, common.HTTP_VAL_ETAG_BASE)}
	}

	if buf, err := json.Marshal(res); err == nil {
		w.WriteHeader(status)
		logging.Tracef("RequestHandler::sendResponse: sending response back to caller. %v", logging.TagStrUD(buf))
		w.Write(buf)
	} else {
		// note : buf is nil if err != nil
		logging.Debugf("RequestHandler::sendResponse: fail to marshall response back to caller. %s", err)
		sendHttpError(w, "RequestHandler::sendResponse: Unable to marshall response", http.StatusInternalServerError)
	}
}

// sendNotModified sends an HTTP(S) 304 Not Modified response and the current ETag again.
func sendNotModified(w http.ResponseWriter, eTagResponse uint64) {
	header := w.Header()
	header[common.HTTP_KEY_CONTENT_TYPE] = []string{common.HTTP_VAL_APPLICATION_JSON}
	header[common.HTTP_KEY_ETAG_RESPONSE] = []string{strconv.FormatUint(eTagResponse, common.HTTP_VAL_ETAG_BASE)}

	w.WriteHeader(http.StatusNotModified)
	logging.Tracef("RequestHandler::sendNotModified: sending StatusNotModified %v response back to caller.",
		http.StatusNotModified)
	w.Write(nil)
}

func sendHttpError(w http.ResponseWriter, reason string, code int) {
	http.Error(w, reason, code)
}

// convertResponse attempts to unmarshal the body of an HTTP(S) response into an
// object the caller passes in. They must pass in an object of the correct type.
// Currently this does not verify that r.StatusCode == http.StatusOK, so the
// unmarshaling could fail because the expected payload is not present.
func convertResponse(r *http.Response, resp interface{}) string {

	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(r.Body); err != nil {
		logging.Debugf("RequestHandler::convertResponse: unable to read request body, err %v", err)
		return RESP_ERROR
	}

	if err := json.Unmarshal(buf.Bytes(), resp); err != nil {
		logging.Debugf("convertResponse: unable to unmarshall response body. Buf = %s, err %v", buf, err)
		return RESP_ERROR
	}

	return RESP_SUCCESS
}

// doAuth authenticates credentials from an HTTP request and responds
// directly to bad request and unauthorized failures, else leaves it up to
// the caller to respond.
// calledBy is "Class::Method" of calling function for auditing.
func doAuth(r *http.Request, w http.ResponseWriter, calledBy string) (cbauth.Creds, bool) {
	const method string = "request_handler::doAuth" // for logging

	creds, valid, err := common.IsAuthValid(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error() + "\n"))
		return nil, false
	} else if valid == false {
		audit.Audit(common.AUDIT_UNAUTHORIZED, r, method, "Called by "+calledBy)
		w.WriteHeader(http.StatusUnauthorized)
		w.Write(common.HTTP_STATUS_UNAUTHORIZED)
		return nil, false
	}

	return creds, true
}

// isAllowed checks whether creds has ANY of the specified permissions.
// calledBy is "Class::Method" of calling function for auditing.
// TODO: This function shouldn't always return IndexResponse on error in
// verifying auth. It should depend on the caller.
func isAllowed(creds cbauth.Creds, permissions []string, r *http.Request,
	w http.ResponseWriter, calledBy string) bool {
	const method string = "request_handler::isAllowed" // for logging

	allow := false
	err := error(nil)

	for _, permission := range permissions {
		allow, err = creds.IsAllowed(permission)
		if allow && err == nil {
			break
		}
	}

	if err != nil {
		if w != nil {
			sendIndexResponseWithError(http.StatusInternalServerError, w, err.Error())
		}
		return false
	}

	if !allow {
		if w != nil {
			audit.Audit(common.AUDIT_FORBIDDEN, r, method, "Called by "+calledBy)
			w.WriteHeader(http.StatusForbidden)
			w.Write(common.HTTP_STATUS_FORBIDDEN)
		}
		return false
	}

	return true
}

// getWithAuth does an HTTP(S) GET request with Basic Authentication.
func getWithAuth(url string) (*http.Response, error) {
	return getWithAuthAndETag(url, common.HTTP_VAL_ETAG_INVALID)
}

// getWithAuthAndETag does an HTTP(S) GET request with Basic Authentication
// and an optional ETag. (If the ETag is 0 it is invalid and not transmitted.)
// The caller should be able to handle a response of http.StatusNotModified (304).
func getWithAuthAndETag(url string, eTag uint64) (*http.Response, error) {
	params := &security.RequestParams{Timeout: time.Duration(10) * time.Second}
	var eTagString string // "" means do not transmit, used for 0 eTag value
	if eTag != common.HTTP_VAL_ETAG_INVALID {
		eTagString = strconv.FormatUint(eTag, common.HTTP_VAL_ETAG_BASE)
	}
	return security.GetWithAuthAndETag(url, params, eTagString)
}

func postWithAuth(url string, bodyType string, body io.Reader) (*http.Response, error) {
	params := &security.RequestParams{Timeout: time.Duration(120) * time.Second}
	return security.PostWithAuth(url, bodyType, body, params)
}

func findTopologyByCollection(topologies []IndexTopology, bucket, scope, collection string) *IndexTopology {

	for _, topology := range topologies {
		t := &topology
		t.SetCollectionDefaults()
		if t.Bucket == bucket && t.Scope == scope && t.Collection == collection {
			return t
		}
	}

	return nil
}

///////////////////////////////////////////////////////
// indexStatusSorter
///////////////////////////////////////////////////////

func (s indexStatusSorter) Len() int {
	return len(s)
}

func (s indexStatusSorter) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// TODO (Collections): Revisit scalabity of sorting with large number of indexes
// Also, check if sorting still necessary.
func (s indexStatusSorter) Less(i, j int) bool {
	if s[i].Name < s[j].Name {
		return true
	}

	if s[i].Name > s[j].Name {
		return false
	}

	if s[i].Collection < s[j].Collection {
		return true
	}

	if s[i].Collection > s[j].Collection {
		return false
	}

	if s[i].Scope < s[j].Scope {
		return true
	}

	if s[i].Scope > s[j].Scope {
		return false
	}

	return s[i].Bucket < s[j].Bucket
}

///////////////////////////////////////////////////////
// retrieve / persist cached local index metadata
///////////////////////////////////////////////////////

// getLocalIndexMetadataForNode will retrieve the latest LocalIndexMetadata for a given
// indexer node if they are available anywhere. If the source host is responding they will
// be from that host (or from local cache if the source host reported they have not changed
// based on the cached ETag), else they will be from whatever indexer host has the most
// recent cached version.
func (m *requestHandlerContext) getLocalIndexMetadataForNode(
	addr string, host string, ninfo common.NodesInfoProvider) (
	localMeta *LocalIndexMetadata, latest bool, isFromCache bool, err error) {

	meta, isFromCache, err := m.getLocalIndexMetadataFromREST(addr, host)
	if err == nil {
		return meta, true, isFromCache, nil
	}

	if ninfo.GetClusterVersion() >= common.INDEXER_65_VERSION {
		var latest *LocalIndexMetadata
		nids := ninfo.GetNodesByServiceType(common.INDEX_HTTP_SERVICE)
		for _, nid := range nids {
			addr, err1 := ninfo.GetServiceAddress(nid, common.INDEX_HTTP_SERVICE, true)
			if err1 == nil {
				cached, err1 := m.getCachedLocalIndexMetadataFromREST(addr, host)
				if cached != nil && err1 == nil {
					if latest == nil || cached.Timestamp > latest.Timestamp {
						latest = cached
					}
				}
			}
		}

		if latest != nil {
			return latest, false, false, nil
		}
	}

	return nil, false, false, err
}

// getLocalIndexMetadataFromREST gets the LocalIndexMetadata values from a (usually remote)
// indexer node. It uses ETags to avoid regetting a payload that is cached locally and has
// not changed; return value isFromCache is true iff that was the case.
func (m *requestHandlerContext) getLocalIndexMetadataFromREST(addr string, hostname string) (
	localMeta *LocalIndexMetadata, isFromCache bool, err error) {

	var eTag uint64 // 0 = missing or invalid
	metaCached := m.rhc.GetLocalIndexMetadataFromCache(Host2key(hostname))
	if metaCached != nil {
		eTag = metaCached.ETag
	}

	resp, err := getWithAuthAndETag(addr+"/getLocalIndexMetadata", eTag)
	defer func() {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
	}()
	if err == nil {
		// StatusNotModified can only occur if metaCached was retrieved from cache, as
		// that is the only time we may send a valid ETag in the request to trigger it.
		if metaCached != nil && resp.StatusCode == http.StatusNotModified {
			return metaCached, true, nil
		}

		// Process newly retrieved payload
		localMeta := new(LocalIndexMetadata)
		if status := convertResponse(resp, localMeta); status == RESP_SUCCESS {
			return localMeta, false, nil
		}
		err = fmt.Errorf("Fail to unmarshal response from %v", hostname)
	}
	return nil, false, err
}

func (m *requestHandlerContext) getCachedLocalIndexMetadataFromREST(addr string, host string) (*LocalIndexMetadata, error) {

	resp, err := getWithAuth(fmt.Sprintf("%v/getCachedLocalIndexMetadata?host=\"%v\"", addr, host))
	defer func() {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
	}()

	if err == nil {
		localMeta := new(LocalIndexMetadata)
		if status := convertResponse(resp, localMeta); status == RESP_SUCCESS {
			return localMeta, nil
		}

		err = fmt.Errorf("Fail to unmarshal response from %v", host)
	}

	return nil, err
}

///////////////////////////////////////////////////////
// retrieve / persist cached index stats
///////////////////////////////////////////////////////

// getStatsForNode will retrieve the latest IndexStats subset for a given indexer node
// if they are available anywhere. If the source host is responding they will be from
// that host (or from local cache if the source host reported they have not changed
// based on the cached ETag), else they will be from whatever indexer host has the
// most recent cached version.
func (m *requestHandlerContext) getStatsForNode(addr string, host string, ninfo common.NodesInfoProvider) (stats *common.Statistics, latest bool, err error) {

	stats, err = m.getStatsFromREST(addr, host)
	if err == nil {
		return stats, true, nil
	}

	if ninfo.GetClusterVersion() >= common.INDEXER_65_VERSION {
		var latest *common.Statistics
		nids := ninfo.GetNodesByServiceType(common.INDEX_HTTP_SERVICE)
		for _, nid := range nids {
			addr, err1 := ninfo.GetServiceAddress(nid, common.INDEX_HTTP_SERVICE, true)
			if err1 == nil {
				cached, err1 := m.getCachedStatsFromREST(addr, host)
				if cached != nil && err1 == nil {
					if latest == nil {
						latest = cached
						continue
					}

					ts1 := latest.Get("timestamp")
					if ts1 == nil {
						latest = cached
						continue
					}

					ts2 := cached.Get("timestamp")
					if ts2 == nil {
						continue
					}

					t1, ok1 := ts1.(float64)
					t2, ok2 := ts2.(float64)

					if ok1 && ok2 {
						if t2 > t1 {
							latest = cached
						}
					}
				}
			}
		}

		if latest != nil {
			return latest, false, nil
		}
	}

	return nil, false, err
}

// getStatsFromREST gets a subset of IndexStats from a (usually remote) indexer node.
func (m *requestHandlerContext) getStatsFromREST(addr string, hostname string) (*common.Statistics, error) {

	resp, err := getWithAuth(addr + "/stats?async=true&consumerFilter=indexStatus")
	defer func() {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
	}()

	if err == nil {
		// Process newly retrieved payload
		stats := new(common.Statistics)
		if status := convertResponse(resp, stats); status == RESP_SUCCESS {
			return stats, nil
		}
		err = fmt.Errorf("Fail to unmarshal response from %v", hostname)
	}
	return nil, err
}

func (m *requestHandlerContext) getCachedStatsFromREST(addr string, host string) (*common.Statistics, error) {

	resp, err := getWithAuth(fmt.Sprintf("%v/getCachedStats?host=\"%v\"", addr, host))
	defer func() {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
	}()

	if err == nil {
		stats := new(common.Statistics)
		if status := convertResponse(resp, stats); status == RESP_SUCCESS {
			return stats, nil
		}

		err = fmt.Errorf("Fail to unmarshal response from %v", host)
	}

	return nil, err
}

func (m *requestHandlerContext) handleScheduleCreateRequest(w http.ResponseWriter, r *http.Request) {
	const method string = "RequestHandler::handleScheduleCreateRequest" // for logging

	creds, ok := doAuth(r, w, method)
	if !ok {
		return
	}

	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(r.Body); err != nil {
		logging.Debugf("%v: Unable to read request body, err: %v", method, err)
		send(http.StatusBadRequest, w, "Unable to read request body")
		return
	}

	req := &client.ScheduleCreateRequest{}
	if err := json.Unmarshal(buf.Bytes(), req); err != nil {
		logging.Debugf("%v: Unable to unmarshal request body. Buf = %s, err: %v",
			method, logging.TagStrUD(buf), err)
		send(http.StatusBadRequest, w, "Unable to unmarshal request body")
		return
	}

	if req.Definition.DefnId == common.IndexDefnId(0) {
		logging.Warnf("%v: Empty index definition", method)
		send(http.StatusBadRequest, w, "Empty index definition")
		return
	}

	permission := fmt.Sprintf("cluster.collection[%s:%s:%s].n1ql.index!create", req.Definition.Bucket, req.Definition.Scope, req.Definition.Collection)
	if !isAllowed(creds, []string{permission}, r, w, method) {
		msg := "Specified user cannot create an index on the bucket"
		audit.Audit(common.AUDIT_FORBIDDEN, r, method, msg)
		send(http.StatusForbidden, w, msg)
		return
	}

	err := m.processScheduleCreateRequest(req)
	if err != nil {
		msg := fmt.Sprintf("Error in processing schedule create token: %v", err)
		logging.Errorf("%v: %v", method, msg)
		send(http.StatusInternalServerError, w, msg)
		return
	}

	send(http.StatusOK, w, "OK")
}

func (m *requestHandlerContext) validateScheduleCreateRequest(req *client.ScheduleCreateRequest) (string, string, string, error) {

	// Check for all possible fail-fast situations. Fail scheduling of index
	// creation if any of the required preconditions are not satisfied.

	defn := req.Definition

	if common.GetBuildMode() != common.ENTERPRISE {
		if defn.NumReplica != 0 {
			err := errors.New("Index Replica not supported in non-Enterprise Edition")
			return "", "", "", err
		}
		if common.IsPartitioned(defn.PartitionScheme) {
			err := errors.New("Index Partitioning is not supported in non-Enterprise Edition")
			return "", "", "", err
		}
	}

	// Check for bucket, scope, collection to be present.
	var bucketUUID, scopeId, collectionId string
	var err error

	m.mgr.cinfoProviderLockReqHandler.RLock()
	collnInfo, err := m.mgr.cinfoProviderReqHandler.GetCollectionInfoProvider(defn.Bucket)
	m.mgr.cinfoProviderLockReqHandler.RUnlock()
	if err != nil || collnInfo == nil {
		errMsg := "ClusterInfoCache unavailable in IndexManager"
		logging.Errorf("validateScheduleCreateRequest: %v", errMsg)
		err = errors.New(errMsg)
		return "", "", "", err
	}

	err = collnInfo.FetchBucketInfo(defn.Bucket)
	if err != nil {
		errMsg := "ClusterInfoCache unable to FetchBucketInfo"
		logging.Errorf("validateScheduleCreateRequest: %v", errMsg)
		err = errors.New(errMsg)
		return "", "", "", err
	}

	err = collnInfo.FetchManifestInfo(defn.Bucket)
	if err != nil {
		errMsg := "ClusterInfoCache unable to FetchManifestInfo"
		logging.Errorf("validateScheduleCreateRequest: %v", errMsg)
		err = errors.New(errMsg)
		return "", "", "", err
	}

	m.mgr.cinfoProviderLockReqHandler.RLock()
	bucketUUID, err = m.mgr.cinfoProviderReqHandler.GetBucketUUID(defn.Bucket)
	m.mgr.cinfoProviderLockReqHandler.RUnlock()
	if err != nil || bucketUUID == common.BUCKET_UUID_NIL {
		return "", "", "", common.ErrBucketNotFound
	}

	scopeId, collectionId = collnInfo.ScopeAndCollectionID(defn.Bucket, defn.Scope, defn.Collection)
	if scopeId == collections.SCOPE_ID_NIL {
		return "", "", "", common.ErrScopeNotFound
	}

	if collectionId == collections.COLLECTION_ID_NIL {
		return "", "", "", common.ErrCollectionNotFound
	}

	if common.GetStorageMode() == common.NOT_SET {
		return "", "", "", fmt.Errorf("Please Set Indexer Storage Mode Before Create Index")
	}

	err = m.validateStorageMode(&defn)
	if err != nil {
		return "", "", "", err
	}

	// TODO: Check indexer state to be active

	var ephemeral bool
	ephemeral, err = m.isEphemeral(defn.Bucket)
	if err != nil {
		logging.Errorf("validateScheduleCreateRequest: isEphemeral %v", err)
		return "", "", "", err
	}

	if ephemeral {
		allowed, reason, err := m.isAllowedEphemeral(defn.Bucket)
		if err != nil {
			err1 := fmt.Errorf("Cannot check if index creation is allowed on ephemeral bucket %v. Error %v",
				defn.Bucket, err)
			logging.Errorf("validateScheduleCreateRequest: isAllowedEphemeral %v", err1)
			return "", "", "", err1
		}

		if !allowed {
			logging.Errorf("validateScheduleCreateRequest: %v", reason)
			return "", "", "", fmt.Errorf("Standard GSI Index on ephemeral bucket requires fully upgraded cluster")
		}
	}

	return bucketUUID, scopeId, collectionId, nil
}

func (m *requestHandlerContext) isAllowedEphemeral(bucket string) (bool, string, error) {

	m.mgr.cinfoProviderLockReqHandler.RLock()
	defer m.mgr.cinfoProviderLockReqHandler.RUnlock()

	ninfo, err := m.mgr.cinfoProviderReqHandler.GetNodesInfoProvider()
	if err != nil || ninfo == nil {
		return false, "", errors.New("ClusterInfoCache unavailable in IndexManager")
	}

	if common.GetStorageMode() == common.MOI {
		return true, "", nil
	}

	ninfo.RLock()
	cVersion := ninfo.GetClusterVersion()
	ninfo.RUnlock()

	if cVersion < common.INDEXER_70_VERSION {
		retMsg := fmt.Sprintf("Bucket %v is Ephemeral. Standard GSI index on Ephemeral buckets"+
			" is supported only on fully upgraded cluster.", bucket)
		return false, retMsg, nil
	}

	if cVersion >= common.INDEXER_71_VERSION {
		return true, "", nil
	}

	ver, err := common.GetInternalClusterVersion(ninfo)
	if err != nil {
		return false, "", err
	}

	logging.Infof("requestHandlerContext:isAllowedEphemeral While creating index on ephemeral bucket %v, internal indexer version is (%v)", bucket, ver)

	if ver.LessThan(common.InternalVersion(common.MIN_VER_STD_GSI_EPHEMERAL)) {
		retMsg := fmt.Sprintf("Bucket %v is Ephemeral. Standard GSI index on Ephemeral buckets"+
			" is supported only on fully upgraded cluster.", bucket)
		return false, retMsg, nil
	}

	return true, "", nil
}

func (m *requestHandlerContext) isEphemeral(bucket string) (bool, error) {
	m.mgr.cinfoProviderLockReqHandler.RLock()
	defer m.mgr.cinfoProviderLockReqHandler.RUnlock()

	binfo, err := m.mgr.cinfoProviderReqHandler.GetBucketInfoProvider(bucket)
	if err != nil || binfo == nil {
		return false, errors.New("ClusterInfoCache unavailable in IndexManager")
	}

	binfo.RLock()
	defer binfo.RUnlock()

	return binfo.IsEphemeral(bucket)
}

func (m *requestHandlerContext) validateStorageMode(defn *common.IndexDefn) error {

	//if no index_type has been specified
	if strings.ToLower(string(defn.Using)) == "gsi" {
		if common.GetStorageMode() != common.NOT_SET {
			//if there is a storage mode, default to that
			defn.Using = common.IndexType(common.GetStorageMode().String())
		} else {
			//default to plasma
			defn.Using = common.PlasmaDB
		}
	} else {
		if common.IsValidIndexType(string(defn.Using)) {
			defn.Using = common.IndexType(strings.ToLower(string(defn.Using)))
		} else {
			err := fmt.Sprintf("Create Index fails. Reason = Unsupported Using Clause %v", string(defn.Using))
			return errors.New(err)
		}
	}

	if common.IsPartitioned(defn.PartitionScheme) {
		if defn.Using != common.PlasmaDB && defn.Using != common.MemDB && defn.Using != common.MemoryOptimized {
			err := fmt.Sprintf("Create Index fails. Reason = Cannot create partitioned index using %v", string(defn.Using))
			return errors.New(err)
		}
	}

	if common.IndexTypeToStorageMode(defn.Using) != common.GetStorageMode() {
		return fmt.Errorf("Cannot Create Index with Using %v. Indexer Storage Mode %v",
			defn.Using, common.GetStorageMode())
	}

	return nil
}

// This function returns an error if it cannot connect for fetching bucket info.
// It returns BUCKET_UUID_NIL (err == nil) if bucket does not exist.
//
func (m *requestHandlerContext) getBucketUUID(bucket string) (string, error) {
	count := 0
RETRY:
	uuid, err := common.GetBucketUUID(m.clusterUrl, bucket)
	if err != nil && count < 5 {
		count++
		time.Sleep(time.Duration(100) * time.Millisecond)
		goto RETRY
	}

	if err != nil {
		return common.BUCKET_UUID_NIL, err
	}

	return uuid, nil
}

// This function returns an error if it cannot connect for fetching manifest info.
// It returns SCOPE_ID_NIL, COLLECTION_ID_NIL (err == nil) if scope, collection does
// not exist.
//
func (m *requestHandlerContext) getScopeAndCollectionID(bucket, scope, collection string) (string, string, error) {
	count := 0
RETRY:
	scopeId, colldId, err := common.GetScopeAndCollectionID(m.clusterUrl, bucket, scope, collection)
	if err != nil && count < 5 {
		count++
		time.Sleep(time.Duration(100) * time.Millisecond)
		goto RETRY
	}

	if err != nil {
		return "", "", err
	}

	return scopeId, colldId, nil
}

func (m *requestHandlerContext) processScheduleCreateRequest(req *client.ScheduleCreateRequest) error {
	bucketUUID, scopeId, collectionId, err := m.validateScheduleCreateRequest(req)
	if err != nil {
		logging.Errorf("requestHandlerContext: Error in validateScheduleCreateRequest %v", err)
		return err
	}

	err = mc.PostScheduleCreateToken(req.Definition, req.Plan, bucketUUID, scopeId, collectionId,
		req.IndexerId, time.Now().UnixNano())
	if err != nil {
		logging.Errorf("requestHandlerContext: Error in PostScheduleCreateToken %v", err)
		return err
	}

	return nil
}

//
// Handle restore of a bucket.
//
func (m *requestHandlerContext) bucketRestoreHandler(bucket, include, exclude string, r *http.Request) (int, string) {

	filters, filterType, err := getFilters(r, bucket)
	if err != nil {
		logging.Errorf("RequestHandler::bucketRestoreHandler: err in getFilters %v", err)
		return http.StatusBadRequest, err.Error()
	}

	remap, err1 := getRestoreRemapParam(r)
	if err1 != nil {
		logging.Errorf("RequestHandler::bucketRestoreHandler: err in getRestoreRemapParam %v", err1)
		return http.StatusBadRequest, err1.Error()
	}

	logging.Debugf("bucketRestoreHandler: remap %v", remap)

	image := m.convertIndexMetadataRequest(r)
	if image == nil {
		return http.StatusBadRequest, "Unable to process request input"
	}

	context := createRestoreContext(image, m.clusterUrl, bucket, filters, filterType, remap)
	hostIndexMap, err2 := context.computeIndexLayout()
	if err2 != nil {
		logging.Errorf("RequestHandler::bucketRestoreHandler: err in computeIndexLayout %v", err2)
		return http.StatusInternalServerError, err2.Error()
	}

	if err := m.restoreIndexMetadataToNodes(hostIndexMap); err != nil {
		return http.StatusInternalServerError, fmt.Sprintf("%v", err)
	}

	return http.StatusOK, ""
}

//
// Handle backup of a bucket.
// Note that this function does not verify auths or RBAC
//
func (m *requestHandlerContext) bucketBackupHandler(bucket, include, exclude string,
	r *http.Request) (*ClusterIndexMetadata, error) {

	cinfo, err := m.mgr.FetchNewClusterInfoCache()
	if err != nil {
		return nil, err
	}

	// find all nodes that has a index http service
	nids := cinfo.GetNodesByServiceType(common.INDEX_HTTP_SERVICE)

	clusterMeta := &ClusterIndexMetadata{Metadata: make([]LocalIndexMetadata, len(nids))}

	respMap := make(map[common.NodeId]*http.Response)
	errMap := make(map[common.NodeId]error)

	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, nid := range nids {

		getLocalMeta := func(nid common.NodeId) {
			defer wg.Done()

			cinfo.RLock()
			defer cinfo.RUnlock()

			addr, err := cinfo.GetServiceAddress(nid, common.INDEX_HTTP_SERVICE, true)
			if err == nil {
				url := "/getLocalIndexMetadata?bucket=" + u.QueryEscape(bucket)
				if len(include) != 0 {
					url += "&include=" + u.QueryEscape(include)
				}

				if len(exclude) != 0 {
					url += "&exclude=" + u.QueryEscape(exclude)
				}

				resp, err := getWithAuth(addr + url)
				mu.Lock()
				defer mu.Unlock()

				if err != nil {
					logging.Debugf("RequestHandler::bucketBackupHandler: Error while retrieving %v with auth %v", addr+"/getLocalIndexMetadata", err)
					errMap[nid] = errors.New(fmt.Sprintf("Fail to retrieve index definition from url %s: err = %v", addr, err))
					respMap[nid] = nil
				} else {
					respMap[nid] = resp
				}
			} else {
				mu.Lock()
				defer mu.Unlock()

				errMap[nid] = errors.New(fmt.Sprintf("Fail to retrieve http endpoint for index node"))
			}
		}

		wg.Add(1)
		go getLocalMeta(nid)
	}

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	for _, resp := range respMap {
		if resp != nil && resp.Body != nil {
			defer resp.Body.Close()
		}
	}

	if len(errMap) != 0 {
		for _, err := range errMap {
			return nil, err
		}
	}

	cinfo.RLock()
	defer cinfo.RUnlock()

	i := 0
	for nid, resp := range respMap {

		localMeta := new(LocalIndexMetadata)
		status := convertResponse(resp, localMeta)
		if status == RESP_ERROR {
			addr, err := cinfo.GetServiceAddress(nid, common.INDEX_HTTP_SERVICE, true)
			if err != nil {
				return nil, errors.New(fmt.Sprintf("Fail to retrieve local metadata from node id %v.", nid))
			} else {
				return nil, errors.New(fmt.Sprintf("Fail to retrieve local metadata from url %v.", addr))
			}
		}

		newLocalMeta := LocalIndexMetadata{
			IndexerId:   localMeta.IndexerId,
			NodeUUID:    localMeta.NodeUUID,
			StorageMode: localMeta.StorageMode,
		}

		for _, topology := range localMeta.IndexTopologies {
			newLocalMeta.IndexTopologies = append(newLocalMeta.IndexTopologies, topology)
		}

		for _, defn := range localMeta.IndexDefinitions {
			newLocalMeta.IndexDefinitions = append(newLocalMeta.IndexDefinitions, defn)
		}

		clusterMeta.Metadata[i] = newLocalMeta
		i++
	}

	filters, filterType, err := getFilters(r, bucket)
	if err != nil {
		return nil, err
	}

	schedTokens, err1 := getSchedCreateTokens(bucket, filters, filterType)
	if err1 != nil {
		return nil, err1
	}

	clusterMeta.SchedTokens = schedTokens

	return clusterMeta, nil
}

func getSchedCreateTokens(bucket string, filters map[string]bool, filterType string) (
	map[common.IndexDefnId]*mc.ScheduleCreateToken, error) {

	schedTokensMap := make(map[common.IndexDefnId]*mc.ScheduleCreateToken)
	stopSchedTokensMap := make(map[common.IndexDefnId]bool)

	scheduleTokens, err := mc.ListAllScheduleCreateTokens()
	if err != nil {
		return nil, err
	}

	stopScheduleTokens, err1 := mc.ListAllStopScheduleCreateTokens()
	if err1 != nil {
		return nil, err1
	}

	for _, token := range stopScheduleTokens {
		stopSchedTokensMap[token.DefnId] = true
	}

	for _, token := range scheduleTokens {
		if _, ok := stopSchedTokensMap[token.Definition.DefnId]; !ok {
			if !applyFilters(bucket, token.Definition.Bucket, token.Definition.Scope,
				token.Definition.Collection, "", filters, filterType) {

				continue
			}

			schedTokensMap[token.Definition.DefnId] = token
		}
	}

	return schedTokensMap, nil
}

func (m *requestHandlerContext) authorizeBucketRequest(w http.ResponseWriter,
	r *http.Request, creds cbauth.Creds, bucket, include, exclude string) bool {
	const method string = "RequestHandler::authorizeBucketRequest" // for logging

	// Basic RBAC.
	// 1. If include filter is specified, verify user has permissions to access
	//    indexes created on all component scopes and collections.
	// 2. If include filter is not specified, verify user has permissions to
	//    access the indexes for the bucket.
	//
	// During backup, Local index metadata call can peform RBAC based filtering.
	// So, in case of unauthorized access to a specific scope / collection,
	// backup service will get an appropriate error.

	var op string
	switch r.Method {
	case "GET":
		op = "list"

	case "POST":
		op = "create"

	default:
		send(http.StatusBadRequest, w, fmt.Sprintf("Unsupported method %v", r.Method))
		return false
	}

	if len(include) == 0 {
		switch r.Method {
		case "GET":
			permission := fmt.Sprintf("cluster.bucket[%s].n1ql.index!%s", bucket, op)
			if !isAllowed(creds, []string{permission}, r, w, method) {
				return false
			}

		case "POST":
			permission := fmt.Sprintf("cluster.bucket[%s].n1ql.index!%s", bucket, op)
			if !isAllowed(creds, []string{permission}, r, w, method) {
				// TODO: If bucket level verification fails, then as a best effort,
				// iterate over restore metadata and verify for each scope/collection.
				// This will be needed only if backup was performed by a user using
				// include filter (or without any filter) and restore is being
				// performed by a another user (with less privileges) using an exclude
				// filter. This scenario seems unlikely.
				return false
			}

		default:
			send(http.StatusBadRequest, w, fmt.Sprintf("Unsupported method %v", r.Method))
			return false
		}
	} else {
		incls := strings.Split(include, ",")
		for _, incl := range incls {
			inc := strings.Split(incl, ".")
			if len(inc) == 1 {
				scope := fmt.Sprintf("%s:%s", bucket, inc[0])
				permission := fmt.Sprintf("cluster.scope[%s].n1ql.index!%s", scope, op)
				if !isAllowed(creds, []string{permission}, r, w, method) {
					return false
				}
			} else if len(inc) == 2 {
				collection := fmt.Sprintf("%s:%s:%s", bucket, inc[0], inc[1])
				permission := fmt.Sprintf("cluster.collection[%s].n1ql.index!%s", collection, op)
				if !isAllowed(creds, []string{permission}, r, w, method) {
					return false
				}
			} else {
				send(http.StatusBadRequest, w, fmt.Sprintf("Malformed url %v, include %v", r.URL.Path, include))
				return false
			}
		}
	}

	return true
}

func (m *requestHandlerContext) bucketReqHandler(w http.ResponseWriter, r *http.Request, creds cbauth.Creds) {
	url := strings.TrimSpace(r.URL.Path)
	if url[len(url)-1] == byte('/') {
		url = url[:len(url)-1]
	}
	segs := strings.Split(url, "/")
	logging.Debugf("bucketReqHandler: url %v, segs:%v", url, segs)

	if len(segs) != 6 {
		switch r.Method {

		case "GET":
			resp := &BackupResponse{Code: RESP_ERROR, Error: fmt.Sprintf("Malformed url %v", r.URL.Path)}
			send(http.StatusBadRequest, w, resp)

		case "POST":
			resp := &RestoreResponse{Code: RESP_ERROR, Error: fmt.Sprintf("Malformed url %v", r.URL.Path)}
			send(http.StatusBadRequest, w, resp)

		default:
			send(http.StatusBadRequest, w, fmt.Sprintf("Unsupported method %v", r.Method))
		}

		return
	}

	bucket := segs[4]
	function := segs[5]

	switch function {

	case "backup":
		// Note that for backup, bucketReqHandler does not validate input. Input
		// validation is performed in the local RPC implementation on each node.
		// The local RPC handler should not skip the input validation.

		include := r.FormValue("include")
		exclude := r.FormValue("exclude")

		logging.Debugf("bucketReqHandler:backup url %v, include %v, exclude %v", url, include, exclude)

		if !m.authorizeBucketRequest(w, r, creds, bucket, include, exclude) {
			return
		}

		switch r.Method {

		case "GET":
			// Backup
			clusterMeta, err := m.bucketBackupHandler(bucket, include, exclude, r)
			if err == nil {
				resp := &BackupResponse{Code: RESP_SUCCESS, Result: *clusterMeta}
				send(http.StatusOK, w, resp)
			} else {
				logging.Infof("RequestHandler::bucketBackupHandler: err %v", err)
				resp := &BackupResponse{Code: RESP_ERROR, Error: err.Error()}
				send(http.StatusInternalServerError, w, resp)
			}

		case "POST":
			status, errStr := m.bucketRestoreHandler(bucket, include, exclude, r)
			if status == http.StatusOK {
				send(http.StatusOK, w, &RestoreResponse{Code: RESP_SUCCESS})
			} else {
				send(http.StatusInternalServerError, w, &RestoreResponse{Code: RESP_ERROR, Error: errStr})
			}

		default:
			send(http.StatusBadRequest, w, fmt.Sprintf("Unsupported method %v", r.Method))
		}

	default:
		send(http.StatusBadRequest, w, fmt.Sprintf("Malformed URL %v", r.URL.Path))
	}
}

//
// Returns intenal version of local indexer node
//
func (m *requestHandlerContext) handleInternalVersionRequest(w http.ResponseWriter, r *http.Request) {

	const method string = "RequestHandler::handleInternalVersionRequest" // for logging

	_, ok := doAuth(r, w, method)
	if !ok {
		return
	}

	data, err := common.GetMarshalledInternalVersion()
	if err != nil {
		logging.Debugf("requestHandlerContext:handleInternalVersionRequest error %v", err)
		sendHttpError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	logging.Debugf("requestHandlerContext:handleInternalVersionRequest data %s", data)

	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

//
// Handler for /api/v1/bucket/<bucket-name>/<function-name>
//
func BucketRequestHandler(w http.ResponseWriter, r *http.Request, creds cbauth.Creds) {
	handlerContext.bucketReqHandler(w, r, creds)
}

//
// Schedule tokens
//
var SCHED_TOKEN_CHECK_INTERVAL = 5000 // Milliseconds

type schedTokenMonitor struct {
	indexes   []*IndexStatus
	listener  *mc.CommandListener
	lock      sync.Mutex
	lCloseCh  chan bool
	processed map[string]common.IndexerId

	cinfo     *common.ClusterInfoCache
	cinfoLock sync.Mutex
	mgr       *IndexManager
}

func newSchedTokenMonitor(mgr *IndexManager) *schedTokenMonitor {

	lCloseCh := make(chan bool)
	listener := mc.NewCommandListener(lCloseCh, false, false, false, false, true, true)

	s := &schedTokenMonitor{
		indexes:   make([]*IndexStatus, 0),
		listener:  listener,
		lCloseCh:  lCloseCh,
		processed: make(map[string]common.IndexerId),
		mgr:       mgr,
	}

	cinfo, err := common.FetchNewClusterInfoCache2(s.mgr.clusterURL, common.DEFAULT_POOL, "request_handler:schedTokenMonitor")
	if err != nil || cinfo == nil {
		logging.Errorf("newSchedTokenMonitor: Error fetching cluster info cache %v", err)
	}
	if cinfo != nil {
		err = cinfo.FetchNodesAndSvsInfo()
		if err != nil {
			logging.Errorf("newSchedTokenMonitor: Error fetching nodes and svs in cluster info cache %v", err)
			cinfo = nil
		}
	}
	s.cinfoLock.Lock()
	defer s.cinfoLock.Unlock()
	s.cinfo = cinfo

	s.listener.ListenTokens()

	return s
}

func (s *schedTokenMonitor) getNodeAddr(token *mc.ScheduleCreateToken) (string, error) {
	s.cinfoLock.Lock()
	defer s.cinfoLock.Unlock()

	var nid common.NodeId
	var found bool
	nodeUUID := fmt.Sprintf("%v", token.IndexerId)

	if s.cinfo != nil {
		nid, found = s.cinfo.GetNodeIdByUUID(nodeUUID)
		if found {
			return s.cinfo.GetServiceAddress(nid, "mgmt", true)
		}
	} else {
		cinfo, err := common.FetchNewClusterInfoCache2(s.mgr.clusterURL, common.DEFAULT_POOL, "request_handler:schedTokenMonitor")
		if err != nil || cinfo == nil {
			logging.Errorf("getNodeAddr: Error fetching cluster info cache %v", err)
			return "", fmt.Errorf("ClusterInfoCache unavailable")
		}
		s.cinfo = cinfo
	}

	err := s.cinfo.FetchNodesAndSvsInfo()
	if err != nil {
		s.cinfo = nil
		return "", fmt.Errorf("ClusterInfoCache unable to fetch nodes and svs info")
	}

	nid, found = s.cinfo.GetNodeIdByUUID(nodeUUID)
	if found {
		return s.cinfo.GetServiceAddress(nid, "mgmt", true)
	}

	return "", fmt.Errorf("node id for %v not found", nodeUUID)
}

func (s *schedTokenMonitor) makeIndexStatus(token *mc.ScheduleCreateToken) *IndexStatus {

	mgmtAddr, err := s.getNodeAddr(token)
	if err != nil {
		logging.Errorf("schedTokenMonitor::makeIndexStatus: error in getNodeAddr: %v", err)
		return nil
	}

	defn := &token.Definition
	numPartitons := defn.NumPartitions
	stmt := common.IndexStatement(*defn, int(numPartitons), -1, true)

	// TODO: Scheduled: Should we rename it to ScheduledBuild ?

	secExprs, _, _ := common.GetUnexplodedExprs(defn.SecExprs, defn.Desc)
	// Use DefnId for InstId as a placeholder value because InstId cannot zero.
	return &IndexStatus{
		DefnId:       defn.DefnId,
		InstId:       common.IndexInstId(defn.DefnId),
		Name:         defn.Name,
		Bucket:       defn.Bucket,
		Scope:        defn.Scope,
		Collection:   defn.Collection,
		IsPrimary:    defn.IsPrimary,
		SecExprs:     secExprs,
		WhereExpr:    defn.WhereExpr,
		IndexType:    common.GetStorageMode().String(),
		Status:       "Scheduled for Creation",
		Definition:   stmt,
		Completion:   0,
		Progress:     0,
		Scheduled:    false,
		Partitioned:  common.IsPartitioned(defn.PartitionScheme),
		NumPartition: int(numPartitons),
		PartitionMap: nil,
		NumReplica:   defn.GetNumReplica(),
		IndexName:    defn.Name,
		LastScanTime: "NA",
		Error:        "",
		Hosts:        []string{mgmtAddr},
	}
}

func (s *schedTokenMonitor) checkProcessed(key string, token *mc.ScheduleCreateToken) (bool, bool) {

	if indexerId, ok := s.processed[key]; ok {
		if token == nil {
			return true, false
		}

		if indexerId == token.IndexerId {
			return true, true
		}

		return true, false
	}

	return false, false
}

func (s *schedTokenMonitor) markProcessed(key string, indexerId common.IndexerId) {
	s.processed[key] = indexerId
}

func (s *schedTokenMonitor) getIndexesFromTokens(createTokens map[string]*mc.ScheduleCreateToken,
	stopTokens map[string]*mc.StopScheduleCreateToken) []*IndexStatus {

	indexes := make([]*IndexStatus, 0, len(createTokens))

	for key, token := range createTokens {
		logging.Debugf("schedTokenMonitor::getIndexesFromTokens new schedule create token %v", key)
		if marked, match := s.checkProcessed(key, token); marked && match {
			logging.Debugf("schedTokenMonitor::getIndexesFromTokens skip processing schedule create token %v as it is already processed.", key)
			continue
		} else if marked && !match {
			logging.Debugf("schedTokenMonitor::getIndexesFromTokens updating schedule create token %v", key)
			s.updateIndex(token)
			continue
		}

		stopKey := mc.GetStopScheduleCreateTokenPathFromDefnId(token.Definition.DefnId)
		if _, ok := stopTokens[stopKey]; ok {
			logging.Debugf("schedTokenMonitor::getIndexesFromTokens skip processing schedule create token %v as stop schedule create token is seen.", key)
			continue
		}

		// TODO: Check for the index in s.indexes, before checking for stop token.

		// Explicitly check for stop token.
		stopToken, err := mc.GetStopScheduleCreateToken(token.Definition.DefnId)
		if err != nil {
			logging.Errorf("schedTokenMonitor:getIndexesFromTokens error (%v) in getting stop schedule create token for %v",
				err, token.Definition.DefnId)
			continue
		}

		if stopToken != nil {
			logging.Debugf("schedTokenMonitor:getIndexesFromTokens stop schedule token exists for %v",
				token.Definition.DefnId)
			if marked, _ := s.checkProcessed(key, token); marked {
				logging.Debugf("schedTokenMonitor::getIndexesFromTokens marking index as failed for %v", key)
				marked := s.markIndexFailed(stopToken)
				if marked {
					continue
				} else {
					// This is unexpected as checkProcessed for this key true.
					// Which means the index should have been found in the s.indexrs.
					logging.Warnf("schedTokenMonitor:getIndexesFromTokens failed to mark index as failed for %v",
						token.Definition.DefnId)
				}
			}

			continue
		}

		idx := s.makeIndexStatus(token)
		if idx == nil {
			continue
		}

		indexes = append(indexes, idx)
		s.markProcessed(key, token.IndexerId)
	}

	for key, token := range stopTokens {
		// If create token was already processed, then just mark the
		// index as failed.
		logging.Debugf("schedTokenMonitor::getIndexesFromTokens new stop schedule create token %v", key)
		marked := s.markIndexFailed(token)
		if marked {
			s.markProcessed(key, common.IndexerId(""))
			continue
		}

		scheduleKey := mc.GetScheduleCreateTokenPathFromDefnId(token.DefnId)
		ct, ok := createTokens[scheduleKey]
		if !ok {
			logging.Debugf("schedTokenMonitor::getIndexesFromTokens skip processing stop schedule create token %v as schedule create token does not exist", key)
			continue
		}

		if marked, _ := s.checkProcessed(key, nil); marked {
			logging.Debugf("schedTokenMonitor::getIndexesFromTokens skip processing stop schedule create token %v as it is already processed", key)
			continue
		}

		idx := s.makeIndexStatus(ct)
		if idx == nil {
			continue
		}

		idx.Status = "Error"
		idx.Error = token.Reason

		indexes = append(indexes, idx)
		s.markProcessed(key, common.IndexerId(""))
	}

	return indexes
}

func (s *schedTokenMonitor) markIndexFailed(token *mc.StopScheduleCreateToken) bool {
	// Note that this is an idempotent operation - as long as the value
	// of the token doesn't change.
	for _, index := range s.indexes {
		if index.DefnId == token.DefnId {
			index.Status = "Error"
			index.Error = token.Reason
			return true
		}
	}

	return false
}

func (s *schedTokenMonitor) updateIndex(token *mc.ScheduleCreateToken) {
	for _, index := range s.indexes {
		if index.DefnId == token.Definition.DefnId {
			mgmtAddr, err := s.getNodeAddr(token)
			if err != nil {
				logging.Errorf("schedTokenMonitor::updateIndex: error in getNodeAddr: %v",
					err)
				return
			}
			index.Hosts = []string{mgmtAddr}
			return
		}
	}

	logging.Warnf("schedTokenMonitor:getIndexesFromTokens failed to update index for %v",
		token.Definition.DefnId)

	return
}

func (s *schedTokenMonitor) clenseIndexes(indexes []*IndexStatus,
	stopTokens map[string]*mc.StopScheduleCreateToken, delPaths map[string]bool) []*IndexStatus {

	newIndexes := make([]*IndexStatus, 0, len(indexes))
	for _, idx := range indexes {
		path := mc.GetScheduleCreateTokenPathFromDefnId(idx.DefnId)

		if _, ok := delPaths[path]; ok {
			logging.Debugf("schedTokenMonitor::clenseIndexes skip processing index %v as the key is deleted.", path)
			continue
		}

		path = mc.GetStopScheduleCreateTokenPathFromDefnId(idx.DefnId)
		if _, ok := stopTokens[path]; ok {
			if idx.Status != "Error" {
				continue
			} else {
				newIndexes = append(newIndexes, idx)
			}
		} else {
			newIndexes = append(newIndexes, idx)
		}
	}

	return newIndexes
}

func (s *schedTokenMonitor) getIndexes() []*IndexStatus {
	s.lock.Lock()
	defer s.lock.Unlock()

	logging.Debugf("schedTokenMonitor getIndexes ...")

	createTokens := s.listener.GetNewScheduleCreateTokens()
	stopTokens := s.listener.GetNewStopScheduleCreateTokens()
	delPaths := s.listener.GetDeletedScheduleCreateTokenPaths()

	indexes := s.getIndexesFromTokens(createTokens, stopTokens)

	indexes = append(indexes, s.indexes...)
	s.indexes = indexes
	s.indexes = s.clenseIndexes(s.indexes, stopTokens, delPaths)

	return s.indexes
}

func (s *schedTokenMonitor) Close() {
	s.listener.Close()
}
