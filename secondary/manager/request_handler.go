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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/indexing/secondary/common"
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
	Timestamp        int64              `json:"timestamp,omitempty"`
	LocalSettings    map[string]string  `json:"localSettings,omitempty"`
	IndexTopologies  []IndexTopology    `json:"topologies,omitempty"`
	IndexDefinitions []common.IndexDefn `json:"definitions,omitempty"`
}

type ClusterIndexMetadata struct {
	Metadata []LocalIndexMetadata `json:"metadata,omitempty"`
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
	DefnId       common.IndexDefnId `json:"defnId,omitempty"`
	InstId       common.IndexInstId `json:"instId,omitempty"`
	Name         string             `json:"name,omitempty"`
	Bucket       string             `json:"bucket,omitempty"`
	IsPrimary    bool               `json:"isPrimary,omitempty"`
	SecExprs     []string           `json:"secExprs,omitempty"`
	WhereExpr    string             `json:"where,omitempty"`
	IndexType    string             `json:"indexType,omitempty"`
	Status       string             `json:"status,omitempty"`
	Definition   string             `json:"definition"`
	Hosts        []string           `json:"hosts,omitempty"`
	Error        string             `json:"error,omitempty"`
	Completion   int                `json:"completion"`
	Progress     float64            `json:"progress"`
	Scheduled    bool               `json:"scheduled"`
	Partitioned  bool               `json:"partitioned"`
	NumPartition int                `json:"numPartition"`

	// PartitionMap is a map from node host:port to partitionIds,
	// telling which partition(s) are on which node(s). If an
	// index is not partitioned, it will have a single
	// partition with ID 0.
	PartitionMap map[string][]int `json:"partitionMap"`

	NodeUUID     string `json:"nodeUUID,omitempty"`
	NumReplica   int    `json:"numReplica"`
	IndexName    string `json:"indexName"`
	ReplicaId    int    `json:"replicaId"`
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

	///////////////////////////////////////////////////////////////////////////
	// IndexStatus caches of info from all indexer nodes. New info is written
	// into the memory cache first and eventually persisted to disk. Lookups
	// check the memory cache first. Only the latest known full set of status
	// data (i.e. for all indexes on a node) is cached for each node - partial
	// sets are never cached.

	// Index metadata cache
	metaCache map[string]*LocalIndexMetadata      // IndexMetadata mem cache (key = host2key(host:httpPort))
	metaCh    chan map[string]*LocalIndexMetadata // metaCache persistence feed
	metaDir   string                              // metaCache persistence directory
	metaMutex sync.RWMutex                        // metaCache mutex

	// IndexStats subset cache
	statsCache map[string]*common.Statistics      // IndexStats subset mem cache (key = host2key(host:httpPort))
	statsCh    chan map[string]*common.Statistics // statsCache persistence feed
	statsDir   string                             // statsCache persistence directory
	statsMutex sync.RWMutex                       // statsCache mutex
	///////////////////////////////////////////////////////////////////////////

	doneCh chan bool
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
		handlerContext.hostKey = host2key(handlerContext.hostname)

		// Scatter-gather endpoints. These are the entry points to a single indexer that will
		// scatter the request to all indexers and gather the results to return to the caller.
		mux.HandleFunc("/getIndexMetadata", handlerContext.handleIndexMetadataRequest)
		mux.HandleFunc("/getIndexStatus", handlerContext.handleIndexStatusRequest)

		// Single-indexer endpoints (non-scatter-gather).
		mux.HandleFunc("/createIndex", handlerContext.createIndexRequest)
		mux.HandleFunc("/createIndexRebalance", handlerContext.createIndexRequestRebalance)
		mux.HandleFunc("/dropIndex", handlerContext.dropIndexRequest)
		mux.HandleFunc("/buildIndex", handlerContext.buildIndexRequest)
		mux.HandleFunc("/getLocalIndexMetadata", handlerContext.handleLocalIndexMetadataRequest)
		mux.HandleFunc("/restoreIndexMetadata", handlerContext.handleRestoreIndexMetadataRequest)
		mux.HandleFunc("/getIndexStatement", handlerContext.handleIndexStatementRequest)
		mux.HandleFunc("/planIndex", handlerContext.handleIndexPlanRequest)
		mux.HandleFunc("/settings/storageMode", handlerContext.handleIndexStorageModeRequest)
		mux.HandleFunc("/settings/planner", handlerContext.handlePlannerRequest)
		mux.HandleFunc("/listReplicaCount", handlerContext.handleListLocalReplicaCountRequest)
		mux.HandleFunc("/getCachedLocalIndexMetadata", handlerContext.handleCachedLocalIndexMetadataRequest)
		mux.HandleFunc("/getCachedStats", handlerContext.handleCachedStats)

		cacheDir := path.Join(config["storage_dir"].String(), "cache")
		handlerContext.metaDir = path.Join(cacheDir, "meta")
		handlerContext.statsDir = path.Join(cacheDir, "stats")

		os.MkdirAll(handlerContext.metaDir, 0755)
		os.MkdirAll(handlerContext.statsDir, 0755)

		handlerContext.metaCh = make(chan map[string]*LocalIndexMetadata, 100)
		handlerContext.statsCh = make(chan map[string]*common.Statistics, 100)
		handlerContext.doneCh = make(chan bool)

		handlerContext.metaCache = make(map[string]*LocalIndexMetadata)
		handlerContext.statsCache = make(map[string]*common.Statistics)

		go handlerContext.runPersistor()
	})
}

// Close permanently shuts down the HTTP(S) server created by RegisterRequestHandler.
func (m *requestHandlerContext) Close() {
	m.finalizer.Do(func() {
		close(m.doneCh)
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

func (m *requestHandlerContext) createIndexRequest(w http.ResponseWriter, r *http.Request) {

	m.doCreateIndex(w, r, false)

}

func (m *requestHandlerContext) createIndexRequestRebalance(w http.ResponseWriter, r *http.Request) {

	m.doCreateIndex(w, r, true)

}

func (m *requestHandlerContext) doCreateIndex(w http.ResponseWriter, r *http.Request, isRebalReq bool) {

	creds, ok := doAuth(r, w)
	if !ok {
		return
	}

	// convert request
	request := m.convertIndexRequest(r)
	if request == nil {
		sendIndexResponseWithError(http.StatusBadRequest, w, "Unable to convert request for create index")
		return
	}

	permission := fmt.Sprintf("cluster.bucket[%s].n1ql.index!create", request.Index.Bucket)
	if !isAllowed(creds, []string{permission}, w) {
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
	logging.Debugf("RequestHandler::createIndexRequest: invoke IndexManager for create index bucket %s name %s",
		indexDefn.Bucket, indexDefn.Name)

	if err := m.mgr.HandleCreateIndexDDL(&indexDefn, isRebalReq); err == nil {
		// No error, return success
		sendIndexResponse(w)
	} else {
		// report failure
		sendIndexResponseWithError(http.StatusInternalServerError, w, fmt.Sprintf("%v", err))
	}

}

func (m *requestHandlerContext) dropIndexRequest(w http.ResponseWriter, r *http.Request) {

	creds, ok := doAuth(r, w)
	if !ok {
		return
	}

	// convert request
	request := m.convertIndexRequest(r)
	if request == nil {
		sendIndexResponseWithError(http.StatusBadRequest, w, "Unable to convert request for drop index")
		return
	}

	permission := fmt.Sprintf("cluster.bucket[%s].n1ql.index!drop", request.Index.Bucket)
	if !isAllowed(creds, []string{permission}, w) {
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

func (m *requestHandlerContext) buildIndexRequest(w http.ResponseWriter, r *http.Request) {

	creds, ok := doAuth(r, w)
	if !ok {
		return
	}

	// convert request
	request := m.convertIndexRequest(r)
	if request == nil {
		sendIndexResponseWithError(http.StatusBadRequest, w, "Unable to convert request for build index")
		return
	}

	permission := fmt.Sprintf("cluster.bucket[%s].n1ql.index!build", request.Index.Bucket)
	if !isAllowed(creds, []string{permission}, w) {
		return
	}

	// call the index manager to handle the DDL
	indexIds := request.IndexIds
	if err := m.mgr.HandleBuildIndexDDL(indexIds); err == nil {
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

	return req
}

//////////////////////////////////////////////////////
// Index Status
///////////////////////////////////////////////////////

func (m *requestHandlerContext) handleIndexStatusRequest(w http.ResponseWriter, r *http.Request) {

	creds, ok := doAuth(r, w)
	if !ok {
		return
	}

	bucket := m.getBucket(r)

	getAll := false
	val := r.FormValue("getAll")
	if len(val) != 0 && val == "true" {
		getAll = true
	}

	list, failedNodes, err := m.getIndexStatus(creds, bucket, getAll)
	if err == nil && len(failedNodes) == 0 {
		sort.Sort(indexStatusSorter(list))
		resp := &IndexStatusResponse{Code: RESP_SUCCESS, Status: list}
		send(http.StatusOK, w, resp)
	} else {
		logging.Debugf("RequestHandler::handleIndexStatusRequest: failed nodes %v", failedNodes)
		sort.Sort(indexStatusSorter(list))
		resp := &IndexStatusResponse{Code: RESP_ERROR, Error: "Fail to retrieve cluster-wide metadata from index service",
			Status: list, FailedNodes: failedNodes}
		send(http.StatusInternalServerError, w, resp)
	}
}

func (m *requestHandlerContext) getBucket(r *http.Request) string {

	return r.FormValue("bucket")
}

// memCacheLocalIndexMetadata adds an entry to the local metadata memory cache.
// hostKey is host2key(host:httpPort).
func (m *requestHandlerContext) memCacheLocalIndexMetadata(hostKey string, value *LocalIndexMetadata) {
	m.metaMutex.Lock()
	m.metaCache[hostKey] = value
	m.metaMutex.Unlock()
}

// memCacheStats adds an entry to the local IndexStats subset memory cache.
// hostKey is host2key(host:httpPort).
func (m *requestHandlerContext) memCacheStats(hostKey string, value *common.Statistics) {
	m.statsMutex.Lock()
	m.statsCache[hostKey] = value
	m.statsMutex.Unlock()
}

// mergeCounter is a helper for getIndexStatus.
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

// getIndexStatus returns statuses of all indexer nodes. An unresponsive node's status is served
// from the cache of whichever responsive node has the newest data cached. Only if no responsive
// node has a cached value will an unresponsive node's status be omitted. (The caches were only
// added in common.INDEXER_65_VERSION.) Status consists of two parts:
//   1. LocalIndexMetadata
//   2. A subset of IndexStats (currently: buildProgress, completionProgress, lastScanTime)
func (m *requestHandlerContext) getIndexStatus(creds cbauth.Creds, bucket string, getAll bool) ([]IndexStatus, []string, error) {

	var cinfo *common.ClusterInfoCache
	cinfo = m.mgr.cinfoClient.GetClusterInfoCache()

	if cinfo == nil {
		return nil, nil, errors.New("ClusterInfoCache unavailable in IndexManager")
	}

	cinfo.RLock()
	defer cinfo.RUnlock()

	// find all nodes that has a index http service
	nids := cinfo.GetNodesByServiceType(common.INDEX_HTTP_SERVICE)

	numReplicas := make(map[common.IndexDefnId]common.Counter)
	defns := make(map[common.IndexDefnId]common.IndexDefn)
	list := make([]IndexStatus, 0)   // return 1: flat list of statuses
	failedNodes := make([]string, 0) // return 2: flat list of unreachable indexer nodes

	// IndexStatus pieces by node to cache to local disk, corresponding to metaCache and statsCache
	metaToCache := make(map[string]*LocalIndexMetadata)
	statsToCache := make(map[string]*common.Statistics)

	defnToHostMap := make(map[common.IndexDefnId][]string)
	isInstanceDeferred := make(map[common.IndexInstId]bool)

	keepKeys := make([]string, 0, len(nids)) // memory cache keys of current indexer nodes
	for _, nid := range nids {

		// mgmtAddr is this node's "cluster" address (host:uiPort), NOT a key for caches
		mgmtAddr, err := cinfo.GetServiceAddress(nid, "mgmt")
		if err != nil {
			logging.Errorf("RequestHandler::getIndexStatus: Error from GetServiceAddress (mgmt) for node id %v. Error = %v", nid, err)
			continue
		}

		addr, err := cinfo.GetServiceAddress(nid, common.INDEX_HTTP_SERVICE)
		if err != nil {
			logging.Debugf("RequestHandler::getIndexStatus: Error from GetServiceAddress (indexHttp) for node id %v. Error = %v", nid, err)
			failedNodes = append(failedNodes, mgmtAddr)
			continue
		}

		u, err := security.GetURL(addr)
		if err != nil {
			logging.Debugf("RequestHandler::getIndexStatus: Fail to parse URL %v", addr)
			failedNodes = append(failedNodes, mgmtAddr)
			continue
		}

		hostname := u.Host
		hostKey := host2key(hostname) // key to caches
		keepKeys = append(keepKeys, hostKey)

		stale := false
		metaToCache[hostKey] = nil

		//
		// Get metadata for all indexes of current node
		//
		localMeta, latest, err := m.getLocalIndexMetadataForNode(addr, hostname, cinfo)
		if localMeta == nil || err != nil {
			logging.Debugf("RequestHandler::getIndexStatus: Error while retrieving %v with auth %v", addr+"/getLocalIndexMetadata", err)
			failedNodes = append(failedNodes, mgmtAddr)
			continue
		}
		if !latest {
			stale = true
		}
		metaToCache[hostKey] = localMeta

		//
		// Get stats subset for all indexes of current node
		//
		statsToCache[hostKey] = nil
		var stats *common.Statistics
		stats, latest, err = m.getStatsForNode(addr, hostname, cinfo)
		if stats == nil || err != nil {
			logging.Debugf("RequestHandler::getIndexStatus: Error while retrieving %v with auth %v", addr+"/stats?async=true", err)
			failedNodes = append(failedNodes, mgmtAddr)
			continue
		}

		if !latest {
			stale = true
		}
		statsToCache[hostKey] = stats

		//
		// Process all the data for current host
		//
		for _, defn := range localMeta.IndexDefinitions {

			if len(bucket) != 0 && bucket != defn.Bucket {
				// Do not cache partial results
				metaToCache[hostKey] = nil
				statsToCache[hostKey] = nil
				continue
			}

			permission := fmt.Sprintf("cluster.bucket[%s].n1ql.index!list", defn.Bucket)
			if !isAllowed(creds, []string{permission}, nil) {
				// Do not cache partial results
				metaToCache[hostKey] = nil
				statsToCache[hostKey] = nil
				continue
			}

			mergeCounter(defn.DefnId, defn.NumReplica2, numReplicas)
			if topology := findTopologyByBucket(localMeta.IndexTopologies, defn.Bucket); topology != nil {
				instances := topology.GetIndexInstancesByDefn(defn.DefnId)
				for _, instance := range instances {
					state, errStr := topology.GetStatusByInst(defn.DefnId, common.IndexInstId(instance.InstId))

					if state != common.INDEX_STATE_CREATED &&
						state != common.INDEX_STATE_DELETED &&
						state != common.INDEX_STATE_NIL {

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
							stateStr = "Replicating"
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
						if len(errStr) != 0 {
							stateStr = "Error"
						}

						name := common.FormatIndexInstDisplayName(defn.Name, int(instance.ReplicaId))

						completion := int(0)
						key := fmt.Sprintf("%v:%v:build_progress", defn.Bucket, name)
						if progress, ok := stats.ToMap()[key]; ok {
							completion = int(progress.(float64))
						}

						progress := float64(0)
						key = fmt.Sprintf("%v:completion_progress", instance.InstId)
						if stat, ok := stats.ToMap()[key]; ok {
							progress = math.Float64frombits(uint64(stat.(float64)))
						}

						lastScanTime := "NA"
						key = fmt.Sprintf("%v:%v:last_known_scan_time", defn.Bucket, name)
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

						status := IndexStatus{
							DefnId:       defn.DefnId,
							InstId:       common.IndexInstId(instance.InstId),
							Name:         name,
							Bucket:       defn.Bucket,
							IsPrimary:    defn.IsPrimary,
							SecExprs:     defn.SecExprs,
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
							IndexName:    defn.Name,
							ReplicaId:    int(instance.ReplicaId),
							Stale:        stale,
							LastScanTime: lastScanTime,
						}

						list = append(list, status)
					}
				}
			}
			defns[defn.DefnId] = defn
		}

		// Memory cache the data if it is a full set
		if metaToCache[hostKey] != nil && statsToCache[hostKey] != nil {
			m.memCacheLocalIndexMetadata(hostKey, metaToCache[hostKey])
			m.memCacheStats(hostKey, statsToCache[hostKey])
		}
	} // for each nid

	// Delete obsolete entries from LocalIndexMetadata and IndexStats subset memory caches
	m.cleanupMemoryCaches(keepKeys)

	//Fix replica count
	for i, index := range list {
		if counter, ok := numReplicas[index.DefnId]; ok {
			numReplica, exist := counter.Value()
			if exist {
				list[i].NumReplica = int(numReplica)
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
	for i, index := range list {
		defnId := index.DefnId
		defn := defns[defnId]
		if len(defnToHostMap[defnId]) > 1 || defn.Nodes != nil {
			defn.Nodes = defnToHostMap[defnId]
			// The deferred field will be set to true by default for a rebalanced index
			// For the non-rebalanced index, it can either be true or false depending on
			// how it was created
			defn.Deferred = isInstanceDeferred[index.InstId]
			list[i].Definition = common.IndexStatement(defn, int(defn.NumPartitions), index.NumReplica, true)
		}
	}

	if !getAll {
		list = m.consolideIndexStatus(list)
	}

	// Stage local metadata and stats subset for persisting to disk cache
	m.metaCh <- metaToCache
	m.statsCh <- statsToCache

	return list, failedNodes, nil
}

func (m *requestHandlerContext) consolideIndexStatus(statuses []IndexStatus) []IndexStatus {

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

	if str1 == "Replicating" || str2 == "Replicating" {
		return "Replicating"
	}

	// must be ready
	return str1
}

//////////////////////////////////////////////////////
// Index Statement
///////////////////////////////////////////////////////

func (m *requestHandlerContext) handleIndexStatementRequest(w http.ResponseWriter, r *http.Request) {

	creds, ok := doAuth(r, w)
	if !ok {
		return
	}

	bucket := m.getBucket(r)

	list, err := m.getIndexStatement(creds, bucket)
	if err == nil {
		sort.Strings(list)
		send(http.StatusOK, w, list)
	} else {
		send(http.StatusInternalServerError, w, err.Error())
	}
}

func (m *requestHandlerContext) getIndexStatement(creds cbauth.Creds, bucket string) ([]string, error) {

	indexes, failedNodes, err := m.getIndexStatus(creds, bucket, false)
	if err != nil {
		return nil, err
	}
	if len(failedNodes) != 0 {
		return nil, errors.New(fmt.Sprintf("Failed to connect to indexer nodes %v", failedNodes))
	}

	defnMap := make(map[common.IndexDefnId]bool)
	statements := ([]string)(nil)
	for _, index := range indexes {
		if _, ok := defnMap[index.DefnId]; !ok {
			defnMap[index.DefnId] = true
			statements = append(statements, index.Definition)
		}
	}

	return statements, nil
}

///////////////////////////////////////////////////////
// ClusterIndexMetadata
///////////////////////////////////////////////////////

func (m *requestHandlerContext) handleIndexMetadataRequest(w http.ResponseWriter, r *http.Request) {

	creds, ok := doAuth(r, w)
	if !ok {
		return
	}

	bucket := m.getBucket(r)

	meta, err := m.getIndexMetadata(creds, bucket)
	if err == nil {
		resp := &BackupResponse{Code: RESP_SUCCESS, Result: *meta}
		send(http.StatusOK, w, resp)
	} else {
		logging.Debugf("RequestHandler::handleIndexMetadataRequest: err %v", err)
		resp := &BackupResponse{Code: RESP_ERROR, Error: err.Error()}
		send(http.StatusInternalServerError, w, resp)
	}
}

func (m *requestHandlerContext) getIndexMetadata(creds cbauth.Creds, bucket string) (*ClusterIndexMetadata, error) {

	cinfo, err := m.mgr.FetchNewClusterInfoCache()
	if err != nil {
		return nil, err
	}

	// find all nodes that has a index http service
	nids := cinfo.GetNodesByServiceType(common.INDEX_HTTP_SERVICE)

	clusterMeta := &ClusterIndexMetadata{Metadata: make([]LocalIndexMetadata, len(nids))}

	for i, nid := range nids {

		addr, err := cinfo.GetServiceAddress(nid, common.INDEX_HTTP_SERVICE)
		if err == nil {

			url := "/getLocalIndexMetadata"
			if len(bucket) != 0 {
				url += "?bucket=" + bucket
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
				permission := fmt.Sprintf("cluster.bucket[%s].n1ql.index!list", topology.Bucket)
				if isAllowed(creds, []string{permission}, nil) {
					newLocalMeta.IndexTopologies = append(newLocalMeta.IndexTopologies, topology)
				}
			}

			for _, defn := range localMeta.IndexDefinitions {
				permission := fmt.Sprintf("cluster.bucket[%s].n1ql.index!list", defn.Bucket)
				if isAllowed(creds, []string{permission}, nil) {
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

///////////////////////////////////////////////////////
// LocalIndexMetadata
///////////////////////////////////////////////////////

// handleLocalIndexMetadataRequest handles incoming requests for the /getLocalIndexMetadata
// REST endpoint.
func (m *requestHandlerContext) handleLocalIndexMetadataRequest(w http.ResponseWriter, r *http.Request) {

	creds, ok := doAuth(r, w)
	if !ok {
		return
	}

	bucket := m.getBucket(r)

	meta, err := m.getLocalIndexMetadata(creds, bucket)
	if err == nil {
		send(http.StatusOK, w, meta)
	} else {
		logging.Debugf("RequestHandler::handleLocalIndexMetadataRequest: err %v", err)
		sendHttpError(w, " Unable to retrieve index metadata", http.StatusInternalServerError)
	}
}

func (m *requestHandlerContext) getLocalIndexMetadata(creds cbauth.Creds, bucket string) (meta *LocalIndexMetadata, err error) {

	repo := m.mgr.getMetadataRepo()

	meta = &LocalIndexMetadata{IndexTopologies: nil, IndexDefinitions: nil}
	indexerId, err := repo.GetLocalIndexerId()
	if err != nil {
		return nil, err
	}
	meta.IndexerId = string(indexerId)

	nodeUUID, err := repo.GetLocalNodeUUID()
	if err != nil {
		return nil, err
	}
	meta.NodeUUID = string(nodeUUID)

	meta.StorageMode = string(common.StorageModeToIndexType(common.GetStorageMode()))
	meta.LocalSettings = make(map[string]string)

	meta.Timestamp = time.Now().UnixNano()

	if exclude, err := m.mgr.GetLocalValue("excludeNode"); err == nil {
		meta.LocalSettings["excludeNode"] = exclude
	}

	iter, err := repo.NewIterator()
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var defn *common.IndexDefn
	_, defn, err = iter.Next()
	for err == nil {
		if len(bucket) == 0 || bucket == defn.Bucket {
			permission := fmt.Sprintf("cluster.bucket[%s].n1ql.index!list", defn.Bucket)
			if isAllowed(creds, []string{permission}, nil) {
				meta.IndexDefinitions = append(meta.IndexDefinitions, *defn)
			}
		}
		_, defn, err = iter.Next()
	}

	iter1, err := repo.NewTopologyIterator()
	if err != nil {
		return nil, err
	}
	defer iter1.Close()

	var topology *IndexTopology
	topology, err = iter1.Next()
	for err == nil {
		if len(bucket) == 0 || bucket == topology.Bucket {
			permission := fmt.Sprintf("cluster.bucket[%s].n1ql.index!list", topology.Bucket)
			if isAllowed(creds, []string{permission}, nil) {
				meta.IndexTopologies = append(meta.IndexTopologies, *topology)
			}
		}
		topology, err = iter1.Next()
	}

	return meta, nil
}

///////////////////////////////////////////////////////
// Cached LocalIndexMetadata and Stats
///////////////////////////////////////////////////////

func (m *requestHandlerContext) handleCachedLocalIndexMetadataRequest(w http.ResponseWriter, r *http.Request) {

	creds, ok := doAuth(r, w)
	if !ok {
		return
	}

	host := r.FormValue("host")
	host = strings.Trim(host, "\"")

	meta, err := m.getLocalIndexMetadataFromCache(host2key(host))
	if meta != nil && err == nil {
		newMeta := *meta
		newMeta.IndexDefinitions = make([]common.IndexDefn, 0, len(meta.IndexDefinitions))
		newMeta.IndexTopologies = make([]IndexTopology, 0, len(meta.IndexTopologies))

		for _, defn := range meta.IndexDefinitions {
			permission := fmt.Sprintf("cluster.bucket[%s].n1ql.index!list", defn.Bucket)
			if isAllowed(creds, []string{permission}, nil) {
				newMeta.IndexDefinitions = append(newMeta.IndexDefinitions, defn)
			}
		}

		for _, topology := range meta.IndexTopologies {
			permission := fmt.Sprintf("cluster.bucket[%s].n1ql.index!list", topology.Bucket)
			if isAllowed(creds, []string{permission}, nil) {
				newMeta.IndexTopologies = append(newMeta.IndexTopologies, topology)
			}
		}

		send(http.StatusOK, w, newMeta)

	} else {
		logging.Debugf("RequestHandler::handleCachedLocalIndexMetadataRequest: err %v", err)
		sendHttpError(w, " Unable to retrieve index metadata", http.StatusInternalServerError)
	}
}

func (m *requestHandlerContext) handleCachedStats(w http.ResponseWriter, r *http.Request) {

	_, ok := doAuth(r, w)
	if !ok {
		return
	}

	host := r.FormValue("host")
	host = strings.Trim(host, "\"")

	stats, err := m.getStatsFromCache(host2key(host))
	if stats != nil && err == nil {
		send(http.StatusOK, w, stats)
	} else {
		logging.Debugf("RequestHandler::handleCachedStats: err %v", err)
		sendHttpError(w, " Unable to retrieve index metadata", http.StatusInternalServerError)
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
func (m *requestHandlerContext) handleRestoreIndexMetadataRequest(w http.ResponseWriter, r *http.Request) {

	creds, ok := doAuth(r, w)
	if !ok {
		return
	}

	// convert backup image into runtime data structure
	image := m.convertIndexMetadataRequest(r)
	if image == nil {
		send(http.StatusBadRequest, w, &RestoreResponse{Code: RESP_ERROR, Error: "Unable to process request input"})
		return
	}

	for _, localMeta := range image.Metadata {
		for _, topology := range localMeta.IndexTopologies {
			permission := fmt.Sprintf("cluster.bucket[%s].n1ql.index!create", topology.Bucket)
			if !isAllowed(creds, []string{permission}, w) {
				return
			}
		}

		for _, defn := range localMeta.IndexDefinitions {
			permission := fmt.Sprintf("cluster.bucket[%s].n1ql.index!create", defn.Bucket)
			if !isAllowed(creds, []string{permission}, w) {
				return
			}
		}
	}

	// Restore
	bucket := m.getBucket(r)
	logging.Infof("restore to target bucket %v", bucket)

	context := createRestoreContext(image, m.clusterUrl, bucket)
	hostIndexMap, err := context.computeIndexLayout()
	if err != nil {
		send(http.StatusInternalServerError, w, &RestoreResponse{Code: RESP_ERROR, Error: fmt.Sprintf("Unable to restore metadata.  Error=%v", err)})
	}

	var mu sync.Mutex
	var wg sync.WaitGroup

	errMap := make(map[string]bool)

	restoreIndexes := func(host string, indexes []*common.IndexDefn) {
		defer wg.Done()

		for _, index := range indexes {
			if !m.makeCreateIndexRequest(*index, host) {
				mu.Lock()
				defer mu.Unlock()

				errMap[host] = true
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
	if len(errMap) != 0 {
		send(http.StatusInternalServerError, w, &RestoreResponse{Code: RESP_ERROR, Error: "Unable to restore metadata."})
		return
	}

	send(http.StatusOK, w, &RestoreResponse{Code: RESP_SUCCESS})
}

func (m *requestHandlerContext) makeCreateIndexRequest(defn common.IndexDefn, host string) bool {

	// deferred build for restore
	defn.Deferred = true

	req := IndexRequest{Version: uint64(1), Type: CREATE, Index: defn}
	body, err := json.Marshal(&req)
	if err != nil {
		logging.Errorf("requestHandler.makeCreateIndexRequest(): cannot marshall create index request %v", err)
		return false
	}

	bodybuf := bytes.NewBuffer(body)

	resp, err := postWithAuth(host+"/createIndex", "application/json", bodybuf)
	if err != nil {
		logging.Errorf("requestHandler.makeCreateIndexRequest(): create index request fails for %v/createIndex. Error=%v", host, err)
		return false
	}
	defer resp.Body.Close()

	response := new(IndexResponse)
	status := convertResponse(resp, response)
	if status == RESP_ERROR || response.Code == RESP_ERROR {
		logging.Errorf("requestHandler.makeCreateIndexRequest(): create index request fails. Error=%v", response.Error)
		return false
	}

	return true
}

//////////////////////////////////////////////////////
// Planner
///////////////////////////////////////////////////////

func (m *requestHandlerContext) handleIndexPlanRequest(w http.ResponseWriter, r *http.Request) {

	_, ok := doAuth(r, w)
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

	plan, err := planner.RetrievePlanFromCluster(m.clusterUrl, nil)
	if err != nil {
		return "", errors.New(fmt.Sprintf("Fail to retreive index information from cluster.   Error=%v", err))
	}

	specs, err := m.convertIndexPlanRequest(r)
	if err != nil {
		return "", errors.New(fmt.Sprintf("Fail to read index spec from request.   Error=%v", err))
	}

	solution, err := planner.ExecutePlanWithOptions(plan, specs, true, "", "", 0, -1, -1, false, true)
	if err != nil {
		return "", errors.New(fmt.Sprintf("Fail to plan index.   Error=%v", err))
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

	creds, ok := doAuth(r, w)
	if !ok {
		return
	}

	if !isAllowed(creds, []string{"cluster.settings!write"}, w) {
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
						logging.Infof("RequestHandler::handleIndexStorageModeRequest: unable to identify nodeUUID.  Cannot downgrade.")
						send(http.StatusOK, w, "Unable to identify nodeUUID.  Cannot downgrade.")
						return
					}

					mc.PostIndexerStorageModeOverride(string(nodeUUID), common.ForestDB)
					logging.Infof("RequestHandler::handleIndexStorageModeRequest: set override storage mode to forestdb")
					send(http.StatusOK, w, "downgrade storage mode to forestdb after indexer restart.")
				} else {
					logging.Infof("RequestHandler::handleIndexStorageModeRequest: local storage mode is not plasma.  Cannot downgrade.")
					send(http.StatusOK, w, "Indexer storage mode is not plasma.  Cannot downgrade.")
				}
			} else {
				nodeUUID, err := m.mgr.getMetadataRepo().GetLocalNodeUUID()
				if err != nil {
					logging.Infof("RequestHandler::handleIndexStorageModeRequest: unable to identify nodeUUID. Cannot disable storage mode downgrade.")
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

	creds, ok := doAuth(r, w)
	if !ok {
		return
	}

	if !isAllowed(creds, []string{"cluster.settings!write"}, w) {
		return
	}

	// Override the storage mode for the local indexer.  Override will not take into effect until
	// indexer has restarted manually by administrator.   During indexer bootstrap, it will upgrade/downgrade
	// individual index to the override storage mode.
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

	creds, ok := doAuth(r, w)
	if !ok {
		return
	}

	result, err := m.getLocalReplicaCount(creds)
	if err == nil {
		send(http.StatusOK, w, result)
	} else {
		logging.Debugf("RequestHandler::handleListReplicaCountRequest: err %v", err)
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
	permissions := make(map[string]bool)

	_, defn, err = iter.Next()
	for err == nil {
		if _, ok := permissions[defn.Bucket]; !ok {
			permission := fmt.Sprintf("cluster.bucket[%s].n1ql.index!list", defn.Bucket)
			if !isAllowed(creds, []string{permission}, nil) {
				return nil, fmt.Errorf("Permission denied on reading metadata for bucket %v", defn.Bucket)
			}
			permissions[defn.Bucket] = true
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

	header := w.Header()
	header["Content-Type"] = []string{"application/json"}

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

func doAuth(r *http.Request, w http.ResponseWriter) (cbauth.Creds, bool) {

	creds, valid, err := common.IsAuthValid(r)
	if err != nil {
		sendIndexResponseWithError(http.StatusInternalServerError, w, err.Error())
		return nil, false
	} else if valid == false {
		w.WriteHeader(401)
		w.Write([]byte("401 Unauthorized\n"))
		return nil, false
	}

	return creds, true
}

func isAllowed(creds cbauth.Creds, permissions []string, w http.ResponseWriter) bool {

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
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(http.StatusText(http.StatusUnauthorized)))
		}
		return false
	}

	return true
}

// getWithAuth does an HTTP(S) GET request with Basic Authentication.
func getWithAuth(url string) (*http.Response, error) {
	params := &security.RequestParams{Timeout: time.Duration(10) * time.Second}
	return security.GetWithAuth(url, params)
}

func postWithAuth(url string, bodyType string, body io.Reader) (*http.Response, error) {
	params := &security.RequestParams{Timeout: time.Duration(120) * time.Second}
	return security.PostWithAuth(url, bodyType, body, params)
}

func findTopologyByBucket(topologies []IndexTopology, bucket string) *IndexTopology {

	for _, topology := range topologies {
		if topology.Bucket == bucket {
			return &topology
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

func (s indexStatusSorter) Less(i, j int) bool {
	if s[i].Name < s[j].Name {
		return true
	}

	if s[i].Name > s[j].Name {
		return false
	}

	return s[i].Bucket < s[j].Bucket
}

///////////////////////////////////////////////////////
// retrieve / persist cached local index metadata
///////////////////////////////////////////////////////

// getLocalIndexMetadataForNode will retrieve the latest LocalIndexMetadata for a given
// indexer node if they are available anywhere. If the source host is responding they will
// be from that host, else they will be from whatever indexer host has the most recent
// cached version.
func (m *requestHandlerContext) getLocalIndexMetadataForNode(addr string, host string, cinfo *common.ClusterInfoCache) (localMeta *LocalIndexMetadata, latest bool, err error) {

	meta, err := m.getLocalIndexMetadataFromREST(addr, host)
	if err == nil {
		return meta, true, nil
	}

	if cinfo.GetClusterVersion() >= common.INDEXER_65_VERSION {
		var latest *LocalIndexMetadata
		nids := cinfo.GetNodesByServiceType(common.INDEX_HTTP_SERVICE)
		for _, nid := range nids {
			addr, err1 := cinfo.GetServiceAddress(nid, common.INDEX_HTTP_SERVICE)
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
			return latest, false, nil
		}
	}

	return nil, false, err
}

// getLocalIndexMetadataFromREST gets the LocalIndexMetadata values from a (usually remote)
// indexer node.
func (m *requestHandlerContext) getLocalIndexMetadataFromREST(addr string, hostname string) (*LocalIndexMetadata, error) {

	resp, err := getWithAuth(addr + "/getLocalIndexMetadata")
	defer func() {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
	}()

	if err == nil {
		// Process newly retrieved payload
		localMeta := new(LocalIndexMetadata)
		if status := convertResponse(resp, localMeta); status == RESP_SUCCESS {
			return localMeta, nil
		}
		err = fmt.Errorf("Fail to unmarshal response from %v", hostname)
	}

	return nil, err
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

// getLocalIndexMetadataFromCache looks up the cached LocalIndexMetadata for the given
// hostname from the cache (memory first, and if not found then disk). If missing
// from memory but found on disk, it also copies the disk version to the memory cache.
// hostKey is host2key(host:httpPort).
func (m *requestHandlerContext) getLocalIndexMetadataFromCache(hostKey string) (*LocalIndexMetadata, error) {

	// Look in memory cache
	m.metaMutex.RLock()
	localMeta, ok := m.metaCache[hostKey]
	m.metaMutex.RUnlock()
	if ok && localMeta != nil {
		if logging.IsEnabled(logging.Debug) {
			logging.Debugf("getLocalIndexMetadataFromCache: found metadata in memory cache %v", hostKey)
		}
		return localMeta, nil
	}

	// Not found in mem cache; look in disk cache
	filepath := path.Join(m.metaDir, hostKey)
	content, err := ioutil.ReadFile(filepath)
	if err != nil {
		logging.Errorf("getLocalIndexMetadataFromCache: fail to read metadata from file %v.  Error %v", filepath, err)
		return nil, err
	}

	localMeta = new(LocalIndexMetadata)
	if err := json.Unmarshal(content, localMeta); err != nil {
		logging.Errorf("getLocalIndexMetadataFromCache: fail to unmarshal metadata from file %v.  Error %v", filepath, err)
		return nil, err
	}

	// Found on disk but not in mem, so add to mem cache
	m.memCacheLocalIndexMetadata(hostKey, localMeta)
	if logging.IsEnabled(logging.Debug) {
		logging.Debugf("getLocalIndexMetadataFromCache: saved metadata to memory cache %v", hostKey)

	}

	return localMeta, nil
}

func (m *requestHandlerContext) saveLocalIndexMetadataToDisk(hostKey string, meta *LocalIndexMetadata) error {

	filepath := path.Join(m.metaDir, hostKey)
	temp := filepath + ".tmp"

	content, err := json.Marshal(meta)
	if err != nil {
		logging.Errorf("saveLocalIndexMetadataToDisk: fail to marshal metadata to file %v.  Error %v", filepath, err)
		return err
	}

	err = ioutil.WriteFile(temp, content, 0755)
	if err != nil {
		logging.Errorf("saveLocalIndexMetadataToDisk: fail to save metadata to file %v.  Error %v", temp, err)
		return err
	}

	err = os.Rename(temp, filepath)
	if err != nil {
		logging.Errorf("saveLocalIndexMetadataToDisk: fail to rename metadata to file %v.  Error %v", filepath, err)
		return err
	}

	logging.Debugf("saveLocalIndexMetadataToDisk: successfully written metadata to disk for %v", hostKey)

	return nil
}

// cleanupMemoryCaches deletes obsolete entries from all getIndexStatus memory caches.
// keepKeys correspond to the current indexer nodes; all other entries are deleted.
func (m *requestHandlerContext) cleanupMemoryCaches(keepKeys []string) {

	// metaCache
	m.metaMutex.Lock()
	for cacheKey := range m.metaCache {
		keep := false
		for _, keepKey := range keepKeys {
			if cacheKey == keepKey {
				keep = true
				break
			}
		}
		if !keep {
			delete(m.metaCache, cacheKey)
		}
	}
	m.metaMutex.Unlock()

	// statsCache
	m.statsMutex.Lock()
	for cacheKey := range m.statsCache {
		keep := false
		for _, keepKey := range keepKeys {
			if cacheKey == keepKey {
				keep = true
				break
			}
		}
		if !keep {
			delete(m.statsCache, cacheKey)
		}
	}
	m.statsMutex.Unlock()
}

// cleanupLocalIndexMetadataOnDisk takes a list of hostnames of indexer nodes that currently
// exist, reads all filenames in the disk directory of the LocalIndexMetadata cache,
// and deletes any that do not correspond to an entry in the hostKeys list.
func (m *requestHandlerContext) cleanupLocalIndexMetadataOnDisk(hostKeys []string) {

	// Disk files that exist
	files, err := ioutil.ReadDir(m.metaDir)
	if err != nil {
		logging.Errorf("cleanupLocalIndexMetadataOnDisk: failed to read directory %v.  Error %v", m.metaDir, err)
		return
	}

	for _, file := range files {
		filename := file.Name()

		found := false
		for _, hostKey := range hostKeys {
			if hostKey == filename {
				found = true
				break
			}
		}

		if !found {
			filepath := path.Join(m.metaDir, filename)
			if err := os.RemoveAll(filepath); err != nil {
				logging.Errorf("cleanupLocalIndexMetadataOnDisk: failed to remove file %v.  Error %v", filepath, err)
			} else if logging.IsEnabled(logging.Debug) {
				logging.Debugf("cleanupLocalIndexMetadataOnDisk: successfully removed file %v.", filepath)
			}
		}
	}
}

///////////////////////////////////////////////////////
// retrieve / persist cached index stats
///////////////////////////////////////////////////////

// getStatsForNode will retrieve the latest IndexStats subset for a given indexer node if
// they are available anywhere. If the source host is responding they will be from that
// host, else they will be from whatever indexer host has the most recent cached version.
func (m *requestHandlerContext) getStatsForNode(addr string, host string, cinfo *common.ClusterInfoCache) (stats *common.Statistics, latest bool, err error) {

	stats, err = m.getStatsFromREST(addr, host)
	if err == nil {
		return stats, true, nil
	}

	if cinfo.GetClusterVersion() >= common.INDEXER_65_VERSION {
		var latest *common.Statistics
		nids := cinfo.GetNodesByServiceType(common.INDEX_HTTP_SERVICE)
		for _, nid := range nids {
			addr, err1 := cinfo.GetServiceAddress(nid, common.INDEX_HTTP_SERVICE)
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

	resp, err := getWithAuth(addr + "/stats?async=true")
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

// getStatsFromCache looks up the cached subset of IndexStats for the given
// hostname from the cache (memory first, and if not found then disk). If missing
// from memory but found on disk, it also copies the disk version to the memory cache.
// hostKey is host2Key(host:httpPort).
func (m *requestHandlerContext) getStatsFromCache(hostKey string) (*common.Statistics, error) {

	// Look in memory cache
	m.statsMutex.RLock()
	stats, ok := m.statsCache[hostKey]
	m.statsMutex.RUnlock()
	if ok && stats != nil {
		if logging.IsEnabled(logging.Debug) {
			logging.Debugf("getStatsFromCache: found stats in memory cache %v", hostKey)
		}
		return stats, nil
	}

	// Not found in mem cache; look in disk cache
	filepath := path.Join(m.statsDir, hostKey)
	content, err := ioutil.ReadFile(filepath)
	if err != nil {
		logging.Errorf("getStatsFromCache: fail to read stats from file %v.  Error %v", filepath, err)
		return nil, err
	}

	stats = new(common.Statistics)
	if err := json.Unmarshal(content, stats); err != nil {
		logging.Errorf("getStatsFromCache: fail to unmarshal stats from file %v.  Error %v", filepath, err)
		return nil, err
	}

	// Found on disk but not in mem, so add to mem cache
	m.memCacheStats(hostKey, stats)
	if logging.IsEnabled(logging.Debug) {
		logging.Debugf("getStatsFromCache: saved stats to memory cache %v", hostKey)
	}

	return stats, nil
}

func (m *requestHandlerContext) saveStatsToDisk(hostKey string, stats *common.Statistics) error {

	filepath := path.Join(m.statsDir, hostKey)
	temp := filepath + ".tmp"

	content, err := json.Marshal(stats)
	if err != nil {
		logging.Errorf("saveStatsToDisk: fail to marshal stats to file %v.  Error %v", filepath, err)
		return err
	}

	err = ioutil.WriteFile(temp, content, 0755)
	if err != nil {
		logging.Errorf("saveStatsToDisk: fail to save stats to file %v.  Error %v", temp, err)
		return err
	}

	err = os.Rename(temp, filepath)
	if err != nil {
		logging.Errorf("saveStatsToDisk: fail to rename stats to file %v.  Error %v", filepath, err)
		return err
	}

	logging.Debugf("saveStatsToDisk: successfully written stats to disk for %v", hostKey)

	return nil
}

// cleanupStatsOnDisk takes a list of hostnames of indexer nodes that currently
// exist, reads all filenames in the disk directory of the IndexStats subset cache,
// and deletes any that do not correspond to an entry in the hostKeys list.
func (m *requestHandlerContext) cleanupStatsOnDisk(hostKeys []string) {

	// Disk files that exist
	files, err := ioutil.ReadDir(m.statsDir)
	if err != nil {
		logging.Errorf("cleanupStatsOnDisk: failed to read directory %v.  Error %v", m.statsDir, err)
		return
	}

	for _, file := range files {
		filename := file.Name()

		found := false
		for _, hostKey := range hostKeys {
			if hostKey == filename {
				found = true
				break
			}
		}

		if !found {
			filepath := path.Join(m.statsDir, filename)
			if err := os.RemoveAll(filepath); err != nil {
				logging.Errorf("cleanupStatsOnDisk: failed to remove file %v.  Error %v", filepath, err)
			} else if logging.IsEnabled(logging.Debug) {
				logging.Debugf("cleanupStatsOnDisk: successfully removed file %v.", filepath)
			}
		}
	}
}

///////////////////////////////////////////////////////
// persistor
///////////////////////////////////////////////////////

// runPersistor runs in a Go routine and persists IndexStatus data
// from all indexer nodes to local disk cache.
func (m *requestHandlerContext) runPersistor() {
	updateMeta := func(metaToCache map[string]*LocalIndexMetadata) {
		hostKeys := make([]string, 0, len(metaToCache))
		for hostKey, meta := range metaToCache {
			if meta != nil {
				m.saveLocalIndexMetadataToDisk(hostKey, meta)
			}
			hostKeys = append(hostKeys, hostKey)
		}
		m.cleanupLocalIndexMetadataOnDisk(hostKeys)
	}
	updateStats := func(statsToCache map[string]*common.Statistics) {
		hostKeys := make([]string, 0, len(statsToCache))
		for hostKey, stats := range statsToCache {
			if stats != nil {
				m.saveStatsToDisk(hostKey, stats)
			}
			hostKeys = append(hostKeys, hostKey)
		}
		m.cleanupStatsOnDisk(hostKeys)
	}

	for {
		select {
		case metaToCache, ok := <-m.metaCh:
			if !ok {
				return
			}
			for len(m.metaCh) > 0 {
				metaToCache = <-m.metaCh
			}
			updateMeta(metaToCache)

		case statsToCache, ok := <-m.statsCh:
			if !ok {
				return
			}
			for len(m.statsCh) > 0 {
				statsToCache = <-m.statsCh
			}
			updateStats(statsToCache)

		case <-m.doneCh:
			logging.Infof("request_handler persistor exits")
			return
		}
	}
}

// host2key converts a host:httpPort string to a key for the metaCache and statsCache.
// This is also used as the filename for the disk halves of these caches.
func host2key(hostname string) string {

	hostname = strings.Replace(hostname, ".", "_", -1)
	hostname = strings.Replace(hostname, ":", "_", -1)

	return hostname
}
