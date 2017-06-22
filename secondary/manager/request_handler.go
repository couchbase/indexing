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
	"github.com/couchbase/cbauth"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/manager/client"
	"github.com/couchbase/indexing/secondary/planner"
	"io"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
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
}

//
// Index Backup / Restore
//

type LocalIndexMetadata struct {
	IndexerId        string             `json:"indexerId,omitempty"`
	NodeUUID         string             `json:"nodeUUID,omitempty"`
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
	DefnId     common.IndexDefnId `json:"defnId,omitempty"`
	Name       string             `json:"name,omitempty"`
	Bucket     string             `json:"bucket,omitempty"`
	IsPrimary  bool               `json:"isPrimary,omitempty"`
	SecExprs   []string           `json:"secExprs,omitempty"`
	WhereExpr  string             `json:"where,omitempty"`
	IndexType  string             `json:"indexType,omitempty"`
	Status     string             `json:"status,omitempty"`
	Definition string             `json:"definition"`
	Hosts      []string           `json:"hosts,omitempty"`
	Error      string             `json:"error,omitempty"`
	Completion int                `json:"completion"`
	Progress   float64            `json:"progress"`
	Scheduled  bool               `json:"scheduled"`
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
// Internal data structure
//

type requestHandlerContext struct {
	initializer sync.Once
	mgr         *IndexManager
	clusterUrl  string
}

var handlerContext requestHandlerContext

///////////////////////////////////////////////////////
// Registration
///////////////////////////////////////////////////////

func registerRequestHandler(mgr *IndexManager, clusterUrl string) {

	handlerContext.initializer.Do(func() {
		defer func() {
			if r := recover(); r != nil {
				logging.Warnf("error encountered when registering http createIndex handler : %v.  Ignored.\n", r)
			}
		}()

		http.HandleFunc("/createIndex", handlerContext.createIndexRequest)
		http.HandleFunc("/createIndexRebalance", handlerContext.createIndexRequestRebalance)
		http.HandleFunc("/dropIndex", handlerContext.dropIndexRequest)
		http.HandleFunc("/buildIndex", handlerContext.buildIndexRequest)
		http.HandleFunc("/getLocalIndexMetadata", handlerContext.handleLocalIndexMetadataRequest)
		http.HandleFunc("/getIndexMetadata", handlerContext.handleIndexMetadataRequest)
		http.HandleFunc("/restoreIndexMetadata", handlerContext.handleRestoreIndexMetadataRequest)
		http.HandleFunc("/getIndexStatus", handlerContext.handleIndexStatusRequest)
		http.HandleFunc("/getIndexStatement", handlerContext.handleIndexStatementRequest)
		http.HandleFunc("/planIndex", handlerContext.handleIndexPlanRequest)
	})

	handlerContext.mgr = mgr
	handlerContext.clusterUrl = clusterUrl
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

	if err := m.mgr.HandleDeleteIndexDDL(indexDefn.DefnId); err == nil {
		// No error, return success
		sendIndexResponse(w)
	} else {
		// report failure
		sendIndexResponseWithError(http.StatusInternalServerError, w, fmt.Sprintf("%v", err))
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
		logging.Debugf("RequestHandler::convertIndexRequest: unable to unmarshall request body. Buf = %s, err %v", buf, err)
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

	list, failedNodes, err := m.getIndexStatus(creds, bucket)
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

func (m *requestHandlerContext) getIndexStatus(creds cbauth.Creds, bucket string) ([]IndexStatus, []string, error) {

	cinfo, err := m.mgr.FetchNewClusterInfoCache()
	if err != nil {
		return nil, nil, err
	}

	// find all nodes that has a index http service
	nids := cinfo.GetNodesByServiceType(common.INDEX_HTTP_SERVICE)

	list := make([]IndexStatus, 0)
	failedNodes := make([]string, 0)

	for _, nid := range nids {

		addr, err := cinfo.GetServiceAddress(nid, common.INDEX_HTTP_SERVICE)
		if err == nil {

			resp, err := getWithAuth(addr + "/getLocalIndexMetadata")
			if err != nil {
				logging.Debugf("RequestHandler::getIndexStatus: Error while retrieving %v with auth %v", addr+"/getLocalIndexMetadata", err)
				failedNodes = append(failedNodes, addr)
				continue
			}
			defer resp.Body.Close()

			localMeta := new(LocalIndexMetadata)
			status := convertResponse(resp, localMeta)
			if status == RESP_ERROR {
				logging.Debugf("RequestHandler::getIndexStatus: Error from convertResponse for localMeta: %v", err)
				failedNodes = append(failedNodes, addr)
				continue
			}

			curl, err := cinfo.GetServiceAddress(nid, "mgmt")
			if err != nil {
				logging.Debugf("RequestHandler::getIndexStatus: Error from  GetServiceAddress (mgmt) for node id %v. Error = %v", nid, err)
				failedNodes = append(failedNodes, addr)
				continue
			}

			resp, err = getWithAuth(addr + "/stats?async=true")
			if err != nil {
				logging.Debugf("RequestHandler::getIndexStatus: Error while retrieving %v with auth %v", addr+"/stats?async=true", err)
				failedNodes = append(failedNodes, addr)
				continue
			}
			defer resp.Body.Close()

			stats := new(common.Statistics)
			status = convertResponse(resp, stats)
			if status == RESP_ERROR {
				logging.Debugf("RequestHandler::getIndexStatus: Error from convertResponse for stats: %v", err)
				failedNodes = append(failedNodes, addr)
				continue
			}

			for _, defn := range localMeta.IndexDefinitions {

				if len(bucket) != 0 && bucket != defn.Bucket {
					continue
				}

				permission := fmt.Sprintf("cluster.bucket[%s].n1ql.index!list", defn.Bucket)
				if !isAllowed(creds, []string{permission}, nil) {
					continue
				}

				if topology := findTopologyByBucket(localMeta.IndexTopologies, defn.Bucket); topology != nil {

					instances := topology.GetIndexInstancesByDefn(defn.DefnId)
					for _, instance := range instances {

						state, errStr := topology.GetStatusByDefn(defn.DefnId)

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
							key = fmt.Sprintf("%v:%v:completion_progress", defn.Bucket, name)
							if stat, ok := stats.ToMap()[key]; ok {
								progress = math.Float64frombits(uint64(stat.(float64)))
							}

							status := IndexStatus{
								DefnId:     defn.DefnId,
								Name:       name,
								Bucket:     defn.Bucket,
								IsPrimary:  defn.IsPrimary,
								SecExprs:   defn.SecExprs,
								WhereExpr:  defn.WhereExpr,
								IndexType:  string(defn.Using),
								Status:     stateStr,
								Error:      errStr,
								Hosts:      []string{curl},
								Definition: common.IndexStatement(defn, false),
								Completion: completion,
								Progress:   progress,
								Scheduled:  instance.Scheduled,
							}

							list = append(list, status)
						}
					}
				}
			}
		} else {
			logging.Debugf("RequestHandler::getIndexStatus: Error from GetServiceAddress (indexHttp) for node id %v. Error = %v", nid, err)
			failedNodes = append(failedNodes, addr)
			continue
		}
	}

	return list, failedNodes, nil
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

	indexes, failedNodes, err := m.getIndexStatus(creds, bucket)
	if err != nil {
		return nil, err
	}
	if len(failedNodes) != 0 {
		return nil, errors.New(fmt.Sprintf("Failed to connect to indexer nodes %v", failedNodes))
	}

	defnMap := make(map[common.IndexDefnId]bool)
	statements := make([]string, len(indexes))
	for i, index := range indexes {
		if _, ok := defnMap[index.DefnId]; !ok {
			defnMap[index.DefnId] = true
			statements[i] = index.Definition
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
				IndexerId: localMeta.IndexerId,
				NodeUUID:  localMeta.NodeUUID,
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
	context := createRestoreContext(image, m.clusterUrl)
	hostIndexMap, err := context.computeIndexLayout()
	if err != nil {
		send(http.StatusInternalServerError, w, &RestoreResponse{Code: RESP_ERROR, Error: fmt.Sprintf("Unable to restore metadata.  Error=%v", err)})
	}

	for host, indexes := range hostIndexMap {
		for _, index := range indexes {
			if !m.makeCreateIndexRequest(*index, host) {
				send(http.StatusInternalServerError, w, &RestoreResponse{Code: RESP_ERROR, Error: "Unable to restore metadata."})
			}
		}
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

	plan, err := planner.RetrievePlanFromCluster(m.clusterUrl)
	if err != nil {
		return "", errors.New(fmt.Sprintf("Fail to retreive index information from cluster.   Error=%v", err))
	}

	specs, err := m.convertIndexPlanRequest(r)
	if err != nil {
		return "", errors.New(fmt.Sprintf("Fail to read index spec from request.   Error=%v", err))
	}

	solution, err := planner.ExecutePlanWithOptions(plan, specs, true, "", "", 0, -1, -1, false)
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

func send(status int, w http.ResponseWriter, res interface{}) {

	header := w.Header()
	header["Content-Type"] = []string{"application/json"}

	if buf, err := json.Marshal(res); err == nil {
		w.WriteHeader(status)
		logging.Tracef("RequestHandler::sendResponse: sending response back to caller. %v", string(buf))
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

func getWithAuth(url string) (*http.Response, error) {

	if !strings.HasPrefix(url, "http://") {
		url = "http://" + url
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	cbauth.SetRequestAuthVia(req, nil)

	client := http.Client{Timeout: time.Duration(10 * time.Second)}
	return client.Do(req)
}

func postWithAuth(url string, bodyType string, body io.Reader) (*http.Response, error) {

	if !strings.HasPrefix(url, "http://") {
		url = "http://" + url
	}

	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", bodyType)
	cbauth.SetRequestAuthVia(req, nil)

	client := http.Client{Timeout: time.Duration(10 * time.Second)}
	return client.Do(req)
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
