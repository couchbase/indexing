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
	"io"
	"math"
	"net/http"
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
)

type IndexRequest struct {
	Version uint64           `json:"version,omitempty"`
	Type    RequestType      `json:"type,omitempty"`
	Index   common.IndexDefn `json:"index,omitempty"`
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

type RestoreContext struct {
	idxToRestore map[common.IndexerId][]common.IndexDefn
	idxToResolve map[common.IndexerId][]common.IndexDefn
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
	Status     string             `json:"status,omitempty"`
	Definition string             `json:"definition"`
	Hosts      []string           `json:"hosts,omitempty"`
	Error      string             `json:"error,omitempty"`
	Completion int                `json:"completion,omitempty"`
}

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
		http.HandleFunc("/dropIndex", handlerContext.dropIndexRequest)
		http.HandleFunc("/getLocalIndexMetadata", handlerContext.handleLocalIndexMetadataRequest)
		http.HandleFunc("/getIndexMetadata", handlerContext.handleIndexMetadataRequest)
		http.HandleFunc("/restoreIndexMetadata", handlerContext.handleRestoreIndexMetadataRequest)
		http.HandleFunc("/getIndexStatus", handlerContext.handleIndexStatusRequest)
	})

	handlerContext.mgr = mgr
	handlerContext.clusterUrl = clusterUrl
}

///////////////////////////////////////////////////////
// Create / Drop Index
///////////////////////////////////////////////////////

func (m *requestHandlerContext) createIndexRequest(w http.ResponseWriter, r *http.Request) {

	if !doAuth(r, w, m.clusterUrl) {
		return
	}

	// convert request
	request := m.convertIndexRequest(r)
	if request == nil {
		sendIndexResponseWithError(w, "Unable to convert request for create index")
		return
	}

	indexDefn := request.Index

	if indexDefn.DefnId == 0 {
		defnId, err := common.NewIndexDefnId()
		if err != nil {
			sendIndexResponseWithError(w, fmt.Sprintf("Fail to generate index definition id %v", err))
			return
		}
		indexDefn.DefnId = defnId
	}

	// call the index manager to handle the DDL
	logging.Debugf("RequestHandler::createIndexRequest: invoke IndexManager for create index bucket %s name %s",
		indexDefn.Bucket, indexDefn.Name)

	if err := m.mgr.HandleCreateIndexDDL(&indexDefn); err == nil {
		// No error, return success
		sendIndexResponse(w)
	} else {
		// report failure
		sendIndexResponseWithError(w, fmt.Sprintf("%v", err))
	}
}

func (m *requestHandlerContext) dropIndexRequest(w http.ResponseWriter, r *http.Request) {

	if !doAuth(r, w, m.clusterUrl) {
		return
	}

	// convert request
	request := m.convertIndexRequest(r)
	if request == nil {
		sendIndexResponseWithError(w, "Unable to convert request for drop index")
		return
	}

	// call the index manager to handle the DDL
	indexDefn := request.Index
	if err := m.mgr.HandleDeleteIndexDDL(indexDefn.DefnId); err == nil {
		// No error, return success
		sendIndexResponse(w)
	} else {
		// report failure
		sendIndexResponseWithError(w, fmt.Sprintf("%v", err))
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

	if !doAuth(r, w, m.clusterUrl) {
		return
	}

	list, failedNodes, err := m.getIndexStatus(m.mgr.getServiceAddrProvider().(*common.ClusterInfoCache))
	if err == nil && len(failedNodes) == 0 {
		resp := &IndexStatusResponse{Code: RESP_SUCCESS, Status: list}
		send(w, resp)
	} else {
		logging.Debugf("RequestHandler::handleIndexStatusRequest: failed nodes %v", failedNodes)
		resp := &IndexStatusResponse{Code: RESP_ERROR, Error: "Fail to retrieve cluster-wide metadata from index service",
			Status: list, FailedNodes: failedNodes}
		send(w, resp)
	}
}

func (m *requestHandlerContext) getIndexStatus(cinfo *common.ClusterInfoCache) ([]IndexStatus, []string, error) {

	if err := cinfo.Fetch(); err != nil {
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
				failedNodes = append(failedNodes, addr)
				continue
			}

			localMeta := new(LocalIndexMetadata)
			status := convertResponse(resp, localMeta)
			if status == RESP_ERROR {
				failedNodes = append(failedNodes, addr)
				continue
			}

			curl, err := cinfo.GetServiceAddress(nid, "mgmt")
			if err != nil {
				failedNodes = append(failedNodes, addr)
				continue
			}

			resp, err = getWithAuth(addr + "/stats?async=true")
			if err != nil {
				failedNodes = append(failedNodes, addr)
				continue
			}

			stats := new(common.Statistics)
			status = convertResponse(resp, stats)
			if status == RESP_ERROR {
				failedNodes = append(failedNodes, addr)
				continue
			}

			for _, defn := range localMeta.IndexDefinitions {

				if topology := m.findTopologyByBucket(localMeta.IndexTopologies, defn.Bucket); topology != nil {
					state, errStr := topology.GetStatusByDefn(defn.DefnId)

					if state != common.INDEX_STATE_DELETED && state != common.INDEX_STATE_NIL {

						stateStr := "Not Available"
						switch state {
						case common.INDEX_STATE_CREATED:
							stateStr = "Created"
						case common.INDEX_STATE_READY:
							stateStr = "Created"
						case common.INDEX_STATE_INITIAL:
							stateStr = "Building"
						case common.INDEX_STATE_ACTIVE:
							stateStr = "Ready"
						}

						if len(errStr) != 0 {
							stateStr = "Error"
						}

						completion := int(0)
						key := fmt.Sprintf("%v:%v:build_progress", defn.Bucket, defn.Name)
						if progress, ok := stats.ToMap()[key]; ok {
							completion = int(progress.(float64))
						}

						status := IndexStatus{
							DefnId:     defn.DefnId,
							Name:       defn.Name,
							Bucket:     defn.Bucket,
							IsPrimary:  defn.IsPrimary,
							SecExprs:   defn.SecExprs,
							WhereExpr:  defn.WhereExpr,
							Status:     stateStr,
							Error:      errStr,
							Hosts:      []string{curl},
							Definition: common.IndexStatement(defn),
							Completion: completion,
						}

						list = append(list, status)
					}
				}
			}
		} else {
			failedNodes = append(failedNodes, addr)
			continue
		}
	}

	return list, failedNodes, nil
}

///////////////////////////////////////////////////////
// ClusterIndexMetadata
///////////////////////////////////////////////////////

func (m *requestHandlerContext) handleIndexMetadataRequest(w http.ResponseWriter, r *http.Request) {

	if !doAuth(r, w, m.clusterUrl) {
		return
	}

	indexerHostMap := make(map[common.IndexerId]string)
	meta, err := m.getIndexMetadata(m.mgr.getServiceAddrProvider().(*common.ClusterInfoCache), indexerHostMap)
	if err == nil {
		resp := &BackupResponse{Code: RESP_SUCCESS, Result: *meta}
		send(w, resp)
	} else {
		logging.Debugf("RequestHandler::handleIndexMetadataRequest: err %v", err)
		resp := &BackupResponse{Code: RESP_ERROR, Error: err.Error()}
		send(w, resp)
	}
}

func (m *requestHandlerContext) getIndexMetadata(cinfo *common.ClusterInfoCache,
	indexerHostMap map[common.IndexerId]string) (*ClusterIndexMetadata, error) {

	if err := cinfo.Fetch(); err != nil {
		return nil, err
	}

	// find all nodes that has a index http service
	nids := cinfo.GetNodesByServiceType(common.INDEX_HTTP_SERVICE)

	clusterMeta := &ClusterIndexMetadata{Metadata: make([]LocalIndexMetadata, len(nids))}

	for i, nid := range nids {

		addr, err := cinfo.GetServiceAddress(nid, common.INDEX_HTTP_SERVICE)
		if err == nil {

			resp, err := getWithAuth(addr + "/getLocalIndexMetadata")
			if err != nil {
				return nil, errors.New(fmt.Sprintf("Fail to retrieve index definition from url %s", addr))
			}

			localMeta := new(LocalIndexMetadata)
			status := convertResponse(resp, localMeta)
			if status == RESP_ERROR {
				return nil, errors.New(fmt.Sprintf("Fail to retrieve local metadata from url %s.", addr))
			}

			indexerHostMap[common.IndexerId(localMeta.IndexerId)] = addr
			clusterMeta.Metadata[i] = *localMeta

		} else {
			return nil, errors.New(fmt.Sprintf("Fail to retrieve http endpoint for index node"))
		}
	}

	return clusterMeta, nil
}

func (m *requestHandlerContext) convertIndexMetadataRequest(r *http.Request) *ClusterIndexMetadata {

	meta := &ClusterIndexMetadata{}

	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(r.Body); err != nil {
		logging.Debugf("RequestHandler::convertIndexRequest: unable to read request body, err %v", err)
		return nil
	}

	logging.Debugf("requestHandler.convertIndexMetadataRequest(): input %v", string(buf.Bytes()))

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

	if !doAuth(r, w, m.clusterUrl) {
		return
	}

	meta, err := m.getLocalIndexMetadata()
	if err == nil {
		send(w, meta)
	} else {
		logging.Debugf("RequestHandler::handleLocalIndexMetadataRequest: err %v", err)
		sendHttpError(w, " Unable to retrieve index metadata", http.StatusInternalServerError)
	}
}

func (m *requestHandlerContext) getLocalIndexMetadata() (meta *LocalIndexMetadata, err error) {

	repo := m.mgr.getMetadataRepo()

	meta = &LocalIndexMetadata{IndexTopologies: nil, IndexDefinitions: nil}
	indexerId, err := repo.GetLocalIndexerId()
	if err != nil {
		return nil, err
	}
	meta.IndexerId = string(indexerId)

	iter, err := repo.NewIterator()
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var defn *common.IndexDefn
	_, defn, err = iter.Next()
	for err == nil {
		meta.IndexDefinitions = append(meta.IndexDefinitions, *defn)
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
		meta.IndexTopologies = append(meta.IndexTopologies, *topology)
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

	if !doAuth(r, w, m.clusterUrl) {
		return
	}

	image := m.convertIndexMetadataRequest(r)
	if image == nil {
		send(w, &RestoreResponse{Code: RESP_ERROR, Error: "Unable to process request input"})
		return
	}

	indexerHostMap := make(map[common.IndexerId]string)
	current, err := m.getIndexMetadata(m.mgr.getServiceAddrProvider().(*common.ClusterInfoCache), indexerHostMap)
	if err != nil {
		send(w, &RestoreResponse{Code: RESP_ERROR, Error: "Unable to get the latest index metadata for restore"})
		return
	}

	context := &RestoreContext{idxToRestore: make(map[common.IndexerId][]common.IndexDefn),
		idxToResolve: make(map[common.IndexerId][]common.IndexDefn)}

	// Figure out what index to restore that has the same IndexDefnId
	for _, imeta := range image.Metadata {
		found := false
		for _, cmeta := range current.Metadata {
			if imeta.IndexerId == cmeta.IndexerId {
				m.findIndexToRestoreById(&imeta, &cmeta, context)
				found = true
			}
		}

		if !found {
			logging.Debugf("requestHandler.handleRestoreIndexMetadataRequest(): cannot find matching indexer id %s", imeta.IndexerId)
			for _, idefn := range imeta.IndexDefinitions {
				logging.Debugf("requestHandler.handleRestoreIndexMetadataRequest(): adding index definition (%v,%v) to to-be-resolve list", idefn.Bucket, idefn.Name)
				context.idxToResolve[common.IndexerId(imeta.IndexerId)] =
					append(context.idxToResolve[common.IndexerId(imeta.IndexerId)], idefn)
			}
		}
	}

	// Figure out what index to restore that has the same bucket and name
	for indexerId, idxs := range context.idxToResolve {
		for _, idx := range idxs {
			m.findIndexToRestoreByName(current, idx, indexerId, context)
		}
	}

	// recreate index
	success := m.restoreIndex(current, context, indexerHostMap)

	if success {
		send(w, &RestoreResponse{Code: RESP_SUCCESS})
		return
	}

	send(w, &RestoreResponse{Code: RESP_ERROR, Error: "Unable to restore metadata"})
}

func (m *requestHandlerContext) findIndexToRestoreById(image *LocalIndexMetadata,
	current *LocalIndexMetadata, context *RestoreContext) {

	context.idxToRestore[common.IndexerId(image.IndexerId)] = make([]common.IndexDefn, 0)
	context.idxToResolve[common.IndexerId(image.IndexerId)] = make([]common.IndexDefn, 0)

	for _, idefn := range image.IndexDefinitions {
		match := false
		for _, cdefn := range current.IndexDefinitions {
			if idefn.DefnId == cdefn.DefnId {
				// find index defn in the current repository, check the status
				logging.Debugf("requestHandler.findIndexToRestoreById(): find matching index definition (%v,%v) in repository", idefn.Bucket, idefn.Name)

				if topology := m.findTopologyByBucket(current.IndexTopologies, idefn.Bucket); topology != nil {
					match = true
					state, _ := topology.GetStatusByDefn(idefn.DefnId)
					logging.Debugf("requestHandler.findIndexToRestoreById(): index definition (%v,%v) in repository in state %d", idefn.Bucket, idefn.Name, state)

					if state == common.INDEX_STATE_DELETED || state == common.INDEX_STATE_NIL {
						// Index Defn exists it the current repository, but it must be
						// 1) Index Instance does not exist
						// 2) Index Instance has DELETED state
						logging.Debugf("requestHandler.findIndexToRestoreById(): adding index definition (%v,%v) to restore list", idefn.Bucket, idefn.Name)
						context.idxToRestore[common.IndexerId(image.IndexerId)] =
							append(context.idxToRestore[common.IndexerId(image.IndexerId)], idefn)
					}
				}
			}
		}

		if !match {
			// Index Defn does not exist.  Need to find if there is another index with matching <bucket, name>
			logging.Debugf("requestHandler.findIndexToRestoreById(): adding index definition (%v,%v) to to-be-resolve list", idefn.Bucket, idefn.Name)
			context.idxToResolve[common.IndexerId(image.IndexerId)] =
				append(context.idxToResolve[common.IndexerId(image.IndexerId)], idefn)
		}
	}
}

func (m *requestHandlerContext) findIndexToRestoreByName(current *ClusterIndexMetadata,
	defn common.IndexDefn, indexerId common.IndexerId, context *RestoreContext) {

	logging.Debugf("requestHandler.findIndexToRestoreById(): checking for index definition (%v,%v) in repository by name", defn.Bucket, defn.Name)
	for _, meta := range current.Metadata {
		for _, ldefn := range meta.IndexDefinitions {
			if ldefn.Bucket == defn.Bucket && ldefn.Name == defn.Name {
				if topology := m.findTopologyByBucket(meta.IndexTopologies, ldefn.Bucket); topology != nil {
					state, _ := topology.GetStatusByDefn(ldefn.DefnId)
					if state != common.INDEX_STATE_DELETED && state != common.INDEX_STATE_NIL {
						logging.Debugf("requestHandler.findIndexToRestoreByName(): find matching index definition (%v,%v) in repository in state %d", ldefn.Bucket, ldefn.Name, state)
						return
					}
				}
			}
		}
	}

	logging.Debugf("requestHandler.findIndexToRestoreByName(): adding index definition (%v,%v) to restore list", defn.Bucket, defn.Name)
	context.idxToRestore[common.IndexerId(indexerId)] = append(context.idxToRestore[common.IndexerId(indexerId)], defn)
}

func (m *requestHandlerContext) restoreIndex(current *ClusterIndexMetadata,
	context *RestoreContext, indexerHostMap map[common.IndexerId]string) bool {

	indexerCountMap := make(map[common.IndexerId]int)
	for _, meta := range current.Metadata {
		indexerCountMap[common.IndexerId(meta.IndexerId)] = len(meta.IndexDefinitions)
	}

	result := true
	for indexerId, idxs := range context.idxToRestore {
		for _, defn := range idxs {
			host, ok := indexerHostMap[indexerId]
			if !ok {
				indexerId = m.findMinIndexer(indexerCountMap)
				host = indexerHostMap[indexerId]
			}

			logging.Debugf("requestHandler.restoreIndex(): restore index definition (%v,%v) on host %v", defn.Bucket, defn.Name, host)
			result = result && m.makeCreateIndexRequest(defn, host)

			indexerCountMap[indexerId] = indexerCountMap[indexerId] + 1
		}
	}

	return result
}

func (m *requestHandlerContext) makeCreateIndexRequest(defn common.IndexDefn, host string) bool {

	id, err := common.NewIndexDefnId()
	if err != nil {
		logging.Debugf("requestHandler.makeCreateIndexRequest(): fail to generate index definition id %v", err)
		return false
	}
	defn.DefnId = id

	// deferred build for restore
	defn.Deferred = true

	req := IndexRequest{Version: uint64(1), Type: CREATE, Index: defn}
	body, err := json.Marshal(&req)
	if err != nil {
		logging.Debugf("requestHandler.makeCreateIndexRequest(): cannot marshall create index request %v", err)
		return false
	}

	bodybuf := bytes.NewBuffer(body)

	resp, err := postWithAuth(host+"/createIndex", "application/json", bodybuf)
	if err != nil {
		logging.Debugf("requestHandler.makeCreateIndexRequest(): create index request fails %v", err)
		return false
	}

	response := new(IndexResponse)
	status := convertResponse(resp, response)
	if status == RESP_ERROR || response.Code == RESP_ERROR {
		logging.Debugf("requestHandler.makeCreateIndexRequest(): create index request fails")
		return false
	}

	return true
}

func (m *requestHandlerContext) findTopologyByBucket(topologies []IndexTopology, bucket string) *IndexTopology {

	for _, topology := range topologies {
		if topology.Bucket == bucket {
			return &topology
		}
	}

	return nil
}

func (m *requestHandlerContext) findMinIndexer(indexerCountMap map[common.IndexerId]int) common.IndexerId {

	minIndexerId := common.INDEXER_ID_NIL
	minCount := math.MaxInt32

	for indexerId, count := range indexerCountMap {
		if count < minCount {
			minCount = count
			minIndexerId = indexerId
		}
	}

	return minIndexerId
}

///////////////////////////////////////////////////////
// Utility
///////////////////////////////////////////////////////

func sendIndexResponseWithError(w http.ResponseWriter, msg string) {
	res := &IndexResponse{Code: RESP_ERROR, Error: msg}
	send(w, res)
}

func sendIndexResponse(w http.ResponseWriter) {
	result := &IndexResponse{Code: RESP_SUCCESS}
	send(w, result)
}

func send(w http.ResponseWriter, res interface{}) {

	header := w.Header()
	header["Content-Type"] = []string{"application/json"}

	if buf, err := json.Marshal(res); err == nil {
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

func doAuth(r *http.Request, w http.ResponseWriter, clusterUrl string) bool {

	valid, err := common.IsAuthValid(r, clusterUrl)
	if err != nil {
		sendIndexResponseWithError(w, err.Error())
		return false
	} else if valid == false {
		w.WriteHeader(401)
		w.Write([]byte("401 Unauthorized\n"))
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
