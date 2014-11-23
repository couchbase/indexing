// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package indexer

import (
	"encoding/json"
	"github.com/couchbase/indexing/secondary/common"
	"math/rand"
	"net/http"
	"strconv"
)

//CbqBridge is a temporary solution to allow Cbq Engine to talk to Indexing
type CbqBridge interface {
}

type cbqBridge struct {
	supvCmdch  MsgChannel //supervisor sends commands on this channel
	supvRespch MsgChannel //channel to send any message to supervisor

	indexMap map[common.IndexInstId]IndexInfo
}

func NewCbqBridge(supvCmdch MsgChannel, supvRespch MsgChannel,
	indexInstMap common.IndexInstMap) (
	CbqBridge, Message) {

	//Init the cbqBridge struct
	cbq := &cbqBridge{
		supvCmdch:  supvCmdch,
		supvRespch: supvRespch,
		indexMap:   make(map[common.IndexInstId]IndexInfo),
	}

	cbq.updateIndexMap(indexInstMap)

	go cbq.initCbqBridge()

	//start the loop which listens to commands from its supervisor
	go cbq.run()

	return cbq, &MsgSuccess{}

}

//run starts the loop which listens to messages
//from it supervisor(indexer)
func (cbq *cbqBridge) run() {

	//main loop
loop:
	for {
		select {

		case cmd, ok := <-cbq.supvCmdch:
			if ok {
				if cmd.GetMsgType() == CBQ_BRIDGE_SHUTDOWN {
					break loop
				}
				cbq.handleSupvervisorCommands(cmd)
			} else {
				//supervisor channel closed. exit
				break loop
			}

		}
	}
}

func (cbq *cbqBridge) handleSupvervisorCommands(cmd Message) {

	//TODO
}

func (cbq *cbqBridge) initCbqBridge() error {

	// Subscribe to HTTP server handlers
	http.HandleFunc("/create", cbq.handleCreate)
	http.HandleFunc("/drop", cbq.handleDrop)
	http.HandleFunc("/list", cbq.handleList)

	common.Infof("CbqBridge::initCbqBridge Listening on %v", CBQ_BRIDGE_HTTP_ADDR)
	if err := http.ListenAndServe(CBQ_BRIDGE_HTTP_ADDR, nil); err != nil {
		common.Errorf("CbqBridge: Error Starting Http Server: %v", err)
		return err
	}
	return nil

}

//create
func (cbq *cbqBridge) handleCreate(w http.ResponseWriter, r *http.Request) {
	var res IndexMetaResponse

	indexinfo := indexRequest(r).Index

	common.Debugf("CbqBridge::handleCreate Received CreateIndex %v", indexinfo)

	//generate a new unique id
	defnID := rand.Int()

	idxDefn := common.IndexDefn{DefnId: common.IndexDefnId(defnID),
		Name:            indexinfo.Name,
		Using:           common.ForestDB,
		Bucket:          indexinfo.Bucket,
		IsPrimary:       indexinfo.IsPrimary,
		SecExprs:        indexinfo.SecExprs,
		ExprType:        common.N1QL,
		PartitionScheme: common.SINGLE,
		PartitionKey:    indexinfo.PartnExpr}

	idxInst := common.IndexInst{InstId: common.IndexInstId(defnID),
		Defn:  idxDefn,
		State: common.INDEX_STATE_INITIAL,
	}

	if !ENABLE_MANAGER {
		pc := common.NewKeyPartitionContainer()

		//Add one partition for now
		partnId := common.PartitionId(0)
		endpt := []common.Endpoint{INDEXER_MAINT_DATA_PORT_ENDPOINT}
		partnDefn := common.KeyPartitionDefn{Id: partnId,
			Endpts: endpt}
		pc.AddPartition(partnId, partnDefn)

		idxInst.Pc = pc
	}

	indexinfo.DefnID = strconv.Itoa(defnID)

	respCh := make(MsgChannel)
	cbq.supvRespch <- &MsgCreateIndex{mType: CBQ_CREATE_INDEX_DDL,
		indexInst: idxInst,
		respCh:    respCh}

	//wait for response from indexer
	msg := <-respCh
	if msg.GetMsgType() == MSG_SUCCESS {
		res = IndexMetaResponse{
			Status:  RESP_SUCCESS,
			Indexes: []IndexInfo{indexinfo},
		}
		cbq.indexMap[idxInst.InstId] = indexinfo
	} else {
		err := msg.(*MsgError).GetError()

		common.Debugf("CbqBridge::handleCreate Received Error %s", err.cause)

		ierr := IndexError{Code: string(RESP_ERROR),
			Msg: err.cause.Error()}

		res = IndexMetaResponse{
			Status: RESP_ERROR,
			Errors: []IndexError{ierr},
		}
	}
	sendResponse(w, res)
}

//drop
func (cbq *cbqBridge) handleDrop(w http.ResponseWriter, r *http.Request) {
	var res IndexMetaResponse

	indexinfo := indexRequest(r).Index

	common.Debugf("CbqBridge::handleDrop Received DropIndex %v", indexinfo)

	defnID, _ := strconv.Atoi(indexinfo.DefnID)

	respCh := make(MsgChannel)
	cbq.supvRespch <- &MsgDropIndex{mType: CBQ_DROP_INDEX_DDL,
		indexInstId: common.IndexInstId(defnID),
		respCh:      respCh}

	//wait for response from indexer
	msg := <-respCh
	if msg.GetMsgType() == MSG_SUCCESS {
		res = IndexMetaResponse{
			Status: RESP_SUCCESS,
		}
		delete(cbq.indexMap, common.IndexInstId(defnID))
	} else {
		err := msg.(*MsgError).GetError()

		common.Debugf("CbqBridge: DropIndex Received Error %s", err.cause)

		ierr := IndexError{Code: string(RESP_ERROR),
			Msg: err.cause.Error()}

		res = IndexMetaResponse{
			Status: RESP_ERROR,
			Errors: []IndexError{ierr},
		}
	}
	sendResponse(w, res)

}

//list
func (cbq *cbqBridge) handleList(w http.ResponseWriter, r *http.Request) {
	var res IndexMetaResponse

	common.Debugf("CbqBridge::handleList Received ListIndex")

	var indexList []IndexInfo
	for _, idx := range cbq.indexMap {
		indexList = append(indexList, idx)
	}

	res = IndexMetaResponse{
		Status:  RESP_SUCCESS,
		Indexes: indexList,
	}
	sendResponse(w, res)
}

//handleUpdateIndexInstMap updates the indexInstMap
func (cbq *cbqBridge) updateIndexMap(indexInstMap common.IndexInstMap) {

	common.Debugf("CbqBridge::updateIndexMap %v", indexInstMap)

	for id, inst := range indexInstMap {
		cbq.indexMap[id] = getIndexInfoFromInst(inst)
	}

}

func getIndexInfoFromInst(inst common.IndexInst) IndexInfo {

	var idx IndexInfo

	idx.Name = inst.Defn.Name
	idx.Bucket = inst.Defn.Name
	idx.DefnID = strconv.Itoa(int(inst.Defn.DefnId))
	idx.Using = string(inst.Defn.Using)
	idx.Exprtype = string(inst.Defn.ExprType)
	idx.PartnExpr = inst.Defn.PartitionKey
	idx.SecExprs = inst.Defn.SecExprs
	idx.WhereExpr = ""
	idx.IsPrimary = inst.Defn.IsPrimary

	return idx

}

// Parse HTTP Request to get IndexInfo.
func indexRequest(r *http.Request) *IndexRequest {
	indexreq := IndexRequest{}
	buf := make([]byte, r.ContentLength, r.ContentLength)
	r.Body.Read(buf)
	json.Unmarshal(buf, &indexreq)
	return &indexreq
}

func createMetaResponseFromError(err error) IndexMetaResponse {

	indexerr := IndexError{Code: string(RESP_ERROR), Msg: err.Error()}
	res := IndexMetaResponse{
		Status: RESP_ERROR,
		Errors: []IndexError{indexerr},
	}
	return res
}

func sendResponse(w http.ResponseWriter, res interface{}) {
	var buf []byte
	var err error
	header := w.Header()
	header["Content-Type"] = []string{"application/json"}

	if buf, err = json.Marshal(&res); err != nil {
		common.Errorf("CbqBridge::sendResponse Unable to marshal response", res)
	}
	w.Write(buf)
}
