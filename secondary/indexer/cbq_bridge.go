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
	"log"
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

}

const CBQ_BRIDGE_HTTP_ADDR = ":9101"

func NewCbqBridge(supvCmdch MsgChannel, supvRespch MsgChannel) (
	CbqBridge, Message) {

	//Init the cbqBridge struct
	cbq := &cbqBridge{
		supvCmdch:  supvCmdch,
		supvRespch: supvRespch,
	}

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

	log.Println("CbqBridge: Indexer Listening on", CBQ_BRIDGE_HTTP_ADDR)
	if err := http.ListenAndServe(CBQ_BRIDGE_HTTP_ADDR, nil); err != nil {
		log.Printf("CbqBridge: Error Starting Http Server: %v", err)
		return err
	}
	return nil

}

// /create
func (cbq *cbqBridge) handleCreate(w http.ResponseWriter, r *http.Request) {
	var res IndexMetaResponse

	indexinfo := indexRequest(r).Index // Get IndexInfo

	//generate a new unique id
	uuid := rand.Int()

	idxDefn := common.IndexDefn{DefnId: common.IndexDefnId(uuid),
		Name:            indexinfo.Name,
		Using:           common.ForestDB,
		Bucket:          indexinfo.Bucket,
		IsPrimary:       indexinfo.IsPrimary,
		OnExprList:      indexinfo.OnExprList,
		ExprType:        common.N1QL,
		PartitionScheme: common.TEST,
		PartitionKey:    indexinfo.OnExprList[0]}

	pc := common.NewKeyPartitionContainer()
	//Add one partition for now

	endpt := []common.Endpoint{"localhost:8100"}
	partnDefn := common.KeyPartitionDefn{Id: common.PartitionId(1),
		Endpts: endpt}
	pc.AddPartition(common.PartitionId(1), partnDefn)

	idxInst := common.IndexInst{InstId: common.IndexInstId(uuid),
		Defn:  idxDefn,
		State: common.INDEX_STATE_INITIAL,
		Pc:    pc,
	}

	indexinfo.Uuid = strconv.Itoa(uuid)

	cbq.supvRespch <- &MsgCreateIndex{indexInst: idxInst}

	res = IndexMetaResponse{
		Status:     RESP_SUCCESS,
		Indexes:    []IndexInfo{indexinfo},
		ServerUuid: "",
	}

	sendResponse(w, res)
}

//drop
func (cbq *cbqBridge) handleDrop(w http.ResponseWriter, r *http.Request) {
	var res IndexMetaResponse

	indexinfo := indexRequest(r).Index

	uuid, _ := strconv.Atoi(indexinfo.Uuid)
	cbq.supvRespch <- &MsgDropIndex{indexInstId: common.IndexInstId(uuid)}

	res = IndexMetaResponse{
		Status:     RESP_SUCCESS,
		ServerUuid: "",
	}
	sendResponse(w, res)
}

// /list
func (cbq *cbqBridge) handleList(w http.ResponseWriter, r *http.Request) {
	var res IndexMetaResponse

	serverUuid := indexRequest(r).ServerUuid
	res = IndexMetaResponse{
		Status:     RESP_SUCCESS,
		Indexes:    nil,
		ServerUuid: serverUuid,
	}
	sendResponse(w, res)
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
		log.Println("Unable to marshal response", res)
	}
	w.Write(buf)
}
