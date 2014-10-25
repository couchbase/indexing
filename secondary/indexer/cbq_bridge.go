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

func NewCbqBridge(supvCmdch MsgChannel, supvRespch MsgChannel) (
	CbqBridge, Message) {

	//Init the cbqBridge struct
	cbq := &cbqBridge{
		supvCmdch:  supvCmdch,
		supvRespch: supvRespch,
		indexMap:   make(map[common.IndexInstId]IndexInfo),
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
	http.HandleFunc("/scan", cbq.handleScan)

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

	/*
		pc := common.NewKeyPartitionContainer()

		//Add one partition for now
		endpt := []common.Endpoint{INDEXER_MAINT_DATA_PORT_ENDPOINT}
		partnDefn := common.KeyPartitionDefn{Id: common.PartitionId(1),
			Endpts: endpt}
		pc.AddPartition(common.PartitionId(1), partnDefn)
	*/

	idxInst := common.IndexInst{InstId: common.IndexInstId(uuid),
		Defn:  idxDefn,
		State: common.INDEX_STATE_INITIAL,
		//Pc:    pc,
	}

	indexinfo.Uuid = strconv.Itoa(uuid)

	respCh := make(MsgChannel)
	cbq.supvRespch <- &MsgCreateIndex{mType: CBQ_CREATE_INDEX_DDL,
		indexInst: idxInst,
		respCh:    respCh}

	//wait for response from indexer
	msg := <-respCh
	if msg.GetMsgType() == MSG_SUCCESS {
		res = IndexMetaResponse{
			Status:     RESP_SUCCESS,
			Indexes:    []IndexInfo{indexinfo},
			ServerUuid: "",
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

	uuid, _ := strconv.Atoi(indexinfo.Uuid)

	respCh := make(MsgChannel)
	cbq.supvRespch <- &MsgDropIndex{mType: CBQ_DROP_INDEX_DDL,
		indexInstId: common.IndexInstId(uuid),
		respCh:      respCh}

	//wait for response from indexer
	msg := <-respCh
	if msg.GetMsgType() == MSG_SUCCESS {
		res = IndexMetaResponse{
			Status:     RESP_SUCCESS,
			ServerUuid: "",
		}
		delete(cbq.indexMap, common.IndexInstId(uuid))
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

	serverUuid := indexRequest(r).ServerUuid

	common.Debugf("CbqBridge::handleList Received ListIndex")

	var indexList []IndexInfo
	for _, idx := range cbq.indexMap {
		indexList = append(indexList, idx)
	}

	res = IndexMetaResponse{
		Status:     RESP_SUCCESS,
		Indexes:    indexList,
		ServerUuid: serverUuid,
	}
	sendResponse(w, res)
}

//Scan
func (cbq *cbqBridge) handleScan(w http.ResponseWriter, r *http.Request) {

	indexreq := indexRequest(r)
	uuid, _ := strconv.Atoi(indexreq.Index.Uuid)
	qp := indexreq.Params

	common.Debugf("CbqBridge::handleScan Received ScanIndex %v", indexreq)

	var lowkey, highkey Key
	var err error

	if lowkey, err = NewKey(qp.Low, []byte("")); err != nil {
		ierr := IndexError{Code: string(RESP_ERROR),
			Msg: err.Error()}
		sendScanResponse(w, nil, 0, []IndexError{ierr})
		return
	}

	if highkey, err = NewKey(qp.High, []byte("")); err != nil {
		ierr := IndexError{Code: string(RESP_ERROR),
			Msg: err.Error()}
		sendScanResponse(w, nil, 0, []IndexError{ierr})
		return
	}

	p := ScanParams{scanType: qp.ScanType,
		low:      lowkey,
		high:     highkey,
		partnKey: []byte("partnKey"), //dummy partn key for now
		incl:     qp.Inclusion,
		limit:    qp.Limit,
	}

	msgScan := &MsgScanIndex{scanId: rand.Int63(),
		indexInstId: common.IndexInstId(uuid),
		stopch:      make(StopChannel),
		p:           p,
		resCh:       make(chan Value),
		errCh:       make(chan Message),
		countCh:     make(chan uint64)}

	//send scan request to Indexer
	cbq.supvRespch <- msgScan

	cbq.receiveValue(w, msgScan.scanId, msgScan.resCh,
		msgScan.countCh, msgScan.errCh)

}

//receiveValue keeps listening to the response/error channels for results/errors
//till any of the response/error channel is closed by the sender.
//All results/errors received are appended to the response which will be
//sent back to Cbq Engine.
func (cbq *cbqBridge) receiveValue(w http.ResponseWriter, scanId int64,
	chres chan Value, chcount chan uint64, cherr chan Message) {

	var totalRows uint64
	rows := make([]IndexRow, 0)
	errors := make([]IndexError, 0)

	ok := true
	var value Value
	var errMsg Message

	for ok {
		select {
		case value, ok = <-chres:
			if ok {
				common.Tracef("CbqBridge::receiveValue ScanId %v Received Value %s",
					scanId, value.String())

				row := IndexRow{
					Key:   value.KeyBytes(),
					Value: string(value.Docid()),
				}
				rows = append(rows, row)
			}

		case totalRows, ok = <-chcount:
			if ok {
				common.Tracef("CbqBridge::receiveValue ScanId %v Received Count %s",
					scanId, totalRows)
			}

		case errMsg, ok = <-cherr:
			if ok {
				err := errMsg.(*MsgError).GetError()
				common.Tracef("CbqBridge::receiveValue ScanId %v Received Error %s",
					scanId, err.cause)
				ierr := IndexError{Code: string(RESP_ERROR),
					Msg: err.cause.Error()}
				errors = append(errors, ierr)
			}
		}
	}

	sendScanResponse(w, rows, totalRows, errors)

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

func sendScanResponse(w http.ResponseWriter, rows []IndexRow,
	totalRows uint64, errors []IndexError) {

	var res IndexScanResponse

	if len(errors) == 0 {
		res = IndexScanResponse{
			Status:    RESP_SUCCESS,
			TotalRows: totalRows,
			Rows:      rows,
			Errors:    nil,
		}
	} else {
		res = IndexScanResponse{
			Status:    RESP_ERROR,
			TotalRows: uint64(0),
			Rows:      nil,
			Errors:    errors,
		}
	}
	sendResponse(w, res)
}
