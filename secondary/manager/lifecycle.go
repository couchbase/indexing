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
	c "github.com/couchbase/gometa/common"
	"github.com/couchbase/gometa/message"
	"github.com/couchbase/gometa/protocol"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/manager/client"
	//"runtime/debug"
)

type LifecycleMgr struct {
	repo      *MetadataRepo
	scanport  string
	notifier  MetadataNotifier
	incomings chan *requestHolder
	outgoings chan c.Packet
	killch    chan bool
}

type requestHolder struct {
	request protocol.RequestMsg
	fid     string
}

type topologyChange struct {
	Bucket   string `json:"bucket,omitempty"`
	DefnId   uint64 `json:"defnId,omitempty"`
	State    uint32 `json:"state,omitempty"`
	StreamId uint32 `json:"steamId,omitempty"`
	Error    string `json:"error,omitempty"`
}

func NewLifecycleMgr(scanport string, notifier MetadataNotifier) *LifecycleMgr {

	mgr := &LifecycleMgr{repo: nil,
		scanport:  scanport,
		notifier:  notifier,
		incomings: make(chan *requestHolder, 1000),
		outgoings: make(chan c.Packet, 1000),
		killch:    make(chan bool)}

	return mgr
}

func (m *LifecycleMgr) Run(repo *MetadataRepo) {
	m.repo = repo
	go m.processRequest()
}

func (m *LifecycleMgr) RegisterNotifier(notifier MetadataNotifier) {
	m.notifier = notifier
}

func (m *LifecycleMgr) Terminate() {
	if m.killch != nil {
		close(m.killch)
		m.killch = nil
	}
}

func (m *LifecycleMgr) OnNewRequest(fid string, request protocol.RequestMsg) {
	common.Debugf("LifecycleMgr.OnNewRequest(): queuing new request. reqId %v", request.GetReqId())
	
	m.incomings <- &requestHolder{request: request, fid: fid}
}

func (m *LifecycleMgr) GetResponseChannel() <-chan c.Packet {
	return (<-chan c.Packet)(m.outgoings)
}

func (m *LifecycleMgr) processRequest() {

	/*
	defer func() {
		if r := recover(); r != nil {
			common.Debugf("panic in LifecycleMgr.processRequest() : %s\n", r)
			common.Debugf("%s", debug.Stack())
		}
	}()
	*/

	common.Debugf("LifecycleMgr.processRequest(): LifecycleMgr is ready to proces request")
	factory := message.NewConcreteMsgFactory()

	for {
		select {
		case request, ok := <-m.incomings:
			if ok {
				// TOOD: deal with error
				m.dispatchRequest(request, factory)
			} else {
				// server shutdown.
				common.Debugf("LifecycleMgr.handleRequest(): channel for receiving client request is closed. Terminate.")
			}
		case <-m.killch:
			// server shutdown
			common.Debugf("LifecycleMgr.processRequest(): receive kill signal. Stop Client request processing.")
		}
	}
}

func (m *LifecycleMgr) dispatchRequest(request *requestHolder, factory *message.ConcreteMsgFactory) {

	reqId := request.request.GetReqId()
	op := c.OpCode(request.request.GetOpCode())
	key := request.request.GetKey()
	content := request.request.GetContent()
	fid := request.fid

	common.Debugf("LifecycleMgr.dispatchRequest () : requestId %d, op %d, key %v", reqId, op, key)
	
	var err error
	switch op {
	case client.OPCODE_CREATE_INDEX:
		err = m.handleCreateIndex(key, content, m.scanport)
	case client.OPCODE_UPDATE_INDEX_INST:
		err = m.handleTopologyChange(content)
	case client.OPCODE_DROP_INDEX:
		err = m.handleDeleteIndex(key)
	case client.OPCODE_BUILD_INDEX:
		err = m.handleBuildIndexes(content, m.scanport)
	}

	common.Debugf("LifecycleMgr.dispatchRequest () : send response for requestId %d", reqId)

	if err == nil {
		msg := factory.CreateResponse(fid, reqId, "")
		m.outgoings <- msg
	} else {
		msg := factory.CreateResponse(fid, reqId, err.Error())
		m.outgoings <- msg
	}
}

func (m *LifecycleMgr) handleCreateIndex(key string, content []byte, scanport string) error {

	defn, err := common.UnmarshallIndexDefn(content)
	if err != nil {
		common.Errorf("LifecycleMgr.handleCreateIndex() : createIndex fails. Unable to unmarshall index definition. Reason = %v", err)
		return err
	}

	return m.CreateIndex(defn, scanport)
}

func (m *LifecycleMgr) CreateIndex(defn *common.IndexDefn, scanport string) error {

	if err := m.repo.CreateIndex(defn); err != nil {
		common.Errorf("LifecycleMgr.handleCreateIndex() : createIndex fails. Reason = %v", err)
		return err
	}

	if err := m.repo.addIndexToTopology(defn, common.IndexInstId(defn.DefnId), scanport); err != nil {
		common.Errorf("LifecycleMgr.handleCreateIndex() : createIndex fails. Reason = %v", err)
		m.repo.DropIndexById(defn.DefnId)
		return err
	}

	if m.notifier != nil {
		if err := m.notifier.OnIndexCreate(defn); err != nil {
			common.Errorf("LifecycleMgr.handleCreateIndex() : createIndex fails. Reason = %v", err)
			m.repo.DropIndexById(defn.DefnId)
			m.repo.deleteIndexFromTopology(defn.Bucket, defn.DefnId)
			return err
		}
	}

	if err := m.updateIndexState(defn.Bucket, defn.DefnId, common.INDEX_STATE_READY); err != nil {
		common.Errorf("LifecycleMgr.handleCreateIndex() : createIndex fails. Reason = %v", err)

		if m.notifier != nil {
			m.notifier.OnIndexDelete(defn.DefnId)
		}
		m.repo.DropIndexById(defn.DefnId)
		m.repo.deleteIndexFromTopology(defn.Bucket, defn.DefnId)
		return err
	}

	if !defn.Deferred {
		if m.notifier != nil {
			common.Debugf("LifecycleMgr.handleCreateIndex() : start Index Build")
			if err := m.notifier.OnIndexBuild([]common.IndexDefnId{defn.DefnId}); err != nil {
				common.Errorf("LifecycleMgr.hanaleCreateIndex() : createIndex fails. Reason = %v", err)
				return err
			}
		}
	}

	common.Debugf("LifecycleMgr.handleCreateIndex() : createIndex completes")

	return nil
}

func (m *LifecycleMgr) handleBuildIndexes(content []byte, scanport string) error {

	list, err := client.UnmarshallIndexIdList(content)
	if err != nil {
		common.Errorf("LifecycleMgr.handleBuildIndexes() : buildIndex fails. Unable to unmarshall index list. Reason = %v", err)
		return err
	}

	input := make([]common.IndexDefnId, len(list.DefnIds))
	for i, id := range list.DefnIds {
		input[i] = common.IndexDefnId(id)
	}

	return m.BuildIndexes(input, scanport)
}

func (m *LifecycleMgr) BuildIndexes(ids []common.IndexDefnId, scanport string) error {

	for _, id := range ids {
		_, err := m.repo.GetIndexDefnById(id)
		if err != nil {
			common.Errorf("LifecycleMgr.handleBuildIndexes() : buildIndex fails. Reason = %v", err)
			return err
		}
	}

	if m.notifier != nil {
		if err := m.notifier.OnIndexBuild(ids); err != nil {
			common.Errorf("LifecycleMgr.hanaleBuildIndexes() : buildIndex fails. Reason = %v", err)
			return err
		}
	}

	common.Debugf("LifecycleMgr.handleBuildIndexes() : buildIndex completes")

	return nil
}

func (m *LifecycleMgr) handleDeleteIndex(key string) error {

	id, err := indexDefnId(key)
	if err != nil {
		common.Errorf("LifecycleMgr.handleDeleteIndex() : deleteIndex fails. Reason = %v", err)
		return err
	}

	return m.DeleteIndex(id)
}

func (m *LifecycleMgr) DeleteIndex(id common.IndexDefnId) error {

	defn, err := m.repo.GetIndexDefnById(id)
	if err != nil {
		return err
	}

	if err := m.updateIndexState(defn.Bucket, defn.DefnId, common.INDEX_STATE_DELETED); err != nil {
		common.Errorf("LifecycleMgr.handleDeleteIndex() : deleteIndex fails. Reason = %v", err)
		return err
	}

	if m.notifier != nil {
		m.notifier.OnIndexDelete(defn.DefnId)
	}
	m.repo.DropIndexById(defn.DefnId)
	m.repo.deleteIndexFromTopology(defn.Bucket, defn.DefnId)

	return nil
}

func (m *LifecycleMgr) handleTopologyChange(content []byte) error {

	change := new(topologyChange)
	if err := json.Unmarshal(content, change); err != nil {
		return err
	}

	return m.UpdateIndexInstance(change.Bucket, common.IndexDefnId(change.DefnId), common.IndexState(change.State),
		common.StreamId(change.StreamId), change.Error)
}

func (m *LifecycleMgr) UpdateIndexInstance(bucket string, defnId common.IndexDefnId, state common.IndexState,
	streamId common.StreamId, errStr string) error {

	topology, err := m.repo.GetTopologyByBucket(bucket)
	if err != nil {
		common.Errorf("LifecycleMgr.handleTopologyChange() : index instance update fails. Reason = %v", err)
		return err
	}

	if state != common.INDEX_STATE_NIL {
		topology.UpdateStateForIndexInstByDefn(common.IndexDefnId(defnId), common.IndexState(state))
	}
	
	if streamId != common.NIL_STREAM {
		topology.UpdateStreamForIndexInstByDefn(common.IndexDefnId(defnId), common.StreamId(streamId))
	}

	topology.SetErrorForIndexInstByDefn(common.IndexDefnId(defnId), errStr)

	if err := m.repo.SetTopologyByBucket(bucket, topology); err != nil {
		common.Errorf("LifecycleMgr.handleTopologyChange() : index instance update fails. Reason = %v", err)
		return err
	}

	return nil
}

func (m *LifecycleMgr) updateIndexState(bucket string, defnId common.IndexDefnId, state common.IndexState) error {

	topology, err := m.repo.GetTopologyByBucket(bucket)
	if err != nil {
		common.Errorf("LifecycleMgr.updateIndexState() : fails to find index instance. Reason = %v", err)
		return err
	}

	topology.UpdateStateForIndexInstByDefn(defnId, state)

	if err := m.repo.SetTopologyByBucket(bucket, topology); err != nil {
		common.Errorf("LifecycleMgr.updateIndexState() : fail to update state of index instance.  Reason = %v", err)
		return err
	}

	return nil
}
