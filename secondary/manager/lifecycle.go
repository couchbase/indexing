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
	"runtime/debug"
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

const topologyChangeKey = "::topologyChange::"

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
		incomings: make(chan *requestHolder),
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
	m.incomings <- &requestHolder{request: request, fid: fid}
}

func (m *LifecycleMgr) GetResponseChannel() <-chan c.Packet {
	return (<-chan c.Packet)(m.outgoings)
}

func (m *LifecycleMgr) processRequest() {

	defer func() {
		if r := recover(); r != nil {
			common.Debugf("panic in LifecycleMgr.processRequest() : %s\n", r)
			common.Debugf("%s", debug.Stack())
		}
	}()

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

	var err error
	switch op {
	case c.OPCODE_CUSTOM_ADD:
		err = m.handleCreateIndex(key, content, m.scanport)
	case c.OPCODE_CUSTOM_SET:
		if key == topologyChangeKey {
			err = m.handleTopologyChange(content)
		} else {
			err = m.handleCreateIndex(key, content, m.scanport)
		}
	case c.OPCODE_CUSTOM_DELETE:
		err = m.handleDeleteIndex(key)
	}

	common.Debugf("LifecycleMgr.dispatchRequest () : send response")

	if err == nil {
		msg := factory.CreateResponse(fid, reqId, "")
		m.outgoings <- msg
	} else {
		msg := factory.CreateResponse(fid, reqId, err.Error())
		m.outgoings <- msg
	}
}

func (m *LifecycleMgr) handleCreateIndex(key string, content []byte, scanport string) error {

	defn, err := UnmarshallIndexDefn(content)
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

	if err := m.repo.addIndexToTopology(defn, scanport); err != nil {
		common.Errorf("LifecycleMgr.hanaleCreateIndex() : createIndex fails. Reason = %v", err)
		m.repo.DropIndexById(defn.DefnId)
		return err
	}

	common.Debugf("LifecycleMgr.handleCreateIndex() : before Index Build")

	if m.notifier != nil {
		if err := m.notifier.OnIndexBuild([]common.IndexDefnId{defn.DefnId}); err != nil {
			common.Errorf("LifecycleMgr.hanaleCreateIndex() : createIndex fails. Reason = %v", err)
			m.repo.DropIndexById(defn.DefnId)
			m.repo.deleteIndexFromTopology(defn.Bucket, defn.DefnId)
			return err
		}
	}

	common.Debugf("LifecycleMgr.handleCreateIndex() : createIndex completes")

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

	defn, _ := m.repo.GetIndexDefnById(id)

	// Drop the index defnition before removing it from the topology.  If it fails to
	// remove the index defn from topology, it can mean that there is a dangling reference
	// in the topology with a deleted index defn, but it is easier to detect.
	if err := m.repo.DropIndexById(id); err != nil {
		common.Errorf("LifecycleMgr.handleDeleteIndex() : deleteIndex fails. Reason = %v", err)
		return err
	}

	if err := m.repo.deleteIndexFromTopology(defn.Bucket, id); err != nil {
		common.Errorf("LifecycleMgr.handleDeleteIndex() : deleteIndex fails. Reason = %v", err)
		return err
	}

	return nil
}

func (m *LifecycleMgr) handleTopologyChange(content []byte) error {

	change := new(topologyChange)
	if err := json.Unmarshal(content, change); err != nil {
		return err
	}

	topology, err := m.repo.GetTopologyByBucket(change.Bucket)
	if err != nil {
		common.Errorf("LifecycleMgr.handleTopologyChange() : fails to find topology. Reason = %v", err)
		return err
	}

	topology.UpdateStateForIndexInstByDefn(common.IndexDefnId(change.DefnId), common.IndexState(change.State))
	topology.UpdateStreamForIndexInstByDefn(common.IndexDefnId(change.DefnId), common.StreamId(change.StreamId))

	if len(change.Error) != 0 {
		topology.SetErrorForIndexInstByDefn(common.IndexDefnId(change.DefnId), change.Error)
	}

	if err := m.repo.SetTopologyByBucket(change.Bucket, topology); err != nil {
		common.Errorf("LifecycleMgr.handleTopologyChange() : toplogy change fails. Reason = %v", err)
		return err
	}

	return nil
}
