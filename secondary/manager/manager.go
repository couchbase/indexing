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
	"fmt"
	gometa "github.com/couchbase/gometa/common"
	"github.com/couchbase/indexing/secondary/common"
	"sync"
	"time"
)

///////////////////////////////////////////////////////
// Type Definition
///////////////////////////////////////////////////////

type IndexManager struct {
	repo        *MetadataRepo
	coordinator *Coordinator
	reqHandler  *requestHandler
	eventMgr    *eventManager

	// stream management
	streamMgr *StreamManager
	admin     StreamAdmin

	// timestamp management
	timer            *Timer
	timestampCh      map[common.StreamId]chan *common.TsVbuuid
	timekeeperStopCh chan bool

	mutex    sync.Mutex
	isClosed bool
}

///////////////////////////////////////////////////////
// public function
///////////////////////////////////////////////////////

//
// Create a new IndexManager
//
func NewIndexManager(requestAddr string,
	leaderAddr string,
	config string) (mgr *IndexManager, err error) {

	return NewIndexManagerInternal(requestAddr, leaderAddr, config, nil)
}

//
// Create a new IndexManager
//
func NewIndexManagerInternal(requestAddr string,
	leaderAddr string,
	config string,
	admin StreamAdmin) (mgr *IndexManager, err error) {

	mgr = new(IndexManager)
	mgr.isClosed = false

	// stream mgmt	- stream services will start if the indexer node becomes master
	mgr.streamMgr = nil
	mgr.admin = admin

	// timestamp mgmt	- timestamp servcie will start if indexer node becomes master
	mgr.timestampCh = make(map[common.StreamId]chan *common.TsVbuuid)
	mgr.timer = nil
	mgr.timekeeperStopCh = nil

	// Initialize MetadataRepo.  This a blocking call until the
	// the metadataRepo (including watcher) is operational (e.g.
	// finish sync with remote metadata repo master).
	mgr.repo, err = NewMetadataRepo(requestAddr, leaderAddr, config, mgr)
	if err != nil {
		return nil, err
	}

	// Initialize request handler.  This is non-blocking.  The index manager
	// will not be able handle new request until request handler is done initialization.
	mgr.reqHandler, err = NewRequestHandler(mgr)
	if err != nil {
		mgr.Close()
		return nil, err
	}

	// Initialize the event manager.  This is non-blocking.  The event manager can be
	// called indirectly by watcher/meta-repo when new metadata changes are sent
	// from gometa master to the indexer node.
	mgr.eventMgr, err = newEventManager()
	if err != nil {
		mgr.Close()
		return nil, err
	}

	// Initialize Coordinator.  This is non-blocking.  The coordinator
	// is operational only after it can syncrhonized with the majority
	// of the indexers.   Any request made to the coordinator will be
	// put in a channel for later processing (once leader election is done).
	// Once the coordinator becomes the leader, it will invoke teh stream
	// manager.
	mgr.coordinator = NewCoordinator(mgr.repo, mgr)
	go mgr.coordinator.Run(config)

	return mgr, nil
}

func (m *IndexManager) IsClose() bool {

	m.mutex.Lock()
	defer m.mutex.Unlock()

	return m.isClosed
}

//
// Clean up the IndexManager
//
func (m *IndexManager) Close() {

	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.isClosed {
		return
	}

	m.stopMasterServiceNoLock()

	if m.repo != nil {
		m.repo.Close()
	}

	if m.coordinator != nil {
		m.coordinator.Terminate()
	}

	if m.eventMgr != nil {
		m.eventMgr.close()
	}

	if m.reqHandler != nil {
		m.reqHandler.close()
	}

	m.isClosed = true
}

///////////////////////////////////////////////////////
// public function - Metadata Operation
///////////////////////////////////////////////////////

//
// Get an index definiton by name
//
func (m *IndexManager) GetIndexDefnByName(bucket string, name string) (*common.IndexDefn, error) {
	return m.repo.GetIndexDefnByName(bucket, name)
}

//
// Get an index definiton by id
//
func (m *IndexManager) GetIndexDefnById(id common.IndexDefnId) (*common.IndexDefn, error) {
	return m.repo.GetIndexDefnById(id)
}

//
// Get Metadata Iterator for index definition
//
func (m *IndexManager) NewIndexDefnIterator() (*MetaIterator, error) {
	return m.repo.NewIterator()
}

//
// Listen to create Index Request
//
func (m *IndexManager) StartListenIndexCreate(id string) (<-chan interface{}, error) {
	return m.eventMgr.register(id, EVENT_CREATE_INDEX)
}

//
// Stop Listen to create Index Request
//
func (m *IndexManager) StopListenIndexCreate(id string) {
	m.eventMgr.unregister(id, EVENT_CREATE_INDEX)
}

//
// Listen to delete Index Request
//
func (m *IndexManager) StartListenIndexDelete(id string) (<-chan interface{}, error) {
	return m.eventMgr.register(id, EVENT_DROP_INDEX)
}

//
// Stop Listen to delete Index Request
//
func (m *IndexManager) StopListenIndexDelete(id string) {
	m.eventMgr.unregister(id, EVENT_DROP_INDEX)
}

//
// Listen to update Topology Request
//
func (m *IndexManager) StartListenTopologyUpdate(id string) (<-chan interface{}, error) {
	return m.eventMgr.register(id, EVENT_UPDATE_TOPOLOGY)
}

//
// Stop Listen to update Topology Request
//
func (m *IndexManager) StopListenTopologyUpdate(id string) {
	m.eventMgr.unregister(id, EVENT_UPDATE_TOPOLOGY)
}

//
// Handle Create Index DDL.  This function will block until
// 1) The index defn is persisted durably in the dictionary
// 2) The index defn is applied locally to each "active" indexer
//    node.  An active node is a running node that is in the same
//    network partition as the leader.   A leader is always in
//    the majority partition.
//
// This function will return an error if the outcome of the
// request is not known (e.g. the node is partitioned
// from the network).  It may still mean that the request
// is able to go through (processed by some other nodes).
//
// A Index DDL can be processed by any node. If this node is a leader,
// then the DDL request will be processed by the leader.  If it is a
// follower, it will forward the request to the leader.
//
// This function will not be processed until the index manager
// is either a leader or follower. Therefore, if (1) the node is
// in the minority partition after network partition or (2) the leader
// dies, this node will unblock any in-flight request initiated
// by this node (by returning error).  The node will run leader
// election again. Until this node has became a leader or follower,
// it will not be able to handle another request.
//
// If this node is partitioned from its leader, it can still recieve
// updates from the dictionary if this node still connects to it.
//
func (m *IndexManager) HandleCreateIndexDDL(defn *common.IndexDefn) error {

	//
	// Save the index definition
	//
	content, err := marshallIndexDefn(defn)
	if err != nil {
		return err
	}

	// TODO: Make request id a string
	id := uint64(time.Now().UnixNano())
	if !m.coordinator.NewRequest(id, uint32(OPCODE_ADD_IDX_DEFN), indexName(defn.Bucket, defn.Name), content) {
		// TODO: double check if it exists in the dictionary
		return NewError(ERROR_MGR_DDL_CREATE_IDX, NORMAL, INDEX_MANAGER, nil,
			fmt.Sprintf("Fail to complete processing create index statement for index '%s'", defn.Name))
	}

	return nil
}

func (m *IndexManager) HandleDeleteIndexDDL(bucket string, name string) error {

	// TODO: Make request id a string
	id := uint64(time.Now().UnixNano())
	if !m.coordinator.NewRequest(id, uint32(OPCODE_DEL_IDX_DEFN), indexName(bucket, name), nil) {
		// TODO: double check if it exists in the dictionary
		return NewError(ERROR_MGR_DDL_DROP_IDX, NORMAL, INDEX_MANAGER, nil,
			fmt.Sprintf("Fail to complete processing delete index statement for index '%s'", name))
	}

	return nil
}

//
// Get Topology from dictionary
//
func (m *IndexManager) GetTopologyByBucket(bucket string) (*IndexTopology, error) {

	return m.repo.GetTopologyByBucket(bucket)
}

//
// Set Topology to dictionary
//
func (m *IndexManager) SetTopologyByBucket(bucket string, topology *IndexTopology) error {

	return m.repo.SetTopologyByBucket(bucket, topology)
}

//
// Get the global topology
//
func (m *IndexManager) GetGlobalTopology() (*GlobalTopology, error) {

	return m.repo.GetGlobalTopology()
}

///////////////////////////////////////////////////////
// public function - Timestamp Operation
///////////////////////////////////////////////////////

func (m *IndexManager) GetStabilityTimestampChannel(streamId common.StreamId) chan *common.TsVbuuid {

	ch, ok := m.timestampCh[streamId]
	if !ok {
		ch = make(chan *common.TsVbuuid, TIMESTAMP_NOTIFY_CH_SIZE)
		m.timestampCh[streamId] = ch
	}

	return ch
}

func (m *IndexManager) runTimestampKeeper() {

	defer common.Debugf("IndexManager.runTimestampKeeper() : terminate")

	inboundch := m.timer.getOutputChannel()
	for {
		select {
		case <-m.timekeeperStopCh:
			return

		case timestamp, ok := <-inboundch:

			if !ok {
				return
			}

			gometa.SafeRun("IndexManager.runTimestampKeeper()",
				func() {
					data, err := marshallTimestampWrapper(timestamp)
					if err != nil {
						common.Debugf(
							"IndexManager.runTimestampKeeper(): error when marshalling timestamp. Ignore timestamp.  Error=%s",
							err.Error())
					} else {
						id := uint64(time.Now().UnixNano())
						m.coordinator.NewRequest(id, uint32(OPCODE_NOTIFY_TIMESTAMP), "Stability Timestamp", data)
					}
				})
		}
	}
}

func (m *IndexManager) notifyNewTimestamp(wrapper *timestampWrapper) {

	common.Debugf("IndexManager.notifyNewTimestamp(): receive new timestamp, notifying to listener")
	streamId := common.StreamId(wrapper.StreamId)
	timestamp, err := unmarshallTimestamp(wrapper.Timestamp)
	if err != nil {
		common.Debugf("IndexManager.notifyNewTimestamp(): error when unmarshalling timestamp. Ignore timestamp.  Error=%s", err.Error())
	} else {
		ch, ok := m.timestampCh[streamId]
		if ok {
			if len(ch) < TIMESTAMP_NOTIFY_CH_SIZE {
				ch <- timestamp
			}
		}
	}
}

func (m *IndexManager) getTimer() *Timer {
	return m.timer
}

///////////////////////////////////////////////////////
// package local function
///////////////////////////////////////////////////////

//
// Notify new event
//
func (m *IndexManager) notify(evtType EventType, obj interface{}) {
	m.eventMgr.notify(evtType, obj)
}

func (m *IndexManager) startMasterService() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Initialize the timer.   The timer will be activated by the
	// stream manager when the stream manager opens the stream
	// during initialization.  The stream manager, in turn, is
	// started when the coordinator becomes the master.   There
	// is gorounine in index manager that will listen to the
	// timer and broadcast the stability timestamp to all the
	// listening node.   This goroutine will be started when
	// the indexer node becomes the coordinator master.
	m.timer = newTimer()
	m.timekeeperStopCh = make(chan bool)
	go m.runTimestampKeeper()

	// Initialize the stream manager.
	admin := m.admin
	if admin == nil {
		admin = NewProjectorAdmin(nil, nil)
	}
	handler := NewMgrMutHandler(m, admin)
	var err error
	m.streamMgr, err = NewStreamManager(m, handler, admin)
	if err != nil {
		return err
	}
	m.streamMgr.StartHandlingTopologyChange()
	return nil
}

func (m *IndexManager) stopMasterService() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.stopMasterServiceNoLock()
}

func (m *IndexManager) stopMasterServiceNoLock() {

	if m.streamMgr != nil {
		m.streamMgr.Close()
		m.streamMgr = nil
	}

	if m.timer != nil {
		m.timer.stopAll()
		m.timer = nil

		// use timekeeperStopCh to close the timekeeper gorountime right away
		if m.timekeeperStopCh != nil {
			close(m.timekeeperStopCh)
			m.timekeeperStopCh = nil
		}
	}
}
