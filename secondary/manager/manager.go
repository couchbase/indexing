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
	//"fmt"
	"encoding/json"
	"fmt"
	gometaC "github.com/couchbase/gometa/common"
	gometaL "github.com/couchbase/gometa/log"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/manager/client"
	"os"
	"path/filepath"
	"sync"
	"time"
)

///////////////////////////////////////////////////////
// Type Definition
///////////////////////////////////////////////////////

var USE_MASTER_REPO = false

type IndexManager struct {
	repo          *MetadataRepo
	coordinator   *Coordinator
	eventMgr      *eventManager
	lifecycleMgr  *LifecycleMgr
	requestServer RequestServer
	basepath      string
	quota         uint64
	addrProvider  common.ServiceAddressProvider

	// stream management
	streamMgr *StreamManager
	admin     StreamAdmin

	// timestamp management
	timer                    *Timer
	timestampCh              map[common.StreamId]chan *common.TsVbuuid
	timekeeperStopCh         chan bool
	timestampPersistInterval uint64

	mutex    sync.Mutex
	isClosed bool
}

//
// Index Lifecycle
// 1) Index Creation
//   A) When an index is created, the index definition is assigned to a 64 bits UUID (IndexDefnId).
//   B) IndexManager will persist the index definition.
//   C) IndexManager will persist the index instance with INDEX_STATE_CREATED status.
//      Each instance is assigned a 64 bits IndexInstId. For the first instance of an index,
//      the IndexInstId is equal to the IndexDefnId.
//   D) IndexManager will invovke MetadataNotifier.OnIndexCreate().
//   E) IndexManager will update instance to status INDEX_STATE_READY.
//   F) If there is any error in (1B) - (1E), IndexManager will cleanup by deleting index definition and index instance.
//      Since there is no atomic transaction, cleanup may not be completed, and the index will be left in an invalid state.
//      See (5) for conditions where the index is considered valid.
//   G) If there is any error in (1E), IndexManager will also invoke OnIndexDelete()
//   H) Any error from (1A) or (1F), the error will be reported back to MetadataProvider.
//
// 2) Immediate Index Build (index definition is persisted successfully and deferred build flag is false)
//   A) MetadataNotifier.OnIndexBuild() is invoked.   OnIndexBuild() is responsible for updating the state of the index
//      instance (e.g. from READY to INITIAL).
//   B) If there is an error in (2A), the error will be returned to the MetadataProvider.
//   C) No cleanup will be perfromed by IndexManager if OnIndexBuild() fails.  In other words, the index can be left in
//      INDEX_STATE_READY.   The user should be able to kick off index build again using deferred build.
//   D) OnIndexBuild() can be running on a separate go-rountine.  It can invoke UpdateIndexInstance() at any time during
//      index build.  This update will be queued serially and apply to the topology specific for that index instance (will
//      not affect any other index instance).  The new index state will be returned to the MetadataProvider asynchronously.
//
// 3) Deferred Index Build
//    A) For Deferred Index Build, it will follow step (2A) - (2D).
//
// 4) Index Deletion
//    A) When an index is deleted, IndexManager will set the index to INDEX_STATE_DELETED.
//    B) If (4A) fails, the error will be returned and the index is considered as NOT deleted.
//    C) IndexManager will then invoke MetadataNotifier.OnIndexDelete().
//    D) The IndexManager will delete the index definition first before deleting the index instance.  since there is no atomic
//       transaction, the cleanup may not be completed, and index can be in inconsistent state. See (5) for valid index state.
//    E) Any error returned from (4C) to (4D) will not be returned to the client (since these are cleanup steps)
//
// 5) Valid Index States
//    A) Both index definition and index instance exist.
//    B) Index Instance is not in INDEX_STATE_CREATE or INDEX_STATE_DELETED.
//
type MetadataNotifier interface {
	OnIndexCreate(*common.IndexDefn) error
	OnIndexDelete(common.IndexDefnId, string) error
	OnIndexBuild([]common.IndexDefnId, []string) error
}

type RequestServer interface {
	MakeRequest(opCode gometaC.OpCode, key string, value []byte) error
	MakeAsyncRequest(opCode gometaC.OpCode, key string, value []byte) error
}

///////////////////////////////////////////////////////
// public function
///////////////////////////////////////////////////////

//
// Create a new IndexManager
//
func NewIndexManager(addrProvider common.ServiceAddressProvider, config common.Config) (mgr *IndexManager, err error) {

	return NewIndexManagerInternal(addrProvider, NewProjectorAdmin(nil, nil, nil), config)
}

//
// Create a new IndexManager
//
func NewIndexManagerInternal(
	addrProvider common.ServiceAddressProvider,
	admin StreamAdmin,
	config common.Config) (mgr *IndexManager, err error) {

	gometaL.Current = &logging.SystemLogger

	mgr = new(IndexManager)
	mgr.isClosed = false
	mgr.addrProvider = addrProvider
	totalQuota := config["settings.memory_quota"].Uint64()
	mgr.quota = mgr.calcBufCacheFromMemQuota(totalQuota)

	// stream mgmt  - stream services will start if the indexer node becomes master
	mgr.streamMgr = nil
	mgr.admin = admin

	// timestamp mgmt   - timestamp servcie will start if indexer node becomes master
	mgr.timestampCh = make(map[common.StreamId]chan *common.TsVbuuid)
	mgr.timer = nil
	mgr.timekeeperStopCh = nil

	// Initialize the event manager.  This is non-blocking.  The event manager can be
	// called indirectly by watcher/meta-repo when new metadata changes are sent
	// from gometa master to the indexer node.
	mgr.eventMgr, err = newEventManager()
	if err != nil {
		mgr.Close()
		return nil, err
	}

	// Initialize LifecycleMgr.
	clusterURL := config["clusterAddr"].String()
	mgr.lifecycleMgr = NewLifecycleMgr(addrProvider, nil, clusterURL)

	// Initialize MetadataRepo.  This a blocking call until the
	// the metadataRepo (including watcher) is operational (e.g.
	// finish sync with remote metadata repo master).
	//mgr.repo, err = NewMetadataRepo(requestAddr, leaderAddr, config, mgr)
	mgr.basepath = config["storage_dir"].String()
	os.Mkdir(mgr.basepath, 0755)
	repoName := filepath.Join(mgr.basepath, gometaC.REPOSITORY_NAME)

	adminPort, err := addrProvider.GetLocalServicePort(common.INDEX_ADMIN_SERVICE)
	if err != nil {
		mgr.Close()
		return nil, err
	}

	mgr.repo, mgr.requestServer, err = NewLocalMetadataRepo(adminPort, mgr.eventMgr, mgr.lifecycleMgr, repoName, mgr.quota)
	if err != nil {
		mgr.Close()
		return nil, err
	}

	// start lifecycle manager
	mgr.lifecycleMgr.Run(mgr.repo)

	// register request handler
	clusterAddr := config["clusterAddr"].String()
	registerRequestHandler(mgr, clusterAddr)

	// coordinator
	mgr.coordinator = nil

	return mgr, nil
}

/*
//
// Create a new IndexManager
//
func NewIndexManager(requestAddr string,
    leaderAddr string,
    config string) (mgr *IndexManager, err error) {

    return NewIndexManagerInternal(requestAddr, leaderAddr, config, nil)
}
*/

func (mgr *IndexManager) StartCoordinator(config string) {

	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()

	// Initialize Coordinator.  This is non-blocking.  The coordinator
	// is operational only after it can syncrhonized with the majority
	// of the indexers.   Any request made to the coordinator will be
	// put in a channel for later processing (once leader election is done).
	// Once the coordinator becomes the leader, it will invoke teh stream
	// manager.
	mgr.coordinator = NewCoordinator(mgr.repo, mgr, mgr.basepath)
	go mgr.coordinator.Run(config)
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

	if m.coordinator != nil {
		m.coordinator.Terminate()
	}

	if m.eventMgr != nil {
		m.eventMgr.close()
	}

	if m.lifecycleMgr != nil {
		m.lifecycleMgr.Terminate()
	}

	if m.repo != nil {
		m.repo.Close()
	}

	m.isClosed = true
}

func (m *IndexManager) getServiceAddrProvider() common.ServiceAddressProvider {
	return m.addrProvider
}

///////////////////////////////////////////////////////
// public function - Metadata Operation
///////////////////////////////////////////////////////

func (m *IndexManager) GetMemoryQuota() uint64 {
	return m.quota
}

func (m *IndexManager) RegisterNotifier(notifier MetadataNotifier) {
	m.repo.RegisterNotifier(notifier)
	m.lifecycleMgr.RegisterNotifier(notifier)
}

func (m *IndexManager) SetLocalValue(key string, value string) error {
	return m.repo.SetLocalValue(key, value)
}

func (m *IndexManager) DeleteLocalValue(key string) error {
	return m.repo.DeleteLocalValue(key)
}

func (m *IndexManager) GetLocalValue(key string) (string, error) {
	return m.repo.GetLocalValue(key)
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

	key := fmt.Sprintf("%d", defn.DefnId)
	content, err := common.MarshallIndexDefn(defn)
	if err != nil {
		return err
	}

	if USE_MASTER_REPO {
		if !m.coordinator.NewRequest(uint32(OPCODE_ADD_IDX_DEFN), indexDefnIdStr(defn.DefnId), content) {
			// TODO: double check if it exists in the dictionary
			return NewError(ERROR_MGR_DDL_CREATE_IDX, NORMAL, INDEX_MANAGER, nil,
				fmt.Sprintf("Fail to complete processing create index statement for index '%s'", defn.Name))
		}
	} else {
		return m.requestServer.MakeRequest(client.OPCODE_CREATE_INDEX, key, content)
	}

	return nil
}

func (m *IndexManager) HandleDeleteIndexDDL(defnId common.IndexDefnId) error {

	key := fmt.Sprintf("%d", defnId)

	if USE_MASTER_REPO {

		if !m.coordinator.NewRequest(uint32(OPCODE_DEL_IDX_DEFN), indexDefnIdStr(defnId), nil) {
			// TODO: double check if it exists in the dictionary
			return NewError(ERROR_MGR_DDL_DROP_IDX, NORMAL, INDEX_MANAGER, nil,
				fmt.Sprintf("Fail to complete processing delete index statement for index id = '%d'", defnId))
		}
	} else {
		return m.requestServer.MakeRequest(client.OPCODE_DROP_INDEX, key, []byte(""))
	}

	return nil
}

func (m *IndexManager) UpdateIndexInstance(bucket string, defnId common.IndexDefnId, state common.IndexState,
	streamId common.StreamId, err string, buildTime []uint64) error {

	inst := &topologyChange{
		Bucket:    bucket,
		DefnId:    uint64(defnId),
		State:     uint32(state),
		StreamId:  uint32(streamId),
		Error:     err,
		BuildTime: buildTime}

	buf, e := json.Marshal(&inst)
	if e != nil {
		return e
	}

	// Update index instance is an async operation.  Since indexer may update index instance during
	// callback from MetadataNotifier.  By making async, it avoids deadlock.
	logging.Debugf("IndexManager.UpdateIndexInstance(): making request for Index instance update")
	return m.requestServer.MakeAsyncRequest(client.OPCODE_UPDATE_INDEX_INST, fmt.Sprintf("%v", defnId), buf)
}

func (m *IndexManager) DeleteIndexForBucket(bucket string, streamId common.StreamId) error {

	logging.Debugf("IndexManager.DeleteIndexForBucket(): making request for deleting index for bucket")
	return m.requestServer.MakeRequest(client.OPCODE_DELETE_BUCKET, bucket, []byte{byte(streamId)})
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

	defer logging.Debugf("IndexManager.runTimestampKeeper() : terminate")

	inboundch := m.timer.getOutputChannel()

	persistTimestamp := true // save the first timestamp always
	lastPersistTime := uint64(time.Now().UnixNano())

	timestamps, err := m.repo.GetStabilityTimestamps()
	if err != nil {
		// TODO : Determine timestamp not exist versus forestdb error
		logging.Errorf("IndexManager.runTimestampKeeper() : cannot get stability timestamp from repository. Create a new one.")
		timestamps = createTimestampListSerializable()
	}

	for {
		select {
		case <-m.timekeeperStopCh:
			return

		case timestamp, ok := <-inboundch:

			if !ok {
				return
			}

			gometaC.SafeRun("IndexManager.runTimestampKeeper()",
				func() {
					timestamps.addTimestamp(timestamp)
					persistTimestamp = persistTimestamp ||
						uint64(time.Now().UnixNano())-lastPersistTime > m.timestampPersistInterval
					if persistTimestamp {
						if err := m.repo.SetStabilityTimestamps(timestamps); err != nil {
							logging.Errorf("IndexManager.runTimestampKeeper() : cannot set stability timestamp into repository.")
						} else {
							logging.Debugf("IndexManager.runTimestampKeeper() : saved stability timestamp to repository")
							persistTimestamp = false
							lastPersistTime = uint64(time.Now().UnixNano())
						}
					}

					data, err := marshallTimestampSerializable(timestamp)
					if err != nil {
						logging.Debugf(
							"IndexManager.runTimestampKeeper(): error when marshalling timestamp. Ignore timestamp.  Error=%s",
							err.Error())
					} else {
						m.coordinator.NewRequest(uint32(OPCODE_NOTIFY_TIMESTAMP), "Stability Timestamp", data)
					}
				})
		}
	}
}

func (m *IndexManager) notifyNewTimestamp(wrapper *timestampSerializable) {

	logging.Debugf("IndexManager.notifyNewTimestamp(): receive new timestamp, notifying to listener")
	streamId := common.StreamId(wrapper.StreamId)
	timestamp, err := unmarshallTimestamp(wrapper.Timestamp)
	if err != nil {
		logging.Debugf("IndexManager.notifyNewTimestamp(): error when unmarshalling timestamp. Ignore timestamp.  Error=%s", err.Error())
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
// Get MetadataRepo
// Any caller uses MetadatdaRepo should only for read purpose.
// Writer operation should go through LifecycleMgr
//
func (m *IndexManager) getMetadataRepo() *MetadataRepo {
	return m.repo
}

//
// Get lifecycle manager
//
func (m *IndexManager) getLifecycleMgr() *LifecycleMgr {
	return m.lifecycleMgr
}

//
// Notify new event
//
func (m *IndexManager) notify(evtType EventType, obj interface{}) {
	m.eventMgr.notify(evtType, obj)
}

func (m *IndexManager) startMasterService() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	admin := m.admin
	if admin != nil {
		// Initialize the timer.   The timer will be activated by the
		// stream manager when the stream manager opens the stream
		// during initialization.  The stream manager, in turn, is
		// started when the coordinator becomes the master.   There
		// is gorounine in index manager that will listen to the
		// timer and broadcast the stability timestamp to all the
		// listening node.   This goroutine will be started when
		// the indexer node becomes the coordinator master.
		m.timestampPersistInterval = TIMESTAMP_PERSIST_INTERVAL
		m.timer = newTimer(m.repo)
		m.timekeeperStopCh = make(chan bool)
		go m.runTimestampKeeper()

		monitor := NewStreamMonitor(m, m.timer)

		// Initialize the stream manager.
		admin.Initialize(monitor)

		handler := NewMgrMutHandler(m, admin, monitor)
		var err error
		m.streamMgr, err = NewStreamManager(m, handler, admin, monitor)
		if err != nil {
			return err
		}
		m.streamMgr.StartHandlingTopologyChange()
	}
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

//Calculate forestdb  buffer cache from memory quota
func (m *IndexManager) calcBufCacheFromMemQuota(quota uint64) uint64 {

	//Formula for calculation(see MB-14876)
	//Below 2GB - 256MB to buffercache
	//2GB to 4GB - 40% to buffercache
	//Above 4GB - 60% to buffercache
	if quota <= 2*1024*1024*1024 {
		return 256 * 1024 * 1024
	} else if quota <= 4*1024*1024*1024 {
		return uint64(0.4 * float64(quota))
	} else {
		return uint64(0.6 * float64(quota))
	}

}

///////////////////////////////////////////////////////
// public function - for testing only
///////////////////////////////////////////////////////

func (m *IndexManager) GetStabilityTimestampForVb(streamId common.StreamId, bucket string, vb uint16) (uint64, bool) {

	logging.Debugf("IndexManager.GetStabilityTimestampForVb() : get stability timestamp from repo")
	savedTimestamps, err := m.repo.GetStabilityTimestamps()
	if err == nil {
		seqno, _, ok, err := savedTimestamps.findTimestamp(streamId, bucket, vb)
		if ok && err == nil {
			return seqno, true
		}
	} else {
		logging.Errorf("IndexManager.GetStabilityTimestampForVb() : cannot get stability timestamp from repository.")
	}

	return 0, false
}

func (m *IndexManager) SetTimestampPersistenceInterval(elapsed uint64) {
	m.timestampPersistInterval = elapsed
}

func (m *IndexManager) CleanupTopology() {

	globalTop, err := m.GetGlobalTopology()
	if err != nil {
		logging.Errorf("IndexManager.CleanupTopology() : error %v.", err)
		return
	}

	for _, key := range globalTop.TopologyKeys {
		if err := m.repo.deleteMeta(key); err != nil {
			logging.Errorf("IndexManager.CleanupTopology() : error %v.", err)
		}
	}

	key := globalTopologyKey()
	if err := m.repo.deleteMeta(key); err != nil {
		logging.Errorf("IndexManager.CleanupTopology() : error %v.", err)
	}
}

func (m *IndexManager) CleanupStabilityTimestamp() {

	m.stopMasterServiceNoLock()

	key := stabilityTimestampKey()
	if err := m.repo.deleteMeta(key); err != nil {
		logging.Errorf("IndexManager.CleanupStabilityTimestamp() : error %v.", err)
	}
}
