// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package manager

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	c "github.com/couchbase/gometa/common"
	"github.com/couchbase/gometa/message"
	"github.com/couchbase/gometa/protocol"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/common/collections"
	fdb "github.com/couchbase/indexing/secondary/fdb"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/manager/client"
	mc "github.com/couchbase/indexing/secondary/manager/common"
	//"runtime/debug"
)

//////////////////////////////////////////////////////////////
// concrete type/struct
//////////////////////////////////////////////////////////////

// LifecycleMgr is a singleton created as a child of the IndexManager singleton (manager.go).
// Singleton chain: Indexer -> ClustMgrAgent -> IndexManager -> LifecycleMgr
type LifecycleMgr struct {
	repo        *MetadataRepo
	cinfoClient *common.ClusterInfoClient
	notifier    MetadataNotifier // object to call Indexer to perform index DDL
	clusterURL  string

	// Request queues from and to gometa intermediary layer
	incomings  chan *requestHolder // normal priority DDL requests from clients
	expedites  chan *requestHolder // high priority DDL-related requests from clients
	bootstraps chan *requestHolder // bootstrap-related requests from clients
	parallels  chan *requestHolder // lightweight requests that can run in parallel with expediates & incomings safely
	outgoings  chan c.Packet       // responses back to clients

	killch           chan bool
	indexerReady     bool
	builder          *builder
	janitor          *janitor
	updator          *updator
	requestServer    RequestServer
	prepareLock      *client.PrepareCreateRequest
	stats            StatsHolder
	done             sync.WaitGroup
	isDone           bool
	collAwareCluster uint32 // 0: false, 1: true

	clientStatsRefreshInterval uint64

	lastSendClientStats *client.IndexStats2 // always full stats; send will filter out index list if unchanged
	clientStatsMutex    sync.Mutex          // for lastSendClientStats
	acceptedNames       map[string]*indexNameRequest
	accIgnoredIds       map[common.IndexDefnId]bool
}

type requestHolder struct {
	request protocol.RequestMsg
	fid     string
}

type StatsHolder struct {
	ptr unsafe.Pointer
}

func (h *StatsHolder) Get() *common.Statistics {
	return (*common.Statistics)(atomic.LoadPointer(&h.ptr))
}

func (h *StatsHolder) Set(s *common.Statistics) {
	atomic.StorePointer(&h.ptr, unsafe.Pointer(s))
}

type topologyChange struct {
	Bucket      string   `json:"bucket,omitempty"`
	Scope       string   `json:"scope,omitempty"`
	Collection  string   `json:"collection,omitempty"`
	DefnId      uint64   `json:"defnId,omitempty"`
	InstId      uint64   `json:"instId,omitempty"`
	State       uint32   `json:"state,omitempty"`
	StreamId    uint32   `json:"steamId,omitempty"`
	Error       string   `json:"error,omitempty"`
	BuildTime   []uint64 `json:"buildTime,omitempty"`
	RState      uint32   `json:"rState,omitempty"`
	Partitions  []uint64 `json:"partitions,omitempty"`
	Versions    []int    `json:"versions,omitempty"`
	InstVersion int      `json:"instVersion,omitempty"`
}

type dropInstance struct {
	Defn             common.IndexDefn `json:"defn,omitempty"`
	Notify           bool             `json:"notify,omitempty"`
	UpdateStatusOnly bool             `json:"updateStatus,omitempty"`
	DeletedOnly      bool             `json:"deletedOnly,omitempty"`
}

type mergePartition struct {
	DefnId         uint64   `json:"defnId,omitempty"`
	SrcInstId      uint64   `json:"srcInstId,omitempty"`
	SrcRState      uint64   `json:"srcRState,omitempty"`
	TgtInstId      uint64   `json:"tgtInstId,omitempty"`
	TgtPartitions  []uint64 `json:"tgtPartitions,omitempty"`
	TgtVersions    []int    `json:"tgtVersions,omitempty"`
	TgtInstVersion uint64   `json:"tgtInstVersion,omitempty"`
}

type builder struct {
	manager   *LifecycleMgr
	pendings  map[string][]uint64    // map of "bucket/scope/collection" to list of index defnIds pending build
	notifych  chan *common.IndexDefn // incoming requests to build indexes
	batchSize int32                  // max number of indexes to build per iteration of builder.run; <= 0 means unlimited
	disable   int32

	commandListener *mc.CommandListener
	listenerDonech  chan bool
}

type janitor struct {
	manager *LifecycleMgr

	commandListener  *mc.CommandListener
	listenerDonech   chan bool
	runch            chan bool
	deleteTokenCache map[common.IndexDefnId]int64 // unixnano timestamp
}

const DELETE_TOKEN_DELAYED_CLEANUP_INTERVAL = 24 * time.Hour

type updator struct {
	manager        *LifecycleMgr
	indexerVersion uint64
	serverGroup    string
	nodeAddr       string
	clusterVersion uint64
	excludeNode    string
	storageMode    uint64
}

//////////////////////////////////////////////////////////////
// Lifecycle Mgr - event processing
//////////////////////////////////////////////////////////////

// NewLifecycleMgr constructs a new LifecycleMgr object. The Indexer creates a singleton
// ClustMgrAgent which owns a singleton LifecycleMgr as a child.
func NewLifecycleMgr(clusterURL string) (*LifecycleMgr, error) {

	var err error

	mgr := &LifecycleMgr{
		repo:                       nil,
		notifier:                   nil, // set later via RegisterNotifier API
		clusterURL:                 clusterURL,
		incomings:                  make(chan *requestHolder, 100000),
		expedites:                  make(chan *requestHolder, 100000),
		parallels:                  make(chan *requestHolder, 100000),
		outgoings:                  make(chan c.Packet, 100000),
		killch:                     make(chan bool),
		bootstraps:                 make(chan *requestHolder, 1000),
		indexerReady:               false,
		lastSendClientStats:        &client.IndexStats2{},
		clientStatsRefreshInterval: 5000,
		acceptedNames:              make(map[string]*indexNameRequest),
		accIgnoredIds:              make(map[common.IndexDefnId]bool),
	}

	mgr.cinfoClient, err = common.NewClusterInfoClient(clusterURL, common.DEFAULT_POOL, nil)
	if err != nil {
		return nil, err
	}
	mgr.cinfoClient.SetUserAgent("LifecycleMgr")

	cinfo := mgr.cinfoClient.GetClusterInfoCache()
	if cinfo.GetClusterVersion() >= common.INDEXER_70_VERSION {
		atomic.StoreUint32(&mgr.collAwareCluster, 1)
	}
	mgr.builder = newBuilder(mgr)
	mgr.janitor = newJanitor(mgr)
	mgr.updator = newUpdator(mgr)

	return mgr, nil
}

func (m *LifecycleMgr) Run(repo *MetadataRepo, requestServer RequestServer) {
	m.repo = repo
	m.requestServer = requestServer
	go m.processRequest()
	go m.broadcastStats()
}

func (m *LifecycleMgr) RegisterNotifier(notifier MetadataNotifier) {
	m.notifier = notifier
}

func (m *LifecycleMgr) Terminate() {
	if !m.isDone {
		m.isDone = true
		close(m.killch)
		m.done.Wait()
	}
}

///////////////////////////////////////////////////////////////////////////////
// Gometa CustomRequestHandler interface methods (in: OnNewRequest,
// out: GetResponseChannel). These form the gometa communication bridge between
// IndexManager (manager.go) and LifecycleMgr (lifecycle.go).
///////////////////////////////////////////////////////////////////////////////

// OnNewRequest implements a gometa CustomRequestHandler interface method.
// It is the input side of the gometa bridge between IndexManager and LifecycleMgr
// and will be invoked for each new request.
//
// This is the main event processing loop.  It is important not to having any blocking
// call in this function (e.g. mutex).  If this function is blocked, it will also
// block gometa event processing loop.
func (m *LifecycleMgr) OnNewRequest(fid string, request protocol.RequestMsg) {

	req := &requestHolder{request: request, fid: fid}
	op := c.OpCode(request.GetOpCode())

	logging.Debugf("LifecycleMgr.OnNewRequest(): queuing new request. reqId %v opCode %v", request.GetReqId(), op)

	if op == client.OPCODE_INDEXER_READY {
		// Indexer bootstrap is done
		m.indexerReady = true

		// check for cleanup periodically in case cleanup fails or
		// catching up with delete token from metakv
		go m.janitor.run()

		// allow background build to go through
		go m.builder.run()

		// detect if there is any change to indexer info (e.g. serverGroup)
		go m.updator.run()

		// allows requests processing to start for parallels requests
		go m.processParallelsRequests()

		// allow request processing to go through
		close(m.bootstraps)

	} else if op == client.OPCODE_SERVICE_MAP {
		// short cut the connection request by spawn its own go-routine
		// This call does not change the state of the repository, so it
		// is OK to shortcut.
		go m.dispatchRequest(req, message.NewConcreteMsgFactory())

	} else {
		// if indexer is not yet ready, put them in the bootstrap queue so
		// they can get processed.  For client side, they will be queued
		// up in the regular queue until indexer is ready.
		if !m.indexerReady {
			if op == client.OPCODE_UPDATE_INDEX_INST ||
				op == client.OPCODE_DELETE_BUCKET ||
				op == client.OPCODE_DELETE_COLLECTION ||
				op == client.OPCODE_CLEANUP_INDEX ||
				op == client.OPCODE_CLEANUP_PARTITION ||
				op == client.OPCODE_RESET_INDEX ||
				op == client.OPCODE_RESET_INDEX_ON_ROLLBACK ||
				op == client.OPCODE_BROADCAST_STATS ||
				op == client.OPCODE_REBALANCE_RUNNING {
				m.bootstraps <- req
				return
			}
		}

		if op == client.OPCODE_CONFIG_UPDATE ||
			op == client.OPCODE_GET_REPLICA_COUNT ||
			op == client.OPCODE_CHECK_TOKEN_EXIST ||
			op == client.OPCODE_CLIENT_STATS ||
			op == client.OPCODE_REBALANCE_RUNNING ||
			op == client.OPCODE_BROADCAST_STATS {
			m.parallels <- req

		} else if op == client.OPCODE_UPDATE_INDEX_INST ||
			op == client.OPCODE_DROP_OR_PRUNE_INSTANCE ||
			op == client.OPCODE_MERGE_PARTITION ||
			op == client.OPCODE_PREPARE_CREATE_INDEX ||
			op == client.OPCODE_COMMIT_CREATE_INDEX {
			m.expedites <- req

		} else {
			// for create/drop/build index, always go to the client queue -- which will wait for
			// indexer to be ready.
			m.incomings <- req
		}
	}
}

// GetResponseChannel implements a gometa CustomRequestHandler interface method.
// It is the output side of the gometa bridge between IndexManager and LifecycleMgr.
func (m *LifecycleMgr) GetResponseChannel() <-chan c.Packet {
	return (<-chan c.Packet)(m.outgoings)
}

///////////////////////////////////////////////////////////////////////////////
// Regular methods
///////////////////////////////////////////////////////////////////////////////

func (m *LifecycleMgr) processRequest() {

	logging.Debugf("LifecycleMgr.processRequest(): LifecycleMgr is ready to proces request")
	factory := message.NewConcreteMsgFactory()

	m.done.Add(1)
	defer m.done.Done()

	// Process any requests from the boostrap phase. Once indexer is ready, this channel
	// will be closed, and this go-routine will proceed to process regular message.
END_BOOTSTRAP:
	for {
		select {
		case request, ok := <-m.bootstraps:
			if ok {
				m.dispatchRequest(request, factory)
			} else {
				logging.Debugf("LifecycleMgr.handleRequest(): closing bootstrap channel")
				break END_BOOTSTRAP
			}
		case <-m.killch:
			// server shutdown
			logging.Infof("LifecycleMgr.processRequest(): receive kill signal. Stop boostrap request processing.")
			return
		}
	}

	logging.Debugf("LifecycleMgr.processRequest(): indexer is ready to process new client request.")

	// dispatchExpdites is a processRequest helper that drains the expedites queue before returning.
	// Returns true unless its channel closed; then it returns false so caller will shut down.
	dispatchExpedites := func() bool {
		for {
			select {
			case request, ok := <-m.expedites:
				if ok {
					// TOOD: deal with error
					m.dispatchRequest(request, factory)
				} else {
					// server shutdown.
					logging.Infof("LifecycleMgr.handleRequest(): channel for receiving client request is closed. Terminate.")
					return false
				}
			default:
				return true
			}
		}
	}

	// Indexer is ready and all bootstrap requests are processed.  Proceed to handle regular messages.
	for {
		select {
		case request, ok := <-m.expedites:
			if ok {
				// TOOD: deal with error
				m.dispatchRequest(request, factory)
			} else {
				// server shutdown.
				logging.Infof("LifecycleMgr.handleRequest(): channel for receiving client request is closed. Terminate.")
				return
			}
		case request, ok := <-m.incomings:
			if ok {
				if dispatchExpedites() {
					// TOOD: deal with error
					m.dispatchRequest(request, factory)
				} else {
					return
				}
			} else {
				// server shutdown.
				logging.Infof("LifecycleMgr.handleRequest(): channel for receiving client request is closed. Terminate.")
				return
			}
		case <-m.killch:
			// server shutdown
			logging.Infof("LifecycleMgr.processRequest(): receive kill signal. Stop Client request processing.")
			return
		}
	}
}

func (m *LifecycleMgr) processParallelsRequests() {

	logging.Debugf("LifecycleMgr.processParallelsRequests(): indexer is ready to process client request on parallels channel.")
	factory := message.NewConcreteMsgFactory()

	m.done.Add(1)
	defer m.done.Done()

	for {
		select {
		case request, ok := <-m.parallels:
			if ok {
				// TOOD: deal with error
				m.dispatchRequest(request, factory)
			} else {
				// server shutdown.
				logging.Infof("LifecycleMgr.processParallelsRequests(): channel for receiving client request is closed. Terminate.")
				return
			}
		case <-m.killch:
			// server shutdown
			logging.Infof("LifecycleMgr.processParallelsRequests(): receive kill signal. Stop Client request processing.")
			return
		}
	}
}

// dispatchRequest *synchronously* executes requests from the bootstrap, expidites, and incomings
// queues of client requests, as well as the special case OPCODE_SERVICE_MAP operation for which
// this function is called in its own go routine and thus effectively runs asynchronously. It then
// constructs a response message (which may contain an error message) and queues it to outgoings channel.
func (m *LifecycleMgr) dispatchRequest(request *requestHolder, factory *message.ConcreteMsgFactory) {

	reqId := request.request.GetReqId()
	op := c.OpCode(request.request.GetOpCode())
	key := request.request.GetKey()
	content := request.request.GetContent()
	fid := request.fid

	logging.Debugf("LifecycleMgr.dispatchRequest () : requestId %d, op %d, key %v", reqId, op, key)

	var err error = nil
	var result []byte = nil

	start := time.Now()
	defer func() {
		if op != client.OPCODE_BROADCAST_STATS {
			logging.Infof("lifecycleMgr.dispatchRequest: op %v elapsed %v len(expediates) %v len(incomings) %v len(outgoings) %v len(parallels) %v error %v",
				client.Op2String(op), time.Now().Sub(start), len(m.expedites), len(m.incomings), len(m.outgoings), len(m.parallels), err)
		}
	}()

	switch op {
	case client.OPCODE_CREATE_INDEX:
		err = m.handleCreateIndexScheduledBuild(key, content, common.NewUserRequestContext())
	case client.OPCODE_UPDATE_INDEX_INST:
		err = m.handleTopologyChange(content)
	case client.OPCODE_DROP_INDEX:
		err = m.handleDeleteIndex(key, common.NewUserRequestContext())
	case client.OPCODE_BUILD_INDEX:
		err = m.handleBuildIndexes(content, common.NewUserRequestContext(), false)
	case client.OPCODE_SERVICE_MAP:
		result, err = m.handleServiceMap(content)
	case client.OPCODE_DELETE_BUCKET:
		err = m.handleDeleteBucket(key, content)
	case client.OPCODE_DELETE_COLLECTION:
		err = m.handleDeleteCollection(key, content)
	case client.OPCODE_CLEANUP_INDEX:
		err = m.handleCleanupIndexMetadata(content)
	case client.OPCODE_INVALID_COLLECTION:
		err = m.handleCleanupIndexFromInvalidKeyspace(key)
	case client.OPCODE_CREATE_INDEX_REBAL:
		err = m.handleCreateIndexScheduledBuild(key, content, common.NewRebalanceRequestContext())
	case client.OPCODE_BUILD_INDEX_REBAL:
		err = m.handleBuildIndexes(content, common.NewRebalanceRequestContext(), true)
	case client.OPCODE_DROP_INDEX_REBAL:
		err = m.handleDeleteIndex(key, common.NewRebalanceRequestContext())
	case client.OPCODE_BUILD_INDEX_RETRY:
		err = m.handleBuildIndexes(content, common.NewUserRequestContext(), false)
	case client.OPCODE_BROADCAST_STATS:
		m.handleNotifyStats(content)
	case client.OPCODE_RESET_INDEX:
		err = m.handleResetIndex(content)
	case client.OPCODE_RESET_INDEX_ON_ROLLBACK:
		err = m.handleResetIndexOnRollback(content)
	case client.OPCODE_CONFIG_UPDATE:
		err = m.handleConfigUpdate(content)
	case client.OPCODE_DROP_OR_PRUNE_INSTANCE:
		err = m.handleDeleteOrPruneIndexInstance(content, common.NewRebalanceRequestContext())
	case client.OPCODE_DROP_OR_PRUNE_INSTANCE_DDL:
		err = m.handleDeleteOrPruneIndexInstance(content, common.NewUserRequestContext())
	case client.OPCODE_CLEANUP_PARTITION:
		err = m.handleDeleteOrPruneIndexInstance(content, common.NewUserRequestContext())
	case client.OPCODE_MERGE_PARTITION:
		err = m.handleMergePartition(content, common.NewRebalanceRequestContext())
	case client.OPCODE_PREPARE_CREATE_INDEX:
		result, err = m.handlePrepareCreateIndex(content)
	case client.OPCODE_COMMIT_CREATE_INDEX:
		result, err = m.handleCommit(content)
	case client.OPCODE_REBALANCE_RUNNING:
		err = m.handleRebalanceRunning(content)
	case client.OPCODE_CREATE_INDEX_DEFER_BUILD:
		err = m.handleCreateIndexDeferBuild(key, content, common.NewUserRequestContext())
	case client.OPCODE_DROP_INSTANCE:
		err = m.handleDropInstance(content, common.NewUserRequestContext())
	case client.OPCODE_UPDATE_REPLICA_COUNT:
		err = m.handleUpdateReplicaCount(content)
	case client.OPCODE_GET_REPLICA_COUNT:
		result, err = m.handleGetIndexReplicaCount(content)
	case client.OPCODE_CHECK_TOKEN_EXIST:
		result, err = m.handleCheckTokenExist(content)
	case client.OPCODE_CLIENT_STATS:
		result, err = m.handleClientStats(content)
	}

	logging.Debugf("LifecycleMgr.dispatchRequest () : send response for requestId %d, op %d, len(result) %d", reqId, op, len(result))

	if fid == "internal" {
		return
	}

	if err == nil {
		msg := factory.CreateResponse(fid, reqId, "", result)
		m.outgoings <- msg
	} else {
		msg := factory.CreateResponse(fid, reqId, err.Error(), result)
		m.outgoings <- msg
	}

}

//////////////////////////////////////////////////////////////
// Lifecycle Mgr - handler functions
//////////////////////////////////////////////////////////////

//-----------------------------------------------------------
// Atomic Create Index
//-----------------------------------------------------------

//
// Prepare create index
//
func (m *LifecycleMgr) handlePrepareCreateIndex(content []byte) ([]byte, error) {

	prepareCreateIndex, err := client.UnmarshallPrepareCreateRequest(content)
	if err != nil {
		logging.Errorf("LifecycleMgr.handlePrepareCreateIndex() : prepareCreateIndex fails. Unable to unmarshall request. Reason = %v", err)
		return nil, err
	}

	prepareCreateIndex.Scope, prepareCreateIndex.Collection = common.GetCollectionDefaults(prepareCreateIndex.Scope,
		prepareCreateIndex.Collection)

	// Check for duplicate index before proceeding further.
	if prepareCreateIndex.Op == client.PREPARE {
		exists, err := m.checkDuplicateIndex(prepareCreateIndex)
		if err != nil {
			logging.Infof("LifecycleMgr.handlePrepareCreateIndex() : Reject "+
				"%v because of error (%v) in GetIndexDefnByName", prepareCreateIndex.DefnId, err)

			response := &client.PrepareCreateResponse{
				Accept: false,
				Msg:    client.RespUnexpectedError,
			}
			return client.MarshallPrepareCreateResponse(response)
		}

		if exists {
			logging.Infof("LifecycleMgr.handlePrepareCreateIndex() : Reject "+
				"%v because of duplicate index name.", prepareCreateIndex.DefnId)

			response := &client.PrepareCreateResponse{
				Accept: false,
				Msg:    client.RespDuplicateIndex,
			}
			return client.MarshallPrepareCreateResponse(response)
		}
	}

	if prepareCreateIndex.Op == client.PREPARE {
		if _, err := m.repo.GetLocalValue("RebalanceRunning"); err == nil {
			logging.Infof("LifecycleMgr.handlePrepareCreateIndex() : Reject %v because rebalance in progress", prepareCreateIndex.DefnId)
			response := &client.PrepareCreateResponse{
				Accept: false,
				Msg:    client.RespRebalanceRunning,
			}
			return client.MarshallPrepareCreateResponse(response)
		}

		if m.prepareLock != nil {
			if m.prepareLock.RequesterId != prepareCreateIndex.RequesterId ||
				m.prepareLock.DefnId != prepareCreateIndex.DefnId {

				if !m.isHigherPriorityRequest(prepareCreateIndex) {
					logging.Infof("LifecycleMgr.handlePrepareCreateIndex() : Reject %v because another index %v holding lock",
						prepareCreateIndex.DefnId, m.prepareLock.DefnId)
					response := &client.PrepareCreateResponse{
						Accept: false,
						Msg:    client.RespAnotherIndexCreation,
					}
					return client.MarshallPrepareCreateResponse(response)
				}
			}
		}

		m.prepareLock = prepareCreateIndex
		m.prepareLock.StartTime = time.Now().UnixNano()

		logging.Infof("LifecycleMgr.handlePrepareCreateIndex() : Accept %v", m.prepareLock.DefnId)
		response := &client.PrepareCreateResponse{Accept: true}
		return client.MarshallPrepareCreateResponse(response)

	} else if prepareCreateIndex.Op == client.CANCEL_PREPARE {
		if m.prepareLock != nil {
			if m.prepareLock.RequesterId == prepareCreateIndex.RequesterId &&
				m.prepareLock.DefnId == prepareCreateIndex.DefnId {
				logging.Infof("LifecycleMgr.handlePrepareCreateIndex() : release lock for %v", m.prepareLock.DefnId)
				m.prepareLock = nil
			}
		}
		return nil, nil
	}

	return nil, fmt.Errorf("Unknown operation %v for prepare create index", prepareCreateIndex.Op)
}

//
// Following function takes a prepare create request as input and returns
// a boolean value base on the priority of the current in-progress request
// and the new incoming request. Returns true if the new incoming request
// has higher priority than the current in-progress request.
//
func (m *LifecycleMgr) isHigherPriorityRequest(req *client.PrepareCreateRequest) bool {
	if m.prepareLock == nil {
		return true
	}

	// Don't look at the actual request priorities if the current in-progress
	// request has timed out.
	// TODO: Need to increase timeout as planner can take more time with large
	//       number of indexes.
	if m.prepareLock.Timeout < (time.Now().UnixNano() - m.prepareLock.StartTime) {
		logging.Infof("LifecycleMgr.handlePrepareCreateIndex() : Prepare timeout for %v", m.prepareLock.DefnId)
		return true
	}

	if m.prepareLock.Ctime == 0 {
		logging.Debugf("LifecycleMgr.handlePrepareCreateIndex() : Rejecting request for %v:%v"+
			" as the ongoing request (%v, %v) has equal or higher priority.", req.DefnId, req.Ctime,
			m.prepareLock.DefnId, m.prepareLock.Ctime)
		return false
	}

	if req.Ctime == 0 {
		logging.Infof("LifecycleMgr.handlePrepareCreateIndex() : Prioritising  request %v:%v"+
			" over the ongoing request (%v, %v) as it has equal or higher priority.", req.DefnId, req.Ctime,
			m.prepareLock.DefnId, m.prepareLock.Ctime)
		return true
	}

	if m.prepareLock.Ctime > req.Ctime {
		logging.Infof("LifecycleMgr.handlePrepareCreateIndex() : Prioritising  request %v:%v"+
			" over the ongoing request (%v, %v) as it has equal or higher priority.", req.DefnId, req.Ctime,
			m.prepareLock.DefnId, m.prepareLock.Ctime)
		return true
	}

	logging.Debugf("LifecycleMgr.handlePrepareCreateIndex() : Rejecting request for %v:%v"+
		" as the ongoing request (%v, %v) has equal or higher priority.", req.DefnId, req.Ctime,
		m.prepareLock.DefnId, m.prepareLock.Ctime)
	return false
}

//
// handle Commit operation
//
func (m *LifecycleMgr) handleCommit(content []byte) ([]byte, error) {

	commit, err := client.UnmarshallCommitCreateRequest(content)
	if err != nil {
		logging.Errorf("LifecycleMgr.handleCommit() : Unable to unmarshall request. Reason = %v", err)
		return nil, err
	}

	for _, definitions := range commit.Definitions {
		for i, _ := range definitions {
			definitions[i].SetCollectionDefaults()
		}
	}

	if commit.Op == client.NEW_INDEX {
		return m.handleCommitCreateIndex(commit)
	} else if commit.Op == client.ADD_REPLICA {
		return m.handleCommitAddReplica(commit)
	} else if commit.Op == client.DROP_REPLICA {
		return m.handleCommitDropReplica(commit)
	}

	return nil, fmt.Errorf("Unknown operation %v", commit.Op)
}

//
// Commit create index
//
func (m *LifecycleMgr) handleCommitCreateIndex(commitCreateIndex *client.CommitCreateRequest) ([]byte, error) {

	if m.prepareLock == nil {
		logging.Infof("LifecycleMgr.handleCommitCreateIndex() : Reject %v because there is no lock", commitCreateIndex.DefnId)
		response := &client.CommitCreateResponse{Accept: false}
		return client.MarshallCommitCreateResponse(response)
	}

	if m.prepareLock.RequesterId != commitCreateIndex.RequesterId ||
		m.prepareLock.DefnId != commitCreateIndex.DefnId {

		logging.Infof("LifecycleMgr.handleCommitCreateIndex() : Reject %v because defnId and requesterId do not match", commitCreateIndex.DefnId)
		response := &client.CommitCreateResponse{Accept: false}
		return client.MarshallCommitCreateResponse(response)
	}

	if _, err := m.repo.GetLocalValue("RebalanceRunning"); err == nil {
		logging.Infof("LifecycleMgr.handleCommitCreateIndex() : Reject %v because rebalance in progress", commitCreateIndex.DefnId)
		response := &client.PrepareCreateResponse{Accept: false}
		return client.MarshallPrepareCreateResponse(response)
	}

	// Verify and release the indexNames lock.
	if msg := m.verifyDuplicateIndexCommit(commitCreateIndex); msg != "" {
		logging.Infof("LifecycleMgr.handleCommitCreateIndex() : Reject %v because %v", commitCreateIndex.DefnId, msg)
		response := &client.CommitCreateResponse{Accept: false}
		return client.MarshallCommitCreateResponse(response)
	}

	defnId := commitCreateIndex.DefnId
	definitions := commitCreateIndex.Definitions
	m.prepareLock = nil
	asyncCreate := commitCreateIndex.AsyncCreate

	commit, bucketUUID, scopeId, collectionId, err := m.processCommitToken(defnId, definitions, asyncCreate)
	if commit {
		// If fails to post the command token, the return failure.  If none of the indexer can post the command token,
		// the command token will be malformed and it will get cleaned up by DDLServiceMgr upon rebalancing.
		if err1 := mc.PostCreateCommandToken(defnId, bucketUUID, scopeId, collectionId, 0, definitions); err1 != nil {
			logging.Infof("LifecycleMgr.handleCommitCreateIndex() : Reject %v because fail to post token", commitCreateIndex.DefnId)

			if err == nil {
				err = fmt.Errorf("Create Index fails.  Cause: %v", err1)
			}

			m.DeleteIndex(defnId, true, false, common.NewUserRequestContext())

			response := &client.CommitCreateResponse{Accept: false}
			msg, _ := client.MarshallCommitCreateResponse(response)
			return msg, err
		}

		logging.Infof("LifecycleMgr.handleCommitCreateIndex() : Create token posted for %v", defnId)
		response := &client.CommitCreateResponse{Accept: true}
		msg, err1 := client.MarshallCommitCreateResponse(response)
		if err1 != nil {
			m.DeleteIndex(defnId, true, false, common.NewUserRequestContext())
			if err == nil {
				err = err1
			}
		}

		return msg, err
	}

	logging.Infof("LifecycleMgr.handleCommitCreateIndex() : Create token posted for %v", defnId)
	response := &client.CommitCreateResponse{Accept: false}
	msg, _ := client.MarshallCommitCreateResponse(response)
	return msg, err
}

//
// Check for duplicate index name; returns true if duplicate index exists
//
// This function checks if an index with same name/keyspace is either
// 1. already created and corresponding entry exists in the metadata repo OR
// 2. accepted for creation.
//
// This function acquires the indexNames lock if it isn't alreday acquired by
// some other request. The indexNames lock will be released only when the index
// definition is being committed.
//
// TODO:
// As the indexNames lock is released only during commit, in case of scheduled
// creation + drop before create, the lock won't be overwritten until the timeout.
// So, user needs to wait until the timeout to create index with same name on
// same keyspace. This can be avoided if during checkDuplicateIndex below,
// the metakv tokens are also checked - assuming the consistent metakv is
// not far away in the future. But checking metakv tokens here is an costly
// operation and will holdup lifecycle manager for a long time.
//
// In case of lock timeout, the new request will forcefully hold the lock and
// the older request will fail to release the lock during commit phase.
//
func (m *LifecycleMgr) checkDuplicateIndex(req *client.PrepareCreateRequest) (exists bool, err error) {

	key := fmt.Sprintf("%v:%v:%v:%v", req.Bucket, req.Scope, req.Collection, req.Name)

	acquire := false

	defer func() {
		if acquire && !exists {
			// Acquire indexNames lock
			startTime := time.Now().UnixNano()
			ireq := &indexNameRequest{
				startTime: startTime,
				defnId:    req.DefnId,
				timeout:   req.Timeout,
			}
			m.acceptedNames[key] = ireq
		}
	}()

	if req.Op != client.PREPARE {
		return false, nil
	}

	// Check for duplicate index name only if name and bucket name
	// are specified in the request
	if req.Name == "" || req.Bucket == "" {
		m.accIgnoredIds[req.DefnId] = true
		return exists, nil
	}

	// Check for the index with same name and same keyspace in meta repo.
	var existDefn *common.IndexDefn
	existDefn, err = m.repo.GetIndexDefnByName(req.Bucket, req.Scope, req.Collection, req.Name)
	if err != nil {
		return exists, err
	}

	if existDefn != nil {
		exists = true
		return exists, nil
	}

	// Check for an index name and keyspace in the list of already accepted reqs
	accReq, ok := m.acceptedNames[key]
	if ok {
		// If it is the same index (same DefnId), then treat it as it is not a duplicate.
		if accReq.defnId == req.DefnId {
			return exists, nil
		}

		// Check for lock timeout
		if accReq.timeout < (time.Now().UnixNano() - accReq.startTime) {
			acquire = true
			logging.Infof("LifecycleMgr.handlePrepareCreateIndex() : Index name conflict timeout."+
				"Prioritizing request %v with %v.", accReq.defnId, req.DefnId)
			return exists, nil
		}

		exists = true
	}

	acquire = true

	return exists, nil
}

func (m *LifecycleMgr) verifyDuplicateIndexCommit(commitCreateIndex *client.CommitCreateRequest) string {

	key := ""

	var defnId common.IndexDefnId

loop:
	for _, defns := range commitCreateIndex.Definitions {
		// Assuming that the index name and keyspace doesn't change across definitions.
		for _, defn := range defns {
			defnId = defn.DefnId
			key = fmt.Sprintf("%v:%v:%v:%v", defn.Bucket, defn.Scope, defn.Collection, defn.Name)
			break loop
		}
	}

	if key == "" {
		return "of missing index definitions in commit request."
	}

	if _, ok := m.accIgnoredIds[defnId]; ok {
		delete(m.accIgnoredIds, defnId)
		return ""
	}

	accReq, ok := m.acceptedNames[key]
	if ok {
		if accReq.defnId == commitCreateIndex.DefnId {
			// Release the lock
			delete(m.acceptedNames, key)
			return ""
		}
	}

	return fmt.Sprintf("the index name lock is not acquired by defnId %v", commitCreateIndex.DefnId)
}

//
// Notify rebalance running
//
func (m *LifecycleMgr) handleRebalanceRunning(content []byte) error {

	if m.prepareLock != nil {
		logging.Infof("LifecycleMgr.handleRebalanceRunning() : releasing token %v", m.prepareLock.DefnId)
	}

	m.prepareLock = nil
	return nil
}

//
// Process commit token
//
func (m *LifecycleMgr) processCommitToken(defnId common.IndexDefnId,
	layout map[common.IndexerId][]common.IndexDefn, asyncCreate bool) (bool, string, string, string, error) {

	indexerId, err := m.repo.GetLocalIndexerId()
	if err != nil {
		return false, "", "", "", fmt.Errorf("Create Index fails.  Internal Error: %v", err)
	}

	if definitions, ok := layout[indexerId]; ok && len(definitions) > 0 {

		defn := definitions[0]
		reqCtx := common.NewUserRequestContext()

		// Create the index instance.   This is to ensure that definiton passes invariant validation (e.g bucket exists).
		// If create index fails due to transient error (indexer in recovery), a token will be placed to retry the operation
		// later.  The definiton is always deleted upon error.
		if err := m.CreateIndexOrInstance(&defn, false, reqCtx, asyncCreate); err != nil {
			// If there is error, the defintion will not be created.
			// But if it is recoverable error, then we still want to create the commit token.
			logging.Errorf("LifecycleMgr.processCommitToken() : create index fails.  Reason = %v", err)

			// canRetryCreateError logic may not be future safe as it defaults to true.
			// So, ensure that the token will be posted only when the valid keyspace ids
			// are available.
			commit := true
			if defn.BucketUUID == common.BUCKET_UUID_NIL ||
				defn.ScopeId == collections.SCOPE_ID_NIL ||
				defn.CollectionId == collections.COLLECTION_ID_NIL {

				commit = false
			}

			return commit && m.canRetryCreateError(err), defn.BucketUUID, defn.ScopeId, defn.CollectionId, err
		}

		if !definitions[0].Deferred && len(definitions) == 1 && !asyncCreate {
			// If there is only one definition, then try to do the build as well.
			retryList, skipList, errList, _ := m.buildIndexesLifecycleMgr([]common.IndexDefnId{defnId}, reqCtx, false)

			if len(retryList) != 0 {
				// It is a recoverable error.  Create commit token and return error.
				logging.Errorf("LifecycleMgr.processCommitToken() : build index fails.  Reason = %v", retryList[0])
				return true, defn.BucketUUID, defn.ScopeId, defn.CollectionId, retryList[0]
			}

			if len(errList) != 0 {
				// It is not a recoverable error.  Do not create commit token and return error.
				logging.Errorf("LifecycleMgr.processCommitToken() : build index fails.  Reason = %v", errList[0])
				m.DeleteIndex(defnId, true, false, reqCtx)
				return false, "", "", "", errList[0]
			}

			if len(skipList) != 0 {
				// It is very unlikely to skip an index.  Let create the commit token and retry.
				logging.Errorf("LifecycleMgr.processCommitToken() : index is skipped during build.  Create create-token and retry.")
			}
		}

		// create commit token
		return true, defn.BucketUUID, defn.ScopeId, defn.CollectionId, nil
	}

	// these definitions are not for my indexer, do not create commit token.
	return false, "", "", "", nil
}

//-----------------------------------------------------------
// Atomic Alter Index
//-----------------------------------------------------------

//
// Commit add replica
//
func (m *LifecycleMgr) handleCommitAddReplica(commitRequest *client.CommitCreateRequest) ([]byte, error) {

	if common.GetBuildMode() != common.ENTERPRISE {
		err := errors.New("Index Replica not supported in non-Enterprise Edition")
		logging.Errorf("LifecycleMgr.handleCommitAddReplica() : %v", err)
		response := &client.CommitCreateResponse{Accept: false}
		return client.MarshallCommitCreateResponse(response)
	}

	if m.prepareLock == nil {
		logging.Infof("LifecycleMgr.handleCommitAddReplica() : Reject %v because there is no lock", commitRequest.DefnId)
		response := &client.CommitCreateResponse{Accept: false}
		return client.MarshallCommitCreateResponse(response)
	}

	if m.prepareLock.RequesterId != commitRequest.RequesterId ||
		m.prepareLock.DefnId != commitRequest.DefnId {

		logging.Infof("LifecycleMgr.handleCommitAddReplica() : Reject %v because defnId and requesterId do not match", commitRequest.DefnId)
		response := &client.CommitCreateResponse{Accept: false}
		return client.MarshallCommitCreateResponse(response)
	}

	if _, err := m.repo.GetLocalValue("RebalanceRunning"); err == nil {
		logging.Infof("LifecycleMgr.handleCommitAddReplica() : Reject %v because rebalance in progress", commitRequest.DefnId)
		response := &client.CommitCreateResponse{Accept: false}
		return client.MarshallCommitCreateResponse(response)
	}

	defnId := commitRequest.DefnId
	definitions := commitRequest.Definitions
	requestId := commitRequest.RequestId
	m.prepareLock = nil

	commit, numReplica, bucketUUID, scopeId, collectionId, err := m.processAddReplicaCommitToken(defnId, definitions)
	if commit {
		// If fails to post the command token, the return failure.  If none of the indexer can post the command token,
		// the command token will be malformed and it will get cleaned up by DDLServiceMgr upon rebalancing.
		if err1 := mc.PostCreateCommandToken(defnId, bucketUUID, scopeId, collectionId, requestId, definitions); err1 != nil {
			logging.Infof("LifecycleMgr.handleCommitAddReplica() : Reject %v because fail to post token", commitRequest.DefnId)

			if err == nil {
				err = fmt.Errorf("Alter Index fails.  Cause: %v", err1)
			}

			response := &client.CommitCreateResponse{Accept: false}
			msg, _ := client.MarshallCommitCreateResponse(response)
			return msg, err
		}

		// We are successful in creating the commit token.  Now try updating index replica count.  This operation is idempotent,
		// so even if it fails, DDLServiceMgr will retry as long as the token is created.
		m.updateIndexReplicaCount(defnId, *numReplica)

		logging.Infof("LifecycleMgr.handleCommitAddReplica() : Create token posted for %v", defnId)
		response := &client.CommitCreateResponse{Accept: true}
		msg, err1 := client.MarshallCommitCreateResponse(response)
		if err1 != nil {
			if err == nil {
				err = err1
			}
		}

		return msg, err
	}

	logging.Infof("LifecycleMgr.handleCommitAddReplica() : Create token posted for %v", defnId)
	response := &client.CommitCreateResponse{Accept: false}
	msg, _ := client.MarshallCommitCreateResponse(response)
	return msg, err
}

//
// Process commit token for add replica index
//
func (m *LifecycleMgr) processAddReplicaCommitToken(defnId common.IndexDefnId, layout map[common.IndexerId][]common.IndexDefn) (bool,
	*common.Counter, string, string, string, error) {

	indexerId, err := m.repo.GetLocalIndexerId()
	if err != nil {
		return false, nil, "", "", "", fmt.Errorf("Alter Index fails.  Internal Error: %v", err)
	}

	if definitions, ok := layout[indexerId]; ok && len(definitions) > 0 {

		// Get the bucket UUID. This is needed for creating commit token.
		defn := definitions[0]
		if err := m.setBucketUUID(&defn); err != nil {
			return false, nil, "", "", "", err
		}

		if err := m.setScopeIdAndCollectionId(&defn); err != nil {
			return false, nil, "", "", "", err
		}

		// create commit token
		return true, &defn.NumReplica2, defn.BucketUUID, defn.ScopeId, defn.CollectionId, nil
	}

	// these definitions are not for my indexer, do not create commit token.
	return false, nil, "", "", "", nil
}

//
// Commit remove replica
//
func (m *LifecycleMgr) handleCommitDropReplica(commitRequest *client.CommitCreateRequest) ([]byte, error) {

	if common.GetBuildMode() != common.ENTERPRISE {
		err := errors.New("Index Replica not supported in non-Enterprise Edition")
		logging.Errorf("LifecycleMgr.handleCommitDropReplica() : %v", err)
		response := &client.CommitCreateResponse{Accept: false}
		return client.MarshallCommitCreateResponse(response)
	}

	if m.prepareLock == nil {
		logging.Infof("LifecycleMgr.handleCommitDropReplica() : Reject %v because there is no lock", commitRequest.DefnId)
		response := &client.CommitCreateResponse{Accept: false}
		return client.MarshallCommitCreateResponse(response)
	}

	if m.prepareLock.RequesterId != commitRequest.RequesterId ||
		m.prepareLock.DefnId != commitRequest.DefnId {

		logging.Infof("LifecycleMgr.handleCommitDropReplica() : Reject %v because defnId and requesterId do not match", commitRequest.DefnId)
		response := &client.CommitCreateResponse{Accept: false}
		return client.MarshallCommitCreateResponse(response)
	}

	if _, err := m.repo.GetLocalValue("RebalanceRunning"); err == nil {
		logging.Infof("LifecycleMgr.handleCommitDropReplica() : Reject %v because rebalance in progress", commitRequest.DefnId)
		response := &client.CommitCreateResponse{Accept: false}
		return client.MarshallCommitCreateResponse(response)
	}

	indexerId, err := m.repo.GetLocalIndexerId()
	if err != nil {
		logging.Infof("LifecycleMgr.handleCommitDropReplica() : Error: %v", err)
		response := &client.CommitCreateResponse{Accept: false}
		return client.MarshallCommitCreateResponse(response)
	}

	definitions := commitRequest.Definitions[indexerId]
	m.prepareLock = nil

	for _, defn := range definitions {

		defnId := defn.DefnId
		instId := defn.InstId
		replicaId := defn.ReplicaId

		if replicaId != int(math.MaxInt64) {

			// If fails to post the command token, the return failure.  If none of the indexer can post the command token,
			// the command token will be malformed and it will get cleaned up by DDLServiceMgr upon rebalancing.
			if err1 := mc.PostDropInstanceCommandToken(defnId, instId, replicaId, defn); err1 != nil {
				logging.Errorf("LifecycleMgr.handleCommitDropReplica() : Reject %v because fail to post token", defnId)

				if err == nil {
					err = fmt.Errorf("Alter Index fails.  Cause: %v", err1)
				}
			} else {
				logging.Infof("LifecycleMgr.handleCommitDropReplica() : Drop Instance token posted for instance %v", instId)
			}
		}

		// Now try updating index replica count.  This operation is idempotent, so even if it fails, DDLServiceMgr
		// will retry as long as the token is created.
		m.updateIndexReplicaCount(defn.DefnId, defn.NumReplica2)
	}

	response := &client.CommitCreateResponse{Accept: err == nil}
	msg, err1 := client.MarshallCommitCreateResponse(response)
	if err1 != nil {
		if err == nil {
			err = err1
		}
	}

	m.janitor.runOnce()

	return msg, err
}

//
// handle updating replica count
//
func (m *LifecycleMgr) handleUpdateReplicaCount(content []byte) error {

	defn, err := common.UnmarshallIndexDefn(content)
	if err != nil {
		logging.Errorf("LifecycleMgr.handleUpdateReplicaCount() : Unable to unmarshall request. Reason = %v", err)
		return err
	}
	defn.SetCollectionDefaults()

	return m.updateIndexReplicaCount(defn.DefnId, defn.NumReplica2)
}

// Update replica Count. This function is idepmpotent.
func (m *LifecycleMgr) updateIndexReplicaCount(defnId common.IndexDefnId, numReplica common.Counter) error {

	existDefn, err := m.repo.GetIndexDefnById(defnId)
	if err != nil {
		logging.Errorf("LifecycleMgr.updateIndexReplicaCount() : %v", err)
		return err
	}

	if existDefn == nil {
		logging.Infof("LifecycleMgr.updateIndexReplicaCount() : Index Definition does not exist for %v.  No update is performed.", defnId)
		return nil
	}

	newNumReplica, changed, err := existDefn.NumReplica2.MergeWith(numReplica)
	if err != nil {
		logging.Errorf("LifecycleMgr.updateIndexReplicaCount() : %v", err)
		return err
	}

	if changed {
		defn := *existDefn
		defn.NumReplica2 = newNumReplica
		if err := m.repo.UpdateIndex(&defn); err != nil {
			logging.Errorf("LifecycleMgr.updateIndexReplicaCount() : alter index fails for index %v. Reason = %v", defnId, err)
			return err
		}
	}

	return nil
}

//
// handle retrieve index replica count
//
func (m *LifecycleMgr) handleGetIndexReplicaCount(content []byte) ([]byte, error) {

	var defnId common.IndexDefnId
	if err := json.Unmarshal(content, &defnId); err != nil {
		logging.Errorf("LifecycleMgr.handleGetIndexReplicaCount() : Unable to unmarshall. Reason = %v", err)
		return nil, err
	}

	existDefn, err := m.repo.GetIndexDefnById(defnId)
	if err != nil {
		logging.Errorf("LifecycleMgr.handleGetIndexReplicaCount() : %v", err)
		return nil, err
	}

	var numReplica *common.Counter

	if existDefn != nil {
		var err error
		numReplica, err = GetLatestReplicaCount(existDefn)
		if err != nil {
			logging.Errorf("LifecycleMgr.handleGetIndexReplicaCount(): Fail to merge counter with index definition for index %v: %v", defnId, err)
			return nil, err
		}
	}

	result, err := common.MarshallCounter(numReplica)
	if err != nil {
		logging.Errorf("LifecycleMgr.handleGetIndexReplicaCount() : Unable to marshall. Reason = %v", err)
		return nil, err
	}

	return result, nil
}

//
// handle check for tokens
//
func (m *LifecycleMgr) handleCheckTokenExist(content []byte) ([]byte, error) {

	checkToken, err := client.UnmarshallChecKToken(content)
	if err != nil {
		logging.Errorf("LifecycleMgr.handleCheckTokenExist() : Unable to unmarshall. Reason = %v", err)
		return nil, err
	}

	exist := false
	if (checkToken.Flag & client.CREATE_INDEX_TOKEN) != 0 {
		t, err := mc.CreateCommandTokenExist(checkToken.DefnId)
		if err != nil {
			logging.Errorf("LifecycleMgr.handleCheckTokenExist(): Fail to retrieve create token for index %v: %v", checkToken.DefnId, err)
			return nil, err
		}
		exist = exist || t
	}

	if (checkToken.Flag & client.DROP_INDEX_TOKEN) != 0 {
		t, err := mc.DeleteCommandTokenExist(checkToken.DefnId)
		if err != nil {
			logging.Errorf("LifecycleMgr.handleCheckTokenExist(): Fail to retrieve delete token for index %v: %v", checkToken.DefnId, err)
			return nil, err
		}
		exist = exist || t
	}

	if (checkToken.Flag & client.DROP_INSTANCE_TOKEN) != 0 {
		t, err := mc.DropInstanceCommandTokenExist(checkToken.DefnId, checkToken.InstId)
		if err != nil {
			logging.Errorf("LifecycleMgr.handleCheckTokenExist(): Fail to retrieve delete instance token for instance %v: %v", checkToken.InstId, err)
			return nil, err
		}
		exist = exist || t
	}

	result, err := json.Marshal(&exist)
	if err != nil {
		logging.Errorf("LifecycleMgr.handleCheckTokenExist() : Unable to marshall. Reason = %v", err)
		return nil, err
	}

	return result, nil
}

//-----------------------------------------------------------
// Create Index
//-----------------------------------------------------------

// handleCreateIndexDeferBuild handles only create index for deferred builds (OPCODE_CREATE_INDEX_DEFER_BUILD).
func (m *LifecycleMgr) handleCreateIndexDeferBuild(key string, content []byte, reqCtx *common.MetadataRequestContext) error {

	defn, err := common.UnmarshallIndexDefn(content)
	if err != nil {
		logging.Errorf("LifecycleMgr.handleCreateIndexDeferBuild() : createIndex fails. Unable to unmarshal index definition. Reason = %v", err)
		return err
	}
	defn.SetCollectionDefaults()

	return m.CreateIndexOrInstance(defn, false, reqCtx, false)
}

// handleCreateIndexScheduledBuild handles both normal and rebalance create index requests
// (OPCODE_CREATE_INDEX, OPCODE_CREATE_INDEX_REBAL).
func (m *LifecycleMgr) handleCreateIndexScheduledBuild(key string, content []byte,
	reqCtx *common.MetadataRequestContext) error {

	defn, err := common.UnmarshallIndexDefn(content)
	if err != nil {
		logging.Errorf("LifecycleMgr.handleCreateIndexScheduledBuild() : createIndex fails. Unable to unmarshall index definition. Reason = %v", err)
		return err
	}
	defn.SetCollectionDefaults()

	// Create index with the scheduled flag.
	return m.CreateIndexOrInstance(defn, true, reqCtx, false)
}

func (m *LifecycleMgr) CreateIndexOrInstance(defn *common.IndexDefn, scheduled bool,
	reqCtx *common.MetadataRequestContext, asyncCreate bool) error {

	if common.GetBuildMode() != common.ENTERPRISE {
		if defn.NumReplica != 0 {
			err := errors.New("Index Replica not supported in non-Enterprise Edition")
			logging.Errorf("LifecycleMgr.CreateIndexOrInstance() : createIndex fails. Reason = %v", err)
			return err
		}
		if common.IsPartitioned(defn.PartitionScheme) {
			err := errors.New("Index Partitining is not supported in non-Enterprise Edition")
			logging.Errorf("LifecycleMgr.CreateIndexOrInstance() : createIndex fails. Reason = %v", err)
			return err
		}
	}

	existDefn, err := m.verifyDuplicateDefn(defn, reqCtx)
	if err != nil {
		return err
	}

	if err := m.verifyDuplicateInstance(defn, reqCtx); err != nil {
		return err
	}

	hasIndex := existDefn != nil && (defn.DefnId == existDefn.DefnId)
	isPartitioned := common.IsPartitioned(defn.PartitionScheme)

	if isPartitioned && hasIndex {
		return m.CreateIndexInstance(defn, scheduled, reqCtx, asyncCreate)
	}

	return m.CreateIndex(defn, scheduled, reqCtx, asyncCreate)
}

func (m *LifecycleMgr) CreateIndex(defn *common.IndexDefn, scheduled bool,
	reqCtx *common.MetadataRequestContext, asyncCreate bool) error {

	/////////////////////////////////////////////////////
	// Verify input parameters
	/////////////////////////////////////////////////////

	if err := m.setBucketUUID(defn); err != nil {
		return err
	}

	if err := m.setScopeIdAndCollectionId(defn); err != nil {
		return err
	}

	if err := m.setStorageMode(defn); err != nil {
		return err
	}

	if err := m.setImmutable(defn); err != nil {
		return err
	}

	instId, realInstId, err := m.setInstId(defn)
	if err != nil {
		return err
	}

	if realInstId != 0 {
		realInst, err := m.FindLocalIndexInst(defn.Bucket, defn.Scope, defn.Collection, defn.DefnId, realInstId)
		if err != nil {
			logging.Errorf("LifecycleMgr.CreateIndex() : CreateIndex fails. Reason = %v", err)
			return err
		}

		// Lifeccyle mgr is the source of truth of metadata.  If it sees that there is an existing index instance in
		// DELETED state, it means that indexer has not been able to delete this instance.   Given that index defn does
		// not exist, we can simply delete the index instance without notifying indexer.
		if realInst != nil && common.IndexState(realInst.State) == common.INDEX_STATE_DELETED {
			logging.Infof("LifecycleMgr.CreateIndex() : Remove deleted index instances for index %v", defn.DefnId)
			m.repo.deleteIndexFromTopology(defn.Bucket, defn.Scope, defn.Collection, defn.DefnId)
			realInst = nil
		}

		if realInst == nil {
			instId = realInstId
			realInstId = 0
		}
	}

	replicaId := m.setReplica(defn)

	partitions, versions, numPartitions := m.setPartition(defn)

	/////////////////////////////////////////////////////
	// Create Index Metadata
	/////////////////////////////////////////////////////

	// Create index definiton.   It will fail if there is another index defintion of the same
	// index defnition id.
	if err := m.repo.CreateIndex(defn); err != nil {
		logging.Errorf("LifecycleMgr.CreateIndex() : createIndex fails. Reason = %v", err)
		return err
	}

	// Create index instance
	// If there is any dangling index instance of the same index name and bucket, this will first
	// remove the dangling index instance first.
	// If indexer crash during creation of index instance, there could be a dangling index definition.
	// It is possible to create index of the same name later, as long as the new index has a different
	// definition id, since an index is consider valid only if it has both index definiton and index instance.
	// So the dangling index definition is considered invalid.
	if err := m.repo.addIndexToTopology(defn, instId, replicaId, partitions, versions, numPartitions, realInstId, !defn.Deferred && scheduled); err != nil {
		logging.Errorf("LifecycleMgr.CreateIndex() : createIndex fails. Reason = %v", err)
		m.repo.DropIndexById(defn.DefnId)
		return err
	}

	/////////////////////////////////////////////////////
	// Create Index in Indexer
	/////////////////////////////////////////////////////

	// At this point, an index is created successfully with state=CREATED.  If indexer crashes now,
	// indexer will repair the index upon bootstrap (either cleanup or move to READY state).
	// If indexer returns an error when creating index, then cleanup metadata now.    During metadata cleanup,
	// errors are ignored.  This means the following:
	// 1) Index Definition is not deleted due to error from metadata repository.   The index will be repaired
	//    during indexer bootstrap or implicit dropIndex.
	// 2) Index definition is deleted.  This effectively "delete index".
	if m.notifier != nil {
		if err := m.notifier.OnIndexCreate(defn, instId, replicaId, partitions, versions, numPartitions, 0, reqCtx); err != nil {
			logging.Errorf("LifecycleMgr.CreateIndex() : createIndex fails. Reason = %v", err)
			m.DeleteIndex(defn.DefnId, false, false, nil)
			return err
		}
	}

	/////////////////////////////////////////////////////
	// Update Index State
	/////////////////////////////////////////////////////

	// If cannot move the index to READY state, then abort create index by cleaning up the metadata.
	// Metadata cleanup is not atomic.  The index is effectively "deleted" if it is able to drop
	// the index definition from repository. If drop index is not successful during cleanup,
	// the index will be repaired upon bootstrap or cleanup by janitor.
	if err := m.updateIndexState(defn.Bucket, defn.Scope, defn.Collection, defn.DefnId, instId, common.INDEX_STATE_READY); err != nil {
		logging.Errorf("LifecycleMgr.CreateIndex() : createIndex fails. Reason = %v", err)
		m.DeleteIndex(defn.DefnId, true, false, reqCtx)
		return err
	}

	instState := m.getInstStateFromTopology(defn.Bucket, defn.Scope, defn.Collection, defn.DefnId, instId)
	if instState != common.INDEX_STATE_READY {
		logging.Fatalf("LifecycleMgr.CreateIndex(): Instance state is not INDEX_STATE_READY. Instance: %v (%v, %v, %v, %v). "+
			"Instance state in topology: %v", instId, defn.Bucket, defn.Scope, defn.Collection, defn.DefnId, instState)
		err := fmt.Errorf("Unexpected Instance State %v for Index (%v, %v, %v, %v). Expected %v.", instState,
			defn.Bucket, defn.Scope, defn.Collection, defn.Name, common.INDEX_STATE_READY)
		return err
	}
	/////////////////////////////////////////////////////
	// Build Index
	/////////////////////////////////////////////////////

	// Run index build
	if !defn.Deferred && scheduled && !asyncCreate {
		if m.notifier != nil {
			logging.Debugf("LifecycleMgr.CreateIndex() : start Index Build")

			retryList, skipList, errList, _ := m.buildIndexesLifecycleMgr([]common.IndexDefnId{defn.DefnId}, reqCtx, false)

			if len(retryList) != 0 {
				return errors.New("Failed to build index.  Index build will be retried in background.")
			}

			if len(errList) != 0 {
				logging.Errorf("LifecycleMgr.CreateIndex() : build index fails.  Reason = %v", errList[0])
				m.DeleteIndex(defn.DefnId, true, false, reqCtx)
				return errList[0]
			}

			if len(skipList) != 0 {
				logging.Errorf("LifecycleMgr.CreateIndex() : build index fails due to internal errors.")

				m.DeleteIndex(defn.DefnId, true, false, reqCtx)
				return errors.New("Failed to create index due to internal build error.  Please retry the operation.")
			}
		}
	}

	logging.Debugf("LifecycleMgr.CreateIndex() : createIndex completes")

	return nil
}

func (m *LifecycleMgr) getInstStateFromTopology(bucket, scope, collection string, defnId common.IndexDefnId, instId common.IndexInstId) common.IndexState {
	inst, err := m.FindLocalIndexInst(bucket, scope, collection, defnId, instId)
	if inst == nil || err != nil {
		logging.Fatalf("LifecycleMgr.getInstStateFromTopology Error observed while retrieving inst state from topology. "+
			"InstId: %v (%v, %v, %v, %v). Inst: %+v, err: %v", instId, bucket, scope, collection, defnId, inst, err)
	}
	if inst != nil {
		return common.IndexState(inst.State)
	}
	return common.INDEX_STATE_NIL // As instance is not found
}

func (m *LifecycleMgr) setBucketUUID(defn *common.IndexDefn) error {

	// Fetch bucket UUID.   Note that this confirms that the bucket has existed, but it cannot confirm if the bucket
	// is still existing in the cluster (due to race condition or network partitioned).
	//
	// Lifecycle manager is a singleton that ensures all metadata operation is serialized.  Therefore, a
	// call to verifyBucket() here  will also make sure that all existing indexes belong to the same bucket UUID.
	// To ensure verifyBucket can succeed, indexes from stale bucket must be cleaned up (eventually).
	//
	bucketUUID, err := m.verifyBucket(defn.Bucket)
	if err != nil || bucketUUID == common.BUCKET_UUID_NIL {
		if err == nil {
			err = common.ErrBucketNotFound
		}
		// Error msg returned to user. *KEEP [%v] FOR ORIGINAL ERROR STRING* so we can detect it without ambiguity.
		return fmt.Errorf("[%v] Bucket %v does not exist or temporarily unavailable for creating new index."+
			" Please retry the operation at a later time.", err, defn.Bucket)
	}

	if len(defn.BucketUUID) != 0 && defn.BucketUUID != bucketUUID {
		return common.ErrBucketUUIDChanged
	}

	defn.BucketUUID = bucketUUID
	return nil
}

func (m *LifecycleMgr) setScopeIdAndCollectionId(defn *common.IndexDefn) error {

	if atomic.LoadUint32(&m.collAwareCluster) == 0 {

		// Get cluster version from common
		clusterVersion := common.GetClusterVersion()

		if clusterVersion < (int64)(common.INDEXER_70_VERSION) {
			// The cluster compatibiltity is less than 7.0
			// ScopeId and CollectionId cannot be obtained during mixed mode,
			// hence use default IDs and skip the verification
			defn.ScopeId = common.DEFAULT_SCOPE_ID
			defn.CollectionId = common.DEFAULT_COLLECTION_ID
			return nil
		} else {
			// The cluster has now fully upgraded, cache this information
			atomic.StoreUint32(&m.collAwareCluster, 1)
		}
	}

	scopeId, collectionId, err := m.verifyScopeAndCollection(defn.Bucket, defn.Scope, defn.Collection)
	if err != nil {
		return err
	}

	if scopeId == collections.SCOPE_ID_NIL {
		// Error msg returned to user. *KEEP [%v] FOR ORIGINAL ERROR STRING* so we can detect it without ambiguity.
		return fmt.Errorf("[%v] Error retrieving scope ID."+
			" Bucket: %v, Scope: %v. Possibly dropped, else please retry later.",
			common.ErrScopeNotFound, defn.Bucket, defn.Scope)
	}

	if collectionId == collections.COLLECTION_ID_NIL {
		// Error msg returned to user. *KEEP [%v] FOR ORIGINAL ERROR STRING* so we can detect it without ambiguity.
		return fmt.Errorf("[%v] Error retrieving collection ID."+
			" Bucket: %v, Scope: %v, Collection: %v. Possibly dropped, else please retry later.",
			common.ErrCollectionNotFound, defn.Bucket, defn.Scope, defn.Collection)
	}

	if len(defn.ScopeId) != 0 && defn.ScopeId != scopeId {
		return common.ErrScopeIdChanged
	}

	if len(defn.CollectionId) != 0 && defn.CollectionId != collectionId {
		return common.ErrCollectionIdChanged
	}

	defn.ScopeId = scopeId
	defn.CollectionId = collectionId
	return nil
}

func (m *LifecycleMgr) setStorageMode(defn *common.IndexDefn) error {

	//if no index_type has been specified
	if strings.ToLower(string(defn.Using)) == "gsi" {
		if common.GetStorageMode() != common.NOT_SET {
			//if there is a storage mode, default to that
			defn.Using = common.IndexType(common.GetStorageMode().String())
		} else {
			//default to plasma
			defn.Using = common.PlasmaDB
		}
	} else {
		if common.IsValidIndexType(string(defn.Using)) {
			defn.Using = common.IndexType(strings.ToLower(string(defn.Using)))
		} else {
			err := fmt.Sprintf("Create Index fails. Reason = Unsupported Using Clause %v", string(defn.Using))
			logging.Errorf("LifecycleMgr.setStorageType: " + err)
			return errors.New(err)
		}
	}

	if common.IsPartitioned(defn.PartitionScheme) {
		if defn.Using != common.PlasmaDB && defn.Using != common.MemDB && defn.Using != common.MemoryOptimized {
			err := fmt.Sprintf("Create Index fails. Reason = Cannot create partitioned index using %v", string(defn.Using))
			logging.Errorf("LifecycleMgr.setStorageType: " + err)
			return errors.New(err)
		}
	}

	return nil
}

func (m *LifecycleMgr) setImmutable(defn *common.IndexDefn) error {

	// If it is a partitioned index, immutable is set to true by default.
	// If immutable is true, then set it to false for MOI, so flusher can process
	// upsertDeletion mutation.
	if common.IsPartitioned(defn.PartitionScheme) && defn.Immutable {
		if defn.Using == common.MemoryOptimized {
			//defn.Immutable = false
		}
	}

	return nil
}

func (m *LifecycleMgr) setInstId(defn *common.IndexDefn) (common.IndexInstId, common.IndexInstId, error) {

	//
	// Figure out the index instance id
	//
	var instId common.IndexInstId
	if defn.InstId > 0 {
		//use already supplied instance id (e.g. rebalance case)
		instId = defn.InstId
	} else {
		// create index id
		var err error
		instId, err = common.NewIndexInstId()
		if err != nil {
			return 0, 0, err
		}
	}
	defn.InstId = 0

	var realInstId common.IndexInstId
	if defn.RealInstId > 0 {
		realInstId = defn.RealInstId
	}
	defn.RealInstId = 0

	return instId, realInstId, nil
}

func (m *LifecycleMgr) setReplica(defn *common.IndexDefn) int {

	// create replica id
	replicaId := defn.ReplicaId
	defn.ReplicaId = -1

	return replicaId
}

func (m *LifecycleMgr) setPartition(defn *common.IndexDefn) ([]common.PartitionId, []int, uint32) {

	// partitions
	var partitions []common.PartitionId
	var versions []int
	var numPartitions uint32

	if !common.IsPartitioned(defn.PartitionScheme) {
		partitions = []common.PartitionId{common.PartitionId(0)}
		versions = []int{defn.InstVersion}
		numPartitions = 1
	} else {
		partitions = defn.Partitions
		versions = defn.Versions
		numPartitions = defn.NumPartitions
	}

	defn.NumPartitions = 0
	defn.Partitions = nil
	defn.Versions = nil

	return partitions, versions, numPartitions
}

func (m *LifecycleMgr) verifyDuplicateInstance(defn *common.IndexDefn, reqCtx *common.MetadataRequestContext) error {

	existDefn, err := m.repo.GetIndexDefnByName(defn.Bucket, defn.Scope, defn.Collection, defn.Name)
	if err != nil {
		logging.Errorf("LifecycleMgr.CreateIndexInstance() : createIndex fails. Reason = %v", err)
		return err
	}

	if existDefn != nil {
		// The index already exist in this node.  Make sure there is no overlapping partition.
		topology, err := m.repo.GetTopologyByCollection(existDefn.Bucket, existDefn.Scope, existDefn.Collection)
		if err != nil {
			logging.Errorf("LifecycleMgr.CreateIndexInstance() : fails to find index instance. Reason = %v", err)
			return err
		}

		if topology != nil {
			insts := topology.GetIndexInstancesByDefn(existDefn.DefnId)

			// Go through each partition that we want to create for this index instance
			for _, inst := range insts {

				// Guard against duplicate index instance id
				if common.IndexInstId(inst.InstId) == defn.InstId &&
					common.IndexState(inst.State) != common.INDEX_STATE_DELETED {
					return fmt.Errorf("Duplicate Index Instance %v.", inst.InstId)
				}
			}
		}
	}

	return nil
}

func (m *LifecycleMgr) verifyDuplicateDefn(defn *common.IndexDefn, reqCtx *common.MetadataRequestContext) (*common.IndexDefn, error) {

	existDefn, err := m.repo.GetIndexDefnByName(defn.Bucket, defn.Scope, defn.Collection, defn.Name)
	if err != nil {
		logging.Errorf("LifecycleMgr.verifyDuplicateDefn() : createIndex fails. Reason = %v", err)
		return nil, err
	}

	if existDefn != nil {

		topology, err := m.repo.GetTopologyByCollection(existDefn.Bucket, existDefn.Scope, existDefn.Collection)
		if err != nil {
			logging.Errorf("LifecycleMgr.verifyDuplicateDefn() : fails to find index instance. Reason = %v", err)
			return nil, err
		}

		if topology != nil {

			// This is not non-partitioned index, so make sure the old instance does not exist.
			if !common.IsPartitioned(defn.PartitionScheme) ||
				!common.IsPartitioned(existDefn.PartitionScheme) {

				// Allow index creation even if there is an existing index defn with the same name and bucket, as long
				// as the index state is NIL (index instance non-existent) or DELETED state.
				// If an index is in CREATED state, it will be repaired during indexer bootstrap:
				// 1) For non-deferred index, it removed by the indexer.
				// 2) For deferred index, it will be moved to READY state.
				// For partitioned index, it requires all instances residing on this node be nil or DELETED for
				// create index to go through.
				insts := topology.GetIndexInstancesByDefn(existDefn.DefnId)
				for _, inst := range insts {
					state, _ := topology.GetStatusByInst(existDefn.DefnId, common.IndexInstId(inst.InstId))
					if state != common.INDEX_STATE_NIL && state != common.INDEX_STATE_DELETED {
						return existDefn, errors.New(fmt.Sprintf("Index %s.%s already exists", defn.Bucket, defn.Name))
					}
				}
			}
		}
	}

	return existDefn, nil
}

// GetLatestReplicaCount will fetch CreateCommand and DropInstance tokens from metakv and get latest replica count.
func GetLatestReplicaCount(defn *common.IndexDefn) (*common.Counter, error) {

	defnID := defn.DefnId

	// Check if there is any create token.  If so, it means that there is a pending create or alter index.
	// The create token should contain the latest numReplica.
	createTokenList, err := mc.ListAndFetchCreateCommandToken(defnID)
	if err != nil {
		logging.Errorf("LifecycleMgr.GetLatestReplicaCount(): Fail to retrieve create token for index %v: %v", defnID, err)
		return nil, err
	}

	// Get numReplica from drop instance token.
	dropInstTokenList, err := mc.ListAndFetchDropInstanceCommandToken(defnID)
	if err != nil {
		logging.Errorf("LifecycleMgr.GetLatestReplicaCount(): Fail to retrieve drop instance token for index %v: %v", defnID, err)
		return nil, err
	}

	return GetLatestReplicaCountFromTokens(defn, createTokenList, dropInstTokenList)
}

// GetLatestReplicaCountFromTokens will merge the replica count from given set of tokens and index definition.
func GetLatestReplicaCountFromTokens(defn *common.IndexDefn,
	createTokenList []*mc.CreateCommandToken,
	dropInstTokenList []*mc.DropInstanceCommandToken) (*common.Counter, error) {

	merge := func(numReplica *common.Counter, defn *common.IndexDefn) (*common.Counter, error) {

		if defn == nil {
			return numReplica, nil
		}

		if defn.NumReplica2.IsValid() {
			result, merged, err := numReplica.MergeWith(defn.NumReplica2)
			if err != nil {
				return nil, err
			}
			if merged {
				numReplica = &result
			}

		} else if !numReplica.IsValid() {
			numReplica.Initialize(defn.NumReplica)
		}

		return numReplica, nil
	}

	numReplica := &common.Counter{}
	defnID := defn.DefnId

	var err error

	for _, token := range createTokenList {
		// Get the numReplica from the create command token.
		for _, definitions := range token.Definitions {
			if len(definitions) != 0 {
				numReplica, err = merge(numReplica, &definitions[0])
				if err != nil {
					logging.Errorf("LifecycleMgr.GetLatestReplicaCountFromTokens(): Fail to merge counter with create token for index %v: %v", defnID, err)
					return nil, err
				}
				break
			}
		}
	}

	for _, token := range dropInstTokenList {
		numReplica, err = merge(numReplica, &token.Defn)
		if err != nil {
			logging.Errorf("LifecycleMgr.GetLatestReplicaCountFromTokens(): Fail to merge counter with drop instance token for index %v: %v", defnID, err)
			return nil, err
		}
	}

	numReplica, err = merge(numReplica, defn)
	if err != nil {
		logging.Errorf("LifecycleMgr.GetLatestReplicaCountFromTokens(): Fail to merge counter with index definition for index %v: %v", defnID, err)
		return nil, err
	}

	return numReplica, nil
}

//-----------------------------------------------------------
// Build Index
//-----------------------------------------------------------

// handleBuildIndexes handles all kinds of build index requests
// (OPCODE_BUILD_INDEX, OPCODE_BUILD_INDEX_REBAL, OPCODE_BUILD_INDEX_RETRY).
func (m *LifecycleMgr) handleBuildIndexes(content []byte, reqCtx *common.MetadataRequestContext, isRebal bool) error {

	list, err := client.UnmarshallIndexIdList(content)
	if err != nil {
		logging.Errorf("LifecycleMgr::handleBuildIndexes: buildIndex fails. Unable to unmarshal index list. Reason = %v", err)
		return err
	}

	input := make([]common.IndexDefnId, len(list.DefnIds))
	for i, id := range list.DefnIds {
		input[i] = common.IndexDefnId(id)
	}

	retryList, skipList, errList, errMap := m.buildIndexesLifecycleMgr(input, reqCtx, isRebal)
	var errMsg string
	if !isRebal {
		if len(retryList) != 0 || len(skipList) != 0 || len(errList) != 0 { // at least one reportable error
			errMsg = "Build index fails."

			if len(retryList) == 1 {
				errMsg += fmt.Sprintf(" %v", retryList[0])
			}

			if len(errList) == 1 {
				errMsg += fmt.Sprintf(" %v.", errList[0])
			}

			if len(retryList) > 1 {
				errMsg += " Some index will be retried building in the background."
			}

			if len(errList) > 1 {
				errMsg += " Some index cannot be built due to errors."
			}

			if len(skipList) != 0 {
				errMsg += " Some index cannot be built since it may not exist.  Please check if the list of indexes are valid."
			}

			if len(errList) > 1 || len(retryList) > 1 {
				errMsg += " For more details, please check index status."
			}

			return errors.New(errMsg)
		}
	} else { // rebalance
		if len(errMap) != 0 { // at least one reportable error
			// Convert errMap to a JSON string as errMsg to preserve the instance-specific error messages.
			// Rebalance should check for ErrMarshalFailed message before trying to unmarshal.
			errMsgBytes, err := json.Marshal(errMap)
			if err != nil {
				errMsg = common.ErrMarshalFailed.Error()
			} else {
				errMsg = string(errMsgBytes)
			}
			return errors.New(errMsg)
		}
	}
	return nil
}

// buildIndexesLifecycleMgr builds the set of indexes specified by "defnIds". Return values:
//   1. retryErrList -- decorated errors from failed build attempts that WILL be retried
//   2. skipList -- index defnIds not built and not retried because their metadata could not be found
//   3. errList -- decorated errors from failed build attempts that will NOT be retried
//   4. errMap -- undecorated error strings by instId from Indexer for entries in retryErrList and errList;
//      messages added here for failures in skipList since Indexer was never called for these
//      (and the keys for these will be defnId instead of instId as we don't have the latter)
func (m *LifecycleMgr) buildIndexesLifecycleMgr(defnIds []common.IndexDefnId,
	reqCtx *common.MetadataRequestContext, isRebal bool) (
	retryErrList []error, skipList []common.IndexDefnId, errList []error, errMap map[common.IndexInstId]string) {

	errMap = make(map[common.IndexInstId]string)
	retryList := ([]*common.IndexDefn)(nil) // retryable builds that failed; will go into notifych
	instIdList := []common.IndexInstId(nil)
	defnIdMap := make(map[common.IndexDefnId]bool)
	buckets := []string(nil)
	inst2DefnMap := make(map[common.IndexInstId]common.IndexDefnId)

	for _, defnId := range defnIds {

		if defnIdMap[defnId] {
			logging.Infof("LifecycleMgr::handleBuildIndexes: Duplicate index definition in the build list. Skip this index %v.", defnId)
			continue
		}
		defnIdMap[defnId] = true

		// Get index definition from meta_repo; failures here add entries to skipList and errMap[defnId]
		defn, err := m.repo.GetIndexDefnById(defnId)
		if err != nil {
			logging.Errorf("LifecycleMgr::handleBuildIndexes: Failed to find index definition for defnId %v, error: %v. Skipping this index.", defnId, err)
			errMap[common.IndexInstId(defnId)] = common.ErrIndexNotFoundRebal.Error()
			skipList = append(skipList, defnId)
			continue
		}
		if defn == nil {
			logging.Warnf("LifecycleMgr::handleBuildIndexes: Index defnId %v is nil. Skipping this index.", defnId)
			errMap[common.IndexInstId(defnId)] = common.ErrIndexNotFoundRebal.Error()
			skipList = append(skipList, defnId)
			continue
		}
		insts, err := m.findAllLocalIndexInst(defn.Bucket, defn.Scope, defn.Collection, defnId)
		if len(insts) == 0 || err != nil {
			logging.Errorf("LifecycleMgr::handleBuildIndexes: Failed to find index instances for index (%v, %v, %v, %v) for defnId %v. Skipping this index.",
				defn.Bucket, defn.Scope, defn.Collection, defn.Name, defnId)
			errMap[common.IndexInstId(defnId)] = common.ErrIndexNotFoundRebal.Error()
			skipList = append(skipList, defnId)
			continue
		}

		for _, inst := range insts {

			if inst.State != uint32(common.INDEX_STATE_READY) {
				logging.Warnf("LifecycleMgr.handleBuildIndexes: index instance %v (%v, %v, %v, %v, %v) is not in ready state. Inst state: %v. Skip this index.",
					inst.InstId, defn.Bucket, defn.Scope, defn.Collection, defn.Name, inst.ReplicaId, inst.State)

				// Instance can exist in any state but not in INDEX_STATE_CREATED
				// This code path can get executed only after an index is successfully created
				// On a successful index creation, index state would be in INDEX_STATE_READY or higher
				if inst.State == uint32(common.INDEX_STATE_CREATED) {
					logging.Fatalf("LifecycleMgr.handleBuildIndexes: Index instance: %+v is in state: INDEX_STATE_CREATED", inst)

					// It could be possible that the in-memory topoCache is corrupt(?) - a guess at this point
					// Retrieve the topology directly from meta and log the instance to understand the state in meta store
					topoFromMeta, err := m.repo.CloneTopologyByCollection(defn.Bucket, defn.Scope, defn.Collection)
					if err == nil {
						for i, _ := range topoFromMeta.Definitions {
							if topoFromMeta.Definitions[i].DefnId == uint64(defn.DefnId) {
								for j, _ := range topoFromMeta.Definitions[i].Instances {
									if inst.InstId == topoFromMeta.Definitions[i].Instances[j].InstId {
										logging.Fatalf("LifecycleMgr.handleBuildIndexes: Value of index instance: %+v in metastore", inst)
									}
								}
							}
						}
					}
					logging.Errorf("LifecycleMgr.handleBuildIndexes: Error while retrieving topology from meta, err: %v", err)
				}
				continue
			}

			//Schedule the build
			if err := m.SetScheduledFlag(defn.Bucket, defn.Scope, defn.Collection, defnId, common.IndexInstId(inst.InstId), true); err != nil {
				msg := fmt.Sprintf("LifecycleMgr.handleBuildIndexes: Unable to set scheduled flag in index instance (%v, %v, %v, %v, %v).",
					defn.Bucket, defn.Scope, defn.Collection, defn.Name, inst.ReplicaId)
				logging.Warnf("%v  Will try to build index now, but it will not be able to retry index build upon server restart.", msg)
			}

			// Reset any previous error
			m.UpdateIndexInstance(defn.Bucket, defn.Scope, defn.Collection, defnId, common.IndexInstId(inst.InstId), common.INDEX_STATE_NIL, common.NIL_STREAM, "", nil,
				inst.RState, nil, nil, -1)

			instIdList = append(instIdList, common.IndexInstId(inst.InstId))
			inst2DefnMap[common.IndexInstId(inst.InstId)] = defn.DefnId
		}

		found := false
		for _, bucket := range buckets {
			if bucket == defn.Bucket {
				found = true
			}
		}

		if !found {
			buckets = append(buckets, defn.Bucket)
		}
	}

	if m.notifier != nil && len(instIdList) != 0 {

		if errMap2 := m.notifier.OnIndexBuild(instIdList, buckets, reqCtx); len(errMap2) != 0 {
			logging.Errorf("LifecycleMgr::handleBuildIndexes: received build errors for instIds %v", errMap2)

			// Handle all the errors from the build attempt(s)
			for instId, build_err := range errMap2 {
				errMap[instId] = build_err.Error() // add to return map

				defnId, ok := inst2DefnMap[instId]
				if !ok {
					logging.Warnf("LifecycleMgr::handleBuildIndexes: Cannot find index defn for index inst %v when processing build error.")
					continue
				}

				defn, err := m.repo.GetIndexDefnById(defnId)
				if err != nil || defn == nil {
					logging.Warnf("LifecycleMgr::handleBuildIndexes: Cannot find index defn for index inst %v when processing build error.")
					continue
				}

				inst, err := m.FindLocalIndexInst(defn.Bucket, defn.Scope, defn.Collection, defnId, instId)
				if inst != nil && err == nil {
					if m.canRetryBuildError(inst, build_err, isRebal) {
						build_err = errors.New(fmt.Sprintf("Index %v will retry building in the background for reason: %v.", defn.Name, build_err.Error()))
					}
					m.UpdateIndexInstance(defn.Bucket, defn.Scope, defn.Collection, defnId, common.IndexInstId(inst.InstId), common.INDEX_STATE_NIL,
						common.NIL_STREAM, build_err.Error(), nil, inst.RState, nil, nil, -1)
				} else {
					logging.Infof("LifecycleMgr::handleBuildIndexes: Failed to persist, error in index instance (%v, %v, %v, %v, %v).",
						defn.Bucket, defn.Scope, defn.Collection, defn.Name, inst.ReplicaId)
				}

				if m.canRetryBuildError(inst, build_err, isRebal) {
					logging.Infof("LifecycleMgr::handleBuildIndexes: Encountered build error.  Retry building index (%v, %v, %v, %v, %v) at later time.",
						defn.Bucket, defn.Scope, defn.Collection, defn.Name, inst.ReplicaId)

					if inst != nil && !inst.Scheduled {
						if err := m.SetScheduledFlag(defn.Bucket, defn.Scope, defn.Collection, defnId, common.IndexInstId(inst.InstId), true); err != nil {
							msg := fmt.Sprintf("LifecycleMgr.handleBuildIndexes: Unable to set scheduled flag in index instance (%v, %v, %v, %v, %v).",
								defn.Bucket, defn.Scope, defn.Collection, defn.Name, inst.ReplicaId)
							logging.Warnf("%v  Will try to build index now, but it will not be able to retry index build upon server restart.", msg)
						}
					}

					retryList = append(retryList, defn)
					retryErrList = append(retryErrList, build_err)
				} else {
					errList = append(errList, errors.New(fmt.Sprintf("Index %v fails to build for reason: %v", defn.Name, build_err)))
				}
			}

			// Schedule retriable failed index builds for retry
			for _, defn := range retryList {
				m.builder.notifych <- defn
			}
		}
	}
	logging.Debugf("LifecycleMgr.buildIndexesLifecycleMgr() : buildIndexRebalance completes")
	return retryErrList, skipList, errList, errMap
}

//-----------------------------------------------------------
// Delete Index
//-----------------------------------------------------------

// handleDeleteIndex handles both normal and rebalance drop index operations
// (OPCODE_DROP_INDEX, OPCODE_DROP_INDEX_REBAL).
func (m *LifecycleMgr) handleDeleteIndex(key string, reqCtx *common.MetadataRequestContext) error {

	id, err := indexDefnId(key)
	if err != nil {
		logging.Errorf("LifecycleMgr.handleDeleteIndex() : deleteIndex fails. Reason = %v", err)
		return err
	}

	return m.DeleteIndex(id, true, false, reqCtx)
}

func (m *LifecycleMgr) DeleteIndex(id common.IndexDefnId, notify bool, updateStatusOnly bool,
	reqCtx *common.MetadataRequestContext) error {

	defn, err := m.repo.GetIndexDefnById(id)
	if err != nil {
		logging.Errorf("LifecycleMgr.DeleteIndex() : drop index fails for index defn %v.  Error = %v.", id, err)
		return err
	}
	if defn == nil {
		logging.Infof("LifecycleMgr.DeleteIndex() : index %v does not exist.", id)
		return nil
	}

	//
	// Mark all index inst as DELETED.  If it fail to mark an inst, then it will proceed to mark the other insts.
	// If any inst fails to be marked as DELETED, then this function will return without deleting the index defn.
	// An index is considered deleted/dropped as long as any inst is DELETED:
	// 1) It will be cleaned up by janitor
	// 2) Cannot create another index of the same name until all insts are marked deleted
	// 3) Deleted instances will be removed during bootstrap
	//
	insts, err := m.findAllLocalIndexInst(defn.Bucket, defn.Scope, defn.Collection, id)
	if err != nil {
		// Cannot read bucket topology.  Likely a transient error.  But without index inst, we cannot
		// proceed without letting indexer to clean up.
		logging.Warnf("LifecycleMgr.handleDeleteIndex() : Encountered error during delete index. Error = %v", err)
		return err
	}

	hasError := false
	for _, inst := range insts {
		// updateIndexState will not return an error if there is no index inst
		if err := m.updateIndexState(defn.Bucket, defn.Scope, defn.Collection, defn.DefnId, common.IndexInstId(inst.InstId), common.INDEX_STATE_DELETED); err != nil {
			hasError = true
		}
	}

	if hasError {
		logging.Errorf("LifecycleMgr.handleDeleteIndex() : deleteIndex fails. Reason = %v", err)
		return err
	}

	if updateStatusOnly {
		return nil
	}

	if notify && m.notifier != nil {
		insts, err := m.findAllLocalIndexInst(defn.Bucket, defn.Scope, defn.Collection, id)
		if err != nil {
			// Cannot read bucket topology.  Likely a transient error.  But without index inst, we cannot
			// proceed without letting indexer to clean up.
			logging.Warnf("LifecycleMgr.handleDeleteIndex() : Encountered error during delete index. Error = %v", err)
			return err
		}

		// If we cannot find the local index inst, then skip notifying the indexer, but proceed to delete the
		// index definition.  Lifecycle manager create index definition before index instance.  It also delete
		// index defintion before deleting index instance.  So if an index definition exists, but there is no
		// index instance, then it means the index has never succesfully created.
		//
		var dropErr error
		for _, inst := range insts {
			// Can call index delete again even after indexer has cleaned up -- if indexer crashes after
			// this point but before it can delete the index definition.
			// Note that the phsyical index is removed asynchronously by the indexer.
			if err := m.notifier.OnIndexDelete(common.IndexInstId(inst.InstId), defn.Bucket, reqCtx); err != nil {
				// Do not remove index defnition if indexer is unable to delete the index.   This is to ensure the
				// the client can call DeleteIndex again and free up indexer resource.
				indexerErr, ok := err.(*common.IndexerError)
				if ok && indexerErr.Code != common.IndexNotExist {
					errStr := fmt.Sprintf("Encountered error when dropping index: %v. Drop index will be retried in background.", indexerErr.Reason)
					logging.Errorf("LifecycleMgr.handleDeleteIndex(): %v", errStr)
					dropErr = errors.New(errStr)

				} else if !strings.Contains(err.Error(), "Unknown Index Instance") {
					errStr := fmt.Sprintf("Encountered error when dropping index: %v. Drop index will be retried in background.", err.Error())
					logging.Errorf("LifecycleMgr.handleDeleteIndex(): %v", errStr)
					dropErr = errors.New(errStr)
				}
			}
		}

		if dropErr != nil {
			return dropErr
		}
	}

	m.repo.DropIndexById(defn.DefnId)

	// If indexer crashes at this point, there is a chance topology may leave a orphan index
	// instance.  But this index will consider invalid (state=DELETED + no index definition).
	m.repo.deleteIndexFromTopology(defn.Bucket, defn.Scope, defn.Collection, defn.DefnId)

	logging.Debugf("LifecycleMgr.DeleteIndex() : deleted index:  bucket : %v bucket uuid %v scope %v collection %v name %v",
		defn.Bucket, defn.BucketUUID, defn.Scope, defn.Collection, defn.Name)
	return nil
}

//-----------------------------------------------------------
// Cleanup Index
//-----------------------------------------------------------

func (m *LifecycleMgr) handleCleanupIndexMetadata(content []byte) error {

	inst, err := common.UnmarshallIndexInst(content)
	if err != nil {
		logging.Errorf("LifecycleMgr.handleCleanupIndexMetadata() : Unable to unmarshall index definition. Reason = %v", err)
		return err
	}

	return m.DeleteIndexInstance(inst.Defn.DefnId, inst.InstId, false, false, false, nil)
}

//-----------------------------------------------------------
// Topology change (on index inst)
//-----------------------------------------------------------

func (m *LifecycleMgr) handleTopologyChange(content []byte) error {

	change := new(topologyChange)
	if err := json.Unmarshal(content, change); err != nil {
		return err
	}

	// Find index defnition
	defn, err := m.repo.GetIndexDefnById(common.IndexDefnId(change.DefnId))
	if err != nil {
		return err
	}
	if defn == nil {
		return nil
	}

	// Find out the current index instance state
	inst, err := m.FindLocalIndexInst(change.Bucket, change.Scope, change.Collection,
		common.IndexDefnId(change.DefnId), common.IndexInstId(change.InstId))
	if err != nil {
		return err
	}
	if inst == nil {
		return nil
	}

	state := inst.State
	scheduled := inst.Scheduled

	// update the index instance
	if err := m.UpdateIndexInstance(change.Bucket, change.Scope, change.Collection,
		common.IndexDefnId(change.DefnId), common.IndexInstId(change.InstId),
		common.IndexState(change.State), common.StreamId(change.StreamId), change.Error,
		change.BuildTime, change.RState, change.Partitions, change.Versions, change.InstVersion); err != nil {
		return err
	}

	// If the indexer changes the instance state changes from CREATED to READY, then send the index to the builder.
	// Lifecycle manager will not come to this code path when changing index state from CREATED to READY.
	if common.IndexState(state) == common.INDEX_STATE_CREATED &&
		common.IndexState(change.State) == common.INDEX_STATE_READY {
		if scheduled {
			m.builder.notifych <- defn
		}
	}

	return nil
}

//-----------------------------------------------------------
// Delete Bucket
//-----------------------------------------------------------

//
// Indexer will crash if this function returns an error.
// On bootstap, it will retry deleting the bucket again.
//
func (m *LifecycleMgr) handleDeleteBucket(bucket string, content []byte) error {

	result := error(nil)

	if len(content) == 0 {
		return errors.New("invalid argument")
	}

	streamId := common.StreamId(content[0])

	//
	// Remove index from repository
	//
	topologies, err := m.repo.GetTopologiesByBucket(bucket)
	if err == nil && len(topologies) > 0 {
		for _, topology := range topologies {
			definitions := make([]IndexDefnDistribution, len(topology.Definitions))
			copy(definitions, topology.Definitions)

			for _, defnRef := range definitions {

				if defn, err := m.repo.GetIndexDefnById(common.IndexDefnId(defnRef.DefnId)); err == nil && defn != nil {

					// Remove the index definition if:
					// 1) StreamId is NIL_STREAM (any stream)
					// 2) index has at least one inst matching the given stream
					// 3) index has at least one inst with NIL_STREAM

					if streamId == common.NIL_STREAM {
						if err := m.DeleteIndex(common.IndexDefnId(defn.DefnId), false, false, nil); err != nil {
							result = err
						}
						mc.DeleteAllCreateCommandToken(common.IndexDefnId(defn.DefnId))

					} else {
						for _, instRef := range defnRef.Instances {

							if common.StreamId(instRef.StreamId) == streamId || common.StreamId(instRef.StreamId) == common.NIL_STREAM {

								logging.Debugf("LifecycleMgr.handleDeleteBucket() : index instance : id %v, streamId %v.",
									instRef.InstId, instRef.StreamId)

								if err := m.DeleteIndex(common.IndexDefnId(defn.DefnId), false, false, nil); err != nil {
									result = err
								}
								mc.DeleteAllCreateCommandToken(common.IndexDefnId(defn.DefnId))

								break
							}
						}
					}
				} else {
					logging.Debugf("LifecycleMgr.handleDeleteBucket() : Cannot find index %v.  Skip.", defnRef.DefnId)
				}
			}
		}
	} else if err != fdb.FDB_RESULT_KEY_NOT_FOUND {
		result = err
	}

	//
	// Drop create token
	//
	result = m.deleteCreateTokenForBucket(bucket)

	return result
}

func (m *LifecycleMgr) deleteCreateTokenForBucket(bucket string) error {

	var result error

	entries, err := mc.ListCreateCommandToken()
	if err != nil {
		logging.Warnf("LifecycleMgr.handleDeleteBucket: Failed to fetch token from metakv.  Internal Error = %v", err)
		return err
	}

	for _, entry := range entries {

		defnId, requestId, err := mc.GetDefnIdFromCreateCommandTokenPath(entry)
		if err != nil {
			logging.Warnf("LifecycleMgr: Failed to process create index token %v.  Internal Error = %v.", entry, err)
			result = err
			continue
		}

		token, err := mc.FetchCreateCommandToken(defnId, requestId)
		if err != nil {
			logging.Warnf("LifecycleMgr: Failed to process create index token %v.  Internal Error = %v.", entry, err)
			result = err
			continue
		}

		if token != nil {
			for _, definitions := range token.Definitions {
				if len(definitions) > 0 && definitions[0].Bucket == bucket {
					if err := mc.DeleteCreateCommandToken(definitions[0].DefnId, requestId); err != nil {
						logging.Warnf("LifecycleMgr: Failed to delete create index token %v.  Internal Error = %v.", entry, err)
						result = err
					}
					break
				}
			}
		}
	}

	return result
}

//-----------------------------------------------------------
// Delete Collection
//-----------------------------------------------------------

func (m *LifecycleMgr) handleDeleteCollection(key string, content []byte) error {

	result := error(nil)

	input := strings.Split(key, "/")
	if len(input) != 3 {
		return errors.New(fmt.Sprintf("LifecycleMgr.handleDeleteCollection() invalid key %v", key))
	}
	bucket, scope, collection := input[0], input[1], input[2]

	if len(content) == 0 {
		return errors.New("invalid argument")
	}

	streamId := common.StreamId(content[0])

	//
	// Remove index from repository
	//
	topology, err := m.repo.GetTopologyByCollection(bucket, scope, collection)
	if err == nil && topology != nil {

		definitions := make([]IndexDefnDistribution, len(topology.Definitions))
		copy(definitions, topology.Definitions)

		for _, defnRef := range definitions {

			if defn, err := m.repo.GetIndexDefnById(common.IndexDefnId(defnRef.DefnId)); err == nil && defn != nil {

				// Remove the index definition if:
				// 1) StreamId is NIL_STREAM (any stream)
				// 2) index has at least one inst matching the given stream
				// 3) index has at least one inst with NIL_STREAM

				if streamId == common.NIL_STREAM {
					if err := m.DeleteIndex(common.IndexDefnId(defn.DefnId), false, false, nil); err != nil {
						result = err
					}
					mc.DeleteAllCreateCommandToken(common.IndexDefnId(defn.DefnId))

				} else {
					for _, instRef := range defnRef.Instances {

						if common.StreamId(instRef.StreamId) == streamId || common.StreamId(instRef.StreamId) == common.NIL_STREAM {

							logging.Debugf("LifecycleMgr.handleDeleteCollection() : index instance : id %v, streamId %v.",
								instRef.InstId, instRef.StreamId)

							if err := m.DeleteIndex(common.IndexDefnId(defn.DefnId), false, false, nil); err != nil {
								result = err
							}
							mc.DeleteAllCreateCommandToken(common.IndexDefnId(defn.DefnId))

							break
						}
					}
				}
			} else {
				logging.Debugf("LifecycleMgr.handleDeleteCollection() : Cannot find index %v.  Skip.", defnRef.DefnId)
			}
		}

	} else if err != fdb.FDB_RESULT_KEY_NOT_FOUND {
		result = err
	}

	//
	// Drop create token
	//
	result = m.deleteCreateTokenForCollection(bucket, scope, collection)

	return result
}

func (m *LifecycleMgr) deleteCreateTokenForCollection(bucket, scope, collection string) error {

	var result error

	entries, err := mc.ListCreateCommandToken()
	if err != nil {
		logging.Warnf("LifecycleMgr.deleteCreateTokenForCollection: Failed to fetch token from metakv.  Internal Error = %v", err)
		return err
	}

	for _, entry := range entries {

		defnId, requestId, err := mc.GetDefnIdFromCreateCommandTokenPath(entry)
		if err != nil {
			logging.Warnf("LifecycleMgr: Failed to process create index token %v.  Internal Error = %v.", entry, err)
			result = err
			continue
		}

		token, err := mc.FetchCreateCommandToken(defnId, requestId)
		if err != nil {
			logging.Warnf("LifecycleMgr: Failed to process create index token %v.  Internal Error = %v.", entry, err)
			result = err
			continue
		}

		if token != nil {
			for _, definitions := range token.Definitions {
				if len(definitions) > 0 {
					tokenScope, tokenCollection := definitions[0].Scope, definitions[0].Collection
					if tokenScope == "" {
						tokenScope = common.DEFAULT_SCOPE
					}
					if tokenCollection == "" {
						tokenCollection = common.DEFAULT_COLLECTION
					}
					if definitions[0].Bucket == bucket && tokenScope == scope && tokenCollection == collection {
						if err := mc.DeleteCreateCommandToken(definitions[0].DefnId, requestId); err != nil {
							logging.Warnf("LifecycleMgr: Failed to delete create index token %v.  Internal Error = %v.", entry, err)
							result = err
						}
						break
					}
				}
			}
		}
	}

	return result
}

//-----------------------------------------------------------
// Cleanup Index from invalid keyspace
//-----------------------------------------------------------

//
// Cleanup any deferred and active MAINT_STREAM indexes from invalid keyspace.
//
func (m *LifecycleMgr) handleCleanupIndexFromInvalidKeyspace(keyspace string) error {
	bucket, scope, collection := SplitKeyspaceId(keyspace)

	// Get bucket UUID.  if err==nil, bucket uuid is BUCKET_UUID_NIL for non-existent bucket.
	currentUUID, err := m.getBucketUUID(bucket, false)
	if err != nil {
		logging.Errorf("LifecycleMgr::handleCleanupIndexFromInvalidKeyspace Error while fetching bucketUUID from cinfocache, err: %v")
		// If err != nil, then cannot connect to fetch bucket info.  Do not attempt to delete index.
		return nil
	}

	collectionID, err := m.getCollectionID(bucket, scope, collection, false)
	if err != nil {
		logging.Errorf("LifecycleMgr::handleCleanupIndexFromInvalidKeyspace Error while fetching collectionID from cinfocache, err: %v")
		// If err != nil, then cannot connect to fetch collection info.  Do not attempt to delete index.
		return nil
	}
	logging.Infof("LifecycleMgr::handleCleanupIndexFromInvalidKeyspace cleaning up keyspace %v %v %v %v", bucket, scope, collection, collectionID)

	topology, err := m.repo.GetTopologyByCollection(bucket, scope, collection)
	if err == nil && topology != nil {
		// we will handle deferred indexes, active indexes on MAINT_STREAM
		// as well as any index with NIL_STREAM on invalid keyspace
		// we will not handled deleted indexs also indexes with INIT_STREAM should be handled
		// when that stream gets closed due to collection drop event recieved for that stream.
		deleteToken := false
		for _, defnRef := range topology.Definitions {
			if defn, err := m.repo.GetIndexDefnById(common.IndexDefnId(defnRef.DefnId)); err == nil && defn != nil {
				if defn.BucketUUID != currentUUID || defn.CollectionId != collectionID {
					for _, instRef := range defnRef.Instances {
						if instRef.State != uint32(common.INDEX_STATE_DELETED) &&
							(common.StreamId(instRef.StreamId) == common.MAINT_STREAM ||
								common.StreamId(instRef.StreamId) == common.NIL_STREAM) {
							if err := m.DeleteIndex(common.IndexDefnId(defn.DefnId), true, false, common.NewUserRequestContext()); err != nil {
								logging.Errorf("LifecycleMgr::handleCleanupIndexFromInvalidKeyspace: Encountered error %v", err)
								continue
							}
							deleteToken = true
							mc.DeleteAllCreateCommandToken(common.IndexDefnId(defn.DefnId))
							logging.Infof("LifecycleMgr::handleCleanupIndexFromInvalidKeyspace cleaning up index %v, %v, %v, %v, %v", common.IndexDefnId(defn.DefnId), defn.Name, defn.Bucket, defn.Scope, defn.Collection)
							break
						} else {
							logging.Infof("LifecycleMgr::handleCleanupIndexFromInvalidKeyspace skipping index inst %v, %v, %v, %v, %v, %v, %v, %v", instRef.InstId, instRef.State, instRef.StreamId, common.IndexDefnId(defn.DefnId), defn.Name, defn.Bucket, defn.Scope, defn.Collection)
						}
					}
				}
			}
		}
		// drop create tokens
		if deleteToken {
			m.deleteCreateTokenForCollection(bucket, scope, collection)
		}
	}
	return nil
}

//-----------------------------------------------------------
// Broadcast Stats
//-----------------------------------------------------------

func (m *LifecycleMgr) handleNotifyStats(buf []byte) {

	if len(buf) > 0 {
		stats := make(common.Statistics)
		if err := json.Unmarshal(buf, &stats); err == nil {
			m.stats.Set(&stats)
		} else {
			logging.Errorf("lifecycleMgr: fail to marshall index stats.  Error = %v", err)
		}
	}
}

// broadcastStats runs in a go routine and periodically broadcasts stats. If any buckets or indexes have changed since
// the prior boroadcast, it sends the full stats including the Indexes maps; otherwise it sends a version with the
// Indexes maps set to nil to save on comm payload, except that it will force a full resend every 5 minutes so new
// recipients can recover if their initial bootstrap request for full stats timed out or got corrupted.
func (m *LifecycleMgr) broadcastStats() {
	const RESEND_INTERVAL time.Duration = time.Duration(5 * time.Minute) // interval to force full resend

	// stats_manager refreshes progress stats every "client_stats_refresh_interval"
	// and this method also broadcasts stats for the same period. To avoid any race
	// condition between these two methods, sleep for 1 sec here so that
	// stats_manager is ahead and by the time lifecycle manager reads the stats
	// for broadcast, it contains latest stats
	time.Sleep(1 * time.Second)

	m.done.Add(1)
	defer m.done.Done()

	refreshMs := atomic.LoadUint64(&m.clientStatsRefreshInterval)
	refreshInterval := time.Duration(refreshMs) * time.Millisecond
	resendFullTicks := int64(RESEND_INTERVAL / refreshInterval) // # ticks before force full send

	ticker := time.NewTicker(refreshInterval)
	defer ticker.Stop()

	shortSends := int64(1) // number of sends since last complete set with Indexes maps
	for {
		select {
		case <-ticker.C:

			clusterVersion := common.GetClusterVersion()

			stats := m.stats.Get()
			if stats != nil {
				if clusterVersion < (int64)(common.INDEXER_70_VERSION) {
					idxStats := &client.IndexStats{Stats: *stats}
					if err := m.repo.BroadcastIndexStats(idxStats); err != nil {
						logging.Errorf("lifecycleMgr: fail to send index stats.  Error = %v", err)
					}
				} else {
					idxStats2 := convertToIndexStats2(*stats)
					var statsToBroadCast *client.IndexStats2
					if shortSends < resendFullTicks {
						statsToBroadCast = m.getDiffFromLastSent(idxStats2)
						shortSends++
					} else {
						statsToBroadCast = idxStats2
						shortSends = 1
					}

					m.clientStatsMutex.Lock()
					m.lastSendClientStats = idxStats2
					m.clientStatsMutex.Unlock()

					if err := m.repo.BroadcastIndexStats2(statsToBroadCast); err != nil {
						logging.Errorf("lifecycleMgr: fail to send indexStats2.  Error = %v", err)
					}
				}
			}

			refreshMsNew := atomic.LoadUint64(&m.clientStatsRefreshInterval)
			if refreshMs != refreshMsNew {
				refreshMs = refreshMsNew
				refreshInterval = time.Duration(refreshMs) * time.Millisecond
				resendFullTicks = int64(RESEND_INTERVAL / refreshInterval)

				ticker.Stop()
				ticker = time.NewTicker(refreshInterval)
			}

		case <-m.killch:
			// lifecycle manager shutdown
			logging.Infof("LifecycleMgr.broadcastStats(): received kill signal. Shutting down broadcastStats routine.")
			return
		}
	}
}

// getDiffFromLastSent returns a set of stats to broadcast. If there has been any change in buckets
// or indexes per bucket since prior send, it returns the input currStats (full set of stats), else
// it returns a version with the Indexes maps all set to nil to save on communication payload.
//
// TODO: This method can further be optimized to broadcast only incremental changes.
// The IndexStats2 data structure must be modified to contain deleted indexes list
// and current indexes map with storage stats
func (m *LifecycleMgr) getDiffFromLastSent(currStats *client.IndexStats2) *client.IndexStats2 {
	m.clientStatsMutex.Lock()
	defer m.clientStatsMutex.Unlock()

	if len(m.lastSendClientStats.Stats) != len(currStats.Stats) {
		return currStats
	}

	statsToBroadCast := &client.IndexStats2{}
	statsToBroadCast.Stats = make(map[string]*client.DedupedIndexStats)

	for bucket, lastSentDeduped := range m.lastSendClientStats.Stats {
		if currDeduped, ok := currStats.Stats[bucket]; !ok {
			return currStats
		} else {
			if len(currDeduped.Indexes) != len(lastSentDeduped.Indexes) {
				return currStats
			}
			for indexName, _ := range lastSentDeduped.Indexes {
				if _, ok := currDeduped.Indexes[indexName]; !ok {
					return currStats
				}
			}
		}
		statsToBroadCast.Stats[bucket] = &client.DedupedIndexStats{}
		statsToBroadCast.Stats[bucket].NumDocsPending = currStats.Stats[bucket].NumDocsPending
		statsToBroadCast.Stats[bucket].NumDocsQueued = currStats.Stats[bucket].NumDocsQueued
		statsToBroadCast.Stats[bucket].LastRollbackTime = currStats.Stats[bucket].LastRollbackTime
		statsToBroadCast.Stats[bucket].ProgressStatTime = currStats.Stats[bucket].ProgressStatTime
		statsToBroadCast.Stats[bucket].Indexes = nil
	}

	return statsToBroadCast
}

// This method takes in common.Statistics as input,
// de-duplicates the stats and converts them to client.IndexStats2 format
func convertToIndexStats2(stats common.Statistics) *client.IndexStats2 {

	// Returns the fully qualified index name and bucket name
	getIndexAndBucketName := func(indexName string) (string, string) {
		split := strings.Split(indexName, ":")
		return strings.Join(split[0:len(split)-1], ":"), split[0]
	}

	clearIndexFromStats := func(indexName string) {
		delete(stats, indexName+":num_docs_pending")
		delete(stats, indexName+":num_docs_queued")
		delete(stats, indexName+":last_rollback_time")
		delete(stats, indexName+":progress_stat_time")
		delete(stats, indexName+":index_state")
	}

	indexStats2 := &client.IndexStats2{}
	indexStats2.Stats = make(map[string]*client.DedupedIndexStats)

	for key, _ := range stats {
		indexName, bucketName := getIndexAndBucketName(key)
		if status, ok := stats[indexName+":index_state"]; ok {
			if status.(float64) != (float64)(common.INDEX_STATE_ACTIVE) {
				// Delete all stats beloning to this index as index is not ready for scans
				clearIndexFromStats(indexName)
			} else {
				if _, ok := indexStats2.Stats[bucketName]; !ok {
					indexStats2.Stats[bucketName] = &client.DedupedIndexStats{}
					indexStats2.Stats[bucketName].Indexes = make(map[string]*client.PerIndexStats)
				}

				indexStats2.Stats[bucketName].NumDocsPending = stats[indexName+":num_docs_pending"].(float64)
				indexStats2.Stats[bucketName].NumDocsQueued = stats[indexName+":num_docs_queued"].(float64)
				indexStats2.Stats[bucketName].LastRollbackTime = stats[indexName+":last_rollback_time"].(string)
				indexStats2.Stats[bucketName].ProgressStatTime = stats[indexName+":progress_stat_time"].(string)
				indexStats2.Stats[bucketName].Indexes[indexName] = nil

				clearIndexFromStats(indexName)
			}
		}
	}
	return indexStats2
}

//-----------------------------------------------------------
// Client Stats
//-----------------------------------------------------------
func (m *LifecycleMgr) handleClientStats(content []byte) ([]byte, error) {
	m.clientStatsMutex.Lock()
	defer m.clientStatsMutex.Unlock()

	return client.MarshallIndexStats2(m.lastSendClientStats)
}

//-----------------------------------------------------------
// Reset Index (for upgrade)
//-----------------------------------------------------------

func (m *LifecycleMgr) handleResetIndex(content []byte) error {

	inst, err := common.UnmarshallIndexInst(content)
	if err != nil {
		logging.Errorf("LifecycleMgr.handleResetIndex() : Unable to unmarshall index instance. Reason = %v", err)
		return err
	}

	logging.Infof("LifecycleMgr.handleResetIndex() : Reset Index %v", inst.InstId)

	defn := &inst.Defn
	oldDefn, err := m.repo.GetIndexDefnById(defn.DefnId)
	if err != nil {
		logging.Warnf("LifecycleMgr.handleResetIndex(): Failed to find index definition (%v, %v).", defn.DefnId, defn.Bucket)
		return err
	}

	oldStorageMode := ""
	if oldDefn != nil {
		oldStorageMode = string(oldDefn.Using)
	}

	//
	// Change storage mode in index definition
	//

	if err := m.repo.UpdateIndex(defn); err != nil {
		logging.Errorf("LifecycleMgr.handleResetIndex() : Fails to upgrade index (%v, %v, %v, %v). Reason = %v",
			defn.Bucket, defn.Scope, defn.Collection, defn.Name, err)
		return err
	}

	//
	// Restore index instance (as if index is created again)
	//

	topology, err := m.repo.CloneTopologyByCollection(defn.Bucket, defn.Scope, defn.Collection)
	if err != nil {
		logging.Errorf("LifecycleMgr.handleResetIndex() : Fails to upgrade index (%v, %v, %v, %v). Reason = %v",
			defn.Bucket, defn.Scope, defn.Collection, defn.Name, err)
		return err
	}

	rinst, err := m.FindLocalIndexInst(defn.Bucket, defn.Scope, defn.Collection, defn.DefnId, inst.InstId)
	if err != nil {
		logging.Errorf("LifecycleMgr.handleResetIndex() : Fails to upgrade index (%v, %v, %v, %v). Reason = %v",
			defn.Bucket, defn.Scope, defn.Collection, defn.Name, err)
		return err
	}

	if rinst == nil {
		logging.Errorf("LifecycleMgr.handleResetIndex() : Fails to upgrade index (%v, %v, %v, %v). Index instance does not exist.",
			defn.Bucket, defn.Scope, defn.Collection, defn.Name)
		return nil
	}

	if common.IndexState(rinst.State) == common.INDEX_STATE_INITIAL ||
		common.IndexState(rinst.State) == common.INDEX_STATE_CATCHUP ||
		common.IndexState(rinst.State) == common.INDEX_STATE_ACTIVE {

		topology.UpdateScheduledFlagForIndexInst(defn.DefnId, inst.InstId, true)
	}

	topology.UpdateOldStorageModeForIndexInst(defn.DefnId, inst.InstId, oldStorageMode)
	topology.UpdateStorageModeForIndexInst(defn.DefnId, inst.InstId, string(defn.Using))
	topology.UpdateStateForIndexInst(defn.DefnId, inst.InstId, common.INDEX_STATE_READY)
	topology.SetErrorForIndexInst(defn.DefnId, inst.InstId, "")
	topology.UpdateStreamForIndexInst(defn.DefnId, inst.InstId, common.NIL_STREAM)

	if err := m.repo.SetTopologyByCollection(defn.Bucket, defn.Scope, defn.Collection, topology); err != nil {
		// Topology update is in place.  If there is any error, SetTopologyByCollection will purge the cache copy.
		logging.Errorf("LifecycleMgr.handleResetIndex() : index instance (%v, %v, %v, %v) update fails. Reason = %v",
			defn.Bucket, defn.Scope, defn.Collection, defn.Name, err)
		return err
	}

	return nil
}

//-----------------------------------------------------------
// Reset Index (for rollback)
//-----------------------------------------------------------

func (m *LifecycleMgr) handleResetIndexOnRollback(content []byte) error {

	inst, err := common.UnmarshallIndexInst(content)
	if err != nil {
		logging.Errorf("LifecycleMgr.handleResetIndexOnRollback() : Unable to unmarshall index instance. Reason = %v", err)
		return err
	}

	logging.Infof("LifecycleMgr.handleResetIndexOnRollback() : Reset Index %v", inst.InstId)

	defn := &inst.Defn

	//
	// Restore index instance (as if index is created again)
	//

	topology, err := m.repo.CloneTopologyByCollection(defn.Bucket, defn.Scope, defn.Collection)
	if err != nil {
		logging.Errorf("LifecycleMgr.handleResetIndexOnRollback() : Fails to reset index (%v, %v, %v, %v). Reason = %v",
			defn.Bucket, defn.Scope, defn.Collection, defn.Name, err)
		return err
	}

	rinst, err := m.FindLocalIndexInst(defn.Bucket, defn.Scope, defn.Collection, defn.DefnId, inst.InstId)
	if err != nil {
		logging.Errorf("LifecycleMgr.handleResetIndexOnRollback() : Fails to reset index (%v, %v, %v, %v). Reason = %v",
			defn.Bucket, defn.Scope, defn.Collection, defn.Name, err)
		return err
	}

	if rinst == nil {
		logging.Errorf("LifecycleMgr.handleResetIndexOnRollback() : Fails to reset index (%v, %v, %v, %v). Index instance does not exist.",
			defn.Bucket, defn.Scope, defn.Collection, defn.Name)
		return nil
	}

	//reset only for active state index. If index gets deleted, it doesn't need to be reset.
	if common.IndexState(rinst.State) == common.INDEX_STATE_ACTIVE {
		topology.UpdateScheduledFlagForIndexInst(defn.DefnId, inst.InstId, true)

		topology.UpdateStateForIndexInst(defn.DefnId, inst.InstId, common.INDEX_STATE_READY)
		topology.SetErrorForIndexInst(defn.DefnId, inst.InstId, "")
		topology.UpdateStreamForIndexInst(defn.DefnId, inst.InstId, common.NIL_STREAM)

		if err := m.repo.SetTopologyByCollection(defn.Bucket, defn.Scope, defn.Collection, topology); err != nil {
			// Topology update is in place.  If there is any error, SetTopologyByCollection will purge the cache copy.
			logging.Errorf("LifecycleMgr.handleResetIndexOnRollback() : index instance (%v, %v, %v, %v) update fails. Reason = %v",
				defn.Bucket, defn.Scope, defn.Collection, defn.Name, err)
			return err
		}

		//notify builder
		m.builder.notifych <- defn
	}
	return nil
}

//-----------------------------------------------------------
// Indexer Config update
//-----------------------------------------------------------

func (m *LifecycleMgr) handleConfigUpdate(content []byte) error {

	config := new(common.Config)
	if err := json.Unmarshal(content, config); err != nil {
		return err
	}

	m.builder.configUpdate(config)

	if val, ok := (*config)["client_stats_refresh_interval"]; ok {
		atomic.StoreUint64(&m.clientStatsRefreshInterval, uint64(val.Float64()))
	}

	return nil
}

//-----------------------------------------------------------
// Create Index Instance
//-----------------------------------------------------------

func (m *LifecycleMgr) CreateIndexInstance(defn *common.IndexDefn, scheduled bool,
	reqCtx *common.MetadataRequestContext, asyncCreate bool) error {

	/////////////////////////////////////////////////////
	// Verify input parameters
	/////////////////////////////////////////////////////

	if err := m.verifyOverlapPartition(defn, reqCtx); err != nil {
		return err
	}

	if err := m.setBucketUUID(defn); err != nil {
		return err
	}

	if err := m.setScopeIdAndCollectionId(defn); err != nil {
		return err
	}

	if err := m.setStorageMode(defn); err != nil {
		return err
	}

	if err := m.setImmutable(defn); err != nil {
		return err
	}

	instId, realInstId, err := m.setInstId(defn)
	if err != nil {
		return err
	}

	replicaId := m.setReplica(defn)

	partitions, versions, numPartitions := m.setPartition(defn)

	if realInstId != 0 {
		realInst, err := m.FindLocalIndexInst(defn.Bucket, defn.Scope, defn.Collection, defn.DefnId, realInstId)
		if err != nil {
			logging.Errorf("LifecycleMgr.CreateIndexInstance() : CreateIndexInstance fails. Reason = %v", err)
			return err
		}

		// Lifeccyle mgr is the source of truth of metadata.  If it sees that there is an existing index instance in
		// DELETED state, it means that indexer has not been able to delete this instance.  Try to delete it now before
		// creating an identical instance.
		if realInst != nil && common.IndexState(realInst.State) == common.INDEX_STATE_DELETED {
			logging.Infof("LifecycleMgr.CreateIndexInstance() : Remove deleted index instance %v", realInst.InstId)
			if err := m.DeleteIndexInstance(defn.DefnId, realInstId, true, false, true, reqCtx); err != nil {
				logging.Errorf("LifecycleMgr.CreateIndexInstance() : CreateIndexInstance fails while trying to delete old index instance. Reason = %v", err)
				return err
			}
			realInst = nil
		}

		if realInst == nil {
			instId = realInstId
			realInstId = 0
		}
	}

	/////////////////////////////////////////////////////
	// Create Index Metadata
	/////////////////////////////////////////////////////

	// Create index instance
	// If there is any dangling index instance of the same index name and bucket, this will first
	// remove the dangling index instance first.
	// If indexer crash during creation of index instance, there could be a dangling index definition.
	// It is possible to create index of the same name later, as long as the new index has a different
	// definition id, since an index is consider valid only if it has both index definiton and index instance.
	// So the dangling index definition is considered invalid.
	if err := m.repo.addInstanceToTopology(defn, instId, replicaId, partitions, versions, numPartitions, realInstId, !defn.Deferred && scheduled); err != nil {
		logging.Errorf("LifecycleMgr.CreateIndexInstance() : CreateIndexInstance fails. Reason = %v", err)
		return err
	}

	/////////////////////////////////////////////////////
	// Create Index in Indexer
	/////////////////////////////////////////////////////

	// At this point, an index is created successfully with state=CREATED.  If indexer crashes now,
	// indexer will repair the index upon bootstrap (either cleanup or move to READY state).
	// If indexer returns an error when creating index, then cleanup metadata now.    During metadata cleanup,
	// errors are ignored.  This means the following:
	// 1) Index Definition is not deleted due to error from metadata repository.   The index will be repaired
	//    during indexer bootstrap or implicit dropIndex.
	// 2) Index definition is deleted.  This effectively "delete index".
	if m.notifier != nil {
		if err := m.notifier.OnIndexCreate(defn, instId, replicaId, partitions, versions, numPartitions, realInstId, reqCtx); err != nil {
			logging.Errorf("LifecycleMgr.CreateIndexInstance() : CreateIndexInstance fails. Reason = %v", err)
			m.DeleteIndexInstance(defn.DefnId, instId, false, false, false, reqCtx)
			return err
		}
	}

	/////////////////////////////////////////////////////
	// Update Index State
	/////////////////////////////////////////////////////

	// If cannot move the index to READY state, then abort create index by cleaning up the metadata.
	// Metadata cleanup is not atomic.  The index is effectively "deleted" if it is able to drop
	// the index definition from repository. If drop index is not successful during cleanup,
	// the index will be repaired upon bootstrap or cleanup by janitor.
	if err := m.updateIndexState(defn.Bucket, defn.Scope, defn.Collection, defn.DefnId, instId, common.INDEX_STATE_READY); err != nil {
		logging.Errorf("LifecycleMgr.CreateIndexInstance() : CreateIndexInstance fails. Reason = %v", err)
		m.DeleteIndexInstance(defn.DefnId, instId, false, false, false, reqCtx)
		return err
	}

	instState := m.getInstStateFromTopology(defn.Bucket, defn.Scope, defn.Collection, defn.DefnId, instId)
	if instState != common.INDEX_STATE_READY {
		logging.Fatalf("LifecycleMgr.CreateIndex(): Instance state is not INDEX_STATE_READY. Instance: %v (%v, %v, %v, %v). "+
			"Instance state in topology: %v", instId, defn.Bucket, defn.Scope, defn.Collection, defn.DefnId, instState)
	}

	/////////////////////////////////////////////////////
	// Build Index
	/////////////////////////////////////////////////////

	// Run index build
	if !defn.Deferred && scheduled && !asyncCreate {
		if m.notifier != nil {
			logging.Debugf("LifecycleMgr.CreateIndexInstance() : start Index Build")

			retryList, skipList, errList, _ := m.buildIndexesLifecycleMgr([]common.IndexDefnId{defn.DefnId}, reqCtx, false)

			if len(retryList) != 0 {
				return errors.New("Failed to build index.  Index build will be retried in background.")
			}

			if len(errList) != 0 {
				logging.Errorf("LifecycleMgr.CreateIndexInstance() : build index fails.  Reason = %v", errList[0])
				m.DeleteIndexInstance(defn.DefnId, instId, false, false, false, reqCtx)
				return errList[0]
			}

			if len(skipList) != 0 {
				logging.Errorf("LifecycleMgr.CreateIndexInstance() : build index fails due to internal errors.")
				m.DeleteIndexInstance(defn.DefnId, instId, false, false, false, reqCtx)
				return errors.New("Failed to create index due to internal build error.  Please retry the operation.")
			}
		}
	}

	logging.Debugf("LifecycleMgr.CreateIndexInstance() : CreateIndexInstance completes")

	return nil
}

func (m *LifecycleMgr) verifyOverlapPartition(defn *common.IndexDefn, reqCtx *common.MetadataRequestContext) error {

	isRebalance := reqCtx.ReqSource == common.DDLRequestSourceRebalance
	isRebalancePartition := isRebalance && common.IsPartitioned(defn.PartitionScheme)

	if isRebalancePartition {

		if defn.RealInstId == 0 {
			err := errors.New("Missing real defn id when rebalancing partitioned index")
			logging.Errorf("LifecycleMgr.CreateIndexInstance() : createIndex fails. Reason: %v", err)
			return err
		}

		existDefn, err := m.repo.GetIndexDefnByName(defn.Bucket, defn.Scope, defn.Collection, defn.Name)
		if err != nil {
			logging.Errorf("LifecycleMgr.CreateIndexInstance() : createIndex fails. Reason = %v", err)
			return err
		}

		if existDefn != nil {
			// The index already exist in this node.  Make sure there is no overlapping partition.
			topology, err := m.repo.GetTopologyByCollection(existDefn.Bucket, existDefn.Scope, existDefn.Collection)
			if err != nil {
				logging.Errorf("LifecycleMgr.CreateIndexInstance() : fails to find index instance. Reason = %v", err)
				return err
			}

			if topology != nil {
				insts := topology.GetIndexInstancesByDefn(existDefn.DefnId)

				// Go through each partition that we want to create for this index instance
				for i, partnId := range defn.Partitions {
					for _, inst := range insts {

						// Guard against duplicate index instance id
						if common.IndexInstId(inst.InstId) == defn.InstId {
							err := fmt.Errorf("Found Duplicate instance %v already existed in index.", inst.InstId)
							logging.Errorf("LifecycleMgr.CreateIndexInstance() : createIndex fails. Reason: %v", err)
							return err
						}

						// check for duplicate partition in real instance or proxy instance
						if common.IndexInstId(inst.InstId) == defn.RealInstId ||
							common.IndexInstId(inst.RealInstId) == defn.RealInstId {

							// Guard against duplicate partition in ALL instances, except DELETED instance.
							for _, partn := range inst.Partitions {
								if common.PartitionId(partn.PartId) == partnId &&
									common.IndexState(inst.State) != common.INDEX_STATE_DELETED {

									// 1) Consider all REBAL_ACTIVE and REBAL_MERGED instances
									// 2) For REBAL_PENDING instances, only consider those that have the same or higher rebalance version.
									//    During rebalancing, new instance can be created.  But rebalancing could start before cleanup
									//    (on previous rebalance attempt) is finished.  Therefore, it is possible that there will be
									//    some REBAL_PENDING instances from previous rebalancing. But those REBAL_PENDING instances
									//    would have lower version numbers.   We can skip those REBAL_PENDING instances since they will
									//    eventually been cleaned up. Note that instance version is increasing for every rebalance.
									if common.RebalanceState(inst.RState) == common.REBAL_MERGED ||
										common.RebalanceState(inst.RState) == common.REBAL_ACTIVE ||
										int(partn.Version) >= defn.Versions[i] {
										err := fmt.Errorf("Found overlapping partition when rebalancing.  Instance %v partition %v.", inst.InstId, partnId)
										logging.Errorf("LifecycleMgr.CreateIndexInstance() : createIndex fails. Reason: %v", err)
										return err
									}
								}
							}
						}
					}
				}
			}
		}
	}

	return nil
}

//-----------------------------------------------------------
// Delete Index Instance
//-----------------------------------------------------------

func (m *LifecycleMgr) handleDropInstance(content []byte, reqCtx *common.MetadataRequestContext) error {

	change := new(dropInstance)
	if err := json.Unmarshal(content, change); err != nil {
		return err
	}

	return m.DeleteIndexInstance(change.Defn.DefnId, change.Defn.InstId, change.Notify, change.UpdateStatusOnly, false, reqCtx)
}

func (m *LifecycleMgr) handleDeleteOrPruneIndexInstance(content []byte, reqCtx *common.MetadataRequestContext) error {

	change := new(dropInstance)
	if err := json.Unmarshal(content, change); err != nil {
		return err
	}

	return m.DeleteOrPruneIndexInstance(change.Defn, change.Notify, change.UpdateStatusOnly, change.DeletedOnly, reqCtx)
}

//
// DeleteOrPruneIndexInstance either delete index, delete instance or prune instance, depending on metadata state and
// given index definition.   This operation is idempotent.   Caller (e.g. rebalancer) can retry this operation until
// successful.    If this operation returns successfully, it means that
// 1) metadata is cleaned up
// 2) indexer internal data structure is cleaned up
// 3) index data files are cleaned up asynchronously.
// 4) projector stream is cleaned up asyncrhronously.
//
// Note that if a new create index instance request comes before previously created index files are removed, the
// indexer may crash.  This can happen during rebalance:
// 1) partitioned index can use the same index inst id
// 2) index data file uses index inst id
// After indexer crash, rebalancer recovery logic will kick in to remove the newly created index.
//
// For projector, stream operation is serialized.  So stream request for new index cannot proceed until the delete request
// has processed.
//
func (m *LifecycleMgr) DeleteOrPruneIndexInstance(defn common.IndexDefn, notify bool, updateStatusOnly bool, deletedOnly bool,
	reqCtx *common.MetadataRequestContext) error {

	id := defn.DefnId
	instId := defn.InstId

	logging.Infof("LifecycleMgr.DeleteOrPruneIndexInstance() : index defnId %v instance id %v real instance id %v partitions %v",
		id, instId, defn.RealInstId, defn.Partitions)

	stream := common.NIL_STREAM

	inst, err := m.FindLocalIndexInst(defn.Bucket, defn.Scope, defn.Collection, id, instId)
	if err != nil {
		logging.Errorf("LifecycleMgr.DeleteOrPruneIndexInstance() : Encountered error during delete index. Error = %v", err)
		return err
	}

	if inst == nil {
		inst, err := m.FindLocalIndexInst(defn.Bucket, defn.Scope, defn.Collection, id, defn.RealInstId)
		if err != nil {
			logging.Errorf("LifecycleMgr.DeleteOrPruneIndexInstance() : Encountered error during delete index. Error = %v", err)
			return err
		}

		if inst == nil {
			return nil
		}
		instId = common.IndexInstId(inst.InstId)
		stream = common.StreamId(inst.StreamId)

	} else {
		stream = common.StreamId(inst.StreamId)
	}

	if deletedOnly && inst.State != uint32(common.INDEX_STATE_DELETED) {
		return nil
	}

	if len(defn.Partitions) == 0 || stream == common.INIT_STREAM {
		// 1) If this is coming from drop index, or
		// 2) It is from INIT_STREAM (cannot prune on INIT_STREAM)
		return m.DeleteIndexInstance(id, instId, notify, updateStatusOnly, false, reqCtx)
	}

	return m.PruneIndexInstance(id, instId, defn.Partitions, notify, updateStatusOnly, reqCtx)
}

func (m *LifecycleMgr) DeleteIndexInstance(id common.IndexDefnId, instId common.IndexInstId, notify bool,
	updateStatusOnly bool, instanceOnly bool, reqCtx *common.MetadataRequestContext) error {

	logging.Infof("LifecycleMgr.DeleteIndexInstance() : index defnId %v instance id %v", id, instId)

	defn, err := m.repo.GetIndexDefnById(id)
	if err != nil {
		logging.Errorf("LifecycleMgr.DeleteIndexInstance() : drop index fails for index defn %v.  Error = %v.", id, err)
		return err
	}
	if defn == nil {
		logging.Infof("LifecycleMgr.DeleteIndexInstance() : index %v does not exist.", id)
		return nil
	}

	insts, err := m.findAllLocalIndexInst(defn.Bucket, defn.Scope, defn.Collection, id)
	if err != nil {
		// Cannot read bucket topology.  Likely a transient error.  But without index inst, we cannot
		// proceed without letting indexer to clean up.
		logging.Warnf("LifecycleMgr.DeleteIndexInstance() : Encountered error during delete index. Error = %v", err)
		return err
	}

	var validInst int
	var inst *IndexInstDistribution
	for _, n := range insts {
		if common.IndexInstId(n.InstId) == instId {
			temp := n
			inst = &temp
		} else if n.State != uint32(common.INDEX_STATE_DELETED) {
			// any valid instance outside of the one being deleted?
			validInst++
		}
	}

	// no match index instance to delete
	if inst == nil {
		return nil
	}

	// This is no other instance to delete.  Delete the index itself.
	if validInst == 0 && !instanceOnly {
		logging.Infof("LifecycleMgr.DeleteIndexInstance() : there is only a single instance.  Delete index %v", id)
		return m.DeleteIndex(id, notify, updateStatusOnly, reqCtx)
	}

	// updateIndexState will not return an error if there is no index inst
	if err := m.updateIndexState(defn.Bucket, defn.Scope, defn.Collection, id, instId, common.INDEX_STATE_DELETED); err != nil {
		logging.Errorf("LifecycleMgr.DeleteIndexInstance() : fails to update index state to DELETED. Reason = %v", err)
		return err
	}

	if updateStatusOnly {
		return nil
	}

	if notify && m.notifier != nil {
		// Note that the phsyical index is removed asynchronously by the indexer.
		if err := m.notifier.OnIndexDelete(instId, defn.Bucket, reqCtx); err != nil {
			indexerErr, ok := err.(*common.IndexerError)
			if ok && indexerErr.Code != common.IndexNotExist {
				logging.Errorf("LifecycleMgr.DeleteIndexInstance(): Encountered error when dropping index: %v.",
					indexerErr.Reason)
				return err

			} else if !strings.Contains(err.Error(), "Unknown Index Instance") {
				logging.Errorf("LifecycleMgr.handleDeleteInstance(): Encountered error when dropping index: %v.",
					err.Error())
				return err
			}
		}
	}

	// Remove the index instance from metadata.
	m.repo.deleteInstanceFromTopology(defn.Bucket, defn.Scope, defn.Collection, id, instId)

	// Remove the real proxy if necessary
	m.deleteRealIndexInstIfNecessary(defn, inst, notify, updateStatusOnly, instanceOnly, reqCtx)

	return nil
}

func (m *LifecycleMgr) deleteRealIndexInstIfNecessary(defn *common.IndexDefn, inst *IndexInstDistribution,
	notify bool, updateStatusOnly bool, instanceOnly bool, reqCtx *common.MetadataRequestContext) error {

	// There is no real inst
	if inst.RealInstId == 0 || inst.InstId == inst.RealInstId {
		return nil
	}

	// Find the real inst
	realInst, err := m.FindLocalIndexInst(defn.Bucket, defn.Scope, defn.Collection, defn.DefnId, common.IndexInstId(inst.RealInstId))
	if err != nil {
		logging.Errorf("LifecycleMgr.deleteRealIndexInstIfNecessary() : Encountered error during delete index. Error = %v", err)
		return err
	}

	// Do nothing if the real inst is already gone
	if realInst == nil {
		return nil
	}

	// Find if there is any proxy depends on real instance.  This only covers proxy that has yet
	// to be merged to this instance.
	numProxy, err := m.findNumValidProxy(defn.Bucket, defn.Scope, defn.Collection, defn.DefnId, common.IndexInstId(realInst.InstId))
	if err != nil {
		return err
	}

	// If there is no proxy waiting to be merged to the real instance and real inst has no partition,
	// then delete the real instance itself.  If this happens during bootstrap when the proxy is being
	// cleanup, then just mark the real instance as deleted.  The janitor will clean up this index eventually.
	if numProxy == 0 && len(realInst.Partitions) == 0 {
		logging.Infof("LifecycleMgr.deleteRealIndexInstIfNecessary() : Removing empty real inst %v", realInst.InstId)
		return m.DeleteIndexInstance(defn.DefnId, common.IndexInstId(realInst.InstId), notify, !notify, instanceOnly, reqCtx)
	}

	return nil
}

//-----------------------------------------------------------
// Merge Partition
//-----------------------------------------------------------

func (m *LifecycleMgr) handleMergePartition(content []byte, reqCtx *common.MetadataRequestContext) error {

	change := new(mergePartition)
	if err := json.Unmarshal(content, change); err != nil {
		return err
	}

	return m.MergePartition(common.IndexDefnId(change.DefnId), common.IndexInstId(change.SrcInstId),
		common.RebalanceState(change.SrcRState), common.IndexInstId(change.TgtInstId), change.TgtPartitions, change.TgtVersions,
		change.TgtInstVersion, reqCtx)
}

func (m *LifecycleMgr) MergePartition(id common.IndexDefnId, srcInstId common.IndexInstId, srcRState common.RebalanceState,
	tgtInstId common.IndexInstId, tgtPartitions []uint64, tgtVersions []int, tgtInstVersion uint64, reqCtx *common.MetadataRequestContext) error {

	logging.Infof("LifecycleMgr.MergePartition() : index defnId %v source %v target %v", id, srcInstId, tgtInstId)

	defn, err := m.repo.GetIndexDefnById(id)
	if err != nil {
		logging.Errorf("LifecycleMgr.MergePartition() : merge partition fails for index defn %v.  Error = %v.", id, err)
		return err
	}
	if defn == nil {
		logging.Infof("LifecycleMgr.MergePartition() : index %v does not exist.", id)
		return nil
	}

	// Check if the source inst still exist
	inst, err := m.FindLocalIndexInst(defn.Bucket, defn.Scope, defn.Collection, id, srcInstId)
	if err != nil {
		// Cannot read bucket topology.  Likely a transient error.  But without index inst, we cannot
		// proceed without letting indexer to clean up.
		logging.Errorf("LifecycleMgr.MergePartition() : Encountered error during merge index. Error = %v", err)
		return err
	}
	if inst == nil || inst.State == uint32(common.INDEX_STATE_DELETED) {
		// no match index instance to merge
		return nil
	}

	// Check if the target inst still exist
	inst, err = m.FindLocalIndexInst(defn.Bucket, defn.Scope, defn.Collection, id, tgtInstId)
	if err != nil {
		// Cannot read bucket topology.  Likely a transient error.  But without index inst, we cannot
		// proceed without letting indexer to clean up.
		logging.Errorf("LifecycleMgr.MergePartition() : Encountered error during merge index. Error = %v", err)
		return err
	}
	if inst == nil || inst.State == uint32(common.INDEX_STATE_DELETED) {
		// no match index instance to merge
		return nil
	}

	indexerId, err := m.repo.GetLocalIndexerId()
	if err != nil {
		logging.Errorf("LifecycleMgr.MergePartition() : Failed to find indexerId. Reason = %v", err)
		return err
	}

	m.repo.mergePartitionFromTopology(string(indexerId), defn.Bucket, defn.Scope, defn.Collection, id, srcInstId, srcRState, tgtInstId, tgtInstVersion,
		tgtPartitions, tgtVersions)

	return nil
}

//-----------------------------------------------------------
// Prune Partition
//-----------------------------------------------------------

func (m *LifecycleMgr) PruneIndexInstance(id common.IndexDefnId, instId common.IndexInstId, partitions []common.PartitionId,
	notify bool, updateStatusOnly bool, reqCtx *common.MetadataRequestContext) error {

	logging.Infof("LifecycleMgr.PruneIndexPartition() : index defnId %v instance %v partitions %v", id, instId, partitions)

	defn, err := m.repo.GetIndexDefnById(id)
	if err != nil {
		logging.Errorf("LifecycleMgr.PruneIndexInstance() : prune index fails for index defn %v.  Error = %v.", id, err)
		return err
	}
	if defn == nil {
		logging.Infof("LifecycleMgr.PruneIndexInstance() : index %v does not exist.", id)
		return nil
	}

	inst, err := m.FindLocalIndexInst(defn.Bucket, defn.Scope, defn.Collection, id, instId)
	if err != nil {
		// Cannot read bucket topology.  Likely a transient error.  But without index inst, we cannot
		// proceed without letting indexer to clean up.
		logging.Errorf("LifecycleMgr.PruneIndexInstance() : Encountered error during prune index. Error = %v", err)
		return err
	}
	if inst == nil {
		// no match index instance to delete
		return nil
	}

	newPartitions := make([]common.PartitionId, 0, len(partitions))
	for _, partitionId := range partitions {
		for _, partition := range inst.Partitions {
			if partition.PartId == uint64(partitionId) {
				newPartitions = append(newPartitions, partitionId)
				break
			}
		}
	}

	// Find if there is any proxy depends on this instance.  This only covers proxy that has yet
	// to be merged to this instance.
	numProxy, err := m.findNumValidProxy(defn.Bucket, defn.Scope, defn.Collection, id, instId)
	if err != nil {
		return err
	}

	// If there is no proxy waiting to be merged to this instance and all partitions are to be pruned,
	// then delete this instance itself.
	if numProxy == 0 && (len(newPartitions) == len(inst.Partitions) || len(inst.Partitions) == 0) {
		return m.DeleteIndexInstance(id, instId, notify, updateStatusOnly, false, reqCtx)
	}

	//
	// Prune the partition from the index instance in metadata.
	// A DELETED inst (tombstone) will be created to contain the pruned partition.  The tombstone will have RState = REBAL_PENDING_DELETE.
	// The tombstone is for recovery purpose only.   If the indexer crashes after metadata is updated, the tombstone will
	// get cleaned up by removing the pruned partition data during indexer bootstrap.  The slice file will also get cleanup during
	// crash recovery.
	// When a index instance is merged or created, it has to remove its partition from tombstone to ensure that tombstone does not
	// accidently delete the instance.
	//
	if len(newPartitions) == 0 {
		logging.Errorf("LifecycleMgr.PrunePartition() : There is no matching partition to prune for inst %v. Partitions=%v", instId, newPartitions)
		return nil
	}

	tombstoneInstId, err := common.NewIndexInstId()
	if err != nil {
		logging.Errorf("LifecycleMgr.PrunePartition() : Failed to generate index inst id. Reason = %v", err)
		return err
	}

	if err := m.repo.splitPartitionFromTopology(defn.Bucket, defn.Scope, defn.Collection, id, instId, tombstoneInstId, newPartitions); err != nil {
		logging.Errorf("LifecycleMgr.PrunePartition() : Failed to find split index.  Reason = %v", err)
		return err
	}

	//
	// Once splitPartitionFromTopology() successfully returns, the metadata change is committed.  The prune operation is
	// consider logically succeed.   It will then call the indexer to cleanup its data structure.   If indexer fails to
	// cleanup its data structure, the indexer runtime state can be corrupted and it will require a restart.
	//
	if notify && m.notifier != nil {
		if err := m.notifier.OnPartitionPrune(instId, newPartitions, reqCtx); err != nil {
			indexerErr, ok := err.(*common.IndexerError)
			if ok && indexerErr.Code != common.IndexNotExist {
				logging.Errorf("LifecycleMgr.PruneIndexInstance(): Encountered error when dropping index: %v.",
					indexerErr.Reason)
				return err

			} else if !strings.Contains(err.Error(), "Unknown Index Instance") {
				logging.Errorf("LifecycleMgr.PruneIndexInstance(): Encountered error when dropping index: %v.  Drop index will be retried in background",
					err.Error())
				return err
			}
		}
	}

	return nil
}

//////////////////////////////////////////////////////////////
// Lifecycle Mgr - support functions
//////////////////////////////////////////////////////////////

//
// A proxy can be
// 1) index instance that yet to be merged.  If a proxy has been merged, it will be removed from metadata.
// 2) A DELETED instance that contains the partitions already pruned.   This proxy is only used for crash recovery.
//
// This function will only return proxy belong to (1)
//
func (m *LifecycleMgr) findNumValidProxy(bucket, scope, collection string,
	defnId common.IndexDefnId, instId common.IndexInstId) (int, error) {

	insts, err := m.findAllLocalIndexInst(bucket, scope, collection, defnId)
	if err != nil {
		return -1, err
	}

	var count int
	for _, inst := range insts {
		if inst.State != uint32(common.INDEX_STATE_DELETED) && inst.RealInstId == uint64(instId) {
			count++
		}
	}

	return count, nil
}

// canRetryBuildError determines whether a particular build error can be retried.
// Index builds are never retried in rebalance as Rebalancer would never learn their fates.
func (m *LifecycleMgr) canRetryBuildError(inst *IndexInstDistribution, err error, isRebal bool) bool {

	if inst == nil || isRebal || inst.RState != uint32(common.REBAL_ACTIVE) {
		return false
	}

	indexerErr, ok := err.(*common.IndexerError)
	if !ok {
		return true
	}

	if indexerErr.Code == common.IndexNotExist ||
		indexerErr.Code == common.InvalidBucket ||
		indexerErr.Code == common.BucketEphemeral ||
		indexerErr.Code == common.IndexAlreadyExist ||
		indexerErr.Code == common.IndexInvalidState ||
		indexerErr.Code == common.BucketEphemeralStd {
		return false
	}

	return true
}

func (m *LifecycleMgr) canRetryCreateError(err error) bool {

	indexerErr, ok := err.(*common.IndexerError)
	if !ok {
		return false
	}

	if indexerErr.Code == common.IndexNotExist ||
		indexerErr.Code == common.InvalidBucket ||
		indexerErr.Code == common.BucketEphemeral ||
		indexerErr.Code == common.IndexAlreadyExist ||
		indexerErr.Code == common.TransientError ||
		indexerErr.Code == common.IndexInvalidState ||
		indexerErr.Code == common.BucketEphemeralStd {
		return false
	}

	return true
}

func (m *LifecycleMgr) UpdateIndexInstance(bucket, scope, collection string, defnId common.IndexDefnId, instId common.IndexInstId,
	state common.IndexState, streamId common.StreamId, errStr string, buildTime []uint64, rState uint32,
	partitions []uint64, versions []int, version int) error {

	topology, err := m.repo.CloneTopologyByCollection(bucket, scope, collection)
	if err != nil {
		logging.Errorf("LifecycleMgr.UpdateIndexInstance() : index instance update fails. Reason = %v", err)
		return err
	}
	if topology == nil {
		logging.Warnf("LifecycleMgr.UpdateIndexInstance() : toplogy does not exist.  Skip index instance update for %v", defnId)
		return nil
	}

	defn, err := m.repo.GetIndexDefnById(defnId)
	if err != nil {
		logging.Errorf("LifecycleMgr.UpdateIndexInstance() : Failed to find index definiton %v. Reason = %v", defnId, err)
		return err
	}
	if defn == nil {
		logging.Warnf("LifecycleMgr.UpdateIndexInstance() : index does not exist. Skip index instance update for %v.", defnId)
		return nil
	}

	indexerId, err := m.repo.GetLocalIndexerId()
	if err != nil {
		logging.Errorf("LifecycleMgr.UpdateIndexInstance() : Failed to find indexerId for defn %v. Reason = %v", defnId, err)
		return err
	}

	changed := false

	if rState != uint32(common.REBAL_NIL) {
		changed = topology.UpdateRebalanceStateForIndexInst(defnId, instId, common.RebalanceState(rState))
	}

	if state != common.INDEX_STATE_NIL {
		changed = topology.UpdateStateForIndexInst(defnId, instId, common.IndexState(state)) || changed

		if state == common.INDEX_STATE_INITIAL ||
			state == common.INDEX_STATE_CATCHUP ||
			state == common.INDEX_STATE_ACTIVE ||
			state == common.INDEX_STATE_DELETED {

			changed = topology.UpdateScheduledFlagForIndexInst(defnId, instId, false) || changed
		}
	}

	if streamId != common.NIL_STREAM {
		changed = topology.UpdateStreamForIndexInst(defnId, instId, common.StreamId(streamId)) || changed
	}

	changed = topology.SetErrorForIndexInst(defnId, instId, errStr) || changed

	changed = topology.UpdateStorageModeForIndexInst(defnId, instId, string(defn.Using)) || changed

	if len(partitions) != 0 {
		changed = topology.AddPartitionsForIndexInst(defnId, instId, string(indexerId), partitions, versions) || changed
	}

	if version != -1 {
		changed = topology.UpdateVersionForIndexInst(defnId, instId, uint64(version)) || changed
	}

	if changed {
		if err := m.repo.SetTopologyByCollection(bucket, defn.Scope, defn.Collection, topology); err != nil {
			// Topology update is in place.  If there is any error, SetTopologyByCollection will purge the cache copy.
			logging.Errorf("LifecycleMgr.handleTopologyChange() : index instance update fails. Reason = %v", err)
			return err
		}
	}

	return nil
}

func (m *LifecycleMgr) SetScheduledFlag(bucket, scope, collection string, defnId common.IndexDefnId,
	instId common.IndexInstId, scheduled bool) error {

	topology, err := m.repo.CloneTopologyByCollection(bucket, scope, collection)
	if err != nil {
		logging.Errorf("LifecycleMgr.SetScheduledFlag() : index instance update fails. Reason = %v", err)
		return err
	}
	if topology == nil {
		logging.Warnf("LifecycleMgr.SetScheduledFlag() : toplogy does not exist.  Skip index instance update for %v", defnId)
		return nil
	}

	changed := topology.UpdateScheduledFlagForIndexInst(defnId, instId, scheduled)

	if changed {
		if err := m.repo.SetTopologyByCollection(bucket, scope, collection, topology); err != nil {
			// Topology update is in place.  If there is any error, SetTopologyByCollection will purge the cache copy.
			logging.Errorf("LifecycleMgr.SetScheduledFlag() : index instance update fails. Reason = %v", err)
			return err
		}
	}

	return nil
}

func (m *LifecycleMgr) findAllLocalIndexInst(bucket, scope, collection string,
	defnId common.IndexDefnId) ([]IndexInstDistribution, error) {

	topology, err := m.repo.GetTopologyByCollection(bucket, scope, collection)
	if err != nil {
		logging.Errorf("LifecycleMgr.findAllLocalIndexInst() : Cannot read topology metadata. Reason = %v", err)
		return nil, err
	}
	if topology == nil {
		return nil, nil
	}

	return topology.GetIndexInstancesByDefn(defnId), nil
}

func (m *LifecycleMgr) FindLocalIndexInst(bucket, scope, collection string,
	defnId common.IndexDefnId, instId common.IndexInstId) (*IndexInstDistribution, error) {

	insts, err := m.findAllLocalIndexInst(bucket, scope, collection, defnId)
	if err != nil {
		logging.Errorf("LifecycleMgr.FindLocalIndexInst() : Cannot read topology metadata. Reason = %v", err)
		return nil, err
	}

	for _, inst := range insts {
		if common.IndexInstId(inst.InstId) == instId {
			return &inst, nil
		}
	}

	return nil, nil
}

func (m *LifecycleMgr) updateIndexState(bucket, scope, collection string, defnId common.IndexDefnId, instId common.IndexInstId, state common.IndexState) error {

	topology, err := m.repo.CloneTopologyByCollection(bucket, scope, collection)
	if err != nil {
		logging.Errorf("LifecycleMgr.updateIndexState() : fails to find index instance. Reason = %v", err)
		return err
	}
	if topology == nil {
		logging.Warnf("LifecycleMgr.updateIndexState() : fails to find index instance. Skip update for %v to %v.", defnId, state)
		return nil
	}

	changed := topology.UpdateStateForIndexInst(defnId, instId, state)
	if changed {
		if err := m.repo.SetTopologyByCollection(bucket, scope, collection, topology); err != nil {
			// Topology update is in place.  If there is any error, SetTopologyByCollection will purge the cache copy.
			logging.Errorf("LifecycleMgr.updateIndexState() : fail to update state of index instance.  Reason = %v", err)
			return err
		}
	}

	return nil
}

func (m *LifecycleMgr) canBuildIndex(bucket, scope, collection string) bool {

	t, _ := m.repo.GetTopologyByCollection(bucket, scope, collection)
	if t == nil {
		return true
	}

	for i, _ := range t.Definitions {
		for j, _ := range t.Definitions[i].Instances {
			if t.Definitions[i].Instances[j].State == uint32(common.INDEX_STATE_CATCHUP) ||
				t.Definitions[i].Instances[j].State == uint32(common.INDEX_STATE_INITIAL) {
				return false
			}
		}
	}

	return true
}

func (m *LifecycleMgr) handleServiceMap(content []byte) ([]byte, error) {

	srvMap, err := m.getServiceMap()
	if err != nil {
		return nil, err
	}

	result, err := client.MarshallServiceMap(srvMap)
	if err == nil {
		if m.notifier != nil {
			m.notifier.OnFetchStats()
		}
	}

	return result, err
}

func (m *LifecycleMgr) getServiceMap() (*client.ServiceMap, error) {

	uuid := time.Now().UnixNano()
	userAgent := fmt.Sprintf("GetServiceMap_%v", uuid)

	cinfo, err := common.FetchNewClusterInfoCache2(m.clusterURL, common.DEFAULT_POOL, userAgent)
	if err != nil {
		return nil, err
	}

	if err := cinfo.FetchNodesAndSvsInfo(); err != nil {
		return nil, err
	}

	if err := cinfo.FetchServerGroups(); err != nil {
		return nil, err
	}

	srvMap := new(client.ServiceMap)

	id, err := m.repo.GetLocalIndexerId()
	if err != nil {
		return nil, err
	}
	srvMap.IndexerId = string(id)

	srvMap.ScanAddr, err = cinfo.GetLocalServiceAddress(common.INDEX_SCAN_SERVICE, true)
	if err != nil {
		return nil, err
	}

	srvMap.HttpAddr, err = cinfo.GetLocalServiceAddress(common.INDEX_HTTP_SERVICE, true)
	if err != nil {
		return nil, err
	}

	srvMap.AdminAddr, err = cinfo.GetLocalServiceAddress(common.INDEX_ADMIN_SERVICE, true)
	if err != nil {
		return nil, err
	}

	srvMap.NodeAddr, err = cinfo.GetLocalHostAddress()
	if err != nil {
		return nil, err
	}

	srvMap.ServerGroup, err = cinfo.GetLocalServerGroup()
	if err != nil {
		return nil, err
	}

	srvMap.NodeUUID, err = m.repo.GetLocalNodeUUID()
	if err != nil {
		return nil, err
	}

	srvMap.IndexerVersion = common.INDEXER_CUR_VERSION

	srvMap.ClusterVersion = cinfo.GetClusterVersion()

	exclude, err := m.repo.GetLocalValue("excludeNode")
	if err != nil && !strings.Contains(err.Error(), "FDB_RESULT_KEY_NOT_FOUND") {
		return nil, err
	}
	srvMap.ExcludeNode = string(exclude)

	srvMap.StorageMode = uint64(common.GetStorageMode())

	return srvMap, nil
}

// This function returns an error if it cannot connect for fetching bucket info.
// It returns BUCKET_UUID_NIL (err == nil) if bucket does not exist.
//
func (m *LifecycleMgr) getBucketUUID(bucket string, retry bool) (string, error) {
	count := 0
RETRY:
	cinfo := m.cinfoClient.GetClusterInfoCache()

	cinfo.RLock()
	uuid := cinfo.GetBucketUUID(bucket)
	cinfo.RUnlock()

	if uuid == common.BUCKET_UUID_NIL && count < 5 && retry {
		count++
		time.Sleep(time.Duration(100) * time.Millisecond)
		// Force fetch cluster info client
		err := m.cinfoClient.FetchWithLock()
		if err != nil {
			return common.BUCKET_UUID_NIL, err
		}
		goto RETRY
	}

	return uuid, nil
}

// This function returns an error if it cannot connect for fetching manifest info.
// It returns COLLECTION_ID_NIL (err == nil) if collection does not exist.
//
func (m *LifecycleMgr) getCollectionID(bucket, scope, collection string, retry bool) (string, error) {
	count := 0
RETRY:
	cinfo := m.cinfoClient.GetClusterInfoCache()

	cinfo.RLock()
	colldId := cinfo.GetCollectionID(bucket, scope, collection)
	cinfo.RUnlock()

	if colldId == collections.COLLECTION_ID_NIL && count < 5 && retry {
		count++
		time.Sleep(time.Duration(100) * time.Millisecond)

		// Force fetch cluster info cache
		err := m.cinfoClient.FetchWithLock()
		if err != nil {
			return collections.COLLECTION_ID_NIL, err
		}
		goto RETRY
	}
	return colldId, nil
}

// This function returns an error if it cannot connect for fetching manifest info.
// It returns SCOPE_ID_NIL (err == nil) if scope does not exist.
//
func (m *LifecycleMgr) getScopeID(bucket, scope string) (string, error) {
	count := 0
RETRY:
	cinfo := m.cinfoClient.GetClusterInfoCache()

	cinfo.RLock()
	scopeId := cinfo.GetScopeID(bucket, scope)
	cinfo.RUnlock()

	if scopeId == collections.SCOPE_ID_NIL && count < 5 {
		count++
		time.Sleep(time.Duration(100) * time.Millisecond)
		// Force fetch cluster info cache
		err := m.cinfoClient.FetchWithLock()
		if err != nil {
			return collections.SCOPE_ID_NIL, err
		}
		goto RETRY
	}

	return scopeId, nil
}

// This function returns an error if it cannot connect for fetching manifest info.
// It returns SCOPE_ID_NIL, COLLECTION_ID_NIL (err == nil) if scope, collection does
// not exist.
//
func (m *LifecycleMgr) getScopeAndCollectionID(bucket, scope, collection string) (string, string, error) {
	count := 0
RETRY:
	cinfo := m.cinfoClient.GetClusterInfoCache()

	cinfo.RLock()
	scopeId, colldId := cinfo.GetScopeAndCollectionID(bucket, scope, collection)
	cinfo.RUnlock()

	if (scopeId == collections.SCOPE_ID_NIL || colldId == collections.COLLECTION_ID_NIL) && count < 5 {
		count++
		time.Sleep(time.Duration(100) * time.Millisecond)

		// Force fetch cluster info cache on errors
		err := m.cinfoClient.FetchWithLock()
		if err != nil {
			return collections.SCOPE_ID_NIL, collections.COLLECTION_ID_NIL, err
		}
		goto RETRY
	}

	return scopeId, colldId, nil
}

// This function ensures:
// 1) Bucket exists
// 2) Existing Index Definition matches the UUID of existing bucket
// 3) If bucket does not exist AND there is no existing definition, this returns common.BUCKET_UUID_NIL
//
func (m *LifecycleMgr) verifyBucket(bucket string) (string, error) {

	// If this function returns an error, then it cannot fetch bucket UUID.
	// Otherwise, if it returns BUCKET_UUID_NIL, it means none of the node recognize this bucket.
	currentUUID, err := m.getBucketUUID(bucket, true)
	if err != nil {
		return common.BUCKET_UUID_NIL, err
	}

	topologies, err := m.repo.GetTopologiesByBucket(bucket)
	if err != nil {
		return common.BUCKET_UUID_NIL, err
	}

	if len(topologies) > 0 {
		for i, _ := range topologies {
			topology := topologies[i]
			for _, defnRef := range topology.Definitions {
				valid := false
				insts := topology.GetIndexInstancesByDefn(common.IndexDefnId(defnRef.DefnId))
				for _, inst := range insts {
					state, _ := topology.GetStatusByInst(common.IndexDefnId(defnRef.DefnId), common.IndexInstId(inst.InstId))
					if state != common.INDEX_STATE_DELETED {
						valid = true
						break
					}
				}

				if valid {
					if defn, err := m.repo.GetIndexDefnById(common.IndexDefnId(defnRef.DefnId)); err == nil && defn != nil {
						if defn.BucketUUID != currentUUID {
							return common.BUCKET_UUID_NIL,
								errors.New("Bucket does not exist or temporarily unavailable for creating new index." +
									" Please retry the operation at a later time.")
						}
					}
				}
			}
		}
	}

	// topology is either nil or all index defn matches bucket UUID
	// if topology is nil, then currentUUID == common.BUCKET_UUID_NIL
	return currentUUID, nil
}

// This function ensures:
// 1) Scope and Collection exist
// 2) Existing Index Definition matches the UUID of existing Scope and Collection
// 3) If scope does not exist AND there is no existing definition in scope, this returns SCOPE_ID_NIL
// 4) If collection does not exist AND there is no existing definition in collection, this returns COLLECTION_ID_NIL
//
func (m *LifecycleMgr) verifyScopeAndCollection(bucket, scope, collection string) (string, string, error) {

	scopeID, collectionID, err := m.getScopeAndCollectionID(bucket, scope, collection)
	if err != nil {
		return collections.SCOPE_ID_NIL, collections.COLLECTION_ID_NIL, err
	}

	topology, err := m.repo.GetTopologyByCollection(bucket, scope, collection)
	if err != nil {
		return collections.SCOPE_ID_NIL, collections.COLLECTION_ID_NIL, err
	}

	if topology != nil {
		for _, defnRef := range topology.Definitions {
			if defnRef.Scope == scope && defnRef.Collection == collection {
				valid := false
				insts := topology.GetIndexInstancesByDefn(common.IndexDefnId(defnRef.DefnId))
				for _, inst := range insts {
					state, _ := topology.GetStatusByInst(common.IndexDefnId(defnRef.DefnId), common.IndexInstId(inst.InstId))
					if state != common.INDEX_STATE_DELETED {
						valid = true
						break
					}
				}

				if valid {
					if defn, err := m.repo.GetIndexDefnById(common.IndexDefnId(defnRef.DefnId)); err == nil && defn != nil {
						if defn.ScopeId != scopeID {
							return collections.SCOPE_ID_NIL, collections.COLLECTION_ID_NIL,
								errors.New(fmt.Sprintf("Scope does not exist or temporarily unavailable for creating new index."+
									"Bucket = %v Scope = %v. Please retry the operation at a later time.",
									bucket, scope))
						}
						if defn.CollectionId != collectionID {
							return collections.SCOPE_ID_NIL, collections.COLLECTION_ID_NIL,
								errors.New(fmt.Sprintf("Collection does not exist or temporarily unavailable for creating new index."+
									"Bucket = %v Scope = %v Collection = %v. Please retry the operation at a later time.",
									bucket, scope, collection))
						}
					}
				}
			}
		}
	}

	return scopeID, collectionID, nil
}

//////////////////////////////////////////////////////////////
// Lifecycle Mgr - janitor
// Jantior cleanup deleted index in the background.  This
// operation is idempotent -- unless the metadata store is
// corrupted.
//////////////////////////////////////////////////////////////

//
// 1) This is important that this function does not mutate the repository directly.
// 2) Any call to mutate the repository must be async request.
//
func (m *janitor) cleanup() {

	// if rebalancing is running
	if _, err := m.manager.repo.GetLocalValue("RebalanceRunning"); err == nil {
		return
	}

	//
	// Cleanup based on delete token
	//
	logging.Infof("janitor: running cleanup.")

	entries := m.commandListener.GetNewDeleteTokens()
	retryList := make(map[string]*mc.DeleteCommandToken)

	for entry, command := range entries {

		logging.Infof("janitor: Processing delete token %v", entry)

		if err := m.deleteScheduleTokens(command.DefnId); err != nil {
			logging.Errorf("janitor: Failed to delete scheduled tokens upon cleanup for %v.  Internal Error = %v.", entry, err)
			retryList[entry] = command
		}

		defn, err := m.manager.repo.GetIndexDefnById(command.DefnId)
		if err != nil {
			retryList[entry] = command
			logging.Warnf("janitor: Failed to drop index upon cleanup.  Skp command %v.  Internal Error = %v.", entry, err)
			continue
		}

		// index may already be deleted or does not exist in this node
		// delayed processing of drop commands when index defn is null
		// to avoid some corner cases with scheduled index creation and droping of those indexes.
		// this effectively delays almost every drop token cleanup by janitor by 24 hours.
		if defn == nil {
			var timestamp int64
			timestamp, ok := m.deleteTokenCache[command.DefnId]
			if !ok {
				timestamp = time.Now().UnixNano()
				m.deleteTokenCache[command.DefnId] = timestamp
			}
			if time.Duration(time.Now().UnixNano()-timestamp) <= time.Duration(DELETE_TOKEN_DELAYED_CLEANUP_INTERVAL) {
				retryList[entry] = command
			} else {
				delete(m.deleteTokenCache, command.DefnId)
				logging.Debugf("Janitor: finishing cleanup for index %v", command.DefnId)
			}
			continue
		} else {
			// defn was nil earlier hence we added DefnId to deleteTokenCache in previous iteration
			// and defn is now available so remove the entry from cache.
			delete(m.deleteTokenCache, command.DefnId)
		}

		// Queue up the cleanup request.  The request wont' happen until bootstrap is ready.
		if err := m.manager.requestServer.MakeRequest(client.OPCODE_DROP_INDEX, fmt.Sprintf("%v", command.DefnId), nil); err != nil {
			retryList[entry] = command
			logging.Warnf("janitor: Failed to drop index upon cleanup.  Skp command %v.  Internal Error = %v.", entry, err)
		} else {
			logging.Infof("janitor: Clean up deleted index %v during periodic cleanup ", command.DefnId)
		}
	}

	for path, token := range retryList {
		m.commandListener.AddNewDeleteToken(path, token)
	}

	//
	// Cleanup based on drop instance token
	//
	entries2 := m.commandListener.GetNewDropInstanceTokens()
	retryList2 := make(map[string]*mc.DropInstanceCommandToken)

	for entry, command := range entries2 {

		logging.Infof("janitor: Processing drop instance token %v", entry)

		defn, err := m.manager.repo.GetIndexDefnById(command.DefnId)
		if err != nil {
			retryList2[entry] = command
			logging.Warnf("janitor: Failed to drop index isntance upon cleanup.  Skp command %v.  Internal Error = %v.", entry, err)
			continue
		}

		// index may already be deleted or does not exist in this node
		if defn == nil {
			continue
		}

		inst, err := m.manager.FindLocalIndexInst(defn.Bucket, defn.Scope, defn.Collection, command.DefnId, command.InstId)
		if err != nil {
			retryList2[entry] = command
			logging.Warnf("janitor: Failed to find index instance (%v, %v) during cleanup. Internal error = %v.  Skipping.", defn.Bucket, defn.Name, err)
			continue
		}

		if inst != nil &&
			inst.State != uint32(common.INDEX_STATE_DELETED) &&
			inst.RState != uint32(common.REBAL_PENDING_DELETE) &&
			inst.RState != uint32(common.REBAL_MERGED) {

			idxDefn := *defn
			idxDefn.InstId = common.IndexInstId(command.InstId)
			idxDefn.Partitions = nil

			msg := &dropInstance{Defn: idxDefn, Notify: true, UpdateStatusOnly: false, DeletedOnly: false}
			if buf, err := json.Marshal(&msg); err == nil {

				if err := m.manager.requestServer.MakeRequest(client.OPCODE_DROP_INSTANCE,
					fmt.Sprintf("%v", command.DefnId), buf); err != nil {

					retryList2[entry] = command
					logging.Warnf("janitor: Failed to drop instance upon cleanup.  Skip instance (%v, %v, %v).  Internal Error = %v.",
						defn.Bucket, defn.Name, inst.InstId, err)
					continue
				} else {
					logging.Infof("janitor: Clean up deleted instance (%v, %v, %v) during periodic cleanup ", defn.Bucket, defn.Name, inst.InstId)
				}
			} else {
				retryList2[entry] = command
				logging.Warnf("janitor: Failed to drop instance upon cleanup.  Skip instance (%v, %v, %v).  Internal Error = %v.",
					defn.Bucket, defn.Name, inst.InstId, err)
				continue
			}
		}

		//
		// update the replica count
		//
		needsMerge, err := defn.NumReplica2.NeedMergeWith(command.Defn.NumReplica2)
		if err != nil {
			retryList2[entry] = command
			logging.Warnf("janitor: Failed to update replica count for index (%v, %v).  Internal Error = %v.", defn.Bucket, defn.Name, err)
			continue
		}

		if needsMerge {
			idxDefn := *defn
			idxDefn.NumReplica2 = command.Defn.NumReplica2

			content, err := common.MarshallIndexDefn(&idxDefn)
			if err != nil {
				retryList2[entry] = command
				logging.Warnf("janitor: Failed to update replica count for index (%v, %v).  Internal Error = %v.", defn.Bucket, defn.Name, err)
				continue
			}

			if err := m.manager.requestServer.MakeRequest(client.OPCODE_UPDATE_REPLICA_COUNT, "", content); err != nil {
				retryList2[entry] = command
				logging.Warnf("janitor: Failed to update replia count cleanup.  Skip (%v, %v).  Internal Error = %v.",
					defn.Bucket, defn.Name, err)
				continue
			}
		}
	}

	for path, token := range retryList2 {
		m.commandListener.AddNewDropInstanceToken(path, token)
	}

	//
	// Cleanup based on index status (DELETED index)
	//

	metaIter, err := m.manager.repo.NewIterator()
	if err != nil {
		logging.Warnf("janitor: Failed to  upon instantiate metadata iterator during cleanup.  Internal Error = %v", err)
		return
	}
	defer metaIter.Close()

	for _, defn, err := metaIter.Next(); err == nil; _, defn, err = metaIter.Next() {

		insts, err := m.manager.findAllLocalIndexInst(defn.Bucket, defn.Scope, defn.Collection, defn.DefnId)
		if err != nil {
			logging.Warnf("janitor: Failed to find index instance (%v, %v) during cleanup. Internal error = %v.  Skipping.", defn.Bucket, defn.Name, err)
			continue
		}

		for _, inst := range insts {
			// Queue up the cleanup request.  The request wont' happen until bootstrap is ready.
			// Do not clean up any proxy instance created due to rebalance.  These proxy instances could be left in metadata to
			// assist cleanup during bootstrap.  Removing them by janitor could cause some data not being cleaned up.
			if inst.State == uint32(common.INDEX_STATE_DELETED) &&
				inst.RState != uint32(common.REBAL_PENDING_DELETE) &&
				inst.RState != uint32(common.REBAL_MERGED) {

				idxDefn := *defn
				idxDefn.InstId = common.IndexInstId(inst.InstId)
				idxDefn.Partitions = nil

				msg := &dropInstance{Defn: idxDefn, Notify: true, DeletedOnly: true}
				if buf, err := json.Marshal(&msg); err == nil {

					if err := m.manager.requestServer.MakeRequest(client.OPCODE_DROP_OR_PRUNE_INSTANCE_DDL,
						fmt.Sprintf("%v", idxDefn.DefnId), buf); err != nil {

						logging.Warnf("janitor: Failed to drop instance upon cleanup.  Skip instance (%v, %v, %v).  Internal Error = %v.",
							defn.Bucket, defn.Name, inst.InstId, err)
					} else {
						logging.Infof("janitor: Clean up deleted instance (%v, %v, %v) during periodic cleanup ", defn.Bucket, defn.Name, inst.InstId)
					}
				} else {
					logging.Warnf("janitor: Failed to drop instance upon cleanup.  Skip instance (%v, %v, %v).  Internal Error = %v.",
						defn.Bucket, defn.Name, inst.InstId, err)
				}
			}
		}
	}
}

func (m *janitor) deleteScheduleTokens(defnID common.IndexDefnId) error {

	// TODO: Avoid these calls if these calls were successful once.

	if err := mc.DeleteScheduleCreateToken(defnID); err != nil {
		return fmt.Errorf("DeleteScheduleCreateToken:%v:%v", defnID, err)
	}

	if err := mc.DeleteStopScheduleCreateToken(defnID); err != nil {
		return fmt.Errorf("DeleteStopScheduleCreateToken:%v:%v", defnID, err)
	}

	return nil
}

// janitor.run runs as a go routine *per request*.
func (m *janitor) run() {

	m.manager.done.Add(1)
	defer m.manager.done.Done()

	// start listener
	m.commandListener.ListenTokens()
	time.Sleep(time.Minute)

	// do initial cleanup
	m.cleanup()

	ticker := time.NewTicker(time.Second * 60)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.cleanup()

		case <-m.runch:
			m.cleanup()

		case <-m.manager.killch:
			m.commandListener.Close()
			logging.Infof("janitor: go-routine terminates.")
			return

		case <-m.listenerDonech:
			m.listenerDonech = make(chan bool)
			m.commandListener = mc.NewCommandListener(m.listenerDonech, false, false, true, true, false, false)
			m.commandListener.ListenTokens()
		}
	}
}

func (m *janitor) runOnce() {
	select {
	case m.runch <- true:
	default:
	}
}

func newJanitor(mgr *LifecycleMgr) *janitor {

	donech := make(chan bool)

	janitor := &janitor{
		manager:          mgr,
		commandListener:  mc.NewCommandListener(donech, false, false, true, true, false, false),
		listenerDonech:   donech,
		runch:            make(chan bool),
		deleteTokenCache: make(map[common.IndexDefnId]int64),
	}

	return janitor
}

//////////////////////////////////////////////////////////////
// Lifecycle Mgr - builder
// Buidler builds index in the background.  This
// operation is idempotent -- unless the metadata store is
// corrupted.
//////////////////////////////////////////////////////////////

// builder.run runs as a go routine *per request*. It asynchronously builds indexes both from requests
// arriving via builder.notifych and those retrieved from tokens.
func (s *builder) run() {

	s.manager.done.Add(1)
	defer s.manager.done.Done()

	// start listener
	s.commandListener.ListenTokens()

	// Sleep before checking for index to build.  When a recovered node starts up, it needs to wait until
	// the rebalancing token is saved.  This is to avoid the bulider to get ahead of the rebalancer.
	// Otherwise, rebalancer could fail if builder has issued an index build ahead of the rebalancer.
	func() {
		timer := time.NewTimer(time.Second * 60)
		defer timer.Stop()

		select {
		case <-timer.C:
		case <-s.manager.killch:
			s.commandListener.Close()
			logging.Infof("builder: go-routine terminates.")
			return
		}
	}()

	//builder is called only after indexer bootstrap completes
	s.recover()

	// check if there is any pending index build every tick
	ticker := time.NewTicker(time.Millisecond * 200)
	defer ticker.Stop()

	for {
		select {
		case defn := <-s.notifych:
			// This case just adds incoming defnIds to the pendings map
			logging.Infof("builder:  Received new index build request %v.  "+
				"Schedule to build index for bucket: %v, scope: %v, collection: %v",
				defn.DefnId, defn.Bucket, defn.Scope, defn.Collection)
			s.addPending(defn.Bucket, defn.Scope, defn.Collection, uint64(defn.DefnId))

		case <-ticker.C:
			// This case submits index builds for as many defnIds in the pendings map as allowed
			// (batchSize minus number already building).
			processed := s.processBuildToken(false)

			//when building from build tokens, sleep for 30 seconds,
			//to avoid racing ahead of build index command coming
			//from the client. If multiple defer indexes are being
			//built using single Build Index, user can see duplicate
			//build error if builder initiates few requests.
			if processed {
				func() {
					timer := time.NewTimer(time.Second * 30)
					defer timer.Stop()
					select {
					case <-timer.C:
					case <-s.manager.killch:
						s.commandListener.Close()
						logging.Infof("builder: go-routine terminates.")
						return
					}
				}()
			}

			if len(s.pendings) > 0 {
				buildList, quota := s.getBuildList()
				for _, key := range buildList {
					quota = s.tryBuildIndex(key, quota) // submits defnId builds and reduces quota by number submitted
					if quota <= 0 {
						break
					}
				}
			}

		case <-s.manager.killch:
			s.commandListener.Close()
			logging.Infof("builder: go-routine terminates.")
			return

		case <-s.listenerDonech:
			s.listenerDonech = make(chan bool)
			s.commandListener = mc.NewCommandListener(s.listenerDonech, false, true, false, false, false, false)
			s.commandListener.ListenTokens()
		}
	}
}

// getBuildList returns a filtered version of the pendings list that removes already building keys and orders the
// remainder into a preferred build order. It also returns the max number of indexes to build in this iteration (quota).
func (s *builder) getBuildList() ([]string, int32) {

	// quota is max index builds to start; skipList is keyspaces that have at least one instance already building
	quota, skipList := s.getQuota()

	// Initialize buildList with all pending keyspaces that do not already have builds ongoing
	buildList := ([]string)(nil)
	for key, _ := range s.pendings {
		if _, ok := skipList[key]; !ok {
			buildList = append(buildList, key)
		}
	}

	// Primary sort buildList ascending by number of pending indexes for each key (to prevent starvation)
	for i := 0; i < len(buildList)-1; i++ {
		for j := i + 1; j < len(buildList); j++ {
			key_i := buildList[i]
			key_j := buildList[j]

			if len(s.pendings[key_i]) < len(s.pendings[key_j]) {
				tmp := buildList[i]
				buildList[i] = buildList[j]
				buildList[j] = tmp
			}
		}
	}

	// Secondary sort buildList from closest to farthest from quota (same distance over or under quota equally weighted)
	for i := 0; i < len(buildList)-1; i++ {
		for j := i + 1; j < len(buildList); j++ {
			key_i := buildList[i]
			key_j := buildList[j]

			if math.Abs(float64(len(s.pendings[key_i])-int(quota))) > math.Abs(float64(len(s.pendings[key_j])-int(quota))) {
				tmp := buildList[i]
				buildList[i] = buildList[j]
				buildList[j] = tmp
			}
		}
	}

	return buildList, quota
}

// addPending adds the defnId to the list of pending index builds for the given b/s/c
// if it was not already there. Returns true if it added id, else false (duplicate).
func (s *builder) addPending(bucket, scope, collection string, defnId uint64) bool {
	key := getPendingKey(bucket, scope, collection)
	for _, defnId2 := range s.pendings[key] {
		if defnId2 == defnId {
			return false
		}
	}

	s.pendings[key] = append(s.pendings[key], uint64(defnId))
	return true
}

// tryBuildIndex starts as many index builds as possible for keyspace pendingKey w.r.t. the quota argument
// (which is per index, not per instance). It returns the remaining quota (original minus number of builds started).
func (s *builder) tryBuildIndex(pendingKey string, quota int32) int32 {

	bucket, scope, collection := getCollectionFromKey(pendingKey)
	quotaRemaining := quota // original quota minus number of index builds started here

	defnIds := s.pendings[pendingKey] // all indexes needing builds for keyspace pendingKey
	if len(defnIds) != 0 {
		// This is a pre-cautionary check if there is any index being
		// built for the collection. The authortative check is done by indexer.
		if s.manager.canBuildIndex(bucket, scope, collection) {

			buildList := ([]uint64)(nil)       // defnIds to start building
			isDisableBuild := s.disableBuild() // is background index building disabled?

			pendingList := make([]uint64, len(defnIds)) // defnIds not built here
			copy(pendingList, defnIds)

			for _, defnId := range defnIds {
				if quotaRemaining <= 0 {
					break
				}

				buildDefnId := false // submit this defnId for build?
				foundReady := false  // found any instId of this defnId in INDEX_STATE_READY state?
				pendingList = pendingList[1:]

				defn, err := s.manager.repo.GetIndexDefnById(common.IndexDefnId(defnId))
				if defn == nil || err != nil {
					logging.Warnf("builder: Failed to find index definition (%v, %v).  Skipping.", defnId, bucket)
					continue
				}

				insts, err := s.manager.findAllLocalIndexInst(bucket, scope, collection, common.IndexDefnId(defnId))
				if len(insts) == 0 || err != nil {
					logging.Warnf("builder: Failed to find index instance (%v, %v).  Skipping.", defnId, bucket)
					continue
				}

				// If any inst of defnId is ready or has old storage mode, add that defnId to buildList if possible
				for _, inst := range insts {
					if quotaRemaining <= 0 {
						break
					}

					if inst.State == uint32(common.INDEX_STATE_READY) {
						foundReady = true
						// build index if
						// 1) background index build is enabled
						// 2) index build is due to upgrade
						if !isDisableBuild || len(inst.OldStorageMode) != 0 {
							if !buildDefnId {
								buildDefnId = true
								buildList = append(buildList, defnId)
								quotaRemaining--
							}
						}
					}
				}
				if !foundReady {
					logging.Warnf("builder: Index (%v, %v) has no instances in READY state.  Skipping.", defnId, bucket)
				} else if !buildDefnId {
					// put it back to the pending list if index build is disable
					pendingList = append(pendingList, defnId)
					logging.Warnf("builder: Background build is disabled.  Will retry building index (%v, %v) in next iteration.", defnId, bucket)
				}
			}

			// Clean up pendings map.  If there is any index that needs retry, they will be put into the notifych again.
			// Once this function is done, the map will be populated again from the notifych.
			if len(pendingList) == 0 {
				pendingList = nil
			}
			s.pendings[pendingKey] = pendingList

			// Submit the defnIds to be built, if any
			if len(buildList) != 0 {
				idList := &client.IndexIdList{DefnIds: buildList}
				key := fmt.Sprintf("%d", idList.DefnIds[0])
				content, err := client.MarshallIndexIdList(idList)
				if err != nil {
					logging.Warnf("builder: Failed to marshall index defnIds during index build.  Error = %v. Retry later.", err)
					return quota
				}
				logging.Infof("builder: Try build index for bucket: %v, scope: %v, collection: %v. Index %v",
					bucket, scope, collection, idList)

				// If any of the index cannot be built, those index will be skipped by lifecycle manager, so it
				// will send the rest of the indexes to the indexer.  An index cannot be built if it does not have
				// an index instance or the index instance is not in READY state.
				if err := s.manager.requestServer.MakeRequest(client.OPCODE_BUILD_INDEX_RETRY, key, content); err != nil {
					logging.Warnf("builder: Failed to build index.  Error = %v.", err)
				}
				logging.Infof("builder: defnIds still pending to be built: %v.", pendingList)
			}
		}
	}
	return quotaRemaining
}

// getQuota returns the number of new index builds that can be started now (quota) as the batchSize minus
// number of indexes already building, plus a list of "b/s/c" keys (skipList) of the ones already building.
func (s *builder) getQuota() (int32, map[string]bool) {

	quota := atomic.LoadInt32(&s.batchSize)
	if quota <= 0 { // unlimited quota (documented as batchSize of -1 in config.go)
		quota = math.MaxInt32
	}
	skipList := make(map[string]bool) // keyspaces w/ index(es) now building; can't start new build on these till current one completes

	metaIter, err := s.manager.repo.NewIterator()
	if err != nil {
		logging.Warnf("builder.getQuota():  Unable to read from metadata repository. Skipping quota check")
		return quota, skipList
	}
	defer metaIter.Close()

	for _, defn, err := metaIter.Next(); err == nil; _, defn, err = metaIter.Next() {

		insts, err := s.manager.findAllLocalIndexInst(defn.Bucket, defn.Scope, defn.Collection, defn.DefnId)
		if len(insts) == 0 || err != nil {
			logging.Warnf("builder.getQuota: Unable to read index instance for definition (%v, %v, %v, %v).   Skipping index for quota check",
				defn.Bucket, defn.Scope, defn.Collection, defn.Name)
			continue
		}

		// Check if any instances of this index are already currently building
		for _, inst := range insts {
			if inst.State == uint32(common.INDEX_STATE_INITIAL) || inst.State == uint32(common.INDEX_STATE_CATCHUP) {
				quota-- // an index already building reduces the quota for new build starts
				key := getPendingKey(defn.Bucket, defn.Scope, defn.Collection)
				skipList[key] = true
				break // only decrement quota once for the whole index, not for each building instance
			}
		}
	}

	if quota < 0 { // can happen if batchSize gets reduced
		quota = 0
	}
	return quota, skipList
}

func (s *builder) processBuildToken(bootstrap bool) bool {

	entries := s.commandListener.GetNewBuildTokens()
	retryList := make(map[string]*mc.BuildCommandToken)

	processed := false
	for entry, command := range entries {

		defn, err := s.manager.repo.GetIndexDefnById(command.DefnId)
		if err != nil {
			retryList[entry] = command
			logging.Warnf("builder: Unable to read index definition.  Skp command %v.  Internal Error = %v.", entry, err)
			continue
		}

		// index may already be deleted or does not exist in this node
		if defn == nil {
			continue
		}

		insts, err := s.manager.findAllLocalIndexInst(defn.Bucket, defn.Scope, defn.Collection, defn.DefnId)
		if err != nil {
			retryList[entry] = command
			logging.Warnf("builder: Unable to read index instance for definition (%v, %v, %v, %v).   Skipping ...",
				defn.Bucket, defn.Scope, defn.Collection, defn.Name)
			continue
		}

		for _, inst := range insts {

			if inst.State == uint32(common.INDEX_STATE_READY) {
				logging.Infof("builder: Processing build token %v", entry)

				if s.addPending(defn.Bucket, defn.Scope, defn.Collection, uint64(defn.DefnId)) {
					logging.Infof("builder: Schedule index build for (%v, %v, %v, %v).",
						defn.Bucket, defn.Scope, defn.Collection, defn.Name)
					processed = true
				}
			}
		}
	}

	for path, token := range retryList {
		s.commandListener.AddNewBuildToken(path, token)
	}

	return processed
}

func (s *builder) recover() {

	logging.Infof("builder: recovering scheduled index")

	//
	// Cleanup based on build token
	//
	s.processBuildToken(true)

	//
	// Cleanup based on index status
	//
	metaIter, err := s.manager.repo.NewIterator()
	if err != nil {
		logging.Errorf("builder:  Unable to read from metadata repository.   Will not recover scheduled build index from repository.")
		return
	}
	defer metaIter.Close()

	for _, defn, err := metaIter.Next(); err == nil; _, defn, err = metaIter.Next() {

		insts, err := s.manager.findAllLocalIndexInst(defn.Bucket, defn.Scope, defn.Collection, defn.DefnId)
		if len(insts) == 0 || err != nil {
			logging.Errorf("builder: Unable to read index instance for definition (%v, %v, %v, %v).   Skipping ...",
				defn.Bucket, defn.Scope, defn.Collection, defn.Name)
			continue
		}

		for _, inst := range insts {

			if inst.Scheduled && inst.State == uint32(common.INDEX_STATE_READY) {
				if s.addPending(defn.Bucket, defn.Scope, defn.Collection, uint64(defn.DefnId)) {
					logging.Infof("builder: Schedule index build for (%v, %v, %v, %v, %v).",
						defn.Bucket, defn.Scope, defn.Collection, defn.Name, inst.ReplicaId)
				}
			}
		}
	}
}

func (s *builder) configUpdate(config *common.Config) {
	newBatchSize := int32((*config)["settings.build.batch_size"].Int())
	atomic.StoreInt32(&s.batchSize, newBatchSize)

	disable := (*config)["build.background.disable"].Bool()
	if disable {
		atomic.StoreInt32(&s.disable, int32(1))
	} else {
		atomic.StoreInt32(&s.disable, int32(0))
	}
}

func (s *builder) disableBuild() bool {

	if atomic.LoadInt32(&s.disable) == 1 {
		return true
	}

	return false
}

// newBuilder constructs a new builder object. The singleton LifecycleMgr has a singleton builder as a child.
func newBuilder(mgr *LifecycleMgr) *builder {

	donech := make(chan bool)

	builder := &builder{
		manager:         mgr,
		pendings:        make(map[string][]uint64),
		notifych:        make(chan *common.IndexDefn, 50000),
		batchSize:       int32(common.SystemConfig["indexer.settings.build.batch_size"].Int()),
		commandListener: mc.NewCommandListener(donech, false, true, false, false, false, false),
		listenerDonech:  donech,
	}

	disable := common.SystemConfig["indexer.build.background.disable"].Bool()
	if disable {
		atomic.StoreInt32(&builder.disable, int32(1))
	} else {
		atomic.StoreInt32(&builder.disable, int32(0))
	}
	return builder
}

// getPendingKey constructs a key literal "bucket/scope/collection".
func getPendingKey(bucket, scope, collection string) string {
	return bucket + "/" + scope + "/" + collection
}

// getCollectionFromKey extracts and returns the bucket, scope, and
// collection from a key of the form "bucket/scope/collection".
func getCollectionFromKey(pendingKey string) (string, string, string) {
	input := strings.Split(pendingKey, "/")
	if len(input) == 3 {
		return input[0], input[1], input[2]
	}
	return "", "", ""
}

//////////////////////////////////////////////////////////////
// Lifecycle Mgr - udpator
//////////////////////////////////////////////////////////////

func newUpdator(mgr *LifecycleMgr) *updator {

	updator := &updator{
		manager: mgr,
	}

	return updator
}

// updator.run runs as a go routine *per request*.
func (m *updator) run() {

	m.manager.done.Add(1)
	defer m.manager.done.Done()

	ticker := time.NewTicker(time.Second * 60)
	defer ticker.Stop()

	lastUpdate := time.Now()
	m.checkServiceMap(false)

	for {
		select {
		case <-ticker.C:
			update := time.Now().Sub(lastUpdate) > (time.Minute * time.Duration(5))
			m.checkServiceMap(update)
			if update {
				lastUpdate = time.Now()
			}

		case <-m.manager.killch:
			logging.Infof("updator: go-routine terminates.")
			return
		}
	}
}

func (m *updator) checkServiceMap(update bool) {

	serviceMap, err := m.manager.getServiceMap()
	if err != nil {
		logging.Errorf("updator: fail to get indexer serivce map.  Error = %v", err)
		return
	}

	if update ||
		serviceMap.ServerGroup != m.serverGroup ||
		m.indexerVersion != serviceMap.IndexerVersion ||
		serviceMap.NodeAddr != m.nodeAddr ||
		serviceMap.ClusterVersion != m.clusterVersion ||
		serviceMap.ExcludeNode != m.excludeNode ||
		serviceMap.StorageMode != m.storageMode {

		m.serverGroup = serviceMap.ServerGroup
		m.indexerVersion = serviceMap.IndexerVersion
		m.nodeAddr = serviceMap.NodeAddr
		m.clusterVersion = serviceMap.ClusterVersion
		m.excludeNode = serviceMap.ExcludeNode
		m.storageMode = serviceMap.StorageMode

		logging.Infof("updator: updating service map.  server group=%v, indexerVersion=%v nodeAddr %v "+
			"clusterVersion %v excludeNode %v storageMode %v", m.serverGroup, m.indexerVersion, m.nodeAddr,
			m.clusterVersion, m.excludeNode, m.storageMode)

		if err := m.manager.repo.BroadcastServiceMap(serviceMap); err != nil {
			logging.Errorf("updator: fail to set service map.  Error = %v", err)
			return
		}
	}
}

func SplitKeyspaceId(keyspaceId string) (string, string, string) {

	var ret []string
	ret = strings.Split(keyspaceId, ":")

	if len(ret) == 3 {
		return ret[0], ret[1], ret[2]
	} else if len(ret) == 1 {
		return ret[0], "", ""
	} else {
		return "", "", ""
	}

}

type indexNameRequest struct {
	defnId    common.IndexDefnId
	startTime int64
	timeout   int64
}
