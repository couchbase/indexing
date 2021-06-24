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
	fdb "github.com/couchbase/indexing/secondary/fdb"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/manager/client"
	mc "github.com/couchbase/indexing/secondary/manager/common"
	//"runtime/debug"
)

//////////////////////////////////////////////////////////////
// concrete type/struct
//////////////////////////////////////////////////////////////

type LifecycleMgr struct {
	repo          *MetadataRepo
	cinfo         *common.ClusterInfoCache
	cinfoClient   *common.ClusterInfoClient
	notifier      MetadataNotifier
	clusterURL    string
	incomings     chan *requestHolder
	expedites     chan *requestHolder
	bootstraps    chan *requestHolder
	outgoings     chan c.Packet
	killch        chan bool
	indexerReady  bool
	builder       *builder
	janitor       *janitor
	updator       *updator
	requestServer RequestServer
	prepareLock   *client.PrepareCreateRequest
	stats         StatsHolder
	done          sync.WaitGroup
	isDone        bool
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
	pendings  map[string][]uint64
	notifych  chan *common.IndexDefn
	batchSize int32
	disable   int32

	commandListener *mc.CommandListener
	listenerDonech  chan bool
}

type janitor struct {
	manager *LifecycleMgr

	commandListener *mc.CommandListener
	listenerDonech  chan bool
	runch           chan bool
}

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

func NewLifecycleMgr(notifier MetadataNotifier, clusterURL string,
	cinfoClient *common.ClusterInfoClient) (*LifecycleMgr, error) {

	cinfo, err := common.FetchNewClusterInfoCache(clusterURL, common.DEFAULT_POOL, "NewLifecycleMgr")
	if err != nil {
		return nil, err
	}
	cinfo.SetUserAgent("LifecycleMgr")

	mgr := &LifecycleMgr{repo: nil,
		cinfo:        cinfo,
		cinfoClient:  cinfoClient,
		notifier:     notifier,
		clusterURL:   clusterURL,
		incomings:    make(chan *requestHolder, 100000),
		expedites:    make(chan *requestHolder, 100000),
		outgoings:    make(chan c.Packet, 100000),
		killch:       make(chan bool),
		bootstraps:   make(chan *requestHolder, 1000),
		indexerReady: false}
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

//
// This is the main event processing loop.  It is important not to having any blocking
// call in this function (e.g. mutex).  If this function is blocked, it will also
// block gometa event processing loop.
//
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
				op == client.OPCODE_CLEANUP_INDEX ||
				op == client.OPCODE_CLEANUP_PARTITION ||
				op == client.OPCODE_RESET_INDEX ||
				op == client.OPCODE_BROADCAST_STATS {
				m.bootstraps <- req
				return
			}
		}

		if op == client.OPCODE_UPDATE_INDEX_INST ||
			op == client.OPCODE_DROP_OR_PRUNE_INSTANCE ||
			op == client.OPCODE_MERGE_PARTITION ||
			op == client.OPCODE_PREPARE_CREATE_INDEX ||
			op == client.OPCODE_COMMIT_CREATE_INDEX ||
			op == client.OPCODE_REBALANCE_RUNNING {
			m.expedites <- req

		} else {
			// for create/drop/build index, always go to the client queue -- which will wait for
			// indexer to be ready.
			m.incomings <- req
		}
	}
}

func (m *LifecycleMgr) GetResponseChannel() <-chan c.Packet {
	return (<-chan c.Packet)(m.outgoings)
}

func (m *LifecycleMgr) processRequest() {

	logging.Debugf("LifecycleMgr.processRequest(): LifecycleMgr is ready to proces request")
	factory := message.NewConcreteMsgFactory()

	m.done.Add(1)
	defer m.done.Done()

	// process any requests form the boostrap phase.   Once indexer is ready, this channel
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

	dispatchExpediates := func() bool {
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
				if dispatchExpediates() {
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
			logging.Infof("lifecycleMgr.dispatchRequest: op %v elapsed %v len(expediates) %v len(incomings) %v len(outgoings) %v",
				client.Op2String(op), time.Now().Sub(start), len(m.expedites), len(m.incomings), len(m.outgoings))
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
		err = m.handleBuildIndexes(content, common.NewUserRequestContext(), true)
	case client.OPCODE_SERVICE_MAP:
		result, err = m.handleServiceMap(content)
	case client.OPCODE_DELETE_BUCKET:
		err = m.handleDeleteBucket(key, content)
	case client.OPCODE_CLEANUP_INDEX:
		err = m.handleCleanupIndexMetadata(content)
	case client.OPCODE_CLEANUP_DEFER_INDEX:
		err = m.handleCleanupDeferIndexFromBucket(key)
	case client.OPCODE_CREATE_INDEX_REBAL:
		err = m.handleCreateIndexScheduledBuild(key, content, common.NewRebalanceRequestContext())
	case client.OPCODE_BUILD_INDEX_REBAL:
		err = m.handleBuildIndexes(content, common.NewRebalanceRequestContext(), false)
	case client.OPCODE_DROP_INDEX_REBAL:
		err = m.handleDeleteIndex(key, common.NewRebalanceRequestContext())
	case client.OPCODE_BUILD_INDEX_RETRY:
		err = m.handleBuildIndexes(content, common.NewUserRequestContext(), true)
	case client.OPCODE_BROADCAST_STATS:
		m.handleNotifyStats(content)
	case client.OPCODE_RESET_INDEX:
		err = m.handleResetIndex(content)
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
		err = m.handleCreateIndex(key, content, common.NewUserRequestContext())
	case client.OPCODE_DROP_INSTANCE:
		err = m.handleDropInstance(content, common.NewUserRequestContext())
	case client.OPCODE_UPDATE_REPLICA_COUNT:
		err = m.handleUpdateReplicaCount(content)
	case client.OPCODE_GET_REPLICA_COUNT:
		result, err = m.handleGetIndexReplicaCount(content)
	case client.OPCODE_CHECK_TOKEN_EXIST:
		result, err = m.handleCheckTokenExist(content)
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

	if prepareCreateIndex.Op == client.PREPARE {
		if m.prepareLock != nil {
			if m.prepareLock.RequesterId != prepareCreateIndex.RequesterId ||
				m.prepareLock.DefnId != prepareCreateIndex.DefnId {

				if m.prepareLock.Timeout > (time.Now().UnixNano() - m.prepareLock.StartTime) {
					logging.Infof("LifecycleMgr.handlePrepareCreateIndex() : Reject %v because another index %v holding lock",
						prepareCreateIndex.DefnId, m.prepareLock.DefnId)
					response := &client.PrepareCreateResponse{Accept: false}
					return client.MarshallPrepareCreateResponse(response)
				}
				logging.Infof("LifecycleMgr.handlePrepareCreateIndex() : Prepare timeout for %v", m.prepareLock.DefnId)
			}
		}

		if _, err := m.repo.GetLocalValue("RebalanceRunning"); err == nil {
			logging.Infof("LifecycleMgr.handlePrepareCreateIndex() : Reject %v because rebalance in progress", prepareCreateIndex.DefnId)
			response := &client.PrepareCreateResponse{Accept: false}
			return client.MarshallPrepareCreateResponse(response)
		}

		if prepareCreateIndex.Name != "" && prepareCreateIndex.Bucket != "" {
			// Check for duplicate index name only if name and bucket name
			// are specified in the request
			existDefn, err := m.repo.GetIndexDefnByName(prepareCreateIndex.Bucket,
				prepareCreateIndex.Name)
			if err != nil {
				logging.Infof("LifecycleMgr.handlePrepareCreateIndex() : Reject "+
					"%v because of error (%v) in GetIndexDefnByName", prepareCreateIndex.DefnId, err)

				response := &client.PrepareCreateResponse{Accept: false}
				return client.MarshallPrepareCreateResponse(response)
			}

			if existDefn != nil {
				logging.Infof("LifecycleMgr.handlePrepareCreateIndex() : Reject "+
					"%v because of duplicate index name with existing defnId %v",
					prepareCreateIndex.DefnId, existDefn.DefnId)

				response := &client.PrepareCreateResponse{Accept: false}
				return client.MarshallPrepareCreateResponse(response)
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
// handle Commit operation
//
func (m *LifecycleMgr) handleCommit(content []byte) ([]byte, error) {

	commit, err := client.UnmarshallCommitCreateRequest(content)
	if err != nil {
		logging.Errorf("LifecycleMgr.handleCommit() : Unable to unmarshall request. Reason = %v", err)
		return nil, err
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

	defnId := commitCreateIndex.DefnId
	definitions := commitCreateIndex.Definitions
	m.prepareLock = nil

	commit, bucketUUID, err := m.processCommitToken(defnId, definitions)
	if commit {
		// If fails to post the command token, the return failure.  If none of the indexer can post the command token,
		// the command token will be malformed and it will get cleaned up by DDLServiceMgr upon rebalancing.
		if err1 := mc.PostCreateCommandToken(defnId, bucketUUID, 0, definitions); err1 != nil {
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
func (m *LifecycleMgr) processCommitToken(defnId common.IndexDefnId, layout map[common.IndexerId][]common.IndexDefn) (bool, string, error) {

	indexerId, err := m.repo.GetLocalIndexerId()
	if err != nil {
		return false, "", fmt.Errorf("Create Index fails.  Internal Error: %v", err)
	}

	if definitions, ok := layout[indexerId]; ok && len(definitions) > 0 {

		defn := definitions[0]
		reqCtx := common.NewUserRequestContext()

		// Create the index instance.   This is to ensure that definiton passes invariant validation (e.g bucket exists).
		// If create index fails due to transient error (indexer in recovery), a token will be placed to retry the operation
		// later.  The definiton is always deleted upon error.
		if err := m.CreateIndexOrInstance(&defn, false, reqCtx); err != nil {
			// If there is error, the defintion will not be created.
			// But if it is recoverable error, then we still want to create the commit token.
			logging.Errorf("LifecycleMgr.processCommitToken() : build index fails.  Reason = %v", err)
			return m.canRetryCreateError(err), "", err
		}

		if !definitions[0].Deferred && len(definitions) == 1 {
			// If there is only one definition, then try to do the build as well.
			retryList, skipList, errList := m.BuildIndexes([]common.IndexDefnId{defnId}, reqCtx, false)

			if len(retryList) != 0 {
				// It is a recoverable error.  Create commit token and return error.
				logging.Errorf("LifecycleMgr.processCommitToken() : build index fails.  Reason = %v", retryList[0])
				return true, "", retryList[0]
			}

			if len(errList) != 0 {
				// It is not a recoverable error.  Do not create commit token and return error.
				logging.Errorf("LifecycleMgr.processCommitToken() : build index fails.  Reason = %v", errList[0])
				m.DeleteIndex(defnId, true, false, reqCtx)
				return false, "", errList[0]
			}

			if len(skipList) != 0 {
				// It is very unlikely to skip an index.  Let create the commit token and retry.
				logging.Errorf("LifecycleMgr.processCommitToken() : index is skipped during build.  Create create-token and retry.")
			}
		}

		// create commit token
		return true, defn.BucketUUID, nil
	}

	// these definitions are not for my indexer, do not create commit token.
	return false, "", nil
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

	commit, numReplica, bucketUUID, err := m.processAddReplicaCommitToken(defnId, definitions)
	if commit {
		// If fails to post the command token, the return failure.  If none of the indexer can post the command token,
		// the command token will be malformed and it will get cleaned up by DDLServiceMgr upon rebalancing.
		if err1 := mc.PostCreateCommandToken(defnId, bucketUUID, requestId, definitions); err1 != nil {
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
	*common.Counter, string, error) {

	indexerId, err := m.repo.GetLocalIndexerId()
	if err != nil {
		return false, nil, "", fmt.Errorf("Alter Index fails.  Internal Error: %v", err)
	}

	if definitions, ok := layout[indexerId]; ok && len(definitions) > 0 {

		// Get the bucket UUID. This is needed for creating commit token.
		defn := definitions[0]
		if err := m.setBucketUUID(&defn); err != nil {
			return false, nil, "", err
		}

		// create commit token
		return true, &defn.NumReplica2, defn.BucketUUID, nil
	}

	// these definitions are not for my indexer, do not create commit token.
	return false, nil, "", nil
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

func (m *LifecycleMgr) handleCreateIndex(key string, content []byte, reqCtx *common.MetadataRequestContext) error {

	defn, err := common.UnmarshallIndexDefn(content)
	if err != nil {
		logging.Errorf("LifecycleMgr.handleCreateIndex() : createIndex fails. Unable to unmarshall index definition. Reason = %v", err)
		return err
	}

	return m.CreateIndexOrInstance(defn, false, reqCtx)
}

func (m *LifecycleMgr) handleCreateIndexScheduledBuild(key string, content []byte,
	reqCtx *common.MetadataRequestContext) error {

	defn, err := common.UnmarshallIndexDefn(content)
	if err != nil {
		logging.Errorf("LifecycleMgr.handleCreateIndexScheduledBuild() : createIndex fails. Unable to unmarshall index definition. Reason = %v", err)
		return err
	}

	// Create index with the scheduled flag.
	return m.CreateIndexOrInstance(defn, true, reqCtx)
}

func (m *LifecycleMgr) CreateIndexOrInstance(defn *common.IndexDefn, scheduled bool,
	reqCtx *common.MetadataRequestContext) error {

	if common.GetBuildMode() != common.ENTERPRISE {
		if defn.NumReplica != 0 {
			err := errors.New("Index Replica not supported in non-Enterprise Edition")
			logging.Errorf("LifecycleMgr.handleCreateIndex() : createIndex fails. Reason = %v", err)
			return err
		}
		if common.IsPartitioned(defn.PartitionScheme) {
			err := errors.New("Index Partitining is not supported in non-Enterprise Edition")
			logging.Errorf("LifecycleMgr.handleCreateIndex() : createIndex fails. Reason = %v", err)
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
		return m.CreateIndexInstance(defn, scheduled, reqCtx)
	}

	return m.CreateIndex(defn, scheduled, reqCtx)
}

func (m *LifecycleMgr) CreateIndex(defn *common.IndexDefn, scheduled bool,
	reqCtx *common.MetadataRequestContext) error {

	/////////////////////////////////////////////////////
	// Verify input parameters
	/////////////////////////////////////////////////////

	if err := m.setBucketUUID(defn); err != nil {
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
		realInst, err := m.FindLocalIndexInst(defn.Bucket, defn.DefnId, realInstId)
		if err != nil {
			logging.Errorf("LifecycleMgr.CreateIndex() : CreateIndex fails. Reason = %v", err)
			return err
		}

		// Lifeccyle mgr is the source of truth of metadata.  If it sees that there is an existing index instance in
		// DELETED state, it means that indexer has not been able to delete this instance.   Given that index defn does
		// not exist, we can simply delete the index instance without notifying indexer.
		if realInst != nil && common.IndexState(realInst.State) == common.INDEX_STATE_DELETED {
			logging.Infof("LifecycleMgr.CreateIndex() : Remove deleted index instances for index %v", defn.DefnId)
			m.repo.deleteIndexFromTopology(defn.Bucket, defn.DefnId)
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
	if err := m.updateIndexState(defn.Bucket, defn.DefnId, instId, common.INDEX_STATE_READY); err != nil {
		logging.Errorf("LifecycleMgr.CreateIndex() : createIndex fails. Reason = %v", err)
		m.DeleteIndex(defn.DefnId, true, false, reqCtx)
		return err
	}

	/////////////////////////////////////////////////////
	// Build Index
	/////////////////////////////////////////////////////

	// Run index build
	if !defn.Deferred && scheduled {
		if m.notifier != nil {
			logging.Debugf("LifecycleMgr.CreateIndex() : start Index Build")

			retryList, skipList, errList := m.BuildIndexes([]common.IndexDefnId{defn.DefnId}, reqCtx, false)

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

func (m *LifecycleMgr) setBucketUUID(defn *common.IndexDefn) error {

	// Fetch bucket UUID.   Note that this confirms that the bucket has existed, but it cannot confirm if the bucket
	// is still existing in the cluster (due to race condition or network partitioned).
	//
	// Lifecycle manager is a singleton that ensures all metadata operation is serialized.  Therefore, a
	// call to verifyBucket() here  will also make sure that all existing indexes belong to the same bucket UUID.
	// To esnure verifyBucket can succeed, indexes from stale bucket must be cleaned up (eventually).
	//
	bucketUUID, err := m.verifyBucket(defn.Bucket)
	if err != nil || bucketUUID == common.BUCKET_UUID_NIL {
		if err == nil {
			err = errors.New("Bucket not found")
		}
		return fmt.Errorf("Bucket does not exist or temporarily unavailable for creating new index."+
			" Please retry the operation at a later time (err=%v).", err)
	}

	if len(defn.BucketUUID) != 0 && defn.BucketUUID != bucketUUID {
		return fmt.Errorf("Bucket UUID has changed.  Bucket may have been dropped and recreatd.")
	}

	defn.BucketUUID = bucketUUID
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

	existDefn, err := m.repo.GetIndexDefnByName(defn.Bucket, defn.Name)
	if err != nil {
		logging.Errorf("LifecycleMgr.CreateIndexInstance() : createIndex fails. Reason = %v", err)
		return err
	}

	if existDefn != nil {
		// The index already exist in this node.  Make sure there is no overlapping partition.
		topology, err := m.repo.GetTopologyByBucket(existDefn.Bucket)
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

	existDefn, err := m.repo.GetIndexDefnByName(defn.Bucket, defn.Name)
	if err != nil {
		logging.Errorf("LifecycleMgr.verifyDuplicateDefn() : createIndex fails. Reason = %v", err)
		return nil, err
	}

	if existDefn != nil {

		topology, err := m.repo.GetTopologyByBucket(existDefn.Bucket)
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

func (m *LifecycleMgr) handleBuildIndexes(content []byte, reqCtx *common.MetadataRequestContext, retry bool) error {

	list, err := client.UnmarshallIndexIdList(content)
	if err != nil {
		logging.Errorf("LifecycleMgr.handleBuildIndexes() : buildIndex fails. Unable to unmarshall index list. Reason = %v", err)
		return err
	}

	input := make([]common.IndexDefnId, len(list.DefnIds))
	for i, id := range list.DefnIds {
		input[i] = common.IndexDefnId(id)
	}

	retryList, skipList, errList := m.BuildIndexes(input, reqCtx, retry)

	if len(retryList) != 0 || len(skipList) != 0 || len(errList) != 0 {
		msg := "Build index fails."

		if len(retryList) == 1 {
			msg += fmt.Sprintf(" %v", retryList[0])
		}

		if len(errList) == 1 {
			msg += fmt.Sprintf(" %v.", errList[0])
		}

		if len(retryList) > 1 {
			msg += " Some index will be retried building in the background."
		}

		if len(errList) > 1 {
			msg += " Some index cannot be built due to errors."
		}

		if len(skipList) != 0 {
			msg += " Some index cannot be built since it may not exist.  Please check if the list of indexes are valid."
		}

		if len(errList) > 1 || len(retryList) > 1 {
			msg += " For more details, please check index status."
		}

		return errors.New(msg)
	}

	return nil
}

func (m *LifecycleMgr) BuildIndexes(ids []common.IndexDefnId,
	reqCtx *common.MetadataRequestContext, retry bool) ([]error, []common.IndexDefnId, []error) {

	retryList := ([]*common.IndexDefn)(nil)
	retryErrList := ([]error)(nil)
	errList := ([]error)(nil)
	skipList := ([]common.IndexDefnId)(nil)

	instIdList := []common.IndexInstId(nil)
	defnIdMap := make(map[common.IndexDefnId]bool)
	buckets := []string(nil)
	inst2DefnMap := make(map[common.IndexInstId]common.IndexDefnId)

	for _, id := range ids {

		if defnIdMap[id] {
			logging.Infof("LifecycleMgr.handleBuildIndexes() : Duplicate index definition in the build list. Skip this index %v.", id)
			continue
		}
		defnIdMap[id] = true

		defn, err := m.repo.GetIndexDefnById(id)
		if err != nil {
			logging.Errorf("LifecycleMgr.handleBuildIndexes() : buildIndex fails. Reason = %v. Skip this index.", err)
			skipList = append(skipList, id)
			continue
		}
		if defn == nil {
			logging.Warnf("LifecycleMgr.handleBuildIndexes() : index %v does not exist. Skip this index.", id)
			skipList = append(skipList, id)
			continue
		}

		insts, err := m.FindAllLocalIndexInst(defn.Bucket, id)
		if len(insts) == 0 || err != nil {
			logging.Errorf("LifecycleMgr.handleBuildIndexes: Failed to find index instance (%v, %v).  Skip this index.", defn.Name, defn.Bucket)
			skipList = append(skipList, id)
			continue
		}

		for _, inst := range insts {

			if inst.State != uint32(common.INDEX_STATE_READY) {
				logging.Warnf("LifecycleMgr.handleBuildIndexes: index instance (%v, %v, %v) is not in ready state.  Skip this index.",
					defn.Name, defn.Bucket, inst.ReplicaId)
				continue
			}

			//Schedule the build
			if err := m.SetScheduledFlag(defn.Bucket, id, common.IndexInstId(inst.InstId), true); err != nil {
				msg := fmt.Sprintf("LifecycleMgr.handleBuildIndexes: Unable to set scheduled flag in index instance (%v, %v).", defn.Name, defn.Bucket)
				logging.Warnf("%v  Will try to build index now, but it will not be able to retry index build upon server restart.", msg)
			}

			// Reset any previous error
			m.UpdateIndexInstance(defn.Bucket, id, common.IndexInstId(inst.InstId), common.INDEX_STATE_NIL, common.NIL_STREAM, "", nil,
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

		if errMap := m.notifier.OnIndexBuild(instIdList, buckets, reqCtx); len(errMap) != 0 {
			logging.Errorf("LifecycleMgr.handleBuildIndexes() : buildIndex fails. Reason = %v", errMap)

			for instId, build_err := range errMap {

				defnId, ok := inst2DefnMap[instId]
				if !ok {
					logging.Warnf("LifecycleMgr.handleBuildIndexes() : Cannot find index defn for index inst %v when processing build error.")
					continue
				}

				defn, err := m.repo.GetIndexDefnById(defnId)
				if err != nil || defn == nil {
					logging.Warnf("LifecycleMgr.handleBuildIndexes() : Cannot find index defn for index inst %v when processing build error.")
					continue
				}

				inst, err := m.FindLocalIndexInst(defn.Bucket, defnId, instId)
				if inst != nil && err == nil {
					if m.canRetryBuildError(inst, build_err, retry) {
						build_err = errors.New(fmt.Sprintf("Index %v will retry building in the background for reason: %v.", defn.Name, build_err.Error()))
					}
					m.UpdateIndexInstance(defn.Bucket, defnId, common.IndexInstId(inst.InstId), common.INDEX_STATE_NIL,
						common.NIL_STREAM, build_err.Error(), nil, inst.RState, nil, nil, -1)
				} else {
					logging.Infof("LifecycleMgr.handleBuildIndexes() : Failed to persist, error in index instance (%v, %v, %v).",
						defn.Bucket, defn.Name, inst.ReplicaId)
				}

				if m.canRetryBuildError(inst, build_err, retry) {
					logging.Infof("LifecycleMgr.handleBuildIndexes() : Encountered build error.  Retry building index (%v, %v, %v) at later time.",
						defn.Bucket, defn.Name, inst.ReplicaId)

					if inst != nil && !inst.Scheduled {
						if err := m.SetScheduledFlag(defn.Bucket, defnId, common.IndexInstId(inst.InstId), true); err != nil {
							msg := fmt.Sprintf("LifecycleMgr.handleBuildIndexes: Unable to set scheduled flag in index instance (%v, %v, %v).",
								defn.Name, defn.Bucket, inst.ReplicaId)
							logging.Warnf("%v  Will try to build index now, but it will not be able to retry index build upon server restart.", msg)
						}
					}

					retryList = append(retryList, defn)
					retryErrList = append(retryErrList, build_err)
				} else {
					errList = append(errList, errors.New(fmt.Sprintf("Index %v fails to build for reason: %v", defn.Name, build_err)))
				}
			}

			// schedule index for retry
			for _, defn := range retryList {
				m.builder.notifych <- defn
			}
		}
	}

	logging.Debugf("LifecycleMgr.handleBuildIndexes() : buildIndex completes")

	return retryErrList, skipList, errList
}

//-----------------------------------------------------------
// Delete Index
//-----------------------------------------------------------

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
	insts, err := m.FindAllLocalIndexInst(defn.Bucket, id)
	if err != nil {
		// Cannot read bucket topology.  Likely a transient error.  But without index inst, we cannot
		// proceed without letting indexer to clean up.
		logging.Warnf("LifecycleMgr.handleDeleteIndex() : Encountered error during delete index. Error = %v", err)
		return err
	}

	hasError := false
	for _, inst := range insts {
		// updateIndexState will not return an error if there is no index inst
		if err := m.updateIndexState(defn.Bucket, defn.DefnId, common.IndexInstId(inst.InstId), common.INDEX_STATE_DELETED); err != nil {
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
		insts, err := m.FindAllLocalIndexInst(defn.Bucket, id)
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
	m.repo.deleteIndexFromTopology(defn.Bucket, defn.DefnId)

	logging.Debugf("LifecycleMgr.DeleteIndex() : deleted index:  bucket : %v bucket uuid %v name %v",
		defn.Bucket, defn.BucketUUID, defn.Name)
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
	inst, err := m.FindLocalIndexInst(change.Bucket, common.IndexDefnId(change.DefnId), common.IndexInstId(change.InstId))
	if err != nil {
		return err
	}
	if inst == nil {
		return nil
	}

	state := inst.State
	scheduled := inst.Scheduled

	// update the index instance
	if err := m.UpdateIndexInstance(change.Bucket, common.IndexDefnId(change.DefnId), common.IndexInstId(change.InstId),
		common.IndexState(change.State), common.StreamId(change.StreamId), change.Error, change.BuildTime, change.RState, change.Partitions,
		change.Versions, change.InstVersion); err != nil {
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
	topology, err := m.repo.GetTopologyByBucket(bucket)
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
// Cleanup Defer Index
//-----------------------------------------------------------

//
// Cleanup any defer index from invalid bucket.
//
func (m *LifecycleMgr) handleCleanupDeferIndexFromBucket(bucket string) error {

	// Get bucket UUID.  if err==nil, bucket uuid is BUCKET_UUID_NIL for non-existent bucket.
	currentUUID, err := m.getBucketUUID(bucket)
	if err != nil {
		// If err != nil, then cannot connect to fetch bucket info.  Do not attempt to delete index.
		return nil
	}

	topology, err := m.repo.GetTopologyByBucket(bucket)
	if err == nil && topology != nil {

		hasValidActiveIndex := false
		for _, defnRef := range topology.Definitions {
			// Check for index with active stream.  If there is any index with active stream, all
			// index in the bucket will be deleted when the stream is closed due to bucket delete.

			for _, instRef := range defnRef.Instances {
				if instRef.State != uint32(common.INDEX_STATE_DELETED) &&
					common.StreamId(instRef.StreamId) != common.NIL_STREAM {
					hasValidActiveIndex = true
					break
				}
			}
		}

		if !hasValidActiveIndex {
			deleteToken := false

			for _, defnRef := range topology.Definitions {
				if defn, err := m.repo.GetIndexDefnById(common.IndexDefnId(defnRef.DefnId)); err == nil && defn != nil {
					if defn.BucketUUID != currentUUID {
						for _, instRef := range defnRef.Instances {
							if instRef.State != uint32(common.INDEX_STATE_DELETED) &&
								common.StreamId(instRef.StreamId) == common.NIL_STREAM {
								deleteToken = true
								if err := m.DeleteIndex(common.IndexDefnId(defn.DefnId), true, false, common.NewUserRequestContext()); err != nil {
									logging.Errorf("LifecycleMgr.handleCleanupDeferIndexFromBucket: Encountered error %v", err)
									continue
								}
								mc.DeleteAllCreateCommandToken(common.IndexDefnId(defn.DefnId))
								break
							}
						}
					}
				}
			}

			// VerifyBucket ensures that all index have the same bucket UUID.  So if one index has bucket UUID mismatch, then all
			// can be deleted.
			if deleteToken {
				m.deleteCreateTokenForBucket(bucket)
			}
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

			filtered := make(common.Statistics)
			for key, value := range stats {
				if strings.Contains(key, "num_docs_pending") ||
					strings.Contains(key, "num_docs_queued") ||
					strings.Contains(key, "last_rollback_time") ||
					strings.Contains(key, "progress_stat_time") {

					filtered[key] = value
				}
			}
			m.stats.Set(&filtered)

		} else {
			logging.Errorf("lifecycleMgr: fail to marshall index stats.  Error = %v", err)
		}
	}
}

// Broadcast stats in a go-routine
func (m *LifecycleMgr) broadcastStats() {

	m.done.Add(1)
	defer m.done.Done()

	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			filtered := m.stats.Get()
			if filtered != nil {
				idxStats := &client.IndexStats{Stats: *filtered}
				if err := m.repo.BroadcastIndexStats(idxStats); err != nil {
					logging.Errorf("lifecycleMgr: fail to send index stats.  Error = %v", err)
				}
			}

		case <-m.killch:
			// lifecycle manager shutdown
			logging.Infof("LifecycleMgr.broadcastStats(): received kill signal. Shutting down broadcastStats routine.")
			return
		}
	}
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
		logging.Errorf("LifecycleMgr.handleResetIndex() : Fails to upgrade index (%v, %v). Reason = %v", defn.Bucket, defn.Name, err)
		return err
	}

	//
	// Restore index instance (as if index is created again)
	//

	topology, err := m.repo.CloneTopologyByBucket(defn.Bucket)
	if err != nil {
		logging.Errorf("LifecycleMgr.handleResetIndex() : Fails to upgrade index (%v, %v). Reason = %v", defn.Bucket, defn.Name, err)
		return err
	}

	rinst, err := m.FindLocalIndexInst(defn.Bucket, defn.DefnId, inst.InstId)
	if err != nil {
		logging.Errorf("LifecycleMgr.handleResetIndex() : Fails to upgrade index (%v, %v). Reason = %v", defn.Bucket, defn.Name, err)
		return err
	}

	if rinst == nil {
		logging.Errorf("LifecycleMgr.handleResetIndex() : Fails to upgrade index (%v, %v). Index instance does not exist.", defn.Bucket, defn.Name)
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

	if err := m.repo.SetTopologyByBucket(defn.Bucket, topology); err != nil {
		// Topology update is in place.  If there is any error, SetTopologyByBucket will purge the cache copy.
		logging.Errorf("LifecycleMgr.handleResetIndex() : index instance (%v, %v) update fails. Reason = %v", defn.Bucket, defn.Name, err)
		return err
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
	return nil
}

//-----------------------------------------------------------
// Create Index Instance
//-----------------------------------------------------------

func (m *LifecycleMgr) CreateIndexInstance(defn *common.IndexDefn, scheduled bool,
	reqCtx *common.MetadataRequestContext) error {

	/////////////////////////////////////////////////////
	// Verify input parameters
	/////////////////////////////////////////////////////

	if err := m.verifyOverlapPartition(defn, reqCtx); err != nil {
		return err
	}

	if err := m.setBucketUUID(defn); err != nil {
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
		realInst, err := m.FindLocalIndexInst(defn.Bucket, defn.DefnId, realInstId)
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
	if err := m.updateIndexState(defn.Bucket, defn.DefnId, instId, common.INDEX_STATE_READY); err != nil {
		logging.Errorf("LifecycleMgr.CreateIndexInstance() : CreateIndexInstance fails. Reason = %v", err)
		m.DeleteIndexInstance(defn.DefnId, instId, false, false, false, reqCtx)
		return err
	}

	/////////////////////////////////////////////////////
	// Build Index
	/////////////////////////////////////////////////////

	// Run index build
	if !defn.Deferred && scheduled {
		if m.notifier != nil {
			logging.Debugf("LifecycleMgr.CreateIndexInstance() : start Index Build")

			retryList, skipList, errList := m.BuildIndexes([]common.IndexDefnId{defn.DefnId}, reqCtx, false)

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

		existDefn, err := m.repo.GetIndexDefnByName(defn.Bucket, defn.Name)
		if err != nil {
			logging.Errorf("LifecycleMgr.CreateIndexInstance() : createIndex fails. Reason = %v", err)
			return err
		}

		if existDefn != nil {
			// The index already exist in this node.  Make sure there is no overlapping partition.
			topology, err := m.repo.GetTopologyByBucket(existDefn.Bucket)
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

	inst, err := m.FindLocalIndexInst(defn.Bucket, id, instId)
	if err != nil {
		logging.Errorf("LifecycleMgr.DeleteOrPruneIndexInstance() : Encountered error during delete index. Error = %v", err)
		return err
	}

	if inst == nil {
		inst, err := m.FindLocalIndexInst(defn.Bucket, id, defn.RealInstId)
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

	insts, err := m.FindAllLocalIndexInst(defn.Bucket, id)
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
	if err := m.updateIndexState(defn.Bucket, id, instId, common.INDEX_STATE_DELETED); err != nil {
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
	m.repo.deleteInstanceFromTopology(defn.Bucket, id, instId)

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
	realInst, err := m.FindLocalIndexInst(defn.Bucket, defn.DefnId, common.IndexInstId(inst.RealInstId))
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
	numProxy, err := m.findNumValidProxy(defn.Bucket, defn.DefnId, common.IndexInstId(realInst.InstId))
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
	inst, err := m.FindLocalIndexInst(defn.Bucket, id, srcInstId)
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
	inst, err = m.FindLocalIndexInst(defn.Bucket, id, tgtInstId)
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

	m.repo.mergePartitionFromTopology(string(indexerId), defn.Bucket, id, srcInstId, srcRState, tgtInstId, tgtInstVersion,
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

	inst, err := m.FindLocalIndexInst(defn.Bucket, id, instId)
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
	numProxy, err := m.findNumValidProxy(defn.Bucket, id, instId)
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

	if err := m.repo.splitPartitionFromTopology(defn.Bucket, id, instId, tombstoneInstId, newPartitions); err != nil {
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
func (m *LifecycleMgr) findNumValidProxy(bucket string, defnId common.IndexDefnId, instId common.IndexInstId) (int, error) {

	insts, err := m.FindAllLocalIndexInst(bucket, defnId)
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

func (m *LifecycleMgr) canRetryBuildError(inst *IndexInstDistribution, err error, retry bool) bool {

	if inst == nil || inst.RState != uint32(common.REBAL_ACTIVE) {
		return false
	}

	indexerErr, ok := err.(*common.IndexerError)
	if !ok {
		return true
	}

	if indexerErr.Code == common.IndexNotExist ||
		indexerErr.Code == common.InvalidBucket ||
		indexerErr.Code == common.BucketEphemeral ||
		(!retry && indexerErr.Code == common.RebalanceInProgress) ||
		indexerErr.Code == common.IndexAlreadyExist ||
		indexerErr.Code == common.IndexInvalidState {
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
		indexerErr.Code == common.IndexInvalidState {
		return false
	}

	return true
}

func (m *LifecycleMgr) UpdateIndexInstance(bucket string, defnId common.IndexDefnId, instId common.IndexInstId,
	state common.IndexState, streamId common.StreamId, errStr string, buildTime []uint64, rState uint32,
	partitions []uint64, versions []int, version int) error {

	topology, err := m.repo.CloneTopologyByBucket(bucket)
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
		if err := m.repo.SetTopologyByBucket(bucket, topology); err != nil {
			// Topology update is in place.  If there is any error, SetTopologyByBucket will purge the cache copy.
			logging.Errorf("LifecycleMgr.handleTopologyChange() : index instance update fails. Reason = %v", err)
			return err
		}
	}

	return nil
}

func (m *LifecycleMgr) SetScheduledFlag(bucket string, defnId common.IndexDefnId, instId common.IndexInstId, scheduled bool) error {

	topology, err := m.repo.CloneTopologyByBucket(bucket)
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
		if err := m.repo.SetTopologyByBucket(bucket, topology); err != nil {
			// Topology update is in place.  If there is any error, SetTopologyByBucket will purge the cache copy.
			logging.Errorf("LifecycleMgr.SetScheduledFlag() : index instance update fails. Reason = %v", err)
			return err
		}
	}

	return nil
}

func (m *LifecycleMgr) FindAllLocalIndexInst(bucket string, defnId common.IndexDefnId) ([]IndexInstDistribution, error) {

	topology, err := m.repo.GetTopologyByBucket(bucket)
	if err != nil {
		logging.Errorf("LifecycleMgr.FindAllLocalIndexInst() : Cannot read topology metadata. Reason = %v", err)
		return nil, err
	}
	if topology == nil {
		logging.Infof("LifecycleMgr.FindAllLocalIndexInst() : Index Inst does not exist %v", defnId)
		return nil, nil
	}

	return topology.GetIndexInstancesByDefn(defnId), nil
}

func (m *LifecycleMgr) FindLocalIndexInst(bucket string, defnId common.IndexDefnId, instId common.IndexInstId) (*IndexInstDistribution, error) {

	insts, err := m.FindAllLocalIndexInst(bucket, defnId)
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

func (m *LifecycleMgr) updateIndexState(bucket string, defnId common.IndexDefnId, instId common.IndexInstId, state common.IndexState) error {

	topology, err := m.repo.CloneTopologyByBucket(bucket)
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
		if err := m.repo.SetTopologyByBucket(bucket, topology); err != nil {
			// Topology update is in place.  If there is any error, SetTopologyByBucket will purge the cache copy.
			logging.Errorf("LifecycleMgr.updateIndexState() : fail to update state of index instance.  Reason = %v", err)
			return err
		}
	}

	return nil
}

func (m *LifecycleMgr) canBuildIndex(bucket string) bool {

	t, _ := m.repo.GetTopologyByBucket(bucket)
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

	m.cinfo.Lock()
	defer m.cinfo.Unlock()

	if err := m.cinfo.Fetch(); err != nil {
		return nil, err
	}

	srvMap := new(client.ServiceMap)

	id, err := m.repo.GetLocalIndexerId()
	if err != nil {
		return nil, err
	}
	srvMap.IndexerId = string(id)

	srvMap.ScanAddr, err = m.cinfo.GetLocalServiceAddress(common.INDEX_SCAN_SERVICE)
	if err != nil {
		return nil, err
	}

	srvMap.HttpAddr, err = m.cinfo.GetLocalServiceAddress(common.INDEX_HTTP_SERVICE)
	if err != nil {
		return nil, err
	}

	srvMap.AdminAddr, err = m.cinfo.GetLocalServiceAddress(common.INDEX_ADMIN_SERVICE)
	if err != nil {
		return nil, err
	}

	srvMap.NodeAddr, err = m.cinfo.GetLocalHostAddress()
	if err != nil {
		return nil, err
	}

	srvMap.ServerGroup, err = m.cinfo.GetLocalServerGroup()
	if err != nil {
		return nil, err
	}

	srvMap.NodeUUID, err = m.repo.GetLocalNodeUUID()
	if err != nil {
		return nil, err
	}

	srvMap.IndexerVersion = common.INDEXER_CUR_VERSION

	srvMap.ClusterVersion = m.cinfo.GetClusterVersion()

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
func (m *LifecycleMgr) getBucketUUID(bucket string) (string, error) {
	count := 0
RETRY:
	cinfo := m.cinfoClient.GetClusterInfoCache()
	cinfo.RLock()
	uuid := cinfo.GetBucketUUID(bucket)
	if uuid == common.BUCKET_UUID_NIL && count < 5 {
		count++
		cinfo.RUnlock()
		time.Sleep(time.Duration(100) * time.Millisecond)
		// Refresh cluster info cache
		err := cinfo.FetchWithLock()
		if err != nil {
			return common.BUCKET_UUID_NIL, err
		}
		goto RETRY
	}

	defer cinfo.RUnlock()
	return uuid, nil
}

// This function ensures:
// 1) Bucket exists
// 2) Existing Index Definition matches the UUID of exixisting bucket
// 3) If bucket does not exist AND there is no existing definition, this returns common.BUCKET_UUID_NIL
//
func (m *LifecycleMgr) verifyBucket(bucket string) (string, error) {

	// If this function returns an error, then it cannot fetch bucket UUID.
	// Otherwise, if it returns BUCKET_UUID_NIL, it means none of the node recognize this bucket.
	currentUUID, err := m.getBucketUUID(bucket)
	if err != nil {
		return common.BUCKET_UUID_NIL, err
	}

	topology, err := m.repo.GetTopologyByBucket(bucket)
	if err != nil {
		return common.BUCKET_UUID_NIL, err
	}

	if topology != nil {
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

	// topology is either nil or all index defn matches bucket UUID
	// if topology is nil, then currentUUID == common.BUCKET_UUID_NIL
	return currentUUID, nil
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

		defn, err := m.manager.repo.GetIndexDefnById(command.DefnId)
		if err != nil {
			retryList[entry] = command
			logging.Warnf("janitor: Failed to drop index upon cleanup.  Skp command %v.  Internal Error = %v.", entry, err)
			continue
		}

		// index may already be deleted or does not exist in this node
		if defn == nil {
			continue
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

		inst, err := m.manager.FindLocalIndexInst(defn.Bucket, command.DefnId, command.InstId)
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

		insts, err := m.manager.FindAllLocalIndexInst(defn.Bucket, defn.DefnId)
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
			m.commandListener = mc.NewCommandListener(m.listenerDonech, false, false, true, true)
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
		manager:         mgr,
		commandListener: mc.NewCommandListener(donech, false, false, true, true),
		listenerDonech:  donech,
		runch:           make(chan bool),
	}

	return janitor
}

//////////////////////////////////////////////////////////////
// Lifecycle Mgr - builder
// Buidler builds index in the background.  This
// operation is idempotent -- unless the metadata store is
// corrupted.
//////////////////////////////////////////////////////////////

func (s *builder) run() {

	s.manager.done.Add(1)
	defer s.manager.done.Done()

	// start listener
	s.commandListener.ListenTokens()
	time.Sleep(time.Minute)

	// wait for indexer bootstrap to complete before recover
	s.recover()

	// check if there is any pending index build every second
	ticker := time.NewTicker(time.Millisecond * 200)
	defer ticker.Stop()

	for {
		select {
		case defn := <-s.notifych:
			logging.Infof("builder:  Received new index build request %v.  Schedule to build index for bucket %v", defn.DefnId, defn.Bucket)
			s.addPending(defn.Bucket, uint64(defn.DefnId))

		case <-ticker.C:
			s.processBuildToken(false)

			// Sleep before checking for index to build.  When a recovered node starts up, it needs to wait until
			// the rebalancing token is saved.  This is to avoid the bulider to get ahead of the rebalancer.
			// Otherwise, rebalancer could fail if builder has issued an index build ahead of the rebalancer.
			timer := time.NewTimer(time.Second * 120)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-s.manager.killch:
				s.commandListener.Close()
				logging.Infof("builder: go-routine terminates.")
				return
			}

			buildList, quota := s.getBuildList()

			for _, bucket := range buildList {
				quota = s.tryBuildIndex(bucket, quota)
			}

		case <-s.manager.killch:
			s.commandListener.Close()
			logging.Infof("builder: go-routine terminates.")
			return

		case <-s.listenerDonech:
			s.listenerDonech = make(chan bool)
			s.commandListener = mc.NewCommandListener(s.listenerDonech, false, true, false, false)
			s.commandListener.ListenTokens()
		}
	}
}

func (s *builder) getBuildList() ([]string, int32) {

	// get quota
	quota, skipList := s.getQuota()

	// filter bucket that is not available
	buildList := ([]string)(nil)
	for bucket, _ := range s.pendings {
		if _, ok := skipList[bucket]; !ok {
			buildList = append(buildList, bucket)
		}
	}

	// sort buildList by ascending order
	for i := 0; i < len(buildList)-1; i++ {
		for j := i + 1; j < len(buildList); j++ {
			bucket_i := buildList[i]
			bucket_j := buildList[j]

			if len(s.pendings[bucket_i]) < len(s.pendings[bucket_j]) {
				tmp := buildList[i]
				buildList[i] = buildList[j]
				buildList[j] = tmp
			}
		}
	}

	// sort buildList based on closest to quota
	for i := 0; i < len(buildList)-1; i++ {
		for j := i + 1; j < len(buildList); j++ {
			bucket_i := buildList[i]
			bucket_j := buildList[j]

			if math.Abs(float64(len(s.pendings[bucket_i])-int(quota))) > math.Abs(float64(len(s.pendings[bucket_j])-int(quota))) {
				tmp := buildList[i]
				buildList[i] = buildList[j]
				buildList[j] = tmp
			}
		}
	}

	return buildList, quota
}

func (s *builder) addPending(bucket string, id uint64) bool {

	for _, id2 := range s.pendings[bucket] {
		if id2 == id {
			return false
		}
	}

	s.pendings[bucket] = append(s.pendings[bucket], uint64(id))
	return true
}

func (s *builder) tryBuildIndex(bucket string, quota int32) int32 {

	newQuota := quota

	defnIds := s.pendings[bucket]
	if len(defnIds) != 0 {
		// This is a pre-cautionary check if there is any index being
		// built for the bucket.   The authortative check is done by indexer.
		if s.manager.canBuildIndex(bucket) {

			buildList := ([]uint64)(nil)
			buildMap := make(map[uint64]bool)

			pendingList := make([]uint64, len(defnIds))
			copy(pendingList, defnIds)

			for _, defnId := range defnIds {

				if newQuota == 0 {
					break
				}

				pendingList = pendingList[1:]

				defn, err := s.manager.repo.GetIndexDefnById(common.IndexDefnId(defnId))
				if defn == nil || err != nil {
					logging.Warnf("builder: Failed to find index definition (%v, %v).  Skipping.", defnId, bucket)
					continue
				}

				insts, err := s.manager.FindAllLocalIndexInst(bucket, common.IndexDefnId(defnId))
				if len(insts) == 0 || err != nil {
					logging.Warnf("builder: Failed to find index instance (%v, %v).  Skipping.", defnId, bucket)
					continue
				}

				for _, inst := range insts {

					if newQuota == 0 {
						break
					}

					if inst.State == uint32(common.INDEX_STATE_READY) {
						if !s.disableBuild() || len(inst.OldStorageMode) != 0 {
							// build index if
							// 1) background index build is enabled
							// 2) index build is due to upgrade
							if _, ok := buildMap[defnId]; !ok {
								buildList = append(buildList, defnId)
								buildMap[defnId] = true
							}
							newQuota = newQuota - 1
						} else {
							// put it back to the pending list if index build is disable
							pendingList = append(pendingList, defnId)
							logging.Warnf("builder: Background build is disabled.  Will retry building index (%v, %v) in next iteration.", defnId, bucket)
						}
					} else {
						logging.Warnf("builder: Index instance (%v, %v) is not in READY state.  Skipping.", defnId, bucket)
					}
				}
			}

			if len(pendingList) == 0 {
				pendingList = nil
			}

			if len(buildList) != 0 {

				idList := &client.IndexIdList{DefnIds: buildList}
				key := fmt.Sprintf("%d", idList.DefnIds[0])
				content, err := client.MarshallIndexIdList(idList)
				if err != nil {
					logging.Warnf("builder: Failed to marshall index defnIds during index build.  Error = %v. Retry later.", err)
					return quota
				}

				logging.Infof("builder: Try build index for bucket %v. Index %v", bucket, idList)

				// Clean up the map.  If there is any index that needs retry, they will be put into the notifych again.
				// Once this function is done, the map will be populated again from the notifych.
				s.pendings[bucket] = pendingList

				// If any of the index cannot be built, those index will be skipped by lifecycle manager, so it
				// will send the rest of the indexes to the indexer.  An index cannot be built if it does not have
				// an index instance or the index instance is not in READY state.
				if err := s.manager.requestServer.MakeRequest(client.OPCODE_BUILD_INDEX_RETRY, key, content); err != nil {
					logging.Warnf("builder: Failed to build index.  Error = %v.", err)
				}

				logging.Infof("builder: pending definitons to be build %v.", pendingList)

			} else {

				// Clean up the map.  If there is any index that needs retry, they will be put into the notifych again.
				// Once this function is done, the map will be populated again from the notifych.
				s.pendings[bucket] = pendingList
			}
		}
	}

	return newQuota
}

func (s *builder) getQuota() (int32, map[string]bool) {

	quota := atomic.LoadInt32(&s.batchSize)
	skipList := make(map[string]bool)

	metaIter, err := s.manager.repo.NewIterator()
	if err != nil {
		logging.Warnf("builder.getQuota():  Unable to read from metadata repository. Skipping quota check")
		return quota, skipList
	}
	defer metaIter.Close()

	for _, defn, err := metaIter.Next(); err == nil; _, defn, err = metaIter.Next() {

		insts, err := s.manager.FindAllLocalIndexInst(defn.Bucket, defn.DefnId)
		if len(insts) == 0 || err != nil {
			logging.Warnf("builder.getQuota: Unable to read index instance for definition (%v, %v).   Skipping index for quota check",
				defn.Bucket, defn.Name)
			continue
		}

		for _, inst := range insts {
			if inst.State == uint32(common.INDEX_STATE_INITIAL) || inst.State == uint32(common.INDEX_STATE_CATCHUP) {
				quota = quota - 1
				skipList[defn.Bucket] = true
			}
		}
	}

	return quota, skipList
}

func (s *builder) processBuildToken(bootstrap bool) {

	entries := s.commandListener.GetNewBuildTokens()
	retryList := make(map[string]*mc.BuildCommandToken)

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

		insts, err := s.manager.FindAllLocalIndexInst(defn.Bucket, defn.DefnId)
		if err != nil {
			retryList[entry] = command
			logging.Warnf("builder: Unable to read index instance for definition (%v, %v).   Skipping ...",
				defn.Bucket, defn.Name)
			continue
		}

		for _, inst := range insts {

			if inst.State == uint32(common.INDEX_STATE_READY) {
				logging.Infof("builder: Processing build token %v", entry)

				if s.addPending(defn.Bucket, uint64(defn.DefnId)) {
					logging.Infof("builder: Schedule index build for (%v, %v).", defn.Bucket, defn.Name)
				}
			}
		}
	}

	for path, token := range retryList {
		s.commandListener.AddNewBuildToken(path, token)
	}
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

		insts, err := s.manager.FindAllLocalIndexInst(defn.Bucket, defn.DefnId)
		if len(insts) == 0 || err != nil {
			logging.Errorf("builder: Unable to read index instance for definition (%v, %v).   Skipping ...",
				defn.Bucket, defn.Name)
			continue
		}

		for _, inst := range insts {

			if inst.Scheduled && inst.State == uint32(common.INDEX_STATE_READY) {
				if s.addPending(defn.Bucket, uint64(defn.DefnId)) {
					logging.Infof("builder: Schedule index build for (%v, %v, %v).", defn.Bucket, defn.Name, inst.ReplicaId)
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

func newBuilder(mgr *LifecycleMgr) *builder {

	donech := make(chan bool)

	builder := &builder{
		manager:         mgr,
		pendings:        make(map[string][]uint64),
		notifych:        make(chan *common.IndexDefn, 10000),
		batchSize:       int32(common.SystemConfig["indexer.settings.build.batch_size"].Int()),
		commandListener: mc.NewCommandListener(donech, false, true, false, false),
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

//////////////////////////////////////////////////////////////
// Lifecycle Mgr - udpator
//////////////////////////////////////////////////////////////

func newUpdator(mgr *LifecycleMgr) *updator {

	updator := &updator{
		manager: mgr,
	}

	return updator
}

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
