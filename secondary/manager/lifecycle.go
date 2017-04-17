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
	"github.com/couchbase/cbauth/metakv"
	c "github.com/couchbase/gometa/common"
	"github.com/couchbase/gometa/message"
	"github.com/couchbase/gometa/protocol"
	"github.com/couchbase/indexing/secondary/common"
	fdb "github.com/couchbase/indexing/secondary/fdb"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/manager/client"
	"strings"
	"time"
	//"runtime/debug"
)

//////////////////////////////////////////////////////////////
// concrete type/struct
//////////////////////////////////////////////////////////////

type LifecycleMgr struct {
	repo          *MetadataRepo
	cinfo         *common.ClusterInfoCache
	notifier      MetadataNotifier
	clusterURL    string
	incomings     chan *requestHolder
	bootstraps    chan *requestHolder
	outgoings     chan c.Packet
	killch        chan bool
	indexerReady  bool
	builder       *builder
	janitor       *janitor
	updator       *updator
	requestServer RequestServer
}

type requestHolder struct {
	request protocol.RequestMsg
	fid     string
}

type topologyChange struct {
	Bucket    string   `json:"bucket,omitempty"`
	DefnId    uint64   `json:"defnId,omitempty"`
	State     uint32   `json:"state,omitempty"`
	StreamId  uint32   `json:"steamId,omitempty"`
	Error     string   `json:"error,omitempty"`
	BuildTime []uint64 `json:"buildTime,omitempty"`
	RState    uint32   `json:"rState,omitempty"`
}

type builder struct {
	manager  *LifecycleMgr
	pendings map[string][]uint64
	notifych chan *common.IndexDefn
}

type janitor struct {
	manager *LifecycleMgr
}

type updator struct {
	manager        *LifecycleMgr
	indexerVersion uint64
	serverGroup    string
	nodeAddr       string
}

//////////////////////////////////////////////////////////////
// Lifecycle Mgr - event processing
//////////////////////////////////////////////////////////////

func NewLifecycleMgr(notifier MetadataNotifier, clusterURL string) (*LifecycleMgr, error) {

	cinfo, err := common.FetchNewClusterInfoCache(clusterURL, common.DEFAULT_POOL)
	if err != nil {
		return nil, err
	}

	mgr := &LifecycleMgr{repo: nil,
		cinfo:        cinfo,
		notifier:     notifier,
		clusterURL:   clusterURL,
		incomings:    make(chan *requestHolder, 1000),
		outgoings:    make(chan c.Packet, 1000),
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
				op == client.OPCODE_CLEANUP_INDEX {
				m.bootstraps <- req
				return
			}
		}

		// for create/drop/build index, always go to the client queue -- which will wait for
		// indexer to be ready.
		m.incomings <- req
	}
}

func (m *LifecycleMgr) GetResponseChannel() <-chan c.Packet {
	return (<-chan c.Packet)(m.outgoings)
}

func (m *LifecycleMgr) processRequest() {

	logging.Debugf("LifecycleMgr.processRequest(): LifecycleMgr is ready to proces request")
	factory := message.NewConcreteMsgFactory()

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
			logging.Debugf("LifecycleMgr.processRequest(): receive kill signal. Stop boostrap request processing.")
			return
		}
	}

	logging.Debugf("LifecycleMgr.processRequest(): indexer is ready to process new client request.")

	// Indexer is ready and all bootstrap requests are processed.  Proceed to handle regular messages.
	for {
		select {
		case request, ok := <-m.incomings:
			if ok {
				// TOOD: deal with error
				m.dispatchRequest(request, factory)
			} else {
				// server shutdown.
				logging.Debugf("LifecycleMgr.handleRequest(): channel for receiving client request is closed. Terminate.")
				return
			}
		case <-m.killch:
			// server shutdown
			logging.Debugf("LifecycleMgr.processRequest(): receive kill signal. Stop Client request processing.")
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

	switch op {
	case client.OPCODE_CREATE_INDEX:
		err = m.handleCreateIndexScheduledBuild(key, content)
	case client.OPCODE_UPDATE_INDEX_INST:
		err = m.handleTopologyChange(content)
	case client.OPCODE_DROP_INDEX:
		err = m.handleDeleteIndex(key)
	case client.OPCODE_BUILD_INDEX:
		err = m.handleBuildIndexes(content)
	case client.OPCODE_SERVICE_MAP:
		result, err = m.handleServiceMap(content)
	case client.OPCODE_DELETE_BUCKET:
		err = m.handleDeleteBucket(key, content)
	case client.OPCODE_CLEANUP_INDEX:
		err = m.handleCleanupIndex(key)
	case client.OPCODE_CLEANUP_DEFER_INDEX:
		err = m.handleCleanupDeferIndexFromBucket(key)
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

func (m *LifecycleMgr) handleCreateIndex(key string, content []byte) error {

	defn, err := common.UnmarshallIndexDefn(content)
	if err != nil {
		logging.Errorf("LifecycleMgr.handleCreateIndex() : createIndex fails. Unable to unmarshall index definition. Reason = %v", err)
		return err
	}

	return m.CreateIndex(defn, false)
}

func (m *LifecycleMgr) CreateIndex(defn *common.IndexDefn, scheduled bool) error {

	if !defn.Deferred && !m.canBuildIndex(defn.Bucket) {
		logging.Errorf("LifecycleMgr.handleCreateIndex() : Cannot create index %s.%s while another index is being built",
			defn.Bucket, defn.Name)
		return errors.New(fmt.Sprintf("Cannot create Index %s.%s while another index is being built.",
			defn.Bucket, defn.Name))
	}

	existDefn, err := m.repo.GetIndexDefnByName(defn.Bucket, defn.Name)
	if err != nil {
		logging.Errorf("LifecycleMgr.handleCreateIndex() : createIndex fails. Reason = %v", err)
		return err
	}

	if existDefn != nil {
		topology, err := m.repo.GetTopologyByBucket(existDefn.Bucket)
		if err != nil {
			logging.Errorf("LifecycleMgr.handleCreateIndex() : fails to find index instance. Reason = %v", err)
			return err
		}

		// Allow index creation even if there is an existing index defn with the same name and bucket, as long
		// as the index state is NIL (index instance non-existent) or DELETED state.
		// If an index is in CREATED state, it will be repaired during indexer bootstrap:
		// 1) For non-deferred index, it removed by the indexer.
		// 2) For deferred index, it will be moved to READY state.
		if topology != nil {
			state, _ := topology.GetStatusByDefn(existDefn.DefnId)
			if state != common.INDEX_STATE_NIL && state != common.INDEX_STATE_DELETED {
				return errors.New(fmt.Sprintf("Index %s.%s already exists", defn.Bucket, defn.Name))
			}
		}
	}

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
	defn.BucketUUID = bucketUUID

	//if no index_type has been specified
	if strings.ToLower(string(defn.Using)) == "gsi" {
		if common.GetStorageMode() != common.NOT_SET {
			//if there is a storage mode, default to that
			defn.Using = common.IndexType(common.GetStorageMode().String())
		} else {
			//default to forestdb
			defn.Using = common.ForestDB
		}
	} else {
		if common.IsValidIndexType(string(defn.Using)) {
			defn.Using = common.IndexType(strings.ToLower(string(defn.Using)))
		} else {
			err := fmt.Sprintf("Create Index fails. Reason = Unsupported Using Clause %v", string(defn.Using))
			logging.Errorf("LifecycleMgr.handleCreateIndex: " + err)
			return errors.New(err)
		}
	}

	//
	// Figure out the index instance id
	var instId common.IndexInstId
	if defn.InstId > 0 {
		//use already supplied instance id (e.g. rebalance case)
		instId = defn.InstId
	} else {
		// create index id
		instId, err = common.NewIndexInstId()
		if err != nil {
			return err
		}
	}
	defn.InstId = 0

	// create replica id
	replicaId := defn.ReplicaId
	defn.ReplicaId = -1

	// Create index definiton.   It will fail if there is another index defintion of the same
	// index defnition id.
	if err := m.repo.CreateIndex(defn); err != nil {
		logging.Errorf("LifecycleMgr.handleCreateIndex() : createIndex fails. Reason = %v", err)
		return err
	}

	// Create index instance
	// If there is any dangling index instance of the same index name and bucket, this will first
	// remove the dangling index instance first.
	// If indexer crash during creation of index instance, there could be a dangling index definition.
	// It is possible to create index of the same name later, as long as the new index has a different
	// definition id, since an index is consider valid only if it has both index definiton and index instance.
	// So the dangling index definition is considered invalid.
	if err := m.repo.addIndexToTopology(defn, instId, replicaId, !defn.Deferred && scheduled); err != nil {
		logging.Errorf("LifecycleMgr.handleCreateIndex() : createIndex fails. Reason = %v", err)
		m.repo.DropIndexById(defn.DefnId)
		return err
	}

	// At this point, an index is created successfully with state=CREATED.  If indexer crashes now,
	// indexer will repair the index upon bootstrap (either cleanup or move to READY state).
	// If indexer returns an error when creating index, then cleanup metadata now.    During metadata cleanup,
	// errors are ignored.  This means the following:
	// 1) Index Definition is not deleted due to error from metadata repository.   The index will be repaired
	//    during indexer bootstrap or implicit dropIndex.
	// 2) Index definition is deleted.  This effectively "delete index".
	if m.notifier != nil {
		if err := m.notifier.OnIndexCreate(defn, instId, replicaId); err != nil {
			logging.Errorf("LifecycleMgr.handleCreateIndex() : createIndex fails. Reason = %v", err)

			m.DeleteIndex(defn.DefnId, false)
			return err
		}
	}

	// If cannot move the index to READY state, then abort create index by cleaning up the metadata.
	// Metadata cleanup is not atomic.  The index is effectively "deleted" if it is able to drop
	// the index definition from repository. If drop index is not successful during cleanup,
	// the index will be repaired upon bootstrap or cleanup by janitor.
	if err := m.updateIndexState(defn.Bucket, defn.DefnId, common.INDEX_STATE_READY); err != nil {
		logging.Errorf("LifecycleMgr.handleCreateIndex() : createIndex fails. Reason = %v", err)

		m.DeleteIndex(defn.DefnId, true)
		return err
	}

	// Run index build
	if !defn.Deferred {
		if m.notifier != nil {
			logging.Debugf("LifecycleMgr.handleCreateIndex() : start Index Build")

			retryList, skipList, errList := m.BuildIndexes([]common.IndexDefnId{defn.DefnId})

			if len(retryList) != 0 {
				return errors.New("Fail to build index.  Index build will retry in background.")
			}

			if len(errList) != 0 {
				logging.Errorf("LifecycleMgr.hanaleCreateIndex() : build index fails.  Reason = %v", errList[0])
				m.DeleteIndex(defn.DefnId, true)
				return errList[0]
			}

			if len(skipList) != 0 {
				logging.Errorf("LifecycleMgr.hanaleCreateIndex() : build index fails due to internal errors.")
				m.DeleteIndex(defn.DefnId, true)
				return errors.New("Fail to create index due to internal build error.  Please retry the operation.")
			}
		}
	}

	logging.Debugf("LifecycleMgr.handleCreateIndex() : createIndex completes")

	return nil
}

func (m *LifecycleMgr) handleBuildIndexes(content []byte) error {

	list, err := client.UnmarshallIndexIdList(content)
	if err != nil {
		logging.Errorf("LifecycleMgr.handleBuildIndexes() : buildIndex fails. Unable to unmarshall index list. Reason = %v", err)
		return err
	}

	input := make([]common.IndexDefnId, len(list.DefnIds))
	for i, id := range list.DefnIds {
		input[i] = common.IndexDefnId(id)
	}

	retryList, skipList, errList := m.BuildIndexes(input)

	if len(retryList) != 0 || len(skipList) != 0 || len(errList) != 0 {
		msg := "Build index fails."

		if len(retryList) == 1 {

			inst, err := m.FindLocalIndexInst(retryList[0].Bucket, retryList[0].DefnId)
			if inst != nil && err == nil {
				msg += fmt.Sprintf("  Index %v will retry building in the background for reason: %v.", retryList[0].Name, inst.Error)
			}
		}

		if len(errList) == 1 {
			msg += fmt.Sprintf("  %v.", errList[0])
		}

		if len(retryList) > 1 {
			msg += "  Some index will be retried building in the background."
		}

		if len(errList) > 1 {
			msg += "  Some index cannot be built due to errors."
		}

		if len(skipList) != 0 {
			msg += "  Some index cannot be built since it may not exist.  Please check if the list of indexes are valid."
		}

		if len(errList) > 1 || len(retryList) > 1 {
			msg += "  For more details, please check index status."
		}

		return errors.New(msg)
	}

	return nil
}

func (m *LifecycleMgr) BuildIndexes(ids []common.IndexDefnId) ([]*common.IndexDefn, []common.IndexDefnId, []error) {

	retryList := ([]*common.IndexDefn)(nil)
	errList := ([]error)(nil)
	skipList := ([]common.IndexDefnId)(nil)

	instIdList := []common.IndexInstId(nil)
	buckets := []string(nil)
	inst2DefnMap := make(map[common.IndexInstId]common.IndexDefnId)

	for _, id := range ids {
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

		inst, err := m.FindLocalIndexInst(defn.Bucket, id)
		if inst == nil || err != nil {
			logging.Errorf("LifecycleMgr.handleBuildIndexes: Fail to find index instance (%v, %v).  Skip this index.", defn.Name, defn.Bucket)
			skipList = append(skipList, id)
			continue
		}

		if inst.State != uint32(common.INDEX_STATE_READY) {
			logging.Errorf("LifecycleMgr.handleBuildIndexes: index instance (%v, %v) is not in ready state.  Skip this index.", defn.Name, defn.Bucket)
			continue
		}

		//Schedule the build
		if err := m.SetScheduledFlag(defn.Bucket, id, true); err != nil {
			msg := fmt.Sprintf("LifecycleMgr.handleBuildIndexes: Unable to set scheduled flag in index instance (%v, %v).", defn.Name, defn.Bucket)
			logging.Warnf("%v  Will try to build index now, but it will not be able to retry index build upon server restart.", msg)
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

		instIdList = append(instIdList, common.IndexInstId(inst.InstId))
		inst2DefnMap[common.IndexInstId(inst.InstId)] = defn.DefnId
	}

	if m.notifier != nil && len(instIdList) != 0 {

		if errMap := m.notifier.OnIndexBuild(instIdList, buckets); len(errMap) != 0 {
			logging.Errorf("LifecycleMgr.hanaleBuildIndexes() : buildIndex fails. Reason = %v", errMap)

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

				inst, err := m.FindLocalIndexInst(defn.Bucket, defnId)
				if inst != nil && err == nil {
					m.UpdateIndexInstance(defn.Bucket, defnId, common.INDEX_STATE_NIL, common.NIL_STREAM, build_err.Error(), nil, inst.RState)

				} else {
					logging.Infof("LifecycleMgr.handleBuildIndexes() : Fail to persist error in index instance (%v, %v).",
						defn.Bucket, defn.Name)
				}

				if m.canRetryError(inst, build_err) {
					logging.Infof("LifecycleMgr.handleBuildIndexes() : Encounter build error.  Retry building index (%v, %v) at later time.",
						defn.Bucket, defn.Name)

					if inst != nil && !inst.Scheduled {
						if err := m.SetScheduledFlag(defn.Bucket, defnId, true); err != nil {
							msg := fmt.Sprintf("LifecycleMgr.handleBuildIndexes: Unable to set scheduled flag in index instance (%v, %v).",
								defn.Name, defn.Bucket)
							logging.Warnf("%v  Will try to build index now, but it will not be able to retry index build upon server restart.", msg)
						}
					}

					retryList = append(retryList, defn)
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

	return retryList, skipList, errList
}

func (m *LifecycleMgr) handleDeleteIndex(key string) error {

	id, err := indexDefnId(key)
	if err != nil {
		logging.Errorf("LifecycleMgr.handleDeleteIndex() : deleteIndex fails. Reason = %v", err)
		return err
	}

	return m.DeleteIndex(id, true)
}

func (m *LifecycleMgr) handleCleanupIndex(key string) error {

	id, err := indexDefnId(key)
	if err != nil {
		logging.Errorf("LifecycleMgr.handleCleanupIndex() : deleteIndex fails. Reason = %v", err)
		return err
	}

	return m.DeleteIndex(id, false)
}

func (m *LifecycleMgr) DeleteIndex(id common.IndexDefnId, notify bool) error {

	defn, err := m.repo.GetIndexDefnById(id)
	if err != nil {
		logging.Errorf("LifecycleMgr.DeleteIndex() : drop index fails for index defn %v.  Error = %v.", id, err)
		return err
	}
	if defn == nil {
		logging.Infof("LifecycleMgr.DeleteIndex() : index %v does not exist.", id)
		return nil
	}

	// updateIndexState will not return an error if there is no index inst
	if err := m.updateIndexState(defn.Bucket, defn.DefnId, common.INDEX_STATE_DELETED); err != nil {
		logging.Errorf("LifecycleMgr.handleDeleteIndex() : deleteIndex fails. Reason = %v", err)
		return err
	}

	if notify && m.notifier != nil {
		inst, err := m.FindLocalIndexInst(defn.Bucket, id)
		if err != nil {
			// Cannot read bucket topology.  Likely a transient error.  But without index inst, we cannot
			// proceed without letting indexer to clean up.
			logging.Warnf("LifecycleMgr.handleDeleteIndex() : Encounter error during delete index. Error = %v", err)
			return err
		}

		// If we cannot find the local index inst, then skip notifying the indexer, but proceed to delete the
		// index definition.  Lifecycle manager create index definition before index instance.  It also delete
		// index defintion before deleting index instance.  So if an index definition exists, but there is no
		// index instance, then it means the index has never succesfully created.
		//
		if inst != nil {
			// Can call index delete again even after indexer has cleaned up -- if indexer crashes after
			// this point but before it can delete the index definition.
			if err := m.notifier.OnIndexDelete(common.IndexInstId(inst.InstId), defn.Bucket); err != nil {
				// Do not remove index defnition if indexer is unable to delete the index.   This is to ensure the
				// the client can call DeleteIndex again and free up indexer resource.
				indexerErr, ok := err.(*common.IndexerError)
				if ok && indexerErr.Code != common.IndexNotExist {
					return err
				} else if !strings.Contains(err.Error(), "Unknown Index Instance") {
					return err
				}
			}
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
	inst, err := m.FindLocalIndexInst(change.Bucket, common.IndexDefnId(change.DefnId))
	if err != nil {
		return err
	}
	if inst == nil {
		return nil
	}

	state := inst.State
	scheduled := inst.Scheduled

	// update the index instance
	if err := m.UpdateIndexInstance(change.Bucket, common.IndexDefnId(change.DefnId), common.IndexState(change.State),
		common.StreamId(change.StreamId), change.Error, change.BuildTime, change.RState); err != nil {
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

func (m *LifecycleMgr) handleDeleteBucket(bucket string, content []byte) error {

	result := error(nil)

	if len(content) == 0 {
		return errors.New("invalid argument")
	}

	streamId := common.StreamId(content[0])

	topology, err := m.repo.GetTopologyByBucket(bucket)
	if err == nil && topology != nil {
		/*
			// if there is an error getting the UUID, this means that
			// the node is not able to connect to pool service in order
			// to fetch the bucket UUID.   Return an error and skip.
			uuid, err := m.getBucketUUID(bucket)
			if err != nil {
				logging.Errorf("LifecycleMgr.handleDeleteBucket() : Encounter when connecting to pool service = %v", err)
				return err
			}
		*/

		// At this point, we are able to connect to pool service.  If pool
		// does not contain the bucket, then we delete all index defn in
		// the bucket.  Otherwise, delete index defn that does not have the
		// same bucket UUID.  Note that any other create index request will
		// be blocked while this call is run.
		definitions := make([]IndexDefnDistribution, len(topology.Definitions))
		copy(definitions, topology.Definitions)

		for _, defnRef := range definitions {

			if defn, err := m.repo.GetIndexDefnById(common.IndexDefnId(defnRef.DefnId)); err == nil && defn != nil {

				logging.Debugf("LifecycleMgr.handleDeleteBucket() : index instance: id %v, streamId %v.",
					defn.DefnId, defnRef.Instances[0].StreamId)

				// delete index defn from the bucket if bucket uuid is not specified or
				// index does *not* belong to bucket uuid
				if /* (uuid == common.BUCKET_UUID_NIL || defn.BucketUUID != uuid) && */
				streamId == common.NIL_STREAM || (common.StreamId(defnRef.Instances[0].StreamId) == streamId ||
					common.StreamId(defnRef.Instances[0].StreamId) == common.NIL_STREAM) {
					if err := m.DeleteIndex(common.IndexDefnId(defn.DefnId), false); err != nil {
						result = err
					}
				}
			} else {
				logging.Debugf("LifecycleMgr.handleDeleteBucket() : Cannot find index instance %v.  Skip.", defnRef.DefnId)
			}
		}
	} else if err != fdb.FDB_RESULT_KEY_NOT_FOUND {
		result = err
	}

	return result
}

//
// Cleanup any defer index from invalid bucket.
//
func (m *LifecycleMgr) handleCleanupDeferIndexFromBucket(bucket string) error {

	// Get bucket UUID.  bucket uuid could be BUCKET_UUID_NIL for non-existent bucket.
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
			if defnRef.Instances[0].State != uint32(common.INDEX_STATE_DELETED) &&
				common.StreamId(defnRef.Instances[0].StreamId) != common.NIL_STREAM {
				hasValidActiveIndex = true
				break
			}
		}

		if !hasValidActiveIndex {
			for _, defnRef := range topology.Definitions {
				if defn, err := m.repo.GetIndexDefnById(common.IndexDefnId(defnRef.DefnId)); err == nil && defn != nil {
					if defn.BucketUUID != currentUUID && defn.Deferred &&
						defnRef.Instances[0].State != uint32(common.INDEX_STATE_DELETED) &&
						common.StreamId(defnRef.Instances[0].StreamId) == common.NIL_STREAM {
						if err := m.DeleteIndex(common.IndexDefnId(defn.DefnId), true); err != nil {
							return err
						}
					}
				}
			}
		}
	}

	return nil
}

func (m *LifecycleMgr) handleCreateIndexScheduledBuild(key string, content []byte) error {

	defn, err := common.UnmarshallIndexDefn(content)
	if err != nil {
		logging.Errorf("LifecycleMgr.handleCreateIndexScheduledBuild() : createIndex fails. Unable to unmarshall index definition. Reason = %v", err)
		return err
	}

	// Create index with the scheduled flag.
	return m.CreateIndex(defn, true)
}

//////////////////////////////////////////////////////////////
// Lifecycle Mgr - support functions
//////////////////////////////////////////////////////////////

func (m *LifecycleMgr) canRetryError(inst *IndexInstDistribution, err error) bool {

	if inst == nil || inst.RState != uint32(common.REBAL_ACTIVE) {
		return false
	}

	indexerErr, ok := err.(*common.IndexerError)
	if !ok {
		return true
	}

	if indexerErr.Code == common.IndexNotExist ||
		indexerErr.Code == common.InvalidBucket ||
		indexerErr.Code == common.RebalanceInProgress ||
		indexerErr.Code == common.IndexAlreadyExist ||
		indexerErr.Code == common.IndexInvalidState {
		return false
	}

	return true
}

func (m *LifecycleMgr) UpdateIndexInstance(bucket string, defnId common.IndexDefnId, state common.IndexState,
	streamId common.StreamId, errStr string, buildTime []uint64, rState uint32) error {

	topology, err := m.repo.GetTopologyByBucket(bucket)
	if err != nil {
		logging.Errorf("LifecycleMgr.handleTopologyChange() : index instance update fails. Reason = %v", err)
		return err
	}
	if topology == nil {
		logging.Warnf("LifecycleMgr.handleTopologyChange() : toplogy does not exist.  Skip index instance update for %v", defnId)
		return nil
	}

	changed := topology.UpdateRebalanceStateForIndexInstByDefn(common.IndexDefnId(defnId), common.RebalanceState(rState))

	if state != common.INDEX_STATE_NIL {
		changed = topology.UpdateStateForIndexInstByDefn(common.IndexDefnId(defnId), common.IndexState(state)) || changed

		if state == common.INDEX_STATE_INITIAL ||
			state == common.INDEX_STATE_CATCHUP ||
			state == common.INDEX_STATE_ACTIVE ||
			state == common.INDEX_STATE_DELETED {

			changed = topology.UpdateScheduledFlagForIndexInstByDefn(common.IndexDefnId(defnId), false) || changed
		}
	}

	if streamId != common.NIL_STREAM {
		changed = topology.UpdateStreamForIndexInstByDefn(common.IndexDefnId(defnId), common.StreamId(streamId)) || changed
	}

	changed = topology.SetErrorForIndexInstByDefn(common.IndexDefnId(defnId), errStr) || changed

	if changed {
		if err := m.repo.SetTopologyByBucket(bucket, topology); err != nil {
			// Topology update is in place.  If there is any error, SetTopologyByBucket will purge the cache copy.
			logging.Errorf("LifecycleMgr.handleTopologyChange() : index instance update fails. Reason = %v", err)
			return err
		}
	}

	return nil
}

func (m *LifecycleMgr) SetScheduledFlag(bucket string, defnId common.IndexDefnId, scheduled bool) error {

	topology, err := m.repo.GetTopologyByBucket(bucket)
	if err != nil {
		logging.Errorf("LifecycleMgr.SetScheduledFlag() : index instance update fails. Reason = %v", err)
		return err
	}
	if topology == nil {
		logging.Warnf("LifecycleMgr.SetScheduledFlag() : toplogy does not exist.  Skip index instance update for %v", defnId)
		return nil
	}

	changed := topology.UpdateScheduledFlagForIndexInstByDefn(common.IndexDefnId(defnId), scheduled)

	if changed {
		if err := m.repo.SetTopologyByBucket(bucket, topology); err != nil {
			// Topology update is in place.  If there is any error, SetTopologyByBucket will purge the cache copy.
			logging.Errorf("LifecycleMgr.SetScheduledFlag() : index instance update fails. Reason = %v", err)
			return err
		}
	}

	return nil
}

func (m *LifecycleMgr) FindLocalIndexInst(bucket string, defnId common.IndexDefnId) (*IndexInstDistribution, error) {

	topology, err := m.repo.GetTopologyByBucket(bucket)
	if err != nil {
		logging.Errorf("LifecycleMgr.FindLocalIndexInst() : Cannot read topology metadata. Reason = %v", err)
		return nil, err
	}
	if topology == nil {
		logging.Infof("LifecycleMgr.FindLocalIndexInst() : Index Inst does not exist %v", defnId)
		return nil, nil
	}

	return topology.GetIndexInstByDefn(defnId), nil
}

func (m *LifecycleMgr) updateIndexState(bucket string, defnId common.IndexDefnId, state common.IndexState) error {

	topology, err := m.repo.GetTopologyByBucket(bucket)
	if err != nil {
		logging.Errorf("LifecycleMgr.updateIndexState() : fails to find index instance. Reason = %v", err)
		return err
	}
	if topology == nil {
		logging.Warnf("LifecycleMgr.updateIndexState() : fails to find index instance. Skip update for %v to %v.", defnId, state)
		return nil
	}

	changed := topology.UpdateStateForIndexInstByDefn(defnId, state)
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

	return client.MarshallServiceMap(srvMap)
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

	return srvMap, nil
}

// This function returns an error if it cannot connect for fetching bucket info.
// It returns BUCKET_UUID_NIL (err == nil) if bucket does not exist.
//
func (m *LifecycleMgr) getBucketUUID(bucket string) (string, error) {
	count := 0
RETRY:
	uuid, err := common.GetBucketUUID(m.clusterURL, bucket)
	if err != nil && count < 5 {
		count++
		time.Sleep(time.Duration(100) * time.Millisecond)
		goto RETRY
	}

	if err != nil {
		return common.BUCKET_UUID_NIL, err
	}

	return uuid, nil
}

// This function ensures:
// 1) Bucket exists
// 2) Existing Index Definition matches the UUID of exixisting bucket
// 3) If bucket does not exist AND there is no existing definition, this returns common.BUCKET_UUID_NIL
//
func (m *LifecycleMgr) verifyBucket(bucket string) (string, error) {

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
			if state, _ := topology.GetStatusByDefn(common.IndexDefnId(defnRef.DefnId)); state != common.INDEX_STATE_DELETED {
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
// Lifecycle Mgr - recovery
//////////////////////////////////////////////////////////////

//
// 1) This is important that this function does not mutate the repository directly.
// 2) Any call to mutate the repository must be async request.
//
func (m *janitor) cleanup() {

	//
	// Cleanup based on delete token
	//
	logging.Infof("janitor: running cleanup.")

	entries, err := metakv.ListAllChildren(client.DeleteDDLCommandTokenPath)
	if err != nil {
		logging.Warnf("janitor: Fail to drop index upon cleanup.  Internal Error = %v", err)
		return
	}

	for _, entry := range entries {

		if strings.Contains(entry.Path, client.DeleteDDLCommandTokenPath) && entry.Value != nil {

			logging.Infof("janitor: Processing delete token %v", entry.Path)

			command, err := client.UnmarshallDeleteCommandToken(entry.Value)
			if err != nil {
				logging.Warnf("janitor: Fail to drop index upon cleanup.  Skp command %v.  Internal Error = %v.", entry.Path, err)
				continue
			}

			defn, err := m.manager.repo.GetIndexDefnById(command.DefnId)
			if err != nil {
				logging.Warnf("janitor: Fail to drop index upon cleanup.  Skp command %v.  Internal Error = %v.", entry.Path, err)
				continue
			}

			// index may already be deleted or does not exist in this node
			if defn == nil {
				continue
			}

			// Queue up the cleanup request.  The request wont' happen until bootstrap is ready.
			if err := m.manager.requestServer.MakeAsyncRequest(client.OPCODE_DROP_INDEX, fmt.Sprintf("%v", command.DefnId), nil); err != nil {
				logging.Warnf("janitor: Fail to drop index upon cleanup.  Skp command %v.  Internal Error = %v.", entry.Path, err)
			} else {
				logging.Infof("janitor: Clean up deleted index %v during periodic cleanup ", command.DefnId)
			}
		}
	}

	//
	// Cleanup based on index status (DELETED index)
	//

	metaIter, err := m.manager.repo.NewIterator()
	if err != nil {
		logging.Warnf("janitor: Fail to  upon instantiate metadata iterator during cleanup.  Internal Error = %v", err)
		return
	}
	defer metaIter.Close()

	for _, defn, err := metaIter.Next(); err == nil; _, defn, err = metaIter.Next() {

		inst, err := m.manager.FindLocalIndexInst(defn.Bucket, defn.DefnId)
		if err != nil {
			logging.Warnf("janitor: Fail to find index instance (%v, %v) during cleanup. Internal error = %v.  Skipping.", defn.Bucket, defn.Name, err)
			continue
		}

		// Queue up the cleanup request.  The request wont' happen until bootstrap is ready.
		if inst != nil && inst.State == uint32(common.INDEX_STATE_DELETED) {
			if err := m.manager.requestServer.MakeAsyncRequest(client.OPCODE_DROP_INDEX, fmt.Sprintf("%v", defn.DefnId), nil); err != nil {
				logging.Warnf("janitor: Fail to drop index upon cleanup.  Skip index (%v, %v).  Internal Error = %v.", defn.Bucket, defn.Name, err)
			} else {
				logging.Infof("janitor: Clean up deleted index (%v, %v) during periodic cleanup ", defn.Bucket, defn.DefnId)
			}
		}
	}
}

func (m *janitor) run() {

	// do initial cleanup
	m.cleanup()

	ticker := time.NewTicker(time.Second * 60)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.cleanup()

		case <-m.manager.killch:
			logging.Infof("janitor: Index recovery go-routine terminates.")
		}
	}
}

func newJanitor(mgr *LifecycleMgr) *janitor {

	janitor := &janitor{
		manager: mgr,
	}

	return janitor
}

//////////////////////////////////////////////////////////////
// Lifecycle Mgr - builder
//////////////////////////////////////////////////////////////

func (s *builder) run() {

	// wait for indexer bootstrap to complete before recover
	s.recover()

	// check if there is any pending index build every second
	ticker := time.NewTicker(time.Millisecond * 200)
	defer ticker.Stop()

	for {
		select {
		case defn := <-s.notifych:
			logging.Infof("builder:  Received new index build request %v.  Schedule to build index for bucket %v", defn.DefnId, defn.Bucket)
			s.pendings[defn.Bucket] = append(s.pendings[defn.Bucket], uint64(defn.DefnId))

		case <-ticker.C:
			for bucket, _ := range s.pendings {
				s.tryBuildIndex(bucket)
			}

		case <-s.manager.killch:
			logging.Infof("builder: Index builder terminates.")
		}
	}
}

func (s *builder) tryBuildIndex(bucket string) {

	defnIds := s.pendings[bucket]
	if len(defnIds) != 0 {
		// This is a pre-cautionary check if there is any index being
		// built for the bucket.   The authortative check is done by indexer.
		if s.manager.canBuildIndex(bucket) {

			buildList := ([]uint64)(nil)
			buildMap := make(map[uint64]bool)
			for _, defnId := range defnIds {

				defn, err := s.manager.repo.GetIndexDefnById(common.IndexDefnId(defnId))
				if defn == nil || err != nil {
					logging.Infof("builder: Fail to find index definition (%v, %v).  Skipping.", defnId, bucket)
					continue
				}

				inst, err := s.manager.FindLocalIndexInst(bucket, common.IndexDefnId(defnId))
				if inst == nil || err != nil {
					logging.Infof("builder: Fail to find index instance (%v, %v).  Skipping.", defnId, bucket)
					continue
				}

				if inst.State == uint32(common.INDEX_STATE_READY) && inst.Scheduled {
					if _, ok := buildMap[defnId]; !ok {
						buildList = append(buildList, defnId)
						buildMap[defnId] = true
					}
				} else {
					logging.Infof("builder: Index instance (%v, %v) is not in READY state.  Skipping.", defnId, bucket)
				}
			}

			if len(buildList) != 0 {

				idList := &client.IndexIdList{DefnIds: buildList}
				key := fmt.Sprintf("%d", idList.DefnIds[0])
				content, err := client.MarshallIndexIdList(idList)
				if err != nil {
					logging.Infof("builder: Fail to marshall index defnIds during index build.  Error = %v. Retry later.", err)
					return
				}

				logging.Infof("builder: Try build index for bucket %v. Index %v", bucket, idList)

				// If any of the index cannot be built, those index will be skipped by lifecycle manager, so it
				// will send the rest of the indexes to the indexer.  An index cannot be built if it does not have
				// an index instance or the index instance is not in READY state.
				if err := s.manager.requestServer.MakeRequest(client.OPCODE_BUILD_INDEX, key, content); err != nil {
					logging.Errorf("builder: Fail to build index.  Error = %v.  Retry later.", err)
				}
			}

			// Clean up the map.  If there is any index that needs retry, they will be put into the notifych again.
			// Once this function is done, the map will be populated again from the notifych.
			s.pendings[bucket] = nil
		}
	}
}

func (s *builder) recover() {

	logging.Infof("builder: recovering scheduled index")

	metaIter, err := s.manager.repo.NewIterator()
	if err != nil {
		logging.Errorf("builder:  Unable to read from metadata repository.   Will not recover scheduled build index from repository.")
		return
	}
	defer metaIter.Close()

	for _, defn, err := metaIter.Next(); err == nil; _, defn, err = metaIter.Next() {

		inst, err := s.manager.FindLocalIndexInst(defn.Bucket, defn.DefnId)
		if inst == nil || err != nil {
			logging.Errorf("builder: Unable to read index instance for definition (%v, %v).   Skipping ...",
				defn.Bucket, defn.Name)
			continue
		}

		if inst.Scheduled && inst.State == uint32(common.INDEX_STATE_READY) {
			logging.Infof("builder: Schedule index build for (%v, %v).", defn.Bucket, defn.Name)
			s.notifych <- defn
		}
	}
}

func newBuilder(mgr *LifecycleMgr) *builder {

	builder := &builder{
		manager:  mgr,
		pendings: make(map[string][]uint64),
		notifych: make(chan *common.IndexDefn, 10000),
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

	ticker := time.NewTicker(time.Second * 60)
	defer ticker.Stop()

	m.check()

	for {
		select {
		case <-ticker.C:
			m.check()

		case <-m.manager.killch:
			logging.Infof("updator: Index recovery go-routine terminates.")
		}
	}
}

func (m *updator) check() {

	serviceMap, err := m.manager.getServiceMap()
	if err != nil {
		logging.Errorf("updator: fail to get indexer serivce map.  Error = %v", err)
		return
	}

	if serviceMap.ServerGroup != m.serverGroup || m.indexerVersion != serviceMap.IndexerVersion || serviceMap.NodeAddr != m.nodeAddr {

		m.serverGroup = serviceMap.ServerGroup
		m.indexerVersion = serviceMap.IndexerVersion
		m.nodeAddr = serviceMap.NodeAddr

		logging.Infof("updator: updating service map.  server group=%v, indexerVersion=%v nodeAddr %v", m.serverGroup, m.indexerVersion, m.nodeAddr)

		if err := m.manager.repo.SetServiceMap(serviceMap); err != nil {
			logging.Errorf("updator: fail to set service map.  Error = %v", err)
			return
		}
	}
}
