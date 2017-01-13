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

type LifecycleMgr struct {
	repo         *MetadataRepo
	addrProvider common.ServiceAddressProvider
	notifier     MetadataNotifier
	clusterURL   string
	incomings    chan *requestHolder
	bootstraps   chan *requestHolder
	outgoings    chan c.Packet
	killch       chan bool
	indexerReady bool
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

func NewLifecycleMgr(addrProvider common.ServiceAddressProvider, notifier MetadataNotifier, clusterURL string) *LifecycleMgr {

	mgr := &LifecycleMgr{repo: nil,
		addrProvider: addrProvider,
		notifier:     notifier,
		clusterURL:   clusterURL,
		incomings:    make(chan *requestHolder, 1000),
		outgoings:    make(chan c.Packet, 1000),
		killch:       make(chan bool),
		bootstraps:   make(chan *requestHolder, 1000),
		indexerReady: false}

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

	req := &requestHolder{request: request, fid: fid}
	op := c.OpCode(request.GetOpCode())

	logging.Debugf("LifecycleMgr.OnNewRequest(): queuing new request. reqId %v opCode %v", request.GetReqId(), op)

	if op == client.OPCODE_INDEXER_READY {
		m.indexerReady = true
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
			if op == client.OPCODE_UPDATE_INDEX_INST || op == client.OPCODE_DELETE_BUCKET ||
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

	/*
		defer func() {
			if r := recover(); r != nil {
				logging.Debugf("panic in LifecycleMgr.processRequest() : %s\n", r)
				logging.Debugf("%s", debug.Stack())
			}
		}()
	*/

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
		err = m.handleCreateIndex(key, content)
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

	if err == nil {
		msg := factory.CreateResponse(fid, reqId, "", result)
		m.outgoings <- msg
	} else {
		msg := factory.CreateResponse(fid, reqId, err.Error(), result)
		m.outgoings <- msg
	}
}

func (m *LifecycleMgr) handleCreateIndex(key string, content []byte) error {

	defn, err := common.UnmarshallIndexDefn(content)
	if err != nil {
		logging.Errorf("LifecycleMgr.handleCreateIndex() : createIndex fails. Unable to unmarshall index definition. Reason = %v", err)
		return err
	}

	return m.CreateIndex(defn)
}

func (m *LifecycleMgr) CreateIndex(defn *common.IndexDefn) error {

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

		state, _ := topology.GetStatusByDefn(existDefn.DefnId)
		if state != common.INDEX_STATE_NIL && state != common.INDEX_STATE_DELETED {
			return errors.New(fmt.Sprintf("Index %s.%s already exists", defn.Bucket, defn.Name))
		}
	}

	// Fetch bucket UUID.   This confirms that the bucket has existed, but it cannot confirm if the bucket
	// is still existing in the cluster (due to race condition or network partitioned).
	// Lifecycle manager is a singleton that ensures all metadata operation is serialized.  Therefore, a
	// call to verifyBucket() here  will also make sure that all existing indexes belong to the same bucket UUID.
	// To esnure verifyBucket can succeed, indexes from stale bucket must be cleaned up (eventually).
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
			err := fmt.Sprintf("LifecycleMgr.handleCreateIndex() : createIndex fails."+
				"Reason = Unsupported Using Clause %v", string(defn.Using))
			logging.Errorf(err)
			return errors.New(err)
		}
	}

	// create index id
	instId, err := common.NewIndexInstId()
	if err != nil {
		return err
	}

	// create replica id
	replicaId := defn.ReplicaId
	defn.ReplicaId = -1

	if err := m.repo.CreateIndex(defn); err != nil {
		logging.Errorf("LifecycleMgr.handleCreateIndex() : createIndex fails. Reason = %v", err)
		return err
	}

	if err := m.repo.addIndexToTopology(defn, instId, replicaId); err != nil {
		logging.Errorf("LifecycleMgr.handleCreateIndex() : createIndex fails. Reason = %v", err)
		m.repo.DropIndexById(defn.DefnId)
		return err
	}

	if m.notifier != nil {
		if err := m.notifier.OnIndexCreate(defn, instId, replicaId); err != nil {
			logging.Errorf("LifecycleMgr.handleCreateIndex() : createIndex fails. Reason = %v", err)
			m.repo.DropIndexById(defn.DefnId)
			m.repo.deleteIndexFromTopology(defn.Bucket, defn.DefnId)
			return err
		}
	}

	if err := m.updateIndexState(defn.Bucket, defn.DefnId, common.INDEX_STATE_READY); err != nil {
		logging.Errorf("LifecycleMgr.handleCreateIndex() : createIndex fails. Reason = %v", err)

		if m.notifier != nil {
			m.notifier.OnIndexDelete(instId, defn.Bucket)
		}
		m.repo.DropIndexById(defn.DefnId)
		m.repo.deleteIndexFromTopology(defn.Bucket, defn.DefnId)
		return err
	}

	if !defn.Deferred {
		if m.notifier != nil {
			logging.Debugf("LifecycleMgr.handleCreateIndex() : start Index Build")
			if errMap := m.notifier.OnIndexBuild([]common.IndexInstId{instId}, []string{defn.Bucket}); len(errMap) != 0 {
				err := errMap[instId]
				logging.Errorf("LifecycleMgr.hanaleCreateIndex() : createIndex fails. Reason = %v", err)

				m.notifier.OnIndexDelete(instId, defn.Bucket)
				m.repo.DropIndexById(defn.DefnId)
				m.repo.deleteIndexFromTopology(defn.Bucket, defn.DefnId)
				return err
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

	return m.BuildIndexes(input)
}

func (m *LifecycleMgr) BuildIndexes(ids []common.IndexDefnId) error {

	instIdList := []common.IndexInstId(nil)
	buckets := []string(nil)
	for _, id := range ids {
		defn, err := m.repo.GetIndexDefnById(id)
		if err != nil {
			logging.Errorf("LifecycleMgr.handleBuildIndexes() : buildIndex fails. Reason = %v", err)
			return err
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

		instId, err := m.FindLocalIndexInstId(defn.Bucket, id)
		if err != nil {
			logging.Errorf("LifecycleMgr.handleBuildIndexes() : buildIndex fails. Reason = %v", err)
			return err
		}

		instIdList = append(instIdList, instId)
	}

	if m.notifier != nil {

		if errMap := m.notifier.OnIndexBuild(instIdList, buckets); len(errMap) != 0 {
			logging.Errorf("LifecycleMgr.hanaleBuildIndexes() : buildIndex fails. Reason = %v", errMap)
			result := error(nil)

			for instId, build_err := range errMap {
				defnId := common.IndexDefnId(0)
				for i, instId2 := range instIdList {
					if instId == instId2 {
						defnId = ids[i]
					}
				}

				if defn, err := m.repo.GetIndexDefnById(defnId); err == nil {
					inst, err := m.FindLocalIndexInst(defn.Bucket, defnId)
					if inst != nil && err == nil {
						m.UpdateIndexInstance(defn.Bucket, defnId, common.INDEX_STATE_NIL, common.NIL_STREAM, build_err.Error(), nil, inst.RState)
					}
				}

				if result == nil {
					result = build_err
				} else if result.Error() != build_err.Error() {
					result = errors.New("Build index fails. Please check index status for error.")
				}
			}

			return result
		}
	}

	logging.Debugf("LifecycleMgr.handleBuildIndexes() : buildIndex completes")

	return nil
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
		return err
	}

	if err := m.updateIndexState(defn.Bucket, defn.DefnId, common.INDEX_STATE_DELETED); err != nil {
		logging.Errorf("LifecycleMgr.handleDeleteIndex() : deleteIndex fails. Reason = %v", err)
		return err
	}

	instId, err := m.FindLocalIndexInstId(defn.Bucket, id)
	if err != nil {
		logging.Errorf("LifecycleMgr.handleDeleteIndex() : deleteIndex fails. Reason = %v", err)
		return err
	}

	// Can call index delete again on already deleted defn
	if notify && m.notifier != nil {
		m.notifier.OnIndexDelete(instId, defn.Bucket)
	}
	m.repo.DropIndexById(defn.DefnId)
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

	return m.UpdateIndexInstance(change.Bucket, common.IndexDefnId(change.DefnId), common.IndexState(change.State),
		common.StreamId(change.StreamId), change.Error, change.BuildTime, change.RState)
}

func (m *LifecycleMgr) UpdateIndexInstance(bucket string, defnId common.IndexDefnId, state common.IndexState,
	streamId common.StreamId, errStr string, buildTime []uint64, rState uint32) error {

	topology, err := m.repo.GetTopologyByBucket(bucket)
	if err != nil {
		logging.Errorf("LifecycleMgr.handleTopologyChange() : index instance update fails. Reason = %v", err)
		return err
	}

	changed := topology.UpdateRebalanceStateForIndexInstByDefn(common.IndexDefnId(defnId), common.RebalanceState(rState))

	if state != common.INDEX_STATE_NIL {
		changed = topology.UpdateStateForIndexInstByDefn(common.IndexDefnId(defnId), common.IndexState(state)) || changed
	}

	if streamId != common.NIL_STREAM {
		changed = topology.UpdateStreamForIndexInstByDefn(common.IndexDefnId(defnId), common.StreamId(streamId)) || changed
	}

	changed = topology.SetErrorForIndexInstByDefn(common.IndexDefnId(defnId), errStr) || changed

	if changed {
		if err := m.repo.SetTopologyByBucket(bucket, topology); err != nil {
			logging.Errorf("LifecycleMgr.handleTopologyChange() : index instance update fails. Reason = %v", err)
			return err
		}
	}

	return nil
}

func (m *LifecycleMgr) FindLocalIndexInstId(bucket string, defnId common.IndexDefnId) (common.IndexInstId, error) {

	inst, err := m.FindLocalIndexInst(bucket, defnId)
	if inst != nil && err == nil {
		return common.IndexInstId(inst.InstId), nil
	}

	return common.IndexInstId(0), fmt.Errorf("Fail to find index instance Id for the given index definition %v", defnId)
}

func (m *LifecycleMgr) FindLocalIndexInst(bucket string, defnId common.IndexDefnId) (*IndexInstDistribution, error) {

	topology, err := m.repo.GetTopologyByBucket(bucket)
	if err != nil {
		logging.Errorf("LifecycleMgr.FindLocalIndexInst() : Cannot read topology metadata. Reason = %v", err)
		return nil, err
	}

	inst := topology.GetIndexInstByDefn(defnId)
	if inst != nil {
		return inst, nil
	}

	return nil, fmt.Errorf("Fail to find index instance for the given index definition %v", defnId)
}

func (m *LifecycleMgr) updateIndexState(bucket string, defnId common.IndexDefnId, state common.IndexState) error {

	topology, err := m.repo.GetTopologyByBucket(bucket)
	if err != nil {
		logging.Errorf("LifecycleMgr.updateIndexState() : fails to find index instance. Reason = %v", err)
		return err
	}

	topology.UpdateStateForIndexInstByDefn(defnId, state)

	if err := m.repo.SetTopologyByBucket(bucket, topology); err != nil {
		logging.Errorf("LifecycleMgr.updateIndexState() : fail to update state of index instance.  Reason = %v", err)
		return err
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

	srvMap := new(client.ServiceMap)

	id, err := m.repo.GetLocalIndexerId()
	if err != nil {
		return nil, err
	}
	srvMap.IndexerId = string(id)

	srvMap.ScanAddr, err = m.addrProvider.GetLocalServiceAddress(common.INDEX_SCAN_SERVICE)
	if err != nil {
		return nil, err
	}

	srvMap.HttpAddr, err = m.addrProvider.GetLocalServiceAddress(common.INDEX_HTTP_SERVICE)
	if err != nil {
		return nil, err
	}

	srvMap.AdminAddr, err = m.addrProvider.GetLocalServiceAddress(common.INDEX_ADMIN_SERVICE)
	if err != nil {
		return nil, err
	}

	srvMap.NodeAddr, err = m.addrProvider.GetLocalHostAddress()
	if err != nil {
		return nil, err
	}

	return client.MarshallServiceMap(srvMap)
}

func (m *LifecycleMgr) handleDeleteBucket(bucket string, content []byte) error {

	result := error(nil)

	if len(content) == 0 {
		return errors.New("invalid argument")
	}

	streamId := common.StreamId(content[0])

	topology, err := m.repo.GetTopologyByBucket(bucket)
	if err == nil {
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

			if defn, err := m.repo.GetIndexDefnById(common.IndexDefnId(defnRef.DefnId)); err == nil {

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
	if err == nil {

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
				if defn, err := m.repo.GetIndexDefnById(common.IndexDefnId(defnRef.DefnId)); err == nil {
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
	if err != nil && err != fdb.FDB_RESULT_KEY_NOT_FOUND {
		return common.BUCKET_UUID_NIL, err
	}

	if topology != nil {
		for _, defnRef := range topology.Definitions {
			if state, _ := topology.GetStatusByDefn(common.IndexDefnId(defnRef.DefnId)); state != common.INDEX_STATE_DELETED {
				if defn, err := m.repo.GetIndexDefnById(common.IndexDefnId(defnRef.DefnId)); err == nil {
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
