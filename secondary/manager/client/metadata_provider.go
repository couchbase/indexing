// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package client

import (
	"errors"
	"fmt"
	"github.com/couchbase/gometa/common"
	gometaL "github.com/couchbase/gometa/log"
	"github.com/couchbase/gometa/message"
	"github.com/couchbase/gometa/protocol"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

///////////////////////////////////////////////////////
// Type Definition
///////////////////////////////////////////////////////

type MetadataProvider struct {
	providerId string
	watchers   map[c.IndexerId]*watcher
	timeout    int64
	repo       *metadataRepo
	mutex      sync.Mutex
}

type metadataRepo struct {
	definitions map[c.IndexDefnId]*c.IndexDefn
	instances   map[c.IndexDefnId]*IndexInstDistribution
	indices     map[c.IndexDefnId]*IndexMetadata
	mutex       sync.Mutex
}

type watcher struct {
	provider    *MetadataProvider
	leaderAddr  string
	factory     protocol.MsgFactory
	pendings    map[common.Txnid]protocol.LogEntryMsg
	killch      chan bool
	mutex       sync.Mutex
	indices     map[c.IndexDefnId]interface{}
	timerKillCh chan bool
	isClosed    bool
	serviceMap  *ServiceMap

	incomingReqs chan *protocol.RequestHandle
	pendingReqs  map[uint64]*protocol.RequestHandle // key : request id
	loggedReqs   map[common.Txnid]*protocol.RequestHandle
}

type IndexMetadata struct {
	Definition *c.IndexDefn
	Instances  []*InstanceDefn
}

type InstanceDefn struct {
	InstId    c.IndexInstId
	State     c.IndexState
	Error     string
	BuildTime []uint64
	IndexerId c.IndexerId
	Endpts    []c.Endpoint
}

var REQUEST_CHANNEL_COUNT = 1000

///////////////////////////////////////////////////////
// Public function : MetadataProvider
///////////////////////////////////////////////////////

func NewMetadataProvider(providerId string) (s *MetadataProvider, err error) {

	gometaL.Current = &logging.SystemLogger

	s = new(MetadataProvider)
	s.watchers = make(map[c.IndexerId]*watcher)
	s.repo = newMetadataRepo()
	s.timeout = int64(time.Minute) * 5

	s.providerId, err = s.getWatcherAddr(providerId)
	if err != nil {
		return nil, err
	}
	logging.Debugf("MetadataProvider.NewMetadataProvider(): MetadataProvider follower ID %s", s.providerId)

	return s, nil
}

func (o *MetadataProvider) SetTimeout(timeout int64) {
	o.timeout = timeout
}

func (o *MetadataProvider) WatchMetadata(indexAdminPort string) (c.IndexerId, error) {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	for _, watcher := range o.watchers {
		if watcher.getAdminAddr() == indexAdminPort {
			return watcher.getIndexerId(), nil
		}
	}

	watcher, err := o.startWatcher(indexAdminPort)
	if err != nil {
		return c.INDEXER_ID_NIL, errors.New(fmt.Sprintf("Cannot initiate connection to indexer port %s.", indexAdminPort))
	}

	indexerId := watcher.getIndexerId()
	o.watchers[indexerId] = watcher
	return indexerId, nil
}

func (o *MetadataProvider) UnwatchMetadata(indexerId c.IndexerId) {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	watcher, ok := o.watchers[indexerId]
	if !ok {
		return
	}

	delete(o.watchers, indexerId)
	if watcher != nil {
		watcher.cleanupIndices(o.repo)
		watcher.close()
	}
}

func (o *MetadataProvider) CreateIndexWithPlan(
	name, bucket, using, exprType, partnExpr, whereExpr string,
	secExprs []string, isPrimary bool, plan map[string]interface{}) (c.IndexDefnId, error) {

	// FindIndexByName will only return valid index
	if o.FindIndexByName(name, bucket) != nil {
		return c.IndexDefnId(0), errors.New(fmt.Sprintf("Index %s already exist.", name))
	}

	var deferred bool = false
	var nodes []string = nil

	if plan != nil {
		ns, ok := plan["nodes"].([]interface{})
		if ok {
			if len(ns) != 1 {
				return c.IndexDefnId(0), errors.New("Create Index is allowed for one and only one node")
			}
			nodes = []string{ns[0].(string)}
		}

		deferred, ok = plan["defer_build"].(bool)
		if !ok {
			deferred = false
		}
	}

	var watcher *watcher
	if nodes == nil {
		watcher = o.findNextAvailWatcher()
		if watcher == nil {
			return c.IndexDefnId(0),
				errors.New(fmt.Sprintf("Fails to create index.  Cannot find available node for new index"))
		}
	} else {
		watcher = o.findWatcherByNodeAddr(nodes[0])
		if watcher == nil {
			return c.IndexDefnId(0),
				errors.New(fmt.Sprintf("Fails to create index.  Node %s does not exist or is not running", nodes[0]))
		}
	}

	// set the node list using indexerId
	nodes = []string{string(watcher.getIndexerId())}

	defnID, err := c.NewIndexDefnId()
	if err != nil {
		return c.IndexDefnId(0), errors.New(fmt.Sprintf("Fails to create index. Fail to create uuid for index definition."))
	}

	idxDefn := &c.IndexDefn{
		DefnId:          defnID,
		Name:            name,
		Using:           c.IndexType(using),
		Bucket:          bucket,
		IsPrimary:       isPrimary,
		SecExprs:        secExprs,
		ExprType:        c.ExprType(exprType),
		PartitionScheme: c.SINGLE,
		PartitionKey:    partnExpr,
		WhereExpr:       whereExpr,
		Deferred:        deferred,
		Nodes:           nodes}

	content, err := c.MarshallIndexDefn(idxDefn)
	if err != nil {
		return 0, err
	}

	key := fmt.Sprintf("%d", defnID)
	_, err = watcher.makeRequest(OPCODE_CREATE_INDEX, key, content)

	return defnID, err
}

func (o *MetadataProvider) DropIndex(defnID c.IndexDefnId) error {

	// find index -- this method will not return the index if the index is in DELETED
	// status (but defn exists).
	meta := o.FindIndex(defnID)
	if meta == nil {
		return errors.New("Index does not exist.")
	}

	// find watcher -- This method does not check index status (return the watcher even
	// if index is in deleted status). So this return an error if  watcher is dropped
	// asynchronously (some parallel go-routine unwatchMetadata).
	watcher, err := o.findWatcherByDefnIdIgnoreStatus(defnID)
	if err != nil {
		return errors.New(fmt.Sprintf("Cannot locate cluster node hosting Index %s.", meta.Definition.Name))
	}

	// Make a request to drop the index, the index may be dropped in parallel before this MetadataProvider
	// is aware of it.  (e.g. bucket flush).  The server side will have to check for this condition.
	key := fmt.Sprintf("%d", defnID)
	_, err = watcher.makeRequest(OPCODE_DROP_INDEX, key, []byte(""))
	return err
}

func (o *MetadataProvider) BuildIndexes(defnIDs []c.IndexDefnId) error {

	watcherIndexMap := make(map[c.IndexerId][]c.IndexDefnId)

	for _, id := range defnIDs {

		// find index -- this method will not return the index if the index is in DELETED
		// status (but defn exists).
		meta := o.FindIndex(id)
		if meta == nil {
			return errors.New("Cannot build index. Index Definition not found")
		}

		if meta.Instances != nil && meta.Instances[0].State != c.INDEX_STATE_READY {
			return errors.New(fmt.Sprintf("Index %s is not in READY state.", meta.Definition.Name))
		}

		// find watcher -- This method does not check index status (return the watcher even
		// if index is in deleted status). So this return an error if  watcher is dropped
		// asynchronously (some parallel go-routine unwatchMetadata).
		watcher, err := o.findWatcherByDefnIdIgnoreStatus(id)
		if err != nil {
			return errors.New(fmt.Sprintf("Cannot locate cluster node hosting Index %s.", meta.Definition.Name))
		}

		indexerId := watcher.getIndexerId()
		_, ok := watcherIndexMap[indexerId]
		if !ok {
			watcherIndexMap[indexerId] = make([]c.IndexDefnId, 0)
		}
		watcherIndexMap[indexerId] = append(watcherIndexMap[indexerId], id)
	}

	for indexerId, idList := range watcherIndexMap {

		watcher, err := o.findWatcherByIndexerId(indexerId)
		if err != nil {
			meta := o.findIndexIgnoreStatus(idList[0])
			if meta != nil {
				return errors.New(fmt.Sprintf("Cannot locate cluster node hosting Index %s.", meta.Definition.Name))
			} else {
				return errors.New("Cannot locate cluster node hosting Index.")
			}
		}

		list := BuildIndexIdList(idList)

		content, err := MarshallIndexIdList(list)
		if err != nil {
			return err
		}

		_, err = watcher.makeRequest(OPCODE_BUILD_INDEX, "Index Build", content)
		if err != nil {
			return err
		}
	}

	return nil
}

func (o *MetadataProvider) ListIndex() []*IndexMetadata {
	o.repo.mutex.Lock()
	defer o.repo.mutex.Unlock()

	result := make([]*IndexMetadata, 0, len(o.repo.indices))
	for _, meta := range o.repo.indices {
		if isValidIndex(meta) {
			result = append(result, meta)
		}
	}

	return result
}

func (o *MetadataProvider) FindIndex(id c.IndexDefnId) *IndexMetadata {
	o.repo.mutex.Lock()
	defer o.repo.mutex.Unlock()

	if meta, ok := o.repo.indices[id]; ok {
		if isValidIndex(meta) {
			return meta
		}
	}

	return nil
}

func (o *MetadataProvider) FindServiceForIndex(id c.IndexDefnId) (adminport string, queryport string, err error) {

	// find index -- this method will not return the index if the index is in DELETED
	// status (but defn exists).
	meta := o.FindIndex(id)
	if meta == nil {
		return "", "", errors.New(fmt.Sprintf("Index %s does not exist.", meta.Definition.Name))
	}

	watcher, err := o.findWatcherByDefnIdIgnoreStatus(id)
	if err != nil {
		return "", "", errors.New(fmt.Sprintf("Cannot locate cluster node hosting Index %s.", meta.Definition.Name))
	}

	return watcher.getAdminAddr(), watcher.getScanAddr(), nil
}

func (o *MetadataProvider) FindServiceForIndexer(id c.IndexerId) (adminport string, queryport string, err error) {

	watcher, err := o.findWatcherByIndexerId(id)
	if err != nil {
		return "", "", errors.New(fmt.Sprintf("Cannot locate cluster node."))
	}

	return watcher.getAdminAddr(), watcher.getScanAddr(), nil
}

func (o *MetadataProvider) FindIndexByName(name string, bucket string) *IndexMetadata {
	o.repo.mutex.Lock()
	defer o.repo.mutex.Unlock()

	for _, meta := range o.repo.indices {
		if isValidIndex(meta) {
			if meta.Definition.Name == name && meta.Definition.Bucket == bucket {
				return meta
			}
		}
	}

	return nil
}

func (o *MetadataProvider) Close() {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	for _, watcher := range o.watchers {
		watcher.close()
	}
}

///////////////////////////////////////////////////////
// private function : MetadataProvider
///////////////////////////////////////////////////////

func (o *MetadataProvider) startWatcher(addr string) (*watcher, error) {

	s := newWatcher(o, addr)
	readych := make(chan bool)

	// TODO: call Close() to cleanup the state upon retry by the MetadataProvider server
	go protocol.RunWatcherServerWithRequest(
		s.leaderAddr,
		s,
		s,
		s.factory,
		s.killch,
		readych)

	// TODO: timeout
	<-readych

	s.startTimer()

	if err := s.refreshServiceMap(); err != nil {
		s.close()
		return nil, err
	}

	return s, nil
}

func (o *MetadataProvider) findIndexIgnoreStatus(id c.IndexDefnId) *IndexMetadata {
	o.repo.mutex.Lock()
	defer o.repo.mutex.Unlock()

	if meta, ok := o.repo.indices[id]; ok {
		return meta
	}

	return nil
}

func (o *MetadataProvider) findNextAvailWatcher() *watcher {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	var minCount = math.MaxUint16
	var nextWatcher *watcher = nil

	for _, watcher := range o.watchers {
		count := o.repo.getValidDefnCount(watcher.getIndexerId())
		if count <= minCount {
			minCount = count
			nextWatcher = watcher
		}
	}

	return nextWatcher
}

func (o *MetadataProvider) findWatcherByDefnIdIgnoreStatus(defnId c.IndexDefnId) (*watcher, error) {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	for _, watcher := range o.watchers {
		if o.repo.hasDefnIgnoreStatus(watcher.getIndexerId(), defnId) {
			return watcher, nil
		}
	}

	return nil, errors.New(fmt.Sprintf("MetadataProvider.findWatcher() : Cannot find watcher with index defniton %v", defnId))
}

func (o *MetadataProvider) findWatcherByIndexerId(id c.IndexerId) (*watcher, error) {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	for indexerId, watcher := range o.watchers {
		if indexerId == id {
			return watcher, nil
		}
	}

	return nil, errors.New(fmt.Sprintf("MetadataProvider.findWatcher() : Cannot find watcher with IndexerId %v", id))
}

func (o *MetadataProvider) getWatcherAddr(MetadataProviderId string) (string, error) {

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	if len(addrs) == 0 {
		return "", errors.New("MetadataProvider.getWatcherAddr() : No network address is available")
	}

	for _, addr := range addrs {
		switch s := addr.(type) {
		case *net.IPAddr:
			if s.IP.IsGlobalUnicast() {
				return fmt.Sprintf("%s:indexer:MetadataProvider:%s", addr.String(), MetadataProviderId), nil
			}
		case *net.IPNet:
			if s.IP.IsGlobalUnicast() {
				return fmt.Sprintf("%s:indexer:MetadataProvider:%s", addr.String(), MetadataProviderId), nil
			}
		}
	}

	return "", errors.New("MetadataProvider.getWatcherAddr() : Fail to find an IP address")
}

func (o *MetadataProvider) findWatcherByNodeAddr(nodeAddr string) *watcher {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	for _, watcher := range o.watchers {
		if watcher.getNodeAddr() == nodeAddr {
			return watcher
		}
	}

	return nil
}

func isValidIndex(meta *IndexMetadata) bool {

	if meta.Definition == nil {
		return false
	}

	if len(meta.Instances) == 0 {
		return false
	}

	if meta.Instances[0].State == c.INDEX_STATE_CREATED ||
		meta.Instances[0].State == c.INDEX_STATE_DELETED {
		return false
	}

	return true
}

///////////////////////////////////////////////////////
// private function : metadataRepo
///////////////////////////////////////////////////////

func newMetadataRepo() *metadataRepo {

	return &metadataRepo{
		definitions: make(map[c.IndexDefnId]*c.IndexDefn),
		instances:   make(map[c.IndexDefnId]*IndexInstDistribution),
		indices:     make(map[c.IndexDefnId]*IndexMetadata)}
}

func (r *metadataRepo) addDefn(defn *c.IndexDefn) {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.definitions[defn.DefnId] = defn
	r.indices[defn.DefnId] = r.makeIndexMetadata(defn)

	inst, ok := r.instances[defn.DefnId]
	if ok {
		r.updateIndexMetadataNoLock(defn.DefnId, inst)
	}
}

func (r *metadataRepo) hasDefnIgnoreStatus(indexerId c.IndexerId, defnId c.IndexDefnId) bool {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	meta, ok := r.indices[defnId]
	return ok && meta.Instances[0].IndexerId == indexerId
}

func (r *metadataRepo) getValidDefnCount(indexerId c.IndexerId) int {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	count := 0

	for _, meta := range r.indices {
		if isValidIndex(meta) && meta.Instances[0].IndexerId == indexerId {
			count++
		}
	}

	return count
}

func (r *metadataRepo) removeDefn(defnId c.IndexDefnId) {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	delete(r.definitions, defnId)
	delete(r.instances, defnId)
	delete(r.indices, defnId)
}

func (r *metadataRepo) updateTopology(topology *IndexTopology) {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	for _, defnRef := range topology.Definitions {
		defnId := c.IndexDefnId(defnRef.DefnId)
		for _, instRef := range defnRef.Instances {
			r.instances[defnId] = &instRef
			r.updateIndexMetadataNoLock(defnId, &instRef)
		}
	}
}

func (r *metadataRepo) unmarshallAndAddDefn(content []byte) error {

	defn, err := c.UnmarshallIndexDefn(content)
	if err != nil {
		return err
	}
	r.addDefn(defn)
	return nil
}

func (r *metadataRepo) unmarshallAndAddInst(content []byte) error {

	topology, err := unmarshallIndexTopology(content)
	if err != nil {
		return err
	}
	r.updateTopology(topology)
	return nil
}

func (r *metadataRepo) makeIndexMetadata(defn *c.IndexDefn) *IndexMetadata {

	return &IndexMetadata{Definition: defn,
		Instances: nil}
}

func (r *metadataRepo) updateIndexMetadataNoLock(defnId c.IndexDefnId, inst *IndexInstDistribution) {

	meta, ok := r.indices[defnId]
	if ok {
		idxInst := new(InstanceDefn)
		idxInst.InstId = c.IndexInstId(inst.InstId)
		idxInst.State = c.IndexState(inst.State)
		idxInst.Error = inst.Error
		idxInst.BuildTime = inst.BuildTime

		for _, partition := range inst.Partitions {
			for _, slice := range partition.SinglePartition.Slices {
				idxInst.IndexerId = c.IndexerId(slice.IndexerId)
				break
			}
		}
		meta.Instances = []*InstanceDefn{idxInst}
	}
}

///////////////////////////////////////////////////////
// private function : Watcher
///////////////////////////////////////////////////////

func newWatcher(o *MetadataProvider, addr string) *watcher {
	s := new(watcher)

	s.provider = o
	s.leaderAddr = addr
	s.killch = make(chan bool, 1) // make it buffered to unblock sender
	s.factory = message.NewConcreteMsgFactory()
	s.pendings = make(map[common.Txnid]protocol.LogEntryMsg)
	s.incomingReqs = make(chan *protocol.RequestHandle, REQUEST_CHANNEL_COUNT)
	s.pendingReqs = make(map[uint64]*protocol.RequestHandle)
	s.loggedReqs = make(map[common.Txnid]*protocol.RequestHandle)
	s.indices = make(map[c.IndexDefnId]interface{})
	s.isClosed = false

	return s
}

func (w *watcher) getIndexerId() c.IndexerId {

	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.serviceMap == nil {
		panic("Index node metadata is not initialized")
	}

	return c.IndexerId(w.serviceMap.IndexerId)
}

func (w *watcher) getNodeAddr() string {

	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.serviceMap == nil {
		panic("Index node metadata is not initialized")
	}

	return w.serviceMap.NodeAddr
}

func (w *watcher) getAdminAddr() string {

	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.serviceMap == nil {
		panic("Index node metadata is not initialized")
	}

	return w.serviceMap.AdminAddr
}

func (w *watcher) getScanAddr() string {

	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.serviceMap == nil {
		panic("Index node metadata is not initialized")
	}

	return w.serviceMap.ScanAddr
}

func (w *watcher) addDefn(defnId c.IndexDefnId) {

	w.mutex.Lock()
	defer w.mutex.Unlock()

	w.indices[defnId] = nil
}

func (w *watcher) removeDefn(defnId c.IndexDefnId) {

	w.mutex.Lock()
	defer w.mutex.Unlock()

	delete(w.indices, defnId)
}

func (w *watcher) addDefnWithNoLock(defnId c.IndexDefnId) {

	w.indices[defnId] = nil
}

func (w *watcher) removeDefnWithNoLock(defnId c.IndexDefnId) {

	delete(w.indices, defnId)
}

func (w *watcher) refreshServiceMap() error {

	content, err := w.makeRequest(OPCODE_SERVICE_MAP, "Service Map", []byte(""))
	if err != nil {
		logging.Errorf("watcher.refreshServiceMap() %s", err)
		return err
	}

	srvMap, err := UnmarshallServiceMap(content)
	if err != nil {
		logging.Errorf("watcher.refreshServiceMap() %s", err)
		return err
	}

	w.mutex.Lock()
	defer w.mutex.Unlock()

	w.serviceMap = srvMap
	return nil
}

func (w *watcher) cleanupIndices(repo *metadataRepo) {

	w.mutex.Lock()
	defer w.mutex.Unlock()

	for defnId, _ := range w.indices {
		repo.removeDefn(defnId)
	}
}

func (w *watcher) close() {

	// kill the watcherServer
	if len(w.killch) == 0 {
		w.killch <- true
	}

	// kill the timeoutChecker
	if len(w.timerKillCh) == 0 {
		w.timerKillCh <- true
	}

	w.cleanupOnClose()
}

func (w *watcher) makeRequest(opCode common.OpCode, key string, content []byte) ([]byte, error) {

	uuid, err := c.NewUUID()
	if err != nil {
		return nil, err
	}
	id := uuid.Uint64()

	request := w.factory.CreateRequest(id, uint32(opCode), key, content)

	handle := &protocol.RequestHandle{Request: request, Err: nil, StartTime: 0, Content: nil}
	handle.CondVar = sync.NewCond(&handle.Mutex)

	handle.CondVar.L.Lock()
	defer handle.CondVar.L.Unlock()

	if w.queueRequest(handle) {
		handle.CondVar.Wait()
	}

	return handle.Content, handle.Err
}

func (w *watcher) startTimer() {

	w.timerKillCh = make(chan bool, 1)
	go w.timeoutChecker()
}

func (w *watcher) timeoutChecker() {

	timer := time.NewTicker(time.Second)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			w.cleanupOnTimeout()
		case <-w.timerKillCh:
			return
		}
	}
}

func (w *watcher) cleanupOnTimeout() {

	// Mutex for protecting the following go-routine:
	// 1) WatcherServer main processing loop
	// 2) Commit / LogProposal / Respond / Abort
	// 3) CleanupOnClose / CleaupOnError

	w.mutex.Lock()
	defer w.mutex.Unlock()

	current := time.Now().UnixNano()

	for key, request := range w.pendingReqs {
		if current-request.StartTime >= w.provider.timeout {
			delete(w.pendingReqs, key)
			w.signalError(request, "Request Timeout")
		}
	}

	for key, request := range w.loggedReqs {
		if current-request.StartTime >= w.provider.timeout {
			delete(w.loggedReqs, key)
			w.signalError(request, "Request Timeout")
		}
	}
}

func (w *watcher) cleanupOnClose() {

	// Mutex for protecting the following go-routine:
	// 1) WatcherServer main processing loop
	// 2) Commit / LogProposal / Respond / Abort
	// 3) CleanupOnTimeout / CleaupOnError

	w.mutex.Lock()
	defer w.mutex.Unlock()

	w.isClosed = true

	for len(w.incomingReqs) != 0 {
		request := <-w.incomingReqs
		w.signalError(request, "Terminate Request during cleanup")
	}

	for key, request := range w.pendingReqs {
		delete(w.pendingReqs, key)
		w.signalError(request, "Terminate Request during cleanup")
	}

	for key, request := range w.loggedReqs {
		delete(w.loggedReqs, key)
		w.signalError(request, "Terminate Request during cleanup")
	}
}

func (w *watcher) signalError(request *protocol.RequestHandle, errStr string) {
	request.Err = errors.New(errStr)
	request.CondVar.L.Lock()
	defer request.CondVar.L.Unlock()
	request.CondVar.Signal()
}

///////////////////////////////////////////////////////
// private function
///////////////////////////////////////////////////////

func isIndexDefnKey(key string) bool {
	return strings.Contains(key, "IndexDefinitionId/")
}

func isIndexTopologyKey(key string) bool {
	return strings.Contains(key, "IndexTopology/")
}

///////////////////////////////////////////////////////
// Interface : RequestMgr
///////////////////////////////////////////////////////

func (w *watcher) queueRequest(handle *protocol.RequestHandle) bool {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.isClosed {
		handle.Err = errors.New("Connection is shutting down.  Cannot process incoming request")
		return false
	}

	w.incomingReqs <- handle
	return true
}

func (w *watcher) AddPendingRequest(handle *protocol.RequestHandle) {

	// Mutex for protecting the following go-routine:
	// 1) Commit / LogProposal / Respond / Abort
	// 2) CleanupOnClose / CleaupOnTimeout / CleanupOnError

	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.isClosed {
		w.signalError(handle, "Terminate Request during cleanup")
		return
	}

	// remember the request
	handle.StartTime = time.Now().UnixNano()
	w.pendingReqs[handle.Request.GetReqId()] = handle
}

func (w *watcher) GetRequestChannel() <-chan *protocol.RequestHandle {

	// Mutex for protecting the following go-routine:
	// 1) CleanupOnClose / CleaupOnTimeout

	w.mutex.Lock()
	defer w.mutex.Unlock()

	return (<-chan *protocol.RequestHandle)(w.incomingReqs)
}

func (w *watcher) CleanupOnError() {

	// Mutex for protecting the following go-routine:
	// 1) Commit / LogProposal / Respond / Abort
	// 2) CleanupOnClose / CleaupOnTimeout

	w.mutex.Lock()
	defer w.mutex.Unlock()

	for key, request := range w.pendingReqs {
		delete(w.pendingReqs, key)
		w.signalError(request, "Terminate Request due to server termination")
	}

	for key, request := range w.loggedReqs {
		delete(w.loggedReqs, key)
		w.signalError(request, "Terminate Request due to server termination")
	}
}

///////////////////////////////////////////////////////
// Interface : QuorumVerifier
///////////////////////////////////////////////////////

func (w *watcher) HasQuorum(count int) bool {
	return count == 1
}

///////////////////////////////////////////////////////
// Interface : ServerAction
///////////////////////////////////////////////////////

///////////////////////////////////////////////////////
// Server Action for Environment
///////////////////////////////////////////////////////

func (w *watcher) GetEnsembleSize() uint64 {
	return 1
}

func (w *watcher) GetQuorumVerifier() protocol.QuorumVerifier {
	return w
}

///////////////////////////////////////////////////////
// Server Action for Broadcast stage (normal execution)
///////////////////////////////////////////////////////

func (w *watcher) Commit(txid common.Txnid) error {

	w.mutex.Lock()
	defer w.mutex.Unlock()

	msg, ok := w.pendings[txid]
	if !ok {
		logging.Warnf("Watcher.commit(): unknown txnid %d.  Txn not processed at commit", txid)
		return nil
	}

	delete(w.pendings, txid)
	err := w.processChange(msg.GetOpCode(), msg.GetKey(), msg.GetContent())

	handle, ok := w.loggedReqs[txid]
	if ok {
		delete(w.loggedReqs, txid)

		handle.CondVar.L.Lock()
		defer handle.CondVar.L.Unlock()

		handle.CondVar.Signal()
	}

	return err
}

func (w *watcher) LogProposal(p protocol.ProposalMsg) error {

	w.mutex.Lock()
	defer w.mutex.Unlock()

	msg := w.factory.CreateLogEntry(p.GetTxnid(), p.GetOpCode(), p.GetKey(), p.GetContent())
	w.pendings[common.Txnid(p.GetTxnid())] = msg

	handle, ok := w.pendingReqs[p.GetReqId()]
	if ok {
		delete(w.pendingReqs, p.GetReqId())
		w.loggedReqs[common.Txnid(p.GetTxnid())] = handle
	}

	return nil
}

func (w *watcher) Abort(fid string, reqId uint64, err string) error {
	w.respond(reqId, err, nil)
	return nil
}

func (w *watcher) Respond(fid string, reqId uint64, err string, content []byte) error {
	w.respond(reqId, err, content)
	return nil
}

func (w *watcher) respond(reqId uint64, err string, content []byte) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	handle, ok := w.pendingReqs[reqId]
	if ok {
		delete(w.pendingReqs, reqId)

		handle.CondVar.L.Lock()
		defer handle.CondVar.L.Unlock()

		if len(err) != 0 {
			handle.Err = errors.New(err)
		}

		logging.Debugf("watcher.Respond() : len(content) %d", len(content))
		handle.Content = content

		handle.CondVar.Signal()
	}
}

func (w *watcher) GetFollowerId() string {
	return w.provider.providerId
}

func (w *watcher) GetNextTxnId() common.Txnid {
	panic("Calling watcher.GetCommitedEntries() : not supported")
}

///////////////////////////////////////////////////////
// Server Action for retrieving repository state
///////////////////////////////////////////////////////

func (w *watcher) GetLastLoggedTxid() (common.Txnid, error) {
	return common.Txnid(0), nil
}

func (w *watcher) GetLastCommittedTxid() (common.Txnid, error) {
	return common.Txnid(0), nil
}

func (w *watcher) GetStatus() protocol.PeerStatus {
	return protocol.WATCHING
}

func (w *watcher) GetCurrentEpoch() (uint32, error) {
	return 0, nil
}

func (w *watcher) GetAcceptedEpoch() (uint32, error) {
	return 0, nil
}

///////////////////////////////////////////////////////
// Server Action for updating repository state
///////////////////////////////////////////////////////

func (w *watcher) NotifyNewAcceptedEpoch(epoch uint32) error {
	// no-op
	return nil
}

func (w *watcher) NotifyNewCurrentEpoch(epoch uint32) error {
	// no-op
	return nil
}

///////////////////////////////////////////////////////
// Function for discovery phase
///////////////////////////////////////////////////////

func (w *watcher) GetCommitedEntries(txid1, txid2 common.Txnid) (<-chan protocol.LogEntryMsg, <-chan error, chan<- bool, error) {
	panic("Calling watcher.GetCommitedEntries() : not supported")
}

func (w *watcher) LogAndCommit(txid common.Txnid, op uint32, key string, content []byte, toCommit bool) error {

	if err := w.processChange(op, key, content); err != nil {
		logging.Errorf("watcher.LogAndCommit(): receive error when processing log entry from server.  Error = %v", err)
	}

	return nil
}

func (w *watcher) processChange(op uint32, key string, content []byte) error {

	logging.Debugf("watcher.processChange(): key = %v", key)
	defer logging.Debugf("watcher.processChange(): done -> key = %v", key)

	opCode := common.OpCode(op)

	switch opCode {
	case common.OPCODE_ADD, common.OPCODE_SET:
		if isIndexDefnKey(key) {
			if len(content) == 0 {
				logging.Debugf("watcher.processChange(): content of key = %v is empty.", key)
			}

			id, err := extractDefnIdFromKey(key)
			if err != nil {
				return err
			}
			w.addDefnWithNoLock(c.IndexDefnId(id))
			return w.provider.repo.unmarshallAndAddDefn(content)

		} else if isIndexTopologyKey(key) {
			if len(content) == 0 {
				logging.Debugf("watcher.processChange(): content of key = %v is empty.", key)
			}
			return w.provider.repo.unmarshallAndAddInst(content)
		}
	case common.OPCODE_DELETE:
		if isIndexDefnKey(key) {

			id, err := extractDefnIdFromKey(key)
			if err != nil {
				return err
			}
			w.removeDefnWithNoLock(c.IndexDefnId(id))
			w.provider.repo.removeDefn(c.IndexDefnId(id))
		}
	}

	return nil
}

func extractDefnIdFromKey(key string) (c.IndexDefnId, error) {
	i := strings.Index(key, "/")
	if i != -1 && i < len(key)-1 {
		id, err := strconv.ParseUint(key[i+1:], 10, 64)
		return c.IndexDefnId(id), err
	}

	return c.IndexDefnId(0), errors.New("watcher.processChange() : cannot parse index definition id")
}
