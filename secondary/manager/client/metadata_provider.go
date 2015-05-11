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
	providerId   string
	watchers     map[c.IndexerId]*watcher
	pendings     map[c.IndexerId]chan bool
	timeout      int64
	repo         *metadataRepo
	mutex        sync.Mutex
	watcherCount int
}

type metadataRepo struct {
	definitions map[c.IndexDefnId]*c.IndexDefn
	instances   map[c.IndexDefnId]*IndexInstDistribution
	indices     map[c.IndexDefnId]*IndexMetadata
	mutex       sync.RWMutex
}

type watcher struct {
	provider    *MetadataProvider
	leaderAddr  string
	factory     protocol.MsgFactory
	pendings    map[common.Txnid]protocol.LogEntryMsg
	killch      chan bool
	alivech     chan bool
	pingch      chan bool
	mutex       sync.Mutex
	indices     map[c.IndexDefnId]interface{}
	timerKillCh chan bool
	isClosed    bool
	serviceMap  *ServiceMap

	incomingReqs chan *protocol.RequestHandle
	pendingReqs  map[uint64]*protocol.RequestHandle // key : request id
	loggedReqs   map[common.Txnid]*protocol.RequestHandle

	notifiers map[c.IndexDefnId]*event
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

type event struct {
	defnId   c.IndexDefnId
	status   c.IndexState
	notifyCh chan error
}

type IndexerStatus struct {
	Adminport string
	Connected bool
}

type watcherCallback func(string, c.IndexerId, c.IndexerId)

var REQUEST_CHANNEL_COUNT = 1000

///////////////////////////////////////////////////////
// Public function : MetadataProvider
///////////////////////////////////////////////////////

func NewMetadataProvider(providerId string) (s *MetadataProvider, err error) {

	gometaL.Current = &logging.SystemLogger

	s = new(MetadataProvider)
	s.watchers = make(map[c.IndexerId]*watcher)
	s.pendings = make(map[c.IndexerId]chan bool)
	s.repo = newMetadataRepo()
	s.timeout = int64(time.Second) * 30

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

func (o *MetadataProvider) WatchMetadata(indexAdminPort string, callback watcherCallback) c.IndexerId {

	o.mutex.Lock()
	defer o.mutex.Unlock()

	logging.Debugf("MetadataProvider.WatchMetadata(): indexer %v", indexAdminPort)

	for _, watcher := range o.watchers {
		if watcher.getAdminAddr() == indexAdminPort {
			return watcher.getIndexerId()
		}
	}

	// start a watcher to the indexer admin
	watcher, readych := o.startWatcher(indexAdminPort)

	// wait for indexer to connect
	success, _ := watcher.waitForReady(readych, 1000, nil)
	if success {
		// if successfully connected, retrieve indexerId
		success, _ = watcher.notifyReady(indexAdminPort, 0, nil)
		if success {
			logging.Infof("WatchMetadata(): successfully reach indexer at %v.", indexAdminPort)
			// watcher succesfully initialized, add it to MetadataProvider
			o.addWatcherNoLock(watcher, c.INDEXER_ID_NIL)
			return watcher.getIndexerId()

		} else {
			// watcher is ready, but no able to read indexerId
			readych = nil
		}
	}

	logging.Infof("WatchMetadata(): unable to reach indexer at %v. Retry in background.", indexAdminPort)

	// watcher is not connected to indexer or fail to get indexer id,
	// create a temporary index id
	o.watcherCount = o.watcherCount + 1
	tempIndexerId := c.IndexerId(fmt.Sprintf("%v_Indexer_Id_%d", indexAdminPort, o.watcherCount))
	killch := make(chan bool, 1)
	o.pendings[tempIndexerId] = killch

	// retry it in the background.  Return a temporary indexerId for book-keeping.
	go o.retryHelper(watcher, readych, indexAdminPort, tempIndexerId, killch, callback)
	return tempIndexerId
}

func (o *MetadataProvider) UnwatchMetadata(indexerId c.IndexerId) {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	logging.Infof("UnwatchMetadata(): indexer %v", indexerId)

	watcher, ok := o.watchers[indexerId]
	if !ok {
		killch, ok := o.pendings[indexerId]
		if ok {
			delete(o.pendings, indexerId)
			killch <- true
		}
		return
	}

	delete(o.watchers, indexerId)
	if watcher != nil {
		watcher.close()
		watcher.cleanupIndices(o.repo)
	}
}

func (o *MetadataProvider) CheckIndexerStatus() []IndexerStatus {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	status := make([]IndexerStatus, len(o.watchers))
	i := 0
	for _, watcher := range o.watchers {
		status[i].Adminport = watcher.leaderAddr
		status[i].Connected = watcher.isAlive()
		logging.Infof("MetadataProvider.CheckIndexerStatus(): adminport=%v connected=%v", status[i].Adminport, status[i].Connected)
		i++
	}

	return status
}

func (o *MetadataProvider) CreateIndexWithPlan(
	name, bucket, using, exprType, partnExpr, whereExpr string,
	secExprs []string, isPrimary bool, plan map[string]interface{}) (c.IndexDefnId, error, bool) {

	// FindIndexByName will only return valid index
	if o.FindIndexByName(name, bucket) != nil {
		return c.IndexDefnId(0), errors.New(fmt.Sprintf("Index %s already exist.", name)), false
	}

	var deferred bool = false
	var wait bool = false
	var nodes []string = nil

	if plan != nil {
		logging.Debugf("MetadataProvider:CreateIndexWithPlan(): plan %v", plan)

		ns, ok := plan["nodes"].([]interface{})
		if ok {
			if len(ns) != 1 {
				return c.IndexDefnId(0), errors.New("Create Index is allowed for one and only one node"), false
			}
			n, ok := ns[0].(string)
			if ok {
				nodes = []string{n}
			} else {
				return c.IndexDefnId(0),
					errors.New(fmt.Sprintf("Fails to create index.  Node '%v' is not valid", plan["nodes"])),
					false
			}
		} else {
			n, ok := plan["nodes"].(string)
			if ok {
				nodes = []string{n}
			} else if _, ok := plan["nodes"]; ok {
				return c.IndexDefnId(0),
					errors.New(fmt.Sprintf("Fails to create index.  Node '%v' is not valid", plan["nodes"])),
					false
			}
		}

		deferred2, ok := plan["defer_build"].(bool)
		if !ok {
			deferred_str, ok := plan["defer_build"].(string)
			if ok {
				var err error
				deferred2, err = strconv.ParseBool(deferred_str)
				if err != nil {
					return c.IndexDefnId(0),
						errors.New("Fails to create index.  Parameter defer_build must be a boolean value of (true or false)."),
						false
				}
				deferred = deferred2
				if !deferred {
					wait = true
				}
			} else if _, ok := plan["defer_build"]; ok {
				return c.IndexDefnId(0),
					errors.New("Fails to create index.  Parameter defer_build must be a boolean value of (true or false)."),
					false
			}
		} else {
			deferred = deferred2
			if !deferred {
				wait = true
			}
		}
	}

	logging.Debugf("MetadataProvider:CreateIndex(): deferred_build %v sync %v nodes %v", deferred, wait, nodes)

	watcher, err, retry := o.findWatcherWithRetry(nodes)
	if err != nil {
		return c.IndexDefnId(0), err, retry
	}

	// set the node list using indexerId
	nodes = []string{string(watcher.getIndexerId())}

	defnID, err := c.NewIndexDefnId()
	if err != nil {
		return c.IndexDefnId(0),
			errors.New(fmt.Sprintf("Fails to create index. Fail to create uuid for index definition.")),
			false
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
		return 0, err, false
	}

	key := fmt.Sprintf("%d", defnID)
	if _, err = watcher.makeRequest(OPCODE_CREATE_INDEX, key, content); err != nil {
		return defnID, err, false
	}

	if wait {
		err := watcher.waitForEvent(defnID, c.INDEX_STATE_ACTIVE)
		return defnID, err, false
	}

	return defnID, nil, false
}

func (o *MetadataProvider) findWatcherWithRetry(nodes []string) (*watcher, error, bool) {

	var watcher *watcher
	count := 0

RETRY1:
	watcher = nil
	errCode := 0
	if nodes == nil {
		watcher = o.findNextAvailWatcher()
		if watcher == nil {
			errCode = 1
		}
	} else {
		watcher = o.findWatcherByNodeAddr(nodes[0])
		if watcher == nil {
			errCode = 2
		}
	}

	if errCode != 0 && count < 10 {
		logging.Debugf("MetadataProvider:findWatcherWithRetry(): cannot find available watcher. Retrying ...")
		time.Sleep(time.Duration(500) * time.Millisecond)
		count++
		goto RETRY1
	}

	if errCode == 1 {
		stmt1 := "Fails to create index.  There is no available index service that can process this request at this time."
		stmt2 := "Index Service can be in bootstrap, recovery, or non-reachable."
		stmt3 := "Please retry the operation at a later time."
		return nil, errors.New(fmt.Sprintf("%s %s %s", stmt1, stmt2, stmt3)), false

	} else if errCode == 2 {
		stmt1 := "Fails to create index.  Node %s does not exist or is not running"
		return nil, errors.New(fmt.Sprintf(stmt1, nodes[0])), true
	}

	return watcher, nil, false
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

			if meta.Instances[0].State == c.INDEX_STATE_INITIAL || meta.Instances[0].State == c.INDEX_STATE_CATCHUP {
				return errors.New(fmt.Sprintf("Index %s is being built .", meta.Definition.Name))
			}

			if meta.Instances[0].State == c.INDEX_STATE_ACTIVE {
				return errors.New(fmt.Sprintf("Index %s is already built .", meta.Definition.Name))
			}

			return errors.New("Cannot build index. Index Definition not found")
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

	indices := o.repo.listDefn()
	result := make([]*IndexMetadata, 0, len(indices))

	for _, meta := range indices {
		if o.isValidIndexFromActiveIndexer(meta) {
			result = append(result, meta)
		}
	}

	return result
}

func (o *MetadataProvider) FindIndex(id c.IndexDefnId) *IndexMetadata {

	indices := o.repo.listDefn()
	if meta, ok := indices[id]; ok {
		if o.isValidIndexFromActiveIndexer(meta) {
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
		return "", "", errors.New(fmt.Sprintf("Index does not exist."))
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

func (o *MetadataProvider) UpdateServiceAddrForIndexer(id c.IndexerId, adminport string) error {

	watcher, err := o.findWatcherByIndexerId(id)
	if err != nil {
		return errors.New(fmt.Sprintf("Cannot locate cluster node."))
	}

	return watcher.updateServiceMap(adminport)
}

func (o *MetadataProvider) FindIndexByName(name string, bucket string) *IndexMetadata {

	indices := o.repo.listDefn()
	for _, meta := range indices {
		if o.isValidIndexFromActiveIndexer(meta) {
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
	logging.Infof("MetadataProvider is terminated. Cleaning up ...")

	for _, watcher := range o.watchers {
		watcher.close()
	}
}

///////////////////////////////////////////////////////
// private function : MetadataProvider
///////////////////////////////////////////////////////

func (o *MetadataProvider) isActiveWatcherNoLock(indexerId c.IndexerId) bool {

	for _, watcher := range o.watchers {
		if watcher.getIndexerId() == indexerId {
			return true
		}
	}

	return false
}

func (o *MetadataProvider) retryHelper(watcher *watcher, readych chan bool, indexAdminPort string,
	tempIndexerId c.IndexerId, killch chan bool, callback watcherCallback) {

	if readych != nil {
		// if watcher is not ready, let's wait.
		if _, killed := watcher.waitForReady(readych, 0, killch); killed {
			watcher.cleanupIndices(o.repo)
			return
		}
	}

	// get the indexerId
	if _, killed := watcher.notifyReady(indexAdminPort, -1, killch); killed {
		watcher.close()
		watcher.cleanupIndices(o.repo)
		return
	}

	// add the watcher
	func() {
		o.mutex.Lock()
		defer o.mutex.Unlock()
		o.addWatcherNoLock(watcher, tempIndexerId)
	}()

	logging.Infof("WatchMetadata(): Successfully connected to indexer at %v after retry.", indexAdminPort)

	indexerId := watcher.getIndexerId()
	callback(indexAdminPort, indexerId, tempIndexerId)
}

func (o *MetadataProvider) addWatcherNoLock(watcher *watcher, tempIndexerId c.IndexerId) {

	delete(o.pendings, tempIndexerId)

	indexerId := watcher.getIndexerId()
	oldWatcher, ok := o.watchers[indexerId]
	if ok {
		// there is an old watcher with an matching indexerId.  Close it ...
		oldWatcher.close()
	}
	o.watchers[indexerId] = watcher
}

func (o *MetadataProvider) startWatcher(addr string) (*watcher, chan bool) {

	s := newWatcher(o, addr)
	readych := make(chan bool)

	// TODO: call Close() to cleanup the state upon retry by the MetadataProvider server
	go protocol.RunWatcherServerWithRequest(
		s.leaderAddr,
		s,
		s,
		s.factory,
		s.killch,
		readych,
		s.alivech,
		s.pingch)

	return s, readych
}

func (o *MetadataProvider) findIndexIgnoreStatus(id c.IndexDefnId) *IndexMetadata {

	indices := o.repo.listDefn()
	if meta, ok := indices[id]; ok {
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

func (o *MetadataProvider) isValidIndexFromActiveIndexer(meta *IndexMetadata) bool {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	if !isValidIndex(meta) {
		return false
	}

	return o.isActiveWatcherNoLock(meta.Instances[0].IndexerId)
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

func (r *metadataRepo) listDefn() map[c.IndexDefnId]*IndexMetadata {

	r.mutex.RLock()
	defer r.mutex.RUnlock()

	result := make(map[c.IndexDefnId]*IndexMetadata)
	for id, meta := range r.indices {
		result[id] = meta
	}

	return result
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

	r.mutex.RLock()
	defer r.mutex.RUnlock()

	meta, ok := r.indices[defnId]
	return ok && meta.Instances != nil && meta.Instances[0].IndexerId == indexerId
}

func (r *metadataRepo) hasDefnMatchingStatus(defnId c.IndexDefnId, status c.IndexState) bool {

	r.mutex.RLock()
	defer r.mutex.RUnlock()

	meta, ok := r.indices[defnId]
	return ok && meta != nil && meta.Instances != nil && meta.Instances[0].State == status
}

func (r *metadataRepo) getDefnError(defnId c.IndexDefnId) error {

	r.mutex.RLock()
	defer r.mutex.RUnlock()

	meta, ok := r.indices[defnId]
	if ok && meta.Instances != nil && len(meta.Instances[0].Error) != 0 {
		return errors.New(meta.Instances[0].Error)
	}
	return nil
}

func (r *metadataRepo) getValidDefnCount(indexerId c.IndexerId) int {

	r.mutex.RLock()
	defer r.mutex.RUnlock()

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

	return &IndexMetadata{Definition: defn, Instances: nil}
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
	s.alivech = make(chan bool, 1)
	s.pingch = make(chan bool, 1)
	s.factory = message.NewConcreteMsgFactory()
	s.pendings = make(map[common.Txnid]protocol.LogEntryMsg)
	s.incomingReqs = make(chan *protocol.RequestHandle, REQUEST_CHANNEL_COUNT)
	s.pendingReqs = make(map[uint64]*protocol.RequestHandle)
	s.loggedReqs = make(map[common.Txnid]*protocol.RequestHandle)
	s.notifiers = make(map[c.IndexDefnId]*event)
	s.indices = make(map[c.IndexDefnId]interface{})
	s.isClosed = false

	return s
}

func (w *watcher) waitForReady(readych chan bool, timeout int, killch chan bool) (done bool, killed bool) {

	if killch == nil {
		killch = make(chan bool, 1)
	}

	if timeout > 0 {
		// if there is a timeout
		ticker := time.NewTicker(time.Duration(timeout) * time.Millisecond)
		select {
		case <-readych:
			return true, false
		case <-ticker.C:
			return false, false
		case <-killch:
			w.killch <- true
			return false, true
		}
	} else {
		// if there is no timeout
		select {
		case <-readych:
			return true, false
		case <-killch:
			w.killch <- true
			return false, true
		}
	}

	return true, false
}

func (w *watcher) notifyReady(addr string, retry int, killch chan bool) (done bool, killed bool) {

	if killch == nil {
		killch = make(chan bool, 1)
	}

	// start a timer if it has not restarted yet
	if w.timerKillCh == nil {
		w.startTimer()
	}

RETRY2:
	// get IndexerId from indexer
	err := w.refreshServiceMap()
	if err == nil {
		err = w.updateServiceMap(addr)
	}

	if err != nil && retry != 0 {
		ticker := time.NewTicker(time.Duration(500) * time.Millisecond)
		select {
		case <-killch:
			return false, true
		case <-ticker.C:
			// do nothing
		}
		retry--
		goto RETRY2
	}

	if err != nil {
		return false, false
	}

	return true, false
}

func (w *watcher) isAlive() bool {

	for len(w.pingch) != 0 {
		<-w.pingch
	}

	for len(w.alivech) != 0 {
		<-w.alivech
	}

	w.pingch <- true

	ticker := time.NewTicker(time.Duration(100) * time.Millisecond)
	select {
	case <-w.alivech:
		return true
	case <-ticker.C:
		return false
	}

	return false
}

func (w *watcher) updateServiceMap(adminport string) error {

	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.serviceMap == nil {
		panic("Index node metadata is not initialized")
	}

	h, _, err := net.SplitHostPort(adminport)
	if err != nil {
		return err
	}

	if len(h) > 0 {
		w.serviceMap.AdminAddr = adminport

		_, p, err := net.SplitHostPort(w.serviceMap.NodeAddr)
		if err != nil {
			return err
		}
		w.serviceMap.NodeAddr = net.JoinHostPort(h, p)

		_, p, err = net.SplitHostPort(w.serviceMap.ScanAddr)
		if err != nil {
			return err
		}
		w.serviceMap.ScanAddr = net.JoinHostPort(h, p)

		_, p, err = net.SplitHostPort(w.serviceMap.HttpAddr)
		if err != nil {
			return err
		}
		w.serviceMap.HttpAddr = net.JoinHostPort(h, p)
	}

	return nil
}

func (w *watcher) waitForEvent(defnId c.IndexDefnId, status c.IndexState) error {

	event := &event{defnId: defnId, status: status, notifyCh: make(chan error, 1)}
	if w.registerEvent(event) {
		logging.Debugf("watcher.waitForEvent(): wait event : id %v status %v", event.defnId, event.status)
		err, ok := <-event.notifyCh
		if ok && err != nil {
			logging.Debugf("watcher.waitForEvent(): wait arrives : id %v status %v", event.defnId, event.status)
			return err
		}
	}
	return nil
}

func (w *watcher) registerEvent(event *event) bool {

	// by locking the watcher, it will not process any commit
	// while this function is executing
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if !w.provider.repo.hasDefnMatchingStatus(event.defnId, event.status) {
		logging.Debugf("watcher.registerEvent(): add event : id %v status %v", event.defnId, event.status)
		w.notifiers[event.defnId] = event
		return true
	}

	logging.Debugf("watcher.registerEvent(): found event existed: id %v status %v", event.defnId, event.status)
	return false
}

func (w *watcher) notifyEventNoLock() {

	for defnId, event := range w.notifiers {
		if w.provider.repo.hasDefnMatchingStatus(defnId, event.status) {
			delete(w.notifiers, defnId)
			close(event.notifyCh)
		} else if err := w.provider.repo.getDefnError(defnId); err != nil {
			delete(w.notifiers, defnId)
			event.notifyCh <- err
			close(event.notifyCh)
		}
	}
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

	logging.Infof("Unwatching metadata for indexer at %v.", w.leaderAddr)

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
	handle.StartTime = time.Now().UnixNano()
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

	for key, event := range w.notifiers {
		delete(w.notifiers, key)
		event.notifyCh <- errors.New("Terminate Request due to client termination")
		close(event.notifyCh)
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
		// Once the watcher is synchronized with the server, it is possible that the first message
		// is a commit.  It is OK to ignore this commit, since during server synchronization, the
		// server is responsible to send a snapshot of the repository that includes this commit txid.
		// If the server cannot send a snapshot that is as recent as this commit's txid, server
		// synchronization will fail, so it won't even come to this function.
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
			if err := w.provider.repo.unmarshallAndAddDefn(content); err != nil {
				return err
			}
			w.notifyEventNoLock()

		} else if isIndexTopologyKey(key) {
			if len(content) == 0 {
				logging.Debugf("watcher.processChange(): content of key = %v is empty.", key)
			}
			if err := w.provider.repo.unmarshallAndAddInst(content); err != nil {
				return err
			}
			w.notifyEventNoLock()
		}
	case common.OPCODE_DELETE:
		if isIndexDefnKey(key) {

			id, err := extractDefnIdFromKey(key)
			if err != nil {
				return err
			}
			w.removeDefnWithNoLock(c.IndexDefnId(id))
			w.provider.repo.removeDefn(c.IndexDefnId(id))
			w.notifyEventNoLock()
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
