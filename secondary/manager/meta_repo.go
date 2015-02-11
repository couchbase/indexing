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
	//"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/couchbase/gometa/protocol"
	repo "github.com/couchbase/gometa/repository"
	gometa "github.com/couchbase/gometa/server"
	"github.com/couchbase/indexing/secondary/common"
	"net/rpc"
	"strconv"
	"strings"
	"sync"
)

type MetadataRepo struct {
	repo     RepoRef
	mutex    sync.Mutex
	isClosed bool
}

type RepoRef interface {
	getMeta(name string) ([]byte, error)
	setMeta(name string, value []byte) error
	deleteMeta(name string) error
	newIterator() (*MetaIterator, error)
	registerNotifier(notifier MetadataNotifier)
	setLocalValue(name string, value string) error
	getLocalValue(name string) (string, error)
	deleteLocalValue(name string) error
	close()
}

type RemoteRepoRef struct {
	remoteReqAddr string
	repository    *repo.Repository
	watcher       *watcher
}

type LocalRepoRef struct {
	server   *gometa.EmbeddedServer
	eventMgr *eventManager
	notifier MetadataNotifier
}

type MetaIterator struct {
	iterator *repo.RepoIterator
}

type Request struct {
	OpCode string
	Key    string
	Value  []byte
}

type Reply struct {
	Result []byte
}

type MetadataKind byte

const (
	KIND_UNKNOWN MetadataKind = iota
	KIND_INDEX_DEFN
	KIND_TOPOLOGY
	KIND_GLOBAL_TOPOLOGY
	KIND_STABILITY_TIMESTAMP
)

///////////////////////////////////////////////////////
//  Public Function : MetadataRepo
///////////////////////////////////////////////////////

func NewMetadataRepo(requestAddr string,
	leaderAddr string,
	config string,
	mgr *IndexManager) (*MetadataRepo, error) {

	ref, err := newRemoteRepoRef(requestAddr, leaderAddr, config, mgr)
	if err != nil {
		return nil, err
	}
	repo := &MetadataRepo{repo: ref, isClosed: false}
	return repo, nil
}

func NewLocalMetadataRepo(msgAddr string,
	eventMgr *eventManager,
	reqHandler protocol.CustomRequestHandler,
	repoName string) (*MetadataRepo, RequestServer, error) {

	ref, err := newLocalRepoRef(msgAddr, eventMgr, reqHandler, repoName)
	if err != nil {
		return nil, nil, err
	}
	repo := &MetadataRepo{repo: ref, isClosed: false}
	return repo, ref.server, nil
}

func (c *MetadataRepo) GetLocalIndexerId() (common.IndexerId, error) {
	val, err := c.GetLocalValue("IndexerId")
	return common.IndexerId(val), err
}

func (c *MetadataRepo) RegisterNotifier(notifier MetadataNotifier) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.repo.registerNotifier(notifier)
}

func (c *MetadataRepo) SetLocalValue(key string, value string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.repo.setLocalValue(key, value)
}

func (c *MetadataRepo) DeleteLocalValue(key string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.repo.deleteLocalValue(key)
}

func (c *MetadataRepo) GetLocalValue(key string) (string, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.repo.getLocalValue(key)
}

func (c *MetadataRepo) Close() {

	/*
		defer func() {
			if r := recover(); r != nil {
				common.Warnf("panic in MetadataRepo.Close() : %s.  Ignored.\n", r)
				common.Warnf("%s", debug.Stack())
			}
		}()
	*/

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if !c.isClosed {
		c.isClosed = true
		c.repo.close()
	}
}

///////////////////////////////////////////////////////
// Public Function : ID generation
///////////////////////////////////////////////////////

func (c *MetadataRepo) GetNextPartitionId() (common.PartitionId, error) {

	id, err := common.NewUUID()
	if err != nil {
		return common.PartitionId(0), err
	}

	return common.PartitionId(id.Uint64()), nil
}

func (c *MetadataRepo) GetNextIndexInstId() (common.IndexInstId, error) {

	id, err := common.NewUUID()
	if err != nil {
		return common.IndexInstId(0), err
	}

	return common.IndexInstId(id.Uint64()), nil
}

///////////////////////////////////////////////////////
//  Public Function : Index Defnition Lookup
///////////////////////////////////////////////////////

func (c *MetadataRepo) GetIndexDefnById(id common.IndexDefnId) (*common.IndexDefn, error) {
	lookupName := indexDefnKeyById(id)
	data, err := c.getMeta(lookupName)
	if err != nil {
		return nil, err
	}

	return common.UnmarshallIndexDefn(data)
}

func (c *MetadataRepo) GetIndexDefnByName(bucket string, name string) (*common.IndexDefn, error) {

	iter, err := c.NewIterator()
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	_, defn, err := iter.Next()
	for err == nil {
		if defn.Bucket == bucket && defn.Name == name {
			return defn, nil
		}
		_, defn, err = iter.Next()
	}

	return nil, nil
}

///////////////////////////////////////////////////////
//  Public Function : Stability Timestamp
///////////////////////////////////////////////////////

func (c *MetadataRepo) GetStabilityTimestamps() (*timestampListSerializable, error) {

	lookupName := stabilityTimestampKey()
	data, err := c.getMeta(lookupName)
	if err != nil {
		return nil, err
	}

	return unmarshallTimestampListSerializable(data)
}

func (c *MetadataRepo) SetStabilityTimestamps(timestamps *timestampListSerializable) error {

	data, err := marshallTimestampListSerializable(timestamps)
	if err != nil {
		return err
	}

	lookupName := stabilityTimestampKey()
	return c.setMeta(lookupName, data)
}

///////////////////////////////////////////////////////
//  Public Function : Index Topology
///////////////////////////////////////////////////////

func (c *MetadataRepo) GetTopologyByBucket(bucket string) (*IndexTopology, error) {

	lookupName := indexTopologyKey(bucket)
	data, err := c.getMeta(lookupName)
	if err != nil {
		return nil, err
	}

	return unmarshallIndexTopology(data)
}

func (c *MetadataRepo) SetTopologyByBucket(bucket string, topology *IndexTopology) error {

	topology.Version = topology.Version + 1

	data, err := MarshallIndexTopology(topology)
	if err != nil {
		return err
	}

	lookupName := indexTopologyKey(bucket)
	return c.setMeta(lookupName, data)
}

func (c *MetadataRepo) GetGlobalTopology() (*GlobalTopology, error) {

	lookupName := globalTopologyKey()
	data, err := c.getMeta(lookupName)
	if err != nil {
		return nil, err
	}

	return unmarshallGlobalTopology(data)
}

func (c *MetadataRepo) SetGlobalTopology(topology *GlobalTopology) error {

	data, err := marshallGlobalTopology(topology)
	if err != nil {
		return err
	}

	lookupName := globalTopologyKey()
	return c.setMeta(lookupName, data)
}

///////////////////////////////////////////////////////
//  Public Function : Index DDL
///////////////////////////////////////////////////////

//
// TODO: This function is not transactional.
//
func (c *MetadataRepo) CreateIndex(defn *common.IndexDefn) error {

	// check if defn already exist
	exist, err := c.GetIndexDefnById(defn.DefnId)
	if exist != nil {
		// TODO: should not return error if not found (should return nil)
		return NewError(ERROR_META_IDX_DEFN_EXIST, NORMAL, METADATA_REPO, nil,
			fmt.Sprintf("Index Definition '%s' already exist", defn.Name))
	}

	// marshall the defn
	data, err := common.MarshallIndexDefn(defn)
	if err != nil {
		return err
	}

	// save by defn id
	lookupName := indexDefnKeyById(defn.DefnId)
	if err := c.setMeta(lookupName, data); err != nil {
		return err
	}

	return nil
}

func (c *MetadataRepo) DropIndexById(id common.IndexDefnId) error {

	// check if defn already exist
	exist, _ := c.GetIndexDefnById(id)
	if exist == nil {
		// TODO: should not return error if not found (should return nil)
		return NewError(ERROR_META_IDX_DEFN_NOT_EXIST, NORMAL, METADATA_REPO, nil,
			fmt.Sprintf("Index Definition '%s' does not exist", id))
	}

	lookupName := indexDefnKeyById(id)
	if err := c.deleteMeta(lookupName); err != nil {
		return err
	}

	return nil
}

/////////////////////////////////////////////////////////////////////////////
// Public Function : RepoIterator
/////////////////////////////////////////////////////////////////////////////

//
// Create a new iterator
//
func (c *MetadataRepo) NewIterator() (*MetaIterator, error) {

	return c.repo.newIterator()
}

// Get value from iterator
func (i *MetaIterator) Next() (string, *common.IndexDefn, error) {

	for {
		key, content, err := i.iterator.Next()
		if err != nil {
			return "", nil, err
		}

		if isIndexDefnKey(key) {
			name := indexDefnIdFromKey(key)
			if name != "" {
				defn, err := common.UnmarshallIndexDefn(content)
				if err != nil {
					return "", nil, err
				}
				return name, defn, nil
			}
			return "", nil, NewError(ERROR_META_WRONG_KEY, NORMAL, METADATA_REPO, nil,
				fmt.Sprintf("Index Definition Key %s is mal-formed", key))
		}
	}
}

// close iterator
func (i *MetaIterator) Close() {

	i.iterator.Close()
}

///////////////////////////////////////////////////////
// private function : LocalRepoRef
///////////////////////////////////////////////////////

func newLocalRepoRef(msgAddr string,
	eventMgr *eventManager,
	reqHandler protocol.CustomRequestHandler,
	repoName string) (*LocalRepoRef, error) {

	repoRef := &LocalRepoRef{eventMgr: eventMgr, notifier: nil}
	server, err := gometa.RunEmbeddedServerWithCustomHandler(msgAddr, nil, reqHandler, repoName)
	if err != nil {
		return nil, err
	}

	repoRef.server = server
	return repoRef, nil
}

func (c *LocalRepoRef) getMeta(name string) ([]byte, error) {
	return c.server.GetValue(name)
}

func (c *LocalRepoRef) setMeta(name string, value []byte) error {
	if err := c.server.Set(name, value); err != nil {
		return err
	}

	evtType := getEventType(name)
	if c.eventMgr != nil && evtType != EVENT_NONE {
		c.eventMgr.notify(evtType, value)
	}
	return nil
}

func (c *LocalRepoRef) deleteMeta(name string) error {
	if err := c.server.Delete(name); err != nil {
		return err
	}

	if c.eventMgr != nil && findTypeFromKey(name) == KIND_INDEX_DEFN {
		c.eventMgr.notify(EVENT_DROP_INDEX, []byte(name))
	}
	return nil
}

func (c *LocalRepoRef) newIterator() (*MetaIterator, error) {
	iter, err := c.server.GetIterator("/", "")
	if err != nil {
		return nil, err
	}

	result := &MetaIterator{iterator: iter}

	return result, nil
}

func (c *LocalRepoRef) close() {

	// c.server.Terminate() is idempotent
	c.server.Terminate()
}

func getEventType(key string) EventType {

	evtType := EVENT_NONE
	metaType := findTypeFromKey(key)
	if metaType == KIND_INDEX_DEFN {
		evtType = EVENT_CREATE_INDEX
	} else if metaType == KIND_TOPOLOGY {
		evtType = EVENT_UPDATE_TOPOLOGY
	}

	return evtType
}

func (c *LocalRepoRef) registerNotifier(notifier MetadataNotifier) {
	c.notifier = notifier
}

func (c *LocalRepoRef) setLocalValue(key string, value string) error {
	return c.server.SetConfigValue(key, value)
}

func (c *LocalRepoRef) deleteLocalValue(key string) error {
	return c.server.DeleteConfigValue(key)
}

func (c *LocalRepoRef) getLocalValue(key string) (string, error) {
	return c.server.GetConfigValue(key)
}

///////////////////////////////////////////////////////
// private function : RemoteRepoRef
///////////////////////////////////////////////////////

func newRemoteRepoRef(requestAddr string,
	leaderAddr string,
	config string,
	mgr *IndexManager) (*RemoteRepoRef, error) {

	// Initialize local repository
	repository, err := repo.OpenRepository()
	if err != nil {
		return nil, err
	}

	// This is a blocking call unit the watcher is ready.  This means
	// the watcher has succesfully synchronized with the remote metadata
	// repository.

	var watcherId string
	env, err := newEnv(config)
	if err == nil {
		watcherId = env.getHostElectionPort()
	} else {
		uuid, err := common.NewUUID()
		if err != nil {
			return nil, err
		}
		watcherId = strconv.FormatUint(uuid.Uint64(), 10)
	}

	watcher, err := startWatcher(mgr, repository, leaderAddr, watcherId)
	if err != nil {
		return nil, err
	}

	repoRef := &RemoteRepoRef{remoteReqAddr: requestAddr,
		repository: repository,
		watcher:    watcher}

	return repoRef, nil
}

func (c *RemoteRepoRef) newIterator() (*MetaIterator, error) {
	iter, err := c.repository.NewIterator(repo.MAIN, "/", "")
	if err != nil {
		return nil, err
	}

	result := &MetaIterator{iterator: iter}

	return result, nil
}

func (c *RemoteRepoRef) getMetaFromWatcher(name string) ([]byte, error) {

	// Get the value from the local cache first
	value, err := c.watcher.Get(name)
	if err == nil && value != nil {
		common.Debugf("RemoteRepoRef.getMeta(): Found metadata in local repository for key %s", name)
		return value, nil
	}

	return nil, err
}

func (c *RemoteRepoRef) getMeta(name string) ([]byte, error) {
	common.Debugf("RemoteRepoRef.getMeta(): key=%s", name)

	// Get the metadata locally from watcher first
	value, err := c.getMetaFromWatcher(name)
	if err == nil && value != nil {
		return value, nil
	}

	// If metadata not exist, check the remote dictionary
	request := &Request{OpCode: "Get", Key: name, Value: nil}
	var reply *Reply
	if err := c.newDictionaryRequest(request, &reply); err != nil {
		return nil, err
	}

	common.Debugf("RemoteRepoRef.getMeta(): remote metadata for key %s exist=%v", name, reply != nil && reply.Result != nil)
	if reply != nil {
		// reply.Result can be nil if metadata does not exist
		return reply.Result, nil
	}

	return nil, nil
}

func (c *RemoteRepoRef) setMeta(name string, value []byte) error {

	request := &Request{OpCode: "Set", Key: name, Value: value}
	var reply *Reply
	if err := c.newDictionaryRequest(request, &reply); err != nil {
		return err
	}

	return nil
}

func (c *RemoteRepoRef) deleteMeta(name string) error {

	request := &Request{OpCode: "Delete", Key: name, Value: nil}
	var reply *Reply
	if err := c.newDictionaryRequest(request, &reply); err != nil {
		return err
	}

	return nil
}

func (c *RemoteRepoRef) newDictionaryRequest(request *Request, reply **Reply) error {

	client, err := rpc.DialHTTP("tcp", c.remoteReqAddr)
	if err != nil {
		return err
	}

	err = client.Call("RequestReceiver.NewRequest", request, reply)
	if err != nil {
		common.Debugf("MetadataRepo.newDictionaryRequest(): Got Error = %s", err.Error())
		return err
	}

	return nil
}

func (c *RemoteRepoRef) close() {
	if c.repository != nil {
		c.repository.Close()
		c.repository = nil
	}

	if c.watcher != nil {
		c.watcher.Close()
		c.watcher = nil
	}
}

func (c *RemoteRepoRef) registerNotifier(notifier MetadataNotifier) {
	panic("Function not supported")
}

func (c *RemoteRepoRef) setLocalValue(key string, value string) error {
	panic("Function not supported")
}

func (c *RemoteRepoRef) deleteLocalValue(key string) error {
	panic("Function not supported")
}

func (c *RemoteRepoRef) getLocalValue(key string) (string, error) {
	panic("Function not supported")
}

///////////////////////////////////////////////////////
// private function
///////////////////////////////////////////////////////

func (c *MetadataRepo) getMeta(name string) ([]byte, error) {
	return c.repo.getMeta(name)
}

func (c *MetadataRepo) setMeta(name string, value []byte) error {
	return c.repo.setMeta(name, value)
}

func (c *MetadataRepo) deleteMeta(name string) error {
	return c.repo.deleteMeta(name)
}

func findTypeFromKey(key string) MetadataKind {

	if isIndexDefnKey(key) {
		return KIND_INDEX_DEFN
	} else if isIndexTopologyKey(key) {
		return KIND_TOPOLOGY
	} else if isGlobalTopologyKey(key) {
		return KIND_GLOBAL_TOPOLOGY
	} else if isStabilityTimestampKey(key) {
		return KIND_STABILITY_TIMESTAMP
	}
	return KIND_UNKNOWN
}

///////////////////////////////////////////////////////
// package local function : Index Definition
///////////////////////////////////////////////////////

func indexDefnIdStr(id common.IndexDefnId) string {
	return strconv.FormatUint(uint64(id), 10)
}

func indexDefnId(key string) (common.IndexDefnId, error) {
	val, err := strconv.ParseUint(key, 10, 64)
	if err != nil {
		return common.IndexDefnId(0), err
	}
	return common.IndexDefnId(val), nil
}

func indexDefnKeyById(id common.IndexDefnId) string {
	return fmt.Sprintf("IndexDefinitionId/%d", id)
}

func isIndexDefnKey(key string) bool {
	return strings.Contains(key, "IndexDefinitionId/")
}

func indexDefnIdFromKey(key string) string {

	i := strings.Index(key, "IndexDefinitionId/")
	if i != -1 {
		return key[i+len("IndexDefinitionId/"):]
	}

	return ""
}

///////////////////////////////////////////////////////
// package local function : Index Topology
///////////////////////////////////////////////////////

func indexTopologyKey(bucket string) string {
	return fmt.Sprintf("IndexTopology/%s", bucket)
}

func getBucketFromTopologyKey(key string) string {
	i := strings.Index(key, "IndexTopology/")
	if i != -1 {
		return key[i+len("IndexTopology/"):]
	}

	return ""
}

func isIndexTopologyKey(key string) bool {
	return strings.Contains(key, "IndexTopology/")
}

func MarshallIndexTopology(topology *IndexTopology) ([]byte, error) {

	buf, err := json.Marshal(&topology)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func unmarshallIndexTopology(data []byte) (*IndexTopology, error) {

	topology := new(IndexTopology)
	if err := json.Unmarshal(data, topology); err != nil {
		return nil, err
	}

	return topology, nil
}

func globalTopologyKey() string {
	return "GlobalIndexTopology"
}

func isGlobalTopologyKey(key string) bool {
	return strings.Contains(key, "GlobalIndexTopology")
}

func marshallGlobalTopology(topology *GlobalTopology) ([]byte, error) {

	buf, err := json.Marshal(&topology)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func unmarshallGlobalTopology(data []byte) (*GlobalTopology, error) {

	topology := new(GlobalTopology)
	if err := json.Unmarshal(data, topology); err != nil {
		return nil, err
	}

	return topology, nil
}

///////////////////////////////////////////////////////
// package local function : Stability Timestamp
///////////////////////////////////////////////////////

func stabilityTimestampKey() string {
	return fmt.Sprintf("StabilityTimestamp")
}

func isStabilityTimestampKey(key string) bool {
	return strings.Contains(key, "StabilityTimestamp")
}

///////////////////////////////////////////////////////////
// package local function : Index Definition and Topology
///////////////////////////////////////////////////////////

//
// Add Index to Topology
//
func (m *MetadataRepo) addIndexToTopology(defn *common.IndexDefn, id common.IndexInstId) error {

	// get existing topology
	topology, err := m.GetTopologyByBucket(defn.Bucket)
	if err != nil {
		// TODO: Need to check what type of error before creating a new topologyi
		topology = new(IndexTopology)
		topology.Bucket = defn.Bucket
		topology.Version = 0
	}

	indexerId, err := m.GetLocalIndexerId()
	if err != nil {
		return err
	}

	topology.AddIndexDefinition(defn.Bucket, defn.Name, uint64(defn.DefnId),
		uint64(id), uint32(common.INDEX_STATE_CREATED), string(indexerId))

	// Add a reference of the bucket-level topology to the global topology.
	// If it fails later to create bucket-level topology, it will have
	// a dangling reference, but it is easier to discover this issue.  Otherwise,
	// we can end up having a bucket-level topology without being referenced.
	if err = m.addToGlobalTopologyIfNecessary(topology.Bucket); err != nil {
		return err
	}

	if err = m.SetTopologyByBucket(topology.Bucket, topology); err != nil {
		return err
	}

	return nil
}

//
// Delete Index from Topology
//
func (m *MetadataRepo) deleteIndexFromTopology(bucket string, id common.IndexDefnId) error {

	// get existing topology
	topology, err := m.GetTopologyByBucket(bucket)
	if err != nil {
		return err
	}

	topology.RemoveIndexDefinitionById(id)

	if err = m.SetTopologyByBucket(topology.Bucket, topology); err != nil {
		return err
	}

	return nil
}

//
// Add a reference of the bucket-level index topology to global topology.
// If not exist, create a new one.
//
func (m *MetadataRepo) addToGlobalTopologyIfNecessary(bucket string) error {

	globalTop, err := m.GetGlobalTopology()
	if err != nil {
		globalTop = new(GlobalTopology)
	}

	if globalTop.AddTopologyKeyIfNecessary(indexTopologyKey(bucket)) {
		return m.SetGlobalTopology(globalTop)
	}

	return nil
}

/*
///////////////////////////////////////////////////////
//  Interface : EventNotifier
///////////////////////////////////////////////////////

func (m *LocalRepoRef) OnNewProposal(txnid c.Txnid, op c.OpCode, key string, content []byte) error {

	if m.notifier == nil {
		return nil
	}

	common.Debugf("LocalRepoRef.OnNewProposal(): key %s", key)

	switch op {
	case c.OPCODE_ADD:
		if isIndexDefnKey(key) {
			return m.onNewProposalForCreateIndexDefn(txnid, op, key, content)
		}

	case c.OPCODE_SET:
		if isIndexDefnKey(key) {
			return m.onNewProposalForCreateIndexDefn(txnid, op, key, content)
		}

	case c.OPCODE_DELETE:
		if isIndexDefnKey(key) {
			return m.onNewProposalForDeleteIndexDefn(txnid, op, key, content)
		}
	}

	return nil
}

func (m *LocalRepoRef) OnCommit(txnid c.Txnid, key string) {
	// nothing to do
}

func (m *LocalRepoRef) onNewProposalForCreateIndexDefn(txnid c.Txnid, op c.OpCode, key string, content []byte) error {

	common.Debugf("LocalRepoRef.OnNewProposalForCreateIndexDefn(): key %s", key)

	indexDefn, err := common.UnmarshallIndexDefn(content)
	if err != nil {
		common.Debugf("LocalRepoRef.OnNewProposalForCreateIndexDefn(): fail to unmarshall index defn for key %s", key)
		return &c.RecoverableError{Reason: err.Error()}
	}

	if err := m.notifier.OnIndexCreate(indexDefn); err != nil {
		return &c.RecoverableError{Reason: err.Error()}
	}

	return nil
}

func (m *LocalRepoRef) onNewProposalForDeleteIndexDefn(txnid c.Txnid, op c.OpCode, key string, content []byte) error {

	common.Debugf("LocalRepoRef.OnNewProposalForDeleteIndexDefn(): key %s", key)

	i := strings.Index(key, "/")
	if i != -1 && i < len(key)-1 {

		id, err := strconv.ParseUint(key[i+1:], 10, 64)
		if err != nil {
			common.Debugf("LocalRepoRef.OnNewProposalForDeleteIndexDefn(): fail to unmarshall IndexDefnId key %s", key)
			return &c.RecoverableError{Reason: err.Error()}
		}

		if err := m.notifier.OnIndexDelete(common.IndexDefnId(id)); err != nil {
			return &c.RecoverableError{Reason: err.Error()}
		}
		return nil

	} else {
		common.Debugf("LocalRepoRef.OnNewProposalForDeleteIndexDefn(): fail to unmarshall IndexDefnId key %s", key)
		err := NewError(ERROR_META_FAIL_TO_PARSE_INT, NORMAL, METADATA_REPO, nil,
			"MetadataRepo.OnNewProposalForDeleteIndexDefn() : cannot parse index definition id")
		return &c.RecoverableError{Reason: err.Error()}
	}
}
*/
