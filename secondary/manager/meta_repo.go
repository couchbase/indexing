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
	"encoding/binary"
	"encoding/json"
	"fmt"
	repo "github.com/couchbase/gometa/repository"
	"github.com/couchbase/indexing/secondary/common"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
	"github.com/couchbaselabs/goprotobuf/proto"
	"math/rand"
	"net/rpc"
	"strconv"
	"strings"
	"sync"
)

type MetadataRepo struct {
	remoteReqAddr string
	repository    *repo.Repository
	watcher       *watcher

	mutex    sync.Mutex
	isClosed bool
}

type MetaIterator struct {
	repository *repo.Repository
	iterator   *repo.RepoIterator
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
	KIND_INDEX_INSTANCE_ID
	KIND_INDEX_PARTITION_ID
	KIND_STABILITY_TIMESTAMP
)

///////////////////////////////////////////////////////
//  Public Function : MetadataRepo
///////////////////////////////////////////////////////

func NewMetadataRepo(requestAddr string,
	leaderAddr string,
	config string,
	mgr *IndexManager) (*MetadataRepo, error) {

	// Initialize local repository
	repository, err := repo.OpenRepository()
	if err != nil {
		return nil, err
	}

	// This is a blocking call unit the watcher is ready.  This means
	// the watcher has succesfully synchronized with the remote metadata
	// repository.
	var watcherId string = strconv.FormatUint(uint64(rand.Uint32()), 10)
	env, err := newEnv(config)
	if err == nil {
		watcherId = env.getHostElectionPort()
	}

	watcher, err := startWatcher(mgr, repository, leaderAddr, watcherId)
	if err != nil {
		return nil, err
	}

	meta := &MetadataRepo{remoteReqAddr: requestAddr,
		repository: repository,
		watcher:    watcher,
		isClosed:   false}
	return meta, nil
}

func (c *MetadataRepo) Close() {

	defer func() {
		if r := recover(); r != nil {
			common.Warnf("panic in MetadataRepo.Close() : %s.  Ignored.\n", r)
		}
	}()

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if !c.isClosed {
		c.isClosed = true
		c.repository.Close()
		c.watcher.Close()
	}
}

///////////////////////////////////////////////////////
// Public Function : ID generation
///////////////////////////////////////////////////////

func (c *MetadataRepo) GetNextPartitionId() (common.PartitionId, error) {

	id, err := c.GetIndexPartitionId()
	if err != nil {
		return common.PartitionId(0), err
	}

	id = id + 1
	if err := c.SetIndexPartitionId(id); err != nil {
		return common.PartitionId(0), err
	}

	return common.PartitionId(id), nil
}

func (c *MetadataRepo) GetNextIndexInstId() (common.IndexInstId, error) {

	id, err := c.GetIndexInstanceId()
	if err != nil {
		return common.IndexInstId(0), err
	}

	id = id + 1
	if err := c.SetIndexInstanceId(id); err != nil {
		return common.IndexInstId(0), err
	}

	return common.IndexInstId(id), nil
}

///////////////////////////////////////////////////////
//  Public Function : Index Defnition Lookup
///////////////////////////////////////////////////////

func (c *MetadataRepo) GetIndexDefnByName(bucket string, name string) (*common.IndexDefn, error) {
	lookupName := indexDefnKeyByName(indexName(bucket, name))
	data, err := c.getMeta(lookupName)
	if err != nil {
		return nil, err
	}

	return UnmarshallIndexDefn(data)
}

func (c *MetadataRepo) GetIndexDefnById(id common.IndexDefnId) (*common.IndexDefn, error) {
	lookupName := indexDefnKeyById(id)
	data, err := c.getMeta(lookupName)
	if err != nil {
		return nil, err
	}

	return UnmarshallIndexDefn(data)
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
	exist, err := c.GetIndexDefnByName(defn.Bucket, defn.Name)
	if exist != nil {
		// TODO: should not return error if not found (should return nil)
		return NewError(ERROR_META_IDX_DEFN_EXIST, NORMAL, METADATA_REPO, nil,
			fmt.Sprintf("Index Definition '%s' already exist", defn.Name))
	}

	// marshall the defn
	data, err := marshallIndexDefn(defn)
	if err != nil {
		return err
	}

	// save by defn name
	lookupName := indexDefnKeyByName(indexName(defn.Bucket, defn.Name))
	if err := c.setMeta(lookupName, data); err != nil {
		return err
	}

	// save by defn id
	lookupName = indexDefnKeyById(defn.DefnId)
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

	lookupName = indexDefnKeyByName(indexName(exist.Bucket, exist.Name))
	if err := c.deleteMeta(lookupName); err != nil {
		return err
	}

	return nil
}

func (c *MetadataRepo) DropIndexByName(bucket string, name string) error {

	// check if defn already exist
	exist, _ := c.GetIndexDefnByName(bucket, name)
	if exist == nil {
		// TODO: should not return error if not found (should return nil)
		return NewError(ERROR_META_IDX_DEFN_NOT_EXIST, NORMAL, METADATA_REPO, nil,
			fmt.Sprintf("Index Definition '%s' does not exist", name))
	}

	lookupName := indexDefnKeyByName(indexName(bucket, name))
	if err := c.deleteMeta(lookupName); err != nil {
		return err
	}

	lookupName = indexDefnKeyById(exist.DefnId)
	if err := c.deleteMeta(lookupName); err != nil {
		return err
	}

	return nil
}

///////////////////////////////////////////////////////
// public function : Observe
///////////////////////////////////////////////////////

func (c *MetadataRepo) ObserveAddIndexDefn(bucket string, key string) (*common.IndexDefn, error) {
	lookupName := indexDefnKeyByName(indexName(bucket, key))
	c.watcher.observeForAdd(lookupName)
	return c.GetIndexDefnByName(bucket, key)
}

func (c *MetadataRepo) ObserveDeleteIndexDefn(bucket string, key string) {
	lookupName := indexDefnKeyByName(indexName(bucket, key))
	c.watcher.observeForDelete(lookupName)
}

/////////////////////////////////////////////////////////////////////////////
// Public Function : RepoIterator
/////////////////////////////////////////////////////////////////////////////

//
// Create a new iterator
//
func (c *MetadataRepo) NewIterator() (*MetaIterator, error) {

	iter, err := c.repository.NewIterator("/", "")
	if err != nil {
		return nil, err
	}

	result := &MetaIterator{
		iterator:   iter,
		repository: c.repository}

	return result, nil
}

// Get value from iterator
func (i *MetaIterator) Next() (key string, content []byte, err error) {

	for {
		key, content, err = i.iterator.Next()
		if err != nil {
			return "", nil, err
		}

		if isIndexDefnKey(key) {
			name := indexDefnNameFromKey(key)
			if name != "" {
				return name, content, nil
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
// public function : Local Write
///////////////////////////////////////////////////////

func (c *MetadataRepo) SetLocalMeta(name string, value []byte) error {
	key := LocalMetaKey(name)
	return c.watcher.Set(key, value)
}

func (c *MetadataRepo) GetLocalMeta(name string) ([]byte, error) {
	key := LocalMetaKey(name)
	return c.getMetaFromWatcher(key)
}

func (c *MetadataRepo) GetLocalRepo() *repo.Repository {
	return c.repository
}

func LocalMetaKey(name string) string {
	return fmt.Sprintf("LocalMetadata/%s", name)
}

///////////////////////////////////////////////////////
// private function : DDL
///////////////////////////////////////////////////////

func (c *MetadataRepo) getMetaFromWatcher(name string) ([]byte, error) {

	// Get the value from the local cache first
	value, err := c.watcher.Get(name)
	if err == nil && value != nil {
		common.Debugf("MetadataRepo.getMeta(): Found metadata in local repository for key %s", name)
		return value, nil
	}

	return nil, err
}

func (c *MetadataRepo) getMeta(name string) ([]byte, error) {
	common.Debugf("MetadataRepo.getMeta(): key=%s", name)

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

	common.Debugf("MetadataRepo.getMeta(): remote metadata for key %s exist=%v", name, reply != nil && reply.Result != nil)
	if reply != nil {
		// reply.Result can be nil if metadata does not exist
		return reply.Result, nil
	}

	return nil, nil
}

func (c *MetadataRepo) setMeta(name string, value []byte) error {

	request := &Request{OpCode: "Set", Key: name, Value: value}
	var reply *Reply
	if err := c.newDictionaryRequest(request, &reply); err != nil {
		return err
	}

	return nil
}

func (c *MetadataRepo) deleteMeta(name string) error {

	request := &Request{OpCode: "Delete", Key: name, Value: nil}
	var reply *Reply
	if err := c.newDictionaryRequest(request, &reply); err != nil {
		return err
	}

	return nil
}

func (c *MetadataRepo) newDictionaryRequest(request *Request, reply **Reply) error {

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

func findTypeFromKey(key string) MetadataKind {

	if isIndexDefnKey(key) {
		return KIND_INDEX_DEFN
	} else if isIndexTopologyKey(key) {
		return KIND_TOPOLOGY
	} else if isGlobalTopologyKey(key) {
		return KIND_GLOBAL_TOPOLOGY
	} else if isIndexInstanceIdKey(key) {
		return KIND_INDEX_INSTANCE_ID
	} else if isIndexPartitionIdKey(key) {
		return KIND_INDEX_PARTITION_ID
	} else if isStabilityTimestampKey(key) {
		return KIND_STABILITY_TIMESTAMP
	}
	return KIND_UNKNOWN
}

///////////////////////////////////////////////////////
// package local function : Index Definition
///////////////////////////////////////////////////////

func bucketFromIndexDefnRepoKey(key string) string {
	name := indexDefnNameFromKey(key)
	return bucketFromIndexDefnName(name)
}

func nameFromIndexDefnRepoKey(key string) string {
	name := indexDefnNameFromKey(key)
	return nameFromIndexDefnName(name)
}

func bucketFromIndexDefnName(name string) string {
	i := strings.Index(name, "/")
	if i != -1 {
		return name[:i]
	}

	return ""
}

func nameFromIndexDefnName(name string) string {
	i := strings.Index(name, "/")
	if i != -1 && i < len(name)-1 {
		return name[i+1:]
	}

	return ""
}

func indexName(bucket string, name string) string {
	return bucket + "/" + name
}

func indexDefnKeyByName(name string) string {
	return fmt.Sprintf("IndexDefinitionName/%s", name)
}

func indexDefnKeyById(id common.IndexDefnId) string {
	return fmt.Sprintf("IndexDefinitionId/%d", id)
}

func isIndexDefnKey(key string) bool {
	return strings.Contains(key, "IndexDefinitionName/")
}

func indexDefnNameFromKey(key string) string {

	i := strings.Index(key, "IndexDefinitionName/")
	if i != -1 {
		return key[i+len("IndexDefinitionName/"):]
	}

	return ""
}

//
//
// TODO: This function is copied from indexer.kv_sender.  It would be nice if this
// go to common.
//
func marshallIndexDefn(defn *common.IndexDefn) ([]byte, error) {

	using := protobuf.StorageType(
		protobuf.StorageType_value[string(defn.Using)]).Enum()

	exprType := protobuf.ExprType(
		protobuf.ExprType_value[string(defn.ExprType)]).Enum()

	partnScheme := protobuf.PartitionScheme(
		protobuf.PartitionScheme_value[string(defn.PartitionScheme)]).Enum()

	pDefn := &protobuf.IndexDefn{
		DefnID:          proto.Uint64(uint64(defn.DefnId)),
		Bucket:          proto.String(defn.Bucket),
		IsPrimary:       proto.Bool(defn.IsPrimary),
		Name:            proto.String(defn.Name),
		Using:           using,
		ExprType:        exprType,
		SecExpressions:  defn.SecExprs,
		PartitionScheme: partnScheme,
		PartnExpression: proto.String(defn.PartitionKey),
	}

	return proto.Marshal(pDefn)
}

//
// !! This function is made public only for testing purpose.
//
func UnmarshallIndexDefn(data []byte) (*common.IndexDefn, error) {

	pDefn := new(protobuf.IndexDefn)
	if err := proto.Unmarshal(data, pDefn); err != nil {
		return nil, err
	}

	using := common.IndexType(pDefn.GetUsing().String())
	exprType := common.ExprType(pDefn.GetExprType().String())
	partnScheme := common.PartitionScheme(pDefn.GetPartitionScheme().String())

	idxDefn := &common.IndexDefn{
		DefnId:          common.IndexDefnId(pDefn.GetDefnID()),
		Name:            pDefn.GetName(),
		Using:           using,
		Bucket:          pDefn.GetBucket(),
		IsPrimary:       pDefn.GetIsPrimary(),
		SecExprs:        pDefn.GetSecExpressions(),
		ExprType:        exprType,
		PartitionScheme: partnScheme,
		PartitionKey:    pDefn.GetPartnExpression()}

	return idxDefn, nil
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
// package local function : Index Instance Id
///////////////////////////////////////////////////////

func (c *MetadataRepo) GetIndexInstanceId() (uint64, error) {

	lookupName := indexInstanceIdKey()
	data, err := c.getMeta(lookupName)
	if err != nil {
		// TODO : Differentiate the case for real error
		return 0, nil
	}

	id, read := binary.Uvarint(data)
	if read < 0 {
		return 0, NewError2(ERROR_META_FAIL_TO_PARSE_INT, METADATA_REPO)
	}

	return id, nil
}

func (c *MetadataRepo) SetIndexInstanceId(id uint64) error {

	data := make([]byte, 8)
	binary.PutUvarint(data, id)

	lookupName := indexInstanceIdKey()
	return c.setMeta(lookupName, data)
}

func indexInstanceIdKey() string {
	return "IndexInstanceId"
}

func isIndexInstanceIdKey(key string) bool {
	return strings.Contains(key, "IndexInstanceId")
}

///////////////////////////////////////////////////////
// package local function : Index Partition Id
///////////////////////////////////////////////////////

func (c *MetadataRepo) GetIndexPartitionId() (uint64, error) {

	lookupName := indexPartitionIdKey()
	data, err := c.getMeta(lookupName)
	if err != nil {
		// TODO : Differentiate the case for real error
		return 0, nil
	}

	id, read := binary.Uvarint(data)
	if read < 0 {
		return 0, NewError2(ERROR_META_FAIL_TO_PARSE_INT, METADATA_REPO)
	}

	return id, nil
}

func (c *MetadataRepo) SetIndexPartitionId(id uint64) error {

	data := make([]byte, 8)
	binary.PutUvarint(data, id)

	lookupName := indexPartitionIdKey()
	return c.setMeta(lookupName, data)
}

func indexPartitionIdKey() string {
	return "IndexPartitionId"
}

func isIndexPartitionIdKey(key string) bool {
	return strings.Contains(key, "IndexPartitionId")
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
