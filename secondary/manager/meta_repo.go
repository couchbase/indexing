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
	"github.com/couchbaselabs/goprotobuf/proto"
	"fmt"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/protobuf"
	"log"
	"net/rpc"
	"sync"
)

type MetadataRepo struct {
	metaRepoHost string
	watcher      *watcher

	mutex    sync.Mutex
	isClosed bool
}

type Request struct {
	OpCode string
	Key    string
	Value  []byte
}

type Reply struct {
	Result []byte
}

///////////////////////////////////////////////////////
//  MetadataRepo
///////////////////////////////////////////////////////

func NewMetadataRepo(repoHost string,
	leader string) (*MetadataRepo, error) {

	watcher, err := startWatcher(leader)
	if err != nil {
		return nil, err
	}

	meta := &MetadataRepo{metaRepoHost: repoHost, watcher: watcher, isClosed: false}
	return meta, nil
}

func (c *MetadataRepo) Close() {

	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in MetadataRepo.Close() : %s.  Ignored.\n", r)
		}
	}()

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if !c.isClosed {
		c.isClosed = true
		c.watcher.Close()
	}
}

///////////////////////////////////////////////////////
//
///////////////////////////////////////////////////////

func (c *MetadataRepo) GetNextPartitionId() common.PartitionId {
	return common.PartitionId(0)
}

func (c *MetadataRepo) GetNextIndexInstId() common.IndexInstId {
	return common.IndexInstId(0)
}

///////////////////////////////////////////////////////
//  Index Definition : Lookup
///////////////////////////////////////////////////////

func (c *MetadataRepo) GetIndexDefnByName(name string) (*common.IndexDefn, error) {
	lookupName := indexDefnKeyByName(name)
	data, err := c.getMeta(lookupName)
	if err != nil {
		return nil, err
	}

	return unmarshallIndexDefn(data)
}

func (c *MetadataRepo) GetIndexDefnById(id common.IndexDefnId) (*common.IndexDefn, error) {
	lookupName := indexDefnKeyById(id)
	data, err := c.getMeta(lookupName)
	if err != nil {
		return nil, err
	}

	return unmarshallIndexDefn(data)
}

///////////////////////////////////////////////////////
//  Index Definition : DDL
///////////////////////////////////////////////////////

//
// TODO: This function is not transactional.
// TODO: We need the mechansim for the other coordinator to get notified on new metadata.
//
func (c *MetadataRepo) CreateIndex(defn *common.IndexDefn) error {

	// check if defn already exist
	exist, err := c.GetIndexDefnById(defn.DefnId)
	if exist != nil {
		// TODO: should not return error if not found (should return nil)
		return fmt.Errorf("Index Definition '%s' already exist", defn.Name)
	}

	// marshall the defn
	data, err := marshallIndexDefn(defn)
	if err != nil {
		return err
	}

	// save by defn name
	lookupName := indexDefnKeyByName(defn.Name)
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
		return fmt.Errorf("Index Definition '%d' does not exist", id)
	}

	lookupName := indexDefnKeyById(id)
	if err := c.deleteMeta(lookupName); err != nil {
		return err
	}

	lookupName = indexDefnKeyByName(exist.Name)
	if err := c.deleteMeta(lookupName); err != nil {
		return err
	}

	return nil
}

func (c *MetadataRepo) DropIndexByName(name string) error {

	// check if defn already exist
	exist, _ := c.GetIndexDefnByName(name)
	if exist == nil {
		// TODO: should not return error if not found (should return nil)
		return fmt.Errorf("Index Definition '%s' does not exist", name)
	}

	lookupName := indexDefnKeyByName(name)
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

func (c *MetadataRepo) ObserveForAdd(key string) *observeHandle {

	lookupName := indexDefnKeyByName(key)
	return c.watcher.addObserveForAdd(lookupName)
}

func (c *MetadataRepo) ObserveForDelete(key string) *observeHandle {

	lookupName := indexDefnKeyByName(key)
	return c.watcher.addObserveForDelete(lookupName)
}

///////////////////////////////////////////////////////
// private function : DDL
///////////////////////////////////////////////////////

func (c *MetadataRepo) getMetaFromWatcher(name string) ([]byte, error) {

	// Get the value from the local cache first
	value, err := c.watcher.Get(name)
	if err == nil && value != nil {
		log.Printf("MetadataRepo.getMeta(): Found metadata in local repository for key %s", name)
		return value, nil
	}

	return nil, err
}

func (c *MetadataRepo) getMeta(name string) ([]byte, error) {

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

	client, err := rpc.DialHTTP("tcp", c.metaRepoHost)
	if err != nil {
		return err
	}

	err = client.Call("RequestReceiver.NewRequest", request, reply)
	if err != nil {
		return err
	}

	return nil
}

///////////////////////////////////////////////////////
// private function : Index Definition
///////////////////////////////////////////////////////

func indexDefnKeyByName(name string) string {
	return fmt.Sprintf("IndexDefintionName/%s", name)
}

func indexDefnKeyById(id common.IndexDefnId) string {
	return fmt.Sprintf("IndexDefintionId/%d", id)
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
		SecExpressions:  defn.OnExprList,
		PartitionScheme: partnScheme,
		PartnExpression: proto.String(defn.PartitionKey),
	}

	return proto.Marshal(pDefn)
}

func unmarshallIndexDefn(data []byte) (*common.IndexDefn, error) {

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
		OnExprList:      pDefn.GetSecExpressions(),
		ExprType:        exprType,
		PartitionScheme: partnScheme,
		PartitionKey:    pDefn.GetPartnExpression()}

	return idxDefn, nil
}
