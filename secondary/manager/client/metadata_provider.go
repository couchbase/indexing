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
	"github.com/couchbase/gometa/message"
	"github.com/couchbase/gometa/protocol"
	c "github.com/couchbase/indexing/secondary/common"
	"math/rand"
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
	watchers   map[string]*watcher
	repo       *metadataRepo
	mutex      sync.Mutex
}

type metadataRepo struct {
	definitions map[c.IndexDefnId]*c.IndexDefn
	instances   map[c.IndexDefnId]*IndexInstDistribution
	mutex       sync.Mutex
}

type watcher struct {
	provider   *MetadataProvider
	leaderAddr string
	factory    protocol.MsgFactory
	pendings   map[common.Txnid]protocol.LogEntryMsg
	killch     chan bool
	mutex      sync.Mutex

	incomingReqs chan *protocol.RequestHandle
	pendingReqs  map[uint64]*protocol.RequestHandle // key : request id
	loggedReqs   map[common.Txnid]*protocol.RequestHandle
}

///////////////////////////////////////////////////////
// Public function : MetadataProvider
///////////////////////////////////////////////////////

func NewMetadataProvider(providerId string) (s *MetadataProvider, err error) {

	s = new(MetadataProvider)
	s.watchers = make(map[string]*watcher)
	s.repo = newMetadataRepo()

	s.providerId, err = s.getWatcherAddr(providerId)
	if err != nil {
		return nil, err
	}
	c.Debugf("MetadataProvider.NewMetadataProvider(): MetadataProvider follower ID %s", s.providerId)

	return s, nil
}

func (o *MetadataProvider) WatchMetadata(indexAdminPort string) {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	_, ok := o.watchers[indexAdminPort]
	if ok {
		return
	}

	o.watchers[indexAdminPort] = o.startWatcher(indexAdminPort)
}

func (o *MetadataProvider) UnwatchMetadata(indexAdminPort string) {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	watcher, ok := o.watchers[indexAdminPort]
	if !ok {
		return
	}

	delete(o.watchers, indexAdminPort)
	if watcher != nil {
		watcher.close()
	}
}

func (o *MetadataProvider) CreateIndex(
	name, bucket, using, exprType, partnExpr, whereExpr, indexAdminPort string,
	secExprs []string, isPrimary bool) (c.IndexDefnId, error) {

	defnID := c.IndexDefnId(rand.Int())

	// TODO : whereExpr
	whereExpr = whereExpr
	idxDefn := &c.IndexDefn{
		DefnId:          defnID,
		Name:            name,
		Using:           c.IndexType(using),
		Bucket:          bucket,
		IsPrimary:       isPrimary,
		SecExprs:        secExprs,
		ExprType:        c.ExprType(exprType),
		PartitionScheme: c.HASH,
		PartitionKey:    partnExpr}

	watcher, err := o.findWatcher(indexAdminPort)
	if err != nil {
		return 0, err
	}

	content, err := marshallIndexDefn(idxDefn)
	if err != nil {
		return 0, err
	}

	key := fmt.Sprintf("IndexDefinitionId/%d", defnID)
	watcher.makeRequest(common.OPCODE_SET, key, content)

	return defnID, nil
}

func (o *MetadataProvider) DropIndex(defnID c.IndexDefnId, indexAdminPort string) error {

	watcher, err := o.findWatcher(indexAdminPort)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("IndexDefinitionId/%d", defnID)
	watcher.makeRequest(common.OPCODE_DELETE, key, []byte(""))

	return nil
}

func (o *MetadataProvider) ListIndex() []*c.IndexDefn {
	o.repo.mutex.Lock()
	defer o.repo.mutex.Unlock()

	result := make([]*c.IndexDefn, 0, len(o.repo.definitions))
	for _, defn := range o.repo.definitions {
		result = append(result, defn)
	}

	return result
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

func (o *MetadataProvider) startWatcher(addr string) *watcher {

	s := newWatcher(o, addr)
	readych := make(chan bool)

	// TODO: call Close() to cleanup the state upon retry by the MetadataProvider server
	go protocol.RunWatcherServer(
		s.leaderAddr,
		s,
		s,
		s.factory,
		s.killch,
		readych)

	// TODO: timeout
	<-readych

	return s
}

func (o *MetadataProvider) findWatcher(indexAdminPort string) (*watcher, error) {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	watcher, ok := o.watchers[indexAdminPort]
	if !ok {
		return nil, errors.New(fmt.Sprintf("MetadataProvider.findWatcher() : Cannot find watcher for index admin %s", indexAdminPort))
	}

	return watcher, nil
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

///////////////////////////////////////////////////////
// private function : metadataRepo
///////////////////////////////////////////////////////

func newMetadataRepo() *metadataRepo {

	return &metadataRepo{definitions: make(map[c.IndexDefnId]*c.IndexDefn),
		instances: make(map[c.IndexDefnId]*IndexInstDistribution)}
}

func (r *metadataRepo) addDefn(defn *c.IndexDefn) {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.definitions[defn.DefnId] = defn
}

func (r *metadataRepo) removeDefn(defnId c.IndexDefnId) {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	delete(r.definitions, defnId)
	delete(r.instances, defnId)
}

func (r *metadataRepo) updateTopology(topology *IndexTopology) {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	for _, defnRef := range topology.Definitions {
		defnId := defnRef.DefnId
		for _, instRef := range defnRef.Instances {
			r.instances[c.IndexDefnId(defnId)] = &instRef
		}
	}
}

func (r *metadataRepo) unmarshallAndAddDefn(content []byte) error {

	defn, err := unmarshallIndexDefn(content)
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
	s.incomingReqs = make(chan *protocol.RequestHandle)
	s.pendingReqs = make(map[uint64]*protocol.RequestHandle)
	s.loggedReqs = make(map[common.Txnid]*protocol.RequestHandle)

	return s
}

func (w *watcher) close() {

	if len(w.killch) == 0 {
		w.killch <- true
	}
}

func (w *watcher) makeRequest(opCode common.OpCode, key string, content []byte) {

	id := uint64(time.Now().UnixNano())
	request := w.factory.CreateRequest(id, uint32(opCode), key, content)

	handle := &protocol.RequestHandle{Request: request, Err: nil}
	handle.CondVar = sync.NewCond(&handle.Mutex)

	handle.CondVar.L.Lock()
	defer handle.CondVar.L.Unlock()

	w.incomingReqs <- handle

	handle.CondVar.Wait()
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

func (w *watcher) AddPendingRequest(handle *protocol.RequestHandle) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// remember the request
	w.pendingReqs[handle.Request.GetReqId()] = handle
}

func (w *watcher) GetRequestChannel() <-chan *protocol.RequestHandle {

	return (<-chan *protocol.RequestHandle)(w.incomingReqs)
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
		c.Warnf("Watcher.commit(): unknown txnid %d.  Txn not processed at commit", txid)
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
		c.Errorf("watcher.LogAndCommit(): receive error when processing log entry from server.  Error = %v", err)
	}

	return nil
}

func (w *watcher) processChange(op uint32, key string, content []byte) error {

	opCode := common.OpCode(op)

	switch opCode {
	case common.OPCODE_ADD:
		if isIndexDefnKey(key) {
			return w.provider.repo.unmarshallAndAddDefn(content)
		} else if isIndexTopologyKey(key) {
			return w.provider.repo.unmarshallAndAddInst(content)
		}
	case common.OPCODE_SET:
		if isIndexDefnKey(key) {
			return w.provider.repo.unmarshallAndAddDefn(content)
		} else if isIndexTopologyKey(key) {
			return w.provider.repo.unmarshallAndAddInst(content)
		}
	case common.OPCODE_DELETE:
		if isIndexDefnKey(key) {

			i := strings.Index(key, "/")
			if i != -1 && i < len(key)-1 {
				id, err := strconv.ParseUint(key[i+1:], 10, 64)
				if err != nil {
					return err
				}
				w.provider.repo.removeDefn(c.IndexDefnId(id))
			} else {
				return errors.New("watcher.processChange() : cannot parse index definition id")
			}
		}
	}

	return nil
}
