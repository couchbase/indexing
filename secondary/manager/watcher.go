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
	"fmt"
	"github.com/couchbase/gometa/action"
	"github.com/couchbase/gometa/common"
	"github.com/couchbase/gometa/message"
	"github.com/couchbase/gometa/protocol"
	repo "github.com/couchbase/gometa/repository"
	"log"
	"net"
	"sync"
)

///////////////////////////////////////////////////////
// Type Definition
///////////////////////////////////////////////////////

type watcher struct {
	mgr			*IndexManager
	leaderAddr  string
	watcherAddr string
	repo        *repo.Repository
	factory     protocol.MsgFactory
	handler     *action.ServerAction
	killch      chan bool
	status      protocol.PeerStatus

	mutex           sync.Mutex
	isClosed        bool
	observePendings map[string]*observeHandle
	observeProposed map[common.Txnid]*observeHandle
	notifications	map[common.Txnid]*notificationHandle
}

type observeHandle struct {
	key         string
	txnid       common.Txnid
	w           *watcher
	mutex       sync.Mutex
	condVar     *sync.Cond
	checkForAdd bool
	done        bool
}

type notificationHandle struct {
	key				string
	content			[]byte
	evtType			EventType
}

///////////////////////////////////////////////////////
// private function : Watcher
///////////////////////////////////////////////////////

func startWatcher(mgr *IndexManager, leaderAddr string) (s *watcher, err error) {

	s = new(watcher)

	s.mgr = mgr
	s.leaderAddr = leaderAddr
	s.isClosed = false
	s.observePendings = make(map[string]*observeHandle)
	s.observeProposed = make(map[common.Txnid]*observeHandle)
	s.notifications = make(map[common.Txnid]*notificationHandle)

	s.watcherAddr, err = getWatcherAddr()
	if err != nil {
		return nil, err
	}
	log.Printf("watcher.startWatcher(): watcher follower ID %s", s.watcherAddr)

	// Initialize repository service
	s.repo, err = repo.OpenRepository()
	if err != nil {
		return nil, err
	}

	s.factory = message.NewConcreteMsgFactory()
	// TODO: Using DefaultServerAction, but need a callback on LogAndCommit
	s.handler = action.NewDefaultServerAction(s.repo, s)
	s.killch = make(chan bool, 1) // make it buffered to unblock sender
	s.status = protocol.ELECTING

	readych := make(chan bool)

	go protocol.RunWatcherServer(
		leaderAddr,
		s.handler,
		s.factory,
		s.killch,
		readych)

	// TODO: timeout
	<-readych

	return s, nil
}

func (s *watcher) Close() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if !s.isClosed {
		s.isClosed = true
		s.repo.Close()
		s.killch <- true
	}
}

func (s *watcher) Get(key string) ([]byte, error) {
	return s.handler.Get(key)
}

/////////////////////////////////////////////////////////////////////////////
// Private Function : Metadata Observe
/////////////////////////////////////////////////////////////////////////////

//
// Observe will first for existence of the local repository in the watcher.
// If the condition is satisied, then this function will just return.  Otherwise,
// this function will wait until condition arrives when dictionary notifies the
// watcher on new metadata update.   If the watcher looses contact with the dictionary
// leader,
//
func (s *watcher) addObserveForAdd(key string) *observeHandle {

	s.mutex.Lock()
	defer s.mutex.Unlock()

	value, err := s.Get(key)
	if err == nil && value != nil {
		return nil
	}

	handle := newObserveHandle(key, true, s)
	s.observePendings[key] = handle

	return handle
}

func (s *watcher) addObserveForDelete(key string) *observeHandle {

	s.mutex.Lock()
	defer s.mutex.Unlock()

	// TODO : Check for the real error for non-existence (from goforestDB)
	value, err := s.Get(key)
	if err != nil || value == nil {
		return nil
	}

	handle := newObserveHandle(key, false, s)
	s.observePendings[key] = handle

	return handle
}

func (s *watcher) removeObserve(handle *observeHandle) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	h, ok := s.observePendings[handle.key]
	if ok && h == handle {
		delete(s.observePendings, handle.key)
	}

	if handle.txnid != 0 {
		h, ok = s.observeProposed[handle.txnid]
		if ok && h == handle {
			delete(s.observeProposed, handle.txnid)
		}
	}
}

func newObserveHandle(key string, isAdd bool, w *watcher) *observeHandle {
	handle := new(observeHandle)
	handle.condVar = sync.NewCond(&handle.mutex)
	handle.key = key
	handle.txnid = 0
	handle.checkForAdd = isAdd
	handle.done = false
	handle.w = w

	return handle
}

func (o *observeHandle) wait() {
	o.condVar.L.Lock()
	defer o.condVar.L.Unlock()
	o.condVar.Wait()
}

func (o *observeHandle) signal(done bool) {
	o.w.removeObserve(o)

	o.condVar.L.Lock()
	defer o.condVar.L.Unlock()
	o.done = done
	o.condVar.Signal()
}

/////////////////////////////////////////////////////////////////////////////
// Private Function : Metadata Notification 
/////////////////////////////////////////////////////////////////////////////

func newNotificationHandle(key string, evtType EventType, content []byte) *notificationHandle {
	handle := new(notificationHandle)
	handle.key = key
	handle.evtType = evtType
	handle.content = content 

	return handle
}

/////////////////////////////////////////////////////////////////////////////
// Private Function
/////////////////////////////////////////////////////////////////////////////

func getWatcherAddr() (string, error) {

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	if len(addrs) == 0 {
		return "", fmt.Errorf("watcher.getWatcherAddr() : No network address is available")
	}

	for _, addr := range addrs {
		switch s := addr.(type) {
		case *net.IPAddr:
			if s.IP.IsGlobalUnicast() {
				return fmt.Sprintf("%s:indexer:watcher", addr.String()), nil
			}
		case *net.IPNet:
			if s.IP.IsGlobalUnicast() {
				return fmt.Sprintf("%s:indexer:watcher", addr.String()), nil
			}
		}
	}

	return "", fmt.Errorf("watcher.getWatcherAddr() : Fail to find an IP address")
}

/////////////////////////////////////////////////////////////////////////////
// ServerCallback Interface
/////////////////////////////////////////////////////////////////////////////

func (s *watcher) UpdateStateOnNewProposal(proposal protocol.ProposalMsg) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	opCode := common.OpCode(proposal.GetOpCode())
	handle, ok := s.observePendings[proposal.GetKey()]
	if ok {
		if opCode == common.OPCODE_ADD && handle.checkForAdd ||
			opCode == common.OPCODE_DELETE && !handle.checkForAdd {
			delete(s.observePendings, proposal.GetKey())
			handle.txnid = common.Txnid(proposal.GetTxnid())
			s.observeProposed[common.Txnid(proposal.GetTxnid())] = handle
		}
		// TODO : raise error if the condition does not get satisfied?
	}

	// register the event for notification	
	var evtType EventType
	switch opCode {
		case common.OPCODE_ADD : evtType = CREATE_INDEX
		case common.OPCODE_DELETE : evtType = DELETE_INDEX
	}
	s.notifications[common.Txnid(proposal.GetTxnid())] = 
		newNotificationHandle(proposal.GetKey(), evtType, proposal.GetContent())
}

func (s *watcher) UpdateStateOnCommit(txnid common.Txnid, key string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	handle, ok := s.observeProposed[txnid]
	if ok && handle != nil {
		handle.signal(true)
	}
	
	notification, ok := s.notifications[txnid]
	if ok && handle != nil {
		s.mgr.notify(notification.evtType, notification.content)
	}
}

func (s *watcher) GetStatus() protocol.PeerStatus {
	return s.status
}

func (s *watcher) UpdateWinningEpoch(epoch uint32) {
}

func (s *watcher) GetEnsembleSize() uint64 {
	return 1 // just myself -- only used for leader election
}

func (s *watcher) GetFollowerId() string {
	return s.watcherAddr
}

/////////////////////////////////////////////////////////////////////////////
// QuorumVerifier
/////////////////////////////////////////////////////////////////////////////

func (s *watcher) HasQuorum(count int) bool {
	ensembleSz := s.handler.GetEnsembleSize() - 1
	return count > int(ensembleSz/2)
}
