// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package manager

import (
	"sync"

	"github.com/couchbase/gometa/action"
	"github.com/couchbase/gometa/common"
	"github.com/couchbase/gometa/message"
	"github.com/couchbase/gometa/protocol"
	repo "github.com/couchbase/gometa/repository"
	"github.com/couchbase/indexing/secondary/logging"
)

///////////////////////////////////////////////////////
// Type Definition
///////////////////////////////////////////////////////

type watcher struct {
	mgr         *IndexManager
	leaderAddr  string
	watcherAddr string
	txn         *common.TxnState
	repo        repo.IRepository
	factory     protocol.MsgFactory
	handler     *action.ServerAction
	killch      chan bool
	status      protocol.PeerStatus

	mutex         sync.Mutex
	isClosed      bool
	observes      map[string]*observeHandle
	notifications map[common.Txnid]*notificationHandle
}

type observeHandle struct {
	key         string
	w           *watcher
	mutex       sync.Mutex
	condVar     *sync.Cond
	checkForAdd bool
	done        bool
}

type notificationHandle struct {
	key     string
	content []byte
	evtType EventType
}

///////////////////////////////////////////////////////
// private function : Watcher
///////////////////////////////////////////////////////

func startWatcher(mgr *IndexManager,
	repo repo.IRepository,
	leaderAddr string,
	watcherId string) (s *watcher, err error) {

	s = new(watcher)

	s.mgr = mgr
	s.leaderAddr = leaderAddr
	s.repo = repo
	s.isClosed = false
	s.observes = make(map[string]*observeHandle)
	s.notifications = make(map[common.Txnid]*notificationHandle)

	s.watcherAddr = watcherId
	if err != nil {
		return nil, err
	}
	logging.Debugf("watcher.startWatcher(): watcher follower ID %s", s.watcherAddr)

	s.txn = common.NewTxnState()
	s.factory = message.NewConcreteMsgFactory()
	// TODO: Using DefaultServerAction, but need a callback on LogAndCommit
	s.handler = action.NewDefaultServerAction(s.repo, s, s.txn)
	s.killch = make(chan bool, 1) // make it buffered to unblock sender
	s.status = protocol.ELECTING

	readych := make(chan bool)

	// TODO: call Close() to cleanup the state upon retry by the watcher server
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
		s.killch <- true
	}

	for _, handle := range s.observes {
		handle.signal(false)
	}
}

func (s *watcher) Get(key string) ([]byte, error) {
	return s.handler.Get(key)
}

func (s *watcher) Set(key string, content []byte) error {
	return s.handler.Set(key, content)
}

/////////////////////////////////////////////////////////////////////////////
// Private Function : Metadata Observe
/////////////////////////////////////////////////////////////////////////////

// Observe will first for existence of the local repository in the watcher.
// If the condition is satisied, then this function will just return.  Otherwise,
// this function will wait until condition arrives when dictionary notifies the
// watcher on new metadata update.
// TODO: Support timeout
func (s *watcher) observeForAdd(key string) {

	handle := func() *observeHandle {
		s.mutex.Lock()
		defer s.mutex.Unlock()

		// This function now has the mutex.  Check if the key if it already exists.
		value, err := s.Get(key)
		if err == nil && value != nil {
			return nil
		}

		// Create a handle to observe the key when it is committed.  Note that the watcher
		// will need to acquire the mutex for processing the commit.  Therefore, we don't
		// have to worry about race condition.
		handle, ok := s.observes[key]
		if !ok {
			handle = newObserveHandle(key)
			s.observes[key] = handle
		}

		return handle
	}()

	if handle != nil {
		// wait to get notified
		handle.wait()

		// Double check if the key exist
		value, err := s.Get(key)
		if err == nil && value != nil {
			return
		}

		// If key still does not exist, then continue to wait
		s.observeForAdd(key)
	}
}

func (s *watcher) observeForDelete(key string) {

	handle := func() *observeHandle {
		s.mutex.Lock()
		defer s.mutex.Unlock()

		// TODO : Check for the real error for non-existence (from goforestDB)
		value, err := s.Get(key)
		if err != nil || value == nil {
			return nil
		}

		// Create a handle to observe the key when it is committed.  Note that the watcher
		// will need to acquire the mutex for processing the commit.  Therefore, we don't
		// have to worry about race condition.
		handle, ok := s.observes[key]
		if !ok {
			handle = newObserveHandle(key)
			s.observes[key] = handle
		}

		return handle
	}()

	if handle != nil {
		handle.wait()

		// Double check if the key exist
		value, err := s.Get(key)
		if err != nil || value == nil {
			return
		}

		// If key still exist, then continue to wait
		s.observeForDelete(key)
	}
}

func (s *watcher) removeObserve(handle *observeHandle) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	h, ok := s.observes[handle.key]
	if ok && h == handle {
		delete(s.observes, handle.key)
	}
}

func newObserveHandle(key string) *observeHandle {
	handle := new(observeHandle)
	handle.condVar = sync.NewCond(&handle.mutex)
	handle.key = key
	handle.done = false

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
	o.condVar.Broadcast()
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
// ServerCallback Interface
/////////////////////////////////////////////////////////////////////////////

func (s *watcher) UpdateStateOnNewProposal(proposal protocol.ProposalMsg) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	opCode := common.OpCode(proposal.GetOpCode())
	logging.Debugf("Watcher.UpdateStateOnNewProposal(): receive proposal on metadata kind %d", findTypeFromKey(proposal.GetKey()))

	// register the event for notification
	var evtType EventType = EVENT_NONE
	switch opCode {
	case common.OPCODE_ADD:
		metaType := findTypeFromKey(proposal.GetKey())
		if metaType == KIND_INDEX_DEFN {
			evtType = EVENT_CREATE_INDEX
		} else if metaType == KIND_TOPOLOGY {
			evtType = EVENT_UPDATE_TOPOLOGY
		}
	case common.OPCODE_SET:
		metaType := findTypeFromKey(proposal.GetKey())
		if metaType == KIND_INDEX_DEFN {
			evtType = EVENT_CREATE_INDEX
		} else if metaType == KIND_TOPOLOGY {
			evtType = EVENT_UPDATE_TOPOLOGY
		}
	case common.OPCODE_DELETE:
		if findTypeFromKey(proposal.GetKey()) == KIND_INDEX_DEFN {
			evtType = EVENT_DROP_INDEX
		}
	default:
		logging.Debugf("Watcher.UpdateStateOnNewProposal(): recieve proposal with opcode %d.  Skip convert proposal to event.", opCode)
	}

	logging.Debugf("Watcher.UpdateStateOnNewProposal(): convert metadata type to event  %d", evtType)
	if evtType != EVENT_NONE {
		logging.Debugf("Watcher.UpdateStateOnNewProposal(): register event for txid %d", proposal.GetTxnid())
		s.notifications[common.Txnid(proposal.GetTxnid())] =
			newNotificationHandle(proposal.GetKey(), evtType, proposal.GetContent())
	}
}

func (s *watcher) UpdateStateOnCommit(txnid common.Txnid, key string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	handle, ok := s.observes[key]
	if ok && handle != nil {
		// Signal will remove observeHandle from watcher
		handle.signal(true)
	}

	notification, ok := s.notifications[txnid]
	if ok && notification != nil && s.mgr != nil {
		logging.Debugf("Watcher.UpdateStateOnCommit(): notify event for txid %d", txnid)
		s.mgr.notify(notification.evtType, notification.content)
		delete(s.notifications, txnid)
	}
}

func (s *watcher) UpdateStateOnRespond(fid string, reqId uint64, err string, content []byte) {
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
