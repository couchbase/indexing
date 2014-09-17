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
	common "github.com/couchbase/gometa/common"
	message "github.com/couchbase/gometa/message"
	protocol "github.com/couchbase/gometa/protocol"
	"log"
	"sync"
	"time"
)

/////////////////////////////////////////////////////////////////////////////
// Type Declaration
/////////////////////////////////////////////////////////////////////////////

const (
	OPCODE_ADD_IDX_DEFN common.OpCode = iota
	OPCODE_DEL_IDX_DEFN
)

type Coordinator struct {
	state    *CoordinatorState
	repo     *MetadataRepo
	env      *env
	site     *protocol.ElectionSite
	listener *common.PeerListener
	factory  protocol.MsgFactory
	skillch  chan bool

	mutex sync.Mutex
	cond  *sync.Cond
	ready bool
}

type CoordinatorState struct {
	incomings chan *protocol.RequestHandle

	// mutex protected variables
	mutex     sync.Mutex
	done      bool
	status    protocol.PeerStatus
	pendings  map[uint64]*protocol.RequestHandle       // key : request id
	proposals map[common.Txnid]*protocol.RequestHandle // key : txnid
	observes  map[string]*observeHandle                // key : metadata key
}

// TODO: CurrentEpoch needs to be persisted. For testing now, use a transient variable.
var gEpoch uint32 = common.BOOTSTRAP_CURRENT_EPOCH

/////////////////////////////////////////////////////////////////////////////
// Public API
/////////////////////////////////////////////////////////////////////////////

func NewCoordinator(repo *MetadataRepo) *Coordinator {
	coordinator := new(Coordinator)
	coordinator.repo = repo
	coordinator.ready = false
	coordinator.cond = sync.NewCond(&coordinator.mutex)
	return coordinator
}

//
// Run Coordinator
//
func (s *Coordinator) Run(config string) {

	repeat := true
	for repeat {
		pauseTime := s.runOnce(config)
		if !s.IsDone() {
			if pauseTime > 0 {
				// wait before restart
				time.Sleep(time.Duration(pauseTime) * time.Millisecond)
			}
		} else {
			repeat = false
		}
	}
}

//
// Terminate the Coordinator
//
func (s *Coordinator) Terminate() {

	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in MetadataRepo.Close() : %s.  Ignored.\n", r)
		}
	}()

	s.state.mutex.Lock()
	defer s.state.mutex.Unlock()

	if s.state.done {
		return
	}

	s.state.done = true

	s.site.Close()
	s.site = nil

	s.skillch <- true // kill leader/follower server
}

//
// Check if server is terminated
//
func (s *Coordinator) IsDone() bool {

	s.state.mutex.Lock()
	defer s.state.mutex.Unlock()

	return s.state.done
}

//
// Handle a new request.  This function will block until the request is being processed
// (by returning true) or until the request is being interrupted (by returning false).
// If request is interrupted, then the request may still be processed by some other
// nodes.  So the outcome of the request is unknown when this function returns false.
//
func (s *Coordinator) NewRequest(id uint64, opCode uint32, key string, content []byte) bool {

	s.waitForReady()

	req := s.factory.CreateRequest(id, opCode, key, content)

	handle := &protocol.RequestHandle{Request: req, Err: nil}
	handle.CondVar = sync.NewCond(&handle.Mutex)

	handle.CondVar.L.Lock()
	defer handle.CondVar.L.Unlock()

	s.state.incomings <- handle

	handle.CondVar.Wait()

	return handle.Err == nil
}

/////////////////////////////////////////////////////////////////////////////
// Main Control Loop
/////////////////////////////////////////////////////////////////////////////

//
// Run the server until it stop.  Will not attempt to re-run.
//
func (c *Coordinator) runOnce(config string) int {

	log.Printf("Coordinator.runOnce() : Start Running Coordinator")

	pauseTime := 0

	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in Coordinator.runOnce() : %s\n", r)
		}

		common.SafeRun("Coordinator.cleanupState()",
			func() {
				c.cleanupState()
			})
	}()

	err := c.bootstrap(config)
	if err != nil {
		pauseTime = 200
	}

	// Check if the server has been terminated explicitly. If so, don't run.
	if !c.IsDone() {

		// runElection() finishes if there is an error, election result is known or
		// it being terminated. Unless being killed explicitly, a goroutine
		// will continue to run to responds to other peer election request
		leader, err := c.runElection()
		if err != nil {
			log.Printf("Coordinator.runOnce() : Error Encountered During Election : %s", err.Error())
			pauseTime = 100
		} else {

			// Check if the server has been terminated explicitly. If so, don't run.
			if !c.IsDone() {
				// runCoordinator() is done if there is an error	or being terminated explicitly (killch)
				err := c.runProtocol(leader)
				if err != nil {
					log.Printf("Coordinator.RunOnce() : Error Encountered From Coordinator : %s", err.Error())
				}
			}
		}
	} else {
		log.Printf("Coordinator.RunOnce(): Coordinator has been terminated explicitly. Terminate.")
	}

	return pauseTime
}

/////////////////////////////////////////////////////////////////////////////
// Bootstrap and cleanup
/////////////////////////////////////////////////////////////////////////////

//
// Bootstrp
//
func (s *Coordinator) bootstrap(config string) (err error) {

	s.env, err = newEnv(config)
	if err != nil {
		return err
	}

	// Initialize server state
	s.state = newCoordinatorState()

	// Initialize various callback facility for leader election and
	// voting protocol.
	s.factory = message.NewConcreteMsgFactory()
	s.skillch = make(chan bool, 1) // make it buffered to unblock sender
	s.site = nil

	// Need to start the peer listener before election. A follower may
	// finish its election before a leader finishes its election. Therefore,
	// a follower node can request a connection to the leader node before that
	// node knows it is a leader.  By starting the listener now, it allows the
	// follower to establish the connection and let the leader handles this
	// connection at a later time (when it is ready to be a leader).
	s.listener, err = common.StartPeerListener(s.getHostTCPAddr())
	if err != nil {
		return fmt.Errorf("Index Coordinator : Fail to start PeerListener: %s", err)
	}

	// tell boostrap is ready
	s.markReady()

	return nil
}

//
// Cleanup internal state upon exit
//
func (s *Coordinator) cleanupState() {

	// tell that coordinator is no longer ready
	s.markNotReady()

	s.state.mutex.Lock()
	defer s.state.mutex.Unlock()

	common.SafeRun("Coordinator.cleanupState()",
		func() {
			if s.listener != nil {
				s.listener.Close()
			}
		})

	common.SafeRun("Coordinator.cleanupState()",
		func() {
			if s.site != nil {
				s.site.Close()
			}
		})

	for len(s.state.incomings) > 0 {
		request := <-s.state.incomings
		request.Err = fmt.Errorf("Terminate Request due to server termination")

		common.SafeRun("Coordinator.cleanupState()",
			func() {
				request.CondVar.L.Lock()
				defer request.CondVar.L.Unlock()
				request.CondVar.Signal()
			})
	}

	for _, request := range s.state.pendings {
		request.Err = fmt.Errorf("Terminate Request due to server termination")

		common.SafeRun("Coordinator.cleanupState()",
			func() {
				request.CondVar.L.Lock()
				defer request.CondVar.L.Unlock()
				request.CondVar.Signal()
			})
	}

	for _, request := range s.state.proposals {
		request.Err = fmt.Errorf("Terminate Request due to server termination")

		common.SafeRun("Coordinator.cleanupState()",
			func() {
				request.CondVar.L.Lock()
				defer request.CondVar.L.Unlock()
				request.CondVar.Signal()
			})
	}

	for _, observe := range s.state.observes {
		common.SafeRun("Coordinator.cleanupState()",
			func() {
				observe.signal(false)
			})
	}
}

func (s *Coordinator) markReady() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.ready = true
	s.cond.Signal()
}

func (s *Coordinator) markNotReady() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.ready = false
}

func (s *Coordinator) isReady() bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.ready
}

func (s *Coordinator) waitForReady() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for !s.ready {
		s.cond.Wait()
	}
}

/////////////////////////////////////////////////////////////////////////////
//  Election
/////////////////////////////////////////////////////////////////////////////

//
// run election
//
func (s *Coordinator) runElection() (leader string, err error) {

	host := s.getHostUDPAddr()
	peers := s.getPeerUDPAddr()

	// Create an election site to start leader election.
	log.Printf("Coordinator.runElection(): Local Coordinator %s start election", host)
	log.Printf("Coordinator.runElection(): Peer in election")
	for _, peer := range peers {
		log.Printf("	peer : %s", peer)
	}

	s.site, err = protocol.CreateElectionSite(host, peers, s.factory, s, false)
	if err != nil {
		return "", err
	}

	// blocked until leader is elected. coordinator.Terminate() will unblock this.
	resultCh := s.site.StartElection()
	leader, ok := <-resultCh
	if !ok {
		return "", fmt.Errorf("Index Coordinator Election Fails")
	}

	return leader, nil
}

/////////////////////////////////////////////////////////////////////////////
//  Run Leader/Follower protocol
/////////////////////////////////////////////////////////////////////////////

//
// run server (as leader or follower)
//
func (s *Coordinator) runProtocol(leader string) (err error) {

	host := s.getHostUDPAddr()

	// If this host is the leader, then start the leader server.
	// Otherwise, start the followerCoordinator.
	if leader == host {
		log.Printf("Coordinator.runServer() : Local Coordinator %s is elected as leader. Leading ...", leader)
		s.state.setStatus(protocol.LEADING)
		err = protocol.RunLeaderServer(s.getHostTCPAddr(), s.listener, s, s, s.factory, s.skillch)
	} else {
		log.Printf("Coordinator.runServer() : Remote Coordinator %s is elected as leader. Following ...", leader)
		s.state.setStatus(protocol.FOLLOWING)
		leaderAddr := s.findMatchingPeerTCPAddr(leader)
		if len(leaderAddr) == 0 {
			return fmt.Errorf("Index Coordinator cannot find matching TCP addr for leader " + leader)
		}
		err = protocol.RunFollowerServer(s.getHostTCPAddr(), leaderAddr, s, s, s.factory, s.skillch)
	}

	return err
}

/////////////////////////////////////////////////////////////////////////////
// CoordinatorState
/////////////////////////////////////////////////////////////////////////////

//
// Create a new CoordinatorState
//
func newCoordinatorState() *CoordinatorState {

	incomings := make(chan *protocol.RequestHandle, common.MAX_PROPOSALS)
	pendings := make(map[uint64]*protocol.RequestHandle)
	proposals := make(map[common.Txnid]*protocol.RequestHandle)
	state := &CoordinatorState{
		incomings: incomings,
		pendings:  pendings,
		proposals: proposals,
		status:    protocol.ELECTING,
		done:      false}

	return state
}

func (s *CoordinatorState) getStatus() protocol.PeerStatus {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.status
}

func (s *CoordinatorState) setStatus(status protocol.PeerStatus) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.status = status
}

/////////////////////////////////////////////////////////////////////////////
//  Coordinator Action (Callback)
/////////////////////////////////////////////////////////////////////////////

func (c *Coordinator) GetEnsembleSize() uint64 {
	return uint64(len(c.getPeerUDPAddr())) + 1 // including myself
}

func (c *Coordinator) GetLastLoggedTxid() (common.Txnid, error) {
	return common.BOOTSTRAP_LAST_LOGGED_TXID, nil
}

func (c *Coordinator) GetLastCommittedTxid() (common.Txnid, error) {
	return common.BOOTSTRAP_LAST_COMMITTED_TXID, nil
}

func (c *Coordinator) GetStatus() protocol.PeerStatus {
	return c.state.getStatus()
}

func (c *Coordinator) GetCurrentEpoch() (uint32, error) {
	// TODO
	return gEpoch, nil
}

func (c *Coordinator) GetAcceptedEpoch() (uint32, error) {
	return c.GetCurrentEpoch()
}

func (c *Coordinator) GetCommitedEntries(txid1, txid2 common.Txnid) (<-chan protocol.LogEntryMsg, <-chan error, chan<- bool, error) {
	// TODO
	return nil, nil, nil, nil
}

func (c *Coordinator) LogAndCommit(txid common.Txnid, op uint32, key string, content []byte, toCommit bool) error {
	// Nothing to do : Coorindator does not have local repository
	return nil
}

func (c *Coordinator) NotifyNewAcceptedEpoch(uint32) error {
	// Nothing to do
	return nil
}

func (c *Coordinator) NotifyNewCurrentEpoch(epoch uint32) error {
	// TODO
	gEpoch = epoch

	return nil
}

func (c *Coordinator) GetFollowerId() string {
	return c.getHostUDPAddr()
}

// TODO : what to do if createIndex returns error
func (c *Coordinator) LogProposal(proposal protocol.ProposalMsg) error {

	if c.GetStatus() == protocol.LEADING {
		switch common.OpCode(proposal.GetOpCode()) {
		case OPCODE_ADD_IDX_DEFN:
			success := c.createIndex(proposal.GetKey(), proposal.GetContent())
			log.Printf("success", success)
		case OPCODE_DEL_IDX_DEFN:
			success := c.deleteIndex(proposal.GetKey())
			log.Printf("success", success)
		}
	} /* else if c.GetStatus() == protocol.FOLLOWING {
		switch common.OpCode(proposal.GetOpCode()) {
			case OPCODE_ADD_IDX_DEFN:
				c.observeForAdd(proposal.GetKey())
			case OPCODE_DEL_IDX_DEFN:
				c.observeForDelete(proposal.GetKey())
		}
	} */

	c.updateRequestOnNewProposal(proposal)

	return nil
}

func (c *Coordinator) Commit(txid common.Txnid) error {
	c.updateRequestOnCommit(txid)
	return nil
}

func (c *Coordinator) GetQuorumVerifier() protocol.QuorumVerifier {
	return c
}

//  TODO : Quorum should be based on active participants
func (c *Coordinator) HasQuorum(count int) bool {
	ensembleSz := c.GetEnsembleSize()
	return count > int(ensembleSz/2)
}

/////////////////////////////////////////////////////////////////////////////
//  Request Handling
/////////////////////////////////////////////////////////////////////////////

// Return a channel of request for the leader to process on.
func (c *Coordinator) GetRequestChannel() <-chan *protocol.RequestHandle {
	return (<-chan *protocol.RequestHandle)(c.state.incomings)
}

// This is called when the leader has de-queued the request for processing.
func (c *Coordinator) AddPendingRequest(handle *protocol.RequestHandle) {
	c.state.mutex.Lock()
	defer c.state.mutex.Unlock()

	c.state.pendings[handle.Request.GetReqId()] = handle
}

//
// Update the request upon new proposal.
//
func (c *Coordinator) updateRequestOnNewProposal(proposal protocol.ProposalMsg) {

	fid := proposal.GetFid()
	reqId := proposal.GetReqId()
	txnid := proposal.GetTxnid()

	// If this host is the one that sends the request to the leader
	if fid == c.GetFollowerId() {
		c.state.mutex.Lock()
		defer c.state.mutex.Unlock()

		// look up the request handle from the pending list and
		// move it to the proposed list
		handle, ok := c.state.pendings[reqId]
		if ok {
			delete(c.state.pendings, reqId)
			c.state.proposals[common.Txnid(txnid)] = handle
		}
	}
}

//
// Update the request upon new commit
//
func (c *Coordinator) updateRequestOnCommit(txnid common.Txnid) {

	c.state.mutex.Lock()
	defer c.state.mutex.Unlock()

	// If I can find the proposal based on the txnid in this host, this means
	// that this host originates the request.   Get the request handle and
	// notify the waiting goroutine that the request is done.
	handle, ok := c.state.proposals[txnid]

	if ok {
		delete(c.state.proposals, txnid)

		handle.CondVar.L.Lock()
		defer handle.CondVar.L.Unlock()
		handle.CondVar.Signal()
	}
}

/////////////////////////////////////////////////////////////////////////////
//  Environment
/////////////////////////////////////////////////////////////////////////////

func (c *Coordinator) getHostUDPAddr() string {
	return c.env.getHostUDPAddr()
}

func (c *Coordinator) getHostTCPAddr() string {
	return c.env.getHostTCPAddr()
}

func (c *Coordinator) findMatchingPeerTCPAddr(udpAddr string) string {
	return c.env.findMatchingPeerTCPAddr(udpAddr)
}

func (c *Coordinator) getPeerUDPAddr() []string {
	return c.env.getPeerUDPAddr()
}

/////////////////////////////////////////////////////////////////////////////
//  Operations
/////////////////////////////////////////////////////////////////////////////

//
// Handle create Index request in the dictionary.  If this function
// returns true, it means createIndex request completes successfully.
// If this function returns false, then the result is unknown.  The
// request may still being completed (by some other nodes).
//
func (c *Coordinator) createIndex(key string, content []byte) bool {

	defn, err := unmarshallIndexDefn(content)
	if err != nil {
		return false
	}

	if err = c.repo.CreateIndex(defn); err != nil {
		return false
	}

	return true
}

//
// Handle delete Index request in the dictionary.  If this function
// returns true, it means deleteIndex request completes successfully.
// If this function returns false, then the result is unknown.  The
// request may still being completed (by some other nodes).
//
func (c *Coordinator) deleteIndex(key string) bool {

	if err := c.repo.DropIndexByName(key); err != nil {
		return false
	}
	return true
}

//
//
//
func (c *Coordinator) observeForAdd(key string) bool {

	handle, ok := c.state.observes[key]
	if !ok {
		handle = c.repo.ObserveForAdd(key)
		if handle != nil {
			c.state.observes[key] = handle
		}
	}

	if handle != nil {
		handle.wait()
		return handle.done
	}

	return true
}

func (c *Coordinator) observeForDelete(key string) bool {

	handle, ok := c.state.observes[key]
	if !ok {
		handle := c.repo.ObserveForDelete(key)
		if handle != nil {
			c.state.observes[key] = handle
		}
	}

	if handle != nil {
		handle.wait()
		return handle.done
	}

	return true
}
