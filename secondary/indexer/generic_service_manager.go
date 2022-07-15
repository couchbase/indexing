// @copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

import (
	"fmt"
	"net/http"
	"strconv"
	"sync"

	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
)

////////////////////////////////////////////////////////////////////////////////////////////////////
// GenericServiceManager class
////////////////////////////////////////////////////////////////////////////////////////////////////

// genericMgr points to the singleton of this class
var genericMgr *GenericServiceManager

// GenericServiceManager provides the implementation of the generic subset of the ns_server RPC
// Manager interface (defined in cbauth/service/interface.go). These have been moved here from
// rebalance_service_manager.go where they originated when the Manager iface contained generic and
// Rebalance APIs only. Now that iface contains generic, Rebalance, and Pause-Resume, and these
// latter two share GetTaskList and CancelTask, so it would get messy to try to put all that into
// RebalanceServiceManager.
type GenericServiceManager struct {
	nodeInfo *service.NodeInfo        // never changes; info about the local node
	pauseMgr *PauseServiceManager     // Pause-Resume Manager singleton from Indexer
	rebalMgr *RebalanceServiceManager // Rebalance Manager singleton from Indexer

	// waiters is a set of notification channels waiting to be notified that rev has incremented
	waiters genericWaiters

	// waitersMu is the mutex protecting waiters
	waitersMu sync.Mutex

	// rev is the canonical task list revision number for ns_server GetTaskList long polls. This
	// gets converted to and from service.Revision. Both pauseMgr and rebalMgr children can generate
	// new revisions, so rev is now owned by GenericServiceManager.
	rev uint64

	// revMu is the mutex protecting rev
	revMu sync.RWMutex
}

// NewGenericServiceManager is the constructor for the GenericServiceManager class. It needs all the
// args to be passed to NewPauseServiceManager and NewRebalanceServiceManager.
func NewGenericServiceManager(mux *http.ServeMux, httpAddr string, rebalSupvCmdch MsgChannel,
	rebalSupvMsgch MsgChannel, rebalSupvPrioMsgch MsgChannel, config common.Config, nodeInfo *service.NodeInfo, rebalanceRunning bool,
	rebalanceToken *RebalanceToken, statsMgr *statsManager) (
	*GenericServiceManager, *PauseServiceManager, *RebalanceServiceManager) {

	m := &GenericServiceManager{
		nodeInfo: nodeInfo,
		waiters:  make(genericWaiters),
	}
	pauseMgr := NewPauseServiceManager(mux, m, httpAddr)
	m.pauseMgr = pauseMgr

	rebalMgr := NewRebalanceServiceManager(m, httpAddr, rebalSupvCmdch, rebalSupvMsgch,
		rebalSupvPrioMsgch, config, nodeInfo, rebalanceRunning, rebalanceToken, statsMgr)
	m.rebalMgr = rebalMgr

	// Save the singleton
	genericMgr = m

	// Unit test REST APIs -- THESE MUST STILL DO AUTHENTICATION!!
	mux.HandleFunc("/test/CancelTask", genericMgr.testCancelTask)
	mux.HandleFunc("/test/GetTaskList", genericMgr.testGetTaskList)

	return m, pauseMgr, rebalMgr
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Type definitions
////////////////////////////////////////////////////////////////////////////////////////////////////

// TaskResponse is the REST return payload of testCancelTask, testPause, and testResume, as they
// only need generic fields.
type TaskResponse struct {
	Code   string `json:"code,omitempty"`
	Error  string `json:"error,omitempty"`
	TaskId string `json:"taskId,omitempty"`
}

// GetTaskListResponse is the REST return payload of testGetTaskList.
type GetTaskListResponse struct {
	Code     string            `json:"code,omitempty"`
	Error    string            `json:"error,omitempty"`
	TaskList *service.TaskList `json:"taskList,omitempty"`
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//
//  Implementation of generic APIs of the service.Manager interface
//  defined in cbauth/service/interface.go.
//
////////////////////////////////////////////////////////////////////////////////////////////////////

// GetNodeInfo returns never-changing info about this node.
func (m *GenericServiceManager) GetNodeInfo() (*service.NodeInfo, error) {
	return m.nodeInfo, nil
}

// Shutdown is a NO-OP.
func (m *GenericServiceManager) Shutdown() error {
	return nil
}

// GetTaskList gets the list of ns_server-assigned task(s) currently running by delegating to area-
// specific managers that might be running tasks. If rev is non-nil, respond only when the revision
// differs from that, else respond immediately. The cancel arg is a channel ns_server may use to
// cancel the call.
func (m *GenericServiceManager) GetTaskList(rev service.Revision,
	cancel service.Cancel) (*service.TaskList, error) {
	const _GetTaskList = "GenericServiceManager::GetTaskList:"
	logging.Infof("%v called with rev: %v", _GetTaskList, rev)

	// Wait for new revision if needed
	err := m.waitForNewRev(rev, cancel)
	if err != nil {
		// Log as Info because it is not really an error, just service.ErrCanceled
		logging.Infof("%v return from rev %v call. err: %v", _GetTaskList, rev, err)
		return nil, err
	}

	// Assemble the full task list from the child delegates
	taskList := m.rebalMgr.RebalGetTaskList()
	taskList.Tasks = append(taskList.Tasks, m.pauseMgr.PauseGetTaskList()...)
	taskList.Rev = EncodeRev(m.getRev()) // overwrite the rev from RebalGetTaskList

	logging.Infof("%v return from rev %v call. taskList: %+v", _GetTaskList, rev, taskList)
	return taskList, nil
}

// CancelTask cancels an existing task by delegating to children that might be running it. All tasks
// except the Rebalance task have rev == 0 so rev is irrelevant for PauseCancelTask.
func (m *GenericServiceManager) CancelTask(id string, rev service.Revision) error {
	const _CancelTask = "GenericServiceManager::CancelTask:"
	logging.Infof("%v called with id: %v, rev: %v", _CancelTask, id, rev)

	// Delegate to Rebalance
	err := m.rebalMgr.RebalCancelTask(id, rev)
	if err != nil && err != service.ErrNotFound { // task was found but cancel failed
		logging.Infof("%v return from rev %v call. RebalCancelTask err: %v", _CancelTask,
			rev, err)
		return err
	}

	// Task not found in Rebalance so delegate to Pause-Resume
	err = m.pauseMgr.PauseCancelTask(id)
	if err != nil && err != service.ErrNotFound { // task was found but cancel failed
		logging.Infof("%v return from rev %v call. PauseCancelTask err: %v", _CancelTask,
			rev, err)
		return err
	}

	// Here err is either nil or service.ErrNotFound
	logging.Infof("%v return from id: %v, rev: %v call. err: %v", _CancelTask, id, rev, err)
	return err
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Handlers for unit test REST APIs (/test/methodName) -- MUST STILL DO AUTHENTICATION!!
////////////////////////////////////////////////////////////////////////////////////////////////////

// testCancelTask handles unit test REST API "/test/CancelTask" by calling
// generic_service_manager.go CancelTask, which is normally called by ns_server via cbauth RPC.
func (m *GenericServiceManager) testCancelTask(w http.ResponseWriter, r *http.Request) {
	const _testCancelTask = "GenericServiceManager::testCancelTask:"

	logging.Infof("%v called", _testCancelTask)
	defer logging.Infof("%v returned", _testCancelTask)

	// Authenticate
	_, ok := doAuth(r, w, _testCancelTask)
	if !ok {
		return
	}

	// Required parameters
	id := r.FormValue("id")
	revString := r.FormValue("rev") // can include prefix indicating base
	rev := revStringToServiceRevision(revString)

	err := m.CancelTask(id, rev)
	if err == nil {
		resp := &TaskResponse{Code: RESP_SUCCESS, TaskId: id}
		rhSend(http.StatusOK, w, resp)
		return
	}
	err = fmt.Errorf("%v CancelTask RPC returned err: %v", _testCancelTask, err)
	resp := &TaskResponse{Code: RESP_ERROR, Error: err.Error()}
	rhSend(http.StatusInternalServerError, w, resp)
}

// testGetTaskList handles unit test REST API "/test/GetTaskList" by calling
// generic_service_manager.go GetTaskList, which is normally called by ns_server via cbauth RPC.
func (m *GenericServiceManager) testGetTaskList(w http.ResponseWriter, r *http.Request) {
	const _testGetTaskList = "GenericServiceManager::testGetTaskList:"

	logging.Infof("%v called", _testGetTaskList)
	defer logging.Infof("%v returned", _testGetTaskList)

	// Authenticate
	_, ok := doAuth(r, w, _testGetTaskList)
	if !ok {
		return
	}

	// Call GetTaskList with nils to force immediate return of current task list.
	taskList, err := m.GetTaskList(nil, nil)
	if err == nil {
		resp := &GetTaskListResponse{Code: RESP_SUCCESS, TaskList: taskList}
		rhSend(http.StatusOK, w, resp)
		return
	}
	err = fmt.Errorf("%v GetTaskList RPC returned err: %v", _testGetTaskList, err)
	resp := &GetTaskListResponse{Code: RESP_ERROR, Error: err.Error()}
	rhSend(http.StatusInternalServerError, w, resp)
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// General functions and methods
////////////////////////////////////////////////////////////////////////////////////////////////////

// getRev returns the current rev value.
func (m *GenericServiceManager) getRev() uint64 {
	m.revMu.RLock()
	defer m.revMu.RUnlock()
	return m.rev
}

// incRev atomically increments the rev member, notifies all waiters, and returns the new rev value.
func (m *GenericServiceManager) incRev() uint64 {
	m.revMu.Lock()
	m.rev++
	rev := m.rev
	m.revMu.Unlock()

	m.notifyGenericWaiters()
	return rev
}

// genericWaiter is a notification channel for one rev increment waiter.
type genericWaiter chan struct{}

// genericWaiters is a set of notification channels of all rev increment waiters.
type genericWaiters map[genericWaiter]struct{}

// addGenericWaiter creates and returns a notification channel to m.waiters that will be signaled
// when m.rev is incremented.
func (m *GenericServiceManager) addGenericWaiter() genericWaiter {
	ch := make(genericWaiter, 1)

	m.waitersMu.Lock()
	m.waiters[ch] = struct{}{}
	m.waitersMu.Unlock()

	return ch
}

// notifyGenericWaiters sends an empty signal down all channels in the m.waiters set to notify the
// waiters that m.rev has been incremented, then replaces m.waiters with a new empty set, enabling
// the old notification channels to be garbage collected.
func (m *GenericServiceManager) notifyGenericWaiters() {
	m.waitersMu.Lock()
	for ch := range m.waiters {
		if ch != nil {
			ch <- struct{}{}
		}
	}
	m.waiters = make(genericWaiters) // reinit to the empty set
	m.waitersMu.Unlock()
}

// removeGenericWaiter removes a notification channel from m.waiters due to a request cancellation.
func (m *GenericServiceManager) removeGenericWaiter(w genericWaiter) {
	m.waitersMu.Lock()
	delete(m.waiters, w)
	m.waitersMu.Unlock()
}

// waitForNewRev supports a long poll that does not return until canceled or a new revision occurs,
// as well as a request for immediate reply.
//   o Returns immediately if
//     a. rev == nil: caller wants immediate reply
//     b. rev != m.rev: caller does not yet have current rev
//   o Otherwise waits until m.rev increments or caller cancels the request
func (m *GenericServiceManager) waitForNewRev(callerRev service.Revision,
	cancel service.Cancel) error {

	if callerRev == nil {
		return nil
	}
	callerRevUint64 := DecodeRev(callerRev)
	if callerRevUint64 != m.getRev() {
		return nil
	}

	// Caller has current revision so wait for the next one before returning
	ch := m.addGenericWaiter()
	select {
	case <-cancel:
		m.removeGenericWaiter(ch)
		return service.ErrCanceled
	case <-ch:
		return nil
	}
}

// revStringToServiceRevision converts a numeric string, which may include a prefix indicating its
// base, e.g. 0x for hex, to a big-endian 8-byte service.Revision vector representing a canonical
// form of a uint64. Note that anything higher than what a signed int64 can represent will fail to
// parse per the limitation of Go's strconv.ParseInt. A non-integer value will also fail to parse.
// All parse failures simply return a result of all 1 bits, which real rev #'s will never reach.
func revStringToServiceRevision(revString string) (result service.Revision) {
	revInt64, err := strconv.ParseInt(revString, 0, 64)
	if err != nil {
		return []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	}
	return EncodeRev(uint64(revInt64))
}
