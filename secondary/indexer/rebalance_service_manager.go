// @author Couchbase <info@couchbase.com>
// @copyright 2016-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"sync"
	"time"

	"errors"
	"net/http"
	"reflect"
	"strings"

	"encoding/json"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/indexing/secondary/audit"
	"github.com/couchbase/indexing/secondary/common"
	c "github.com/couchbase/indexing/secondary/common"
	forestdb "github.com/couchbase/indexing/secondary/fdb"
	l "github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/manager"
	"github.com/couchbase/indexing/secondary/manager/client"
	"github.com/couchbase/indexing/secondary/security"
)

// RebalanceServiceManager implements the ns_server cbauth/service.Manager interface, which drives
// Rebalance and Failover through RPC. This class also handles index move, which is an internal GSI
// feature that does not involve ns_server.
type RebalanceServiceManager struct {
	genericMgr *GenericServiceManager // pointer to our parent

	// svcMgrMu protects m.rebalancer, m.rebalancerF, and other shared fields that do not have their
	// own mutexes
	svcMgrMu *sync.RWMutex

	state   state         // state of the current rebalance, failover, or index move
	stateMu *sync.RWMutex // protects state field; may be taken *after* svcMgrMu mutex

	waiters   waiters     // set of channels of states for go routines waiting for next state change
	waitersMu *sync.Mutex // protects waiters field; may be taken *after* svcMgrMu mutex

	rebalancer  RebalanceProvider // runs the rebalance, failover, or index move
	rebalancerF RebalanceProvider // follower rebalancer handle

	rebalanceCtx *rebalanceContext
	nodeInfo     *service.NodeInfo // never changes; info about the local node

	config        c.ConfigHolder // Indexer config settings; updated via handleConfigUpdate
	supvCmdch     MsgChannel     //supervisor sends commands on this channel (idx.rebalMgrCmdCh)
	supvMsgch     MsgChannel     //channel to send msg to supervisor for normal handling (idx.wrkrRecvCh)
	supvPrioMsgch MsgChannel     //channel to send msg to supervisor for high-priority handling (idx.wrkrPrioRecvCh)
	moveStatusCh  chan error
	monitorStopCh StopChannel

	localhttp string // local indexer host:port for HTTP, e.g. "127.0.0.1:9102", 9108,...

	cleanupPending   bool // prior rebalance or move did not finish and will be cleaned up
	indexerReady     bool
	rebalanceRunning bool
	rebalanceToken   *RebalanceToken

	p runParams
}

type rebalanceContext struct {
	change service.TopologyChange
	rev    uint64
}

// incRev increments the rebalanceContext revision number and returns the OLD value.
func (ctx *rebalanceContext) incRev() uint64 {
	curr := ctx.rev
	ctx.rev++

	return curr
}

type waiter chan state
type waiters map[waiter]struct{}

// state contains the state information for a rebalance, failover, or index move.
type state struct {
	// revCopy is the service.Revision number associated with this current Rebalance state. Other
	// components can also trigger revision changes, so GenericServiceManager now owns the canonical
	// rev counter. revCopy is a copy of the value corresponding to the last Rebalance state rev.
	revCopy uint64

	// servers holds Index Service NodeUUIDs for GetCurrentTopology response
	servers []service.NodeID

	// isBalanced tells whether GSI considers itself balanced. It is also sent to ns_server as part
	// of GetCurrentTopology response, which uses it as one input to tell UI whether to enable the
	// Rebalance button. (If any service reports isBalanced == false, the button becomes enabled.)
	// revCopy is not incremented for changes to this.
	isBalanced bool

	// rebalanceID is "" when no rebalance/failover/move is running. PrepareTopologyChange sets it
	// to the TopologyChange.ID from ns_server. It gets reset to "" when the topology change either
	// finishes or gets canceled (RebalCancelTask).
	rebalanceID string

	// rebalanceTask is nil if a rebalance/failover/move has never run or the last one was canceled
	// before it either completed or failed. It is non-nil if either:
	//   1. One is currently running (rebalanceID will be non-""), or
	//   2. The last one failed (rebalanceID will be "").
	// #2 causes RebalGetTaskList to return info on the failed rebalance.
	rebalanceTask *service.Task
}

// newState constructs a zero-value state object.
func newState() state {
	return state{
		revCopy:       0,
		servers:       nil,
		isBalanced:    true, // Default value of isBalanced should be true
		rebalanceID:   "",
		rebalanceTask: nil,
	}
}

type runParams struct {
	ddlRunning           bool
	ddlRunningIndexNames []string
}

var rebalanceHttpTimeout int
var MoveIndexStarted = "Move Index has started. Check Indexes UI for progress and Logs UI for any error"

var ErrDDLRunning = errors.New("indexer rebalance failure - ddl in progress")

// NewRebalanceServiceManager is the constructor for the RebalanceServiceManager class.
// GenericServiceManager constructs a singleton at boot.
func NewRebalanceServiceManager(genericMgr *GenericServiceManager, httpAddr string,
	supvCmdch MsgChannel, supvMsgch MsgChannel, supvPrioMsgch MsgChannel, config c.Config,
	nodeInfo *service.NodeInfo, rebalanceRunning bool, rebalanceToken *RebalanceToken,
) *RebalanceServiceManager {

	l.Infof("RebalanceServiceManager::NewRebalanceServiceManager %v %v ", rebalanceRunning, rebalanceToken)

	mgr := &RebalanceServiceManager{
		genericMgr: genericMgr,
		svcMgrMu:   &sync.RWMutex{},

		state:   newState(),
		stateMu: &sync.RWMutex{},

		waiters:   make(waiters),
		waitersMu: &sync.Mutex{},

		supvCmdch:     supvCmdch,
		supvMsgch:     supvMsgch,
		supvPrioMsgch: supvPrioMsgch,
		moveStatusCh:  make(chan error),
	}
	mgr.config.Store(config)

	rebalanceHttpTimeout = config["rebalance.httpTimeout"].Int()

	mgr.nodeInfo = nodeInfo
	mgr.rebalanceRunning = rebalanceRunning
	mgr.rebalanceToken = rebalanceToken
	mgr.localhttp = httpAddr

	if rebalanceToken != nil {
		mgr.cleanupPending = true
		if rebalanceToken.Source == RebalSourceClusterOp { // rebalance, not move
			mgr.state.isBalanced = false
		}
	}

	go mgr.run()
	go mgr.initService(mgr.cleanupPending)

	return mgr
}

func (m *RebalanceServiceManager) initService(cleanupPending bool) {

	l.Infof("RebalanceServiceManager::initService Init")

	//allow trivial cleanups to finish to reduce noise
	if cleanupPending {
		time.Sleep(5 * time.Second)
	}

	go m.updateNodeList()

	mux := GetHTTPMux()
	mux.HandleFunc("/registerRebalanceToken", m.handleRegisterRebalanceToken)
	mux.HandleFunc("/listRebalanceTokens", m.handleListRebalanceTokens)
	mux.HandleFunc("/cleanupRebalance", m.handleCleanupRebalance)
	mux.HandleFunc("/moveIndex", m.handleMoveIndex)
	mux.HandleFunc("/moveIndexInternal", m.handleMoveIndexInternal)
	mux.HandleFunc("/nodeuuid", m.handleNodeuuid)
}

// updateNodeList initializes RebalanceServiceManager.state.servers node list on start or restart.
func (m *RebalanceServiceManager) updateNodeList() {
	const _updateNodeList = "RebalanceServiceManager::updateNodeList:"

	// This needs retries because the cbauth database for REST authorization might still be booting
	var err error
	var nodeList []service.NodeID
	done := false
	const RETRIES = 60
	for retry := 0; !done && retry <= RETRIES; retry++ {
		nodeList, err = m.getCachedIndexerNodeUUIDs()
		if err == nil {
			done = true
		} else if retry < RETRIES {
			time.Sleep(time.Second)
		}
	}
	if err != nil {
		l.Errorf("%v getCachedIndexerNodeUUIDs returned err: %v", _updateNodeList, err)
		return
	}

	//update only if not yet updated by PrepareTopologyChange
	m.updateState(func(s *state) {
		if s.servers == nil {
			s.servers = nodeList
		}
	})
	l.Infof("%v Initialized with %v NodeUUIDs: %v", _updateNodeList, len(nodeList), nodeList)
}

// run starts the rebalance manager loop which listens to messages
// from it supervisor(indexer)
func (m *RebalanceServiceManager) run() {

	//main Rebalance Manager loop
loop:
	for {
		select {

		case cmd, ok := <-m.supvCmdch:
			if ok {
				if cmd.GetMsgType() == ADMIN_MGR_SHUTDOWN {
					l.Infof("Rebalance Manager: Shutting Down")
					m.supvCmdch <- &MsgSuccess{}
					break loop
				}
				m.handleSupervisorCommands(cmd)
			} else {
				//supervisor channel closed. exit
				break loop
			}

		}
	}
}

func (m *RebalanceServiceManager) handleSupervisorCommands(cmd Message) {
	switch cmd.GetMsgType() {

	case CONFIG_SETTINGS_UPDATE:
		m.handleConfigUpdate(cmd)

	case CLUST_MGR_INDEXER_READY:
		m.handleIndexerReady(cmd)

	default:
		l.Fatalf("RebalanceServiceManager::handleSupervisorCommands Unknown Message %+v", cmd)
		c.CrashOnError(errors.New("Unknown Msg On Supv Channel"))
	}

}

func (m *RebalanceServiceManager) handleConfigUpdate(cmd Message) {
	cfgUpdate := cmd.(*MsgConfigUpdate)
	m.config.Store(cfgUpdate.GetConfig())
	m.supvCmdch <- &MsgSuccess{}
}

func (m *RebalanceServiceManager) handleIndexerReady(cmd Message) {

	m.supvCmdch <- &MsgSuccess{}

	go m.recoverRebalance()

}

/////////////////////////////////////////////////////////////////////////
//
//  service.Manager interface implementations
//
//  1. Delegates for generic APIs used by Rebalance:
//       RebalGetTaskList: Rebalance's delegate for GetTaskList
//       RebalCancelTask:  Rebalance's delegate for CancelTask
//
//  2. Direct implementation of Rebalance-specific APIs:
//       GetCurrentTopology
//       PrepareTopologyChange
//       StartTopologyChange
//
//  Interface defined in cbauth/service/interface.go
//
/////////////////////////////////////////////////////////////////////////

// RebalGetTaskList is a delegate of GenericServiceManager.GetTaskList which is an external API
// called by ns_server (via cbauth). Waiting for new rev is now done in the caller instead of here.
func (m *RebalanceServiceManager) RebalGetTaskList() *service.TaskList {
	return stateToTaskList(m.copyState())
}

// RebalCancelTask is a delegate of GenericServiceManager.CancelTask which is an external API
// called by ns_server (via cbauth).
func (m *RebalanceServiceManager) RebalCancelTask(id string, rev service.Revision) error {
	const _RebalCancelTask = "RebalanceServiceManager::RebalCancelTask:"
	l.Infof("%v called. id: %v, rev: %v", _RebalCancelTask, id, rev)

	currState := m.copyState()
	tasks := stateToTaskList(currState).Tasks
	task := (*service.Task)(nil)

	for i := range tasks {
		t := &tasks[i]

		if t.ID == id {
			task = t
			break
		}
	}

	if task == nil {
		return service.ErrNotFound
	}

	if !task.IsCancelable {
		return service.ErrNotSupported
	}

	if rev != nil && !bytes.Equal(rev, task.Rev) {
		l.Errorf("%v returning error: %v. rev: %v, task.Rev: %v", _RebalCancelTask,
			service.ErrConflict, rev, task.Rev)
		return service.ErrConflict
	}

	return m.cancelActualTask(task)
}

// GetCurrentTopology is an external API called by ns_server (via cbauth).
// If rev is non-nil, respond only when the revision changes from that,
// else respond immediately.
func (m *RebalanceServiceManager) GetCurrentTopology(rev service.Revision,
	cancel service.Cancel) (*service.Topology, error) {

	l.Infof("RebalanceServiceManager::GetCurrentTopology %v", rev)

	currState, err := m.wait(rev, cancel)
	if err != nil {
		return nil, err
	}

	topology := m.stateToTopology(currState)
	l.Infof("RebalanceServiceManager::GetCurrentTopology returns %+v", topology)
	return topology, nil
}

// PrepareTopologyChange is an external API called by ns_server (via cbauth) on all indexer nodes
// to prepare for a rebalance or failover that will later be started by StartTopologyChange
// on the rebalance/failover master indexer node.
//
// All errors need to be reported as return value. Status of prepared task is not
// considered for failure reporting.
func (m *RebalanceServiceManager) PrepareTopologyChange(change service.TopologyChange) error {
	l.Infof("RebalanceServiceManager::PrepareTopologyChange %v", change)

	currState := m.copyState()
	if currState.rebalanceID != "" {
		l.Errorf("RebalanceServiceManager::PrepareTopologyChange err %v %v",
			service.ErrConflict, currState.rebalanceID)
		if change.Type == service.TopologyChangeTypeRebalance {
			m.setStateIsBalanced(false)
		}
		return service.ErrConflict
	}

	var err error
	if change.Type == service.TopologyChangeTypeFailover {
		err = m.prepareFailover(change)
	} else if change.Type == service.TopologyChangeTypeRebalance {
		err = m.prepareRebalance(change)
		if err != nil {
			m.setStateIsBalanced(false)
			currState.isBalanced = false // keep in sync for logging below
		}
	} else {
		err = service.ErrNotSupported
	}
	if err != nil {
		return err
	}

	nodeList := make([]service.NodeID, 0)
	for _, n := range change.KeepNodes {
		nodeList = append(nodeList, n.NodeInfo.NodeID)
	}

	m.updateState(func(s *state) {
		s.rebalanceID = change.ID
		s.servers = nodeList
	})

	l.Infof("RebalanceServiceManager::PrepareTopologyChange Success. isBalanced %v",
		currState.isBalanced)
	return nil
}

// prepareFailover does sanity checks for the failover case under PrepareTopologyChange.
func (m *RebalanceServiceManager) prepareFailover(change service.TopologyChange) error {
	const method = "RebalanceServiceManager::prepareFailover:" // for logging

	lockTime := c.TraceRWMutexLOCK(c.LOCK_WRITE, m.svcMgrMu, "svcMgrMu", method, "")
	defer c.TraceRWMutexUNLOCK(lockTime, c.LOCK_WRITE, m.svcMgrMu, "svcMgrMu", method, "")

	if !m.indexerReady {
		return nil
	}

	var err error
	if m.rebalanceToken != nil && m.rebalanceToken.Source == RebalSourceClusterOp {

		l.Infof("%v Found Rebalance In Progress %v", method, m.rebalanceToken)

		if m.rebalancerF != nil {
			m.rebalancerF.Cancel()
			m.rebalancerF = nil
		}

		masterAlive := false
		for _, node := range change.KeepNodes {
			if m.rebalanceToken.MasterId == string(node.NodeInfo.NodeID) {
				masterAlive = true
				break
			}
		}
		if !masterAlive {
			l.Infof("%v Master Missing From Cluster Node List. Cleanup", method)
			err = m.runCleanupPhaseLOCKED(RebalanceTokenPath, true)
		} else {
			err = m.runCleanupPhaseLOCKED(RebalanceTokenPath, false)
		}
		return err
	}

	if m.rebalanceRunning && m.rebalanceToken.Source == RebalSourceClusterOp {
		l.Infof("%v Found Node In Prepared State. Cleanup.", method)
		err = m.runCleanupPhaseLOCKED(RebalanceTokenPath, false)
		return err
	}

	if m.rebalanceToken != nil && m.rebalanceToken.Source == RebalSourceMoveIndex {

		l.Infof("%v Found Move Index In Progress %v. Aborting.", method, m.rebalanceToken)

		masterAlive := false
		masterCleanup := false
		for _, node := range change.KeepNodes {
			if m.rebalanceToken.MasterId == string(node.NodeInfo.NodeID) {
				masterAlive = true
				break
			}
		}

		if !masterAlive {
			l.Infof("%v Master Missing From Cluster Node List. Cleanup MoveIndex As Master.",
				method)
			masterCleanup = true
		}

		if m.rebalanceToken.MasterId == string(m.nodeInfo.NodeID) {
			if m.rebalancer != nil {
				m.rebalancer.Cancel()
				m.rebalancer = nil
				m.moveStatusCh <- errors.New("Move Index Aborted due to Node Failover")
			}
			if err = m.runCleanupPhaseLOCKED(MoveIndexTokenPath, true); err != nil {
				return err
			}
		} else {
			if m.rebalancerF != nil {
				m.rebalancerF.Cancel()
				m.rebalancerF = nil
			}
			if err = m.runCleanupPhaseLOCKED(MoveIndexTokenPath, masterCleanup); err != nil {
				return err
			}
		}

		return err
	}

	return nil

}

// prepareRebalance does sanity checks for the rebalance case under PrepareTopologyChange.
func (m *RebalanceServiceManager) prepareRebalance(change service.TopologyChange) error {
	const method = "RebalanceServiceManager::prepareRebalance:" // for logging

	lockTime := c.TraceRWMutexLOCK(c.LOCK_WRITE, m.svcMgrMu, "svcMgrMu", method, "")
	defer c.TraceRWMutexUNLOCK(lockTime, c.LOCK_WRITE, m.svcMgrMu, "svcMgrMu", method, "")

	var err error
	if m.cleanupPending {
		m.setStateIsBalanced(false)
		err = errors.New("indexer rebalance failure - cleanup pending from previous  " +
			"failed/aborted rebalance/failover/move index. please retry the request later.")
		l.Errorf("%v %v", method, err)
		return err
	}

	if m.rebalanceToken != nil && m.rebalanceToken.Source == RebalSourceMoveIndex {
		err = errors.New("indexer rebalance failure - move index in progress")
		l.Errorf("%v %v", method, err)
		return err
	}

	if m.rebalanceToken != nil && m.rebalanceToken.Source == RebalSourceClusterOp {
		l.Warnf("%v Found Rebalance In Progress. Cleanup.", method)
		if m.rebalancerF != nil {
			m.rebalancerF.Cancel()
			m.rebalancerF = nil
		}
		if err = m.runCleanupPhaseLOCKED(RebalanceTokenPath, false); err != nil {
			return err
		}
	}

	if m.checkRebalanceRunning() {
		l.Warnf("%v Found Rebalance Running Flag. Cleanup Prepare Phase", method)
		if err = m.runCleanupPhaseLOCKED(RebalanceTokenPath, false); err != nil {
			return err
		}
	}

	if m.checkLocalCleanupPending() {
		l.Warnf("%v Found Pending Local Cleanup Token. Run Cleanup.", method)
		if err = m.runCleanupPhaseLOCKED(RebalanceTokenPath, false); err != nil {
			return err
		}
		//check again if the cleanup was successful
		if m.checkLocalCleanupPending() {
			err = errors.New("indexer rebalance failure - cleanup pending from previous  " +
				"failed/aborted rebalance/failover/move index. please retry the request later.")
			l.Errorf("%v %v", method, err)
			return err
		}
	}

	if c.GetBuildMode() == c.ENTERPRISE {
		m.p.ddlRunning, m.p.ddlRunningIndexNames = m.checkDDLRunning()
		l.Infof("%v Found DDL Running %v", method, m.p.ddlRunningIndexNames)
	}

	l.Infof("%v Init Prepare Phase", method)

	if isSingleNodeRebal(change) && change.KeepNodes[0].NodeInfo.NodeID != m.nodeInfo.NodeID {
		err := errors.New("indexer - node receiving prepare request not part of cluster")
		l.Errorf("%v %v", method, err)
		return err
	} else {
		if err := m.initPreparePhaseRebalance(); err != nil {
			return err
		}
	}

	m.setStateIsBalanced(true) // kludge to disable UI "Rebalance" button during rebalance as ns_server doesn't
	return nil
}

// StartTopologyChange is an external API called by ns_server (via cbauth) only on the
// GSI master node to initiate a rebalance or failover that has already been prepared
// via PrepareTopologyChange calls on all indexer nodes.
func (m *RebalanceServiceManager) StartTopologyChange(change service.TopologyChange) error {
	const method = "RebalanceServiceManager::StartTopologyChange:" // for logging

	l.Infof("%v change: %v", method, change)

	// To avoid having more than one Rebalancer object at a time, we must hold svcMgrMu write locked from
	// the check for nil m.rebalancer through execution of children startFailover or startRebalance
	// which will overwrite this field.
	lockTime := c.TraceRWMutexLOCK(c.LOCK_WRITE, m.svcMgrMu, "svcMgrMu", method, "")
	defer c.TraceRWMutexUNLOCK(lockTime, c.LOCK_WRITE, m.svcMgrMu, "svcMgrMu", method, "")

	currState := m.copyState()
	rebalancer := m.rebalancer
	if currState.rebalanceID != change.ID || rebalancer != nil {
		l.Errorf("%v err %v %v %v %v", method, service.ErrConflict,
			currState.rebalanceID, change.ID, rebalancer)
		if change.Type == service.TopologyChangeTypeRebalance {
			m.setStateIsBalanced(false)
		}
		return service.ErrConflict
	}

	if change.CurrentTopologyRev != nil {
		haveRev := DecodeRev(change.CurrentTopologyRev)
		if haveRev != currState.revCopy {
			l.Errorf("%v err %v %v %v", method, service.ErrConflict,
				haveRev, currState.revCopy)
			if change.Type == service.TopologyChangeTypeRebalance {
				m.setStateIsBalanced(false)
			}
			return service.ErrConflict
		}
	}

	var err error
	if change.Type == service.TopologyChangeTypeFailover {
		err = m.startFailover(change)
	} else if change.Type == service.TopologyChangeTypeRebalance {
		err = m.startRebalance(change)
		if err != nil {
			m.setStateIsBalanced(false)
			currState.isBalanced = false // keep in sync for logging below
		}
	} else {
		err = service.ErrNotSupported
	}

	l.Infof("%v returns Error %v. isBalanced %v.", method, err, currState.isBalanced)
	return err
}

func (m *RebalanceServiceManager) startFailover(change service.TopologyChange) error {

	ctx := &rebalanceContext{
		rev:    0,
		change: change,
	}

	m.rebalanceCtx = ctx
	m.updateRebalanceProgressLOCKED(0)

	m.rebalancer = NewRebalancer(nil, nil, string(m.nodeInfo.NodeID), true,
		m.rebalanceProgressCallback, m.failoverDoneCallback, m.supvMsgch, "", m.config.Load(), &change, false, nil, m.genericMgr.statsMgr)

	return nil
}

// startRebalance is a helper for StartTopologyChange. It first cleans up orphan
// tokens (those whose master or owner node is not part of the cluster), then
// verifies there is not already an existing rebalance token or move token. If
// these pass it registers the rebalance token and creates the Rebalancer object
// that will master the rebalance. This object launhes go routines to perform the
// rebalance asynchronously.
func (m *RebalanceServiceManager) startRebalance(change service.TopologyChange) error {

	var runPlanner bool
	var skipRebalance bool
	var transferTokens map[string]*c.TransferToken

	cfg := m.config.Load()

	if isSingleNodeRebal(change) && change.KeepNodes[0].NodeInfo.NodeID != m.nodeInfo.NodeID {
		err := errors.New("Node receiving Start request not part of cluster")
		l.Errorf("RebalanceServiceManager::startRebalance %v Self %v Cluster %v", err, m.nodeInfo.NodeID,
			change.KeepNodes[0].NodeInfo.NodeID)
		m.runCleanupPhaseLOCKED(RebalanceTokenPath, true)
		return err
	} else {

		var err error

		err = m.cleanupOrphanTokens(change)
		if err != nil {
			l.Errorf("RebalanceServiceManager::startRebalance Error During Cleanup Orphan Tokens %v", err)
			m.runCleanupPhaseLOCKED(RebalanceTokenPath, true)
			return err
		}

		rtoken, err := m.checkExistingGlobalRToken()
		if err != nil {
			l.Errorf("RebalanceServiceManager::startRebalance Error Checking Global RToken %v", err)
			m.runCleanupPhaseLOCKED(RebalanceTokenPath, true)
			return err
		}

		if rtoken != nil {
			l.Errorf("RebalanceServiceManager::startRebalance Found Existing Global RToken %v", rtoken)
			m.runCleanupPhaseLOCKED(RebalanceTokenPath, true)
			return errors.New("Protocol Conflict Error: Existing Rebalance Token Found")
		}

		err, skipRebalance = m.initStartPhase(change)
		if err != nil {
			l.Errorf("RebalanceServiceManager::startRebalance Error During Start Phase Init %v", err)
			m.runCleanupPhaseLOCKED(RebalanceTokenPath, true)
			return err
		}

		if c.GetBuildMode() != c.ENTERPRISE {
			l.Infof("RebalanceServiceManager::startRebalance skip planner for non-enterprise edition")
			runPlanner = false
		} else if cfg["rebalance.disable_index_move"].Bool() {
			l.Infof("RebalanceServiceManager::startRebalance skip planner as disable_index_move is set")
			runPlanner = false
		} else if skipRebalance {
			l.Infof("RebalanceServiceManager::startRebalance skip planner due to skipRebalance flag")
			runPlanner = false
		} else {
			runPlanner = true
		}
	}

	ctx := &rebalanceContext{
		rev:    0,
		change: change,
	}

	m.rebalanceCtx = ctx
	m.updateRebalanceProgressLOCKED(0)

	// TODO: Add a cluster version check
	isShardAwareRebalance := cfg["rebalance.shard_aware_rebalance"].Bool()

	if isShardAwareRebalance {
		m.rebalancer = NewShardRebalancer(transferTokens, m.rebalanceToken, string(m.nodeInfo.NodeID),
			true, m.rebalanceProgressCallback, m.rebalanceDoneCallback, m.supvMsgch,
			m.localhttp, m.config.Load(), &change, runPlanner, &m.p, m.genericMgr.statsMgr)
	} else {
		m.rebalancer = NewRebalancer(transferTokens, m.rebalanceToken, string(m.nodeInfo.NodeID),
			true, m.rebalanceProgressCallback, m.rebalanceDoneCallback, m.supvMsgch,
			m.localhttp, m.config.Load(), &change, runPlanner, &m.p, m.genericMgr.statsMgr)
	}

	return nil
}

/////////////////////////////////////////////////////////////////////////
//
//  service.Manager interface implementation helpers
//
/////////////////////////////////////////////////////////////////////////

func isSingleNodeRebal(change service.TopologyChange) bool {

	if len(change.KeepNodes) == 1 && len(change.EjectNodes) == 0 {
		return true
	}
	return false

}

func (m *RebalanceServiceManager) isNoOpRebal(change service.TopologyChange) bool {

	cfg := m.config.Load()
	onEjectOnly := cfg["rebalance.node_eject_only"].Bool()

	if onEjectOnly && len(change.EjectNodes) == 0 {
		return true
	}
	return false

}

// checkDDLRunning is called under PrepareTopologyChange for rebalance and asks
// indexer if there is DDL running via its high-priority message channel so it will
// get a quick reply even if indexer is processing other work, to try to avoid causing
// a rebalance timeout failure. It returns true and a slice of index names currently
// in DDL processing if DDL is currently running, else false and an empty slice.
func (m *RebalanceServiceManager) checkDDLRunning() (bool, []string) {

	respCh := make(MsgChannel)
	m.supvPrioMsgch <- &MsgCheckDDLInProgress{respCh: respCh}
	msg := <-respCh

	ddlInProgress := msg.(*MsgDDLInProgressResponse).GetDDLInProgress()
	inProgressIndexNames := msg.(*MsgDDLInProgressResponse).GetInProgressIndexNames()
	return ddlInProgress, inProgressIndexNames
}

func (m *RebalanceServiceManager) checkRebalanceRunning() bool {
	return m.rebalanceRunning
}

// checkExistingGlobalRToken is a helper for startRebalance (master node only). It
// checks for an existing rebalance token or, if not found, an existing move token.
// If one is found it is returned, else nil is returned. Errors are only returned
// if an existing token is found but cannot be retrieved.
func (m *RebalanceServiceManager) checkExistingGlobalRToken() (*RebalanceToken, error) {

	var rtoken RebalanceToken
	var found bool
	var err error
	found, err = c.MetakvGet(RebalanceTokenPath, &rtoken)
	if err != nil {
		l.Errorf("RebalanceServiceManager::checkExistingGlobalRToken Error Fetching "+
			"Rebalance Token From Metakv %v. Path %v", err, RebalanceTokenPath)
		return nil, err
	}
	if found {
		return &rtoken, nil
	}

	found, err = c.MetakvGet(MoveIndexTokenPath, &rtoken)
	if err != nil {
		l.Errorf("RebalanceServiceManager::checkExistingGlobalRToken Error Fetching "+
			"MoveIndex Token From Metakv %v. Path %v", err, MoveIndexTokenPath)
		return nil, err
	}
	if found {
		return &rtoken, nil
	}

	return nil, nil

}

// initPreparePhaseRebalance is a helper for prepareRebalance.
func (m *RebalanceServiceManager) initPreparePhaseRebalance() error {

	err := m.registerRebalanceRunning(true)
	if err != nil {
		return err
	}

	m.rebalanceRunning = true
	m.monitorStopCh = make(StopChannel)

	go m.monitorStartPhaseInit(m.monitorStopCh)

	return nil
}

// registerRebalanceRunning is called under PrepareTopologyChange so uses the
// indexer's high-priority message channel to get a quick reply even if indexer
// is processing other work, to try to avoid causing a rebalance timeout failure.
func (m *RebalanceServiceManager) registerRebalanceRunning(checkDDL bool) error {
	respch := make(MsgChannel)
	m.supvPrioMsgch <- &MsgClustMgrLocal{
		mType:    CLUST_MGR_SET_LOCAL,
		key:      RebalanceRunning,
		value:    "",
		respch:   respch,
		checkDDL: checkDDL,
	}

	respMsg := <-respch
	resp := respMsg.(*MsgClustMgrLocal)

	m.resetRunParams()

	errMsg := resp.GetError()
	if errMsg != nil {
		if errMsg == ErrDDLRunning {
			m.p.ddlRunning = true
			m.p.ddlRunningIndexNames = resp.GetInProgressIndexes()
			l.Infof("RebalanceServiceManager::registerRebalanceRunning Found DDL Running %v", m.p.ddlRunningIndexNames)
		} else {
			l.Errorf("RebalanceServiceManager::registerRebalanceRunning Unable to set RebalanceRunning In Local"+
				"Meta Storage. Err %v", errMsg)

			return errMsg
		}
	}

	// for ddl service manager
	stopDDLProcessing()

	return nil
}

// runCleanupPhaseLOCKED caller should be holding mutex svcMgrMu write(?) locked.
func (m *RebalanceServiceManager) runCleanupPhaseLOCKED(path string, isMaster bool) error {

	l.Infof("RebalanceServiceManager::runCleanupPhase path %v isMaster %v", path, isMaster)

	if m.monitorStopCh != nil {
		close(m.monitorStopCh)
		m.monitorStopCh = nil
	}

	if isMaster {
		err := m.cleanupGlobalRToken(path)
		if err != nil {
			return err
		}
	}

	if m.indexerReady {
		rtokens, err := m.getCurrRebalTokens()
		if err != nil {
			l.Errorf("RebalanceServiceManager::runCleanupPhase Error Fetching Metakv Tokens %v", err)
		}

		if rtokens != nil && len(rtokens.TT) != 0 {
			err := m.cleanupTransferTokens(rtokens.TT)
			if err != nil {
				l.Errorf("RebalanceServiceManager::runCleanupPhase Error Cleaning Transfer Tokens %v", err)
			}
		}
	}

	err := m.cleanupLocalRToken()
	if err != nil {
		return err
	}

	err = m.cleanupRebalanceRunning()
	if err != nil {
		return err
	}

	return nil
}

func (m *RebalanceServiceManager) cleanupGlobalRToken(path string) error {

	var rtoken RebalanceToken
	found, err := c.MetakvGet(path, &rtoken)
	if err != nil {
		l.Errorf("RebalanceServiceManager::cleanupGlobalRToken Error Fetching Rebalance Token From Metakv %v. Path %v", err, path)
		return err
	}

	if found {
		l.Infof("RebalanceServiceManager::cleanupGlobalRToken Delete Global Rebalance Token %v", rtoken)

		err := c.MetakvDel(path)
		if err != nil {
			l.Fatalf("RebalanceServiceManager::cleanupGlobalRToken Unable to delete RebalanceToken from "+
				"Meta Storage. %v. Err %v", rtoken, err)
			return err
		}
	}
	return nil
}

// cleanupOrphanTokens is a helper for startRebalance (master node only). It deletes
// Rebalance Tokens (RT) and Move Tokens (MT) whose master nodes are not alive, and
// Transfer Tokens (TT) whose current owners are not alive. "Alive" means the node is
// in the topology change.KeepNodes list.
func (m *RebalanceServiceManager) cleanupOrphanTokens(change service.TopologyChange) error {

	rtokens, err := m.getCurrRebalTokens()
	if err != nil {
		l.Errorf("RebalanceServiceManager::cleanupOrphanTokens Error Fetching Metakv Tokens %v", err)
		return err
	}

	if rtokens == nil {
		return nil
	}

	cleanup := func(path, token string) error {
		err := c.MetakvDel(path)
		if err != nil {
			l.Errorf("RebalanceServiceManager::cleanupOrphanTokens Unable to delete %v from "+
				"Meta Storage. %v. Err %v", token, rtokens.RT, err)
			return err
		}
		return nil
	}

	masterAlive := false
	if rtokens.RT != nil {
		l.Infof("RebalanceServiceManager::cleanupOrphanTokens Found Token %v", rtokens.RT)

		for _, node := range change.KeepNodes {
			if rtokens.RT.MasterId == string(node.NodeInfo.NodeID) {
				masterAlive = true
				break
			}
		}
		if !masterAlive || rtokens.RT.Error != "" {
			l.Infof("RebalanceServiceManager::cleanupOrphanTokens Cleaning Up Token %v as masterAlive: %v, err: %v",
				rtokens.RT, masterAlive, rtokens.RT.Error)
			if err := cleanup(RebalanceTokenPath, "RebalanceToken"); err != nil {
				return err
			}
		}
	}

	masterAlive = false
	if rtokens.MT != nil {
		l.Infof("RebalanceServiceManager::cleanupOrphanTokens Found MoveIndexToken %v", rtokens.MT)

		for _, node := range change.KeepNodes {
			if rtokens.MT.MasterId == string(node.NodeInfo.NodeID) {
				masterAlive = true
				break
			}
		}
		if !masterAlive || rtokens.MT.Error != "" {
			l.Infof("RebalanceServiceManager::cleanupOrphanTokens Cleaning Up Token %v as masterAlive: %v, err: %v",
				rtokens.MT, masterAlive, rtokens.MT.Error)
			if err := cleanup(MoveIndexTokenPath, "MoveIndexToken"); err != nil {
				return err
			}
		}
	}

	var ownerId string
	var ownerAlive bool
	for ttid, tt := range rtokens.TT {

		ownerAlive = false
		ownerId = m.getTransferTokenOwner(tt)
		for _, node := range change.KeepNodes {
			if ownerId == string(node.NodeInfo.NodeID) {
				ownerAlive = true
				break
			}
		}

		if !ownerAlive {
			l.Infof("RebalanceServiceManager::cleanupOrphanTokens Cleaning Up Token Owner %v %v %v", ownerId, ttid, tt)
			err := c.MetakvDel(RebalanceMetakvDir + ttid)
			if err != nil {
				l.Errorf("RebalanceServiceManager::cleanupOrphanTokens Unable to delete TransferToken from "+
					"Meta Storage. %v. Err %v", ttid, err)
				return err
			}
		}
	}

	return nil

}

func (m *RebalanceServiceManager) cleanupTransferTokens(tts map[string]*c.TransferToken) error {

	if tts == nil || len(tts) == 0 {
		l.Infof("RebalanceServiceManager::cleanupTransferTokens No Tokens Found For Cleanup")
		return nil
	}

	// cancel any merge
	indexStateMap := make(map[c.IndexInstId]c.RebalanceState)
	respch := make(chan error)
	for _, tt := range tts {
		indexStateMap[tt.InstId] = c.REBAL_NIL
		indexStateMap[tt.RealInstId] = c.REBAL_NIL
	}
	m.supvMsgch <- &MsgCancelMergePartition{
		indexStateMap: indexStateMap,
		respCh:        respch,
	}
	<-respch

	// order the transfer tokens to detect multiple partitions of same index on the same destination (this node)
	// with same realInstId, using local meta determine which TT represents realinst and move it to end of list
	// to avoide deleting realInst before proxy inst is deleted.

	hasMultiPartsToSameDest := false
	thisNodeId := string(m.nodeInfo.NodeID)
	// count of partitions with same defn and realInst for this destination, for partitioned indexes.
	destTTRealInstMap := make(map[c.IndexDefnId]map[c.IndexInstId]int)

	for _, tt := range tts {
		// only this node as dest and partitioned indexes
		if tt.DestId == thisNodeId && common.IsPartitioned(tt.IndexInst.Defn.PartitionScheme) {
			defnId := tt.IndexInst.Defn.DefnId
			realInstId := tt.RealInstId
			if _, ok := destTTRealInstMap[defnId]; !ok {
				destTTRealInstMap[defnId] = make(map[c.IndexInstId]int)
			}
			if _, ok := destTTRealInstMap[defnId][realInstId]; !ok {
				destTTRealInstMap[defnId][realInstId] = 1
			} else {
				// we have more than one instances to same dest, defn and same realInstid
				destTTRealInstMap[defnId][realInstId] = destTTRealInstMap[defnId][realInstId] + 1
				hasMultiPartsToSameDest = true
			}
		}
	}

	var localMeta *manager.LocalIndexMetadata
	var err error

	if hasMultiPartsToSameDest {
		localMeta, err = getLocalMeta(m.localhttp)
		if err != nil {
			l.Errorf("%v Error Fetching Local Meta %v %v", "RebalanceServiceManager::cleanupTransferTokens", m.localhttp, err)
			return err
		}
	}

	type ttListElement struct {
		ttid string
		tt   *c.TransferToken
	}

	// normally transfer tokens are added to ttList
	ttList := []ttListElement{}
	// only transfter tokens of realInst where we have two or more TTs to same destid (this node)
	// are added to ttListRealInst so that these real insts are processed at end.
	ttListRealInst := []ttListElement{}

	for ttid, tt := range tts {
		if hasMultiPartsToSameDest {
			if tt.DestId == thisNodeId {
				defnId := tt.IndexInst.Defn.DefnId
				realInstId := tt.RealInstId
				if _, ok := destTTRealInstMap[defnId]; ok {
					// more than one tts for partitioned index to same dest with same defn and realInst
					if cnt, ok := destTTRealInstMap[defnId][realInstId]; ok && cnt > 1 {
						isProxy, err := m.isProxyFromMeta(tt, localMeta)
						// we wont fail the rebalance here if isProxyFromMeta returned error and
						// let it do the normal cleanup which may or may not fail later
						if err == nil && !isProxy { // this is a realInst
							ttListRealInst = append(ttListRealInst, ttListElement{ttid, tt})
							continue
						}
					}
				}
			}
		}
		ttList = append(ttList, ttListElement{ttid, tt})
	}

	ttList = append(ttList, ttListRealInst...)

	// cleanup transfer token
	for _, t := range ttList {

		l.Infof("RebalanceServiceManager::cleanupTransferTokens Cleaning Up %v %v", t.ttid, t.tt)

		if t.tt.MasterId == string(m.nodeInfo.NodeID) {
			m.cleanupTransferTokensForMaster(t.ttid, t.tt)
		}
		if t.tt.SourceId == string(m.nodeInfo.NodeID) {
			m.cleanupTransferTokensForSource(t.ttid, t.tt)
		}
		if t.tt.DestId == string(m.nodeInfo.NodeID) {
			m.cleanupTransferTokensForDest(t.ttid, t.tt, indexStateMap)
		}

	}

	return nil
}

func (m *RebalanceServiceManager) isProxyFromMeta(tt *c.TransferToken, localMeta *manager.LocalIndexMetadata) (bool, error) {

	method := "RebalanceServiceManager::isProxyFromMeta"

	inst := tt.IndexInst

	topology := findTopologyByCollection(localMeta.IndexTopologies, inst.Defn.Bucket, inst.Defn.Scope, inst.Defn.Collection)
	if topology == nil {
		err := fmt.Errorf("Topology Information Missing for Bucket %v Scope %v Collection %v",
			inst.Defn.Bucket, inst.Defn.Scope, inst.Defn.Collection)
		l.Errorf("%v %v", method, err)
		return false, err
	}
	isProxy := topology.IsProxyIndexInst(tt.IndexInst.Defn.DefnId, tt.InstId)
	return isProxy, nil
}

func (m *RebalanceServiceManager) cleanupTransferTokensForMaster(ttid string, tt *c.TransferToken) error {

	switch tt.State {

	case c.TransferTokenCommit, c.TransferTokenDeleted:
		l.Infof("RebalanceServiceManager::cleanupTransferTokensForMaster Cleanup Token %v %v", ttid, tt)
		err := c.MetakvDel(RebalanceMetakvDir + ttid)
		if err != nil {
			l.Errorf("RebalanceServiceManager::cleanupTransferTokensForMaster Unable to delete TransferToken In "+
				"Meta Storage. %v. Err %v", tt, err)
			return err
		}

	}

	return nil

}

func (m *RebalanceServiceManager) cleanupTransferTokensForSource(ttid string, tt *c.TransferToken) error {

	switch tt.State {

	case c.TransferTokenReady:
		var err error
		l.Infof("RebalanceServiceManager::cleanupTransferTokensForSource Cleanup Token %v %v", ttid, tt)
		defn := tt.IndexInst.Defn
		defn.InstId = tt.InstId
		defn.RealInstId = tt.RealInstId
		err = m.cleanupIndex(defn)
		if err == nil {
			err = c.MetakvDel(RebalanceMetakvDir + ttid)
			if err != nil {
				l.Errorf("RebalanceServiceManager::cleanupTransferTokensForSource Unable to delete TransferToken In "+
					"Meta Storage. %v. Err %v", tt, err)
				return err
			}
		}
	}

	return nil

}

func (m *RebalanceServiceManager) cleanupTransferTokensForDest(ttid string, tt *c.TransferToken, indexStateMap map[c.IndexInstId]c.RebalanceState) error {

	cleanup := func() error {
		var err error
		l.Infof("RebalanceServiceManager::cleanupTransferTokensForDest Cleanup Token %v %v", ttid, tt)
		defn := tt.IndexInst.Defn
		defn.InstId = tt.InstId
		defn.RealInstId = tt.RealInstId
		err = m.cleanupIndex(defn)
		if err == nil {
			err = c.MetakvDel(RebalanceMetakvDir + ttid)
			if err != nil {
				l.Errorf("RebalanceServiceManager::cleanupTransferTokensForDest Unable to delete TransferToken In "+
					"Meta Storage. %v. Err %v", tt, err)
				return err
			}
		}

		return nil
	}

	switch tt.State {

	case c.TransferTokenCreated, c.TransferTokenAccepted, c.TransferTokenRefused,
		c.TransferTokenInitiate, c.TransferTokenInProgress:
		return cleanup()

	case c.TransferTokenMerge:
		// the proxy instance could have been deleted after it has gone to MERGED state
		rebalState, ok := indexStateMap[tt.InstId]
		if !ok {
			rebalState, ok = indexStateMap[tt.RealInstId]
		}

		if !ok {
			if err := c.MetakvDel(RebalanceMetakvDir + ttid); err != nil {
				l.Errorf("RebalanceServiceManager::cleanupTransferTokensForDest Unable to delete TransferToken In "+
					"Meta Storage. %v. Err %v", tt, err)
				return err
			}

			// proxy instance: REBAL_PENDING -> REBAL_MERGED
			// real instance: REBAL_PENDING -> REBAL_ACTIVE
		} else if rebalState == c.REBAL_MERGED || rebalState == c.REBAL_ACTIVE {
			tt.State = c.TransferTokenReady
			setTransferTokenInMetakv(ttid, tt)

		} else {
			return cleanup()
		}
	}

	return nil
}

func (m *RebalanceServiceManager) cleanupIndex(indexDefn c.IndexDefn) error {

	localaddr := m.localhttp

	url := "/dropIndex"
	resp, err := postWithHandleEOF(indexDefn, localaddr, url, "RebalanceServiceManager::cleanupIndex")
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	bytes, _ := ioutil.ReadAll(resp.Body)
	response := new(IndexResponse)
	if err := json.Unmarshal(bytes, &response); err != nil {
		l.Errorf("RebalanceServiceManager::cleanupIndex Error unmarshal response %v %v", localaddr+url, err)
		return err
	}
	if response.Code == RESP_ERROR {
		if strings.Contains(response.Error, forestdb.FDB_RESULT_KEY_NOT_FOUND.Error()) {
			l.Errorf("RebalanceServiceManager::cleanupIndex Error dropping index %v %v. Ignored.", localaddr+url, response.Error)
			return nil
		}
		l.Errorf("RebalanceServiceManager::cleanupIndex Error dropping index %v %v", localaddr+url, response.Error)
		return err
	}

	return nil

}

func (m *RebalanceServiceManager) cleanupRebalanceRunning() error {

	l.Infof("RebalanceServiceManager::cleanupRebalanceRunning Cleanup")

	respch := make(MsgChannel)
	m.supvMsgch <- &MsgClustMgrLocal{
		mType:  CLUST_MGR_DEL_LOCAL,
		key:    RebalanceRunning,
		respch: respch,
	}

	respMsg := <-respch
	resp := respMsg.(*MsgClustMgrLocal)

	errMsg := resp.GetError()
	if errMsg != nil {
		l.Fatalf("RebalanceServiceManager::cleanupRebalanceRunning Unable to delete RebalanceRunning In Local"+
			"Meta Storage. Err %v", errMsg)
		c.CrashOnError(errMsg)
	}

	m.rebalanceRunning = false

	// notify DDLServiceManager and SchedIndexCreator
	resumeDDLProcessing()

	return nil

}

func (m *RebalanceServiceManager) cleanupLocalRToken() error {

	l.Infof("RebalanceServiceManager::cleanupLocalRToken Cleanup")

	respch := make(MsgChannel)
	m.supvMsgch <- &MsgClustMgrLocal{
		mType:  CLUST_MGR_DEL_LOCAL,
		key:    RebalanceTokenTag,
		respch: respch,
	}

	respMsg := <-respch
	resp := respMsg.(*MsgClustMgrLocal)

	errMsg := resp.GetError()
	if errMsg != nil {
		l.Fatalf("RebalanceServiceManager::cleanupLocalRToken Unable to delete Rebalance Token In Local"+
			"Meta Storage. Path %v Err %v", RebalanceTokenPath, errMsg)
		c.CrashOnError(errMsg)
	}

	m.rebalanceToken = nil
	return nil
}

// monitorStartPhaseInit runs in a goroutine and checks whether rebalanceToken is created in a reasonable time.
func (m *RebalanceServiceManager) monitorStartPhaseInit(stopch StopChannel) error {
	const method = "RebalanceServiceManager::monitorStartPhaseInit:" // for logging

	cfg := m.config.Load()
	startPhaseBeginTimeout := cfg["rebalance.startPhaseBeginTimeout"].Int()

	elapsed := 1
	done := false
loop:
	for {

		select {
		case <-stopch:
			break loop

		default:
			func() {
				lockTime := c.TraceRWMutexLOCK(c.LOCK_WRITE, m.svcMgrMu, "svcMgrMu", method, "")
				defer c.TraceRWMutexUNLOCK(lockTime, c.LOCK_WRITE, m.svcMgrMu, "svcMgrMu", method, "")

				if m.rebalanceToken == nil && elapsed > startPhaseBeginTimeout {
					l.Infof("%v Timeout Waiting for RebalanceToken. Cleanup Prepare Phase",
						method)
					//TODO handle server side differently
					m.runCleanupPhaseLOCKED(RebalanceTokenPath, false)
					done = true
				} else if m.rebalanceToken != nil {
					l.Infof("%v Found RebalanceToken %v.", method, m.rebalanceToken)
					m.monitorStopCh = nil
					done = true
				}
			}()
		}
		if done {
			break loop
		}

		time.Sleep(time.Second * time.Duration(1))
		elapsed += 1
	}

	return nil

}

func (m *RebalanceServiceManager) rebalanceJanitor() {
	const _rebalanceJanitor = "RebalanceServiceManager::rebalanceJanitor" // for logging

	for {
		time.Sleep(time.Second * 30)

		l.Infof("%v Running Periodic Cleanup", _rebalanceJanitor)
		lockTime := c.TraceRWMutexLOCK(c.LOCK_WRITE, m.svcMgrMu, "svcMgrMu", _rebalanceJanitor, "")
		if !m.rebalanceRunning {
			rtokens, err := m.getCurrRebalTokens()
			if err != nil {
				l.Errorf("%v Error Fetching Metakv Tokens %v", _rebalanceJanitor, err)
			}

			if rtokens != nil && len(rtokens.TT) != 0 {
				l.Infof("%v Found %v tokens. Cleaning up.", _rebalanceJanitor, len(rtokens.TT))
				err := m.cleanupTransferTokens(rtokens.TT)
				if err != nil {
					l.Errorf("%v Error Cleaning Transfer Tokens %v", _rebalanceJanitor, err)
				}
			}

			if rtokens != nil && rtokens.MT != nil {
				cfg := m.config.Load()
				nodeID := cfg["nodeuuid"].String()
				if rtokens.MT.Error != "" && rtokens.MT.MasterId == nodeID { // let janitor in master node clean-up the move token
					l.Infof("%v Found erroneous MoveIndexToken: %v. Cleaning up.", _rebalanceJanitor, rtokens.MT)
					err := c.MetakvDel(MoveIndexTokenPath)
					if err != nil {
						l.Errorf("%v Unable to delete MoveIndexToken from Meta Storage. %v. Err %v", _rebalanceJanitor, rtokens.RT, err)
					}
				}
			}
		}
		c.TraceRWMutexUNLOCK(lockTime, c.LOCK_WRITE, m.svcMgrMu, "svcMgrMu", _rebalanceJanitor, "")

	}

}

// initStartPhase is a helper for startRebalance (master node only). It generates and
// registers the rebalance token. If there was a problem registering the global token
// it returns true as its second return value, indicating the rebalance should be skipped.
func (m *RebalanceServiceManager) initStartPhase(
	change service.TopologyChange) (err error, skipRebalance bool) {

	err = func() error {
		m.genericMgr.cinfo.Lock()
		defer m.genericMgr.cinfo.Unlock()
		return m.genericMgr.cinfo.Fetch() // can be very slow; do here so multiple children don't need to
	}()
	if err != nil {
		return err, true
	}

	var masterIP string // real IP address of this node (Rebal master), so other nodes can reach it
	masterIP, err = func() (string, error) {
		m.genericMgr.cinfo.RLock()
		defer m.genericMgr.cinfo.RUnlock()
		return m.genericMgr.cinfo.GetLocalHostname()
	}()
	if err != nil {
		return err, true
	}
	m.rebalanceToken = m.genRebalanceToken(masterIP)

	if err = m.registerLocalRebalanceToken(m.rebalanceToken); err != nil {
		return err, true
	}

	if err = m.registerRebalanceTokenInMetakv(m.rebalanceToken); err != nil {
		return err, true
	}

	if err, skipRebalance = m.registerGlobalRebalanceToken(m.rebalanceToken, change); err != nil {
		return err, skipRebalance
	}

	return nil, skipRebalance
}

// genRebalanceToken creates and returns a new rebalance token. masterIP must be the true IP of this
// node, not 127.0.0.1 from m.localhttp, so other nodes can find the Rebalance master.
func (m *RebalanceServiceManager) genRebalanceToken(masterIP string) *RebalanceToken {
	cfg := m.config.Load()
	return &RebalanceToken{
		MasterId: cfg["nodeuuid"].String(),
		RebalId:  m.getStateRebalanceID(),
		Source:   RebalSourceClusterOp,
		MasterIP: masterIP,
	}
}

// registerLocalRebalanceToken registers rebalToken in local metadata storage.
func (m *RebalanceServiceManager) registerLocalRebalanceToken(rebalToken *RebalanceToken) error {
	const method = "RebalanceServiceManager::registerLocalRebalanceToken:" // for logging

	rtoken, err := json.Marshal(rebalToken)
	if err != nil {
		return err
	}

	respch := make(MsgChannel)
	m.supvMsgch <- &MsgClustMgrLocal{
		mType:  CLUST_MGR_SET_LOCAL,
		key:    RebalanceTokenTag,
		value:  string(rtoken),
		respch: respch,
	}

	respMsg := <-respch
	resp := respMsg.(*MsgClustMgrLocal)

	errMsg := resp.GetError()
	if errMsg != nil {
		l.Errorf("%v Unable to set RebalanceToken In Local Meta Storage. Err %v",
			method, errMsg)
		return err
	}
	l.Infof("%v Registered Rebalance Token In Local Meta %v", method, rebalToken)

	return nil
}

// registerRebalanceTokenInMetakv registers rebalToken in the Metakv repository.
func (m *RebalanceServiceManager) registerRebalanceTokenInMetakv(rebalToken *RebalanceToken) error {
	const method = "RebalanceServiceManager::registerRebalanceTokenInMetakv:" // for logging

	err := c.MetakvSet(RebalanceTokenPath, rebalToken)
	if err != nil {
		l.Errorf("%v Unable to set RebalanceToken In Metakv Storage. Err %v", method, err)
		return err
	}
	l.Infof("%v Registered Global Rebalance Token In Metakv %v", method, rebalToken)

	return nil
}

// registerGlobalRebalanceToken registers rebalToken with all remote Index nodes by calling REST API
// /registerRebalanceToken, which is handled by handleRegisterRebalanceToken in this class. This may
// return skipRebalance == true even if returning err == nil, for "non-error" failure cases. Parent
// should have called cinfo.Fetch already so this function does not unless first iteration fails.
func (m *RebalanceServiceManager) registerGlobalRebalanceToken(rebalToken *RebalanceToken,
	change service.TopologyChange) (err error, skipRebalance bool) {
	const method = "RebalanceServiceManager::registerGlobalRebalanceToken" // for logging

	numKnownNodes := len(change.KeepNodes) + len(change.EjectNodes)

	m.genericMgr.cinfo.Lock()
	defer m.genericMgr.cinfo.Unlock()

	var nids []c.NodeId
	valid := false
	const maxRetry = 10
	for i := 0; i <= maxRetry; i++ {
		nids = m.genericMgr.cinfo.GetNodeIdsByServiceType(c.INDEX_HTTP_SERVICE)
		if len(nids) != numKnownNodes { // invalid case
			if isSingleNodeRebal(change) {
				l.Infof("%v ClusterInfo Node List doesn't match Known Nodes in Rebalance"+
					" Request. Skip Rebalance. cinfo.nodes : %v, change : %v",
					method, m.genericMgr.cinfo.Nodes(), change)
				return nil, true
			}

			err = errors.New("ClusterInfo Node List doesn't match Known Nodes in Rebalance Request")
			l.Errorf("%v err: %v, nids: %v, allKnownNodes: %v. Retrying %v",
				method, err, nids, numKnownNodes, i)

			if i+1 <= maxRetry { // will retry
				time.Sleep(2 * time.Second)

				// cinfo.Fetch has its own internal retries
				if err = m.genericMgr.cinfo.Fetch(); err != nil {
					l.Errorf("%v Error on iter %v fetching cluster information: %v", method, i, err)
				}
			}

			continue // maxRetry loop
		} // if incorrect number of nodes
		valid = true
		break
	}

	if !valid {
		l.Errorf("%v cinfo.nodes: %v, change: %v", method, m.genericMgr.cinfo.Nodes(), change)
		return err, true
	}

	url := "/registerRebalanceToken"
	for _, nid := range nids {

		addr, err := m.genericMgr.cinfo.GetServiceAddress(nid, c.INDEX_HTTP_SERVICE, true)
		if err == nil {

			localaddr, err := m.genericMgr.cinfo.GetLocalServiceAddress(c.INDEX_HTTP_SERVICE, true)
			if err != nil {
				l.Errorf("%v Error Fetching Local Service Address %v", method, err)
				return errors.New(fmt.Sprintf("Fail to retrieve http endpoint for local node %v", err)), true
			}

			if addr == localaddr {
				l.Infof("%v Skip local service %v", method, addr)
				continue
			}

			body, err := json.Marshal(&rebalToken)
			if err != nil {
				l.Errorf("%v Error registering rebalance token on %v, err: %v",
					method, addr+url, err)
				return err, true
			}

			bodybuf := bytes.NewBuffer(body)

			resp, err := postWithAuth(addr+url, "application/json", bodybuf)
			if err != nil {
				l.Errorf("%v Error registering rebalance token on %v, err: %v",
					method, addr+url, err)
				return err, true
			}
			ioutil.ReadAll(resp.Body)
			resp.Body.Close()
		} else {
			l.Errorf("%v Error Fetching Service Address %v", method, err)
			return errors.New(fmt.Sprintf("Fail to retrieve http endpoint for index node %v", err)), true
		}

		l.Infof("%v Successfully registered rebalance token on %v", method, addr+url)
	}

	return nil, false
}

func (m *RebalanceServiceManager) observeGlobalRebalanceToken(rebalToken RebalanceToken) bool {

	cfg := m.config.Load()
	globalTokenWaitTimeout := cfg["rebalance.globalTokenWaitTimeout"].Int()

	elapsed := 1

	for elapsed < globalTokenWaitTimeout {

		var rtoken RebalanceToken
		found, err := c.MetakvGet(RebalanceTokenPath, &rtoken)
		if err != nil {
			l.Errorf("RebalanceServiceManager::observeGlobalRebalanceToken Error Checking Rebalance Token In Metakv %v", err)
			continue
		}

		if found {
			if reflect.DeepEqual(rtoken, rebalToken) {
				l.Infof("RebalanceServiceManager::observeGlobalRebalanceToken Global And Local Rebalance Token Match %v", rtoken)
				return true
			} else {
				l.Errorf("RebalanceServiceManager::observeGlobalRebalanceToken Mismatch in Global and Local Rebalance Token. Global %v. Local %v.", rtoken, rebalToken)
				return false
			}
		}

		l.Infof("RebalanceServiceManager::observeGlobalRebalanceToken Waiting for Global Rebalance Token In Metakv")
		time.Sleep(time.Second * time.Duration(1))
		elapsed += 1
	}

	l.Errorf("RebalanceServiceManager::observeGlobalRebalanceToken Timeout Waiting for Global Rebalance Token In Metakv")

	return false

}

// runRebalanceCallback is a callback used by Rebalancer used for all of:
//  1. Rebalance progress
//  2. Rebalance done
//  3. MoveIndex done
func (m *RebalanceServiceManager) runRebalanceCallback(cancel <-chan struct{}, body func()) {
	done := make(chan struct{})
	const method = "RebalanceServiceManager::runRebalanceCallback:" // for logging

	go func() {
		lockTime := c.TraceRWMutexLOCK(c.LOCK_WRITE, m.svcMgrMu, "svcMgrMu", method, "")
		defer c.TraceRWMutexUNLOCK(lockTime, c.LOCK_WRITE, m.svcMgrMu, "svcMgrMu", method, "")

		select {
		case <-cancel:
			break
		default:
			body()
		}

		close(done)
	}()

	select {
	case <-done:
	case <-cancel:
	}
}

// rebalanceProgressCallback is the Rebalancer.cb.progress callback function for a Rebalance.
func (m *RebalanceServiceManager) rebalanceProgressCallback(progress float64, cancel <-chan struct{}) {
	m.runRebalanceCallback(cancel, func() {
		m.updateRebalanceProgressLOCKED(progress)
	})
}

// updateRebalanceProgressLOCKED caller should be holding mutex svcMgrMu write locked.
func (m *RebalanceServiceManager) updateRebalanceProgressLOCKED(progress float64) {
	rev := m.rebalanceCtx.incRev()
	changeID := m.rebalanceCtx.change.ID
	task := &service.Task{
		Rev:          EncodeRev(rev),
		ID:           fmt.Sprintf("rebalance/%s", changeID),
		Type:         service.TaskTypeRebalance,
		Status:       service.TaskStatusRunning,
		IsCancelable: true,
		Progress:     progress,

		Extra: map[string]interface{}{
			"rebalanceId": changeID,
		},
	}

	m.updateState(func(s *state) {
		s.rebalanceTask = task
	})
}

// failoverDoneCallback is the Rebalancer.cb.done callback function for Failovers.
func (m *RebalanceServiceManager) failoverDoneCallback(err error, cancel <-chan struct{}) {
	m.rebalanceOrFailoverDoneCallback(err, cancel, true)
}

// rebalanceDoneCallback is the Rebalancer.cb.done callback function for Rebalances.
func (m *RebalanceServiceManager) rebalanceDoneCallback(err error, cancel <-chan struct{}) {
	m.rebalanceOrFailoverDoneCallback(err, cancel, false)
}

// rebalanceOrFailoverDoneCallback is a delegate for both failoverDoneCallback and
// rebalanceDoneCallback. isFailover arg distinguishes between these.
func (m *RebalanceServiceManager) rebalanceOrFailoverDoneCallback(err error, cancel <-chan struct{}, isFailover bool) {

	m.runRebalanceCallback(cancel, func() {

		if m.rebalancer != nil {
			change := &m.rebalanceCtx.change
			defer func() {
				go notifyRebalanceDone(change, err != nil)
			}()
		}

		m.onRebalanceDoneLOCKED(err, isFailover)
	})
}

// onRebalanceDoneLOCKED is invoked when a Rebalance or Failover completes (succeeds or fails).
// Non-nil err means the rebalance failed.
// forceUnbalanced flag forces isBalanced = false (for failovers and cancels).
// Caller should be holding mutex svcMgrMu write locked.
func (m *RebalanceServiceManager) onRebalanceDoneLOCKED(err error, forceUnbalanced bool) {
	isMaster := m.rebalancer != nil
	isBalancedNew := !forceUnbalanced // new isBalanced state to set; below should only change this to false
	if isMaster {
		newTask := (*service.Task)(nil) // no task if succeeded
		if err != nil {
			isBalancedNew = false
			ctx := m.rebalanceCtx
			rev := ctx.incRev()

			// Failed rebalance task
			newTask = &service.Task{
				Rev:          EncodeRev(rev),
				ID:           fmt.Sprintf("rebalance/%s", ctx.change.ID),
				Type:         service.TaskTypeRebalance,
				Status:       service.TaskStatusFailed,
				IsCancelable: true,

				ErrorMessage: err.Error(),

				Extra: map[string]interface{}{
					"rebalanceId": ctx.change.ID,
				},
			}
		}

		if !m.cleanupPending && m.runCleanupPhaseLOCKED(RebalanceTokenPath, isMaster) != nil {
			isBalancedNew = false
		}

		m.rebalanceCtx = nil

		m.updateState(func(s *state) {
			s.rebalanceTask = newTask
			s.rebalanceID = ""
		})
	} else if m.runCleanupPhaseLOCKED(RebalanceTokenPath, isMaster) != nil {
		isBalancedNew = false
	}

	m.setStateIsBalanced(isBalancedNew) // set new isBalanced state
	m.rebalancer = nil
	m.rebalancerF = nil
	l.Infof("RebalanceServiceManager::onRebalanceDoneLOCKED Rebalance Done: "+
		"isBalanced %v, isMaster %v, forceUnbalanced %v, err: %v",
		isBalancedNew, isMaster, forceUnbalanced, err)
}

// notifyWaiters sends the current (new) state to all state waiters.
func (m *RebalanceServiceManager) notifyWaiters() {
	currState := m.copyState()

	m.waitersMu.Lock()
	for ch := range m.waiters {
		if ch != nil {
			ch <- currState
		}
	}
	m.waiters = make(waiters) // reinit to the empty set
	m.waitersMu.Unlock()
}

// addWaiter creates and returns a state notification channel of type
// waiter (for the current go routine) and adds it to the waiters list.
func (m *RebalanceServiceManager) addWaiter() waiter {
	ch := make(waiter, 1)

	m.waitersMu.Lock()
	m.waiters[ch] = struct{}{}
	m.waitersMu.Unlock()

	return ch
}

// removeWaiter removes the channel of type waiter (for the current
// go routine) from the waiters list.
func (m *RebalanceServiceManager) removeWaiter(w waiter) {
	m.waitersMu.Lock()
	delete(m.waiters, w)
	m.waitersMu.Unlock()
}

// copyState retuns a shallow copy of the m.state object.
func (m *RebalanceServiceManager) copyState() state {
	m.stateMu.RLock()
	defer m.stateMu.RUnlock()
	return m.state
}

// updateState executes a caller-supplied function that makes arbitrary updates to
// to the m.state object, then increments the state revision # and notifies waiters.
func (m *RebalanceServiceManager) updateState(body func(state *state)) {
	m.stateMu.Lock()
	body(&m.state)
	m.state.revCopy = m.genericMgr.incRev()
	m.stateMu.Unlock()

	m.notifyWaiters()
}

// getStateIsBalanced gets the m.state.isBalanced flag.
func (m *RebalanceServiceManager) getStateIsBalanced() bool {
	m.stateMu.RLock()
	defer m.stateMu.RUnlock()
	return m.state.isBalanced
}

// setStateIsBalanced sets the m.state.isBalanced flag to the value passed in.
func (m *RebalanceServiceManager) setStateIsBalanced(isBal bool) {
	m.stateMu.Lock()
	m.state.isBalanced = isBal
	m.stateMu.Unlock()
}

// getStateRebalanceID gets the m.state.rebalanceID string.
func (m *RebalanceServiceManager) getStateRebalanceID() string {
	m.stateMu.RLock()
	defer m.stateMu.RUnlock()
	return m.state.rebalanceID
}

// getStateServers gets the m.state.servers slice.
func (m *RebalanceServiceManager) getStateServers() []service.NodeID {
	m.stateMu.RLock()
	defer m.stateMu.RUnlock()
	return m.state.servers
}

// wait returns the current state immediately if it is newer than what the
// caller (ultimately ns_server via GetTaskList or GetTopologyChange APIs)
// already has or if caller passed a nil rev. Otherwise it waits until the
// revision changes and returns the new state. ns_server's cbauth API stub
// may cancel the wait before that happens by closing the cancel channel, in
// which case this returns a zero-value state structure and an ErrCanceled
// error. cbauth will then send a nil-rev follow-up call immediately. Note
// that this follow-up call will not be sent until indexer first replies to
// the cancel.
func (m *RebalanceServiceManager) wait(rev service.Revision,
	cancel service.Cancel) (state, error) {

	currState := m.copyState()

	if rev == nil {
		return currState, nil
	}

	haveRev := DecodeRev(rev)
	if haveRev != currState.revCopy {
		return currState, nil
	}

	// Caller has current revision so wait for the next one before replying
	ch := m.addWaiter()

	select {
	case <-cancel:
		m.removeWaiter(ch)
		return newState(), service.ErrCanceled
	case newState := <-ch:
		return newState, nil
	}
}

// cancelActualTask cancels a PrepareTopologyChange or StartTopologyChange.
func (m *RebalanceServiceManager) cancelActualTask(task *service.Task) error {
	const method = "RebalanceServiceManager::cancelActualTask:" // for logging

	lockTime := c.TraceRWMutexLOCK(c.LOCK_WRITE, m.svcMgrMu, "svcMgrMu", method, "")
	defer c.TraceRWMutexUNLOCK(lockTime, c.LOCK_WRITE, m.svcMgrMu, "svcMgrMu", method, "")

	if m.rebalancer != nil {
		change := &m.rebalanceCtx.change
		defer func() {
			go notifyRebalanceDone(change, true)
		}()
	}

	switch task.Type {
	case service.TaskTypePrepared:
		return m.cancelPrepareTaskLOCKED()
	case service.TaskTypeRebalance:
		return m.cancelRebalanceTaskLOCKED(task)
	default:
		panic("can't happen")
	}
}

// cancelPrepareTaskLOCKED caller should be holding mutex svcMgrMu write locked.
func (m *RebalanceServiceManager) cancelPrepareTaskLOCKED() error {
	if m.rebalancer != nil {
		return service.ErrConflict
	}

	if m.monitorStopCh != nil {
		close(m.monitorStopCh)
		m.monitorStopCh = nil
	}

	m.cleanupRebalanceRunning()

	m.updateState(func(s *state) {
		s.rebalanceID = ""
	})

	return nil
}

// cancelRebalanceTaskLOCKED caller should be holding mutex svcMgrMu write locked.
func (m *RebalanceServiceManager) cancelRebalanceTaskLOCKED(task *service.Task) error {
	switch task.Status {
	case service.TaskStatusRunning:
		return m.cancelRunningRebalanceTaskLOCKED()
	case service.TaskStatusFailed:
		return m.cancelFailedRebalanceTask()
	default:
		panic("can't happen")
	}
}

// cancelRunningRebalanceTaskLOCKED cancels a currently running rebalance.
// Caller should be holding mutex svcMgrMu write locked.
func (m *RebalanceServiceManager) cancelRunningRebalanceTaskLOCKED() error {
	m.rebalancer.Cancel()
	m.onRebalanceDoneLOCKED(nil, true)
	return nil
}

// cancelFailedRebalanceTask cancels a failed rebalance.
func (m *RebalanceServiceManager) cancelFailedRebalanceTask() error {
	m.updateState(func(s *state) {
		s.rebalanceTask = nil
	})
	return nil
}

// stateToTopology formats the state into a Topology as needed in GetCurrentTopology response. If
// there are no indexer NodeUUIDs in the state's servers field, this always includes the NodeUUID of
// the current node.
func (m *RebalanceServiceManager) stateToTopology(s state) *service.Topology {
	topology := &service.Topology{}

	topology.Rev = EncodeRev(s.revCopy)
	if s.servers != nil && len(s.servers) != 0 {
		topology.Nodes = append([]service.NodeID(nil), s.servers...)
	} else {
		topology.Nodes = append([]service.NodeID(nil), m.nodeInfo.NodeID)
	}
	topology.IsBalanced = s.isBalanced
	topology.Messages = nil

	return topology
}

// stateToTaskList formats the state into a TaskList as needed in GetTaskList response.
func stateToTaskList(s state) *service.TaskList {
	tasks := &service.TaskList{}

	tasks.Rev = EncodeRev(s.revCopy)
	tasks.Tasks = make([]service.Task, 0)

	if s.rebalanceID != "" {
		id := s.rebalanceID

		task := service.Task{
			Rev:          EncodeRev(0),
			ID:           fmt.Sprintf("prepare/%s", id),
			Type:         service.TaskTypePrepared,
			Status:       service.TaskStatusRunning,
			IsCancelable: true,

			Extra: map[string]interface{}{
				"rebalanceId": id,
			},
		}

		tasks.Tasks = append(tasks.Tasks, task)
	}

	if s.rebalanceTask != nil {
		tasks.Tasks = append(tasks.Tasks, *s.rebalanceTask)
	}

	return tasks
}

// recoverRebalance is called only during bootstrap to clean up any prior
// failed rebalances or index moves.
func (m *RebalanceServiceManager) recoverRebalance() {
	const method = "RebalanceServiceManager::recoverRebalance:" // for logging

	lockTime := c.TraceRWMutexLOCK(c.LOCK_WRITE, m.svcMgrMu, "svcMgrMu", method, "")
	defer c.TraceRWMutexUNLOCK(lockTime, c.LOCK_WRITE, m.svcMgrMu, "svcMgrMu", method, "")

	m.indexerReady = true

	if m.cleanupPending {
		l.Infof("%v Init Pending Cleanup", method)

		rtokens, err := m.getCurrRebalTokens()
		if err != nil {
			l.Errorf("%v Error Fetching Metakv Tokens %v", method, err)
			c.CrashOnError(err)
		}

		if rtokens != nil {
			if rtokens.RT != nil {
				m.doRecoverRebalance(rtokens.RT)
			}
			if rtokens.MT != nil {
				m.doRecoverMoveIndex(rtokens.MT)
			}
		}

		if m.rebalanceRunning {
			m.runCleanupPhaseLOCKED(RebalanceTokenPath, false)
		}
	}

	m.cleanupPending = false

	go m.listenMoveIndex()
	go m.rebalanceJanitor()

}

func (m *RebalanceServiceManager) doRecoverRebalance(gtoken *RebalanceToken) {

	l.Infof("RebalanceServiceManager::doRecoverRebalance Found Global Rebalance Token %v", gtoken)

	if gtoken.MasterId == string(m.nodeInfo.NodeID) {
		m.runCleanupPhaseLOCKED(RebalanceTokenPath, true)
	} else {
		m.runCleanupPhaseLOCKED(RebalanceTokenPath, false)
	}

}

func (m *RebalanceServiceManager) doRecoverMoveIndex(gtoken *RebalanceToken) {

	l.Infof("RebalanceServiceManager::doRecoverMoveIndex Found Global Rebalance Token %v.", gtoken)
	m.runCleanupPhaseLOCKED(MoveIndexTokenPath, true)
}

func (m *RebalanceServiceManager) checkLocalCleanupPending() bool {

	rtokens, err := m.getCurrRebalTokens()
	if err != nil {
		l.Errorf("RebalanceServiceManager::checkLocalCleanupPending Error Fetching Metakv Tokens %v", err)
		return true
	}

	if rtokens != nil && len(rtokens.TT) != 0 {
		for _, tt := range rtokens.TT {
			ownerId := m.getTransferTokenOwner(tt)
			if ownerId == string(m.nodeInfo.NodeID) {
				l.Infof("RebalanceServiceManager::checkLocalCleanupPending Found Local Pending Cleanup Token %v", tt)
				return true
			}
		}
	}

	return false
}

func (m *RebalanceServiceManager) checkGlobalCleanupPending() bool {

	rtokens, err := m.getCurrRebalTokens()
	if err != nil {
		l.Errorf("RebalanceServiceManager::checkGlobalCleanupPending Error Fetching Metakv Tokens %v", err)
		return true
	}

	servers := m.getStateServers()
	if rtokens != nil && len(rtokens.TT) != 0 {
		for _, tt := range rtokens.TT {
			ownerId := m.getTransferTokenOwner(tt)
			for _, s := range servers {
				if ownerId == string(s) {
					l.Infof("RebalanceServiceManager::checkGlobalCleanupPending Found Global Pending Cleanup for Owner %v Token %v", ownerId, tt)
					return true
				}
			}
			l.Infof("RebalanceServiceManager::checkGlobalCleanupPending Found Global Pending Cleanup Token Without Owner Token %v", tt)
		}
	}

	return false
}

// getTransferTokenOwner returns the node ID of the token's owning node. Owner
// defined for each state here should match those processed by/under rebalancer
// functions processTokenAsMaster, processTokenAsSource, and processTokenAsDest.
func (m *RebalanceServiceManager) getTransferTokenOwner(tt *c.TransferToken) string {

	switch tt.State {
	case c.TransferTokenReady:
		return tt.SourceId
	case c.TransferTokenCreated, c.TransferTokenAccepted, c.TransferTokenRefused,
		c.TransferTokenInitiate, c.TransferTokenInProgress, c.TransferTokenMerge:
		return tt.DestId
	case c.TransferTokenCommit, c.TransferTokenDeleted:
		return tt.MasterId
	}

	return ""

}

/////////////////////////////////////////////////////////////////////////
//
//  REST handlers
//
/////////////////////////////////////////////////////////////////////////

func (m *RebalanceServiceManager) handleListRebalanceTokens(w http.ResponseWriter, r *http.Request) {

	_, ok := m.validateAuth(w, r)
	if !ok {
		l.Errorf("RebalanceServiceManager::handleListRebalanceTokens Validation Failure req: %v", c.GetHTTPReqInfo(r))
		return
	}

	if r.Method == "GET" {

		l.Infof("RebalanceServiceManager::handleListRebalanceTokens Processing Request req: %v", c.GetHTTPReqInfo(r))
		rinfo, err := m.getCurrRebalTokens()
		if err != nil {
			l.Errorf("RebalanceServiceManager::handleListRebalanceTokens Error %v", err)
			m.writeError(w, err)
			return
		}
		out, err1 := json.Marshal(rinfo)
		if err1 != nil {
			l.Errorf("RebalanceServiceManager::handleListRebalanceTokens Error %v", err1)
			m.writeError(w, err1)
		} else {
			m.writeJson(w, out)
		}
	} else {
		m.writeError(w, errors.New("Unsupported method"))
		return
	}

}

func (m *RebalanceServiceManager) handleCleanupRebalance(w http.ResponseWriter, r *http.Request) {
	const method = "RebalanceServiceManager::handleCleanupRebalance:" // for logging

	_, ok := m.validateAuth(w, r)
	if !ok {
		l.Errorf("%v Validation Failure req: %v", method, c.GetHTTPReqInfo(r))
		return
	}

	if r.Method == "GET" || r.Method == "POST" {

		l.Infof("%v Processing Request req: %v", method, c.GetHTTPReqInfo(r))
		lockTime := c.TraceRWMutexLOCK(c.LOCK_WRITE, m.svcMgrMu, "svcMgrMu", method, "")
		defer c.TraceRWMutexUNLOCK(lockTime, c.LOCK_WRITE, m.svcMgrMu, "svcMgrMu", method, "")

		if !m.indexerReady {
			l.Errorf("%v Cannot Process Request %v", method, c.ErrIndexerInBootstrap)
			m.writeError(w, c.ErrIndexerInBootstrap)
			return
		}

		rtokens, err := m.getCurrRebalTokens()
		if err != nil {
			l.Errorf("%v Error %v", method, err)
		}

		if rtokens != nil {
			if rtokens.RT != nil {
				m.setStateIsBalanced(false)
				err = m.runCleanupPhaseLOCKED(RebalanceTokenPath, true)
				if err != nil {
					l.Errorf("%v RebalanceTokenPath Error %v", method, err)
				}
			}
			if rtokens.MT != nil {
				err = m.runCleanupPhaseLOCKED(MoveIndexTokenPath, true)
				if err != nil {
					l.Errorf("%v MoveIndexTokenPath Error %v", method, err)
				}
			}
		}

		//local cleanup
		err = m.runCleanupPhaseLOCKED(RebalanceTokenPath, false)

		if err != nil {
			l.Errorf("%v Error %v", method, err)
			m.writeError(w, err)
		} else {
			m.writeOk(w)
		}

	} else {
		m.writeError(w, errors.New("Unsupported method"))
	}
}

func (m *RebalanceServiceManager) handleNodeuuid(w http.ResponseWriter, r *http.Request) {

	_, ok := m.validateAuth(w, r)
	if !ok {
		l.Errorf("RebalanceServiceManager::handleNodeuuid Validation Failure req: %v", c.GetHTTPReqInfo(r))
		return
	}

	if r.Method == "GET" || r.Method == "POST" {
		l.Infof("RebalanceServiceManager::handleNodeuuid Processing Request req: %v", c.GetHTTPReqInfo(r))
		m.writeBytes(w, []byte(m.nodeInfo.NodeID))
	} else {
		m.writeError(w, errors.New("Unsupported method"))
	}
}

func (m *RebalanceServiceManager) getCurrRebalTokens() (*RebalTokens, error) {

	metainfo, err := metakv.ListAllChildren(RebalanceMetakvDir)
	if err != nil {
		return nil, err
	}

	if len(metainfo) == 0 {
		return nil, nil
	}

	var rinfo RebalTokens
	rinfo.TT = make(map[string]*c.TransferToken)

	for _, kv := range metainfo {

		var rt, mt RebalanceToken
		if strings.Contains(kv.Path, RebalanceTokenTag) {
			json.Unmarshal(kv.Value, &rt)
			rinfo.RT = &rt

		} else if strings.Contains(kv.Path, MoveIndexTokenTag) {
			json.Unmarshal(kv.Value, &mt)
			rinfo.MT = &mt

		} else if strings.Contains(kv.Path, TransferTokenTag) {
			ttidpos := strings.Index(kv.Path, TransferTokenTag)
			ttid := kv.Path[ttidpos:]

			var tt c.TransferToken
			json.Unmarshal(kv.Value, &tt)
			rinfo.TT[ttid] = &tt
		} else {
			l.Errorf("RebalanceServiceManager::getCurrRebalTokens Unknown Token %v. Ignored.", kv)
		}

	}

	return &rinfo, nil
}

// handleRegisterRebalanceToken handles REST API /registerRebalanceToken used to register a
// rebalance token on Rebalance follower Index nodes.
func (m *RebalanceServiceManager) handleRegisterRebalanceToken(w http.ResponseWriter, r *http.Request) {
	const method = "RebalanceServiceManager::handleRegisterRebalanceToken:" // for logging

	_, ok := m.validateAuth(w, r)
	if !ok {
		l.Errorf("%v Validation Failure req: %v", method, c.GetHTTPReqInfo(r))
		return
	}

	var rebalToken RebalanceToken
	if r.Method == "POST" {
		bytes, _ := ioutil.ReadAll(r.Body)
		if err := json.Unmarshal(bytes, &rebalToken); err != nil {
			l.Errorf("%v %v", method, err)
			m.writeError(w, err)
			return
		}

		l.Infof("%v New Rebalance Token %v", method, rebalToken)

		if m.observeGlobalRebalanceToken(rebalToken) {

			lockTime := c.TraceRWMutexLOCK(c.LOCK_WRITE, m.svcMgrMu, "svcMgrMu", method, "")
			defer c.TraceRWMutexUNLOCK(lockTime, c.LOCK_WRITE, m.svcMgrMu, "svcMgrMu", method, "")

			if !m.rebalanceRunning {
				errStr := fmt.Sprintf("Node %v not in Prepared State for Rebalance", string(m.nodeInfo.NodeID))
				l.Errorf("%v %v", method, errStr)
				m.writeError(w, errors.New(errStr))
				return
			}

			m.rebalanceToken = &rebalToken
			if err := m.registerLocalRebalanceToken(m.rebalanceToken); err != nil {
				l.Errorf("%v %v", method, err)
				m.writeError(w, err)
				return
			}

			// TODO: Add a cluster version check
			cfg := m.config.Load()
			isShardAwareRebalance := cfg["rebalance.shard_aware_rebalance"].Bool()

			if isShardAwareRebalance {
				m.rebalancerF = NewShardRebalancer(nil, m.rebalanceToken, string(m.nodeInfo.NodeID),
					false, nil, m.rebalanceDoneCallback, m.supvMsgch,
					m.localhttp, m.config.Load(), nil, false, &m.p,
					m.genericMgr.statsMgr)
			} else {
				m.rebalancerF = NewRebalancer(nil, m.rebalanceToken, string(m.nodeInfo.NodeID),
					false, nil, m.rebalanceDoneCallback, m.supvMsgch,
					m.localhttp, m.config.Load(), nil, false, &m.p,
					m.genericMgr.statsMgr)
			}

			m.writeOk(w)
			return

		} else {
			err := errors.New("Rebalance Token Wait Timeout")
			l.Errorf("%v %v", method, err)
			m.writeError(w, err)
			return
		}

	} else {
		m.writeError(w, errors.New("Unsupported method"))
		return
	}
}

// GetGlobalTopology does a scatter-gather to all available Index Service nodes to get their full
// index topologies. addr is the local node's Index Service HTTP address, e.g. "127.0.0.1:9102".
func GetGlobalTopology(addr string) (*manager.ClusterIndexMetadata, error) {
	const _GetGlobalTopology = "RebalanceServiceManager::GetGlobalTopology:"
	url := "/getIndexMetadata"

	resp, err := getWithAuth(addr + url)
	if err != nil {
		l.Errorf("%v %v returned err: %v", _GetGlobalTopology, addr+url, err)
		return nil, err
	}
	defer resp.Body.Close()

	bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		l.Errorf("%v ReadAll returned err: %v", _GetGlobalTopology, err)
		return nil, err
	}

	var topology BackupResponse
	if err := json.Unmarshal(bytes, &topology); err != nil {
		l.Errorf("%v Unmarshaling response from %v returned err: %v %s", _GetGlobalTopology,
			addr+url, err, l.TagUD(string(bytes)))
		return nil, err
	}

	if topology.Error != "" {
		l.Errorf("%v getIndexMetadata response reported err: %v", _GetGlobalTopology, err)
		return nil, err
	}

	return &topology.Result, nil
}

// getCachedIndexerNodeUUIDs is called at boot to get the persistently cached NodeUUIDs of all Index
// Service nodes to respond to ns_server GetCurrentTopology RPC API requests. This cannot use a
// ClusterInfoCache-based scatter-gather because this and potentially other indexer nodes that are
// not fully up yet would not be included. Rare: if the cache is stale, a Rebalance will fix things.
func (m *RebalanceServiceManager) getCachedIndexerNodeUUIDs() (
	nodeUUIDs []service.NodeID, err error) {
	const _getCachedIndexerNodeUUIDs = "RebalanceServiceManager::getCachedIndexerNodeUUIDs:"

	addr := m.localhttp
	url := "/getCachedIndexerNodeUUIDs"

	resp, err := getWithAuth(addr + url)
	if err != nil {
		l.Errorf("%v %v returned err: %v", _getCachedIndexerNodeUUIDs, addr+url, err)
		return nil, err
	}
	defer resp.Body.Close()
	bytes, _ := ioutil.ReadAll(resp.Body)
	nodeUUIDsResponse := new(NodeUUIDsResponse)
	if err = json.Unmarshal(bytes, &nodeUUIDsResponse); err != nil {
		l.Errorf("%v Unmarshaling response from %v returned err: %v %s", _getCachedIndexerNodeUUIDs,
			url, err, l.TagUD(string(bytes)))
		return nil, err
	}

	// Ensure we always include our own NodeUUID if it was missing from cache (extreme edge case)
	nodeUUIDs = nodeUUIDsResponse.NodeUUIDs
	foundLocalNodeUUID := false
	for _, nodeUUID := range nodeUUIDs {
		if nodeUUID == m.nodeInfo.NodeID {
			foundLocalNodeUUID = true
			break
		}
	}
	if !foundLocalNodeUUID {
		nodeUUIDs = append(nodeUUIDs, m.nodeInfo.NodeID)
	}

	l.Infof("%v Returning %v NodeUUIDs: %v", _getCachedIndexerNodeUUIDs, len(nodeUUIDs), nodeUUIDs)
	return nodeUUIDs, nil
}

/////////////////////////////////////////////////////////////////////////
//
//  move index implementation
//
/////////////////////////////////////////////////////////////////////////

func (m *RebalanceServiceManager) listenMoveIndex() {

	l.Infof("RebalanceServiceManager::listenMoveIndex %v", m.nodeInfo)

	cancel := make(chan struct{})
	for {
		err := metakv.RunObserveChildren(RebalanceMetakvDir, m.processMoveIndex, cancel)
		if err != nil {
			l.Infof("RebalanceServiceManager::listenMoveIndex metakv err %v. Retrying...", err)
			time.Sleep(2 * time.Second)
		}
	}

}

func (m *RebalanceServiceManager) processMoveIndex(kve metakv.KVEntry) error {
	if kve.Path == MoveIndexTokenPath {
		const method = "RebalanceServiceManager::processMoveIndex:" // for logging

		l.Infof("%v MoveIndexToken Received %v %s", method, kve.Path, kve.Value)

		var rebalToken RebalanceToken
		if kve.Value == nil { //move index token deleted
			return nil
		} else {
			if err := json.Unmarshal(kve.Value, &rebalToken); err != nil {
				l.Errorf("%v Error reading move index token %v", method, err)
				return err
			}
		}

		lockTime := c.TraceRWMutexLOCK(c.LOCK_WRITE, m.svcMgrMu, "svcMgrMu", method, "")
		defer c.TraceRWMutexUNLOCK(lockTime, c.LOCK_WRITE, m.svcMgrMu, "svcMgrMu", method, "")

		//skip if token was generated by self
		if rebalToken.MasterId == string(m.nodeInfo.NodeID) {

			// if rebalanceToken matches
			if m.rebalanceToken != nil && m.rebalanceToken.RebalId == rebalToken.RebalId {

				// If master (source) receving a token with error, then cancel move index
				// and return the error to user.
				if len(rebalToken.Error) != 0 {
					l.Infof("%v received error from destination", method)

					if m.rebalancer != nil {
						m.rebalancer.Cancel()
						m.rebalancer = nil
						m.moveStatusCh <- errors.New(rebalToken.Error)
						m.runCleanupPhaseLOCKED(MoveIndexTokenPath, true)
					}
					return nil
				}
			}

			l.Infof("%v Skip MoveIndex Token for Self Node", method)
			return nil

		} else {
			// If destination receving a token with error, then skip.  The destination is
			// the one that posted the error.
			if len(rebalToken.Error) != 0 {
				l.Infof("%v Skip MoveIndex Token with error", method)
				return nil
			}
		}

		if m.rebalanceRunning {
			err := errors.New("Cannot Process Move Index - Rebalance In Progress")
			l.Errorf("%v %v %v", method, err, m.rebalanceToken)
			m.setErrorInMoveIndexToken(&rebalToken, err)
			return nil
		} else {
			m.rebalanceRunning = true
			m.rebalanceToken = &rebalToken
			var err error
			if err = m.registerRebalanceRunning(true); err != nil || m.p.ddlRunning {
				if m.p.ddlRunning {
					l.Errorf("%v Found index build running. Cannot process move index.", method)
					fmtMsg := "move index failure - index build is in progress for indexes: %v."
					err = errors.New(fmt.Sprintf(fmtMsg, m.p.ddlRunningIndexNames))
				}
				m.setErrorInMoveIndexToken(m.rebalanceToken, err)
				m.runCleanupPhaseLOCKED(MoveIndexTokenPath, false)
				return nil
			}

			if err = m.registerLocalRebalanceToken(m.rebalanceToken); err != nil {
				m.setErrorInMoveIndexToken(m.rebalanceToken, err)
				m.runCleanupPhaseLOCKED(MoveIndexTokenPath, false)
				return nil
			}
			m.rebalancerF = NewRebalancer(nil, m.rebalanceToken, string(m.nodeInfo.NodeID),
				false, nil, m.moveIndexDoneCallback, m.supvMsgch,
				m.localhttp, m.config.Load(), nil, false, nil, m.genericMgr.statsMgr)
		}
	}

	return nil
}

func (m *RebalanceServiceManager) handleMoveIndex(w http.ResponseWriter, r *http.Request) {
	const method = "RebalanceServiceManager::handleMoveIndex" // for logging

	creds, ok := m.validateAuth(w, r)
	if !ok {
		l.Errorf("%v: Validation Failure req: %v", method, c.GetHTTPReqInfo(r))
		return
	}

	if r.Method == "POST" {

		bytes, _ := ioutil.ReadAll(r.Body)
		in := make(map[string]interface{})
		if err := json.Unmarshal(bytes, &in); err != nil {
			send(http.StatusBadRequest, w, err.Error())
			return
		}

		var bucket, scope, collection, index string
		var ok bool
		var nodes interface{}

		if bucket, ok = in["bucket"].(string); !ok {
			send(http.StatusBadRequest, w, "Bad Request - Bucket Information Missing")
			return
		}

		if scope, ok = in["scope"].(string); !ok {
			// TODO (Collections): Should this be made a mandatory parameter?
			// default the scope
			scope = c.DEFAULT_SCOPE
		}

		if collection, ok = in["collection"].(string); !ok {
			// TODO (Collections): Should this be made a mandatory parameter?
			// default the collection
			collection = c.DEFAULT_COLLECTION
		}

		if index, ok = in["index"].(string); !ok {
			send(http.StatusBadRequest, w, "Bad Request - Index Information Missing")
			return
		}

		if nodes, ok = in["nodes"]; !ok {
			send(http.StatusBadRequest, w, "Bad Request - Nodes Information Missing")
			return
		}

		if c.GetBuildMode() != c.ENTERPRISE {
			send(http.StatusBadRequest, w, "Alter index is only supported in Enterprise Edition")
			return
		}

		permission := fmt.Sprintf("cluster.collection[%s:%s:%s].n1ql.index!alter", bucket, scope, collection)
		if !c.IsAllowed(creds, []string{permission}, r, w, method) {
			return
		}

		topology, err := GetGlobalTopology(m.localhttp)
		if err != nil {
			send(http.StatusInternalServerError, w, err.Error())
			return
		}

		var defn *manager.IndexDefnDistribution
		for _, localMeta := range topology.Metadata {
			bTopology := findTopologyByCollection(localMeta.IndexTopologies, bucket, scope, collection)
			if bTopology != nil {
				defn = bTopology.FindIndexDefinition(bucket, scope, collection, index)
			}
			if defn != nil {
				break
			}
		}
		if defn == nil {
			err := errors.New(fmt.Sprintf("Fail to find index definition for bucket %v index %v.", bucket, index))
			l.Errorf("%v: %v", method, err)
			send(http.StatusInternalServerError, w, err.Error())
			return
		}

		var req IndexRequest

		idList := client.IndexIdList{DefnIds: []uint64{defn.DefnId}}
		plan := make(map[string]interface{})
		plan["nodes"] = nodes

		req = IndexRequest{IndexIds: idList, Plan: plan}

		code, errStr := m.doHandleMoveIndex(&req)
		if errStr != "" {
			sendIndexResponseWithError(code, w, errStr)
		} else {
			sendIndexResponseMsg(w, MoveIndexStarted)
		}
	} else {
		sendIndexResponseWithError(http.StatusBadRequest, w, "Unsupported method")
		return
	}
}

func (m *RebalanceServiceManager) handleMoveIndexInternal(w http.ResponseWriter, r *http.Request) {
	const method = "RebalanceServiceManager::handleMoveIndexInternal" // for logging

	creds, ok := m.validateAuth(w, r)
	if !ok {
		l.Errorf("%v: Validation Failure req: %v", method, c.GetHTTPReqInfo(r))
		return
	}

	if r.Method == "POST" {
		bytes, _ := ioutil.ReadAll(r.Body)
		var req IndexRequest
		if err := json.Unmarshal(bytes, &req); err != nil {
			l.Errorf("%v: err: %v", method, err)
			sendIndexResponseWithError(http.StatusBadRequest, w, err.Error())
			return
		}

		if c.GetBuildMode() != c.ENTERPRISE {
			sendIndexResponseWithError(http.StatusBadRequest, w, "Alter index is only supported in Enterprise Edition")
			return
		}

		// Populate scope and collection defaults
		scope := req.Index.Scope
		if scope == "" {
			scope = c.DEFAULT_SCOPE
		}
		collection := req.Index.Collection
		if collection == "" {
			collection = c.DEFAULT_COLLECTION
		}

		permission := fmt.Sprintf("cluster.collection[%s:%s:%s].n1ql.index!alter", req.Index.Bucket, scope, collection)
		if !c.IsAllowed(creds, []string{permission}, r, w, method) {
			return
		}

		code, errStr := m.doHandleMoveIndex(&req)
		if errStr != "" {
			sendIndexResponseWithError(code, w, errStr)
		} else {
			sendIndexResponseMsg(w, MoveIndexStarted)
		}

	} else {
		sendIndexResponseWithError(http.StatusBadRequest, w, "Unsupported method")
		return
	}
}

func (m *RebalanceServiceManager) doHandleMoveIndex(req *IndexRequest) (int, string) {

	l.Infof("RebalanceServiceManager::doHandleMoveIndex %v", l.TagUD(req))

	nodes, err := validateMoveIndexReq(req)
	if err != nil {
		l.Errorf("RebalanceServiceManager::doHandleMoveIndex %v", err)
		return http.StatusBadRequest, err.Error()
	}

	err, noop := m.initMoveIndex(req, nodes)
	if err != nil {
		l.Errorf("RebalanceServiceManager::doHandleMoveIndex %v %v", err, m.rebalanceToken)
		return http.StatusInternalServerError, err.Error()
	} else if noop {
		warnStr := "No Index Movement Required for Specified Destination List"
		l.Warnf("RebalanceServiceManager::doHandleMoveIndex %v", warnStr)
		return http.StatusBadRequest, warnStr
	} else {
		go m.monitorMoveIndex()
		return http.StatusOK, ""
	}
}

func (m *RebalanceServiceManager) monitorMoveIndex() {
	select {
	case err := <-m.moveStatusCh:
		if err != nil {
			cfg := m.config.Load()
			clusterAddr := cfg["clusterAddr"].String()
			l.Errorf("RebalanceServiceManager::doHandleMoveIndex MoveIndex failed: %v", err.Error())
			c.Console(clusterAddr, fmt.Sprintf("MoveIndex failed: %v", err.Error()))
		} else {
			l.Infof("RebalanceServiceManager: Move Index succeeded")
		}
	}
}

func (m *RebalanceServiceManager) initMoveIndex(req *IndexRequest, nodes []string) (error, bool) {
	const method = "RebalanceServiceManager::initMoveIndex" // for logging

	lockTime := c.TraceRWMutexLOCK(c.LOCK_WRITE, m.svcMgrMu, "svcMgrMu", method, "")
	defer c.TraceRWMutexUNLOCK(lockTime, c.LOCK_WRITE, m.svcMgrMu, "svcMgrMu", method, "")

	var err error
	if !m.indexerReady {
		l.Errorf("%v: Cannot Process Request %v", method, c.ErrIndexerInBootstrap)
		return c.ErrIndexerInBootstrap, false
	}

	if m.checkRebalanceRunning() {
		err = errors.New("Cannot Process Move Index - Rebalance/MoveIndex In Progress")
		l.Errorf("%v: err: %v", method, err)
		return err, false
	}

	if m.getStateRebalanceID() != "" {
		err = errors.New("Cannot Process Move Index - Failover In Progress")
		l.Errorf("%v: err: %v", method, err)
		return err, false
	}

	//check globally as move index init happens on only one node
	if m.checkGlobalCleanupPending() {
		err = errors.New("Cannot Process Move Index - cleanup pending from previous " +
			"failed/aborted rebalance/failover/move index. please retry the request later.")
		l.Errorf("%v: err: %v", method, err)
		return err, false
	}

	cfg := m.config.Load()
	allWarmedup, _ := checkAllIndexersWarmedup(cfg["clusterAddr"].String())
	if !allWarmedup {
		return errors.New("Cannot Process Move Index - All Indexers are not Active"), false
	}

	if err := m.genMoveIndexToken(); err != nil {
		m.rebalanceToken = nil
		return err, false
	}

	l.Infof("%v: New Move Index Token %v Dest %v", method, m.rebalanceToken, nodes)
	transferTokens, err := m.generateTransferTokenForMoveIndex(req, nodes)
	if err != nil {
		m.rebalanceToken = nil
		return err, false
	}

	if len(transferTokens) == 0 {
		m.rebalanceToken = nil
		return nil, true
	}

	if err = m.registerRebalanceRunning(true); err != nil || m.p.ddlRunning {
		if m.p.ddlRunning {
			l.Errorf("%v: Found index build running. Cannot process move index.", method)
			fmtMsg := "move index failure - index build is in progress for indexes: %v."
			err = errors.New(fmt.Sprintf(fmtMsg, m.p.ddlRunningIndexNames))
		}
		m.runCleanupPhaseLOCKED(MoveIndexTokenPath, false)
		return err, false
	}

	if err = m.registerLocalRebalanceToken(m.rebalanceToken); err != nil {
		m.runCleanupPhaseLOCKED(MoveIndexTokenPath, false)
		return err, false
	}

	if err = m.registerMoveIndexTokenInMetakv(m.rebalanceToken, true); err != nil {
		m.runCleanupPhaseLOCKED(MoveIndexTokenPath, false)
		return err, false
	}

	//sleep for a bit to let the rebalance token propagate to other nodes via metakv.
	//unlike rebalance, this is not a fully coordinated step for move index.
	time.Sleep(2 * time.Second)

	rebalancer := NewRebalancer(transferTokens, m.rebalanceToken, string(m.nodeInfo.NodeID),
		true, nil, m.moveIndexDoneCallback, m.supvMsgch, m.localhttp, m.config.Load(), nil, false, nil, m.genericMgr.statsMgr)

	m.rebalancer = rebalancer
	m.rebalanceRunning = true
	return nil, false
}

func (m *RebalanceServiceManager) genMoveIndexToken() error {

	cfg := m.config.Load()
	ustr, _ := c.NewUUID()
	m.rebalanceToken = &RebalanceToken{
		MasterId: cfg["nodeuuid"].String(),
		RebalId:  ustr.Str(),
		Source:   RebalSourceMoveIndex,
	}
	return nil
}

func (m *RebalanceServiceManager) setErrorInMoveIndexToken(token *RebalanceToken, err error) error {

	token.Error = err.Error()

	if err := m.registerMoveIndexTokenInMetakv(token, false); err != nil {
		return err
	}

	l.Infof("RebalanceServiceManager::setErrorInMoveIndexToken done")

	return nil
}

func (m *RebalanceServiceManager) registerMoveIndexTokenInMetakv(token *RebalanceToken, upsert bool) error {

	updateRToken := func() error {
		err := c.MetakvSet(MoveIndexTokenPath, token)
		if err != nil {
			l.Errorf("RebalanceServiceManager::registerMoveIndexTokenInMetakv Unable to set "+
				"RebalanceToken In Meta Storage. Err %v", err)
			return err
		}
		l.Infof("RebalanceServiceManager::registerMoveIndexTokenInMetakv Registered Global Rebalance"+
			"Token In Metakv %v %v", MoveIndexTokenPath, token)
		return nil
	}

	var rtoken RebalanceToken
	found, err := c.MetakvGet(MoveIndexTokenPath, &rtoken)
	if err != nil {
		l.Errorf("RebalanceServiceManager::registerMoveIndexTokenInMetakv Unable to get "+
			"RebalanceToken from Meta Storage. Err %v", err)
		return err
	}

	// The caller's token and metakv token has to be the same as MoveIndexTokenPath supports
	// only one metakv value. If they are not same, report error to the caller
	if !found && upsert { // No token found in metakv and the caller is requesting to insert new token
		return updateRToken()
	} else if found && rtoken.RebalId == token.RebalId { // Caller's token is same as the token in metakv. Update the token
		return updateRToken()
	} else if found && upsert { // Caller has a different token than the token in metakv. Return err
		l.Errorf("RebalanceServiceManager::registerMoveIndexTokenInMetakv Move token: %v is different "+
			"from the token in metakv. found: %v, rtoken: %v", token, found, rtoken)
		return errors.New("Inconsistent MoveToken in metakv")
	} else { // The token in metakv is different from the caller's version and the caller is trying to update it.
		// Ignore the update as the caller's version of the token might have been deleted
		l.Infof("RebalanceServiceManager::registerMoveIndexTokenInMetakv Move token: %v is probably deleted "+
			"from metakv. found: %v, rtoken: %v", found, rtoken)
	}

	return nil
}

func (m *RebalanceServiceManager) generateTransferTokenForMoveIndex(req *IndexRequest,
	reqNodes []string) (map[string]*c.TransferToken, error) {
	const method = "RebalanceServiceManager::generateTransferTokenForMoveIndex" // for logging

	topology, err := GetGlobalTopology(m.localhttp)
	if err != nil {
		return nil, err
	}

	reqNodeUUID := make([]string, len(reqNodes))
	for i, node := range reqNodes {
		reqNodeUUID[i], err = m.getNodeIdFromDest(node)
		if err != nil {
			return nil, err
		} else if reqNodeUUID[i] == "" {
			errStr := fmt.Sprintf("Unable to Fetch Node UUID for %v", node)
			return nil, errors.New(errStr)
		}
	}

	l.Infof("%v: nodes %v, uuid %v", method, reqNodes, reqNodeUUID)

	var currNodeUUID []string
	var currInst [][]*c.IndexInst
	var numCurrInst int
	for _, localMeta := range topology.Metadata {

	outerloop:
		for _, index := range localMeta.IndexDefinitions {

			if c.IndexDefnId(req.IndexIds.DefnIds[0]) == index.DefnId {

				numCurrInst++
				for i, uuid := range reqNodeUUID {
					if localMeta.IndexerId == uuid {
						l.Infof("%v: Skip Index %v. Already exist on dest %v.", method, index.DefnId, uuid)
						reqNodeUUID = append(reqNodeUUID[:i], reqNodeUUID[i+1:]...)
						break outerloop
					}
				}

				topology := findTopologyByCollection(localMeta.IndexTopologies, index.Bucket, index.Scope, index.Collection)
				if topology == nil {
					err := errors.New(fmt.Sprintf("Fail to find index topology for bucket %v for node %v.", index.Bucket, localMeta.NodeUUID))
					l.Errorf("%v: err: %v", method, err)
					return nil, err
				}

				insts := topology.GetIndexInstancesByDefn(index.DefnId)
				if len(insts) == 0 {
					err := errors.New(fmt.Sprintf("Fail to find index instance for definition %v for node %v.", index.DefnId, localMeta.NodeUUID))
					l.Errorf("%v: err: %v", method, err)
					return nil, err
				}

				var instList []*c.IndexInst
				for _, inst := range insts {

					pc := c.NewKeyPartitionContainer(int(inst.NumPartitions), index.PartitionScheme, index.HashScheme)
					for _, partition := range inst.Partitions {
						partnDefn := c.KeyPartitionDefn{Id: c.PartitionId(partition.PartId), Version: int(partition.Version)}
						pc.AddPartition(c.PartitionId(partition.PartId), partnDefn)
					}

					localInst := &c.IndexInst{
						InstId:    c.IndexInstId(inst.InstId),
						Defn:      index,
						State:     c.IndexState(inst.State),
						Stream:    c.StreamId(inst.StreamId),
						Error:     inst.Error,
						Version:   int(inst.Version),
						ReplicaId: int(inst.ReplicaId),
						Pc:        pc,
					}
					instList = append(instList, localInst)
				}

				currInst = append(currInst, instList)
				currNodeUUID = append(currNodeUUID, localMeta.NodeUUID)
			}
		}
	}

	if len(reqNodes) != numCurrInst {
		err := errors.New(fmt.Sprintf("Target node list must specify exactly one destination for each "+
			"instances of the index. Request Nodes %v", reqNodes))
		l.Errorf("%v: err: %v", method, err)
		return nil, err
	}

	if len(currNodeUUID) != len(reqNodeUUID) {
		err := errors.New(fmt.Sprintf("Server error in computing new destination for index. "+
			"Request Nodes %v. Curr Nodes %v", reqNodeUUID, currNodeUUID))
		l.Errorf("%v: err: %v", method, err)
		return nil, err
	}

	transferTokens := make(map[string]*c.TransferToken)
	for i, _ := range reqNodeUUID {
		for _, inst := range currInst[i] {
			ttid, tt, err := m.genTransferToken(inst, currNodeUUID[i], reqNodeUUID[i])
			if err != nil {
				return nil, err
			}
			if tt.SourceId == tt.DestId {
				l.Infof("%v: Skip No-op TransferToken %v %v", method, ttid, tt)
				continue
			}
			l.Infof("%v: Generated TransferToken %v %v", method, ttid, tt)
			transferTokens[ttid] = tt
		}
	}
	return transferTokens, nil
}

func (m *RebalanceServiceManager) getNodeIdFromDest(dest string) (string, error) {

	isDest := func(addr, addrSSL string) bool {
		if security.EncryptionEnabled() && security.DisableNonSSLPort() {
			// Encryption Level : Strict -> allow only SSL Address
			return dest == addrSSL
		} else if security.EncryptionEnabled() && !security.DisableNonSSLPort() {
			// Encryption Level : All -> allow both SSL and Non SSL
			return dest == addr || dest == addrSSL
		} else {
			// Encryption not Enabled -> allow only Non SSL
			return dest == addr
		}
	}

	m.genericMgr.cinfo.Lock()
	defer m.genericMgr.cinfo.Unlock()

	if err := m.genericMgr.cinfo.FetchNodesAndSvsInfo(); err != nil {
		l.Errorf("RebalanceServiceManager::getNodeIdFromDest Error Fetching Cluster Information %v", err)
		return "", err
	}

	nids := m.genericMgr.cinfo.GetNodeIdsByServiceType(c.INDEX_HTTP_SERVICE)
	url := "/nodeuuid"

	for _, nid := range nids {

		maddr, err := m.genericMgr.cinfo.GetServiceAddress(nid, "mgmt", false)
		if err != nil {
			return "", err
		}

		meaddr, err := m.genericMgr.cinfo.GetServiceAddress(nid, "mgmt", true)
		if err != nil {
			return "", err
		}

		if isDest(maddr, meaddr) {

			haddr, err := m.genericMgr.cinfo.GetServiceAddress(nid, c.INDEX_HTTP_SERVICE, true)
			if err != nil {
				return "", err
			}

			resp, err := getWithAuth(haddr + url)
			if err != nil {
				l.Errorf("RebalanceServiceManager::getNodeIdFromDest Unable to Fetch Node UUID %v %v", haddr, err)
				return "", err
			} else {
				bytes, _ := ioutil.ReadAll(resp.Body)
				defer resp.Body.Close()
				return string(bytes), nil
			}
		}

	}
	errStr := fmt.Sprintf("Unable to find Index service for destination %v or desintation is not part of the cluster", dest)
	l.Errorf("RebalanceServiceManager::getNodeIdFromDest %v", errStr)

	return "", errors.New(errStr)
}

func (m *RebalanceServiceManager) genTransferToken(indexInst *c.IndexInst, sourceId string, destId string) (string, *c.TransferToken, error) {

	ustr, _ := c.NewUUID()

	ttid := fmt.Sprintf("TransferToken%s", ustr.Str())
	tt := &c.TransferToken{
		MasterId:  string(m.nodeInfo.NodeID),
		SourceId:  sourceId,
		DestId:    destId,
		RebalId:   m.rebalanceToken.RebalId,
		State:     c.TransferTokenCreated,
		InstId:    indexInst.InstId,
		IndexInst: *indexInst,
	}

	partitions, versions := tt.IndexInst.Pc.GetAllPartitionIds()
	for i := 0; i < len(versions); i++ {
		versions[i] = versions[i] + 1
	}
	tt.IndexInst.Defn.InstVersion = tt.IndexInst.Version + 1
	tt.IndexInst.Defn.ReplicaId = tt.IndexInst.ReplicaId
	tt.IndexInst.Defn.NumPartitions = uint32(tt.IndexInst.Pc.GetNumPartitions())
	tt.IndexInst.Defn.Partitions = partitions
	tt.IndexInst.Defn.Versions = versions
	tt.IndexInst.Pc = nil

	// reset defn id and instance id as if it is a new index.
	if c.IsPartitioned(tt.IndexInst.Defn.PartitionScheme) {
		instId, err := c.NewIndexInstId()
		if err != nil {
			return "", nil, fmt.Errorf("Fail to generate transfer token.  Reason: %v", err)
		}

		tt.RealInstId = tt.InstId
		tt.InstId = instId
	}
	return ttid, tt, nil
}

// moveIndexDoneCallback is the Rebalancer.cb.done callback function for a MoveIndex.
func (m *RebalanceServiceManager) moveIndexDoneCallback(err error, cancel <-chan struct{}) {
	m.runRebalanceCallback(cancel, func() { m.onMoveIndexDoneLOCKED(err) })
}

// onMoveIndexDoneLOCKED is invoked when a MoveIndex completes (succeeds or fails).
// Its caller should be holding mutex svcMgrMu write(?) locked.
func (m *RebalanceServiceManager) onMoveIndexDoneLOCKED(err error) {

	if err != nil {
		l.Errorf("RebalanceServiceManager::onMoveIndexDone Err %v", err)
	}

	l.Infof("RebalanceServiceManager::onMoveIndexDone Cleanup")

	if m.rebalancer != nil {
		m.runCleanupPhaseLOCKED(MoveIndexTokenPath, true)
		m.moveStatusCh <- err
	} else {
		m.runCleanupPhaseLOCKED(MoveIndexTokenPath, false)
	}
	m.rebalancer = nil
	m.rebalancerF = nil
}

func validateMoveIndexReq(req *IndexRequest) ([]string, error) {

	if len(req.IndexIds.DefnIds) == 0 {
		return nil, errors.New("Empty Index List for Move Index")
	}

	if len(req.IndexIds.DefnIds) != 1 {
		return nil, errors.New("Only 1 Index Can Be Moved Per Command")
	}

	if req.Plan == nil || len(req.Plan) == 0 {
		return nil, errors.New("Empty Plan For Move Index")
	}

	var nodes []string

	ns, ok := req.Plan["nodes"].([]interface{})
	if ok {
		for _, nse := range ns {
			n, ok := nse.(string)
			if ok {
				nodes = append(nodes, n)
			} else {
				return nil, errors.New(fmt.Sprintf("Node '%v' is not valid", req.Plan["nodes"]))
			}
		}
	} else {
		n, ok := req.Plan["nodes"].(string)
		if ok {
			nodes = []string{n}
		} else if _, ok := req.Plan["nodes"]; ok {
			return nil, errors.New(fmt.Sprintf("Node '%v' is not valid", req.Plan["nodes"]))
		}
	}

	if len(nodes) != 0 {
		nodeSet := make(map[string]bool)
		for _, node := range nodes {
			if _, ok := nodeSet[node]; ok {
				return nil, errors.New(fmt.Sprintf("Node '%v' contain duplicate nodes", req.Plan["nodes"]))
			}
			nodeSet[node] = true
		}
	} else {
		return nil, errors.New("Missing Node Information For Move Index")
	}

	return nodes, nil

}

/////////////////////////////////////////////////////////////////////////
//
//  local helper methods
//
/////////////////////////////////////////////////////////////////////////

func (m *RebalanceServiceManager) setLocalMeta(key string, value string) error {
	respch := make(MsgChannel)
	m.supvMsgch <- &MsgClustMgrLocal{
		mType:  CLUST_MGR_SET_LOCAL,
		key:    key,
		value:  value,
		respch: respch,
	}

	respMsg := <-respch
	resp := respMsg.(*MsgClustMgrLocal)

	err := resp.GetError()

	return err
}

func (m *RebalanceServiceManager) getLocalMeta(key string) (string, error) {
	respch := make(MsgChannel)
	m.supvMsgch <- &MsgClustMgrLocal{
		mType:  CLUST_MGR_GET_LOCAL,
		key:    key,
		respch: respch,
	}

	respMsg := <-respch
	resp := respMsg.(*MsgClustMgrLocal)

	val := resp.GetValue()
	err := resp.GetError()

	return val, err
}

func (m *RebalanceServiceManager) resetRunParams() {
	m.p.ddlRunning = false
	m.p.ddlRunningIndexNames = nil
}

func (m *RebalanceServiceManager) writeOk(w http.ResponseWriter) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK\n"))
}

func (m *RebalanceServiceManager) writeError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusBadRequest)
	w.Write([]byte(err.Error() + "\n"))
}

func (m *RebalanceServiceManager) writeJson(w http.ResponseWriter, json []byte) {
	header := w.Header()
	header["Content-Type"] = []string{"application/json"}
	w.WriteHeader(http.StatusOK)
	w.Write(json)
	w.Write([]byte("\n"))
}

func (m *RebalanceServiceManager) writeBytes(w http.ResponseWriter, bytes []byte) {
	w.WriteHeader(http.StatusOK)
	w.Write(bytes)
}

func (m *RebalanceServiceManager) validateAuth(w http.ResponseWriter, r *http.Request) (cbauth.Creds, bool) {
	creds, valid, err := c.IsAuthValid(r)
	if err != nil {
		m.writeError(w, err)
	} else if valid == false {
		audit.Audit(c.AUDIT_UNAUTHORIZED, r, "RebalanceServiceManager::validateAuth", "")
		w.WriteHeader(http.StatusUnauthorized)
		w.Write(c.HTTP_STATUS_UNAUTHORIZED)
	}
	return creds, valid
}

func getWithAuth(url string) (*http.Response, error) {
	params := &security.RequestParams{Timeout: time.Duration(rebalanceHttpTimeout) * time.Second}
	return security.GetWithAuth(url, params)
}

func postWithAuth(url string, bodyType string, body io.Reader) (*http.Response, error) {
	params := &security.RequestParams{Timeout: time.Duration(rebalanceHttpTimeout) * time.Second}
	return security.PostWithAuth(url, bodyType, body, params)
}

func sendIndexResponseWithError(status int, w http.ResponseWriter, msg string) {
	res := &IndexResponse{Code: RESP_ERROR, Error: msg}
	send(status, w, res)
}

func sendIndexResponse(w http.ResponseWriter) {
	result := &IndexResponse{Code: RESP_SUCCESS}
	send(http.StatusOK, w, result)
}

func sendIndexResponseMsg(w http.ResponseWriter, msg string) {
	result := &IndexResponse{Code: RESP_SUCCESS, Message: msg}
	send(http.StatusOK, w, result)
}

func send(status int, w http.ResponseWriter, res interface{}) {

	header := w.Header()
	header["Content-Type"] = []string{"application/json"}

	if buf, err := json.Marshal(res); err == nil {
		w.WriteHeader(status)
		l.Debugf("RebalanceServiceManager::sendResponse: sending response back to caller. %v", string(buf))
		w.Write(buf)
	} else {
		// note : buf is nil if err != nil
		l.Debugf("RebalanceServiceManager::sendResponse: fail to marshall response back to caller. %s", err)
		sendHttpError(w, "RebalanceServiceManager::sendResponse: Unable to marshall response", http.StatusInternalServerError)
	}
}

func sendHttpError(w http.ResponseWriter, reason string, code int) {
	http.Error(w, reason, code)
}

func findTopologyByCollection(topologies []manager.IndexTopology, bucket,
	scope, collection string) *manager.IndexTopology {

	for _, topology := range topologies {
		if topology.Bucket == bucket && topology.Scope == scope && topology.Collection == collection {
			return &topology
		}
	}

	return nil
}
