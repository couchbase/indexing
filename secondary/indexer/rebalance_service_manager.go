// @author Couchbase <info@couchbase.com>
// @copyright 2016 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package indexer

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net"
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
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/fdb"
	l "github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/manager"
	"github.com/couchbase/indexing/secondary/manager/client"
	"github.com/couchbase/indexing/secondary/security"
)

//RebalanceMgr manages the integration with ns-server and
//execution of all cluster wide operations like rebalance/failover
type RebalanceMgr interface {
}

type ServiceMgr struct {
	mu *sync.RWMutex

	rebalancer   *Rebalancer
	rebalancerF  *Rebalancer //follower rebalancer handle
	rebalanceCtx *rebalanceContext

	waiters waiters

	state

	nodeInfo *service.NodeInfo

	rebalanceRunning bool
	rebalanceToken   *RebalanceToken

	monitorStopCh StopChannel

	config    c.ConfigHolder
	supvCmdch MsgChannel //supervisor sends commands on this channel
	supvMsgch MsgChannel //channel to send any message to supervisor

	cinfo *c.ClusterInfoCache

	localhttp string

	moveStatusCh chan error

	cleanupPending bool // prior rebalance or move did not finish and will be cleaned up
	indexerReady   bool

	p runParams
}

type rebalanceContext struct {
	change service.TopologyChange
	rev    uint64
}

func (ctx *rebalanceContext) incRev() uint64 {
	curr := ctx.rev
	ctx.rev++

	return curr
}

type waiter chan state
type waiters map[waiter]struct{}

type state struct {
	rev uint64

	servers []service.NodeID

	isBalanced bool

	rebalanceID   string
	rebalanceTask *service.Task
}

func NewState() state {
	return state{
		rev:           0,
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

func NewRebalanceMgr(supvCmdch MsgChannel, supvMsgch MsgChannel, config c.Config,
	rebalanceRunning bool, rebalanceToken *RebalanceToken) (RebalanceMgr, Message) {

	l.Infof("RebalanceMgr::NewRebalanceMgr %v %v ", rebalanceRunning, rebalanceToken)

	mu := &sync.RWMutex{}

	mgr := &ServiceMgr{
		mu:           mu,
		state:        NewState(),
		supvCmdch:    supvCmdch,
		supvMsgch:    supvMsgch,
		moveStatusCh: make(chan error),
	}

	mgr.config.Store(config)

	var cinfo *c.ClusterInfoCache
	url, err := c.ClusterAuthUrl(config["clusterAddr"].String())
	if err == nil {
		cinfo, err = c.NewClusterInfoCache(url, DEFAULT_POOL)
	}
	if err != nil {
		panic("Unable to initialize cluster_info - " + err.Error())
	}
	cinfo.SetMaxRetries(MAX_CLUSTER_FETCH_RETRY)
	cinfo.SetLogPrefix("ServiceMgr: ")
	cinfo.SetUserAgent("ServiceMgr")

	mgr.cinfo = cinfo

	rebalanceHttpTimeout = config["rebalance.httpTimeout"].Int()
	mgr.waiters = make(waiters)

	mgr.nodeInfo = &service.NodeInfo{
		NodeID:   service.NodeID(config["nodeuuid"].String()),
		Priority: service.Priority(c.INDEXER_CUR_VERSION),
	}

	mgr.rebalanceRunning = rebalanceRunning
	mgr.rebalanceToken = rebalanceToken
	mgr.localhttp = mgr.getLocalHttpAddr()

	if rebalanceToken != nil {
		mgr.cleanupPending = true
		if rebalanceToken.Source == RebalSourceClusterOp { // rebalance, not move
			mgr.isBalanced = false
		}
	}

	go mgr.run()
	go mgr.initService(mgr.cleanupPending)

	return mgr, &MsgSuccess{}
}

func (m *ServiceMgr) initService(cleanupPending bool) {

	l.Infof("RebalanceMgr::initService Init")

	//allow trivial cleanups to finish to reduce noise
	if cleanupPending {
		time.Sleep(5 * time.Second)
	}

	go m.registerWithServer()
	go m.updateNodeList()

	mux := GetHTTPMux()
	mux.HandleFunc("/registerRebalanceToken", m.handleRegisterRebalanceToken)
	mux.HandleFunc("/listRebalanceTokens", m.handleListRebalanceTokens)
	mux.HandleFunc("/cleanupRebalance", m.handleCleanupRebalance)
	mux.HandleFunc("/moveIndex", m.handleMoveIndex)
	mux.HandleFunc("/moveIndexInternal", m.handleMoveIndexInternal)
	mux.HandleFunc("/nodeuuid", m.handleNodeuuid)
}

//update node list after restart
func (m *ServiceMgr) updateNodeList() {

	topology, err := getGlobalTopology(m.localhttp)
	if err != nil {
		l.Errorf("ServiceMgr::updateNodeList Error Fetching Topology %v", err)
		return
	}

	nodeList := make([]service.NodeID, 0)
	for _, meta := range topology.Metadata {
		nodeList = append(nodeList, service.NodeID(meta.NodeUUID))
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	//update only if not yet updated by prepare
	if m.servers == nil {
		m.updateStateLOCKED(func(s *state) {
			s.servers = nodeList
		})
		l.Infof("ServiceMgr::updateNodeList Updated Node List %v", nodeList)
	}

}

//run starts the rebalance manager loop which listens to messages
//from it supervisor(indexer)
func (m *ServiceMgr) run() {

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

func (m *ServiceMgr) handleSupervisorCommands(cmd Message) {
	switch cmd.GetMsgType() {

	case CONFIG_SETTINGS_UPDATE:
		m.handleConfigUpdate(cmd)

	case CLUST_MGR_INDEXER_READY:
		m.handleIndexerReady(cmd)

	default:
		l.Fatalf("ServiceMgr::handleSupervisorCommands Unknown Message %+v", cmd)
		c.CrashOnError(errors.New("Unknown Msg On Supv Channel"))
	}

}

func (m *ServiceMgr) handleConfigUpdate(cmd Message) {
	cfgUpdate := cmd.(*MsgConfigUpdate)
	m.config.Store(cfgUpdate.GetConfig())
	m.supvCmdch <- &MsgSuccess{}
}

func (m *ServiceMgr) handleIndexerReady(cmd Message) {

	m.supvCmdch <- &MsgSuccess{}

	go m.recoverRebalance()

}

func (m *ServiceMgr) registerWithServer() {

	cfg := m.config.Load()
	l.Infof("ServiceMgr::registerWithServer nodeuuid %v ", cfg["nodeuuid"].String())

	err := service.RegisterManager(m, nil)
	if err != nil {
		l.Infof("ServiceMgr::registerWithServer error %v", err)
		return
	}

	l.Infof("ServiceMgr::registerWithServer success %v")

}

/////////////////////////////////////////////////////////////////////////
//
//  service.Manager interface implementation
//
/////////////////////////////////////////////////////////////////////////

func (m *ServiceMgr) GetNodeInfo() (*service.NodeInfo, error) {

	return m.nodeInfo, nil
}

func (m *ServiceMgr) Shutdown() error {

	return nil
}

func (m *ServiceMgr) GetTaskList(rev service.Revision,
	cancel service.Cancel) (*service.TaskList, error) {

	l.Infof("ServiceMgr::GetTaskList %v", rev)

	state, err := m.wait(rev, cancel)
	if err != nil {
		return nil, err
	}

	taskList := stateToTaskList(state)
	l.Infof("ServiceMgr::GetTaskList returns %v", taskList)

	return taskList, nil
}

func (m *ServiceMgr) CancelTask(id string, rev service.Revision) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	l.Infof("ServiceMgr::CancelTask %v %v", id, rev)

	tasks := stateToTaskList(m.state).Tasks
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
		l.Errorf("ServiceMgr::CancelTask %v %v", rev, task.Rev)
		return service.ErrConflict
	}

	return m.cancelActualTaskLOCKED(task)
}

func (m *ServiceMgr) GetCurrentTopology(rev service.Revision,
	cancel service.Cancel) (*service.Topology, error) {

	l.Infof("ServiceMgr::GetCurrentTopology %v", rev)

	state, err := m.wait(rev, cancel)
	if err != nil {
		return nil, err
	}

	topology := m.stateToTopology(state)

	l.Infof("ServiceMgr::GetCurrentTopology returns %v", topology)

	return topology, nil
}

//All errors need to be reported as return value. Status of prepared task is not
//considered for failure reporting.
func (m *ServiceMgr) PrepareTopologyChange(change service.TopologyChange) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	l.Infof("ServiceMgr::PrepareTopologyChange %v", change)

	if m.state.rebalanceID != "" {
		l.Errorf("ServiceMgr::PrepareTopologyChange err %v %v", service.ErrConflict, m.state.rebalanceID)
		if (change.Type == service.TopologyChangeTypeRebalance) {
			m.isBalanced = false
		}
		return service.ErrConflict
	}

	var err error
	if change.Type == service.TopologyChangeTypeFailover {
		err = m.prepareFailover(change)
	} else if change.Type == service.TopologyChangeTypeRebalance {
		err = m.prepareRebalance(change)
		if err != nil {
			m.isBalanced = false
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

	m.updateStateLOCKED(func(s *state) {
		s.rebalanceID = change.ID
		s.servers = nodeList
	})

	return nil

}

func (m *ServiceMgr) prepareFailover(change service.TopologyChange) error {

	if !m.indexerReady {
		return nil
	}

	var err error
	if m.rebalanceToken != nil && m.rebalanceToken.Source == RebalSourceClusterOp {

		l.Infof("ServiceMgr::prepareFailover Found Rebalance In Progress %v", m.rebalanceToken)

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
			l.Infof("ServiceMgr::prepareFailover Master Missing From Cluster Node List. Cleanup")
			err = m.runCleanupPhaseLOCKED(RebalanceTokenPath, true)
		} else {
			err = m.runCleanupPhaseLOCKED(RebalanceTokenPath, false)
		}
		return err
	}

	if m.rebalanceRunning && m.rebalanceToken.Source == RebalSourceClusterOp {
		l.Infof("ServiceMgr::prepareFailover Found Node In Prepared State. Cleanup.")
		err = m.runCleanupPhaseLOCKED(RebalanceTokenPath, false)
		return err
	}

	if m.rebalanceToken != nil && m.rebalanceToken.Source == RebalSourceMoveIndex {

		l.Infof("ServiceMgr::prepareFailover Found Move Index In Progress %v. Aborting.", m.rebalanceToken)

		masterAlive := false
		masterCleanup := false
		for _, node := range change.KeepNodes {
			if m.rebalanceToken.MasterId == string(node.NodeInfo.NodeID) {
				masterAlive = true
				break
			}
		}

		if !masterAlive {
			l.Infof("ServiceMgr::prepareFailover Master Missing From Cluster Node List. Cleanup MoveIndex As Master.")
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

func (m *ServiceMgr) prepareRebalance(change service.TopologyChange) error {

	var err error
	if m.cleanupPending {
		m.isBalanced = false
		err = errors.New("indexer rebalance failure - cleanup pending from previous  " +
			"failed/aborted rebalance/failover/move index. please retry the request later.")
		l.Errorf("ServiceMgr::prepareRebalance %v", err)
		return err
	}

	if m.rebalanceToken != nil && m.rebalanceToken.Source == RebalSourceMoveIndex {
		err = errors.New("indexer rebalance failure - move index in progress")
		l.Errorf("ServiceMgr::prepareRebalance %v", err)
		return err
	}

	if m.rebalanceToken != nil && m.rebalanceToken.Source == RebalSourceClusterOp {
		l.Warnf("ServiceMgr::prepareRebalance Found Rebalance In Progress. Cleanup.")
		if m.rebalancerF != nil {
			m.rebalancerF.Cancel()
			m.rebalancerF = nil
		}
		if err = m.runCleanupPhaseLOCKED(RebalanceTokenPath, false); err != nil {
			return err
		}
	}

	if m.checkRebalanceRunning() {
		l.Warnf("ServiceMgr::prepareRebalance Found Rebalance Running Flag. Cleanup Prepare Phase")
		if err = m.runCleanupPhaseLOCKED(RebalanceTokenPath, false); err != nil {
			return err
		}
	}

	if m.checkLocalCleanupPending() {
		l.Warnf("ServiceMgr::prepareRebalance Found Pending Local Cleanup Token. Run Cleanup.")
		if err = m.runCleanupPhaseLOCKED(RebalanceTokenPath, false); err != nil {
			return err
		}
		//check again if the cleanup was successful
		if m.checkLocalCleanupPending() {
			err = errors.New("indexer rebalance failure - cleanup pending from previous  " +
				"failed/aborted rebalance/failover/move index. please retry the request later.")
			l.Errorf("ServiceMgr::prepareRebalance %v", err)
			return err
		}
	}

	if c.GetBuildMode() == c.ENTERPRISE {
		m.p.ddlRunning, m.p.ddlRunningIndexNames = m.checkDDLRunning()
		l.Infof("ServiceMgr::prepareRebalance Found DDL Running %v", m.p.ddlRunningIndexNames)
	}

	l.Infof("ServiceMgr::prepareRebalance Init Prepare Phase")

	if isSingleNodeRebal(change) && change.KeepNodes[0].NodeInfo.NodeID != m.nodeInfo.NodeID {
		err := errors.New("indexer - node receiving prepare request not part of cluster")
		l.Errorf("ServiceMgr::prepareRebalance %v", err)
		return err
	} else {
		if err := m.initPreparePhaseRebalance(); err != nil {
			return err
		}
	}

	m.isBalanced = true

	return nil
}

func (m *ServiceMgr) StartTopologyChange(change service.TopologyChange) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	l.Infof("ServiceMgr::StartTopologyChange %v", change)

	if m.state.rebalanceID != change.ID || m.rebalancer != nil {
		l.Errorf("ServiceMgr::StartTopologyChange err %v %v %v %v", service.ErrConflict,
			m.state.rebalanceID, change.ID, m.rebalancer)
		if change.Type == service.TopologyChangeTypeRebalance {
			m.isBalanced = false
		}
		return service.ErrConflict
	}

	if change.CurrentTopologyRev != nil {
		haveRev := DecodeRev(change.CurrentTopologyRev)
		if haveRev != m.state.rev {
			l.Errorf("ServiceMgr::StartTopologyChange err %v %v %v", service.ErrConflict,
				haveRev, m.state.rev)
			if change.Type == service.TopologyChangeTypeRebalance {
				m.isBalanced = false
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
			m.isBalanced = false
		}
	} else {
		err = service.ErrNotSupported
	}

	return err
}

func (m *ServiceMgr) startFailover(change service.TopologyChange) error {

	ctx := &rebalanceContext{
		rev:    0,
		change: change,
	}

	m.rebalanceCtx = ctx
	m.updateRebalanceProgressLOCKED(0)

	m.rebalancer = NewRebalancer(nil, nil, string(m.nodeInfo.NodeID), true,
		m.rebalanceProgressCallback, m.rebalanceDoneCallback, m.supvMsgch, "", m.config.Load(), nil, false, nil)

	return nil
}

func (m *ServiceMgr) startRebalance(change service.TopologyChange) error {

	var runPlanner bool
	var skipRebalance bool
	var transferTokens map[string]*c.TransferToken

	if isSingleNodeRebal(change) && change.KeepNodes[0].NodeInfo.NodeID != m.nodeInfo.NodeID {
		err := errors.New("Node receiving Start request not part of cluster")
		l.Errorf("ServiceMgr::startRebalance %v Self %v Cluster %v", err, m.nodeInfo.NodeID,
			change.KeepNodes[0].NodeInfo.NodeID)
		m.runCleanupPhaseLOCKED(RebalanceTokenPath, true)
		return err
	} else {

		var err error

		err = m.cleanupOrphanTokens(change)
		if err != nil {
			l.Errorf("ServiceMgr::startRebalance Error During Cleanup Orphan Tokens %v", err)
			m.runCleanupPhaseLOCKED(RebalanceTokenPath, true)
			return err
		}

		rtoken, err := m.checkExistingGlobalRToken()
		if err != nil {
			l.Errorf("ServiceMgr::startRebalance Error Checking Global RToken %v", err)
			m.runCleanupPhaseLOCKED(RebalanceTokenPath, true)
			return err
		}

		if rtoken != nil {
			l.Errorf("ServiceMgr::startRebalance Found Existing Global RToken %v", rtoken)
			m.runCleanupPhaseLOCKED(RebalanceTokenPath, true)
			return errors.New("Protocol Conflict Error: Existing Rebalance Token Found")
		}

		err, skipRebalance = m.initStartPhase(change)
		if err != nil {
			l.Errorf("ServiceMgr::startRebalance Error During Start Phase Init %v", err)
			m.runCleanupPhaseLOCKED(RebalanceTokenPath, true)
			return err
		}

		cfg := m.config.Load()

		if c.GetBuildMode() != c.ENTERPRISE {
			l.Infof("ServiceMgr::startRebalance skip planner for non-enterprise edition")
			runPlanner = false
		} else if cfg["rebalance.disable_index_move"].Bool() {
			l.Infof("ServiceMgr::startRebalance skip planner as disable_index_move is set")
			runPlanner = false
		} else if skipRebalance {
			l.Infof("ServiceMgr::startRebalance skip planner due to skipRebalance flag")
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

	m.rebalancer = NewRebalancer(transferTokens, m.rebalanceToken, string(m.nodeInfo.NodeID),
		true, m.rebalanceProgressCallback, m.rebalanceDoneCallback, m.supvMsgch,
		m.localhttp, m.config.Load(), &change, runPlanner, &m.p)

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

func (m *ServiceMgr) isNoOpRebal(change service.TopologyChange) bool {

	cfg := m.config.Load()
	onEjectOnly := cfg["rebalance.node_eject_only"].Bool()

	if onEjectOnly && len(change.EjectNodes) == 0 {
		return true
	}
	return false

}

func (m *ServiceMgr) checkDDLRunning() (bool, []string) {

	respCh := make(MsgChannel)
	m.supvMsgch <- &MsgCheckDDLInProgress{respCh: respCh}
	msg := <-respCh

	ddlInProgress := msg.(*MsgDDLInProgressResponse).GetDDLInProgress()
	inProgressIndexNames := msg.(*MsgDDLInProgressResponse).GetInProgressIndexNames()
	return ddlInProgress, inProgressIndexNames
}

func (m *ServiceMgr) checkRebalanceRunning() bool {
	return m.rebalanceRunning
}

func (m *ServiceMgr) checkExistingGlobalRToken() (*RebalanceToken, error) {

	var rtoken RebalanceToken
	var found bool
	var err error
	found, err = c.MetakvGet(RebalanceTokenPath, &rtoken)
	if err != nil {
		l.Errorf("ServiceMgr::checkExistingGlobalRToken Error Fetching "+
			"Rebalance Token From Metakv %v. Path %v", err, RebalanceTokenPath)
		return nil, err
	}
	if found {
		return &rtoken, nil
	}

	found, err = c.MetakvGet(MoveIndexTokenPath, &rtoken)
	if err != nil {
		l.Errorf("ServiceMgr::checkExistingGlobalRToken Error Fetching "+
			"MoveIndex Token From Metakv %v. Path %v", err, MoveIndexTokenPath)
		return nil, err
	}
	if found {
		return &rtoken, nil
	}

	return nil, nil

}

func (m *ServiceMgr) initPreparePhaseRebalance() error {

	err := m.registerRebalanceRunning(true)
	if err != nil {
		return err
	}

	m.rebalanceRunning = true
	m.monitorStopCh = make(StopChannel)

	go m.monitorStartPhaseInit(m.monitorStopCh)

	return nil
}

func (m *ServiceMgr) initPreparePhaseFailover() error {

	err := m.registerRebalanceRunning(false)
	if err != nil {
		return err
	}

	m.rebalanceRunning = true
	return nil
}

func (m *ServiceMgr) runCleanupPhaseLOCKED(path string, isMaster bool) error {

	l.Infof("ServiceMgr::runCleanupPhase path %v isMaster %v", path, isMaster)

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
			l.Errorf("ServiceMgr::runCleanupPhase Error Fetching Metakv Tokens %v", err)
		}

		if rtokens != nil && len(rtokens.TT) != 0 {
			err := m.cleanupTransferTokens(rtokens.TT)
			if err != nil {
				l.Errorf("ServiceMgr::runCleanupPhase Error Cleaning Transfer Tokens %v", err)
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

func (m *ServiceMgr) cleanupGlobalRToken(path string) error {

	var rtoken RebalanceToken
	found, err := c.MetakvGet(path, &rtoken)
	if err != nil {
		l.Errorf("ServiceMgr::cleanupGlobalRToken Error Fetching Rebalance Token From Metakv %v. Path %v", err, path)
		return err
	}

	if found {
		l.Infof("ServiceMgr::cleanupGlobalRToken Delete Global Rebalance Token %v", rtoken)

		err := c.MetakvDel(path)
		if err != nil {
			l.Fatalf("ServiceMgr::cleanupGlobalRToken Unable to delete RebalanceToken from "+
				"Meta Storage. %v. Err %v", rtoken, err)
			return err
		}
	}
	return nil
}

func (m *ServiceMgr) cleanupOrphanTokens(change service.TopologyChange) error {

	rtokens, err := m.getCurrRebalTokens()
	if err != nil {
		l.Errorf("ServiceMgr::cleanupOrphanTokens Error Fetching Metakv Tokens %v", err)
		return err
	}

	if rtokens == nil {
		return nil
	}

	masterAlive := false
	if rtokens.RT != nil {
		l.Infof("ServiceMgr::cleanupOrphanTokens Found Token %v", rtokens.RT)

		for _, node := range change.KeepNodes {
			if rtokens.RT.MasterId == string(node.NodeInfo.NodeID) {
				masterAlive = true
				break
			}
		}
		if !masterAlive {
			l.Infof("ServiceMgr::cleanupOrphanTokens Cleaning Up Token %v", rtokens.RT)
			err := c.MetakvDel(RebalanceTokenPath)
			if err != nil {
				l.Errorf("ServiceMgr::cleanupOrphanTokens Unable to delete RebalanceToken from "+
					"Meta Storage. %v. Err %v", rtokens.RT, err)
				return err
			}
		}
	}

	masterAlive = false
	if rtokens.MT != nil {
		l.Infof("ServiceMgr::cleanupOrphanTokens Found Rebalance Token %v", rtokens.MT)

		for _, node := range change.KeepNodes {
			if rtokens.MT.MasterId == string(node.NodeInfo.NodeID) {
				masterAlive = true
				break
			}
		}
		if !masterAlive {
			l.Infof("ServiceMgr::cleanupOrphanTokens Cleaning Up Token %v", rtokens.MT)
			err := c.MetakvDel(MoveIndexTokenPath)
			if err != nil {
				l.Errorf("ServiceMgr::cleanupOrphanTokens Unable to delete MoveIndex Token from "+
					"Meta Storage. %v. Err %v", rtokens.MT, err)
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
			l.Infof("ServiceMgr::cleanupOrphanTokens Cleaning Up Token Owner %v %v %v", ownerId, ttid, tt)
			err := c.MetakvDel(RebalanceMetakvDir + ttid)
			if err != nil {
				l.Errorf("ServiceMgr::cleanupOrphanTokens Unable to delete TransferToken from "+
					"Meta Storage. %v. Err %v", ttid, err)
				return err
			}
		}
	}

	return nil

}

func (m *ServiceMgr) cleanupTransferTokens(tts map[string]*c.TransferToken) error {

	if tts == nil || len(tts) == 0 {
		l.Infof("ServiceMgr::cleanupTransferTokens No Tokens Found For Cleanup")
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

	// cleanup transfer token
	for ttid, tt := range tts {

		l.Infof("ServiceMgr::cleanupTransferTokens Cleaning Up %v %v", ttid, tt)

		if tt.MasterId == string(m.nodeInfo.NodeID) {
			m.cleanupTransferTokensForMaster(ttid, tt)
		}
		if tt.SourceId == string(m.nodeInfo.NodeID) {
			m.cleanupTransferTokensForSource(ttid, tt)
		}
		if tt.DestId == string(m.nodeInfo.NodeID) {
			m.cleanupTransferTokensForDest(ttid, tt, indexStateMap)
		}

	}

	return nil
}

func (m *ServiceMgr) cleanupTransferTokensForMaster(ttid string, tt *c.TransferToken) error {

	switch tt.State {

	case c.TransferTokenCommit, c.TransferTokenDeleted:
		l.Infof("ServiceMgr::cleanupTransferTokensForMaster Cleanup Token %v %v", ttid, tt)
		err := c.MetakvDel(RebalanceMetakvDir + ttid)
		if err != nil {
			l.Errorf("ServiceMgr::cleanupTransferTokensForMaster Unable to delete TransferToken In "+
				"Meta Storage. %v. Err %v", tt, err)
			return err
		}

	}

	return nil

}

func (m *ServiceMgr) cleanupTransferTokensForSource(ttid string, tt *c.TransferToken) error {

	switch tt.State {

	case c.TransferTokenReady:
		var err error
		l.Infof("ServiceMgr::cleanupTransferTokensForSource Cleanup Token %v %v", ttid, tt)
		defn := tt.IndexInst.Defn
		defn.InstId = tt.InstId
		defn.RealInstId = tt.RealInstId
		err = m.cleanupIndex(defn)
		if err == nil {
			err = c.MetakvDel(RebalanceMetakvDir + ttid)
			if err != nil {
				l.Errorf("ServiceMgr::cleanupTransferTokensForSource Unable to delete TransferToken In "+
					"Meta Storage. %v. Err %v", tt, err)
				return err
			}
		}
	}

	return nil

}

func (m *ServiceMgr) cleanupTransferTokensForDest(ttid string, tt *c.TransferToken, indexStateMap map[c.IndexInstId]c.RebalanceState) error {

	cleanup := func() error {
		var err error
		l.Infof("ServiceMgr::cleanupTransferTokensForDest Cleanup Token %v %v", ttid, tt)
		defn := tt.IndexInst.Defn
		defn.InstId = tt.InstId
		defn.RealInstId = tt.RealInstId
		err = m.cleanupIndex(defn)
		if err == nil {
			err = c.MetakvDel(RebalanceMetakvDir + ttid)
			if err != nil {
				l.Errorf("ServiceMgr::cleanupTransferTokensForDest Unable to delete TransferToken In "+
					"Meta Storage. %v. Err %v", tt, err)
				return err
			}
		}

		return nil
	}

	switch tt.State {

	case c.TransferTokenCreated, c.TransferTokenAccepted, c.TransferTokenRefused,
		c.TransferTokenInitate, c.TransferTokenInProgress:
		return cleanup()

	case c.TransferTokenMerge:
		// the proxy instance could have been deleted after it has gone to MERGED state
		rebalState, ok := indexStateMap[tt.InstId]
		if !ok {
			rebalState, ok = indexStateMap[tt.RealInstId]
		}

		if !ok {
			if err := c.MetakvDel(RebalanceMetakvDir + ttid); err != nil {
				l.Errorf("ServiceMgr::cleanupTransferTokensForDest Unable to delete TransferToken In "+
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

func (m *ServiceMgr) cleanupIndex(indexDefn c.IndexDefn) error {

	req := manager.IndexRequest{Index: indexDefn}
	body, err := json.Marshal(&req)
	if err != nil {
		l.Errorf("ServiceMgr::cleanupIndex Error marshal drop index %v", err)
		return err
	}

	bodybuf := bytes.NewBuffer(body)

	localaddr := m.localhttp

	url := "/dropIndex"
	resp, err := postWithAuth(localaddr+url, "application/json", bodybuf)
	if err != nil {
		l.Errorf("ServiceMgr::cleanupIndex Error drop index on %v %v", localaddr+url, err)
		return err
	}
	defer resp.Body.Close()
	bytes, _ := ioutil.ReadAll(resp.Body)
	response := new(manager.IndexResponse)
	if err := json.Unmarshal(bytes, &response); err != nil {
		l.Errorf("ServiceMgr::cleanupIndex Error unmarshal response %v %v", localaddr+url, err)
		return err
	}
	if response.Code == manager.RESP_ERROR {
		if strings.Contains(response.Error, forestdb.FDB_RESULT_KEY_NOT_FOUND.Error()) {
			l.Errorf("ServiceMgr::cleanupIndex Error dropping index %v %v. Ignored.", localaddr+url, response.Error)
			return nil
		}
		l.Errorf("ServiceMgr::cleanupIndex Error dropping index %v %v", localaddr+url, response.Error)
		return err
	}

	return nil

}

func (m *ServiceMgr) cleanupRebalanceRunning() error {

	l.Infof("ServiceMgr::cleanupRebalanceRunning Cleanup")

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
		l.Fatalf("ServiceMgr::cleanupRebalanceRunning Unable to delete RebalanceRunning In Local"+
			"Meta Storage. Err %v", errMsg)
		c.CrashOnError(errMsg)
	}

	m.rebalanceRunning = false

	// notify DDLServiceManager
	resumeDDLProcessing()

	return nil

}

func (m *ServiceMgr) cleanupLocalRToken() error {

	l.Infof("ServiceMgr::cleanupLocalRToken Cleanup")

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
		l.Fatalf("ServiceMgr::cleanupLocalRToken Unable to delete Rebalance Token In Local"+
			"Meta Storage. Path %v Err %v", RebalanceTokenPath, errMsg)
		c.CrashOnError(errMsg)
	}

	m.rebalanceToken = nil
	return nil
}

func (m *ServiceMgr) monitorStartPhaseInit(stopch StopChannel) error {

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
				m.mu.Lock()
				defer m.mu.Unlock()
				if m.rebalanceToken == nil && elapsed > startPhaseBeginTimeout {
					l.Infof("ServiceMgr::monitorStartPhaseInit Timout Waiting for RebalanceToken. Cleanup Prepare Phase")
					//TODO handle server side differently
					m.runCleanupPhaseLOCKED(RebalanceTokenPath, false)
					done = true
				} else if m.rebalanceToken != nil {
					l.Infof("ServiceMgr::monitorStartPhaseInit Found RebalanceToken %v.", m.rebalanceToken)
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

func (m *ServiceMgr) rebalanceJanitor() {

	for {
		time.Sleep(time.Second * 30)

		m.mu.Lock()

		l.Infof("ServiceMgr::rebalanceJanitor Running Periodic Cleanup")

		if !m.rebalanceRunning {
			rtokens, err := m.getCurrRebalTokens()
			if err != nil {
				l.Errorf("ServiceMgr::rebalanceJanitor Error Fetching Metakv Tokens %v", err)
			}

			if rtokens != nil && len(rtokens.TT) != 0 {
				l.Infof("ServiceMgr::rebalanceJanitor Found %v tokens. Cleaning up.", len(rtokens.TT))
				err := m.cleanupTransferTokens(rtokens.TT)
				if err != nil {
					l.Errorf("ServiceMgr::rebalanceJanitor Error Cleaning Transfer Tokens %v", err)
				}
			}
		}
		m.mu.Unlock()
	}

}

func (m *ServiceMgr) initStartPhase(change service.TopologyChange) (error, bool) {

	var err error
	var skipRebalance bool

	if err = m.genRebalanceToken(); err != nil {
		return err, true
	}

	if err = m.registerLocalRebalanceToken(); err != nil {
		return err, true
	}

	if err = m.registerRebalanceTokenInMetakv(); err != nil {
		return err, true
	}

	if err, skipRebalance = m.registerGlobalRebalanceToken(change); err != nil {
		return err, skipRebalance
	}

	return nil, skipRebalance
}

func (m *ServiceMgr) genRebalanceToken() error {

	m.cinfo.Lock()
	defer m.cinfo.Unlock()

	var localIP string
	if err := m.cinfo.Fetch(); err == nil {
		localIP, _ = m.cinfo.GetLocalHostname()
	}

	cfg := m.config.Load()

	m.rebalanceToken = &RebalanceToken{
		MasterId: cfg["nodeuuid"].String(),
		RebalId:  m.state.rebalanceID,
		Source:   RebalSourceClusterOp,
		MasterIP: localIP,
	}
	return nil
}

func (m *ServiceMgr) registerRebalanceRunning(checkDDL bool) error {
	respch := make(MsgChannel)
	m.supvMsgch <- &MsgClustMgrLocal{
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
			l.Infof("ServiceMgr::registerRebalanceRunning Found DDL Running %v", m.p.ddlRunningIndexNames)
		} else {
			l.Errorf("ServiceMgr::registerRebalanceRunning Unable to set RebalanceRunning In Local"+
				"Meta Storage. Err %v", errMsg)

			return errMsg
		}
	}

	// for ddl service manager
	stopDDLProcessing()

	return nil
}

func (m *ServiceMgr) registerLocalRebalanceToken() error {

	rtoken, err := json.Marshal(m.rebalanceToken)
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
		l.Errorf("ServiceMgr::registerLocalRebalanceToken Unable to set RebalanceToken In Local"+
			"Meta Storage. Err %v", errMsg)
		return err
	}
	l.Infof("ServiceMgr::registerLocalRebalanceToken Registered Rebalance Token In Local Meta %v", m.rebalanceToken)

	return nil

}

func (m *ServiceMgr) registerRebalanceTokenInMetakv() error {

	err := c.MetakvSet(RebalanceTokenPath, m.rebalanceToken)
	if err != nil {
		l.Errorf("ServiceMgr::registerRebalanceTokenInMetakv Unable to set RebalanceToken In "+
			"Meta Storage. Err %v", err)
		return err
	}
	l.Infof("ServiceMgr::registerRebalanceTokenInMetakv Registered Global Rebalance Token In Metakv %v", m.rebalanceToken)

	return nil
}

func (m *ServiceMgr) observeGlobalRebalanceToken(rebalToken RebalanceToken) bool {

	cfg := m.config.Load()
	globalTokenWaitTimeout := cfg["rebalance.globalTokenWaitTimeout"].Int()

	elapsed := 1

	for elapsed < globalTokenWaitTimeout {

		var rtoken RebalanceToken
		found, err := c.MetakvGet(RebalanceTokenPath, &rtoken)
		if err != nil {
			l.Errorf("ServiceMgr::observeGlobalRebalanceToken Error Checking Rebalance Token In Metakv %v", err)
			continue
		}

		if found {
			if reflect.DeepEqual(rtoken, rebalToken) {
				l.Infof("ServiceMgr::observeGlobalRebalanceToken Global And Local Rebalance Token Match %v", rtoken)
				return true
			} else {
				l.Errorf("ServiceMgr::observeGlobalRebalanceToken Mismatch in Global and Local Rebalance Token. Global %v. Local %v.", rtoken, rebalToken)
				return false
			}
		}

		l.Infof("ServiceMgr::observeGlobalRebalanceToken Waiting for Global Rebalance Token In Metakv")
		time.Sleep(time.Second * time.Duration(1))
		elapsed += 1
	}

	l.Errorf("ServiceMgr::observeGlobalRebalanceToken Timeout Waiting for Global Rebalance Token In Metakv")

	return false

}

func (m *ServiceMgr) registerGlobalRebalanceToken(change service.TopologyChange) (error, bool) {

	m.cinfo.Lock()
	defer m.cinfo.Unlock()

	var err error
	var nids []c.NodeId

	maxRetry := 10
	valid := false
	for i := 0; i <= maxRetry; i++ {

		if i != 0 {
			time.Sleep(2 * time.Second)
		}

		if err = m.cinfo.Fetch(); err != nil {
			l.Errorf("ServiceMgr::registerGlobalRebalanceToken Error Fetching Cluster Information %v", err)
			continue
		}

		nids = m.cinfo.GetNodesByServiceType(c.INDEX_HTTP_SERVICE)

		allKnownNodes := len(change.KeepNodes) + len(change.EjectNodes)
		if len(nids) != allKnownNodes {

			if isSingleNodeRebal(change) {
				l.Infof("ServiceMgr::registerGlobalRebalanceToken ClusterInfo Node List doesn't "+
					"match Known Nodes in Rebalance Request. Skip Rebalance. cinfo.nodes : %v, change : %v", m.cinfo.Nodes(), change)
				return nil, true
			}

			err = errors.New("ClusterInfo Node List doesn't match Known Nodes in Rebalance Request")
			l.Errorf("ServiceMgr::registerGlobalRebalanceToken %v %v %v Retrying %v", err, nids, allKnownNodes, i)
			continue
		}
		valid = true
		break
	}

	if !valid {
		l.Errorf("ServiceMgr::registerGlobalRebalanceToken cinfo.nodes : %v, change : %v", m.cinfo.Nodes(), change)
		return err, true
	}

	url := "/registerRebalanceToken"

	for _, nid := range nids {

		addr, err := m.cinfo.GetServiceAddress(nid, c.INDEX_HTTP_SERVICE, true)
		if err == nil {

			localaddr, err := m.cinfo.GetLocalServiceAddress(c.INDEX_HTTP_SERVICE, true)
			if err != nil {
				l.Errorf("ServiceMgr::registerGlobalRebalanceToken Error Fetching Local Service Address %v", err)
				return errors.New(fmt.Sprintf("Fail to retrieve http endpoint for local node %v", err)), true
			}

			if addr == localaddr {
				l.Infof("ServiceMgr::registerGlobalRebalanceToken skip local service %v", addr)
				continue
			}

			body, err := json.Marshal(&m.rebalanceToken)
			if err != nil {
				l.Errorf("ServiceMgr::registerGlobalRebalanceToken Error registering rebalance token on %v %v", addr+url, err)
				return err, true
			}

			bodybuf := bytes.NewBuffer(body)

			resp, err := postWithAuth(addr+url, "application/json", bodybuf)
			if err != nil {
				l.Errorf("ServiceMgr::registerGlobalRebalanceToken Error registering rebalance token on %v %v", addr+url, err)
				return err, true
			}
			ioutil.ReadAll(resp.Body)
			resp.Body.Close()
		} else {
			l.Errorf("ServiceMgr::registerGlobalRebalanceToken Error Fetching Service Address %v", err)
			return errors.New(fmt.Sprintf("Fail to retrieve http endpoint for index node %v", err)), true
		}

		l.Infof("ServiceMgr::registerGlobalRebalanceToken Successfully registered rebalance token on %v", addr+url)
	}

	return nil, false

}

func (m *ServiceMgr) runRebalanceCallback(cancel <-chan struct{}, body func()) {
	done := make(chan struct{})

	go func() {
		m.mu.Lock()
		defer m.mu.Unlock()

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

func (m *ServiceMgr) rebalanceProgressCallback(progress float64, cancel <-chan struct{}) {
	m.runRebalanceCallback(cancel, func() {
		m.updateRebalanceProgressLOCKED(progress)
	})
}

func (m *ServiceMgr) updateRebalanceProgressLOCKED(progress float64) {
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

	m.updateStateLOCKED(func(s *state) {
		s.rebalanceTask = task
	})
}

func (m *ServiceMgr) rebalanceDoneCallback(err error, cancel <-chan struct{}) {

	m.runRebalanceCallback(cancel, func() {

		if m.rebalancer != nil {
			change := &m.rebalanceCtx.change
			defer func() {
				go notifyRebalanceDone(change, err != nil)
			}()
		}

		m.onRebalanceDoneLOCKED(err, false)
	})
}

func (m *ServiceMgr) onRebalanceDoneLOCKED(err error, cancelRebalance bool) {
	logPrefix := "ServiceMgr::onRebalanceDoneLOCKED"
	l.Infof("%s Rebalance Done, cancel: %v, err: %v", logPrefix, cancelRebalance, err)

	isMaster := m.rebalancer != nil
	if isMaster {
		newTask := (*service.Task)(nil)
		if err != nil {
			ctx := m.rebalanceCtx
			rev := ctx.incRev()

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
			m.isBalanced = false
		} else if cancelRebalance == true {
			m.isBalanced = false
		} else {
			m.isBalanced = true
		}

		if !m.cleanupPending {
			if m.runCleanupPhaseLOCKED(RebalanceTokenPath, isMaster) != nil {
				m.isBalanced = false
			}
		}

		m.rebalanceCtx = nil

		m.updateStateLOCKED(func(s *state) {
			s.rebalanceTask = newTask
			s.rebalanceID = ""
		})
	} else if m.runCleanupPhaseLOCKED(RebalanceTokenPath, isMaster) != nil {
		m.isBalanced = false
	}

	m.rebalancer = nil
	m.rebalancerF = nil
	l.Infof("%s isBalanced: %v, isMaster: %v", logPrefix, m.isBalanced, isMaster)
}

func (m *ServiceMgr) notifyWaitersLOCKED() {
	s := m.copyStateLOCKED()
	for ch := range m.waiters {
		if ch != nil {
			ch <- s
		}
	}

	m.waiters = make(waiters)
}

func (m *ServiceMgr) addWaiterLOCKED() waiter {
	ch := make(waiter, 1)
	m.waiters[ch] = struct{}{}

	return ch
}

func (m *ServiceMgr) removeWaiter(w waiter) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.waiters, w)
}

func (m *ServiceMgr) updateState(body func(state *state)) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.updateStateLOCKED(body)
}

func (m *ServiceMgr) updateStateLOCKED(body func(state *state)) {
	body(&m.state)
	m.state.rev++

	m.notifyWaitersLOCKED()
}

func (m *ServiceMgr) wait(rev service.Revision,
	cancel service.Cancel) (state, error) {

	m.mu.Lock()

	unlock := NewCleanup(func() { m.mu.Unlock() })
	defer unlock.Run()

	currState := m.copyStateLOCKED()

	if rev == nil {
		return currState, nil
	}

	haveRev := DecodeRev(rev)
	if haveRev != m.rev {
		return currState, nil
	}

	ch := m.addWaiterLOCKED()
	unlock.Run()

	select {
	case <-cancel:
		m.removeWaiter(ch)
		return NewState(), service.ErrCanceled
	case newState := <-ch:
		return newState, nil
	}
}

func (m *ServiceMgr) copyStateLOCKED() state {
	s := m.state

	return s
}

func (m *ServiceMgr) cancelActualTaskLOCKED(task *service.Task) error {
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

func (m *ServiceMgr) cancelPrepareTaskLOCKED() error {
	if m.rebalancer != nil {
		return service.ErrConflict
	}

	if m.monitorStopCh != nil {
		close(m.monitorStopCh)
		m.monitorStopCh = nil
	}

	m.cleanupRebalanceRunning()

	m.updateStateLOCKED(func(s *state) {
		s.rebalanceID = ""
	})

	return nil
}

func (m *ServiceMgr) cancelRebalanceTaskLOCKED(task *service.Task) error {
	switch task.Status {
	case service.TaskStatusRunning:
		return m.cancelRunningRebalanceTaskLOCKED()
	case service.TaskStatusFailed:
		return m.cancelFailedRebalanceTaskLOCKED()
	default:
		panic("can't happen")
	}
}

func (m *ServiceMgr) cancelRunningRebalanceTaskLOCKED() error {
	m.rebalancer.Cancel()
	m.onRebalanceDoneLOCKED(nil, true)

	return nil
}

func (m *ServiceMgr) cancelFailedRebalanceTaskLOCKED() error {
	m.updateStateLOCKED(func(s *state) {
		s.rebalanceTask = nil
	})

	return nil
}

func (m *ServiceMgr) stateToTopology(s state) *service.Topology {
	topology := &service.Topology{}

	topology.Rev = EncodeRev(s.rev)
	if m.servers != nil && len(m.servers) != 0 {
		topology.Nodes = append([]service.NodeID(nil), m.servers...)
	} else {
		topology.Nodes = append([]service.NodeID(nil), m.nodeInfo.NodeID)
	}
	topology.IsBalanced = m.isBalanced
	topology.Messages = nil

	return topology
}

func stateToTaskList(s state) *service.TaskList {
	tasks := &service.TaskList{}

	tasks.Rev = EncodeRev(s.rev)
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
func (m *ServiceMgr) recoverRebalance() {

	m.mu.Lock()
	defer m.mu.Unlock()

	m.indexerReady = true

	if m.cleanupPending {
		l.Infof("ServiceMgr::recoverRebalance Init Pending Cleanup")

		rtokens, err := m.getCurrRebalTokens()
		if err != nil {
			l.Errorf("ServiceMgr::recoverRebalance Error Fetching Metakv Tokens %v", err)
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

func (m *ServiceMgr) doRecoverRebalance(gtoken *RebalanceToken) {

	l.Infof("ServiceMgr::doRecoverRebalance Found Global Rebalance Token %v", gtoken)

	if gtoken.MasterId == string(m.nodeInfo.NodeID) {
		m.runCleanupPhaseLOCKED(RebalanceTokenPath, true)
	} else {
		m.runCleanupPhaseLOCKED(RebalanceTokenPath, false)
	}

}

func (m *ServiceMgr) doRecoverMoveIndex(gtoken *RebalanceToken) {

	l.Infof("ServiceMgr::doRecoverMoveIndex Found Global Rebalance Token %v.", gtoken)
	m.runCleanupPhaseLOCKED(MoveIndexTokenPath, true)
}

func (m *ServiceMgr) checkLocalCleanupPending() bool {

	rtokens, err := m.getCurrRebalTokens()
	if err != nil {
		l.Errorf("ServiceMgr::checkLocalCleanupPending Error Fetching Metakv Tokens %v", err)
		return true
	}

	if rtokens != nil && len(rtokens.TT) != 0 {
		for _, tt := range rtokens.TT {
			ownerId := m.getTransferTokenOwner(tt)
			if ownerId == string(m.nodeInfo.NodeID) {
				l.Infof("ServiceMgr::checkLocalCleanupPending Found Local Pending Cleanup Token %v", tt)
				return true
			}
		}
	}

	return false
}

func (m *ServiceMgr) checkGlobalCleanupPending() bool {

	rtokens, err := m.getCurrRebalTokens()
	if err != nil {
		l.Errorf("ServiceMgr::checkGlobalCleanupPending Error Fetching Metakv Tokens %v", err)
		return true
	}

	if rtokens != nil && len(rtokens.TT) != 0 {
		for _, tt := range rtokens.TT {
			ownerId := m.getTransferTokenOwner(tt)
			for _, s := range m.servers {
				if ownerId == string(s) {
					l.Infof("ServiceMgr::checkGlobalCleanupPending Found Global Pending Cleanup for Owner %v Token %v", ownerId, tt)
					return true
				}
			}
			l.Infof("ServiceMgr::checkGlobalCleanupPending Found Global Pending Cleanup Token Without Owner Token %v", tt)
		}
	}

	return false
}

func (m *ServiceMgr) getTransferTokenOwner(tt *c.TransferToken) string {

	switch tt.State {
	case c.TransferTokenReady:
		return tt.SourceId
	case c.TransferTokenCreated, c.TransferTokenAccepted, c.TransferTokenRefused,
		c.TransferTokenInitate, c.TransferTokenInProgress, c.TransferTokenMerge:
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

func (m *ServiceMgr) handleListRebalanceTokens(w http.ResponseWriter, r *http.Request) {

	_, ok := m.validateAuth(w, r)
	if !ok {
		l.Errorf("ServiceMgr::handleListRebalanceTokens Validation Failure req: %v", c.GetHTTPReqInfo(r))
		return
	}

	if r.Method == "GET" {

		l.Infof("ServiceMgr::handleListRebalanceTokens Processing Request req: %v", c.GetHTTPReqInfo(r))
		rinfo, err := m.getCurrRebalTokens()
		if err != nil {
			l.Errorf("ServiceMgr::handleListRebalanceTokens Error %v", err)
			m.writeError(w, err)
			return
		}
		out, err1 := json.Marshal(rinfo)
		if err1 != nil {
			l.Errorf("ServiceMgr::handleListRebalanceTokens Error %v", err1)
			m.writeError(w, err1)
		} else {
			m.writeJson(w, out)
		}
	} else {
		m.writeError(w, errors.New("Unsupported method"))
		return
	}

}

func (m *ServiceMgr) handleCleanupRebalance(w http.ResponseWriter, r *http.Request) {

	_, ok := m.validateAuth(w, r)
	if !ok {
		l.Errorf("ServiceMgr::handleCleanupRebalance Validation Failure req: %v", c.GetHTTPReqInfo(r))
		return
	}

	if r.Method == "GET" || r.Method == "POST" {

		l.Infof("ServiceMgr::handleCleanupRebalance Processing Request req: %v", c.GetHTTPReqInfo(r))
		m.mu.Lock()
		defer m.mu.Unlock()

		if !m.indexerReady {
			l.Errorf("ServiceMgr::handleCleanupRebalance Cannot Process Request %v", c.ErrIndexerInBootstrap)
			m.writeError(w, c.ErrIndexerInBootstrap)
			return
		}

		rtokens, err := m.getCurrRebalTokens()
		if err != nil {
			l.Errorf("ServiceMgr::handleCleanupRebalance Error %v", err)
		}

		if rtokens != nil {
			if rtokens.RT != nil {
				m.isBalanced = false
				err = m.runCleanupPhaseLOCKED(RebalanceTokenPath, true)
				if err != nil {
					l.Errorf("ServiceMgr::handleCleanupRebalance RebalanceTokenPath Error %v", err)
				}
			}
			if rtokens.MT != nil {
				err = m.runCleanupPhaseLOCKED(MoveIndexTokenPath, true)
				if err != nil {
					l.Errorf("ServiceMgr::handleCleanupRebalance MoveIndexTokenPath Error %v", err)
				}
			}
		}

		//local cleanup
		err = m.runCleanupPhaseLOCKED(RebalanceTokenPath, false)

		if err != nil {
			l.Errorf("ServiceMgr::handleCleanupRebalance Error %v", err)
			m.writeError(w, err)
		} else {
			m.writeOk(w)
		}

	} else {
		m.writeError(w, errors.New("Unsupported method"))
	}
}

func (m *ServiceMgr) handleNodeuuid(w http.ResponseWriter, r *http.Request) {

	_, ok := m.validateAuth(w, r)
	if !ok {
		l.Errorf("ServiceMgr::handleNodeuuid Validation Failure req: %v", c.GetHTTPReqInfo(r))
		return
	}

	if r.Method == "GET" || r.Method == "POST" {
		l.Infof("ServiceMgr::handleNodeuuid Processing Request req: %v", c.GetHTTPReqInfo(r))
		m.writeBytes(w, []byte(m.nodeInfo.NodeID))
		return
	} else {
		m.writeError(w, errors.New("Unsupported method"))
		return
	}
}

func (m *ServiceMgr) getCurrRebalTokens() (*RebalTokens, error) {

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
			l.Errorf("ServiceMgr::getCurrRebalTokens Unknown Token %v. Ignored.", kv)
		}

	}

	return &rinfo, nil
}

func (m *ServiceMgr) handleRegisterRebalanceToken(w http.ResponseWriter, r *http.Request) {

	_, ok := m.validateAuth(w, r)
	if !ok {
		l.Errorf("ServiceMgr::handleRegisterRebalanceToken Validation Failure req: %v", c.GetHTTPReqInfo(r))
		return
	}

	var rebalToken RebalanceToken
	if r.Method == "POST" {
		bytes, _ := ioutil.ReadAll(r.Body)
		if err := json.Unmarshal(bytes, &rebalToken); err != nil {
			l.Errorf("ServiceMgr::handleRegisterRebalanceToken %v", err)
			m.writeError(w, err)
			return
		}

		l.Infof("ServiceMgr::handleRegisterRebalanceToken New Rebalance Token %v", rebalToken)

		if m.observeGlobalRebalanceToken(rebalToken) {

			m.mu.Lock()
			defer m.mu.Unlock()

			if !m.rebalanceRunning {
				errStr := fmt.Sprintf("Node %v not in Prepared State for Rebalance", string(m.nodeInfo.NodeID))
				l.Errorf("ServiceMgr::handleRegisterRebalanceToken %v", errStr)
				m.writeError(w, errors.New(errStr))
				return
			}

			m.rebalanceToken = &rebalToken
			if err := m.registerLocalRebalanceToken(); err != nil {
				l.Errorf("ServiceMgr::handleRegisterRebalanceToken %v", err)
				m.writeError(w, err)
				return
			}

			m.rebalancerF = NewRebalancer(nil, &rebalToken, string(m.nodeInfo.NodeID),
				false, nil, m.rebalanceDoneCallback, m.supvMsgch,
				m.localhttp, m.config.Load(), nil, false, &m.p)
			m.writeOk(w)
			return

		} else {
			err := errors.New("Rebalance Token Wait Timeout")
			l.Errorf("ServiceMgr::handleRegisterRebalanceToken %v", err)
			m.writeError(w, err)
			return
		}

	} else {
		m.writeError(w, errors.New("Unsupported method"))
		return
	}

}

func getGlobalTopology(addr string) (*manager.ClusterIndexMetadata, error) {

	url := "/getIndexMetadata"

	resp, err := getWithAuth(addr + url)
	if err != nil {
		l.Errorf("ServiceMgr::getGlobalTopology Error gathering global topology %v %v", addr+url, err)
		return nil, err
	}

	var topology manager.BackupResponse
	bytes, _ := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err := json.Unmarshal(bytes, &topology); err != nil {
		l.Errorf("ServiceMgr::getGlobalTopology Error unmarshal global topology %v %v %s",
			addr+url, err, l.TagUD(string(bytes)))
		return nil, err
	}

	return &topology.Result, nil

}

/////////////////////////////////////////////////////////////////////////
//
//  move index implementation
//
/////////////////////////////////////////////////////////////////////////

func (m *ServiceMgr) listenMoveIndex() {

	l.Infof("ServiceMgr::listenMoveIndex %v", m.nodeInfo)

	cancel := make(chan struct{})
	for {
		err := metakv.RunObserveChildren(RebalanceMetakvDir, m.processMoveIndex, cancel)
		if err != nil {
			l.Infof("ServiceMgr::listenMoveIndex metakv err %v. Retrying...", err)
			time.Sleep(2 * time.Second)
		}
	}

}

func (m *ServiceMgr) processMoveIndex(path string, value []byte, rev interface{}) error {

	if path == MoveIndexTokenPath {
		l.Infof("ServiceMgr::processMoveIndex MoveIndexToken Received %v %v", path, value)

		var rebalToken RebalanceToken
		if value == nil { //move index token deleted
			return nil
		} else {
			if err := json.Unmarshal(value, &rebalToken); err != nil {
				l.Errorf("ServiceMgr::processMoveIndex Error reading move index token %v", err)
				return err
			}
		}

		m.mu.Lock()
		defer m.mu.Unlock()

		//skip if token was generated by self
		if rebalToken.MasterId == string(m.nodeInfo.NodeID) {

			// if rebalanceToken matches
			if m.rebalanceToken != nil && m.rebalanceToken.RebalId == rebalToken.RebalId {

				// If master (source) receving a token with error, then cancel move index
				// and return the error to user.
				if len(rebalToken.Error) != 0 {
					l.Infof("ServiceMgr::processMoveIndex received error from destination")

					if m.rebalancer != nil {
						m.rebalancer.Cancel()
						m.rebalancer = nil
						m.moveStatusCh <- errors.New(rebalToken.Error)
						m.runCleanupPhaseLOCKED(MoveIndexTokenPath, true)
					}
					return nil
				}
			}

			l.Infof("ServiceMgr::processMoveIndex Skip MoveIndex Token for Self Node")
			return nil

		} else {
			// If destination receving a token with error, then skip.  The destination is
			// the one that posted the error.
			if len(rebalToken.Error) != 0 {
				l.Infof("ServiceMgr::processMoveIndex Skip MoveIndex Token with error")
				return nil
			}
		}

		if m.rebalanceRunning {
			err := errors.New("Cannot Process Move Index - Rebalance In Progress")
			l.Errorf("ServiceMgr::processMoveIndex %v %v", err, m.rebalanceToken)
			m.setErrorInMoveIndexToken(&rebalToken, err)
			return nil
		} else {
			m.rebalanceRunning = true
			m.rebalanceToken = &rebalToken
			var err error
			if err = m.registerRebalanceRunning(true); err != nil || m.p.ddlRunning {
				if m.p.ddlRunning {
					l.Errorf("ServiceMgr::processMoveIndex Found index build running. Cannot process move index.")
					fmtMsg := "move index failure - index build is in progress for indexes: %v."
					err = errors.New(fmt.Sprintf(fmtMsg, m.p.ddlRunningIndexNames))
				}
				m.setErrorInMoveIndexToken(&rebalToken, err)
				m.runCleanupPhaseLOCKED(MoveIndexTokenPath, false)
				return nil
			}

			if err = m.registerLocalRebalanceToken(); err != nil {
				m.setErrorInMoveIndexToken(&rebalToken, err)
				m.runCleanupPhaseLOCKED(MoveIndexTokenPath, false)
				return nil
			}
			m.rebalancerF = NewRebalancer(nil, &rebalToken, string(m.nodeInfo.NodeID),
				false, nil, m.moveIndexDoneCallback, m.supvMsgch,
				m.localhttp, m.config.Load(), nil, false, nil)
		}
	}

	return nil
}

func (m *ServiceMgr) handleMoveIndex(w http.ResponseWriter, r *http.Request) {

	creds, ok := m.validateAuth(w, r)
	if !ok {
		l.Errorf("ServiceMgr::handleMoveIndex Validation Failure req: %v", c.GetHTTPReqInfo(r))
		return
	}

	if r.Method == "POST" {

		bytes, _ := ioutil.ReadAll(r.Body)
		in := make(map[string]interface{})
		if err := json.Unmarshal(bytes, &in); err != nil {
			send(http.StatusBadRequest, w, err.Error())
			return
		}

		var bucket, index string
		var ok bool
		var nodes interface{}

		if bucket, ok = in["bucket"].(string); !ok {
			send(http.StatusBadRequest, w, "Bad Request - Bucket Information Missing")
			return
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

		permission := fmt.Sprintf("cluster.bucket[%s].n1ql.index!alter", bucket)
		if !c.IsAllowed(creds, []string{permission}, w) {
			return
		}

		topology, err := getGlobalTopology(m.localhttp)
		if err != nil {
			send(http.StatusInternalServerError, w, err.Error())
			return
		}

		var defn *manager.IndexDefnDistribution
		for _, localMeta := range topology.Metadata {
			bTopology := findTopologyByBucket(localMeta.IndexTopologies, bucket)
			if bTopology != nil {
				defn = bTopology.FindIndexDefinition(bucket, index)
			}
			if defn != nil {
				break
			}
		}
		if defn == nil {
			err := errors.New(fmt.Sprintf("Fail to find index definition for bucket %v index %v.", bucket, index))
			l.Errorf("ServiceMgr::handleMoveIndex %v", err)
			send(http.StatusInternalServerError, w, err.Error())
			return
		}

		var req manager.IndexRequest

		idList := client.IndexIdList{DefnIds: []uint64{defn.DefnId}}
		plan := make(map[string]interface{})
		plan["nodes"] = nodes

		req = manager.IndexRequest{IndexIds: idList, Plan: plan}

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

func (m *ServiceMgr) handleMoveIndexInternal(w http.ResponseWriter, r *http.Request) {

	creds, ok := m.validateAuth(w, r)
	if !ok {
		l.Errorf("ServiceMgr::handleMoveIndexInternal Validation Failure req: %v", c.GetHTTPReqInfo(r))
		return
	}

	if r.Method == "POST" {
		bytes, _ := ioutil.ReadAll(r.Body)
		var req manager.IndexRequest
		if err := json.Unmarshal(bytes, &req); err != nil {
			l.Errorf("ServiceMgr::handleMoveIndexInternal %v", err)
			sendIndexResponseWithError(http.StatusBadRequest, w, err.Error())
			return
		}

		if c.GetBuildMode() != c.ENTERPRISE {
			sendIndexResponseWithError(http.StatusBadRequest, w, "Alter index is only supported in Enterprise Edition")
			return
		}

		permission := fmt.Sprintf("cluster.bucket[%s].n1ql.index!alter", req.Index.Bucket)
		if !c.IsAllowed(creds, []string{permission}, w) {
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

func (m *ServiceMgr) doHandleMoveIndex(req *manager.IndexRequest) (int, string) {

	l.Infof("ServiceMgr::doHandleMoveIndex %v", l.TagUD(req))

	nodes, err := validateMoveIndexReq(req)
	if err != nil {
		l.Errorf("ServiceMgr::doHandleMoveIndex %v", err)
		return http.StatusBadRequest, err.Error()
	}

	err, noop := m.initMoveIndex(req, nodes)
	if err != nil {
		l.Errorf("ServiceMgr::doHandleMoveIndex %v %v", err, m.rebalanceToken)
		return http.StatusInternalServerError, err.Error()
	} else if noop {
		warnStr := "No Index Movement Required for Specified Destination List"
		l.Warnf("ServiceMgr::doHandleMoveIndex %v", warnStr)
		return http.StatusBadRequest, warnStr
	} else {
		go m.monitorMoveIndex()
		return http.StatusOK, ""
	}
}

func (m *ServiceMgr) monitorMoveIndex() {
	select {
	case err := <-m.moveStatusCh:
		if err != nil {
			cfg := m.config.Load()
			clusterAddr := cfg["clusterAddr"].String()
			l.Errorf("ServiceMgr::doHandleMoveIndex MoveIndex failed: %v", err.Error())
			c.Console(clusterAddr, fmt.Sprintf("MoveIndex failed: %v", err.Error()))
		} else {
			l.Infof("ServiceMgr: Move Index succeeded")
		}
	}
}

func (m *ServiceMgr) initMoveIndex(req *manager.IndexRequest, nodes []string) (error, bool) {

	m.mu.Lock()
	defer m.mu.Unlock()

	var err error
	if !m.indexerReady {
		l.Errorf("ServiceMgr::initMoveIndex Cannot Process Request %v", c.ErrIndexerInBootstrap)
		return c.ErrIndexerInBootstrap, false
	}

	if m.checkRebalanceRunning() {
		err = errors.New("Cannot Process Move Index - Rebalance/MoveIndex In Progress")
		l.Errorf("ServiceMgr::initMoveIndex %v", err)
		return err, false
	}

	if m.state.rebalanceID != "" {
		err = errors.New("Cannot Process Move Index - Failover In Progress")
		l.Errorf("ServiceMgr::initMoveIndex %v", err)
		return err, false
	}

	//check globally as move index init happens on only one node
	if m.checkGlobalCleanupPending() {
		err = errors.New("Cannot Process Move Index - cleanup pending from previous " +
			"failed/aborted rebalance/failover/move index. please retry the request later.")
		l.Errorf("ServiceMgr::initMoveIndex %v", err)
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

	l.Infof("ServiceMgr::handleMoveIndex New Move Index Token %v Dest %v", m.rebalanceToken, nodes)

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
			l.Errorf("ServiceMgr::handleMoveIndex Found index build running. Cannot process move index.")
			fmtMsg := "move index failure - index build is in progress for indexes: %v."
			err = errors.New(fmt.Sprintf(fmtMsg, m.p.ddlRunningIndexNames))
		}
		m.runCleanupPhaseLOCKED(MoveIndexTokenPath, false)
		return err, false
	}

	if err = m.registerLocalRebalanceToken(); err != nil {
		m.runCleanupPhaseLOCKED(MoveIndexTokenPath, false)
		return err, false
	}

	if err = m.registerMoveIndexTokenInMetakv(m.rebalanceToken); err != nil {
		m.runCleanupPhaseLOCKED(MoveIndexTokenPath, false)
		return err, false
	}

	//sleep for a bit to let the rebalance token propagate to other nodes via metakv.
	//unlike rebalance, this is not a fully coordinated step for move index.
	time.Sleep(2 * time.Second)

	rebalancer := NewRebalancer(transferTokens, m.rebalanceToken, string(m.nodeInfo.NodeID),
		true, nil, m.moveIndexDoneCallback, m.supvMsgch, m.localhttp, m.config.Load(), nil, false, nil)

	m.rebalancer = rebalancer
	m.rebalanceRunning = true
	return nil, false

}

func (m *ServiceMgr) genMoveIndexToken() error {

	cfg := m.config.Load()
	ustr, _ := c.NewUUID()
	m.rebalanceToken = &RebalanceToken{
		MasterId: cfg["nodeuuid"].String(),
		RebalId:  ustr.Str(),
		Source:   RebalSourceMoveIndex,
	}
	return nil
}

func (m *ServiceMgr) setErrorInMoveIndexToken(token *RebalanceToken, err error) error {

	token.Error = err.Error()

	if err := m.registerMoveIndexTokenInMetakv(token); err != nil {
		return err
	}

	l.Infof("ServiceMgr::setErrorInMoveIndexToken done")

	return nil
}

func (m *ServiceMgr) registerMoveIndexTokenInMetakv(token *RebalanceToken) error {

	err := c.MetakvSet(MoveIndexTokenPath, token)
	if err != nil {
		l.Errorf("ServiceMgr::registerMoveIndexTokenInMetakv Unable to set "+
			"RebalanceToken In Meta Storage. Err %v", err)
		return err
	}
	l.Infof("ServiceMgr::registerMoveIndexTokenInMetakv Registered Global Rebalance"+
		"Token In Metakv %v %v", MoveIndexTokenPath, token)

	return nil
}

func (m *ServiceMgr) generateTransferTokenForMoveIndex(req *manager.IndexRequest,
	reqNodes []string) (map[string]*c.TransferToken, error) {

	topology, err := getGlobalTopology(m.localhttp)
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

	l.Infof("ServiceMgr::handleMoveIndex nodes %v uuid %v", reqNodes, reqNodeUUID)

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
						l.Infof("ServiceMgr::generateTransferTokenForMoveIndex Skip Index %v. Already exist on dest %v.", index.DefnId, uuid)
						reqNodeUUID = append(reqNodeUUID[:i], reqNodeUUID[i+1:]...)
						break outerloop
					}
				}

				topology := findTopologyByBucket(localMeta.IndexTopologies, index.Bucket)
				if topology == nil {
					err := errors.New(fmt.Sprintf("Fail to find index topology for bucket %v for node %v.", index.Bucket, localMeta.NodeUUID))
					l.Errorf("ServiceMgr::generateTransferTokenForMoveIndex %v", err)
					return nil, err
				}

				insts := topology.GetIndexInstancesByDefn(index.DefnId)
				if len(insts) == 0 {
					err := errors.New(fmt.Sprintf("Fail to find index instance for definition %v for node %v.", index.DefnId, localMeta.NodeUUID))
					l.Errorf("ServiceMgr::generateTransferTokenForMoveIndex %v", err)
					return nil, err
				}

				cfg := m.config.Load()
				numVbuckets := cfg["numVbuckets"].Int()

				var instList []*c.IndexInst
				for _, inst := range insts {

					pc := c.NewKeyPartitionContainer(numVbuckets, int(inst.NumPartitions), index.PartitionScheme, index.HashScheme)
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
		l.Errorf("ServiceMgr::generateTransferTokenForMoveIndex %v", err)
		return nil, err
	}

	if len(currNodeUUID) != len(reqNodeUUID) {
		err := errors.New(fmt.Sprintf("Server error in computing new destination for index. "+
			"Request Nodes %v. Curr Nodes %v", reqNodeUUID, currNodeUUID))
		l.Errorf("ServiceMgr::generateTransferTokenForMoveIndex %v", err)
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
				l.Infof("ServiceMgr::generateTransferTokenForMoveIndex Skip No-op TransferToken %v", tt)
				continue
			}

			l.Infof("ServiceMgr::generateTransferTokenForMoveIndex Generated TransferToken %v %v", ttid, tt)
			transferTokens[ttid] = tt
		}

	}

	return transferTokens, nil

}

func (m *ServiceMgr) getNodeIdFromDest(dest string) (string, error) {

	m.cinfo.Lock()
	defer m.cinfo.Unlock()

	if err := m.cinfo.Fetch(); err != nil {
		l.Errorf("ServiceMgr::getNodeIdFromDest Error Fetching Cluster Information %v", err)
		return "", err
	}

	nids := m.cinfo.GetNodesByServiceType(c.INDEX_HTTP_SERVICE)
	url := "/nodeuuid"

	for _, nid := range nids {

		// TODO: Check this when user can specify encrypted port from query.
		maddr, err := m.cinfo.GetServiceAddress(nid, "mgmt", false)
		if err != nil {
			return "", err
		}

		if maddr == dest {

			haddr, err := m.cinfo.GetServiceAddress(nid, c.INDEX_HTTP_SERVICE, true)
			if err != nil {
				return "", err
			}

			resp, err := getWithAuth(haddr + url)
			if err != nil {
				l.Errorf("ServiceMgr::getNodeIdFromDest Unable to Fetch Node UUID %v %v", haddr, err)
				return "", err
			} else {
				bytes, _ := ioutil.ReadAll(resp.Body)
				defer resp.Body.Close()
				return string(bytes), nil
			}
		}

	}
	errStr := fmt.Sprintf("Unable to find Index service for destination %v or desintation is not part of the cluster", dest)
	l.Errorf("ServiceMgr::getNodeIdFromDest %v", errStr)

	return "", errors.New(errStr)
}

func (m *ServiceMgr) genTransferToken(indexInst *c.IndexInst, sourceId string, destId string) (string, *c.TransferToken, error) {

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

func (m *ServiceMgr) moveIndexDoneCallback(err error, cancel <-chan struct{}) {
	m.runRebalanceCallback(cancel, func() { m.onMoveIndexDoneLOCKED(err) })
}

func (m *ServiceMgr) onMoveIndexDoneLOCKED(err error) {

	if err != nil {
		l.Errorf("ServiceMgr::onMoveIndexDone Err %v", err)
	}

	l.Infof("ServiceMgr::onMoveIndexDone Cleanup")

	if m.rebalancer != nil {
		m.runCleanupPhaseLOCKED(MoveIndexTokenPath, true)
		m.moveStatusCh <- err
	} else {
		m.runCleanupPhaseLOCKED(MoveIndexTokenPath, false)
	}
	m.rebalancer = nil
	m.rebalancerF = nil
}

func validateMoveIndexReq(req *manager.IndexRequest) ([]string, error) {

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

func (m *ServiceMgr) setLocalMeta(key string, value string) error {
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

func (m *ServiceMgr) getLocalMeta(key string) (string, error) {
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

func (m *ServiceMgr) resetRunParams() {
	m.p.ddlRunning = false
	m.p.ddlRunningIndexNames = nil
}

func (m *ServiceMgr) writeOk(w http.ResponseWriter) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK\n"))
}

func (m *ServiceMgr) writeError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusBadRequest)
	w.Write([]byte(err.Error() + "\n"))
}

func (m *ServiceMgr) writeJson(w http.ResponseWriter, json []byte) {
	header := w.Header()
	header["Content-Type"] = []string{"application/json"}
	w.WriteHeader(http.StatusOK)
	w.Write(json)
	w.Write([]byte("\n"))
}

func (m *ServiceMgr) writeBytes(w http.ResponseWriter, bytes []byte) {
	w.WriteHeader(http.StatusOK)
	w.Write(bytes)
}

func (m *ServiceMgr) validateAuth(w http.ResponseWriter, r *http.Request) (cbauth.Creds, bool) {
	creds, valid, err := c.IsAuthValid(r)
	if err != nil {
		m.writeError(w, err)
	} else if valid == false {
		w.WriteHeader(401)
		w.Write([]byte("401 Unauthorized\n"))
	}
	return creds, valid
}

func (m *ServiceMgr) getLocalHttpAddr() string {

	cfg := m.config.Load()
	addr := cfg["clusterAddr"].String()
	host, _, _ := net.SplitHostPort(addr)
	port := cfg["httpPort"].String()
	return net.JoinHostPort(host, port)

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
	res := &manager.IndexResponse{Code: manager.RESP_ERROR, Error: msg}
	send(status, w, res)
}

func sendIndexResponse(w http.ResponseWriter) {
	result := &manager.IndexResponse{Code: manager.RESP_SUCCESS}
	send(http.StatusOK, w, result)
}

func sendIndexResponseMsg(w http.ResponseWriter, msg string) {
	result := &manager.IndexResponse{Code: manager.RESP_SUCCESS, Message: msg}
	send(http.StatusOK, w, result)
}

func send(status int, w http.ResponseWriter, res interface{}) {

	header := w.Header()
	header["Content-Type"] = []string{"application/json"}

	if buf, err := json.Marshal(res); err == nil {
		w.WriteHeader(status)
		l.Debugf("ServiceMgr::sendResponse: sending response back to caller. %v", string(buf))
		w.Write(buf)
	} else {
		// note : buf is nil if err != nil
		l.Debugf("ServiceMgr::sendResponse: fail to marshall response back to caller. %s", err)
		sendHttpError(w, "ServiceMgr::sendResponse: Unable to marshall response", http.StatusInternalServerError)
	}
}

func sendHttpError(w http.ResponseWriter, reason string, code int) {
	http.Error(w, reason, code)
}

func findTopologyByBucket(topologies []manager.IndexTopology, bucket string) *manager.IndexTopology {

	for _, topology := range topologies {
		if topology.Bucket == bucket {
			return &topology
		}
	}

	return nil
}
