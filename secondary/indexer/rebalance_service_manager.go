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
	"github.com/couchbase/indexing/secondary/planner"
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

	rebalanceID   string
	rebalanceTask *service.Task
}

func NewRebalanceMgr(supvCmdch MsgChannel, supvMsgch MsgChannel, config c.Config,
	rebalanceRunning bool, rebalanceToken *RebalanceToken) (RebalanceMgr, Message) {

	l.Infof("RebalanceMgr::NewRebalanceMgr %v %v ", rebalanceRunning, rebalanceToken)

	mu := &sync.RWMutex{}

	mgr := &ServiceMgr{
		mu: mu,
		state: state{
			rev:           0,
			servers:       make([]service.NodeID, 0),
			rebalanceID:   "",
			rebalanceTask: nil,
		},
		supvCmdch: supvCmdch,
		supvMsgch: supvMsgch,
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

	mgr.cinfo = cinfo

	mgr.waiters = make(waiters)

	mgr.nodeInfo = &service.NodeInfo{
		NodeID: service.NodeID(config["nodeuuid"].String()),
	}

	mgr.servers = append(mgr.servers, mgr.nodeInfo.NodeID)

	mgr.rebalanceRunning = rebalanceRunning
	mgr.rebalanceToken = rebalanceToken
	mgr.localhttp = mgr.getLocalHttpAddr()

	go mgr.recoverRebalance()

	//start Rebalance Manager loop which listens to commands from its supervisor
	go mgr.registerWithServer()
	go mgr.run()
	go mgr.listenMoveIndex()

	http.HandleFunc("/registerRebalanceToken", mgr.handleRegisterRebalanceToken)
	http.HandleFunc("/listRebalanceTokens", mgr.handleListRebalanceTokens)
	http.HandleFunc("/cleanupRebalance", mgr.handleCleanupRebalance)
	http.HandleFunc("/moveIndex", mgr.handleMoveIndex)
	http.HandleFunc("/nodeuuid", mgr.handleNodeuuid)

	return mgr, &MsgSuccess{}

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
		return service.ErrConflict
	}

	var err error
	if change.Type == service.TopologyChangeTypeFailover {
		err = m.prepareFailover(change)
	} else if change.Type == service.TopologyChangeTypeRebalance {
		err = m.prepareRebalance(change)
	} else {
		err = service.ErrNotSupported
	}

	if err != nil {
		return err
	}

	m.updateStateLOCKED(func(s *state) {
		s.rebalanceID = change.ID
	})

	return nil

}

func (m *ServiceMgr) prepareFailover(change service.TopologyChange) error {

	var err error
	if m.rebalanceToken != nil && m.rebalanceToken.Source == RebalSourceClusterOp {

		l.Infof("ServiceMgr::prepareFailover Found Rebalance In Progress %v", m.rebalanceToken)

		if m.rebalancerF != nil {
			m.rebalancerF.Cancel()
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

		if m.rebalanceToken.MasterId == string(m.nodeInfo.NodeID) {
			if m.rebalancer != nil {
				m.rebalancer.Cancel()
			}
			if err = m.runCleanupPhaseLOCKED(RebalanceTokenPath, true); err != nil {
				return err
			}
		} else {
			if m.rebalancerF != nil {
				m.rebalancerF.Cancel()
			}
			if err = m.runCleanupPhaseLOCKED(RebalanceTokenPath, false); err != nil {
				return err
			}
		}

		masterAlive := false
		for _, node := range change.KeepNodes {
			if m.rebalanceToken.MasterId == string(node.NodeInfo.NodeID) {
				masterAlive = true
				break
			}
		}
		if !masterAlive {
			l.Infof("ServiceMgr::prepareFailover Master Missing From Cluster Node List. Cleanup MoveIndex")
			err = m.runCleanupPhaseLOCKED(RebalanceTokenPath, true)
		}
		return err
	}

	return nil

}

func (m *ServiceMgr) prepareRebalance(change service.TopologyChange) error {

	var err error
	if m.rebalanceToken != nil && m.rebalanceToken.Source == RebalSourceMoveIndex {
		err = errors.New("indexer rebalance failure - move index in progress")
		l.Errorf("ServiceMgr::prepareRebalance %v", err)
		return err
	}

	if m.checkDDLRunning() {
		l.Errorf("ServiceMgr::prepareRebalance Found DDL Running. Cannot Initiate Prepare Phase")
		return errors.New("indexer rebalance failure - ddl in progress")
	}

	if m.rebalanceToken != nil && m.rebalanceToken.Source == RebalSourceClusterOp {
		l.Warnf("ServiceMgr::prepareRebalance Found Rebalance In Progress. Cleanup.")
		if m.rebalancerF != nil {
			m.rebalancerF.Cancel()
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

	l.Infof("ServiceMgr::prepareRebalance Init Prepare Phase")

	if isSingleNodeRebal(change) {
		if change.KeepNodes[0].NodeInfo.NodeID == m.nodeInfo.NodeID {
			l.Infof("ServiceMgr::prepareRebalance I'm the only node in cluster. Nothing to do.")
		} else {
			err := errors.New("indexer - node receiving prepare request not part of cluster")
			l.Errorf("ServiceMgr::prepareRebalance %v", err)
			return err
		}
	} else {
		if err := m.initPreparePhaseRebalance(); err != nil {
			return err
		}
	}

	return nil
}

func (m *ServiceMgr) StartTopologyChange(change service.TopologyChange) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	l.Infof("ServiceMgr::StartTopologyChange %v", change)

	if m.state.rebalanceID != change.ID || m.rebalancer != nil {
		l.Errorf("ServiceMgr::StartTopologyChange err %v %v %v %v", service.ErrConflict,
			m.state.rebalanceID, change.ID, m.rebalancer)
		return service.ErrConflict
	}

	if change.CurrentTopologyRev != nil {
		haveRev := DecodeRev(change.CurrentTopologyRev)
		if haveRev != m.state.rev {
			l.Errorf("ServiceMgr::StartTopologyChange err %v %v %v", service.ErrConflict,
				haveRev, m.state.rev)
			return service.ErrConflict
		}
	}

	var err error
	if change.Type == service.TopologyChangeTypeFailover {
		err = m.startFailover(change)
	} else if change.Type == service.TopologyChangeTypeRebalance {
		err = m.startRebalance(change)
	} else {
		err = service.ErrNotSupported
	}

	return err
}

func (m *ServiceMgr) startFailover(change service.TopologyChange) error {

	m.cleanupOrphanTokens(change)

	ctx := &rebalanceContext{
		rev:    0,
		change: change,
	}

	m.rebalanceCtx = ctx
	m.updateRebalanceProgressLOCKED(0)

	//TODO Directly call rebalanceDone?
	m.rebalancer = NewRebalancer(nil, nil, string(m.nodeInfo.NodeID), true,
		m.rebalanceProgressCallback, m.rebalanceDoneCallback, m.supvMsgch, "")

	return nil
}

func (m *ServiceMgr) startRebalance(change service.TopologyChange) error {

	var topology *manager.ClusterIndexMetadata
	var transferTokens map[string]*c.TransferToken
	if isSingleNodeRebal(change) {
		if change.KeepNodes[0].NodeInfo.NodeID == m.nodeInfo.NodeID {
			l.Infof("ServiceMgr::startRebalance I'm the only node in cluster. Nothing to do.")
		} else {
			err := errors.New("Node receiving Start request not part of cluster")
			l.Errorf("ServiceMgr::startRebalance %v Self %v Cluster %v", err, m.nodeInfo.NodeID,
				change.KeepNodes[0].NodeInfo.NodeID)
			m.runCleanupPhaseLOCKED(RebalanceTokenPath, true)
			return err
		}
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

		err = m.initStartPhase()
		if err != nil {
			l.Errorf("ServiceMgr::startRebalance Error During Start Phase Init %v", err)
			m.runCleanupPhaseLOCKED(RebalanceTokenPath, true)
			return err
		}

		topology, err = m.getGlobalTopology()
		if err != nil {
			m.runCleanupPhaseLOCKED(RebalanceTokenPath, true)
			return err
		}

		l.Infof("ServiceMgr::startRebalance Global Topology %v", topology)

		cfg := m.config.Load()
		start := time.Now()
		if cfg["rebalance.disable_index_move"].Bool() {
			l.Infof("ServiceMgr::startRebalance skip planner as disable_index_move is set")

		} else if cfg["rebalance.use_simple_planner"].Bool() {
			planner := NewSimplePlanner(topology, change, string(m.nodeInfo.NodeID))
			transferTokens = planner.PlanIndexMoves()
		} else {
			onEjectOnly := cfg["rebalance.node_eject_only"].Bool()
			transferTokens, err = planner.ExecuteRebalance(cfg["clusterAddr"].String(), change,
				string(m.nodeInfo.NodeID), onEjectOnly)
			if err != nil {
				l.Errorf("ServiceMgr::startRebalance Planner Error %v", err)
				m.runCleanupPhaseLOCKED(RebalanceTokenPath, true)
				return err
			}
			if len(transferTokens) == 0 {
				transferTokens = nil
			}
		}
		elapsed := time.Since(start)
		l.Infof("ServiceMgr::startRebalance Planner Time Taken %v", elapsed)
	}

	ctx := &rebalanceContext{
		rev:    0,
		change: change,
	}

	m.rebalanceCtx = ctx
	m.updateRebalanceProgressLOCKED(0)

	m.rebalancer = NewRebalancer(transferTokens, m.rebalanceToken, string(m.nodeInfo.NodeID),
		true, m.rebalanceProgressCallback, m.rebalanceDoneCallback, m.supvMsgch, m.localhttp)

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

func (m *ServiceMgr) checkDDLRunning() bool {

	respCh := make(chan bool)
	m.supvMsgch <- &MsgCheckDDLInProgress{respCh: respCh}
	return <-respCh
}

func (m *ServiceMgr) checkRebalanceRunning() bool {
	return m.rebalanceRunning
}

func (m *ServiceMgr) checkExistingGlobalRToken() (*RebalanceToken, error) {

	var rtoken RebalanceToken
	var found bool
	var err error
	found, err = MetakvGet(RebalanceTokenPath, &rtoken)
	if err != nil {
		l.Errorf("ServiceMgr::checkExistingGlobalRToken Error Fetching "+
			"Rebalance Token From Metakv %v. Path %v", err, RebalanceTokenPath)
		return nil, err
	}
	if found {
		return &rtoken, nil
	}

	found, err = MetakvGet(MoveIndexTokenPath, &rtoken)
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

	err := m.registerRebalanceRunning()
	if err != nil {
		return err
	}

	m.rebalanceRunning = true
	m.monitorStopCh = make(StopChannel)

	go m.monitorStartPhaseInit(m.monitorStopCh)

	return nil
}

func (m *ServiceMgr) initPreparePhaseFailover() error {

	err := m.registerRebalanceRunning()
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

	err = m.cleanupLocalRToken()
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
	found, err := MetakvGet(path, &rtoken)
	if err != nil {
		l.Errorf("ServiceMgr::cleanupGlobalRToken Error Fetching Rebalance Token From Metakv %v. Path %v", err, path)
		return err
	}

	if found {
		l.Infof("ServiceMgr::cleanupGlobalRToken Found Global Rebalance Token In Metakv %v", rtoken)

		if rtoken.MasterId == string(m.nodeInfo.NodeID) {
			l.Infof("ServiceMgr::cleanupGlobalRToken Delete Global Rebalance Token As Master %v", rtoken)

			err := MetakvDel(path)
			if err != nil {
				l.Fatalf("ServiceMgr::cleanupGlobalRToken Unable to delete RebalanceToken from "+
					"Meta Storage. %v. Err %v", rtoken, err)
				return err
			}
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
			err := MetakvDel(RebalanceTokenPath)
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
			err := MetakvDel(MoveIndexTokenPath)
			if err != nil {
				l.Errorf("ServiceMgr::cleanupOrphanTokens Unable to delete MoveIndex Token from "+
					"Meta Storage. %v. Err %v", rtokens.MT, err)
				return err
			}
		}
	}

	for ttid, tt := range rtokens.TT {

		masterAlive = false
		for _, node := range change.KeepNodes {
			if tt.MasterId == string(node.NodeInfo.NodeID) {
				masterAlive = true
				break
			}
		}

		if !masterAlive {
			l.Infof("ServiceMgr::cleanupOrphanTokens Cleaning Up Token %v %v", ttid, tt)
			err := MetakvDel(RebalanceMetakvDir + ttid)
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

	for ttid, tt := range tts {

		l.Infof("ServiceMgr::cleanupTransferTokens Cleaning Up %v %v", ttid, tt)

		if tt.MasterId == string(m.nodeInfo.NodeID) {
			m.cleanupTransferTokensForMaster(ttid, tt)
		}
		if tt.SourceId == string(m.nodeInfo.NodeID) {
			m.cleanupTransferTokensForSource(ttid, tt)
		}
		if tt.DestId == string(m.nodeInfo.NodeID) {
			m.cleanupTransferTokensForDest(ttid, tt)
		}

	}

	return nil
}

func (m *ServiceMgr) cleanupTransferTokensForMaster(ttid string, tt *c.TransferToken) error {

	switch tt.State {

	case c.TransferTokenCommit:
	case c.TransferTokenDeleted:
		l.Infof("ServiceMgr::cleanupTransferTokensForMaster Cleanup Token %v %v", ttid, tt)
		err := MetakvDel(RebalanceMetakvDir + ttid)
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
		err = m.cleanupIndex(tt.IndexInst.Defn)
		if err == nil {
			err = MetakvDel(RebalanceMetakvDir + ttid)
			if err != nil {
				l.Errorf("ServiceMgr::cleanupTransferTokensForSource Unable to delete TransferToken In "+
					"Meta Storage. %v. Err %v", tt, err)
				return err
			}
		}
	}

	return nil

}

func (m *ServiceMgr) cleanupTransferTokensForDest(ttid string, tt *c.TransferToken) error {

	switch tt.State {

	case c.TransferTokenCreated, c.TransferTokenAccepted, c.TransferTokenRefused,
		c.TransferTokenInitate, c.TransferTokenInProgress:
		var err error
		l.Infof("ServiceMgr::cleanupTransferTokensForDest Cleanup Token %v %v", ttid, tt)
		err = m.cleanupIndex(tt.IndexInst.Defn)
		if err == nil {
			err = MetakvDel(RebalanceMetakvDir + ttid)
			if err != nil {
				l.Errorf("ServiceMgr::cleanupTransferTokensForDest Unable to delete TransferToken In "+
					"Meta Storage. %v. Err %v", tt, err)
				return err
			}
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
	response := new(manager.IndexResponse)
	bytes, _ := ioutil.ReadAll(resp.Body)
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
				if m.rebalanceToken == nil && elapsed > StartPhaseBeginTimeout {
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

func (m *ServiceMgr) initStartPhase() error {

	var err error
	if err = m.genRebalanceToken(); err != nil {
		return err
	}

	if err = m.registerLocalRebalanceToken(); err != nil {
		return err
	}

	if err = m.registerRebalanceTokenInMetakv(); err != nil {
		return err
	}

	if err = m.registerGlobalRebalanceToken(); err != nil {
		return err
	}

	return nil
}

func (m *ServiceMgr) genRebalanceToken() error {

	cfg := m.config.Load()
	m.rebalanceToken = &RebalanceToken{
		MasterId: cfg["nodeuuid"].String(),
		RebalId:  m.state.rebalanceID,
		Source:   RebalSourceClusterOp,
	}
	return nil
}

func (m *ServiceMgr) registerRebalanceRunning() error {
	respch := make(MsgChannel)
	m.supvMsgch <- &MsgClustMgrLocal{
		mType:  CLUST_MGR_SET_LOCAL,
		key:    RebalanceRunning,
		value:  "",
		respch: respch,
	}

	respMsg := <-respch
	resp := respMsg.(*MsgClustMgrLocal)

	errMsg := resp.GetError()
	if errMsg != nil {
		l.Errorf("ServiceMgr::registerRebalanceRunning Unable to set RebalanceRunning In Local"+
			"Meta Storage. Err %v", errMsg)
		return errMsg
	}

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

	err := MetakvSet(RebalanceTokenPath, m.rebalanceToken)
	if err != nil {
		l.Errorf("ServiceMgr::registerRebalanceTokenInMetakv Unable to set RebalanceToken In "+
			"Meta Storage. Err %v", err)
		return err
	}
	l.Infof("ServiceMgr::registerRebalanceTokenInMetakv Registered Global Rebalance Token In Metakv %v", m.rebalanceToken)

	return nil
}

func (m *ServiceMgr) observeGlobalRebalanceToken(rebalToken RebalanceToken) bool {

	elapsed := 1

	for elapsed < RebalanceTokenWaitTimeout {

		var rtoken RebalanceToken
		found, err := MetakvGet(RebalanceTokenPath, &rtoken)
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

func (m *ServiceMgr) registerGlobalRebalanceToken() error {

	m.cinfo.Lock()
	defer m.cinfo.Unlock()

	if err := m.cinfo.Fetch(); err != nil {
		l.Errorf("ServiceMgr::registerGlobalRebalanceToken Error Fetching Cluster Information %v", err)
		return err
	}

	nids := m.cinfo.GetNodesByServiceType(c.INDEX_HTTP_SERVICE)
	url := "/registerRebalanceToken"

	for _, nid := range nids {

		addr, err := m.cinfo.GetServiceAddress(nid, c.INDEX_HTTP_SERVICE)
		if err == nil {

			localaddr, err := m.cinfo.GetLocalServiceAddress(c.INDEX_HTTP_SERVICE)
			if err != nil {
				l.Errorf("ServiceMgr::registerGlobalRebalanceToken Error Fetching Local Service Address %v", err)
				return errors.New(fmt.Sprintf("Fail to retrieve http endpoint for local node %v", err))
			}

			if addr == localaddr {
				l.Infof("ServiceMgr::registerGlobalRebalanceToken skip local service %v", addr)
				continue
			}

			body, err := json.Marshal(&m.rebalanceToken)
			if err != nil {
				l.Errorf("ServiceMgr::registerGlobalRebalanceToken Error registering rebalance token on %v %v", addr+url, err)
				return err
			}

			bodybuf := bytes.NewBuffer(body)

			resp, err := postWithAuth(addr+url, "application/json", bodybuf)
			defer resp.Body.Close()
			if err != nil {
				l.Errorf("ServiceMgr::registerGlobalRebalanceToken Error registering rebalance token on %v %v", addr+url, err)
				return err
			}

		} else {
			l.Errorf("ServiceMgr::registerGlobalRebalanceToken Error Fetching Service Address %v", err)
			return errors.New(fmt.Sprintf("Fail to retrieve http endpoint for index node %v", err))
		}

		l.Infof("ServiceMgr::registerGlobalRebalanceToken Successfully registered rebalance token on %v", addr+url)
	}

	return nil

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
	m.runRebalanceCallback(cancel, func() { m.onRebalanceDoneLOCKED(err) })
}

func (m *ServiceMgr) onRebalanceDoneLOCKED(err error) {
	if m.rebalancer != nil {
		newTask := (*service.Task)(nil)
		newServers := make([]service.NodeID, 0)
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
		} else {
			ctx := m.rebalanceCtx
			for _, node := range ctx.change.KeepNodes {
				newServers = append(newServers, node.NodeInfo.NodeID)
			}
		}

		m.runCleanupPhaseLOCKED(RebalanceTokenPath, true)
		m.rebalanceCtx = nil

		m.updateStateLOCKED(func(s *state) {
			s.rebalanceTask = newTask
			s.rebalanceID = ""
			if len(newServers) != 0 {
				s.servers = newServers
			}
		})
	} else {
		m.runCleanupPhaseLOCKED(RebalanceTokenPath, false)
	}

	m.rebalancer = nil
	m.rebalancerF = nil

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
		return state{}, service.ErrCanceled
	case newState := <-ch:
		return newState, nil
	}
}

func (m *ServiceMgr) copyStateLOCKED() state {
	s := m.state

	return s
}

func (m *ServiceMgr) cancelActualTaskLOCKED(task *service.Task) error {
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
	m.onRebalanceDoneLOCKED(nil)

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
	topology.Nodes = append([]service.NodeID(nil), m.servers...)
	topology.IsBalanced = true
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

func (m *ServiceMgr) recoverRebalance() {

	m.mu.Lock()
	defer m.mu.Unlock()

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

/////////////////////////////////////////////////////////////////////////
//
//  REST handlers
//
/////////////////////////////////////////////////////////////////////////

func (m *ServiceMgr) handleListRebalanceTokens(w http.ResponseWriter, r *http.Request) {

	if !m.validateAuth(w, r) {
		l.Errorf("ServiceMgr::handleListRebalanceTokens Validation Failure for Request %v", r)
		return
	}

	if r.Method == "GET" {

		l.Infof("ServiceMgr::handleListRebalanceTokens Processing Request %v", r)
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

	if !m.validateAuth(w, r) {
		l.Errorf("ServiceMgr::handleCleanupRebalance Validation Failure for Request %v", r)
		return
	}

	if r.Method == "GET" || r.Method == "POST" {

		l.Infof("ServiceMgr::handleCleanupRebalance Processing Request %v", r)
		m.mu.Lock()
		defer m.mu.Unlock()

		rtokens, err := m.getCurrRebalTokens()
		if err != nil {
			l.Errorf("ServiceMgr::handleCleanupRebalance Error %v", err)
		}

		if rtokens != nil {
			if rtokens.RT != nil {
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
		return
	}

}

func (m *ServiceMgr) handleNodeuuid(w http.ResponseWriter, r *http.Request) {

	if !m.validateAuth(w, r) {
		l.Errorf("ServiceMgr::handleNodeuuid Validation Failure for Request %v", r)
		return
	}

	if r.Method == "GET" || r.Method == "POST" {
		l.Infof("ServiceMgr::handleNodeuuid Processing Request %v", r)
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

	if !m.validateAuth(w, r) {
		l.Errorf("ServiceMgr::handleRegisterRebalanceToken Validation Failure for Request %v", r)
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

			m.rebalancerF = NewRebalancer(nil, &rebalToken, string(m.nodeInfo.NodeID), false, nil,
				m.rebalanceDoneCallback, m.supvMsgch, m.localhttp)
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

func (m *ServiceMgr) getGlobalTopology() (*manager.ClusterIndexMetadata, error) {

	addr := m.localhttp
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
		l.Errorf("ServiceMgr::getGlobalTopology Error unmarshal global topology %v %v %s", addr+url, err, string(bytes))
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

		//skip if token was generated by self
		if rebalToken.MasterId == string(m.nodeInfo.NodeID) {
			l.Infof("ServiceMgr::processMoveIndex Skip MoveIndex Token for Self Node")
			return nil
		}

		m.mu.Lock()
		defer m.mu.Unlock()

		if m.rebalanceRunning {
			err := errors.New("Cannot Process Move Index - Rebalance In Progress")
			l.Errorf("ServiceMgr::processMoveIndex %v %v", err, m.rebalanceToken)
			m.runCleanupPhaseLOCKED(MoveIndexTokenPath, true)
			return nil
		} else {
			m.rebalanceRunning = true
			m.rebalanceToken = &rebalToken
			var err error
			if err = m.registerRebalanceRunning(); err != nil {
				m.runCleanupPhaseLOCKED(MoveIndexTokenPath, true)
				return nil
			}

			if err = m.registerLocalRebalanceToken(); err != nil {
				m.runCleanupPhaseLOCKED(MoveIndexTokenPath, true)
				return nil
			}
			m.rebalancerF = NewRebalancer(nil, &rebalToken, string(m.nodeInfo.NodeID), false, nil, m.moveIndexDoneCallback, m.supvMsgch, m.localhttp)
		}
	}

	return nil
}

func (m *ServiceMgr) handleMoveIndex(w http.ResponseWriter, r *http.Request) {

	if !m.validateAuth(w, r) {
		l.Errorf("ServiceMgr::handleMoveIndex Validation Failure for Request %v", r)
		return
	}

	if r.Method == "POST" {
		bytes, _ := ioutil.ReadAll(r.Body)
		var req manager.IndexRequest
		if err := json.Unmarshal(bytes, &req); err != nil {
			l.Errorf("ServiceMgr::handleMoveIndex %v", err)
			sendIndexResponseWithError(http.StatusBadRequest, w, err.Error())
			return
		}

		if len(req.IndexIds.DefnIds) == 0 {
			err := errors.New("Empty Index List for Move Index")
			l.Errorf("ServiceMgr::handleMoveIndex %v", err)
			sendIndexResponseWithError(http.StatusBadRequest, w, err.Error())
			return
		}

		if req.Plan == nil || len(req.Plan) == 0 {
			err := errors.New("Empty Plan For Move Index")
			l.Errorf("ServiceMgr::handleMoveIndex %v", err)
			sendIndexResponseWithError(http.StatusBadRequest, w, err.Error())
			return
		}

		if dest, ok := req.Plan["dest"]; ok {

			m.mu.Lock()
			defer m.mu.Unlock()

			var err error

			if m.checkRebalanceRunning() {
				err = errors.New("Cannot Process Move Index - Rebalance/MoveIndex In Progress")
				l.Errorf("ServiceMgr::handleMoveIndex %v %v", err, m.rebalanceToken)
				sendIndexResponseWithError(http.StatusInternalServerError, w, err.Error())
				return
			}

			if err = m.genMoveIndexToken(); err != nil {
				sendIndexResponseWithError(http.StatusInternalServerError, w, err.Error())
				return
			}

			l.Infof("ServiceMgr::handleMoveIndex New Move Index Token %v Dest %v", m.rebalanceToken, dest)

			transferTokens, err := m.generateTransferTokenForMoveIndex(&req)
			if err != nil {
				l.Errorf("ServiceMgr::handleMoveIndex %v %v", err, m.rebalanceToken)
				sendIndexResponseWithError(http.StatusInternalServerError, w, err.Error())
				return
			}

			if len(transferTokens) == 0 {
				l.Errorf("ServiceMgr::handleMoveIndex No TransferTokens Generated. Skip Move Index.")
				sendIndexResponse(w)
				return
			}

			if err = m.registerRebalanceRunning(); err != nil {
				m.runCleanupPhaseLOCKED(MoveIndexTokenPath, false)
				sendIndexResponseWithError(http.StatusInternalServerError, w, err.Error())
				return
			}

			if err = m.registerLocalRebalanceToken(); err != nil {
				m.runCleanupPhaseLOCKED(MoveIndexTokenPath, false)
				sendIndexResponseWithError(http.StatusInternalServerError, w, err.Error())
				return
			}

			if err = m.registerMoveIndexTokenInMetakv(); err != nil {
				m.runCleanupPhaseLOCKED(MoveIndexTokenPath, false)
				sendIndexResponseWithError(http.StatusInternalServerError, w, err.Error())
				return
			}

			rebalancer := NewRebalancer(transferTokens, m.rebalanceToken, string(m.nodeInfo.NodeID),
				true, nil, m.moveIndexDoneCallback, m.supvMsgch, m.localhttp)

			m.rebalancer = rebalancer
			m.rebalanceRunning = true
			sendIndexResponse(w)
			return

		} else {
			err := errors.New("No Destination For Move Index")
			l.Errorf("ServiceMgr::handleMoveIndex %v, %v", err, req.Plan)
			sendIndexResponseWithError(http.StatusBadRequest, w, err.Error())
			return
		}

	} else {
		sendIndexResponseWithError(http.StatusBadRequest, w, "Unsupported method")
		return
	}

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

func (m *ServiceMgr) registerMoveIndexTokenInMetakv() error {

	err := MetakvSet(MoveIndexTokenPath, m.rebalanceToken)
	if err != nil {
		l.Errorf("ServiceMgr::registerMoveIndexTokenInMetakv Unable to set RebalanceToken In "+
			"Meta Storage. Err %v", err)
		return err
	}
	l.Infof("ServiceMgr::registerMoveIndexTokenInMetakv Registered Global Rebalance Token In Metakv %v %v", MoveIndexTokenPath, m.rebalanceToken)

	return nil
}

func (m *ServiceMgr) generateTransferTokenForMoveIndex(req *manager.IndexRequest) (map[string]*c.TransferToken, error) {

	topology, err := m.getGlobalTopology()
	if err != nil {
		return nil, err
	}

	dest, _ := req.Plan["dest"]
	destUUID, err := m.getNodeIdFromDest(dest.(string))
	if err != nil {
		return nil, err
	} else if destUUID == "" {
		errStr := fmt.Sprintf("Unable to Fetch Node UUID for %v", dest)
		return nil, errors.New(errStr)
	}

	transferTokens := make(map[string]*c.TransferToken)

	for _, reqId := range req.IndexIds.DefnIds {

	outerloop:
		for _, localMeta := range topology.Metadata {

			for _, index := range localMeta.IndexDefinitions {

				if c.IndexDefnId(reqId) == index.DefnId {

					if localMeta.IndexerId == destUUID {
						l.Infof("ServiceMgr::generateTransferTokenForMoveIndex Skip Index %v. Source and Dest are same.", reqId)
						continue
					}

					topology := findTopologyByBucket(localMeta.IndexTopologies, index.Bucket)
					if topology == nil {
						err := errors.New(fmt.Sprintf("Fail to find index topology for bucket %v for node %v.", index.Bucket, localMeta.NodeUUID))
						l.Errorf("ServiceMgr::generateTransferTokenForMoveIndex %v", err)
						return nil, err
					}

					inst := topology.GetIndexInstByDefn(index.DefnId)
					if inst == nil {
						err := errors.New(fmt.Sprintf("Fail to find index instance for definition %v for node %v.", index.DefnId, localMeta.NodeUUID))
						l.Errorf("ServiceMgr::generateTransferTokenForMoveIndex %v", err)
						return nil, err
					}

					localInst := &c.IndexInst{
						InstId: c.IndexInstId(inst.InstId),
						Defn:   index,
						State:  c.IndexState(inst.State),
						Stream: c.StreamId(inst.StreamId),
						Error:  inst.Error,
					}

					ttid, tt := m.genTransferToken(index, localInst, localMeta.IndexerId, destUUID)

					if tt.SourceId == tt.DestId {
						l.Infof("ServiceMgr::generateTransferTokenForMoveIndex Skip No-op TransferToken %v", tt)
						continue
					}

					l.Infof("ServiceMgr::generateTransferTokenForMoveIndex Generated TransferToken %v %v", ttid, tt)
					transferTokens[ttid] = tt
					break outerloop
				}
			}
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

		maddr, err := m.cinfo.GetServiceAddress(nid, "mgmt")
		if err != nil {
			return "", err
		}

		if maddr == dest {

			haddr, err := m.cinfo.GetServiceAddress(nid, c.INDEX_HTTP_SERVICE)
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
	errStr := fmt.Sprintf("Unable to find Index service for destination %v", dest)
	l.Errorf("ServiceMgr::getNodeIdFromDest %v")

	return "", errors.New(errStr)
}

func (m *ServiceMgr) genTransferToken(indexDefn c.IndexDefn, indexInst *c.IndexInst, sourceId string, destId string) (string, *c.TransferToken) {

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

	tt.IndexInst.Defn.InstVersion++

	return ttid, tt

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
	} else {
		m.runCleanupPhaseLOCKED(MoveIndexTokenPath, false)
	}
	m.rebalancer = nil
	m.rebalancerF = nil

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

func (m *ServiceMgr) validateAuth(w http.ResponseWriter, r *http.Request) bool {
	cfg := m.config.Load()
	valid, err := c.IsAuthValid(r, cfg["clusterAddr"].String())
	if err != nil {
		m.writeError(w, err)
	} else if valid == false {
		w.WriteHeader(401)
		w.Write([]byte("401 Unauthorized\n"))
	}
	return valid
}

func (m *ServiceMgr) getLocalHttpAddr() string {

	cfg := m.config.Load()
	addr := cfg["clusterAddr"].String()
	host, _, _ := net.SplitHostPort(addr)
	port := cfg["httpPort"].String()
	return net.JoinHostPort(host, port)

}

func getWithAuth(url string) (*http.Response, error) {

	if !strings.HasPrefix(url, "http://") {
		url = "http://" + url
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	err = cbauth.SetRequestAuthVia(req, nil)
	if err != nil {
		l.Errorf("ServiceMgr::getWithAuth Error setting auth %v", err)
	}

	client := http.Client{Timeout: time.Duration(10 * time.Second)}
	return client.Do(req)
}

func postWithAuth(url string, bodyType string, body io.Reader) (*http.Response, error) {

	if !strings.HasPrefix(url, "http://") {
		url = "http://" + url
	}

	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", bodyType)
	err = cbauth.SetRequestAuthVia(req, nil)
	if err != nil {
		l.Errorf("ServiceMgr::postWithAuth Error setting auth %v", err)
		return nil, err
	}

	client := http.Client{Timeout: time.Duration(10 * time.Second)}
	return client.Do(req)
}

func sendIndexResponseWithError(status int, w http.ResponseWriter, msg string) {
	res := &manager.IndexResponse{Code: manager.RESP_ERROR, Error: msg}
	send(status, w, res)
}

func sendIndexResponse(w http.ResponseWriter) {
	result := &manager.IndexResponse{Code: manager.RESP_SUCCESS}
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
