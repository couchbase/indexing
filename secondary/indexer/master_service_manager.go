// @copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package indexer

import (
	"math"
	"sync/atomic"
	"time"

	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/indexing/secondary/logging"

	c "github.com/couchbase/indexing/secondary/common"
)

const DEFAULT_RPC_TIMEOUT_SECS = 60

////////////////////////////////////////////////////////////////////////////////////////////////////
// MasterServiceManager class
////////////////////////////////////////////////////////////////////////////////////////////////////

// MasterServiceManager is used to work around cbauth's monolithic service manager architecture that
// requires a singleton to implement all the different interfaces, as cbauth requires this to be
// registered only once. These are thus "implemented" here as delegates to the real GSI implementing
// classes. This is intentionally done via explicit delegation rather than just adding anonymous
// members of the delegate classes to the MasterServiceManager class because explicit code is far
// easier to understand, troubleshoot, and maintain than implicit (i.e. invisible) code.
//
// ns_server interfaces implemented (defined in cbauth/service/interface.go)
//
//	AutofailoverManager
//	  GSI: AutofailoverServiceManager (autofailover_service_manager.go)
//	Manager - a single mash-up iface in cbauth broken down into multiple separate pieces in GSI
//	  GSI: GenericServiceManager (generic_service_manager.go)
//	    GSI: PauseServiceManager (pause_service_manager.go)
//	    GSI: RebalanceServiceManager (rebalance_service_manager.go)
//	ServerlessManager
//	GSI: ServerlessManager (serverless_manager.go)
type MasterServiceManager struct {
	autofailMgrPtr   atomic.Pointer[AutofailoverServiceManager]
	genericMgrPtr    atomic.Pointer[GenericServiceManager]
	pauseMgrPtr      atomic.Pointer[PauseServiceManager]
	rebalMgrPtr      atomic.Pointer[RebalanceServiceManager]
	serverlessMgrPtr atomic.Pointer[ServerlessManager]
	bootstrapFinCh   <-chan struct{}
}

// DEPRECATED!! NewMasterServiceManager is the constructor for the MasterServiceManager class.
// The service managers passed in are all singletons created by NewIndexer.
func NewMasterServiceManager(
	autofailMgr *AutofailoverServiceManager,
	genericMgr *GenericServiceManager,
	pauseMgr *PauseServiceManager,
	rebalMgr *RebalanceServiceManager,
	serverlessMgr *ServerlessManager,
	bootstrapFinCh <-chan struct{},
) *MasterServiceManager {
	var msm = NewMasterServiceManager2(bootstrapFinCh)

	msm.SetAutoFailoverManager(autofailMgr)
	msm.SetGenericServiceManager(genericMgr)
	msm.SetPauseServiceManager(pauseMgr)
	msm.SetRebalanceManager(rebalMgr)
	msm.SetServerlessManager(serverlessMgr)

	go msm.registerWithServer()

	return msm
}

func NewMasterServiceManager2(bootstrapFinCh <-chan struct{}) *MasterServiceManager {
	var newmsm = &MasterServiceManager{bootstrapFinCh: bootstrapFinCh}
	newmsm.autofailMgrPtr.Store(nil)
	newmsm.genericMgrPtr.Store(nil)
	newmsm.pauseMgrPtr.Store(nil)
	newmsm.rebalMgrPtr.Store(nil)
	newmsm.serverlessMgrPtr.Store(nil)

	return newmsm
}

func (msm *MasterServiceManager) SetAutoFailoverManager(mgr *AutofailoverServiceManager) {
	msm.autofailMgrPtr.Store(mgr)
}

func (msm *MasterServiceManager) SetGenericServiceManager(mgr *GenericServiceManager) {
	msm.genericMgrPtr.Store(mgr)
}

func (msm *MasterServiceManager) SetPauseServiceManager(mgr *PauseServiceManager) {
	msm.pauseMgrPtr.Store(mgr)
}

func (msm *MasterServiceManager) SetRebalanceManager(mgr *RebalanceServiceManager) {
	msm.rebalMgrPtr.Store(mgr)
}

func (msm *MasterServiceManager) SetServerlessManager(mgr *ServerlessManager) {
	msm.serverlessMgrPtr.Store(mgr)
}

func (msm *MasterServiceManager) GetAutoFailoverManager() *AutofailoverServiceManager {
	return msm.autofailMgrPtr.Load()
}

func (msm *MasterServiceManager) GetGenericServiceManager() *GenericServiceManager {
	return msm.genericMgrPtr.Load()
}

func (msm *MasterServiceManager) GetPauseServiceManager() *PauseServiceManager {
	return msm.pauseMgrPtr.Load()
}

func (msm *MasterServiceManager) GetRebalanceManager() *RebalanceServiceManager {
	return msm.rebalMgrPtr.Load()
}

func (msm *MasterServiceManager) GetServerlessManager() *ServerlessManager {
	return msm.serverlessMgrPtr.Load()
}

// registerWithServer runs in a goroutine that registers this object as the singleton handler
// implementing the ns_server RPC interfaces Manager (historically generic name for Rebalance
// manager) and AutofailoverManager. Errors are logged but indexer will continue on regardless.
func (msm *MasterServiceManager) registerWithServer() {
	// Ensure this class implements the interfaces we intend. The type assertions will panic if not.
	var iface interface{} = msm
	logging.Infof(
		"MasterServiceManager::registerWithServer: %T implements service.AutofailoverManager; %T implements service.Manager; %T implements service.ServerlessManager",
		iface.(service.AutofailoverManager),
		iface.(service.Manager),
		iface.(service.ServerlessManager),
	)

	// Unless it returns an error, RegisterManager will actually run forever instead of returning
	err := service.RegisterManager(msm, nil)
	if err != nil {
		logging.Errorf(
			"MasterServiceManager::registerWithServer: Failed to register with Cluster Manager. err: %v",
			err)
		return
	}
	logging.Infof("MasterServiceManager::registerWithServer: Registered with Cluster Manager")
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// ns_server AutofailoverManager interface methods
////////////////////////////////////////////////////////////////////////////////////////////////////

func (msm *MasterServiceManager) HealthCheck() (*service.HealthInfo, error) {
	var autofailMgr = msm.GetAutoFailoverManager()
	if autofailMgr == nil {
		logging.Warnf("MasterServiceManager::HealthCheck: failover manager not yet initialised")
		return nil, c.ErrIndexerInBootstrap
	}
	return autofailMgr.HealthCheck()
}

func (msm *MasterServiceManager) IsSafe(nodeUUIDs []service.NodeID) error {
	// bootstrap initialises important pieces to the IsSafe call like ClusterInfoCache,
	// requestHandler for /getIndexStatus call, etc. hence we should wait for bootstrap to finish
	// before answering the `IsSafe` RPC call else it's response will not be reliable
	if !msm.waitForBootstrapWithTimeout(DEFAULT_RPC_TIMEOUT_SECS, "IsSafe()") {
		logging.Warnf("MasterServiceManager::IsSafe: indexer bootstrap not complete. Cannot determine metadata integrity")
		return c.ErrIndexerInBootstrap
	}
	var autofailMgr = msm.GetAutoFailoverManager()
	if autofailMgr == nil {
		logging.Warnf("MasterServiceManager::IsSafe: failover mgr not yet initialised")
		return c.ErrIndexerInBootstrap
	}
	return autofailMgr.IsSafe(nodeUUIDs)
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// ns_server Manager interface methods (generic)
////////////////////////////////////////////////////////////////////////////////////////////////////

func (msm *MasterServiceManager) GetNodeInfo() (*service.NodeInfo, error) {
	var genericMgr = msm.GetGenericServiceManager()
	if genericMgr == nil {
		logging.Warnf("MasterServiceManager::GetNodeInfo: generic service mgr not yet initialised")
		return nil, c.ErrIndexerInBootstrap
	}
	return genericMgr.GetNodeInfo()
}

func (msm *MasterServiceManager) Shutdown() error {
	var genericMgr = msm.GetGenericServiceManager()
	if genericMgr == nil {
		logging.Warnf("MasterServiceManager::Shutdown: generic service mgr not yet initialised")
		return c.ErrIndexerInBootstrap
	}
	return genericMgr.Shutdown()
}

func (msm *MasterServiceManager) GetTaskList(rev service.Revision, cancel service.Cancel) (
	*service.TaskList, error) {
	// GetTaskList is polled by ns_server regularly. More so, it is also used to track
	// rebalance progress. Since rebalance is a user triggered activity, we should rather wait
	// for bootstrap to complete so that the rebalance does not fail if indexer was going to
	// come out of bootstap in `RPC` timeout duration
	msm.waitForBootstrapWithTimeout(DEFAULT_RPC_TIMEOUT_SECS, "GetTaskList()")
	var genericMgr = msm.GetGenericServiceManager()
	if genericMgr == nil {
		logging.Warnf("MasterServiceManager::GetTaskList: generic service mgr not yet initialised")
		return nil, c.ErrIndexerInBootstrap
	}
	return genericMgr.GetTaskList(rev, cancel)
}

func (msm *MasterServiceManager) CancelTask(id string, rev service.Revision) error {
	// we do not need to wait for bootstrap here as if the service manager is not available then
	// the rebalance is not running anyways. if indexer is in bootstrap then it means we will
	// run cleanup for old rebalance once indexer is out of warmup

	var genericMgr = msm.GetGenericServiceManager()
	if genericMgr == nil {
		logging.Warnf("MasterServiceManager::CancelTask: generic service mgr not yet initialised")
		return c.ErrIndexerInBootstrap
	}
	return genericMgr.CancelTask(id, rev)
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// ns_server (Pause)Manager interface methods -- Pause-Resume-specific APIs in Manager iface
////////////////////////////////////////////////////////////////////////////////////////////////////

func (msm *MasterServiceManager) PreparePause(params service.PauseParams) error {
	var pauseMgr = msm.GetPauseServiceManager()
	if pauseMgr == nil {
		logging.Warnf("MasterServiceManager::PreparePause: pause service mgr not yet initialised")
		return c.ErrIndexerInBootstrap
	}
	return pauseMgr.PreparePause(params)
}

func (msm *MasterServiceManager) Pause(params service.PauseParams) error {
	var pauseMgr = msm.GetPauseServiceManager()
	if pauseMgr == nil {
		logging.Warnf("MasterServiceManager::Pause: pause service mgr not yet initialised")
		return c.ErrIndexerInBootstrap
	}
	return pauseMgr.Pause(params)
}

func (msm *MasterServiceManager) PrepareResume(params service.ResumeParams) error {
	var pauseMgr = msm.GetPauseServiceManager()
	if pauseMgr == nil {
		logging.Warnf("MasterServiceManager::PrepareResume: pause service mgr not yet initialised")
		return c.ErrIndexerInBootstrap
	}
	return pauseMgr.PrepareResume(params)
}

func (msm *MasterServiceManager) Resume(params service.ResumeParams) error {
	var pauseMgr = msm.GetPauseServiceManager()
	if pauseMgr == nil {
		logging.Warnf("MasterServiceManager::Resume: pause service mgr not yet initialised")
		return c.ErrIndexerInBootstrap
	}
	return pauseMgr.Resume(params)
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// ns_server (Rebalance)Manager interface methods -- Rebalance-specific APIs in Manager iface
////////////////////////////////////////////////////////////////////////////////////////////////////

func (msm *MasterServiceManager) GetCurrentTopology(rev service.Revision, cancel service.Cancel) (
	*service.Topology, error) {
	// GetCurrentTopology is polled by ns_server regularly. More so, it is also called along with
	// GetTaskList during rebalance. Since rebalance is a user triggered activity, we should rather
	// wait for bootstrap to complete so that the rebalance does not fail if indexer was going to
	// come out of bootstap in `RPC` timeout duration
	msm.waitForBootstrapWithTimeout(DEFAULT_RPC_TIMEOUT_SECS, "GetCurrentTopology()")
	var rebalMgr = msm.GetRebalanceManager()
	if rebalMgr == nil {
		logging.Warnf("MasterServiceManager::GetCurrentTopology: rebalance service mgr not yet initialised")
		return nil, c.ErrIndexerInBootstrap
	}
	return rebalMgr.GetCurrentTopology(rev, cancel)
}

func (msm *MasterServiceManager) PrepareTopologyChange(change service.TopologyChange) error {
	// Since rebalance is a user triggered activity, we want to wait for indexer to come out of
	// warmup and respond so that the rebalance request don't fail. Rebalance requests on
	// new nodes/failover recovered nodes could potentially fail here if we dont' wait for indexer
	// to come out of warmup and then respond back
	msm.waitForBootstrapWithTimeout(DEFAULT_RPC_TIMEOUT_SECS, "PrepareTopologyChange()")
	var rebalMgr = msm.GetRebalanceManager()
	if rebalMgr == nil {
		logging.Warnf("MasterServiceManager::PrepareTopologyChange: rebalance service mgr not yet initialised")
		return c.ErrIndexerInBootstrap
	}
	return rebalMgr.PrepareTopologyChange(change)
}

func (msm *MasterServiceManager) StartTopologyChange(change service.TopologyChange) error {
	// StartTopologyChange need not necessarily wait for indexer to come out of recovery as it is
	// always followed by a successful PrepareTopologyChange. Hence this is just a safety net and
	// in most cases it will be a no-op
	msm.waitForBootstrapWithTimeout(DEFAULT_RPC_TIMEOUT_SECS, "StartTopologyChange()")
	var rebalMgr = msm.GetRebalanceManager()
	if rebalMgr == nil {
		logging.Warnf("MasterServiceManager::StartTopologyChange: rebalance service mgr not yet initialised")
		return c.ErrIndexerInBootstrap
	}
	return rebalMgr.StartTopologyChange(change)
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// ns_server ServerlessManager interface methods
////////////////////////////////////////////////////////////////////////////////////////////////////

func (msm *MasterServiceManager) GetDefragmentedUtilization() (
	*service.DefragmentedUtilizationInfo, error) {

	var serverlessMgr = msm.GetServerlessManager()
	if serverlessMgr == nil {
		logging.Warnf("MasterServiceManager::GetDefragmentedUtilization: serverless mgr not yet initialised")
		return nil, c.ErrIndexerInBootstrap
	}
	return serverlessMgr.GetDefragmentedUtilization()
}

func (msm *MasterServiceManager) waitForBootstrapWithTimeout(timeoutInSeconds int,
	rpcFuncName string) bool {

	if msm.bootstrapFinCh == nil {
		return false
	}

	// short circuit - if the channel is already closed then return without logging and creating
	// any new timers
	select {
	case <-msm.bootstrapFinCh:
		return true
	default:
	}

	if timeoutInSeconds <= 0 {
		timeoutInSeconds = math.MaxInt
	}

	var startTimestamp = time.Now()
	var timer = time.NewTimer(time.Duration(timeoutInSeconds) * time.Second)
	defer timer.Stop()

	select {
	case <-msm.bootstrapFinCh:
		var totalWaitTime time.Duration = time.Since(startTimestamp)
		logging.Infof("MasterServiceManager::waitForBootstrapWithTimeout bootstrap finish. total wait time - %v for RPC call `%v`",
			totalWaitTime.String(), rpcFuncName)
		return true
	case <-timer.C:
		logging.Warnf("MasterServiceManager::waitForBootstrapWithTimeout timed out while waiting for bootstrap. RPC call possibly failed `%v`",
			rpcFuncName)
		return false
	}
}
