// @copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package indexer

import (
	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/indexing/secondary/logging"
)

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
//   AutofailoverManager
//     GSI: AutofailoverServiceManager (autofailover_service_manager.go)
//   Manager - a single mash-up iface in cbauth broken down into multiple separate pieces in GSI
//     GSI: GenericServiceManager (generic_service_manager.go)
//       GSI: PauseServiceManager (pause_service_manager.go)
//       GSI: RebalanceServiceManager (rebalance_service_manager.go)
//   ServerlessManager
//   GSI: ServerlessManager (serverless_manager.go)
type MasterServiceManager struct {
	autofailMgr   *AutofailoverServiceManager
	genericMgr    *GenericServiceManager
	pauseMgr      *PauseServiceManager
	rebalMgr      *RebalanceServiceManager
	serverlessMgr *ServerlessManager
}

// NewMasterServiceManager is the constructor for the MasterServiceManager class. The service
// managers passed in are all singletons created by NewIndexer.
func NewMasterServiceManager(
	autofailMgr *AutofailoverServiceManager,
	genericMgr *GenericServiceManager,
	pauseMgr *PauseServiceManager,
	rebalMgr *RebalanceServiceManager,
	serverlessMgr *ServerlessManager,
) *MasterServiceManager {
	this := &MasterServiceManager{
		autofailMgr:   autofailMgr,
		genericMgr:    genericMgr,
		pauseMgr:      pauseMgr,
		rebalMgr:      rebalMgr,
		serverlessMgr: serverlessMgr,
	}
	go this.registerWithServer() // register for ns_server RPC calls from cbauth
	return this
}

// registerWithServer runs in a goroutine that registers this object as the singleton handler
// implementing the ns_server RPC interfaces Manager (historically generic name for Rebalance
// manager) and AutofailoverManager. Errors are logged but indexer will continue on regardless.
func (this *MasterServiceManager) registerWithServer() {
	const method = "MasterServiceManager::registerWithServer:" // for logging

	// Ensure this class implements the interfaces we intend. The type assertions will panic if not.
	var iface interface{} = this
	logging.Infof("%v %T implements service.AutofailoverManager; %T implements service.Manager; "+
		"%T implements service.ServerlessManager", method, iface.(service.AutofailoverManager),
		iface.(service.Manager), iface.(service.ServerlessManager))

	// Unless it returns an error, RegisterManager will actually run forever instead of returning
	err := service.RegisterManager(this, nil)
	if err != nil {
		logging.Errorf("%v Failed to register with Cluster Manager. err: %v", method, err)
		return
	}
	logging.Infof("%v Registered with Cluster Manager", method)
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// ns_server AutofailoverManager interface methods
////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *MasterServiceManager) HealthCheck() (*service.HealthInfo, error) {
	return this.autofailMgr.HealthCheck()
}

func (this *MasterServiceManager) IsSafe(nodeUUIDs []service.NodeID) error {
	return this.autofailMgr.IsSafe(nodeUUIDs)
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// ns_server Manager interface methods (generic)
////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *MasterServiceManager) GetNodeInfo() (*service.NodeInfo, error) {
	return this.genericMgr.GetNodeInfo()
}

func (this *MasterServiceManager) Shutdown() error {
	return this.genericMgr.Shutdown()
}

func (this *MasterServiceManager) GetTaskList(rev service.Revision, cancel service.Cancel) (
	*service.TaskList, error) {
	return this.genericMgr.GetTaskList(rev, cancel)
}

func (this *MasterServiceManager) CancelTask(id string, rev service.Revision) error {
	return this.genericMgr.CancelTask(id, rev)
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// ns_server (Pause)Manager interface methods -- Pause-Resume-specific APIs in Manager iface
// kjc pending final signatures TBD by ns_server
////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *MasterServiceManager) PreparePause(params service.PauseParams) error {
	return this.pauseMgr.PreparePause(params)
}

func (this *MasterServiceManager) Pause(params service.PauseParams) error {
	return this.pauseMgr.Pause(params)
}

func (this *MasterServiceManager) PrepareResume(params service.ResumeParams) error {
	return this.pauseMgr.PrepareResume(params)
}

func (this *MasterServiceManager) Resume(params service.ResumeParams) error {
	return this.pauseMgr.Resume(params)
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// ns_server (Rebalance)Manager interface methods -- Rebalance-specific APIs in Manager iface
////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *MasterServiceManager) GetCurrentTopology(rev service.Revision, cancel service.Cancel) (*service.Topology, error) {
	return this.rebalMgr.GetCurrentTopology(rev, cancel)
}

func (this *MasterServiceManager) PrepareTopologyChange(change service.TopologyChange) error {
	return this.rebalMgr.PrepareTopologyChange(change)
}

func (this *MasterServiceManager) StartTopologyChange(change service.TopologyChange) error {
	return this.rebalMgr.StartTopologyChange(change)
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// ns_server ServerlessManager interface methods
////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *MasterServiceManager) GetDefragmentedUtilization() (*service.DefragmentedUtilizationInfo, error) {
	return this.serverlessMgr.GetDefragmentedUtilization()
}
