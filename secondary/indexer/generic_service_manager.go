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
)

////////////////////////////////////////////////////////////////////////////////////////////////////
// GenericServiceManager class
////////////////////////////////////////////////////////////////////////////////////////////////////

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
}

// NewGenericServiceManager is the constructor for the GenericServiceManager class.
func NewGenericServiceManager(nodeInfo *service.NodeInfo,
	pauseMgr *PauseServiceManager, rebalMgr *RebalanceServiceManager) *GenericServiceManager {

	m := &GenericServiceManager{
		nodeInfo: nodeInfo,
		pauseMgr: pauseMgr,
		rebalMgr: rebalMgr,
	}

	return m
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
// specific managers that might be running tasks. The cancel arg is a channel ns_server may use to
// cancel the call.
func (m *GenericServiceManager) GetTaskList(rev service.Revision,
	cancel service.Cancel) (*service.TaskList, error) {

	return m.rebalMgr.RebalGetTaskList(rev, cancel)
	// kjc Pause-Resume feature: need to add delegation to pauseMgr
}

// CancelTask cancels an existing task by delegating to area-specific managers that might be running
// it.
func (m *GenericServiceManager) CancelTask(id string, rev service.Revision) error {
	return m.rebalMgr.RebalCancelTask(id, rev)
	// kjc Pause-Resume feature: need to add delegation to pauseMgr
}
