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
	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/manager"
	"github.com/couchbase/indexing/secondary/manager/client"
	mc "github.com/couchbase/indexing/secondary/manager/common"

	//"github.com/couchbase/indexing/secondary/planner"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

//////////////////////////////////////////////////////////////
// Concrete Type/Struct
//////////////////////////////////////////////////////////////

//
// DDLServiceMgr Definition
//
type DDLServiceMgr struct {
	indexerId   common.IndexerId
	config      common.ConfigHolder
	provider    *client.MetadataProvider
	supvCmdch   MsgChannel //supervisor sends commands on this channel
	supvMsgch   MsgChannel //channel to send any message to supervisor
	nodeID      service.NodeID
	localAddr   string
	clusterAddr string
	settings    *ddlSettings
	nodes       map[service.NodeID]bool
	donech      chan bool
	killch      chan bool
	allowDDL    bool
	mutex       sync.Mutex

	commandListener *mc.CommandListener
	listenerDonech  chan bool
	btCleanerStopCh chan bool
	buildCleanupLck sync.Mutex
	dtCleanerStopCh chan bool
	dropCleanupLck  sync.Mutex
}

//
// DDL related settings
//
type ddlSettings struct {
	numReplica   int32
	numPartition int32

	storageMode string
	mutex       sync.RWMutex
}

//////////////////////////////////////////////////////////////
// Global Variables
//////////////////////////////////////////////////////////////

var gDDLServiceMgr *DDLServiceMgr
var gDDLServiceMgrLck sync.Mutex

//////////////////////////////////////////////////////////////
// DDLServiceMgr
//////////////////////////////////////////////////////////////

//
// Constructor
//
func NewDDLServiceMgr(indexerId common.IndexerId, supvCmdch MsgChannel, supvMsgch MsgChannel, config common.Config) (*DDLServiceMgr, Message) {

	addr := config["clusterAddr"].String()
	port := config["httpPort"].String()
	host, _, _ := net.SplitHostPort(addr)
	localaddr := net.JoinHostPort(host, port)

	nodeId := service.NodeID(config["nodeuuid"].String())

	numReplica := int32(config["settings.num_replica"].Int())
	settings := &ddlSettings{numReplica: numReplica}

	mgr := &DDLServiceMgr{
		indexerId:       indexerId,
		supvCmdch:       supvCmdch,
		supvMsgch:       supvMsgch,
		localAddr:       localaddr,
		clusterAddr:     addr,
		nodeID:          nodeId,
		settings:        settings,
		donech:          nil,
		killch:          make(chan bool),
		allowDDL:        true,
		btCleanerStopCh: make(chan bool),
		dtCleanerStopCh: make(chan bool),
	}

	mgr.startCommandListner()

	mgr.config.Store(config)

	mux := GetHTTPMux()
	mux.HandleFunc("/listMetadataTokens", mgr.handleListMetadataTokens)
	mux.HandleFunc("/listCreateTokens", mgr.handleListCreateTokens)
	mux.HandleFunc("/listDeleteTokens", mgr.handleListDeleteTokens)
	mux.HandleFunc("/listDropInstanceTokens", mgr.handleListDropInstanceTokens)

	go mgr.run()

	gDDLServiceMgrLck.Lock()
	defer gDDLServiceMgrLck.Unlock()
	gDDLServiceMgr = mgr

	go mgr.buildTokenCleaner()
	go mgr.dropTokenCleaner()

	logging.Infof("DDLServiceMgr: intialized. Local nodeUUID %v", mgr.nodeID)

	return mgr, &MsgSuccess{}
}

//
// Get DDLServiceMgr singleton
//
func getDDLServiceMgr() *DDLServiceMgr {

	gDDLServiceMgrLck.Lock()
	defer gDDLServiceMgrLck.Unlock()

	return gDDLServiceMgr
}

//////////////////////////////////////////////////////////////
// Admin Service Processing
//////////////////////////////////////////////////////////////

func (m *DDLServiceMgr) run() {

	go m.processCreateCommand()

loop:
	for {
		select {

		case cmd, ok := <-m.supvCmdch:
			if ok {
				if cmd.GetMsgType() == ADMIN_MGR_SHUTDOWN {
					logging.Infof("DDL Rebalance Manager: Shutting Down")
					close(m.killch)
					m.commandListener.Close()
					close(m.btCleanerStopCh)
					close(m.dtCleanerStopCh)
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

func (m *DDLServiceMgr) handleSupervisorCommands(cmd Message) {
	switch cmd.GetMsgType() {

	case CONFIG_SETTINGS_UPDATE:
		cfgUpdate := cmd.(*MsgConfigUpdate)
		m.config.Store(cfgUpdate.GetConfig())
		m.settings.handleSettings(cfgUpdate.GetConfig())
		m.supvCmdch <- &MsgSuccess{}

	default:
		logging.Fatalf("DDLServiceMgr::handleSupervisorCommands Unknown Message %+v", cmd)
		common.CrashOnError(errors.New("Unknown Msg On Supv Channel"))
	}
}

//////////////////////////////////////////////////////////////
// Rebalance
//////////////////////////////////////////////////////////////

func stopDDLProcessing() {

	mgr := getDDLServiceMgr()
	if mgr != nil {
		mgr.stopProcessDDL()
	}
}

func resumeDDLProcessing() {

	mgr := getDDLServiceMgr()
	if mgr != nil {
		mgr.startProcessDDL()
	}
}

//
// This is run as a go-routine.  Rebalancing could have finished while
// this gorountine is still running.
//
func notifyRebalanceDone(change *service.TopologyChange, isCancel bool) {

	mgr := getDDLServiceMgr()
	if mgr != nil {
		mgr.rebalanceDone(change, isCancel)
	}
}

//
// Recover DDL command
//
func (m *DDLServiceMgr) rebalanceDone(change *service.TopologyChange, isCancel bool) {

	logging.Infof("DDLServiceMgr: handling rebalacne done")

	gDDLServiceMgrLck.Lock()
	defer gDDLServiceMgrLck.Unlock()

	defer func() {
		m.startProcessDDL()
		if m.provider != nil {
			m.provider.Close()
			m.provider = nil
		}
	}()

	// Refresh metadata provider on topology change
	httpAddrMap, err := m.refreshOnTopologyChange(change, isCancel)
	if err != nil {
		logging.Warnf("DDLServiceMgr: Failed to clean delete index token upon rebalancing.  Skip Cleanup. Internal Error = %v", err)
		return
	}

	m.cleanupCreateCommand()
	m.cleanupDropCommand(false, m.provider)
	m.cleanupDropInstanceCommand()
	m.cleanupBuildCommand(false, m.provider)
	m.handleClusterStorageMode(httpAddrMap)
}

func (m *DDLServiceMgr) stopProcessDDL() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.allowDDL = false
}

func (m *DDLServiceMgr) canProcessDDL() bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.allowDDL
}

func (m *DDLServiceMgr) startProcessDDL() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.allowDDL = true
}

//////////////////////////////////////////////////////////////
// Drop Token
//////////////////////////////////////////////////////////////

//
// Recover drop index command
//
func (m *DDLServiceMgr) cleanupDropCommand(checkDDL bool, provider *client.MetadataProvider) {

	m.dropCleanupLck.Lock()
	defer m.dropCleanupLck.Unlock()

	entries, err := metakv.ListAllChildren(mc.DeleteDDLCommandTokenPath)
	if err != nil {
		logging.Warnf("DDLServiceMgr: Failed to cleanup delete index token upon rebalancing.  Skip cleanup.  Internal Error = %v", err)
		return
	}

	if len(entries) == 0 {
		return
	}

	if provider == nil {
		// Use latest metadata provider.
		provider, _, err = m.newMetadataProvider(nil)
		if err != nil {
			logging.Errorf("DDLServiceMgr: cleanupDropCommand error in newMetadataProvider %v. Skip cleanup.", err)
			return
		}
		defer provider.Close()
	}

	if provider == nil {
		logging.Errorf("DDLServiceMgr: cleanupDropCommand nil MetadataProvider. Skip cleanup.")
		return
	}

	for _, entry := range entries {

		if strings.Contains(entry.Path, mc.DeleteDDLCommandTokenPath) && entry.Value != nil {

			logging.Infof("DDLServiceMgr: processing delete index token %v", entry.Path)

			command, err := mc.UnmarshallDeleteCommandToken(entry.Value)
			if err != nil {
				logging.Warnf("DDLServiceMgr: Failed to clean delete index token upon rebalancing.  Skip command %v.  Internal Error = %v.", entry.Path, err)
				continue
			}

			// If there is a create token, then do not process the drop token.
			// Let the create token be deleted first to avoid any unexpected
			// race condition.  This is more for safety than necessity.
			exist, err := mc.CreateCommandTokenExist(command.DefnId)
			if err != nil {
				logging.Warnf("DDLServiceMgr: Failed to check create token.  Skip command %v.  Error = %v.", entry.Path, err)
				continue
			}
			if exist {
				logging.Warnf("DDLServiceMgr: Create token exist for %v.  Skip processing drop token %v.", command.DefnId, entry.Path)
				continue
			}

			// Find if the index still exist in the cluster.  DDLServiceManger will only cleanup the delete token IF there is no index definition.
			// This means the indexer must have been able to process the deleted token before DDLServiceManager has a chance to clean it up.
			//
			// 1) It will skip DELETED index.  DELETED index will be cleaned up by lifecycle manager periodically.
			// 2) At this point, the metadata provider has been connected to all indexer at least once (refreshOnTopology gurantees that).   So
			//    metadata provider has a snapshot of the metadata from each indexer at some point in time.   It will return index even if metadata
			//    provider is not connected to the indexer at the exact moment when this call is made.
			//
			//
			if provider.FindIndexIgnoreStatus(command.DefnId) == nil {
				// There is no index in the cluster,  remove token

				if checkDDL && !m.canProcessDDL() {
					return
				}

				if err := common.MetakvDel(entry.Path); err != nil {
					logging.Warnf("DDLServiceMgr: Failed to remove delete index token %v. Error = %v", entry.Path, err)
				} else {
					logging.Infof("DDLServiceMgr: Remove delete index token %v.", entry.Path)
				}
			} else {
				logging.Infof("DDLServiceMgr: Indexer still holding index definiton.  Skip removing delete index token %v.", entry.Path)
			}
		}
	}
}

func (m *DDLServiceMgr) dropTokenCleaner() {

	ticker := time.NewTicker(10 * time.Minute)
	logging.Infof("DDLServiceMgr: starting dropTokenCleaner ...")
	for {
		select {

		case <-ticker.C:
			if m.canProcessDDL() {
				m.cleanupDropCommand(true, nil)
			}

		case <-m.dtCleanerStopCh:
			logging.Infof("DDLServiceMgr: stopping dropTokenCleaner ...")
			return
		}
	}
}

//////////////////////////////////////////////////////////////
// Build Token
//////////////////////////////////////////////////////////////

//
// Recover build index command
//
func (m *DDLServiceMgr) cleanupBuildCommand(checkDDL bool, provider *client.MetadataProvider) {

	m.buildCleanupLck.Lock()
	defer m.buildCleanupLck.Unlock()

	entries, err := metakv.ListAllChildren(mc.BuildDDLCommandTokenPath)
	if err != nil {
		logging.Warnf("DDLServiceMgr: Failed to cleanup build index token upon rebalancing.  Skip cleanup.  Internal Error = %v", err)
		return
	}

	if len(entries) == 0 {
		return
	}

	if provider == nil {
		// Use latest metadata provider.
		provider, _, err = m.newMetadataProvider(nil)
		if err != nil {
			logging.Errorf("DDLServiceMgr: cleanupBuildCommand error in newMetadataProvider %v. Skip cleanup.", err)
			return
		}
		defer provider.Close()
	}

	if provider == nil {
		logging.Errorf("DDLServiceMgr: cleanupBuildCommand nil MetadataProvider. Skip cleanup.")
		return
	}

	for _, entry := range entries {

		if strings.Contains(entry.Path, mc.BuildDDLCommandTokenPath) && entry.Value != nil {

			logging.Infof("DDLServiceMgr: processing build index token %v", entry.Path)

			command, err := mc.UnmarshallBuildCommandToken(entry.Value)
			if err != nil {
				logging.Warnf("DDLServiceMgr: Failed to clean build index token upon rebalancing.  Skip command %v.  Internal Error = %v.", entry.Path, err)
				continue
			}

			//
			// At this point, the metadata provider has been connected to all indexer at least once (refreshOnTopology gurantees that).   So
			// metadata provider has a snapshot of the metadata from each indexer at some point in time.   It will return index even if metadata
			// provider is not connected to the indexer at the exact moment when this call is made.
			//
			cleanup := true
			if index := provider.FindIndexIgnoreStatus(command.DefnId); index != nil {
				for _, inst := range index.Instances {
					if inst.State == common.INDEX_STATE_READY || inst.State == common.INDEX_STATE_CREATED {
						// no need to clean up if there is still instance to be built
						logging.Warnf("DDLServiceMgr: There are still index not yet build.  Skip cleaning up build token %v.", entry.Path)
						cleanup = false
						break
					}
				}

				for _, inst := range index.InstsInRebalance {
					if inst.State == common.INDEX_STATE_READY || inst.State == common.INDEX_STATE_CREATED {
						// no need to clean up if there is still instance to be built
						logging.Warnf("DDLServiceMgr: There are still index not yet build.  Skip cleaning up build token %v.", entry.Path)
						cleanup = false
						break
					}
				}
			}

			// Just for extra safety.
			if checkDDL && !m.canProcessDDL() {
				return
			}

			// Remove token
			if cleanup {
				if err := common.MetakvDel(entry.Path); err != nil {
					logging.Warnf("DDLServiceMgr: Failed to remove build index token %v. Error = %v", entry.Path, err)
				} else {
					logging.Infof("DDLServiceMgr: Remove build index token %v.", entry.Path)
				}
			}
		}
	}
}

func (m *DDLServiceMgr) buildTokenCleaner() {

	ticker := time.NewTicker(10 * time.Minute)
	logging.Infof("DDLServiceMgr: starting buildTokenCleaner ...")
	for {
		select {

		case <-ticker.C:
			if m.canProcessDDL() {
				m.cleanupBuildCommand(true, nil)
			}

		case <-m.btCleanerStopCh:
			logging.Infof("DDLServiceMgr: stopping buildTokenCleaner ...")
			return
		}
	}
}

//////////////////////////////////////////////////////////////
// Create Token
//////////////////////////////////////////////////////////////

func (m *DDLServiceMgr) cleanupCreateCommand() {

	// Get all virtual paths of create tokens from metakv. Since they are
	// big value tokens there are no values stored directly in these paths,
	// so entries[i].Path is all that is used here; entries[i].Value will be nil.
	entries, err := metakv.ListAllChildren(mc.CreateDDLCommandTokenPath)
	if err != nil {
		logging.Warnf("DDLServiceMgr: Failed to fetch token from metakv.  Internal Error = %v", err)
		return
	}

	deleted := make(map[common.IndexDefnId]map[uint64]bool)
	malformed := make(map[common.IndexDefnId]map[uint64]bool)

	isDeleted := func(defnId common.IndexDefnId, requestId uint64) bool {
		if _, ok := deleted[defnId]; ok {
			return deleted[defnId][requestId]
		}
		return false
	}

	addDeleted := func(defnId common.IndexDefnId, requestId uint64) {
		if _, ok := deleted[defnId]; !ok {
			deleted[defnId] = make(map[uint64]bool)
		}
		deleted[defnId][requestId] = true
	}

	addMalformed := func(defnId common.IndexDefnId, requestId uint64) {
		if _, ok := malformed[defnId]; !ok {
			malformed[defnId] = make(map[uint64]bool)
		}
		malformed[defnId][requestId] = true
	}

	for _, entry := range entries {

		delete := false

		defnId, requestId, err := mc.GetDefnIdFromCreateCommandTokenPath(entry.Path)
		if err != nil {
			logging.Warnf("DDLServiceMgr: Failed to process create index token.  Skip %v.  Internal Error = %v.", entry.Path, err)
			continue
		}

		if isDeleted(defnId, requestId) {
			continue
		}

		// Retrieve the current (big value) create token
		token, err := mc.FetchCreateCommandToken(defnId, requestId)
		if err != nil {
			logging.Warnf("DDLServiceMgr: Failed to process create index token.  Skip %v.  Internal Error = %v.", entry.Path, err)
			continue
		}

		if token != nil {
			// If there is a drop token, then do not process the create token.
			exist, err := mc.DeleteCommandTokenExist(token.DefnId)
			if err != nil {
				logging.Warnf("DDLServiceMgr: Failed to check delete token.  Skip processing %v.  Error = %v.", entry.Path, err)
			}

			if exist {
				delete = true
			} else {
				foundAnyIndexer := false
				for indexerId, _ := range token.Definitions {
					_, _, _, err := m.provider.FindServiceForIndexer(indexerId)
					if err == nil {
						foundAnyIndexer = true
						break
					}
				}

				// For a given token, if we cannot any matching indexer in the cluster after rebalancing, delete the token.
				if !foundAnyIndexer {
					delete = true
				}
			}

		} else {
			// There is an entry in metakv, but cannot fetch the token.  The token can be malformed or there is a create DDL
			// in progress.
			addMalformed(defnId, requestId)
		}

		if delete && !isDeleted(defnId, requestId) {
			// If a drop token exist, then delete the create token.
			if err := mc.DeleteCreateCommandToken(defnId, requestId); err != nil {
				logging.Warnf("DDLServiceMgr: Failed to remove create index token %v. Error = %v", entry.Path, err)
			} else {
				logging.Infof("DDLServiceMgr: Remove create index token %v.", entry.Path)
			}
			addDeleted(defnId, requestId)
		}
	}

	if len(malformed) != 0 {
		// wait for a second to ride out any race condition
		time.Sleep(time.Second)

		// make sure if all watchers are still alive
		if m.provider.AllWatchersAlive() {

			// Go through the list of tokens that have failed before.
			for defnId, requestIds := range malformed {
				for requestId, _ := range requestIds {

					// If already deleted, then ingore it.
					if isDeleted(defnId, requestId) {
						continue
					}

					// Try to fetch it again.  See if this time being successful.
					token, err := mc.FetchCreateCommandToken(defnId, requestId)
					if err != nil {
						continue
					}

					// Still cannot fetch the token.   Then it means the token could be deleted or
					// still being malformed.  Delete the token.
					if token == nil {
						// If a drop token exist, then delete the create token.
						if err := mc.DeleteCreateCommandToken(defnId, requestId); err != nil {
							logging.Warnf("DDLServiceMgr: Failed to remove create index token %v. Error = %v", defnId, err)
						} else {
							logging.Infof("DDLServiceMgr: Remove create index token %v.", defnId)
						}
						addDeleted(defnId, requestId)
					}
				}
			}
		}
	}
}

func (m *DDLServiceMgr) handleCreateCommand(needRefresh bool) {

	if !needRefresh && !m.commandListener.HasNewCreateTokens() {
		return
	}

	if needRefresh && !m.commandListener.HasCreateTokens() {
		return
	}

	if !m.canProcessDDL() {
		logging.Debugf("DDLServiceMgr: cannot process create token during rebalancing")
		return
	}

	// Start metadata provider.   Metadata provider will not start unless it can be connected to
	// all the indexer nodes.   The metadata provider will skip any inactive_failed and inactive_new node.
	// It is important that the DDLServiceMgr does not act on behalf on the failed node (e.g. repair
	// any pending create partition on the failed node), since the failed node may delta-recovery.
	//
	// This method may delete create token, but it can only happen when all partitions are accounted for.
	//
	// If there is a network partitioning before or during fetching metadata, metadata provider may not be
	// able to start.  But once the metadata provider is able to fetch metadata for all the nodes, the
	// metadata will be cached locally even if there is network partitioning afterwards.
	//
	provider, _, err := m.newMetadataProvider(nil)
	if err != nil {
		logging.Debugf("DDLServiceMgr: Failed to start metadata provider.  Internal Error = %v", err)
		return
	}
	defer provider.Close()

	findPartition := func(instId common.IndexInstId, partitionId common.PartitionId, instances []*client.InstanceDefn) (bool,
		common.IndexState, common.IndexerId) {

		for _, inst := range instances {
			if inst.InstId == instId {
				for partition2, indexerId := range inst.IndexerId {
					if partitionId == partition2 {
						return true, inst.State, indexerId
					}
				}
			}
		}
		return false, common.INDEX_STATE_NIL, common.INDEXER_ID_NIL
	}

	entries := m.commandListener.GetNewCreateTokens()

	// nothing to do
	if len(entries) == 0 {
		return
	}

	retryList := make(map[string]*mc.CreateCommandToken)
	for path, token := range entries {
		retryList[path] = token
	}

	defer func() {
		// Add back those tokens cannot be completely processed
		for path, token := range retryList {
			m.commandListener.AddNewCreateToken(path, token)
		}
	}()

	buildMap := make(map[common.IndexDefnId]bool)
	deleteMap := make(map[string]*mc.CreateCommandToken)

	for entry, token := range entries {

		logging.Infof("DDLServiceMgr: processing create index token %v", entry)

		if token != nil {

			// If there is a drop token, then do not process the create token.
			exist, err := mc.DeleteCommandTokenExist(token.DefnId)
			if err != nil {
				logging.Warnf("DDLServiceMgr: Failed to check delete token.  Skip processing %v.  Error = %v.", entry, err)

			} else if exist {
				// Avoid deleting create token during rebalance.  This is just for extra safety.  Even if the planner
				// may use the create token during planning, it just mean that it is trying to create those partitions on
				// behalf of the DDL service manager.   As long as the drop token exists, then those partitions will be
				// dropped eventually.   If the planner do not see this create token, then no harm is done.
				if !m.canProcessDDL() {
					logging.Infof("DDLServiceMgr: cannot delete create token during rebalancing")
					return
				}

				// If a drop token exist, then delete the create token.
				if err := mc.DeleteCreateCommandToken(token.DefnId, token.RequestId); err != nil {
					logging.Warnf("DDLServiceMgr: Failed to remove create index token %v. Error = %v", entry, err)
				} else {
					delete(retryList, entry)
					logging.Infof("DDLServiceMgr: Remove create index token %v due to delete token.", entry)
				}
				continue
			}

			// Go through each IndexDefn in the token.  Each IndexDefn contains a unique tuple of <defn, inst, partitions>.
			// So the same defnId and instId can appear in each IndexDefn in the token.   But the token will only
			// have 1 defnId -- two different index cannot share the same token.
			canDelete := true

			for indexerId, definitions := range token.Definitions {
				for _, defn := range definitions {
					var newPartitionList []common.PartitionId
					var newVersionList []int

					// If there is a drop token, then do not process the create token.
					exist, err := mc.DropInstanceCommandTokenExist(token.DefnId, defn.InstId)
					if err != nil {
						logging.Warnf("DDLServiceMgr: Failed to check drop instance token.  Skip processing %v.  Error = %v.", entry, err)
					}
					if exist {
						logging.Infof("DDLServiceMgr: Drop instance token exist.  Will not create index instance for %v.", entry)
						continue
					}

					// for every partition for this instance, check to see if the partition exist in the cluster.
					for _, partition := range defn.Partitions {

						found := false
						status := common.INDEX_STATE_NIL
						indexerId2 := common.INDEXER_ID_NIL

						// find if partition exist in cluster
						index := provider.FindIndexIgnoreStatus(defn.DefnId)
						if index != nil {
							found, status, indexerId2 = findPartition(defn.InstId, partition, index.Instances)
							if !found {
								// is the partition under rebalance?
								found, status, indexerId2 = findPartition(defn.InstId, partition, index.InstsInRebalance)
							}
						}

						// cannot delete if not found or has not been built
						if !found || (!defn.Deferred && status < common.INDEX_STATE_INITIAL) {
							canDelete = false
						}

						// cannot delete if it is deferred but overall index state is INITIAL/CATCHUP/ACTIVE
						if defn.Deferred && found && status < common.INDEX_STATE_INITIAL && index != nil &&
							(index.State == common.INDEX_STATE_INITIAL || index.State == common.INDEX_STATE_CATCHUP || index.State == common.INDEX_STATE_ACTIVE) {
							canDelete = false
						}

						// If the partition is not found in the cluster, then create it locally.
						if !found && indexerId == m.indexerId {
							newPartitionList = append(newPartitionList, partition)
							newVersionList = append(newVersionList, 0)
						}

						// If the partition is not deferred, then we may need to build it
						// if the partition has not been build and it matches the local indexer id
						if !defn.Deferred && found && status < common.INDEX_STATE_INITIAL && indexerId2 == m.indexerId {
							buildMap[defn.DefnId] = true
						}

						// If index is deferred, but overall index status is active, build the remaining replica/partition.
						if defn.Deferred && found && status < common.INDEX_STATE_INITIAL && indexerId2 == m.indexerId && index != nil &&
							(index.State == common.INDEX_STATE_INITIAL || index.State == common.INDEX_STATE_CATCHUP || index.State == common.INDEX_STATE_ACTIVE) {
							buildMap[defn.DefnId] = true
						}
					}

					// Cannot find the partitions in the cluster.   Create the index locally.  If there is an
					// error, create index will retry in the next iteration.
					if len(newPartitionList) != 0 {

						// Avoid DDL during rebalance.   This is just for extra safety since indexer will reject
						// DDL when rebalancing going on.
						if !m.canProcessDDL() {
							logging.Infof("DDLServiceMgr: cannot process create token during rebalancing")
							return
						}

						// If bucket UUID has chnanged, create index would fail
						defn.BucketUUID = token.BucketUUID
						defn.Partitions = newPartitionList
						defn.Versions = newVersionList

						// Before a create token is posted, at least one indexer has tried to create the index to validate
						// all invariant conditions (e.g. enterprise version).   These invariant conditions are applicable
						// to all indexers.   By the time when DDL Service manager tries to create the index, it should
						// not have to worry about the invariant conditions have changed.  Therefore, we should expect
						// all errros from create index is recoverable, except for the following cases:
						// 1) bucket is deleted.   BucketUUID will ensure that the index cannot be created.   LifecyleMgr
						//    will remove the create token when cleaning up metadata for the bucket.
						// 2) metadata is corrupted.   We cannot detect this, but we will know since the indexer would be
						//    in a bad state.
						if err := provider.SendCreateIndexRequest(m.indexerId, &defn, false); err != nil {
							logging.Warnf("DDLServiceMgr: Failed to process create index (%v, %v, %v, %v).  Error = %v.",
								defn.Bucket, defn.Name, defn.DefnId, defn.InstId, err)
						} else {
							logging.Infof("DDLServiceMgr: Index successfully created (%v, %v, %v, %v).",
								defn.Bucket, defn.Name, defn.DefnId, defn.InstId)
							// If the partition is not deferred, build it
							if !defn.Deferred {
								buildMap[defn.DefnId] = true
							}
						}
					}
				}
			}

			if canDelete {
				// If all the instances and partitions are accounted for, then delete the create token.
				deleteMap[entry] = token
			}
		}
	}

	// Try build the index that has just been created
	if len(buildMap) != 0 {
		// Avoid DDL during rebalance.   This is just for extra safety since indexer will reject
		// DDL when rebalancing going on.
		if !m.canProcessDDL() {
			logging.Infof("DDLServiceMgr: cannot process create token during rebalancing")
			return
		}

		buildList := make([]common.IndexDefnId, 0, len(buildMap))
		for id, _ := range buildMap {
			buildList = append(buildList, id)
		}

		logging.Infof("DDLServiceMgr: Build Index.  Index Defn List: %v", buildList)

		if err := provider.SendBuildIndexRequest(m.indexerId, buildList, m.localAddr); err != nil {
			// All errors received from build index are expected to be recoverable.
			logging.Warnf("DDLServiceMgr: Failed to build index after creation. Error = %v.", err)
			return
		}
	}

	// At this point, we have a list of token which has all the instances and partitions being created and built.
	// Delete those create token.
	if len(deleteMap) != 0 {
		// Avoid deleting create token during rebalance.  This is just for extra safety.  Even if the planner
		// may use the create token during planning, it just mean that it is trying to create those partitions on
		// behalf of the DDL service manager.   As long as the drop token exists, then those partitions will be
		// dropped eventually.   If the planner do not see this create token, then no harm is done.
		if !m.canProcessDDL() {
			logging.Infof("DDLServiceMgr: cannot delete create token during rebalancing")
			return

		}

		for path, token := range deleteMap {
			var defn common.IndexDefn
			var numReplica common.Counter

			for _, definitions := range token.Definitions {
				if len(definitions) != 0 {
					numReplica = definitions[0].NumReplica2
					break
				}
			}

			if numReplica.IsValid() {
				defn.DefnId = token.DefnId
				defn.NumReplica2 = numReplica

				logging.Infof("DDLServiceMgr: Update Replica Count.  Index Defn %v replica Count %v", defn.DefnId, defn.NumReplica2)

				if err := provider.BroadcastAlterReplicaCountRequest(&defn); err != nil {
					// All errors received from alter replica count are expected to be recoverable.
					logging.Warnf("DDLServiceMgr: Failed to alter replica count. Error = %v.", err)
					continue
				}
			}

			if err := mc.DeleteCreateCommandToken(token.DefnId, token.RequestId); err != nil {
				logging.Warnf("DDLServiceMgr: Failed to remove create index token %v. Error = %v", token.DefnId, err)
			} else {
				delete(retryList, path)
				logging.Infof("DDLServiceMgr: Remove create index token %v.", token.DefnId)
			}
		}
	}
}

func (m *DDLServiceMgr) processCreateCommand() {

	m.commandListener.ListenTokens()

	lastCheck := time.Now()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		retryInterval := 5 * time.Minute
		if config := m.config.Load(); config != nil {
			retryInterval = time.Duration(config["ddl.create.retryInterval"].Int()) * time.Second
		}

		select {
		case <-ticker.C:
			needRefresh := false
			if time.Now().Sub(lastCheck) > retryInterval {
				logging.Infof("DDLServiceMgr checking create token progress")
				needRefresh = true
				lastCheck = time.Now()
			}

			m.handleCreateCommand(needRefresh)

		case _, ok := <-m.listenerDonech:
			if !ok {
				m.startCommandListner()
				m.commandListener.ListenTokens()
			}

		case <-m.killch:
			logging.Infof("author: Index author go-routine terminates.")
			return
		}
	}
}

//////////////////////////////////////////////////////////////
// Drop Instance Token
//////////////////////////////////////////////////////////////

//
// Recover drop instance command
//
func (m *DDLServiceMgr) cleanupDropInstanceCommand() {

	entries, err := metakv.ListAllChildren(mc.DropInstanceDDLCommandTokenPath)
	if err != nil {
		logging.Warnf("DDLServiceMgr: Failed to cleanup delete index instance token upon rebalancing.  Skip cleanup.  Internal Error = %v", err)
		return
	}

	for _, entry := range entries {

		if strings.Contains(entry.Path, mc.DropInstanceDDLCommandTokenPath) && entry.Value != nil {

			logging.Infof("DDLServiceMgr: processing delete index instance token %v", entry.Path)

			command, err := mc.UnmarshallDropInstanceCommandToken(entry.Value)
			if err != nil {
				logging.Warnf("DDLServiceMgr: Failed to clean delete index instance token upon rebalancing.  Skip command %v.  Internal Error = %v.",
					entry.Path, err)
				continue
			}

			// If there is a create token, then do not process the drop token.
			// Let the create token be deleted first to avoid any unexpected
			// race condition.  This is more for safety than necessity.
			exist, err := mc.CreateCommandTokenExist(command.DefnId)
			if err != nil {
				logging.Warnf("DDLServiceMgr: Failed to check create token.  Skip command %v.  Error = %v.", entry.Path, err)
				continue
			}
			if exist {
				logging.Warnf("DDLServiceMgr: Create token exist for %v.  Skip processing drop token %v.", command.DefnId, entry.Path)
				continue
			}

			// Find if the index still exist in the cluster.  DDLServiceManger will only cleanup the delete token IF there is no index instance.
			// This means the indexer must have been able to process the deleted token before DDLServiceManager has a chance to clean it up.
			//
			// 1) It will skip DELETED index.  DELETED index will be cleaned up by lifecycle manager periodically.
			// 2) At this point, the metadata provider has been connected to all indexer at least once (refreshOnTopology gurantees that).   So
			//    metadata provider has a snapshot of the metadata from each indexer at some point in time.   It will return index even if metadata
			//    provider is not connected to the indexer at the exact moment when this call is made.
			//
			//
			if m.provider.FindIndexInstanceIgnoreStatus(command.DefnId, command.InstId) == nil {
				var defn common.IndexDefn
				defn.DefnId = command.DefnId
				defn.NumReplica2 = command.Defn.NumReplica2

				logging.Infof("DDLServiceMgr: Update Replica Count.  Index Defn %v replica Count %v", defn.DefnId, defn.NumReplica2)

				if err := m.provider.BroadcastAlterReplicaCountRequest(&defn); err != nil {
					// All errors received from alter replica count are expected to be recoverable.
					logging.Warnf("DDLServiceMgr: Failed to alter replica count. Error = %v.", err)
					continue
				}

				// There is no index in the cluster,  remove token
				if err := common.MetakvDel(entry.Path); err != nil {
					logging.Warnf("DDLServiceMgr: Failed to remove delete index token %v. Error = %v", entry.Path, err)
				} else {
					logging.Infof("DDLServiceMgr: Remove delete index token %v.", entry.Path)
				}
			} else {
				logging.Infof("DDLServiceMgr: Indexer still holding index definiton.  Skip removing delete index token %v.", entry.Path)
			}
		}
	}
}

//////////////////////////////////////////////////////////////
// Storage Mode
//////////////////////////////////////////////////////////////

//
// Update clsuter storage mode if necessary
//
func (m *DDLServiceMgr) handleClusterStorageMode(httpAddrMap map[string]string) {

	if m.donech != nil {
		close(m.donech)
		m.donech = nil
	}

	m.provider.RefreshIndexerVersion()
	if m.provider.GetIndexerVersion() != common.INDEXER_CUR_VERSION {
		return
	}

	storageMode := common.StorageMode(common.NOT_SET)
	initialized := false
	indexCount := 0

	indexes, _ := m.provider.ListIndex()
	for _, index := range indexes {

		for _, inst := range index.Instances {

			indexCount++

			// Any plasma index should have storage mode in index instance.
			// So skip any index that does not have storage mode (either not
			// upgraded yet or no need to upgrade).
			if len(inst.StorageMode) == 0 {
				return
			}

			// If this is not a valid index type, then return.
			if !common.IsValidIndexType(inst.StorageMode) {
				logging.Errorf("DDLServiceMgr: unable to change storage mode to %v after rebalance.  Invalid storage type for index %v (%v, %v)",
					inst.StorageMode, index.Definition.Name, index.Definition.Bucket)
				return
			}

			indexStorageMode := common.IndexTypeToStorageMode(common.IndexType(inst.StorageMode))

			if !initialized {
				storageMode = indexStorageMode
				initialized = true
				continue
			}

			// storage mode has not yet converged
			if indexStorageMode != storageMode {
				return
			}
		}
	}

	if indexCount == 0 {
		storageMode = m.provider.GetStorageMode()
	}

	// if storage mode for all indexes converge, then change storage mode setting
	clusterStorageMode := common.GetClusterStorageMode()
	if storageMode != common.StorageMode(common.NOT_SET) && storageMode != clusterStorageMode {
		if !m.updateStorageMode(storageMode, httpAddrMap) {
			m.donech = make(chan bool)
			go m.retryUpdateStorageMode(storageMode, httpAddrMap, m.donech)
		}
	}
}

func (m *DDLServiceMgr) retryUpdateStorageMode(storageMode common.StorageMode, httpAddrMap map[string]string, donech chan bool) {

	for true {
		select {
		case <-donech:
			return
		default:
			time.Sleep(time.Minute)
			if m.updateStorageMode(storageMode, httpAddrMap) {
				return
			}
		}
	}
}

func (m *DDLServiceMgr) updateStorageMode(storageMode common.StorageMode, httpAddrMap map[string]string) bool {

	clusterStorageMode := common.GetClusterStorageMode()
	if storageMode == clusterStorageMode {
		logging.Infof("DDLServiceMgr: All indexes have converged to cluster storage mode %v after rebalance.", storageMode)
		return true
	}

	settings := make(map[string]string)
	settings["indexer.settings.storage_mode"] = string(common.StorageModeToIndexType(storageMode))

	body, err := json.Marshal(&settings)
	if err != nil {
		logging.Errorf("DDLServiceMgr: unable to change storage mode to %v after rebalance.  Error:%v", storageMode, err)
		return false
	}
	bodybuf := bytes.NewBuffer(body)

	for _, addr := range httpAddrMap {

		resp, err := postWithAuth(addr+"/internal/settings", "application/json", bodybuf)
		if err != nil {
			logging.Errorf("DDLServiceMgr:handleClusterStorageMode(). Encountered error when try to change setting.  Retry with another indexer node. Error:%v", err)
			continue
		}

		if resp != nil && resp.StatusCode != 200 {
			logging.Errorf("DDLServiceMgr:handleClusterStorageMode(). HTTP status (%v) when try to change setting.  Retry with another indexer node.", resp.Status)
			continue
		}

		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}

		logging.Infof("DDLServiceMgr: cluster storage mode changed to %v after rebalance.", storageMode)
		return true
	}

	logging.Errorf("DDLServiceMgr: unable to change storage mode to %v after rebalance.", storageMode)
	return false
}

//////////////////////////////////////////////////////////////
// REST
//////////////////////////////////////////////////////////////

func (m *DDLServiceMgr) handleListMetadataTokens(w http.ResponseWriter, r *http.Request) {

	if !m.validateAuth(w, r) {
		logging.Errorf("DDLServiceMgr::handleListMetadataTokens Validation Failure req: %v", common.GetHTTPReqInfo(r))
		return
	}

	if r.Method == "GET" {

		logging.Infof("DDLServiceMgr::handleListMetadataTokens Processing Request req: %v", common.GetHTTPReqInfo(r))

		buildTokens, err := metakv.ListAllChildren(mc.BuildDDLCommandTokenPath)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error() + "\n"))
			return
		}

		deleteTokens, err1 := metakv.ListAllChildren(mc.DeleteDDLCommandTokenPath)
		if err1 != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err1.Error() + "\n"))
			return
		}

		createTokens, err2 := mc.ListCreateCommandToken()
		if err2 != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err2.Error() + "\n"))
			return
		}

		header := w.Header()
		header["Content-Type"] = []string{"application/json"}

		for _, entry := range buildTokens {

			if strings.Contains(entry.Path, mc.BuildDDLCommandTokenPath) && entry.Value != nil {
				w.Write([]byte(entry.Path + " - "))
				w.Write(entry.Value)
				w.Write([]byte("\n"))
			}
		}

		for _, entry := range deleteTokens {

			if strings.Contains(entry.Path, mc.DeleteDDLCommandTokenPath) && entry.Value != nil {
				w.Write([]byte(entry.Path + " - "))
				w.Write(entry.Value)
				w.Write([]byte("\n"))
			}
		}

		for _, entry := range createTokens {

			defnId, requestId, err := mc.GetDefnIdFromCreateCommandTokenPath(entry)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error() + "\n"))
				return
			}

			token, err := mc.FetchCreateCommandToken(defnId, requestId)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error() + "\n"))
				return
			}

			buf, err := json.Marshal(token)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error() + "\n"))
				return
			}

			w.Write([]byte(entry + " - "))
			w.Write(buf)
			w.Write([]byte("\n"))
		}

		w.WriteHeader(http.StatusOK)
	}
}

func (m *DDLServiceMgr) handleListCreateTokens(w http.ResponseWriter, r *http.Request) {

	if !m.validateAuth(w, r) {
		logging.Errorf("DDLServiceMgr::handleListCreateTokens Validation Failure req: %v", common.GetHTTPReqInfo(r))
		return
	}

	if r.Method == "GET" {

		logging.Infof("DDLServiceMgr::handleListCreateTokens Processing Request req: %v", common.GetHTTPReqInfo(r))

		createTokens, err := mc.ListCreateCommandToken()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error() + "\n"))
			return
		}

		list := &mc.CreateCommandTokenList{}

		for _, entry := range createTokens {

			defnId, requestId, err := mc.GetDefnIdFromCreateCommandTokenPath(entry)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error() + "\n"))
				continue
			}

			token, err := mc.FetchCreateCommandToken(defnId, requestId)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error() + "\n"))
				return
			}

			if token != nil {
				list.Tokens = append(list.Tokens, *token)
			}
		}

		buf, err := mc.MarshallCreateCommandTokenList(list)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error() + "\n"))
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write(buf)
	}
}

// handleListDeleteTokens responds to /listDeleteTokens REST endpoint
// with a list of all delete tokens in metakv on this indexer host.
func (m *DDLServiceMgr) handleListDeleteTokens(w http.ResponseWriter, r *http.Request) {

	if !m.validateAuth(w, r) {
		logging.Errorf("DDLServiceMgr::handleListDeleteTokens Validation Failure req: %v", common.GetHTTPReqInfo(r))
		return
	}

	if r.Method == "GET" {

		logging.Infof("DDLServiceMgr::handleListDeleteTokens Processing Request req: %v", common.GetHTTPReqInfo(r))

		deleteTokens, err := mc.ListDeleteCommandToken()
		if err != nil {
			logging.Errorf("DDLServiceMgr::handleListDeleteTokens error %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error() + "\n"))
			return
		}

		list := &mc.DeleteCommandTokenList{}
		list.Tokens = make([]mc.DeleteCommandToken, 0, len(deleteTokens))

		for _, token := range deleteTokens {
			list.Tokens = append(list.Tokens, *token)
		}

		buf, err := mc.MarshallDeleteCommandTokenList(list)
		if err != nil {
			logging.Errorf("DDLServiceMgr::handleListDeleteTokens error %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error() + "\n"))
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write(buf)
	}
}

// handleListDropInstanceTokens responds to /listDropInstanceTokens REST endpoint
// with a list of all drop instance tokens in metakv on this indexer host.
func (m *DDLServiceMgr) handleListDropInstanceTokens(w http.ResponseWriter, r *http.Request) {

	if !m.validateAuth(w, r) {
		logging.Errorf("DDLServiceMgr::handleListDropInstanceTokens Validation Failure req: %v", common.GetHTTPReqInfo(r))
		return
	}

	if r.Method == "GET" {

		logging.Infof("DDLServiceMgr::handleListDropInstanceTokens Processing Request req: %v", common.GetHTTPReqInfo(r))

		deleteTokens, err := mc.ListAndFetchAllDropInstanceCommandToken()
		if err != nil {
			logging.Errorf("DDLServiceMgr::handleListDropInstanceTokens Error %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error() + "\n"))
			return
		}

		list := &mc.DropInstanceCommandTokenList{}
		list.Tokens = make([]mc.DropInstanceCommandToken, 0, len(deleteTokens))

		for _, token := range deleteTokens {
			list.Tokens = append(list.Tokens, *token)
		}

		buf, err := mc.MarshallDropInstanceCommandTokenList(list)
		if err != nil {
			logging.Errorf("DDLServiceMgr::handleListDropInstanceTokens Error %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error() + "\n"))
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write(buf)
	}
}

func (m *DDLServiceMgr) validateAuth(w http.ResponseWriter, r *http.Request) bool {
	_, valid, err := common.IsAuthValid(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error() + "\n"))
	} else if valid == false {
		w.WriteHeader(401)
		w.Write([]byte("401 Unauthorized\n"))
	}
	return valid
}

func (m *DDLServiceMgr) startCommandListner() {
	donech := make(chan bool)
	m.commandListener = mc.NewCommandListener(donech, true, true, true, false)
	m.listenerDonech = donech
}

//////////////////////////////////////////////////////////////
// Metadata Provider
//////////////////////////////////////////////////////////////

func (m *DDLServiceMgr) refreshMetadataProvider() (map[string]string, error) {

	if m.provider != nil {
		m.provider.Close()
		m.provider = nil
	}

	nodes := make(map[service.NodeID]bool)
	for key, value := range m.nodes {
		nodes[key] = value
	}

	// nodes can be empty but it cannot be nil.
	// If emtpy, then no node will be considered.
	// If nil, all nodes will be considered.
	provider, httpAddrMap, err := m.newMetadataProvider(nodes)
	if err != nil {
		return nil, err
	}

	m.provider = provider
	return httpAddrMap, nil
}

func (m *DDLServiceMgr) newMetadataProvider(nodes map[service.NodeID]bool) (*client.MetadataProvider, map[string]string, error) {

	// initialize ClusterInfoCache
	url, err := common.ClusterAuthUrl(m.clusterAddr)
	if err != nil {
		return nil, nil, err
	}

	cinfo, err := common.NewClusterInfoCache(url, DEFAULT_POOL)
	if err != nil {
		return nil, nil, err
	}
	cinfo.SetUserAgent("newMetadataProvider")

	if err := cinfo.Fetch(); err != nil {
		return nil, nil, err
	}

	adminAddrMap := make(map[string]string)
	httpAddrMap := make(map[string]string)

	// If a node list is given, then honor the node list by verifying that it can reach
	// to every node in the list.
	if nodes != nil {
		// Discover indexer service from ClusterInfoCache
		nids := cinfo.GetNodesByServiceType(common.INDEX_HTTP_SERVICE)
		for _, nid := range nids {

			addr, err := cinfo.GetServiceAddress(nid, common.INDEX_HTTP_SERVICE, true)
			if err == nil {

				resp, err := getWithAuth(addr + "/getLocalIndexMetadata")
				if err != nil {
					continue
				}

				localMeta := new(manager.LocalIndexMetadata)
				if err := convertResponse(resp, localMeta); err != nil {
					continue
				}

				// Only consider valid nodes.  If nodes is nil, then all nodes are considered.
				if nodes[service.NodeID(localMeta.NodeUUID)] {
					httpAddrMap[localMeta.NodeUUID] = addr

					adminAddr, err := cinfo.GetServiceAddress(nid, common.INDEX_ADMIN_SERVICE, true)
					if err != nil {
						return nil, nil, err
					}

					adminAddrMap[localMeta.NodeUUID] = adminAddr
					delete(nodes, service.NodeID(localMeta.NodeUUID))
				}
			}
		}

		if len(nodes) != 0 {
			return nil, nil, errors.New(
				fmt.Sprintf("DDLServiceMgr: Failed to initialize metadata provider.  Unknown host=%v", nodes))
		}
	} else {
		// Find all nodes that has a index http service
		// 1) This method will exclude inactive_failed node in the cluster.  But if a node failed after the topology is fetched, then
		//    metadata provider could eventually fail (if cannot connect to indexer service).  Note that ns-server will shutdown indexer
		//    service due to failed over.
		// 2) This method will exclude inactive_new node in the cluster.
		// 3) This may include unhealthy node since unhealthiness is not a cluster membership state (need verification).  If it
		//    metadata provider cannot reach the unhealthy node, the metadata provider may not start (expected behavior).
		nids := cinfo.GetNodesByServiceType(common.INDEX_HTTP_SERVICE)

		for _, nid := range nids {
			adminAddr, err := cinfo.GetServiceAddress(nid, common.INDEX_ADMIN_SERVICE, true)
			if err != nil {
				return nil, nil, err
			}
			nodeUUID := cinfo.GetNodeUUID(nid)
			adminAddrMap[nodeUUID] = adminAddr
		}
	}

	// initialize a new MetadataProvider
	ustr, err := common.NewUUID()
	if err != nil {
		return nil, nil, errors.New(fmt.Sprintf("DDLServiceMgr: Failed to initialize metadata provider.  Internal Error = %v", err))
	}
	providerId := ustr.Str()

	provider, err := client.NewMetadataProvider(m.clusterAddr, providerId, nil, nil, m.settings)
	if err != nil {
		if provider != nil {
			provider.Close()
		}
		return nil, nil, err
	}

	// Watch Metadata
	for _, addr := range adminAddrMap {
		logging.Infof("DDLServiceMgr: connecting to node %v", addr)
		provider.WatchMetadata(addr, nil, len(adminAddrMap))
	}

	// Make sure that the metadata provider is synchronized with the index.
	// If it cannot synchronize within 5sec, then return error.
	if !provider.AllWatchersAlive() {

		// Wait for initialization complete
		ticker := time.NewTicker(time.Millisecond * 500)
		defer ticker.Stop()
		retry := 60 // Is set to 60 for 30 Sec timeout for watchers to go live.

		for range ticker.C {
			retry = retry - 1
			if provider.AllWatchersAlive() {
				return provider, httpAddrMap, nil
			}

			if retry == 0 {
				for nodeUUID, adminport := range adminAddrMap {
					if !provider.IsWatcherAlive(nodeUUID) {
						logging.Warnf("DDLServiceMgr: cannot connect to node %v", adminport)
					}
				}

				provider.Close()
				return nil, nil, errors.New("DDLServiceMgr: Failed to initialize metadata provider. " +
					" Unable to connect to all indexer nodes within 30 seconds.")
			}
		}
	}

	return provider, httpAddrMap, nil
}

//////////////////////////////////////////////////////////////
// Topology change
//////////////////////////////////////////////////////////////

//
// Callback to notify there is a topology change
//
func (m *DDLServiceMgr) refreshOnTopologyChange(change *service.TopologyChange, isCancel bool) (map[string]string, error) {

	logging.Infof("DDLServiceMgr.refreshOnTopologyChange()")

	m.nodes = make(map[service.NodeID]bool)
	for _, node := range change.KeepNodes {
		m.nodes[node.NodeInfo.NodeID] = true
	}

	if isCancel {
		for _, node := range change.EjectNodes {
			m.nodes[node.NodeID] = true
		}
	}

	// If fail to intiialize metadata provider, then just continue.  It will try
	// to repair metadata provider upon the first DDL comes.
	httpAddrMap, err := m.refreshMetadataProvider()
	if err != nil {
		logging.Errorf("DDLServiceMgr: notifyNewTopologyChange(): Failed to initialize metadata provider.  Error=%v.", err)
		return nil, err
	}

	return httpAddrMap, nil
}

//////////////////////////////////////////////////////////////
// Settings
//////////////////////////////////////////////////////////////

func (s *ddlSettings) NumReplica() int32 {
	return atomic.LoadInt32(&s.numReplica)
}

func (s *ddlSettings) NumPartition() int32 {
	return atomic.LoadInt32(&s.numPartition)
}

func (s *ddlSettings) StorageMode() string {

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return s.storageMode
}

func (s *ddlSettings) UsePlanner() bool {
	return true
}

// DDLServiceMgr does not trigger recoverableCreateIndex for index creation.
// So, this setting will not be used by DDLServiceMgr.
func (s *ddlSettings) AllowPartialQuorum() bool {
	return false
}

func (s *ddlSettings) handleSettings(config common.Config) {

	numReplica := int32(config["settings.num_replica"].Int())
	if numReplica >= 0 {
		atomic.StoreInt32(&s.numReplica, numReplica)
	} else {
		logging.Errorf("DDLServiceMgr: invalid setting value for num_replica=%v", numReplica)
	}

	numPartition := int32(config["numPartitions"].Int())
	if numPartition > 0 {
		atomic.StoreInt32(&s.numPartition, numPartition)
	} else {
		logging.Errorf("DDLServiceMgr: invalid setting value for numPartitions=%v", numPartition)
	}

	storageMode := config["settings.storage_mode"].String()
	if len(storageMode) != 0 {
		func() {
			s.mutex.Lock()
			defer s.mutex.Unlock()
			s.storageMode = storageMode
		}()
	}
}
