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
	"github.com/couchbase/cbauth"
	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/indexing/secondary/audit"
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
	"io/ioutil"
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

// DDLServiceMgr Definition
type DDLServiceMgr struct {
	indexerId                    common.IndexerId
	config                       common.ConfigHolder
	supvCmdch                    MsgChannel //supervisor sends commands on this channel
	supvMsgch                    MsgChannel //channel to send any message to supervisor
	nodeID                       service.NodeID
	localAddr                    string
	clusterAddr                  string
	settings                     *ddlSettings
	retryUpdateStorageModeDoneCh chan bool
	killch                       chan bool

	allowDDL      bool         // allow new DDL to start? false during Rebalance
	allowDDLMutex sync.RWMutex // protects allowDDL

	commandListener *mc.CommandListener
	listenerDonech  chan bool

	tokenCleanerStopCh            chan struct{} // close to stop runTokenCleaner goroutine
	handleClusterStorageModeMutex sync.Mutex    // serializes handleClusterStorageMode calls

	deleteTokenCache map[common.IndexDefnId]int64 // unixnano timestamp
}

const DELETE_TOKEN_DELAYED_CLEANUP_INTERVAL = 24 * time.Hour

// DDL related settings
type ddlSettings struct {
	numReplica   int32
	numPartition int32

	maxNumPartition int32

	storageMode      string
	storageModeMutex sync.RWMutex // protects storageMode

	allowPartialQuorum uint32
	useGreedyPlanner   uint32

	isShardAffinityEnabled uint32
	binSize                uint64

	//serverless configs
	memHighThreshold int32
	memLowThreshold  int32
	indexLimit       uint32

	allowDDLDuringScaleUp uint32

	allowNodesClause uint32
	deferBuild       atomic.Bool
}

//////////////////////////////////////////////////////////////
// Global Variables
//////////////////////////////////////////////////////////////

var gDDLServiceMgr *DDLServiceMgr
var gDDLServiceMgrLck sync.RWMutex // protects gDDLServiceMgr, which is assigned only once

//////////////////////////////////////////////////////////////
// DDLServiceMgr
//////////////////////////////////////////////////////////////

// Constructor
func NewDDLServiceMgr(indexerId common.IndexerId, supvCmdch MsgChannel, supvMsgch MsgChannel, config common.Config) (*DDLServiceMgr, Message) {

	addr := config["clusterAddr"].String()
	port := config["httpPort"].String()
	host, _, _ := net.SplitHostPort(addr)
	localaddr := net.JoinHostPort(host, port)

	nodeId := service.NodeID(config["nodeuuid"].String())

	numReplica := int32(config["settings.num_replica"].Int())
	settings := &ddlSettings{numReplica: numReplica}

	mgr := &DDLServiceMgr{
		indexerId:                    indexerId,
		supvCmdch:                    supvCmdch,
		supvMsgch:                    supvMsgch,
		localAddr:                    localaddr,
		clusterAddr:                  addr,
		nodeID:                       nodeId,
		settings:                     settings,
		retryUpdateStorageModeDoneCh: nil,
		killch:                       make(chan bool),
		allowDDL:                     true,
		tokenCleanerStopCh:           make(chan struct{}),
		deleteTokenCache:             make(map[common.IndexDefnId]int64),
	}

	mgr.startCommandListner()

	mgr.config.Store(config)

	mux := GetHTTPMux()
	mux.HandleFunc("/listMetadataTokens", mgr.handleListMetadataTokens)
	mux.HandleFunc("/listCreateTokens", mgr.handleListCreateTokens)
	mux.HandleFunc("/listDeleteTokens", mgr.handleListDeleteTokens)
	mux.HandleFunc("/listDeleteTokenPaths", mgr.handleListDeleteTokenPaths)
	mux.HandleFunc("/listDropInstanceTokens", mgr.handleListDropInstanceTokens)
	mux.HandleFunc("/listDropInstanceTokenPaths", mgr.handleListDropInstanceTokenPaths)
	mux.HandleFunc("/listScheduleCreateTokens", mgr.handleListScheduleCreateTokens)
	mux.HandleFunc("/listStopScheduleCreateTokens", mgr.handleListStopScheduleCreateTokens)
	mux.HandleFunc("/transferScheduleCreateTokens", mgr.handleTransferScheduleCreateTokens)

	go mgr.run()
	go mgr.runTokenCleaner()

	setDDLServiceMgr(mgr)

	logging.Infof("DDLServiceMgr: intialized. Local nodeUUID %v", mgr.nodeID)
	return mgr, &MsgSuccess{}
}

// Get DDLServiceMgr singleton
func getDDLServiceMgr() *DDLServiceMgr {
	gDDLServiceMgrLck.RLock()
	defer gDDLServiceMgrLck.RUnlock()

	return gDDLServiceMgr
}

// Set DDLServiceMgr singleton
func setDDLServiceMgr(mgr *DDLServiceMgr) {
	gDDLServiceMgrLck.Lock()
	gDDLServiceMgr = mgr
	gDDLServiceMgrLck.Unlock()
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
					close(m.tokenCleanerStopCh)
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

// runTokenCleaner runs in a goroutine that periodically deletes old DDL command tokens from metakv.
func (m *DDLServiceMgr) runTokenCleaner() {
	const method = "DDLServiceMgr::runTokenCleaner:" // for logging
	const period = 5 * time.Minute                   // period of cleanup runs

	logging.Infof("%v Starting with period %v", method, period)
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for {
		select {

		case <-ticker.C:
			m.cleanupTokens()

		case <-m.tokenCleanerStopCh:
			logging.Infof("%v Shutting down", method)
			return
		}
	}
}

// cleanupTokens is a helper for runTokenCleaner. It does one pass of deleting old DDL command
// tokens from metakv.
func (m *DDLServiceMgr) cleanupTokens() {
	const method = "DDLServiceMgr::cleanupTokens:" // for logging

	// Use latest metadata provider.
	provider, _, failedNodes, err := newMetadataProvider(m.clusterAddr, nil, m.settings, method)
	if err != nil {
		logging.Errorf("%v newMetadataProvider returned error: %v. Skipping cleanup.",
			method, err)
		return
	}
	if provider == nil {
		logging.Errorf("%v nil MetadataProvider. Skipping cleanup.", method)
		return
	}
	defer provider.Close()

	if failedNodes {
		logging.Errorf("%v cluster has failed nodes. Skipping cleanup.", method)
		return
	}

	// Clean up old tokens
	allCreateCommandTokens := m.cleanupCreateCommand(provider)
	m.cleanupBuildCommand(provider)
	m.cleanupDelCommand(provider)
	m.cleanupDropInstanceCommand(provider, allCreateCommandTokens)
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

	sic := getSchedIndexCreator()
	if sic != nil {
		sic.stopProcessDDL()
	}
}

func resumeDDLProcessing() {

	mgr := getDDLServiceMgr()
	if mgr != nil {
		mgr.startProcessDDL()
	}

	sic := getSchedIndexCreator()
	if sic != nil {
		sic.startProcessDDL()
	}
}

// This is run as a go-routine.  Rebalancing could have finished while
// this gorountine is still running.
func notifyRebalanceDone(change *service.TopologyChange, isCancel bool) {

	mgr := getDDLServiceMgr()
	if mgr != nil {
		mgr.rebalanceDone(change, isCancel)
	}

	sic := getSchedIndexCreator()
	if sic != nil {
		sic.rebalanceDone()
	}
}

// Recover DDL command
func (m *DDLServiceMgr) rebalanceDone(change *service.TopologyChange, isCancel bool) {
	const method = "DDLServiceMgr::rebalanceDone:" // for logging

	logging.Infof("%v Called", method)
	defer logging.Infof("%v Returned", method)

	m.startProcessDDL()

	// nodes can be empty but it cannot be nil.
	nodes := getNodesInfo(change, isCancel)
	provider, httpAddrMap, _, err := newMetadataProvider(m.clusterAddr, nodes, m.settings, method)
	if err != nil {
		logging.Errorf("%v Failed to initialize metadata provider.  Error=%v.", method, err)
		return
	}
	defer provider.Close()
	m.handleClusterStorageMode(httpAddrMap, provider)
}

func getNodesInfo(change *service.TopologyChange, isCancel bool) map[service.NodeID]bool {
	nodes := make(map[service.NodeID]bool)
	for _, node := range change.KeepNodes {
		nodes[node.NodeInfo.NodeID] = true
	}

	if isCancel {
		for _, node := range change.EjectNodes {
			nodes[node.NodeID] = true
		}
	}
	return nodes
}

func (m *DDLServiceMgr) stopProcessDDL() {
	m.allowDDLMutex.Lock()
	defer m.allowDDLMutex.Unlock()
	m.allowDDL = false
}

func (m *DDLServiceMgr) canProcessDDL() bool {
	m.allowDDLMutex.RLock()
	defer m.allowDDLMutex.RUnlock()
	return m.allowDDL
}

func (m *DDLServiceMgr) startProcessDDL() {
	m.allowDDLMutex.Lock()
	defer m.allowDDLMutex.Unlock()
	m.allowDDL = true
}

//////////////////////////////////////////////////////////////
// Drop Token
//////////////////////////////////////////////////////////////

// Recover drop index command
func (m *DDLServiceMgr) cleanupDelCommand(provider *client.MetadataProvider) {
	if !m.canProcessDDL() {
		return
	}

	entries, err := metakv.ListAllChildren(mc.DeleteDDLCommandTokenPath)
	if err != nil {
		logging.Warnf("DDLServiceMgr: Failed to cleanup delete index token upon rebalancing.  Skip cleanup.  Internal Error = %v", err)
		return
	}

	if len(entries) == 0 {
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

			// Delete the schedule create token
			if err := mc.DeleteScheduleCreateToken(command.DefnId); err != nil {
				logging.Warnf("DDLServiceMgr: Failed to delete schedule create token upon rebalancing.  Skip command %v.  Internal Error = %v.", entry.Path, err)
				continue
			}

			// Delete the stop schedule create token
			if err := mc.DeleteStopScheduleCreateToken(command.DefnId); err != nil {
				logging.Warnf("DDLServiceMgr: Failed to delete stop schedule create token upon rebalancing.  Skip command %v.  Internal Error = %v.", entry.Path, err)
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
			// 2) At this point, metadata provider has been connected to all indexer at least once,
			//    so it has a snapshot of the metadata from each indexer at some point in time.
			//    It will return index even if metadata
			//    provider is not connected to the indexer at the exact moment when this call is made.
			//
			//
			if provider.FindIndexIgnoreStatus(command.DefnId) == nil {
				// There is no index in the cluster,  remove token

				if !m.canProcessDDL() {
					return
				}
				var timestamp int64
				timestamp, ok := m.deleteTokenCache[command.DefnId]
				if !ok {
					timestamp = time.Now().UnixNano()
					m.deleteTokenCache[command.DefnId] = timestamp
				}
				if time.Duration(time.Now().UnixNano()-timestamp) <= time.Duration(DELETE_TOKEN_DELAYED_CLEANUP_INTERVAL) {
					//logging.Debugf("DDLServiceMgr: skipping delete token processing due to delayed processing. %v", entry.Path)
					continue
				}

				// MetakvDel failures are assumed to be rare and hence priortizing map cleanup
				// In case of error
				// a) If error was before metakv marked the token as deleted, next iteration of
				// cleanupDelCommand would find the same delete token and will process the request
				// b) If error is after metakv marked the token as deleted and during the phase of
				// sending response of delete operation to caller then metakv has deleted the token
				// and hence it will not appear in next iteration of cleanupDelCommand.
				// In such cases we will leak the command.DefnId entry in deleteTokenCache map.
				// Hence not worrying about the rare case of error on MetakvDel for token deletion
				// and priortizing map cleanup.
				delete(m.deleteTokenCache, command.DefnId)

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
// Build Token
//////////////////////////////////////////////////////////////

// Recover build index command
func (m *DDLServiceMgr) cleanupBuildCommand(provider *client.MetadataProvider) {
	if !m.canProcessDDL() {
		return
	}

	entries, err := metakv.ListAllChildren(mc.BuildDDLCommandTokenPath)
	if err != nil {
		logging.Warnf("DDLServiceMgr: Failed to cleanup build index token upon rebalancing.  Skip cleanup.  Internal Error = %v", err)
		return
	}

	if len(entries) == 0 {
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

			if common.IsServerlessDeployment() && command.Issuer == mc.INDEX_RESTORE {
				exist1, err1 := mc.StopScheduleCreateTokenExist(command.DefnId)
				exist2, err2 := mc.ScheduleCreateTokenExist(command.DefnId)
				exist3, err3 := mc.DeleteCommandTokenExist(command.DefnId)

				// Do not cleanup build token if there is error reading tokens
				if err1 != nil {
					logging.Warnf("DDLServiceMgr:cleanupBuildCommand Error when reading StopScheduleCreateToken for defn: %v. Err: %v", command.DefnId, err1)
					continue
				} else if err2 != nil {
					logging.Warnf("DDLServiceMgr:cleanupBuildCommand Error when reading ScheduleCreateToken for defn: %v. Err: %v", command.DefnId, err2)
					continue
				} else if err3 != nil {
					logging.Warnf("DDLServiceMgr:cleanupBuildCommand Error when reading DeleteCommandToken for defn: %v. Err: %v", command.DefnId, err3)
					continue
				}

				if exist1 {
					logging.Infof("DDLServiceMgr:cleanupBuildCommand StopScheduleCreateToken exists. Proceed to clean up restore build token %v.", entry.Path)
				} else if exist3 {
					logging.Infof("DDLServiceMgr:cleanupBuildCommand DeleteCommandToken exists. Proceed to clean up restore build token %v.", entry.Path)
				} else if exist2 {
					// Do not cleanup build token if schedule create token of restore operation exists
					continue
				}
			}

			//
			// At this point, metadata provider has been connected to all indexer at least once.
			// So it has a snapshot of the metadata from each indexer at some point in time.
			// It will return index even if metadata
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

			if !m.canProcessDDL() {
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

//////////////////////////////////////////////////////////////
// Create Token
//////////////////////////////////////////////////////////////

// cleanupCreateCommand deletes old create DDL command tokens from metakv. Unlike other DDL tokens,
// these are normally cleaned up as soon as processed, so this is only to catch any that "leaked"
// due to a failure in the normal processing.
func (m *DDLServiceMgr) cleanupCreateCommand(provider *client.MetadataProvider) map[common.IndexDefnId]map[uint64]*mc.CreateCommandToken {
	if !m.canProcessDDL() {
		return nil
	}

	// Get all virtual paths of create tokens from metakv. Since they are
	// big value tokens there are no values stored directly in these paths,
	// so entries[i].Path is all that is used here; entries[i].Value will be nil.
	entries, err := metakv.ListAllChildren(mc.CreateDDLCommandTokenPath)
	if err != nil {
		logging.Warnf("DDLServiceMgr: Failed to fetch token from metakv.  Internal Error = %v", err)
		return nil
	}

	allCreateCommandTokens := make(map[common.IndexDefnId]map[uint64]*mc.CreateCommandToken)

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

	addToAllCreateTokens := func(defnId common.IndexDefnId, requestId uint64, token *mc.CreateCommandToken) {
		if _, ok := allCreateCommandTokens[defnId]; !ok {
			allCreateCommandTokens[defnId] = make(map[uint64]*mc.CreateCommandToken)
		}
		allCreateCommandTokens[defnId][requestId] = token
	}

	removeFromAllCreateTokens := func(defnId common.IndexDefnId, requestId uint64) {
		delete(allCreateCommandTokens[defnId], requestId)
		if len(allCreateCommandTokens[defnId]) == 0 {
			delete(allCreateCommandTokens, defnId)
		}
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
			addToAllCreateTokens(defnId, requestId, nil) // since the token value can not be deduced, add a nil entry
			continue
		}
		addToAllCreateTokens(defnId, requestId, token)

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
					_, _, _, err := provider.FindServiceForIndexer(indexerId)
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
				removeFromAllCreateTokens(defnId, requestId)
				logging.Infof("DDLServiceMgr: Remove create index token %v.", entry.Path)
			}
			addDeleted(defnId, requestId)
		}
	}

	if len(malformed) != 0 {
		// wait for a second to ride out any race condition
		time.Sleep(time.Second)

		// make sure if all watchers are still alive
		if provider.AllWatchersAlive() {

			// Go through the list of tokens that have failed before.
			for defnId, requestIds := range malformed {
				for requestId, _ := range requestIds {

					// If already deleted, then ingore it.
					if isDeleted(defnId, requestId) {
						removeFromAllCreateTokens(defnId, requestId)
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
							removeFromAllCreateTokens(defnId, requestId)
							logging.Infof("DDLServiceMgr: Remove create index token %v.", defnId)
						}
						addDeleted(defnId, requestId)
					}
				}
			}
		}
	}

	return allCreateCommandTokens
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
	provider, _, _, err := newMetadataProvider(m.clusterAddr, nil, m.settings, "DDLServiceMgr:handleCreateCommand")
	if err != nil {
		logging.Errorf("DDLServiceMgr: Failed to start metadata provider.  Internal Error = %v", err)
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

					defn.SetCollectionDefaults()

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
						if defn.Deferred && (!found || (found &&
							status < common.INDEX_STATE_INITIAL && indexerId2 == m.indexerId)) &&
							index != nil && (index.State == common.INDEX_STATE_INITIAL ||
							index.State == common.INDEX_STATE_CATCHUP ||
							index.State == common.INDEX_STATE_ACTIVE) {
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
						defn.ScopeId = token.ScopeId
						defn.CollectionId = token.CollectionId

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
							logging.Warnf("DDLServiceMgr: Failed to process create index (%v, %v, %v, %v, %v, %v).  Error = %v.",
								defn.Bucket, defn.Scope, defn.Collection, defn.Name, defn.DefnId, defn.InstId, err)
						} else {
							logging.Infof("DDLServiceMgr: Index successfully created (%v, %v, %v, %v, %v, %v).",
								defn.Bucket, defn.Scope, defn.Collection, defn.Name, defn.DefnId, defn.InstId)
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
			// Do-not return. Continue with processing deleteMap
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

func createTokenExists(allCreateCommandTokens map[common.IndexDefnId]map[uint64]*mc.CreateCommandToken,
	command *mc.DropInstanceCommandToken) (uint64, bool) {

	// If there is a create token, then do not process the drop token.
	// Let the create token be deleted first to avoid any unexpected
	// race condition.  If the drop token is removed before create token
	// is removed, then the instance can get re-created again as the system
	// does not know that the instance has been dropped (since the token is
	// removed from metaKV)

	for requestId, token := range allCreateCommandTokens[command.DefnId] {
		// token will be nil if the value can not be fetched from metaKV.
		// In such a case, assume the presence of create token in metaKV
		if token == nil {
			return requestId, true
		} else {
			for _, defns := range token.Definitions {
				for _, defn := range defns {
					if defn.InstId == command.InstId {
						return requestId, true
					}
				}
			}
		}
	}
	return 0, false
}

// Recover drop instance command
func (m *DDLServiceMgr) cleanupDropInstanceCommand(provider *client.MetadataProvider,
	allCreateCommandTokens map[common.IndexDefnId]map[uint64]*mc.CreateCommandToken) {
	if !m.canProcessDDL() {
		return
	}

	entries, err := metakv.ListAllChildren(mc.DropInstanceDDLCommandTokenPath)
	if err != nil {
		logging.Warnf("DDLServiceMgr: Failed to cleanup delete index instance token upon rebalancing.  Skip cleanup.  Internal Error = %v", err)
		return
	}

	if len(entries) == 0 {
		return
	}

	for _, entry := range entries {

		if strings.Contains(entry.Path, mc.DropInstanceDDLCommandTokenPath) && entry.Value != nil {

			logging.Infof("DDLServiceMgr: processing delete index instance token %v", entry.Path)

			command, err := mc.GetDropInstanceTokenFromPath(entry.Path)
			if command == nil || err != nil {
				logging.Verbosef("DDLServiceMgr: delete index instance token for path %v does not exist, err: %v", entry.Path, err)
				continue
			}

			if requestId, exists := createTokenExists(allCreateCommandTokens, command); exists {
				logging.Warnf("DDLServiceMgr: Create token exist for %v, %v with requestId: %v.  Skip processing drop token %v.",
					command.DefnId, command.InstId, requestId, entry.Path)
				continue
			}

			// Find if the index still exist in the cluster.  DDLServiceManger will only cleanup the delete token IF there is no index instance.
			// This means the indexer must have been able to process the deleted token before DDLServiceManager has a chance to clean it up.
			//
			// 1) It will skip DELETED index.  DELETED index will be cleaned up by lifecycle manager periodically.
			// 2) At this point, metadata provider has been connected to all indexer at least once.
			//    So it has a snapshot of the metadata from each indexer at some point in time.
			//    It will return index even if metadata
			//    provider is not connected to the indexer at the exact moment when this call is made.
			//
			//
			if provider.FindIndexInstanceIgnoreStatus(command.DefnId, command.InstId) == nil {
				var defn common.IndexDefn
				defn.DefnId = command.DefnId
				defn.NumReplica2 = command.Defn.NumReplica2

				logging.Infof("DDLServiceMgr: Update Replica Count.  Index Defn %v replica Count %v", defn.DefnId, defn.NumReplica2)

				if err := provider.BroadcastAlterReplicaCountRequest(&defn); err != nil {
					// All errors received from alter replica count are expected to be recoverable.
					logging.Warnf("DDLServiceMgr: Failed to alter replica count. Error = %v.", err)
					continue
				}

				if !m.canProcessDDL() {
					return
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

// handleClusterStorageMode attempts to update cluster storage mode if necessary at the end of
// Rebalance. It tries once in the foreground but if that fails it launches a goroutine to keep
// retrying in the background, which will continue until it succeeds or a new rebalance calls this
// method again, causing it to close m.retryUpdateStorageModeDoneCh, ending the background retries.
func (m *DDLServiceMgr) handleClusterStorageMode(httpAddrMap map[string]string, provider *client.MetadataProvider) {

	// Avoid any possibility of collision with the next Rebalance
	m.handleClusterStorageModeMutex.Lock()
	defer m.handleClusterStorageModeMutex.Unlock()

	if m.retryUpdateStorageModeDoneCh != nil {
		close(m.retryUpdateStorageModeDoneCh)
		m.retryUpdateStorageModeDoneCh = nil
	}

	provider.RefreshIndexerVersion()
	if provider.GetIndexerVersion() != common.INDEXER_CUR_VERSION {
		return
	}

	storageMode := common.StorageMode(common.NOT_SET)
	initialized := false
	indexCount := 0

	indexes, _ := provider.ListIndex()
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
				logging.Errorf("DDLServiceMgr: unable to change storage mode to %v after rebalance.  Invalid storage type for index %v (%v, %v, %v, %v)",
					inst.StorageMode, index.Definition.Name, index.Definition.Bucket, index.Definition.Scope,
					index.Definition.Collection)
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
		storageMode = provider.GetStorageMode()
	}

	// if storage mode for all indexes converge, then change storage mode setting
	clusterStorageMode := common.GetClusterStorageMode()
	if storageMode != common.StorageMode(common.NOT_SET) && storageMode != clusterStorageMode {
		if !m.updateStorageMode(storageMode, httpAddrMap) {
			m.retryUpdateStorageModeDoneCh = make(chan bool)
			go m.retryUpdateStorageMode(storageMode, httpAddrMap, m.retryUpdateStorageModeDoneCh)
		}
	}
}

// retryUpdateStorageMode runs in a goroutine to retry updateStorageMode in the background if the
// foreground call made by handleClusterStorageMode failed.
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

// updateStorageMode attempts to update the cluster storage mode (done after Rebalance). It returns
// true on success, false on failure.
func (m *DDLServiceMgr) updateStorageMode(storageMode common.StorageMode, httpAddrMap map[string]string) bool {
	const method = "DDLServiceMgr::updateStorageMode:" // for logging

	clusterStorageMode := common.GetClusterStorageMode()
	if storageMode == clusterStorageMode {
		logging.Infof("%v All indexes have converged to cluster storage mode %v after rebalance.",
			method, storageMode)
		return true
	}

	settings := make(map[string]string)
	settings["indexer.settings.storage_mode"] = string(common.StorageModeToIndexType(storageMode))

	body, err := json.Marshal(&settings)
	if err != nil {
		logging.Errorf("%v Unable to change storage mode to %v after rebalance.  Error:%v",
			method, storageMode, err)
		return false
	}
	bodybuf := bytes.NewBuffer(body)

	for _, addr := range httpAddrMap {

		resp, err := postWithAuth(addr+"/internal/settings", "application/json", bodybuf)
		if err != nil {
			logging.Errorf("%v Encountered error when try to change setting.  Retry with another indexer node. Error:%v",
				method, err)
			continue
		}

		if resp != nil && resp.StatusCode != 200 {
			logging.Errorf("%v HTTP status (%v) when try to change setting.  Retry with another indexer node.",
				method, resp.Status)
			continue
		}

		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}

		logging.Infof("%v Cluster storage mode changed to %v after rebalance.",
			method, storageMode)
		return true
	}

	logging.Errorf("%v Unable to change storage mode to %v after rebalance.",
		method, storageMode)
	return false
}

//////////////////////////////////////////////////////////////
// REST
//////////////////////////////////////////////////////////////

func (m *DDLServiceMgr) handleListMetadataTokens(w http.ResponseWriter, r *http.Request) {

	creds, valid := m.validateAuth(w, r)
	if !valid {
		logging.Errorf("DDLServiceMgr::handleListMetadataTokens Validation Failure req: %v", common.GetHTTPReqInfo(r))
		return
	}

	if !isAllowed(creds, []string{"cluster.admin.internal.index!read"}, r, w, "DDLServiceMgr::handleListMetadataTokens:") {
		return
	}

	if r.Method == "GET" {

		logging.Infof("DDLServiceMgr::handleListMetadataTokens Processing Request req: %v", common.GetHTTPReqInfo(r))

		buildTokens, err := metakv.ListAllChildren(mc.BuildDDLCommandTokenPath)
		if err != nil {
			logging.Errorf("DDLServiceMgr::handleListMetadataTokens error %v in ListAllChildren for BuildDDLCommandTokenPath. req: %v", err, common.GetHTTPReqInfo(r))
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error() + "\n"))
			return
		}

		deleteTokens, err1 := metakv.ListAllChildren(mc.DeleteDDLCommandTokenPath)
		if err1 != nil {
			logging.Errorf("DDLServiceMgr::handleListMetadataTokens error %v in ListAllChildren for DeleteDDLCommandTokenPath. req: %v", err1, common.GetHTTPReqInfo(r))
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err1.Error() + "\n"))
			return
		}

		createTokens, err2 := mc.ListCreateCommandToken()
		if err2 != nil {
			logging.Errorf("DDLServiceMgr::handleListMetadataTokens error %v in ListCreateCommandToken. req: %v", err2, common.GetHTTPReqInfo(r))
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err2.Error() + "\n"))
			return
		}

		scheduleTokens, err3 := mc.ListAllScheduleCreateTokens()
		if err3 != nil {
			logging.Errorf("DDLServiceMgr::handleListMetadataTokens error %v in ListAllScheduleCreateTokens. req: %v", err3, common.GetHTTPReqInfo(r))
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err3.Error() + "\n"))
			return
		}

		stopScheduleTokens, err4 := mc.ListAllStopScheduleCreateTokens()
		if err4 != nil {
			logging.Errorf("DDLServiceMgr::handleListMetadataTokens error %v in ListAllStopScheduleCreateTokens req: %v", err4, common.GetHTTPReqInfo(r))
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err4.Error() + "\n"))
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
				logging.Errorf("DDLServiceMgr::handleListMetadataTokens error %v in GetDefnIdFromCreateCommandTokenPath for %v. req: %v", err, entry, common.GetHTTPReqInfo(r))
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error() + "\n"))
				return
			}

			token, err := mc.FetchCreateCommandToken(defnId, requestId)
			if err != nil {
				logging.Errorf("DDLServiceMgr::handleListMetadataTokens error %v in FetchCreateCommandToken for %v. req: %v", err, entry, common.GetHTTPReqInfo(r))
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error() + "\n"))
				return
			}

			buf, err := json.Marshal(token)
			if err != nil {
				logging.Errorf("DDLServiceMgr::handleListMetadataTokens error %v in json marshal for %v. req: %v", err, entry, common.GetHTTPReqInfo(r))
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error() + "\n"))
				return
			}

			w.Write([]byte(entry + " - "))
			w.Write(buf)
			w.Write([]byte("\n"))
		}

		for _, token := range scheduleTokens {
			if token == nil {
				continue
			}

			path := mc.GetScheduleCreateTokenPathFromDefnId(token.Definition.DefnId)
			buf, err := json.Marshal(token)
			if err != nil {
				logging.Errorf("DDLServiceMgr::handleListMetadataTokens error %v in json.Marshal after GetScheduleCreateTokenPathFromDefnId for %v. req: %v", err, token.Definition.DefnId, common.GetHTTPReqInfo(r))
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error() + "\n"))
				return
			}

			w.Write([]byte(path + " - "))
			w.Write(buf)
			w.Write([]byte("\n"))
		}

		for _, token := range stopScheduleTokens {
			if token == nil {
				continue
			}

			path := mc.GetStopScheduleCreateTokenPathFromDefnId(token.DefnId)

			buf, err := json.Marshal(token)
			if err != nil {
				logging.Errorf("DDLServiceMgr::handleListMetadataTokens error %v in json.Marshal after GetScheduleCreateTokenPathFromDefnId for %v. req: %v", err, token.DefnId, common.GetHTTPReqInfo(r))
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error() + "\n"))
				return
			}

			w.Write([]byte(path + " - "))
			w.Write(buf)
			w.Write([]byte("\n"))
		}

		w.WriteHeader(http.StatusOK)
	}
}

func (m *DDLServiceMgr) handleListCreateTokens(w http.ResponseWriter, r *http.Request) {

	creds, valid := m.validateAuth(w, r)
	if !valid {
		logging.Errorf("DDLServiceMgr::handleListCreateTokens Validation Failure req: %v", common.GetHTTPReqInfo(r))
		return
	}

	if !isAllowed(creds, []string{"cluster.admin.internal.index!read"}, r, w, "DDLServiceMgr::handleListCreateTokens:") {
		return
	}

	if r.Method == "GET" {

		logging.Infof("DDLServiceMgr::handleListCreateTokens Processing Request req: %v", common.GetHTTPReqInfo(r))

		createTokens, err := mc.ListCreateCommandToken()
		if err != nil {
			logging.Errorf("DDLServiceMgr::handleListCreateTokens error %v in ListCreateCommandToken. req: %v", err, common.GetHTTPReqInfo(r))
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error() + "\n"))
			return
		}

		list := &mc.CreateCommandTokenList{}

		for _, entry := range createTokens {

			defnId, requestId, err := mc.GetDefnIdFromCreateCommandTokenPath(entry)
			if err != nil {
				logging.Errorf("DDLServiceMgr::handleListCreateTokens error %v in GetDefnIdFromCreateCommandTokenPath for entry %v. req: %v", err, entry, common.GetHTTPReqInfo(r))
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(err.Error() + "\n"))
				continue
			}

			//
			// Use retry helper for fetching create command token.
			// Metakv consistency model may not return a complete set of children
			// on ListAllChildren, if all the children are not available at the
			// time of calling. So, wait for sometime and retry.
			//
			// mc.FetchCreateCommandToken is a complete function in itself which
			// internally gets all the sub-paths and consolidates and unmarshals
			// the create command token. In case of half-baked token, internal
			// json unmarshal will return error.
			//
			var token *mc.CreateCommandToken

			fn := func(retryAttempt int, lastErr error) error {
				var err error

				token, err = mc.FetchCreateCommandToken(defnId, requestId)
				if err != nil {
					logging.Errorf("DDLServiceMgr::handleListCreateTokens error %v in FetchCreateCommandToken for entry %v. req: %v", err, entry, common.GetHTTPReqInfo(r))
				}

				return err
			}

			rh := common.NewRetryHelper(8, 2*time.Second, 1, fn)
			err = rh.Run()

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
			logging.Errorf("DDLServiceMgr::handleListCreateTokens error %v in MarshallCreateCommandTokenList. req: %v", err, common.GetHTTPReqInfo(r))
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

	creds, ok := m.validateAuth(w, r)
	if !ok {
		logging.Errorf("DDLServiceMgr::handleListDeleteTokens Validation Failure req: %v", common.GetHTTPReqInfo(r))
		return
	}

	if !isAllowed(creds, []string{"cluster.admin.internal.index!read"}, r, w, "DDLServiceMgr::handleListDeleteTokens:") {
		return
	}

	if r.Method == "GET" {

		logging.Infof("DDLServiceMgr::handleListDeleteTokens Processing Request req: %v", common.GetHTTPReqInfo(r))

		deleteTokens, err := mc.ListDeleteCommandToken()
		if err != nil {
			logging.Errorf("DDLServiceMgr::handleListDeleteTokens error %v in ListDeleteCommandToken. req: %v", err, common.GetHTTPReqInfo(r))
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
			logging.Errorf("DDLServiceMgr::handleListDeleteTokens error %v in MarshallDeleteCommandTokenList. req: %v", err, common.GetHTTPReqInfo(r))
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error() + "\n"))
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write(buf)
	}
}

// handleListGenericTokenPaths is a helper for all handlers that need only a
// list of token paths rather than the tokens themselves. callerName is used
// for logging. listerFunc is the function in token.go to get the path list
// from metakv for the specific token type desired.
func (m *DDLServiceMgr) handleListGenericTokenPaths(w http.ResponseWriter,
	r *http.Request, callerName string, listerFunc func() ([]string, error)) {

	creds, ok := m.validateAuth(w, r)
	if !ok {
		logging.Errorf("DDLServiceMgr::handleListGenericTokenPaths Validation Failure caller: %v, req: %v", callerName, common.GetHTTPReqInfo(r))
		return
	}

	if !isAllowed(creds, []string{"cluster.admin.internal.index!read"}, r, w, "DDLServiceMgr::handleListGenericTokenPaths:") {
		return
	}

	if r.Method == "GET" {

		logging.Infof("DDLServiceMgr::handleListGenericTokenPaths Processing Request caller: %v, req: %v", callerName, common.GetHTTPReqInfo(r))

		tokenPaths, err := listerFunc()
		if err != nil {
			logging.Errorf("DDLServiceMgr::handleListGenericTokenPaths caller %v, error %v in listerFunc. req: %v", callerName, err, common.GetHTTPReqInfo(r))
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error() + "\n"))
			return
		}

		list := &mc.TokenPathList{}
		list.Paths = make([]string, 0, len(tokenPaths))

		for _, path := range tokenPaths {
			list.Paths = append(list.Paths, path)
		}

		buf, err := mc.MarshallTokenPathList(list)
		if err != nil {
			logging.Errorf("DDLServiceMgr::handleListGenericTokenPaths caller %v, error %v in MarshallTokenPathList. req: %v", callerName, err, common.GetHTTPReqInfo(r))
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error() + "\n"))
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write(buf)
	}
}

// handleListDeleteTokenPaths responds to /listDeleteTokenPaths REST endpoint with
// a list of the paths (keys) of all delete tokens in metakv on this indexer host.
func (m *DDLServiceMgr) handleListDeleteTokenPaths(w http.ResponseWriter, r *http.Request) {

	m.handleListGenericTokenPaths(w, r,
		"handleListDeleteTokenPaths", mc.ListDeleteCommandTokenPaths)
}

// handleListDropInstanceTokens responds to /listDropInstanceTokens REST endpoint
// with a list of all drop instance tokens in metakv on this indexer host.
func (m *DDLServiceMgr) handleListDropInstanceTokens(w http.ResponseWriter, r *http.Request) {

	creds, valid := m.validateAuth(w, r)
	if !valid {
		logging.Errorf("DDLServiceMgr::handleListDropInstanceTokens Validation Failure req: %v", common.GetHTTPReqInfo(r))
		return
	}

	if !isAllowed(creds, []string{"cluster.admin.internal.index!read"}, r, w, "DDLServiceMgr::handleListDropInstanceTokens:") {
		return
	}

	if r.Method == "GET" {

		logging.Infof("DDLServiceMgr::handleListDropInstanceTokens Processing Request req: %v", common.GetHTTPReqInfo(r))

		numRetries := 8
		deleteTokens, err := mc.ListAndFetchAllDropInstanceCommandToken(numRetries)
		if err != nil {
			logging.Errorf("DDLServiceMgr::handleListDropInstanceTokens Error %v in ListAndFetchAllDropInstanceCommandToken. req: %v", err, common.GetHTTPReqInfo(r))
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
			logging.Errorf("DDLServiceMgr::handleListDropInstanceTokens Error %v in MarshallDropInstanceCommandTokenList. req: %v", err, common.GetHTTPReqInfo(r))
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error() + "\n"))
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write(buf)
	}
}

// handleListDropInstanceTokenPaths responds to /listDropInstanceTokenPaths REST endpoint
// with a list of the paths (keys) of all drop instance tokens in metakv on this indexer host.
func (m *DDLServiceMgr) handleListDropInstanceTokenPaths(w http.ResponseWriter, r *http.Request) {
	m.handleListGenericTokenPaths(w, r,
		"handleListDropInstanceTokenPaths", mc.ListDropInstanceCommandTokenPaths)
}

func (m *DDLServiceMgr) handleListScheduleCreateTokens(w http.ResponseWriter, r *http.Request) {

	creds, valid := m.validateAuth(w, r)
	if !valid {
		logging.Errorf("DDLServiceMgr::handleListScheduleCreateTokens Validation Failure req: %v", common.GetHTTPReqInfo(r))
		return
	}

	if !isAllowed(creds, []string{"cluster.admin.internal.index!read"}, r, w, "DDLServiceMgr::handleListScheduleCreateTokens:") {
		return
	}

	if r.Method == "GET" {

		logging.Infof("DDLServiceMgr::handleListScheduleCreateTokens Processing Request req: %v", common.GetHTTPReqInfo(r))

		scheduleTokens, err := mc.ListAllScheduleCreateTokens()
		if err != nil {
			logging.Errorf("DDLServiceMgr::handleListScheduleCreateTokens Error %v in ListAllScheduleCreateTokens. req: %v", err, common.GetHTTPReqInfo(r))
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error() + "\n"))
			return
		}

		list := &mc.ScheduleCreateTokenList{}
		list.Tokens = make([]mc.ScheduleCreateToken, 0, len(scheduleTokens))

		for _, token := range scheduleTokens {
			list.Tokens = append(list.Tokens, *token)
		}

		buf, err := mc.MarshallScheduleCreateTokenList(list)
		if err != nil {
			logging.Errorf("DDLServiceMgr::handleListScheduleCreateTokens Error %v in MarshallScheduleCreateTokenList. req: %v", err, common.GetHTTPReqInfo(r))
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error() + "\n"))
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write(buf)
	}
}

func (m *DDLServiceMgr) handleListStopScheduleCreateTokens(w http.ResponseWriter, r *http.Request) {

	creds, ok := m.validateAuth(w, r)
	if !ok {
		logging.Errorf("DDLServiceMgr::handleListStopScheduleCreateTokens Validation Failure req: %v", common.GetHTTPReqInfo(r))
		return
	}

	if !isAllowed(creds, []string{"cluster.admin.internal.index!read"}, r, w, "DDLServiceMgr::handleListStopScheduleCreateTokens:") {
		return
	}

	if r.Method == "GET" {

		logging.Infof("DDLServiceMgr::handleListStopScheduleCreateTokens Processing Request req: %v", common.GetHTTPReqInfo(r))

		scheduleTokens, err := mc.ListAllStopScheduleCreateTokens()
		if err != nil {
			logging.Errorf("DDLServiceMgr::handleListStopScheduleCreateTokens Error %v in ListAllStopScheduleCreateTokens. req: %v", err, common.GetHTTPReqInfo(r))
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error() + "\n"))
			return
		}

		list := &mc.StopScheduleCreateTokenList{}
		list.Tokens = make([]mc.StopScheduleCreateToken, 0, len(scheduleTokens))

		for _, token := range scheduleTokens {
			list.Tokens = append(list.Tokens, *token)
		}

		buf, err := mc.MarshallStopScheduleCreateTokenList(list)
		if err != nil {
			logging.Errorf("DDLServiceMgr::handleListStopScheduleCreateTokens Error %v in MarshallStopScheduleCreateTokenList. req: %v", err, common.GetHTTPReqInfo(r))
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error() + "\n"))
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write(buf)
	}
}

func (m *DDLServiceMgr) handleTransferScheduleCreateTokens(w http.ResponseWriter, r *http.Request) {
	creds, valid := m.validateAuth(w, r)
	if !valid {
		logging.Errorf("DDLServiceMgr::handleTransferScheduleCreateTokens Validation Failure req: %v", common.GetHTTPReqInfo(r))
		return
	}

	if !isAllowed(creds, []string{"cluster.admin.internal.index!write"}, r, w, "DDLServiceMgr::handleTransferScheduleCreateTokens:") {
		return
	}

	if r.Method == "POST" {
		bytes, _ := ioutil.ReadAll(r.Body)
		tokens := make([]mc.ScheduleCreateToken, 0)
		if err := json.Unmarshal(bytes, &tokens); err != nil {
			logging.Errorf("DDLServiceMgr::handleTransferScheduleCreateTokens error in json.Unmarshal %v", err)
			send(http.StatusBadRequest, w, err)
			return
		}

		if err := m.transferScheduleCreateTokens(tokens); err != nil {
			errStr := fmt.Sprintf("Error while transferring scheduled create tokens %v", err)
			send(http.StatusInternalServerError, w, errStr)
		} else {
			send(http.StatusOK, w, "")
		}

	} else {
		send(http.StatusBadRequest, w, fmt.Errorf("Unsupported Method"))
		return
	}
}

func (m *DDLServiceMgr) transferScheduleCreateTokens(tokens []mc.ScheduleCreateToken) error {

	for _, token := range tokens {
		logging.Infof("transferScheduleCreateTokens:: DefnId %v, old indexer %v, new indexer %v",
			token.Definition.DefnId, token.IndexerId, m.indexerId)
		token.IndexerId = m.indexerId

		// Reset the nodes param for the token
		resetNodesParam(&token)

		err := mc.UpdateScheduleCreateToken(&token)
		if err != nil {
			return fmt.Errorf("Error (%v) in UpdateScheduleCreateToken for %v", err, token.Definition.DefnId)
		}
	}

	return nil
}

func (m *DDLServiceMgr) validateAuth(w http.ResponseWriter, r *http.Request) (cbauth.Creds, bool) {
	creds, valid, err := common.IsAuthValid(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error() + "\n"))
	} else if valid == false {
		audit.Audit(common.AUDIT_UNAUTHORIZED, r, "DDLServiceMgr::validateAuth", "")
		w.WriteHeader(http.StatusUnauthorized)
		w.Write(common.HTTP_STATUS_UNAUTHORIZED)
	}
	return creds, valid
}

func (m *DDLServiceMgr) startCommandListner() {
	donech := make(chan bool)
	m.commandListener = mc.NewCommandListener(donech, true, true, true, false, false, false)
	m.listenerDonech = donech
}

//////////////////////////////////////////////////////////////
// Metadata Provider
//////////////////////////////////////////////////////////////

func newMetadataProvider(clusterAddr string, nodes map[service.NodeID]bool, settings *ddlSettings,
	logPrefix string) (*client.MetadataProvider, map[string]string, bool, error) {

	// initialize ClusterInfoCache
	url, err := common.ClusterAuthUrl(clusterAddr)
	if err != nil {
		return nil, nil, false, err
	}

	cinfo, err := common.NewClusterInfoCache(url, DEFAULT_POOL)
	if err != nil {
		return nil, nil, false, err
	}
	cinfo.SetUserAgent(fmt.Sprintf("newMetadataProvider:%v", logPrefix))

	if err := cinfo.FetchNodesAndSvsInfo(); err != nil {
		return nil, nil, false, err
	}

	hasFailedNodes := false
	failedNodes := cinfo.GetFailedIndexerNodes()
	if len(failedNodes) >= 1 {
		hasFailedNodes = true
		nodeIds := []string{}
		for _, n := range failedNodes {
			nodeIds = append(nodeIds, n.NodeUUID)
		}
		logging.Infof("%v found failed nodes %v", logPrefix, nodeIds)
	}

	adminAddrMap := make(map[string]string)
	httpAddrMap := make(map[string]string)

	// If a node list is given, then honor the node list by verifying that it can reach
	// to every node in the list.
	if nodes != nil {
		// Discover indexer service from ClusterInfoCache
		nids := cinfo.GetNodeIdsByServiceType(common.INDEX_HTTP_SERVICE)
		for _, nid := range nids {

			addr, err := cinfo.GetServiceAddress(nid, common.INDEX_HTTP_SERVICE, true)
			if err == nil {

				resp, err := getWithAuth(addr + "/getLocalIndexMetadata?useETag=false")
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
						return nil, nil, hasFailedNodes, err
					}

					adminAddrMap[localMeta.NodeUUID] = adminAddr
					delete(nodes, service.NodeID(localMeta.NodeUUID))
				}
			}
		}

		if len(nodes) != 0 {
			return nil, nil, hasFailedNodes, errors.New(
				fmt.Sprintf("%v: Failed to initialize metadata provider.  Unknown host=%v", logPrefix, nodes))
		}
	} else {
		// Find all nodes that has a index http service
		// 1) This method will exclude inactive_failed node in the cluster.  But if a node failed after the topology is fetched, then
		//    metadata provider could eventually fail (if cannot connect to indexer service).  Note that ns-server will shutdown indexer
		//    service due to failed over.
		// 2) This method will exclude inactive_new node in the cluster.
		// 3) This may include unhealthy node since unhealthiness is not a cluster membership state (need verification).  If it
		//    metadata provider cannot reach the unhealthy node, the metadata provider may not start (expected behavior).
		nids := cinfo.GetNodeIdsByServiceType(common.INDEX_HTTP_SERVICE)

		for _, nid := range nids {
			adminAddr, err := cinfo.GetServiceAddress(nid, common.INDEX_ADMIN_SERVICE, true)
			if err != nil {
				return nil, nil, hasFailedNodes, err
			}
			nodeUUID := cinfo.GetNodeUUID(nid)
			adminAddrMap[nodeUUID] = adminAddr
		}
	}

	// initialize a new MetadataProvider
	ustr, err := common.NewUUID()
	if err != nil {
		return nil, nil, hasFailedNodes, errors.New(fmt.Sprintf("%v: Failed to initialize metadata provider.  Internal Error = %v", logPrefix, err))
	}
	providerId := ustr.Str()

	provider, err := client.NewMetadataProvider(clusterAddr, providerId, nil, nil, settings)
	if err != nil {
		if provider != nil {
			provider.Close()
		}
		return nil, nil, hasFailedNodes, err
	}

	// Watch Metadata
	for _, addr := range adminAddrMap {
		logging.Infof("%v: connecting to node %v", logPrefix, addr)
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
				return provider, httpAddrMap, hasFailedNodes, nil
			}

			if retry == 0 {
				for nodeUUID, adminport := range adminAddrMap {
					if !provider.IsWatcherAlive(nodeUUID) {
						logging.Warnf("%v: cannot connect to node %v", logPrefix, adminport)
					}
				}

				provider.Close()
				return nil, nil, hasFailedNodes, errors.New(fmt.Sprintf("%v: Failed to initialize metadata provider.  "+
					"%v within 30 seconds.", logPrefix, common.ErrIndexerConnection.Error()))
			}
		}
	}

	return provider, httpAddrMap, hasFailedNodes, nil
}

//////////////////////////////////////////////////////////////
// Settings
//////////////////////////////////////////////////////////////

func (s *ddlSettings) NumReplica() int32 {
	return atomic.LoadInt32(&s.numReplica)
}

func (s *ddlSettings) DeferBuild() bool {
	return s.deferBuild.Load()
}

func (s *ddlSettings) NumPartition() int32 {
	return atomic.LoadInt32(&s.numPartition)
}

func (s *ddlSettings) MaxNumPartition() int32 {
	return atomic.LoadInt32(&s.maxNumPartition)
}

func (s *ddlSettings) StorageMode() string {
	s.storageModeMutex.RLock()
	defer s.storageModeMutex.RUnlock()

	return s.storageMode
}

func (s *ddlSettings) UsePlanner() bool {
	return true
}

func (s *ddlSettings) AllowPartialQuorum() bool {
	return atomic.LoadUint32(&s.allowPartialQuorum) == 1
}

func (s *ddlSettings) AllowScheduleCreate() bool {
	return false
}

func (s *ddlSettings) AllowScheduleCreateRebal() bool {
	return false
}

func (s *ddlSettings) WaitForScheduledIndex() bool {
	return false
}

func (s *ddlSettings) UseGreedyPlanner() bool {
	return atomic.LoadUint32(&s.useGreedyPlanner) == 1
}

func (s *ddlSettings) MemHighThreshold() int32 {
	return atomic.LoadInt32(&s.memHighThreshold)
}

func (s *ddlSettings) MemLowThreshold() int32 {
	return atomic.LoadInt32(&s.memLowThreshold)
}

func (s *ddlSettings) ServerlessIndexLimit() uint32 {
	return atomic.LoadUint32(&s.indexLimit)
}

func (s *ddlSettings) IsShardAffinityEnabled() bool {
	return atomic.LoadUint32(&s.isShardAffinityEnabled) == 1
}

func (s *ddlSettings) GetBinSize() uint64 {
	return atomic.LoadUint64(&s.binSize)
}

func (s *ddlSettings) AllowDDLDuringScaleUp() bool {
	return atomic.LoadUint32(&s.allowDDLDuringScaleUp) == 1
}

func (s *ddlSettings) ShouldHonourNodesClause() bool {
	return atomic.LoadUint32(&s.allowNodesClause) == 1
}

func (s *ddlSettings) handleSettings(config common.Config) {

	numReplica := int32(config["settings.num_replica"].Int())
	if numReplica >= 0 {
		atomic.StoreInt32(&s.numReplica, numReplica)
	} else {
		logging.Errorf("DDLServiceMgr: invalid setting value for num_replica=%v", numReplica)
	}

	deferBuild := bool(config["settings.defer_build"].Bool())
	s.deferBuild.Store(deferBuild)

	maxNumPartition := int32(config["settings.maxNumPartitions"].Int())
	if maxNumPartition > 0 {
		atomic.StoreInt32(&s.maxNumPartition, maxNumPartition)
	} else {
		logging.Errorf("DDLServiceMgr: invalid setting value for maxNumPartition=%v", maxNumPartition)
	}

	numPartition := int32(config["numPartitions"].Int())
	if numPartition > 0 {
		if numPartition <= s.MaxNumPartition() {
			atomic.StoreInt32(&s.numPartition, numPartition)
		} else if numPartition > s.MaxNumPartition() {
			//handle case when maxNumPartition is decreased but numPartition remains unchanged
			atomic.StoreInt32(&s.numPartition, s.MaxNumPartition())
			logging.Infof("ClientSettings: use value of maxNumPartitions=%v to reset old numPartitions=%v", s.MaxNumPartition(), numPartition)
		} else {
			logging.Errorf("ClientSettings: invalid setting value for numPartitions=%v, maxNumPartitions=%v", numPartition, s.MaxNumPartition())
		}
	} else {
		logging.Errorf("DDLServiceMgr: invalid setting value for numPartitions=%v", numPartition)
	}

	storageMode := config["settings.storage_mode"].String()
	if len(storageMode) != 0 {
		func() {
			s.storageModeMutex.Lock()
			defer s.storageModeMutex.Unlock()
			s.storageMode = storageMode
		}()
	}

	allowPartialQuorum, ok := config["allowPartialQuorum"]
	if ok {
		if allowPartialQuorum.Bool() {
			atomic.StoreUint32(&s.allowPartialQuorum, 1)
		} else {
			atomic.StoreUint32(&s.allowPartialQuorum, 0)
		}
	} else {
		// Use default config value on error
		logging.Errorf("DDLServiceMgr: missing indexer.allowPartialQuorum, setting to false")
		atomic.StoreUint32(&s.allowPartialQuorum, 0)
	}

	useGreedyPlanner, ok := config["planner.useGreedyPlanner"]
	if ok {
		if useGreedyPlanner.Bool() {
			atomic.StoreUint32(&s.useGreedyPlanner, 1)
		} else {
			atomic.StoreUint32(&s.useGreedyPlanner, 0)
		}
	} else {
		// Use default config value on error
		logging.Errorf("DDLServiceMgr: missing indexer.planner.useGreedyPlanner, setting to true")
		atomic.StoreUint32(&s.useGreedyPlanner, 1)
	}

	isShardAffinityEnabled := config.GetDeploymentAwareShardAffinity()
	if isShardAffinityEnabled {
		atomic.StoreUint32(&s.isShardAffinityEnabled, 1)
	} else {
		atomic.StoreUint32(&s.isShardAffinityEnabled, 0)
	}

	binSize := common.GetBinSize(config)
	atomic.StoreUint64(&s.binSize, binSize)

	memHighThreshold := int32(config["settings.thresholds.mem_high"].Int())
	if memHighThreshold >= 0 {
		atomic.StoreInt32(&s.memHighThreshold, memHighThreshold)
	} else {
		logging.Errorf("DDLServiceMgr: invalid setting value for mem_high = %v", memHighThreshold)
	}

	memLowThreshold := int32(config["settings.thresholds.mem_low"].Int())
	if memLowThreshold >= 0 {
		atomic.StoreInt32(&s.memLowThreshold, memLowThreshold)
	} else {
		logging.Errorf("DDLServiceMgr: invalid setting value for mem_low = %v", memLowThreshold)
	}

	indexLimit := uint32(config["settings.serverless.indexLimit"].Int())
	if indexLimit >= 0 {
		atomic.StoreUint32(&s.indexLimit, indexLimit)
	} else {
		logging.Errorf("DDLServiceMgr: invalid setting value for indexLimit = %v", indexLimit)
	}

	allowDDLDuringScaleUp, ok := config["allow_ddl_during_scaleup"]
	if ok {
		if allowDDLDuringScaleUp.Bool() {
			atomic.StoreUint32(&s.allowDDLDuringScaleUp, 1)
		} else {
			atomic.StoreUint32(&s.allowDDLDuringScaleUp, 0)
		}
	} else {
		// Use default config value on error
		logging.Errorf("DDLServiceMgr: missing indexer.allow_ddl_during_scaleup, setting to false")
		atomic.StoreUint32(&s.allowDDLDuringScaleUp, 0)
	}

	allowNodesClause, ok := config["planner.honourNodesInDefn"]
	if ok {
		if allowNodesClause.Bool() {
			atomic.StoreUint32(&s.allowNodesClause, 1)
		} else {
			atomic.StoreUint32(&s.allowNodesClause, 0)
		}
	} else {
		// Use default config value on error
		logging.Errorf("DDLServiceMgr: missing indexer.planner.honourNodesInDefn, setting to false")
		atomic.StoreUint32(&s.allowNodesClause, 0)
	}
}

// Utilityfunctions used for trasferring scheduled create tokens.
func transferScheduleTokens(keepNodes map[string]bool, clusterAddr string) error {

	//
	// transferScheduleTokens gets called on topology change triggered either
	// due to rebalance or due to failover. So, nodes param specified by user
	// may get invalidated.
	//
	// So, need to reset all the with nodes values from all the tokens.
	//

	if len(keepNodes) <= 0 {
		// Nothing to do.
		return nil
	}

	//
	// To avoid multiple updates to a single token, transfer tokens will be
	// undergo reset of nodes param during token transfer. On the other hand,
	// the tokens which do not need any transfer, will undergo reset of nodes
	// param before token transfer begins.
	//

	getTokensToUpdate := func() ([]*mc.ScheduleCreateToken, []*mc.ScheduleCreateToken, error) {
		tokensToTransfer := make([]*mc.ScheduleCreateToken, 0)
		tokensWithNodes := make([]*mc.ScheduleCreateToken, 0)

		tokens, err := mc.ListAllScheduleCreateTokens()
		if err != nil {
			return nil, nil, err
		}

		for _, token := range tokens {
			if _, ok := keepNodes[string(token.IndexerId)]; !ok {
				tokensToTransfer = append(tokensToTransfer, token)
				continue
			}

			if len(token.Definition.Nodes) != 0 {
				tokensWithNodes = append(tokensWithNodes, token)
				continue
			}

			if token.Plan != nil {
				if _, ok := token.Plan["nodes"]; ok {
					tokensWithNodes = append(tokensWithNodes, token)
					continue
				}
			}
		}

		return tokensToTransfer, tokensWithNodes, nil
	}

	// TODO: Progress update for rebalace?

	transTokens, nodesTokens, err := getTokensToUpdate()
	if err != nil {
		logging.Errorf("transferScheduleTokens: Error in getTokensToTransfer %v", err)
		return err
	}

	if len(transTokens) <= 0 && len(nodesTokens) == 0 {
		return nil
	}

	// Reset the nodes param from the tokens
	for _, token := range nodesTokens {
		resetNodesParam(token)
		err := mc.UpdateScheduleCreateToken(token)
		if err != nil {
			// Log error. Don't return the error just now. Let the overall transfer finish.
			logging.Errorf("transferScheduleTokens: Error in UpdateScheduleCreateToken %v", err)
		}
	}

	if len(transTokens) == 0 {
		return nil
	}

	// Transfer the tokens.

	getTransferMap := func(tokens []*mc.ScheduleCreateToken) map[string][]*mc.ScheduleCreateToken {

		// Use Round Robin

		// Note that the list of nodes provided by the user in with nodes
		// clause is ignored here as - even for alreeady created indexes,
		// that clause is not enforced during rebalance.
		transferMap := make(map[string][]*mc.ScheduleCreateToken)
		numNodes := len(keepNodes)

		targetNodes := make([]string, 0, numNodes)
		for node, _ := range keepNodes {
			targetNodes = append(targetNodes, node)
		}

		for i, token := range tokens {
			nodeId := string(targetNodes[i%numNodes])
			if _, ok := transferMap[nodeId]; !ok {
				transferMap[nodeId] = make([]*mc.ScheduleCreateToken, 0)
			}

			transferMap[nodeId] = append(transferMap[nodeId], token)
		}

		return transferMap
	}

	transferMap := getTransferMap(transTokens)
	if len(transferMap) == 0 {
		return nil
	}

	if err := postSchedTransferMap(transferMap, clusterAddr); err != nil {
		logging.Errorf("transferScheduleTokens: Error in postTransferMap %v", err)
		return err
	}

	if err := verifySchedTransfer(transferMap); err != nil {
		// No need to fail the entire successful rebalance operation just
		// becasue the verification has failed. The verification can fail due
		// to the consistency model of token store.
		logging.Warnf("transferScheduleTokens: failed verification of the transfer "+
			"with error %v. The operation may have been successful. Please check later.", err)
		return err
	}

	return nil
}

func postSchedTransferMap(transferMap map[string][]*mc.ScheduleCreateToken, clusterAddr string) error {
	cinfo, err := common.FetchNewClusterInfoCache(clusterAddr, common.DEFAULT_POOL, "transferScheduleTokens")
	if err != nil {
		return fmt.Errorf("postSchedTransferMap: Error Fetching Cluster Information %v", err)
	}

	var wg sync.WaitGroup
	var mu sync.RWMutex
	errMap := make(map[string]error)

	postTokens := func(nodeUUID string, tokens []*mc.ScheduleCreateToken) {

		defer wg.Done()

		err := func() error {
			nid, found := cinfo.GetNodeIdByUUID(nodeUUID)
			if !found {
				return fmt.Errorf("node with uuiid %v not found in cluster info cache", nodeUUID)
			}

			addr, err := cinfo.GetServiceAddress(nid, common.INDEX_HTTP_SERVICE, true)
			if err != nil {
				return fmt.Errorf("error in GetServiceAddress %v", err)
			}

			var body []byte
			body, err = json.Marshal(&tokens)
			if err != nil {
				return fmt.Errorf("error in json.Marshal %v", err)
			}

			bodybuf := bytes.NewBuffer(body)

			resp, err := postWithAuth(addr+"/transferScheduleCreateTokens", "application/json", bodybuf)
			if err != nil {
				return fmt.Errorf("error in postWithAuth %v", err)
			}

			if resp != nil && resp.StatusCode != 200 {
				return fmt.Errorf("HTTP status (%v)", resp.Status)
			}

			logging.Infof("postSchedTransferMap: Posted %v tokens to node %v", len(tokens), addr)
			return nil
		}()

		if err != nil {
			func() {
				mu.Lock()
				defer mu.Unlock()

				errMap[nodeUUID] = err
			}()
		}
	}

	for nodeUUID, tokens := range transferMap {
		wg.Add(1)
		go postTokens(nodeUUID, tokens)
	}

	wg.Wait()

	mu.RLock()
	defer mu.RUnlock()

	if len(errMap) != 0 {
		logging.Errorf("postSchedTransferMap: error map %v", errMap)

		// return any one error
		for _, err := range errMap {
			return err
		}
	}

	return nil
}

func verifySchedTransfer(transferMap map[string][]*mc.ScheduleCreateToken) error {
	// TODO: This can be done in a loop - for limited number of iterations.

	defns := make([]string, 0)

	time.Sleep(5 * time.Second)
	for nodeUUID, tokens := range transferMap {
		for _, t := range tokens {
			token, err := mc.GetScheduleCreateToken(t.Definition.DefnId)
			if err != nil {
				return err
			}

			if token == nil {
				logging.Warnf("verifySchedTransfer: Nil token is observed for %v."+
					" Index may have got dropped.", t.Definition.DefnId)
				defns = append(defns, fmt.Sprintf("%v", t.Definition.DefnId))
				continue
			}

			if nodeUUID != string(token.IndexerId) {
				defns = append(defns, fmt.Sprintf("%v", t.Definition.DefnId))
			}
		}
	}

	if len(defns) != 0 {
		return fmt.Errorf("Transfer verification failed for %v", strings.Join(defns, ", "))
	}

	return nil
}

func resetNodesParam(token *mc.ScheduleCreateToken) {
	// Update index Definition
	token.Definition.Nodes = nil

	// Update the plan
	if _, ok := token.Plan["nodes"]; ok {
		delete(token.Plan, "nodes")
	}
}
