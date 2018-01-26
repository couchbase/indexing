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
func NewDDLServiceMgr(supvCmdch MsgChannel, supvMsgch MsgChannel, config common.Config) (*DDLServiceMgr, Message) {

	addr := config["clusterAddr"].String()
	port := config["httpPort"].String()
	host, _, _ := net.SplitHostPort(addr)
	localaddr := net.JoinHostPort(host, port)

	nodeId := service.NodeID(config["nodeuuid"].String())

	numReplica := int32(config["settings.num_replica"].Int())
	settings := &ddlSettings{numReplica: numReplica}

	mgr := &DDLServiceMgr{
		supvCmdch:   supvCmdch,
		supvMsgch:   supvMsgch,
		localAddr:   localaddr,
		clusterAddr: addr,
		nodeID:      nodeId,
		settings:    settings,
		donech:      nil,
	}

	mgr.config.Store(config)

	http.HandleFunc("/listMetadataTokens", mgr.handleListMetadataTokens)

	gDDLServiceMgrLck.Lock()
	defer gDDLServiceMgrLck.Unlock()
	gDDLServiceMgr = mgr

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
// Recovery
//////////////////////////////////////////////////////////////

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
		if m.provider != nil {
			m.provider.Close()
			m.provider = nil
		}
	}()

	// Refresh metadata provider on topology change
	httpAddrMap, err := m.refreshOnTopologyChange(change, isCancel)
	if err != nil {
		logging.Warnf("DDLServiceMgr: Fail to clean delete index token upon rebalancing.  Skip Cleanup. Internal Error = %v", err)
		return
	}

	m.handleDropCommand()
	m.handleBuildCommand()
	m.handleClusterStorageMode(httpAddrMap)
}

//
// Recover drop index command
//
func (m *DDLServiceMgr) handleDropCommand() {

	entries, err := metakv.ListAllChildren(mc.DeleteDDLCommandTokenPath)
	if err != nil {
		logging.Warnf("DDLServiceMgr: Fail to cleanup delete index token upon rebalancing.  Skip cleanup.  Internal Error = %v", err)
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
				logging.Warnf("DDLServiceMgr: Fail to clean delete index token upon rebalancing.  Skp command %v.  Internal Error = %v.", entry.Path, err)
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
			if m.provider.FindIndexIgnoreStatus(command.DefnId) == nil {
				// There is no index in the cluster,  remove token
				if err := MetakvDel(entry.Path); err != nil {
					logging.Warnf("DDLServiceMgr: Fail to remove delete index token %v. Error = %v", entry.Path, err)
				} else {
					logging.Infof("DDLServiceMgr: Remove delete index token %v.", entry.Path)
				}
			} else {
				logging.Infof("DDLServiceMgr: Indexer still holding index definiton.  Skip removing delete index token %v.", entry.Path)
			}
		}
	}
}

//
// Recover build index command
//
func (m *DDLServiceMgr) handleBuildCommand() {

	entries, err := metakv.ListAllChildren(mc.BuildDDLCommandTokenPath)
	if err != nil {
		logging.Warnf("DDLServiceMgr: Fail to cleanup build index token upon rebalancing.  Skip cleanup.  Internal Error = %v", err)
		return
	}

	for _, entry := range entries {

		if strings.Contains(entry.Path, mc.BuildDDLCommandTokenPath) && entry.Value != nil {

			logging.Infof("DDLServiceMgr: processing build index token %v", entry.Path)

			command, err := mc.UnmarshallBuildCommandToken(entry.Value)
			if err != nil {
				logging.Warnf("DDLServiceMgr: Fail to clean build index token upon rebalancing.  Skp command %v.  Internal Error = %v.", entry.Path, err)
				continue
			}

			//
			// At this point, the metadata provider has been connected to all indexer at least once (refreshOnTopology gurantees that).   So
			// metadata provider has a snapshot of the metadata from each indexer at some point in time.   It will return index even if metadata
			// provider is not connected to the indexer at the exact moment when this call is made.
			//
			cleanup := true
			if index := m.provider.FindIndexIgnoreStatus(command.DefnId); index != nil {
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

			// Remove token
			if cleanup {
				if err := MetakvDel(entry.Path); err != nil {
					logging.Warnf("DDLServiceMgr: Fail to remove build index token %v. Error = %v", entry.Path, err)
				} else {
					logging.Infof("DDLServiceMgr: Remove build index token %v.", entry.Path)
				}
			}
		}
	}
}

func (m *DDLServiceMgr) handleListMetadataTokens(w http.ResponseWriter, r *http.Request) {

	if !m.validateAuth(w, r) {
		logging.Errorf("DDLServiceMgr::handleListMetadataTokens Validation Failure for Request %v", r)
		return
	}

	if r.Method == "GET" {

		logging.Infof("DDLServiceMgr::handleListMetadataTokens Processing Request %v", r)

		buildTokens, err := metakv.ListAllChildren(mc.BuildDDLCommandTokenPath)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error() + "\n"))
			return
		}

		deleteTokens, err1 := metakv.ListAllChildren(mc.DeleteDDLCommandTokenPath)
		if err1 != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error() + "\n"))
			return
		}

		header := w.Header()
		header["Content-Type"] = []string{"application/json"}
		w.WriteHeader(http.StatusOK)

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
	}
}

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

	indexes, _ := m.provider.ListIndex()
	for _, index := range indexes {

		for _, inst := range index.Instances {

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

		resp, err := postWithAuth(addr+"/settings", "application/json", bodybuf)
		if err != nil {
			logging.Errorf("DDLServiceMgr:handleClusterStorageMode(). Encounter error when try to change setting.  Retry with another indexer node. Error:%v", err)
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

	if err := cinfo.Fetch(); err != nil {
		return nil, nil, err
	}

	adminAddrMap := make(map[string]string)
	httpAddrMap := make(map[string]string)

	// Discover indexer service from ClusterInfoCache
	nids := cinfo.GetNodesByServiceType(common.INDEX_HTTP_SERVICE)
	for _, nid := range nids {

		addr, err := cinfo.GetServiceAddress(nid, common.INDEX_HTTP_SERVICE)
		if err == nil {

			resp, err := getWithAuth(addr + "/getLocalIndexMetadata")
			if err != nil {
				continue
			}

			localMeta := new(manager.LocalIndexMetadata)
			if err := convertResponse(resp, localMeta); err != nil {
				continue
			}

			// Only consider valid nodes
			if nodes != nil && nodes[service.NodeID(localMeta.NodeUUID)] {
				httpAddrMap[localMeta.NodeUUID] = addr

				adminAddr, err := cinfo.GetServiceAddress(nid, common.INDEX_ADMIN_SERVICE)
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
			fmt.Sprintf("DDLServiceMgr: Fail to initialize metadata provider.  Unknown host=%v", nodes))
	}

	// initialize a new MetadataProvider
	ustr, err := common.NewUUID()
	if err != nil {
		return nil, nil, errors.New(fmt.Sprintf("DDLServiceMgr: Fail to initialize metadata provider.  Internal Error = %v", err))
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
	// If it cannot synchronized with 500ms, then return error.
	if !provider.AllWatchersAlive() {

		// Wait for initialization complete
		ticker := time.NewTicker(time.Millisecond * 50)
		defer ticker.Stop()
		retry := 10

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
				return nil, nil, errors.New("DDLServiceMgr: Fail to initialize metadata provider.  Unable to connect to all indexer nodes within 500ms.")
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
		logging.Errorf("DDLServiceMgr: notifyNewTopologyChange(): Fail to initialize metadata provider.  Error=%v.", err)
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
