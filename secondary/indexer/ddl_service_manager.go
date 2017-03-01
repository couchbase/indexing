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
	//"github.com/couchbase/indexing/secondary/planner"
	"errors"
	"fmt"
	"net"
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
}

//
// DDL related settings
//
type ddlSettings struct {
	numReplica int32
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
	}

	mgr.config.Store(config)

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
// Recover drop index command
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

	entries, err := metakv.ListAllChildren(client.DeleteDDLCommandTokenPath)
	if err != nil {
		logging.Warnf("DDLServiceMgr: Fail to cleanup delete index token upon rebalancing.  Skip cleanup.  Internal Error = %v", err)
		return
	}

	if len(entries) == 0 {
		return
	}

	// Refresh metadata provider on topology change
	if err := m.refreshOnTopologyChange(change, isCancel); err != nil {
		logging.Warnf("DDLServiceMgr: Fail to clean delete index token upon rebalancing.  Skip Cleanup. Internal Error = %v", err)
		return
	}

	for _, entry := range entries {

		if strings.Contains(entry.Path, client.DeleteDDLCommandTokenPath) && entry.Value != nil {

			logging.Infof("DDLServiceMgr: processing delete index token %v", entry.Path)

			command, err := client.UnmarshallDeleteCommandToken(entry.Value)
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

//////////////////////////////////////////////////////////////
// Metadata Provider
//////////////////////////////////////////////////////////////

func (m *DDLServiceMgr) refreshMetadataProvider() error {

	if m.provider != nil {
		m.provider.Close()
		m.provider = nil
	}

	nodes := make(map[service.NodeID]bool)
	for key, value := range m.nodes {
		nodes[key] = value
	}

	provider, err := m.newMetadataProvider(nodes)
	if err != nil {
		return err
	}

	m.provider = provider
	return nil
}

func (m *DDLServiceMgr) newMetadataProvider(nodes map[service.NodeID]bool) (*client.MetadataProvider, error) {

	// initialize ClusterInfoCache
	url, err := common.ClusterAuthUrl(m.clusterAddr)
	if err != nil {
		return nil, err
	}

	cinfo, err := common.NewClusterInfoCache(url, DEFAULT_POOL)
	if err != nil {
		return nil, err
	}

	if err := cinfo.Fetch(); err != nil {
		return nil, err
	}

	adminAddrMap := make(map[string]string)

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

				adminAddr, err := cinfo.GetServiceAddress(nid, common.INDEX_ADMIN_SERVICE)
				if err != nil {
					return nil, err
				}

				adminAddrMap[localMeta.NodeUUID] = adminAddr
				delete(nodes, service.NodeID(localMeta.NodeUUID))
			}
		}
	}

	if len(nodes) != 0 {
		return nil, errors.New(
			fmt.Sprintf("DDLServiceMgr: Fail to initialize metadata provider.  Unknown host=%v", nodes))
	}

	// initialize a new MetadataProvider
	ustr, err := common.NewUUID()
	if err != nil {
		return nil, errors.New(fmt.Sprintf("DDLServiceMgr: Fail to initialize metadata provider.  Internal Error = %v", err))
	}
	providerId := ustr.Str()

	provider, err := client.NewMetadataProvider(providerId, nil, m.settings)
	if err != nil {
		if provider != nil {
			provider.Close()
		}
		return nil, err
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
				return provider, nil
			}

			if retry == 0 {
				for nodeUUID, adminport := range adminAddrMap {
					if !provider.IsWatcherAlive(nodeUUID) {
						logging.Warnf("DDLServiceMgr: cannot connect to node %v", adminport)
					}
				}

				provider.Close()
				return nil, errors.New("DDLServiceMgr: Fail to initialize metadata provider.  Unable to connect to all indexer nodes within 500ms.")
			}
		}
	}

	return provider, nil
}

//////////////////////////////////////////////////////////////
// Topology change
//////////////////////////////////////////////////////////////

//
// Callback to notify there is a topology change
//
func (m *DDLServiceMgr) refreshOnTopologyChange(change *service.TopologyChange, isCancel bool) error {

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
	if err := m.refreshMetadataProvider(); err != nil {
		logging.Errorf("DDLServiceMgr: notifyNewTopologyChange(): Fail to initialize metadata provider.  Error=%v.", err)
		return err
	}

	return nil
}

//////////////////////////////////////////////////////////////
// Settings
//////////////////////////////////////////////////////////////

func (s *ddlSettings) NumReplica() int32 {
	return atomic.LoadInt32(&s.numReplica)
}

func (s *ddlSettings) handleSettings(config common.Config) {

	numReplica := int32(config["settings.num_replica"].Int())
	if numReplica >= 0 {
		atomic.StoreInt32(&s.numReplica, numReplica)
	} else {
		logging.Errorf("DDLServiceMgr: invalid setting value for num_replica=%v", numReplica)
	}
}
