// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.
package planner

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	mc "github.com/couchbase/indexing/secondary/manager/common"
	"github.com/couchbase/indexing/secondary/security"
)

// ClusterInfoClient gets initialized in RetrievePlanFromCluster
// when this method is invoked for the first time
var cinfoClient *common.ClusterInfoClient

// This mutex will protect cinfoClient from mulitple initializations
// since it is possible for RetrievePlanFromCluster to get invoked
// from multiple go-routines
var cinfoClientMutex sync.Mutex

// restRequestTimeout in Seconds
var restRequestTimeout uint32 = 120

func GetRestRequestTimeout() uint32 {
	return atomic.LoadUint32(&restRequestTimeout)
}

func SetRestRequestTimeout(val uint32) {
	atomic.StoreUint32(&restRequestTimeout, val)
}

//////////////////////////////////////////////////////////////
// Concrete Type/Struct
//////////////////////////////////////////////////////////////

type LocalIndexMetadata struct {
	IndexerId        string             `json:"indexerId,omitempty"`
	NodeUUID         string             `json:"nodeUUID,omitempty"`
	StorageMode      string             `json:"storageMode,omitempty"`
	Timestamp        int64              `json:"timestamp,omitempty"`
	LocalSettings    map[string]string  `json:"localSettings,omitempty"`
	IndexTopologies  []mc.IndexTopology `json:"topologies,omitempty"`
	IndexDefinitions []common.IndexDefn `json:"definitions,omitempty"`
}

// tokenKey holds match fields from tokens in a type that can
// be used as a map key.
type tokenKey struct {
	DefnId common.IndexDefnId
	InstId common.IndexInstId
}

///////////////////////////////////////////////////////
// Function
///////////////////////////////////////////////////////

//
// This function retrieves the index layout plan from a live cluster.
// This function uses REST API to retrieve index metadata, instead of
// using metadata provider.  This method should not use metadata provider
// since this method can be called from metadata provider, so it is to
// avoid code cyclic dependency.
//
func RetrievePlanFromCluster(clusterUrl string, hosts []string) (*Plan, error) {

	config, err := common.GetSettingsConfig(common.SystemConfig)
	if err != nil {
		logging.Errorf("Planner::getIndexLayout: Error from retrieving indexer settings. Error = %v", err)
		return nil, err
	}

	// Initialize cluster info client at the first call of RetrievePlanFromCluster
	cinfoClientMutex.Lock()
	if cinfoClient == nil {
		cinfoClient, err = common.NewClusterInfoClient(clusterUrl, "default", config)
		if err == nil {
			cinfoClient.SetUserAgent("RetrievePlanFromCluster")
		}
	}
	cinfoClientMutex.Unlock()
	if err != nil { // Error while initilizing clusterInfoClient
		logging.Errorf("Planner::RetrievePlanFromCluster: Error while initializing cluster info client at %v. Error = %v", clusterUrl, err)
		return nil, err
	}

	cinfo := cinfoClient.GetClusterInfoCache()
	if err := cinfo.FetchWithLock(); err != nil {
		return nil, err
	}

	indexers, err := getIndexLayout(config, hosts)
	if err != nil {
		return nil, err
	}

	if err := processCreateToken(indexers, config); err != nil {
		return nil, err
	}

	replicaMap := generateReplicaMap(indexers)

	if err := processDeleteToken(indexers); err != nil {
		return nil, err
	}

	if err := processDropInstanceToken(indexers, replicaMap); err != nil {
		return nil, err
	}

	cleanseIndexLayout(indexers)

	// If there is no indexer, plan.Placement will be nil.
	plan := &Plan{Placement: indexers,
		MemQuota:         0,
		CpuQuota:         0,
		IsLive:           true,
		UsedReplicaIdMap: replicaMap,
	}

	err = getIndexStats(plan, config)
	if err != nil {
		return nil, err
	}

	err = getIndexSettings(plan)
	if err != nil {
		return nil, err
	}

	err = getIndexNumReplica(plan)
	if err != nil {
		return nil, err
	}

	// Recalculate the index and indexer memory and cpu usage using the sizing formaula.
	// The stats retrieved from indexer typically has lower memory/cpu utilization than
	// sizing formula, since sizing forumula captures max usage capacity. By recalculating
	// the usage, it makes sure that planning does not partially skewed data.
	recalculateIndexerSize(plan)

	return plan, nil
}

//
// This function recalculates the index and indexer sizes baesd on sizing formula.
//
func recalculateIndexerSize(plan *Plan) {

	sizing := newGeneralSizingMethod()

	for _, indexer := range plan.Placement {
		for _, index := range indexer.Indexes {
			index.ComputeSizing(true, sizing)
		}
	}

	for _, indexer := range plan.Placement {
		sizing.ComputeIndexerSize(indexer)
	}
}

//
// This function retrieves the index layout.
//
func getIndexLayout(config common.Config, hosts []string) ([]*IndexerNode, error) {
	cinfo := cinfoClient.GetClusterInfoCache()
	cinfo.RLock()
	defer cinfo.RUnlock()

	// Find all nodes that has a index http service
	// 1) This method will exclude inactive_failed node in the cluster.  But if a node failed after the topology is fetched, then
	//    the following code could fail (if cannot connect to indexer service).  Note that ns-server will shutdown indexer
	//    service due to failed over.
	// 2) For rebalancing, the planner will also skip failed nodes, even if this method succeed.
	// 3) Note that if the planner is invoked by the rebalancer, the rebalancer will receive callback ns_server if there is
	//    an indexer node fails over while planning is happening.
	// 4) This method will exclude inactive_new node in the cluster.
	// 5) This may include unhealthy node since unhealthiness is not a cluster membership state (need verification).  This
	//    function can fail if it cannot reach the unhealthy node.
	//
	nids := cinfo.GetNodesByServiceType(common.INDEX_HTTP_SERVICE)

	if len(nids) == 0 {
		return nil, errors.New("No indexing service available.")
	}

	list := make([]*IndexerNode, 0)
	numIndexes := 0

	for _, nid := range nids {

		// create an empty indexer object using the indexer host name
		node, err := createIndexerNode(cinfo, nid)
		if err != nil {
			logging.Errorf("Planner::getIndexLayout: Error from initializing indexer node. Error = %v", err)
			return nil, err
		}

		// If a host list is given, then only consider those hosts.  For planning, this is to
		// ensure that planner only consider those nodes that have acquired locks.
		if len(hosts) != 0 {
			found := false
			for _, host := range hosts {
				if strings.ToLower(host) == strings.ToLower(node.NodeId) {
					found = true
					break
				} else {
					hp, _, _, err := security.EncryptPortFromAddr(host)
					if err != nil {
						return nil, err
					}
					if strings.ToLower(hp) == strings.ToLower(node.NodeId) {
						found = true
						break
					}
				}
			}

			if !found {
				logging.Infof("Planner:Skip node %v since it is not in the given host list %v", node.NodeId, hosts)
				continue
			}
		}

		// assign server group
		node.ServerGroup = cinfo.GetServerGroup(nid)

		// obtain the admin port for the indexer node
		addr, err := cinfo.GetServiceAddress(nid, common.INDEX_HTTP_SERVICE, true)
		if err != nil {
			logging.Errorf("Planner::getIndexLayout: Error from getting service address for node %v. Error = %v", node.NodeId, err)
			return nil, err
		}
		node.RestUrl = addr

		// Read the index metadata from the indexer node.
		localMeta, err := getLocalMetadata(addr)
		if err != nil {
			logging.Errorf("Planner::getIndexLayout: Error from reading index metadata for node %v. Error = %v", node.NodeId, err)
			return nil, err
		}

		// get the node UUID
		node.NodeUUID = localMeta.NodeUUID
		node.IndexerId = localMeta.IndexerId
		node.StorageMode = localMeta.StorageMode
		node.exclude = localMeta.LocalSettings["excludeNode"]

		// convert from LocalIndexMetadata to IndexUsage
		indexes, err := ConvertToIndexUsages(config, localMeta, node)
		if err != nil {
			logging.Errorf("Planner::getIndexLayout: Error for converting index metadata to index usage for node %v. Error = %v", node.NodeId, err)
			return nil, err
		}

		node.Indexes = indexes
		numIndexes += len(indexes)
		list = append(list, node)
	}

	if numIndexes != 0 {
		for _, node := range list {
			if !common.IsValidIndexType(node.StorageMode) {
				err := errors.New(fmt.Sprintf("Fail to get storage mode	from %v. Storage mode = %v", node.RestUrl, node.StorageMode))
				logging.Errorf("Planner::getIndexLayout: Error = %v", err)
				return nil, err
			}
		}
	}

	return list, nil
}

//
// This function convert index defintions from a single metadatda repository to a list of IndexUsage.
//
func ConvertToIndexUsages(config common.Config, localMeta *LocalIndexMetadata, node *IndexerNode) ([]*IndexUsage, error) {

	list := ([]*IndexUsage)(nil)

	// Iterate through all the index definition.    For each index definition, create an index usage object.
	for i := 0; i < len(localMeta.IndexDefinitions); i++ {

		defn := &localMeta.IndexDefinitions[i]
		indexes, err := ConvertToIndexUsage(config, defn, localMeta)
		if err != nil {
			return nil, err
		}

		if len(indexes) != 0 {
			for _, index := range indexes {
				index.initialNode = node
				list = append(list, index)
			}
		}
	}

	return list, nil
}

//
// This function convert a single index defintion to IndexUsage.
//
func ConvertToIndexUsage(config common.Config, defn *common.IndexDefn, localMeta *LocalIndexMetadata) ([]*IndexUsage, error) {

	// find the topology metadata
	topology := findTopologyByBucket(localMeta.IndexTopologies, defn.Bucket)
	if topology == nil {
		logging.Errorf("Planner::getIndexLayout: Fail to find index topology for bucket %v.", defn.Bucket)
		return nil, nil
	}

	// find the index instance from topology metadata
	insts := topology.GetIndexInstancesByDefn(defn.DefnId)
	if len(insts) == 0 {
		logging.Errorf("Planner::getIndexLayout: Fail to find index instance for definition %v.", defn.DefnId)
		return nil, nil
	}

	var result []*IndexUsage

	for _, inst := range insts {

		// Check the index state.  Only handle index that is active or being built.
		// For index that is in the process of being deleted, planner expects the resource
		// will eventually be freed, so it won't included in planning.
		state, _ := topology.GetStatusByInst(defn.DefnId, common.IndexInstId(inst.InstId))
		if state != common.INDEX_STATE_CREATED &&
			state != common.INDEX_STATE_DELETED &&
			state != common.INDEX_STATE_ERROR &&
			state != common.INDEX_STATE_NIL {

			if !common.IsPartitioned(defn.PartitionScheme) {
				inst.NumPartitions = 1
			}

			for _, partn := range inst.Partitions {

				// create an index usage object
				index := makeIndexUsageFromDefn(defn, common.IndexInstId(inst.InstId), common.PartitionId(partn.PartId), uint64(inst.NumPartitions))

				// Copy the index state from the instance to IndexUsage.
				index.state = state

				// index is pinned to a node
				if len(defn.Nodes) != 0 {
					index.Hosts = defn.Nodes
				}

				// This value will be reset in IndexUsage.ComputeSizing()
				index.NoUsageInfo = defn.Deferred && (state == common.INDEX_STATE_READY || state == common.INDEX_STATE_CREATED)

				// Ensure that the size estimation is always triggered for deferred indexes.
				index.NeedsEstimate = index.NoUsageInfo

				// update partition
				numVbuckets := config["indexer.numVbuckets"].Int()
				pc := common.NewKeyPartitionContainer(numVbuckets, int(inst.NumPartitions), defn.PartitionScheme, defn.HashScheme)

				// Is the index being deleted by user?   Thsi will read the delete token from metakv.  If untable read from metakv,
				// pendingDelete is false (cannot assert index is to-be-delete).
				pendingDelete, err := mc.DeleteCommandTokenExist(defn.DefnId)
				if err != nil {
					return nil, err
				}
				index.pendingDelete = pendingDelete

				pendingBuild, err := mc.BuildCommandTokenExist(defn.DefnId)
				if err != nil {
					return nil, err
				}
				index.pendingBuild = pendingBuild

				// get the version from inst or partition
				version := partn.Version
				if !common.IsPartitioned(defn.PartitionScheme) {
					// for backward compatibility on non-partitioned index (pre-5.5)
					if version == 0 && version != inst.Version {
						version = inst.Version
					}
				}

				// update internal info
				index.Instance = &common.IndexInst{
					InstId:    common.IndexInstId(inst.InstId),
					Defn:      *defn,
					State:     common.IndexState(inst.State),
					Stream:    common.StreamId(inst.StreamId),
					Error:     inst.Error,
					ReplicaId: int(inst.ReplicaId),
					Version:   int(version),
					RState:    common.RebalanceState(inst.RState),
					Pc:        pc,
				}

				logging.Debugf("Create Index usage %v %v %v %v %v",
					index.Name, index.Bucket, index.Instance.InstId, index.PartnId, index.Instance.ReplicaId)

				result = append(result, index)
			}
		}
	}

	return result, nil
}

//
// This function retrieves the index stats.
//
func getIndexStats(plan *Plan, config common.Config) error {

	cinfo := cinfoClient.GetClusterInfoCache()
	cinfo.RLock()
	defer cinfo.RUnlock()
	clusterVersion := cinfo.GetClusterVersion()

	// find all nodes that has a index http service
	nids := cinfo.GetNodesByServiceType(common.INDEX_HTTP_SERVICE)

	if len(nids) == 0 {
		return errors.New("No indexing service available.")
	}

	for _, nid := range nids {

		// Find the indexer host name
		nodeId, err := getIndexerHost(cinfo, nid)
		if err != nil {
			logging.Errorf("Planner::getIndexStats: Error from initializing indexer node. Error = %v", err)
			return err
		}

		// look up the corresponding indexer object based on the nodeId
		indexer := findIndexerByNodeId(plan.Placement, nodeId)
		if indexer == nil {
			logging.Verbosef("Planner::getIndexStats: Skip indexer %v since it is not in the included list")
			continue
		}

		// obtain the admin port for the indexer node
		addr, err := cinfo.GetServiceAddress(nid, common.INDEX_HTTP_SERVICE, true)
		if err != nil {
			logging.Errorf("Planner::getIndexStats: Error from getting service address for node %v. Error = %v", nodeId, err)
			return err
		}

		// Read the index stats from the indexer node.
		stats, err := getLocalStats(addr)
		if err != nil {
			logging.Errorf("Planner::getIndexStats: Error from reading index stats for node %v. Error = %v", nodeId, err)
			return err
		}
		statsMap := stats.ToMap()

		/*
			CpuUsage    uint64 `json:"cpuUsage,omitempty"`
			DiskUsage   uint64 `json:"diskUsage,omitempty"`
		*/

		var actualStorageMem uint64
		// memory_used_storage constains the total storage consumption,
		// including fdb overhead, main index and back index.  This also
		// includes overhead (skip list / back index).
		if memUsedStorage, ok := statsMap["memory_used_storage"]; ok {
			actualStorageMem = uint64(memUsedStorage.(float64))
		}

		// memory_used is the memory used by indexer.  This includes
		// golang in-use heap space, golang idle heap space, and
		// storage memory manager space (e.g. jemalloc heap space).
		var actualTotalMem uint64
		if memUsed, ok := statsMap["memory_used"]; ok {
			actualTotalMem = uint64(memUsed.(float64))
		}

		// memory_quota is user specified memory quota.
		if memQuota, ok := statsMap["memory_quota"]; ok {
			plan.MemQuota = uint64(memQuota.(float64))
		}

		// uptime
		var elapsed uint64
		if uptimeStat, ok := statsMap["uptime"]; ok {
			uptime := uptimeStat.(string)
			if duration, err := time.ParseDuration(uptime); err == nil {
				elapsed = uint64(duration.Seconds())
			}
		}
		var indexerVersion int
		if indexerVersion, err = cinfo.GetServerVersion(nid); err != nil {
			logging.Errorf("Planner::getIndexStats: Error from reading indexer version for node %v. Error = %v", nodeId, err)
			return err
		}

		// cpu core in host.   This is the actual num of cpu core, not cpu quota.
		/*
			var actualCpuCore uint64
			if cpuCore, ok := statsMap["num_cpu_core"]; ok {
				actualCpuCore = uint64(cpuCore.(float64))
			}
		*/

		// cpu utilization for the indexer process
		var actualCpuUtil float64
		if cpuUtil, ok := statsMap["cpu_utilization"]; ok {
			actualCpuUtil = cpuUtil.(float64) / 100
		}

		var totalIndexMemUsed uint64
		var totalMutation uint64
		var totalScan uint64
		for _, index := range indexer.Indexes {

			/*
				CpuUsage    uint64 `json:"cpuUsage,omitempty"`
				DiskUsage   uint64 `json:"diskUsage,omitempty"`
			*/

			var key string
			var key1 string

			var indexName string
			var indexName1 string

			if clusterVersion >= common.INDEXER_55_VERSION {
				indexName = index.GetStatsName()
				indexName1 = index.GetInstStatsName()
			} else {
				indexName = index.GetInstStatsName()
				indexName1 = index.GetInstStatsName()
			}

			// items_count captures number of key per index
			key = fmt.Sprintf("%v:%v:items_count", index.Bucket, indexName)
			if itemsCount, ok := statsMap[key]; ok {
				index.ActualNumDocs = uint64(itemsCount.(float64))
			}

			// build completion
			key = fmt.Sprintf("%v:%v:build_progress", index.Bucket, indexName1)
			if buildProgress, ok := statsMap[key]; ok {
				index.ActualBuildPercent = uint64(buildProgress.(float64))
			}

			// resident ratio
			key = fmt.Sprintf("%v:%v:resident_percent", index.Bucket, indexName)
			if residentPercent, ok := statsMap[key]; ok {
				index.ActualResidentPercent = uint64(residentPercent.(float64))
			}

			// data_size is the total key size of index, including back index.
			// data_size for MOI is 0.
			key = fmt.Sprintf("%v:%v:data_size", index.Bucket, indexName)
			if dataSize, ok := statsMap[key]; ok {
				index.ActualDataSize = uint64(dataSize.(float64))
				// From 6.5, data_size stat contains uncompressed data size for plasma
				// So, no need to consider the compressed version of data so that other
				// parameters like ActualKeySize, AvgDocKeySize, AvgSecKeySize does not break
				if indexerVersion >= common.INDEXER_65_VERSION {
					if index.StorageMode == common.PlasmaDB {
						if config["indexer.plasma.useCompression"].Bool() {
							// factor in compression estimation (compression ratio defaulted to 3)
							index.ActualDataSize = index.ActualDataSize / 3
						}
					}
				}
			}

			// memory usage per index
			key = fmt.Sprintf("%v:%v:memory_used", index.Bucket, indexName)
			if memUsed, ok := statsMap[key]; ok {
				index.ActualMemStats = uint64(memUsed.(float64))
				index.ActualMemUsage = index.ActualMemStats
				totalIndexMemUsed += index.ActualMemUsage
			} else {
				// calibrate memory usage based on resident percent
				// ActualMemUsage will be rewritten later
				index.ActualMemUsage = index.ActualDataSize * index.ActualResidentPercent / 100
				// factor in compression estimation (compression ratio defaulted to 3)
				if index.StorageMode == common.PlasmaDB {
					if config["indexer.plasma.useCompression"].Bool() {
						index.ActualMemUsage = index.ActualMemUsage * 3
					}
				}
				totalIndexMemUsed += index.ActualMemUsage
			}

			// disk usage per index
			key = fmt.Sprintf("%v:%v:avg_disk_bps", index.Bucket, indexName)
			if diskUsed, ok := statsMap[key]; ok {
				index.ActualDiskUsage = uint64(diskUsed.(float64))
			}

			// avg_sec_key_size is currently unavailable in 4.5.   To estimate,
			// the key size, it divides index data_size by items_count.  This
			// contains sec key size + doc key size + main index overhead (74 bytes).
			// Subtract 74 bytes to get sec key size.
			key = fmt.Sprintf("%v:%v:avg_sec_key_size", index.Bucket, indexName)
			if avgSecKeySize, ok := statsMap[key]; ok {
				index.AvgSecKeySize = uint64(avgSecKeySize.(float64))
			} else if !index.IsPrimary {
				// Aproximate AvgSecKeySize.   AvgSecKeySize includes both
				// sec key len + doc key len
				if index.ActualNumDocs != 0 && index.ActualDataSize != 0 {
					index.ActualKeySize = index.ActualDataSize / index.ActualNumDocs
				}
			}

			// These stats are currently unavailable in 4.5.
			key = fmt.Sprintf("%v:%v:avg_doc_key_size", index.Bucket, indexName)
			if avgDocKeySize, ok := statsMap[key]; ok {
				index.AvgDocKeySize = uint64(avgDocKeySize.(float64))
			} else if index.IsPrimary {
				// Aproximate AvgDocKeySize.  Subtract 74 bytes for main
				// index overhead
				if index.ActualNumDocs != 0 && index.ActualDataSize != 0 {
					index.ActualKeySize = index.ActualDataSize / index.ActualNumDocs
				}
			}

			// These stats are currently unavailable in 4.5.
			key = fmt.Sprintf("%v:%v:avg_arr_size", index.Bucket, indexName)
			if avgArrSize, ok := statsMap[key]; ok {
				index.AvgArrSize = uint64(avgArrSize.(float64))
			}

			// These stats are currently unavailable in 4.5.
			key = fmt.Sprintf("%v:%v:avg_arr_key_size", index.Bucket, indexName)
			if avgArrKeySize, ok := statsMap[key]; ok {
				index.AvgArrKeySize = uint64(avgArrKeySize.(float64))
			}

			// avg_drain_rate computes the actual number of rows persisted to storage.
			// This does not include incoming rows that are filtered out by back-index.
			// These stats are currently unavailable in 4.5.
			key = fmt.Sprintf("%v:%v:avg_drain_rate", index.Bucket, indexName)
			if avgDrainRate, ok := statsMap[key]; ok {
				index.DrainRate = uint64(avgDrainRate.(float64))
			}

			// avg_mutation_rate computes the incoming number of docs.
			// This does not include the individual element in an array index.
			// These stats are currently unavailable in 4.5.
			key = fmt.Sprintf("%v:%v:avg_mutation_rate", index.Bucket, indexName)
			if avgMutationRate, ok := statsMap[key]; ok {
				index.MutationRate = uint64(avgMutationRate.(float64))
			} else {
				key = fmt.Sprintf("%v:%v:num_flush_queued", index.Bucket, indexName)
				if flushQueuedStat, ok := statsMap[key]; ok {
					flushQueued := uint64(flushQueuedStat.(float64))

					if flushQueued != 0 {
						index.MutationRate = flushQueued / elapsed
						totalMutation += index.MutationRate
					}
				}
			}

			// In general, drain rate should be greater or equal to mutation rate.
			// Mutation rate could be greater than drain rate if index is based on field A
			// which is a slow mutating field.  But there are more rapidly changing field
			// in the document.  In this case, mutation will still be sent to index A.
			// Indexer will still spend CPU on mutation, but they will get dropped by the back-index,
			// since field A has not changed.
			if index.MutationRate < index.DrainRate {
				totalMutation += index.DrainRate
			} else {
				totalMutation += index.MutationRate
			}

			// These stats are currently unavailable in 4.5.
			key = fmt.Sprintf("%v:%v:avg_scan_rate", index.Bucket, indexName)
			key1 = fmt.Sprintf("%v:%v:avg_scan_rate", index.Bucket, indexName1)
			if avgScanRate, ok := statsMap[key]; ok {
				index.ScanRate = uint64(avgScanRate.(float64))
				totalScan += index.ScanRate
			} else if avgScanRate, ok := statsMap[key1]; ok {
				index.ScanRate = uint64(avgScanRate.(float64))
				totalScan += index.ScanRate
			} else {
				key = fmt.Sprintf("%v:%v:num_rows_returned", index.Bucket, indexName)
				if rowReturnedStat, ok := statsMap[key]; ok {
					rowReturned := uint64(rowReturnedStat.(float64))

					if rowReturned != 0 {
						index.ScanRate = rowReturned / elapsed
						totalScan += index.ScanRate
					}
				}
			}
		}

		// Compute the estimated memory usage for each index.  This also computes the aggregated indexer mem usage.
		//
		// The memory usage is computed as follows (per node):
		// 1) Compute the memory resident data size for each index (based on data_size and resident_percent stats)
		// 2) Compute the total memory resident data size for all indexes
		// 3) Compute the utilization ratio of each index based on (resident data size / total resident data size)
		// 4) Compute memory usage of each index based on indexer memory used (utilization ratio * memory_used_storage)
		// 5) Compute memory overhead of each index based on indexer memory used (utilization ratio * (memory_used - memory_used_storage))
		// 6) Calibrate index memory usage and overhead based on build percent
		//
		// Mem usage can be 0 if
		// 1) there is no index stats
		// 2) index has no data (datasize = 0) (e.g. deferred index)
		//
		// Note:
		// 1) Resident data size is sensitive to scan and mutation traffic.   It is a reflection of the working set
		//    of the index at the time when the planner is run.
		//
		for _, index := range indexer.Indexes {
			ratio := float64(0)
			if totalIndexMemUsed != 0 {
				ratio = float64(index.ActualMemUsage) / float64(totalIndexMemUsed)
			}

			if index.ActualMemStats == 0 {
				index.ActualMemUsage = uint64(float64(actualStorageMem) * ratio)
			}

			if actualTotalMem > actualStorageMem {
				index.ActualMemOverhead = uint64(float64(actualTotalMem-actualStorageMem) * ratio)
			} else {
				index.ActualMemOverhead = 0
			}

			// If index is not fully build, estimate memory consumption after it is fully build
			// at the same resident ratio.
			if index.ActualBuildPercent != 0 {
				index.ActualMemUsage = index.ActualMemUsage * 100 / index.ActualBuildPercent
				index.ActualMemOverhead = index.ActualMemOverhead * 100 / index.ActualBuildPercent
				index.ActualDataSize = index.ActualDataSize * 100 / index.ActualBuildPercent
			}

			// compute the minimum memory requirement for the index
			// 1) If index resident ratio is above 0, then compute memory required for min resident ratio (default:20%)
			// 2) If index resident ratio is 0, then use sizing equation to estimate key size.
			// 3) For MOI, min memory is the same as actual memory usage

			minRatio := config["indexer.planner.minResidentRatio"].Float64()
			if index.StorageMode == common.MemDB || index.StorageMode == common.MemoryOptimized {
				minRatio = 1.0
			}

			// If index has resident memory, then compute minimum memory usage using current memory usage.
			if !index.NoUsageInfo {
				if index.ActualResidentPercent > 0 {
					indexTotalMem := index.ActualMemUsage + index.ActualMemOverhead
					ratio := float64(index.ActualResidentPercent) / 100
					index.ActualMemMin = uint64(float64(indexTotalMem) / ratio * minRatio)

				} else if index.ActualNumDocs > 0 {
					// If index has no resident memory but it has keys, then estimate using sizing equation.
					dataSize := index.ActualDataSize
					if index.StorageMode == common.PlasmaDB {
						if config["indexer.plasma.useCompression"].Bool() {
							dataSize = dataSize * 3
						}
					}
					index.ActualMemMin = uint64(float64(dataSize) * minRatio)
				}
			}

			indexer.ActualDataSize += index.ActualDataSize
			indexer.ActualMemUsage += index.ActualMemUsage
			indexer.ActualMemOverhead += index.ActualMemOverhead
			indexer.ActualDiskUsage += index.ActualDiskUsage
			indexer.ActualMemMin += index.ActualMemMin
		}

		// Compute the estimated cpu usage for each index.  This also computes the aggregated indexer cpu usage.
		//
		// The cpu usage is computed as follows (per node):
		// 1) Compute the mutation rate for each index (based on avg_mutation_rate stats)
		// 2) Compute the scan rate for each index (based on avg_scan_rate stats)
		// 3) Compute the total mutation rate for all indexes
		// 4) Compute the total scan rate for all indexes
		// 5) Compute the mutation ratio of each index based on (index mutation rate / total mutation rate)
		// 6) Compute the scan ratio of each index based on (index scan rate / total scan rate)
		// 7) Compute an aggregated ratio of each index (mutation rate / 5 + scan rate / 2)
		// 8) Compute cpu utilization for each index (cpu utilization * aggregated ratio)
		//
		// CPU usge can be 0 if
		// 1) there is no index stats
		// 2) index has no scan or mutation (e.g. deferred index)
		//
		// Note:
		// 1) Mutation rate and scan rate are computed using running average.   Even though it
		//    is not based on instantaneous information, running average can quickly converge.
		// 2) Mutation rate is index storage drain rate (numItemsFlushed).  It reflects num keys flushed in array index.
		//
		for _, index := range indexer.Indexes {

			mutationRatio := float64(0)
			if totalMutation != 0 {

				rate := index.MutationRate
				if index.MutationRate < index.DrainRate {
					rate = index.DrainRate
				}

				mutationRatio = float64(rate) / float64(totalMutation)
			}

			scanRatio := float64(0)
			if totalScan != 0 {
				scanRatio = float64(index.ScanRate) / float64(totalScan)
			}

			ratio := mutationRatio
			if scanRatio != 0 {
				if mutationRatio != 0 {
					// mutation uses 5 times less cpu than scan (using MOI equation)
					ratio = ((mutationRatio / 5) + scanRatio) / 2
				} else {
					ratio = scanRatio
				}
			}

			usage := float64(actualCpuUtil) * ratio

			if usage > 0 {
				index.ActualCpuUsage = usage
			}

			index.ActualDrainRate = index.MutationRate
			if index.MutationRate < index.DrainRate {
				index.ActualDrainRate = index.DrainRate
			}
			index.ActualScanRate = index.ScanRate

			indexer.ActualCpuUsage += index.ActualCpuUsage
			indexer.ActualDrainRate += index.ActualDrainRate
			indexer.ActualScanRate += index.ActualScanRate
		}
	}

	return nil
}

//
// This function retrieves the index settings.
//
func getIndexSettings(plan *Plan) error {

	cinfo := cinfoClient.GetClusterInfoCache()
	cinfo.RLock()
	defer cinfo.RUnlock()

	// find all nodes that has a index http service
	nids := cinfo.GetNodesByServiceType(common.INDEX_HTTP_SERVICE)

	if len(nids) == 0 {
		return errors.New("No indexing service available.")
	}

	for _, nid := range nids {

		// Find the indexer host name
		nodeId, err := getIndexerHost(cinfo, nid)
		if err != nil {
			logging.Errorf("Planner::getIndexSettings: Error from initializing indexer node. Error = %v", err)
			return err
		}

		// look up the corresponding indexer object based on the nodeId
		indexer := findIndexerByNodeId(plan.Placement, nodeId)
		if indexer == nil {
			logging.Verbosef("Planner::getIndexSettings: Skip indexer %v since it is not in the included list")
			continue
		}

		// obtain the admin port for the indexer node
		addr, err := cinfo.GetServiceAddress(nid, common.INDEX_HTTP_SERVICE, true)
		if err != nil {
			logging.Errorf("Planner::getIndexSettings: Error from getting service address for node %v. Error = %v", nodeId, err)
			return err
		}

		// Read the index settings from the indexer node.
		settings, err := getLocalSettings(addr)
		if err != nil {
			logging.Errorf("Planner::getIndexSettings: Error from reading index settings for node %v. Error = %v", nodeId, err)
			return err
		}

		// Find the cpu quota from setting.  If it is set to 0, then find out avail core on the node.
		quota, ok := settings["indexer.settings.max_cpu_percent"]
		if !ok || uint64(quota.(float64)) == 0 {
			plan.CpuQuota = uint64(runtime.NumCPU())
		} else {
			plan.CpuQuota = uint64(quota.(float64) / 100)
		}

		return nil
	}

	return nil
}

//
// This function extract the topology metadata for a bucket.
//
func findTopologyByBucket(topologies []mc.IndexTopology, bucket string) *mc.IndexTopology {

	for _, topology := range topologies {
		if topology.Bucket == bucket {
			return &topology
		}
	}

	return nil
}

//
// This function creates an indexer node for plan
//
func createIndexerNode(cinfo *common.ClusterInfoCache, nid common.NodeId) (*IndexerNode, error) {

	host, err := getIndexerHost(cinfo, nid)
	if err != nil {
		return nil, err
	}

	sizing := newGeneralSizingMethod()
	return newIndexerNode(host, sizing), nil
}

//
// This function gets the indexer host name from ClusterInfoCache.
//
func getIndexerHost(cinfo *common.ClusterInfoCache, nid common.NodeId) (string, error) {

	addr, err := cinfo.GetServiceAddress(nid, "mgmt", true)
	if err != nil {
		return "", err
	}

	host, port, err := net.SplitHostPort(addr)
	if err == nil {
		if host == "localhost" {
			// If this is called within a process triggered by ns-server, we
			// should get an IP address instead of localhost from clusterInfoCache.
			// If we get localhost, try to resolve the IP.   If this function is
			// called from indexer, we will know if it is IPv4 or IPv6.  But if
			// this is called outside the indexer, we will assume it is ipv4.
			addr = net.JoinHostPort(common.GetLocalIpAddr(common.IsIpv6()), port)
		}
	}

	return addr, nil
}

//
// This function gets the metadata for a specific indexer host.
//
func getLocalMetadata(addr string) (*LocalIndexMetadata, error) {

	resp, err := getWithCbauth(addr + "/getLocalIndexMetadata")
	if err != nil {
		return nil, err
	}

	localMeta := new(LocalIndexMetadata)
	if err := convertResponse(resp, localMeta); err != nil {
		return nil, err
	}

	return localMeta, nil
}

//
// This function gets the indexer stats for a specific indexer host.
//
func getLocalStats(addr string) (*common.Statistics, error) {

	resp, err := getWithCbauth(addr + "/stats?async=false&partition=true")
	if err != nil {
		logging.Warnf("Planner.getLocalStats(): Unable to get the most recent stats.  Try fetch cached stats.")
		resp, err = getWithCbauth(addr + "/stats?async=true&partition=true")
		if err != nil {
			return nil, err
		}
	}

	stats := new(common.Statistics)
	if err := convertResponse(resp, stats); err != nil {
		return nil, err
	}

	return stats, nil
}

//
// This function gets the create tokens for a specific indexer host.
//
func getLocalCreateTokens(addr string) (*mc.CreateCommandTokenList, error) {

	resp, err := getWithCbauth(addr + "/listCreateTokens")
	if err != nil {
		return nil, err
	}

	tokens := new(mc.CreateCommandTokenList)
	if err := convertResponse(resp, tokens); err != nil {
		return nil, err
	}

	return tokens, nil
}

//
// This function gets the delete tokens for a specific indexer host.
//
func getLocalDeleteTokens(addr string) (*mc.DeleteCommandTokenList, error) {

	resp, err := getWithCbauth(addr + "/listDeleteTokens")
	if err != nil {
		return nil, err
	}

	tokens := new(mc.DeleteCommandTokenList)
	if err := convertResponse(resp, tokens); err != nil {
		return nil, err
	}

	return tokens, nil
}

//
// This function gets the drop instance tokens for a specific indexer host.
//
func getLocalDropInstanceTokens(addr string) (*mc.DropInstanceCommandTokenList, error) {

	resp, err := getWithCbauth(addr + "/listDropInstanceTokens")
	if err != nil {
		return nil, err
	}

	tokens := new(mc.DropInstanceCommandTokenList)
	if err := convertResponse(resp, tokens); err != nil {
		return nil, err
	}

	return tokens, nil
}

//
// This function gets the indexer settings for a specific indexer host.
//
func getLocalSettings(addr string) (map[string]interface{}, error) {

	resp, err := getWithCbauth(addr + "/settings")
	if err != nil {
		return nil, err
	}

	settings := make(map[string]interface{})
	if err := convertResponse(resp, &settings); err != nil {
		return nil, err
	}

	return settings, nil
}

//
// This function gets the num replica for a specific indexer host.
//
func getLocalNumReplicas(addr string) (map[common.IndexDefnId]common.Counter, error) {

	resp, err := getWithCbauth(addr + "/listReplicaCount")
	if err != nil {
		return nil, err
	}

	numReplicas := make(map[common.IndexDefnId]common.Counter)
	if err := convertResponse(resp, &numReplicas); err != nil {
		return nil, err
	}

	return numReplicas, nil
}

func getWithCbauth(url string) (*http.Response, error) {

	t := GetRestRequestTimeout()
	params := &security.RequestParams{Timeout: time.Duration(t) * time.Second}
	response, err := security.GetWithAuth(url, params)
	if err == nil && response.StatusCode != http.StatusOK {
		return response, convertError(response)
	}

	return response, err
}

//
// This function unmarshalls a response.
//
func convertResponse(r *http.Response, resp interface{}) error {
	defer r.Body.Close()

	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(r.Body); err != nil {
		return err
	}

	if err := json.Unmarshal(buf.Bytes(), resp); err != nil {
		return err
	}

	return nil
}

func convertError(r *http.Response) error {
	if r.StatusCode != http.StatusOK {
		if r.Body != nil {
			defer r.Body.Close()

			buf := new(bytes.Buffer)
			if cause, err := buf.ReadFrom(r.Body); err == nil {
				return fmt.Errorf("response status:%v cause:%v", r.StatusCode, string(cause))
			}
		}
		return fmt.Errorf("response status:%v cause:Unknown", r.StatusCode)
	}

	return nil
}

//
// This function find a matching indexer host given the nodeId.
//
func findIndexerByNodeId(indexers []*IndexerNode, nodeId string) *IndexerNode {

	for _, node := range indexers {
		if node.NodeId == nodeId {
			return node
		}
	}

	return nil
}

//
// The planner is called every rebalance, but it is not guaranteed that rebalance cleanup is completed before another
// rebalance start.  Therefore, planner needs to clean up the index based on rebalancer clean up logic.
// 1) REBAL_PENDING index will not become ACTIVE during clean up.
// 2) REBAL_MERGED index can be ignored (it can be replaced with another REBAL_ACTIVE partition).
// 3) Higher version partition/indexUsage wins
//
func cleanseIndexLayout(indexers []*IndexerNode) {

	for _, indexer := range indexers {
		newIndexes := make([]*IndexUsage, 0, len(indexer.Indexes))

		for _, index := range indexer.Indexes {

			if index.Instance == nil {
				continue
			}

			// ignore any index with RState being pending.
			// **For pre-spock backup, RState of an instance is ACTIVE (0).
			if index.Instance.RState != common.REBAL_ACTIVE {
				logging.Infof("Planner: Skip index (%v, %v, %v, %v) that is not RState ACTIVE (%v)",
					index.Bucket, index.Name, index.InstId, index.PartnId, index.Instance.RState)
				continue
			}

			// find another instance with a higher instance version.
			// **For pre-spock backup, inst version is always 0. In fact, there should not be another instance (max == inst).
			max := findMaxVersionInst(indexers, index.DefnId, index.PartnId, index.InstId)
			if max != index {
				logging.Infof("Planner:  Skip index (%v, %v, %v, %v) with lower version number %v.",
					index.Bucket, index.Name, index.InstId, index.PartnId, index.Instance.Version)
				continue
			}

			newIndexes = append(newIndexes, index)
		}

		indexer.Indexes = newIndexes
	}
}

func findMaxVersionInst(indexers []*IndexerNode, defnId common.IndexDefnId, partnId common.PartitionId, instId common.IndexInstId) *IndexUsage {

	var max *IndexUsage

	for _, indexer := range indexers {
		for _, index := range indexer.Indexes {

			if index.Instance == nil {
				continue
			}

			// ignore any index with RState being pending
			if index.Instance.RState != common.REBAL_ACTIVE {
				continue
			}

			if index.DefnId == defnId && index.PartnId == partnId && index.InstId == instId {
				if max == nil || index.Instance.Version > max.Instance.Version {
					max = index
				}
			}
		}
	}

	return max
}

//
// There may be create token that has yet to process.  Update the indexer layout based on token information.
//
func processCreateToken(indexers []*IndexerNode, config common.Config) error {

	cinfo := cinfoClient.GetClusterInfoCache()
	cinfo.RLock()
	defer cinfo.RUnlock()

	clusterVersion := cinfo.GetClusterVersion()
	if clusterVersion < common.INDEXER_55_VERSION {
		logging.Infof("Planner::Cluster in upgrade.  Skip fetching create token.")
		return nil
	}

	// find all nodes that has a index http service
	nids := cinfo.GetNodesByServiceType(common.INDEX_HTTP_SERVICE)

	for _, nid := range nids {

		// Find the indexer host name
		nodeId, err := getIndexerHost(cinfo, nid)
		if err != nil {
			logging.Errorf("Planner::processCreateToken: Error from initializing indexer node. Error = %v", err)
			return err
		}

		// look up the corresponding indexer object based on the nodeId
		indexer := findIndexerByNodeId(indexers, nodeId)
		if indexer == nil {
			logging.Verbosef("Planner::processCreateToken: Skip indexer %v since it is not in the included list")
			continue
		}

		// obtain the admin port for the indexer node
		addr, err := cinfo.GetServiceAddress(nid, common.INDEX_HTTP_SERVICE, true)
		if err != nil {
			logging.Errorf("Planner::processCreateToken: Error from getting service address for node %v. Error = %v", nodeId, err)
			return err
		}

		// Read the create token from the indexer node using REST.  This is to ensure that it can read the token from the node
		// that place the token.   If that node is partitioned away, then it will rely on other nodes that have got the token.
		// If there is no node that can provide the token,
		// 1) the planner will not consider those pending-create index for planning
		// 2) the planner will not move those pending-create index from the out-node.
		tokens, err := getLocalCreateTokens(addr)
		if err != nil {
			logging.Errorf("Planner::processCreateToken: Error from reading create tokens for node %v. Error = %v", nodeId, err)
			return err
		}

		// nothing to do
		if len(tokens.Tokens) == 0 {
			logging.Infof("Planner::processCreateToken: There is no create token to process for node %v", nodeId)
			continue
		}

		findPartition := func(instId common.IndexInstId, partitionId common.PartitionId) bool {
			for _, indexer := range indexers {
				for _, index := range indexer.Indexes {
					if index.InstId == instId && index.PartnId == partitionId {
						return true
					}
				}
			}
			return false
		}

		makeIndexUsage := func(defn *common.IndexDefn, partition common.PartitionId) *IndexUsage {
			index := makeIndexUsageFromDefn(defn, defn.InstId, partition, uint64(defn.NumPartitions))

			numVbuckets := config["indexer.numVbuckets"].Int()
			pc := common.NewKeyPartitionContainer(numVbuckets, int(defn.NumPartitions), defn.PartitionScheme, defn.HashScheme)

			index.Instance = &common.IndexInst{
				InstId:    defn.InstId,
				Defn:      *defn,
				State:     common.INDEX_STATE_READY,
				Stream:    common.NIL_STREAM,
				Error:     "",
				ReplicaId: defn.ReplicaId,
				Version:   0,
				RState:    common.REBAL_ACTIVE,
				Pc:        pc,
			}

			// For planning, caller can specify a node list.  This will only add the pending-create index
			// if it is in the node list.  Indexers outside of node list will not be considered for planning purpose.
			// For rebalancing, it will not repair pending-create index on a failed node.  But it will move pending-create
			// index for out-nodes.
			index.pendingCreate = true

			return index
		}

		addIndex := func(indexerId common.IndexerId, index *IndexUsage) bool {
			for _, indexer := range indexers {
				if common.IndexerId(indexer.IndexerId) == indexerId {
					indexer.Indexes = append(indexer.Indexes, index)
					index.initialNode = indexer
					return true
				}
			}
			return false
		}

		logging.Infof("Planner::processCreateToken: processing %v tokens for node %v",
			len(tokens.Tokens), nodeId)

		verbose := logging.IsEnabled(logging.Verbose)
		for _, token := range tokens.Tokens {
			if verbose {
				logging.Verbosef(
					"Planner::processCreateToken: Processing create token for index %v from node: %v",
					token.DefnId, nodeId)
			}

			for indexerId, definitions := range token.Definitions {
				for _, defn := range definitions {
					for _, partition := range defn.Partitions {
						if !findPartition(defn.InstId, partition) {
							if addIndex(indexerId, makeIndexUsage(&defn, partition)) {
								logging.Infof("Planner::processCreateToken: Add index (%v, %v, %v)", defn.DefnId, defn.InstId, partition)
							}
						}
					}
				}
			}
		}
	}

	return nil
}

//
// There may be delete token that has yet to process.  Update the indexer layout based on token information.
//
func processDeleteToken(indexers []*IndexerNode) error {

	cinfo := cinfoClient.GetClusterInfoCache()
	cinfo.RLock()
	defer cinfo.RUnlock()

	clusterVersion := cinfo.GetClusterVersion()
	if clusterVersion < common.INDEXER_65_VERSION {
		logging.Infof("Planner::Cluster in upgrade.  Skip fetching delete token.")
		return nil
	}

	// find all nodes that has a index http service
	nids := cinfo.GetNodesByServiceType(common.INDEX_HTTP_SERVICE)

	for _, nid := range nids {

		// Find the indexer host name
		nodeId, err := getIndexerHost(cinfo, nid)
		if err != nil {
			logging.Errorf("Planner::processDeleteToken: Error from initializing indexer node. Error = %v", err)
			return err
		}

		// look up the corresponding indexer object based on the nodeId
		indexer := findIndexerByNodeId(indexers, nodeId)
		if indexer == nil {
			logging.Verbosef("Planner::processDeleteToken: Skip indexer %v since it is not in the included list")
			continue
		}

		// obtain the admin port for the indexer node
		addr, err := cinfo.GetServiceAddress(nid, common.INDEX_HTTP_SERVICE, true)
		if err != nil {
			logging.Errorf("Planner::processDeleteToken: Error from getting service address for node %v. Error = %v", nodeId, err)
			return err
		}

		// Read the delete token from the indexer node using REST.  This is to ensure that it can read the token from the node
		// that place the token.   If that node is partitioned away, then it will rely on other nodes that have got the token.
		// If there is no node that can provide the token,
		// 1) the planner will not consider those pending-delete index for planning
		// 2) the planner could end up repairing replica for those definitions
		tokens, err := getLocalDeleteTokens(addr)
		if err != nil {
			logging.Errorf("Planner::processDeleteToken: Error from reading delete tokens for node %v. Error = %v", nodeId, err)
			return err
		}

		// Create lookup map of deleted DefnIds from tokens
		tokenMap, err := getDeletedDefnIds(tokens, nodeId)
		if err != nil {
			return err
		}

		// nothing to do
		if len(tokenMap) == 0 {
			logging.Infof("Planner::processDeleteToken: There is no delete token to process for node %v", nodeId)
			continue
		}

		logging.Infof("Planner::processDeleteToken: processing %v tokens for node %v",
			len(tokenMap), nodeId)

		// Remove any deleted indexes from indexer.Indexes
		for _, indexer := range indexers {
			indexes := make([]*IndexUsage, 0, len(indexer.Indexes))
			for _, index := range indexer.Indexes {
				_, exists := tokenMap[index.DefnId]
				if !exists {
					indexes = append(indexes, index)
				} else { // token matches index
					logging.Infof(
						"Planner::processDeleteToken: Remove index (%v, %v)",
						index.GetDisplayName(), index.Bucket)
				}
			}
			indexer.Indexes = indexes
		}
	}

	return nil
}

// getDeletedDefnIds is a helper for processDeleteToken that returns a map whose
// keys are the DefnIds of indexes deleted by the delete tokens.
func getDeletedDefnIds(tokens *mc.DeleteCommandTokenList, nodeId string) (
	map[common.IndexDefnId]bool, error) {

	tokenMap := make(map[common.IndexDefnId]bool) // return value

	// Extract defnIds and put into lookup map
	verbose := logging.IsEnabled(logging.Verbose)
	for _, token := range tokens.Tokens {
		tokenMap[token.DefnId] = true
		if verbose {
			logging.Verbosef("Planner::getDeletedDefnIds: Received delete token for index %v from node %v", token.DefnId, nodeId)
		}
	}

	return tokenMap, nil
}

//
// There may be drop instance token that has yet to process.  Update the indexer layout based on token information.
//
func processDropInstanceToken(indexers []*IndexerNode,
	replicaIdMap map[common.IndexDefnId]map[int]bool) error {

	cinfo := cinfoClient.GetClusterInfoCache()
	cinfo.RLock()
	defer cinfo.RUnlock()

	clusterVersion := cinfo.GetClusterVersion()
	if clusterVersion < common.INDEXER_65_VERSION {
		logging.Infof("Planner::Cluster in upgrade.  Skip fetching drop instance token.")
		return nil
	}

	// find all nodes that has a index http service
	nids := cinfo.GetNodesByServiceType(common.INDEX_HTTP_SERVICE)

	for _, nid := range nids {

		// Find the indexer host name
		nodeId, err := getIndexerHost(cinfo, nid)
		if err != nil {
			logging.Errorf("Planner::processDropInstanceToken: Error from initializing indexer node. Error = %v", err)
			return err
		}

		// look up the corresponding indexer object based on the nodeId
		indexer := findIndexerByNodeId(indexers, nodeId)
		if indexer == nil {
			logging.Verbosef("Planner::processDropInstanceToken: Skip indexer %v since it is not in the included list")
			continue
		}

		// obtain the admin port for the indexer node
		addr, err := cinfo.GetServiceAddress(nid, common.INDEX_HTTP_SERVICE, true)
		if err != nil {
			logging.Errorf("Planner::processDropInstanceToken: Error from getting service address for node %v. Error = %v", nodeId, err)
			return err
		}

		// Read the drop instance token from the indexer node using REST.  This is to ensure that it can read the token from the node
		// that place the token.   If that node is partitioned away, then it will rely on other nodes that have got the token.
		// If there is no node that can provide the token,
		// 1) the planner will not consider those pending-delete index for planning
		// 2) the planner could end up repairing replica for those definitions
		// 3) when handling drop replica, it may not drop an already deleted replica
		tokens, err := getLocalDropInstanceTokens(addr)
		if err != nil {
			logging.Errorf("Planner::processDropInstanceToken: Error from reading drop instance tokens for node %v. Error = %v", nodeId, err)
			return err
		}

		// nothing to do
		if len(tokens.Tokens) == 0 {
			logging.Infof("Planner::processDropInstanceToken: There is no drop instance token to process for node %v", nodeId)
			continue
		}

		logging.Infof("Planner::processDropInstanceToken: processing %v tokens for node %v",
			len(tokens.Tokens), nodeId)

		// Create lookup map of the token keys, cashing ReplicaId in the value
		tokenMap := make(map[tokenKey]int, len(tokens.Tokens))
		verbose := logging.IsEnabled(logging.Verbose)
		for _, token := range tokens.Tokens {
			tokenMap[tokenKey{token.DefnId, token.InstId}] = token.ReplicaId
			if verbose {
				logging.Verbosef("Planner::processDropInstanceToken: Received drop instance token for index %v (replicaId %v inst %v) from node %v",
					token.DefnId, token.ReplicaId, token.InstId, nodeId)
			}
		}

		// Remove any dropped instances from indexer.Indexes and update replicaIdMap
		for _, indexer := range indexers {
			indexes := make([]*IndexUsage, 0, len(indexer.Indexes))
			for _, index := range indexer.Indexes {
				replicaId, exists := tokenMap[tokenKey{index.DefnId, index.InstId}]
				if !exists {
					indexes = append(indexes, index)
				} else { // token matches index
					replicaIdMap[index.DefnId][replicaId] = true
					logging.Infof(
						"Planner::processDropInstanceToken: Remove index (%v, %v)",
						index.GetDisplayName(), index.Bucket)
				}
			}
			indexer.Indexes = indexes
		}
	}

	return nil
}

//
// get index numReplica
//
func getIndexNumReplica(plan *Plan) error {
	cinfo := cinfoClient.GetClusterInfoCache()
	cinfo.RLock()
	defer cinfo.RUnlock()

	clusterVersion := cinfo.GetClusterVersion()
	if clusterVersion < common.INDEXER_65_VERSION {
		logging.Infof("Planner::Cluster in upgrade.  Skip fetching drop instance token.")
		return nil
	}

	// find all nodes that has a index http service
	nids := cinfo.GetNodesByServiceType(common.INDEX_HTTP_SERVICE)

	if len(nids) == 0 {
		return errors.New("No indexing service available.")
	}

	numReplicas := make(map[common.IndexDefnId]common.Counter)

	for _, nid := range nids {

		// Find the indexer host name
		nodeId, err := getIndexerHost(cinfo, nid)
		if err != nil {
			logging.Errorf("Planner::getIndexNumReplica: Error from initializing indexer node. Error = %v", err)
			return err
		}

		// look up the corresponding indexer object based on the nodeId
		indexer := findIndexerByNodeId(plan.Placement, nodeId)
		if indexer == nil {
			logging.Verbosef("Planner::getIndexNumReplica: Skip indexer %v since it is not in the included list")
			continue
		}

		// obtain the admin port for the indexer node
		addr, err := cinfo.GetServiceAddress(nid, common.INDEX_HTTP_SERVICE, true)
		if err != nil {
			logging.Errorf("Planner::getIndexNumReplica: Error from getting service address for node %v. Error = %v", nodeId, err)
			return err
		}

		localNumReplicas, err := getLocalNumReplicas(addr)
		if err != nil {
			logging.Errorf("Planner::getIndexNumReplica: Error from reading index num replica for node %v. Error = %v", nodeId, err)
			return err
		}

		for defnId, numReplica1 := range localNumReplicas {
			if numReplica2, ok := numReplicas[defnId]; !ok {
				numReplicas[defnId] = numReplica1
			} else {
				newNumReplica, merged, err := numReplica2.MergeWith(numReplica1)
				if err != nil {
					logging.Errorf("Planner::getIndexNumReplica: Error merging num replica for node %v. Error = %v", nodeId, err)
					return err
				}
				if merged {
					numReplicas[defnId] = newNumReplica
				}
			}
		}
	}

	for _, indexer := range plan.Placement {
		for _, index := range indexer.Indexes {
			cached := numReplicas[index.DefnId]
			index.Instance.Defn.NumReplica, _ = cached.Value()
			index.Instance.Defn.NumReplica2 = cached
		}
	}

	return nil
}

//
// Generate a map for replicaId
//
func generateReplicaMap(indexers []*IndexerNode) map[common.IndexDefnId]map[int]bool {

	result := make(map[common.IndexDefnId]map[int]bool)
	for _, indexer := range indexers {
		for _, index := range indexer.Indexes {
			if _, ok := result[index.DefnId]; !ok {
				result[index.DefnId] = make(map[int]bool)
			}

			result[index.DefnId][index.Instance.ReplicaId] = true
		}
	}

	return result
}
