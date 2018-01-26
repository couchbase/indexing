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
	"github.com/couchbase/cbauth"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	mc "github.com/couchbase/indexing/secondary/manager/common"
	"net"
	"net/http"
	"runtime"
	"strings"
	"time"
)

//////////////////////////////////////////////////////////////
// Concrete Type/Struct
//////////////////////////////////////////////////////////////

type LocalIndexMetadata struct {
	IndexerId        string             `json:"indexerId,omitempty"`
	NodeUUID         string             `json:"nodeUUID,omitempty"`
	StorageMode      string             `json:"storageMode,omitempty"`
	LocalSettings    map[string]string  `json:"localSettings,omitempty"`
	IndexTopologies  []mc.IndexTopology `json:"topologies,omitempty"`
	IndexDefinitions []common.IndexDefn `json:"definitions,omitempty"`
}

///////////////////////////////////////////////////////
// Function
///////////////////////////////////////////////////////

//
// This function retrieves the index layout plan from a live cluster.
//
func RetrievePlanFromCluster(clusterUrl string) (*Plan, error) {

	indexers, err := getIndexLayout(clusterUrl)
	if err != nil {
		return nil, err
	}

	cleanseIndexLayout(indexers)

	// If there is no indexer, plan.Placement will be nil.
	plan := &Plan{Placement: indexers,
		MemQuota: 0,
		CpuQuota: 0,
		IsLive:   true,
	}

	err = getIndexStats(clusterUrl, plan)
	if err != nil {
		return nil, err
	}

	err = getIndexSettings(clusterUrl, plan)
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
			sizing.ComputeIndexSize(index)
		}
	}

	for _, indexer := range plan.Placement {
		sizing.ComputeIndexerSize(indexer)
	}
}

//
// This function retrieves the index layout.
//
func getIndexLayout(clusterUrl string) ([]*IndexerNode, error) {

	cinfo, err := clusterInfoCache(clusterUrl)
	if err != nil {
		logging.Errorf("Planner::getIndexLayout: Error from connecting to cluster at %v. Error = %v", clusterUrl, err)
		return nil, err
	}

	config, err := common.GetSettingsConfig(common.SystemConfig)
	if err != nil {
		logging.Errorf("Planner::getIndexLayout: Error from retrieving indexer settings. Error = %v", err)
		return nil, err
	}

	// find all nodes that has a index http service
	// If there is any indexer node that is not in active state (e.g. failover), then planner will skip those indexers.
	// Note that if the planner is invoked by the rebalancer, the rebalancer will receive callback ns_server if there is
	// an indexer node fails over while planning is happening.
	nids := cinfo.GetNodesByServiceType(common.INDEX_HTTP_SERVICE)

	list := make([]*IndexerNode, 0)
	numIndexes := 0

	for _, nid := range nids {

		// create an empty indexer object using the indexer host name
		node, err := createIndexerNode(cinfo, nid)
		if err != nil {
			logging.Errorf("Planner::getIndexLayout: Error from initializing indexer node. Error = %v", err)
			return nil, err
		}

		// assign server group
		node.ServerGroup = cinfo.GetServerGroup(nid)

		// obtain the admin port for the indexer node
		addr, err := cinfo.GetServiceAddress(nid, common.INDEX_HTTP_SERVICE)
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

			for _, partn := range inst.Partitions {

				// create an index usage object
				index := newIndexUsage(defn.DefnId, common.IndexInstId(inst.InstId), common.PartitionId(partn.PartId), defn.Name, defn.Bucket)

				// index is pinned to a node
				if len(defn.Nodes) != 0 {
					index.Hosts = defn.Nodes
				}

				// update sizing
				index.IsPrimary = defn.IsPrimary
				index.StorageMode = common.IndexTypeToStorageMode(defn.Using).String()
				index.NoUsageInfo = defn.Deferred && (state == common.INDEX_STATE_READY || state == common.INDEX_STATE_CREATED)

				// update partition
				numVbuckets := config["indexer.numVbuckets"].Int()
				pc := common.NewKeyPartitionContainer(numVbuckets, int(inst.NumPartitions), defn.PartitionScheme)

				// Is the index being deleted by user?   Thsi will read the delete token from metakv.  If untable read from metakv,
				// pendingDelete is false (cannot assert index is to-be-delete).
				pendingDelete, err := mc.DeleteCommandTokenExist(defn.DefnId)
				if err != nil {
					return nil, err
				}
				index.pendingDelete = pendingDelete

				// get the version from inst or partition
				version := partn.Version
				if version == 0 && version != inst.Version {
					version = inst.Version
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
func getIndexStats(clusterUrl string, plan *Plan) error {

	cinfo, err := clusterInfoCache(clusterUrl)
	if err != nil {
		logging.Errorf("Planner::getIndexStats: Error from connecting to cluster at %v. Error = %v", clusterUrl, err)
		return err
	}

	// find all nodes that has a index http service
	nids := cinfo.GetNodesByServiceType(common.INDEX_HTTP_SERVICE)

	for _, nid := range nids {

		// Find the indexer host name
		nodeId, err := getIndexerHost(cinfo, nid)
		if err != nil {
			logging.Errorf("Planner::getIndexStats: Error from initializing indexer node. Error = %v", err)
			return err
		}

		// obtain the admin port for the indexer node
		addr, err := cinfo.GetServiceAddress(nid, common.INDEX_HTTP_SERVICE)
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

		// look up the corresponding indexer object based on the nodeId
		indexer := findIndexerByNodeId(plan.Placement, nodeId)
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

			indexName := index.GetStatsName()
			indexName1 := index.GetInstStatsName()

			// items_count captures number of key per index
			key = fmt.Sprintf("%v:%v:items_count", index.Bucket, indexName)
			if itemsCount, ok := statsMap[key]; ok {
				index.NumOfDocs = uint64(itemsCount.(float64))
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

			// data_size is the total key size of index, excluding back index overhead.
			// Therefore data_size is typically smaller than index sizing equation which
			// includes overhead for back-index.
			key = fmt.Sprintf("%v:%v:data_size", index.Bucket, indexName)
			if dataSize, ok := statsMap[key]; ok {
				index.ActualDataSize = uint64(dataSize.(float64))
				// calibrate memory usage based on resident percent
				index.ActualMemUsage = index.ActualDataSize * index.ActualResidentPercent / 100
				totalIndexMemUsed += index.ActualMemUsage
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
				if index.NumOfDocs != 0 && index.ActualMemUsage != 0 {
					index.ActualKeySize = index.ActualMemUsage / index.NumOfDocs
				}
				if index.Instance.Defn.SecKeySize != 0 {
					index.AvgSecKeySize = index.Instance.Defn.SecKeySize
				}
				if index.Instance.Defn.DocKeySize != 0 {
					index.AvgDocKeySize = index.Instance.Defn.DocKeySize
				}
			}

			// These stats are currently unavailable in 4.5.
			key = fmt.Sprintf("%v:%v:avg_doc_key_size", index.Bucket, indexName)
			if avgDocKeySize, ok := statsMap[key]; ok {
				index.AvgDocKeySize = uint64(avgDocKeySize.(float64))
			} else if index.IsPrimary {
				// Aproximate AvgDocKeySize.  Subtract 74 bytes for main
				// index overhead
				if index.NumOfDocs != 0 && index.ActualMemUsage != 0 {
					index.ActualKeySize = index.ActualMemUsage / index.NumOfDocs
				}
				if index.Instance.Defn.DocKeySize != 0 {
					index.AvgDocKeySize = index.Instance.Defn.DocKeySize
				}
			}

			// These stats are currently unavailable in 4.5.
			key = fmt.Sprintf("%v:%v:avg_arr_size", index.Bucket, indexName)
			if avgArrSize, ok := statsMap[key]; ok {
				index.AvgArrSize = uint64(avgArrSize.(float64))
			} else if !index.IsPrimary {
				if index.Instance.Defn.IsArrayIndex && index.Instance.Defn.ArrSize != 0 {
					index.AvgArrSize = index.Instance.Defn.ArrSize
				}
			}

			// These stats are currently unavailable in 4.5.
			key = fmt.Sprintf("%v:%v:avg_arr_key_size", index.Bucket, indexName)
			if avgArrKeySize, ok := statsMap[key]; ok {
				index.AvgArrKeySize = uint64(avgArrKeySize.(float64))
			} else if !index.IsPrimary {
				if index.Instance.Defn.IsArrayIndex && index.Instance.Defn.SecKeySize != 0 {
					index.AvgArrKeySize = index.Instance.Defn.SecKeySize
				}
			}

			// These stats are currently unavailable in 4.5.
			key = fmt.Sprintf("%v:%v:avg_drain_rate", index.Bucket, indexName)
			if avgMutationRate, ok := statsMap[key]; ok {
				index.MutationRate = uint64(avgMutationRate.(float64))
				totalMutation += index.MutationRate
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

			// resident ratio
			if index.Instance.Defn.ResidentRatio != 0 {
				index.ResidentRatio = index.Instance.Defn.ResidentRatio
			} else {
				index.ResidentRatio = 100
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

			index.ActualMemUsage = uint64(float64(actualStorageMem) * ratio)

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

			if index.ActualMemUsage != 0 {
				index.NoUsageInfo = false
			}

			indexer.ActualDataSize += index.ActualDataSize
			indexer.ActualMemUsage += index.ActualMemUsage
			indexer.ActualMemOverhead += index.ActualMemOverhead
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
		//
		for _, index := range indexer.Indexes {

			mutationRatio := float64(0)
			if totalMutation != 0 {
				mutationRatio = float64(index.MutationRate) / float64(totalMutation)
			}

			scanRatio := float64(0)
			if totalScan != 0 {
				scanRatio = float64(index.ScanRate) / float64(totalScan)
			}

			ratio := mutationRatio
			if scanRatio != 0 {
				if mutationRatio != 0 {
					// mutation uses 5 times less cpu than scan
					ratio = ((mutationRatio / 5) + scanRatio) / 2
				} else {
					ratio = scanRatio
				}
			}

			usage := float64(actualCpuUtil) * ratio

			if usage > 0 {
				index.ActualCpuUsage = usage
				index.NoUsageInfo = false
			}

			indexer.ActualCpuUsage += index.ActualCpuUsage
		}
	}

	return nil
}

//
// This function retrieves the index settings.
//
func getIndexSettings(clusterUrl string, plan *Plan) error {

	cinfo, err := clusterInfoCache(clusterUrl)
	if err != nil {
		logging.Errorf("Planner::getIndexSettings: Error from connecting to cluster at %v. Error = %v", clusterUrl, err)
		return err
	}

	// find all nodes that has a index http service
	nids := cinfo.GetNodesByServiceType(common.INDEX_HTTP_SERVICE)

	if len(nids) == 0 {
		logging.Infof("Planner::getIndexSettings: No indexing service.")
		return nil
	}

	// Find the indexer host name
	nodeId, err := getIndexerHost(cinfo, nids[0])
	if err != nil {
		logging.Errorf("Planner::getIndexSettings: Error from initializing indexer node. Error = %v", err)
		return err
	}

	// obtain the admin port for the indexer node
	addr, err := cinfo.GetServiceAddress(nids[0], common.INDEX_HTTP_SERVICE)
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
// This function finds the index instance id from bucket topology.
//
func findIndexInstId(topology *mc.IndexTopology, defnId common.IndexDefnId) (common.IndexInstId, error) {

	for _, defnRef := range topology.Definitions {
		if defnRef.DefnId == uint64(defnId) {
			for _, inst := range defnRef.Instances {
				return common.IndexInstId(inst.InstId), nil
			}
		}
	}

	return common.IndexInstId(0), errors.New(fmt.Sprintf("Cannot find index instance id for defnition %v", defnId))
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

	addr, err := cinfo.GetServiceAddress(nid, "mgmt")
	if err != nil {
		return "", err
	}

	host, port, err := net.SplitHostPort(addr)
	if err == nil {
		if host == "localhost" {
			addr = net.JoinHostPort("127.0.0.1", port)
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
		return nil, err
	}

	stats := new(common.Statistics)
	if err := convertResponse(resp, stats); err != nil {
		return nil, err
	}

	return stats, nil
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

func getWithCbauth(url string) (*http.Response, error) {

	if !strings.HasPrefix(url, "http://") {
		url = "http://" + url
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	cbauth.SetRequestAuthVia(req, nil)

	client := http.Client{Timeout: time.Duration(10 * time.Second)}
	return client.Do(req)
}

//
// This function gets a pointer to clusterInfoCache.
//
func clusterInfoCache(clusterUrl string) (*common.ClusterInfoCache, error) {

	url, err := common.ClusterAuthUrl(clusterUrl)
	if err != nil {
		return nil, err
	}

	cinfo, err := common.NewClusterInfoCache(url, "default")
	if err != nil {
		return nil, err
	}

	if err := cinfo.Fetch(); err != nil {
		return nil, err
	}

	return cinfo, nil
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
// The planner is called every rebalance, but it is guaranteed that rebalance cleanup is completed before another
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
