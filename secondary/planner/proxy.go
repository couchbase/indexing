// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package planner

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"runtime"
	"strconv"
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

// This mutex will protect cinfoClient from multiple initializations
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

// TODO: Index Topology related changes (if any)

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
// This function retrieves the current index layout from a live cluster.
// This function uses REST API to retrieve index metadata, instead of
// using metadata provider.  This method should not use metadata provider
// since this method can be called from metadata provider, so it is to
// avoid code cyclic dependency.
//
func RetrievePlanFromCluster(clusterUrl string, hosts []string, isRebalance bool) (*Plan, error) {

	config, err := common.GetSettingsConfig(common.SystemConfig)
	if err != nil {
		logging.Errorf("Planner::getIndexLayout: Error from retrieving indexer settings, err: %v", err)
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
	if err != nil { // Error while initializing clusterInfoClient
		logging.Errorf("Planner::RetrievePlanFromCluster: Error while initializing cluster info client at %v, err:  %v", clusterUrl, err)
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

	// GOMAXPROCS is now set to the logical minimum of
	//   1. runtime.NumCPU
	//   2. SigarControlGroupInfo.NumCpuPrc / 100 (only included if cgroups are supported)
	//   3. indexer.settings.max_cpu_percent / 100 (current Indexer Threads UI setting)
	// We assume all Index nodes are configured the same w.r.t the above. It is impossible to
	// configure #3 differently across nodes, but it is possible, though not advised, to configure
	// #1 and #2 differently. The resulting CpuQuota is correct if Planner is running directly on
	// an Index node and they are all configured the same, but it will not pick up #3 if it is
	// running on a non-Index node (e.g. Query not colocated with Index) and the result in this case
	// may also be based on a different value for #2 than Index nodes use.
	//
	// Currently plan.CpuQuota is not actually used other than to be logged in some messages, so we
	// have not spent time making this more sophisticated. Note also there is only one plan.CpuQuota
	// value, so if nodes are configured differently it is not clear which one's value to use.
	//
	// If CpuQuota constraints are ever re-enabled, we will need to revisit the above questions. We
	// could implement a means of retrieving GOMAXPROCS from all Index nodes, but we'd also need to
	// address the issue of them possibly being different, e.g. by enhancing Planner to save
	// CpuQuota on a per-node basis and related enhancements to how it uses these values.
	plan.CpuQuota = uint64(runtime.GOMAXPROCS(0))

	err = getIndexNumReplica(plan, isRebalance)
	if err != nil {
		return nil, err
	}

	// Recalculate the index and indexer memory and cpu usage using the sizing formula.
	// The stats retrieved from indexer typically has lower memory/cpu utilization than
	// sizing formula, since sizing formula captures max usage capacity. By recalculating
	// the usage, it makes sure that planning does not partially skewed data.
	recalculateIndexerSize(plan)

	return plan, nil
}

//
// This function recalculates the index and indexer sizes based on sizing formula.
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

	list := make([]*IndexerNode, 0)
	numIndexes := 0

	resp, err := restHelperNoLock(getLocalMetadataResp, hosts, nil, cinfo, "LocalMetadataResp")
	if err != nil {
		return nil, err
	}

	var delTokens map[common.IndexDefnId]*mc.DeleteCommandToken
	delTokens, err = mc.FetchIndexDefnToDeleteCommandTokensMap()
	if err != nil {
		logging.Errorf("Planner::getIndexLayout: Error in FetchIndexDefnToDeleteCommandTokensMap %v", err)
		return nil, err
	}

	var buildTokens map[common.IndexDefnId]*mc.BuildCommandToken
	buildTokens, err = mc.FetchIndexDefnToBuildCommandTokensMap()
	if err != nil {
		logging.Errorf("Planner::getIndexLayout: Error in FetchIndexDefnToBuildCommandTokensMap %v", err)
		return nil, err
	}

	for nid, res := range resp {

		// create an empty indexer object using the indexer host name
		node, err := createIndexerNode(cinfo, nid)
		if err != nil {
			logging.Errorf("Planner::getIndexLayout: Error from initializing indexer nodeId: %v, err: %v", nid, err)
			return nil, err
		}

		localMeta := res.meta

		// assign server group
		node.ServerGroup = cinfo.GetServerGroup(nid)

		// obtain the admin port for the indexer node
		addr, err := cinfo.GetServiceAddress(nid, common.INDEX_HTTP_SERVICE, true)
		if err != nil {
			logging.Errorf("Planner::getIndexLayout: Error from getting service address for node %v, err: = %v", node.NodeId, err)
			return nil, err
		}
		node.RestUrl = addr

		// get the node UUID
		node.NodeUUID = localMeta.NodeUUID
		node.IndexerId = localMeta.IndexerId
		node.StorageMode = localMeta.StorageMode
		node.exclude = localMeta.LocalSettings["excludeNode"]

		// convert from LocalIndexMetadata to IndexUsage
		indexes, err := ConvertToIndexUsages(config, localMeta, node, buildTokens, delTokens)
		if err != nil {
			logging.Errorf("Planner::getIndexLayout: Error for converting index metadata to index usage for node %v, err: %v", node.NodeId, err)
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
// This function convert index definitions from a single metadata repository to a list of IndexUsage.
//
func ConvertToIndexUsages(config common.Config, localMeta *LocalIndexMetadata, node *IndexerNode,
	buildTokens map[common.IndexDefnId]*mc.BuildCommandToken,
	delTokens map[common.IndexDefnId]*mc.DeleteCommandToken) ([]*IndexUsage, error) {

	list := ([]*IndexUsage)(nil)

	// Iterate through all the index definition.    For each index definition, create an index usage object.
	for i := 0; i < len(localMeta.IndexDefinitions); i++ {

		defn := &localMeta.IndexDefinitions[i]
		defn.SetCollectionDefaults()

		indexes, err := ConvertToIndexUsage(config, defn, localMeta, buildTokens, delTokens)
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
// This function convert a single index definition to IndexUsage.
//
func ConvertToIndexUsage(config common.Config, defn *common.IndexDefn, localMeta *LocalIndexMetadata,
	buildTokens map[common.IndexDefnId]*mc.BuildCommandToken,
	delTokens map[common.IndexDefnId]*mc.DeleteCommandToken) ([]*IndexUsage, error) {

	// find the topology metadata
	topology := findTopologyByCollection(localMeta.IndexTopologies, defn.Bucket, defn.Scope, defn.Collection)
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

				// Is the index being deleted by user?   This will read the delete token from metakv.  If unable read from metakv,
				// pendingDelete is false (cannot assert index is to-be-delete).s
				if delTokens != nil {
					_, index.pendingDelete = delTokens[defn.DefnId]
				} else {
					pendingDelete, err := mc.DeleteCommandTokenExist(defn.DefnId)
					if err != nil {
						return nil, err
					}
					index.pendingDelete = pendingDelete
				}

				var pendingBuild bool
				if buildTokens != nil {
					_, pendingBuild = buildTokens[defn.DefnId]
				} else {
					var err error
					pendingBuild, err = mc.BuildCommandTokenExist(defn.DefnId)
					if err != nil {
						return nil, err
					}
				}

				//index can be scheduled without a build token for cases like
				//rollback to 0 or flush
				if inst.Scheduled {
					logging.Infof("Planner::ConvertToIndexUsage Set pendingBuild for inst %v", inst.InstId)
					pendingBuild = true
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

				logging.Debugf("Create Index usage %v %v %v %v %v %v %v",
					index.Name, index.Bucket, index.Scope, index.Collection,
					index.Instance.InstId, index.PartnId, index.Instance.ReplicaId)

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

	resp, err := restHelperNoLock(getLocalStatsResp, nil, plan.Placement, cinfo, "LocalStatsResp")
	if err != nil {
		return err
	}

	for nid, res := range resp {

		nodeId, err := getIndexerHost(cinfo, nid)
		if err != nil {
			logging.Errorf("Planner::getIndexStats: Error from initializing indexer nodeId: %v, err: %v", nid, err)
			return err
		}

		stats := res.stats

		// look up the corresponding indexer object based on the nodeId
		indexer := findIndexerByNodeId(plan.Placement, nodeId)
		if indexer == nil {
			logging.Errorf("Planner::getIndexStats: skipping stats collection for indexer nodeId %v, restURL %v", nodeId, indexer.RestUrl)
			continue
		}

		// Read the index stats from the indexer node.
		statsMap := stats.ToMap()

		/*
			CpuUsage    uint64 `json:"cpuUsage,omitempty"`
			DiskUsage   uint64 `json:"diskUsage,omitempty"`
		*/

		var actualStorageMem uint64
		// memory_used_storage contains the total storage consumption,
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

			// items_count captures number of key per index
			if itemsCount, ok := GetIndexStat(index, "items_count", statsMap, true, clusterVersion); ok {
				index.ActualNumDocs = uint64(itemsCount.(float64))
			}

			// build completion
			if buildProgress, ok := GetIndexStat(index, "build_progress", statsMap, false, clusterVersion); ok {
				index.ActualBuildPercent = uint64(buildProgress.(float64))
			}

			// most of the code uses resident ratio of mainstore only and they will continue to use "resident_percent" stat
			// but for planner we need to use resident ratio combined for mainstore and backstore and hence here we use "combined_resident_percent"
			if residentPercent, ok := GetIndexStat(index, "combined_resident_percent", statsMap, true, clusterVersion); ok {
				index.ActualResidentPercent = uint64(residentPercent.(float64))
			} else { // in mixed mode when combined_resident_percent is not available
				if residentPercent, ok := GetIndexStat(index, "resident_percent", statsMap, true, clusterVersion); ok {
					index.ActualResidentPercent = uint64(residentPercent.(float64))
				}
			}

			// data_size is the total key size of index, including back index.
			// data_size for MOI is 0.
			if dataSize, ok := GetIndexStat(index, "data_size", statsMap, true, clusterVersion); ok {
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
			if memUsed, ok := GetIndexStat(index, "memory_used", statsMap, true, clusterVersion); ok {
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
			if diskUsed, ok := GetIndexStat(index, "avg_disk_bps", statsMap, true, clusterVersion); ok {
				index.ActualDiskUsage = uint64(diskUsed.(float64))
			}

			if numDocsQueued, ok := GetIndexStat(index, "num_docs_queued", statsMap, true, clusterVersion); ok {
				index.numDocsQueued = int64(numDocsQueued.(float64))
			}

			if numDocsPending, ok := GetIndexStat(index, "num_docs_pending", statsMap, true, clusterVersion); ok {
				index.numDocsPending = int64(numDocsPending.(float64))
			}

			if rollbackTime, ok := GetIndexStat(index, "last_rollback_time", statsMap, true, clusterVersion); ok {
				if rollback, err := strconv.ParseInt(rollbackTime.(string), 10, 64); err == nil {
					index.rollbackTime = rollback
				} else {
					logging.Errorf("Planner::getIndexStats: Error in converting last_rollback_time %v, err %v", rollbackTime, err)
				}
			}

			if progressStatTime, ok := GetIndexStat(index, "progress_stat_time", statsMap, true, clusterVersion); ok {
				if progress, err := strconv.ParseInt(progressStatTime.(string), 10, 64); err == nil {
					index.progressStatTime = progress
				} else {
					logging.Errorf("Planner::getIndexStats: Error in converting progress_stat_time %v, err %v", progressStatTime, err)
				}
			}

			// avg_sec_key_size is currently unavailable in 4.5.   To estimate,
			// the key size, it divides index data_size by items_count.  This
			// contains sec key size + doc key size + main index overhead (74 bytes).
			// Subtract 74 bytes to get sec key size.
			if avgSecKeySize, ok := GetIndexStat(index, "avg_sec_key_size", statsMap, true, clusterVersion); ok {
				index.AvgSecKeySize = uint64(avgSecKeySize.(float64))
			} else if !index.IsPrimary {
				// Approximate AvgSecKeySize.   AvgSecKeySize includes both
				// sec key len + doc key len
				if index.ActualNumDocs != 0 && index.ActualDataSize != 0 {
					index.ActualKeySize = index.ActualDataSize / index.ActualNumDocs
				}
			}

			// These stats are currently unavailable in 4.5.
			if avgDocKeySize, ok := GetIndexStat(index, "avg_doc_key_size", statsMap, true, clusterVersion); ok {
				index.AvgDocKeySize = uint64(avgDocKeySize.(float64))
			} else if index.IsPrimary {
				// Approximate AvgDocKeySize.  Subtract 74 bytes for main
				// index overhead
				if index.ActualNumDocs != 0 && index.ActualDataSize != 0 {
					index.ActualKeySize = index.ActualDataSize / index.ActualNumDocs
				}
			}

			// These stats are currently unavailable in 4.5.
			if avgArrSize, ok := GetIndexStat(index, "avg_arr_size", statsMap, true, clusterVersion); ok {
				index.AvgArrSize = uint64(avgArrSize.(float64))
			}

			// These stats are currently unavailable in 4.5.
			if avgArrKeySize, ok := GetIndexStat(index, "avg_arr_key_size", statsMap, true, clusterVersion); ok {
				index.AvgArrKeySize = uint64(avgArrKeySize.(float64))
			}

			// avg_drain_rate computes the actual number of rows persisted to storage.
			// This does not include incoming rows that are filtered out by back-index.
			// These stats are currently unavailable in 4.5.
			if avgDrainRate, ok := GetIndexStat(index, "avg_drain_rate", statsMap, true, clusterVersion); ok {
				index.DrainRate = uint64(avgDrainRate.(float64))
			}

			// avg_mutation_rate computes the incoming number of docs.
			// This does not include the individual element in an array index.
			// These stats are currently unavailable in 4.5.
			if avgMutationRate, ok := GetIndexStat(index, "avg_mutation_rate", statsMap, true, clusterVersion); ok {
				index.MutationRate = uint64(avgMutationRate.(float64))
			} else {
				if flushQueuedStat, ok := GetIndexStat(index, "num_flush_queued", statsMap, true, clusterVersion); ok {
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
			if avgScanRate, ok := GetIndexStat(index, "avg_scan_rate", statsMap, true, clusterVersion); ok {
				index.ScanRate = uint64(avgScanRate.(float64))
				totalScan += index.ScanRate
			} else if avgScanRate, ok := GetIndexStat(index, "avg_scan_rate", statsMap, false, clusterVersion); ok {
				index.ScanRate = uint64(avgScanRate.(float64))
				totalScan += index.ScanRate
			} else {
				if rowReturnedStat, ok := GetIndexStat(index, "num_rows_returned", statsMap, true, clusterVersion); ok {
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
		// CPU usage can be 0 if
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
// This function extract the topology metadata for a bucket, scope and collection.
//
func findTopologyByCollection(topologies []mc.IndexTopology, bucket, scope, collection string) *mc.IndexTopology {

	for _, topology := range topologies {
		t := &topology
		t.SetCollectionDefaults()
		if t.Bucket == bucket && t.Scope == scope && t.Collection == collection {
			return t
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
// This function gets the marshalled metadata for a specific indexer host.
//
func getLocalMetadataResp(addr string) (*http.Response, error) {

	resp, err := getWithCbauth(addr + "/getLocalIndexMetadata?useETag=false")
	if err != nil {
		logging.Errorf("Planner::getLocalMetadataResp: Failed to get local index metadata from node: %v, err: %v", addr, err)
		return nil, err
	}

	return resp, nil
}

//
// This function gets the marshalled index stats from a specific indexer host.
//
func getLocalStatsResp(addr string) (*http.Response, error) {

	resp, err := getWithCbauth(addr + "/stats?async=false&partition=true&consumerFilter=planner")
	if err != nil {
		logging.Warnf("Planner::getLocalStatsResp: Failed to get the most recent stats from node: %v, err: %v"+
			" Try fetch cached stats.", addr, err)
		resp, err = getWithCbauth(addr + "/stats?async=true&partition=true&consumerFilter=planner")
		if err != nil {
			logging.Errorf("Planner::getLocalStatsResp: Failed to get stats from node: %v, err: %v", addr, err)
			return nil, err
		}
	}

	return resp, nil
}

//
// This function gets the marshalled list of create tokens in metakv
// on a specific indexer host.
//
func getLocalCreateTokensResp(addr string) (*http.Response, error) {

	resp, err := getWithCbauth(addr + "/listCreateTokens")
	if err != nil {
		logging.Errorf("Planner::getLocalCreateTokensResp: Failed to get create tokens from node: %v, err: %v", addr, err)
		return nil, err
	}

	return resp, nil
}

//
// getLocalDeleteTokensResp gets the marshalled list of delete tokens from metakv
// on a specific indexer host. Used only in pre-7.0 clusters.
//
func getLocalDeleteTokensResp(addr string) (*http.Response, error) {

	resp, err := getWithCbauth(addr + "/listDeleteTokens")
	if err != nil {
		logging.Errorf("Planner::getLocalDeleteTokensResp: Failed to get delete tokens from node: %v, err: %v", addr, err)
		return nil, err
	}

	return resp, nil
}

//
// getLocalDeleteTokenPathsResp gets the marshalled list of delete token paths
// from metakv on a specific indexer host. All the needed info is in the paths,
// so we do not need to retrieve the tokens, which is much more expensive.
//
func getLocalDeleteTokenPathsResp(addr string) (*http.Response, error) {

	resp, err := getWithCbauth(addr + "/listDeleteTokenPaths")
	if err != nil {
		logging.Errorf(
			"Planner::getLocalDeleteTokenPathsResp: Failed to get delete token paths from node: %v, err: %v",
			addr, err)
		return nil, err
	}

	return resp, nil
}

//
// This function gets the marshalled list of drop instance tokens in metakv
// on a specific indexer host.
//
func getLocalDropInstanceTokensResp(addr string) (*http.Response, error) {

	resp, err := getWithCbauth(addr + "/listDropInstanceTokens")
	if err != nil {
		logging.Errorf("Planner::getLocalDropInstanceTokensResp: Failed to get drop instance tokens from node: %v, err: %v", addr, err)
		return nil, err
	}

	return resp, nil
}

//
// This function gets the marshalled num replica for a specific indexer host.
//
func getLocalNumReplicasResp(addr string) (*http.Response, error) {

	resp, err := getWithCbauth(addr + "/listReplicaCount")
	if err != nil {
		logging.Errorf("Planner::getLocalNumReplicasResp: Failed to get num replicas from node: %v, err: %v", addr, err)
		return nil, err
	}

	return resp, nil
}

func getWithCbauth(url string) (*http.Response, error) {
	return security.GetWithAuthAndTimeout(url, GetRestRequestTimeout())
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
				logging.Infof("Planner: Skip index (%v, %v, %v, %v, %v, %v) that is not RState ACTIVE (%v)",
					index.Bucket, index.Scope, index.Collection, index.Name, index.InstId, index.PartnId,
					index.Instance.RState)
				continue
			}

			// find another instance with a higher instance version.
			// **For pre-spock backup, inst version is always 0. In fact, there should not be another instance (max == inst).
			max := findMaxVersionInst(indexers, index.DefnId, index.PartnId, index.InstId)
			if max != index {
				logging.Infof("Planner:  Skip index (%v, %v, %v, %v, %v, %v) with lower version number %v.",
					index.Bucket, index.Scope, index.Collection, index.Name, index.InstId, index.PartnId,
					index.Instance.Version)
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

	// Read the create token from the indexer node using REST.  This is to ensure that it can read the token from the node
	// that place the token.   If that node is partitioned away, then it will rely on other nodes that have got the token.
	// If there is no node that can provide the token,
	// 1) the planner will not consider those pending-create index for planning
	// 2) the planner will not move those pending-create index from the out-node.
	resp, err := restHelperNoLock(getLocalCreateTokensResp, nil, indexers, cinfo, "LocalCreateTokensResp")
	if err != nil {
		return err
	}

	for nid, res := range resp {
		delete(resp, nid)

		// Find the indexer host name
		nodeId, err := getIndexerHost(cinfo, nid)
		if err != nil {
			logging.Errorf("Planner::processCreateToken: Error while getting host for nodeId: %v, err: %v", nid, err)
			return err
		}

		tokens := res.createTokens

		// nothing to do
		if len(tokens.Tokens) == 0 {
			logging.Infof("Planner::processCreateToken: There is no create token to process for node: %v", nodeId)
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
					defn.SetCollectionDefaults()

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

// getDefnIdFromDeleteTokenPath extracts the IndexDefnId from the
// metakv path (key) of a delete token.
func getDefnIdFromDeleteTokenPath(path string) (common.IndexDefnId, error) {
	pieces := strings.Split(path, "/") // even empty string will return one piece
	defnId, err := strconv.ParseUint(pieces[len(pieces)-1], 10, 64)
	if err != nil {
		return 0, err
	}
	return common.IndexDefnId(defnId), nil
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

	// Read the delete token from the indexer node using REST.  This is to ensure that it can read the token from the node
	// that place the token.   If that node is partitioned away, then it will rely on other nodes that have got the token.
	// If there is no node that can provide the token,
	// 1) the planner will not consider those pending-delete index for planning
	// 2) the planner could end up repairing replica for those definitions
	var getterFunc func(addr string) (*http.Response, error) // fn to retrieve token paths or tokens
	var respType string
	if clusterVersion >= common.INDEXER_70_VERSION {
		getterFunc = getLocalDeleteTokenPathsResp
		respType = "LocalDeleteTokenPathsResp"
	} else {
		getterFunc = getLocalDeleteTokensResp
		respType = "LocalDeleteTokensResp"
	}
	resp, err := restHelperNoLock(getterFunc, nil, indexers, cinfo, respType)
	if err != nil {
		return err
	}

	for nid, res := range resp {
		delete(resp, nid)

		// Find the indexer host name
		nodeId, err := getIndexerHost(cinfo, nid)
		if err != nil {
			logging.Errorf("Planner::processDeleteToken: Error while getting host from nodeId: %v, err: %v", nid, err)
			return err
		}

		// Create lookup map of deleted DefnIds from token paths or tokens
		tokenMap, err := getDeletedDefnIds(res, clusterVersion, nodeId)
		if err != nil {
			return err
		}

		// nothing to do
		if len(tokenMap) == 0 {
			logging.Infof("Planner::processDeleteToken: There is no delete token to process for node: %v", nodeId)
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
						"Planner::processDeleteToken: Remove index (%v, %v, %v, %v)",
						index.GetDisplayName(), index.Bucket, index.Scope, index.Collection)
				}
			}
			indexer.Indexes = indexes
		}
	}

	return nil
}

// getDeletedDefnIds is a helper for processDeleteToken that returns a map whose
// keys are the DefnIds of indexes deleted by the delete tokens. In a pre-7.0
// cluster it has to retrieve full tokens to fill this, but starting in 7.0 it
// retrieves only token paths, as these have the DefnId as the last field.
func getDeletedDefnIds(res *RestResponse, clusterVersion uint64, nodeId string) (
	map[common.IndexDefnId]bool, error) {

	var tokenPaths *mc.TokenPathList      // delete token paths in 7.0 and up
	var tokens *mc.DeleteCommandTokenList // delete tokens in pre-7.0
	var err error

	tokenMap := make(map[common.IndexDefnId]bool) // return value

	// Extract token paths or full tokens from HTTP response
	if clusterVersion >= common.INDEXER_70_VERSION {
		tokenPaths = res.delTokenPaths
	} else {
		tokens = res.delTokens
	}
	if err != nil {
		logging.Errorf("Planner::getDeletedDefnIds: Error when converting response from node: %v, err: %v", nodeId, err)
		return nil, err
	}

	// Extract defnIds and put into lookup map
	verbose := logging.IsEnabled(logging.Verbose)
	if clusterVersion >= common.INDEXER_70_VERSION {
		for _, path := range tokenPaths.Paths {
			defnId, err := getDefnIdFromDeleteTokenPath(path)
			if err != nil {
				logging.Errorf("Planner::getDeletedDefnIds: Error extracting DefnId from path %v from node %v", path, nodeId)
				return nil, err
			}
			tokenMap[defnId] = true
			if verbose {
				logging.Verbosef("Planner::getDeletedDefnIds: Received delete token for index %v from node %v", defnId, nodeId)
			}
		}
	} else { // pre-7.0 cluster
		for _, token := range tokens.Tokens {
			tokenMap[token.DefnId] = true
			if verbose {
				logging.Verbosef("Planner::getDeletedDefnIds: Received delete token for index %v from node %v", token.DefnId, nodeId)
			}
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

	// Read the drop instance token from the indexer node using REST.  This is to ensure that it can read the token from the node
	// that place the token.   If that node is partitioned away, then it will rely on other nodes that have got the token.
	// If there is no node that can provide the token,
	// 1) the planner will not consider those pending-delete index for planning
	// 2) the planner could end up repairing replica for those definitions
	// 3) when handling drop replica, it may not drop an already deleted replica
	resp, err := restHelperNoLock(getLocalDropInstanceTokensResp, nil, indexers, cinfo, "LocalDropInstanceTokensResp")
	if err != nil {
		return err
	}

	for nid, res := range resp {

		// Find the indexer host name
		nodeId, err := getIndexerHost(cinfo, nid)
		if err != nil {
			logging.Errorf("Planner::processDropInstanceToken: Error while getting host from nodeId: %v, err: %v", nid, err)
			return err
		}

		tokens := res.dropInstTokens

		// nothing to do
		if len(tokens.Tokens) == 0 {
			logging.Infof("Planner::processDropInstanceToken: There is no drop instance token to process for node: %v", nodeId)
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
						"Planner::processDropInstanceToken: Remove index (%v, %v, %v, %v)",
						index.GetDisplayName(), index.Bucket, index.Scope, index.Collection)
				}
			}
			indexer.Indexes = indexes
		}
	}

	return nil
}

// getIndexNumReplica computes the number of replicas of every index by merging their NumReplica2
// counters across all nodes. If isRebalance is true, indexes with counter merge errors are deleted
// from the plan instead of returning an error so Planner and subsequent Rebalance can proceed.
func getIndexNumReplica(plan *Plan, isRebalance bool) error {
	const _getIndexNumReplica = "Planner::getIndexNumReplica:"

	cinfo := cinfoClient.GetClusterInfoCache()
	cinfo.RLock()
	defer cinfo.RUnlock()

	clusterVersion := cinfo.GetClusterVersion()
	if clusterVersion < common.INDEXER_65_VERSION {
		logging.Infof("%v Cluster in upgrade. Skip fetching drop instance token.",
			_getIndexNumReplica)
		return nil
	}

	resp, err := restHelperNoLock(getLocalNumReplicasResp, nil, plan.Placement, cinfo, "LocalNumReplicasResp")
	if err != nil {
		return err
	}

	numReplicas := make(map[common.IndexDefnId]common.Counter)
	nodeIds := make([]string, 0)
	counterMergeErrMap := make(map[common.IndexDefnId]error)
	replicaCounterMap := make(map[common.IndexDefnId]map[string]common.Counter)
	for nid, res := range resp {
		// Find the indexer host name
		nodeId, err := getIndexerHost(cinfo, nid)
		if err != nil {
			logging.Errorf("%v Error while getting host from nodeId: %v, err: %v",
				_getIndexNumReplica, nid, err)
			return err
		}

		nodeIds = append(nodeIds, nodeId)
		localNumReplicas := res.numReplicas

		for defnId, numReplica1 := range localNumReplicas {
			if _, ok := replicaCounterMap[defnId]; !ok {
				replicaCounterMap[defnId] = make(map[string]common.Counter)
			}
			replicaCounterMap[defnId][nodeId] = numReplica1

			if numReplica2, ok := numReplicas[defnId]; !ok {
				numReplicas[defnId] = numReplica1
			} else {
				newNumReplica, merged, err := numReplica2.MergeWith(numReplica1)
				if err != nil {
					counterMergeErrMap[defnId] = err
				}
				if merged {
					numReplicas[defnId] = newNumReplica
				}
			}
		}
	}

	// Process counterMergeErrMap and report errors. In Rebalance case remove bad indexes from plan.
	if len(counterMergeErrMap) > 0 {
		logging.Errorf("%v Processed nodes in order: %v", _getIndexNumReplica, nodeIds)
		var err error
		var defnId common.IndexDefnId

		for defnId, err = range counterMergeErrMap {
			var errStr string
			counterMap := replicaCounterMap[defnId] // always exists per the code above
			for nodeId, counter := range counterMap {
				errStr += fmt.Sprintf("%v:%+v ", nodeId, counter)
			}
			logging.Errorf(
				"%v Inconsistent number of replicas for defnId %v, counter values: %v, err: %v",
				_getIndexNumReplica, defnId, errStr, err)
		}
		if !isRebalance {
			return err // one arbitrary error from counterMergeErrMap
		} else {
			// Rebalance: remove erroring indexes from plan so Planner can proceed
			rebalanceRemoveFromPlan(counterMergeErrMap, plan)
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

// rebalanceRemoveFromPlan is called only in the Rebalance case to remove any indexes whose numbers
// of replicas are inconsistent across nodes from the current index layout (plan) so Planner can
// proceed, thus allowing Rebalance to succeed instead of fail. This makes the removed indexes
// invisible to Planner so the resultant plan may be far from optimal, but this is deemed better
// than failing Rebalance.
func rebalanceRemoveFromPlan(counterMergeErrMap map[common.IndexDefnId]error, plan *Plan) {
	var keepIndexes []*IndexUsage
	for _, indexer := range plan.Placement {
		keepIndexes = nil
		for _, index := range indexer.Indexes {
			if _, member := counterMergeErrMap[index.DefnId]; !member {
				keepIndexes = append(keepIndexes, index)
			}
		}
		indexer.Indexes = keepIndexes
	}
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

type RestResponse struct {
	meta           *LocalIndexMetadata
	stats          *common.Statistics
	settings       map[string]interface{}
	createTokens   *mc.CreateCommandTokenList
	delTokens      *mc.DeleteCommandTokenList
	delTokenPaths  *mc.TokenPathList
	dropInstTokens *mc.DropInstanceCommandTokenList
	numReplicas    map[common.IndexDefnId]common.Counter

	respType string
}

func NewRestResponse(respType string) (*RestResponse, error) {

	r := &RestResponse{
		respType: respType,
	}

	switch respType {

	case "LocalMetadataResp":
		r.meta = new(LocalIndexMetadata)

	case "LocalStatsResp":
		r.stats = new(common.Statistics)

	case "LocalSettingsResp":
		m := make(map[string]interface{})
		r.settings = m

	case "LocalCreateTokensResp":
		r.createTokens = new(mc.CreateCommandTokenList)

	case "LocalDeleteTokensResp":
		r.delTokens = new(mc.DeleteCommandTokenList)

	case "LocalDeleteTokenPathsResp":
		r.delTokenPaths = new(mc.TokenPathList)

	case "LocalDropInstanceTokensResp":
		r.dropInstTokens = new(mc.DropInstanceCommandTokenList)

	case "LocalNumReplicasResp":
		m := make(map[common.IndexDefnId]common.Counter)
		r.numReplicas = m

	default:
		return nil, fmt.Errorf("NewRestResponse: Unexpected HTTP Response Type")
	}

	return r, nil
}

func (r *RestResponse) SetResponse(res *http.Response) error {

	switch r.respType {

	case "LocalMetadataResp":
		if err := security.ConvertHttpResponse(res, r.meta); err != nil {
			return err
		}

	case "LocalStatsResp":
		if err := security.ConvertHttpResponse(res, r.stats); err != nil {
			return err
		}

	case "LocalSettingsResp":
		if err := security.ConvertHttpResponse(res, &r.settings); err != nil {
			return err
		}

	case "LocalCreateTokensResp":
		if err := security.ConvertHttpResponse(res, r.createTokens); err != nil {
			return err
		}

	case "LocalDeleteTokensResp":
		if err := security.ConvertHttpResponse(res, r.delTokens); err != nil {
			return err
		}

	case "LocalDeleteTokenPathsResp":
		if err := security.ConvertHttpResponse(res, r.delTokenPaths); err != nil {
			return err
		}

	case "LocalDropInstanceTokensResp":
		if err := security.ConvertHttpResponse(res, r.dropInstTokens); err != nil {
			return err
		}

	case "LocalNumReplicasResp":
		if err := security.ConvertHttpResponse(res, &r.numReplicas); err != nil {
			return err
		}

	default:
		return fmt.Errorf("SetResponse: Unexpected HTTP Response Type")
	}

	return nil
}

//
// Helper function for sending REST requests in parallel to indexer nodes.
// This function assumes that the cinfoClient is already initialized and
// the latest information is fetched. All the callers use the same cache.
//
// IMP: Note that the callers of this function should hold cinfo lock
//
func restHelperNoLock(rest func(string) (*http.Response, error), hosts []string,
	indexers []*IndexerNode, cinfo *common.ClusterInfoCache,
	respType string) (map[common.NodeId]*RestResponse, error) {

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
	nodes := cinfo.GetNodesByServiceType(common.INDEX_HTTP_SERVICE)

	if len(nodes) == 0 {
		return nil, errors.New("No indexing service available.")
	}

	var nids []common.NodeId
	if len(hosts) != 0 {
		nids = make([]common.NodeId, 0)
		for _, nid := range nodes {
			nodeId, err := getIndexerHost(cinfo, nid)
			if err != nil {
				return nil, err
			}

			found := false
			for _, host := range hosts {
				if strings.ToLower(host) == strings.ToLower(nodeId) {
					found = true
				}
			}

			if !found {
				logging.Infof("Planner:Skip node %v since it is not in the given host list %v", nodeId, hosts)
				continue
			}

			nids = append(nids, nid)
		}
	} else if len(indexers) != 0 {
		for _, nid := range nodes {
			nodeId, err := getIndexerHost(cinfo, nid)
			if err != nil {
				return nil, err
			}

			indexer := findIndexerByNodeId(indexers, nodeId)
			if indexer == nil {
				logging.Verbosef("Planner::%v: Skip indexer %v since it is not in the included list",
					runtime.FuncForPC(reflect.ValueOf(rest).Pointer()).Name(), nid)
				continue
			}

			nids = append(nids, nid)
		}
	} else {
		nids = nodes
	}

	var mu sync.Mutex
	var wg sync.WaitGroup

	errMap := make(map[common.NodeId]error)
	respMap := make(map[common.NodeId]*RestResponse)

	for _, nid := range nids {
		// obtain the admin port for the indexer node
		addr, err := cinfo.GetServiceAddress(nid, common.INDEX_HTTP_SERVICE, true)
		if err != nil {
			logging.Errorf("Planner::restHelperNoLock: Error from getting service address for node %v. Error = %v", nid, err)
			return nil, err
		}

		restCall := func(nid common.NodeId, addr string) {
			defer wg.Done()

			var resp *http.Response
			restResp, err := NewRestResponse(respType)
			if err == nil {
				t0 := time.Now()

				resp, err = rest(addr)

				dur := time.Since(t0)
				if dur > 30*time.Second || err != nil {
					logging.Warnf("Planner::restHelperNoLock %v took %v for addr %v with err %v", respType, dur, addr, err)
				}
			}

			if err == nil {
				err = restResp.SetResponse(resp)
				if err != nil {
					logging.Errorf("Planner::restHelperNoLock SetResponse %v for addr %v with err %v", respType, addr, err)
				}
			}

			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				errMap[nid] = err
				respMap[nid] = nil
			} else {
				respMap[nid] = restResp
			}
		}

		wg.Add(1)
		go restCall(nid, addr)
	}

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	if len(errMap) != 0 {
		for _, err := range errMap {
			return nil, err
		}
	}

	return respMap, nil
}
