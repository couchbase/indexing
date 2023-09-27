package planner

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
)

type Solution struct {
	command        CommandType
	constraint     ConstraintMethod
	sizing         SizingMethod
	cost           CostMethod
	place          PlacementMethod
	isLiveData     bool
	useLiveData    bool
	disableRepair  bool
	initialPlan    bool
	numServerGroup int
	numDeletedNode int
	numNewNode     int

	// for size estimation
	estimatedIndexSize uint64
	estimate           bool
	numEstimateRun     int

	// for rebalance
	enableExclude bool

	// for resource utilization
	memMean   float64
	cpuMean   float64
	dataMean  float64
	diskMean  float64
	drainMean float64
	scanMean  float64

	// placement of indexes	in nodes
	Placement []*IndexerNode `json:"placement,omitempty"`

	// maps for non-eligible indexes
	indexSGMap       ServerGroupMap
	replicaMap       ReplicaMap
	usedReplicaIdMap UsedReplicaIdMap

	// maps for eligible indexes
	// with large number of indexes, cloning these maps after every planner
	// iteration becomes a bottleneck. As planner only moves the list of
	// eligible indexes, maps of non-eligible indexes will not change and
	// clone method can just copy the reference to the existing map.
	eligIndexSGMap       ServerGroupMap
	eligReplicaMap       ReplicaMap
	eligUsedReplicaIdMap UsedReplicaIdMap

	// constraint check
	enforceConstraint bool

	// flag for ignoring constraint check during heterogenous swap
	// rebalance while upgrading
	swapOnlyRebalance bool

	// When set to 1, planner ignores excludeNode params as well as resource
	// contraints during DDL operations
	allowDDLDuringScaleUp bool

	// Contains the slot mapping for each index that is present in the
	// cluster. DefnId -> replicaId -> PartnId -> SlotId on which the partition exists
	indexSlots map[common.IndexDefnId]map[int]map[common.PartitionId]uint64

	// Contains the list of all nodes on which the slot exists
	// slotId -> indexerNode -> replica number of the slot on the corresponding node
	slotMap map[uint64]map[*IndexerNode]int
}

// Constructor
func newSolution(constraint ConstraintMethod, sizing SizingMethod, indexers []*IndexerNode, isLive bool, useLive bool,
	disableRepair bool, allowDDLDuringScaleUp bool) *Solution {

	r := &Solution{
		constraint:            constraint,
		sizing:                sizing,
		Placement:             make([]*IndexerNode, len(indexers)),
		isLiveData:            isLive,
		useLiveData:           useLive,
		disableRepair:         disableRepair,
		estimate:              true,
		enableExclude:         true,
		indexSGMap:            make(ServerGroupMap),
		replicaMap:            make(ReplicaMap),
		usedReplicaIdMap:      make(UsedReplicaIdMap),
		eligIndexSGMap:        make(ServerGroupMap),
		eligReplicaMap:        make(ReplicaMap),
		eligUsedReplicaIdMap:  make(UsedReplicaIdMap),
		swapOnlyRebalance:     false,
		allowDDLDuringScaleUp: allowDDLDuringScaleUp,

		indexSlots: make(map[common.IndexDefnId]map[int]map[common.PartitionId]uint64),
		slotMap:    make(map[uint64]map[*IndexerNode]int),
	}

	// initialize list of indexers
	if len(indexers) == 0 {
		// create at least one indexer if none exist
		nodeId := strconv.FormatUint(uint64(rand.Uint32()), 10)
		r.addNewNode(nodeId)
		r.initialPlan = true
	} else {
		// initialize placement from the current set of indexers
		for i, _ := range indexers {
			r.Placement[i] = indexers[i].clone()
		}
	}

	r.numServerGroup = r.findNumServerGroup()

	return r
}

// Whether solution should use live data
func (s *Solution) UseLiveData() bool {
	return s.isLiveData && s.useLiveData
}

// Get the constraint method for the solution
func (s *Solution) getConstraintMethod() ConstraintMethod {
	return s.constraint
}

// Get the sizing method for the solution
func (s *Solution) getSizingMethod() SizingMethod {
	return s.sizing
}

// Add new indexer node
func (s *Solution) addNewNode(nodeId string) {

	node := newIndexerNode(nodeId, s.sizing)
	s.Placement = append(s.Placement, node)
	s.estimationOn()
}

// Move a single index from one node to another
func (s *Solution) moveIndex(source *IndexerNode, idx *IndexUsage, target *IndexerNode, meetConstraint bool) {

	sourceIndex := s.findIndexOffset(source, idx)
	if sourceIndex == -1 {
		return
	}

	// add to new node
	s.addIndex(target, idx, meetConstraint)

	// remove from old node
	s.removeIndex(source, sourceIndex)
}

// Find an indexer offset from the placement list
func (s *Solution) findIndexerOffset(node *IndexerNode) int {

	for i, indexer := range s.Placement {
		if indexer == node {
			return i
		}
	}

	return -1
}

// Find an index offset from indexer node
func (s *Solution) findIndexOffset(node *IndexerNode, index *IndexUsage) int {

	for i, idx := range node.Indexes {
		if idx == index {
			return i
		}
	}

	return -1
}

// Add index to a node
func (s *Solution) addIndex(n *IndexerNode, idx *IndexUsage, meetConstraint bool) {
	n.EvaluateNodeConstraint(s, meetConstraint, idx, nil)
	n.Indexes = append(n.Indexes, idx)

	n.AddMemUsageOverhead(s, idx.GetMemUsage(s.UseLiveData()), idx.GetMemOverhead(s.UseLiveData()), idx.GetMemMin(s.UseLiveData()))
	n.AddCpuUsage(s, idx.GetCpuUsage(s.UseLiveData()))
	n.AddDataSize(s, idx.GetDataSize(s.UseLiveData()))
	n.AddDiskUsage(s, idx.GetDiskUsage(s.UseLiveData()))
	n.AddScanRate(s, idx.GetScanRate(s.UseLiveData()))
	n.AddDrainRate(s, idx.GetDrainRate(s.UseLiveData()))

	n.EvaluateNodeStats(s)
	s.updateServerGroupMap(idx, n)
}

// Remove index from a node
func (s *Solution) removeIndex(n *IndexerNode, i int) {
	idx := n.Indexes[i]
	if i+1 < len(n.Indexes) {
		n.Indexes = append(n.Indexes[:i], n.Indexes[i+1:]...)
	} else {
		n.Indexes = n.Indexes[:i]
	}

	n.SubtractMemUsageOverhead(s, idx.GetMemUsage(s.UseLiveData()), idx.GetMemOverhead(s.UseLiveData()), idx.GetMemMin(s.UseLiveData()))
	n.SubtractCpuUsage(s, idx.GetCpuUsage(s.UseLiveData()))
	n.SubtractDataSize(s, idx.GetDataSize(s.UseLiveData()))
	n.SubtractDiskUsage(s, idx.GetDiskUsage(s.UseLiveData()))
	n.SubtractScanRate(s, idx.GetScanRate(s.UseLiveData()))
	n.SubtractDrainRate(s, idx.GetDrainRate(s.UseLiveData()))

	n.EvaluateNodeConstraint(s, false, nil, idx)
	n.EvaluateNodeStats(s)
}

// Remove indexes in topology
func (s *Solution) removeIndexes(indexes []*IndexUsage) {

	eligibles := s.place.GetEligibleIndexes()

	for _, target := range indexes {
		for _, indexer := range s.Placement {
			for i, index := range indexer.Indexes {
				if index == target {
					s.removeIndex(indexer, i)
					s.updateServerGroupMap(index, nil)
					if isEligibleIndex(index, eligibles) {
						if index.Instance != nil {
							delete(s.eligReplicaMap[index.DefnId][index.PartnId], index.Instance.ReplicaId)
						}
					} else {
						if index.Instance != nil {
							delete(s.replicaMap[index.DefnId][index.PartnId], index.Instance.ReplicaId)
						}
					}
				}
			}
		}
	}
}

// Move a single index from one node to another without constraint check
func (s *Solution) moveIndex2(source *IndexerNode, idx *IndexUsage, target *IndexerNode) {

	sourceIndex := s.findIndexOffset(source, idx)
	if sourceIndex == -1 {
		return
	}

	// add to new node
	s.addIndex2(target, idx)

	// remove from old node
	s.removeIndex2(source, sourceIndex)
}

// Add index to a node and update usage stats
func (s *Solution) addIndex2(n *IndexerNode, idx *IndexUsage) {
	n.Indexes = append(n.Indexes, idx)
	n.MandatoryQuota += idx.ActualMemUsage
	n.ActualUnits += idx.ActualUnitsUsage
}

// Remove index from a node and update usage stats
func (s *Solution) removeIndex2(n *IndexerNode, i int) {
	idx := n.Indexes[i]
	if i+1 < len(n.Indexes) {
		n.Indexes = append(n.Indexes[:i], n.Indexes[i+1:]...)
	} else {
		n.Indexes = n.Indexes[:i]
	}
	n.MandatoryQuota -= idx.ActualMemUsage
	n.ActualUnits -= idx.ActualUnitsUsage
}

// Remove replica
func (s *Solution) removeReplicas(defnId common.IndexDefnId, replicaId int) {

	eligibles := s.place.GetEligibleIndexes()

	for _, indexer := range s.Placement {
	RETRY:
		for i, index := range indexer.Indexes {
			if index.DefnId == defnId && int(index.Instance.ReplicaId) == replicaId {
				s.removeIndex(indexer, i)
				s.updateServerGroupMap(index, nil)
				if isEligibleIndex(index, eligibles) {
					if index.Instance != nil {
						delete(s.eligReplicaMap[index.DefnId][index.PartnId], index.Instance.ReplicaId)
					}
				} else {
					if index.Instance != nil {
						delete(s.replicaMap[index.DefnId][index.PartnId], index.Instance.ReplicaId)
					}
				}
				goto RETRY
			}
		}
	}
}

// Reduce minimum memory
func (s *Solution) reduceMinimumMemory() {

	for _, indexer := range s.Placement {
		var total uint64
		for _, index := range indexer.Indexes {
			if !index.NoUsageInfo {
				index.ActualMemMin = uint64(float64(index.ActualMemMin) * 0.9)
				total += index.ActualMemMin
			}
		}
		indexer.ActualMemMin = total
	}
}

// This function makes a copy of existing solution.
func (s *Solution) clone() *Solution {

	r := solutionPool.Get()

	r.command = s.command
	r.constraint = s.constraint
	r.sizing = s.sizing
	r.cost = s.cost
	r.place = s.place
	r.Placement = make([]*IndexerNode, 0, len(s.Placement))
	r.isLiveData = s.isLiveData
	r.useLiveData = s.useLiveData
	r.initialPlan = s.initialPlan
	r.numServerGroup = s.numServerGroup
	r.numDeletedNode = s.numDeletedNode
	r.numNewNode = s.numNewNode
	r.disableRepair = s.disableRepair
	r.estimatedIndexSize = s.estimatedIndexSize
	r.estimate = s.estimate
	r.numEstimateRun = s.numEstimateRun
	r.enableExclude = s.enableExclude
	r.memMean = s.memMean
	r.cpuMean = s.cpuMean
	r.dataMean = s.dataMean
	r.diskMean = s.diskMean
	r.scanMean = s.scanMean
	r.drainMean = s.drainMean
	r.enforceConstraint = s.enforceConstraint
	r.swapOnlyRebalance = s.swapOnlyRebalance
	r.indexSGMap = s.indexSGMap
	r.replicaMap = s.replicaMap
	r.usedReplicaIdMap = s.usedReplicaIdMap
	r.eligIndexSGMap = make(ServerGroupMap)
	r.eligReplicaMap = make(ReplicaMap)
	r.eligUsedReplicaIdMap = make(UsedReplicaIdMap)
	r.allowDDLDuringScaleUp = s.allowDDLDuringScaleUp

	for _, node := range s.Placement {
		if node.isDelete && len(node.Indexes) == 0 {
			continue
		}
		r.Placement = append(r.Placement, node.clone())
	}

	for defnId, partitions := range s.eligIndexSGMap {
		r.eligIndexSGMap[defnId] = make(map[common.PartitionId]map[int]string)
		for partnId, replicaIds := range partitions {
			r.eligIndexSGMap[defnId][partnId] = make(map[int]string)
			for replicaId, sg := range replicaIds {
				r.eligIndexSGMap[defnId][partnId][replicaId] = sg
			}
		}
	}

	for defnId, partitions := range s.eligReplicaMap {
		r.eligReplicaMap[defnId] = make(map[common.PartitionId]map[int]*IndexUsage)
		for partnId, replicaIds := range partitions {
			r.eligReplicaMap[defnId][partnId] = make(map[int]*IndexUsage)
			for replicaId, index := range replicaIds {
				r.eligReplicaMap[defnId][partnId][replicaId] = index
			}
		}
	}

	for defnId, replicaIds := range s.eligUsedReplicaIdMap {
		r.eligUsedReplicaIdMap[defnId] = make(map[int]bool)
		for replicaId, _ := range replicaIds {
			r.eligUsedReplicaIdMap[defnId][replicaId] = true
		}
	}

	r.indexSlots = s.indexSlots
	r.slotMap = s.slotMap

	return r
}

func (s *Solution) free() {

	for _, node := range s.Placement {
		node.free()
	}

	solutionPool.Put(s)
}

// This function update the cost stats
func (s *Solution) updateCost() {
	if s.cost != nil {
		s.memMean = s.cost.GetMemMean()
		s.cpuMean = s.cost.GetCpuMean()
		s.dataMean = s.cost.GetDataMean()
		s.diskMean = s.cost.GetDiskMean()
		s.scanMean = s.cost.GetScanMean()
		s.drainMean = s.cost.GetDrainMean()
	}
}

// This function makes a copy of existing solution.
func (s *Solution) removeEmptyDeletedNode() {

	var result []*IndexerNode

	for _, node := range s.Placement {
		if node.isDelete && len(node.Indexes) == 0 {
			continue
		}
		result = append(result, node)
	}

	s.Placement = result
}

// This function finds the indexer with matching nodeId.
func (s *Solution) findMatchingIndexer(id string) *IndexerNode {
	for _, indexer := range s.Placement {
		if indexer.NodeId == id {
			return indexer
		}
	}

	return nil
}

// Find node to be deleted
func (s *Solution) getDeleteNodes() []*IndexerNode {

	result := ([]*IndexerNode)(nil)
	for _, indexer := range s.Placement {
		if indexer.isDelete {
			result = append(result, indexer)
		}
	}

	return result
}

// find list of new nodes i.e. nodes without any index
func (s *Solution) getNewNodes() []*IndexerNode {

	result := ([]*IndexerNode)(nil)
	for _, indexer := range s.Placement {
		if indexer.isNew {
			result = append(result, indexer)
		}
	}

	return result
}

// When set to true, this flag ignores excludeNode params, as well as
// planner resource constraints during DDL operations.
func (s *Solution) AllowDDLDuringScaleUp() bool {
	return s.allowDDLDuringScaleUp
}

// This prints the vital statistics from Solution.
func (s *Solution) PrintStats() {

	numOfIndex := 0
	maxIndexSize := uint64(0)
	totalIndexSize := uint64(0)
	maxIndexerOverhead := uint64(0)
	totalIndexCpu := float64(0)
	maxIndexCpu := float64(0)
	avgIndexSize := uint64(0)
	avgIndexCpu := float64(0)

	for _, indexer := range s.Placement {
		numOfIndex += len(indexer.Indexes)

		overhead := indexer.GetMemOverhead(s.UseLiveData())
		if overhead > maxIndexerOverhead {
			maxIndexerOverhead = overhead
		}

		for _, index := range indexer.Indexes {
			totalIndexSize += index.GetMemUsage(s.UseLiveData())
			totalIndexCpu += index.GetCpuUsage(s.UseLiveData())

			if index.GetMemUsage(s.UseLiveData()) > maxIndexSize {
				maxIndexSize = index.GetMemUsage(s.UseLiveData())
			}

			if index.GetCpuUsage(s.UseLiveData()) > maxIndexCpu {
				maxIndexCpu = index.GetCpuUsage(s.UseLiveData())
			}
		}
	}

	if numOfIndex != 0 {
		avgIndexSize = totalIndexSize / uint64(numOfIndex)
		avgIndexCpu = totalIndexCpu / float64(numOfIndex)
	}

	logging.Infof("Number of indexes: %v", numOfIndex)
	logging.Infof("Number of indexers: %v", len(s.Placement))
	logging.Infof("Avg Index Size: %v (%s)", avgIndexSize, formatMemoryStr(uint64(avgIndexSize)))
	logging.Infof("Max Index Size: %v (%s)", uint64(maxIndexSize), formatMemoryStr(uint64(maxIndexSize)))
	logging.Infof("Max Indexer Overhead: %v (%s)", uint64(maxIndexerOverhead), formatMemoryStr(uint64(maxIndexerOverhead)))
	logging.Infof("Avg Index Cpu: %.4f", avgIndexCpu)
	logging.Infof("Max Index Cpu: %.4f", maxIndexCpu)
	logging.Infof("Num Estimation Run: %v", s.numEstimateRun)
}

// This prints out layout for the solution
func (s *Solution) PrintLayout() {

	for _, indexer := range s.Placement {

		logging.Infof("")
		logging.Infof("Indexer serverGroup:%v, nodeId:%v, nodeUUID:%v, useLiveData:%v", indexer.ServerGroup, indexer.NodeId, indexer.NodeUUID, s.UseLiveData())
		logging.Infof("Indexer total memory:%v (%s), mem:%v (%s), overhead:%v (%s), min:%v (%s), data:%v (%s) cpu:%.4f, io:%v (%s), scan:%v drain:%v numIndexes:%v shardCompatVersion: %v",
			indexer.GetMemTotal(s.UseLiveData()), formatMemoryStr(uint64(indexer.GetMemTotal(s.UseLiveData()))),
			indexer.GetMemUsage(s.UseLiveData()), formatMemoryStr(uint64(indexer.GetMemUsage(s.UseLiveData()))),
			indexer.GetMemOverhead(s.UseLiveData()), formatMemoryStr(uint64(indexer.GetMemOverhead(s.UseLiveData()))),
			indexer.GetMemMin(s.UseLiveData()), formatMemoryStr(uint64(indexer.GetMemMin(s.UseLiveData()))),
			indexer.GetDataSize(s.UseLiveData()), formatMemoryStr(uint64(indexer.GetDataSize(s.UseLiveData()))),
			indexer.GetCpuUsage(s.UseLiveData()),
			indexer.GetDiskUsage(s.UseLiveData()), formatMemoryStr(uint64(indexer.GetDiskUsage(s.UseLiveData()))),
			indexer.GetScanRate(s.UseLiveData()), indexer.GetDrainRate(s.UseLiveData()),
			len(indexer.Indexes), indexer.ShardCompatVersion)
		logging.Infof("Indexer isDeleted:%v isNew:%v exclude:%v meetConstraint:%v usageRatio:%v allowDDLDuringScaleup:%v",
			indexer.IsDeleted(), indexer.isNew, indexer.Exclude, indexer.meetConstraint, s.computeUsageRatio(indexer), s.AllowDDLDuringScaleUp())

		for _, index := range indexer.Indexes {
			logging.Infof("\t\t------------------------------------------------------------------------------------------------------------------")
			logging.Infof("\t\tIndex name:%v, bucket:%v, scope:%v, collection:%v, defnId:%v, instId:%v, Partition: %v, new/moved:%v "+
				"shardProxy: %v, numInstances: %v, alternateShardIds: %v",
				index.GetDisplayName(), index.Bucket, index.Scope, index.Collection, index.DefnId, index.InstId, index.PartnId,
				index.initialNode == nil || index.initialNode.NodeId != indexer.NodeId,
				index.IsShardProxy, index.NumInstances, index.AlternateShardIds)
			logging.Infof("\t\tIndex total memory:%v (%s), mem:%v (%s), overhead:%v (%s), min:%v (%s), data:%v (%s) cpu:%.4f io:%v (%s) scan:%v drain:%v",
				index.GetMemTotal(s.UseLiveData()), formatMemoryStr(uint64(index.GetMemTotal(s.UseLiveData()))),
				index.GetMemUsage(s.UseLiveData()), formatMemoryStr(uint64(index.GetMemUsage(s.UseLiveData()))),
				index.GetMemOverhead(s.UseLiveData()), formatMemoryStr(uint64(index.GetMemOverhead(s.UseLiveData()))),
				index.GetMemMin(s.UseLiveData()), formatMemoryStr(uint64(index.GetMemMin(s.UseLiveData()))),
				index.GetDataSize(s.UseLiveData()), formatMemoryStr(uint64(index.GetDataSize(s.UseLiveData()))),
				index.GetCpuUsage(s.UseLiveData()),
				index.GetDiskUsage(s.UseLiveData()), formatMemoryStr(uint64(index.GetDiskUsage(s.UseLiveData()))),
				index.GetScanRate(s.UseLiveData()), index.GetDrainRate(s.UseLiveData()))
			logging.Infof("\t\tIndex resident:%v%% build:%v%% estimated:%v equivCheck:%v pendingCreate:%v pendingDelete:%v",
				uint64(index.GetResidentRatio(s.UseLiveData())),
				index.GetBuildPercent(s.UseLiveData()),
				index.NeedsEstimation() && index.HasSizing(s.UseLiveData()),
				!index.suppressEquivIdxCheck,
				index.pendingCreate, index.PendingDelete)
			for _, subIndex := range index.GroupedIndexes {
				logging.Infof("\t\t\t* Sub-Index name:%v, bucket:%v, scope:%v, collection:%v, defnId:%v, instId:%v, Partition: %v, new/moved:%v "+
					"shardProxy: %v, numInstances: %v, alternateShardIds: %v, initialASIs: %v",
					subIndex.GetDisplayName(), subIndex.Bucket, subIndex.Scope, subIndex.Collection, subIndex.DefnId, subIndex.InstId, subIndex.PartnId,
					subIndex.initialNode == nil || subIndex.initialNode.NodeId != indexer.NodeId,
					subIndex.IsShardProxy, subIndex.NumInstances, subIndex.AlternateShardIds,
					subIndex.initialAlternateShardIds)
				logging.Infof("\t\t\t Sub-Index total memory:%v (%s), mem:%v (%s), overhead:%v (%s), min:%v (%s), data:%v (%s) cpu:%.4f io:%v (%s) scan:%v drain:%v",
					subIndex.GetMemTotal(s.UseLiveData()), formatMemoryStr(uint64(subIndex.GetMemTotal(s.UseLiveData()))),
					subIndex.GetMemUsage(s.UseLiveData()), formatMemoryStr(uint64(subIndex.GetMemUsage(s.UseLiveData()))),
					subIndex.GetMemOverhead(s.UseLiveData()), formatMemoryStr(uint64(subIndex.GetMemOverhead(s.UseLiveData()))),
					subIndex.GetMemMin(s.UseLiveData()), formatMemoryStr(uint64(subIndex.GetMemMin(s.UseLiveData()))),
					subIndex.GetDataSize(s.UseLiveData()), formatMemoryStr(uint64(subIndex.GetDataSize(s.UseLiveData()))),
					subIndex.GetCpuUsage(s.UseLiveData()),
					subIndex.GetDiskUsage(s.UseLiveData()), formatMemoryStr(uint64(subIndex.GetDiskUsage(s.UseLiveData()))),
					subIndex.GetScanRate(s.UseLiveData()), subIndex.GetDrainRate(s.UseLiveData()))
				logging.Infof("\t\t\t Sub-Index resident:%v%% build:%v%% estimated:%v equivCheck:%v pendingCreate:%v pendingDelete:%v",
					uint64(subIndex.GetResidentRatio(s.UseLiveData())),
					subIndex.GetBuildPercent(s.UseLiveData()),
					subIndex.NeedsEstimation() && subIndex.HasSizing(s.UseLiveData()),
					!subIndex.suppressEquivIdxCheck,
					subIndex.pendingCreate, subIndex.PendingDelete)
			}
		}
	}
}

// Compute statistics on memory usage
func (s *Solution) ComputeMemUsage() (float64, float64) {

	// Compute mean memory usage
	var meanMemUsage float64
	for _, indexerUsage := range s.Placement {
		meanMemUsage += float64(indexerUsage.GetMemUsage(s.UseLiveData()))
	}
	meanMemUsage = meanMemUsage / float64(len(s.Placement))

	// compute memory variance
	var varianceMemUsage float64
	for _, indexerUsage := range s.Placement {
		v := float64(indexerUsage.GetMemUsage(s.UseLiveData())) - meanMemUsage
		varianceMemUsage += v * v
	}
	varianceMemUsage = varianceMemUsage / float64(len(s.Placement))

	// compute memory std dev
	stdDevMemUsage := math.Sqrt(varianceMemUsage)

	return meanMemUsage, stdDevMemUsage
}

// Compute statistics on cpu usage
func (s *Solution) ComputeCpuUsage() (float64, float64) {

	// Compute mean cpu usage
	var meanCpuUsage float64
	for _, indexerUsage := range s.Placement {
		meanCpuUsage += float64(indexerUsage.GetCpuUsage(s.UseLiveData()))
	}
	meanCpuUsage = meanCpuUsage / float64(len(s.Placement))

	// compute cpu variance
	var varianceCpuUsage float64
	for _, indexerUsage := range s.Placement {
		v := float64(indexerUsage.GetCpuUsage(s.UseLiveData())) - meanCpuUsage
		varianceCpuUsage += v * v
	}
	varianceCpuUsage = varianceCpuUsage / float64(len(s.Placement))

	// compute cpu std dev
	stdDevCpuUsage := math.Sqrt(varianceCpuUsage)

	return meanCpuUsage, stdDevCpuUsage
}

// Compute statistics on disk Usage
func (s *Solution) ComputeDiskUsage() (float64, float64) {

	// Compute mean disk usage
	var meanDiskUsage float64
	for _, indexerUsage := range s.Placement {
		meanDiskUsage += float64(indexerUsage.GetDiskUsage(s.UseLiveData()))
	}
	meanDiskUsage = meanDiskUsage / float64(len(s.Placement))

	// compute disk variance
	var varianceDiskUsage float64
	for _, indexerUsage := range s.Placement {
		v := float64(indexerUsage.GetDiskUsage(s.UseLiveData())) - meanDiskUsage
		varianceDiskUsage += v * v
	}
	varianceDiskUsage = varianceDiskUsage / float64(len(s.Placement))

	// compute disk std dev
	stdDevDiskUsage := math.Sqrt(varianceDiskUsage)

	return meanDiskUsage, stdDevDiskUsage
}

// Compute statistics on drain rate
func (s *Solution) ComputeDrainRate() (float64, float64) {

	// Compute mean drain rate
	var meanDrainRate float64
	for _, indexerUsage := range s.Placement {
		meanDrainRate += float64(indexerUsage.GetDrainRate(s.UseLiveData()))
	}
	meanDrainRate = meanDrainRate / float64(len(s.Placement))

	// compute drain rate variance
	var varianceDrainRate float64
	for _, indexerUsage := range s.Placement {
		v := float64(indexerUsage.GetDrainRate(s.UseLiveData())) - meanDrainRate
		varianceDrainRate += v * v
	}
	varianceDrainRate = varianceDrainRate / float64(len(s.Placement))

	// compute drain rate std dev
	stdDevDrainRate := math.Sqrt(varianceDrainRate)

	return meanDrainRate, stdDevDrainRate
}

// Compute statistics on scan rate
func (s *Solution) ComputeScanRate() (float64, float64) {

	// Compute mean scan rate
	var meanScanRate float64
	for _, indexerUsage := range s.Placement {
		meanScanRate += float64(indexerUsage.GetScanRate(s.UseLiveData()))
	}
	meanScanRate = meanScanRate / float64(len(s.Placement))

	// compute scan rate variance
	var varianceScanRate float64
	for _, indexerUsage := range s.Placement {
		v := float64(indexerUsage.GetScanRate(s.UseLiveData())) - meanScanRate
		varianceScanRate += v * v
	}
	varianceScanRate = varianceScanRate / float64(len(s.Placement))

	// compute scan rate std dev
	stdDevScanRate := math.Sqrt(varianceScanRate)

	return meanScanRate, stdDevScanRate
}

// Compute statistics on number of index. This only consider
// index that has no stats or sizing information.
func (s *Solution) ComputeEmptyIndexDistribution() (float64, float64) {

	// Compute mean number of index
	var meanIdxUsage float64
	for _, indexer := range s.Placement {
		meanIdxUsage += float64(indexer.numEmptyIndex)
	}
	meanIdxUsage = meanIdxUsage / float64(len(s.Placement))

	// compute variance on number of index
	var varianceIdxUsage float64
	for _, indexer := range s.Placement {
		v := float64(indexer.numEmptyIndex) - meanIdxUsage
		varianceIdxUsage += v * v
	}
	varianceIdxUsage = varianceIdxUsage / float64(len(s.Placement))

	// compute std dev on number of index
	stdDevIdxUsage := math.Sqrt(varianceIdxUsage)

	return meanIdxUsage, stdDevIdxUsage
}

// This function returns the residual memory after subtracting ONLY the estimated empty index memory.
// The cost function tries to maximize the total memory of empty index (or minimize
// the residual memory after subtracting empty index).
// The residual memory is further calibrated by empty index usage ratio (the total empty index memory over the total memory)
// If there is no empty index, this function returns 0.
func (s *Solution) ComputeCapacityAfterEmptyIndex() float64 {

	max := s.computeMaxMemUsage()
	if max < s.constraint.GetMemQuota() {
		max = s.constraint.GetMemQuota()
	}

	var emptyIdxCnt uint64
	var totalIdxCnt uint64
	var emptyIdxMem uint64

	for _, indexer := range s.Placement {
		emptyIdxMem += indexer.computeFreeMemPerEmptyIndex(s, max, 0) * uint64(indexer.numEmptyIndex)
		emptyIdxCnt += uint64(indexer.numEmptyIndex)
		totalIdxCnt += uint64(len(indexer.Indexes))
	}

	maxTotal := max * uint64(len(s.Placement))
	usageAfterEmptyIdx := float64(maxTotal-emptyIdxMem) / float64(maxTotal)
	weight := float64(emptyIdxCnt) / float64(totalIdxCnt)

	return usageAfterEmptyIdx * weight
}

// Compute statistics on data size
func (s *Solution) ComputeDataSize() (float64, float64) {

	// Compute mean data size
	var meanDataSize float64
	for _, indexerUsage := range s.Placement {
		meanDataSize += float64(indexerUsage.GetDataSize(s.UseLiveData()))
	}
	meanDataSize = meanDataSize / float64(len(s.Placement))

	// compute data size variance
	var varianceDataSize float64
	for _, indexerUsage := range s.Placement {
		v := float64(indexerUsage.GetDataSize(s.UseLiveData())) - meanDataSize
		varianceDataSize += v * v
	}
	varianceDataSize = varianceDataSize / float64(len(s.Placement))

	// compute data size std dev
	stdDevDataSize := math.Sqrt(varianceDataSize)

	return meanDataSize, stdDevDataSize
}

// Compute statistics on index movement
func (s *Solution) computeIndexMovement(useNewNode bool) (uint64, uint64, uint64, uint64) {

	totalSize := uint64(0)
	dataMoved := uint64(0)
	totalIndex := uint64(0)
	indexMoved := uint64(0)

	if s.place != nil {
		for _, indexer := range s.Placement {
			totalSize += indexer.totalData
			dataMoved += indexer.dataMovedIn
			totalIndex += indexer.totalIndex
			indexMoved += indexer.indexMovedIn
		}
	} else {
		for _, indexer := range s.Placement {

			// ignore cost moving to a new node
			if !useNewNode && indexer.isNew {
				continue
			}

			for _, index := range indexer.Indexes {

				// ignore cost of moving an index out of an to-be-deleted node
				if index.initialNode != nil && !index.initialNode.isDelete {
					totalSize += index.GetDataSize(s.UseLiveData())
					totalIndex++
				}

				// ignore cost of moving an index out of an to-be-deleted node
				if index.initialNode != nil && !index.initialNode.isDelete &&
					index.initialNode.NodeId != indexer.NodeId {
					dataMoved += index.GetDataSize(s.UseLiveData())
					indexMoved++
				}
			}
		}
	}

	return totalSize, dataMoved, totalIndex, indexMoved
}

// Find the number of replica or equivalent index (including itself).
func (s *Solution) findNumEquivalentIndex(u *IndexUsage) int {

	var count int

	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {

			// check replica
			if index.IsReplica(u) || index.IsEquivalentIndex(u, false) {
				count++
			}
		}
	}

	return count
}

// Find the number of replica (including itself).
func (s *Solution) findNumReplica(u *IndexUsage) int {

	count := 0
	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {

			// check replica
			if index.IsReplica(u) {
				count++
			}
		}
	}

	return count
}

// Find the missing replica.  Return a list of replicaId
func (s *Solution) findMissingReplica(u *IndexUsage) map[int]common.IndexInstId {

	found := make(map[int]common.IndexInstId)
	instances := make(map[common.IndexInstId]int)
	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {

			// check replica for each partition (including self)
			if index.IsReplica(u) {
				if index.Instance == nil {
					logging.Warnf("Cannot determine replicaId for index (%v,%v,%v,%v)", index.Name, index.Bucket, index.Scope, index.Collection)
					return (map[int]common.IndexInstId)(nil)
				}
				found[index.Instance.ReplicaId] = index.InstId
			}

			// check instance/replica for the index definition (including self)
			if index.IsSameIndex(u) {
				if index.Instance == nil {
					logging.Warnf("Cannot determine replicaId for index (%v,%v,%v,%v)", index.Name, index.Bucket, index.Scope, index.Collection)
					return (map[int]common.IndexInstId)(nil)
				}
				instances[index.InstId] = index.Instance.ReplicaId
			}
		}
	}

	// replicaId starts with 0
	// numReplica excludes itself
	missing := make(map[int]common.IndexInstId)
	if u.Instance != nil {
		// Going through all the instances of this definition (across partitions).
		// Find if any replica is missing for this partition.
		for instId, replicaId := range instances {
			if _, ok := found[replicaId]; !ok {
				missing[replicaId] = instId
			}
		}

		add := int(u.Instance.Defn.NumReplica+1) - len(missing) - len(found)
		if add > 0 {
			for k := 0; k < add; k++ {
				for i := 0; i < int(math.MaxUint32); i++ {
					_, ok1 := found[i]
					_, ok2 := missing[i]
					_, ok3 := s.usedReplicaIdMap[u.DefnId][i]
					_, ok4 := s.eligUsedReplicaIdMap[u.DefnId][i]

					if !ok1 && !ok2 && !ok3 && !ok4 {
						missing[i] = 0
						break
					}
				}
			}
		}
	}

	return missing
}

// Find the number of partition (including itself).
func (s *Solution) findNumPartition(u *IndexUsage) int {

	count := 0
	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {
			if index.IsSameInst(u) {
				count++
			}
		}
	}

	return count
}

// Find the missing partition
func (s *Solution) findMissingPartition(u *IndexUsage) []common.PartitionId {

	if u.Instance == nil || u.Instance.Pc == nil {
		return []common.PartitionId(nil)
	}

	found := make(map[common.PartitionId]bool)
	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {

			// check instance (including self)
			if index.IsSameInst(u) {
				found[index.PartnId] = true
			}
		}
	}

	startPartnId := 0
	if common.IsPartitioned(u.Instance.Defn.PartitionScheme) {
		startPartnId = 1
	}
	endPartnId := startPartnId + int(u.Instance.Pc.GetNumPartitions())

	var missing []common.PartitionId
	for i := startPartnId; i < endPartnId; i++ {
		if _, ok := found[common.PartitionId(i)]; !ok {
			missing = append(missing, common.PartitionId(i))
		}
	}

	return missing
}

// Find the number of server group.   If a
// server group consists of only ejected node,
// this server group will be skipped.
func (s *Solution) findNumServerGroup() int {

	groups := make(map[string]bool)
	for _, indexer := range s.Placement {
		if indexer.isDelete {
			continue
		}

		if _, ok := groups[indexer.ServerGroup]; !ok {
			groups[indexer.ServerGroup] = true
		}
	}

	return len(groups)
}

// This function recalculates the index and indexer sizes based on sizing formula.
// Data captured from live cluster will not be overwritten.
func (s *Solution) calculateSize() {

	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {
			index.ComputeSizing(s.UseLiveData(), s.sizing)
		}
	}

	for _, indexer := range s.Placement {
		s.sizing.ComputeIndexerSize(indexer)
	}
}

// is this a MOI Cluster?
func (s *Solution) isMOICluster() bool {

	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {
			if index.IsMOI() {
				return true
			}
		}
	}

	return false
}

// Find num of deleted node
func (s *Solution) findNumDeleteNodes() int {

	count := 0
	for _, indexer := range s.Placement {
		if indexer.isDelete {
			count++
		}
	}

	return count
}

// Find num of excludeIn node.
func (s *Solution) findNumExcludeInNodes() int {

	count := 0
	for _, indexer := range s.Placement {
		if indexer.ExcludeIn(s) {
			count++
		}
	}

	return count
}

// find list of nodes that have node.Exclude = "out" set
func (s *Solution) getExcludedOutNodes() []*IndexerNode {

	result := ([]*IndexerNode)(nil)
	for _, indexer := range s.Placement {
		if indexer.Exclude == "out" {
			result = append(result, indexer)
		}
	}

	return result
}

// Find num of empty node
func (s *Solution) findNumEmptyNodes() int {

	count := 0
	for _, indexer := range s.Placement {
		if len(indexer.Indexes) == 0 && !indexer.isDelete {
			count++
		}
	}

	return count
}

// find number of live node (excluding ejected node)
func (s *Solution) findNumLiveNode() int {

	return len(s.Placement) - s.findNumDeleteNodes()
}

// find number of available live node (excluding ejected node and excluded node)
func (s *Solution) findNumAvailLiveNode() int {

	count := 0
	for _, indexer := range s.Placement {
		if !indexer.ExcludeIn(s) && !indexer.IsDeleted() {
			count++
		}
	}

	return count
}

// Evaluate if each indexer meets constraint
func (s *Solution) evaluateNodes() {
	for _, indexer := range s.Placement {
		indexer.Evaluate(s)
	}
}

// check to see if we should ignore resource (memory/cpu) constraint
func (s *Solution) ignoreResourceConstraint() bool {

	// always honor resource constraint when doing simulation
	if !s.UseLiveData() {
		return false
	}

	// if size estimation is turned on, then honor the constraint since it needs to find the best fit under the
	// assumption that new indexes are going to fit under constraint.
	if s.command == CommandPlan {
		if s.canRunEstimation() {
			return false
		}
	}

	// TODO: Add checks for DDL during a transient heterogenous state in swap-only rebalance
	// Check for Swap-only Rebalance during upgrade
	if s.swapOnlyRebalance {
		return true
	}

	return !s.enforceConstraint
}

// check to see if every node in the cluster meet constraint
func (s *Solution) SatisfyClusterConstraint() bool {

	for _, indexer := range s.Placement {
		if !indexer.SatisfyNodeConstraint() {
			return false
		}
	}

	return true
}

// findServerGroup gets called only if solution.numServerGroup > 1
func (s *Solution) findServerGroup(defnId common.IndexDefnId, partnId common.PartitionId, replicaId int) (string, bool) {

	findSG := func(sgMap ServerGroupMap) (string, bool) {
		partitions, ok := sgMap[defnId]
		if !ok {
			return "", false
		}

		replicas, ok := partitions[partnId]
		if !ok {
			return "", false
		}

		sg, ok := replicas[replicaId]
		return sg, ok
	}

	if sg, ok := findSG(s.eligIndexSGMap); ok {
		return sg, ok
	}

	return findSG(s.indexSGMap)
}

// Check if there is any replica (excluding self) in the server group
//
// hasReplicaInServerGroup gets called only if solution.numServerGroup > 1
func (s *Solution) hasReplicaInServerGroup(u *IndexUsage, group string) bool {

	hasReplicaInSG := func(replicaM ReplicaMap) bool {

		if u.Instance != nil {
			replicas := replicaM[u.DefnId][u.PartnId]
			for i, _ := range replicas {
				if i != u.Instance.ReplicaId {
					if sg, ok := s.findServerGroup(u.DefnId, u.PartnId, i); ok {
						if sg == group {
							return true
						}
					}
				}
			}
		} else {
			for _, indexer := range s.Placement {
				if indexer.isDelete {
					continue
				}
				for _, index := range indexer.Indexes {
					if index != u && index.IsReplica(u) { // replica
						if group == indexer.ServerGroup {
							return true
						}
					}
				}
			}
		}

		return false
	}

	if hasReplicaInSG(s.eligReplicaMap) {
		return true
	}

	return hasReplicaInSG(s.replicaMap)
}

// Get server groups having replicas for this index (excluding self)
//
// getServerGroupsWithReplica gets called only if solution.numServerGroup > 1
func (s *Solution) getServerGroupsWithReplica(u *IndexUsage) map[string]bool {

	getSGWithReplica := func(replicaM ReplicaMap) map[string]bool {

		counts := make(map[string]bool)
		if u.Instance != nil {
			replicas := replicaM[u.DefnId][u.PartnId]
			for i, _ := range replicas {
				if i != u.Instance.ReplicaId {
					if group, ok := s.findServerGroup(u.DefnId, u.PartnId, i); ok {
						counts[group] = true
					}
				}
			}
		} else {
			for _, indexer := range s.Placement {
				if indexer.isDelete {
					continue
				}

				for _, index := range indexer.Indexes {
					if index != u && index.IsReplica(u) { // replica
						counts[indexer.ServerGroup] = true
					}
				}
			}
		}
		return counts
	}

	groups := getSGWithReplica(s.replicaMap)
	for k, _ := range getSGWithReplica(s.eligReplicaMap) {
		groups[k] = true
	}

	return groups
}

// Check if any server group without this replica (excluding self)
//
// hasServerGroupWithNoReplica gets called only if solution.numServerGroup > 1
func (s *Solution) hasServerGroupWithNoReplica(u *IndexUsage) bool {

	if s.numServerGroup > len(s.getServerGroupsWithReplica(u)) {
		return true
	}

	return false
}

//
// For a given index, find out all replica with the lower replicaId.
// The given index should have the higher replica count in its server
// group than all its replica (with lower replicaId).
//
/*
func (s *Solution) hasHighestReplicaCountInServerGroup(u *IndexUsage) bool {

	if !s.place.IsEligibleIndex(u) {
		return true
	}

	counts := make(map[string]int)
	replicaIds := make([]int)
	for i, _ := range s.replicaMap[u.DefnId][u.PartnId] {
		if i < u.Instance.ReplicaId {
			if sg, ok := s.findServerGroup(u.DefnId, u.PartnId, i); ok {
				counts[group]++
				replicaIds = append(replicas, i)
			}
		}
	}

	idxSG := s.findServerGroup(u.DefnId, u.PartnId, u.Instance.ReplicaId)
	for i := 0; i < len(replicaIds); i++ {
		replica := s.replicaMap[u.DefnId][u.PartnId][i]
		if s.place.IsEligibleIndex(replica) {
			sg := s.findServerGroup(u.DefnId, u.PartnId, i)

			if counts[idxSG] < counts[sg] {
				return false
			}
		}
	}

	return true
}
*/

// Does the index node has replica?
func (s *Solution) hasReplica(indexer *IndexerNode, target *IndexUsage) bool {

	for _, index := range indexer.Indexes {
		if index != target && index.IsReplica(target) {
			return true
		}
	}

	return false
}

// Find the indexer node that contains the replica
func (s *Solution) FindIndexerWithReplica(name, bucket, scope, collection string,
	partnId common.PartitionId, replicaId int) *IndexerNode {

	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {
			if index.Name == name &&
				index.Bucket == bucket &&
				index.Scope == scope &&
				index.Collection == collection &&
				index.PartnId == partnId &&
				index.Instance != nil &&
				index.Instance.ReplicaId == replicaId {
				return indexer
			}
		}
	}

	return nil
}

// Find possible targets to place lost replicas.
// Returns the exact list of nodes which can be directly used to
// place the lost replicas.
func (s *Solution) FindNodesForReplicaRepair(source *IndexUsage, numNewReplica int, useExcludeNode bool) []*IndexerNode {
	// TODO: Make this function aware of equivalent indexes.

	rs := rand.New(rand.NewSource(time.Now().UnixNano()))

	nodes := shuffleNode(rs, s.Placement)

	// TODO: Need to evaluate the cost of calculating replica and SG maps
	// before replica repair.
	replicaNodes := make(map[string]bool)
	replicaServerGroups := make(map[string]bool)

	result := make([]*IndexerNode, 0, len(nodes))
	for _, indexer := range nodes {
		if indexer.IsDeleted() {
			continue
		}

		if !useExcludeNode && indexer.ExcludeIn(s) {
			continue
		}

		found := false
		for _, index := range indexer.Indexes {
			if source.IsReplica(index) {
				found = true
				replicaNodes[indexer.IndexerId] = true
				replicaServerGroups[indexer.ServerGroup] = true
				break
			}
		}

		if !found {
			result = append(result, indexer)
		}
	}

	// If there are more nodes than the number of missing replica, then prioritize the nodes based
	// on server group.
	if len(result) > numNewReplica {
		final := make([]*IndexerNode, 0, len(result))

		// Add a node from each server group that does not have the index
		for _, indexer := range result {
			if numNewReplica <= 0 {
				break
			}

			if _, ok := replicaServerGroups[indexer.ServerGroup]; !ok {
				final = append(final, indexer)
				replicaNodes[indexer.IndexerId] = true
				replicaServerGroups[indexer.ServerGroup] = true
				numNewReplica--
			}
		}

		// Still need more node.  Add nodes from server group that already has the index.
		for _, indexer := range result {
			if numNewReplica <= 0 {
				break
			}

			if _, ok := replicaNodes[indexer.IndexerId]; !ok {
				final = append(final, indexer)
				replicaNodes[indexer.IndexerId] = true
				replicaServerGroups[indexer.ServerGroup] = true
				numNewReplica--
			}
		}

		return final
	}

	return result
}

// Mark the node with no indexes as "new" node
func (s *Solution) markNewNodes() {

	for _, indexer := range s.Placement {
		if !indexer.isDelete && len(indexer.Indexes) == 0 {
			indexer.isNew = true
		}
	}
}

// Does cluster has new node?
func (s *Solution) hasNewNodes() bool {

	for _, indexer := range s.Placement {
		if !indexer.isDelete && indexer.isNew {
			return true
		}
	}
	return false
}

// Reset the new node flag
func (s *Solution) resetNewNodes() {

	for _, indexer := range s.Placement {
		indexer.isNew = false
	}
}

// markDeletedNodes sets isDelete flag as true for deletec nodes
func (s *Solution) markDeletedNodes(deletedNodes []string) error {

	if len(deletedNodes) != 0 {

		if len(deletedNodes) > len(s.Placement) {
			return errors.New("The number of node in cluster is smaller than the number of node to be deleted.")
		}

		for _, nodeId := range deletedNodes {

			candidate := s.findMatchingIndexer(nodeId)
			if candidate == nil {
				return errors.New(fmt.Sprintf("Cannot find to-be-deleted indexer in solution: %v", nodeId))
			}

			candidate.isDelete = true
		}
	}
	return nil

}

// Does cluster has deleted node?
func (s *Solution) hasDeletedNodes() bool {

	for _, indexer := range s.Placement {
		if indexer.isDelete {
			return true
		}
	}
	return false
}

// Update SG mapping for index
func (s *Solution) updateServerGroupMap(index *IndexUsage, indexer *IndexerNode) {

	if s.numServerGroup <= 1 {
		return
	}

	updateSGMap := func(sgMap ServerGroupMap) {
		if index.Instance != nil {
			if indexer != nil {
				if _, ok := sgMap[index.DefnId]; !ok {
					sgMap[index.DefnId] = make(map[common.PartitionId]map[int]string)
				}

				if _, ok := sgMap[index.DefnId][index.PartnId]; !ok {
					sgMap[index.DefnId][index.PartnId] = make(map[int]string)
				}

				sgMap[index.DefnId][index.PartnId][index.Instance.ReplicaId] = indexer.ServerGroup
			} else {
				if _, ok := sgMap[index.DefnId]; ok {
					if _, ok := sgMap[index.DefnId][index.PartnId]; ok {
						delete(sgMap[index.DefnId][index.PartnId], index.Instance.ReplicaId)
					}
				}
			}
		}
	}

	if index.eligible {
		updateSGMap(s.eligIndexSGMap)
	} else {
		if s.place != nil {
			eligibles := s.place.GetEligibleIndexes()
			if isEligibleIndex(index, eligibles) {
				updateSGMap(s.eligIndexSGMap)
			} else {
				updateSGMap(s.indexSGMap)
			}
		} else {
			// Assume that it is not an eligible index
			updateSGMap(s.indexSGMap)
		}
	}
}

// initialize SG mapping for index
func (s *Solution) initializeServerGroupMap() {
	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {
			s.updateServerGroupMap(index, indexer)
		}
	}
}

func (s *Solution) resetReplicaMap() {
	s.eligReplicaMap = make(ReplicaMap)
	s.replicaMap = make(ReplicaMap)
	s.indexSGMap = make(ServerGroupMap)
	s.eligIndexSGMap = make(ServerGroupMap)
}

// Generate a map for replicaId
func (s *Solution) generateReplicaMap() {

	eligibles := s.place.GetEligibleIndexes()

	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {
			s.updateReplicaMap(index, indexer, eligibles)
		}
	}
}

func (s *Solution) updateReplicaMap(index *IndexUsage, indexer *IndexerNode, eligibles map[*IndexUsage]bool) {
	var replicaM ReplicaMap
	if isEligibleIndex(index, eligibles) {
		replicaM = s.eligReplicaMap
	} else {
		replicaM = s.replicaMap
	}

	if index.Instance != nil {
		if _, ok := replicaM[index.DefnId]; !ok {
			replicaM[index.DefnId] = make(map[common.PartitionId]map[int]*IndexUsage)
		}

		if _, ok := replicaM[index.DefnId][index.PartnId]; !ok {
			replicaM[index.DefnId][index.PartnId] = make(map[int]*IndexUsage)
		}

		replicaM[index.DefnId][index.PartnId][index.Instance.ReplicaId] = index
	}
}

// NoUsageInfo index is index without any sizing information.
// For placement, planner still want to layout noUsage index such that
// 1) indexer with more free memory will take up more partitions.
// 2) partitions should be spread out as much as possible
// 3) non-partition index have different weight than individual partition of a partitioned index
//
// It is impossible to achieve accurate number for (1) and (3) without sizing info (even with sizing,
// it is only an approximation).    But if we make 2 assumptions, we can provide estimated sizing of partitions:
// 1) the new index will take up the remaining free memory of all indexers
// 2) the partition of new index are equal size
//
// To estimate NoUsageInfo index size based on free memory available.
// 1) calculates total free memory from indexers which has less than 80% memory usage
// 2) Based on total free memory, estimates index size based on number of noUsage index.
// 3) Estimate the partition size
//   - non-partitioned index has 1 partition, so it will take up full index size
//   - individual partition of a partitioned index will take up (index-size / numPartition)
//
// 4) Given the estimated size, if cannot find a placement solution, reduce index size by 10% and repeat (4)
//
// Behavior:
// 1) Using ConstraintMethod, indexes/Partitions will more likely to move to indexers with more free memory
// 2) Using CostMethod, partitions will likely to spread out to minimize memory variation
//
// Empty Index:
// When an index is empty, it can have DataSize and MemUsage as zero (Actual or otherwise). Note that
// this can happen with MOI index created on a bucket which never had any mutations). For such indexes,
// the index state will be ACTIVE. So, there is no need to estimate the size of such indexes.
// But the planner cannot work with zero sized indexes. And the above mentioned strategy (i.e. index will
// take up the remaining free memory of all indexers) will not work - as it misrepresent the actual
// size of the index. So, size of the empty index will be estimated to some small constant value.
//
// Caveats:
//  1. When placing the new index onto a cluster with existing NoUsageInfo (deferred) index, the existing
//     index will also need to be sized.   The existing index is assumed to have the similar size as
//     the new index.   Therefore, the estimated index size represents the "average index size" for
//     all unsized indexes.
func (s *Solution) runSizeEstimation(placement PlacementMethod, newInstCount int) {

	estimate := func(estimatedIndexSize uint64) {
		for _, indexer := range s.Placement {

			// excludeIn - indexes cannot moved into this node.  So effectively, this indexer is taken out of cluster.
			if indexer.ExcludeIn(s) {
				continue
			}

			for _, index := range indexer.Indexes {
				if index.NeedsEstimation() {
					if index.state == common.INDEX_STATE_ACTIVE {
						index.EstimatedDataSize = EMPTY_INDEX_DATASIZE
						index.EstimatedMemUsage = EMPTY_INDEX_MEMUSAGE
					} else {
						if index.Instance != nil && index.Instance.Pc != nil && common.IsPartitioned(index.Instance.Defn.PartitionScheme) {
							index.EstimatedMemUsage = estimatedIndexSize / uint64(index.Instance.Pc.GetNumPartitions())
							index.EstimatedDataSize = index.EstimatedMemUsage
						} else {
							index.EstimatedMemUsage = estimatedIndexSize
							index.EstimatedDataSize = index.EstimatedMemUsage
						}
					}

					indexer.AddMemUsageOverhead(s, index.EstimatedMemUsage, 0, index.EstimatedMemUsage)
					indexer.AddDataSize(s, index.EstimatedDataSize)
				}
			}
		}
	}

	// nothing to do if size estimation is turned off
	if !s.estimate {
		return
	}

	// only estimate size for planning
	if s.command != CommandPlan {
		s.estimationOff()
		return
	}

	// Only enable estimation if eligible indexes have no sizing info.
	// Do not turn on estimation if eligible has sizing info, but
	// there are existing deferred index.   Turn it on may cause
	// new index not being able to place properly, since existing
	// deferred index may use up available memory during estimation.
	eligibles := placement.GetEligibleIndexes()
	for eligible, _ := range eligibles {
		if !eligible.NoUsageInfo {
			s.estimationOff()
			return
		}
	}

	s.numEstimateRun++

	// cleanup
	s.cleanupEstimation()

	// if there is a previous calculation, use it
	if s.estimatedIndexSize != 0 {
		// if previous sizing is available, then use it by sizing it down.
		s.estimatedIndexSize = uint64(float64(s.estimatedIndexSize) * 0.9)
		if s.estimatedIndexSize > 0 {
			estimate(s.estimatedIndexSize)
		} else {
			s.estimationOff()
		}
		return
	}

	//
	// Calculate the initial estimated index size
	//

	//
	// count the number of indexes for estimation
	//
	insts := make(map[common.IndexInstId]*common.IndexInst)
	for _, indexer := range s.Placement {

		// eligible index cannot be placed in an indexer that has been excluded (in or out)
		// excludeIn - indexes cannot moved into this node.  So effectively, this indexer is taken out of cluster.
		if indexer.ExcludeIn(s) {
			continue
		}

		// count NoUsageInfo index.  This covers new index as well as existing index.
		for _, index := range indexer.Indexes {
			if index.NeedsEstimation() {
				insts[index.InstId] = index.Instance
			}
		}
	}

	//
	// calculate total free memory
	//
	threshold := float64(0.2)
	max := float64(s.computeMaxMemUsage())
	if max < float64(s.constraint.GetMemQuota()) {
		max = float64(s.constraint.GetMemQuota())
	}

retry1:
	var indexers []*IndexerNode
	totalMemFree := uint64(0)
	maxMemFree := uint64(0)

	for _, indexer := range s.Placement {
		// excludeIn - indexes cannot moved into this node.  So effectively, this indexer is taken out of cluster.
		if indexer.ExcludeIn(s) {
			continue
		}

		// Do not use Cpu for estimation for now since cpu measurement is fluctuating
		freeMem := max - float64(indexer.GetMemTotal(s.UseLiveData()))
		freeMemRatio := freeMem / float64(max)
		if freeMem > 0 && freeMemRatio > threshold {
			// freeMem is a positive number
			adjFreeMem := uint64(freeMem * 0.8)
			totalMemFree += adjFreeMem
			if adjFreeMem > maxMemFree {
				maxMemFree = adjFreeMem
			}
			indexers = append(indexers, indexer)
		}
	}

	// If there is no indexer, then retry with lower free Ratio
	if len(indexers) == 0 && threshold > 0.05 {
		threshold -= 0.05
		goto retry1
	}

	// not enough indexers with free memory. Do not estimate sizing.
	if len(indexers) == 0 {
		s.estimationOff()
		return
	}

	//
	// Estimate index size
	//
	if len(insts) > 0 {

		// Compute initial slot size based on total free memory.
		// The slot size is the "average" index size.
		s.estimatedIndexSize = totalMemFree / uint64(len(insts)+newInstCount)

	retry2:
		for s.estimatedIndexSize > 0 {
			// estimate noUsage index size
			s.cleanupEstimation()
			estimate(s.estimatedIndexSize)

			// adjust slot size
			for index, _ := range eligibles {
				// cannot be higher than max free mem size
				if index.EstimatedMemUsage > maxMemFree {
					s.estimatedIndexSize = uint64(float64(s.estimatedIndexSize) * 0.9)
					goto retry2
				}
			}

			return
		}

		s.estimationOff()
		logging.Infof("Planner.runSizeEstimation: cannot estimate deferred index size.  Will not set estimatedMemUsage in deferred index")
	}
}

// Turn off estimation
func (s *Solution) estimationOff() {
	s.cleanupEstimation()
	s.estimatedIndexSize = 0
	s.estimate = false
}

// Turn on estimation
func (s *Solution) estimationOn() {
	s.estimate = true
}

// Clean up all calculation based on estimation
func (s *Solution) cleanupEstimation() {
	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {
			if index.NeedsEstimation() {
				indexer.SubtractMemUsageOverhead(s, index.EstimatedMemUsage, 0, index.EstimatedMemUsage)
				indexer.SubtractDataSize(s, index.EstimatedDataSize)
				index.EstimatedMemUsage = 0
				index.EstimatedDataSize = 0
			}
		}
	}
}

// Copy the estimation
func (s *Solution) copyEstimationFrom(source *Solution) {
	s.estimatedIndexSize = source.estimatedIndexSize
	s.estimate = source.estimate
	s.numEstimateRun = source.numEstimateRun
}

// Can run estimation?
func (s *Solution) canRunEstimation() bool {
	return s.estimate
}

// compute average usage for
// 1) memory usage
// 2) cpu
// 3) data
// 4) io
// 5) drain rate
// 6) scan rate
func (s *Solution) computeUsageRatio(indexer *IndexerNode) float64 {

	memCost := float64(0)
	//cpuCost := float64(0)
	dataCost := float64(0)
	diskCost := float64(0)
	drainCost := float64(0)
	scanCost := float64(0)

	if s.memMean != 0 {
		memCost = float64(indexer.GetMemUsage(s.UseLiveData())) / s.memMean
	}

	//if s.cpuMean != 0 {
	//	cpuCost = float64(indexer.GetCpuUsage(s.UseLiveData())) / s.cpuMean
	//}

	if s.dataMean != 0 {
		dataCost = float64(indexer.GetDataSize(s.UseLiveData())) / s.dataMean
	}

	if s.diskMean != 0 {
		diskCost = float64(indexer.GetDiskUsage(s.UseLiveData())) / s.diskMean
	}

	if s.drainMean != 0 {
		drainCost = float64(indexer.GetDrainRate(s.UseLiveData())) / s.drainMean
	}

	if s.scanMean != 0 {
		scanCost = float64(indexer.GetScanRate(s.UseLiveData())) / s.scanMean
	}

	//return (memCost + cpuCost + dataCost + diskCost + drainCost + scanCost) / 6
	return (memCost + dataCost + diskCost + drainCost + scanCost) / 5
}

// compute average mean usage
// 1) memory usage
// 2) cpu
// 3) data
// 4) io
// 5) drain rate
// 6) scan rate
func (s *Solution) computeMeanUsageRatio() float64 {

	//return 6
	return 5
}

func (s *Solution) computeMaxMemUsage() uint64 {

	max := uint64(0)
	for _, indexer := range s.Placement {
		if indexer.GetMemTotal(s.UseLiveData()) > max {
			max = indexer.GetMemTotal(s.UseLiveData())
		}
	}

	return max
}

func (s *Solution) getReplicas(defnId common.IndexDefnId) map[common.PartitionId]map[int]*IndexUsage {
	getReps := func(replicaM ReplicaMap) map[common.PartitionId]map[int]*IndexUsage {
		reps := make(map[common.PartitionId]map[int]*IndexUsage)
		for partnId, rep := range replicaM[defnId] {
			reps[partnId] = make(map[int]*IndexUsage)
			for rid, index := range rep {
				reps[partnId][rid] = index
			}
		}
		return reps
	}

	reps := getReps(s.replicaMap)
	for partnId, rep := range getReps(s.eligReplicaMap) {
		if _, ok := reps[partnId]; !ok {
			reps[partnId] = make(map[int]*IndexUsage)
		}

		for rid, index := range rep {
			reps[partnId][rid] = index
		}
	}

	return reps
}

func (s *Solution) demoteEligIndexes(indexes []*IndexUsage) {

	for _, index := range indexes {
		s.demoteEligIndex(index)
	}
}

func (s *Solution) demoteEligIndex(index *IndexUsage) {

	index.eligible = false

	// Step 1: If present remove index from elig maps
	// Step 2: If index was present in elig maps, add them to non-elig maps

	// In case of partition/replica repair, the index.Instance won't not be nil.
	if index.Instance == nil {
		return
	}

	// Transfer from Server Group Map
	func() {
		sg, ok := s.eligIndexSGMap[index.DefnId][index.PartnId][index.Instance.ReplicaId]
		if !ok {
			return
		}

		delete(s.eligIndexSGMap[index.DefnId][index.PartnId], index.Instance.ReplicaId)

		if _, ok := s.indexSGMap[index.DefnId]; !ok {
			s.indexSGMap[index.DefnId] = make(map[common.PartitionId]map[int]string)
		}

		if _, ok := s.indexSGMap[index.DefnId][index.PartnId]; !ok {
			s.indexSGMap[index.DefnId][index.PartnId] = make(map[int]string)
		}

		s.indexSGMap[index.DefnId][index.PartnId][index.Instance.ReplicaId] = sg
	}()

	// Transfer from Replica Map
	func() {
		idx, ok := s.eligReplicaMap[index.DefnId][index.PartnId][index.Instance.ReplicaId]
		if !ok {
			return
		}

		delete(s.eligReplicaMap[index.DefnId][index.PartnId], index.Instance.ReplicaId)

		if _, ok := s.replicaMap[index.DefnId]; !ok {
			s.replicaMap[index.DefnId] = make(map[common.PartitionId]map[int]*IndexUsage)
		}

		if _, ok := s.replicaMap[index.DefnId][index.PartnId]; !ok {
			s.replicaMap[index.DefnId][index.PartnId] = make(map[int]*IndexUsage)
		}

		s.replicaMap[index.DefnId][index.PartnId][index.Instance.ReplicaId] = idx
	}()

	// Transfer from Used ReplicaId Map
	func() {
		used, ok := s.eligUsedReplicaIdMap[index.DefnId][index.Instance.ReplicaId]
		if !ok {
			return
		}

		delete(s.eligUsedReplicaIdMap[index.DefnId], index.Instance.ReplicaId)

		if _, ok := s.usedReplicaIdMap[index.DefnId]; !ok {
			s.usedReplicaIdMap[index.DefnId] = make(map[int]bool)
		}

		s.usedReplicaIdMap[index.DefnId][index.Instance.ReplicaId] = used
	}()
}

// Check for Swap-only Rebalance
// We mark if cluster has
// 1. only 1 add node OR only 1 Node with ExcludeNode="out" set with no add Nodes
// 2. 1 deleted node with Node.Exclude="in"
// 3. and the remaining nodes have ExcludeNode="inout"
func (s *Solution) markSwapOnlyRebalance() {

	s.swapOnlyRebalance = false
	if s.command == CommandRebalance && s.numDeletedNode == 1 {
		scaledNode := (*IndexerNode)(nil)
		deletedNode := s.getDeleteNodes()[0]
		if s.numNewNode == 1 {
			scaledNode = s.getNewNodes()[0]
		} else if s.numNewNode == 0 {
			excludedOutNodes := s.getExcludedOutNodes()
			if len(excludedOutNodes) == 1 && excludedOutNodes[0] != deletedNode {
				scaledNode = excludedOutNodes[0]
			} else {
				return
			}
		}

		if scaledNode != nil && deletedNode.Exclude == "in" {
			inoutEnabledOnRemaining := true
			for _, node := range s.Placement {
				if node.NodeId == deletedNode.NodeId || node.NodeId == scaledNode.NodeId {
					continue
				}
				if node.Exclude != "inout" {
					inoutEnabledOnRemaining = false
					break
				}
			}
			if inoutEnabledOnRemaining {
				s.swapOnlyRebalance = true
				return
			}
		}
	}
}

func (s *Solution) addToIndexSlots(defnId common.IndexDefnId, replicaId int, partnId common.PartitionId, slotId uint64) {
	// Update indexSlots
	if _, ok := s.indexSlots[defnId]; !ok {
		s.indexSlots[defnId] = make(map[int]map[common.PartitionId]uint64)
	}

	if _, ok := s.indexSlots[defnId][replicaId]; !ok {
		s.indexSlots[defnId][replicaId] = make(map[common.PartitionId]uint64)
	}

	s.indexSlots[defnId][replicaId][partnId] = slotId
}

func (s *Solution) addToSlotMap(slotId uint64, indexer *IndexerNode, replicaId int) {
	// Add to slotMap
	if _, ok := s.slotMap[slotId]; !ok {
		s.slotMap[slotId] = make(map[*IndexerNode]int)
	}
	s.slotMap[slotId][indexer] = replicaId
}

func (s *Solution) updateSlotMapEntry(slotId uint64, oldNode, newNode *IndexerNode, replicaId int) {
	delete(s.slotMap[slotId], oldNode)
	s.addToSlotMap(slotId, newNode, replicaId)
}

func (s *Solution) getIndexSlot(index *IndexUsage) uint64 {
	defnId := index.DefnId

	for _, replicaSlots := range s.indexSlots[defnId] {
		for partnId, slotId := range replicaSlots {
			if partnId == index.PartnId && slotId != 0 {
				// There exists atleast one replica with required partnId for the instance.
				// Use the slotId
				return slotId
			}
		}
	}

	// This index does not have any replicas which are placed with alternate shardIds
	return 0
}

func (s *Solution) updateSlotMap() {
	s.indexSlots = make(map[common.IndexDefnId]map[int]map[common.PartitionId]uint64)
	s.slotMap = map[uint64]map[*IndexerNode]int{}

	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {

			// Only consider indexes with valid alternate shardIds
			if len(index.AlternateShardIds) > 0 {
				alternateShardId, err := common.ParseAlternateId(index.AlternateShardIds[0])
				if err != nil || alternateShardId == nil {
					logging.Fatalf("Solution::updateSlotMap Invalid alternateShardId seen. Index: %v, defnId: %v, "+
						" partnId: %v, alternateShardId: %v, err: %v", index.Name, index.DefnId, index.PartnId, index.AlternateShardIds[0], err)
					continue // Ignore the index
				}

				slotId, replicaId := alternateShardId.SlotId, int(alternateShardId.ReplicaId)
				s.addToIndexSlots(index.DefnId, replicaId, index.PartnId, slotId)
				s.addToSlotMap(slotId, indexer, replicaId)
			}
		}
	}
}

// All replicas of an index share the same slotId and they only differ by
// replica number of the slot. Get the slot ID for the missing replica from slotMap
func (s *Solution) findIndexerForReplica(indexDefnId common.IndexDefnId,
	indexReplicaId int, indexPartnId common.PartitionId, indexers []*IndexerNode) (uint64, *IndexerNode, []*IndexerNode, bool) {

	// If no slots exists for this index in the cluster,
	// return the first node from the list of eligible indexer nodes
	if _, ok := s.indexSlots[indexDefnId]; !ok {
		return 0, indexers[0], indexers[1:], true
	}

	// Atleast one replica of the index exists.
	// Instances with same partition Id of replica instances share the same slotId.
	// E.g, partnId: 0 for replica: 0 and replica:1 share same slotId
	//      partnId: 1 for replica: 0 and replica:1 share same slotId
	//      partnId: 0 and partnId: 1 can have different slotIds
	//
	// Find the slotId for the partition of interest
	var indexSlotId uint64
	found := false
	for _, partnMap := range s.indexSlots[indexDefnId] {
		for partnId, slotId := range partnMap {
			if partnId == indexPartnId {
				indexSlotId = slotId
				found = true
				break
			}
		}
		if found {
			break
		}
	}

	// No partition that is a replica of the current partition is found in the cluster
	// Return the first node from the list of eligible indexer nodes
	if !found {
		return 0, indexers[0], indexers[1:], true
	}

	// If there is any indexer node that hosts this slot with same replicaId,
	// use that indexer node for placing the index being repaired and prune
	// that indexer from list of eligible indexer nodes
	for target, replicaId := range s.slotMap[indexSlotId] {
		if replicaId == indexReplicaId {
			for i, indexer := range indexers {
				if target == indexer {
					return indexSlotId, indexer, append(indexers[:i], indexers[i+1:]...), false
				}
			}

			// At this point, the target indexer is not in the list of eligible indexer nodes
			// (Either because the indexer is getting deleted or it was excluded from taking
			// any new indexes). In such a case, map the index to the indexer node on which
			// the slot with required replica exists as rebalance will move the slot to a
			// different node
			return indexSlotId, target, indexers, false
		}
	}

	// At this point, either any replica of the index being repaired does not exist
	// or the indexer is beyond the list of eligible indexer nodes
	// Return the first eligible node from the list
	//
	// E.g., if replica:2 is being repaired but only replica:0 and replica:1 is a part
	// of the cluster, then we still create index on slot "indexSlotId" on a different
	// node in the cluster
	return indexSlotId, indexers[0], indexers[1:], true
}

func (s *Solution) getShardLimits() (int, int) {
	shardLimitPerTenant := 0

	shardMap := make(map[string]bool)
	for _, indexer := range s.Placement {

		if shardLimitPerTenant < indexer.ShardLimitPerTenant {
			shardLimitPerTenant = indexer.ShardLimitPerTenant
		}

		for _, index := range indexer.Indexes {
			for _, alternateShardId := range index.AlternateShardIds {
				shardMap[alternateShardId] = true
			}
		}
	}

	return shardLimitPerTenant, len(shardMap)
}
