package planner

import (
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
)

const tempNodeUUID = "tempNodeUUID_"

// Keep this consistent with indexer.go
const PLASMA_MEMQUOTA_FRAC = 0.9

// Indexer Node is a description of one index node used by Planner to track which indexes are
// planned to reside on this node.
type IndexerNode struct {
	// input: node identification
	NodeId      string `json:"nodeId"`
	NodeUUID    string `json:"nodeUUID"`
	IndexerId   string `json:"indexerId"`
	RestUrl     string `json:"restUrl"`
	ServerGroup string `json:"serverGroup,omitempty"`
	StorageMode string `json:"storageMode,omitempty"`

	// input/output: resource consumption (from sizing)
	MemUsage    uint64  `json:"memUsage"`
	CpuUsage    float64 `json:"cpuUsage"`
	DiskUsage   uint64  `json:"diskUsage,omitempty"`
	MemOverhead uint64  `json:"memOverhead"`
	DataSize    uint64  `json:"dataSize"`

	// input/output: resource consumption (from live cluster)
	ActualMemUsage    uint64  `json:"actualMemUsage"`
	ActualMemOverhead uint64  `json:"actualMemOverhead"`
	ActualCpuUsage    float64 `json:"actualCpuUsage"`
	ActualDataSize    uint64  `json:"actualDataSize"`
	ActualDiskUsage   uint64  `json:"actualDiskUsage"`
	ActualDrainRate   uint64  `json:"actualDrainRate"`
	ActualScanRate    uint64  `json:"actualScanRate"`
	ActualMemMin      uint64  `json:"actualMemMin"`
	ActualRSS         uint64  `json:"actualRSS"`
	ActualUnits       uint64  `json:"actualUnits"`
	MandatoryQuota    uint64  `json:"mandatoryQuota"`
	NumTenants        uint64  `json:"numTenants"`

	// input: index residing on the node
	Indexes []*IndexUsage `json:"indexes"`

	// input: node status
	isDelete bool
	isNew    bool
	Exclude  string

	// input/output: planning
	meetConstraint bool
	numEmptyIndex  int
	hasEligible    bool
	totalData      uint64
	totalIndex     uint64
	dataMovedIn    uint64
	indexMovedIn   uint64

	UsageRatio float64 `json:"usageRatio"`

	//for serverless subCluster grouping
	BucketsInRebalance map[string]bool

	// Planner will not generate alternate shardIds if the
	// NodeVersion is < 7.6
	NodeVersion int `json:"nodeVersion,omitempty"`
	// Shard level statistics

	// Current number of shards on the node
	// Derived from index grouping based on alternate shardId
	numShards int

	// Minium number of shards that can be created on the node
	// before trying to place indexes on existing shards
	// Derived from memory quota, flush buffer size and percentage of memory
	// required for flush buffers
	MinShardCapacity int `json:"minShardCapacity,omitempty"`

	memQuota uint64 // Indexer memory quota for this node

	// shard-level resource utilisation thresholds
	MaxInstancesPerShard uint64  `json:"maxInstancesPerShard,omitempty"`
	MaxDiskUsagePerShard uint64  `json:"maxDiskUsagePerShard,omitempty"`
	DiskUsageThreshold   float64 `json:"diskUsageThreshold,omitempty"`

	// ShardLimitPerTenant is a cluster wide property which gets decided on
	// three factors:
	// a. MinShardCapacity of the node
	// b. ShardTenantMultiplier
	// c. Value of "ShardLimitPerTenant" in config
	//
	// The final value will be min(MinShardCapacity * ShardTenantMultiplier, ShardLimitPerTenant)
	// As each indexer node can have different shard capacity (due to the support
	// of percentage memory quota), the ShardLimitPerTenant is tracked per indexer node
	// The maximum value across all indexer nodes will be considered for cluster wide limit
	ShardLimitPerTenant   int `json:"shardLimitPerTenant,omitempty"`
	ShardTenantMultiplier int `json:"shardTenantMultiplier,omitempty"`

	ShardStats map[string]*common.ShardStats
}

// This function creates a new indexer node
func newIndexerNode(nodeId string, sizing SizingMethod) *IndexerNode {

	r := &IndexerNode{
		NodeId:         nodeId,
		NodeUUID:       tempNodeUUID + nodeId,
		meetConstraint: true,
	}

	sizing.ComputeIndexerSize(r)

	return r
}

// This function creates a new indexer node.  This function expects that each index is already
// "sized".   If sizing method is provided, it will compute sizing for indexer as well.
func CreateIndexerNodeWithIndexes(nodeId string, sizing SizingMethod, indexes []*IndexUsage) *IndexerNode {

	r := &IndexerNode{
		NodeId:         nodeId,
		NodeUUID:       tempNodeUUID + nodeId,
		Indexes:        indexes,
		meetConstraint: true,
	}

	for _, index := range indexes {
		index.initialNode = r
	}

	if sizing != nil {
		sizing.ComputeIndexerSize(r)
	}

	return r
}

// Mark the node as deleted
func (o *IndexerNode) MarkDeleted() {

	o.isDelete = true
}

// Is indexer deleted?
func (o *IndexerNode) IsDeleted() bool {
	return o.isDelete
}

// Get a list of index usages that are moved to this node
func (o *IndexerNode) GetMovedIndex() []*IndexUsage {

	result := ([]*IndexUsage)(nil)
	for _, index := range o.Indexes {
		if index.initialNode == nil || index.initialNode.NodeId != o.NodeId {
			result = append(result, index)
		}
	}

	return result
}

// This function makes a copy of a indexer node.
func (o *IndexerNode) clone() *IndexerNode {

	r := indexerNodePool.Get()

	r.NodeId = o.NodeId
	r.NodeUUID = o.NodeUUID
	r.IndexerId = o.IndexerId
	r.RestUrl = o.RestUrl
	r.ServerGroup = o.ServerGroup
	r.StorageMode = o.StorageMode
	r.MemUsage = o.MemUsage
	r.MemOverhead = o.MemOverhead
	r.DataSize = o.DataSize
	r.CpuUsage = o.CpuUsage
	r.DiskUsage = o.DiskUsage
	r.Indexes = make([]*IndexUsage, len(o.Indexes))
	r.isDelete = o.isDelete
	r.isNew = o.isNew
	r.Exclude = o.Exclude
	r.ActualMemUsage = o.ActualMemUsage
	r.ActualMemMin = o.ActualMemMin
	r.ActualMemOverhead = o.ActualMemOverhead
	r.ActualCpuUsage = o.ActualCpuUsage
	r.ActualDataSize = o.ActualDataSize
	r.ActualDiskUsage = o.ActualDiskUsage
	r.ActualDrainRate = o.ActualDrainRate
	r.ActualScanRate = o.ActualScanRate
	r.MandatoryQuota = o.MandatoryQuota
	r.ActualUnits = o.ActualUnits
	r.meetConstraint = o.meetConstraint
	r.numEmptyIndex = o.numEmptyIndex
	r.hasEligible = o.hasEligible
	r.dataMovedIn = o.dataMovedIn
	r.indexMovedIn = o.indexMovedIn
	r.totalData = o.totalData
	r.totalIndex = o.totalIndex
	r.NodeVersion = o.NodeVersion
	r.numShards = o.numShards
	r.MinShardCapacity = o.MinShardCapacity
	r.memQuota = o.memQuota
	r.MaxInstancesPerShard = o.MaxInstancesPerShard
	r.MaxDiskUsagePerShard = o.MaxDiskUsagePerShard
	r.DiskUsageThreshold = o.DiskUsageThreshold
	r.ShardLimitPerTenant = o.ShardLimitPerTenant
	r.ShardTenantMultiplier = o.ShardTenantMultiplier

	r.ShardStats = o.ShardStats

	for i, _ := range o.Indexes {
		r.Indexes[i] = o.Indexes[i]
	}

	return r
}

func (o *IndexerNode) free() {

	indexerNodePool.Put(o)
}

// This function returns a string representing the indexer
func (o *IndexerNode) String() string {
	return o.NodeId
}

// Get the free memory and cpu usage of this node
func (o *IndexerNode) freeUsage(s *Solution, constraint ConstraintMethod) (uint64, float64) {

	freeMem := constraint.GetMemQuota() - o.GetMemTotal(s.UseLiveData())
	freeCpu := float64(constraint.GetCpuQuota()) - o.GetCpuUsage(s.UseLiveData())

	return freeMem, freeCpu
}

// Get cpu usage
func (o *IndexerNode) GetCpuUsage(useLive bool) float64 {

	if useLive {
		return o.ActualCpuUsage
	}

	return o.CpuUsage
}

// Add Cpu
func (o *IndexerNode) AddCpuUsage(s *Solution, usage float64) {

	if !s.UseLiveData() {
		o.CpuUsage += usage
	} else {
		o.ActualCpuUsage += usage
	}
}

// Subtract Cpu
func (o *IndexerNode) SubtractCpuUsage(s *Solution, usage float64) {

	if !s.UseLiveData() {
		o.CpuUsage -= usage
	} else {
		o.ActualCpuUsage -= usage
	}
}

// Get memory usage
func (o *IndexerNode) GetMemUsage(useLive bool) uint64 {

	if useLive {
		return o.ActualMemUsage
	}

	return o.MemUsage
}

// Get memory overhead
func (o *IndexerNode) GetMemOverhead(useLive bool) uint64 {

	if useLive {
		return o.ActualMemOverhead
	}

	return o.MemOverhead
}

// Get memory total
func (o *IndexerNode) GetMemTotal(useLive bool) uint64 {

	if useLive {
		return o.ActualMemUsage + o.ActualMemOverhead
	}

	return o.MemUsage + o.MemOverhead
}

// Get memory min
func (o *IndexerNode) GetMemMin(useLive bool) uint64 {

	if useLive {
		return o.ActualMemMin
	}

	return o.GetMemTotal(useLive)
}

// Add memory
func (o *IndexerNode) AddMemUsageOverhead(s *Solution, usage uint64, overhead uint64, min uint64) {

	if s.UseLiveData() {
		o.ActualMemUsage += usage
		o.ActualMemOverhead += overhead
		o.ActualMemMin += min
	} else {
		o.MemUsage += usage
		o.MemOverhead += overhead
	}
}

func (o *IndexerNode) AddMemUsage(s *Solution, usage uint64) {

	if s.UseLiveData() {
		o.ActualMemUsage += usage
	} else {
		o.MemUsage += usage
	}
}

// Subtract memory
func (o *IndexerNode) SubtractMemUsageOverhead(s *Solution, usage uint64, overhead uint64, min uint64) {

	if s.UseLiveData() {
		o.ActualMemUsage -= usage
		o.ActualMemOverhead -= overhead
		o.ActualMemMin -= min
	} else {
		o.MemUsage -= usage
		o.MemOverhead -= overhead
	}
}

func (o *IndexerNode) SubtractMemUsage(s *Solution, usage uint64) {

	if s.UseLiveData() {
		o.ActualMemUsage -= usage
	} else {
		o.MemUsage -= usage
	}
}

// Get data size
func (o *IndexerNode) GetDataSize(useLive bool) uint64 {

	if useLive {
		return o.ActualDataSize
	}

	return o.DataSize
}

// Add data size
func (o *IndexerNode) AddDataSize(s *Solution, datasize uint64) {

	if s.UseLiveData() {
		o.ActualDataSize += datasize
	} else {
		o.DataSize += datasize
	}
}

// Subtract data size
func (o *IndexerNode) SubtractDataSize(s *Solution, datasize uint64) {

	if s.UseLiveData() {
		o.ActualDataSize -= datasize
	} else {
		o.DataSize -= datasize
	}
}

// Get disk bps
func (o *IndexerNode) GetDiskUsage(useLive bool) uint64 {

	if useLive {
		return o.ActualDiskUsage
	}

	return 0
}

// Add disk bps
func (o *IndexerNode) AddDiskUsage(s *Solution, diskUsage uint64) {

	if s.UseLiveData() {
		o.ActualDiskUsage += diskUsage
	}
}

// Subtract disk bps
func (o *IndexerNode) SubtractDiskUsage(s *Solution, diskUsage uint64) {

	if s.UseLiveData() {
		o.ActualDiskUsage -= diskUsage
	}
}

// Get scan rate
func (o *IndexerNode) GetScanRate(useLive bool) uint64 {

	if useLive {
		return o.ActualScanRate
	}

	return 0
}

// Add scan rate
func (o *IndexerNode) AddScanRate(s *Solution, scanRate uint64) {

	if s.UseLiveData() {
		o.ActualScanRate += scanRate
	}
}

// Subtract scan rate
func (o *IndexerNode) SubtractScanRate(s *Solution, scanRate uint64) {

	if s.UseLiveData() {
		o.ActualScanRate -= scanRate
	}
}

// Get drain rate
func (o *IndexerNode) GetDrainRate(useLive bool) uint64 {

	if useLive {
		return o.ActualDrainRate
	}

	return 0
}

// Add drain rate
func (o *IndexerNode) AddDrainRate(s *Solution, drainRate uint64) {

	if s.UseLiveData() {
		o.ActualDrainRate += drainRate
	}
}

// Subtract drain rate
func (o *IndexerNode) SubtractDrainRate(s *Solution, drainRate uint64) {

	if s.UseLiveData() {
		o.ActualDrainRate -= drainRate
	}
}

// This function returns whether the indexer node should exclude the given index
func (o *IndexerNode) shouldExcludeIndex(s *Solution, n *IndexUsage) bool {
	if o.ExcludeIn(s) {
		return o.IsDeleted() || !(n.overrideExclude || n.initialNode.NodeId == o.NodeId)
	}
	return false
}

// This function returns whether to exclude this node for taking in new index
func (o *IndexerNode) ExcludeIn(s *Solution) bool {
	return !s.AllowDDLDuringScaleUp() && s.enableExclude && (o.IsDeleted() || o.Exclude == "in" || o.Exclude == "inout")
}

// This function returns whether to exclude this node for rebalance out index
func (o *IndexerNode) ExcludeOut(s *Solution) bool {
	return !s.AllowDDLDuringScaleUp() && s.enableExclude && (!o.IsDeleted() && (o.Exclude == "out" || o.Exclude == "inout"))
}

// This function returns whether to exclude this node for rebalance in or out any index
func (o *IndexerNode) ExcludeAny(s *Solution) bool {
	return o.ExcludeIn(s) || o.ExcludeOut(s)
}

// This function returns whether to exclude this node for rebalance any index
func (o *IndexerNode) ExcludeAll(s *Solution) bool {
	return o.ExcludeIn(s) && o.ExcludeOut(s)
}

// This function changes the exclude setting of a node
func (o *IndexerNode) SetExclude(exclude string) {
	o.Exclude = exclude
}

// This function changes the exclude setting of a node
func (o *IndexerNode) UnsetExclude() {
	o.Exclude = ""
}

// Does indexer satisfy constraint?
func (o *IndexerNode) SatisfyNodeConstraint() bool {
	return o.meetConstraint
}

// Evaluate if indexer satisfy constraint after adding/removing index
func (o *IndexerNode) EvaluateNodeConstraint(s *Solution, canAddIndex bool, idxToAdd *IndexUsage, idxRemoved *IndexUsage) {

	if idxToAdd != nil {
		if !canAddIndex {
			violations := s.constraint.CanAddIndex(s, o, idxToAdd)
			canAddIndex = violations == NoViolation
		}

		// If node does not meet constraint when an index is added,
		// the node must still not meeting constraint after the addition.
		o.meetConstraint = canAddIndex && o.meetConstraint
		return
	}

	if idxRemoved != nil {
		if o.meetConstraint {
			// If node already meet constraint when an index is removed,
			// the node must be meeting constraint after the removal.
			return
		}
	}

	if s.place != nil && s.constraint != nil {
		eligibles := s.place.GetEligibleIndexes()
		o.meetConstraint = s.constraint.SatisfyNodeConstraint(s, o, eligibles)
	}
}

// Evaluate node stats for planning purpose
func (o *IndexerNode) EvaluateNodeStats(s *Solution) {

	o.numEmptyIndex = 0
	o.totalData = 0
	o.totalIndex = 0
	o.dataMovedIn = 0
	o.indexMovedIn = 0
	o.hasEligible = false

	if s.place == nil {
		return
	}

	eligibles := s.place.GetEligibleIndexes()
	for _, index := range o.Indexes {

		// calculate num empty index
		if !index.HasSizing(s.UseLiveData()) {
			o.numEmptyIndex++
		}

		// has eligible index?
		if _, ok := eligibles[index]; ok {
			o.hasEligible = true
		}

		// calculate data movement
		// 1) only consider eligible index
		// 2) ignore cost of moving an index out of an to-be-deleted node
		// 3) ignore cost of moving to new node
		if s.command == CommandRebalance || s.command == CommandSwap || s.command == CommandRepair {
			if _, ok := eligibles[index]; ok {

				if index.initialNode != nil && !index.initialNode.isDelete {
					o.totalData += index.GetDataSize(s.UseLiveData())
					o.totalIndex++
				}

				if index.initialNode != nil && !index.initialNode.isDelete &&
					index.initialNode.NodeId != o.NodeId && !o.isNew {
					o.dataMovedIn += index.GetDataSize(s.UseLiveData())
					o.indexMovedIn++
				}
			}
		}
	}
}

// Evaluate indexer stats and constraints when node has changed
func (o *IndexerNode) Evaluate(s *Solution) {
	o.EvaluateNodeConstraint(s, false, nil, nil)
	o.EvaluateNodeStats(s)
}

// Evaluate free memory per empty index
func (o *IndexerNode) computeFreeMemPerEmptyIndex(s *Solution, maxThreshold uint64, indexToAdd int) uint64 {

	memPerEmpIndex := uint64(0)

	if maxThreshold > o.GetMemTotal(s.UseLiveData()) {
		freeMem := maxThreshold - o.GetMemTotal(s.UseLiveData())

		if o.numEmptyIndex+indexToAdd > 0 {
			memPerEmpIndex = freeMem / uint64(o.numEmptyIndex+indexToAdd)
		}
	}

	return memPerEmpIndex
}

// Set Indexes to the node
func (o *IndexerNode) SetIndexes(indexes []*IndexUsage) {
	o.Indexes = indexes

	for _, index := range indexes {
		index.initialNode = o
	}
}

// Add Indexes to the node
func (o *IndexerNode) AddIndexes(indexes []*IndexUsage) {
	o.Indexes = append(o.Indexes, indexes...)

	for _, index := range indexes {
		index.initialNode = o
	}
}

// Set Indexes to the node without setting the initialNode
func (o *IndexerNode) SetIndexes2(indexes []*IndexUsage) {
	o.Indexes = indexes
}

// Add Indexes to the node without setting the initialNode
func (o *IndexerNode) AddIndexes2(indexes []*IndexUsage) {
	o.Indexes = append(o.Indexes, indexes...)
}

// Compute Size of the node using sizing
func (o *IndexerNode) ComputeSizing(sizing SizingMethod) {
	if sizing != nil {
		sizing.ComputeIndexerSize(o)
	}
}

func (o *IndexerNode) ComputeMinShardCapacity(config common.Config) {
	flushBufferSz := config["indexer.plasma.sharedFlushBufferSize"].Int()
	flushBufferQuota := config["indexer.plasma.flushBufferQuota"].Float64()
	shardLimitPerNode := config["indexer.plasma.shardLimitPerNode"].Int()

	if flushBufferQuota == 0 || flushBufferSz == 0 {
		// Limit the number of shards at "shardLimitPerNode"
		logging.Warnf("IndexerNode::ComputeMinShardCapacity Limiting the number of shards at: %v. "+
			"FlushBufferQuota: %v, FlushBufferSize: %v, indexer: %v",
			shardLimitPerNode, flushBufferQuota, flushBufferSz, o.NodeId)
		o.MinShardCapacity = shardLimitPerNode
		return
	}

	o.MinShardCapacity = int((float64(o.memQuota) * PLASMA_MEMQUOTA_FRAC * (flushBufferQuota / 100.0)) / float64(flushBufferSz))

	// As each index requires 2 shards - one for main store and one for back store, always
	// make the maxShards value even
	if o.MinShardCapacity%2 != 0 {
		o.MinShardCapacity++
	}

	if o.MinShardCapacity > shardLimitPerNode {
		logging.Warnf("IndexerNode::ComputeMinShardCapacity Limiting the number of shards at: %v. "+
			"Num shards per quota: %v, indexer node: %v", shardLimitPerNode, o.MinShardCapacity, o.NodeId)
		o.MinShardCapacity = shardLimitPerNode
	}

	if o.ShardTenantMultiplier*o.MinShardCapacity != 0 {
		if o.ShardTenantMultiplier*o.MinShardCapacity < o.ShardLimitPerTenant {
			o.ShardLimitPerTenant = o.ShardTenantMultiplier * o.MinShardCapacity
		}
	}
}
