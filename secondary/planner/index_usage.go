package planner

import (
	"fmt"

	"github.com/couchbase/indexing/secondary/common"
)

// IndexUsage is a description of one instance of an index used by Planner to keep track of which
// indexes are planned to be on which nodes.
type IndexUsage struct {
	// input: index identification
	DefnId     common.IndexDefnId `json:"defnId"`
	InstId     common.IndexInstId `json:"instId"`
	PartnId    common.PartitionId `json:"partnId"`
	Name       string             `json:"name"`
	Bucket     string             `json:"bucket"`
	Scope      string             `json:"scope"`
	Collection string             `json:"collection"`
	Hosts      []string           `json:"host"`
	ShardIds   []common.ShardId   `json:"shardIds,omitempty"`

	// For all indexes created from 7.6+ version of planner and on
	// 7.6+ version of indexer, alternateShardIds will be non-nil
	AlternateShardIds []string `json:"alternateShardIds,omitempty"`

	// input: index sizing
	IsPrimary     bool    `json:"isPrimary,omitempty"`
	StorageMode   string  `json:"storageMode,omitempty"`
	AvgSecKeySize uint64  `json:"avgSecKeySize"`
	AvgDocKeySize uint64  `json:"avgDocKeySize"`
	AvgArrSize    uint64  `json:"avgArrSize"`
	AvgArrKeySize uint64  `json:"avgArrKeySize"`
	NumOfDocs     uint64  `json:"numOfDocs"`
	ResidentRatio float64 `json:"residentRatio,omitempty"`
	MutationRate  uint64  `json:"mutationRate"`
	DrainRate     uint64  `json:"drainRate"`
	ScanRate      uint64  `json:"scanRate"`

	// input: resource consumption (from sizing equation)
	MemUsage    uint64  `json:"memUsage"`
	CpuUsage    float64 `json:"cpuUsage"`
	DiskUsage   uint64  `json:"diskUsage,omitempty"`
	MemOverhead uint64  `json:"memOverhead,omitempty"`
	DataSize    uint64  `json:"dataSize,omitempty"`

	// input: resource consumption (from live cluster)
	ActualMemUsage     uint64  `json:"actualMemUsage"`
	ActualMemOverhead  uint64  `json:"actualMemOverhead"`
	ActualKeySize      uint64  `json:"actualKeySize"`
	ActualCpuUsage     float64 `json:"actualCpuUsage"`
	ActualBuildPercent uint64  `json:"actualBuildPercent"`
	// in planner for plasma we use combined resident percent from mainstore as well as backstore
	// so the value of stat "combined_resident_percent will be stored in ActualResidentPercent"
	ActualResidentPercent uint64 `json:"actualResidentPercent"`
	ActualDataSize        uint64 `json:"actualDataSize"`
	ActualNumDocs         uint64 `json:"actualNumDocs"`
	ActualDiskUsage       uint64 `json:"actualDiskUsage"`
	ActualMemStats        uint64 `json:"actualMemStats"`
	ActualDrainRate       uint64 `json:"actualDrainRate"`
	ActualScanRate        uint64 `json:"actualScanRate"`
	ActualMemMin          uint64 `json:"actualMemMin"`
	ActualUnitsUsage      uint64 `json:"actualUnitsUsage"`

	// input: resource consumption (estimated sizing)
	NoUsageInfo       bool   `json:"NoUsageInfo"`
	EstimatedMemUsage uint64 `json:"estimatedMemUsage"`
	EstimatedDataSize uint64 `json:"estimatedDataSize"`
	NeedsEstimate     bool   `json:"NeedsEstimate"`

	// input: index definition (optional)
	Instance *common.IndexInst `json:"instance,omitempty"`

	// input: node where index initially placed (optional)
	// for new indexes to be placed on an existing topology (e.g. live cluster), this must not be set.
	initialNode *IndexerNode

	// For replica repair, the index from which indexer will transfer
	// the data to destination node (Used only in shard rebalancer)
	siblingIndex *IndexUsage

	// The node to which planner moves this index from initialNode
	// Equals "initialNode" if planner does not move the index
	destNode *IndexerNode

	// input: flag to indicate if the index in delete or create token
	PendingDelete bool // true if there is a delete token associated with this index
	pendingCreate bool // true if there is a create token associated with this index
	pendingBuild  bool // true if there is a build token associated with this index

	// mutable: hint for placement / constraint
	suppressEquivIdxCheck bool

	// Copy of the index state from the corresponding index instance.
	state common.IndexState

	// Miscellaneous fields
	partnStatPrefix string
	instStatPrefix  string

	eligible        bool
	overrideExclude bool

	// stats for duplicate index removal logic
	numDocsQueued    int64
	numDocsPending   int64
	rollbackTime     int64
	progressStatTime int64
}

// This function makes a copy of a index usage
func (o *IndexUsage) clone() *IndexUsage {

	r := *o
	r.Hosts = nil
	r.initialNode = nil // should set to nil

	if o.Instance != nil {
		inst := *o.Instance
		r.Instance = &inst
	}

	return &r
}

// This function returns a string representing the index
func (o *IndexUsage) String() string {
	return fmt.Sprintf("%v:%v:%v", o.DefnId, o.InstId, o.PartnId)
}

// This function creates a new index usage
func newIndexUsage(defnId common.IndexDefnId, instId common.IndexInstId, partnId common.PartitionId,
	name, bucket, scope, collection string) *IndexUsage {

	return &IndexUsage{DefnId: defnId,
		InstId:     instId,
		PartnId:    partnId,
		Name:       name,
		Bucket:     bucket,
		Scope:      scope,
		Collection: collection,
	}
}

// Get cpu usage
func (o *IndexUsage) GetCpuUsage(useLive bool) float64 {

	if useLive {
		return o.ActualCpuUsage
	}

	return o.CpuUsage
}

// Get memory usage
func (o *IndexUsage) GetMemUsage(useLive bool) uint64 {

	if o.NeedsEstimation() {
		return o.EstimatedMemUsage
	}

	if useLive {
		return o.ActualMemUsage
	}

	return o.MemUsage
}

// Get memory overhead
func (o *IndexUsage) GetMemOverhead(useLive bool) uint64 {

	if useLive {
		return o.ActualMemOverhead
	}

	return o.MemOverhead
}

// Get total memory
func (o *IndexUsage) GetMemTotal(useLive bool) uint64 {

	if o.NeedsEstimation() {
		return o.EstimatedMemUsage
	}

	if useLive {
		return o.ActualMemUsage + o.ActualMemOverhead
	}

	return o.MemUsage + o.MemOverhead
}

// Get data size
func (o *IndexUsage) GetDataSize(useLive bool) uint64 {

	if o.NeedsEstimation() {
		return o.EstimatedDataSize
	}

	if useLive {
		return o.ActualDataSize
	}

	return o.DataSize
}

// Get disk Usage
func (o *IndexUsage) GetDiskUsage(useLive bool) uint64 {

	if o.NeedsEstimation() {
		return 0
	}

	if useLive {
		return o.ActualDiskUsage
	}

	return 0
}

// Get scan rate
func (o *IndexUsage) GetScanRate(useLive bool) uint64 {

	if o.NeedsEstimation() {
		return 0
	}

	if useLive {
		return o.ActualScanRate
	}

	return 0
}

// Get drain rate
func (o *IndexUsage) GetDrainRate(useLive bool) uint64 {

	if o.NeedsEstimation() {
		return 0
	}

	if useLive {
		return o.ActualDrainRate
	}

	return 0
}

// Get minimum memory usage
func (o *IndexUsage) GetMemMin(useLive bool) uint64 {

	if o.NeedsEstimation() {
		return o.EstimatedMemUsage
	}

	if useLive {
		return o.ActualMemMin
	}

	return o.GetMemTotal(useLive)
}

// Get resident ratio
func (o *IndexUsage) GetResidentRatio(useLive bool) float64 {

	var ratio float64
	if useLive {
		ratio = float64(o.ActualResidentPercent)
		return ratio
	} else {
		ratio = o.ResidentRatio
	}

	if ratio == 0 {
		ratio = 100
	}

	return ratio
}

// Get build percent
func (o *IndexUsage) GetBuildPercent(useLive bool) uint64 {

	if useLive {
		return o.ActualBuildPercent
	}

	return 100
}

func (o *IndexUsage) HasSizing(useLive bool) bool {

	if o.NeedsEstimation() {
		return o.EstimatedMemUsage != 0
	}

	if useLive {
		//return o.ActualMemUsage != 0 || o.ActualCpuUsage != 0 || o.ActualDataSize != 0
		return o.ActualMemUsage != 0 || o.ActualDataSize != 0
	}

	//return o.MemUsage != 0 || o.CpuUsage != 0 || o.DataSize != 0
	return o.MemUsage != 0 || o.DataSize != 0
}

func (o *IndexUsage) HasSizingInputs() bool {

	return o.NumOfDocs != 0 && ((!o.IsPrimary && o.AvgSecKeySize != 0) || (o.IsPrimary && o.AvgDocKeySize != 0))
}

func (o *IndexUsage) ComputeSizing(useLive bool, sizing SizingMethod) {

	// Compute Sizing.  This can be based on either sizing inputs or real index stats.
	// If coming from real index stats, this will not overwrite the derived sizing from stats (e.g. ActualMemUsage).
	sizing.ComputeIndexSize(o)

	if useLive && o.HasSizingInputs() {
		// If an index has sizing inputs, but it does not have actual sizing stats in a live cluster, then
		// use the computed sizing formula for planning and rebalancing.
		// The index could be a new index to be placed, or an existing index already in cluster.
		// For an existing index in cluster that does not have actual sizing stats:
		// 1) A deferred index that has yet to be built
		// 2) A index still waiting to be create/build (from create token)
		// 3) A index from an empty bucket
		//if o.ActualMemUsage == 0 && o.ActualDataSize == 0 && o.ActualCpuUsage == 0 {
		if o.ActualMemUsage == 0 && o.ActualDataSize == 0 {
			o.ActualMemUsage = o.MemUsage
			o.ActualDataSize = o.DataSize
			o.ActualCpuUsage = o.CpuUsage
			// do not copy mem overhead since this is usually over-estimated
			o.ActualMemOverhead = 0
		}
	}

	if !useLive {
		//o.NoUsageInfo = o.MemUsage == 0 && o.CpuUsage == 0 && o.DataSize == 0
		o.NoUsageInfo = o.MemUsage == 0 && o.DataSize == 0
	} else {
		//o.NoUsageInfo = o.ActualMemUsage == 0 && o.ActualCpuUsage == 0 && o.ActualDataSize == 0
		o.NoUsageInfo = o.ActualMemUsage == 0 && o.ActualDataSize == 0
	}
}

func (o *IndexUsage) GetDisplayName() string {

	if o.Instance == nil {
		return o.Name
	}

	return common.FormatIndexPartnDisplayName(o.Instance.Defn.Name, o.Instance.ReplicaId, int(o.PartnId), true)
}

func (o *IndexUsage) GetStatsName() string {

	return o.GetDisplayName()
}

func (o *IndexUsage) GetInstStatsName() string {

	if o.Instance == nil {
		return o.Name
	}

	return common.FormatIndexInstDisplayName(o.Instance.Defn.Name, o.Instance.ReplicaId)
}

func (o *IndexUsage) GetPartnStatsPrefix() string {
	if o.Instance == nil {
		return common.GetStatsPrefix(o.Bucket, o.Scope, o.Collection, o.Name, 0, 0, false)
	}

	return common.GetStatsPrefix(o.Instance.Defn.Bucket, o.Instance.Defn.Scope,
		o.Instance.Defn.Collection, o.Instance.Defn.Name, o.Instance.ReplicaId,
		int(o.PartnId), true)
}

func (o *IndexUsage) GetInstStatsPrefix() string {
	if o.Instance == nil {
		return common.GetStatsPrefix(o.Bucket, o.Scope, o.Collection, o.Name, 0, 0, false)
	}

	return common.GetStatsPrefix(o.Instance.Defn.Bucket, o.Instance.Defn.Scope,
		o.Instance.Defn.Collection, o.Instance.Defn.Name, 0, 0, false)
}

func (o *IndexUsage) GetPartitionName() string {

	if o.Instance == nil {
		return o.Name
	}

	return common.FormatIndexPartnDisplayName(o.Instance.Defn.Name, 0, int(o.PartnId), true)
}

func (o *IndexUsage) GetReplicaName() string {

	if o.Instance == nil {
		return o.Name
	}

	return common.FormatIndexPartnDisplayName(o.Instance.Defn.Name, o.Instance.ReplicaId, 0, false)
}

func (o *IndexUsage) IsReplica(other *IndexUsage) bool {

	return o.DefnId == other.DefnId && o.PartnId == other.PartnId
}

func (o *IndexUsage) IsSameIndex(other *IndexUsage) bool {

	return o.DefnId == other.DefnId
}

func (o *IndexUsage) IsSameInst(other *IndexUsage) bool {

	return o.DefnId == other.DefnId && o.InstId == other.InstId
}

func (o *IndexUsage) IsSamePartition(other *IndexUsage) bool {

	return o.PartnId == other.PartnId
}

func (o *IndexUsage) IsEquivalentIndex(other *IndexUsage, checkSuppress bool) bool {

	if o.IsSameIndex(other) {
		return false
	}

	if !o.IsSamePartition(other) {
		return false
	}

	// suppressEquivCheck is only enabled for eligible index.  So as long as one index
	// has suppressEquivCheck, we should not check.
	if !checkSuppress || !o.suppressEquivIdxCheck && !other.suppressEquivIdxCheck {
		if o.Instance != nil && other.Instance != nil {
			return common.IsEquivalentIndex(&o.Instance.Defn, &other.Instance.Defn)
		}
	}

	return false
}

func (o *IndexUsage) IsMOI() bool {

	return o.StorageMode == common.MemoryOptimized || o.StorageMode == common.MemDB
}

func (o *IndexUsage) IsPlasma() bool {

	return o.StorageMode == common.PlasmaDB
}

func (o *IndexUsage) IsNew() bool {
	return o.initialNode == nil
}

func (o *IndexUsage) IndexOnCollection() bool {
	// Empty scope OR collection name is not expected. Assume that the index
	// definition is not upgraded yet.
	if o.Scope == "" || o.Collection == "" {
		return false
	}

	if o.Scope == common.DEFAULT_SCOPE && o.Collection == common.DEFAULT_COLLECTION {
		return false
	}

	return true
}

func (o *IndexUsage) NeedsEstimation() bool {
	return o.NoUsageInfo || o.NeedsEstimate
}
