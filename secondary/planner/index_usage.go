package planner

import (
	"fmt"
	"strings"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
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
	AlternateShardIds        []string `json:"alternateShardIds,omitempty"`
	InitialAlternateShardIds []string `json:"initialAlternateShardIds,omitempty"`

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
	ActualResidentPercent float64 `json:"actualResidentPercent"`
	ActualDataSize        uint64  `json:"actualDataSize"`
	ActualNumDocs         uint64  `json:"actualNumDocs"`
	ActualDiskUsage       uint64  `json:"actualDiskUsage"`
	ActualMemStats        uint64  `json:"actualMemStats"`
	ActualDrainRate       uint64  `json:"actualDrainRate"`
	ActualScanRate        uint64  `json:"actualScanRate"`
	ActualMemMin          uint64  `json:"actualMemMin"`
	ActualUnitsUsage      uint64  `json:"actualUnitsUsage"`
	// For Vector Index (Composite & Bhive), this will be populated, for other cases, this will be zero value
	ActualCodebookMemUsage uint64 `json:"actualCodebookMemUsage"`

	// Available from 7.6+ version of server

	// When grouping indexes based on alternate shard Id, the
	// ActualRecsInMem and TotalRecords for each index are
	// added to compute the resident ratio of the proxy index
	ActualRecsInMem uint64 `json:"actualRecsInMem"`
	TotalRecords    uint64 `json:"totalRecords"`

	// This field captures the actual size of the index on disk (including fragmentation)
	ActualDiskSize uint64 `json:"actualDiskSize,omitempty"`

	// input: resource consumption (estimated sizing)
	NoUsageInfo          bool   `json:"NoUsageInfo"`
	EstimatedMemUsage    uint64 `json:"estimatedMemUsage"`
	EstimatedDataSize    uint64 `json:"estimatedDataSize"`
	EstimatedCodebookMem uint64 `json:"estimatedCodebookMemUsage"`
	NeedsEstimate        bool   `json:"NeedsEstimate"`

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

	// Shard level information
	// Set to true if this structure is a proxy for shard
	IsShardProxy bool

	// Number of instances on this proxy shard
	NumInstances uint64

	// Maximum instances that can be assigned to a shard
	MaxInstances uint64

	// When index is used as shard proxy, this field represents the
	// maximum amount of data each shard can hold
	MaxDiskSize uint64

	// When index is used as shard proxy, this field represents the
	// percentage of MaxDiskSize that can be used as a cut-off to
	// decide if the shard can be used for new indexes.
	// If the 'ActualDiskUsage' of a shard is greater than
	// 'DiskSizeThreshold'* 'MaxDiskSize', then the shard will not be
	// used for placing new indexes
	DiskSizeThreshold float64

	// When index is shard proxy, this field captures the list of all indexes
	// that are grouped to make the proxy index
	GroupedIndexes []*IndexUsage

	// When this index is grouped under a proxy, the "ProxyIndex" field
	// holds the reference to the proxy
	ProxyIndex *IndexUsage
}

// This function makes a copy of a index usage
func (o *IndexUsage) clone() *IndexUsage {

	r := *o
	r.Hosts = nil
	r.initialNode = nil // should set to nil
	r.InitialAlternateShardIds = nil

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
		InstId:       instId,
		PartnId:      partnId,
		Name:         name,
		Bucket:       bucket,
		Scope:        scope,
		Collection:   collection,
		NumInstances: 1,
	}
}

func newProxyIndexUsage(slotId uint64, replicaId int) (*IndexUsage, error) {

	// Build proxy defnId based on slotId so that planner will not move
	// this IndexUsage to a node that already has same alternate ID but
	// different replica number
	//
	// As instances get shared between shards with same slotID, during
	// rebalance, if planner decides to move the proxy to a node that
	// contains same slotID, a replica violation will be generated and
	// planner can choose to skip the movement
	proxyDefnId := common.IndexDefnId(slotId)

	retry := 0
	var proxyInstId common.IndexInstId
	var err error
	for retry < 10 {
		proxyInstId, err = common.NewIndexInstId()
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
		retry++
	}
	if err != nil {
		logging.Errorf("newProxyIndexUsage: Error observed while generating indexInstId. err: %v", err)
		return nil, err
	}

	slotName := fmt.Sprintf("shard_proxy_%v", slotId)

	msAltId := &common.AlternateShardId{SlotId: slotId, ReplicaId: uint8(replicaId), GroupId: 0}
	bsAltId := &common.AlternateShardId{SlotId: slotId, ReplicaId: uint8(replicaId), GroupId: 1}

	proxy := &IndexUsage{
		DefnId:            proxyDefnId,
		InstId:            proxyInstId,
		Name:              slotName,
		IsShardProxy:      true,
		AlternateShardIds: []string{msAltId.String(), bsAltId.String()},
		Instance: &common.IndexInst{
			InstId:    proxyInstId,
			ReplicaId: replicaId,
			Defn: common.IndexDefn{
				DefnId:    common.IndexDefnId(proxyDefnId),
				InstId:    proxyInstId,
				Name:      slotName,
				ReplicaId: replicaId,
			},
		},

		// Since each proxy can hold variable number of instances,
		// supress equivalent index check for proxy
		// E.g. proxy-1 can have i1, i2, i3
		//      proxy-2 can have i1 (replica 1), i2 (replica 1) - No i3
		suppressEquivIdxCheck: true,
		eligible:              true,
		overrideExclude:       false,
	}

	return proxy, nil

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

// Get the memory taken by codebook of Composite Vector Indexes
func (o *IndexUsage) GetCodebookMemUsage(useLive bool) uint64 {

	// currently there is no estimation for Codebook mem usage. Return 0 mem usage
	if o.NeedsEstimation() {
		return o.EstimatedCodebookMem
	}

	if useLive {
		return o.ActualCodebookMemUsage
	}
	// if no live data is present, return 0
	return 0
}

// Get total memory
func (o *IndexUsage) GetMemTotal(useLive bool) uint64 {

	if o.NeedsEstimation() {
		return o.EstimatedMemUsage
	}

	if useLive {
		return o.ActualMemUsage + o.ActualMemOverhead + o.GetCodebookMemUsage(useLive)
	}

	return o.MemUsage + o.MemOverhead + o.GetCodebookMemUsage(useLive)
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

// Get disk consumption
func (o *IndexUsage) GetDiskSize() uint64 {
	return o.ActualDiskSize
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

func (o *IndexUsage) GetKeyspaceIndexPartitionName() string {

	return fmt.Sprintf("%v:%v:%v:%v:%v", o.Bucket, o.Scope, o.Collection, o.Name, o.PartnId)
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

	// Either both indexes have to be shard proxy (or) both
	// should not be shard proxy
	if o.IsShardProxy != other.IsShardProxy {
		return false
	}

	// suppressEquivCheck is only enabled for eligible index.  So as long as one index
	// has suppressEquivCheck, we should not check.
	if !checkSuppress || !o.suppressEquivIdxCheck && !other.suppressEquivIdxCheck {
		if o.IsShardProxy == false && other.IsShardProxy == false {
			if o.Instance != nil && other.Instance != nil {
				return common.IsEquivalentIndex(&o.Instance.Defn, &other.Instance.Defn)
			}
		} else {

			findEquivalent := func(src, tgt *IndexUsage) bool {
				for _, index_src := range src.GroupedIndexes {
					foundEquivalent := false
					for _, index_tgt := range tgt.GroupedIndexes {
						if index_src.IsEquivalentIndex(index_tgt, checkSuppress) {
							foundEquivalent = true
							break
						}
					}

					if !foundEquivalent {
						return false
					}
				}
				return true
			}

			// Return true only if all indexes in both proxies are equivalent.
			return findEquivalent(o, other) && findEquivalent(other, o)
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

func (o *IndexUsage) HasAlternateShardIds() bool {
	return len(o.AlternateShardIds) > 0
}

func (o *IndexUsage) AddToGroupedIndexes(in *IndexUsage) {
	o.GroupedIndexes = append(o.GroupedIndexes, in)

	// Case-1: All indices have eligible = true.
	// 		This is a case of shard repair and we need eligible as true
	// Case-2: Atleast one index has eligible = true. Others have eligible = false
	//		This is a case of replica repair where we are adding an index to an existing shard for repair
	//		In this case, only the replica is do be added to that proxy but the proxy does not move
	//		So eligible remains false
	// Case-3: All indices have eligible = false
	//		Here there is no need for any movement
	o.eligible = o.eligible && in.eligible

	// Case-1: All indices have overrideExclude = true.
	// 		This is a case of shard repair and we need overrideExclude as true
	// Case-2: Atleast one index has overrideExclude = true. Others have overrideExclude = false
	//		This is a case of replica repair where we are adding an index to an existing shard for repair
	//		In this case, we need overrideExclude as true
	// Case-3: All indices have overrideExclude = false
	//		Here there is no need for repair so we can ignore overrideExclude
	o.overrideExclude = o.overrideExclude || in.overrideExclude

	in.ProxyIndex = o
}

func (o *IndexUsage) Union(in *IndexUsage) {
	o.ActualDataSize += in.ActualDataSize
	o.ActualNumDocs += in.ActualNumDocs
	o.ActualDiskUsage += in.ActualDiskUsage
	o.ActualDrainRate += in.ActualDrainRate
	o.ActualScanRate += in.ActualScanRate
	o.ActualMemStats += in.ActualMemStats
	o.ActualMemUsage += in.ActualMemUsage
	o.ActualMemMin += in.ActualMemMin
	o.ActualMemOverhead += in.ActualMemOverhead
	o.ActualCpuUsage += in.ActualCpuUsage

	o.MutationRate += in.MutationRate
	o.DrainRate += in.DrainRate

	o.ActualResidentPercent += in.ActualResidentPercent
	o.ActualBuildPercent += in.ActualBuildPercent

	o.ActualRecsInMem += in.ActualRecsInMem
	o.TotalRecords += in.TotalRecords
	// For proxied shards, we will recalculate this value from ShardStats
	o.ActualDiskSize += in.ActualDiskSize
	// Accumulate the codebook memory for the Indexes present on the Shard Proxy
	o.ActualCodebookMemUsage += in.ActualCodebookMemUsage

	// If the first index in the list is a primay index, then we can end-up
	// copying only one shardId. Hence, always copy until we see a secondary
	// index which will have 2 shardIds. Also, use the shardIds only from
	// indexes which have valid initialAlternateShardIds
	if len(o.ShardIds) < 2 && len(in.InitialAlternateShardIds) > 0 {
		o.ShardIds = in.ShardIds
	}
}

func (o *IndexUsage) Normalize() {

	// Normalize build percent
	o.ActualBuildPercent = o.ActualBuildPercent / o.NumInstances
	o.ActualDrainRate = max(o.MutationRate, o.DrainRate)

	if o.TotalRecords > 0 {
		o.ActualResidentPercent = float64(o.ActualRecsInMem) * 100.0 / float64(o.TotalRecords)
	} else {
		o.ActualResidentPercent = o.ActualResidentPercent / float64(o.NumInstances)
	}

	// Compute the AcutalKeySize
	if o.ActualNumDocs > 0 {
		o.ActualKeySize = o.ActualDataSize / o.ActualNumDocs
	}
}

func (o *IndexUsage) SetInitialNode() {
	if o.IsShardProxy == false {
		return
	}

	// Even if one index of the grouped instances has a non-nil initialNode,
	// and initial alternate shardIds are non-nil, then
	// it means that there exists a node in the cluster containing the
	// alternateShardId relevant to this proxy usage. Hence, set the initialNode
	// of that index as the initialNode of proxy usage.
	//
	// If all the indexes of proxy usage have "nil" initialNode values, it means
	// that the entire shard has to be rebuilt using DCP. In such a case, set the initial
	// node as the first non-nil initial node in grouped indexes.
	for _, in := range o.GroupedIndexes {
		if in.initialNode != nil && len(in.InitialAlternateShardIds) > 0 {
			o.initialNode = in.initialNode
			break
		}
	}

	// If no index exists with valid initialAlternateShardIds, use the first non-nil
	// initialNode as initialNode as shard proxy. This will ensure that the shard is
	// not treated as shard being repaired and can filter cyclic movements
	if o.initialNode == nil {
		for _, in := range o.GroupedIndexes {
			if in.initialNode != nil {
				o.initialNode = in.initialNode
				break
			}
		}
	}

}

func (in *IndexUsage) updateProxyStats(shardStats *common.ShardStats) {
	in.ActualDataSize = uint64(shardStats.LSSDataSize)
	in.ActualNumDocs = uint64(shardStats.ItemsCount)
	in.ActualMemUsage = uint64(shardStats.MemSz) + uint64(shardStats.MemSzIndex)
	in.ActualRecsInMem = uint64(shardStats.CachedRecords)
	in.TotalRecords = uint64(shardStats.TotalRecords)
	in.ActualDiskSize = uint64(shardStats.LSSDiskSize)

	// For indexes getting added to the proxy as a part of replica repair (or)
	// due to upgrade, shard stats may not have the index information. Update
	// the stats with the indexes getting newly added
	for _, index := range in.GroupedIndexes {
		found := false
		for instPath, _ := range shardStats.Instances {
			if strings.Contains(instPath, fmt.Sprintf("%v_%v", index.Instance.InstId, index.PartnId)) {
				found = true
				break
			}
		}

		// Instance is not present in shard statistics. Add stats of the index to the shard
		// related statistics
		if !found {
			in.ActualDataSize += index.ActualDataSize
			in.ActualNumDocs += index.ActualNumDocs
			in.ActualMemUsage += index.MemUsage
			in.ActualRecsInMem += index.ActualRecsInMem
			in.TotalRecords += index.TotalRecords
			in.ActualDiskSize += index.ActualDiskSize
		}
	}

	if in.TotalRecords > 0 {
		in.ActualResidentPercent = (float64(in.ActualRecsInMem) * 100.0) / float64(in.TotalRecords)
	}
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
