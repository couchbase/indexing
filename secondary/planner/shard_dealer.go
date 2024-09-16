package planner

import (
	"fmt"

	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
)

type ShardCategory uint8

const (
	// default shard category only to be used for empty struct initialisation
	DEFAULT_SHARD_CATEGORY ShardCategory = iota // 0
	// shards for standard plasma indexes. also any index whose type is not recognised by
	// older versions of planner will fall back to this category
	STANDARD_SHARD_CATEGORY // 1
	// shards for plasma based composite vector indexes
	VECTOR_SHARD_CATEGORY // 2
	// shards for vector indexes with bhive storgae
	BHIVE_SHARD_CATEGORY // 3
	// indexes which do not require shard, for eg MOI or fdb
	INVALID_SHARD_CATEGORY = 255 // 255
)

func (ic ShardCategory) String() string {
	switch ic {
	case DEFAULT_SHARD_CATEGORY:
		return "DefaultShardCategory"
	case STANDARD_SHARD_CATEGORY:
		return "StandardShardCategory"
	case VECTOR_SHARD_CATEGORY:
		return "VectorShardCategory"
	case BHIVE_SHARD_CATEGORY:
		return "BHiveShardCategory"
	case INVALID_SHARD_CATEGORY:
		return "InvalidShardCategory"
	default:
		return "InvalidShardCategory"
	}
}

func getIndexCategory(partn *IndexUsage) ShardCategory {
	if partn.IsPlasma() {
		if partn.Instance != nil && partn.Instance.Defn.IsVectorIndex {
			if partn.Instance.Defn.VectorMeta != nil && partn.Instance.Defn.VectorMeta.IsBhive {
				return BHIVE_SHARD_CATEGORY
			}
			return VECTOR_SHARD_CATEGORY
		}
		return STANDARD_SHARD_CATEGORY
	}
	return INVALID_SHARD_CATEGORY
}

type pseudoShardContainer struct {
	insts              map[c.IndexInstId][]*IndexUsage // instId to multiple partitions
	totalPartitions    uint64
	memUsage           uint64 // derived from index cumulation
	diskUsage          uint64 // derived from index cumulation
	dataSize           uint64 // derived from index cumulation
	memUsageFromStats  uint64 // populated from indexer stats
	diskUsageFromStats uint64 // populated from indexer stats
	dataSizeFromStats  uint64 // populated from indexer stats
}

func newPseudoShardContainer() *pseudoShardContainer {
	return &pseudoShardContainer{
		insts:              make(map[c.IndexInstId][]*IndexUsage),
		totalPartitions:    0,
		memUsage:           0,
		diskUsage:          0,
		dataSize:           0,
		memUsageFromStats:  0,
		diskUsageFromStats: 0,
		dataSizeFromStats:  0,
	}
}

// record inst in shard container. returns true if the index is added else returns false
func (psc *pseudoShardContainer) addInstToShardContainer(index *IndexUsage) bool {
	if index == nil {
		return false
	}
	if psc.insts == nil {
		psc.insts = make(map[c.IndexInstId][]*IndexUsage)
	}

	for _, partn := range psc.insts[index.InstId] {
		if partn.PartnId == index.PartnId {
			return false
		}
	}

	psc.insts[index.InstId] = append(psc.insts[index.InstId], index)
	psc.totalPartitions++
	psc.memUsage += index.ActualMemUsage
	psc.diskUsage += index.ActualDiskSize
	psc.dataSize += index.ActualDataSize
	return true
}

// ShardDealer is a shard distributor on cluster level. it is a part of the solution from planner
type ShardDealer struct {
	// slots per category
	slotsPerCategory map[ShardCategory]map[c.AlternateShard_SlotId]bool
	// cluster level picture
	slotsMap map[c.AlternateShard_SlotId]map[c.AlternateShard_ReplicaId]map[c.AlternateShard_GroupId]*pseudoShardContainer
	// defnId to slotId
	indexSlots map[c.IndexDefnId]c.AlternateShard_SlotId

	// per node pic of which shard pair belongs to which node
	nodeToSlotMap      map[string]map[c.AlternateShard_SlotId]c.AlternateShard_ReplicaId
	nodeToSlotCountMap map[string]uint64

	// config
	minShardsPerNode      uint64
	minPartitionsPerShard uint64
	shardCapacityPerNode  uint64
}

func (sd *ShardDealer) logDealerConfig() {
	logging.Infof(
		"ShardDealer::logDealerConfig: config - minShardsPerNode %v; minPartitionsPerShard %v; shardCapacityPerNode - %v;",
		sd.minShardsPerNode, sd.minPartitionsPerShard, sd.shardCapacityPerNode)
}

func NewShardDealer(minShardsPerNode, minPartitionsPerShard, shardCapacity uint64) *ShardDealer {
	return &ShardDealer{
		minShardsPerNode:      minShardsPerNode,
		minPartitionsPerShard: minPartitionsPerShard,
		shardCapacityPerNode:  shardCapacity,

		slotsPerCategory:   make(map[ShardCategory]map[c.AlternateShard_SlotId]bool),
		slotsMap:           make(map[c.AlternateShard_SlotId]map[c.AlternateShard_ReplicaId]map[c.AlternateShard_GroupId]*pseudoShardContainer),
		indexSlots:         make(map[c.IndexDefnId]c.AlternateShard_SlotId),
		nodeToSlotMap:      make(map[string]map[c.AlternateShard_SlotId]c.AlternateShard_ReplicaId),
		nodeToSlotCountMap: make(map[string]uint64),
	}
}

// RecordIndexUsage takes 2 parameters - index and node so that the dealer can record an already
// created shard in its book keeping; node is required so it can track the node this index to record
// for as it can be either destination node or initial node record always happen using initialASIs
// as they are not expected to change.
// if index, or initialASI is nil, this func does not return an error but node cannot be nil
func (sd *ShardDealer) RecordIndexUsage(index *IndexUsage, node *IndexerNode) error {
	if index == nil || len(index.InitialAlternateShardIds) == 0 {
		return nil
	}

	if index.IsShardProxy {
		// TODO: handle adding shard proxies recursively
		// or we can also use the shard proxy in a separate call to also use the stats from shard
		// proxy (reported by shard directly)
	}

	var category = getIndexCategory(index)
	if category == INVALID_SHARD_CATEGORY {
		return fmt.Errorf("invalid shard category for index defn %v", index.DefnId)
	}

	// calculate alternate shard id of main index
	var alternateShardId, err = c.ParseAlternateId(index.InitialAlternateShardIds[0])
	if err != nil {
		return err
	}

	// record slot id in category
	var slotId = alternateShardId.GetSlotId()
	if sd.slotsPerCategory == nil {
		sd.slotsPerCategory = make(map[ShardCategory]map[c.AlternateShard_SlotId]bool)
	}
	if sd.slotsPerCategory[category] == nil {
		sd.slotsPerCategory[category] = make(map[c.AlternateShard_SlotId]bool)
	}
	sd.slotsPerCategory[category][slotId] = true

	// record defnId in indexSlots map
	if sd.indexSlots == nil {
		sd.indexSlots = make(map[c.IndexDefnId]c.AlternateShard_SlotId)
	}
	sd.indexSlots[index.DefnId] = slotId

	// record index in slotsMap
	if sd.slotsMap == nil {
		sd.slotsMap = make(map[c.AlternateShard_SlotId]map[c.AlternateShard_ReplicaId]map[c.AlternateShard_GroupId]*pseudoShardContainer)
	}
	if sd.slotsMap[slotId] == nil {
		sd.slotsMap[slotId] = make(map[c.AlternateShard_ReplicaId]map[c.AlternateShard_GroupId]*pseudoShardContainer)
	}
	if sd.slotsMap[slotId][alternateShardId.GetReplicaId()] == nil {
		sd.slotsMap[slotId][alternateShardId.GetReplicaId()] = make(map[c.AlternateShard_GroupId]*pseudoShardContainer)
	}
	if sd.slotsMap[slotId][alternateShardId.GetReplicaId()][alternateShardId.GetGroupId()] == nil {
		sd.slotsMap[slotId][alternateShardId.GetReplicaId()][alternateShardId.GetGroupId()] = newPseudoShardContainer()
	}
	var isNewPartn = sd.slotsMap[slotId][alternateShardId.GetReplicaId()][alternateShardId.GetGroupId()].addInstToShardContainer(index)

	// record new partn in a shard count only if the partn is new to the shard container
	if isNewPartn {
		if sd.nodeToSlotCountMap == nil {
			sd.nodeToSlotCountMap = make(map[string]uint64)
		}
		if _, exists := sd.nodeToSlotCountMap[node.NodeUUID]; !exists {
			sd.nodeToSlotCountMap[node.NodeUUID] = 0
		}
		sd.nodeToSlotCountMap[node.NodeUUID]++
		if !index.IsPrimary {
			sd.nodeToSlotCountMap[node.NodeUUID]++
		}
	}

	// record what slot ids are present on which node
	if sd.nodeToSlotMap == nil {
		sd.nodeToSlotMap = make(map[string]map[c.AlternateShard_SlotId]c.AlternateShard_ReplicaId)
	}
	if sd.nodeToSlotMap[node.NodeUUID] == nil {
		sd.nodeToSlotMap[node.NodeUUID] = make(map[c.AlternateShard_SlotId]c.AlternateShard_ReplicaId)
	}
	sd.nodeToSlotMap[node.NodeUUID][slotId] = alternateShardId.GetReplicaId()

	return nil
}
