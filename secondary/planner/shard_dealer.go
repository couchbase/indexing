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
func (sd *ShardDealer) RecordIndexUsage(index *IndexUsage, node *IndexerNode, isInit bool) error {
	if index == nil {
		return nil
	}

	if isInit && len(index.InitialAlternateShardIds) == 0 {
		return nil
	} else if !isInit && len(index.AlternateShardIds) == 0 {
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

	var inputAlternateShardIDs = index.AlternateShardIds
	if isInit {
		inputAlternateShardIDs = index.InitialAlternateShardIds
	}

	// calculate alternate shard id of main index
	var alternateShardId, err = c.ParseAlternateId(inputAlternateShardIDs[0])
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

// GetSlot - returns an appropriate Slot to place the indexes of the defn `defnId` into
// This could be a new slot or it could be an old slot being re-used
// GetSlot is the implementation of the 3 pass shard distribution
func (sd *ShardDealer) GetSlot(defnID c.IndexDefnId,
	replicaMap map[int]map[*IndexerNode]*IndexUsage) c.AlternateShard_SlotId {

	var mainstoreShard, backstoreShard *c.AlternateShardId

	// setStoreAnAllUsages is util func to set mainstoreShard, backstoreShard on all
	// index usages in replica map. only call once mainstore and backstore have the SlotID and
	// GroupID initialised. This func will set the ReplicaID to the shards. It does *not* update
	// internal book keeping of the Shard Dealer
	var setStoreOnAllUsages = func() {
		for replicaID, nodeMap := range replicaMap {
			for idxrNode, indexUsage := range nodeMap {
				if indexUsage == nil {
					logging.Warnf("ShardDealer::GetShard: nil index {defnID: %v, replicaID: %v, partnID: %v, nodeUUID: %v}. skipping",
						defnID, replicaID, "-", idxrNode.NodeUUID)
					continue
				}

				var shardIDs = make([]string, 0, 2)
				mainstoreShard.SetReplicaId(c.AlternateShard_ReplicaId(replicaID))
				shardIDs = append(shardIDs, mainstoreShard.String())
				if !indexUsage.IsPrimary {
					backstoreShard.SetReplicaId(c.AlternateShard_ReplicaId(replicaID))
					shardIDs = append(shardIDs, backstoreShard.String())
				}

				if len(indexUsage.AlternateShardIds) != 0 {
					if indexUsage.AlternateShardIds[0] == shardIDs[0] {
						continue
					}

					// Index Usage existing shard ID does not match with ShardDealer book
					// keeping slot. force update the same
					logging.Warnf("ShardDealer::GetShard: index {defnID: %v, replicaID: %v, partnID: %v, nodeUUID: %v} curr shards %v does not match shard dealer book keeping. Forcing new Alternate Shards",
						defnID, replicaID, indexUsage.PartnId, idxrNode.NodeUUID,
						indexUsage.AlternateShardIds)
				}

				indexUsage.AlternateShardIds = shardIDs
				logging.Infof("ShardDealer::GetShard: assiging AlternateShardIDs %v to index {defnID: %v, replicaID: %v, partnID: %v, nodeUUID: %v}",
					shardIDs, defnID, replicaID, indexUsage.PartnId, idxrNode.NodeUUID)
			}
		}
	}

	// updateShardDealerRecords is a util func which updates shard dealer book keeping. if there are
	// no updates then this func is a no-op
	var updateShardDealerRecords = func() []error {
		var errSlice []error = nil
		for replicaID, nodeMap := range replicaMap {
			for idxrNode, indexUsage := range nodeMap {
				var err = sd.RecordIndexUsage(indexUsage, idxrNode, false)
				if err != nil {
					logging.Warnf("ShardDealer::GetSlot failed to update book keeping with err %v for index {defnID: %v, replicaID: %v, partnID: %v, nodeUUID: %v}",
						err, defnID, replicaID, indexUsage.PartnId, idxrNode.NodeUUID)

					indexUsage.AlternateShardIds = nil
					// TODO: delete book keeping updates if any

					errSlice = append(errSlice, err)
				}
			}
		}
		return errSlice
	}

	// Check if defnID already has a slot assigned. If that is the case, use the same slot to
	// maintain consistency
	if alternateShard, exists := sd.indexSlots[defnID]; exists {
		mainstoreShard, backstoreShard = &c.AlternateShardId{}, &c.AlternateShardId{}
		mainstoreShard.SetSlotId(alternateShard)
		backstoreShard.SetSlotId(alternateShard)

		mainstoreShard.SetGroupId(0)
		backstoreShard.SetGroupId(1)

		setStoreOnAllUsages()

		return mainstoreShard.GetSlotId()
	}

	var nodesForShard = make(map[string]bool, 0)
	for _, nodeMap := range replicaMap {
		for idxrNode := range nodeMap {
			nodesForShard[idxrNode.NodeUUID] = true
		}
	}

	// Pass 0: are all the indexer nodes under minShardsPerNode?
	var nodesUnderMinShards = make([]string, 0, len(nodesForShard))
	for nodeUUID := range nodesForShard {
		if sd.nodeToSlotCountMap[nodeUUID] < sd.minShardsPerNode {
			nodesUnderMinShards = append(nodesUnderMinShards, nodeUUID)
		}
	}

	if len(nodesUnderMinShards) == len(nodesForShard) {
		// all nodes under min shard. create new alternate shards and return
		mainstoreShard, _ = c.NewAlternateId()
		backstoreShard = mainstoreShard.Clone()

		mainstoreShard.SetGroupId(0)
		backstoreShard.SetGroupId(1)

		// update index usages
		setStoreOnAllUsages()

		// update book keeping
		var errSlice = updateShardDealerRecords()
		if len(errSlice) != 0 {
			return 0
		}

		return mainstoreShard.GetSlotId()
	}

	return 0
}
