package planner

import (
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/stretchr/testify/assert"
)

const (
	minPartitionsPerShard uint64 = 16
	minShardsPerNode      uint64 = 6
	shardCapacity         uint64 = 1000
	maxDiskUsagePerShard  uint64 = 256 * 1024 * 1024
)

// func createDummyIndexUsage(id uint64, isPrimary, isVector, isBhive, isMoi bool) *IndexUsage {
func createDummyIndexUsage(params createIdxParam) *IndexUsage {
	var storageMode = c.PlasmaDB
	if params.isMoi {
		storageMode = c.MemDB
	}
	return &IndexUsage{
		Bucket:     "bucket",
		Scope:      "scope",
		Collection: "collection",

		DefnId:  c.IndexDefnId(params.defnid),
		InstId:  c.IndexInstId(params.defnid),
		PartnId: c.PartitionId(0),

		StorageMode: storageMode,

		IsPrimary: params.isPrimary,

		Name: fmt.Sprintf("index_%v", params.defnid),

		Instance: &c.IndexInst{
			InstId:    c.IndexInstId(params.defnid),
			State:     c.INDEX_STATE_ACTIVE,
			ReplicaId: 0,

			Defn: c.IndexDefn{
				IsVectorIndex: params.isVector || params.isBhive,
				VectorMeta: &c.VectorMetadata{
					IsBhive: params.isBhive,
				},
			},
		},
	}
}

func createDummyReplicaIndexUsages(params createIdxParam) []*IndexUsage {
	var indexes = make([]*IndexUsage, 0, params.numReplicas)
	for i := 1; i <= params.numReplicas; i++ {
		var index = createDummyIndexUsage(params)
		index.InstId = c.IndexInstId(time.Now().UnixNano())
		index.Instance.InstId = index.InstId
		index.Instance.ReplicaId = i
		index.Instance.Defn.NumReplica = uint32(params.numReplicas)
	}
	return indexes
}

func createDummyPartitionedIndexUsages(params createIdxParam) []*IndexUsage {
	var partitions = make([]*IndexUsage, 0, params.numPartns)
	for i := 1; i <= params.numPartns; i++ {
		var partn = createDummyIndexUsage(params)
		partn.PartnId = c.PartitionId(i)
		partn.Instance.Defn.NumPartitions = uint32(params.numPartns)
		partitions = append(partitions, partn)
	}
	return partitions
}

func createDummyReplicaPartitionedIndexUsage(params createIdxParam) [][]*IndexUsage {
	var indexes = make([][]*IndexUsage, 0, params.numReplicas)
	for i := 1; i <= params.numReplicas; i++ {
		var instID = c.IndexInstId(time.Now().UnixNano())
		var partitions = createDummyPartitionedIndexUsages(params)
		for _, partn := range partitions {
			partn.InstId = instID
			partn.Instance.InstId = instID
			partn.Instance.ReplicaId = i
			partn.Instance.Defn.NumReplica = uint32(params.numReplicas)
		}
	}
	return indexes
}

func createNewAlternateShardIDGenerator() func() (*c.AlternateShardId, error) {
	var counter uint64
	return func() (*c.AlternateShardId, error) {
		counter++
		return &c.AlternateShardId{
			SlotId: counter,
		}, nil
	}
}

func TestBasicSlotAssignment(t *testing.T) {
	t.Parallel()

	var dealer = NewShardDealer(
		minShardsPerNode,
		minPartitionsPerShard,
		maxDiskUsagePerShard,
		shardCapacity,
		createNewAlternateShardIDGenerator(), nil)

	var cips = []createIdxParam{
		{count: 1, isPrimary: true},
		{count: 1},
	}
	var indexerNode = createDummyIndexerNode(t.Name(), cips...)

	for _, inst := range indexerNode.Indexes {
		var replicaMap = make(map[int]map[*IndexerNode]*IndexUsage)
		replicaMap[inst.Instance.ReplicaId] = make(map[*IndexerNode]*IndexUsage)
		replicaMap[inst.Instance.ReplicaId][indexerNode] = inst

		slotID := dealer.GetSlot(inst.DefnId, inst.PartnId, replicaMap)

		if slotID == 0 {
			t.Fatalf("%v failed to get slot id for index %v in 0th pass",
				t.Name(), inst.String())
		}

		var expectedAlternateShardIDCount = 2
		if inst.IsPrimary {
			expectedAlternateShardIDCount = 1
		}

		if len(inst.AlternateShardIds) != expectedAlternateShardIDCount {
			t.Fatalf("%v expected index (IsPrimary %v?) to have %v AlternateShardIDs but it has %v",
				t.Name(), inst.IsPrimary, expectedAlternateShardIDCount,
				inst.AlternateShardIds)
		}
	}

	if len(dealer.partnSlots) != 2 {
		t.Fatalf(
			"%v shard dealer book keeping mismatch - expected it to contain 2 index defns but it has %v",
			t.Name(),
			dealer.partnSlots,
		)
	}

	if dealer.nodeToShardCountMap[indexerNode.NodeUUID] != 3 {
		t.Fatalf(
			"%v shard dealer book keeping mismatch - expected it to contain 3 slots but it has %v. (%v)",
			t.Name(),
			dealer.nodeToShardCountMap[indexerNode.NodeUUID],
			dealer.nodeToSlotMap[indexerNode.NodeUUID],
		)
	}

	if len(dealer.slotsPerCategory) > 1 {
		t.Fatalf(
			"%v shard dealer book keeping mismatch - expected it to contain 1 shard category but it has %v",
			t.Name(),
			dealer.slotsPerCategory,
		)
	}

	if len(dealer.slotsPerCategory[StandardShardCategory]) != 2 {
		t.Fatalf(
			"%v shard dealer book keeping mismatch - expected it to contain 2 shard category but it has %v",
			t.Name(),
			dealer.slotsPerCategory,
		)
	}

	err := validateShardDealerInternals(dealer, []*IndexerNode{indexerNode})
	if err != nil {
		t.Fatalf("%v internal shard dealer validation failed with err: \n\t\t%v",
			t.Name(), err)
	}
}

type createIdxParam struct {
	count                               uint64
	isPrimary, isVector, isBhive, isMoi bool
	numPartns, numReplicas              int
	defnid                              uint64
}

func createDummyIndexerNode(nodeID string, cips ...createIdxParam) *IndexerNode {
	var indexerNode = IndexerNode{
		NodeId:   "indexer-node-id-" + nodeID,
		NodeUUID: "indexer-node-uuid-" + nodeID,

		Indexes: make([]*IndexUsage, 0),
	}

	var id uint64
	for _, cip := range cips {
		for i := uint64(0); i < cip.count; i++ {
			cip.defnid = id
			if cip.numPartns > 0 {
				var newIndex = createDummyPartitionedIndexUsages(cip)

				for _, partn := range newIndex {
					partn.initialNode = &indexerNode
				}
				indexerNode.Indexes = append(indexerNode.Indexes, newIndex...)
			} else {
				var newIndex = createDummyIndexUsage(cip)
				newIndex.initialNode = &indexerNode
				indexerNode.Indexes = append(indexerNode.Indexes, newIndex)
			}
			id++
		}
	}

	return &indexerNode
}

func getReplicaMapsForIndexerNode(
	node *IndexerNode,
) map[c.IndexDefnId]map[c.PartitionId]map[int]map[*IndexerNode]*IndexUsage {
	var defnMap = make(map[c.IndexDefnId]map[c.PartitionId]map[int]map[*IndexerNode]*IndexUsage)

	for _, partn := range node.Indexes {
		if defnMap[partn.DefnId] == nil {
			defnMap[partn.DefnId] = make(map[c.PartitionId]map[int]map[*IndexerNode]*IndexUsage)
		}

		if defnMap[partn.DefnId][partn.PartnId] == nil {
			defnMap[partn.DefnId][partn.PartnId] = make(map[int]map[*IndexerNode]*IndexUsage)
		}

		if defnMap[partn.DefnId][partn.PartnId][partn.Instance.ReplicaId] == nil {
			defnMap[partn.DefnId][partn.PartnId][partn.Instance.ReplicaId] = make(
				map[*IndexerNode]*IndexUsage,
			)
		}

		defnMap[partn.DefnId][partn.PartnId][partn.Instance.ReplicaId][node] = partn
	}

	return defnMap
}

func TestSingleNodePass0(t *testing.T) {
	t.Parallel()

	t.Run("Basic-AllPrimary", func(t0 *testing.T) {
		t0.Parallel()

		var dealer = NewShardDealer(
			minShardsPerNode,
			minPartitionsPerShard,
			maxDiskUsagePerShard,
			shardCapacity,
			createNewAlternateShardIDGenerator(), nil)

		var cip = createIdxParam{count: minShardsPerNode, isPrimary: true}
		var indexerNode = createDummyIndexerNode(t0.Name(), cip)

		var slotIDs = make(map[c.AlternateShard_SlotId]bool)

		var replicaMaps = getReplicaMapsForIndexerNode(indexerNode)

		for defnID, repMaps := range replicaMaps {
			for partnID, repmap := range repMaps {
				slotID := dealer.GetSlot(defnID, partnID, repmap)

				if slotID == 0 {
					t0.Fatalf("%v failed to get slot id for replicaMap %v in 0th pass",
						t0.Name(), repmap)
				}

				slotIDs[slotID] = true
			}
		}

		if len(slotIDs) != int(minShardsPerNode) {
			t0.Fatalf("%v slots created are %v but should have been only %v slots",
				t0.Name(), slotIDs, minShardsPerNode)
		}

		if len(dealer.partnSlots) != int(minShardsPerNode) {
			t0.Fatalf(
				"%v shard dealer book keeping mismatch - index defns recorded are %v but should have been only %v defns",
				t0.Name(),
				dealer.partnSlots,
				minShardsPerNode,
			)
		}
		if len(dealer.slotsMap) != int(minShardsPerNode) {
			t0.Fatalf(
				"%v shard dealer book keeping mismatch - slots created are %v but should have been only %v slots",
				t0.Name(),
				dealer.slotsMap,
				minShardsPerNode,
			)
		}
		if dealer.nodeToShardCountMap[indexerNode.NodeUUID] != minShardsPerNode {
			t0.Fatalf(
				"%v shard dealer book keeping mismatch - node slot count is %v but should have been only %v",
				t0.Name(),
				dealer.nodeToShardCountMap[indexerNode.NodeUUID],
				minShardsPerNode,
			)
		}
		assert.NoError(
			t0,
			validateShardDealerInternals(dealer, []*IndexerNode{indexerNode}),
			"internal shard dealer validation failed for basic test",
		)

		cip = createIdxParam{defnid: minShardsPerNode + 1, isPrimary: true}
		var extraIndex = createDummyIndexUsage(cip)
		extraIndex.initialNode = indexerNode
		indexerNode.Indexes = append(indexerNode.Indexes, extraIndex)

		var replicaMap = make(map[int]map[*IndexerNode]*IndexUsage)
		replicaMap[extraIndex.Instance.ReplicaId] = make(map[*IndexerNode]*IndexUsage)
		replicaMap[extraIndex.Instance.ReplicaId][indexerNode] = extraIndex
		slotID := dealer.GetSlot(extraIndex.DefnId, extraIndex.PartnId, replicaMap)
		if _, exists := slotIDs[slotID]; slotID != 0 && !exists {
			t0.Fatalf(
				"%v extra slot created when it was not expected. New slot %v. All Slots %v",
				t0.Name(),
				slotID,
				slotIDs,
			)
		}

		assert.NoErrorf(t0, validateShardDealerInternals(dealer, []*IndexerNode{indexerNode}),
			"internal shard dealer validation failed for basic test after adding extra index")
	})

	// Consider a scenario where we have minShardsPerNode as 5. New we have already created
	// 4 shards and then we get a request to create a non-primary index. since node is under
	// limit, we can create new slot. so we create 2 shards taking total shard count to 6
	// and the inverse also that we create 2 2i indexes and then we create 2 prim indexes
	// this test verifies the same. - with pass 0
	t.Run("PrimaryAndSecondary", func(t0 *testing.T) {
		t0.Parallel()

		// PRIMARY: minShardsPerNode - 1
		// 2i: 1
		{
			var dealer = NewShardDealer(
				minShardsPerNode,
				minPartitionsPerShard,
				maxDiskUsagePerShard,
				shardCapacity,
				createNewAlternateShardIDGenerator(), nil)

			var cips = []createIdxParam{
				{count: minShardsPerNode - 1, isPrimary: true},
				{count: 1},
			}
			var indexerNode = createDummyIndexerNode(t0.Name(), cips...)

			var alternateShardIDs = make(map[string]bool)

			var replicaMaps = getReplicaMapsForIndexerNode(indexerNode)

			for defnID := 0; defnID < len(indexerNode.Indexes); defnID++ {
				for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID)] {
					slotID := dealer.GetSlot(c.IndexDefnId(defnID), partnID, repmap)

					if slotID == 0 {
						t0.Fatalf("%v failed to get slot id for replica map %v in 0th pass",
							t0.Name(), repmap)
					}

					for _, nodeMap := range repmap {
						for _, inst := range nodeMap {
							var expectedAlternateShardIDCount = 2
							if inst.IsPrimary {
								expectedAlternateShardIDCount = 1
							}

							if len(inst.AlternateShardIds) != expectedAlternateShardIDCount {
								t0.Fatalf(
									"%v expected index (IsPrimary %v?) to have %v AlternateShardIDs but it has %v",
									t0.Name(),
									inst.IsPrimary,
									expectedAlternateShardIDCount,
									inst.AlternateShardIds,
								)
							}

							for _, asi := range inst.AlternateShardIds {
								alternateShardIDs[asi] = true
							}
						}
					}
				}
			}

			assert.Equalf(t0, int(minShardsPerNode+1), len(alternateShardIDs),
				"%v expected %v alternate shard ids but the we have (%v) %v",
				t0.Name(), minShardsPerNode+1, len(alternateShardIDs), alternateShardIDs)

			err := validateShardDealerInternals(dealer, []*IndexerNode{indexerNode})
			assert.NoErrorf(t0, err,
				"%v internal shard dealer validation failed with err: \n\t\t%v",
				t0.Name(), err)
		}

		// INVERSE SCENARIO
		// 2i: 2
		// prim: 2
		{
			var dealer = NewShardDealer(
				minShardsPerNode,
				minPartitionsPerShard,
				maxDiskUsagePerShard,
				shardCapacity,
				createNewAlternateShardIDGenerator(), nil)

			var num2i = uint64(math.Ceil(float64(minShardsPerNode) / 4))
			var cips = []createIdxParam{
				{count: num2i},
				{count: num2i * 2, isPrimary: true},
			}
			var indexerNode = createDummyIndexerNode(t0.Name(), cips...)

			var alternateShardIDs = make(map[string]bool)

			var replicaMaps = getReplicaMapsForIndexerNode(indexerNode)

			for defnID := 0; defnID < len(indexerNode.Indexes); defnID++ {
				for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID)] {
					slotID := dealer.GetSlot(c.IndexDefnId(defnID), partnID, repmap)

					if slotID == 0 {
						t0.Fatalf("%v failed to get slot id for replica map %v in 0th pass",
							t0.Name(), repmap)
					}

					for _, nodeMap := range repmap {
						for _, inst := range nodeMap {
							var expectedAlternateShardIDCount = 2
							if inst.IsPrimary {
								expectedAlternateShardIDCount = 1
							}

							if len(inst.AlternateShardIds) != expectedAlternateShardIDCount {
								t0.Fatalf(
									"%v expected index (IsPrimary %v?) to have %v AlternateShardIDs but it has %v",
									t0.Name(),
									inst.IsPrimary,
									expectedAlternateShardIDCount,
									inst.AlternateShardIds,
								)
							}

							for _, asi := range inst.AlternateShardIds {
								alternateShardIDs[asi] = true
							}
						}
					}
				}
			}

			assert.Equalf(t0, int(minShardsPerNode), len(alternateShardIDs),
				"%v expected %v alternate shard ids but the we have (%v) %v",
				t0.Name(), minShardsPerNode, len(alternateShardIDs), alternateShardIDs)

			err := validateShardDealerInternals(dealer, []*IndexerNode{indexerNode})
			assert.NoErrorf(t0, err,
				"%v internal shard dealer validation failed with err: \n\t\t%v",
				t0.Name(), err)

		}

	})

	// TODO: add partitioned index tests
	// same as above test but we here use partitions instead of multiple indexes
	t.Run("PartitionedIndexes", func(t0 *testing.T) {
		t0.Parallel()

		// PRIMARY: numPartns = minShards - 1
		// 2i: 1
		{
			var dealer = NewShardDealer(
				minShardsPerNode,
				minPartitionsPerShard,
				maxDiskUsagePerShard,
				shardCapacity,
				createNewAlternateShardIDGenerator(), nil)

			var cips = []createIdxParam{
				{count: 1, isPrimary: true, numPartns: int(minShardsPerNode - 1)},
				{count: 1},
			}
			var indexerNode = createDummyIndexerNode(t0.Name(), cips...)

			var alternateShardIDs = make(map[string]bool)

			var replicaMaps = getReplicaMapsForIndexerNode(indexerNode)

			for defnID := 0; defnID < len(indexerNode.Indexes); defnID++ {
				for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID)] {
					slotID := dealer.GetSlot(c.IndexDefnId(defnID), partnID, repmap)

					if slotID == 0 {
						t0.Fatalf("%v failed to get slot id for replica map %v in 0th pass",
							t0.Name(), repmap)
					}

					for _, nodeMap := range repmap {
						for _, inst := range nodeMap {
							var expectedAlternateShardIDCount = 2
							if inst.IsPrimary {
								expectedAlternateShardIDCount = 1
							}

							if len(inst.AlternateShardIds) != expectedAlternateShardIDCount {
								t0.Fatalf(
									"%v expected index (IsPrimary %v?) to have %v AlternateShardIDs but it has %v",
									t0.Name(),
									inst.IsPrimary,
									expectedAlternateShardIDCount,
									inst.AlternateShardIds,
								)
							}

							for _, asi := range inst.AlternateShardIds {
								alternateShardIDs[asi] = true
							}
						}
					}
				}
			}

			assert.Equalf(t0, int(minShardsPerNode+1), len(alternateShardIDs),
				"%v expected %v alternate shard ids but the we have (%v) %v",
				t0.Name(), minShardsPerNode+1, len(alternateShardIDs), alternateShardIDs)

			var defnIDpartnCheck = make(map[c.IndexDefnId]bool)
			for _, partn := range indexerNode.Indexes {
				var defnID = partn.DefnId
				if !defnIDpartnCheck[defnID] && partn.PartnId != 0 {
					var numPartitions = partn.Instance.Defn.NumPartitions
					assert.Equalf(t0, int(numPartitions), len(dealer.partnSlots[defnID]),
						"%v expected defn %v to have %v partitions but it has only %v",
						t0.Name(), defnID, numPartitions, len(dealer.partnSlots[defnID]))
					defnIDpartnCheck[defnID] = true
				}
			}

			err := validateShardDealerInternals(dealer, []*IndexerNode{indexerNode})
			assert.NoErrorf(t0, err,
				"%v internal shard dealer validation failed with err: \n\t\t%v",
				t0.Name(), err)

		}

		// INVERSE SCENARIO
		// 2i: numPartns = 2
		// prim: numPartns = 2
		{
			var dealer = NewShardDealer(
				minShardsPerNode,
				minPartitionsPerShard,
				maxDiskUsagePerShard,
				shardCapacity,
				createNewAlternateShardIDGenerator(), nil)

			var num2iPartns = int(math.Ceil(float64(minShardsPerNode) / 4))
			var cips = []createIdxParam{
				{count: 1, numPartns: num2iPartns},
				{count: 1, isPrimary: true, numPartns: num2iPartns * 2},
			}
			var indexerNode = createDummyIndexerNode(t0.Name(), cips...)

			var alternateShardIDs = make(map[string]bool)

			var replicaMaps = getReplicaMapsForIndexerNode(indexerNode)

			for defnID := 0; defnID < len(indexerNode.Indexes); defnID++ {
				for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID)] {
					slotID := dealer.GetSlot(c.IndexDefnId(defnID), partnID, repmap)

					if slotID == 0 {
						t0.Fatalf("%v failed to get slot id for replica map %v in 0th pass",
							t0.Name(), repmap)
					}

					for _, nodeMap := range repmap {
						for _, inst := range nodeMap {
							var expectedAlternateShardIDCount = 2
							if inst.IsPrimary {
								expectedAlternateShardIDCount = 1
							}

							if len(inst.AlternateShardIds) != expectedAlternateShardIDCount {
								t0.Fatalf(
									"%v expected index (IsPrimary %v?) to have %v AlternateShardIDs but it has %v",
									t0.Name(),
									inst.IsPrimary,
									expectedAlternateShardIDCount,
									inst.AlternateShardIds,
								)
							}

							for _, asi := range inst.AlternateShardIds {
								alternateShardIDs[asi] = true
							}
						}
					}
				}
			}

			assert.Equalf(t0, int(minShardsPerNode), len(alternateShardIDs),
				"%v expected %v alternate shard ids but the we have (%v) %v",
				t0.Name(), minShardsPerNode, len(alternateShardIDs), alternateShardIDs)

			err := validateShardDealerInternals(dealer, []*IndexerNode{indexerNode})
			assert.NoErrorf(t0, err,
				"%v internal shard dealer validation failed with err: \n\t\t%v",
				t0.Name(), err)
		}
	})
}

func TestSingleNodePass1(t *testing.T) {
	t.Parallel()

	t.Run("Basic-AllPrimary", func(t0 *testing.T) {
		t0.Parallel()

		var dealer = NewShardDealer(
			minShardsPerNode,
			minPartitionsPerShard,
			maxDiskUsagePerShard,
			shardCapacity,
			createNewAlternateShardIDGenerator(), nil)

		var cip = createIdxParam{count: minPartitionsPerShard, isPrimary: true}

		var indexerNode = createDummyIndexerNode(t0.Name(), cip)

		var slotIDs = make(map[c.AlternateShard_SlotId]bool)

		var replicaMaps = getReplicaMapsForIndexerNode(indexerNode)

		for defnID, repMaps := range replicaMaps {
			for partnID, repmap := range repMaps {
				slotID := dealer.GetSlot(defnID, partnID, repmap)

				if slotID == 0 {
					t0.Fatalf("%v failed to get slot id for replicaMap %v in 0th pass",
						t0.Name(), repmap)
				}

				slotIDs[slotID] = true
			}
		}

		if len(slotIDs) != int(minShardsPerNode) {
			t0.Fatalf("%v slots created are %v but should have been only %v slots",
				t0.Name(), slotIDs, minShardsPerNode)
		}

		if len(dealer.partnSlots) != int(minPartitionsPerShard) {
			t0.Fatalf(
				"%v shard dealer book keeping mismatch - index defns recorded are %v but should have been only %v defns",
				t0.Name(),
				dealer.partnSlots,
				minPartitionsPerShard,
			)
		}
		if len(dealer.slotsMap) != int(minShardsPerNode) {
			t0.Fatalf(
				"%v shard dealer book keeping mismatch - slots created are %v but should have been only %v slots",
				t0.Name(),
				dealer.slotsMap,
				minShardsPerNode,
			)
		}
		if dealer.nodeToShardCountMap[indexerNode.NodeUUID] != minShardsPerNode {
			t0.Fatalf(
				"%v shard dealer book keeping mismatch - expected %v alternate shard ids but the we have %v",
				t0.Name(),
				minShardsPerNode,
				dealer.nodeToShardCountMap[indexerNode.NodeUUID],
			)
		}

		err := validateShardDealerInternals(dealer, []*IndexerNode{indexerNode})
		assert.NoErrorf(t0, err,
			"%v internal shard dealer validation failed with err: \n\t\t%v",
			t0.Name(), err)

	})

	// This is similar to Pass-0 except here we also test that post creating minShardsPerNode
	// shards, we reuse existing shards
	t.Run("PrimaryAndSecondary", func(t0 *testing.T) {
		t0.Parallel()

		var dealer = NewShardDealer(
			minShardsPerNode,
			minPartitionsPerShard,
			maxDiskUsagePerShard,
			shardCapacity,
			createNewAlternateShardIDGenerator(), nil)

		var cips = []createIdxParam{
			{count: minShardsPerNode * 2, isPrimary: true},
			{count: minShardsPerNode * 2},
		}

		var indexerNode = createDummyIndexerNode(t0.Name(), cips...)

		var replicaMaps = getReplicaMapsForIndexerNode(indexerNode)
		var alternateShardIDs = make(map[string]bool)

		for defnID := 0; defnID < len(indexerNode.Indexes); defnID++ {
			for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID)] {
				slotID := dealer.GetSlot(c.IndexDefnId(defnID), partnID, repmap)

				if slotID == 0 {
					t0.Fatalf("%v expected to get a valid slot for replica map %v but got 0",
						t0.Name(), repmap)
				}

				for _, nodeMap := range repmap {
					for _, inst := range nodeMap {
						var expectedAlternateShardIDCount = 2
						if inst.IsPrimary {
							expectedAlternateShardIDCount = 1
						}

						if len(inst.AlternateShardIds) != expectedAlternateShardIDCount {
							t0.Fatalf(
								"%v expected index (IsPrimary %v?) to have %v AlternateShardIDs but it has %v",
								t0.Name(),
								inst.IsPrimary,
								expectedAlternateShardIDCount,
								inst.AlternateShardIds,
							)
						}

						for _, asi := range inst.AlternateShardIds {
							alternateShardIDs[asi] = true
						}
					}
				}
			}
		}

		if len(alternateShardIDs) != int(minShardsPerNode)*2 {
			t0.Fatalf("%v expected %v alternate shard ids but the we have %v (%v)",
				t0.Name(), 2*minShardsPerNode, len(alternateShardIDs),
				alternateShardIDs)
		}

		// since all primary indexes are created before, we create all minShardsPerNode
		// now when the new 2i indexes are created, we re-use the slots but we still end
		// up creating new shards
		if dealer.nodeToShardCountMap[indexerNode.NodeUUID] != 2*minShardsPerNode {
			t0.Fatalf(
				"%v shard dealer book keeping mismatch - expected %v alternate shard ids but the we have %v",
				t0.Name(),
				2*minShardsPerNode,
				dealer.nodeToShardCountMap[indexerNode.NodeUUID],
			)
		}
		if len(dealer.partnSlots) != int(minShardsPerNode)*4 {
			t0.Fatalf(
				"%v shard dealer book keeping mismatch - expected %v index defns but it has %v",
				t0.Name(),
				minShardsPerNode*4,
				len(dealer.partnSlots),
			)
		}

		err := validateShardDealerInternals(dealer, []*IndexerNode{indexerNode})
		assert.NoErrorf(t0, err,
			"%v internal shard dealer validation failed with err: \n\t\t%v",
			t0.Name(), err)

	})

	// This is similar to Pass-0 except here we also test that post creating minShardsPerNode
	// shards, we test reuse of existing shards
	t.Run("PartitionedIndexes", func(t0 *testing.T) {
		t0.Parallel()

		// PRIMARY: numPartns = minPartitionsPerShard, count = minShardsPerNode - 1
		// 2i: count = 1, numPartns = minPartitionsPerShard
		{
			var dealer = NewShardDealer(
				minShardsPerNode,
				minPartitionsPerShard,
				maxDiskUsagePerShard,
				shardCapacity,
				createNewAlternateShardIDGenerator(), nil)

			var cips = []createIdxParam{
				{count: minShardsPerNode - 1, isPrimary: true, numPartns: int(minPartitionsPerShard)},
				{count: 1, numPartns: int(minPartitionsPerShard)},
			}
			var indexerNode = createDummyIndexerNode(t0.Name(), cips...)

			var alternateShardIDs = make(map[string]bool)

			var replicaMaps = getReplicaMapsForIndexerNode(indexerNode)

			for defnID := 0; defnID < len(indexerNode.Indexes); defnID++ {
				for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID)] {
					slotID := dealer.GetSlot(c.IndexDefnId(defnID), partnID, repmap)

					if slotID == 0 {
						t0.Fatalf("%v failed to get slot id for replica map %v in 0th pass",
							t0.Name(), repmap)
					}

					for _, nodeMap := range repmap {
						for _, inst := range nodeMap {
							var expectedAlternateShardIDCount = 2
							if inst.IsPrimary {
								expectedAlternateShardIDCount = 1
							}

							if len(inst.AlternateShardIds) != expectedAlternateShardIDCount {
								t0.Fatalf(
									"%v expected index (IsPrimary %v?) to have %v AlternateShardIDs but it has %v",
									t0.Name(),
									inst.IsPrimary,
									expectedAlternateShardIDCount,
									inst.AlternateShardIds,
								)
							}

							for _, asi := range inst.AlternateShardIds {
								alternateShardIDs[asi] = true
							}
						}
					}
				}
			}

			assert.Equalf(t0, int(minShardsPerNode*2), len(alternateShardIDs),
				"%v expected %v alternate shard ids but the we have (%v) %v",
				t0.Name(), minShardsPerNode*2, len(alternateShardIDs), alternateShardIDs)

			var defnIDpartnCheck = make(map[c.IndexDefnId]bool)
			for _, partn := range indexerNode.Indexes {
				var defnID = partn.DefnId
				if !defnIDpartnCheck[defnID] && partn.PartnId != 0 {
					var numPartitions = partn.Instance.Defn.NumPartitions
					assert.Equalf(t0, int(numPartitions), len(dealer.partnSlots[defnID]),
						"%v expected defn %v to have %v partitions but it has only %v",
						t0.Name(), defnID, numPartitions, len(dealer.partnSlots[defnID]))
					defnIDpartnCheck[defnID] = true
				}
			}

			err := validateShardDealerInternals(dealer, []*IndexerNode{indexerNode})
			assert.NoErrorf(t0, err,
				"%v internal shard dealer validation failed with err: \n\t\t%v",
				t0.Name(), err)
		}

		// INVERSE SCENARIO
		// 2i: count = 2, numPartns = minPartitionsPerShard
		// prim: count = 1 numPartns = minPartitionsPerShard
		{
			var dealer = NewShardDealer(
				minShardsPerNode,
				minPartitionsPerShard,
				maxDiskUsagePerShard,
				shardCapacity,
				createNewAlternateShardIDGenerator(), nil)

			var cips = []createIdxParam{
				{count: 2, numPartns: int(minPartitionsPerShard)},
				{count: 1, isPrimary: true, numPartns: int(minPartitionsPerShard)},
			}
			var indexerNode = createDummyIndexerNode(t0.Name(), cips...)

			var alternateShardIDs = make(map[string]bool)

			var replicaMaps = getReplicaMapsForIndexerNode(indexerNode)

			for defnID := 0; defnID < len(indexerNode.Indexes); defnID++ {
				for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID)] {
					slotID := dealer.GetSlot(c.IndexDefnId(defnID), partnID, repmap)

					if slotID == 0 {
						t0.Fatalf("%v failed to get slot id for replica map %v in 0th pass",
							t0.Name(), repmap)
					}

					for _, nodeMap := range repmap {
						for _, inst := range nodeMap {
							var expectedAlternateShardIDCount = 2
							if inst.IsPrimary {
								expectedAlternateShardIDCount = 1
							}

							if len(inst.AlternateShardIds) != expectedAlternateShardIDCount {
								t0.Fatalf(
									"%v expected index (IsPrimary %v?) to have %v AlternateShardIDs but it has %v",
									t0.Name(),
									inst.IsPrimary,
									expectedAlternateShardIDCount,
									inst.AlternateShardIds,
								)
							}

							for _, asi := range inst.AlternateShardIds {
								alternateShardIDs[asi] = true
							}
						}
					}
				}
			}

			assert.Equalf(t0, int(minShardsPerNode), len(alternateShardIDs),
				"%v expected %v alternate shard ids but the we have (%v) %v",
				t0.Name(), minShardsPerNode, len(alternateShardIDs), alternateShardIDs)

			err := validateShardDealerInternals(dealer, []*IndexerNode{indexerNode})
			assert.NoErrorf(t0, err,
				"%v internal shard dealer validation failed with err: \n\t\t%v",
				t0.Name(), err)
		}
	})

	t.Run("MixCategoryIndexes", func(t0 *testing.T) {
		t0.Parallel()

		var dealer = NewShardDealer(
			minShardsPerNode,
			minPartitionsPerShard,
			maxDiskUsagePerShard,
			shardCapacity,
			createNewAlternateShardIDGenerator(), nil)

		var cips = []createIdxParam{
			{count: 1, isPrimary: true},
			{count: 1},
			{count: 1, isVector: true},
			{count: 1, isBhive: true},
		}
		var indexerNode = createDummyIndexerNode(t0.Name(), cips...)

		var replicaMaps = getReplicaMapsForIndexerNode(indexerNode)
		var alternateShardIDs = make(map[string]bool)

		for defnID := 0; defnID < len(indexerNode.Indexes); defnID++ {
			for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID)] {
				slotID := dealer.GetSlot(c.IndexDefnId(defnID), partnID, repmap)

				if slotID == 0 {
					t0.Fatalf("%v failed to get slot for replica map %v",
						t0.Name(), repmap)
				}

				for _, nodeMap := range repmap {
					for _, inst := range nodeMap {
						var expectedAlternateShardIDCount = 2
						if inst.IsPrimary {
							expectedAlternateShardIDCount = 1
						}

						if len(inst.AlternateShardIds) != expectedAlternateShardIDCount {
							t0.Fatalf(
								"%v expected index (IsPrimary %v?) to have %v AlternateShardIDs but it has %v",
								t0.Name(),
								inst.IsPrimary,
								expectedAlternateShardIDCount,
								inst.AlternateShardIds,
							)
						}

						for _, asi := range inst.AlternateShardIds {
							alternateShardIDs[asi] = true
						}
					}
				}
			}
		}

		if len(alternateShardIDs) != 7 {
			t0.Fatalf("%v expected %v alternate shards but we have only %v (%v)",
				t0.Name(), 7, len(alternateShardIDs), alternateShardIDs)
		}

		if len(dealer.slotsPerCategory) != 3 {
			t0.Fatalf(
				"%v shard dealer book keeping mismatch - expected %v categories but we have %v",
				t0.Name(),
				3,
				dealer.slotsPerCategory,
			)
		}

		if len(dealer.slotsPerCategory[StandardShardCategory]) != 2 {
			t0.Fatalf(
				"%v shard dealer book keeping mismatch - slots for %v are %v but expected only %v",
				t0.Name(),
				StandardShardCategory,
				dealer.slotsPerCategory[StandardShardCategory],
				minShardsPerNode*2,
			)
		}

		if len(dealer.slotsPerCategory[VectorShardCategory]) != 1 {
			t0.Fatalf(
				"%v shard dealer book keeping mismatch - slots for %v are %v but expected only %v",
				t0.Name(),
				VectorShardCategory,
				dealer.slotsPerCategory[VectorShardCategory],
				1,
			)
		}
		if len(dealer.slotsPerCategory[BhiveShardCategory]) != 1 {
			t0.Fatalf(
				"%v shard dealer book keeping mismatch - slots for %v are %v but expected only %v",
				t0.Name(),
				BhiveShardCategory,
				dealer.slotsPerCategory[BhiveShardCategory],
				1,
			)
		}

		if int(dealer.nodeToShardCountMap[indexerNode.NodeUUID]) != len(alternateShardIDs) {
			t0.Fatalf(
				"%v shard dealer book keeping mismatch - node has more shards %v than expected %v",
				t0.Name(),
				dealer.nodeToShardCountMap[indexerNode.NodeUUID],
				len(alternateShardIDs),
			)
		}

		err := validateShardDealerInternals(dealer, []*IndexerNode{indexerNode})
		if err != nil {
			t0.Fatalf("%v internal shard dealer validation failed with err: \n\t\t%v",
				t0.Name(), err)
		}

	})
}

func TestSingleNodePass2(t *testing.T) {
	t.Parallel()

	t.Run("Basic-AllPrimary", func(t0 *testing.T) {
		t0.Parallel()

		var dealer = NewShardDealer(
			minShardsPerNode,
			1,
			maxDiskUsagePerShard,
			shardCapacity,
			createNewAlternateShardIDGenerator(), nil)

		var cip = createIdxParam{count: shardCapacity / 10, isPrimary: true}
		var indexerNode = createDummyIndexerNode(t0.Name(), cip)

		var replicaMaps = getReplicaMapsForIndexerNode(indexerNode)

		var slotIDs = make(map[c.AlternateShard_SlotId]bool)

		for defnID := 0; defnID < len(indexerNode.Indexes); defnID++ {
			for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID)] {
				slotID := dealer.GetSlot(c.IndexDefnId(defnID), partnID, repmap)

				if slotID == 0 {
					t0.Fatalf("%v failed to get slot id for replicaMap %v in 0th pass",
						t0.Name(), repmap)
				}

				slotIDs[slotID] = true
			}
		}

		if len(slotIDs) != int(shardCapacity/10) {
			t0.Fatalf("%v slots created are (%v) %v but should have been only %v slots",
				t0.Name(), len(slotIDs), slotIDs, shardCapacity/10)
		}

		if len(dealer.partnSlots) != len(slotIDs) {
			t0.Fatalf(
				"%v shard dealer book keeping mismatch - index defns recorded are %v but should have been only %v defns",
				t0.Name(),
				dealer.partnSlots,
				minShardsPerNode,
			)
		}
		if len(dealer.slotsMap) != len(slotIDs) {
			t0.Fatalf(
				"%v shard dealer book keeping mismatch - slots created are %v but should have been only %v slots",
				t0.Name(),
				dealer.slotsMap,
				len(slotIDs),
			)
		}
		if int(dealer.nodeToShardCountMap[indexerNode.NodeUUID]) != len(slotIDs) {
			t0.Fatalf(
				"%v shard dealer book keeping mismatch - node slot count is %v but should have been only %v",
				t0.Name(),
				dealer.nodeToShardCountMap[indexerNode.NodeUUID],
				len(slotIDs),
			)
		}

		err := validateShardDealerInternals(dealer, []*IndexerNode{indexerNode})
		if err != nil {
			t0.Fatalf("%v internal shard dealer validation failed with err: \n\t\t%v",
				t0.Name(), err)
		}
	})

	t.Run("MaxShards", func(t0 *testing.T) {
		t0.Parallel()

		var testShardCapacity uint64 = 10
		var dealer = NewShardDealer(minShardsPerNode,
			1,
			maxDiskUsagePerShard,
			testShardCapacity,
			createNewAlternateShardIDGenerator(), nil)

		var cip = createIdxParam{count: testShardCapacity}
		var indexerNode = createDummyIndexerNode(t0.Name(), cip)

		var replicaMaps = getReplicaMapsForIndexerNode(indexerNode)
		var slotIDs = make(map[c.AlternateShard_SlotId]bool)
		var alternateShardIDs = make(map[string]bool)

		for defnID := 0; defnID < len(indexerNode.Indexes); defnID++ {
			for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID)] {
				var slotID = dealer.GetSlot(c.IndexDefnId(defnID), partnID, repmap)

				if slotID == 0 {
					t0.Fatalf("%v failed to get slot for replica map %v",
						t0.Name(), repmap)
				}
				slotIDs[slotID] = true
				for _, nodeMap := range repmap {
					for _, inst := range nodeMap {
						var expectedAlternateShardIDCount = 2
						if inst.IsPrimary {
							expectedAlternateShardIDCount = 1
						}

						if len(inst.AlternateShardIds) != expectedAlternateShardIDCount {
							t0.Fatalf(
								"%v expected index (IsPrimary %v?) to have %v AlternateShardIDs but it has %v",
								t0.Name(),
								inst.IsPrimary,
								expectedAlternateShardIDCount,
								inst.AlternateShardIds,
							)
						}

						for _, asi := range inst.AlternateShardIds {
							alternateShardIDs[asi] = true
						}
					}
				}
			}
		}

		if len(slotIDs) != int(testShardCapacity)/2 {
			t0.Fatalf("%v expected to create %v slots we have %v (%v)",
				t0.Name(), testShardCapacity, len(slotIDs), slotIDs)
		}
		if len(alternateShardIDs) != int(testShardCapacity) {
			t0.Fatalf("%v expected to create %v shards we have %v (%v)",
				t0.Name(), testShardCapacity, len(alternateShardIDs), alternateShardIDs)
		}
		if len(dealer.partnSlots) != int(testShardCapacity) {
			t0.Fatalf(
				"%v shard dealer book keeping mismatch - index defns recorded are %v but should have been only %v defns",
				t0.Name(),
				dealer.partnSlots,
				testShardCapacity,
			)
		}
		if len(dealer.slotsMap) != len(slotIDs) {
			t0.Fatalf(
				"%v shard dealer book keeping mismatch - slots created are %v but should have been only %v slots",
				t0.Name(),
				dealer.slotsMap,
				len(slotIDs),
			)
		}
		if int(dealer.nodeToShardCountMap[indexerNode.NodeUUID]) != len(alternateShardIDs) {
			t0.Fatalf(
				"%v shard dealer book keeping mismatch - node slot count is %v but should have been only %v",
				t0.Name(),
				dealer.nodeToShardCountMap[indexerNode.NodeUUID],
				len(slotIDs),
			)
		}

		cip = createIdxParam{defnid: shardCapacity + 1}
		// create extra index. it should not create a new shard
		var extraIndex = createDummyIndexUsage(cip)
		extraIndex.initialNode = indexerNode
		indexerNode.Indexes = append(indexerNode.Indexes, extraIndex)

		var replicaMap = make(map[int]map[*IndexerNode]*IndexUsage)
		replicaMap[extraIndex.Instance.ReplicaId] = make(map[*IndexerNode]*IndexUsage)
		replicaMap[extraIndex.Instance.ReplicaId][indexerNode] = extraIndex
		slotID := dealer.GetSlot(extraIndex.DefnId, extraIndex.PartnId, replicaMap)
		if _, exists := slotIDs[slotID]; slotID != 0 && !exists {
			t0.Fatalf(
				"%v extra slot created when it was not expected. New slot %v. All Slots %v",
				t0.Name(),
				slotID,
				slotIDs,
			)
		}

		err := validateShardDealerInternals(dealer, []*IndexerNode{indexerNode})
		assert.NoErrorf(t0, err, "%v internal shard dealer validation failed with err: \n\t\t%v", t0.Name(), err)
	})

	t.Run("MixCategoryIndexes", func(t0 *testing.T) {
		t0.Parallel()

		var dealer = NewShardDealer(
			minShardsPerNode,
			minPartitionsPerShard,
			maxDiskUsagePerShard,
			shardCapacity,
			createNewAlternateShardIDGenerator(), nil)

		var cips = []createIdxParam{
			{count: minShardsPerNode, isPrimary: true},
			{count: minShardsPerNode},
			{count: 1, isVector: true},
			{count: 1, isBhive: true},
		}
		var indexerNode = createDummyIndexerNode(t0.Name(), cips...)

		var replicaMaps = getReplicaMapsForIndexerNode(indexerNode)
		var alternateShardIDs = make(map[string]bool)

		for defnID := 0; defnID < len(indexerNode.Indexes); defnID++ {
			for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID)] {
				slotID := dealer.GetSlot(c.IndexDefnId(defnID), partnID, repmap)

				if slotID == 0 {
					t0.Fatalf("%v failed to get slot for replica map %v",
						t0.Name(), repmap)
				}

				for _, nodeMap := range repmap {
					for _, inst := range nodeMap {
						var expectedAlternateShardIDCount = 2
						if inst.IsPrimary {
							expectedAlternateShardIDCount = 1
						}

						if len(inst.AlternateShardIds) != expectedAlternateShardIDCount {
							t0.Fatalf(
								"%v expected index (IsPrimary %v?) to have %v AlternateShardIDs but it has %v",
								t0.Name(),
								inst.IsPrimary,
								expectedAlternateShardIDCount,
								inst.AlternateShardIds,
							)
						}

						for _, asi := range inst.AlternateShardIds {
							alternateShardIDs[asi] = true
						}
					}
				}
			}
		}

		// 1 shard-pair for vec-index, 1 for bhive, 2 shard pairs for std indexes, 2 shards for
		// primary indexes
		if len(alternateShardIDs) != int(minShardsPerNode)*2+4 {
			t0.Fatalf("%v expected %v alternate shards but we have only %v (%v)",
				t0.Name(), minShardsPerNode*2, len(alternateShardIDs), alternateShardIDs)
		}

		if len(dealer.slotsPerCategory) != 3 {
			t0.Fatalf(
				"%v shard dealer book keeping mismatch - expected %v categories but we have %v",
				t0.Name(),
				3,
				dealer.slotsPerCategory,
			)
		}

		if len(dealer.slotsPerCategory[StandardShardCategory]) != int(minShardsPerNode) {
			t0.Fatalf(
				"%v shard dealer book keeping mismatch - slots for %v are %v but expected only %v",
				t0.Name(),
				StandardShardCategory,
				dealer.slotsPerCategory[StandardShardCategory],
				minShardsPerNode,
			)
		}
		if len(dealer.slotsPerCategory[VectorShardCategory]) != 1 {
			t0.Fatalf(
				"%v shard dealer book keeping mismatch - slots for %v are %v but expected only %v",
				t0.Name(),
				VectorShardCategory,
				dealer.slotsPerCategory[VectorShardCategory],
				1,
			)
		}
		if len(dealer.slotsPerCategory[BhiveShardCategory]) != 1 {
			t0.Fatalf(
				"%v shard dealer book keeping mismatch - slots for %v are %v but expected only %v",
				t0.Name(),
				BhiveShardCategory,
				dealer.slotsPerCategory[BhiveShardCategory],
				1,
			)
		}

		if int(dealer.nodeToShardCountMap[indexerNode.NodeUUID]) != len(alternateShardIDs) {
			t0.Fatalf(
				"%v shard dealer book keeping mismatch - node has more shards %v than expected %v",
				t0.Name(),
				dealer.nodeToShardCountMap[indexerNode.NodeUUID],
				len(alternateShardIDs),
			)
		}

		err := validateShardDealerInternals(dealer, []*IndexerNode{indexerNode})
		if err != nil {
			t0.Fatalf("%v internal shard dealer validation failed with err: \n\t\t%v",
				t0.Name(), err)
		}

	})

	// We create load of upto pass-1 and then add another index to test if new slot is created
	t.Run("PartitionedIndexes", func(t0 *testing.T) {
		t0.Parallel()

		var dealer = NewShardDealer(
			minShardsPerNode,
			minPartitionsPerShard,
			maxDiskUsagePerShard,
			shardCapacity,
			createNewAlternateShardIDGenerator(), nil)

		var cips = []createIdxParam{
			{count: 2, numPartns: int(minPartitionsPerShard)},
			{count: 1, isPrimary: true, numPartns: int(minPartitionsPerShard)},
			{count: 1, numPartns: 5},
		}
		var indexerNode = createDummyIndexerNode(t0.Name(), cips...)

		var replicaMaps = getReplicaMapsForIndexerNode(indexerNode)
		var alternateShardIDs = make(map[string]bool)

		for defnID := 0; defnID < len(indexerNode.Indexes); defnID++ {
			for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID)] {
				slotID := dealer.GetSlot(c.IndexDefnId(defnID), partnID, repmap)

				if slotID == 0 {
					t0.Fatalf("%v failed to get slot for replica map %v",
						t0.Name(), repmap)
				}

				for _, nodeMap := range repmap {
					for _, inst := range nodeMap {
						var expectedAlternateShardIDCount = 2
						if inst.IsPrimary {
							expectedAlternateShardIDCount = 1
						}

						if len(inst.AlternateShardIds) != expectedAlternateShardIDCount {
							t0.Fatalf(
								"%v expected index (IsPrimary %v?) to have %v AlternateShardIDs but it has %v",
								t0.Name(),
								inst.IsPrimary,
								expectedAlternateShardIDCount,
								inst.AlternateShardIds,
							)
						}

						for _, asi := range inst.AlternateShardIds {
							alternateShardIDs[asi] = true
						}
					}
				}
			}
		}

		assert.Equalf(t0, 4, len(dealer.slotsMap),
			"%v expected cluster to have %v slots but it has %v - %v",
			t0.Name(), 4, len(dealer.slotsMap), dealer.slotsMap,
		)

		err := validateShardDealerInternals(dealer, []*IndexerNode{indexerNode})
		assert.NoError(t0, err, t0.Name(), "expected to have all shard dealer internals validated")

	})

}

func TestSingleNodePass3(t *testing.T) {
	t.Parallel()

	var testShardCapacity uint64 = 10

	var cips = []createIdxParam{
		{count: testShardCapacity * 2},
		{count: testShardCapacity / 4, isVector: true},
		{count: testShardCapacity / 4, isBhive: true},
	}

	// maximise re-use case
	{
		var dealer = NewShardDealer(
			minShardsPerNode,
			1,
			maxDiskUsagePerShard,
			testShardCapacity,
			createNewAlternateShardIDGenerator(), nil)
		var indexerNode = createDummyIndexerNode(t.Name(), cips...)
		var replicaMaps = getReplicaMapsForIndexerNode(indexerNode)
		var slotIDs = make(map[c.AlternateShard_SlotId]bool)
		var alternateShardIDs = make(map[string]bool)

		for defnID := len(indexerNode.Indexes); defnID >= 0; defnID-- {
			for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID)] {
				var slotID = dealer.GetSlot(c.IndexDefnId(defnID), partnID, repmap)

				if slotID == 0 {
					t.Fatalf("%v failed to get slot for replica map %v",
						t.Name(), repmap)
				}
				slotIDs[slotID] = true
				for _, nodeMap := range repmap {
					for _, inst := range nodeMap {
						var expectedAlternateShardIDCount = 2
						if inst.IsPrimary {
							expectedAlternateShardIDCount = 1
						}

						if len(inst.AlternateShardIds) != expectedAlternateShardIDCount {
							t.Fatalf(
								"%v expected index (IsPrimary %v?) to have %v AlternateShardIDs but it has %v",
								t.Name(),
								inst.IsPrimary,
								expectedAlternateShardIDCount,
								inst.AlternateShardIds,
							)
						}

						for _, asi := range inst.AlternateShardIds {
							alternateShardIDs[asi] = true
						}
					}
				}
			}
		}

		if len(slotIDs) != int(testShardCapacity)/2 {
			t.Fatalf("%v expected to create %v slots we have %v (%v)",
				t.Name(), testShardCapacity, len(slotIDs), slotIDs)
		}
		if len(alternateShardIDs) != int(testShardCapacity) {
			t.Fatalf("%v expected to create %v shards we have %v (%v)",
				t.Name(), testShardCapacity, len(alternateShardIDs), alternateShardIDs)
		}

		if len(dealer.partnSlots) != len(indexerNode.Indexes) {
			t.Fatalf(
				"%v shard dealer book keeping mismatch - index defns recorded are %v but should have been only %v defns",
				t.Name(),
				dealer.partnSlots,
				len(indexerNode.Indexes),
			)
		}
		if len(dealer.slotsMap) != len(slotIDs) {
			t.Fatalf(
				"%v shard dealer book keeping mismatch - slots created are %v but should have been only %v slots",
				t.Name(),
				dealer.slotsMap,
				len(slotIDs),
			)
		}
		if int(dealer.nodeToShardCountMap[indexerNode.NodeUUID]) != len(alternateShardIDs) {
			t.Fatalf(
				"%v shard dealer book keeping mismatch - node slot count is %v but should have been only %v",
				t.Name(),
				dealer.nodeToShardCountMap[indexerNode.NodeUUID],
				len(slotIDs),
			)
		}

		var cip = createIdxParam{defnid: uint64(len(indexerNode.Indexes))}
		// create extra index. it should not create a new shard
		var extraIndex = createDummyIndexUsage(cip)
		extraIndex.initialNode = indexerNode
		indexerNode.Indexes = append(indexerNode.Indexes, extraIndex)

		var replicaMap = make(map[int]map[*IndexerNode]*IndexUsage)
		replicaMap[extraIndex.Instance.ReplicaId] = make(map[*IndexerNode]*IndexUsage)
		replicaMap[extraIndex.Instance.ReplicaId][indexerNode] = extraIndex
		slotID := dealer.GetSlot(extraIndex.DefnId, extraIndex.PartnId, replicaMap)
		if slotID == 0 {
			t.Fatalf("%v failed to get slot id for replicaMap %v",
				t.Name(), replicaMap)
		}
		if _, exists := slotIDs[slotID]; !exists {
			t.Fatalf("%v extra slot created when it was not expected. New slot %v. All Slots %v",
				t.Name(), slotID, slotIDs)
		}

		err := validateShardDealerInternals(dealer, []*IndexerNode{indexerNode})
		if err != nil {
			t.Fatalf("%v internal shard dealer validation failed with err: \n\t\t%v",
				t.Name(), err)
		}
		logging.Infof("******* max re-use ensured *******")
	}

	// test upgrade aka overflow case
	{
		var dealer = NewShardDealer(
			minShardsPerNode,
			1,
			maxDiskUsagePerShard,
			testShardCapacity,
			createNewAlternateShardIDGenerator(), nil)

		var indexerNode = createDummyIndexerNode(t.Name(), cips...)
		var replicaMaps = getReplicaMapsForIndexerNode(indexerNode)
		var slotIDs = make(map[c.AlternateShard_SlotId]bool)
		var alternateShardIDs = make(map[string]bool)

		for defnID := 0; defnID < len(indexerNode.Indexes); defnID++ {
			for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID)] {
				var slotID = dealer.GetSlot(c.IndexDefnId(defnID), partnID, repmap)

				if slotID == 0 {
					t.Fatalf("%v failed to get slot for replica map %v",
						t.Name(), repmap)
				}
				slotIDs[slotID] = true
				for _, nodeMap := range repmap {
					for _, inst := range nodeMap {
						var expectedAlternateShardIDCount = 2
						if inst.IsPrimary {
							expectedAlternateShardIDCount = 1
						}

						if len(inst.AlternateShardIds) != expectedAlternateShardIDCount {
							t.Fatalf(
								"%v expected index (IsPrimary %v?) to have %v AlternateShardIDs but it has %v",
								t.Name(),
								inst.IsPrimary,
								expectedAlternateShardIDCount,
								inst.AlternateShardIds,
							)
						}

						for _, asi := range inst.AlternateShardIds {
							alternateShardIDs[asi] = true
						}
					}
				}
			}
		}

		if len(slotIDs) != int(testShardCapacity)/2+2 {
			t.Fatalf("%v expected to create %v slots we have %v (%v)",
				t.Name(), testShardCapacity/2+2, len(slotIDs), slotIDs)
		}

		if len(alternateShardIDs) != int(testShardCapacity)+4 {
			t.Fatalf("%v expected to create %v shards we have %v (%v)",
				t.Name(), testShardCapacity+4, len(alternateShardIDs), alternateShardIDs)
		}

		if len(dealer.partnSlots) != len(indexerNode.Indexes) {
			t.Fatalf(
				"%v shard dealer book keeping mismatch - index defns recorded are %v but should have been only %v defns",
				t.Name(),
				dealer.partnSlots,
				len(indexerNode.Indexes),
			)
		}
		if len(dealer.slotsMap) != len(slotIDs) {
			t.Fatalf(
				"%v shard dealer book keeping mismatch - slots created are %v but should have been only %v slots",
				t.Name(),
				dealer.slotsMap,
				len(slotIDs),
			)
		}

		if int(dealer.nodeToShardCountMap[indexerNode.NodeUUID]) != len(alternateShardIDs) {
			t.Fatalf(
				"%v shard dealer book keeping mismatch - node slot count is %v but should have been only %v",
				t.Name(),
				dealer.nodeToShardCountMap[indexerNode.NodeUUID],
				len(slotIDs),
			)
		}

		var cip = createIdxParam{defnid: uint64(len(indexerNode.Indexes))}
		// create extra index. it should not create a new shard
		var extraIndex = createDummyIndexUsage(cip)
		extraIndex.initialNode = indexerNode
		indexerNode.Indexes = append(indexerNode.Indexes, extraIndex)

		var replicaMap = make(map[int]map[*IndexerNode]*IndexUsage)
		replicaMap[extraIndex.Instance.ReplicaId] = make(map[*IndexerNode]*IndexUsage)
		replicaMap[extraIndex.Instance.ReplicaId][indexerNode] = extraIndex
		slotID := dealer.GetSlot(extraIndex.DefnId, extraIndex.PartnId, replicaMap)
		if slotID == 0 {
			t.Fatalf("%v failed to get slot id for replicaMap %v",
				t.Name(), replicaMap)
		}
		if _, exists := slotIDs[slotID]; !exists {
			t.Fatalf("%v extra slot created when it was not expected. New slot %v. All Slots %v",
				t.Name(), slotID, slotIDs)
		}

		err := validateShardDealerInternals(dealer, []*IndexerNode{indexerNode})
		if err != nil {
			t.Fatalf("%v internal shard dealer validation failed with err: \n\t\t%v",
				t.Name(), err)
		}
	}
}

func TestGetShardCategory(t *testing.T) {
	t.Parallel()

	var moiCip = createIdxParam{defnid: 1, isMoi: true}
	// MOI index
	var moiIndex = createDummyIndexUsage(moiCip)
	var shardCategory = getIndexCategory(moiIndex)
	if shardCategory != InvalidShardCategory {
		t.Fatalf(
			"%v MoI index should have had invalid shard category but is %v",
			t.Name(),
			shardCategory,
		)
	}

	var plasmaCip = createIdxParam{defnid: 0, isPrimary: true}
	var plasmaIndex = createDummyIndexUsage(plasmaCip)
	shardCategory = getIndexCategory(plasmaIndex)
	if shardCategory != StandardShardCategory {
		t.Fatalf(
			"%v primary index should have had std shard category but is %v",
			t.Name(),
			shardCategory,
		)
	}

	plasmaCip = createIdxParam{defnid: 0}
	plasmaIndex = createDummyIndexUsage(plasmaCip)
	shardCategory = getIndexCategory(plasmaIndex)
	if shardCategory != StandardShardCategory {
		t.Fatalf(
			"%v plasma index should have had std shard category but is %v",
			t.Name(),
			shardCategory,
		)
	}

	var vecCip = createIdxParam{defnid: 0, isVector: true}
	var compositeVectorIndex = createDummyIndexUsage(vecCip)
	shardCategory = getIndexCategory(compositeVectorIndex)
	if shardCategory != VectorShardCategory {
		t.Fatalf(
			"%v vector index should have had vector shard category but is %v",
			t.Name(),
			shardCategory,
		)
	}

	var bhiveCip = createIdxParam{defnid: 0, isBhive: true}
	var bhiveIndex = createDummyIndexUsage(bhiveCip)
	shardCategory = getIndexCategory(bhiveIndex)
	if shardCategory != BhiveShardCategory {
		t.Fatalf(
			"%v bhive index should have had bhive shard category but is %v",
			t.Name(),
			shardCategory,
		)
	}
}

func TestRecordIndexUsage(t *testing.T) {
	t.Parallel()

	var dealer = NewShardDealer(
		minPartitionsPerShard, minPartitionsPerShard, maxDiskUsagePerShard,
		shardCapacity, createNewAlternateShardIDGenerator(),
		nil)

	var err = dealer.RecordIndexUsage(nil, nil, true)
	if err != nil {
		t.Fatalf("expected nil error on nil index but got %v", err)
	}

	var moiCip = createIdxParam{defnid: 0, isMoi: true}
	var moiIndex = createDummyIndexUsage(moiCip)
	err = dealer.RecordIndexUsage(moiIndex, nil, true)
	if err != nil {
		t.Fatalf("expected nil error on MoI index but got %v", err)
	}
	if len(moiIndex.AlternateShardIds) != 0 {
		t.Fatalf(
			"expected alternate shard IDs to be nil for MoI index but they are %v",
			moiIndex.AlternateShardIds,
		)
	}

	// this fails on invalid shard category
	moiIndex.InitialAlternateShardIds = []string{""}
	err = dealer.RecordIndexUsage(moiIndex, nil, true)
	if err == nil {
		t.Fatalf(
			"expected to fail in recording MoI index due to invalid category but it got recorded",
		)
	}

	// this fails on invalid shard category
	moiIndex.AlternateShardIds = []string{""}
	err = dealer.RecordIndexUsage(moiIndex, nil, false)
	if err == nil {
		t.Fatalf(
			"expected to fail in recording MoI index due to invalid category but it got recorded",
		)
	}

	var plasmaCip = createIdxParam{}
	var plasmaIndex = createDummyIndexUsage(plasmaCip)
	err = dealer.RecordIndexUsage(plasmaIndex, nil, false)
	if err != nil {
		t.Fatalf(
			"expected to not fail record index usage if index has nil shards but it failed with %v",
			err,
		)
	}

	// this fails on invalid alternate shard IDs
	plasmaIndex.InitialAlternateShardIds = []string{""}
	err = dealer.RecordIndexUsage(plasmaIndex, nil, true)
	if err == nil {
		t.Fatalf(
			"expected to fail record index usage as index has invalid shards but it did not fail",
		)
	}

	// this fails on invalid alternate shard IDs
	plasmaIndex.AlternateShardIds = []string{""}
	err = dealer.RecordIndexUsage(plasmaIndex, nil, false)
	if err == nil {
		t.Fatalf(
			"expected to fail record index usage as index has invalid shards but it did not fail",
		)
	}

	plasmaCip = createIdxParam{defnid: 1}
	plasmaIndex = createDummyIndexUsage(plasmaCip)
	var mainAlternateID, _ = c.NewAlternateId()
	mainAlternateID.SetSlotId(1)    // dont use `0` here
	mainAlternateID.SetReplicaId(1) // dont use `0` here
	mainAlternateID.SetGroupId(0)
	var backAlternateID = mainAlternateID.Clone()
	backAlternateID.SetGroupId(1)

	var alternateShardIDs = []string{mainAlternateID.String(), backAlternateID.String()}
	plasmaIndex.InitialAlternateShardIds = alternateShardIDs
	err = dealer.RecordIndexUsage(plasmaIndex, nil, false)
	if err != nil {
		t.Fatalf("expected no-op record usage to succeed but it failed with err %v",
			err)
	}
	if len(dealer.slotsMap[mainAlternateID.GetSlotId()]) != 0 {
		t.Fatalf("slot 0 should have been empty but it contains %v",
			dealer.slotsMap[mainAlternateID.GetSlotId()])
	}

	var indexer = createDummyIndexerNode("0")
	indexer.Indexes = append(indexer.Indexes, plasmaIndex)

	err = dealer.RecordIndexUsage(plasmaIndex, indexer, true)
	if err != nil {
		t.Fatalf("expected index to be recorded but it failed with err %v", err)
	}

	if len(dealer.slotsMap[mainAlternateID.GetSlotId()]) == 0 ||
		len(dealer.slotsMap[mainAlternateID.GetSlotId()][mainAlternateID.GetReplicaId()]) == 0 ||
		dealer.slotsMap[mainAlternateID.GetSlotId()][mainAlternateID.GetReplicaId()][mainAlternateID.GetGroupId()] == nil ||
		dealer.slotsMap[mainAlternateID.GetSlotId()][mainAlternateID.GetReplicaId()][backAlternateID.GetGroupId()] == nil {
		t.Fatalf(
			"book keeping mismatch in shard dealer. slots map is not correct - %v",
			dealer.slotsMap[mainAlternateID.GetSlotId()],
		)
	}
	var shardContainer = dealer.slotsMap[mainAlternateID.GetSlotId()][mainAlternateID.GetReplicaId()][mainAlternateID.GetGroupId()]
	if len(shardContainer.insts) != 1 {
		t.Fatalf(
			"book keeping mismatch in shard container. expected only 1 inst but there are - %v",
			shardContainer.insts,
		)
	}
	if len(shardContainer.insts[plasmaIndex.InstId]) > 1 {
		t.Fatalf(
			"book keeping mismatch in shard container. expected only 1 index usage but there are - %v",
			shardContainer.insts[plasmaIndex.InstId],
		)
	}
	if shardContainer.insts[plasmaIndex.InstId][0] != plasmaIndex {
		t.Fatalf("book keeping mismatch in shard container. expected index to be %p but it is %p",
			shardContainer.insts[plasmaIndex.InstId][0], plasmaIndex)
	}
	if shardContainer.totalPartitions != uint64(plasmaIndex.Instance.Defn.NumPartitions)+1 {
		t.Fatalf(
			"book keeping mismatch is shard container. expected %v partitions but there are %v",
			plasmaIndex.Instance.Defn.NumPartitions,
			shardContainer.totalPartitions,
		)
	}
	if len(dealer.partnSlots[plasmaIndex.DefnId]) == 0 ||
		dealer.partnSlots[plasmaIndex.DefnId][plasmaIndex.PartnId] != mainAlternateID.GetSlotId() {
		t.Fatalf(
			"book keeping mismatch. expected slotID to be %v but it is (partnSlot map for defn) %v",
			mainAlternateID.GetSlotId(),
			dealer.partnSlots[plasmaIndex.DefnId],
		)
	}

	var shardCategory = getIndexCategory(plasmaIndex)
	if len(dealer.slotsPerCategory[shardCategory]) == 0 ||
		!dealer.slotsPerCategory[shardCategory][mainAlternateID.GetSlotId()] {
		t.Fatalf(
			"book keeping mismatch. expected dealer to have slot in category %v but it has (slotsMap) %v",
			shardCategory,
			dealer.slotsPerCategory,
		)
	}

	if dealer.nodeToShardCountMap[indexer.NodeUUID] != uint64(
		len(plasmaIndex.InitialAlternateShardIds),
	) {
		t.Fatalf("book keeping mismatch. expected dealer to have %v shards but has %v",
			len(plasmaIndex.AlternateShardIds), dealer.nodeToShardCountMap[indexer.NodeUUID])
	}

	if len(dealer.nodeToSlotMap[indexer.NodeUUID]) == 0 ||
		dealer.nodeToSlotMap[indexer.NodeUUID][mainAlternateID.GetSlotId()] != mainAlternateID.GetReplicaId() {
		t.Fatalf(
			"book keeping mismatch. expected nodeToSlotMap to have %v replicaID but it has (slotMap for node) %v",
			mainAlternateID.GetReplicaId(),
			dealer.nodeToSlotMap[indexer.NodeUUID],
		)
	}

	// duplicate insert of same index. should not lead to any changes in dealer internal records
	err = dealer.RecordIndexUsage(plasmaIndex, indexer, true)
	if err != nil {
		t.Fatalf("expected index to be recorded but it failed with err %v", err)
	}

	if len(dealer.slotsMap[mainAlternateID.GetSlotId()]) == 0 ||
		len(dealer.slotsMap[mainAlternateID.GetSlotId()][mainAlternateID.GetReplicaId()]) == 0 ||
		dealer.slotsMap[mainAlternateID.GetSlotId()][mainAlternateID.GetReplicaId()][mainAlternateID.GetGroupId()] == nil ||
		dealer.slotsMap[mainAlternateID.GetSlotId()][mainAlternateID.GetReplicaId()][backAlternateID.GetGroupId()] == nil {
		t.Fatalf(
			"book keeping mismatch in shard dealer. slots map is not correct - %v",
			dealer.slotsMap[mainAlternateID.GetSlotId()],
		)
	}
	shardContainer = dealer.slotsMap[mainAlternateID.GetSlotId()][mainAlternateID.GetReplicaId()][mainAlternateID.GetGroupId()]
	if len(shardContainer.insts) != 1 {
		t.Fatalf(
			"book keeping mismatch in shard container. expected only 1 inst but there are - %v",
			shardContainer.insts,
		)
	}
	if len(shardContainer.insts[plasmaIndex.InstId]) > 1 {
		t.Fatalf(
			"book keeping mismatch in shard container. expected only 1 index usage but there are - %v",
			shardContainer.insts[plasmaIndex.InstId],
		)
	}
	if shardContainer.insts[plasmaIndex.InstId][0] != plasmaIndex {
		t.Fatalf("book keeping mismatch in shard container. expected index to be %p but it is %p",
			shardContainer.insts[plasmaIndex.InstId][0], plasmaIndex)
	}
	if shardContainer.totalPartitions != uint64(plasmaIndex.Instance.Defn.NumPartitions)+1 {
		t.Fatalf(
			"book keeping mismatch is shard container. expected %v partitions but there are %v",
			plasmaIndex.Instance.Defn.NumPartitions,
			shardContainer.totalPartitions,
		)
	}
	if len(dealer.partnSlots[plasmaIndex.DefnId]) == 0 ||
		dealer.partnSlots[plasmaIndex.DefnId][plasmaIndex.PartnId] != mainAlternateID.GetSlotId() {
		t.Fatalf(
			"book keeping mismatch. expected slotID to be %v but it is (partnSlot map for defn) %v",
			mainAlternateID.GetSlotId(),
			dealer.partnSlots[plasmaIndex.DefnId],
		)
	}

	shardCategory = getIndexCategory(plasmaIndex)
	if len(dealer.slotsPerCategory[shardCategory]) == 0 ||
		!dealer.slotsPerCategory[shardCategory][mainAlternateID.GetSlotId()] {
		t.Fatalf(
			"book keeping mismatch. expected dealer to have slot in category %v but it has (slotsMap) %v",
			shardCategory,
			dealer.slotsPerCategory,
		)
	}

	if dealer.nodeToShardCountMap[indexer.NodeUUID] != uint64(
		len(plasmaIndex.InitialAlternateShardIds),
	) {
		t.Fatalf("book keeping mismatch. expected dealer to have %v shards but has %v",
			len(plasmaIndex.AlternateShardIds), dealer.nodeToShardCountMap[indexer.NodeUUID])
	}

	if len(dealer.nodeToSlotMap[indexer.NodeUUID]) == 0 ||
		dealer.nodeToSlotMap[indexer.NodeUUID][mainAlternateID.GetSlotId()] != mainAlternateID.GetReplicaId() {
		t.Fatalf(
			"book keeping mismatch. expected nodeToSlotMap to have %v replicaID but it has (slotMap for node) %v",
			mainAlternateID.GetReplicaId(),
			dealer.nodeToSlotMap[indexer.NodeUUID],
		)
	}

	var oldPlasmaIndex = plasmaIndex
	plasmaCip = createIdxParam{defnid: 2}
	plasmaIndex = createDummyIndexUsage(plasmaCip)
	mainAlternateID.SetSlotId(2)
	backAlternateID.SetSlotId(2)

	alternateShardIDs = []string{mainAlternateID.String(), backAlternateID.String()}
	plasmaIndex.AlternateShardIds = alternateShardIDs

	indexer.Indexes = append(indexer.Indexes, plasmaIndex)

	err = dealer.RecordIndexUsage(plasmaIndex, indexer, false)
	if err != nil {
		t.Fatalf("expected index to be recorded but it failed with err %v", err)
	}

	if len(dealer.slotsMap[mainAlternateID.GetSlotId()]) == 0 ||
		len(dealer.slotsMap[mainAlternateID.GetSlotId()][mainAlternateID.GetReplicaId()]) == 0 ||
		dealer.slotsMap[mainAlternateID.GetSlotId()][mainAlternateID.GetReplicaId()][mainAlternateID.GetGroupId()] == nil ||
		dealer.slotsMap[mainAlternateID.GetSlotId()][mainAlternateID.GetReplicaId()][backAlternateID.GetGroupId()] == nil {
		t.Fatalf(
			"book keeping mismatch in shard dealer. slots map is not correct - %v",
			dealer.slotsMap[mainAlternateID.GetSlotId()],
		)
	}
	shardContainer = dealer.slotsMap[mainAlternateID.GetSlotId()][mainAlternateID.GetReplicaId()][mainAlternateID.GetGroupId()]
	if len(shardContainer.insts) != 1 {
		t.Fatalf("book keeping mismatch in shard container. expected 1 inst but there are - %v",
			shardContainer.insts)
	}
	if len(shardContainer.insts[plasmaIndex.InstId]) > 1 {
		t.Fatalf(
			"book keeping mismatch in shard container. expected only 1 index usage but there are - %v",
			shardContainer.insts[plasmaIndex.InstId],
		)
	}
	if shardContainer.insts[plasmaIndex.InstId][0] != plasmaIndex {
		t.Fatalf("book keeping mismatch in shard container. expected index to be %p but it is %p",
			shardContainer.insts[plasmaIndex.InstId][0], plasmaIndex)
	}
	if shardContainer.totalPartitions != uint64(plasmaIndex.Instance.Defn.NumPartitions)+1 {
		t.Fatalf(
			"book keeping mismatch is shard container. expected %v partitions but there are %v",
			plasmaIndex.Instance.Defn.NumPartitions,
			shardContainer.totalPartitions,
		)
	}
	if len(dealer.partnSlots[plasmaIndex.DefnId]) == 0 ||
		dealer.partnSlots[plasmaIndex.DefnId][plasmaIndex.PartnId] != mainAlternateID.GetSlotId() {
		t.Fatalf(
			"book keeping mismatch. expected slotID to be %v but it is (partnSlot map for defn) %v",
			mainAlternateID.GetSlotId(),
			dealer.partnSlots[plasmaIndex.DefnId],
		)
	}

	shardCategory = getIndexCategory(plasmaIndex)
	if len(dealer.slotsPerCategory[shardCategory]) == 0 ||
		!dealer.slotsPerCategory[shardCategory][mainAlternateID.GetSlotId()] {
		t.Fatalf(
			"book keeping mismatch. expected dealer to have slot in category %v but it has (slotsMap) %v",
			shardCategory,
			dealer.slotsPerCategory,
		)
	}

	if int(
		dealer.nodeToShardCountMap[indexer.NodeUUID],
	) != len(
		plasmaIndex.AlternateShardIds,
	)+len(
		oldPlasmaIndex.InitialAlternateShardIds,
	) {
		t.Fatalf(
			"book keeping mismatch. expected dealer to have %v shards but has %v",
			len(
				plasmaIndex.AlternateShardIds,
			)+len(
				oldPlasmaIndex.InitialAlternateShardIds,
			),
			dealer.nodeToShardCountMap[indexer.NodeUUID],
		)
	}

	if len(dealer.nodeToSlotMap[indexer.NodeUUID]) == 0 ||
		dealer.nodeToSlotMap[indexer.NodeUUID][mainAlternateID.GetSlotId()] != mainAlternateID.GetReplicaId() {
		t.Fatalf(
			"book keeping mismatch. expected nodeToSlotMap to have %v replicaID but it has (slotMap for node) %v",
			mainAlternateID.GetReplicaId(),
			dealer.nodeToSlotMap[indexer.NodeUUID],
		)
	}

	// duplicate insert of same index. should not lead to any changes in dealer internal records
	err = dealer.RecordIndexUsage(plasmaIndex, indexer, false)
	if err != nil {
		t.Fatalf("expected index to be recorded but it failed with err %v", err)
	}

	if len(dealer.slotsMap[mainAlternateID.GetSlotId()]) == 0 ||
		len(dealer.slotsMap[mainAlternateID.GetSlotId()][mainAlternateID.GetReplicaId()]) == 0 ||
		dealer.slotsMap[mainAlternateID.GetSlotId()][mainAlternateID.GetReplicaId()][mainAlternateID.GetGroupId()] == nil ||
		dealer.slotsMap[mainAlternateID.GetSlotId()][mainAlternateID.GetReplicaId()][backAlternateID.GetGroupId()] == nil {
		t.Fatalf(
			"book keeping mismatch in shard dealer. slots map is not correct - %v",
			dealer.slotsMap[mainAlternateID.GetSlotId()],
		)
	}
	shardContainer = dealer.slotsMap[mainAlternateID.GetSlotId()][mainAlternateID.GetReplicaId()][mainAlternateID.GetGroupId()]
	if len(shardContainer.insts) != 1 {
		t.Fatalf(
			"book keeping mismatch in shard container. expected only 1 inst but there are - %v",
			shardContainer.insts,
		)
	}
	if len(shardContainer.insts[plasmaIndex.InstId]) > 1 {
		t.Fatalf(
			"book keeping mismatch in shard container. expected only 1 index usage but there are - %v",
			shardContainer.insts[plasmaIndex.InstId],
		)
	}
	if shardContainer.insts[plasmaIndex.InstId][0] != plasmaIndex {
		t.Fatalf("book keeping mismatch in shard container. expected index to be %p but it is %p",
			shardContainer.insts[plasmaIndex.InstId][0], plasmaIndex)
	}
	if shardContainer.totalPartitions != uint64(plasmaIndex.Instance.Defn.NumPartitions)+1 {
		t.Fatalf(
			"book keeping mismatch is shard container. expected %v partitions but there are %v",
			plasmaIndex.Instance.Defn.NumPartitions,
			shardContainer.totalPartitions,
		)
	}
	if len(dealer.partnSlots[plasmaIndex.DefnId]) == 0 ||
		dealer.partnSlots[plasmaIndex.DefnId][plasmaIndex.PartnId] != mainAlternateID.GetSlotId() {
		t.Fatalf(
			"book keeping mismatch. expected slotID to be %v but it is (partnSlot map for defn) %v",
			mainAlternateID.GetSlotId(),
			dealer.partnSlots[plasmaIndex.DefnId],
		)
	}

	shardCategory = getIndexCategory(plasmaIndex)
	if len(dealer.slotsPerCategory[shardCategory]) == 0 ||
		!dealer.slotsPerCategory[shardCategory][mainAlternateID.GetSlotId()] {
		t.Fatalf(
			"book keeping mismatch. expected dealer to have slot in category %v but it has (slotsMap) %v",
			shardCategory,
			dealer.slotsPerCategory,
		)
	}

	if int(
		dealer.nodeToShardCountMap[indexer.NodeUUID],
	) != len(
		plasmaIndex.AlternateShardIds,
	)+len(
		oldPlasmaIndex.InitialAlternateShardIds,
	) {
		t.Fatalf(
			"book keeping mismatch. expected dealer to have %v shards but has %v",
			len(
				plasmaIndex.AlternateShardIds,
			)+len(
				oldPlasmaIndex.InitialAlternateShardIds,
			),
			dealer.nodeToShardCountMap[indexer.NodeUUID],
		)
	}

	if len(dealer.nodeToSlotMap[indexer.NodeUUID]) == 0 ||
		dealer.nodeToSlotMap[indexer.NodeUUID][mainAlternateID.GetSlotId()] != mainAlternateID.GetReplicaId() {
		t.Fatalf(
			"book keeping mismatch. expected nodeToSlotMap to have %v replicaID but it has (slotMap for node) %v",
			mainAlternateID.GetReplicaId(),
			dealer.nodeToSlotMap[indexer.NodeUUID],
		)
	}

}

func validateShardDealerInternals(sd *ShardDealer, cluster []*IndexerNode) error {
	if sd == nil || len(cluster) == 0 {
		return nil
	}

	// generate internal structures according to layout

	var slotsPerCategory = make(map[ShardCategory]map[asSlotID]bool)
	// cluster level picture
	var slotsMap = make(map[asSlotID]map[asReplicaID]map[asGroupID]*pseudoShardContainer)
	// <defnId, partnId> to slotID
	var partnSlots = make(map[c.IndexDefnId]map[c.PartitionId]asSlotID)

	// per node pic of which shard pair belongs to which node
	var nodeToSlotMap = make(map[string]map[asSlotID]asReplicaID)
	var nodeToShardCountMap = make(map[string]uint64)

	for _, indexer := range cluster {
		for _, partn := range indexer.Indexes {
			if partn == nil || (len(partn.InitialAlternateShardIds) == 0 &&
				len(partn.AlternateShardIds) == 0) {
				continue
			}

			var alternateShardIDs = partn.InitialAlternateShardIds
			if len(partn.AlternateShardIds) != 0 && len(alternateShardIDs) == 0 {
				alternateShardIDs = partn.AlternateShardIds
			} else if len(partn.AlternateShardIds) != 0 &&
				alternateShardIDs[0] != partn.AlternateShardIds[0] {
				return fmt.Errorf("InitialAlternateShardIDs != AlternateShardIDs (%v != %v) for index %v. dealer validation not deterministic",
					alternateShardIDs, partn.AlternateShardIds, partn)
			}

			var alternateID, err = c.ParseAlternateId(alternateShardIDs[0])
			if err != nil {
				return fmt.Errorf("failed to parse alternateShardID for index %v with err (%v)",
					partn, err)
			}
			var shardCategory = getIndexCategory(partn)
			if shardCategory == InvalidShardCategory {
				continue
			}
			if slotsPerCategory[shardCategory] == nil {
				slotsPerCategory[shardCategory] = make(map[asSlotID]bool)
			}
			slotsPerCategory[shardCategory][alternateID.GetSlotId()] = true

			if partnSlots[partn.DefnId] == nil {
				partnSlots[partn.DefnId] = make(map[c.PartitionId]asSlotID)
			}
			partnSlots[partn.DefnId][partn.PartnId] = alternateID.GetSlotId()

			if nodeToSlotMap[indexer.NodeUUID] == nil {
				nodeToSlotMap[indexer.NodeUUID] = make(map[asSlotID]asReplicaID)
			}
			nodeToSlotMap[indexer.NodeUUID][alternateID.GetSlotId()] = alternateID.GetReplicaId()

			var slotID = alternateID.GetSlotId()
			var replicaID = alternateID.GetReplicaId()
			var groupID = alternateID.GetGroupId()

			if slotsMap[slotID] == nil {
				slotsMap[slotID] = make(map[asReplicaID]map[asGroupID]*pseudoShardContainer)
			}
			if slotsMap[slotID][replicaID] == nil {
				slotsMap[slotID][replicaID] = make(map[asGroupID]*pseudoShardContainer)
			}
			var newShardCount = 0
			if slotsMap[slotID][replicaID][groupID] == nil {
				slotsMap[slotID][replicaID][groupID] = newPseudoShardContainer()
				newShardCount++
			}
			if len(alternateShardIDs) > 1 {
				var backstoreAlternateID, err = c.ParseAlternateId(alternateShardIDs[1])
				if err != nil {
					return fmt.Errorf("failed to parse alternateShardID for index %v with err (%v)",
						partn, err)
				}
				var bGroupID = backstoreAlternateID.GetGroupId()
				if slotsMap[slotID][replicaID][bGroupID] == nil {
					slotsMap[slotID][replicaID][bGroupID] = newPseudoShardContainer()
					newShardCount++
				}
				slotsMap[slotID][replicaID][bGroupID].addInstToShardContainer(partn)
			}
			var isNew = slotsMap[slotID][replicaID][groupID].addInstToShardContainer(partn)
			if isNew {
				if _, exists := nodeToShardCountMap[indexer.NodeUUID]; !exists {
					nodeToShardCountMap[indexer.NodeUUID] = 0
				}
				nodeToShardCountMap[indexer.NodeUUID] += uint64(newShardCount)
			}
		}

	}

	// validate internals
	if !reflect.DeepEqual(slotsPerCategory, sd.slotsPerCategory) {
		return fmt.Errorf(
			"cluster slotsPerCategory does not match shard dealer - ([l: %v] %v != [l:%v] %v)",
			len(slotsPerCategory),
			slotsPerCategory,
			len(sd.slotsPerCategory),
			sd.slotsPerCategory,
		)
	}

	if !reflect.DeepEqual(partnSlots, sd.partnSlots) {
		return fmt.Errorf(
			"cluster partnSlots does not match shard dealer - ([l: %v] %v != [l:%v] %v)",
			len(partnSlots),
			partnSlots,
			len(sd.partnSlots),
			sd.partnSlots,
		)
	}

	if !reflect.DeepEqual(nodeToShardCountMap, sd.nodeToShardCountMap) {
		return fmt.Errorf(
			"cluster nodeToShardCountMap does not match shard dealer - ([l: %v] %v != [l:%v] %v)",
			len(
				nodeToShardCountMap,
			),
			nodeToShardCountMap,
			len(sd.nodeToShardCountMap),
			sd.nodeToShardCountMap,
		)
	}

	if !reflect.DeepEqual(nodeToSlotMap, sd.nodeToSlotMap) {
		return fmt.Errorf(
			"cluster slotsPerCategory does not match shard dealer - ([l: %v] %v != [l:%v] %v)",
			len(nodeToSlotMap),
			nodeToSlotMap,
			len(sd.nodeToSlotMap),
			sd.nodeToSlotMap,
		)
	}

	for slotID, clusterSlotMap := range slotsMap {
		dealerSlotMap, exists := sd.slotsMap[slotID]
		if !exists {
			return fmt.Errorf("cluster slotsMap does not match shard dealer - (%v != %v)",
				clusterSlotMap, dealerSlotMap)
		}

		for clusterSlotReplicaID, clusterSlotReplicaMap := range clusterSlotMap {
			dealerSlotReplicaMap, exists := dealerSlotMap[clusterSlotReplicaID]
			if !exists {
				return fmt.Errorf(
					"cluster replicaMap for slot %v does not match shard dealer - (%v != %v)",
					clusterSlotReplicaID,
					clusterSlotReplicaMap,
					dealerSlotReplicaMap,
				)
			}

			for clusterStoreID, clusterStoreShard := range clusterSlotReplicaMap {
				dealerStoreShard, exists := dealerSlotReplicaMap[clusterStoreID]
				if !exists || !clusterStoreShard.Equal(dealerStoreShard) {
					return fmt.Errorf(
						"cluster shardContainer does not match for slot-replica %v-%v-%v with shard dealer - (%v != %v)",
						slotID,
						clusterSlotReplicaID,
						clusterStoreID,
						clusterStoreShard,
						dealerStoreShard,
					)
				}
			}
		}

	}

	return nil
}

func (psc1 *pseudoShardContainer) Equal(psc2 *pseudoShardContainer) bool {
	if psc1 == nil && psc2 == nil {
		return true
	} else if (psc1 == nil && psc2 != nil) ||
		(psc1 != nil && psc2 == nil) {
		return false
	}

	if len(psc1.insts) != len(psc2.insts) ||
		psc1.totalPartitions != psc2.totalPartitions ||
		psc1.memUsage != psc2.memUsage ||
		psc1.dataSize != psc2.dataSize ||
		psc1.diskUsage != psc2.diskUsage {
		return false
	}

	for instID, indexes1 := range psc1.insts {
		indexes2, exists := psc2.insts[instID]
		if !exists || len(indexes1) != len(indexes2) {
			return false
		}

		for _, index1 := range indexes1 {
			var found = false
			for _, index2 := range indexes2 {
				if index1.InstId == index2.InstId &&
					index1.PartnId == index2.PartnId &&
					index1.Instance.ReplicaId == index2.Instance.ReplicaId {
					found = true
				}
			}
			if !found {
				return false
			}
		}
	}

	return true
}

func (psc1 *pseudoShardContainer) String() string {
	return fmt.Sprintf("ShardContainer: {totalPartitions: %v, memUsage: %v,"+
		"dataSize: %v, diskUsage: %v, insts: (%v)}", psc1.totalPartitions,
		psc1.memUsage, psc1.dataSize, psc1.diskUsage, psc1.insts)
}

// TODO: add multinode replica index based tests
// TODO: add multinode partitioned index tests
// TODO: add multinode replicated partitioned index tests
// TODO: add tests for disk usage check in recordIndex
