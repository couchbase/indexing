package planner

import (
	"fmt"
	"testing"

	c "github.com/couchbase/indexing/secondary/common"
)

const (
	minPartitionsPerShard uint64 = 16
	minShardsPerNode      uint64 = 6
	shardCapacity         uint64 = 1000
	maxDiskUsagePerShard  uint64 = 250 * 1024 * 1024 * 1024
)

func createDummyIndexUsage(id uint64, isPrimary, isVector, isBhive, isMoi bool) *IndexUsage {
	var storageMode = c.PlasmaDB
	if isMoi {
		storageMode = c.MemDB
	}
	return &IndexUsage{
		Bucket:     "bucket",
		Scope:      "scope",
		Collection: "collection",

		DefnId:  c.IndexDefnId(id),
		InstId:  c.IndexInstId(id),
		PartnId: c.PartitionId(0),

		StorageMode: storageMode,

		IsPrimary: isPrimary,

		Name: fmt.Sprintf("index_%v", id),

		Instance: &c.IndexInst{
			InstId:    c.IndexInstId(id),
			State:     c.INDEX_STATE_ACTIVE,
			ReplicaId: 0,

			Defn: c.IndexDefn{
				IsVectorIndex: isVector || isBhive,
				VectorMeta: &c.VectorMetadata{
					IsBhive: isBhive,
				},
			},
		},
	}
}

func TestBasicSlotAssignment(t *testing.T) {
	var dealer = NewShardDealer(
		minShardsPerNode,
		minPartitionsPerShard,
		maxDiskUsagePerShard,
		shardCapacity,
	)

	var indexerNode = createDummyIndexerNode(t.Name(), 1, 1, 0, 0, 0)

	for _, inst := range indexerNode.Indexes {
		var replicaMap = make(map[int]map[*IndexerNode]*IndexUsage)
		replicaMap[inst.Instance.ReplicaId] = make(map[*IndexerNode]*IndexUsage)
		replicaMap[inst.Instance.ReplicaId][indexerNode] = inst

		slotID := dealer.GetSlot(inst.DefnId, replicaMap)

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

	if len(dealer.indexSlots) != 2 {
		t.Fatalf("%v shard dealer book keeping mismatch - expected it to contain 2 index defns but it has %v",
			t.Name(), dealer.indexSlots)
	}

	if dealer.nodeToShardCountMap[indexerNode.NodeUUID] != 3 {
		t.Fatalf("%v shard dealer book keeping mismatch - expected it to contain 3 slots but it has %v. (%v)",
			t.Name(), dealer.nodeToShardCountMap[indexerNode.NodeUUID],
			dealer.nodeToSlotMap[indexerNode.NodeUUID])
	}

	if len(dealer.slotsPerCategory) > 1 {
		t.Fatalf("%v shard dealer book keeping mismatch - expected it to contain 1 shard category but it has %v",
			t.Name(), dealer.slotsPerCategory)
	}

	if len(dealer.slotsPerCategory[STANDARD_SHARD_CATEGORY]) != 2 {
		t.Fatalf("%v shard dealer book keeping mismatch - expected it to contain 2 shard category but it has %v",
			t.Name(), dealer.slotsPerCategory)
	}
}

func createDummyIndexerNode(nodeID string, num2iIndexes, numPrimaryIndexes, numVectorIndexes,
	numBhiveIndexes, numMoiIndexes uint64) *IndexerNode {
	var indexerNode = IndexerNode{
		NodeId:   "indexer-node-id-" + nodeID,
		NodeUUID: "indexer-node-uuid-" + nodeID,

		Indexes: make([]*IndexUsage, 0, num2iIndexes),
	}

	var id uint64

	for i := uint64(0); i < numPrimaryIndexes; i++ {
		var newIndex = createDummyIndexUsage(id, true, false, false, false)
		newIndex.initialNode = &indexerNode

		indexerNode.Indexes = append(indexerNode.Indexes, newIndex)
		id++
	}

	for i := uint64(0); i < num2iIndexes; i++ {
		var newIndex = createDummyIndexUsage(id, false, false, false, false)
		newIndex.initialNode = &indexerNode

		indexerNode.Indexes = append(indexerNode.Indexes, newIndex)
		id++
	}

	for i := uint64(0); i < numVectorIndexes; i++ {
		var newIndex = createDummyIndexUsage(id, false, true, false, false)
		newIndex.initialNode = &indexerNode

		indexerNode.Indexes = append(indexerNode.Indexes, newIndex)
		id++
	}

	for i := uint64(0); i < numBhiveIndexes; i++ {
		var newIndex = createDummyIndexUsage(id, false, false, true, false)
		newIndex.initialNode = &indexerNode

		indexerNode.Indexes = append(indexerNode.Indexes, newIndex)
		id++
	}

	for i := uint64(0); i < numMoiIndexes; i++ {
		var newIndex = createDummyIndexUsage(id, false, false, false, true)
		newIndex.initialNode = &indexerNode

		indexerNode.Indexes = append(indexerNode.Indexes, newIndex)
		id++
	}

	return &indexerNode
}

func getReplicaMapsForIndexerNode(
	node *IndexerNode,
) map[c.IndexDefnId][]map[int]map[*IndexerNode]*IndexUsage {
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

	var res = make(map[c.IndexDefnId][]map[int]map[*IndexerNode]*IndexUsage)
	for defnID, partnReplicaMap := range defnMap {
		res[defnID] = make([]map[int]map[*IndexerNode]*IndexUsage, 0, len(partnReplicaMap))
		for _, replicaMap := range partnReplicaMap {
			res[defnID] = append(res[defnID], replicaMap)
		}
	}

	return res
}

func TestSingleNodeAssignment(t *testing.T) {
	t.Run("Pass-0", func(subt *testing.T) {
		subt.Run("Basic-AllPrimary", func(t0 *testing.T) {
			t0.Parallel()

			var dealer = NewShardDealer(
				minShardsPerNode,
				minPartitionsPerShard,
				maxDiskUsagePerShard,
				shardCapacity,
			)

			var indexerNode = createDummyIndexerNode(t0.Name(), 0, minShardsPerNode, 0, 0, 0)

			var slotIDs = make(map[c.AlternateShard_SlotId]bool)

			var replicaMaps = getReplicaMapsForIndexerNode(indexerNode)

			for defnID, repMaps := range replicaMaps {
				for _, repmap := range repMaps {
					slotID := dealer.GetSlot(defnID, repmap)

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

			// TODO: temp. replace with `validateShardDealerInternals`
			if len(dealer.indexSlots) != int(minShardsPerNode) {
				t0.Fatalf("%v shard dealer book keeping mismatch - index defns recorded are %v but should have been only %v defns",
					t0.Name(), dealer.indexSlots, minShardsPerNode)
			}
			if len(dealer.slotsMap) != int(minShardsPerNode) {
				t0.Fatalf("%v shard dealer book keeping mismatch - slots created are %v but should have been only %v slots",
					t0.Name(), dealer.slotsMap, minShardsPerNode)
			}
			if dealer.nodeToShardCountMap[indexerNode.NodeUUID] != minShardsPerNode {
				t0.Fatalf("%v shard dealer book keeping mismatch - node slot count is %v but should have been only %v",
					t0.Name(), dealer.nodeToShardCountMap[indexerNode.NodeUUID],
					minShardsPerNode)
			}

			var extraIndex = createDummyIndexUsage(minShardsPerNode+1, false, false, false, false)
			extraIndex.initialNode = indexerNode

			var replicaMap = make(map[int]map[*IndexerNode]*IndexUsage)
			replicaMap[extraIndex.Instance.ReplicaId] = make(map[*IndexerNode]*IndexUsage)
			replicaMap[extraIndex.Instance.ReplicaId][indexerNode] = extraIndex
			slotID := dealer.GetSlot(extraIndex.DefnId, replicaMap)
			if _, exists := slotIDs[slotID]; slotID != 0 && !exists {
				t0.Fatalf("%v extra slot created when it was not expected. New slot %v. All Slots %v",
					t0.Name(), slotID, slotIDs)
			}
		})

		// Consider a scenario where we have minShardsPerNode as 5. New we have already created
		// 4 shards and then we get a request to create a non-primary index. since node is under
		// limit, we can create new slot. so we create 2 shards taking total shard count to 6
		// this test verifies the same. - with pass 1
		subt.Run("PrimaryAndSecondary", func(t0 *testing.T) {
			t0.Parallel()

			var dealer = NewShardDealer(
				minShardsPerNode,
				minPartitionsPerShard,
				maxDiskUsagePerShard,
				shardCapacity,
			)

			var indexerNode = createDummyIndexerNode(t0.Name(), minShardsPerNode, minShardsPerNode, 0, 0, 0)

			var alternateShardIDs = make(map[string]bool)

			var replicaMaps = getReplicaMapsForIndexerNode(indexerNode)

			for defnID := 0; defnID < len(indexerNode.Indexes); defnID++ {
				for _, repmap := range replicaMaps[c.IndexDefnId(defnID)] {
					slotID := dealer.GetSlot(c.IndexDefnId(defnID), repmap)

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
								t0.Fatalf("%v expected index (IsPrimary %v?) to have %v AlternateShardIDs but it has %v",
									t0.Name(), inst.IsPrimary, expectedAlternateShardIDCount,
									inst.AlternateShardIds)
							}

							for _, asi := range inst.AlternateShardIds {
								alternateShardIDs[asi] = true
							}
						}
					}
				}
			}

			if len(alternateShardIDs) != int(minShardsPerNode)*2 {
				// t0.Logf("Shard Dealer internal - %v", )
				t0.Fatalf("%v expected %v alternate shard ids but the we have (%v) %v",
					t0.Name(), minShardsPerNode*2,
					len(alternateShardIDs), alternateShardIDs)
			}

			// TODO: temp. replace with `validateShardDealerInternals`
			if int(dealer.nodeToShardCountMap[indexerNode.NodeUUID]) != len(alternateShardIDs) {
				t0.Fatalf("%v shard dealer book keeping mismatch - expected %v alternate shard ids but the we have %v",
					t0.Name(), len(alternateShardIDs), dealer.nodeToShardCountMap[indexerNode.NodeUUID])
			}
			if len(dealer.indexSlots) != len(indexerNode.Indexes) {
				t0.Fatalf("%v shard dealer book keeping mismatch - expected %v index defns but it has %v",
					t0.Name(), len(indexerNode.Indexes), len(dealer.indexSlots))
			}
		})

		// TODO: add partitioned index tests
		// TODO: add partitioned replicated index tests (replica index tests are not valid for
		// 	single node)
	})

	t.Run("Pass-1", func(subt *testing.T) {
		subt.Run("Basic-AllPrimary", func(t0 *testing.T) {
			t0.Parallel()

			var dealer = NewShardDealer(
				minShardsPerNode,
				minPartitionsPerShard,
				maxDiskUsagePerShard,
				shardCapacity,
			)

			var indexerNode = createDummyIndexerNode(t0.Name(), 0, minPartitionsPerShard, 0, 0, 0)

			var slotIDs = make(map[c.AlternateShard_SlotId]bool)

			var replicaMaps = getReplicaMapsForIndexerNode(indexerNode)

			for defnID, repMaps := range replicaMaps {
				for _, repmap := range repMaps {
					slotID := dealer.GetSlot(defnID, repmap)

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

			// TODO: temp. replace with `validateShardDealerInternals`
			if len(dealer.indexSlots) != int(minPartitionsPerShard) {
				t0.Fatalf("%v shard dealer book keeping mismatch - index defns recorded are %v but should have been only %v defns",
					t0.Name(), dealer.indexSlots, minPartitionsPerShard)
			}
			if len(dealer.slotsMap) != int(minShardsPerNode) {
				t0.Fatalf("%v shard dealer book keeping mismatch - slots created are %v but should have been only %v slots",
					t0.Name(), dealer.slotsMap, minShardsPerNode)
			}
			if dealer.nodeToShardCountMap[indexerNode.NodeUUID] != minShardsPerNode {
				t0.Fatalf("%v shard dealer book keeping mismatch - expected %v alternate shard ids but the we have %v",
					t0.Name(), minShardsPerNode, dealer.nodeToShardCountMap[indexerNode.NodeUUID])
			}
		})

		// This is similar to Pass-0 except here we also test that post creating minShardsPerNode
		// shards, we reuse existing shards
		subt.Run("PrimaryAndSecondary", func(t0 *testing.T) {
			t0.Parallel()

			var dealer = NewShardDealer(
				minShardsPerNode,
				minPartitionsPerShard,
				maxDiskUsagePerShard,
				shardCapacity,
			)

			var indexerNode = createDummyIndexerNode(t0.Name(), minShardsPerNode*2, minShardsPerNode*2, 0, 0, 0)

			var replicaMaps = getReplicaMapsForIndexerNode(indexerNode)
			var alternateShardIDs = make(map[string]bool)

			for defnID := 0; defnID < len(indexerNode.Indexes); defnID++ {
				for _, repmap := range replicaMaps[c.IndexDefnId(defnID)] {
					slotID := dealer.GetSlot(c.IndexDefnId(defnID), repmap)

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
								t0.Fatalf("%v expected index (IsPrimary %v?) to have %v AlternateShardIDs but it has %v",
									t0.Name(), inst.IsPrimary, expectedAlternateShardIDCount,
									inst.AlternateShardIds)
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

			// TODO: temp. replace with `validateShardDealerInternals`
			// since all primary indexes are created before, we create all minShardsPerNode
			// now when the new 2i indexes are created, we re-use the slots but we still end
			// up creating new shards
			if dealer.nodeToShardCountMap[indexerNode.NodeUUID] != 2*minShardsPerNode {
				t0.Fatalf("%v shard dealer book keeping mismatch - expected %v alternate shard ids but the we have %v",
					t0.Name(), 2*minShardsPerNode, dealer.nodeToShardCountMap[indexerNode.NodeUUID])
			}
			if len(dealer.indexSlots) != int(minShardsPerNode)*4 {
				t0.Fatalf("%v shard dealer book keeping mismatch - expected %v index defns but it has %v",
					t0.Name(), minShardsPerNode*4, len(dealer.indexSlots))
			}
		})

		subt.Run("MixCategoryIndexes", func(t0 *testing.T) {
			t0.Parallel()

			var dealer = NewShardDealer(
				minShardsPerNode,
				minPartitionsPerShard,
				maxDiskUsagePerShard,
				shardCapacity,
			)

			var indexerNode = createDummyIndexerNode(t0.Name(), 1, 1, 1, 1, 0)

			var replicaMaps = getReplicaMapsForIndexerNode(indexerNode)
			var alternateShardIDs = make(map[string]bool)

			for defnID := 0; defnID < len(indexerNode.Indexes); defnID++ {
				for _, repmap := range replicaMaps[c.IndexDefnId(defnID)] {
					slotID := dealer.GetSlot(c.IndexDefnId(defnID), repmap)

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
								t0.Fatalf("%v expected index (IsPrimary %v?) to have %v AlternateShardIDs but it has %v",
									t0.Name(), inst.IsPrimary, expectedAlternateShardIDCount,
									inst.AlternateShardIds)
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
				t0.Fatalf("%v shard dealer book keeping mismatch - expected %v categories but we have %v",
					t0.Name(), 3, dealer.slotsPerCategory)
			}

			if len(dealer.slotsPerCategory[STANDARD_SHARD_CATEGORY]) != 2 {
				t0.Fatalf("%v shard dealer book keeping mismatch - slots for %v are %v but expected only %v",
					t0.Name(), STANDARD_SHARD_CATEGORY,
					dealer.slotsPerCategory[STANDARD_SHARD_CATEGORY], minShardsPerNode*2,
				)
			}

			if len(dealer.slotsPerCategory[VECTOR_SHARD_CATEGORY]) != 1 {
				t0.Fatalf("%v shard dealer book keeping mismatch - slots for %v are %v but expected only %v",
					t0.Name(), VECTOR_SHARD_CATEGORY,
					dealer.slotsPerCategory[VECTOR_SHARD_CATEGORY], 1,
				)
			}
			if len(dealer.slotsPerCategory[BHIVE_SHARD_CATEGORY]) != 1 {
				t0.Fatalf("%v shard dealer book keeping mismatch - slots for %v are %v but expected only %v",
					t0.Name(), BHIVE_SHARD_CATEGORY,
					dealer.slotsPerCategory[BHIVE_SHARD_CATEGORY], 1,
				)
			}

			if int(dealer.nodeToShardCountMap[indexerNode.NodeUUID]) != len(alternateShardIDs) {
				t0.Fatalf("%v shard dealer book keeping mismatch - node has more shards %v than expected %v",
					t0.Name(), dealer.nodeToShardCountMap[indexerNode.NodeUUID],
					len(alternateShardIDs),
				)
			}

		})
	})

	t.Run("Pass-2", func(subt *testing.T) {
		subt.Run("Basic-AllPrimary", func(t0 *testing.T) {
			t0.Parallel()

			var dealer = NewShardDealer(minShardsPerNode, 1, shardCapacity)

			var indexerNode = createDummyIndexerNode(t0.Name(), 0, shardCapacity/10, 0, 0, 0)

			var replicaMaps = getReplicaMapsForIndexerNode(indexerNode)

			var slotIDs = make(map[c.AlternateShard_SlotId]bool)

			for defnID := 0; defnID < len(indexerNode.Indexes); defnID++ {
				for _, repmap := range replicaMaps[c.IndexDefnId(defnID)] {
					slotID := dealer.GetSlot(c.IndexDefnId(defnID), repmap)

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

			// TODO: temp. replace with `validateShardDealerInternals`
			if len(dealer.indexSlots) != len(slotIDs) {
				t0.Fatalf("%v shard dealer book keeping mismatch - index defns recorded are %v but should have been only %v defns",
					t0.Name(), dealer.indexSlots, minShardsPerNode)
			}
			if len(dealer.slotsMap) != len(slotIDs) {
				t0.Fatalf("%v shard dealer book keeping mismatch - slots created are %v but should have been only %v slots",
					t0.Name(), dealer.slotsMap, len(slotIDs))
			}
			if int(dealer.nodeToShardCountMap[indexerNode.NodeUUID]) != len(slotIDs) {
				t0.Fatalf("%v shard dealer book keeping mismatch - node slot count is %v but should have been only %v",
					t0.Name(), dealer.nodeToShardCountMap[indexerNode.NodeUUID],
					len(slotIDs))
			}
		})

		subt.Run("MaxShards", func(t0 *testing.T) {
			t0.Parallel()

			var testShardCapacity uint64 = 10
			var dealer = NewShardDealer(minShardsPerNode, 1, testShardCapacity)

			var indexerNode = createDummyIndexerNode(t0.Name(), testShardCapacity, 0, 0, 0, 0)

			var replicaMaps = getReplicaMapsForIndexerNode(indexerNode)
			var slotIDs = make(map[c.AlternateShard_SlotId]bool)
			var alternateShardIDs = make(map[string]bool)

			for defnID := 0; defnID < len(indexerNode.Indexes); defnID++ {
				for _, repmap := range replicaMaps[c.IndexDefnId(defnID)] {
					var slotID = dealer.GetSlot(c.IndexDefnId(defnID), repmap)

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
								t0.Fatalf("%v expected index (IsPrimary %v?) to have %v AlternateShardIDs but it has %v",
									t0.Name(), inst.IsPrimary, expectedAlternateShardIDCount,
									inst.AlternateShardIds)
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
			// TODO: temp. replace with `validateShardDealerInternals`
			if len(dealer.indexSlots) != int(testShardCapacity) {
				t0.Fatalf("%v shard dealer book keeping mismatch - index defns recorded are %v but should have been only %v defns",
					t0.Name(), dealer.indexSlots, testShardCapacity)
			}
			if len(dealer.slotsMap) != len(slotIDs) {
				t0.Fatalf("%v shard dealer book keeping mismatch - slots created are %v but should have been only %v slots",
					t0.Name(), dealer.slotsMap, len(slotIDs))
			}
			if int(dealer.nodeToShardCountMap[indexerNode.NodeUUID]) != len(alternateShardIDs) {
				t0.Fatalf("%v shard dealer book keeping mismatch - node slot count is %v but should have been only %v",
					t0.Name(), dealer.nodeToShardCountMap[indexerNode.NodeUUID],
					len(slotIDs))
			}

			// create extra index. it should not create a new shard
			var extraIndex = createDummyIndexUsage(shardCapacity+1, false, false, false, false)
			extraIndex.initialNode = indexerNode

			var replicaMap = make(map[int]map[*IndexerNode]*IndexUsage)
			replicaMap[extraIndex.Instance.ReplicaId] = make(map[*IndexerNode]*IndexUsage)
			replicaMap[extraIndex.Instance.ReplicaId][indexerNode] = extraIndex
			slotID := dealer.GetSlot(extraIndex.DefnId, replicaMap)
			if _, exists := slotIDs[slotID]; slotID != 0 && !exists {
				t0.Fatalf("%v extra slot created when it was not expected. New slot %v. All Slots %v",
					t0.Name(), slotID, slotIDs)
			}
		})

		subt.Run("MixCategoryIndexes", func(t0 *testing.T) {
			t0.Parallel()

			var dealer = NewShardDealer(
				minShardsPerNode,
				minPartitionsPerShard,
				maxDiskUsagePerShard,
				shardCapacity,
			)

			var indexerNode = createDummyIndexerNode(t0.Name(), minShardsPerNode, minShardsPerNode, 1, 1, 0)

			var replicaMaps = getReplicaMapsForIndexerNode(indexerNode)
			var alternateShardIDs = make(map[string]bool)

			for defnID := 0; defnID < len(indexerNode.Indexes); defnID++ {
				for _, repmap := range replicaMaps[c.IndexDefnId(defnID)] {
					slotID := dealer.GetSlot(c.IndexDefnId(defnID), repmap)

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
								t0.Fatalf("%v expected index (IsPrimary %v?) to have %v AlternateShardIDs but it has %v",
									t0.Name(), inst.IsPrimary, expectedAlternateShardIDCount,
									inst.AlternateShardIds)
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
				t0.Fatalf("%v shard dealer book keeping mismatch - expected %v categories but we have %v",
					t0.Name(), 3, dealer.slotsPerCategory)
			}

			if len(dealer.slotsPerCategory[STANDARD_SHARD_CATEGORY]) != int(minShardsPerNode) {
				t0.Fatalf("%v shard dealer book keeping mismatch - slots for %v are %v but expected only %v",
					t0.Name(), STANDARD_SHARD_CATEGORY,
					dealer.slotsPerCategory[STANDARD_SHARD_CATEGORY], minShardsPerNode,
				)
			}
			if len(dealer.slotsPerCategory[VECTOR_SHARD_CATEGORY]) != 1 {
				t0.Fatalf("%v shard dealer book keeping mismatch - slots for %v are %v but expected only %v",
					t0.Name(), VECTOR_SHARD_CATEGORY,
					dealer.slotsPerCategory[VECTOR_SHARD_CATEGORY], 1,
				)
			}
			if len(dealer.slotsPerCategory[BHIVE_SHARD_CATEGORY]) != 1 {
				t0.Fatalf("%v shard dealer book keeping mismatch - slots for %v are %v but expected only %v",
					t0.Name(), BHIVE_SHARD_CATEGORY,
					dealer.slotsPerCategory[BHIVE_SHARD_CATEGORY], 1,
				)
			}

			if int(dealer.nodeToShardCountMap[indexerNode.NodeUUID]) != len(alternateShardIDs) {
				t0.Fatalf("%v shard dealer book keeping mismatch - node has more shards %v than expected %v",
					t0.Name(), dealer.nodeToShardCountMap[indexerNode.NodeUUID],
					len(alternateShardIDs),
				)
			}

		})
	})

	t.Run("Pass-3", func(subt *testing.T) {
		var testShardCapacity uint64 = 10
		var dealer = NewShardDealer(minShardsPerNode, 1, testShardCapacity)

		var indexerNode = createDummyIndexerNode(subt.Name(), testShardCapacity, testShardCapacity, testShardCapacity/4, testShardCapacity/4, 0)

		var replicaMaps = getReplicaMapsForIndexerNode(indexerNode)
		var slotIDs = make(map[c.AlternateShard_SlotId]bool)
		var alternateShardIDs = make(map[string]bool)

		for defnID := len(indexerNode.Indexes); defnID >= 0; defnID-- {
			for _, repmap := range replicaMaps[c.IndexDefnId(defnID)] {
				var slotID = dealer.GetSlot(c.IndexDefnId(defnID), repmap)

				if slotID == 0 {
					subt.Fatalf("%v failed to get slot for replica map %v",
						subt.Name(), repmap)
				}
				slotIDs[slotID] = true
				for _, nodeMap := range repmap {
					for _, inst := range nodeMap {
						var expectedAlternateShardIDCount = 2
						if inst.IsPrimary {
							expectedAlternateShardIDCount = 1
						}

						if len(inst.AlternateShardIds) != expectedAlternateShardIDCount {
							subt.Fatalf("%v expected index (IsPrimary %v?) to have %v AlternateShardIDs but it has %v",
								subt.Name(), inst.IsPrimary, expectedAlternateShardIDCount,
								inst.AlternateShardIds)
						}

						for _, asi := range inst.AlternateShardIds {
							alternateShardIDs[asi] = true
						}
					}
				}
			}
		}

		if len(slotIDs) != int(testShardCapacity)/2 {
			subt.Fatalf("%v expected to create %v slots we have %v (%v)",
				subt.Name(), testShardCapacity, len(slotIDs), slotIDs)
		}
		if len(alternateShardIDs) != int(testShardCapacity) {
			subt.Fatalf("%v expected to create %v shards we have %v (%v)",
				subt.Name(), testShardCapacity, len(alternateShardIDs), alternateShardIDs)
		}
		// TODO: temp. replace with `validateShardDealerInternals`
		if len(dealer.indexSlots) != len(indexerNode.Indexes) {
			subt.Fatalf("%v shard dealer book keeping mismatch - index defns recorded are %v but should have been only %v defns",
				subt.Name(), dealer.indexSlots, len(indexerNode.Indexes))
		}
		if len(dealer.slotsMap) != len(slotIDs) {
			subt.Fatalf("%v shard dealer book keeping mismatch - slots created are %v but should have been only %v slots",
				subt.Name(), dealer.slotsMap, len(slotIDs))
		}
		if int(dealer.nodeToShardCountMap[indexerNode.NodeUUID]) != len(alternateShardIDs) {
			subt.Fatalf("%v shard dealer book keeping mismatch - node slot count is %v but should have been only %v",
				subt.Name(), dealer.nodeToShardCountMap[indexerNode.NodeUUID],
				len(slotIDs))
		}

		// create extra index. it should not create a new shard
		var extraIndex = createDummyIndexUsage(uint64(len(indexerNode.Indexes)), false, false, false, false)
		extraIndex.initialNode = indexerNode

		var replicaMap = make(map[int]map[*IndexerNode]*IndexUsage)
		replicaMap[extraIndex.Instance.ReplicaId] = make(map[*IndexerNode]*IndexUsage)
		replicaMap[extraIndex.Instance.ReplicaId][indexerNode] = extraIndex
		slotID := dealer.GetSlot(extraIndex.DefnId, replicaMap)
		if slotID == 0 {
			subt.Fatalf("%v failed to get slot id for replicaMap %v",
				subt.Name(), replicaMap)
		}
		if _, exists := slotIDs[slotID]; !exists {
			subt.Fatalf("%v extra slot created when it was not expected. New slot %v. All Slots %v",
				subt.Name(), slotID, slotIDs)
		}

	})
}

// TODO: add tests for getShardCategory
// TODO: add tests for RecordIndexUsage
// TODO: add tests for cluster level validation of dealer
// func validateShardDealerInternals(*ShardDealer, []*IndexerNode) error
