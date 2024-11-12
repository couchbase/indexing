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
	maxDiskUsagePerShard  uint64 = 256 * 1024 * 1024
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

	var indexerNode = createDummyIndexerNode(t.Name(), 1, 1, 0, 0, 0)

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

func TestSingleNodeAssignment(t *testing.T) {
	t.Parallel()

	t.Run("Pass-0", func(subt *testing.T) {
		subt.Run("Basic-AllPrimary", func(t0 *testing.T) {
			t0.Parallel()

			var dealer = NewShardDealer(
				minShardsPerNode,
				minPartitionsPerShard,
				maxDiskUsagePerShard,
				shardCapacity,
				createNewAlternateShardIDGenerator(), nil)

			var indexerNode = createDummyIndexerNode(t0.Name(), 0, minShardsPerNode, 0, 0, 0)

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

			// TODO: temp. replace with `validateShardDealerInternals`
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

			var extraIndex = createDummyIndexUsage(minShardsPerNode+1, false, false, false, false)
			extraIndex.initialNode = indexerNode

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
				createNewAlternateShardIDGenerator(), nil)

			var indexerNode = createDummyIndexerNode(t0.Name(), minShardsPerNode, minShardsPerNode, 0, 0, 0)

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

			if len(alternateShardIDs) != int(minShardsPerNode)*2 {
				// t0.Logf("Shard Dealer internal - %v", )
				t0.Fatalf("%v expected %v alternate shard ids but the we have (%v) %v",
					t0.Name(), minShardsPerNode*2,
					len(alternateShardIDs), alternateShardIDs)
			}

			// TODO: temp. replace with `validateShardDealerInternals`
			if int(dealer.nodeToShardCountMap[indexerNode.NodeUUID]) != len(alternateShardIDs) {
				t0.Fatalf(
					"%v shard dealer book keeping mismatch - expected %v alternate shard ids but the we have %v",
					t0.Name(),
					len(alternateShardIDs),
					dealer.nodeToShardCountMap[indexerNode.NodeUUID],
				)
			}
			if len(dealer.partnSlots) != len(indexerNode.Indexes) {
				t0.Fatalf(
					"%v shard dealer book keeping mismatch - expected %v index defns but it has %v",
					t0.Name(),
					len(indexerNode.Indexes),
					len(dealer.partnSlots),
				)
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
				createNewAlternateShardIDGenerator(), nil)

			var indexerNode = createDummyIndexerNode(t0.Name(), 0, minPartitionsPerShard, 0, 0, 0)

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

			// TODO: temp. replace with `validateShardDealerInternals`
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
				createNewAlternateShardIDGenerator(), nil)

			var indexerNode = createDummyIndexerNode(t0.Name(), minShardsPerNode*2, minShardsPerNode*2, 0, 0, 0)

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

			// TODO: temp. replace with `validateShardDealerInternals`
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
		})

		subt.Run("MixCategoryIndexes", func(t0 *testing.T) {
			t0.Parallel()

			var dealer = NewShardDealer(
				minShardsPerNode,
				minPartitionsPerShard,
				maxDiskUsagePerShard,
				shardCapacity,
				createNewAlternateShardIDGenerator(), nil)

			var indexerNode = createDummyIndexerNode(t0.Name(), 1, 1, 1, 1, 0)

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

		})
	})

	t.Run("Pass-2", func(subt *testing.T) {
		subt.Run("Basic-AllPrimary", func(t0 *testing.T) {
			t0.Parallel()

			var dealer = NewShardDealer(
				minShardsPerNode,
				1,
				maxDiskUsagePerShard,
				shardCapacity,
				createNewAlternateShardIDGenerator(), nil)

			var indexerNode = createDummyIndexerNode(t0.Name(), 0, shardCapacity/10, 0, 0, 0)

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

			// TODO: temp. replace with `validateShardDealerInternals`
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
		})

		subt.Run("MaxShards", func(t0 *testing.T) {
			t0.Parallel()

			var testShardCapacity uint64 = 10
			var dealer = NewShardDealer(minShardsPerNode,
				1,
				maxDiskUsagePerShard,
				testShardCapacity,
				createNewAlternateShardIDGenerator(), nil)

			var indexerNode = createDummyIndexerNode(t0.Name(), testShardCapacity, 0, 0, 0, 0)

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
			// TODO: temp. replace with `validateShardDealerInternals`
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

			// create extra index. it should not create a new shard
			var extraIndex = createDummyIndexUsage(shardCapacity+1, false, false, false, false)
			extraIndex.initialNode = indexerNode

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
		})

		subt.Run("MixCategoryIndexes", func(t0 *testing.T) {
			t0.Parallel()

			var dealer = NewShardDealer(
				minShardsPerNode,
				minPartitionsPerShard,
				maxDiskUsagePerShard,
				shardCapacity,
				createNewAlternateShardIDGenerator(), nil)

			var indexerNode = createDummyIndexerNode(t0.Name(), minShardsPerNode, minShardsPerNode, 1, 1, 0)

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

		})
	})

	t.Run("Pass-3", func(subt *testing.T) {
		var testShardCapacity uint64 = 10
		var dealer = NewShardDealer(
			minShardsPerNode,
			1,
			maxDiskUsagePerShard,
			testShardCapacity,
			createNewAlternateShardIDGenerator(), nil)

		var indexerNode = createDummyIndexerNode(subt.Name(), testShardCapacity, testShardCapacity, testShardCapacity/4, testShardCapacity/4, 0)

		var replicaMaps = getReplicaMapsForIndexerNode(indexerNode)
		var slotIDs = make(map[c.AlternateShard_SlotId]bool)
		var alternateShardIDs = make(map[string]bool)

		for defnID := len(indexerNode.Indexes); defnID >= 0; defnID-- {
			for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID)] {
				var slotID = dealer.GetSlot(c.IndexDefnId(defnID), partnID, repmap)

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
							subt.Fatalf(
								"%v expected index (IsPrimary %v?) to have %v AlternateShardIDs but it has %v",
								subt.Name(),
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
			subt.Fatalf("%v expected to create %v slots we have %v (%v)",
				subt.Name(), testShardCapacity, len(slotIDs), slotIDs)
		}
		if len(alternateShardIDs) != int(testShardCapacity) {
			subt.Fatalf("%v expected to create %v shards we have %v (%v)",
				subt.Name(), testShardCapacity, len(alternateShardIDs), alternateShardIDs)
		}
		// TODO: temp. replace with `validateShardDealerInternals`
		if len(dealer.partnSlots) != len(indexerNode.Indexes) {
			subt.Fatalf(
				"%v shard dealer book keeping mismatch - index defns recorded are %v but should have been only %v defns",
				subt.Name(),
				dealer.partnSlots,
				len(indexerNode.Indexes),
			)
		}
		if len(dealer.slotsMap) != len(slotIDs) {
			subt.Fatalf(
				"%v shard dealer book keeping mismatch - slots created are %v but should have been only %v slots",
				subt.Name(),
				dealer.slotsMap,
				len(slotIDs),
			)
		}
		if int(dealer.nodeToShardCountMap[indexerNode.NodeUUID]) != len(alternateShardIDs) {
			subt.Fatalf(
				"%v shard dealer book keeping mismatch - node slot count is %v but should have been only %v",
				subt.Name(),
				dealer.nodeToShardCountMap[indexerNode.NodeUUID],
				len(slotIDs),
			)
		}

		// create extra index. it should not create a new shard
		var extraIndex = createDummyIndexUsage(uint64(len(indexerNode.Indexes)), false, false, false, false)
		extraIndex.initialNode = indexerNode

		var replicaMap = make(map[int]map[*IndexerNode]*IndexUsage)
		replicaMap[extraIndex.Instance.ReplicaId] = make(map[*IndexerNode]*IndexUsage)
		replicaMap[extraIndex.Instance.ReplicaId][indexerNode] = extraIndex
		slotID := dealer.GetSlot(extraIndex.DefnId, extraIndex.PartnId, replicaMap)
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

func TestGetShardCategory(t *testing.T) {
	t.Parallel()

	// MOI index
	var moiIndex = createDummyIndexUsage(0, false, false, false, true)
	var shardCategory = getIndexCategory(moiIndex)
	if shardCategory != InvalidShardCategory {
		t.Fatalf(
			"%v MoI index should have had invalid shard category but is %v",
			t.Name(),
			shardCategory,
		)
	}

	var plasmaIndex = createDummyIndexUsage(0, true, false, false, false)
	shardCategory = getIndexCategory(plasmaIndex)
	if shardCategory != StandardShardCategory {
		t.Fatalf(
			"%v primary index should have had std shard category but is %v",
			t.Name(),
			shardCategory,
		)
	}

	plasmaIndex = createDummyIndexUsage(0, false, false, false, false)
	shardCategory = getIndexCategory(plasmaIndex)
	if shardCategory != StandardShardCategory {
		t.Fatalf(
			"%v plasma index should have had std shard category but is %v",
			t.Name(),
			shardCategory,
		)
	}

	var compositeVectorIndex = createDummyIndexUsage(0, false, true, false, false)
	shardCategory = getIndexCategory(compositeVectorIndex)
	if shardCategory != VectorShardCategory {
		t.Fatalf(
			"%v vector index should have had vector shard category but is %v",
			t.Name(),
			shardCategory,
		)
	}

	var bhiveIndex = createDummyIndexUsage(0, false, false, true, false)
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

	var moiIndex = createDummyIndexUsage(0, false, false, false, true)
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

	var plasmaIndex = createDummyIndexUsage(0, false, false, false, false)
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

	plasmaIndex = createDummyIndexUsage(1, false, false, false, false)
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

	var indexer = createDummyIndexerNode("0", 0, 0, 0, 0, 0)
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
	plasmaIndex = createDummyIndexUsage(2, false, false, false, false)
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

// TODO: add tests for cluster level validation of dealer
// func validateShardDealerInternals(*ShardDealer, []*IndexerNode) error
