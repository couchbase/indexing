package planner

import (
	"fmt"
	"testing"

	c "github.com/couchbase/indexing/secondary/common"
)

var minPartitionsPerShard = 15
var minShardsPerNode = 5
var shardCapacity = 1000

func createDummyIndexUsage(id int, isPrimary, isVector, isBhive bool) *IndexUsage {
	return &IndexUsage{
		Bucket:     "bucket",
		Scope:      "scope",
		Collection: "collection",

		DefnId:  c.IndexDefnId(id),
		InstId:  c.IndexInstId(id),
		PartnId: c.PartitionId(0),

		StorageMode: c.PlasmaDB,

		IsPrimary: isPrimary,

		Name: fmt.Sprintf("index_%v", id),

		Instance: &c.IndexInst{
			InstId:    c.IndexInstId(id),
			State:     c.INDEX_STATE_ACTIVE,
			ReplicaId: 0,

			Defn: c.IndexDefn{
				IsVectorIndex: isVector,
				VectorMeta: &c.VectorMetadata{
					IsBhive: isBhive,
				},
			},
		},
	}
}

func TestBasicSlotAssignment(t *testing.T) {
	var dealer = NewShardDealer(5, 15, 1000)

	var indexerNode = IndexerNode{
		NodeId:   "indexer-node-id",
		NodeUUID: "indexer-node-uuid",
		Indexes:  make([]*IndexUsage, 0, 2),
	}

	for i := 0; i < 2; i++ {
		var inst = createDummyIndexUsage(i, i == 0, false, false)
		inst.initialNode = &indexerNode

		indexerNode.Indexes = append(indexerNode.Indexes, inst)
	}

	for _, inst := range indexerNode.Indexes {
		var replicaMap = make(map[int]map[*IndexerNode]*IndexUsage)
		replicaMap[inst.Instance.ReplicaId] = make(map[*IndexerNode]*IndexUsage)
		replicaMap[inst.Instance.ReplicaId][&indexerNode] = inst

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
}

func TestSingleNodeAssignment(t *testing.T) {
	t.Run("Pass-0", func(subt *testing.T) {
		subt.Run("Basic-AllPrimary", func(t0 *testing.T) {
			t0.Parallel()

			var dealer = NewShardDealer(5, 15, 1000)

			var indexerNode = IndexerNode{
				NodeId:   "indexer-node-id",
				NodeUUID: "indexer-node-uuid",
				Indexes:  make([]*IndexUsage, 0, minShardsPerNode),
			}

			for i := 0; i < minShardsPerNode; i++ {
				var newIndex = createDummyIndexUsage(i, true, false, false)
				newIndex.initialNode = &indexerNode

				indexerNode.Indexes = append(indexerNode.Indexes, newIndex)
			}

			var slotIDs = make(map[c.AlternateShard_SlotId]bool)

			for _, inst := range indexerNode.Indexes {
				var replicaMap = make(map[int]map[*IndexerNode]*IndexUsage)
				replicaMap[inst.Instance.ReplicaId] = make(map[*IndexerNode]*IndexUsage)
				replicaMap[inst.Instance.ReplicaId][&indexerNode] = inst

				slotID := dealer.GetSlot(inst.DefnId, replicaMap)

				if slotID == 0 {
					t0.Fatalf("%v failed to get slot id for index %v in 0th pass",
						t0.Name(), inst.String())
				}

				slotIDs[slotID] = true

				var expectedAlternateShardIDCount = 1

				if len(inst.AlternateShardIds) != expectedAlternateShardIDCount {
					t0.Fatalf("%v expected index (IsPrimary %v?) to have %v AlternateShardIDs but it has %v",
						t0.Name(), inst.IsPrimary, expectedAlternateShardIDCount,
						inst.AlternateShardIds)
				}
			}

			if len(slotIDs) != minShardsPerNode {
				t0.Fatalf("%v slots created are %v but should have been only %v slots",
					t0.Name(), slotIDs, minShardsPerNode)
			}

			var extraIndex = createDummyIndexUsage(minShardsPerNode+1, false, false, false)
			extraIndex.initialNode = &indexerNode

			var replicaMap = make(map[int]map[*IndexerNode]*IndexUsage)
			replicaMap[extraIndex.Instance.ReplicaId] = make(map[*IndexerNode]*IndexUsage)
			replicaMap[extraIndex.Instance.ReplicaId][&indexerNode] = extraIndex
			slotID := dealer.GetSlot(extraIndex.DefnId, replicaMap)
			if _, exists := slotIDs[slotID]; slotID != 0 && !exists {
				t0.Fatalf("%v extra slot created when it was not expected. New slot %v. All Slots %v",
					t0.Name(), slotID, slotIDs)
			}
		})

		// Consider a scenario where we have minShardsPerNode as 5. New we have already created
		// 4 shards and then we get a request to create a non-primary index. since node is under
		// limit, we can create new slot. so we create 2 shards taking total shard count to 6
		// this test verifies the same
		subt.Run("PrimaryAndSecondary", func(t0 *testing.T) {
			t0.Parallel()

			var dealer = NewShardDealer(5, 15, 1000)

			var indexerNode = IndexerNode{
				NodeId:   "indexer-node-id",
				NodeUUID: "indexer-node-uuid",
				Indexes:  make([]*IndexUsage, 0, minShardsPerNode),
			}
			var isPrimaryGenerator = true

			var i = 0
			for i = 0; i < minShardsPerNode; i++ {
				var newIndex = createDummyIndexUsage(i, isPrimaryGenerator, false, false)
				newIndex.initialNode = &indexerNode
				if !isPrimaryGenerator {
					i++
				}

				isPrimaryGenerator = !isPrimaryGenerator

				indexerNode.Indexes = append(indexerNode.Indexes, newIndex)
			}

			var alternateShardIDs = make(map[string]bool)

			for _, inst := range indexerNode.Indexes {
				var replicaMap = make(map[int]map[*IndexerNode]*IndexUsage)
				replicaMap[inst.Instance.ReplicaId] = make(map[*IndexerNode]*IndexUsage)
				replicaMap[inst.Instance.ReplicaId][&indexerNode] = inst

				slotID := dealer.GetSlot(inst.DefnId, replicaMap)

				if slotID == 0 {
					t0.Fatalf("%v failed to get slot id for index %v in 0th pass",
						t0.Name(), inst.String())
				}

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

			if len(alternateShardIDs) != i {
				t0.Fatalf("%v expected %v alternate shard ids but the we have %v",
					t0.Name(), i, alternateShardIDs)
			}

		})
	})
}
