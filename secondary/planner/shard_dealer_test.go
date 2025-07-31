package planner

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/stretchr/testify/assert"
)

const (
	minPartitionsPerShard   uint64  = 16
	minShardsPerNode        uint64  = 6
	shardCapacity           uint64  = 1000
	maxDiskUsagePerShard    uint64  = 256 * 1024 * 1024
	diskUsageThresholdRatio float64 = 0.5
	maxPartitionsPerSlot    uint64  = 10200
)

func getTestShardDealer(minShardsPerNode, minPartitionsPerShard, maxDiskUsage, shardCapacity uint64,
	alternateShardIDGenerator func() (*c.AlternateShardId, error), moveCallback moveFuncCb) *ShardDealer {
	return NewShardDealer(
		minShardsPerNode,
		minPartitionsPerShard,
		maxDiskUsage,
		maxPartitionsPerSlot,
		shardCapacity,
		diskUsageThresholdRatio,
		alternateShardIDGenerator, moveCallback)
}

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
	var indexes = make([]*IndexUsage, 0, params.numReplicas+1)
	for i := 1; i <= params.numReplicas+1; i++ {
		var index = createDummyIndexUsage(params)
		index.InstId = c.IndexInstId(time.Now().UnixNano())
		index.Instance.InstId = index.InstId
		index.Instance.ReplicaId = i
		index.Instance.Defn.NumReplica = uint32(params.numReplicas)
		indexes = append(indexes, index)
	}
	return indexes
}

func createDummyPartitionedIndexUsages(params createIdxParam) []*IndexUsage {
	var partitions = make([]*IndexUsage, 0, params.numPartns)
	if params.numPartns == 0 {
		return []*IndexUsage{createDummyIndexUsage(params)}
	}
	for i := 1; i <= params.numPartns; i++ {
		var partn = createDummyIndexUsage(params)
		partn.PartnId = c.PartitionId(i)
		partn.Instance.Defn.NumPartitions = uint32(params.numPartns)
		partitions = append(partitions, partn)
	}
	return partitions
}

func createDummyReplicaPartitionedIndexUsage(params createIdxParam) [][]*IndexUsage {
	var indexes = make([][]*IndexUsage, 0, params.numReplicas+1)
	for i := 1; i <= params.numReplicas+1; i++ {
		var instID = c.IndexInstId(time.Now().UnixNano())
		var partitions = createDummyPartitionedIndexUsages(params)
		for _, partn := range partitions {
			partn.InstId = instID
			partn.Instance.InstId = instID
			partn.Instance.ReplicaId = i
			partn.Instance.Defn.NumReplica = uint32(params.numReplicas)
		}
		indexes = append(indexes, partitions)
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

var op strings.Builder

func TestMain(m *testing.M) {
	op = strings.Builder{}
	logging.SetLogWriter(&op)
	logging.SetLogLevel(logging.Warn)
	exitCode := m.Run()
	if exitCode != 0 {
		fmt.Println(op.String())
	}
	os.Exit(exitCode)
}

func TestBasicSlotAssignment(t *testing.T) {
	// t.Parallel()

	var dealer = getTestShardDealer(
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

	var tracker uint64
	for _, inst := range indexerNode.Indexes {
		var replicaMap = make(map[int]map[*IndexerNode]*IndexUsage)
		replicaMap[inst.Instance.ReplicaId] = make(map[*IndexerNode]*IndexUsage)
		replicaMap[inst.Instance.ReplicaId][indexerNode] = inst

		tracker++
		slotID := dealer.GetSlot(inst.DefnId, inst.PartnId, replicaMap, tracker, false)

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

func (cip *createIdxParam) clone() *createIdxParam {
	return &createIdxParam{
		count:       cip.count,
		isPrimary:   cip.isPrimary,
		isVector:    cip.isVector,
		isBhive:     cip.isBhive,
		isMoi:       cip.isMoi,
		numPartns:   cip.numPartns,
		numReplicas: cip.numReplicas,
		defnid:      cip.defnid,
	}
}

func createDummyIndexerNode(nodeID nodeUUID, cips ...createIdxParam) *IndexerNode {
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
				indexerNode.Indexes = append(indexerNode.Indexes, newIndex...)
			} else {
				var newIndex = createDummyIndexUsage(cip)
				indexerNode.Indexes = append(indexerNode.Indexes, newIndex)
			}
			id++
		}
	}

	return &indexerNode
}

func createDummyIndexerNodes(nNodesHint int, cips ...createIdxParam) []*IndexerNode {
	var nodes = make([]*IndexerNode, 0, nNodesHint) // replicaID to IndexerNode map

	for i := 0; i < nNodesHint; i++ {
		nodes = append(nodes, createDummyIndexerNode(fmt.Sprintf("%v", i)))
	}

	getNNodes := func(n int) []*IndexerNode {
		if len(nodes) < n {
			for i := len(nodes); i < n; i++ {
				nodes = append(nodes, createDummyIndexerNode(fmt.Sprintf("%v", i)))
			}
		}

		var selNodes = make([]*IndexerNode, n)
		var randoStart = rand.Intn(len(nodes))
		for i := 0; i < n; i++ {
			selNodes[i] = nodes[(randoStart+i)%len(nodes)]
		}

		return selNodes
	}

	shuffleNodes := func(nodes []*IndexerNode) {
		rand.Shuffle(len(nodes), func(i, j int) {
			nodes[i], nodes[j] = nodes[j], nodes[i]
		})
	}

	var defnID uint64 = 1
	for _, cip := range cips {
		for i := 0; i < int(cip.count); i++ {
			cip.defnid = defnID
			defnID++
			var indexes = createDummyReplicaPartitionedIndexUsage(cip)

			var nodesForDist = getNNodes(cip.numReplicas + 1)
			var partnCount = cip.numPartns
			if partnCount == 0 {
				partnCount++
			}
			for i := 0; i < partnCount; i++ {
				var nodesForThisReplica = make([]*IndexerNode, len(nodesForDist))
				copy(nodesForThisReplica, nodesForDist)
				shuffleNodes(nodesForThisReplica)

				for j := 0; j <= cip.numReplicas; j++ {
					nodesForThisReplica[j].Indexes = append(nodesForThisReplica[j].Indexes, indexes[j][i])
				}
			}
		}
	}

	return nodes
}

func getReplicaMapsForIndexerNodes(
	nodes ...*IndexerNode,
) map[c.IndexDefnId]map[c.PartitionId]map[int]map[*IndexerNode]*IndexUsage {
	var defnMap = make(map[c.IndexDefnId]map[c.PartitionId]map[int]map[*IndexerNode]*IndexUsage)

	for _, node := range nodes {
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
	}

	return defnMap
}

func TestSingleNode_Pass0(t *testing.T) {
	// t.Parallel()

	t.Run("Basic-AllPrimary", func(t0 *testing.T) {
		t0.Parallel()

		var dealer = getTestShardDealer(
			minShardsPerNode,
			minPartitionsPerShard,
			maxDiskUsagePerShard,
			shardCapacity,
			createNewAlternateShardIDGenerator(), nil)

		var cip = createIdxParam{count: minShardsPerNode, isPrimary: true}
		var indexerNode = createDummyIndexerNode(t0.Name(), cip)

		var slotIDs = make(map[c.AlternateShard_SlotId]bool)

		var replicaMaps = getReplicaMapsForIndexerNodes(indexerNode)

		var tracker uint64
		for defnID, repMaps := range replicaMaps {
			for partnID, repmap := range repMaps {
				tracker++
				slotID := dealer.GetSlot(defnID, partnID, repmap, tracker, false)

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
		indexerNode.Indexes = append(indexerNode.Indexes, extraIndex)

		var replicaMap = make(map[int]map[*IndexerNode]*IndexUsage)
		replicaMap[extraIndex.Instance.ReplicaId] = make(map[*IndexerNode]*IndexUsage)
		replicaMap[extraIndex.Instance.ReplicaId][indexerNode] = extraIndex
		tracker++
		slotID := dealer.GetSlot(extraIndex.DefnId, extraIndex.PartnId, replicaMap, tracker, false)
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
			var dealer = getTestShardDealer(
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

			var replicaMaps = getReplicaMapsForIndexerNodes(indexerNode)

			var tracker uint64
			for defnID := 0; defnID < len(indexerNode.Indexes); defnID++ {
				for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID)] {
					tracker++
					slotID := dealer.GetSlot(c.IndexDefnId(defnID), partnID, repmap, tracker, false)

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
			var dealer = getTestShardDealer(
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

			var replicaMaps = getReplicaMapsForIndexerNodes(indexerNode)
			var tracker uint64
			for defnID := 0; defnID < len(indexerNode.Indexes); defnID++ {
				for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID)] {
					tracker++
					slotID := dealer.GetSlot(c.IndexDefnId(defnID), partnID, repmap, tracker, false)

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

	// add partitioned index tests
	// same as above test but we here use partitions instead of multiple indexes
	t.Run("PartitionedIndexes", func(t0 *testing.T) {
		t0.Parallel()

		// PRIMARY: numPartns = minShards - 1
		// 2i: 1
		{
			var dealer = getTestShardDealer(
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

			var replicaMaps = getReplicaMapsForIndexerNodes(indexerNode)

			var tracker uint64
			for defnID := 0; defnID < len(indexerNode.Indexes); defnID++ {
				for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID)] {
					tracker++
					slotID := dealer.GetSlot(c.IndexDefnId(defnID), partnID, repmap, tracker, false)

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
			var dealer = getTestShardDealer(
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

			var replicaMaps = getReplicaMapsForIndexerNodes(indexerNode)

			var tracker uint64
			for defnID := 0; defnID < len(indexerNode.Indexes); defnID++ {
				for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID)] {
					tracker++
					slotID := dealer.GetSlot(c.IndexDefnId(defnID), partnID, repmap, tracker, false)

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

func TestSingleNode_Pass1(t *testing.T) {
	// t.Parallel()

	t.Run("Basic-AllPrimary", func(t0 *testing.T) {
		t0.Parallel()

		var dealer = getTestShardDealer(
			minShardsPerNode,
			minPartitionsPerShard,
			maxDiskUsagePerShard,
			shardCapacity,
			createNewAlternateShardIDGenerator(), nil)

		var cip = createIdxParam{count: minPartitionsPerShard, isPrimary: true}

		var indexerNode = createDummyIndexerNode(t0.Name(), cip)

		var slotIDs = make(map[c.AlternateShard_SlotId]bool)

		var replicaMaps = getReplicaMapsForIndexerNodes(indexerNode)

		var tracker uint64
		for defnID, repMaps := range replicaMaps {
			for partnID, repmap := range repMaps {
				tracker++
				slotID := dealer.GetSlot(defnID, partnID, repmap, tracker, false)

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

		var dealer = getTestShardDealer(
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

		var replicaMaps = getReplicaMapsForIndexerNodes(indexerNode)
		var alternateShardIDs = make(map[string]bool)

		var tracker uint64
		for defnID := 0; defnID < len(indexerNode.Indexes); defnID++ {
			for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID)] {
				tracker++
				slotID := dealer.GetSlot(c.IndexDefnId(defnID), partnID, repmap, tracker, false)

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
			var dealer = getTestShardDealer(
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

			var replicaMaps = getReplicaMapsForIndexerNodes(indexerNode)

			var tracker uint64
			for defnID := 0; defnID < len(indexerNode.Indexes); defnID++ {
				for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID)] {
					tracker++
					slotID := dealer.GetSlot(c.IndexDefnId(defnID), partnID, repmap, tracker, false)

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
			var dealer = getTestShardDealer(
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

			var replicaMaps = getReplicaMapsForIndexerNodes(indexerNode)

			var tracker uint64
			for defnID := 0; defnID < len(indexerNode.Indexes); defnID++ {
				for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID)] {
					tracker++
					slotID := dealer.GetSlot(c.IndexDefnId(defnID), partnID, repmap, tracker, false)

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

		var dealer = getTestShardDealer(
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

		var replicaMaps = getReplicaMapsForIndexerNodes(indexerNode)
		var alternateShardIDs = make(map[string]bool)

		var tracker uint64
		for defnID := 0; defnID < len(indexerNode.Indexes); defnID++ {
			for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID)] {
				tracker++
				slotID := dealer.GetSlot(c.IndexDefnId(defnID), partnID, repmap, tracker, false)

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

func TestSingleNode_Pass2(t *testing.T) {
	// t.Parallel()

	t.Run("Basic-AllPrimary", func(t0 *testing.T) {
		t0.Parallel()

		var dealer = getTestShardDealer(
			minShardsPerNode,
			1,
			maxDiskUsagePerShard,
			shardCapacity,
			createNewAlternateShardIDGenerator(), nil)

		var cip = createIdxParam{count: shardCapacity / 10, isPrimary: true}
		var indexerNode = createDummyIndexerNode(t0.Name(), cip)

		var replicaMaps = getReplicaMapsForIndexerNodes(indexerNode)

		var slotIDs = make(map[c.AlternateShard_SlotId]bool)

		var tracker uint64
		for defnID := 0; defnID < len(indexerNode.Indexes); defnID++ {
			for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID)] {
				tracker++
				slotID := dealer.GetSlot(c.IndexDefnId(defnID), partnID, repmap, tracker, false)

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
		var dealer = getTestShardDealer(minShardsPerNode,
			1,
			maxDiskUsagePerShard,
			testShardCapacity,
			createNewAlternateShardIDGenerator(), nil)

		var cip = createIdxParam{count: testShardCapacity}
		var indexerNode = createDummyIndexerNode(t0.Name(), cip)

		var replicaMaps = getReplicaMapsForIndexerNodes(indexerNode)
		var slotIDs = make(map[c.AlternateShard_SlotId]bool)
		var alternateShardIDs = make(map[string]bool)

		var tracker uint64
		for defnID := 0; defnID < len(indexerNode.Indexes); defnID++ {
			for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID)] {
				tracker++
				var slotID = dealer.GetSlot(c.IndexDefnId(defnID), partnID, repmap, tracker, false)

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
		indexerNode.Indexes = append(indexerNode.Indexes, extraIndex)

		var replicaMap = make(map[int]map[*IndexerNode]*IndexUsage)
		replicaMap[extraIndex.Instance.ReplicaId] = make(map[*IndexerNode]*IndexUsage)
		replicaMap[extraIndex.Instance.ReplicaId][indexerNode] = extraIndex
		tracker++
		slotID := dealer.GetSlot(extraIndex.DefnId, extraIndex.PartnId, replicaMap, tracker, false)
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

		var dealer = getTestShardDealer(
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

		var replicaMaps = getReplicaMapsForIndexerNodes(indexerNode)
		var alternateShardIDs = make(map[string]bool)

		var tracker uint64
		for defnID := 0; defnID < len(indexerNode.Indexes); defnID++ {
			for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID)] {
				tracker++
				slotID := dealer.GetSlot(c.IndexDefnId(defnID), partnID, repmap, tracker, false)

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

		var dealer = getTestShardDealer(
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

		var replicaMaps = getReplicaMapsForIndexerNodes(indexerNode)
		var alternateShardIDs = make(map[string]bool)

		var tracker uint64
		for defnID := 0; defnID < len(indexerNode.Indexes); defnID++ {
			for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID)] {
				tracker++
				slotID := dealer.GetSlot(c.IndexDefnId(defnID), partnID, repmap, tracker, false)

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

func TestSingleNode_Pass3(t *testing.T) {
	// t.Parallel()

	var testShardCapacity uint64 = 10

	var cips = []createIdxParam{
		{count: testShardCapacity * 2},
		{count: testShardCapacity / 4, isVector: true},
		{count: testShardCapacity / 4, isBhive: true},
	}

	// maximise re-use case
	{
		var dealer = getTestShardDealer(
			minShardsPerNode,
			1,
			maxDiskUsagePerShard,
			testShardCapacity,
			createNewAlternateShardIDGenerator(), nil)
		var indexerNode = createDummyIndexerNode(t.Name(), cips...)
		var replicaMaps = getReplicaMapsForIndexerNodes(indexerNode)
		var slotIDs = make(map[c.AlternateShard_SlotId]bool)
		var alternateShardIDs = make(map[string]bool)

		var tracker uint64
		for defnID := len(indexerNode.Indexes); defnID >= 0; defnID-- {
			for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID)] {
				tracker++
				var slotID = dealer.GetSlot(c.IndexDefnId(defnID), partnID, repmap, tracker, false)

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
		indexerNode.Indexes = append(indexerNode.Indexes, extraIndex)

		var replicaMap = make(map[int]map[*IndexerNode]*IndexUsage)
		replicaMap[extraIndex.Instance.ReplicaId] = make(map[*IndexerNode]*IndexUsage)
		replicaMap[extraIndex.Instance.ReplicaId][indexerNode] = extraIndex
		tracker++
		slotID := dealer.GetSlot(extraIndex.DefnId, extraIndex.PartnId, replicaMap, tracker, false)
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
		var dealer = getTestShardDealer(
			minShardsPerNode,
			1,
			maxDiskUsagePerShard,
			testShardCapacity,
			createNewAlternateShardIDGenerator(), nil)

		var indexerNode = createDummyIndexerNode(t.Name(), cips...)
		var replicaMaps = getReplicaMapsForIndexerNodes(indexerNode)
		var slotIDs = make(map[c.AlternateShard_SlotId]bool)
		var alternateShardIDs = make(map[string]bool)

		var tracker uint64
		for defnID := 0; defnID < len(indexerNode.Indexes); defnID++ {
			for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID)] {
				tracker++
				var slotID = dealer.GetSlot(c.IndexDefnId(defnID), partnID, repmap, tracker, false)

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
		indexerNode.Indexes = append(indexerNode.Indexes, extraIndex)

		var replicaMap = make(map[int]map[*IndexerNode]*IndexUsage)
		replicaMap[extraIndex.Instance.ReplicaId] = make(map[*IndexerNode]*IndexUsage)
		replicaMap[extraIndex.Instance.ReplicaId][indexerNode] = extraIndex
		tracker++
		slotID := dealer.GetSlot(extraIndex.DefnId, extraIndex.PartnId, replicaMap, tracker, false)
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
	// t.Parallel()

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
	// t.Parallel()

	var dealer = getTestShardDealer(
		minPartitionsPerShard, minPartitionsPerShard, maxDiskUsagePerShard,
		shardCapacity,
		createNewAlternateShardIDGenerator(),
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
	err = dealer.RecordIndexUsage(moiIndex, &IndexerNode{}, true)
	if err == nil {
		t.Fatalf(
			"expected to fail in recording MoI index due to invalid category but it got recorded",
		)
	}

	// this fails on invalid shard category
	moiIndex.AlternateShardIds = []string{""}
	err = dealer.RecordIndexUsage(moiIndex, &IndexerNode{}, false)
	if err == nil {
		t.Fatalf(
			"expected to fail in recording MoI index due to invalid category but it got recorded",
		)
	}

	var plasmaCip = createIdxParam{}
	var plasmaIndex = createDummyIndexUsage(plasmaCip)
	err = dealer.RecordIndexUsage(plasmaIndex, &IndexerNode{}, false)
	if err != nil {
		t.Fatalf(
			"expected to not fail record index usage if index has nil shards but it failed with %v",
			err,
		)
	}

	// this fails on invalid alternate shard IDs
	plasmaIndex.InitialAlternateShardIds = []string{""}
	err = dealer.RecordIndexUsage(plasmaIndex, &IndexerNode{}, true)
	if err == nil {
		t.Fatalf(
			"expected to fail record index usage as index has invalid shards but it did not fail",
		)
	}

	// this fails on invalid alternate shard IDs
	plasmaIndex.AlternateShardIds = []string{""}
	err = dealer.RecordIndexUsage(plasmaIndex, &IndexerNode{}, false)
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
	err = dealer.RecordIndexUsage(plasmaIndex, &IndexerNode{}, false)
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
	var slotsToNodeMap = make(map[asSlotID]map[asReplicaID]nodeUUID)
	// <defnId, partnId> to slotID
	var partnSlots = make(map[c.IndexDefnId]map[c.PartitionId]asSlotID)

	// per node pic of which shard pair belongs to which node
	var nodeToSlotMap = make(map[nodeUUID]map[asSlotID]asReplicaID)
	var nodeToShardCountMap = make(map[nodeUUID]uint64)

	var catPerSlots = make(map[asSlotID]ShardCategory)

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
			if catPerSlots == nil {
				catPerSlots = make(map[asSlotID]ShardCategory)
			}
			if existsCat, exists := catPerSlots[alternateID.GetSlotId()]; exists &&
				existsCat != shardCategory {
				return fmt.Errorf("slot %v already belongs to category %v. cannot goto category %v",
					alternateID.GetSlotId(), existsCat, shardCategory)
			}
			slotsPerCategory[shardCategory][alternateID.GetSlotId()] = true

			if partnSlots[partn.DefnId] == nil {
				partnSlots[partn.DefnId] = make(map[c.PartitionId]asSlotID)
			}
			if existingSlot, exists := partnSlots[partn.DefnId][partn.PartnId]; exists &&
				existingSlot != alternateID.GetSlotId() {
				return fmt.Errorf("index (defn %v, partn %v, replicaID %v) going to multiple slots %v and %v. invalid scenario",
					partn.DefnId, partn.PartnId, partn.Instance.ReplicaId, existingSlot, alternateID.GetSlotId())
			}
			partnSlots[partn.DefnId][partn.PartnId] = alternateID.GetSlotId()

			if nodeToSlotMap[indexer.NodeUUID] == nil {
				nodeToSlotMap[indexer.NodeUUID] = make(map[asSlotID]asReplicaID)
			}
			if existingReplicaOnNode, exists := nodeToSlotMap[indexer.NodeUUID][alternateID.GetSlotId()]; exists &&
				existingReplicaOnNode != alternateID.GetReplicaId() {
				return fmt.Errorf("slot %v with replica %v is already present on node %v. %v replica cannot go there",
					alternateID.GetSlotId(), existingReplicaOnNode, indexer.NodeUUID, alternateID.GetReplicaId())
			}
			nodeToSlotMap[indexer.NodeUUID][alternateID.GetSlotId()] = alternateID.GetReplicaId()

			var slotID = alternateID.GetSlotId()
			var replicaID = alternateID.GetReplicaId()
			var groupID = alternateID.GetGroupId()

			if slotsMap[slotID] == nil {
				slotsMap[slotID] = make(map[asReplicaID]map[asGroupID]*pseudoShardContainer)
			}
			if slotsToNodeMap[slotID] == nil {
				slotsToNodeMap[slotID] = make(map[asReplicaID]nodeUUID)
			}
			if slotsMap[slotID][replicaID] == nil {
				slotsMap[slotID][replicaID] = make(map[asGroupID]*pseudoShardContainer)
			}
			var alternateIDReplicaID = alternateID.GetReplicaId()
			var indexDefnReplicaID = partn.Instance.ReplicaId
			if int(alternateIDReplicaID) != indexDefnReplicaID {
				return fmt.Errorf("alternateID replicaID %v != indexDefn replicaID %v for index %v",
					alternateIDReplicaID, indexDefnReplicaID, partn)
			}
			if slotOnNode, exists := slotsToNodeMap[slotID][replicaID]; exists && slotOnNode != indexer.NodeUUID {
				return fmt.Errorf("slot %v with replica %v is already present on node %v. cannot place it on node %v",
					slotID, replicaID, slotOnNode, indexer.NodeUUID)
			}
			slotsToNodeMap[slotID][replicaID] = indexer.NodeUUID

			var newShardCount = 0
			if slotsMap[slotID][replicaID][groupID] == nil {
				slotsMap[slotID][replicaID][groupID] = newPseudoShardContainer()
				newShardCount++
			}
			if partn.IsPrimary && len(alternateShardIDs) > 1 {
				return fmt.Errorf("primary index %v has alternateShardIDs %v. invalid scenario",
					partn, alternateShardIDs)
			} else if !partn.IsPrimary && len(alternateShardIDs) < 2 {
				return fmt.Errorf("non-primary index %v has alternateShardIDs %v. invalid scenario",
					partn, alternateShardIDs)
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

	// maps slotToNodeMap and nodeToSlotMap are inverse of each other. validate that
	for slotID, replicaMap := range slotsToNodeMap {
		for replicaID, nodeID := range replicaMap {
			if existingReplicaID, exists := nodeToSlotMap[nodeID][slotID]; !exists ||
				existingReplicaID != replicaID {
				return fmt.Errorf("slot %v with replica %v is not present on node %v. nodeToSlotMap is not inverse of slotsToNodeMap",
					slotID, replicaID, nodeID)
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

	if !reflect.DeepEqual(slotsToNodeMap, sd.slotsToNodeMap) {
		return fmt.Errorf(
			"cluster slotsToNodeMap does not match shard dealer - ([l: %v] %v != [l:%v] %v)",
			len(slotsToNodeMap),
			slotsToNodeMap,
			len(sd.slotsToNodeMap),
			sd.slotsToNodeMap,
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

func TestMultNode_NoReplicas(t *testing.T) {
	// t.Parallel()

	var dealer = getTestShardDealer(
		minShardsPerNode,
		minPartitionsPerShard,
		maxDiskUsagePerShard,
		shardCapacity,
		createNewAlternateShardIDGenerator(),
		nil,
	)

	numNodes := rand.Intn(int(minShardsPerNode))

	var cips = []createIdxParam{
		{count: minShardsPerNode, isPrimary: true},
		{count: shardCapacity},
		{count: shardCapacity, isVector: true},
		{count: shardCapacity, isBhive: true},
	}

	var cluster = createDummyIndexerNodes(numNodes, cips...)

	var slotIDs = make(map[c.AlternateShard_SlotId]bool)

	var replicaMaps = getReplicaMapsForIndexerNodes(cluster...)
	for defnID, repMaps := range replicaMaps {
		for partnID, repmap := range repMaps {
			slotID := dealer.GetSlot(defnID, partnID, repmap, 0, false)

			if slotID == 0 {
				t.Fatalf("%v failed to get slot id for replicaMap %v in 0th pass",
					t.Name(), repmap)
			}

			slotIDs[slotID] = true
		}
	}

	err := validateShardDealerInternals(dealer, cluster)
	assert.NoError(t, err, "internal shard dealer validation failed for basic test")
}

func TestMultiNode_NegTestSameIndexOnAllNodes(t *testing.T) {
	// t.Parallel()

	var dealer = getTestShardDealer(minShardsPerNode,
		minPartitionsPerShard,
		maxDiskUsagePerShard,
		shardCapacity,
		createNewAlternateShardIDGenerator(), nil)

	var index = createDummyIndexUsage(createIdxParam{defnid: 1})
	var cluster = createDummyIndexerNodes(3)

	for _, indexerNode := range cluster {
		var idx = index.clone()
		indexerNode.Indexes = append(indexerNode.Indexes, idx)
	}

	var replicaMaps = getReplicaMapsForIndexerNodes(cluster...)
	for defnID, repMaps := range replicaMaps {
		for partnID, repmap := range repMaps {
			slotID := dealer.GetSlot(defnID, partnID, repmap, 0, false)
			assert.Zero(
				t,
				slotID,
				"expected slot assigment to fail as same index-replica is going to multiple nodes but we have a valid slot",
			)
		}
	}
}

func TestMultiNode_NegTestSameDefnMultiReplicaSameNode(t *testing.T) {
	// t.Parallel()

	var dealer = getTestShardDealer(
		minShardsPerNode,
		minPartitionsPerShard,
		maxDiskUsagePerShard,
		shardCapacity,
		createNewAlternateShardIDGenerator(),
		nil,
	)

	var indexerNode = createDummyIndexerNode("0")

	// Manually add replicas to the same indexer node
	var cip = createIdxParam{defnid: 1, numReplicas: 2}
	var indexes = createDummyReplicaIndexUsages(cip)
	indexerNode.Indexes = append(indexerNode.Indexes, indexes...)

	var replicaMaps = getReplicaMapsForIndexerNodes(indexerNode)
	for defnID, repMaps := range replicaMaps {
		for partnID, repmap := range repMaps {
			slotID := dealer.GetSlot(defnID, partnID, repmap, 0, false)
			assert.Zero(
				t,
				slotID,
				"expected slot assignment to fail as same index definition with multiple replicas is on the same node but we have a valid slot",
			)
		}
	}
}

// TestMultiNode_Pass0 tests the shard dealer for multi node setup with replicated indexes for
// only Pass 0 cases
func TestMultiNode_Pass0(t *testing.T) {
	// t.Parallel()

	t.Run("BasicAllPrimary", func(t0 *testing.T) {
		t0.Parallel()

		var dealer = getTestShardDealer(
			minShardsPerNode,
			minPartitionsPerShard,
			maxDiskUsagePerShard,
			shardCapacity,
			createNewAlternateShardIDGenerator(),
			nil,
		)

		var cips = []createIdxParam{
			{count: minShardsPerNode, isPrimary: true, numReplicas: int(rand.Intn(6))},
		}

		var cluster = createDummyIndexerNodes(0, cips...)

		var slotIDs = make(map[c.AlternateShard_SlotId]bool)

		var tracker uint64
		var replicaMaps = getReplicaMapsForIndexerNodes(cluster...)
		for defnID, repMaps := range replicaMaps {
			for partnID, repmap := range repMaps {
				tracker++
				slotID := dealer.GetSlot(defnID, partnID, repmap, tracker, false)

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

		for _, indexerNode := range cluster {
			if dealer.nodeToShardCountMap[indexerNode.NodeUUID] != minShardsPerNode {
				t0.Fatalf(
					"%v shard dealer book keeping mismatch - node slot count is %v but should have been only %v",
					t0.Name(),
					dealer.nodeToShardCountMap[indexerNode.NodeUUID],
					minShardsPerNode,
				)
			}
		}

		for _, repMap := range dealer.slotsMap {
			if len(repMap) != len(cluster) {
				t0.Fatalf(
					"%v shard dealer book keeping mismatch - replica map length is %v (%v) but should have been only %v",
					t0.Name(),
					len(repMap), repMap,
					len(cluster),
				)
			}
		}

		assert.NoError(
			t0,
			validateShardDealerInternals(dealer, cluster),
			"internal shard dealer validation failed for basic test",
		)
	})

	t.Run("PrimaryAndSecondary", func(t0 *testing.T) {
		t0.Parallel()

		var dealer = getTestShardDealer(
			minShardsPerNode,
			minPartitionsPerShard,
			maxDiskUsagePerShard,
			shardCapacity,
			createNewAlternateShardIDGenerator(),
			nil,
		)

		var highReplicaCount int
		c.NewRetryHelper(10, 0, 1, func(_ int, _ error) error {
			highReplicaCount = rand.Intn(10)
			if highReplicaCount == 0 {
				return fmt.Errorf("highReplicaCount is 0")
			}
			return nil
		}).Run()

		var cips = []createIdxParam{
			{count: minShardsPerNode / 4, isPrimary: true, numReplicas: highReplicaCount},
			{count: minShardsPerNode/2 - (minShardsPerNode / 4), numReplicas: rand.Intn(highReplicaCount)},
		}

		var cluster = createDummyIndexerNodes(0, cips...)

		var slotIDs = make(map[c.AlternateShard_SlotId]bool)
		var replicaMaps = getReplicaMapsForIndexerNodes(cluster...)

		var tracker uint64
		for defnID, repMaps := range replicaMaps {
			for partnID, repmap := range repMaps {
				tracker++
				slotID := dealer.GetSlot(defnID, partnID, repmap, tracker, false)

				if slotID == 0 {
					t0.Fatalf("%v failed to get slot id for replicaMap %v in 0th pass",
						t0.Name(), repmap)
				}

				slotIDs[slotID] = true
			}
		}

		var highSlotCount = int(minShardsPerNode / 2)
		var lowSlotCount = int((minShardsPerNode - 1) / 2)

		if len(slotIDs) != highSlotCount && len(slotIDs) != lowSlotCount {
			t0.Fatalf("%v slots created are %v but should have been only %v/%v slots",
				t0.Name(), slotIDs,
				highSlotCount,
				lowSlotCount,
			)
		}
		if len(dealer.partnSlots) != highSlotCount &&
			len(dealer.partnSlots) != lowSlotCount {
			t0.Fatalf(
				"%v shard dealer book keeping mismatch - index defns recorded are %v but should have been only %v/%v defns",
				t0.Name(),
				dealer.partnSlots,
				highSlotCount,
				lowSlotCount,
			)
		}
		if len(dealer.slotsMap) != highSlotCount &&
			len(dealer.slotsMap) != lowSlotCount {
			t0.Fatalf(
				"%v shard dealer book keeping mismatch - slots created are %v but should have been only %v/%v slots",
				t0.Name(),
				dealer.slotsMap,
				highSlotCount,
				lowSlotCount,
			)
		}
		for _, indexerNode := range cluster {
			if dealer.nodeToShardCountMap[indexerNode.NodeUUID] > minShardsPerNode {
				t0.Fatalf(
					"%v shard dealer book keeping mismatch - node slot count is %v but should have been < %v",
					t0.Name(),
					dealer.nodeToShardCountMap[indexerNode.NodeUUID],
					minShardsPerNode,
				)
			}
		}

		assert.NoError(
			t0,
			validateShardDealerInternals(dealer, cluster),
			"internal shard dealer validation failed for primary and secondary mix tests",
		)
	})

	t.Run("PartitionedIndexes", func(t0 *testing.T) {
		t0.Parallel()

		var dealer = getTestShardDealer(
			minShardsPerNode,
			minPartitionsPerShard,
			maxDiskUsagePerShard,
			shardCapacity,
			createNewAlternateShardIDGenerator(),
			nil,
		)

		var highReplicaCount int
		c.NewRetryHelper(10, 0, 1, func(_ int, _ error) error {
			highReplicaCount = rand.Intn(10)
			if highReplicaCount == 0 {
				return fmt.Errorf("highReplicaCount is 0")
			}
			return nil
		}).Run()

		var cips = []createIdxParam{
			{count: 1, isPrimary: true, numReplicas: highReplicaCount, numPartns: int(minShardsPerNode / 4)},
			{count: 1, numReplicas: rand.Intn(highReplicaCount), numPartns: int(minShardsPerNode/2 - minShardsPerNode/4)},
		}

		var cluster = createDummyIndexerNodes(0, cips...)

		var slotIDs = make(map[c.AlternateShard_SlotId]bool)
		var replicaMaps = getReplicaMapsForIndexerNodes(cluster...)
		var tracker uint64
		for defnID, repMaps := range replicaMaps {
			for partnID, repmap := range repMaps {
				tracker++
				slotID := dealer.GetSlot(defnID, partnID, repmap, tracker, false)

				if slotID == 0 {
					t0.Fatalf("%v failed to get slot id for replicaMap %v in 0th pass",
						t0.Name(), repmap)
				}

				slotIDs[slotID] = true
			}
		}

		var highSlotCount = int(minShardsPerNode / 2)
		var lowSlotCount = int((minShardsPerNode - 1) / 2)

		if len(slotIDs) != highSlotCount && len(slotIDs) != lowSlotCount {
			t0.Fatalf("%v slots created are %v but should have been only %v/%v slots",
				t0.Name(), slotIDs,
				highSlotCount,
				lowSlotCount,
			)
		}
		if len(dealer.partnSlots) != 2 {
			t0.Fatalf(
				"%v shard dealer book keeping mismatch - index defns recorded are %v but should have been only %v defns",
				t0.Name(),
				dealer.partnSlots,
				2,
			)
		}
		if len(dealer.slotsMap) != highSlotCount &&
			len(dealer.slotsMap) != lowSlotCount {
			t0.Fatalf(
				"%v shard dealer book keeping mismatch - slots created are %v but should have been only %v/%v slots",
				t0.Name(),
				dealer.slotsMap,
				highSlotCount,
				lowSlotCount,
			)
		}
		for _, indexerNode := range cluster {
			if dealer.nodeToShardCountMap[indexerNode.NodeUUID] > minShardsPerNode {
				t0.Fatalf(
					"%v shard dealer book keeping mismatch - node slot count is %v but should have been < %v",
					t0.Name(),
					dealer.nodeToShardCountMap[indexerNode.NodeUUID],
					minShardsPerNode,
				)
			}
		}

		assert.NoError(
			t0,
			validateShardDealerInternals(dealer, cluster),
			"internal shard dealer validation failed for primary and secondary mix tests",
		)
	})
}

func genMoveInstanceCb(cluster []*IndexerNode) moveFuncCb {
	return func(fromNodeUUID, toNodeUUID nodeUUID, index *IndexUsage) (map[*IndexerNode]*IndexUsage, error) {
		if index == nil {
			return nil, fmt.Errorf("invalid move request")
		}

		var fromNode, toNode *IndexerNode

		for _, indexerNode := range cluster {
			switch indexerNode.NodeUUID {
			case fromNodeUUID:
				fromNode = indexerNode
			case toNodeUUID:
				toNode = indexerNode
			}
		}

		if fromNode == nil || toNode == nil {
			return nil, fmt.Errorf("invalid move request. source node/dest node nil")
		}

		for i, idx := range fromNode.Indexes {
			if idx.InstId == index.InstId && idx.PartnId == index.PartnId {
				idx.destNode = toNode
				fromNode.Indexes = append(fromNode.Indexes[:i], fromNode.Indexes[i+1:]...)
				toNode.Indexes = append(toNode.Indexes, idx)
				return map[*IndexerNode]*IndexUsage{toNode: index}, nil
			}
		}

		return nil, fmt.Errorf("index not found on source")
	}
}

func TestMultiNode_Pass1(t *testing.T) {
	// t.Parallel()

	t.Run("BasicAllPrimary", func(t0 *testing.T) {
		t0.Parallel()

		var cips = []createIdxParam{
			{count: minPartitionsPerShard * minShardsPerNode, isPrimary: true, numReplicas: rand.Intn(6)},
		}

		var cluster = createDummyIndexerNodes(0, cips...)

		var dealer = getTestShardDealer(
			minShardsPerNode,
			minPartitionsPerShard,
			maxDiskUsagePerShard,
			shardCapacity,
			createNewAlternateShardIDGenerator(),
			genMoveInstanceCb(cluster),
		)

		var slotIDs = make(map[c.AlternateShard_SlotId]bool)

		var replicaMaps = getReplicaMapsForIndexerNodes(cluster...)
		var tracker uint64
		for defnID, repMaps := range replicaMaps {
			for partnID, repmap := range repMaps {
				tracker++
				slotID := dealer.GetSlot(defnID, partnID, repmap, tracker, false)

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

		if len(dealer.partnSlots) != int(minShardsPerNode*minPartitionsPerShard) {
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

		for _, indexerNode := range cluster {
			if dealer.nodeToShardCountMap[indexerNode.NodeUUID] != minShardsPerNode {
				t0.Fatalf(
					"%v shard dealer book keeping mismatch - node slot count is %v but should have been only %v",
					t0.Name(),
					dealer.nodeToShardCountMap[indexerNode.NodeUUID],
					minShardsPerNode,
				)
			}
		}

		assert.NoError(
			t0,
			validateShardDealerInternals(dealer, cluster),
			"internal shard dealer validation failed for basic test",
		)
	})

	t.Run("PrimaryAndSecondary", func(t0 *testing.T) {
		t0.Parallel()

		var highReplicaCount = rand.Intn(10)
		var numPrimIdxs = minShardsPerNode / 4
		var cips = []createIdxParam{
			{count: numPrimIdxs, isPrimary: true, numReplicas: highReplicaCount},
			{count: minPartitionsPerShard*minShardsPerNode/2 - numPrimIdxs, numReplicas: highReplicaCount},
		}

		var cluster = createDummyIndexerNodes(0, cips...)

		var dealer = getTestShardDealer(
			minShardsPerNode,
			minPartitionsPerShard,
			maxDiskUsagePerShard,
			shardCapacity,
			createNewAlternateShardIDGenerator(),
			genMoveInstanceCb(cluster),
		)

		var slotIDs = make(map[c.AlternateShard_SlotId]bool)
		var replicaMaps = getReplicaMapsForIndexerNodes(cluster...)
		var tracker uint64
		for defnID := 0; defnID < len(replicaMaps); defnID++ {
			for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID+1)] {
				tracker++
				slotID := dealer.GetSlot(c.IndexDefnId(defnID+1), partnID, repmap, tracker, false)

				if slotID == 0 {
					t0.Fatalf("%v failed to get slot id for replicaMap %v in 0th pass",
						t0.Name(), repmap)
				}

				slotIDs[slotID] = true
			}
		}

		var highSlotCount = int(minShardsPerNode + 1)
		var lowSlotCount = int(minShardsPerNode / 2)

		if len(slotIDs) > highSlotCount || len(slotIDs) < lowSlotCount {
			t0.Fatalf("%v slots created are %v but should have been only %v/%v slots",
				t0.Name(), slotIDs,
				highSlotCount,
				lowSlotCount,
			)
		}
		if len(dealer.partnSlots) != len(replicaMaps) {
			t0.Fatalf(
				"%v shard dealer book keeping mismatch - index defns recorded are %v but should have been %v defns",
				t0.Name(),
				dealer.partnSlots,
				len(replicaMaps),
			)
		}

		assert.NoError(
			t0,
			validateShardDealerInternals(dealer, cluster),
			"internal shard dealer validation failed for primary and secondary mix tests",
		)

	})

	t.Run("PartitionedIndexes", func(t0 *testing.T) {
		t0.Parallel()

		var highReplicaCount = rand.Intn(10)
		var numPrimIdxs = minShardsPerNode / 4
		var cips = []createIdxParam{
			{count: 1, isPrimary: true, numReplicas: highReplicaCount, numPartns: int(numPrimIdxs)},
			{count: 1, numReplicas: highReplicaCount, numPartns: int(minPartitionsPerShard*minShardsPerNode/2 - numPrimIdxs)},
		}

		var cluster = createDummyIndexerNodes(0, cips...)

		var dealer = getTestShardDealer(
			minShardsPerNode,
			minPartitionsPerShard,
			maxDiskUsagePerShard,
			shardCapacity,
			createNewAlternateShardIDGenerator(),
			genMoveInstanceCb(cluster),
		)

		var slotIDs = make(map[c.AlternateShard_SlotId]bool)
		var replicaMaps = getReplicaMapsForIndexerNodes(cluster...)
		var tracker uint64
		for defnID := 0; defnID < len(replicaMaps); defnID++ {
			for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID+1)] {
				tracker++
				slotID := dealer.GetSlot(c.IndexDefnId(defnID+1), partnID, repmap, tracker, false)

				if slotID == 0 {
					t0.Fatalf("%v failed to get slot id for replicaMap %v in 0th pass",
						t0.Name(), repmap)
				}

				slotIDs[slotID] = true
			}
		}

		var highSlotCount = int(minShardsPerNode + 1)
		var lowSlotCount = int(minShardsPerNode / 2)

		if len(slotIDs) > highSlotCount || len(slotIDs) < lowSlotCount {
			t0.Fatalf("%v slots created are %v but should have been only %v/%v slots",
				t0.Name(), slotIDs,
				highSlotCount,
				lowSlotCount,
			)
		}
		if len(dealer.partnSlots) != len(replicaMaps) {
			t0.Fatalf(
				"%v shard dealer book keeping mismatch - index defns recorded are %v but should have been only %v defns",
				t0.Name(),
				dealer.partnSlots,
				2,
			)
		}

		assert.NoError(
			t0,
			validateShardDealerInternals(dealer, cluster),
			"internal shard dealer validation failed for primary and secondary mix tests",
		)
	})
}

func TestMultiNode_Pass2(t *testing.T) {
	// t.Parallel()

	t.Run("BasicAllPrimary", func(t0 *testing.T) {
		t0.Parallel()

		var cips = []createIdxParam{
			{count: minPartitionsPerShard*minShardsPerNode + 1, isPrimary: true, numReplicas: rand.Intn(6)},
		}
		var cluster = createDummyIndexerNodes(0, cips...)
		var dealer = getTestShardDealer(
			minShardsPerNode,
			minPartitionsPerShard,
			maxDiskUsagePerShard,
			shardCapacity,
			createNewAlternateShardIDGenerator(),
			genMoveInstanceCb(cluster),
		)

		var slotIDs = make(map[c.AlternateShard_SlotId]bool)
		var replicaMaps = getReplicaMapsForIndexerNodes(cluster...)
		var tracker uint64
		for defnID := 0; defnID < len(replicaMaps); defnID++ {
			for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID+1)] {
				tracker++
				slotID := dealer.GetSlot(c.IndexDefnId(defnID+1), partnID, repmap, tracker, false)

				if slotID == 0 {
					t0.Fatalf("%v failed to get slot id for replicaMap %v in 0th pass",
						t0.Name(), repmap)
				}

				slotIDs[slotID] = true
			}
		}

		if len(slotIDs) != int(minShardsPerNode)+1 {
			t0.Fatalf("%v slots created are %v but should have been only %v slots",
				t0.Name(), slotIDs, minShardsPerNode)
		}

		assert.NoError(
			t0,
			validateShardDealerInternals(dealer, cluster),
			"internal shard dealer validation failed for basic test",
		)
	})

	t.Run("MixCategoryIndexes", func(t0 *testing.T) {
		t0.Parallel()

		var numReps = rand.Intn(10)

		var ciplist = []createIdxParam{
			{count: minShardsPerNode/2 + 1, isPrimary: true, numReplicas: numReps},
			{count: minShardsPerNode / 4, numReplicas: numReps},
			{count: 1, numReplicas: numReps, isVector: true},
			{count: 1, numReplicas: numReps, isBhive: true},
		}

		var cluster = createDummyIndexerNodes(0, ciplist...)
		var dealer = getTestShardDealer(
			minShardsPerNode,
			minPartitionsPerShard,
			maxDiskUsagePerShard,
			shardCapacity,
			createNewAlternateShardIDGenerator(),
			genMoveInstanceCb(cluster),
		)

		var slotIDs = make(map[c.AlternateShard_SlotId]bool)
		var replicaMaps = getReplicaMapsForIndexerNodes(cluster...)
		var tracker uint64
		for defnID := 0; defnID < len(replicaMaps); defnID++ {
			for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID+1)] {
				tracker++
				slotID := dealer.GetSlot(c.IndexDefnId(defnID+1), partnID, repmap, tracker, false)

				if slotID == 0 {
					t0.Fatalf("%v failed to get slot id for replicaMap %v in 0th pass",
						t0.Name(), repmap)
				}

				slotIDs[slotID] = true
			}
		}

		if len(slotIDs) != int(minShardsPerNode)+1 {
			t0.Fatalf("%v slots created are %v but should have been only %v slots",
				t0.Name(), slotIDs, minShardsPerNode+1)
		}
		if len(dealer.slotsPerCategory) != 3 {
			t0.Fatalf(
				"%v shard dealer book keeping mismatch - slots per category are %v but should have been only 3",
				t0.Name(),
				dealer.slotsPerCategory,
			)
		}

		assert.NoError(
			t0,
			validateShardDealerInternals(dealer, cluster),
			"internal shard dealer validation failed for basic test",
		)
	})
}

func TestMultiNode_Pass3(t *testing.T) {
	// t.Parallel()

	var testShardCapacity uint64 = 200

	var numReps = rand.Intn(5)
	var idxCreations = testShardCapacity * minPartitionsPerShard * uint64(rand.Intn(5))

	var ciplist = []createIdxParam{
		{count: 1, isPrimary: true, numReplicas: numReps},
		{count: 1, numReplicas: numReps},
		{count: 1, isVector: true, numReplicas: numReps},
		{count: 1, isBhive: true, numReplicas: numReps},
		{count: idxCreations, isPrimary: true, numReplicas: numReps},
		{count: idxCreations, numReplicas: numReps},
		{count: idxCreations, isVector: true, numReplicas: numReps},
		{count: idxCreations, isBhive: true, numReplicas: numReps},
	}

	var cluster = createDummyIndexerNodes(0, ciplist...)

	var dealer = getTestShardDealer(
		minShardsPerNode,
		minPartitionsPerShard,
		maxDiskUsagePerShard,
		testShardCapacity,
		createNewAlternateShardIDGenerator(),
		genMoveInstanceCb(cluster),
	)

	var slotIDs = make(map[c.AlternateShard_SlotId]bool)

	var replicaMaps = getReplicaMapsForIndexerNodes(cluster...)
	var tracker uint64
	for defnID := 0; defnID < len(replicaMaps); defnID++ {
		for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID+1)] {
			tracker++
			slotID := dealer.GetSlot(c.IndexDefnId(defnID+1), partnID, repmap, tracker, false)

			if slotID == 0 {
				t.Fatalf("%v failed to get slot id for replicaMap %v in 0th pass",
					t.Name(), repmap)
			}

			slotIDs[slotID] = true
		}
	}

	if len(slotIDs) > int(testShardCapacity) {
		t.Fatalf("%v slots created are %v but should have been only %v slots",
			t.Name(), slotIDs, shardCapacity)
	}

	assert.NoError(
		t,
		validateShardDealerInternals(dealer, cluster),
		"internal shard dealer validation failed for basic test",
	)
}

func TestMultiNode_RandomLayoutTests(t *testing.T) {
	// t.Parallel()

	var testShardCapacity uint64 = 200

	var numReps, numIndexesPerCat, numPartitions int

	c.NewRetryHelper(10, 0, 1, func(_ int, _ error) error {
		if numReps == 0 {
			numReps = rand.Intn(7)
		}
		if numIndexesPerCat == 0 {
			numIndexesPerCat = rand.Intn(10)
		}
		if numPartitions == 0 {
			numPartitions = rand.Intn(int(minPartitionsPerShard) * 3)
		}

		if numReps == 0 || numIndexesPerCat == 0 || numPartitions == 0 {
			return fmt.Errorf("invalid random values")
		}
		return nil
	}).Run()

	var numPartitionsCreated = 0

	var ciplist = make([]createIdxParam, 0, 4*numIndexesPerCat)

	for numPartitionsCreated < int(testShardCapacity)*numReps {
		count := uint64(rand.Intn(numIndexesPerCat))
		replicas := rand.Intn(numReps)
		partitions := rand.Intn(numPartitions)

		var cip = createIdxParam{
			count:       count,
			numReplicas: replicas,
			numPartns:   partitions,
		}

		var coinToss = rand.Intn(2) == 0
		if coinToss {
			insertCip := cip.clone()
			insertCip.isPrimary = true
			ciplist = append(ciplist, *insertCip)
			numPartitionsCreated += int(count) * (replicas + 1) * partitions
		}

		coinToss = rand.Intn(2) == 0
		if coinToss {
			insertCip := cip.clone()
			ciplist = append(ciplist, *insertCip)
			numPartitionsCreated += int(count) * (replicas + 1) * partitions
		}

		coinToss = rand.Intn(2) == 0
		if coinToss {
			insertCip := cip.clone()
			insertCip.isVector = true
			ciplist = append(ciplist, *insertCip)
			numPartitionsCreated += int(count) * (replicas + 1) * partitions
		}

		coinToss = rand.Intn(2) == 0
		if coinToss {
			insertCip := cip.clone()
			insertCip.isBhive = true
			ciplist = append(ciplist, *insertCip)
			numPartitionsCreated += int(count) * (replicas + 1) * partitions
		}
	}

	var cluster = createDummyIndexerNodes(numReps, ciplist...)

	var dealer = getTestShardDealer(
		minShardsPerNode,
		minPartitionsPerShard,
		maxDiskUsagePerShard,
		testShardCapacity,
		createNewAlternateShardIDGenerator(),
		genMoveInstanceCb(cluster),
	)

	var slotIDs = make(map[c.AlternateShard_SlotId]bool)

	var replicaMaps = getReplicaMapsForIndexerNodes(cluster...)
	for defnID := 0; defnID < len(replicaMaps); defnID++ {
		for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID+1)] {
			slotID := dealer.GetSlot(c.IndexDefnId(defnID+1), partnID, repmap, 0, false)

			if slotID == 0 {
				t.Fatalf("%v failed to get slot id for replicaMap %v in 0th pass",
					t.Name(), repmap)
			}

			slotIDs[slotID] = true
		}
	}

	assert.NoErrorf(
		t,
		validateShardDealerInternals(dealer, cluster),
		"internal shard dealer validation failed for random cluster layout test. cluster - %v.\nshard dealer - %v",
		clusterStr(cluster...),
		dealer,
	)
}

func TestMultiNode_HighReuseTests(t *testing.T) {
	// t.Parallel()

	var testShardCapacity uint64 = 10

	var numReps = 2
	var numIndexesPerCat = testShardCapacity * 2
	var numPartitions = 3

	var ciplist = []createIdxParam{
		{count: 1, isPrimary: true, numReplicas: numReps, numPartns: numPartitions},
		{count: 1, numReplicas: numReps, numPartns: numPartitions},
		{count: 1, isVector: true, numReplicas: numReps, numPartns: numPartitions},
		{count: 1, isBhive: true, numReplicas: numReps, numPartns: numPartitions},
		{count: uint64(numIndexesPerCat), isPrimary: true, numReplicas: numReps, numPartns: numPartitions},
		{count: uint64(numIndexesPerCat), numReplicas: numReps, numPartns: numPartitions},
		{count: uint64(numIndexesPerCat), isVector: true, numReplicas: numReps, numPartns: numPartitions},
		{count: uint64(numIndexesPerCat), isBhive: true, numReplicas: numReps, numPartns: numPartitions},
	}

	var cluster = createDummyIndexerNodes(3, ciplist...)

	var dealer = getTestShardDealer(
		minShardsPerNode,
		1,
		maxDiskUsagePerShard,
		testShardCapacity,
		createNewAlternateShardIDGenerator(),
		genMoveInstanceCb(cluster),
	)

	var slotIDs = make(map[c.AlternateShard_SlotId]bool)

	var replicaMaps = getReplicaMapsForIndexerNodes(cluster...)
	var tracker uint64
	for defnID := 0; defnID < len(replicaMaps); defnID++ {
		for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID+1)] {
			tracker++
			slotID := dealer.GetSlot(c.IndexDefnId(defnID+1), partnID, repmap, tracker, false)

			if slotID == 0 {
				t.Fatalf("%v failed to get slot id for replicaMap %v in 0th pass",
					t.Name(), repmap)
			}

			slotIDs[slotID] = true
		}
	}

	if len(slotIDs) > int(testShardCapacity) {
		t.Fatalf("%v slots created are %v but should have been only %v slots",
			t.Name(), slotIDs, testShardCapacity)
	}

	assert.NoError(
		t,
		validateShardDealerInternals(dealer, cluster),
		"internal shard dealer validation failed for high reuse test",
	)
}

func clusterStr(cluster ...*IndexerNode) string {
	var sb strings.Builder
	for _, node := range cluster {
		sb.WriteString(fmt.Sprintf("* node %v (%v). Num Indexes - %v\n", node.NodeUUID, node.NodeId, len(node.Indexes)))
		for _, index := range node.Indexes {
			sb.WriteString(
				fmt.Sprintf("\t** defnID %v partn ID %v replicaID %v isPrimary %v isBhive %v isVector %v alternateShardIDs %v\n",
					index.DefnId, index.PartnId, index.Instance.ReplicaId, index.IsPrimary,
					index.Instance.Defn.IsBhive(), index.Instance.Defn.IsVectorIndex, index.AlternateShardIds,
				))
		}
	}
	return sb.String()
}

// TestMultiNode_UnevenDistribution tests the first pass case where we have one node above
// minShardsPerNode and the rest are below minShardsPerNode.
// there are 2 cases here, each node has 1 slot common and other is each pair of nodes has 1 slot common
// in both the cases, we are mainly targeting to test shard reuse logic and the logic to ensure
// that index-replicaID and slot-replicaID mapping is aligned
func TestMultiNode_UnevenDistribution(t *testing.T) {
	// t.Parallel()

	{
		// each node has 1 slot common
		var testShardCapacity uint64 = 10

		var testMinShardsPerNode = uint64(10)
		var testMinPartitionsPerShard = uint64(1)
		var testMaxDiskUsagePerShard = uint64(1000)

		var ciplist = []createIdxParam{
			{count: testMinShardsPerNode/2 - 1},
			{count: 1, numReplicas: 2},
		}

		var cluster = createDummyIndexerNodes(0, ciplist...)

		var dealer = getTestShardDealer(
			testMinShardsPerNode,
			testMinPartitionsPerShard,
			testMaxDiskUsagePerShard,
			testShardCapacity,
			createNewAlternateShardIDGenerator(),
			genMoveInstanceCb(cluster),
		)

		var slotIDs = make(map[c.AlternateShard_SlotId]bool)

		var replicaMaps = getReplicaMapsForIndexerNodes(cluster...)
		var tracker uint64
		for defnID := 0; defnID < len(replicaMaps); defnID++ {
			for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID+1)] {
				tracker++
				slotID := dealer.GetSlot(c.IndexDefnId(defnID+1), partnID, repmap, tracker, false)

				if slotID == 0 {
					t.Fatalf("%v failed to get slot id for replicaMap %v in 0th pass",
						t.Name(), repmap)
				}

				slotIDs[slotID] = true
			}
		}

		var defnID = len(replicaMaps) + 1
		replicas := createDummyReplicaIndexUsages(createIdxParam{count: 1, numReplicas: 2})
		for i, replica := range replicas {
			replica.DefnId = c.IndexDefnId(defnID)
			cluster[i].Indexes = append(cluster[i].Indexes, replica)
		}

		replicaMaps = getReplicaMapsForIndexerNodes(cluster...)

		for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID+1)] {
			slotID := dealer.GetSlot(c.IndexDefnId(defnID), partnID, repmap, 0, false)
			if slotID == 0 {
				t.Fatalf("%v failed to get slot id for replicaMap %v in 0th pass",
					t.Name(), repmap)
			}

			if _, exists := slotIDs[slotID]; !exists {
				t.Fatalf("%v new slot id %v used for index but we should have re-used slot", t.Name(), slotID)
			}
		}

		if len(slotIDs) > int(testMinShardsPerNode) {
			t.Fatalf("%v slots created are %v but should have been only %v slots",
				t.Name(), slotIDs, testMinShardsPerNode)
		}

		assert.NoError(
			t,
			validateShardDealerInternals(dealer, cluster),
			"internal shard dealer validation failed for uneven distribution test",
		)
	}

	{
		// each pair of nodes has 1 slot common
		var testShardCapacity uint64 = 10

		var testMinShardsPerNode = uint64(10)
		var testMinPartitionsPerShard = uint64(1)
		var testMaxDiskUsagePerShard = uint64(1000)

		var ciplist = []createIdxParam{
			{count: testMinShardsPerNode/2 - 1},
		}

		var node0 = createDummyIndexerNode("0", ciplist...)
		var node1 = createDummyIndexerNode("1")
		var node2 = createDummyIndexerNode("2")

		defnID := len(node0.Indexes) + 1
		var replica1s = createDummyReplicaIndexUsages(createIdxParam{count: 1, numReplicas: 1})
		var replica2s = createDummyReplicaIndexUsages(createIdxParam{count: 1, numReplicas: 1})

		node0.Indexes = append(node0.Indexes, replica1s[0])
		node1.Indexes = append(node1.Indexes, replica1s[1])
		replica1s[0].DefnId = c.IndexDefnId(defnID)
		replica1s[1].DefnId = c.IndexDefnId(defnID)

		defnID++
		node0.Indexes = append(node0.Indexes, replica2s[0])
		node2.Indexes = append(node2.Indexes, replica2s[1])
		replica2s[0].DefnId = c.IndexDefnId(defnID)
		replica2s[1].DefnId = c.IndexDefnId(defnID)

		var cluster = []*IndexerNode{node0, node1, node2}

		var dealer = getTestShardDealer(
			testMinShardsPerNode,
			testMinPartitionsPerShard,
			testMaxDiskUsagePerShard,
			testShardCapacity,
			createNewAlternateShardIDGenerator(),
			genMoveInstanceCb(cluster),
		)

		var slotIDs = make(map[c.AlternateShard_SlotId]bool)

		var replicaMaps = getReplicaMapsForIndexerNodes(cluster...)
		var tracker uint64
		for defnID := 0; defnID < len(replicaMaps); defnID++ {
			for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID+1)] {
				tracker++
				slotID := dealer.GetSlot(c.IndexDefnId(defnID+1), partnID, repmap, tracker, false)

				if slotID == 0 {
					t.Fatalf("%v failed to get slot id for replicaMap %v in 0th pass",
						t.Name(), repmap)
				}

				slotIDs[slotID] = true
			}
		}

		defnID = len(replicaMaps) + 1

		var replica3s = createDummyReplicaIndexUsages(createIdxParam{count: 1, numReplicas: 2})

		node0.Indexes = append(node0.Indexes, replica3s[0])
		node1.Indexes = append(node1.Indexes, replica3s[1])
		node2.Indexes = append(node2.Indexes, replica3s[2])
		replica3s[0].DefnId = c.IndexDefnId(defnID)
		replica3s[1].DefnId = c.IndexDefnId(defnID)
		replica3s[2].DefnId = c.IndexDefnId(defnID)

		replicaMaps = getReplicaMapsForIndexerNodes(cluster...)

		for partnID, repmap := range replicaMaps[c.IndexDefnId(defnID+1)] {
			slotID := dealer.GetSlot(c.IndexDefnId(defnID), partnID, repmap, 0, false)
			if slotID == 0 {
				t.Fatalf("%v failed to get slot id for replicaMap %v in 0th pass",
					t.Name(), repmap)
			}

			if _, exists := slotIDs[slotID]; !exists {
				t.Fatalf("%v new slot id %v used for index but we should have re-used slot", t.Name(), slotID)
			}
		}

		if len(slotIDs) > int(testMinShardsPerNode) {
			t.Fatalf("%v slots created are %v but should have been only %v slots",
				t.Name(), slotIDs, testMinShardsPerNode)
		}

		assert.NoError(
			t,
			validateShardDealerInternals(dealer, cluster),
			"internal shard dealer validation failed for uneven distribution test",
		)
	}
}

func TestMultiNode_MixedModeRebalance(t *testing.T) {
	// t.Parallel()

	var testShardCapacity uint64 = 10

	var testMinShardsPerNode = uint64(1)
	var testMinPartitionsPerShard = uint64(1)
	var testMaxDiskUsagePerShard = uint64(1000)

	var ciplist = []createIdxParam{
		{count: 6, numReplicas: 2},
	}

	var cluster = createDummyIndexerNodes(0, ciplist...)

	var dealer = getTestShardDealer(
		testMinShardsPerNode,
		testMinPartitionsPerShard,
		testMaxDiskUsagePerShard,
		testShardCapacity,
		createNewAlternateShardIDGenerator(),
		genMoveInstanceCb(cluster),
	)

	// shuffle initial nodes of all indexes except the first one
	for _, node := range cluster[1:] {
		for _, index := range node.Indexes {
			index.initialNode = cluster[rand.Intn(len(cluster)-1)+1]
		}
	}

	var node0ReplicaMap = getReplicaMapsForIndexerNodes(cluster[0])
	var recordedDefnID c.IndexDefnId = 0

	for defnID, repMap := range node0ReplicaMap {
		for partnID, repmap := range repMap {
			slotID := dealer.GetSlot(defnID, partnID, repmap, 0, false)
			if slotID == 0 {
				t.Fatalf("%v failed to get slot id for replicaMap %v in 0th pass", t.Name(), repmap)
			}
			break
		}
		recordedDefnID = defnID
		break
	}

	delete(node0ReplicaMap, recordedDefnID)

	var replicaMaps = getReplicaMapsForIndexerNodes(cluster...)

	for defnID, repMap := range replicaMaps {
		for partnID, repmap := range repMap {
			slotID := dealer.GetSlot(defnID, partnID, repmap, 0, false)
			if slotID == 0 {
				t.Fatalf("%v failed to get slot id for replicaMap %v", t.Name(), repmap)
			}
		}
	}

	assert.NoError(
		t,
		validateShardDealerInternals(dealer, cluster),
		"internal shard dealer validation failed for mixed mode rebalance test",
	)
}

func TestSingleNode_HardLimit(t *testing.T) {
	// t.Parallel()

	const (
		testShardCapacity         = uint64(10)
		testMinShardsPerNode      = uint64(1)
		testMinPartitionsPerShard = uint64(1)
		testMaxPartitionsPerSlot  = uint64(5)
	)

	var ciplist = []createIdxParam{
		{count: 3},                 // 6 shards (3 slots)
		{count: 1, isVector: true}, // 6 shards + 2 = 8 shards (4 slots)
		{count: 1, isBhive: true},  // 8 shards + 2 = 10 shards -> shard capacity (5 slots)
		{count: 4, numPartns: 3},   // 3 slots * (1+ 4 new indexes) = 15 partitions
	}

	var dealer = NewShardDealer(
		testMinShardsPerNode, testMinPartitionsPerShard,
		maxDiskUsagePerShard,
		testMaxPartitionsPerSlot,
		testShardCapacity,
		diskUsageThresholdRatio,
		createNewAlternateShardIDGenerator(), nil)

	var indexerNode = createDummyIndexerNode(t.Name(), ciplist...)

	var slotIDs = make(map[c.AlternateShard_SlotId]bool)

	var replicaMaps = getReplicaMapsForIndexerNodes(indexerNode)

	var tracker uint64
	for i := 0; i < len(indexerNode.Indexes); i++ {
		defnID := c.IndexDefnId(i)
		repMaps := replicaMaps[defnID]
		for partnID, repmap := range repMaps {
			tracker++
			slotID := dealer.GetSlot(defnID, partnID, repmap, tracker, false)

			if slotID == 0 {
				t.Fatalf("%v failed to get slot id for replicaMap %v in 0th pass",
					t.Name(), repmap)
			}

			slotIDs[slotID] = true
		}
	}

	if len(slotIDs) != int(testShardCapacity/2) {
		t.Fatalf("%v slots created are %v but should have been only %v slots",
			t.Name(), slotIDs, testShardCapacity/2)
	}

	if len(dealer.partnSlots) != len(replicaMaps) {
		t.Fatalf(
			"%v shard dealer book keeping mismatch - index defns recorded are %v but should have been only %v defns",
			t.Name(),
			dealer.partnSlots,
			len(replicaMaps),
		)
	}
	if len(dealer.slotsMap) != int(testShardCapacity/2) {
		t.Fatalf(
			"%v shard dealer book keeping mismatch - slots created are %v but should have been only %v slots",
			t.Name(),
			dealer.slotsMap,
			testShardCapacity/2,
		)
	}
	if dealer.nodeToShardCountMap[indexerNode.NodeUUID] != testShardCapacity {
		t.Fatalf(
			"%v shard dealer book keeping mismatch - node slot count is %v but should have been only %v",
			t.Name(),
			dealer.nodeToShardCountMap[indexerNode.NodeUUID],
			testShardCapacity,
		)
	}
	assert.NoError(
		t,
		validateShardDealerInternals(dealer, []*IndexerNode{indexerNode}),
		"internal shard dealer validation failed for basic test",
	)

	cip := createIdxParam{defnid: uint64(len(replicaMaps) + 1)}
	var extraIndex = createDummyIndexUsage(cip)
	indexerNode.Indexes = append(indexerNode.Indexes, extraIndex)

	var replicaMap = make(map[int]map[*IndexerNode]*IndexUsage)
	replicaMap[extraIndex.Instance.ReplicaId] = make(map[*IndexerNode]*IndexUsage)
	replicaMap[extraIndex.Instance.ReplicaId][indexerNode] = extraIndex
	tracker++
	slotID := dealer.GetSlot(extraIndex.DefnId, extraIndex.PartnId, replicaMap, tracker, false)
	if _, exists := slotIDs[slotID]; slotID != 0 && exists {
		t.Fatalf(
			"%v extra slot not created when it was not expected. slot returned %v. All Slots %v. Dealer info about slot %v",
			t.Name(),
			slotID,
			slotIDs,
			dealer.slotsMap[slotID],
		)
	}

	assert.NoErrorf(t, validateShardDealerInternals(dealer, []*IndexerNode{indexerNode}),
		"internal shard dealer validation failed for basic test after adding extra index")
}

// TODO: add tests for disk usage check in recordIndex
