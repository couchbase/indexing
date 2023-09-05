// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package planner

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/common/collections"
	"github.com/couchbase/indexing/secondary/iowrap"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/security"
)

type PartnDistMap map[common.PartitionId]map[int]map[*IndexerNode]*IndexUsage
type ReplicaDistMap map[int]map[*IndexerNode]*IndexUsage

//////////////////////////////////////////////////////////////
// Concrete Type/Struct
/////////////////////////////////////////////////////////////

type RunConfig struct {
	Detail           bool
	GenStmt          string
	MemQuotaFactor   float64
	CpuQuotaFactor   float64
	Resize           bool
	MaxNumNode       int
	Output           string
	Shuffle          int
	AllowMove        bool
	AllowSwap        bool
	AllowUnpin       bool
	AddNode          int
	DeleteNode       int
	MaxMemUse        int
	MaxCpuUse        int
	MemQuota         int64
	CpuQuota         int
	DataCostWeight   float64
	CpuCostWeight    float64
	MemCostWeight    float64
	EjectOnly        bool
	DisableRepair    bool
	Timeout          int
	UseLive          bool
	Runtime          *time.Time
	Threshold        float64
	CpuProfile       bool
	MinIterPerTemp   int
	MaxIterPerTemp   int
	UseGreedyPlanner bool

	AllowDDLDuringScaleup bool

	// For file transfer based rebalance, this field is set to
	// "true" if indexes are to be grouped based on alternate shardIds
	EnableShardAffinity bool

	binSize uint64
}

type RunStats struct {
	AvgIndexSize    float64
	StdDevIndexSize float64
	AvgIndexCpu     float64
	StdDevIndexCpu  float64
	MemoryQuota     uint64
	CpuQuota        uint64
	IndexCount      uint64

	Initial_score             float64
	Initial_indexCount        uint64
	Initial_indexerCount      uint64
	Initial_avgIndexSize      float64
	Initial_stdDevIndexSize   float64
	Initial_avgIndexCpu       float64
	Initial_stdDevIndexCpu    float64
	Initial_avgIndexerSize    float64
	Initial_stdDevIndexerSize float64
	Initial_avgIndexerCpu     float64
	Initial_stdDevIndexerCpu  float64
	Initial_movedIndex        uint64
	Initial_movedData         uint64
}

type SubCluster []*IndexerNode

type Plan struct {
	// placement of indexes in nodes
	Placement []*IndexerNode `json:"placement,omitempty"`
	MemQuota  uint64         `json:"memQuota,omitempty"`
	CpuQuota  uint64         `json:"cpuQuota,omitempty"`
	IsLive    bool           `json:"isLive,omitempty"`

	UsedReplicaIdMap map[common.IndexDefnId]map[int]bool

	SubClusters    []SubCluster
	UsageThreshold *UsageThreshold `json:usageThreshold,omitempty"`

	DeletedNodes []string `json:deletedNodes,omitempty` //used only for unit testing
}

type IndexSpec struct {
	// definition
	Name               string             `json:"name,omitempty"`
	Bucket             string             `json:"bucket,omitempty"`
	Scope              string             `json:"scope,omitempty"`
	Collection         string             `json:"collection,omitempty"`
	DefnId             common.IndexDefnId `json:"defnId,omitempty"`
	IsPrimary          bool               `json:"isPrimary,omitempty"`
	SecExprs           []string           `json:"secExprs,omitempty"`
	WhereExpr          string             `json:"where,omitempty"`
	Deferred           bool               `json:"deferred,omitempty"`
	Immutable          bool               `json:"immutable,omitempty"`
	IsArrayIndex       bool               `json:"isArrayIndex,omitempty"`
	RetainDeletedXATTR bool               `json:"retainDeletedXATTR,omitempty"`
	NumPartition       uint64             `json:"numPartition,omitempty"`
	PartitionScheme    string             `json:"partitionScheme,omitempty"`
	HashScheme         uint64             `json:"hashScheme,omitempty"`
	PartitionKeys      []string           `json:"partitionKeys,omitempty"`
	Replica            uint64             `json:"replica,omitempty"`
	Desc               []bool             `json:"desc,omitempty"`
	Using              string             `json:"using,omitempty"`
	ExprType           string             `json:"exprType,omitempty"`

	IndexMissingLeadingKey bool `json:"indexMissingLeadingKey,omitempty"`

	// usage
	NumDoc        uint64  `json:"numDoc,omitempty"`
	DocKeySize    uint64  `json:"docKeySize,omitempty"`
	SecKeySize    uint64  `json:"secKeySize,omitempty"`
	ArrKeySize    uint64  `json:"arrKeySize,omitempty"`
	ArrSize       uint64  `json:"arrSize,omitempty"`
	ResidentRatio float64 `json:"residentRatio,omitempty"`
	MutationRate  uint64  `json:"mutationRate,omitempty"`
	ScanRate      uint64  `json:"scanRate,omitempty"`
}

// Resource Usage Thresholds for serverless model
type UsageThreshold struct {
	MemHighThreshold int32 `json:memHighThreshold,omitempty"`
	MemLowThreshold  int32 `json:memLowThreshold,omitempty"`

	UnitsHighThreshold int32 `json:unitsHighThreshold,omitempty"`
	UnitsLowThreshold  int32 `json:unitsLowThreshold,omitempty"`

	MemQuota   uint64 `json:memQuota,omitempty"`
	UnitsQuota uint64 `json:unitsQuota,omitempty"`
}

type TenantUsage struct {
	SourceId string

	TenantId    string
	MemoryUsage uint64
	UnitsUsage  uint64
}

//////////////////////////////////////////////////////////////
// Integration with Rebalancer
/////////////////////////////////////////////////////////////

func ExecuteRebalance(clusterUrl string, topologyChange service.TopologyChange, masterId string, ejectOnly bool,
	disableReplicaRepair bool, threshold float64, timeout int, cpuProfile bool, minIterPerTemp int,
	maxIterPerTemp int, binSize uint64, enableShardAffinity bool) (map[string]*common.TransferToken, map[string]map[common.IndexDefnId]*common.IndexDefn, error) {
	runtime := time.Now()
	return ExecuteRebalanceInternal(clusterUrl, topologyChange, masterId, false, true, ejectOnly, disableReplicaRepair,
		timeout, threshold, cpuProfile, minIterPerTemp, maxIterPerTemp, binSize, enableShardAffinity, &runtime)
}

func ExecuteRebalanceInternal(clusterUrl string,
	topologyChange service.TopologyChange, masterId string, addNode bool, detail bool, ejectOnly bool,
	disableReplicaRepair bool, timeout int, threshold float64, cpuProfile bool, minIterPerTemp, maxIterPerTemp int,
	binSize uint64, enableShardAffinity bool, runtime *time.Time) (map[string]*common.TransferToken, map[string]map[common.IndexDefnId]*common.IndexDefn, error) {

	plan, err := RetrievePlanFromCluster(clusterUrl, nil, true)
	if err != nil {
		return nil, nil, errors.New(fmt.Sprintf("Unable to read index layout from cluster %v. err = %s", clusterUrl, err))
	}

	nodes := make(map[string]string)
	for _, node := range plan.Placement {
		nodes[node.NodeUUID] = node.NodeId
	}

	deleteNodes := make([]string, len(topologyChange.EjectNodes))
	for i, node := range topologyChange.EjectNodes {
		if _, ok := nodes[string(node.NodeID)]; !ok {
			return nil, nil, errors.New(fmt.Sprintf("Unable to find indexer node with node UUID %v", node.NodeID))
		}
		deleteNodes[i] = nodes[string(node.NodeID)]
	}

	// make sure we have all the keep nodes
	for _, node := range topologyChange.KeepNodes {
		if _, ok := nodes[string(node.NodeInfo.NodeID)]; !ok {
			return nil, nil, errors.New(fmt.Sprintf("Unable to find indexer node with node UUID %v", node.NodeInfo.NodeID))
		}
	}

	var numNode int
	if addNode {
		numNode = len(deleteNodes)
	}

	config := DefaultRunConfig()
	config.Detail = detail
	config.Resize = false
	config.AddNode = numNode
	config.EjectOnly = ejectOnly
	config.DisableRepair = disableReplicaRepair
	config.Timeout = timeout
	config.Runtime = runtime
	config.Threshold = threshold
	config.CpuProfile = cpuProfile
	config.MinIterPerTemp = minIterPerTemp
	config.MaxIterPerTemp = maxIterPerTemp
	config.EnableShardAffinity = enableShardAffinity
	config.binSize = binSize

	p, _, hostToIndexToRemove, err := executeRebal(config, CommandRebalance, plan, nil, deleteNodes, true)
	if p != nil && detail {
		logging.Infof("************ Indexer Layout *************")
		p.Print()
		logging.Infof("****************************************")
	}

	if err != nil {
		return nil, nil, err
	}

	filterSolution(p.Result.Placement)

	var transferTokens map[string]*common.TransferToken
	if enableShardAffinity {
		transferTokens, err = genShardTransferToken2(p.Result, masterId, topologyChange, false)
	} else {
		transferTokens, err = genTransferToken(p.Result, masterId, topologyChange, deleteNodes)
	}
	if err != nil {
		return nil, nil, err
	}

	return transferTokens, hostToIndexToRemove, nil
}

// filterSolution will iterate through the new placement generated by planner and filter out all
// un-necessary movements. (It is also actually required to be called to remove circular replica
// movements Planner may otherwise leave in the plan, which would trigger Rebalance failures.)
//
// E.g. if for an index, there exists 3 replicas on nodes n1 (replica 0),
// n2 (replica 1), n3 (replica 2) in a 4 node cluster (n1,n2,n3,n4) and
// if planner has generated a placement to move replica 0 from n1->n2,
// replica 1 from n2->n3 and replica 3 from n3->n4, the movement of replicas
// from n2 and n3 are unnecessary as replica instances exist on the node before
// and after movement. filterSolution will eliminate all such movements and
// update the solution to have one final movement from n1->n4
//
// Similarly, if there are any cyclic movements i.e. n1->n2,n2->n3,n3->n1,
// all such movements will be avoided
func filterSolution(placement []*IndexerNode) {

	indexDefnMap := make(map[common.IndexDefnId]map[common.PartitionId][]*IndexUsage)
	indexerMap := make(map[string]*IndexerNode)

	// Group the index based on replica, partition. This grouping
	// will help to identify if multiple replicas are being moved
	// between nodes
	for _, indexer := range placement {
		indexerMap[indexer.NodeId] = indexer
		for _, index := range indexer.Indexes {
			// Update destNode for each of the index as planner has
			// finished the run and generated a tentative placement.
			// A transfer token will not be generated if initialNode
			// and destNode are same.
			index.destNode = indexer
			if _, ok := indexDefnMap[index.DefnId]; !ok {
				indexDefnMap[index.DefnId] = make(map[common.PartitionId][]*IndexUsage)
			}
			indexDefnMap[index.DefnId][index.PartnId] = append(indexDefnMap[index.DefnId][index.PartnId], index)
		}
	}

	for _, defnMap := range indexDefnMap {
		for _, indexes := range defnMap {
			if len(indexes) == 1 {
				continue
			}

			transferMap := make(map[string]string)

			// Generate a map of all transfers between nodes for this replica instance
			for _, index := range indexes {
				if index.initialNode != nil && index.initialNode.NodeId != index.destNode.NodeId {
					transferMap[index.initialNode.NodeId] = index.destNode.NodeId
				} else if index.initialNode == nil {
					// Create a dummy source node for replica repair. This is to
					// address scenarios like lost_replica -> n0, n0 -> n1
					key := fmt.Sprintf("ReplicaRepair_%v_%v", index.InstId, index.PartnId)
					transferMap[key] = index.destNode.NodeId
				}
			}

			if len(transferMap) == 0 {
				continue
			}

		loop:
			// Search the transferMap in Depth-First fashion and find the appropriate
			// source and destination
			for src, dst := range transferMap {
				if newDest, ok := transferMap[dst]; ok {
					delete(transferMap, dst)
					if newDest != src {
						transferMap[src] = newDest
					} else { // Delete src to avoid cyclic transfers (n0 -> n1, n1 -> n0)
						delete(transferMap, src)
					}
					goto loop
				}
			}

			// Filter out un-necessary movements from based on transferMap
			// and update the solution to have only valid movements
			for _, index := range indexes {
				var initialNodeId string
				var replicaRepair bool
				if index.initialNode == nil {
					initialNodeId = fmt.Sprintf("ReplicaRepair_%v_%v", index.InstId, index.PartnId)
					replicaRepair = true
				} else {
					initialNodeId = index.initialNode.NodeId
				}

				if destNodeId, ok := transferMap[initialNodeId]; ok {
					// Inst. is moved to a different node after filtering the solution
					if destNodeId != index.destNode.NodeId {
						destIndexer := indexerMap[destNodeId]
						preFilterDest := index.destNode
						index.destNode = destIndexer

						fmsg := "Planner::filterSolution - Planner intended to move the inst: %v, " +
							"partn: %v from node %v to node %v. Instead the inst is moved to node: %v " +
							"after eliminating the un-necessary replica movements"

						if replicaRepair { // initialNode would be nil incase of replica repair
							logging.Infof(fmsg, index.InstId, index.PartnId,
								"", preFilterDest.NodeId, index.destNode.NodeId)
						} else {
							logging.Infof(fmsg, index.InstId, index.PartnId,
								index.initialNode.NodeId, preFilterDest.NodeId, index.destNode.NodeId)
						}
					} else {
						// Initial destination and final destination are same. No change
						// in placement required
					}
				} else {
					// Planner initially planned a movement for this index but after filtering the
					// solution, the movement is deemed un-necessary
					if index.initialNode != nil && index.destNode.NodeId != index.initialNode.NodeId {
						logging.Infof("Planner::filterSolution - Planner intended to move the inst: %v, "+
							"partn: %v from node %v to node %v. This movement is deemed un-necessary as node: %v "+
							"already has a replica partition", index.InstId, index.PartnId, index.initialNode.NodeId,
							index.destNode.NodeId, index.destNode.NodeId)
						index.destNode = index.initialNode
					}
				}
			}
		}
	}
}

// genTransferToken generates transfer tokens for the plan. Since the addition of the filterSolution
// plan post-processing, the destination node of an IndexUsage at this point is explicitly kept in
// IndexUsage.destNode.NodeUUID (index.destNode.NodeUUID in code below). filterSolution does NOT
// move the IndexUsages to their new destination Indexers in the plan (which are the implicit
// destinations as understood by Planner before filterSolution was called), thus the
// IndexerNode.NodeUUID (indexer.NodeUUID) field should NEVER be used in genTransferToken anymore.
func genTransferToken(solution *Solution, masterId string, topologyChange service.TopologyChange,
	deleteNodes []string) (map[string]*common.TransferToken, error) {

	tokens := make(map[string]*common.TransferToken)

	for _, indexer := range solution.Placement {
		for _, index := range indexer.Indexes {
			if index.initialNode != nil && index.initialNode.NodeId != index.destNode.NodeId && !index.pendingCreate {

				// one token for every index replica between a specific source and destination
				tokenKey := fmt.Sprintf("%v %v %v %v", index.DefnId, index.Instance.ReplicaId,
					index.initialNode.NodeUUID, index.destNode.NodeUUID)

				token, ok := tokens[tokenKey]
				if !ok {
					token = &common.TransferToken{
						MasterId:     masterId,
						SourceId:     index.initialNode.NodeUUID,
						DestId:       index.destNode.NodeUUID,
						RebalId:      topologyChange.ID,
						State:        common.TransferTokenCreated,
						InstId:       index.InstId,
						IndexInst:    *index.Instance,
						TransferMode: common.TokenTransferModeMove,
						SourceHost:   index.initialNode.NodeId,
						DestHost:     index.destNode.NodeId,
					}

					token.IndexInst.Defn.InstVersion = token.IndexInst.Version + 1
					token.IndexInst.Defn.ReplicaId = token.IndexInst.ReplicaId
					token.IndexInst.Defn.Using = common.IndexType(indexer.StorageMode)
					token.IndexInst.Defn.Partitions = []common.PartitionId{index.PartnId}
					token.IndexInst.Defn.Versions = []int{token.IndexInst.Version + 1}
					token.IndexInst.Defn.NumPartitions = uint32(token.IndexInst.Pc.GetNumPartitions())
					token.IndexInst.Pc = nil

					// reset defn id and instance id as if it is a new index.
					if common.IsPartitioned(token.IndexInst.Defn.PartitionScheme) {
						instId, err := common.NewIndexInstId()
						if err != nil {
							return nil, fmt.Errorf("Fail to generate transfer token.  Reason: %v", err)
						}

						token.RealInstId = token.InstId
						token.InstId = instId
					}

					// if there is a build token for the definition, set index STATE to active so the
					// index will be built as part of rebalancing.
					if index.pendingBuild && !index.PendingDelete {
						if token.IndexInst.State == common.INDEX_STATE_CREATED || token.IndexInst.State == common.INDEX_STATE_READY {
							token.IndexInst.State = common.INDEX_STATE_ACTIVE
						}
					}

					tokens[tokenKey] = token

				} else {
					// Token exist for the same index replica between the same source and target.   Add partition to token.
					token.IndexInst.Defn.Partitions = append(token.IndexInst.Defn.Partitions, index.PartnId)
					token.IndexInst.Defn.Versions = append(token.IndexInst.Defn.Versions, index.Instance.Version+1)

					if token.IndexInst.Defn.InstVersion < index.Instance.Version+1 {
						token.IndexInst.Defn.InstVersion = index.Instance.Version + 1
					}
				}

			} else if index.initialNode == nil || index.pendingCreate {
				// There is no source node (index is added during rebalance).

				// one token for every index replica between a specific source and destination
				tokenKey := fmt.Sprintf("%v %v %v %v", index.DefnId, index.Instance.ReplicaId,
					"N/A", index.destNode.NodeUUID)

				token, ok := tokens[tokenKey]
				if !ok {
					token = &common.TransferToken{
						MasterId:     masterId,
						SourceId:     "",
						DestId:       index.destNode.NodeUUID,
						RebalId:      topologyChange.ID,
						State:        common.TransferTokenCreated,
						InstId:       index.InstId,
						IndexInst:    *index.Instance,
						TransferMode: common.TokenTransferModeCopy,
						DestHost:     index.destNode.NodeId,
					}

					token.IndexInst.Defn.InstVersion = 1
					token.IndexInst.Defn.ReplicaId = token.IndexInst.ReplicaId
					token.IndexInst.Defn.Using = common.IndexType(indexer.StorageMode)
					token.IndexInst.Defn.Partitions = []common.PartitionId{index.PartnId}
					token.IndexInst.Defn.Versions = []int{1}
					token.IndexInst.Defn.NumPartitions = uint32(token.IndexInst.Pc.GetNumPartitions())
					token.IndexInst.Pc = nil

					// reset defn id and instance id as if it is a new index.
					if common.IsPartitioned(token.IndexInst.Defn.PartitionScheme) {
						instId, err := common.NewIndexInstId()
						if err != nil {
							return nil, fmt.Errorf("Fail to generate transfer token.  Reason: %v", err)
						}

						token.RealInstId = token.InstId
						token.InstId = instId
					}

					tokens[tokenKey] = token

				} else {
					// Token exist for the same index replica between the same source and target.   Add partition to token.
					token.IndexInst.Defn.Partitions = append(token.IndexInst.Defn.Partitions, index.PartnId)
					token.IndexInst.Defn.Versions = append(token.IndexInst.Defn.Versions, 1)

					if token.IndexInst.Defn.InstVersion < index.Instance.Version+1 {
						token.IndexInst.Defn.InstVersion = index.Instance.Version + 1
					}
				}
			}
		}
	}

	result := make(map[string]*common.TransferToken)
	for _, token := range tokens {
		ustr, _ := common.NewUUID()
		ttid := fmt.Sprintf("TransferToken%s", ustr.Str())
		result[ttid] = token

		if len(token.SourceId) != 0 {
			logging.Infof("Generating Transfer Token (%v) for rebalance (%v)", ttid, token)
		} else {
			logging.Infof("Generating Transfer Token (%v) for rebuilding lost replica (%v)", ttid, token)
		}
	}

	return result, nil
}

func getDefnIdToInstIdMap(solution *Solution) map[common.IndexDefnId]map[int]common.IndexInstId {
	defnIdToInstIdMap := make(map[common.IndexDefnId]map[int]common.IndexInstId)
	for _, indexer := range solution.Placement {
		for _, index := range indexer.Indexes {
			if index.initialNode == nil { // Ignore all replica instances
				continue
			}
			if _, ok := defnIdToInstIdMap[index.DefnId]; !ok {
				defnIdToInstIdMap[index.DefnId] = make(map[int]common.IndexInstId)
			}
			defnIdToInstIdMap[index.DefnId][index.Instance.ReplicaId] = index.InstId
		}
	}
	return defnIdToInstIdMap
}

func groupIndexInstances(index *IndexUsage, defnIdToInstIdMap map[common.IndexDefnId]map[int]common.IndexInstId) {
	if index.initialNode == nil {
		if newInstId, ok := defnIdToInstIdMap[index.DefnId][index.Instance.ReplicaId]; ok {
			index.InstId = newInstId
			index.Instance.InstId = newInstId
		} else { // Update existing map with new instance
			if _, ok := defnIdToInstIdMap[index.DefnId]; !ok {
				defnIdToInstIdMap[index.DefnId] = make(map[int]common.IndexInstId)
			}
			defnIdToInstIdMap[index.DefnId][index.Instance.ReplicaId] = index.InstId
		}
	}
}

func addToInstRenamePath(token *common.TransferToken, index *IndexUsage, sliceIndex int) {
	if index.initialNode != nil {
		return
	}

	currPathInMeta := fmt.Sprintf("%v_%v_%v_%v.index", index.Bucket, index.Name, index.siblingIndex.InstId, index.PartnId)

	newInstId := token.InstIds[sliceIndex]
	if common.IsPartitioned(index.Instance.Defn.PartitionScheme) {
		newInstId = token.RealInstIds[sliceIndex]
	}
	newPathInMeta := fmt.Sprintf("%v_%v_%v_%v.index", index.Bucket, index.Name, newInstId, index.PartnId)

	if token.InstRenameMap == nil {
		token.InstRenameMap = make(map[common.ShardId]map[string]string)
	}

	for i, shardId := range index.ShardIds {
		if _, ok := token.InstRenameMap[shardId]; !ok {
			token.InstRenameMap[shardId] = make(map[string]string)
		}
		if i == 0 { // Main index only
			token.InstRenameMap[shardId][currPathInMeta+"/mainIndex"] = newPathInMeta + "/mainIndex"
		} else { // Back index only
			token.InstRenameMap[shardId][currPathInMeta+"/docIndex"] = newPathInMeta + "/docIndex"
		}
	}
}

func genShardTransferToken(solution *Solution, masterId string, topologyChange service.TopologyChange,
	deleteNodes []string) (map[string]*common.TransferToken, error) {

	// Generate a mapping between defnId -> replicaId -> instanceId.
	defnIdToInstIdMap := getDefnIdToInstIdMap(solution)

	getInstIds := func(index *IndexUsage) (common.IndexInstId /* instId*/, common.IndexInstId /*realInstId*/, error) {

		if common.IsPartitioned(index.Instance.Defn.PartitionScheme) {
			instId, err := common.NewIndexInstId()
			if err != nil {
				return 0, 0, fmt.Errorf("Fail to generate transfer token.  Reason: %v", err)
			}

			return instId, index.InstId, nil
		}
		return index.InstId, 0, nil
	}

	initInstInToken := func(token *common.TransferToken, index *IndexUsage) error {

		tempInst := *index.Instance

		// Reset pertitions and versions in definitions
		// transfer token generator will later populate these fields
		tempInst.Defn.Partitions = nil
		tempInst.Defn.Versions = nil
		tempInst.Defn.ShardIdsForDest = make(map[common.PartitionId][]common.ShardId)
		tempInst.Defn.AlternateShardIds = make(map[common.PartitionId][]string)

		token.IndexInsts = append(token.IndexInsts, tempInst)

		instId, realInstId, err := getInstIds(index)
		if err != nil {
			return err
		}
		token.InstIds = append(token.InstIds, instId)
		token.RealInstIds = append(token.RealInstIds, realInstId)
		return nil
	}

	tokens := make(map[string]*common.TransferToken)

	initToken := func(index *IndexUsage) (*common.TransferToken, error) {

		token := &common.TransferToken{
			MasterId:                masterId,
			DestId:                  index.destNode.NodeUUID,
			RebalId:                 topologyChange.ID,
			ShardTransferTokenState: common.ShardTokenCreated,
			DestHost:                index.destNode.NodeId,
			Version:                 common.MULTI_INST_SHARD_TRANSFER,
			ShardIds:                index.ShardIds,
		}

		if index.initialNode != nil && index.initialNode.NodeId != index.destNode.NodeId {
			token.SourceHost = index.initialNode.NodeId
			token.SourceId = index.initialNode.NodeUUID
			token.TransferMode = common.TokenTransferModeMove
		} else if index.initialNode == nil { // Replica repair case
			token.SourceId = index.siblingIndex.initialNode.NodeUUID
			token.SourceHost = index.siblingIndex.initialNode.NodeId
			token.TransferMode = common.TokenTransferModeCopy
		}

		if err := initInstInToken(token, index); err != nil {
			return nil, err
		}
		return token, nil
	}

	addIndexToToken := func(tokenKey string, index *IndexUsage, indexer *IndexerNode) error {

		// Currently, planner is generating a new index instance per partition.
		// This method will group all partitions of an index residing on a node
		// into a single index instance
		groupIndexInstances(index, defnIdToInstIdMap)

		token, ok := tokens[tokenKey]
		if !ok {
			var err error
			if token, err = initToken(index); err != nil {
				return err
			}
			tokens[tokenKey] = token
		}

		if token != nil {
			sliceIndex := len(token.InstIds) - 1
			found := false
			instIds := token.InstIds
			if common.IsPartitioned(index.Instance.Defn.PartitionScheme) {
				// For partitioned index, use realInstIds. If it is the first
				// time this partition index is being processed, it will be
				// added due to !found becoming true
				instIds = token.RealInstIds
			}

			for i, instId := range instIds {
				if instId == index.InstId {
					sliceIndex = i
					found = true
					break
				}
			}

			if !found { // Instance not already appended to list. Add now
				if err := initInstInToken(token, index); err != nil {
					return err
				}

				sliceIndex = len(token.InstIds) - 1
			}

			if index.initialNode == nil {
				addToInstRenamePath(token, index, sliceIndex)
			}

			// If there is a build token for the definition, set index STATE to INITIAL so the
			// index will be built as part of rebalancing. It is not a good idea to build this
			// index with other indexes of the keyspace as they may have a non-zero timestamp.
			// If all the indexes are built in same batch, then indexer will compute the minimum
			// restartable timestamp & build for all indexes will start at "0" seqno. Hence, all
			// indexes with INITIAL state are built separately during recovery
			if index.pendingBuild && !index.PendingDelete {
				if token.IndexInsts[sliceIndex].State == common.INDEX_STATE_CREATED ||
					token.IndexInsts[sliceIndex].State == common.INDEX_STATE_READY {
					token.IndexInsts[sliceIndex].State = common.INDEX_STATE_INITIAL
				}
			}

			if len(token.IndexInsts[sliceIndex].Defn.ShardIdsForDest) == 0 {
				token.IndexInsts[sliceIndex].Defn.ShardIdsForDest = make(map[common.PartitionId][]common.ShardId)
			}

			// For indexes marked pendingCreate, shardIds do not exist yet
			// Explicitly set the ShardIdsForDest to nil. Rebalancer will
			// use this information to create a new index instead of
			// recovering the index from restored shard
			if index.pendingCreate {
				token.IndexInsts[sliceIndex].Defn.ShardIdsForDest[index.PartnId] = nil
			} else {
				token.IndexInsts[sliceIndex].Defn.ShardIdsForDest[index.PartnId] = index.ShardIds
			}

			token.IndexInsts[sliceIndex].Defn.InstStateAtRebal = token.IndexInsts[sliceIndex].State
			token.IndexInsts[sliceIndex].Defn.InstVersion = token.IndexInsts[sliceIndex].Version + 1
			token.IndexInsts[sliceIndex].Defn.ReplicaId = token.IndexInsts[sliceIndex].ReplicaId
			token.IndexInsts[sliceIndex].Defn.Using = common.IndexType(indexer.StorageMode)
			if token.IndexInsts[sliceIndex].Pc != nil {
				token.IndexInsts[sliceIndex].Defn.NumPartitions = uint32(token.IndexInsts[sliceIndex].Pc.GetNumPartitions())
			}
			token.IndexInsts[sliceIndex].Pc = nil

			if found {
				// Token exist for the same index replica between the same source and target.   Add partition to token.
				token.IndexInsts[sliceIndex].Defn.Partitions = append(token.IndexInsts[sliceIndex].Defn.Partitions, index.PartnId)
				token.IndexInsts[sliceIndex].Defn.Versions = append(token.IndexInsts[sliceIndex].Defn.Versions, index.Instance.Version+1)
			} else { // Initialise the partitions as this is the first time
				token.IndexInsts[sliceIndex].Defn.Partitions = []common.PartitionId{index.PartnId}
				token.IndexInsts[sliceIndex].Defn.Versions = []int{index.Instance.Version + 1}
			}

			if token.IndexInsts[sliceIndex].Defn.InstVersion < index.Instance.Version+1 {
				token.IndexInsts[sliceIndex].Defn.InstVersion = index.Instance.Version + 1
			}
		}
		return nil
	}

	for _, indexer := range solution.Placement {
		for _, index := range indexer.Indexes {

			// Primary index contians only one shard. Process primary
			// index shard after all secondary index tokens are generated
			// so that it can be added to one of the existing token for
			// the shard
			if len(index.ShardIds) == 1 || index.pendingCreate {
				continue
			}

			shardIds := make([]common.ShardId, len(index.ShardIds))
			copy(shardIds, index.ShardIds)

			// one token for every pair of shard movements of a bucket between
			// a specific source and destination
			sort.Slice(shardIds, func(i, j int) bool {
				return shardIds[i] < shardIds[j]
			})

			// one token for every pair of shard movements of a bucket between
			// a specific source and destination
			var tokenKey string
			if index.initialNode != nil && index.initialNode.NodeId != index.destNode.NodeId && !index.pendingCreate {
				tokenKey = fmt.Sprintf("%v %v %v %v", index.Bucket, shardIds, index.initialNode.NodeUUID, index.destNode.NodeUUID)
			} else if index.initialNode == nil && !index.pendingCreate {
				// There is no source node (index is added during rebalance).
				tokenKey = fmt.Sprintf("%v %v %v %v", index.Bucket, shardIds,
					"replica_repair_"+index.siblingIndex.initialNode.NodeUUID, index.destNode.NodeUUID)
			}
			if len(tokenKey) > 0 {
				if err := addIndexToToken(tokenKey, index, indexer); err != nil {
					return nil, err
				}
			}
		}
	}

	// Process primary indexes only
	for _, indexer := range solution.Placement {
		for _, index := range indexer.Indexes {

			if len(index.ShardIds) != 1 || index.pendingCreate {
				continue
			}

			// Find a token key that contains the shard of this index
			// Note: Since tokens are grouped based on shardId's, it is
			// expected to have only one token matching the shardId of
			// primary index
			found := false
			for tokenKey, token := range tokens {

				if index.Bucket != token.IndexInsts[0].Defn.Bucket {
					continue
				}

				if index.destNode.NodeUUID != token.DestId {
					continue
				}

				if index.initialNode == nil && index.siblingIndex.initialNode.NodeUUID != token.SourceId {
					continue
				}

				if index.initialNode != nil && index.initialNode.NodeUUID != token.SourceId {
					continue
				}

				for _, shardId := range token.ShardIds {
					if index.ShardIds[0] == shardId {
						// Add index to this token
						addIndexToToken(tokenKey, index, indexer)
						found = true
						break
					}

				}
			}
			// Could be only primary indexes in this shard
			// Generate a new token and add it to the group
			if !found {
				var tokenKey string
				if index.initialNode != nil && index.initialNode.NodeId != index.destNode.NodeId && !index.pendingCreate {
					tokenKey = fmt.Sprintf("%v %v %v %v", index.Bucket, index.ShardIds, index.initialNode.NodeUUID, index.destNode.NodeUUID)
				} else if index.initialNode == nil && !index.pendingCreate {
					// There is no source node (index is added during rebalance).
					tokenKey = fmt.Sprintf("%v %v %v %v", index.Bucket, index.ShardIds,
						"replica_repair_"+index.siblingIndex.initialNode.NodeUUID, index.destNode.NodeUUID)
				}
				if len(tokenKey) > 0 {
					if err := addIndexToToken(tokenKey, index, indexer); err != nil {
						return nil, err
					}
				}
			}
		}
	}

	// Process all pendingCreate indexes
	for _, indexer := range solution.Placement {
		for _, index := range indexer.Indexes {

			if !index.pendingCreate {
				continue
			}

			// Find a token key that is already moving the index belonging
			// to this bucket.
			// Note: Since tokens are grouped based on shardId's, it is
			// expected to have only one token matching the shardId of
			// primary index
			found := false
			for tokenKey, token := range tokens {

				if index.Bucket != token.IndexInsts[0].Defn.Bucket {
					continue
				}

				if index.destNode.NodeUUID != token.DestId {
					continue
				}

				// for pendingCreate index, initial node is always non-nil
				if index.initialNode == nil {
					logging.Fatalf("genShardTransferToken: initialNode is nil for pendingCreate index (%v, %v, %v, %v, %v)",
						index.Bucket, index.Scope, index.Collection, index.Name, index.Instance.ReplicaId)
					continue
				}

				if index.initialNode != nil && index.initialNode.NodeUUID != token.SourceId {
					continue
				}

				if index.destNode != nil && index.destNode.NodeUUID != token.DestId {
					continue
				}

				// At this point, "token" is moving the indexes from source node to
				// destination node for this bucket. Add the pendingCreate index to
				// this token.
				addIndexToToken(tokenKey, index, indexer)
				found = true
				break
			}
			// Could be only pendingCreate index is this group
			// Generate a new token and add it to the group
			if !found {
				var tokenKey string
				if index.initialNode != nil && index.initialNode.NodeId != index.destNode.NodeId {
					tokenKey = fmt.Sprintf("%v pending_create_%v %v %v", index.Bucket, index.ShardIds, index.initialNode.NodeUUID, index.destNode.NodeUUID)
				} // initialNode is non-nil for pending create indexes
				if len(tokenKey) > 0 {
					if err := addIndexToToken(tokenKey, index, indexer); err != nil {
						return nil, err
					}
				}
			}

		}
	}

	result := make(map[string]*common.TransferToken)
	for _, token := range tokens {
		ustr, _ := common.NewUUID()
		ttid := fmt.Sprintf("ShardToken%s", ustr.Str())
		result[ttid] = token
	}

	var err error
	result, err = populateSiblingTokenId(solution, result)
	if err != nil {
		return nil, err
	}

	logTransferTokens := func(logErr bool) {
		for ttid, token := range result {
			if len(token.SourceId) != 0 {
				if logErr {
					logging.Errorf("Generating Shard Transfer Token (%v) for rebalance (%v)", ttid, token)
				} else {
					logging.Infof("Generating Shard Transfer Token (%v) for rebalance (%v)", ttid, token)
				}
			} else {
				if logErr {
					logging.Errorf("Generating Shard Transfer Token (%v) for rebuilding lost replica (%v)", ttid, token)
				} else {
					logging.Infof("Generating Shard Transfer Token (%v) for rebalance (%v)", ttid, token)
				}
			}
		}
	}

	logTransferTokens(false)

	return result, nil
}

func genShardTransferToken2(soln *Solution, masterId string, topologyChange service.TopologyChange,
	isServerless bool) (map[string]*common.TransferToken, error) {

	tokens := make(map[string]*common.TransferToken)

	// merge tokenB into tokenA for DCP based tokens
	mergeToken := func(tokenA, tokenB *common.TransferToken) bool {
		if tokenB.BuildSource != tokenA.BuildSource {
			return false
		}

		switch tokenA.BuildSource {
		case common.TokenBuildSourceDcp:
			tokenA.IndexInst.Defn.Partitions = append(tokenA.IndexInst.Defn.Partitions,
				tokenB.IndexInst.Defn.Partitions...)
			tokenA.IndexInst.Defn.Versions = append(tokenA.IndexInst.Defn.Versions,
				tokenB.IndexInst.Defn.Versions...)
			if tokenA.IndexInst.Defn.AlternateShardIds == nil {
				tokenA.IndexInst.Defn.AlternateShardIds = make(map[common.PartitionId][]string)
			}
			for partnId, asi := range tokenB.IndexInst.Defn.AlternateShardIds {
				tokenA.IndexInst.Defn.AlternateShardIds[partnId] = asi
			}

			if tokenA.IndexInst.Defn.ShardIdsForDest == nil {
				tokenA.IndexInst.Defn.ShardIdsForDest = make(map[common.PartitionId][]common.ShardId)
			}

			for partnId, shardIds := range tokenB.IndexInst.Defn.ShardIdsForDest {
				tokenA.IndexInst.Defn.ShardIdsForDest[partnId] = shardIds
			}

		default:
			return false
		}
		return true
	}

	var createTokenForIndex func(*IndexUsage) ([]*common.TransferToken, []string, error)

	// createTokenForIndex - creates TransferTokens for IndexUsage for shard affinity
	// if an IndexUsage which represents a single partition, this will only generate a single token
	// if the usage is for a shard proxy, it can generate 1+(len(indexes-in-shard)) tokens
	// depending on the state of replicas
	createTokenForIndex = func(index *IndexUsage) ([]*common.TransferToken, []string, error) {
		allTokens := make([]*common.TransferToken, 1)
		keys := make([]string, 1)

		token := common.TransferToken{
			DestId:   index.destNode.NodeUUID,
			DestHost: index.destNode.NodeId,
			MasterId: masterId,
			RebalId:  topologyChange.ID,
		}
		var tokenKey string

		// set transfer details
		if index.initialNode == nil || index.pendingCreate {
			// replica repair
			token.SourceId = ""
			token.SourceHost = ""
			token.TransferMode = common.TokenTransferModeCopy

			if index.siblingIndex != nil && index.siblingIndex.initialNode != nil {
				// shard repair
				token.SourceId = index.siblingIndex.initialNode.NodeId
				token.SourceHost = index.siblingIndex.initialNode.NodeUUID
			}

			// reset instance version for new replicas being created
			token.IndexInst.Version = 0
		} else if index.initialNode.NodeId != index.destNode.NodeId {
			// move
			token.TransferMode = common.TokenTransferModeMove
			token.SourceId = index.initialNode.NodeUUID
			token.SourceHost = index.initialNode.NodeId
		}

		// set index inst
		if index.IsShardProxy {
			// index is a proxy for shard (grouped indexes);
			// all indexes under the proxy move together.
			// original indexes should be found from `index.AllIndexes` by alternateShardId
			// index.DefnId -> proxy of AlternateShardId.SlotNo

			token.BuildSource = common.TokenBuildSourcePeer
			token.ShardTransferTokenState = common.ShardTokenCreated
			token.Version = common.MULTI_INST_SHARD_TRANSFER

			var asi *common.AlternateShardId
			subTokenMap := make(map[string]*common.TransferToken)

			// set token.Insts, token.InstIds and token.RealInstIds by alternateShardId-slot
			// there should only be 1 <slotNo, destId>/<slotNo, replicaId> pair
			// all indexes under it move together
			if index.destNode.NodeVersion < common.INDEXER_76_VERSION {
				// TODO: if the dest node is on version less than 7.6
				// need to fallback to DCP based movement here;
				// this is possible in a mixed mode cluster with server versions 7.6 (or above) and
				// server version before 7.6 (nodes which can honour and can't honour shardAffintiy
				// are in the same cluster) as planner should always run on latest version nodes
			} else {
				var err error
				asi, err = common.ParseAlternateId(index.AlternateShardIds[0])
				if err != nil {
					return nil, nil, fmt.Errorf("failed to parse alternate shard id with err %v", err)
				}
			}

			for _, realIndex := range index.GroupedIndexes {
				// we use recursion here to initialise the IndexInst to correct values

				// realIndex alternateShardIds might not be initialised in the plan
				realIndex.AlternateShardIds = index.AlternateShardIds
				realIndex.destNode = index.destNode

				childTokens, childKeys, err := createTokenForIndex(realIndex)
				if err != nil {
					return nil, nil, err
				}

				if len(childTokens) != 1 || len(childKeys) != 1 {
					// this case should not happen
					return nil, nil, fmt.Errorf("unexpected child tokens created")
				}

				if realIndex.initialNode == nil || realIndex.pendingCreate {
					// case of replica repair for lost replica (replica is not a part of shard)
					// recreate replica as a part of the shard but with DCP
					allTokens = append(allTokens, childTokens[0])
					keys = append(keys, childKeys[0])
				} else {
					if len(index.ShardIds) > 0 {
						childTokens[0].IndexInst.Defn.ShardIdsForDest = make(map[common.PartitionId][]common.ShardId)
						childTokens[0].IndexInst.Defn.ShardIdsForDest[realIndex.PartnId] = index.ShardIds
					}

					oldChildToken, ok := subTokenMap[childKeys[0]]
					if !ok {
						subTokenMap[childKeys[0]] = childTokens[0]
					} else {
						if !mergeToken(oldChildToken, childTokens[0]) {
							return nil, nil, fmt.Errorf("failed to merge token %v and %v",
								oldChildToken, childTokens[0])
						}
					}
				}
			}

			for _, childToken := range subTokenMap {
				token.IndexInsts = append(token.IndexInsts, childToken.IndexInst)
				token.InstIds = append(token.InstIds, childToken.InstId)
				token.RealInstIds = append(token.RealInstIds, childToken.RealInstId)
			}

			token.ShardIds = index.ShardIds

			tokenKey = fmt.Sprintf("asi_%v_%v-%v-%v", asi.SlotId, asi.ReplicaId, token.SourceId,
				token.DestId)

		} else {
			// single index only
			// set token.Inst, token.InstId and token.RealInstId

			token.BuildSource = common.TokenBuildSourceDcp
			token.State = common.TransferTokenCreated
			token.Version = common.SINGLE_INST_DCP_BUILD

			token.IndexInst = *index.Instance
			token.InstId = index.InstId

			token.IndexInst.Defn.InstStateAtRebal = token.IndexInst.State
			token.IndexInst.Defn.InstVersion = token.IndexInst.Version + 1
			token.IndexInst.Defn.ReplicaId = token.IndexInst.ReplicaId
			token.IndexInst.Defn.Using = common.IndexType(index.StorageMode)
			token.IndexInst.Defn.Partitions = []common.PartitionId{index.PartnId}
			token.IndexInst.Defn.Versions = []int{token.IndexInst.Version + 1}
			token.IndexInst.Defn.NumPartitions = uint32(token.IndexInst.Pc.GetNumPartitions())
			token.IndexInst.Defn.AlternateShardIds = make(map[common.PartitionId][]string)
			if index.HasAlternateShardIds() {
				token.IndexInst.Defn.AlternateShardIds[index.PartnId] = index.AlternateShardIds
			}

			token.IndexInst.Pc = nil

			// reset defn id and instance id as if it is a new index.
			if common.IsPartitioned(token.IndexInst.Defn.PartitionScheme) {
				instId, err := common.NewIndexInstId()
				if err != nil {
					return nil, nil, fmt.Errorf("Fail to generate transfer token.  Reason: %v", err)
				}

				token.RealInstId = token.InstId
				token.InstId = instId
			}

			// if there is a build token for the definition, set index STATE to active so the
			// index will be built as part of rebalancing.
			if index.pendingBuild && !index.PendingDelete && (token.IndexInst.State == common.INDEX_STATE_CREATED ||
				token.IndexInst.State == common.INDEX_STATE_READY) {

				token.IndexInst.State = common.INDEX_STATE_ACTIVE

			}

			tokenKey = fmt.Sprintf("dcp_%v-%v-%v-%v", index.DefnId, index.Instance.ReplicaId,
				token.SourceId, token.DestId)

		}

		allTokens[0] = &token
		keys[0] = tokenKey

		return allTokens, keys, nil
	}

	for _, indexer := range soln.Placement {
		for _, index := range indexer.Indexes {

			if index.initialNode != nil && index.initialNode.NodeId == index.destNode.NodeId {
				// nothing to generate for indexes which are not moving
				continue
			}

			groupedTokens, tokenKeys, err := createTokenForIndex(index)
			if err != nil {
				return nil, err
			}

			// merge using token key
			for i := 0; i < len(groupedTokens); i++ {
				token := groupedTokens[i]
				tokenKey := tokenKeys[i]

				if token.BuildSource == common.TokenBuildSourcePeer && len(token.IndexInsts) == 0 {
					// because of the way we generate groupedTokens, it can be that we end up adding
					// an empty token for a shard proxy while all of it's indexes are needed to be
					// rebuilt with DCP; if that is the case skip the token
					continue
				}

				oldToken, ok := tokens[tokenKey]
				if !ok {
					tokens[tokenKey] = token
				} else {
					mergeToken(oldToken, token)
				}
			}
		}
	}

	if isServerless {
		// TODO: populate siblingToken for AlternateShardIds
	}

	result := make(map[string]*common.TransferToken)

	for _, token := range tokens {
		uuid, _ := common.NewUUID()
		var ttid string
		if token.IsShardTransferToken() {
			ttid = fmt.Sprintf("ShardToken%s", uuid.Str())
		} else {
			ttid = fmt.Sprintf("TransferToken%s", uuid.Str())
		}
		result[ttid] = token

		logging.Infof("ShardTransferToken(v2) generated token - %v | %v", ttid, token)
	}

	return result, nil
}

func validateSiblingTokenId(transferTokens map[string]*common.TransferToken) error {
	for ttid, token := range transferTokens {
		if token.TransferMode != common.TokenTransferModeMove {
			continue
		}

		if token.SiblingTokenId == "" {
			return fmt.Errorf("validateSiblingTokenId: Sibling tokenId empty for ttid: %v, token: %v", ttid, token)
		}
		if sibling, ok := transferTokens[token.SiblingTokenId]; !ok {
			return fmt.Errorf("validateSiblingTokenId: Sibling token not found in list for ttid: %v, token: %v", ttid, token)
		} else if sibling.SiblingTokenId != ttid {
			return fmt.Errorf("validateSiblingTokenId: Invalid token Id for sibling. ttid: %v, siblingTokenId: %v, token: %v, siblingToken: %v", ttid, sibling.SiblingTokenId, token, sibling)
		}
	}
	return nil
}

func populateSiblingTokenId(solution *Solution, transferTokens map[string]*common.TransferToken) (map[string]*common.TransferToken, error) {
	// It is ok to ignore the error here as this method is only called
	// after err == nil is seen the same solution
	subClusters, _ := groupIndexNodesIntoSubClusters(solution.Placement)

	// Group transfer tokens moving from one sub cluster to another cluster
	subClusterTokenIdMap := make(map[string][]string)
	for ttid, token := range transferTokens {

		srcSubClusterPos := getSubClusterPosForNode(subClusters, token.SourceId)
		destSubClusterPos := getSubClusterPosForNode(subClusters, token.DestId)
		// Group all tokens of a bucket moving from a source subcluster to a
		// destination subcluster
		groupKey := fmt.Sprintf("%v %v %v", srcSubClusterPos, destSubClusterPos, token.IndexInsts[0].Defn.Bucket)
		subClusterTokenIdMap[groupKey] = append(subClusterTokenIdMap[groupKey], ttid)
	}

	allPartitionsMatch := func(inst1, inst2 []common.PartitionId) bool {

		if len(inst1) != len(inst2) {
			return false
		}

		sort.Slice(inst1, func(i, j int) bool {
			return inst1[i] < inst1[j]
		})

		sort.Slice(inst2, func(i, j int) bool {
			return inst2[i] < inst2[j]
		})

		for i := range inst1 {
			if inst1[i] != inst2[i] {
				return false
			}
		}
		return true
	}

	areTokensMatchingAllInsts := func(tid1, tid2 string) bool {
		token1, _ := transferTokens[tid1]
		token2, _ := transferTokens[tid2]

		for i, inst1 := range token1.IndexInsts {

			foundInst := false
			for j, inst2 := range token2.IndexInsts {
				if inst1.Defn.DefnId == inst2.Defn.DefnId { // Defn ID will be same for replica index instances

					// As partitions can be spread across multiple nodes, they share the same defnId and instId
					// Do not compare partitions belonging to same instances. Only compare partitions
					// of replicas
					if common.IsPartitioned(inst1.Defn.PartitionScheme) && token1.RealInstIds[i] == token2.RealInstIds[j] {
						continue
					}

					inst1Partns := inst1.Defn.Partitions
					inst2Partns := inst2.Defn.Partitions
					foundInst = allPartitionsMatch(inst1Partns, inst2Partns)
					break
				}
			}

			if !foundInst {
				return false
			}
		}
		return true
	}

	for _, tokens := range subClusterTokenIdMap {
		for i, _ := range tokens {
			for j, _ := range tokens {
				if i != j {
					if areTokensMatchingAllInsts(tokens[i], tokens[j]) {
						transferTokens[tokens[i]].SiblingTokenId = tokens[j]
						transferTokens[tokens[j]].SiblingTokenId = tokens[i]
						continue
					}
				}
			}
		}
	}

	return transferTokens, nil
}

//////////////////////////////////////////////////////////////
// Integration with Metadata Provider
/////////////////////////////////////////////////////////////

func ExecutePlan(clusterUrl string, indexSpecs []*IndexSpec, nodes []string, override bool,
	useGreedyPlanner bool, enforceLimits bool, allowDDLDuringScaleUp bool,
	binSize uint64, enableShardAffinity bool) (*Solution, error) {

	plan, err := RetrievePlanFromCluster(clusterUrl, nodes, false)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Unable to read index layout from cluster %v. err = %s", clusterUrl, err))
	}

	if enforceLimits {
		scopeSet := make(map[string]bool)
		for _, indexSpec := range indexSpecs {
			if _, found := scopeSet[indexSpec.Bucket+":"+indexSpec.Scope]; !found {
				scopeSet[indexSpec.Bucket+":"+indexSpec.Scope] = true
				scopeLimit, err := GetIndexScopeLimit(clusterUrl, indexSpec.Bucket, indexSpec.Scope)
				if err != nil {
					return nil, err
				}

				if scopeLimit != collections.NUM_INDEXES_NIL {
					numIndexes := GetNumIndexesPerScope(plan, indexSpec.Bucket, indexSpec.Scope)
					// GetNewIndexesPerScope will be called only once per Bucket+Scope
					newIndexes := GetNewIndexesPerScope(indexSpecs, indexSpec.Bucket, indexSpec.Scope)
					if numIndexes+newIndexes > scopeLimit {
						errMsg := fmt.Sprintf("%v Limit : %v", common.ErrIndexScopeLimitReached.Error(), scopeLimit)
						return nil, errors.New(errMsg)
					}
				}
			}
		}
	}

	if override && len(nodes) != 0 {
		for _, indexer := range plan.Placement {
			found := false
			for _, node := range nodes {
				encryptedNodeAddr, _ := security.EncryptPortInAddr(node)
				if indexer.NodeId == node ||
					indexer.NodeId == encryptedNodeAddr {
					found = true
					break
				}
			}

			if found {
				indexer.UnsetExclude()
			} else {
				indexer.SetExclude("in")
			}
		}
	}

	if err = verifyDuplicateIndex(plan, indexSpecs); err != nil {
		return nil, err
	}

	detail := logging.IsEnabled(logging.Info)

	return ExecutePlanWithOptions(plan, indexSpecs, detail, "", "", -1, -1, -1, false, true, useGreedyPlanner, allowDDLDuringScaleUp, binSize, enableShardAffinity)
}

func GetNumIndexesPerScope(plan *Plan, Bucket string, Scope string) uint32 {
	var numIndexes uint32 = 0
	for _, indexernode := range plan.Placement {
		for _, index := range indexernode.Indexes {
			if index.Bucket == Bucket && index.Scope == Scope {
				numIndexes = numIndexes + 1
			}
		}
	}
	return numIndexes
}

func GetNewIndexesPerScope(indexSpecs []*IndexSpec, bucket string, scope string) uint32 {
	var newIndexes uint32 = 0
	for _, indexSpec := range indexSpecs {
		if indexSpec.Bucket == bucket && indexSpec.Scope == scope {
			newIndexes = newIndexes + uint32(indexSpec.NumPartition*indexSpec.Replica)
		}
	}
	return newIndexes
}

// Alternate approach: Get Scope Limit from clusterInfoClient from proxy.go
// But in order to get Scope limit only bucket and scope name is required and multiple calls need
// not be made to fetch bucket list, pool etc. For future use of this function this Implementation
// removes the dependency on Fetching ClusterInfoCache and uses only one API call.
func GetIndexScopeLimit(clusterUrl, bucket, scope string) (uint32, error) {
	clusterURL, err := common.ClusterAuthUrl(clusterUrl)
	if err != nil {
		return 0, err
	}
	cinfo, err := common.NewClusterInfoCache(clusterURL, "default")
	if err != nil {
		return 0, err
	}
	cinfo.Lock()
	defer cinfo.Unlock()
	cinfo.SetUserAgent("Planner:Executor:GetIndexScopeLimit")
	return cinfo.GetIndexScopeLimit(bucket, scope)
}

func verifyDuplicateIndex(plan *Plan, indexSpecs []*IndexSpec) error {
	for _, spec := range indexSpecs {
		for _, indexer := range plan.Placement {
			for _, index := range indexer.Indexes {
				if index.Name == spec.Name && index.Bucket == spec.Bucket &&
					index.Scope == spec.Scope && index.Collection == spec.Collection {

					errMsg := fmt.Sprintf("%v.  Fail to create %v in bucket %v, scope %v, collection %v",
						common.ErrIndexAlreadyExists.Error(), spec.Name, spec.Bucket, spec.Scope, spec.Collection)
					return errors.New(errMsg)
				}
			}
		}
	}

	return nil
}

func FindIndexReplicaNodes(clusterUrl string, nodes []string, defnId common.IndexDefnId) ([]string, error) {

	plan, err := RetrievePlanFromCluster(clusterUrl, nodes, false)
	if err != nil {
		return nil, fmt.Errorf("Unable to read index layout from cluster %v. err = %s", clusterUrl, err)
	}

	replicaNodes := make([]string, 0, len(plan.Placement))
	for _, indexer := range plan.Placement {
		for _, index := range indexer.Indexes {
			if index.DefnId == defnId {
				replicaNodes = append(replicaNodes, indexer.NodeId)
			}
		}
	}

	return replicaNodes, nil
}

func ExecuteReplicaRepair(clusterUrl string, defnId common.IndexDefnId, increment int, nodes []string,
	override bool, enforceLimits bool, binSize uint64, enableShardAffinity bool) (*Solution, error) {

	plan, err := RetrievePlanFromCluster(clusterUrl, nodes, false)
	if err != nil {
		return nil, fmt.Errorf("Unable to read index layout from cluster %v. err = %s", clusterUrl, err)
	}

	var bucket, scope string
	if enforceLimits {
		for _, indexer := range plan.Placement {
			for _, index := range indexer.Indexes {
				if index.DefnId == defnId {
					bucket = index.Bucket
					scope = index.Scope
				}
			}
		}

		scopeLimit, err := GetIndexScopeLimit(clusterUrl, bucket, scope)
		if err != nil {
			return nil, err
		}

		if scopeLimit != collections.NUM_INDEXES_NIL {
			numIndexes := GetNumIndexesPerScope(plan, bucket, scope)

			if numIndexes+uint32(increment) > scopeLimit {
				errMsg := fmt.Sprintf("%v Limit : %v", common.ErrIndexScopeLimitReached.Error(), scopeLimit)
				return nil, errors.New(errMsg)
			}
		}
	}

	if override && len(nodes) != 0 {
		for _, indexer := range plan.Placement {
			found := false
			for _, node := range nodes {
				encryptedNodeAddr, _ := security.EncryptPortInAddr(node)
				if indexer.NodeId == node ||
					indexer.NodeId == encryptedNodeAddr {
					found = true
					break
				}
			}

			if found {
				indexer.UnsetExclude()
			} else {
				indexer.SetExclude("in")
			}
		}
	}

	config := DefaultRunConfig()
	config.Detail = logging.IsEnabled(logging.Info)
	config.Resize = false
	config.EnableShardAffinity = enableShardAffinity
	config.binSize = binSize

	p, err := replicaRepair(config, plan, defnId, increment)
	if p != nil && config.Detail {
		logging.Infof("************ Indexer Layout *************")
		p.Print()
		logging.Infof("****************************************")
	}

	if err != nil {
		return nil, err
	}

	return p.Result, nil
}

func ExecuteReplicaDrop(clusterUrl string, defnId common.IndexDefnId, nodes []string, numPartition int, decrement int, dropReplicaId int) (*Solution, []int, error) {

	plan, err := RetrievePlanFromCluster(clusterUrl, nodes, false)
	if err != nil {
		return nil, nil, fmt.Errorf("Unable to read index layout from cluster %v. err = %s", clusterUrl, err)
	}

	config := DefaultRunConfig()
	config.Detail = logging.IsEnabled(logging.Info)
	config.Resize = false

	p, original, result, err := replicaDrop(config, plan, defnId, numPartition, decrement, dropReplicaId)
	if p != nil && config.Detail {
		logging.Infof("************ Indexer Layout *************")
		p.Print()
		logging.Infof("****************************************")
	}

	if err != nil {
		return nil, nil, err
	}

	return original, result, nil
}

func ExecuteRetrieve(clusterUrl string, nodes []string, output string) (*Solution, error) {

	plan, err := RetrievePlanFromCluster(clusterUrl, nodes, false)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Unable to read index layout from cluster %v. err = %s", clusterUrl, err))
	}

	config := DefaultRunConfig()
	config.Detail = logging.IsEnabled(logging.Info)
	config.Resize = false
	config.Output = output

	return ExecuteRetrieveWithOptions(plan, config, nil)
}

func ExecuteRetrieveWithOptions(plan *Plan, config *RunConfig, params map[string]interface{}) (*Solution, error) {

	sizing := newGeneralSizingMethod()
	solution, constraint, initialIndexes, movedIndex, movedData := solutionFromPlan(CommandRebalance, config, sizing, plan)

	if params != nil && config.UseGreedyPlanner {
		var gub, ok, getUsage bool
		var gu interface{}
		gu, ok = params["getUsage"]
		if ok {
			gub, ok = gu.(bool)
			if ok {
				getUsage = gub
			}
		}

		if getUsage {

			if config.Shuffle != 0 {
				return nil, fmt.Errorf("Cannot use greedy planner as shuffle flag is set to %v", config.Shuffle)
			}

			// If indexes were moved for deciding initial placement, let the SAPlanner run
			if movedIndex != uint64(0) || movedData != uint64(0) {
				return nil, fmt.Errorf("Cannot use greedy planner as indxes/data has been moved (%v, %v)", movedIndex, movedData)
			}

			numNewReplica := 0
			nnr, ok := params["numNewReplica"]
			if ok {
				nnri, ok := nnr.(int)
				if ok {
					numNewReplica = nnri
				}
			}

			indexes := ([]*IndexUsage)(nil)

			// update runtime stats
			s := &RunStats{}
			setIndexPlacementStats(s, indexes, false)

			setInitialLayoutStats(s, config, constraint, solution, initialIndexes, movedIndex, movedData, false)

			// create placement method
			placement := newRandomPlacement(indexes, config.AllowSwap, false)

			// Compute Index Sizing info and add them to solution
			computeNewIndexSizingInfo(solution, indexes)

			// New cost mothod
			cost := newUsageBasedCostMethod(constraint, config.DataCostWeight, config.CpuCostWeight, config.MemCostWeight)

			// initialise greedy planner
			planner := newGreedyPlanner(cost, constraint, placement, sizing, indexes)

			// set numNewIndexes for size estimation
			planner.numNewIndexes = numNewReplica

			sol, err := planner.Initialise(CommandPlan, solution)
			if err != nil {
				return nil, err
			}

			solution = sol
		}
	}

	if config.Output != "" {
		if err := savePlan(config.Output, solution, constraint); err != nil {
			return nil, err
		}
	}

	return solution, nil
}

//////////////////////////////////////////////////////////////
// Execution
/////////////////////////////////////////////////////////////

func ExecutePlanWithOptions(plan *Plan, indexSpecs []*IndexSpec, detail bool, genStmt string,
	output string, addNode int, cpuQuota int, memQuota int64, allowUnpin bool, useLive bool,
	useGreedyPlanner, allowDDLDuringScaleup bool, binSize uint64, enableShardAffinity bool) (*Solution, error) {

	resize := false
	if plan == nil {
		resize = true
	}

	config := DefaultRunConfig()
	config.Detail = detail
	config.GenStmt = genStmt
	config.Resize = resize
	config.Output = output
	config.AddNode = addNode
	config.MemQuota = memQuota
	config.CpuQuota = cpuQuota
	config.AllowUnpin = allowUnpin
	config.UseLive = useLive
	config.UseGreedyPlanner = useGreedyPlanner
	config.AllowDDLDuringScaleup = allowDDLDuringScaleup
	config.EnableShardAffinity = enableShardAffinity
	config.binSize = binSize

	p, _, err := executePlan(config, CommandPlan, plan, indexSpecs, ([]string)(nil))
	if p != nil && detail {
		logging.Infof("************ Indexer Layout *************")
		p.Print()
		logging.Infof("****************************************")
	}

	if p != nil {
		return p.GetResult(), err
	}

	return nil, err
}

func ExecuteRebalanceWithOptions(plan *Plan, indexSpecs []*IndexSpec, detail bool, genStmt string,
	output string, addNode int, cpuQuota int, memQuota int64, allowUnpin bool,
	deletedNodes []string, binSize uint64, enableShardAffinity bool) (*Solution, error) {

	config := DefaultRunConfig()
	config.Detail = detail
	config.GenStmt = genStmt
	config.Resize = false
	config.Output = output
	config.AddNode = addNode
	config.MemQuota = memQuota
	config.CpuQuota = cpuQuota
	config.AllowUnpin = allowUnpin
	config.EnableShardAffinity = enableShardAffinity
	config.binSize = binSize

	p, _, _, err := executeRebal(config, CommandRebalance, plan, indexSpecs, deletedNodes, false)

	if detail {
		logging.Infof("************ Indexer Layout *************")
		p.PrintLayout()
		logging.Infof("****************************************")
	}

	if p != nil {
		return p.Result, err
	}

	return nil, err
}

func ExecuteSwapWithOptions(plan *Plan, detail bool, genStmt string,
	output string, addNode int, cpuQuota int, memQuota int64, allowUnpin bool,
	deletedNodes []string, binSize uint64, enableShardAffinity bool) (*Solution, error) {

	config := DefaultRunConfig()
	config.Detail = detail
	config.GenStmt = genStmt
	config.Resize = false
	config.Output = output
	config.AddNode = addNode
	config.MemQuota = memQuota
	config.CpuQuota = cpuQuota
	config.AllowUnpin = allowUnpin
	config.EnableShardAffinity = enableShardAffinity
	config.binSize = binSize

	p, _, _, err := executeRebal(config, CommandSwap, plan, nil, deletedNodes, false)

	if detail {
		logging.Infof("************ Indexer Layout *************")
		p.PrintLayout()
		logging.Infof("****************************************")
	}

	if p != nil {
		return p.Result, err
	}

	return nil, err
}

func executePlan(config *RunConfig, command CommandType, p *Plan, indexSpecs []*IndexSpec, deletedNodes []string) (Planner, *RunStats, error) {

	var indexes []*IndexUsage
	var err error

	sizing := newGeneralSizingMethod()

	if indexSpecs != nil {
		indexes, err = IndexUsagesFromSpec(sizing, indexSpecs)
		if err != nil {
			return nil, nil, err
		}
	} else {
		return nil, nil, errors.New("missing argument: index spec must be present")
	}

	return plan(config, p, indexes)
}

func executeRebal(config *RunConfig, command CommandType, p *Plan, indexSpecs []*IndexSpec, deletedNodes []string, isInternal bool) (
	*SAPlanner, *RunStats, map[string]map[common.IndexDefnId]*common.IndexDefn, error) {

	var indexes []*IndexUsage

	if p == nil {
		return nil, nil, nil, errors.New("missing argument: either workload or plan must be present")
	}

	return rebalance(command, config, p, indexes, deletedNodes, isInternal)
}

func plan(config *RunConfig, plan *Plan, indexes []*IndexUsage) (Planner, *RunStats, error) {

	var constraint ConstraintMethod
	var sizing SizingMethod
	var placement PlacementMethod
	var cost CostMethod

	var solution *Solution
	var initialIndexes []*IndexUsage

	sizing = newGeneralSizingMethod()

	// update runtime stats
	s := &RunStats{}
	setIndexPlacementStats(s, indexes, false)

	var movedIndex, movedData uint64

	// create a solution
	if plan != nil {
		// create a solution from plan
		solution, constraint, initialIndexes, movedIndex, movedData = solutionFromPlan(CommandPlan, config, sizing, plan)
		setInitialLayoutStats(s, config, constraint, solution, initialIndexes, movedIndex, movedData, false)

	} else {
		// create an empty solution
		solution, constraint = emptySolution(config, sizing, indexes)
	}

	// create placement method
	if plan != nil && config.AllowMove {
		// incremental placement with move
		total := ([]*IndexUsage)(nil)
		total = append(total, initialIndexes...)
		total = append(total, indexes...)
		total = filterPinnedIndexes(config, total)
		placement = newRandomPlacement(total, config.AllowSwap, false)
	} else {
		// initial placement
		indexes = filterPinnedIndexes(config, indexes)
		placement = newRandomPlacement(indexes, config.AllowSwap, false)
	}

	// Compute Index Sizing info and add them to solution
	computeNewIndexSizingInfo(solution, indexes)

	cost = newUsageBasedCostMethod(constraint, config.DataCostWeight, config.CpuCostWeight, config.MemCostWeight)

	planner, err := NewPlannerForCommandPlan(config, indexes, movedIndex, movedData, solution, cost, constraint, placement, sizing)
	if err != nil {
		return nil, nil, err
	}

	param := make(map[string]interface{})
	param["MinIterPerTemp"] = config.MinIterPerTemp
	param["MaxIterPerTemp"] = config.MaxIterPerTemp
	if err := planner.SetParam(param); err != nil {
		return nil, nil, err
	}

	planner.SetShardAffinity(config.EnableShardAffinity)

	// run planner
	if _, err := planner.Plan(CommandPlan, solution); err != nil {
		return nil, nil, err
	}

	// save result
	s.MemoryQuota = constraint.GetMemQuota()
	s.CpuQuota = constraint.GetCpuQuota()

	if config.Output != "" {
		if err := savePlan(config.Output, planner.GetResult(), constraint); err != nil {
			return nil, nil, err
		}
	}

	if config.GenStmt != "" {
		if err := genCreateIndexDDL(config.GenStmt, planner.GetResult()); err != nil {
			return nil, nil, err
		}
	}

	if config.EnableShardAffinity {
		PopulateAlternateShardIds(planner.GetResult(), indexes, config.binSize)
		UngroupIndexes(planner.GetResult())
	}

	return planner, s, nil
}

func prepareIndexNameToUsageMap(indexes []*IndexUsage, idxMap map[string]map[common.IndexDefnId]map[int]map[common.PartitionId][]*IndexUsage) {

	for _, index := range indexes {
		if index.Instance == nil || index.Instance.Pc == nil {
			continue
		}
		key := fmt.Sprintf("%v:%v:%v:%v", index.Bucket, index.Scope, index.Collection, index.Name)
		partnId := index.PartnId
		replicaId := index.Instance.ReplicaId
		defnId := index.DefnId
		if _, ok := idxMap[key]; !ok {
			idxMap[key] = make(map[common.IndexDefnId]map[int]map[common.PartitionId][]*IndexUsage)
		}

		if _, ok := idxMap[key][defnId]; !ok {
			idxMap[key][defnId] = make(map[int]map[common.PartitionId][]*IndexUsage)
		}

		if _, ok := idxMap[key][defnId][replicaId]; !ok {
			idxMap[key][defnId][replicaId] = make(map[common.PartitionId][]*IndexUsage)
		}

		if _, ok := idxMap[key][defnId][replicaId][partnId]; !ok {
			idxMap[key][defnId][replicaId][partnId] = make([]*IndexUsage, 0)
		}

		idxMap[key][defnId][replicaId][partnId] = append(idxMap[key][defnId][replicaId][partnId], index)
	}
}

func getIndexDefnsToRemove(idxMap map[string]map[common.IndexDefnId]map[int]map[common.PartitionId][]*IndexUsage,
	indexDefnIdToRemove map[common.IndexDefnId]bool) map[string]map[common.IndexDefnId]*common.IndexDefn {

	hostToIndexToRemove := make(map[string]map[common.IndexDefnId]*common.IndexDefn)
	for _, defnMap := range idxMap {
		if len(defnMap) > 1 {
			for _, replicas := range defnMap {
				for _, partitions := range replicas {
					for _, idxusages := range partitions {
						for _, idx := range idxusages {
							remove := indexDefnIdToRemove[idx.DefnId]
							if remove == false {
								continue
							}
							// if this index is to be removed add to hostToIndexToRemove map
							if idx.initialNode == nil {
								continue
							}
							if _, ok := hostToIndexToRemove[idx.initialNode.RestUrl]; !ok {
								hostToIndexToRemove[idx.initialNode.RestUrl] = make(map[common.IndexDefnId]*common.IndexDefn)
							}
							if _, ok := hostToIndexToRemove[idx.initialNode.RestUrl][idx.DefnId]; !ok {
								hostToIndexToRemove[idx.initialNode.RestUrl][idx.DefnId] = &idx.Instance.Defn
							}
						}
					}
				}
			}
		}
	}
	return hostToIndexToRemove
}

func getIndexStateValue(idxMap map[string]map[common.IndexDefnId]map[int]map[common.PartitionId][]*IndexUsage,
	key string, defnId common.IndexDefnId) int {
	// for partations of replica even if one of the partation is not fully formed
	// that replica can not be used for serving queries
	// Also if one replica is in active state that index can serve queries.
	// hence we will get the lowest state among partations of a replica as state of replica and
	// highest state of replica will be returned as state of index.

	// define the usefulness of index when comparing two equivalent indexes with INDEX_STATE_ACTIVE being most useful
	getIndexStateValue := func(state common.IndexState) int {
		switch state {
		case common.INDEX_STATE_DELETED, common.INDEX_STATE_ERROR, common.INDEX_STATE_NIL:
			return 0
		case common.INDEX_STATE_CREATED:
			return 1
		case common.INDEX_STATE_READY:
			return 2
		case common.INDEX_STATE_INITIAL:
			return 3
		case common.INDEX_STATE_CATCHUP:
			return 4
		case common.INDEX_STATE_ACTIVE:
			return 5
		}
		return 0
	}

	replicaMap := make(map[int]int) // replicaMap[replicaId]indexStateValue

	if _, ok := idxMap[key]; !ok {
		return 0
	}
	if _, ok := idxMap[key][defnId]; !ok {
		return 0
	}

	replicas := idxMap[key][defnId]
	for _, partitions := range replicas {
		for _, idxUsages := range partitions {
			for _, idx := range idxUsages {
				replicaId := idx.Instance.ReplicaId
				indexStateVal := getIndexStateValue(idx.state)
				if _, ok := replicaMap[replicaId]; !ok {
					replicaMap[replicaId] = indexStateVal
				} else {
					if replicaMap[replicaId] > indexStateVal { // lowest partition state becomes the state of replica.
						replicaMap[replicaId] = indexStateVal
					}
				}
			}
		}
	}

	state := int(0) // lowest state value
	for _, stateVal := range replicaMap {
		if stateVal > state {
			state = stateVal // return the highest state value among replicas
		}
	}
	return state
}

// return values calculated as per available partations in idxMap input param
func getNumDocsForDefn(idxMap map[string]map[common.IndexDefnId]map[int]map[common.PartitionId][]*IndexUsage, key string, defnId common.IndexDefnId) (int64, int64) {
	replicaMap := make(map[int][]int64)
	// we will calculate the numDocsPending as replicaMap[replicaId][0] and numDocsQueued as replicaMap[replicaId][1]
	// at each replica level and the replica with lesser total is the replica which is more uptodate.
	// hence we will return numDocs numbers for that replica.
	if _, ok := idxMap[key]; !ok {
		return int64(0), int64(0)
	}
	if _, ok := idxMap[key][defnId]; !ok {
		return int64(0), int64(0)
	}

	replicas := idxMap[key][defnId]
	for _, partitions := range replicas {
		for _, idxUsages := range partitions {
			for _, idx := range idxUsages {
				replicaId := idx.Instance.ReplicaId
				if _, ok := replicaMap[replicaId]; !ok {
					replicaMap[replicaId] = make([]int64, 2)
				}
				replicaMap[replicaId][0] = replicaMap[replicaId][0] + idx.numDocsPending
				replicaMap[replicaId][1] = replicaMap[replicaId][1] + idx.numDocsQueued
			}
		}
	}

	const MaxUint64 = ^uint64(0)
	const MaxInt64 = int64(MaxUint64 >> 1)
	var numDocsPending, numDocsQueued, numDocsTotal int64
	numDocsTotal = MaxInt64
	for _, docs := range replicaMap {
		numDocs := docs[0] + docs[1]
		if numDocs < numDocsTotal {
			numDocsTotal = numDocs
			numDocsPending, numDocsQueued = docs[0], docs[1]
		}
	}
	return numDocsPending, numDocsQueued
}

// return values calculated as per available partations in idxMap input param
func getNumReplicaForDefn(idxMap map[string]map[common.IndexDefnId]map[int]map[common.PartitionId][]*IndexUsage,
	key string, defnId common.IndexDefnId, numPartations uint32) (uint32, uint32) {

	missingReplicaCount := uint32(0) // total of missing partations across all replicas as observed on idxMap
	observedReplicaCount := uint32(0)

	if _, ok := idxMap[key]; !ok {
		return observedReplicaCount, missingReplicaCount
	}
	if _, ok := idxMap[key][defnId]; !ok {
		return observedReplicaCount, missingReplicaCount
	}

	replicas := idxMap[key][defnId]
	// get observed replica count for this defn
	observedReplicaCount = uint32(len(replicas))
	for replicaId, partitions := range replicas {
		if uint32(len(partitions)) > numPartations { // can this happen?
			logging.Warnf("getNumReplicaForDefn: found more than expected partations for index defn %v, replicaId %v, expected partations %v, observed partations %v",
				defnId, replicaId, numPartations, len(partitions))
			return uint32(0), uint32(0)
		} else {
			missingReplicaCount += (numPartations - uint32(len(partitions))) // missing = expected - observed
		}
	}
	return observedReplicaCount, missingReplicaCount
}

func getNumServerGroupsForDefn(idxMap map[string]map[common.IndexDefnId]map[int]map[common.PartitionId][]*IndexUsage,
	key string, defnId common.IndexDefnId) int {
	serverGroups := make(map[string]map[int]map[common.PartitionId]int)
	//number of serverGroups this index (pointed by defnId) and its replicas are part of is given by len(serverGroups) .
	if _, ok := idxMap[key]; !ok {
		return 0
	}
	if _, ok := idxMap[key][defnId]; !ok {
		return 0
	}

	replicas := idxMap[key][defnId]
	for _, partitions := range replicas {
		for _, idxUsages := range partitions {
			for _, idx := range idxUsages {
				sgroup := idx.initialNode.ServerGroup
				replicaId := idx.Instance.ReplicaId
				partnId := idx.PartnId
				if _, ok := serverGroups[sgroup]; !ok {
					serverGroups[sgroup] = make(map[int]map[common.PartitionId]int)
				}
				if _, ok := serverGroups[sgroup][replicaId]; !ok {
					serverGroups[sgroup][replicaId] = make(map[common.PartitionId]int)
				}
				if _, ok := serverGroups[sgroup][replicaId][partnId]; !ok {
					serverGroups[sgroup][replicaId][partnId] = 1
				} else {
					serverGroups[sgroup][replicaId][partnId] = serverGroups[sgroup][replicaId][partnId] + 1
				}
			}
		}
	}
	return len(serverGroups)
}

func hasDuplicateIndexes(idxMap map[string]map[common.IndexDefnId]map[int]map[common.PartitionId][]*IndexUsage) bool {
	var hasDuplicate bool
	for _, uniqueDefns := range idxMap {
		if len(uniqueDefns) > 1 { // there is at least one duplicate index entry for this index name
			hasDuplicate = true
			break
		}
	}
	return hasDuplicate
}

func selectDuplicateIndexesToRemove(idxMap map[string]map[common.IndexDefnId]map[int]map[common.PartitionId][]*IndexUsage) map[common.IndexDefnId]bool {

	indexDefnIdToRemove := make(map[common.IndexDefnId]bool)

	type indexToEliminate struct {
		defnId                  common.IndexDefnId
		numServerGroup          uint32 // observed count of server groups
		numReplica              uint32 // numReplica count as available in index defn
		numObservedReplicaCount uint32 // observed count of replicas
		numMissingReplica       uint32 // value calculated at index level for num missing partations across all replicas
		numDocsPending          int64  // calculated as max of numDocsPending from all the replicas
		numDocsQueued           int64  //  calculated as max of numDocsQueued from all the replica
		stateValue              int    // Index with higher state (even for one of the replica) is more useful
	}

	filterDupIndex := func(dupIndexes []*indexToEliminate, indexDefnIdToRemove map[common.IndexDefnId]bool) {
		eligibles := []*indexToEliminate{}
		for _, index := range dupIndexes {
			if indexDefnIdToRemove[index.defnId] == false { // if index is marked for removal already skip it.
				eligibles = append(eligibles, index)
			}
		}

		less := func(i, j int) bool { // comparision function
			if eligibles[i].stateValue == eligibles[j].stateValue {
				if eligibles[i].numServerGroup == eligibles[i].numServerGroup {
					if eligibles[i].numReplica == eligibles[i].numReplica {
						if eligibles[i].numMissingReplica == eligibles[j].numMissingReplica {
							numDocsTotal_i := eligibles[i].numDocsQueued + eligibles[i].numDocsPending
							numDocsTotal_j := eligibles[j].numDocsQueued + eligibles[j].numDocsPending
							if numDocsTotal_i == numDocsTotal_j {
								return true
							} else {
								return numDocsTotal_i < numDocsTotal_j
							}
						} else {
							return eligibles[i].numMissingReplica < eligibles[j].numMissingReplica
						}
					} else {
						return eligibles[i].numReplica > eligibles[i].numReplica
					}
				} else {
					return eligibles[i].numServerGroup > eligibles[i].numServerGroup
				}
			} else {
				return eligibles[i].stateValue > eligibles[j].stateValue
			}
		}

		if len(eligibles) > 1 {
			sort.Slice(eligibles, less)
			for i, index := range eligibles {
				if i == 0 { // keep the 0th element defn remove other indexes
					continue
				}
				indexDefnIdToRemove[index.defnId] = true
			}
		}
	}

	groupEquivalantDefns := func(defns []*common.IndexDefn) [][]*common.IndexDefn {
		//TODO... complexity of this function is n^2 see if this can be improved in separate patch
		equiList := make([][]*common.IndexDefn, 0)
		for {
			eql := []*common.IndexDefn{}
			k := -1
			progress := false
			for i := 0; i < len(defns); i++ {
				defn := defns[i]
				if defn == nil {
					continue
				}
				progress = true
				if k == -1 {
					eql = append(eql, defn)
					k = 0
					defns[i] = nil // we are done processing this defn
					continue
				}
				if common.IsEquivalentIndex(eql[k], defn) {
					eql = append(eql, defn)
					k++
					defns[i] = nil // we are done processing this defn
				}
			}
			if len(eql) > 0 {
				equiList = append(equiList, eql)
			}
			if progress == false {
				// we are done processing all defns
				break
			}
		}
		return equiList
	}

	for _, uniqueDefns := range idxMap {
		if len(uniqueDefns) <= 1 { // there are no duplicates for this index name skip it
			continue
		}
		// make 2d slice of equivalant index defns here
		// if one of the slice count is more than 1 we may have equivalent duplicae indexes
		// now find which index to keep / eliminate.
		defnList := make([]*common.IndexDefn, len(uniqueDefns))
		i := 0
		for _, replicas := range uniqueDefns {
		loop1: // we just need defn for this defnId which can be taken from one of the instance
			for _, partitions := range replicas {
				for _, idxUsages := range partitions {
					defnList[i] = &idxUsages[0].Instance.Defn
					i++
					break loop1
				}
			}
		}
		equiDefns := groupEquivalantDefns(defnList) // we got equivalent denfs grouped

		for _, defns := range equiDefns {
			if len(defns) <= 1 { // no duplicate equivalent indexes here move ahead
				continue
			}
			dupIndexes := []*indexToEliminate{}
			// out of equivalant defns now choose which one to keep and which ones to remove.
			for _, defn := range defns {
				key := fmt.Sprintf("%v:%v:%v:%v", defn.Bucket, defn.Scope, defn.Collection, defn.Name)
				numServerGroup := getNumServerGroupsForDefn(idxMap, key, defn.DefnId)
				numDocsPending, numDocsQueued := getNumDocsForDefn(idxMap, key, defn.DefnId)
				numObservedReplicaCount, missingReplicaCount := getNumReplicaForDefn(idxMap, key, defn.DefnId, defn.NumPartitions)
				stateVal := getIndexStateValue(idxMap, key, defn.DefnId)
				dupEntry := &indexToEliminate{
					defnId:                  defn.DefnId,
					numServerGroup:          uint32(numServerGroup),
					numReplica:              defn.NumReplica,
					numObservedReplicaCount: numObservedReplicaCount,
					numMissingReplica:       missingReplicaCount,
					numDocsPending:          numDocsPending,
					numDocsQueued:           numDocsQueued,
					stateValue:              stateVal,
				}
				dupIndexes = append(dupIndexes, dupEntry)
			}
			filterDupIndex(dupIndexes, indexDefnIdToRemove)
		}
	}
	return indexDefnIdToRemove
}

func filterIndexesToRemove(indexes []*IndexUsage, indexDefnIdToRemove map[common.IndexDefnId]bool) []*IndexUsage {
	toKeep := make([]*IndexUsage, 0)
	for _, index := range indexes {
		if indexDefnIdToRemove[index.DefnId] == true {
			continue
		}
		toKeep = append(toKeep, index)
	}
	return toKeep
}

// setExcludeInForNonEmptyNodes sets the Exclude parameter to "in"
// for all the non-empty/non-deleted nodes in the cluster.
// For rebalance, it means that any new/empty node
// would be the only eligible node(s) to receive new indexes.
// This helps to reduce the shuffling of indexes between existing nodes
// of the cluster.
// Important to note that replica repair can happen on any node in the cluster
// and is not restricted due to using the Exclude parameter.
func setExcludeInForNonEmptyNodes(s *Solution) {

	for _, indexer := range s.Placement {
		if len(indexer.Indexes) == 0 && !indexer.isDelete {
			//empty node skip
		} else {
			//non empty node. If Exclude is already set, skip.
			if indexer.Exclude == "" {
				logging.Infof("Planner::setExcludeInForNonEmptyNodes Set Exclude=in for %v", indexer.NodeId)
				indexer.SetExclude("in")
			}
		}
	}

}

func rebalance(command CommandType, config *RunConfig, plan *Plan, indexes []*IndexUsage, deletedNodes []string, isInternal bool) (
	*SAPlanner, *RunStats, map[string]map[common.IndexDefnId]*common.IndexDefn, error) {

	var constraint ConstraintMethod
	var sizing SizingMethod
	var placement PlacementMethod
	var cost CostMethod

	var solution *Solution
	var initialIndexes []*IndexUsage
	var outIndexes []*IndexUsage

	var err error
	var indexDefnsToRemove map[string]map[common.IndexDefnId]*common.IndexDefn

	s := &RunStats{}

	sizing = newGeneralSizingMethod()

	// create an initial solution
	if plan != nil {
		// create an initial solution from plan
		var movedIndex, movedData uint64
		solution, constraint, initialIndexes, movedIndex, movedData = solutionFromPlan(command, config, sizing, plan)
		setInitialLayoutStats(s, config, constraint, solution, initialIndexes, movedIndex, movedData, true)

	} else {
		// create an initial solution
		initialIndexes = indexes
		solution, constraint, err = initialSolution(config, sizing, initialIndexes)
		if err != nil {
			return nil, nil, nil, err
		}
		setInitialLayoutStats(s, config, constraint, solution, initialIndexes, 0, 0, false)
	}

	// change topology before rebalancing
	outIndexes, err = changeTopology(config, solution, deletedNodes)
	if err != nil {
		return nil, nil, nil, err
	}

	//if there is any empty node in the cluster, set ExcludeIn to true
	//for all the non-empty nodes.
	if solution.findNumEmptyNodes() > 0 {
		setExcludeInForNonEmptyNodes(solution)
	}

	//
	// create placement method
	// 1) Swap rebalancing:  If there are any nodes being ejected, only consider those indexes.
	// 2) General Rebalancing: If option EjectOnly is true, then this is no-op.  Otherwise consider all indexes in all nodes.
	//
	if len(outIndexes) != 0 {
		indexes = outIndexes
	} else if !config.EjectOnly {
		indexes = initialIndexes
	} else {
		indexes = nil
	}

	if isInternal {
		//map key is string bucket:scope:collection:name this is used to group duplicate indexUsages by defn/replica/partition
		indexNameToUsageMap := make(map[string]map[common.IndexDefnId]map[int]map[common.PartitionId][]*IndexUsage)
		prepareIndexNameToUsageMap(indexes, indexNameToUsageMap)
		for _, indexer := range solution.Placement {
			prepareIndexNameToUsageMap(indexer.Indexes, indexNameToUsageMap)
		}
		if hasDuplicateIndexes(indexNameToUsageMap) {
			indexDefnIdToRemove := selectDuplicateIndexesToRemove(indexNameToUsageMap)

			// build map of index definations to remove from each indexer this will be used to make drop index calls.
			indexDefnsToRemove = getIndexDefnsToRemove(indexNameToUsageMap, indexDefnIdToRemove)

			// remove identified duplicate indexes from the initial solution
			indexes = filterIndexesToRemove(indexes, indexDefnIdToRemove)
			for _, indexer := range solution.Placement {
				indexer.Indexes = filterIndexesToRemove(indexer.Indexes, indexDefnIdToRemove)
			}
		}
	}
	// run planner
	placement = newRandomPlacement(indexes, config.AllowSwap, command == CommandSwap)
	cost = newUsageBasedCostMethod(constraint, config.DataCostWeight, config.CpuCostWeight, config.MemCostWeight)
	planner := newSAPlanner(cost, constraint, placement, sizing)
	planner.SetTimeout(config.Timeout)
	planner.SetRuntime(config.Runtime)
	planner.SetVariationThreshold(config.Threshold)
	planner.SetShardAffinity(config.EnableShardAffinity)

	param := make(map[string]interface{})
	param["MinIterPerTemp"] = config.MinIterPerTemp
	param["MaxIterPerTemp"] = config.MaxIterPerTemp
	if err := planner.SetParam(param); err != nil {
		return nil, nil, nil, err
	}

	planner.SetCpuProfile(config.CpuProfile)
	if config.Detail {
		logging.Infof("************ Index Layout Before Rebalance *************")
		solution.PrintLayout()
		logging.Infof("****************************************")
	}
	if _, err := planner.Plan(command, solution); err != nil {
		return planner, s, indexDefnsToRemove, err
	}

	// save result
	s.MemoryQuota = constraint.GetMemQuota()
	s.CpuQuota = constraint.GetCpuQuota()

	if config.Output != "" {
		if err := savePlan(config.Output, planner.Result, constraint); err != nil {
			return nil, nil, nil, err
		}
	}

	if config.EnableShardAffinity {
		PopulateSiblingIndexForReplicaRepair(planner.GetResult(), config.binSize)
	}

	return planner, s, indexDefnsToRemove, nil
}

func replicaRepair(config *RunConfig, plan *Plan, defnId common.IndexDefnId, increment int) (*SAPlanner, error) {

	var constraint ConstraintMethod
	var sizing SizingMethod
	var placement PlacementMethod
	var cost CostMethod

	var solution *Solution

	// create an initial solution from plan
	sizing = newGeneralSizingMethod()
	solution, constraint, _, _, _ = solutionFromPlan(CommandRepair, config, sizing, plan)

	// run planner
	placement = newRandomPlacement(nil, config.AllowSwap, false)
	cost = newUsageBasedCostMethod(constraint, config.DataCostWeight, config.CpuCostWeight, config.MemCostWeight)
	planner := newSAPlanner(cost, constraint, placement, sizing)
	planner.SetTimeout(config.Timeout)
	planner.SetRuntime(config.Runtime)
	planner.SetVariationThreshold(config.Threshold)
	planner.SetCpuProfile(config.CpuProfile)
	planner.SetShardAffinity(config.EnableShardAffinity)

	for _, indexer := range solution.Placement {
		for _, index := range indexer.Indexes {
			if index.DefnId == defnId {
				index.Instance.Defn.NumReplica += uint32(increment)
			}
		}
	}

	if config.Detail {
		logging.Infof("************ Index Layout Before Rebalance *************")
		solution.PrintLayout()
		logging.Infof("****************************************")
	}

	if _, err := planner.Plan(CommandRepair, solution); err != nil {
		return planner, err
	}

	return planner, nil
}

func replicaDrop(config *RunConfig, plan *Plan, defnId common.IndexDefnId, numPartition int, decrement int, dropReplicaId int) (*SAPlanner, *Solution, []int, error) {

	var constraint ConstraintMethod
	var sizing SizingMethod
	var placement PlacementMethod
	var cost CostMethod

	var solution *Solution
	var err error
	var result []int
	var original *Solution

	// create an initial solution from plan
	sizing = newGeneralSizingMethod()
	solution, constraint, _, _, _ = solutionFromPlan(CommandDrop, config, sizing, plan)

	// run planner
	placement = newRandomPlacement(nil, config.AllowSwap, false)
	cost = newUsageBasedCostMethod(constraint, config.DataCostWeight, config.CpuCostWeight, config.MemCostWeight)
	planner := newSAPlanner(cost, constraint, placement, sizing)
	planner.SetTimeout(config.Timeout)
	planner.SetRuntime(config.Runtime)
	planner.SetVariationThreshold(config.Threshold)
	planner.SetCpuProfile(config.CpuProfile)

	if config.Detail {
		logging.Infof("************ Index Layout Before Drop Replica *************")
		solution.PrintLayout()
		logging.Infof("****************************************")
	}

	original, result, err = planner.DropReplica(solution, defnId, numPartition, decrement, dropReplicaId)
	return planner, original, result, err
}

// ExecutePlan2 is the entry point for tenant aware planner
// for integration with metadata provider.
func ExecutePlan2(clusterUrl string, indexSpec *IndexSpec, nodes []string,
	serverlessIndexLimit uint32) (*Solution, bool, error) {

	plan, err := RetrievePlanFromCluster(clusterUrl, nodes, false)
	if err != nil {
		return nil, false, errors.New(fmt.Sprintf("Unable to read index layout "+
			"from cluster %v. err = %s", clusterUrl, err))
	}

	numIndexes := GetNumIndexesPerBucket(plan, indexSpec.Bucket)
	if numIndexes >= serverlessIndexLimit {
		errMsg := fmt.Sprintf("%v Limit : %v", common.ErrIndexBucketLimitReached.Error(), serverlessIndexLimit)
		return nil, false, errors.New(errMsg)
	}

	if err = verifyDuplicateIndex(plan, []*IndexSpec{indexSpec}); err != nil {
		return nil, false, err
	}

	tenantAwarePlanner, schedIndex, err := executeTenantAwarePlan(plan, indexSpec)

	if tenantAwarePlanner != nil {
		return tenantAwarePlanner.GetResult(), schedIndex, nil
	} else {
		return nil, schedIndex, err
	}

}

func GetNumIndexesPerBucket(plan *Plan, Bucket string) uint32 {
	var numIndexes uint32 = 0
	indexSet := make(map[common.IndexDefnId]bool)
	for _, indexernode := range plan.Placement {
		for _, index := range indexernode.Indexes {
			if index.Bucket == Bucket {
				if _, found := indexSet[index.DefnId]; !found {
					indexSet[index.DefnId] = true
					numIndexes = numIndexes + 1
				}
			}
		}
	}
	return numIndexes
}

//executeTenantAwarePlan implements the tenant aware planning logic based on
//the following rules:
//1. All indexer nodes will be grouped into sub-clusters of 2 nodes each.
//2. Each node in a sub-cluster belongs to a different server group.
//3. All indexes will be created with 1 replica.
//4. Index(and its replica) follow symmetrical distribution in a sub-cluster.
//5. Indexes belonging to a tenant(bucket) will be mapped to a single sub-cluster.
//6. Index of a new tenant can be placed on a sub-cluster with memory usage
//   lower than LWM(Low Watermark Threshold).
//7. Index of an existing tenant can be placed on a sub-cluster with memory usage
//   lower than HWM(High Watermark Threshold).
//8. No Index can be placed on a node with memory usage above HWM(High Watermark Threshold).

func executeTenantAwarePlan(plan *Plan, indexSpec *IndexSpec) (Planner, bool, error) {

	const _executeTenantAwarePlan = "Planner::executeTenantAwarePlan"

	solution := solutionFromPlan2(plan)

	subClusters, err := groupIndexNodesIntoSubClusters(solution.Placement)
	if err != nil {
		if err == common.ErrRebalanceOrCleanupPending {
			//create index can be scheduled in background
			return nil, true /*schedIndex*/, nil
		} else {
			return nil, false, err
		}
	}

	err = validateSubClusterGrouping(subClusters, indexSpec)
	if err != nil {
		return nil, false, err
	}

	logging.Infof("%v Found SubClusters  %v", _executeTenantAwarePlan, subClusters)

	tenantSubCluster, err := filterCandidateBasedOnTenantAffinity(subClusters, indexSpec)
	if err != nil {
		return nil, false, err
	}

	logging.Infof("%v Found Candidate Based on Tenant Affinity %v", _executeTenantAwarePlan, tenantSubCluster)

	var result SubCluster
	var errStr string
	if len(tenantSubCluster) == 0 {

		//exclude partial subclusters except if there is only 1 node left in the cluster(MB-54706)
		if len(subClusters) != 1 {
			subClusters = filterPartialSubClusters(subClusters)
		}

		if len(subClusters) == 0 {
			errStr = "Unable to find any valid SubCluster"
		} else {
			//No subCluster found with matching tenant. Choose any subCluster
			//below LWM for memory. Ignore units usage as regulator will throttle
			//beyond tenant limit.

			usageThreshold := *plan.UsageThreshold

			usageThreshold.UnitsHighThreshold = 100 //ignore units usage
			usageThreshold.UnitsLowThreshold = 100  //ignore units usage

			subClustersBelowLWM, err := findSubClustersBelowLowThreshold(subClusters, &usageThreshold)
			if err != nil {
				return nil, false, err
			}

			if len(subClustersBelowLWM) != 0 {
				result = findLeastLoadedSubCluster(subClustersBelowLWM)
			} else {
				errStr = "No SubCluster Below Low Usage Threshold"
			}
		}
	} else {
		//Found subCluster with matching tenant. Choose this subCluster
		//if below HWM for memory. Units usage can be high. It will be throttled
		//as per tenant limit.
		usageThreshold := *plan.UsageThreshold

		usageThreshold.UnitsHighThreshold = 100 //ignore units usage
		usageThreshold.UnitsLowThreshold = 100  //ignore units usage

		subClusterBelowHWM, err := findSubClustersBelowHighThreshold([]SubCluster{tenantSubCluster}, &usageThreshold)
		if err != nil {
			return nil, false, err
		}
		if len(subClusterBelowHWM) != 0 {
			result = subClusterBelowHWM[0]
		} else {
			errStr = "Tenant SubCluster Above High Usage Threshold"
		}
	}

	if len(result) == 0 {
		logging.Infof("%v Found no matching candidate for tenant %v. Error - %v", _executeTenantAwarePlan,
			indexSpec, errStr)
		retErr := "Planner not able to find any node for placement - " + errStr
		return nil, false, errors.New(retErr)
	} else {
		logging.Infof("%v Found Result %v", _executeTenantAwarePlan, result)
	}

	indexes, err := IndexUsagesFromSpec(nil, []*IndexSpec{indexSpec})
	if err != nil {
		return nil, false, err
	}

	err = placeIndexOnSubCluster(result, indexes)
	if err != nil {
		return nil, false, err
	}

	tenantAwarePlanner := &TenantAwarePlanner{Result: solution}

	return tenantAwarePlanner, false, nil

}

// solutionFromPlan2 creates a Solution from the input Plan.
// This function does a shallow copy of the Placement from the Plan
// to create a Solution.
func solutionFromPlan2(plan *Plan) *Solution {

	s := &Solution{
		Placement: make([]*IndexerNode, len(plan.Placement)),
	}

	for i, indexer := range plan.Placement {
		for _, index := range indexer.Indexes {
			index.initialNode = indexer
		}
		s.Placement[i] = indexer
	}

	return s

}

// groupIndexNodesIntoSubClusters groups index nodes into sub-clusters.
// Nodes are considered to be part of a sub-cluster if those are hosting
// replicas of the same index. Empty nodes can be paired into a sub-cluster
// if those belong to a different server group.
func groupIndexNodesIntoSubClusters(indexers []*IndexerNode) ([]SubCluster, error) {

	const _groupIndexNodesIntoSubClusters = "Planner::groupIndexNodesIntoSubClusters"

	var err error
	var subClusters []SubCluster

	emptyNodesWithoutPairs := make(map[string]*IndexerNode)
	emptyNodesPaired := make(map[string]*IndexerNode)

	for _, node := range indexers {
		var subcluster SubCluster

		if node.IsDeleted() {
			logging.Infof("%v Skip Deleted Index Node %v SG %v Memory %v Units %v", _groupIndexNodesIntoSubClusters,
				node.NodeId, node.ServerGroup, node.MandatoryQuota, node.ActualUnits)
			continue
		}

		//skip empty nodes
		if len(node.Indexes) == 0 {
			//Number of indexes can also be 0 in case there are buckets being moved
			//out of the node(as planner will skip indexes with lower instance version)
			//or into the node with RState as pending.
			//Such node should not be considered empty. Return error as planner cannot
			//determine node grouping in this case as the node is not
			//empty and should not be paired with any other empty node.
			if len(node.BucketsInRebalance) != 0 {
				logging.Infof("%v Cannot group empty node %v due to in progress rebalance "+
					"for all buckets on the node. %v", _groupIndexNodesIntoSubClusters, node,
					node.BucketsInRebalance)
				return nil, common.ErrRebalanceOrCleanupPending
			}
			emptyNodesWithoutPairs[node.NodeUUID] = node
			logging.Debugf("%v Added %v to emptyNodesWithoutPairs %v", _groupIndexNodesIntoSubClusters,
				node, emptyNodesWithoutPairs)
			continue
		}

		logging.Infof("%v Index Node %v SG %v Memory %v Units %v Tenants %v", _groupIndexNodesIntoSubClusters,
			node.NodeId, node.ServerGroup, node.MandatoryQuota, node.ActualUnits, node.NumTenants)

		if checkIfNodeBelongsToAnySubCluster(subClusters, node) {
			continue
		}

		for _, index := range node.Indexes {
			if inRebal, ok := node.BucketsInRebalance[index.Bucket]; ok && inRebal {
				logging.Infof("%v Skip index %v node %v from subCluster grouping. "+
					"Rebalance in progress.", _groupIndexNodesIntoSubClusters, index, node)
				continue
			}
			subcluster, err = findSubClusterForIndex(indexers, index)
			if err != nil {
				return nil, err
			}
			//In case of missing replica, subCluster may be smaller than the expected size.
			//Try with all the indexes on the node.
			if len(subcluster) == cSubClusterLen {
				break
			}
		}

		//empty subcluster means that the indexes were skipped due to all buckets on
		//the node being in rebalance.
		if len(subcluster) == 0 {
			logging.Infof("%v Cannot group node %v due to in progress rebalance "+
				"for all buckets on the node. %v", _groupIndexNodesIntoSubClusters, node,
				node.BucketsInRebalance)
			return nil, common.ErrRebalanceOrCleanupPending
		}

		if len(subcluster) == 1 {
			var pairNode *IndexerNode
			subcluster, pairNode, err = findPairForSingleNodeSubCluster(indexers, subcluster, subClusters)
			if err != nil {
				return nil, err
			}
			if pairNode != nil {
				emptyNodesPaired[pairNode.NodeUUID] = pairNode
				logging.Debugf("%v Added %v to emptyNodesPaired %v", _groupIndexNodesIntoSubClusters,
					pairNode, emptyNodesPaired)
			}
		}

		subClusters = append(subClusters, subcluster)
	}

	//remove the already paired empty nodes
	for nodeUUID, _ := range emptyNodesPaired {
		delete(emptyNodesWithoutPairs, nodeUUID)
	}

	if len(emptyNodesWithoutPairs) == 0 {
		return subClusters, nil
	}

	//It is possible to have a server group assignment which fails to pair all nodes
	//e.g. n1[sg1], n2[sg2]. CP adds n3[sg2], n4[sg3]. This can be paired as (n1, n3) and
	//(n2, n4). But if n1 gets paired with n4 first i.e. (n1, n4), then (n2, n3) cannot happen
	//as both belong to the same server group sg2.
	emptyNodeList := make([]*IndexerNode, 0, len(emptyNodesWithoutPairs))

	for _, node := range emptyNodesWithoutPairs {
		emptyNodeList = append(emptyNodeList, node)
	}

	//sort for deterministic output
	sort.Slice(emptyNodeList, func(i, j int) bool {
		return emptyNodeList[i].NodeUUID > emptyNodeList[j].NodeUUID
	})

	emptyNodePermutations := permuteNodes(emptyNodeList)
	found := false
	for _, emptyNodePerm := range emptyNodePermutations {
		if validateServerGroupForEmptyNodes(emptyNodePerm) {
			emptyNodeList = emptyNodePerm
			found = true
			break
		}
	}
	if found {
		//form sub-clusters for the valid permutation
		var subCluster SubCluster
		for _, node := range emptyNodeList {
			subCluster = append(subCluster, node)
			if len(subCluster) == cSubClusterLen {
				subClusters = append(subClusters, subCluster)
				subCluster = nil
			}
		}
		//append the last single node subcluster if available
		if len(subCluster) == 1 {
			subClusters = append(subClusters, subCluster)
		}

	} else {
		//otherwise add empty nodes as single node subcluster
		var subCluster SubCluster
		for _, node := range emptyNodeList {
			subCluster = append(subCluster, node)
			subClusters = append(subClusters, subCluster)
			subCluster = nil
		}
	}

	return subClusters, nil
}

// validateServerGroupForEmptyNodes validates if the input nodes can form sub-cluster
// without violating server group constraint. Each node in odd position is considered
// to be a pair with the next even position node starting with 0 index.
func validateServerGroupForEmptyNodes(emptyNodePermutations []*IndexerNode) bool {

	if len(emptyNodePermutations) == 1 {
		return true
	}

	for i := 0; i < len(emptyNodePermutations); i += 2 {

		//length is odd
		if i >= len(emptyNodePermutations)-1 {
			continue
		}

		if emptyNodePermutations[i].ServerGroup == emptyNodePermutations[i+1].ServerGroup {
			return false
		}

	}

	return true

}

// validateSubClusterGrouping validates the constraints for
// sub-cluster grouping.
func validateSubClusterGrouping(subClusters []SubCluster,
	indexSpec *IndexSpec) error {

	const _validateSubClusterGrouping = "Planner::validateSubClusterGrouping"

	for _, subCluster := range subClusters {

		//Number of nodes in the subCluster must not be greater than
		//number of replicas. It can be less than number of replicas in
		//case of a failed over node.
		if len(subCluster) > int(indexSpec.Replica) {
			errStr := fmt.Sprintf(" SubCluster %v has more nodes than number of replicas %v.", subCluster, indexSpec.Replica)
			logging.Errorf("%v %v", _validateSubClusterGrouping, errStr)
			return errors.New(common.ErrPlannerConstraintViolation.Error() + errStr)
		}
		sgMap := make(map[string]bool)
		for _, indexer := range subCluster {
			//All nodes in a sub-cluster must belong to a different server group
			if _, ok := sgMap[indexer.ServerGroup]; ok {
				errStr := fmt.Sprintf(" SubCluster %v has multiple nodes in the same server group.", subCluster)
				logging.Errorf("%v %v", _validateSubClusterGrouping, errStr)
				return errors.New(common.ErrPlannerConstraintViolation.Error() + errStr)
			} else {
				sgMap[indexer.ServerGroup] = true
			}

			//all nodes must belong to a single subcluster only
			result := findSubClustersForNode(subClusters, indexer)
			if len(result) > 1 {
				errStr := fmt.Sprintf(" Node %v belongs to multiple subclusters %v.", indexer, result)
				logging.Errorf("%v %v", _validateSubClusterGrouping, errStr)
				return errors.New(common.ErrPlannerConstraintViolation.Error() + errStr)
			}
		}
	}

	return nil
}

// filterCandidateBasedOnTenantAffinity finds candidate sub-cluster
// on which input index can be placed based on tenant affinity
func filterCandidateBasedOnTenantAffinity(subClusters []SubCluster,
	indexSpec *IndexSpec) (SubCluster, error) {

	var candidates []SubCluster
	var result SubCluster

	for _, subCluster := range subClusters {
		for _, indexer := range subCluster {
			for _, index := range indexer.Indexes {
				if index.Bucket == indexSpec.Bucket {
					candidates = append(candidates, subCluster)
				}
			}
		}
	}

	//One or more matching subCluster found. Choose least loaded subCluster
	//from the matching ones. For phase 1, there will be only 1 matching
	//sub-cluster as a single tenant's indexes can only be placed on one node.
	if len(candidates) >= 1 {
		result = findLeastLoadedSubCluster(candidates)
		return result, nil
	}

	return nil, nil

}

// findLeastLoadedSubCluster finds the least loaded sub-cluster from the input
// list of sub-clusters. Load is calculated based on the actual RSS of the indexer
// process on the node.
func findLeastLoadedSubCluster(subClusters []SubCluster) SubCluster {

	//TODO This code assumes identically loaded nodes in the sub-cluster.
	//When indexes move during rebalance, this may not always be true.

	var leastLoad uint64
	var result SubCluster

	for _, subCluster := range subClusters {
		for _, indexer := range subCluster {
			if leastLoad == 0 || indexer.MandatoryQuota < leastLoad {
				leastLoad = indexer.MandatoryQuota
				result = subCluster
			}
			break
		}
	}

	return result

}

// findCandidateSubClustersBasedOnUsage finds candidates sub-clusters for
// placement based on resource usage. All sub-clusters below High Threshold
// are potential candidates for index placement.
func findCandidateSubClustersBasedOnUsage(subClusters []SubCluster,
	usageThreshold *UsageThreshold) ([]SubCluster, error) {

	candidates, err := findSubClustersBelowHighThreshold(subClusters,
		usageThreshold)
	if err != nil {
		return nil, err
	}

	if len(candidates) == 0 {
		return nil, common.ErrPlannerMaxResourceUsageLimit
	}

	return candidates, nil
}

// findSubClustersBelowLowThreshold finds sub-clusters with usage lower than
// LWM(Low Watermark Threshold). A subCluster is considered below LWM if usage for
// both units and memory is below threshold.
func findSubClustersBelowLowThreshold(subClusters []SubCluster,
	usageThreshold *UsageThreshold) ([]SubCluster, error) {

	var result []SubCluster
	var found bool

	memQuota := usageThreshold.MemQuota
	unitsQuota := usageThreshold.UnitsQuota

	for _, subCluster := range subClusters {
		for _, indexNode := range subCluster {

			//exclude deleted nodes
			if indexNode.IsDeleted() {
				found = false
				break
			}

			//all nodes in the sub-cluster need to satisfy the condition
			found = true
			if indexNode.MandatoryQuota >=
				(uint64(usageThreshold.MemLowThreshold)*memQuota)/100 {
				found = false
				break
			}
			if indexNode.ActualUnits >=
				(uint64(usageThreshold.UnitsLowThreshold)*unitsQuota)/100 {
				found = false
				break
			}
		}
		if found {
			result = append(result, subCluster)
		}
	}
	return result, nil
}

// findSubClustersBelowHighThreshold finds sub-clusters with usage lower than
// HWT(High Watermark Threshold). A subCluster is considered below high
// threhsold if both units and memory usage is below HWM.
func findSubClustersBelowHighThreshold(subClusters []SubCluster,
	usageThreshold *UsageThreshold) ([]SubCluster, error) {

	var result []SubCluster
	var found bool

	memQuota := usageThreshold.MemQuota
	unitsQuota := usageThreshold.UnitsQuota

	for _, subCluster := range subClusters {
		found = true
		for _, indexNode := range subCluster {

			//exclude deleted nodes
			if indexNode.IsDeleted() {
				found = false
				break
			}

			//all nodes in the sub-cluster need to satisfy the condition
			if indexNode.MandatoryQuota >
				(uint64(usageThreshold.MemHighThreshold)*memQuota)/100 {
				found = false
				break
			}
			if indexNode.ActualUnits >
				(uint64(usageThreshold.UnitsHighThreshold)*unitsQuota)/100 {
				found = false
				break
			}
		}
		if found {
			result = append(result, subCluster)
		}
	}
	return result, nil
}

// findSubClusterForEmptyNode finds another empty node in the cluster to
// pair with the input single node subCluster to form a sub-cluster. If excludeNodes is specified,
// those nodes are excluded while finding the pair.
func findPairForSingleNodeSubCluster(indexers []*IndexerNode, subCluster SubCluster,
	excludeNodes []SubCluster) (SubCluster, *IndexerNode, error) {

	if len(subCluster) != 1 {
		return nil, nil, nil
	}

	node := subCluster[0]
	for _, indexer := range indexers {
		if node.IsDeleted() {
			continue
		}
		if indexer.NodeUUID != node.NodeUUID &&
			indexer.ServerGroup != node.ServerGroup &&
			len(indexer.Indexes) == 0 {

			if !checkIfNodeBelongsToAnySubCluster(excludeNodes, indexer) {
				subCluster = append(subCluster, indexer)
				return subCluster, indexer, nil

			}
		}
	}
	return subCluster, nil, nil
}

// findSubClusterForEmptyNode finds another empty node in the cluster to
// pair with the input node to form a sub-cluster. If excludeNodes is specified,
// those nodes are excluded while finding the pair.
func findSubClusterForEmptyNode(indexers []*IndexerNode,
	node *IndexerNode, excludeNodes []SubCluster) (SubCluster, error) {

	var subCluster SubCluster
	for _, indexer := range indexers {
		if indexer.NodeUUID != node.NodeUUID &&
			indexer.ServerGroup != node.ServerGroup &&
			len(indexer.Indexes) == 0 {

			if !checkIfNodeBelongsToAnySubCluster(excludeNodes, indexer) {
				subCluster = append(subCluster, indexer)
				subCluster = append(subCluster, node)
				return subCluster, nil

			}
		}
	}

	//create a single node subcluster. Such a situation can happen if one
	//node fails over in a subcluster. Replica will be created via repair
	//during the next rebalance.
	if len(subCluster) == 0 {
		subCluster = append(subCluster, node)
	}

	return subCluster, nil
}

// checkIfNodeBelongsToAnySubCluster checks if the given node belongs
// to the list of input sub-cluster based on its NodeUUID
func checkIfNodeBelongsToAnySubCluster(subClusters []SubCluster,
	node *IndexerNode) bool {

	for _, subCluster := range subClusters {

		for _, subNode := range subCluster {
			if node.NodeUUID == subNode.NodeUUID {
				return true
			}
		}
	}

	return false
}

// findSubClustersForNode returns the list of subClusters
// an input node belongs.
func findSubClustersForNode(subClusters []SubCluster,
	node *IndexerNode) []SubCluster {

	var result []SubCluster

	for _, subCluster := range subClusters {

		for _, subNode := range subCluster {
			if node.NodeUUID == subNode.NodeUUID {
				result = append(result, subCluster)
			}
		}
	}

	return result
}

// getSubClusterPosForNode returns the position of "node"
// in the input "subClusters" slice
func getSubClusterPosForNode(subClusters []SubCluster,
	nodeId string) int {

	for index, subCluster := range subClusters {

		for _, subNode := range subCluster {
			if nodeId == subNode.NodeUUID {
				return index
			}
		}
	}

	return -1
}

// findSubClusterForIndex finds the sub-cluster for a given index.
// Nodes are considered to be part of a sub-cluster if
// those are hosting replicas of the same index.
func findSubClusterForIndex(indexers []*IndexerNode,
	index *IndexUsage) (SubCluster, error) {

	var subCluster SubCluster
	for _, indexer := range indexers {
		if indexer.IsDeleted() {
			continue
		}
		for _, checkIdx := range indexer.Indexes {
			if index.DefnId == checkIdx.DefnId &&
				index.PartnId == checkIdx.PartnId {
				subCluster = append(subCluster, indexer)
			}
		}
	}

	return subCluster, nil
}

// placeIndexOnSubCluster places the input list of indexes on the given sub-cluster
func placeIndexOnSubCluster(subCluster SubCluster, indexes []*IndexUsage) error {

	for i, indexer := range subCluster {
		for _, index := range indexes {
			if index.Instance.ReplicaId == i {
				indexer.AddIndexes([]*IndexUsage{index})
			}
		}
	}
	return nil
}

//////////////////////////////////////////////////////////////
// Generate DDL
/////////////////////////////////////////////////////////////

//
// CREATE INDEX [index_name] ON named_keyspace_ref( expression [ , expression ] * )
//   WHERE filter_expressions
//   [ USING GSI | VIEW ]
//   [ WITH { "nodes": [ "node_name" ], "defer_build":true|false } ];
// BUILD INDEX ON named_keyspace_ref(index_name[,index_name]*) USING GSI;
//

func CreateIndexDDL(solution *Solution) string {

	if solution == nil {
		return ""
	}

	replicas := make(map[common.IndexDefnId][]*IndexerNode)
	newIndexDefns := make(map[common.IndexDefnId]*IndexUsage)
	buckets := make(map[string][]*IndexUsage)
	collections := make(map[string]map[string]map[string][]*IndexUsage)

	for _, indexer := range solution.Placement {
		for _, index := range indexer.Indexes {
			if index.initialNode == nil && index.Instance != nil {
				if _, ok := newIndexDefns[index.DefnId]; !ok {
					newIndexDefns[index.DefnId] = index
					if index.IndexOnCollection() {
						bucket := index.Bucket
						scope := index.Scope
						collection := index.Collection
						if _, ok := collections[bucket]; !ok {
							collections[bucket] = make(map[string]map[string][]*IndexUsage)
						}

						if _, ok := collections[bucket][scope]; !ok {
							collections[bucket][scope] = make(map[string][]*IndexUsage)
						}

						if _, ok := collections[bucket][scope][collection]; !ok {
							collections[bucket][scope][collection] = make([]*IndexUsage, 0)
						}

						collections[bucket][scope][collection] = append(collections[bucket][scope][collection], index)
					} else {
						buckets[index.Bucket] = append(buckets[index.Bucket], index)
					}
				}
				replicas[index.DefnId] = append(replicas[index.DefnId], indexer)
			}
		}
	}

	var stmts string

	// create index
	for _, index := range newIndexDefns {

		index.Instance.Defn.Nodes = make([]string, len(replicas[index.DefnId]))
		for i, indexer := range replicas[index.DefnId] {
			index.Instance.Defn.Nodes[i] = indexer.NodeId
		}
		index.Instance.Defn.Deferred = true

		numPartitions := index.Instance.Pc.GetNumPartitions()
		stmt := common.IndexStatement(index.Instance.Defn, numPartitions, int(index.Instance.Defn.NumReplica), true) + ";\n"

		stmts += stmt
	}

	buildStmt := "BUILD INDEX ON `%v`("
	buildStmtCollection := "BUILD INDEX ON `%v`.`%v`.`%v`("

	// build index
	for bucket, indexes := range buckets {
		stmt := fmt.Sprintf(buildStmt, bucket)
		for i := 0; i < len(indexes); i++ {
			stmt += "`" + indexes[i].Instance.Defn.Name + "`"
			if i < len(indexes)-1 {
				stmt += ","
			}
		}
		stmt += ");\n"

		stmts += stmt
	}

	for bucket, scopes := range collections {
		for scope, colls := range scopes {
			for collection, indexes := range colls {
				stmt := fmt.Sprintf(buildStmtCollection, bucket, scope, collection)
				for i := 0; i < len(indexes); i++ {
					stmt += "`" + indexes[i].Instance.Defn.Name + "`"
					if i < len(indexes)-1 {
						stmt += ","
					}
				}
				stmt += ");\n"

				stmts += stmt
			}
		}
	}
	stmts += "\n"

	return stmts
}

func genCreateIndexDDL(ddl string, solution *Solution) error {

	if solution == nil || ddl == "" {
		return nil
	}

	if stmts := CreateIndexDDL(solution); len(stmts) > 0 {
		if err := iowrap.Ioutil_WriteFile(ddl, ([]byte)(stmts), os.ModePerm); err != nil {
			return errors.New(fmt.Sprintf("Unable to write DDL statements into %v. err = %s", ddl, err))
		}
	}

	return nil
}

//////////////////////////////////////////////////////////////
// RunConfig
/////////////////////////////////////////////////////////////

func DefaultRunConfig() *RunConfig {

	return &RunConfig{
		Detail:                false,
		GenStmt:               "",
		MemQuotaFactor:        1.0,
		CpuQuotaFactor:        1.0,
		Resize:                true,
		MaxNumNode:            int(math.MaxInt16),
		Output:                "",
		Shuffle:               0,
		AllowMove:             false,
		AllowSwap:             true,
		AllowUnpin:            false,
		AddNode:               0,
		DeleteNode:            0,
		MaxMemUse:             -1,
		MaxCpuUse:             -1,
		MemQuota:              -1,
		CpuQuota:              -1,
		DataCostWeight:        1,
		CpuCostWeight:         1,
		MemCostWeight:         1,
		EjectOnly:             false,
		DisableRepair:         false,
		MinIterPerTemp:        100,
		MaxIterPerTemp:        20000,
		AllowDDLDuringScaleup: false,
		binSize:               common.DEFAULT_BIN_SIZE, // 2.5G
	}
}

//////////////////////////////////////////////////////////////
// Indexer Nodes Generation
/////////////////////////////////////////////////////////////

func indexerNodes(constraint ConstraintMethod, indexes []*IndexUsage, sizing SizingMethod, useLive bool) []*IndexerNode {

	rs := rand.New(rand.NewSource(time.Now().UnixNano()))

	quota := constraint.GetMemQuota()

	total := uint64(0)
	for _, index := range indexes {
		total += index.GetMemTotal(useLive)
	}

	numOfIndexers := int(total / quota)
	var indexers []*IndexerNode
	for i := 0; i < numOfIndexers; i++ {
		nodeId := strconv.FormatUint(uint64(rs.Uint32()), 10)
		indexers = append(indexers, newIndexerNode(nodeId, sizing))
	}

	return indexers
}

func indexerNode(rs *rand.Rand, prefix string, sizing SizingMethod) *IndexerNode {
	nodeId := strconv.FormatUint(uint64(rs.Uint32()), 10)
	return newIndexerNode(prefix+nodeId, sizing)
}

//////////////////////////////////////////////////////////////
// Solution Generation
/////////////////////////////////////////////////////////////

func initialSolution(config *RunConfig,
	sizing SizingMethod,
	indexes []*IndexUsage) (*Solution, ConstraintMethod, error) {

	resize := config.Resize
	maxNumNode := config.MaxNumNode
	maxCpuUse := config.MaxCpuUse
	maxMemUse := config.MaxMemUse
	// initialSolution is only called from Plan
	allowDDLDuringScaleup := configureAllowDDLDuringScaleup(CommandPlan, config.AllowDDLDuringScaleup)

	memQuota, cpuQuota := computeQuota(config, sizing, indexes, false)

	constraint := newIndexerConstraint(memQuota, cpuQuota, resize, maxNumNode, maxCpuUse, maxMemUse)

	indexers := indexerNodes(constraint, indexes, sizing, false)

	r := newSolution(constraint, sizing, indexers, false, false, config.DisableRepair, allowDDLDuringScaleup)

	placement := newRandomPlacement(indexes, config.AllowSwap, false)
	if err := placement.InitialPlace(r, indexes); err != nil {
		return nil, nil, err
	}

	return r, constraint, nil
}

func emptySolution(config *RunConfig,
	sizing SizingMethod,
	indexes []*IndexUsage) (*Solution, ConstraintMethod) {

	resize := config.Resize
	maxNumNode := config.MaxNumNode
	maxCpuUse := config.MaxCpuUse
	maxMemUse := config.MaxMemUse
	// emptySolution is only called from CommandPlan
	allowDDLDuringScaleup := configureAllowDDLDuringScaleup(CommandPlan, config.AllowDDLDuringScaleup)

	memQuota, cpuQuota := computeQuota(config, sizing, indexes, false)

	constraint := newIndexerConstraint(memQuota, cpuQuota, resize, maxNumNode, maxCpuUse, maxMemUse)

	r := newSolution(constraint, sizing, ([]*IndexerNode)(nil), false, false, config.DisableRepair, allowDDLDuringScaleup)

	return r, constraint
}

func solutionFromPlan(command CommandType, config *RunConfig, sizing SizingMethod,
	plan *Plan) (*Solution, ConstraintMethod, []*IndexUsage, uint64, uint64) {

	memQuotaFactor := config.MemQuotaFactor
	cpuQuotaFactor := config.CpuQuotaFactor
	resize := config.Resize
	maxNumNode := config.MaxNumNode
	shuffle := config.Shuffle
	maxCpuUse := config.MaxCpuUse
	maxMemUse := config.MaxMemUse
	useLive := config.UseLive || command == CommandRebalance || command == CommandSwap || command == CommandRepair || command == CommandDrop
	allowDDLDuringScaleup := configureAllowDDLDuringScaleup(command, config.AllowDDLDuringScaleup)

	movedData := uint64(0)
	movedIndex := uint64(0)

	indexes := ([]*IndexUsage)(nil)

	for _, indexer := range plan.Placement {
		for _, index := range indexer.Indexes {
			index.initialNode = indexer
			indexes = append(indexes, index)
		}
	}

	memQuota, cpuQuota := computeQuota(config, sizing, indexes, plan.IsLive && useLive)

	if config.MemQuota == -1 && plan.MemQuota != 0 {
		memQuota = uint64(float64(plan.MemQuota) * memQuotaFactor)
	}

	if config.CpuQuota == -1 && plan.CpuQuota != 0 {
		cpuQuota = uint64(float64(plan.CpuQuota) * cpuQuotaFactor)
	}

	constraint := newIndexerConstraint(memQuota, cpuQuota, resize, maxNumNode, maxCpuUse, maxMemUse)

	r := newSolution(constraint, sizing, plan.Placement, plan.IsLive, useLive, config.DisableRepair, allowDDLDuringScaleup)
	r.calculateSize() // in case sizing formula changes after the plan is saved
	r.usedReplicaIdMap = plan.UsedReplicaIdMap

	if shuffle != 0 {
		placement := newRandomPlacement(indexes, config.AllowSwap, false)
		movedIndex, movedData = placement.randomMoveNoConstraint(r, shuffle)
	}

	for _, indexer := range r.Placement {
		for _, index := range indexer.Indexes {
			index.initialNode = indexer
		}
	}

	return r, constraint, indexes, movedIndex, movedData
}

func computeQuota(config *RunConfig, sizing SizingMethod, indexes []*IndexUsage, useLive bool) (uint64, uint64) {

	memQuotaFactor := config.MemQuotaFactor
	cpuQuotaFactor := config.CpuQuotaFactor

	memQuota := uint64(config.MemQuota)
	cpuQuota := uint64(config.CpuQuota)

	imemQuota, icpuQuota := sizing.ComputeMinQuota(indexes, useLive)

	if config.MemQuota == -1 {
		memQuota = imemQuota
	}
	memQuota = uint64(float64(memQuota) * memQuotaFactor)

	if config.CpuQuota == -1 {
		cpuQuota = icpuQuota
	}
	cpuQuota = uint64(float64(cpuQuota) * cpuQuotaFactor)

	return memQuota, cpuQuota
}

// This function is only called during placement to make existing index as
// eligible candidate for planner.
func filterPinnedIndexes(config *RunConfig, indexes []*IndexUsage) []*IndexUsage {

	result := ([]*IndexUsage)(nil)
	for _, index := range indexes {

		if config.AllowUnpin || (index.initialNode != nil && index.initialNode.isDelete) {
			index.Hosts = nil
		}

		if len(index.Hosts) == 0 {
			result = append(result, index)
		}
	}

	return result
}

// Find all partitioned index in teh solution
func findAllPartitionedIndex(solution *Solution) []*IndexUsage {

	result := ([]*IndexUsage)(nil)

	for _, indexer := range solution.Placement {
		for _, index := range indexer.Indexes {
			if index.Instance != nil && common.IsPartitioned(index.Instance.Defn.PartitionScheme) {
				result = append(result, index)
			}
		}
	}

	return result
}

func findAllPartitionedIndexExcluding(solution *Solution, excludes []*IndexUsage) []*IndexUsage {

	result := ([]*IndexUsage)(nil)

	for _, indexer := range solution.Placement {
		for _, index := range indexer.Indexes {
			if index.Instance != nil && common.IsPartitioned(index.Instance.Defn.PartitionScheme) {
				found := false
				for _, exclude := range excludes {
					if exclude == index {
						found = true
						break
					}
				}
				if !found {
					result = append(result, index)
				}
			}
		}
	}

	return result
}

// This function should only be called if there are new indexes to be placed.
func computeNewIndexSizingInfo(s *Solution, indexes []*IndexUsage) {

	for _, index := range indexes {
		index.ComputeSizing(s.UseLiveData(), s.sizing)
	}
}

//////////////////////////////////////////////////////////////
// Topology Change
/////////////////////////////////////////////////////////////

func changeTopology(config *RunConfig, solution *Solution, deletedNodes []string) ([]*IndexUsage, error) {

	outNodes := ([]*IndexerNode)(nil)
	outNodeIds := ([]string)(nil)
	inNodeIds := ([]string)(nil)
	outIndexes := ([]*IndexUsage)(nil)

	if len(deletedNodes) != 0 {

		if len(deletedNodes) > len(solution.Placement) {
			return nil, errors.New("The number of node in cluster is smaller than the number of node to be deleted.")
		}

		for _, nodeId := range deletedNodes {

			candidate := solution.findMatchingIndexer(nodeId)
			if candidate == nil {
				return nil, errors.New(fmt.Sprintf("Cannot find to-be-deleted indexer in solution: %v", nodeId))
			}

			candidate.isDelete = true
			outNodes = append(outNodes, candidate)
		}

		for _, outNode := range outNodes {
			outNodeIds = append(outNodeIds, outNode.String())
			for _, index := range outNode.Indexes {
				outIndexes = append(outIndexes, index)
			}
		}

		logging.Tracef("Nodes to be removed : %v", outNodeIds)
	}

	if config.AddNode != 0 {
		rs := rand.New(rand.NewSource(time.Now().UnixNano()))

		for i := 0; i < config.AddNode; i++ {
			indexer := indexerNode(rs, "newNode-", solution.getSizingMethod())
			solution.Placement = append(solution.Placement, indexer)
			inNodeIds = append(inNodeIds, indexer.String())
		}
		logging.Tracef("Nodes to be added: %v", inNodeIds)
	}

	return outIndexes, nil
}

//////////////////////////////////////////////////////////////
// RunStats
/////////////////////////////////////////////////////////////

// Set stats for index to be placed
func setIndexPlacementStats(s *RunStats, indexes []*IndexUsage, useLive bool) {
	s.AvgIndexSize, s.StdDevIndexSize = computeIndexMemStats(indexes, useLive)
	s.AvgIndexCpu, s.StdDevIndexCpu = computeIndexCpuStats(indexes, useLive)
	s.IndexCount = uint64(len(indexes))
}

// Set stats for initial layout
func setInitialLayoutStats(s *RunStats,
	config *RunConfig,
	constraint ConstraintMethod,
	solution *Solution,
	initialIndexes []*IndexUsage,
	movedIndex uint64,
	movedData uint64,
	useLive bool) {

	s.Initial_avgIndexerSize, s.Initial_stdDevIndexerSize = solution.ComputeMemUsage()
	s.Initial_avgIndexerCpu, s.Initial_stdDevIndexerCpu = solution.ComputeCpuUsage()
	s.Initial_avgIndexSize, s.Initial_stdDevIndexSize = computeIndexMemStats(initialIndexes, useLive)
	s.Initial_avgIndexCpu, s.Initial_stdDevIndexCpu = computeIndexCpuStats(initialIndexes, useLive)
	s.Initial_indexCount = uint64(len(initialIndexes))
	s.Initial_indexerCount = uint64(len(solution.Placement))

	initial_cost := newUsageBasedCostMethod(constraint, config.DataCostWeight, config.CpuCostWeight, config.MemCostWeight)
	s.Initial_score = initial_cost.Cost(solution)

	s.Initial_movedIndex = movedIndex
	s.Initial_movedData = movedData
}

//////////////////////////////////////////////////////////////
// Index Generation (from Index Spec)
/////////////////////////////////////////////////////////////

func IndexUsagesFromSpec(sizing SizingMethod, specs []*IndexSpec) ([]*IndexUsage, error) {

	var indexes []*IndexUsage
	for _, spec := range specs {
		usages, err := indexUsageFromSpec(sizing, spec)
		if err != nil {
			return nil, err
		}
		indexes = append(indexes, usages...)
	}

	return indexes, nil
}

func indexUsageFromSpec(sizing SizingMethod, spec *IndexSpec) ([]*IndexUsage, error) {

	var result []*IndexUsage

	uuid, err := common.NewUUID()
	if err != nil {
		return nil, errors.New("unable to generate UUID")
	}

	config, err := common.GetSettingsConfig(common.SystemConfig)
	if err != nil {
		logging.Errorf("Error from retrieving indexer settings. Error = %v", err)
		return nil, err
	}

	if len(spec.PartitionScheme) == 0 {
		spec.PartitionScheme = common.SINGLE
	}

	if spec.NumPartition == 0 {
		spec.NumPartition = 1
		if common.IsPartitioned(common.PartitionScheme(spec.PartitionScheme)) {
			spec.NumPartition = uint64(config["indexer.numPartitions"].Int())
		}
	}

	var startPartnId int
	if common.IsPartitioned(common.PartitionScheme(spec.PartitionScheme)) {
		startPartnId = 1
	}

	if len(spec.Using) == 0 {
		spec.Using = common.MemoryOptimized
	}

	defnId := spec.DefnId
	if spec.DefnId == 0 {
		defnId = common.IndexDefnId(uuid.Uint64())
	}

	for i := 0; i < int(spec.Replica); i++ {
		instId := common.IndexInstId(uuid.Uint64())
		for j := 0; j < int(spec.NumPartition); j++ {
			index := &IndexUsage{}
			index.DefnId = defnId
			index.InstId = instId
			index.PartnId = common.PartitionId(startPartnId + j)
			index.Name = spec.Name
			index.Bucket = spec.Bucket
			index.Scope = spec.Scope
			index.Collection = spec.Collection
			index.StorageMode = spec.Using
			index.IsPrimary = spec.IsPrimary

			index.Instance = &common.IndexInst{}
			index.Instance.InstId = index.InstId
			index.Instance.ReplicaId = i
			index.Instance.Pc = common.NewKeyPartitionContainer(int(spec.NumPartition),
				common.PartitionScheme(spec.PartitionScheme), common.HashScheme(spec.HashScheme))
			index.Instance.State = common.INDEX_STATE_READY
			index.Instance.Stream = common.NIL_STREAM
			index.Instance.Error = ""
			index.Instance.Version = 0
			index.Instance.RState = common.REBAL_ACTIVE

			index.Instance.Defn.DefnId = defnId
			index.Instance.Defn.Name = index.Name
			index.Instance.Defn.Bucket = spec.Bucket
			index.Instance.Defn.Scope = spec.Scope
			index.Instance.Defn.Collection = spec.Collection
			index.Instance.Defn.IsPrimary = spec.IsPrimary
			index.Instance.Defn.SecExprs = spec.SecExprs
			index.Instance.Defn.WhereExpr = spec.WhereExpr
			index.Instance.Defn.Immutable = spec.Immutable
			index.Instance.Defn.IsArrayIndex = spec.IsArrayIndex
			index.Instance.Defn.RetainDeletedXATTR = spec.RetainDeletedXATTR
			index.Instance.Defn.Deferred = spec.Deferred
			index.Instance.Defn.Desc = spec.Desc
			index.Instance.Defn.IndexMissingLeadingKey = spec.IndexMissingLeadingKey
			index.Instance.Defn.NumReplica = uint32(spec.Replica) - 1
			index.Instance.Defn.PartitionScheme = common.PartitionScheme(spec.PartitionScheme)
			index.Instance.Defn.PartitionKeys = spec.PartitionKeys
			index.Instance.Defn.NumDoc = spec.NumDoc / uint64(spec.NumPartition)
			index.Instance.Defn.DocKeySize = spec.DocKeySize
			index.Instance.Defn.SecKeySize = spec.SecKeySize
			index.Instance.Defn.ArrSize = spec.ArrSize
			index.Instance.Defn.ResidentRatio = spec.ResidentRatio
			index.Instance.Defn.ExprType = common.ExprType(spec.ExprType)
			if index.Instance.Defn.ResidentRatio == 0 {
				index.Instance.Defn.ResidentRatio = 100
			}

			index.NumOfDocs = spec.NumDoc / uint64(spec.NumPartition)
			index.AvgDocKeySize = spec.DocKeySize
			index.AvgSecKeySize = spec.SecKeySize
			index.AvgArrKeySize = spec.ArrKeySize
			index.AvgArrSize = spec.ArrSize
			index.ResidentRatio = spec.ResidentRatio
			index.MutationRate = spec.MutationRate
			index.ScanRate = spec.ScanRate

			// This is need to compute stats for new indexes
			// The index size will be recomputed later on in plan/rebalance
			if sizing != nil {
				sizing.ComputeIndexSize(index)
			}

			result = append(result, index)
		}
	}

	return result, nil
}

//////////////////////////////////////////////////////////////
// Plan
/////////////////////////////////////////////////////////////

func printPlanSummary(plan *Plan) {

	if plan == nil {
		return
	}

	logging.Infof("--------------------------------------")
	logging.Infof("Mem Quota:	%v", formatMemoryStr(plan.MemQuota))
	logging.Infof("Cpu Quota:	%v", plan.CpuQuota)
	logging.Infof("--------------------------------------")
}

func savePlan(output string, solution *Solution, constraint ConstraintMethod) error {

	plan := &Plan{
		Placement: solution.Placement,
		MemQuota:  constraint.GetMemQuota(),
		CpuQuota:  constraint.GetCpuQuota(),
		IsLive:    solution.isLiveData,
	}

	data, err := json.MarshalIndent(plan, "", "	")
	if err != nil {
		return errors.New(fmt.Sprintf("Unable to save plan into %v. err = %s", output, err))
	}

	err = iowrap.Ioutil_WriteFile(output, data, os.ModePerm)
	if err != nil {
		return errors.New(fmt.Sprintf("Unable to save plan into %v. err = %s", output, err))
	}

	return nil
}

func ReadPlan(planFile string) (*Plan, error) {

	if planFile != "" {

		plan := &Plan{}

		buf, err := iowrap.Ioutil_ReadFile(planFile)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Unable to read plan from %v. err = %s", planFile, err))
		}

		if err := json.Unmarshal(buf, plan); err != nil {
			return nil, errors.New(fmt.Sprintf("Unable to parse plan from %v. err = %s", planFile, err))
		}

		return plan, nil
	}

	return nil, nil
}

//////////////////////////////////////////////////////////////
// IndexSpec
/////////////////////////////////////////////////////////////

func ReadIndexSpecs(specFile string) ([]*IndexSpec, error) {

	if specFile != "" {

		var specs []*IndexSpec

		buf, err := iowrap.Ioutil_ReadFile(specFile)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Unable to read index spec from %v. err = %s", specFile, err))
		}

		if err := json.Unmarshal(buf, &specs); err != nil {
			return nil, errors.New(fmt.Sprintf("Unable to parse index spec from %v. err = %s", specFile, err))
		}

		return specs, nil
	}

	return nil, nil
}

//////////////////////////////////////////////////////////////
// IndexSpec
/////////////////////////////////////////////////////////////

func ReadDefragUtilStats(statsFile string) (map[string]map[string]interface{}, error) {

	if statsFile != "" {

		var stats map[string]map[string]interface{}

		buf, err := iowrap.Ioutil_ReadFile(statsFile)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Unable to read index spec from %v. err = %s", statsFile, err))
		}

		if err := json.Unmarshal(buf, &stats); err != nil {
			return nil, errors.New(fmt.Sprintf("Unable to parse index spec from %v. err = %s", statsFile, err))
		}

		return stats, nil
	}

	return nil, nil
}

// ExecuteTenantAwareRebalance is the entry point for tenant aware rebalancer.
// Given an input cluster url and the requested topology change, it will return
// the set of transfer tokens for the desired index movements.
func ExecuteTenantAwareRebalance(clusterUrl string,
	topologyChange service.TopologyChange,
	masterId string) (map[string]*common.TransferToken,
	map[string]map[common.IndexDefnId]*common.IndexDefn, error) {
	runtime := time.Now()
	return ExecuteTenantAwareRebalanceInternal(clusterUrl, topologyChange, masterId, &runtime)
}

func ExecuteTenantAwareRebalanceInternal(clusterUrl string,
	topologyChange service.TopologyChange,
	masterId string, runtime *time.Time) (map[string]*common.TransferToken,
	map[string]map[common.IndexDefnId]*common.IndexDefn, error) {

	plan, err := RetrievePlanFromCluster(clusterUrl, nil, true)
	if err != nil {
		return nil, nil, errors.New(fmt.Sprintf("Unable to read index layout from cluster %v. err = %s", clusterUrl, err))
	}

	nodes := make(map[string]string)
	for _, node := range plan.Placement {
		nodes[node.NodeUUID] = node.NodeId
	}

	deleteNodes := make([]string, len(topologyChange.EjectNodes))
	for i, node := range topologyChange.EjectNodes {
		if _, ok := nodes[string(node.NodeID)]; !ok {
			return nil, nil, errors.New(fmt.Sprintf("Unable to find indexer node with node UUID %v", node.NodeID))
		}
		deleteNodes[i] = nodes[string(node.NodeID)]
	}

	// make sure we have all the keep nodes
	for _, node := range topologyChange.KeepNodes {
		if _, ok := nodes[string(node.NodeInfo.NodeID)]; !ok {
			return nil, nil, errors.New(fmt.Sprintf("Unable to find indexer node with node UUID %v", node.NodeInfo.NodeID))
		}
	}

	p, _, err := executeTenantAwareRebal(CommandRebalance, plan, deleteNodes)
	if p != nil {
		logging.Infof("************ Indexer Layout *************")
		p.Print()
		logging.Infof("****************************************")
	}

	if err != nil {
		return nil, nil, err
	}

	filterSolution(p.Result.Placement)

	transferTokens, err := genShardTransferToken(p.Result, masterId,
		topologyChange, deleteNodes)
	if err != nil {
		return nil, nil, err
	}

	return transferTokens, nil, nil
}

// executeTenantAwareRebal is the actual implementation of tenant aware rebalancer
func executeTenantAwareRebal(command CommandType, plan *Plan, deletedNodes []string) (
	*TenantAwarePlanner, map[string]map[common.IndexDefnId]*common.IndexDefn, error) {

	const _executeTenantAwareRebal = "Planner::executeTenantAwareRebal"

	solution := solutionFromPlan2(plan)
	tenantAwarePlanner := &TenantAwarePlanner{Result: solution}

	// update the number of new and deleted nodes in the solution

	var err error
	err = solution.markDeletedNodes(deletedNodes)
	if err != nil {
		return nil, nil, err
	}
	solution.numDeletedNode = solution.findNumDeleteNodes()
	solution.numNewNode = solution.findNumEmptyNodes()
	solution.markNewNodes()

	//find placement for indexes of deleted indexer nodes
	if solution.numDeletedNode > 0 {
		err = findPlacementForDeletedNodes(solution, plan.UsageThreshold)
		if err != nil {
			return nil, nil, err
		}
	}

	//group indexer nodes into subclusters
	allSubClusters, err := groupIndexNodesIntoSubClusters(solution.Placement)
	if err != nil {
		return nil, nil, err
	}

	logging.Infof("%v Found SubClusters  %v", _executeTenantAwareRebal, allSubClusters)

	//repair missing replica indexes
	repairMissingReplica(solution, allSubClusters)

	subClustersOverHWM, err := findSubClusterAboveHighThreshold(allSubClusters,
		plan.UsageThreshold)
	if err != nil {
		return nil, nil, err
	}

	logging.Infof("%v Found SubClusters above HWM %v", _executeTenantAwareRebal, subClustersOverHWM)

	//if no subClusters above HWM, no index movement is required
	if len(subClustersOverHWM) == 0 {
		return tenantAwarePlanner, nil, nil
	}

	tenantsToBeMoved := findCandidateTenantsToMoveOut(subClustersOverHWM, plan.UsageThreshold)

	for i, tenants := range tenantsToBeMoved {
		logging.Infof("%v TenantsToBeMoved from source %v", _executeTenantAwareRebal, subClustersOverHWM[i])
		for _, tenant := range tenants {
			logging.Infof("%v %v", _executeTenantAwareRebal, tenant)
		}
	}
	if len(tenantsToBeMoved) == 0 {
		return tenantAwarePlanner, nil, nil
	}

	subClustersBelowLWM, err := findSubClustersBelowLowThreshold(allSubClusters,
		plan.UsageThreshold)
	if err != nil {
		return nil, nil, err
	}

	subClustersBelowLWM = filterPartialSubClusters(subClustersBelowLWM)

	logging.Infof("%v Found SubClusters Below LWM %v", _executeTenantAwareRebal, subClustersBelowLWM)

	if len(subClustersBelowLWM) == 0 {
		return tenantAwarePlanner, nil, nil
	}

	subClustersBelowLWM = sortSubClustersByMemUsage(subClustersBelowLWM)

	moveTenantsToLowUsageSubCluster(solution, tenantsToBeMoved, subClustersBelowLWM,
		subClustersOverHWM, plan.UsageThreshold)

	//provide stats with the final solution based on the new memory/units usage
	return tenantAwarePlanner, nil, nil
}

// findSubClusterAboveHighThreshold finds sub-clusters with usage higher than
// HWM(High Watermark Threshold). If either memory or units is above HWM, then
// the subcluster is considered to be above HWM.
func findSubClusterAboveHighThreshold(subClusters []SubCluster,
	usageThreshold *UsageThreshold) ([]SubCluster, error) {

	var result []SubCluster
	var found bool

	memQuota := usageThreshold.MemQuota
	unitsQuota := usageThreshold.UnitsQuota

	for _, subCluster := range subClusters {
		for _, indexNode := range subCluster {
			//if any node in the subCluster is above HWM,
			//it is considered above HWM
			found = false
			if indexNode.MandatoryQuota >
				(uint64(usageThreshold.MemHighThreshold)*memQuota)/100 {
				found = true
				break
			}
			if indexNode.ActualUnits >
				(uint64(usageThreshold.UnitsHighThreshold)*unitsQuota)/100 {
				found = true
				break
			}
		}
		if found {
			result = append(result, subCluster)
		}
	}
	return result, nil
}

// findCandidateTenantsToMoveOut computes the tenants that
// can be moved out of the list of input SubClusters to bring down its
// memory/units usage less than or equal to LWM threshold.
func findCandidateTenantsToMoveOut(subClusters []SubCluster,
	usageThreshold *UsageThreshold) [][]*TenantUsage {

	var candidates [][]*TenantUsage
	for _, subCluster := range subClusters {
		tenantList := findTenantsToMoveOutFromSubCluster(subCluster, usageThreshold)
		if len(tenantList) != 0 {
			candidates = append(candidates, tenantList)
		}
	}

	return candidates
}

// findTenantsToMoveOutFromSubCluster computes the tenants that
// can be moved out of the input SubCluster to bring down its
// memory/units usage less than or equal to LWM threshold.
func findTenantsToMoveOutFromSubCluster(subCluster SubCluster,
	usageThreshold *UsageThreshold) []*TenantUsage {

	const _findTenantsToMoveOutFromSubCluster = "Planner::findTenantsToMoveOutFromSubCluster"

	var maxMemoryUsage, maxUnitsUsage uint64
	var lowThresholdMemoryUsage, lowThresholdUnitsUsage uint64
	var highestUsageNode *IndexerNode

	memQuota := usageThreshold.MemQuota
	unitsQuota := usageThreshold.UnitsQuota

	//TODO Elixir What if the highestUsageNode doesn't have enough units usage to move
	//unitsToBeMoved
	for _, indexNode := range subCluster {
		if indexNode.MandatoryQuota > maxMemoryUsage {
			maxMemoryUsage = indexNode.MandatoryQuota
			highestUsageNode = indexNode
		}
		if indexNode.ActualUnits > maxUnitsUsage {
			maxUnitsUsage = indexNode.ActualUnits
		}

		if lowThresholdMemoryUsage == 0 {
			lowThresholdMemoryUsage = (uint64(usageThreshold.MemLowThreshold) * memQuota) / 100
		}

		if lowThresholdUnitsUsage == 0 {
			lowThresholdUnitsUsage = (uint64(usageThreshold.UnitsLowThreshold) * unitsQuota) / 100
		}
	}

	var memoryToBeMoved, unitsToBeMoved uint64
	if maxMemoryUsage > lowThresholdMemoryUsage {
		memoryToBeMoved = maxMemoryUsage - lowThresholdMemoryUsage
	}
	if maxUnitsUsage > lowThresholdUnitsUsage {
		unitsToBeMoved = maxUnitsUsage - lowThresholdUnitsUsage
	}

	//compute usage by tenant
	usagePerTenant := computeUsageByTenant(highestUsageNode)

	//sort the usage from lowest to highest
	usageSortedByMemory := sortTenantUsageByMemory(usagePerTenant)
	usageSortedByUnits := sortTenantUsageByUnits(usagePerTenant)

	mem_iter := 0
	units_iter := 0

	processedTenants := make(map[string]bool)

	//last tenant shouldn't be evicted
	maxEvict := len(usagePerTenant) - 1
	numEvict := 0

	for (memoryToBeMoved > 0 || unitsToBeMoved > 0) && (numEvict < maxEvict) {

		var memCandidate, unitsCandidate *TenantUsage
		if memoryToBeMoved > 0 {
			memCandidate = usageSortedByMemory[mem_iter]
			if _, found := processedTenants[memCandidate.TenantId]; !found {
				if memoryToBeMoved > memCandidate.MemoryUsage {
					memoryToBeMoved -= memCandidate.MemoryUsage
				} else {
					memoryToBeMoved = 0
				}
				if unitsToBeMoved > memCandidate.UnitsUsage {
					unitsToBeMoved -= memCandidate.UnitsUsage
				} else {
					unitsToBeMoved = 0
				}
				processedTenants[memCandidate.TenantId] = true
				numEvict++
			}
			mem_iter++
		}

		if unitsToBeMoved > 0 && (numEvict < maxEvict) {
			unitsCandidate = usageSortedByUnits[units_iter]
			if _, found := processedTenants[unitsCandidate.TenantId]; !found {
				if unitsToBeMoved > unitsCandidate.UnitsUsage {
					unitsToBeMoved -= unitsCandidate.UnitsUsage
				} else {
					unitsToBeMoved = 0
				}
				if memoryToBeMoved > unitsCandidate.MemoryUsage {
					memoryToBeMoved -= unitsCandidate.MemoryUsage
				} else {
					memoryToBeMoved = 0
				}
				processedTenants[unitsCandidate.TenantId] = true
				numEvict++
			}
			units_iter++
		}
	}

	usageSortedByMemory = usageSortedByMemory[:mem_iter]
	usageSortedByUnits = usageSortedByUnits[:units_iter]

	//reverse sort from highest to lowest usage. The algorithm
	//will try to move tenant based on highest usage first so
	//it has better chances of being able to find a slot on the target.
	sort.Slice(usageSortedByMemory, func(i, j int) bool {
		return usageSortedByMemory[i].MemoryUsage > usageSortedByMemory[j].MemoryUsage
	})
	sort.Slice(usageSortedByUnits, func(i, j int) bool {
		return usageSortedByUnits[i].UnitsUsage > usageSortedByUnits[j].UnitsUsage
	})

	//create combined usage
	combinedUsage := make([]*TenantUsage, 0)
	processedTenants = make(map[string]bool)

	maxlen := len(usageSortedByMemory)
	if maxlen < len(usageSortedByUnits) {
		maxlen = len(usageSortedByUnits)
	}

	var candidate *TenantUsage
	for i := 0; i < maxlen; i++ {

		if i < len(usageSortedByMemory) {
			candidate = usageSortedByMemory[i]
			if _, found := processedTenants[candidate.TenantId]; !found {
				combinedUsage = append(combinedUsage, candidate)
				processedTenants[candidate.TenantId] = true
			}
		}

		if i < len(usageSortedByUnits) {
			candidate = usageSortedByUnits[i]
			if _, found := processedTenants[candidate.TenantId]; !found {
				combinedUsage = append(combinedUsage, candidate)
				processedTenants[candidate.TenantId] = true
			}
		}
	}

	return combinedUsage

}

// computeUsageByTenant computes the actual memory and units usage
// per tenant on a given indexer node. Returns a map of TenantUsage
// indexed by tenantId.
func computeUsageByTenant(indexerNode *IndexerNode) map[string]*TenantUsage {

	usagePerTenant := make(map[string]*TenantUsage)
	for _, index := range indexerNode.Indexes {

		if usage, ok := usagePerTenant[index.Bucket]; ok {
			usage.MemoryUsage += index.ActualMemUsage
			usage.UnitsUsage += index.ActualUnitsUsage
		} else {
			newUsage := &TenantUsage{
				SourceId:    indexerNode.NodeId,
				TenantId:    index.Bucket,
				MemoryUsage: index.ActualMemUsage,
				UnitsUsage:  index.ActualUnitsUsage,
			}
			usagePerTenant[index.Bucket] = newUsage
		}
	}
	return usagePerTenant
}

// sortTenantUsageByMemory sorts the input map of TenantUsage indexed by tenantId into
// a slice of TenantUsage sorted by memory in the ascending order.
func sortTenantUsageByMemory(usagePerTenant map[string]*TenantUsage) []*TenantUsage {

	usageSortedByMemory := make([]*TenantUsage, 0)

	//convert map to slice
	for _, tenantUsage := range usagePerTenant {
		usageSortedByMemory = append(usageSortedByMemory, tenantUsage)
	}

	//sort by memoryUsage ascending
	sort.Slice(usageSortedByMemory, func(i, j int) bool {
		return usageSortedByMemory[i].MemoryUsage < usageSortedByMemory[j].MemoryUsage
	})

	return usageSortedByMemory

}

// sortTenantUsageByUnits sorts the input map of TenantUsage indexed by tenantId into
// a slice of TenantUsage sorted by units in the ascending order.
func sortTenantUsageByUnits(usagePerTenant map[string]*TenantUsage) []*TenantUsage {

	usageSortedByUnits := make([]*TenantUsage, 0)

	//convert map to slice
	for _, tenantUsage := range usagePerTenant {
		usageSortedByUnits = append(usageSortedByUnits, tenantUsage)
	}

	//sort by unitsUsage ascending
	sort.Slice(usageSortedByUnits, func(i, j int) bool {
		return usageSortedByUnits[i].UnitsUsage < usageSortedByUnits[j].UnitsUsage
	})

	return usageSortedByUnits
}

// moveTenantsToLowUsageSubCluster moves the list of input tenants from source subclusters
// to the target subClusters.
func moveTenantsToLowUsageSubCluster(solution *Solution, tenantsToBeMoved [][]*TenantUsage,
	targetSubClusters []SubCluster, sourceSubClusters []SubCluster,
	usageThreshold *UsageThreshold) bool {

	const _moveTenantsToLowUsageSubCluster = "Planner::moveTenantsToLowUsageSubCluster"

	//pick the highest usage tenant from the list and find destination
	//round robin for each source subCluster

	maxLen := 0
	for _, tenantUsageInSubCluster := range tenantsToBeMoved {

		if maxLen < len(tenantUsageInSubCluster) {
			maxLen = len(tenantUsageInSubCluster)
		}
	}

	var candidate *TenantUsage
	var placed bool

	allTenantsPlaced := true
	for i := 0; i < maxLen; i++ {

		for _, tenantUsageInSubCluster := range tenantsToBeMoved {

			if i < len(tenantUsageInSubCluster) {
				candidate = tenantUsageInSubCluster[i]
			}
			placed = findTenantPlacement(solution, candidate,
				targetSubClusters, sourceSubClusters, usageThreshold)
			if !placed {
				allTenantsPlaced = false
				logging.Infof("%v Unable to place %v on any target", _moveTenantsToLowUsageSubCluster, candidate)
			}
		}

	}
	return allTenantsPlaced
	//TODO Elixir consider building state indexes also during planning
}

// findTenantPlacement finds the placement for a tenant based on the available resources
// from the given list of SubClusters below LWM threshold usage. Returns false if no
// placement can be found.
func findTenantPlacement(solution *Solution, tenant *TenantUsage, targetSubClusters []SubCluster,
	sourceSubClusters []SubCluster, usageThreshold *UsageThreshold) bool {

	const _findTenantPlacement = "Planner::findTenantPlacement"

	//find the first subCluster that can fit the tenant
	for _, subCluster := range targetSubClusters {

		target := subCluster
		if checkIfTenantCanBePlacedOnTarget(tenant, target, usageThreshold) {
			logging.Infof("%v %v can be placed on %v", _findTenantPlacement, tenant, target)
			source := findSourceForTenant(tenant, sourceSubClusters)
			placeTenantOnTarget(solution, tenant, source, target)
			return true
		}
	}

	return false
}

// checkIfTenantCanBePlacedOnTarget check if input tenant can be placed on the given
// SubCluster based on its memory/units usage and limit thresholds.
func checkIfTenantCanBePlacedOnTarget(tenant *TenantUsage, target SubCluster, usageThreshold *UsageThreshold) bool {

	//check if tenant can be placed without exceeding subCluster's memory and units
	//above LWM
	memoryToBeAdded := tenant.MemoryUsage
	unitsToBeAdded := tenant.UnitsUsage

	memQuota := usageThreshold.MemQuota
	unitsQuota := usageThreshold.UnitsQuota

	for _, indexerNode := range target {
		lowThresholdMemoryUsage := (uint64(usageThreshold.MemLowThreshold) * memQuota) / 100

		if indexerNode.MandatoryQuota+memoryToBeAdded > lowThresholdMemoryUsage {
			return false
		}

		lowThresholdUnitsUsage := (uint64(usageThreshold.UnitsLowThreshold) * unitsQuota) / 100

		if indexerNode.ActualUnits+unitsToBeAdded > lowThresholdUnitsUsage {
			return false
		}
	}

	return true

}

// findSourceForTenant finds the source subCluster for the input tenant from
// the list of subClusters provided.
func findSourceForTenant(tenant *TenantUsage, subClustersOverHWM []SubCluster) SubCluster {

	//find source node
	var source SubCluster
	for _, subCluster := range subClustersOverHWM {
		for _, indexerNode := range subCluster {
			if indexerNode.NodeId == tenant.SourceId {
				source = subCluster
			}
		}
	}
	return source

}

// placeTenantOnTarget places the input tenant on target subCluster
// and removes it from the source subCluster.
func placeTenantOnTarget(solution *Solution, tenant *TenantUsage,
	source SubCluster, target SubCluster) {

	const _placeTenantOnTarget = "Planner::placeTenantOnTarget"

	if len(source) != len(target) {
		logging.Warnf("%v Invariant Violation num "+
			"source %v is different from num target %v nodes", _placeTenantOnTarget,
			len(source), len(target))
	}

	//place indexes
	for i, indexerNode := range source {
		indexes := getIndexesForTenant(indexerNode, tenant)
		for _, index := range indexes {
			logging.Verbosef("%v Moving index %v from %v to %v", _placeTenantOnTarget,
				index, indexerNode.NodeId, target[i].NodeId)
			solution.moveIndex2(indexerNode, index, target[i])
		}
	}
}

// updateTargetSubClusterUsage add the tenantUsage memory/units to input subcluster's
// node statistics.
func updateTargetSubClusterUsage(subCluster SubCluster, tenant *TenantUsage) {

	//update the target subCluster statistics
	for _, indexerNode := range subCluster {
		indexerNode.MandatoryQuota += tenant.MemoryUsage
		indexerNode.ActualUnits += tenant.UnitsUsage
	}
}

// updateTargetSubClusterUsage subtracts the tenantUsage memory/units from input subcluster's
// node statistics.
func updateSourceSubClusterUsage(source SubCluster, tenant *TenantUsage) {

	for _, indexerNode := range source {
		if tenant.MemoryUsage < indexerNode.MandatoryQuota {
			indexerNode.MandatoryQuota -= tenant.MemoryUsage
		} else {
			indexerNode.MandatoryQuota = 0
		}

		if tenant.UnitsUsage < indexerNode.ActualUnits {
			indexerNode.ActualUnits -= tenant.UnitsUsage
		} else {
			indexerNode.ActualUnits = 0
		}
	}
}

// sortSubClustersByMemUsage sorts the input slice of subClusters into ascending
// order based on memory usage and returns the new sorted slice
func sortSubClustersByMemUsage(subClusters []SubCluster) []SubCluster {

	findMaxMemUsageInSubCluster := func(subCluster SubCluster) uint64 {

		var maxMem uint64
		for _, indexerNode := range subCluster {
			if indexerNode.MandatoryQuota > maxMem {
				maxMem = indexerNode.MandatoryQuota
			}
		}
		return maxMem
	}

	sort.Slice(subClusters, func(i, j int) bool {
		return findMaxMemUsageInSubCluster(subClusters[i]) <
			findMaxMemUsageInSubCluster(subClusters[j])
	})

	return subClusters
}

// getIndexesForTenant returns the list of indexes for the specified tenant
// from the input indexer node.
func getIndexesForTenant(indexerNode *IndexerNode, tenant *TenantUsage) []*IndexUsage {

	var indexes []*IndexUsage

	for _, index := range indexerNode.Indexes {
		if index.Bucket == tenant.TenantId {
			indexes = append(indexes, index)
		}
	}

	return indexes
}

// Stringer from TenantUsage
func (t *TenantUsage) String() string {

	str := fmt.Sprintf("TenantUsage - ")
	str += fmt.Sprintf("SourceId %v ", t.SourceId)
	str += fmt.Sprintf("TenantId %v ", t.TenantId)
	str += fmt.Sprintf("MemoryUsage %v ", t.MemoryUsage)
	str += fmt.Sprintf("UnitsUsage %v ", t.UnitsUsage)
	return str

}

// repairMissingReplica repairs the missing replicas in the subCluster
func repairMissingReplica(solution *Solution, allSubClusters []SubCluster) {

	const _repairMissingReplica = "Planner::repairMissingReplica"

	for _, subCluster := range allSubClusters {
		if len(subCluster) != cSubClusterLen {
			logging.Infof("%v Found SubCluster %v with len %v. Skipping replica repair attempt.", _repairMissingReplica,
				subCluster, len(subCluster))
			continue
		}
		for i, indexer := range subCluster {
			missingReplicaList := findMissingReplicaForIndexerNode(indexer, subCluster)
			target := findPairNodeUsingIndex(subCluster, i)
			for _, missingReplica := range missingReplicaList {
				placeMissingReplicaOnTarget(missingReplica, target, solution)
			}
		}
	}
}

// findMissingReplicaForIndexerNode checks if there is any missing index
// on the input indexer node which doesn't have a replica in the given subCluster.
func findMissingReplicaForIndexerNode(indexer *IndexerNode, subCluster SubCluster) []*IndexUsage {

	const _findMissingReplicaForIndexerNode = "Planner::findMissingReplicaForIndexerNode"
	missingReplicaList := make([]*IndexUsage, 0)

	for _, index := range indexer.Indexes {
		numReplica := findNumReplicaForSubCluster(index, subCluster)

		missingReplica := 0
		if index.Instance != nil {
			missingReplica = int(index.Instance.Defn.NumReplica+1) - numReplica
		}

		if index.Instance != nil && missingReplica > 0 &&
			!index.PendingDelete {
			missingReplicaList = append(missingReplicaList, index)
		}

		if index.PendingDelete {
			logging.Infof("%v Skipping Replica Repair for %v. PendingDelete %v", _findMissingReplicaForIndexerNode,
				index, index.PendingDelete)
		}
	}
	return missingReplicaList

}

// placeMissingReplicaOnTarget places the repaired replica on the input target node and
// adds it to the solution.
func placeMissingReplicaOnTarget(index *IndexUsage, target *IndexerNode, solution *Solution) {

	const _placeMissingReplicaOnTarget = "Planner::placeMissingReplicaOnTarget"

	// clone the original and update the replicaId
	if index.Instance != nil {
		newReplicaId := findMissingReplicaId(index)

		cloned := index.clone()
		cloned.Instance.ReplicaId = newReplicaId
		cloned.initialNode = nil
		cloned.siblingIndex = index

		instId, err := common.NewIndexInstId()
		if err != nil {
			logging.Errorf("%v Error while generating instId, err: %v", _placeMissingReplicaOnTarget, err)
			return
		}
		cloned.InstId = instId
		cloned.Instance.InstId = instId

		// add the new replica to the solution
		solution.addIndex2(target, cloned)

		logging.Infof("%v Rebuilding lost replica for (%v,%v,%v,%v,%v) on %v",
			_placeMissingReplicaOnTarget, index.Bucket, index.Scope,
			index.Collection, index.Name, newReplicaId, target)
	}
}

// findMissingReplicaId returns the replicaId of the missing replica
// based on the replicaId of the input index. This function only works
// for serverless model with fixed NumReplica=1.
func findMissingReplicaId(index *IndexUsage) int {

	if index.Instance.ReplicaId == 0 {
		return 1
	} else {
		return 0
	}
}

// findNumReplicaForSubCluster returns the number of replicas for the given index
// in the input subcluster including self..
func findNumReplicaForSubCluster(u *IndexUsage, subCluster SubCluster) int {

	count := 0
	for _, indexer := range subCluster {
		for _, index := range indexer.Indexes {

			// check replica
			if index.IsReplica(u) {
				count++
			}
		}
	}

	return count
}

// findPairNodeUsingIndex finds the pair node in the subCluster using the index
// of the input node.
func findPairNodeUsingIndex(subCluster SubCluster, index int) *IndexerNode {

	if index == 0 {
		return subCluster[1]
	} else {
		return subCluster[0]
	}

}

// findPlacementForDeletedNodes finds the placement of indexes which belong to a
// deleted node. Once the right placement is found, the indexes are moved to the
// new target node in the input solution. It returns the list of non ejected/deleted
// nodes remaining in the cluster.
func findPlacementForDeletedNodes(solution *Solution, usageThreshold *UsageThreshold) error {

	const _findPlacementForDeletedNodes = "Planner::findPlacementForDeletedNodes"
	allIndexers := solution.Placement

	deletedNodes := solution.getDeleteNodes()
	newNodes := solution.getNewNodes()

	logging.Infof("%v Deleted Nodes %v", _findPlacementForDeletedNodes, deletedNodes)

	err := moveTenantsFromDeletedNodes(deletedNodes, newNodes, solution, usageThreshold)
	if err != nil {
		return err
	}

	//remove the deleted nodes from the list
	nonEjectedNodes := make([]*IndexerNode, 0, len(allIndexers))

	for _, indexer := range allIndexers {
		if !indexer.IsDeleted() {
			nonEjectedNodes = append(nonEjectedNodes, indexer)
		} else {
			logging.Infof("%v Remove Deleted Node from solution %v SG %v Memory %v Units %v", _findPlacementForDeletedNodes,
				indexer.NodeId, indexer.ServerGroup, indexer.MandatoryQuota, indexer.ActualUnits)
		}
	}

	solution.Placement = nonEjectedNodes
	return nil
}

// moveTenantsFromDeletedNodes moves the tenants from the deleted nodes
// to remaining nodes in the cluster based on the following rules:
// 1. Ignore empty deleted nodes as nothing needs to be done.
// 2. Find candidate node for deleted node to which indexes of this deleted
// node can be swapped to. It is important to swap the indexes from
// deleted nodes entirely to the candidate node to maintain sub-cluster affinity.
// This will work well if a single or both nodes of the sub-cluster are
// getting deleted.
// 3. If deleted nodes are equal to new nodes, then swap one for one.
// 4. If deleted nodes are less than new nodes,
// pick any new nodes which can match the server group constraint.
// 5. If deleted nodes are more than new nodes, then:
// 5a. If new nodes are non-zero, return error as planner cannot determine
// which nodes to place the indexes on.
// 5b. If new nodes are zero, it is treated as a case of failed swap rebalance.
// Indexes from deleted nodes are placed to maintain tenant affinity in subcluster.
func moveTenantsFromDeletedNodes(deletedNodes []*IndexerNode,
	newNodes []*IndexerNode, solution *Solution, usageThreshold *UsageThreshold) error {

	const _moveTenantsFromDeletedNodes = "Planner::moveTenantsFromDeletedNodes"

	//remove empty deleted nodes
	var nonEmptyDeletedNodes []*IndexerNode
	for _, indexer := range deletedNodes {
		if len(indexer.Indexes) != 0 {
			nonEmptyDeletedNodes = append(nonEmptyDeletedNodes, indexer)
		}
	}

	//if there is no non-empty deleted node, nothing to do
	if len(nonEmptyDeletedNodes) == 0 {
		logging.Infof("%v No non-empty deleted nodes found.", _moveTenantsFromDeletedNodes)
		return nil
	}

	//find pair nodes for all non empty deleted nodes i.e. the node
	//which pairs up as a sub-cluster with the deleted node. If pair node cannot
	//be found, it is fine. It is only required to check for server group
	//mapping constraint(i.e. nodes in a sub-cluster cannot be in the same SG).
	//If both nodes in the sub-cluster are going out, then both pairs will be
	//added to the list.
	var pairForDeletedNodes []*IndexerNode
	for _, indexer := range nonEmptyDeletedNodes {
		pairNode := findPairNodeForIndexer(indexer, solution.Placement)
		pairForDeletedNodes = append(pairForDeletedNodes, pairNode)
	}

	logging.Infof("%v nonEmptyDeletedNodes %v", _moveTenantsFromDeletedNodes, nonEmptyDeletedNodes)
	logging.Infof("%v pairForDeletedNodes %v", _moveTenantsFromDeletedNodes, pairForDeletedNodes)
	logging.Infof("%v newNodes %v", _moveTenantsFromDeletedNodes, newNodes)

	if len(nonEmptyDeletedNodes) > len(newNodes) {

		logging.Infof("%v Num deleted nodes %v is more than num new/empty nodes %v", _moveTenantsFromDeletedNodes,
			len(nonEmptyDeletedNodes), len(newNodes))

		//if number of outgoing nodes is more than nodes coming in, planner cannot decide which
		//ones need to be used for swap.
		if len(newNodes) != 0 {
			errStr := fmt.Sprintf("Planner - Number of non-empty deleted nodes cannot be greater than number of added nodes.")
			logging.Errorf(errStr)
			return errors.New(errStr)
		}

		var outSubClusters []SubCluster
		dedup := make(map[string]bool)
		for i, delNode := range nonEmptyDeletedNodes {

			//check if this delNode has already been processed as a pairNode
			if _, ok := dedup[delNode.NodeUUID]; ok {
				continue
			}

			pairNode := pairForDeletedNodes[i]

			var isPairBeingDeleted bool
			if pairNode != nil {
				dedup[pairNode.NodeUUID] = true
				isPairBeingDeleted = pairNode.IsDeleted()
			}

			if isPairBeingDeleted {
				//Both nodes of the subcluster are being deleted.
				//This can be a case of failed swap rebalance of a subcluster or
				//a subcluster being removed from the cluster.
				//In both cases, attempt to move indexes to remaining nodes
				//in the cluster.
				var subCluster SubCluster
				subCluster = append(subCluster, delNode)
				subCluster = append(subCluster, pairNode)

				outSubClusters = append(outSubClusters, subCluster)
				tenantsToBeMoved := getTenantUsageForSubClusters(outSubClusters)

				for i, tenants := range tenantsToBeMoved {
					logging.Infof("%v TenantsToBeMoved from source %v", _moveTenantsFromDeletedNodes, outSubClusters[i])
					for _, tenant := range tenants {
						logging.Infof("%v %v", _moveTenantsFromDeletedNodes, tenant)
					}
				}
				if len(tenantsToBeMoved) == 0 {
					return nil
				}

				//consider all subclusters below LWM as candidates for index placement
				nonEjectedSubClusters, err := groupIndexNodesIntoSubClusters(solution.Placement)
				if err != nil {
					return err
				}
				subClustersBelowLWM, err := findSubClustersBelowLowThreshold(nonEjectedSubClusters, usageThreshold)
				if err != nil {
					return err
				}

				subClustersBelowLWM = filterPartialSubClusters(subClustersBelowLWM)

				logging.Infof("%v Found SubClusters Below LWM %v", _moveTenantsFromDeletedNodes, subClustersBelowLWM)

				if len(subClustersBelowLWM) == 0 {
					errStr := "Planner - Not enough capacity to place indexes of deleted nodes."
					logging.Errorf(errStr)
					return errors.New(errStr)
				}

				subClustersBelowLWM = sortSubClustersByMemUsage(subClustersBelowLWM)

				allTenantsPlaced := moveTenantsToLowUsageSubCluster(solution, tenantsToBeMoved, subClustersBelowLWM,
					outSubClusters, usageThreshold)
				if !allTenantsPlaced {
					errStr := "Planner - Not enough capacity to place indexes of deleted nodes."
					logging.Errorf(errStr)
					return errors.New(errStr)
				}
			} else {

				if pairNode == nil {
					logging.Infof("%v Pair node not found for deleted node %v.", _moveTenantsFromDeletedNodes,
						delNode)

					errStr := fmt.Sprintf("Planner - Pair node for %v not found. "+
						"Provide additional node as replacement.", delNode.NodeId)
					logging.Errorf(errStr)
					return errors.New(errStr)

				}

				//Identify if the single node moving out is part of a failed swap rebalance.
				//In that case, move indexes to maintain subcluster affinity.
				//Else fail the request as moving out single node without replacement is not allowed.
				nonEjectedSubClusters, err := groupIndexNodesIntoSubClusters(solution.Placement)
				if err != nil {
					return err
				}

				var targetNode *IndexerNode
				for _, subCluster := range nonEjectedSubClusters {

					for i, node := range subCluster {
						if len(subCluster) != cSubClusterLen {
							continue
						}
						if node.NodeUUID == pairNode.NodeUUID && !node.IsDeleted() {
							targetNode = findPairNodeUsingIndex(subCluster, i)
							if targetNode.IsDeleted() {
								targetNode = nil
							}
						}
					}
				}

				if targetNode == nil {
					logging.Infof("%v No replacement node found for deleted node %v.", _moveTenantsFromDeletedNodes,
						delNode)

					errStr := fmt.Sprintf("Planner - Removing node %v will result in losing indexes. "+
						"Provide additional node as replacement.", delNode.NodeId)
					logging.Errorf(errStr)
					return errors.New(errStr)
				} else {
					//move indexes from deleted node to target node
					logging.Infof("%v Considering %v as replacement node for deleted node %v.", _moveTenantsFromDeletedNodes,
						targetNode, delNode)
					swapTenantsFromDeleteNodes([]*IndexerNode{delNode}, []*IndexerNode{targetNode}, solution)
				}

			}
		}

	} else {

		//In order to make sure that all chosen swap candidate nodes can satisfy the server group constraints after
		//forming sub-clusters, generate permutations of candidate nodes and validate server group constraint.
		//Pick the combination which satisfies the server group constraint.
		newNodesPermutations := permuteNodes(newNodes)
		found := false
		for _, newNodesPerm := range newNodesPermutations {
			if validateServerGroupForPairNode(nonEmptyDeletedNodes, pairForDeletedNodes, newNodesPerm) {
				newNodes = newNodesPerm
				found = true
				break
			}
		}
		if found {
			logging.Infof("%v selected newNodes for swap %v", _moveTenantsFromDeletedNodes, newNodes)
			swapTenantsFromDeleteNodes(nonEmptyDeletedNodes, newNodes, solution)
		} else {
			return errors.New("Planner - Unable to satisfy server group constraint while replacing " +
				"removed nodes with new nodes.")
		}
	}

	return nil
}

// findPairNodeForIndexer finds the sub-cluster pair for the input indexer node
// from all the nodes in the cluster. Returns nil if pair node cannot be found.
func findPairNodeForIndexer(node *IndexerNode, allIndexers []*IndexerNode) *IndexerNode {

	for _, index := range node.Indexes {

		for _, indexer := range allIndexers {

			if indexer.NodeUUID == node.NodeUUID {
				//skip self
				continue
			}
			for _, checkIdx := range indexer.Indexes {
				if index.DefnId == checkIdx.DefnId &&
					index.PartnId == checkIdx.PartnId {
					return indexer
				}
			}
		}
	}
	return nil
}

// swapTenantsFromDeleteNodes swaps the indexes from deletedNodes
// to newNodes.
func swapTenantsFromDeleteNodes(deletedNodes []*IndexerNode,
	newNodes []*IndexerNode, solution *Solution) {

	for i, source := range deletedNodes {

		outIndex := make([]*IndexUsage, len(source.Indexes))
		copy(outIndex, source.Indexes)
		for _, index := range outIndex {
			logging.Infof("Moving index %v from source %v to dest %v", index, source, newNodes[i])
			solution.moveIndex2(source, index, newNodes[i])
		}
	}

}

// validateServerGroupForPairNode validates if nodes in deletedNodes can be paired with nodes
// in newNodes based on server group mapping. Two nodes can only be paired if both belong
// to different server group.
func validateServerGroupForPairNode(deletedNodes []*IndexerNode,
	pairForDeletedNodes []*IndexerNode, newNodes []*IndexerNode) bool {

	const _validateServerGroupForPairNode = "Planner::validateServerGroupForPairNode"

	logging.Verbosef("%v DeletedNodes %v PairNodes %v NewNodes %v", _validateServerGroupForPairNode,
		deletedNodes, pairForDeletedNodes, newNodes)

	//node with the same index in the slice are considered to be a pair
	for i, _ := range deletedNodes {

		//if newNodes are less than deletedNodes
		if i > len(newNodes)-1 {
			return false
		}

		pairNode := pairForDeletedNodes[i]
		if pairNode != nil {
			//if pair node is also being deleted, then make sure the
			//swap candidates for deleted node and its pair do not
			//belong to the same server group.
			if pairNode.IsDeleted() {
				//find index of pairNode in the deletedNode
				delIndex := 0
				for j, delNode := range deletedNodes {
					if pairNode.NodeUUID == delNode.NodeUUID {
						delIndex = j
					}
				}
				//validate server group
				if pairNode.ServerGroup == newNodes[delIndex].ServerGroup {
					logging.Verbosef("%v SG Mismatch for deleted pairNode %v newNode %v",
						_validateServerGroupForPairNode, pairNode, newNodes[delIndex])
					return false
				}
				continue
			}

			if pairNode.ServerGroup == newNodes[i].ServerGroup {
				logging.Verbosef("%v SG Mismatch pairNode %v newNode %v",
					_validateServerGroupForPairNode, pairNode, newNodes[i])
				return false
			}
		}
	}

	return true

}

// permuteNodes returns all the permutation of the input slice
// of indexer nodes.
func permuteNodes(nodes []*IndexerNode) [][]*IndexerNode {

	var perm func([]*IndexerNode, int)

	result := make([][]*IndexerNode, 0)

	perm = func(nodes []*IndexerNode, n int) {
		if n == 1 {
			tmp := make([]*IndexerNode, len(nodes))
			copy(tmp, nodes)
			result = append(result, tmp)
		} else {
			for i := 0; i < n; i++ {
				perm(nodes, n-1)
				if n%2 == 1 {
					tmp := nodes[i]
					nodes[i] = nodes[n-1]
					nodes[n-1] = tmp
				} else {
					tmp := nodes[0]
					nodes[0] = nodes[n-1]
					nodes[n-1] = tmp
				}
			}
		}
	}
	perm(nodes, len(nodes))
	return result
}

// findCandidateTenantsToMoveOut computes the tenants that
// can be moved out of the list of input SubClusters to bring down its
// memory/units usage less than or equal to LWM threshold.
func getTenantUsageForSubClusters(subClusters []SubCluster) [][]*TenantUsage {

	var tenantUsage [][]*TenantUsage
	for _, subCluster := range subClusters {
		tenantList := getTenantUsageForSubCluster(subCluster)
		if len(tenantList) != 0 {
			tenantUsage = append(tenantUsage, tenantList)
		}
	}

	return tenantUsage
}

// findTenantsToMoveOutFromSubCluster computes the tenants that
// can be moved out of the input SubCluster to bring down its
// memory/units usage less than or equal to LWM threshold.
func getTenantUsageForSubCluster(subCluster SubCluster) []*TenantUsage {

	const _getTenantUsageForSubCluster = "Planner::getTenantUsageForSubCluster"

	var maxMemoryUsage, maxUnitsUsage uint64
	var highestUsageNode *IndexerNode

	for _, indexNode := range subCluster {

		//pairNode can be nil in case it is already removed from cluster
		if indexNode == nil {
			continue
		}

		if highestUsageNode == nil {
			highestUsageNode = indexNode
		}

		if indexNode.MandatoryQuota > maxMemoryUsage {
			maxMemoryUsage = indexNode.MandatoryQuota
			highestUsageNode = indexNode
		}
		if indexNode.ActualUnits > maxUnitsUsage {
			maxUnitsUsage = indexNode.ActualUnits
		}

	}

	//compute usage by tenant
	usagePerTenant := computeUsageByTenant(highestUsageNode)

	//sort the usage from lowest to highest
	usageSortedByMemory := sortTenantUsageByMemory(usagePerTenant)
	usageSortedByUnits := sortTenantUsageByUnits(usagePerTenant)

	//reverse sort from highest to lowest usage. The algorithm
	//will try to move tenant based on highest usage first so
	//it has better chances of being able to find a slot on the target.
	sort.Slice(usageSortedByMemory, func(i, j int) bool {
		return usageSortedByMemory[i].MemoryUsage > usageSortedByMemory[j].MemoryUsage
	})
	sort.Slice(usageSortedByUnits, func(i, j int) bool {
		return usageSortedByUnits[i].UnitsUsage > usageSortedByUnits[j].UnitsUsage
	})

	//create combined usage
	combinedUsage := make([]*TenantUsage, 0)
	processedTenants := make(map[string]bool)

	maxlen := len(usageSortedByMemory)
	if maxlen < len(usageSortedByUnits) {
		maxlen = len(usageSortedByUnits)
	}

	var candidate *TenantUsage
	for i := 0; i < maxlen; i++ {

		if i < len(usageSortedByMemory) {
			candidate = usageSortedByMemory[i]
			if _, found := processedTenants[candidate.TenantId]; !found {
				combinedUsage = append(combinedUsage, candidate)
				processedTenants[candidate.TenantId] = true
			}
		}

		if i < len(usageSortedByUnits) {
			candidate = usageSortedByUnits[i]
			if _, found := processedTenants[candidate.TenantId]; !found {
				combinedUsage = append(combinedUsage, candidate)
				processedTenants[candidate.TenantId] = true
			}
		}
	}

	return combinedUsage

}

// GetDefragmentedUtilization is the entry method for planner to compute the utilization
// stats in the given cluster after rebalance.
func GetDefragmentedUtilization(clusterUrl string) (map[string]map[string]interface{}, error) {

	plan, err := RetrievePlanFromCluster(clusterUrl, nil, true)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Unable to read index layout from cluster %v. err = %s", clusterUrl, err))
	}
	return getDefragmentedUtilization(plan)
}

// getDefragmentedUtilization runs the actual rebalance algorithm to generate the new placement
// plan and compute the utilization stats from it.
func getDefragmentedUtilization(plan *Plan) (map[string]map[string]interface{}, error) {

	p, _, err := executeTenantAwareRebal(CommandRebalance, plan, nil)
	if err != nil {
		return nil, err
	}

	var deletedNodes []string
	p.Result, deletedNodes = evaluateSolutionForScaleIn(p.Result, plan.UsageThreshold)

	defragUtilStats := genDefragUtilStats(p.Result.Placement, deletedNodes)
	return defragUtilStats, nil
}

// getDefragmentedUtilization computes the utilization stats from the new placement plan generated
// by the planner after executing the rebalance algorithm.
// Following per-node stats are returned currently:
// 1. "memory_used_actual"
// 2. "units_used_actual"
// 3. "num_tenants"
// 4. "num_index_repaired"
func genDefragUtilStats(placement []*IndexerNode, deletedNodes []string) map[string]map[string]interface{} {

	defragUtilStats := make(map[string]map[string]interface{})
	for _, indexerNode := range placement {
		nodeUsageStats := make(map[string]interface{})
		nodeUsageStats["memory_used_actual"] = indexerNode.MandatoryQuota
		nodeUsageStats["units_used_actual"] = indexerNode.ActualUnits
		nodeUsageStats["num_tenants"] = getNumTenantsForNode(indexerNode)
		nodeUsageStats["num_index_repaired"] = getNumIndexRepaired(indexerNode)

		defragUtilStats[indexerNode.NodeId] = nodeUsageStats
	}

	//add stats for deleted nodes
	for _, delNode := range deletedNodes {
		nodeUsageStats := make(map[string]interface{})
		nodeUsageStats["memory_used_actual"] = uint64(0)
		nodeUsageStats["units_used_actual"] = uint64(0)
		nodeUsageStats["num_tenants"] = uint64(0)
		nodeUsageStats["num_index_repaired"] = uint64(0)

		defragUtilStats[delNode] = nodeUsageStats
	}

	return defragUtilStats
}

// getNumTenantsForNode computes the number of tenants on an indexer node
func getNumTenantsForNode(indexerNode *IndexerNode) uint64 {

	numTenants := make(map[string]bool)
	for _, index := range indexerNode.Indexes {
		if _, ok := numTenants[index.Bucket]; !ok {
			numTenants[index.Bucket] = true
		}
	}
	return uint64(len(numTenants))
}

// evaluateSolutionForScaleIn checks if there is enough excess capacity to allow some
// nodes to be removed from the cluster based on usage thresholds.
func evaluateSolutionForScaleIn(solution *Solution, usageThreshold *UsageThreshold) (*Solution, []string) {

	const _evaluateSolutionForScaleIn = "Planner::evaluateSolutionForScaleIn"

	//If there are already empty nodes in the cluster, there is no need for
	//further evaluation. CP can remove the empty nodes for scale in.
	numNewNode := solution.findNumEmptyNodes()
	if numNewNode > 0 {
		return solution, nil
	}

	//clone the original solution
	cloneSolution := solution.clone()

	//reset any new node flag in the cloned solution
	cloneSolution.resetNewNodes()

	//group indexer nodes into subclusters
	allSubClusters, err := groupIndexNodesIntoSubClusters(cloneSolution.Placement)
	if err != nil {
		logging.Errorf("%v Error group into subclusters %v", _evaluateSolutionForScaleIn, err)
		return solution, nil
	}

	logging.Infof("%v Found SubClusters  %v", _evaluateSolutionForScaleIn, allSubClusters)

	subClustersBelowLWM, err := findSubClustersBelowLowThreshold(allSubClusters,
		usageThreshold)

	logging.Infof("%v Found SubClusters below LWM %v", _evaluateSolutionForScaleIn, subClustersBelowLWM)

	//if number of subclusters below LWM are less than 2, nothing to be done
	//minimum 2 subClusters are required below LWM to allow one subcluster to be removed.
	if len(subClustersBelowLWM) < 2 {
		return solution, nil
	}

	leastLoadedSubCluster := findLeastLoadedSubCluster(subClustersBelowLWM)

	//mark the nodes of least loaded subcluster as deleted
	var deletedNodes []string
	for _, indexerNode := range leastLoadedSubCluster {
		deletedNodes = append(deletedNodes, indexerNode.NodeId)
	}

	err = cloneSolution.markDeletedNodes(deletedNodes)
	if err != nil {
		logging.Errorf("%v Error marking deleted nodes %v", _evaluateSolutionForScaleIn, err)
		return solution, nil
	}

	cloneSolution.numDeletedNode = cloneSolution.findNumDeleteNodes()

	//adjust the usage threshold for scale-in
	usageThresholdScaleIn := computeUsageThresholdForScaleIn(usageThreshold)

	err = findPlacementForDeletedNodes(cloneSolution, usageThresholdScaleIn)
	if err != nil {
		//Return the original solution as there is not enough capacity for scale-in
		return solution, nil
	} else {
		//If there is no error, it means the indexes from deleted nodes can fit
		//in on remaining nodes under LWM with reduced scale-in thresholds.
		//The new solution can be accepted.
		return cloneSolution, deletedNodes
	}

}

func computeUsageThresholdForScaleIn(usageThreshold *UsageThreshold) *UsageThreshold {

	scaleInMemThreshold := (usageThreshold.MemLowThreshold * (100 - cScaleInUsageThreshold)) / 100
	scaleInUnitsThreshold := (usageThreshold.UnitsLowThreshold * (100 - cScaleInUsageThreshold)) / 100

	return &UsageThreshold{
		MemHighThreshold: usageThreshold.MemHighThreshold,
		MemLowThreshold:  scaleInMemThreshold,

		UnitsHighThreshold: usageThreshold.UnitsHighThreshold,
		UnitsLowThreshold:  scaleInUnitsThreshold,

		MemQuota:   usageThreshold.MemQuota,
		UnitsQuota: usageThreshold.UnitsQuota,
	}

}

// getNumIndexRepaired returns the count of repaired replicas for the given indexer node
func getNumIndexRepaired(indexerNode *IndexerNode) uint64 {

	var numIndexRepaired uint64
	for _, index := range indexerNode.Indexes {
		if index.initialNode == nil && !index.pendingCreate {
			numIndexRepaired++
		}
	}
	return numIndexRepaired
}

func filterPartialSubClusters(subClusters []SubCluster) []SubCluster {

	for i := len(subClusters) - 1; i >= 0; i-- {
		if len(subClusters[i]) != cSubClusterLen {
			logging.Infof("Planner::filterPartialSubClusters Filter partial subcluster %v", subClusters[i])
			subClusters = append(subClusters[:i], subClusters[i+1:]...)
		}
	}
	return subClusters
}

// ExecuteTenantAwarePlanForResume provides index placement planning on a tenant resume .
// Given an input cluster url and slice of index metadata for resume nodes, it will return
// the set of resume tokens.
func ExecuteTenantAwarePlanForResume(clusterUrl string, resumeId, masterId, bucket string,
	resumeNodes []*IndexerNode) (map[string]*common.ResumeDownloadToken, error) {

	plan, err := RetrievePlanFromCluster(clusterUrl, nil, false)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Unable to read index layout from cluster %v. err = %s", clusterUrl, err))
	}

	p, err := executeTenantAwarePlanForResume(plan, resumeNodes)
	if err != nil {
		return nil, err
	}

	//find destination subcluster for the resumed tenant
	destSubCluster := findDestSubClusterForResumedTenant(p.Result, bucket)

	rdts, err := genResumeDownloadToken(p.Result, destSubCluster, bucket, resumeId, masterId)
	if err != nil {
		return nil, err
	}
	return rdts, nil

}

func executeTenantAwarePlanForResume(plan *Plan, resumeNodes []*IndexerNode) (*TenantAwarePlanner,
	error) {

	const _executeTenantAwarePlanForResume = "Planner::executeTenantAwarePlanForResume"

	logging.Infof("%v Resume Nodes %v", _executeTenantAwarePlanForResume, resumeNodes)

	solution := solutionFromPlan2(plan)
	tenantAwarePlanner := &TenantAwarePlanner{Result: solution}

	//group indexer nodes into subclusters
	allSubClusters, err := groupIndexNodesIntoSubClusters(solution.Placement)
	if err != nil {
		return nil, err
	}

	logging.Infof("%v Found SubClusters  %v", _executeTenantAwarePlanForResume, allSubClusters)

	//Only 1 tenant can be resumed at a time. And 1 tenant can only reside on 1 subcluster.
	if len(resumeNodes) != cSubClusterLen {
		errStr := fmt.Sprintf("Unexpected number of resumeNodes %v. Expected %v", len(resumeNodes), cSubClusterLen)
		logging.Errorf("%v %v", _executeTenantAwarePlanForResume, errStr)
		return nil, errors.New(errStr)
	}

	var resumeSubCluster SubCluster
	for _, node := range resumeNodes {
		resumeSubCluster = append(resumeSubCluster, node)
	}

	tenantsToBeResumed := getTenantUsageForSubClusters([]SubCluster{resumeSubCluster})

	for _, tenants := range tenantsToBeResumed {
		for _, tenant := range tenants {
			logging.Infof("%v TenantToBeResumed %v", _executeTenantAwarePlanForResume, tenant)
		}
	}
	if len(tenantsToBeResumed) == 0 {
		errStr := "Unable to find any tenant for resume"
		logging.Errorf("%v %v", _executeTenantAwarePlanForResume, errStr)
		return nil, errors.New(errStr)
	}

	subClustersBelowLWM, err := findSubClustersBelowLowThreshold(allSubClusters,
		plan.UsageThreshold)
	if err != nil {
		return nil, err
	}

	subClustersBelowLWM = filterPartialSubClusters(subClustersBelowLWM)

	logging.Infof("%v Found SubClusters Below LWM %v", _executeTenantAwarePlanForResume, subClustersBelowLWM)

	if len(subClustersBelowLWM) == 0 {
		errStr := "No SubCluster Below Low Usage Threshold"
		return nil, errors.New(errStr)
	}

	subClustersBelowLWM = sortSubClustersByMemUsage(subClustersBelowLWM)

	tenantPlaced := moveTenantsToLowUsageSubCluster(solution, tenantsToBeResumed, subClustersBelowLWM,
		[]SubCluster{resumeSubCluster}, plan.UsageThreshold)

	if !tenantPlaced {
		errStr := "Not Enough Capacity To Place Tenant"
		return nil, errors.New(errStr)
	}

	return tenantAwarePlanner, nil
}

// findDestSubClusterForResumedTenant finds the dest subCluster for the resumed tenant.
// In V1, resumed tenant can be planced on a single subcluster only.
func findDestSubClusterForResumedTenant(solution *Solution, bucket string) SubCluster {

	//There is only one tenant resumed at a time. Find resumed nodes based on matching bucket name.
	var destSubCluster SubCluster

	for _, node := range solution.Placement {
		for _, index := range node.Indexes {
			if index.Bucket == bucket {
				destSubCluster = append(destSubCluster, node)
				break
			}
		}
	}

	return destSubCluster
}

// generate resume download tokens from solution
func genResumeDownloadToken(solution *Solution,
	destSubCluster SubCluster, bucket, resumeId,
	masterId string) (map[string]*common.ResumeDownloadToken, error) {

	// Generate a mapping between defnId -> replicaId -> instanceId.
	defnIdToInstIdMap := getDefnIdToInstIdMap(solution)

	getInstIds := func(index *IndexUsage) (common.IndexInstId /* instId*/, common.IndexInstId /*realInstId*/, error) {

		if common.IsPartitioned(index.Instance.Defn.PartitionScheme) {
			instId, err := common.NewIndexInstId()
			if err != nil {
				return 0, 0, fmt.Errorf("Fail to generate transfer token.  Reason: %v", err)
			}

			return instId, index.InstId, nil
		}
		return index.InstId, 0, nil
	}

	initInstInToken := func(token *common.ResumeDownloadToken, index *IndexUsage) error {

		token.IndexInsts = append(token.IndexInsts, *index.Instance)

		instId, realInstId, err := getInstIds(index)
		if err != nil {
			return err
		}
		token.InstIds = append(token.InstIds, instId)
		token.RealInstIds = append(token.RealInstIds, realInstId)
		return nil
	}

	tokens := make(map[string]*common.ResumeDownloadToken)

	initToken := func(index *IndexUsage, destNode *IndexerNode) (*common.ResumeDownloadToken, error) {

		token := &common.ResumeDownloadToken{
			MasterId:     masterId,
			FollowerId:   destNode.NodeUUID,
			FollowerHost: destNode.NodeId,
			ResumeId:     resumeId,
			State:        common.ResumeDownloadTokenPosted,
			BucketName:   bucket,
			UploaderId:   strings.TrimPrefix(index.initialNode.NodeUUID, tempNodeUUID),
			ShardIds:     index.ShardIds,
		}

		if err := initInstInToken(token, index); err != nil {
			return nil, err
		}
		return token, nil
	}

	addIndexToToken := func(tokenKey string, index *IndexUsage, dest *IndexerNode) error {

		// Currently, planner is generating a new index instance per partition.
		// This method will group all partitions of an index residing on a node
		// into a single index instance
		groupIndexInstances(index, defnIdToInstIdMap)

		token, ok := tokens[tokenKey]
		if !ok {
			var err error
			if token, err = initToken(index, dest); err != nil {
				return err
			}
			tokens[tokenKey] = token
		}

		if token != nil {
			sliceIndex := len(token.InstIds) - 1
			found := false
			instIds := token.InstIds
			if common.IsPartitioned(index.Instance.Defn.PartitionScheme) {
				// For partitioned index, use realInstIds. If it is the first
				// time this partition index is being processed, it will be
				// added due to !found becoming true
				instIds = token.RealInstIds
			}

			for i, instId := range instIds {
				if instId == index.InstId {
					sliceIndex = i
					found = true
					break
				}
			}

			if !found { // Instance not already appended to list. Add now
				if err := initInstInToken(token, index); err != nil {
					return err
				}

				sliceIndex = len(token.InstIds) - 1
			}

			token.IndexInsts[sliceIndex].Defn.InstStateAtRebal = token.IndexInsts[sliceIndex].State
			token.IndexInsts[sliceIndex].Defn.InstVersion = token.IndexInsts[sliceIndex].Version
			token.IndexInsts[sliceIndex].Defn.ReplicaId = token.IndexInsts[sliceIndex].ReplicaId
			token.IndexInsts[sliceIndex].Defn.Using = common.IndexType(dest.StorageMode)
			if token.IndexInsts[sliceIndex].Pc != nil {
				token.IndexInsts[sliceIndex].Defn.NumPartitions = uint32(token.IndexInsts[sliceIndex].Pc.GetNumPartitions())
			}
			token.IndexInsts[sliceIndex].Pc = nil

			// Token exist for the same index replica between the same source and target.   Add partition to token.
			token.IndexInsts[sliceIndex].Defn.Partitions = append(token.IndexInsts[sliceIndex].Defn.Partitions, index.PartnId)
			token.IndexInsts[sliceIndex].Defn.Versions = append(token.IndexInsts[sliceIndex].Defn.Versions, index.Instance.Version)

		}
		return nil
	}

	for _, dest := range destSubCluster {
		for _, index := range dest.Indexes {

			//skip indexes which do not belong to the resumed tenant
			if index.Bucket != bucket {
				continue
			}

			// Primary index contians only one shard. Process primary
			// index shard after all secondary index tokens are generated
			// so that it can be added to one of the existing token for
			// the shard
			if len(index.ShardIds) == 1 {
				continue
			}

			shardIds := make([]common.ShardId, len(index.ShardIds))
			copy(shardIds, index.ShardIds)

			// one token for every pair of shard movements of a bucket between
			// a specific source and destination
			sort.Slice(shardIds, func(i, j int) bool {
				return shardIds[i] < shardIds[j]
			})

			//one token for resume of every tenant on every destination(currently there is
			//only one tenant and destination)
			tokenKey := fmt.Sprintf("%v %v %v", index.Bucket, index.ShardIds, dest.NodeUUID)
			if err := addIndexToToken(tokenKey, index, dest); err != nil {
				return nil, err
			}
		}
	}

	// Process primary indexes only
	for _, dest := range destSubCluster {
		for _, index := range dest.Indexes {

			//skip indexes which do not belong to the resumed tenant
			if index.Bucket != bucket {
				continue
			}

			if len(index.ShardIds) != 1 {
				continue
			}

			// Find a token key that contains the shard of this index
			// Note: Since tokens are grouped based on shardId's, it is
			// expected to have only one token matching the shardId of
			// primary index
			found := false
			for tokenKey, token := range tokens {

				if index.Bucket != token.IndexInsts[0].Defn.Bucket {
					continue
				}

				if dest.NodeUUID != token.FollowerId {
					continue
				}

				for _, shardId := range token.ShardIds {
					if index.ShardIds[0] == shardId {
						// Add index to this token
						addIndexToToken(tokenKey, index, dest)
						found = true
						break
					}

				}
			}
			// Could be only primary indexes in this shard
			// Generate a new token and add it to the group
			if !found {
				tokenKey := fmt.Sprintf("%v %v %v", index.Bucket, index.ShardIds, dest.NodeUUID)
				if err := addIndexToToken(tokenKey, index, dest); err != nil {
					return nil, err
				}
			}
		}
	}

	result := make(map[string]*common.ResumeDownloadToken)
	for _, token := range tokens {
		ustr, _ := common.NewUUID()
		rdtId := fmt.Sprintf("%s%s", common.ResumeDownloadTokenTag, ustr.Str())
		result[rdtId] = token
	}

	logResumeDownloadTokens := func() {
		for rdtid, token := range result {
			logging.Infof("Generating Resume Download Token (%v) for resume (%v)", rdtid, token)
		}
	}

	logResumeDownloadTokens()

	return result, nil
}

func getIndexesFromReplicaMap(replicaMap ReplicaDistMap) string {
	var str string
	for _, indexerMap := range replicaMap {
		for indexerNode, indexUsage := range indexerMap {
			str += fmt.Sprintf("Node: %v, numShards: %v, minShardCapacity: %v, Index name: %v, defnId: %v, replicaId: %v, partnId: %v\n",
				indexerNode.NodeId, indexerNode.numShards, indexerNode.MinShardCapacity,
				indexUsage.Name, indexUsage.DefnId,
				indexUsage.Instance.ReplicaId, indexUsage.PartnId)
		}
	}
	return str
}

func getLoadDist(nodes map[*IndexerNode]bool) string {
	var str string
	for k := range nodes {
		str += fmt.Sprintf("Node: %v, numShards: %v, minShardCap: %v\n", k.NodeId, k.numShards, k.MinShardCapacity)
	}
	return str
}

func PopulateAlternateShardIds(solution *Solution, indexes []*IndexUsage, binSize uint64) {

	targetNodes := make(map[*IndexerNode]map[*IndexUsage]bool)

	// Get the list of indexer nodes on to which the "indexes" are going to
	// be placed on
	for _, newIndex := range indexes {
		for _, indexer := range solution.Placement {
			for _, index := range indexer.Indexes {

				if index.IsShardProxy == false && index.DefnId == newIndex.DefnId &&
					index.Instance.ReplicaId == newIndex.Instance.ReplicaId {
					if _, ok := targetNodes[indexer]; !ok {
						targetNodes[indexer] = make(map[*IndexUsage]bool)
					}
					targetNodes[indexer][index] = true
				}

			}
		}
	}

	targetNodes = pruneIndexers(targetNodes) // Prune indexers that are <7.6 version

	// Group indexes for each partition
	// partnId -> replicaId -> indexerNode to corresponding index usage
	allPartnDist := getPartnDistribution(targetNodes)

	for _, partnDist := range allPartnDist {
		for _, replicaMap := range partnDist {

			shardLimitPerTenant, currShardCount := solution.getShardLimits()

			// Re-group indexes on the target nodes so that the index can use the grouping
			// done from earlier iteration
			for indexer := range targetNodes {
				indexer.Indexes, indexer.numShards, _ = GroupIndexes(indexer.Indexes, indexer)
			}

			// If a new shard can be created for this partition across all indexer nodes,
			// then generate new shardIds and populate the IndexUsage structure. A new shard
			// can be created if the current shard count is less than the shard limit per tenant
			if currShardCount <= shardLimitPerTenant && canCreateNewShards(replicaMap) {
				logging.LazyVerbose(func() string {
					return fmt.Sprintf("Planner::PopulateAlternateShardIds Generating new shards for index: %v "+
						"as there is room for new shards", getIndexesFromReplicaMap(replicaMap))
				})

				// If there is an error in generating alternateID's, do not fail
				// index creation. Ignore the error so that the index can still be
				// created without alternateId
				genAlterntateShardIds(replicaMap)

			} else {
				// Atleast one indexer node has reached the capacity of shards
				// Try placing the index on existing shards. If no such placement
				// if possible across the nodes, then force create new shards

				logging.LazyVerbose(func() string {
					return fmt.Sprintf("Planner::PopulateAlternateShardIds All indexer nodes are full with shards "+
						"Current shard capacity: %v, re-using existing shards", getIndexesFromReplicaMap(replicaMap))
				})

				allIndexerNodes := make(map[*IndexerNode]bool) // All indexer nodes in the cluster
				fullCapNodes := make(map[*IndexerNode]bool)    // All nodes where shard capacity is full
				for _, indexer := range solution.Placement {
					allIndexerNodes[indexer] = true
				}

				for indexer := range targetNodes {
					if indexer.numShards >= indexer.MinShardCapacity {
						fullCapNodes[indexer] = true
					}
				}

				logging.LazyVerbose(func() string {
					return fmt.Sprintf("Planner::PopulateAlternateShardIds Full capacity nodes are: %v", getLoadDist(fullCapNodes))
				})

				shardSlots, replan := pruneAndSortByLoad(allIndexerNodes, fullCapNodes, targetNodes, replicaMap, binSize)
				if replan {
					logging.Warnf("Planner::populateAlternateShardIds Re-planning placement as no common shard has been found")

					// TODO: Do actual replanning instead of just assining new shardIds
					genAlterntateShardIds(replicaMap)

				} else {
					// Assign alternate Ids based on the least loaded slot
					slot := shardSlots[0]
					logging.Infof("Planner::populateAlternateShardIds Using slot: %v for placing replicas: %v as "+
						"it is least loaded", slot[0].slotId, getIndexesFromReplicaMap(replicaMap))
					assignAlternateIds(replicaMap, slot, targetNodes)
				}
			}
		}
	}
}

func findReplicaProxies(solution *Solution, needsRepairProxy *IndexUsage) []*IndexUsage {
	alternateId, _ := common.ParseAlternateId(needsRepairProxy.AlternateShardIds[0])
	slotId := alternateId.SlotId
	var out []*IndexUsage

	for _, indexer := range solution.Placement {
		for _, index := range indexer.Indexes {
			if index.IsShardProxy == false || index.initialNode == nil {
				continue
			}

			indexAlternateId, _ := common.ParseAlternateId(index.AlternateShardIds[0])

			if indexAlternateId.SlotId != slotId {
				continue
			}

			out = append(out, index)
		}
	}
	return out
}

func getDiskUsageAndMatchingInstances(replicaProxy, needsRepairProxy *IndexUsage) (uint64, int) {
	var totalDiskUsage uint64
	replicaInsts := 0
	for _, index := range needsRepairProxy.GroupedIndexes {
		for _, replicaIndex := range replicaProxy.GroupedIndexes {
			if index.IsReplica(replicaIndex) {
				replicaInsts++
				totalDiskUsage += replicaIndex.ActualDiskSize
			}
		}
	}
	return totalDiskUsage, replicaInsts
}

func sortProxiesByDiskUsage(replicaProxies []*IndexUsage, needsRepairProxy *IndexUsage, binSize uint64) []*IndexUsage {

	if binSize == 0 {
		binSize = 1 // '1' means that each index will be compared based on their actual disk size
	}

	type proxyDiskUsage struct {
		diskSize         uint64
		replicaInstances int
		replicaIndex     *IndexUsage
	}

	proxyUsages := []*proxyDiskUsage{}

	for _, replicaProxy := range replicaProxies {
		diskUsage, replicaInsts := getDiskUsageAndMatchingInstances(replicaProxy, needsRepairProxy)
		proxyUsages = append(proxyUsages, &proxyDiskUsage{diskUsage, replicaInsts, replicaProxy})
	}

	sort.Slice(proxyUsages, func(i, j int) bool {
		iSlot, jSlot := proxyUsages[i].diskSize/binSize, proxyUsages[j].diskSize/binSize
		a := iSlot < jSlot
		b := (iSlot == jSlot) && (proxyUsages[i].replicaInstances < proxyUsages[i].replicaInstances)
		c := (iSlot == jSlot) && (proxyUsages[i].replicaInstances == proxyUsages[i].replicaInstances) &&
			(proxyUsages[i].diskSize < proxyUsages[j].diskSize)
		return a || b || c
	})

	var out []*IndexUsage
	for i := range proxyUsages {
		out = append(out, proxyUsages[i].replicaIndex)
	}
	return out
}

// Sibling index is only populated for shard proxies where an entire shard
// has to be repaired (E.g., the node containing the shard is failed over)
//
// The initialNode for a shard that needs repair will be to nil
func PopulateSiblingIndexForReplicaRepair(solution *Solution, binSize uint64) {
	shardsRequiringRepair := []*IndexUsage{}
	for _, indexer := range solution.Placement {
		for _, index := range indexer.Indexes {
			if index.IsShardProxy == false || index.initialNode != nil {
				continue
			}

			// Index is a shard proxy and initialNode is nil
			shardsRequiringRepair = append(shardsRequiringRepair, index)
		}
	}

	for _, needsRepairProxy := range shardsRequiringRepair {
		replicaProxies := findReplicaProxies(solution, needsRepairProxy)

		if len(replicaProxies) == 0 {
			continue
		}

		// Atleast one replica shard exits. Since there can be more than
		// one replica shard, use the shard that has covered higher disk
		// size for all the instances of interest so that the time taken
		// to copy the shard is minimized
		replicaProxies = sortProxiesByDiskUsage(replicaProxies, needsRepairProxy, binSize)
		needsRepairProxy.siblingIndex = replicaProxies[0]
		logging.Infof("PopulateSiblingIndexForReplicaRepair: Using sibling index: %v from node: %v for replica repair",
			needsRepairProxy.siblingIndex.Name, needsRepairProxy.siblingIndex.initialNode.NodeId)
	}
}

func pruneIndexers(nodes map[*IndexerNode]map[*IndexUsage]bool) map[*IndexerNode]map[*IndexUsage]bool {
	resp := make(map[*IndexerNode]map[*IndexUsage]bool)
	for indexer, indexUsageMap := range nodes {
		if indexer.NodeVersion < common.INDEXER_76_VERSION {
			continue
		}

		resp[indexer] = indexUsageMap
	}

	return resp
}

// Generates a distribution map for each partition across all definitions
// Mapping contains defnId -> partnId -> replicaId -> indexerNode to corresponding index usage
func getPartnDistribution(nodes map[*IndexerNode]map[*IndexUsage]bool) map[common.IndexDefnId]PartnDistMap {
	allPartnDist := make(map[common.IndexDefnId]PartnDistMap)

	for indexer, indexUsageMap := range nodes {
		for indexUsage, _ := range indexUsageMap {
			defnId := indexUsage.DefnId
			partnId := indexUsage.PartnId
			replicaId := indexUsage.Instance.ReplicaId

			if _, ok := allPartnDist[defnId]; !ok {
				allPartnDist[defnId] = make(PartnDistMap)
			}

			if _, ok := allPartnDist[defnId][partnId]; !ok {
				allPartnDist[defnId][partnId] = make(ReplicaDistMap)
			}

			if _, ok := allPartnDist[defnId][partnId][replicaId]; !ok {
				allPartnDist[defnId][partnId][replicaId] = make(map[*IndexerNode]*IndexUsage)
			}

			allPartnDist[defnId][partnId][replicaId][indexer] = indexUsage
		}
	}

	return allPartnDist
}

func canCreateNewShards(replicaMap ReplicaDistMap) bool {
	for _, indexerMap := range replicaMap {
		for indexer := range indexerMap {

			// One of the indexer nodes has reached the minimum number of shards limit
			// Return false so that planner will try populating existing shards
			// instead of creating new shards
			if indexer.numShards >= indexer.MinShardCapacity {
				return false
			}
		}
	}
	return true
}

func genAlterntateShardIds(replicaMap ReplicaDistMap) error {

	// All replicas share same slotId. Hence, generate a single slotId
	// and re-use the slotId for all replicas
	var alternateId *common.AlternateShardId
	var err error
	retry := 0
	for retry < 10 {
		alternateId, err = common.NewAlternateId()
		if err == nil {
			break
		}

		logging.Errorf("genAlternateShardIds: Error observed while generating alternateShardId, err: %v", err)
		time.Sleep(10 * time.Millisecond)
		retry++
	}
	if alternateId == nil || err != nil {
		return err
	}

	for replicaId, indexerMap := range replicaMap {
		for indexerNode, indexUsage := range indexerMap {

			alternateId.SetReplicaGroup(uint8(replicaId))
			if indexUsage.IsPrimary {
				alternateId.SetInstaceGroup(0)
				logging.Infof("genAlternateShardIds: Generating new alternageShardId: %v for primary index "+
					"name: %v, defnId: %v, replicaId: %v, partnId: %v",
					alternateId.String(), indexUsage.Name, indexUsage.DefnId,
					indexUsage.Instance.ReplicaId, indexUsage.PartnId)

				indexUsage.AlternateShardIds = []string{alternateId.String()}
				indexerNode.numShards += 1 // Increment new shard count as more shards are getting created
			} else {
				alternateId.SetInstaceGroup(0)
				msAltId := alternateId.String() // compute mainstore alternate ID
				alternateId.SetInstaceGroup(1)
				bsAltId := alternateId.String() // compute backstore alternate ID

				logging.Infof("genAlternateShardIds: Generating new alternateShardIds: [%v, %v] for index "+
					"name: %v, defnId: %v, replicaId: %v, partnId: %v",
					msAltId, bsAltId, indexUsage.Name, indexUsage.DefnId,
					indexUsage.Instance.ReplicaId, indexUsage.PartnId)

				indexUsage.AlternateShardIds = []string{msAltId, bsAltId}
				indexerNode.numShards += 2
			}
		}
	}
	return nil
}

type ReplicaLoad struct {
	ReplicaId int
	node      *IndexerNode
	usage     *IndexUsage
}

func (r *ReplicaLoad) String() string {
	var out string
	out += fmt.Sprintf("[replicaId: %v, node: %v, numShards: %v, minShardCap: %v, diskSize: %v, numInsts: %v], ",
		r.ReplicaId, r.node.NodeId, r.node.numShards, r.node.MinShardCapacity, r.usage.ActualDiskSize, r.usage.NumInstances)
	return out
}

// ShardLoad
// Captures the load of a shard "slot" across all nodes in the cluster
type ShardLoad struct {
	slotId uint64

	replicas []*ReplicaLoad

	// Maximum number of instances the shard with "slotId" holds
	// on any node in the cluster
	maxInstances uint64

	// Disk size of the shard with "slotId" across all nodes in
	// the cluster
	totalDiskSize uint64
}

func (s *ShardLoad) String() string {
	var out string
	out += fmt.Sprintf("Slot: %v, ReplicaDist:\n", s.slotId)
	for _, v := range s.replicas {
		out += fmt.Sprintf("\t\t%v,", v.String())
	}
	out += fmt.Sprintf("TotalDiskSize: %v, MaxInstances: %v ", s.totalDiskSize, s.maxInstances)
	return out
}

// Returns the shard distribution for nodes
func getShardDist(nodes map[*IndexerNode]bool) map[uint64]*ShardLoad {

	shardDist := make(map[uint64]*ShardLoad)
	for indexer := range nodes {
		for _, index := range indexer.Indexes {
			if index.IsShardProxy == false {
				continue
			}

			alternateId, _ := common.ParseAlternateId(index.AlternateShardIds[0])
			slotId, replicaId := alternateId.SlotId, int(alternateId.ReplicaId)

			if _, ok := shardDist[slotId]; !ok {
				shardDist[slotId] = &ShardLoad{
					slotId: slotId,
				}
			}

			shardDist[slotId].replicas = append(shardDist[slotId].replicas, &ReplicaLoad{replicaId, indexer, index})
			shardDist[slotId].totalDiskSize += index.ActualDiskSize
			shardDist[slotId].maxInstances = max(shardDist[slotId].maxInstances, index.NumInstances)
		}
	}

	return shardDist
}

func pruneAndSortByLoad(allIndexerNodes, fullCapNodes map[*IndexerNode]bool,
	targetNodes map[*IndexerNode]map[*IndexUsage]bool,
	replicaMap map[int]map[*IndexerNode]*IndexUsage, binSize uint64) ([][2]*ShardLoad, bool) {

	//allShardDist := getShardDist(allIndexerNodes)
	shardDistOnFullCapNodes := getShardDist(fullCapNodes)

	// Step-1: Prune all the shards that do not exist on all full capacity nodes
	// Even if the shard does not exist on one full capacity node, indexer may have
	// to allocate a shard on the full capacity node - which violates the capacity
	// constraint. If no common shard exists, then planner will replan the placement
	//
	// If a shard exists on all full capacity nodes but the shard usage is beyond the
	// capacity, then prune the shard

	for slotId, shardLoad := range shardDistOnFullCapNodes {
		if len(shardLoad.replicas) != len(fullCapNodes) {
			logging.Verbosef("Planner::pruneAndSortByLoad pruning slot: %v as it is not present on "+
				"all full capacity nodes. shardLoad: %v", shardLoad.String())
			delete(shardDistOnFullCapNodes, slotId)
			continue
		}

		for _, replica := range shardLoad.replicas {
			diskUsageThreshold := uint64(float64(replica.usage.MaxDiskSize) * (replica.usage.DiskSizeThreshold / 100.0))
			if replica.usage.ActualDiskSize >= diskUsageThreshold || replica.usage.NumInstances >= replica.usage.MaxInstances {
				logging.Verbosef("Planner::pruneAndSortByLoad pruning slot: %v as the disk usage is beyond threshold or "+
					"the shard has hit maximum instance capacity. shardLoad: %v", slotId, shardLoad.String())
				delete(shardDistOnFullCapNodes, slotId)
				continue
			}
		}
	}

	if len(shardDistOnFullCapNodes) == 0 {
		return nil, true // re-plan
	}

	// Step-2: Among the shards that exist on all full capacity nodes,
	// make sure that the replicas of those shards are with in the replicas of interest
	//
	// E.g., if nodes n1 & n2 are full at its capacity and has a shard with replicaIds: 2, 3
	// on each of them respectively but planner wants to create only replicas 0 & 1, then
	// such shards can not be used
	for slotId, shardLoad := range shardDistOnFullCapNodes {
		for _, replicaDist := range shardLoad.replicas {

			if _, ok := replicaMap[replicaDist.ReplicaId]; !ok {
				logging.Verbosef("Planner::pruneAndSortByLoad pruning slot: %v as shard does not contain replicas of interest. "+
					"shardLoad: %v, replicaDist: %v", slotId, shardLoad.String(), getIndexesFromReplicaMap(replicaMap))
				// Shard replica is not of interest for index creation
				delete(shardDistOnFullCapNodes, slotId)
			}
		}
	}

	if len(shardDistOnFullCapNodes) == 0 {
		return nil, true // re-plan
	}

	// Step-3: For the remaining shards, it is possible that global distribution of replicas
	// can exist beyond the target nodes
	//
	// E.g., if there are 5 nodes n0, n1, n2, n3, n4 in the cluster and the target nodes are
	// n0, n1, n2 with n0, n1 being at full capacity, then do not choose a shard that exists
	// on n0 (replica: 0), n1 (replica: 1), n3 (replica: 2) as planner has to place (replica: 2)
	// on n2 while replica already exists on n3 i.e. if a replica of interest exists outside
	// target nodes, ignore the shard
	globalShardDist := getShardDist(allIndexerNodes)

	for slotId, shardLoad := range shardDistOnFullCapNodes {
		if globalDist, ok := globalShardDist[slotId]; ok {

			for _, replica := range globalDist.replicas {
				_, ok1 := replicaMap[replica.ReplicaId]
				_, ok2 := targetNodes[replica.node]

				// "ok1 && !ok2" means that replica of interest for this shard exists outside
				// target nodes. Skip this shard
				//
				// Note: Do not check for !ok1 case as the shard can have replica: 3 on a
				// different node but planner wants to create only replicas 0, 1 and 2. In
				// that case !ok1 will be true but the shard can still be eligible for placement
				if ok1 && !ok2 {
					logging.Verbosef("Planner::pruneAndSortByLoad pruning slot: %v as shards of interest are outside target nodes. "+
						"shardLoad: %v, replicaDist: %v", slotId, shardLoad.String(), getIndexesFromReplicaMap(replicaMap))
					delete(shardDistOnFullCapNodes, slotId)
				}
			}
		} else {
			// This case should never happen
			delete(shardDistOnFullCapNodes, slotId)
		}
	}

	if len(shardDistOnFullCapNodes) == 0 {
		return nil, true
	}

	var shardSlice [][2]*ShardLoad
	for slotId, v := range shardDistOnFullCapNodes {
		shardSlice = append(shardSlice, [2]*ShardLoad{v, globalShardDist[slotId]})
	}

	return sortByLoad(shardSlice, binSize), false
}

func sortByLoad(shardSlice [][2]*ShardLoad, binSize uint64) [][2]*ShardLoad {

	if binSize == 0 {
		binSize = 1 // '1' means that each index will be compared based on their actual disk size
	}

	// Instead of storing based on totalDiskSize, use bin based sorting
	// If two shards have usage of 20G and 21G but the shard with 20G is having
	// more indexes, then it is ideal to place the new indexes on the shard with
	// 21G of disk usage.
	//
	// To facilitate such sorting, the totalDiskSize is divided into bins of 2.5G size
	// (1% of maximum disk usage threshold)
	// If two shards fall in the same bin, then the number of instances are compared
	// If the number of instances match, then the absolute value of disk size is used
	sort.Slice(shardSlice, func(i, j int) bool {
		iSlot, jSlot := shardSlice[i][0].totalDiskSize/binSize, shardSlice[j][0].totalDiskSize/binSize
		a := iSlot < jSlot
		b := (iSlot == jSlot) && (shardSlice[i][0].maxInstances < shardSlice[j][0].maxInstances)
		c := (iSlot == jSlot) && (shardSlice[i][0].maxInstances == shardSlice[j][0].maxInstances) &&
			(shardSlice[i][0].totalDiskSize < shardSlice[j][0].totalDiskSize)
		return a || b || c
	})

	return shardSlice
}

func assignAlternateIds(replicaMap map[int]map[*IndexerNode]*IndexUsage, slot [2]*ShardLoad, targetNodes map[*IndexerNode]map[*IndexUsage]bool) {

	slotId := slot[0].slotId
	slotDist := make(map[*IndexerNode]int)
	for _, replica := range slot[1].replicas {
		slotDist[replica.node] = replica.ReplicaId
	}

	assignedReplicas := make(map[int]bool)
	var remainingIndexes []*IndexUsage
	for _, indexDist := range replicaMap {
		for indexerNode, index := range indexDist {
			if val, ok := slotDist[indexerNode]; ok {
				index.Instance.ReplicaId = val
				index.AlternateShardIds = []string{fmt.Sprintf("%v-%v-0", slotId, val), fmt.Sprintf("%v-%v-1", slotId, val)}
				assignedReplicas[val] = true
			} else {
				remainingIndexes = append(remainingIndexes, index)
			}
		}
	}

	i := 0
	for replicaId, _ := range replicaMap {
		if _, ok := assignedReplicas[replicaId]; !ok {
			index := remainingIndexes[i]
			index.Instance.ReplicaId = replicaId
			msAltId := &common.AlternateShardId{SlotId: slotId, ReplicaId: uint8(replicaId), GroupId: 0}
			bsAltId := &common.AlternateShardId{SlotId: slotId, ReplicaId: uint8(replicaId), GroupId: 1}
			index.AlternateShardIds = []string{msAltId.String(), bsAltId.String()}
			i++
		}
	}
}
