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
	"time"

	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/common/collections"
	"github.com/couchbase/indexing/secondary/iowrap"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/security"
)

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
	// placement of indexes	in nodes
	Placement []*IndexerNode `json:"placement,omitempty"`
	MemQuota  uint64         `json:"memQuota,omitempty"`
	CpuQuota  uint64         `json:"cpuQuota,omitempty"`
	IsLive    bool           `json:"isLive,omitempty"`

	UsedReplicaIdMap map[common.IndexDefnId]map[int]bool

	SubClusters []SubCluster
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

//Resource Usage Thresholds for serverless model
type UsageThreshold struct {
	MemHighThreshold int32
	MemLowThreshold  int32
}

//////////////////////////////////////////////////////////////
// Integration with Rebalancer
/////////////////////////////////////////////////////////////

func ExecuteRebalance(clusterUrl string, topologyChange service.TopologyChange, masterId string, ejectOnly bool,
	disableReplicaRepair bool, threshold float64, timeout int, cpuProfile bool, minIterPerTemp int,
	maxIterPerTemp int) (map[string]*common.TransferToken, map[string]map[common.IndexDefnId]*common.IndexDefn, error) {
	runtime := time.Now()
	return ExecuteRebalanceInternal(clusterUrl, topologyChange, masterId, false, true, ejectOnly, disableReplicaRepair,
		timeout, threshold, cpuProfile, minIterPerTemp, maxIterPerTemp, &runtime)
}

func ExecuteRebalanceInternal(clusterUrl string,
	topologyChange service.TopologyChange, masterId string, addNode bool, detail bool, ejectOnly bool,
	disableReplicaRepair bool, timeout int, threshold float64, cpuProfile bool, minIterPerTemp, maxIterPerTemp int,
	runtime *time.Time) (map[string]*common.TransferToken, map[string]map[common.IndexDefnId]*common.IndexDefn, error) {

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

	transferTokens, err := genTransferToken(p.Result, masterId, topologyChange, deleteNodes)
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
					if index.pendingBuild && !index.pendingDelete {
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

//////////////////////////////////////////////////////////////
// Integration with Metadata Provider
/////////////////////////////////////////////////////////////

func ExecutePlan(clusterUrl string, indexSpecs []*IndexSpec, nodes []string, override bool, useGreedyPlanner bool, enforceLimits bool) (*Solution, error) {

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
	return ExecutePlanWithOptions(plan, indexSpecs, detail, "", "", -1, -1, -1, false, true, useGreedyPlanner)
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

func ExecuteReplicaRepair(clusterUrl string, defnId common.IndexDefnId, increment int, nodes []string, override bool, enforceLimits bool) (*Solution, error) {

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
	useGreedyPlanner bool) (*Solution, error) {

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
	output string, addNode int, cpuQuota int, memQuota int64, allowUnpin bool, deletedNodes []string) (*Solution, error) {

	config := DefaultRunConfig()
	config.Detail = detail
	config.GenStmt = genStmt
	config.Resize = false
	config.Output = output
	config.AddNode = addNode
	config.MemQuota = memQuota
	config.CpuQuota = cpuQuota
	config.AllowUnpin = allowUnpin

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
	output string, addNode int, cpuQuota int, memQuota int64, allowUnpin bool, deletedNodes []string) (*Solution, error) {

	config := DefaultRunConfig()
	config.Detail = detail
	config.GenStmt = genStmt
	config.Resize = false
	config.Output = output
	config.AddNode = addNode
	config.MemQuota = memQuota
	config.CpuQuota = cpuQuota
	config.AllowUnpin = allowUnpin

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

	if command == CommandRebalance && (len(outIndexes) != 0 || solution.findNumEmptyNodes() != 0) {
		partitioned := findAllPartitionedIndexExcluding(solution, indexes)
		if len(partitioned) != 0 {
			indexes = append(indexes, partitioned...)
		}
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

//ExecutePlan2 is the entry point for tenant aware planner
//for integration with metadata provider.
func ExecutePlan2(clusterUrl string, indexSpec *IndexSpec, nodes []string,
	usageThreshold *UsageThreshold, serverlessIndexLimit uint32) (*Solution, error) {

	plan, err := RetrievePlanFromCluster(clusterUrl, nodes, false)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Unable to read index layout "+
			"from cluster %v. err = %s", clusterUrl, err))
	}

	numIndexes := GetNumIndexesPerBucket(plan, indexSpec.Bucket)
	if numIndexes >= serverlessIndexLimit {
		errMsg := fmt.Sprintf("%v Limit : %v", common.ErrIndexBucketLimitReached.Error(), serverlessIndexLimit)
		return nil, errors.New(errMsg)
	}

	if err = verifyDuplicateIndex(plan, []*IndexSpec{indexSpec}); err != nil {
		return nil, err
	}

	solution, err := executeTenantAwarePlan(plan, indexSpec, usageThreshold)

	return solution, err

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
//6. Index of a new tenant can be placed on a sub-cluster with usage
//   lower than LWM(Low Watermark Threshold).
//7. Index of an existing tenant can be placed on a sub-cluster with usage
//   lower than HWM(High Watermark Threshold).
//8. No Index can be placed on a node above HWM(High Watermark Threshold).

func executeTenantAwarePlan(plan *Plan, indexSpec *IndexSpec,
	usageThreshold *UsageThreshold) (*Solution, error) {

	const _executeTenantAwarePlan = "Planner::executeTenantAwarePlan:"

	solution := solutionFromPlan2(plan)

	subClusters, err := groupIndexNodesIntoSubClusters(solution.Placement)
	if err != nil {
		return nil, err
	}

	err = validateSubClusterGrouping(subClusters, indexSpec)
	if err != nil {
		return nil, err
	}

	logging.Infof("%v Found SubClusters  %v", _executeTenantAwarePlan, subClusters)

	candidates, err := findCandidateSubClustersBasedOnUsage(subClusters,
		plan.MemQuota, usageThreshold)
	if err != nil {
		return nil, err
	}

	logging.Infof("%v Found Candidates %v", _executeTenantAwarePlan, candidates)

	result, err := filterCandidatesBasedOnTenantAffinity(candidates, indexSpec)
	if err != nil {
		return nil, err
	}

	if len(result) == 0 {
		logging.Infof("%v Found no matching candidate for tenant %v", _executeTenantAwarePlan,
			indexSpec)
		return nil, errors.New("Planner not able to find any node for placement")
	} else {
		logging.Infof("%v Found Result %v", _executeTenantAwarePlan, result)
	}

	indexes, err := IndexUsagesFromSpec(nil, []*IndexSpec{indexSpec})
	if err != nil {
		return nil, err
	}

	err = placeIndexOnSubCluster(result, indexes)
	if err != nil {
		return nil, err
	}

	return solution, nil

}

//solutionFromPlan2 creates a Solution from the input Plan.
//This function does a shallow copy of the Placement from the Plan
//to create a Solution.
func solutionFromPlan2(plan *Plan) *Solution {

	s := &Solution{
		Placement: make([]*IndexerNode, len(plan.Placement)),
	}

	for i, indexer := range plan.Placement {
		s.Placement[i] = indexer
	}

	return s

}

//groupIndexNodesIntoSubClusters groups index nodes into sub-clusters.
//Nodes are considered to be part of a sub-cluster if those are hosting
//replicas of the same index. Empty nodes can be paired into a sub-cluster
//if those belong to a different server group.
func groupIndexNodesIntoSubClusters(indexers []*IndexerNode) ([]SubCluster, error) {

	var err error
	var subClusters []SubCluster

	for _, node := range indexers {
		var subcluster SubCluster

		if checkIfNodeBelongsToAnySubCluster(subClusters, node) {
			continue
		}

		//if there are no indexes on a node
		if len(node.Indexes) == 0 {
			subcluster, err = findSubClusterForEmptyNode(indexers, node, subClusters)
			if err != nil {
				return nil, err
			}
		}

		for _, index := range node.Indexes {
			subcluster, err = findSubClusterForIndex(indexers, index)
			if err != nil {
				return nil, err
			}
			break
		}
		subClusters = append(subClusters, subcluster)
	}

	return subClusters, nil
}

//validateSubClusterGrouping validates the constraints for
//sub-cluster grouping.
func validateSubClusterGrouping(subClusters []SubCluster,
	indexSpec *IndexSpec) error {

	const _validateSubClusterGrouping = "Planner::validateSubClusterGrouping:"

	for _, subCluster := range subClusters {

		//Number of nodes in the subCluster must not be greater than
		//number of replicas. It can be less than number of replicas in
		//case of a failed over node.
		if len(subCluster) > int(indexSpec.Replica) {
			logging.Errorf("%v SubCluster %v has more nodes than number of replicas %v.", _validateSubClusterGrouping,
				subCluster, indexSpec.Replica)
			return common.ErrPlannerConstraintViolation
		}
		sgMap := make(map[string]bool)
		for _, indexer := range subCluster {
			//All nodes in a sub-cluster must belong to a different server group
			if _, ok := sgMap[indexer.ServerGroup]; ok {
				logging.Errorf("%v SubCluster %v has multiple nodes in a server group.", _validateSubClusterGrouping,
					subCluster)
				return common.ErrPlannerConstraintViolation
			} else {
				sgMap[indexer.ServerGroup] = true
			}
		}
	}

	return nil
}

//filterCandidatesBasedOnTenantAffinity finds candidate sub-cluster
//based on tenant affinity
func filterCandidatesBasedOnTenantAffinity(subClusters []SubCluster,
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
	//from the matching ones. /For phase 1, there will be only 1 matching
	//sub-cluster as a single tenant's indexes can only be placed on one node.
	if len(candidates) >= 1 {
		result = findLeastLoadedSubCluster(candidates)
		return result, nil
	}

	//No node found with matching tenant. Choose least loaded subCluster
	//from all input subClusters.
	if len(candidates) == 0 {
		result = findLeastLoadedSubCluster(subClusters)
		return result, nil
	}

	return nil, nil

}

//findLeastLoadedSubCluster finds the least loaded sub-cluster from the input
//list of sub-clusters. Load is calculated based on the actual RSS of the indexer
//process on the node.
func findLeastLoadedSubCluster(subClusters []SubCluster) SubCluster {

	//TODO This code assumes identically loaded nodes in the sub-cluster.
	//When indexes move during rebalance, this may not always be true.

	var leastLoad uint64
	var result SubCluster

	for _, subCluster := range subClusters {
		for _, indexer := range subCluster {
			if leastLoad == 0 || indexer.ActualRSS < leastLoad {
				leastLoad = indexer.ActualRSS
				result = subCluster
			}
			break
		}
	}

	return result

}

//findCandidateSubClustersBasedOnUsage finds candidates sub-clusters for
//placement based on resource usage. All sub-clusters below High Threshold
//are potential candidates for index placement.
func findCandidateSubClustersBasedOnUsage(subClusters []SubCluster, quota uint64,
	usageThreshold *UsageThreshold) ([]SubCluster, error) {

	candidates, err := findSubClusterBelowHighThreshold(subClusters,
		quota, usageThreshold)
	if err != nil {
		return nil, err
	}

	if len(candidates) == 0 {
		return nil, common.ErrPlannerMaxResourceUsageLimit
	}

	return candidates, nil
}

//findSubClusterBelowLowThreshold finds sub-clusters with usage lower than
//LWT(Low Watermark Threshold).
func findSubClusterBelowLowThreshold(subClusters []SubCluster, quota uint64,
	usageThreshold *UsageThreshold) ([]SubCluster, error) {

	var result []SubCluster
	var found bool

	for _, subCluster := range subClusters {
		for _, indexNode := range subCluster {
			//all nodes in the sub-cluster need to satisfy the condition
			found = true
			if indexNode.ActualRSS >
				(uint64(usageThreshold.MemLowThreshold)*quota)/100 {
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

//findSubClusterBelowHighThreshold finds sub-clusters with usage lower than
//HWT(High Watermark Threshold).
func findSubClusterBelowHighThreshold(subClusters []SubCluster, quota uint64,
	usageThreshold *UsageThreshold) ([]SubCluster, error) {

	var result []SubCluster
	var found bool

	for _, subCluster := range subClusters {
		found = true
		for _, indexNode := range subCluster {
			//all nodes in the sub-cluster need to satisfy the condition
			if indexNode.ActualRSS >
				(uint64(usageThreshold.MemHighThreshold)*quota)/100 {
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

//findSubClusterForEmptyNode finds another empty node in the cluster to
//pair with the input node to form a sub-cluster. If excludeNodes is specified,
//those nodes are excluded while finding the pair.
func findSubClusterForEmptyNode(indexers []*IndexerNode,
	node *IndexerNode, excludeNodes []SubCluster) (SubCluster, error) {

	//TODO handle the case if a node is failed over/unhealthy in the cluster.

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
	return nil, nil
}

//checkIfNodeBelongsToAnySubCluster checks if the given node belongs
//to the list of input sub-cluster based on its NodeUUID
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

//findSubClusterForIndex finds the sub-cluster for a given index.
//Nodes are considered to be part of a sub-cluster if
//those are hosting replicas of the same index.
func findSubClusterForIndex(indexers []*IndexerNode,
	index *IndexUsage) (SubCluster, error) {

	//TODO Handle cases for in-flight indexes during rebalance. Create Index will
	//be allowed during rebalance in Elixir.
	//TODO handle the case if a node is failed over/unhealthy in the cluster.

	var subCluster SubCluster
	for _, indexer := range indexers {
		for _, checkIdx := range indexer.Indexes {
			if index.DefnId == checkIdx.DefnId &&
				index.Instance.Version == checkIdx.Instance.Version &&
				index.PartnId == checkIdx.PartnId {
				subCluster = append(subCluster, indexer)
			}
		}
	}

	return subCluster, nil
}

//placeIndexOnSubCluster places the input list of indexes on the given sub-cluster
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
		Detail:         false,
		GenStmt:        "",
		MemQuotaFactor: 1.0,
		CpuQuotaFactor: 1.0,
		Resize:         true,
		MaxNumNode:     int(math.MaxInt16),
		Output:         "",
		Shuffle:        0,
		AllowMove:      false,
		AllowSwap:      true,
		AllowUnpin:     false,
		AddNode:        0,
		DeleteNode:     0,
		MaxMemUse:      -1,
		MaxCpuUse:      -1,
		MemQuota:       -1,
		CpuQuota:       -1,
		DataCostWeight: 1,
		CpuCostWeight:  1,
		MemCostWeight:  1,
		EjectOnly:      false,
		DisableRepair:  false,
		MinIterPerTemp: 100,
		MaxIterPerTemp: 20000,
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

	memQuota, cpuQuota := computeQuota(config, sizing, indexes, false)

	constraint := newIndexerConstraint(memQuota, cpuQuota, resize, maxNumNode, maxCpuUse, maxMemUse)

	indexers := indexerNodes(constraint, indexes, sizing, false)

	r := newSolution(constraint, sizing, indexers, false, false, config.DisableRepair)

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

	memQuota, cpuQuota := computeQuota(config, sizing, indexes, false)

	constraint := newIndexerConstraint(memQuota, cpuQuota, resize, maxNumNode, maxCpuUse, maxMemUse)

	r := newSolution(constraint, sizing, ([]*IndexerNode)(nil), false, false, config.DisableRepair)

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

	r := newSolution(constraint, sizing, plan.Placement, plan.IsLive, useLive, config.DisableRepair)
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

//
// This function is only called during placement to make existing index as
// eligible candidate for planner.
//
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

//
// Find all partitioned index in teh solution
//
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

//
// This function should only be called if there are new indexes to be placed.
//
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

//
// Set stats for index to be placed
//
func setIndexPlacementStats(s *RunStats, indexes []*IndexUsage, useLive bool) {
	s.AvgIndexSize, s.StdDevIndexSize = computeIndexMemStats(indexes, useLive)
	s.AvgIndexCpu, s.StdDevIndexCpu = computeIndexCpuStats(indexes, useLive)
	s.IndexCount = uint64(len(indexes))
}

//
// Set stats for initial layout
//
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
