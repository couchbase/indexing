// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package planner

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
)

//////////////////////////////////////////////////////////////
// Concrete Type/Struct
/////////////////////////////////////////////////////////////

type RunConfig struct {
	Detail         bool
	GenStmt        string
	MemQuotaFactor float64
	CpuQuotaFactor float64
	Resize         bool
	MaxNumNode     int
	Output         string
	Shuffle        int
	AllowMove      bool
	AllowSwap      bool
	AllowUnpin     bool
	AddNode        int
	DeleteNode     int
	MaxMemUse      int
	MaxCpuUse      int
	MemQuota       int64
	CpuQuota       int
	DataCostWeight float64
	CpuCostWeight  float64
	MemCostWeight  float64
	EjectOnly      bool
	DisableRepair  bool
	Timeout        int
	UseLive        bool
	Runtime        *time.Time
	Threshold      float64
	CpuProfile     bool
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

type Plan struct {
	// placement of indexes	in nodes
	Placement []*IndexerNode `json:"placement,omitempty"`
	MemQuota  uint64         `json:"memQuota,omitempty"`
	CpuQuota  uint64         `json:"cpuQuota,omitempty"`
	IsLive    bool           `json:"isLive,omitempty"`

	UsedReplicaIdMap map[common.IndexDefnId]map[int]bool
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

//////////////////////////////////////////////////////////////
// Integration with Rebalancer
/////////////////////////////////////////////////////////////

func ExecuteRebalance(clusterUrl string, topologyChange service.TopologyChange, masterId string, ejectOnly bool,
	disableReplicaRepair bool, threshold float64, timeout int, cpuProfile bool) (map[string]*common.TransferToken, error) {
	runtime := time.Now()
	return ExecuteRebalanceInternal(clusterUrl, topologyChange, masterId, false, true, ejectOnly, disableReplicaRepair,
		timeout, threshold, cpuProfile, &runtime)
}

func ExecuteRebalanceInternal(clusterUrl string,
	topologyChange service.TopologyChange, masterId string, addNode bool, detail bool, ejectOnly bool,
	disableReplicaRepair bool, timeout int, threshold float64, cpuProfile bool, runtime *time.Time) (map[string]*common.TransferToken, error) {

	plan, err := RetrievePlanFromCluster(clusterUrl, nil)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Unable to read index layout from cluster %v. err = %s", clusterUrl, err))
	}

	nodes := make(map[string]string)
	for _, node := range plan.Placement {
		nodes[node.NodeUUID] = node.NodeId
	}

	deleteNodes := make([]string, len(topologyChange.EjectNodes))
	for i, node := range topologyChange.EjectNodes {
		if _, ok := nodes[string(node.NodeID)]; !ok {
			return nil, errors.New(fmt.Sprintf("Unable to find indexer node with node UUID %v", node.NodeID))
		}
		deleteNodes[i] = nodes[string(node.NodeID)]
	}

	// make sure we have all the keep nodes
	for _, node := range topologyChange.KeepNodes {
		if _, ok := nodes[string(node.NodeInfo.NodeID)]; !ok {
			return nil, errors.New(fmt.Sprintf("Unable to find indexer node with node UUID %v", node.NodeInfo.NodeID))
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

	p, _, err := execute(config, CommandRebalance, plan, nil, deleteNodes)
	if p != nil && detail {
		logging.Infof("************ Indexer Layout *************")
		p.Print()
		logging.Infof("****************************************")
	}

	if err != nil {
		return nil, err
	}

	filterSolution(p.Result.Placement)

	transferTokens, err := genTransferToken(p.Result, masterId, topologyChange, deleteNodes)
	if err != nil {
		return nil, err
	}

	return transferTokens, nil
}

// filterSolution will iterate through the new placement generated
// by planner and filter out all un-necessary movements.
//
// E.g. if for an index, there exists 3 replicas on nodes n1 (replica 0),
// n2 (replica 1), n3 (replica 2) in a 4 node cluster (n1,n2,n3,n4) and
// if planner has generated a placement to move replica 0 from n1->n2,
// replica 1 from n2->n3 and replica 3 from n3->n4, the movement of replicas
// from n2 and n3 are unnecessary as replica instaces exist on the node before
// and after movement. filterSolution will elimimate all such movements and
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
						// Initial destination and final destiantion are same. No change
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

func genTransferToken(solution *Solution, masterId string, topologyChange service.TopologyChange,
	deleteNodes []string) (map[string]*common.TransferToken, error) {

	tokens := make(map[string]*common.TransferToken)

	for _, indexer := range solution.Placement {
		for _, index := range indexer.Indexes {
			if index.initialNode != nil && index.initialNode.NodeId != index.destNode.NodeId && !index.pendingCreate {

				// one token for every index replica between a specific source and destination
				tokenKey := fmt.Sprintf("%v %v %v %v", index.DefnId, index.Instance.ReplicaId, index.initialNode.NodeUUID, indexer.NodeUUID)

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
				tokenKey := fmt.Sprintf("%v %v %v %v", index.DefnId, index.Instance.ReplicaId, "N/A", indexer.NodeUUID)

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

func ExecutePlan(clusterUrl string, indexSpecs []*IndexSpec, nodes []string, override bool) (*Solution, error) {

	plan, err := RetrievePlanFromCluster(clusterUrl, nodes)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Unable to read index layout from cluster %v. err = %s", clusterUrl, err))
	}

	if override && len(nodes) != 0 {
		for _, indexer := range plan.Placement {
			found := false
			for _, node := range nodes {
				if indexer.NodeId == node {
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
	return ExecutePlanWithOptions(plan, indexSpecs, detail, "", "", -1, -1, -1, false, true)
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

	plan, err := RetrievePlanFromCluster(clusterUrl, nodes)
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

func ExecuteReplicaRepair(clusterUrl string, defnId common.IndexDefnId, increment int, nodes []string, override bool) (*Solution, error) {

	plan, err := RetrievePlanFromCluster(clusterUrl, nodes)
	if err != nil {
		return nil, fmt.Errorf("Unable to read index layout from cluster %v. err = %s", clusterUrl, err)
	}

	if override && len(nodes) != 0 {
		for _, indexer := range plan.Placement {
			found := false
			for _, node := range nodes {
				if indexer.NodeId == node {
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

	plan, err := RetrievePlanFromCluster(clusterUrl, nodes)
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

	plan, err := RetrievePlanFromCluster(clusterUrl, nodes)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Unable to read index layout from cluster %v. err = %s", clusterUrl, err))
	}

	config := DefaultRunConfig()
	config.Detail = logging.IsEnabled(logging.Info)
	config.Resize = false
	config.Output = output

	return ExecuteRetrieveWithOptions(plan, config)
}

func ExecuteRetrieveWithOptions(plan *Plan, config *RunConfig) (*Solution, error) {

	sizing := newGeneralSizingMethod()
	solution, constraint, _, _, _ := solutionFromPlan(CommandRebalance, config, sizing, plan)

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
	output string, addNode int, cpuQuota int, memQuota int64, allowUnpin bool, useLive bool) (*Solution, error) {

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

	p, _, err := execute(config, CommandPlan, plan, indexSpecs, ([]string)(nil))
	if p != nil && detail {
		logging.Infof("************ Indexer Layout *************")
		p.Print()
		logging.Infof("****************************************")
	}

	if p != nil {
		return p.Result, err
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

	p, _, err := execute(config, CommandRebalance, plan, indexSpecs, deletedNodes)

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

	p, _, err := execute(config, CommandSwap, plan, nil, deletedNodes)

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

func execute(config *RunConfig, command CommandType, p *Plan, indexSpecs []*IndexSpec, deletedNodes []string) (*SAPlanner, *RunStats, error) {

	var indexes []*IndexUsage
	var err error

	sizing := newGeneralSizingMethod()

	if command == CommandPlan {
		if indexSpecs != nil {
			indexes, err = IndexUsagesFromSpec(sizing, indexSpecs)
			if err != nil {
				return nil, nil, err
			}
		} else {
			return nil, nil, errors.New("missing argument: index spec must be present")
		}

		return plan(config, p, indexes)

	} else if command == CommandRebalance || command == CommandSwap {
		if p == nil {
			return nil, nil, errors.New("missing argument: either workload or plan must be present")
		}

		return rebalance(command, config, p, indexes, deletedNodes)

	} else {
		panic(fmt.Sprintf("uknown command: %v", command))
	}

	return nil, nil, nil
}

func plan(config *RunConfig, plan *Plan, indexes []*IndexUsage) (*SAPlanner, *RunStats, error) {

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

	// create a solution
	if plan != nil {
		// create a solution from plan
		var movedIndex, movedData uint64
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
	if err := placement.Add(solution, indexes); err != nil {
		return nil, nil, err
	}

	// run planner
	cost = newUsageBasedCostMethod(constraint, config.DataCostWeight, config.CpuCostWeight, config.MemCostWeight)
	planner := newSAPlanner(cost, constraint, placement, sizing)
	if _, err := planner.Plan(CommandPlan, solution); err != nil {
		return planner, s, err
	}

	// save result
	s.MemoryQuota = constraint.GetMemQuota()
	s.CpuQuota = constraint.GetCpuQuota()

	if config.Output != "" {
		if err := savePlan(config.Output, planner.Result, constraint); err != nil {
			return nil, nil, err
		}
	}

	if config.GenStmt != "" {
		if err := genCreateIndexDDL(config.GenStmt, planner.Result); err != nil {
			return nil, nil, err
		}
	}

	return planner, s, nil
}

func rebalance(command CommandType, config *RunConfig, plan *Plan, indexes []*IndexUsage, deletedNodes []string) (*SAPlanner, *RunStats, error) {

	var constraint ConstraintMethod
	var sizing SizingMethod
	var placement PlacementMethod
	var cost CostMethod

	var solution *Solution
	var initialIndexes []*IndexUsage
	var outIndexes []*IndexUsage

	var err error

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
			return nil, nil, err
		}
		setInitialLayoutStats(s, config, constraint, solution, initialIndexes, 0, 0, false)
	}

	// change topology before rebalancing
	outIndexes, err = changeTopology(config, solution, deletedNodes)
	if err != nil {
		return nil, nil, err
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

	// run planner
	placement = newRandomPlacement(indexes, config.AllowSwap, command == CommandSwap)
	cost = newUsageBasedCostMethod(constraint, config.DataCostWeight, config.CpuCostWeight, config.MemCostWeight)
	planner := newSAPlanner(cost, constraint, placement, sizing)
	planner.SetTimeout(config.Timeout)
	planner.SetRuntime(config.Runtime)
	planner.SetVariationThreshold(config.Threshold)
	planner.SetCpuProfile(config.CpuProfile)
	if config.Detail {
		logging.Infof("************ Index Layout Before Rebalance *************")
		solution.PrintLayout()
		logging.Infof("****************************************")
	}
	if _, err := planner.Plan(command, solution); err != nil {
		return planner, s, err
	}

	// save result
	s.MemoryQuota = constraint.GetMemQuota()
	s.CpuQuota = constraint.GetCpuQuota()

	if config.Output != "" {
		if err := savePlan(config.Output, planner.Result, constraint); err != nil {
			return nil, nil, err
		}
	}

	return planner, s, nil
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
		if err := ioutil.WriteFile(ddl, ([]byte)(stmts), os.ModePerm); err != nil {
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
	numVbuckets := config["indexer.numVbuckets"].Int()

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
			index.Instance.Pc = common.NewKeyPartitionContainer(numVbuckets, int(spec.NumPartition),
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
			sizing.ComputeIndexSize(index)

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

	err = ioutil.WriteFile(output, data, os.ModePerm)
	if err != nil {
		return errors.New(fmt.Sprintf("Unable to save plan into %v. err = %s", output, err))
	}

	return nil
}

func ReadPlan(planFile string) (*Plan, error) {

	if planFile != "" {

		plan := &Plan{}

		buf, err := ioutil.ReadFile(planFile)
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

		buf, err := ioutil.ReadFile(specFile)
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
