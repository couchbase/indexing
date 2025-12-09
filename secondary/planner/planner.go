// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package planner

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
)

//TODO

// - retry proxy when there is transient network error
// - tuning parameter (spock)
// - generate cpu usage stats for index (spock)
// - generate move index statement (spock)
// - handle cloned index in proxy (spock)
// - provide an option to find out index that violates HA property
// - support saving plan in utility

//////////////////////////////////////////////////////////////
// Constant
//////////////////////////////////////////////////////////////

// constant - simulated annealing
const (
	ResizePerIteration int     = 1000
	RunPerPlan         int     = 12
	MaxTemperature     float64 = 1.0
	MinTemperature     float64 = 0.00001
	Alpha              float64 = 0.90
	MinNumMove         int64   = 1
	MinNumPositiveMove int64   = 1
)

// constant - index sizing - MOI
const (
	MOIMutationRatePerCore uint64 = 25000
	MOIScanRatePerCore            = 5000
	MOIScanTimeout                = 120
)

// constant - command
type CommandType string

const (
	CommandPlan      CommandType = "plan"
	CommandRebalance             = "rebalance"
	CommandSwap                  = "swap"
	CommandRepair                = "repair"
	CommandDrop                  = "drop"
	CommandRetrieve              = "retrieve"
)

// constant - violation code
type ViolationCode string

const (
	NoViolation     ViolationCode = "NoViolation"
	MemoryViolation               = "MemoryViolation"
	// CpuViolation               = "CpuViolation"
	ReplicaViolation     = "ReplicaViolation"
	EquivIndexViolation  = "EquivIndexViolation"
	ServerGroupViolation = "ServerGroupViolation"
	DeleteNodeViolation  = "DeleteNodeViolation"
	ExcludeNodeViolation = "ExcludeNodeViolation"
)

const (
	EMPTY_INDEX_DATASIZE = 8
	EMPTY_INDEX_MEMUSAGE = 8
)

const cSubClusterLen = 2
const cScaleInUsageThreshold = 20

var ErrNoAvailableIndexer = errors.New("Cannot find any indexer that can add new indexes")

//////////////////////////////////////////////////////////////
// Interface
//////////////////////////////////////////////////////////////

type Planner interface {
	Plan(command CommandType, solution *Solution) (*Solution, error)
	GetResult() *Solution
	Print()
	PrintCost()
	SetParam(map[string]interface{}) error
	SetShardAffinity(bool)
}

type CostMethod interface {
	Cost(s *Solution) float64
	Print()
	Validate(s *Solution) error
	GetMemMean() float64
	GetCpuMean() float64
	GetDataMean() float64
	GetDiskMean() float64
	GetScanMean() float64
	GetDrainMean() float64
	ComputeResourceVariation() float64
}

type PlacementMethod interface {
	Move(s *Solution) (bool, bool, bool, error)
	Add(s *Solution, indexes []*IndexUsage) error
	AddToIndexer(s *Solution, indexer *IndexerNode, idx *IndexUsage)
	InitialPlace(s *Solution, indexes []*IndexUsage) error
	Validate(s *Solution) error
	GetEligibleIndexes() map[*IndexUsage]bool
	IsEligibleIndex(*IndexUsage) bool
	AddOptionalIndexes([]*IndexUsage)
	AddRequiredIndexes([]*IndexUsage)
	RemoveOptionalIndexes() []*IndexUsage
	HasOptionalIndexes() bool
	RemoveEligibleIndex([]*IndexUsage)
	RegroupIndexes(bool)
	Print()
}

type ConstraintMethod interface {
	GetMemQuota() uint64
	GetCpuQuota() uint64
	SatisfyClusterResourceConstraint(s *Solution) bool
	SatisfyNodeResourceConstraint(s *Solution, n *IndexerNode) bool
	SatisfyNodeHAConstraint(s *Solution, n *IndexerNode, eligibles map[*IndexUsage]bool, chkEquiv bool) bool
	SatisfyIndexHAConstraint(s *Solution, n *IndexerNode, index *IndexUsage, eligibles map[*IndexUsage]bool, chkEquiv bool) bool
	SatisfyClusterConstraint(s *Solution, eligibles map[*IndexUsage]bool) bool
	SatisfyNodeConstraint(s *Solution, n *IndexerNode, eligibles map[*IndexUsage]bool) bool
	SatisfyServerGroupConstraint(s *Solution, n *IndexUsage, group string) bool
	CanAddIndex(s *Solution, n *IndexerNode, u *IndexUsage) ViolationCode
	CanSwapIndex(s *Solution, n *IndexerNode, t *IndexUsage, i *IndexUsage) ViolationCode
	CanAddNode(s *Solution) bool
	Print()
	Validate(s *Solution) error
	GetViolations(s *Solution, indexes map[*IndexUsage]bool) *Violations
	SatisfyClusterHAConstraint(s *Solution, eligibles map[*IndexUsage]bool, chkEquiv bool) bool
}

type SizingMethod interface {
	ComputeIndexSize(u *IndexUsage)
	ComputeIndexerOverhead(n *IndexerNode)
	ComputeIndexerSize(n *IndexerNode)
	ComputeIndexOverhead(idx *IndexUsage) uint64
	ComputeMinQuota(u []*IndexUsage, useLive bool) (uint64, uint64)
	Validate(s *Solution) error
}

//////////////////////////////////////////////////////////////
// Concrete Type/Struct
//////////////////////////////////////////////////////////////

type ServerGroupMap map[common.IndexDefnId]map[common.PartitionId]map[int]string
type ReplicaMap map[common.IndexDefnId]map[common.PartitionId]map[int]*IndexUsage
type UsedReplicaIdMap map[common.IndexDefnId]map[int]bool

type Violations struct {
	Violations []*Violation
	MemQuota   uint64
	CpuQuota   uint64
}

type Violation struct {
	Name       string
	Bucket     string
	Scope      string
	Collection string
	NodeId     string
	CpuUsage   float64
	MemUsage   uint64
	Details    []string
}

//////////////////////////////////////////////////////////////
// Interface Implementation - Planner
//////////////////////////////////////////////////////////////

type SAPlanner struct {
	placement  PlacementMethod
	cost       CostMethod
	constraint ConstraintMethod
	sizing     SizingMethod

	// config
	timeout        int
	runtime        *time.Time
	threshold      float64
	cpuProfile     bool
	minIterPerTemp int
	maxIterPerTemp int

	// Set to true if indexes are to grouped based on shard affinity
	shardAffinity bool

	// result
	Result          *Solution `json:"result,omitempty"`
	Score           float64   `json:"score,omitempty"`
	ElapseTime      uint64    `json:"elapsedTime,omitempty"`
	ConvergenceTime uint64    `json:"convergenceTime,omitempty"`
	Iteration       uint64    `json:"iteration,omitempty"`
	Move            uint64    `json:"move,omitempty"`
	PositiveMove    uint64    `json:"positiveMove,omitempty"`
	StartTemp       float64   `json:"startTemp,omitempty"`
	StartScore      float64   `json:"startScore,omitempty"`
	Try             uint64    `json:"try,omitempty"`
}

// SAPlanner uses simulated annealing method to find optimal index
// placement. When the index is being created, the placement of existing
// indexes won't change. Only the indexes that are newly getting created
// will get placed optimally. For this, the planner finds the initial
// node for the new indexes randomly - and then performs simulated
// annealing to find the optimal target node. This step of performing
// simulated annealing tries large number of solutions - by randomly moving
// one new index to a differrent target node - in every step.
//
// When a single index is getting created, with limited number of nodes
// present in the cluster, finding the optimal solution should not require
// large number of random index movements (performed by simulated annealing),
// but deterministic placement should work trivially. The only exception being
// the placement of index partitions. So, one can use a greedy approach
// and place the new indexes on the least loaded nodes while making sure that
// the replica and server group constraints are satisfied.
type GreedyPlanner struct {
	placement  PlacementMethod
	cost       CostMethod
	constraint ConstraintMethod
	sizing     SizingMethod

	newIndexes []*IndexUsage

	equivIndexMap map[string]bool
	numEquivIndex int

	Result *Solution

	numNewIndexes int

	shardAffinity bool

	// SGs which have atleast one node without an equivalent index
	sgHasNodeWithoutEquivIdx map[string]bool
}

type TenantAwarePlanner struct {
	Result *Solution
}

//////////////////////////////////////////////////////////////
// Interface Implementation - SizingMethod
//////////////////////////////////////////////////////////////

type GeneralSizingMethod struct {
	MOI    *MOISizingMethod
	Plasma *PlasmaSizingMethod
}

type MOISizingMethod struct {
}

type PlasmaSizingMethod struct {
}

//////////////////////////////////////////////////////////////
// Sync pool
//////////////////////////////////////////////////////////////

type SolutionPool struct {
	pool *sync.Pool
}

func newSolutionPool() *SolutionPool {
	fn := func() interface{} {
		return &Solution{}
	}

	return &SolutionPool{
		pool: &sync.Pool{
			New: fn,
		},
	}
}

func (p *SolutionPool) Get() *Solution {
	return p.pool.Get().(*Solution)
}

func (p *SolutionPool) Put(buf *Solution) {
	p.pool.Put(buf)
}

type IndexerNodePool struct {
	pool *sync.Pool
}

func newIndexerNodePool() *IndexerNodePool {
	fn := func() interface{} {
		return &IndexerNode{}
	}

	return &IndexerNodePool{
		pool: &sync.Pool{
			New: fn,
		},
	}
}

func (p *IndexerNodePool) Get() *IndexerNode {
	return p.pool.Get().(*IndexerNode)
}

func (p *IndexerNodePool) Put(buf *IndexerNode) {
	p.pool.Put(buf)
}

var solutionPool *SolutionPool
var indexerNodePool *IndexerNodePool

func init() {
	solutionPool = newSolutionPool()
	indexerNodePool = newIndexerNodePool()
}

//////////////////////////////////////////////////////////////
// SAPlanner
//////////////////////////////////////////////////////////////

// Constructor
func newSAPlanner(cost CostMethod, constraint ConstraintMethod, placement PlacementMethod, sizing SizingMethod) *SAPlanner {
	return &SAPlanner{
		cost:       cost,
		constraint: constraint,
		placement:  placement,
		sizing:     sizing,
	}
}

// Given a solution, this function use simulated annealing
// to find an alternative solution with a lower cost.
func (p *SAPlanner) Plan(command CommandType, solution *Solution) (*Solution, error) {

	if p.cpuProfile {
		startCPUProfile("planner.pprof")
		defer stopCPUProfile()
	}

	var result *Solution
	var err error
	var violations *Violations

	solution.command = command
	solution = p.adjustInitialSolutionIfNecessary(solution)

	if p.shardAffinity {
		err := solution.PrePopulateAlternateShardIds(command)
		if err != nil {
			return nil, err
		}

		for _, indexer := range solution.Placement {
			indexer.Indexes, indexer.NumShards, _ = GroupIndexes(
				indexer.Indexes,        // indexes []*IndexUsage
				indexer,                // indexer *IndexerNode
				command == CommandPlan, // skipDeferredIndexGrouping bool
				solution,               // solution *Solution
			)
		}
		solution.place.RegroupIndexes(command == CommandPlan)

		// Re-generate replica map based on the index grouping
		solution.resetReplicaMap()
		solution.generateReplicaMap()
		solution.initializeServerGroupMap()

		p.dropReplicaIfNecessary(solution)
	}

	solution.enforceConstraint = true

	for i := 0; i < RunPerPlan; i++ {
		p.Try++
		startTime := time.Now()
		solution.runSizeEstimation(p.placement, 0)
		solution.evaluateNodes()

		err = p.Validate(solution)
		if err == nil {
			result, err, violations = p.planSingleRun(command, solution)

			if violations == nil {
				return result, err
			}

			err = errors.New(violations.Error())

			// copy estimation information
			if result != nil {
				solution.copyEstimationFrom(result)
			}
		}

		logging.Infof("Planner::Fail to create plan satisfying constraint. Re-planning. Num of Try=%v.  Elapsed Time=%v, err: %v",
			p.Try, formatTimeStr(uint64(time.Now().Sub(startTime).Nanoseconds())), err)

		// reduce minimum memory for each round
		solution.reduceMinimumMemory()

		// If planner get to this point, it means we see violation errors.
		// If planner has retries 3 times, then remove any optional indexes.
		if i > 3 && p.placement.HasOptionalIndexes() {
			logging.Infof("Cannot rebuild lost replica due to resource constraint in cluster.  Will not rebuild lost replica.")
			logging.Warnf(err.Error())

			optionals := p.placement.RemoveOptionalIndexes()
			solution.demoteEligIndexes(optionals)
			solution.removeIndexes(optionals)
		}

		// After 6 tries, disable resource constraint check needed
		if i == 5 && solution.enforceConstraint {
			// can relax constraint if there is deleted node or it is not rebalancing
			solution.enforceConstraint = !(solution.numDeletedNode > 0 || solution.command == CommandPlan || solution.command == CommandRepair)
			if !solution.enforceConstraint {
				logging.Warnf("Unable to find a solution with resource constraint.  Relax resource constraint check.")
			}
		}

		// If cannot find a solution after 9 tries and there are deleted nodes, then disable exclude flag.
		if i == 9 && solution.numDeletedNode != 0 {
			solution.enableExclude = false
		}
	}

	// Ignore error for rebalancing error when there is no deleted node
	if err != nil && (solution.command == CommandRebalance || solution.command == CommandSwap) {
		if solution.numDeletedNode == 0 {
			err = nil
			p.Result = solution
			result = solution
		}
	}

	if err != nil {
		logging.Infof("************ Indexer Layout After Planning *************")
		solution.PrintLayout()
		logging.Infof("****************************************")
		logging.Errorf(err.Error())
	}

	return result, err
}

// Given a solution, this function finds a replica to drop while
// maintaining HA constraint.
func (p *SAPlanner) DropReplica(solution *Solution, defnId common.IndexDefnId, numPartition int, decrement int, dropReplicaId int) (*Solution, []int, error) {

	// setup
	if p.cpuProfile {
		startCPUProfile("planner.pprof")
		defer stopCPUProfile()
	}

	solution.command = CommandDrop
	solution = p.adjustInitialSolutionIfNecessary(solution)

	// set eligible index
	var eligibles []*IndexUsage
	for _, indexer := range solution.Placement {
		for _, index := range indexer.Indexes {
			if index.DefnId == defnId {
				index.eligible = true
				eligibles = append(eligibles, index)
			}
		}
	}
	p.placement.AddRequiredIndexes(eligibles)

	// find all replicaId for the definition
	// healthy replica -- replica with all partitions
	// unhealthy replica -- replica with missing partitions
	allReplicaIds := solution.getReplicas(defnId)
	replicaPartitionMap := make(map[int]map[common.PartitionId]bool)
	for partitionId, replicaIds := range allReplicaIds {
		for replicaId, _ := range replicaIds {
			if _, ok := replicaPartitionMap[replicaId]; !ok {
				replicaPartitionMap[replicaId] = make(map[common.PartitionId]bool)
			}
			replicaPartitionMap[replicaId][partitionId] = true
		}
	}

	// if a specific replicaId is specified, then drop that one.
	if dropReplicaId != -1 {
		if _, ok := replicaPartitionMap[dropReplicaId]; ok {
			return solution, []int{dropReplicaId}, nil
		} else {
			msg1 := fmt.Sprintf("Fail to drop replica.  Replica (%v) does not exist.", dropReplicaId)
			msg2 := "If the replica has been rebalanced out of the cluster, it will be repaired when a new indexer node is rebalanced in."
			msg3 := "Use alter index to lower the replica count to avoid repair during next rebalance."
			return nil, nil, fmt.Errorf("%v %v %v", msg1, msg2, msg3)
		}
	}

	if len(eligibles) != 0 {
		u := eligibles[0]

		if u.Instance != nil {
			// If cluster has already fewer replica than requested, then just return.
			if int(u.Instance.Defn.NumReplica)-decrement+1 >= len(replicaPartitionMap) {
				return solution, []int{}, nil
			}
		}
	}

	var unhealthy []int
	var healthy []int
	for replicaId, partitionIds := range replicaPartitionMap {
		if len(partitionIds) == numPartition {
			healthy = append(healthy, replicaId)
		} else {
			unhealthy = append(unhealthy, replicaId)
		}
	}

	if len(healthy)+len(unhealthy)-decrement <= 0 {
		return nil, nil, fmt.Errorf("Index only has %v replica.  Cannot satisfy request to drop %v copy", len(healthy)+len(unhealthy)-1, decrement)
	}

	reverse := func(arr []int) {
		sort.Ints(arr)
		for i := 0; i < len(arr)/2; i++ {
			tmp := arr[i]
			arr[i] = arr[len(arr)-1-i]
			arr[len(arr)-1-i] = tmp
		}
	}

	// try to drop the unhealthy replica first
	var result []int
	numDrop := 0

	current := solution.clone()
	current.evaluateNodes()

	reverse(unhealthy)
	for _, replicaId := range unhealthy {
		if numDrop == decrement {
			p.Result = current
			return solution, result, nil
		}

		current.removeReplicas(defnId, replicaId)

		numDrop++
		result = append(result, replicaId)
	}

	// Have we dropped enough replica yet?
	if numDrop == decrement {
		p.Result = current
		return solution, result, nil
	}

	// try to drop the healthy replica
	// make sure constraint is enforced
	reverse(healthy)
	for _, replicaId := range healthy {
		if numDrop == decrement {
			p.Result = current
			return solution, result, nil
		}

		current.removeReplicas(defnId, replicaId)

		if !p.constraint.SatisfyClusterConstraint(current, p.placement.GetEligibleIndexes()) {
			logging.Warnf("Dropping replica %v for index %v.   Cluster may not follow server group constraint after drop.",
				replicaId, defnId)
		}

		numDrop++
		result = append(result, replicaId)
	}

	if numDrop == decrement {
		p.Result = current
		return solution, result, nil
	}

	return nil, nil, fmt.Errorf("Unable to drop %v replica without violating availability constraint", decrement)
}

// Given a solution, this function use simulated annealing
// to find an alternative solution with a lower cost.
func (p *SAPlanner) planSingleRun(command CommandType, solution *Solution) (*Solution, error, *Violations) {

	current := solution.clone()
	initialPlan := solution.initialPlan

	logging.Tracef("Planner: memQuota %v (%v) cpuQuota %v",
		p.constraint.GetMemQuota(), formatMemoryStr(p.constraint.GetMemQuota()), p.constraint.GetCpuQuota())

	rs := rand.New(rand.NewSource(time.Now().UnixNano()))

	old_cost := p.cost.Cost(current)
	current.updateCost()
	startScore := old_cost
	startTime := time.Now()
	lastUpdateTime := time.Now()
	move := uint64(0)
	iteration := uint64(0)
	positiveMove := uint64(0)
	skipPlanner := false

	currentResourceVariation := current.cost.ComputeResourceVariation()
	logging.Infof("Planner::planSingleRun Initial variance of the solution: %v", currentResourceVariation)

	eligibles := p.placement.GetEligibleIndexes()
	violations := p.constraint.GetViolations(current, eligibles)
	if currentResourceVariation <= p.threshold && command == CommandRebalance &&
		current.hasNewNodes() == false && current.hasDeletedNodes() == false && violations == nil {
		logging.Infof("Planner::planSingleRun Skip running planner as current solution resource variation: %v is less than threshold: %v. "+
			"No nodes have been added or deleted and there are no violations observed", currentResourceVariation, p.threshold)
		skipPlanner = true
	}

	temperature := p.initialTemperatureFromResourceVariation(command, current.cost.ComputeResourceVariation())
	startTemp := temperature
	done := false

	eligibles = p.placement.GetEligibleIndexes()
	if !p.constraint.SatisfyClusterConstraint(current, eligibles) {
		temperature = MaxTemperature
		startTemp = temperature
	}

	for temperature > MinTemperature && !done && !skipPlanner {
		lastMove := move
		lastPositiveMove := positiveMove

		i := 0
		for {
			if !p.runIteration(i, current) {
				break
			}

			i++

			new_solution, force, final, err := p.findNeighbor(current)
			if err != nil {
				return nil, err, nil
			}
			if new_solution != nil {
				new_cost := p.cost.Cost(new_solution)
				prob := p.getAcceptProbability(old_cost, new_cost, temperature)

				logging.Tracef("Planner::old_cost-new_cost %v new_cost % v temp %v prob %v force %v",
					old_cost-new_cost, new_cost, temperature, prob, force)

				if old_cost-new_cost > 0 {
					positiveMove++
				}

				// if force=true, then just accept the new solution.  Do
				// not need to change the temperature since new solution
				// could have higher score.
				if force || prob > rs.Float64() {
					current.free()
					current = new_solution
					current.updateCost()
					old_cost = new_cost
					lastUpdateTime = time.Now()
					move++

					logging.Tracef("Planner::accept solution: new_cost %v temp %v", new_cost, temperature)

				} else {
					new_solution.free()
				}

				iteration++
			}

			if final {
				logging.Infof("Planner::finalizing the solution as final solution is found.")
				done = true
				break
			}
		}

		if int64(move-lastMove) < MinNumMove && int64(positiveMove-lastPositiveMove) < MinNumPositiveMove {
			logging.Infof("Planner::finalizing the solution as there are no more valid index movements.")
			done = true
		}

		currCost := current.cost.ComputeResourceVariation()
		if p.threshold > 0 && currCost <= p.threshold {
			logging.Infof("Planner::finalizing the solution as the current resource variation is under control (%v).", currCost)
			done = true
		}

		temperature = temperature * Alpha

		if command == CommandPlan && initialPlan {
			// adjust temperature based on score for faster convergence
			temperature = temperature * old_cost
		}

		if p.timeout > 0 && p.runtime != nil {
			elapsed := time.Now().Sub(*p.runtime).Seconds()
			if elapsed >= float64(p.timeout) {
				logging.Infof("Planner::stop planner due to timeout.  Elapsed %vs", elapsed)
				break
			}
		}
	}

	p.ElapseTime = uint64(time.Now().Sub(startTime).Nanoseconds())
	p.ConvergenceTime = uint64(lastUpdateTime.Sub(startTime).Nanoseconds())
	p.Result = current
	p.Score = old_cost
	p.StartTemp = startTemp
	p.StartScore = startScore
	p.Move = move
	p.PositiveMove = positiveMove
	p.Iteration = iteration

	eligibles = p.placement.GetEligibleIndexes()
	if !p.constraint.SatisfyClusterConstraint(p.Result, eligibles) {
		return current, nil, p.constraint.GetViolations(p.Result, eligibles)
	}

	p.cost.Cost(p.Result)
	return current, nil, nil
}

func (p *SAPlanner) runIteration(i int, s *Solution) bool {
	if i < p.minIterPerTemp {
		return true
	}

	if i >= p.maxIterPerTemp {
		logging.Infof("Planner::stop planner iter per temp as maxIterPerTemp limit (%v, %v) exceeded.", i, p.maxIterPerTemp)
		return false
	}

	if s.command != CommandRebalance && s.command != CommandSwap {
		// TODO: Can this happen for an index creation with numReplica*numPartition > minIterPerTemp ?
		return false
	}

	// Check if there are any non-empty deleted nodes
	if s.numDeletedNode != 0 {
		for _, node := range s.Placement {
			if !node.isDelete {
				continue
			}

			if len(node.Indexes) > 0 {
				if i == p.minIterPerTemp {
					logging.Infof("Planner::Running more iterations than %v because of deleted nodes.", p.minIterPerTemp)
				}

				return true
			}
		}
	}

	//
	// Note that a newly added node may not get more than minIterPerTemp number
	// of indexes, if the movement to new node is allowed and variance is
	// under threshold after minIterPerTemp number of movements.
	//

	// Check if any lost indexes need to be rebuilt, if yes allow more iterations
	if s.place.HasOptionalIndexes() {

		// Check if the current placement of optional indexes needs any more
		// indexes to be moved.

		haConstrained := false
		eligibles := p.placement.GetEligibleIndexes()
		for _, node := range s.Placement {
			if !p.constraint.SatisfyNodeHAConstraint(s, node, eligibles, true) {
				haConstrained = true
			}
		}

		if !haConstrained {
			// This means that the lost indexes will be rebuilt. No need for any
			// more iterations.
			return false
		}

		// Planner decides not to rebuild lost indexes before it decides not
		// to enforce resource constraints. This means that if the cluster is
		// resource constrained, there is no need to overburden the cluster
		// further, by adding new indexes.
		if !p.constraint.SatisfyClusterResourceConstraint(s) {
			logging.Infof("Planner::Even though there are optional indexes, the cluster" +
				"resource constraints are not satisfied. Not running any extra iterations.")
			return false
		}

		if i == p.minIterPerTemp {
			logging.Infof("Planner::Running more iterations than %v because of HA constraints.", p.minIterPerTemp)
		}

		return true
	}

	if i != p.minIterPerTemp {
		logging.Infof("Planner::Finished planner run after %v iterations.", i)
	}

	return false
}

func (p *SAPlanner) SetTimeout(timeout int) {
	p.timeout = timeout
}

func (p *SAPlanner) SetRuntime(runtime *time.Time) {
	p.runtime = runtime
}

func (p *SAPlanner) SetVariationThreshold(threshold float64) {
	p.threshold = threshold
}

func (p *SAPlanner) SetCpuProfile(cpuProfile bool) {
	p.cpuProfile = cpuProfile
}

func (p *SAPlanner) SetMinIterPerTemp(minIterPerTemp int) {
	p.minIterPerTemp = minIterPerTemp
}

func (p *SAPlanner) SetMaxIterPerTemp(maxIterPerTemp int) {
	p.maxIterPerTemp = maxIterPerTemp
}

func (p *SAPlanner) SetShardAffinity(shardAffinity bool) {
	p.shardAffinity = shardAffinity
}

// Validate the solution
func (p *SAPlanner) Validate(s *Solution) error {

	if err := p.sizing.Validate(s); err != nil {
		logging.Errorf("SAPlanner::Validated Error observed when validating sizing, err: %v", err)
		return err
	}

	if err := p.cost.Validate(s); err != nil {
		logging.Errorf("SAPlanner::Validated Error observed when validating cost, err: %v", err)
		return err
	}

	if err := p.constraint.Validate(s); err != nil {
		logging.Errorf("SAPlanner::Validated Error observed when constraint sizing, err: %v", err)
		return err
	}

	if err := p.placement.Validate(s); err != nil {
		logging.Errorf("SAPlanner::Validated Error observed when placement sizing, err: %v", err)
		return err
	}

	return nil
}

// This function prints the result of evaluation
func (p *SAPlanner) PrintRunSummary() {

	logging.Infof("Score: %v", p.Score)
	logging.Infof("variation: %v", p.cost.ComputeResourceVariation())
	logging.Infof("ElapsedTime: %v", formatTimeStr(p.ElapseTime))
	logging.Infof("ConvergenceTime: %v", formatTimeStr(p.ConvergenceTime))
	logging.Infof("Iteration: %v", p.Iteration)
	logging.Infof("Move: %v", p.Move)
}

// This function prints the result of evaluation
func (p *SAPlanner) Print() {

	p.PrintRunSummary()
	logging.Infof("----------------------------------------")

	if p.Result != nil {
		logging.Infof("Using SAPlanner")
		p.cost.Print()
		logging.Infof("----------------------------------------")
		p.Result.PrintStats()
		logging.Infof("----------------------------------------")
		p.constraint.Print()
		logging.Infof("----------------------------------------")
		p.placement.Print()
		logging.Infof("----------------------------------------")
		p.Result.PrintLayout()
	}
}

// This function prints the result of evaluation
func (p *SAPlanner) PrintLayout() {

	if p.Result != nil {
		logging.Infof("----------------------------------------")
		logging.Infof("Memory Quota: %v (%v)", p.constraint.GetMemQuota(),
			formatMemoryStr(p.constraint.GetMemQuota()))
		logging.Infof("CPU Quota: %v", p.constraint.GetCpuQuota())
		logging.Infof("----------------------------------------")
		p.cost.Print()
		logging.Infof("----------------------------------------")
		p.Result.PrintLayout()
	} else {
		logging.Infof("No result is available")
	}
}

// This function prints the result of evaluation
func (p *SAPlanner) PrintCost() {

	if p.Result != nil {
		logging.Infof("Score: %v", p.Score)
		logging.Infof("Memory Quota: %v (%v)", p.constraint.GetMemQuota(),
			formatMemoryStr(p.constraint.GetMemQuota()))
		logging.Infof("CPU Quota: %v", p.constraint.GetCpuQuota())
		p.cost.Print()
	} else {
		logging.Infof("No result is available")
	}
}

// This function finds a neighbor placement layout using
// given placement method.
func (p *SAPlanner) findNeighbor(s *Solution) (*Solution, bool, bool, error) {

	currentOK := s.SatisfyClusterConstraint()
	neighbor := s.clone()

	force := false
	done := false
	retry := 0

	for retry = 0; retry < ResizePerIteration; retry++ {
		success, final, mustAccept, err := p.placement.Move(neighbor)
		if err != nil {
			logging.Warnf("Planner::findNeighbor failed in placement.Move, retry:%v", retry)
			return nil, false, done, err
		}
		if success {
			neighborOK := neighbor.SatisfyClusterConstraint()
			logging.Tracef("Planner::findNeighbor retry: %v", retry)
			return neighbor, (mustAccept || force || (!currentOK && neighborOK)), final, nil
		}

		// Add new node to change cluster in order to ensure constraint can be satisfied
		if !neighbor.SatisfyClusterConstraint() {
			if neighbor.canRunEstimation() {
				neighbor.runSizeEstimation(p.placement, 0)
			} else if p.constraint.CanAddNode(s) {
				nodeId := strconv.FormatUint(uint64(rand.Uint32()), 10)
				neighbor.addNewNode(nodeId)
				logging.Tracef("Planner::add node: %v", nodeId)
				force = true
			} else {
				done = final
				break
			}
		} else {
			done = final
			break
		}
	}

	logging.Tracef("Planner::findNeighbor retry: %v", retry)
	s.copyEstimationFrom(neighbor)
	return nil, false, done, nil
}

// Get the initial temperature.
func (p *SAPlanner) initialTemperature(command CommandType, cost float64) float64 {

	if command == CommandPlan {
		return MaxTemperature
	}

	temp := MaxTemperature
	if cost > 0 && cost < 0.3 {
		temp = cost * MaxTemperature * 0.1
	}

	logging.Infof("Planner::initial temperature: initial cost %v temp %v", cost, temp)
	return temp
}

// Get the initial temperature.
func (p *SAPlanner) initialTemperatureFromResourceVariation(command CommandType, resourceVariation float64) float64 {

	if command == CommandPlan {
		return MaxTemperature
	}

	temp := math.Max(MinTemperature, math.Abs(resourceVariation-p.threshold)*0.1)

	logging.Infof("Planner::initial temperature: initial resource variation %v temp %v", resourceVariation, temp)
	return temp
}

// This function calculates the acceptance probability of this solution based on cost.
func (p *SAPlanner) getAcceptProbability(old_cost float64, new_cost float64, temperature float64) float64 {
	// always accept if new_cost is lower than old cost
	if new_cost < old_cost {
		return 1.0
	}

	// new_cost is higher or equal to old_cost.  But still consider this solution based on probability.
	// Low probability when
	// 1) low temperature (many iterations have passed)
	// 2) difference between new_cost and old_cost are high
	cost := (old_cost - new_cost)
	return math.Exp(cost / temperature)
}

// Adjust solution constraint depending on the solution and command.
func (p *SAPlanner) adjustInitialSolutionIfNecessary(s *Solution) *Solution {

	s.constraint = p.constraint
	s.sizing = p.sizing
	s.cost = p.cost
	s.place = p.placement
	s.numServerGroup = s.findNumServerGroup()

	// update the number of new nodes and deleted node
	s.numDeletedNode = s.findNumDeleteNodes()
	s.numNewNode = s.findNumEmptyNodes()
	s.markNewNodes()
	s.initializeServerGroupMap()
	s.markSwapOnlyRebalance()

	// if there is deleted node and all nodes are excluded to take in new indexes,
	// then do not enable node exclusion
	if s.numDeletedNode != 0 && s.findNumExcludeInNodes() == len(s.Placement) {
		s.enableExclude = false
	}

	// If not using live data, then no need to relax constraint.
	if !s.UseLiveData() {
		return s
	}

	if p.constraint.CanAddNode(s) {
		return s
	}

	cloned := s.clone()
	cloned.updateSlotMap()

	// Make sure we only repair when it is rebalancing
	if s.command == CommandRebalance || s.command == CommandSwap || s.command == CommandRepair {
		p.addReplicaIfNecessary(cloned)

		if s.command == CommandRebalance || s.command == CommandSwap {
			p.addPartitionIfNecessary(cloned)
			// When shard affinity is enabled, then do not drop the replica yet.
			// Replica will be dropped after grouping the indexes. Otherwise,
			// a proxy may initially contain 'n' indexes and due to the drop here,
			// planner may consider only 'm' indexes for tranfser (n < m). This can
			// lead to partial transfer of idnexes on a shard which is incorrect.
			if p.shardAffinity == false {
				p.dropReplicaIfNecessary(cloned)
			}
		}
	}
	cloned.generateReplicaMap()
	p.suppressEqivIndexIfNecessary(cloned)

	cloned.evaluateNodes() // must be after all the adjustments and before Validate
	if s.command != CommandPlan {
		// Validate only for rebalancing
		err := p.Validate(cloned)
		if err != nil {
			logging.Warnf("Validation error after adjusting solution for planner.   Restore to original plan.  Error=%v", err)
			return s
		}
	}

	return cloned
}

// Drop replica from ejected node if there is not enough nodes in the cluster.
func (p *SAPlanner) dropReplicaIfNecessary(s *Solution) {

	eligibles := p.placement.GetEligibleIndexes()
	numLiveNode := s.findNumLiveNode()

	// Check to see if it is needed to drop replica from a ejected node
	deleteCandidates := make(map[string][]*IndexUsage)
	numReplicas := make(map[string]int)

	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {
			if isEligibleIndex(index, eligibles) {

				// if there are more replica than the number of nodes, then
				// do not move this index if this node is going away.
				numReplica := s.findNumReplica(index)
				if (numReplica > numLiveNode) && (indexer.isDelete || index.initialNode == nil) {
					deleteCandidates[index.GetKeyspaceIndexPartitionName()] = append(deleteCandidates[index.GetKeyspaceIndexPartitionName()], index)
					numReplicas[index.GetKeyspaceIndexPartitionName()] = numReplica
				}
			}
		}
	}

	for key, candidates := range deleteCandidates {

		// sort the candidates in descending order
		for i := 0; i < len(candidates)-1; i++ {
			for j := i + 1; j < len(candidates); j++ {
				if candidates[i].Instance != nil && candidates[j].Instance != nil &&
					candidates[i].Instance.ReplicaId < candidates[j].Instance.ReplicaId {
					tmp := candidates[i]
					candidates[i] = candidates[j]
					candidates[j] = tmp
				}
			}
		}

		//prune the candidate list
		numToDelete := numReplicas[key] - numLiveNode
		if len(candidates) > numToDelete {
			deleteCandidates[key] = candidates[:numToDelete]
		}
	}

	for _, indexer := range s.Placement {
		keepCandidates := ([]*IndexUsage)(nil)

		for _, index := range indexer.Indexes {
			found := false
			for _, candidate := range deleteCandidates[index.GetKeyspaceIndexPartitionName()] {
				if candidate == index {
					found = true
					break
				}
			}

			if !found {
				keepCandidates = append(keepCandidates, index)
			} else {
				if index.Instance != nil {
					logging.Warnf("There is more replica than available nodes.  Will not move index replica (%v,%v,%v,%v,%v,%v) from ejected node %v",
						index.Bucket, index.Scope, index.Collection, index.Name, index.Instance.ReplicaId, index.PartnId, indexer.NodeId)
				} else {
					logging.Warnf("There is more replica than available nodes.  Will not move index replica (%v,%v,%v,%v,<nil>,%v) from ejected node %v",
						index.Bucket, index.Scope, index.Collection, index.Name, index.PartnId, indexer.NodeId)
				}

				// Check if proxy needs to be broken to prevent individual index definition loss
				if keepIdx := p.breakOutOfProxyIfNecessary(s, indexer, index); keepIdx != nil {
					keepCandidates = append(keepCandidates, keepIdx...)
				}
				c := []*IndexUsage{index}
				p.placement.RemoveEligibleIndex(c)
			}
		}

		indexer.Indexes = keepCandidates
	}
}

// Suppress equivalent index check if there are not enough nodes in the cluster to host all
// equivalent index.
func (p *SAPlanner) suppressEqivIndexIfNecessary(s *Solution) {

	eligibles := p.placement.GetEligibleIndexes()
	numLiveNode := s.findNumLiveNode()

	// Check to see if need to suppress equivalent index.
	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {

			if isEligibleIndex(index, eligibles) {

				// if there are more equiv idx than number of nodes, then
				// allow placement of this index over equiv index.  Even
				// though replica is considered as "equivalent" as well,
				// this does not affect replica (replica will not place over
				// one another).
				count := s.findNumEquivalentIndex(index)
				if count > numLiveNode {
					logging.Warnf("There are more equivalent index than available nodes.  Allow equivalent index of (%v, %v, %v, %v) to be replaced on same node.",
						index.Bucket, index.Scope, index.Collection, index.Name)
					index.suppressEquivIdxCheck = true

					if index.Instance != nil {
						logging.Warnf("Definition %v Instance %v ReplicaId %v partitionId %v count %v numLiveNode %v.",
							index.DefnId, index.InstId, index.Instance.ReplicaId, index.PartnId, count, numLiveNode)
					}
				} else {
					index.suppressEquivIdxCheck = false

					// An Edge case, where for an index on Deleted Node there
					// exist atleast one Server Group which does not contain
					// replica of the index, but every node on this SG
					// has an equivalent index, causing rebalance failure.
					// This can happen even if numReplica <= numServerGroup.
					// An Equivalent Index should not prevent HA across SGs for
					// a replica. The replicas are to be treated as first class
					// citizen
					// A preliminary check is made to confirm if the index can
					// be placed on that SG. If the check fails, there is no
					// other place this index can be moved without suppressing
					// the check

					// Currently only 1 node removal is supported i.e. only 1
					// replica instance causing the issue
					if s.numDeletedNode != 1 || s.numServerGroup <= 1 {
						continue
					}

					if !indexer.IsDeleted() {
						continue
					}

					// If index is already present on all the SGs, incorrect
					// ServerGroup Violation won't be triggered, dont suppress
					SGsWithReplica := s.getServerGroupsWithReplica(index)
					if len(SGsWithReplica) == s.numServerGroup {
						continue
					}

					// No node in the SG-without-replica on which the index can
					// be placed without EquivIdx Violation. Suppress the check
					SGsWithNodesWithoutEquiv := s.sgsHasNodeWithoutEquiv(index, SGsWithReplica)
					if len(SGsWithNodesWithoutEquiv) == 0 {
						index.suppressEquivIdxCheck = true
					}

				}
			}
		}
	}
}

// Add replica if there is enough nodes in the cluster.
func (p *SAPlanner) addReplicaIfNecessary(s *Solution) {

	if s.disableRepair {
		return
	}

	// Check to see if it is needed to add replica
	for _, indexer := range s.Placement {
		addCandidates := make(map[*IndexUsage][]*IndexerNode)

		for _, index := range indexer.Indexes {
			// If the number of replica in cluster is smaller than the desired number
			// of replica (from index definition), and there is enough nodes in the
			// cluster to host all the replica.  Also do not repair if the index
			// could be deleted by user.
			numReplica := s.findNumReplica(index)

			missingReplica := 0
			if index.Instance != nil {
				missingReplica = int(index.Instance.Defn.NumReplica+1) - numReplica
			}

			if index.Instance != nil && missingReplica > 0 && !index.PendingDelete {
				targets := s.FindNodesForReplicaRepair(index, missingReplica, true)
				if len(targets) != 0 {
					addCandidates[index] = targets
				}
			}
		}

		if len(addCandidates) != 0 {
			clonedCandidates := ([]*IndexUsage)(nil)

			for index, indexers := range addCandidates {
				numReplica := s.findNumReplica(index)
				missing := s.findMissingReplica(index)

				for replicaId := 0; replicaId < int(index.Instance.Defn.NumReplica+1); replicaId++ {
					if instId, ok := missing[replicaId]; ok && len(indexers) != 0 {

						var indexSlot uint64
						indexSlot, indexer, indexers = s.findIndexerForReplica(index.DefnId, replicaId, index.PartnId, indexers, p.shardAffinity)

						if index.Instance != nil {

							// clone the original and update the replicaId
							cloned := index.clone()
							cloned.Instance.ReplicaId = replicaId
							cloned.initialNode = nil
							cloned.ShardIds = nil //reset ShardIds

							if indexSlot != 0 {
								msAltId := &common.AlternateShardId{SlotId: indexSlot, ReplicaId: uint8(replicaId), GroupId: 0}
								if !cloned.IsPrimary {
									bsAltId := &common.AlternateShardId{SlotId: indexSlot, ReplicaId: uint8(replicaId), GroupId: 1}
									cloned.AlternateShardIds = []string{msAltId.String(), bsAltId.String()}
								} else {
									cloned.AlternateShardIds = []string{msAltId.String()}
								}
							}

							// generate a new instance id for the new replica
							if instId == 0 {
								var err error
								instId, err = common.NewIndexInstId()
								if err != nil {
									continue
								}
							}
							cloned.InstId = instId
							cloned.Instance.InstId = instId
							cloned.overrideExclude = true

							// add the new replica to the solution
							s.addIndex(indexer, cloned, false)

							if indexSlot != 0 {
								// update slotMap and index slots for indexer
								if s.shardDealer != nil {
									s.shardDealer.RecordIndexUsage(cloned, indexer, false)
								}
								s.addToIndexSlots(cloned.DefnId, replicaId, cloned.PartnId, indexSlot)
								s.addToSlotMap(indexSlot, indexer, replicaId)
							}

							clonedCandidates = append(clonedCandidates, cloned)
							numReplica++

							if len(cloned.AlternateShardIds) > 0 {
								logging.Infof("Rebuilding lost replica for (%v,%v,%v,%v,%v,%v), inst: %v with alternate shardIds: %v "+
									"with initial placement on node: %v",
									index.Bucket, index.Scope, index.Collection, index.Name, replicaId, index.PartnId,
									index.InstId, cloned.AlternateShardIds, indexer.NodeId)
							} else {
								logging.Infof("Rebuilding lost replica for (%v,%v,%v,%v,%v,%v), inst: %v with initial placement on node: %v",
									index.Bucket, index.Scope, index.Collection, index.Name, replicaId, index.PartnId,
									index.InstId, indexer.NodeId)
							}
						}
					}
				}
			}

			if len(clonedCandidates) != 0 {
				if s.command == CommandRepair {
					p.placement.AddRequiredIndexes(clonedCandidates)
				} else {
					p.placement.AddOptionalIndexes(clonedCandidates)
				}
			}
		}
	}
}

// Add missing partition if there is enough nodes in the cluster.
func (p *SAPlanner) addPartitionIfNecessary(s *Solution) {

	if s.disableRepair {
		return
	}

	candidates := make(map[common.IndexInstId]*IndexUsage)
	done := make(map[common.IndexInstId]bool)
	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {
			if !index.PendingDelete {
				if index.Instance != nil && index.Instance.Pc != nil {
					if _, ok := done[index.InstId]; !ok {
						if s.findNumPartition(index) < int(index.Instance.Pc.GetNumPartitions()) {
							candidates[index.InstId] = index
						}
						done[index.InstId] = true
					}
				}
			}
		}
	}

	if len(candidates) != 0 {
		var available []*IndexerNode
		var allCloned []*IndexUsage

		for _, indexer := range s.Placement {
			if !indexer.isDelete {
				available = append(available, indexer)
			}
		}

		if len(available) == 0 {
			logging.Warnf("Planner: Cannot repair lost partitions because all indexer nodes are excluded or deleted for rebalancing")
			return
		}

		newPartns := make([]*IndexUsage, 0, len(candidates))

		for _, candidate := range candidates {
			missing := s.findMissingPartition(candidate)
			for _, partitionId := range missing {
				// clone the original and update the partitionId
				// Does not need to modify Instance.Pc
				cloned := candidate.clone()
				cloned.PartnId = partitionId
				cloned.initialNode = nil
				cloned.overrideExclude = true
				cloned.AlternateShardIds = nil
				cloned.ShardIds = nil

				// repair only if there is no replica, otherwise, replica repair would have handle this.
				if s.findNumReplica(cloned) == 0 {
					newPartns = append(newPartns, cloned)
				}
			}
		}

		for _, cloned := range newPartns {
			// Partition repair always places one replica at a time.
			indexers := s.FindNodesForReplicaRepair(cloned, 1, true)
			if len(indexers) < 1 {
				logging.Warnf("Planner: Cannot repair lost partition (%v,%v,%v,%v,%v,%v)"+
					" because of unavailaility of appropriate indexer nodes. The nodes "+
					"can be excluded or deleted for rebalancing", cloned.Bucket, cloned.Scope,
					cloned.Collection, cloned.Name, cloned.Instance.ReplicaId, cloned.PartnId)

				continue
			}

			indexer := indexers[0]

			// add the new partition to the solution
			s.addIndex(indexer, cloned, false)
			allCloned = append(allCloned, cloned)

			logging.Infof("Rebuilding lost partition for (%v,%v,%v,%v,%v,%v)",
				cloned.Bucket, cloned.Scope, cloned.Collection, cloned.Name,
				cloned.Instance.ReplicaId, cloned.PartnId)
		}

		if len(allCloned) != 0 {
			p.placement.AddOptionalIndexes(allCloned)
		}
	}
}

func (p *SAPlanner) GetResult() *Solution {
	return p.Result
}

func (p *SAPlanner) SetParam(param map[string]interface{}) error {
	minIter, ok := param["MinIterPerTemp"]
	if !ok {
		return fmt.Errorf("Planner Error: Missing expected param MinIterPerTemp")
	}

	maxIter, ok := param["MaxIterPerTemp"]
	if !ok {
		return fmt.Errorf("Planner Error: Missing expected param MaxIterPerTemp")
	}

	minIterPerTemp, ok := minIter.(int)
	if !ok {
		return fmt.Errorf("Planner Error: Uexpected type of param MinIterPerTemp")
	}

	maxIterPerTemp, ok := maxIter.(int)
	if !ok {
		return fmt.Errorf("Planner Error: Uexpected type of param MaxIterPerTemp")
	}

	p.SetMinIterPerTemp(minIterPerTemp)
	p.SetMaxIterPerTemp(maxIterPerTemp)

	return nil
}

//////////////////////////////////////////////////////////////
// GeneralSizingMethod
//////////////////////////////////////////////////////////////

// Constructor
func newGeneralSizingMethod() *GeneralSizingMethod {
	return &GeneralSizingMethod{
		MOI:    newMOISizingMethod(),
		Plasma: newPlasmaSizingMethod(),
	}
}

func GetNewGeneralSizingMethod() *GeneralSizingMethod {
	return newGeneralSizingMethod()
}

// Validate
func (s *GeneralSizingMethod) Validate(solution *Solution) error {

	return nil
}

// This function computes the index size
func (s *GeneralSizingMethod) ComputeIndexSize(idx *IndexUsage) {

	if idx.IsPlasma() {
		s.Plasma.ComputeIndexSize(idx)
	} else {
		// for both MOI and forestdb
		// we don't have sizing for forestdb but we have simulation tests that run with forestdb
		s.MOI.ComputeIndexSize(idx)
	}
}

// This function computes the indexer memory and cpu usage
func (s *GeneralSizingMethod) ComputeIndexerSize(o *IndexerNode) {

	o.MemUsage = 0
	o.CpuUsage = 0
	o.DataSize = 0

	for _, idx := range o.Indexes {
		o.MemUsage += idx.MemUsage
		o.CpuUsage += idx.CpuUsage
		o.DataSize += idx.DataSize
	}

	s.ComputeIndexerOverhead(o)
}

// This function computes the indexer memory overhead
func (s *GeneralSizingMethod) ComputeIndexerOverhead(o *IndexerNode) {

	// channel overhead : 100MB
	overhead := uint64(100 * 1024 * 1024)

	for _, idx := range o.Indexes {
		overhead += s.ComputeIndexOverhead(idx)
	}

	o.MemOverhead = uint64(overhead)
}

// This function estimates the index memory overhead
func (s *GeneralSizingMethod) ComputeIndexOverhead(idx *IndexUsage) uint64 {

	if idx.IsMOI() {
		return s.MOI.ComputeIndexOverhead(idx)
	} else if idx.IsPlasma() {
		return s.Plasma.ComputeIndexOverhead(idx)
	}

	return 0
}

// This function estimates the min memory quota given a set of indexes
func (s *GeneralSizingMethod) ComputeMinQuota(indexes []*IndexUsage, useLive bool) (uint64, uint64) {

	maxCpuUsage := float64(0)
	maxMemUsage := uint64(0)

	for _, index := range indexes {
		if index.GetMemTotal(useLive) > maxMemUsage {
			maxMemUsage = index.GetMemTotal(useLive)
		}

		if index.GetCpuUsage(useLive) > maxCpuUsage {
			maxCpuUsage = index.GetCpuUsage(useLive)
		}
	}

	// channel overhead : 100MB
	overhead := float64(100 * 1024 * 1024)

	// 20% buffer for mem quota
	// TODO
	//memQuota := uint64((float64(maxMemUsage) + overhead) * 1.2)
	memQuota := maxMemUsage + uint64(overhead)

	// 20% buffer for cpu quota
	// TODO
	//cpuQuota := uint64(float64(maxCpuUsage) * 1.2)
	cpuQuota := uint64(math.Floor(maxCpuUsage)) + 1

	return memQuota, cpuQuota
}

//////////////////////////////////////////////////////////////
// MOISizingMethod
//////////////////////////////////////////////////////////////

// Constructor
func newMOISizingMethod() *MOISizingMethod {
	return &MOISizingMethod{}
}

// This function computes the index size
func (s *MOISizingMethod) ComputeIndexSize(idx *IndexUsage) {

	if idx.AvgSecKeySize == 0 && idx.AvgArrKeySize == 0 && idx.AvgDocKeySize == 0 && idx.ActualKeySize == 0 {
		idx.MemOverhead = s.ComputeIndexOverhead(idx)
		return
	}

	// compute memory usage
	if !idx.IsPrimary {
		if idx.AvgSecKeySize != 0 {
			// secondary index mem size : (120 + SizePerItem[KeyLen + DocIdLen]) * NumberOfItems
			idx.DataSize = (120 + idx.AvgSecKeySize + idx.AvgDocKeySize) * idx.NumOfDocs
		} else if idx.AvgArrKeySize != 0 {
			// secondary array index mem size : (46 + (74 + DocIdLen + ArrElemSize) * NumArrElems) * NumberOfItems
			idx.DataSize = (46 + (74+idx.AvgArrKeySize+idx.AvgDocKeySize)*idx.AvgArrSize) * idx.NumOfDocs
		} else if idx.ActualKeySize != 0 {
			// secondary index mem size : (46 + ActualKeySize) * NumberOfItems
			idx.DataSize = (46 + idx.ActualKeySize) * idx.ActualNumDocs
		}
	} else {
		if idx.AvgDocKeySize != 0 {
			// primary index mem size : (74 + DocIdLen) * NumberOfItems
			idx.DataSize = (74 + idx.AvgDocKeySize) * idx.NumOfDocs
		} else if idx.ActualKeySize != 0 {
			// primary index mem size : ActualKeySize * NumberOfItems
			idx.DataSize = idx.ActualKeySize * idx.ActualNumDocs
		}
	}
	idx.MemUsage = idx.DataSize

	// compute cpu usage
	idx.CpuUsage = float64(idx.MutationRate)/float64(MOIMutationRatePerCore) + float64(idx.ScanRate)/float64(MOIScanRatePerCore)
	//idx.CpuUsage = math.Floor(idx.CpuUsage) + 1

	idx.MemOverhead = s.ComputeIndexOverhead(idx)
}

// This function estimates the index memory overhead
func (s *MOISizingMethod) ComputeIndexOverhead(idx *IndexUsage) uint64 {

	// protobuf overhead : 150MB per index
	overhead := float64(150 * 1024 * 1024)

	snapshotOverhead := float64(0)

	// incoming mutation buffer overhead: 30K * SizePerItem * NumberOfIndexes * MutationRate/500
	if idx.AvgSecKeySize != 0 {
		overhead += float64(30*1000*(idx.AvgSecKeySize+idx.AvgDocKeySize)) * float64(idx.MutationRate) / float64(500)
		snapshotOverhead += float64(idx.MutationRate * MOIScanTimeout * (idx.AvgSecKeySize + idx.AvgDocKeySize + 120))
	} else if idx.AvgArrKeySize != 0 {
		overhead += float64(30*1000*(idx.AvgArrKeySize*idx.AvgArrSize+idx.AvgDocKeySize)) * float64(idx.MutationRate) / float64(500)
		snapshotOverhead += float64(idx.MutationRate * MOIScanTimeout * (idx.AvgArrKeySize + idx.AvgDocKeySize + 74) * idx.AvgArrSize)
	} else if idx.AvgDocKeySize != 0 {
		overhead += float64(30*1000*idx.AvgDocKeySize) * float64(idx.MutationRate) / float64(500)
		snapshotOverhead += float64(idx.MutationRate * MOIScanTimeout * (idx.AvgDocKeySize + 120))
	} else if idx.ActualKeySize != 0 {
		overhead += float64(30*1000*(idx.ActualKeySize)) * float64(idx.MutationRate) / float64(500)
		snapshotOverhead += float64(idx.MutationRate * MOIScanTimeout * idx.ActualKeySize)
	}

	// snapshot overhead
	overhead += snapshotOverhead

	// mutation queue size : 10% of indexer memory usage
	mutationQueueOverhead := (float64(idx.MemUsage) + snapshotOverhead) * 0.1
	overhead += mutationQueueOverhead

	// golang overhead: 5% of total memory
	golangOverhead := (float64(idx.MemUsage) + snapshotOverhead + mutationQueueOverhead) * 0.05
	overhead += golangOverhead

	return uint64(overhead)
}

//////////////////////////////////////////////////////////////
// PlasmaSizingMethod
//////////////////////////////////////////////////////////////

// Constructor
func newPlasmaSizingMethod() *PlasmaSizingMethod {
	return &PlasmaSizingMethod{}
}

// This function computes the index size
func (s *PlasmaSizingMethod) ComputeIndexSize(idx *IndexUsage) {

	if idx.AvgSecKeySize == 0 && idx.AvgArrKeySize == 0 && idx.AvgDocKeySize == 0 && idx.ActualKeySize == 0 {
		idx.MemOverhead = s.ComputeIndexOverhead(idx)
		return
	}

	// compute memory usage
	if !idx.IsPrimary {
		if idx.AvgSecKeySize != 0 {
			// secondary index mem size : (114 + SizePerItem[KeyLen + DocIdLen]) * NumberOfItems * 2 (for back index)
			idx.DataSize = (114 + idx.AvgSecKeySize + idx.AvgDocKeySize) * idx.NumOfDocs * 2
		} else if idx.AvgArrKeySize != 0 {
			// secondary array index mem size : (46 + (74 + DocIdLen + ArrElemSize) * NumArrElems) * NumberOfItems * 2 (for back index)
			idx.DataSize = (46 + (74+idx.AvgArrKeySize+idx.AvgDocKeySize)*idx.AvgArrSize) * idx.NumOfDocs * 2
		} else if idx.ActualKeySize != 0 {
			// secondary index mem size : (46 + ActualKeySize) * NumberOfItems
			idx.DataSize = (46 + idx.ActualKeySize) * idx.ActualNumDocs
		}
	} else {
		if idx.AvgDocKeySize != 0 {
			// primary index mem size : (74 + DocIdLen) * NumberOfItems
			idx.DataSize = (74 + idx.AvgDocKeySize) * idx.NumOfDocs
		} else if idx.ActualKeySize != 0 {
			// primary index mem size : ActualKeySize * NumberOfItems
			// actual key size = mem used / num docs (include both main and back index)
			idx.DataSize = idx.ActualKeySize * idx.ActualNumDocs
		}
	}

	if idx.ResidentRatio == 0 {
		idx.ResidentRatio = 100
	}
	idx.MemUsage = idx.DataSize * uint64(idx.ResidentRatio) / 100

	// compute cpu usage
	idx.CpuUsage = float64(idx.MutationRate)/float64(MOIMutationRatePerCore) + float64(idx.ScanRate)/float64(MOIScanRatePerCore)

	idx.MemOverhead = s.ComputeIndexOverhead(idx)
}

// This function estimates the index memory overhead
func (s *PlasmaSizingMethod) ComputeIndexOverhead(idx *IndexUsage) uint64 {

	// protobuf overhead : 150MB per index
	overhead := float64(150 * 1024 * 1024)

	snapshotOverhead := float64(0)

	mvcc := func() uint64 {
		count := uint64(idx.MutationRate * 60 * 20)
		numDocs := uint64(0)
		if idx.ActualNumDocs != 0 {
			numDocs = idx.ActualNumDocs
		} else {
			numDocs = idx.NumOfDocs
		}
		if numDocs*3 < count {
			return numDocs * 3
		}
		return count
	}

	// incoming mutation buffer overhead: 30K * SizePerItem * NumberOfIndexes * MutationRate/500
	if idx.AvgSecKeySize != 0 {
		overhead += float64(30*1000*(idx.AvgSecKeySize+idx.AvgDocKeySize)) * float64(idx.MutationRate) / float64(500)
		snapshotOverhead += float64(mvcc()*(idx.AvgSecKeySize+idx.AvgDocKeySize+114)) * 2 // for back index
	} else if idx.AvgArrKeySize != 0 {
		overhead += float64(30*1000*(idx.AvgArrKeySize*idx.AvgArrSize+idx.AvgDocKeySize)) * float64(idx.MutationRate) / float64(500)
		snapshotOverhead += float64(mvcc()*(46+(idx.AvgArrKeySize+idx.AvgDocKeySize+74)*idx.AvgArrSize)) * 2 // for back index
	} else if idx.AvgDocKeySize != 0 {
		overhead += float64(30*1000*idx.AvgDocKeySize) * float64(idx.MutationRate) / float64(500)
		snapshotOverhead += float64(mvcc() * (idx.AvgDocKeySize + 74))
	} else if idx.ActualKeySize != 0 {
		overhead += float64(30*1000*(idx.ActualKeySize)) * float64(idx.MutationRate) / float64(500)
		// actual key size = mem used / num docs (include both main and back index)
		snapshotOverhead += float64(mvcc() * idx.ActualKeySize)
	}

	// snapshot overhead
	overhead += snapshotOverhead

	// mutation queue size : 10% of indexer memory usage
	mutationQueueOverhead := (float64(idx.MemUsage) + snapshotOverhead) * 0.1
	overhead += mutationQueueOverhead

	// golang overhead: 5% of total memory
	golangOverhead := (float64(idx.MemUsage) + snapshotOverhead + mutationQueueOverhead) * 0.05
	overhead += golangOverhead

	return uint64(overhead)
}

// This function estimates the min memory quota given a set of indexes
func (s *PlasmaSizingMethod) ComputeMinQuota(indexes []*IndexUsage, useLive bool) (uint64, uint64) {

	maxCpuUsage := float64(0)
	maxMemUsage := uint64(0)

	for _, index := range indexes {
		if index.GetMemTotal(useLive) > maxMemUsage {
			maxMemUsage = index.GetMemTotal(useLive)
		}

		if index.GetCpuUsage(useLive) > maxCpuUsage {
			maxCpuUsage = index.GetCpuUsage(useLive)
		}
	}

	// channel overhead : 100MB
	overhead := float64(100 * 1024 * 1024)

	// 20% buffer for mem quota
	// TODO
	//memQuota := uint64((float64(maxMemUsage) + overhead) * 1.2)
	memQuota := maxMemUsage + uint64(overhead)

	// 20% buffer for cpu quota
	// TODO
	//cpuQuota := uint64(float64(maxCpuUsage) * 1.2)
	cpuQuota := uint64(math.Floor(maxCpuUsage)) + 1

	return memQuota, cpuQuota
}

//////////////////////////////////////////////////////////////
// Violations
//////////////////////////////////////////////////////////////

// This function returns violations as a string
func (v *Violations) Error() string {
	if v == nil {
		return ""
	}

	err := fmt.Sprintf("\nMemoryQuota: %v\n", v.MemQuota)
	err += fmt.Sprintf("CpuQuota: %v\n", v.CpuQuota)

	for _, violation := range v.Violations {
		err += fmt.Sprintf("--- Violations for index <%v, %v, %v, %v> (mem %v, cpu %v) at node %v \n",
			violation.Name, violation.Bucket, violation.Scope, violation.Collection, formatMemoryStr(violation.MemUsage), violation.CpuUsage, violation.NodeId)

		for _, detail := range violation.Details {
			err += fmt.Sprintf("\t%v\n", detail)
		}
	}

	return err
}

// This function returns if there is any violation
func (v *Violations) IsEmpty() bool {

	return v == nil || len(v.Violations) == 0
}

//////////////////////////////////////////////////////////////
// GreedyPlanner
//////////////////////////////////////////////////////////////

// Constructor
func newGreedyPlanner(cost CostMethod, constraint ConstraintMethod, placement PlacementMethod,
	sizing SizingMethod, indexes []*IndexUsage) *GreedyPlanner {

	return &GreedyPlanner{
		cost:                     cost,
		constraint:               constraint,
		placement:                placement,
		sizing:                   sizing,
		newIndexes:               indexes,
		equivIndexMap:            make(map[string]bool),
		sgHasNodeWithoutEquivIdx: make(map[string]bool),
	}
}

func canUseGreedyIndexPlacement(config *RunConfig, indexes []*IndexUsage,
	movedIndex, movedData uint64, solution *Solution) (bool, common.IndexDefnId) {

	if !config.UseGreedyPlanner {
		return false, common.IndexDefnId(0)
	}

	// At this time, there aren't any eligible indexes. It is possible that the
	// cluster constraints not satisfied due to resource constraints. That should
	// not lead to use of SAPlanner. Failure of SAPlanner can lead to round robin
	// index placement which does not look at the usage based cost of the indexer
	// nodes and hence can lead to bad index placement.

	// Avoid using greedy planner if the HA constraints in the cluster are not satisfied.

	if ok := solution.constraint.SatisfyClusterHAConstraint(solution, nil, false); !ok {
		logging.Infof("Cannot use greedy planner as the cluster constraints are not satisfied.")

		return false, common.IndexDefnId(0)
	}

	// Safety check, if there are no indexes, return false.
	if len(indexes) == 0 {
		logging.Infof("Cannot use greedy planner as there aren't any indexes to place.")
		return false, common.IndexDefnId(0)
	}

	// If initial shuffling was allowed, let the SAPlanner run
	if config.Shuffle != 0 {
		logging.Infof("Cannot use greedy planner as shuffle flag is set to %v", config.Shuffle)
		return false, common.IndexDefnId(0)
	}

	// If indexes were moved for deciding initial placement, let the SAPlanner run
	if movedIndex != uint64(0) || movedData != uint64(0) {
		logging.Infof("Cannot use greedy planner as indxes/data has been moved (%v, %v)", movedIndex, movedData)
		return false, common.IndexDefnId(0)
	}

	// If there are any partitioned indexes OR
	// If there is any sizing info provided by the user OR
	// If there are instances of more than 1 index in the list of indexes,
	//     let the SAPlanner run.
	defns := make(map[common.IndexDefnId]bool)
	var defnId common.IndexDefnId
	for _, idx := range indexes {
		if idx.PartnId != 0 {
			return false, common.IndexDefnId(0)
		}

		if idx.HasSizingInputs() {
			// TODO: Why can't we allow greedy method even if sizing inputs are
			//       provided ? Esp when the index is non-partitioned ?
			logging.Infof("Cannot use greedy planner as the index has sizing inputs.")
			return false, common.IndexDefnId(0)
		}

		if len(defns) == 1 {
			if _, ok := defns[idx.DefnId]; !ok {
				logging.Infof("Cannot use greedy planner as more than one index is being placed.")
				return false, common.IndexDefnId(0)
			}
		} else {
			defns[idx.DefnId] = true
			defnId = idx.DefnId
		}
	}

	return true, defnId
}

func (p *GreedyPlanner) initializeSolution(command CommandType, solution *Solution) {
	// Do not enforce constraints (keep solution.enforceConstraint as false) as the
	// greedy approach anyways places the indexes on the least loaded nodes.
	solution.command = command
	solution.constraint = p.constraint
	solution.sizing = p.sizing
	solution.cost = p.cost
	solution.place = p.placement

	// Update server group map.
	// This will be required while validating the final solution.
	solution.initializeServerGroupMap()

	// Generate replica map.
	// This will be required for validating server group constraints of the final solution.
	solution.generateReplicaMap()

	// No need to maintain used replica id map for CommandPlan.

	// Run Size Estimation
	// Even if the size estimation is tuned for SAPlanner, the same size estimation should
	// work for greedy planner as well as the greedy planner is just trying to deterministically
	// decide which node is least loade - and hence is the optimal target for the index.

	numNewIdx := len(p.newIndexes)
	if numNewIdx == 0 {
		numNewIdx = p.numNewIndexes
	}

	solution.runSizeEstimation(p.placement, numNewIdx)

	// Update cost
	_ = p.cost.Cost(solution)
	solution.updateCost()

	// Initialise equivalent index map
	p.initEquivIndexMap(solution)
}

// Check if equivalent indexes exist in the cluster.
func (p *GreedyPlanner) initEquivIndexMap(solution *Solution) {

	if len(p.newIndexes) == 0 {
		return
	}

	for _, indexer := range solution.Placement {
		if indexer.ExcludeAny(solution) {
			continue
		}

		if indexer.IsDeleted() {
			continue
		}

		found := false

	equivLoop:
		for _, index := range indexer.Indexes {
			// This check will always work if one of the index is non-partitioned.
			if p.newIndexes[0].Instance == nil {
				continue
			}

			if index.Instance == nil {
				continue
			}

			if common.IsEquivalentIndex(&index.Instance.Defn, &p.newIndexes[0].Instance.Defn) {
				p.equivIndexMap[indexer.IndexerId] = true
				found = true
				p.numEquivIndex++

				// It should not matter if a node has one or more equivalent indexes.
				break equivLoop
			}
		}

		if !found {
			p.equivIndexMap[indexer.IndexerId] = false
			p.sgHasNodeWithoutEquivIdx[indexer.ServerGroup] = true
		}
	}
}

func (p *GreedyPlanner) Plan(command CommandType, sol *Solution) (*Solution, error) {

	if command != CommandPlan {
		return nil, fmt.Errorf("Greedy Planner does not support command %v", command)
	}

	indexes := p.newIndexes
	if len(indexes) == 0 {
		p.Result = sol
		return sol, nil
	}

	if len(p.newIndexes) != 0 && p.numNewIndexes != 0 {
		logging.Errorf("Greedy planner initialisation error. Index count mismatch (%v, %v)", len(p.newIndexes), p.numNewIndexes)
		return nil, fmt.Errorf("Greedy planner initialisation error")
	}

	solution := sol.clone()

	// Initialize solution
	p.initializeSolution(command, solution)

	if p.shardAffinity {
		err := solution.PrePopulateAlternateShardIds(command)
		if err != nil {
			return nil, err
		}

		for _, indexer := range solution.Placement {
			indexer.Indexes, indexer.NumShards, _ = GroupIndexes(
				indexer.Indexes,        // indexes []*IndexUsage
				indexer,                // indexer *IndexerNode
				command == CommandPlan, // skipDeferredIndexGrouping bool
				solution,               // solution *Solution
			)
		}

		solution.place.RegroupIndexes(command == CommandPlan)

		// Re-generate replica map based on the index grouping
		solution.resetReplicaMap()
		solution.generateReplicaMap()
		solution.initializeServerGroupMap()
	}

	// Sort the list of nodes based on usages
	// (For MOI, may be), do we need to use tie breaker as number of indexes
	// that needs estimation, instead of number of indexes that has no
	// usage info?
	// TODO:
	sortedNodeList := sortNodeByUsage(solution, solution.Placement)

	// Filter the excluded and deleted nodes
	serverGroups := make(map[string]bool)
	filteredNodeList := make([]*IndexerNode, 0)
	for _, node := range sortedNodeList {

		// Why shouldn't this be ExcludeIn? RandomPlacement.Add does ExcludeAny!
		if node.ExcludeAny(solution) {
			continue
		}

		if node.IsDeleted() {
			continue
		}

		serverGroups[node.ServerGroup] = true
		filteredNodeList = append(filteredNodeList, node)
	}

	if len(filteredNodeList) == 0 {
		solution.PrintLayout()
		return nil, ErrNoAvailableIndexer
	}

	if len(filteredNodeList) < len(indexes) {
		solution.PrintLayout()
		return nil, ErrNoAvailableIndexer
	}

	numServerGroups := len(serverGroups)
	filledServerGroups := make(map[string]bool)
	filledNodes := make(map[string]bool)

	// number of indexes to be forcefully placed even if their
	// equivalent index(es) residing are on the same node
	numForcePlaceEquivalent := p.numEquivIndex + len(indexes) - len(filteredNodeList)

	checkEquivalent := true
	if numForcePlaceEquivalent > 0 {
		checkEquivalent = false
	}

	maxSGsForced := len(indexes)
	if len(indexes) > numServerGroups {
		maxSGsForced = numServerGroups
	}

	numForceEquivForSGConstraint := maxSGsForced - len(p.sgHasNodeWithoutEquivIdx)

	getNextIndexer := func(i int) *IndexerNode {
		// Update the filled nodes map, server group map and forced equivalent index
		// placement count before returning the node
		useNode := func(node *IndexerNode) {
			if numForcePlaceEquivalent > 0 {
				if p.equivIndexMap[node.IndexerId] {
					numForcePlaceEquivalent--
				}
			}

			filledServerGroups[node.ServerGroup] = true
			filledNodes[node.NodeId] = true
		}

		// If required, forcefully fill any SGs which have equiv on every node in the SG
		// to maintain availability across SGs
		if numForceEquivForSGConstraint > 0 && numServerGroups > len(filledServerGroups) {
			for _, node := range filteredNodeList {
				if _, ok := filledNodes[node.NodeId]; ok {
					continue
				}

				if _, ok := filledServerGroups[node.ServerGroup]; ok {
					continue
				}

				if hasNode, ok := p.sgHasNodeWithoutEquivIdx[node.ServerGroup]; ok && hasNode {
					continue
				}

				useNode(node)
				numForceEquivForSGConstraint--
				return node
			}
		}

		// Check if Server Group Constraint needs to be honored.
		if numServerGroups > len(filledServerGroups) {
			for _, node := range filteredNodeList {
				if _, ok := filledNodes[node.NodeId]; ok {
					continue
				}

				if _, ok := filledServerGroups[node.ServerGroup]; ok {
					continue
				}

				if numForcePlaceEquivalent > 0 || !p.equivIndexMap[node.IndexerId] {
					useNode(node)
					return node
				}
			}

			return nil
		}

		// Server Group Constraint is already honored
		for _, node := range filteredNodeList {
			if _, ok := filledNodes[node.NodeId]; ok {
				continue
			}

			if numForcePlaceEquivalent > 0 || !p.equivIndexMap[node.IndexerId] {
				useNode(node)
				return node
			}
		}

		return nil
	}

	// Place the indexes
	for i, idx := range indexes {
		indexer := getNextIndexer(i)
		if indexer == nil {
			solution.PrintLayout()
			return nil, ErrNoAvailableIndexer
		}

		if equiv, ok := p.equivIndexMap[indexer.IndexerId]; ok && equiv {
			idx.suppressEquivIdxCheck = true
		}

		p.placement.AddToIndexer(solution, indexer, idx)
	}

	if ok := solution.constraint.SatisfyClusterHAConstraint(solution, nil, checkEquivalent); !ok {
		solution.PrintLayout()
		return nil, errors.New("Cannot satisfy cluster constraints by greedy placement method")
	}

	p.Result = solution
	return solution, nil
}

func (p *GreedyPlanner) Initialise(command CommandType, sol *Solution) (*Solution, error) {
	if command != CommandPlan {
		return nil, fmt.Errorf("Greedy Planner does not support command %v", command)
	}

	if len(p.newIndexes) != 0 && p.numNewIndexes != 0 {
		logging.Errorf("Greedy planner initialisation error. Index count mismatch (%v, %v)", len(p.newIndexes), p.numNewIndexes)
		return nil, fmt.Errorf("Greedy planner initialisation error")
	}

	solution := sol.clone()

	// Initialize solution
	p.initializeSolution(command, solution)

	for _, indexer := range solution.Placement {
		indexer.UsageRatio = solution.computeUsageRatio(indexer)
	}

	return solution, nil
}

func (p *GreedyPlanner) GetResult() *Solution {
	return p.Result
}

func (p *GreedyPlanner) Print() {
	if p.Result != nil {
		logging.Infof("Using GreedyPlanner")
		p.cost.Print()
		logging.Infof("----------------------------------------")
		p.Result.PrintStats()
		logging.Infof("----------------------------------------")
		p.constraint.Print()
		logging.Infof("----------------------------------------")
		p.placement.Print()
		logging.Infof("----------------------------------------")
		p.Result.PrintLayout()
	}
}

func (p *GreedyPlanner) PrintCost() {
}

func (p *GreedyPlanner) SetParam(param map[string]interface{}) error {
	return nil
}

func (p *GreedyPlanner) SetShardAffinity(shardAffinity bool) {
	p.shardAffinity = shardAffinity
}

// During index creation, either of SAPlanner and GreedyPlanner can be used. The
// decision to use a specific planner depends on the input indexes - to be created
// and the planner config.
// This function (NewPlannerForCommandPlan) decides which planner implementation is
// better for the given input indexes and given planner config. Based on the
// decision, the correspnoding planner object will be returned.
func NewPlannerForCommandPlan(config *RunConfig, indexes []*IndexUsage, movedIndex, movedData uint64,
	solution *Solution, cost CostMethod, constraint ConstraintMethod, placement PlacementMethod,
	sizing SizingMethod) (Planner, error) {

	var planner Planner

	if ok, defnId := canUseGreedyIndexPlacement(config, indexes, movedIndex, movedData, solution); ok {
		logging.Infof("Using greedy index placement for index %v", defnId)
		planner = newGreedyPlanner(cost, constraint, placement, sizing, indexes)
	} else {
		if err := placement.Add(solution, indexes); err != nil {
			return nil, err
		}

		planner = newSAPlanner(cost, constraint, placement, sizing)
	}

	return planner, nil
}

//TenantAwarePlanner Implmentation

func (p *TenantAwarePlanner) Plan(command CommandType, sol *Solution) (*Solution, error) {
	return nil, nil
}

func (p *TenantAwarePlanner) GetResult() *Solution {
	return p.Result
}

func (p *TenantAwarePlanner) Print() {
	if p.Result != nil {
		logging.Infof("Using TenantAwarePlanner")
		logging.Infof("----------------------------------------")
		p.Result.PrintLayout()
	}
}

func (p *TenantAwarePlanner) PrintCost() {
}

func (p *TenantAwarePlanner) SetParam(param map[string]interface{}) error {
	return nil
}

func (p *TenantAwarePlanner) SetShardAffinity(shardAffinity bool) {
	return // no-op for tenant aware planner
}
