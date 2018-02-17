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
	"errors"
	"fmt"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"math"
	"math/rand"
	"strconv"
	"time"
)

//TODO

// - retry proxy when there is transient netowrk error
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
	IterationPerTemp   int     = 1000
	ResizePerIteration int     = 1000
	RunPerPlan         int     = 10
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
)

// constant - violation code
type ViolationCode string

const (
	NoViolation          ViolationCode = "NoViolation"
	MemoryViolation                    = "MemoryViolation"
	CpuViolation                       = "CpuViolation"
	ReplicaViolation                   = "ReplicaViolation"
	EquivIndexViolation                = "EquivIndexViolation"
	ServerGroupViolation               = "ServerGroupViolation"
	DeleteNodeViolation                = "DeleteNodeViolation"
	ExcludeNodeViolation               = "ExcludeNodeViolation"
)

//////////////////////////////////////////////////////////////
// Interface
//////////////////////////////////////////////////////////////

type Planner interface {
	Plan(indexers []*IndexerNode, indexes []*IndexUsage) *Solution
	Print()
}

type CostMethod interface {
	Cost(s *Solution) float64
	Print()
	Validate(s *Solution) error
}

type PlacementMethod interface {
	Move(s *Solution) (bool, bool, bool)
	Add(s *Solution, indexes []*IndexUsage) error
	InitialPlace(s *Solution, indexes []*IndexUsage) error
	Validate(s *Solution) error
	GetEligibleIndexes() map[*IndexUsage]bool
	AddOptionalIndexes([]*IndexUsage)
	RemoveOptionalIndexes() []*IndexUsage
	HasOptionalIndexes() bool
	RemoveEligibleIndex([]*IndexUsage)
}

type ConstraintMethod interface {
	GetMemQuota() uint64
	GetCpuQuota() uint64
	SatisfyClusterResourceConstraint(s *Solution) bool
	SatisfyNodeResourceConstraint(s *Solution, n *IndexerNode) bool
	SatisfyNodeHAConstraint(s *Solution, n *IndexerNode, eligibles map[*IndexUsage]bool) bool
	SatisfyIndexHAConstraint(s *Solution, n *IndexerNode, index *IndexUsage, eligibles map[*IndexUsage]bool) bool
	SatisfyClusterConstraint(s *Solution, eligibles map[*IndexUsage]bool) bool
	SatisfyNodeConstraint(s *Solution, n *IndexerNode, eligibles map[*IndexUsage]bool) bool
	SatisfyServerGroupConstraint(s *Solution, n *IndexUsage, group string) bool
	CanAddIndex(s *Solution, n *IndexerNode, u *IndexUsage) ViolationCode
	CanSwapIndex(s *Solution, n *IndexerNode, t *IndexUsage, i *IndexUsage) ViolationCode
	CanAddNode(s *Solution) bool
	Print()
	Validate(s *Solution) error
	GetViolations(s *Solution, indexes map[*IndexUsage]bool) *Violations
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

type IndexerNode struct {
	// input: node identification
	NodeId      string `json:"nodeId"`
	NodeUUID    string `json:"nodeUUID"`
	IndexerId   string `json:"indexerId"`
	RestUrl     string `json:"restUrl"`
	ServerGroup string `json:"serverGroup,omitempty"`
	StorageMode string `json:"storageMode,omitempty"`

	// input/output: resource consumption (from sizing)
	MemUsage    uint64  `json:"memUsage"`
	CpuUsage    float64 `json:"cpuUsage"`
	DiskUsage   uint64  `json:"diskUsage,omitempty"`
	MemOverhead uint64  `json:"memOverhead"`
	DataSize    uint64  `json:"dataSize"`

	// input/output: resource consumption (from live cluster)
	ActualMemUsage    uint64  `json:"actualMemUsage"`
	ActualMemOverhead uint64  `json:"actualMemOverhead"`
	ActualCpuUsage    float64 `json:"actualCpuUsage"`
	ActualDataSize    uint64  `json:"actualDataSize"`

	// input: index residing on the node
	Indexes []*IndexUsage `json:"indexes"`

	// input: node status
	isDelete bool
	isNew    bool
	exclude  string

	// intput/output: planning
	meetConstraint bool
}

type IndexUsage struct {
	// input: index identification
	DefnId  common.IndexDefnId `json:"defnId"`
	InstId  common.IndexInstId `json:"instId"`
	PartnId common.PartitionId `json:"partnId"`
	Name    string             `json:"name"`
	Bucket  string             `json:"bucket"`
	Hosts   []string           `json:"host"`

	// input: index sizing
	IsPrimary     bool    `json:"isPrimary,omitempty"`
	StorageMode   string  `json:"storageMode,omitempty"`
	AvgSecKeySize uint64  `json:"avgSecKeySize"`
	AvgDocKeySize uint64  `json:"avgDocKeySize"`
	AvgArrSize    uint64  `json:"avgArrSize"`
	AvgArrKeySize uint64  `json:"avgArrKeySize"`
	NumOfDocs     uint64  `json:"numOfDocs"`
	ResidentRatio float64 `json:"residentRatio,omitempty"`
	MutationRate  uint64  `json:"mutationRate"`
	ScanRate      uint64  `json:"scanRate"`

	// input: resource consumption (from sizing equation)
	MemUsage    uint64  `json:"memUsage"`
	CpuUsage    float64 `json:"cpuUsage"`
	DiskUsage   uint64  `json:"diskUsage,omitempty"`
	MemOverhead uint64  `json:"memOverhead,omitempty"`
	DataSize    uint64  `json:"dataSize,omitempty"`

	// input: resource consumption (from live cluster)
	ActualMemUsage        uint64  `json:"actualMemUsage"`
	ActualMemOverhead     uint64  `json:"actualMemOverhead"`
	ActualKeySize         uint64  `json:"actualKeySize"`
	ActualCpuUsage        float64 `json:"actualCpuUsage"`
	ActualBuildPercent    uint64  `json:"actualBuildPercent"`
	ActualResidentPercent uint64  `json:"actualResidentPercent"`
	ActualDataSize        uint64  `json:"actualDataSize"`

	// input: resource consumption (estimated sizing)
	NoUsageInfo       bool   `json:"NoUsageInfo"`
	EstimatedMemUsage uint64 `json:"estimatedMemUsage"`
	EstimatedDataSize uint64 `json:"estimatedDataSize"`

	// input: index definition (optional)
	Instance *common.IndexInst `json:"instance,omitempty"`

	// input: node where index initially placed (optional)
	// for new indexes to be placed on an existing topology (e.g. live cluster), this must not be set.
	initialNode *IndexerNode

	// input: flag to indicate if the index in delete or create token
	pendingDelete bool // true if there is a delete token associated with this index
	pendingCreate bool // true if there is a create token associated with this index

	// mutable: hint for placement / constraint
	suppressEquivIdxCheck bool
}

type Solution struct {
	command        CommandType
	constraint     ConstraintMethod
	sizing         SizingMethod
	cost           CostMethod
	place          PlacementMethod
	isLiveData     bool
	useLiveData    bool
	disableRepair  bool
	initialPlan    bool
	numServerGroup int
	numDeletedNode int
	numNewNode     int

	// for size estimation
	estimatedIndexSize uint64
	estimate           bool
	numEstimateRun     int

	// for rebalance
	enableExclude bool

	// for placement
	currentCost float64

	// placement of indexes	in nodes
	Placement []*IndexerNode `json:"placement,omitempty"`
}

type Violations struct {
	Violations []*Violation
	MemQuota   uint64
	CpuQuota   uint64
}

type Violation struct {
	Name     string
	Bucket   string
	NodeId   string
	CpuUsage float64
	MemUsage uint64
	Details  []string
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
	timeout int

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

//////////////////////////////////////////////////////////////
// Interface Implementation - CostMethod
//////////////////////////////////////////////////////////////

type UsageBasedCostMethod struct {
	MemMean        float64 `json:"memMean,omitempty"`
	MemStdDev      float64 `json:"memStdDev,omitempty"`
	CpuMean        float64 `json:"cpuMean,omitempty"`
	CpuStdDev      float64 `json:"cpuStdDev,omitempty"`
	DataSizeMean   float64 `json:"dataSizeMean,omitempty"`
	DataSizeStdDev float64 `json:"dataSizeStdDev,omitempty"`
	TotalData      uint64  `json:"totalData,omitempty"`
	DataMoved      uint64  `json:"dataMoved,omitempty"`
	TotalIndex     uint64  `json:"totalIndex,omitempty"`
	IndexMoved     uint64  `json:"indexMoved,omitempty"`
	IdxMean        float64 `json:"idxMean,omitempty"`
	IdxStdDev      float64 `json:"idxStdDev,omitempty"`
	MemFree        float64 `json:"memFree,omitempty"`
	CpuFree        float64 `json:"cpuFree,omitempty"`
	constraint     ConstraintMethod
	dataCostWeight float64
	cpuCostWeight  float64
	memCostWeight  float64
}

//////////////////////////////////////////////////////////////
// Interface Implementation - PlacementMethod
//////////////////////////////////////////////////////////////

type RandomPlacement struct {
	rs              *rand.Rand
	indexes         map[*IndexUsage]bool
	eligibles       []*IndexUsage
	optionals       []*IndexUsage
	allowSwap       bool
	swapDeletedOnly bool
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
// Interface Implementation - ConstraintMethod
//////////////////////////////////////////////////////////////

type IndexerConstraint struct {
	// system level constraint
	MemQuota   uint64 `json:"memQuota,omitempty"`
	CpuQuota   uint64 `json:"cpuQuota,omitempty"`
	MaxMemUse  int64  `json:"maxMemUse,omitempty"`
	MaxCpuUse  int64  `json:"maxCpuUse,omitempty"`
	canResize  bool
	maxNumNode uint64
}

//////////////////////////////////////////////////////////////
// SAPlanner
//////////////////////////////////////////////////////////////

//
// Constructor
//
func newSAPlanner(cost CostMethod, constraint ConstraintMethod, placement PlacementMethod, sizing SizingMethod) *SAPlanner {
	return &SAPlanner{
		cost:       cost,
		constraint: constraint,
		placement:  placement,
		sizing:     sizing,
	}
}

//
// Given a solution, this function use simulated annealing
// to find an alternative solution with a lower cost.
//
func (p *SAPlanner) Plan(command CommandType, solution *Solution) (*Solution, error) {

	var result *Solution
	var err error

	solution.command = command
	solution = p.adjustInitialSolutionIfNecessary(solution)

	for i := 0; i < RunPerPlan; i++ {
		p.Try++
		startTime := time.Now()
		solution.runSizeEstimation(p.placement)

		err = p.Validate(solution)
		if err == nil {
			result, err = p.planSingleRun(command, solution)

			// if err == nil, type assertion will return !ok
			if _, ok := err.(*Violations); !ok {
				return result, err
			}

			// copy estimation information
			if result != nil {
				solution.copyEstimationFrom(result)
			}
		}

		// If planner get to this point, it means we see violation errors.
		// If planner has retries 3 times, then remove any optional indexes.
		if i > 3 && p.placement.HasOptionalIndexes() {
			logging.Infof("Cannot rebuild lost replica due to resource constraint in cluster.  Will not rebuild lost replica.")
			optionals := p.placement.RemoveOptionalIndexes()
			solution.removeIndexes(optionals)
		}

		// If cannot find a solution after 3 tries and there are deleted nodes, then disable exclude flag.
		if i == 3 && solution.numDeletedNode != 0 {
			solution.enableExclude = false
		}

		logging.Infof("Planner::Fail to create plan satisyfig constraint. Re-planning. Num of Try=%v.  Elapsed Time=%v",
			p.Try, formatTimeStr(uint64(time.Now().Sub(startTime).Nanoseconds())))
	}

	return result, err
}

//
// Given a solution, this function use simulated annealing
// to find an alternative solution with a lower cost.
//
func (p *SAPlanner) planSingleRun(command CommandType, solution *Solution) (*Solution, error) {

	current := solution.clone()
	initialPlan := solution.initialPlan

	logging.Tracef("Planner: memQuota %v (%v) cpuQuota %v",
		p.constraint.GetMemQuota(), formatMemoryStr(p.constraint.GetMemQuota()), p.constraint.GetCpuQuota())

	rs := rand.New(rand.NewSource(time.Now().UnixNano()))

	old_cost := p.cost.Cost(current)
	startScore := old_cost
	startTime := time.Now()
	lastUpdateTime := time.Now()
	move := uint64(0)
	iteration := uint64(0)
	positiveMove := uint64(0)

	temperature := p.initialTemperature(command, old_cost)
	startTemp := temperature
	done := false

	for temperature > MinTemperature && !done {
		lastMove := move
		lastPositiveMove := positiveMove
		current.currentCost = old_cost
		for i := 0; i < IterationPerTemp; i++ {
			new_solution, force, final := p.findNeighbor(current)
			if new_solution != nil {
				new_cost := p.cost.Cost(new_solution)
				prob := p.getAcceptProbability(old_cost, new_cost, temperature)

				logging.Tracef("Planner::old_cost-new_cost %v new_cost % v temp %v prob %v force %v",
					old_cost-new_cost, new_cost, temperature, prob, force)

				if old_cost-new_cost > 0 {
					positiveMove++
				}

				// if force=true, then jsut accept the new solution.  Do
				// not need to change the temperature since new solution
				// could have higher score.
				if force || prob > rs.Float64() {
					current = new_solution
					current.currentCost = new_cost
					old_cost = new_cost
					lastUpdateTime = time.Now()
					move++

					logging.Tracef("Planner::accept solution: new_cost %v temp %v", new_cost, temperature)
				}

				iteration++
			}

			if final {
				done = true
				break
			}
		}

		if int64(move-lastMove) < MinNumMove && int64(positiveMove-lastPositiveMove) < MinNumPositiveMove {
			done = true
		}

		temperature = temperature * Alpha

		if command == CommandPlan && initialPlan {
			// adjust temperature based on score for faster convergence
			temperature = temperature * old_cost
		}

		if p.timeout > 0 {
			elapsed := time.Now().Sub(startTime).Seconds()
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

	eligibles := p.placement.GetEligibleIndexes()
	if !p.constraint.SatisfyClusterConstraint(p.Result, eligibles) {
		return current, p.constraint.GetViolations(p.Result, eligibles)
	}

	p.cost.Cost(p.Result)
	return current, nil
}

func (p *SAPlanner) SetTimeout(timeout int) {
	p.timeout = timeout
}

//
// Validate the solution
//
func (p *SAPlanner) Validate(s *Solution) error {

	if err := p.sizing.Validate(s); err != nil {
		return err
	}

	if err := p.cost.Validate(s); err != nil {
		return err
	}

	if err := p.constraint.Validate(s); err != nil {
		return err
	}

	if err := p.placement.Validate(s); err != nil {
		return err
	}

	return nil
}

//
// This function prints the result of evaluation
//
func (p *SAPlanner) PrintRunSummary() {

	logging.Infof("Score: %v", p.Score)
	logging.Infof("ElapsedTime: %v", formatTimeStr(p.ElapseTime))
	logging.Infof("ConvergenceTime: %v", formatTimeStr(p.ConvergenceTime))
	logging.Infof("Iteration: %v", p.Iteration)
	logging.Infof("Move: %v", p.Move)
}

//
// This function prints the result of evaluation
//
func (p *SAPlanner) Print() {

	p.PrintRunSummary()
	logging.Infof("----------------------------------------")

	if p.Result != nil {
		p.cost.Print()
		logging.Infof("----------------------------------------")
		p.Result.PrintStats()
		logging.Infof("----------------------------------------")
		p.constraint.Print()
		logging.Infof("----------------------------------------")
		p.Result.PrintLayout()
	}
}

//
// This function prints the result of evaluation
//
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

//
// This function prints the result of evaluation
//
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

//
// This function finds a neigbhor placement layout using
// given placement method.
//
func (p *SAPlanner) findNeighbor(s *Solution) (*Solution, bool, bool) {

	currentOK := s.SatisfyClusterConstraint()
	neighbor := s.clone()

	force := false
	done := false
	retry := 0

	for retry = 0; retry < ResizePerIteration; retry++ {
		success, final, mustAccept := p.placement.Move(neighbor)
		if success {
			neighborOK := neighbor.SatisfyClusterConstraint()
			logging.Tracef("Planner::findNeighbor retry: %v", retry)
			return neighbor, (mustAccept || force || (!currentOK && neighborOK)), final
		}

		// Add new node to change cluster in order to ensure constraint can be satisfied
		if !neighbor.SatisfyClusterConstraint() {
			if neighbor.canRunEstimation() {
				neighbor.runSizeEstimation(p.placement)
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
	return nil, false, done
}

//
// Get the initial temperature.
//
func (p *SAPlanner) initialTemperature(command CommandType, cost float64) float64 {

	if command == CommandPlan {
		return MaxTemperature
	}

	temp := MaxTemperature
	if cost > 0 && cost < 0.3 {
		temp = cost * MaxTemperature * 0.1
	}

	logging.Tracef("Planner::initial temperature: initial cost %v temp %v", cost, temp)
	return temp
}

//
// This function calculates the acceptance probability of this solution based on cost.
//
func (p *SAPlanner) getAcceptProbability(old_cost float64, new_cost float64, temperature float64) float64 {
	// always accept if new_cost is lower than old cost
	if new_cost < old_cost {
		return 1.0
	}

	// new_cost is higher or equal to old_cost.  But still consider this solution based on probability.
	// Low propbabilty when
	// 1) low temperature (many iterations have passed)
	// 2) differnce between new_cost and old_cost are high
	cost := (old_cost - new_cost)
	return math.Exp(cost / temperature)
}

//
// Adjust solution constraint depending on the solution and command.
//
func (p *SAPlanner) adjustInitialSolutionIfNecessary(s *Solution) *Solution {

	s.constraint = p.constraint
	s.sizing = p.sizing
	s.cost = p.cost
	s.place = p.placement

	// update the number of new nodes and deleted node
	s.numDeletedNode = s.findNumDeleteNodes()
	s.numNewNode = s.findNumEmptyNodes()
	s.markNewNodes()

	// if there is deleted node and all nodes are excluded to take in new indexes,
	// then do not enable node exclusion
	if s.numDeletedNode != 0 && s.findNumExcludeInNodes() == len(s.Placement) {
		s.enableExclude = false
	}

	// evalute if node meet constraint
	s.evaluateNodeConstraint()

	// If not using live data, then no need to relax constraint.
	if !s.UseLiveData() {
		return s
	}

	if p.constraint.CanAddNode(s) {
		return s
	}

	cloned := s.clone()

	// Make sure we only repair when it is rebalancing
	if s.command != CommandPlan {
		p.dropReplicaIfNecessary(cloned)
		p.addReplicaIfNecessary(cloned)
		p.addPartitionIfNecessary(cloned)
	}
	p.suppressEqivIndexIfNecessary(cloned)

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

//
// Drop replica from ejected node if there is not enough nodes in the cluster.
//
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
				if (numReplica > numLiveNode) && indexer.isDelete {
					deleteCandidates[index.GetPartitionName()] = append(deleteCandidates[index.GetPartitionName()], index)
					numReplicas[index.GetPartitionName()] = numReplica
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
			for _, candidate := range deleteCandidates[index.GetPartitionName()] {
				if candidate == index {
					found = true
					break
				}
			}

			if !found {
				keepCandidates = append(keepCandidates, index)
			} else {
				logging.Warnf("There is more replia than available nodes.  Will not move index replica (%v,%v) from ejected node %v",
					index.Bucket, index.Name, indexer.NodeId)

				c := []*IndexUsage{index}
				p.placement.RemoveEligibleIndex(c)
			}
		}

		indexer.Indexes = keepCandidates
	}
}

//
// Suppress equivalent index check if there are not enough nodes in the cluster to host all
// equivalent index.
//
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
					logging.Warnf("There are more equivalent index than available nodes.  Allow equivalent index of (%v, %v) to be replaced on same node.",
						index.Bucket, index.Name)
					index.suppressEquivIdxCheck = true

					if index.Instance != nil {
						logging.Warnf("Definition %v Instance %v ReplicaId %v partitionId %v count %v numLiveNode %v.",
							index.DefnId, index.InstId, index.Instance.ReplicaId, index.PartnId, count, numLiveNode)
					}
				} else {
					index.suppressEquivIdxCheck = false
				}
			}
		}
	}
}

//
// Add replica if there is enough nodes in the cluster.
//
func (p *SAPlanner) addReplicaIfNecessary(s *Solution) {

	if s.disableRepair {
		return
	}

	// numLiveNode = newNode + existing node - excluded node
	numLiveNode := s.findNumAvailLiveNode()

	// Check to see if it is needed to add replica
	for _, indexer := range s.Placement {
		addCandidates := make(map[*IndexUsage]*IndexerNode)

		for _, index := range indexer.Indexes {
			// If the number of replica in cluster is smaller than the desired number
			// of replica (from index definition), and there is enough nodes in the
			// cluster to host all the replica.  Also do not repair if the index
			// could be deleted by user.
			numReplica := s.findNumReplica(index)
			if index.Instance != nil && int(index.Instance.Defn.NumReplica+1) > numReplica &&
				numReplica < numLiveNode && !index.pendingDelete {

				target := s.FindIndexerWithNoReplica(index)
				if target == nil && !indexer.ExcludeAny(s) {
					target = indexer
				}

				if target != nil {
					addCandidates[index] = target
				}
			}
		}

		if len(addCandidates) != 0 {
			clonedCandidates := ([]*IndexUsage)(nil)

			for index, indexer := range addCandidates {
				numReplica := s.findNumReplica(index)
				missing := s.findMissingReplica(index)

				for replicaId, instId := range missing {
					if numReplica < numLiveNode {

						if index.Instance != nil {

							// clone the original and update the replicaId
							cloned := index.clone()
							cloned.Instance.ReplicaId = replicaId
							cloned.initialNode = nil

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

							// add the new replica to the solution
							s.addIndex(indexer, cloned)

							clonedCandidates = append(clonedCandidates, cloned)
							numReplica++

							logging.Infof("Rebuilding lost replica for (%v,%v,%v)", index.Bucket, index.Name, replicaId)
						}
					}
				}
			}

			if len(clonedCandidates) != 0 {
				p.placement.AddOptionalIndexes(clonedCandidates)
			}
		}
	}
}

//
// Add missing partition if there is enough nodes in the cluster.
//
func (p *SAPlanner) addPartitionIfNecessary(s *Solution) {

	if s.disableRepair {
		return
	}

	candidates := make(map[common.IndexInstId]*IndexUsage)
	done := make(map[common.IndexInstId]bool)
	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {
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

	if len(candidates) != 0 {
		var available []*IndexerNode
		var allCloned []*IndexUsage

		for _, indexer := range s.Placement {
			if !indexer.ExcludeAny(s) {
				available = append(available, indexer)
			}
		}

		if len(available) == 0 {
			logging.Warnf("Planner: Cannot repair lost partitions because all indexer nodes are excluded for rebalancing")
			return
		}

		for _, candidate := range candidates {
			missing := s.findMissingPartition(candidate)
			for _, partitionId := range missing {

				// clone the original and update the partitionId
				// Does not need to modify Instance.Pc
				cloned := candidate.clone()
				cloned.PartnId = partitionId
				cloned.initialNode = nil

				n := rand.Intn(len(available))
				indexer := available[n]

				// add the new partition to the solution
				s.addIndex(indexer, cloned)
				allCloned = append(allCloned, cloned)

				logging.Infof("Rebuilding lost partition for (%v,%v,%v,%v)", cloned.Bucket, cloned.Name, cloned.Instance.ReplicaId, cloned.PartnId)
			}
		}

		if len(allCloned) != 0 {
			p.placement.AddOptionalIndexes(allCloned)
		}
	}
}

//////////////////////////////////////////////////////////////
// Solution
//////////////////////////////////////////////////////////////

//
// Constructor
//
func newSolution(constraint ConstraintMethod, sizing SizingMethod, indexers []*IndexerNode, isLive bool, useLive bool, disableRepair bool) *Solution {

	r := &Solution{
		constraint:    constraint,
		sizing:        sizing,
		Placement:     make([]*IndexerNode, len(indexers)),
		isLiveData:    isLive,
		useLiveData:   useLive,
		disableRepair: disableRepair,
		estimate:      true,
		enableExclude: true,
	}

	// initialize list of indexers
	if len(indexers) == 0 {
		// create at least one indexer if none exist
		nodeId := strconv.FormatUint(uint64(rand.Uint32()), 10)
		r.addNewNode(nodeId)
		r.initialPlan = true
	} else {
		// initialize placement from the current set of indexers
		for i, _ := range indexers {
			r.Placement[i] = indexers[i].clone()
		}
	}

	r.numServerGroup = r.findNumServerGroup()

	return r
}

//
// Whether solution should use live data
//
func (s *Solution) UseLiveData() bool {
	return s.isLiveData && s.useLiveData
}

//
// Get the constriant method for the solution
//
func (s *Solution) getConstraintMethod() ConstraintMethod {
	return s.constraint
}

//
// Get the sizing method for the solution
//
func (s *Solution) getSizingMethod() SizingMethod {
	return s.sizing
}

//
// Add new indexer node
//
func (s *Solution) addNewNode(nodeId string) {

	node := newIndexerNode(nodeId, s.sizing)
	s.Placement = append(s.Placement, node)
	s.estimationOn()
}

//
// Move a single index from one node to another
//
func (s *Solution) moveIndex(source *IndexerNode, idx *IndexUsage, target *IndexerNode) {

	sourceIndex := s.findIndexOffset(source, idx)
	if sourceIndex == -1 {
		return
	}

	// add to new node
	s.addIndex(target, idx)

	// remove from old node
	s.removeIndex(source, sourceIndex)
}

//
// Find an indexer offset from the placement list
//
func (s *Solution) findIndexerOffset(node *IndexerNode) int {

	for i, indexer := range s.Placement {
		if indexer == node {
			return i
		}
	}

	return -1
}

//
// Find an index offset from indexer node
//
func (s *Solution) findIndexOffset(node *IndexerNode, index *IndexUsage) int {

	for i, idx := range node.Indexes {
		if idx == index {
			return i
		}
	}

	return -1
}

//
// Add index to a node
//
func (s *Solution) addIndex(n *IndexerNode, idx *IndexUsage) {
	n.Indexes = append(n.Indexes, idx)
	n.AddMemUsageOverhead(s, idx.GetMemUsage(s.UseLiveData()), idx.GetMemOverhead(s.UseLiveData()))
	n.AddCpuUsage(s, idx.GetCpuUsage(s.UseLiveData()))
	n.AddDataSize(s, idx.GetDataSize(s.UseLiveData()))
	n.EvaluateNodeConstraint(s)
}

//
// Remove index from a node
//
func (s *Solution) removeIndex(n *IndexerNode, i int) {
	idx := n.Indexes[i]

	if i+1 < len(n.Indexes) {
		n.Indexes = append(n.Indexes[:i], n.Indexes[i+1:]...)
	} else {
		n.Indexes = n.Indexes[:i]
	}

	n.SubtractMemUsageOverhead(s, idx.GetMemUsage(s.UseLiveData()), idx.GetMemOverhead(s.UseLiveData()))
	n.SubtractCpuUsage(s, idx.GetCpuUsage(s.UseLiveData()))
	n.SubtractDataSize(s, idx.GetDataSize(s.UseLiveData()))
	n.EvaluateNodeConstraint(s)
}

//
// Remove indexes in topology
//
func (s *Solution) removeIndexes(indexes []*IndexUsage) {
	for _, target := range indexes {
		for _, indexer := range s.Placement {
			for i, index := range indexer.Indexes {
				if index == target {
					s.removeIndex(indexer, i)
				}
			}
		}
	}
}

//
// This function makes a copy of existing solution.
//
func (s *Solution) clone() *Solution {

	r := &Solution{
		command:            s.command,
		constraint:         s.constraint,
		sizing:             s.sizing,
		cost:               s.cost,
		place:              s.place,
		Placement:          ([]*IndexerNode)(nil),
		isLiveData:         s.isLiveData,
		useLiveData:        s.useLiveData,
		initialPlan:        s.initialPlan,
		numServerGroup:     s.numServerGroup,
		numDeletedNode:     s.numDeletedNode,
		numNewNode:         s.numNewNode,
		disableRepair:      s.disableRepair,
		estimatedIndexSize: s.estimatedIndexSize,
		estimate:           s.estimate,
		numEstimateRun:     s.numEstimateRun,
		enableExclude:      s.enableExclude,
		currentCost:        s.currentCost,
	}

	for _, node := range s.Placement {
		if node.isDelete && len(node.Indexes) == 0 {
			continue
		}
		r.Placement = append(r.Placement, node.clone())
	}

	return r
}

//
// This function makes a copy of existing solution.
//
func (s *Solution) removeEmptyDeletedNode() {

	var result []*IndexerNode

	for _, node := range s.Placement {
		if node.isDelete && len(node.Indexes) == 0 {
			continue
		}
		result = append(result, node.clone())
	}

	s.Placement = result
}

//
// This function finds the indexer with matching nodeId.
//
func (s *Solution) findMatchingIndexer(id string) *IndexerNode {
	for _, indexer := range s.Placement {
		if indexer.NodeId == id {
			return indexer
		}
	}

	return nil
}

//
// Find node to be deleted
//
func (s *Solution) getDeleteNodes() []*IndexerNode {

	result := ([]*IndexerNode)(nil)
	for _, indexer := range s.Placement {
		if indexer.isDelete {
			result = append(result, indexer)
		}
	}

	return result
}

//
// This prints the vital statistics from Solution.
//
func (s *Solution) PrintStats() {

	numOfIndex := 0
	maxIndexSize := uint64(0)
	totalIndexSize := uint64(0)
	maxIndexerOverhead := uint64(0)
	totalIndexCpu := float64(0)
	maxIndexCpu := float64(0)
	avgIndexSize := uint64(0)
	avgIndexCpu := float64(0)

	for _, indexer := range s.Placement {
		numOfIndex += len(indexer.Indexes)

		overhead := indexer.GetMemOverhead(s.UseLiveData())
		if overhead > maxIndexerOverhead {
			maxIndexerOverhead = overhead
		}

		for _, index := range indexer.Indexes {
			totalIndexSize += index.GetMemUsage(s.UseLiveData())
			totalIndexCpu += index.GetCpuUsage(s.UseLiveData())

			if index.GetMemUsage(s.UseLiveData()) > maxIndexSize {
				maxIndexSize = index.GetMemUsage(s.UseLiveData())
			}

			if index.GetCpuUsage(s.UseLiveData()) > maxIndexCpu {
				maxIndexCpu = index.GetCpuUsage(s.UseLiveData())
			}
		}
	}

	if numOfIndex != 0 {
		avgIndexSize = totalIndexSize / uint64(numOfIndex)
		avgIndexCpu = totalIndexCpu / float64(numOfIndex)
	}

	logging.Infof("Number of indexes: %v", numOfIndex)
	logging.Infof("Number of indexers: %v", len(s.Placement))
	logging.Infof("Avg Index Size: %v (%s)", avgIndexSize, formatMemoryStr(uint64(avgIndexSize)))
	logging.Infof("Max Index Size: %v (%s)", uint64(maxIndexSize), formatMemoryStr(uint64(maxIndexSize)))
	logging.Infof("Max Indexer Overhead: %v (%s)", uint64(maxIndexerOverhead), formatMemoryStr(uint64(maxIndexerOverhead)))
	logging.Infof("Avg Index Cpu: %.4f", avgIndexCpu)
	logging.Infof("Max Index Cpu: %.4f", maxIndexCpu)
	logging.Infof("Num Estimation Run: %v", s.numEstimateRun)
}

//
// This prints out layout for the solution
//
func (s *Solution) PrintLayout() {

	for _, indexer := range s.Placement {

		logging.Infof("")
		logging.Infof("Indexer serverGroup:%v, nodeId:%v, nodeUUID:%v, useLiveData:%v", indexer.ServerGroup, indexer.NodeId, indexer.NodeUUID, s.UseLiveData())
		logging.Infof("Indexer total memory:%v (%s), mem:%v (%s), overhead:%v (%s), data:%v (%s) cpu:%.4f, numIndexes:%v isDeleted:%v isNew:%v exclude:%v",
			indexer.GetMemTotal(s.UseLiveData()), formatMemoryStr(uint64(indexer.GetMemTotal(s.UseLiveData()))),
			indexer.GetMemUsage(s.UseLiveData()), formatMemoryStr(uint64(indexer.GetMemUsage(s.UseLiveData()))),
			indexer.GetMemOverhead(s.UseLiveData()), formatMemoryStr(uint64(indexer.GetMemOverhead(s.UseLiveData()))),
			indexer.GetDataSize(s.UseLiveData()), formatMemoryStr(uint64(indexer.GetDataSize(s.UseLiveData()))),
			indexer.GetCpuUsage(s.UseLiveData()), len(indexer.Indexes), indexer.IsDeleted(), indexer.isNew, indexer.exclude)

		for _, index := range indexer.Indexes {
			logging.Infof("\t\t------------------------------------------------------------------------------------------------------------------")
			logging.Infof("\t\tIndex name:%v, bucket:%v, defnId:%v, instId:%v, Partition: %v, new/moved:%v equivCheck:%v pendingCreate:%v",
				index.GetDisplayName(), index.Bucket, index.DefnId, index.InstId, index.PartnId,
				index.initialNode == nil || index.initialNode.NodeId != indexer.NodeId, !index.suppressEquivIdxCheck,
				index.pendingCreate)
			logging.Infof("\t\tIndex total memory:%v (%s), mem:%v (%s), overhead:%v (%s), data:%v (%s) cpu:%.4f resident:%v%% build:%v%% estimated:%v",
				index.GetMemTotal(s.UseLiveData()), formatMemoryStr(uint64(index.GetMemTotal(s.UseLiveData()))),
				index.GetMemUsage(s.UseLiveData()), formatMemoryStr(uint64(index.GetMemUsage(s.UseLiveData()))),
				index.GetMemOverhead(s.UseLiveData()), formatMemoryStr(uint64(index.GetMemOverhead(s.UseLiveData()))),
				index.GetDataSize(s.UseLiveData()), formatMemoryStr(uint64(index.GetDataSize(s.UseLiveData()))),
				index.GetCpuUsage(s.UseLiveData()),
				uint64(index.GetResidentRatio(s.UseLiveData())),
				index.GetBuildPercent(s.UseLiveData()),
				index.NoUsageInfo)
		}
	}
}

//
// Compute statistics on memory usage
//
func (s *Solution) ComputeMemUsage() (float64, float64) {

	// Compute mean memory usage
	var meanMemUsage float64
	for _, indexerUsage := range s.Placement {
		meanMemUsage += float64(indexerUsage.GetMemTotal(s.UseLiveData()))
	}
	meanMemUsage = meanMemUsage / float64(len(s.Placement))

	// compute memory variance
	var varianceMemUsage float64
	for _, indexerUsage := range s.Placement {
		v := float64(indexerUsage.GetMemTotal(s.UseLiveData())) - meanMemUsage
		varianceMemUsage += v * v
	}
	varianceMemUsage = varianceMemUsage / float64(len(s.Placement))

	// compute memory std dev
	stdDevMemUsage := math.Sqrt(varianceMemUsage)

	return meanMemUsage, stdDevMemUsage
}

//
// Compute statistics on cpu usage
//
func (s *Solution) ComputeCpuUsage() (float64, float64) {

	// Compute mean cpu usage
	var meanCpuUsage float64
	for _, indexerUsage := range s.Placement {
		meanCpuUsage += float64(indexerUsage.GetCpuUsage(s.UseLiveData()))
	}
	meanCpuUsage = meanCpuUsage / float64(len(s.Placement))

	// compute cpu variance
	var varianceCpuUsage float64
	for _, indexerUsage := range s.Placement {
		v := float64(indexerUsage.GetCpuUsage(s.UseLiveData())) - meanCpuUsage
		varianceCpuUsage += v * v
	}
	varianceCpuUsage = varianceCpuUsage / float64(len(s.Placement))

	// compute cpu std dev
	stdDevCpuUsage := math.Sqrt(varianceCpuUsage)

	return meanCpuUsage, stdDevCpuUsage
}

//
// Compute statistics on number of index. This only consider
// index that has no stats or sizing information.
//
func (s *Solution) ComputeEmptyIndexDistribution() (float64, float64) {

	// Compute mean number of index
	var meanIdxUsage float64
	for _, indexer := range s.Placement {
		meanIdxUsage += float64(s.numEmptyIndex(indexer))
	}
	meanIdxUsage = meanIdxUsage / float64(len(s.Placement))

	// compute variance on number of index
	var varianceIdxUsage float64
	for _, indexer := range s.Placement {
		v := float64(s.numEmptyIndex(indexer)) - meanIdxUsage
		varianceIdxUsage += v * v
	}
	varianceIdxUsage = varianceIdxUsage / float64(len(s.Placement))

	// compute std dev on number of index
	stdDevIdxUsage := math.Sqrt(varianceIdxUsage)

	return meanIdxUsage, stdDevIdxUsage
}

//
// Compute statistics on data size
//
func (s *Solution) ComputeDataSize() (float64, float64) {

	// Compute mean data size
	var meanDataSize float64
	for _, indexerUsage := range s.Placement {
		meanDataSize += float64(indexerUsage.GetDataSize(s.UseLiveData()))
	}
	meanDataSize = meanDataSize / float64(len(s.Placement))

	// compute data size variance
	var varianceDataSize float64
	for _, indexerUsage := range s.Placement {
		v := float64(indexerUsage.GetDataSize(s.UseLiveData())) - meanDataSize
		varianceDataSize += v * v
	}
	varianceDataSize = varianceDataSize / float64(len(s.Placement))

	// compute data size std dev
	stdDevDataSize := math.Sqrt(varianceDataSize)

	return meanDataSize, stdDevDataSize
}

//
// Find the number of indexes that has no stats or sizing information.
// This does not take into consideration for index fixed overhead.
//
func (s *Solution) numEmptyIndex(indexer *IndexerNode) int {

	//TODO
	count := 0
	for _, index := range indexer.Indexes {
		if index.GetMemUsage(s.UseLiveData()) == 0 {
			count++
		}
	}

	return count
}

//
// Compute statistics on index movement
//
func (s *Solution) computeIndexMovement(useNewNode bool) (uint64, uint64, uint64, uint64) {

	totalSize := uint64(0)
	dataMoved := uint64(0)
	totalIndex := uint64(0)
	indexMoved := uint64(0)

	for _, indexer := range s.Placement {

		// ignore cost moving to a new node
		if !useNewNode && indexer.isNew {
			continue
		}

		//TODO
		for _, index := range indexer.Indexes {

			// ignore cost of moving an index out of an to-be-deleted node
			if index.initialNode != nil && !index.initialNode.isDelete {
				totalSize += index.GetDataSize(s.UseLiveData())
				totalIndex++
			}

			// ignore cost of moving an index out of an to-be-deleted node
			if index.initialNode != nil && !index.initialNode.isDelete &&
				index.initialNode.NodeId != indexer.NodeId {
				dataMoved += index.GetDataSize(s.UseLiveData())
				indexMoved++
			}
		}
	}

	return totalSize, dataMoved, totalIndex, indexMoved
}

//
// Compute indexer free ratio
//
func (s *Solution) computeFreeRatio() (float64, float64) {

	cpuTotal := float64(0)
	memTotal := float64(0)
	count := uint64(0)

	for _, indexer := range s.Placement {

		cpu := (float64(s.constraint.GetCpuQuota()) - float64(indexer.GetCpuUsage(s.UseLiveData()))) / float64(s.constraint.GetCpuQuota())
		mem := (float64(s.constraint.GetMemQuota()) - float64(indexer.GetMemTotal(s.UseLiveData()))) / float64(s.constraint.GetMemQuota())

		if cpu > 0 {
			cpuTotal += cpu
		}

		if mem > 0 {
			memTotal += mem
		}

		count++
	}

	if count > 0 {
		return (memTotal / float64(count)), (cpuTotal / float64(count))
	} else {
		return 0, 0
	}
}

//
// Find the number of replica or equivalent index (including itself).
//
func (s *Solution) findNumEquivalentIndex(u *IndexUsage) int {

	var count int

	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {

			// check replica
			if index.IsReplica(u) || index.IsEquivalentIndex(u, false) {
				count++
			}
		}
	}

	return count
}

//
// Find the number of replica (including itself).
//
func (s *Solution) findNumReplica(u *IndexUsage) int {

	count := 0
	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {

			// check replica
			if index.IsReplica(u) {
				count++
			}
		}
	}

	return count
}

//
// Find the missing replica.  Return a list of replicaId
//
func (s *Solution) findMissingReplica(u *IndexUsage) map[int]common.IndexInstId {

	found := make(map[int]common.IndexInstId)
	instances := make(map[common.IndexInstId]bool)
	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {

			// check replica (including self)
			if index.IsReplica(u) {
				if index.Instance == nil {
					logging.Warnf("Cannot determinte replicaId for index (%v,%v)", index.Name, index.Bucket)
					return (map[int]common.IndexInstId)(nil)
				}
				found[index.Instance.ReplicaId] = index.InstId
			}

			if index.IsSameIndex(u) {
				instances[index.InstId] = true
			}
		}
	}

	// replicaId starts with 0
	// numReplica excludes itself
	missing := make(map[int]common.IndexInstId)
	if u.Instance != nil {
		for i := 0; i < int(u.Instance.Defn.NumReplica+1); i++ {
			if _, ok := found[i]; !ok {
				missing[i] = 0

				// Replica is missing.  Found out which instance with the missing replica.
				for instId, _ := range instances {

					match := false
					for _, instId2 := range found {
						if instId2 == instId {
							match = true
							break
						}
					}

					// There is an index inst with no matching replica
					if !match {
						missing[i] = instId
						break
					}
				}
			}
		}
	}

	return missing
}

//
// Find the number of partition (including itself).
//
func (s *Solution) findNumPartition(u *IndexUsage) int {

	count := 0
	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {
			if index.IsSameInst(u) {
				count++
			}
		}
	}

	return count
}

//
// Find the missing partition
//
func (s *Solution) findMissingPartition(u *IndexUsage) []common.PartitionId {

	if u.Instance == nil || u.Instance.Pc == nil {
		return []common.PartitionId(nil)
	}

	found := make(map[common.PartitionId]bool)
	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {

			// check instance (including self)
			if index.IsSameInst(u) {
				found[index.PartnId] = true
			}
		}
	}

	startPartnId := 0
	if common.IsPartitioned(u.Instance.Defn.PartitionScheme) {
		startPartnId = 1
	}
	endPartnId := startPartnId + int(u.Instance.Pc.GetNumPartitions())

	var missing []common.PartitionId
	for i := startPartnId; i < endPartnId; i++ {
		if _, ok := found[common.PartitionId(i)]; !ok {
			missing = append(missing, common.PartitionId(i))
		}
	}

	return missing
}

//
// Find the number of server group.   If a
// server group consists of only ejected node,
// this server group will be skipped.
//
func (s *Solution) findNumServerGroup() int {

	groups := make(map[string]bool)
	for _, indexer := range s.Placement {
		if indexer.isDelete {
			continue
		}

		if _, ok := groups[indexer.ServerGroup]; !ok {
			groups[indexer.ServerGroup] = true
		}
	}

	return len(groups)
}

//
// This function recalculates the index and indexer sizes baesd on sizing formula.
// Data captured from live cluser will not be overwritten.
//
func (s *Solution) calculateSize() {

	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {
			s.sizing.ComputeIndexSize(index)
		}
	}

	for _, indexer := range s.Placement {
		s.sizing.ComputeIndexerSize(indexer)
	}
}

//
// is this a MOI Cluster?
//
func (s *Solution) isMOICluster() bool {

	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {
			if index.IsMOI() {
				return true
			}
		}
	}

	return false
}

//
// Find num of deleted node
//
func (s *Solution) findNumDeleteNodes() int {

	count := 0
	for _, indexer := range s.Placement {
		if indexer.isDelete {
			count++
		}
	}

	return count
}

//
// Find num of excludeIn node.
//
func (s *Solution) findNumExcludeInNodes() int {

	count := 0
	for _, indexer := range s.Placement {
		if indexer.ExcludeIn(s) {
			count++
		}
	}

	return count
}

//
// Find num of emptpy node
//
func (s *Solution) findNumEmptyNodes() int {

	count := 0
	for _, indexer := range s.Placement {
		if len(indexer.Indexes) == 0 && !indexer.isDelete {
			count++
		}
	}

	return count
}

//
// find number of live node (excluding ejected node)
//
func (s *Solution) findNumLiveNode() int {

	return len(s.Placement) - s.findNumDeleteNodes()
}

//
// find number of availabe live node (excluding ejected node and excluded node)
//
func (s *Solution) findNumAvailLiveNode() int {

	count := 0
	for _, indexer := range s.Placement {
		if !indexer.ExcludeIn(s) && !indexer.IsDeleted() {
			count++
		}
	}

	return count
}

//
// Eavluate if each indexer meets constraint
//
func (s *Solution) evaluateNodeConstraint() {

	for _, indexer := range s.Placement {
		indexer.EvaluateNodeConstraint(s)
	}
}

//
// check to see if we should ignore resource (memory/cpu) constraint
//
func (s *Solution) ignoreResourceConstraint() bool {

	// always honor resource constriant when doing simulation
	if !s.UseLiveData() {
		return false
	}

	// ignore resource constraint for plasma or fdb during rebalance
	// for plasma and forestdb, memory and cpu represents transient working set and this can change over time.
	// for MOI, memory consumption is not transient so constraint needs to be honored.
	if s.command == CommandRebalance || s.command == CommandSwap {
		// planner tends to even out memory, cpu and data usage across nodes based on cost function.
		// so ignore resource constraint assuming the resources will even out
		//return !s.isMOICluster()
		return true
	}

	// ignore constraint for plasma and fdb during planning since memory and cpu are transient working set.
	// in addition, sizing equation is only approximation.   The cost function will ensure that the resources
	// are evenly spread out, so it is ok to go over resource constraint during planning.
	//
	// for MOI, memory is not transient, so it needs to be honored.
	//
	// if size estimation is turned on, then honor the constraint since it needs to find the best fit under the
	// assumption that new indexes are going to fit under constraint.
	if s.command == CommandPlan {
		if s.canRunEstimation() {
			return false
		}
		// planner tends to even out memory, cpu and data usage across nodes based on cost function.
		// so ignore resource constraint assuming the resources will even out
		//return !s.isMOICluster()
		return true
	}

	return true
}

//
// check to see if every node in the cluster meet constraint
//
func (s *Solution) SatisfyClusterConstraint() bool {

	for _, indexer := range s.Placement {
		if !indexer.meetConstraint {
			return false
		}
	}

	return true
}

//
// Check if there is any replica (excluding serlf) in the server group
//
func (s *Solution) hasReplicaInServerGroup(u *IndexUsage, group string) bool {

	for _, indexer := range s.Placement {
		if indexer.isDelete {
			continue
		}
		for _, index := range indexer.Indexes {
			if index != u && index.IsReplica(u) { // replica
				if group == indexer.ServerGroup {
					return true
				}
			}
		}
	}

	return false
}

//
// Check if any server group without this replica
//
func (s *Solution) hasServerGroupWithNoReplica(u *IndexUsage) bool {

	counts := make(map[string]int)

	for _, indexer := range s.Placement {
		if indexer.isDelete {
			continue
		}

		if _, ok := counts[indexer.ServerGroup]; !ok {
			counts[indexer.ServerGroup] = 0
		}

		for _, index := range indexer.Indexes {
			if index != u && index.IsReplica(u) { // replica
				counts[indexer.ServerGroup] = counts[indexer.ServerGroup] + 1
			}
		}
	}

	for _, count := range counts {
		if count == 0 {
			return true
		}
	}

	return false
}

//
// Does the index node has replia?
//
func (s *Solution) hasReplica(indexer *IndexerNode, target *IndexUsage) bool {

	for _, index := range indexer.Indexes {
		if index != target && index.IsReplica(target) {
			return true
		}
	}

	return false
}

//
// Find the indexer node that contains the replica
//
func (s *Solution) FindIndexerWithReplica(name string, bucket string, partnId common.PartitionId, replicaId int) *IndexerNode {

	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {
			if index.Name == name &&
				index.Bucket == bucket &&
				index.PartnId == partnId &&
				index.Instance != nil &&
				index.Instance.ReplicaId == replicaId {
				return indexer
			}
		}
	}

	return nil
}

//
// Find the indexer node that does not contain the replica
// This ignores any indexer that is deleted or cannot rebalance
//
func (s *Solution) FindIndexerWithNoReplica(source *IndexUsage) *IndexerNode {

	for _, indexer := range s.Placement {
		if indexer.IsDeleted() || indexer.ExcludeAny(s) {
			continue
		}

		found := false
		for _, index := range indexer.Indexes {
			if source.IsReplica(index) {
				found = true
				break
			}
		}

		if !found {
			return indexer
		}
	}

	return nil
}

//
// Mark the node with no indexes as "new" node
//
func (s *Solution) markNewNodes() {

	for _, indexer := range s.Placement {
		if !indexer.isDelete && len(indexer.Indexes) == 0 {
			indexer.isNew = true
		}
	}
}

//
// Does cluster has new node?
//
func (s *Solution) hasNewNodes() bool {

	for _, indexer := range s.Placement {
		if !indexer.isDelete && indexer.isNew {
			return true
		}
	}
	return false
}

//
// Does cluster has deleted node?
//
func (s *Solution) hasDeletedNodes() bool {

	for _, indexer := range s.Placement {
		if indexer.isDelete {
			return true
		}
	}
	return false
}

//
// NoUsageInfo index is index without any sizing information.
// For placement, planner still want to layout noUsage index such that
// 1) indexer with more free memory will take up more partitions.
// 2) partitions should be spread out as much as possible
// 3) non-partiton index have different weight than individual partition of a partitioned index
//
// It is impossible to achieve accurate number for (1) and (3) without sizing info (even with sizing,
// it is only an approximation).    But if we make 2 assumptions, we can provide estimated sizing of partitions:
// 1) the new index will take up the remaining free memory of all indexers
// 2) the partiton of new index are equal size
//
// To estimate NoUsageInfo index size based on free memory available.
// 1) calculates total free memory from indexers which has less than 80% memory usage
// 2) Based on total free memory, estimates index size based on number of noUsage index.
// 3) Estimate the partiton size
//    - non-partitioned index has 1 partition, so it will take up full index size
//    - individual partition of a partitioned index will take up (index-size / numPartition)
// 4) Given the estimated size, if cannot find a placement solution, reduce index size by 10% and repeat (4)
//
// Behavior:
// 1) Using ConstraintMethod, indexes/Partitions will more likely to move to indexers with more free memory
// 2) Using CostMethod, partitions will likely to spread out to minimize memory variation
//
// Caveats:
// 1) When placing the new index onto a cluster with existing NoUsageInfo (deferred) index, the existing
//    index will also need to be sized.   The existing index is assumed to have the similar size as
//    the new index.   Therefore, the estimated index size represents the "average index size" for
//    all unsized indexes.
//
func (s *Solution) runSizeEstimation(placement PlacementMethod) {

	estimate := func(estimatedIndexSize uint64) {
		for _, indexer := range s.Placement {

			// excludeIn - indexes cannot moved into this node.  So effectively, this indexer is taken out of cluster.
			if indexer.ExcludeIn(s) {
				continue
			}

			for _, index := range indexer.Indexes {
				if index.NoUsageInfo {
					if index.Instance != nil && index.Instance.Pc != nil && common.IsPartitioned(index.Instance.Defn.PartitionScheme) {
						index.EstimatedMemUsage = estimatedIndexSize / uint64(index.Instance.Pc.GetNumPartitions())
						index.EstimatedDataSize = index.EstimatedMemUsage
					} else {
						index.EstimatedMemUsage = estimatedIndexSize
						index.EstimatedDataSize = index.EstimatedMemUsage
					}
					indexer.AddMemUsageOverhead(s, index.EstimatedMemUsage, 0)
					indexer.AddDataSize(s, index.EstimatedDataSize)
				}
			}
		}
	}

	// nothing to do if size estimation is turned off
	if !s.estimate {
		return
	}

	// only estimate size for planning
	if s.command != CommandPlan {
		s.estimationOff()
		return
	}

	// only enable estimation if eligible indexes have no sizing info
	eligibles := placement.GetEligibleIndexes()
	for eligible, _ := range eligibles {
		if !eligible.NoUsageInfo {
			s.estimationOff()
			return
		}
	}

	s.numEstimateRun++

	// cleanup
	s.cleanupEstimation()

	// if there is a previous calculation, use it
	if s.estimatedIndexSize != 0 {
		// if previous sizing is available, then use it by sizing it down.
		s.estimatedIndexSize = uint64(float64(s.estimatedIndexSize) * 0.9)
		if s.estimatedIndexSize > 0 {
			estimate(s.estimatedIndexSize)
		} else {
			s.estimationOff()
		}
		return
	}

	//
	// Calculate the initial estimated index size
	//

	//
	// count the number of indexes for estimation
	//
	insts := make(map[common.IndexInstId]*common.IndexInst)
	for _, indexer := range s.Placement {

		// eligible index cannot be placed in an indexer that has been excluded (in or out)
		// excludeIn - indexes cannot moved into this node.  So effectively, this indexer is taken out of cluster.
		if indexer.ExcludeIn(s) {
			continue
		}

		// count NoUsageInfo index.  This covers new index as well as existing index.
		for _, index := range indexer.Indexes {
			if index.NoUsageInfo {
				insts[index.InstId] = index.Instance
			}
		}
	}

	//
	// calculate total free memory
	//
	threshold := float64(0.2)

retry1:
	var indexers []*IndexerNode
	totalMemFree := uint64(0)
	maxMemFree := uint64(0)

	for _, indexer := range s.Placement {
		// excludeIn - indexes cannot moved into this node.  So effectively, this indexer is taken out of cluster.
		if indexer.ExcludeIn(s) {
			continue
		}

		// Do not use Cpu for estimation for now since cpu measurement is fluctuating
		freeMem := float64(s.constraint.GetMemQuota()) - float64(indexer.GetMemTotal(s.UseLiveData()))
		freeMemRatio := freeMem / float64(s.constraint.GetMemQuota())
		if freeMem > 0 && freeMemRatio > threshold {
			// freeMem is a positive number
			adjFreeMem := uint64(freeMem * 0.8)
			totalMemFree += adjFreeMem
			if adjFreeMem > maxMemFree {
				maxMemFree = adjFreeMem
			}
			indexers = append(indexers, indexer)
		}
	}

	// If there is no indexer, then retry with lower free Ratio
	if len(indexers) == 0 && threshold > 0.05 {
		threshold -= 0.05
		goto retry1
	}

	// not enough indexers with free memory. Do not estimate sizing.
	if len(indexers) == 0 {
		s.estimationOff()
		return
	}

	//
	// Estimate index size
	//
	if len(insts) > 0 {

		// Compute initial slot size based on total free memory.
		// The slot size is the "average" index size.
		s.estimatedIndexSize = totalMemFree / uint64(len(insts))

	retry2:
		for s.estimatedIndexSize > 0 {
			// estimate noUsage index size
			s.cleanupEstimation()
			estimate(s.estimatedIndexSize)

			// adjust slot size
			for index, _ := range eligibles {
				// cannot be higher than max free mem size
				if index.EstimatedMemUsage > maxMemFree {
					s.estimatedIndexSize = uint64(float64(s.estimatedIndexSize) * 0.9)
					goto retry2
				}
			}

			return
		}

		s.estimationOff()
		logging.Infof("Planner.runSizeEstimation: cannot estimate deferred index size.  Will not set estimatedMemUsage in deferred index")
	}
}

//
// Turn off estimation
//
func (s *Solution) estimationOff() {
	s.cleanupEstimation()
	s.estimatedIndexSize = 0
	s.estimate = false
}

//
// Turn on estimation
//
func (s *Solution) estimationOn() {
	s.estimate = true
}

//
// Clean up all calculation based on estimation
//
func (s *Solution) cleanupEstimation() {
	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {
			if index.NoUsageInfo {
				indexer.SubtractMemUsageOverhead(s, index.EstimatedMemUsage, 0)
				indexer.SubtractDataSize(s, index.EstimatedDataSize)
				index.EstimatedMemUsage = 0
				index.EstimatedDataSize = 0
			}
		}
	}
}

//
// Copy the estimation
//
func (s *Solution) copyEstimationFrom(source *Solution) {
	s.estimatedIndexSize = source.estimatedIndexSize
	s.estimate = source.estimate
	s.numEstimateRun = source.numEstimateRun
}

//
// Can run estimation?
//
func (s *Solution) canRunEstimation() bool {
	return s.estimate
}

//////////////////////////////////////////////////////////////
// IndexerConstraint
//////////////////////////////////////////////////////////////

//
// Constructor
//
func newIndexerConstraint(memQuota uint64,
	cpuQuota uint64,
	canResize bool,
	maxNumNode int,
	maxCpuUse int,
	maxMemUse int) *IndexerConstraint {
	return &IndexerConstraint{
		MemQuota:   memQuota,
		CpuQuota:   cpuQuota,
		canResize:  canResize,
		maxNumNode: uint64(maxNumNode),
		MaxCpuUse:  int64(maxCpuUse),
		MaxMemUse:  int64(maxMemUse),
	}
}

//
// Print quota
//
func (c *IndexerConstraint) Print() {
	logging.Infof("Memory Quota %v (%s)", c.MemQuota, formatMemoryStr(c.MemQuota))
	logging.Infof("CPU Quota %v", c.CpuQuota)
	logging.Infof("Max Cpu Utilization %v", c.MaxCpuUse)
	logging.Infof("Max Memory Utilization %v", c.MaxMemUse)
}

//
// Validate the solution
//
func (c *IndexerConstraint) Validate(s *Solution) error {

	if c.CanAddNode(s) {
		return nil
	}

	if s.ignoreResourceConstraint() {
		return nil
	}

	if s.canRunEstimation() {
		return nil
	}

	var totalIndexMem uint64
	var totalIndexCpu float64

	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {
			totalIndexMem += index.GetMemTotal(s.UseLiveData())
			totalIndexCpu += index.GetCpuUsage(s.UseLiveData())
		}
	}

	if totalIndexMem > (c.MemQuota * uint64(s.findNumLiveNode())) {
		return errors.New(fmt.Sprintf("Total memory usage of all indexes (%v) exceed aggregated memory quota of all indexer nodes (%v)",
			totalIndexMem, (c.MemQuota * uint64(s.findNumLiveNode()))))
	}

	/*
		if totalIndexCpu > float64(c.CpuQuota*uint64(s.findNumLiveNode())) {
			return errors.New(fmt.Sprintf("Total cpu usage of all indexes (%v) exceed aggregated cpu quota of all indexer nodes (%v)",
				totalIndexCpu, c.CpuQuota*uint64(s.findNumLiveNode())))
		}
	*/

	return nil
}

//
// Return an error with a list of violations
//
func (c *IndexerConstraint) GetViolations(s *Solution, eligibles map[*IndexUsage]bool) *Violations {

	violations := &Violations{
		MemQuota: s.getConstraintMethod().GetMemQuota(),
		CpuQuota: s.getConstraintMethod().GetCpuQuota(),
	}

	for _, indexer := range s.Placement {

		// This indexer node does not satisfy constraint
		if !c.SatisfyNodeConstraint(s, indexer, eligibles) {
			for _, index := range indexer.Indexes {
				if isEligibleIndex(index, eligibles) {

					if !c.acceptViolation(s, index, indexer) {
						continue
					}

					violation := &Violation{
						Name:     index.GetDisplayName(),
						Bucket:   index.Bucket,
						NodeId:   indexer.NodeId,
						MemUsage: index.GetMemTotal(s.UseLiveData()),
						CpuUsage: index.GetCpuUsage(s.UseLiveData()),
						Details:  nil}

					// If this indexer node has a placeable index, then check if the
					// index can be moved to other nodes.
					for _, indexer2 := range s.Placement {
						if indexer.NodeId == indexer2.NodeId {
							continue
						}

						if indexer2.isDelete {
							continue
						}

						if code := c.CanAddIndex(s, indexer2, index); code != NoViolation {
							freeMem, freeCpu := indexer2.freeUsage(s, s.getConstraintMethod())
							err := fmt.Sprintf("Cannot move to %v: %v (free mem %v, free cpu %v)",
								indexer2.NodeId, code, formatMemoryStr(freeMem), freeCpu)
							violation.Details = append(violation.Details, err)
						} else {
							freeMem, freeCpu := indexer2.freeUsage(s, s.getConstraintMethod())
							err := fmt.Sprintf("Can move to %v: %v (free mem %v, free cpu %v)",
								indexer2.NodeId, code, formatMemoryStr(freeMem), freeCpu)
							violation.Details = append(violation.Details, err)
						}
					}

					violations.Violations = append(violations.Violations, violation)
				}
			}
		}
	}

	if violations.IsEmpty() {
		return nil
	}

	return violations
}

//
// Is this a violation?
//
func (c *IndexerConstraint) acceptViolation(s *Solution, index *IndexUsage, indexer *IndexerNode) bool {

	if s.getConstraintMethod().CanAddNode(s) {
		return true
	}

	numReplica := s.findNumReplica(index)

	if s.UseLiveData() && numReplica > s.findNumLiveNode() {
		return false
	}

	// if cannot load balance, don't report error.
	if index.initialNode != nil && index.initialNode.NodeId == indexer.NodeId && !indexer.IsDeleted() {
		return false
	}

	return true
}

//
// Get memory quota
//
func (c *IndexerConstraint) GetMemQuota() uint64 {
	return c.MemQuota
}

//
// Get cpu quota
//
func (c *IndexerConstraint) GetCpuQuota() uint64 {
	return c.CpuQuota
}

//
// Allow Add Node
//
func (c *IndexerConstraint) CanAddNode(s *Solution) bool {
	return c.canResize && len(s.Placement) < int(c.maxNumNode)
}

//
// Check replica server group
//
func (c *IndexerConstraint) SatisfyServerGroupConstraint(s *Solution, u *IndexUsage, group string) bool {

	// More than 1 server group?
	if s.numServerGroup <= 1 {
		return true
	}

	// If there is no replica (excluding self) in the server group.
	hasReplicaInServerGroup := s.hasReplicaInServerGroup(u, group)
	if !hasReplicaInServerGroup {
		// no replica in this server group
		return true
	}

	// There are replica in this server group. Check to see if there are any server group without this index.
	hasServerGroupWithNoReplica := s.hasServerGroupWithNoReplica(u)
	if !hasServerGroupWithNoReplica {
		// every server group has a replica of this index
		return true
	}

	// There is replica in this server group and there is other server group without this replica.
	return false
}

//
// This function determines if an index can be placed into the given node,
// while satsifying availability and resource constraint.
//
func (c *IndexerConstraint) CanAddIndex(s *Solution, n *IndexerNode, u *IndexUsage) ViolationCode {

	if n.ExcludeIn(s) {
		return ExcludeNodeViolation
	}

	if n.isDelete {
		return DeleteNodeViolation
	}

	//TODO
	for _, index := range n.Indexes {
		// check replica
		if index.IsReplica(u) {
			return ReplicaViolation
		}

		// check equivalent index
		if index.IsEquivalentIndex(u, true) {
			return EquivIndexViolation
		}
	}

	// Are replica in the same server group?
	if !c.SatisfyServerGroupConstraint(s, u, n.ServerGroup) {
		return ServerGroupViolation
	}

	if s.ignoreResourceConstraint() {
		return NoViolation
	}

	memQuota := c.MemQuota
	cpuQuota := float64(c.CpuQuota)

	if c.MaxMemUse != -1 {
		memQuota = memQuota * uint64(c.MaxMemUse) / 100
	}

	if c.MaxCpuUse != -1 {
		cpuQuota = cpuQuota * float64(c.MaxCpuUse) / 100
	}

	if u.GetMemTotal(s.UseLiveData())+n.GetMemTotal(s.UseLiveData()) > memQuota {
		return MemoryViolation
	}

	/*
		if u.GetCpuUsage(s.UseLiveData())+n.GetCpuUsage(s.UseLiveData()) > cpuQuota {
			return CpuViolation
		}
	*/

	return NoViolation
}

//
// This function determines if an index can be swapped with another index in the given node,
// while satsifying availability and resource constraint.
//
func (c *IndexerConstraint) CanSwapIndex(sol *Solution, n *IndexerNode, s *IndexUsage, t *IndexUsage) ViolationCode {

	if n.ExcludeIn(sol) {
		return ExcludeNodeViolation
	}

	if n.isDelete {
		return DeleteNodeViolation
	}

	//TODO
	for _, index := range n.Indexes {
		// check replica
		if index.IsReplica(s) {
			return ReplicaViolation
		}

		// check equivalent index
		if index.IsEquivalentIndex(s, true) {
			return EquivIndexViolation
		}
	}

	// Are replica in the same server group?
	if !c.SatisfyServerGroupConstraint(sol, s, n.ServerGroup) {
		return ServerGroupViolation
	}

	if sol.ignoreResourceConstraint() {
		return NoViolation
	}

	memQuota := c.MemQuota
	cpuQuota := float64(c.CpuQuota)

	if c.MaxMemUse != -1 {
		memQuota = memQuota * uint64(c.MaxMemUse) / 100
	}

	if c.MaxCpuUse != -1 {
		cpuQuota = cpuQuota * float64(c.MaxCpuUse) / 100
	}

	if s.GetMemTotal(sol.UseLiveData())+n.GetMemTotal(sol.UseLiveData())-t.GetMemTotal(sol.UseLiveData()) > memQuota {
		return MemoryViolation
	}

	/*
		if s.GetCpuUsage(sol.UseLiveData())+n.GetCpuUsage(sol.UseLiveData())-t.GetCpuUsage(sol.UseLiveData()) > cpuQuota {
			return CpuViolation
		}
	*/

	return NoViolation
}

//
// This function determines if a node constraint is satisfied.
//
func (c *IndexerConstraint) SatisfyNodeResourceConstraint(s *Solution, n *IndexerNode) bool {

	if s.ignoreResourceConstraint() {
		return true
	}

	memQuota := c.MemQuota
	cpuQuota := float64(c.CpuQuota)

	if c.MaxMemUse != -1 {
		memQuota = memQuota * uint64(c.MaxMemUse) / 100
	}

	if c.MaxCpuUse != -1 {
		cpuQuota = cpuQuota * float64(c.MaxCpuUse) / 100
	}

	if n.GetMemTotal(s.UseLiveData()) > memQuota {
		return false
	}

	/*
		if n.GetCpuUsage(s.UseLiveData()) > cpuQuota {
			return false
		}
	*/

	return true
}

//
// This function determines if a node HA constraint is satisfied.
//
func (c *IndexerConstraint) SatisfyNodeHAConstraint(s *Solution, n *IndexerNode, eligibles map[*IndexUsage]bool) bool {

	for offset, index := range n.Indexes {
		if !c.SatisfyIndexHAConstraintAt(s, n, offset+1, index, eligibles) {
			return false
		}
	}

	return true
}

//
// This function determines if a HA constraint is satisfied for a particular index in indexer node.
//
func (c *IndexerConstraint) SatisfyIndexHAConstraint(s *Solution, n *IndexerNode, source *IndexUsage, eligibles map[*IndexUsage]bool) bool {

	return c.SatisfyIndexHAConstraintAt(s, n, 0, source, eligibles)
}

func (c *IndexerConstraint) SatisfyIndexHAConstraintAt(s *Solution, n *IndexerNode, offset int, source *IndexUsage, eligibles map[*IndexUsage]bool) bool {

	if n.isDelete {
		return false
	}

	for i := offset; i < len(n.Indexes); i++ {
		index := n.Indexes[i]

		if index == source {
			continue
		}

		// Ignore any pair of indexes that are not eligible index
		if !isEligibleIndex(index, eligibles) && !isEligibleIndex(source, eligibles) {
			continue
		}

		// check replica
		if index.IsReplica(source) {
			return false
		}

		// check equivalent index
		if index.IsEquivalentIndex(source, true) {
			return false
		}
	}

	// Are replica in the same server group?
	if isEligibleIndex(source, eligibles) && !c.SatisfyServerGroupConstraint(s, source, n.ServerGroup) {
		return false
	}

	return true
}

//
// This function determines if cluster wide constraint is satisifed.
//
func (c *IndexerConstraint) SatisfyClusterResourceConstraint(s *Solution) bool {

	if s.ignoreResourceConstraint() {
		return true
	}

	memQuota := c.MemQuota
	cpuQuota := float64(c.CpuQuota)

	if c.MaxMemUse != -1 {
		memQuota = memQuota * uint64(c.MaxMemUse) / 100
	}

	if c.MaxCpuUse != -1 {
		cpuQuota = cpuQuota * float64(c.MaxCpuUse) / 100
	}

	for _, indexer := range s.Placement {
		if indexer.GetMemTotal(s.UseLiveData()) > memQuota {
			return false
		}
		/*
			if indexer.GetCpuUsage(s.UseLiveData()) > cpuQuota {
				return false
			}
		*/
	}

	return true
}

//
// This function determines if a node constraint is satisfied.
//
func (c *IndexerConstraint) SatisfyNodeConstraint(s *Solution, n *IndexerNode, eligibles map[*IndexUsage]bool) bool {

	if n.isDelete && len(n.Indexes) != 0 {
		return false
	}

	checkConstraint := false
	for _, index := range n.Indexes {
		if isEligibleIndex(index, eligibles) {
			checkConstraint = true
			break
		}
	}

	if !checkConstraint {
		return true
	}

	if !c.SatisfyNodeResourceConstraint(s, n) {
		return false
	}

	return c.SatisfyNodeHAConstraint(s, n, eligibles)
}

//
// This function determines if cluster wide constraint is satisifed.
//
func (c *IndexerConstraint) SatisfyClusterConstraint(s *Solution, eligibles map[*IndexUsage]bool) bool {

	for _, indexer := range s.Placement {
		if !c.SatisfyNodeConstraint(s, indexer, eligibles) {
			return false
		}
	}

	return true
}

//////////////////////////////////////////////////////////////
// IndexerNode
//////////////////////////////////////////////////////////////

//
// This function creates a new indexer node
//
func newIndexerNode(nodeId string, sizing SizingMethod) *IndexerNode {

	r := &IndexerNode{
		NodeId:   nodeId,
		NodeUUID: "tempNodeUUID_" + nodeId,
	}

	sizing.ComputeIndexerSize(r)

	return r
}

//
// This function creates a new indexer node.  This function expects that each index is already
// "sized".   If sizing method is provided, it will compute sizing for indexer as well.
//
func CreateIndexerNodeWithIndexes(nodeId string, sizing SizingMethod, indexes []*IndexUsage) *IndexerNode {

	r := &IndexerNode{
		NodeId:   nodeId,
		NodeUUID: "tempNodeUUID_" + nodeId,
		Indexes:  indexes,
	}

	for _, index := range indexes {
		index.initialNode = r
	}

	if sizing != nil {
		sizing.ComputeIndexerSize(r)
	}

	return r
}

//
// Mark the node as deleted
//
func (o *IndexerNode) MarkDeleted() {

	o.isDelete = true
}

//
// Is indexer deleted?
//
func (o *IndexerNode) IsDeleted() bool {
	return o.isDelete
}

//
// Get a list of index usages that are moved to this node
//
func (o *IndexerNode) GetMovedIndex() []*IndexUsage {

	result := ([]*IndexUsage)(nil)
	for _, index := range o.Indexes {
		if index.initialNode == nil || index.initialNode.NodeId != o.NodeId {
			result = append(result, index)
		}
	}

	return result
}

//
// This function makes a copy of a indexer node.
//
func (o *IndexerNode) clone() *IndexerNode {

	r := &IndexerNode{
		NodeId:            o.NodeId,
		NodeUUID:          o.NodeUUID,
		IndexerId:         o.IndexerId,
		RestUrl:           o.RestUrl,
		ServerGroup:       o.ServerGroup,
		StorageMode:       o.StorageMode,
		MemUsage:          o.MemUsage,
		MemOverhead:       o.MemOverhead,
		DataSize:          o.DataSize,
		CpuUsage:          o.CpuUsage,
		DiskUsage:         o.DiskUsage,
		Indexes:           make([]*IndexUsage, len(o.Indexes)),
		isDelete:          o.isDelete,
		isNew:             o.isNew,
		exclude:           o.exclude,
		ActualMemUsage:    o.ActualMemUsage,
		ActualMemOverhead: o.ActualMemOverhead,
		ActualCpuUsage:    o.ActualCpuUsage,
		ActualDataSize:    o.ActualDataSize,
		meetConstraint:    o.meetConstraint,
	}

	for i, _ := range o.Indexes {
		r.Indexes[i] = o.Indexes[i]
	}

	return r
}

//
// This function returns a string representing the indexer
//
func (o *IndexerNode) String() string {
	return o.NodeId
}

//
// Get the free memory and cpu usage of this node
//
func (o *IndexerNode) freeUsage(s *Solution, constraint ConstraintMethod) (uint64, float64) {

	freeMem := constraint.GetMemQuota() - o.GetMemTotal(s.UseLiveData())
	freeCpu := float64(constraint.GetCpuQuota()) - o.GetCpuUsage(s.UseLiveData())

	return freeMem, freeCpu
}

//
// Get cpu usage
//
func (o *IndexerNode) GetCpuUsage(useLive bool) float64 {

	if useLive {
		return o.ActualCpuUsage
	}

	return o.CpuUsage
}

//
// Add Cpu
//
func (o *IndexerNode) AddCpuUsage(s *Solution, usage float64) {

	if !s.UseLiveData() {
		o.CpuUsage += usage
	} else {
		o.ActualCpuUsage += usage
	}
}

//
// Subtract Cpu
//
func (o *IndexerNode) SubtractCpuUsage(s *Solution, usage float64) {

	if !s.UseLiveData() {
		o.CpuUsage -= usage
	} else {
		o.ActualCpuUsage -= usage
	}
}

//
// Get memory usage
//
func (o *IndexerNode) GetMemUsage(useLive bool) uint64 {

	if useLive {
		return o.ActualMemUsage
	}

	return o.MemUsage
}

//
// Get memory overhead
//
func (o *IndexerNode) GetMemOverhead(useLive bool) uint64 {

	if useLive {
		return o.ActualMemOverhead
	}

	return o.MemOverhead
}

//
// Get memory total
//
func (o *IndexerNode) GetMemTotal(useLive bool) uint64 {

	if useLive {
		return o.ActualMemUsage + o.ActualMemOverhead
	}

	return o.MemUsage + o.MemOverhead
}

//
// Add memory
//
func (o *IndexerNode) AddMemUsageOverhead(s *Solution, usage uint64, overhead uint64) {

	if s.UseLiveData() {
		o.ActualMemUsage += usage
		o.ActualMemOverhead += overhead
	} else {
		o.MemUsage += usage
		o.MemOverhead += overhead
	}
}

//
// Subtract memory
//
func (o *IndexerNode) SubtractMemUsageOverhead(s *Solution, usage uint64, overhead uint64) {

	if s.UseLiveData() {
		o.ActualMemUsage -= usage
		o.ActualMemOverhead -= overhead
	} else {
		o.MemUsage -= usage
		o.MemOverhead -= overhead
	}
}

//
// Get data size
//
func (o *IndexerNode) GetDataSize(useLive bool) uint64 {

	if useLive {
		return o.ActualDataSize
	}

	return o.DataSize
}

//
// Add data size
//
func (o *IndexerNode) AddDataSize(s *Solution, datasize uint64) {

	if s.UseLiveData() {
		o.ActualDataSize += datasize
	} else {
		o.DataSize += datasize
	}
}

//
// Subtract data size
//
func (o *IndexerNode) SubtractDataSize(s *Solution, datasize uint64) {

	if s.UseLiveData() {
		o.ActualDataSize -= datasize
	} else {
		o.DataSize -= datasize
	}
}

//
// This function returns whether to exclude this node for taking in new index
//
func (o *IndexerNode) ExcludeIn(s *Solution) bool {
	return s.enableExclude && (o.IsDeleted() || o.exclude == "in" || o.exclude == "inout")
}

//
// This function returns whether to exclude this node for rebalance out index
//
func (o *IndexerNode) ExcludeOut(s *Solution) bool {
	return s.enableExclude && (!o.IsDeleted() && (o.exclude == "out" || o.exclude == "inout"))
}

//
// This function returns whether to exclude this node for rebalance in or out any index
//
func (o *IndexerNode) ExcludeAny(s *Solution) bool {
	return o.ExcludeIn(s) || o.ExcludeOut(s)
}

//
// This function returns whether to exclude this node for rebalance any index
//
func (o *IndexerNode) ExcludeAll(s *Solution) bool {
	return o.ExcludeIn(s) && o.ExcludeOut(s)
}

//
// This function changes the exclude setting of a node
//
func (o *IndexerNode) SetExclude(exclude string) {
	o.exclude = exclude
}

//
// This function changes the exclude setting of a node
//
func (o *IndexerNode) UnsetExclude() {
	o.exclude = ""
}

//
// Does indexer satisfy constraint?
//
func (o *IndexerNode) SatisfyNodeConstraint() bool {

	return o.meetConstraint
}

//
// Evaluate if indexer satisfy constraint
//
func (o *IndexerNode) EvaluateNodeConstraint(s *Solution) {

	if s.place != nil && s.constraint != nil {
		eligibles := s.place.GetEligibleIndexes()
		o.meetConstraint = s.constraint.SatisfyNodeConstraint(s, o, eligibles)
	}
}

//////////////////////////////////////////////////////////////
// IndexUsage
//////////////////////////////////////////////////////////////

//
// This function makes a copy of a index usage
//
func (o *IndexUsage) clone() *IndexUsage {

	r := *o
	r.Hosts = nil
	r.initialNode = nil // should set to nil

	if o.Instance != nil {
		inst := *o.Instance
		r.Instance = &inst
	}

	return &r
}

//
// This function returns a string representing the index
//
func (o *IndexUsage) String() string {
	return fmt.Sprintf("%v:%v:%v", o.DefnId, o.InstId, o.PartnId)
}

//
// This function creates a new index usage
//
func newIndexUsage(defnId common.IndexDefnId, instId common.IndexInstId, partnId common.PartitionId, name string, bucket string) *IndexUsage {

	return &IndexUsage{DefnId: defnId,
		InstId:  instId,
		PartnId: partnId,
		Name:    name,
		Bucket:  bucket,
	}
}

//
// Get cpu usage
//
func (o *IndexUsage) GetCpuUsage(useLive bool) float64 {

	if useLive {
		return o.ActualCpuUsage
	}

	return o.CpuUsage
}

//
// Get memory usage
//
func (o *IndexUsage) GetMemUsage(useLive bool) uint64 {

	if o.NoUsageInfo {
		return o.EstimatedMemUsage
	}

	if useLive {
		return o.ActualMemUsage
	}

	return o.MemUsage
}

//
// Get memory overhead
//
func (o *IndexUsage) GetMemOverhead(useLive bool) uint64 {

	if useLive {
		return o.ActualMemOverhead
	}

	return o.MemOverhead
}

//
// Get total memory
//
func (o *IndexUsage) GetMemTotal(useLive bool) uint64 {

	if o.NoUsageInfo {
		return o.EstimatedMemUsage
	}

	if useLive {
		return o.ActualMemUsage + o.ActualMemOverhead
	}

	return o.MemUsage + o.MemOverhead
}

//
// Get data size
//
func (o *IndexUsage) GetDataSize(useLive bool) uint64 {

	if o.NoUsageInfo {
		return o.EstimatedDataSize
	}

	if useLive {
		return o.ActualDataSize
	}

	return o.DataSize
}

//
// Get resident ratio
//
func (o *IndexUsage) GetResidentRatio(useLive bool) float64 {

	var ratio float64
	if useLive {
		ratio = float64(o.ActualResidentPercent)
	} else {
		ratio = o.ResidentRatio
	}

	if ratio == 0 {
		ratio = 100
	}

	return ratio
}

//
// Get build percent
//
func (o *IndexUsage) GetBuildPercent(useLive bool) uint64 {

	if useLive {
		return o.ActualBuildPercent
	}

	return 100
}

func (o *IndexUsage) HasSizing(useLive bool) bool {

	if o.NoUsageInfo {
		return o.EstimatedMemUsage != 0
	}

	if useLive {
		return o.ActualMemUsage != 0
	}

	return o.MemUsage != 0
}

func (o *IndexUsage) GetDisplayName() string {

	if o.Instance == nil {
		return o.Name
	}

	return common.FormatIndexPartnDisplayName(o.Instance.Defn.Name, o.Instance.ReplicaId, int(o.PartnId), true)
}

func (o *IndexUsage) GetStatsName() string {

	return o.GetDisplayName()
}

func (o *IndexUsage) GetInstStatsName() string {

	if o.Instance == nil {
		return o.Name
	}

	return common.FormatIndexInstDisplayName(o.Instance.Defn.Name, o.Instance.ReplicaId)
}

func (o *IndexUsage) GetPartitionName() string {

	if o.Instance == nil {
		return o.Name
	}

	return common.FormatIndexPartnDisplayName(o.Instance.Defn.Name, 0, int(o.PartnId), true)
}

func (o *IndexUsage) GetReplicaName() string {

	if o.Instance == nil {
		return o.Name
	}

	return common.FormatIndexPartnDisplayName(o.Instance.Defn.Name, o.Instance.ReplicaId, 0, false)
}

func (o *IndexUsage) IsReplica(other *IndexUsage) bool {

	return o.DefnId == other.DefnId && o.PartnId == other.PartnId
}

func (o *IndexUsage) IsSameIndex(other *IndexUsage) bool {

	return o.DefnId == other.DefnId
}

func (o *IndexUsage) IsSameInst(other *IndexUsage) bool {

	return o.DefnId == other.DefnId && o.InstId == other.InstId
}

func (o *IndexUsage) IsSamePartition(other *IndexUsage) bool {

	return o.PartnId == other.PartnId
}

func (o *IndexUsage) IsEquivalentIndex(other *IndexUsage, checkSuppress bool) bool {

	if o.IsSameIndex(other) {
		return false
	}

	if !o.IsSamePartition(other) {
		return false
	}

	// suppressEquivCheck is only enabled for eligible index.  So as long as one index
	// has suppressEquivCheck, we should not check.
	if !checkSuppress || !o.suppressEquivIdxCheck && !other.suppressEquivIdxCheck {
		if o.Instance != nil && other.Instance != nil {
			return common.IsEquivalentIndex(&o.Instance.Defn, &other.Instance.Defn)
		}
	}

	return false
}

func (o *IndexUsage) IsMOI() bool {

	return o.StorageMode == common.MemoryOptimized || o.StorageMode == common.MemDB
}

func (o *IndexUsage) IsPlasma() bool {

	return o.StorageMode == common.PlasmaDB
}

//////////////////////////////////////////////////////////////
// UsageBasedCostMethod
//////////////////////////////////////////////////////////////

//
// Constructor
//
func newUsageBasedCostMethod(constraint ConstraintMethod,
	dataCostWeight float64,
	cpuCostWeight float64,
	memCostWeight float64) *UsageBasedCostMethod {

	return &UsageBasedCostMethod{
		constraint:     constraint,
		dataCostWeight: dataCostWeight,
		memCostWeight:  memCostWeight,
		cpuCostWeight:  cpuCostWeight,
	}
}

//
// Compute cost based on variance on memory and cpu usage across indexers
//
func (c *UsageBasedCostMethod) Cost(s *Solution) float64 {

	// compute usage statistics
	c.MemMean, c.MemStdDev = s.ComputeMemUsage()
	c.CpuMean, c.CpuStdDev = s.ComputeCpuUsage()
	c.TotalData, c.DataMoved, c.TotalIndex, c.IndexMoved = s.computeIndexMovement(false)
	c.MemFree, c.CpuFree = s.computeFreeRatio()
	c.IdxMean, c.IdxStdDev = s.ComputeEmptyIndexDistribution()
	c.DataSizeMean, c.DataSizeStdDev = s.ComputeDataSize()

	memCost := float64(0)
	cpuCost := float64(0)
	movementCost := float64(0)
	indexCost := float64(0)
	emptyIdxCost := float64(0)
	dataSizeCost := float64(0)
	count := 0

	if c.memCostWeight > 0 && c.MemMean != 0 {
		memCost = c.MemStdDev / c.MemMean * c.memCostWeight
	}
	count++

	if c.cpuCostWeight > 0 && c.CpuMean != 0 {
		cpuCost = c.CpuStdDev / c.CpuMean * c.cpuCostWeight
	}
	count++

	if c.DataSizeMean != 0 {
		dataSizeCost = c.DataSizeStdDev / c.DataSizeMean
	}
	count++

	// consider the number of "emtpy" index per node.  Empty index
	// is index with no recored memory or cpu usage (exlcuding mem overhead).
	// It could be index without stats or sizing information.  This
	// help to distribute empty index evenly across nodes. Note that if
	// an index holds no key, it may still have some memory overhead usage
	// (from sizing).
	if c.IdxMean != 0 {
		emptyIdxCost = c.IdxStdDev / c.IdxMean
		count++
	}

	// UsageCost is used as a weight to scale the impact of
	// moving data during rebalance.  Usage cost is affected by:
	// 1) relative ratio of memory deviation and memory mean
	// 2) relative ratio of cpu deviation and cpu mean
	// 3) relative ratio of data size deviation and data size mean
	usageCost := (cpuCost + memCost + dataSizeCost) / float64(3)

	if c.dataCostWeight > 0 && c.TotalData != 0 {
		// The cost of moving data is inversely adjust by the usage cost.
		// If the cluster resource usage is highly unbalanced (high
		// usage cost), the cost of data movement has less hinderance
		// for balancing resource consumption.
		weight := c.dataCostWeight * (1 - usageCost)
		movementCost = float64(c.DataMoved) / float64(c.TotalData) * weight
		count++
	}

	if c.dataCostWeight > 0 && c.TotalIndex != 0 {
		weight := c.dataCostWeight * (1 - usageCost)
		indexCost = float64(c.IndexMoved) / float64(c.TotalIndex) * weight
		count++
	}

	logging.Tracef("Planner::cost: mem cost %v cpu cost %v data moved %v index moved %v emptyIdx cost %v dataSize cost %v count %v",
		memCost, cpuCost, movementCost, indexCost, emptyIdxCost, dataSizeCost, count)

	return (memCost + cpuCost + emptyIdxCost + movementCost + indexCost + dataSizeCost) / float64(count)
}

//
// Print statistics
//
func (s *UsageBasedCostMethod) Print() {

	var memUtil float64
	var cpuUtil float64
	var dataSizeUtil float64
	var dataMoved float64
	var indexMoved float64

	if s.MemMean != 0 {
		memUtil = float64(s.MemStdDev) / float64(s.MemMean) * 100
	}

	if s.CpuMean != 0 {
		cpuUtil = float64(s.CpuStdDev) / float64(s.CpuMean) * 100
	}

	if s.DataSizeMean != 0 {
		dataSizeUtil = float64(s.DataSizeStdDev) / float64(s.DataSizeMean) * 100
	}

	if s.TotalData != 0 {
		dataMoved = float64(s.DataMoved) / float64(s.TotalData)
	}

	if s.TotalIndex != 0 {
		indexMoved = float64(s.IndexMoved) / float64(s.TotalIndex)
	}

	logging.Infof("Indexer Memory Mean %v (%s)", uint64(s.MemMean), formatMemoryStr(uint64(s.MemMean)))
	logging.Infof("Indexer Memory Deviation %v (%s) (%.2f%%)", uint64(s.MemStdDev), formatMemoryStr(uint64(s.MemStdDev)), memUtil)
	logging.Infof("Indexer Memory Utilization %.4f", float64(s.MemMean)/float64(s.constraint.GetMemQuota()))
	logging.Infof("Indexer CPU Mean %.4f", s.CpuMean)
	logging.Infof("Indexer CPU Deviation %.2f (%.2f%%)", s.CpuStdDev, cpuUtil)
	logging.Infof("Indexer CPU Utilization %.4f", float64(s.CpuMean)/float64(s.constraint.GetCpuQuota()))
	logging.Infof("Indexer Data Size Mean %v (%s)", uint64(s.DataSizeMean), formatMemoryStr(uint64(s.DataSizeMean)))
	logging.Infof("Indexer Data Size Deviation %v (%s) (%.2f%%)", uint64(s.DataSizeStdDev), formatMemoryStr(uint64(s.DataSizeStdDev)), dataSizeUtil)
	logging.Infof("Total Index Data (in original layout) %v", formatMemoryStr(s.TotalData))
	logging.Infof("Index Data Moved (after planning) %v (%.2f%%)", formatMemoryStr(s.DataMoved), dataMoved)
	logging.Infof("No. Index (in original layout) %v", formatMemoryStr(s.TotalIndex))
	logging.Infof("No. Index Moved (after planning) %v (%.2f%%)", formatMemoryStr(s.IndexMoved), indexMoved)
}

//
// Validate the solution
//
func (c *UsageBasedCostMethod) Validate(s *Solution) error {

	return nil
}

//////////////////////////////////////////////////////////////
// RandomPlacement
//////////////////////////////////////////////////////////////

//
// Constructor
//
func newRandomPlacement(indexes []*IndexUsage, allowSwap bool, swapDeletedOnly bool) *RandomPlacement {
	p := &RandomPlacement{
		rs:              rand.New(rand.NewSource(time.Now().UnixNano())),
		indexes:         make(map[*IndexUsage]bool),
		eligibles:       make([]*IndexUsage, len(indexes)),
		optionals:       nil,
		allowSwap:       allowSwap,
		swapDeletedOnly: swapDeletedOnly,
	}

	// index to be balanced
	for i, index := range indexes {
		p.indexes[index] = true
		p.eligibles[i] = index
	}

	return p
}

//
// Get index for placement
//
func (p *RandomPlacement) GetEligibleIndexes() map[*IndexUsage]bool {

	return p.indexes
}

//
// Add optional index for placement
//
func (p *RandomPlacement) AddOptionalIndexes(indexes []*IndexUsage) {

	p.optionals = append(p.optionals, indexes...)
	for _, index := range indexes {
		p.indexes[index] = true
	}
}

//
// Remove optional index for placement
//
func (p *RandomPlacement) RemoveOptionalIndexes() []*IndexUsage {

	for _, index := range p.optionals {
		delete(p.indexes, index)
	}

	result := p.optionals
	p.optionals = nil

	return result
}

//
// Is there any optional index for placement
//
func (p *RandomPlacement) HasOptionalIndexes() bool {

	return len(p.optionals) > 0
}

//
// Validate
//
func (p *RandomPlacement) Validate(s *Solution) error {

	if !s.getConstraintMethod().CanAddNode(s) {

		for index, _ := range p.indexes {
			numReplica := s.findNumReplica(index)

			if numReplica > s.findNumLiveNode() {
				if s.UseLiveData() {
					logging.Warnf("Index has more replica than indexer nodes. Index=%v Bucket=%v",
						index.GetDisplayName(), index.Bucket)
				} else {
					return errors.New(fmt.Sprintf("Index has more replica than indexer nodes. Index=%v Bucket=%v",
						index.GetDisplayName(), index.Bucket))
				}
			}

			if s.numServerGroup > 1 && numReplica > s.numServerGroup {
				logging.Warnf("Index has more replica than server group. Index=%v Bucket=%v",
					index.GetDisplayName(), index.Bucket)
			}
		}
	}

	if s.ignoreResourceConstraint() {
		return nil
	}

	if s.canRunEstimation() {
		return nil
	}

	memQuota := s.getConstraintMethod().GetMemQuota()
	//cpuQuota := float64(s.getConstraintMethod().GetCpuQuota())

	for index, _ := range p.indexes {

		//if index.GetMemTotal(s.UseLiveData()) > memQuota || index.GetCpuUsage(s.UseLiveData()) > cpuQuota {
		if index.GetMemTotal(s.UseLiveData()) > memQuota {
			return errors.New(fmt.Sprintf("Index exceeding quota. Index=%v Bucket=%v Memory=%v Cpu=%.4f MemoryQuota=%v CpuQuota=%v",
				index.GetDisplayName(), index.Bucket, index.GetMemTotal(s.UseLiveData()), index.GetCpuUsage(s.UseLiveData()), s.getConstraintMethod().GetMemQuota(),
				s.getConstraintMethod().GetCpuQuota()))
		}

		if !s.constraint.CanAddNode(s) {
			found := false
			for _, indexer := range s.Placement {
				freeMem := s.getConstraintMethod().GetMemQuota()
				freeCpu := float64(s.getConstraintMethod().GetCpuQuota())

				for _, index2 := range indexer.Indexes {
					if !p.isEligibleIndex(index2) {
						freeMem -= index2.GetMemTotal(s.UseLiveData())
						freeCpu -= index2.GetCpuUsage(s.UseLiveData())
					}
				}

				//if freeMem >= index.GetMemTotal(s.UseLiveData()) && freeCpu >= index.GetCpuUsage(s.UseLiveData()) {
				if freeMem >= index.GetMemTotal(s.UseLiveData()) {
					found = true
					break
				}
			}

			if !found {
				return errors.New(fmt.Sprintf("Cannot find an indexer with enough free memory or cpu for index. Index=%v Bucket=%v",
					index.GetDisplayName(), index.Bucket))
			}
		}
	}

	return nil
}

//
// Has any eligible index?
//
func (p *RandomPlacement) hasEligibleIndex() bool {
	return len(p.indexes) != 0
}

//
// Randomly select a single index to move to a different node
//
// rebalance steps:
// 1) Find out index that are eligible to be moved
//    - swap rebalance: index on ejected node
//    - general rebalance: all index
// 2) Move indexes from a ejected node to a "new" node (node with no index)
// 3) If it is a simple swap (no. of ejected node == no. of new node), then stop.
// 4) If there is still any ejected node left after step (2), move those
//    indexes to any node.   After this step, no index on ejected node.
// 5) Perform general rebalance on eligible index.
//    - For index with usage info, rebalance by minimizing usage variance.
//    - For index with no usage info (e.g. deferred index), rebalance by
//      round robin across nodes.
//
func (p *RandomPlacement) Move(s *Solution) (bool, bool, bool) {

	if !p.hasEligibleIndex() {
		return false, true, true
	}

	if p.swapDeleteNode(s) {
		s.removeEmptyDeletedNode()
		return true, false, true
	}

	if p.swapDeletedOnly {
		done := len(s.getDeleteNodes()) == 0
		return done, done, done
	}

	success, final, force := p.randomMoveByLoad(s, true)
	if success {
		s.removeEmptyDeletedNode()
	}

	return success, final, force
}

//
// If there is delete node, try to see if there is an indexer
// node that can host all the indexes for that delete node.
//
func (p *RandomPlacement) swapDeleteNode(s *Solution) bool {

	result := false

	outNodes := s.getDeleteNodes()
	outNodes = sortNodeByUsage(s, outNodes)
	outNodes = reverseNode(outNodes)

	for _, outNode := range outNodes {

		indexer := p.findSwapCandidateNode(s, outNode)

		if indexer != nil {
			if indexer.NodeId == outNode.NodeId {
				continue
			}

			logging.Tracef("Planner::move delete: out node %v swap node %v", outNode, indexer)

			outIndex := make([]*IndexUsage, len(outNode.Indexes))
			copy(outIndex, outNode.Indexes)
			for _, index := range outIndex {
				logging.Tracef("Planner::move delete: source %v index %v target %v",
					outNode.NodeId, index, indexer.NodeId)
				s.moveIndex(outNode, index, indexer)
			}

			result = true
		}
	}

	return result
}

//
// Remove Eligible Index.  It does not remove "optional eligible" index.
//
func (p *RandomPlacement) RemoveEligibleIndex(indexes []*IndexUsage) {

	for _, index := range indexes {
		delete(p.indexes, index)
	}

	newEligibles := make([]*IndexUsage, len(p.indexes))
	count := 0
	for _, eligible := range p.eligibles {
		if _, ok := p.indexes[eligible]; ok {
			newEligibles[count] = eligible
			count++
		}
	}

	p.eligibles = newEligibles
}

//
// This function finds a node that has least usage consumption and index count,
// while allowing to add the "source" index to this node without violating
// constraints.
//
func (p *RandomPlacement) findLeastUsedAndPopulatedTargetNode(s *Solution, source *IndexUsage, exclude *IndexerNode) *IndexerNode {

	memFree, cpuFree := s.computeFreeRatio()
	threshold := memFree + cpuFree

	for threshold >= -0.1 {

		indexers := ([]*IndexerNode)(nil)
		for _, indexer := range s.Placement {
			if !indexer.ExcludeIn(s) &&
				indexer.NodeId != exclude.NodeId &&
				computeIndexerFreeQuota(s, indexer) >= threshold {
				indexers = append(indexers, indexer)
			}
		}

		if len(indexers) > 0 {
			indexers = sortNodeByNoUsageInfoIndexCount(indexers)

			for _, indexer := range indexers {
				if s.constraint.CanAddIndex(s, indexer, source) == NoViolation {
					return indexer
				}
			}
		}

		threshold -= 0.1
	}

	return nil
}

//
// Find a node that is a swap candidate for the current node.
// 1) node that matches the resource usage requirement.
// 2) replacement is not a deleted node
// 3) indexes do not violate HA properties
// 4) If current node has index with no sizing info, then
//    try to find an empty node.
//
func (p *RandomPlacement) findSwapCandidateNode(s *Solution, node *IndexerNode) *IndexerNode {

	for _, indexer := range s.Placement {

		// skip if node is the same
		if indexer.NodeId == node.NodeId {
			continue
		}

		// exclude indexer
		if indexer.ExcludeIn(s) {
			continue
		}

		// skip if target node is to be ejected
		if indexer.isDelete {
			continue
		}

		// try to swap to an empty node
		if len(indexer.Indexes) != 0 {
			continue
		}

		satisfyConstraint := true
		for _, index := range node.Indexes {
			if s.constraint.CanAddIndex(s, indexer, index) != NoViolation {
				satisfyConstraint = false
				break
			}
		}

		if satisfyConstraint {
			return indexer
		}
	}

	return nil
}

//
// Try random swap
//
func (p *RandomPlacement) tryRandomSwap(s *Solution, sources []*IndexerNode, targets []*IndexerNode, checkConstraint bool) bool {

	// Swap can improve the quality of solution but only when the
	// solution is fairly balanced.
	if s.currentCost < 0.005 {
		n := int64(p.rs.Int63n(3))
		if n < 1 {
			return p.randomSwap(s, sources, targets, checkConstraint)
		}
	}

	return false
}

//
// Randomly select a single index to move to a different node
//
func (p *RandomPlacement) randomMoveByLoad(s *Solution, checkConstraint bool) (bool, bool, bool) {

	numOfIndexers := len(s.Placement)
	if numOfIndexers == 1 {
		// only one indexer
		return false, false, false
	}

	// Find a set of candidates (indexer node) that has eligible index
	// From the set of candidates, find those that are under resource constraint.
	// Compute the loads for every constrained candidate
	candidates := p.findCandidates(s)
	logging.Tracef("Planner::candidates: len=%v, %v", len(candidates), candidates)
	constrained := p.findConstrainedNodes(s, s.constraint, candidates)
	logging.Tracef("Planner::constrained: len=%v, %v", len(constrained), constrained)
	loads, total := computeLoads(s, constrained)

	// Done with basic swap rebalance case?
	if len(s.getDeleteNodes()) == 0 &&
		s.numDeletedNode > 0 &&
		s.numNewNode == s.numDeletedNode &&
		len(constrained) == 0 {
		return true, true, true
	}

	retryCount := numOfIndexers * 10
	for i := 0; i < retryCount; i++ {

		// If there is one node that does not satisfy constriant,
		if len(constrained) == 1 {
			if !s.constraint.CanAddNode(s) {
				// If planner is working on a fixed cluster, then
				// try exhaustively moving or swapping indexes away from this node.

				if s.hasNewNodes() && s.hasDeletedNodes() {
					// Try moving to new nodes first
					success, force := p.exhaustiveMove(s, constrained, s.Placement, checkConstraint, true)
					if success {
						return true, false, force
					}
				}

				success, force := p.exhaustiveMove(s, constrained, s.Placement, checkConstraint, false)
				if success {
					return true, false, force
				}

				if p.exhaustiveSwap(s, constrained, candidates, checkConstraint) {
					return true, false, false
				}

				// if we cannot find a solution after exhaustively trying to swap or move
				// index in the last constrained node, then we possibly cannot reach a
				// solution.
				return false, true, true
			} else {
				// If planner can grow the cluster, then just try to randomly swap.
				// If cannot swap, then logic fall through to move index.
				if p.randomSwap(s, constrained, candidates, checkConstraint) {
					return true, false, false
				}
			}
		}

		// Select an constrained candidate based on weighted probability
		// The most constrained candidate has a higher probabilty to be selected.
		source := getWeightedRandomNode(p.rs, constrained, loads, total)

		// If cannot find a constrained candidate, then try to randomly
		// pick two candidates and try to swap their indexes.
		if source == nil {

			if p.tryRandomSwap(s, candidates, candidates, checkConstraint) {
				return true, false, false
			}

			// If swap fails, then randomly select a candidate as source.
			source = getRandomNode(p.rs, candidates)
			if source == nil {
				return false, false, false
			}
		}

		// From the candidate, randomly select a movable index.
		index := p.getRandomEligibleIndex(p.rs, source.Indexes)
		if index == nil {
			continue
		}

		target := (*IndexerNode)(nil)
		if !index.HasSizing(s.UseLiveData()) {
			target = p.findLeastUsedAndPopulatedTargetNode(s, index, source)
		} else {
			// Select an uncongested indexer which is different from source.
			// The most uncongested indexer has a higher probability to be selected.
			target = p.getRandomUncongestedNodeExcluding(s, source, index, checkConstraint)
		}

		if target == nil {
			// if cannot find a uncongested indexer, then check if there is only
			// one candidate and it satisfy resource constraint.  If so, there is
			// no more move (final state).
			if len(candidates) == 1 && source.SatisfyNodeConstraint() {
				logging.Tracef("Planner::final move: source %v index %v", source.NodeId, index)
				return true, true, true
			}

			logging.Tracef("Planner::no target : index %v mem %v cpu %.4f source %v",
				index, formatMemoryStr(index.GetMemTotal(s.UseLiveData())), index.GetCpuUsage(s.UseLiveData()), source.NodeId)

			// There could be more candidates, pick another one.
			continue
		}

		logging.Tracef("Planner::try move: index %v mem %v cpu %.4f source %v target %v",
			index, formatMemoryStr(index.GetMemTotal(s.UseLiveData())), index.GetCpuUsage(s.UseLiveData()), source.NodeId, target.NodeId)

		// See if the index can be moved while obeying resource constraint.
		violation := s.constraint.CanAddIndex(s, target, index)
		if !checkConstraint || violation == NoViolation {
			logging.Tracef("Planner::move: source %v index %v target %v checkConstraint %v",
				source.NodeId, index, target.NodeId, checkConstraint)
			s.moveIndex(source, index, target)
			return true, false, source.isDelete

		} else {
			logging.Tracef("Planner::try move fail: violation %s", violation)
		}
	}

	if logging.IsEnabled(logging.Trace) {
		for _, indexer := range s.Placement {
			logging.Tracef("Planner::no move: indexer %v mem %v cpu %.4f ",
				indexer.NodeId, formatMemoryStr(indexer.GetMemTotal(s.UseLiveData())), indexer.GetCpuUsage(s.UseLiveData()))
		}
	}

	// Give it one more try to swap constrained node
	return p.tryRandomSwap(s, constrained, candidates, checkConstraint), false, false
}

//
// Randomly select a single index to move to a different node
//
func (p *RandomPlacement) randomMoveNoConstraint(s *Solution, target int) (uint64, uint64) {

	numOfIndexers := len(s.Placement)
	if numOfIndexers == 1 {
		// only one indexer
		return 0, 0
	}

	movedIndex := uint64(0)
	movedData := uint64(0)
	numOfIndexes := len(p.indexes)

	for percentage := 0; percentage < target; {

		source := getRandomNode(p.rs, s.Placement)
		if source == nil {
			return 0, 0
		}

		index := getRandomIndex(p.rs, source.Indexes)
		if index == nil {
			continue
		}

		target := getRandomNode(p.rs, s.Placement)
		if source == target {
			continue
		}

		s.moveIndex(source, index, target)
		movedIndex++
		movedData += index.GetMemUsage(s.UseLiveData())

		_, _, _, indexMoved := s.computeIndexMovement(true)
		percentage = int(float64(indexMoved) / float64(numOfIndexes) * 100)
	}

	return movedIndex, movedData
}

//
// Find a set of candidate indexer nodes
//
func (p *RandomPlacement) findCandidates(s *Solution) []*IndexerNode {

	candidates := ([]*IndexerNode)(nil)
	outNodes := s.getDeleteNodes()

	if len(outNodes) > 0 {
		for _, indexer := range outNodes {
			if len(indexer.Indexes) > 0 {
				candidates = append(candidates, indexer)
			}
		}

		if len(candidates) > 0 {
			return candidates
		}
	}

	//TODO
	// only include node with index to be rebalanced
	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {
			if _, ok := p.indexes[index]; ok {
				candidates = append(candidates, indexer)
				break
			}
		}
	}

	return candidates
}

//
// This function get a random uncongested node.
//
func (p *RandomPlacement) getRandomUncongestedNodeExcluding(s *Solution, exclude *IndexerNode, index *IndexUsage, checkConstraint bool) *IndexerNode {

	if s.hasDeletedNodes() && s.hasNewNodes() {

		indexers := ([]*IndexerNode)(nil)

		for _, indexer := range s.Placement {
			if !indexer.ExcludeIn(s) &&
				exclude.NodeId != indexer.NodeId &&
				s.constraint.SatisfyNodeResourceConstraint(s, indexer) &&
				!indexer.isDelete &&
				indexer.isNew {
				indexers = append(indexers, indexer)
			}
		}

		target := p.getRandomFittedNode(s, indexers, index, checkConstraint)
		if target != nil {
			return target
		}
	}

	indexers := ([]*IndexerNode)(nil)

	for _, indexer := range s.Placement {
		if !indexer.ExcludeIn(s) &&
			exclude.NodeId != indexer.NodeId &&
			s.constraint.SatisfyNodeResourceConstraint(s, indexer) &&
			!indexer.isDelete {
			indexers = append(indexers, indexer)
		}
	}

	return p.getRandomFittedNode(s, indexers, index, checkConstraint)
}

//
// This function get a random node that can fit the index.
//
func (p *RandomPlacement) getRandomFittedNode(s *Solution, indexers []*IndexerNode, index *IndexUsage, checkConstraint bool) *IndexerNode {

	total := int64(0)
	loads := make([]int64, len(indexers))

	for i, indexer := range indexers {
		violation := s.constraint.CanAddIndex(s, indexer, index)
		if !checkConstraint || violation == NoViolation {
			loads[i] = int64(computeIndexerFreeQuota(s, indexer) * 100)
			total += loads[i]
		}
	}

	logging.Tracef("Planner::uncongested: %v loads %v total %v", indexers, loads, total)

	if total > 0 {
		n := int64(p.rs.Int63n(total))

		for i, load := range loads {
			if load != 0 {
				if n <= load {
					return indexers[i]
				} else {
					n -= load
				}
			}
		}
	}

	return nil
}

//
// Find a random index
//
func (p *RandomPlacement) getRandomEligibleIndex(rs *rand.Rand, indexes []*IndexUsage) *IndexUsage {

	var candidates []*IndexUsage
	for _, index := range indexes {
		if _, ok := p.indexes[index]; ok {
			candidates = append(candidates, index)
		}
	}

	numOfIndexes := len(candidates)
	if numOfIndexes > 0 {
		n := rs.Intn(numOfIndexes)
		return candidates[n]
	}

	return nil
}

//
// This function randomly place indexes among indexer nodes
//
func (p *RandomPlacement) Add(s *Solution, indexes []*IndexUsage) error {

	candidates := make([]*IndexerNode, 0, len(s.Placement))
	for _, indexer := range s.Placement {
		if !indexer.ExcludeAny(s) {
			candidates = append(candidates, indexer)
		}
	}

	if len(candidates) == 0 {
		return errors.New("Cannot find any indexer that can add new indexes")
	}

	for _, idx := range indexes {
		indexer := getRandomNode(p.rs, candidates)
		s.addIndex(indexer, idx)
		idx.initialNode = nil
	}

	return nil
}

//
// This function randomly place indexes among indexer nodes for initial placement
//
func (p *RandomPlacement) InitialPlace(s *Solution, indexes []*IndexUsage) error {

	candidates := make([]*IndexerNode, 0, len(s.Placement))
	for _, indexer := range s.Placement {
		if !indexer.ExcludeAny(s) {
			candidates = append(candidates, indexer)
		}
	}

	if len(candidates) == 0 {
		return errors.New("Cannot find any indexer that can add new indexes")
	}

	for _, idx := range indexes {
		indexer := getRandomNode(p.rs, candidates)
		s.addIndex(indexer, idx)
		idx.initialNode = indexer
	}

	return nil
}

//
// Randomly select two index and swap them.
//
func (p *RandomPlacement) randomSwap(s *Solution, sources []*IndexerNode, targets []*IndexerNode, checkConstraint bool) bool {

	if !p.allowSwap {
		return false
	}

	outNodes := s.getDeleteNodes()
	retryCount := len(sources) * 10
	for i := 0; i < retryCount; i++ {

		source := getRandomNode(p.rs, sources)
		target := getRandomNode(p.rs, targets)

		if source == nil || target == nil || source == target {
			continue
		}

		if hasMatchingNode(target.NodeId, outNodes) {
			continue
		}

		if target.ExcludeIn(s) {
			continue
		}

		sourceIndex := p.getRandomEligibleIndex(p.rs, source.Indexes)
		targetIndex := p.getRandomEligibleIndex(p.rs, target.Indexes)

		if sourceIndex == nil || targetIndex == nil {
			continue
		}

		if sourceIndex.NoUsageInfo != targetIndex.NoUsageInfo {
			continue
		}

		// do not swap replica
		if sourceIndex.IsReplica(targetIndex) {
			continue
		}

		// If index has no usage info, then swap only if violate HA constraint.
		if sourceIndex.NoUsageInfo && s.constraint.SatisfyIndexHAConstraint(s, source, sourceIndex, p.GetEligibleIndexes()) {
			continue
		}

		logging.Tracef("Planner::try swap: source index %v (mem %v cpu %.4f) target index %v (mem %v cpu %.4f) source %v target %v",
			sourceIndex, formatMemoryStr(sourceIndex.GetMemTotal(s.UseLiveData())), sourceIndex.GetCpuUsage(s.UseLiveData()),
			targetIndex, formatMemoryStr(targetIndex.GetMemTotal(s.UseLiveData())), targetIndex.GetCpuUsage(s.UseLiveData()),
			source.NodeId, target.NodeId)

		sourceViolation := s.constraint.CanSwapIndex(s, target, sourceIndex, targetIndex)
		targetViolation := s.constraint.CanSwapIndex(s, source, targetIndex, sourceIndex)

		if !checkConstraint || (sourceViolation == NoViolation && targetViolation == NoViolation) {
			logging.Tracef("Planner::swap: source %v source index %v target %v target index %v checkConstraint %v",
				source.NodeId, sourceIndex, target.NodeId, targetIndex, checkConstraint)
			s.moveIndex(source, sourceIndex, target)
			s.moveIndex(target, targetIndex, source)
			return true

		} else {
			logging.Tracef("Planner::try swap fail: source violation %s target violation %v", sourceViolation, targetViolation)
		}
	}

	if logging.IsEnabled(logging.Trace) {
		for _, indexer := range s.Placement {
			logging.Tracef("Planner::no swap: indexer %v mem %v cpu %.4f",
				indexer.NodeId, formatMemoryStr(indexer.GetMemTotal(s.UseLiveData())), indexer.GetCpuUsage(s.UseLiveData()))
		}
	}

	return false
}

//
// From the list of source indexes, iterate through the list of indexer to find a smaller index that it can swap with.
//
func (p *RandomPlacement) exhaustiveSwap(s *Solution, sources []*IndexerNode, targets []*IndexerNode, checkConstraint bool) bool {

	if !p.allowSwap {
		return false
	}

	for _, source := range sources {

		shuffledSourceIndexes := shuffleIndex(p.rs, source.Indexes)
		logging.Tracef("Planner::exhaustive swap: source index after shuffle len=%v, %v", len(shuffledSourceIndexes), shuffledSourceIndexes)

		for _, sourceIndex := range shuffledSourceIndexes {

			if !p.isEligibleIndex(sourceIndex) {
				continue
			}

			// If index has no usage info, then swap only if violate HA constraint.
			if sourceIndex.NoUsageInfo && s.constraint.SatisfyIndexHAConstraint(s, source, sourceIndex, p.GetEligibleIndexes()) {
				continue
			}

			shuffledTargets := shuffleNode(p.rs, targets)
			logging.Tracef("Planner::exhaustive swap: targets after shuffled len=%v, %v", len(shuffledTargets), shuffledTargets)

			for _, target := range shuffledTargets {

				if source.NodeId == target.NodeId || target.isDelete || target.ExcludeIn(s) {
					continue
				}

				shuffledTargetIndexes := shuffleIndex(p.rs, target.Indexes)
				logging.Tracef("Planner::exhaustive swap: target index after shuffle len=%v, %v", len(shuffledTargetIndexes), shuffledTargetIndexes)

				for _, targetIndex := range shuffledTargetIndexes {

					if !p.isEligibleIndex(targetIndex) {
						continue
					}

					if sourceIndex.NoUsageInfo != targetIndex.NoUsageInfo {
						continue
					}

					if sourceIndex.GetMemTotal(s.UseLiveData()) >= targetIndex.GetMemTotal(s.UseLiveData()) &&
						sourceIndex.GetCpuUsage(s.UseLiveData()) >= targetIndex.GetCpuUsage(s.UseLiveData()) {

						targetViolation := s.constraint.CanSwapIndex(s, target, sourceIndex, targetIndex)
						sourceViolation := s.constraint.CanSwapIndex(s, source, targetIndex, sourceIndex)

						logging.Tracef("Planner::try exhaustive swap: source index %v (mem %v cpu %.4f) target index %v (mem %v cpu %.4f) source %v target %v",
							sourceIndex, formatMemoryStr(sourceIndex.GetMemTotal(s.UseLiveData())), sourceIndex.GetCpuUsage(s.UseLiveData()),
							targetIndex, formatMemoryStr(targetIndex.GetMemTotal(s.UseLiveData())), targetIndex.GetCpuUsage(s.UseLiveData()),
							source.NodeId, target.NodeId)

						if !checkConstraint || (targetViolation == NoViolation && sourceViolation == NoViolation) {
							logging.Tracef("Planner::exhaustive swap: source %v source index %v target %v target index %v checkConstraint %v",
								source.NodeId, sourceIndex, target.NodeId, targetIndex, checkConstraint)
							s.moveIndex(source, sourceIndex, target)
							s.moveIndex(target, targetIndex, source)
							return true

						} else {
							logging.Tracef("Planner::try exhaustive swap fail: source violation %s target violation %v", sourceViolation, targetViolation)
						}
					}
				}
			}
		}
	}

	return false
}

//
// From the list of source indexes, iterate through the list of indexer that it can move to.
//
func (p *RandomPlacement) exhaustiveMove(s *Solution, sources []*IndexerNode, targets []*IndexerNode, checkConstraint bool, newNodeOnly bool) (bool, bool) {

	for _, source := range sources {

		shuffledSourceIndexes := shuffleIndex(p.rs, source.Indexes)
		logging.Tracef("Planner::exhaustive move: source index after shuffle len=%v, %v", len(shuffledSourceIndexes), shuffledSourceIndexes)

		for _, sourceIndex := range shuffledSourceIndexes {

			if !p.isEligibleIndex(sourceIndex) {
				continue
			}

			// If index has no usage info, then swap only if violate HA constraint.
			if !sourceIndex.HasSizing(s.UseLiveData()) {
				if target := p.findLeastUsedAndPopulatedTargetNode(s, sourceIndex, source); target != nil {
					logging.Tracef("Planner::exhaustive move: source %v index %v target %v checkConstraint %v",
						source.NodeId, sourceIndex, target.NodeId, checkConstraint)
					s.moveIndex(source, sourceIndex, target)
					return true, source.isDelete
				}
				continue
			}

			shuffledTargets := shuffleNode(p.rs, targets)
			logging.Tracef("Planner::exhaustive move: targets after shuffled len=%v, %v", len(shuffledTargets), shuffledTargets)

			for _, target := range shuffledTargets {

				if source.NodeId == target.NodeId || target.isDelete || (newNodeOnly && !target.isNew) || target.ExcludeIn(s) {
					continue
				}

				logging.Tracef("Planner::try exhaustive move: index %v mem %v cpu %.4f source %v target %v",
					sourceIndex, formatMemoryStr(sourceIndex.GetMemTotal(s.UseLiveData())), sourceIndex.GetCpuUsage(s.UseLiveData()),
					source.NodeId, target.NodeId)

				// See if the index can be moved while obeying resource constraint.
				violation := s.constraint.CanAddIndex(s, target, sourceIndex)
				if !checkConstraint || violation == NoViolation {
					logging.Tracef("Planner::exhaustive move: source %v index %v target %v checkConstraint %v",
						source.NodeId, sourceIndex, target.NodeId, checkConstraint)
					s.moveIndex(source, sourceIndex, target)
					return true, source.isDelete

				} else {
					logging.Tracef("Planner::try exhaustive move fail: violation %s", violation)
				}
			}
		}
	}

	return false, false
}

//
// Find a set of indexers do not satisfy node constriant.
//
func (p *RandomPlacement) findConstrainedNodes(s *Solution, constraint ConstraintMethod, indexers []*IndexerNode) []*IndexerNode {

	outNodes := s.getDeleteNodes()
	result := ([]*IndexerNode)(nil)

	if len(outNodes) > 0 {
		for _, indexer := range outNodes {
			if len(indexer.Indexes) > 0 {
				result = append(result, indexer)
			}
		}

		if len(result) > 0 {
			return result
		}
	}

	// look for indexer node that do not satisfy constraint
	for _, indexer := range indexers {
		if !indexer.SatisfyNodeConstraint() {
			result = append(result, indexer)
		}
	}

	return result
}

//
// Is this index an eligible index?
//
func (p *RandomPlacement) isEligibleIndex(index *IndexUsage) bool {

	_, ok := p.indexes[index]
	return ok
}

//////////////////////////////////////////////////////////////
// GeneralSizingMethod
//////////////////////////////////////////////////////////////

//
// Constructor
//
func newGeneralSizingMethod() *GeneralSizingMethod {
	return &GeneralSizingMethod{
		MOI:    newMOISizingMethod(),
		Plasma: newPlasmaSizingMethod(),
	}
}

//
// Validate
//
func (s *GeneralSizingMethod) Validate(solution *Solution) error {

	return nil
}

//
// This function computes the index size
//
func (s *GeneralSizingMethod) ComputeIndexSize(idx *IndexUsage) {

	if idx.IsPlasma() {
		s.Plasma.ComputeIndexSize(idx)
	} else {
		// for both MOI and forestdb
		// we don't have sizing for forestdb but we have simulation tests that run with forestdb
		s.MOI.ComputeIndexSize(idx)
	}
}

//
// This function computes the indexer memory and cpu usage
//
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

//
// This function computes the indexer memory overhead
//
func (s *GeneralSizingMethod) ComputeIndexerOverhead(o *IndexerNode) {

	// channel overhead : 100MB
	overhead := uint64(100 * 1024 * 1024)

	for _, idx := range o.Indexes {
		overhead += s.ComputeIndexOverhead(idx)
	}

	o.MemOverhead = uint64(overhead)
}

//
// This function estimates the index memory overhead
//
func (s *GeneralSizingMethod) ComputeIndexOverhead(idx *IndexUsage) uint64 {

	if idx.IsMOI() {
		return s.MOI.ComputeIndexOverhead(idx)
	} else if idx.IsPlasma() {
		return s.Plasma.ComputeIndexOverhead(idx)
	}

	return 0
}

//
// This function estimates the min memory quota given a set of indexes
//
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

//
// Constructor
//
func newMOISizingMethod() *MOISizingMethod {
	return &MOISizingMethod{}
}

//
// This function computes the index size
//
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
			idx.DataSize = (46 + idx.ActualKeySize) * idx.NumOfDocs
		}
	} else {
		if idx.AvgDocKeySize != 0 {
			// primary index mem size : (74 + DocIdLen) * NumberOfItems
			idx.DataSize = (74 + idx.AvgDocKeySize) * idx.NumOfDocs
		} else if idx.ActualKeySize != 0 {
			// primary index mem size : ActualKeySize * NumberOfItems
			idx.DataSize = idx.ActualKeySize * idx.NumOfDocs
		}
	}
	idx.MemUsage = idx.DataSize

	// compute cpu usage
	idx.CpuUsage = float64(idx.MutationRate)/float64(MOIMutationRatePerCore) + float64(idx.ScanRate)/float64(MOIScanRatePerCore)
	//idx.CpuUsage = math.Floor(idx.CpuUsage) + 1

	idx.MemOverhead = s.ComputeIndexOverhead(idx)
}

//
// This function estimates the index memory overhead
//
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

//
// Constructor
//
func newPlasmaSizingMethod() *PlasmaSizingMethod {
	return &PlasmaSizingMethod{}
}

//
// This function computes the index size
//
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
			idx.DataSize = (46 + idx.ActualKeySize) * idx.NumOfDocs
		}
	} else {
		if idx.AvgDocKeySize != 0 {
			// primary index mem size : (74 + DocIdLen) * NumberOfItems
			idx.DataSize = (74 + idx.AvgDocKeySize) * idx.NumOfDocs
		} else if idx.ActualKeySize != 0 {
			// primary index mem size : ActualKeySize * NumberOfItems
			// actual key size = mem used / num docs (include both main and back index)
			idx.DataSize = idx.ActualKeySize * idx.NumOfDocs
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

//
// This function estimates the index memory overhead
//
func (s *PlasmaSizingMethod) ComputeIndexOverhead(idx *IndexUsage) uint64 {

	// protobuf overhead : 150MB per index
	overhead := float64(150 * 1024 * 1024)

	snapshotOverhead := float64(0)

	mvcc := func() uint64 {
		count := uint64(idx.MutationRate * 60 * 20)
		if idx.NumOfDocs*3 < count {
			return idx.NumOfDocs * 3
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

//
// This function estimates the min memory quota given a set of indexes
//
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

//
// This function returns violations as a string
//
func (v *Violations) Error() string {
	err := fmt.Sprintf("\nMemoryQuota: %v\n", v.MemQuota)
	err += fmt.Sprintf("CpuQuota: %v\n", v.CpuQuota)

	for _, violation := range v.Violations {
		err += fmt.Sprintf("--- Violations for index <%v, %v> (mem %v, cpu %v) at node %v \n",
			violation.Name, violation.Bucket, formatMemoryStr(violation.MemUsage), violation.CpuUsage, violation.NodeId)

		for _, detail := range violation.Details {
			err += fmt.Sprintf("\t%v\n", detail)
		}
	}

	return err
}

//
// This function returns if there is any violation
//
func (v *Violations) IsEmpty() bool {

	return len(v.Violations) == 0
}
