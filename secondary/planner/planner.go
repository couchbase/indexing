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
// - support server group during planning (spock+)
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
	NoViolation           ViolationCode = "NoViolation"
	ResourceViolation                   = "ResourceViolation"
	AvailabilityViolation               = "AvailabilityViolation"
	DeleteNodeViolation                 = "DeleteNodeViolation"
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
	Move(s *Solution) (bool, bool)
	Add(s *Solution, indexes []*IndexUsage)
	InitialPlace(s *Solution, indexes []*IndexUsage)
	Validate(s *Solution) error
	GetEligibleIndexes() []*IndexUsage
}

type ConstraintMethod interface {
	GetMemQuota() uint64
	GetCpuQuota() uint64
	SatisfyClusterResourceConstraint(s *Solution) bool
	SatisfyNodeResourceConstraint(s *Solution, n *IndexerNode) bool
	SatisfyClusterConstraint(s *Solution, eligibles []*IndexUsage) bool
	SatisfyNodeConstraint(s *Solution, n *IndexerNode, eligibles []*IndexUsage) bool
	CanAddIndex(s *Solution, n *IndexerNode, u *IndexUsage) ViolationCode
	CanSwapIndex(s *Solution, n *IndexerNode, t *IndexUsage, i *IndexUsage) ViolationCode
	CanAddNode(s *Solution) bool
	Print()
	Validate(s *Solution) error
	GetViolations(s *Solution, indexes []*IndexUsage) *Violations
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
	ServerGroup string `json:"serverGroup,omitempty"`

	// input/output: resource consumption (from sizing)
	MemUsage    uint64 `json:"memUsage"`
	CpuUsage    uint64 `json:"cpuUsage"`
	DiskUsage   uint64 `json:"diskUsage,omitempty"`
	MemOverhead uint64 `json:"memOverhead"`

	// input/output: resource consumption (from live cluster)
	ActualMemUsage    uint64 `json:"actualMemUsage"`
	ActualMemOverhead uint64 `json:"actualMemOverhead"`

	// input: index residing on the node
	Indexes []*IndexUsage `json:"indexes"`

	// input: node to be removed
	delete bool
}

type IndexUsage struct {
	// input: index identification
	DefnId common.IndexDefnId `json:"defnId"`
	InstId common.IndexInstId `json:"instId"`
	Name   string             `json:"name"`
	Bucket string             `json:"bucket"`
	Hosts  []string           `json:"host"`

	// input: index sizing
	IsPrimary        bool   `json:"isPrimary,omitempty"`
	IsMOI            bool   `json:"isMOI,omitempty"`
	AvgSecKeySize    uint64 `json:"avgSecKeySize"`
	AvgDocKeySize    uint64 `json:"avgDocKeySize"`
	AvgArrSize       uint64 `json:"avgArrSize"`
	AvgArrKeySize    uint64 `json:"avgArrKeySize"`
	NumOfDocs        uint64 `json:"numOfDocs"`
	MemResidentRatio uint64 `json:"memResidentRatio,omitempty"`
	MutationRate     uint64 `json:"mutationRate"`
	ScanRate         uint64 `json:"scanRate"`

	// input: constraint (optional)
	ServerGroup string `json:"serverGroup,omitempty"`

	// input: resource consumption (from sizing)
	MemUsage    uint64 `json:"memUsage"`
	CpuUsage    uint64 `json:"cpuUsage"`
	DiskUsage   uint64 `json:"diskUsage,omitempty"`
	MemOverhead uint64 `json:"memOverhead,omitempty"`

	// input: resource consumption (from live cluster)
	ActualMemUsage    uint64 `json:"actualMemUsage"`
	ActualMemOverhead uint64 `json:"actualMemOverhead"`
	ActualKeySize     uint64 `json:"actualKeySize"`

	// input: index definition (optional)
	Instance *common.IndexInst `json:"instance,omitempty"`

	// input: node where index initially placed (optional)
	initialNode *IndexerNode

	// input: hint for placement / constraint
	suppressEquivIdxCheck bool
}

type Solution struct {
	constraint  ConstraintMethod
	sizing      SizingMethod
	isLiveData  bool
	useLiveData bool
	initialPlan bool

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
	CpuUsage uint64
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
	TotalData      uint64  `json:"totalData,omitempty"`
	DataMoved      uint64  `json:"dataMoved,omitempty"`
	TotalIndex     uint64  `json:"totalIndex,omitempty"`
	IndexMoved     uint64  `json:"indexMoved,omitempty"`
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
	rs        *rand.Rand
	indexes   map[*IndexUsage]*IndexUsage
	eligibles []*IndexUsage
	allowSwap bool
}

//////////////////////////////////////////////////////////////
// Interface Implementation - SizingMethod
//////////////////////////////////////////////////////////////

type MOISizingMethod struct {
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

	eligibles := p.placement.GetEligibleIndexes()
	solution.RelaxConstraintIfNecessary(eligibles)

	for i := 0; i < RunPerPlan; i++ {
		p.Try++
		startTime := time.Now()
		result, err = p.planSingleRun(command, solution)

		// if err == nil, type assertion will return !ok
		if _, ok := err.(*Violations); !ok {
			return result, err
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

	if err := p.Validate(current); err != nil {
		return nil, errors.New(fmt.Sprintf("Validation fails: %s", err))
	}

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
func (p *SAPlanner) Print() {

	logging.Infof("Score: %v", p.Score)
	logging.Infof("ElapsedTime: %v", formatTimeStr(p.ElapseTime))
	logging.Infof("ConvergenceTime: %v", formatTimeStr(p.ConvergenceTime))
	logging.Infof("Iteration: %v", p.Iteration)
	logging.Infof("Move: %v", p.Move)
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

	eligibles := p.placement.GetEligibleIndexes()
	neighbor := s.clone()
	force := false
	done := false
	retry := 0

	for retry = 0; retry < ResizePerIteration; retry++ {
		success, final := p.placement.Move(neighbor)
		if success {
			currentOK := s.constraint.SatisfyClusterConstraint(s, eligibles)
			neighborOK := neighbor.constraint.SatisfyClusterConstraint(neighbor, eligibles)
			logging.Tracef("Planner::findNeighbor retry: %v", retry)
			return neighbor, (force || (!currentOK && neighborOK)), final
		}

		// Add new node to change cluster in order to ensure constraint can be satisfied
		if !p.constraint.SatisfyClusterConstraint(neighbor, eligibles) && p.constraint.CanAddNode(s) {
			nodeId := strconv.FormatUint(uint64(rand.Uint32()), 10)
			neighbor.addNewNode(nodeId)
			logging.Tracef("Planner::add node: %v", nodeId)
			force = true

		} else {
			done = final
			break
		}
	}

	logging.Tracef("Planner::findNeighbor retry: %v", retry)
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

//////////////////////////////////////////////////////////////
// Solution
//////////////////////////////////////////////////////////////

//
// Constructor
//
func newSolution(constraint ConstraintMethod, sizing SizingMethod, indexers []*IndexerNode, isLive bool, useLive bool) *Solution {

	r := &Solution{
		constraint:  constraint,
		sizing:      sizing,
		Placement:   make([]*IndexerNode, len(indexers)),
		isLiveData:  isLive,
		useLiveData: useLive,
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
}

//
// This function makes a copy of existing solution.
//
func (s *Solution) clone() *Solution {

	r := &Solution{
		constraint:  s.constraint,
		sizing:      s.sizing,
		Placement:   ([]*IndexerNode)(nil),
		isLiveData:  s.isLiveData,
		useLiveData: s.useLiveData,
		initialPlan: s.initialPlan,
	}

	for _, node := range s.Placement {
		if node.delete && len(node.Indexes) == 0 {
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
		if node.delete && len(node.Indexes) == 0 {
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
		if indexer.delete {
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
	totalIndexCpu := uint64(0)
	maxIndexCpu := uint64(0)
	avgIndexSize := uint64(0)
	avgIndexCpu := uint64(0)

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
		avgIndexCpu = totalIndexCpu / uint64(numOfIndex)
	}

	logging.Infof("Number of indexes: %v", numOfIndex)
	logging.Infof("Number of indexers: %v", len(s.Placement))
	logging.Infof("Avg Index Size: %v (%s)", avgIndexSize, formatMemoryStr(uint64(avgIndexSize)))
	logging.Infof("Max Index Size: %v (%s)", uint64(maxIndexSize), formatMemoryStr(uint64(maxIndexSize)))
	logging.Infof("Max Indexer Overhead: %v (%s)", uint64(maxIndexerOverhead), formatMemoryStr(uint64(maxIndexerOverhead)))
	logging.Infof("Avg Index Cpu: %v", avgIndexCpu)
	logging.Infof("Max Index Cpu: %v", uint64(maxIndexCpu))
}

//
// This prints out layout for the solution
//
func (s *Solution) PrintLayout() {

	for _, indexer := range s.Placement {

		logging.Infof("")
		logging.Infof("Indexer serverGroup:%v, nodeId:%v, useLiveData:%v", indexer.ServerGroup, indexer.NodeId, s.UseLiveData())
		logging.Infof("Indexer total memory:%v (%s) data:%v (%s), overhead:%v (%s), cpu:%v, number of indexes:%v",
			indexer.GetMemTotal(s.UseLiveData()), formatMemoryStr(uint64(indexer.GetMemTotal(s.UseLiveData()))),
			indexer.GetMemUsage(s.UseLiveData()), formatMemoryStr(uint64(indexer.GetMemUsage(s.UseLiveData()))),
			indexer.GetMemOverhead(s.UseLiveData()), formatMemoryStr(uint64(indexer.GetMemOverhead(s.UseLiveData()))),
			indexer.GetCpuUsage(s.UseLiveData()), len(indexer.Indexes))

		for _, index := range indexer.Indexes {
			logging.Infof("\t\t------------------------------------------------------------------------------------------------------------------")
			logging.Infof("\t\tIndex name:%v, bucket:%v, defnId:%v, instId:%v", index.GetDisplayName(), index.Bucket, index.DefnId, index.InstId)
			logging.Infof("\t\tIndex memory:%v (%s), overhead: %v (%s), cpu:%v",
				index.GetMemUsage(s.UseLiveData()), formatMemoryStr(uint64(index.GetMemUsage(s.UseLiveData()))),
				index.GetMemOverhead(s.UseLiveData()), formatMemoryStr(uint64(index.GetMemOverhead(s.UseLiveData()))), index.GetCpuUsage(s.UseLiveData()))
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
// Compute statistics on index movement
//
func (s *Solution) computeIndexMovement() (uint64, uint64, uint64, uint64) {

	totalSize := uint64(0)
	dataMoved := uint64(0)
	totalIndex := uint64(0)
	indexMoved := uint64(0)

	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {

			// ignore cost of moving an index out of an to-be-deleted node
			if index.initialNode != nil && !index.initialNode.delete {
				totalSize += index.GetMemUsage(s.UseLiveData())
				totalIndex++
			}

			// ignore cost of moving an index out of an to-be-deleted node
			if index.initialNode != nil && !index.initialNode.delete &&
				index.initialNode.NodeId != indexer.NodeId {
				dataMoved += index.GetMemUsage(s.UseLiveData())
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

		if cpu > 0 && mem > 0 {
			cpuTotal += cpu
			memTotal += mem
			count++
		}
	}

	if count > 0 {
		return (memTotal / float64(count)), (cpuTotal / float64(count))
	} else {
		return 0, 0
	}
}

//
// Find node that matches the resource usage requirement.  This function will
// skip over deleted node.
//
func (s *Solution) findNodeWithFreeUsage(memUsage uint64, cpuUsage uint64) *IndexerNode {

	indexers := sortNodeByUsage(s)
	for i := 0; i < len(indexers); i++ {

		indexer := indexers[i]

		if indexer.delete {
			continue
		}

		freeCpu := s.constraint.GetCpuQuota() - indexer.GetCpuUsage(s.UseLiveData())
		freeMem := s.constraint.GetMemQuota() - indexer.GetMemTotal(s.UseLiveData())

		if freeCpu >= cpuUsage && freeMem >= memUsage {
			return indexer
		}
	}

	return nil
}

//
// Find the number of replica or equivalent index (including itself).
//
func (s *Solution) findNumEquivalentIndex(u *IndexUsage) int {

	var count int

	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {

			// check replica
			if index.DefnId == u.DefnId {
				count++

			} else {
				// check equivalent index
				if index.Instance != nil &&
					u.Instance != nil &&
					common.IsEquivalentIndex(&index.Instance.Defn, &u.Instance.Defn) {

					count++
				}
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
			if index.DefnId == u.DefnId {
				count++
			}
		}
	}

	return count
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
// Relax constraint depending on the solution and command.
//
func (s *Solution) RelaxConstraintIfNecessary(eligibles []*IndexUsage) {

	// If not using live data, then no need to relax constraint.
	if !s.UseLiveData() {
		return
	}

	numLiveNode := s.findNumLiveNode()

	// Check to see if it is needed to drop replica from a ejected node
	for _, indexer := range s.Placement {
		deleteCandidates := make(map[*IndexUsage]bool)

		for _, index := range indexer.Indexes {
			if isEligibleIndex(index, eligibles) {

				// if there are more replica than the number of nodes, then
				// do not move this index if this node is going away.  If the
				// node is not going away, then do nothing and let validation
				// fails later.
				if (s.findNumReplica(index) > numLiveNode) && indexer.delete {
					deleteCandidates[index] = true
				}
			}
		}

		if len(deleteCandidates) != 0 {
			keepCandidates := ([]*IndexUsage)(nil)

			for _, index := range indexer.Indexes {
				if !deleteCandidates[index] {
					keepCandidates = append(keepCandidates, index)
				} else {
					logging.Warnf("There is more replia than available nodes.  Will not move index replica (%v,%v) from ejected node %v",
						index.Bucket, index.Name, indexer.NodeId)
				}
			}
			indexer.Indexes = keepCandidates
		}
	}

	// Check to see if need to suppress equivalent index.
	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {

			if isEligibleIndex(index, eligibles) {

				// if there are more equiv idx than number of nodes, then
				// allow placement of this index over equiv index
				// (but not over replica).
				if s.findNumEquivalentIndex(index) > numLiveNode {
					index.suppressEquivIdxCheck = true
				}
			}
		}
	}
}

//
// is this a MOI Cluster?
//
func (s *Solution) isMOICluster() bool {

	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {
			if index.IsMOI {
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
		if indexer.delete {
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
// ignore resource constraint if
// 1) use live data (command == rebalance and live cluster)
// 2) is not a MOI cluster
//
func (s *Solution) ignoreResourceConstraint() bool {

	return s.UseLiveData() && !s.isMOICluster()
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

	var totalIndexMem uint64
	var totalIndexCpu uint64

	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {
			totalIndexMem += index.GetMemTotal(s.UseLiveData())
			totalIndexCpu += index.GetCpuUsage(s.UseLiveData())
		}
	}

	if totalIndexMem > (c.MemQuota * uint64(s.findNumLiveNode())) {
		return errors.New(fmt.Sprintf("Total memory usage of all indexes exceed aggregated memory quota of all indexer nodes"))
	}

	if totalIndexCpu > (c.CpuQuota * uint64(s.findNumLiveNode())) {
		return errors.New(fmt.Sprintf("Total cpu usage of all indexes exceed aggregated cpu quota of all indexer nodes"))
	}

	return nil
}

//
// Return an error with a list of violations
//
func (c *IndexerConstraint) GetViolations(s *Solution, eligibles []*IndexUsage) *Violations {

	violations := &Violations{
		MemQuota: s.getConstraintMethod().GetMemQuota(),
		CpuQuota: s.getConstraintMethod().GetCpuQuota(),
	}

	for _, indexer := range s.Placement {

		// This indexer node does not satisfy constraint
		if !c.SatisfyNodeConstraint(s, indexer, eligibles) {
			for _, index := range eligibles {
				if hasIndex(indexer, index) {

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

						if indexer2.delete {
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
// This function determines if an index can be placed into the given node,
// while satsifying availability and resource constraint.
//
func (c *IndexerConstraint) CanAddIndex(s *Solution, n *IndexerNode, u *IndexUsage) ViolationCode {

	if n.delete {
		return DeleteNodeViolation
	}

	for _, index := range n.Indexes {
		// check replica
		if index.DefnId == u.DefnId {
			return AvailabilityViolation
		}

		// check equivalent index
		if !index.suppressEquivIdxCheck && !u.suppressEquivIdxCheck {
			if index.Instance != nil &&
				u.Instance != nil &&
				common.IsEquivalentIndex(&index.Instance.Defn, &u.Instance.Defn) {
				return AvailabilityViolation
			}
		}
	}

	if s.ignoreResourceConstraint() {
		return NoViolation
	}

	memQuota := c.MemQuota
	cpuQuota := c.CpuQuota

	if c.MaxMemUse != -1 {
		memQuota = memQuota * uint64(c.MaxMemUse) / 100
	}

	if c.MaxCpuUse != -1 {
		cpuQuota = cpuQuota * uint64(c.MaxCpuUse) / 100
	}

	if u.GetMemTotal(s.UseLiveData())+n.GetMemTotal(s.UseLiveData()) > memQuota {
		return ResourceViolation
	}

	if u.GetCpuUsage(s.UseLiveData())+n.GetCpuUsage(s.UseLiveData()) > cpuQuota {
		return ResourceViolation
	}

	return NoViolation
}

//
// This function determines if an index can be swapped with another index in the given node,
// while satsifying availability and resource constraint.
//
func (c *IndexerConstraint) CanSwapIndex(sol *Solution, n *IndexerNode, s *IndexUsage, t *IndexUsage) ViolationCode {

	if n.delete {
		return DeleteNodeViolation
	}

	for _, index := range n.Indexes {
		// check replica
		if index.DefnId == s.DefnId {
			return AvailabilityViolation
		}

		// check equivalent index
		if !index.suppressEquivIdxCheck && !s.suppressEquivIdxCheck {
			if index.Instance != nil &&
				s.Instance != nil &&
				common.IsEquivalentIndex(&index.Instance.Defn, &s.Instance.Defn) {
				return AvailabilityViolation
			}
		}
	}

	if sol.ignoreResourceConstraint() {
		return NoViolation
	}

	memQuota := c.MemQuota
	cpuQuota := c.CpuQuota

	if c.MaxMemUse != -1 {
		memQuota = memQuota * uint64(c.MaxMemUse) / 100
	}

	if c.MaxCpuUse != -1 {
		cpuQuota = cpuQuota * uint64(c.MaxCpuUse) / 100
	}

	if s.GetMemTotal(sol.UseLiveData())+n.GetMemTotal(sol.UseLiveData())-t.GetMemTotal(sol.UseLiveData()) > memQuota {
		return ResourceViolation
	}

	if s.GetCpuUsage(sol.UseLiveData())+n.GetCpuUsage(sol.UseLiveData())-t.GetCpuUsage(sol.UseLiveData()) > cpuQuota {
		return ResourceViolation
	}

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
	cpuQuota := c.CpuQuota

	if c.MaxMemUse != -1 {
		memQuota = memQuota * uint64(c.MaxMemUse) / 100
	}

	if c.MaxCpuUse != -1 {
		cpuQuota = cpuQuota * uint64(c.MaxCpuUse) / 100
	}

	if n.GetMemTotal(s.UseLiveData()) > memQuota {
		return false
	}

	if n.GetCpuUsage(s.UseLiveData()) > cpuQuota {
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
	cpuQuota := c.CpuQuota

	if c.MaxMemUse != -1 {
		memQuota = memQuota * uint64(c.MaxMemUse) / 100
	}

	if c.MaxCpuUse != -1 {
		cpuQuota = cpuQuota * uint64(c.MaxCpuUse) / 100
	}

	for _, indexer := range s.Placement {
		if indexer.GetMemTotal(s.UseLiveData()) > memQuota {
			return false
		}
		if indexer.GetCpuUsage(s.UseLiveData()) > cpuQuota {
			return false
		}
	}

	return true
}

//
// This function determines if a node constraint is satisfied.
//
func (c *IndexerConstraint) SatisfyNodeConstraint(s *Solution, n *IndexerNode, eligibles []*IndexUsage) bool {

	if n.delete && len(n.Indexes) != 0 {
		return false
	}

	checkConstraint := false
	for _, eligible := range eligibles {
		if hasIndex(n, eligible) {
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

	for i := 0; i < len(n.Indexes)-1; i++ {
		for j := i + 1; j < len(n.Indexes); j++ {

			// Ignore any pair of indexes that are not eligible index
			if !isEligibleIndex(n.Indexes[i], eligibles) &&
				!isEligibleIndex(n.Indexes[j], eligibles) {
				continue
			}

			// check replica
			if n.Indexes[i].DefnId == n.Indexes[j].DefnId {
				return false
			}

			// check equivalent index
			if !n.Indexes[i].suppressEquivIdxCheck && !n.Indexes[j].suppressEquivIdxCheck {
				if n.Indexes[i].Instance != nil &&
					n.Indexes[j].Instance != nil &&
					common.IsEquivalentIndex(&n.Indexes[i].Instance.Defn, &n.Indexes[j].Instance.Defn) {
					return false
				}
			}
		}
	}

	return true
}

//
// This function determines if cluster wide constraint is satisifed.
//
func (c *IndexerConstraint) SatisfyClusterConstraint(s *Solution, eligibles []*IndexUsage) bool {

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
// This function makes a copy of a indexer node.
//
func (o *IndexerNode) clone() *IndexerNode {

	r := &IndexerNode{
		NodeId:            o.NodeId,
		NodeUUID:          o.NodeUUID,
		ServerGroup:       o.ServerGroup,
		MemUsage:          o.MemUsage,
		MemOverhead:       o.MemOverhead,
		CpuUsage:          o.CpuUsage,
		DiskUsage:         o.DiskUsage,
		Indexes:           make([]*IndexUsage, len(o.Indexes)),
		delete:            o.delete,
		ActualMemUsage:    o.ActualMemUsage,
		ActualMemOverhead: o.ActualMemOverhead,
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
func (o *IndexerNode) freeUsage(s *Solution, constraint ConstraintMethod) (uint64, uint64) {

	freeMem := constraint.GetMemQuota() - o.GetMemTotal(s.UseLiveData())
	freeCpu := constraint.GetCpuQuota() - o.GetCpuUsage(s.UseLiveData())

	return freeMem, freeCpu
}

//
// Get cpu usage
//
func (o *IndexerNode) GetCpuUsage(useLive bool) uint64 {

	// If using live data, then do not return cpu usage, since there is no reliable way to figure it out.
	if useLive {
		return 0
	}

	return o.CpuUsage
}

//
// Add Cpu
//
func (o *IndexerNode) AddCpuUsage(s *Solution, usage uint64) {

	// If using live data, then do nothing, since we dont't have cpu stats from live cluster.
	if !s.UseLiveData() {
		o.CpuUsage += usage
	}
}

//
// Subtract Cpu
//
func (o *IndexerNode) SubtractCpuUsage(s *Solution, usage uint64) {

	// If using live data, then do nothing, since we dont't have cpu stats from live cluster.
	if !s.UseLiveData() {
		o.CpuUsage -= usage
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
// Add memory
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

//////////////////////////////////////////////////////////////
// IndexUsage
//////////////////////////////////////////////////////////////

//
// This function returns a string representing the index
//
func (o *IndexUsage) String() string {
	return fmt.Sprintf("%v:%v", o.DefnId, o.InstId)
}

//
// This function creates a new index usage
//
func newIndexUsage(defnId common.IndexDefnId, instId common.IndexInstId, name string, bucket string) *IndexUsage {

	return &IndexUsage{DefnId: defnId,
		InstId: instId,
		Name:   name,
		Bucket: bucket,
	}
}

//
// Get cpu usage
//
func (o *IndexUsage) GetCpuUsage(useLive bool) uint64 {

	// If using live data, then do not return cpu usage, since there is no reliable way to figure it out.
	if useLive {
		return 0
	}

	return o.CpuUsage
}

//
// Get memory usage
//
func (o *IndexUsage) GetMemUsage(useLive bool) uint64 {

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

	if useLive {
		return o.ActualMemUsage + o.ActualMemOverhead
	}

	return o.MemUsage + o.MemOverhead
}

func (o *IndexUsage) GetDisplayName() string {

	if o.Instance == nil {
		return o.Name
	}

	return o.Instance.DisplayName()
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
	c.TotalData, c.DataMoved, c.TotalIndex, c.IndexMoved = s.computeIndexMovement()
	c.MemFree, c.CpuFree = s.computeFreeRatio()

	memCost := float64(0)
	cpuCost := float64(0)
	dataCost := float64(0)
	indexCost := float64(0)
	count := 0

	if c.memCostWeight > 0 && c.MemMean != 0 {
		memCost = c.MemStdDev / c.MemMean * c.memCostWeight
		count++
	}

	if c.cpuCostWeight > 0 && c.CpuMean != 0 {
		cpuCost = c.CpuStdDev / c.CpuMean * c.cpuCostWeight
		count++
	}

	if c.dataCostWeight > 0 && c.TotalData != 0 {
		dataCost = float64(c.DataMoved) / float64(c.TotalData) * c.dataCostWeight
		count++
	}

	if c.dataCostWeight > 0 && c.TotalIndex != 0 {
		indexCost = float64(c.IndexMoved) / float64(c.TotalIndex) * c.dataCostWeight
		count++
	}

	logging.Tracef("Planner::cost: mem cost %v cpu cost %v data moved %v index moved %v count %v", memCost, cpuCost, dataCost, indexCost, count)

	return (memCost + cpuCost + dataCost + indexCost) / float64(count)
}

//
// Print statistics
//
func (s *UsageBasedCostMethod) Print() {

	var memUtil float64
	var cpuUtil float64
	var dataMoved float64
	var indexMoved float64

	if s.MemMean != 0 {
		memUtil = float64(s.MemStdDev) / float64(s.MemMean) * 100
	}

	if s.CpuMean != 0 {
		cpuUtil = float64(s.CpuStdDev) / float64(s.CpuMean) * 100
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
	logging.Infof("Indexer CPU Mean %v", s.CpuMean)
	logging.Infof("Indexer CPU Deviation %v (%.2f%%)", s.CpuStdDev, cpuUtil)
	logging.Infof("Indexer CPU Utilization %.4f", float64(s.CpuMean)/float64(s.constraint.GetCpuQuota()))
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
func newRandomPlacement(indexes []*IndexUsage, allowSwap bool) *RandomPlacement {
	p := &RandomPlacement{
		rs:        rand.New(rand.NewSource(time.Now().UnixNano())),
		indexes:   make(map[*IndexUsage]*IndexUsage),
		eligibles: make([]*IndexUsage, len(indexes)),
		allowSwap: allowSwap,
	}

	// index to be balanced
	for i, index := range indexes {
		p.indexes[index] = index
		p.eligibles[i] = index
	}

	return p
}

//
// Get index for placement
//
func (p *RandomPlacement) GetEligibleIndexes() []*IndexUsage {

	return p.eligibles
}

//
// Validate
//
func (p *RandomPlacement) Validate(s *Solution) error {

	if s.ignoreResourceConstraint() {
		return nil
	}

	memQuota := s.getConstraintMethod().GetMemQuota()
	cpuQuota := s.getConstraintMethod().GetCpuQuota()

	for _, index := range p.indexes {

		if index.GetMemTotal(s.UseLiveData()) > memQuota || index.GetCpuUsage(s.UseLiveData()) > cpuQuota {
			return errors.New(fmt.Sprintf("Index exceeding quota. Index=%v Bucket=%v Memory=%v Cpu=%v MemoryQuota=%v CpuQuota=%v",
				index.GetDisplayName(), index.Bucket, index.GetMemTotal(s.UseLiveData()), index.GetCpuUsage(s.UseLiveData()), s.getConstraintMethod().GetMemQuota(),
				s.getConstraintMethod().GetCpuQuota()))
		}

		if !s.getConstraintMethod().CanAddNode(s) && s.findNumEquivalentIndex(index) > s.findNumLiveNode() {
			return errors.New(fmt.Sprintf("Index has more replica (or equivalent index) than indexer nodes. Index=%v Bucket=%v",
				index.GetDisplayName(), index.Bucket))
		}

		found := false
		for _, indexer := range s.Placement {
			freeMem := s.getConstraintMethod().GetMemQuota()
			freeCpu := s.getConstraintMethod().GetCpuQuota()

			for _, index2 := range indexer.Indexes {
				if !p.isEligibleIndex(index2) {
					freeMem -= index2.GetMemTotal(s.UseLiveData())
					freeCpu -= index2.GetCpuUsage(s.UseLiveData())
				}
			}

			if freeMem >= index.GetMemTotal(s.UseLiveData()) && freeCpu >= index.GetCpuUsage(s.UseLiveData()) {
				found = true
				break
			}
		}

		if !found {
			return errors.New(fmt.Sprintf("Cannot find an indexer with enough free memory or cpu for index. Index=%v Bucket=%v",
				index.GetDisplayName(), index.Bucket))
		}
	}

	return nil
}

//
// Randomly select a single index to move to a different node
//
func (p *RandomPlacement) Move(s *Solution) (bool, bool) {

	if len(p.eligibles) == 0 {
		return false, true
	}

	if p.swapDeleteNode(s) {
		s.removeEmptyDeletedNode()
		return true, false
	}

	success, final := p.randomMoveByLoad(s, true)
	if success {
		s.removeEmptyDeletedNode()
	}

	return success, final
}

//
// If there is delete node, try to see if there is an indexer
// node that can host all the indexes for that delete node.
//
func (p *RandomPlacement) swapDeleteNode(s *Solution) bool {

	result := false

	outNodes := s.getDeleteNodes()
	for _, outNode := range outNodes {

		memUsage := outNode.GetMemTotal(s.UseLiveData())
		cpuUsage := outNode.GetCpuUsage(s.UseLiveData())

		indexer := s.findNodeWithFreeUsage(memUsage, cpuUsage)

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
// Remove Eligible Index
//
func (p *RandomPlacement) removeEligibleIndex(indexes []*IndexUsage) {

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
// Randomly select a single index to move to a different node
//
func (p *RandomPlacement) randomMoveByLoad(s *Solution, checkConstraint bool) (bool, bool) {

	numOfIndexers := len(s.Placement)
	if numOfIndexers == 1 {
		// only one indexer
		return false, false
	}

	// Find a set of candidates (indexer node) that has eligible index
	// From the set of candidates, find those that are under resource constraint.
	// Compute the loads for every constrained candidate
	candidates := p.findCandidates(s)
	logging.Tracef("Planner::candidates: len=%v, %v", len(candidates), candidates)
	constrained := p.findConstrainedNodes(s, s.constraint, candidates)
	logging.Tracef("Planner::constrained: len=%v, %v", len(constrained), constrained)
	loads, total := computeLoads(s, constrained)

	retryCount := numOfIndexers * 10
	for i := 0; i < retryCount; i++ {

		// If there is one node that does not satisfy constriant,
		if len(constrained) == 1 {
			if !s.constraint.CanAddNode(s) {
				// If planner is working on a fixed cluster, then
				// try exhaustively moving or swapping indexes away from this node.
				if p.exhaustiveMove(s, constrained, s.Placement, checkConstraint) {
					return true, false
				}

				if p.exhaustiveSwap(s, constrained, candidates, checkConstraint) {
					return true, false
				}

				// if we cannot find a solution after exhaustively trying to swap or move
				// index in the last constrained node, then we possibly cannot reach a
				// solution.
				return false, true
			} else {
				// If planner can grow the cluster, then just try to randomly swap.
				// If cannot swap, then logic fall through to move index.
				if p.randomSwap(s, constrained, candidates, checkConstraint) {
					return true, false
				}
			}
		}

		// Select an constrained candidate based on weighted probability
		// The most constrained candidate has a higher probabilty to be selected.
		source := getWeightedRandomNode(p.rs, constrained, loads, total)

		// If cannot find a constrained candidate, then try to randomly
		// pick two candidates and try to swap their indexes.
		if source == nil {

			n := int64(p.rs.Int63n(2))
			switch n {
			case 0:
				if p.randomSwap(s, candidates, candidates, checkConstraint) {
					return true, false
				}
			default:
			}

			// If swap fails, then randomly select a candidate as source.
			source = getRandomNode(p.rs, candidates)
			if source == nil {
				return false, false
			}
		}

		// From the candidate, randomly select a movable index.
		index := p.getRandomEligibleIndex(p.rs, source.Indexes)
		if index == nil {
			continue
		}

		// Select an uncongested indexer which is different from source.
		// The most uncongested indexer has a higher probability to be selected.
		target := p.getRandomUncongestedNodeExcluding(s, source)
		if target == nil {
			// if cannot find a uncongested indexer, then check if there is only
			// one candidate and it satisfy resource constraint.  If so, there is
			// no more move (final state).
			eligibles := p.GetEligibleIndexes()
			if len(candidates) == 1 && s.constraint.SatisfyNodeConstraint(s, source, eligibles) {
				logging.Tracef("Planner::final move: source %v index %v", source.NodeId, index)
				return true, true
			}

			logging.Tracef("Planner::no target : index %v mem %v cpu %v source %v",
				index, formatMemoryStr(index.GetMemTotal(s.UseLiveData())), index.GetCpuUsage(s.UseLiveData()), source.NodeId)

			// There could be more candidates, pick another one.
			continue
		}

		logging.Tracef("Planner::try move: index %v mem %v cpu %v source %v target %v",
			index, formatMemoryStr(index.GetMemTotal(s.UseLiveData())), index.GetCpuUsage(s.UseLiveData()), source.NodeId, target.NodeId)

		// See if the index can be moved while obeying resource constraint.
		violation := s.constraint.CanAddIndex(s, target, index)
		if !checkConstraint || violation == NoViolation {
			logging.Tracef("Planner::move: source %v index %v target %v checkConstraint %v",
				source.NodeId, index, target.NodeId, checkConstraint)
			s.moveIndex(source, index, target)
			return true, false

		} else {
			logging.Tracef("Planner::try move fail: violation %s", violation)
		}
	}

	if logging.IsEnabled(logging.Trace) {
		for _, indexer := range s.Placement {
			logging.Tracef("Planner::no move: indexer %v mem %v cpu %v",
				indexer.NodeId, formatMemoryStr(indexer.GetMemTotal(s.UseLiveData())), indexer.GetCpuUsage(s.UseLiveData()))
		}
	}

	// Give it one more try to swap constrained node
	return p.randomSwap(s, constrained, candidates, checkConstraint), false
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

		_, _, _, indexMoved := s.computeIndexMovement()
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

	// only include node with index to be rebalanced
	for _, indexer := range s.Placement {
		for _, index := range p.indexes {
			if hasIndex(indexer, index) {
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
func (p *RandomPlacement) getRandomUncongestedNodeExcluding(s *Solution, exclude *IndexerNode) *IndexerNode {

	indexers := ([]*IndexerNode)(nil)

	for _, indexer := range s.Placement {
		if exclude.NodeId != indexer.NodeId && s.constraint.SatisfyNodeResourceConstraint(s, indexer) && !indexer.delete {
			indexers = append(indexers, indexer)
		}
	}

	total := int64(0)
	loads := make([]int64, len(indexers))
	for i, indexer := range indexers {
		loads[i] = int64(computeIndexerFreeQuota(s, indexer) * 100)
		total += loads[i]
	}

	logging.Tracef("Planner::uncongested: %v loads %v total %v", indexers, loads, total)

	if total > 0 {
		n := int64(p.rs.Int63n(total))

		for i, load := range loads {
			if n <= load {
				return indexers[i]
			} else {
				n -= load
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
func (p *RandomPlacement) Add(s *Solution, indexes []*IndexUsage) {

	for _, idx := range indexes {
		indexer := getRandomNode(p.rs, s.Placement)
		s.addIndex(indexer, idx)
	}
}

//
// This function randomly place indexes among indexer nodes for initial placement
//
func (p *RandomPlacement) InitialPlace(s *Solution, indexes []*IndexUsage) {

	for _, idx := range indexes {
		indexer := getRandomNode(p.rs, s.Placement)
		s.addIndex(indexer, idx)
		idx.initialNode = indexer
	}
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

		sourceIndex := p.getRandomEligibleIndex(p.rs, source.Indexes)
		targetIndex := p.getRandomEligibleIndex(p.rs, target.Indexes)

		if sourceIndex == nil || targetIndex == nil {
			continue
		}

		logging.Tracef("Planner::try swap: source index %v (mem %v cpu %v) target index %v (mem %v cpu %v) source %v target %v",
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
			logging.Tracef("Planner::no swap: indexer %v mem %v cpu %v",
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

			shuffledTargets := shuffleNode(p.rs, targets)
			logging.Tracef("Planner::exhaustive swap: targets after shuffled len=%v, %v", len(shuffledTargets), shuffledTargets)

			for _, target := range shuffledTargets {

				if source.NodeId == target.NodeId || target.delete {
					continue
				}

				shuffledTargetIndexes := shuffleIndex(p.rs, target.Indexes)
				logging.Tracef("Planner::exhaustive swap: target index after shuffle len=%v, %v", len(shuffledTargetIndexes), shuffledTargetIndexes)

				for _, targetIndex := range shuffledTargetIndexes {

					if !p.isEligibleIndex(targetIndex) {
						continue
					}

					if sourceIndex.GetMemTotal(s.UseLiveData()) >= targetIndex.GetMemTotal(s.UseLiveData()) &&
						sourceIndex.GetCpuUsage(s.UseLiveData()) >= targetIndex.GetCpuUsage(s.UseLiveData()) {

						targetViolation := s.constraint.CanSwapIndex(s, target, sourceIndex, targetIndex)
						sourceViolation := s.constraint.CanSwapIndex(s, source, targetIndex, sourceIndex)

						logging.Tracef("Planner::try exhaustive swap: source index %v (mem %v cpu %v) target index %v (mem %v cpu %v) source %v target %v",
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
func (p *RandomPlacement) exhaustiveMove(s *Solution, sources []*IndexerNode, targets []*IndexerNode, checkConstraint bool) bool {

	for _, source := range sources {

		shuffledSourceIndexes := shuffleIndex(p.rs, source.Indexes)
		logging.Tracef("Planner::exhaustive move: source index after shuffle len=%v, %v", len(shuffledSourceIndexes), shuffledSourceIndexes)

		for _, sourceIndex := range shuffledSourceIndexes {

			if !p.isEligibleIndex(sourceIndex) {
				continue
			}

			shuffledTargets := shuffleNode(p.rs, targets)
			logging.Tracef("Planner::exhaustive move: targets after shuffled len=%v, %v", len(shuffledTargets), shuffledTargets)

			for _, target := range shuffledTargets {

				if source.NodeId == target.NodeId || target.delete {
					continue
				}

				logging.Tracef("Planner::try exhaustive move: index %v mem %v cpu %v source %v target %v",
					sourceIndex, formatMemoryStr(sourceIndex.GetMemTotal(s.UseLiveData())), sourceIndex.GetCpuUsage(s.UseLiveData()),
					source.NodeId, target.NodeId)

				// See if the index can be moved while obeying resource constraint.
				violation := s.constraint.CanAddIndex(s, target, sourceIndex)
				if !checkConstraint || violation == NoViolation {
					logging.Tracef("Planner::exhaustive move: source %v index %v target %v checkConstraint %v",
						source.NodeId, sourceIndex, target.NodeId, checkConstraint)
					s.moveIndex(source, sourceIndex, target)
					return true

				} else {
					logging.Tracef("Planner::try exhaustive move fail: violation %s", violation)
				}
			}
		}
	}

	return false
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
	eligibles := p.GetEligibleIndexes()
	for _, indexer := range indexers {
		if !constraint.SatisfyNodeConstraint(s, indexer, eligibles) {
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
// MOISizingMethod
//////////////////////////////////////////////////////////////

//
// Constructor
//
func newMOISizingMethod() *MOISizingMethod {
	return &MOISizingMethod{}
}

//
// Validate
//
func (s *MOISizingMethod) Validate(solution *Solution) error {

	// If using cpu/mem usage from live cluster, no need to validate.
	if solution.UseLiveData() {
		return nil
	}

	for _, indexer := range solution.Placement {
		for _, index := range indexer.Indexes {
			if !index.IsMOI {
				return errors.New(fmt.Sprintf("Planner does not support non-MOI index. Index=%v Bucket=%v", index.GetDisplayName(), index.Bucket))
			}
		}
	}

	return nil
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
			idx.MemUsage = (120 + idx.AvgSecKeySize + idx.AvgDocKeySize) * idx.NumOfDocs
		} else if idx.AvgArrKeySize != 0 {
			// secondary array index mem size : (46 + (74 + DocIdLen + ArrElemSize) * NumArrElems) * NumberOfItems
			idx.MemUsage = (46 + (74+idx.AvgArrKeySize+idx.AvgDocKeySize)*idx.AvgArrSize) * idx.NumOfDocs
		} else if idx.ActualKeySize != 0 {
			// secondary index mem size : (46 + ActualKeySize) * NumberOfItems
			idx.MemUsage = (46 + idx.ActualKeySize) * idx.NumOfDocs
		}
	} else {
		if idx.AvgDocKeySize != 0 {
			// primary index mem size : (74 + DocIdLen) * NumberOfItems
			idx.MemUsage = (74 + idx.AvgDocKeySize) * idx.NumOfDocs
		} else if idx.ActualKeySize != 0 {
			// primary index mem size : ActualKeySize * NumberOfItems
			idx.MemUsage = idx.ActualKeySize * idx.NumOfDocs
		}
	}

	// compute cpu usage
	cpu := float64(idx.MutationRate)/float64(MOIMutationRatePerCore) + float64(idx.ScanRate)/float64(MOIScanRatePerCore)
	idx.CpuUsage = uint64(math.Floor(cpu)) + 1

	idx.MemOverhead = s.ComputeIndexOverhead(idx)
}

//
// This function computes the indexer memory and cpu usage
//
func (s *MOISizingMethod) ComputeIndexerSize(o *IndexerNode) {

	o.MemUsage = 0
	o.CpuUsage = 0

	for _, idx := range o.Indexes {
		o.MemUsage += idx.MemUsage
		o.CpuUsage += idx.CpuUsage
	}

	s.ComputeIndexerOverhead(o)
}

//
// This function computes the indexer memory overhead
//
func (s *MOISizingMethod) ComputeIndexerOverhead(o *IndexerNode) {

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

//
// This function estimates the min memory quota given a set of indexes
//
func (s *MOISizingMethod) ComputeMinQuota(indexes []*IndexUsage, useLive bool) (uint64, uint64) {

	maxCpuUsage := uint64(0)
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
	cpuQuota := maxCpuUsage

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
