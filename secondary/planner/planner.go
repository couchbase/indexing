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
	Move(s *Solution) (bool, bool, bool)
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

// Indexer Node is a description of one index node used by Planner to track which indexes are
// planned to reside on this node.
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
	ActualDiskUsage   uint64  `json:"actualDiskUsage"`
	ActualDrainRate   uint64  `json:"actualDrainRate"`
	ActualScanRate    uint64  `json:"actualScanRate"`
	ActualMemMin      uint64  `json:"actualMemMin"`

	// input: index residing on the node
	Indexes []*IndexUsage `json:"indexes"`

	// input: node status
	isDelete bool
	isNew    bool
	Exclude  string

	// input/output: planning
	meetConstraint bool
	numEmptyIndex  int
	hasEligible    bool
	totalData      uint64
	totalIndex     uint64
	dataMovedIn    uint64
	indexMovedIn   uint64

	UsageRatio float64 `json:"usageRatio"`
}

// IndexUsage is a description of one instance of an index used by Planner to keep track of which
// indexes are planned to be on which nodes.
type IndexUsage struct {
	// input: index identification
	DefnId     common.IndexDefnId `json:"defnId"`
	InstId     common.IndexInstId `json:"instId"`
	PartnId    common.PartitionId `json:"partnId"`
	Name       string             `json:"name"`
	Bucket     string             `json:"bucket"`
	Scope      string             `json:"scope"`
	Collection string             `json:"collection"`
	Hosts      []string           `json:"host"`

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
	DrainRate     uint64  `json:"drainRate"`
	ScanRate      uint64  `json:"scanRate"`

	// input: resource consumption (from sizing equation)
	MemUsage    uint64  `json:"memUsage"`
	CpuUsage    float64 `json:"cpuUsage"`
	DiskUsage   uint64  `json:"diskUsage,omitempty"`
	MemOverhead uint64  `json:"memOverhead,omitempty"`
	DataSize    uint64  `json:"dataSize,omitempty"`

	// input: resource consumption (from live cluster)
	ActualMemUsage     uint64  `json:"actualMemUsage"`
	ActualMemOverhead  uint64  `json:"actualMemOverhead"`
	ActualKeySize      uint64  `json:"actualKeySize"`
	ActualCpuUsage     float64 `json:"actualCpuUsage"`
	ActualBuildPercent uint64  `json:"actualBuildPercent"`
	// in planner for plasma we use combined resident percent from mainstore as well as backstore
	// so the value of stat "combined_resident_percent will be stored in ActualResidentPercent"
	ActualResidentPercent uint64 `json:"actualResidentPercent"`
	ActualDataSize        uint64 `json:"actualDataSize"`
	ActualNumDocs         uint64 `json:"actualNumDocs"`
	ActualDiskUsage       uint64 `json:"actualDiskUsage"`
	ActualMemStats        uint64 `json:"actualMemStats"`
	ActualDrainRate       uint64 `json:"actualDrainRate"`
	ActualScanRate        uint64 `json:"actualScanRate"`
	ActualMemMin          uint64 `json:"actualMemMin"`

	// input: resource consumption (estimated sizing)
	NoUsageInfo       bool   `json:"NoUsageInfo"`
	EstimatedMemUsage uint64 `json:"estimatedMemUsage"`
	EstimatedDataSize uint64 `json:"estimatedDataSize"`
	NeedsEstimate     bool   `json:"NeedsEstimate"`

	// input: index definition (optional)
	Instance *common.IndexInst `json:"instance,omitempty"`

	// input: node where index initially placed (optional)
	// for new indexes to be placed on an existing topology (e.g. live cluster), this must not be set.
	initialNode *IndexerNode

	// The node to which planner moves this index from initialNode
	// Equals "initialNode" if planner does not move the index
	destNode *IndexerNode

	// input: flag to indicate if the index in delete or create token
	pendingDelete bool // true if there is a delete token associated with this index
	pendingCreate bool // true if there is a create token associated with this index
	pendingBuild  bool // true if there is a build token associated with this index

	// mutable: hint for placement / constraint
	suppressEquivIdxCheck bool

	// Copy of the index state from the corresponding index instance.
	state common.IndexState

	// Miscellaneous fields
	partnStatPrefix string
	instStatPrefix  string

	eligible        bool
	overrideExclude bool

	// stats for duplicate index removal logic
	numDocsQueued    int64
	numDocsPending   int64
	rollbackTime     int64
	progressStatTime int64
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

	// for resource utilization
	memMean   float64
	cpuMean   float64
	dataMean  float64
	diskMean  float64
	drainMean float64
	scanMean  float64

	// placement of indexes	in nodes
	Placement []*IndexerNode `json:"placement,omitempty"`

	// maps for non-eligible indexes
	indexSGMap       ServerGroupMap
	replicaMap       ReplicaMap
	usedReplicaIdMap UsedReplicaIdMap

	// maps for eligible indexes
	// with large number of indexes, cloning these maps after every planner
	// iteration becomes a bottleneck. As planner only moves the list of
	// eligible indexes, maps of non-eligible indexes will not change and
	// clone method can just copy the reference to the existing map.
	eligIndexSGMap       ServerGroupMap
	eligReplicaMap       ReplicaMap
	eligUsedReplicaIdMap UsedReplicaIdMap

	// constraint check
	enforceConstraint bool

	// flag for ignoring constraint check during heterogenous swap
	// rebalance while upgrading
	swapOnlyRebalance bool

	// When set to 1, planner ignores excludeNode params as well as resource
	// contraints during DDL operations
	allowDDLDuringScaleUp bool
}

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

//
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
//
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
}

//////////////////////////////////////////////////////////////
// Interface Implementation - CostMethod
//////////////////////////////////////////////////////////////

type UsageBasedCostMethod struct {
	MemMean        float64 `json:"memMean,omitempty"`
	MemStdDev      float64 `json:"memStdDev,omitempty"`
	CpuMean        float64 `json:"cpuMean,omitempty"`
	CpuStdDev      float64 `json:"cpuStdDev,omitempty"`
	DiskMean       float64 `json:"diskMean,omitempty"`
	DiskStdDev     float64 `json:"diskStdDev,omitempty"`
	DrainMean      float64 `json:"drainMean,omitempty"`
	DrainStdDev    float64 `json:"drainStdDev,omitempty"`
	ScanMean       float64 `json:"scanMean,omitempty"`
	ScanStdDev     float64 `json:"scanStdDev,omitempty"`
	DataSizeMean   float64 `json:"dataSizeMean,omitempty"`
	DataSizeStdDev float64 `json:"dataSizeStdDev,omitempty"`
	TotalData      uint64  `json:"totalData,omitempty"`
	DataMoved      uint64  `json:"dataMoved,omitempty"`
	TotalIndex     uint64  `json:"totalIndex,omitempty"`
	IndexMoved     uint64  `json:"indexMoved,omitempty"`
	constraint     ConstraintMethod
	dataCostWeight float64
	cpuCostWeight  float64
	memCostWeight  float64
}

//////////////////////////////////////////////////////////////
// Interface Implementation - PlacementMethod
//////////////////////////////////////////////////////////////

type RandomPlacement struct {
	// rs is a random number generator.
	rs *rand.Rand

	// indexes is all existing and potentially to be recreated (i.e. missing replicas) indexes,
	// mapped to whether they are "eligible" indexes (i.e. can be moved or placed by the plan). Each
	// key will also have an entry in either the eligibles or optionals slice fields.
	// ATTENTION: indexes is often passed into a constraint checking method argument called
	//   "eligibles" which can lead to confusion between it and this struct's eligibles field.
	indexes map[*IndexUsage]bool

	// eligibles is all existing indexes that can be moved by the plan. This is all existing indexes
	// in the Rebalance case but only the new index in the Create Index case. These indexes are also
	// included in the indexes map field.
	eligibles []*IndexUsage

	// optionals is the missing replicas (of whole indexes or partitions) the plan will try to
	// recreate and place. It may not recreate them if it has trouble finding a valid plan. These
	// indexes are also included in the indexes map field.
	optionals []*IndexUsage

	allowSwap       bool
	swapDeletedOnly bool

	// stats
	totalIteration     int
	randomSwapCnt      int
	randomSwapDur      int64
	randomSwapRetry    int
	randomMoveCnt      int
	randomMoveEmptyCnt int
	randomMoveDur      int64
	exhaustSwapCnt     int
	exhaustSwapDur     int64
	exhaustMoveCnt     int
	exhaustMoveDur     int64
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

	if p.cpuProfile {
		startCPUProfile("planner.pprof")
		defer stopCPUProfile()
	}

	var result *Solution
	var err error
	var violations *Violations

	solution.command = command
	solution = p.adjustInitialSolutionIfNecessary(solution)
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

		logging.Infof("Planner::Fail to create plan satisfying constraint. Re-planning. Num of Try=%v.  Elapsed Time=%v",
			p.Try, formatTimeStr(uint64(time.Now().Sub(startTime).Nanoseconds())))

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
		logging.Errorf(err.Error())
	}

	return result, err
}

//
// Given a solution, this function finds a replica to drop while
// maintaining HA constraint.
//
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

//
// Given a solution, this function use simulated annealing
// to find an alternative solution with a lower cost.
//
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

			new_solution, force, final := p.findNeighbor(current)
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
	logging.Infof("variation: %v", p.cost.ComputeResourceVariation())
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
// This function finds a neighbor placement layout using
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

	logging.Infof("Planner::initial temperature: initial cost %v temp %v", cost, temp)
	return temp
}

//
// Get the initial temperature.
//
func (p *SAPlanner) initialTemperatureFromResourceVariation(command CommandType, resourceVariation float64) float64 {

	if command == CommandPlan {
		return MaxTemperature
	}

	temp := math.Max(MinTemperature, math.Abs(resourceVariation-p.threshold)*0.1)

	logging.Infof("Planner::initial temperature: initial resource variation %v temp %v", resourceVariation, temp)
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
	// Low probability when
	// 1) low temperature (many iterations have passed)
	// 2) difference between new_cost and old_cost are high
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

	// Make sure we only repair when it is rebalancing
	if s.command == CommandRebalance || s.command == CommandSwap || s.command == CommandRepair {
		p.addReplicaIfNecessary(cloned)

		if s.command == CommandRebalance || s.command == CommandSwap {
			p.addPartitionIfNecessary(cloned)
			p.dropReplicaIfNecessary(cloned)
		}
	}
	p.suppressEqivIndexIfNecessary(cloned)
	cloned.generateReplicaMap()

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
				logging.Warnf("There is more replica than available nodes.  Will not move index replica (%v,%v,%v,%v) from ejected node %v",
					index.Bucket, index.Scope, index.Collection, index.Name, indexer.NodeId)

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
					logging.Warnf("There are more equivalent index than available nodes.  Allow equivalent index of (%v, %v, %v, %v) to be replaced on same node.",
						index.Bucket, index.Scope, index.Collection, index.Name)
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

			if index.Instance != nil && missingReplica > 0 && !index.pendingDelete {
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

						indexer := indexers[0]
						indexers = indexers[1:]

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
							cloned.overrideExclude = true

							// add the new replica to the solution
							s.addIndex(indexer, cloned, false)

							clonedCandidates = append(clonedCandidates, cloned)
							numReplica++

							logging.Infof("Rebuilding lost replica for (%v,%v,%v,%v,%v)",
								index.Bucket, index.Scope, index.Collection, index.Name, replicaId)
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
			if !index.pendingDelete {
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
// Solution
//////////////////////////////////////////////////////////////

//
// Constructor
//
func newSolution(constraint ConstraintMethod, sizing SizingMethod, indexers []*IndexerNode, isLive bool, useLive bool,
	disableRepair bool, allowDDLDuringScaleUp bool) *Solution {

	r := &Solution{
		constraint:            constraint,
		sizing:                sizing,
		Placement:             make([]*IndexerNode, len(indexers)),
		isLiveData:            isLive,
		useLiveData:           useLive,
		disableRepair:         disableRepair,
		estimate:              true,
		enableExclude:         true,
		indexSGMap:            make(ServerGroupMap),
		replicaMap:            make(ReplicaMap),
		usedReplicaIdMap:      make(UsedReplicaIdMap),
		eligIndexSGMap:        make(ServerGroupMap),
		eligReplicaMap:        make(ReplicaMap),
		eligUsedReplicaIdMap:  make(UsedReplicaIdMap),
		swapOnlyRebalance:     false,
		allowDDLDuringScaleUp: allowDDLDuringScaleUp,
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
// Get the constraint method for the solution
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
func (s *Solution) moveIndex(source *IndexerNode, idx *IndexUsage, target *IndexerNode, meetConstraint bool) {

	sourceIndex := s.findIndexOffset(source, idx)
	if sourceIndex == -1 {
		return
	}

	// add to new node
	s.addIndex(target, idx, meetConstraint)

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
func (s *Solution) addIndex(n *IndexerNode, idx *IndexUsage, meetConstraint bool) {
	n.EvaluateNodeConstraint(s, meetConstraint, idx, nil)
	n.Indexes = append(n.Indexes, idx)

	n.AddMemUsageOverhead(s, idx.GetMemUsage(s.UseLiveData()), idx.GetMemOverhead(s.UseLiveData()), idx.GetMemMin(s.UseLiveData()))
	n.AddCpuUsage(s, idx.GetCpuUsage(s.UseLiveData()))
	n.AddDataSize(s, idx.GetDataSize(s.UseLiveData()))
	n.AddDiskUsage(s, idx.GetDiskUsage(s.UseLiveData()))
	n.AddScanRate(s, idx.GetScanRate(s.UseLiveData()))
	n.AddDrainRate(s, idx.GetDrainRate(s.UseLiveData()))

	n.EvaluateNodeStats(s)
	s.updateServerGroupMap(idx, n)
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

	n.SubtractMemUsageOverhead(s, idx.GetMemUsage(s.UseLiveData()), idx.GetMemOverhead(s.UseLiveData()), idx.GetMemMin(s.UseLiveData()))
	n.SubtractCpuUsage(s, idx.GetCpuUsage(s.UseLiveData()))
	n.SubtractDataSize(s, idx.GetDataSize(s.UseLiveData()))
	n.SubtractDiskUsage(s, idx.GetDiskUsage(s.UseLiveData()))
	n.SubtractScanRate(s, idx.GetScanRate(s.UseLiveData()))
	n.SubtractDrainRate(s, idx.GetDrainRate(s.UseLiveData()))

	n.EvaluateNodeConstraint(s, false, nil, idx)
	n.EvaluateNodeStats(s)
}

//
// Remove indexes in topology
//
func (s *Solution) removeIndexes(indexes []*IndexUsage) {

	eligibles := s.place.GetEligibleIndexes()

	for _, target := range indexes {
		for _, indexer := range s.Placement {
			for i, index := range indexer.Indexes {
				if index == target {
					s.removeIndex(indexer, i)
					s.updateServerGroupMap(index, nil)
					if isEligibleIndex(index, eligibles) {
						if index.Instance != nil {
							delete(s.eligReplicaMap[index.DefnId][index.PartnId], index.Instance.ReplicaId)
						}
					} else {
						if index.Instance != nil {
							delete(s.replicaMap[index.DefnId][index.PartnId], index.Instance.ReplicaId)
						}
					}
				}
			}
		}
	}
}

//
// Remove replica
//
func (s *Solution) removeReplicas(defnId common.IndexDefnId, replicaId int) {

	eligibles := s.place.GetEligibleIndexes()

	for _, indexer := range s.Placement {
	RETRY:
		for i, index := range indexer.Indexes {
			if index.DefnId == defnId && int(index.Instance.ReplicaId) == replicaId {
				s.removeIndex(indexer, i)
				s.updateServerGroupMap(index, nil)
				if isEligibleIndex(index, eligibles) {
					if index.Instance != nil {
						delete(s.eligReplicaMap[index.DefnId][index.PartnId], index.Instance.ReplicaId)
					}
				} else {
					if index.Instance != nil {
						delete(s.replicaMap[index.DefnId][index.PartnId], index.Instance.ReplicaId)
					}
				}
				goto RETRY
			}
		}
	}
}

//
// Reduce minimum memory
//
func (s *Solution) reduceMinimumMemory() {

	for _, indexer := range s.Placement {
		var total uint64
		for _, index := range indexer.Indexes {
			if !index.NoUsageInfo {
				index.ActualMemMin = uint64(float64(index.ActualMemMin) * 0.9)
				total += index.ActualMemMin
			}
		}
		indexer.ActualMemMin = total
	}
}

//
// This function makes a copy of existing solution.
//
func (s *Solution) clone() *Solution {

	r := solutionPool.Get()

	r.command = s.command
	r.constraint = s.constraint
	r.sizing = s.sizing
	r.cost = s.cost
	r.place = s.place
	r.Placement = make([]*IndexerNode, 0, len(s.Placement))
	r.isLiveData = s.isLiveData
	r.useLiveData = s.useLiveData
	r.initialPlan = s.initialPlan
	r.numServerGroup = s.numServerGroup
	r.numDeletedNode = s.numDeletedNode
	r.numNewNode = s.numNewNode
	r.disableRepair = s.disableRepair
	r.estimatedIndexSize = s.estimatedIndexSize
	r.estimate = s.estimate
	r.numEstimateRun = s.numEstimateRun
	r.enableExclude = s.enableExclude
	r.memMean = s.memMean
	r.cpuMean = s.cpuMean
	r.dataMean = s.dataMean
	r.diskMean = s.diskMean
	r.scanMean = s.scanMean
	r.drainMean = s.drainMean
	r.enforceConstraint = s.enforceConstraint
	r.swapOnlyRebalance = s.swapOnlyRebalance
	r.indexSGMap = s.indexSGMap
	r.replicaMap = s.replicaMap
	r.usedReplicaIdMap = s.usedReplicaIdMap
	r.eligIndexSGMap = make(ServerGroupMap)
	r.eligReplicaMap = make(ReplicaMap)
	r.eligUsedReplicaIdMap = make(UsedReplicaIdMap)
	r.allowDDLDuringScaleUp = s.allowDDLDuringScaleUp

	for _, node := range s.Placement {
		if node.isDelete && len(node.Indexes) == 0 {
			continue
		}
		r.Placement = append(r.Placement, node.clone())
	}

	for defnId, partitions := range s.eligIndexSGMap {
		r.eligIndexSGMap[defnId] = make(map[common.PartitionId]map[int]string)
		for partnId, replicaIds := range partitions {
			r.eligIndexSGMap[defnId][partnId] = make(map[int]string)
			for replicaId, sg := range replicaIds {
				r.eligIndexSGMap[defnId][partnId][replicaId] = sg
			}
		}
	}

	for defnId, partitions := range s.eligReplicaMap {
		r.eligReplicaMap[defnId] = make(map[common.PartitionId]map[int]*IndexUsage)
		for partnId, replicaIds := range partitions {
			r.eligReplicaMap[defnId][partnId] = make(map[int]*IndexUsage)
			for replicaId, index := range replicaIds {
				r.eligReplicaMap[defnId][partnId][replicaId] = index
			}
		}
	}

	for defnId, replicaIds := range s.eligUsedReplicaIdMap {
		r.eligUsedReplicaIdMap[defnId] = make(map[int]bool)
		for replicaId, _ := range replicaIds {
			r.eligUsedReplicaIdMap[defnId][replicaId] = true
		}
	}

	return r
}

func (s *Solution) free() {

	for _, node := range s.Placement {
		node.free()
	}

	solutionPool.Put(s)
}

//
// This function update the cost stats
//
func (s *Solution) updateCost() {
	if s.cost != nil {
		s.memMean = s.cost.GetMemMean()
		s.cpuMean = s.cost.GetCpuMean()
		s.dataMean = s.cost.GetDataMean()
		s.diskMean = s.cost.GetDiskMean()
		s.scanMean = s.cost.GetScanMean()
		s.drainMean = s.cost.GetDrainMean()
	}
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
		result = append(result, node)
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

//find list of new nodes i.e. nodes without any index
func (s *Solution) getNewNodes() []*IndexerNode {

	result := ([]*IndexerNode)(nil)
	for _, indexer := range s.Placement {
		if indexer.isNew {
			result = append(result, indexer)
		}
	}

	return result
}

//
// When set to true, this flag ignores excludeNode params, as well as
// planner resource constraints during DDL operations.
//
func (s *Solution) AllowDDLDuringScaleUp() bool {
	return s.allowDDLDuringScaleUp
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
		logging.Infof("Indexer total memory:%v (%s), mem:%v (%s), overhead:%v (%s), min:%v (%s), data:%v (%s) cpu:%.4f, io:%v (%s), scan:%v drain:%v numIndexes:%v",
			indexer.GetMemTotal(s.UseLiveData()), formatMemoryStr(uint64(indexer.GetMemTotal(s.UseLiveData()))),
			indexer.GetMemUsage(s.UseLiveData()), formatMemoryStr(uint64(indexer.GetMemUsage(s.UseLiveData()))),
			indexer.GetMemOverhead(s.UseLiveData()), formatMemoryStr(uint64(indexer.GetMemOverhead(s.UseLiveData()))),
			indexer.GetMemMin(s.UseLiveData()), formatMemoryStr(uint64(indexer.GetMemMin(s.UseLiveData()))),
			indexer.GetDataSize(s.UseLiveData()), formatMemoryStr(uint64(indexer.GetDataSize(s.UseLiveData()))),
			indexer.GetCpuUsage(s.UseLiveData()),
			indexer.GetDiskUsage(s.UseLiveData()), formatMemoryStr(uint64(indexer.GetDiskUsage(s.UseLiveData()))),
			indexer.GetScanRate(s.UseLiveData()), indexer.GetDrainRate(s.UseLiveData()),
			len(indexer.Indexes))
		logging.Infof("Indexer isDeleted:%v isNew:%v exclude:%v meetConstraint:%v usageRatio:%v allowDDLDuringScaleup:%v" ,
			indexer.IsDeleted(), indexer.isNew, indexer.Exclude, indexer.meetConstraint, s.computeUsageRatio(indexer), s.AllowDDLDuringScaleUp())
		for _, index := range indexer.Indexes {
			logging.Infof("\t\t------------------------------------------------------------------------------------------------------------------")
			logging.Infof("\t\tIndex name:%v, bucket:%v, scope:%v, collection:%v, defnId:%v, instId:%v, Partition: %v, new/moved:%v",
				index.GetDisplayName(), index.Bucket, index.Scope, index.Collection, index.DefnId, index.InstId, index.PartnId,
				index.initialNode == nil || index.initialNode.NodeId != indexer.NodeId)
			logging.Infof("\t\tIndex total memory:%v (%s), mem:%v (%s), overhead:%v (%s), min:%v (%s), data:%v (%s) cpu:%.4f io:%v (%s) scan:%v drain:%v",
				index.GetMemTotal(s.UseLiveData()), formatMemoryStr(uint64(index.GetMemTotal(s.UseLiveData()))),
				index.GetMemUsage(s.UseLiveData()), formatMemoryStr(uint64(index.GetMemUsage(s.UseLiveData()))),
				index.GetMemOverhead(s.UseLiveData()), formatMemoryStr(uint64(index.GetMemOverhead(s.UseLiveData()))),
				index.GetMemMin(s.UseLiveData()), formatMemoryStr(uint64(index.GetMemMin(s.UseLiveData()))),
				index.GetDataSize(s.UseLiveData()), formatMemoryStr(uint64(index.GetDataSize(s.UseLiveData()))),
				index.GetCpuUsage(s.UseLiveData()),
				index.GetDiskUsage(s.UseLiveData()), formatMemoryStr(uint64(index.GetDiskUsage(s.UseLiveData()))),
				index.GetScanRate(s.UseLiveData()), index.GetDrainRate(s.UseLiveData()))
			logging.Infof("\t\tIndex resident:%v%% build:%v%% estimated:%v equivCheck:%v pendingCreate:%v pendingDelete:%v",
				uint64(index.GetResidentRatio(s.UseLiveData())),
				index.GetBuildPercent(s.UseLiveData()),
				index.NeedsEstimation() && index.HasSizing(s.UseLiveData()),
				!index.suppressEquivIdxCheck,
				index.pendingCreate, index.pendingDelete)
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
		meanMemUsage += float64(indexerUsage.GetMemUsage(s.UseLiveData()))
	}
	meanMemUsage = meanMemUsage / float64(len(s.Placement))

	// compute memory variance
	var varianceMemUsage float64
	for _, indexerUsage := range s.Placement {
		v := float64(indexerUsage.GetMemUsage(s.UseLiveData())) - meanMemUsage
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
// Compute statistics on disk Usage
//
func (s *Solution) ComputeDiskUsage() (float64, float64) {

	// Compute mean disk usage
	var meanDiskUsage float64
	for _, indexerUsage := range s.Placement {
		meanDiskUsage += float64(indexerUsage.GetDiskUsage(s.UseLiveData()))
	}
	meanDiskUsage = meanDiskUsage / float64(len(s.Placement))

	// compute disk variance
	var varianceDiskUsage float64
	for _, indexerUsage := range s.Placement {
		v := float64(indexerUsage.GetDiskUsage(s.UseLiveData())) - meanDiskUsage
		varianceDiskUsage += v * v
	}
	varianceDiskUsage = varianceDiskUsage / float64(len(s.Placement))

	// compute disk std dev
	stdDevDiskUsage := math.Sqrt(varianceDiskUsage)

	return meanDiskUsage, stdDevDiskUsage
}

//
// Compute statistics on drain rate
//
func (s *Solution) ComputeDrainRate() (float64, float64) {

	// Compute mean drain rate
	var meanDrainRate float64
	for _, indexerUsage := range s.Placement {
		meanDrainRate += float64(indexerUsage.GetDrainRate(s.UseLiveData()))
	}
	meanDrainRate = meanDrainRate / float64(len(s.Placement))

	// compute drain rate variance
	var varianceDrainRate float64
	for _, indexerUsage := range s.Placement {
		v := float64(indexerUsage.GetDrainRate(s.UseLiveData())) - meanDrainRate
		varianceDrainRate += v * v
	}
	varianceDrainRate = varianceDrainRate / float64(len(s.Placement))

	// compute drain rate std dev
	stdDevDrainRate := math.Sqrt(varianceDrainRate)

	return meanDrainRate, stdDevDrainRate
}

//
// Compute statistics on scan rate
//
func (s *Solution) ComputeScanRate() (float64, float64) {

	// Compute mean scan rate
	var meanScanRate float64
	for _, indexerUsage := range s.Placement {
		meanScanRate += float64(indexerUsage.GetScanRate(s.UseLiveData()))
	}
	meanScanRate = meanScanRate / float64(len(s.Placement))

	// compute scan rate variance
	var varianceScanRate float64
	for _, indexerUsage := range s.Placement {
		v := float64(indexerUsage.GetScanRate(s.UseLiveData())) - meanScanRate
		varianceScanRate += v * v
	}
	varianceScanRate = varianceScanRate / float64(len(s.Placement))

	// compute scan rate std dev
	stdDevScanRate := math.Sqrt(varianceScanRate)

	return meanScanRate, stdDevScanRate
}

//
// Compute statistics on number of index. This only consider
// index that has no stats or sizing information.
//
func (s *Solution) ComputeEmptyIndexDistribution() (float64, float64) {

	// Compute mean number of index
	var meanIdxUsage float64
	for _, indexer := range s.Placement {
		meanIdxUsage += float64(indexer.numEmptyIndex)
	}
	meanIdxUsage = meanIdxUsage / float64(len(s.Placement))

	// compute variance on number of index
	var varianceIdxUsage float64
	for _, indexer := range s.Placement {
		v := float64(indexer.numEmptyIndex) - meanIdxUsage
		varianceIdxUsage += v * v
	}
	varianceIdxUsage = varianceIdxUsage / float64(len(s.Placement))

	// compute std dev on number of index
	stdDevIdxUsage := math.Sqrt(varianceIdxUsage)

	return meanIdxUsage, stdDevIdxUsage
}

//
// This function returns the residual memory after subtracting ONLY the estimated empty index memory.
// The cost function tries to maximize the total memory of empty index (or minimize
// the residual memory after subtracting empty index).
// The residual memory is further calibrated by empty index usage ratio (the total empty index memory over the total memory)
// If there is no empty index, this function returns 0.
//
func (s *Solution) ComputeCapacityAfterEmptyIndex() float64 {

	max := s.computeMaxMemUsage()
	if max < s.constraint.GetMemQuota() {
		max = s.constraint.GetMemQuota()
	}

	var emptyIdxCnt uint64
	var totalIdxCnt uint64
	var emptyIdxMem uint64

	for _, indexer := range s.Placement {
		emptyIdxMem += indexer.computeFreeMemPerEmptyIndex(s, max, 0) * uint64(indexer.numEmptyIndex)
		emptyIdxCnt += uint64(indexer.numEmptyIndex)
		totalIdxCnt += uint64(len(indexer.Indexes))
	}

	maxTotal := max * uint64(len(s.Placement))
	usageAfterEmptyIdx := float64(maxTotal-emptyIdxMem) / float64(maxTotal)
	weight := float64(emptyIdxCnt) / float64(totalIdxCnt)

	return usageAfterEmptyIdx * weight
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
// Compute statistics on index movement
//
func (s *Solution) computeIndexMovement(useNewNode bool) (uint64, uint64, uint64, uint64) {

	totalSize := uint64(0)
	dataMoved := uint64(0)
	totalIndex := uint64(0)
	indexMoved := uint64(0)

	if s.place != nil {
		for _, indexer := range s.Placement {
			totalSize += indexer.totalData
			dataMoved += indexer.dataMovedIn
			totalIndex += indexer.totalIndex
			indexMoved += indexer.indexMovedIn
		}
	} else {
		for _, indexer := range s.Placement {

			// ignore cost moving to a new node
			if !useNewNode && indexer.isNew {
				continue
			}

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
	}

	return totalSize, dataMoved, totalIndex, indexMoved
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
	instances := make(map[common.IndexInstId]int)
	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {

			// check replica for each partition (including self)
			if index.IsReplica(u) {
				if index.Instance == nil {
					logging.Warnf("Cannot determine replicaId for index (%v,%v,%v,%v)", index.Name, index.Bucket, index.Scope, index.Collection)
					return (map[int]common.IndexInstId)(nil)
				}
				found[index.Instance.ReplicaId] = index.InstId
			}

			// check instance/replica for the index definition (including self)
			if index.IsSameIndex(u) {
				if index.Instance == nil {
					logging.Warnf("Cannot determine replicaId for index (%v,%v,%v,%v)", index.Name, index.Bucket, index.Scope, index.Collection)
					return (map[int]common.IndexInstId)(nil)
				}
				instances[index.InstId] = index.Instance.ReplicaId
			}
		}
	}

	// replicaId starts with 0
	// numReplica excludes itself
	missing := make(map[int]common.IndexInstId)
	if u.Instance != nil {
		// Going through all the instances of this definition (across partitions).
		// Find if any replica is missing for this partition.
		for instId, replicaId := range instances {
			if _, ok := found[replicaId]; !ok {
				missing[replicaId] = instId
			}
		}

		add := int(u.Instance.Defn.NumReplica+1) - len(missing) - len(found)
		if add > 0 {
			for k := 0; k < add; k++ {
				for i := 0; i < int(math.MaxUint32); i++ {
					_, ok1 := found[i]
					_, ok2 := missing[i]
					_, ok3 := s.usedReplicaIdMap[u.DefnId][i]
					_, ok4 := s.eligUsedReplicaIdMap[u.DefnId][i]

					if !ok1 && !ok2 && !ok3 && !ok4 {
						missing[i] = 0
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
// This function recalculates the index and indexer sizes based on sizing formula.
// Data captured from live cluster will not be overwritten.
//
func (s *Solution) calculateSize() {

	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {
			index.ComputeSizing(s.UseLiveData(), s.sizing)
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

// find list of nodes that have node.Exclude = "out" set
func (s *Solution) getExcludedOutNodes() []*IndexerNode {

	result := ([]*IndexerNode)(nil)
	for _, indexer := range s.Placement {
		if indexer.Exclude == "out" {
			result = append(result, indexer)
		}
	}

	return result
}

//
// Find num of empty node
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
// find number of available live node (excluding ejected node and excluded node)
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
// Evaluate if each indexer meets constraint
//
func (s *Solution) evaluateNodes() {
	for _, indexer := range s.Placement {
		indexer.Evaluate(s)
	}
}

//
// check to see if we should ignore resource (memory/cpu) constraint
//
func (s *Solution) ignoreResourceConstraint() bool {

	// always honor resource constraint when doing simulation
	if !s.UseLiveData() {
		return false
	}

	// if size estimation is turned on, then honor the constraint since it needs to find the best fit under the
	// assumption that new indexes are going to fit under constraint.
	if s.command == CommandPlan {
		if s.canRunEstimation() {
			return false
		}
	}

	// TODO: Add checks for DDL during a transient heterogenous state in swap-only rebalance
	// Check for Swap-only Rebalance during upgrade
	if s.swapOnlyRebalance {
		return true
	}

	return !s.enforceConstraint
}

//
// check to see if every node in the cluster meet constraint
//
func (s *Solution) SatisfyClusterConstraint() bool {

	for _, indexer := range s.Placement {
		if !indexer.SatisfyNodeConstraint() {
			return false
		}
	}

	return true
}

//
// findServerGroup gets called only if solution.numServerGroup > 1
//
func (s *Solution) findServerGroup(defnId common.IndexDefnId, partnId common.PartitionId, replicaId int) (string, bool) {

	findSG := func(sgMap ServerGroupMap) (string, bool) {
		partitions, ok := sgMap[defnId]
		if !ok {
			return "", false
		}

		replicas, ok := partitions[partnId]
		if !ok {
			return "", false
		}

		sg, ok := replicas[replicaId]
		return sg, ok
	}

	if sg, ok := findSG(s.eligIndexSGMap); ok {
		return sg, ok
	}

	return findSG(s.indexSGMap)
}

//
// Check if there is any replica (excluding self) in the server group
//
// hasReplicaInServerGroup gets called only if solution.numServerGroup > 1
//
func (s *Solution) hasReplicaInServerGroup(u *IndexUsage, group string) bool {

	hasReplicaInSG := func(replicaM ReplicaMap) bool {

		if u.Instance != nil {
			replicas := replicaM[u.DefnId][u.PartnId]
			for i, _ := range replicas {
				if i != u.Instance.ReplicaId {
					if sg, ok := s.findServerGroup(u.DefnId, u.PartnId, i); ok {
						if sg == group {
							return true
						}
					}
				}
			}
		} else {
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
		}

		return false
	}

	if hasReplicaInSG(s.eligReplicaMap) {
		return true
	}

	return hasReplicaInSG(s.replicaMap)
}

//
// Get server groups having replicas for this index (excluding self)
//
// getServerGroupsWithReplica gets called only if solution.numServerGroup > 1
//
func (s *Solution) getServerGroupsWithReplica(u *IndexUsage) map[string]bool {

	getSGWithReplica := func(replicaM ReplicaMap) map[string]bool {

		counts := make(map[string]bool)
		if u.Instance != nil {
			replicas := replicaM[u.DefnId][u.PartnId]
			for i, _ := range replicas {
				if i != u.Instance.ReplicaId {
					if group, ok := s.findServerGroup(u.DefnId, u.PartnId, i); ok {
						counts[group] = true
					}
				}
			}
		} else {
			for _, indexer := range s.Placement {
				if indexer.isDelete {
					continue
				}

				for _, index := range indexer.Indexes {
					if index != u && index.IsReplica(u) { // replica
						counts[indexer.ServerGroup] = true
					}
				}
			}
		}
		return counts
	}

	groups := getSGWithReplica(s.replicaMap)
	for k, _ := range getSGWithReplica(s.eligReplicaMap) {
		groups[k] = true
	}

	return groups
}

//
// Check if any server group without this replica (excluding self)
//
// hasServerGroupWithNoReplica gets called only if solution.numServerGroup > 1
//
func (s *Solution) hasServerGroupWithNoReplica(u *IndexUsage) bool {

	if s.numServerGroup > len(s.getServerGroupsWithReplica(u)) {
		return true
	}

	return false
}

//
// For a given index, find out all replica with the lower replicaId.
// The given index should have the higher replica count in its server
// group than all its replica (with lower replicaId).
//
/*
func (s *Solution) hasHighestReplicaCountInServerGroup(u *IndexUsage) bool {

	if !s.place.IsEligibleIndex(u) {
		return true
	}

	counts := make(map[string]int)
	replicaIds := make([]int)
	for i, _ := range s.replicaMap[u.DefnId][u.PartnId] {
		if i < u.Instance.ReplicaId {
			if sg, ok := s.findServerGroup(u.DefnId, u.PartnId, i); ok {
				counts[group]++
				replicaIds = append(replicas, i)
			}
		}
	}

	idxSG := s.findServerGroup(u.DefnId, u.PartnId, u.Instance.ReplicaId)
	for i := 0; i < len(replicaIds); i++ {
		replica := s.replicaMap[u.DefnId][u.PartnId][i]
		if s.place.IsEligibleIndex(replica) {
			sg := s.findServerGroup(u.DefnId, u.PartnId, i)

			if counts[idxSG] < counts[sg] {
				return false
			}
		}
	}

	return true
}
*/

//
// Does the index node has replica?
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
func (s *Solution) FindIndexerWithReplica(name, bucket, scope, collection string,
	partnId common.PartitionId, replicaId int) *IndexerNode {

	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {
			if index.Name == name &&
				index.Bucket == bucket &&
				index.Scope == scope &&
				index.Collection == collection &&
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
// Find possible targets to place lost replicas.
// Returns the exact list of nodes which can be directly used to
// place the lost replicas.
//
func (s *Solution) FindNodesForReplicaRepair(source *IndexUsage, numNewReplica int, useExcludeNode bool) []*IndexerNode {
	// TODO: Make this function aware of equivalent indexes.

	rs := rand.New(rand.NewSource(time.Now().UnixNano()))

	nodes := shuffleNode(rs, s.Placement)

	// TODO: Need to evaluate the cost of calculating replica and SG maps
	// before replica repair.
	replicaNodes := make(map[string]bool)
	replicaServerGroups := make(map[string]bool)

	result := make([]*IndexerNode, 0, len(nodes))
	for _, indexer := range nodes {
		if indexer.IsDeleted() {
			continue
		}

		if !useExcludeNode && indexer.ExcludeIn(s) {
			continue
		}

		found := false
		for _, index := range indexer.Indexes {
			if source.IsReplica(index) {
				found = true
				replicaNodes[indexer.IndexerId] = true
				replicaServerGroups[indexer.ServerGroup] = true
				break
			}
		}

		if !found {
			result = append(result, indexer)
		}
	}

	// If there are more nodes than the number of missing replica, then prioritize the nodes based
	// on server group.
	if len(result) > numNewReplica {
		final := make([]*IndexerNode, 0, len(result))

		// Add a node from each server group that does not have the index
		for _, indexer := range result {
			if numNewReplica <= 0 {
				break
			}

			if _, ok := replicaServerGroups[indexer.ServerGroup]; !ok {
				final = append(final, indexer)
				replicaNodes[indexer.IndexerId] = true
				replicaServerGroups[indexer.ServerGroup] = true
				numNewReplica--
			}
		}

		// Still need more node.  Add nodes from server group that already has the index.
		for _, indexer := range result {
			if numNewReplica <= 0 {
				break
			}

			if _, ok := replicaNodes[indexer.IndexerId]; !ok {
				final = append(final, indexer)
				replicaNodes[indexer.IndexerId] = true
				replicaServerGroups[indexer.ServerGroup] = true
				numNewReplica--
			}
		}

		return final
	}

	return result
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
// Update SG mapping for index
//
func (s *Solution) updateServerGroupMap(index *IndexUsage, indexer *IndexerNode) {

	if s.numServerGroup <= 1 {
		return
	}

	updateSGMap := func(sgMap ServerGroupMap) {
		if index.Instance != nil {
			if indexer != nil {
				if _, ok := sgMap[index.DefnId]; !ok {
					sgMap[index.DefnId] = make(map[common.PartitionId]map[int]string)
				}

				if _, ok := sgMap[index.DefnId][index.PartnId]; !ok {
					sgMap[index.DefnId][index.PartnId] = make(map[int]string)
				}

				sgMap[index.DefnId][index.PartnId][index.Instance.ReplicaId] = indexer.ServerGroup
			} else {
				if _, ok := sgMap[index.DefnId]; ok {
					if _, ok := sgMap[index.DefnId][index.PartnId]; ok {
						delete(sgMap[index.DefnId][index.PartnId], index.Instance.ReplicaId)
					}
				}
			}
		}
	}

	if index.eligible {
		updateSGMap(s.eligIndexSGMap)
	} else {
		if s.place != nil {
			eligibles := s.place.GetEligibleIndexes()
			if isEligibleIndex(index, eligibles) {
				updateSGMap(s.eligIndexSGMap)
			} else {
				updateSGMap(s.indexSGMap)
			}
		} else {
			// Assume that it is not an eligible index
			updateSGMap(s.indexSGMap)
		}
	}
}

//
// initialize SG mapping for index
//
func (s *Solution) initializeServerGroupMap() {
	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {
			s.updateServerGroupMap(index, indexer)
		}
	}
}

//
// Generate a map for replicaId
//
func (s *Solution) generateReplicaMap() {

	eligibles := s.place.GetEligibleIndexes()

	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {
			s.updateReplicaMap(index, indexer, eligibles)
		}
	}
}

func (s *Solution) updateReplicaMap(index *IndexUsage, indexer *IndexerNode, eligibles map[*IndexUsage]bool) {
	var replicaM ReplicaMap
	if isEligibleIndex(index, eligibles) {
		replicaM = s.eligReplicaMap
	} else {
		replicaM = s.replicaMap
	}

	if index.Instance != nil {
		if _, ok := replicaM[index.DefnId]; !ok {
			replicaM[index.DefnId] = make(map[common.PartitionId]map[int]*IndexUsage)
		}

		if _, ok := replicaM[index.DefnId][index.PartnId]; !ok {
			replicaM[index.DefnId][index.PartnId] = make(map[int]*IndexUsage)
		}

		replicaM[index.DefnId][index.PartnId][index.Instance.ReplicaId] = index
	}
}

//
// NoUsageInfo index is index without any sizing information.
// For placement, planner still want to layout noUsage index such that
// 1) indexer with more free memory will take up more partitions.
// 2) partitions should be spread out as much as possible
// 3) non-partition index have different weight than individual partition of a partitioned index
//
// It is impossible to achieve accurate number for (1) and (3) without sizing info (even with sizing,
// it is only an approximation).    But if we make 2 assumptions, we can provide estimated sizing of partitions:
// 1) the new index will take up the remaining free memory of all indexers
// 2) the partition of new index are equal size
//
// To estimate NoUsageInfo index size based on free memory available.
// 1) calculates total free memory from indexers which has less than 80% memory usage
// 2) Based on total free memory, estimates index size based on number of noUsage index.
// 3) Estimate the partition size
//    - non-partitioned index has 1 partition, so it will take up full index size
//    - individual partition of a partitioned index will take up (index-size / numPartition)
// 4) Given the estimated size, if cannot find a placement solution, reduce index size by 10% and repeat (4)
//
// Behavior:
// 1) Using ConstraintMethod, indexes/Partitions will more likely to move to indexers with more free memory
// 2) Using CostMethod, partitions will likely to spread out to minimize memory variation
//
// Empty Index:
// When an index is empty, it can have DataSize and MemUsage as zero (Actual or otherwise). Note that
// this can happen with MOI index created on a bucket which never had any mutations). For such indexes,
// the index state will be ACTIVE. So, there is no need to estimate the size of such indexes.
// But the planner cannot work with zero sized indexes. And the above mentioned strategy (i.e. index will
// take up the remaining free memory of all indexers) will not work - as it misrepresent the actual
// size of the index. So, size of the empty index will be estimated to some small constant value.
//
// Caveats:
// 1) When placing the new index onto a cluster with existing NoUsageInfo (deferred) index, the existing
//    index will also need to be sized.   The existing index is assumed to have the similar size as
//    the new index.   Therefore, the estimated index size represents the "average index size" for
//    all unsized indexes.
//
func (s *Solution) runSizeEstimation(placement PlacementMethod, newInstCount int) {

	estimate := func(estimatedIndexSize uint64) {
		for _, indexer := range s.Placement {

			// excludeIn - indexes cannot moved into this node.  So effectively, this indexer is taken out of cluster.
			if indexer.ExcludeIn(s) {
				continue
			}

			for _, index := range indexer.Indexes {
				if index.NeedsEstimation() {
					if index.state == common.INDEX_STATE_ACTIVE {
						index.EstimatedDataSize = EMPTY_INDEX_DATASIZE
						index.EstimatedMemUsage = EMPTY_INDEX_MEMUSAGE
					} else {
						if index.Instance != nil && index.Instance.Pc != nil && common.IsPartitioned(index.Instance.Defn.PartitionScheme) {
							index.EstimatedMemUsage = estimatedIndexSize / uint64(index.Instance.Pc.GetNumPartitions())
							index.EstimatedDataSize = index.EstimatedMemUsage
						} else {
							index.EstimatedMemUsage = estimatedIndexSize
							index.EstimatedDataSize = index.EstimatedMemUsage
						}
					}

					indexer.AddMemUsageOverhead(s, index.EstimatedMemUsage, 0, index.EstimatedMemUsage)
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

	// Only enable estimation if eligible indexes have no sizing info.
	// Do not turn on estimation if eligible has sizing info, but
	// there are existing deferred index.   Turn it on may cause
	// new index not being able to place properly, since existing
	// deferred index may use up available memory during estimation.
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
			if index.NeedsEstimation() {
				insts[index.InstId] = index.Instance
			}
		}
	}

	//
	// calculate total free memory
	//
	threshold := float64(0.2)
	max := float64(s.computeMaxMemUsage())
	if max < float64(s.constraint.GetMemQuota()) {
		max = float64(s.constraint.GetMemQuota())
	}

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
		freeMem := max - float64(indexer.GetMemTotal(s.UseLiveData()))
		freeMemRatio := freeMem / float64(max)
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
		s.estimatedIndexSize = totalMemFree / uint64(len(insts)+newInstCount)

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
			if index.NeedsEstimation() {
				indexer.SubtractMemUsageOverhead(s, index.EstimatedMemUsage, 0, index.EstimatedMemUsage)
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

//
// compute average usage for
// 1) memory usage
// 2) cpu
// 3) data
// 4) io
// 5) drain rate
// 6) scan rate
//
func (s *Solution) computeUsageRatio(indexer *IndexerNode) float64 {

	memCost := float64(0)
	//cpuCost := float64(0)
	dataCost := float64(0)
	diskCost := float64(0)
	drainCost := float64(0)
	scanCost := float64(0)

	if s.memMean != 0 {
		memCost = float64(indexer.GetMemUsage(s.UseLiveData())) / s.memMean
	}

	//if s.cpuMean != 0 {
	//	cpuCost = float64(indexer.GetCpuUsage(s.UseLiveData())) / s.cpuMean
	//}

	if s.dataMean != 0 {
		dataCost = float64(indexer.GetDataSize(s.UseLiveData())) / s.dataMean
	}

	if s.diskMean != 0 {
		diskCost = float64(indexer.GetDiskUsage(s.UseLiveData())) / s.diskMean
	}

	if s.drainMean != 0 {
		drainCost = float64(indexer.GetDrainRate(s.UseLiveData())) / s.drainMean
	}

	if s.scanMean != 0 {
		scanCost = float64(indexer.GetScanRate(s.UseLiveData())) / s.scanMean
	}

	//return (memCost + cpuCost + dataCost + diskCost + drainCost + scanCost) / 6
	return (memCost + dataCost + diskCost + drainCost + scanCost) / 5
}

//
// compute average mean usage
// 1) memory usage
// 2) cpu
// 3) data
// 4) io
// 5) drain rate
// 6) scan rate
//
func (s *Solution) computeMeanUsageRatio() float64 {

	//return 6
	return 5
}

func (s *Solution) computeMaxMemUsage() uint64 {

	max := uint64(0)
	for _, indexer := range s.Placement {
		if indexer.GetMemTotal(s.UseLiveData()) > max {
			max = indexer.GetMemTotal(s.UseLiveData())
		}
	}

	return max
}

func (s *Solution) getReplicas(defnId common.IndexDefnId) map[common.PartitionId]map[int]*IndexUsage {
	getReps := func(replicaM ReplicaMap) map[common.PartitionId]map[int]*IndexUsage {
		reps := make(map[common.PartitionId]map[int]*IndexUsage)
		for partnId, rep := range replicaM[defnId] {
			reps[partnId] = make(map[int]*IndexUsage)
			for rid, index := range rep {
				reps[partnId][rid] = index
			}
		}
		return reps
	}

	reps := getReps(s.replicaMap)
	for partnId, rep := range getReps(s.eligReplicaMap) {
		if _, ok := reps[partnId]; !ok {
			reps[partnId] = make(map[int]*IndexUsage)
		}

		for rid, index := range rep {
			reps[partnId][rid] = index
		}
	}

	return reps
}

func (s *Solution) demoteEligIndexes(indexes []*IndexUsage) {

	for _, index := range indexes {
		s.demoteEligIndex(index)
	}
}

func (s *Solution) demoteEligIndex(index *IndexUsage) {

	index.eligible = false

	// Step 1: If present remove index from elig maps
	// Step 2: If index was present in elig maps, add them to non-elig maps

	// In case of partition/replica repair, the index.Instance won't not be nil.
	if index.Instance == nil {
		return
	}

	// Transfer from Server Group Map
	func() {
		sg, ok := s.eligIndexSGMap[index.DefnId][index.PartnId][index.Instance.ReplicaId]
		if !ok {
			return
		}

		delete(s.eligIndexSGMap[index.DefnId][index.PartnId], index.Instance.ReplicaId)

		if _, ok := s.indexSGMap[index.DefnId]; !ok {
			s.indexSGMap[index.DefnId] = make(map[common.PartitionId]map[int]string)
		}

		if _, ok := s.indexSGMap[index.DefnId][index.PartnId]; !ok {
			s.indexSGMap[index.DefnId][index.PartnId] = make(map[int]string)
		}

		s.indexSGMap[index.DefnId][index.PartnId][index.Instance.ReplicaId] = sg
	}()

	// Transfer from Replica Map
	func() {
		idx, ok := s.eligReplicaMap[index.DefnId][index.PartnId][index.Instance.ReplicaId]
		if !ok {
			return
		}

		delete(s.eligReplicaMap[index.DefnId][index.PartnId], index.Instance.ReplicaId)

		if _, ok := s.replicaMap[index.DefnId]; !ok {
			s.replicaMap[index.DefnId] = make(map[common.PartitionId]map[int]*IndexUsage)
		}

		if _, ok := s.replicaMap[index.DefnId][index.PartnId]; !ok {
			s.replicaMap[index.DefnId][index.PartnId] = make(map[int]*IndexUsage)
		}

		s.replicaMap[index.DefnId][index.PartnId][index.Instance.ReplicaId] = idx
	}()

	// Transfer from Used ReplicaId Map
	func() {
		used, ok := s.eligUsedReplicaIdMap[index.DefnId][index.Instance.ReplicaId]
		if !ok {
			return
		}

		delete(s.eligUsedReplicaIdMap[index.DefnId], index.Instance.ReplicaId)

		if _, ok := s.usedReplicaIdMap[index.DefnId]; !ok {
			s.usedReplicaIdMap[index.DefnId] = make(map[int]bool)
		}

		s.usedReplicaIdMap[index.DefnId][index.Instance.ReplicaId] = used
	}()
}

// Check for Swap-only Rebalance
// We mark if cluster has
// 1. only 1 add node OR only 1 Node with ExcludeNode="out" set with no add Nodes
// 2. 1 deleted node with Node.Exclude="in"
// 3. and the remaining nodes have ExcludeNode="inout"
func (s *Solution) markSwapOnlyRebalance() {

	s.swapOnlyRebalance = false
	if s.command == CommandRebalance && s.numDeletedNode == 1 {
		scaledNode := (*IndexerNode)(nil)
		deletedNode := s.getDeleteNodes()[0]
		if s.numNewNode == 1 {
			scaledNode = s.getNewNodes()[0]
		} else if s.numNewNode == 0 {
			excludedOutNodes := s.getExcludedOutNodes()
			if len(excludedOutNodes) == 1 && excludedOutNodes[0] != deletedNode {
				scaledNode = excludedOutNodes[0]
			} else {
				return
			}
		}

		if scaledNode != nil && deletedNode.Exclude == "in" {
			inoutEnabledOnRemaining := true
			for _, node := range s.Placement {
				if node.NodeId == deletedNode.NodeId || node.NodeId == scaledNode.NodeId {
					continue
				}
				if node.Exclude != "inout" {
					inoutEnabledOnRemaining = false
					break
				}
			}
			if inoutEnabledOnRemaining {
				s.swapOnlyRebalance = true
				return
			}
		}
	}
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
	//var totalIndexCpu float64

	for _, indexer := range s.Placement {
		for _, index := range indexer.Indexes {
			totalIndexMem += index.GetMemMin(s.UseLiveData())
			//totalIndexCpu += index.GetCpuUsage(s.UseLiveData())
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

			satisfyResourceConstraint := c.SatisfyNodeResourceConstraint(s, indexer)

			for _, index := range indexer.Indexes {
				if isEligibleIndex(index, eligibles) {

					if !c.acceptViolation(s, index, indexer) {
						continue
					}

					if satisfyResourceConstraint && c.SatisfyIndexHAConstraint(s, indexer, index, eligibles, true) {
						continue
					}

					violation := &Violation{
						Name:       index.GetDisplayName(),
						Bucket:     index.Bucket,
						Scope:      index.Scope,
						Collection: index.Collection,
						NodeId:     indexer.NodeId,
						MemUsage:   index.GetMemMin(s.UseLiveData()),
						CpuUsage:   index.GetCpuUsage(s.UseLiveData()),
						Details:    nil}

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

	// There are replica in this server group. Check to see if there are any server group without this index (excluding self).
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
// while satisfying availability and resource constraint.
//
func (c *IndexerConstraint) CanAddIndex(s *Solution, n *IndexerNode, u *IndexUsage) ViolationCode {

	if n.shouldExcludeIndex(s, u) {
		return ExcludeNodeViolation
	}

	if n.isDelete {
		return DeleteNodeViolation
	}

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

	if u.GetMemMin(s.UseLiveData())+n.GetMemMin(s.UseLiveData()) > memQuota {
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
// while satisfying availability and resource constraint.
//
func (c *IndexerConstraint) CanSwapIndex(sol *Solution, n *IndexerNode, s *IndexUsage, t *IndexUsage) ViolationCode {

	if n.shouldExcludeIndex(sol, s) {
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

	if s.GetMemMin(sol.UseLiveData())+n.GetMemMin(sol.UseLiveData())-t.GetMemMin(sol.UseLiveData()) > memQuota {
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

	if n.GetMemMin(s.UseLiveData()) > memQuota {
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
func (c *IndexerConstraint) SatisfyNodeHAConstraint(s *Solution, n *IndexerNode, eligibles map[*IndexUsage]bool, chkEquiv bool) bool {

	for offset, index := range n.Indexes {
		if !c.SatisfyIndexHAConstraintAt(s, n, offset+1, index, eligibles, chkEquiv) {
			return false
		}
	}

	return true
}

//
// This function determines if a HA constraint is satisfied for a particular index in indexer node.
//
func (c *IndexerConstraint) SatisfyIndexHAConstraint(s *Solution, n *IndexerNode, source *IndexUsage, eligibles map[*IndexUsage]bool, chkEquiv bool) bool {

	return c.SatisfyIndexHAConstraintAt(s, n, 0, source, eligibles, chkEquiv)
}

func (c *IndexerConstraint) SatisfyIndexHAConstraintAt(s *Solution, n *IndexerNode, offset int, source *IndexUsage, eligibles map[*IndexUsage]bool, chkEquiv bool) bool {

	if n.isDelete {
		return false
	}

	length := len(n.Indexes)
	for i := offset; i < length; i++ {
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

		if chkEquiv {
			// check equivalent index
			if index.IsEquivalentIndex(source, true) {
				return false
			}
		}
	}

	// Are replica in the same server group?
	if isEligibleIndex(source, eligibles) && !c.SatisfyServerGroupConstraint(s, source, n.ServerGroup) {
		return false
	}

	return true
}

//
// This function determines if cluster wide constraint is satisfied.
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
		if indexer.GetMemMin(s.UseLiveData()) > memQuota {
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

	return c.SatisfyNodeHAConstraint(s, n, eligibles, true)
}

//
// This function determines if cluster wide constraint is satisfied.
//
func (c *IndexerConstraint) SatisfyClusterConstraint(s *Solution, eligibles map[*IndexUsage]bool) bool {

	for _, indexer := range s.Placement {
		if !c.SatisfyNodeConstraint(s, indexer, eligibles) {
			return false
		}
	}

	return true
}

//
// This function determines if cluster wide constraint is satisfied.
// This function ignores the resource constraint.
//
func (c *IndexerConstraint) SatisfyClusterHAConstraint(s *Solution, eligibles map[*IndexUsage]bool, chkEquiv bool) bool {

	for _, indexer := range s.Placement {
		if !c.SatisfyNodeHAConstraint(s, indexer, eligibles, chkEquiv) {
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
		NodeId:         nodeId,
		NodeUUID:       "tempNodeUUID_" + nodeId,
		meetConstraint: true,
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
		NodeId:         nodeId,
		NodeUUID:       "tempNodeUUID_" + nodeId,
		Indexes:        indexes,
		meetConstraint: true,
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

	r := indexerNodePool.Get()

	r.NodeId = o.NodeId
	r.NodeUUID = o.NodeUUID
	r.IndexerId = o.IndexerId
	r.RestUrl = o.RestUrl
	r.ServerGroup = o.ServerGroup
	r.StorageMode = o.StorageMode
	r.MemUsage = o.MemUsage
	r.MemOverhead = o.MemOverhead
	r.DataSize = o.DataSize
	r.CpuUsage = o.CpuUsage
	r.DiskUsage = o.DiskUsage
	r.Indexes = make([]*IndexUsage, len(o.Indexes))
	r.isDelete = o.isDelete
	r.isNew = o.isNew
	r.Exclude = o.Exclude
	r.ActualMemUsage = o.ActualMemUsage
	r.ActualMemMin = o.ActualMemMin
	r.ActualMemOverhead = o.ActualMemOverhead
	r.ActualCpuUsage = o.ActualCpuUsage
	r.ActualDataSize = o.ActualDataSize
	r.ActualDiskUsage = o.ActualDiskUsage
	r.ActualDrainRate = o.ActualDrainRate
	r.ActualScanRate = o.ActualScanRate
	r.meetConstraint = o.meetConstraint
	r.numEmptyIndex = o.numEmptyIndex
	r.hasEligible = o.hasEligible
	r.dataMovedIn = o.dataMovedIn
	r.indexMovedIn = o.indexMovedIn
	r.totalData = o.totalData
	r.totalIndex = o.totalIndex

	for i, _ := range o.Indexes {
		r.Indexes[i] = o.Indexes[i]
	}

	return r
}

func (o *IndexerNode) free() {

	indexerNodePool.Put(o)
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
// Get memory min
//
func (o *IndexerNode) GetMemMin(useLive bool) uint64 {

	if useLive {
		return o.ActualMemMin
	}

	return o.GetMemTotal(useLive)
}

//
// Add memory
//
func (o *IndexerNode) AddMemUsageOverhead(s *Solution, usage uint64, overhead uint64, min uint64) {

	if s.UseLiveData() {
		o.ActualMemUsage += usage
		o.ActualMemOverhead += overhead
		o.ActualMemMin += min
	} else {
		o.MemUsage += usage
		o.MemOverhead += overhead
	}
}

//
// Subtract memory
//
func (o *IndexerNode) SubtractMemUsageOverhead(s *Solution, usage uint64, overhead uint64, min uint64) {

	if s.UseLiveData() {
		o.ActualMemUsage -= usage
		o.ActualMemOverhead -= overhead
		o.ActualMemMin -= min
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
// Get disk bps
//
func (o *IndexerNode) GetDiskUsage(useLive bool) uint64 {

	if useLive {
		return o.ActualDiskUsage
	}

	return 0
}

//
// Add disk bps
//
func (o *IndexerNode) AddDiskUsage(s *Solution, diskUsage uint64) {

	if s.UseLiveData() {
		o.ActualDiskUsage += diskUsage
	}
}

//
// Subtract disk bps
//
func (o *IndexerNode) SubtractDiskUsage(s *Solution, diskUsage uint64) {

	if s.UseLiveData() {
		o.ActualDiskUsage -= diskUsage
	}
}

//
// Get scan rate
//
func (o *IndexerNode) GetScanRate(useLive bool) uint64 {

	if useLive {
		return o.ActualScanRate
	}

	return 0
}

//
// Add scan rate
//
func (o *IndexerNode) AddScanRate(s *Solution, scanRate uint64) {

	if s.UseLiveData() {
		o.ActualScanRate += scanRate
	}
}

//
// Subtract scan rate
//
func (o *IndexerNode) SubtractScanRate(s *Solution, scanRate uint64) {

	if s.UseLiveData() {
		o.ActualScanRate -= scanRate
	}
}

//
// Get drain rate
//
func (o *IndexerNode) GetDrainRate(useLive bool) uint64 {

	if useLive {
		return o.ActualDrainRate
	}

	return 0
}

//
// Add drain rate
//
func (o *IndexerNode) AddDrainRate(s *Solution, drainRate uint64) {

	if s.UseLiveData() {
		o.ActualDrainRate += drainRate
	}
}

//
// Subtract drain rate
//
func (o *IndexerNode) SubtractDrainRate(s *Solution, drainRate uint64) {

	if s.UseLiveData() {
		o.ActualDrainRate -= drainRate
	}
}

//
// This function returns whether the indexer node should exclude the given index
func (o *IndexerNode) shouldExcludeIndex(s *Solution, n *IndexUsage) bool {
	if o.ExcludeIn(s) {
		return o.IsDeleted() || !(n.overrideExclude || n.initialNode.NodeId == o.NodeId)
	}
	return false
}

//
// This function returns whether to exclude this node for taking in new index
//
func (o *IndexerNode) ExcludeIn(s *Solution) bool {
	return !s.AllowDDLDuringScaleUp() && s.enableExclude && (o.IsDeleted() || o.Exclude == "in" || o.Exclude == "inout")
}

//
// This function returns whether to exclude this node for rebalance out index
//
func (o *IndexerNode) ExcludeOut(s *Solution) bool {
	return !s.AllowDDLDuringScaleUp() && s.enableExclude && (!o.IsDeleted() && (o.Exclude == "out" || o.Exclude == "inout"))
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
	o.Exclude = exclude
}

//
// This function changes the exclude setting of a node
//
func (o *IndexerNode) UnsetExclude() {
	o.Exclude = ""
}

//
// Does indexer satisfy constraint?
//
func (o *IndexerNode) SatisfyNodeConstraint() bool {
	return o.meetConstraint
}

//
// Evaluate if indexer satisfy constraint after adding/removing index
//
func (o *IndexerNode) EvaluateNodeConstraint(s *Solution, canAddIndex bool, idxToAdd *IndexUsage, idxRemoved *IndexUsage) {

	if idxToAdd != nil {
		if !canAddIndex {
			violations := s.constraint.CanAddIndex(s, o, idxToAdd)
			canAddIndex = violations == NoViolation
		}

		// If node does not meet constraint when an index is added,
		// the node must still not meeting constraint after the addition.
		o.meetConstraint = canAddIndex && o.meetConstraint
		return
	}

	if idxRemoved != nil {
		if o.meetConstraint {
			// If node already meet constraint when an index is removed,
			// the node must be meeting constraint after the removal.
			return
		}
	}

	if s.place != nil && s.constraint != nil {
		eligibles := s.place.GetEligibleIndexes()
		o.meetConstraint = s.constraint.SatisfyNodeConstraint(s, o, eligibles)
	}
}

//
// Evaluate node stats for planning purpose
//
func (o *IndexerNode) EvaluateNodeStats(s *Solution) {

	o.numEmptyIndex = 0
	o.totalData = 0
	o.totalIndex = 0
	o.dataMovedIn = 0
	o.indexMovedIn = 0
	o.hasEligible = false

	if s.place == nil {
		return
	}

	eligibles := s.place.GetEligibleIndexes()
	for _, index := range o.Indexes {

		// calculate num empty index
		if !index.HasSizing(s.UseLiveData()) {
			o.numEmptyIndex++
		}

		// has eligible index?
		if _, ok := eligibles[index]; ok {
			o.hasEligible = true
		}

		// calculate data movement
		// 1) only consider eligible index
		// 2) ignore cost of moving an index out of an to-be-deleted node
		// 3) ignore cost of moving to new node
		if s.command == CommandRebalance || s.command == CommandSwap || s.command == CommandRepair {
			if _, ok := eligibles[index]; ok {

				if index.initialNode != nil && !index.initialNode.isDelete {
					o.totalData += index.GetDataSize(s.UseLiveData())
					o.totalIndex++
				}

				if index.initialNode != nil && !index.initialNode.isDelete &&
					index.initialNode.NodeId != o.NodeId && !o.isNew {
					o.dataMovedIn += index.GetDataSize(s.UseLiveData())
					o.indexMovedIn++
				}
			}
		}
	}
}

//
// Evaluate indexer stats and constraints when node has changed
//
func (o *IndexerNode) Evaluate(s *Solution) {
	o.EvaluateNodeConstraint(s, false, nil, nil)
	o.EvaluateNodeStats(s)
}

//
// Evaluate free memory per empty index
//
func (o *IndexerNode) computeFreeMemPerEmptyIndex(s *Solution, maxThreshold uint64, indexToAdd int) uint64 {

	memPerEmpIndex := uint64(0)

	if maxThreshold > o.GetMemTotal(s.UseLiveData()) {
		freeMem := maxThreshold - o.GetMemTotal(s.UseLiveData())

		if o.numEmptyIndex+indexToAdd > 0 {
			memPerEmpIndex = freeMem / uint64(o.numEmptyIndex+indexToAdd)
		}
	}

	return memPerEmpIndex
}

//
// Set Indexes to the node
//
func (o *IndexerNode) SetIndexes(indexes []*IndexUsage) {
	o.Indexes = indexes

	for _, index := range indexes {
		index.initialNode = o
	}
}

//
// Add Indexes to the node
//
func (o *IndexerNode) AddIndexes(indexes []*IndexUsage) {
	o.Indexes = append(o.Indexes, indexes...)

	for _, index := range indexes {
		index.initialNode = o
	}
}

//
// Compute Size of the node using sizing
//
func (o *IndexerNode) ComputeSizing(sizing SizingMethod) {
	if sizing != nil {
		sizing.ComputeIndexerSize(o)
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
func newIndexUsage(defnId common.IndexDefnId, instId common.IndexInstId, partnId common.PartitionId,
	name, bucket, scope, collection string) *IndexUsage {

	return &IndexUsage{DefnId: defnId,
		InstId:     instId,
		PartnId:    partnId,
		Name:       name,
		Bucket:     bucket,
		Scope:      scope,
		Collection: collection,
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

	if o.NeedsEstimation() {
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

	if o.NeedsEstimation() {
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

	if o.NeedsEstimation() {
		return o.EstimatedDataSize
	}

	if useLive {
		return o.ActualDataSize
	}

	return o.DataSize
}

//
// Get disk Usage
//
func (o *IndexUsage) GetDiskUsage(useLive bool) uint64 {

	if o.NeedsEstimation() {
		return 0
	}

	if useLive {
		return o.ActualDiskUsage
	}

	return 0
}

//
// Get scan rate
//
func (o *IndexUsage) GetScanRate(useLive bool) uint64 {

	if o.NeedsEstimation() {
		return 0
	}

	if useLive {
		return o.ActualScanRate
	}

	return 0
}

//
// Get drain rate
//
func (o *IndexUsage) GetDrainRate(useLive bool) uint64 {

	if o.NeedsEstimation() {
		return 0
	}

	if useLive {
		return o.ActualDrainRate
	}

	return 0
}

//
// Get minimum memory usage
//
func (o *IndexUsage) GetMemMin(useLive bool) uint64 {

	if o.NeedsEstimation() {
		return o.EstimatedMemUsage
	}

	if useLive {
		return o.ActualMemMin
	}

	return o.GetMemTotal(useLive)
}

//
//
// Get resident ratio
//
func (o *IndexUsage) GetResidentRatio(useLive bool) float64 {

	var ratio float64
	if useLive {
		ratio = float64(o.ActualResidentPercent)
		return ratio
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

	if o.NeedsEstimation() {
		return o.EstimatedMemUsage != 0
	}

	if useLive {
		//return o.ActualMemUsage != 0 || o.ActualCpuUsage != 0 || o.ActualDataSize != 0
		return o.ActualMemUsage != 0 || o.ActualDataSize != 0
	}

	//return o.MemUsage != 0 || o.CpuUsage != 0 || o.DataSize != 0
	return o.MemUsage != 0 || o.DataSize != 0
}

func (o *IndexUsage) HasSizingInputs() bool {

	return o.NumOfDocs != 0 && ((!o.IsPrimary && o.AvgSecKeySize != 0) || (o.IsPrimary && o.AvgDocKeySize != 0))
}

func (o *IndexUsage) ComputeSizing(useLive bool, sizing SizingMethod) {

	// Compute Sizing.  This can be based on either sizing inputs or real index stats.
	// If coming from real index stats, this will not overwrite the derived sizing from stats (e.g. ActualMemUsage).
	sizing.ComputeIndexSize(o)

	if useLive && o.HasSizingInputs() {
		// If an index has sizing inputs, but it does not have actual sizing stats in a live cluster, then
		// use the computed sizing formula for planning and rebalancing.
		// The index could be a new index to be placed, or an existing index already in cluster.
		// For an existing index in cluster that does not have actual sizing stats:
		// 1) A deferred index that has yet to be built
		// 2) A index still waiting to be create/build (from create token)
		// 3) A index from an empty bucket
		//if o.ActualMemUsage == 0 && o.ActualDataSize == 0 && o.ActualCpuUsage == 0 {
		if o.ActualMemUsage == 0 && o.ActualDataSize == 0 {
			o.ActualMemUsage = o.MemUsage
			o.ActualDataSize = o.DataSize
			o.ActualCpuUsage = o.CpuUsage
			// do not copy mem overhead since this is usually over-estimated
			o.ActualMemOverhead = 0
		}
	}

	if !useLive {
		//o.NoUsageInfo = o.MemUsage == 0 && o.CpuUsage == 0 && o.DataSize == 0
		o.NoUsageInfo = o.MemUsage == 0 && o.DataSize == 0
	} else {
		//o.NoUsageInfo = o.ActualMemUsage == 0 && o.ActualCpuUsage == 0 && o.ActualDataSize == 0
		o.NoUsageInfo = o.ActualMemUsage == 0 && o.ActualDataSize == 0
	}
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

func (o *IndexUsage) GetPartnStatsPrefix() string {
	if o.Instance == nil {
		return common.GetStatsPrefix(o.Bucket, o.Scope, o.Collection, o.Name, 0, 0, false)
	}

	return common.GetStatsPrefix(o.Instance.Defn.Bucket, o.Instance.Defn.Scope,
		o.Instance.Defn.Collection, o.Instance.Defn.Name, o.Instance.ReplicaId,
		int(o.PartnId), true)
}

func (o *IndexUsage) GetInstStatsPrefix() string {
	if o.Instance == nil {
		return common.GetStatsPrefix(o.Bucket, o.Scope, o.Collection, o.Name, 0, 0, false)
	}

	return common.GetStatsPrefix(o.Instance.Defn.Bucket, o.Instance.Defn.Scope,
		o.Instance.Defn.Collection, o.Instance.Defn.Name, 0, 0, false)
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

func (o *IndexUsage) IsNew() bool {
	return o.initialNode == nil
}

func (o *IndexUsage) IndexOnCollection() bool {
	// Empty scope OR collection name is not expected. Assume that the index
	// definition is not upgraded yet.
	if o.Scope == "" || o.Collection == "" {
		return false
	}

	if o.Scope == common.DEFAULT_SCOPE && o.Collection == common.DEFAULT_COLLECTION {
		return false
	}

	return true
}

func (o *IndexUsage) NeedsEstimation() bool {
	return o.NoUsageInfo || o.NeedsEstimate
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
// Get mem mean
//
func (c *UsageBasedCostMethod) GetMemMean() float64 {
	return c.MemMean
}

//
// Get cpu mean
//
func (c *UsageBasedCostMethod) GetCpuMean() float64 {
	return c.CpuMean
}

//
// Get data mean
//
func (c *UsageBasedCostMethod) GetDataMean() float64 {
	return c.DataSizeMean
}

//
// Get disk mean
//
func (c *UsageBasedCostMethod) GetDiskMean() float64 {
	return c.DiskMean
}

//
// Get scan mean
//
func (c *UsageBasedCostMethod) GetScanMean() float64 {
	return c.ScanMean
}

//
// Get drain mean
//
func (c *UsageBasedCostMethod) GetDrainMean() float64 {
	return c.DrainMean
}

//
//
// Compute average resource variation across 4 dimensions:
// 1) memory
// 2) cpu
// 3) disk
// 4) data size
// This computes a "score" from 0 to 4.  The lowest the score,
// the lower the variation.
//
func (c *UsageBasedCostMethod) ComputeResourceVariation() float64 {

	memCost := float64(0)
	//cpuCost := float64(0)
	dataSizeCost := float64(0)
	diskCost := float64(0)
	drainCost := float64(0)
	scanCost := float64(0)
	count := 0

	if c.MemMean != 0 {
		memCost = c.MemStdDev / c.MemMean
		count++
	}

	/*
		if c.CpuMean != 0 {
			cpuCost = c.CpuStdDev / c.CpuMean
			count++
		}
	*/

	if c.DataSizeMean != 0 {
		dataSizeCost = c.DataSizeStdDev / c.DataSizeMean
		count++
	}

	if c.DiskMean != 0 {
		diskCost = c.DiskStdDev / c.DiskMean
		count++
	}

	if c.DrainMean != 0 {
		drainCost = c.DrainStdDev / c.DrainMean
		count++
	}

	if c.ScanMean != 0 {
		scanCost = c.ScanStdDev / c.ScanMean
		count++
	}

	// return (memCost + cpuCost + dataSizeCost + diskCost + drainCost + scanCost) / float64(count)
	return (memCost + dataSizeCost + diskCost + drainCost + scanCost) / float64(count)
}

//
// Compute cost based on variance on memory and cpu usage across indexers
//
func (c *UsageBasedCostMethod) Cost(s *Solution) float64 {

	// compute usage statistics
	c.MemMean, c.MemStdDev = s.ComputeMemUsage()
	c.CpuMean, c.CpuStdDev = s.ComputeCpuUsage()
	c.DiskMean, c.DiskStdDev = s.ComputeDiskUsage()
	c.ScanMean, c.ScanStdDev = s.ComputeScanRate()
	c.DrainMean, c.DrainStdDev = s.ComputeDrainRate()
	c.TotalData, c.DataMoved, c.TotalIndex, c.IndexMoved = s.computeIndexMovement(false)
	c.DataSizeMean, c.DataSizeStdDev = s.ComputeDataSize()

	memCost := float64(0)
	//cpuCost := float64(0)
	diskCost := float64(0)
	drainCost := float64(0)
	scanCost := float64(0)
	movementCost := float64(0)
	indexCost := float64(0)
	emptyIdxCost := float64(0)
	dataSizeCost := float64(0)
	count := 0

	if c.memCostWeight > 0 && c.MemMean != 0 {
		memCost = c.MemStdDev / c.MemMean * c.memCostWeight
	}
	count++

	/*
		if c.cpuCostWeight > 0 && c.CpuMean != 0 {
			cpuCost = c.CpuStdDev / c.CpuMean * c.cpuCostWeight
			count++
		}
	*/

	if c.DataSizeMean != 0 {
		dataSizeCost = c.DataSizeStdDev / c.DataSizeMean
	}
	count++

	if c.DiskMean != 0 {
		diskCost = c.DiskStdDev / c.DiskMean
		count++
	}

	if c.ScanMean != 0 {
		scanCost = c.ScanStdDev / c.ScanMean
		count++
	}

	if c.DrainMean != 0 {
		drainCost = c.DrainStdDev / c.DrainMean
		count++
	}

	// Empty index is index with no recorded memory or cpu usage (excluding mem overhead).
	// It could be index without stats or sizing information.
	// The cost function minimize the residual memory after subtracting the estimated empty
	// index usage.
	//
	emptyIdxCost = s.ComputeCapacityAfterEmptyIndex()
	if emptyIdxCost != 0 {
		count++
	}

	// UsageCost is used as a weight to scale the impact of
	// moving data during rebalance.  Usage cost is affected by:
	// 1) relative ratio of memory deviation and memory mean
	// 2) relative ratio of data size deviation and data size mean
	usageCost := (memCost + dataSizeCost) / float64(2)

	if c.dataCostWeight > 0 && c.TotalData != 0 {
		// The cost of moving data is inversely adjust by the usage cost.
		// If the cluster resource usage is highly unbalanced (high
		// usage cost), the cost of data movement has less hinderance
		// for balancing resource consumption.
		weight := c.dataCostWeight * (1 - usageCost)
		movementCost = float64(c.DataMoved) / float64(c.TotalData) * weight
	}

	if c.dataCostWeight > 0 && c.TotalIndex != 0 {
		weight := c.dataCostWeight * (1 - usageCost)
		indexCost = float64(c.IndexMoved) / float64(c.TotalIndex) * weight
	}

	avgIndexMovementCost := (indexCost + movementCost) / 2
	avgResourceCost := (memCost + emptyIdxCost + dataSizeCost + diskCost + drainCost + scanCost) / float64(count)

	//logging.Tracef("Planner::cost: mem cost %v cpu cost %v data moved %v index moved %v emptyIdx cost %v dataSize cost %v disk cost %v drain %v scan %v count %v",
	//	memCost, cpuCost, movementCost, indexCost, emptyIdxCost, dataSizeCost, diskCost, drainCost, scanCost, count)
	logging.Tracef("Planner::cost: mem cost %v data moved %v index moved %v emptyIdx cost %v dataSize cost %v disk cost %v drain %v scan %v count %v",
		memCost, movementCost, indexCost, emptyIdxCost, dataSizeCost, diskCost, drainCost, scanCost, count)

	//return (memCost + cpuCost + emptyIdxCost + movementCost + indexCost + dataSizeCost + diskCost + drainCost + scanCost) / float64(count)
	return (avgResourceCost + avgIndexMovementCost) / 2
}

//
// Print statistics
//
func (s *UsageBasedCostMethod) Print() {

	var memUtil float64
	var cpuUtil float64
	var diskUtil float64
	var drainUtil float64
	var scanUtil float64
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

	if s.DiskMean != 0 {
		diskUtil = float64(s.DiskStdDev) / float64(s.DiskMean) * 100
	}

	if s.DrainMean != 0 {
		drainUtil = float64(s.DrainStdDev) / float64(s.DrainMean) * 100
	}

	if s.ScanMean != 0 {
		scanUtil = float64(s.ScanStdDev) / float64(s.ScanMean) * 100
	}

	if s.TotalData != 0 {
		dataMoved = float64(s.DataMoved) / float64(s.TotalData) * 100
	}

	if s.TotalIndex != 0 {
		indexMoved = float64(s.IndexMoved) / float64(s.TotalIndex) * 100
	}

	logging.Infof("Indexer Memory Mean %v (%s)", uint64(s.MemMean), formatMemoryStr(uint64(s.MemMean)))
	logging.Infof("Indexer Memory Deviation %v (%s) (%.2f%%)", uint64(s.MemStdDev), formatMemoryStr(uint64(s.MemStdDev)), memUtil)
	logging.Infof("Indexer Memory Utilization %.4f", float64(s.MemMean)/float64(s.constraint.GetMemQuota()))
	logging.Infof("Indexer CPU Mean %.4f", s.CpuMean)
	logging.Infof("Indexer CPU Deviation %.2f (%.2f%%)", s.CpuStdDev, cpuUtil)
	logging.Infof("Indexer CPU Utilization %.4f", float64(s.CpuMean)/float64(s.constraint.GetCpuQuota()))
	logging.Infof("Indexer IO Mean %.4f", s.DiskMean)
	logging.Infof("Indexer IO Deviation %.2f (%.2f%%)", s.DiskStdDev, diskUtil)
	logging.Infof("Indexer Drain Rate Mean %.4f", s.DrainMean)
	logging.Infof("Indexer Drain Rate Deviation %.2f (%.2f%%)", s.DrainStdDev, drainUtil)
	logging.Infof("Indexer Scan Rate Mean %.4f", s.ScanMean)
	logging.Infof("Indexer Scan Rate Deviation %.2f (%.2f%%)", s.ScanStdDev, scanUtil)
	logging.Infof("Indexer Data Size Mean %v (%s)", uint64(s.DataSizeMean), formatMemoryStr(uint64(s.DataSizeMean)))
	logging.Infof("Indexer Data Size Deviation %v (%s) (%.2f%%)", uint64(s.DataSizeStdDev), formatMemoryStr(uint64(s.DataSizeStdDev)), dataSizeUtil)
	logging.Infof("Total Index Data (from non-deleted node) %v", formatMemoryStr(s.TotalData))
	logging.Infof("Index Data Moved (exclude new node) %v (%.2f%%)", formatMemoryStr(s.DataMoved), dataMoved)
	logging.Infof("No. Index (from non-deleted node) %v", formatMemoryStr(s.TotalIndex))
	logging.Infof("No. Index Moved (exclude new node) %v (%.2f%%)", formatMemoryStr(s.IndexMoved), indexMoved)
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
		if !index.pendingDelete {
			p.indexes[index] = true
			index.eligible = true
			p.eligibles[i] = index
		}
	}

	return p
}

//
// Print
//
func (p *RandomPlacement) Print() {

	logging.Infof("Total iteration: %v", p.totalIteration)

	logging.Infof("RandomSwap time: %v", formatTimeStr(uint64(p.randomSwapDur)))
	logging.Infof("RandomSwap call: %v", p.randomSwapCnt)
	logging.Infof("RandomSwap iteration: %v", p.randomSwapRetry)
	if p.randomSwapCnt != 0 {
		logging.Infof("RandomSwap average time per call: %v", formatTimeStr(uint64(p.randomSwapDur/int64(p.randomSwapCnt))))
	}
	if p.randomSwapRetry != 0 {
		logging.Infof("RandomSwap average time per iteration: %v", formatTimeStr(uint64(p.randomSwapDur/int64(p.randomSwapRetry))))
	}

	logging.Infof("RandomMove time: %v", formatTimeStr(uint64(p.randomMoveDur)))
	logging.Infof("RandomMove call: %v (empty index %v)", p.randomMoveCnt, p.randomMoveEmptyCnt)
	if p.randomMoveCnt != 0 {
		logging.Infof("RandomMove average time: %v", formatTimeStr(uint64(p.randomMoveDur/int64(p.randomMoveCnt))))
	}

	logging.Infof("ExhaustMove time: %v", formatTimeStr(uint64(p.exhaustMoveDur)))
	logging.Infof("ExhaustMove call: %v", p.exhaustMoveCnt)
	if p.exhaustMoveCnt != 0 {
		logging.Infof("ExhaustMove average time: %v", formatTimeStr(uint64(p.exhaustMoveDur/int64(p.exhaustMoveCnt))))
	}

	logging.Infof("ExhaustSwap time: %v", formatTimeStr(uint64(p.exhaustSwapDur)))
	logging.Infof("ExhaustSwap call: %v", p.exhaustSwapCnt)
	if p.exhaustSwapCnt != 0 {
		logging.Infof("ExhaustSwap average time: %v", formatTimeStr(uint64(p.exhaustSwapDur/int64(p.exhaustSwapCnt))))
	}
}

//
// Get index for placement
//
func (p *RandomPlacement) GetEligibleIndexes() map[*IndexUsage]bool {
	return p.indexes
}

//
// Is it an eligible index?
//
func (p *RandomPlacement) IsEligibleIndex(index *IndexUsage) bool {

	if index.eligible {
		return true
	}

	return p.indexes[index]
}

//
// Add optional index for placement
//
func (p *RandomPlacement) AddOptionalIndexes(indexes []*IndexUsage) {
	p.optionals = append(p.optionals, indexes...)
	for _, index := range indexes {
		p.indexes[index] = true
		index.eligible = true
	}
}

//
// Add index for placement
//
func (p *RandomPlacement) AddRequiredIndexes(indexes []*IndexUsage) {
	p.eligibles = append(p.eligibles, indexes...)
	for _, index := range indexes {
		p.indexes[index] = true
		index.eligible = true
	}
}

//
// Remove optional index for placement
//
func (p *RandomPlacement) RemoveOptionalIndexes() []*IndexUsage {

	for _, index := range p.optionals {
		delete(p.indexes, index)
		index.eligible = false
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
					logging.Warnf("Index has more replica than indexer nodes. Index=%v Bucket=%v Scope=%v Collection=%v",
						index.GetDisplayName(), index.Bucket, index.Scope, index.Collection)
				} else {
					return errors.New(fmt.Sprintf("Index has more replica than indexer nodes. Index=%v Bucket=%v Scope=%v Collection=%v",
						index.GetDisplayName(), index.Bucket, index.Scope, index.Collection))
				}
			}

			if s.numServerGroup > 1 && numReplica > s.numServerGroup {
				logging.Warnf("Index has more replica than server group. Index=%v Bucket=%v Scope=%v Collection=%v",
					index.GetDisplayName(), index.Bucket, index.Scope, index.Collection)
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
		if index.GetMemMin(s.UseLiveData()) > memQuota {
			return errors.New(fmt.Sprintf("Index exceeding quota. Index=%v Bucket=%v Scope=%v Collection=%v Memory=%v Cpu=%.4f MemoryQuota=%v CpuQuota=%v",
				index.GetDisplayName(), index.Bucket, index.Scope, index.Collection, index.GetMemMin(s.UseLiveData()), index.GetCpuUsage(s.UseLiveData()), s.getConstraintMethod().GetMemQuota(),
				s.getConstraintMethod().GetCpuQuota()))
		}

		if !s.constraint.CanAddNode(s) {
			found := false
			for _, indexer := range s.Placement {
				freeMem := s.getConstraintMethod().GetMemQuota()
				//freeCpu := float64(s.getConstraintMethod().GetCpuQuota())

				for _, index2 := range indexer.Indexes {
					if !p.isEligibleIndex(index2) {
						freeMem -= index2.GetMemMin(s.UseLiveData())
						//freeCpu -= index2.GetCpuUsage(s.UseLiveData())
					}
				}

				//if freeMem >= index.GetMemTotal(s.UseLiveData()) && freeCpu >= index.GetCpuUsage(s.UseLiveData()) {
				if freeMem >= index.GetMemMin(s.UseLiveData()) {
					found = true
					break
				}
			}

			if !found {
				return errors.New(fmt.Sprintf("Cannot find an indexer with enough free memory or cpu for index. Index=%v Bucket=%v Scope=%v Collection=%v",
					index.GetDisplayName(), index.Bucket, index.Scope, index.Collection))
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

	success, final, force := p.randomMoveByLoad(s)
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
				s.moveIndex(outNode, index, indexer, false)
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
		index.eligible = false
	}

	newEligibles := make([]*IndexUsage, len(p.indexes))
	count := 0
	for _, eligible := range p.eligibles {
		if _, ok := p.indexes[eligible]; ok {
			newEligibles[count] = eligible
			count++
			eligible.eligible = true
		}
	}

	p.eligibles = newEligibles
}

//
// This function is for finding a target node for an empty index.  An empty index is index
// with no sizing info (e.g. deferred index). For a node with more free resource, it should be
// able to hold more empty index.
//
// To achieve the goal, this function computes the free memory per empty index.  For a node
// with higher memory per empty index, it has more capacity to hold the empty index (when the
// empty index is eventually build).
//
// Along with the cost method, this function will try to optimize the solution by
// 1) This function move empty index to a indexer with higher memory per empty index.
// 2) Using cost function, it will try to get the higher mean memory per empty index
//
// For (1), it tries to find a better mem-per-empty-index for the index.  The node with higher
// mem-per-empty-index will more likely to be chosen.  But it is not greedy as to avoid local maximum.
// For (2), it will evaluate the cost by considering if the solution has higher mean memory per index.
//
//
func (p *RandomPlacement) findLeastUsedAndPopulatedTargetNode(s *Solution, source *IndexUsage, exclude *IndexerNode) *IndexerNode {

	max := uint64(float64(s.computeMaxMemUsage()) * 1.1)
	if max < s.constraint.GetMemQuota() {
		max = s.constraint.GetMemQuota()
	}

	currentMemPerIndex := int64(exclude.computeFreeMemPerEmptyIndex(s, max, 0))

	indexers := make([]*IndexerNode, 0, len(s.Placement))
	loads := make([]int64, 0, len(s.Placement))
	total := int64(0)
	violateHA := !s.constraint.SatisfyServerGroupConstraint(s, source, exclude.ServerGroup)

	for _, indexer := range s.Placement {

		if indexer.isDelete {
			continue
		}

		if indexer.NodeId == exclude.NodeId {
			continue
		}

		if s.constraint.CanAddIndex(s, indexer, source) == NoViolation {
			memPerIndex := int64(indexer.computeFreeMemPerEmptyIndex(s, max, 1))
			// If current solution has ServerGroupViolation AND moving the index to
			// the target indexer node is resolving that ServerGroupViolation, then
			// allow the index movement by ignoring free-memory-per-empty-index check.
			if memPerIndex > 0 && (exclude.isDelete || memPerIndex > currentMemPerIndex || violateHA) {
				loads = append(loads, memPerIndex)
				indexers = append(indexers, indexer)
				total += memPerIndex
			}
		}
	}

	if len(indexers) != 0 {
		p.randomMoveEmptyCnt++
		return getWeightedRandomNode(p.rs, nil, indexers, loads, total)
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

		// skip if target node is to be ejected
		if indexer.isDelete {
			continue
		}

		// try to swap to an empty node
		if len(indexer.Indexes) != 0 {
			continue
		}

		satisfyConstraint := true
		enforceConstraintOrig := s.enforceConstraint
		s.enforceConstraint = true
		for _, index := range node.Indexes {
			if s.constraint.CanAddIndex(s, indexer, index) != NoViolation {
				satisfyConstraint = false
				break
			}
		}
		s.enforceConstraint = enforceConstraintOrig

		if satisfyConstraint {
			return indexer
		}
	}

	return nil
}

//
// Try random swap
//
func (p *RandomPlacement) tryRandomSwap(s *Solution, sources []*IndexerNode, targets []*IndexerNode, prob float64) bool {

	n := p.rs.Float64()
	if n < prob && s.cost.ComputeResourceVariation() < 0.05 {
		return p.randomSwap(s, sources, targets)
	}

	return false
}

//
// Randomly select a single index to move to a different node
//
func (p *RandomPlacement) randomMoveByLoad(s *Solution) (bool, bool, bool) {

	numOfIndexers := len(s.Placement)
	if numOfIndexers == 1 {
		// only one indexer
		return false, false, false
	}

	// Find a set of candidates (indexer node) that has eligible index
	// From the set of candidates, find those that are under resource constraint.
	// Compute the loads for every constrained candidate
	deleted := p.findDeletedFilledNodes(s)
	logging.Tracef("Planner::deleted: len=%v, %v", len(deleted), deleted)
	candidates := p.findCandidates(s)
	logging.Tracef("Planner::candidates: len=%v, %v", len(candidates), candidates)
	constrained := p.findConstrainedNodes(s, s.constraint, candidates)
	logging.Tracef("Planner::constrained: len=%v, %v", len(constrained), constrained)
	loads, total := computeLoads(s, constrained)

	// Done with basic swap rebalance case for non-partitioned index?
	if len(s.getDeleteNodes()) == 0 &&
		s.numDeletedNode > 0 &&
		s.numNewNode == s.numDeletedNode &&
		len(constrained) == 0 {
		return true, true, true
	}

	retryCount := numOfIndexers * 10
	for i := 0; i < retryCount; i++ {
		p.totalIteration++

		// If there is one node that does not satisfy constraint,
		// and if there is a deleted indexer node, then that node
		// is the constrained node. This node will be treated as
		// the source of index movement/swap.
		if len(constrained) == 1 {
			if !s.constraint.CanAddNode(s) {
				// If planner is working on a fixed cluster, then
				// try exhaustively moving or swapping indexes away from this node.

				if s.hasNewNodes() && s.hasDeletedNodes() {
					// Try moving to new nodes first
					success, force := p.exhaustiveMove(s, constrained, s.Placement, true)
					if success {
						return true, false, force
					}
				}

				success, force := p.exhaustiveMove(s, constrained, s.Placement, false)
				if success {
					return true, false, force
				}

				if p.exhaustiveSwap(s, constrained, candidates) {
					return true, false, false
				}

				// if we cannot find a solution after exhaustively trying to swap or move
				// index in the last constrained node, then we possibly cannot reach a
				// solution.
				return false, true, true
			} else {
				// If planner can grow the cluster, then just try to randomly swap.
				// If cannot swap, then logic fall through to move index.
				if p.randomSwap(s, constrained, candidates) {
					return true, false, false
				}
			}
		}

		// Select an constrained candidate based on weighted probability
		// The most constrained candidate has a higher probability to be selected.
		// This function may not return a source if all indexes are empty indexes.
		// This function always returns a deleted non-empty node, before
		// returning non-deleted consrained nodes.
		source := getWeightedRandomNode(p.rs, deleted, constrained, loads, total)

		// If cannot find a constrained candidate, then try to randomly
		// pick two candidates and try to swap their indexes.
		if source == nil {

			prob := float64(i) / float64(retryCount)
			if p.tryRandomSwap(s, candidates, candidates, prob) {
				return true, false, false
			}

			// If swap fails, then randomly select a candidate as source.
			source = getRandomNode(p.rs, candidates)
			if source == nil {
				return false, false, false
			}
		}

		now := time.Now()

		// From the candidate, randomly select a movable index.
		index := p.getRandomEligibleIndex(s, s.constraint, p.rs, source)
		if index == nil {
			continue
		}

		target := (*IndexerNode)(nil)
		if !index.HasSizing(s.UseLiveData()) {
			target = p.findLeastUsedAndPopulatedTargetNode(s, index, source)
		} else {
			// Select an uncongested indexer which is different from source.
			// The most uncongested indexer has a higher probability to be selected.
			target = p.getRandomUncongestedNodeExcluding(s, source, index)
		}

		if target == nil {
			// if cannot find a uncongested indexer, then check if there is only
			// one candidate and it satisfy resource constraint.  If so, there is
			// no more move (final state).
			if len(candidates) == 1 && source.SatisfyNodeConstraint() {
				logging.Tracef("Planner::final move: source %v index %v", source.NodeId, index)
				p.randomMoveDur += time.Now().Sub(now).Nanoseconds()
				p.randomMoveCnt++
				return true, true, true
			}

			logging.Tracef("Planner::no target : index %v mem %v cpu %.4f source %v",
				index, formatMemoryStr(index.GetMemMin(s.UseLiveData())), index.GetCpuUsage(s.UseLiveData()), source.NodeId)

			// There could be more candidates, pick another one.
			continue
		}

		logging.Tracef("Planner::try move: index %v mem %v cpu %.4f source %v target %v",
			index, formatMemoryStr(index.GetMemMin(s.UseLiveData())), index.GetCpuUsage(s.UseLiveData()), source.NodeId, target.NodeId)

		// See if the index can be moved while obeying resource constraint.
		violation := s.constraint.CanAddIndex(s, target, index)
		if violation == NoViolation {
			force := source.isDelete || !s.constraint.SatisfyIndexHAConstraint(s, source, index, p.GetEligibleIndexes(), true)
			s.moveIndex(source, index, target, true)
			p.randomMoveDur += time.Now().Sub(now).Nanoseconds()
			p.randomMoveCnt++

			logging.Tracef("Planner::randomMoveByLoad: source %v index '%v' (%v,%v,%v) target %v force %v",
				source.NodeId, index.GetDisplayName(), index.Bucket, index.Scope, index.Collection, target.NodeId, force)
			return true, false, force

		} else {
			logging.Tracef("Planner::try move fail: violation %s", violation)
		}
	}

	if logging.IsEnabled(logging.Trace) {
		for _, indexer := range s.Placement {
			logging.Tracef("Planner::no move: indexer %v mem %v cpu %.4f ",
				indexer.NodeId, formatMemoryStr(indexer.GetMemMin(s.UseLiveData())), indexer.GetCpuUsage(s.UseLiveData()))
		}
	}

	// Give it one more try to swap constrained node
	return p.randomSwap(s, constrained, candidates), false, false
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

		s.moveIndex(source, index, target, false)
		movedIndex++
		movedData += index.GetMemUsage(s.UseLiveData())

		_, _, _, indexMoved := s.computeIndexMovement(true)
		percentage = int(float64(indexMoved) / float64(numOfIndexes) * 100)
	}

	return movedIndex, movedData
}

//
// Find a set of deleted indexer nodes that are not empty
//
func (p *RandomPlacement) findDeletedFilledNodes(s *Solution) []*IndexerNode {

	outNodes := s.getDeleteNodes()
	result := ([]*IndexerNode)(nil)

	for _, node := range outNodes {
		if len(node.Indexes) > 0 {
			result = append(result, node)
		}
	}

	if len(result) > 0 {
		return shuffleNode(p.rs, result)
	}

	return result
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
			return shuffleNode(p.rs, candidates)
		}
	}

	// only include node with index to be rebalanced
	for _, indexer := range s.Placement {
		if indexer.hasEligible {
			candidates = append(candidates, indexer)
		}
	}

	return shuffleNode(p.rs, candidates)
}

//
// This function get a random uncongested node.
//
func (p *RandomPlacement) getRandomUncongestedNodeExcluding(s *Solution, exclude *IndexerNode, index *IndexUsage) *IndexerNode {

	if s.hasDeletedNodes() && s.hasNewNodes() {

		indexers := ([]*IndexerNode)(nil)

		for _, indexer := range s.Placement {
			// Call shouldExcludeIndex for shortcut.
			// getRandomFittedNode() will also check if node is excluded.
			if !indexer.shouldExcludeIndex(s, index) &&
				exclude.NodeId != indexer.NodeId &&
				s.constraint.SatisfyNodeResourceConstraint(s, indexer) &&
				!indexer.isDelete &&
				indexer.isNew {
				indexers = append(indexers, indexer)
			}
		}

		target := p.getRandomFittedNode(s, indexers, index)
		if target != nil {
			return target
		}
	}

	indexers := ([]*IndexerNode)(nil)

	for _, indexer := range s.Placement {
		// Call shouldExcludeIndex for shortcut.
		// getRandomFittedNode() will also check if node is excluded.
		if !indexer.shouldExcludeIndex(s, index) &&
			exclude.NodeId != indexer.NodeId &&
			s.constraint.SatisfyNodeResourceConstraint(s, indexer) &&
			!indexer.isDelete {
			indexers = append(indexers, indexer)
		}
	}

	return p.getRandomFittedNode(s, indexers, index)
}

//
// This function get a random node that can fit the index.
//
func (p *RandomPlacement) getRandomFittedNode(s *Solution, indexers []*IndexerNode, index *IndexUsage) *IndexerNode {

	indexers = shuffleNode(p.rs, indexers)

	total := int64(0)
	loads := make([]int64, len(indexers))

	for i, indexer := range indexers {
		violation := s.constraint.CanAddIndex(s, indexer, index)
		if violation == NoViolation {
			if usage := s.computeMeanUsageRatio() - s.computeUsageRatio(indexer); usage > 0 {
				loads[i] = int64(usage * 100)
			}
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

// getRandomEligibleIndex finds a random eligible index from a node. Chooses from those that do not
// satisfy HA contraints. If there are none of those, it chooses from all eligibles on the node.
func (p *RandomPlacement) getRandomEligibleIndex(s *Solution, constraint ConstraintMethod, rs *rand.Rand, node *IndexerNode) *IndexUsage {

	var candidates []*IndexUsage

	if !node.SatisfyNodeConstraint() {
		eligibles := p.GetEligibleIndexes()
		for _, index := range node.Indexes {
			if _, ok := p.indexes[index]; ok {
				if !constraint.SatisfyIndexHAConstraint(s, node, index, eligibles, true) {
					candidates = append(candidates, index)
				}
			}
		}
	}

	if len(candidates) == 0 {
		for _, index := range node.Indexes {
			if _, ok := p.indexes[index]; ok {
				candidates = append(candidates, index)
			}
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
		// This function is used for initial placement (create index).
		// Do not place index on excluded nodes unless overridden by
		// allowDDLDuringScaleUp flag
		if !indexer.ExcludeAny(s) {
			candidates = append(candidates, indexer)
		}
	}

	if len(candidates) == 0 {
		return ErrNoAvailableIndexer
	}

	for _, idx := range indexes {
		indexer := getRandomNode(p.rs, candidates)
		s.addIndex(indexer, idx, false)
		idx.initialNode = nil
	}

	return nil
}

//
// This function places the index on the specified indexer node
//
func (p *RandomPlacement) AddToIndexer(s *Solution, indexer *IndexerNode, idx *IndexUsage) {

	// Explicitly pass meetconstraint to true (forced addition)
	s.addIndex(indexer, idx, true)
	idx.initialNode = nil
	s.updateReplicaMap(idx, indexer, s.place.GetEligibleIndexes())
}

//
// This function randomly place indexes among indexer nodes for initial placement
//
func (p *RandomPlacement) InitialPlace(s *Solution, indexes []*IndexUsage) error {

	candidates := make([]*IndexerNode, 0, len(s.Placement))
	for _, indexer := range s.Placement {
		// This function is used for simulation.
		// Do not place in excluded nodes unless overridden by
		// allowDDLDuringScaleUp flag
		if !indexer.ExcludeAny(s) {
			candidates = append(candidates, indexer)
		}
	}

	if len(candidates) == 0 {
		return errors.New("Cannot find any indexer that can add new indexes")
	}

	for _, idx := range indexes {
		indexer := getRandomNode(p.rs, candidates)
		s.addIndex(indexer, idx, false)
		idx.initialNode = indexer
	}

	return nil
}

//
// Randomly select two index and swap them.
//
func (p *RandomPlacement) randomSwap(s *Solution, sources []*IndexerNode, targets []*IndexerNode) bool {

	if !p.allowSwap {
		return false
	}

	now := time.Now()
	defer func() {
		p.randomSwapDur += time.Now().Sub(now).Nanoseconds()
		p.randomSwapCnt++
	}()

	outNodes := s.getDeleteNodes()
	retryCount := len(sources) * 10
	for i := 0; i < retryCount; i++ {

		p.randomSwapRetry++

		source := getRandomNode(p.rs, sources)
		target := getRandomNode(p.rs, targets)

		if source == nil || target == nil || source == target {
			continue
		}

		if hasMatchingNode(target.NodeId, outNodes) {
			continue
		}

		sourceIndex := p.getRandomEligibleIndex(s, s.getConstraintMethod(), p.rs, source)
		targetIndex := p.getRandomEligibleIndex(s, s.getConstraintMethod(), p.rs, target)

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
		if sourceIndex.NoUsageInfo && s.constraint.SatisfyIndexHAConstraint(s, source, sourceIndex, p.GetEligibleIndexes(), true) {
			continue
		}

		logging.Tracef("Planner::try swap: source index %v (mem %v cpu %.4f) target index %v (mem %v cpu %.4f) source %v target %v",
			sourceIndex, formatMemoryStr(sourceIndex.GetMemMin(s.UseLiveData())), sourceIndex.GetCpuUsage(s.UseLiveData()),
			targetIndex, formatMemoryStr(targetIndex.GetMemMin(s.UseLiveData())), targetIndex.GetCpuUsage(s.UseLiveData()),
			source.NodeId, target.NodeId)

		sourceViolation := s.constraint.CanSwapIndex(s, target, sourceIndex, targetIndex)
		targetViolation := s.constraint.CanSwapIndex(s, source, targetIndex, sourceIndex)

		if sourceViolation == NoViolation && targetViolation == NoViolation {
			logging.Tracef("Planner::swap: source %v source index '%v' (%v,%v,%v) target %v target index '%v' (%v,%v,%v)",
				source.NodeId, sourceIndex.GetDisplayName(), sourceIndex.Bucket, sourceIndex.Scope, sourceIndex.Collection,
				target.NodeId, targetIndex.GetDisplayName(), targetIndex.Bucket, targetIndex.Scope, targetIndex.Collection)
			s.moveIndex(source, sourceIndex, target, true)
			s.moveIndex(target, targetIndex, source, true)
			return true

		} else {
			logging.Tracef("Planner::try swap fail: source violation %s target violation %v", sourceViolation, targetViolation)
		}
	}

	if logging.IsEnabled(logging.Trace) {
		for _, indexer := range s.Placement {
			logging.Tracef("Planner::no swap: indexer %v mem %v cpu %.4f",
				indexer.NodeId, formatMemoryStr(indexer.GetMemMin(s.UseLiveData())), indexer.GetCpuUsage(s.UseLiveData()))
		}
	}

	return false
}

//
// From the list of source indexes, iterate through the list of indexer to find a smaller index that it can swap with.
//
func (p *RandomPlacement) exhaustiveSwap(s *Solution, sources []*IndexerNode, targets []*IndexerNode) bool {

	if !p.allowSwap {
		return false
	}

	now := time.Now()
	defer func() {
		p.exhaustSwapDur += time.Now().Sub(now).Nanoseconds()
		p.exhaustSwapCnt++
	}()

	for _, source := range sources {

		shuffledSourceIndexes := shuffleIndex(p.rs, source.Indexes)
		logging.Tracef("Planner::exhaustive swap: source index after shuffle len=%v, %v", len(shuffledSourceIndexes), shuffledSourceIndexes)

		for _, sourceIndex := range shuffledSourceIndexes {

			if !p.isEligibleIndex(sourceIndex) {
				continue
			}

			// If index has no usage info, then swap only if violate HA constraint.
			if sourceIndex.NoUsageInfo && s.constraint.SatisfyIndexHAConstraint(s, source, sourceIndex, p.GetEligibleIndexes(), true) {
				continue
			}

			shuffledTargets := shuffleNode(p.rs, targets)
			logging.Tracef("Planner::exhaustive swap: targets after shuffled len=%v, %v", len(shuffledTargets), shuffledTargets)

			for _, target := range shuffledTargets {

				// Call shouldExcludeIndex for shortcut.
				// CanSwapIndex() will also check if node is excluded.
				if source.NodeId == target.NodeId || target.isDelete || target.shouldExcludeIndex(s, sourceIndex) {
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

					//if sourceIndex.GetMemTotal(s.UseLiveData()) >= targetIndex.GetMemTotal(s.UseLiveData()) &&
					//	sourceIndex.GetCpuUsage(s.UseLiveData()) >= targetIndex.GetCpuUsage(s.UseLiveData()) {
					if sourceIndex.GetMemMin(s.UseLiveData()) >= targetIndex.GetMemMin(s.UseLiveData()) {

						targetViolation := s.constraint.CanSwapIndex(s, target, sourceIndex, targetIndex)
						sourceViolation := s.constraint.CanSwapIndex(s, source, targetIndex, sourceIndex)

						logging.Tracef("Planner::try exhaustive swap: source index %v (mem %v cpu %.4f) target index %v (mem %v cpu %.4f) source %v target %v",
							sourceIndex, formatMemoryStr(sourceIndex.GetMemMin(s.UseLiveData())), sourceIndex.GetCpuUsage(s.UseLiveData()),
							targetIndex, formatMemoryStr(targetIndex.GetMemMin(s.UseLiveData())), targetIndex.GetCpuUsage(s.UseLiveData()),
							source.NodeId, target.NodeId)

						if targetViolation == NoViolation && sourceViolation == NoViolation {
							logging.Tracef("Planner::exhaustive swap: source %v source index '%v' (%v,%v,%v) target %v target index '%v' (%v,%v,%v)",
								source.NodeId, sourceIndex.GetDisplayName(), sourceIndex.Bucket, sourceIndex.Scope, sourceIndex.Collection,
								target.NodeId, targetIndex.GetDisplayName(), targetIndex.Bucket, targetIndex.Scope, targetIndex.Collection)
							s.moveIndex(source, sourceIndex, target, true)
							s.moveIndex(target, targetIndex, source, true)
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
func (p *RandomPlacement) exhaustiveMove(s *Solution, sources []*IndexerNode, targets []*IndexerNode, newNodeOnly bool) (bool, bool) {

	now := time.Now()
	defer func() {
		p.exhaustMoveDur += time.Now().Sub(now).Nanoseconds()
		p.exhaustMoveCnt++
	}()

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
					force := source.isDelete || !s.constraint.SatisfyIndexHAConstraint(s, source, sourceIndex, p.GetEligibleIndexes(), true)
					s.moveIndex(source, sourceIndex, target, false)

					logging.Tracef("Planner::exhaustive move: source %v index '%v' (%v,%v,%v) target %v force %v",
						source.NodeId, sourceIndex.GetDisplayName(), sourceIndex.Bucket, sourceIndex.Scope, sourceIndex.Collection, target.NodeId, force)
					return true, force
				}
				continue
			}

			shuffledTargets := shuffleNode(p.rs, targets)
			logging.Tracef("Planner::exhaustive move: targets after shuffled len=%v, %v", len(shuffledTargets), shuffledTargets)

			for _, target := range shuffledTargets {

				// Call shouldExcludeIndex for shortcut.
				// CanAddIndex() will also check if node is excluded.
				if source.NodeId == target.NodeId || target.isDelete || (newNodeOnly && !target.isNew) || target.shouldExcludeIndex(s, sourceIndex) {
					continue
				}

				logging.Tracef("Planner::try exhaustive move: index %v mem %v cpu %.4f source %v target %v",
					sourceIndex, formatMemoryStr(sourceIndex.GetMemMin(s.UseLiveData())), sourceIndex.GetCpuUsage(s.UseLiveData()),
					source.NodeId, target.NodeId)

				// See if the index can be moved while obeying resource constraint.
				violation := s.constraint.CanAddIndex(s, target, sourceIndex)
				if violation == NoViolation {
					force := source.isDelete || !s.constraint.SatisfyIndexHAConstraint(s, source, sourceIndex, p.GetEligibleIndexes(), true)
					s.moveIndex(source, sourceIndex, target, true)

					logging.Tracef("Planner::exhaustive move2: source %v index '%v' (%v,%v,%v) target %v force %v",
						source.NodeId, sourceIndex.GetDisplayName(), sourceIndex.Bucket, sourceIndex.Scope, sourceIndex.Collection, target.NodeId, force)
					return true, force

				} else {
					logging.Tracef("Planner::try exhaustive move fail: violation %s", violation)
				}
			}
		}
	}

	return false, false
}

//
// Find a set of indexers do not satisfy node constraint.
//
func (p *RandomPlacement) findConstrainedNodes(s *Solution, constraint ConstraintMethod, indexers []*IndexerNode) []*IndexerNode {

	result := ([]*IndexerNode)(nil)

	// look for indexer node that do not satisfy constraint
	for _, indexer := range indexers {
		if !indexer.SatisfyNodeConstraint() {
			result = append(result, indexer)
		}
	}

	if len(result) > 0 {
		return result
	}

	// Find out if any indexer has problem holding the indexes
	for _, indexer := range indexers {
		if indexer.GetMemMin(s.UseLiveData()) > s.constraint.GetMemQuota() {
			result = append(result, indexer)
		}
	}

	if len(result) > 0 {
		return result
	}

	return indexers

}

//
// Is this index an eligible index?
//
func (p *RandomPlacement) isEligibleIndex(index *IndexUsage) bool {

	if index.eligible {
		return true
	}

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

func GetNewGeneralSizingMethod() *GeneralSizingMethod {
	return newGeneralSizingMethod()
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

//
// This function estimates the index memory overhead
//
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

//
// This function returns if there is any violation
//
func (v *Violations) IsEmpty() bool {

	return v == nil || len(v.Violations) == 0
}

//////////////////////////////////////////////////////////////
// GreedyPlanner
//////////////////////////////////////////////////////////////

//
// Constructor
//
func newGreedyPlanner(cost CostMethod, constraint ConstraintMethod, placement PlacementMethod,
	sizing SizingMethod, indexes []*IndexUsage) *GreedyPlanner {

	return &GreedyPlanner{
		cost:          cost,
		constraint:    constraint,
		placement:     placement,
		sizing:        sizing,
		newIndexes:    indexes,
		equivIndexMap: make(map[string]bool),
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

//
// Check if equivalent indexes exist in the cluster.
//
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

//
// During index creation, either of SAPlanner and GreedyPlanner can be used. The
// decision to use a specific planner depends on the input indexes - to be created
// and the planner config.
// This function (NewPlannerForCommandPlan) decides which planner implementation is
// better for the given input indexes and given planner config. Based on the
// decision, the correspnoding planner object will be returned.
//
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
