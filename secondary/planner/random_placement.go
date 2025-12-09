package planner

import (
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/couchbase/indexing/secondary/logging"
)

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

// Constructor
func newRandomPlacement(indexes []*IndexUsage, allowSwap bool, swapDeletedOnly bool) *RandomPlacement {
	p := &RandomPlacement{
		rs:              rand.New(rand.NewSource(time.Now().UnixNano())),
		indexes:         make(map[*IndexUsage]bool),
		eligibles:       make([]*IndexUsage, 0, len(indexes)),
		optionals:       nil,
		allowSwap:       allowSwap,
		swapDeletedOnly: swapDeletedOnly,
	}

	// index to be balanced
	for _, index := range indexes {
		if !index.PendingDelete {
			p.indexes[index] = true
			index.eligible = true
			p.eligibles = append(p.eligibles, index)
		}
	}

	return p
}

// Print
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

// Get index for placement
func (p *RandomPlacement) GetEligibleIndexes() map[*IndexUsage]bool {
	return p.indexes
}

// Is it an eligible index?
func (p *RandomPlacement) IsEligibleIndex(index *IndexUsage) bool {

	if index.eligible {
		return true
	}

	return p.indexes[index]
}

// Add optional index for placement
func (p *RandomPlacement) AddOptionalIndexes(indexes []*IndexUsage) {
	p.optionals = append(p.optionals, indexes...)
	for _, index := range indexes {
		p.indexes[index] = true
		index.eligible = true
	}
}

// Add index for placement
func (p *RandomPlacement) AddRequiredIndexes(indexes []*IndexUsage) {
	p.eligibles = append(p.eligibles, indexes...)
	for _, index := range indexes {
		p.indexes[index] = true
		index.eligible = true
	}
}

// Remove optional index for placement
func (p *RandomPlacement) RemoveOptionalIndexes() []*IndexUsage {

	for _, index := range p.optionals {
		delete(p.indexes, index)
		index.eligible = false
	}

	result := p.optionals
	p.optionals = nil

	return result
}

// Is there any optional index for placement
func (p *RandomPlacement) HasOptionalIndexes() bool {

	return len(p.optionals) > 0
}

// Validate
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

// Has any eligible index?
func (p *RandomPlacement) hasEligibleIndex() bool {
	return len(p.indexes) != 0
}

// Randomly select a single index to move to a different node
//
// rebalance steps:
// 1) Find out index that are eligible to be moved
//   - swap rebalance: index on ejected node
//   - general rebalance: all index
//     2. Move indexes from a ejected node to a "new" node (node with no index)
//     3. If it is a simple swap (no. of ejected node == no. of new node), then stop.
//     4. If there is still any ejected node left after step (2), move those
//     indexes to any node.   After this step, no index on ejected node.
//     5. Perform general rebalance on eligible index.
//   - For index with usage info, rebalance by minimizing usage variance.
//   - For index with no usage info (e.g. deferred index), rebalance by
//     round robin across nodes.
func (p *RandomPlacement) Move(s *Solution) (bool, bool, bool, error) {

	if !p.hasEligibleIndex() {
		return false, true, true, nil
	}

	if p.swapDeleteNode(s) {
		s.removeEmptyDeletedNode()
		return true, false, true, nil
	}

	if p.swapDeletedOnly {
		done := len(s.getDeleteNodes()) == 0
		return done, done, done, nil
	}

	success, final, force, err := p.randomMoveByLoad(s)
	if success {
		s.removeEmptyDeletedNode()
	}

	return success, final, force, err
}

// If there is delete node, try to see if there is an indexer
// node that can host all the indexes for that delete node.
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
				err := s.moveIndex(outNode, index, indexer, false)
				if err != nil {
					logging.Warnf("RandomPlacement.swapDeleteNode failed to move index.")
					return false
				}
			}

			result = true
		}
	}

	return result
}

// Remove Eligible Index.  It does not remove "optional eligible" index.
func (p *RandomPlacement) RemoveEligibleIndex(indexes []*IndexUsage) {

	for _, index := range indexes {
		delete(p.indexes, index)
		index.eligible = false
	}

	var newEligibles []*IndexUsage

	for _, eligible := range p.eligibles {
		if _, ok := p.indexes[eligible]; ok {
			newEligibles = append(newEligibles, eligible)
			eligible.eligible = true
		}
	}

	p.eligibles = newEligibles
}

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

// Find a node that is a swap candidate for the current node.
//  1. node that matches the resource usage requirement.
//  2. replacement is not a deleted node
//  3. indexes do not violate HA properties
//  4. If current node has index with no sizing info, then
//     try to find an empty node.
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

// Try random swap
func (p *RandomPlacement) tryRandomSwap(s *Solution, sources []*IndexerNode, targets []*IndexerNode, prob float64) (bool, error) {

	n := p.rs.Float64()
	if n < prob && s.cost.ComputeResourceVariation() < 0.05 {
		return p.randomSwap(s, sources, targets)
	}

	return false, nil
}

// Randomly select a single index to move to a different node
func (p *RandomPlacement) randomMoveByLoad(s *Solution) (bool, bool, bool, error) {

	numOfIndexers := len(s.Placement)
	if numOfIndexers == 1 {
		// only one indexer
		return false, false, false, nil
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
		return true, true, true, nil
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
					success, force, err := p.exhaustiveMove(s, constrained, s.Placement, true)
					if err != nil {
						logging.Warnf("RandomPlacement.randomMoveByLoad failed exhaustiveMove for new nodes, retryCount:%v.", retryCount)
						return false, false, force, err
					} else if success {
						return true, false, force, nil
					}
				}

				success, force, err := p.exhaustiveMove(s, constrained, s.Placement, false)
				if err != nil {
					logging.Warnf("RandomPlacement.randomMoveByLoad failed exhaustiveMove, retryCount:%v.", retryCount)
					return false, false, force, err
				} else if success {
					return true, false, force, nil
				}

				exhaustiveSwap, err := p.exhaustiveSwap(s, constrained, candidates)
				if err != nil {
					logging.Warnf("RandomPlacement.randomMoveByLoad failed exhaustiveSwap, retryCount:%v.", retryCount)
					return false, false, false, err
				} else if exhaustiveSwap {
					return true, false, false, nil
				}

				// if we cannot find a solution after exhaustively trying to swap or move
				// index in the last constrained node, then we possibly cannot reach a
				// solution.
				return false, true, true, nil
			} else {
				// If planner can grow the cluster, then just try to randomly swap.
				// If cannot swap, then logic fall through to move index.
				randomSwap, err := p.randomSwap(s, constrained, candidates)
				if err != nil {
					logging.Warnf("RandomPlacement.randomMoveByLoad failed randomSwap, retryCount:%v.", retryCount)
					return false, false, false, err
				} else if randomSwap {
					return true, false, false, nil
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
			tryRandomSwap, err := p.tryRandomSwap(s, candidates, candidates, prob)
			if err != nil {
				logging.Warnf("RandomPlacement.randomMoveByLoad failed tryRandomSwap, retryCount:%v.", retryCount)
				return false, false, false, err
			} else if tryRandomSwap {
				return true, false, false, nil
			}

			// If swap fails, then randomly select a candidate as source.
			source = getRandomNode(p.rs, candidates)
			if source == nil {
				return false, false, false, nil
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
				return true, true, true, nil
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
			err := s.moveIndex(source, index, target, true)
			if err != nil {
				logging.Warnf("RandomPlacement.randomMoveByLoad failed moveIndex, retryCount:%v.", retryCount)
				return false, false, false, err
			}
			p.randomMoveDur += time.Now().Sub(now).Nanoseconds()
			p.randomMoveCnt++

			logging.Tracef("Planner::randomMoveByLoad: source %v index '%v' (%v,%v,%v) target %v force %v",
				source.NodeId, index.GetDisplayName(), index.Bucket, index.Scope, index.Collection, target.NodeId, force)
			return true, false, force, nil

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
	randomSwap, err := p.randomSwap(s, constrained, candidates)

	return randomSwap, false, false, err
}

// Randomly select a single index to move to a different node
func (p *RandomPlacement) randomMoveNoConstraint(s *Solution, target int, allowEmptyNode bool) (uint64, uint64) {

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

		// Dont shift the last index on the random source node
		if !allowEmptyNode && len(source.Indexes) == 1 {
			continue
		}
		index := getRandomIndex(p.rs, source.Indexes)
		if index == nil {
			continue
		}

		target := getRandomNode(p.rs, s.Placement)
		if source == target {
			continue
		}

		//No need to handle moveIndex error since getRandomIndex gets index from source.Indexes
		s.moveIndex(source, index, target, false)
		movedIndex++
		movedData += index.GetMemUsage(s.UseLiveData())

		_, _, _, indexMoved := s.computeIndexMovement(true)
		percentage = int(float64(indexMoved) / float64(numOfIndexes) * 100)
	}

	return movedIndex, movedData
}

// Find a set of deleted indexer nodes that are not empty
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

// Find a set of candidate indexer nodes
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

// This function get a random uncongested node.
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

// This function get a random node that can fit the index.
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

// This function randomly place indexes among indexer nodes
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

// This function places the index on the specified indexer node
func (p *RandomPlacement) AddToIndexer(s *Solution, indexer *IndexerNode, idx *IndexUsage) {

	// Explicitly pass meetconstraint to true (forced addition)
	s.addIndex(indexer, idx, true)
	idx.initialNode = nil
	s.updateReplicaMap(idx, indexer, s.place.GetEligibleIndexes())
}

// This function randomly place indexes among indexer nodes for initial placement
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

// Randomly select two index and swap them.
func (p *RandomPlacement) randomSwap(s *Solution, sources []*IndexerNode, targets []*IndexerNode) (bool, error) {

	if !p.allowSwap {
		return false, nil
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
			err := s.moveIndex(source, sourceIndex, target, true)
			if err != nil {
				logging.Warnf("RandomPlacement.randomSwap failed to move index from source to target.")
				return false, err
			}
			err = s.moveIndex(target, targetIndex, source, true)
			if err != nil {
				logging.Warnf("RandomPlacement.randomSwap failed to move index from target to source.")
				return false, err
			}
			return true, nil

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

	return false, nil
}

// From the list of source indexes, iterate through the list of indexer to find a smaller index that it can swap with.
func (p *RandomPlacement) exhaustiveSwap(s *Solution, sources []*IndexerNode, targets []*IndexerNode) (bool, error) {

	if !p.allowSwap {
		return false, nil
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

							err := s.moveIndex(source, sourceIndex, target, true)
							if err != nil {
								logging.Warnf("RandomPlacement.exhaustiveSwap failed to move index from source to target.")
								return false, err
							}
							err = s.moveIndex(target, targetIndex, source, true)
							if err != nil {
								logging.Warnf("RandomPlacement.exhaustiveSwap failed to move index from target to source.")
								return false, err
							}
							return true, nil

						} else {
							logging.Tracef("Planner::try exhaustive swap fail: source violation %s target violation %v", sourceViolation, targetViolation)
						}
					}
				}
			}
		}
	}

	return false, nil
}

// From the list of source indexes, iterate through the list of indexer that it can move to.
func (p *RandomPlacement) exhaustiveMove(s *Solution, sources []*IndexerNode, targets []*IndexerNode, newNodeOnly bool) (bool, bool, error) {

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
					err := s.moveIndex(source, sourceIndex, target, false)
					if err != nil {
						return false, false, err
					}

					logging.Tracef("Planner::exhaustive move: source %v index '%v' (%v,%v,%v) target %v force %v",
						source.NodeId, sourceIndex.GetDisplayName(), sourceIndex.Bucket, sourceIndex.Scope, sourceIndex.Collection, target.NodeId, force)
					return true, force, nil
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
					err := s.moveIndex(source, sourceIndex, target, true)
					if err != nil {
						logging.Warnf("RandomPlacement.exhaustiveMove failed to move index from source to target.")
						return false, false, err
					}

					logging.Tracef("Planner::exhaustive move2: source %v index '%v' (%v,%v,%v) target %v force %v",
						source.NodeId, sourceIndex.GetDisplayName(), sourceIndex.Bucket, sourceIndex.Scope, sourceIndex.Collection, target.NodeId, force)
					return true, force, nil

				} else {
					logging.Tracef("Planner::try exhaustive move fail: violation %s", violation)
				}
			}
		}
	}

	return false, false, nil
}

// Find a set of indexers do not satisfy node constraint.
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

// It is possible that the Proxy replicas are more than the nodes
// but an index within a proxy could be accommodated if broken out
// of the proxy.
// e.g.
//
//	Node n1 has s1-r0{ i1 (replica 0), i2 (replica 0), i3 (replica 0) }
//	Node n2 has s1-r1{ i1 (replica 1)}.
//	Node n3 has s1-r2{ i1 (replica 2)}
//
// If node n1 is removed, the s1-r0 can't be kept in the cluster, but
// i2 and i3 will be lost if the entire shard is dropped blindly.
//
// This function ensures that if such indexes exist whose definition
// will be lost if the entire proxy is dropped but can exist out of the
// proxy, then break the proxy and transfer the index as a replica repair
func (p *SAPlanner) breakOutOfProxyIfNecessary(s *Solution, indexer *IndexerNode, proxy *IndexUsage) (keepIdx []*IndexUsage) {

	if !proxy.IsShardProxy {
		return
	}

	// remove all the individual indexes from the Eligible list
	p.placement.RemoveEligibleIndex(proxy.GroupedIndexes)
	for _, index := range proxy.GroupedIndexes {
		numReplica := s.findNumReplicaWithinProxy(index)
		// If there are more replicas in the cluster (including the deleteNodes)
		// don't consider breaking out of the shard affinity
		if numReplica != 1 {
			continue
		}
		// clone the original and update the initialNode and ShardIds
		cloned := index.clone()
		cloned.initialNode = nil       // consider it as replica repair
		cloned.ShardIds = nil          // reset ShardIds
		cloned.AlternateShardIds = nil // break it away from the current Alternate Shard Ids

		if cloned.Instance != nil {
			logging.Infof("Break the index (%v,%v,%v,%v,%v,%v) out of the proxy %v(%v) from ejected node %v",
				cloned.Bucket, cloned.Scope, cloned.Collection, cloned.Name, cloned.Instance.ReplicaId, cloned.PartnId,
				proxy.AlternateShardIds, proxy.ShardIds, indexer.NodeId)
		} else {
			logging.Infof("Break the index (%v,%v,%v,%v,<nil>,%v) out of the proxy %v(%v) from ejected node %v",
				cloned.Bucket, cloned.Scope, cloned.Collection, cloned.Name, cloned.PartnId,
				proxy.AlternateShardIds, proxy.ShardIds, indexer.NodeId)
		}
		keepIdx = append(keepIdx, cloned)
		// make the index as an eligible index
		p.placement.AddRequiredIndexes([]*IndexUsage{cloned})
	}

	return keepIdx
}

// Is this index an eligible index?
func (p *RandomPlacement) isEligibleIndex(index *IndexUsage) bool {

	if index.eligible {
		return true
	}

	_, ok := p.indexes[index]
	return ok
}

func updateIndexes(indexes []*IndexUsage, skipDeferredIndexGrouping bool) []*IndexUsage {

	var result []*IndexUsage

	indexMap := make(map[*IndexUsage]bool)

	for _, index := range indexes {
		if index.ProxyIndex == nil || (index.NeedsEstimation() && skipDeferredIndexGrouping) {
			result = append(result, index)
		} else {
			if _, ok := indexMap[index.ProxyIndex]; !ok {
				result = append(result, index.ProxyIndex)
				index.ProxyIndex.eligible = true
				indexMap[index.ProxyIndex] = true
			}
		}
	}
	return result
}

// Note: Always call this method after calling grouping on the
// solution
func (p *RandomPlacement) RegroupIndexes(skipDeferredIndexGrouping bool) {
	p.eligibles = updateIndexes(p.eligibles, skipDeferredIndexGrouping)
	p.optionals = updateIndexes(p.optionals, skipDeferredIndexGrouping)

	var indexList []*IndexUsage
	for index := range p.indexes {
		indexList = append(indexList, index)
	}

	indexList = updateIndexes(indexList, skipDeferredIndexGrouping)
	p.indexes = make(map[*IndexUsage]bool)
	for _, index := range indexList {
		p.indexes[index] = true
	}
}
