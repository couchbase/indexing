package planner

import (
	"errors"
	"fmt"

	"github.com/couchbase/indexing/secondary/logging"
)

type IndexerConstraint struct {
	// system level constraint
	MemQuota   uint64 `json:"memQuota,omitempty"`
	CpuQuota   uint64 `json:"cpuQuota,omitempty"`
	MaxMemUse  int64  `json:"maxMemUse,omitempty"`
	MaxCpuUse  int64  `json:"maxCpuUse,omitempty"`
	canResize  bool
	maxNumNode uint64
}

// Constructor
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

// Print quota
func (c *IndexerConstraint) Print() {
	logging.Infof("Memory Quota %v (%s)", c.MemQuota, formatMemoryStr(c.MemQuota))
	logging.Infof("CPU Quota %v", c.CpuQuota)
	logging.Infof("Max Cpu Utilization %v", c.MaxCpuUse)
	logging.Infof("Max Memory Utilization %v", c.MaxMemUse)
}

// Validate the solution
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

// Return an error with a list of violations
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

// Is this a violation?
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

// Get memory quota
func (c *IndexerConstraint) GetMemQuota() uint64 {
	return c.MemQuota
}

// Get cpu quota
func (c *IndexerConstraint) GetCpuQuota() uint64 {
	return c.CpuQuota
}

// Allow Add Node
func (c *IndexerConstraint) CanAddNode(s *Solution) bool {
	return c.canResize && len(s.Placement) < int(c.maxNumNode)
}

// Check replica server group
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

// This function determines if an index can be placed into the given node,
// while satisfying availability and resource constraint.
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

// This function determines if an index can be swapped with another index in the given node,
// while satisfying availability and resource constraint.
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

// This function determines if a node constraint is satisfied.
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

// This function determines if a node HA constraint is satisfied.
func (c *IndexerConstraint) SatisfyNodeHAConstraint(s *Solution, n *IndexerNode, eligibles map[*IndexUsage]bool, chkEquiv bool) bool {

	for offset, index := range n.Indexes {
		if !c.SatisfyIndexHAConstraintAt(s, n, offset+1, index, eligibles, chkEquiv) {
			return false
		}
	}

	return true
}

// This function determines if a HA constraint is satisfied for a particular index in indexer node.
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

// This function determines if cluster wide constraint is satisfied.
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

// This function determines if a node constraint is satisfied.
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

// This function determines if cluster wide constraint is satisfied.
func (c *IndexerConstraint) SatisfyClusterConstraint(s *Solution, eligibles map[*IndexUsage]bool) bool {

	for _, indexer := range s.Placement {
		if !c.SatisfyNodeConstraint(s, indexer, eligibles) {
			return false
		}
	}

	return true
}

// This function determines if cluster wide constraint is satisfied.
// This function ignores the resource constraint.
func (c *IndexerConstraint) SatisfyClusterHAConstraint(s *Solution, eligibles map[*IndexUsage]bool, chkEquiv bool) bool {

	for _, indexer := range s.Placement {
		if !c.SatisfyNodeHAConstraint(s, indexer, eligibles, chkEquiv) {
			return false
		}
	}

	return true
}
