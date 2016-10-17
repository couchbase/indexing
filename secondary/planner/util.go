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
	"math"
	"math/rand"
	"strconv"
	"strings"
)

//////////////////////////////////////////////////////////////
// Utility
//////////////////////////////////////////////////////////////

//
// Format memory into friendly string
//
func formatMemoryStr(memory uint64) string {
	mem := float64(memory)

	if mem < 1024 {
		return strconv.FormatFloat(mem, 'g', 6, 64)
	}

	mem = mem / 1024
	if mem < 1024 {
		return strconv.FormatFloat(mem, 'g', 6, 64) + "K"
	}

	mem = mem / 1024
	if mem < 1024 {
		return strconv.FormatFloat(mem, 'g', 6, 64) + "M"
	}

	mem = mem / 1024
	if mem < 1024 {
		return strconv.FormatFloat(mem, 'g', 6, 64) + "G"
	}

	mem = mem / 1024
	return strconv.FormatFloat(mem, 'g', 6, 64) + "T"
}

//
// Format time into friendly string
//
func formatTimeStr(time uint64) string {
	if time < 1000 {
		return strconv.FormatUint(time, 10) + "ns"
	}

	time = uint64(time / 1000)
	if time < 1000 {
		return strconv.FormatUint(time, 10) + "us"
	}

	time = uint64(time / 1000)
	if time < 1000 {
		return strconv.FormatUint(time, 10) + "ms"
	}

	time = uint64(time / 1000)
	return strconv.FormatUint(time, 10) + "s"
}

//
// This function calculates the load of indexer as percentage of quota
//
func computeIndexerUsage(constraint ConstraintMethod, indexer *IndexerNode) float64 {

	memUsage := float64(indexer.GetMemTotal()) / float64(constraint.GetMemQuota())
	cpuUsage := float64(indexer.CpuUsage) / float64(constraint.GetCpuQuota())

	return memUsage + cpuUsage
}

//
// This function calculates the free resource of indexer as percentage of quota
//
func computeIndexerFreeQuota(constraint ConstraintMethod, indexer *IndexerNode) float64 {

	memUsage := (float64(constraint.GetMemQuota()) - float64(indexer.GetMemTotal())) / float64(constraint.GetMemQuota())
	if memUsage < 0 {
		memUsage = 0
	}

	cpuUsage := (float64(constraint.GetCpuQuota()) - float64(indexer.CpuUsage)) / float64(constraint.GetCpuQuota())
	if cpuUsage < 0 {
		cpuUsage = 0
	}

	return memUsage + cpuUsage
}

//
// This function calculates the load of index as percentage of quota
//
func computeIndexUsage(constraint ConstraintMethod, index *IndexUsage) float64 {

	memUsage := float64(index.GetMemTotal()) / float64(constraint.GetMemQuota())
	cpuUsage := float64(index.CpuUsage) / float64(constraint.GetCpuQuota())

	return memUsage + cpuUsage
}

//
// Find a random node
//
func getRandomNode(rs *rand.Rand, indexers []*IndexerNode) *IndexerNode {

	numOfNodes := len(indexers)
	if numOfNodes > 0 {
		n := rs.Intn(numOfNodes)
		return indexers[n]
	}

	return nil
}

//
// Tell if an indexer node holds the given index
//
func hasIndex(indexer *IndexerNode, candidate *IndexUsage) bool {

	for _, index := range indexer.Indexes {
		if candidate == index {
			return true
		}
	}

	return false
}

//
// Compute the loads on a list of nodes
//
func computeLoads(constraint ConstraintMethod, indexers []*IndexerNode) ([]int64, int64) {

	loads := ([]int64)(nil)
	total := int64(0)

	// compute load for each candidate index
	if len(indexers) > 0 {
		loads = make([]int64, len(indexers))
		for i, indexer := range indexers {
			loads[i] = int64(computeIndexerUsage(constraint, indexer) * 100)
			total += loads[i]
		}
	}

	return loads, total
}

//
// This function get a random node.
//
func getWeightedRandomNode(rs *rand.Rand, indexers []*IndexerNode, loads []int64, total int64) *IndexerNode {

	if total > 0 {
		n := int64(rs.Int63n(total))

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
// This function sorts the indexer node by usage
//
func sortNodeByUsage(s *Solution) []*IndexerNode {

	numOfIndexers := len(s.Placement)
	result := make([]*IndexerNode, numOfIndexers)
	copy(result, s.Placement)

	for i, _ := range result {
		max := i
		for j := i + 1; j < numOfIndexers; j++ {

			if computeIndexerUsage(s.constraint, result[j]) > computeIndexerUsage(s.constraint, result[max]) {
				max = j
			}
		}

		if max != i {
			tmp := result[i]
			result[i] = result[max]
			result[max] = tmp
		}
	}

	return result
}

//
// This function sorts the index by usage
//
func sortIndexByUsage(s *Solution, indexes []*IndexUsage) []*IndexUsage {

	numOfIndexes := len(indexes)
	result := make([]*IndexUsage, numOfIndexes)
	copy(result, indexes)

	for i, _ := range result {
		max := i
		for j := i + 1; j < numOfIndexes; j++ {
			if computeIndexUsage(s.constraint, result[j]) > computeIndexUsage(s.constraint, result[max]) {
				max = j
			}
		}

		if max != i {
			tmp := result[i]
			result[i] = result[max]
			result[max] = tmp
		}
	}

	return result
}

//
// This function gets a list of elibigle index to move.
//
func getEligibleIndexes(indexes []*IndexUsage, eligibles []*IndexUsage) []*IndexUsage {

	result := ([]*IndexUsage)(nil)

	for _, index := range indexes {
		for _, eligible := range eligibles {
			if index == eligible {
				result = append(result, index)
				break
			}
		}
	}

	return result
}

//
// Find a random index
//
func getRandomIndex(rs *rand.Rand, indexes []*IndexUsage) *IndexUsage {

	numOfIndexes := len(indexes)
	if numOfIndexes > 0 {
		n := rs.Intn(numOfIndexes)
		return indexes[n]
	}

	return nil
}

//
// Find a matching node
//
func hasMatchingNode(indexerId string, indexers []*IndexerNode) bool {

	for _, idx := range indexers {
		if indexerId == idx.NodeId {
			return true
		}
	}

	return false
}

//
// compute Index memory stats
//
func computeIndexMemStats(indexes []*IndexUsage) (float64, float64) {

	// Compute mean memory usage
	var meanMemUsage float64
	for _, index := range indexes {
		meanMemUsage += float64(index.GetMemUsage())
	}
	meanMemUsage = meanMemUsage / float64(len(indexes))

	// compute memory variance
	var varianceMemUsage float64
	for _, index := range indexes {
		v := float64(index.GetMemUsage()) - meanMemUsage
		varianceMemUsage += v * v
	}
	varianceMemUsage = varianceMemUsage / float64(len(indexes))

	// compute memory std dev
	stdDevMemUsage := math.Sqrt(varianceMemUsage)

	return meanMemUsage, stdDevMemUsage
}

//
// compute index cpu stats
//
func computeIndexCpuStats(indexes []*IndexUsage) (float64, float64) {

	// Compute mean cpu usage
	var meanCpuUsage float64
	for _, index := range indexes {
		meanCpuUsage += float64(index.CpuUsage)
	}
	meanCpuUsage = meanCpuUsage / float64(len(indexes))

	// compute cpu variance
	var varianceCpuUsage float64
	for _, index := range indexes {
		v := float64(index.CpuUsage) - meanCpuUsage
		varianceCpuUsage += v * v
	}
	varianceCpuUsage = varianceCpuUsage / float64(len(indexes))

	// compute memory std dev
	stdDevCpuUsage := math.Sqrt(varianceCpuUsage)

	return meanCpuUsage, stdDevCpuUsage
}

//
// Convert memory string from string to int
//
func ParseMemoryStr(mem string) (int64, error) {
	if mem == "" {
		return -1, nil
	}

	if loc := strings.IndexAny(mem, "KMG"); loc != -1 {
		if loc != len(mem)-1 {
			return -1, errors.New(fmt.Sprintf("unrecognizable memory format %v", mem))
		}

		unit := mem[loc:]
		size, err := strconv.ParseInt(mem[:loc], 10, 64)
		if err != nil {
			return -1, errors.New(fmt.Sprintf("unrecognizable memory format %v", mem))
		}

		if strings.ToUpper(unit) == "K" {
			return size * 1024, nil
		} else if strings.ToUpper(unit) == "M" {
			return size * 1024 * 1024, nil
		} else if strings.ToUpper(unit) == "G" {
			return size * 1024 * 1024 * 1024, nil
		}

		return -1, errors.New(fmt.Sprintf("unrecognizable memory format %v", mem))

	}

	size, err := strconv.ParseInt(mem, 10, 64)
	if err != nil {
		return -1, errors.New(fmt.Sprintf("unrecognizable memory format %v", mem))
	}

	return size, nil
}

//
// Is same indexer node?
//
func isSameIndexer(indexer1 *IndexerNode, indexer2 *IndexerNode) bool {

	return indexer1.NodeId == indexer2.NodeId
}

//
// has replica or equivalent co-located?
//
func violateHA(indexers []*IndexerNode) bool {

	for _, n := range indexers {

		for i := 0; i < len(n.Indexes)-1; i++ {
			for j := i + 1; j < len(n.Indexes); j++ {

				// check replica
				if n.Indexes[i].DefnId == n.Indexes[j].DefnId {
					return false
				}

				// check equivalent index
				if n.Indexes[i].Definition != nil &&
					n.Indexes[j].Definition != nil &&
					common.IsEquivalentIndex(n.Indexes[i].Definition, n.Indexes[j].Definition) {
					return false
				}
			}
		}
	}

	return true
}

//
// Shuffle a list of indexer node
//
func shuffleNode(rs *rand.Rand, indexers []*IndexerNode) []*IndexerNode {

	numOfNodes := len(indexers)
	result := make([]*IndexerNode, numOfNodes)

	for _, indexer := range indexers {
		found := false
		for !found {
			n := rs.Intn(numOfNodes)
			if result[n] == nil {
				result[n] = indexer
				found = true
			}
		}
	}

	return result
}

//
// Shuffle a list of indexes
//
func shuffleIndex(rs *rand.Rand, indexes []*IndexUsage) []*IndexUsage {

	numOfIndexes := len(indexes)
	result := make([]*IndexUsage, numOfIndexes)

	for _, index := range indexes {
		found := false
		for !found {
			n := rs.Intn(numOfIndexes)
			if result[n] == nil {
				result[n] = index
				found = true
			}
		}
	}

	return result
}

//
// Prepare initial solution
//
func setCommandInIndexers(command CommandType, indexers []*IndexerNode) {

	for _, indexer := range indexers {
		indexer.command = command
		for _, index := range indexer.Indexes {
			index.command = command
		}
	}
}
