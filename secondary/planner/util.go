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
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"math"
	"math/rand"
	"os"
	"runtime/pprof"
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
func computeIndexerUsage(s *Solution, indexer *IndexerNode) float64 {

	memUsage := float64(indexer.GetMemTotal(s.UseLiveData())) / float64(s.constraint.GetMemQuota())
	cpuUsage := float64(indexer.GetCpuUsage(s.UseLiveData())) / float64(s.constraint.GetCpuQuota())

	return memUsage + cpuUsage
}

//
// This function calculates the free resource of indexer as percentage of quota
//
func computeIndexerFreeQuota(s *Solution, indexer *IndexerNode) float64 {

	memUsage := (float64(s.constraint.GetMemQuota()) - float64(indexer.GetMemTotal(s.UseLiveData()))) / float64(s.constraint.GetMemQuota())
	if memUsage < 0 {
		memUsage = 0
	}

	cpuUsage := (float64(s.constraint.GetCpuQuota()) - float64(indexer.GetCpuUsage(s.UseLiveData()))) / float64(s.constraint.GetCpuQuota())
	if cpuUsage < 0 {
		cpuUsage = 0
	}

	return memUsage + cpuUsage
}

//
// This function calculates the load of index as percentage of quota
//
func computeIndexUsage(s *Solution, index *IndexUsage) float64 {

	memUsage := float64(index.GetMemTotal(s.UseLiveData())) / float64(s.constraint.GetMemQuota())
	cpuUsage := float64(index.GetCpuUsage(s.UseLiveData())) / float64(s.constraint.GetCpuQuota())

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
func computeLoads(s *Solution, indexers []*IndexerNode) ([]int64, int64) {

	loads := ([]int64)(nil)
	total := int64(0)

	// compute load for each candidate index
	if len(indexers) > 0 {
		loads = make([]int64, len(indexers))
		for i, indexer := range indexers {
			loads[i] = int64(s.computeUsageRatio(indexer) * 100)
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
// This function sorts the indexer node by usage in ascending order.
// For indexer usage, it will consider both cpu and memory.
// For indexer nodes that have the same usage, it will sort by
// the number of indexes with unkown usage info (index.NoUsageInfo=true).
// Two indexers could have the same usage if the indexers are emtpy
// or holding deferred index (no usage stats).
//
func sortNodeByUsage(s *Solution, indexers []*IndexerNode) []*IndexerNode {

	numOfIndexers := len(indexers)
	result := make([]*IndexerNode, numOfIndexers)
	copy(result, indexers)

	for i, _ := range result {
		min := i
		for j := i + 1; j < numOfIndexers; j++ {

			minNodeUsage := s.computeUsageRatio(result[min])
			newNodeUsage := s.computeUsageRatio(result[j])

			if newNodeUsage < minNodeUsage {
				min = j

			} else if newNodeUsage == minNodeUsage {
				// Tiebreaker: Only consider count of index with no usage info since these
				// indexes do not contribute to usage stats.
				if numIndexWithNoUsageInfo(result[j]) < numIndexWithNoUsageInfo(result[min]) {
					min = j
				}
			}
		}

		if min != i {
			tmp := result[i]
			result[i] = result[min]
			result[min] = tmp
		}
	}

	return result
}

//
// This function sorts the indexer node by number of NoUsageInfo indexes
// in ascending order.
//
func sortNodeByNoUsageInfoIndexCount(indexers []*IndexerNode) []*IndexerNode {

	numOfIndexers := len(indexers)
	result := make([]*IndexerNode, numOfIndexers)
	copy(result, indexers)

	for i, _ := range result {
		min := i
		for j := i + 1; j < numOfIndexers; j++ {

			if numIndexWithNoUsageInfo(result[j]) < numIndexWithNoUsageInfo(result[min]) {
				min = j
			}
		}

		if min != i {
			tmp := result[i]
			result[i] = result[min]
			result[min] = tmp
		}
	}

	return result
}

//
// This function sorts the index by usage in descending order.  Index
// with no usage will be placed at the end (0 usage).
//
func sortIndexByUsage(s *Solution, indexes []*IndexUsage) []*IndexUsage {

	numOfIndexes := len(indexes)
	result := make([]*IndexUsage, numOfIndexes)
	copy(result, indexes)

	for i, _ := range result {
		max := i
		for j := i + 1; j < numOfIndexes; j++ {
			if computeIndexUsage(s, result[j]) > computeIndexUsage(s, result[max]) {
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
// This function checks is the index is an eligible index
//
func isEligibleIndex(index *IndexUsage, eligibles map[*IndexUsage]bool) bool {

	if index.eligible {
		return true
	}

	return eligibles[index]
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
func computeIndexMemStats(indexes []*IndexUsage, useLive bool) (float64, float64) {

	// Compute mean memory usage
	var meanMemUsage float64
	for _, index := range indexes {
		meanMemUsage += float64(index.GetMemUsage(useLive))
	}
	meanMemUsage = meanMemUsage / float64(len(indexes))

	// compute memory variance
	var varianceMemUsage float64
	for _, index := range indexes {
		v := float64(index.GetMemUsage(useLive)) - meanMemUsage
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
func computeIndexCpuStats(indexes []*IndexUsage, useLive bool) (float64, float64) {

	// Compute mean cpu usage
	var meanCpuUsage float64
	for _, index := range indexes {
		meanCpuUsage += float64(index.GetCpuUsage(useLive))
	}
	meanCpuUsage = meanCpuUsage / float64(len(indexes))

	// compute cpu variance
	var varianceCpuUsage float64
	for _, index := range indexes {
		v := float64(index.GetCpuUsage(useLive)) - meanCpuUsage
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
// Validate solution
//
func ValidateSolution(s *Solution) error {

	for _, indexer := range s.Placement {
		totalMem := uint64(0)
		totalOverhead := uint64(0)
		totalCpu := float64(0)

		for _, index := range indexer.Indexes {
			totalMem += index.GetMemUsage(s.UseLiveData())
			totalOverhead += index.GetMemOverhead(s.UseLiveData())
			totalCpu += index.GetCpuUsage(s.UseLiveData())
		}

		if !s.UseLiveData() {
			totalOverhead += 100 * 1024 * 1024
		}

		if indexer.GetMemUsage(s.UseLiveData()) != totalMem {
			return errors.New("validation fails: memory usage of indexer does not match sum of index memory use")
		}

		if math.Floor(indexer.GetCpuUsage(s.UseLiveData())) != math.Floor(totalCpu) {
			return errors.New("validation fails: cpu usage of indexer does not match sum of index cpu use")
		}

		if indexer.GetMemOverhead(s.UseLiveData()) != totalOverhead {
			return errors.New("validation fails: memory overhead of indexer does not match sum of index memory overhead")
		}

		if indexer.GetMemTotal(s.UseLiveData()) != indexer.GetMemUsage(s.UseLiveData())+indexer.GetMemOverhead(s.UseLiveData()) {
			return errors.New("validation fails: total indexer memory does not match sum of indexer memory usage + overhead")
		}
	}

	return nil
}

//
// Reverse list of nodes
//
func reverseNode(indexers []*IndexerNode) []*IndexerNode {

	numOfNodes := len(indexers)
	for i := 0; i < numOfNodes/2; i++ {
		tmp := indexers[i]
		indexers[i] = indexers[numOfNodes-i-1]
		indexers[numOfNodes-i-1] = tmp
	}

	return indexers
}

//
// Find the number of indexes that has no stats or sizing information.
//
func numIndexWithNoUsageInfo(indexer *IndexerNode) int {

	count := 0
	for _, index := range indexer.Indexes {
		if index.NoUsageInfo {
			count++
		}
	}

	return count
}

func startCPUProfile(filename string) {
	fd, err := os.Create(filename)
	if err != nil {
		logging.Errorf("Planner:: unable to create %q: %v\n", filename, err)
		return
	}
	pprof.StartCPUProfile(fd)
}

func stopCPUProfile() {
	pprof.StopCPUProfile()
}

//
// Make index usage from definition
//
func makeIndexUsageFromDefn(defn *common.IndexDefn, instId common.IndexInstId, partnId common.PartitionId, numPartition uint64) *IndexUsage {

	index := &IndexUsage{
		DefnId:        defn.DefnId,
		InstId:        instId,
		PartnId:       partnId,
		Name:          defn.Name,
		Bucket:        defn.Bucket,
		Scope:         defn.Scope,
		Collection:    defn.Collection,
		IsPrimary:     defn.IsPrimary,
		StorageMode:   common.IndexTypeToStorageMode(defn.Using).String(),
		NumOfDocs:     defn.NumDoc / numPartition,
		AvgSecKeySize: defn.SecKeySize,
		AvgDocKeySize: defn.DocKeySize,
		AvgArrSize:    defn.ArrSize,
		AvgArrKeySize: defn.SecKeySize,
		ResidentRatio: defn.ResidentRatio,
		NoUsageInfo:   defn.Deferred, // This value will be reset in IndexUsage.ComputeSizing()
	}

	if defn.ResidentRatio == 0 {
		index.ResidentRatio = 100
	}

	if !defn.IsArrayIndex {
		index.AvgArrKeySize = 0
		index.AvgArrSize = 0
	}

	return index
}

//
// GetIndexStat function relies on the format of the stat returned by the
// indexer. If the indexer stats format changes, this function will hide
// the stats format details from the consumer of the stats.
//
func GetIndexStat(index *IndexUsage, stat string, stats map[string]interface{},
	isPartn bool, clusterVersion uint64) (interface{}, bool) {

	var prefix string
	if isPartn {
		if index.partnStatPrefix == "" {
			if clusterVersion >= common.INDEXER_55_VERSION {
				prefix = index.GetPartnStatsPrefix()
			} else {
				prefix = index.GetInstStatsPrefix()
			}
			index.partnStatPrefix = prefix
		}
		prefix = index.partnStatPrefix
	} else {
		if index.instStatPrefix == "" {
			prefix = index.GetInstStatsPrefix()
			index.instStatPrefix = prefix
		}
		prefix = index.instStatPrefix
	}

	key := common.GetIndexStatKey(prefix, stat)
	val, ok := stats[key]
	return val, ok
}
