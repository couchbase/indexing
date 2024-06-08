package indexer

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/couchbase/indexing/secondary/common"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/vector/codebook"
)

func TestGetNearestCentroidIDs(t *testing.T) {
	vm := &c.VectorMetadata{
		Dimension:  4,
		Similarity: c.L2,
	}

	cb := &codebook.MockCodebook{
		Trained: true,
		VecMeta: vm,
		Centroids: [][]float32{
			{1.2, 3.4, 5.6, 7.8},
			{2.0, 3.1, 5.0, 7.0},
			{2.2, 3.5, 6.7, 8.9},
			{1.5, 2.3, 4.6, 7.1},
			{2.0, 3.0, 5.1, 7.0},
			{3.1, 4.2, 5.8, 9.1},
			{2.8, 3.6, 5.9, 7.3},
			{2.1, 3.0, 5.0, 7.0},
		},
	}

	sr := &ScanRequest{
		queryVector:  []float32{2.0, 3.0, 5.0, 7.0},
		PartitionIds: []c.PartitionId{1, 2},
		codebookMap: map[c.PartitionId]codebook.Codebook{
			c.PartitionId(1): cb,
			c.PartitionId(2): cb,
		},
	}

	sr.nprobes = 2
	err := sr.getNearestCentroids()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Nearest CIDs: ", sr.centroidMap)
	if !compareMaps(sr.centroidMap, map[common.PartitionId][]int64{
		common.PartitionId(1): []int64{1, 4}, common.PartitionId(2): []int64{1, 4},
	}) {
		t.Fatal(fmt.Errorf("wrong centoid ids"))
	}

	sr.nprobes = 3
	err = sr.getNearestCentroids()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Nearest CIDs: ", sr.centroidMap)
	if !compareMaps(sr.centroidMap, map[common.PartitionId][]int64{
		common.PartitionId(1): []int64{1, 4, 7}, common.PartitionId(2): []int64{1, 4, 7},
	}) {
		t.Fatal(fmt.Errorf("wrong centoid ids"))
	}

	sr.nprobes = 1
	err = sr.getNearestCentroids()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Nearest CIDs: ", sr.centroidMap)
	if !compareMaps(sr.centroidMap, map[common.PartitionId][]int64{
		common.PartitionId(1): []int64{1}, common.PartitionId(2): []int64{1},
	}) {
		t.Fatal(fmt.Errorf("wrong centoid ids"))
	}

	cb.Trained = false
	sr.nprobes = 1
	err = sr.getNearestCentroids()
	if !strings.Contains(err.Error(), "is not trained") {
		t.Fatal(err)
	}
	t.Log(err)
}

// Function to check if two slices are equal
func equalSlices(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}

	// Sort the slices to make sure they are in the same order
	sortedA := append([]int64(nil), a...)
	sortedB := append([]int64(nil), b...)
	sort.Slice(sortedA, func(i, j int) bool { return sortedA[i] < sortedA[j] })
	sort.Slice(sortedB, func(i, j int) bool { return sortedB[i] < sortedB[j] })

	return reflect.DeepEqual(sortedA, sortedB)
}

// Function to compare two maps
func compareMaps(map1, map2 map[common.PartitionId][]int64) bool {
	if len(map1) != len(map2) {
		return false
	}

	for key, val1 := range map1 {
		val2, ok := map2[key]
		if !ok {
			return false
		}
		if !equalSlices(val1, val2) {
			return false
		}
	}

	return true
}
