package indexer

import (
	"sort"
	"strconv"
	"testing"

	"github.com/couchbase/indexing/secondary/logging"
)

var testPatterns = []struct {
	name    string
	minHeap bool
	input   []float32
	output  []float32
}{
	{
		"MaxHeapRandomInts",
		false,
		[]float32{6.0, 7.0, 8.0, 1.0, 2.0, 3.0, 4.0, 5.0, 9.0, 10.0},
		[]float32{5.0, 4.0, 3.0, 2.0, 1.0},
	},
	{
		"MaxHeapRandomFloats",
		false,
		[]float32{3.14, 2.36789, 4.839},
		[]float32{2.36789},
	},
	{
		"MaxHeapEmpty",
		false,
		[]float32{3.14, 2.36789, 4.839},
		[]float32{},
	},
	{
		"MaxHeapSizeEqRepeated",
		false,
		[]float32{2.14, 2.14, 3.14, 3.14, 3.14, 3.14},
		[]float32{2.14, 2.14},
	},
	{
		"MaxHeapSizeGtRepeated",
		false,
		[]float32{2.14, 2.14, 3.14, 3.14, 3.14, 3.14},
		[]float32{3.14, 3.14, 2.14, 2.14},
	},
	{
		"MaxHeapSizeAllRepeated",
		false,
		[]float32{3.14, 3.14, 3.14, 3.14, 3.14, 3.14, 3.14, 3.14},
		[]float32{3.14, 3.14},
	},
	{
		"MinHeapRandomInt",
		true,
		[]float32{6.0, 7.0, 8.0, 1.0, 2.0, 3.0, 4.0, 5.0, 9.0, 10.0},
		[]float32{6.0, 7.0, 8.0, 9.0, 10.0},
	},
	{
		"MinHeapRandomFloat",
		true,
		[]float32{3.14, 2.36789, 4.839},
		[]float32{4.839},
	},
	{
		"MinHeapEmpty",
		true,
		[]float32{3.14, 2.36789, 4.839},
		[]float32{},
	},
	{
		"MinHeapSizeEqRepeated",
		true,
		[]float32{2.14, 2.14, 3.14, 3.14, 3.14, 3.14},
		[]float32{3.14, 3.14},
	},
	{
		"MinHeapSizeGtRepeated",
		true,
		[]float32{2.14, 2.14, 2.14, 2.14, 3.14, 3.14},
		[]float32{2.14, 2.14, 3.14, 3.14},
	},
	{
		"MinHeapRepeatedAll",
		true,
		[]float32{3.14, 3.14, 3.14, 3.14, 3.14, 3.14, 3.14, 3.14},
		[]float32{3.14, 3.14},
	},
}

var testPatterns1 = []struct {
	name    string
	minHeap bool
	input   []float32
	output  []float32
}{
	{
		name:    "EmptyHeap",
		minHeap: true,
		input:   []float32{},
		output:  []float32{},
	},
	{
		name:    "SingleElementMinHeap",
		minHeap: true,
		input:   []float32{5.5},
		output:  []float32{5.5},
	},
	{
		name:    "SingleElementMaxHeap",
		minHeap: false,
		input:   []float32{5.5},
		output:  []float32{5.5},
	},
	{
		name:    "TwoElementsMinHeap",
		minHeap: true,
		input:   []float32{9.9, 1.1},
		output:  []float32{1.1, 9.9},
	},
	{
		name:    "TwoElementsMaxHeap",
		minHeap: false,
		input:   []float32{1.1, 9.9},
		output:  []float32{9.9, 1.1},
	},
	{
		name:    "MultipleElementsMinHeap",
		minHeap: true,
		input:   []float32{10.1, 5.5, 3.3, 4.4, 9.9},
		output:  []float32{3.3, 4.4, 5.5, 9.9, 10.1},
	},
	{
		name:    "MultipleElementsMaxHeap",
		minHeap: false,
		input:   []float32{10.1, 5.5, 3.3, 4.4, 9.9},
		output:  []float32{10.1, 9.9, 5.5, 4.4, 3.3},
	},
	{
		name:    "DuplicateElementsMinHeap",
		minHeap: true,
		input:   []float32{5.5, 5.5, 5.5},
		output:  []float32{5.5, 5.5, 5.5},
	},
	{
		name:    "DuplicateElementsMaxHeap",
		minHeap: false,
		input:   []float32{5.5, 5.5, 5.5},
		output:  []float32{5.5, 5.5, 5.5},
	},
	{
		name:    "NegativeElementsMinHeap",
		minHeap: true,
		input:   []float32{-1.1, -5.5, -3.3, -4.4},
		output:  []float32{-5.5, -4.4, -3.3, -1.1},
	},
	{
		name:    "NegativeElementsMaxHeap",
		minHeap: false,
		input:   []float32{-1.1, -5.5, -3.3, -4.4},
		output:  []float32{-1.1, -3.3, -4.4, -5.5},
	},
	{
		name:    "MixedElementsMinHeap",
		minHeap: true,
		input:   []float32{10.1, -5.5, 3.3, -4.4, 9.9},
		output:  []float32{-5.5, -4.4, 3.3, 9.9, 10.1},
	},
	{
		name:    "MixedElementsMaxHeap",
		minHeap: false,
		input:   []float32{10.1, -5.5, 3.3, -4.4, 9.9},
		output:  []float32{10.1, 9.9, 3.3, -4.4, -5.5},
	},
	{
		name:    "LargeNumbersMinHeap",
		minHeap: true,
		input:   []float32{1000000.0, 500000.0, 100000.0, 250000.0, 750000.0},
		output:  []float32{100000.0, 250000.0, 500000.0, 750000.0, 1000000.0},
	},
	{
		name:    "LargeNumbersMaxHeap",
		minHeap: false,
		input:   []float32{1000000.0, 500000.0, 100000.0, 250000.0, 750000.0},
		output:  []float32{1000000.0, 750000.0, 500000.0, 250000.0, 100000.0},
	},
}

var testPatterns2 = []struct {
	name    string
	minHeap bool
	input   []float32
	output  []float32
}{
	{
		name:    "FloatingPointPrecisionMinHeap",
		minHeap: true,
		input:   []float32{1.000001, 1.000002, 1.000000, 1.000005, 1.000003},
		output:  []float32{1.000000, 1.000001, 1.000002, 1.000003, 1.000005},
	},
	{
		name:    "FloatingPointPrecisionMaxHeap",
		minHeap: false,
		input:   []float32{1.000001, 1.000002, 1.000000, 1.000005, 1.000003},
		output:  []float32{1.000005, 1.000003, 1.000002, 1.000001, 1.000000},
	},
	{
		name:    "SmallAndLargeValuesMinHeap",
		minHeap: true,
		input:   []float32{1000000.0, 0.000001, 500.0, 0.000002, 250.0},
		output:  []float32{0.000001, 0.000002, 250.0, 500.0, 1000000.0},
	},
	{
		name:    "SmallAndLargeValuesMaxHeap",
		minHeap: false,
		input:   []float32{1000000.0, 0.000001, 500.0, 0.000002, 250.0},
		output:  []float32{1000000.0, 500.0, 250.0, 0.000002, 0.000001},
	},
	{
		name:    "VeryCloseValuesMinHeap",
		minHeap: true,
		input:   []float32{1.0000001, 1.0000002, 1.0000003, 1.0000004, 1.0000005},
		output:  []float32{1.0000001, 1.0000002, 1.0000003, 1.0000004, 1.0000005},
	},
	{
		name:    "VeryCloseValuesMaxHeap",
		minHeap: false,
		input:   []float32{1.0000001, 1.0000002, 1.0000003, 1.0000004, 1.0000005},
		output:  []float32{1.0000005, 1.0000004, 1.0000003, 1.0000002, 1.0000001},
	},
	{
		name:    "NegativeFloatingPointMinHeap",
		minHeap: true,
		input:   []float32{-1.000001, -1.000002, -1.000000, -1.000005, -1.000003},
		output:  []float32{-1.000005, -1.000003, -1.000002, -1.000001, -1.000000},
	},
	{
		name:    "NegativeFloatingPointMaxHeap",
		minHeap: false,
		input:   []float32{-1.000001, -1.000002, -1.000000, -1.000005, -1.000003},
		output:  []float32{-1.000000, -1.000001, -1.000002, -1.000003, -1.000005},
	},
}

func TestRowHeap(t *testing.T) {
	logging.SetLogLevel(logging.Info)
	testPatterns = append(testPatterns, testPatterns1...)
	testPatterns = append(testPatterns, testPatterns2...)
	for _, tp := range testPatterns {
		t.Run(tp.name, func(t *testing.T) {
			heap, err := NewTopKRowHeap(len(tp.output), tp.minHeap, nil)
			if err != nil && err != ErrorZeroCapactiy {
				t.Fatal(err)
			}
			if err == ErrorZeroCapactiy {
				return
			}

			for i, dist := range tp.input {
				r := &Row{
					key:   []byte(strconv.Itoa(i)),
					value: []byte(`something valuable`),
					dist:  dist,
				}
				heap.Push(r)
			}

			logging.Infof("\n")
			heap.PrintHeap()

			rowList := heap.List()
			sortedRows := RowHeap{
				rows:  make([]*Row, 0),
				isMin: tp.minHeap,
			}
			sortedRows.rows = append(sortedRows.rows, rowList...)
			sort.Sort(sortedRows)

			i := 0
			for row := heap.Pop(); row != nil; row = heap.Pop() {
				logging.Infof("heap(key: %s, dist: %v)", row.key, row.dist)
				sr := sortedRows.GetRow(i)
				logging.Infof("sorted(key: %s, dist: %v)", sr.key, sr.dist)
				if i >= len(tp.output) {
					t.Fatal("More values in heap")
				}
				if row.dist != tp.output[i] || row.dist != sr.dist {
					t.Fatalf("Wrong value returned from heap(key: %s, dist: %v) expected(key:_, dist: %v) sorted(key: %s, dist: %v)",
						row.key, row.dist, tp.output[i], sr.key, sr.dist)
				}
				i++
			}
		})
	}
}
