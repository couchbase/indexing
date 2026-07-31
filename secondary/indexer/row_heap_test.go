package indexer

import (
	"math"
	"sort"
	"strconv"
	"sync/atomic"
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

// TestCasMinFloat32 verifies the shared top-K threshold lowers correctly,
// including for negative distances (negated inner products) where uint32
// bit-pattern ordering does not match float ordering.
func TestCasMinFloat32(t *testing.T) {
	var a atomic.Uint32
	a.Store(math.Float32bits(float32(math.Inf(1))))

	load := func() float32 { return math.Float32frombits(a.Load()) }

	casMinFloat32(&a, 5.0)
	if load() != 5.0 {
		t.Fatalf("expected 5.0 got %v", load())
	}
	casMinFloat32(&a, 7.0) // larger, must not raise
	if load() != 5.0 {
		t.Fatalf("expected 5.0 got %v", load())
	}
	casMinFloat32(&a, -3.5) // negative must lower below positive
	if load() != -3.5 {
		t.Fatalf("expected -3.5 got %v", load())
	}
	casMinFloat32(&a, -1.0) // less negative, must not raise
	if load() != -3.5 {
		t.Fatalf("expected -3.5 got %v", load())
	}
	casMinFloat32(&a, -8.25) // more negative must lower
	if load() != -8.25 {
		t.Fatalf("expected -8.25 got %v", load())
	}

	// A NaN must be dropped, not stored: a stored NaN compares false against
	// everything, so it would stop all pruning and let the next call install
	// any value, including a larger one.
	casMinFloat32(&a, float32(math.NaN()))
	if load() != -8.25 {
		t.Fatalf("NaN must not be stored, expected -8.25 got %v", load())
	}
	casMinFloat32(&a, -2.0) // still must not raise after the NaN attempt
	if load() != -8.25 {
		t.Fatalf("expected -8.25 got %v", load())
	}
	casMinFloat32(&a, -9.0) // and must still lower
	if load() != -9.0 {
		t.Fatalf("expected -9.0 got %v", load())
	}
}

// TestTopKRowHeapReplaceRows simulates the persistent-heap materialization
// done at job end: every row in the heap is replaced in place with a copy
// carrying the same dist. The heap must keep its order and honor further
// pushes after the replacement.
func TestTopKRowHeapReplaceRows(t *testing.T) {
	logging.SetLogLevel(logging.Info)

	heap, err := NewTopKRowHeap(5, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	input := []float32{6.0, 7.0, 8.0, 1.0, 2.0, 3.0}
	for i, dist := range input {
		heap.Push(&Row{key: []byte(strconv.Itoa(i)), dist: dist})
	}

	// replace every row with a copy holding the same dist
	replacements := make(map[*Row]bool)
	heap.ReplaceRows(func(row *Row) *Row {
		newRow := &Row{key: append([]byte(nil), row.key...), dist: row.dist}
		replacements[newRow] = true
		return newRow
	})

	// push more rows after replacement; heap invariant must hold
	heap.Push(&Row{key: []byte("x"), dist: 4.0})
	heap.Push(&Row{key: []byte("y"), dist: 9.0})

	expected := []float32{6.0, 4.0, 3.0, 2.0, 1.0}
	i := 0
	for row := heap.Pop(); row != nil; row = heap.Pop() {
		if i >= len(expected) {
			t.Fatal("More values in heap than expected")
		}
		if row.dist != expected[i] {
			t.Fatalf("Wrong value from heap at %v: got dist %v expected %v",
				i, row.dist, expected[i])
		}
		if row.dist != 4.0 && !replacements[row] {
			t.Fatalf("Row with dist %v was not the replaced copy", row.dist)
		}
		i++
	}
	if i != len(expected) {
		t.Fatalf("Heap returned %v rows, expected %v", i, len(expected))
	}
}

// TestTopKRowHeapReplaceRowsReorders covers what ReplaceRows guarantees over a
// bare slot write: replacements whose dist differs from the row they displace
// still leave a valid heap, and rows the callback declines (nil) stay put.
// Without the re-heap the root would be stale, so the next Push would evict
// the wrong row and silently drop a genuine top-K member.
func TestTopKRowHeapReplaceRowsReorders(t *testing.T) {
	logging.SetLogLevel(logging.Info)

	heap, err := NewTopKRowHeap(5, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	for _, dist := range []float32{10.0, 20.0, 30.0, 40.0, 50.0} {
		heap.Push(&Row{key: []byte("orig"), dist: dist})
	}

	// Replace two of the five, moving them far from the slot they sit in so
	// that slot is no longer a valid heap position for them.
	replaced := 0
	heap.ReplaceRows(func(row *Row) *Row {
		if row.dist != 20.0 && row.dist != 40.0 {
			return nil
		}
		replaced++
		return &Row{key: []byte("copy"), dist: row.dist * 100}
	})
	if replaced != 2 {
		t.Fatalf("substitute called on wrong rows: %v replacements, expected 2", replaced)
	}
	if heap.Len() != 5 {
		t.Fatalf("Heap holds %v rows after replacement, expected 5", heap.Len())
	}

	// Heap now holds {10, 30, 50, 2000, 4000} and is full, so this push must
	// evict the largest - 4000 - and not whatever happens to sit at slot 0.
	heap.Push(&Row{key: []byte("new"), dist: 1.0})

	expected := []float32{2000.0, 50.0, 30.0, 10.0, 1.0}
	i := 0
	for row := heap.Pop(); row != nil; row = heap.Pop() {
		if i >= len(expected) {
			t.Fatal("More values in heap than expected")
		}
		if row.dist != expected[i] {
			t.Fatalf("Wrong value from heap at %v: got dist %v expected %v",
				i, row.dist, expected[i])
		}
		i++
	}
	if i != len(expected) {
		t.Fatalf("Heap returned %v rows, expected %v", i, len(expected))
	}
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
