package indexer

import (
	"container/heap"
	"errors"
	"math"

	"github.com/couchbase/indexing/secondary/logging"
)

var ErrorZeroCapactiy = errors.New("Empty heap is not allowed")

// RowHeap is a heap of *Row based on the dist field.
type RowHeap struct {
	rows  []*Row
	isMin bool // determines if this is a min-heap or max-heap
}

func (h RowHeap) Len() int { return len(h.rows) }
func (h RowHeap) Less(i, j int) bool {
	di := float64(h.rows[i].dist)
	dj := float64(h.rows[j].dist)

	// Handle NaN comparisons
	if math.IsNaN(di) && math.IsNaN(dj) {
		return false // consider NaNs equal to each other
	}
	if math.IsNaN(di) {
		// if di is NaN, in a min-heap it should be considered greater, in a max-heap it should be considered lesser
		return h.isMin
	}
	if math.IsNaN(dj) {
		// if dj is NaN, in a min-heap it should be considered lesser, in a max-heap it should be considered greater
		return !h.isMin
	}

	if h.isMin {
		return di < dj
	}
	return di > dj
}
func (h RowHeap) Swap(i, j int) { h.rows[i], h.rows[j] = h.rows[j], h.rows[i] }

// Push adds an element to the heap.
func (h *RowHeap) Push(x interface{}) {
	h.rows = append(h.rows, x.(*Row))
}

// Pop removes the minimum or maximum element from the heap.
func (h *RowHeap) Pop() interface{} {
	old := h.rows
	n := len(old)
	if n == 0 {
		return nil
	}

	x := old[n-1]
	h.rows[n-1] = nil
	h.rows = old[0 : n-1]
	return x
}

func (h *RowHeap) SetRow(index int, row *Row) {
	h.rows[index] = row
}

func (h *RowHeap) GetRow(index int) (row *Row) {
	return h.rows[index]
}

func (h *RowHeap) IsMin() bool {
	return h.isMin
}

func (h *RowHeap) Rows() []*Row {
	return h.rows
}

// TopKRowHeap is a heap that maintains a fixed size.
type TopKRowHeap struct {
	heap     RowHeap
	capacity int
}

// NewTopKRowHeap creates a new TopKRowHeap with the given capacity and heap type.
func NewTopKRowHeap(capacity int, isMin bool) (*TopKRowHeap, error) {
	if capacity == 0 {
		return nil, ErrorZeroCapactiy
	}

	h := &TopKRowHeap{
		heap: RowHeap{
			rows:  make([]*Row, 0, capacity),
			isMin: isMin,
		},
		capacity: capacity,
	}
	heap.Init(&h.heap)
	return h, nil
}

// Add inserts a new element into the heap, maintaining the fixed size.
func (h *TopKRowHeap) Push(row *Row) {
	if h.heap.Len() < h.capacity {
		heap.Push(&h.heap, row)
	} else {
		isMin := h.heap.IsMin()
		row0 := h.heap.GetRow(0)
		if (isMin && row.dist > row0.dist) ||
			(!isMin && row.dist < row0.dist) {
			h.heap.SetRow(0, row)
			heap.Fix(&h.heap, 0)
		}
	}
}

// Pop removes and returns the top element (min or max) from the heap
func (h *TopKRowHeap) Pop() (row *Row) {
	if h.heap.Len() == 0 {
		return nil
	}
	row = heap.Pop(&h.heap).(*Row)
	return
}

// PrintHeap prints all elements in the heap.
func (h *TopKRowHeap) PrintHeap() {
	logging.Infof("Start of Heap")
	for i := 0; i < h.heap.Len(); i++ {
		row := h.heap.GetRow(i)
		logging.Infof("Row: %+v", row)
	}
	logging.Infof("End of Heap")
}

// List returns all the elements cached in the heap. When user does not need rows
// in any specific order they can use this
func (h *TopKRowHeap) List() []*Row {
	return h.heap.Rows()
}

// Len returns the number of valid rows in the heap
func (h *TopKRowHeap) Len() int {
	return h.heap.Len()
}

// Destroy dereferences the rows used in the heap. TopKRowHeap stores rows allocated
// by user so it wont maintain any sync.Pool to return rows. Its the resposibility
// of the user to manage memory for Rows
func (h *TopKRowHeap) Destroy() {
	for i := 0; i < h.heap.Len(); i++ {
		h.heap.SetRow(i, nil)
	}
}
