package indexer

import (
	"container/heap"
	"errors"
	"sort"

	"github.com/couchbase/indexing/secondary/logging"
)

var ErrorZeroCapactiy = errors.New("Empty heap is not allowed")

type RowsCompareLessFn func(i, j *Row) bool

// lessDist reports whether distance di sorts before dj in a heap of the given
// orientation. Two NaNs are equal; a lone NaN sinks to the bottom of a
// min-heap and rises to the top of a max-heap. Comparison stays in float32
// and detects NaN with self-comparison (x != x), which the compiler lowers to
// a single FP compare - avoiding the math.IsNaN call and float64 conversion
// that dominated the comparator in profiles. The NaN branch is entered only
// when a NaN is actually present, so the common path is just the guard plus
// the comparison.
func lessDist(di, dj float32, isMin bool) bool {
	if di != di || dj != dj { // at least one NaN (rare)
		if di != di && dj != dj {
			return false // consider NaNs equal to each other
		}
		if di != di {
			return isMin
		}
		return !isMin
	}
	if isMin {
		return di < dj
	}
	return di > dj
}

// RowHeap is a heap of *Row based on the dist field.
type RowHeap struct {
	rows  []*Row
	isMin bool // determines if this is a min-heap or max-heap
	less  RowsCompareLessFn
}

func (h RowHeap) Len() int { return len(h.rows) }
func (h RowHeap) Less(i, j int) bool {
	if h.less != nil {
		if h.isMin {
			return h.less(h.rows[i], h.rows[j])
		}
		return h.less(h.rows[j], h.rows[i])
	}
	return lessDist(h.rows[i].dist, h.rows[j].dist, h.isMin)
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

func (h *RowHeap) FreeRow(index int) {
	h.rows[index].free()
	h.rows[index] = nil
}

func (h *RowHeap) IsMin() bool {
	return h.isMin
}

func (h *RowHeap) Rows() []*Row {
	return h.rows
}

func (h *RowHeap) LessRows(rowi, rowj *Row) bool {
	if h.less != nil {
		if h.isMin {
			return h.less(rowi, rowj)
		}
		return h.less(rowj, rowi)
	}
	return lessDist(rowi.dist, rowj.dist, h.isMin)
}

// TopKRowHeap is a heap that maintains a fixed size.
type TopKRowHeap struct {
	heap     RowHeap
	capacity int
}

// NewTopKRowHeap creates a new TopKRowHeap with the given capacity and heap type.
func NewTopKRowHeap(capacity int, isMin bool, less RowsCompareLessFn) (*TopKRowHeap, error) {
	if capacity == 0 {
		return nil, ErrorZeroCapactiy
	}

	h := &TopKRowHeap{
		heap: RowHeap{
			rows:  make([]*Row, 0, capacity),
			isMin: isMin,
			less:  less,
		},
		capacity: capacity,
	}
	heap.Init(&h.heap)
	return h, nil
}

func (h *TopKRowHeap) GetSortedRows() []*Row {
	sort.Sort(h.heap)
	return h.heap.Rows()
}

// Add inserts a new element into the heap, maintaining the fixed size.
func (h *TopKRowHeap) Push(row *Row) {
	if h.heap.Len() < h.capacity {
		heap.Push(&h.heap, row)
	} else {
		row0 := h.heap.GetRow(0)
		if h.heap.LessRows(row0, row) {
			h.heap.SetRow(0, row)
			heap.Fix(&h.heap, 0)
			row0.free() // row0 is replaces so free it
		} else {
			row.free() // not using the row so free it
		}
	}
}

// ReplaceRowAt replaces the row stored at the given index without
// re-establishing heap order. Caller must ensure the new row compares
// identically to the one being replaced (e.g. an owned copy carrying the
// same dist/sortKey).
func (h *TopKRowHeap) ReplaceRowAt(index int, row *Row) {
	h.heap.SetRow(index, row)
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
		logging.Infof("sortKey:<%s> sortKeyDecoded<%s> keyDecoded<%s>",
			row.sortKey, row.SortKeyString(), row.KeyString())
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
		h.heap.FreeRow(i)
	}
}
