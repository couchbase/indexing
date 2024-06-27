package faiss

/*
#include <stdlib.h>
#include <faiss/c_api/Index_c.h>
#include <faiss/c_api/Index_c_ex.h>
#include <faiss/c_api/index_factory_c.h>
*/
import "C"
import (
	"runtime"
	"unsafe"
)

// Index is a Faiss index.
//
// Note that some index implementations do not support all methods.
// Check the Faiss wiki to see what operations an index supports.
type Index interface {
	// D returns the dimension of the indexed vectors.
	D() int

	// IsTrained returns true if the index has been trained or does not require
	// training.
	IsTrained() bool

	// Ntotal returns the number of indexed vectors.
	Ntotal() int64

	// MetricType returns the metric type of the index.
	MetricType() int

	// Train trains the index on a representative set of vectors.
	Train(x []float32) error

	// Add adds vectors to the index.
	Add(x []float32) error

	Assign(x []float32, k int64) (labels []int64, err error)

	// Search queries the index with the vectors in x.
	// Returns the IDs of the k nearest neighbors for each query vector and the
	// corresponding distances.
	Search(x []float32, k int64) (distances []float32, labels []int64, err error)

	// Reset removes all vectors from the index.
	Reset() error

	// Close frees the memory used by the index.
	Close()

	// consults the C++ side to get the size of the index
	Size() uint64

	cPtr() *C.FaissIndex
}

type faissIndex struct {
	idx *C.FaissIndex
}

func (idx *faissIndex) cPtr() *C.FaissIndex {
	return idx.idx
}

func (idx *faissIndex) Size() uint64 {
	size := C.faiss_Index_size(idx.idx)
	return uint64(size)
}

func (idx *faissIndex) D() int {
	return int(C.faiss_Index_d(idx.idx))
}

func (idx *faissIndex) IsTrained() bool {
	return C.faiss_Index_is_trained(idx.idx) != 0
}

func (idx *faissIndex) Ntotal() int64 {
	return int64(C.faiss_Index_ntotal(idx.idx))
}

func (idx *faissIndex) MetricType() int {
	return int(C.faiss_Index_metric_type(idx.idx))
}

func (idx *faissIndex) Train(x []float32) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	n := len(x) / idx.D()
	if c := C.faiss_Index_train(idx.idx, C.idx_t(n), (*C.float)(&x[0])); c != 0 {
		return getLastError()
	}
	return nil
}

func (idx *faissIndex) Add(x []float32) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	n := len(x) / idx.D()
	if c := C.faiss_Index_add(idx.idx, C.idx_t(n), (*C.float)(&x[0])); c != 0 {
		return getLastError()
	}
	return nil
}

func (idx *faissIndex) Assign(x []float32, k int64) (
	labels []int64, err error,
) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	n := len(x) / idx.D()
	labels = make([]int64, int64(n)*k)
	if c := C.faiss_Index_assign(
		idx.idx,
		C.idx_t(n),
		(*C.float)(&x[0]),
		(*C.idx_t)(&labels[0]),
		C.idx_t(k),
	); c != 0 {
		err = getLastError()
	}

	return
}

func (idx *faissIndex) Search(x []float32, k int64) (
	distances []float32, labels []int64, err error,
) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	n := len(x) / idx.D()
	distances = make([]float32, int64(n)*k)
	labels = make([]int64, int64(n)*k)
	if c := C.faiss_Index_search(
		idx.idx,
		C.idx_t(n),
		(*C.float)(&x[0]),
		C.idx_t(k),
		(*C.float)(&distances[0]),
		(*C.idx_t)(&labels[0]),
	); c != 0 {
		err = getLastError()
	}

	return
}

func (idx *faissIndex) Reset() error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	if c := C.faiss_Index_reset(idx.idx); c != 0 {
		return getLastError()
	}
	return nil
}

func (idx *faissIndex) Close() {
	C.faiss_Index_free(idx.idx)
}

// IndexImpl is an abstract structure for an index.
type IndexImpl struct {
	Index
}

// IndexFactory builds a composite index.
// description is a comma-separated list of components.
func IndexFactory(d int, description string, metric int) (*IndexImpl, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	cdesc := C.CString(description)
	defer C.free(unsafe.Pointer(cdesc))
	var idx faissIndex
	c := C.faiss_index_factory(&idx.idx, C.int(d), cdesc, C.FaissMetricType(metric))
	if c != 0 {
		return nil, getLastError()
	}
	return &IndexImpl{&idx}, nil
}

func SetOMPThreads(n uint) {
	C.faiss_set_omp_threads(C.uint(n))
}
