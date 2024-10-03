package faiss

/*
#include <stdlib.h>
#include <stdio.h>
#include <faiss/c_api/index_io_c.h>
#include <faiss/c_api/index_io_c_ex.h>
*/
import "C"
import (
	"runtime"
	"unsafe"
)

const (
	IOFlagMmap     = C.FAISS_IO_FLAG_MMAP
	IOFlagReadOnly = C.FAISS_IO_FLAG_READ_ONLY
)

func WriteIndexIntoBuffer(idx Index) ([]byte, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// the values to be returned by the faiss APIs
	tempBuf := (*C.uchar)(nil)
	bufSize := C.size_t(0)

	if c := C.faiss_write_index_buf(
		idx.cPtr(),
		&bufSize,
		&tempBuf,
	); c != 0 {
		C.faiss_free_buf(&tempBuf)
		return nil, getLastError()
	}

	// at this point, the idx has a valid ref count. furthermore, the index is
	// something that's present on the C memory space, so not available to go's
	// GC. needs to be freed when its of no more use.

	// the content populated in the tempBuf is converted from *C.uchar to unsafe.Pointer
	// and then the pointer is casted into a large byte slice which is then sliced
	// to a length and capacity equal to bufSize returned across the cgo interface.
	// NOTE: it still points to the C memory though
	// the bufSize is of type size_t  which is equivalent to a uint in golang, so
	// the conversion is safe.
	val := unsafe.Slice((*byte)(unsafe.Pointer(tempBuf)), uint(bufSize))

	// NOTE: This method is compatible with 64-bit systems but may encounter issues on 32-bit systems.
	// leading to vector indexing being supported only for 64-bit systems.
	// This limitation arises because the maximum allowed length of a slice on 32-bit systems
	// is math.MaxInt32 (2^31-1), whereas the maximum value of a size_t in C++ is math.MaxUInt32
	// (4^31-1), exceeding the maximum allowed size of a slice in Go.
	// Consequently, the bufSize returned by faiss_write_index_buf might exceed the
	// maximum allowed size of a slice in Go, leading to a panic when attempting to
	// create the following slice rv.
	rv := make([]byte, uint(bufSize))
	// an explicit copy is necessary to free the memory on C heap and then return
	// the rv back to the caller which is definitely on goruntime space (which will
	// GC'd later on).
	copy(rv, val)

	// safe to free the c memory allocated while serializing the index;
	// rv is from go runtime - so different address space altogether
	// (Freeing of memory must be done within
	// C runtime for it was allocated there);
	C.faiss_free_buf(&tempBuf)
	// p.s: no need to free "val" since the underlying memory is same as tempBuf (deferred free)
	val = nil

	return rv, nil
}

func ReadIndexFromBuffer(buf []byte, ioflags int) (*IndexImpl, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	ptr := (*C.uchar)(unsafe.Pointer(&buf[0]))
	size := C.size_t(len(buf))

	// the idx var has C.FaissIndex within the struct which is nil as of now.
	var idx faissIndex
	if c := C.faiss_read_index_buf(ptr,
		size,
		C.int(ioflags),
		&idx.idx); c != 0 {
		return nil, getLastError()
	}

	ptr = nil

	// after exiting the faiss_read_index_buf, the ref count to the memory allocated
	// for the freshly created faiss::index becomes 1 (held by idx.idx of type C.FaissIndex)
	// this is allocated on the C heap, so not available for golang's GC. hence needs
	// to be cleaned up after the index is longer being used.
	return &IndexImpl{&idx}, nil
}
