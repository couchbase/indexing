//go:build linux && arm64 && !community
// +build linux,arm64,!community

package codebook

//Note : A separate file is required for amd64 and arm64 as faiss library
//has a different name (faiss for arm64 and faiss_avx2 for amd64)

/*
#cgo LDFLAGS: -lfaiss

//declare these openblas functions here explicitly as the header file
//is not available for these because openblas is statically linked to faiss.

void openblas_set_num_threads(int);
int  openblas_get_num_threads(void);
*/
import "C"

// SetOpenBLASThreads sets the number of OpenBLAS threads.
func SetOpenBLASThreadsInternal(n int) {
	C.openblas_set_num_threads(C.int(n))
}

// GetOpenBLASThreads returns the current OpenBLAS thread cap (or 0 if unavailable).
func GetOpenBLASThreadsInternal() int {
	return int(C.openblas_get_num_threads())
}
