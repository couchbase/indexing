package vector

import (
	"fmt"

	"github.com/couchbase/indexing/secondary/common"
	faiss "github.com/couchbase/indexing/secondary/vector/faiss"
)

func NewIndexFlat(dim, metric int) (*faiss.IndexImpl, error) {

	description := "Flat"
	return faiss.IndexFactory(dim, description, metric)
}

func NewIndexIVFPQ(dim, nlist, nsub, nbits, metric int, useFastScan bool) (*faiss.IndexImpl, error) {
	fs := ""
	if useFastScan {
		fs = "fs"
	}

	description := fmt.Sprintf("IVF%v,PQ%vx%v%v", nlist, nsub, nbits, fs)
	return faiss.IndexFactory(dim, description, metric)
}

func NewIndexIVFPQ_HNSW(dim, nlist, nsub, nbits, metric int, useFastScan bool) (*faiss.IndexImpl, error) {
	fs := ""
	if useFastScan {
		fs = "fs"
	}

	description := fmt.Sprintf("IVF%v_HNSW,PQ%vx%v%v", nlist, nsub, nbits, fs)
	return faiss.IndexFactory(dim, description, metric)

}

func NewIndexIVFSQ(dim, nlist, metric int, sqRange common.ScalarQuantizerRange) (*faiss.IndexImpl, error) {

	description := fmt.Sprintf("IVF%v,%v", nlist, sqRange)
	return faiss.IndexFactory(dim, description, metric)
}

func NewIndexIVFSQ_HNSW(dim, nlist, metric int, sqRange common.ScalarQuantizerRange) (*faiss.IndexImpl, error) {
	if sqRange == common.SQ_FP16 {
		sqRange = "SQfp16"
	}

	description := fmt.Sprintf("IVF%v_HNSW,%v", nlist, sqRange)
	return faiss.IndexFactory(dim, description, metric)

}
