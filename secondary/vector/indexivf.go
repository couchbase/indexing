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

func NewIndexIVFRaBitQ_HNSW(dim, nlist, nbits, metric int) (*faiss.IndexImpl, error) {
	var description string

	if nbits <= 1 {
		description = fmt.Sprintf("IVF%v_HNSW,RaBitQ", nlist)
	} else {
		description = fmt.Sprintf("IVF%v_HNSW,RaBitQ%v", nlist, nbits)
	}

	index, err := faiss.IndexFactory(dim, description, metric)
	if err != nil {
		return nil, fmt.Errorf("NewIndexIVFRaBitQ_HNSW: %w", err)
	}

	return index, nil
}

func NewIndexIVF_HNSW(dim, nlist, metric int) (*faiss.IndexImpl, error) {

	description := fmt.Sprintf("IVF%v_HNSW,Flat", nlist)
	return faiss.IndexFactory(dim, description, metric)

}
