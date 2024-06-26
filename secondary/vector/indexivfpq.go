package vector

import (
	"fmt"

	faiss "github.com/couchbase/indexing/secondary/vector/faiss"
)

func NewIndexFlat(dim, metric int) (*faiss.IndexImpl, error) {

	description := "Flat"
	return faiss.IndexFactory(dim, description, metric)
}

func NewIndexIVFPQ(dim, nlist, nsub, nbits, metric int) (*faiss.IndexImpl, error) {

	description := fmt.Sprintf("IVF%v,PQ%vx%v", nlist, nsub, nbits)
	return faiss.IndexFactory(dim, description, metric)
}

func NewIndexIVFPQ_HNSW(dim, nlist, nsub, nbits, metric int) (*faiss.IndexImpl, error) {

	description := fmt.Sprintf("IVF%v_HNSW,PQ%vx%v", nlist, nsub, nbits)
	return faiss.IndexFactory(dim, description, metric)

}
