package vector

import (
	"fmt"

	faiss "github.com/blevesearch/go-faiss"
)

func NewIndexFlat(dim, metric int) (*faiss.IndexImpl, error) {

	description := "Flat"
	return faiss.IndexFactory(dim, description, metric)
}

func NewIndexIVFPQ(dim, nlist, nsub, nbits, metric int) (*faiss.IndexImpl, error) {

	description := fmt.Sprintf("IVF%v,PQ%vx%v", nlist, nsub, nbits)
	return faiss.IndexFactory(dim, description, metric)
}
