// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package vector

import (
	"errors"
	"fmt"

	faiss "github.com/couchbase/indexing/secondary/vector/faiss"
)

type codebookIVFPQ struct {
	dim   int //vector dimension
	nsub  int //number of subquantizers
	nbits int //number of bits per subvector index
	nlist int //number of centroids

	metric MetricType //metric

	index *faiss.IndexImpl
}

type codebookIVFSQ struct {
}

func NewCodebookIVFPQ(dim, nsub, nbits, nlist int, metric MetricType) (*codebookIVFPQ, error) {

	var err error

	codebook := &codebookIVFPQ{
		dim:    dim,
		nsub:   nsub,
		nbits:  nbits,
		nlist:  nlist,
		metric: metric,
	}

	faissMetric := convertToFaissMetric(metric)

	codebook.index, err = NewIndexIVFPQ_HNSW(dim, nlist, nsub, nbits, faissMetric)
	if err != nil || codebook.index == nil {
		errStr := fmt.Sprintf("Unable to create index. Err %v", err)
		return nil, errors.New(errStr)
	}
	return codebook, nil

}

//Train the codebook using input vectors.
func (cb *codebookIVFPQ) Train(vecs [][]float32) error {

	train_vecs := convertTo1D(vecs)
	return cb.index.Train(train_vecs)
}

//IsTrained returns true if codebook has been trained.
func (cb *codebookIVFPQ) IsTrained() bool {

	return cb.index.IsTrained()
}

//Compute the quantized code for a given input vector.
//Must be run on a trained codebook.
func (cb *codebookIVFPQ) EncodeVector(vec []float32) ([]uint8, error) {

	return cb.index.EncodeVectors(vec, cb.nsub, cb.nbits, cb.nlist)
}

//Compute the quantized code for a given input vector.
//Must be run on a trained codebook.
func (cb *codebookIVFPQ) EncodeVectors(vec []float32) ([]uint8, error) {

	return cb.index.EncodeVectors(vec, cb.nsub, cb.nbits, cb.nlist)
}

//Find the nearest k centroidIDs for a given vector.
//Must be run on a trained codebook.
func (cb *codebookIVFPQ) FindNearestCentroids(vec []float32, k int64) ([]int64, error) {

	quantizer, err := cb.index.Quantizer()
	if err != nil {
		return nil, nil
	}
	labels, err := quantizer.Assign(vec, k)
	if err != nil {
		return nil, err
	}
	return labels, nil

}
