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

	codeSize int //size of the code

	isTrained bool

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
func (cb *codebookIVFPQ) Train(vecs []float32) error {

	err := cb.index.Train(vecs)
	if err == nil {
		cb.isTrained = true
	}
	return err
}

//IsTrained returns true if codebook has been trained.
func (cb *codebookIVFPQ) IsTrained() bool {

	if !cb.isTrained {
		return cb.index.IsTrained()
	} else {
		return cb.isTrained
	}
}

//CodeSize returns the size of produced code in bytes.
func (cb *codebookIVFPQ) CodeSize() (int, error) {

	if !cb.IsTrained() {
		return 0, ErrCodebookNotTrained
	}
	return cb.index.CodeSize()
}

//Compute the quantized code for a given input vector.
//Must be run on a trained codebook.
func (cb *codebookIVFPQ) EncodeVector(vec []float32, code []byte) error {

	return cb.EncodeVectors(vec, code)
}

//Compute the quantized code for a given input vector.
//Must be run on a trained codebook.
func (cb *codebookIVFPQ) EncodeVectors(vecs []float32, codes []byte) error {

	if !cb.IsTrained() {
		return ErrCodebookNotTrained
	}
	return cb.index.EncodeVectors(vecs, codes, cb.nsub, cb.nbits, cb.nlist)
}

//Find the nearest k centroidIDs for a given vector.
//Must be run on a trained codebook.
func (cb *codebookIVFPQ) FindNearestCentroids(vec []float32, k int64) ([]int64, error) {

	if !cb.IsTrained() {
		return nil, ErrCodebookNotTrained
	}
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

//Decode the quantized code and return float32 vector.
//Must be run on a trained codebook.
func (cb *codebookIVFPQ) DecodeVector(code []byte, vec []float32) error {
	return cb.DecodeVectors(1, code, vec)
}

//Decode the quantized codes and return float32 vectors.
//Must be run on a trained codebook.
func (cb *codebookIVFPQ) DecodeVectors(n int, codes []byte, vecs []float32) error {

	if !cb.IsTrained() {
		return ErrCodebookNotTrained
	}
	return cb.index.DecodeVectors(n, codes, vecs)
}

//Compute the distance between a vector with another given set of vectors.
func (cb *codebookIVFPQ) ComputeDistance(qvec []float32, fvecs []float32, dist []float32) error {
	if cb.metric == METRIC_L2 {
		return faiss.L2sqrNy(dist, qvec, fvecs, cb.dim)
	}
	return nil
}
