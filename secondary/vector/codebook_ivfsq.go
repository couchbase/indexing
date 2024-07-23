// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package vector

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/couchbase/indexing/secondary/common"
	c "github.com/couchbase/indexing/secondary/vector/codebook"
	faiss "github.com/couchbase/indexing/secondary/vector/faiss"
)

type codebookIVFSQ struct {
	dim     int                         //vector dimension
	nlist   int                         //number of centroids
	sqRange common.ScalarQuantizerRange //scalar quantizer type

	codeSize int //size of the code

	isTrained bool

	metric MetricType //metric

	index *faiss.IndexImpl
}

type codebookIVFSQ_IO struct {
	Dim     int                         `json:"dim,omitempty"`
	SQRange common.ScalarQuantizerRange `json:"sqrange,omitempty"`
	Nlist   int                         `json:"nlist,omitempty"`

	IsTrained bool       `json:"istrained,omitempty"`
	Metric    MetricType `json:"metric,omitempty"`

	Checksum    uint32      `json:"checksum,omitempty"`
	CodebookVer CodebookVer `json:"codebookver,omitempty"`

	Index []byte `json:"index,omitempty"`
}

func NewCodebookIVFSQ(dim, nlist int, sqRange common.ScalarQuantizerRange, metric MetricType) (c.Codebook, error) {

	var err error

	if metric != METRIC_L2 {
		return nil, c.ErrUnsupportedMetric
	}

	codebook := &codebookIVFSQ{
		dim:     dim,
		sqRange: sqRange,
		nlist:   nlist,
		metric:  metric,
	}

	faissMetric := convertToFaissMetric(metric)

	faiss.SetOMPThreads(defaultOMPThreads)

	codebook.index, err = NewIndexIVFSQ_HNSW(dim, nlist, faissMetric, sqRange)
	if err != nil || codebook.index == nil {
		errStr := fmt.Sprintf("Unable to create index. Err %v", err)
		return nil, errors.New(errStr)
	}
	return codebook, nil

}

// Train the codebook using input vectors.
func (cb *codebookIVFSQ) Train(vecs []float32) error {

	if cb.index == nil {
		return c.ErrCodebookClosed
	}

	err := cb.index.Train(vecs)
	if err == nil {
		cb.isTrained = true
	}
	return err
}

// IsTrained returns true if codebook has been trained.
func (cb *codebookIVFSQ) IsTrained() bool {

	if cb.index == nil {
		return false
	}

	if !cb.isTrained {
		return cb.index.IsTrained()
	} else {
		return cb.isTrained
	}
}

// CodeSize returns the size of produced code in bytes.
func (cb *codebookIVFSQ) CodeSize() (int, error) {

	if !cb.IsTrained() {
		return 0, c.ErrCodebookNotTrained
	}
	return cb.index.CodeSize()
}

// Compute the quantized code for a given input vector.
// Must be run on a trained codebook.
func (cb *codebookIVFSQ) EncodeVector(vec []float32, code []byte) error {

	return cb.EncodeVectors(vec, code)
}

// Compute the quantized code for a given input vector.
// Must be run on a trained codebook.
func (cb *codebookIVFSQ) EncodeVectors(vecs []float32, codes []byte) error {

	if !cb.IsTrained() {
		return c.ErrCodebookNotTrained
	}
	return cb.index.EncodeVectors(vecs, codes, cb.nlist)
}

//Compute the quantized code and find the nearest centroidID
//for a given list of vectors. Must be run on a trained codebook.
func (cb *codebookIVFSQ) EncodeAndAssignVectors(vecs []float32, codes []byte, labels []int64) error {

	if !cb.IsTrained() {
		return c.ErrCodebookNotTrained
	}
	//VECTOR_TODO implement EncodeAndAssign for SQ
	return nil
}

// Find the nearest k centroidIDs for a given vector.
// Must be run on a trained codebook.
func (cb *codebookIVFSQ) FindNearestCentroids(vec []float32, k int64) ([]int64, error) {

	if !cb.IsTrained() {
		return nil, c.ErrCodebookNotTrained
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

// Decode the quantized code and return float32 vector.
// Must be run on a trained codebook.
func (cb *codebookIVFSQ) DecodeVector(code []byte, vec []float32) error {
	return cb.DecodeVectors(1, code, vec)
}

// Decode the quantized codes and return float32 vectors.
// Must be run on a trained codebook.
func (cb *codebookIVFSQ) DecodeVectors(n int, codes []byte, vecs []float32) error {

	if !cb.IsTrained() {
		return c.ErrCodebookNotTrained
	}
	return cb.index.DecodeVectors(n, codes, vecs)
}

// Compute the distance between a vector with another given set of vectors.
func (cb *codebookIVFSQ) ComputeDistance(qvec []float32, fvecs []float32, dist []float32) error {
	if cb.metric == METRIC_L2 {
		return faiss.L2sqrNy(dist, qvec, fvecs, cb.dim)
	}
	return nil
}

func (cb *codebookIVFSQ) ComputeDistanceTable(vec []float32) ([][]float32, error) {
	//Not yet implemented
	return nil, nil
}

func (cb *codebookIVFSQ) ComputeDistanceWithDT(code []byte, dtable [][]float32) float32 {
	//Not yet implemented
	return 0
}

// Size returns the memory size in bytes.
func (cb *codebookIVFSQ) Size() uint64 {

	var size uint64
	if cb.index != nil {
		//TODO the memory size is not correct
		size = cb.index.Size()
	}
	return size
}

// Close frees the memory used by codebook.
func (cb *codebookIVFSQ) Close() error {

	if cb.index == nil {
		return c.ErrCodebookClosed
	} else {
		cb.index.Close()
		cb.index = nil
		cb.isTrained = false
		return nil
	}
}

func (cb *codebookIVFSQ) Marshal() ([]byte, error) {

	cbio := new(codebookIVFSQ_IO)

	cbio.Dim = cb.dim
	cbio.SQRange = cb.sqRange
	cbio.Nlist = cb.nlist
	cbio.IsTrained = cb.isTrained
	cbio.Metric = cb.metric

	data, err := faiss.WriteIndexIntoBuffer(cb.index)
	if err != nil {
		return nil, err
	}

	cbio.Index = data

	cbio.CodebookVer = CodebookVer1
	cbio.Checksum = crc32.ChecksumIEEE(data)

	return json.Marshal(cbio)
}

func recoverCodebookIVFSQ(data []byte) (c.Codebook, error) {

	cbio := new(codebookIVFSQ_IO)

	if err := json.Unmarshal(data, cbio); err != nil {
		return nil, err
	}

	if cbio.CodebookVer != CodebookVer1 {
		return nil, c.ErrInvalidVersion
	}

	//validate checksum
	checksum := crc32.ChecksumIEEE(cbio.Index)
	if cbio.Checksum != checksum {
		return nil, c.ErrChecksumMismatch
	}

	cb := new(codebookIVFSQ)
	cb.dim = cbio.Dim
	cb.sqRange = cbio.SQRange
	cb.nlist = cbio.Nlist
	cb.isTrained = cbio.IsTrained
	cb.metric = cbio.Metric

	var err error
	cb.index, err = faiss.ReadIndexFromBuffer(cbio.Index, faiss.IOFlagMmap)
	if err != nil {
		return nil, err
	}

	return cb, nil

}
