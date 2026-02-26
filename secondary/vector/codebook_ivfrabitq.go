// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt. As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package vector

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"

	c "github.com/couchbase/indexing/secondary/vector/codebook"
	faiss "github.com/couchbase/indexing/secondary/vector/faiss"
)

const defaultRaBitQQB = 4

var errUnableToCreateRaBitQIndex = errors.New("unable to create RaBitQ index")

type codebookIVFRaBitQ struct {
	dim int // vector dimension

	nlist int // number of centroids
	nbits int // number of bits per dimension
	qb    int // query quantization bits

	isTrained bool

	metric    c.MetricType // metric
	useCosine bool         // use cosine similarity

	index *faiss.IndexImpl
}

type codebookIVFRaBitQ_IO struct {
	Dim   int `json:"dim,omitempty"`
	Nlist int `json:"nlist,omitempty"`
	Nbits int `json:"nbits,omitempty"`
	Qb    int `json:"qb,omitempty"` // May need to keep if we want to support full precision query in future

	IsTrained bool         `json:"istrained,omitempty"`
	Metric    c.MetricType `json:"metric,omitempty"`
	UseCosine bool         `json:"usecosine,omitempty"`

	Checksum    uint32      `json:"checksum,omitempty"`
	CodebookVer CodebookVer `json:"codebookver,omitempty"`

	Index []byte `json:"index,omitempty"`
}

func NewCodebookIVFRaBitQ(
	dim,
	nlist,
	nbits int,
	metric c.MetricType,
	useCosine bool,
) (c.Codebook, error) {

	if (useCosine && metric != c.METRIC_INNER_PRODUCT) ||
		(metric != c.METRIC_L2 && metric != c.METRIC_INNER_PRODUCT) {

		return nil, c.ErrUnsupportedMetric
	}

	if nbits == 0 {
		nbits = 1
	}

	cb := &codebookIVFRaBitQ{
		dim:       dim,
		nlist:     nlist,
		nbits:     nbits,
		qb:        defaultRaBitQQB,
		metric:    metric,
		useCosine: useCosine,
	}

	faissMetric := convertToFaissMetric(metric)

	index, err := NewIndexIVFRaBitQ_HNSW(dim, nlist, nbits, faissMetric)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errUnableToCreateRaBitQIndex, err)
	}
	if index == nil {
		return nil, errUnableToCreateRaBitQIndex
	}

	cb.index = index
	return cb, nil
}

func (cb *codebookIVFRaBitQ) setQb() error {
	ps, err := faiss.NewParameterSpace()
	if err != nil {
		return fmt.Errorf("set qb: create parameter space: %w", err)
	}
	defer ps.Delete()

	err = ps.SetIndexParameter(cb.index, "qb", float64(cb.qb))
	if err != nil {
		return fmt.Errorf("set qb: %w", err)
	}

	return nil
}

func (cb *codebookIVFRaBitQ) Train(vecs []float32) error {
	c.AcquireTraining()
	defer c.ReleaseTraining()

	if cb.index == nil {
		return c.ErrCodebookClosed
	}

	if cb.useCosine {
		nx := len(vecs) / cb.dim
		faiss.RenormL2(cb.dim, nx, vecs)
	}

	nvecs := len(vecs) / cb.dim
	if cb.nlist == nvecs {
		quantizer, err := cb.index.Quantizer()
		if err != nil {
			return fmt.Errorf("train codebook: get quantizer: %w", err)
		}

		err = quantizer.Add(vecs)
		if err != nil {
			return fmt.Errorf("train codebook: add centroids to quantizer: %w", err)
		}
	}

	err := cb.index.Train(vecs)
	if err != nil {
		return fmt.Errorf("train codebook: %w", err)
	}

	err = cb.setQb()
	if err != nil {
		return fmt.Errorf("train codebook: set qb: %w", err)
	}

	cb.isTrained = true
	return nil
}

func (cb *codebookIVFRaBitQ) IsTrained() bool {
	if cb.index == nil {
		return false
	}

	if cb.isTrained {
		return true
	}

	return cb.index.IsTrained()
}

func (cb *codebookIVFRaBitQ) CodeSize() (int, error) {
	if !cb.IsTrained() {
		return 0, c.ErrCodebookNotTrained
	}

	size, err := cb.index.CodeSize()
	if err != nil {
		return 0, fmt.Errorf("get code size: %w", err)
	}

	return size, nil
}

func (cb *codebookIVFRaBitQ) CoarseSize() (int, error) {
	if !cb.IsTrained() {
		return 0, c.ErrCodebookNotTrained
	}

	return computeCoarseCodeSize(cb.nlist), nil
}

func (cb *codebookIVFRaBitQ) EncodeVector(vec []float32, code []byte) error {
	return cb.EncodeVectors(vec, code)
}

func (cb *codebookIVFRaBitQ) EncodeVectors(vecs []float32, codes []byte) error {
	token := c.AcquireGlobal()
	defer c.ReleaseGlobal(token)

	if !cb.IsTrained() {
		return c.ErrCodebookNotTrained
	}

	if cb.useCosine {
		nx := len(vecs) / cb.dim
		faiss.RenormL2(cb.dim, nx, vecs)
	}

	err := cb.index.EncodeVectors(vecs, codes, cb.nlist)
	if err != nil {
		return fmt.Errorf("encode vectors: %w", err)
	}

	return nil
}

func (cb *codebookIVFRaBitQ) EncodeAndAssignVectors(
	vecs []float32,
	codes []byte,
	labels []int64,
) error {

	token := c.AcquireGlobal()
	defer c.ReleaseGlobal(token)

	if !cb.IsTrained() {
		return c.ErrCodebookNotTrained
	}

	if cb.useCosine {
		nx := len(vecs) / cb.dim
		faiss.RenormL2(cb.dim, nx, vecs)
	}

	err := cb.index.EncodeAndAssignRaBitQ(vecs, codes, labels, cb.nlist)
	if err != nil {
		return fmt.Errorf("encode and assign vectors: %w", err)
	}

	return nil
}

func (cb *codebookIVFRaBitQ) FindNearestCentroids(vec []float32, k int64) ([]int64, error) {
	token := c.AcquireGlobal()
	defer c.ReleaseGlobal(token)

	if !cb.IsTrained() {
		return nil, c.ErrCodebookNotTrained
	}

	quantizer, err := cb.index.Quantizer()
	if err != nil {
		return nil, fmt.Errorf("find nearest centroids: get quantizer: %w", err)
	}

	if cb.useCosine {
		nx := len(vec) / cb.dim
		faiss.RenormL2(cb.dim, nx, vec)
	}

	if k > faiss.DEFAULT_EF_SEARCH {
		ps, err := faiss.NewParameterSpace()
		if err != nil {
			return nil, fmt.Errorf("find nearest centroids: create parameter space: %w", err)
		}
		defer ps.Delete()

		err = ps.SetIndexParameter(quantizer, "efSearch", float64(k*2))
		if err != nil {
			return nil, fmt.Errorf("find nearest centroids: set efSearch: %w", err)
		}
	}

	labels, err := quantizer.Assign(vec, k)
	if err != nil {
		return nil, fmt.Errorf("find nearest centroids: %w", err)
	}

	return labels, nil
}

func (cb *codebookIVFRaBitQ) DecodeVector(code []byte, vec []float32) error {
	return cb.DecodeVectors(1, code, vec)
}

func (cb *codebookIVFRaBitQ) DecodeVectors(n int, codes []byte, vecs []float32) error {
	token := c.AcquireGlobal()
	defer c.ReleaseGlobal(token)

	if !cb.IsTrained() {
		return c.ErrCodebookNotTrained
	}

	err := cb.index.DecodeVectors(n, codes, vecs)
	if err != nil {
		return fmt.Errorf("decode vectors: %w", err)
	}

	return nil
}

func (cb *codebookIVFRaBitQ) applyInnerProductDistanceTransform(dists []float32) {
	if cb.useCosine {
		for i := range dists {
			dists[i] = 1 - dists[i]
			dists[i] = clip(dists[i], 0, 2)
		}
		return
	}

	for i := range dists {
		dists[i] = -1 * dists[i]
	}
}

func (cb *codebookIVFRaBitQ) ComputeDistance(
	qvec []float32,
	fvecs []float32,
	dist []float32,
) error {

	token := c.AcquireGlobal()
	defer c.ReleaseGlobal(token)

	switch cb.metric {
	case c.METRIC_L2:
		err := faiss.L2sqrNy(dist, qvec, fvecs, cb.dim)
		if err != nil {
			return fmt.Errorf("compute distance: %w", err)
		}
	case c.METRIC_INNER_PRODUCT:
		var err error
		if cb.useCosine {
			err = faiss.CosineSimNy(dist, qvec, fvecs, cb.dim)
		} else {
			err = faiss.InnerProductsNy(dist, qvec, fvecs, cb.dim)
		}
		if err != nil {
			return fmt.Errorf("compute distance: %w", err)
		}

		cb.applyInnerProductDistanceTransform(dist)
	}

	return nil
}

func (cb *codebookIVFRaBitQ) ComputeDistanceTable(vec []float32, dtable []float32) error {
	token := c.AcquireGlobal()
	defer c.ReleaseGlobal(token)

	return nil
}

func (cb *codebookIVFRaBitQ) ComputeDistanceWithDT(code []byte, dtable []float32) float32 {
	token := c.AcquireGlobal()
	defer c.ReleaseGlobal(token)

	return 0
}

func (cb *codebookIVFRaBitQ) ComputeDistanceEncoded(
	qvec []float32,
	n int,
	codes []byte,
	dists []float32,
	dtable []float32,
	listno int64,
) error {

	token := c.AcquireGlobal()
	defer c.ReleaseGlobal(token)

	err := cb.index.ComputeDistanceEncoded(
		qvec,
		n,
		codes,
		dists,
		nil,
		listno,
		convertToFaissMetric(cb.metric),
		cb.dim,
	)
	if err != nil {
		return fmt.Errorf("compute encoded distance: %w", err)
	}

	if cb.metric == c.METRIC_INNER_PRODUCT {
		cb.applyInnerProductDistanceTransform(dists)
	}

	return nil
}

func (cb *codebookIVFRaBitQ) Size() int64 {
	var totalSize int64

	if cb.index != nil {
		const float32Size = 4
		const hnswIndexStorageSize = 4
		const numConnection = 32

		coarseCbSize := int64(cb.nlist * cb.dim * float32Size)

		hnswGraphSize := int64(cb.nlist * numConnection * hnswIndexStorageSize * 2)

		totalSize = coarseCbSize + hnswGraphSize
	}

	return totalSize
}

func (cb *codebookIVFRaBitQ) Close() error {
	if cb.index == nil {
		return c.ErrCodebookClosed
	}

	cb.index.Close()
	cb.index = nil
	cb.isTrained = false

	return nil
}

func (cb *codebookIVFRaBitQ) Marshal() ([]byte, error) {
	token := c.AcquireGlobal()
	defer c.ReleaseGlobal(token)

	cbio := &codebookIVFRaBitQ_IO{
		Dim:         cb.dim,
		Nlist:       cb.nlist,
		Nbits:       cb.nbits,
		Qb:          cb.qb,
		IsTrained:   cb.isTrained,
		Metric:      cb.metric,
		UseCosine:   cb.useCosine,
		CodebookVer: CodebookVer1,
	}

	data, err := faiss.WriteIndexIntoBuffer(cb.index)
	if err != nil {
		return nil, fmt.Errorf("marshal codebook: write index: %w", err)
	}

	cbio.Index = data
	cbio.Checksum = crc32.ChecksumIEEE(data)

	payload, err := json.Marshal(cbio)
	if err != nil {
		return nil, fmt.Errorf("marshal codebook: %w", err)
	}

	return payload, nil
}

func (cb *codebookIVFRaBitQ) NumCentroids() int {
	return cb.nlist
}

func (cb *codebookIVFRaBitQ) MetricType() c.MetricType {
	return cb.metric
}

func (cb *codebookIVFRaBitQ) Dimension() int {
	return cb.dim
}

func recoverCodebookIVFRaBitQ(data []byte) (c.Codebook, error) {
	cbio := new(codebookIVFRaBitQ_IO)

	err := json.Unmarshal(data, cbio)
	if err != nil {
		return nil, fmt.Errorf("recover codebook: unmarshal payload: %w", err)
	}

	if cbio.CodebookVer != CodebookVer1 {
		return nil, c.ErrInvalidVersion
	}

	checksum := crc32.ChecksumIEEE(cbio.Index)
	if cbio.Checksum != checksum {
		return nil, c.ErrChecksumMismatch
	}

	cb := &codebookIVFRaBitQ{
		dim:       cbio.Dim,
		nlist:     cbio.Nlist,
		nbits:     cbio.Nbits,
		qb:        cbio.Qb,
		isTrained: cbio.IsTrained,
		metric:    cbio.Metric,
		useCosine: cbio.UseCosine,
	}

	if cb.qb == 0 {
		cb.qb = defaultRaBitQQB
	}

	cb.index, err = faiss.ReadIndexFromBuffer(cbio.Index, faiss.IOFlagMmap)
	if err != nil {
		return nil, fmt.Errorf("recover codebook: read index: %w", err)
	}

	if cb.isTrained {
		err = cb.setQb()
		if err != nil {
			return nil, fmt.Errorf("recover codebook: set qb: %w", err)
		}
	}

	return cb, nil
}
