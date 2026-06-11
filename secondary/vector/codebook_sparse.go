// Copyright 2026-Present Couchbase, Inc.
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
	"math"

	"github.com/couchbase/indexing/secondary/common"
	c "github.com/couchbase/indexing/secondary/vector/codebook"
	faiss "github.com/couchbase/indexing/secondary/vector/faiss"
)

const DEFAULT_SPARSEJL_DIM = 2048

// getSparseNormalizationEnabled returns whether L2 normalization is enabled
// for sparse vector training and query operations.
// [SPARSE_TODO] this config is currently being added for perf testing only.
// If required for production, needs to be integrated properly in codebook
// lifecycle.
func getSparseNormalizationEnabled() bool {
	return common.SystemConfig["indexer.vector.sparse.enableNormalization"].Bool()
}

type codebookSparse struct {
	dim   int //SparseJL reduced dimension
	nlist int //number of IVF centroids

	isTrained bool

	metric c.MetricType //metric is inner product for Sparse

	index *faiss.IndexImpl

	// Training-time weight histogram and derived prune threshold. nil when
	// histogram-based pruning was disabled at training, or when loading an
	// older codebook that predates this field.
	weightHist     *common.WeightHistogram
	derivedTau     float32
	retainedL1Frac float64
}

type codebookSparse_IO struct {
	Dim   int `json:"dim,omitempty"`
	Nlist int `json:"nlist,omitempty"`

	IsTrained bool         `json:"istrained,omitempty"`
	Metric    c.MetricType `json:"metric,omitempty"`

	Checksum    uint32      `json:"checksum,omitempty"`
	CodebookVer CodebookVer `json:"codebookver,omitempty"`

	Index []byte `json:"index,omitempty"`

	// Optional, added post-CodebookVer1. omitempty keeps older codebooks
	// (no histogram fields in JSON) deserializing cleanly into a struct
	// where these stay zero/nil.
	WeightHist     *common.WeightHistogram `json:"weight_hist,omitempty"`
	DerivedTau     float32                 `json:"derived_tau,omitempty"`
	RetainedL1Frac float64                 `json:"retained_l1_frac,omitempty"`
}

// Create a new Sparse codebook.
// dim is the SparseJL reduced dimension.
// nlist is the number of IVF centroids.
func NewCodebookSparse(dim, nlist int) (c.SparseCodebook, error) {
	var err error

	codebook := &codebookSparse{
		dim:    dim,
		nlist:  nlist,
		metric: c.METRIC_INNER_PRODUCT,
	}

	faissMetric := convertToFaissMetric(codebook.metric)

	codebook.index, err = NewIndexIVF_HNSW(dim, nlist, faissMetric)
	if err != nil || codebook.index == nil {
		errStr := fmt.Sprintf("Unable to create index. Err %v", err)
		return nil, errors.New(errStr)
	}
	return codebook, nil

}

// Train the codebook using input vectors.
// For Sparse, input vector is expected in SparseJL format.
// Clustering is done using spherical k-means. Faiss automatically
// uses spherical k-means when the metric is inner product
// (see faiss/IndexIVF.cpp). Faiss normalizes centroids but not input
// vectors, so we normalize them here before training if enabled.
func (cb *codebookSparse) Train(vecs []float32) error {

	c.AcquireTraining()
	defer c.ReleaseTraining()

	if cb.index == nil {
		return c.ErrCodebookClosed
	}

	nvecs := len(vecs) / cb.dim
	trainVecs := vecs

	// Normalize input vectors for spherical k-means if enabled.
	// Faiss only normalizes centroids, not input vectors.
	if getSparseNormalizationEnabled() {
		normalizedVecs := make([]float32, len(vecs))
		copy(normalizedVecs, vecs)
		faiss.RenormL2(cb.dim, nvecs, normalizedVecs)
		trainVecs = normalizedVecs
	}

	// If number of centroids is equal to the number of input
	// vectors for training, then clustering is not required.
	// Add the centroids directly to the quantizer from the list
	// of input vectors. Faiss considers this as a valid case to
	// skip training the quantizer.
	if cb.nlist == nvecs {
		quantizer, err := cb.index.Quantizer()
		if err != nil {
			return fmt.Errorf("failed to get quantizer: %w", err)
		}

		err = quantizer.Add(trainVecs)
		if err != nil {
			return fmt.Errorf("failed to add centroids to quantizer: %w", err)
		}
	}

	err := cb.index.Train(trainVecs)
	if err == nil {
		cb.isTrained = true
	} else {
		return fmt.Errorf("failed to train sparse codebook: %w", err)
	}

	return nil
}

// IsTrained returns true if codebook has been trained.
func (cb *codebookSparse) IsTrained() bool {

	if cb.index == nil {
		return false
	}

	if !cb.isTrained {
		return cb.index.IsTrained()
	} else {
		return cb.isTrained
	}
}

// Dimension represents the reduced SparseJL dimension
func (cb *codebookSparse) Dimension() int {
	return cb.dim
}

// Find the nearest k centroidIDs for a given vector.
// Must be run on a trained codebook.
// For Sparse, input vector is expected in SparseJL format.
func (cb *codebookSparse) FindNearestCentroids(vec []float32, k int64) ([]int64, error) {

	token := c.AcquireGlobal()
	defer c.ReleaseGlobal(token)

	if !cb.IsTrained() {
		return nil, c.ErrCodebookNotTrained
	}
	quantizer, err := cb.index.Quantizer()
	if err != nil {
		return nil, fmt.Errorf("failed to get quantizer: %w", err)
	}

	queryVec := vec

	// Normalize the input vector to match the normalized centroids
	// from spherical k-means training if enabled.
	if getSparseNormalizationEnabled() {
		normalizedVec := make([]float32, len(vec))
		copy(normalizedVec, vec)
		faiss.RenormL2(cb.dim, 1, normalizedVec)
		queryVec = normalizedVec
	}

	// If k is greater than default (efSearch=16), then set it
	// to be 2x more than k. This ensures that the required number of
	// centroids are always returned. If efSearch is smaller,
	// assign may not be able to find enough centroids.
	if k > faiss.DEFAULT_EF_SEARCH {
		ps, err := faiss.NewParameterSpace()
		if err != nil {
			return nil, fmt.Errorf("failed to create parameter space: %w", err)
		}
		ps.SetIndexParameter(quantizer, "efSearch", float64(k*2))
		ps.Delete()
	}

	labels, err := quantizer.Assign(queryVec, k)
	if err != nil {
		return nil, fmt.Errorf("failed to assign vector to centroids: %w", err)
	}
	return labels, nil

}

// Compute the distance between a vector with another given set of vectors.
// For Sparse, input is the transposed vector after query term matching,
// returns the inner product score. The length of all input vecs must match
// the length of qvec.

func (cb *codebookSparse) ComputeDistance(qvec []float32, fvecs []float32, dist []float32) error {

	token := c.AcquireGlobal()
	defer c.ReleaseGlobal(token)

	err := faiss.InnerProductsNy(dist, qvec, fvecs, len(qvec))
	if err != nil {
		return fmt.Errorf("failed to compute inner products: %w", err)
	}

	// InnerProduct is a similarity measure,
	// to convert to distance measure negate it.
	for i := range dist {
		dist[i] = -1 * dist[i]
	}
	return nil
}

// Size returns the memory size in bytes.
// Size() should be used after codebook is trained.
func (cb *codebookSparse) Size() int64 {
	var totalSize int64

	if cb.index != nil {
		const float32Size = 4
		// Size of storage_idx_t, used for internal storage of vectors (32 bits)
		const hnswIndexStorageSize = 4
		// Number of connections per point (set to 32 by default)
		const numConnection = 32

		coarseCbSize := int64(cb.nlist * cb.dim * float32Size)

		// Memory usage for HNSW graph as IVF_HNSW is used
		// This memory is used for maintaing centroids' HNSW structure.
		// ref: https://github.com/facebookresearch/faiss/wiki/Guidelines-to-choose-an-index#if-not-hnswm-or-ivf1024pqnx4fsrflat
		hnswGraphSize := int64(cb.nlist * numConnection * hnswIndexStorageSize * 2)

		totalSize = coarseCbSize + hnswGraphSize
	}
	return totalSize
}

// Close frees the memory used by codebook.
func (cb *codebookSparse) Close() error {

	if cb.index == nil {
		return c.ErrCodebookClosed
	} else {
		cb.index.Close()
		cb.index = nil
		cb.isTrained = false
		return nil
	}
}

func (cb *codebookSparse) Marshal() ([]byte, error) {

	token := c.AcquireGlobal()
	defer c.ReleaseGlobal(token)

	cbio := new(codebookSparse_IO)

	cbio.Dim = cb.dim
	cbio.Nlist = cb.nlist
	cbio.IsTrained = cb.isTrained
	cbio.Metric = cb.metric

	data, err := faiss.WriteIndexIntoBuffer(cb.index)
	if err != nil {
		return nil, fmt.Errorf("failed to write index to buffer: %w", err)
	}

	cbio.Index = data

	cbio.CodebookVer = CodebookVer1
	cbio.Checksum = crc32.ChecksumIEEE(data)

	// Histogram fields are optional; only round-trip them when present.
	cbio.WeightHist = cb.weightHist
	cbio.DerivedTau = cb.derivedTau
	cbio.RetainedL1Frac = cb.retainedL1Frac

	return json.Marshal(cbio)
}

func (cb *codebookSparse) NumCentroids() int {
	return cb.nlist
}

func (cb *codebookSparse) MetricType() c.MetricType {
	return cb.metric
}

// SetWeightHistogram stores the training-time weight histogram and the
// derived prune threshold on the codebook so they survive Marshal/recover.
// Intended to be called by the slice once, immediately after Train completes
// successfully and before the codebook is persisted.
func (cb *codebookSparse) SetWeightHistogram(hist *common.WeightHistogram,
	derivedTau float32, retainedL1Frac float64) error {

	token := c.AcquireGlobal()
	defer c.ReleaseGlobal(token)

	if cb.index == nil {
		return c.ErrCodebookClosed
	}
	cb.weightHist = hist
	cb.derivedTau = derivedTau
	cb.retainedL1Frac = retainedL1Frac
	return nil
}

// DerivedTau returns the threshold computed from the training histogram, or
// 0 when no histogram has been attached.
func (cb *codebookSparse) DerivedTau() float32 {
	return cb.derivedTau
}

// WeightHistogramSummary returns persisted histogram observations and the
// derived threshold for stats and logging. available=false signals there is
// no histogram on the codebook (older codebook, or feature was off at
// training time).
func (cb *codebookSparse) WeightHistogramSummary() (uint64, float64, float32, bool) {
	if cb.weightHist == nil {
		return 0, 0, 0, false
	}
	return cb.weightHist.TotalObs, cb.retainedL1Frac, cb.derivedTau, true
}

func recoverCodebookSparse(data []byte) (c.SparseCodebook, error) {
	cbio := new(codebookSparse_IO)

	if err := json.Unmarshal(data, cbio); err != nil {
		return nil, fmt.Errorf("failed to unmarshal codebook data: %w", err)
	}

	if cbio.CodebookVer != CodebookVer1 {
		return nil, c.ErrInvalidVersion
	}

	//validate checksum
	checksum := crc32.ChecksumIEEE(cbio.Index)
	if cbio.Checksum != checksum {
		return nil, c.ErrChecksumMismatch
	}

	cb := new(codebookSparse)
	cb.dim = cbio.Dim
	cb.nlist = cbio.Nlist
	cb.isTrained = cbio.IsTrained
	cb.metric = cbio.Metric

	// Optional fields — zero values on older codebooks mean "no histogram".
	cb.weightHist = cbio.WeightHist
	cb.derivedTau = cbio.DerivedTau
	cb.retainedL1Frac = cbio.RetainedL1Frac

	var err error
	cb.index, err = faiss.ReadIndexFromBuffer(cbio.Index, faiss.IOFlagMmap)
	if err != nil {
		return nil, fmt.Errorf("failed to read index from buffer: %w", err)
	}

	return cb, nil

}

// Helper function to convert a sparse vector from concise to SparseJL format
// concise format = [size,index1,index2,..... , value1, value2,....]
func (cb *codebookSparse) Concise2SparseJL(concise, out []float32) error {

	token := c.AcquireGlobal()
	defer c.ReleaseGlobal(token)

	err := ProjectSparseConcise(concise, out, cb.dim, 3, uint64(42)) //TODO fix seed and s value
	return err
}

//////////////////////////////////////////////////
// Sparse JL projection - copied from bhive/sparse.go
//////////////////////////////////////////////////

// ProjectSparseConcise projects a sparse vector from concise format to SparseJL format.
// concise format = [size,index1,index2,..... , value1, value2,....]
// size is the number of non-zero elements in the vector
// index1,index2,..... are the indices of the non-zero elements
// value1, value2,.... are the values of the non-zero elements
func ProjectSparseConcise(concise, out []float32, m, s int, seed uint64) error {

	if len(concise) == 0 {
		return errors.New("sparse input length is 0")
	}

	if len(out) != m {
		return errors.New("sparse output length mismatch")
	}

	size := int(concise[0])

	if len(concise) != 2*size+1 {
		return errors.New("sparse input length mismatch")
	}

	// scale ensures each non-zero placed per input feature has magnitude 1/√s.
	// With s placements over m rows, a given row receives a non-zero with prob s/m;
	// hence E[a_{r,i}^2] = (s/m) * (1/s) = 1/m. This matches the JL requirement
	// without an additional 1/√m factor. The overall variance normalization is
	// implicit in the sparsity scheme.
	scale := float32(1.0 / math.Sqrt(float64(s)))

	for i := 0; i < int(size); i++ {
		col := uint64(concise[i+1])
		v := concise[i+size+1]

		// Generate s (row, sign) pairs per column deterministically from seed and column id.
		for k := 0; k < s; k++ {
			// Mix the seed, column, and repetition index k.
			mixKey := seed ^ (col * 0x9e3779b97f4a7c15) ^ (uint64(k) * 0x94d049bb133111eb)
			h := splitmix64(mixKey)

			row := int(h % uint64(m))
			if row < 0 || row >= m {
				// Defensive, though modular arithmetic guarantees bounds
				continue
			}

			// Highest bit decides the sign
			var sgn float32 = 1.0
			if (h>>63)&1 == 1 {
				sgn = -1.0
			}

			out[row] += sgn * v * scale
		}
	}

	return nil
}

// splitmix64 is a fast 64-bit mixer suitable for simple deterministic hashing.
// Source: Public-domain SplitMix64 (Sebastiano Vigna).
func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	z := x
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// Transpose performs query term matching between the input query
// and sparse vector and stores the values for the matching terms
// in the `result`. It also returns a bool to indicate if there were
// any matching terms.
// Input for q and s must be in the concise format.
// Output `result` stores only the values of the matching terms.

// This function has been taken from bhive.
// https://github.com/couchbase/bhive/blob/main/vanama_quantized.go
func (cb *codebookSparse) Transpose(q []float32, s []float32, result []float32) bool {
	nqdim := int(q[0])       // #dim query
	ndim := int(s[0])        // #dim vec
	q = q[1 : 1+nqdim]       // query dim
	d := s[1 : 1+ndim]       // vec dim
	s = s[1+ndim : 1+2*ndim] // vec value
	i := 0                   // query dim pos
	j := 0                   // data dim pos

	keep := false
	for ; i < nqdim; i++ {
		for ; j < ndim && q[i] > d[j]; j++ { // fast forward when query dim > vector dim
		}

		if j < ndim && q[i] == d[j] { // dimension matches
			result[i] = s[j]
			j++
			keep = true // at least 1 dim matches
		} else {
			result[i] = 0
		}
	}

	return keep
}

// TransposeQuantized is Transpose for a scalar-quantized document vector. The
// query q is float32 concise; encoded is a common.QuantizedSparseVector packed
// buffer. Matched doc weights are dequantized (code * per-vector scale) into
// result, aligned to query term positions. Returns true if any term matched.
//
// Both query dims and the quantized doc indices must be ascending, matching
// the invariant the float32 Transpose relies on. Doc indices are uint16; query
// dims (integer-valued float32) are narrowed to uint16 for comparison, valid
// because the vocabulary fits uint16.
func (cb *codebookSparse) TransposeQuantized(q []float32, encoded []byte, result []float32) bool {
	nqdim := int(q[0])   // #dim query
	qd := q[1 : 1+nqdim] // query dims

	doc := common.QuantizedSparseVector(encoded)
	ndim := doc.NNZ() // #dim vec
	scale := doc.Scale()

	i := 0 // query dim pos
	j := 0 // data dim pos

	keep := false
	for ; i < nqdim; i++ {
		qdim := uint16(qd[i])
		for ; j < ndim && qdim > doc.IndexAt(j); j++ { // fast forward when query dim > vector dim
		}

		if j < ndim && qdim == doc.IndexAt(j) { // dimension matches
			result[i] = scale * float32(doc.CodeAt(j))
			j++
			keep = true // at least 1 dim matches
		} else {
			result[i] = 0
		}
	}

	return keep
}

// Not implemented for Sparse
func (cb *codebookSparse) CodeSize() (int, error) {

	return 0, c.ErrNotImplemented
}

// Not implemented for Sparse
func (cb *codebookSparse) CoarseSize() (int, error) {

	return 0, c.ErrNotImplemented
}

// Not implemented for Sparse
func (cb *codebookSparse) EncodeVector(vec []float32, code []byte) error {

	return c.ErrNotImplemented
}

// Not implemented for Sparse
func (cb *codebookSparse) EncodeVectors(vecs []float32, codes []byte) error {

	return c.ErrNotImplemented
}

// Not implemented for Sparse
func (cb *codebookSparse) EncodeAndAssignVectors(vecs []float32, codes []byte, labels []int64) error {

	return c.ErrNotImplemented
}

// Not implemented for Sparse
func (cb *codebookSparse) DecodeVector(code []byte, vec []float32) error {
	return c.ErrNotImplemented
}

// Not implemented for Sparse
func (cb *codebookSparse) DecodeVectors(n int, codes []byte, vecs []float32) error {

	return c.ErrNotImplemented
}

// Not implemented for Sparse
func (cb *codebookSparse) ComputeDistanceTable(vec []float32, dtable []float32) error {

	return c.ErrNotImplemented
}

// Not implemented for Sparse
func (cb *codebookSparse) ComputeDistanceWithDT(code []byte, dtable []float32) float32 {

	return 0
}

// Not implemented for Sparse
func (cb *codebookSparse) ComputeDistanceEncoded(qvec []float32,
	n int, codes []byte, dists []float32, dtable []float32, listno int64) error {

	return c.ErrNotImplemented
}
