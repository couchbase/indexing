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

	c "github.com/couchbase/indexing/secondary/vector/codebook"
	faiss "github.com/couchbase/indexing/secondary/vector/faiss"
)

const defaultOMPThreads = 1

type codebookIVFPQ struct {
	dim      int //vector dimension
	nsub     int //number of subquantizers
	nbits    int //number of bits per subvector index
	nlist    int //number of centroids
	fastScan bool

	codeSize int //size of the code

	isTrained bool

	metric    MetricType //metric
	useCosine bool       //use cosine similarity

	index *faiss.IndexImpl
}

type codebookIVFPQ_IO struct {
	Dim      int  `json:"dim,omitempty"`
	Nsub     int  `json:"nsub,omitempty"`
	Nbits    int  `json:"nbits,omitempty"`
	Nlist    int  `json:"nlist,omitempty"`
	FastScan bool `json:"fastScan,omitempty"`

	IsTrained bool       `json:"istrained,omitempty"`
	Metric    MetricType `json:"metric,omitempty"`
	UseCosine bool       `json:"usecosine,omitempty"`

	Checksum    uint32      `json:"checksum,omitempty"`
	CodebookVer CodebookVer `json:"codebookver,omitempty"`

	Index []byte `json:"index,omitempty"`
}

func NewCodebookIVFPQ(dim, nsub, nbits, nlist int, metric MetricType, useCosine, useFastScan bool) (c.Codebook, error) {

	var err error

	if (useCosine && metric != METRIC_INNER_PRODUCT) || (metric != METRIC_L2 && metric != METRIC_INNER_PRODUCT) {
		return nil, c.ErrUnsupportedMetric
	}

	codebook := &codebookIVFPQ{
		dim:       dim,
		nsub:      nsub,
		nbits:     nbits,
		nlist:     nlist,
		fastScan:  useFastScan,
		metric:    metric,
		useCosine: useCosine,
	}

	faissMetric := convertToFaissMetric(metric)

	faiss.SetOMPThreads(defaultOMPThreads)

	codebook.index, err = NewIndexIVFPQ_HNSW(dim, nlist, nsub, nbits, faissMetric, useFastScan)
	if err != nil || codebook.index == nil {
		errStr := fmt.Sprintf("Unable to create index. Err %v", err)
		return nil, errors.New(errStr)
	}
	return codebook, nil

}

// Train the codebook using input vectors.
func (cb *codebookIVFPQ) Train(vecs []float32) error {

	if cb.index == nil {
		return c.ErrCodebookClosed
	}

	if cb.useCosine {
		nx := len(vecs) / cb.dim
		faiss.RenormL2(cb.dim, nx, vecs)
	}

	//If number of centroids is equal to the number of input
	//vectors for training, then clustering is not required.
	//Add the centroids directly to the quantizer from the list
	//of input vectors. Faiss considers this as a valid case to
	//skip training the quantizer. The sub-quantizer training will
	//still happen.
	nvecs := len(vecs) / cb.dim
	if cb.nlist == nvecs {
		quantizer, err := cb.index.Quantizer()
		if err != nil {
			return err
		}
		err = quantizer.Add(vecs)
		if err != nil {
			return err
		}
	}
	err := cb.index.Train(vecs)
	if err == nil {
		cb.isTrained = true
	}
	return err
}

// IsTrained returns true if codebook has been trained.
func (cb *codebookIVFPQ) IsTrained() bool {

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
func (cb *codebookIVFPQ) CodeSize() (int, error) {

	if !cb.IsTrained() {
		return 0, c.ErrCodebookNotTrained
	}
	return cb.index.CodeSize()
}

//compute coarse code size based on nlist
func computeCoarseCodeSize(nlist int) int {

	nl := nlist - 1
	nbyte := 0
	for nl > 0 {
		nbyte++
		nl >>= 8
	}
	return nbyte
}

// CoarseSize returns the size of produced code in bytes.
func (cb *codebookIVFPQ) CoarseSize() (int, error) {

	if !cb.IsTrained() {
		return 0, c.ErrCodebookNotTrained
	}
	return computeCoarseCodeSize(cb.nlist), nil
}

// Compute the quantized code for a given input vector.
// Must be run on a trained codebook.
func (cb *codebookIVFPQ) EncodeVector(vec []float32, code []byte) error {

	return cb.EncodeVectors(vec, code)
}

// Compute the quantized code for a given input vector.
// Must be run on a trained codebook.
func (cb *codebookIVFPQ) EncodeVectors(vecs []float32, codes []byte) error {

	if !cb.IsTrained() {
		return c.ErrCodebookNotTrained
	}

	if cb.useCosine {
		nx := len(vecs) / cb.dim
		faiss.RenormL2(cb.dim, nx, vecs)
	}

	return cb.index.EncodeVectors(vecs, codes, cb.nlist)
}

//Compute the quantized code and find the nearest centroidID
//for a given list of vectors. Must be run on a trained codebook.
func (cb *codebookIVFPQ) EncodeAndAssignVectors(vecs []float32, codes []byte, labels []int64) error {

	if !cb.IsTrained() {
		return c.ErrCodebookNotTrained
	}

	if cb.useCosine {
		nx := len(vecs) / cb.dim
		faiss.RenormL2(cb.dim, nx, vecs)
	}

	return cb.index.EncodeAndAssignPQ(vecs, codes, labels, cb.nsub, cb.nbits, cb.nlist)
}

// Find the nearest k centroidIDs for a given vector.
// Must be run on a trained codebook.
func (cb *codebookIVFPQ) FindNearestCentroids(vec []float32, k int64) ([]int64, error) {

	if !cb.IsTrained() {
		return nil, c.ErrCodebookNotTrained
	}
	quantizer, err := cb.index.Quantizer()
	if err != nil {
		return nil, err
	}

	if cb.useCosine {
		nx := len(vec) / cb.dim
		faiss.RenormL2(cb.dim, nx, vec)
	}

	labels, err := quantizer.Assign(vec, k)
	if err != nil {
		return nil, err
	}
	return labels, nil

}

// Decode the quantized code and return float32 vector.
// Must be run on a trained codebook.
func (cb *codebookIVFPQ) DecodeVector(code []byte, vec []float32) error {
	return cb.DecodeVectors(1, code, vec)
}

// Decode the quantized codes and return float32 vectors.
// Must be run on a trained codebook.
func (cb *codebookIVFPQ) DecodeVectors(n int, codes []byte, vecs []float32) error {

	if !cb.IsTrained() {
		return c.ErrCodebookNotTrained
	}
	return cb.index.DecodeVectors(n, codes, vecs)
}

// Compute the distance between a vector with another given set of vectors.
func (cb *codebookIVFPQ) ComputeDistance(qvec []float32, fvecs []float32, dist []float32) error {
	if cb.metric == METRIC_L2 {
		return faiss.L2sqrNy(dist, qvec, fvecs, cb.dim)
	} else if cb.metric == METRIC_INNER_PRODUCT {
		if cb.useCosine {
			err := faiss.CosineSimNy(dist, qvec, fvecs, cb.dim)
			// Cosine distance is calculated as 1 - (cosine similarity).
			// Cosine similarity ranges from -1 (exactly opposite) to 1 (exactly the same),
			// while cosine distance ranges from 0 (exactly the same) to 2 (exactly opposite).
			if err == nil {
				for i := range dist {
					dist[i] = 1 - dist[i]
				}
			}
			return err
		}
		err := faiss.InnerProductsNy(dist, qvec, fvecs, cb.dim)
		// InnnerProduct is a similarity measure,
		// to convert to distance measure negate it.
		if err == nil {
			for i := range dist {
				dist[i] = -1 * dist[i]
			}
		}
		return err
	}
	return nil
}

func (cb *codebookIVFPQ) ComputeDistanceTable(vec []float32) ([][]float32, error) {
	//Not yet implemented
	return nil, nil
}

func (cb *codebookIVFPQ) ComputeDistanceWithDT(code []byte, dtable [][]float32) float32 {
	//Not yet implemented
	return 0
}

//Compute the distance between a vector and flat quantized codes.
//Quantized codes are decoded first before distance comparison.
//Codes must be provided without coarse code(i.e. centroid ID).
//This function only works with vectors belonging to the same centroid(input as listno).
func (cb *codebookIVFPQ) ComputeDistanceEncoded(qvec []float32,
	n int, codes []byte, dists []float32, listno int64) error {
	//Not yet implemented
	return nil
}

// Size returns the memory size in bytes.
// Size() should be used after codebook is trained.
func (cb *codebookIVFPQ) Size() int64 {
	var totalSize int64

	if cb.index != nil {
		const float32Size = 4
		// Size of storage_idx_t, used for internal storage of vectors (32 bits)
		const hnswIndexStorageSize = 4
		// Number of connections per point (set to 32 by default)
		const numConnection = 32

		var pqCbSize int64

		coarseCbSize := int64(cb.nlist * cb.dim * float32Size)

		// Memory usage for the Product Quantization (PQ) codebook
		// PQ quantizes each sub-vector using a codebook of quantized vectors.
		// Memory size formula: m * (2^nbits) * (d/m) * sizeof(float32)
		// - m: Number of sub-vectors, each with its own codebook.
		// - 2^nbits: Number of quantization levels (codes) for each sub-vector.
		// - d/m: Dimensionality of each sub-vector,
		//since the original vector of dimension d is split into m sub-vectors.
		// Each codebook entry represents a vector of size d/m.
		pqCbSize = int64((1 << cb.nbits) * cb.dim * float32Size)

		// Memory usage for HNSW graph as IVF_HNSW is used.
		// This memory is used for maintaing centroids' HNSW structure.
		// ref: https://github.com/facebookresearch/faiss/wiki/Guidelines-to-choose-an-index#if-not-hnswm-or-ivf1024pqnx4fsrflat
		hnswGraphSize := int64(cb.nlist * numConnection * hnswIndexStorageSize * 2)

		totalSize = coarseCbSize + pqCbSize + hnswGraphSize
	}
	return totalSize
}

// Close frees the memory used by codebook.
func (cb *codebookIVFPQ) Close() error {

	if cb.index == nil {
		return c.ErrCodebookClosed
	} else {
		cb.index.Close()
		cb.index = nil
		cb.isTrained = false
		return nil
	}
}

func (cb *codebookIVFPQ) Marshal() ([]byte, error) {

	cbio := new(codebookIVFPQ_IO)

	cbio.Dim = cb.dim
	cbio.Nsub = cb.nsub
	cbio.Nbits = cb.nbits
	cbio.Nlist = cb.nlist
	cbio.IsTrained = cb.isTrained
	cbio.Metric = cb.metric
	cbio.UseCosine = cb.useCosine

	data, err := faiss.WriteIndexIntoBuffer(cb.index)
	if err != nil {
		return nil, err
	}

	cbio.Index = data

	cbio.CodebookVer = CodebookVer1
	cbio.Checksum = crc32.ChecksumIEEE(data)

	return json.Marshal(cbio)
}

func recoverCodebookIVFPQ(data []byte) (c.Codebook, error) {

	cbio := new(codebookIVFPQ_IO)

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

	cb := new(codebookIVFPQ)
	cb.dim = cbio.Dim
	cb.nsub = cbio.Nsub
	cb.nbits = cbio.Nbits
	cb.nlist = cbio.Nlist
	cb.isTrained = cbio.IsTrained
	cb.metric = cbio.Metric
	cb.useCosine = cbio.UseCosine

	var err error
	cb.index, err = faiss.ReadIndexFromBuffer(cbio.Index, faiss.IOFlagMmap)
	if err != nil {
		return nil, err
	}

	return cb, nil

}
