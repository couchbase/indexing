package planner

import (
	"testing"

	c "github.com/couchbase/indexing/secondary/common"
	"github.com/stretchr/testify/assert"
)

// Expected sizes are computed from the codebook Size() implementations that
// estimateCodebookMemUsage mirrors:
//
//	coarse centroids = nlist * dimension * sizeof(float32)
//	HNSW graph       = nlist * 32 (connections) * 4 (storage_idx_t) * 2
//
// For sparse vectors the dimension is the SparseJL reduced dimension, not the
// (absent) declared dimension of the index definition.
func TestEstimateCodebookMemUsage(t *testing.T) {

	sparseMeta := func(sparseJLDim int) *c.VectorMetadata {
		return &c.VectorMetadata{
			Similarity:        c.DOT,
			SparseJLDimension: sparseJLDim,
			Quantizer:         &c.VectorQuantizer{Type: c.NO_QUANTIZATION_SPARSE},
		}
	}

	tests := []struct {
		name     string
		vecMeta  *c.VectorMetadata
		nlist    int
		expected uint64
	}{
		{
			// Sparse vectors carry no declared dimension, so the default
			// SparseJL dimension (2048) is used.
			// coarse: 1024*2048*4 = 8388608, hnsw: 1024*32*4*2 = 262144
			name:     "sparse with default sparseJL dimension",
			vecMeta:  sparseMeta(0),
			nlist:    1024,
			expected: 8650752,
		},
		{
			// coarse: 100*512*4 = 204800, hnsw: 100*32*4*2 = 25600
			name:     "sparse with explicit sparseJL dimension",
			vecMeta:  sparseMeta(512),
			nlist:    100,
			expected: 230400,
		},
		{
			name:     "sparse with zero nlist",
			vecMeta:  sparseMeta(512),
			nlist:    0,
			expected: 0,
		},
		{
			name:     "sparse with negative nlist",
			vecMeta:  sparseMeta(512),
			nlist:    -1,
			expected: 0,
		},
		{
			// Sparse vectors are never quantized, so a declared dimension is
			// ignored in favour of the SparseJL dimension.
			// coarse: 100*512*4 = 204800, hnsw: 100*32*4*2 = 25600
			name: "sparse ignores declared dimension",
			vecMeta: &c.VectorMetadata{
				Dimension:         128,
				Similarity:        c.DOT,
				SparseJLDimension: 512,
				Quantizer:         &c.VectorQuantizer{Type: c.NO_QUANTIZATION_SPARSE},
			},
			nlist:    100,
			expected: 230400,
		},
		{
			// Dense SQ8 stays on the declared dimension.
			// coarse: 1024*128*4 = 524288, sq: 2*128*4 = 1024,
			// hnsw: 1024*32*4*2 = 262144
			name: "dense SQ8",
			vecMeta: &c.VectorMetadata{
				Dimension:  128,
				Similarity: c.L2_SQUARED,
				Quantizer:  &c.VectorQuantizer{Type: c.SQ, SQRange: c.SQ_8BIT},
			},
			nlist:    1024,
			expected: 787456,
		},
		{
			name: "dense with zero dimension",
			vecMeta: &c.VectorMetadata{
				Similarity: c.L2_SQUARED,
				Quantizer:  &c.VectorQuantizer{Type: c.SQ, SQRange: c.SQ_8BIT},
			},
			nlist:    1024,
			expected: 0,
		},
		{
			name:     "nil vector metadata",
			vecMeta:  nil,
			nlist:    1024,
			expected: 0,
		},
		{
			name:     "nil quantizer",
			vecMeta:  &c.VectorMetadata{Dimension: 128},
			nlist:    1024,
			expected: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, estimateCodebookMemUsage(test.vecMeta, test.nlist))
		})
	}
}
