// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package vector

import (
	"math"
	"math/rand"
	"testing"

	"github.com/couchbase/indexing/secondary/common"
)

// buildConcise builds a concise sparse vector from sorted (dim,value) pairs.
func buildConcise(dims []float32, vals []float32) []float32 {
	n := len(dims)
	out := make([]float32, 1+2*n)
	out[0] = float32(n)
	copy(out[1:1+n], dims)
	copy(out[1+n:], vals)
	return out
}

// TransposeQuantized must agree with Transpose on the keep flag and produce
// matched values within one quantization step of the float32 path.
func TestTransposeQuantized_ParityWithFloat32(t *testing.T) {
	cb := &codebookSparse{dim: 2048}

	query := buildConcise(
		[]float32{2, 5, 9, 40, 1000},
		[]float32{0.3, 0.9, 0.1, 0.7, 0.5},
	)
	// doc shares dims 5, 40, 1000; misses 2 and 9.
	doc := buildConcise(
		[]float32{1, 5, 7, 40, 1000},
		[]float32{4.0, 2.0, 0.25, 1.5, 0.5},
	)

	nq := int(query[0])
	resF := make([]float32, nq)
	keepF := cb.Transpose(query, doc, resF)

	enc, err := common.EncodeQuantizedSparse(doc, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	resQ := make([]float32, nq)
	keepQ := cb.TransposeQuantized(query, enc, resQ)

	if keepF != keepQ {
		t.Fatalf("keep mismatch: float=%v quant=%v", keepF, keepQ)
	}
	scale := common.QuantizedSparseVector(enc).Scale()
	for i := 0; i < nq; i++ {
		// Matched positions must agree within a step; unmatched must both be 0.
		if d := math.Abs(float64(resF[i] - resQ[i])); d > float64(scale) {
			t.Fatalf("pos %d: float=%v quant=%v diff=%v > step %v",
				i, resF[i], resQ[i], d, scale)
		}
		if resF[i] == 0 && resQ[i] != 0 {
			t.Fatalf("pos %d: float zeroed but quant=%v", i, resQ[i])
		}
	}
}

func TestTransposeQuantized_NoMatch(t *testing.T) {
	cb := &codebookSparse{dim: 2048}
	query := buildConcise([]float32{2, 4, 6}, []float32{0.5, 0.5, 0.5})
	doc := buildConcise([]float32{1, 3, 5}, []float32{1.0, 1.0, 1.0})

	enc, _ := common.EncodeQuantizedSparse(doc, nil)
	res := make([]float32, 3)
	if cb.TransposeQuantized(query, enc, res) {
		t.Fatalf("expected keep=false on disjoint dims")
	}
	for i, v := range res {
		if v != 0 {
			t.Fatalf("res[%d]=%v, want 0", i, v)
		}
	}
}

// Randomized parity: many random query/doc pairs, keep flags must always
// agree and matched values stay within a quantization step.
func TestTransposeQuantized_RandomizedParity(t *testing.T) {
	cb := &codebookSparse{dim: 4096}
	rng := rand.New(rand.NewSource(12345))

	sortedDims := func(n int) []float32 {
		seen := map[int]bool{}
		out := make([]float32, 0, n)
		for len(out) < n {
			d := rng.Intn(60000) + 1
			if seen[d] {
				continue
			}
			seen[d] = true
			out = append(out, float32(d))
		}
		// insertion sort (n is small)
		for i := 1; i < len(out); i++ {
			for j := i; j > 0 && out[j-1] > out[j]; j-- {
				out[j-1], out[j] = out[j], out[j-1]
			}
		}
		return out
	}
	randVals := func(n int) []float32 {
		v := make([]float32, n)
		for i := range v {
			v[i] = rng.Float32() * 5 // non-negative, SPLADE-ish range
		}
		return v
	}

	for iter := 0; iter < 200; iter++ {
		nq := rng.Intn(20) + 1
		nd := rng.Intn(40) + 1
		query := buildConcise(sortedDims(nq), randVals(nq))
		doc := buildConcise(sortedDims(nd), randVals(nd))

		resF := make([]float32, nq)
		keepF := cb.Transpose(query, doc, resF)

		enc, err := common.EncodeQuantizedSparse(doc, nil)
		if err != nil {
			t.Fatalf("iter %d encode: %v", iter, err)
		}
		resQ := make([]float32, nq)
		keepQ := cb.TransposeQuantized(query, enc, resQ)

		if keepF != keepQ {
			t.Fatalf("iter %d keep mismatch: float=%v quant=%v", iter, keepF, keepQ)
		}
		scale := common.QuantizedSparseVector(enc).Scale()
		for i := 0; i < nq; i++ {
			if d := math.Abs(float64(resF[i] - resQ[i])); d > float64(scale)+1e-6 {
				t.Fatalf("iter %d pos %d: float=%v quant=%v diff=%v > step %v",
					iter, i, resF[i], resQ[i], d, scale)
			}
		}
	}
}
