// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package common

import (
	"testing"
)

// helper to build a concise sparse vector from parallel (dim, value) slices.
// Dims must already be sorted ascending.
func mkConcise(dims []uint32, vals []float32) []float32 {
	n := len(dims)
	out := make([]float32, 1+2*n)
	out[0] = float32(n)
	for i, d := range dims {
		out[1+i] = float32(d)
	}
	copy(out[1+n:], vals)
	return out
}

func TestTruncateConciseTopN_NoOpWhenDisabled(t *testing.T) {
	in := mkConcise([]uint32{1, 5, 9}, []float32{0.1, 0.9, 0.4})
	out, truncated := TruncateConciseTopN(in, 0, nil)
	if truncated {
		t.Fatalf("expected no truncation when maxNNZ=0")
	}
	if &out[0] != &in[0] {
		t.Fatalf("expected zero-copy return of input")
	}
}

func TestTruncateConciseTopN_NoOpWhenAlreadySmall(t *testing.T) {
	in := mkConcise([]uint32{1, 5, 9}, []float32{0.1, 0.9, 0.4})
	out, truncated := TruncateConciseTopN(in, 10, nil)
	if truncated {
		t.Fatalf("expected no truncation when nnz < maxNNZ")
	}
	if &out[0] != &in[0] {
		t.Fatalf("expected zero-copy return of input")
	}
}

func TestTruncateConciseTopN_PicksTopByAbsValueAndKeepsDimOrder(t *testing.T) {
	// Values chosen so |.| ordering differs from value ordering.
	// dim->|val|: 2->0.10, 5->0.90, 7->0.30, 11->0.80, 14->0.05
	// Top-3 by |val|: dims 5, 11, 7. Sorted: 5, 7, 11.
	dims := []uint32{2, 5, 7, 11, 14}
	vals := []float32{0.10, -0.90, 0.30, 0.80, -0.05}
	in := mkConcise(dims, vals)

	out, truncated := TruncateConciseTopN(in, 3, nil)
	if !truncated {
		t.Fatalf("expected truncation")
	}
	csv := ConciseSparseVector(out)
	if csv.NNZ() != 3 {
		t.Fatalf("expected NNZ=3, got %d", csv.NNZ())
	}
	gotDims := csv.Indices()
	gotVals := csv.Values()

	wantDims := []uint32{5, 7, 11}
	wantVals := []float32{-0.90, 0.30, 0.80}
	for i := range wantDims {
		if gotDims[i] != wantDims[i] {
			t.Fatalf("dim[%d]: want %d got %d", i, wantDims[i], gotDims[i])
		}
		if gotVals[i] != wantVals[i] {
			t.Fatalf("val[%d]: want %f got %f", i, wantVals[i], gotVals[i])
		}
	}
}

func TestTruncateConciseTopN_ReusesProvidedBuffer(t *testing.T) {
	in := mkConcise([]uint32{1, 2, 3, 4}, []float32{0.4, 0.1, 0.3, 0.2})
	buf := make([]float32, 1+2*2) // exact required capacity

	out, truncated := TruncateConciseTopN(in, 2, buf)
	if !truncated {
		t.Fatalf("expected truncation")
	}
	if &out[0] != &buf[0] {
		t.Fatalf("expected buf to be reused (no realloc)")
	}
	csv := ConciseSparseVector(out)
	if csv.NNZ() != 2 {
		t.Fatalf("expected NNZ=2, got %d", csv.NNZ())
	}
	gotDims := csv.Indices()
	if gotDims[0] != 1 || gotDims[1] != 3 {
		t.Fatalf("expected top-2 to be dims {1,3} got %v", gotDims)
	}
}

func TestTruncateConciseTopN_GrowsBufferIfTooSmall(t *testing.T) {
	in := mkConcise([]uint32{1, 2, 3, 4, 5}, []float32{0.1, 0.5, 0.2, 0.9, 0.3})
	buf := make([]float32, 0) // empty, must grow

	out, truncated := TruncateConciseTopN(in, 3, buf)
	if !truncated {
		t.Fatalf("expected truncation")
	}
	if len(out) != 1+2*3 {
		t.Fatalf("expected out len=7, got %d", len(out))
	}
	csv := ConciseSparseVector(out)
	gotDims := csv.Indices()
	// Top-3 by |val|: dim 4 (0.9), dim 2 (0.5), dim 5 (0.3). Sorted: 2, 4, 5.
	wantDims := []uint32{2, 4, 5}
	for i := range wantDims {
		if gotDims[i] != wantDims[i] {
			t.Fatalf("dim[%d]: want %d got %d (all=%v)", i, wantDims[i], gotDims[i], gotDims)
		}
	}
}

func TestTruncateConciseTopN_EmptyInput(t *testing.T) {
	out, truncated := TruncateConciseTopN(nil, 5, nil)
	if truncated || out != nil {
		t.Fatalf("expected no-op on nil input")
	}
}

func TestPruneConciseByThreshold_NoOpWhenDisabled(t *testing.T) {
	in := mkConcise([]uint32{1, 5, 9}, []float32{0.01, 0.9, 0.02})
	out, pruned := PruneConciseByThreshold(in, 0, nil)
	if pruned {
		t.Fatalf("expected no prune when threshold=0")
	}
	if &out[0] != &in[0] {
		t.Fatalf("expected zero-copy return of input")
	}
}

func TestPruneConciseByThreshold_NoOpWhenAllAboveThreshold(t *testing.T) {
	in := mkConcise([]uint32{1, 5, 9}, []float32{0.1, 0.9, 0.4})
	out, pruned := PruneConciseByThreshold(in, 0.05, nil)
	if pruned {
		t.Fatalf("expected no prune when all values >= threshold")
	}
	if &out[0] != &in[0] {
		t.Fatalf("expected zero-copy return of input")
	}
}

func TestPruneConciseByThreshold_DropsBelowThresholdAndKeepsDimOrder(t *testing.T) {
	dims := []uint32{2, 5, 7, 11, 14}
	vals := []float32{0.01, -0.90, 0.03, 0.80, -0.04}
	in := mkConcise(dims, vals)

	out, pruned := PruneConciseByThreshold(in, 0.05, nil)
	if !pruned {
		t.Fatalf("expected prune")
	}
	csv := ConciseSparseVector(out)
	if csv.NNZ() != 2 {
		t.Fatalf("expected NNZ=2, got %d", csv.NNZ())
	}
	gotDims := csv.Indices()
	gotVals := csv.Values()

	wantDims := []uint32{5, 11}
	wantVals := []float32{-0.90, 0.80}
	for i := range wantDims {
		if gotDims[i] != wantDims[i] {
			t.Fatalf("dim[%d]: want %d got %d", i, wantDims[i], gotDims[i])
		}
		if gotVals[i] != wantVals[i] {
			t.Fatalf("val[%d]: want %f got %f", i, wantVals[i], gotVals[i])
		}
	}
}

func TestPruneConciseByThreshold_NoOpWhenAllWouldBePruned(t *testing.T) {
	in := mkConcise([]uint32{1, 2, 3}, []float32{0.001, 0.002, -0.003})
	out, pruned := PruneConciseByThreshold(in, 0.05, nil)
	if pruned {
		t.Fatalf("expected no-op when all values would be dropped")
	}
	if &out[0] != &in[0] {
		t.Fatalf("expected zero-copy return of input")
	}
}

func TestPruneConciseByThreshold_ReusesProvidedBuffer(t *testing.T) {
	in := mkConcise([]uint32{1, 2, 3, 4}, []float32{0.4, 0.001, 0.3, 0.002})
	buf := make([]float32, 1+2*2)

	out, pruned := PruneConciseByThreshold(in, 0.05, buf)
	if !pruned {
		t.Fatalf("expected prune")
	}
	if &out[0] != &buf[0] {
		t.Fatalf("expected buf to be reused (no realloc)")
	}
	gotDims := ConciseSparseVector(out).Indices()
	if len(gotDims) != 2 || gotDims[0] != 1 || gotDims[1] != 3 {
		t.Fatalf("expected dims {1,3} got %v", gotDims)
	}
}

func TestPruneConciseByThreshold_AbsoluteValueMatters(t *testing.T) {
	// Negative value with |v| >= threshold should be kept.
	in := mkConcise([]uint32{1, 2}, []float32{-0.10, 0.03})
	out, pruned := PruneConciseByThreshold(in, 0.05, nil)
	if !pruned {
		t.Fatalf("expected prune")
	}
	gotDims := ConciseSparseVector(out).Indices()
	gotVals := ConciseSparseVector(out).Values()
	if len(gotDims) != 1 || gotDims[0] != 1 || gotVals[0] != -0.10 {
		t.Fatalf("expected single entry (dim=1, val=-0.10), got dims=%v vals=%v", gotDims, gotVals)
	}
}

func TestPruneConciseByThreshold_EmptyInput(t *testing.T) {
	out, pruned := PruneConciseByThreshold(nil, 0.05, nil)
	if pruned || out != nil {
		t.Fatalf("expected no-op on nil input")
	}
}

func TestTruncateConciseTopN_DeterministicTieBreak(t *testing.T) {
	// Two pairs have identical |val|; stable sort keeps earlier original
	// positions, which after restore-by-pos yields the lower dims.
	in := mkConcise(
		[]uint32{10, 20, 30, 40},
		[]float32{0.5, 0.5, 0.5, 0.1},
	)
	out, truncated := TruncateConciseTopN(in, 2, nil)
	if !truncated {
		t.Fatalf("expected truncation")
	}
	gotDims := ConciseSparseVector(out).Indices()
	if gotDims[0] != 10 || gotDims[1] != 20 {
		t.Fatalf("expected stable tie-break to pick dims {10,20} got %v", gotDims)
	}
}
