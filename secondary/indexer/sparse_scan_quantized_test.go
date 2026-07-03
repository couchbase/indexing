//go:build !community
// +build !community

package indexer

// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

import (
	"math"
	"math/rand"
	"testing"

	"github.com/couchbase/bhive"
)

// randConciseSparse builds a concise sparse vector [N, dims..., vals...] with
// nnz sorted unique dims in [0, maxDim) and positive values in [0.1, 3.0),
// mimicking SPLADE term weights.
func randConciseSparse(rnd *rand.Rand, nnz, maxDim int) []float32 {
	dims := make(map[int]struct{}, nnz)
	for len(dims) < nnz {
		dims[rnd.Intn(maxDim)] = struct{}{}
	}
	sorted := make([]int, 0, nnz)
	for d := range dims {
		sorted = append(sorted, d)
	}
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && sorted[j] < sorted[j-1]; j-- {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
		}
	}
	out := make([]float32, 1+2*nnz)
	out[0] = float32(nnz)
	for i, d := range sorted {
		out[1+i] = float32(d)
		out[1+nnz+i] = 0.1 + 2.9*rnd.Float32()
	}
	return out
}

// floatSparseIP computes the exact float32 inner product of two concise
// sparse vectors by merge-walking the sorted dim lists.
func floatSparseIP(q, s []float32) float32 {
	nq, ns := int(q[0]), int(s[0])
	qd, qv := q[1:1+nq], q[1+nq:1+2*nq]
	sd, sv := s[1:1+ns], s[1+ns:1+2*ns]
	var ip float32
	i, j := 0, 0
	for i < nq && j < ns {
		switch {
		case qd[i] < sd[j]:
			i++
		case qd[i] > sd[j]:
			j++
		default:
			ip += qv[i] * sv[j]
			i++
			j++
		}
	}
	return ip
}

// TestSparseScanQuantizedKernelParity verifies the contract the quantized
// sparse scan path relies on: the bhive dot-product kernel over wires
// produced by QuantizeSparseVectorTo / QuantizeQueryVector approximates the
// float32 inner product, and yields IP == 0 exactly when the query and the
// document share no terms (the encoder clamps quantized weights to >= 1, so
// any real overlap must produce a strictly positive IP).
func TestSparseScanQuantizedKernelParity(t *testing.T) {
	rnd := rand.New(rand.NewSource(42))
	const nDocs = 257 // not a multiple of 8: exercises the padded final batch
	const maxDim = 30000

	query := randConciseSparse(rnd, 24, maxDim)
	qWire, err := bhiveQuantizeSparseQuery(query)
	if err != nil {
		t.Fatalf("quantize query: %v", err)
	}

	docs := make([][]float32, nDocs)
	wires := make([][]byte, nDocs)
	for i := range docs {
		doc := randConciseSparse(rnd, 5+rnd.Intn(46), maxDim)
		if i%3 == 0 {
			// Force term overlap with the query for a third of the docs so
			// both the match and no-match paths are exercised (random dims in
			// a 30k vocabulary rarely collide).
			doc[1+rnd.Intn(int(doc[0]))] = query[1+rnd.Intn(int(query[0]))]
			// Re-sort dims (values keep their positions; ordering between
			// dims and values is not required to correlate for this test).
			nnz := int(doc[0])
			d := doc[1 : 1+nnz]
			for x := 1; x < len(d); x++ {
				for y := x; y > 0 && d[y] < d[y-1]; y-- {
					d[y], d[y-1] = d[y-1], d[y]
				}
			}
			// Dedup any collision introduced by the overwrite: bump equal
			// neighbors apart (stays sorted, stays in range).
			for x := 1; x < len(d); x++ {
				if d[x] == d[x-1] {
					d[x]++
				}
			}
		}
		docs[i] = doc
		buf := make([]byte, bhive.QuantizedSparseSize(doc))
		if _, err := bhive.QuantizeSparseVectorTo(doc, buf); err != nil {
			t.Fatalf("quantize doc %d: %v", i, err)
		}
		wires[i] = buf
	}

	ips := make([]float32, nDocs)
	bhiveSparseDotBatchNQuantized(qWire, wires, ips)

	for i := range docs {
		ref := floatSparseIP(query, docs[i])
		got := ips[i]

		if ref == 0 {
			if got != 0 {
				t.Errorf("doc %d: no term overlap but kernel IP = %v", i, got)
			}
			continue
		}
		if got <= 0 {
			t.Errorf("doc %d: term overlap (float IP %v) but kernel IP = %v", i, ref, got)
			continue
		}
		// Scalar quantization error: each side rounds to 1/255 of its peak
		// weight (plus the >= 1 clamp on tiny weights), so allow a generous
		// relative + absolute tolerance.
		diff := math.Abs(float64(got - ref))
		if diff > 0.1*float64(ref)+0.05 {
			t.Errorf("doc %d: kernel IP %v deviates from float IP %v by %v", i, got, ref, diff)
		}
	}
}

// TestSparseScanQuantizedKernelEmptyInputs verifies the edge cases the scan
// path can feed the kernel: empty query wire and empty/zero-count document
// wires must produce IP 0 rather than errors or garbage.
func TestSparseScanQuantizedKernelEmptyInputs(t *testing.T) {
	doc := []float32{2, 3, 8, 1.5, 0.5}
	wire := make([]byte, bhive.QuantizedSparseSize(doc))
	if _, err := bhive.QuantizeSparseVectorTo(doc, wire); err != nil {
		t.Fatalf("quantize doc: %v", err)
	}

	// Empty (nil) query wire.
	out := []float32{-1}
	bhiveSparseDotBatchNQuantized(nil, [][]byte{wire}, out)
	if out[0] != 0 {
		t.Errorf("nil query: expected IP 0, got %v", out[0])
	}

	// Zero-count document wire.
	empty := []float32{0}
	emptyWire := make([]byte, bhive.QuantizedSparseSize(empty))
	if _, err := bhive.QuantizeSparseVectorTo(empty, emptyWire); err != nil {
		t.Fatalf("quantize empty doc: %v", err)
	}
	query := []float32{2, 3, 8, 1.0, 1.0}
	qWire, err := bhiveQuantizeSparseQuery(query)
	if err != nil {
		t.Fatalf("quantize query: %v", err)
	}
	out = []float32{-1, -1}
	bhiveSparseDotBatchNQuantized(qWire, [][]byte{emptyWire, nil}, out)
	if out[0] != 0 || out[1] != 0 {
		t.Errorf("empty/nil docs: expected IP 0s, got %v", out)
	}
}
