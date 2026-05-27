// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package vector

import (
	"math/rand"
	"testing"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	cbpkg "github.com/couchbase/indexing/secondary/vector/codebook"
)

type codebookSparseTestCase struct {
	name string

	dim  int
	size int

	nlist int

	num_vecs  int
	trainlist int
}

var codebookSparseTestCases = []codebookSparseTestCase{

	{"Sparse", 64, 128, 1000, 10000, 10000},
}

// Tests for CodebookSparse
func TestCodebookSparse(t *testing.T) {
	seed := time.Now().UnixNano()
	for _, tc := range codebookSparseTestCases {
		t.Run(tc.name, func(t *testing.T) {

			sparseCB, err := NewCodebookSparse(tc.dim, tc.nlist)
			if err != nil || sparseCB == nil {
				t.Errorf("Unable to create index. Err %v", err)
			}

			//generate random vectors
			vecs_s := genRandomSparseVecs(tc.size, tc.num_vecs, seed)

			//t.Logf("Sample sparse vector %v", vecs[0])
			//set verbose log level
			cb := sparseCB.(*codebookSparse)
			cb.index.SetVerbose(1)

			//convert to sparse JL format using SparseCodebook interface

			vecs_jl := make([][]float32, tc.num_vecs)
			for i := 0; i < tc.num_vecs; i++ {
				vecs_jl[i] = make([]float32, tc.dim)
				err = sparseCB.Concise2SparseJL(vecs_s[i], vecs_jl[i])
				if err != nil {
					t.Errorf("Error converting to sparse JL format %v", err)
				}
			}
			//t.Logf("Sample sparse JL vector %v", vecs_jl[0])

			//train the sparseCB using 10000 vecs
			tvecs_jl := convertTo1D(vecs_jl[:tc.trainlist])
			t0 := time.Now()
			err = sparseCB.Train(tvecs_jl)
			delta := time.Now().Sub(t0)
			t.Logf("Train timing %v vectors %v", tc.trainlist, delta)

			if err != nil || !sparseCB.IsTrained() {
				t.Errorf("Unable to train index. Err %v", err)
			}

			//sanity check quantizer
			quantizer, err := cb.index.Quantizer()
			if err != nil {
				t.Errorf("Unable to get index quantizer. Err %v", err)
			}

			if quantizer.Ntotal() != int64(tc.nlist) {
				t.Errorf("Unexpected number of quantizer items %v. Expected %v", quantizer.Ntotal(), tc.nlist)
			}

			//find the nearest centroid
			qvec_jl := convertTo1D(vecs_jl[:1])
			t0 = time.Now()
			label, err := sparseCB.FindNearestCentroids(qvec_jl, 100)
			delta = time.Now().Sub(t0)
			t.Logf("Assign results %v %v", label, err)
			t.Logf("Assign timing %v", delta)
			for _, l := range label {
				if l > int64(tc.nlist) || l == -1 {
					t.Errorf("Result label out of range. Total %v. Label %v", tc.nlist, l)
				}
			}

			//choose a query vector
			qvec_s := convertTo1D(vecs_s[:1])
			//t.Logf("Random query vector %v", qvec_s)

			//choose a sparse vector
			rand_vec_s := vecs_s[rand.Intn(tc.num_vecs)]
			//t.Logf("Random sparse vector %v", rand_vec_s)

			tvec := make([]float32, len(qvec_s))
			found := sparseCB.Transpose(qvec_s, rand_vec_s, tvec)

			t.Logf("Matching term found = %v, result = %v", found, tvec)

			//compute distance
			dist := make([]float32, len(qvec_s))

			//create a slice with only values for dist computation
			qvec_s_d := qvec_s[1+tc.size : 1+2*tc.size]

			err = sparseCB.ComputeDistance(qvec_s_d, tvec, dist)
			if err != nil {
				t.Errorf("Error computing distance %v", err)
			}

			t.Logf("Computed distance %v", dist)

			//check the size
			pSize := sparseCB.Size()
			if pSize == 0 {
				t.Errorf("Unexpected codebook memory size %v", pSize)
			}
			t.Logf("Codebook Memory Size %v", pSize)

			data, err := sparseCB.Marshal()
			if err != nil {
				t.Errorf("Error marshalling codebook %v", err)
			}

			err = sparseCB.Close()
			if err != nil {
				t.Errorf("Error closing codebook %v", err)
			}

			//close again
			err = sparseCB.Close()
			if err != cbpkg.ErrCodebookClosed {
				t.Errorf("Expected err while double closing codebook")
			}

			newcb, err := RecoverCodebook(data, "SPARSE")
			if err != nil {
				t.Errorf("Error unmarshalling codebook %v", err)
			}

			if !newcb.IsTrained() {
				t.Errorf("Found recovered codebook with IsTrained=false")
			}

			nSize := newcb.Size()
			if pSize != nSize {
				t.Errorf("Unexpected codebook memory size after recovery %v, expected %v", nSize, pSize)
			}

			//sanity check quantizer
			quantizer, err = newcb.(*codebookSparse).index.Quantizer()
			if err != nil {
				t.Errorf("Unable to get index quantizer. Err %v", err)
			}

			if quantizer.Ntotal() != int64(tc.nlist) {
				t.Errorf("Unexpected number of quantizer items %v. Expected %v", quantizer.Ntotal(), tc.nlist)
			}

			//find the nearest centroid
			label, err = newcb.FindNearestCentroids(qvec_jl, 3)
			t.Logf("Assign results %v %v", label, err)
			for _, l := range label {
				if l > int64(tc.nlist) || l == -1 {
					t.Errorf("Result label out of range. Total %v. Label %v", tc.nlist, l)
				}
			}

			err = newcb.Close()
			if err != nil {
				t.Errorf("Error closing codebook %v", err)
			}
		})
	}
}

// TestCodebookSparseHistogramRoundTrip exercises the histogram persistence
// added alongside derived τ pruning: train a tiny codebook, attach a
// histogram, marshal, recover, assert the histogram, derived τ, and retained
// L1 fraction survived intact and DerivedTau() / WeightHistogramSummary()
// report consistent values.
func TestCodebookSparseHistogramRoundTrip(t *testing.T) {
	const dim = 16
	const nlist = 2

	cb, err := NewCodebookSparse(dim, nlist)
	if err != nil {
		t.Fatalf("NewCodebookSparse: %v", err)
	}
	defer cb.Close()

	// Train with the smallest valid set: nlist vectors so faiss skips its
	// usual k-means and seeds centroids directly from input.
	tvecs := make([]float32, dim*nlist)
	for i := range tvecs {
		tvecs[i] = float32(i) * 0.01
	}
	if err := cb.Train(tvecs); err != nil {
		t.Fatalf("Train: %v", err)
	}

	hist := common.NewWeightHistogram()
	for i := 0; i < 1000; i++ {
		hist.Observe(0.01)
	}
	for i := 0; i < 100; i++ {
		hist.Observe(0.5)
	}
	wantTau := float32(0.05)
	wantRetained := 0.97
	if err := cb.SetWeightHistogram(hist, wantTau, wantRetained); err != nil {
		t.Fatalf("SetWeightHistogram: %v", err)
	}

	data, err := cb.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	rec, err := recoverCodebookSparse(data)
	if err != nil {
		t.Fatalf("recoverCodebookSparse: %v", err)
	}
	defer rec.Close()

	if got := rec.DerivedTau(); got != wantTau {
		t.Fatalf("DerivedTau after round-trip: want %v got %v", wantTau, got)
	}
	obs, retained, tau, ok := rec.WeightHistogramSummary()
	if !ok {
		t.Fatalf("WeightHistogramSummary: available=false after round-trip")
	}
	if obs != hist.TotalObs {
		t.Fatalf("TotalObs: want %d got %d", hist.TotalObs, obs)
	}
	if retained != wantRetained {
		t.Fatalf("retainedL1Frac: want %v got %v", wantRetained, retained)
	}
	if tau != wantTau {
		t.Fatalf("derivedTau: want %v got %v", wantTau, tau)
	}
}

// TestCodebookSparseHistogramAbsentForOlderCodebook simulates an older
// codebook payload (no histogram fields) by training+marshaling without
// SetWeightHistogram and confirms recover yields a codebook whose
// histogram methods report "absent".
func TestCodebookSparseHistogramAbsentForOlderCodebook(t *testing.T) {
	const dim = 16
	const nlist = 2

	cb, err := NewCodebookSparse(dim, nlist)
	if err != nil {
		t.Fatalf("NewCodebookSparse: %v", err)
	}
	defer cb.Close()

	tvecs := make([]float32, dim*nlist)
	for i := range tvecs {
		tvecs[i] = float32(i) * 0.01
	}
	if err := cb.Train(tvecs); err != nil {
		t.Fatalf("Train: %v", err)
	}

	data, err := cb.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	rec, err := recoverCodebookSparse(data)
	if err != nil {
		t.Fatalf("recoverCodebookSparse: %v", err)
	}
	defer rec.Close()

	if got := rec.DerivedTau(); got != 0 {
		t.Fatalf("DerivedTau: want 0 for no-histogram codebook, got %v", got)
	}
	_, _, _, ok := rec.WeightHistogramSummary()
	if ok {
		t.Fatalf("WeightHistogramSummary: want available=false, got true")
	}
}
