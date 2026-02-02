// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package vector

import (
	"testing"
	"time"

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

			codebook, err := NewCodebookSparse(tc.dim, tc.nlist)
			if err != nil || codebook == nil {
				t.Errorf("Unable to create index. Err %v", err)
			}

			//generate random vectors
			vecs := genRandomSparseVecs(tc.size, tc.num_vecs, seed)

			//t.Logf("Sample sparse vector %v", vecs[0])
			//set verbose log level
			cb := codebook.(*codebookSparse)
			cb.index.SetVerbose(1)

			//convert to sparse JL format
			sparseJLVecs := make([][]float32, tc.num_vecs)
			for i := 0; i < tc.num_vecs; i++ {
				sparseJLVecs[i] = make([]float32, tc.dim)
				err = cb.Concise2SparseJL(vecs[i], sparseJLVecs[i])
				if err != nil {
					t.Errorf("Error converting to sparse JL format %v", err)
				}
			}
			//t.Logf("Sample sparse JL vector %v", sparseJLVecs[0])

			//train the codebook using 10000 vecs
			train_vecs := convertTo1D(sparseJLVecs[:tc.trainlist])
			t0 := time.Now()
			err = codebook.Train(train_vecs)
			delta := time.Now().Sub(t0)
			t.Logf("Train timing %v vectors %v", tc.trainlist, delta)

			if err != nil || !codebook.IsTrained() {
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
			query_vec := convertTo1D(sparseJLVecs[:1])
			t0 = time.Now()
			label, err := codebook.FindNearestCentroids(query_vec, 100)
			delta = time.Now().Sub(t0)
			t.Logf("Assign results %v %v", label, err)
			t.Logf("Assign timing %v", delta)
			for _, l := range label {
				if l > int64(tc.nlist) || l == -1 {
					t.Errorf("Result label out of range. Total %v. Label %v", tc.nlist, l)
				}
			}

			//check the size
			pSize := codebook.Size()
			if pSize == 0 {
				t.Errorf("Unexpected codebook memory size %v", pSize)
			}
			t.Logf("Codebook Memory Size %v", pSize)

			data, err := codebook.Marshal()
			if err != nil {
				t.Errorf("Error marshalling codebook %v", err)
			}

			err = codebook.Close()
			if err != nil {
				t.Errorf("Error closing codebook %v", err)
			}

			//close again
			err = codebook.Close()
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
			label, err = newcb.FindNearestCentroids(query_vec, 3)
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
