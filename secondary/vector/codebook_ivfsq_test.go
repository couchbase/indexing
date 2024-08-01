package vector

import (
	"testing"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	codebookpkg "github.com/couchbase/indexing/secondary/vector/codebook"
)

// Tests for CodebookIVFSQ
func TestCodebookIVFSQ(t *testing.T) {

	dim := 128
	metric := METRIC_L2

	nlist := 1000
	sqRanges := []common.ScalarQuantizerRange{common.SQ_8BIT, common.SQ_4BIT, common.SQ_6BIT, common.SQ_FP16}

	//generate random vectors, train vectors and query vectors
	num_train_vecs := 10000
	vecs := genRandomVecs(dim, num_train_vecs)
	train_vecs := convertTo1D(vecs)
	query_vec := convertTo1D(vecs[:1])
	n := 10
	query_vecs := convertTo1D(vecs[:n])

	for _, sqr := range sqRanges {
		t.Logf("\n========== Testing %v ==========\n", sqr)
		codebook, err := NewCodebookIVFSQ(dim, nlist, sqr, metric)
		if err != nil || codebook == nil {
			t.Errorf("Unable to create index. Err %v", err)
		}

		//train the codebook using 10000 vecs
		err = codebook.Train(train_vecs)

		if err != nil || !codebook.IsTrained() {
			t.Errorf("Unable to train index. Err %v", err)
		}

		//sanity check quantizer
		cb := codebook.(*codebookIVFSQ)
		quantizer, err := cb.index.Quantizer()
		if err != nil {
			t.Errorf("Unable to get index quantizer. Err %v", err)
		}

		if quantizer.Ntotal() != int64(nlist) {
			t.Errorf("Unexpected number of quantizer items %v. Expected %v", quantizer.Ntotal(), nlist)
		}

		//find the nearest centroid
		t0 := time.Now()
		label, err := codebook.FindNearestCentroids(query_vec, 3)
		delta := time.Now().Sub(t0)
		t.Logf("Assign results %v %v", label, err)
		t.Logf("Assign timing %v", delta)
		for _, l := range label {
			if l > int64(nlist) {
				t.Errorf("Result label out of range. Total %v. Label %v", nlist, l)
			}
		}

		codeSize, err := codebook.CodeSize()
		if err != nil {
			t.Errorf("Error fetching code size %v", err)
		}
		t.Logf("CodeSize %v", codeSize)

		validate_code_size := func(code []byte, codeSize int, n int) {
			if len(code) != n*codeSize {
				t.Errorf("Unexpected code size. Expected %v, Actual %v", n*codeSize, len(code))
			}
		}

		//encode a single vector
		code := make([]byte, codeSize)
		t0 = time.Now()
		err = codebook.EncodeVector(query_vec, code)
		delta = time.Now().Sub(t0)
		if err != nil {
			t.Errorf("Error encoding vector %v", err)
		}
		validate_code_size(code, codeSize, 1)
		t.Logf("Encode code %v", query_vec)
		t.Logf("Encode results %v", code)
		t.Logf("Encode timing %v", delta)

		dvec := make([]float32, dim)
		err = codebook.DecodeVector(code, dvec)
		t.Logf("Decode results %v", dvec)

		//encode multiple vectors
		codes := make([]byte, n*codeSize)
		t0 = time.Now()
		err = codebook.EncodeVectors(query_vecs, codes)
		delta = time.Now().Sub(t0)
		if err != nil {
			t.Errorf("Error encoding vector %v", err)
		}

		validate_code_size(codes, codeSize, n)
		t.Logf("Num Encodes %v", len(codes)/codeSize)
		t.Logf("Encode timing %v", delta)

		dvecs := make([]float32, n*dim)
		t0 = time.Now()
		err = codebook.DecodeVectors(n, codes, dvecs)
		if err != nil {
			t.Errorf("Error encoding vector %v", err)
		}
		delta = time.Now().Sub(t0)
		t.Logf("Num Decodes %v", len(dvecs)/dim)
		t.Logf("Decode timing %v", delta)

		//find the distance between qvec and multiple decoded vecs
		qvec := vecs[n-1]
		dist := make([]float32, n)
		t0 = time.Now()
		err = codebook.ComputeDistance(qvec, dvecs, dist)
		if err != nil {
			t.Errorf("Error computing distance %v", err)
		}
		delta = time.Now().Sub(t0)
		t.Logf("Computed distance %v", dist)
		t.Logf("Computed distance timing %v", delta)

		//encode and assign vector
		codes = make([]byte, n*codeSize)
		labels := make([]int64, n)
		t0 = time.Now()
		err = codebook.EncodeAndAssignVectors(query_vecs, codes, labels)
		delta = time.Now().Sub(t0)
		if err != nil {
			t.Errorf("Error encoding vector %v", err)
		}
		validate_code_size(codes, codeSize, n)
		t.Logf("Num EncodeAndAssign %v", len(codes)/codeSize)
		t.Logf("EncodeAndAssign timing %v", delta)
		t.Logf("EncodeAndAssign code %v", codes[0])
		t.Logf("EncodeAndAssign label %v", labels[0])

		for _, l := range labels {
			if l > int64(nlist) || l == 0 {
				t.Errorf("Result label out of range. Total %v. Label %v", nlist, l)
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
		if err != codebookpkg.ErrCodebookClosed {
			t.Errorf("Expected err while double closing codebook")
		}

		newcb, err := RecoverCodebook(data, "SQ")
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
		quantizer, err = newcb.(*codebookIVFSQ).index.Quantizer()
		if err != nil {
			t.Errorf("Unable to get index quantizer. Err %v", err)
		}

		if quantizer.Ntotal() != int64(nlist) {
			t.Errorf("Unexpected number of quantizer items %v. Expected %v", quantizer.Ntotal(), nlist)
		}

		nCodeSize, err := newcb.CodeSize()
		if err != nil {
			t.Errorf("Error fetching code size %v", err)
		}
		if codeSize != nCodeSize {
			t.Errorf("Mismatch in expected %v vs actual %v code size", codeSize, nCodeSize)
		}

		//find the nearest centroid
		label, err = newcb.FindNearestCentroids(query_vec, 3)
		t.Logf("Assign results %v %v", label, err)
		for _, l := range label {
			if l > int64(nlist) {
				t.Errorf("Result label out of range. Total %v. Label %v", nlist, l)
			}
		}

		//encode a single vector
		ncode := make([]byte, nCodeSize)
		err = newcb.EncodeVector(query_vec, ncode)
		if err != nil {
			t.Errorf("Error encoding vector %v", err)
		}
		validate_code_size(ncode, nCodeSize, 1)
		t.Logf("Encode code%v", query_vec)
		t.Logf("Encode results %v", code)

		dvec = make([]float32, dim)
		err = newcb.DecodeVector(ncode, dvec)
		t.Logf("Decode results %v", dvec)

		err = newcb.Close()
		if err != nil {
			t.Errorf("Error closing codebook %v", err)
		}
		t.Log("\n========================================\n")
	}
}
