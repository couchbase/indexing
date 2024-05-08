package vector

import (
	"testing"
)

//Tests for CodebookIVFPQ
func TestCodebookIVFPQ(t *testing.T) {

	var err error

	dim := 128
	metric := METRIC_L2

	nlist := 128
	nsub := 8
	nbits := 8

	codebook, err := NewCodebookIVFPQ(dim, nsub, nbits, nlist, metric)
	if err != nil || codebook == nil {
		t.Errorf("Unable to create index. Err %v", err)
	}

	num_train_vecs := 10000

	//generate random vectors
	vecs := genRandomVecs(dim, num_train_vecs)

	//train the codebook using 10000 vecs
	train_vecs := convertTo1D(vecs)
	err = codebook.Train(train_vecs)

	if err != nil || !codebook.IsTrained() {
		t.Errorf("Unable to train index. Err %v", err)
	}

	//sanity check quantizer
	quantizer, err := codebook.index.Quantizer()
	if err != nil {
		t.Errorf("Unable to get index quantizer. Err %v", err)
	}

	if quantizer.Ntotal() != int64(nlist) {
		t.Errorf("Unexpected number of quantizer items %v. Expected %v", quantizer.Ntotal(), nlist)
	}

	//find the nearest centroid
	query_vec := convertTo1D(vecs[:1])
	label, err := codebook.FindNearestCentroids(query_vec, 3)
	t.Logf("Assign results %v %v", label, err)
	for _, l := range label {
		if l > int64(nlist) {
			t.Errorf("Result label out of range. Total %v. Label %v", nlist, l)
		}
	}

	codeSize, err := codebook.CodeSize()
	if err != nil {
		t.Errorf("Error fetching code size %v", err)
	}

	validate_code_size := func(code []byte, codeSize int, n int) {
		if len(code) != n*codeSize {
			t.Errorf("Unexpected code size. Expected %v, Actual %v", n*codeSize, len(code))
		}
	}

	//encode a single vector
	code := make([]byte, codeSize)
	err = codebook.EncodeVector(query_vec, code)
	if err != nil {
		t.Errorf("Error encoding vector %v", err)
	}
	validate_code_size(code, codeSize, 1)
	t.Logf("Encode code%v", query_vec)
	t.Logf("Encode results %v", code)

	dvec := make([]float32, dim)
	err = codebook.DecodeVector(code, dvec)
	t.Logf("Decode results %v", dvec)

	//encode multiple vectors
	n := 3
	query_vecs := convertTo1D(vecs[:n])
	codes := make([]byte, n*codeSize)
	err = codebook.EncodeVectors(query_vecs, codes)
	if err != nil {
		t.Errorf("Error encoding vector %v", err)
	}

	validate_code_size(codes, codeSize, n)
	t.Logf("Encode results %v", codes)

	dvecs := make([]float32, n*dim)
	err = codebook.DecodeVectors(n, codes, dvecs)
	if err != nil {
		t.Errorf("Error encoding vector %v", err)
	}
	t.Logf("Decode results %v", dvecs)

	//find the distance between qvec and multiple decoded vecs
	qvec := vecs[n-1]
	dist := make([]float32, n)
	err = codebook.ComputeDistance(qvec, dvecs, dist)
	if err != nil {
		t.Errorf("Error computing distance %v", err)
	}
	t.Logf("Computed distance %v", dist)

}
