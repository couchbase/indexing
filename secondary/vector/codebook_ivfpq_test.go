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
	err = codebook.Train(vecs)

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

	//encode a single vector
	code, err := codebook.EncodeVector(query_vec)
	if err != nil {
		t.Errorf("Error encoding vector %v", err)
	}

	validate_code := func(code []uint8, nvecs int, nsub int, nbits int) {

		code_size := (nbits*nsub + 7) / 8
		if len(code) != code_size*nvecs {
			t.Errorf("Unexpected encoded vector size %v. Expected %v", len(code), code_size*nvecs)
		}

	}

	validate_code(code, 1, nsub, nbits)
	t.Logf("Encode results %v", code)

	//encode multiple vectors
	n := 3
	query_vecs := convertTo1D(vecs[:n])
	codes, err := codebook.EncodeVectors(query_vecs)
	if err != nil {
		t.Errorf("Error encoding vector %v", err)
	}

	validate_code(codes, n, nsub, nbits)
	t.Logf("Encode results %v", codes)

}
