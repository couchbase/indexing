package vector

import (
	"sync"
	"testing"
	"time"

	codebookpkg "github.com/couchbase/indexing/secondary/vector/codebook"
)

// Tests for CodebookIVFPQ
func TestCodebookIVFPQ(t *testing.T) {

	var err error

	dim := 128
	metric := METRIC_L2

	nlist := 1000
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
	t0 := time.Now()
	err = codebook.Train(train_vecs)
	delta := time.Now().Sub(t0)
	t.Logf("Train timing %v", delta)

	if err != nil || !codebook.IsTrained() {
		t.Errorf("Unable to train index. Err %v", err)
	}

	//sanity check quantizer
	cb := codebook.(*codebookIVFPQ)
	quantizer, err := cb.index.Quantizer()
	if err != nil {
		t.Errorf("Unable to get index quantizer. Err %v", err)
	}

	if quantizer.Ntotal() != int64(nlist) {
		t.Errorf("Unexpected number of quantizer items %v. Expected %v", quantizer.Ntotal(), nlist)
	}

	//find the nearest centroid
	query_vec := convertTo1D(vecs[:1])
	t0 = time.Now()
	label, err := codebook.FindNearestCentroids(query_vec, 3)
	delta = time.Now().Sub(t0)
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

	//encode a single vector
	code := make([]byte, codeSize)
	t0 = time.Now()
	err = codebook.EncodeVector(query_vec, code)
	delta = time.Now().Sub(t0)
	if err != nil {
		t.Errorf("Error encoding vector %v", err)
	}
	validate_code_size(t, code, codeSize, 1)
	t.Logf("Encode code %v", query_vec)
	t.Logf("Encode results %v", code)
	t.Logf("Encode timing %v", delta)

	dvec := make([]float32, dim)
	err = codebook.DecodeVector(code, dvec)
	t.Logf("Decode results %v", dvec)

	//encode multiple vectors
	n := 10
	query_vecs := convertTo1D(vecs[:n])
	codes := make([]byte, n*codeSize)
	t0 = time.Now()
	err = codebook.EncodeVectors(query_vecs, codes)
	delta = time.Now().Sub(t0)
	if err != nil {
		t.Errorf("Error encoding vector %v", err)
	}

	validate_code_size(t, codes, codeSize, n)
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
	validate_code_size(t, codes, codeSize, n)
	t.Logf("Num EncodeAndAssign %v", len(codes)/codeSize)
	t.Logf("EncodeAndAssign timing %v", delta)
	t.Logf("EncodeAndAssign code %v", codes[0])
	t.Logf("EncodeAndAssign label %v", labels)

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

	newcb, err := RecoverCodebook(data, "PQ")
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
	quantizer, err = newcb.(*codebookIVFPQ).index.Quantizer()
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
	validate_code_size(t, ncode, nCodeSize, 1)
	t.Logf("Encode code%v", query_vec)
	t.Logf("Encode results %v", code)

	dvec = make([]float32, dim)
	err = newcb.DecodeVector(ncode, dvec)
	t.Logf("Decode results %v", dvec)

	err = newcb.Close()
	if err != nil {
		t.Errorf("Error closing codebook %v", err)
	}
}

type pqEncodeTimingTestCase struct {
	name string

	dim    int
	metric MetricType

	nlist int
	nsub  int
	nbits int

	num_vecs  int
	trainlist int

	batchSize int
	concur    int
	iters     int
}

var pqEncodeTimingTestCases = []pqEncodeTimingTestCase{

	{"PQ8x8 Batch 1 Concur 1", 128, METRIC_L2, 1000, 8, 8, 10000, 10000, 1, 1, 10000},
	{"PQ8x8 Batch 1 Concur 10", 128, METRIC_L2, 1000, 8, 8, 10000, 10000, 1, 10, 10000},
	{"PQ8x8 Batch 10 Concur 1", 128, METRIC_L2, 1000, 8, 8, 10000, 10000, 10, 1, 10000},
	{"PQ8x8 Batch 10 Concur 10", 128, METRIC_L2, 1000, 8, 8, 10000, 10000, 10, 10, 10000},
	//	{"PQ32x8 Batch 10 Concur 20", 128, METRIC_L2, 1000, 32, 8, 10000, 10000, 10, 20, 10000},
}

func TestIVFPQEncodeTiming(t *testing.T) {

	for _, tc := range pqEncodeTimingTestCases {
		t.Run(tc.name, func(t *testing.T) {

			codebook, err := NewCodebookIVFPQ(tc.dim, tc.nsub, tc.nbits, tc.nlist, tc.metric)
			if err != nil || codebook == nil {
				t.Errorf("Unable to create index. Err %v", err)
			}

			//generate random vectors
			vecs := genRandomVecs(tc.dim, tc.num_vecs)

			//train the codebook
			train_vecs := convertTo1D(vecs[:tc.trainlist])
			err = codebook.Train(train_vecs)
			if err != nil || !codebook.IsTrained() {
				t.Errorf("Unable to train index. Err %v", err)
			}

			codeSize, err := codebook.CodeSize()
			if err != nil {
				t.Errorf("Error fetching code size %v", err)
			}

			encode_batch := convertTo1D(vecs[:tc.batchSize])

			pcodes := make([][]byte, tc.concur)
			var timings time.Duration

			for i := range pcodes {
				pcodes[i] = make([]byte, tc.batchSize*codeSize)
			}
			for j := 0; j < tc.iters; j++ {
				var wg sync.WaitGroup
				for i := 0; i < tc.concur; i++ {
					wg.Add(1)
					go func(wg *sync.WaitGroup, pos int) {
						defer wg.Done()
						t0 := time.Now()
						err = codebook.EncodeVectors(encode_batch, pcodes[pos])
						delta := time.Now().Sub(t0)
						timings += delta
						//		t.Logf("Encode %v parallel timing %v", concur, delta)
						if err != nil {
							t.Errorf("Error encoding vector %v", err)
						}
					}(&wg, i)
				}
				wg.Wait()
				var pcodes1D []byte
				for _, pcode := range pcodes {
					pcodes1D = append(pcodes1D, pcode...)
				}
				validate_code_size(t, pcodes1D, codeSize, tc.concur*tc.batchSize)
			}

			total_ops := time.Duration(tc.iters * tc.concur * tc.batchSize)
			t.Logf("Encode average for iter %v concurrency %v batch %v is %v", tc.iters, tc.concur, tc.batchSize, timings/total_ops)

		})
	}

}

func validate_code_size(t *testing.T, code []byte, codeSize int, n int) {
	if len(code) != n*codeSize {
		t.Errorf("Unexpected code size. Expected %v, Actual %v", n*codeSize, len(code))
	}
}
