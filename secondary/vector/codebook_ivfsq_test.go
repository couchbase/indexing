package vector

import (
	"testing"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	codebookpkg "github.com/couchbase/indexing/secondary/vector/codebook"
)

type codebookIVFSQTestCase struct {
	name string

	dim       int
	metric    MetricType
	useCosine bool

	nlist    int
	sqRanges common.ScalarQuantizerRange

	num_vecs  int
	trainlist int
}

var codebookIVFSQTestCases = []codebookIVFSQTestCase{

	{"SQ4_L2", 128, METRIC_L2, false, 1000, common.SQ_4BIT, 10000, 10000},
	{"SQ6_L2", 128, METRIC_L2, false, 1000, common.SQ_6BIT, 10000, 10000},
	{"SQ8_L2", 128, METRIC_L2, false, 1000, common.SQ_8BIT, 10000, 10000},
	{"SQFP16_L2", 128, METRIC_L2, false, 1000, common.SQ_FP16, 10000, 10000},
	{"SQ8_DOT", 128, METRIC_INNER_PRODUCT, false, 1000, common.SQ_8BIT, 10000, 10000},
	{"SQ8_COSINE", 128, METRIC_INNER_PRODUCT, true, 1000, common.SQ_8BIT, 10000, 10000},
}

// Tests for CodebookIVFSQ
func TestCodebookIVFSQ(t *testing.T) {
	seed := time.Now().UnixNano()
	for _, tc := range codebookIVFSQTestCases {
		t.Run(tc.name, func(t *testing.T) {

			codebook, err := NewCodebookIVFSQ(tc.dim, tc.nlist, tc.sqRanges, tc.metric, tc.useCosine)
			if err != nil || codebook == nil {
				t.Errorf("Unable to create index. Err %v", err)
			}

			//generate random vectors, train vectors and query vectors
			vecs := genRandomVecs(tc.dim, tc.num_vecs, seed)

			//train the codebook using 10000 vecs
			train_vecs := convertTo1D(vecs[:tc.trainlist])
			t0 := time.Now()
			err = codebook.Train(train_vecs)
			delta := time.Now().Sub(t0)
			t.Logf("Train timing %v vectors %v", tc.trainlist, delta)

			if err != nil || !codebook.IsTrained() {
				t.Errorf("Unable to train index. Err %v", err)
			}

			//sanity check quantizer
			cb := codebook.(*codebookIVFSQ)
			quantizer, err := cb.index.Quantizer()
			if err != nil {
				t.Errorf("Unable to get index quantizer. Err %v", err)
			}

			if quantizer.Ntotal() != int64(tc.nlist) {
				t.Errorf("Unexpected number of quantizer items %v. Expected %v", quantizer.Ntotal(), tc.nlist)
			}

			//find the nearest centroid
			query_vec := convertTo1D(vecs[:1])
			t0 = time.Now()
			label, err := codebook.FindNearestCentroids(query_vec, 3)
			delta = time.Now().Sub(t0)
			t.Logf("Assign results %v %v", label, err)
			t.Logf("Assign timing %v", delta)
			for _, l := range label {
				if l > int64(tc.nlist) {
					t.Errorf("Result label out of range. Total %v. Label %v", tc.nlist, l)
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

			dvec := make([]float32, tc.dim)
			err = codebook.DecodeVector(code, dvec)
			t.Logf("Decode results %v", dvec)

			validate_decoded(t, dvec)

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

			dvecs := make([]float32, n*tc.dim)
			t0 = time.Now()
			err = codebook.DecodeVectors(n, codes, dvecs)
			if err != nil {
				t.Errorf("Error encoding vector %v", err)
			}
			delta = time.Now().Sub(t0)
			t.Logf("Num Decodes %v", len(dvecs)/tc.dim)
			t.Logf("Decode timing %v", delta)

			validate_decoded(t, dvecs)

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
			t.Logf("EncodeAndAssign label %v", labels[0])

			for _, l := range labels {
				if l > int64(tc.nlist) || l == 0 {
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

			if quantizer.Ntotal() != int64(tc.nlist) {
				t.Errorf("Unexpected number of quantizer items %v. Expected %v", quantizer.Ntotal(), tc.nlist)
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
				if l > int64(tc.nlist) {
					t.Errorf("Result label out of range. Total %v. Label %v", tc.nlist, l)
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

			dvec = make([]float32, tc.dim)
			err = newcb.DecodeVector(ncode, dvec)
			t.Logf("Decode results %v", dvec)

			validate_decoded(t, dvecs)

			err = newcb.Close()
			if err != nil {
				t.Errorf("Error closing codebook %v", err)
			}

			//compute distance
		})
	}
}