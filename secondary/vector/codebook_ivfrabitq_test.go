package vector

import (
	"encoding/json"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	cbpkg "github.com/couchbase/indexing/secondary/vector/codebook"
)

type codebookIVFRaBitQTestCase struct {
	name string

	dim       int
	metric    cbpkg.MetricType
	useCosine bool

	nlist int
	nbits int

	numVecs  int
	trainVec int
}

var codebookIVFRaBitQTestCases = []codebookIVFRaBitQTestCase{
	{"RaBitQ1_L2_Dim1030", 1030, cbpkg.METRIC_L2, false, 64, 1, 1024, 1024},
	{"RaBitQ2_L2_Dim1024", 1024, cbpkg.METRIC_L2, false, 64, 2, 1024, 1024},
	{"RaBitQ8_DOT_Dim1024", 1024, cbpkg.METRIC_INNER_PRODUCT, false, 64, 8, 1024, 1024},
	{"RaBitQ4_COSINE_Dim1024", 1024, cbpkg.METRIC_INNER_PRODUCT, true, 64, 4, 1024, 1024},
	{"RaBitQ1_L2_NoClustering_Dim1024", 1024, cbpkg.METRIC_L2, false, 1024, 1, 1024, 1024},
}

func expectedRaBitQPayloadSize(dim, nbits int) int {
	exBits := nbits - 1
	factorSize := 8
	if exBits > 0 {
		factorSize = 12
	}

	size := (dim+7)/8 + factorSize
	if exBits > 0 {
		size += (dim*exBits+7)/8 + 8
	}

	return size
}

func TestCodebookIVFRaBitQ(t *testing.T) {
	seed := time.Now().UnixNano()

	for _, tc := range codebookIVFRaBitQTestCases {
		t.Run(tc.name, func(t *testing.T) {
			codebook, err := NewCodebookIVFRaBitQ(tc.dim, tc.nlist, tc.nbits, tc.metric, tc.useCosine)
			if err != nil || codebook == nil {
				t.Fatalf("Unable to create index. Err %v", err)
			}

			vecs := genRandomVecs(tc.dim, tc.numVecs, seed)

			cb := codebook.(*codebookIVFRaBitQ)
			cb.index.SetVerbose(1)
			if cb.qb != defaultRaBitQQB {
				t.Fatalf("Unexpected qb %v. Expected %v", cb.qb, defaultRaBitQQB)
			}

			trainVecs := convertTo1D(vecs[:tc.trainVec])
			err = codebook.Train(trainVecs)
			if err != nil || !codebook.IsTrained() {
				t.Fatalf("Unable to train index. Err %v", err)
			}

			quantizer, err := cb.index.Quantizer()
			if err != nil {
				t.Fatalf("Unable to get index quantizer. Err %v", err)
			}
			if quantizer.Ntotal() != int64(tc.nlist) {
				t.Fatalf("Unexpected number of quantizer items %v. Expected %v", quantizer.Ntotal(), tc.nlist)
			}

			codeSize, err := codebook.CodeSize()
			if err != nil {
				t.Fatalf("Error fetching code size %v", err)
			}

			coarseSize, err := codebook.CoarseSize()
			if err != nil {
				t.Fatalf("Error fetching coarse code size %v", err)
			}

			expectedCodeSize := expectedRaBitQPayloadSize(tc.dim, tc.nbits) + coarseSize
			if codeSize != expectedCodeSize {
				t.Fatalf("Unexpected code size %v. Expected %v", codeSize, expectedCodeSize)
			}

			queryVec := convertTo1D(vecs[:1])
			labels, err := codebook.FindNearestCentroids(queryVec, 8)
			if err != nil {
				t.Fatalf("Error finding nearest centroids %v", err)
			}
			for _, label := range labels {
				if label < 0 || label >= int64(tc.nlist) {
					t.Fatalf("Result label out of range. Total %v. Label %v", tc.nlist, label)
				}
			}

			code := make([]byte, codeSize)
			err = codebook.EncodeVector(queryVec, code)
			if err != nil {
				t.Fatalf("Error encoding vector %v", err)
			}
			validate_code_size(t, code, codeSize, 1)

			dvec := make([]float32, tc.dim)
			err = codebook.DecodeVector(code, dvec)
			if err != nil {
				t.Fatalf("Error decoding vector %v", err)
			}
			validate_decoded(t, dvec)

			n := 8
			queryVecs := convertTo1D(vecs[:n])
			codes := make([]byte, n*codeSize)
			assignLabels := make([]int64, n)
			err = codebook.EncodeAndAssignVectors(queryVecs, codes, assignLabels)
			if err != nil {
				t.Fatalf("Error encoding and assigning vectors %v", err)
			}
			validate_code_size(t, codes, codeSize, n)

			for _, label := range assignLabels {
				if label < 0 || label >= int64(tc.nlist) {
					t.Fatalf("Result label out of range. Total %v. Label %v", tc.nlist, label)
				}
			}

			pSize := codebook.Size()
			if pSize == 0 {
				t.Fatalf("Unexpected codebook memory size %v", pSize)
			}

			dvecs := make([]float32, n*tc.dim)
			err = codebook.DecodeVectors(n, codes, dvecs)
			if err != nil {
				t.Fatalf("Error decoding vectors %v", err)
			}
			validate_decoded(t, dvecs)

			qvec := vecs[n-1]
			dists := make([]float32, n)
			err = codebook.ComputeDistance(qvec, dvecs, dists)
			if err != nil {
				t.Fatalf("Error computing distance %v", err)
			}

			for i := 0; i < n; i++ {
				codeStart := i*codeSize + coarseSize
				listNo := decodeListNo(codes[i*codeSize : codeStart])
				tdist := make([]float32, 1)
				err = codebook.ComputeDistanceEncoded(qvec, 1, codes[codeStart:(i+1)*codeSize], tdist, nil, listNo)
				if err != nil {
					t.Fatalf("Error computing encoded distance %v", err)
				}
			}

			data, err := codebook.Marshal()
			if err != nil {
				t.Fatalf("Error marshalling codebook %v", err)
			}

			err = codebook.Close()
			if err != nil {
				t.Fatalf("Error closing codebook %v", err)
			}

			err = codebook.Close()
			if err != cbpkg.ErrCodebookClosed {
				t.Fatalf("Expected err while double closing codebook")
			}

			recovered, err := RecoverCodebook(data, string(common.RaBitQ))
			if err != nil {
				t.Fatalf("Error recovering codebook %v", err)
			}
			if !recovered.IsTrained() {
				t.Fatalf("Found recovered codebook with IsTrained=false")
			}

			nSize := recovered.Size()
			if pSize != nSize {
				t.Fatalf("Unexpected codebook memory size after recovery %v, expected %v", nSize, pSize)
			}

			recoveredCB, ok := recovered.(*codebookIVFRaBitQ)
			if !ok {
				t.Fatalf("Expected *codebookIVFRaBitQ type, got %T", recovered)
			}
			if recoveredCB.qb != defaultRaBitQQB {
				t.Fatalf("Unexpected recovered qb %v. Expected %v", recoveredCB.qb, defaultRaBitQQB)
			}

			recoveredCodeSize, err := recovered.CodeSize()
			if err != nil {
				t.Fatalf("Error fetching recovered code size %v", err)
			}
			if recoveredCodeSize != codeSize {
				t.Fatalf("Mismatch in expected %v vs actual %v code size", codeSize, recoveredCodeSize)
			}

			recoveredQuantizer, err := recoveredCB.index.Quantizer()
			if err != nil {
				t.Fatalf("Unable to get recovered index quantizer. Err %v", err)
			}
			if recoveredQuantizer.Ntotal() != int64(tc.nlist) {
				t.Fatalf("Unexpected recovered quantizer size %v. Expected %v", recoveredQuantizer.Ntotal(), tc.nlist)
			}

			recoveredLabels, err := recovered.FindNearestCentroids(queryVec, 8)
			if err != nil {
				t.Fatalf("Error finding nearest centroids on recovered codebook %v", err)
			}
			for _, label := range recoveredLabels {
				if label < 0 || label >= int64(tc.nlist) {
					t.Fatalf("Recovered result label out of range. Total %v. Label %v", tc.nlist, label)
				}
			}

			recoveredCode := make([]byte, recoveredCodeSize)
			err = recovered.EncodeVector(queryVec, recoveredCode)
			if err != nil {
				t.Fatalf("Error encoding vector with recovered codebook %v", err)
			}
			validate_code_size(t, recoveredCode, recoveredCodeSize, 1)

			recoveredVec := make([]float32, tc.dim)
			err = recovered.DecodeVector(recoveredCode, recoveredVec)
			if err != nil {
				t.Fatalf("Error decoding vector with recovered codebook %v", err)
			}
			validate_decoded(t, recoveredVec)

			err = recovered.Close()
			if err != nil {
				t.Fatalf("Error closing recovered codebook %v", err)
			}

			err = recovered.Close()
			if err != cbpkg.ErrCodebookClosed {
				t.Fatalf("Expected err while double closing recovered codebook")
			}
		})
	}
}

var computeDistanceEncodedRaBitQTests = []codebookIVFRaBitQTestCase{
	{"RaBitQ1_L2_Dim1024", 1024, cbpkg.METRIC_L2, false, 1, 1, 1024, 1024},
	{"RaBitQ4_DOT_Dim1024", 1024, cbpkg.METRIC_INNER_PRODUCT, false, 1, 4, 1024, 1024},
	{"RaBitQ4_COSINE_Dim1024", 1024, cbpkg.METRIC_INNER_PRODUCT, true, 1, 4, 1024, 1024},
}

type raBitQTimingTestCase struct {
	name string

	dim       int
	metric    cbpkg.MetricType
	useCosine bool

	nlist int
	nbits int

	numVecs  int
	trainVec int

	batchSize int
	concur    int
	iters     int
}

var raBitQTimingTestCases = []raBitQTimingTestCase{
	{"RaBitQ1_Batch1_Concur1", 1024, cbpkg.METRIC_L2, false, 64, 1, 2048, 1024, 1, 1, 3000},
	{"RaBitQ1_Batch16_Concur1", 1024, cbpkg.METRIC_L2, false, 64, 1, 2048, 1024, 16, 1, 1200},
	{"RaBitQ4_Batch1_Concur1", 1024, cbpkg.METRIC_L2, false, 64, 4, 2048, 1024, 1, 1, 3000},
	{"RaBitQ4_Batch16_Concur1", 1024, cbpkg.METRIC_L2, false, 64, 4, 2048, 1024, 16, 1, 1200},
	{"RaBitQ1_Batch1_Concur8", 1024, cbpkg.METRIC_L2, false, 64, 1, 2048, 1024, 1, 8, 800},
	{"RaBitQ4_Batch1_Concur8", 1024, cbpkg.METRIC_L2, false, 64, 4, 2048, 1024, 1, 8, 600},
}

func TestComputeDistanceEncodedRaBitQ(t *testing.T) {
	seed := time.Now().UnixNano()

	for _, tc := range computeDistanceEncodedRaBitQTests {
		t.Run(tc.name, func(t *testing.T) {
			codebook, err := NewCodebookIVFRaBitQ(tc.dim, tc.nlist, tc.nbits, tc.metric, tc.useCosine)
			if err != nil || codebook == nil {
				t.Fatalf("Unable to create index. Err %v", err)
			}

			vecs := genRandomVecs(tc.dim, tc.numVecs, seed)
			trainVecs := convertTo1D(vecs[:tc.trainVec])
			err = codebook.Train(trainVecs)
			if err != nil || !codebook.IsTrained() {
				t.Fatalf("Unable to train index. Err %v", err)
			}

			codeSize, err := codebook.CodeSize()
			if err != nil {
				t.Fatalf("Error fetching code size %v", err)
			}

			coarseSize, err := codebook.CoarseSize()
			if err != nil {
				t.Fatalf("Error fetching coarse code size %v", err)
			}

			n := 10
			queryVecs := convertTo1D(vecs[:n])
			codes := make([]byte, n*codeSize)
			err = codebook.EncodeVectors(queryVecs, codes)
			if err != nil {
				t.Fatalf("Error encoding vectors %v", err)
			}
			validate_code_size(t, codes, codeSize, n)

			dvecs := make([]float32, n*tc.dim)
			err = codebook.DecodeVectors(n, codes, dvecs)
			if err != nil {
				t.Fatalf("Error decoding vectors %v", err)
			}

			qvec := vecs[n-1]
			dist := make([]float32, n)
			err = codebook.ComputeDistance(qvec, dvecs, dist)
			if err != nil {
				t.Fatalf("Error computing distance %v", err)
			}

			dist2 := make([]float32, n)
			encodedOnly := make([]byte, 0, n*(codeSize-coarseSize))

			var listNo int64
			for i := 0; i < n; i++ {
				codeStart := i*codeSize + coarseSize
				listNo = decodeListNo(codes[i*codeSize : codeStart])
				encodedOnly = append(encodedOnly, codes[codeStart:(i+1)*codeSize]...)
			}

			err = codebook.ComputeDistanceEncoded(qvec, n, encodedOnly, dist2, nil, listNo)
			if err != nil {
				t.Fatalf("Error computing encoded distance %v", err)
			}

			for i := 0; i < n; i++ {
				if math.IsNaN(float64(dist2[i])) || math.IsInf(float64(dist2[i]), 0) {
					t.Fatalf("Found invalid encoded distance at %v: %v", i, dist2[i])
				}
			}

			err = codebook.Close()
			if err != nil {
				t.Fatalf("Error closing codebook %v", err)
			}
		})
	}
}

func TestIVFRaBitQTiming(t *testing.T) {
	seed := time.Now().UnixNano()

	for _, tc := range raBitQTimingTestCases {
		t.Run(tc.name, func(t *testing.T) {
			codebook, err := NewCodebookIVFRaBitQ(tc.dim, tc.nlist, tc.nbits, tc.metric, tc.useCosine)
			if err != nil || codebook == nil {
				t.Fatalf("Unable to create index. Err %v", err)
			}
			defer func() {
				err := codebook.Close()
				if err != nil && err != cbpkg.ErrCodebookClosed {
					t.Fatalf("Error closing codebook %v", err)
				}
			}()

			vecs := genRandomVecs(tc.dim, tc.numVecs, seed)

			trainVecs := convertTo1D(vecs[:tc.trainVec])
			err = codebook.Train(trainVecs)
			if err != nil || !codebook.IsTrained() {
				t.Fatalf("Unable to train index. Err %v", err)
			}

			codeSize, err := codebook.CodeSize()
			if err != nil {
				t.Fatalf("Error fetching code size %v", err)
			}

			encodeBatch := convertTo1D(vecs[:tc.batchSize])
			codes := make([][]byte, tc.concur)
			decoded := make([][]float32, tc.concur)
			for i := range codes {
				codes[i] = make([]byte, tc.batchSize*codeSize)
				decoded[i] = make([]float32, tc.batchSize*tc.dim)
			}

			var encodeTiming time.Duration
			var decodeTiming time.Duration

			for i := 0; i < tc.iters; i++ {
				var wg sync.WaitGroup
				encodeDeltas := make([]time.Duration, tc.concur)
				encodeErrs := make(chan error, tc.concur)

				for w := 0; w < tc.concur; w++ {
					wg.Add(1)
					go func(pos int) {
						defer wg.Done()
						t0 := time.Now()
						err := codebook.EncodeVectors(encodeBatch, codes[pos])
						encodeDeltas[pos] = time.Since(t0)
						if err != nil {
							encodeErrs <- err
						}
					}(w)
				}
				wg.Wait()
				close(encodeErrs)
				for err := range encodeErrs {
					t.Fatalf("Error encoding vectors %v", err)
				}
				for _, delta := range encodeDeltas {
					encodeTiming += delta
				}

				decodeDeltas := make([]time.Duration, tc.concur)
				decodeErrs := make(chan error, tc.concur)
				for w := 0; w < tc.concur; w++ {
					wg.Add(1)
					go func(pos int) {
						defer wg.Done()
						t0 := time.Now()
						err := codebook.DecodeVectors(tc.batchSize, codes[pos], decoded[pos])
						decodeDeltas[pos] = time.Since(t0)
						if err != nil {
							decodeErrs <- err
						}
					}(w)
				}
				wg.Wait()
				close(decodeErrs)
				for err := range decodeErrs {
					t.Fatalf("Error decoding vectors %v", err)
				}
				for _, delta := range decodeDeltas {
					decodeTiming += delta
				}
			}

			for _, code := range codes {
				validate_code_size(t, code, codeSize, tc.batchSize)
			}

			totalOps := time.Duration(tc.iters * tc.concur * tc.batchSize)
			t.Logf("Encode average for iter %v concurrency %v batch %v is %v per op", tc.iters, tc.concur, tc.batchSize, encodeTiming/totalOps)
			t.Logf("Decode average for iter %v concurrency %v batch %v is %v per op", tc.iters, tc.concur, tc.batchSize, decodeTiming/totalOps)
		})
	}
}

func TestRecoverCodebookRaBitQChecksumMismatch(t *testing.T) {
	seed := time.Now().UnixNano()

	codebook, err := NewCodebookIVFRaBitQ(64, 16, 1, cbpkg.METRIC_L2, false)
	if err != nil {
		t.Fatalf("Unable to create index. Err %v", err)
	}

	vecs := genRandomVecs(64, 512, seed)
	err = codebook.Train(convertTo1D(vecs))
	if err != nil {
		t.Fatalf("Unable to train index. Err %v", err)
	}

	data, err := codebook.Marshal()
	if err != nil {
		t.Fatalf("Error marshalling codebook %v", err)
	}

	err = codebook.Close()
	if err != nil {
		t.Fatalf("Error closing codebook %v", err)
	}

	payload := new(codebookIVFRaBitQ_IO)
	err = json.Unmarshal(data, payload)
	if err != nil {
		t.Fatalf("Error unmarshalling codebook payload %v", err)
	}

	payload.Index[0] = payload.Index[0] ^ 0xFF
	corruptData, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Error marshalling corrupt codebook payload %v", err)
	}

	_, err = RecoverCodebook(corruptData, string(common.RaBitQ))
	if !errors.Is(err, cbpkg.ErrChecksumMismatch) {
		t.Fatalf("Expected checksum mismatch error, got %v", err)
	}
}

func TestRecoverCodebookRaBitQInvalidVersion(t *testing.T) {
	seed := time.Now().UnixNano()

	codebook, err := NewCodebookIVFRaBitQ(64, 16, 1, cbpkg.METRIC_L2, false)
	if err != nil {
		t.Fatalf("Unable to create index. Err %v", err)
	}

	vecs := genRandomVecs(64, 512, seed)
	err = codebook.Train(convertTo1D(vecs))
	if err != nil {
		t.Fatalf("Unable to train index. Err %v", err)
	}

	data, err := codebook.Marshal()
	if err != nil {
		t.Fatalf("Error marshalling codebook %v", err)
	}

	err = codebook.Close()
	if err != nil {
		t.Fatalf("Error closing codebook %v", err)
	}

	payload := new(codebookIVFRaBitQ_IO)
	err = json.Unmarshal(data, payload)
	if err != nil {
		t.Fatalf("Error unmarshalling codebook payload %v", err)
	}

	payload.CodebookVer = CodebookVer1 + 1
	invalidVersionData, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Error marshalling invalid-version codebook payload %v", err)
	}

	_, err = RecoverCodebook(invalidVersionData, string(common.RaBitQ))
	if !errors.Is(err, cbpkg.ErrInvalidVersion) {
		t.Fatalf("Expected invalid version error, got %v", err)
	}
}

func TestMarshalCodebookRaBitQIncludesQB(t *testing.T) {
	seed := time.Now().UnixNano()

	codebook, err := NewCodebookIVFRaBitQ(64, 16, 4, cbpkg.METRIC_L2, false)
	if err != nil {
		t.Fatalf("Unable to create index. Err %v", err)
	}

	vecs := genRandomVecs(64, 512, seed)
	err = codebook.Train(convertTo1D(vecs))
	if err != nil {
		t.Fatalf("Unable to train index. Err %v", err)
	}

	data, err := codebook.Marshal()
	if err != nil {
		t.Fatalf("Error marshalling codebook %v", err)
	}

	err = codebook.Close()
	if err != nil {
		t.Fatalf("Error closing codebook %v", err)
	}

	payload := new(codebookIVFRaBitQ_IO)
	err = json.Unmarshal(data, payload)
	if err != nil {
		t.Fatalf("Error unmarshalling codebook payload %v", err)
	}

	if payload.Qb != defaultRaBitQQB {
		t.Fatalf("Unexpected marshalled qb %v. Expected %v", payload.Qb, defaultRaBitQQB)
	}
}

func TestNewCodebookRaBitQWiring(t *testing.T) {
	vectorMeta := &common.VectorMetadata{
		Dimension:  128,
		Similarity: common.L2_SQUARED,
		Quantizer: &common.VectorQuantizer{
			Type:        common.RaBitQ,
			RaBitQNbits: 4,
		},
	}

	cb, err := NewCodebook(vectorMeta, 64)
	if err != nil {
		t.Fatalf("Unable to initialize codebook via NewCodebook. Err %v", err)
	}

	if _, ok := cb.(*codebookIVFRaBitQ); !ok {
		t.Fatalf("Expected *codebookIVFRaBitQ type, got %T", cb)
	}

	err = cb.Close()
	if err != nil {
		t.Fatalf("Error closing codebook %v", err)
	}
}

func TestNewCodebookRaBitQDefaultNbits(t *testing.T) {
	vectorMeta := &common.VectorMetadata{
		Dimension:  128,
		Similarity: common.L2_SQUARED,
		Quantizer: &common.VectorQuantizer{
			Type: common.RaBitQ,
		},
	}

	cb, err := NewCodebook(vectorMeta, 64)
	if err != nil {
		t.Fatalf("Unable to initialize codebook via NewCodebook. Err %v", err)
	}

	rbcb, ok := cb.(*codebookIVFRaBitQ)
	if !ok {
		t.Fatalf("Expected *codebookIVFRaBitQ type, got %T", cb)
	}

	if rbcb.nbits != 1 {
		t.Fatalf("Unexpected default nbits %v. Expected 1", rbcb.nbits)
	}

	err = cb.Close()
	if err != nil {
		t.Fatalf("Error closing codebook %v", err)
	}
}

func TestNewCodebookIVFRaBitQUnsupportedMetric(t *testing.T) {
	_, err := NewCodebookIVFRaBitQ(128, 64, 1, cbpkg.METRIC_L2, true)
	if !errors.Is(err, cbpkg.ErrUnsupportedMetric) {
		t.Fatalf("Expected unsupported metric error for cosine with L2, got %v", err)
	}

	_, err = NewCodebookIVFRaBitQ(128, 64, 1, cbpkg.MetricType(999), false)
	if !errors.Is(err, cbpkg.ErrUnsupportedMetric) {
		t.Fatalf("Expected unsupported metric error for unknown metric, got %v", err)
	}
}
