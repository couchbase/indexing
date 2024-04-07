package vector

import (
	"math/rand"
	"testing"
	"time"

	faiss "github.com/blevesearch/go-faiss"
)

func TestIndexIVFPQ(t *testing.T) {

	var err error

	var indexPQ *faiss.IndexImpl

	dim := 128
	metric := faiss.MetricL2

	nlist := 128
	nsub := 8
	nbits := 8

	indexPQ, err = NewIndexIVFPQ(dim, nlist, nsub, nbits, metric)
	if err != nil || indexPQ == nil {
		t.Errorf("Unable to create index. Err %v", err)
	}
	indexPQ.SetNProbe(3)

	total_vecs := 50000
	num_train_vecs := 10000

	//generate random vectors
	vecs := genRandomVecs(dim, total_vecs)

	//train the index using 10000 vecs
	train_vecs := convertTo1D(vecs[:num_train_vecs])
	err = indexPQ.Train(train_vecs)

	if !indexPQ.IsTrained() {
		t.Errorf("Unable to train index. Err %v", err)
	}

	//add vectors to the index
	all_vecs := convertTo1D(vecs)
	err = indexPQ.Add(all_vecs)
	if err != nil {
		t.Errorf("Unable to add data to index. Err %v", err)
	}

	//search the index
	query_vec := convertTo1D(vecs[:1])
	dist, label, err := indexPQ.Search(query_vec, 2)
	t.Logf("Search results %v %v %v", dist, label, err)
	for _, l := range label {
		if l > int64(total_vecs) {
			t.Errorf("Result label out of range. Total %v. Label %v", total_vecs, l)
		}
	}

	indexFlat, err := NewIndexFlat(dim, metric)
	if err != nil || indexFlat == nil {
		t.Errorf("Unable to create index. Err %v", err)
	}

	err = indexFlat.Add(all_vecs)
	if err != nil {
		t.Errorf("Unable to add data to flat index. Err %v", err)
	}

	//compute recall with exact match
	t.Logf("Testing recall with exact match")
	recall := computeRecall(t, indexPQ, indexFlat, 10, 100, vecs)
	t.Logf("Recall@%v is %v", 10, recall)

	//compute recall with random query vectors
	t.Logf("Testing recall with random query vectors")
	vecs = genRandomVecs(dim, 1000)
	recall = computeRecall(t, indexPQ, indexFlat, 10, 100, vecs)
	t.Logf("Recall@%v is %v", 10, recall)
}

func TestIndexIVFPQ_HNSW(t *testing.T) {

	var err error

	var indexPQ *faiss.IndexImpl

	dim := 128
	metric := faiss.MetricL2

	nlist := 128
	nsub := 8
	nbits := 8

	indexPQ, err = NewIndexIVFPQ_HNSW(dim, nlist, nsub, nbits, metric)
	if err != nil || indexPQ == nil {
		t.Errorf("Unable to create index. Err %v", err)
	}
	indexPQ.SetNProbe(3)

	total_vecs := 50000
	num_train_vecs := 10000

	//generate random vectors
	vecs := genRandomVecs(dim, total_vecs)

	//train the index using 10000 vecs
	train_vecs := convertTo1D(vecs[:num_train_vecs])
	err = indexPQ.Train(train_vecs)

	if !indexPQ.IsTrained() {
		t.Errorf("Unable to train index. Err %v", err)
	}

	//add vectors to the index
	all_vecs := convertTo1D(vecs)
	err = indexPQ.Add(all_vecs)
	if err != nil {
		t.Errorf("Unable to add data to index. Err %v", err)
	}

	//search the index
	query_vec := convertTo1D(vecs[:1])
	dist, label, err := indexPQ.Search(query_vec, 2)
	t.Logf("Search results %v %v %v", dist, label, err)
	for _, l := range label {
		if l > int64(total_vecs) {
			t.Errorf("Result label out of range. Total %v. Label %v", total_vecs, l)
		}
	}

	indexFlat, err := NewIndexFlat(dim, metric)
	if err != nil || indexFlat == nil {
		t.Errorf("Unable to create index. Err %v", err)
	}

	err = indexFlat.Add(all_vecs)
	if err != nil {
		t.Errorf("Unable to add data to flat index. Err %v", err)
	}

	//compute recall with exact match
	t.Logf("Testing recall with exact match")
	recall := computeRecall(t, indexPQ, indexFlat, 10, 100, vecs)
	t.Logf("Recall@%v is %v", 10, recall)

	//compute recall with random query vectors
	t.Logf("Testing recall with random query vectors")
	vecs = genRandomVecs(dim, 1000)
	recall = computeRecall(t, indexPQ, indexFlat, 10, 100, vecs)
	t.Logf("Recall@%v is %v", 10, recall)
}
func computeRecall(t *testing.T,
	testIndex *faiss.IndexImpl,
	flatIndex *faiss.IndexImpl,
	k int64,
	numQueries int,
	qvecs [][]float32) int {

	var posQuery int

	for i := 0; i < numQueries; i++ {
		//select a random query vector
		qnum := rand.Intn(len(qvecs) - 1)
		query_vec := qvecs[qnum]

		//get the top result from the flat index
		_, flabels, err := flatIndex.Search(query_vec, 1)
		if err != nil {
			t.Errorf("Error searching flat index %v", err)
		}

		//get the top k results from the test index
		_, tlabels, err := testIndex.Search(query_vec, k)
		if err != nil {
			t.Errorf("Error searching test index %v", err)
		}

		found := checkResult(t, tlabels, flabels[0])
		if found {
			posQuery++
		}
	}

	recall := (posQuery * 100) / numQueries

	return int(recall)

}

func checkResult(t *testing.T, result []int64, bench int64) bool {

	for _, l := range result {
		if l == bench {
			//	t.Logf("Found matching label at pos %v, label %v", i, l)
			return true
		}
	}
	return false
}

func genRandomVecs(dims int, n int) [][]float32 {
	rand.Seed(time.Now().UnixNano())

	vecs := make([][]float32, n)
	for i := 0; i < n; i++ {
		vecs[i] = make([]float32, dims)
		for j := 0; j < dims; j++ {
			vecs[i][j] = rand.Float32()
		}
	}

	return vecs
}

func convertTo1D(vecs [][]float32) []float32 {

	var vecs1D []float32
	for _, vec := range vecs {
		vecs1D = append(vecs1D, vec...)
	}
	return vecs1D
}

func compareVecs(vec1, vec2 []float32) bool {
	// Check if vectors have the same dimension
	if len(vec1) != len(vec2) {
		return false
	}

	// Iterate over each element and compare
	for i := 0; i < len(vec1); i++ {
		if vec1[i] != vec2[i] {
			return false
		}
	}

	return true
}
