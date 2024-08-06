package vector

import (
	"math/rand"
	"sort"
	"testing"
	"time"

	faiss "github.com/couchbase/indexing/secondary/vector/faiss"
)

func TestIndexIVFPQ(t *testing.T) {

	var err error

	var indexPQ *faiss.IndexImpl

	dim := 128
	metric := faiss.MetricL2

	nlist := 128
	nsub := 8
	nbits := 8
	useFastScan := false

	indexPQ, err = NewIndexIVFPQ(dim, nlist, nsub, nbits, metric, useFastScan)
	if err != nil || indexPQ == nil {
		t.Errorf("Unable to create index. Err %v", err)
	}
	indexPQ.SetNProbe(3)

	total_vecs := 50000
	num_train_vecs := 10000

	//generate random vectors
	vecs := genRandomVecs(dim, total_vecs, 0)

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
	vecs = genRandomVecs(dim, 1000, 0)
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
	useFastScan := false

	indexPQ, err = NewIndexIVFPQ_HNSW(dim, nlist, nsub, nbits, metric, useFastScan)
	if err != nil || indexPQ == nil {
		t.Errorf("Unable to create index. Err %v", err)
	}
	indexPQ.SetNProbe(3)

	total_vecs := 50000
	num_train_vecs := 10000

	//generate random vectors
	vecs := genRandomVecs(dim, total_vecs, 0)

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
	vecs = genRandomVecs(dim, 1000, 0)
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

func TestIndexIVFPQ_Timing(t *testing.T) {

	var err error

	var indexPQ *faiss.IndexImpl

	dim := 128
	metric := faiss.MetricL2

	nlist := 1024
	nsub := 8
	nbits := 8
	useFastScan := false

	indexPQ, err = NewIndexIVFPQ(dim, nlist, nsub, nbits, metric, useFastScan)
	if err != nil || indexPQ == nil {
		t.Errorf("Unable to create index. Err %v", err)
	}

	total_vecs := 100000

	//generate random vectors
	vecs := genRandomVecs(dim, total_vecs, 0)

	//train the index
	train_vecs := convertTo1D(vecs)

	start := time.Now()
	err = indexPQ.Train(train_vecs)
	elapsed := time.Since(start)

	if !indexPQ.IsTrained() {
		t.Errorf("Unable to train index. Err %v", err)
	}

	t.Logf("Time taken for training %v vectors is %v", total_vecs, elapsed)

	//search the index
	query_vec := convertTo1D(vecs[:1])

	nn := []int64{1, 3, 10}

	quantizer, err := indexPQ.Quantizer()
	if err != nil {
		t.Errorf("Error getting quantizer for index. Err %v", err)
	}

	for _, n := range nn {
		start = time.Now()
		label, err := quantizer.Assign(query_vec, n)
		elapsed = time.Since(start)

		t.Logf("Assign results %v %v", label, err)
		t.Logf("Time taken to assign %v %v", n, elapsed)
	}

}

func TestIndexIVFPQ_HNSW_Timing(t *testing.T) {

	var err error

	var indexPQ *faiss.IndexImpl

	dim := 128
	metric := faiss.MetricL2

	nlist := 1024
	nsub := 8
	nbits := 8
	useFastScan := false

	indexPQ, err = NewIndexIVFPQ_HNSW(dim, nlist, nsub, nbits, metric, useFastScan)
	if err != nil || indexPQ == nil {
		t.Errorf("Unable to create index. Err %v", err)
	}

	total_vecs := 100000

	//generate random vectors
	vecs := genRandomVecs(dim, total_vecs, 0)

	//train the index
	train_vecs := convertTo1D(vecs)

	start := time.Now()
	err = indexPQ.Train(train_vecs)
	elapsed := time.Since(start)

	if !indexPQ.IsTrained() {
		t.Errorf("Unable to train index. Err %v", err)
	}

	t.Logf("Time taken for training %v vectors is %v", total_vecs, elapsed)

	//search the index
	query_vec := convertTo1D(vecs[:1])

	nn := []int64{1, 3, 10}

	quantizer, err := indexPQ.Quantizer()
	if err != nil {
		t.Errorf("Error getting quantizer for index. Err %v", err)
	}

	for _, n := range nn {
		start = time.Now()
		label, err := quantizer.Assign(query_vec, n)
		elapsed = time.Since(start)

		t.Logf("Assign results %v %v", label, err)
		t.Logf("Time taken to assign %v %v", n, elapsed)
	}

}

func TestDistanceMetrics(t *testing.T) {
	dim := 128
	total_vecs := 10

	//generate random vectors
	vecs := genRandomVecs(dim, total_vecs, 0)
	serialVecs := convertTo1D(vecs)
	query_vec := convertTo1D(vecs[:1])

	metric := faiss.MetricL2
	indexFlat, err := NewIndexFlat(dim, metric)
	if err != nil || indexFlat == nil {
		t.Errorf("Unable to create index. Err %v", err)
	}

	err = indexFlat.Add(serialVecs)
	if err != nil {
		t.Errorf("Unable to add data to flat index. Err %v", err)
	}

	dists, _, err := indexFlat.Search(query_vec, int64(total_vecs))
	t.Logf("METRIC_L2 distances using index: %v", dists)

	dists = make([]float32, total_vecs)
	faiss.L2sqrNy(dists, query_vec, serialVecs, dim)

	sort.Slice(dists, func(i, j int) bool {
		return dists[i] < dists[j]
	})

	t.Logf("METRIC_L2 distances: %v", dists)

	metric = faiss.MetricInnerProduct
	indexFlat, err = NewIndexFlat(dim, metric)
	if err != nil || indexFlat == nil {
		t.Errorf("Unable to create index. Err %v", err)
	}

	err = indexFlat.Add(serialVecs)
	if err != nil {
		t.Errorf("Unable to add data to flat index. Err %v", err)
	}

	dists, _, err = indexFlat.Search(query_vec, int64(total_vecs))	
	t.Logf("METRIC_INNER_PRODUCE distances using index: %v", dists)

	dists = make([]float32, total_vecs)
	faiss.InnerProductsNy(dists, query_vec, serialVecs, dim)

	sort.Slice(dists, func(i, j int) bool {
		return dists[i] > dists[j]
	})

	t.Logf("METRIC_INNER_PRODUCT: %v", dists)
}