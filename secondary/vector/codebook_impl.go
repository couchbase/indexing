// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package vector

import (
	"errors"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	codebook "github.com/couchbase/indexing/secondary/vector/codebook"
	faiss "github.com/couchbase/indexing/secondary/vector/faiss"
)

//OpenBLAS is being compiled with max NUM_THREADS as 256. Limit the
//max concurrency to the same number to prevent exceptions from the library.
const MAX_GLOBAL_CONCURRENCY = 256

// Global concurrency control
var (
	globalMaxLimit     int64 // Maximum possible concurrent operations
	globalCurrentLimit int64 // Current effective concurrency

	globalShardedSem atomic.Pointer[shardedSem] // Sharded semaphore to control concurrency

	//training concurrency control
	trainingConcurrencyLock sync.RWMutex
	trainingSemaphore       chan struct{}
	trainingMaxCapacity     int // Maximum possible concurrent trainings
	trainingCurrentCapacity int // Current effective training capacity

)

func init() {
	// Set max capacity as 10 times the number of CPUs
	numCores := runtime.NumCPU()
	globalMaxLimit = int64(10 * numCores)

	if globalMaxLimit > MAX_GLOBAL_CONCURRENCY {
		globalMaxLimit = MAX_GLOBAL_CONCURRENCY
	}

	// Initialize with 1 shard and 1 concurrency per shard
	numShards := 1
	globalCurrentLimit = 1

	initShardedSemaphore(numShards, globalCurrentLimit)

	//set max training capacity as the number of CPUs
	trainingMaxCapacity = numCores
	trainingSemaphore = make(chan struct{}, trainingMaxCapacity)
	trainingSemaphore <- struct{}{}
	trainingCurrentCapacity = 1 //minimum 1 training is allowed

	var gomaxprocs = runtime.GOMAXPROCS(-1)
	os.Setenv("OMP_THREAD_LIMIT", strconv.Itoa(gomaxprocs/2))
	faiss.SetOMPThreads(defaultOMPThreads)
	os.Setenv("OMP_WAIT_POLICY", defaultWaitPolicy)
	os.Setenv("OPENBLAS_NUM_THREADS", strconv.Itoa(1))
}

func initShardedSemaphore(numShards int, capacityPerShard int64) {

	shards := make([]*atomicSem, numShards)
	for i := 0; i < numShards; i++ {
		shards[i] = NewAtomicSemaphore(capacityPerShard)
	}

	newShardedSem := &shardedSem{
		shards:    shards,
		numShards: numShards,
		rngPool: sync.Pool{
			New: func() any {
				seed := time.Now().UnixNano() ^ int64(rand.Int())
				src := rand.NewSource(seed)
				return rand.New(src)
			},
		},
	}

	globalShardedSem.Store(newShardedSem)
}

//SetOMPThreadLimit sets the thread limit for OpenMP regions.
//It is best to call this before calling into faiss for regular operations.
func SetOMPThreadLimit(maxThreads int) {
	os.Setenv("OMP_THREAD_LIMIT", strconv.Itoa(maxThreads))
}

// SetConcurrency sets the global maximum number of concurrent codebook operations
func SetConcurrency(maxConcurrent int64) {

	newLimit := maxConcurrent
	if maxConcurrent <= 0 {
		newLimit = 1
	} else {
		if maxConcurrent > globalMaxLimit {
			logging.Warnf("Codebook::SetConcurrency Requested capacity %d exceeds maximum %d, using maximum",
				maxConcurrent, globalMaxLimit)
			newLimit = globalMaxLimit
		}
	}

	numShards := 1
	capacityPerShard := int64(1)

	if newLimit > 4 {
		//Use 4 as fixed capacity of a shard. The perf degradation due to cache
		//coherence is minimal with 4 cores.
		capacityPerShard = int64(4)
		numShards = int(newLimit / capacityPerShard)
	} else {
		capacityPerShard = newLimit
	}

	initShardedSemaphore(numShards, capacityPerShard)

    atomic.StoreInt64(&globalCurrentLimit, newLimit)
}

// GetConcurrency returns the current global maximum number of concurrent operations
func GetConcurrency() int64 {
	return atomic.LoadInt64(&globalCurrentLimit)
}

// acquireGlobal acquires a permit from the global semaphore
func acquireGlobal() *shardedToken {
	return globalShardedSem.Load().Acquire()
}

// releaseGlobal releases a permit back to the global semaphore
func releaseGlobal(token *shardedToken) {
	token.Release()
}

// SetTrainingConcurrency sets the maximum number of concurrent training operations
// 1 is the minimum training allowed.
func SetTrainingConcurrency(maxConcurrent int) {
	trainingConcurrencyLock.Lock()
	defer trainingConcurrencyLock.Unlock()

	if maxConcurrent <= 0 {
		// Keep 1 permit minimum
		for i := 0; i < trainingCurrentCapacity-1; i++ {
			<-trainingSemaphore // Block until permit is available
		}
		trainingCurrentCapacity = 1
	} else {
		if maxConcurrent > trainingMaxCapacity {
			logging.Warnf("Codebook::SetTrainingConcurrency Requested capacity %d exceeds maximum %d, using maximum",
				maxConcurrent, trainingMaxCapacity)
			maxConcurrent = trainingMaxCapacity
		}

		diff := maxConcurrent - trainingCurrentCapacity
		if diff > 0 {
			// Increasing capacity - add permits to channel
			for i := 0; i < diff; i++ {
				trainingSemaphore <- struct{}{}
			}
		} else if diff < 0 {
			// Decreasing capacity - remove permits from channel
			for i := 0; i < -diff; i++ {
				<-trainingSemaphore // Block until permit is available
			}
		}
		trainingCurrentCapacity = maxConcurrent
	}
}

// GetTrainingConcurrency returns the current global maximum number of concurrent trainings.
func GetTrainingConcurrency() int {
	trainingConcurrencyLock.RLock()
	defer trainingConcurrencyLock.RUnlock()

	return trainingCurrentCapacity
}

// acquireTraining acquires a permit from the training semaphore
func acquireTraining() {
	<-trainingSemaphore
}

// releaseTraining releases a permit back to the training semaphore
func releaseTraining() {
	select {
	case trainingSemaphore <- struct{}{}:
		// Successfully returned the permit
	default:
		// Channel is full
		logging.Warnf("Codebook::releaseTraining Attempted to release when channel is full")
	}
}

type CodebookVer int

const (
	CodebookVer1 = iota
)

func RecoverCodebook(data []byte, qType string) (codebook.Codebook, error) {

	switch qType {
	case "PQ":
		return recoverCodebookIVFPQ(data)
	case "SQ":
		return recoverCodebookIVFSQ(data)
	}

	return nil, codebook.ErrUnknownType
}

func convertToFaissMetric(metric codebook.MetricType) int {

	switch metric {
	case codebook.METRIC_L2:
		return faiss.MetricL2
	case codebook.METRIC_INNER_PRODUCT:
		return faiss.MetricInnerProduct

	}
	//default to L2
	return faiss.MetricL2
}

func NewCodebook(vectorMeta *common.VectorMetadata, nlist int) (cb codebook.Codebook, err error) {
	metric, useCosine := codebook.ConvertSimilarityToMetric(vectorMeta.Similarity)
	switch vectorMeta.Quantizer.Type {
	case common.PQ:
		nsub := vectorMeta.Quantizer.SubQuantizers
		if vectorMeta.Quantizer.FastScan {
			nsub = vectorMeta.Quantizer.BlockSize
		}
		cb, err = NewCodebookIVFPQ(vectorMeta.Dimension, nsub, vectorMeta.Quantizer.Nbits,
			nlist, metric, useCosine, vectorMeta.Quantizer.FastScan)
		if err != nil {
			return nil, err
		}
		logging.Infof("NewCodebookIVFPQ: Initialized codebook with dimension: %v, subquantizers: %v, "+
			"nbits: %v, nlist: %v, fastScan: %v, metric: %v, useCosine: %v", vectorMeta.Dimension, nsub,
			vectorMeta.Quantizer.Nbits, nlist, vectorMeta.Quantizer.FastScan, metric, useCosine)

	case common.SQ:
		cb, err = NewCodebookIVFSQ(vectorMeta.Dimension, nlist, vectorMeta.Quantizer.SQRange, metric, useCosine)
		if err != nil {
			return nil, err
		}
		logging.Infof("NewCodebookIVFSQ: Initialized codebook with dimension: %v, range: %v, nlist: %v, "+
			"metric: %v, useCosine: %v",
			vectorMeta.Dimension, vectorMeta.Quantizer.SQRange, nlist, metric, useCosine)

	default:
		return nil, errors.New("Unsupported quantisation type")
	}

	return cb, nil
}

//Atomic Semaphore is a simple counting semaphore implementation that uses an atomic counter
//to track the number of permits available. Atomic counter is used to avoid the overheads of a mutex.

type atomicSem struct {
	limit int64
	count int64
}

type atomicToken struct {
	sem *atomicSem
}

func (t *atomicToken) Release() {
	atomic.AddInt64(&t.sem.count, -1)
}

func NewAtomicSemaphore(limit int64) *atomicSem {
	return &atomicSem{
		limit: limit,
	}
}

func (s *atomicSem) Acquire() *atomicToken {
	spin := 0
	for {
		cur := atomic.LoadInt64(&s.count)
		lim := atomic.LoadInt64(&s.limit)
		if cur < lim && atomic.CompareAndSwapInt64(&s.count, cur, cur+1) {
			return &atomicToken{sem: s}
		}

		// Adaptive backoff
		if spin < 10 {
			time.Sleep(10 * time.Microsecond)
		} else if spin < 20 {
			// Short sleep (reduce contention, low latency)
			time.Sleep(20 * time.Microsecond)
		} else {
			// Longer sleep (very high contention)
			time.Sleep(100 * time.Microsecond)
		}
		spin++
	}
}

//Sharded Atomic Semaphore is a wrapper around atomicSem that allows for multiple
//shards of the same semaphore. Sharding helps in scaling the semaphore across large
//number of cores by reducing the cache coherence overheads.

type shardedSem struct {
	shards    []*atomicSem
	numShards int
	rngPool   sync.Pool
}

type shardedToken struct {
	token *atomicToken
}

func (t *shardedToken) Release() {
	t.token.Release()
}

func (s *shardedSem) Acquire() *shardedToken {
	rng := s.rngPool.Get().(*rand.Rand)
	idx := rng.Intn(s.numShards)
	s.rngPool.Put(rng)

	return &shardedToken{token: s.shards[idx].Acquire()}
}
