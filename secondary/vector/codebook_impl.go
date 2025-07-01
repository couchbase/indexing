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

// Global concurrency control
var (
	globalMaxLimit     int64 // Maximum possible concurrent operations
	globalCurrentLimit int64 // Current effective concurrency

	globalShardedSem atomic.Pointer[shardedSem] // Sharded semaphore to control concurrency
)

func init() {
	// Set max capacity as 10 times the number of CPUs
	numCores := runtime.NumCPU()
	globalMaxLimit = int64(10 * numCores)

	// Initialize with 1 shard and 1 concurrency per shard
	numShards := 1
	globalCurrentLimit = 1

	initShardedSemaphore(numShards, globalCurrentLimit)

	faiss.SetOMPThreads(defaultOMPThreads)
	var gomaxprocs = runtime.GOMAXPROCS(-1)
	os.Setenv("OMP_THREAD_LIMIT", strconv.Itoa(gomaxprocs))
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
