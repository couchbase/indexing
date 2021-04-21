// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package indexer

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/couchbase/indexing/secondary/common"
	commonjson "github.com/couchbase/indexing/secondary/common/json"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/stats"
	"github.com/couchbase/indexing/secondary/stubs/nitro/mm"
	"github.com/couchbase/logstats/logstats"
	"github.com/golang/snappy"
)

var uptime time.Time
var num_cpu_core int

const APPROX_METRIC_SIZE = 100
const APPROX_METRIC_COUNT = 25

var METRICS_PREFIX = "index_"

// 0-2ms, 2ms-5ms, 5ms-10ms, 10ms-20ms, 20ms-30ms, 30ms-50ms, 50ms-100ms, 100ms-Inf
var latencyDist = []int64{0, 2, 5, 10, 20, 30, 50, 100}

// 0-2ms, 2ms-5ms, 5ms-10ms, 10ms-20ms, 20ms-30ms, 30ms-50ms, 50ms-100ms, 100ms-1000ms,
// 1000ms-5000ms, 5000ms-10000ms, 10000ms-Inf
var snapLatencyDist = []int64{0, 2, 5, 10, 20, 30, 50, 100, 1000, 5000, 10000}

// end-end scan request latency
// 0-2ms, 2ms-5ms, 5ms-10ms, 10ms-20ms, 20ms-30ms, 30ms-50ms, 50ms-100ms, 100ms-1000ms,
// 1000ms-5000ms, 5000ms-10000ms, 10000ms-50000ms, 50000ms-Inf
var scanReqLatencyDist = []int64{0, 2, 5, 10, 20, 30, 50, 100, 1000, 5000, 10000, 50000}

func init() {
	uptime = time.Now()
	num_cpu_core = runtime.NumCPU()
}

// KeyspaceStats tracks statistics of all indexes in a given keyspace in a stream.
// It is used internally for debugging and available in unspecified format under the
// "GET /api/v1/stats" REST API but is not used by the UI.
type KeyspaceStats struct {
	keyspaceId string

	// Statistics in alphabetical order
	mutationQueueSize  stats.Int64Val
	numMutationsQueued stats.Int64Val
	numNonAlignTS      stats.Int64Val
	numRollbacks       stats.Int64Val
	numRollbacksToZero stats.Int64Val
	tsQueueSize        stats.Int64Val
	flushLatDist       stats.Histogram
	snapLatDist        stats.Histogram
	lastSnapDone       stats.Int64Val
}

// KeyspaceStats.Init initializes a per-keyspace stats object.
func (s *KeyspaceStats) Init(keyspaceId string) {
	s.keyspaceId = keyspaceId
	s.numRollbacks.Init()
	s.numRollbacksToZero.Init()
	s.mutationQueueSize.Init()
	s.numMutationsQueued.Init()
	s.tsQueueSize.Init()
	s.numNonAlignTS.Init()
	s.flushLatDist.InitLatency(latencyDist, func(v int64) string { return fmt.Sprintf("%vms", v/int64(time.Millisecond)) })
	s.snapLatDist.InitLatency(snapLatencyDist, func(v int64) string { return fmt.Sprintf("%vms", v/int64(time.Millisecond)) })
	s.lastSnapDone.Init()
}

func (s *KeyspaceStats) addKeyspaceStatsToStatsMap(statMap *StatsMap) {
	statMap.AddStatValueFiltered("num_rollbacks", &s.numRollbacks)
	statMap.AddStatValueFiltered("num_rollbacks_to_zero", &s.numRollbacksToZero)
	statMap.AddStatValueFiltered("mutation_queue_size", &s.mutationQueueSize)
	statMap.AddStatValueFiltered("num_mutations_queued", &s.numMutationsQueued)
	statMap.AddStatValueFiltered("ts_queue_size", &s.tsQueueSize)
	statMap.AddStatValueFiltered("num_nonalign_ts", &s.numNonAlignTS)
	statMap.AddStatValueFiltered("flush_latency_dist", &s.flushLatDist)
	statMap.AddStatValueFiltered("snapshot_latency_dist", &s.snapLatDist)

	bucket := GetBucketFromKeyspaceId(s.keyspaceId)
	if st := common.BucketSeqsTiming(bucket); st != nil {
		statMap.AddStatValueFiltered("timings/dcp_getseqs", st)
	}
}

type IndexTimingStats struct {
	stCloneHandle           stats.TimingStat
	stNewIterator           stats.TimingStat
	stIteratorNext          stats.TimingStat
	stSnapshotCreate        stats.TimingStat
	stSnapshotClose         stats.TimingStat
	stPersistSnapshotCreate stats.TimingStat
	stScanPipelineIterate   stats.TimingStat
	stCommit                stats.TimingStat
	stKVGet                 stats.TimingStat
	stKVSet                 stats.TimingStat
	stKVDelete              stats.TimingStat
	stKVInfo                stats.TimingStat
	stKVMetaGet             stats.TimingStat
	stKVMetaSet             stats.TimingStat
	dcpSeqs                 stats.TimingStat
	n1qlExpr                stats.TimingStat
}

func (it *IndexTimingStats) Init() {
	it.stCloneHandle.Init()
	it.stCommit.Init()
	it.stNewIterator.Init()
	it.stSnapshotCreate.Init()
	it.stSnapshotClose.Init()
	it.stPersistSnapshotCreate.Init()
	it.stKVGet.Init()
	it.stKVSet.Init()
	it.stIteratorNext.Init()
	it.stScanPipelineIterate.Init()
	it.stKVDelete.Init()
	it.stKVInfo.Init()
	it.stKVMetaGet.Init()
	it.stKVMetaSet.Init()
	it.dcpSeqs.Init()
	it.n1qlExpr.Init()
}

type IndexStats struct {
	name, scope, collection, bucket, dispName string

	indexState stats.Uint64Val // Only used by lifecycle manager to filter indexes in MAINT_STREAM

	replicaId    int
	isArrayIndex bool

	partitions map[common.PartitionId]*IndexStats

	scanDuration              stats.Int64Val
	scanReqDuration           stats.Int64Val
	scanReqInitDuration       stats.Int64Val
	scanReqAllocDuration      stats.Int64Val
	dcpSeqsDuration           stats.Int64Val
	insertBytes               stats.Int64Val
	numDocsPending            stats.Int64Val
	scanWaitDuration          stats.Int64Val
	numDocsIndexed            stats.Int64Val
	numDocsProcessed          stats.Int64Val
	numRequests               stats.Int64Val
	lastScanTime              stats.Int64Val
	numCompletedRequests      stats.Int64Val
	numRowsReturned           stats.Int64Val
	numRequestsRange          stats.Int64Val
	numCompletedRequestsRange stats.Int64Val
	numRowsReturnedRange      stats.Int64Val
	numRowsScannedRange       stats.Int64Val
	scanCacheHitRange         stats.Int64Val
	numRequestsAggr           stats.Int64Val
	numCompletedRequestsAggr  stats.Int64Val
	numRowsReturnedAggr       stats.Int64Val
	numRowsScannedAggr        stats.Int64Val
	scanCacheHitAggr          stats.Int64Val
	numRowsScanned            stats.Int64Val
	numStrictConsReqs         stats.Int64Val
	diskSize                  stats.Int64Val
	memUsed                   stats.Int64Val
	buildProgress             stats.Int64Val
	completionProgress        stats.Int64Val
	numDocsQueued             stats.Int64Val
	deleteBytes               stats.Int64Val
	dataSize                  stats.Int64Val
	dataSizeOnDisk            stats.Int64Val
	logSpaceOnDisk            stats.Int64Val
	rawDataSize               stats.Int64Val // Sum of all data inserted into main store and back store
	backstoreRawDataSize      stats.Int64Val // Sum of all data inserted into back store
	docidCount                stats.Int64Val
	scanBytesRead             stats.Int64Val
	getBytes                  stats.Int64Val
	itemsCount                stats.Int64Val
	numCommits                stats.Int64Val
	numSnapshots              stats.Int64Val
	numOpenSnapshots          stats.Int64Val
	numCompactions            stats.Int64Val
	numItemsFlushed           stats.Int64Val
	avgTsInterval             stats.Int64Val
	avgTsItemsCount           stats.Int64Val
	lastNumFlushQueued        stats.Int64Val
	lastTsTime                stats.Int64Val
	numDocsFlushQueued        stats.Int64Val
	fragPercent               stats.Int64Val
	sinceLastSnapshot         stats.Int64Val
	numSnapshotWaiters        stats.Int64Val
	numLastSnapshotReply      stats.Int64Val
	numItemsRestored          stats.Int64Val
	diskSnapStoreDuration     stats.Int64Val
	diskSnapLoadDuration      stats.Int64Val
	notReadyError             stats.Int64Val
	clientCancelError         stats.Int64Val
	numScanTimeouts           stats.Int64Val
	numScanErrors             stats.Int64Val
	avgScanRate               stats.Int64Val
	avgMutationRate           stats.Int64Val
	avgDrainRate              stats.Int64Val
	avgDiskBps                stats.Int64Val
	lastScanGatherTime        stats.Int64Val
	lastNumRowsScanned        stats.Int64Val
	lastMutateGatherTime      stats.Int64Val
	lastNumDocsIndexed        stats.Int64Val
	lastNumItemsFlushed       stats.Int64Val
	lastDiskBytes             stats.Int64Val
	lastRollbackTime          stats.TimeVal
	progressStatTime          stats.TimeVal
	residentPercent           stats.Int64Val
	cacheHitPercent           stats.Int64Val
	cacheHits                 stats.Int64Val
	cacheMisses               stats.Int64Val
	numRecsInMem              stats.Int64Val
	numRecsOnDisk             stats.Int64Val

	numKeySize64     stats.Int64Val // 0 - 64
	numKeySize256    stats.Int64Val // 65 - 256
	numKeySize1K     stats.Int64Val // 257 - 1K
	numKeySize4K     stats.Int64Val // 1025 to 4096
	numKeySize100K   stats.Int64Val // 4097 - 102400
	numKeySizeGt100K stats.Int64Val // > 102400

	numArrayKeySize64     stats.Int64Val
	numArrayKeySize256    stats.Int64Val
	numArrayKeySize1K     stats.Int64Val
	numArrayKeySize4K     stats.Int64Val
	numArrayKeySize100K   stats.Int64Val
	numArrayKeySizeGt100K stats.Int64Val

	keySizeStatsSince stats.Int64Val // Since when are key size stats tracked

	//stats needed for avg_scan_latency
	lastScanDuration stats.Int64Val
	lastNumRequests  stats.Int64Val
	avgScanLatency   stats.Int64Val

	Timings IndexTimingStats

	// Placeholder stats used during GetStats call.
	flushQueue       stats.Int64Val
	avgItemSize      stats.Int64Val
	waitLat          stats.Int64Val
	scanReqLat       stats.Int64Val
	scanReqInitLat   stats.Int64Val
	scanReqAllocLat  stats.Int64Val
	docidCountHolder stats.Int64Val
	avgArrLenHolder  stats.Int64Val
	keySizeDist      stats.MapVal
	arrKeySizeDist   stats.MapVal

	scanReqInitLatDist stats.Histogram
	scanReqWaitLatDist stats.Histogram
	scanReqLatDist     stats.Histogram
	snapGenLatDist     stats.Histogram
}

type IndexerStatsHolder struct {
	ptr unsafe.Pointer
}

func (h *IndexerStatsHolder) Get() *IndexerStats {
	return (*IndexerStats)(atomic.LoadPointer(&h.ptr))
}

func (h *IndexerStatsHolder) Set(s *IndexerStats) {
	atomic.StorePointer(&h.ptr, unsafe.Pointer(s))
}

func (h *IndexerStatsHolder) GetKeyspaceStats(streamId common.StreamId, keyspaceId string) *KeyspaceStats {
	return h.GetKeyspaceStatsMap()[streamId][keyspaceId]
}

func (h *IndexerStatsHolder) GetKeyspaceStatsMap() KeyspaceStatsMap {
	return h.Get().GetKeyspaceStatsMap()
}

func (s *IndexerStats) GetKeyspaceStats(streamId common.StreamId, keyspaceId string) *KeyspaceStats {
	return s.GetKeyspaceStatsMap()[streamId][keyspaceId]
}

func (s *IndexerStats) GetKeyspaceStatsMap() KeyspaceStatsMap {
	return s.keyspaceStatsMap.Get()
}

// KeyspaceStatsMap holds a map of stream IDs to maps of keyspace IDs to per-keyspace
// stats. The outer map avoids key collisions between streams. Only INIT_STREAM
// and MAINT_STREAM have entries as other streams do not keep these stats. These two
// stream maps are always present (see NewKeyspaceStatsMap immediately below). The
// code should never try to get a map for any other stream, and nil is not checked
// for lookups of the streamId dimension.
type KeyspaceStatsMap map[common.StreamId]map[string]*KeyspaceStats

func NewKeyspaceStatsMap() KeyspaceStatsMap {
	var ksm KeyspaceStatsMap = make(map[common.StreamId]map[string]*KeyspaceStats)

	ksm[common.INIT_STREAM] = make(map[string]*KeyspaceStats)
	ksm[common.MAINT_STREAM] = make(map[string]*KeyspaceStats)

	return ksm
}

// KeyspaceStatsMapHolder holds an atomic pointer to a KeyspaceStatsMap.
type KeyspaceStatsMapHolder struct {
	ptr unsafe.Pointer
}

func (h *KeyspaceStatsMapHolder) Init() {
	h.Set(NewKeyspaceStatsMap())
}

func (h *KeyspaceStatsMapHolder) Get() KeyspaceStatsMap {
	return *(*KeyspaceStatsMap)(atomic.LoadPointer(&h.ptr))
}

func (h *KeyspaceStatsMapHolder) Set(s KeyspaceStatsMap) {
	atomic.StorePointer(&h.ptr, unsafe.Pointer(&s))
}

type LatencyMapHolder struct {
	ptr *unsafe.Pointer
}

func (p *LatencyMapHolder) Init() {
	p.ptr = new(unsafe.Pointer)
}

func (p *LatencyMapHolder) Set(prjLatencyMap map[string]interface{}) {
	atomic.StorePointer(p.ptr, unsafe.Pointer(&prjLatencyMap))
}

func (p *LatencyMapHolder) Get() map[string]interface{} {
	if ptr := atomic.LoadPointer(p.ptr); ptr != nil {
		return *(*map[string]interface{})(ptr)
	} else {
		return make(map[string]interface{})
	}
}

func (p *LatencyMapHolder) Clone() map[string]interface{} {
	if ptr := atomic.LoadPointer(p.ptr); ptr != nil {
		currMap := *(*map[string]interface{})(ptr)
		clone := make(map[string]interface{})
		for k, v := range currMap {
			clone[k] = v
		}
		return clone
	} else {
		return make(map[string]interface{})
	}
}

// Contains the mapping between nodeUUID to hostname of a KV node
type NodeToHostMapHolder struct {
	ptr *unsafe.Pointer
}

func (n *NodeToHostMapHolder) Init() {
	n.ptr = new(unsafe.Pointer)
}

func (n *NodeToHostMapHolder) Set(nodeToHostMap map[string]string) {
	atomic.StorePointer(n.ptr, unsafe.Pointer(&nodeToHostMap))
}

func (n *NodeToHostMapHolder) Get() map[string]string {
	if ptr := atomic.LoadPointer(n.ptr); ptr != nil {
		return *(*map[string]string)(ptr)
	} else {
		return make(map[string]string)
	}
}

func (s *IndexStats) Init() {
	s.indexState.Init()
	s.scanDuration.Init()
	s.scanReqDuration.Init()
	s.scanReqInitDuration.Init()
	s.scanReqAllocDuration.Init()
	s.insertBytes.Init()
	s.numDocsPending.Init()
	s.scanWaitDuration.Init()
	s.numDocsIndexed.Init()
	s.numDocsProcessed.Init()
	s.numRequests.Init()
	s.lastScanTime.Init()
	s.numCompletedRequests.Init()
	s.numRowsReturned.Init()
	s.numRequestsRange.Init()
	s.numCompletedRequestsRange.Init()
	s.numRowsReturnedRange.Init()
	s.numRowsScannedRange.Init()
	s.scanCacheHitRange.Init()
	s.numRequestsAggr.Init()
	s.numCompletedRequestsAggr.Init()
	s.numRowsReturnedAggr.Init()
	s.numRowsScannedAggr.Init()
	s.scanCacheHitAggr.Init()
	s.numRowsScanned.Init()
	s.numStrictConsReqs.Init()
	s.diskSize.Init()
	s.memUsed.Init()
	s.buildProgress.Init()
	s.completionProgress.Init()
	s.numDocsQueued.Init()
	s.deleteBytes.Init()
	s.dataSize.Init()
	s.dataSizeOnDisk.Init()
	s.logSpaceOnDisk.Init()
	s.rawDataSize.Init()
	s.backstoreRawDataSize.Init()
	s.docidCount.Init()
	s.fragPercent.Init()
	s.scanBytesRead.Init()
	s.getBytes.Init()
	s.itemsCount.Init()
	s.avgTsInterval.Init()
	s.avgTsItemsCount.Init()
	s.lastNumFlushQueued.Init()
	s.lastTsTime.Init()
	s.numCommits.Init()
	s.numSnapshots.Init()
	s.numOpenSnapshots.Init()
	s.numCompactions.Init()
	s.numItemsFlushed.Init()
	s.numDocsFlushQueued.Init()
	s.sinceLastSnapshot.Init()
	s.numSnapshotWaiters.Init()
	s.numLastSnapshotReply.Init()
	s.numItemsRestored.Init()
	s.diskSnapStoreDuration.Init()
	s.diskSnapLoadDuration.Init()
	s.notReadyError.Init()
	s.clientCancelError.Init()
	s.numScanTimeouts.Init()
	s.numScanErrors.Init()
	s.avgScanRate.Init()
	s.avgMutationRate.Init()
	s.avgDrainRate.Init()
	s.avgDiskBps.Init()
	s.lastScanGatherTime.Init()
	s.lastNumRowsScanned.Init()
	s.lastMutateGatherTime.Init()
	s.lastNumDocsIndexed.Init()
	s.lastNumItemsFlushed.Init()
	s.lastDiskBytes.Init()
	s.lastRollbackTime.Init()
	s.progressStatTime.Init()
	s.residentPercent.Init()
	s.cacheHitPercent.Init()
	s.cacheHits.Init()
	s.cacheMisses.Init()
	s.numRecsInMem.Init()
	s.numRecsOnDisk.Init()

	s.numKeySize64.Init()
	s.numKeySize256.Init()
	s.numKeySize1K.Init()
	s.numKeySize4K.Init()
	s.numKeySize100K.Init()
	s.numKeySizeGt100K.Init()

	s.numArrayKeySize64.Init()
	s.numArrayKeySize256.Init()
	s.numArrayKeySize1K.Init()
	s.numArrayKeySize4K.Init()
	s.numArrayKeySize100K.Init()
	s.numArrayKeySizeGt100K.Init()

	s.keySizeStatsSince.Init()

	//stats needed for avg_scan_latency
	s.lastScanDuration.Init()
	s.lastNumRequests.Init()
	s.avgScanLatency.Init()

	s.Timings.Init()

	s.flushQueue.Init()
	s.avgItemSize.Init()
	s.waitLat.Init()
	s.scanReqLat.Init()
	s.scanReqInitLat.Init()
	s.scanReqAllocLat.Init()
	s.docidCountHolder.Init()
	s.avgArrLenHolder.Init()
	s.keySizeDist.Init()
	s.arrKeySizeDist.Init()

	s.scanReqInitLatDist.InitLatency(latencyDist, func(v int64) string { return fmt.Sprintf("%vms", v/int64(time.Millisecond)) })
	s.scanReqWaitLatDist.InitLatency(latencyDist, func(v int64) string { return fmt.Sprintf("%vms", v/int64(time.Millisecond)) })
	s.scanReqLatDist.InitLatency(scanReqLatencyDist, func(v int64) string { return fmt.Sprintf("%vms", v/int64(time.Millisecond)) })
	s.snapGenLatDist.InitLatency(snapLatencyDist, func(v int64) string { return fmt.Sprintf("%vms", v/int64(time.Millisecond)) })

	s.partitions = make(map[common.PartitionId]*IndexStats)

	// Set filters
	// Note that the filters will be set on both: instance level stats and
	// partition level stats. Instance level filter map and the partition
	// level filter map is the same for each stat value.
	s.SetRebalancerFilters()
	s.SetPlannerFilters()
	s.SetIndexStatusFilters()
	s.SetGSIClientFilters()
	s.dispName = common.FormatIndexInstDisplayName(s.name, s.replicaId)
}

func (s *IndexStats) SetRebalancerFilters() {
	s.numRequests.AddFilter(stats.RebalancerFilter)
	s.numCompletedRequests.AddFilter(stats.RebalancerFilter)
	s.numDocsPending.AddFilter(stats.RebalancerFilter)
	s.numDocsQueued.AddFilter(stats.RebalancerFilter)
	s.numDocsProcessed.AddFilter(stats.RebalancerFilter)
}

func (s *IndexStats) SetIndexStatusFilters() {
	s.buildProgress.AddFilter(stats.IndexStatusFilter)
	s.completionProgress.AddFilter(stats.IndexStatusFilter)
	s.lastScanTime.AddFilter(stats.IndexStatusFilter)
}

func (s *IndexStats) SetGSIClientFilters() {
	s.numDocsPending.AddFilter(stats.GSIClientFilter)
	s.numDocsQueued.AddFilter(stats.GSIClientFilter)
	s.lastRollbackTime.AddFilter(stats.GSIClientFilter)
	s.progressStatTime.AddFilter(stats.GSIClientFilter)
	s.indexState.AddFilter(stats.GSIClientFilter)
}

func (s *IndexStats) SetPlannerFilters() {
	s.itemsCount.AddFilter(stats.PlannerFilter)
	s.buildProgress.AddFilter(stats.PlannerFilter)
	s.residentPercent.AddFilter(stats.PlannerFilter)
	s.dataSize.AddFilter(stats.PlannerFilter)
	s.memUsed.AddFilter(stats.PlannerFilter)
	s.avgDiskBps.AddFilter(stats.PlannerFilter)
	s.avgDrainRate.AddFilter(stats.PlannerFilter)
	s.avgMutationRate.AddFilter(stats.PlannerFilter)
	s.numDocsFlushQueued.AddFilter(stats.PlannerFilter)
	s.avgScanRate.AddFilter(stats.PlannerFilter)
	s.numRowsReturned.AddFilter(stats.PlannerFilter)
}

func (s *IndexStats) getPartitions() []common.PartitionId {

	partitions := make([]common.PartitionId, 0, len(s.partitions))
	for id := range s.partitions {
		partitions = append(partitions, id)
	}
	return partitions
}

func (s *IndexStats) addPartition(id common.PartitionId) {

	if _, ok := s.partitions[id]; !ok {
		partnStats := &IndexStats{isArrayIndex: s.isArrayIndex}
		partnStats.Init()
		s.partitions[id] = partnStats
	}
}

// IndexStats.Clone creates a new copy of the IndexStats object with a new
// partitions map that points to the original stats objects.
func (s *IndexStats) clone() *IndexStats {
	var clone IndexStats = *s // shallow copy

	clone.partitions = make(map[common.PartitionId]*IndexStats)
	for k, v := range s.partitions {
		clone.partitions[k] = v
	}

	return &clone
}

func (s *IndexStats) updateAllPartitionStats(f func(*IndexStats)) {

	for _, ps := range s.partitions {
		f(ps)
	}
}

func (s *IndexStats) updatePartitionStats(pid common.PartitionId, f func(*IndexStats)) {

	if ps, ok := s.partitions[pid]; ok {
		f(ps)
	}
}

func (s *IndexStats) getPartitionStats(pid common.PartitionId) *IndexStats {

	return s.partitions[pid]
}

func (s *IndexStats) partnMaxInt64Stats(f func(*IndexStats) int64) int64 {

	var v int64
	for _, ps := range s.partitions {
		pv := f(ps)
		if pv > v {
			v = pv
		}
	}
	return v
}

func (s *IndexStats) partnInt64Stats(f func(*IndexStats) int64) int64 {

	var v int64
	for _, ps := range s.partitions {
		v += f(ps)
	}

	if v != 0 {
		return v
	}

	return f(s)
}

func (s *IndexStats) partnAvgInt64Stats(f func(*IndexStats) int64) int64 {

	return s.int64Stats(f)
}

func (s *IndexStats) int64Stats(f func(*IndexStats) int64) int64 {

	var v, count int64
	for _, ps := range s.partitions {
		if psv := f(ps); psv != 0 {
			v += psv
			count++
		}
	}

	if v != 0 {
		return (v / count)
	}

	return f(s)
}

func (s *IndexStats) partnTimingStats(f func(*IndexStats) *stats.TimingStat) string {

	var v stats.TimingStat
	v.Init()
	for _, ps := range s.partitions {
		if x := f(ps); x != nil {
			v.Count.Add(x.Count.Value())
			v.Sum.Add(x.Sum.Value())
			v.SumOfSq.Add(x.SumOfSq.Value())
		}
	}

	if v.Count.Value() != 0 {
		return v.Value()
	}

	return f(s).Value()
}

type IndexerStats struct {
	// indexes is a map of index IDs to per-index stats. Never nil.
	indexes map[common.IndexInstId]*IndexStats

	// keyspaceStatsMap wraps per-keyspace stats. Never nil.
	keyspaceStatsMap KeyspaceStatsMapHolder

	numConnections     stats.Int64Val
	memoryQuota        stats.Int64Val
	memoryUsed         stats.Int64Val
	memoryUsedStorage  stats.Int64Val
	memoryTotalStorage stats.Int64Val
	memoryUsedQueue    stats.Int64Val
	needsRestart       stats.BoolVal
	statsResponse      stats.TimingStat
	notFoundError      stats.Int64Val

	indexerState  stats.Int64Val
	prjLatencyMap *LatencyMapHolder
	nodeToHostMap *NodeToHostMapHolder

	timestamp      stats.StringVal
	uptime         stats.StringVal
	storageMode    stats.StringVal
	numCPU         stats.Int64Val
	cpuUtilization stats.Int64Val
	memoryRss      stats.Uint64Val
	memoryFree     stats.Uint64Val
	memoryTotal    stats.Uint64Val
	pauseTotalNs   stats.Uint64Val

	numIndexes          stats.Int64Val
	numStorageInstances stats.Int64Val
	avgResidentPercent  stats.Int64Val
	avgMutationRate     stats.Int64Val
	avgDrainRate        stats.Int64Val
	avgDiskBps          stats.Int64Val
	totalDataSize       stats.Int64Val
	totalDiskSize       stats.Int64Val

	indexerStateHolder stats.StringVal
}

func (s *IndexerStats) Init() {
	s.indexes = make(map[common.IndexInstId]*IndexStats)
	s.keyspaceStatsMap.Init()
	s.numConnections.Init()
	s.memoryQuota.Init()
	s.memoryUsed.Init()
	s.memoryUsedStorage.Init()
	s.memoryTotalStorage.Init()
	s.memoryUsedQueue.Init()
	s.needsRestart.Init()
	s.statsResponse.Init()
	s.indexerState.Init()
	s.notFoundError.Init()
	s.prjLatencyMap = &LatencyMapHolder{}
	s.prjLatencyMap.Init()

	s.nodeToHostMap = &NodeToHostMapHolder{}
	s.nodeToHostMap.Init()

	s.timestamp.Init()
	s.uptime.Init()
	s.storageMode.Init()
	s.numCPU.Init()
	s.cpuUtilization.Init()
	s.memoryRss.Init()
	s.memoryFree.Init()
	s.memoryTotal.Init()
	s.indexerStateHolder.Init()
	s.pauseTotalNs.Init()

	s.numIndexes.Init()
	s.numStorageInstances.Init()
	s.avgResidentPercent.Init()
	s.avgMutationRate.Init()
	s.avgDrainRate.Init()
	s.avgDiskBps.Init()
	s.totalDataSize.Init()
	s.totalDiskSize.Init()

	s.SetPlannerFilters()
	s.SetRebalanceFilters()
	s.SetIndexStatusFilters()
	s.SetSummaryFilters()

	// Set values of invarients on Init.
	s.numCPU.Set(int64(num_cpu_core))
}

func (s *IndexerStats) SetRebalanceFilters() {
	s.indexerStateHolder.AddFilter(stats.RebalancerFilter)
}

func (s *IndexerStats) SetIndexStatusFilters() {
	s.indexerStateHolder.AddFilter(stats.IndexStatusFilter)
}

func (s *IndexerStats) SetPlannerFilters() {
	s.memoryUsedStorage.AddFilter(stats.PlannerFilter)
	s.memoryUsed.AddFilter(stats.PlannerFilter)
	s.memoryQuota.AddFilter(stats.PlannerFilter)
	s.uptime.AddFilter(stats.PlannerFilter)
	s.cpuUtilization.AddFilter(stats.PlannerFilter)
}

func (s *IndexerStats) SetSummaryFilters() {

	s.memoryQuota.AddFilter(stats.SummaryFilter)
	s.memoryUsed.AddFilter(stats.SummaryFilter)
	s.memoryUsedStorage.AddFilter(stats.SummaryFilter)
	s.memoryTotalStorage.AddFilter(stats.SummaryFilter)
	s.memoryUsedQueue.AddFilter(stats.SummaryFilter)
	s.memoryRss.AddFilter(stats.SummaryFilter)

	s.numCPU.AddFilter(stats.SummaryFilter)
	s.cpuUtilization.AddFilter(stats.SummaryFilter)
	s.avgResidentPercent.AddFilter(stats.SummaryFilter)
	s.avgMutationRate.AddFilter(stats.SummaryFilter)
	s.avgDrainRate.AddFilter(stats.SummaryFilter)
	s.avgDiskBps.AddFilter(stats.SummaryFilter)
	s.totalDataSize.AddFilter(stats.SummaryFilter)
	s.totalDiskSize.AddFilter(stats.SummaryFilter)
	s.numStorageInstances.AddFilter(stats.SummaryFilter)
	s.numIndexes.AddFilter(stats.SummaryFilter)

	s.storageMode.AddFilter(stats.SummaryFilter)
	s.indexerStateHolder.AddFilter(stats.SummaryFilter)
	s.uptime.AddFilter(stats.SummaryFilter)

}

// Reset recreates empty IndexStats and KeyspaceStats for each one that existed
// before. This approach avoids resetting structured data types inside the old
// objects while other routines are accessing them concurrently. This routine is
// only called by a stats REST API that few currently use.
func (s *IndexerStats) Reset() {
	old := *s
	*s = IndexerStats{} // overwrite old self pointer
	s.Init()

	// Recreate per-index objects
	for instId, iStats := range old.indexes {
		indexStats := s.addIndexStats(instId, iStats.bucket, iStats.scope, iStats.collection, iStats.name,
			iStats.replicaId, iStats.isArrayIndex)

		// Recreate per-partition subobjects
		for partnId := range iStats.partitions {
			indexStats.addPartition(partnId)
		}
	}

	// Recreate KeyspaceStats objects
	for streamId, ksStats := range old.GetKeyspaceStatsMap() {
		for keyspaceId := range ksStats {
			s.AddKeyspaceStats(streamId, keyspaceId)
		}
	}
}

// AddKeyspaceStats adds or reinitializes an entry to the per-keyspace stats map
// with populated metadata but empty stats values.
func (s *IndexerStats) AddKeyspaceStats(streamId common.StreamId, keyspaceId string) {
	_, ok := s.GetKeyspaceStatsMap()[streamId][keyspaceId]
	if !ok {
		ksStats := &KeyspaceStats{}
		ksStats.Init(keyspaceId)
		s.GetKeyspaceStatsMap()[streamId][keyspaceId] = ksStats
	}
}

// RemoveKeyspaceStats deletes one entry from the per-keyspace stats map.
// NO-OP if entry did not exist.
func (s *IndexerStats) RemoveKeyspaceStats(streamId common.StreamId, keyspaceId string) {
	delete(s.GetKeyspaceStatsMap()[streamId], keyspaceId)
}

// addIndexStats adds or reinitializes an entry to the per-index
// stats map with populated metadata but empty stats values.
func (s *IndexerStats) addIndexStats(instId common.IndexInstId,
	bucket string, scope string, collection string, name string,
	replicaId int, isArrIndex bool) *IndexStats {

	idxStats, ok := s.indexes[instId]
	if !ok {
		idxStats = &IndexStats{
			name:         name,
			bucket:       bucket,
			scope:        scope,
			collection:   collection,
			replicaId:    replicaId,
			isArrayIndex: isArrIndex,
		}
		idxStats.Init()
		s.indexes[instId] = idxStats
	}
	return idxStats
}

// AddPartitionStats adds stats to the per-index stats map.
func (s *IndexerStats) AddPartitionStats(indexInst common.IndexInst, partitionId common.PartitionId) {
	instId := indexInst.InstId
	defn := indexInst.Defn

	if _, ok := s.indexes[instId]; !ok {
		s.addIndexStats(instId, defn.Bucket, defn.Scope, defn.Collection, defn.Name,
			indexInst.ReplicaId, defn.IsArrayIndex)
	}
	s.indexes[instId].addPartition(partitionId)
}

func (s *IndexerStats) GetPartitionStats(id common.IndexInstId, partnId common.PartitionId) *IndexStats {

	if is, ok := s.indexes[id]; ok {
		return is.partitions[partnId]
	}

	return nil
}

func (s *IndexerStats) SetPartitionStats(id common.IndexInstId, partnId common.PartitionId, stats *IndexStats) {

	if is, ok := s.indexes[id]; ok {
		is.partitions[partnId] = stats
	}
}

func (s *IndexerStats) RemovePartitionStats(id common.IndexInstId, partnId common.PartitionId) {

	if is, ok := s.indexes[id]; ok {
		delete(is.partitions, partnId)
	}
}

// RemoveIndexStats removes stats from the per-index stats map.
func (s *IndexerStats) RemoveIndexStats(indexInst common.IndexInst) {
	instId := indexInst.InstId
	_, ok := s.indexes[instId]
	if !ok {
		return
	}
	delete(s.indexes, instId)
}

func (s *IndexStats) getKeySizeStats() map[string]interface{} {

	keySizeStats := make(map[string]interface{})
	keySizeStats["(0-64)"] = s.partnInt64Stats(func(ss *IndexStats) int64 {
		return ss.numKeySize64.Value()
	})
	keySizeStats["(65-256)"] = s.partnInt64Stats(func(ss *IndexStats) int64 {
		return ss.numKeySize256.Value()
	})
	keySizeStats["(257-1024)"] = s.partnInt64Stats(func(ss *IndexStats) int64 {
		return ss.numKeySize1K.Value()
	})
	keySizeStats["(1025-4096)"] = s.partnInt64Stats(func(ss *IndexStats) int64 {
		return ss.numKeySize4K.Value()
	})
	keySizeStats["(4097-102400)"] = s.partnInt64Stats(func(ss *IndexStats) int64 {
		return ss.numKeySize100K.Value()
	})
	keySizeStats["(102401-max)"] = s.partnInt64Stats(func(ss *IndexStats) int64 {
		return ss.numKeySizeGt100K.Value()
	})
	return keySizeStats
}

// arrkey_size_distribution is applicable only for plasma array index
func (s *IndexStats) getArrKeySizeStats() map[string]interface{} {

	keySizeStats := make(map[string]interface{})
	keySizeStats["(0-64)"] = s.partnInt64Stats(func(ss *IndexStats) int64 {
		return ss.numArrayKeySize64.Value()
	})
	keySizeStats["(65-256)"] = s.partnInt64Stats(func(ss *IndexStats) int64 {
		return ss.numArrayKeySize256.Value()
	})
	keySizeStats["(257-1024)"] = s.partnInt64Stats(func(ss *IndexStats) int64 {
		return ss.numArrayKeySize1K.Value()
	})
	keySizeStats["(1025-4096)"] = s.partnInt64Stats(func(ss *IndexStats) int64 {
		return ss.numArrayKeySize4K.Value()
	})
	keySizeStats["(4097-102400)"] = s.partnInt64Stats(func(ss *IndexStats) int64 {
		return ss.numArrayKeySize100K.Value()
	})
	keySizeStats["(102401-max)"] = s.partnInt64Stats(func(ss *IndexStats) int64 {
		return ss.numArrayKeySizeGt100K.Value()
	})

	return keySizeStats
}

func (is *IndexerStats) PopulateIndexerStats(statMap *StatsMap) {
	statMap.AddStatValueFiltered("num_connections", &is.numConnections)
	statMap.AddStatValueFiltered("index_not_found_errcount", &is.notFoundError)
	statMap.AddStatValueFiltered("memory_quota", &is.memoryQuota)
	statMap.AddStatValueFiltered("memory_used", &is.memoryUsed)
	statMap.AddStatValueFiltered("memory_used_storage", &is.memoryUsedStorage)
	statMap.AddStatValueFiltered("memory_total_storage", &is.memoryTotalStorage)
	statMap.AddStatValueFiltered("memory_used_queue", &is.memoryUsedQueue)
	statMap.AddStatValueFiltered("needs_restart", &is.needsRestart)
	statMap.AddStatValueFiltered("num_cpu_core", &is.numCPU)
	statMap.AddStatValueFiltered("avg_resident_percent", &is.avgResidentPercent)
	statMap.AddStatValueFiltered("avg_mutation_rate", &is.avgMutationRate)
	statMap.AddStatValueFiltered("avg_drain_rate", &is.avgDrainRate)
	statMap.AddStatValueFiltered("avg_disk_bps", &is.avgDiskBps)
	statMap.AddStatValueFiltered("total_data_size", &is.totalDataSize)
	statMap.AddStatValueFiltered("total_disk_size", &is.totalDiskSize)
	statMap.AddStatValueFiltered("num_storage_instances", &is.numStorageInstances)
	statMap.AddStatValueFiltered("num_indexes", &is.numIndexes)

	strts := fmt.Sprintf("%v", time.Now().UnixNano())
	is.timestamp.Set(&strts)
	statMap.AddStatValueFiltered("timestamp", &is.timestamp)

	strut := fmt.Sprintf("%s", time.Since(uptime))
	is.uptime.Set(&strut)
	statMap.AddStatValueFiltered("uptime", &is.uptime)

	strmode := fmt.Sprintf("%s", common.GetStorageMode())
	is.storageMode.Set(&strmode)
	statMap.AddStatValueFiltered("storage_mode", &is.storageMode)

	is.cpuUtilization.Set(int64(math.Float64bits(getCpuPercent())))
	statMap.AddFloat64StatFiltered("cpu_utilization", &is.cpuUtilization)

	is.memoryRss.Set(getRSS())
	statMap.AddStatValueFiltered("memory_rss", &is.memoryRss)

	is.memoryFree.Set(getMemFree())
	statMap.AddStatValueFiltered("memory_free", &is.memoryFree)

	is.memoryTotal.Set(getMemTotal())
	statMap.AddStatValueFiltered("memory_total", &is.memoryTotal)

	indexerState := common.IndexerState(is.indexerState.Value())
	if indexerState == common.INDEXER_PREPARE_UNPAUSE {
		indexerState = common.INDEXER_PAUSED
	}
	strst := fmt.Sprintf("%s", indexerState)
	is.indexerStateHolder.Set(&strst)
	statMap.AddStatValueFiltered("indexer_state", &is.indexerStateHolder)

	statMap.AddStatValueFiltered("timings/stats_response", &is.statsResponse)
}

func (is *IndexerStats) PopulateProjectorLatencyStats(statMap *StatsMap) {
	prjLatencyMap := is.prjLatencyMap.Get()
	nodeToHostMap := is.nodeToHostMap.Get()
	for prjAddr, prjLatency := range prjLatencyMap {
		latency := prjLatency.(*stats.Int64Val)
		// Get NodeUUID from prjAddr
		prjAddrSplit := strings.Split(prjAddr, "/")
		stream := prjAddrSplit[0]
		nodeUUID := prjAddrSplit[1]
		if hostname, ok := nodeToHostMap[nodeUUID]; ok {
			newPrjAddr := fmt.Sprintf("%v/%v", stream, hostname)
			statMap.AddStatValueFiltered(newPrjAddr+"/projector_latency", latency)
		}
	}
}

func (is *IndexerStats) GetStats(spec *statsSpec) interface{} {
	var prefix string
	var instId string

	statMap := NewStatsMap(spec)

	is.PopulateIndexerStats(statMap)

	is.PopulateProjectorLatencyStats(statMap)

	addStatsForIndexInst := func(inst common.IndexInstId, s *IndexStats) {
		var ok bool

		if s == nil {
			if s, ok = is.indexes[inst]; !ok {
				logging.Errorf("Error in GetStats: stats for instId %v not found", inst)
				return
			}
		}

		prefix = common.GetStatsPrefix(s.bucket, s.scope, s.collection, s.name,
			s.replicaId, 0, false)
		// prefix = fmt.Sprintf("%s:%s:", s.bucket, name)
		statMap.SetPrefix(prefix)
		instId = fmt.Sprintf("%v:", inst)
		statMap.SetInstId(instId)
		s.addIndexStatsToMap(statMap, spec)

		if spec.partition {

			for partnId, ps := range s.partitions {
				prefix = common.GetStatsPrefix(s.bucket, s.scope, s.collection,
					s.name, s.replicaId, int(partnId), true)
				// prefix = fmt.Sprintf("%s:%s:", s.bucket, name)
				statMap.SetPrefix(prefix)
				ps.addIndexStatsToMap(statMap, spec)
			}
		}
	}

	var instances []common.IndexInstId
	if spec.indexSpec != nil {
		instances = spec.indexSpec.GetInstances()
	}

	if instances == nil {
		for k, s := range is.indexes {
			addStatsForIndexInst(k, s)
		}
	} else {
		for _, inst := range instances {
			addStatsForIndexInst(inst, nil)
		}
	}

	for streamId, ksStats := range is.GetKeyspaceStatsMap() {
		for keyspaceId, ks := range ksStats {
			prefix = fmt.Sprintf("%s:%s:", streamId, keyspaceId)
			statMap.SetPrefix(prefix)
			ks.addKeyspaceStatsToStatsMap(statMap)
		}
	}

	if spec.marshalToByteSlice {
		// Replace last "," with "}"
		if len(statMap.byteSlice) > 1 {
			statMap.byteSlice[len(statMap.byteSlice)-1] = byte('}')
		}
		return statMap.byteSlice
	} else {
		return statMap.GetMap()
	}
}

func (is *IndexerStats) GetVersionedStats(t *target) (common.Statistics, bool) {
	statsMap := make(map[string]interface{})

	var found bool

	addToStatsMap := func(s *IndexStats) {
		prefix := common.GetStatsPrefix(s.bucket, s.scope, s.collection,
			s.name, s.replicaId, 0, false)
		key := prefix[:len(prefix)-1]

		statsMap[key] = s.constructIndexStats(t.skipEmpty, t.version)
		if t.partition {
			for partnId, ps := range s.partitions {
				key = fmt.Sprintf("Partition-%d", int(partnId))
				statsMap[key] = ps.constructIndexStats(t.skipEmpty, t.version)
			}
		}
		found = true
	}

	if t.level == "indexer" {
		querySystemCatalog, _ := t.creds.IsAllowed("cluster.n1ql.meta!read")
		if querySystemCatalog {
			statsMap["indexer"] = is.constructIndexerStats(t.skipEmpty, t.version)
		}
		permissionCache := common.NewSessionPermissionsCache(t.creds)
		for _, s := range is.indexes {
			if querySystemCatalog || permissionCache.IsAllowed(s.bucket, s.scope, s.collection, "list") {
				addToStatsMap(s)
			}
		}
		found = true
	} else if t.level == "bucket" {
		for _, s := range is.indexes {
			if s.bucket == t.bucket {
				addToStatsMap(s)
			}
		}
	} else if t.level == "scope" {
		for _, s := range is.indexes {
			if s.bucket == t.bucket &&
				s.scope == t.scope {
				addToStatsMap(s)
			}
		}
	} else if t.level == "collection" {
		for _, s := range is.indexes {
			if s.bucket == t.bucket &&
				s.scope == t.scope &&
				s.collection == t.collection {
				addToStatsMap(s)
			}
		}
	} else if t.level == "index" {
		for _, s := range is.indexes {
			if s.name == t.index &&
				s.bucket == t.bucket &&
				s.scope == t.scope &&
				s.collection == t.collection {
				addToStatsMap(s)
			}
		}
	}
	return statsMap, found
}

func computeAvgItemSize(raw_data_size, items_count int64) int64 {
	if items_count > 0 {
		return raw_data_size / items_count
	}
	// Return 0 if no items indexed
	return 0
}

func addStatFactory(skipEmpty bool, statsMap common.Statistics) func(string, interface{}) {
	return func(k string, v interface{}) {
		if !skipEmpty {
			statsMap[fmt.Sprintf("%s", k)] = v
		} else if n, ok := v.(int64); ok && n != 0 {
			statsMap[fmt.Sprintf("%s", k)] = v
		} else if s, ok := v.(string); ok && len(s) != 0 && s != "0 0 0" && s != "0" {
			statsMap[fmt.Sprintf("%s", k)] = v
		}
	}
}

func computeAvgArrayLength(itemsCount, docidCount int64) int64 {
	if docidCount > 0 {
		return itemsCount / docidCount
	}
	// Return 0 if no items indexed
	return 0
}

func (is *IndexerStats) constructIndexerStats(skipEmpty bool, version string) common.Statistics {
	indexerStats := make(map[string]interface{})
	addStat := addStatFactory(skipEmpty, indexerStats)

	switch version {
	case "v1":
		addStat("memory_quota", is.memoryQuota.Value())
		addStat("memory_used", is.memoryUsed.Value())
		addStat("memory_total_storage", is.memoryTotalStorage.Value())
		addStat("total_indexer_gc_pause_ns", is.pauseTotalNs.Value())

		indexerState := common.IndexerState(is.indexerState.Value())
		if indexerState == common.INDEXER_PREPARE_UNPAUSE {
			indexerState = common.INDEXER_PAUSED
		}
		addStat("indexer_state", fmt.Sprintf("%s", indexerState))
	}

	return indexerStats
}

func (s *IndexStats) constructIndexStats(skipEmpty bool, version string) common.Statistics {
	s.initializeScanStats()

	indexStats := make(map[string]interface{})
	addStat := addStatFactory(skipEmpty, indexStats)

	reqs := s.int64Stats(func(ss *IndexStats) int64 {
		return ss.numRequests.Value()
	})
	pendingReqs := reqs - s.numCompletedRequests.Value()

	itemsCount := s.partnInt64Stats(func(ss *IndexStats) int64 {
		return ss.itemsCount.Value()
	})

	rawDataSize := s.partnInt64Stats(func(ss *IndexStats) int64 {
		return ss.rawDataSize.Value()
	})

	addStat("total_scan_duration",
		s.int64Stats(func(ss *IndexStats) int64 {
			return ss.scanDuration.Value()
		}))
	addStat("num_docs_pending",
		s.int64Stats(func(ss *IndexStats) int64 {
			return ss.numDocsPending.Value()
		}))
	// partition stats
	addStat("num_docs_indexed",
		s.partnInt64Stats(func(ss *IndexStats) int64 {
			return ss.numDocsIndexed.Value()
		}))
	addStat("num_requests", reqs)
	addStat("num_pending_requests", pendingReqs)

	addStat("num_rows_returned",
		s.int64Stats(func(ss *IndexStats) int64 {
			return ss.numRowsReturned.Value()
		}))

	// partition stats
	addStat("memory_used",
		s.partnInt64Stats(func(ss *IndexStats) int64 {
			return ss.memUsed.Value()
		}))

	// partition stats
	addStat("disk_size",
		s.partnInt64Stats(func(ss *IndexStats) int64 {
			return ss.diskSize.Value()
		}))
	addStat("num_docs_queued",
		s.int64Stats(func(ss *IndexStats) int64 {
			return ss.numDocsQueued.Value()
		}))
	// partition stats
	addStat("data_size",
		s.partnInt64Stats(func(ss *IndexStats) int64 {
			return ss.dataSize.Value()
		}))

	addStat("avg_item_size", computeAvgItemSize(rawDataSize, itemsCount))
	// partition stats
	addStat("frag_percent",
		s.partnAvgInt64Stats(func(ss *IndexStats) int64 {
			return ss.fragPercent.Value()
		}))
	addStat("scan_bytes_read",
		s.int64Stats(func(ss *IndexStats) int64 {
			return ss.scanBytesRead.Value()
		}))
	// partition stats
	addStat("items_count", itemsCount)

	if s.isArrayIndex {
		docidCount := s.partnInt64Stats(func(ss *IndexStats) int64 {
			return ss.docidCount.Value()
		})

		// partition stats
		addStat("docid_count", docidCount)
		addStat("avg_array_length", computeAvgArrayLength(itemsCount, docidCount))
	}

	// partition stats
	addStat("resident_percent",
		s.partnAvgInt64Stats(func(ss *IndexStats) int64 {
			return ss.residentPercent.Value()
		}))
	// partition stats
	addStat("cache_hit_percent",
		s.partnAvgInt64Stats(func(ss *IndexStats) int64 {
			return ss.cacheHitPercent.Value()
		}))
	// partition stats
	addStat("cache_hits",
		s.partnInt64Stats(func(ss *IndexStats) int64 {
			return ss.cacheHits.Value()
		}))
	// partition stats
	addStat("cache_misses",
		s.partnInt64Stats(func(ss *IndexStats) int64 {
			return ss.cacheMisses.Value()
		}))
	// partition stats
	addStat("recs_in_mem",
		s.partnInt64Stats(func(ss *IndexStats) int64 {
			return ss.numRecsInMem.Value()
		}))
	// partition stats
	addStat("recs_on_disk",
		s.partnInt64Stats(func(ss *IndexStats) int64 {
			return ss.numRecsOnDisk.Value()
		}))
	addStat("num_items_flushed",
		s.partnInt64Stats(func(ss *IndexStats) int64 {
			return ss.numItemsFlushed.Value()
		}))

	// last_known_scan_time stat name because exact scan time cant be
	// known if indexer restarts within statsPersistenceInterval
	addStat("last_known_scan_time", s.lastScanTime.Value())

	addStat("avg_scan_latency", s.avgScanLatency.Value())

	addStat("initial_build_progress",
		s.int64Stats(func(ss *IndexStats) int64 {
			return ss.buildProgress.Value()
		}))
	addStat("avg_drain_rate",
		s.partnInt64Stats(func(ss *IndexStats) int64 {
			return ss.avgDrainRate.Value()
		}))
	addStat("num_scan_timeouts",
		s.int64Stats(func(ss *IndexStats) int64 {
			return ss.numScanTimeouts.Value()
		}))
	addStat("num_scan_errors",
		s.int64Stats(func(ss *IndexStats) int64 {
			return ss.numScanErrors.Value()
		}))

	return indexStats
}

func (s *IndexStats) initializeScanStats() {
	var scanLat, waitLat, scanReqLat, scanReqInitLat, scanReqAllocLat int64
	reqs := s.numRequests.Value()

	if reqs > 0 {
		scanDur := s.int64Stats(func(ss *IndexStats) int64 { return ss.scanDuration.Value() })
		waitDur := s.int64Stats(func(ss *IndexStats) int64 { return ss.scanWaitDuration.Value() })
		scanReqDur := s.int64Stats(func(ss *IndexStats) int64 { return ss.scanReqDuration.Value() })
		scanReqInitDur := s.int64Stats(func(ss *IndexStats) int64 { return ss.scanReqInitDuration.Value() })
		scanReqAllocDur := s.int64Stats(func(ss *IndexStats) int64 { return ss.scanReqAllocDuration.Value() })

		reqsSince := reqs - s.lastNumRequests.Value()
		if reqsSince > 0 {
			currScanLat := (scanDur - s.lastScanDuration.Value()) / reqsSince
			scanLat = (currScanLat + s.avgScanLatency.Value()) / 2
			s.lastScanDuration.Set(scanDur)
			s.lastNumRequests.Set(reqs)
			s.avgScanLatency.Set(scanLat)
		}

		waitLat = waitDur / reqs
		scanReqLat = scanReqDur / reqs
		scanReqInitLat = scanReqInitDur / reqs
		scanReqAllocLat = scanReqAllocDur / reqs
	}

	s.waitLat.Set(waitLat)
	s.scanReqLat.Set(scanReqLat)
	s.scanReqInitLat.Set(scanReqInitLat)
	s.scanReqAllocLat.Set(scanReqAllocLat)
}

func (s *IndexStats) addIndexStatsToMap(statMap *StatsMap, spec *statsSpec) {
	s.initializeScanStats()

	statMap.AddStatValueFiltered("index_state", &s.indexState)

	// ----------------------
	// All int64Stats
	// ----------------------
	statMap.AddAggrStatFiltered("total_scan_duration",
		func(ss *IndexStats) int64 {
			return ss.scanDuration.Value()
		},
		&s.scanDuration, s.int64Stats)

	statMap.AddAggrStatFiltered("total_scan_request_duration",
		func(ss *IndexStats) int64 {
			return ss.scanReqDuration.Value()
		},
		&s.scanReqDuration, s.int64Stats)

	statMap.AddAggrStatFiltered("num_docs_pending",
		func(ss *IndexStats) int64 {
			return ss.numDocsPending.Value()
		},
		&s.numDocsPending, s.int64Stats)

	statMap.AddAggrStatFiltered("scan_wait_duration",
		func(ss *IndexStats) int64 {
			return ss.scanWaitDuration.Value()
		},
		&s.scanWaitDuration, s.int64Stats)

	statMap.AddAggrStatFiltered("num_docs_processed",
		func(ss *IndexStats) int64 {
			return ss.numDocsProcessed.Value()
		},
		&s.numDocsProcessed, s.int64Stats)

	statMap.AddAggrStatFiltered("num_rows_returned",
		func(ss *IndexStats) int64 {
			return ss.numRowsReturned.Value()
		},
		&s.numRowsReturned, s.int64Stats)

	statMap.AddAggrStatFiltered("build_progress",
		func(ss *IndexStats) int64 {
			return ss.buildProgress.Value()
		},
		&s.buildProgress, s.int64Stats)

	statMap.AddAggrStatFiltered("num_docs_queued",
		func(ss *IndexStats) int64 {
			return ss.numDocsQueued.Value()
		},
		&s.numDocsQueued, s.int64Stats)

	statMap.AddAggrStatFiltered("scan_bytes_read",
		func(ss *IndexStats) int64 {
			return ss.scanBytesRead.Value()
		},
		&s.scanBytesRead, s.int64Stats)

	statMap.AddAggrStatFiltered("avg_ts_interval",
		func(ss *IndexStats) int64 {
			return ss.avgTsInterval.Value()
		},
		&s.avgTsInterval, s.int64Stats)

	statMap.AddAggrStatFiltered("avg_ts_items_count",
		func(ss *IndexStats) int64 {
			return ss.avgTsItemsCount.Value()
		},
		&s.avgTsItemsCount, s.int64Stats)

	statMap.AddAggrStatFiltered("num_commits",
		func(ss *IndexStats) int64 {
			return ss.numCommits.Value()
		},
		&s.numCommits, s.int64Stats)

	statMap.AddAggrStatFiltered("num_snapshots",
		func(ss *IndexStats) int64 {
			return ss.numSnapshots.Value()
		},
		&s.numSnapshots, s.int64Stats)

	statMap.AddAggrStatFiltered("num_open_snapshots",
		func(ss *IndexStats) int64 {
			return ss.numOpenSnapshots.Value()
		},
		&s.numOpenSnapshots, s.int64Stats)

	statMap.AddAggrStatFiltered("num_compactions",
		func(ss *IndexStats) int64 {
			return ss.numCompactions.Value()
		},
		&s.numCompactions, s.int64Stats)

	// TODO: Does it need to be int64Stat?
	statMap.AddAggrStatFiltered("since_last_snapshot",
		func(ss *IndexStats) int64 {
			return ss.sinceLastSnapshot.Value()
		},
		&s.sinceLastSnapshot, s.int64Stats)

	statMap.AddAggrStatFiltered("num_snapshot_waiters",
		func(ss *IndexStats) int64 {
			return ss.numSnapshotWaiters.Value()
		},
		&s.numSnapshotWaiters, s.int64Stats)

	statMap.AddAggrStatFiltered("num_last_snapshot_reply",
		func(ss *IndexStats) int64 {
			return ss.numLastSnapshotReply.Value()
		},
		&s.numLastSnapshotReply, s.int64Stats)

	statMap.AddAggrStatFiltered("not_ready_errcount",
		func(ss *IndexStats) int64 {
			return ss.notReadyError.Value()
		},
		&s.notReadyError, s.int64Stats)

	statMap.AddAggrStatFiltered("client_cancel_errcount",
		func(ss *IndexStats) int64 {
			return ss.clientCancelError.Value()
		},
		&s.clientCancelError, s.int64Stats)

	statMap.AddAggrStatFiltered("num_scan_timeouts",
		func(ss *IndexStats) int64 {
			return ss.numScanTimeouts.Value()
		},
		&s.numScanTimeouts, s.int64Stats)

	statMap.AddAggrStatFiltered("num_scan_errors",
		func(ss *IndexStats) int64 {
			return ss.numScanErrors.Value()
		},
		&s.numScanErrors, s.int64Stats)

	// ----------------------
	// All partnInt64Stats
	// ----------------------
	statMap.AddAggrStatFiltered("insert_bytes",
		func(ss *IndexStats) int64 {
			return ss.insertBytes.Value()
		},
		&s.insertBytes, s.partnInt64Stats)

	statMap.AddAggrStatFiltered("num_docs_indexed",
		func(ss *IndexStats) int64 {
			return ss.numDocsIndexed.Value()
		},
		&s.numDocsIndexed, s.partnInt64Stats)

	statMap.AddAggrStatFiltered("num_rows_scanned",
		func(ss *IndexStats) int64 {
			return ss.numRowsScanned.Value()
		},
		&s.numRowsScanned, s.partnInt64Stats)

	statMap.AddAggrStatFiltered("disk_size",
		func(ss *IndexStats) int64 {
			return ss.diskSize.Value()
		},
		&s.diskSize, s.partnInt64Stats)

	statMap.AddAggrStatFiltered("memory_used",
		func(ss *IndexStats) int64 {
			return ss.memUsed.Value()
		},
		&s.memUsed, s.partnInt64Stats)

	statMap.AddAggrStatFiltered("delete_bytes",
		func(ss *IndexStats) int64 {
			return ss.deleteBytes.Value()
		},
		&s.deleteBytes, s.partnInt64Stats)

	statMap.AddAggrStatFiltered("data_size",
		func(ss *IndexStats) int64 {
			return ss.dataSize.Value()
		},
		&s.dataSize, s.partnInt64Stats)

	statMap.AddAggrStatFiltered("data_size_on_disk",
		func(ss *IndexStats) int64 {
			return ss.dataSizeOnDisk.Value()
		},
		&s.dataSizeOnDisk, s.partnInt64Stats)

	statMap.AddAggrStatFiltered("log_space_on_disk",
		func(ss *IndexStats) int64 {
			return ss.logSpaceOnDisk.Value()
		},
		&s.logSpaceOnDisk, s.partnInt64Stats)

	statMap.AddAggrStatFiltered("raw_data_size",
		func(ss *IndexStats) int64 {
			return ss.rawDataSize.Value()
		},
		&s.rawDataSize, s.partnInt64Stats)

	statMap.AddAggrStatFiltered("backstore_raw_data_size",
		func(ss *IndexStats) int64 {
			return ss.backstoreRawDataSize.Value()
		},
		&s.backstoreRawDataSize, s.partnInt64Stats)

	statMap.AddAggrStatFiltered("get_bytes",
		func(ss *IndexStats) int64 {
			return ss.getBytes.Value()
		},
		&s.getBytes, s.partnInt64Stats)

	statMap.AddAggrStatFiltered("items_count",
		func(ss *IndexStats) int64 {
			return ss.itemsCount.Value()
		},
		&s.itemsCount, s.partnInt64Stats)

	statMap.AddAggrStatFiltered("num_items_flushed",
		func(ss *IndexStats) int64 {
			return ss.numItemsFlushed.Value()
		},
		&s.numItemsFlushed, s.partnInt64Stats)

	statMap.AddAggrStatFiltered("num_flush_queued",
		func(ss *IndexStats) int64 {
			return ss.numDocsFlushQueued.Value()
		},
		&s.numDocsFlushQueued, s.partnInt64Stats)

	statMap.AddAggrStatFiltered("num_items_restored",
		func(ss *IndexStats) int64 {
			return ss.numItemsRestored.Value()
		},
		&s.numItemsRestored, s.partnInt64Stats)

	statMap.AddAggrStatFiltered("avg_scan_rate",
		func(ss *IndexStats) int64 {
			return ss.avgScanRate.Value()
		},
		&s.avgScanRate, s.partnInt64Stats)

	statMap.AddAggrStatFiltered("avg_mutation_rate",
		func(ss *IndexStats) int64 {
			return ss.avgMutationRate.Value()
		},
		&s.avgMutationRate, s.partnInt64Stats)

	statMap.AddAggrStatFiltered("avg_drain_rate",
		func(ss *IndexStats) int64 {
			return ss.avgDrainRate.Value()
		},
		&s.avgDrainRate, s.partnInt64Stats)

	statMap.AddAggrStatFiltered("avg_disk_bps",
		func(ss *IndexStats) int64 {
			return ss.avgDiskBps.Value()
		},
		&s.avgDiskBps, s.partnInt64Stats)

	statMap.AddAggrStatFiltered("cache_hits",
		func(ss *IndexStats) int64 {
			return ss.cacheHits.Value()
		},
		&s.cacheHits, s.partnInt64Stats)

	statMap.AddAggrStatFiltered("cache_misses",
		func(ss *IndexStats) int64 {
			return ss.cacheMisses.Value()
		},
		&s.cacheMisses, s.partnInt64Stats)

	statMap.AddAggrStatFiltered("recs_in_mem",
		func(ss *IndexStats) int64 {
			return ss.numRecsInMem.Value()
		},
		&s.numRecsInMem, s.partnInt64Stats)

	statMap.AddAggrStatFiltered("recs_on_disk",
		func(ss *IndexStats) int64 {
			return ss.numRecsOnDisk.Value()
		},
		&s.numRecsOnDisk, s.partnInt64Stats)

	// -------------------------------
	// All partition and index stats
	// -------------------------------
	statMap.AddStatValueFiltered("num_requests", &s.numRequests)
	statMap.AddStatValueFiltered("last_known_scan_time", &s.lastScanTime)
	statMap.AddStatValueFiltered("num_completed_requests", &s.numCompletedRequests)
	statMap.AddStatValueFiltered("last_rollback_time", &s.lastRollbackTime)
	statMap.AddStatValueFiltered("progress_stat_time", &s.progressStatTime)
	statMap.AddStatValueFiltered("avg_scan_latency", &s.avgScanLatency)
	statMap.AddStatValueFiltered("num_strict_cons_scans", &s.numStrictConsReqs)

	rawDataSize := s.partnInt64Stats(func(ss *IndexStats) int64 {
		return ss.rawDataSize.Value()
	})

	itemsCount := s.partnInt64Stats(func(ss *IndexStats) int64 {
		return ss.itemsCount.Value()
	})

	flushQueue := s.partnInt64Stats(func(ss *IndexStats) int64 {
		return postiveNum(ss.numDocsFlushQueued.Value() - ss.numDocsIndexed.Value())
	})
	s.flushQueue.Set(flushQueue)
	statMap.AddStatValueFiltered("flush_queue_size", &s.flushQueue)

	s.avgItemSize.Set(computeAvgItemSize(rawDataSize, itemsCount))
	statMap.AddStatValueFiltered("avg_item_size", &s.avgItemSize)

	statMap.AddStatValueFiltered("avg_scan_wait_latency", &s.waitLat)

	statMap.AddStatValueFiltered("avg_scan_request_latency", &s.scanReqLat)

	s.keySizeDist.Set(s.getKeySizeStats())
	statMap.AddStatValueFiltered("key_size_distribution", &s.keySizeDist)

	if s.isArrayIndex {
		if common.GetStorageMode() == common.PLASMA {
			s.arrKeySizeDist.Set(s.getArrKeySizeStats())
			statMap.AddStatValueFiltered("arrkey_size_distribution", &s.arrKeySizeDist)
		}

		docidCount := s.partnInt64Stats(func(ss *IndexStats) int64 {
			return ss.docidCount.Value()
		})

		s.docidCountHolder.Set(docidCount)
		statMap.AddStatValueFiltered("docid_count", &s.docidCountHolder)

		s.avgArrLenHolder.Set(computeAvgArrayLength(itemsCount, docidCount))
		statMap.AddStatValueFiltered("avg_array_length", &s.avgArrLenHolder)
	}

	statMap.AddStatValueFiltered("avg_scan_request_init_latency", &s.scanReqInitLat)
	statMap.AddStatValueFiltered("scan_req_init_latency_dist", &s.scanReqInitLatDist)
	statMap.AddStatValueFiltered("scan_req_wait_latency_dist", &s.scanReqWaitLatDist)
	statMap.AddStatValueFiltered("scan_req_latency_dist", &s.scanReqLatDist)
	statMap.AddStatValueFiltered("snapshot_gen_latency_dist", &s.snapGenLatDist)

	if !spec.essential {
		statMap.AddStatValueFiltered("avg_scan_request_alloc_latency", &s.scanReqAllocLat)
	}

	// -------------------------------
	// All partnMaxInt64Stats
	// -------------------------------
	statMap.AddAggrStatFiltered("key_size_stats_since",
		func(ss *IndexStats) int64 {
			return ss.keySizeStatsSince.Value()
		},
		&s.keySizeStatsSince, s.partnMaxInt64Stats)

	// -------------------------------
	// All partnAvgInt64Stats
	// -------------------------------
	statMap.AddAggrStatFiltered("frag_percent",
		func(ss *IndexStats) int64 {
			return ss.fragPercent.Value()
		},
		&s.fragPercent, s.partnAvgInt64Stats)

	statMap.AddAggrStatFiltered("disk_store_duration",
		func(ss *IndexStats) int64 {
			return ss.diskSnapStoreDuration.Value()
		},
		&s.diskSnapStoreDuration, s.partnAvgInt64Stats)

	statMap.AddAggrStatFiltered("disk_load_duration",
		func(ss *IndexStats) int64 {
			return ss.diskSnapLoadDuration.Value()
		},
		&s.diskSnapLoadDuration, s.partnAvgInt64Stats)

	statMap.AddAggrStatFiltered("resident_percent",
		func(ss *IndexStats) int64 {
			return ss.residentPercent.Value()
		},
		&s.residentPercent, s.partnAvgInt64Stats)

	statMap.AddAggrStatFiltered("cache_hit_percent",
		func(ss *IndexStats) int64 {
			return ss.cacheHitPercent.Value()
		},
		&s.cacheHitPercent, s.partnAvgInt64Stats)

	statMap.AddAggrTimingStatFiltered("timings/dcp_getseqs",
		func(ss *IndexStats) *stats.TimingStat {
			return &ss.Timings.dcpSeqs
		},
		&s.Timings.dcpSeqs, s.partnTimingStats)

	// TODO:
	// Right now, there aren't any consumer specific stats that are essential.
	// But there needs a better way to handle this. May be a separate consumer
	// for non-essential/essential stats?
	if !spec.essential {

		// ------------------------------------------------------------
		// All partnTimingStats
		// If timing stat is partitioned, the final value
		// is aggreated across the partitions (sum, count, sumOfSq).
		// ------------------------------------------------------------

		statMap.AddAggrTimingStatFiltered("timings/storage_clone_handle",
			func(ss *IndexStats) *stats.TimingStat {
				return &ss.Timings.stCloneHandle
			},
			&s.Timings.stCloneHandle, s.partnTimingStats)

		statMap.AddAggrTimingStatFiltered("timings/storage_commit",
			func(ss *IndexStats) *stats.TimingStat {
				return &ss.Timings.stCommit
			},
			&s.Timings.stCommit, s.partnTimingStats)

		statMap.AddAggrTimingStatFiltered("timings/storage_new_iterator",
			func(ss *IndexStats) *stats.TimingStat {
				return &ss.Timings.stNewIterator
			},
			&s.Timings.stNewIterator, s.partnTimingStats)

		statMap.AddAggrTimingStatFiltered("timings/storage_snapshot_create",
			func(ss *IndexStats) *stats.TimingStat {
				return &ss.Timings.stSnapshotCreate
			},
			&s.Timings.stSnapshotCreate, s.partnTimingStats)

		statMap.AddAggrTimingStatFiltered("timings/storage_snapshot_close",
			func(ss *IndexStats) *stats.TimingStat {
				return &ss.Timings.stSnapshotClose
			},
			&s.Timings.stSnapshotClose, s.partnTimingStats)

		statMap.AddAggrTimingStatFiltered("timings/storage_persist_snapshot_create",
			func(ss *IndexStats) *stats.TimingStat {
				return &ss.Timings.stPersistSnapshotCreate
			},
			&s.Timings.stPersistSnapshotCreate, s.partnTimingStats)

		statMap.AddAggrTimingStatFiltered("timings/storage_get",
			func(ss *IndexStats) *stats.TimingStat {
				return &ss.Timings.stKVGet
			},
			&s.Timings.stKVGet, s.partnTimingStats)

		statMap.AddAggrTimingStatFiltered("timings/storage_set",
			func(ss *IndexStats) *stats.TimingStat {
				return &ss.Timings.stKVSet
			},
			&s.Timings.stKVSet, s.partnTimingStats)

		statMap.AddAggrTimingStatFiltered("timings/storage_iterator_next",
			func(ss *IndexStats) *stats.TimingStat {
				return &ss.Timings.stIteratorNext
			},
			&s.Timings.stIteratorNext, s.partnTimingStats)

		statMap.AddAggrTimingStatFiltered("timings/scan_pipeline_iterate",
			func(ss *IndexStats) *stats.TimingStat {
				return &ss.Timings.stScanPipelineIterate
			},
			&s.Timings.stScanPipelineIterate, s.partnTimingStats)

		statMap.AddAggrTimingStatFiltered("timings/storage_del",
			func(ss *IndexStats) *stats.TimingStat {
				return &ss.Timings.stKVDelete
			},
			&s.Timings.stKVDelete, s.partnTimingStats)

		statMap.AddAggrTimingStatFiltered("timings/storage_info",
			func(ss *IndexStats) *stats.TimingStat {
				return &ss.Timings.stKVInfo
			},
			&s.Timings.stKVInfo, s.partnTimingStats)

		statMap.AddAggrTimingStatFiltered("timings/storage_meta_get",
			func(ss *IndexStats) *stats.TimingStat {
				return &ss.Timings.stKVMetaGet
			},
			&s.Timings.stKVMetaGet, s.partnTimingStats)

		statMap.AddAggrTimingStatFiltered("timings/storage_meta_set",
			func(ss *IndexStats) *stats.TimingStat {
				return &ss.Timings.stKVMetaSet
			},
			&s.Timings.stKVMetaSet, s.partnTimingStats)

		statMap.AddAggrTimingStatFiltered("timings/n1ql_expr_eval",
			func(ss *IndexStats) *stats.TimingStat {
				return &ss.Timings.n1qlExpr
			},
			&s.Timings.n1qlExpr, s.partnTimingStats)

		// -------------------------------
		// All int64Stats
		// -------------------------------
		statMap.AddAggrStatFiltered("num_requests_range",
			func(ss *IndexStats) int64 {
				return ss.numRequestsRange.Value()
			},
			&s.numRequestsRange, s.int64Stats)

		statMap.AddAggrStatFiltered("num_completed_requests_range",
			func(ss *IndexStats) int64 {
				return ss.numCompletedRequestsRange.Value()
			},
			&s.numCompletedRequestsRange, s.int64Stats)

		statMap.AddAggrStatFiltered("num_rows_returned_range",
			func(ss *IndexStats) int64 {
				return ss.numRowsReturnedRange.Value()
			},
			&s.numRowsReturnedRange, s.int64Stats)

		statMap.AddAggrStatFiltered("num_rows_scanned_range",
			func(ss *IndexStats) int64 {
				return ss.numRowsScannedRange.Value()
			},
			&s.numRowsScannedRange, s.int64Stats)

		statMap.AddAggrStatFiltered("scan_cache_hit_range",
			func(ss *IndexStats) int64 {
				return ss.scanCacheHitRange.Value()
			},
			&s.scanCacheHitRange, s.int64Stats)

		statMap.AddAggrStatFiltered("num_requests_aggr",
			func(ss *IndexStats) int64 {
				return ss.numRequestsAggr.Value()
			},
			&s.numRequestsAggr, s.int64Stats)

		statMap.AddAggrStatFiltered("num_completed_requests_aggr",
			func(ss *IndexStats) int64 {
				return ss.numCompletedRequestsAggr.Value()
			},
			&s.numCompletedRequestsAggr, s.int64Stats)

		statMap.AddAggrStatFiltered("num_rows_returned_aggr",
			func(ss *IndexStats) int64 {
				return ss.numRowsReturnedAggr.Value()
			},
			&s.numRowsReturnedAggr, s.int64Stats)

		statMap.AddAggrStatFiltered("num_rows_scanned_aggr",
			func(ss *IndexStats) int64 {
				return ss.numRowsScannedAggr.Value()
			},
			&s.numRowsScannedAggr, s.int64Stats)

		statMap.AddAggrStatFiltered("scan_cache_hit_aggr",
			func(ss *IndexStats) int64 {
				return ss.scanCacheHitAggr.Value()
			},
			&s.scanCacheHitAggr, s.int64Stats)

		statMap.AddStatByInstIdFiltered("completion_progress",
			func(ss *IndexStats) int64 {
				return ss.completionProgress.Value()
			},
			&s.completionProgress, s.int64Stats)
	}
}

func (s *IndexStats) populateMetrics(st []byte) []byte {
	s.initializeScanStats()

	var str, collectionLabels string
	fmtStr := "%v%v{bucket=\"%v\", %vindex=\"%v\"} %v\n"

	scope := s.scope
	if scope == "" {
		scope = common.DEFAULT_SCOPE
	}

	collection := s.collection
	if collection == "" {
		collection = common.DEFAULT_COLLECTION
	}
	collectionLabels = fmt.Sprintf("scope=\"%v\", collection=\"%v\", ", scope, collection)

	rawDataSize := s.partnInt64Stats(func(ss *IndexStats) int64 { return ss.rawDataSize.Value() })
	str = fmt.Sprintf(fmtStr, METRICS_PREFIX, "raw_data_size", s.bucket, collectionLabels, s.dispName, rawDataSize)
	st = append(st, []byte(str)...)

	itemsCount := s.partnInt64Stats(func(ss *IndexStats) int64 { return ss.itemsCount.Value() })
	str = fmt.Sprintf(fmtStr, METRICS_PREFIX, "items_count", s.bucket, collectionLabels, s.dispName, itemsCount)
	st = append(st, []byte(str)...)

	scanDuration := s.int64Stats(func(ss *IndexStats) int64 { return ss.scanDuration.Value() })
	str = fmt.Sprintf(fmtStr, METRICS_PREFIX, "total_scan_duration", s.bucket, collectionLabels, s.dispName, scanDuration)
	st = append(st, []byte(str)...)

	numRowsScanned := s.partnInt64Stats(func(ss *IndexStats) int64 { return ss.numRowsScanned.Value() })
	str = fmt.Sprintf(fmtStr, METRICS_PREFIX, "num_rows_scanned", s.bucket, collectionLabels, s.dispName, numRowsScanned)
	st = append(st, []byte(str)...)

	diskSize := s.partnInt64Stats(func(ss *IndexStats) int64 { return ss.diskSize.Value() })
	str = fmt.Sprintf(fmtStr, METRICS_PREFIX, "disk_size", s.bucket, collectionLabels, s.dispName, diskSize)
	st = append(st, []byte(str)...)

	dataSize := s.partnInt64Stats(func(ss *IndexStats) int64 { return ss.dataSize.Value() })
	str = fmt.Sprintf(fmtStr, METRICS_PREFIX, "data_size", s.bucket, collectionLabels, s.dispName, dataSize)
	st = append(st, []byte(str)...)

	scanBytesRead := s.int64Stats(func(ss *IndexStats) int64 { return ss.scanBytesRead.Value() })
	str = fmt.Sprintf(fmtStr, METRICS_PREFIX, "scan_bytes_read", s.bucket, collectionLabels, s.dispName, scanBytesRead)
	st = append(st, []byte(str)...)

	memUsed := s.partnInt64Stats(func(ss *IndexStats) int64 { return ss.memUsed.Value() })
	str = fmt.Sprintf(fmtStr, METRICS_PREFIX, "memory_used", s.bucket, collectionLabels, s.dispName, memUsed)
	st = append(st, []byte(str)...)

	numRowsReturned := s.int64Stats(func(ss *IndexStats) int64 { return ss.numRowsReturned.Value() })
	str = fmt.Sprintf(fmtStr, METRICS_PREFIX, "num_rows_returned", s.bucket, collectionLabels, s.dispName, numRowsReturned)
	st = append(st, []byte(str)...)

	numDocsPending := s.int64Stats(func(ss *IndexStats) int64 { return ss.numDocsPending.Value() })
	str = fmt.Sprintf(fmtStr, METRICS_PREFIX, "num_docs_pending", s.bucket, collectionLabels, s.dispName, numDocsPending)
	st = append(st, []byte(str)...)

	numDocsIndexed := s.partnInt64Stats(func(ss *IndexStats) int64 { return ss.numDocsIndexed.Value() })
	str = fmt.Sprintf(fmtStr, METRICS_PREFIX, "num_docs_indexed", s.bucket, collectionLabels, s.dispName, numDocsIndexed)
	st = append(st, []byte(str)...)

	str = fmt.Sprintf(fmtStr, METRICS_PREFIX, "num_requests", s.bucket, collectionLabels, s.dispName, s.numRequests.Value())
	st = append(st, []byte(str)...)

	numDocsQueued := s.int64Stats(func(ss *IndexStats) int64 { return ss.numDocsQueued.Value() })
	str = fmt.Sprintf(fmtStr, METRICS_PREFIX, "num_docs_queued", s.bucket, collectionLabels, s.dispName, numDocsQueued)
	st = append(st, []byte(str)...)

	cacheHits := s.partnInt64Stats(func(ss *IndexStats) int64 { return ss.cacheHits.Value() })
	str = fmt.Sprintf(fmtStr, METRICS_PREFIX, "cache_hits", s.bucket, collectionLabels, s.dispName, cacheHits)
	st = append(st, []byte(str)...)

	cacheMisses := s.partnInt64Stats(func(ss *IndexStats) int64 { return ss.cacheMisses.Value() })
	str = fmt.Sprintf(fmtStr, METRICS_PREFIX, "cache_misses", s.bucket, collectionLabels, s.dispName, cacheMisses)
	st = append(st, []byte(str)...)

	dataSizeOnDisk := s.partnInt64Stats(func(ss *IndexStats) int64 { return ss.dataSizeOnDisk.Value() })
	str = fmt.Sprintf(fmtStr, METRICS_PREFIX, "data_size_on_disk", s.bucket, collectionLabels, s.dispName, dataSizeOnDisk)
	st = append(st, []byte(str)...)

	logSpaceOnDisk := s.partnInt64Stats(func(ss *IndexStats) int64 { return ss.logSpaceOnDisk.Value() })
	str = fmt.Sprintf(fmtStr, METRICS_PREFIX, "log_space_on_disk", s.bucket, collectionLabels, s.dispName, logSpaceOnDisk)
	st = append(st, []byte(str)...)

	numRecsInMem := s.partnInt64Stats(func(ss *IndexStats) int64 { return ss.numRecsInMem.Value() })
	str = fmt.Sprintf(fmtStr, METRICS_PREFIX, "recs_in_mem", s.bucket, collectionLabels, s.dispName, numRecsInMem)
	st = append(st, []byte(str)...)

	numRecsOnDisk := s.partnInt64Stats(func(ss *IndexStats) int64 { return ss.numRecsOnDisk.Value() })
	str = fmt.Sprintf(fmtStr, METRICS_PREFIX, "recs_on_disk", s.bucket, collectionLabels, s.dispName, numRecsOnDisk)
	st = append(st, []byte(str)...)

	avgItemSize := computeAvgItemSize(rawDataSize, itemsCount)
	str = fmt.Sprintf(fmtStr, METRICS_PREFIX, "avg_item_size", s.bucket, collectionLabels, s.dispName, avgItemSize)
	st = append(st, []byte(str)...)

	str = fmt.Sprintf(fmtStr, METRICS_PREFIX, "avg_scan_latency", s.bucket, collectionLabels, s.dispName, s.avgScanLatency.Value())
	st = append(st, []byte(str)...)

	fragPercent := s.partnAvgInt64Stats(func(ss *IndexStats) int64 { return ss.fragPercent.Value() })
	str = fmt.Sprintf(fmtStr, METRICS_PREFIX, "frag_percent", s.bucket, collectionLabels, s.dispName, fragPercent)
	st = append(st, []byte(str)...)

	avgDrainRate := s.partnInt64Stats(func(ss *IndexStats) int64 { return ss.avgDrainRate.Value() })
	str = fmt.Sprintf(fmtStr, METRICS_PREFIX, "avg_drain_rate", s.bucket, collectionLabels, s.dispName, avgDrainRate)
	st = append(st, []byte(str)...)

	residentPercent := s.partnAvgInt64Stats(func(ss *IndexStats) int64 { return ss.residentPercent.Value() })
	str = fmt.Sprintf(fmtStr, METRICS_PREFIX, "resident_percent", s.bucket, collectionLabels, s.dispName, residentPercent)
	st = append(st, []byte(str)...)

	return st
}

func (is *IndexerStats) MarshalJSON(spec *statsSpec) ([]byte, error) {
	stats := is.GetStats(spec)

	if spec.pretty {
		if val, ok := stats.(map[string]interface{}); ok {
			return json.MarshalIndent(val, "", "   ")
		} else {
			err := fmt.Errorf("StatsManger:MarshalJSON Invalid type for stats, spec: %v", spec)
			logging.Fatalf(err.Error())
			return nil, err
		}
	} else {
		if val, ok := stats.([]byte); ok {
			return val, nil
		} else {
			err := fmt.Errorf("StatsManger:MarshalJSON Invalid type for stats, spec: %v", spec)
			logging.Fatalf(err.Error())
			return nil, err
		}
	}
}

func (is *IndexerStats) VersionedJSON(t *target) ([]byte, error) {
	statsMap, found := is.GetVersionedStats(t)
	if !found {
		return nil, errors.New("404")
	} else if !t.pretty {
		return json.Marshal(statsMap)
	}
	return json.MarshalIndent(statsMap, "", "   ")
}

// IndexerStats.Clone creates a new copy of the IndexerStats object
// with new maps that point to the original stats objects.
func (s *IndexerStats) Clone() *IndexerStats {
	var clone IndexerStats = *s // shallow copy

	clone.indexes = make(map[common.IndexInstId]*IndexStats)
	for k, v := range s.indexes {
		clone.indexes[k] = v.clone()
	}

	clone.keyspaceStatsMap.Set(s.GetKeyspaceStatsMap().Clone())

	return &clone
}

// KeyspaceStatsMap.Clone creates a new version of the KeyspaceStatsMap object
// with new maps that point to the existing stats objects. Intentionally value
// receiver and return since maps are passed by reference in Go.
func (ksm KeyspaceStatsMap) Clone() KeyspaceStatsMap {
	clone := NewKeyspaceStatsMap()

	for streamId, cloneStats := range clone {
		for k, v := range ksm[streamId] {
			cloneStats[k] = v
		}
	}

	return clone
}

func NewIndexerStats() *IndexerStats {
	s := &IndexerStats{}
	s.Init()
	return s
}

// ----------------------------------------------------------------------------
// For many int64 stats, aggregation of the stats across partitions is
// done for reporting the stat values. A generic format of the functions
// used for stats aggregation is defined by StatAggrFunc.
// ----------------------------------------------------------------------------
type StatAggrFunc func(func(*IndexStats) int64) int64

type TimingStatAggrFunc func(func(*IndexStats) *stats.TimingStat) string

// ----------------------------------------------------------------------------
// A map that holds the stats. This struct provides the necessary abstraction
// for the stats map returned by IndexerStats::GetStats.
// ----------------------------------------------------------------------------
type StatsMap struct {
	stMap map[string]interface{}
	// Generating marshalled data in byte slice will avoid
	// JSON marshalling
	byteSlice []byte
	prefix    string
	instId    string
	spec      *statsSpec
}

func NewStatsMap(spec *statsSpec) *StatsMap {
	st := StatsMap{
		stMap:     make(map[string]interface{}),
		byteSlice: make([]byte, 0),
		spec:      spec,
	}
	st.byteSlice = append(st.byteSlice, '{')

	return &st
}

func (st *StatsMap) GetMap() map[string]interface{} {
	return st.stMap
}

func (st *StatsMap) SetInstId(instId string) {
	st.instId = instId
}

func (st *StatsMap) SetPrefix(prefix string) {
	st.prefix = prefix
}

func (st *StatsMap) AddStatValueFiltered(k string, stat stats.StatVal) {
	if !stat.Map(st.spec.consumerFilter) {
		return
	}

	st.AddStat(k, stat.GetValue())
}

func (st *StatsMap) AddStat(k string, v interface{}) {

	addMapValToByteSlice := func(mapKey string, mapVal map[string]interface{}) {
		mapSlice := make([]byte, 0)
		mapSlice = append(mapSlice, []byte(fmt.Sprintf("\"%v\":", mapKey))...)
		mapSlice = append(mapSlice, '{')
		for key, val := range mapVal {
			mapSlice = append(mapSlice, []byte(fmt.Sprintf("\"%v\":%v,", key, val))...)
		}
		if len(mapSlice) > 1 {
			mapSlice[len(mapSlice)-1] = '}'
		}
		mapSlice = append(mapSlice, ',')
		st.byteSlice = append(st.byteSlice, mapSlice...)
	}

	if st.spec.marshalToByteSlice {
		if !st.spec.skipEmpty {
			if str, ok := v.(string); ok {
				st.byteSlice = append(st.byteSlice, []byte(fmt.Sprintf("\"%s%s\":\"%v\",", st.prefix, k, str))...)
			} else if mapVal, ok := v.(map[string]interface{}); ok {
				addMapValToByteSlice(st.prefix+k, mapVal)
			} else {
				st.byteSlice = append(st.byteSlice, []byte(fmt.Sprintf("\"%s%s\":%v,", st.prefix, k, v))...)
			}
		} else if n, ok := v.(int64); ok && n != 0 {
			st.byteSlice = append(st.byteSlice, []byte(fmt.Sprintf("\"%s%s\":%v,", st.prefix, k, v))...)
		} else if s, ok := v.(string); ok && len(s) != 0 && s != "0 0 0" && s != "0" {
			st.byteSlice = append(st.byteSlice, []byte(fmt.Sprintf("\"%s%s\":\"%v\",", st.prefix, k, v))...)
		} else if mapVal, ok := v.(map[string]interface{}); ok && len(mapVal) != 0 {
			addMapValToByteSlice(st.prefix+k, mapVal)
		}
	} else {
		if !st.spec.skipEmpty {
			st.stMap[fmt.Sprintf("%s%s", st.prefix, k)] = v
		} else if n, ok := v.(int64); ok && n != 0 {
			st.stMap[fmt.Sprintf("%s%s", st.prefix, k)] = v
		} else if s, ok := v.(string); ok && len(s) != 0 && s != "0 0 0" && s != "0" {
			st.stMap[fmt.Sprintf("%s%s", st.prefix, k)] = v
		}
	}
}

func (st *StatsMap) AddStatByInstId(k string, v interface{}) {
	if st.spec.marshalToByteSlice {
		if !st.spec.skipEmpty {
			if str, ok := v.(string); ok {
				st.byteSlice = append(st.byteSlice, []byte(fmt.Sprintf("\"%s%s\":\"%v\",", st.instId, k, str))...)
			} else {
				st.byteSlice = append(st.byteSlice, []byte(fmt.Sprintf("\"%s%s\":%v,", st.instId, k, v))...)
			}
		} else if n, ok := v.(int64); ok && n != 0 {
			st.byteSlice = append(st.byteSlice, []byte(fmt.Sprintf("\"%s%s\":%v,", st.instId, k, v))...)
		} else if s, ok := v.(string); ok && len(s) != 0 && s != "0 0 0" && s != "0" {
			st.byteSlice = append(st.byteSlice, []byte(fmt.Sprintf("\"%s%s\":\"%v\",", st.instId, k, v))...)
		}
	} else {
		if !st.spec.skipEmpty {
			st.stMap[fmt.Sprintf("%s%s", st.prefix, k)] = v
		} else if n, ok := v.(int64); ok && n != 0 {
			st.stMap[fmt.Sprintf("%s%s", st.prefix, k)] = v
		} else if s, ok := v.(string); ok && len(s) != 0 && s != "0 0 0" && s != "0" {
			st.stMap[fmt.Sprintf("%s%s", st.prefix, k)] = v
		}
	}
}

// The reference to the function passed to AddAggrStatFiltered (f) has to be a valid function.
func (st *StatsMap) AddAggrStatFiltered(k string, f func(*IndexStats) int64,
	stat stats.StatVal, aggr StatAggrFunc) {
	if !stat.Map(st.spec.consumerFilter) {
		return
	}

	var v interface{}
	if aggr != nil {
		v = aggr(f)
	} else {
		v = stat.GetValue()
	}
	st.AddStat(k, v)
}

// The reference to the function passed to AddAggrTimingStatFiltered (f) has to be a valid function.
func (st *StatsMap) AddAggrTimingStatFiltered(k string, f func(*IndexStats) *stats.TimingStat,
	stat stats.StatVal, aggr TimingStatAggrFunc) {
	if !stat.Map(st.spec.consumerFilter) {
		return
	}

	var v interface{}
	if aggr != nil {
		v = aggr(f)
	} else {
		v = stat.GetValue()
	}
	st.AddStat(k, v)
}

func (st *StatsMap) AddStatByInstIdFiltered(k string, f func(*IndexStats) int64,
	stat stats.StatVal, aggr StatAggrFunc) {
	if !stat.Map(st.spec.consumerFilter) {
		return
	}

	v := aggr(f)
	st.AddStatByInstId(k, v)
}

func (st *StatsMap) AddFloat64StatFiltered(k string, stat stats.StatVal) {
	if !stat.Map(st.spec.consumerFilter) {
		return
	}

	val := stat.GetValue().(int64)
	st.AddStat(k, math.Float64frombits(uint64(val)))
}

//--------------------------------------------------------------------------
// statsSpec can be used to specify which set of stats are to be returned.
//--------------------------------------------------------------------------
type statsSpec struct {
	indexSpec          *common.StatsIndexSpec
	partition          bool
	pretty             bool
	skipEmpty          bool
	essential          bool
	marshalToByteSlice bool // set to true to marshal to byte slice
	consumerFilter     uint64
}

func NewStatsSpec(partition, pretty, skipEmpty, essential, marshalToByteSlice bool, indexSpec *common.StatsIndexSpec) *statsSpec {

	return &statsSpec{
		partition:          partition,
		pretty:             pretty,
		skipEmpty:          skipEmpty,
		essential:          essential,
		marshalToByteSlice: marshalToByteSlice,
		indexSpec:          indexSpec,
		consumerFilter:     stats.AllStatsFilter,
	}
}

func (spec *statsSpec) OverrideFilter(filt string) {
	var filter uint64
	var ok bool

	if filter, ok = statsFilterMap[filt]; ok {
		spec.consumerFilter = filter
	} else {
		spec.consumerFilter = stats.AllStatsFilter
	}
}

var statsFilterMap = map[string]uint64{
	"planner":          stats.PlannerFilter,
	"indexStatus":      stats.IndexStatusFilter,
	"rebalancer":       stats.RebalancerFilter,
	"gsiClient":        stats.GSIClientFilter,
	"n1qlStorageStats": stats.N1QLStorageStatsFilter,
	"summary":          stats.SummaryFilter,
}

const ST_TYPE_INDEXER = "indexer"
const ST_TYPE_INDEX = "index_"
const ST_TYPE_KEYSPACE = "keyspace"
const ST_TYPE_PROJ_LAT = "projlat"
const ST_TYPE_INDEXSTORAGE = "indexstorage_"

type statLogger struct {
	s              *statsManager
	enableStatsLog bool
	sLogger        logstats.LogStats
}

func newStatLogger(s *statsManager, enableStatsLogger bool, sLogger logstats.LogStats) *statLogger {
	return &statLogger{
		s:              s,
		enableStatsLog: enableStatsLogger,
		sLogger:        sLogger,
	}
}
func (l *statLogger) writeIndexStorageStat(spec *statsSpec) {
	res := l.s.getStorageStatsMap(spec)
	for k, r := range res {
		sType := fmt.Sprintf("%s%s", ST_TYPE_INDEXSTORAGE, k)
		err := l.sLogger.Write(sType, r.(map[string]interface{}))
		if err != nil {
			logging.Errorf("Error in writing logs to stats logger type:%v, err:%v", sType, err)
		}
	}
}

func (l *statLogger) writeIndexerStats(stats *IndexerStats, spec *statsSpec) {

	// Indexer Stats
	statMap := NewStatsMap(spec)
	stats.PopulateIndexerStats(statMap)
	err := l.sLogger.Write(ST_TYPE_INDEXER, statMap.GetMap())
	if err != nil {
		logging.Errorf("Error in writing logs to stats logger type:%v, err:%v", ST_TYPE_INDEXER, err)
	}

	// Index Stats
	// In case of stats logging, spec.partition is false.
	for instId, is := range stats.indexes {
		statMap = NewStatsMap(spec)
		statMap.SetInstId(fmt.Sprintf("%v:", instId))

		prefix := common.GetStatsPrefix(is.bucket, is.scope, is.collection, is.name, is.replicaId, 0, false)
		prefix += fmt.Sprintf("%v", instId)
		sType := ST_TYPE_INDEX + prefix

		is.addIndexStatsToMap(statMap, spec)
		err = l.sLogger.Write(sType, statMap.GetMap())
		if err != nil {
			logging.Errorf("Error in writing logs to stats logger type:%v, err:%v", sType, err)
		}
	}

	// Keyspace Stats
	for streamId, ksStats := range stats.GetKeyspaceStatsMap() {
		for keyspaceId, ks := range ksStats {
			statMap = NewStatsMap(spec)
			ks.addKeyspaceStatsToStatsMap(statMap)
			sType := fmt.Sprintf("%v_%v_%v", streamId, ST_TYPE_KEYSPACE, keyspaceId)
			err = l.sLogger.Write(sType, statMap.GetMap())
			if err != nil {
				logging.Errorf("Error in writing logs to stats logger type:%v, err:%v",
					sType, err)
			}
		}
	}

	// Projector Latency Stats
	statMap = NewStatsMap(spec)
	stats.PopulateProjectorLatencyStats(statMap)
	err = l.sLogger.Write(ST_TYPE_PROJ_LAT, statMap.GetMap())
	if err != nil {
		logging.Errorf("Error in writing logs to stats logger type:%v, err:%v", ST_TYPE_PROJ_LAT, err)
	}
}

func (l *statLogger) Write(stats *IndexerStats, essential, writeStorageStats bool) {
	// Use statsMap instead of byte slice when stats are being deduped
	marshalToByteSlice := !l.enableStatsLog
	spec := NewStatsSpec(false, false, false, essential, marshalToByteSlice, nil)

	var sbytes []byte
	logSbytes := false

	if l.enableStatsLog {
		l.writeIndexerStats(stats, spec)
	} else {
		sbytes, _ = stats.MarshalJSON(spec)
		logSbytes = true
	}

	var storageStats string
	if writeStorageStats { //log storage stats every 15mins
		storageMode := common.GetStorageMode()
		if storageMode == common.MOI {
			if l.enableStatsLog {
				l.writeIndexStorageStat(nil)
			} else {
				storageStats = fmt.Sprintf("\n==== StorageStats ====\n%s", l.s.getStorageStats(nil))
			}
		} else if storageMode == common.PLASMA {
			storageStats = fmt.Sprintf("\n==== StorageStats ====\n%s", l.s.getStorageStats(nil))
		} else if logging.IsEnabled(logging.Timing) {
			storageStats = fmt.Sprintf("\n==== StorageStats ====\n%s", l.s.getStorageStats(nil))
		}
	} else {
		storageStats = ""
	}

	if logSbytes {
		logging.Infof("PeriodicStats = %s%s", string(sbytes), storageStats)
	} else {
		if len(storageStats) != 0 {
			logging.Infof(storageStats)
		}
	}
}

type statsManager struct {
	sync.Mutex
	config                common.ConfigHolder
	stats                 IndexerStatsHolder
	supvCmdch             MsgChannel
	supvMsgch             MsgChannel
	lastStatTime          time.Time
	lastProgressStatTime  time.Time
	cacheUpdateInProgress bool
	statsLogDumpInterval  uint64

	statsPersister           StatsPersister
	statsPersistenceInterval uint64
	exitPersister            uint64
	statsUpdaterStopCh       chan bool

	stReqRecCount uint64
}

func NewStatsManager(supvCmdch MsgChannel,
	supvMsgch MsgChannel, config common.Config) (*statsManager, Message) {
	s := &statsManager{
		supvCmdch:                supvCmdch,
		supvMsgch:                supvMsgch,
		lastStatTime:             time.Unix(0, 0),
		statsLogDumpInterval:     config["settings.statsLogDumpInterval"].Uint64(),
		statsPersistenceInterval: config["statsPersistenceInterval"].Uint64(),
	}

	s.config.Store(config)
	statsDir := path.Join(config["storage_dir"].String(), STATS_DATA_DIR)
	chunkSz := config["statsPersistenceChunkSize"].Int()
	s.statsPersister = NewFlatFilePersister(statsDir, chunkSz)

	go s.run()
	go s.runStatsDumpLogger()
	StartCpuCollector()
	return s, &MsgSuccess{}
}

func (s *statsManager) RegisterRestEndpoints() {
	mux := GetHTTPMux()
	mux.HandleFunc("/stats", s.handleStatsReq)
	mux.HandleFunc("/stats/mem", s.handleMemStatsReq)
	mux.HandleFunc("/stats/storage/mm", s.handleStorageMMStatsReq)
	mux.HandleFunc("/stats/storage", s.handleStorageStatsReq)
	mux.HandleFunc("/stats/reset", s.handleStatsResetReq)
	mux.HandleFunc("/_prometheusMetrics", s.handleMetrics)
	mux.HandleFunc("/_prometheusMetricsHigh", s.handleMetricsHigh)
}

func (s *statsManager) tryUpdateStats(sync bool) {
	waitCh := make(chan struct{})
	conf := s.config.Load()
	timeout := time.Millisecond * time.Duration(conf["stats_cache_timeout"].Uint64())
	progressStatsTimeout := time.Millisecond * time.Duration(conf["client_stats_refresh_interval"].Uint64())

	s.Lock()
	cacheTime := s.lastStatTime
	lastProgressStatTime := s.lastProgressStatTime
	shouldUpdate := !s.cacheUpdateInProgress

	if s.lastStatTime.Unix() == 0 {
		sync = true
	}

	// Refresh cache if cache ttl has expired
	allStatsCacheRefreshRequired := time.Now().Sub(cacheTime) > timeout
	progressStatsCacheRefreshRequired := time.Now().Sub(lastProgressStatTime) > progressStatsTimeout
	if shouldUpdate && ((allStatsCacheRefreshRequired || sync) || progressStatsCacheRefreshRequired) {
		s.cacheUpdateInProgress = true
		s.Unlock()

		go func() {

			var stats_list []MsgType
			if allStatsCacheRefreshRequired || sync {
				stats_list = []MsgType{STORAGE_STATS, SCAN_STATS, INDEX_PROGRESS_STATS, INDEXER_STATS}
			} else { // Refresh progress stats cache every 3 seconds for effective load balancing at client
				stats_list = []MsgType{INDEX_PROGRESS_STATS}
			}
			for _, t := range stats_list {
				ch := make(chan bool)
				msg := &MsgStatsRequest{
					mType:    t,
					respch:   ch,
					fetchDcp: true,
				}

				s.supvMsgch <- msg
				<-ch
			}

			s.supvMsgch <- &MsgStatsRequest{mType: INDEX_STATS_DONE, respch: nil}

			s.Lock()
			if allStatsCacheRefreshRequired || sync {
				s.lastStatTime = time.Now()
			}
			s.lastProgressStatTime = time.Now()
			s.cacheUpdateInProgress = false
			s.Unlock()
			close(waitCh)
		}()

		if sync {
			<-waitCh
		}
	} else {
		s.Unlock()
	}
}

func (s *statsManager) handleStatsReq(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if r := recover(); r != nil {
			count := atomic.AddUint64(&s.stReqRecCount, 1)
			if count%60 == 1 {
				logging.Fatalf("handleStatsReq:: Recovered from panic %v. Stacktrace %v",
					count, string(debug.Stack()))
			}
		}
	}()

	_, valid, err := common.IsAuthValid(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error() + "\n"))
		return
	} else if !valid {
		w.WriteHeader(401)
		w.Write([]byte("401 Unauthorized"))
		return
	}

	sync := false
	partition := false
	pretty := false
	skipEmpty := false
	consumerFilter := ""
	if r.Method == "POST" || r.Method == "GET" {
		if r.URL.Query().Get("async") == "false" {
			sync = true
		}
		if r.URL.Query().Get("partition") == "true" {
			partition = true
		}
		if r.URL.Query().Get("pretty") == "true" {
			pretty = true
		}
		if r.URL.Query().Get("skipEmpty") == "true" {
			skipEmpty = true
		}

		consumerFilter = r.URL.Query().Get("consumerFilter")

		var indexSpec *common.StatsIndexSpec
		if r.ContentLength != 0 || r.Body != nil {

			// In case of error in reading the request body, return stats for all indexes
			indexSpecError := false
			buf := new(bytes.Buffer)
			if _, err := buf.ReadFrom(r.Body); err != nil {
				logging.Errorf("handleStatsReq: unable to read request body, err %v", err)
				indexSpecError = true
			}

			if !indexSpecError && buf.Len() != 0 {
				indexSpec = &common.StatsIndexSpec{}
				if err := commonjson.Unmarshal(buf.Bytes(), indexSpec); err != nil {
					logging.Errorf("handleStatsReq: unable to unmarshall request body. Buf = %s, err %v",
						logging.TagStrUD(buf), err)
					indexSpec = nil
				}
			}
		}

		// Marhsal stats to byte slice when pretty is not required
		// Otherwise, marshal to statsMap and JSON marshal will take care of
		// making the output pretty
		marshalToByteSlice := !pretty
		spec := NewStatsSpec(partition, pretty, skipEmpty, false, marshalToByteSlice, indexSpec)
		if consumerFilter != "" {
			spec.OverrideFilter(consumerFilter)
		}
		stats := s.stats.Get()

		t0 := time.Now()
		// If the caller has requested stats with async = false, caller wants
		// the updated stats. tryUpdateStats will ensure the updated stats.
		if common.IndexerState(stats.indexerState.Value()) != common.INDEXER_BOOTSTRAP && sync == true {
			s.tryUpdateStats(sync)
		}
		bytes, _ := stats.MarshalJSON(spec)
		w.WriteHeader(200)
		w.Write(bytes)
		stats.statsResponse.Put(time.Since(t0))
	} else {
		w.WriteHeader(400)
		w.Write([]byte("Unsupported method"))
	}
}

func (s *statsManager) handleMetrics(w http.ResponseWriter, r *http.Request) {
	_, valid, err := common.IsAuthValid(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error() + "\n"))
		return
	} else if !valid {
		w.WriteHeader(401)
		w.Write([]byte("401 Unauthorized"))
		return
	}

	is := s.stats.Get()
	if is == nil {
		w.WriteHeader(200)
		w.Write([]byte(""))
		return
	}

	out := make([]byte, 0, 256)
	out = append(out, []byte(fmt.Sprintf("%vmemory_quota %v\n", METRICS_PREFIX, is.memoryQuota.Value()))...)
	out = append(out, []byte(fmt.Sprintf("%vmemory_used_total %v\n", METRICS_PREFIX, is.memoryUsed.Value()))...)

	w.WriteHeader(200)
	w.Write([]byte(out))
}

func (s *statsManager) handleMetricsHigh(w http.ResponseWriter, r *http.Request) {
	_, valid, err := common.IsAuthValid(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error() + "\n"))
		return
	} else if !valid {
		w.WriteHeader(401)
		w.Write([]byte("401 Unauthorized"))
		return
	}

	is := s.stats.Get()
	if is == nil {
		w.WriteHeader(200)
		w.Write([]byte(""))
		return
	}

	out := make([]byte, 0, len(is.indexes)*APPROX_METRIC_SIZE*APPROX_METRIC_COUNT)
	for _, s := range is.indexes {
		out = s.populateMetrics(out)
	}

	w.WriteHeader(200)
	w.Write(out)
}

func (s *statsManager) handleMemStatsReq(w http.ResponseWriter, r *http.Request) {
	_, valid, err := common.IsAuthValid(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error() + "\n"))
		return
	} else if !valid {
		w.WriteHeader(401)
		w.Write([]byte("401 Unauthorized"))
		return
	}

	stats := new(runtime.MemStats)
	if r.Method == "POST" || r.Method == "GET" {
		runtime.ReadMemStats(stats)
		bytes, _ := json.Marshal(stats)
		w.WriteHeader(200)
		w.Write(bytes)
	} else {
		w.WriteHeader(400)
		w.Write([]byte("Unsupported method"))
	}
}

func (s *statsManager) getStorageStatsMap(spec *statsSpec) map[string]interface{} {
	result := make(map[string]interface{})
	replych := make(chan []IndexStorageStats)
	statReq := &MsgIndexStorageStats{respch: replych, spec: spec}
	s.supvMsgch <- statReq
	res := <-replych

	for _, sts := range res {
		key1 := ""
		scope := sts.Scope
		collection := sts.Collection
		if scope == common.DEFAULT_SCOPE && collection == common.DEFAULT_COLLECTION {
			key1 = fmt.Sprintf("%s:%s", sts.Bucket, sts.Name)
		} else if scope == "" && collection == "" {
			key1 = fmt.Sprintf("%s:%s", sts.Bucket, sts.Name)
		} else {
			key1 = fmt.Sprintf("%s:%s:%s:%s", sts.Bucket, sts.Scope, sts.Collection, sts.Name)
		}
		key := fmt.Sprintf("%s:%v:%v", key1, sts.InstId, sts.PartnId)
		dmap := sts.GetInternalDataMap()
		if dmap == nil || len(sts.GetInternalDataMap()) == 0 {
			logging.Errorf("Error in getStorageStatsMap found InternalStatsMap nil or zero length for %v, skipping stats...", key)
			continue
		}
		result[key] = dmap
	}
	return result
}

func (s *statsManager) getStorageStats(spec *statsSpec) string {
	var result strings.Builder
	replych := make(chan []IndexStorageStats)
	statReq := &MsgIndexStorageStats{respch: replych, spec: spec}
	s.supvMsgch <- statReq
	res := <-replych

	result.WriteString("[\n")
	for i, sts := range res {
		if i > 0 {
			result.WriteString(",")
		}

		scope := sts.Scope
		collection := sts.Collection
		if scope == common.DEFAULT_SCOPE && collection == common.DEFAULT_COLLECTION {
			result.WriteString(fmt.Sprintf("{\n\"Index\": \"%s:%s\", \"Id\": %d, \"PartitionId\": %d,\n",
				sts.Bucket, sts.Name, sts.InstId, sts.PartnId))
		} else if scope == "" && collection == "" {
			result.WriteString(fmt.Sprintf("{\n\"Index\": \"%s:%s\", \"Id\": %d, \"PartitionId\": %d,\n",
				sts.Bucket, sts.Name, sts.InstId, sts.PartnId))
		} else {
			result.WriteString(fmt.Sprintf("{\n\"Index\": \"%s:%s:%s:%s\", \"Id\": %d, \"PartitionId\": %d,\n",
				sts.Bucket, sts.Scope, sts.Collection, sts.Name, sts.InstId, sts.PartnId))
		}

		result.WriteString(fmt.Sprintf("\"Stats\":\n"))
		for _, data := range sts.GetInternalData() {
			result.WriteString(data)
		}
		// No data from storage slice. Add '{}' to make it valid JSON
		if len(sts.GetInternalData()) == 0 {
			result.WriteString("{\n}\n")
		}
		result.WriteString("}\n")
	}

	result.WriteString("]")

	return result.String()
}

func (s *statsManager) handleStorageStatsReq(w http.ResponseWriter, r *http.Request) {
	_, valid, err := common.IsAuthValid(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error() + "\n"))
		return
	} else if !valid {
		w.WriteHeader(401)
		w.Write([]byte("401 Unauthorized"))
		return
	}

	if r.Method == "POST" || r.Method == "GET" {

		stats := s.stats.Get()

		consumerFilter := r.URL.Query().Get("consumerFilter")

		var indexSpec *common.StatsIndexSpec
		if r.ContentLength != 0 || r.Body != nil {

			// In case of error in reading the request body, return stats for all indexes
			indexSpecError := false
			buf := new(bytes.Buffer)
			if _, err := buf.ReadFrom(r.Body); err != nil {
				logging.Errorf("handleStorageStatsReq: unable to read request body, err %v", err)
				indexSpecError = true
			}

			if !indexSpecError && buf.Len() != 0 {
				indexSpec = &common.StatsIndexSpec{}
				if err := commonjson.Unmarshal(buf.Bytes(), indexSpec); err != nil {
					logging.Errorf("handleStorageStatsReq: unable to unmarshall request body. Buf = %s, err %v",
						logging.TagStrUD(buf), err)
					indexSpec = nil
				}
			}
		}

		spec := NewStatsSpec(false, false, false, false, false, indexSpec)
		if consumerFilter != "" {
			spec.OverrideFilter(consumerFilter)
		}

		if common.IndexerState(stats.indexerState.Value()) != common.INDEXER_BOOTSTRAP {
			w.WriteHeader(200)
			w.Write([]byte(s.getStorageStats(spec)))
		} else {
			w.WriteHeader(200)
			w.Write([]byte("Indexer In Warmup. Please try again later."))
		}
	} else {
		w.WriteHeader(400)
		w.Write([]byte("Unsupported method"))
	}
}

func (s *statsManager) handleStorageMMStatsReq(w http.ResponseWriter, r *http.Request) {
	_, valid, err := common.IsAuthValid(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error() + "\n"))
		return
	} else if !valid {
		w.WriteHeader(401)
		w.Write([]byte("401 Unauthorized"))
		return
	}

	if r.Method == "POST" || r.Method == "GET" {

		w.WriteHeader(200)
		w.Write([]byte(mm.Stats()))

	} else {
		w.WriteHeader(400)
		w.Write([]byte("Unsupported method"))
	}
}

func (s *statsManager) handleStatsResetReq(w http.ResponseWriter, r *http.Request) {
	_, valid, err := common.IsAuthValid(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error() + "\n"))
		return
	} else if !valid {
		w.WriteHeader(401)
		w.Write([]byte("401 Unauthorized"))
		return
	}

	if r.Method == "POST" || r.Method == "GET" {
		stats := s.stats.Get()

		if common.IndexerState(stats.indexerState.Value()) != common.INDEXER_BOOTSTRAP {
			s.supvMsgch <- &MsgResetStats{}
			w.WriteHeader(200)
			w.Write([]byte("OK"))
		} else {
			w.WriteHeader(200)
			w.Write([]byte("Indexer In Warmup. Please try again later."))
		}
	} else {
		w.WriteHeader(400)
		w.Write([]byte("Unsupported method"))
	}
}

func (s *statsManager) run() {

loop:
	for {
		select {
		case cmd, ok := <-s.supvCmdch:
			if ok {
				switch cmd.GetMsgType() {
				case STORAGE_MGR_SHUTDOWN:
					logging.Infof("StatsManager::run Shutting Down")
					atomic.StoreUint64(&s.exitPersister, 1)
					if s.statsUpdaterStopCh != nil {
						close(s.statsUpdaterStopCh)
					}
					s.supvCmdch <- &MsgSuccess{}
					break loop
				case UPDATE_INDEX_INSTANCE_MAP:
					// Start the stats updater routine when the index inst map
					// is received for the first time. This ensures that
					// indexer state is set as INDEXER_BOOTSTRAP, before stats
					// updater starts.
					if s.statsUpdaterStopCh == nil {
						s.statsUpdaterStopCh = make(chan bool)
						go s.statsUpdater(s.statsUpdaterStopCh)
					}
					s.handleIndexInstanceUpdate(cmd)
				case CONFIG_SETTINGS_UPDATE:
					s.handleConfigUpdate(cmd)
				case STATS_PERSISTER_START:
					go s.runStatsPersister()
					s.supvCmdch <- &MsgSuccess{}
				case STATS_READ_PERSISTED_STATS:
					req := cmd.(*MsgStatsPersister)
					stats := req.GetStats()
					s.updateStatsFromPersistence(stats)
					s.supvCmdch <- &MsgSuccess{}
				}
			} else {
				break loop
			}
		}
	}
}

func (s *statsManager) statsUpdater(stopCh chan bool) {

	// Set stats Updater's interval such that tryUpdateStats is called at least
	// as frequently as stats_cache_timeout. Keep the interval at max 1 second
	// as the ns_server makes stats request every second.
	conf := s.config.Load()
	timeout := time.Millisecond * time.Duration(conf["stats_cache_timeout"].Uint64())
	if int64(timeout) > int64(time.Second) {
		timeout = time.Second
	}

	ticker := time.NewTicker(timeout)
	defer ticker.Stop()

loop:
	for {
		select {
		case <-stopCh:
			break loop

		case <-ticker.C:
			stats := s.stats.Get()
			if stats != nil {
				if common.IndexerState(stats.indexerState.Value()) != common.INDEXER_BOOTSTRAP {
					s.tryUpdateStats(false)
				}
			}
		}
	}
}

func (s *statsManager) handleIndexInstanceUpdate(cmd Message) {
	req := cmd.(*MsgUpdateInstMap)
	s.stats.Set(req.GetStatsObject())
	s.supvCmdch <- &MsgSuccess{}
}

func (s *statsManager) handleConfigUpdate(cmd Message) {
	cfg := cmd.(*MsgConfigUpdate)
	oldTimeout := s.config.Load()["stats_cache_timeout"].Uint64()
	newTimeout := cfg.GetConfig()["stats_cache_timeout"].Uint64()
	s.config.Store(cfg.GetConfig())

	atomic.StoreUint64(&s.statsLogDumpInterval, cfg.GetConfig()["settings.statsLogDumpInterval"].Uint64())
	atomic.StoreUint64(&s.statsPersistenceInterval, cfg.GetConfig()["statsPersistenceInterval"].Uint64())

	chunksz := cfg.GetConfig()["statsPersistenceChunkSize"].Int()
	s.statsPersister.SetConfig(chunkSz, chunksz)

	// Stop and start the stats updater routine., if required.
	if oldTimeout != newTimeout {
		close(s.statsUpdaterStopCh)
		newStopCh := make(chan bool)
		s.statsUpdaterStopCh = newStopCh
		go s.statsUpdater(s.statsUpdaterStopCh)
	}

	s.supvCmdch <- &MsgSuccess{}
}

func (s *statsManager) tryEnableStatsLog() (bool, logstats.LogStats) {
	conf := s.config.Load()

	// Check if the stats logging is enabled
	enable, ok := conf["statsLogEnable"]
	if !ok {
		return false, nil
	}

	enableStatsLog := enable.Bool()
	if !enableStatsLog {
		return false, nil
	}

	ldir, ok := conf["log_dir"]
	if !ok {
		return false, nil
	}

	logdir := ldir.String()
	if len(logdir) == 0 {
		return false, nil
	}

	fname, ok1 := conf["statsLogFname"]
	fsize, ok2 := conf["statsLogFsize"]
	fcount, ok3 := conf["statsLogFcount"]

	if ok1 && ok2 && ok3 {
		fpath := filepath.Join(logdir, fname.String())
		format := "2006-01-02T15:04:05.000-07:00"
		sLogger, err := logstats.NewDedupeLogStats(fpath, fsize.Int(), fcount.Int(), format)
		if err != nil {
			logging.Infof("Error in NewDedupeLogStats %v. Disabling stats logging.", err)
			return false, nil
		} else {
			return true, sLogger
		}
	}

	return false, nil
}

func (s *statsManager) runStatsDumpLogger() {
	writeStorageStats := 0
	essential := true

	enableStatsLog, sLogger := s.tryEnableStatsLog()
	logger := newStatLogger(s, enableStatsLog, sLogger)

	for {
		stats := s.stats.Get()
		if stats != nil {
			if logging.IsEnabled(logging.Verbose) {
				essential = false
			}

			logger.Write(stats, essential, writeStorageStats > 15)
			if writeStorageStats > 15 {
				writeStorageStats = 0
			} else {
				writeStorageStats++
			}
		}

		time.Sleep(time.Second * time.Duration(atomic.LoadUint64(&s.statsLogDumpInterval)))
	}
}

const last_known_scan_time = "lqt" //last_query_time
const avg_scan_rate = "asr"
const num_rows_scanned = "nrs"
const last_num_rows_scanned = "lrs"
const chunkSz = "chunkSz"

// Periodically persist a subset of index stats
func (s *statsManager) runStatsPersister() {

	defer func() {
		if r := recover(); r != nil {
			logging.Warnf("Encountered panic while running stats persister. Error: %v. Restarting persister.", r)
			time.Sleep(1 * time.Second)
			go s.runStatsPersister()
		}
	}()

	persist := func() { // persist only if statsPersistenceInterval > 0
		if atomic.LoadUint64(&s.statsPersistenceInterval) > 0 {
			indexerStats := s.stats.Get()
			if indexerStats != nil {
				statsToBePersisted := make(map[string]interface{})
				for k, indexStats := range indexerStats.indexes {
					instdId := strconv.FormatUint(uint64(k), 10)
					statsToBePersisted[instdId+":"+last_known_scan_time] = indexStats.lastScanTime.Value()

					for pk, partnStats := range indexStats.partitions {
						partnId := strconv.FormatUint(uint64(pk), 10)
						statsToBePersisted[instdId+":"+partnId+":"+avg_scan_rate] = partnStats.avgScanRate.Value()
						statsToBePersisted[instdId+":"+partnId+":"+num_rows_scanned] = partnStats.numRowsScanned.Value()
						statsToBePersisted[instdId+":"+partnId+":"+last_num_rows_scanned] = partnStats.lastNumRowsScanned.Value()
					}
				}
				err := s.statsPersister.PersistStats(statsToBePersisted)
				if err != nil {
					logging.Warnf("Encountered error while persisting stats. Error: %v", err)
				}
			}
		}
	}

	closePersister := func() {
		err := s.statsPersister.Close()
		if err != nil {
			logging.Warnf("Error closing the persister: %v", err)
		}
	}

loop:
	for {
		if atomic.LoadUint64(&s.exitPersister) == 1 { // exitPersister is 1 only when stats manager shuts down
			logging.Infof("Exiting stats persister")
			break loop
		}
		interval := atomic.LoadUint64(&s.statsPersistenceInterval)
		if interval == 0 {
			closePersister()
			time.Sleep(time.Second * 600) // Sleep for default interval if persistence is disabled
		} else {
			persist()
			time.Sleep(time.Second * time.Duration(interval))
		}
	}
}

func (s *statsManager) updateStatsFromPersistence(indexerStats *IndexerStats) {

	defer func() {
		if r := recover(); r != nil {
			logging.Warnf("Encountered error while reading persisted stats. Skipping read. Error: %v", r)
		}
	}()

	persistedStats, err := s.statsPersister.ReadPersistedStats()
	if err != nil {
		logging.Warnf("Encountered error while reading persisted stats. Skipping read. Error: %v", err)
		return
	}

	getInt64Val := func(value interface{}, statName string) (int64, bool) {
		val, ok := value.(int64)
		if !ok {
			logging.Warnf("StatsPersister: Unable to read stat %v from persistence. Skipping the stat", statName)
		}
		return val, ok
	}

	for k, value := range persistedStats {
		kstrs := strings.Split(k, ":")
		// len(kstrs): 1 =>indexer stat, 2 =>index stat, 3 =>partition stat

		if len(kstrs) == 2 { // index level stat
			id, _ := strconv.ParseUint(kstrs[0], 10, 64)
			instdId := common.IndexInstId(id)
			if _, ok := indexerStats.indexes[instdId]; !ok {
				continue
			}
			statName := kstrs[1]
			switch statName {
			case last_known_scan_time:
				val, ok := getInt64Val(value, statName)
				if ok {
					indexerStats.indexes[instdId].lastScanTime.Set(val)
				}
			}
		}
		if len(kstrs) == 3 { // partition level stat
			id, _ := strconv.ParseUint(kstrs[0], 10, 64)
			instdId := common.IndexInstId(id)
			if _, ok := indexerStats.indexes[instdId]; !ok {
				continue
			}
			pid, _ := strconv.ParseUint(kstrs[1], 10, 64)
			partnId := common.PartitionId(pid)
			if _, ok := indexerStats.indexes[instdId].partitions[partnId]; !ok {
				continue
			}
			statName := kstrs[2]
			switch statName {
			case avg_scan_rate:
				val, ok := getInt64Val(value, statName)
				if ok {
					indexerStats.indexes[instdId].partitions[partnId].avgScanRate.Set(val)
				}
			case num_rows_scanned:
				val, ok := getInt64Val(value, statName)
				if ok {
					indexerStats.indexes[instdId].partitions[partnId].numRowsScanned.Set(val)
				}
			case last_num_rows_scanned:
				val, ok := getInt64Val(value, statName)
				if ok {
					indexerStats.indexes[instdId].partitions[partnId].lastNumRowsScanned.Set(val)
				}
			}
		}
	}
}

func postiveNum(n int64) int64 {
	if n < 0 {
		return 0
	}

	return n
}

// STATS PERSISITER INTERFACE
type StatsPersister interface {
	PersistStats(stats map[string]interface{}) error
	ReadPersistedStats() (map[string]interface{}, error)
	SetConfig(key string, value interface{})
	GetConfig(key string) interface{}
	Close() error
}

const STATS_DATA_DIR = "indexstats"

// Flat file persister
type FlatFileStatsPersister struct {
	statsDir    string
	filePath    string
	newFilePath string

	config map[string]interface{}
}

func NewFlatFilePersister(dir string, chunksz int) *FlatFileStatsPersister {
	os.MkdirAll(dir, 0755)
	file := path.Join(dir, "stats")
	newfile := path.Join(dir, "stats_new")
	fp := FlatFileStatsPersister{
		statsDir:    dir,
		filePath:    file,
		newFilePath: newfile,
	}
	fp.config = make(map[string]interface{})
	fp.SetConfig(chunkSz, chunksz)
	return &fp
}

func (fp *FlatFileStatsPersister) PersistStats(stats map[string]interface{}) error {

	statsJson, err := commonjson.Marshal(stats)
	if err != nil {
		return err
	}

	// Improvement: Implement chunking for large file sizes
	// chunkSize := fp.GetConfig("chunkSize")

	// 8 bytes header for metadata
	// byte 1 : indicates if data is compressed(0 or 1)
	// bytes 2 to 5 : checksum of content
	// remaining bytes - currently unused
	header := make([]byte, 8)
	header[0] = byte(uint8(1))

	compressed := snappy.Encode(nil, statsJson)
	checkSum := crc32.ChecksumIEEE(compressed)
	binary.BigEndian.PutUint32(header[1:5], checkSum)

	content := append(header, compressed...)
	err = ioutil.WriteFile(fp.newFilePath, content, 0755)
	if err != nil {
		return err
	}

	err = os.Rename(fp.newFilePath, fp.filePath)
	if err != nil {
		return err
	}

	return nil
}

func (fp *FlatFileStatsPersister) ReadPersistedStats() (map[string]interface{}, error) {

	content, err := ioutil.ReadFile(fp.filePath)
	if err != nil {
		return nil, err
	}

	var statsJson []byte
	header := content[0:8]

	checkSumHeader := binary.BigEndian.Uint32(header[1:5])
	checkSumContent := crc32.ChecksumIEEE(content[8:])
	if checkSumHeader != checkSumContent {
		return nil, errors.New("ReadPersistedStats: Stats file content checksum mismatch")
	}

	compressed := uint8(header[0])
	if compressed == 1 { //Uncompress the content
		statsJson, err = snappy.Decode(nil, content[8:])
		if err != nil {
			return nil, err
		}
	} else {
		statsJson = content[8:]
	}
	var stats map[string]interface{}
	err = commonjson.Unmarshal(statsJson, &stats)
	if err != nil {
		return nil, err
	}
	return stats, nil
}

func (fp *FlatFileStatsPersister) SetConfig(key string, value interface{}) {
	fp.config[key] = value
}

func (fp *FlatFileStatsPersister) GetConfig(key string) interface{} {
	return fp.config[key]
}

func (fp *FlatFileStatsPersister) Close() error {
	if err := os.RemoveAll(fp.filePath); err != nil {
		return err
	}
	if err := os.RemoveAll(fp.newFilePath); err != nil {
		return err
	}
	return nil
}
