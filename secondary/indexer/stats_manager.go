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
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"errors"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/stats"
	"github.com/couchbase/indexing/secondary/stubs/nitro/mm"
	"strings"
)

var uptime time.Time
var num_cpu_core int

func init() {
	uptime = time.Now()
	num_cpu_core = runtime.NumCPU()
}

type BucketStats struct {
	bucket     string
	indexCount int

	numRollbacks       stats.Int64Val
	mutationQueueSize  stats.Int64Val
	numMutationsQueued stats.Int64Val

	tsQueueSize   stats.Int64Val
	numNonAlignTS stats.Int64Val
}

func (s *BucketStats) Init() {
	s.numRollbacks.Init()
	s.mutationQueueSize.Init()
	s.numMutationsQueued.Init()
	s.tsQueueSize.Init()
	s.numNonAlignTS.Init()
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
}

type IndexStats struct {
	name, bucket string
	replicaId    int

	partitions map[common.PartitionId]*IndexStats

	scanDuration          stats.Int64Val
	scanReqDuration       stats.Int64Val
	scanReqInitDuration   stats.Int64Val
	scanReqAllocDuration  stats.Int64Val
	dcpSeqsDuration       stats.Int64Val
	insertBytes           stats.Int64Val
	numDocsPending        stats.Int64Val
	scanWaitDuration      stats.Int64Val
	numDocsIndexed        stats.Int64Val
	numDocsProcessed      stats.Int64Val
	numRequests           stats.Int64Val
	numCompletedRequests  stats.Int64Val
	numRowsReturned       stats.Int64Val
	diskSize              stats.Int64Val
	memUsed               stats.Int64Val
	buildProgress         stats.Int64Val
	completionProgress    stats.Int64Val
	numDocsQueued         stats.Int64Val
	deleteBytes           stats.Int64Val
	dataSize              stats.Int64Val
	scanBytesRead         stats.Int64Val
	getBytes              stats.Int64Val
	itemsCount            stats.Int64Val
	numCommits            stats.Int64Val
	numSnapshots          stats.Int64Val
	numCompactions        stats.Int64Val
	numItemsFlushed       stats.Int64Val
	avgTsInterval         stats.Int64Val
	avgTsItemsCount       stats.Int64Val
	lastNumFlushQueued    stats.Int64Val
	lastTsTime            stats.Int64Val
	numDocsFlushQueued    stats.Int64Val
	fragPercent           stats.Int64Val
	sinceLastSnapshot     stats.Int64Val
	numSnapshotWaiters    stats.Int64Val
	numLastSnapshotReply  stats.Int64Val
	numItemsRestored      stats.Int64Val
	diskSnapStoreDuration stats.Int64Val
	diskSnapLoadDuration  stats.Int64Val
	notReadyError         stats.Int64Val
	clientCancelError     stats.Int64Val
	avgScanRate           stats.Int64Val
	avgMutationRate       stats.Int64Val
	avgDrainRate          stats.Int64Val
	lastScanGatherTime    stats.Int64Val
	lastNumRowsReturned   stats.Int64Val
	lastMutateGatherTime  stats.Int64Val
	lastNumDocsIndexed    stats.Int64Val
	lastNumItemsFlushed   stats.Int64Val
	lastRollbackTime      stats.TimeVal
	progressStatTime      stats.TimeVal
	residentPercent       stats.Int64Val
	cacheHitPercent       stats.Int64Val

	Timings IndexTimingStats
}

type IndexerStatsHolder struct {
	ptr unsafe.Pointer
}

func (h IndexerStatsHolder) Get() *IndexerStats {
	return (*IndexerStats)(atomic.LoadPointer(&h.ptr))
}

func (h *IndexerStatsHolder) Set(s *IndexerStats) {
	atomic.StorePointer(&h.ptr, unsafe.Pointer(s))
}

func (s *IndexStats) Init() {
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
	s.numCompletedRequests.Init()
	s.numRowsReturned.Init()
	s.diskSize.Init()
	s.memUsed.Init()
	s.buildProgress.Init()
	s.completionProgress.Init()
	s.numDocsQueued.Init()
	s.deleteBytes.Init()
	s.dataSize.Init()
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
	s.avgScanRate.Init()
	s.avgMutationRate.Init()
	s.avgDrainRate.Init()
	s.lastScanGatherTime.Init()
	s.lastNumRowsReturned.Init()
	s.lastMutateGatherTime.Init()
	s.lastNumDocsIndexed.Init()
	s.lastNumItemsFlushed.Init()
	s.lastRollbackTime.Init()
	s.progressStatTime.Init()
	s.residentPercent.Init()
	s.cacheHitPercent.Init()

	s.Timings.Init()

	s.partitions = make(map[common.PartitionId]*IndexStats)
}

func (s *IndexStats) addPartition(id common.PartitionId) {

	if _, ok := s.partitions[id]; !ok {
		partnStats := &IndexStats{}
		partnStats.Init()
		s.partitions[id] = partnStats
	}
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
	indexes map[common.IndexInstId]*IndexStats
	buckets map[string]*BucketStats

	numConnections    stats.Int64Val
	memoryQuota       stats.Int64Val
	memoryUsed        stats.Int64Val
	memoryUsedStorage stats.Int64Val
	memoryUsedQueue   stats.Int64Val
	needsRestart      stats.BoolVal
	statsResponse     stats.TimingStat
	notFoundError     stats.Int64Val

	indexerState stats.Int64Val
}

func (s *IndexerStats) Init() {
	s.indexes = make(map[common.IndexInstId]*IndexStats)
	s.buckets = make(map[string]*BucketStats)
	s.numConnections.Init()
	s.memoryQuota.Init()
	s.memoryUsed.Init()
	s.memoryUsedStorage.Init()
	s.memoryUsedQueue.Init()
	s.needsRestart.Init()
	s.statsResponse.Init()
	s.indexerState.Init()
	s.notFoundError.Init()
}

func (s *IndexerStats) Reset() {
	old := *s
	*s = IndexerStats{}
	s.Init()
	for k, v := range old.indexes {
		s.AddIndex(k, v.bucket, v.name, v.replicaId)
	}
}

func (s *IndexerStats) AddIndex(id common.IndexInstId, bucket string, name string, replicaId int) {

	b, ok := s.buckets[bucket]
	if !ok {
		b = &BucketStats{bucket: bucket}
		b.Init()
		s.buckets[bucket] = b
	}

	if _, ok := s.indexes[id]; !ok {
		idxStats := &IndexStats{name: name, bucket: bucket, replicaId: replicaId}
		idxStats.Init()
		s.indexes[id] = idxStats

		b.indexCount++
	}
}

func (s *IndexerStats) AddPartition(id common.IndexInstId, bucket string, name string, replicaId int, partitionId common.PartitionId) {

	if _, ok := s.indexes[id]; !ok {
		s.AddIndex(id, bucket, name, replicaId)
	}

	s.indexes[id].addPartition(partitionId)
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

func (s *IndexerStats) RemoveIndex(id common.IndexInstId) {
	idx, ok := s.indexes[id]
	if !ok {
		return
	}
	delete(s.indexes, id)
	b := s.buckets[idx.bucket]
	b.indexCount--
	if b.indexCount == 0 {
		delete(s.buckets, idx.bucket)
	}
}

func (is IndexerStats) GetStats(getPartition bool, skipEmpty bool) common.Statistics {

	var prefix string

	statsMap := make(map[string]interface{})
	addStat := func(k string, v interface{}) {
		if !skipEmpty {
			statsMap[fmt.Sprintf("%s%s", prefix, k)] = v
		} else if n, ok := v.(int64); ok && n != 0 {
			statsMap[fmt.Sprintf("%s%s", prefix, k)] = v
		} else if s, ok := v.(string); ok && len(s) != 0 && s != "0 0 0" && s != "0" {
			statsMap[fmt.Sprintf("%s%s", prefix, k)] = v
		}
	}

	addStat("uptime", fmt.Sprintf("%s", time.Since(uptime)))
	addStat("num_connections", is.numConnections.Value())
	addStat("index_not_found_errcount", is.notFoundError.Value())
	addStat("memory_quota", is.memoryQuota.Value())
	addStat("memory_used", is.memoryUsed.Value())
	addStat("memory_used_storage", is.memoryUsedStorage.Value())
	addStat("memory_used_queue", is.memoryUsedQueue.Value())
	addStat("needs_restart", is.needsRestart.Value())
	storageMode := fmt.Sprintf("%s", common.GetStorageMode())
	addStat("storage_mode", storageMode)
	addStat("num_cpu_core", num_cpu_core)
	addStat("cpu_utilization", getCpuPercent())

	indexerState := common.IndexerState(is.indexerState.Value())
	if indexerState == common.INDEXER_PREPARE_UNPAUSE {
		indexerState = common.INDEXER_PAUSED
	}
	addStat("indexer_state", fmt.Sprintf("%s", indexerState))

	addStat("timings/stats_response", is.statsResponse.Value())

	addIndexStats := func(s *IndexStats) {

		var scanLat, waitLat, scanReqLat, scanReqInitLat, scanReqAllocLat int64
		reqs := s.numRequests.Value()

		if reqs > 0 {
			scanDur := s.int64Stats(func(ss *IndexStats) int64 { return ss.scanDuration.Value() })
			waitDur := s.int64Stats(func(ss *IndexStats) int64 { return ss.scanWaitDuration.Value() })
			scanReqDur := s.int64Stats(func(ss *IndexStats) int64 { return ss.scanReqDuration.Value() })
			scanReqInitDur := s.int64Stats(func(ss *IndexStats) int64 { return ss.scanReqInitDuration.Value() })
			scanReqAllocDur := s.int64Stats(func(ss *IndexStats) int64 { return ss.scanReqAllocDuration.Value() })
			scanLat = scanDur / reqs
			waitLat = waitDur / reqs
			scanReqLat = scanReqDur / reqs
			scanReqInitLat = scanReqInitDur / reqs
			scanReqAllocLat = scanReqAllocDur / reqs
		}

		addStat("total_scan_duration",
			s.int64Stats(func(ss *IndexStats) int64 {
				return ss.scanDuration.Value()
			}))
		addStat("total_scan_request_duration",
			s.int64Stats(func(ss *IndexStats) int64 {
				return ss.scanReqDuration.Value()
			}))
		// partition stats
		addStat("insert_bytes",
			s.partnInt64Stats(func(ss *IndexStats) int64 {
				return ss.insertBytes.Value()
			}))
		addStat("num_docs_pending",
			s.int64Stats(func(ss *IndexStats) int64 {
				return ss.numDocsPending.Value()
			}))
		addStat("scan_wait_duration",
			s.int64Stats(func(ss *IndexStats) int64 {
				return ss.scanWaitDuration.Value()
			}))
		// partition stats
		addStat("num_docs_indexed",
			s.partnInt64Stats(func(ss *IndexStats) int64 {
				return ss.numDocsIndexed.Value()
			}))
		addStat("num_docs_processed",
			s.int64Stats(func(ss *IndexStats) int64 {
				return ss.numDocsProcessed.Value()
			}))
		addStat("num_requests",
			s.int64Stats(func(ss *IndexStats) int64 {
				return ss.numRequests.Value()
			}))
		addStat("num_completed_requests",
			s.int64Stats(func(ss *IndexStats) int64 {
				return ss.numCompletedRequests.Value()
			}))
		addStat("num_rows_returned",
			s.int64Stats(func(ss *IndexStats) int64 {
				return ss.numRowsReturned.Value()
			}))
		// partition stats
		addStat("disk_size",
			s.partnInt64Stats(func(ss *IndexStats) int64 {
				return ss.diskSize.Value()
			}))
		// partition stats
		addStat("memory_used",
			s.partnInt64Stats(func(ss *IndexStats) int64 {
				return ss.memUsed.Value()
			}))
		addStat("build_progress",
			s.int64Stats(func(ss *IndexStats) int64 {
				return ss.buildProgress.Value()
			}))
		addStat("completion_progress",
			s.int64Stats(func(ss *IndexStats) int64 {
				return ss.completionProgress.Value()
			}))
		addStat("num_docs_queued",
			s.int64Stats(func(ss *IndexStats) int64 {
				return ss.numDocsQueued.Value()
			}))
		// partition stats
		addStat("delete_bytes",
			s.partnInt64Stats(func(ss *IndexStats) int64 {
				return ss.deleteBytes.Value()
			}))
		// partition stats
		addStat("data_size",
			s.partnInt64Stats(func(ss *IndexStats) int64 {
				return ss.dataSize.Value()
			}))
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
		addStat("get_bytes",
			s.partnInt64Stats(func(ss *IndexStats) int64 {
				return ss.getBytes.Value()
			}))
		// partition stats
		addStat("items_count",
			s.partnInt64Stats(func(ss *IndexStats) int64 {
				return ss.itemsCount.Value()
			}))
		addStat("avg_ts_interval",
			s.int64Stats(func(ss *IndexStats) int64 {
				return ss.avgTsInterval.Value()
			}))
		addStat("avg_ts_items_count",
			s.int64Stats(func(ss *IndexStats) int64 {
				return ss.avgTsItemsCount.Value()
			}))
		addStat("num_commits",
			s.int64Stats(func(ss *IndexStats) int64 {
				return ss.numCommits.Value()
			}))
		addStat("num_snapshots",
			s.int64Stats(func(ss *IndexStats) int64 {
				return ss.numSnapshots.Value()
			}))
		addStat("num_compactions",
			s.int64Stats(func(ss *IndexStats) int64 {
				return ss.numCompactions.Value()
			}))
		// partition stats
		addStat("flush_queue_size",
			s.partnInt64Stats(func(ss *IndexStats) int64 {
				return postiveNum(ss.numDocsFlushQueued.Value() - ss.numDocsIndexed.Value())
			}))
		// partition stats
		addStat("num_items_flushed",
			s.partnInt64Stats(func(ss *IndexStats) int64 {
				return ss.numItemsFlushed.Value()
			}))
		addStat("avg_scan_latency", scanLat)
		addStat("avg_scan_wait_latency", waitLat)
		addStat("avg_scan_request_latency", scanReqLat)
		addStat("avg_scan_request_init_latency", scanReqInitLat)
		addStat("avg_scan_request_alloc_latency", scanReqAllocLat)
		// partition stats
		addStat("num_flush_queued",
			s.partnInt64Stats(func(ss *IndexStats) int64 {
				return ss.numDocsFlushQueued.Value()
			}))
		addStat("since_last_snapshot",
			s.int64Stats(func(ss *IndexStats) int64 {
				return ss.sinceLastSnapshot.Value()
			}))
		addStat("num_snapshot_waiters",
			s.int64Stats(func(ss *IndexStats) int64 {
				return ss.numSnapshotWaiters.Value()
			}))
		addStat("num_last_snapshot_reply",
			s.int64Stats(func(ss *IndexStats) int64 {
				return ss.numLastSnapshotReply.Value()
			}))
		// partition stats
		addStat("num_items_restored",
			s.partnInt64Stats(func(ss *IndexStats) int64 {
				return ss.numItemsRestored.Value()
			}))
		// partition stats
		addStat("disk_store_duration",
			s.partnAvgInt64Stats(func(ss *IndexStats) int64 {
				return ss.diskSnapStoreDuration.Value()
			}))
		// partition stats
		addStat("disk_load_duration",
			s.partnAvgInt64Stats(func(ss *IndexStats) int64 {
				return ss.diskSnapLoadDuration.Value()
			}))
		addStat("not_ready_errcount",
			s.int64Stats(func(ss *IndexStats) int64 {
				return ss.notReadyError.Value()
			}))
		addStat("client_cancel_errcount",
			s.int64Stats(func(ss *IndexStats) int64 {
				return ss.clientCancelError.Value()
			}))
		addStat("avg_scan_rate",
			s.int64Stats(func(ss *IndexStats) int64 {
				return ss.avgScanRate.Value()
			}))
		// partition stats
		addStat("avg_mutation_rate",
			s.partnAvgInt64Stats(func(ss *IndexStats) int64 {
				return ss.avgMutationRate.Value()
			}))
		// partition stats
		addStat("avg_drain_rate",
			s.partnAvgInt64Stats(func(ss *IndexStats) int64 {
				return ss.avgDrainRate.Value()
			}))
		addStat("last_rollback_time", s.lastRollbackTime.Value())
		addStat("progress_stat_time", s.progressStatTime.Value())
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

		addStat("timings/dcp_getseqs",
			s.partnTimingStats(func(ss *IndexStats) *stats.TimingStat {
				return &ss.Timings.dcpSeqs
			}))
		addStat("timings/storage_clone_handle",
			s.partnTimingStats(func(ss *IndexStats) *stats.TimingStat {
				return &ss.Timings.stCloneHandle
			}))
		addStat("timings/storage_commit",
			s.partnTimingStats(func(ss *IndexStats) *stats.TimingStat {
				return &ss.Timings.stCommit
			}))
		addStat("timings/storage_new_iterator",
			s.partnTimingStats(func(ss *IndexStats) *stats.TimingStat {
				return &ss.Timings.stNewIterator
			}))
		addStat("timings/storage_snapshot_create",
			s.partnTimingStats(func(ss *IndexStats) *stats.TimingStat {
				return &ss.Timings.stSnapshotCreate
			}))
		addStat("timings/storage_snapshot_close",
			s.partnTimingStats(func(ss *IndexStats) *stats.TimingStat {
				return &ss.Timings.stSnapshotClose
			}))
		addStat("timings/storage_persist_snapshot_create",
			s.partnTimingStats(func(ss *IndexStats) *stats.TimingStat {
				return &ss.Timings.stPersistSnapshotCreate
			}))
		addStat("timings/storage_get",
			s.partnTimingStats(func(ss *IndexStats) *stats.TimingStat {
				return &ss.Timings.stKVGet
			}))
		addStat("timings/storage_set",
			s.partnTimingStats(func(ss *IndexStats) *stats.TimingStat {
				return &ss.Timings.stKVSet
			}))
		addStat("timings/storage_iterator_next",
			s.partnTimingStats(func(ss *IndexStats) *stats.TimingStat {
				return &ss.Timings.stIteratorNext
			}))
		addStat("timings/scan_pipeline_iterate",
			s.partnTimingStats(func(ss *IndexStats) *stats.TimingStat {
				return &ss.Timings.stScanPipelineIterate
			}))
		addStat("timings/storage_del",
			s.partnTimingStats(func(ss *IndexStats) *stats.TimingStat {
				return &ss.Timings.stKVDelete
			}))
		addStat("timings/storage_info",
			s.partnTimingStats(func(ss *IndexStats) *stats.TimingStat {
				return &ss.Timings.stKVInfo
			}))
		addStat("timings/storage_meta_get",
			s.partnTimingStats(func(ss *IndexStats) *stats.TimingStat {
				return &ss.Timings.stKVMetaGet
			}))
		addStat("timings/storage_meta_set",
			s.partnTimingStats(func(ss *IndexStats) *stats.TimingStat {
				return &ss.Timings.stKVMetaSet
			}))
	}

	for _, s := range is.indexes {

		name := common.FormatIndexInstDisplayName(s.name, s.replicaId)
		prefix = fmt.Sprintf("%s:%s:", s.bucket, name)

		addIndexStats(s)

		if getPartition {

			for partnId, ps := range s.partitions {
				name := common.FormatIndexPartnDisplayName(s.name, s.replicaId, int(partnId), true)
				prefix = fmt.Sprintf("%s:%s:", s.bucket, name)
				addIndexStats(ps)
			}
		}
	}

	for _, s := range is.buckets {
		prefix = fmt.Sprintf("%s:", s.bucket)
		addStat("num_rollbacks", s.numRollbacks.Value())
		addStat("mutation_queue_size", s.mutationQueueSize.Value())
		addStat("num_mutations_queued", s.numMutationsQueued.Value())
		addStat("ts_queue_size", s.tsQueueSize.Value())
		addStat("num_nonalign_ts", s.numNonAlignTS.Value())
		if st := common.BucketSeqsTiming(s.bucket); st != nil {
			addStat("timings/dcp_getseqs", st.Value())
		}
	}

	return statsMap
}

func (is IndexerStats) GetVersionedStats(t *target) (common.Statistics, bool) {
	var key string
	statsMap := make(map[string]interface{})
	var found bool

	if t.level == "indexer" {
		statsMap["indexer"] = is.constructIndexerStats(t.skipEmpty, t.version)
		for _, s := range is.indexes {
			name := common.FormatIndexInstDisplayName(s.name, s.replicaId)
			key = fmt.Sprintf("%s:%s", s.bucket, name)
			statsMap[key] = s.constructIndexStats(t.skipEmpty, t.version)
		}
		found = true
	} else if t.level == "index" {
		for _, s := range is.indexes {
			if strings.EqualFold(s.name, t.resource) {
				name := common.FormatIndexInstDisplayName(s.name, s.replicaId)
				key = fmt.Sprintf("%s:%s", s.bucket, name)
				statsMap[key] = s.constructIndexStats(t.skipEmpty, t.version)
				if t.partition {
					for partnId, ps := range s.partitions {
						key = fmt.Sprintf("Partition-%d", int(partnId))
						statsMap[key] = ps.constructIndexStats(t.skipEmpty, t.version)
					}
				}
				found = true
				break
			}
		}
	}
	return statsMap, found
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

func (is IndexerStats) constructIndexerStats(skipEmpty bool, version string) common.Statistics {
	indexerStats := make(map[string]interface{})
	addStat := addStatFactory(skipEmpty, indexerStats)

	switch version {
	case "v1":
		addStat("memory_quota", is.memoryQuota.Value())
		addStat("memory_used", is.memoryUsed.Value())

		indexerState := common.IndexerState(is.indexerState.Value())
		if indexerState == common.INDEXER_PREPARE_UNPAUSE {
			indexerState = common.INDEXER_PAUSED
		}
		addStat("indexer_state", fmt.Sprintf("%s", indexerState))
	}

	return indexerStats
}

func (s *IndexStats) constructIndexStats(skipEmpty bool, version string) common.Statistics {
	indexStats := make(map[string]interface{})
	addStat := addStatFactory(skipEmpty, indexStats)

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
	addStat("num_requests",
		s.int64Stats(func(ss *IndexStats) int64 {
			return ss.numRequests.Value()
		}))
	addStat("num_rows_returned",
		s.int64Stats(func(ss *IndexStats) int64 {
			return ss.numRowsReturned.Value()
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
	addStat("items_count",
		s.partnInt64Stats(func(ss *IndexStats) int64 {
			return ss.itemsCount.Value()
		}))
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

	return indexStats
}

func (is IndexerStats) MarshalJSON(partition bool, pretty bool, skipEmpty bool) ([]byte, error) {
	stats := is.GetStats(partition, skipEmpty)

	if !pretty {
		return json.Marshal(stats)
	} else {
		return json.MarshalIndent(stats, "", "   ")
	}
}

func (is IndexerStats) VersionedJSON(t *target) ([]byte, error) {
	statsMap, found := is.GetVersionedStats(t)
	if !found {
		return nil, errors.New("404")
	} else if !t.pretty {
		return json.Marshal(statsMap)
	}
	return json.MarshalIndent(statsMap, "", "   ")
}

func (s IndexerStats) Clone() *IndexerStats {
	var clone IndexerStats
	clone = s
	clone.indexes = make(map[common.IndexInstId]*IndexStats)
	clone.buckets = make(map[string]*BucketStats)
	for k, v := range s.indexes {
		clone.indexes[k] = v
	}
	for k, v := range s.buckets {
		clone.buckets[k] = v
	}

	return &clone
}

func NewIndexerStats() *IndexerStats {
	s := &IndexerStats{}
	s.Init()
	return s
}

type statsManager struct {
	sync.Mutex
	config                common.ConfigHolder
	stats                 IndexerStatsHolder
	supvCmdch             MsgChannel
	supvMsgch             MsgChannel
	lastStatTime          time.Time
	cacheUpdateInProgress bool
	statsLogDumpInterval  uint64
}

func NewStatsManager(supvCmdch MsgChannel,
	supvMsgch MsgChannel, config common.Config) (*statsManager, Message) {
	s := &statsManager{
		supvCmdch:            supvCmdch,
		supvMsgch:            supvMsgch,
		lastStatTime:         time.Unix(0, 0),
		statsLogDumpInterval: config["settings.statsLogDumpInterval"].Uint64(),
	}

	s.config.Store(config)

	http.HandleFunc("/stats", s.handleStatsReq)
	http.HandleFunc("/stats/mem", s.handleMemStatsReq)
	http.HandleFunc("/stats/storage/mm", s.handleStorageMMStatsReq)
	http.HandleFunc("/stats/storage", s.handleStorageStatsReq)
	http.HandleFunc("/stats/reset", s.handleStatsResetReq)
	go s.run()
	go s.runStatsDumpLogger()
	StartCpuCollector()
	return s, &MsgSuccess{}
}

func (s *statsManager) tryUpdateStats(sync bool) {
	waitCh := make(chan struct{})
	conf := s.config.Load()
	timeout := time.Millisecond * time.Duration(conf["stats_cache_timeout"].Uint64())

	s.Lock()
	cacheTime := s.lastStatTime
	shouldUpdate := !s.cacheUpdateInProgress

	if s.lastStatTime.Unix() == 0 {
		sync = true
	}

	// Refresh cache if cache ttl has expired
	if shouldUpdate && time.Now().Sub(cacheTime) > timeout || sync {
		s.cacheUpdateInProgress = true
		s.Unlock()

		go func() {

			stats_list := []MsgType{STORAGE_STATS, SCAN_STATS, INDEX_PROGRESS_STATS, INDEXER_STATS}
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
			s.lastStatTime = time.Now()
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
	sync := false
	partition := false
	pretty := false
	skipEmpty := false
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
		stats := s.stats.Get()

		t0 := time.Now()
		if common.IndexerState(stats.indexerState.Value()) != common.INDEXER_BOOTSTRAP {
			s.tryUpdateStats(sync)
		}
		bytes, _ := stats.MarshalJSON(partition, pretty, skipEmpty)
		w.WriteHeader(200)
		w.Write(bytes)
		stats.statsResponse.Put(time.Since(t0))
	} else {
		w.WriteHeader(400)
		w.Write([]byte("Unsupported method"))
	}
}

func (s *statsManager) handleMemStatsReq(w http.ResponseWriter, r *http.Request) {
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

func (s *statsManager) getStorageStats() string {
	var result string
	replych := make(chan []IndexStorageStats)
	statReq := &MsgIndexStorageStats{respch: replych}
	s.supvMsgch <- statReq
	res := <-replych

	result += "[\n"
	for i, sts := range res {
		if i > 0 {
			result += ","
		}
		result += fmt.Sprintf("{\n\"Index\": \"%s:%s\", \"Id\": %d,\n", sts.Bucket, sts.Name, sts.InstId)
		result += fmt.Sprintf("\"Stats\":\n")
		for _, data := range sts.GetInternalData() {
			result += data
		}
		result += "}\n"
	}

	result += "]"

	return result
}

func (s *statsManager) handleStorageStatsReq(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" || r.Method == "GET" {

		w.WriteHeader(200)
		w.Write([]byte(s.getStorageStats()))

	} else {
		w.WriteHeader(400)
		w.Write([]byte("Unsupported method"))
	}
}

func (s *statsManager) handleStorageMMStatsReq(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" || r.Method == "GET" {

		w.WriteHeader(200)
		w.Write([]byte(mm.Stats()))

	} else {
		w.WriteHeader(400)
		w.Write([]byte("Unsupported method"))
	}
}

func (s *statsManager) handleStatsResetReq(w http.ResponseWriter, r *http.Request) {
	_, valid, _ := common.IsAuthValid(r)
	if !valid {
		w.WriteHeader(401)
		w.Write([]byte("401 Unauthorized"))
		return
	}

	if r.Method == "POST" || r.Method == "GET" {
		s.supvMsgch <- &MsgResetStats{}
		w.WriteHeader(200)
		w.Write([]byte("OK"))
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
					logging.Infof("SettingsManager::run Shutting Down")
					s.supvCmdch <- &MsgSuccess{}
					break loop
				case UPDATE_INDEX_INSTANCE_MAP:
					s.handleIndexInstanceUpdate(cmd)
				case CONFIG_SETTINGS_UPDATE:
					s.handleConfigUpdate(cmd)
				}
			} else {
				break loop
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
	s.config.Store(cfg.GetConfig())
	atomic.StoreUint64(&s.statsLogDumpInterval, cfg.GetConfig()["settings.statsLogDumpInterval"].Uint64())
	s.supvCmdch <- &MsgSuccess{}
}

func (s *statsManager) runStatsDumpLogger() {
	for {
		stats := s.stats.Get()
		if stats != nil {
			bytes, _ := stats.MarshalJSON(false, false, false)
			var storageStats string
			if common.GetStorageMode() != common.FORESTDB {
				storageStats = fmt.Sprintf("\n==== StorageStats ====\n%s", s.getStorageStats())
			} else if logging.IsEnabled(logging.Timing) {
				storageStats = fmt.Sprintf("\n==== StorageStats ====\n%s", s.getStorageStats())
			}
			logging.Infof("PeriodicStats = %s%s", string(bytes), storageStats)
		}

		time.Sleep(time.Second * time.Duration(atomic.LoadUint64(&s.statsLogDumpInterval)))
	}
}

func postiveNum(n int64) int64 {
	if n < 0 {
		return 0
	}

	return n
}
