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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"runtime"
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
	"github.com/golang/snappy"
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
	name, bucket string
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

	s.partitions = make(map[common.PartitionId]*IndexStats)
}

func (s *IndexStats) getPartitions() []common.PartitionId {

	partitions := make([]common.PartitionId, 0, len(s.partitions))
	for id, _ := range s.partitions {
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
	indexes map[common.IndexInstId]*IndexStats
	buckets map[string]*BucketStats

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

	pauseTotalNs stats.Uint64Val
}

func (s *IndexerStats) Init() {
	s.indexes = make(map[common.IndexInstId]*IndexStats)
	s.buckets = make(map[string]*BucketStats)
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
	s.pauseTotalNs.Init()
}

func (s *IndexerStats) Reset() {
	old := *s
	*s = IndexerStats{}
	s.Init()
	for k, v := range old.indexes {
		s.AddIndex(k, v.bucket, v.name, v.replicaId, v.isArrayIndex)
	}
}

func (s *IndexerStats) AddIndex(id common.IndexInstId, bucket string, name string,
	replicaId int, isArrIndex bool) {

	b, ok := s.buckets[bucket]
	if !ok {
		b = &BucketStats{bucket: bucket}
		b.Init()
		s.buckets[bucket] = b
	}

	if _, ok := s.indexes[id]; !ok {
		idxStats := &IndexStats{name: name, bucket: bucket,
			replicaId: replicaId, isArrayIndex: isArrIndex}
		idxStats.Init()
		s.indexes[id] = idxStats

		b.indexCount++
	}
}

func (s *IndexerStats) AddPartition(id common.IndexInstId, bucket string, name string,
	replicaId int, partitionId common.PartitionId, isArrIndex bool) {

	if _, ok := s.indexes[id]; !ok {
		s.AddIndex(id, bucket, name, replicaId, isArrIndex)
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

func (is *IndexerStats) GetStats(getPartition bool, skipEmpty bool,
	essential bool) common.Statistics {

	var prefix string
	var instId string

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
	addStatByInstId := func(k string, v interface{}) {
		if !skipEmpty {
			statsMap[fmt.Sprintf("%s%s", instId, k)] = v
		} else if n, ok := v.(int64); ok && n != 0 {
			statsMap[fmt.Sprintf("%s%s", instId, k)] = v
		} else if s, ok := v.(string); ok && len(s) != 0 && s != "0 0 0" && s != "0" {
			statsMap[fmt.Sprintf("%s%s", instId, k)] = v
		}
	}

	addStat("timestamp", fmt.Sprintf("%v", time.Now().UnixNano()))
	addStat("uptime", fmt.Sprintf("%s", time.Since(uptime)))
	addStat("num_connections", is.numConnections.Value())
	addStat("index_not_found_errcount", is.notFoundError.Value())
	addStat("memory_quota", is.memoryQuota.Value())
	addStat("memory_used", is.memoryUsed.Value())
	addStat("memory_used_storage", is.memoryUsedStorage.Value())
	addStat("memory_total_storage", is.memoryTotalStorage.Value())
	addStat("memory_used_queue", is.memoryUsedQueue.Value())
	addStat("needs_restart", is.needsRestart.Value())
	storageMode := fmt.Sprintf("%s", common.GetStorageMode())
	addStat("storage_mode", storageMode)
	addStat("num_cpu_core", num_cpu_core)
	addStat("cpu_utilization", getCpuPercent())
	addStat("memory_rss", getRSS())
	addStat("memory_free", getMemFree())
	addStat("memory_total", getMemTotal())

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
			addStat(newPrjAddr+"/projector_latency", latency.Value())
		}
	}

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
		// partition and index stats
		addStat("num_requests", s.numRequests.Value())

		addStat("last_known_scan_time", s.lastScanTime.Value())

		// partition and index stats
		addStat("num_completed_requests", s.numCompletedRequests.Value())
		addStat("num_rows_returned",
			s.int64Stats(func(ss *IndexStats) int64 {
				return ss.numRowsReturned.Value()
			}))
		// partition stats
		addStat("num_rows_scanned",
			s.partnInt64Stats(func(ss *IndexStats) int64 {
				return ss.numRowsScanned.Value()
			}))

		addStat("num_strict_cons_scans", s.numStrictConsReqs.Value())

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
		addStat("data_size_on_disk",
			s.partnInt64Stats(func(ss *IndexStats) int64 {
				return ss.dataSizeOnDisk.Value()
			}))
		addStat("log_space_on_disk",
			s.partnInt64Stats(func(ss *IndexStats) int64 {
				return ss.logSpaceOnDisk.Value()
			}))

		// partition stats
		addStat("raw_data_size", rawDataSize)

		// partition stats
		addStat("backstore_raw_data_size",
			s.partnInt64Stats(func(ss *IndexStats) int64 {
				return ss.backstoreRawDataSize.Value()
			}))

		addStat("avg_item_size", computeAvgItemSize(rawDataSize, itemsCount))

		// partition stats
		addStat("key_size_distribution", s.getKeySizeStats())

		if s.isArrayIndex {
			if common.GetStorageMode() == common.PLASMA {
				addStat("arrkey_size_distribution", s.getArrKeySizeStats())
			}

			docidCount := s.partnInt64Stats(func(ss *IndexStats) int64 {
				return ss.docidCount.Value()
			})

			// partition stats
			addStat("docid_count", docidCount)
			addStat("avg_array_length", computeAvgArrayLength(itemsCount, docidCount))
		}

		addStat("key_size_stats_since",
			s.partnMaxInt64Stats(func(ss *IndexStats) int64 {
				return ss.keySizeStatsSince.Value()
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
		addStat("items_count", itemsCount)
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
		addStat("num_open_snapshots",
			s.int64Stats(func(ss *IndexStats) int64 { // Partition and index stat
				return ss.numOpenSnapshots.Value()
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

		addStat("avg_scan_latency", s.avgScanLatency.Value())
		addStat("avg_scan_wait_latency", waitLat)
		addStat("avg_scan_request_latency", scanReqLat)
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
		addStat("num_scan_timeouts",
			s.int64Stats(func(ss *IndexStats) int64 {
				return ss.numScanTimeouts.Value()
			}))
		addStat("num_scan_errors",
			s.int64Stats(func(ss *IndexStats) int64 {
				return ss.numScanErrors.Value()
			}))
		// partition stats
		addStat("avg_scan_rate",
			s.partnInt64Stats(func(ss *IndexStats) int64 {
				return ss.avgScanRate.Value()
			}))
		// partition stats
		addStat("avg_mutation_rate",
			s.partnInt64Stats(func(ss *IndexStats) int64 {
				return ss.avgMutationRate.Value()
			}))
		// partition stats
		addStat("avg_drain_rate",
			s.partnInt64Stats(func(ss *IndexStats) int64 {
				return ss.avgDrainRate.Value()
			}))
		// partition stats
		addStat("avg_disk_bps",
			s.partnInt64Stats(func(ss *IndexStats) int64 {
				return ss.avgDiskBps.Value()
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

		if !essential {

			// Timing stats.  If timing stat is partitioned, the final value
			// is aggreated across the partitions (sum, count, sumOfSq).
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
			addStat("timings/n1ql_expr_eval",
				s.partnTimingStats(func(ss *IndexStats) *stats.TimingStat {
					return &ss.Timings.n1qlExpr
				}))
			addStat("avg_scan_request_init_latency", scanReqInitLat)
			addStat("avg_scan_request_alloc_latency", scanReqAllocLat)
			addStat("num_requests_range",
				s.int64Stats(func(ss *IndexStats) int64 {
					return ss.numRequestsRange.Value()
				}))
			addStat("num_completed_requests_range",
				s.int64Stats(func(ss *IndexStats) int64 {
					return ss.numCompletedRequestsRange.Value()
				}))
			addStat("num_rows_returned_range",
				s.int64Stats(func(ss *IndexStats) int64 {
					return ss.numRowsReturnedRange.Value()
				}))
			addStat("num_rows_scanned_range",
				s.int64Stats(func(ss *IndexStats) int64 {
					return ss.numRowsScannedRange.Value()
				}))
			addStat("scan_cache_hit_range",
				s.int64Stats(func(ss *IndexStats) int64 {
					return ss.scanCacheHitRange.Value()
				}))
			addStat("num_requests_aggr",
				s.int64Stats(func(ss *IndexStats) int64 {
					return ss.numRequestsAggr.Value()
				}))
			addStat("num_completed_requests_aggr",
				s.int64Stats(func(ss *IndexStats) int64 {
					return ss.numCompletedRequestsAggr.Value()
				}))
			addStat("num_rows_returned_aggr",
				s.int64Stats(func(ss *IndexStats) int64 {
					return ss.numRowsReturnedAggr.Value()
				}))
			addStat("num_rows_scanned_aggr",
				s.int64Stats(func(ss *IndexStats) int64 {
					return ss.numRowsScannedAggr.Value()
				}))
			addStat("scan_cache_hit_aggr",
				s.int64Stats(func(ss *IndexStats) int64 {
					return ss.scanCacheHitAggr.Value()
				}))
			addStatByInstId("completion_progress",
				s.int64Stats(func(ss *IndexStats) int64 {
					return ss.completionProgress.Value()
				}))

		}
	}

	for k, s := range is.indexes {

		name := common.FormatIndexInstDisplayName(s.name, s.replicaId)
		prefix = fmt.Sprintf("%s:%s:", s.bucket, name)
		instId = fmt.Sprintf("%v:", k)

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

func (is *IndexerStats) GetVersionedStats(t *target) (common.Statistics, bool) {
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
			if s.name == t.index && s.bucket == t.bucket {
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

func (is *IndexerStats) MarshalJSON(partition bool, pretty bool,
	skipEmpty bool, essential bool) ([]byte, error) {
	stats := is.GetStats(partition, skipEmpty, essential)

	if !pretty {
		return json.Marshal(stats)
	} else {
		return json.MarshalIndent(stats, "", "   ")
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
	clone.buckets = make(map[string]*BucketStats)
	for k, v := range s.indexes {
		clone.indexes[k] = v.clone()
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

	statsPersister           StatsPersister
	statsPersistenceInterval uint64
	exitPersister            uint64
	statsUpdaterStopCh       chan bool
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
	_, valid, _ := common.IsAuthValid(r)
	if !valid {
		w.WriteHeader(401)
		w.Write([]byte("401 Unauthorized"))
		return
	}

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
		// If the caller has requested stats with async = false, caller wants
		// the updated stats. tryUpdateStats will ensure the updated stats.
		if common.IndexerState(stats.indexerState.Value()) != common.INDEXER_BOOTSTRAP && sync == true {
			s.tryUpdateStats(sync)
		}
		bytes, _ := stats.MarshalJSON(partition, pretty, skipEmpty, false)
		w.WriteHeader(200)
		w.Write(bytes)
		stats.statsResponse.Put(time.Since(t0))
	} else {
		w.WriteHeader(400)
		w.Write([]byte("Unsupported method"))
	}
}

func (s *statsManager) handleMemStatsReq(w http.ResponseWriter, r *http.Request) {
	_, valid, _ := common.IsAuthValid(r)
	if !valid {
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
		result += fmt.Sprintf("{\n\"Index\": \"%s:%s\", \"Id\": %d, \"PartitionId\": %d,\n",
			sts.Bucket, sts.Name, sts.InstId, sts.PartnId)
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
	_, valid, _ := common.IsAuthValid(r)
	if !valid {
		w.WriteHeader(401)
		w.Write([]byte("401 Unauthorized"))
		return
	}

	if r.Method == "POST" || r.Method == "GET" {

		stats := s.stats.Get()

		if common.IndexerState(stats.indexerState.Value()) != common.INDEXER_BOOTSTRAP {
			w.WriteHeader(200)
			w.Write([]byte(s.getStorageStats()))
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
	_, valid, _ := common.IsAuthValid(r)
	if !valid {
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
	_, valid, _ := common.IsAuthValid(r)
	if !valid {
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

func (s *statsManager) runStatsDumpLogger() {
	skipStorage := 0
	essential := true
	for {
		stats := s.stats.Get()
		if stats != nil {
			if logging.IsEnabled(logging.Verbose) {
				essential = false
			}
			bytes, _ := stats.MarshalJSON(false, false, false, essential)
			var storageStats string
			if skipStorage > 15 { //log storage stats every 15mins
				if common.GetStorageMode() != common.FORESTDB {
					storageStats = fmt.Sprintf("\n==== StorageStats ====\n%s", s.getStorageStats())
				} else if logging.IsEnabled(logging.Timing) {
					storageStats = fmt.Sprintf("\n==== StorageStats ====\n%s", s.getStorageStats())
				}
				logging.Infof("PeriodicStats = %s%s", string(bytes), storageStats)
				skipStorage = 0
			} else {
				logging.Infof("PeriodicStats = %s", string(bytes))
				skipStorage++
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
