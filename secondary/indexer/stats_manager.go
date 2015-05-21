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
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/platform"
	"github.com/couchbase/indexing/secondary/stats"
	"net/http"
	"runtime"
	"sync"
	"time"
	"unsafe"
)

type IndexStats struct {
	name, bucket string

	scanDuration     stats.Int64Val
	insertBytes      stats.Int64Val
	numDocsPending   stats.Int64Val
	scanWaitDuration stats.Int64Val
	numDocsIndexed   stats.Int64Val
	numRequests      stats.Int64Val
	numRowsReturned  stats.Int64Val
	diskSize         stats.Int64Val
	buildProgress    stats.Int64Val
	numDocsQueued    stats.Int64Val
	deleteBytes      stats.Int64Val
	dataSize         stats.Int64Val
	scanBytesRead    stats.Int64Val
	getBytes         stats.Int64Val
	itemsCount       stats.Int64Val
	numCommits       stats.Int64Val
	numSnapshots     stats.Int64Val
	numCompactions   stats.Int64Val

	avgTsInterval stats.Int64Val
	lastTsTime    stats.Int64Val
}

type IndexerStatsHolder struct {
	ptr unsafe.Pointer
}

func (h IndexerStatsHolder) Get() *IndexerStats {
	return (*IndexerStats)(platform.LoadPointer(&h.ptr))
}

func (h *IndexerStatsHolder) Set(s *IndexerStats) {
	platform.StorePointer(&h.ptr, unsafe.Pointer(s))
}

func (s *IndexStats) Init() {
	s.scanDuration.Init()
	s.insertBytes.Init()
	s.numDocsPending.Init()
	s.scanWaitDuration.Init()
	s.numDocsIndexed.Init()
	s.numRequests.Init()
	s.numRowsReturned.Init()
	s.diskSize.Init()
	s.buildProgress.Init()
	s.numDocsQueued.Init()
	s.deleteBytes.Init()
	s.dataSize.Init()
	s.scanBytesRead.Init()
	s.getBytes.Init()
	s.itemsCount.Init()
	s.avgTsInterval.Init()
	s.lastTsTime.Init()
	s.numCommits.Init()
	s.numSnapshots.Init()
	s.numCompactions.Init()
}

type IndexerStats struct {
	indexes map[common.IndexInstId]*IndexStats

	numConnections stats.Int64Val
	memoryQuota    stats.Int64Val
	memoryUsed     stats.Int64Val
	needsRestart   stats.BoolVal
}

func (s *IndexerStats) Init() {
	s.indexes = make(map[common.IndexInstId]*IndexStats)
	s.numConnections.Init()
	s.memoryQuota.Init()
	s.memoryUsed.Init()
	s.needsRestart.Init()
}

func (s *IndexerStats) AddIndex(id common.IndexInstId, bucket string, name string) {
	idxStats := &IndexStats{name: name, bucket: bucket}
	idxStats.Init()
	s.indexes[id] = idxStats
}

func (s *IndexerStats) RemoveIndex(id common.IndexInstId) {
	delete(s.indexes, id)
}

func (is IndexerStats) MarshalJSON() ([]byte, error) {
	var prefix string

	statsMap := make(map[string]interface{})
	addStat := func(k string, v interface{}) {
		statsMap[fmt.Sprintf("%s%s", prefix, k)] = v
	}

	addStat("num_connections", is.numConnections.Value())
	addStat("memory_quota", is.memoryQuota.Value())
	addStat("memory_used", is.memoryUsed.Value())
	addStat("needs_restart", is.needsRestart.Value())

	for _, s := range is.indexes {
		prefix = fmt.Sprintf("%s:%s:", s.bucket, s.name)
		addStat("total_scan_duration", s.scanDuration.Value())
		addStat("insert_bytes", s.insertBytes.Value())
		addStat("num_docs_pending", s.numDocsPending.Value())
		addStat("scan_wait_duration", s.scanWaitDuration.Value())
		addStat("num_docs_indexed", s.numDocsIndexed.Value())
		addStat("num_requests", s.numRequests.Value())
		addStat("num_rows_returned", s.numRowsReturned.Value())
		addStat("disk_size", s.diskSize.Value())
		addStat("build_progress", s.buildProgress.Value())
		addStat("num_docs_queued", s.numDocsQueued.Value())
		addStat("delete_bytes", s.deleteBytes.Value())
		addStat("data_size", s.dataSize.Value())
		addStat("scan_bytes_read", s.scanBytesRead.Value())
		addStat("get_bytes", s.getBytes.Value())
		addStat("items_count", s.itemsCount.Value())
		addStat("avg_ts_interval", s.avgTsInterval.Value())
		addStat("num_commits", s.numCommits.Value())
		addStat("num_snapshots", s.numSnapshots.Value())
		addStat("num_compactions", s.numCompactions.Value())
	}

	return json.Marshal(statsMap)
}

func (s IndexerStats) Clone() *IndexerStats {
	var clone IndexerStats
	clone = s
	clone.indexes = make(map[common.IndexInstId]*IndexStats)
	for k, v := range s.indexes {
		clone.indexes[k] = v
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
	conf                  common.Config
	stats                 IndexerStatsHolder
	supvCmdch             MsgChannel
	supvMsgch             MsgChannel
	lastStatTime          time.Time
	cacheUpdateInProgress bool
}

func NewStatsManager(supvCmdch MsgChannel,
	supvMsgch MsgChannel, config common.Config) (statsManager, Message) {
	s := statsManager{
		conf:         config,
		supvCmdch:    supvCmdch,
		supvMsgch:    supvMsgch,
		lastStatTime: time.Unix(0, 0),
	}

	http.HandleFunc("/stats", s.handleStatsReq)
	http.HandleFunc("/stats/mem", s.handleMemStatsReq)
	go s.run()
	return s, &MsgSuccess{}
}

func (s *statsManager) tryUpdateStats(sync bool) {
	waitCh := make(chan struct{})
	timeout := time.Millisecond * time.Duration(s.conf["stats_cache_timeout"].Uint64())

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
					mType:  t,
					respch: ch,
				}

				s.supvMsgch <- msg
				<-ch
			}

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
	if r.Method == "POST" || r.Method == "GET" {
		if r.URL.Query().Get("async") == "false" {
			sync = true
		}
		s.tryUpdateStats(sync)
		s.Lock()
		stats := s.stats.Get()
		bytes, _ := stats.MarshalJSON()
		s.Unlock()
		w.WriteHeader(200)
		w.Write(bytes)
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
