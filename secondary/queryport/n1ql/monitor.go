//  Copyright (c) 2022 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.
//  Reference: https://github.com/couchbase/n1fty/blob/master/monitor.go

package n1ql

import (
	"io/ioutil"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	l "github.com/couchbase/indexing/secondary/logging"
)

// ----------------------------------------------------------------------------

var gsiMonitor *monitor
var gsiMonitorInitOnce sync.Once

type monitor struct {
	m        sync.RWMutex
	indexers map[string]*gsiKeyspace
}

func initGSIMonitor(backfillTime, logStatsTime time.Duration) {
	gsiMonitor = &monitor{
		indexers: make(map[string]*gsiKeyspace),
	}
	go gsiMonitor.backfillMonitor(backfillTime)
	go gsiMonitor.logStats(logStatsTime)
}

func getKeyForIndexer(gsi *gsiKeyspace) string {
	return strings.Join([]string{gsi.BucketId(), gsi.ScopeId(), gsi.KeyspaceId()}, ":")
}

// ----------------------------------------------------------------------------

func MonitorIndexer(gsi *gsiKeyspace, backfillTime, logStatsTime time.Duration) {
	gsiMonitorInitOnce.Do(func() {
		initGSIMonitor(backfillTime, logStatsTime)
	})

	if gsi != nil {
		gsiMonitor.m.Lock()
		defer gsiMonitor.m.Unlock()

		key := getKeyForIndexer(gsi)
		if _, ok := gsiMonitor.indexers[key]; ok {
			l.Warnf("MonitorIndexer: Indexer for %v is already being monitored", key)
		}
		gsiMonitor.indexers[key] = gsi
		l.Tracef("MonitorIndexer: Monitoring indexer %v", key)
	}
}

func UnmonitorIndexer(gsi *gsiKeyspace) {
	if gsi != nil {
		gsiMonitor.m.Lock()
		defer gsiMonitor.m.Unlock()

		key := getKeyForIndexer(gsi)
		if _, ok := gsiMonitor.indexers[key]; !ok {
			l.Warnf("UnmonitorIndexer: Indexer for %v does not exist", key)
			return
		}
		delete(gsiMonitor.indexers, key)
		l.Tracef("UnmonitorIndexer: UnMonitoring indexer %v", key)
	}
}

// ----------------------------------------------------------------------------

// Blocking method; To be spun off as a goroutine
func (m *monitor) backfillMonitor(period time.Duration) {
	tick := time.NewTicker(period)
	defer tick.Stop()

	for {
		<-tick.C

		n1ql_backfill_temp_dir := getTmpSpaceDir()
		files, err := ioutil.ReadDir(n1ql_backfill_temp_dir)
		if err != nil {
			l.Warnf("monitor::backfillMonitor failed to read dir %v,"+
				" err: %v", n1ql_backfill_temp_dir, err)
			continue
		}

		size := int64(0)
		for _, file := range files {
			fname := path.Join(n1ql_backfill_temp_dir, file.Name())
			if strings.Contains(fname, BACKFILLPREFIX) {
				size += int64(file.Size())
			}
		}

		m.m.RLock()
		for _, gsi := range m.indexers {
			atomic.StoreInt64(&gsi.backfillSize, size)
		}
		m.m.RUnlock()

	}
}

// Blocking method; To be spun off as a goroutine
func (m *monitor) logStats(logtick time.Duration) {
	tick := time.NewTicker(logtick)
	defer tick.Stop()

	for {
		<-tick.C

		m.m.RLock()
		for _, gsi := range m.indexers {
			totalscans := atomic.LoadInt64(&gsi.totalscans)
			prevTotalScans := atomic.LoadInt64(&gsi.prevTotalScans)
			if totalscans == prevTotalScans {
				continue
			}

			scandur := atomic.LoadInt64(&gsi.scandur)
			blockeddur := atomic.LoadInt64(&gsi.blockeddur)
			throttledur := atomic.LoadInt64(&gsi.throttledur)
			primedur := atomic.LoadInt64(&gsi.primedur)
			totalbackfills := atomic.LoadInt64(&gsi.totalbackfills)
			backfillSize := atomic.LoadInt64(&gsi.backfillSize)

			fmsg := `%v logstats %q {` +
				`"gsi_scan_count":%v,"gsi_scan_duration":%v,` +
				`"gsi_throttle_duration":%v,` +
				`"gsi_prime_duration":%v,"gsi_blocked_duration":%v,` +
				`"gsi_total_temp_files":%v,"gsi_backfill_size":%v}`
			l.Infof(
				fmsg, gsi.logPrefix, gsi.bucket, totalscans, scandur,
				throttledur, primedur, blockeddur, totalbackfills, backfillSize)

			atomic.StoreInt64(&gsi.prevTotalScans, totalscans)
		}
		m.m.RUnlock()

	}
}
