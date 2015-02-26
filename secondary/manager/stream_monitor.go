// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package manager

import (
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/common"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
	"sync"
	"time"
)

/////////////////////////////////////////////////////////////////////////
// Type Definition
/////////////////////////////////////////////////////////////////////////

type StreamMonitor struct {
	manager         *IndexManager
	timer           *Timer
	activeMap       map[common.StreamId]map[string][]bool
	startTimestamps map[common.StreamId]map[string]*common.TsVbuuid
	mutex           sync.Mutex
	killch          chan (bool)
}

/////////////////////////////////////////////////////////////////////////
// StreamMonitor - Public Function
/////////////////////////////////////////////////////////////////////////

func NewStreamMonitor(manager *IndexManager, timer *Timer) *StreamMonitor {
	return &StreamMonitor{
		manager:         manager,
		timer:           timer,
		activeMap:       make(map[common.StreamId]map[string][]bool),
		startTimestamps: make(map[common.StreamId]map[string]*common.TsVbuuid),
		killch:          make(chan bool, 1)}
}

func (m *StreamMonitor) Close() {
	if len(m.killch) == 0 {
		m.killch <- true
	}
}

func (m *StreamMonitor) Start() {
	logging.Debugf("StreamMonitor.Start()")
	go m.monitor()
}

func (m *StreamMonitor) StartStream(streamId common.StreamId, bucket string, timestamp *protobuf.TsVbuuid) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	logging.Debugf("StreamMonitor.StartStream() : streamId %d bucket %s #seqNo %d", streamId, bucket, len(timestamp.GetSeqnos()))

	bucketMap, ok := m.startTimestamps[streamId]
	if !ok {
		bucketMap = make(map[string]*common.TsVbuuid)
		m.startTimestamps[streamId] = bucketMap
	}

	ts, ok := bucketMap[bucket]
	if !ok {
		ts = common.NewTsVbuuid(bucket, NUM_VB)
		bucketMap[bucket] = ts
	}

	for i, vb := range timestamp.GetVbnos() {
		ts.Seqnos[vb] = timestamp.GetSeqnos()[i]
		ts.Vbuuids[vb] = timestamp.GetVbuuids()[i]
	}
}

func (m *StreamMonitor) StopStream(streamId common.StreamId, bucket string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	bucketMap, ok := m.startTimestamps[streamId]
	if !ok {
		return
	}

	delete(bucketMap, bucket)
}

func (m *StreamMonitor) Activate(streamId common.StreamId, bucket string, vb uint16) {

	m.mutex.Lock()
	defer m.mutex.Unlock()

	logging.Debugf("StreamMonitor.Activate() : streamId %d bucket %s vb %d", streamId, bucket, vb)

	bucketMap, ok := m.activeMap[streamId]
	if !ok {
		bucketMap = make(map[string][]bool)
		m.activeMap[streamId] = bucketMap
	}

	activeArr, ok := bucketMap[bucket]
	if !ok {
		activeArr = make([]bool, NUM_VB)
		bucketMap[bucket] = activeArr
	}

	activeArr[vb] = true
}

func (m *StreamMonitor) Deactivate(streamId common.StreamId, bucket string, vb uint16) {

	m.mutex.Lock()
	defer m.mutex.Unlock()

	logging.Debugf("StreamMonitor.Deactivate() : streamId %d bucket %s vb %d", streamId, bucket, vb)

	bucketMap, ok := m.activeMap[streamId]
	if !ok {
		bucketMap = make(map[string][]bool)
		m.activeMap[streamId] = bucketMap
	}

	activeArr, ok := bucketMap[bucket]
	if !ok {
		activeArr = make([]bool, NUM_VB)
		bucketMap[bucket] = activeArr
	}

	activeArr[vb] = false
}

/////////////////////////////////////////////////////////////////////////
// StreamMonitor - Private Function
/////////////////////////////////////////////////////////////////////////

func (m *StreamMonitor) monitor() {

	logging.Debugf("StreamMonitor::Monitor(): start")

	ticker := time.NewTicker(MONITOR_INTERVAL)
	defer ticker.Stop()

	for {
		select {
		case <-m.killch:
			return
		case <-ticker.C:
			m.repair()
		}
	}
}

func (m *StreamMonitor) isActive(streamId common.StreamId, bucket string, vb uint16) bool {

	bucketMap, ok := m.activeMap[streamId]
	if !ok {
		return false
	}

	activeArr, ok := bucketMap[bucket]
	if !ok {
		return false
	}

	return activeArr[vb]
}

func (m *StreamMonitor) repair() {

	toRetry := make(map[common.StreamId]map[string]*common.TsVbuuid)

	func() {
		m.mutex.Lock()
		defer m.mutex.Unlock()

		for streamId, buckets := range m.startTimestamps {
			for bucket, ts := range buckets {
				for vb, _ := range ts.Seqnos {
					if !m.isActive(streamId, bucket, uint16(vb)) {

						logging.Debugf("StreamMonitor.repair() : not active -> streamId %d bucket %s vb %d",
							streamId, bucket, vb)
						retryBuckets, ok := toRetry[streamId]
						if !ok {
							retryBuckets = make(map[string]*common.TsVbuuid)
							toRetry[streamId] = retryBuckets
						}

						retryTs, ok := retryBuckets[bucket]
						if !ok {
							retryTs = common.NewTsVbuuid(bucket, NUM_VB)
							retryBuckets[bucket] = retryTs
						}

						retryTs.Seqnos[vb], retryTs.Vbuuids[vb] = m.findRestartSeqno(streamId, bucket, uint16(vb))
						logging.Debugf("StreamMonitor.repair() : ts to retry -> seqno %d vbuuid %d",
							retryTs.Seqnos[vb], retryTs.Vbuuids[vb])
					}
				}
			}
		}
	}()

	for streamId, buckets := range toRetry {
		timestamps := make([]*common.TsVbuuid, 0, len(buckets))
		for _, ts := range buckets {
			timestamps = append(timestamps, ts)
		}
		logging.Debugf("StreamMonitor.repair() : streamId %s len(timetamps) %d", streamId, len(timestamps))
		if err := m.manager.streamMgr.RestartStreamIfNecessary(streamId, timestamps); err != nil {
			logging.Debugf("StreamMonitor::Repair(): error %s", err)
		}
	}
}

func (m *StreamMonitor) findRestartSeqno(streamId common.StreamId, bucket string, vb uint16) (uint64, uint64) {

	// First check if the timer has a timestamp.
	currentTs := m.timer.getLatest(streamId, bucket)
	if currentTs != nil {
		if currentTs.Seqnos[vb] != 0 {
			return currentTs.Seqnos[vb], currentTs.Vbuuids[vb]
		}
	}

	// Find out the restarat timestamp that is used to start the projector
	buckets, ok := m.startTimestamps[streamId]
	if ok {
		startTs, ok := buckets[bucket]
		if ok {
			return startTs.Seqnos[vb], startTs.Vbuuids[vb]
		}
	}

	// start from seqno 0
	return uint64(0), uint64(0)
}
