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
	"encoding/base64"
	"encoding/json"
	"github.com/couchbase/indexing/secondary/common"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
	"github.com/couchbaselabs/goprotobuf/proto"
	"sync"
	"time"
)

/////////////////////////////////////////////////////////////////////////
// Type Definition
/////////////////////////////////////////////////////////////////////////

type timestampHistory struct {
	history []*common.TsVbuuid
	current int
	mutex   sync.Mutex
	last    *common.TsVbuuid
}

type timestampHistoryBucketMap map[string]*timestampHistory
type tickerBucketMap map[string]*time.Ticker
type stopchBucketMap map[string]chan bool

type Timer struct {
	timestamps map[common.StreamId]timestampHistoryBucketMap
	tickers    map[common.StreamId]tickerBucketMap
	stopchs    map[common.StreamId]stopchBucketMap
	outch      chan *timestampSerializable

	mutex sync.Mutex
	ready bool
}

type timestampSerializable struct {
	StreamId  uint16 `json:"streamId,omitempty"`
	Bucket    string `json:"bucket,omitempty"`
	Timestamp string `json:"timestamp,omitempty"`
}

type timestampListSerializable struct {
	Timestamps []timestampSerializable `json:"timestamps,omitempty"`
}

/////////////////////////////////////////////////////////////////////////
// Package Local Function : Timer
/////////////////////////////////////////////////////////////////////////

//
// Create a timer that keeps track of the timestamp history across streams and buckets
//
func newTimer(repo *MetadataRepo) *Timer {

	timestamps := make(map[common.StreamId]timestampHistoryBucketMap)
	tickers := make(map[common.StreamId]tickerBucketMap)
	stopchs := make(map[common.StreamId]stopchBucketMap)
	outch := make(chan *timestampSerializable, TIMESTAMP_CHANNEL_SIZE)

	timer := &Timer{timestamps: timestamps,
		tickers: tickers,
		stopchs: stopchs,
		outch:   outch,
		ready:   false}

	savedTimestamps, err := repo.GetStabilityTimestamps()
	if err == nil {
		for _, timestamp := range savedTimestamps.Timestamps {
			ts, err := unmarshallTimestamp(timestamp.Timestamp)
			if err != nil {
				common.Errorf("Timer.newTimer() : unable to unmarshall timestamp for bucket %v.  Skip initialization.",
					timestamp.Bucket)
				continue
			}
			timer.start(common.StreamId(timestamp.StreamId), timestamp.Bucket)
			for vb, seqno := range ts.Seqnos {
				timer.increment(common.StreamId(timestamp.StreamId), timestamp.Bucket, uint32(vb), ts.Vbuuids[vb], seqno)
			}
			common.Errorf("Timer.newTimer() : initialized timestamp for bucket %v from repository.", timestamp.Bucket)
		}
	} else {
		// TODO : Determine timestamp not exist versus forestdb error
		common.Errorf("Timer.newTimer() : cannot get stability timestamp from repository. Skip initialization.")
	}

	return timer
}

//
// Get Output Channel
//
func (t *Timer) getOutputChannel() <-chan *timestampSerializable {

	return t.outch
}

//
// Stop all the timers
//
func (t *Timer) stopAll() {

	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.ready {
		t.ready = false

		// Stop the timer goroutine
		for streamId, stopchMap := range t.stopchs {
			for _, stopch := range stopchMap {
				close(stopch)
			}
			delete(t.stopchs, streamId)
		}

		// Remove the ticker
		for streamId, _ := range t.tickers {
			delete(t.tickers, streamId)
		}

		// Remove the timestamp
		for streamId, _ := range t.timestamps {
			delete(t.timestamps, streamId)
		}

		close(t.outch)
	}
}

//
// Start timer for a specific stream and bucket.  If the timer
// has already started, this is an no-op.
//
func (t *Timer) start(streamId common.StreamId, bucket string) {

	t.mutex.Lock()
	defer t.mutex.Unlock()

	stopchMap, ok := t.stopchs[streamId]
	if !ok {
		stopchMap = make(stopchBucketMap)
		t.stopchs[streamId] = stopchMap
	}

	stopch, ok := stopchMap.get(bucket)
	if !ok {
		stopch = make(chan bool)
		stopchMap.set(bucket, stopch)
	}

	tickerMap, ok := t.tickers[streamId]
	if !ok {
		tickerMap = make(tickerBucketMap)
		t.tickers[streamId] = tickerMap
	}

	ticker, ok := tickerMap.get(bucket)
	if !ok {
		ticker = time.NewTicker(TIME_INTERVAL)
		tickerMap.set(bucket, ticker)
		go t.run(streamId, bucket, ticker, stopch)
	}

	t.ready = true
}

//
// Stop timer for a specific stream and bucket.  If the timer
// has not started, this is an no-op.
//
func (t *Timer) stop(streamId common.StreamId, bucket string) {

	t.mutex.Lock()
	defer t.mutex.Unlock()

	// close the stopch for the timer goroutine to stop.  This will also cause the ticker to stop.
	if stopchMap, ok := t.stopchs[streamId]; ok {
		if stopch, ok := stopchMap.get(bucket); ok {
			close(stopch)
			stopchMap.remove(bucket)
		}
	}

	// remove the ticker
	if tickerMap, ok := t.tickers[streamId]; ok {
		if _, ok := tickerMap.get(bucket); ok {
			// do not need to stop the ticker here
			tickerMap.remove(bucket)
		}
	}

	// remove the timestamp
	if tsMap, ok := t.timestamps[streamId]; ok {
		if _, ok := tsMap.get(bucket); ok {
			tsMap.remove(bucket)
		}
	}
}

//
// Stop timer for a specific stream.  If the timer
// has not started, this is an no-op.
//
func (t *Timer) stopForStream(streamId common.StreamId) {

	t.mutex.Lock()
	defer t.mutex.Unlock()

	// Stop the timer goroutine
	if stopchMap, ok := t.stopchs[streamId]; ok {
		for _, stopch := range stopchMap {
			close(stopch)
		}
		delete(t.stopchs, streamId)
	}

	// Remove the ticker
	if _, ok := t.tickers[streamId]; ok {
		delete(t.tickers, streamId)
	}

	// Remove timestamp
	if _, ok := t.timestamps[streamId]; ok {
		delete(t.timestamps, streamId)
	}
}

//
// Get the latest timestamp
//
func (t *Timer) getLatest(streamId common.StreamId, bucket string) *common.TsVbuuid {

	t.mutex.Lock()
	defer t.mutex.Unlock()

	if !t.ready {
		return nil
	}

	bucketMap, ok := t.timestamps[streamId]
	if !ok {
		return nil
	}

	history, ok := bucketMap.get(bucket)
	if !ok {
		return nil
	}

	return history.getLatest()
}

//
// Increment the logical time for the given (stream, bucket, vbucket)
//
func (t *Timer) increment(streamId common.StreamId, bucket string, vbucket uint32, vbuuid uint64, seqno uint64) {

	t.mutex.Lock()
	defer t.mutex.Unlock()

	if !t.ready {
		return
	}

	bucketMap, ok := t.timestamps[streamId]
	if !ok {
		bucketMap = make(timestampHistoryBucketMap)
		t.timestamps[streamId] = bucketMap
	}

	history, ok := bucketMap.get(bucket)
	if !ok {
		history = newTimestampHistory(bucket)
		bucketMap.set(bucket, history)
	}

	history.increment(vbucket, vbuuid, seqno)
}

//
// Advance the clock to the next timestamp
//
func (t *Timer) advance(streamId common.StreamId, bucket string) (*common.TsVbuuid, bool) {

	t.mutex.Lock()
	defer t.mutex.Unlock()

	if !t.ready {
		return nil, false
	}

	bucketMap, ok := t.timestamps[streamId]
	if !ok {
		return nil, false
	}

	history, ok := bucketMap.get(bucket)
	if !ok {
		return nil, false
	}

	return history.advance()
}

//
// Get the time signal to freeze the stability timestamp
//
func (t *Timer) run(streamId common.StreamId, bucket string, ticker *time.Ticker, stopch chan bool) {

	common.Debugf("timer.run(): Start for bucket %v", bucket)

	defer ticker.Stop()

	for {
		select {
		// Make sure the stopch is the first one in select.
		case <-stopch:
			common.Debugf("timer.run(): Coordinator timer for bucket %v being explicitly stopped by supervisor.", bucket)
			return

		case <-ticker.C:
			// wrap it around a function just to make sure panic is caught so the timer go-routine does
			// not die unexpectedly.
			func() {
				defer func() {
					if r := recover(); r != nil {
						common.Debugf("panic in Timer.run() : error ignored.  Error = %v\n", r)
					}
				}()

				ts, ok := t.advance(streamId, bucket)
				if ok && len(t.outch) < TIMESTAMP_CHANNEL_SIZE {
					// Make sure that this call is not blocking.  It is OK to drop
					// the timestamp is the channel receiver is slow.
					wrapper, err := createTimestampSerializable(ts, streamId)
					if err != nil {
						common.Debugf("timer.run(): Unable to create wrapper for timestamp.  Skip timestamp.")
					} else {
						common.Debugf("timer.run(): Sending timestamp to channel for bucket %v", bucket)
						t.outch <- wrapper
					}
				}
			}()
		}
	}
}

/////////////////////////////////////////////////////////////////////////
// Private Function : timestampHistoryBucketMap
/////////////////////////////////////////////////////////////////////////

func (m timestampHistoryBucketMap) get(bucket string) (*timestampHistory, bool) {
	result, ok := m[bucket]
	return result, ok
}

func (m timestampHistoryBucketMap) set(bucket string, history *timestampHistory) {
	m[bucket] = history
}

func (m timestampHistoryBucketMap) remove(bucket string) {
	delete(m, bucket)
}

/////////////////////////////////////////////////////////////////////////
// Private Function : stopchBucketMap
/////////////////////////////////////////////////////////////////////////

func (m stopchBucketMap) get(bucket string) (chan bool, bool) {
	result, ok := m[bucket]
	return result, ok
}

func (m stopchBucketMap) set(bucket string, stopch chan bool) {
	m[bucket] = stopch
}

func (m stopchBucketMap) remove(bucket string) {
	delete(m, bucket)
}

/////////////////////////////////////////////////////////////////////////
// Private Function : tickerBucketMap
/////////////////////////////////////////////////////////////////////////

func (m tickerBucketMap) get(bucket string) (*time.Ticker, bool) {
	result, ok := m[bucket]
	return result, ok
}

func (m tickerBucketMap) set(bucket string, ticker *time.Ticker) {
	m[bucket] = ticker
}

func (m tickerBucketMap) remove(bucket string) {
	delete(m, bucket)
}

/////////////////////////////////////////////////////////////////////////
// Private Function : timestampHistory
/////////////////////////////////////////////////////////////////////////

//
// Create a timestamp history for a particular stream, bucket
//
func newTimestampHistory(bucket string) *timestampHistory {

	result := &timestampHistory{history: make([]*common.TsVbuuid, TIMESTAMP_HISTORY_COUNT),
		last:    nil,
		current: 0}

	result.history[result.current] = common.NewTsVbuuid(bucket, NUM_VB)

	return result
}

//
// Increment the timestamp for a given <streamId, bucket>
//
func (t *timestampHistory) increment(vbucket uint32, vbuuid uint64, seqno uint64) {

	t.mutex.Lock()
	defer t.mutex.Unlock()

	timestamp := t.history[t.current]

	if timestamp.Vbuuids[vbucket] != 0 && timestamp.Vbuuids[vbucket] == vbuuid {
		if timestamp.Seqnos[vbucket] == 0 || timestamp.Seqnos[vbucket] < seqno {
			timestamp.Seqnos[vbucket] = seqno
			timestamp.Vbuuids[vbucket] = vbuuid

			common.Debugf("timestampHistory.increment(): increment timestamp: bucket %v : vb id : %d, seqno : %d, vbuuid : %d",
				timestamp.Bucket, vbucket, seqno, vbuuid)
		}
	} else {
		timestamp.Seqnos[vbucket] = seqno
		timestamp.Vbuuids[vbucket] = vbuuid

		common.Debugf("timestampHistory.increment(): increment timestamp: bucket %v : vb id : %d, seqno : %d, vbuuid : %d",
			timestamp.Bucket, vbucket, seqno, vbuuid)
	}

}

//
// Get the next timestamp
//
func (t *timestampHistory) advance() (*common.TsVbuuid, bool) {

	t.mutex.Lock()
	defer t.mutex.Unlock()

	return t.advanceNoLock()
}

//
// Get the next timestamp
//
func (t *timestampHistory) advanceNoLock() (*common.TsVbuuid, bool) {

	if !t.isReady() {
		return nil, false
	}

	result := t.history[t.current]
	t.current = t.current + 1
	if t.current >= len(t.history) {
		t.current = 0
	}
	t.history[t.current] = result.Clone()

	equal := result.Equal(t.last)
	t.last = result

	return result, equal
}

//
// Get the latest timestamp
//
func (t *timestampHistory) getLatest() *common.TsVbuuid {

	t.mutex.Lock()
	defer t.mutex.Unlock()

	return t.history[t.current]
}

//
// Is the current timestamp has seqno for all vb?
//
func (t *timestampHistory) isReady() bool {
	current := t.history[t.current]
	for i, seqno := range current.Seqnos {
		if seqno == 0 {
			common.Errorf("timestampHistory.isReady() : not ready : vb %d seqno %d", i, seqno)
			return false
		}
	}

	return true
}

/////////////////////////////////////////////////////////////////////////
// Private Function : TimestampSerializable
/////////////////////////////////////////////////////////////////////////

func createTimestampSerializable(ts *common.TsVbuuid, streamId common.StreamId) (*timestampSerializable, error) {

	data, err := marshallTimestamp(ts)
	if err != nil {
		return nil, err
	}

	return &timestampSerializable{StreamId: uint16(streamId), Bucket: ts.Bucket, Timestamp: data}, nil
}

func marshallTimestampSerializable(wrapper *timestampSerializable) ([]byte, error) {

	buf, err := json.Marshal(&wrapper)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func unmarshallTimestampSerializable(data []byte) (*timestampSerializable, error) {

	wrapper := new(timestampSerializable)
	if err := json.Unmarshal(data, wrapper); err != nil {
		return nil, err
	}

	return wrapper, nil
}

func marshallTimestamp(input *common.TsVbuuid) (string, error) {

	ts := protobuf.NewTsVbuuid(DEFAULT_POOL_NAME, input.Bucket, NUM_VB)
	ts = ts.FromTsVbuuid(input)
	buf, err := proto.Marshal(ts)
	if err != nil {
		return "", err
	}

	str := base64.StdEncoding.EncodeToString(buf)
	return str, nil
}

func unmarshallTimestamp(str string) (*common.TsVbuuid, error) {

	data, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return nil, err
	}

	source := new(protobuf.TsVbuuid)
	if err := proto.Unmarshal(data, source); err != nil {
		return nil, err
	}

	target := common.NewTsVbuuid(source.GetBucket(), NUM_VB)

	for _, vbno := range source.Vbnos {
		target.Seqnos[vbno] = source.Seqnos[vbno]
		target.Vbuuids[vbno] = source.Vbuuids[vbno]
	}

	return target, nil
}

/////////////////////////////////////////////////////////////////////////
// Private Function : TimestampListSerializable
/////////////////////////////////////////////////////////////////////////

func createTimestampListSerializable() *timestampListSerializable {

	return new(timestampListSerializable)
}

func marshallTimestampListSerializable(list *timestampListSerializable) ([]byte, error) {

	buf, err := json.Marshal(&list)
	if err != nil {
		return nil, err
	}

	common.Debugf("marshallTimestampListSerializable() : serialized timestamp list in bytes %d.", len(buf))
	return buf, nil
}

func unmarshallTimestampListSerializable(data []byte) (*timestampListSerializable, error) {

	list := new(timestampListSerializable)
	if err := json.Unmarshal(data, list); err != nil {
		return nil, err
	}

	return list, nil
}

func (l *timestampListSerializable) addTimestamp(timestamp *timestampSerializable) {

	for i, t := range l.Timestamps {
		if t.Bucket == timestamp.Bucket && t.StreamId == timestamp.StreamId {
			l.Timestamps[i] = *timestamp
			return
		}
	}

	l.Timestamps = append(l.Timestamps, *timestamp)
}

func (l *timestampListSerializable) removeTimestamp(streamId common.StreamId, bucket string) {

	for i, t := range l.Timestamps {
		if t.Bucket == bucket && common.StreamId(t.StreamId) == streamId {
			if i < len(l.Timestamps)-1 {
				l.Timestamps = append(l.Timestamps[0:i], l.Timestamps[i+1:]...)
			} else {
				l.Timestamps = l.Timestamps[0:i]
			}
			return
		}
	}
}

func (l *timestampListSerializable) findTimestamp(streamId common.StreamId, bucket string, vb uint16) (uint64, uint64, bool, error) {

	for _, t := range l.Timestamps {
		if t.Bucket == bucket && common.StreamId(t.StreamId) == streamId {

			common.Debugf("timestampListSerializable.findTimestamp() : found timestamp for streamId %v bucket %v.",
				streamId, bucket)

			ts, err := unmarshallTimestamp(t.Timestamp)
			if err != nil {
				common.Errorf("timestampListSerializable.findTimestamp() : unable to unmarshall timestamp for bucket %v.",
					t.Bucket)
				return 0, 0, false, err
			}

			common.Debugf("timestampListSerializable.findTimestamp() : seqNo for vb %d is %d.", vb, ts.Seqnos[vb])

			return ts.Seqnos[vb], ts.Vbuuids[vb], true, nil
		}
	}

	return 0, 0, false, nil
}

func (l *timestampListSerializable) DebugPrint() {

	if common.LogLevel() != common.LogLevelDebug {
		return
	}

	common.Debugf("timestampListSerializable.DebugPrint() : len(timestamps) = %d", len(l.Timestamps))

	for _, t := range l.Timestamps {
		common.Debugf("timestampListSerializable.DebugPrint() : ----------")
		common.Debugf("timestampListSerializable.DebugPrint() : bucket %s", t.Bucket)
		common.Debugf("timestampListSerializable.DebugPrint() : streamId %d", t.StreamId)
		ts, err := unmarshallTimestamp(t.Timestamp)
		if err != nil {
			common.Errorf("timestampListSerializable.debugPrint() : unable to unmarshall timestamp for bucket")
		} else {
			ts.DebugPrint()
		}
	}
}
