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
	"container/list"
	"sync"
)

//SnapshotContainer manages snapshots for a Slice
type SnapshotContainer interface {
	Add(Snapshot)
	RemoveOldest()
	Len() int

	GetLatestSnapshot() Snapshot
	GetSnapshotEqualToTS(Timestamp) Snapshot
	GetSnapshotRecentThanTS(Timestamp) Snapshot
}

type snapshotContainer struct {
	snapshotList *list.List
	lock         sync.RWMutex
}

//NewSnapshotContainer inits a new snapshotContainer and returns
func NewSnapshotContainer() *snapshotContainer {
	sc := &snapshotContainer{snapshotList: list.New()}
	return sc
}

//Add adds snapshot to container
func (sc *snapshotContainer) Add(s Snapshot) {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	sc.snapshotList.PushFront(s)
}

//Remove removes a snapshot from container
func (sc *snapshotContainer) RemoveOldest() {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	e := sc.snapshotList.Back()

	if e == nil {
		return
	} else {
		sc.snapshotList.Remove(e)
	}
}

//Len returns the number of snapshots currently in container
func (sc *snapshotContainer) Len() int {
	sc.lock.RLock()
	defer sc.lock.RUnlock()

	return sc.snapshotList.Len()
}

//GetLatestSnapshot returns the latest snapshot from container or nil
//in case list is empty
func (sc *snapshotContainer) GetLatestSnapshot() Snapshot {
	sc.lock.RLock()
	defer sc.lock.RUnlock()

	e := sc.snapshotList.Front()

	if e == nil {
		return nil
	} else {
		return e.Value.(Snapshot)
	}
}

//GetSnapshotEqualToTS returns the snapshot from container matching the
//given timestamp or nil if its not able to find any match
func (sc *snapshotContainer) GetSnapshotEqualToTS(ts Timestamp) Snapshot {
	sc.lock.RLock()
	defer sc.lock.RUnlock()

	for e := sc.snapshotList.Front(); e != nil; e = e.Next() {
		snapshot := e.Value.(Snapshot)
		if ts.Equals(snapshot.Timestamp()) {
			return snapshot
		}
	}

	return nil
}

//GetSnapshotRecentThanTS returns a snapshot which is more recent than the
//given TS or atleast equal. Returns nil if its not able to find any match
func (sc *snapshotContainer) GetSnapshotRecentThanTS(ts Timestamp) Snapshot {
	sc.lock.RLock()
	defer sc.lock.RUnlock()

	for e := sc.snapshotList.Front(); e != nil; e = e.Next() {
		snapshot := e.Value.(Snapshot)
		snapTS := snapshot.Timestamp()
		if snapTS.GreaterThanEqual(ts) {
			return snapshot
		} else {
			break
		}
	}

	return nil
}
