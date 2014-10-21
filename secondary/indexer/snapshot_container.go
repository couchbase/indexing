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
	"github.com/couchbase/indexing/secondary/common"
	"sync"
)

//SnapshotContainer manages snapshots for a Slice
type SnapshotContainer interface {
	Add(Snapshot)
	Len() int

	GetLatestSnapshot() Snapshot
	GetSnapshotEqualToTS(*common.TsVbuuid) Snapshot
	GetSnapshotOlderThanTS(*common.TsVbuuid) Snapshot

	RemoveRecentThanTS(*common.TsVbuuid) error
	RemoveOldest() Snapshot
	RemoveAll() error
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

//RemoveOldest removes the oldest snapshot from container
//and returns it. Nil is returned in case there is no
//snapshot.
func (sc *snapshotContainer) RemoveOldest() Snapshot {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	e := sc.snapshotList.Back()

	if e == nil {
		return nil
	} else {
		sc.snapshotList.Remove(e)
		return e.Value.(Snapshot)
	}
}

//RemoveRecentThanTS removes all the snapshots from container
//which are more recent than the given timestamp.
func (sc *snapshotContainer) RemoveRecentThanTS(tsVbuuid *common.TsVbuuid) error {

	sc.lock.Lock()
	defer sc.lock.Unlock()

	ts := getStabilityTSFromTsVbuuid(tsVbuuid)
	for e := sc.snapshotList.Front(); e != nil; e = e.Next() {
		snapshot := e.Value.(Snapshot)
		snapTsVbuuid := snapshot.Timestamp()
		snapTs := getStabilityTSFromTsVbuuid(snapTsVbuuid)
		if snapTs.GreaterThan(ts) {
			sc.snapshotList.Remove(e)
		}
	}

	return nil

}

//RemoveAll removes all the snapshost from container.
//Return any error that happened.
func (sc *snapshotContainer) RemoveAll() error {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	sc.snapshotList.Init()
	return nil
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
func (sc *snapshotContainer) GetSnapshotEqualToTS(tsVbuuid *common.TsVbuuid) Snapshot {
	sc.lock.RLock()
	defer sc.lock.RUnlock()

	ts := getStabilityTSFromTsVbuuid(tsVbuuid)
	for e := sc.snapshotList.Front(); e != nil; e = e.Next() {
		snapshot := e.Value.(Snapshot)
		snapTsVbuuid := snapshot.Timestamp()
		snapTs := getStabilityTSFromTsVbuuid(snapTsVbuuid)
		if ts.Equals(snapTs) {
			return snapshot
		}
	}

	return nil
}

//GetSnapshotOlderThanTS returns a snapshot which is older than the
//given TS or atleast equal. Returns nil if its not able to find any match
func (sc *snapshotContainer) GetSnapshotOlderThanTS(tsVbuuid *common.TsVbuuid) Snapshot {
	sc.lock.RLock()
	defer sc.lock.RUnlock()

	ts := getStabilityTSFromTsVbuuid(tsVbuuid)
	for e := sc.snapshotList.Front(); e != nil; e = e.Next() {
		snapshot := e.Value.(Snapshot)
		snapTsVbuuid := snapshot.Timestamp()
		snapTs := getStabilityTSFromTsVbuuid(snapTsVbuuid)
		if ts.GreaterThanEqual(snapTs) {
			return snapshot
		} else {
			break
		}
	}

	return nil
}
