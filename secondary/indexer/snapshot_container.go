// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

import (
	"container/list"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
)

// A helper data stucture for in-memory snapshot info list
type SnapshotInfoContainer interface {
	List() []SnapshotInfo

	Add(SnapshotInfo)
	Len() int

	GetLatest() SnapshotInfo
	GetOldest() SnapshotInfo
	GetEqualToTS(*common.TsVbuuid) SnapshotInfo
	GetOlderThanTS(*common.TsVbuuid) SnapshotInfo

	RemoveOldest() error
	RemoveRecentThanTS(*common.TsVbuuid) error
	RemoveAll() error
}

type snapshotInfoContainer struct {
	snapshotList *list.List
}

func NewSnapshotInfoContainer(infos []SnapshotInfo) *snapshotInfoContainer {
	sc := &snapshotInfoContainer{snapshotList: list.New()}

	for _, info := range infos {
		sc.snapshotList.PushBack(info)
	}

	return sc
}

func (sc *snapshotInfoContainer) List() []SnapshotInfo {
	var infos []SnapshotInfo
	for e := sc.snapshotList.Front(); e != nil; e = e.Next() {
		info := e.Value.(SnapshotInfo)
		infos = append(infos, info)

	}
	return infos
}

//Add adds snapshot to container
func (sc *snapshotInfoContainer) Add(s SnapshotInfo) {
	sc.snapshotList.PushFront(s)
}

//RemoveOldest removes the oldest snapshot from container.
//Return any error that happened.
func (sc *snapshotInfoContainer) RemoveOldest() error {
	e := sc.snapshotList.Back()

	if e != nil {
		sc.snapshotList.Remove(e)
	}

	return nil
}

//RemoveLatest removes the latest snapshot from container.
func (sc *snapshotInfoContainer) RemoveLatest() error {
	e := sc.snapshotList.Front()

	if e != nil {
		sc.snapshotList.Remove(e)
	}

	return nil
}

//RemoveRecentThanTS discards all the snapshots from container
//which are more recent than the given timestamp. The snaphots
//being removed are closed as well.
func (sc *snapshotInfoContainer) RemoveRecentThanTS(tsVbuuid *common.TsVbuuid) error {
	newList := list.New()
	ts := getSeqTsFromTsVbuuid(tsVbuuid)
	for e := sc.snapshotList.Front(); e != nil; e = e.Next() {
		snapshot := e.Value.(SnapshotInfo)
		snapTsVbuuid := snapshot.Timestamp()
		snapTs := getSeqTsFromTsVbuuid(snapTsVbuuid)

		if !snapTs.GreaterThan(ts) {
			newList.PushBack(snapshot)
		}
	}

	sc.snapshotList = newList
	return nil
}

//RemoveAll discards all the snapshosts from container.
//All snapshots will be closed before being discarded.
//Return any error that happened.
func (sc *snapshotInfoContainer) RemoveAll() error {
	//clear the snapshot list
	sc.snapshotList.Init()
	return nil
}

//Len returns the number of snapshots currently in container
func (sc *snapshotInfoContainer) Len() int {
	return sc.snapshotList.Len()
}

//GetLatest returns the latest snapshot from container or nil
//in case list is empty
func (sc *snapshotInfoContainer) GetLatest() SnapshotInfo {
	e := sc.snapshotList.Front()

	if e == nil {
		return nil
	} else {
		return e.Value.(SnapshotInfo)
	}
}

//GetOldest returns the oldest snapshot from container or nil
//in case list is empty
func (sc *snapshotInfoContainer) GetOldest() SnapshotInfo {
	e := sc.snapshotList.Back()

	if e == nil {
		return nil
	} else {
		return e.Value.(SnapshotInfo)
	}
}

//GetEqualToTS returns the snapshot from container matching the
//given timestamp or nil if its not able to find any match
func (sc *snapshotInfoContainer) GetEqualToTS(tsVbuuid *common.TsVbuuid) SnapshotInfo {
	ts := getSeqTsFromTsVbuuid(tsVbuuid)
	for e := sc.snapshotList.Front(); e != nil; e = e.Next() {
		snapshot := e.Value.(SnapshotInfo)
		snapTsVbuuid := snapshot.Timestamp()
		snapTs := getSeqTsFromTsVbuuid(snapTsVbuuid)
		if ts.Equals(snapTs) {
			return snapshot
		}
	}

	return nil
}

//GetOlderThanTS returns a snapshot which is older than the
//given TS or atleast equal. Returns nil if its not able to find any match
func (sc *snapshotInfoContainer) GetOlderThanTS(tsVbuuid *common.TsVbuuid) SnapshotInfo {
	ts := getSeqTsFromTsVbuuid(tsVbuuid)
	logging.Infof("SnapshotContainer::GetOlderThanTS Snapshot List: %v", sc.List())
	for e := sc.snapshotList.Front(); e != nil; e = e.Next() {
		snapshot := e.Value.(SnapshotInfo)
		snapTsVbuuid := snapshot.Timestamp()
		snapTs := getSeqTsFromTsVbuuid(snapTsVbuuid)
		logging.Infof("SnapshotContainer::GetOlderThanTS Comparing with snapshot %v, snapTs = %v", snapshot, snapTs)

		if ts.GreaterThanEqual(snapTs) {
			return snapshot
		}
	}

	logging.Infof("SnapshotContainer::GetOlderThanTS Returning nil as no matching snapshot found")
	return nil
}
