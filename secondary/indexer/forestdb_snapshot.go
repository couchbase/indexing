// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

import (
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	forestdb "github.com/couchbase/indexing/secondary/fdb"
	"github.com/couchbase/indexing/secondary/logging"
)

var FORESTDB_INMEMSEQ = forestdb.SeqNum(math.MaxUint64)

type fdbSnapshotInfo struct {
	Ts        *common.TsVbuuid
	MainSeq   forestdb.SeqNum
	BackSeq   forestdb.SeqNum
	MetaSeq   forestdb.SeqNum
	Committed bool
	stats     map[string]interface{}
}

func (info *fdbSnapshotInfo) Timestamp() *common.TsVbuuid {
	return info.Ts
}

func (info *fdbSnapshotInfo) IsCommitted() bool {
	return info.Committed
}

func (info *fdbSnapshotInfo) Stats() map[string]interface{} {
	return info.stats
}

func (info *fdbSnapshotInfo) IsOSOSnap() bool {
	if info.Ts != nil && info.Ts.GetSnapType() == common.DISK_SNAP_OSO {
		return true
	}
	return false
}

func (info *fdbSnapshotInfo) String() string {
	return fmt.Sprintf("SnapshotInfo: seqnos: %v, %v, %v committed:%v", info.MainSeq,
		info.BackSeq, info.MetaSeq, info.Committed)
}

type fdbSnapshot struct {
	slice *fdbSlice

	main       *forestdb.KVStore // handle for forward index
	mainSeqNum forestdb.SeqNum

	idxDefnId common.IndexDefnId //index definition id
	idxInstId common.IndexInstId //index instance id
	ts        *common.TsVbuuid   //timestamp
	committed bool

	refCount int32 //Reader count for this snapshot
}

func (s *fdbSnapshot) Create() error {

	var mainSeq forestdb.SeqNum
	if s.committed {
		mainSeq = s.mainSeqNum
	} else {
		mainSeq = FORESTDB_INMEMSEQ
	}

	var err error
	t0 := time.Now()
	s.main, err = s.main.SnapshotOpen(mainSeq)
	if err != nil {
		logging.Errorf("ForestDBSnapshot::Open \n\tUnexpected Error "+
			"Opening Main DB Snapshot (%v) SeqNum %v %v", s.slice.Path(), mainSeq, err)
		return err
	}

	if s.committed {
		s.slice.idxStats.Timings.stPersistSnapshotCreate.Put(time.Now().Sub(t0))
	} else {
		s.slice.idxStats.Timings.stSnapshotCreate.Put(time.Now().Sub(t0))
	}

	s.slice.IncrRef()
	atomic.StoreInt32(&s.refCount, 1)

	return nil
}

func (s *fdbSnapshot) Open() error {
	atomic.AddInt32(&s.refCount, int32(1))

	return nil
}

func (s *fdbSnapshot) IsOpen() bool {

	count := atomic.LoadInt32(&s.refCount)
	return count > 0
}

func (s *fdbSnapshot) Id() SliceId {
	return s.slice.Id()
}

func (s *fdbSnapshot) IndexInstId() common.IndexInstId {
	return s.idxInstId
}

func (s *fdbSnapshot) IndexDefnId() common.IndexDefnId {
	return s.idxDefnId
}

func (s *fdbSnapshot) Timestamp() *common.TsVbuuid {
	return s.ts
}

func (s *fdbSnapshot) MainIndexSeqNum() forestdb.SeqNum {
	return s.mainSeqNum
}

//Close the snapshot
func (s *fdbSnapshot) Close() error {

	count := atomic.AddInt32(&s.refCount, int32(-1))

	if count < 0 {
		logging.Errorf("ForestDBSnapshot::Close Close operation requested " +
			"on already closed snapshot")
		return errors.New("Snapshot Already Closed")

	} else if count == 0 {
		go s.Destroy()
	}

	return nil
}

func (s *fdbSnapshot) Destroy() {

	defer s.slice.DecrRef()

	t0 := time.Now()
	if s.main != nil {
		err := s.main.Close()
		if err != nil {
			logging.Errorf("ForestDBSnapshot::Close Unexpected error "+
				"closing Main DB Snapshot %v", err)
		}
	} else {
		logging.Errorf("ForestDBSnapshot::Close Main DB Handle Nil")
	}

	if !s.committed {
		s.slice.idxStats.Timings.stSnapshotClose.Put(time.Now().Sub(t0))
	}
	s.slice.idxStats.numOpenSnapshots.Add(-1)
}

func (s *fdbSnapshot) String() string {

	str := fmt.Sprintf("Index: %v ", s.idxInstId)
	str += fmt.Sprintf("SliceId: %v ", s.slice.Id())
	str += fmt.Sprintf("MainSeqNum: %v ", s.mainSeqNum)
	str += fmt.Sprintf("TS: %v ", s.ts)
	return str
}

func (s *fdbSnapshot) Info() SnapshotInfo {
	return &fdbSnapshotInfo{
		MainSeq:   s.mainSeqNum,
		Committed: s.committed,
		Ts:        s.ts,
	}
}
