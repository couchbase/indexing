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
	"errors"
	"fmt"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/fdb"
	"github.com/couchbase/indexing/secondary/logging"
	"math"
	"sync"
)

var FORESTDB_INMEMSEQ = forestdb.SeqNum(math.MaxUint64)

type fdbSnapshotInfo struct {
	Ts        *common.TsVbuuid
	MainSeq   forestdb.SeqNum
	BackSeq   forestdb.SeqNum
	MetaSeq   forestdb.SeqNum
	Committed bool
}

func (info *fdbSnapshotInfo) Timestamp() *common.TsVbuuid {
	return info.Ts
}

func (info *fdbSnapshotInfo) IsCommitted() bool {
	return info.Committed
}

func (info *fdbSnapshotInfo) String() string {
	return fmt.Sprintf("SnapshotInfo: seqnos: %v, %v, %v", info.MainSeq,
		info.BackSeq, info.MetaSeq)
}

type fdbSnapshot struct {
	slice Slice

	main *forestdb.KVStore // handle for forward index
	back *forestdb.KVStore // handle for reverse index
	meta *forestdb.KVStore // handle for meta

	mainSeqNum forestdb.SeqNum
	backSeqNum forestdb.SeqNum
	metaSeqNum forestdb.SeqNum

	idxDefnId common.IndexDefnId //index definition id
	idxInstId common.IndexInstId //index instance id
	ts        *common.TsVbuuid   //timestamp
	committed bool

	refCount int          //Reader count for this snapshot
	lock     sync.RWMutex //lock to atomically increment the refCount
}

func (s *fdbSnapshot) Open() error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.refCount > 0 {
		s.refCount++
		return nil
	} else {
		var mainSeq, backSeq forestdb.SeqNum
		if s.committed {
			mainSeq = s.mainSeqNum
			backSeq = s.backSeqNum
		} else {
			mainSeq = FORESTDB_INMEMSEQ
			backSeq = FORESTDB_INMEMSEQ
		}
		var err error
		s.main, err = s.main.SnapshotOpen(mainSeq)
		if err != nil {
			logging.Errorf("ForestDBSnapshot::Open \n\tUnexpected Error "+
				"Opening Main DB Snapshot (%v) SeqNum %v %v", s.slice.Path(), mainSeq, err)
			return err
		}

		//if there is a back-index(non-primary index)
		if s.back != nil {
			s.back, err = s.back.SnapshotOpen(backSeq)
			if err != nil {
				logging.Errorf("ForestDBSnapshot::Open \n\tUnexpected Error "+
					"Opening Back DB Snapshot (%v) SeqNum %v %v", s.slice.Path(), backSeq, err)
				return err
			}
		}

		if s.committed {
			s.meta, err = s.meta.SnapshotOpen(s.metaSeqNum)
			if err != nil {
				logging.Errorf("ForestDBSnapshot::Open \n\tUnexpected Error "+
					"Opening Meta DB Snapshot (%v) SeqNum %v %v", s.slice.Path(), s.metaSeqNum, err)
				return err
			}
		}

		s.slice.IncrRef()
		s.refCount = 1
	}

	return nil
}

func (s *fdbSnapshot) IsOpen() bool {

	s.lock.Lock()
	defer s.lock.Unlock()

	if s.refCount > 0 {
		return true
	} else {
		return false
	}

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

func (s *fdbSnapshot) BackIndexSeqNum() forestdb.SeqNum {
	return s.backSeqNum
}

//Close the snapshot
func (s *fdbSnapshot) Close() error {

	s.lock.Lock()
	defer s.lock.Unlock()

	if s.refCount <= 0 {
		logging.Errorf("ForestDBSnapshot::Close Close operation requested " +
			"on already closed snapshot")
		return errors.New("Snapshot Already Closed")
	} else {
		s.refCount--
		if s.refCount == 0 {
			//close the main index
			defer s.slice.DecrRef()
			if s.main != nil {
				err := s.main.Close()
				if err != nil {
					logging.Errorf("ForestDBSnapshot::Close Unexpected error "+
						"closing Main DB Snapshot %v", err)
					return err
				}
			} else {
				logging.Errorf("ForestDBSnapshot::Close Main DB Handle Nil")
				return errors.New("Main DB Handle Nil")
			}

			//close the back index
			if s.back != nil {
				err := s.back.Close()
				if err != nil {
					logging.Errorf("ForestDBSnapshot::Close Unexpected error closing "+
						"Back DB Snapshot %v", err)
					return err
				}
			} else {
				//valid to be nil in case of primary index
				logging.Warnf("ForestDBSnapshot::Close Back DB Handle Nil")
			}

			//close the meta index
			if s.committed {
				if s.meta != nil {
					err := s.meta.Close()
					if err != nil {
						logging.Errorf("ForestDBSnapshot::Close Unexpected error closing "+
							"Meta DB Snapshot %v", err)
						return err
					}
				} else {
					logging.Errorf("ForestDBSnapshot::Close Meta DB Handle Nil")
					return errors.New("Meta DB Handle Nil")
				}
			}
		}
	}

	return nil
}

func (s *fdbSnapshot) String() string {

	str := fmt.Sprintf("Index: %v ", s.idxInstId)
	str += fmt.Sprintf("SliceId: %v ", s.slice.Id())
	str += fmt.Sprintf("MainSeqNum: %v ", s.mainSeqNum)
	str += fmt.Sprintf("BackSeqNum: %v ", s.backSeqNum)
	str += fmt.Sprintf("TS: %v ", s.ts)
	return str
}

func (s *fdbSnapshot) Info() SnapshotInfo {
	return &fdbSnapshotInfo{
		MainSeq:   s.mainSeqNum,
		BackSeq:   s.backSeqNum,
		MetaSeq:   s.metaSeqNum,
		Committed: s.committed,
		Ts:        s.ts,
	}
}
