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
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbaselabs/goforestdb"
	"log"
	"sync"
)

type fdbSnapshot struct {
	id SliceId //slice id

	main       *forestdb.Database //db handle for forward index
	back       *forestdb.Database //db handle for reverse index
	mainSeqNum forestdb.SeqNum
	backSeqNum forestdb.SeqNum

	idxDefnId common.IndexDefnId //index definition id
	idxInstId common.IndexInstId //index instance id
	ts        Timestamp          //timestamp

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
		var err error
		s.main, err = s.main.SnapshotOpen(s.mainSeqNum)
		if err != nil {
			log.Println("ForestDBSnapshot: Unexpected Error Opening Main DB Snapshot %v", err)
			return err
		}
		s.back, err = s.back.SnapshotOpen(s.backSeqNum)
		if err != nil {
			log.Println("ForestDBSnapshot: Unexpected Error Opening Back DB Snapshot %v", err)
			return err
		}
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
	return s.id
}

func (s *fdbSnapshot) IndexInstId() common.IndexInstId {
	return s.idxInstId
}

func (s *fdbSnapshot) IndexDefnId() common.IndexDefnId {
	return s.idxDefnId
}

func (s *fdbSnapshot) Timestamp() Timestamp {
	return s.ts
}

func (s *fdbSnapshot) SetTimestamp(ts Timestamp) {
	s.ts = ts
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
		log.Println("ForestDBSnapshot: Close operation requested on already" +
			"closed snapshot")
		return errors.New("Snapshot Already Closed")
	} else {

		//close the main index
		if s.main != nil {
			err := s.main.Close()
			if err != nil {
				log.Println("ForestDBSnapshot: Unexpected error closing Main DB Snapshot %v", err)
				return err
			}
		} else {
			log.Println("ForestDBSnapshot: Main DB Handle Nil")
			errors.New("Main DB Handle Nil")
		}

		//close the back index
		if s.back != nil {
			err := s.back.Close()
			if err != nil {
				log.Println("ForestDBSnapshot: Unexpected error closing Back DB Snapshot %v", err)
				return err
			}
		} else {
			log.Println("ForestDBSnapshot: Back DB Handle Nil")
			errors.New("Back DB Handle Nil")
		}
		s.refCount--
	}

	return nil
}
