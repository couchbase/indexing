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
	"github.com/couchbaselabs/goforestdb"
)

//ForestDBIterator taken from
//https://github.com/couchbaselabs/bleve/blob/master/index/store/goforestdb/iterator.go
type ForestDBIterator struct {
	db    *forestdb.KVStore
	valid bool
	curr  *forestdb.Doc
	iter  *forestdb.Iterator
}

func newFDBSnapshotIterator(s Snapshot) (*ForestDBIterator, error) {
	var seq forestdb.SeqNum
	fdbSnap := s.(*fdbSnapshot)
	if !fdbSnap.committed {
		seq = FORESTDB_INMEMSEQ
	} else {
		seq = fdbSnap.mainSeqNum
	}

	itr, err := newForestDBIterator(fdbSnap.main, seq)
	return itr, err
}

func newForestDBIterator(db *forestdb.KVStore,
	seq forestdb.SeqNum) (*ForestDBIterator, error) {
	dbInst, err := db.SnapshotOpen(seq)
	rv := ForestDBIterator{
		db: dbInst,
	}

	if err != nil {
		err = errors.New("ForestDB iterator: alloc failed " + err.Error())
	}
	return &rv, err
}

func (f *ForestDBIterator) SeekFirst() {
	if f.iter != nil {
		f.iter.Close()
		f.iter = nil
	}
	var err error
	f.iter, err = f.db.IteratorInit([]byte{}, nil, forestdb.ITR_NONE|forestdb.ITR_NO_DELETES)
	if err != nil {
		f.valid = false
		return
	}
	f.valid = true
	f.Get()
}

func (f *ForestDBIterator) Seek(key []byte) {
	if f.iter != nil {
		f.iter.Close()
		f.iter = nil
	}
	var err error
	f.iter, err = f.db.IteratorInit(key, nil, forestdb.ITR_NONE|forestdb.ITR_NO_DELETES)
	if err != nil {
		f.valid = false
		return
	}
	f.valid = true
	f.Get()
}

func (f *ForestDBIterator) Next() {
	var err error
	err = f.iter.Next()
	if err != nil {
		f.valid = false
		return
	}
	f.Get()
}

func (f *ForestDBIterator) Get() {
	var err error
	f.curr, err = f.iter.Get()
	if err != nil {
		f.valid = false
	}
}

func (f *ForestDBIterator) Current() ([]byte, []byte, bool) {
	if f.valid {
		return f.Key(), f.Value(), true
	}
	return nil, nil, false
}

func (f *ForestDBIterator) Key() []byte {
	if f.valid && f.curr != nil {
		return f.curr.Key()
	}
	return nil
}

func (f *ForestDBIterator) Value() []byte {
	if f.valid && f.curr != nil {
		return f.curr.Body()
	}
	return nil
}

func (f *ForestDBIterator) Valid() bool {
	return f.valid
}

func (f *ForestDBIterator) Close() error {
	var err error
	f.valid = false
	err = f.iter.Close()
	if err != nil {
		return err
	}
	f.iter = nil
	err = f.db.Close()
	if err != nil {
		return err
	}
	f.db = nil
	return nil
}
