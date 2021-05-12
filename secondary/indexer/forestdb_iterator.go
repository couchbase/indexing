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
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/fdb"
)

var docBufPool *common.BytesBufPool

func init() {
	conf := common.SystemConfig.SectionConfig("indexer.", true /*trim*/)
	keySzCfg := getKeySizeConfig(conf)
	docBufPool = common.NewByteBufferPool(keySzCfg.maxIndexEntrySize)
}

var fdbSnapIterPool *sync.Pool

func init() {
	fdbSnapIterPool = &sync.Pool{
		New: func() interface{} {
			return &ForestDBIterator{}
		},
	}
}

//ForestDBIterator taken from
//https://github.com/couchbaselabs/bleve/blob/master/index/store/goforestdb/iterator.go
type ForestDBIterator struct {
	slice *fdbSlice
	db    *forestdb.KVStore
	valid bool
	curr  *forestdb.Doc
	iter  *forestdb.Iterator
	doc   *[]byte
}

func allocFDBSnapIterator(dbInst *forestdb.KVStore, slice *fdbSlice, doc *[]byte) *ForestDBIterator {
	iter := fdbSnapIterPool.Get().(*ForestDBIterator)
	iter.db = dbInst
	iter.slice = slice
	iter.doc = doc
	iter.valid = false
	iter.curr = nil
	iter.iter = nil
	return iter
}

func freeFDBSnapIterator(iter *ForestDBIterator) {
	iter.valid = false
	fdbSnapIterPool.Put(iter)
}

func newFDBSnapshotIterator(s Snapshot) (*ForestDBIterator, error) {
	var seq forestdb.SeqNum
	fdbSnap := s.(*fdbSnapshot)
	//fdbSnap.lock.Lock()
	//defer fdbSnap.lock.Unlock()

	if !fdbSnap.committed {
		seq = FORESTDB_INMEMSEQ
	} else {
		seq = fdbSnap.mainSeqNum
	}

	itr, err := newForestDBIterator(fdbSnap.slice, fdbSnap.main, seq)
	return itr, err
}

func newForestDBIterator(slice *fdbSlice, db *forestdb.KVStore,
	seq forestdb.SeqNum) (*ForestDBIterator, error) {

	t0 := time.Now()
	dbInst, err := db.SnapshotClone(seq)
	slice.idxStats.Timings.stCloneHandle.Put(time.Now().Sub(t0))

	rv := allocFDBSnapIterator(dbInst, slice, docBufPool.Get())

	if err != nil {
		err = errors.New("ForestDB iterator: alloc failed " + err.Error())
	}
	return rv, err
}

func (f *ForestDBIterator) SeekFirst() {
	if f.iter != nil {
		f.iter.Close()
		f.iter = nil
	}
	var err error
	t0 := time.Now()
	f.iter, err = f.db.IteratorInit([]byte{}, nil, forestdb.ITR_NONE|forestdb.ITR_NO_DELETES)
	f.slice.idxStats.Timings.stNewIterator.Put(time.Now().Sub(t0))
	if err != nil {
		f.valid = false
		return
	}

	//pre-allocate doc
	f.curr, err = forestdb.NewDoc(*f.doc, nil, nil)
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
	t0 := time.Now()
	f.iter, err = f.db.IteratorInit(key, nil, forestdb.ITR_NONE|forestdb.ITR_NO_DELETES)
	f.slice.idxStats.Timings.stNewIterator.Put(time.Now().Sub(t0))
	if err != nil {
		f.valid = false
		return
	}

	//pre-allocate doc
	f.curr, err = forestdb.NewDoc(*f.doc, nil, nil)
	if err != nil {
		f.valid = false
		return
	}

	f.valid = true
	f.Get()
}

func (f *ForestDBIterator) Next() {
	var err error
	t0 := time.Now()
	err = f.iter.Next()
	f.slice.idxStats.Timings.stIteratorNext.Put(time.Now().Sub(t0))
	if err != nil {
		f.valid = false
		return
	}

	f.Get()
}

func (f *ForestDBIterator) Get() {
	var err error
	err = f.iter.GetPreAlloc(f.curr)
	if err != nil {
		f.valid = false
	}
}

func (f *ForestDBIterator) Key() []byte {
	if f.valid && f.curr != nil {
		key := f.curr.KeyNoCopy()
		if f.slice != nil {
			atomic.AddInt64(&f.slice.get_bytes, int64(len(key)))
		}
		return key
	}
	return nil
}

func (f *ForestDBIterator) Value() []byte {
	if f.valid && f.curr != nil {
		body := f.curr.BodyNoCopy()
		if f.slice != nil {
			atomic.AddInt64(&f.slice.get_bytes, int64(len(body)))
		}
		return body
	}
	return nil
}

func (f *ForestDBIterator) Valid() bool {
	return f.valid
}

func (f *ForestDBIterator) Close() error {
	defer freeFDBSnapIterator(f)

	if f.doc != nil {
		temp := f.doc
		f.doc = nil
		docBufPool.Put(temp)
	}

	//free the doc allocated by forestdb
	if f.curr != nil {
		f.curr.Close()
		f.curr = nil
	}

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
