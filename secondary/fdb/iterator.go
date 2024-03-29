package forestdb

//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

//#cgo CFLAGS: -O0
//#include <libforestdb/forestdb.h>
import "C"

import (
	"sync"
	"unsafe"
)

// ForestDB iterator options
type IteratorOpt uint16

const (
	// Return both key and value through iterator
	ITR_NONE IteratorOpt = 0x00
	// Return only non-deleted items through iterator
	ITR_NO_DELETES IteratorOpt = 0x02
	// The lowest key specified will not be returned by the iterator
	FDB_ITR_SKIP_MIN_KEY IteratorOpt = 0x04
	//The highest key specified will not be returned by the iterator
	FDB_ITR_SKIP_MAX_KEY IteratorOpt = 0x08
)

// ForestDB seek options
type SeekOpt uint8

const (
	// If seek_key does not exist return the next sorted key higher than it
	FDB_ITR_SEEK_HIGHER SeekOpt = 0x00
	// If seek_key does not exist return the previous sorted key lower than it
	FDB_ITR_SEEK_LOWER SeekOpt = 0x01
)

// Iterator handle
type Iterator struct {
	iter *C.fdb_iterator
	db   *KVStore
}

var fdbIterPool *sync.Pool

func init() {
	fdbIterPool = &sync.Pool{
		New: func() interface{} {
			return &Iterator{}
		},
	}
}

func allocIterator(db *KVStore) *Iterator {
	rv := fdbIterPool.Get().(*Iterator)
	rv.iter = nil
	rv.db = db

	return rv
}

func freeIterator(iter *Iterator) {
	fdbIterPool.Put(iter)
}

// Prev advances the iterator backwards
func (i *Iterator) Prev() error {
	i.db.Lock()
	defer i.db.Unlock()

	Log.Tracef("fdb_iterator_prev call i:%p iter:%v", i, i.iter)
	errNo := C.fdb_iterator_prev(i.iter)
	Log.Tracef("fdb_iterator_prev retn i:%p iter:%v", i, errNo, i.iter)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// Next advances the iterator forward
func (i *Iterator) Next() error {
	i.db.Lock()
	defer i.db.Unlock()

	Log.Tracef("fdb_iterator_next call i:%p iter:%v", i, i.iter)
	errNo := C.fdb_iterator_next(i.iter)
	Log.Tracef("fdb_iterator_next retn i:%p iter:%v", i, errNo, i.iter)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// Get gets the current item (key, metadata, doc body) from the iterator
func (i *Iterator) Get() (*Doc, error) {
	i.db.Lock()
	defer i.db.Unlock()

	rv := Doc{}
	Log.Tracef("fdb_iterator_get call i:%p iter:%v", i, i.iter)
	errNo := C.fdb_iterator_get(i.iter, &rv.doc)
	Log.Tracef("fdb_iterator_get retn i:%p iter:%v doc:%v", i, errNo, i.iter, rv.doc)
	if errNo != RESULT_SUCCESS {
		return nil, Error(errNo)
	}
	return &rv, nil
}

// GetPreAlloc gets the current item (key, metadata, doc body) from the iterator
// but uses the pre-allocated memory for the Doc
func (i *Iterator) GetPreAlloc(rv *Doc) error {
	i.db.Lock()
	defer i.db.Unlock()

	Log.Tracef("fdb_iterator_get call i:%p iter:%v", i, i.iter)
	errNo := C.fdb_iterator_get(i.iter, &rv.doc)
	Log.Tracef("fdb_iterator_get retn i:%p iter:%v doc:%v", i, errNo, i.iter, rv.doc)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// GetMetaOnly gets the current item (key, metadata, offset to doc body) from the iterator
func (i *Iterator) GetMetaOnly() (*Doc, error) {
	i.db.Lock()
	defer i.db.Unlock()

	rv := Doc{}
	Log.Tracef("fdb_iterator_get_metaonly call i:%p iter:%v", i, i.iter)
	errNo := C.fdb_iterator_get_metaonly(i.iter, &rv.doc)
	Log.Tracef("fdb_iterator_get_metaonly retn i:%p iter:%v doc:%v", i, errNo, i.iter, rv.doc)
	if errNo != RESULT_SUCCESS {
		return nil, Error(errNo)
	}
	return &rv, nil
}

// Seek fast forward / backward an iterator to
// return documents starting from
// the given seek_key. If the seek key does not
// exist, the iterator is positioned based on
// the specified dir (either before or after).
func (i *Iterator) Seek(seekKey []byte, dir SeekOpt) error {
	i.db.Lock()
	defer i.db.Unlock()

	var sk unsafe.Pointer
	lensk := len(seekKey)
	if lensk != 0 {
		sk = unsafe.Pointer(&seekKey[0])
	}
	Log.Tracef("fdb_iterator_seek call i:%p iter:%v sk:%v dir:%v", i, i.iter, sk, dir)
	errNo := C.fdb_iterator_seek(i.iter, sk, C.size_t(lensk), C.fdb_iterator_seek_opt_t(dir))
	Log.Tracef("fdb_iterator_seek retn i:%p errNo:%v iter:%v", i, errNo, i.iter)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// SeekMin moves iterator to the smallest key
// of the iteration
func (i *Iterator) SeekMin() error {
	i.db.Lock()
	defer i.db.Unlock()

	Log.Tracef("fdb_iterator_seek_to_min call i:%p iter:%v", i, i.iter)
	errNo := C.fdb_iterator_seek_to_min(i.iter)
	Log.Tracef("fdb_iterator_seek_to_min retn i:%p errNo:%v iter:%v", i, errNo, i.iter)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// SeekMax moves iterator to the largest key
// of the iteration
func (i *Iterator) SeekMax() error {
	i.db.Lock()
	defer i.db.Unlock()

	Log.Tracef("fdb_iterator_seek_to_max call i:%p iter:%v", i, i.iter)
	errNo := C.fdb_iterator_seek_to_max(i.iter)
	Log.Tracef("fdb_iterator_seek_to_max retn i:%p errNo:%v iter:%v", i, errNo, i.iter)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// Close the iterator and free its associated resources
func (i *Iterator) Close() error {
	i.db.Lock()
	defer i.db.Unlock()
	defer freeIterator(i)

	Log.Tracef("fdb_iterator_close call i:%p iter:%v", i, i.iter)
	errNo := C.fdb_iterator_close(i.iter)
	Log.Tracef("fdb_iterator_close retn i:%p errNo:%v iter:%v", i, errNo, i.iter)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// IteratorInit creates an iterator to traverse a ForestDB snapshot by key range
func (k *KVStore) IteratorInit(startKey, endKey []byte, opt IteratorOpt) (*Iterator, error) {
	k.Lock()
	defer k.Unlock()

	var sk, ek unsafe.Pointer

	lensk := len(startKey)
	lenek := len(endKey)

	if lensk != 0 {
		sk = unsafe.Pointer(&startKey[0])
	}

	if lenek != 0 {
		ek = unsafe.Pointer(&endKey[0])
	}

	rv := allocIterator(k)
	Log.Tracef("fdb_iterator_init call k:%p db:%v sk:%v ek:%v opt:%v", k, k.db, sk, ek, opt)
	errNo := C.fdb_iterator_init(k.db, &rv.iter, sk, C.size_t(lensk), ek, C.size_t(lenek), C.fdb_iterator_opt_t(opt))
	Log.Tracef("fdb_iterator_init retn k:%p rv:%v", k, rv.iter)
	if errNo != RESULT_SUCCESS {
		return nil, Error(errNo)
	}
	return rv, nil
}

// IteratorSequenceInit create an iterator to traverse a ForestDB snapshot by sequence number range
func (k *KVStore) IteratorSequenceInit(startSeq, endSeq SeqNum, opt IteratorOpt) (*Iterator, error) {
	k.Lock()
	defer k.Unlock()

	rv := allocIterator(k)
	Log.Tracef("fdb_iterator_sequence_init call k:%p db:%v sseq:%v eseq:%v opt:%v", k, k.db, startSeq, endSeq, opt)
	errNo := C.fdb_iterator_sequence_init(k.db, &rv.iter, C.fdb_seqnum_t(startSeq), C.fdb_seqnum_t(endSeq), C.fdb_iterator_opt_t(opt))
	Log.Tracef("fdb_iterator_sequence_init retn k:%p rv:%v", k, rv.iter)
	if errNo != RESULT_SUCCESS {
		return nil, Error(errNo)
	}
	return rv, nil
}
