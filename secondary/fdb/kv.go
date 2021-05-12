package forestdb

//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

//#cgo CFLAGS: -O0
//#include <stdlib.h>
//#include <libforestdb/forestdb.h>
import "C"

import (
	"unsafe"
)

// GetKV simplified API for key/value access to Get()
func (k *KVStore) GetKV(key []byte) ([]byte, error) {
	k.Lock()
	defer k.Unlock()

	var kk unsafe.Pointer
	lenk := len(key)
	if lenk != 0 {
		kk = unsafe.Pointer(&key[0])
	}

	var bodyLen C.size_t
	var bodyPointer unsafe.Pointer

	//Log.Tracef("fdb_get_kv call k:%p db:%v kk:%v", k, k.db, kk)
	errNo := C.fdb_get_kv(k.db, kk, C.size_t(lenk), &bodyPointer, &bodyLen)
	//Log.Tracef("fdb_get_kv retn k:%p errNo:%v body:%p len:%v", k, errNo, bodyPointer, bodyLen)
	if errNo != RESULT_SUCCESS {
		return nil, Error(errNo)
	}

	body := C.GoBytes(bodyPointer, C.int(bodyLen))
	C.fdb_free_block(bodyPointer)
	return body, nil
}

// SetKV simplified API for key/value access to Set()
func (k *KVStore) SetKV(key, value []byte) error {
	k.Lock()
	defer k.Unlock()

	var kk, v unsafe.Pointer

	lenk := len(key)
	lenv := len(value)
	if lenk != 0 {
		kk = unsafe.Pointer(&key[0])
	}

	if lenv != 0 {
		v = unsafe.Pointer(&value[0])
	}

	//Log.Tracef("fdb_set_kv call k:%p db:%v kk:%v v:%v", k, k.db, kk, v)
	errNo := C.fdb_set_kv(k.db, kk, C.size_t(lenk), v, C.size_t(lenv))
	//Log.Tracef("fdb_set_kv retn k:%p errNo:%v", k, errNo)

	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// DeleteKV simplified API for key/value access to Delete()
func (k *KVStore) DeleteKV(key []byte) error {
	k.Lock()
	defer k.Unlock()

	var kk unsafe.Pointer
	lenk := len(key)
	if lenk != 0 {
		kk = unsafe.Pointer(&key[0])
	}

	Log.Tracef("fdb_del_kv call k:%p db:%v kk:%v", k, k.db, kk)
	errNo := C.fdb_del_kv(k.db, kk, C.size_t(lenk))
	Log.Tracef("fdb_del_kv retn k:%p errNo:%v", k, errNo)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}
