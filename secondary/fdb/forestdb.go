package forestdb

//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

//#cgo LDFLAGS: -lforestdb
//#include <stdlib.h>
//#include <libforestdb/forestdb.h>
import "C"
import "sync"

// KVStore handle
type KVStore struct {
	sync.Mutex
	f  *File
	db *C.fdb_kvs_handle
}

// Close the KVStore and release related resources.
func (k *KVStore) Close() error {
	k.Lock()
	defer k.Unlock()

	Log.Tracef("fdb_kvs_close call k:%p db:%v", k, k.db)
	errNo := C.fdb_kvs_close(k.db)
	Log.Tracef("fdb_kvs_close retn k:%p errNo:%v", k, errNo)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// Info returns the information about a given kvstore
func (k *KVStore) Info() (*KVStoreInfo, error) {
	k.Lock()
	defer k.Unlock()

	rv := KVStoreInfo{}
	Log.Tracef("fdb_get_kvs_info call k:%p db:%v", k, k.db)
	errNo := C.fdb_get_kvs_info(k.db, &rv.info)
	Log.Tracef("fdb_kvs_close retn k:%p errNo:%v info:%v", k, errNo, rv.info)
	if errNo != RESULT_SUCCESS {
		return nil, Error(errNo)
	}
	return &rv, nil
}

// Get retrieves the metadata and doc body for a given key
func (k *KVStore) Get(doc *Doc) error {
	k.Lock()
	defer k.Unlock()

	Log.Tracef("fdb_get call k:%p db:%v doc:%v", k, k.db, doc.doc)
	errNo := C.fdb_get(k.db, doc.doc)
	Log.Tracef("fdb_get retn k:%p errNo:%v doc:%v", k, errNo, doc.doc)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// GetMetaOnly retrieves the metadata for a given key
func (k *KVStore) GetMetaOnly(doc *Doc) error {
	k.Lock()
	defer k.Unlock()

	Log.Tracef("fdb_get_metaonly call k:%p db:%v doc:%v", k, k.db, doc.doc)
	errNo := C.fdb_get_metaonly(k.db, doc.doc)
	Log.Tracef("fdb_get_metaonly retn k:%p errNo:%v doc:%v", k, errNo, doc.doc)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// GetBySeq retrieves the metadata and doc body for a given sequence number
func (k *KVStore) GetBySeq(doc *Doc) error {
	k.Lock()
	defer k.Unlock()

	Log.Tracef("fdb_get_byseq call k:%p db:%v doc:%v", k, k.db, doc.doc)
	errNo := C.fdb_get_byseq(k.db, doc.doc)
	Log.Tracef("fdb_get_byseq retn k:%p errNo:%v doc:%v", k, errNo, doc.doc)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// GetMetaOnlyBySeq retrieves the metadata for a given sequence number
func (k *KVStore) GetMetaOnlyBySeq(doc *Doc) error {
	k.Lock()
	defer k.Unlock()

	Log.Tracef("fdb_get_metaonly_byseq call k:%p db:%v doc:%v", k, k.db, doc.doc)
	errNo := C.fdb_get_metaonly_byseq(k.db, doc.doc)
	Log.Tracef("fdb_get_metaonly_byseq retn k:%p errNo:%v doc:%v", k, errNo, doc.doc)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// GetByOffset retrieves a doc's metadata and body with a given doc offset in the database file
func (k *KVStore) GetByOffset(doc *Doc) error {
	k.Lock()
	defer k.Unlock()

	Log.Tracef("fdb_get_byoffset call k:%p db:%v doc:%v", k, k.db, doc.doc)
	errNo := C.fdb_get_byoffset(k.db, doc.doc)
	Log.Tracef("fdb_get_byoffset retn k:%p errNo:%v doc:%v", k, errNo, doc.doc)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// Set update the metadata and doc body for a given key
func (k *KVStore) Set(doc *Doc) error {
	k.Lock()
	defer k.Unlock()

	Log.Tracef("fdb_set call k:%p db:%v doc:%v", k, k.db, doc.doc)
	errNo := C.fdb_set(k.db, doc.doc)
	Log.Tracef("fdb_set retn k:%p errNo:%v doc:%v", k, errNo, doc.doc)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// Delete deletes a key, its metadata and value
func (k *KVStore) Delete(doc *Doc) error {
	k.Lock()
	defer k.Unlock()

	Log.Tracef("fdb_del call k:%p db:%v doc:%v", k, k.db, doc.doc)
	errNo := C.fdb_del(k.db, doc.doc)
	Log.Tracef("fdb_set retn k:%p errNo:%v doc:%v", k, errNo, doc.doc)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// Shutdown destroys all the resources (e.g., buffer cache, in-memory WAL indexes, daemon compaction thread, etc.) and then shutdown the ForestDB engine
func Shutdown() error {
	Log.Tracef("fdb_shutdown call")
	errNo := C.fdb_shutdown()
	Log.Tracef("fdb_shutdown retn errNo:%v", errNo)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}
