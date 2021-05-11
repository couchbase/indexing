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

// SnapshotOpen creates an snapshot of a database file in ForestDB
func (k *KVStore) SnapshotOpen(sn SeqNum) (*KVStore, error) {
	k.Lock()
	defer k.Unlock()

	rv := allocKVStore(k.name)

	Log.Tracef("fdb_snapshot_open call k:%p db:%v sn:%v", k, k.db, sn)
	errNo := C.fdb_snapshot_open(k.db, &rv.db, C.fdb_seqnum_t(sn))
	Log.Tracef("fdb_snapshot_open retn k:%p errNo:%v rv:%v", k, errNo, rv.db)
	if errNo != RESULT_SUCCESS {
		return nil, Error(errNo)
	}
	rv.setupLogging()
	return rv, nil
}

// SnapshotClone clones a snapshot of a database file in ForestDB
// It is expected that the kvstore is only used for cloning so that
// it is possible not to retain lock.
func (k *KVStore) SnapshotClone(sn SeqNum) (*KVStore, error) {

	rv := allocKVStore(k.name)

	Log.Tracef("fdb_snapshot_open call k:%p db:%v sn:%v", k, k.db, sn)
	errNo := C.fdb_snapshot_open(k.db, &rv.db, C.fdb_seqnum_t(sn))
	Log.Tracef("fdb_snapshot_open retn k:%p errNo:%v rv:%v", k, errNo, rv.db)
	if errNo != RESULT_SUCCESS {
		return nil, Error(errNo)
	}
	rv.setupLogging()
	return rv, nil
}

// Rollback a database to a specified point represented by the sequence number
func (k *KVStore) Rollback(sn SeqNum) error {
	k.Lock()
	defer k.Unlock()

	Log.Tracef("fdb_rollback call k:%p db:%v sn:%v", k, k.db, sn)
	errNo := C.fdb_rollback(&k.db, C.fdb_seqnum_t(sn))
	Log.Tracef("fdb_rollback retn k:%p errNo:%v db:%v", k, errNo, k.db)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}
