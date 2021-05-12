package forestdb

//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

//#cgo LDFLAGS: -lforestdb
//#cgo CFLAGS: -O0
//#include <stdlib.h>
//#include <libforestdb/forestdb.h>
import "C"

import (
	"reflect"
	"unsafe"
)

type SnapInfo C.fdb_snapshot_info_t

type SnapInfos struct {
	cinfo    *C.fdb_snapshot_info_t
	snapInfo []SnapInfo
}

func (f *File) GetAllSnapMarkers() (*SnapInfos, error) {
	f.Lock()
	defer f.Unlock()

	snapInfos := &SnapInfos{}
	var numMarkers C.uint64_t

	Log.Tracef("get_all_snap_markers call f:%p db:%v", f, f.dbfile)
	errNo := C.fdb_get_all_snap_markers(f.dbfile, &snapInfos.cinfo, &numMarkers)
	Log.Tracef("get_all_snap_markers retn f:%p errNo:%v cinfo:%v num:%v", f, errNo, snapInfos.cinfo, numMarkers)
	if errNo != RESULT_SUCCESS {
		return nil, Error(errNo)
	}

	//convert from C array to go slice
	hdr := reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(snapInfos.cinfo)),
		Len:  int(numMarkers),
		Cap:  int(numMarkers),
	}

	snapInfos.snapInfo = *(*[]SnapInfo)(unsafe.Pointer(&hdr))
	return snapInfos, nil
}

func (s *SnapInfos) SnapInfoList() []SnapInfo {
	return s.snapInfo
}

func (s *SnapInfos) FreeSnapMarkers() error {
	Log.Tracef("free_snap_markers call s:%p cinfo:%v", s, s.cinfo)
	errNo := C.fdb_free_snap_markers(s.cinfo, C.uint64_t(len(s.snapInfo)))
	Log.Tracef("free_snap_markers retn s:%p errNo:%v", s, errNo)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

type SnapMarker struct {
	marker C.fdb_snapshot_marker_t
}

func (si *SnapInfo) GetSnapMarker() *SnapMarker {
	sm := &SnapMarker{}
	sm.marker = si.marker
	return sm
}

func (si *SnapInfo) GetNumKvsMarkers() int64 {
	return int64(si.num_kvs_markers)
}

func (si *SnapInfo) GetKvsCommitMarkers() []CommitMarker {

	//convert from C array to go slice
	hdr := reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(si.kvs_markers)),
		Len:  int(si.num_kvs_markers),
		Cap:  int(si.num_kvs_markers),
	}

	commitMarker := *(*[]CommitMarker)(unsafe.Pointer(&hdr))

	return commitMarker
}

type CommitMarker C.fdb_kvs_commit_marker_t

func (c *CommitMarker) GetKvStoreName() string {
	return C.GoString(c.kv_store_name)
}

func (c *CommitMarker) GetSeqNum() SeqNum {
	return SeqNum(c.seqnum)
}
