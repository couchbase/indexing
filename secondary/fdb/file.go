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
	"fmt"
	"path"
	"unsafe"
)

const FDB_UNKNOWN_FILE_FORMAT = "Unkown"
const FDB_V1_FILE_FORMAT = "ForestDB v1.x format"
const FDB_V2_FILE_FORMAT = "ForestDB v2.x format"

const (
	FdbUnkownFileVersion uint8 = iota
	FdbV1FileVersion
	FdbV2FileVersion
)

var FDB_CORRUPTION_ERR = fmt.Errorf("Storage corrupted and unrecoverable")

// Database handle
type File struct {
	dbfile *C.fdb_file_handle
	advLock
	name string
}

// Open opens the database with a given file name
func Open(filename string, config *Config) (*File, error) {

	if config == nil {
		config = DefaultConfig()
	}

	dbname := C.CString(filename)
	defer C.free(unsafe.Pointer(dbname))

	rv := File{name: path.Base(filename)}
	rv.advLock.Init()
	Log.Tracef("fdb_open call rv:%p dbname:%v conf:%v", &rv, dbname, config.config)
	errNo := C.fdb_open(&rv.dbfile, dbname, config.config)
	Log.Tracef("fdb_open ret rv:%p errNo:%v rv:%v", &rv, errNo, rv)
	if errNo != RESULT_SUCCESS {
		if errNo == C.FDB_NONRECOVERABLE_ERR {
			return nil, FDB_CORRUPTION_ERR
		}
		return nil, Error(errNo)
	}
	return &rv, nil
}

// Options to be passed to Commit()
type CommitOpt uint8

const (
	// Perform commit without any options.
	COMMIT_NORMAL CommitOpt = 0x00
	// Manually flush WAL entries even though it doesn't reach the configured threshol
	COMMIT_MANUAL_WAL_FLUSH CommitOpt = 0x01
)

// Commit all pending changes into disk.
func (f *File) Commit(opt CommitOpt) error {
	f.Lock()
	defer f.Unlock()

	Log.Tracef("fdb_commit call f:%p dbfile:%v opt:%v", f, f.dbfile, opt)
	errNo := C.fdb_commit(f.dbfile, C.fdb_commit_opt_t(opt))
	Log.Tracef("fdb_commit retn f:%p errNo:%v", f, errNo)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// Compact the current database file and create a new compacted file
func (f *File) Compact(newfilename string) error {
	f.Lock()
	defer f.Unlock()

	fn := C.CString(newfilename)
	defer C.free(unsafe.Pointer(fn))

	Log.Tracef("fdb_compact call f:%p dbfile:%v fn:%v", f, f.dbfile, fn)
	errNo := C.fdb_compact(f.dbfile, fn)
	Log.Tracef("fdb_compact retn f:%p errNo:%v", f, errNo)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// CompactUpto compacts the current database file upto given snapshot marker
//and creates a new compacted file
func (f *File) CompactUpto(newfilename string, sm *SnapMarker) error {
	f.Lock()
	defer f.Unlock()

	fn := C.CString(newfilename)
	defer C.free(unsafe.Pointer(fn))

	Log.Tracef("fdb_compact_upto call f:%p dbfile:%v fn:%v marker:%v", f, f.dbfile, fn, sm.marker)
	errNo := C.fdb_compact_upto(f.dbfile, fn, sm.marker)
	Log.Tracef("fdb_compact_upto retn f:%p errNo:%v", f, errNo)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

//CancelCompact cancels in-progress compaction
func (f *File) CancelCompact() error {
	f.Lock()
	defer f.Unlock()

	Log.Tracef("fdb_cancel_compaction call f:%p dbfile:%v", f, f.dbfile)
	errNo := C.fdb_cancel_compaction(f.dbfile)
	Log.Tracef("fdb_cancel_compaction retn f:%p errNo:%v", f, errNo)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// EstimateSpaceUsed returns the overall disk space actively used by the current database file
func (f *File) EstimateSpaceUsed() int {
	f.Lock()
	defer f.Unlock()

	Log.Tracef("fdb_estimate_space_used call f:%p dbfile:%v", f, f.dbfile)
	rv := int(C.fdb_estimate_space_used(f.dbfile))
	Log.Tracef("fdb_estimate_space_used retn f:%p rv:%v", f, rv)
	return rv
}

// DbInfo returns the information about a given database handle
func (f *File) Info() (*FileInfo, error) {
	f.Lock()
	defer f.Unlock()

	rv := FileInfo{}
	Log.Tracef("fdb_get_file_info call f:%p dbfile:%v", f, f.dbfile)
	errNo := C.fdb_get_file_info(f.dbfile, &rv.info)
	Log.Tracef("fdb_get_file_info retn f:%p errNo:%v, info:%v", f, errNo, rv.info)
	if errNo != RESULT_SUCCESS {
		return nil, Error(errNo)
	}
	return &rv, nil
}

// FIXME implement fdb_switch_compaction_mode

// Close the database file
func (f *File) Close() error {
	f.Lock()
	defer f.Unlock()

	Log.Tracef("fdb_close call f:%p dbfile:%v", f, f.dbfile)
	errNo := C.fdb_close(f.dbfile)
	Log.Tracef("fdb_close retn f:%p errNo:%v", f, errNo)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// OpenKVStore opens the named KVStore within the File
// using the provided KVStoreConfig.  If config is
// nil the DefaultKVStoreConfig() will be used.
func (f *File) OpenKVStore(name string, config *KVStoreConfig) (*KVStore, error) {
	f.Lock()
	defer f.Unlock()

	if config == nil {
		config = DefaultKVStoreConfig()
	}

	rv := allocKVStore(fmt.Sprintf("%s/%s", f.name, name))
	rv.f = f

	kvsname := C.CString(name)
	defer C.free(unsafe.Pointer(kvsname))
	Log.Tracef("fdb_kvs_open call f:%p dbfile:%v kvsname:%v config:%v", f, f.dbfile, kvsname, config.config)
	errNo := C.fdb_kvs_open(f.dbfile, &rv.db, kvsname, config.config)
	Log.Tracef("fdb_kvs_open retn f:%p errNo:%v db:%v", f, errNo, rv.db)
	if errNo != RESULT_SUCCESS {
		return nil, Error(errNo)
	}
	rv.setupLogging()
	return rv, nil
}

// OpenKVStore opens the default KVStore within the File
// using the provided KVStoreConfig.  If config is
// nil the DefaultKVStoreConfig() will be used.
func (f *File) OpenKVStoreDefault(config *KVStoreConfig) (*KVStore, error) {
	return f.OpenKVStore("default", config)
}

// Destroy destroys all resources associated with a ForestDB file permanently
func Destroy(filename string, config *Config) error {

	if config == nil {
		config = DefaultConfig()
	}

	dbname := C.CString(filename)
	defer C.free(unsafe.Pointer(dbname))

	Log.Tracef("fdb_destroy call dbname:%v config:%v", dbname, config.config)
	errNo := C.fdb_destroy(dbname, config.config)
	Log.Tracef("fdb_destroy retn dbname:%v errNo:%v", dbname, errNo)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

//
// This function will not hold advisory lock.  According to Chiyoung, this simply sets the configs for a
// given file instance inside ForestDB, therefore:
// 1) it use the same file handle for writer
// 2) does not have to pause writer or compactor
// for reference: https://issues.couchbase.com/browse/MB-17384
//
func (f *File) SetBlockReuseParams(reuseThreshold uint8, numKeepHeaders uint8) error {

	Log.Debugf("fdb_set_block_reusing_params call f:%p dbfile:%v reuseThreshold:%v numKeepHeaders:%v", f, f.dbfile, reuseThreshold, numKeepHeaders)
	errNo := C.fdb_set_block_reusing_params(f.dbfile, C.size_t(reuseThreshold), C.size_t(numKeepHeaders))
	Log.Tracef("fdb_set_block_reusing_params retn f:%p errNo:%v", f, errNo)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

func (f *File) GetFileVersion() uint8 {
	f.Lock()
	defer f.Unlock()

	Log.Tracef("fdb_get_file_version call")
	ptr := C.fdb_get_file_version(f.dbfile)
	Log.Tracef("fdb_get_file_version retn")

	if ptr != nil {
		return FdbStringToFileVersion(C.GoString(ptr))
	}

	return FdbUnkownFileVersion
}

func (f *File) GetLatencyStats() (string, error) {
	var statString string
	var i C.fdb_latency_stat_type
	for i = 0; i < C.FDB_LATENCY_NUM_STATS; i++ {
		var stat C.fdb_latency_stat
		errNo := C.fdb_get_latency_stats(f.dbfile, &stat, i)
		if errNo != RESULT_SUCCESS {
			return statString, Error(errNo)
		}

		statString += fmt.Sprintf("%v:\t%v\t%v\t%v\t%v \n", C.GoString(C.fdb_latency_stat_name(i)), stat.lat_min, stat.lat_avg, stat.lat_max, stat.lat_count)
	}
	return statString, nil
}

func (f *File) SwitchCompactionMode(mode CompactOpt, threshold uint8) error {
	f.Lock()
	defer f.Unlock()

	Log.Tracef("fdb_switch_compaction_mode call f:%p dbfile:%v mode:%v threshold",
		f, f.dbfile, mode, threshold)
	errNo := C.fdb_switch_compaction_mode(f.dbfile, C.uchar(mode), C.size_t(threshold))
	Log.Tracef("fdb_switch_compaction_mode retn f:%p errNo:%v", f, errNo)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

func FdbFileVersionToString(version uint8) string {

	if version == FdbV1FileVersion {
		return FDB_V1_FILE_FORMAT
	} else if version == FdbV2FileVersion {
		return FDB_V2_FILE_FORMAT
	}

	return FDB_UNKNOWN_FILE_FORMAT
}

func FdbStringToFileVersion(versionStr string) uint8 {

	if versionStr == FDB_V1_FILE_FORMAT {
		return FdbV1FileVersion
	} else if versionStr == FDB_V2_FILE_FORMAT {
		return FdbV2FileVersion
	}

	return FdbUnkownFileVersion
}
