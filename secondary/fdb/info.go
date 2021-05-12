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
	"fmt"
)

// FileInfo stores information about a given file
type FileInfo struct {
	info C.fdb_file_info
}

func (i *FileInfo) Filename() string {
	return C.GoString(i.info.filename)
}

func (i *FileInfo) NewFilename() string {
	return C.GoString(i.info.new_filename)
}

func (i *FileInfo) DocCount() uint64 {
	return uint64(i.info.doc_count)
}

func (i *FileInfo) SpaceUsed() uint64 {
	return uint64(i.info.space_used)
}

func (i *FileInfo) FileSize() uint64 {
	return uint64(i.info.file_size)
}

func (i *FileInfo) String() string {
	return fmt.Sprintf("filename: %s new_filename: %s doc_count: %d space_used: %d file_size: %d", i.Filename(), i.NewFilename(), i.DocCount(), i.SpaceUsed(), i.FileSize())
}

// KVStoreInfo stores information about a given kvstore
type KVStoreInfo struct {
	info C.fdb_kvs_info
}

func (i *KVStoreInfo) Name() string {
	return C.GoString(i.info.name)
}

func (i *KVStoreInfo) LastSeqNum() SeqNum {
	return SeqNum(i.info.last_seqnum)
}

func (i *KVStoreInfo) DocCount() uint64 {
	return uint64(i.info.doc_count)
}

func (i *KVStoreInfo) String() string {
	return fmt.Sprintf("name: %s last_seqnum: %d", i.Name(), i.LastSeqNum())
}
