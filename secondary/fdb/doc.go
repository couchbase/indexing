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
	"reflect"
	"unsafe"
	"sync"
)

type SeqNum uint64

// ForestDB doc structure definition
type Doc struct {
	doc   *C.fdb_doc
	freed bool
}

var fdbDocPool *sync.Pool

func init() {
	fdbDocPool = &sync.Pool{
		New: func() interface{} {
			return &Doc{}
		},
	}
}

func allocDoc() *Doc {
	rv := fdbDocPool.Get().(*Doc)
	rv.doc = nil
	rv.freed = false

	return rv
}

func freeDoc(doc *Doc) {
	fdbDocPool.Put(doc)
}

// NewDoc creates a new FDB_DOC instance on heap with a given key, its metadata, and its doc body
func NewDoc(key, meta, body []byte) (*Doc, error) {
	var k, m, b unsafe.Pointer

	if len(key) != 0 {
		k = unsafe.Pointer(&key[0])
	}

	if len(meta) != 0 {
		m = unsafe.Pointer(&meta[0])
	}

	if len(body) != 0 {
		b = unsafe.Pointer(&body[0])
	}

	lenk := len(key)
	lenm := len(meta)
	lenb := len(body)

	rv := allocDoc()

	Log.Tracef("fdb_doc_create call k:%p doc:%v m:%v b:%v", k, rv.doc, m, b)
	errNo := C.fdb_doc_create(&rv.doc,
		k, C.size_t(lenk), m, C.size_t(lenm), b, C.size_t(lenb))
	Log.Tracef("fdb_doc_create ret k:%p errNo:%v doc:%v", k, errNo, rv.doc)
	if errNo != RESULT_SUCCESS {
		return nil, Error(errNo)
	}
	return rv, nil
}

// Update a FDB_DOC instance with a given metadata and body
// NOTE: does not update the database, just the in memory structure
func (d *Doc) Update(meta, body []byte) error {
	var m, b unsafe.Pointer

	if len(meta) != 0 {
		m = unsafe.Pointer(&meta[0])
	}

	if len(body) != 0 {
		b = unsafe.Pointer(&body[0])
	}

	lenm := len(meta)
	lenb := len(body)
	Log.Tracef("fdb_doc_update call d:%p doc:%v m:%v b:%v", d, d.doc, m, b)
	errNo := C.fdb_doc_update(&d.doc, m, C.size_t(lenm), b, C.size_t(lenb))
	Log.Tracef("fdb_doc_update retn d:%p errNo:%v doc:%v m:%v b:%v", d, errNo, d.doc, m, b)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	return nil
}

// Key returns the document key
func (d *Doc) Key() []byte {
	return C.GoBytes(d.doc.key, C.int(d.doc.keylen))
}

func (d *Doc) KeyNoCopy() (s []byte) {
	shdr := (*reflect.SliceHeader)(unsafe.Pointer(&s))
	shdr.Data = uintptr(d.doc.key)
	shdr.Len = int(d.doc.keylen)
	shdr.Cap = shdr.Len
	return
}

// Meta returns the document metadata
func (d *Doc) Meta() []byte {
	return C.GoBytes(d.doc.meta, C.int(d.doc.metalen))
}

// Body returns the document body
func (d *Doc) Body() []byte {
	return C.GoBytes(d.doc.body, C.int(d.doc.bodylen))
}

func (d *Doc) BodyNoCopy() (s []byte) {
	shdr := (*reflect.SliceHeader)(unsafe.Pointer(&s))
	shdr.Data = uintptr(d.doc.body)
	shdr.Len = int(d.doc.bodylen)
	shdr.Cap = shdr.Len
	return
}

// SeqNum returns the document sequence number
func (d *Doc) SeqNum() SeqNum {
	return SeqNum(d.doc.seqnum)
}

// SetSeqNum sets the document sequence number
// NOTE: only to be used when initiating a sequence number lookup
func (d *Doc) SetSeqNum(sn SeqNum) {
	d.doc.seqnum = C.fdb_seqnum_t(sn)
}

// Offset returns the offset position on disk
func (d *Doc) Offset() uint64 {
	return uint64(d.doc.offset)
}

// Deleted returns whether or not this document has been deleted
func (d *Doc) Deleted() bool {
	return bool(d.doc.deleted)
}

// Close releases resources allocated to this document
func (d *Doc) Close() error {
	if d.freed {
		err := fmt.Errorf("Double freed goforestdb doc %v", d)
		Log.Errorf("%v", err)
		return err
	}
	defer freeDoc(d)
	Log.Tracef("fdb_doc_free call d:%p doc:%v", d, d.doc)
	errNo := C.fdb_doc_free(d.doc)
	Log.Tracef("fdb_doc_free retn d:%p errNo:%v", d, errNo)
	if errNo != RESULT_SUCCESS {
		return Error(errNo)
	}
	d.freed = true
	return nil
}
