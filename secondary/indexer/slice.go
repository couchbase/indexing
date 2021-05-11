// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

import (
	"github.com/couchbase/indexing/secondary/common"
)

type SliceId uint64

type SliceStatus int16

const (
	//Slice is warming up(open db files etc), not ready for operations
	SLICE_STATUS_PREPARING SliceStatus = iota
	//Ready for operations
	SLICE_STATUS_ACTIVE
	//Marked for deletion
	SLICE_STATUS_TERMINATE
)

//Slice represents the unit of physical storage for index
type Slice interface {
	Id() SliceId
	Path() string
	Status() SliceStatus
	IndexInstId() common.IndexInstId
	IndexDefnId() common.IndexDefnId
	IsActive() bool
	IsDirty() bool

	SetActive(bool)
	SetStatus(SliceStatus)

	UpdateConfig(common.Config)

	IndexWriter
	GetReaderContext() IndexReaderContext

	RecoveryDone()
}

// cursorCtx implements IndexReaderContext and is used
// for tracking previous cursor key for multiple scans
// for distinct rows
type cursorCtx struct {
	cursor *[]byte
}

func (ctx *cursorCtx) Init(donech chan bool) bool {
	return true
}

func (ctx *cursorCtx) Done() {
}

func (ctx *cursorCtx) SetCursorKey(cur *[]byte) {
	ctx.cursor = cur
}

func (ctx *cursorCtx) GetCursorKey() *[]byte {
	return ctx.cursor
}
