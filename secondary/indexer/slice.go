// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

import (
	"context"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/vector/codebook"
	"golang.org/x/sync/semaphore"
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

// Slice represents the unit of physical storage for index
type Slice interface {
	Id() SliceId
	Path() string
	Status() SliceStatus
	IndexInstId() common.IndexInstId
	IndexPartnId() common.PartitionId
	IndexDefnId() common.IndexDefnId
	IsActive() bool
	IsDirty() bool
	IsCleanupDone() bool

	SetActive(bool)
	SetStatus(SliceStatus)

	UpdateConfig(common.Config)

	IndexWriter
	GetReaderContext(user string, skipReadMetering bool) IndexReaderContext

	RecoveryDone()
	BuildDone(common.IndexInstId, BuildDoneCallback)

	GetShardIds() []common.ShardId
	ClearRebalRunning()
	SetRebalRunning()
	IsPersistanceActive() bool

	GetWriteUnits() uint64
	SetStopWriteUnitBilling(disableBilling bool)

	SetNlist(int)
	GetNlist() int
	InitCodebook() error
	ResetCodebook() error
	InitCodebookFromSerialized([]byte) error
	Train([]float32) error
	GetCodebook() (codebook.Codebook, error)
	IsTrained() bool
	SerializeCodebook() ([]byte, error)

	ReserveReaders(int, chan bool) error
	ReleaseReaders(int)

	SetCurrentEncryptionKey([]byte, []byte, string) error
	DropKeys([][]byte, chan error)
	GetKeyIdList() ([][]byte, error)
}

type SliceEncryptionCallbacks struct {
	getActiveKeyIdCipher func(typename, bucketUUID string) ([]byte, string, string, error)
	getKeyCipherById     func(keyId string) ([]byte, string, error)
	setInUseKeys         func(kdt KeyDataType, key string) error
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

func (ctx *cursorCtx) ReadUnits() uint64 {
	return 0
}

func (ctx *cursorCtx) RecordReadUnits(byteLen uint64) {
}

func (ctx *cursorCtx) User() string {
	return ""
}

func (ctx *cursorCtx) SkipReadMetering() bool {
	return true // Not used
}

type readersReserve struct {
	readerSem *semaphore.Weighted
}

func (rr *readersReserve) InitReadersReserve(numReaders int) {
	rr.readerSem = semaphore.NewWeighted(int64(numReaders))
}

func (rr *readersReserve) ReserveReaders(numReaders int, donech chan bool) error {
	if rr.readerSem == nil {
		return nil
	}

	// Create a context with cancellation
	ctxWithCancel, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure the context is canceled when we're done

	// Set up a goroutine to listen on donech and cancel the context if needed
	go func() {
		select {
		case <-donech:
			cancel() // Cancel the context when something is received on donech
		case <-ctxWithCancel.Done():
			// Context is already done, nothing to do
		}
	}()

	// Use the cancellable context for semaphore acquisition
	err := rr.readerSem.Acquire(ctxWithCancel, int64(numReaders))
	logging.Tracef("Accqured %d readers", numReaders)
	return err
}

func (rr *readersReserve) ReleaseReaders(numReaders int) {
	if rr.readerSem == nil {
		return
	}
	logging.Tracef("Releasing %d readers", numReaders)
	rr.readerSem.Release(int64(numReaders))
}
