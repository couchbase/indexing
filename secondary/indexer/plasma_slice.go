// +build !community

package indexer

// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/common/queryutil"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/plasma"
)

func init() {
	plasma.SetLogger(&logging.SystemLogger)
}

type plasmaSlice struct {
	newBorn                               bool
	get_bytes, insert_bytes, delete_bytes int64
	flushedCount                          uint64
	committedCount                        uint64
	qCount                                int64

	path string
	id   SliceId

	refCount int
	lock     sync.RWMutex

	mainstore *plasma.Plasma
	backstore *plasma.Plasma

	main []*plasma.Writer

	back []*plasma.Writer

	readers chan *plasma.Reader

	idxDefn   common.IndexDefn
	idxDefnId common.IndexDefnId
	idxInstId common.IndexInstId

	status        SliceStatus
	isActive      bool
	isDirty       bool
	isPrimary     bool
	isSoftDeleted bool
	isSoftClosed  bool

	cmdCh  []chan indexMutation
	stopCh []DoneChannel

	workerDone []chan bool

	fatalDbErr error

	numWriters   int
	maxRollbacks int

	totalFlushTime  time.Duration
	totalCommitTime time.Duration

	idxStats *IndexStats
	sysconf  common.Config
	confLock sync.RWMutex

	isPersistorActive int32

	// Array processing
	arrayExprPosition int
	isArrayDistinct   bool

	encodeBuf [][]byte
	arrayBuf1 [][]byte
	arrayBuf2 [][]byte

	hasPersistence bool
}

func newPlasmaSlice(path string, sliceId SliceId, idxDefn common.IndexDefn,
	idxInstId common.IndexInstId, isPrimary bool,
	sysconf common.Config, idxStats *IndexStats) (*plasmaSlice, error) {

	slice := &plasmaSlice{}

	_, err := os.Stat(path)
	if err != nil {
		os.Mkdir(path, 0777)
		slice.newBorn = true
	}

	slice.idxStats = idxStats

	slice.get_bytes = 0
	slice.insert_bytes = 0
	slice.delete_bytes = 0
	slice.flushedCount = 0
	slice.committedCount = 0
	slice.sysconf = sysconf
	slice.path = path
	slice.idxInstId = idxInstId
	slice.idxDefnId = idxDefn.DefnId
	slice.idxDefn = idxDefn
	slice.id = sliceId
	slice.numWriters = sysconf["numSliceWriters"].Int()
	slice.hasPersistence = !sysconf["plasma.disablePersistence"].Bool()

	slice.maxRollbacks = sysconf["settings.plasma.recovery.max_rollbacks"].Int()

	sliceBufSize := sysconf["settings.sliceBufSize"].Uint64()
	if sliceBufSize < uint64(slice.numWriters) {
		sliceBufSize = uint64(slice.numWriters)
	}

	updatePlasmaConfig(sysconf)
	if sysconf["plasma.UseQuotaTuner"].Bool() {
		go plasma.RunMemQuotaTuner()
	}

	slice.encodeBuf = make([][]byte, slice.numWriters)
	slice.arrayBuf1 = make([][]byte, slice.numWriters)
	slice.arrayBuf2 = make([][]byte, slice.numWriters)
	slice.cmdCh = make([]chan indexMutation, slice.numWriters)

	for i := 0; i < slice.numWriters; i++ {
		slice.cmdCh[i] = make(chan indexMutation, sliceBufSize/uint64(slice.numWriters))
		slice.encodeBuf[i] = make([]byte, 0, maxIndexEntrySize)
		slice.arrayBuf1[i] = make([]byte, 0, maxArrayIndexEntrySize)
		slice.arrayBuf2[i] = make([]byte, 0, maxArrayIndexEntrySize)
	}

	slice.workerDone = make([]chan bool, slice.numWriters)
	slice.stopCh = make([]DoneChannel, slice.numWriters)

	numReaders := slice.sysconf["plasma.numReaders"].Int()
	slice.readers = make(chan *plasma.Reader, numReaders)

	slice.isPrimary = isPrimary
	if err := slice.initStores(); err != nil {
		return nil, err
	}

	// Array related initialization
	_, slice.isArrayDistinct, slice.arrayExprPosition, err = queryutil.GetArrayExpressionPosition(idxDefn.SecExprs)
	if err != nil {
		return nil, err
	}

	logging.Infof("plasmaSlice:NewplasmaSlice Created New Slice Id %v IndexInstId %v "+
		"WriterThreads %v", sliceId, idxInstId, slice.numWriters)

	for i := 0; i < slice.numWriters; i++ {
		slice.stopCh[i] = make(DoneChannel)
		slice.workerDone[i] = make(chan bool)
		go slice.handleCommandsWorker(i)
	}

	slice.setCommittedCount()
	return slice, nil
}

func (slice *plasmaSlice) initStores() error {
	var err error
	cfg := plasma.DefaultConfig()
	cfg.UseMemoryMgmt = slice.sysconf["plasma.useMemMgmt"].Bool()
	cfg.FlushBufferSize = int(slice.sysconf["plasma.flushBufferSize"].Int())
	cfg.LSSLogSegmentSize = int64(slice.sysconf["plasma.LSSSegmentFileSize"].Int())
	cfg.UseCompression = slice.sysconf["plasma.useCompression"].Bool()
	cfg.AutoSwapper = true
	cfg.NumPersistorThreads = int(float32(runtime.NumCPU())*float32(slice.sysconf["plasma.persistenceCPUPercent"].Int())/(100*2) + 0.5)
	cfg.DisableReadCaching = slice.sysconf["plasma.disableReadCaching"].Bool()
	cfg.AutoMVCCPurging = slice.sysconf["plasma.purger.enabled"].Bool()
	cfg.PurgerInterval = time.Duration(slice.sysconf["plasma.purger.interval"].Int()) * time.Second
	cfg.PurgeThreshold = slice.sysconf["plasma.purger.highThreshold"].Float64()
	cfg.PurgeLowThreshold = slice.sysconf["plasma.purger.lowThreshold"].Float64()
	cfg.PurgeCompactRatio = slice.sysconf["plasma.purger.compactRatio"].Float64()
	cfg.EnableLSSPageSMO = slice.sysconf["plasma.enableLSSPageSMO"].Bool()
	cfg.LSSReadAheadSize = int64(slice.sysconf["plasma.logReadAheadSize"].Int())
	cfg.CheckpointInterval = time.Second * time.Duration(slice.sysconf["plasma.checkpointInterval"].Int())

	var mode plasma.IOMode

	if slice.sysconf["plasma.useMmapReads"].Bool() {
		mode = plasma.MMapIO
	} else if slice.sysconf["plasma.useDirectIO"].Bool() {
		mode = plasma.DirectIO
	}

	cfg.IOMode = mode

	var mCfg, bCfg plasma.Config

	mCfg = cfg
	bCfg = cfg

	mCfg.MaxDeltaChainLen = slice.sysconf["plasma.mainIndex.maxNumPageDeltas"].Int()
	mCfg.MaxPageItems = slice.sysconf["plasma.mainIndex.pageSplitThreshold"].Int()
	mCfg.MinPageItems = slice.sysconf["plasma.mainIndex.pageMergeThreshold"].Int()
	mCfg.MaxPageLSSSegments = slice.sysconf["plasma.mainIndex.maxLSSPageSegments"].Int()
	mCfg.LSSCleanerThreshold = slice.sysconf["plasma.mainIndex.LSSFragmentation"].Int()
	mCfg.LSSCleanerMaxThreshold = slice.sysconf["plasma.mainIndex.maxLSSFragmentation"].Int()

	bCfg.MaxDeltaChainLen = slice.sysconf["plasma.backIndex.maxNumPageDeltas"].Int()
	bCfg.MaxPageItems = slice.sysconf["plasma.backIndex.pageSplitThreshold"].Int()
	bCfg.MinPageItems = slice.sysconf["plasma.backIndex.pageMergeThreshold"].Int()
	bCfg.MaxPageLSSSegments = slice.sysconf["plasma.backIndex.maxLSSPageSegments"].Int()
	bCfg.LSSCleanerThreshold = slice.sysconf["plasma.backIndex.LSSFragmentation"].Int()
	bCfg.LSSCleanerMaxThreshold = slice.sysconf["plasma.backIndex.maxLSSFragmentation"].Int()

	if slice.hasPersistence {
		mCfg.File = filepath.Join(slice.path, "mainIndex")
		bCfg.File = filepath.Join(slice.path, "docIndex")
	}

	var wg sync.WaitGroup
	var mErr, bErr error
	t0 := time.Now()

	// Recover mainindex
	wg.Add(1)
	go func() {
		defer wg.Done()

		slice.mainstore, err = plasma.New(mCfg)
		if err != nil {
			mErr = fmt.Errorf("Unable to initialize %s, err = %v", mCfg.File, err)
			return
		}

		slice.main = make([]*plasma.Writer, slice.numWriters)
		for i := 0; i < slice.numWriters; i++ {
			slice.main[i] = slice.mainstore.NewWriter()
		}

		for i := 0; i < cap(slice.readers); i++ {
			slice.readers <- slice.mainstore.NewReader()
		}

		slice.mainstore.SetLogPrefix(fmt.Sprintf("%s/%s/Mainstore ", slice.idxDefn.Bucket, slice.idxDefn.Name))
	}()

	if !slice.isPrimary {
		// Recover backindex
		wg.Add(1)
		go func() {
			defer wg.Done()
			slice.backstore, err = plasma.New(bCfg)
			if err != nil {
				bErr = fmt.Errorf("Unable to initialize %s, err = %v", bCfg.File, err)
				return
			}

			slice.back = make([]*plasma.Writer, slice.numWriters)
			for i := 0; i < slice.numWriters; i++ {
				slice.back[i] = slice.backstore.NewWriter()
			}

			slice.backstore.SetLogPrefix(fmt.Sprintf("%s/%s/Backstore ", slice.idxDefn.Bucket, slice.idxDefn.Name))
		}()
	}

	wg.Wait()
	if mErr != nil {
		return mErr
	} else if bErr != nil {
		return bErr
	}

	if !slice.newBorn {
		logging.Infof("plasmaSlice::doRecovery SliceId %v IndexInstId %v Recovering from recovery point ..",
			slice.id, slice.idxInstId)
		err = slice.doRecovery()
		dur := time.Since(t0)
		if err == nil {
			slice.idxStats.diskSnapLoadDuration.Set(int64(dur / time.Millisecond))
			logging.Infof("plasmaSlice::doRecovery SliceId %v IndexInstId %v Warmup took %v",
				slice.id, slice.idxInstId, dur)
		}
	}

	return err
}

type plasmaReaderCtx struct {
	ch chan *plasma.Reader
	r  *plasma.Reader
	cursorCtx
}

func (ctx *plasmaReaderCtx) Init() {
	ctx.r = <-ctx.ch
}

func (ctx *plasmaReaderCtx) Done() {
	if ctx.r != nil {
		ctx.ch <- ctx.r
	}
}

func (mdb *plasmaSlice) GetReaderContext() IndexReaderContext {
	return &plasmaReaderCtx{
		ch: mdb.readers,
	}
}

func cmpRPMeta(a, b []byte) int {
	av := binary.BigEndian.Uint64(a[:8])
	bv := binary.BigEndian.Uint64(b[:8])
	return int(av - bv)
}

func (mdb *plasmaSlice) doRecovery() error {
	snaps, err := mdb.GetSnapshots()
	if err != nil {
		return err
	}

	if len(snaps) == 0 {
		logging.Infof("plasmaSlice::doRecovery SliceId %v IndexInstId %v Unable to find recovery point. Resetting store ..",
			mdb.id, mdb.idxInstId)
		mdb.resetStores()
	} else {
		err := mdb.restore(snaps[0])
		return err
	}

	return nil
}

func (mdb *plasmaSlice) IncrRef() {
	mdb.lock.Lock()
	defer mdb.lock.Unlock()

	mdb.refCount++
}

func (mdb *plasmaSlice) DecrRef() {
	mdb.lock.Lock()
	defer mdb.lock.Unlock()

	mdb.refCount--
	if mdb.refCount == 0 {
		if mdb.isSoftClosed {
			tryCloseplasmaSlice(mdb)
		}
		if mdb.isSoftDeleted {
			tryDeleteplasmaSlice(mdb)
		}
	}
}

func (mdb *plasmaSlice) Insert(key []byte, docid []byte, meta *MutationMeta) error {
	mut := indexMutation{
		op:    opUpdate,
		key:   key,
		docid: docid,
	}
	atomic.AddInt64(&mdb.qCount, 1)
	mdb.cmdCh[int(meta.vbucket)%mdb.numWriters] <- mut
	mdb.idxStats.numDocsFlushQueued.Add(1)
	return mdb.fatalDbErr
}

func (mdb *plasmaSlice) Delete(docid []byte, meta *MutationMeta) error {
	mdb.idxStats.numDocsFlushQueued.Add(1)
	atomic.AddInt64(&mdb.qCount, 1)
	mdb.cmdCh[int(meta.vbucket)%mdb.numWriters] <- indexMutation{op: opDelete, docid: docid}
	return mdb.fatalDbErr
}

func (mdb *plasmaSlice) handleCommandsWorker(workerId int) {
	var start time.Time
	var elapsed time.Duration
	var icmd indexMutation

loop:
	for {
		var nmut int
		select {
		case icmd = <-mdb.cmdCh[workerId]:
			switch icmd.op {
			case opUpdate:
				start = time.Now()
				nmut = mdb.insert(icmd.key, icmd.docid, workerId)
				elapsed = time.Since(start)
				mdb.totalFlushTime += elapsed

			case opDelete:
				start = time.Now()
				nmut = mdb.delete(icmd.docid, workerId)
				elapsed = time.Since(start)
				mdb.totalFlushTime += elapsed

			default:
				logging.Errorf("plasmaSlice::handleCommandsWorker \n\tSliceId %v IndexInstId %v Received "+
					"Unknown Command %v", mdb.id, mdb.idxInstId, icmd)
			}

			mdb.idxStats.numItemsFlushed.Add(int64(nmut))
			mdb.idxStats.numDocsIndexed.Add(1)
			atomic.AddInt64(&mdb.qCount, -1)

		case <-mdb.stopCh[workerId]:
			mdb.stopCh[workerId] <- true
			break loop

		case <-mdb.workerDone[workerId]:
			mdb.workerDone[workerId] <- true

		}
	}
}

func (mdb *plasmaSlice) insert(key []byte, docid []byte, workerId int) int {
	var nmut int

	if mdb.isPrimary {
		nmut = mdb.insertPrimaryIndex(key, docid, workerId)
	} else if len(key) == 0 {
		nmut = mdb.delete(docid, workerId)
	} else {
		if mdb.idxDefn.IsArrayIndex {
			nmut = mdb.insertSecArrayIndex(key, docid, workerId)
		} else {
			nmut = mdb.insertSecIndex(key, docid, workerId)
		}
	}

	mdb.logWriterStat()
	return nmut
}

func (mdb *plasmaSlice) insertPrimaryIndex(key []byte, docid []byte, workerId int) int {
	logging.Tracef("plasmaSlice::insert \n\tSliceId %v IndexInstId %v Set Key - %s", mdb.id, mdb.idxInstId, docid)

	entry, err := NewPrimaryIndexEntry(docid)
	common.CrashOnError(err)

	mdb.main[workerId].Begin()
	defer mdb.main[workerId].End()

	_, err = mdb.main[workerId].LookupKV(entry)
	if err == plasma.ErrItemNotFound {
		t0 := time.Now()
		mdb.main[workerId].InsertKV(entry, nil)
		mdb.idxStats.Timings.stKVSet.Put(time.Now().Sub(t0))
		atomic.AddInt64(&mdb.insert_bytes, int64(len(entry)))
		mdb.isDirty = true
		return 1
	}

	return 0
}

func (mdb *plasmaSlice) insertSecIndex(key []byte, docid []byte, workerId int) int {
	t0 := time.Now()

	ndel := mdb.deleteSecIndex(docid, workerId)

	mdb.encodeBuf[workerId] = resizeEncodeBuf(mdb.encodeBuf[workerId], len(key), allowLargeKeys)
	entry, err := NewSecondaryIndexEntry(key, docid, mdb.idxDefn.IsArrayIndex,
		1, mdb.idxDefn.Desc, mdb.encodeBuf[workerId])
	if err != nil {
		logging.Errorf("plasmaSlice::insertSecIndex Slice Id %v IndexInstId %v "+
			"Skipping docid:%s (%v)", mdb.Id, mdb.idxInstId, docid, err)
		return ndel
	}

	if len(key) > 0 {
		mdb.main[workerId].Begin()
		defer mdb.main[workerId].End()
		mdb.back[workerId].Begin()
		defer mdb.back[workerId].End()

		mdb.main[workerId].InsertKV(entry, nil)
		backEntry := entry2BackEntry(entry)
		mdb.back[workerId].InsertKV(docid, backEntry)

		mdb.idxStats.Timings.stKVSet.Put(time.Now().Sub(t0))
		atomic.AddInt64(&mdb.insert_bytes, int64(len(docid)+len(entry)))
	}

	mdb.isDirty = true
	return 1
}

func (mdb *plasmaSlice) insertSecArrayIndex(key []byte, docid []byte, workerId int) (nmut int) {
	var err error
	var oldkey []byte

	mdb.arrayBuf2[workerId] = resizeArrayBuf(mdb.arrayBuf2[workerId], len(key))

	if !allowLargeKeys && len(key) > maxArrayIndexEntrySize {
		logging.Errorf("plasmaSlice::insertSecArrayIndex Error indexing docid: %s in Slice: %v. Error: Encoded array key (size %v) too long (> %v). Skipped.",
			docid, mdb.id, len(key), maxArrayIndexEntrySize)
		mdb.deleteSecArrayIndex(docid, workerId)
		return 0
	}

	mdb.main[workerId].Begin()
	defer mdb.main[workerId].End()
	mdb.back[workerId].Begin()
	defer mdb.back[workerId].End()

	oldkey, err = mdb.back[workerId].LookupKV(docid)
	if err == plasma.ErrItemNotFound {
		oldkey = nil
	}

	var oldEntriesBytes, newEntriesBytes [][]byte
	var oldKeyCount, newKeyCount []int
	var newbufLen int
	if oldkey != nil {
		if bytes.Equal(oldkey, key) {
			logging.Tracef("plasmaSlice::insertSecArrayIndex \n\tSliceId %v IndexInstId %v Received Unchanged Key for "+
				"Doc Id %s. Key %v. Skipped.", mdb.id, mdb.idxInstId, docid, key)
			return
		}

		var tmpBuf []byte
		if len(oldkey) > cap(mdb.arrayBuf1[workerId]) {
			tmpBuf = make([]byte, 0, len(oldkey)*3)
		} else {
			tmpBuf = mdb.arrayBuf1[workerId]
		}

		//get the key in original form
		if mdb.idxDefn.Desc != nil {
			jsonEncoder.ReverseCollate(oldkey, mdb.idxDefn.Desc)
		}

		oldEntriesBytes, oldKeyCount, newbufLen, err = ArrayIndexItems(oldkey, mdb.arrayExprPosition,
			tmpBuf, mdb.isArrayDistinct, false)
		mdb.arrayBuf1[workerId] = resizeArrayBuf(mdb.arrayBuf1[workerId], newbufLen)

		if err != nil {
			logging.Errorf("plasmaSlice::insertSecArrayIndex SliceId %v IndexInstId %v Error in retrieving "+
				"compostite old secondary keys. Skipping docid:%s Error: %v", mdb.id, mdb.idxInstId, docid, err)
			mdb.deleteSecArrayIndex(docid, workerId)
			return 0
		}
	}

	if key != nil {

		newEntriesBytes, newKeyCount, newbufLen, err = ArrayIndexItems(key, mdb.arrayExprPosition,
			mdb.arrayBuf2[workerId], mdb.isArrayDistinct, !allowLargeKeys)
		mdb.arrayBuf2[workerId] = resizeArrayBuf(mdb.arrayBuf2[workerId], newbufLen)
		if err != nil {
			logging.Errorf("plasmaSlice::insertSecArrayIndex SliceId %v IndexInstId %v Error in creating "+
				"compostite new secondary keys. Skipping docid:%s Error: %v", mdb.id, mdb.idxInstId, docid, err)
			mdb.deleteSecArrayIndex(docid, workerId)
			return 0
		}
	}

	var indexEntriesToBeAdded, indexEntriesToBeDeleted [][]byte
	if len(oldEntriesBytes) == 0 { // It is a new key. Nothing to delete
		indexEntriesToBeDeleted = nil
		indexEntriesToBeAdded = newEntriesBytes
	} else if len(newEntriesBytes) == 0 { // New key is nil. Nothing to add
		indexEntriesToBeAdded = nil
		indexEntriesToBeDeleted = oldEntriesBytes
	} else {
		indexEntriesToBeAdded, indexEntriesToBeDeleted = CompareArrayEntriesWithCount(newEntriesBytes, oldEntriesBytes, newKeyCount, oldKeyCount)
	}

	nmut = 0

	rollbackDeletes := func(upto int) {
		for i := 0; i <= upto; i++ {
			item := indexEntriesToBeDeleted[i]
			if item != nil { // nil item indicates it should be ignored
				entry, err := NewSecondaryIndexEntry(item, docid, false,
					oldKeyCount[i], mdb.idxDefn.Desc, mdb.encodeBuf[workerId][:0])
				common.CrashOnError(err)
				// Add back
				mdb.main[workerId].InsertKV(entry, nil)
			}
		}
	}

	rollbackAdds := func(upto int) {
		for i := 0; i <= upto; i++ {
			key := indexEntriesToBeAdded[i]
			if key != nil { // nil item indicates it should be ignored
				entry, err := NewSecondaryIndexEntry(key, docid, false,
					newKeyCount[i], mdb.idxDefn.Desc, mdb.encodeBuf[workerId][:0])
				common.CrashOnError(err)
				// Delete back
				mdb.main[workerId].DeleteKV(entry)
			}
		}
	}

	// Delete each of indexEntriesToBeDeleted from main index
	for i, item := range indexEntriesToBeDeleted {
		if item != nil { // nil item indicates it should not be deleted
			var keyToBeDeleted []byte
			mdb.encodeBuf[workerId] = resizeEncodeBuf(mdb.encodeBuf[workerId], len(item), true)
			if keyToBeDeleted, err = GetIndexEntryBytes3(item, docid, false, false,
				oldKeyCount[i], mdb.idxDefn.Desc, mdb.encodeBuf[workerId]); err != nil {
				rollbackDeletes(i - 1)
				logging.Errorf("plasmaSlice::insertSecArrayIndex SliceId %v IndexInstId %v Error forming entry "+
					"to be added to main index. Skipping docid:%s Error: %v", mdb.id, mdb.idxInstId, docid, err)
				mdb.deleteSecArrayIndex(docid, workerId)
				return 0
			}
			t0 := time.Now()
			mdb.main[workerId].DeleteKV(keyToBeDeleted)
			mdb.idxStats.Timings.stKVDelete.Put(time.Now().Sub(t0))
			atomic.AddInt64(&mdb.delete_bytes, int64(len(keyToBeDeleted)))
			nmut++
		}
	}

	// Insert each of indexEntriesToBeAdded into main index
	for i, item := range indexEntriesToBeAdded {
		if item != nil { // nil item indicates it should not be added
			var keyToBeAdded []byte
			mdb.encodeBuf[workerId] = resizeEncodeBuf(mdb.encodeBuf[workerId], len(item), allowLargeKeys)
			if keyToBeAdded, err = GetIndexEntryBytes2(item, docid, false, false,
				newKeyCount[i], mdb.idxDefn.Desc, mdb.encodeBuf[workerId]); err != nil {
				rollbackDeletes(len(indexEntriesToBeDeleted) - 1)
				rollbackAdds(i - 1)
				logging.Errorf("plasmaSlice::insertSecArrayIndex SliceId %v IndexInstId %v Error forming entry "+
					"to be added to main index. Skipping docid:%s Error: %v", mdb.id, mdb.idxInstId, docid, err)
				mdb.deleteSecArrayIndex(docid, workerId)
				return 0
			}
			t0 := time.Now()
			mdb.main[workerId].InsertKV(keyToBeAdded, nil)
			mdb.idxStats.Timings.stKVSet.Put(time.Now().Sub(t0))
			atomic.AddInt64(&mdb.insert_bytes, int64(len(keyToBeAdded)))
			nmut++
		}
	}

	// If a field value changed from "existing" to "missing" (ie, key = nil),
	// we need to remove back index entry corresponding to the previous "existing" value.
	if key == nil {
		if oldkey != nil {
			t0 := time.Now()
			mdb.back[workerId].DeleteKV(docid)
			mdb.idxStats.Timings.stKVDelete.Put(time.Now().Sub(t0))
			atomic.AddInt64(&mdb.delete_bytes, int64(len(docid)))
		}
	} else { //set the back index entry <docid, encodedkey>

		//convert to storage format
		if mdb.idxDefn.Desc != nil {
			jsonEncoder.ReverseCollate(key, mdb.idxDefn.Desc)
		}

		if oldkey != nil {
			t0 := time.Now()
			mdb.back[workerId].DeleteKV(docid)
			mdb.idxStats.Timings.stKVDelete.Put(time.Now().Sub(t0))
			atomic.AddInt64(&mdb.delete_bytes, int64(len(docid)))
		}

		t0 := time.Now()
		mdb.back[workerId].InsertKV(docid, key)
		mdb.idxStats.Timings.stKVSet.Put(time.Now().Sub(t0))
		atomic.AddInt64(&mdb.insert_bytes, int64(len(docid)+len(key)))
	}

	mdb.isDirty = true
	return nmut
}

func (mdb *plasmaSlice) delete(docid []byte, workerId int) int {
	var nmut int

	if mdb.isPrimary {
		nmut = mdb.deletePrimaryIndex(docid, workerId)
	} else if !mdb.idxDefn.IsArrayIndex {
		nmut = mdb.deleteSecIndex(docid, workerId)
	} else {
		nmut = mdb.deleteSecArrayIndex(docid, workerId)
	}

	mdb.logWriterStat()
	return nmut
}

func (mdb *plasmaSlice) deletePrimaryIndex(docid []byte, workerId int) (nmut int) {
	if docid == nil {
		common.CrashOnError(errors.New("Nil Primary Key"))
		return
	}

	// docid -> key format
	entry, err := NewPrimaryIndexEntry(docid)
	common.CrashOnError(err)

	// Delete from main index
	t0 := time.Now()
	itm := entry.Bytes()

	mdb.main[workerId].Begin()
	defer mdb.main[workerId].End()

	if _, err := mdb.main[workerId].LookupKV(entry); err == plasma.ErrItemNoValue {
		mdb.main[workerId].DeleteKV(itm)
		mdb.idxStats.Timings.stKVDelete.Put(time.Now().Sub(t0))
		atomic.AddInt64(&mdb.delete_bytes, int64(len(entry.Bytes())))
		mdb.isDirty = true
		return 1
	}

	return 0
}

func (mdb *plasmaSlice) deleteSecIndex(docid []byte, workerId int) int {
	// Delete entry from back and main index if present
	mdb.back[workerId].Begin()
	defer mdb.back[workerId].End()

	backEntry, err := mdb.back[workerId].LookupKV(docid)
	mdb.encodeBuf[workerId] = resizeEncodeBuf(mdb.encodeBuf[workerId], len(backEntry), true)
	buf := mdb.encodeBuf[workerId]

	if err == nil {
		t0 := time.Now()
		atomic.AddInt64(&mdb.delete_bytes, int64(len(docid)))
		mdb.main[workerId].Begin()
		defer mdb.main[workerId].End()
		mdb.back[workerId].DeleteKV(docid)
		entry := backEntry2entry(docid, backEntry, buf)
		mdb.main[workerId].DeleteKV(entry)
		mdb.idxStats.Timings.stKVDelete.Put(time.Since(t0))
	}

	mdb.isDirty = true
	return 1
}

func (mdb *plasmaSlice) deleteSecArrayIndex(docid []byte, workerId int) (nmut int) {
	var olditm []byte
	var err error

	mdb.back[workerId].Begin()
	defer mdb.back[workerId].End()
	olditm, err = mdb.back[workerId].LookupKV(docid)
	if err == plasma.ErrItemNotFound {
		olditm = nil
	}

	if olditm == nil {
		logging.Tracef("plasmaSlice::deleteSecArrayIndex \n\tSliceId %v IndexInstId %v Received NIL Key for "+
			"Doc Id %v. Skipped.", mdb.id, mdb.idxInstId, docid)
		return
	}

	var tmpBuf []byte
	if len(olditm) > cap(mdb.arrayBuf1[workerId]) {
		tmpBuf = make([]byte, 0, len(olditm)*3)
	} else {
		tmpBuf = mdb.arrayBuf1[workerId]
	}

	//get the key in original form
	if mdb.idxDefn.Desc != nil {
		jsonEncoder.ReverseCollate(olditm, mdb.idxDefn.Desc)
	}

	indexEntriesToBeDeleted, keyCount, _, err := ArrayIndexItems(olditm, mdb.arrayExprPosition,
		tmpBuf, mdb.isArrayDistinct, false)
	if err != nil {
		// TODO: Do not crash for non-storage operation. Force delete the old entries
		common.CrashOnError(err)
		logging.Errorf("plasmaSlice::deleteSecArrayIndex \n\tSliceId %v IndexInstId %v Error in retrieving "+
			"compostite old secondary keys %v", mdb.id, mdb.idxInstId, err)
		return
	}

	mdb.main[workerId].Begin()
	defer mdb.main[workerId].End()

	var t0 time.Time
	// Delete each of indexEntriesToBeDeleted from main index
	for i, item := range indexEntriesToBeDeleted {
		var keyToBeDeleted []byte
		var tmpBuf []byte
		tmpBuf = resizeEncodeBuf(mdb.encodeBuf[workerId], len(item), true)
		// TODO: Use method that skips size check for bug MB-22183
		if keyToBeDeleted, err = GetIndexEntryBytes3(item, docid, false, false, keyCount[i],
			mdb.idxDefn.Desc, tmpBuf); err != nil {
			common.CrashOnError(err)
			logging.Errorf("plasmaSlice::deleteSecArrayIndex \n\tSliceId %v IndexInstId %v Error from GetIndexEntryBytes2 "+
				"for entry to be deleted from main index %v", mdb.id, mdb.idxInstId, err)
			return
		}
		t0 := time.Now()
		mdb.main[workerId].DeleteKV(keyToBeDeleted)
		mdb.idxStats.Timings.stKVDelete.Put(time.Now().Sub(t0))
		atomic.AddInt64(&mdb.delete_bytes, int64(len(keyToBeDeleted)))
	}

	//delete from the back index
	t0 = time.Now()
	mdb.back[workerId].DeleteKV(docid)
	mdb.idxStats.Timings.stKVDelete.Put(time.Now().Sub(t0))
	atomic.AddInt64(&mdb.delete_bytes, int64(len(docid)))
	mdb.isDirty = true
	return len(indexEntriesToBeDeleted)
}

//checkFatalDbError checks if the error returned from DB
//is fatal and stores it. This error will be returned
//to caller on next DB operation
func (mdb *plasmaSlice) checkFatalDbError(err error) {

	//panic on all DB errors and recover rather than risk
	//inconsistent db state
	common.CrashOnError(err)

	errStr := err.Error()
	switch errStr {

	case "checksum error", "file corruption", "no db instance",
		"alloc fail", "seek fail", "fsync fail":
		mdb.fatalDbErr = err

	}

}

type plasmaSnapshotInfo struct {
	Ts        *common.TsVbuuid
	Committed bool
	Count     int64

	mRP, bRP *plasma.RecoveryPoint
}

type plasmaSnapshot struct {
	slice     *plasmaSlice
	idxDefnId common.IndexDefnId
	idxInstId common.IndexInstId
	ts        *common.TsVbuuid
	info      SnapshotInfo

	MainSnap *plasma.Snapshot
	BackSnap *plasma.Snapshot

	committed bool

	refCount int32
}

// Creates an open snapshot handle from snapshot info
// Snapshot info is obtained from NewSnapshot() or GetSnapshots() API
// Returns error if snapshot handle cannot be created.
func (mdb *plasmaSlice) OpenSnapshot(info SnapshotInfo) (Snapshot, error) {
	snapInfo := info.(*plasmaSnapshotInfo)

	s := &plasmaSnapshot{slice: mdb,
		idxDefnId: mdb.idxDefnId,
		idxInstId: mdb.idxInstId,
		info:      info,
		ts:        snapInfo.Timestamp(),
		committed: info.IsCommitted(),
		MainSnap:  mdb.mainstore.NewSnapshot(),
	}

	if !mdb.isPrimary {
		s.BackSnap = mdb.backstore.NewSnapshot()
	}

	s.Open()
	s.slice.IncrRef()

	if s.committed && mdb.hasPersistence {
		mdb.doPersistSnapshot(s)
	}

	logging.Infof("plasmaSlice::OpenSnapshot SliceId %v IndexInstId %v Creating New "+
		"Snapshot %v", mdb.id, mdb.idxInstId, snapInfo)
	mdb.setCommittedCount()

	return s, nil
}

func (mdb *plasmaSlice) doPersistSnapshot(s *plasmaSnapshot) {
	var wg sync.WaitGroup

	if atomic.CompareAndSwapInt32(&mdb.isPersistorActive, 0, 1) {
		s.MainSnap.Open()
		if !mdb.isPrimary {
			s.BackSnap.Open()
		}

		go func() {
			defer atomic.StoreInt32(&mdb.isPersistorActive, 0)

			logging.Infof("PlasmaSlice Slice Id %v, IndexInstId %v Creating recovery point ...", mdb.id, mdb.idxInstId)
			t0 := time.Now()

			meta, err := json.Marshal(s.ts)
			common.CrashOnError(err)
			timeHdr := make([]byte, 8)
			binary.BigEndian.PutUint64(timeHdr, uint64(time.Now().UnixNano()))
			meta = append(timeHdr, meta...)

			wg.Add(1)
			go func() {
				mdb.mainstore.CreateRecoveryPoint(s.MainSnap, meta)
				wg.Done()
			}()

			if !mdb.isPrimary {
				mdb.backstore.CreateRecoveryPoint(s.BackSnap, meta)
			}
			wg.Wait()

			dur := time.Since(t0)
			logging.Infof("PlasmaSlice Slice Id %v, IndexInstId %v Created recovery point (took %v)",
				mdb.id, mdb.idxInstId, dur)
			mdb.idxStats.diskSnapStoreDuration.Set(int64(dur / time.Millisecond))

			// Cleanup old recovery points
			mRPs := mdb.mainstore.GetRecoveryPoints()
			if len(mRPs) > mdb.maxRollbacks {
				for i := 0; i < len(mRPs)-mdb.maxRollbacks; i++ {
					mdb.mainstore.RemoveRecoveryPoint(mRPs[i])
				}
			}

			if !mdb.isPrimary {
				bRPs := mdb.backstore.GetRecoveryPoints()
				if len(bRPs) > mdb.maxRollbacks {
					for i := 0; i < len(bRPs)-mdb.maxRollbacks; i++ {
						mdb.backstore.RemoveRecoveryPoint(bRPs[i])
					}
				}
			}
		}()
	} else {
		logging.Infof("PlasmaSlice Slice Id %v, IndexInstId %v Skipping ondisk"+
			" snapshot. A snapshot writer is in progress.", mdb.id, mdb.idxInstId)
	}
}

func (mdb *plasmaSlice) GetSnapshots() ([]SnapshotInfo, error) {
	var mRPs, bRPs []*plasma.RecoveryPoint
	var minRP, maxRP []byte

	getRPs := func(rpts []*plasma.RecoveryPoint) []*plasma.RecoveryPoint {
		var newRpts []*plasma.RecoveryPoint
		for _, rp := range rpts {
			if cmpRPMeta(rp.Meta(), minRP) < 0 {
				continue
			}

			if cmpRPMeta(rp.Meta(), maxRP) > 0 {
				break
			}

			newRpts = append(newRpts, rp)
		}

		return newRpts
	}

	// Find out the common recovery points between mainIndex and backIndex
	mRPs = mdb.mainstore.GetRecoveryPoints()
	if len(mRPs) > 0 {
		minRP = mRPs[0].Meta()
		maxRP = mRPs[len(mRPs)-1].Meta()
	}

	if !mdb.isPrimary {
		bRPs = mdb.backstore.GetRecoveryPoints()
		if len(bRPs) > 0 {
			if cmpRPMeta(bRPs[0].Meta(), minRP) > 0 {
				minRP = bRPs[0].Meta()
			}

			if cmpRPMeta(bRPs[len(bRPs)-1].Meta(), maxRP) < 0 {
				maxRP = bRPs[len(bRPs)-1].Meta()
			}
		}

		bRPs = getRPs(bRPs)
	}

	mRPs = getRPs(mRPs)

	if !mdb.isPrimary && len(mRPs) != len(bRPs) {
		return nil, nil
	}

	var infos []SnapshotInfo
	for i := len(mRPs) - 1; i >= 0; i-- {
		info := &plasmaSnapshotInfo{
			mRP:   mRPs[i],
			Count: mRPs[i].ItemsCount(),
		}

		if err := json.Unmarshal(info.mRP.Meta()[8:], &info.Ts); err != nil {
			return nil, fmt.Errorf("Unable to decode snapshot meta err %v", err)
		}

		if !mdb.isPrimary {
			info.bRP = bRPs[i]
		}

		infos = append(infos, info)
	}

	return infos, nil
}

func (mdb *plasmaSlice) setCommittedCount() {
	curr := mdb.mainstore.ItemsCount()
	atomic.StoreUint64(&mdb.committedCount, uint64(curr))
}

func (mdb *plasmaSlice) GetCommittedCount() uint64 {
	return atomic.LoadUint64(&mdb.committedCount)
}

func (mdb *plasmaSlice) resetStores() {
	// Clear all readers
	for i := 0; i < cap(mdb.readers); i++ {
		<-mdb.readers
	}

	mdb.mainstore.Close()
	if !mdb.isPrimary {
		mdb.backstore.Close()
	}

	os.RemoveAll(mdb.path)
	mdb.newBorn = true
	mdb.initStores()
}

func (mdb *plasmaSlice) Rollback(o SnapshotInfo) error {
	mdb.waitPersist()
	mdb.waitForPersistorThread()
	qc := atomic.LoadInt64(&mdb.qCount)
	if qc > 0 {
		common.CrashOnError(errors.New("Slice Invariant Violation - rollback with pending mutations"))
	}

	// Block all scan requests
	var readers []*plasma.Reader
	for i := 0; i < cap(mdb.readers); i++ {
		readers = append(readers, <-mdb.readers)
	}

	err := mdb.restore(o)
	for i := 0; i < cap(mdb.readers); i++ {
		mdb.readers <- readers[i]
	}

	return err
}

func (mdb *plasmaSlice) restore(o SnapshotInfo) error {
	var wg sync.WaitGroup
	var mErr, bErr error
	info := o.(*plasmaSnapshotInfo)
	wg.Add(1)
	go func() {
		defer wg.Done()
		var s *plasma.Snapshot
		if s, mErr = mdb.mainstore.Rollback(info.mRP); mErr == nil {
			s.Close()
		}
	}()

	if !mdb.isPrimary {

		wg.Add(1)
		go func() {
			defer wg.Done()
			var s *plasma.Snapshot
			if s, bErr = mdb.backstore.Rollback(info.bRP); bErr == nil {
				s.Close()
			}
		}()
	}

	wg.Wait()

	if mErr != nil || bErr != nil {
		return fmt.Errorf("Rollback error %v %v", mErr, bErr)
	}

	return nil
}

//RollbackToZero rollbacks the slice to initial state. Return error if
//not possible
func (mdb *plasmaSlice) RollbackToZero() error {
	mdb.waitPersist()
	mdb.waitForPersistorThread()

	mdb.resetStores()

	return nil
}

//slice insert/delete methods are async. There
//can be outstanding mutations in internal queue to flush even
//after insert/delete have return success to caller.
//This method provides a mechanism to wait till internal
//queue is empty.
func (mdb *plasmaSlice) waitPersist() {

	if !mdb.checkAllWorkersDone() {
		//every SLICE_COMMIT_POLL_INTERVAL milliseconds,
		//check for outstanding mutations. If there are
		//none, proceed with the commit.
		mdb.confLock.RLock()
		commitPollInterval := mdb.sysconf["storage.moi.commitPollInterval"].Uint64()
		mdb.confLock.RUnlock()

		for {
			if mdb.checkAllWorkersDone() {
				break
			}
			time.Sleep(time.Millisecond * time.Duration(commitPollInterval))
		}
	}

}

//Commit persists the outstanding writes in underlying
//forestdb database. If Commit returns error, slice
//should be rolled back to previous snapshot.
func (mdb *plasmaSlice) NewSnapshot(ts *common.TsVbuuid, commit bool) (SnapshotInfo, error) {

	mdb.waitPersist()

	qc := atomic.LoadInt64(&mdb.qCount)
	if qc > 0 {
		common.CrashOnError(errors.New("Slice Invariant Violation - commit with pending mutations"))
	}

	mdb.isDirty = false

	newSnapshotInfo := &plasmaSnapshotInfo{
		Ts:        ts,
		Committed: commit,
		Count:     mdb.mainstore.ItemsCount(),
	}

	return newSnapshotInfo, nil
}

//checkAllWorkersDone return true if all workers have
//finished processing
func (mdb *plasmaSlice) checkAllWorkersDone() bool {

	//if there are mutations in the cmdCh, workers are
	//not yet done
	if mdb.getCmdsCount() > 0 {
		return false
	}

	//worker queue is empty, make sure both workers are done
	//processing the last mutation
	for i := 0; i < mdb.numWriters; i++ {
		mdb.workerDone[i] <- true
		<-mdb.workerDone[i]
	}

	return true
}

func (mdb *plasmaSlice) Close() {
	mdb.lock.Lock()
	defer mdb.lock.Unlock()

	logging.Infof("plasmaSlice::Close Closing Slice Id %v, IndexInstId %v, "+
		"IndexDefnId %v", mdb.idxInstId, mdb.idxDefnId, mdb.id)

	//signal shutdown for command handler routines
	for i := 0; i < mdb.numWriters; i++ {
		mdb.stopCh[i] <- true
		<-mdb.stopCh[i]
	}

	if mdb.refCount > 0 {
		mdb.isSoftClosed = true
	} else {
		tryCloseplasmaSlice(mdb)
	}
}

//Destroy removes the database file from disk.
//Slice is not recoverable after this.
func (mdb *plasmaSlice) Destroy() {
	mdb.lock.Lock()
	defer mdb.lock.Unlock()

	if mdb.refCount > 0 {
		logging.Infof("plasmaSlice::Destroy Softdeleted Slice Id %v, IndexInstId %v, "+
			"IndexDefnId %v", mdb.id, mdb.idxInstId, mdb.idxDefnId)
		mdb.isSoftDeleted = true
	} else {
		tryDeleteplasmaSlice(mdb)
	}
}

//Id returns the Id for this Slice
func (mdb *plasmaSlice) Id() SliceId {
	return mdb.id
}

// FilePath returns the filepath for this Slice
func (mdb *plasmaSlice) Path() string {
	return mdb.path
}

//IsActive returns if the slice is active
func (mdb *plasmaSlice) IsActive() bool {
	return mdb.isActive
}

//SetActive sets the active state of this slice
func (mdb *plasmaSlice) SetActive(isActive bool) {
	mdb.isActive = isActive
}

//Status returns the status for this slice
func (mdb *plasmaSlice) Status() SliceStatus {
	return mdb.status
}

//SetStatus set new status for this slice
func (mdb *plasmaSlice) SetStatus(status SliceStatus) {
	mdb.status = status
}

//IndexInstId returns the Index InstanceId this
//slice is associated with
func (mdb *plasmaSlice) IndexInstId() common.IndexInstId {
	return mdb.idxInstId
}

//IndexDefnId returns the Index DefnId this slice
//is associated with
func (mdb *plasmaSlice) IndexDefnId() common.IndexDefnId {
	return mdb.idxDefnId
}

// IsDirty returns true if there has been any change in
// in the slice storage after last in-mem/persistent snapshot
func (mdb *plasmaSlice) IsDirty() bool {
	mdb.waitPersist()
	return mdb.isDirty
}

func (mdb *plasmaSlice) Compact(abortTime time.Time) error {
	return nil
}

func (mdb *plasmaSlice) Statistics() (StorageStatistics, error) {
	var sts StorageStatistics

	var internalData []string

	var numRecsMem, numRecsDisk, cacheHits, cacheMiss int64
	pStats := mdb.mainstore.GetStats()

	numRecsMem += pStats.NumRecordAllocs - pStats.NumRecordFrees
	numRecsDisk += pStats.NumRecordSwapOut - pStats.NumRecordSwapIn
	cacheHits += pStats.CacheHits
	cacheMiss += pStats.CacheMisses
        sts.MemUsed = pStats.MemSz + pStats.MemSzIndex

	internalData = append(internalData, fmt.Sprintf("{\n\"MainStore\":\n%s", pStats))
	if !mdb.isPrimary {
		pStats := mdb.backstore.GetStats()
		numRecsMem += pStats.NumRecordAllocs - pStats.NumRecordFrees
		numRecsDisk += pStats.NumRecordSwapOut - pStats.NumRecordSwapIn
		cacheHits += pStats.CacheHits
		cacheMiss += pStats.CacheMisses
                sts.MemUsed += pStats.MemSz + pStats.MemSzIndex
		internalData = append(internalData, fmt.Sprintf(",\n\"BackStore\":\n%s", pStats))
	}

	internalData = append(internalData, "}\n")

	sts.InternalData = internalData
	if mdb.hasPersistence {
		_, sts.DataSize, sts.DiskSize = mdb.mainstore.GetLSSInfo()
		if !mdb.isPrimary {
			_, bsDataSz, bsDiskSz := mdb.backstore.GetLSSInfo()
			sts.DataSize += bsDataSz
			sts.DiskSize += bsDiskSz
		}
	}

	mdb.idxStats.residentPercent.Set(common.ComputePercent(numRecsMem, numRecsDisk))
	mdb.idxStats.cacheHitPercent.Set(common.ComputePercent(cacheHits, cacheMiss))
	return sts, nil
}

func updatePlasmaConfig(cfg common.Config) {
	plasma.MTunerMaxFreeMemory = int64(cfg["plasma.memtuner.maxFreeMemory"].Int())
	plasma.MTunerMinFreeMemRatio = cfg["plasma.memtuner.minFreeRatio"].Float64()
	plasma.MTunerTrimDownRatio = cfg["plasma.memtuner.trimDownRatio"].Float64()
	plasma.MTunerIncrementRatio = cfg["plasma.memtuner.incrementRatio"].Float64()
	plasma.MTunerMinQuotaRatio = cfg["plasma.memtuner.minQuotaRatio"].Float64()
	plasma.MTunerIncrCeilPercent = cfg["plasma.memtuner.incrCeilPercent"].Float64()
	plasma.MTunerMinQuota = int64(cfg["plasma.memtuner.minQuota"].Int())
}

func (mdb *plasmaSlice) UpdateConfig(cfg common.Config) {
	mdb.confLock.Lock()
	defer mdb.confLock.Unlock()

	mdb.sysconf = cfg

	updatePlasmaConfig(cfg)
	mdb.mainstore.CheckpointInterval = time.Second * time.Duration(cfg["plasma.checkpointInterval"].Int())
	mdb.mainstore.MaxPageLSSSegments = mdb.sysconf["plasma.mainIndex.maxLSSPageSegments"].Int()
	mdb.mainstore.LSSCleanerThreshold = mdb.sysconf["plasma.mainIndex.LSSFragmentation"].Int()
	mdb.mainstore.LSSCleanerMaxThreshold = mdb.sysconf["plasma.mainIndex.maxLSSFragmentation"].Int()
	mdb.mainstore.DisableReadCaching = mdb.sysconf["plasma.disableReadCaching"].Bool()

	mdb.mainstore.PurgerInterval = time.Duration(mdb.sysconf["plasma.purger.interval"].Int()) * time.Second
	mdb.mainstore.PurgeThreshold = mdb.sysconf["plasma.purger.highThreshold"].Float64()
	mdb.mainstore.PurgeLowThreshold = mdb.sysconf["plasma.purger.lowThreshold"].Float64()
	mdb.mainstore.PurgeCompactRatio = mdb.sysconf["plasma.purger.compactRatio"].Float64()
	mdb.mainstore.EnableLSSPageSMO = mdb.sysconf["plasma.enableLSSPageSMO"].Bool()

	if !mdb.isPrimary {
		mdb.backstore.CheckpointInterval = mdb.mainstore.CheckpointInterval
		mdb.backstore.MaxPageLSSSegments = mdb.sysconf["plasma.backIndex.maxLSSPageSegments"].Int()
		mdb.backstore.LSSCleanerThreshold = mdb.sysconf["plasma.backIndex.LSSFragmentation"].Int()
		mdb.backstore.LSSCleanerMaxThreshold = mdb.sysconf["plasma.backIndex.maxLSSFragmentation"].Int()
		mdb.backstore.DisableReadCaching = mdb.sysconf["plasma.disableReadCaching"].Bool()

		mdb.backstore.PurgerInterval = time.Duration(mdb.sysconf["plasma.purger.interval"].Int()) * time.Second
		mdb.backstore.PurgeThreshold = mdb.sysconf["plasma.purger.highThreshold"].Float64()
		mdb.backstore.PurgeLowThreshold = mdb.sysconf["plasma.purger.lowThreshold"].Float64()
		mdb.backstore.PurgeCompactRatio = mdb.sysconf["plasma.purger.compactRatio"].Float64()
		mdb.backstore.EnableLSSPageSMO = mdb.sysconf["plasma.enableLSSPageSMO"].Bool()
	}
}

func (mdb *plasmaSlice) String() string {

	str := fmt.Sprintf("SliceId: %v ", mdb.id)
	str += fmt.Sprintf("File: %v ", mdb.path)
	str += fmt.Sprintf("Index: %v ", mdb.idxInstId)

	return str

}

func tryDeleteplasmaSlice(mdb *plasmaSlice) {
	//cleanup the disk directory
	if err := os.RemoveAll(mdb.path); err != nil {
		logging.Errorf("plasmaSlice::Destroy Error Cleaning Up Slice Id %v, "+
			"IndexInstId %v, IndexDefnId %v. Error %v", mdb.id, mdb.idxInstId, mdb.idxDefnId, err)
	}
}

func tryCloseplasmaSlice(mdb *plasmaSlice) {
	mdb.waitForPersistorThread()
	mdb.mainstore.Close()

	if !mdb.isPrimary {
		mdb.backstore.Close()
	}
}

func (mdb *plasmaSlice) getCmdsCount() int {
	qc := atomic.LoadInt64(&mdb.qCount)
	return int(qc)
}

func (mdb *plasmaSlice) logWriterStat() {
	count := atomic.AddUint64(&mdb.flushedCount, 1)
	if (count%10000 == 0) || count == 1 {
		logging.Infof("logWriterStat:: %v "+
			"FlushedCount %v QueuedCount %v", mdb.idxInstId,
			count, mdb.getCmdsCount())
	}

}

func (info *plasmaSnapshotInfo) Timestamp() *common.TsVbuuid {
	return info.Ts
}

func (info *plasmaSnapshotInfo) IsCommitted() bool {
	return info.Committed
}

func (info *plasmaSnapshotInfo) String() string {
	return fmt.Sprintf("SnapshotInfo: count:%v committed:%v", info.Count, info.Committed)
}

func (s *plasmaSnapshot) Create() error {
	return nil
}

func (s *plasmaSnapshot) Open() error {
	atomic.AddInt32(&s.refCount, int32(1))

	return nil
}

func (s *plasmaSnapshot) IsOpen() bool {

	count := atomic.LoadInt32(&s.refCount)
	return count > 0
}

func (s *plasmaSnapshot) Id() SliceId {
	return s.slice.Id()
}

func (s *plasmaSnapshot) IndexInstId() common.IndexInstId {
	return s.idxInstId
}

func (s *plasmaSnapshot) IndexDefnId() common.IndexDefnId {
	return s.idxDefnId
}

func (s *plasmaSnapshot) Timestamp() *common.TsVbuuid {
	return s.ts
}

func (s *plasmaSnapshot) Close() error {

	count := atomic.AddInt32(&s.refCount, int32(-1))

	if count < 0 {
		logging.Errorf("plasmaSnapshot::Close Close operation requested " +
			"on already closed snapshot")
		return errors.New("Snapshot Already Closed")

	} else if count == 0 {
		s.Destroy()
	}

	return nil
}

func (mdb *plasmaSlice) waitForPersistorThread() {
	for atomic.LoadInt32(&mdb.isPersistorActive) == 1 {
		time.Sleep(time.Second)
	}
}

func (s *plasmaSnapshot) Destroy() {
	s.MainSnap.Close()
	if s.BackSnap != nil {
		s.BackSnap.Close()
	}

	defer s.slice.DecrRef()
}

func (s *plasmaSnapshot) String() string {

	str := fmt.Sprintf("Index: %v ", s.idxInstId)
	str += fmt.Sprintf("SliceId: %v ", s.slice.Id())
	str += fmt.Sprintf("TS: %v ", s.ts)
	return str
}

func (s *plasmaSnapshot) Info() SnapshotInfo {
	return s.info
}

// ==============================
// Snapshot reader implementation
// ==============================

// Approximate items count
func (s *plasmaSnapshot) StatCountTotal() (uint64, error) {
	c := s.slice.GetCommittedCount()
	return c, nil
}

func (s *plasmaSnapshot) CountTotal(ctx IndexReaderContext, stopch StopChannel) (uint64, error) {
	return uint64(s.MainSnap.Count()), nil
}

func (s *plasmaSnapshot) CountRange(ctx IndexReaderContext, low, high IndexKey, inclusion Inclusion,
	stopch StopChannel) (uint64, error) {

	var count uint64
	callb := func([]byte) error {
		select {
		case <-stopch:
			return common.ErrClientCancel
		default:
			count++
		}

		return nil
	}

	err := s.Range(ctx, low, high, inclusion, callb)
	return count, err
}

func (s *plasmaSnapshot) MultiScanCount(ctx IndexReaderContext, low, high IndexKey, inclusion Inclusion,
	scan Scan, distinct bool,
	stopch StopChannel) (uint64, error) {

	var err error
	var scancount uint64
	count := 1
	checkDistinct := distinct && !s.isPrimary()
	isIndexComposite := len(s.slice.idxDefn.SecExprs) > 1

	buf := secKeyBufPool.Get()
	defer secKeyBufPool.Put(buf)

	previousRow := ctx.GetCursorKey()

	revbuf := secKeyBufPool.Get()
	defer secKeyBufPool.Put(revbuf)

	callb := func(entry []byte) error {
		select {
		case <-stopch:
			return common.ErrClientCancel
		default:
			skipRow := false
			var ck [][]byte

			//get the key in original format
			// TODO: ONLY if scan.ScanType == FilterRangeReq || (checkDistinct && isIndexComposite) {
			if s.slice.idxDefn.Desc != nil {
				revbuf := (*revbuf)[:0]
				//copy is required, otherwise storage may get updated
				revbuf = append(revbuf, entry...)
				jsonEncoder.ReverseCollate(revbuf, s.slice.idxDefn.Desc)
				entry = revbuf
			}
			if scan.ScanType == FilterRangeReq {
				if len(entry) > cap(*buf) {
					*buf = make([]byte, 0, len(entry)+RESIZE_PAD)
				}

				skipRow, ck, err = filterScanRow(entry, scan, (*buf)[:0])
				if err != nil {
					return err
				}
			}
			if skipRow {
				return nil
			}

			if checkDistinct {
				if isIndexComposite {
					// For Count Distinct, only leading key needs to be considered for
					// distinct comparison as N1QL syntax supports distinct on only single key
					entry, err = projectLeadingKey(ck, entry, buf)
				}
				if len(*previousRow) != 0 && distinctCompare(entry, *previousRow) {
					return nil // Ignore the entry as it is same as previous entry
				}
			}

			if !s.isPrimary() {
				e := secondaryIndexEntry(entry)
				count = e.Count()
			}

			if checkDistinct {
				scancount++
				*previousRow = append((*previousRow)[:0], entry...)
			} else {
				scancount += uint64(count)
			}
		}
		return nil
	}
	e := s.Range(ctx, low, high, inclusion, callb)
	return scancount, e
}

func (s *plasmaSnapshot) CountLookup(ctx IndexReaderContext, keys []IndexKey, stopch StopChannel) (uint64, error) {
	var err error
	var count uint64

	callb := func([]byte) error {
		select {
		case <-stopch:
			return common.ErrClientCancel
		default:
			count++
		}

		return nil
	}

	for _, k := range keys {
		if err = s.Lookup(ctx, k, callb); err != nil {
			break
		}
	}

	return count, err
}

func (s *plasmaSnapshot) Exists(ctx IndexReaderContext, key IndexKey, stopch StopChannel) (bool, error) {
	var count uint64
	callb := func([]byte) error {
		select {
		case <-stopch:
			return common.ErrClientCancel
		default:
			count++
		}

		return nil
	}

	err := s.Lookup(ctx, key, callb)
	return count != 0, err
}

func (s *plasmaSnapshot) Lookup(ctx IndexReaderContext, key IndexKey, callb EntryCallback) error {
	return s.Iterate(ctx, key, key, Both, compareExact, callb)
}

func (s *plasmaSnapshot) Range(ctx IndexReaderContext, low, high IndexKey, inclusion Inclusion,
	callb EntryCallback) error {

	var cmpFn CmpEntry
	if s.isPrimary() {
		cmpFn = compareExact
	} else {
		cmpFn = comparePrefix
	}

	return s.Iterate(ctx, low, high, inclusion, cmpFn, callb)
}

func (s *plasmaSnapshot) All(ctx IndexReaderContext, callb EntryCallback) error {
	return s.Range(ctx, MinIndexKey, MaxIndexKey, Both, callb)
}

func (s *plasmaSnapshot) Iterate(ctx IndexReaderContext, low, high IndexKey, inclusion Inclusion,
	cmpFn CmpEntry, callback EntryCallback) error {
	var entry IndexEntry
	var err error
	t0 := time.Now()

	reader := ctx.(*plasmaReaderCtx)

	it, err := reader.r.NewSnapshotIterator(s.MainSnap)

	// Snapshot became invalid due to rollback
	if err == plasma.ErrInvalidSnapshot {
		return ErrIndexRollback
	}

	defer it.Close()

	endKey := high.Bytes()
	if len(endKey) > 0 {
		if inclusion == High || inclusion == Both {
			endKey = common.GenNextBiggerKey(endKey)
		}

		it.SetEndKey(endKey)
	}

	if len(low.Bytes()) == 0 {
		it.SeekFirst()
	} else {
		it.Seek(low.Bytes())

		// Discard equal keys if low inclusion is requested
		if inclusion == Neither || inclusion == High {
			err = s.iterEqualKeys(low, it, cmpFn, nil)
			if err != nil {
				return err
			}
		}
	}
	s.slice.idxStats.Timings.stNewIterator.Put(time.Since(t0))

loop:
	for it.Valid() {
		itm := it.Key()
		entry = s.newIndexEntry(itm)

		// Iterator has reached past the high key, no need to scan further
		if cmpFn(high, entry) <= 0 {
			break loop
		}

		err = callback(entry.Bytes())
		if err != nil {
			return err
		}

		it.Next()
	}

	// Include equal keys if high inclusion is requested
	if inclusion == Both || inclusion == High {
		err = s.iterEqualKeys(high, it, cmpFn, callback)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *plasmaSnapshot) isPrimary() bool {
	return s.slice.isPrimary
}

func (s *plasmaSnapshot) newIndexEntry(b []byte) IndexEntry {
	var entry IndexEntry
	var err error

	if s.slice.isPrimary {
		entry, err = BytesToPrimaryIndexEntry(b)
	} else {
		entry, err = BytesToSecondaryIndexEntry(b)
	}
	common.CrashOnError(err)
	return entry
}

func (s *plasmaSnapshot) iterEqualKeys(k IndexKey, it *plasma.MVCCIterator,
	cmpFn CmpEntry, callback func([]byte) error) error {
	var err error

	var entry IndexEntry
	for ; it.Valid(); it.Next() {
		itm := it.Key()
		entry = s.newIndexEntry(itm)
		if cmpFn(k, entry) == 0 {
			if callback != nil {
				err = callback(itm)
				if err != nil {
					return err
				}
			}
		} else {
			break
		}
	}

	return err
}

// TODO: Cleanup the leaky hack to reuse the buffer
// Extract only secondary key
func entry2BackEntry(entry secondaryIndexEntry) []byte {
	buf := entry.Bytes()
	kl := entry.lenKey()
	if entry.isCountEncoded() {
		// Store count
		dl := entry.lenDocId()
		copy(buf[kl:kl+2], buf[kl+dl:kl+dl+2])
		return buf[:kl+2]
	} else {
		// Set count to 0
		buf[kl] = 0
		buf[kl+1] = 0
	}

	return buf[:kl+2]
}

// Reformat secondary key to entry
func backEntry2entry(docid []byte, bentry []byte, buf []byte) []byte {
	l := len(bentry)
	count := int(binary.LittleEndian.Uint16(bentry[l-2 : l]))
	entry, _ := NewSecondaryIndexEntry2(bentry[:l-2], docid, false, count, nil, buf[:0], false)
	return entry.Bytes()
}
