// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"sync/atomic"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/common/queryutil"
	forestdb "github.com/couchbase/indexing/secondary/fdb"
	"github.com/couchbase/indexing/secondary/iowrap"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/natsort"
)

var (
	snapshotMetaListKey = []byte("snapshots-list")
)

// NewForestDBSlice initiailizes a new slice with forestdb backend.
// Both main and back index gets initialized with default config.
// Slice methods are not thread-safe and application needs to
// handle the synchronization. The only exception being Insert and
// Delete can be called concurrently.
// Returns error in case slice cannot be initialized.
func NewForestDBSlice(path string, sliceId SliceId, idxDefn common.IndexDefn,
	idxInstId common.IndexInstId, partitionId common.PartitionId,
	isPrimary bool, numPartitions int,
	sysconf common.Config, idxStats *IndexStats) (*fdbSlice, error) {

	info, err := iowrap.Os_Stat(path)
	if err != nil || err == nil && info.IsDir() {
		iowrap.Os_Mkdir(path, 0777)
	}

	filepath := newFdbFile(path, false)
	fdb := &fdbSlice{}
	fdb.idxStats = idxStats

	fdb.get_bytes = 0
	fdb.insert_bytes = 0
	fdb.delete_bytes = 0
	fdb.extraSnapDataSize = 0
	fdb.flushedCount = 0
	fdb.committedCount = 0

	config := forestdb.DefaultConfig()
	config.SetDurabilityOpt(forestdb.DRB_ASYNC)

	memQuota := sysconf.GetIndexerMemoryQuota()
	config.SetBufferCacheSize(memQuota)
	logging.Debugf("NewForestDBSlice(): buffer cache size %d", memQuota)

	prob := sysconf["settings.max_writer_lock_prob"].Int()
	config.SetMaxWriterLockProb(uint8(prob))
	walSize := sysconf["settings.wal_size"].Uint64()
	config.SetWalThreshold(walSize)
	kept_headers := sysconf["settings.recovery.max_rollbacks"].Int() + 1 //MB-20753
	config.SetNumKeepingHeaders(uint8(kept_headers))
	logging.Verbosef("NewForestDBSlice(): max writer lock prob %d", prob)
	logging.Verbosef("NewForestDBSlice(): wal size %d", walSize)
	logging.Verbosef("NewForestDBSlice(): num keeping headers %d", kept_headers)

	mode := strings.ToLower(sysconf["settings.compaction.compaction_mode"].String())
	if mode == "full" {
		config.SetBlockReuseThreshold(uint8(0))
		logging.Verbosef("NewForestDBSlice(): full compaction mode. Set Reuse Threshold to 0")
	}

	kvconfig := forestdb.DefaultKVStoreConfig()

retry:
	if fdb.dbfile, err = forestdb.Open(filepath, config); err != nil {
		if err == forestdb.FDB_RESULT_NO_DB_HEADERS {
			logging.Warnf("NewForestDBSlice(): Open failed with no_db_header error...Resetting the forestdb file")
			iowrap.Os_Remove(filepath)
			goto retry
		} else if err == forestdb.FDB_CORRUPTION_ERR {
			logging.Errorf("NewForestDBSlice(): Open failed error %v", forestdb.FDB_CORRUPTION_ERR)
			return nil, errStorageCorrupted
		}
		return nil, err
	}

	fdb.config = config
	fdb.sysconf = sysconf

	//open a separate file handle for compaction
	if fdb.compactFd, err = forestdb.Open(filepath, config); err != nil {
		if err == forestdb.FDB_CORRUPTION_ERR {
			logging.Errorf("NewForestDBSlice(): Open failed error %v", forestdb.FDB_CORRUPTION_ERR)
			return nil, errStorageCorrupted
		}

		return nil, err
	}

	config.SetOpenFlags(forestdb.OPEN_FLAG_RDONLY)
	if fdb.statFd, err = forestdb.Open(filepath, config); err != nil {
		if err == forestdb.FDB_CORRUPTION_ERR {
			logging.Errorf("NewForestDBSlice(): Open failed error %v", forestdb.FDB_CORRUPTION_ERR)
			return nil, errStorageCorrupted
		}

		return nil, err
	}
	fdb.fileVersion = fdb.statFd.GetFileVersion()
	logging.Infof("NewForestDBSlice(): file version %v", forestdb.FdbFileVersionToString(fdb.fileVersion))

	// ForestDB does not support multiwriters
	fdb.numWriters = 1
	fdb.main = make([]*forestdb.KVStore, fdb.numWriters)
	for i := 0; i < fdb.numWriters; i++ {
		if fdb.main[i], err = fdb.dbfile.OpenKVStore("main", kvconfig); err != nil {
			return nil, err
		}
	}

	//create a separate back-index for non-primary indexes
	if !isPrimary {
		fdb.back = make([]*forestdb.KVStore, fdb.numWriters)
		for i := 0; i < fdb.numWriters; i++ {
			if fdb.back[i], err = fdb.dbfile.OpenKVStore("back", kvconfig); err != nil {
				return nil, err
			}
		}
	}

	// Make use of default kvstore provided by forestdb
	if fdb.meta, err = fdb.dbfile.OpenKVStore("default", kvconfig); err != nil {
		return nil, err
	}

	fdb.path = path
	fdb.currfile = filepath
	fdb.idxInstId = idxInstId
	fdb.idxDefnId = idxDefn.DefnId
	fdb.idxDefn = idxDefn
	fdb.id = sliceId

	// Array related initialization
	_, fdb.isArrayDistinct, fdb.isArrayFlattened, fdb.arrayExprPosition, err = queryutil.GetArrayExpressionPosition(idxDefn.SecExprs)
	if err != nil {
		return nil, err
	}

	sliceBufSize := sysconf["settings.sliceBufSize"].Uint64()
	fdb.cmdCh = make(chan interface{}, sliceBufSize)
	fdb.stopCh = make([]DoneChannel, fdb.numWriters)

	fdb.isPrimary = isPrimary

	for i := 0; i < fdb.numWriters; i++ {
		fdb.stopCh[i] = make(DoneChannel)
		go fdb.handleCommandsWorker(i)
	}
	fdb.keySzConf = getKeySizeConfig(sysconf)

	logging.Infof("ForestDBSlice:NewForestDBSlice Created New Slice Id %v IndexInstId %v "+
		"WriterThreads %v", sliceId, idxInstId, fdb.numWriters)

	fdb.setCommittedCount()

	return fdb, nil
}

// kv represents a key/value pair in storage format
type indexItem struct {
	key    []byte
	rawKey []byte
	docid  []byte
}

// fdbSlice represents a forestdb slice
type fdbSlice struct {
	get_bytes, insert_bytes, delete_bytes int64
	//flushed count
	flushedCount uint64
	// persisted items count
	committedCount uint64

	// Extra data overhead due to additional snapshots
	// in the file. This is computed immediately after compaction.
	extraSnapDataSize int64

	qCount int64

	path     string
	currfile string
	id       SliceId //slice id

	refCount int
	lock     sync.RWMutex
	dbfile   *forestdb.File
	statFd   *forestdb.File
	//forestdb requires a separate file handle to be used for compaction
	//as we need to allow concurrent db updates to happen on existing file handle
	//while compaction is running in the background.
	compactFd *forestdb.File

	fileVersion uint8

	metaLock sync.Mutex
	meta     *forestdb.KVStore   // handle for index meta
	main     []*forestdb.KVStore // handle for forward index
	back     []*forestdb.KVStore // handle for reverse index

	config *forestdb.Config

	idxDefn   common.IndexDefn
	idxDefnId common.IndexDefnId
	idxInstId common.IndexInstId

	flushActive uint32

	status        SliceStatus
	isActive      bool
	isDirty       bool
	isPrimary     bool
	isSoftDeleted bool
	isSoftClosed  bool
	isClosed      bool
	isDeleted     bool
	isCompacting  bool

	cmdCh  chan interface{} //internal channel to buffer commands
	stopCh []DoneChannel    //internal channel to signal shutdown

	fatalDbErr error //store any fatal DB error

	numWriters int //number of writer threads

	//TODO: Remove this once these stats are
	//captured by the stats library
	totalFlushTime  time.Duration
	totalCommitTime time.Duration

	idxStats   *IndexStats
	sysconf    common.Config // system configuration settings
	confLock   sync.RWMutex  // protects sysconf
	statFdLock sync.Mutex

	lastRollbackTs *common.TsVbuuid

	// Array processing
	arrayExprPosition int
	isArrayDistinct   bool
	isArrayFlattened  bool

	keySzConf        keySizeConfig
	keySzConfChanged int32 //0 or 1: indicates if key size config has changeed or not
}

func (fdb *fdbSlice) IncrRef() {
	fdb.lock.Lock()
	defer fdb.lock.Unlock()

	fdb.refCount++
}

func (fdb *fdbSlice) CheckAndIncrRef() bool {
	fdb.lock.Lock()
	defer fdb.lock.Unlock()

	if fdb.isClosed {
		return false
	}

	fdb.refCount++

	return true
}

func (fdb *fdbSlice) DecrRef() {
	fdb.lock.Lock()
	defer fdb.lock.Unlock()

	fdb.refCount--
	if fdb.refCount == 0 {
		if fdb.isSoftClosed {
			fdb.isClosed = true
			tryCloseFdbSlice(fdb)
		}
		if fdb.isSoftDeleted {
			fdb.isDeleted = true
			tryDeleteFdbSlice(fdb)
		}
	}
}

// Insert will insert the given key/value pair from slice.
// Internally the request is buffered and executed async.
// If forestdb has encountered any fatal error condition,
// it will be returned as error.
func (fdb *fdbSlice) Insert(rawKey []byte, docid []byte, vectors [][]float32, meta *MutationMeta) error {
	szConf := fdb.updateSliceBuffers()
	key, err := GetIndexEntryBytes(rawKey, docid, fdb.idxDefn.IsPrimary, fdb.idxDefn.IsArrayIndex,
		1, fdb.idxDefn.Desc, meta, szConf)
	if err != nil {
		return err
	}

	fdb.idxStats.numDocsFlushQueued.Add(1)
	atomic.AddInt64(&fdb.qCount, 1)
	atomic.StoreUint32(&fdb.flushActive, 1)
	fdb.cmdCh <- &indexItem{key: key, rawKey: rawKey, docid: docid}
	return fdb.fatalDbErr
}

// Delete will delete the given document from slice.
// Internally the request is buffered and executed async.
// If forestdb has encountered any fatal error condition,
// it will be returned as error.
func (fdb *fdbSlice) Delete(docid []byte, meta *MutationMeta) error {
	fdb.updateSliceBuffers()
	fdb.idxStats.numDocsFlushQueued.Add(1)
	atomic.AddInt64(&fdb.qCount, 1)
	atomic.StoreUint32(&fdb.flushActive, 1)
	fdb.cmdCh <- docid
	return fdb.fatalDbErr
}

// handleCommands keep listening to any buffered
// write requests for the slice and processes
// those. This will shut itself down internal
// shutdown channel is closed.
func (fdb *fdbSlice) handleCommandsWorker(workerId int) {

	var start time.Time
	var elapsed time.Duration
	var c interface{}
	var icmd *indexItem
	var dcmd []byte

loop:
	for {
		var nmut int
		select {
		case c = <-fdb.cmdCh:
			switch c.(type) {
			case *indexItem:
				icmd = c.(*indexItem)
				start = time.Now()
				nmut = fdb.insert((*icmd).key, (*icmd).rawKey, (*icmd).docid, workerId)
				elapsed = time.Since(start)
				fdb.totalFlushTime += elapsed

			case []byte:
				dcmd = c.([]byte)
				start = time.Now()
				nmut = fdb.delete(dcmd, workerId)
				elapsed = time.Since(start)
				fdb.totalFlushTime += elapsed

			default:
				logging.Errorf("ForestDBSlice::handleCommandsWorker \n\tSliceId %v IndexInstId %v Received "+
					"Unknown Command %v", fdb.id, fdb.idxInstId, logging.TagUD(c))
			}

			fdb.idxStats.numItemsFlushed.Add(int64(nmut))
			fdb.idxStats.numDocsIndexed.Add(1)

		case <-fdb.stopCh[workerId]:
			fdb.stopCh[workerId] <- true
			break loop

		}
	}
}

func (fdb *fdbSlice) updateSliceBuffers() keySizeConfig {

	if atomic.LoadInt32(&fdb.keySzConfChanged) >= 1 {
		fdb.confLock.RLock()
		fdb.keySzConf = getKeySizeConfig(fdb.sysconf)
		fdb.confLock.RUnlock()
		// ForestDB does not support multiwriters
		// Hence, reset the slice buffer pools here
		encBufPool = common.NewByteBufferPool(fdb.keySzConf.maxIndexEntrySize + ENCODE_BUF_SAFE_PAD)
		arrayEncBufPool = common.NewByteBufferPool(fdb.keySzConf.maxArrayIndexEntrySize + ENCODE_BUF_SAFE_PAD)
		atomic.AddInt32(&fdb.keySzConfChanged, -1)
	}
	return fdb.keySzConf
}

// insert does the actual insert in forestdb
func (fdb *fdbSlice) insert(key []byte, rawKey []byte, docid []byte, workerId int) int {

	defer func() {
		atomic.AddInt64(&fdb.qCount, -1)
	}()

	var nmut int

	if fdb.isPrimary {
		nmut = fdb.insertPrimaryIndex(key, docid, workerId)
	} else if !fdb.idxDefn.IsArrayIndex {
		nmut = fdb.insertSecIndex(key, docid, workerId)
	} else {
		nmut = fdb.insertSecArrayIndex(key, rawKey, docid, workerId)
	}

	fdb.logWriterStat()
	return nmut
}

func (fdb *fdbSlice) insertPrimaryIndex(key []byte, docid []byte, workerId int) (nmut int) {
	var err error

	logging.Tracef("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Set Key - %s", fdb.id, fdb.idxInstId, logging.TagStrUD(docid))

	//check if the docid exists in the main index
	t0 := time.Now()
	if _, err = fdb.main[workerId].GetKV(key); err == nil {
		fdb.idxStats.Timings.stKVGet.Put(time.Now().Sub(t0))
		//skip
		logging.Tracef("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Key %v Already Exists. "+
			"Primary Index Update Skipped.", fdb.id, fdb.idxInstId, logging.TagStrUD(docid))
	} else if err != nil && err != forestdb.FDB_RESULT_KEY_NOT_FOUND {
		fdb.checkFatalDbError(err)
		logging.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error locating "+
			"mainindex entry %v", fdb.id, fdb.idxInstId, err)
	} else if err == forestdb.FDB_RESULT_KEY_NOT_FOUND {
		//set in main index
		t0 := time.Now()
		if err = fdb.main[workerId].SetKV(key, nil); err != nil {
			fdb.checkFatalDbError(err)
			logging.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error in Main Index Set. "+
				"Skipped Key %s. Error %v", fdb.id, fdb.idxInstId, logging.TagStrUD(docid), err)
		}
		fdb.idxStats.Timings.stKVSet.Put(time.Now().Sub(t0))
		atomic.AddInt64(&fdb.insert_bytes, int64(len(key)))
		fdb.isDirty = true
	}

	return 1
}

func (fdb *fdbSlice) insertSecIndex(key []byte, docid []byte, workerId int) (nmut int) {
	var err error
	var oldkey []byte

	//logging.Tracef("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Set Key - %s "+
	//	"Value - %s", fdb.id, fdb.idxInstId, k, v)

	//check if the docid exists in the back index
	if oldkey, err = fdb.getBackIndexEntry(docid, workerId); err != nil {
		fdb.checkFatalDbError(err)
		logging.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error locating "+
			"backindex entry %v", fdb.id, fdb.idxInstId, err)
		return
	} else if oldkey != nil {
		//If old-key from backindex matches with the new-key
		//in mutation, skip it.
		if bytes.Equal(oldkey, key) {
			logging.Tracef("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Received Unchanged Key for "+
				"Doc Id %v. Key %v. Skipped.", fdb.id, fdb.idxInstId, logging.TagStrUD(docid), logging.TagStrUD(key))
			return
		}

		//there is already an entry in main index for this docid
		//delete from main index
		t0 := time.Now()
		if err = fdb.main[workerId].DeleteKV(oldkey); err != nil {
			fdb.checkFatalDbError(err)
			logging.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error deleting "+
				"entry from main index %v", fdb.id, fdb.idxInstId, err)
			return
		}
		fdb.idxStats.Timings.stKVDelete.Put(time.Now().Sub(t0))
		atomic.AddInt64(&fdb.delete_bytes, int64(len(oldkey)))

		// If a field value changed from "existing" to "missing" (ie, key = nil),
		// we need to remove back index entry corresponding to the previous "existing" value.
		if key == nil {
			t0 := time.Now()
			if err = fdb.back[workerId].DeleteKV(docid); err != nil {
				fdb.checkFatalDbError(err)
				logging.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error deleting "+
					"entry from back index %v", fdb.id, fdb.idxInstId, err)
				return
			}

			fdb.idxStats.Timings.stKVDelete.Put(time.Now().Sub(t0))
			atomic.AddInt64(&fdb.delete_bytes, int64(len(docid)))
		}
		fdb.isDirty = true
	}

	if key == nil {
		logging.Tracef("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Received NIL Key for "+
			"Doc Id %s. Skipped.", fdb.id, fdb.idxInstId, docid)
		return
	}

	//set the back index entry <docid, encodedkey>
	t0 := time.Now()
	if err = fdb.back[workerId].SetKV(docid, key); err != nil {
		fdb.checkFatalDbError(err)
		logging.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error in Back Index Set. "+
			"Skipped Key %s. Value %v. Error %v", fdb.id, fdb.idxInstId, logging.TagStrUD(docid), logging.TagStrUD(key), err)
		return
	}
	fdb.idxStats.Timings.stKVSet.Put(time.Now().Sub(t0))
	atomic.AddInt64(&fdb.insert_bytes, int64(len(docid)+len(key)))

	t0 = time.Now()
	//set in main index
	if err = fdb.main[workerId].SetKV(key, nil); err != nil {
		fdb.checkFatalDbError(err)
		logging.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error in Main Index Set. "+
			"Skipped Key %v. Error %v", fdb.id, fdb.idxInstId, key, err)
		return
	}
	fdb.idxStats.Timings.stKVSet.Put(time.Now().Sub(t0))
	atomic.AddInt64(&fdb.insert_bytes, int64(len(key)))
	fdb.isDirty = true

	nmut = 1
	return
}

func (fdb *fdbSlice) insertSecArrayIndex(key []byte, rawKey []byte, docid []byte, workerId int) (nmut int) {
	var err error
	var oldkey []byte

	//check if the docid exists in the back index and Get old key from back index
	if oldkey, err = fdb.getBackIndexEntry(docid, workerId); err != nil {
		fdb.checkFatalDbError(err)
		logging.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error locating "+
			"backindex entry %v", fdb.id, fdb.idxInstId, err)
		return
	}

	var oldEntriesBytes, newEntriesBytes [][]byte
	var oldKeyCount, newKeyCount []int
	var newbufLen int

	if oldkey != nil {
		if bytes.Equal(oldkey, key) {
			logging.Tracef("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Received Unchanged Key for "+
				"Doc Id %s. Key %v. Skipped.", fdb.id, fdb.idxInstId, logging.TagStrUD(docid), logging.TagStrUD(key))
			return
		}

		var tmpBuf []byte
		// If old key is larger than max array limit, always handle it
		if len(oldkey) > fdb.keySzConf.maxArrayIndexEntrySize {
			// Allocate thrice the size of old key for array explosion
			tmpBuf = make([]byte, 0, len(oldkey)*3) //TODO: Revisit the size of tmpBuf
		} else {
			tmpBufPtr := arrayEncBufPool.Get()
			defer arrayEncBufPool.Put(tmpBufPtr)
			tmpBuf = (*tmpBufPtr)[:0]
		}

		//get the key in original form
		if fdb.idxDefn.Desc != nil {
			_, err = jsonEncoder.ReverseCollate(oldkey, fdb.idxDefn.Desc)
			if err != nil {
				fdb.checkFatalDbError(err)
			}
		}

		if oldEntriesBytes, oldKeyCount, _, err = ArrayIndexItems(oldkey, fdb.arrayExprPosition,
			tmpBuf, fdb.isArrayDistinct, fdb.isArrayFlattened, false, fdb.keySzConf); err != nil {
			logging.Errorf("ForestDBSlice::insert SliceId %v IndexInstId %v Error in retrieving "+
				"compostite old secondary keys. Skipping docid:%s Error: %v", fdb.id, fdb.idxInstId, logging.TagStrUD(docid), err)
			return fdb.deleteSecArrayIndex(docid, workerId)
		}
	}
	if key != nil {

		//get the key in original form
		if fdb.idxDefn.Desc != nil {
			_, err = jsonEncoder.ReverseCollate(key, fdb.idxDefn.Desc)
			if err != nil {
				fdb.checkFatalDbError(err)
			}
		}

		tmpBufPtr := arrayEncBufPool.Get()
		defer arrayEncBufPool.Put(tmpBufPtr)
		newEntriesBytes, newKeyCount, newbufLen, err = ArrayIndexItems(key, fdb.arrayExprPosition,
			(*tmpBufPtr)[:0], fdb.isArrayDistinct, fdb.isArrayFlattened, true, fdb.keySzConf)
		if err != nil {
			logging.Errorf("ForestDBSlice::insert SliceId %v IndexInstId %v Error in creating "+
				"compostite new secondary keys. Skipping docid:%s Error: %v", fdb.id, fdb.idxInstId, logging.TagStrUD(docid), err)
			return fdb.deleteSecArrayIndex(docid, workerId)
		}
		*tmpBufPtr = resizeArrayBuf((*tmpBufPtr)[:0], newbufLen, true)
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

	// Form entries to be deleted from main index
	var keysToBeDeleted [][]byte
	for i, item := range indexEntriesToBeDeleted {
		if item != nil { // nil item indicates it should not be deleted
			var keyToBeDeleted []byte
			var tmpBuf []byte
			tmpBufPtr := encBufPool.Get()
			defer encBufPool.Put(tmpBufPtr)

			if len(item)+MAX_KEY_EXTRABYTES_LEN > fdb.keySzConf.maxSecKeyBufferLen {
				tmpBuf = make([]byte, 0, len(item)+MAX_KEY_EXTRABYTES_LEN)
			} else {
				tmpBuf = (*tmpBufPtr)[:0]
			}
			// TODO: Ensure sufficient buffer size and use method that skips size check for bug MB-22183
			if keyToBeDeleted, err = GetIndexEntryBytes3(item, docid, false, false,
				oldKeyCount[i], fdb.idxDefn.Desc, tmpBuf, nil, fdb.keySzConf); err != nil {
				logging.Errorf("ForestDBSlice::insert SliceId %v IndexInstId %v Error forming entry "+
					"to be deleted from main index. Skipping docid:%s Error: %v", fdb.id, fdb.idxInstId, logging.TagStrUD(docid), err)
				return fdb.deleteSecArrayIndex(docid, workerId)
			}
			keysToBeDeleted = append(keysToBeDeleted, keyToBeDeleted)
		}
	}

	// Form entries to be inserted into main index
	var keysToBeAdded [][]byte
	for i, item := range indexEntriesToBeAdded {
		if item != nil { // nil item indicates it should not be added
			var keyToBeAdded []byte
			tmpBufPtr := encBufPool.Get()
			defer encBufPool.Put(tmpBufPtr)

			// GetIndexEntryBytes2 validates size as well expand buffer if needed
			if keyToBeAdded, err = GetIndexEntryBytes2(item, docid, false, false,
				newKeyCount[i], fdb.idxDefn.Desc, (*tmpBufPtr)[:0], nil, fdb.keySzConf); err != nil {
				logging.Errorf("ForestDBSlice::insert SliceId %v IndexInstId %v Error forming entry "+
					"to be added to main index. Skipping docid:%s Error: %v", fdb.id, fdb.idxInstId, logging.TagStrUD(docid), err)
				return fdb.deleteSecArrayIndex(docid, workerId)
			}
			keysToBeAdded = append(keysToBeAdded, keyToBeAdded)
			*tmpBufPtr = resizeArrayBuf((*tmpBufPtr)[:0], len(keysToBeAdded), true)
		}
	}

	for _, keyToBeDeleted := range keysToBeDeleted {
		t0 := time.Now()
		if err = fdb.main[workerId].DeleteKV(keyToBeDeleted); err != nil {
			fdb.checkFatalDbError(err)
			logging.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error deleting "+
				"entry from main index %v", fdb.id, fdb.idxInstId, err)
			return
		}
		fdb.idxStats.Timings.stKVDelete.Put(time.Now().Sub(t0))
		atomic.AddInt64(&fdb.delete_bytes, int64(len(oldkey)))
		nmut++
	}

	for _, keyToBeAdded := range keysToBeAdded {
		t0 := time.Now()
		//set in main index
		if err = fdb.main[workerId].SetKV(keyToBeAdded, nil); err != nil {
			fdb.checkFatalDbError(err)
			logging.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error in Main Index Set. "+
				"Skipped Key %v. Error %v", fdb.id, fdb.idxInstId, logging.TagStrUD(key), err)
			return
		}
		fdb.idxStats.Timings.stKVSet.Put(time.Now().Sub(t0))
		atomic.AddInt64(&fdb.insert_bytes, int64(len(key)))
		nmut++
	}

	// If a field value changed from "existing" to "missing" (ie, key = nil),
	// we need to remove back index entry corresponding to the previous "existing" value.
	if key == nil {
		t0 := time.Now()
		if err = fdb.back[workerId].DeleteKV(docid); err != nil {
			fdb.checkFatalDbError(err)
			logging.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error deleting "+
				"entry from back index %v", fdb.id, fdb.idxInstId, err)
			return
		}
		fdb.idxStats.Timings.stKVDelete.Put(time.Now().Sub(t0))
		atomic.AddInt64(&fdb.delete_bytes, int64(len(docid)))
	} else { //set the back index entry <docid, encodedkey>
		t0 := time.Now()

		//convert to storage format
		if fdb.idxDefn.Desc != nil {
			_, err = jsonEncoder.ReverseCollate(key, fdb.idxDefn.Desc)
			if err != nil {
				fdb.checkFatalDbError(err)
			}
		}

		if err = fdb.back[workerId].SetKV(docid, key); err != nil {
			fdb.checkFatalDbError(err)
			logging.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error in Back Index Set. "+
				"Skipped Key %s. Value %v. Error %v", fdb.id, fdb.idxInstId, logging.TagStrUD(docid), logging.TagStrUD(key), err)
			return
		}
		fdb.idxStats.Timings.stKVSet.Put(time.Now().Sub(t0))
		atomic.AddInt64(&fdb.insert_bytes, int64(len(docid)+len(key)))
	}

	fdb.isDirty = true
	return nmut
}

// delete does the actual delete in forestdb
func (fdb *fdbSlice) delete(docid []byte, workerId int) int {

	defer func() {
		atomic.AddInt64(&fdb.qCount, -1)
	}()

	var nmut int

	if fdb.isPrimary {
		nmut = fdb.deletePrimaryIndex(docid, workerId)
	} else if !fdb.idxDefn.IsArrayIndex {
		nmut = fdb.deleteSecIndex(docid, workerId)
	} else {
		nmut = fdb.deleteSecArrayIndex(docid, workerId)
	}

	fdb.logWriterStat()
	return nmut
}

func (fdb *fdbSlice) deletePrimaryIndex(docid []byte, workerId int) (nmut int) {

	//logging.Tracef("ForestDBSlice::delete \n\tSliceId %v IndexInstId %v. Delete Key - %s",
	//	fdb.id, fdb.idxInstId, docid)

	if docid == nil {
		common.CrashOnError(errors.New("Nil Primary Key"))
		return
	}

	//docid -> key format
	entry, err := NewPrimaryIndexEntry(docid)
	common.CrashOnError(err)

	//delete from main index
	t0 := time.Now()
	if err := fdb.main[workerId].DeleteKV(entry.Bytes()); err != nil {
		fdb.checkFatalDbError(err)
		logging.Errorf("ForestDBSlice::delete \n\tSliceId %v IndexInstId %v. Error deleting "+
			"entry from main index for Doc %s. Error %v", fdb.id, fdb.idxInstId,
			logging.TagStrUD(docid), err)
		return
	}
	fdb.idxStats.Timings.stKVDelete.Put(time.Now().Sub(t0))
	atomic.AddInt64(&fdb.delete_bytes, int64(len(entry.Bytes())))
	fdb.isDirty = true

	return 1
}

func (fdb *fdbSlice) deleteSecIndex(docid []byte, workerId int) (nmut int) {

	//logging.Tracef("ForestDBSlice::delete \n\tSliceId %v IndexInstId %v. Delete Key - %s",
	//	fdb.id, fdb.idxInstId, docid)

	var olditm []byte
	var err error

	if olditm, err = fdb.getBackIndexEntry(docid, workerId); err != nil {
		fdb.checkFatalDbError(err)
		logging.Errorf("ForestDBSlice::delete \n\tSliceId %v IndexInstId %v. Error locating "+
			"backindex entry for Doc %s. Error %v", fdb.id, fdb.idxInstId, logging.TagStrUD(docid), err)
		return
	}

	//if the oldkey is nil, nothing needs to be done. This is the case of deletes
	//which happened before index was created.
	if olditm == nil {
		logging.Tracef("ForestDBSlice::delete \n\tSliceId %v IndexInstId %v Received NIL Key for "+
			"Doc Id %v. Skipped.", fdb.id, fdb.idxInstId, logging.TagStrUD(docid))
		return
	}

	//delete from main index
	t0 := time.Now()
	if err = fdb.main[workerId].DeleteKV(olditm); err != nil {
		fdb.checkFatalDbError(err)
		logging.Errorf("ForestDBSlice::delete \n\tSliceId %v IndexInstId %v. Error deleting "+
			"entry from main index for Doc %s. Key %v. Error %v", fdb.id, fdb.idxInstId,
			logging.TagStrUD(docid), logging.TagStrUD(olditm), err)
		return
	}
	fdb.idxStats.Timings.stKVDelete.Put(time.Now().Sub(t0))
	atomic.AddInt64(&fdb.delete_bytes, int64(len(olditm)))

	//delete from the back index
	t0 = time.Now()
	if err = fdb.back[workerId].DeleteKV(docid); err != nil {
		fdb.checkFatalDbError(err)
		logging.Errorf("ForestDBSlice::delete \n\tSliceId %v IndexInstId %v. Error deleting "+
			"entry from back index for Doc %s. Error %v", fdb.id, fdb.idxInstId, logging.TagStrUD(docid), err)
		return
	}
	fdb.idxStats.Timings.stKVDelete.Put(time.Now().Sub(t0))
	atomic.AddInt64(&fdb.delete_bytes, int64(len(docid)))
	fdb.isDirty = true
	return 1
}

func (fdb *fdbSlice) deleteSecArrayIndex(docid []byte, workerId int) (nmut int) {
	var olditm []byte
	var err error

	if olditm, err = fdb.getBackIndexEntry(docid, workerId); err != nil {
		fdb.checkFatalDbError(err)
		logging.Errorf("ForestDBSlice::delete \n\tSliceId %v IndexInstId %v. Error locating "+
			"backindex entry for Doc %s. Error %v", fdb.id, fdb.idxInstId, logging.TagStrUD(docid), err)
		return
	}

	if olditm == nil {
		logging.Tracef("ForestDBSlice::delete \n\tSliceId %v IndexInstId %v Received NIL Key for "+
			"Doc Id %v. Skipped.", fdb.id, fdb.idxInstId, logging.TagStrUD(docid))
		return
	}

	var tmpBuf []byte
	// If old key is larger than max array limit, always handle it
	if len(olditm) > fdb.keySzConf.maxArrayIndexEntrySize {
		// Allocate thrice the size of old key for array explosion
		tmpBuf = make([]byte, 0, len(olditm)*3)
	} else {
		tmpBufPtr := arrayEncBufPool.Get()
		defer arrayEncBufPool.Put(tmpBufPtr)
		tmpBuf = (*tmpBufPtr)[:0]
	}

	//get the key in original form
	if fdb.idxDefn.Desc != nil {
		_, err = jsonEncoder.ReverseCollate(olditm, fdb.idxDefn.Desc)
		if err != nil {
			fdb.checkFatalDbError(err)
		}
	}

	indexEntriesToBeDeleted, keyCount, _, err := ArrayIndexItems(olditm, fdb.arrayExprPosition,
		tmpBuf, fdb.isArrayDistinct, fdb.isArrayFlattened, false, fdb.keySzConf)

	if err != nil {
		// TODO: Do not crash for non-storage operation. Force delete the old entries
		fdb.checkFatalDbError(err)
		logging.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error in retrieving "+
			"compostite old secondary keys %v", fdb.id, fdb.idxInstId, err)
		return
	}

	var t0 time.Time
	// Delete each of indexEntriesToBeDeleted from main index
	for i, item := range indexEntriesToBeDeleted {
		var keyToBeDeleted []byte
		var tmpBuf []byte

		tmpBufPtr := encBufPool.Get()
		defer encBufPool.Put(tmpBufPtr)

		if len(item)+MAX_KEY_EXTRABYTES_LEN > fdb.keySzConf.maxSecKeyBufferLen {
			tmpBuf = make([]byte, 0, len(item)+MAX_KEY_EXTRABYTES_LEN)
		} else {
			tmpBuf = (*tmpBufPtr)[:0]
		}

		if keyToBeDeleted, err = GetIndexEntryBytes3(item, docid, false, false, keyCount[i],
			fdb.idxDefn.Desc, tmpBuf, nil, fdb.keySzConf); err != nil {
			fdb.checkFatalDbError(err)
			logging.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error from GetIndexEntryBytes2 for entry to be deleted from main index %v", fdb.id, fdb.idxInstId, err)
			return
		}
		t0 := time.Now()
		if err = fdb.main[workerId].DeleteKV(keyToBeDeleted); err != nil {
			fdb.checkFatalDbError(err)
			logging.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error deleting "+
				"entry from main index %v", fdb.id, fdb.idxInstId, err)
			return
		}
		fdb.idxStats.Timings.stKVDelete.Put(time.Now().Sub(t0))
		atomic.AddInt64(&fdb.delete_bytes, int64(len(keyToBeDeleted)))

	}

	//delete from the back index
	t0 = time.Now()
	if err = fdb.back[workerId].DeleteKV(docid); err != nil {
		fdb.checkFatalDbError(err)
		logging.Errorf("ForestDBSlice::delete \n\tSliceId %v IndexInstId %v. Error deleting "+
			"entry from back index for Doc %s. Error %v", fdb.id, fdb.idxInstId, logging.TagStrUD(docid), err)
		return
	}
	fdb.idxStats.Timings.stKVDelete.Put(time.Now().Sub(t0))
	atomic.AddInt64(&fdb.delete_bytes, int64(len(docid)))
	fdb.isDirty = true
	return len(indexEntriesToBeDeleted)
}

// getBackIndexEntry returns an existing back index entry
// given the docid
func (fdb *fdbSlice) getBackIndexEntry(docid []byte, workerId int) ([]byte, error) {

	//	logging.Tracef("ForestDBSlice::getBackIndexEntry \n\tSliceId %v IndexInstId %v Get BackIndex Key - %s",
	//		fdb.id, fdb.idxInstId, docid)

	var kbytes []byte
	var err error

	t0 := time.Now()
	kbytes, err = fdb.back[workerId].GetKV(docid)
	fdb.idxStats.Timings.stKVGet.Put(time.Now().Sub(t0))
	atomic.AddInt64(&fdb.get_bytes, int64(len(kbytes)))

	//forestdb reports get in a non-existent key as an
	//error, skip that
	if err != nil && err != forestdb.FDB_RESULT_KEY_NOT_FOUND {
		return nil, err
	}

	return kbytes, nil
}

// checkFatalDbError checks if the error returned from DB
// is fatal and stores it. This error will be returned
// to caller on next DB operation
func (fdb *fdbSlice) checkFatalDbError(err error) {

	//panic on all DB errors and recover rather than risk
	//inconsistent db state
	common.CrashOnError(err)

	errStr := err.Error()
	switch errStr {

	case "checksum error", "file corruption", "no db instance",
		"alloc fail", "seek fail", "fsync fail":
		fdb.fatalDbErr = err

	}

}

// Creates an open snapshot handle from snapshot info
// Snapshot info is obtained from NewSnapshot() or GetSnapshots() API
// Returns error if snapshot handle cannot be created.
func (fdb *fdbSlice) OpenSnapshot(info SnapshotInfo) (Snapshot, error) {
	snapInfo := info.(*fdbSnapshotInfo)

	var s *fdbSnapshot

	s = &fdbSnapshot{slice: fdb,
		idxDefnId:  fdb.idxDefnId,
		idxInstId:  fdb.idxInstId,
		main:       fdb.main[0],
		ts:         snapInfo.Timestamp(),
		mainSeqNum: snapInfo.MainSeq,
		committed:  info.IsCommitted(),
	}

	if info.IsCommitted() {
		logging.Infof("ForestDBSlice::OpenSnapshot SliceId %v IndexInstId %v Creating New "+
			"Snapshot %v", fdb.id, fdb.idxInstId, snapInfo)
	}
	err := s.Create()
	fdb.idxStats.numOpenSnapshots.Add(1)
	return s, err
}

// Obtains counts from storage to keep track of items count and the number
// of documents present in index. We avoid updating counts here if getting store
// infos fail. This means that the counts can be stale for a window of time, but
// will get corrected on the next successful attempt.
func (fdb *fdbSlice) setCommittedCount() {

	t0 := time.Now()
	mainDbInfo, err := fdb.main[0].Info()
	if err == nil {
		docCount := mainDbInfo.DocCount()
		atomic.StoreUint64(&fdb.committedCount, docCount)
		if fdb.isPrimary {
			fdb.idxStats.docidCount.Set(int64(docCount))
		}
	} else {
		logging.Errorf("ForestDB setCommittedCount failed %v", err)
	}
	fdb.idxStats.Timings.stKVInfo.Put(time.Now().Sub(t0))

	if !fdb.isPrimary {
		backDbInfo, err := fdb.back[0].Info()
		if err == nil {
			fdb.idxStats.docidCount.Set(int64(backDbInfo.DocCount()))
		} else {
			logging.Errorf("ForestDB setCommittedCount failed to get backDbInfo %v", err)
		}
	}
}

func (fdb *fdbSlice) GetCommittedCount() uint64 {
	return atomic.LoadUint64(&fdb.committedCount)
}

// Rollback slice to given snapshot. Return error if
// not possible
func (fdb *fdbSlice) Rollback(info SnapshotInfo) error {

	//before rollback make sure there are no mutations
	//in the slice buffer. Timekeeper will make sure there
	//are no flush workers before calling rollback.
	fdb.waitPersist()

	qc := atomic.LoadInt64(&fdb.qCount)
	if qc > 0 {
		common.CrashOnError(errors.New("Slice Invariant Violation - rollback with pending mutations"))
	}

	//get the seqnum from snapshot
	snapInfo := info.(*fdbSnapshotInfo)

	infos, err := fdb.getSnapshotsMeta()
	if err != nil {
		return err
	}

	sic := NewSnapshotInfoContainer(infos)
	sic.RemoveRecentThanTS(info.Timestamp())

	//rollback meta-store first, if main/back index rollback fails, recovery
	//will pick up the rolled-back meta information.
	err = fdb.meta.Rollback(snapInfo.MetaSeq)
	if err != nil {
		logging.Errorf("ForestDBSlice::Rollback \n\tSliceId %v IndexInstId %v. Error Rollback "+
			"Meta Index to Snapshot %v. Error %v", fdb.id, fdb.idxInstId, info, err)
		return err
	}

	//call forestdb to rollback for each kv store
	err = fdb.main[0].Rollback(snapInfo.MainSeq)
	if err != nil {
		logging.Errorf("ForestDBSlice::Rollback \n\tSliceId %v IndexInstId %v. Error Rollback "+
			"Main Index to Snapshot %v. Error %v", fdb.id, fdb.idxInstId, info, err)
		return err
	}

	fdb.setCommittedCount()

	//rollback back-index only for non-primary indexes
	if !fdb.isPrimary {
		err = fdb.back[0].Rollback(snapInfo.BackSeq)
		if err != nil {
			logging.Errorf("ForestDBSlice::Rollback \n\tSliceId %v IndexInstId %v. Error Rollback "+
				"Back Index to Snapshot %v. Error %v", fdb.id, fdb.idxInstId, info, err)
			return err
		}
	}

	// Update valid snapshot list and commit
	err = fdb.updateSnapshotsMeta(sic.List())
	if err != nil {
		return err
	}

	err = fdb.dbfile.Commit(forestdb.COMMIT_MANUAL_WAL_FLUSH)

	return err
}

// RollbackToZero rollbacks the slice to initial state. Return error if
// not possible
func (fdb *fdbSlice) RollbackToZero(initialBuild bool) error {

	//before rollback make sure there are no mutations
	//in the slice buffer. Timekeeper will make sure there
	//are no flush workers before calling rollback.
	fdb.waitPersist()

	zeroSeqNum := forestdb.SeqNum(0)
	var err error

	//rollback meta-store first, if main/back index rollback fails, recovery
	//will pick up the rolled-back meta information.
	err = fdb.meta.Rollback(zeroSeqNum)
	if err != nil {
		logging.Errorf("ForestDBSlice::Rollback SliceId %v IndexInstId %v. Error Rollback "+
			"Meta Index to Zero. Error %v", fdb.id, fdb.idxInstId, err)
		return err
	}

	//call forestdb to rollback
	err = fdb.main[0].Rollback(zeroSeqNum)
	if err != nil {
		logging.Errorf("ForestDBSlice::Rollback SliceId %v IndexInstId %v. Error Rollback "+
			"Main Index to Zero. Error %v", fdb.id, fdb.idxInstId, err)
		return err
	}

	fdb.setCommittedCount()

	//rollback back-index only for non-primary indexes
	if !fdb.isPrimary {
		err = fdb.back[0].Rollback(zeroSeqNum)
		if err != nil {
			logging.Errorf("ForestDBSlice::Rollback SliceId %v IndexInstId %v. Error Rollback "+
				"Back Index to Zero. Error %v", fdb.id, fdb.idxInstId, err)
			return err
		}
	}

	fdb.lastRollbackTs = nil

	return nil
}

func (fdb *fdbSlice) LastRollbackTs() *common.TsVbuuid {
	return fdb.lastRollbackTs
}

func (fdb *fdbSlice) SetLastRollbackTs(ts *common.TsVbuuid) {
	fdb.lastRollbackTs = ts
}

// slice insert/delete methods are async. There
// can be outstanding mutations in internal queue to flush even
// after insert/delete have return success to caller.
// This method provides a mechanism to wait till internal
// queue is empty.
func (fdb *fdbSlice) waitPersist() {

	if !fdb.checkAllWorkersDone() {
		//every SLICE_COMMIT_POLL_INTERVAL milliseconds,
		//check for outstanding mutations. If there are
		//none, proceed with the commit.
		fdb.confLock.RLock()
		commitPollInterval := fdb.sysconf["storage.fdb.commitPollInterval"].Uint64()
		fdb.confLock.RUnlock()
		ticker := time.NewTicker(time.Millisecond * time.Duration(commitPollInterval))
		defer ticker.Stop()

		for _ = range ticker.C {
			if fdb.checkAllWorkersDone() {
				break
			}
		}
	}

}

// Commit persists the outstanding writes in underlying
// forestdb database. If Commit returns error, slice
// should be rolled back to previous snapshot.
func (fdb *fdbSlice) NewSnapshot(ts *common.TsVbuuid, commit bool) (SnapshotInfo, error) {

	flushStart := time.Now()
	fdb.waitPersist()
	flushTime := time.Since(flushStart)

	qc := atomic.LoadInt64(&fdb.qCount)
	if qc > 0 {
		common.CrashOnError(errors.New("Slice Invariant Violation - commit with pending mutations"))
	}

	fdb.isDirty = false

	// Coming here means that cmdCh is empty and flush has finished for this index
	atomic.StoreUint32(&fdb.flushActive, 0)

	t0 := time.Now()
	mainDbInfo, err := fdb.main[0].Info()
	if err != nil {
		return nil, err
	}
	fdb.idxStats.Timings.stKVInfo.Put(time.Now().Sub(t0))

	newSnapshotInfo := &fdbSnapshotInfo{
		Ts:        ts,
		MainSeq:   mainDbInfo.LastSeqNum(),
		Committed: commit,
	}

	//for non-primary index add info for back-index
	if !fdb.isPrimary {
		t0 := time.Now()
		backDbInfo, err := fdb.back[0].Info()
		if err != nil {
			return nil, err
		}
		fdb.idxStats.Timings.stKVInfo.Put(time.Now().Sub(t0))
		newSnapshotInfo.BackSeq = backDbInfo.LastSeqNum()
	}

	if commit {
		t0 := time.Now()
		metaDbInfo, err := fdb.meta.Info()
		if err != nil {
			return nil, err
		}
		fdb.idxStats.Timings.stKVInfo.Put(time.Now().Sub(t0))

		//the next meta seqno after this update
		newSnapshotInfo.MetaSeq = metaDbInfo.LastSeqNum() + 1
		infos, err := fdb.getSnapshotsMeta()
		if err != nil {
			return nil, err
		}
		sic := NewSnapshotInfoContainer(infos)
		sic.Add(newSnapshotInfo)

		fdb.confLock.RLock()
		maxRollbacks := fdb.sysconf["settings.recovery.max_rollbacks"].Int()
		fdb.confLock.RUnlock()

		if sic.Len() > maxRollbacks {
			sic.RemoveOldest()
		}

		// Meta update should be done before commit
		// Otherwise, metadata will not be atomically updated along with disk commit.
		err = fdb.updateSnapshotsMeta(sic.List())
		if err != nil {
			return nil, err
		}

		// Commit database file
		start := time.Now()
		err = fdb.dbfile.Commit(forestdb.COMMIT_MANUAL_WAL_FLUSH)
		elapsed := time.Since(start)
		fdb.idxStats.Timings.stCommit.Put(elapsed)

		fdb.totalCommitTime += elapsed
		logging.Infof("ForestDBSlice::Commit SliceId %v IndexInstId %v FlushTime %v CommitTime %v TotalFlushTime %v "+
			"TotalCommitTime %v", fdb.id, fdb.idxInstId, flushTime, elapsed, fdb.totalFlushTime, fdb.totalCommitTime)

		if err != nil {
			logging.Errorf("ForestDBSlice::Commit \n\tSliceId %v IndexInstId %v Error in "+
				"Index Commit %v", fdb.id, fdb.idxInstId, err)
			return nil, err
		}

		fdb.setCommittedCount()
	}

	return newSnapshotInfo, nil
}

func (fdb *fdbSlice) FlushDone() {
	// no-op
}

// checkAllWorkersDone return true if all workers have
// finished processing
func (fdb *fdbSlice) checkAllWorkersDone() bool {

	//if there are mutations in the cmdCh, workers are
	//not yet done
	qc := atomic.LoadInt64(&fdb.qCount)
	if qc > 0 {
		return false
	}

	return true
}

func (fdb *fdbSlice) Close() {
	fdb.lock.Lock()
	defer fdb.lock.Unlock()

	logging.Infof("ForestDBSlice::Close Closing Slice Id %v, IndexInstId %v, "+
		"IndexDefnId %v", fdb.id, fdb.idxInstId, fdb.idxDefnId)

	//signal shutdown for command handler routines
	for i := 0; i < fdb.numWriters; i++ {
		fdb.stopCh[i] <- true
		<-fdb.stopCh[i]
	}

	if fdb.refCount > 0 {
		fdb.isSoftClosed = true
		if fdb.isCompacting {
			go fdb.cancelCompact()
		}
	} else {
		fdb.isClosed = true
		tryCloseFdbSlice(fdb)
	}
}

// Destroy removes the database file from disk.
// Slice is not recoverable after this.
func (fdb *fdbSlice) Destroy() {
	fdb.lock.Lock()
	defer fdb.lock.Unlock()

	if fdb.refCount > 0 {
		logging.Infof("ForestDBSlice::Destroy Softdeleted Slice Id %v, IndexInstId %v, "+
			"IndexDefnId %v", fdb.id, fdb.idxInstId, fdb.idxDefnId)
		fdb.isSoftDeleted = true
	} else {
		fdb.isDeleted = true
		tryDeleteFdbSlice(fdb)
	}
}

// Id returns the Id for this Slice
func (fdb *fdbSlice) Id() SliceId {
	return fdb.id
}

// FilePath returns the filepath for this Slice
func (fdb *fdbSlice) Path() string {
	return fdb.path
}

// IsCleanupDone if the slice is deleted (i.e. slice is
// closed & destroyed
func (fdb *fdbSlice) IsCleanupDone() bool {
	fdb.lock.Lock()
	defer fdb.lock.Unlock()

	return fdb.isClosed && fdb.isDeleted
}

func (fdb *fdbSlice) IsPersistanceActive() bool {
	return false
}

// IsActive returns if the slice is active
func (fdb *fdbSlice) IsActive() bool {
	return fdb.isActive
}

// SetActive sets the active state of this slice
func (fdb *fdbSlice) SetActive(isActive bool) {
	fdb.isActive = isActive
}

// Status returns the status for this slice
func (fdb *fdbSlice) Status() SliceStatus {
	return fdb.status
}

// SetStatus set new status for this slice
func (fdb *fdbSlice) SetStatus(status SliceStatus) {
	fdb.status = status
}

// IndexInstId returns the Index InstanceId this
// slice is associated with
func (fdb *fdbSlice) IndexInstId() common.IndexInstId {
	return fdb.idxInstId
}

func (fdb *fdbSlice) IndexPartnId() common.PartitionId {
	return 0 // Partition indexes are not supported in FDB
}

// IndexDefnId returns the Index DefnId this slice
// is associated with
func (fdb *fdbSlice) IndexDefnId() common.IndexDefnId {
	return fdb.idxDefnId
}

// Returns snapshot info list
func (fdb *fdbSlice) GetSnapshots() ([]SnapshotInfo, error) {
	infos, err := fdb.getSnapshotsMeta()
	return infos, err
}

// IsDirty returns true if there has been any change in
// in the slice storage after last in-mem/persistent snapshot
//
// flushActive will be true if there are going to be any
// messages in the cmdCh of slice after flush is done.
// It will be cleared during snapshot generation as the
// cmdCh would be empty at the time of snapshot generation
func (fdb *fdbSlice) IsDirty() bool {
	flushActive := atomic.LoadUint32(&fdb.flushActive)
	if flushActive == 0 { // No flush happening
		return false
	}
	// Flush in progress - wait till all commands on cmdCh
	// are processed
	fdb.waitPersist()
	return fdb.isDirty
}

func (fdb *fdbSlice) Compact(abortTime time.Time, minFrag int) error {
	fdb.IncrRef()
	defer fdb.DecrRef()

	fdb.setIsCompacting(true)
	defer fdb.setIsCompacting(false)

	if !fdb.canRunCompaction(abortTime) {
		logging.Infof("ForestDBSlice::Skip Compaction outside of compaction interval."+
			"Slice Id %v, IndexInstId %v, IndexDefnId %v", fdb.id, fdb.idxInstId, fdb.idxDefnId)
		return nil
	}

	//get oldest snapshot upto which compaction can be done
	infos, err := fdb.getSnapshotsMeta()
	if err != nil {
		return err
	}

	sic := NewSnapshotInfoContainer(infos)

	osnap := sic.GetOldest()
	if osnap == nil {
		logging.Infof("ForestDBSlice::Compact No Snapshot Found. Skipped Compaction."+
			"Slice Id %v, IndexInstId %v, IndexDefnId %v", fdb.id, fdb.idxInstId, fdb.idxDefnId)
		return nil
	}

	metaSeq := osnap.(*fdbSnapshotInfo).MetaSeq

	//find the db snapshot lower than oldest snapshot
	snap, err := fdb.compactFd.GetAllSnapMarkers()
	if err != nil {
		return err
	}
	defer snap.FreeSnapMarkers()

	var snapMarker *forestdb.SnapMarker
	var compactSeqNum forestdb.SeqNum
	var leastCmSeq forestdb.SeqNum
snaploop:
	for _, s := range snap.SnapInfoList() {

		cm := s.GetKvsCommitMarkers()
		for _, c := range cm {
			//if seqNum of "default" kvs is less than the oldest snapshot seqnum
			//it is safe to compact upto that snapshot
			if c.GetKvStoreName() == "" && c.GetSeqNum() < metaSeq {
				snapMarker = s.GetSnapMarker()
				compactSeqNum = c.GetSeqNum()
				break snaploop
			}
			if leastCmSeq == 0 || leastCmSeq > c.GetSeqNum() {
				leastCmSeq = c.GetSeqNum()
			}
		}
	}

	if snapMarker == nil {
		logging.Infof("ForestDBSlice::Compact No Valid SnapMarker Found. Skipped Compaction."+
			"Slice Id %v, IndexInstId %v, IndexDefnId %v MetaSeq %v LeastCmSeq %v", fdb.id, fdb.idxInstId, fdb.idxDefnId,
			metaSeq, leastCmSeq)
		return nil
	} else {
		logging.Infof("ForestDBSlice::Compact Compacting upto SeqNum %v. "+
			"Slice Id %v, IndexInstId %v, IndexDefnId %v", compactSeqNum, fdb.id,
			fdb.idxInstId, fdb.idxDefnId)
	}

	donech := make(chan bool)
	defer close(donech)
	go fdb.cancelCompactionIfExpire(abortTime, donech)

	newpath := newFdbFile(fdb.path, true)
	// Remove any existing files leftover due to a crash during last compaction attempt
	iowrap.Os_Remove(newpath)
	err = fdb.compactFd.CompactUpto(newpath, snapMarker)
	if err != nil {
		return err
	}

	fdb.currfile = newpath

	config := forestdb.DefaultConfig()
	config.SetOpenFlags(forestdb.OPEN_FLAG_RDONLY)

	fdb.statFdLock.Lock()
	fdb.statFd.Close()
	if fdb.statFd, err = forestdb.Open(fdb.currfile, config); err != nil {
		return err
	}
	fdb.fileVersion = fdb.statFd.GetFileVersion()
	logging.Infof("ForestDBSlice::Compact(): after compaction, file version %v", forestdb.FdbFileVersionToString(fdb.fileVersion))
	fdb.statFdLock.Unlock()

	/*
		FIXME: Use correct accounting of extra snapshots size
			diskSz, err := common.FileSize(fdb.currfile)
			dataSz := int64(fdb.statFd.EstimateSpaceUsed())
			var extraSnapDataSize int64
			if diskSz > dataSz {
				extraSnapDataSize = diskSz - dataSz
			}

			atomic.StoreInt64(&fdb.extraSnapDataSize, extraSnapDataSize)
	*/

	return err
}

func (fdb *fdbSlice) PrepareStats() {
}

func (fdb *fdbSlice) Statistics(consumerFilter uint64) (StorageStatistics, error) {
	var sts StorageStatistics

	sz, err := common.FileSize(fdb.currfile)
	if err != nil {
		return sts, err
	}

	// Compute approximate fragmentation percentage
	// Since we keep multiple index snapshots after compaction, it is not
	// trivial to compute fragmentation as ratio of data size to disk size.
	// Hence we compute approximate fragmentation by adding overhead data size
	// caused by extra snapshots.
	extraSnapDataSize := atomic.LoadInt64(&fdb.extraSnapDataSize)

	fdb.statFdLock.Lock()
	sts.DataSize = int64(fdb.statFd.EstimateSpaceUsed()) + extraSnapDataSize
	sts.DataSizeOnDisk = sts.DataSize
	sts.MemUsed = 0 // as of now this stat is unsupported for FDB, until we figure out a way to calculate it accurately
	sts.NeedUpgrade = fdb.fileVersion < forestdb.FdbV2FileVersion
	fdb.statFdLock.Unlock()

	sts.DiskSize = sz
	sts.LogSpace = sts.DiskSize
	sts.ExtraSnapDataSize = extraSnapDataSize

	sts.GetBytes = atomic.LoadInt64(&fdb.get_bytes)
	sts.InsertBytes = atomic.LoadInt64(&fdb.insert_bytes)
	sts.DeleteBytes = atomic.LoadInt64(&fdb.delete_bytes)

	if logging.IsEnabled(logging.Timing) {
		fdb.statFdLock.Lock()
		latencystats, err := fdb.statFd.GetLatencyStats()
		fdb.statFdLock.Unlock()
		if err != nil {
			return sts, err
		}
		var internalData []string
		internalData = append(internalData, latencystats)
		sts.InternalData = internalData
	}

	// Incase of fdb, set rawDataSize to DataSize
	fdb.idxStats.rawDataSize.Set(sts.DataSize)
	return sts, nil
}

func (fdb *fdbSlice) ShardStatistics(partnId common.PartitionId) *common.ShardStats {
	return nil
}

func (fdb *fdbSlice) GetAlternateShardId(partnId common.PartitionId) string {
	return ""
}

func (fdb *fdbSlice) SetNlist(nlist int) {
	// no-op
}

func (fdb *fdbSlice) InitCodebook() error {
	return nil
}

func (fdb *fdbSlice) ResetCodebook() error {
	return nil
}

func (fdb *fdbSlice) Train(vecs []float32) error {
	return nil
}

func (fdb *fdbSlice) UpdateConfig(cfg common.Config) {
	fdb.confLock.Lock()
	defer fdb.confLock.Unlock()

	oldCfg := fdb.sysconf
	fdb.sysconf = cfg

	// update circular compaction setting in fdb
	nmode := strings.ToLower(cfg["settings.compaction.compaction_mode"].String())
	kept_headers := uint8(cfg["settings.recovery.max_rollbacks"].Int() + 1) //MB-20753
	fconfig := forestdb.DefaultConfig()
	reuse_threshold := uint8(fconfig.BlockReuseThreshold())

	if nmode == "full" {
		reuse_threshold = uint8(0)
	}

	if err := fdb.dbfile.SetBlockReuseParams(reuse_threshold, kept_headers); err != nil {
		logging.Errorf("ForestDBSlice::UpdateConfig Error Changing Circular Compaction Params. Slice Id %v, "+
			"IndexInstId %v, IndexDefnId %v ReuseThreshold %v KeepHeaders %v. Error %v",
			fdb.id, fdb.idxInstId, fdb.idxDefnId, reuse_threshold, kept_headers, err)
	}

	bufResizeNeeded := false
	if cfg["settings.max_array_seckey_size"].Int() !=
		oldCfg["settings.max_array_seckey_size"].Int() {
		bufResizeNeeded = true
	}
	if cfg["settings.max_seckey_size"].Int() !=
		oldCfg["settings.max_seckey_size"].Int() {
		bufResizeNeeded = true
	}
	if bufResizeNeeded {
		keyCfg := getKeySizeConfig(cfg)
		if keyCfg.allowLargeKeys == false {
			atomic.AddInt32(&fdb.keySzConfChanged, 1)
		}
	}
}

func (fdb *fdbSlice) String() string {

	str := fmt.Sprintf("SliceId: %v ", fdb.id)
	str += fmt.Sprintf("File: %v ", fdb.path)
	str += fmt.Sprintf("Index: %v ", fdb.idxInstId)

	return str

}

func (fdb *fdbSlice) updateSnapshotsMeta(infos []SnapshotInfo) error {
	fdb.metaLock.Lock()
	defer fdb.metaLock.Unlock()

	var t0 time.Time
	val, err := json.Marshal(infos)
	if err != nil {
		goto handle_err
	}

	t0 = time.Now()
	err = fdb.meta.SetKV(snapshotMetaListKey, val)
	if err != nil {
		goto handle_err
	}
	fdb.idxStats.Timings.stKVMetaSet.Put(time.Now().Sub(t0))

	return nil

handle_err:
	return errors.New("Failed to update snapshots list -" + err.Error())
}

func (fdb *fdbSlice) getSnapshotsMeta() ([]SnapshotInfo, error) {
	var tmp []*fdbSnapshotInfo
	var snapList []SnapshotInfo

	fdb.metaLock.Lock()
	defer fdb.metaLock.Unlock()

	t0 := time.Now()
	data, err := fdb.meta.GetKV(snapshotMetaListKey)
	if err != nil {
		if err == forestdb.FDB_RESULT_KEY_NOT_FOUND {
			return []SnapshotInfo(nil), nil
		}
		return nil, err
	}
	fdb.idxStats.Timings.stKVMetaGet.Put(time.Now().Sub(t0))

	err = json.Unmarshal(data, &tmp)
	if err != nil {
		goto handle_err
	}

	for i := range tmp {
		snapList = append(snapList, tmp[i])
	}

	return snapList, nil

handle_err:
	return snapList, errors.New("Failed to retrieve snapshots list -" + err.Error())
}

func tryDeleteFdbSlice(fdb *fdbSlice) {
	logging.Infof("ForestDBSlice::Destroy Destroying Slice Id %v, IndexInstId %v, "+
		"IndexDefnId %v", fdb.id, fdb.idxInstId, fdb.idxDefnId)

	if err := forestdb.Destroy(fdb.currfile, fdb.config); err != nil {
		logging.Errorf("ForestDBSlice::Destroy Error Destroying  Slice Id %v, "+
			"IndexInstId %v, IndexDefnId %v. Error %v", fdb.id, fdb.idxInstId, fdb.idxDefnId, err)
	}

	//cleanup the disk directory
	if err := iowrap.Os_RemoveAll(fdb.path); err != nil {
		logging.Errorf("ForestDBSlice::Destroy Error Cleaning Up Slice Id %v, "+
			"IndexInstId %v, IndexDefnId %v. Error %v", fdb.id, fdb.idxInstId, fdb.idxDefnId, err)
	}
}

func tryCloseFdbSlice(fdb *fdbSlice) {
	//close the main index
	if len(fdb.main) == 1 {
		if fdb.main[0] != nil {
			fdb.main[0].Close()
		}
	}

	if !fdb.isPrimary {
		//close the back index
		if len(fdb.back) == 1 {
			if fdb.back[0] != nil {
				fdb.back[0].Close()
			}
		}
	}

	if fdb.meta != nil {
		fdb.meta.Close()
	}

	if fdb.statFd != nil {
		fdb.statFd.Close()
	}

	if fdb.compactFd != nil {
		fdb.compactFd.Close()
	}

	if fdb.dbfile != nil {
		fdb.dbfile.Close()
	}
}

func newFdbFile(dirpath string, newVersion bool) string {
	var version int = 0

	pattern := fmt.Sprintf("data.fdb.*")
	files, _ := filepath.Glob(filepath.Join(dirpath, pattern))
	natsort.Strings(files)
	// Pick the first file with least version
	if len(files) > 0 {
		filename := filepath.Base(files[0])
		_, err := fmt.Sscanf(filename, "data.fdb.%d", &version)
		if err != nil {
			panic(fmt.Sprintf("Invalid data file %s (%v)", files[0], err))
		}
	}

	if newVersion {
		version++
	}

	newFilename := fmt.Sprintf("data.fdb.%d", version)
	return filepath.Join(dirpath, newFilename)
}

func (fdb *fdbSlice) logWriterStat() {
	count := atomic.AddUint64(&fdb.flushedCount, 1)
	if (count%10000 == 0) || count == 1 {
		logging.Debugf("logWriterStat:: %v "+
			"FlushedCount %v QueuedCount %v", fdb.idxInstId,
			count, len(fdb.cmdCh))
	}

}

func (fdb *fdbSlice) setIsCompacting(isCompacting bool) {

	fdb.lock.Lock()
	defer fdb.lock.Unlock()

	fdb.isCompacting = isCompacting
}

func (fdb *fdbSlice) cancelCompactionIfExpire(abortTime time.Time, donech chan bool) {

	ticker := time.NewTicker(time.Minute * time.Duration(5))
	defer ticker.Stop()

	for {
		select {
		case <-donech:
			return
		case <-ticker.C:
			if !fdb.canRunCompaction(abortTime) {
				fdb.lock.Lock()
				defer fdb.lock.Unlock()

				if fdb.isCompacting {
					fdb.cancelCompact()
				}

				return
			}
		}
	}
}

func (fdb *fdbSlice) canRunCompaction(abortTime time.Time) bool {

	// Once compaction starts, only need to find out if it past the end date.
	fdb.confLock.RLock()
	mode := strings.ToLower(fdb.sysconf["settings.compaction.compaction_mode"].String())
	abort := fdb.sysconf["settings.compaction.abort_exceed_interval"].Bool()
	interval := fdb.sysconf["settings.compaction.interval"].String()
	fdb.confLock.RUnlock()

	// No need to stop running compaction if in full compaction mode
	if mode == "full" {
		return true
	}

	// No need to stop compaction if it is ok to exceed time interval
	if !abort {
		return true
	}

	if abort && time.Now().After(abortTime) {
		return false
	}

	var start_hr, start_min, end_hr, end_min int
	n, err := fmt.Sscanf(interval, "%d:%d,%d:%d", &start_hr, &start_min, &end_hr, &end_min)
	start_min += start_hr * 60
	end_min += end_hr * 60

	if n == 4 && err == nil {

		if end_min != 0 {
			hr, min, _ := time.Now().Clock()
			min += hr * 60

			// At this point, we know we have past start time.
			// If end time is next day from current time, add minutes.
			// To know if end time is next day from current time,
			// current time is larger than start time.
			if start_min > end_min && min > start_min {
				end_min += 24 * 60
			}

			if min > end_min {
				return false
			}

		} else {
			// if there is no end time, then allow compaction to continue.
			logging.Errorf("ForestDBSlice::canRunCompaction.  Compaction setting misconfigured.  Allowing compaction to continue without abort.")
		}
	}

	return true
}

func (fdb *fdbSlice) GetReaderContext(user string, skipReadMetering bool) IndexReaderContext {
	return &cursorCtx{}
}

func (fdb *fdbSlice) cancelCompact() {

	logging.Infof("ForestDBSlice::cancelCompact Cancel Compaction Slice Id %v, "+
		"IndexInstId %v", fdb.id, fdb.idxInstId)

	var tempFd *forestdb.File
	var err error

	//open a separate file handle for cancel compaction
	config := forestdb.DefaultConfig()
	if tempFd, err = forestdb.Open(fdb.currfile, config); err != nil {
		logging.Errorf("ForestDBSlice::cancelCompact Error Opening DB %v %v", err,
			fdb.idxInstId)
		return
	}
	err = tempFd.CancelCompact()

	logging.Infof("ForestDBSlice::Cancel Compaction Returns err %v "+
		"Slice Id %v, IndexInstId %v ", err, fdb.id, fdb.idxInstId)
}

func (fdb *fdbSlice) RecoveryDone() {
	// done nothing
}

func (fdb *fdbSlice) BuildDone() {
	// done nothing
}

func (fdb *fdbSlice) GetTenantDiskSize() (int64, error) {
	return int64(0), nil
}

func (fdb *fdbSlice) GetShardIds() []common.ShardId {
	return nil // nothing to do
}

func (fdb *fdbSlice) ClearRebalRunning() {
	// nothing to do
}

func (fdb *fdbSlice) SetRebalRunning() {
	// nothing to do
}

func (fdb *fdbSlice) GetWriteUnits() uint64 {
	return 0
}

func (fdb *fdbSlice) SetStopWriteUnitBilling(isRebalance bool) {
}
