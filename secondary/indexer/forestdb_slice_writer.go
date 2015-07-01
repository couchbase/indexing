// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package indexer

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/fdb"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/platform"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

var (
	snapshotMetaListKey = []byte("snapshots-list")
)

//NewForestDBSlice initiailizes a new slice with forestdb backend.
//Both main and back index gets initialized with default config.
//Slice methods are not thread-safe and application needs to
//handle the synchronization. The only exception being Insert and
//Delete can be called concurrently.
//Returns error in case slice cannot be initialized.
func NewForestDBSlice(path string, sliceId SliceId, idxDefnId common.IndexDefnId,
	idxInstId common.IndexInstId, isPrimary bool,
	sysconf common.Config, idxStats *IndexStats) (*fdbSlice, error) {

	info, err := os.Stat(path)
	if err != nil || err == nil && info.IsDir() {
		os.Mkdir(path, 0777)
	}

	filepath := newFdbFile(path, false)
	slice := &fdbSlice{}
	slice.idxStats = idxStats

	config := forestdb.DefaultConfig()
	config.SetDurabilityOpt(forestdb.DRB_ASYNC)

	memQuota := sysconf["settings.memory_quota"].Uint64()
	logging.Debugf("NewForestDBSlice(): buffer cache size %d", memQuota)
	config.SetBufferCacheSize(memQuota)
	logging.Debugf("NewForestDBSlice(): buffer cache size %d", memQuota)

	prob := sysconf["settings.max_writer_lock_prob"].Int()
	config.SetMaxWriterLockProb(uint8(prob))
	logging.Debugf("NewForestDBSlice(): max writer lock prob %d", prob)

	kvconfig := forestdb.DefaultKVStoreConfig()

retry:
	if slice.dbfile, err = forestdb.Open(filepath, config); err != nil {
		if err == forestdb.RESULT_NO_DB_HEADERS {
			logging.Warnf("NewForestDBSlice(): Open failed with no_db_header error...Resetting the forestdb file")
			os.Remove(filepath)
			goto retry
		}
		return nil, err
	}

	slice.config = config
	slice.sysconf = sysconf

	//open a separate file handle for compaction
	if slice.compactFd, err = forestdb.Open(filepath, config); err != nil {
		return nil, err
	}

	config.SetOpenFlags(forestdb.OPEN_FLAG_RDONLY)
	if slice.statFd, err = forestdb.Open(filepath, config); err != nil {
		return nil, err
	}

	slice.numWriters = sysconf["numSliceWriters"].Int()
	slice.main = make([]*forestdb.KVStore, slice.numWriters)
	for i := 0; i < slice.numWriters; i++ {
		if slice.main[i], err = slice.dbfile.OpenKVStore("main", kvconfig); err != nil {
			return nil, err
		}
	}

	//create a separate back-index for non-primary indexes
	if !isPrimary {
		slice.back = make([]*forestdb.KVStore, slice.numWriters)
		for i := 0; i < slice.numWriters; i++ {
			if slice.back[i], err = slice.dbfile.OpenKVStore("back", kvconfig); err != nil {
				return nil, err
			}
		}
	}

	// Make use of default kvstore provided by forestdb
	if slice.meta, err = slice.dbfile.OpenKVStore("default", kvconfig); err != nil {
		return nil, err
	}

	slice.path = path
	slice.currfile = filepath
	slice.idxInstId = idxInstId
	slice.idxDefnId = idxDefnId
	slice.id = sliceId

	sliceBufSize := sysconf["settings.sliceBufSize"].Uint64()
	slice.cmdCh = make(chan interface{}, sliceBufSize)
	slice.workerDone = make([]chan bool, slice.numWriters)
	slice.stopCh = make([]DoneChannel, slice.numWriters)

	slice.isPrimary = isPrimary

	for i := 0; i < slice.numWriters; i++ {
		slice.stopCh[i] = make(DoneChannel)
		slice.workerDone[i] = make(chan bool)
		go slice.handleCommandsWorker(i)
	}

	logging.Debugf("ForestDBSlice:NewForestDBSlice \n\t Created New Slice Id %v IndexInstId %v "+
		"WriterThreads %v", sliceId, idxInstId, slice.numWriters)

	slice.setCommittedCount()

	return slice, nil
}

//kv represents a key/value pair in storage format
type indexItem struct {
	key   []byte
	docid []byte
}

//fdbSlice represents a forestdb slice
type fdbSlice struct {
	get_bytes, insert_bytes, delete_bytes platform.AlignedInt64
	//flushed count
	flushedCount platform.AlignedUint64
	// persisted items count
	committedCount platform.AlignedUint64

	// Fragmentation percent computed after last compaction
	fragAfterCompaction platform.AlignedInt64

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

	metaLock sync.Mutex
	meta     *forestdb.KVStore   // handle for index meta
	main     []*forestdb.KVStore // handle for forward index
	back     []*forestdb.KVStore // handle for reverse index

	config *forestdb.Config

	idxDefnId common.IndexDefnId
	idxInstId common.IndexInstId

	status        SliceStatus
	isActive      bool
	isPrimary     bool
	isSoftDeleted bool
	isSoftClosed  bool

	cmdCh  chan interface{} //internal channel to buffer commands
	stopCh []DoneChannel    //internal channel to signal shutdown

	workerDone []chan bool //worker status check channel

	fatalDbErr error //store any fatal DB error

	numWriters int //number of writer threads

	//TODO: Remove this once these stats are
	//captured by the stats library
	totalFlushTime  time.Duration
	totalCommitTime time.Duration

	idxStats *IndexStats
	sysconf  common.Config
	confLock sync.Mutex
}

func (fdb *fdbSlice) IncrRef() {
	fdb.lock.Lock()
	defer fdb.lock.Unlock()

	fdb.refCount++
}

func (fdb *fdbSlice) DecrRef() {
	fdb.lock.Lock()
	defer fdb.lock.Unlock()

	fdb.refCount--
	if fdb.refCount == 0 {
		if fdb.isSoftClosed {
			tryCloseFdbSlice(fdb)
		}
		if fdb.isSoftDeleted {
			tryDeleteFdbSlice(fdb)
		}
	}
}

//Insert will insert the given key/value pair from slice.
//Internally the request is buffered and executed async.
//If forestdb has encountered any fatal error condition,
//it will be returned as error.
func (fdb *fdbSlice) Insert(key []byte, docid []byte) error {
	fdb.idxStats.flushQueueSize.Add(1)
	fdb.idxStats.numFlushQueued.Add(1)
	fdb.cmdCh <- &indexItem{key: key, docid: docid}
	return fdb.fatalDbErr
}

//Delete will delete the given document from slice.
//Internally the request is buffered and executed async.
//If forestdb has encountered any fatal error condition,
//it will be returned as error.
func (fdb *fdbSlice) Delete(docid []byte) error {
	fdb.idxStats.flushQueueSize.Add(1)
	fdb.idxStats.numFlushQueued.Add(1)
	fdb.cmdCh <- docid
	return fdb.fatalDbErr
}

//handleCommands keep listening to any buffered
//write requests for the slice and processes
//those. This will shut itself down internal
//shutdown channel is closed.
func (fdb *fdbSlice) handleCommandsWorker(workerId int) {

	var start time.Time
	var elapsed time.Duration
	var c interface{}
	var icmd *indexItem
	var dcmd []byte

loop:
	for {
		select {
		case c = <-fdb.cmdCh:
			switch c.(type) {

			case *indexItem:
				icmd = c.(*indexItem)
				start = time.Now()
				fdb.insert((*icmd).key, (*icmd).docid, workerId)
				elapsed = time.Since(start)
				fdb.totalFlushTime += elapsed

			case []byte:
				dcmd = c.([]byte)
				start = time.Now()
				fdb.delete(dcmd, workerId)
				elapsed = time.Since(start)
				fdb.totalFlushTime += elapsed

			default:
				logging.Errorf("ForestDBSlice::handleCommandsWorker \n\tSliceId %v IndexInstId %v Received "+
					"Unknown Command %v", fdb.id, fdb.idxInstId, c)
			}

			fdb.idxStats.flushQueueSize.Add(-1)

		case <-fdb.stopCh[workerId]:
			fdb.stopCh[workerId] <- true
			break loop

			//worker gets a status check message on this channel, it responds
			//when its not processing any mutation
		case <-fdb.workerDone[workerId]:
			fdb.workerDone[workerId] <- true

		}
	}

}

//insert does the actual insert in forestdb
func (fdb *fdbSlice) insert(key []byte, docid []byte, workerId int) {

	if fdb.isPrimary {
		fdb.insertPrimaryIndex(key, docid, workerId)
	} else {
		fdb.insertSecIndex(key, docid, workerId)
	}

	fdb.logWriterStat()
}

func (fdb *fdbSlice) insertPrimaryIndex(key []byte, docid []byte, workerId int) {
	var err error

	logging.Tracef("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Set Key - %s", fdb.id, fdb.idxInstId, docid)

	//check if the docid exists in the main index
	if _, err = fdb.main[workerId].GetKV(key); err == nil {
		//skip
		logging.Tracef("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Key %v Already Exists. "+
			"Primary Index Update Skipped.", fdb.id, fdb.idxInstId, string(docid))
	} else if err != nil && err != forestdb.RESULT_KEY_NOT_FOUND {
		fdb.checkFatalDbError(err)
		logging.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error locating "+
			"mainindex entry %v", fdb.id, fdb.idxInstId, err)
	} else if err == forestdb.RESULT_KEY_NOT_FOUND {
		//set in main index
		if err = fdb.main[workerId].SetKV(key, nil); err != nil {
			fdb.checkFatalDbError(err)
			logging.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error in Main Index Set. "+
				"Skipped Key %s. Error %v", fdb.id, fdb.idxInstId, string(docid), err)
		}
		platform.AddInt64(&fdb.insert_bytes, int64(len(key)))
	}
}

func (fdb *fdbSlice) insertSecIndex(key []byte, docid []byte, workerId int) {
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
				"Doc Id %v. Key %v. Skipped.", fdb.id, fdb.idxInstId, string(docid), key)
			return
		}

		//there is already an entry in main index for this docid
		//delete from main index
		if err = fdb.main[workerId].DeleteKV(oldkey); err != nil {
			fdb.checkFatalDbError(err)
			logging.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error deleting "+
				"entry from main index %v", fdb.id, fdb.idxInstId, err)
			return
		}
		platform.AddInt64(&fdb.delete_bytes, int64(len(oldkey)))

		//delete from back index
		if err = fdb.back[workerId].DeleteKV(docid); err != nil {
			fdb.checkFatalDbError(err)
			logging.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error deleting "+
				"entry from back index %v", fdb.id, fdb.idxInstId, err)
			return
		}
		platform.AddInt64(&fdb.delete_bytes, int64(len(docid)))
	}

	if key == nil {
		logging.Tracef("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Received NIL Key for "+
			"Doc Id %s. Skipped.", fdb.id, fdb.idxInstId, docid)
		return
	}

	//set the back index entry <docid, encodedkey>
	if err = fdb.back[workerId].SetKV(docid, key); err != nil {
		fdb.checkFatalDbError(err)
		logging.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error in Back Index Set. "+
			"Skipped Key %s. Value %v. Error %v", fdb.id, fdb.idxInstId, string(docid), key, err)
		return
	}
	platform.AddInt64(&fdb.insert_bytes, int64(len(docid)+len(key)))

	//set in main index
	if err = fdb.main[workerId].SetKV(key, nil); err != nil {
		fdb.checkFatalDbError(err)
		logging.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error in Main Index Set. "+
			"Skipped Key %v. Error %v", fdb.id, fdb.idxInstId, key, err)
		return
	}
	platform.AddInt64(&fdb.insert_bytes, int64(len(key)))
}

//delete does the actual delete in forestdb
func (fdb *fdbSlice) delete(docid []byte, workerId int) {

	if fdb.isPrimary {
		fdb.deletePrimaryIndex(docid, workerId)
	} else {
		fdb.deleteSecIndex(docid, workerId)
	}

	fdb.logWriterStat()

}

func (fdb *fdbSlice) deletePrimaryIndex(docid []byte, workerId int) {

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
	if err := fdb.main[workerId].DeleteKV(entry.Bytes()); err != nil {
		fdb.checkFatalDbError(err)
		logging.Errorf("ForestDBSlice::delete \n\tSliceId %v IndexInstId %v. Error deleting "+
			"entry from main index for Doc %s. Error %v", fdb.id, fdb.idxInstId,
			docid, err)
		return
	}
	platform.AddInt64(&fdb.delete_bytes, int64(len(entry.Bytes())))

}

func (fdb *fdbSlice) deleteSecIndex(docid []byte, workerId int) {

	//logging.Tracef("ForestDBSlice::delete \n\tSliceId %v IndexInstId %v. Delete Key - %s",
	//	fdb.id, fdb.idxInstId, docid)

	var olditm []byte
	var err error

	if olditm, err = fdb.getBackIndexEntry(docid, workerId); err != nil {
		fdb.checkFatalDbError(err)
		logging.Errorf("ForestDBSlice::delete \n\tSliceId %v IndexInstId %v. Error locating "+
			"backindex entry for Doc %s. Error %v", fdb.id, fdb.idxInstId, docid, err)
		return
	}

	//if the oldkey is nil, nothing needs to be done. This is the case of deletes
	//which happened before index was created.
	if olditm == nil {
		logging.Tracef("ForestDBSlice::delete \n\tSliceId %v IndexInstId %v Received NIL Key for "+
			"Doc Id %v. Skipped.", fdb.id, fdb.idxInstId, docid)
		return
	}

	//delete from main index
	if err = fdb.main[workerId].DeleteKV(olditm); err != nil {
		fdb.checkFatalDbError(err)
		logging.Errorf("ForestDBSlice::delete \n\tSliceId %v IndexInstId %v. Error deleting "+
			"entry from main index for Doc %s. Key %v. Error %v", fdb.id, fdb.idxInstId,
			docid, olditm, err)
		return
	}
	platform.AddInt64(&fdb.delete_bytes, int64(len(olditm)))

	//delete from the back index
	if err = fdb.back[workerId].DeleteKV(docid); err != nil {
		fdb.checkFatalDbError(err)
		logging.Errorf("ForestDBSlice::delete \n\tSliceId %v IndexInstId %v. Error deleting "+
			"entry from back index for Doc %s. Error %v", fdb.id, fdb.idxInstId, docid, err)
		return
	}
	platform.AddInt64(&fdb.delete_bytes, int64(len(docid)))

}

//getBackIndexEntry returns an existing back index entry
//given the docid
func (fdb *fdbSlice) getBackIndexEntry(docid []byte, workerId int) ([]byte, error) {

	//	logging.Tracef("ForestDBSlice::getBackIndexEntry \n\tSliceId %v IndexInstId %v Get BackIndex Key - %s",
	//		fdb.id, fdb.idxInstId, docid)

	var kbytes []byte
	var err error

	kbytes, err = fdb.back[workerId].GetKV(docid)
	platform.AddInt64(&fdb.get_bytes, int64(len(kbytes)))

	//forestdb reports get in a non-existent key as an
	//error, skip that
	if err != nil && err != forestdb.RESULT_KEY_NOT_FOUND {
		return nil, err
	}

	return kbytes, nil
}

//checkFatalDbError checks if the error returned from DB
//is fatal and stores it. This error will be returned
//to caller on next DB operation
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
	if fdb.isPrimary {
		s = &fdbSnapshot{slice: fdb,
			idxDefnId:  fdb.idxDefnId,
			idxInstId:  fdb.idxInstId,
			main:       fdb.main[0],
			ts:         snapInfo.Timestamp(),
			mainSeqNum: snapInfo.MainSeq,
			committed:  info.IsCommitted(),
		}
	} else {
		s = &fdbSnapshot{slice: fdb,
			idxDefnId:  fdb.idxDefnId,
			idxInstId:  fdb.idxInstId,
			main:       fdb.main[0],
			back:       fdb.back[0],
			ts:         snapInfo.Timestamp(),
			mainSeqNum: snapInfo.MainSeq,
			backSeqNum: snapInfo.BackSeq,
			committed:  info.IsCommitted(),
		}
	}

	if info.IsCommitted() {
		s.meta = fdb.meta
		s.metaSeqNum = snapInfo.MetaSeq
	}

	logging.Debugf("ForestDBSlice::OpenSnapshot \n\tSliceId %v IndexInstId %v Creating New "+
		"Snapshot %v committed:%v", fdb.id, fdb.idxInstId, snapInfo, s.committed)
	err := s.Create()

	return s, err
}

func (fdb *fdbSlice) setCommittedCount() {
	mainDbInfo, err := fdb.main[0].Info()
	if err == nil {
		platform.StoreUint64(&fdb.committedCount, mainDbInfo.DocCount())
	} else {
		logging.Errorf("ForestDB setCommittedCount failed %v", err)
	}
}

func (fdb *fdbSlice) GetCommittedCount() uint64 {
	return platform.LoadUint64(&fdb.committedCount)
}

//Rollback slice to given snapshot. Return error if
//not possible
func (fdb *fdbSlice) Rollback(info SnapshotInfo) error {

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

	return fdb.dbfile.Commit(forestdb.COMMIT_MANUAL_WAL_FLUSH)
}

//RollbackToZero rollbacks the slice to initial state. Return error if
//not possible
func (fdb *fdbSlice) RollbackToZero() error {

	zeroSeqNum := forestdb.SeqNum(0)
	var err error

	//rollback meta-store first, if main/back index rollback fails, recovery
	//will pick up the rolled-back meta information.
	err = fdb.meta.Rollback(zeroSeqNum)
	if err != nil {
		logging.Errorf("ForestDBSlice::Rollback \n\tSliceId %v IndexInstId %v. Error Rollback "+
			"Meta Index to Zero. Error %v", fdb.id, fdb.idxInstId, err)
		return err
	}

	//call forestdb to rollback
	err = fdb.main[0].Rollback(zeroSeqNum)
	if err != nil {
		logging.Errorf("ForestDBSlice::Rollback \n\tSliceId %v IndexInstId %v. Error Rollback "+
			"Main Index to Zero. Error %v", fdb.id, fdb.idxInstId, err)
		return err
	}

	fdb.setCommittedCount()

	//rollback back-index only for non-primary indexes
	if !fdb.isPrimary {
		err = fdb.back[0].Rollback(zeroSeqNum)
		if err != nil {
			logging.Errorf("ForestDBSlice::Rollback \n\tSliceId %v IndexInstId %v. Error Rollback "+
				"Back Index to Zero. Error %v", fdb.id, fdb.idxInstId, err)
			return err
		}
	}

	return nil
}

//slice insert/delete methods are async. There
//can be outstanding mutations in internal queue to flush even
//after insert/delete have return success to caller.
//This method provides a mechanism to wait till internal
//queue is empty.
func (fdb *fdbSlice) waitPersist() {

	//every SLICE_COMMIT_POLL_INTERVAL milliseconds,
	//check for outstanding mutations. If there are
	//none, proceed with the commit.
	ticker := time.NewTicker(time.Millisecond * SLICE_COMMIT_POLL_INTERVAL)
	for _ = range ticker.C {
		if fdb.checkAllWorkersDone() {
			break
		}
	}

}

//Commit persists the outstanding writes in underlying
//forestdb database. If Commit returns error, slice
//should be rolled back to previous snapshot.
func (fdb *fdbSlice) NewSnapshot(ts *common.TsVbuuid, commit bool) (SnapshotInfo, error) {

	fdb.waitPersist()

	mainDbInfo, err := fdb.main[0].Info()
	if err != nil {
		return nil, err
	}

	newSnapshotInfo := &fdbSnapshotInfo{
		Ts:        ts,
		MainSeq:   mainDbInfo.LastSeqNum(),
		Committed: commit,
	}

	//for non-primary index add info for back-index
	if !fdb.isPrimary {
		backDbInfo, err := fdb.back[0].Info()
		if err != nil {
			return nil, err
		}
		newSnapshotInfo.BackSeq = backDbInfo.LastSeqNum()
	}

	if commit {
		metaDbInfo, err := fdb.meta.Info()
		if err != nil {
			return nil, err
		}
		//the next meta seqno after this update
		newSnapshotInfo.MetaSeq = metaDbInfo.LastSeqNum() + 1
		infos, err := fdb.getSnapshotsMeta()
		if err != nil {
			return nil, err
		}
		sic := NewSnapshotInfoContainer(infos)
		sic.Add(newSnapshotInfo)

		fdb.confLock.Lock()
		maxRollbacks := fdb.sysconf["settings.recovery.max_rollbacks"].Int()
		fdb.confLock.Unlock()

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

		fdb.totalCommitTime += elapsed
		logging.Debugf("ForestDBSlice::Commit \n\tSliceId %v IndexInstId %v TotalFlushTime %v "+
			"TotalCommitTime %v", fdb.id, fdb.idxInstId, fdb.totalFlushTime, fdb.totalCommitTime)

		if err != nil {
			logging.Errorf("ForestDBSlice::Commit \n\tSliceId %v IndexInstId %v Error in "+
				"Index Commit %v", fdb.id, fdb.idxInstId, err)
			return nil, err
		}

		fdb.setCommittedCount()
	}

	return newSnapshotInfo, nil
}

//checkAllWorkersDone return true if all workers have
//finished processing
func (fdb *fdbSlice) checkAllWorkersDone() bool {

	//if there are mutations in the cmdCh, workers are
	//not yet done
	if len(fdb.cmdCh) > 0 {
		return false
	}

	//worker queue is empty, make sure both workers are done
	//processing the last mutation
	for i := 0; i < fdb.numWriters; i++ {
		fdb.workerDone[i] <- true
		<-fdb.workerDone[i]
	}
	return true
}

func (fdb *fdbSlice) Close() {
	fdb.lock.Lock()
	defer fdb.lock.Unlock()

	logging.Infof("ForestDBSlice::Close \n\tClosing Slice Id %v, IndexInstId %v, "+
		"IndexDefnId %v", fdb.idxInstId, fdb.idxDefnId, fdb.id)

	//signal shutdown for command handler routines
	for i := 0; i < fdb.numWriters; i++ {
		fdb.stopCh[i] <- true
		<-fdb.stopCh[i]
	}

	if fdb.refCount > 0 {
		fdb.isSoftClosed = true
	} else {
		tryCloseFdbSlice(fdb)
	}
}

//Destroy removes the database file from disk.
//Slice is not recoverable after this.
func (fdb *fdbSlice) Destroy() {
	fdb.lock.Lock()
	defer fdb.lock.Unlock()

	if fdb.refCount > 0 {
		logging.Infof("ForestDBSlice::Destroy \n\tSoftdeleted Slice Id %v, IndexInstId %v, "+
			"IndexDefnId %v", fdb.id, fdb.idxInstId, fdb.idxDefnId)
		fdb.isSoftDeleted = true
	} else {
		tryDeleteFdbSlice(fdb)
	}
}

//Id returns the Id for this Slice
func (fdb *fdbSlice) Id() SliceId {
	return fdb.id
}

// FilePath returns the filepath for this Slice
func (fdb *fdbSlice) Path() string {
	return fdb.path
}

//IsActive returns if the slice is active
func (fdb *fdbSlice) IsActive() bool {
	return fdb.isActive
}

//SetActive sets the active state of this slice
func (fdb *fdbSlice) SetActive(isActive bool) {
	fdb.isActive = isActive
}

//Status returns the status for this slice
func (fdb *fdbSlice) Status() SliceStatus {
	return fdb.status
}

//SetStatus set new status for this slice
func (fdb *fdbSlice) SetStatus(status SliceStatus) {
	fdb.status = status
}

//IndexInstId returns the Index InstanceId this
//slice is associated with
func (fdb *fdbSlice) IndexInstId() common.IndexInstId {
	return fdb.idxInstId
}

//IndexDefnId returns the Index DefnId this slice
//is associated with
func (fdb *fdbSlice) IndexDefnId() common.IndexDefnId {
	return fdb.idxDefnId
}

// Returns snapshot info list
func (fdb *fdbSlice) GetSnapshots() ([]SnapshotInfo, error) {
	infos, err := fdb.getSnapshotsMeta()
	return infos, err
}

func (fdb *fdbSlice) Compact() error {
	fdb.IncrRef()
	defer fdb.DecrRef()

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

	mainSeq := osnap.(*fdbSnapshotInfo).MainSeq

	//find the db snapshot lower than oldest snapshot
	snap, err := fdb.compactFd.GetAllSnapMarkers()
	if err != nil {
		return err
	}
	defer snap.FreeSnapMarkers()

	var snapMarker *forestdb.SnapMarker
	var compactSeqNum forestdb.SeqNum
snaploop:
	for _, s := range snap.SnapInfoList() {

		cm := s.GetKvsCommitMarkers()
		for _, c := range cm {
			//if seqNum of "main" kvs is less than or equal to oldest snapshot seqnum
			//it is safe to compact upto that snapshot
			if c.GetKvStoreName() == "main" && c.GetSeqNum() <= mainSeq {
				snapMarker = s.GetSnapMarker()
				compactSeqNum = c.GetSeqNum()
				break snaploop
			}
		}
	}

	if snapMarker == nil {
		logging.Infof("ForestDBSlice::Compact No Valid SnapMarker Found. Skipped Compaction."+
			"Slice Id %v, IndexInstId %v, IndexDefnId %v", fdb.id, fdb.idxInstId, fdb.idxDefnId)
		return nil
	} else {
		logging.Infof("ForestDBSlice::Compact Compacting upto SeqNum %v. "+
			"Slice Id %v, IndexInstId %v, IndexDefnId %v", compactSeqNum, fdb.id,
			fdb.idxInstId, fdb.idxDefnId)
	}

	newpath := newFdbFile(fdb.path, true)
	err = fdb.compactFd.CompactUpto(newpath, snapMarker)
	if err != nil {
		return err
	}

	if _, e := os.Stat(fdb.currfile); e == nil {
		err = os.Remove(fdb.currfile)
	}

	fdb.currfile = newpath

	diskSz, err := common.FileSize(fdb.currfile)
	config := forestdb.DefaultConfig()
	config.SetOpenFlags(forestdb.OPEN_FLAG_RDONLY)
	fdb.statFd.Close()
	if fdb.statFd, err = forestdb.Open(fdb.currfile, config); err != nil {
		return err
	}
	dataSz := int64(fdb.statFd.EstimateSpaceUsed())
	frag := (diskSz - dataSz) * 100 / diskSz

	platform.StoreInt64(&fdb.fragAfterCompaction, frag)
	return err
}

func (fdb *fdbSlice) Statistics() (StorageStatistics, error) {
	var sts StorageStatistics

	sz, err := common.FileSize(fdb.currfile)
	if err != nil {
		return sts, err
	}

	sts.DataSize = int64(fdb.statFd.EstimateSpaceUsed())
	sts.DiskSize = sz

	// Compute approximate fragmentation percentage
	// Since we keep multiple index snapshots after compaction, it is not
	// trivial to compute fragmentation as ration of data size to disk size.
	// Hence we compute approximate fragmentation by removing fragmentation
	// threshold caused as a result of compaction.
	sts.Fragmentation = 0
	if sts.DataSize > 0 {
		sts.Fragmentation = ((sts.DiskSize - sts.DataSize) * 100) / sts.DiskSize
	}
	compactionFrag := platform.LoadInt64(&fdb.fragAfterCompaction)
	sts.Fragmentation -= compactionFrag

	if sts.Fragmentation < 0 {
		sts.Fragmentation = 0
	}

	sts.GetBytes = platform.LoadInt64(&fdb.get_bytes)
	sts.InsertBytes = platform.LoadInt64(&fdb.insert_bytes)
	sts.DeleteBytes = platform.LoadInt64(&fdb.delete_bytes)

	return sts, nil
}

func (fdb *fdbSlice) UpdateConfig(cfg common.Config) {
	fdb.confLock.Lock()
	defer fdb.confLock.Unlock()

	fdb.sysconf = cfg
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

	val, err := json.Marshal(infos)
	if err != nil {
		goto handle_err
	}
	err = fdb.meta.SetKV(snapshotMetaListKey, val)
	if err != nil {
		goto handle_err
	}
	return nil

handle_err:
	return errors.New("Failed to update snapshots list -" + err.Error())
}

func (fdb *fdbSlice) getSnapshotsMeta() ([]SnapshotInfo, error) {
	var tmp []*fdbSnapshotInfo
	var snapList []SnapshotInfo

	fdb.metaLock.Lock()
	defer fdb.metaLock.Unlock()

	data, err := fdb.meta.GetKV(snapshotMetaListKey)

	if err != nil {
		if err == forestdb.RESULT_KEY_NOT_FOUND {
			return []SnapshotInfo(nil), nil
		}
		return nil, err
	}

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
	logging.Infof("ForestDBSlice::Destroy \n\tDestroying Slice Id %v, IndexInstId %v, "+
		"IndexDefnId %v", fdb.id, fdb.idxInstId, fdb.idxDefnId)

	if err := forestdb.Destroy(fdb.currfile, fdb.config); err != nil {
		logging.Errorf("ForestDBSlice::Destroy \n\t Error Destroying  Slice Id %v, "+
			"IndexInstId %v, IndexDefnId %v. Error %v", fdb.id, fdb.idxInstId, fdb.idxDefnId, err)
	}

	//cleanup the disk directory
	if err := os.RemoveAll(fdb.path); err != nil {
		logging.Errorf("ForestDBSlice::Destroy \n\t Error Cleaning Up Slice Id %v, "+
			"IndexInstId %v, IndexDefnId %v. Error %v", fdb.id, fdb.idxInstId, fdb.idxDefnId, err)
	}
}

func tryCloseFdbSlice(fdb *fdbSlice) {
	//close the main index
	if fdb.main[0] != nil {
		fdb.main[0].Close()
	}

	if !fdb.isPrimary {
		//close the back index
		if fdb.back[0] != nil {
			fdb.back[0].Close()
		}
	}

	if fdb.meta != nil {
		fdb.meta.Close()
	}

	fdb.statFd.Close()
	fdb.compactFd.Close()
	fdb.dbfile.Close()
}

func newFdbFile(dirpath string, newVersion bool) string {
	var version int = 0

	pattern := fmt.Sprintf("data.fdb.*")
	files, _ := filepath.Glob(filepath.Join(dirpath, pattern))
	sort.Strings(files)
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
	count := platform.AddUint64(&fdb.flushedCount, 1)
	if (count%10000 == 0) || count == 1 {
		logging.Infof("logWriterStat:: %v "+
			"FlushedCount %v QueuedCount %v", fdb.idxInstId,
			count, len(fdb.cmdCh))
	}

}
