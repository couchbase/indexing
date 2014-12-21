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
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbaselabs/goforestdb"
	"os"
	"path"
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
	idxInstId common.IndexInstId) (*fdbSlice, error) {

	info, err := os.Stat(path)
	if err != nil || err == nil && info.IsDir() {
		os.Mkdir(path, 0777)
	}

	filepath := newFdbFile(path, false)
	slice := &fdbSlice{}

	config := forestdb.DefaultConfig()
	config.SetDurabilityOpt(forestdb.DRB_ASYNC)
	kvconfig := forestdb.DefaultKVStoreConfig()

	if slice.dbfile, err = forestdb.Open(filepath, config); err != nil {
		return nil, err
	}

	slice.main = make([]*forestdb.KVStore, NUM_WRITER_THREADS_PER_SLICE)
	for i := 0; i < NUM_WRITER_THREADS_PER_SLICE; i++ {
		if slice.main[i], err = slice.dbfile.OpenKVStore("main", kvconfig); err != nil {
			return nil, err
		}
	}

	//create a separate back-index
	slice.back = make([]*forestdb.KVStore, NUM_WRITER_THREADS_PER_SLICE)
	for i := 0; i < NUM_WRITER_THREADS_PER_SLICE; i++ {
		if slice.back[i], err = slice.dbfile.OpenKVStore("back", kvconfig); err != nil {
			return nil, err
		}
	}

	// Make use of default kvstore provided by forestdb
	if slice.meta, err = slice.dbfile.OpenKVStore("default", kvconfig); err != nil {
		return nil, err
	}

	slice.path = path
	slice.config = config
	slice.idxInstId = idxInstId
	slice.idxDefnId = idxDefnId
	slice.id = sliceId

	slice.cmdCh = make(chan interface{}, SLICE_COMMAND_BUFFER_SIZE)
	slice.workerDone = make([]chan bool, NUM_WRITER_THREADS_PER_SLICE)
	slice.stopCh = make([]DoneChannel, NUM_WRITER_THREADS_PER_SLICE)

	for i := 0; i < NUM_WRITER_THREADS_PER_SLICE; i++ {
		slice.stopCh[i] = make(DoneChannel)
		slice.workerDone[i] = make(chan bool)
		go slice.handleCommandsWorker(i)
	}

	common.Debugf("ForestDBSlice:NewForestDBSlice \n\t Created New Slice Id %v IndexInstId %v "+
		"WriterThreads %v", sliceId, idxInstId, NUM_WRITER_THREADS_PER_SLICE)

	return slice, nil
}

//kv represents a key/value pair in storage format
type kv struct {
	k Key
	v Value
}

//fdbSlice represents a forestdb slice
type fdbSlice struct {
	path string
	id   SliceId //slice id

	refCount int
	lock     sync.RWMutex
	dbfile   *forestdb.File
	metaLock sync.Mutex
	meta     *forestdb.KVStore   // handle for index meta
	main     []*forestdb.KVStore // handle for forward index
	back     []*forestdb.KVStore // handle for reverse index

	config *forestdb.Config

	idxDefnId common.IndexDefnId
	idxInstId common.IndexInstId

	status        SliceStatus
	isActive      bool
	isSoftDeleted bool
	isSoftClosed  bool

	cmdCh  chan interface{} //internal channel to buffer commands
	stopCh []DoneChannel    //internal channel to signal shutdown

	workerDone []chan bool //worker status check channel

	fatalDbErr error //store any fatal DB error

	//TODO: Remove this once these stats are
	//captured by the stats library
	totalFlushTime  time.Duration
	totalCommitTime time.Duration
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
func (fdb *fdbSlice) Insert(k Key, v Value) error {

	fdb.cmdCh <- kv{k: k, v: v}
	return fdb.fatalDbErr

}

//Delete will delete the given document from slice.
//Internally the request is buffered and executed async.
//If forestdb has encountered any fatal error condition,
//it will be returned as error.
func (fdb *fdbSlice) Delete(docid []byte) error {

	fdb.cmdCh <- docid
	return fdb.fatalDbErr

}

//handleCommands keep listening to any buffered
//write requests for the slice and processes
//those. This will shut itself down internal
//shutdown channel is closed.
func (fdb *fdbSlice) handleCommandsWorker(workerId int) {

loop:
	for {
		select {
		case c := <-fdb.cmdCh:
			switch c.(type) {
			case kv:
				cmd := c.(kv)
				start := time.Now()
				fdb.insert(cmd.k, cmd.v, workerId)
				elapsed := time.Since(start)
				fdb.totalFlushTime += elapsed
			case []byte:
				cmd := c.([]byte)
				start := time.Now()
				fdb.delete(cmd, workerId)
				elapsed := time.Since(start)
				fdb.totalFlushTime += elapsed
			default:
				common.Errorf("ForestDBSlice::handleCommandsWorker \n\tSliceId %v IndexInstId %v Received "+
					"Unknown Command %v", fdb.id, fdb.idxInstId, c)
			}

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
func (fdb *fdbSlice) insert(k Key, v Value, workerId int) {

	var err error
	var oldkey Key

	common.Tracef("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Set Key - %s "+
		"Value - %s", fdb.id, fdb.idxInstId, k, v)

	//check if the docid exists in the back index
	if oldkey, err = fdb.getBackIndexEntry(v.Docid(), workerId); err != nil {
		fdb.checkFatalDbError(err)
		common.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error locating "+
			"backindex entry %v", fdb.id, fdb.idxInstId, err)
		return
	} else if oldkey.Encoded() != nil {
		//TODO: Handle the case if old-value from backindex matches with the
		//new-value(false mutation). Skip It.

		//there is already an entry in main index for this docid
		//delete from main index
		if err = fdb.main[workerId].DeleteKV(oldkey.Encoded()); err != nil {
			fdb.checkFatalDbError(err)
			common.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error deleting "+
				"entry from main index %v", fdb.id, fdb.idxInstId, err)
			return
		}

		//delete from back index
		if err = fdb.back[workerId].DeleteKV(v.Docid()); err != nil {
			fdb.checkFatalDbError(err)
			common.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error deleting "+
				"entry from back index %v", fdb.id, fdb.idxInstId, err)
			return
		}
	}

	//if the Key is nil, nothing needs to be done
	if k.Encoded() == nil {
		common.Tracef("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Received NIL Key for "+
			"Doc Id %v. Skipped.", fdb.id, fdb.idxInstId, v.Docid())
		return
	}

	//set the back index entry <docid, encodedkey>
	if err = fdb.back[workerId].SetKV([]byte(v.Docid()), k.Encoded()); err != nil {
		fdb.checkFatalDbError(err)
		common.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error in Back Index Set. "+
			"Skipped Key %s. Value %s. Error %v", fdb.id, fdb.idxInstId, v, k, err)
		return
	}

	//set in main index
	if err = fdb.main[workerId].SetKV(k.Encoded(), v.Encoded()); err != nil {
		fdb.checkFatalDbError(err)
		common.Errorf("ForestDBSlice::insert \n\tSliceId %v IndexInstId %v Error in Main Index Set. "+
			"Skipped Key %s. Value %s. Error %v", fdb.id, fdb.idxInstId, k, v, err)
		return
	}

}

//delete does the actual delete in forestdb
func (fdb *fdbSlice) delete(docid []byte, workerId int) {

	common.Tracef("ForestDBSlice::delete \n\tSliceId %v IndexInstId %v. Delete Key - %s",
		fdb.id, fdb.idxInstId, docid)

	var oldkey Key
	var err error

	if oldkey, err = fdb.getBackIndexEntry(docid, workerId); err != nil {
		fdb.checkFatalDbError(err)
		common.Errorf("ForestDBSlice::delete \n\tSliceId %v IndexInstId %v. Error locating "+
			"backindex entry for Doc %s. Error %v", fdb.id, fdb.idxInstId, docid, err)
		return
	}

	//if the oldkey is nil, nothing needs to be done. This is the case of deletes
	//which happened before index was created.
	if oldkey.Encoded() == nil {
		common.Tracef("ForestDBSlice::delete \n\tSliceId %v IndexInstId %v Received NIL Key for "+
			"Doc Id %v. Skipped.", fdb.id, fdb.idxInstId, docid)
		return
	}

	//delete from main index
	if err = fdb.main[workerId].DeleteKV(oldkey.Encoded()); err != nil {
		fdb.checkFatalDbError(err)
		common.Errorf("ForestDBSlice::delete \n\tSliceId %v IndexInstId %v. Error deleting "+
			"entry from main index for Doc %s. Key %v. Error %v", fdb.id, fdb.idxInstId,
			docid, oldkey, err)
		return
	}

	//delete from the back index
	if err = fdb.back[workerId].DeleteKV(docid); err != nil {
		fdb.checkFatalDbError(err)
		common.Errorf("ForestDBSlice::delete \n\tSliceId %v IndexInstId %v. Error deleting "+
			"entry from back index for Doc %s. Error %v", fdb.id, fdb.idxInstId, docid, err)
		return
	}

}

//getBackIndexEntry returns an existing back index entry
//given the docid
func (fdb *fdbSlice) getBackIndexEntry(docid []byte, workerId int) (Key, error) {

	common.Tracef("ForestDBSlice::getBackIndexEntry \n\tSliceId %v IndexInstId %v Get BackIndex Key - %s",
		fdb.id, fdb.idxInstId, docid)

	var k Key
	var kbyte []byte
	var err error

	kbyte, err = fdb.back[workerId].GetKV([]byte(docid))

	//forestdb reports get in a non-existent key as an
	//error, skip that
	if err != nil && err.Error() != "key not found" {
		return k, err
	}

	k, err = NewKeyFromEncodedBytes(kbyte)

	return k, err
}

//checkFatalDbError checks if the error returned from DB
//is fatal and stores it. This error will be returned
//to caller on next DB operation
func (fdb *fdbSlice) checkFatalDbError(err error) {

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
	s := &fdbSnapshot{slice: fdb,
		idxDefnId:  fdb.idxDefnId,
		idxInstId:  fdb.idxInstId,
		main:       fdb.main[0],
		back:       fdb.back[0],
		ts:         snapInfo.Timestamp(),
		mainSeqNum: snapInfo.MainSeq,
		backSeqNum: snapInfo.BackSeq,
		committed:  info.IsCommitted(),
	}

	common.Debugf("ForestDBSlice::OpenSnapshot \n\tSliceId %v IndexInstId %v Creating New "+
		"Snapshot %v committed:%v", fdb.id, fdb.idxInstId, s, s.committed)
	err := s.Open()

	return s, err
}

//Rollback slice to given snapshot. Return error if
//not possible
func (fdb *fdbSlice) Rollback(info SnapshotInfo) error {
	//get the seqnum from snapshot
	mainSeqNum := info.(*fdbSnapshotInfo).MainSeq

	infos, err := fdb.getSnapshotsMeta()
	if err != nil {
		return err
	}

	sic := NewSnapshotInfoContainer(infos)
	sic.RemoveRecentThanTS(info.Timestamp())

	//call forestdb to rollback
	err = fdb.main[0].Rollback(mainSeqNum)
	if err != nil {
		common.Errorf("ForestDBSlice::Rollback \n\tSliceId %v IndexInstId %v. Error Rollback "+
			"Main Index to Snapshot %v. Error %v", fdb.id, fdb.idxInstId, info, err)
		return err
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
	//get the seqnum from snapshot
	mainSeqNum := forestdb.SeqNum(0)

	//call forestdb to rollback
	var err error
	err = fdb.main[0].Rollback(mainSeqNum)
	if err != nil {
		common.Errorf("ForestDBSlice::Rollback \n\tSliceId %v IndexInstId %v. Error Rollback "+
			"Main Index to Zero. Error %v", fdb.id, fdb.idxInstId, err)
		return err
	}

	return nil
}

//Commit persists the outstanding writes in underlying
//forestdb database. If Commit returns error, slice
//should be rolled back to previous snapshot.
func (fdb *fdbSlice) NewSnapshot(ts *common.TsVbuuid, commit bool) (SnapshotInfo, error) {
	//every SLICE_COMMIT_POLL_INTERVAL milliseconds,
	//check for outstanding mutations. If there are
	//none, proceed with the commit.
	ticker := time.NewTicker(time.Millisecond * SLICE_COMMIT_POLL_INTERVAL)
	for _ = range ticker.C {
		if fdb.checkAllWorkersDone() {
			break
		}
	}

	mainDbInfo, err := fdb.main[0].Info()
	if err != nil {
		return nil, err
	}

	backDbInfo, err := fdb.back[0].Info()
	if err != nil {
		return nil, err
	}

	newSnapshotInfo := &fdbSnapshotInfo{
		Ts:        ts,
		MainSeq:   mainDbInfo.LastSeqNum(),
		BackSeq:   backDbInfo.LastSeqNum(),
		Committed: commit,
	}

	if commit {
		infos, err := fdb.getSnapshotsMeta()
		if err != nil {
			return nil, err
		}
		sic := NewSnapshotInfoContainer(infos)
		sic.Add(newSnapshotInfo)

		if sic.Len() > MAX_SNAPSHOTS_PER_INDEX {
			sic.RemoveOldest()
		}

		err = fdb.updateSnapshotsMeta(sic.List())
		if err != nil {
			return nil, err
		}

		// Commit database file
		start := time.Now()
		err = fdb.dbfile.Commit(forestdb.COMMIT_MANUAL_WAL_FLUSH)
		elapsed := time.Since(start)

		fdb.totalCommitTime += elapsed
		common.Debugf("ForestDBSlice::Commit \n\tSliceId %v IndexInstId %v TotalFlushTime %v "+
			"TotalCommitTime %v", fdb.id, fdb.idxInstId, fdb.totalFlushTime, fdb.totalCommitTime)

		if err != nil {
			common.Errorf("ForestDBSlice::Commit \n\tSliceId %v IndexInstId %v Error in "+
				"Index Commit %v", fdb.id, fdb.idxInstId, err)
			return nil, err
		}
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
	for i := 0; i < NUM_WRITER_THREADS_PER_SLICE; i++ {
		fdb.workerDone[i] <- true
		<-fdb.workerDone[i]
	}
	return true
}

func (fdb *fdbSlice) Close() {
	fdb.lock.Lock()
	defer fdb.lock.Unlock()

	common.Infof("ForestDBSlice::Close \n\tClosing Slice Id %v, IndexInstId %v, "+
		"IndexDefnId %v", fdb.idxInstId, fdb.idxDefnId, fdb.id)

	//signal shutdown for command handler routines
	for i := 0; i < NUM_WRITER_THREADS_PER_SLICE; i++ {
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
		common.Infof("ForestDBSlice::Destroy \n\tSoftdeleted Slice Id %v, IndexInstId %v, "+
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

	oldpath := newFdbFile(fdb.path, false)
	newpath := newFdbFile(fdb.path, true)
	err := fdb.dbfile.Compact(newpath)
	if err != nil {
		return err
	}
	err = os.Remove(oldpath)
	return err
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
	common.Infof("ForestDBSlice::Destroy \n\tDestroying Slice Id %v, IndexInstId %v, "+
		"IndexDefnId %v", fdb.id, fdb.idxInstId, fdb.idxDefnId)

	if err := forestdb.Destroy(fdb.path, fdb.config); err != nil {
		common.Errorf("ForestDBSlice::Destroy \n\t Error Destroying  Slice Id %v, "+
			"IndexInstId %v, IndexDefnId %v. Error %v", fdb.id, fdb.idxInstId, fdb.idxDefnId, err)
	}
}

func tryCloseFdbSlice(fdb *fdbSlice) {
	//close the main index
	if fdb.main[0] != nil {
		fdb.main[0].Close()
	}
	//close the back index
	if fdb.back[0] != nil {
		fdb.back[0].Close()
	}

	if fdb.meta != nil {
		fdb.meta.Close()
	}

	fdb.dbfile.Close()
}

func newFdbFile(dirpath string, newVersion bool) string {
	var version int = 0

	pattern := fmt.Sprintf("data.fdb.*")
	files, _ := filepath.Glob(path.Join(dirpath, pattern))
	sort.Strings(files)
	// Pick the first file with least version
	if len(files) > 0 {
		filename := path.Base(files[0])
		_, err := fmt.Sscanf(filename, "data.fdb.%d", &version)
		if err != nil {
			panic(fmt.Sprintf("Invalid data file %s (%v)", files[0], err))
		}
	}

	if newVersion {
		version++
	}

	newFilename := fmt.Sprintf("data.fdb.%d", version)
	return path.Join(dirpath, newFilename)
}
