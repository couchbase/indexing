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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/memdb"
	"github.com/couchbase/indexing/secondary/platform"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

func docIdFromEntryBytes(e []byte) []byte {
	offset := len(e) - 2
	l := int(binary.LittleEndian.Uint16(e[offset : offset+2]))
	offset -= l
	return e[offset : offset+l]
}

func entryBytesFromDocId(docid []byte) []byte {
	l := len(docid)
	entry := make([]byte, l+2)
	copy(entry, docid)
	binary.LittleEndian.PutUint16(entry[l:l+2], uint16(l))
	return entry
}

func byteItemDocIdCompare(a, b []byte) int {
	docid1 := docIdFromEntryBytes(a)
	docid2 := docIdFromEntryBytes(b)

	return bytes.Compare(docid1, docid2)
}

func byteItemCompare(a, b []byte) int {
	return bytes.Compare(a, b)
}

type memdbSlice struct {
	get_bytes, insert_bytes, delete_bytes platform.AlignedInt64
	flushedCount                          platform.AlignedUint64
	committedCount                        platform.AlignedUint64

	path string
	id   SliceId

	refCount int
	lock     sync.RWMutex

	mainstore *memdb.MemDB
	backstore *memdb.MemDB

	main []*memdb.Writer
	back []*memdb.Writer

	idxDefnId common.IndexDefnId
	idxInstId common.IndexInstId

	status        SliceStatus
	isActive      bool
	isDirty       bool
	isPrimary     bool
	isSoftDeleted bool
	isSoftClosed  bool

	cmdCh  []chan interface{}
	stopCh []DoneChannel

	workerDone []chan bool

	fatalDbErr error

	numWriters   int
	maxRollbacks int

	totalFlushTime  time.Duration
	totalCommitTime time.Duration

	idxStats *IndexStats
	sysconf  common.Config
	confLock sync.Mutex

	isPersistorActive int32
}

func NewMemDBSlice(path string, sliceId SliceId, idxDefnId common.IndexDefnId,
	idxInstId common.IndexInstId, isPrimary bool,
	sysconf common.Config, idxStats *IndexStats) (*memdbSlice, error) {

	info, err := os.Stat(path)
	if err != nil || err == nil && info.IsDir() {
		os.Mkdir(path, 0777)
	}

	slice := &memdbSlice{}
	slice.idxStats = idxStats

	slice.get_bytes = platform.NewAlignedInt64(0)
	slice.insert_bytes = platform.NewAlignedInt64(0)
	slice.delete_bytes = platform.NewAlignedInt64(0)
	slice.flushedCount = platform.NewAlignedUint64(0)
	slice.committedCount = platform.NewAlignedUint64(0)
	slice.sysconf = sysconf
	slice.path = path
	slice.idxInstId = idxInstId
	slice.idxDefnId = idxDefnId
	slice.id = sliceId
	slice.numWriters = sysconf["numSliceWriters"].Int()
	slice.maxRollbacks = sysconf["settings.recovery.max_rollbacks"].Int()

	sliceBufSize := sysconf["settings.sliceBufSize"].Uint64()
	slice.cmdCh = make([]chan interface{}, slice.numWriters)
	for i := 0; i < slice.numWriters; i++ {
		slice.cmdCh[i] = make(chan interface{}, sliceBufSize)
	}
	slice.workerDone = make([]chan bool, slice.numWriters)
	slice.stopCh = make([]DoneChannel, slice.numWriters)

	slice.isPrimary = isPrimary

	slice.mainstore = memdb.New()
	slice.mainstore.SetKeyComparator(byteItemCompare)
	slice.main = make([]*memdb.Writer, slice.numWriters)
	for i := 0; i < slice.numWriters; i++ {
		slice.main[i] = slice.mainstore.NewWriter()
	}

	if !isPrimary {
		slice.backstore = memdb.New()
		slice.backstore.SetKeyComparator(byteItemDocIdCompare)
		slice.back = make([]*memdb.Writer, slice.numWriters)
		for i := 0; i < slice.numWriters; i++ {
			slice.back[i] = slice.backstore.NewWriter()
		}
	}

	for i := 0; i < slice.numWriters; i++ {
		slice.stopCh[i] = make(DoneChannel)
		slice.workerDone[i] = make(chan bool)
		go slice.handleCommandsWorker(i)
	}

	logging.Infof("MemDBSlice:NewMemDBSlice Created New Slice Id %v IndexInstId %v "+
		"WriterThreads %v", sliceId, idxInstId, slice.numWriters)

	slice.setCommittedCount()

	return slice, nil
}

func (mdb *memdbSlice) IncrRef() {
	mdb.lock.Lock()
	defer mdb.lock.Unlock()

	mdb.refCount++
}

func (mdb *memdbSlice) DecrRef() {
	mdb.lock.Lock()
	defer mdb.lock.Unlock()

	mdb.refCount--
	if mdb.refCount == 0 {
		if mdb.isSoftClosed {
			tryClosememdbSlice(mdb)
		}
		if mdb.isSoftDeleted {
			tryDeletememdbSlice(mdb)
		}
	}
}

func (mdb *memdbSlice) Insert(key []byte, docid []byte, meta *MutationMeta) error {
	mdb.idxStats.flushQueueSize.Add(1)
	mdb.idxStats.numFlushQueued.Add(1)
	mdb.cmdCh[int(meta.vbucket)%mdb.numWriters] <- &indexItem{key: key, docid: docid}
	return mdb.fatalDbErr
}

func (mdb *memdbSlice) Delete(docid []byte, meta *MutationMeta) error {
	mdb.idxStats.flushQueueSize.Add(1)
	mdb.idxStats.numFlushQueued.Add(1)
	mdb.cmdCh[int(meta.vbucket)%mdb.numWriters] <- docid
	return mdb.fatalDbErr
}

func (mdb *memdbSlice) handleCommandsWorker(workerId int) {
	var start time.Time
	var elapsed time.Duration
	var c interface{}
	var icmd *indexItem
	var dcmd []byte

loop:
	for {
		select {
		case c = <-mdb.cmdCh[workerId]:
			switch c.(type) {

			case *indexItem:
				icmd = c.(*indexItem)
				start = time.Now()
				mdb.insert((*icmd).key, (*icmd).docid, workerId)
				elapsed = time.Since(start)
				mdb.totalFlushTime += elapsed

			case []byte:
				dcmd = c.([]byte)
				start = time.Now()
				mdb.delete(dcmd, workerId)
				elapsed = time.Since(start)
				mdb.totalFlushTime += elapsed

			default:
				logging.Errorf("MemDBSlice::handleCommandsWorker \n\tSliceId %v IndexInstId %v Received "+
					"Unknown Command %v", mdb.id, mdb.idxInstId, c)
			}

			mdb.idxStats.flushQueueSize.Add(-1)

		case <-mdb.stopCh[workerId]:
			mdb.stopCh[workerId] <- true
			break loop

		case <-mdb.workerDone[workerId]:
			mdb.workerDone[workerId] <- true

		}
	}
}

func (mdb *memdbSlice) insert(entry []byte, docid []byte, workerId int) {
	if mdb.isPrimary {
		mdb.insertPrimaryIndex(entry, docid, workerId)
	} else {
		mdb.insertSecIndex(entry, docid, workerId)
	}

	mdb.logWriterStat()
}

func (mdb *memdbSlice) insertPrimaryIndex(entry []byte, docid []byte, workerId int) {
	var found *memdb.Item

	logging.Tracef("MemDBSlice::insert \n\tSliceId %v IndexInstId %v Set Key - %s", mdb.id, mdb.idxInstId, docid)

	//check if the docid exists in the main index
	t0 := time.Now()
	itm := memdb.NewItem(entry)
	if found = mdb.main[workerId].Get(itm); found != nil {
		mdb.idxStats.Timings.stKVGet.Put(time.Now().Sub(t0))
		//skip
		logging.Tracef("MemDBSlice::insert \n\tSliceId %v IndexInstId %v Key %v Already Exists. "+
			"Primary Index Update Skipped.", mdb.id, mdb.idxInstId, string(docid))
	} else {
		t0 := time.Now()
		itm := memdb.NewItem(entry)
		mdb.main[workerId].Put(itm)
		mdb.idxStats.Timings.stKVSet.Put(time.Now().Sub(t0))
		platform.AddInt64(&mdb.insert_bytes, int64(len(entry)))
		mdb.isDirty = true
	}
}

func (mdb *memdbSlice) insertSecIndex(entry []byte, docid []byte, workerId int) {
	var oldentry []byte
	var lookupentry []byte
	if entry == nil {
		lookupentry = entryBytesFromDocId(docid)
	} else {
		lookupentry = entry
	}

	oldentry = mdb.getBackIndexEntry(lookupentry, workerId)
	if oldentry != nil {
		if bytes.Equal(oldentry, entry) {
			logging.Tracef("MemDBSlice::insert \n\tSliceId %v IndexInstId %v Received Unchanged Key for "+
				"Doc Id %v. Key %v. Skipped.", mdb.id, mdb.idxInstId, string(docid), entry)
			return
		}

		t0 := time.Now()
		itm := memdb.NewItem(oldentry)
		mdb.main[workerId].Delete(itm)
		mdb.idxStats.Timings.stKVDelete.Put(time.Now().Sub(t0))
		platform.AddInt64(&mdb.delete_bytes, int64(len(oldentry)))

		t0 = time.Now()
		mdb.back[workerId].Delete(itm)
		mdb.idxStats.Timings.stKVDelete.Put(time.Now().Sub(t0))
		platform.AddInt64(&mdb.delete_bytes, int64(len(docid)))
		mdb.isDirty = true
	}

	if entry == nil {
		logging.Tracef("MemDBSlice::insert \n\tSliceId %v IndexInstId %v Received NIL Key for "+
			"Doc Id %s. Skipped.", mdb.id, mdb.idxInstId, docid)
		return
	}

	//set the back index entry <docid, encodedkey>
	t0 := time.Now()
	itm := memdb.NewItem(entry)
	mdb.back[workerId].Put(itm)
	mdb.idxStats.Timings.stKVSet.Put(time.Now().Sub(t0))
	platform.AddInt64(&mdb.insert_bytes, int64(len(docid)+len(entry)))

	t0 = time.Now()
	itm = memdb.NewItem(entry)
	mdb.main[workerId].Put(itm)
	mdb.idxStats.Timings.stKVSet.Put(time.Now().Sub(t0))
	platform.AddInt64(&mdb.insert_bytes, int64(len(entry)))
	mdb.isDirty = true
}

//delete does the actual delete in forestdb
func (mdb *memdbSlice) delete(docid []byte, workerId int) {
	if mdb.isPrimary {
		mdb.deletePrimaryIndex(docid, workerId)
	} else {
		mdb.deleteSecIndex(docid, workerId)
	}

	mdb.logWriterStat()
}

func (mdb *memdbSlice) deletePrimaryIndex(docid []byte, workerId int) {
	if docid == nil {
		common.CrashOnError(errors.New("Nil Primary Key"))
		return
	}

	//docid -> key format
	entry, err := NewPrimaryIndexEntry(docid)
	common.CrashOnError(err)

	//delete from main index
	t0 := time.Now()
	itm := memdb.NewItem(entry.Bytes())
	mdb.main[workerId].Delete(itm)
	mdb.idxStats.Timings.stKVDelete.Put(time.Now().Sub(t0))
	platform.AddInt64(&mdb.delete_bytes, int64(len(entry.Bytes())))
	mdb.isDirty = true

}

func (mdb *memdbSlice) deleteSecIndex(docid []byte, workerId int) {
	lookupentry := entryBytesFromDocId(docid)
	oldentry := mdb.getBackIndexEntry(lookupentry, workerId)

	//if the oldkey is nil, nothing needs to be done. This is the case of deletes
	//which happened before index was created.
	if oldentry == nil {
		logging.Tracef("MemDBSlice::delete \n\tSliceId %v IndexInstId %v Received NIL Key for "+
			"Doc Id %v. Skipped.", mdb.id, mdb.idxInstId, docid)
		return
	}

	//delete from main index
	t0 := time.Now()
	itm := memdb.NewItem(oldentry)
	mdb.main[workerId].Delete(itm)
	mdb.idxStats.Timings.stKVDelete.Put(time.Now().Sub(t0))
	platform.AddInt64(&mdb.delete_bytes, int64(len(oldentry)))

	//delete from the back index
	t0 = time.Now()
	itm = memdb.NewItem(oldentry)
	mdb.back[workerId].Delete(itm)
	mdb.idxStats.Timings.stKVDelete.Put(time.Now().Sub(t0))
	platform.AddInt64(&mdb.delete_bytes, int64(len(docid)))
	mdb.isDirty = true
}

//getBackIndexEntry returns an existing back index entry
//given the docid
func (mdb *memdbSlice) getBackIndexEntry(entry []byte, workerId int) []byte {
	var gotBytes []byte

	t0 := time.Now()
	itm := memdb.NewItem(entry)
	gotItm := mdb.back[workerId].Get(itm)
	if gotItm == nil {
		gotBytes = nil
	} else {
		gotBytes = gotItm.Bytes()
	}
	mdb.idxStats.Timings.stKVGet.Put(time.Now().Sub(t0))
	platform.AddInt64(&mdb.get_bytes, int64(len(gotBytes)))

	return gotBytes
}

//checkFatalDbError checks if the error returned from DB
//is fatal and stores it. This error will be returned
//to caller on next DB operation
func (mdb *memdbSlice) checkFatalDbError(err error) {

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

type memdbSnapshotInfo struct {
	Ts       *common.TsVbuuid
	MainSnap *memdb.Snapshot `json:"-"`

	Committed bool `json:"-"`
	dataPath  string
}

type memdbSnapshot struct {
	slice     *memdbSlice
	idxDefnId common.IndexDefnId
	idxInstId common.IndexInstId
	ts        *common.TsVbuuid
	info      *memdbSnapshotInfo
	committed bool

	refCount int32
}

// Creates an open snapshot handle from snapshot info
// Snapshot info is obtained from NewSnapshot() or GetSnapshots() API
// Returns error if snapshot handle cannot be created.
func (mdb *memdbSlice) OpenSnapshot(info SnapshotInfo) (Snapshot, error) {
	snapInfo := info.(*memdbSnapshotInfo)

	s := &memdbSnapshot{slice: mdb,
		idxDefnId: mdb.idxDefnId,
		idxInstId: mdb.idxInstId,
		info:      info.(*memdbSnapshotInfo),
		ts:        snapInfo.Timestamp(),
		committed: info.IsCommitted(),
	}

	s.Open()
	s.slice.IncrRef()
	if s.committed {
		s.Open()
		go mdb.doPersistSnapshot(s)
	}

	if s.info.MainSnap == nil {
		mdb.loadSnapshot(s.info)
	}

	logging.Infof("MemDBSlice::OpenSnapshot SliceId %v IndexInstId %v Creating New "+
		"Snapshot %v", mdb.id, mdb.idxInstId, snapInfo)

	return s, nil
}

func (mdb *memdbSlice) doPersistSnapshot(s *memdbSnapshot) {
	defer s.Close()
	if platform.CompareAndSwapInt32(&mdb.isPersistorActive, 0, 1) {
		defer platform.StoreInt32(&mdb.isPersistorActive, 0)

		dir := newSnapshotPath(mdb.path, true)
		tmpdir := filepath.Join(mdb.path, "tmp")
		datadir := filepath.Join(tmpdir, "data")
		manifest := filepath.Join(tmpdir, "manifest.json")
		os.RemoveAll(tmpdir)
		err := mdb.mainstore.StoreToDisk(datadir, s.info.MainSnap, nil)
		if err == nil {
			var fd *os.File
			bs, err := json.Marshal(s.info)
			if err == nil {
				fd, err = os.OpenFile(manifest, os.O_WRONLY|os.O_CREATE, 0755)
				_, err = fd.Write(bs)
				if err == nil {
					err = fd.Close()
				}
			}

			if err == nil {
				err = os.Rename(tmpdir, dir)
				if err == nil {
					mdb.cleanupOldSnapshotFiles()
				}
			}
		}
	}
}

func (mdb *memdbSlice) cleanupOldSnapshotFiles() {
	manifests := mdb.getSnapshotManifests()
	if len(manifests) > mdb.maxRollbacks {
		toRemove := len(manifests) - mdb.maxRollbacks
		manifests = manifests[:toRemove]
		for _, m := range manifests {
			dir := path.Dir(m)
			os.RemoveAll(dir)
		}
	}
}

func (mdb *memdbSlice) getSnapshotManifests() []string {
	pattern := fmt.Sprintf("*/manifest.json")
	files, _ := filepath.Glob(filepath.Join(mdb.path, pattern))
	sort.Strings(files)
	return files
}

// Returns snapshot info list
func (mdb *memdbSlice) GetSnapshots() ([]SnapshotInfo, error) {
	var infos []SnapshotInfo

	files := mdb.getSnapshotManifests()
	for _, f := range files {
		info := &memdbSnapshotInfo{dataPath: path.Dir(f)}
		fd, err := os.Open(f)
		if err == nil {
			defer fd.Close()
			bs, err := ioutil.ReadAll(fd)
			if err == nil {
				err = json.Unmarshal(bs, info)
				if err == nil {
					infos = append(infos, info)
				}
			}
		}

	}
	return infos, nil
}

func (mdb *memdbSlice) setCommittedCount() {
	platform.StoreUint64(&mdb.committedCount, uint64(mdb.mainstore.ItemsCount()))
}

func (mdb *memdbSlice) GetCommittedCount() uint64 {
	return platform.LoadUint64(&mdb.committedCount)
}

//Rollback slice to given snapshot. Return error if
//not possible
func (mdb *memdbSlice) Rollback(info SnapshotInfo) error {
	snapInfo := info.(*memdbSnapshotInfo)
	mdb.mainstore.Reset()
	if mdb.backstore != nil {
		mdb.backstore.Reset()
	}

	return mdb.loadSnapshot(snapInfo)
}

func (mdb *memdbSlice) loadSnapshot(snapInfo *memdbSnapshotInfo) error {
	var wg sync.WaitGroup

	var backIndexCallback memdb.ItemCallback
	entryCh := make(chan []byte, 1000)

	logging.Infof("MemDBSlice::loadSnapshot Slice Id %v, IndexInstId %v reading %v",
		mdb.id, mdb.idxInstId, snapInfo.dataPath)

	t0 := time.Now()
	datadir := filepath.Join(snapInfo.dataPath, "data")

	if !mdb.isPrimary {
		for wId := 0; wId < mdb.numWriters; wId++ {
			wg.Add(1)
			go func(i int, wg *sync.WaitGroup) {
				defer wg.Done()
				for entry := range entryCh {
					if !mdb.isPrimary {
						itm := memdb.NewItem(entry)
						mdb.back[i].Put(itm)
					}
				}
			}(wId, &wg)
		}

		backIndexCallback = func(itm *memdb.Item) {
			entryCh <- itm.Bytes()
		}
	}

	snap, err := mdb.mainstore.LoadFromDisk(datadir, backIndexCallback)

	if !mdb.isPrimary {
		close(entryCh)
		wg.Wait()
	}

	if err == nil {
		snapInfo.MainSnap = snap
	}

	dur := time.Since(t0)
	logging.Infof("MemDBSlice::loadSnapshot Slice Id %v, IndexInstId %v finished reading %v. Took %v",
		mdb.id, mdb.idxInstId, snapInfo.dataPath, dur)

	return err
}

//RollbackToZero rollbacks the slice to initial state. Return error if
//not possible
func (mdb *memdbSlice) RollbackToZero() error {
	mdb.mainstore.Reset()
	if !mdb.isPrimary {
		mdb.backstore.Reset()
	}
	return nil
}

//slice insert/delete methods are async. There
//can be outstanding mutations in internal queue to flush even
//after insert/delete have return success to caller.
//This method provides a mechanism to wait till internal
//queue is empty.
func (mdb *memdbSlice) waitPersist() {

	if !mdb.checkAllWorkersDone() {
		//every SLICE_COMMIT_POLL_INTERVAL milliseconds,
		//check for outstanding mutations. If there are
		//none, proceed with the commit.
		ticker := time.NewTicker(time.Millisecond * SLICE_COMMIT_POLL_INTERVAL)
		for _ = range ticker.C {
			if mdb.checkAllWorkersDone() {
				break
			}
		}
	}

}

//Commit persists the outstanding writes in underlying
//forestdb database. If Commit returns error, slice
//should be rolled back to previous snapshot.
func (mdb *memdbSlice) NewSnapshot(ts *common.TsVbuuid, commit bool) (SnapshotInfo, error) {

	mdb.waitPersist()
	mdb.isDirty = false

	newSnapshotInfo := &memdbSnapshotInfo{
		Ts:        ts,
		MainSnap:  mdb.mainstore.NewSnapshot(),
		Committed: commit,
	}
	mdb.setCommittedCount()

	return newSnapshotInfo, nil
}

//checkAllWorkersDone return true if all workers have
//finished processing
func (mdb *memdbSlice) checkAllWorkersDone() bool {

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

func (mdb *memdbSlice) Close() {
	mdb.lock.Lock()
	defer mdb.lock.Unlock()

	logging.Infof("MemDBSlice::Close Closing Slice Id %v, IndexInstId %v, "+
		"IndexDefnId %v", mdb.idxInstId, mdb.idxDefnId, mdb.id)

	//signal shutdown for command handler routines
	for i := 0; i < mdb.numWriters; i++ {
		mdb.stopCh[i] <- true
		<-mdb.stopCh[i]
	}

	if mdb.refCount > 0 {
		mdb.isSoftClosed = true
	} else {
		tryClosememdbSlice(mdb)
	}
}

//Destroy removes the database file from disk.
//Slice is not recoverable after this.
func (mdb *memdbSlice) Destroy() {
	mdb.lock.Lock()
	defer mdb.lock.Unlock()

	if mdb.refCount > 0 {
		logging.Infof("MemDBSlice::Destroy Softdeleted Slice Id %v, IndexInstId %v, "+
			"IndexDefnId %v", mdb.id, mdb.idxInstId, mdb.idxDefnId)
		mdb.isSoftDeleted = true
	} else {
		tryDeletememdbSlice(mdb)
	}
}

//Id returns the Id for this Slice
func (mdb *memdbSlice) Id() SliceId {
	return mdb.id
}

// FilePath returns the filepath for this Slice
func (mdb *memdbSlice) Path() string {
	return mdb.path
}

//IsActive returns if the slice is active
func (mdb *memdbSlice) IsActive() bool {
	return mdb.isActive
}

//SetActive sets the active state of this slice
func (mdb *memdbSlice) SetActive(isActive bool) {
	mdb.isActive = isActive
}

//Status returns the status for this slice
func (mdb *memdbSlice) Status() SliceStatus {
	return mdb.status
}

//SetStatus set new status for this slice
func (mdb *memdbSlice) SetStatus(status SliceStatus) {
	mdb.status = status
}

//IndexInstId returns the Index InstanceId this
//slice is associated with
func (mdb *memdbSlice) IndexInstId() common.IndexInstId {
	return mdb.idxInstId
}

//IndexDefnId returns the Index DefnId this slice
//is associated with
func (mdb *memdbSlice) IndexDefnId() common.IndexDefnId {
	return mdb.idxDefnId
}

// IsDirty returns true if there has been any change in
// in the slice storage after last in-mem/persistent snapshot
func (mdb *memdbSlice) IsDirty() bool {
	mdb.waitPersist()
	return mdb.isDirty
}

func (mdb *memdbSlice) Compact() error {
	return nil
}

func (mdb *memdbSlice) Statistics() (StorageStatistics, error) {
	var sts StorageStatistics
	return sts, nil
}

func (mdb *memdbSlice) UpdateConfig(cfg common.Config) {
	mdb.confLock.Lock()
	defer mdb.confLock.Unlock()

	mdb.sysconf = cfg
}

func (mdb *memdbSlice) String() string {

	str := fmt.Sprintf("SliceId: %v ", mdb.id)
	str += fmt.Sprintf("File: %v ", mdb.path)
	str += fmt.Sprintf("Index: %v ", mdb.idxInstId)

	return str

}

func tryDeletememdbSlice(mdb *memdbSlice) {

	//cleanup the disk directory
	if err := os.RemoveAll(mdb.path); err != nil {
		logging.Errorf("MemDBSlice::Destroy Error Cleaning Up Slice Id %v, "+
			"IndexInstId %v, IndexDefnId %v. Error %v", mdb.id, mdb.idxInstId, mdb.idxDefnId, err)
	}
}

func tryClosememdbSlice(mdb *memdbSlice) {
}

func (mdb *memdbSlice) getCmdsCount() int {
	c := 0
	for i := 0; i < mdb.numWriters; i++ {
		c += len(mdb.cmdCh[i])
	}

	return c
}

func (mdb *memdbSlice) logWriterStat() {
	count := platform.AddUint64(&mdb.flushedCount, 1)
	if (count%10000 == 0) || count == 1 {
		logging.Infof("logWriterStat:: %v "+
			"FlushedCount %v QueuedCount %v", mdb.idxInstId,
			count, mdb.getCmdsCount())
	}

}

func (info *memdbSnapshotInfo) Timestamp() *common.TsVbuuid {
	return info.Ts
}

func (info *memdbSnapshotInfo) IsCommitted() bool {
	return info.Committed
}

func (info *memdbSnapshotInfo) String() string {
	return fmt.Sprintf("SnapshotInfo: committed:%v", info.Committed)
}

func (s *memdbSnapshot) Create() error {
	return nil
}

func (s *memdbSnapshot) Open() error {
	platform.AddInt32(&s.refCount, int32(1))

	return nil
}

func (s *memdbSnapshot) IsOpen() bool {

	count := platform.LoadInt32(&s.refCount)
	return count > 0
}

func (s *memdbSnapshot) Id() SliceId {
	return s.slice.Id()
}

func (s *memdbSnapshot) IndexInstId() common.IndexInstId {
	return s.idxInstId
}

func (s *memdbSnapshot) IndexDefnId() common.IndexDefnId {
	return s.idxDefnId
}

func (s *memdbSnapshot) Timestamp() *common.TsVbuuid {
	return s.ts
}

//Close the snapshot
func (s *memdbSnapshot) Close() error {

	count := platform.AddInt32(&s.refCount, int32(-1))

	if count < 0 {
		logging.Errorf("MemDBSnapshot::Close Close operation requested " +
			"on already closed snapshot")
		return errors.New("Snapshot Already Closed")

	} else if count == 0 {
		go s.Destroy()
	}

	return nil
}

func (s *memdbSnapshot) Destroy() {
	s.info.MainSnap.Close()

	defer s.slice.DecrRef()
}

func (s *memdbSnapshot) String() string {

	str := fmt.Sprintf("Index: %v ", s.idxInstId)
	str += fmt.Sprintf("SliceId: %v ", s.slice.Id())
	str += fmt.Sprintf("TS: %v ", s.ts)
	return str
}

func (s *memdbSnapshot) Info() SnapshotInfo {
	return s.info
}

// ==============================
// Snapshot reader implementation
// ==============================

// Approximate items count
func (s *memdbSnapshot) StatCountTotal() (uint64, error) {
	c := s.slice.GetCommittedCount()
	return c, nil
}

func (s *memdbSnapshot) CountTotal(stopch StopChannel) (uint64, error) {
	return s.CountRange(MinIndexKey, MaxIndexKey, Both, stopch)
}

func (s *memdbSnapshot) CountRange(low, high IndexKey, inclusion Inclusion,
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

	err := s.Range(low, high, inclusion, callb)
	return count, err
}

func (s *memdbSnapshot) CountLookup(keys []IndexKey, stopch StopChannel) (uint64, error) {
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
		if err = s.Lookup(k, callb); err != nil {
			break
		}
	}

	return count, err
}

func (s *memdbSnapshot) Exists(key IndexKey, stopch StopChannel) (bool, error) {
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

	err := s.Lookup(key, callb)
	return count != 0, err
}

func (s *memdbSnapshot) Lookup(key IndexKey, callb EntryCallback) error {
	return s.Iterate(key, key, Both, compareExact, callb)
}

func (s *memdbSnapshot) Range(low, high IndexKey, inclusion Inclusion,
	callb EntryCallback) error {

	var cmpFn CmpEntry
	if s.isPrimary() {
		cmpFn = compareExact
	} else {
		cmpFn = comparePrefix
	}

	return s.Iterate(low, high, inclusion, cmpFn, callb)
}

func (s *memdbSnapshot) All(callb EntryCallback) error {
	return s.Range(MinIndexKey, MaxIndexKey, Both, callb)
}

func (s *memdbSnapshot) Iterate(low, high IndexKey, inclusion Inclusion,
	cmpFn CmpEntry, callback EntryCallback) error {

	var entry IndexEntry
	var err error
	it := s.info.MainSnap.NewIterator()
	defer it.Close()

	if low.Bytes() == nil {
		it.SeekFirst()
	} else {
		itm := memdb.NewItem(low.Bytes())
		it.Seek(itm)

		// Discard equal keys if low inclusion is requested
		if inclusion == Neither || inclusion == High {
			err = s.iterEqualKeys(low, it, cmpFn, nil)
			if err != nil {
				return err
			}
		}
	}

loop:
	for ; it.Valid(); it.Next() {
		itm := it.Get().Bytes()
		entry = s.newIndexEntry(itm)

		// Iterator has reached past the high key, no need to scan further
		if cmpFn(high, entry) <= 0 {
			break loop
		}

		err = callback(itm)
		if err != nil {
			return err
		}
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

func (s *memdbSnapshot) isPrimary() bool {
	return s.slice.isPrimary
}

func (s *memdbSnapshot) newIndexEntry(b []byte) IndexEntry {
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

func (s *memdbSnapshot) iterEqualKeys(k IndexKey, it *memdb.Iterator,
	cmpFn CmpEntry, callback func([]byte) error) error {
	var err error

	var entry IndexEntry
	for ; it.Valid(); it.Next() {
		itm := it.Get().Bytes()
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

func newSnapshotPath(dirpath string, newVersion bool) string {
	var version int = 0
	pattern := fmt.Sprintf("snapshot.*")
	files, _ := filepath.Glob(filepath.Join(dirpath, pattern))
	sort.Strings(files)

	// Pick the first file with highest version
	if len(files) > 0 {
		filename := filepath.Base(files[len(files)-1])
		_, err := fmt.Sscanf(filename, "snapshot.%d", &version)
		if err != nil {
			panic(fmt.Sprintf("Invalid data file %s (%v)", files[0], err))
		}
	}

	if newVersion {
		version++
	}

	newFilename := fmt.Sprintf("snapshot.%d", version)
	return filepath.Join(dirpath, newFilename)
}
