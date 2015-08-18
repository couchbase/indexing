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
	"errors"
	"fmt"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/memdb"
	"github.com/couchbase/indexing/secondary/platform"
	"os"
	"sync"
	"time"
)

type byteItem []byte

func newByteItem(k, v []byte) byteItem {
	b := make([]byte, 2, 2+len(k)+len(v))
	binary.LittleEndian.PutUint16(b[0:2], uint16(len(k)))
	b = append(b, k...)
	b = append(b, v...)

	return byteItem(b)
}

func (b *byteItem) valOffset() int {
	buf := []byte(*b)
	l := binary.LittleEndian.Uint16(buf[0:2])
	return 2 + int(l)
}

func (b *byteItem) Key() []byte {
	buf := []byte(*b)
	return buf[2:b.valOffset()]
}

func (b *byteItem) Value() []byte {
	buf := []byte(*b)
	return buf[b.valOffset():]
}

func byteItemKeyCompare(a, b []byte) int {
	var l int
	itm1 := byteItem(a)
	itm2 := byteItem(b)

	k1 := []byte(itm1)[2:itm1.valOffset()]
	k2 := []byte(itm2)[2:itm2.valOffset()]

	if len(k1) > len(k2) {
		l = len(k2)
	} else {
		l = len(k1)
	}

	return bytes.Compare(k1[:l], k2[:l])
}

type memdbSlice struct {
	get_bytes, insert_bytes, delete_bytes platform.AlignedInt64
	flushedCount                          platform.AlignedUint64
	committedCount                        platform.AlignedUint64

	path string
	id   SliceId

	refCount int
	lock     sync.RWMutex

	metastore *memdb.MemDB
	mainstore *memdb.MemDB
	backstore *memdb.MemDB

	meta *memdb.Writer
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

	cmdCh  chan interface{}
	stopCh []DoneChannel

	workerDone []chan bool

	fatalDbErr error

	numWriters int

	totalFlushTime  time.Duration
	totalCommitTime time.Duration

	idxStats *IndexStats
	sysconf  common.Config
	confLock sync.Mutex
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

	sliceBufSize := sysconf["settings.sliceBufSize"].Uint64()
	slice.cmdCh = make(chan interface{}, sliceBufSize)
	slice.workerDone = make([]chan bool, slice.numWriters)
	slice.stopCh = make([]DoneChannel, slice.numWriters)

	slice.isPrimary = isPrimary

	slice.mainstore = memdb.New()
	slice.mainstore.SetKeyComparator(byteItemKeyCompare)
	slice.main = make([]*memdb.Writer, slice.numWriters)
	for i := 0; i < slice.numWriters; i++ {
		slice.main[i] = slice.mainstore.NewWriter()
	}

	if !isPrimary {
		slice.backstore = memdb.New()
		slice.backstore.SetKeyComparator(byteItemKeyCompare)
		slice.back = make([]*memdb.Writer, slice.numWriters)
		for i := 0; i < slice.numWriters; i++ {
			slice.back[i] = slice.backstore.NewWriter()
		}
	}

	slice.metastore = memdb.New()
	slice.metastore.SetKeyComparator(byteItemKeyCompare)
	slice.meta = slice.metastore.NewWriter()

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

func (mdb *memdbSlice) Insert(key []byte, docid []byte) error {
	mdb.idxStats.flushQueueSize.Add(1)
	mdb.idxStats.numFlushQueued.Add(1)
	mdb.cmdCh <- &indexItem{key: key, docid: docid}
	return mdb.fatalDbErr
}

func (mdb *memdbSlice) Delete(docid []byte) error {
	mdb.idxStats.flushQueueSize.Add(1)
	mdb.idxStats.numFlushQueued.Add(1)
	mdb.cmdCh <- docid
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
		case c = <-mdb.cmdCh:
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

func (mdb *memdbSlice) insert(key []byte, docid []byte, workerId int) {
	if mdb.isPrimary {
		mdb.insertPrimaryIndex(key, docid, workerId)
	} else {
		mdb.insertSecIndex(key, docid, workerId)
	}

	mdb.logWriterStat()
}

func (mdb *memdbSlice) insertPrimaryIndex(key []byte, docid []byte, workerId int) {
	var found *memdb.Item

	logging.Tracef("MemDBSlice::insert \n\tSliceId %v IndexInstId %v Set Key - %s", mdb.id, mdb.idxInstId, docid)

	//check if the docid exists in the main index
	t0 := time.Now()
	data := newByteItem(key, nil)
	itm := memdb.NewItem(data)
	if found = mdb.main[workerId].Get(itm); found != nil {
		mdb.idxStats.Timings.stKVGet.Put(time.Now().Sub(t0))
		//skip
		logging.Tracef("MemDBSlice::insert \n\tSliceId %v IndexInstId %v Key %v Already Exists. "+
			"Primary Index Update Skipped.", mdb.id, mdb.idxInstId, string(docid))
	} else {
		t0 := time.Now()
		data := newByteItem(key, nil)
		itm := memdb.NewItem(data)
		mdb.main[workerId].Put(itm)
		mdb.idxStats.Timings.stKVSet.Put(time.Now().Sub(t0))
		platform.AddInt64(&mdb.insert_bytes, int64(len(key)))
		mdb.isDirty = true
	}
}

func (mdb *memdbSlice) insertSecIndex(key []byte, docid []byte, workerId int) {
	var oldkey []byte

	oldkey = mdb.getBackIndexEntry(docid, workerId)
	if oldkey != nil {
		if bytes.Equal(oldkey, key) {
			logging.Tracef("MemDBSlice::insert \n\tSliceId %v IndexInstId %v Received Unchanged Key for "+
				"Doc Id %v. Key %v. Skipped.", mdb.id, mdb.idxInstId, string(docid), key)
			return
		}

		t0 := time.Now()
		data := newByteItem(oldkey, nil)
		itm := memdb.NewItem(data)
		mdb.main[workerId].Delete(itm)
		mdb.idxStats.Timings.stKVDelete.Put(time.Now().Sub(t0))
		platform.AddInt64(&mdb.delete_bytes, int64(len(oldkey)))

		t0 = time.Now()
		data = newByteItem(docid, nil)
		itm = memdb.NewItem(data)
		mdb.back[workerId].Delete(itm)

		mdb.idxStats.Timings.stKVDelete.Put(time.Now().Sub(t0))
		platform.AddInt64(&mdb.delete_bytes, int64(len(docid)))
		mdb.isDirty = true
	}

	if key == nil {
		logging.Tracef("MemDBSlice::insert \n\tSliceId %v IndexInstId %v Received NIL Key for "+
			"Doc Id %s. Skipped.", mdb.id, mdb.idxInstId, docid)
		return
	}

	//set the back index entry <docid, encodedkey>
	t0 := time.Now()
	data := newByteItem(docid, key)
	itm := memdb.NewItem(data)
	mdb.back[workerId].Put(itm)
	mdb.idxStats.Timings.stKVSet.Put(time.Now().Sub(t0))
	platform.AddInt64(&mdb.insert_bytes, int64(len(docid)+len(key)))

	t0 = time.Now()
	data = newByteItem(key, nil)
	itm = memdb.NewItem(data)
	mdb.main[workerId].Put(itm)
	mdb.idxStats.Timings.stKVSet.Put(time.Now().Sub(t0))
	platform.AddInt64(&mdb.insert_bytes, int64(len(key)))
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
	data := newByteItem(entry.Bytes(), nil)
	itm := memdb.NewItem(data)
	mdb.main[workerId].Delete(itm)
	mdb.idxStats.Timings.stKVDelete.Put(time.Now().Sub(t0))
	platform.AddInt64(&mdb.delete_bytes, int64(len(entry.Bytes())))
	mdb.isDirty = true

}

func (mdb *memdbSlice) deleteSecIndex(docid []byte, workerId int) {
	olditm := mdb.getBackIndexEntry(docid, workerId)

	//if the oldkey is nil, nothing needs to be done. This is the case of deletes
	//which happened before index was created.
	if olditm == nil {
		logging.Tracef("MemDBSlice::delete \n\tSliceId %v IndexInstId %v Received NIL Key for "+
			"Doc Id %v. Skipped.", mdb.id, mdb.idxInstId, docid)
		return
	}

	//delete from main index
	t0 := time.Now()
	data := newByteItem(olditm, nil)
	itm := memdb.NewItem(data)
	mdb.main[workerId].Delete(itm)
	mdb.idxStats.Timings.stKVDelete.Put(time.Now().Sub(t0))
	platform.AddInt64(&mdb.delete_bytes, int64(len(olditm)))

	//delete from the back index
	t0 = time.Now()
	data = newByteItem(docid, nil)
	itm = memdb.NewItem(data)
	mdb.back[workerId].Delete(itm)
	mdb.idxStats.Timings.stKVDelete.Put(time.Now().Sub(t0))
	platform.AddInt64(&mdb.delete_bytes, int64(len(docid)))
	mdb.isDirty = true
}

//getBackIndexEntry returns an existing back index entry
//given the docid
func (mdb *memdbSlice) getBackIndexEntry(docid []byte, workerId int) []byte {
	var kbytes []byte

	t0 := time.Now()
	data := newByteItem(docid, nil)
	itm := memdb.NewItem(data)
	gotItm := mdb.back[workerId].Get(itm)
	if gotItm == nil {
		kbytes = nil
	} else {
		bItem := byteItem(gotItm.Bytes())
		kbytes = bItem.Value()
	}
	mdb.idxStats.Timings.stKVGet.Put(time.Now().Sub(t0))
	platform.AddInt64(&mdb.get_bytes, int64(len(kbytes)))

	return kbytes
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
	Ts        *common.TsVbuuid
	MainSnap  *memdb.Snapshot
	BackSnap  *memdb.Snapshot
	MetaSnap  *memdb.Snapshot
	Committed bool
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

	var s *memdbSnapshot
	if mdb.isPrimary {
		s = &memdbSnapshot{slice: mdb,
			idxDefnId: mdb.idxDefnId,
			idxInstId: mdb.idxInstId,
			info:      info.(*memdbSnapshotInfo),
			ts:        snapInfo.Timestamp(),
			committed: info.IsCommitted(),
		}
	} else {
		s = &memdbSnapshot{slice: mdb,
			idxDefnId: mdb.idxDefnId,
			idxInstId: mdb.idxInstId,
			info:      info.(*memdbSnapshotInfo),
			ts:        snapInfo.Timestamp(),
			committed: info.IsCommitted(),
		}
	}

	s.Open()
	s.slice.IncrRef()

	logging.Infof("MemDBSlice::OpenSnapshot SliceId %v IndexInstId %v Creating New "+
		"Snapshot %v", mdb.id, mdb.idxInstId, snapInfo)

	return s, nil
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
	return nil
}

//RollbackToZero rollbacks the slice to initial state. Return error if
//not possible
func (mdb *memdbSlice) RollbackToZero() error {
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
		BackSnap:  mdb.backstore.NewSnapshot(),
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
	if len(mdb.cmdCh) > 0 {
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

// Returns snapshot info list
func (mdb *memdbSlice) GetSnapshots() ([]SnapshotInfo, error) {
	return nil, nil
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

func (mdb *memdbSlice) updateSnapshotsMeta(infos []SnapshotInfo) error {
	return nil
}

func (mdb *memdbSlice) getSnapshotsMeta() ([]SnapshotInfo, error) {
	return nil, nil
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

func (mdb *memdbSlice) logWriterStat() {
	count := platform.AddUint64(&mdb.flushedCount, 1)
	if (count%10000 == 0) || count == 1 {
		logging.Infof("logWriterStat:: %v "+
			"FlushedCount %v QueuedCount %v", mdb.idxInstId,
			count, len(mdb.cmdCh))
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
	s.info.BackSnap.Close()

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
		itm := memdb.NewItem(newByteItem(low.Bytes(), nil))
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
		itm := byteItem(it.Get().Bytes())
		entry = s.newIndexEntry(itm.Key())

		// Iterator has reached past the high key, no need to scan further
		if cmpFn(high, entry) <= 0 {
			break loop
		}

		err = callback(itm.Key())
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
		itm := byteItem(it.Get().Bytes())
		entry = s.newIndexEntry(itm.Key())
		if cmpFn(k, entry) == 0 {
			if callback != nil {
				err = callback(itm.Key())
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
