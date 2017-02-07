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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/common/queryutil"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/platform"
	"github.com/couchbase/nitro/plasma"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type plasmaSlice struct {
	newBorn                               bool
	get_bytes, insert_bytes, delete_bytes platform.AlignedInt64
	flushedCount                          platform.AlignedUint64
	committedCount                        platform.AlignedUint64
	qCount                                platform.AlignedInt64

	path string
	id   SliceId

	refCount int
	lock     sync.RWMutex

	mainstore *plasma.Plasma
	backstore *plasma.Plasma

	main []*plasma.Writer

	back []*plasma.Writer

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
	arrayBuf  [][]byte

	hasPersistence bool
}

func NewPlasmaSlice(path string, sliceId SliceId, idxDefn common.IndexDefn,
	idxInstId common.IndexInstId, isPrimary bool,
	sysconf common.Config, idxStats *IndexStats) (*plasmaSlice, error) {

	slice := &plasmaSlice{}

	_, err := os.Stat(path)
	if err != nil {
		os.Mkdir(path, 0777)
		slice.newBorn = true
	}

	slice.idxStats = idxStats

	slice.get_bytes = platform.NewAlignedInt64(0)
	slice.insert_bytes = platform.NewAlignedInt64(0)
	slice.delete_bytes = platform.NewAlignedInt64(0)
	slice.flushedCount = platform.NewAlignedUint64(0)
	slice.committedCount = platform.NewAlignedUint64(0)
	slice.sysconf = sysconf
	slice.path = path
	slice.idxInstId = idxInstId
	slice.idxDefnId = idxDefn.DefnId
	slice.idxDefn = idxDefn
	slice.id = sliceId
	slice.numWriters = sysconf["numSliceWriters"].Int()
	slice.hasPersistence = !sysconf["plasma.disablePersistence"].Bool()
	// slice.maxRollbacks = sysconf["settings.recovery.max_rollbacks"].Int()
	slice.maxRollbacks = 2

	sliceBufSize := sysconf["settings.sliceBufSize"].Uint64()
	if sliceBufSize < uint64(slice.numWriters) {
		sliceBufSize = uint64(slice.numWriters)
	}

	slice.encodeBuf = make([][]byte, slice.numWriters)
	slice.arrayBuf = make([][]byte, slice.numWriters)
	slice.cmdCh = make([]chan indexMutation, slice.numWriters)

	for i := 0; i < slice.numWriters; i++ {
		slice.cmdCh[i] = make(chan indexMutation, sliceBufSize/uint64(slice.numWriters))
		slice.encodeBuf[i] = make([]byte, 0, maxIndexEntrySize)
		slice.arrayBuf[i] = make([]byte, 0, maxArrayIndexEntrySize)
	}

	slice.workerDone = make([]chan bool, slice.numWriters)
	slice.stopCh = make([]DoneChannel, slice.numWriters)

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
	cfg.FlushBufferSize = int(slice.sysconf["plasma.flushBufferSize"].Int())
	cfg.AutoSwapper = true
	cfg.LSSCleanerThreshold = 30

	if slice.hasPersistence {
		cfg.File = filepath.Join(slice.path, "mainIndex")
	}

	t0 := time.Now()
	slice.mainstore, err = plasma.New(cfg)
	if err != nil {
		return fmt.Errorf("Unable to initialize %s, err = %v", cfg.File, err)
	}
	slice.main = make([]*plasma.Writer, slice.numWriters)
	for i := 0; i < slice.numWriters; i++ {
		slice.main[i] = slice.mainstore.NewWriter()
	}

	if !slice.isPrimary {
		cfg.MaxPageItems = 300
		cfg.MaxDeltaChainLen = 30
		if slice.hasPersistence {
			cfg.File = filepath.Join(slice.path, "docIndex")
		}

		slice.backstore, err = plasma.New(cfg)
		if err != nil {
			return fmt.Errorf("Unable to initialize %s, err = %v", cfg.File, err)
		}
		slice.back = make([]*plasma.Writer, slice.numWriters)
		for i := 0; i < slice.numWriters; i++ {
			slice.back[i] = slice.backstore.NewWriter()
		}
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
			go tryCloseplasmaSlice(mdb)
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
	platform.AddInt64(&mdb.qCount, 1)
	mdb.cmdCh[int(meta.vbucket)%mdb.numWriters] <- mut
	mdb.idxStats.numDocsFlushQueued.Add(1)
	return mdb.fatalDbErr
}

func (mdb *plasmaSlice) Delete(docid []byte, meta *MutationMeta) error {
	mdb.idxStats.numDocsFlushQueued.Add(1)
	platform.AddInt64(&mdb.qCount, 1)
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
			platform.AddInt64(&mdb.qCount, -1)

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

	_, err = mdb.main[workerId].LookupKV(entry)
	if err == plasma.ErrItemNotFound {
		t0 := time.Now()
		mdb.main[workerId].InsertKV(entry, nil)
		mdb.idxStats.Timings.stKVSet.Put(time.Now().Sub(t0))
		platform.AddInt64(&mdb.insert_bytes, int64(len(entry)))
		mdb.isDirty = true
		return 1
	}

	return 0
}

func (mdb *plasmaSlice) insertSecIndex(key []byte, docid []byte, workerId int) int {
	t0 := time.Now()

	ndel := mdb.deleteSecIndex(docid, workerId)
	entry, err := NewSecondaryIndexEntry(key, docid, mdb.idxDefn.IsArrayIndex,
		1, mdb.encodeBuf[workerId])
	if err != nil {
		logging.Errorf("plasmaSlice::insertSecIndex Slice Id %v IndexInstId %v "+
			"Skipping docid:%s (%v)", mdb.Id, mdb.idxInstId, docid, err)
		return ndel
	}

	if len(key) > 0 {
		mdb.main[workerId].InsertKV(entry, nil)
		backEntry := entry2BackEntry(entry)
		mdb.back[workerId].InsertKV(docid, backEntry)

		mdb.idxStats.Timings.stKVSet.Put(time.Now().Sub(t0))
		platform.AddInt64(&mdb.insert_bytes, int64(len(docid)+len(entry)))
	}

	mdb.isDirty = true
	return 1
}

func (mdb *plasmaSlice) insertSecArrayIndex(keys []byte, docid []byte, workerId int) int {
	return 0
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
	mdb.main[workerId].DeleteKV(itm)
	mdb.idxStats.Timings.stKVDelete.Put(time.Now().Sub(t0))
	platform.AddInt64(&mdb.delete_bytes, int64(len(entry.Bytes())))
	mdb.isDirty = true
	return 1

}

func (mdb *plasmaSlice) deleteSecIndex(docid []byte, workerId int) int {
	buf := mdb.encodeBuf[workerId]

	// Delete entry from back and main index if present
	backEntry, err := mdb.back[workerId].LookupKV(docid)
	if err != plasma.ErrItemNotFound {
		t0 := time.Now()
		platform.AddInt64(&mdb.delete_bytes, int64(len(docid)))
		mdb.back[workerId].DeleteKV(docid)
		entry := backEntry2entry(docid, backEntry, buf)
		mdb.main[workerId].DeleteKV(entry)
		mdb.idxStats.Timings.stKVDelete.Put(time.Since(t0))
	}

	mdb.isDirty = true
	return 1
}

func (mdb *plasmaSlice) deleteSecArrayIndex(docid []byte, workerId int) (nmut int) {
	return 0
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
		s.MainSnap.Open()
		if !mdb.isPrimary {
			s.BackSnap.Open()
		}

		go mdb.doPersistSnapshot(s)
	}

	logging.Infof("plasmaSlice::OpenSnapshot SliceId %v IndexInstId %v Creating New "+
		"Snapshot %v", mdb.id, mdb.idxInstId, snapInfo)

	return s, nil
}

func (mdb *plasmaSlice) doPersistSnapshot(s *plasmaSnapshot) {
	var wg sync.WaitGroup

	if platform.CompareAndSwapInt32(&mdb.isPersistorActive, 0, 1) {
		defer platform.StoreInt32(&mdb.isPersistorActive, 0)

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
	} else {
		logging.Infof("PlasmaSlice Slice Id %v, IndexInstId %v Skipping ondisk"+
			" snapshot. A snapshot writer is in progress.", mdb.id, mdb.idxInstId)
		s.MainSnap.Close()
		s.BackSnap.Close()
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
			mRP: mRPs[i],
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
	/*
		prev := platform.LoadUint64(&mdb.committedCount)
		curr := mdb.mainstore.ItemsCount()
		platform.AddInt64(&totalplasmaItems, int64(curr)-int64(prev))
		platform.StoreUint64(&mdb.committedCount, uint64(curr))
	*/
}

func (mdb *plasmaSlice) GetCommittedCount() uint64 {
	return platform.LoadUint64(&mdb.committedCount)
}

func (mdb *plasmaSlice) resetStores() {
	// This is blocking call if snap refcounts != 0
	go mdb.mainstore.Close()
	if !mdb.isPrimary {
		mdb.backstore.Close()
	}

	os.RemoveAll(mdb.path)
	mdb.newBorn = true
	mdb.initStores()
}

func (mdb *plasmaSlice) Rollback(o SnapshotInfo) error {
	mdb.waitPersist()
	qc := platform.LoadInt64(&mdb.qCount)
	if qc > 0 {
		common.CrashOnError(errors.New("Slice Invariant Violation - rollback with pending mutations"))
	}

	return mdb.restore(o)
}

func (mdb *plasmaSlice) restore(o SnapshotInfo) error {
	info := o.(*plasmaSnapshotInfo)
	if s, err := mdb.mainstore.Rollback(info.mRP); err != nil {
		s.Close()
		return err
	}

	if !mdb.isPrimary {
		s, err := mdb.backstore.Rollback(info.bRP)
		s.Close()
		return err
	}

	return nil
}

//RollbackToZero rollbacks the slice to initial state. Return error if
//not possible
func (mdb *plasmaSlice) RollbackToZero() error {
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

	qc := platform.LoadInt64(&mdb.qCount)
	if qc > 0 {
		common.CrashOnError(errors.New("Slice Invariant Violation - commit with pending mutations"))
	}

	mdb.isDirty = false

	newSnapshotInfo := &plasmaSnapshotInfo{
		Ts:        ts,
		Committed: commit,
	}
	mdb.setCommittedCount()

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
		go tryCloseplasmaSlice(mdb)
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

	internalData = append(internalData, fmt.Sprintf("----MainStore----\n%s", mdb.mainstore.GetStats()))
	if !mdb.isPrimary {
		internalData = append(internalData, fmt.Sprintf("\n----BackStore----\n%s", mdb.backstore.GetStats()))
	}

	sts.InternalData = internalData
	return sts, nil
}

func (mdb *plasmaSlice) UpdateConfig(cfg common.Config) {
	mdb.confLock.Lock()
	defer mdb.confLock.Unlock()

	mdb.sysconf = cfg
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
	mdb.mainstore.Close()

	if !mdb.isPrimary {
		mdb.backstore.Close()
	}
}

func (mdb *plasmaSlice) getCmdsCount() int {
	qc := platform.LoadInt64(&mdb.qCount)
	return int(qc)
}

func (mdb *plasmaSlice) logWriterStat() {
	count := platform.AddUint64(&mdb.flushedCount, 1)
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
	return fmt.Sprintf("SnapshotInfo: count:%v committed:%v", 0, info.Committed)
}

func (s *plasmaSnapshot) Create() error {
	return nil
}

func (s *plasmaSnapshot) Open() error {
	platform.AddInt32(&s.refCount, int32(1))

	return nil
}

func (s *plasmaSnapshot) IsOpen() bool {

	count := platform.LoadInt32(&s.refCount)
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

	count := platform.AddInt32(&s.refCount, int32(-1))

	if count < 0 {
		logging.Errorf("plasmaSnapshot::Close Close operation requested " +
			"on already closed snapshot")
		return errors.New("Snapshot Already Closed")

	} else if count == 0 {
		s.Destroy()
	}

	return nil
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

func (s *plasmaSnapshot) CountTotal(stopch StopChannel) (uint64, error) {
	return s.CountRange(MinIndexKey, MaxIndexKey, Both, stopch)
}

func (s *plasmaSnapshot) CountRange(low, high IndexKey, inclusion Inclusion,
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

func (s *plasmaSnapshot) CountLookup(keys []IndexKey, stopch StopChannel) (uint64, error) {
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

func (s *plasmaSnapshot) Exists(key IndexKey, stopch StopChannel) (bool, error) {
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

func (s *plasmaSnapshot) Lookup(key IndexKey, callb EntryCallback) error {
	return s.Iterate(key, key, Both, compareExact, callb)
}

func (s *plasmaSnapshot) Range(low, high IndexKey, inclusion Inclusion,
	callb EntryCallback) error {

	var cmpFn CmpEntry
	if s.isPrimary() {
		cmpFn = compareExact
	} else {
		cmpFn = comparePrefix
	}

	return s.Iterate(low, high, inclusion, cmpFn, callb)
}

func (s *plasmaSnapshot) All(callb EntryCallback) error {
	return s.Range(MinIndexKey, MaxIndexKey, Both, callb)
}

func (s *plasmaSnapshot) Iterate(low, high IndexKey, inclusion Inclusion,
	cmpFn CmpEntry, callback EntryCallback) error {
	var entry IndexEntry
	var err error
	t0 := time.Now()
	it := s.MainSnap.NewIterator()
	defer it.Close()

	if low.Bytes() == nil {
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

		t0 := time.Now()
		it.Next()
		s.slice.idxStats.Timings.stIteratorNext.Put(time.Since(t0))
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
	count := int(binary.LittleEndian.Uint16(buf[l-2 : l]))
	entry, _ := NewSecondaryIndexEntry(bentry[:l-2], docid, false, count, buf[:0])
	return entry.Bytes()
}
