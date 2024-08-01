//go:build !community
// +build !community

package indexer

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"github.com/couchbase/bhive"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/iowrap"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/vector"
	"github.com/couchbase/indexing/secondary/vector/codebook"
	"github.com/couchbase/plasma"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

////////////////////////////////////////////////
// Type Declaration
////////////////////////////////////////////////

const centroidIdLen int = 8

type bhiveSlice struct {
	//
	// metadata
	id         SliceId
	idxDefn    common.IndexDefn
	idxDefnId  common.IndexDefnId
	idxInstId  common.IndexInstId
	idxPartnId common.PartitionId
	replicaId  int

	//
	// location
	path         string
	storageDir   string
	logDir       string
	codebookPath string

	//
	// lock
	lock sync.Mutex

	//
	// status
	newBorn        bool
	isDirty        bool
	isInitialBuild int32
	isActive       bool
	status         SliceStatus
	isClosed       bool
	isDeleted      bool
	isSoftClosed   bool
	isSoftDeleted  bool
	refCount       uint64

	//
	// main store
	mainstore   *bhive.Bhive
	mainWriters []*bhive.Writer

	//
	// back store
	backstore   *bhive.Bhive
	backWriters []*bhive.Writer

	//
	// config / settings
	sysconf            common.Config // system configuration settings
	confLock           sync.RWMutex  // protects sysconf
	numWriters         int           // number of writers
	maxNumWriters      int
	numPartitions      int
	indexerMemoryQuota int64 // indexer mem quota
	numVbuckets        int
	clusterAddr        string
	maxRollbacks       int
	maxDiskSnaps       int

	//
	// rebalance
	shardIds []plasma.Shard

	//
	// stats
	idxStats        *IndexStats // indexer stats
	qCount          int64       // number of mutations remaining in worker queue
	flushActive     uint32      // flag to tell if there has been any mutation since last snapshot
	get_bytes       int64
	insert_bytes    int64
	delete_bytes    int64
	flushedCount    uint64
	totalFlushTime  time.Duration
	totalCommitTime time.Duration
	numKeysSkipped  int32 // //track number of keys skipped due to errors
	committedCount  uint64

	//
	// mutation processing
	cmdCh     []chan *indexMutation
	stopCh    []DoneChannel
	cmdStopCh DoneChannel

	// buffer
	quantizedCodeBuf [][]byte // For vector index, used for quantized code computation of vectors

	//
	// vector index related metadata
	nlist     int               // number of centroids to use for training
	codebook  codebook.Codebook // cookbook
	codeSize  int               // Size of the quantized codes after training
	vectorPos int               // Position of the vector field in the encoded key. Derived from index defn

	// snapshot
	persistorLock     sync.RWMutex
	isPersistorActive bool
	stopPersistor     bool
	persistorQueue    *bhiveSnapshot
	snapCount         uint64

	// error
	fatalDbErr error // TODO
}

type bhiveSnapshotInfo struct {
	Ts        *common.TsVbuuid
	Committed bool
	Count     int64

	// TBD
	//mRP, bRP *bhive.RecoveryPoint

	IndexStats map[string]interface{}
	Version    int
	InstId     common.IndexInstId
	PartnId    common.PartitionId
}

type bhiveSnapshot struct {
	id         uint64
	slice      *bhiveSlice
	idxDefnId  common.IndexDefnId
	idxInstId  common.IndexInstId
	idxPartnId common.PartitionId
	ts         *common.TsVbuuid
	info       *bhiveSnapshotInfo

	MainSnap bhive.Snapshot
	BackSnap bhive.Snapshot

	committed bool

	refCount int32
}

////////////////////////////////////////////////
// Bhive
////////////////////////////////////////////////

func NewBhiveSlice(storage_dir string, log_dir string, path string, sliceId SliceId, idxDefn common.IndexDefn,
	idxInstId common.IndexInstId, partitionId common.PartitionId,
	numPartitions int,
	sysconf common.Config, idxStats *IndexStats, memQuota int64,
	isNew bool, isInitialBuild bool,
	numVBuckets int, replicaId int, shardIds []common.ShardId,
	cancelCh chan bool, codebookPath string) (*bhiveSlice, error) {

	if !idxDefn.IsVectorIndex {
		return nil, fmt.Errorf("index %v.%v is not a vector index", idxDefn.Bucket, idxDefn.Name)
	}

	slice := &bhiveSlice{}

	// create directory
	err := createBhiveSliceDir(storage_dir, path, isNew)
	if err != nil {
		return nil, err
	}
	slice.newBorn = isNew

	// initialize metadata
	slice.id = sliceId
	slice.idxDefn = idxDefn
	slice.idxInstId = idxInstId
	slice.idxDefnId = idxDefn.DefnId
	slice.idxPartnId = partitionId
	slice.replicaId = replicaId

	// path
	slice.path = path
	slice.storageDir = storage_dir
	slice.logDir = log_dir
	slice.codebookPath = codebookPath

	// settings
	slice.sysconf = sysconf
	slice.numPartitions = numPartitions
	slice.clusterAddr = sysconf["clusterAddr"].String()
	slice.numVbuckets = numVBuckets
	slice.maxRollbacks = sysconf["settings.plasma.recovery.max_rollbacks"].Int()
	slice.maxDiskSnaps = sysconf["recovery.max_disksnaps"].Int()
	slice.maxNumWriters = sysconf["numSliceWriters"].Int()
	//numReaders := sysconf["plasma.numReaders"].Int()

	// stats
	slice.idxStats = idxStats
	slice.indexerMemoryQuota = memQuota

	// initialize main and back stores
	if err := slice.initStores(isInitialBuild, cancelCh); err != nil {
		// Index is unusable. Remove the data files and reinit
		if err == errStorageCorrupted || err == errStoragePathNotFound {
			logging.Errorf("bhiveSlice:NewBhiveSlice Id %v IndexInstId %v PartitionId %v isNew %v"+
				"fatal error occured: %v", sliceId, idxInstId, partitionId, isNew, err)
		}
		if isNew {
			// TODO
			// destroyBhiveSlice(storage_dir, path)
		}
		return nil, err
	}

	if isInitialBuild {
		atomic.StoreInt32(&slice.isInitialBuild, 1)
	}

	// intiialize and start the writers
	slice.setupWriters()

	logging.Infof("bhiveSlice:NewBhiveSlice Created New Slice Id %v IndexInstId %v partitionId %v "+
		"WriterThreads %v", sliceId, idxInstId, partitionId, slice.numWriters)

	// setup codebook
	if !isNew && slice.idxDefn.IsVectorIndex {
		codebookRecoveryStartTm := time.Now()
		err = slice.recoverCodebook(slice.codebookPath)
		if err != nil {
			logging.Errorf("bhieSlice::recoverCodebook SliceId: %v IndexInstId: %v PartitionId %v Codebook "+
				"recovery finished with err %v", slice.id, slice.idxInstId, slice.idxPartnId, err)
			return slice, err
		} else {
			logging.Infof("bhiveSlice::recoverCodebook SliceId: %v IndexInstId: %v PartitionId %v Codebook "+
				"recovery finished successfully. Elapsed: %v", slice.id, slice.idxInstId, slice.idxPartnId,
				time.Since(codebookRecoveryStartTm))
		}
	}

	return slice, nil
}

func createBhiveSliceDir(storageDir string, path string, isNew bool) error {

	_, err := iowrap.Os_Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			iowrap.Os_Mkdir(path, 0777)
			return nil
		}
	} else if isNew {
		// TODO
		/*
			// if we expect a new instance but there is residual file, destroy old data.
			err = bhive.DestroyInstance(storageDir, path)
		*/
	}

	return err
}

////////////////////////////////////////////////
// initialization
////////////////////////////////////////////////

func (slice *bhiveSlice) setupMainstoreConfig() bhive.Config {
	var i uint64 // centroid is uin64

	cfg := *bhive.MakeDefaultConfig()
	cfg.File = filepath.Join(slice.path, "mainIndex")
	cfg.StorageDir = slice.storageDir
	cfg.Group = bhive.InstanceGroup(MAIN_INDEX)

	cfg.EnableKeyPrefixMode = true
	cfg.EnableUpdateStatusForSet = false

	cfg.CentroidIDSize = int(reflect.TypeOf(i).Size())
	cfg.KeyPrefixSize = uint64(cfg.CentroidIDSize)
	cfg.NumKVStore = 10    // FIXME - from setting
	cfg.MaxBatchSize = 256 // FIXME - from setting

	cfg.NumWriters = slice.maxNumWriters
	return cfg
}

func (slice *bhiveSlice) setupBackstoreConfig() bhive.Config {
	cfg := *bhive.MakeDefaultConfig()
	cfg.File = filepath.Join(slice.path, "docIndex")
	cfg.StorageDir = slice.storageDir
	cfg.Group = bhive.InstanceGroup(BACK_INDEX)

	cfg.EnableKeyPrefixMode = false
	cfg.EnableUpdateStatusForSet = false

	cfg.CentroidIDSize = 0
	//cfg.KeyPrefixSize = 0
	cfg.NumKVStore = 10    // FIXME - from setting
	cfg.MaxBatchSize = 256 // FIXME - from setting

	cfg.NumWriters = slice.maxNumWriters
	return cfg
}

func (slice *bhiveSlice) initStores(isInitialBuild bool, cancelCh chan bool) error {
	var err error

	mCfg := slice.setupMainstoreConfig()
	bCfg := slice.setupBackstoreConfig()

	mCfg.File = filepath.Join(slice.path, "mainIndex")
	bCfg.File = filepath.Join(slice.path, "docIndex")

	var wg sync.WaitGroup
	var mErr, bErr error

	// Recover mainindex
	wg.Add(1)
	go func() {
		defer wg.Done()

		var alternateShardId string
		if len(slice.idxDefn.AlternateShardIds) > 0 && len(slice.idxDefn.AlternateShardIds[slice.idxPartnId]) > 0 {
			alternateShardId = slice.idxDefn.AlternateShardIds[slice.idxPartnId][MAIN_INDEX-1] // "-1" because MAIN_INDEX is "1" and back-index is "2"
		}

		slice.mainstore, mErr = bhive.New(
			alternateShardId, // AlternateId
			mCfg,             // config bhive.Config
			slice.newBorn,    // new bool
		)
		if mErr != nil {
			mErr = fmt.Errorf("unable to initialize %s, err = %v", mCfg.File, mErr)
			return
		}
	}()

	// Recover backindex
	wg.Add(1)
	go func() {
		defer wg.Done()

		var alternateShardId string
		if len(slice.idxDefn.AlternateShardIds) > 0 && len(slice.idxDefn.AlternateShardIds[slice.idxPartnId]) > 0 {
			alternateShardId = slice.idxDefn.AlternateShardIds[slice.idxPartnId][BACK_INDEX-1] // "-1" because MAIN_INDEX is "1" and back-index is "2"
		}

		slice.backstore, mErr = bhive.New(
			alternateShardId, // AlternateId
			bCfg,             // config bhive.Config
			slice.newBorn,    // new bool
		)
		if bErr != nil {
			bErr = fmt.Errorf("Unable to initialize %s, err = %v", bCfg.File, bErr)
			return
		}
	}()

	wg.Wait()

	handleError := func() error {
		// In case of errors, close the opened stores
		if mErr != nil {
			if bErr == nil {
				slice.backstore.Close()
			}
		} else if bErr != nil {
			if mErr == nil {
				slice.mainstore.Close()
			}
		}

		// Return fatal error with higher priority.
		if mErr != nil && bhive.IsFatalError(mErr) {
			logging.Errorf("bhiveSlice:NewBhiveSlice Id %v IndexInstId %v "+
				"fatal error occured: %v", slice.id, slice.idxInstId, mErr)

			if !slice.newBorn && bhive.IsErrorRecoveryInstPathNotFound(mErr) {
				return errStoragePathNotFound
			}
			return errStorageCorrupted
		}

		if bErr != nil && plasma.IsFatalError(bErr) {
			logging.Errorf("bhiveSlice:NewBhiveSlice Id %v IndexInstId %v "+
				"fatal error occured: %v", slice.id, slice.idxInstId, bErr)

			if !slice.newBorn && plasma.IsErrorRecoveryInstPathNotFound(bErr) {
				return errStoragePathNotFound
			}
			return errStorageCorrupted
		}

		// TODO
		/*
			if (mErr != nil && bhive.IsRecoveryCancelError(mErr)) ||
				(bErr != nil && bhive.IsRecoveryCancelError(bErr)) {
				logging.Warnf("bhiveSlice:NewBhiveSlice recovery cancelled for inst %v", slice.idxInstId)
				return errRecoveryCancelled
			}
		*/

		// If both mErr and bErr are not fatal, return mErr with higher priority
		if mErr != nil {
			return mErr
		}

		if bErr != nil {
			return bErr
		}

		return nil
	}
	if err := handleError(); err != nil {
		return err
	}

	return err
}

////////////////////////////////////////////////
// Setter/Getter
////////////////////////////////////////////////

func (mdb *bhiveSlice) Id() SliceId {
	return mdb.id
}

func (mdb *bhiveSlice) IndexInstId() common.IndexInstId {
	return mdb.idxInstId
}

func (mdb *bhiveSlice) SetActive(active bool) {
	mdb.isActive = active
}

func (mdb *bhiveSlice) SetStatus(status SliceStatus) {
	mdb.status = status
}

func (mdb *bhiveSlice) Path() string {
	return mdb.path
}

func (mdb *bhiveSlice) Status() SliceStatus {
	return mdb.Status()
}

func (mdb *bhiveSlice) IndexPartnId() common.PartitionId {
	return mdb.idxPartnId
}

func (mdb *bhiveSlice) IndexDefnId() common.IndexDefnId {
	return mdb.idxDefnId
}

func (mdb *bhiveSlice) IsActive() bool {
	return mdb.isActive
}

func (mdb *bhiveSlice) IsDirty() bool {
	return mdb.isDirty
}

func (mdb *bhiveSlice) IsCleanupDone() bool {
	mdb.lock.Lock()
	defer mdb.lock.Unlock()

	return mdb.isClosed && mdb.isDeleted
}

////////////////////////////////////////////////
// Settings
////////////////////////////////////////////////

func (slice *bhiveSlice) UpdateConfig(common.Config) {

}

////////////////////////////////////////////////
// Flusher API
////////////////////////////////////////////////

func (mdb *bhiveSlice) Insert(key []byte, docid []byte, vectors [][]float32, centroidPos []int32, meta *MutationMeta) error {
	if mdb.CheckCmdChStopped() {
		return mdb.fatalDbErr
	}

	op := opUpdate
	if meta.firstSnap {
		op = opInsert
	}

	mut := &indexMutation{
		op:          op,
		key:         key,
		docid:       docid,
		vecs:        vectors,
		centroidPos: centroidPos,
		meta:        meta,
	}

	atomic.AddInt64(&mdb.qCount, 1)
	atomic.StoreUint32(&mdb.flushActive, 1)
	workerId := int(meta.vbucket) % mdb.numWriters

	select {
	case mdb.cmdCh[workerId] <- mut:
		break
	case <-mdb.cmdStopCh:
		atomic.AddInt64(&mdb.qCount, -1)
		return mdb.fatalDbErr
	}

	mdb.idxStats.numDocsFlushQueued.Add(1)
	return mdb.fatalDbErr
}

func (mdb *bhiveSlice) Delete(docid []byte, meta *MutationMeta) error {
	if !meta.firstSnap {
		if mdb.CheckCmdChStopped() {
			return mdb.fatalDbErr
		}

		atomic.AddInt64(&mdb.qCount, 1)
		mdb.idxStats.numDocsFlushQueued.Add(1)
		atomic.StoreUint32(&mdb.flushActive, 1)
		workerId := int(meta.vbucket) % mdb.numWriters

		select {
		case mdb.cmdCh[workerId] <- &indexMutation{op: opDelete, docid: docid}:
			break
		case <-mdb.cmdStopCh:
			atomic.AddInt64(&mdb.qCount, -1)
			return mdb.fatalDbErr
		}

	}
	return mdb.fatalDbErr
}

////////////////////////////////////////////////
// Setup Writers
////////////////////////////////////////////////

// Default number of num writers
func (slice *bhiveSlice) numWritersPerPartition() int {
	return int(math.Ceil(float64(slice.maxNumWriters) / float64(slice.numPartitions)))
}

// Get command handler queue size
func (slice *bhiveSlice) defaultCmdQueueSize() uint64 {

	slice.confLock.RLock()
	sliceBufSize := slice.sysconf["settings.sliceBufSize"].Uint64()
	slice.confLock.RUnlock()

	//use lower config for small memory quota
	var scaleDownFactor uint64
	if slice.indexerMemoryQuota <= 4*1024*1024*1024 {
		scaleDownFactor = 8
	} else if slice.indexerMemoryQuota <= 8*1024*1024*1024 {
		scaleDownFactor = 4
	} else if slice.indexerMemoryQuota <= 16*1024*1024*1024 {
		scaleDownFactor = 2
	} else {
		scaleDownFactor = 1
	}

	sliceBufSize = sliceBufSize / scaleDownFactor

	numWriters := slice.numWritersPerPartition()

	if sliceBufSize < uint64(numWriters) {
		sliceBufSize = uint64(numWriters)
	}

	return sliceBufSize / uint64(numWriters)
}

// Allocate array for writers
func (slice *bhiveSlice) setupWriters() {

	// initialize buffer
	slice.quantizedCodeBuf = make([][]byte, 0, slice.maxNumWriters)

	// initialize comand handler
	slice.cmdCh = make([]chan *indexMutation, 0, slice.maxNumWriters)
	slice.stopCh = make([]DoneChannel, 0, slice.maxNumWriters)
	slice.cmdStopCh = make(DoneChannel)

	// initialize writers
	slice.mainWriters = make([]*bhive.Writer, 0, slice.maxNumWriters)
	slice.backWriters = make([]*bhive.Writer, 0, slice.maxNumWriters)

	// start writers
	numWriter := slice.numWritersPerPartition()
	slice.startWriters(numWriter)
}

// Initialize any field related to numWriters
func (slice *bhiveSlice) initWriters(numWriters int) {

	curNumWriters := len(slice.cmdCh)

	// initialize buffer
	slice.quantizedCodeBuf = slice.quantizedCodeBuf[:numWriters]
	for i := curNumWriters; i < numWriters; i++ {
		if slice.idxDefn.IsVectorIndex {
			slice.quantizedCodeBuf[i] = make([]byte, 0) // After training is completed, the codebuf will be resized
		}
	}

	// initialize command handler
	queueSize := slice.defaultCmdQueueSize()
	slice.cmdCh = slice.cmdCh[:numWriters]
	slice.stopCh = slice.stopCh[:numWriters]
	for i := curNumWriters; i < numWriters; i++ {
		slice.cmdCh[i] = make(chan *indexMutation, queueSize)
		slice.stopCh[i] = make(DoneChannel)

		go slice.handleCommandsWorker(i)
	}

	// initialize mainsotre workers
	slice.mainWriters = slice.mainWriters[:numWriters]
	for i := curNumWriters; i < numWriters; i++ {
		slice.mainWriters[i] = slice.mainstore.NewWriter()
	}

	// initialize backstore writers
	slice.backWriters = slice.backWriters[:numWriters]
	for i := curNumWriters; i < numWriters; i++ {
		slice.backWriters[i] = slice.backstore.NewWriter()
	}
}

// Start the writers by passing in the desired number of writers
func (slice *bhiveSlice) startWriters(numWriters int) {

	// If slice already have more writers that the desired number, return.
	if slice.numWriters >= numWriters {
		return
	}

	// If desired number is more than length of the slice, then need to resize.
	if numWriters > len(slice.cmdCh) {
		slice.stopWriters(0)
		slice.initWriters(numWriters)
	}

	// update the number of slice writers
	slice.numWriters = numWriters
}

// Stop the writers by passing in the desired number of writers
func (slice *bhiveSlice) stopWriters(numWriters int) {

	// If slice already have fewer writers that the desired number, return.
	if numWriters >= slice.numWriters {
		return
	}

	// update the number of slice writers
	slice.numWriters = numWriters
}

////////////////////////////////////////////////
// Command Processing
////////////////////////////////////////////////

func (mdb *bhiveSlice) handleCommandsWorker(workerId int) {
	var start time.Time
	var elapsed time.Duration
	var icmd *indexMutation

	defer func() {
		if r := recover(); r != nil {
			logging.Fatalf("bhiveSlice::handleCommandsWorker: panic detected while processing mutation for "+
				"operation %v key = %s docid = %s Index %v, Bucket %v, IndexInstId %v, "+
				"PartitionId %v", icmd.op, logging.TagStrUD(icmd.key), logging.TagStrUD(icmd.docid),
				mdb.idxDefn.Name, mdb.idxDefn.Bucket, mdb.idxInstId, mdb.idxPartnId)
			logging.Fatalf("%s", logging.StackTraceAll())
			panic(r)
		}
	}()

loop:
	for {
		var nmut int
		select {
		case icmd = <-mdb.cmdCh[workerId]:

			switch icmd.op {
			case opUpdate, opInsert:
				start = time.Now()
				nmut = mdb.insert(icmd.key, icmd.docid, workerId, icmd.op == opInsert, icmd.vecs)
				elapsed = time.Since(start)
				mdb.totalFlushTime += elapsed

			case opDelete:
				start = time.Now()
				nmut = mdb.delete(icmd.docid, workerId)
				elapsed = time.Since(start)
				mdb.totalFlushTime += elapsed

			default:
				logging.Errorf("plasmaSlice::handleCommandsWorker \n\tSliceId %v IndexInstId %v PartitionId %v Received "+
					"Unknown Command %v", mdb.id, mdb.idxInstId, mdb.idxPartnId, logging.TagUD(icmd))
			}

			mdb.idxStats.numItemsFlushed.Add(int64(nmut))
			mdb.idxStats.numDocsIndexed.Add(1)

		case _, ok := <-mdb.stopCh[workerId]:
			if ok {
				mdb.stopCh[workerId] <- true
			}
			break loop

		}
	}
}

func (mdb *bhiveSlice) CheckCmdChStopped() bool {
	select {
	case <-mdb.cmdStopCh:
		return true
	default:
		return false
	}
}

// slice insert/delete methods are async. There
// can be outstanding mutations in internal queue to flush even
// after insert/delete have return success to caller.
// This method provides a mechanism to wait till internal
// queue is empty.
func (mdb *bhiveSlice) waitPersist() {
	start := time.Now()
	logging.Tracef("bhiveSlice::waitPersist waiting for cmdCh to be empty Id %v, IndexInstId %v, PartitionId %v, "+
		"IndexDefnId %v", mdb.id, mdb.idxInstId, mdb.idxPartnId, mdb.idxDefnId)
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
	logging.Tracef("bhiveSlice::waitPersist waited %v for cmdCh to be empty Id %v, IndexInstId %v, PartitionId %v, "+
		"IndexDefnId %v", time.Since(start), mdb.id, mdb.idxInstId, mdb.idxPartnId, mdb.idxDefnId)
}

// checkAllWorkersDone return true if all workers have
// finished processing
func (mdb *bhiveSlice) checkAllWorkersDone() bool {

	//if there are mutations in the cmdCh, workers are
	//not yet done
	if mdb.getCmdsCount() > 0 {
		return false
	}

	return true
}

func (mdb *bhiveSlice) getCmdsCount() int {
	qc := atomic.LoadInt64(&mdb.qCount)
	return int(qc)
}

////////////////////////////////////////////////
// Mutation
////////////////////////////////////////////////

func (mdb *bhiveSlice) insert(key []byte, docid []byte, workerId int, init bool, vecs [][]float32) int {

	defer func() {
		atomic.AddInt64(&mdb.qCount, -1)
	}()

	var nmut int

	if len(key) == 0 {
		nmut = mdb.delete2(docid, workerId)
	} else {
		// Primary vector indexes do not exist
		if mdb.idxDefn.IsArrayIndex {
			// [VECTOR_TODO]: Add support for array indexes with VECTOR attribute
		} else {
			nmut = mdb.insertVectorIndex(key, docid, workerId, init, vecs)
		}
	}

	mdb.logWriterStat()
	return nmut
}

func (mdb *bhiveSlice) delete(docid []byte, workerId int) int {
	defer func() {
		atomic.AddInt64(&mdb.qCount, -1)
	}()

	return mdb.delete2(docid, workerId)
}

func (mdb *bhiveSlice) delete2(docid []byte, workerId int) int {

	var nmut int

	if !mdb.idxDefn.IsArrayIndex {
		nmut, _ = mdb.deleteVectorIndex(docid, nil, nil, workerId)
	} else {
		// [VECTOR_TODO]: Add support for array vector index
	}

	mdb.logWriterStat()
	return nmut
}

//  1. main
//     a) key: CentroidID+docID
//     b) meta: quantizedVector+Scalar
//     c) value: full vector
//  2. back:
//     a) key: docID
//     b) value: SHA(vector)+SHA(scalar)+centroidID
func (mdb *bhiveSlice) insertVectorIndex(key []byte, docid []byte, workerId int,
	init bool, vecs [][]float32) (nmut int) {

	start := time.Now()
	defer func() {
		mdb.idxStats.Timings.stKVSet.Put(time.Now().Sub(start))
	}()

	if len(vecs) != 1 {
		err := fmt.Errorf("Fatal - Expected only one vector for docId: %s, instance: %v. "+
			"Found: %v vectors in the documents", logging.TagUD(docid), mdb.idxInstId, len(vecs))
		logging.Fatalf("bhiveSlice::insertVectorIndex %v", err)
		atomic.AddInt32(&mdb.numKeysSkipped, 1)
		panic(err) // [VECTOR_TODO]: Having panics will help catch bugs. Remove panics after code stabilizes
	}

	if mdb.codebook.IsTrained() == false || mdb.codeSize <= 0 {
		err := fmt.Errorf("Fatal - Mutation processing should not happen on an untrained codebook. "+
			"docId: %s, Index instId: %v", logging.TagUD(docid), mdb.IndexInstId())
		logging.Fatalf("bhiveSlice::insertVectorIndex %v", err)
		atomic.AddInt32(&mdb.numKeysSkipped, 1)
		panic(err) // [VECTOR_TODO]: Having panics will help catch bugs. Remove panics after code stabilizes
	}

	vec := vecs[0]
	if vec != nil {

		// remove old record
		if !init {
			if _, changed := mdb.deleteVectorIndex(docid, vec, key, workerId); !changed {
				return 0
			}
		}

		// Compute centroidId for the vector
		centroidId, err := mdb.getNearestCentroidId(vec)
		if err != nil {
			err := fmt.Errorf("Fatal - Error observed while retrieving centroidId for vector. "+
				"docId: %s, Index instId: %v, err: %v", logging.TagUD(docid), mdb.IndexInstId(), err)
			logging.Fatalf("bhiveSlice::insertVectorIndex %v", err)
			atomic.AddInt32(&mdb.numKeysSkipped, 1)
			panic(err) // [VECTOR_TODO]: Having panics will help catch bugs. Remove panics after code stabilizes
		}

		// Compute quantized code
		quantizedCode, err := mdb.getQuantizedCodeForVector(vec, mdb.codeSize, mdb.quantizedCodeBuf[workerId])
		if err != nil {
			logging.Errorf("bhiveSlice::insertVectorIndex Slice Id %v IndexInstId %v PartitionId %v "+
				"Skipping docid:%s (%v)", mdb.Id, mdb.idxInstId, mdb.idxPartnId, logging.TagStrUD(docid), err)
			atomic.AddInt32(&mdb.numKeysSkipped, 1)
			panic(err) // [VECTOR_TODO]: Having panics will help catch bugs. Remove panics after code stabilizes
		}

		mdb.mainWriters[workerId].Begin()
		defer mdb.mainWriters[workerId].End()

		mdb.backWriters[workerId].Begin()
		defer mdb.backWriters[workerId].End()

		var cid [8]byte
		binary.LittleEndian.PutUint64(cid[:], uint64(centroidId))

		// insert into main index
		v := ((bhive.Vector)(vec)).Bytes()
		err = mdb.mainWriters[workerId].Insert(cid[:], docid, quantizedCode, v)
		if err != nil {
			logging.Errorf("bhiveSlice:insertVectorIndex.  Error during insert main index: msg=%v", err.Error())
		}
		mainIndexEntrySz := int64(len(docid) + 8 + len(key) + len(v))
		if err == nil {
			mdb.idxStats.rawDataSize.Add(mainIndexEntrySz)
			addKeySizeStat(mdb.idxStats, int(mainIndexEntrySz))
			atomic.AddInt64(&mdb.insert_bytes, mainIndexEntrySz)
		}

		// insert into back index
		backIndexEntrySz := sha256.Size + sha256.Size + 8 // sha(vector) + sha(scalar)+ centroidId
		var buffer [sha256.Size + sha256.Size + 8]byte
		buf := buffer[:]

		offset := 0
		copy(buf[offset:offset+sha256.Size], common.ComputeSHA256ForFloat32Array(vec))

		offset += sha256.Size
		copy(buf[offset:offset+sha256.Size], common.ComputeSHA256ForByteArray(key))

		offset += sha256.Size
		copy(buf[offset:], cid[:])

		err = mdb.backWriters[workerId].InsertLocal(docid, buf)
		if err != nil {
			logging.Errorf("bhiveSlice:insertVectorIndex.  Error during insert back index: msg=%v", err.Error())
		}
		if err == nil {
			// rawDataSize is the sum of all data inserted into main store and back store
			mdb.idxStats.backstoreRawDataSize.Add(int64(len(docid) + backIndexEntrySz))
			mdb.idxStats.rawDataSize.Add(int64(len(docid) + backIndexEntrySz))
		}
	}

	mdb.isDirty = true
	return 1
}

// back entry: SHA(vector)+SHA(scalar)+centroidID
func (mdb *bhiveSlice) deleteVectorIndex(docid []byte, vector []float32, fields []byte, workerId int) (nmut int, changed bool) {

	// Delete entry from back and main index if present
	mdb.backWriters[workerId].Begin()
	defer mdb.backWriters[workerId].End()

	itemFound := false
	backEntry, err := mdb.backWriters[workerId].GetLocal(docid)

	if err == nil && len(backEntry) > 0 {
		itemFound = true

		shaVec := common.ComputeSHA256ForFloat32Array(vector)
		shaField := common.ComputeSHA256ForByteArray(fields)

		backShaVec := backEntry[0:sha256.Size]
		backShaField := backEntry[sha256.Size : sha256.Size*2]
		centroidId := backEntry[sha256.Size*2:]

		// back entry has not changed
		if bytes.Equal(shaVec, backShaVec) && bytes.Equal(shaField, backShaField) {
			return 0, false
		}

		t0 := time.Now()
		atomic.AddInt64(&mdb.delete_bytes, int64(len(docid)))

		err = mdb.backWriters[workerId].DeleteLocal(docid)
		if err == nil {
			mdb.idxStats.backstoreRawDataSize.Add(0 - int64(len(docid)+len(backEntry)))
			mdb.idxStats.rawDataSize.Add(0 - int64(len(docid)+len(backEntry)))
		}

		mdb.mainWriters[workerId].Begin()
		defer mdb.mainWriters[workerId].End()

		err = mdb.mainWriters[workerId].Delete(centroidId, docid)
		mdb.idxStats.Timings.stKVDelete.Put(time.Since(t0))

		// TODO: Cannot update rawDataSize for main index
		/*
			if err == nil {
				mdb.idxStats.rawDataSize.Add(0 - int64(entrySz))
				subtractKeySizeStat(mdb.idxStats, entrySz)
			}
		*/
	}

	mdb.isDirty = true

	if itemFound {
		return 1, true
	} else {
		//nothing deleted
		return 0, true
	}
}

////////////////////////////////////////////////
// logWriterStat
////////////////////////////////////////////////

func (mdb *bhiveSlice) logWriterStat() {
	count := atomic.AddUint64(&mdb.flushedCount, 1)
	if (count%10000 == 0) || count == 1 {
		logging.Debugf("logWriterStat:: %v:%v "+
			"FlushedCount %v QueuedCount %v", mdb.idxInstId, mdb.idxPartnId,
			count, mdb.getCmdsCount())
	}
}

// //////////////////////////////////////////////////////////
// Vector
// //////////////////////////////////////////////////////////

func (mdb *bhiveSlice) SetNlist(nlist int) {
	mdb.nlist = nlist
}

func (mdb *bhiveSlice) InitCodebook() error {
	if mdb.idxDefn.IsVectorIndex == false {
		mdb.codebook = nil // Codebook is relevant only for vector indexes
		return nil
	}

	if mdb.idxDefn.VectorMeta == nil {
		return fmt.Errorf("empty vector metadata for vector index with defnId: %v, instId: %v", mdb.idxDefnId, mdb.idxInstId)
	}

	if mdb.nlist <= 0 {
		return fmt.Errorf("number of centroids required to train the index are not set for defnId: %v, instId: %v", mdb.idxDefnId, mdb.idxInstId)
	}

	codebook, err := vector.NewCodebook(mdb.idxDefn.VectorMeta, mdb.nlist)
	if err != nil {
		return err
	}

	logging.Infof("bhiveSlice: cookbook initialized with %v centroids", mdb.nlist)
	mdb.codebook = codebook
	return nil
}

// This function is to only handle the cases where training
// phase fails due to some error. Indexer will call this method
// to reset the codebook so that the next build command will
// create a new instance. Once the index moved to ACTIVE state,
// this method should not be called
func (mdb *bhiveSlice) ResetCodebook() error {

	// reset codebookSize
	mdb.codeSize = 0

	if mdb.codebook != nil {
		err := mdb.codebook.Close()
		mdb.codebook = nil
		if err != nil {
			logging.Errorf("plasmaSlice::ResetCodebook Error observed while closing codebook for instId: %v, partnId: %v",
				mdb.IndexInstId(), mdb.IndexPartnId())
			return err
		}
	}
	return nil
}

func (mdb *bhiveSlice) Train(vecs []float32) error {
	if mdb.codebook == nil {
		return ErrorCodebookNotInitialized
	}

	err := mdb.codebook.Train(vecs)
	if err != nil {
		return err
	}

	mdb.codeSize, err = mdb.codebook.CodeSize()
	if err != nil {
		mdb.codeSize = 0
		return err
	}

	mdb.initQuantizedCodeBuf()
	return nil
}

func (mdb *bhiveSlice) GetCodebook() (codebook.Codebook, error) {
	if mdb.codebook == nil {
		return nil, ErrorCodebookNotInitialized
	}

	if !mdb.codebook.IsTrained() {
		return nil, codebook.ErrCodebookNotTrained
	}

	return mdb.codebook, nil
}

func (mdb *bhiveSlice) IsTrained() bool {
	if mdb.codebook == nil {
		return false
	}

	return mdb.codebook.IsTrained()
}

func (mdb *bhiveSlice) initQuantizedCodeBuf() {
	numWriters := mdb.numWritersPerPartition()
	for i := 0; i < numWriters; i++ {
		mdb.quantizedCodeBuf[i] = resizeQuantizedCodeBuf(mdb.quantizedCodeBuf[i], 1, mdb.codeSize, true)
	}
}

func (mdb *bhiveSlice) SerializeCodebook() ([]byte, error) {

	if mdb.codebook == nil {
		return nil, fmt.Errorf("codebook is not initialized")
	}

	if mdb.codebook.IsTrained() == false {
		return nil, fmt.Errorf("codebook is not trained")
	}

	return mdb.codebook.Marshal()
}

func (mdb *bhiveSlice) getNearestCentroidId(vec []float32) (int64, error) {

	// For mutation path, only one centroidId is required
	// Scan paths can try to compute more than "1" closest
	// centroid depending on "nprobes" value

	t0 := time.Now()

	oneNearIds, err := mdb.codebook.FindNearestCentroids(vec, 1)
	if err != nil {
		err := fmt.Errorf("Error observed while computing centroidIds for vector: %v, instId: %v", vec, mdb.IndexInstId())
		return -1, err
	}

	mdb.idxStats.Timings.vtAssign.Put(time.Now().Sub(t0))
	return oneNearIds[0], nil
}

// Quantized codes are arranged as a slice of bytes
func (mdb *bhiveSlice) getQuantizedCodeForVector(vec []float32, codeSize int, buf []byte) ([]byte, error) {

	buf = buf[:codeSize]

	t0 := time.Now()

	err := mdb.codebook.EncodeVector(vec, buf)
	if err != nil {
		return nil, err
	}

	mdb.idxStats.Timings.vtEncode.Put(time.Now().Sub(t0))

	return buf, nil
}

// Similar to getQuantizedCodeForVector() but processes array of vectors
func (mdb *bhiveSlice) getQuantizedCodeForVectors(vecs [][]float32, codeSize int, buf []byte) ([]byte, error) {
	offset := 0
	buf = buf[:len(vecs)*codeSize+4]

	for _, vec := range vecs {

		err := mdb.codebook.EncodeVector(vec, buf[offset:])
		if err != nil {
			return nil, err
		}
		offset += mdb.codeSize
	}

	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(vecs)))
	return buf, nil
}

func (mdb *bhiveSlice) recoverCodebook(codebookPath string) error {
	// Construct codebook path
	newFilePath := filepath.Join(mdb.storageDir, codebookPath)
	_, err := iowrap.Os_Stat(newFilePath)
	if os.IsNotExist(err) {
		logging.Warnf("plasmaSlice::recoverCodebook error observed while recovering from codebookPath: %v, err: %v", newFilePath, err)
		return errCodebookPathNotFound
	}

	// Codebook path exists. Recover codebook from disk
	content, err := iowrap.Ioutil_ReadFile(newFilePath)
	if err != nil {
		logging.Errorf("plasmaSlice::recoverCodebook: Error observed while reading from disk for path: %v, err: %v", newFilePath, err)
		return errCodebookCorrupted
	}

	logging.Infof("plasmaSlice::recoverCodebook: reading from disk is successful for path: %v", newFilePath)

	codebook, err := vector.RecoverCodebook(content, string(mdb.idxDefn.VectorMeta.Quantizer.Type))
	if err != nil {
		logging.Errorf("plasmaSlice::recoverCodebook: Error observed while deserializing codebook at path: %v, err: %v", newFilePath, err)
		return errCodebookCorrupted
	}

	mdb.codebook = codebook
	mdb.codeSize, err = mdb.codebook.CodeSize()
	if err != nil {
		mdb.ResetCodebook() // Ignore error for now
		return err
	}

	mdb.initQuantizedCodeBuf()
	return nil
}

// //////////////////////////////////////////////////////////
// Serverless
// //////////////////////////////////////////////////////////

func (mdb *bhiveSlice) GetWriteUnits() uint64 {
	return 0
}

func (mdb *bhiveSlice) SetStopWriteUnitBilling(disableBilling bool) {

}

// //////////////////////////////////////////////////////////
// rebalance
// //////////////////////////////////////////////////////////

func (mdb *bhiveSlice) GetShardIds() []common.ShardId {
	return nil
}

func (mdb *bhiveSlice) ClearRebalRunning() {

}

func (mdb *bhiveSlice) SetRebalRunning() {

}

func (mdb *bhiveSlice) IsPersistanceActive() bool {
	return true
}

// //////////////////////////////////////////////////////////
// recovery
// //////////////////////////////////////////////////////////

func (mdb *bhiveSlice) RecoveryDone() {

}

func (mdb *bhiveSlice) BuildDone() {
	count := mdb.mainstore.ItemCount()
	logging.Infof("bhiveSlice: BuildDone.  Item count %v", count)

	atomic.StoreInt32(&mdb.isInitialBuild, 0)
}

func (mdb *bhiveSlice) GetAlternateShardId(common.PartitionId) string {
	return ""
}

// //////////////////////////////////////////////////////////
// reader
// //////////////////////////////////////////////////////////

func (mdb *bhiveSlice) GetReaderContext(user string, skipReadMetering bool) IndexReaderContext {
	return nil
}

// //////////////////////////////////////////////////////////
// snapshot
// //////////////////////////////////////////////////////////

func (mdb *bhiveSlice) NewSnapshot(ts *common.TsVbuuid, commit bool) (SnapshotInfo, error) {

	mdb.waitPersist()

	if mdb.CheckCmdChStopped() {
		return nil, common.ErrSliceClosed
	}

	qc := atomic.LoadInt64(&mdb.qCount)
	if qc > 0 {
		common.CrashOnError(fmt.Errorf("Slice Invariant Violation - commit with pending mutations"))
	}

	mdb.isDirty = false

	// Coming here means that cmdCh is empty and flush has finished for this index
	atomic.StoreUint32(&mdb.flushActive, 0)

	newSnapshotInfo := &bhiveSnapshotInfo{
		Ts:        ts,
		Committed: commit,
	}

	return newSnapshotInfo, nil
}

func (mdb *bhiveSlice) FlushDone() {
	mdb.waitPersist()

	qc := atomic.LoadInt64(&mdb.qCount)
	if qc > 0 {
		common.CrashOnError(fmt.Errorf("Slice Invariant Violation - commit with pending mutations"))
	}
}

// TODO - Get Persistent Snapshots from recovery points
func (mdb *bhiveSlice) GetSnapshots() ([]SnapshotInfo, error) {
	return nil, nil
}

func (mdb *bhiveSlice) OpenSnapshot(info SnapshotInfo) (Snapshot, error) {
	if mdb.CheckCmdChStopped() {
		return nil, common.ErrSliceClosed
	}

	snapInfo := info.(*bhiveSnapshotInfo)

	s := &bhiveSnapshot{slice: mdb,
		idxDefnId:  mdb.idxDefnId,
		idxInstId:  mdb.idxInstId,
		idxPartnId: mdb.idxPartnId,
		info:       snapInfo,
		ts:         snapInfo.Timestamp(),
		committed:  info.IsCommitted(),
	}

	s.Open()
	if !s.slice.CheckAndIncrRef() {
		return nil, common.ErrSliceClosed
	}
	mdb.snapCount++
	s.id = mdb.snapCount
	s.slice.idxStats.numOpenSnapshots.Add(1)

	var err error
	if s.MainSnap, err = mdb.mainstore.NewSnapshot(); err != nil {
		return nil, err
	}
	s.info.Count = int64(mdb.mainstore.ItemCount())

	if s.BackSnap, err = mdb.backstore.NewSnapshot(); err != nil {
		s.MainSnap.Close()
		return nil, err
	}

	if s.committed {
		mdb.mainstore.Sync()
		mdb.backstore.Sync()

		// TODO - Persistent Snapshot
		// mdb.doPersistSnapshot(s)
		// mdb.updateUsageStatsOnCommit()
	}

	if info.IsCommitted() {
		logging.Infof("bhiveSlice::OpenSnapshot SliceId %v IndexInstId %v PartitionId %v Creating New "+
			"Snapshot %v", mdb.id, mdb.idxInstId, mdb.idxPartnId, snapInfo)
	}

	mdb.setCommittedCount()

	return s, nil
}

// //////////////////////////////////////////////////////////
// rollback
// //////////////////////////////////////////////////////////

func (mdb *bhiveSlice) Rollback(s SnapshotInfo) error {
	return nil
}

func (mdb *bhiveSlice) RollbackToZero(bool) error {
	return nil
}

func (mdb *bhiveSlice) LastRollbackTs() *common.TsVbuuid {
	return nil
}

func (mdb *bhiveSlice) SetLastRollbackTs(ts *common.TsVbuuid) {

}

// //////////////////////////////////////////////////////////
// stats
// //////////////////////////////////////////////////////////

func (mdb *bhiveSlice) Statistics(consumerFilter uint64) (StorageStatistics, error) {
	return StorageStatistics{}, nil
}

func (mdb *bhiveSlice) GetTenantDiskSize() (int64, error) {
	return 0, nil
}

func (mdb *bhiveSlice) PrepareStats() {

}

func (mdb *bhiveSlice) ShardStatistics(common.PartitionId) *common.ShardStats {
	return nil
}

func (mdb *bhiveSlice) setCommittedCount() {
	curr := mdb.mainstore.ItemCount()
	atomic.StoreUint64(&mdb.committedCount, uint64(curr))
}

func (mdb *bhiveSlice) GetCommittedCount() uint64 {
	return atomic.LoadUint64(&mdb.committedCount)
}

// //////////////////////////////////////////////////////////
// lifecycle
// //////////////////////////////////////////////////////////

func (mdb *bhiveSlice) Compact(abortTime time.Time, minFrag int) error {
	return nil
}

func (mdb *bhiveSlice) Close() {
	mdb.lock.Lock()
	defer mdb.lock.Unlock()

	// Stop producer of cmdCh - if producer is blocked on cmdCh it will by
	// pass and quit processing that mutation. Any mutation thereafter will
	// be no-op
	close(mdb.cmdStopCh)

	// After producer and consumer is closed set the qCount to 0 so that any
	// routine waiting for queue to be empty can go ahead. Mutations remaining
	// in cmdCh will be garbage collected eventually upon close
	atomic.StoreInt64(&mdb.qCount, 0)

	// If we are closing after a Snapshot is created we refcount will be non 0
	// close will be tried after that snapshot is destroyed
	if mdb.refCount > 0 {
		mdb.isSoftClosed = true
		logging.Infof("bhiveSlice::Close Soft Closing Slice Id %v, IndexInstId %v, PartitionId %v, "+
			"IndexDefnId %v", mdb.id, mdb.idxInstId, mdb.idxPartnId, mdb.idxDefnId)
	} else {
		mdb.isClosed = true
		tryCloseBhiveSlice(mdb)
	}
}

func (mdb *bhiveSlice) IncrRef() {
	mdb.lock.Lock()
	defer mdb.lock.Unlock()

	mdb.refCount++
}

func (mdb *bhiveSlice) CheckAndIncrRef() bool {
	mdb.lock.Lock()
	defer mdb.lock.Unlock()

	if mdb.isClosed {
		return false
	}

	mdb.refCount++

	return true
}

func (mdb *bhiveSlice) DecrRef() {
	mdb.lock.Lock()
	defer mdb.lock.Unlock()

	mdb.refCount--
	if mdb.refCount == 0 {
		if mdb.isSoftClosed {
			mdb.isClosed = true
			tryCloseBhiveSlice(mdb)
		}
		if mdb.isSoftDeleted {
			mdb.isDeleted = true
			tryDeleteBhiveSlice(mdb)
		}
	}
}

func (mdb *bhiveSlice) Destroy() {

}

func tryDeleteBhiveSlice(mdb *bhiveSlice) {

	//cleanup the disk directory
	if err := destroyBhiveSlice(mdb.storageDir, mdb.path); err != nil {
		logging.Errorf("bhiveSlice::Destroy Error Cleaning Up Slice Id %v, "+
			"IndexInstId %v, PartitionId %v, IndexDefnId %v. Error %v", mdb.id,
			mdb.idxInstId, mdb.idxPartnId, mdb.idxDefnId, err)
	} else {
		logging.Infof("bhiveSlice::Destroy Cleaned Up Slice Id %v, "+
			"IndexInstId %v, PartitionId %v, IndexDefnId %v.", mdb.id,
			mdb.idxInstId, mdb.idxPartnId, mdb.idxDefnId)
	}
}

func tryCloseBhiveSlice(mdb *bhiveSlice) {

	logging.Infof("bhiveSlice::Close Closed Slice Id %v, "+
		"IndexInstId %v, PartitionId %v, IndexDefnId %v.", mdb.id,
		mdb.idxInstId, mdb.idxPartnId, mdb.idxDefnId)

	// FIXME -- persistent snapshot
	//mdb.waitForPersistorThread()
	mdb.mainstore.Close()
	mdb.backstore.Close()
}

func destroyBhiveSlice(storageDir string, path string) error {
	//TODO
	//if err := bhive.DestroyInstance(storageDir, path); err != nil {
	//	return err
	//}
	return nil
}

// //////////////////////////////////////////////////////////
// Snapshot
// //////////////////////////////////////////////////////////

func (s *bhiveSnapshot) Open() error {
	atomic.AddInt32(&s.refCount, int32(1))
	return nil
}

func (s *bhiveSnapshot) IsOpen() bool {
	count := atomic.LoadInt32(&s.refCount)
	return count > 0
}

func (s *bhiveSnapshot) Id() SliceId {
	return s.slice.Id()
}

func (s *bhiveSnapshot) IndexInstId() common.IndexInstId {
	return s.idxInstId
}

func (s *bhiveSnapshot) IndexDefnId() common.IndexDefnId {
	return s.idxDefnId
}

func (s *bhiveSnapshot) Timestamp() *common.TsVbuuid {
	return s.ts
}

func (s *bhiveSnapshot) Info() SnapshotInfo {
	return s.info
}

func (s *bhiveSnapshot) Close() error {

	count := atomic.AddInt32(&s.refCount, int32(-1))

	if count < 0 {
		logging.Errorf("bhiveSnapshot::Close Close operation requested "+
			"on already closed snapshot. Index %v, Bucket %v, IndexInstId %v, PartitionId %v",
			s.slice.idxDefn.Name, s.slice.idxDefn.Bucket, s.slice.idxInstId, s.slice.idxPartnId)
		return fmt.Errorf("Snapshot Already Closed")

	} else if count == 0 {
		s.Destroy()
	}

	return nil
}

func (s *bhiveSnapshot) Destroy() {
	s.MainSnap.Close()
	if s.BackSnap != nil {
		s.BackSnap.Close()
	}
	s.slice.idxStats.numOpenSnapshots.Add(-1)
	defer s.slice.DecrRef()
}

// //////////////////////////////////////////////////////////
// Snapshot Reader - Placeholder (to be implemented by Varun)
// //////////////////////////////////////////////////////////

func (s *bhiveSnapshot) CountTotal(ctx IndexReaderContext, stopch StopChannel) (uint64, error) {
	return 0, nil
}

func (s *bhiveSnapshot) StatCountTotal() (uint64, error) {
	return 0, nil
}

func (s *bhiveSnapshot) Range(IndexReaderContext, IndexKey, IndexKey, Inclusion, EntryCallback) error {
	return nil
}

func (s *bhiveSnapshot) CountRange(ctx IndexReaderContext, low, high IndexKey, inclusion Inclusion, stopch StopChannel) (uint64, error) {
	return 0, nil
}

func (s *bhiveSnapshot) CountLookup(ctx IndexReaderContext, keys []IndexKey, stopch StopChannel) (uint64, error) {
	return 0, nil
}

func (s *bhiveSnapshot) MultiScanCount(ctx IndexReaderContext, low, high IndexKey, inclusion Inclusion,
	scan Scan, distinct bool, stopch StopChannel) (uint64, error) {
	return 0, nil
}

func (s *bhiveSnapshot) Lookup(IndexReaderContext, IndexKey, EntryCallback) error {
	return nil
}

func (s *bhiveSnapshot) All(IndexReaderContext, EntryCallback) error {
	return nil
}

func (s *bhiveSnapshot) Exists(ctx IndexReaderContext, Indexkey IndexKey, stopch StopChannel) (bool, error) {
	return true, nil
}

// //////////////////////////////////////////////////////////
// SnapshotInfo
// //////////////////////////////////////////////////////////

func (info *bhiveSnapshotInfo) Timestamp() *common.TsVbuuid {
	return info.Ts
}

func (mdb *bhiveSnapshotInfo) IsCommitted() bool {
	return mdb.Committed
}

func (info *bhiveSnapshotInfo) IsOSOSnap() bool {
	if info.Ts != nil && info.Ts.GetSnapType() == common.DISK_SNAP_OSO {
		return true
	}
	return false
}

func (info *bhiveSnapshotInfo) Stats() map[string]interface{} {
	return info.IndexStats
}

// //////////////////////////////////////////////////////////
// error handling
// //////////////////////////////////////////////////////////

func (mdb *bhiveSlice) logErrorsToConsole(msg string) {
	common.Console(mdb.clusterAddr, msg)
}
