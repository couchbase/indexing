//go:build !community
// +build !community

package indexer

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/bhive"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/iowrap"
	"github.com/couchbase/indexing/secondary/logging"
	statsMgmt "github.com/couchbase/indexing/secondary/stats"
	"github.com/couchbase/indexing/secondary/vector"
	"github.com/couchbase/indexing/secondary/vector/codebook"
	"github.com/couchbase/plasma"
)

// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

////////////////////////////////////////////////
// Constant
////////////////////////////////////////////////

const (
	NumKVStore   = 10
	MaxBatchSize = 256
)

////////////////////////////////////////////////
// Type Declaration
////////////////////////////////////////////////

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

	// main store readers
	readers chan *bhive.Reader

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
	nlist    int               // number of centroids to use for training
	codebook codebook.Codebook // cookbook
	codeSize int               // Size of the quantized codes after training

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

	codec    bhive.Codec
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

	numReaders := sysconf["bhive.numReaders"].Int()
	slice.readers = make(chan *bhive.Reader, numReaders)

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
	cfg.NumKVStore = NumKVStore
	cfg.MaxBatchSize = MaxBatchSize

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
	cfg.KeyPrefixSize = uint64(cfg.CentroidIDSize)
	cfg.NumKVStore = NumKVStore
	cfg.MaxBatchSize = MaxBatchSize

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

	// Initialize readers
	for i := 0; i < cap(slice.readers); i++ {
		slice.readers <- slice.mainstore.NewReader()
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

	// compute centroidId and quantized code
	quantizedCode, centroidId, err := mdb.getQuantizedCodeForVector(vec, mdb.codeSize, mdb.quantizedCodeBuf[workerId])
	if err != nil {
		logging.Errorf("bhiveSlice::insertVectorIndex Slice Id %v IndexInstId %v PartitionId %v "+
			"Skipping docid:%s  due to error in computing centroidId and quantized code.  Error: %v",
			mdb.Id, mdb.idxInstId, mdb.idxPartnId, logging.TagStrUD(docid), err)
		atomic.AddInt32(&mdb.numKeysSkipped, 1)
		panic(err) // [VECTOR_TODO]: Having panics will help catch bugs. Remove panics after code stabilizes
	}

	if vec != nil {

		// remove old record
		if !init {
			if _, changed := mdb.deleteVectorIndex(docid, vec, key, workerId); !changed {
				return 0
			}
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
func (mdb *bhiveSlice) getQuantizedCodeForVector(vec []float32, codeSize int, buf []byte) ([]byte, int64, error) {

	buf = buf[:codeSize]

	t0 := time.Now()

	centroidId := make([]int64, 1, 1)
	err := mdb.codebook.EncodeAndAssignVectors(vec, buf, centroidId)
	if err != nil {
		return nil, 0, err
	}

	mdb.idxStats.Timings.vtEncode.Put(time.Now().Sub(t0))

	return buf, centroidId[0], nil
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

// //////////////////////////////////////////////////////////
// reader
// //////////////////////////////////////////////////////////

////////////////////////////////////////////////
// bhive readerCtx implementation
////////////////////////////////////////////////

type bhiveReaderCtx struct {
	ch               chan *bhive.Reader
	r                *bhive.Reader
	readUnits        uint64
	user             string
	skipReadMetering bool
}

func (ctx *bhiveReaderCtx) Init(donech chan bool) bool {
	select {
	case ctx.r = <-ctx.ch:
		return true
	case <-donech:
	}

	return false
}

func (ctx *bhiveReaderCtx) Done() {
	if ctx.r != nil {
		ctx.ch <- ctx.r
	}
}

func (ctx *bhiveReaderCtx) ReadUnits() uint64 {
	return ctx.readUnits
}

func (ctx *bhiveReaderCtx) RecordReadUnits(ru uint64) {
	ctx.readUnits += ru
}

func (ctx *bhiveReaderCtx) User() string {
	return ctx.user
}

func (ctx *bhiveReaderCtx) SkipReadMetering() bool {
	return ctx.skipReadMetering
}

// For vector indices, there is no support for "distinct" pushdown
// Hence, return nil for GetCursorKey()
func (ctx *bhiveReaderCtx) GetCursorKey() *[]byte {
	return nil
}

func (ctx *bhiveReaderCtx) SetCursorKey(key *[]byte) {
	// no-op
}

func (mdb *bhiveSlice) GetReaderContext(user string, skipReadMetering bool) IndexReaderContext {
	return &bhiveReaderCtx{
		ch:               mdb.readers,
		user:             user,
		skipReadMetering: skipReadMetering,
	}
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
		codec:      mdb.mainstore.GetCodec(),
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
	defer func() {
		if r := recover(); r != nil {
			logging.Fatalf("bhiveSlice::Statistics Error observed when processing Statistics on instId: %v, partnId: %v",
				mdb.IndexInstId(), mdb.IndexPartnId())
			panic(r)
		}
	}()

	if consumerFilter == statsMgmt.N1QLStorageStatsFilter {
		return mdb.handleN1QLStorageStatistics()
	}

	var sts StorageStatistics

	var internalData []string
	internalDataMap := make(map[string]interface{})

	var docidCount int64
	var numRecsMem, numRecsDisk int64
	var cacheHits, cacheMiss int64
	var bsNumRecsMem, bsNumRecsDisk int64

	mStats := mdb.mainstore.GetStats()
	numRecsMem += int64(float32(mStats.ItemCount) * mStats.ResidentRatio)
	numRecsDisk += int64(float32(mStats.ItemCount) * (1 - mStats.ResidentRatio))
	cacheHits += int64(mStats.CacheHits)
	cacheMiss += int64(mStats.CacheMisses)

	sts.MemUsed = int64(mStats.MemUsed)
	sts.InsertBytes = int64(mStats.NWriteBytes)
	sts.GetBytes = int64(mStats.NReadBytes)
	sts.DiskSize = int64(mStats.TotalDiskUsage)
	sts.DataSizeOnDisk = int64(mStats.TotalDataSize)
	sts.LogSpace = int64(mStats.TotalDiskUsage)
	sts.DataSize = int64(float32(mStats.TotalDataSize) * mStats.CompressionRatio)

	bStats := mdb.backstore.GetStats()
	docidCount = int64(bStats.ItemCount)
	bsNumRecsMem += int64(float32(bStats.ItemCount) * bStats.ResidentRatio)
	bsNumRecsDisk += int64(float32(bStats.ItemCount) * (1 - bStats.ResidentRatio))

	sts.MemUsed += int64(bStats.MemUsed)
	sts.InsertBytes += int64(bStats.NWriteBytes)
	sts.GetBytes += int64(bStats.NReadBytes)
	sts.DiskSize += int64(bStats.TotalDiskUsage)
	sts.DataSizeOnDisk += int64(bStats.TotalDataSize)
	sts.LogSpace += int64(bStats.TotalDiskUsage)
	sts.DataSize = int64(float32(mStats.TotalDataSize) * mStats.CompressionRatio)

	if consumerFilter == statsMgmt.AllStatsFilter {
		internalData = append(internalData, fmt.Sprintf("{\n\"MainStore\":\n%s", mStats))

		statsMap1 := make(map[string]interface{})
		if err := json.Unmarshal([]byte(mStats.String()), &statsMap1); err == nil {
			internalDataMap["MainStore"] = statsMap1
		} else {
			logging.Errorf("bhiveSlice::Statistics unable to unmarshal mainstore stats for"+
				" IndexInstId %v, PartitionId %v, IndexDefnId %v SliceId %v err: %v",
				mdb.idxInstId, mdb.idxPartnId, mdb.idxDefnId, mdb.id, err)
			internalDataMap["MainStore"] = fmt.Sprintf("%v", mStats)
		}

		internalData = append(internalData, fmt.Sprintf(",\n\"BackStore\":\n%s", bStats))

		statsMap2 := make(map[string]interface{})
		if err := json.Unmarshal([]byte(bStats.String()), &statsMap2); err == nil {
			internalDataMap["BackStore"] = statsMap2
		} else {
			logging.Errorf("bhiveSlice::Statistics unable to unmarshal backstore stats for"+
				" IndexInstId %v, PartitionId %v, IndexDefnId %v SliceId %v err: %v",
				mdb.idxInstId, mdb.idxPartnId, mdb.idxDefnId, mdb.id, err)
			internalDataMap["BackStore"] = fmt.Sprintf("%v", mStats)
		}

		internalData = append(internalData, "}\n")
	}

	sts.InternalData = internalData
	sts.InternalDataMap = internalDataMap
	sts.LoggingDisabled = false

	mdb.idxStats.docidCount.Set(docidCount)
	mdb.idxStats.residentPercent.Set(common.ComputePercent(numRecsMem, numRecsDisk))
	mdb.idxStats.cacheHitPercent.Set(common.ComputePercent(cacheHits, cacheMiss))
	mdb.idxStats.combinedResidentPercent.Set(common.ComputePercent((numRecsMem + bsNumRecsMem), (numRecsDisk + bsNumRecsDisk)))
	mdb.idxStats.cacheHits.Set(cacheHits)
	mdb.idxStats.cacheMisses.Set(cacheMiss)
	mdb.idxStats.numRecsInMem.Set(numRecsMem)
	mdb.idxStats.numRecsOnDisk.Set(numRecsDisk)
	mdb.idxStats.bsNumRecsInMem.Set(bsNumRecsMem)
	mdb.idxStats.bsNumRecsOnDisk.Set(bsNumRecsDisk)

	return sts, nil
}

// CBO is not supported
func (mdb *bhiveSlice) handleN1QLStorageStatistics() (StorageStatistics, error) {
	var sts StorageStatistics
	return sts, nil
}

func (mdb *bhiveSlice) GetTenantDiskSize() (int64, error) {
	return 0, nil
}

func (mdb *bhiveSlice) PrepareStats() {

}

func (mdb *bhiveSlice) ShardStatistics(partnId common.PartitionId) *common.ShardStats {
	var ss *common.ShardStats

	if len(mdb.idxDefn.AlternateShardIds) == 0 || len(mdb.idxDefn.AlternateShardIds[partnId]) == 0 {
		return nil
	}
	msAlternateShardId := mdb.idxDefn.AlternateShardIds[partnId][0]

	ss = common.NewShardStats(msAlternateShardId)

	val, err := bhive.GetShardInfo(msAlternateShardId)
	if err != nil {
		logging.Infof("bhiveSlice::ShardStatistics ShardInfo is not available for shard: %v, err: %v", msAlternateShardId, err)
		return nil
	}

	getShardId := func(alternateShardId string) common.ShardId {
		alternateId, err := plasma.ParseAlternateId(alternateShardId)
		if err != nil {
			logging.Errorf("bhiveSlice::ShardStatistics: plasma failed to parse alternate id %v for instance %v - partnId %v",
				alternateShardId, mdb.idxDefn.InstId, partnId)
			return 0
		}
		shard := bhive.AcquireShardByAltId(alternateId)
		defer bhive.ReleaseShard(shard)

		return common.ShardId(shard.GetShardId())
	}

	ss.ShardId = getShardId(msAlternateShardId)

	ss.MemSz = val.MemSz
	ss.LSSDataSize = val.LSSDataSize
	ss.ItemsCount = val.ItemsCount
	ss.LSSDiskSize = val.LSSDiskSize

	// For computing resident ratio
	ss.CachedRecords = val.CachedRecords
	ss.TotalRecords = val.TotalRecords

	for _, instPath := range val.InstList {
		ss.Instances[instPath] = true
	}

	if len(mdb.idxDefn.AlternateShardIds[partnId]) > 1 {
		bsAlternateShardId := mdb.idxDefn.AlternateShardIds[partnId][1]
		val, err := bhive.GetShardInfo(bsAlternateShardId)
		if err != nil {
			logging.Infof("plasmaSlice::ShardStatistics ShardInfo is not available for shard: %v", bsAlternateShardId)
			return nil
		}
		ss.MemSz += val.MemSz
		ss.LSSDataSize += val.LSSDataSize
		ss.ItemsCount += val.ItemsCount
		ss.LSSDiskSize += val.LSSDiskSize

		// For computing resident ratio
		ss.CachedRecords += val.CachedRecords
		ss.TotalRecords += val.TotalRecords

		ss.BackstoreShardId = getShardId(bsAlternateShardId)
	}

	return ss
}

func (mdb *bhiveSlice) GetAlternateShardId(partnId common.PartitionId) string {
	if len(mdb.idxDefn.AlternateShardIds) == 0 || len(mdb.idxDefn.AlternateShardIds[partnId]) == 0 {
		return ""
	}
	return mdb.idxDefn.AlternateShardIds[partnId][0]
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

// [VECTOR_TODO] The Count logic needs support from snapshot. Currently,
// implement dummy methods for IndexReader compatibility
func (s *bhiveSnapshot) CountTotal(ctx IndexReaderContext, stopch StopChannel) (uint64, error) {
	return 0, nil
}

// [VECTOR_TODO] The Count logic needs support from snapshot. Currently,
// implement dummy methods for IndexReader compatibility
func (s *bhiveSnapshot) StatCountTotal() (uint64, error) {
	return 0, nil
}
func (s *bhiveSnapshot) Iterate(ctx IndexReaderContext, centroidId IndexKey, callb EntryCallback) error {

	defer func() {
		if r := recover(); r != nil {
			logging.Fatalf("bhiveSnapshot::Iterate: panic detected while iterating snapshot "+
				"key = %s Index %v, Bucket %v, IndexInstId %v, "+
				"PartitionId %v, centroidId: %v", logging.TagStrUD(centroidId), s.slice.idxDefn.Name,
				s.slice.idxDefn.Bucket, s.slice.idxInstId, s.slice.idxPartnId, centroidId)
			logging.Fatalf("%s", logging.StackTraceAll())
			panic(r)
		}
	}()

	var err error
	t0 := time.Now()

	reader := ctx.(*bhiveReaderCtx)
	reader.r.Begin()
	defer reader.r.End()

	iter, err := reader.r.NewKeyPrefixIterator()
	if err != nil {
		return err
	}

	defer iter.Close()

	// Capture the time taken to initialize new iterator
	s.slice.idxStats.Timings.stNewIterator.Put(time.Since(t0))

	// [VECTOR_TODO]: Add more timing stats

	err = iter.Execute(s.MainSnap, bhive.CentroidID(centroidId.Bytes()))
	if err != nil {
		return err
	}

	for iter.Valid() {
		// rawKey would be only "docid"
		// rawMeta would include recordID (used internally by Magma) and the meta information
		// like include columns, quantized codes etc. Scan pipeline will split the recordID
		// from rawMeta and use the recordID to extract the actual Value field for re-ranking
		// purposes
		_, rawKey, rawMeta, err := iter.GetRawKeyAndMeta()
		if err != nil {
			return err
		}

		if err := callb(rawKey, rawMeta); err != nil {
			return err
		}

		iter.Next()
	}

	return nil
}

func (s *bhiveSnapshot) Range(ctx IndexReaderContext, low IndexKey, high IndexKey, incl Inclusion, callb EntryCallback) error {
	if low.CompareIndexKey(high) != 0 || incl != Both {
		panic(fmt.Errorf("bhiveSnapshot::Range low: %v and high: %v should be same for Range on bhive snapshot with inclusion: %v being Both", low, high, incl))
	}
	return s.Iterate(ctx, low, callb)
}

func (s *bhiveSnapshot) CountRange(ctx IndexReaderContext, low, high IndexKey, inclusion Inclusion, stopch StopChannel) (uint64, error) {

	var count uint64
	callb := func(key, value []byte) error {
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

func (s *bhiveSnapshot) Lookup(ctx IndexReaderContext, centroidId IndexKey, callb EntryCallback) error {
	return s.Iterate(ctx, centroidId, callb)
}

func (s *bhiveSnapshot) CountLookup(ctx IndexReaderContext, keys []IndexKey, stopch StopChannel) (uint64, error) {
	var err error
	var count uint64

	callb := func(key, value []byte) error {
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

func (s *bhiveSnapshot) Exists(ctx IndexReaderContext, key IndexKey, stopch StopChannel) (bool, error) {
	var count uint64
	callb := func(key, value []byte) error {
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

// VECTOR_TODO: Add support for multi scan count. Till then panic
func (s *bhiveSnapshot) MultiScanCount(ctx IndexReaderContext, low, high IndexKey, inclusion Inclusion,
	scan Scan, distinct bool, stopch StopChannel) (uint64, error) {
	panic("bhiveSnapshot::MultiScanCount - Currently not supported")
}

// VECTOR_TODO: All can be implemented using KeyIterator by scanning the entire storage
func (s *bhiveSnapshot) All(IndexReaderContext, EntryCallback) error {
	panic("bhiveSnapshot::All - Currently not supported")
}

func (s *bhiveSnapshot) DecodeMeta(meta []byte) (uint64, []byte) {
	recordId, actualMeta := s.codec.DecodeMeta(meta)
	return uint64(recordId), actualMeta
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