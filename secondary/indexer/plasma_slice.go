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
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/common/queryutil"
	"github.com/couchbase/indexing/secondary/logging"
	statsMgmt "github.com/couchbase/indexing/secondary/stats"
	"github.com/couchbase/plasma"
)

// Note - CE builds do not pull in plasma_slice.go
// Do not put any shared variables here

func init() {
	plasma.SetLogger(&logging.SystemLogger)
}

const (
	MAIN_INDEX plasma.InstanceGroup = iota + 1
	BACK_INDEX
)

type plasmaSlice struct {
	newBorn                               bool
	get_bytes, insert_bytes, delete_bytes int64
	flushedCount                          uint64
	committedCount                        uint64
	qCount                                int64

	path       string
	storageDir string
	logDir     string
	id         SliceId

	refCount int
	lock     sync.RWMutex

	mainstore *plasma.Plasma
	backstore *plasma.Plasma

	main []*plasma.Writer

	back []*plasma.Writer

	readers chan *plasma.Reader

	idxDefn    common.IndexDefn
	idxDefnId  common.IndexDefnId
	idxInstId  common.IndexInstId
	idxPartnId common.PartitionId

	flushActive uint32

	status        SliceStatus
	isActive      bool
	isDirty       bool
	isPrimary     bool
	isSoftDeleted bool
	isSoftClosed  bool
	numPartitions int
	isCompacting  bool

	cmdCh  []chan *indexMutation
	stopCh []DoneChannel

	fatalDbErr error

	numWriters    int
	maxNumWriters int
	maxRollbacks  int
	maxDiskSnaps  int
	numVbuckets   int

	totalFlushTime  time.Duration
	totalCommitTime time.Duration

	idxStats *IndexStats
	sysconf  common.Config
	confLock sync.RWMutex

	isPersistorActive int32

	lastRollbackTs *common.TsVbuuid

	// Array processing
	arrayExprPosition int
	isArrayDistinct   bool

	encodeBuf        [][]byte
	arrayBuf1        [][]byte
	arrayBuf2        [][]byte
	keySzConf        []keySizeConfig
	keySzConfChanged []int32 // Per worker, 0: key size not changed, >=1: key size changed

	hasPersistence bool

	indexerStats *IndexerStats

	clusterAddr string

	//
	// The following fields are used for tuning writers
	//

	// stats sampling
	drainTime     int64          // elapsed time for draining
	numItems      int64          // num of items in each flush
	drainRate     *common.Sample // samples of drain rate per writer (numItems per writer per flush interval)
	mutationRate  *common.Sample // samples of mutation rate (numItems per flush interval)
	lastCheckTime int64          // last time when checking whether writers need adjustment

	// logging
	numExpand int // number of expansion
	numReduce int // number of reduction

	// throttling
	minimumDrainRate float64 // minimum drain rate after adding/removing writer
	saturateCount    int     // number of misses on meeting minimum drain rate

	// config
	enableWriterTuning bool    // enable tuning on writers
	adjustInterval     uint64  // interval to check whether writer need tuning
	samplingWindow     uint64  // sampling window
	samplingInterval   uint64  // sampling interval
	snapInterval       uint64  // snapshot interval
	scalingFactor      float64 // scaling factor for percentage increase on drain rate
	threshold          int     // threshold on number of misses on drain rate

	writerLock    sync.Mutex // mutex for writer tuning
	samplerStopCh chan bool  // stop sampler
	token         *token     // token

	// Below are used to periodically reset/shrink slice buffers
	lastBufferSizeCheckTime     time.Time
	maxKeySizeInLastInterval    int64
	maxArrKeySizeInLastInterval int64

	//Below is used to track number of keys skipped due to errors
	//This count is used to log message to console logs
	//The count is reset when messages are logged to console
	numKeysSkipped int32
}

func newPlasmaSlice(storage_dir string, log_dir string, path string, sliceId SliceId, idxDefn common.IndexDefn,
	idxInstId common.IndexInstId, partitionId common.PartitionId,
	isPrimary bool, numPartitions int,
	sysconf common.Config, idxStats *IndexStats, indexerStats *IndexerStats, isNew bool) (*plasmaSlice, error) {

	slice := &plasmaSlice{}

	err := createSliceDir(storage_dir, path, isNew)
	if err != nil {
		return nil, err
	}
	slice.newBorn = isNew

	slice.idxStats = idxStats
	slice.indexerStats = indexerStats

	slice.get_bytes = 0
	slice.insert_bytes = 0
	slice.delete_bytes = 0
	slice.flushedCount = 0
	slice.committedCount = 0
	slice.sysconf = sysconf
	slice.path = path
	slice.storageDir = storage_dir
	slice.logDir = log_dir
	slice.idxInstId = idxInstId
	slice.idxDefnId = idxDefn.DefnId
	slice.idxPartnId = partitionId
	slice.idxDefn = idxDefn
	slice.id = sliceId
	slice.maxNumWriters = sysconf["numSliceWriters"].Int()
	slice.hasPersistence = !sysconf["plasma.disablePersistence"].Bool()
	slice.clusterAddr = sysconf["clusterAddr"].String()
	slice.numVbuckets = sysconf["numVbuckets"].Int()

	slice.maxRollbacks = sysconf["settings.plasma.recovery.max_rollbacks"].Int()
	slice.maxDiskSnaps = sysconf["recovery.max_disksnaps"].Int()

	updatePlasmaConfig(sysconf)
	if sysconf["plasma.UseQuotaTuner"].Bool() {
		go plasma.RunMemQuotaTuner()
	}

	numReaders := slice.sysconf["plasma.numReaders"].Int()
	slice.readers = make(chan *plasma.Reader, numReaders)

	slice.isPrimary = isPrimary
	slice.numPartitions = numPartitions

	slice.samplingWindow = uint64(sysconf["plasma.writer.tuning.sampling.window"].Int()) * uint64(time.Millisecond)
	slice.enableWriterTuning = sysconf["plasma.writer.tuning.enable"].Bool()
	slice.adjustInterval = uint64(sysconf["plasma.writer.tuning.adjust.interval"].Int()) * uint64(time.Millisecond)
	slice.samplingInterval = uint64(sysconf["plasma.writer.tuning.sampling.interval"].Int()) * uint64(time.Millisecond)
	slice.scalingFactor = sysconf["plasma.writer.tuning.throughput.scalingFactor"].Float64()
	slice.threshold = sysconf["plasma.writer.tuning.throttling.threshold"].Int()
	slice.drainRate = common.NewSample(int(slice.samplingWindow / slice.samplingInterval))
	slice.mutationRate = common.NewSample(int(slice.samplingWindow / slice.samplingInterval))
	slice.samplerStopCh = make(chan bool)
	slice.snapInterval = sysconf["settings.inmemory_snapshot.moi.interval"].Uint64() * uint64(time.Millisecond)

	if err := slice.initStores(); err != nil {
		// Index is unusable. Remove the data files and reinit
		if err == errStorageCorrupted {
			logging.Errorf("plasmaSlice:NewplasmaSlice Id %v IndexInstId %v PartitionId %v "+
				"fatal error occured: %v", sliceId, idxInstId, partitionId, err)
		}
		if isNew {
			destroyPlasmaSlice(storage_dir, path)
		}
		return nil, err
	}

	// Array related initialization
	_, slice.isArrayDistinct, slice.arrayExprPosition, err = queryutil.GetArrayExpressionPosition(idxDefn.SecExprs)
	if err != nil {
		return nil, err
	}

	// intiialize and start the writers
	slice.setupWriters()

	logging.Infof("plasmaSlice:NewplasmaSlice Created New Slice Id %v IndexInstId %v partitionId %v "+
		"WriterThreads %v", sliceId, idxInstId, partitionId, slice.numWriters)

	slice.setCommittedCount()
	return slice, nil
}

func createSliceDir(storageDir string, path string, isNew bool) error {

	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			os.Mkdir(path, 0777)
			return nil
		}
	} else if isNew {
		// if we expect a new instance but there is residual file, destroy old data.
		err = plasma.DestroyInstance(storageDir, path)
	}

	return err
}

func destroyPlasmaSlice(storageDir string, path string) error {
	if err := plasma.DestroyInstance(storageDir, path); err != nil {
		return err
	}

	// remove directory created in newPlasmaSlice()
	return os.RemoveAll(path)
}

func listPlasmaSlices() ([]string, error) {
	return plasma.ListInstancePaths(), nil
}

func backupCorruptedPlasmaSlice(storageDir string, prefix string, rename func(string) (string, error), clean func(string)) error {
	return plasma.BackupCorruptedInstance(storageDir, prefix, rename, clean)
}

func (slice *plasmaSlice) initStores() error {
	var err error
	cfg := plasma.DefaultConfig()
	cfg.UseMemoryMgmt = slice.sysconf["plasma.useMemMgmt"].Bool()
	cfg.FlushBufferSize = int(slice.sysconf["plasma.flushBufferSize"].Int())
	cfg.LSSLogSegmentSize = int64(slice.sysconf["plasma.LSSSegmentFileSize"].Int())
	cfg.UseCompression = slice.sysconf["plasma.useCompression"].Bool()
	cfg.AutoSwapper = true
	cfg.NumEvictorThreads = int(float32(runtime.GOMAXPROCS(0))*
		float32(slice.sysconf["plasma.evictionCPUPercent"].Int())/(100) + 0.5)
	cfg.DisableReadCaching = slice.sysconf["plasma.disableReadCaching"].Bool()
	cfg.AutoMVCCPurging = slice.sysconf["plasma.purger.enabled"].Bool()
	cfg.PurgerInterval = time.Duration(slice.sysconf["plasma.purger.interval"].Int()) * time.Second
	cfg.PurgeThreshold = slice.sysconf["plasma.purger.highThreshold"].Float64()
	cfg.PurgeLowThreshold = slice.sysconf["plasma.purger.lowThreshold"].Float64()
	cfg.PurgeCompactRatio = slice.sysconf["plasma.purger.compactRatio"].Float64()
	cfg.EnablePageChecksum = slice.sysconf["plasma.enablePageChecksum"].Bool()
	cfg.EnableLSSPageSMO = slice.sysconf["plasma.enableLSSPageSMO"].Bool()
	cfg.LSSReadAheadSize = int64(slice.sysconf["plasma.logReadAheadSize"].Int())
	cfg.CheckpointInterval = time.Second * time.Duration(slice.sysconf["plasma.checkpointInterval"].Int())
	cfg.LSSCleanerConcurrency = slice.sysconf["plasma.LSSCleanerConcurrency"].Int()
	cfg.AutoTuneLSSCleaning = slice.sysconf["plasma.AutoTuneLSSCleaner"].Bool()
	cfg.AutoTuneDiskQuota = int64(slice.sysconf["plasma.AutoTuneDiskQuota"].Uint64())
	cfg.AutoTuneCleanerTargetFragRatio = slice.sysconf["plasma.AutoTuneCleanerTargetFragRatio"].Int()
	cfg.AutoTuneCleanerMinBandwidthRatio = slice.sysconf["plasma.AutoTuneCleanerMinBandwidthRatio"].Float64()
	cfg.AutoTuneDiskFullTimeLimit = slice.sysconf["plasma.AutoTuneDiskFullTimeLimit"].Int()
	cfg.AutoTuneAvailDiskLimit = slice.sysconf["plasma.AutoTuneAvailDiskLimit"].Float64()
	cfg.Compression = slice.sysconf["plasma.compression"].String()
	cfg.MaxPageSize = slice.sysconf["plasma.MaxPageSize"].Int()
	cfg.AutoLSSCleaning = !slice.sysconf["settings.compaction.plasma.manual"].Bool()
	cfg.EnforceKeyRange = slice.sysconf["plasma.enforceKeyRange"].Bool()
	cfg.MaxInstsPerShard = slice.sysconf["plasma.maxInstancePerShard"].Uint64()
	cfg.MaxDiskPerShard = slice.sysconf["plasma.maxDiskUsagePerShard"].Uint64()
	cfg.MinNumShard = slice.sysconf["plasma.minNumShard"].Uint64()

	cfg.StatsRunInterval = time.Duration(slice.sysconf["plasma.stats.runInterval"].Uint64()) * time.Second
	cfg.StatsLogInterval = time.Duration(slice.sysconf["plasma.stats.logInterval"].Uint64()) * time.Second
	cfg.StatsKeySizeThreshold = slice.sysconf["plasma.stats.threshold.keySize"].Uint64()
	cfg.StatsPercentileThreshold = slice.sysconf["plasma.stats.threshold.percentile"].Float64()
	cfg.StatsNumInstsThreshold = slice.sysconf["plasma.stats.threshold.numInstances"].Int()
	cfg.StatsLoggerFileName = slice.sysconf["plasma.stats.logger.fileName"].String()
	cfg.StatsLoggerFileSize = slice.sysconf["plasma.stats.logger.fileSize"].Uint64()
	cfg.StatsLoggerFileCount = slice.sysconf["plasma.stats.logger.fileCount"].Uint64()
	cfg.RecoveryCheckpointInterval = slice.sysconf["plasma.recovery.checkpointInterval"].Uint64()

	cfg.StorageDir = slice.storageDir
	cfg.LogDir = slice.logDir

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
	mCfg.EnablePeriodicEvict = slice.sysconf["plasma.mainIndex.enablePeriodicEvict"].Bool()
	mCfg.EvictMinThreshold = slice.sysconf["plasma.mainIndex.evictMinThreshold"].Float64()
	mCfg.EvictMaxThreshold = slice.sysconf["plasma.mainIndex.evictMaxThreshold"].Float64()
	mCfg.EvictDirtyOnPersistRatio = slice.sysconf["plasma.mainIndex.evictDirtyOnPersistRatio"].Float64()
	mCfg.EvictDirtyPercent = slice.sysconf["plasma.mainIndex.evictDirtyPercent"].Float64()
	mCfg.EvictSweepInterval = time.Duration(slice.sysconf["plasma.mainIndex.evictSweepInterval"].Int()) * time.Second
	mCfg.EvictRunInterval = time.Duration(slice.sysconf["plasma.mainIndex.evictRunInterval"].Int()) * time.Millisecond
	mCfg.EvictUseMemEstimate = slice.sysconf["plasma.mainIndex.evictUseMemEstimate"].Bool()
	mCfg.LogPrefix = fmt.Sprintf("%s/%s/Mainstore#%d:%d ", slice.idxDefn.Bucket, slice.idxDefn.Name, slice.idxInstId, slice.idxPartnId)
	mCfg.EnablePageBloomFilter = slice.sysconf["plasma.mainIndex.enablePageBloomFilter"].Bool()
	mCfg.BloomFilterFalsePositiveRate = slice.sysconf["plasma.mainIndex.bloomFilterFalsePositiveRate"].Float64()
	mCfg.BloomFilterExpectedMaxItems = slice.sysconf["plasma.mainIndex.bloomFilterExpectedMaxItems"].Uint64()
	mCfg.EnableInMemoryCompression = slice.sysconf["plasma.mainIndex.enableInMemoryCompression"].Bool()

	bCfg.MaxDeltaChainLen = slice.sysconf["plasma.backIndex.maxNumPageDeltas"].Int()
	bCfg.MaxPageItems = slice.sysconf["plasma.backIndex.pageSplitThreshold"].Int()
	bCfg.MinPageItems = slice.sysconf["plasma.backIndex.pageMergeThreshold"].Int()
	bCfg.MaxPageLSSSegments = slice.sysconf["plasma.backIndex.maxLSSPageSegments"].Int()
	bCfg.LSSCleanerThreshold = slice.sysconf["plasma.backIndex.LSSFragmentation"].Int()
	bCfg.LSSCleanerMaxThreshold = slice.sysconf["plasma.backIndex.maxLSSFragmentation"].Int()
	bCfg.EnablePeriodicEvict = slice.sysconf["plasma.backIndex.enablePeriodicEvict"].Bool()
	bCfg.EvictMinThreshold = slice.sysconf["plasma.backIndex.evictMinThreshold"].Float64()
	bCfg.EvictMaxThreshold = slice.sysconf["plasma.backIndex.evictMaxThreshold"].Float64()
	bCfg.EvictDirtyOnPersistRatio = slice.sysconf["plasma.backIndex.evictDirtyOnPersistRatio"].Float64()
	bCfg.EvictDirtyPercent = slice.sysconf["plasma.backIndex.evictDirtyPercent"].Float64()
	bCfg.EvictSweepInterval = time.Duration(slice.sysconf["plasma.backIndex.evictSweepInterval"].Int()) * time.Second
	bCfg.EvictRunInterval = time.Duration(slice.sysconf["plasma.backIndex.evictRunInterval"].Int()) * time.Millisecond
	bCfg.EvictUseMemEstimate = slice.sysconf["plasma.backIndex.evictUseMemEstimate"].Bool()
	bCfg.LogPrefix = fmt.Sprintf("%s/%s/Backstore#%d:%d ", slice.idxDefn.Bucket, slice.idxDefn.Name, slice.idxInstId, slice.idxPartnId)
	bCfg.EnablePageBloomFilter = slice.sysconf["plasma.backIndex.enablePageBloomFilter"].Bool()
	bCfg.BloomFilterFalsePositiveRate = slice.sysconf["plasma.backIndex.bloomFilterFalsePositiveRate"].Float64()
	bCfg.BloomFilterExpectedMaxItems = slice.sysconf["plasma.backIndex.bloomFilterExpectedMaxItems"].Uint64()
	bCfg.EnableInMemoryCompression = slice.sysconf["plasma.backIndex.enableInMemoryCompression"].Bool()

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

		slice.mainstore, mErr = plasma.New3(mCfg, slice.idxDefn.IndexOnCollection(), slice.newBorn, MAIN_INDEX)
		if mErr != nil {
			mErr = fmt.Errorf("Unable to initialize %s, err = %v", mCfg.File, mErr)
			return
		}
	}()

	if !slice.isPrimary {
		// Recover backindex
		wg.Add(1)
		go func() {
			defer wg.Done()
			slice.backstore, bErr = plasma.New3(bCfg, slice.idxDefn.IndexOnCollection(), slice.newBorn, BACK_INDEX)
			if bErr != nil {
				bErr = fmt.Errorf("Unable to initialize %s, err = %v", bCfg.File, bErr)
				return
			}
		}()
	}

	wg.Wait()

	// In case of errors, close the opened stores
	if mErr != nil {
		if !slice.isPrimary && bErr == nil {
			slice.backstore.Close()
		}
	} else if bErr != nil {
		if mErr == nil {
			slice.mainstore.Close()
		}
	}

	// Return fatal error with higher priority.
	if mErr != nil && plasma.IsFatalError(mErr) {
		logging.Errorf("plasmaSlice:NewplasmaSlice Id %v IndexInstId %v "+
			"fatal error occured: %v", slice.Id, slice.idxInstId, mErr)
		return errStorageCorrupted
	}

	if bErr != nil && plasma.IsFatalError(bErr) {
		logging.Errorf("plasmaSlice:NewplasmaSlice Id %v IndexInstId %v "+
			"fatal error occured: %v", slice.Id, slice.idxInstId, bErr)
		return errStorageCorrupted
	}

	// If both mErr and bErr are not fatal, return mErr with higher priority
	if mErr != nil {
		return mErr
	}

	if bErr != nil {
		return bErr
	}

	for i := 0; i < cap(slice.readers); i++ {
		slice.readers <- slice.mainstore.NewReader()
	}

	if !slice.newBorn {
		logging.Infof("plasmaSlice::doRecovery SliceId %v IndexInstId %v PartitionId %v Recovering from recovery point ..",
			slice.id, slice.idxInstId, slice.idxPartnId)
		err = slice.doRecovery()
		dur := time.Since(t0)
		if err == nil {
			slice.idxStats.diskSnapLoadDuration.Set(int64(dur / time.Millisecond))
			logging.Infof("plasmaSlice::doRecovery SliceId %v IndexInstId %v PartitionId %v Warmup took %v",
				slice.id, slice.idxInstId, slice.idxPartnId, dur)
		}
	}

	return err
}

type plasmaReaderCtx struct {
	ch chan *plasma.Reader
	r  *plasma.Reader
	cursorCtx
}

func (ctx *plasmaReaderCtx) Init(donech chan bool) bool {
	select {
	case ctx.r = <-ctx.ch:
		return true
	case <-donech:
	}

	return false
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
		logging.Infof("plasmaSlice::doRecovery SliceId %v IndexInstId %v PartitionId %v Unable to find recovery point. Resetting store ..",
			mdb.id, mdb.idxInstId, mdb.idxPartnId)
		if err := mdb.resetStores(); err != nil {
			return err
		}
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
	op := opUpdate
	if meta.firstSnap {
		op = opInsert
	}

	mut := &indexMutation{
		op:    op,
		key:   key,
		docid: docid,
		meta:  meta,
	}

	atomic.AddInt64(&mdb.qCount, 1)
	atomic.StoreUint32(&mdb.flushActive, 1)
	mdb.cmdCh[int(meta.vbucket)%mdb.numWriters] <- mut
	mdb.idxStats.numDocsFlushQueued.Add(1)
	return mdb.fatalDbErr
}

func (mdb *plasmaSlice) Delete(docid []byte, meta *MutationMeta) error {
	if !meta.firstSnap {
		atomic.AddInt64(&mdb.qCount, 1)
		mdb.idxStats.numDocsFlushQueued.Add(1)
		atomic.StoreUint32(&mdb.flushActive, 1)
		mdb.cmdCh[int(meta.vbucket)%mdb.numWriters] <- &indexMutation{op: opDelete, docid: docid}
	}
	return mdb.fatalDbErr
}

func (mdb *plasmaSlice) handleCommandsWorker(workerId int) {
	var start time.Time
	var elapsed time.Duration
	var icmd *indexMutation

	defer func() {
		if r := recover(); r != nil {
			logging.Fatalf("plasmaSlice::handleCommandsWorker: panic detected while processing mutation for "+
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
				nmut = mdb.insert(icmd.key, icmd.docid, workerId, icmd.op == opInsert, icmd.meta)
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
			atomic.AddInt64(&mdb.qCount, -1)

			if mdb.enableWriterTuning {
				atomic.AddInt64(&mdb.drainTime, elapsed.Nanoseconds())
				atomic.AddInt64(&mdb.numItems, int64(nmut))
			}

		case _, ok := <-mdb.stopCh[workerId]:
			if ok {
				mdb.stopCh[workerId] <- true
			}
			break loop

		}
	}
}

func (mdb *plasmaSlice) updateSliceBuffers(workerId int) keySizeConfig {

	if atomic.LoadInt32(&mdb.keySzConfChanged[workerId]) >= 1 {

		mdb.confLock.RLock()
		mdb.keySzConf[workerId] = getKeySizeConfig(mdb.sysconf)
		mdb.confLock.RUnlock()

		// Reset buffers if allow_large_keys is false
		if !mdb.keySzConf[workerId].allowLargeKeys {
			mdb.encodeBuf[workerId] = make([]byte, 0, mdb.keySzConf[workerId].maxIndexEntrySize)
			if mdb.idxDefn.IsArrayIndex {
				mdb.arrayBuf1[workerId] = make([]byte, 0, mdb.keySzConf[workerId].maxArrayIndexEntrySize)
				mdb.arrayBuf2[workerId] = make([]byte, 0, mdb.keySzConf[workerId].maxArrayIndexEntrySize)
			}
		}

		atomic.AddInt32(&mdb.keySzConfChanged[workerId], -1)
	}
	return mdb.keySzConf[workerId]
}

func (mdb *plasmaSlice) periodicSliceBuffersReset() {
	checkInterval := time.Minute * 60
	if time.Since(mdb.lastBufferSizeCheckTime) > checkInterval {

		mdb.confLock.RLock()
		allowLargeKeys := mdb.sysconf["settings.allow_large_keys"].Bool()
		mdb.confLock.RUnlock()

		if allowLargeKeys == true {
			maxSz := atomic.LoadInt64(&mdb.maxKeySizeInLastInterval)
			maxArrSz := atomic.LoadInt64(&mdb.maxArrKeySizeInLastInterval)

			// account for extra bytes used in insert path
			maxSz += MAX_KEY_EXTRABYTES_LEN

			for i := range mdb.encodeBuf {
				if maxSz > defaultKeySz && (int64(cap(mdb.encodeBuf[i]))-maxSz > 1024) {
					// Shrink the buffer
					mdb.encodeBuf[i] = make([]byte, 0, maxSz)
				}
			}
			for i := range mdb.arrayBuf1 {
				if maxArrSz > defaultArrKeySz && (int64(cap(mdb.arrayBuf1[i]))-maxArrSz > 1024) {
					// Shrink the buffer
					mdb.arrayBuf1[i] = make([]byte, 0, maxArrSz)
				}
			}
			for i := range mdb.arrayBuf2 {
				if maxArrSz > defaultArrKeySz && (int64(cap(mdb.arrayBuf2[i]))-maxArrSz > 1024) {
					// Shrink the buffer
					mdb.arrayBuf2[i] = make([]byte, 0, maxArrSz)
				}
			}
		}

		mdb.lastBufferSizeCheckTime = time.Now()
		atomic.StoreInt64(&mdb.maxKeySizeInLastInterval, 0)
		atomic.StoreInt64(&mdb.maxArrKeySizeInLastInterval, 0)
	}
}

func (mdb *plasmaSlice) logErrorsToConsole() {

	numSkipped := atomic.LoadInt32(&mdb.numKeysSkipped)
	if numSkipped == 0 {
		return
	}

	logMsg := fmt.Sprintf("Index entries were skipped in index: %v, bucket: %v, "+
		"IndexInstId: %v PartitionId: %v due to errors. Please check indexer logs for more details.",
		mdb.idxDefn.Name, mdb.idxDefn.Bucket, mdb.idxInstId, mdb.idxPartnId)
	common.Console(mdb.clusterAddr, logMsg)
	atomic.StoreInt32(&mdb.numKeysSkipped, 0)
}

func (mdb *plasmaSlice) insert(key []byte, docid []byte, workerId int,
	init bool, meta *MutationMeta) int {
	var nmut int

	if mdb.isPrimary {
		nmut = mdb.insertPrimaryIndex(key, docid, workerId)
	} else if len(key) == 0 {
		nmut = mdb.delete(docid, workerId)
	} else {
		if mdb.idxDefn.IsArrayIndex {
			nmut = mdb.insertSecArrayIndex(key, docid, workerId, init, meta)
		} else {
			nmut = mdb.insertSecIndex(key, docid, workerId, init, meta)
		}
	}

	mdb.logWriterStat()
	return nmut
}

func (mdb *plasmaSlice) insertPrimaryIndex(key []byte, docid []byte, workerId int) int {

	entry, err := NewPrimaryIndexEntry(docid)
	common.CrashOnError(err)

	mdb.main[workerId].Begin()
	defer mdb.main[workerId].End()

	_, err = mdb.main[workerId].LookupKV(entry)
	if err == plasma.ErrItemNotFound {
		t0 := time.Now()
		err = mdb.main[workerId].InsertKV(entry, nil)
		mdb.idxStats.Timings.stKVSet.Put(time.Now().Sub(t0))
		if err == nil {
			atomic.AddInt64(&mdb.insert_bytes, int64(len(entry)))
			mdb.idxStats.rawDataSize.Add(int64(len(entry)))
		}
		mdb.isDirty = true
		return 1
	}

	return 0
}

func (mdb *plasmaSlice) insertSecIndex(key []byte, docid []byte, workerId int, init bool, meta *MutationMeta) int {
	t0 := time.Now()

	var ndel int
	var changed bool

	szConf := mdb.updateSliceBuffers(workerId)

	// The docid does not exist if the doc is initialized for the first time
	if !init {
		if ndel, changed = mdb.deleteSecIndex(docid, key, workerId); !changed {
			return 0
		}
	}

	mdb.encodeBuf[workerId] = resizeEncodeBuf(mdb.encodeBuf[workerId], len(key), szConf.allowLargeKeys)
	entry, err := NewSecondaryIndexEntry(key, docid, mdb.idxDefn.IsArrayIndex,
		1, mdb.idxDefn.Desc, mdb.encodeBuf[workerId], meta, szConf)
	if err != nil {
		logging.Errorf("plasmaSlice::insertSecIndex Slice Id %v IndexInstId %v PartitionId %v "+
			"Skipping docid:%s (%v)", mdb.Id, mdb.idxInstId, mdb.idxPartnId, logging.TagStrUD(docid), err)
		atomic.AddInt32(&mdb.numKeysSkipped, 1)
		return ndel
	}

	if len(key) > 0 {
		mdb.main[workerId].Begin()
		defer mdb.main[workerId].End()
		mdb.back[workerId].Begin()
		defer mdb.back[workerId].End()

		err = mdb.main[workerId].InsertKV(entry, nil)
		if err == nil {
			mdb.idxStats.rawDataSize.Add(int64(len(entry)))
			addKeySizeStat(mdb.idxStats, len(entry))
			atomic.AddInt64(&mdb.insert_bytes, int64(len(entry)))
		}

		// entry2BackEntry overwrites the buffer to remove docid
		backEntry := entry2BackEntry(entry)
		err = mdb.back[workerId].InsertKV(docid, backEntry)
		mdb.idxStats.Timings.stKVSet.Put(time.Now().Sub(t0))

		if err == nil {
			// rawDataSize is the sum of all data inserted into main store and back store
			mdb.idxStats.backstoreRawDataSize.Add(int64(len(docid) + len(backEntry)))
			mdb.idxStats.rawDataSize.Add(int64(len(docid) + len(backEntry)))
		}

		if int64(len(key)) > atomic.LoadInt64(&mdb.maxKeySizeInLastInterval) {
			atomic.StoreInt64(&mdb.maxKeySizeInLastInterval, int64(len(key)))
		}
	}

	mdb.isDirty = true
	return 1
}

func (mdb *plasmaSlice) insertSecArrayIndex(key []byte, docid []byte, workerId int,
	init bool, meta *MutationMeta) (nmut int) {
	var err error
	var oldkey []byte

	szConf := mdb.updateSliceBuffers(workerId)
	mdb.arrayBuf2[workerId] = resizeArrayBuf(mdb.arrayBuf2[workerId], 3*len(key), szConf.allowLargeKeys)

	if !szConf.allowLargeKeys && len(key) > szConf.maxArrayIndexEntrySize {
		logging.Errorf("plasmaSlice::insertSecArrayIndex Error indexing docid: %s in Slice: %v. Error: Encoded array key (size %v) too long (> %v). Skipped.",
			logging.TagStrUD(docid), mdb.id, len(key), szConf.maxArrayIndexEntrySize)
		atomic.AddInt32(&mdb.numKeysSkipped, 1)
		mdb.deleteSecArrayIndex(docid, workerId)
		return 0
	}

	mdb.main[workerId].Begin()
	defer mdb.main[workerId].End()
	mdb.back[workerId].Begin()
	defer mdb.back[workerId].End()

	// The docid does not exist if the doc is initialized for the first time
	if !init {
		oldkey, err = mdb.back[workerId].LookupKV(docid)
		if err == plasma.ErrItemNotFound {
			oldkey = nil
		}
	}

	var oldEntriesBytes, newEntriesBytes [][]byte
	var oldKeyCount, newKeyCount []int
	var newbufLen int
	if oldkey != nil {
		if bytes.Equal(oldkey, key) {
			return
		}

		var tmpBuf []byte
		if len(oldkey)*3 > cap(mdb.arrayBuf1[workerId]) {
			tmpBuf = make([]byte, 0, len(oldkey)*3)
		} else {
			tmpBuf = mdb.arrayBuf1[workerId]
		}

		//get the key in original form
		if mdb.idxDefn.Desc != nil {
			_, err = jsonEncoder.ReverseCollate(oldkey, mdb.idxDefn.Desc)
			if err != nil {
				logging.Errorf("plasmaSlice::insertSecArrayIndex SliceId %v IndexInstId %v PartitionId %v. "+
					"Error from ReverseCollate of old key. Skipping docid:%s Error: %v",
					mdb.id, mdb.idxInstId, mdb.idxPartnId, logging.TagStrUD(docid), err)
				mdb.deleteSecArrayIndexNoTx(docid, workerId)
				return 0
			}
		}

		oldEntriesBytes, oldKeyCount, newbufLen, err = ArrayIndexItems(oldkey, mdb.arrayExprPosition,
			tmpBuf, mdb.isArrayDistinct, false, szConf)
		mdb.arrayBuf1[workerId] = resizeArrayBuf(mdb.arrayBuf1[workerId], newbufLen, szConf.allowLargeKeys)

		if err != nil {
			logging.Errorf("plasmaSlice::insertSecArrayIndex SliceId %v IndexInstId %v PartitionId %v Error in retrieving "+
				"compostite old secondary keys. Skipping docid:%s Error: %v",
				mdb.id, mdb.idxInstId, mdb.idxPartnId, logging.TagStrUD(docid), err)
			atomic.AddInt32(&mdb.numKeysSkipped, 1)
			mdb.deleteSecArrayIndexNoTx(docid, workerId)
			return 0
		}
	}

	if key != nil {

		newEntriesBytes, newKeyCount, newbufLen, err = ArrayIndexItems(key, mdb.arrayExprPosition,
			mdb.arrayBuf2[workerId], mdb.isArrayDistinct, !szConf.allowLargeKeys, szConf)
		mdb.arrayBuf2[workerId] = resizeArrayBuf(mdb.arrayBuf2[workerId], newbufLen, szConf.allowLargeKeys)
		if err != nil {
			logging.Errorf("plasmaSlice::insertSecArrayIndex SliceId %v IndexInstId %v PartitionId %v Error in creating "+
				"compostite new secondary keys. Skipping docid:%s Error: %v",
				mdb.id, mdb.idxInstId, mdb.idxPartnId, logging.TagStrUD(docid), err)
			atomic.AddInt32(&mdb.numKeysSkipped, 1)
			mdb.deleteSecArrayIndexNoTx(docid, workerId)
			return 0
		}
		if int64(newbufLen) > atomic.LoadInt64(&mdb.maxArrKeySizeInLastInterval) {
			atomic.StoreInt64(&mdb.maxArrKeySizeInLastInterval, int64(newbufLen))
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
					oldKeyCount[i], mdb.idxDefn.Desc, mdb.encodeBuf[workerId][:0], nil, szConf)
				common.CrashOnError(err)
				// Add back
				err = mdb.main[workerId].InsertKV(entry, nil)
				if err == nil {
					mdb.idxStats.rawDataSize.Add(int64(len(entry)))
					addKeySizeStat(mdb.idxStats, len(entry))
				}
			}
		}
	}

	rollbackAdds := func(upto int) {
		for i := 0; i <= upto; i++ {
			key := indexEntriesToBeAdded[i]
			if key != nil { // nil item indicates it should be ignored
				entry, err := NewSecondaryIndexEntry(key, docid, false,
					newKeyCount[i], mdb.idxDefn.Desc, mdb.encodeBuf[workerId][:0], meta, szConf)
				common.CrashOnError(err)
				// Delete back
				entrySz := len(entry)
				err = mdb.main[workerId].DeleteKV(entry)
				if err == nil {
					mdb.idxStats.rawDataSize.Add(0 - int64(entrySz))
					subtractKeySizeStat(mdb.idxStats, entrySz)
				}
			}
		}
	}

	// Delete each of indexEntriesToBeDeleted from main index
	for i, item := range indexEntriesToBeDeleted {
		if item != nil { // nil item indicates it should not be deleted
			var keyToBeDeleted []byte
			mdb.encodeBuf[workerId] = resizeEncodeBuf(mdb.encodeBuf[workerId], len(item), true)
			if keyToBeDeleted, err = GetIndexEntryBytes3(item, docid, false, false,
				oldKeyCount[i], mdb.idxDefn.Desc, mdb.encodeBuf[workerId], nil, szConf); err != nil {
				rollbackDeletes(i - 1)
				logging.Errorf("plasmaSlice::insertSecArrayIndex SliceId %v IndexInstId %v PartitionId %v Error forming entry "+
					"to be added to main index. Skipping docid:%s Error: %v",
					mdb.id, mdb.idxInstId, mdb.idxPartnId, logging.TagStrUD(docid), err)
				atomic.AddInt32(&mdb.numKeysSkipped, 1)
				mdb.deleteSecArrayIndexNoTx(docid, workerId)
				return 0
			}
			keyDelSz := len(keyToBeDeleted)

			t0 := time.Now()
			err = mdb.main[workerId].DeleteKV(keyToBeDeleted)
			mdb.idxStats.Timings.stKVDelete.Put(time.Now().Sub(t0))

			if err == nil {
				mdb.idxStats.rawDataSize.Add(0 - int64(keyDelSz))
				subtractKeySizeStat(mdb.idxStats, keyDelSz)
				atomic.AddInt64(&mdb.delete_bytes, int64(keyDelSz))
			}
			nmut++
		}
	}

	// Insert each of indexEntriesToBeAdded into main index
	for i, item := range indexEntriesToBeAdded {
		if item != nil { // nil item indicates it should not be added
			var keyToBeAdded []byte
			mdb.encodeBuf[workerId] = resizeEncodeBuf(mdb.encodeBuf[workerId], len(item), szConf.allowLargeKeys)
			if keyToBeAdded, err = GetIndexEntryBytes2(item, docid, false, false,
				newKeyCount[i], mdb.idxDefn.Desc, mdb.encodeBuf[workerId], meta, szConf); err != nil {
				rollbackDeletes(len(indexEntriesToBeDeleted) - 1)
				rollbackAdds(i - 1)
				logging.Errorf("plasmaSlice::insertSecArrayIndex SliceId %v IndexInstId %v PartitionId %v Error forming entry "+
					"to be added to main index. Skipping docid:%s Error: %v",
					mdb.id, mdb.idxInstId, mdb.idxPartnId, logging.TagStrUD(docid), err)
				atomic.AddInt32(&mdb.numKeysSkipped, 1)
				mdb.deleteSecArrayIndexNoTx(docid, workerId)
				return 0
			}

			t0 := time.Now()
			err = mdb.main[workerId].InsertKV(keyToBeAdded, nil)
			mdb.idxStats.Timings.stKVSet.Put(time.Now().Sub(t0))

			if err == nil {
				mdb.idxStats.rawDataSize.Add(int64(len(keyToBeAdded)))
				addKeySizeStat(mdb.idxStats, len(keyToBeAdded))
				atomic.AddInt64(&mdb.insert_bytes, int64(len(keyToBeAdded)))
			}

			if int64(len(keyToBeAdded)) > atomic.LoadInt64(&mdb.maxKeySizeInLastInterval) {
				atomic.StoreInt64(&mdb.maxKeySizeInLastInterval, int64(len(keyToBeAdded)))
			}
			nmut++
		}
	}

	// If a field value changed from "existing" to "missing" (ie, key = nil),
	// we need to remove back index entry corresponding to the previous "existing" value.
	if key == nil {
		if oldkey != nil {
			t0 := time.Now()
			oldSz := len(oldkey)
			err := mdb.back[workerId].DeleteKV(docid)
			mdb.idxStats.Timings.stKVDelete.Put(time.Now().Sub(t0))

			if err == nil {
				mdb.idxStats.backstoreRawDataSize.Add(0 - int64(len(docid)+oldSz))
				mdb.idxStats.rawDataSize.Add(0 - int64(len(docid)+oldSz))
				subtractArrayKeySizeStat(mdb.idxStats, oldSz)
				atomic.AddInt64(&mdb.delete_bytes, int64(len(docid)))
			}
		}
	} else { //set the back index entry <docid, encodedkey>

		//convert to storage format
		if mdb.idxDefn.Desc != nil {
			_, err = jsonEncoder.ReverseCollate(key, mdb.idxDefn.Desc)
			if err != nil {
				// If error From ReverseCollate here, rollback adds, rollback deletes,
				// skip mutation, log and delete old key
				rollbackDeletes(len(indexEntriesToBeDeleted) - 1)
				rollbackAdds(len(indexEntriesToBeAdded) - 1)
				logging.Errorf("plasmaSlice::insertSecArrayIndex SliceId %v IndexInstId %v PartitionId %v."+
					"Error from ReverseCollate of new key. Skipping docid:%s Error: %v",
					mdb.id, mdb.idxInstId, mdb.idxPartnId, logging.TagStrUD(docid), err)
				mdb.deleteSecArrayIndexNoTx(docid, workerId)
				return 0
			}
		}

		if oldkey != nil {
			t0 := time.Now()
			oldSz := len(oldkey)
			err := mdb.back[workerId].DeleteKV(docid)
			mdb.idxStats.Timings.stKVDelete.Put(time.Now().Sub(t0))

			if err == nil {
				mdb.idxStats.backstoreRawDataSize.Add(0 - int64(len(docid)+oldSz))
				mdb.idxStats.rawDataSize.Add(0 - int64(len(docid)+oldSz))
				subtractArrayKeySizeStat(mdb.idxStats, oldSz)
				atomic.AddInt64(&mdb.delete_bytes, int64(len(docid)))
			}
		}

		t0 := time.Now()
		err = mdb.back[workerId].InsertKV(docid, key)
		mdb.idxStats.Timings.stKVSet.Put(time.Now().Sub(t0))

		if err == nil {
			mdb.idxStats.backstoreRawDataSize.Add(int64(len(docid) + len(key)))
			mdb.idxStats.rawDataSize.Add(int64(len(docid) + len(key)))
			addArrayKeySizeStat(mdb.idxStats, len(key))
			atomic.AddInt64(&mdb.insert_bytes, int64(len(docid)+len(key)))
		}
	}

	mdb.isDirty = true
	return nmut
}

func (mdb *plasmaSlice) delete(docid []byte, workerId int) int {
	var nmut int

	if mdb.isPrimary {
		nmut = mdb.deletePrimaryIndex(docid, workerId)
	} else if !mdb.idxDefn.IsArrayIndex {
		nmut, _ = mdb.deleteSecIndex(docid, nil, workerId)
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
		err1 := mdb.main[workerId].DeleteKV(itm)
		mdb.idxStats.Timings.stKVDelete.Put(time.Now().Sub(t0))

		if err1 == nil {
			mdb.idxStats.rawDataSize.Add(0 - int64(len(entry.Bytes())))
			atomic.AddInt64(&mdb.delete_bytes, int64(len(entry.Bytes())))
		}

		mdb.isDirty = true
		return 1
	}

	return 0
}

func (mdb *plasmaSlice) deleteSecIndex(docid []byte, compareKey []byte, workerId int) (ndel int, changed bool) {

	// Delete entry from back and main index if present
	mdb.back[workerId].Begin()
	defer mdb.back[workerId].End()

	backEntry, err := mdb.back[workerId].LookupKV(docid)

	mdb.encodeBuf[workerId] = resizeEncodeBuf(mdb.encodeBuf[workerId], len(backEntry), true)
	buf := mdb.encodeBuf[workerId]

	if err == nil {
		// Delete the entries only if the entry is different
		if hasEqualBackEntry(compareKey, backEntry) {
			return 0, false
		}

		t0 := time.Now()
		atomic.AddInt64(&mdb.delete_bytes, int64(len(docid)))
		mdb.main[workerId].Begin()
		defer mdb.main[workerId].End()
		err = mdb.back[workerId].DeleteKV(docid)
		if err == nil {
			mdb.idxStats.backstoreRawDataSize.Add(0 - int64(len(docid)+len(backEntry)))
			mdb.idxStats.rawDataSize.Add(0 - int64(len(docid)+len(backEntry)))
		}

		entry := backEntry2entry(docid, backEntry, buf, mdb.keySzConf[workerId])
		entrySz := len(entry)
		err = mdb.main[workerId].DeleteKV(entry)
		mdb.idxStats.Timings.stKVDelete.Put(time.Since(t0))

		if err == nil {
			mdb.idxStats.rawDataSize.Add(0 - int64(entrySz))
			subtractKeySizeStat(mdb.idxStats, entrySz)
		}
	}

	mdb.isDirty = true
	return 1, true
}

func (mdb *plasmaSlice) deleteSecArrayIndex(docid []byte, workerId int) (nmut int) {

	mdb.back[workerId].Begin()
	defer mdb.back[workerId].End()

	mdb.main[workerId].Begin()
	defer mdb.main[workerId].End()

	return mdb.deleteSecArrayIndexNoTx(docid, workerId)
}

func (mdb *plasmaSlice) deleteSecArrayIndexNoTx(docid []byte, workerId int) (nmut int) {
	var olditm []byte
	var err error

	szConf := mdb.updateSliceBuffers(workerId)

	olditm, err = mdb.back[workerId].LookupKV(docid)
	if err == plasma.ErrItemNotFound {
		olditm = nil
	}

	if olditm == nil {
		return
	}

	var tmpBuf []byte
	if len(olditm)*3 > cap(mdb.arrayBuf1[workerId]) {
		tmpBuf = make([]byte, 0, len(olditm)*3)
	} else {
		tmpBuf = mdb.arrayBuf1[workerId]
	}

	//get the key in original form
	if mdb.idxDefn.Desc != nil {
		_, err = jsonEncoder.ReverseCollate(olditm, mdb.idxDefn.Desc)
		// If error From ReverseCollate here, crash as it is old key
		common.CrashOnError(err)
	}

	indexEntriesToBeDeleted, keyCount, _, err := ArrayIndexItems(olditm, mdb.arrayExprPosition,
		tmpBuf, mdb.isArrayDistinct, false, szConf)
	if err != nil {
		// TODO: Do not crash for non-storage operation. Force delete the old entries
		common.CrashOnError(err)
		logging.Errorf("plasmaSlice::deleteSecArrayIndex \n\tSliceId %v IndexInstId %v PartitionId %v Error in retrieving "+
			"compostite old secondary keys %v", mdb.id, mdb.idxInstId, mdb.idxPartnId, err)
		return
	}

	var t0 time.Time
	// Delete each of indexEntriesToBeDeleted from main index
	for i, item := range indexEntriesToBeDeleted {
		var keyToBeDeleted []byte
		var tmpBuf []byte
		tmpBuf = resizeEncodeBuf(mdb.encodeBuf[workerId], len(item), true)
		// TODO: Use method that skips size check for bug MB-22183
		if keyToBeDeleted, err = GetIndexEntryBytes3(item, docid, false, false, keyCount[i],
			mdb.idxDefn.Desc, tmpBuf, nil, szConf); err != nil {
			common.CrashOnError(err)
			logging.Errorf("plasmaSlice::deleteSecArrayIndex \n\tSliceId %v IndexInstId %v PartitionId %v Error from GetIndexEntryBytes2 "+
				"for entry to be deleted from main index %v", mdb.id, mdb.idxInstId, mdb.idxPartnId, err)
			return
		}
		t0 := time.Now()
		keyDelSz := len(keyToBeDeleted)
		err = mdb.main[workerId].DeleteKV(keyToBeDeleted)
		mdb.idxStats.Timings.stKVDelete.Put(time.Now().Sub(t0))

		if err == nil {
			mdb.idxStats.rawDataSize.Add(0 - int64(keyDelSz))
			subtractKeySizeStat(mdb.idxStats, keyDelSz)
			atomic.AddInt64(&mdb.delete_bytes, int64(keyDelSz))
		}
	}

	//delete from the back index
	t0 = time.Now()
	oldSz := len(olditm)
	err = mdb.back[workerId].DeleteKV(docid)
	mdb.idxStats.Timings.stKVDelete.Put(time.Now().Sub(t0))

	if err == nil {
		mdb.idxStats.backstoreRawDataSize.Add(0 - int64(len(docid)+oldSz))
		mdb.idxStats.rawDataSize.Add(0 - int64(len(docid)+oldSz))
		subtractArrayKeySizeStat(mdb.idxStats, oldSz)
		atomic.AddInt64(&mdb.delete_bytes, int64(len(docid)))
	}

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

	IndexStats map[string]interface{}
	Version    int
	InstId     common.IndexInstId
	PartnId    common.PartitionId
}

type plasmaSnapshot struct {
	slice      *plasmaSlice
	idxDefnId  common.IndexDefnId
	idxInstId  common.IndexInstId
	idxPartnId common.PartitionId
	ts         *common.TsVbuuid
	info       *plasmaSnapshotInfo

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
		idxDefnId:  mdb.idxDefnId,
		idxInstId:  mdb.idxInstId,
		idxPartnId: mdb.idxPartnId,
		info:       snapInfo,
		ts:         snapInfo.Timestamp(),
		committed:  info.IsCommitted(),
		MainSnap:   mdb.mainstore.NewSnapshot(),
	}

	if !mdb.isPrimary {
		s.BackSnap = mdb.backstore.NewSnapshot()
	}

	s.Open()
	s.slice.IncrRef()
	s.slice.idxStats.numOpenSnapshots.Add(1)

	if s.committed && mdb.hasPersistence {
		mdb.doPersistSnapshot(s)
	}

	if info.IsCommitted() {
		logging.Infof("plasmaSlice::OpenSnapshot SliceId %v IndexInstId %v PartitionId %v Creating New "+
			"Snapshot %v", mdb.id, mdb.idxInstId, mdb.idxPartnId, snapInfo)

		// Reset buffer sizes periodically
		mdb.periodicSliceBuffersReset()

		// Check if there are errors that need to be logged to console
		mdb.logErrorsToConsole()
	}
	mdb.setCommittedCount()

	return s, nil
}

var plasmaPersistenceMutex sync.Mutex

func (mdb *plasmaSlice) doPersistSnapshot(s *plasmaSnapshot) {
	if atomic.CompareAndSwapInt32(&mdb.isPersistorActive, 0, 1) {
		s.MainSnap.Open()
		if !mdb.isPrimary {
			s.BackSnap.Open()
		}

		go func() {
			defer atomic.StoreInt32(&mdb.isPersistorActive, 0)

			logging.Infof("PlasmaSlice Slice Id %v, IndexInstId %v, PartitionId %v "+
				"Creating recovery point ...", mdb.id, mdb.idxInstId, mdb.idxPartnId)
			t0 := time.Now()

			snapshotStats := make(map[string]interface{})
			snapshotStats[SNAP_STATS_KEY_SIZES] = getKeySizesStats(mdb.idxStats)
			snapshotStats[SNAP_STATS_ARRKEY_SIZES] = getArrayKeySizesStats(mdb.idxStats)
			snapshotStats[SNAP_STATS_KEY_SIZES_SINCE] = mdb.idxStats.keySizeStatsSince.Value()
			snapshotStats[SNAP_STATS_RAW_DATA_SIZE] = mdb.idxStats.rawDataSize.Value()
			snapshotStats[SNAP_STATS_BACKSTORE_RAW_DATA_SIZE] = mdb.idxStats.backstoreRawDataSize.Value()
			s.info.IndexStats = snapshotStats
			s.info.Version = SNAPSHOT_META_VERSION_PLASMA_1
			s.info.InstId = mdb.idxInstId
			s.info.PartnId = mdb.idxPartnId

			meta, err := json.Marshal(s.info)
			common.CrashOnError(err)
			timeHdr := make([]byte, 8)
			binary.BigEndian.PutUint64(timeHdr, uint64(time.Now().UnixNano()))
			meta = append(timeHdr, meta...)

			// To prevent persistence from eating up all the disk bandwidth
			// and slowing down query, we wish to ensure that only 1 instance
			// gets persisted at once across all instances on this node.
			// Since both main and back snapshots are open, we wish to ensure
			// that serialization of the main and back index persistence happens
			// only via this callback to ensure that neither of these snapshots
			// are held open until the other completes recovery point creation.
			tokenCh := make(chan bool, 1) // To locally serialize main & back
			tokenCh <- true
			serializePersistence := func(s *plasma.Plasma) error {
				<-tokenCh
				plasmaPersistenceMutex.Lock()
				return nil
			}

			var concurr int = int(float32(runtime.GOMAXPROCS(0))*float32(mdb.sysconf["plasma.persistenceCPUPercent"].Int())/(100*2) + 0.75)

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				mErr := mdb.mainstore.CreateRecoveryPoint(s.MainSnap, meta,
					concurr, serializePersistence)

				if mErr != nil {
					logging.Infof("PlasmaSlice Slice Id %v, IndexInstId %v, PartitionId %v: "+
						"Failed to create mainstore recovery point: %v",
						mdb.id, mdb.idxInstId, mdb.idxPartnId, mErr)
				}

				tokenCh <- true
				plasmaPersistenceMutex.Unlock()
				wg.Done()
			}()

			if !mdb.isPrimary {
				bErr := mdb.backstore.CreateRecoveryPoint(s.BackSnap, meta, concurr,
					serializePersistence)

				if bErr != nil {
					logging.Infof("PlasmaSlice Slice Id %v, IndexInstId %v, PartitionId %v: "+
						"Failed to create backstore recovery point: %v",
						mdb.id, mdb.idxInstId, mdb.idxPartnId, bErr)
				}

				tokenCh <- true
				plasmaPersistenceMutex.Unlock()
			}
			wg.Wait()

			dur := time.Since(t0)
			logging.Infof("PlasmaSlice Slice Id %v, IndexInstId %v, PartitionId %v "+
				"Created recovery point (took %v)", mdb.id, mdb.idxInstId, mdb.idxPartnId, dur)

			mdb.idxStats.diskSnapStoreDuration.Set(int64(dur / time.Millisecond))

			// In case there is an error creating one of recovery
			// points, the successful one has to be cleaned up.
			mdb.removeNotCommonRecoveryPoints()

			mdb.cleanupOldRecoveryPoints(s.info)

		}()
	} else {
		logging.Infof("PlasmaSlice Slice Id %v, IndexInstId %v, PartitionId %v Skipping ondisk"+
			" snapshot. A snapshot writer is in progress.", mdb.id, mdb.idxInstId, mdb.idxPartnId)
	}
}

// Find rps that are present in only one of mainstore and
// backstore and remove them.
func (mdb *plasmaSlice) removeNotCommonRecoveryPoints() {
	if !mdb.isPrimary {
		mRPs := mdb.mainstore.GetRecoveryPoints()
		bRPs := mdb.backstore.GetRecoveryPoints()

		for _, rp := range setDifferenceRPs(mRPs, bRPs) {
			mdb.mainstore.RemoveRecoveryPoint(rp)
		}

		for _, rp := range setDifferenceRPs(bRPs, mRPs) {
			mdb.backstore.RemoveRecoveryPoint(rp)
		}
	}
}

// Find xRPs - yRPs: rps that are in xRPs, but not in yRPs.
func setDifferenceRPs(xRPs, yRPs []*plasma.RecoveryPoint) []*plasma.RecoveryPoint {
	var onlyInX []*plasma.RecoveryPoint

	for _, xRP := range xRPs {
		isInY := false
		for _, yRP := range yRPs {
			if cmpRPMeta(xRP.Meta(), yRP.Meta()) == 0 {
				isInY = true
				break
			}
		}

		if !isInY {
			onlyInX = append(onlyInX, xRP)
		}
	}

	return onlyInX
}

func (mdb *plasmaSlice) cleanupOldRecoveryPoints(sinfo *plasmaSnapshotInfo) {

	var seqTs Timestamp

	if !sinfo.IsOSOSnap() {

		seqTs = NewTimestamp(mdb.numVbuckets)
		for i := 0; i < MAX_GETSEQS_RETRIES; i++ {

			seqnos, err := common.BucketMinSeqnos(mdb.clusterAddr, "default", mdb.idxDefn.Bucket)
			if err != nil {
				logging.Errorf("PlasmaSlice Slice Id %v, IndexInstId %v, PartitionId %v "+
					"Error collecting cluster seqnos %v",
					mdb.id, mdb.idxInstId, mdb.idxPartnId, err)
				time.Sleep(time.Second)
				continue
			}

			for i := 0; i < mdb.numVbuckets; i++ {
				seqTs[i] = seqnos[i]
			}
			break

		}

	}

	//discard old disk snapshots for OSO as those cannot be
	//used for recovery
	maxRollbacks := mdb.maxRollbacks
	maxDiskSnaps := mdb.maxDiskSnaps
	if sinfo.IsOSOSnap() {
		maxRollbacks = 1
		maxDiskSnaps = 1
	}

	// Cleanup old recovery points
	mRPs := mdb.mainstore.GetRecoveryPoints()
	if len(mRPs) > maxRollbacks {
		for i := 0; i < len(mRPs)-maxRollbacks; i++ {

			snapInfo, err := mdb.getRPSnapInfo(mRPs[i])
			if err != nil {
				logging.Errorf("PlasmaSlice Slice Id %v, IndexInstId %v, PartitionId %v "+
					"Skipped recovery point cleanup. err %v",
					mdb.id, mdb.idxInstId, mdb.idxPartnId, err)
				continue
			}
			snapTsVbuuid := snapInfo.Timestamp()
			snapTs := getSeqTsFromTsVbuuid(snapTsVbuuid)

			if (seqTs.GreaterThanEqual(snapTs) && //min cluster seqno is greater than snap ts
				mdb.lastRollbackTs == nil) || //last rollback was successful
				len(mRPs)-i > maxDiskSnaps { //num RPs is more than max disk snapshots
				logging.Infof("PlasmaSlice Slice Id %v, IndexInstId %v, PartitionId %v "+
					"Cleanup mainstore recovery point %v. num RPs %v.", mdb.id, mdb.idxInstId,
					mdb.idxPartnId, snapInfo, len(mRPs)-i)
				mdb.mainstore.RemoveRecoveryPoint(mRPs[i])
			} else {
				logging.Infof("PlasmaSlice Slice Id %v, IndexInstId %v, PartitionId %v "+
					"Skipped mainstore recovery point cleanup. num RPs %v ",
					mdb.id, mdb.idxInstId, mdb.idxPartnId, len(mRPs)-i)
				break
			}
		}
	}

	if !mdb.isPrimary {
		bRPs := mdb.backstore.GetRecoveryPoints()
		if len(bRPs) > maxRollbacks {
			for i := 0; i < len(bRPs)-maxRollbacks; i++ {

				snapInfo, err := mdb.getRPSnapInfo(bRPs[i])
				if err != nil {
					logging.Errorf("PlasmaSlice Slice Id %v, IndexInstId %v, PartitionId %v "+
						"Skipped recovery point cleanup. err %v",
						mdb.id, mdb.idxInstId, mdb.idxPartnId, err)
					continue
				}
				snapTsVbuuid := snapInfo.Timestamp()
				snapTs := getSeqTsFromTsVbuuid(snapTsVbuuid)
				if (seqTs.GreaterThanEqual(snapTs) && //min cluster seqno is greater than snap ts
					mdb.lastRollbackTs == nil) || //last rollback was successful
					len(bRPs)-i > maxDiskSnaps { //num RPs is more than max disk snapshots
					logging.Infof("PlasmaSlice Slice Id %v, IndexInstId %v, PartitionId %v "+
						"Cleanup backstore recovery point %v. num RPs %v.", mdb.id, mdb.idxInstId,
						mdb.idxPartnId, snapInfo, len(bRPs)-i)
					mdb.backstore.RemoveRecoveryPoint(bRPs[i])
				} else {
					logging.Infof("PlasmaSlice Slice Id %v, IndexInstId %v, PartitionId %v "+
						"Skipped backstore recovery point cleanup. num RPs %v ",
						mdb.id, mdb.idxInstId, mdb.idxPartnId, len(bRPs)-i)
					break
				}

			}
		}
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
	} else {
		return nil, nil
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
		info, err := mdb.getRPSnapInfo(mRPs[i])
		if err != nil {
			return nil, err
		}

		if !mdb.isPrimary {
			info.bRP = bRPs[i]
		}

		infos = append(infos, info)
	}

	return infos, nil
}

func (mdb *plasmaSlice) getRPSnapInfo(rp *plasma.RecoveryPoint) (*plasmaSnapshotInfo, error) {

	info := &plasmaSnapshotInfo{
		mRP:   rp,
		Count: rp.ItemsCount(),
	}

	var snapMeta map[string]interface{}
	if err := json.Unmarshal(info.mRP.Meta()[8:], &snapMeta); err != nil {
		return nil, fmt.Errorf("Unable to decode snapshot meta err %v", err)
	}

	if _, ok := snapMeta["Version"]; ok {
		// new format
		var err error
		var snapInfo plasmaSnapshotInfo
		if err = json.Unmarshal(info.mRP.Meta()[8:], &snapInfo); err != nil {
			return nil, fmt.Errorf("Unable to decode snapshot info from meta. err %v", err)
		}
		info.Ts = snapInfo.Ts
		info.IndexStats = snapInfo.IndexStats
	} else {
		// old format
		if err := json.Unmarshal(info.mRP.Meta()[8:], &info.Ts); err != nil {
			return nil, fmt.Errorf("Unable to decode snapshot meta err %v", err)
		}
	}

	return info, nil
}

func (mdb *plasmaSlice) setCommittedCount() {
	curr := mdb.mainstore.ItemsCount()
	atomic.StoreUint64(&mdb.committedCount, uint64(curr))
}

func (mdb *plasmaSlice) GetCommittedCount() uint64 {
	return atomic.LoadUint64(&mdb.committedCount)
}

func (mdb *plasmaSlice) resetStores() error {
	// Clear all readers
	for i := 0; i < cap(mdb.readers); i++ {
		<-mdb.readers
	}

	numWriters := mdb.numWriters
	mdb.freeAllWriters()

	mdb.mainstore.Close()
	if !mdb.isPrimary {
		mdb.backstore.Close()
	}

	if err := plasma.DestroyInstance(mdb.storageDir, mdb.path); err != nil {
		return err
	}

	mdb.newBorn = true
	if err := mdb.initStores(); err != nil {
		return err
	}

	mdb.startWriters(numWriters)
	mdb.setCommittedCount()

	mdb.resetStats()

	return nil
}

func (mdb *plasmaSlice) resetStats() {

	mdb.idxStats.itemsCount.Set(0)

	resetKeySizeStats(mdb.idxStats)
	resetArrKeySizeStats(mdb.idxStats)
	// Slice is rolling back to zero, but there is no need to update keySizeStatsSince

	mdb.idxStats.backstoreRawDataSize.Set(0)
	mdb.idxStats.rawDataSize.Set(0)

	mdb.idxStats.lastDiskBytes.Set(0)
	mdb.idxStats.lastNumItemsFlushed.Set(0)
	mdb.idxStats.lastNumDocsIndexed.Set(0)
	mdb.idxStats.lastNumFlushQueued.Set(0)
	mdb.idxStats.lastMutateGatherTime.Set(0)

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

	// Update stats available in snapshot info
	mdb.updateStatsFromSnapshotMeta(o)
	return nil
}

// Update stats available in snapshot info
func (mdb *plasmaSlice) updateStatsFromSnapshotMeta(o SnapshotInfo) {

	// Update stats *if* available in snapshot info
	// In case of upgrade, older snapshots will not have stats
	// in which case, do not update index stats
	// Older snapshots may have only subset of stats persisted
	// For stats not available through persistence, set default values
	stats := o.Stats()
	if stats != nil {
		keySizes := stats[SNAP_STATS_KEY_SIZES].([]interface{})
		arrkeySizes := stats[SNAP_STATS_ARRKEY_SIZES].([]interface{})

		if keySizes != nil && len(keySizes) == 6 {
			mdb.idxStats.numKeySize64.Set(safeGetInt64(keySizes[0]))
			mdb.idxStats.numKeySize256.Set(safeGetInt64(keySizes[1]))
			mdb.idxStats.numKeySize1K.Set(safeGetInt64(keySizes[2]))
			mdb.idxStats.numKeySize4K.Set(safeGetInt64(keySizes[3]))
			mdb.idxStats.numKeySize100K.Set(safeGetInt64(keySizes[4]))
			mdb.idxStats.numKeySizeGt100K.Set(safeGetInt64(keySizes[5]))
		}

		if arrkeySizes != nil && len(arrkeySizes) == 6 {
			mdb.idxStats.numArrayKeySize64.Set(safeGetInt64(arrkeySizes[0]))
			mdb.idxStats.numArrayKeySize256.Set(safeGetInt64(arrkeySizes[1]))
			mdb.idxStats.numArrayKeySize1K.Set(safeGetInt64(arrkeySizes[2]))
			mdb.idxStats.numArrayKeySize4K.Set(safeGetInt64(arrkeySizes[3]))
			mdb.idxStats.numArrayKeySize100K.Set(safeGetInt64(arrkeySizes[4]))
			mdb.idxStats.numArrayKeySizeGt100K.Set(safeGetInt64(arrkeySizes[5]))
		}

		mdb.idxStats.rawDataSize.Set(safeGetInt64(stats[SNAP_STATS_RAW_DATA_SIZE]))
		mdb.idxStats.backstoreRawDataSize.Set(safeGetInt64(stats[SNAP_STATS_BACKSTORE_RAW_DATA_SIZE]))

		mdb.idxStats.keySizeStatsSince.Set(safeGetInt64(stats[SNAP_STATS_KEY_SIZES_SINCE]))
	} else {
		// Since stats are not available, update keySizeStatsSince to current time
		// to indicate we start tracking the stat since now.
		mdb.idxStats.keySizeStatsSince.Set(time.Now().UnixNano())
	}
}

//RollbackToZero rollbacks the slice to initial state. Return error if
//not possible
func (mdb *plasmaSlice) RollbackToZero() error {
	mdb.waitPersist()
	mdb.waitForPersistorThread()

	if err := mdb.resetStores(); err != nil {
		return err
	}

	mdb.lastRollbackTs = nil

	return nil
}

func (mdb *plasmaSlice) LastRollbackTs() *common.TsVbuuid {
	return mdb.lastRollbackTs
}

func (mdb *plasmaSlice) SetLastRollbackTs(ts *common.TsVbuuid) {
	mdb.lastRollbackTs = ts
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

	// Coming here means that cmdCh is empty and flush has finished for this index
	atomic.StoreUint32(&mdb.flushActive, 0)

	newSnapshotInfo := &plasmaSnapshotInfo{
		Ts:        ts,
		Committed: commit,
		Count:     mdb.mainstore.ItemsCount(),
	}

	return newSnapshotInfo, nil
}

func (mdb *plasmaSlice) FlushDone() {

	if !mdb.enableWriterTuning {
		return
	}

	mdb.waitPersist()

	qc := atomic.LoadInt64(&mdb.qCount)
	if qc > 0 {
		common.CrashOnError(errors.New("Slice Invariant Violation - commit with pending mutations"))
	}

	// Adjust the number of writers at inmemory snapshot or persisted snapshot
	mdb.adjustWriters()
}

//checkAllWorkersDone return true if all workers have
//finished processing
func (mdb *plasmaSlice) checkAllWorkersDone() bool {

	//if there are mutations in the cmdCh, workers are
	//not yet done
	if mdb.getCmdsCount() > 0 {
		return false
	}

	return true
}

func (mdb *plasmaSlice) Close() {
	mdb.lock.Lock()
	defer mdb.lock.Unlock()

	//signal shutdown for command handler routines
	mdb.cleanupWritersOnClose()

	if mdb.refCount > 0 {
		mdb.isSoftClosed = true
		logging.Infof("plasmaSlice::Close Soft Closing Slice Id %v, IndexInstId %v, PartitionId %v, "+
			"IndexDefnId %v", mdb.id, mdb.idxInstId, mdb.idxPartnId, mdb.idxDefnId)

	} else {
		tryCloseplasmaSlice(mdb)
	}
}

func (mdb *plasmaSlice) cleanupWritersOnClose() {

	mdb.token.increment(mdb.numWriters)

	mdb.freeAllWriters()
	close(mdb.samplerStopCh)
}

//Destroy removes the database file from disk.
//Slice is not recoverable after this.
func (mdb *plasmaSlice) Destroy() {
	mdb.lock.Lock()
	defer mdb.lock.Unlock()

	if mdb.refCount > 0 {
		openSnaps := mdb.idxStats.numOpenSnapshots.Value()
		logging.Infof("plasmaSlice::Destroy Soft deleted Slice Id %v, IndexInstId %v, PartitionId %v "+
			"IndexDefnId %v RefCount %v NumOpenSnapshots %v", mdb.id, mdb.idxInstId, mdb.idxPartnId,
			mdb.idxDefnId, mdb.refCount, openSnaps)
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
//
// flushActive will be true if there are going to be any
// messages in the cmdCh of slice after flush is done.
// It will be cleared during snapshot generation as the
// cmdCh would be empty at the time of snapshot generation
func (mdb *plasmaSlice) IsDirty() bool {
	flushActive := atomic.LoadUint32(&mdb.flushActive)
	if flushActive == 0 { // No flush happening
		return false
	}

	// Flush in progress - wait till all commands on cmdCh
	// are processed
	mdb.waitPersist()
	return mdb.isDirty
}

func (mdb *plasmaSlice) IsCompacting() bool {
	mdb.lock.Lock()
	defer mdb.lock.Unlock()
	return mdb.isCompacting
}

func (mdb *plasmaSlice) SetCompacting(compacting bool) {
	mdb.lock.Lock()
	defer mdb.lock.Unlock()
	mdb.isCompacting = compacting
}

func (mdb *plasmaSlice) IsSoftDeleted() bool {
	mdb.lock.Lock()
	defer mdb.lock.Unlock()
	return mdb.isSoftDeleted
}

func (mdb *plasmaSlice) IsSoftClosed() bool {
	mdb.lock.Lock()
	defer mdb.lock.Unlock()
	return mdb.isSoftClosed
}

func (mdb *plasmaSlice) Compact(abortTime time.Time, minFrag int) error {

	if mdb.IsCompacting() {
		return nil
	}

	var err error
	var wg sync.WaitGroup

	mdb.SetCompacting(true)
	defer mdb.SetCompacting(false)

	wg.Add(1)
	go func() {
		defer wg.Done()

		if mdb.mainstore.AutoLSSCleaning {
			return
		}

		shouldClean := func() bool {
			if mdb.IsSoftDeleted() || mdb.IsSoftClosed() {
				return false
			}
			return mdb.mainstore.TriggerLSSCleaner(minFrag, mdb.mainstore.LSSCleanerMinSize)
		}

		err = mdb.mainstore.CleanLSS(shouldClean)
	}()

	if !mdb.isPrimary && mdb.backstore != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()

			if mdb.backstore.AutoLSSCleaning {
				return
			}

			shouldClean := func() bool {
				if mdb.IsSoftDeleted() || mdb.IsSoftClosed() {
					return false
				}
				return mdb.backstore.TriggerLSSCleaner(minFrag, mdb.backstore.LSSCleanerMinSize)
			}

			err = mdb.backstore.CleanLSS(shouldClean)
		}()
	}

	wg.Wait()

	return err
}

func (mdb *plasmaSlice) PrepareStats() {
	plasma.PrepareStats()
}

func (mdb *plasmaSlice) Statistics(consumerFilter uint64) (StorageStatistics, error) {

	if consumerFilter == statsMgmt.N1QLStorageStatsFilter {
		return mdb.handleN1QLStorageStatistics()
	}

	var sts StorageStatistics

	var internalData []string

	var numRecsMem, numRecsDisk, cacheHits, cacheMiss, docidCount int64
	var msCompressionRatio, bsCompressionRatio float64
	pStats := mdb.mainstore.GetPreparedStats()

	docidCount = pStats.ItemsCount
	numRecsMem += pStats.NumRecordAllocs - pStats.NumRecordFrees
	numRecsDisk += pStats.NumRecordSwapOut - pStats.NumRecordSwapIn
	cacheHits += pStats.CacheHits
	cacheMiss += pStats.CacheMisses
	sts.MemUsed = pStats.MemSz + pStats.MemSzIndex
	sts.InsertBytes = pStats.BytesWritten
	sts.GetBytes = pStats.LSSBlkReadBytes
	checkpointFileSize := pStats.CheckpointSize
	msCompressionRatio = getCompressionRatio(pStats)

	mainStoreStatsLoggingEnabled := false
	backStoreStatsLoggingEnabled := false

	if pStats.StatsLoggingEnabled || (consumerFilter == statsMgmt.AllStatsFilter) {
		mainStoreStatsLoggingEnabled = true
		internalData = append(internalData, fmt.Sprintf("{\n\"MainStore\":\n%s", pStats))
	}

	if !mdb.isPrimary {
		pStats := mdb.backstore.GetPreparedStats()
		docidCount = pStats.ItemsCount
		sts.MemUsed += pStats.MemSz + pStats.MemSzIndex
		if pStats.StatsLoggingEnabled || (consumerFilter == statsMgmt.AllStatsFilter) {
			backStoreStatsLoggingEnabled = true
			if mainStoreStatsLoggingEnabled {
				internalData = append(internalData, fmt.Sprintf(",\n\"BackStore\":\n%s", pStats))
			} else {
				internalData = append(internalData, fmt.Sprintf("{\n\"BackStore\":\n%s", pStats))
			}
		}
		sts.InsertBytes += pStats.BytesWritten
		sts.GetBytes += pStats.LSSBlkReadBytes
		checkpointFileSize += pStats.CheckpointSize
		bsCompressionRatio = getCompressionRatio(pStats)
	}

	if mainStoreStatsLoggingEnabled || backStoreStatsLoggingEnabled {
		internalData = append(internalData, "}\n")
	}

	sts.InternalData = internalData
	if mdb.hasPersistence {
		sts.DiskSize = mdb.mainstore.LSSDiskSize()
		sts.DataSizeOnDisk = mdb.mainstore.LSSDataSize()
		sts.LogSpace = mdb.mainstore.LSSUsedSpace()
		sts.DataSize = (int64)((float64)(sts.DataSizeOnDisk) * msCompressionRatio)
		if !mdb.isPrimary {
			bsDiskSz := mdb.backstore.LSSDiskSize()
			sts.DiskSize += bsDiskSz
			bsDataSize := mdb.backstore.LSSDataSize()
			bsLogSpace := mdb.backstore.LSSUsedSpace()
			sts.DataSizeOnDisk += bsDataSize
			sts.DataSize += (int64)((float64)(bsDataSize) * bsCompressionRatio)
			sts.LogSpace += bsLogSpace
		}
		sts.DiskSize += checkpointFileSize
	}

	mdb.idxStats.docidCount.Set(docidCount)
	mdb.idxStats.residentPercent.Set(common.ComputePercent(numRecsMem, numRecsDisk))
	mdb.idxStats.cacheHitPercent.Set(common.ComputePercent(cacheHits, cacheMiss))
	mdb.idxStats.cacheHits.Set(cacheHits)
	mdb.idxStats.cacheMisses.Set(cacheMiss)
	mdb.idxStats.numRecsInMem.Set(numRecsMem)
	mdb.idxStats.numRecsOnDisk.Set(numRecsDisk)
	return sts, nil
}

func (mdb *plasmaSlice) handleN1QLStorageStatistics() (StorageStatistics, error) {
	var sts StorageStatistics

	pstats := mdb.mainstore.GetPreparedStats()
	var avg_item_size, avg_page_size int64
	if atomic.LoadInt64(&pstats.ItemCnt) > 0 {
		avg_item_size = atomic.LoadInt64(&pstats.PageBytes) / atomic.LoadInt64(&pstats.ItemCnt)
	}
	if atomic.LoadInt64(&pstats.PageCnt) > 0 {
		avg_page_size = atomic.LoadInt64(&pstats.PageBytes) / atomic.LoadInt64(&pstats.PageCnt)
	}
	internalData := fmt.Sprintf("{\n\"MainStore\":\n"+
		"{\n"+
		"\"num_pages\":%d,\n"+
		"\"items_count\":%d,\n"+
		"\"resident_ratio\":%.5f,\n"+
		"\"inserts\":%d,\n"+
		"\"deletes\":%d,\n"+
		"\"avg_item_size\":%d,\n"+
		"\"avg_page_size\":%d\n}\n}",
		pstats.NumPages,
		pstats.ItemsCount,
		pstats.ResidentRatio,
		pstats.Inserts,
		pstats.Deletes,
		avg_item_size,
		avg_page_size)

	sts.InternalData = []string{internalData}
	return sts, nil
}

func updatePlasmaConfig(cfg common.Config) {
	plasma.MTunerMaxFreeMemory = int64(cfg["plasma.memtuner.maxFreeMemory"].Int())
	plasma.MTunerMinFreeMemRatio = cfg["plasma.memtuner.minFreeRatio"].Float64()
	plasma.MTunerTrimDownRatio = cfg["plasma.memtuner.trimDownRatio"].Float64()
	plasma.MTunerIncrementRatio = cfg["plasma.memtuner.incrementRatio"].Float64()
	plasma.MTunerMinQuotaRatio = cfg["plasma.memtuner.minQuotaRatio"].Float64()
	plasma.MTunerOvershootRatio = cfg["plasma.memtuner.overshootRatio"].Float64()
	plasma.MTunerIncrCeilPercent = cfg["plasma.memtuner.incrCeilPercent"].Float64()
	plasma.MTunerMinQuota = int64(cfg["plasma.memtuner.minQuota"].Int())
	plasma.MFragThreshold = cfg["plasma.memFragThreshold"].Float64()
}

func (mdb *plasmaSlice) UpdateConfig(cfg common.Config) {
	mdb.confLock.Lock()
	defer mdb.confLock.Unlock()

	oldCfg := mdb.sysconf
	mdb.sysconf = cfg

	updatePlasmaConfig(cfg)
	mdb.mainstore.AutoTuneLSSCleaning = cfg["plasma.AutoTuneLSSCleaner"].Bool()
	mdb.mainstore.AutoTuneDiskQuota = int64(cfg["plasma.AutoTuneDiskQuota"].Uint64())
	mdb.mainstore.AutoTuneCleanerTargetFragRatio = cfg["plasma.AutoTuneCleanerTargetFragRatio"].Int()
	mdb.mainstore.AutoTuneCleanerMinBandwidthRatio = cfg["plasma.AutoTuneCleanerMinBandwidthRatio"].Float64()
	mdb.mainstore.AutoTuneDiskFullTimeLimit = cfg["plasma.AutoTuneDiskFullTimeLimit"].Int()
	mdb.mainstore.AutoTuneAvailDiskLimit = cfg["plasma.AutoTuneAvailDiskLimit"].Float64()
	mdb.mainstore.MaxPageSize = cfg["plasma.MaxPageSize"].Int()
	mdb.mainstore.EnforceKeyRange = cfg["plasma.enforceKeyRange"].Bool()

	mdb.mainstore.CheckpointInterval = time.Second * time.Duration(cfg["plasma.checkpointInterval"].Int())
	mdb.mainstore.MaxPageLSSSegments = mdb.sysconf["plasma.mainIndex.maxLSSPageSegments"].Int()
	mdb.mainstore.LSSCleanerThreshold = mdb.sysconf["plasma.mainIndex.LSSFragmentation"].Int()
	mdb.mainstore.LSSCleanerMaxThreshold = mdb.sysconf["plasma.mainIndex.maxLSSFragmentation"].Int()
	mdb.mainstore.DisableReadCaching = mdb.sysconf["plasma.disableReadCaching"].Bool()
	mdb.mainstore.EnablePeriodicEvict = mdb.sysconf["plasma.mainIndex.enablePeriodicEvict"].Bool()
	mdb.mainstore.EvictMinThreshold = mdb.sysconf["plasma.mainIndex.evictMinThreshold"].Float64()
	mdb.mainstore.EvictMaxThreshold = mdb.sysconf["plasma.mainIndex.evictMaxThreshold"].Float64()
	mdb.mainstore.EvictDirtyOnPersistRatio = mdb.sysconf["plasma.mainIndex.evictDirtyOnPersistRatio"].Float64()
	mdb.mainstore.EvictDirtyPercent = mdb.sysconf["plasma.mainIndex.evictDirtyPercent"].Float64()
	mdb.mainstore.EvictSweepInterval = time.Duration(mdb.sysconf["plasma.mainIndex.evictSweepInterval"].Int()) * time.Second
	mdb.mainstore.EvictRunInterval = time.Duration(mdb.sysconf["plasma.mainIndex.evictRunInterval"].Int()) * time.Millisecond
	mdb.mainstore.EvictUseMemEstimate = mdb.sysconf["plasma.mainIndex.evictUseMemEstimate"].Bool()

	mdb.mainstore.PurgerInterval = time.Duration(mdb.sysconf["plasma.purger.interval"].Int()) * time.Second
	mdb.mainstore.PurgeThreshold = mdb.sysconf["plasma.purger.highThreshold"].Float64()
	mdb.mainstore.PurgeLowThreshold = mdb.sysconf["plasma.purger.lowThreshold"].Float64()
	mdb.mainstore.PurgeCompactRatio = mdb.sysconf["plasma.purger.compactRatio"].Float64()
	mdb.mainstore.EnableLSSPageSMO = mdb.sysconf["plasma.enableLSSPageSMO"].Bool()
	mdb.mainstore.PageStatsSamplePercent = mdb.sysconf["plasma.PageStatsSamplePercent"].Float64()

	mdb.mainstore.EnablePageBloomFilter = mdb.sysconf["plasma.mainIndex.enablePageBloomFilter"].Bool()
	mdb.mainstore.BloomFilterFalsePositiveRate = mdb.sysconf["plasma.mainIndex.bloomFilterFalsePositiveRate"].Float64()
	mdb.mainstore.BloomFilterExpectedMaxItems = mdb.sysconf["plasma.mainIndex.bloomFilterExpectedMaxItems"].Uint64()

	mdb.mainstore.MaxInstsPerShard = mdb.sysconf["plasma.maxInstancePerShard"].Uint64()
	mdb.mainstore.MaxDiskPerShard = mdb.sysconf["plasma.maxDiskUsagePerShard"].Uint64()
	mdb.mainstore.MinNumShard = mdb.sysconf["plasma.minNumShard"].Uint64()

	mdb.mainstore.StatsRunInterval = time.Duration(cfg["plasma.stats.runInterval"].Uint64()) * time.Second
	mdb.mainstore.StatsLogInterval = time.Duration(cfg["plasma.stats.logInterval"].Uint64()) * time.Second
	mdb.mainstore.StatsKeySizeThreshold = cfg["plasma.stats.threshold.keySize"].Uint64()
	mdb.mainstore.StatsPercentileThreshold = cfg["plasma.stats.threshold.percentile"].Float64()
	mdb.mainstore.StatsNumInstsThreshold = cfg["plasma.stats.threshold.numInstances"].Int()
	mdb.mainstore.StatsLoggerFileName = cfg["plasma.stats.logger.fileName"].String()
	mdb.mainstore.StatsLoggerFileSize = cfg["plasma.stats.logger.fileSize"].Uint64()
	mdb.mainstore.StatsLoggerFileCount = cfg["plasma.stats.logger.fileCount"].Uint64()
	mdb.mainstore.RecoveryCheckpointInterval = cfg["plasma.recovery.checkpointInterval"].Uint64()

	mdb.mainstore.EnableInMemoryCompression = mdb.sysconf["plasma.mainIndex.enableInMemoryCompression"].Bool()

	mdb.mainstore.UpdateConfig()

	if !mdb.isPrimary {
		mdb.backstore.AutoTuneLSSCleaning = cfg["plasma.AutoTuneLSSCleaner"].Bool()
		mdb.backstore.AutoTuneDiskQuota = int64(cfg["plasma.AutoTuneDiskQuota"].Uint64())
		mdb.backstore.AutoTuneCleanerTargetFragRatio = cfg["plasma.AutoTuneCleanerTargetFragRatio"].Int()
		mdb.backstore.AutoTuneCleanerMinBandwidthRatio = cfg["plasma.AutoTuneCleanerMinBandwidthRatio"].Float64()
		mdb.backstore.AutoTuneDiskFullTimeLimit = cfg["plasma.AutoTuneDiskFullTimeLimit"].Int()
		mdb.backstore.AutoTuneAvailDiskLimit = cfg["plasma.AutoTuneAvailDiskLimit"].Float64()
		mdb.backstore.MaxPageSize = cfg["plasma.MaxPageSize"].Int()
		mdb.backstore.EnforceKeyRange = cfg["plasma.enforceKeyRange"].Bool()
		mdb.backstore.CheckpointInterval = mdb.mainstore.CheckpointInterval
		mdb.backstore.MaxPageLSSSegments = mdb.sysconf["plasma.backIndex.maxLSSPageSegments"].Int()
		mdb.backstore.LSSCleanerThreshold = mdb.sysconf["plasma.backIndex.LSSFragmentation"].Int()
		mdb.backstore.LSSCleanerMaxThreshold = mdb.sysconf["plasma.backIndex.maxLSSFragmentation"].Int()
		mdb.backstore.DisableReadCaching = mdb.sysconf["plasma.disableReadCaching"].Bool()
		mdb.backstore.EnablePeriodicEvict = mdb.sysconf["plasma.backIndex.enablePeriodicEvict"].Bool()
		mdb.backstore.EvictMinThreshold = mdb.sysconf["plasma.backIndex.evictMinThreshold"].Float64()
		mdb.backstore.EvictMaxThreshold = mdb.sysconf["plasma.backIndex.evictMaxThreshold"].Float64()
		mdb.backstore.EvictDirtyOnPersistRatio = mdb.sysconf["plasma.backIndex.evictDirtyOnPersistRatio"].Float64()
		mdb.backstore.EvictDirtyPercent = mdb.sysconf["plasma.backIndex.evictDirtyPercent"].Float64()
		mdb.backstore.EvictSweepInterval = time.Duration(mdb.sysconf["plasma.backIndex.evictSweepInterval"].Int()) * time.Second
		mdb.backstore.EvictRunInterval = time.Duration(mdb.sysconf["plasma.backIndex.evictRunInterval"].Int()) * time.Millisecond
		mdb.backstore.EvictUseMemEstimate = mdb.sysconf["plasma.backIndex.evictUseMemEstimate"].Bool()

		mdb.backstore.PurgerInterval = time.Duration(mdb.sysconf["plasma.purger.interval"].Int()) * time.Second
		mdb.backstore.PurgeThreshold = mdb.sysconf["plasma.purger.highThreshold"].Float64()
		mdb.backstore.PurgeLowThreshold = mdb.sysconf["plasma.purger.lowThreshold"].Float64()
		mdb.backstore.PurgeCompactRatio = mdb.sysconf["plasma.purger.compactRatio"].Float64()
		mdb.backstore.EnableLSSPageSMO = mdb.sysconf["plasma.enableLSSPageSMO"].Bool()
		mdb.backstore.PageStatsSamplePercent = mdb.sysconf["plasma.PageStatsSamplePercent"].Float64()

		mdb.backstore.EnablePageBloomFilter = mdb.sysconf["plasma.backIndex.enablePageBloomFilter"].Bool()
		mdb.backstore.BloomFilterFalsePositiveRate = mdb.sysconf["plasma.backIndex.bloomFilterFalsePositiveRate"].Float64()
		mdb.backstore.BloomFilterExpectedMaxItems = mdb.sysconf["plasma.backIndex.bloomFilterExpectedMaxItems"].Uint64()

		mdb.backstore.MaxInstsPerShard = mdb.sysconf["plasma.maxInstancePerShard"].Uint64()
		mdb.backstore.MaxDiskPerShard = mdb.sysconf["plasma.maxDiskUsagePerShard"].Uint64()
		mdb.backstore.MinNumShard = mdb.sysconf["plasma.minNumShard"].Uint64()

		mdb.backstore.StatsRunInterval = time.Duration(cfg["plasma.stats.runInterval"].Uint64()) * time.Second
		mdb.backstore.StatsLogInterval = time.Duration(cfg["plasma.stats.logInterval"].Uint64()) * time.Second
		mdb.backstore.StatsKeySizeThreshold = cfg["plasma.stats.threshold.keySize"].Uint64()
		mdb.backstore.StatsPercentileThreshold = cfg["plasma.stats.threshold.percentile"].Float64()
		mdb.backstore.StatsNumInstsThreshold = cfg["plasma.stats.threshold.numInstances"].Int()
		mdb.backstore.StatsLoggerFileName = cfg["plasma.stats.logger.fileName"].String()
		mdb.backstore.StatsLoggerFileSize = cfg["plasma.stats.logger.fileSize"].Uint64()
		mdb.backstore.StatsLoggerFileCount = cfg["plasma.stats.logger.fileCount"].Uint64()
		mdb.backstore.RecoveryCheckpointInterval = cfg["plasma.recovery.checkpointInterval"].Uint64()

		mdb.backstore.EnableInMemoryCompression = mdb.sysconf["plasma.backIndex.enableInMemoryCompression"].Bool()

		mdb.backstore.UpdateConfig()
	}
	mdb.maxRollbacks = cfg["settings.plasma.recovery.max_rollbacks"].Int()
	mdb.maxDiskSnaps = cfg["recovery.max_disksnaps"].Int()

	if keySizeConfigUpdated(cfg, oldCfg) {
		for i := 0; i < len(mdb.keySzConfChanged); i++ {
			atomic.AddInt32(&mdb.keySzConfChanged[i], 1)
		}
	}
}

func (mdb *plasmaSlice) String() string {

	str := fmt.Sprintf("SliceId: %v ", mdb.id)
	str += fmt.Sprintf("File: %v ", mdb.path)
	str += fmt.Sprintf("Index: %v ", mdb.idxInstId)
	str += fmt.Sprintf("Partition: %v ", mdb.idxPartnId)

	return str

}

func tryDeleteplasmaSlice(mdb *plasmaSlice) {

	//cleanup the disk directory
	if err := destroyPlasmaSlice(mdb.storageDir, mdb.path); err != nil {
		logging.Errorf("plasmaSlice::Destroy Error Cleaning Up Slice Id %v, "+
			"IndexInstId %v, PartitionId %v, IndexDefnId %v. Error %v", mdb.id,
			mdb.idxInstId, mdb.idxPartnId, mdb.idxDefnId, err)
	} else {
		logging.Infof("plasmaSlice::Destroy Cleaned Up Slice Id %v, "+
			"IndexInstId %v, PartitionId %v, IndexDefnId %v.", mdb.id,
			mdb.idxInstId, mdb.idxPartnId, mdb.idxDefnId)
	}
}

func tryCloseplasmaSlice(mdb *plasmaSlice) {

	logging.Infof("plasmaSlice::Close Closed Slice Id %v, "+
		"IndexInstId %v, PartitionId %v, IndexDefnId %v.", mdb.id,
		mdb.idxInstId, mdb.idxPartnId, mdb.idxDefnId)

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
		logging.Debugf("logWriterStat:: %v:%v "+
			"FlushedCount %v QueuedCount %v", mdb.idxInstId, mdb.idxPartnId,
			count, mdb.getCmdsCount())
	}

}

func (mdb *plasmaSlice) RecoveryDone() {
	plasma.RecoveryDone()
}

func (info *plasmaSnapshotInfo) Timestamp() *common.TsVbuuid {
	return info.Ts
}

func (info *plasmaSnapshotInfo) IsCommitted() bool {
	return info.Committed
}

func (info *plasmaSnapshotInfo) Stats() map[string]interface{} {
	return info.IndexStats
}

func (info *plasmaSnapshotInfo) IsOSOSnap() bool {
	if info.Ts != nil && info.Ts.GetSnapType() == common.DISK_SNAP_OSO {
		return true
	}
	return false
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
		logging.Errorf("plasmaSnapshot::Close Close operation requested "+
			"on already closed snapshot. Index %v, Bucket %v, IndexInstId %v, PartitionId %v",
			s.slice.idxDefn.Name, s.slice.idxDefn.Bucket, s.slice.idxInstId, s.slice.idxPartnId)
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
	s.slice.idxStats.numOpenSnapshots.Add(-1)
	defer s.slice.DecrRef()
}

func (s *plasmaSnapshot) String() string {

	str := fmt.Sprintf("Index: %v ", s.idxInstId)
	str += fmt.Sprintf("PartitionId: %v ", s.idxPartnId)
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
				_, err = jsonEncoder.ReverseCollate(revbuf, s.slice.idxDefn.Desc)
				if err != nil {
					return err
				}

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
					if err != nil {
						return err
					}
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

	defer func() {
		if r := recover(); r != nil {
			logging.Fatalf("plasmaSnapshot::Iterate: panic detected while iterating snapshot "+
				"low key = %s high key = %s Index %v, Bucket %v, IndexInstId %v, "+
				"PartitionId %v", logging.TagStrUD(low), logging.TagStrUD(high),
				s.slice.idxDefn.Name, s.slice.idxDefn.Bucket, s.slice.idxInstId, s.slice.idxPartnId)
			logging.Fatalf("%s", logging.StackTraceAll())
			panic(r)
		}
	}()

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
			endKey = common.GenNextBiggerKey(endKey, s.isPrimary())
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
		s.newIndexEntry(itm, &entry)

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

func (s *plasmaSnapshot) newIndexEntry(b []byte, entry *IndexEntry) {
	var err error

	if s.slice.isPrimary {
		*entry, err = BytesToPrimaryIndexEntry(b)
	} else {
		*entry, err = BytesToSecondaryIndexEntry(b)
	}
	common.CrashOnError(err)
}

func (s *plasmaSnapshot) iterEqualKeys(k IndexKey, it *plasma.MVCCIterator,
	cmpFn CmpEntry, callback func([]byte) error) error {
	var err error

	var entry IndexEntry
	for ; it.Valid(); it.Next() {
		itm := it.Key()
		s.newIndexEntry(itm, &entry)
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
func backEntry2entry(docid []byte, bentry []byte, buf []byte, sz keySizeConfig) []byte {
	l := len(bentry)
	count := int(binary.LittleEndian.Uint16(bentry[l-2 : l]))
	entry, _ := NewSecondaryIndexEntry2(bentry[:l-2], docid, false, count, nil, buf[:0], false, nil, sz)
	return entry.Bytes()
}

func hasEqualBackEntry(key []byte, bentry []byte) bool {
	if key == nil || isJSONEncoded(key) {
		return false
	}

	// Ignore 2 byte count for comparison
	return bytes.Equal(key, bentry[:len(bentry)-2])
}

////////////////////////////////////////////////////////////
// Writer Auto-Tuning
////////////////////////////////////////////////////////////

//
// Default number of num writers
//
func (slice *plasmaSlice) numWritersPerPartition() int {
	return int(math.Ceil(float64(slice.maxNumWriters) / float64(slice.numPartitions)))
}

//
// Get command handler queue size
//
func (slice *plasmaSlice) defaultCmdQueueSize() uint64 {

	sliceBufSize := slice.sysconf["settings.sliceBufSize"].Uint64()
	numWriters := slice.numWritersPerPartition()

	if sliceBufSize < uint64(numWriters) {
		sliceBufSize = uint64(numWriters)
	}

	return sliceBufSize / uint64(numWriters)
}

//
// Allocate array for writers
//
func (slice *plasmaSlice) setupWriters() {

	// initialize buffer
	slice.encodeBuf = make([][]byte, 0, slice.maxNumWriters)
	slice.arrayBuf1 = make([][]byte, 0, slice.maxNumWriters)
	slice.arrayBuf2 = make([][]byte, 0, slice.maxNumWriters)
	slice.keySzConfChanged = make([]int32, 0, slice.maxNumWriters)
	slice.keySzConf = make([]keySizeConfig, 0, slice.maxNumWriters)

	// initialize comand handler
	slice.cmdCh = make([]chan *indexMutation, 0, slice.maxNumWriters)
	slice.stopCh = make([]DoneChannel, 0, slice.maxNumWriters)

	// initialize writers
	slice.main = make([]*plasma.Writer, 0, slice.maxNumWriters)
	slice.back = make([]*plasma.Writer, 0, slice.maxNumWriters)

	// initialize tokens
	slice.token = registerFreeWriters(slice.idxInstId, slice.maxNumWriters)

	// start writers
	numWriter := slice.numWritersPerPartition()
	slice.token.decrement(numWriter, true)
	slice.startWriters(numWriter)
	// start stats sampler
	go slice.runSampler()
}

//
// Initialize any field related to numWriters
//
func (slice *plasmaSlice) initWriters(numWriters int) {

	curNumWriters := len(slice.cmdCh)

	// initialize buffer
	slice.encodeBuf = slice.encodeBuf[:numWriters]
	if slice.idxDefn.IsArrayIndex {
		slice.arrayBuf1 = slice.arrayBuf1[:numWriters]
		slice.arrayBuf2 = slice.arrayBuf2[:numWriters]
	}
	slice.keySzConfChanged = slice.keySzConfChanged[:numWriters]
	slice.keySzConf = slice.keySzConf[:numWriters]

	for i := curNumWriters; i < numWriters; i++ {
		slice.confLock.RLock()
		keyCfg := getKeySizeConfig(slice.sysconf)
		slice.confLock.RUnlock()
		slice.encodeBuf[i] = make([]byte, 0, keyCfg.maxIndexEntrySize)
		if slice.idxDefn.IsArrayIndex {
			slice.arrayBuf1[i] = make([]byte, 0, keyCfg.maxArrayIndexEntrySize)
			slice.arrayBuf2[i] = make([]byte, 0, keyCfg.maxArrayIndexEntrySize)
		}
		slice.keySzConf[i] = keyCfg
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
	slice.main = slice.main[:numWriters]
	for i := curNumWriters; i < numWriters; i++ {
		slice.main[i] = slice.mainstore.NewWriter()
	}

	// initialize backstore writers
	if !slice.isPrimary {
		slice.back = slice.back[:numWriters]
		for i := curNumWriters; i < numWriters; i++ {
			slice.back[i] = slice.backstore.NewWriter()
		}
	}
}

//
// Start the writers by passing in the desired number of writers
//
func (slice *plasmaSlice) startWriters(numWriters int) {

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

//
// Stop the writers by passing in the desired number of writers
//
func (slice *plasmaSlice) stopWriters(numWriters int) {

	// If slice already have fewer writers that the desired number, return.
	if numWriters >= slice.numWriters {
		return
	}

	// free writer memory
	for i := numWriters; i < slice.numWriters; i++ {
		slice.main[i].ResetBuffers()
		if !slice.isPrimary {
			slice.back[i].ResetBuffers()
		}
	}

	// update the number of slice writers
	slice.numWriters = numWriters
}

//
// Free all writers
//
func (slice *plasmaSlice) freeAllWriters() {
	// Stop all command workers
	for _, stopCh := range slice.stopCh {
		stopCh <- true
		<-stopCh
	}

	slice.stopWriters(0)

	slice.encodeBuf = slice.encodeBuf[:0]
	slice.arrayBuf1 = slice.arrayBuf1[:0]
	slice.arrayBuf2 = slice.arrayBuf2[:0]
	slice.keySzConfChanged = slice.keySzConfChanged[:0]
	slice.keySzConf = slice.keySzConf[:0]

	slice.cmdCh = slice.cmdCh[:0]
	slice.stopCh = slice.stopCh[:0]

	slice.main = slice.main[:0]
	if !slice.isPrimary {
		slice.back = slice.back[:0]
	}
}

//
// Logging
//
func (slice *plasmaSlice) logSample(numWriters int) {

	logging.Infof("plasmaSlice %v:%v mutation rate %.2f drain rate %.2f saturateCount %v minimum drain rate %.2f",
		slice.idxInstId, slice.idxPartnId,
		slice.adjustedMeanMutationRate(),
		slice.adjustedMeanDrainRate()*float64(numWriters),
		slice.saturateCount,
		slice.minimumDrainRate)
}

//
// Expand the number of writer
//
func (slice *plasmaSlice) expandWriters(needed int) {

	// increment writer one at a 1 to avoid saturation.    This means that
	// it will be less responsive for sporadic traffic.  It will take
	// longer for stale=false query to catch up when there is a spike in
	// mutation rate.

	//increment := int(needed - slice.numWriters)
	increment := 1

	mean := slice.adjustedMeanDrainRate() * float64(slice.numWriters)
	if increment > 0 && mean > 0 {
		// Is there any free writer available?
		if increment = slice.token.decrement(increment, false); increment > 0 {
			lastNumWriters := slice.numWriters

			// start writer
			slice.startWriters(slice.numWriters + increment)

			slice.minimumDrainRate = slice.computeMinimumDrainRate(lastNumWriters)
			slice.numExpand++

			logging.Verbosef("plasmaSlice %v:%v expand writers from %v to %v (standby writer %v) token %v",
				slice.idxInstId, slice.idxPartnId, lastNumWriters, slice.numWriters,
				len(slice.cmdCh)-slice.numWriters, slice.token.num())

			if logging.IsEnabled(logging.Verbose) {
				slice.logSample(lastNumWriters)
			}
		}
	}
}

//
// Reduce the number of writer
//
func (slice *plasmaSlice) reduceWriters(needed int) {

	//decrement := int(math.Ceil(float64(slice.numWriters-needed) / 2))
	decrement := 1

	if decrement > 0 {
		lastNumWriters := slice.numWriters

		// stop writer
		slice.stopWriters(slice.numWriters - decrement)

		// add token after the writer is freed
		slice.token.increment(decrement)

		slice.minimumDrainRate = slice.computeMinimumDrainRate(lastNumWriters)
		slice.numReduce++

		logging.Verbosef("plasmaSlice %v:%v reduce writers from %v to %v (standby writer %v) token %v",
			slice.idxInstId, slice.idxPartnId, lastNumWriters, slice.numWriters,
			len(slice.cmdCh)-slice.numWriters, slice.token.num())

		if logging.IsEnabled(logging.Verbose) {
			slice.logSample(lastNumWriters)
		}
	}
}

//
// Calculate minimum drain rate
// Minimum drain rate is calculated everytime when expanding or reducing writers, so it keeps
// adjusting to the trailing 1 second mean drain rate. If drain rate is trending down,
// then minimum drain rate will also trending down.
//
func (slice *plasmaSlice) computeMinimumDrainRate(lastNumWriters int) float64 {

	// compute expected drain rate based on mean drain rate adjusted based on memory usage
	mean := slice.adjustedMeanDrainRate() * float64(lastNumWriters)
	newMean := mean * float64(slice.numWriters) / float64(lastNumWriters)

	if slice.numWriters > lastNumWriters {
		return mean + ((newMean - mean) * slice.scalingFactor)
	}

	return newMean
}

//
// Does drain rate meet the minimum level?
//
func (slice *plasmaSlice) meetMinimumDrainRate() {

	// If the slice does not meet the minimum drain rate requirement after expanding/reducing writers, increment
	// saturation count.  Saturation count is token to keep track of how many misses on minimum drain rate.
	// If drain rate is not saturated or trending down, normal flucturation in drain rate should not keep
	// saturation count reaching threshold.
	//
	// The minimum drain rate is computed to be an easy-to-reach target in order to reduce chances of false
	// positive on drain rate saturation.
	//
	// If drain rate is saturated or trending down, there will be more misses than hits.  The saturation count should increase,
	// since the minimum drain rate is trailing the actual drain rate.
	//
	if slice.adjustedMeanDrainRateWithInterval(slice.adjustInterval)*float64(slice.numWriters) < slice.minimumDrainRate {
		if slice.saturateCount < slice.threshold {
			slice.saturateCount++
		}
	} else {
		if slice.saturateCount > 0 {
			slice.saturateCount--
		}
	}
}

//
// Adjust number of writers needed
//
func (slice *plasmaSlice) adjustNumWritersNeeded(needed int) int {

	// Find a victim to release token if running out of token
	if slice.token.num() < 0 {
		if float64(slice.numWriters)/float64(slice.maxNumWriters) > rand.Float64() {
			return slice.numWriters - 1
		}
	}

	// do not allow expansion when reaching minimum memory
	if slice.minimumMemory() && needed > slice.numWriters {
		return slice.numWriters
	}

	// limit writer when memory is 95% full
	if slice.memoryFull() &&
		needed > slice.numWriters &&
		needed > slice.numWritersPerPartition() {

		if slice.numWriters > slice.numWritersPerPartition() {
			return slice.numWriters
		}

		return slice.numWritersPerPartition()
	}

	// There are different situations where drain rate goes down and cannot meet minimum requiremnts:
	// 1) IO saturation
	// 2) new plasma instance is added to the node
	// 3) log cleaner running
	// 4) DGM ratio goes down
	//
	// If it gets 10 misses, then it could mean the drain rate has saturated or trending down over 1s interval.
	// If so, redcue the number of writers by 1, and re-calibrate by recomputing the minimum drain rate again.
	// In the next interval, if the 100ms drain rate is able to meet the minimum requirement, it will allow
	// number of writers to expand. Otherwise, it will keep reducing the number of writers until it can meet
	// the minimum drain rate.
	//
	/*
		if slice.saturateCount >= slice.threshold {
			return slice.numWriters - 1
		}
	*/

	return needed
}

//
// Adjust the number of writer
//
func (slice *plasmaSlice) adjustWriters() {

	slice.writerLock.Lock()
	defer slice.writerLock.Unlock()

	// Is it the time to adjust the number of writers?
	if slice.shouldAdjustWriter() {
		slice.meetMinimumDrainRate()

		needed := slice.numWritersNeeded()
		needed = slice.adjustNumWritersNeeded(needed)

		if slice.canExpandWriters(needed) {
			slice.expandWriters(needed)
		} else if slice.canReduceWriters(needed) {
			slice.reduceWriters(needed)
		}
	}
}

//
// Expand the writer when
// 1) enableWriterTuning is enabled
// 2) numWriters is fewer than the maxNumWriters
// 3) numWriters needed is greater than numWriters
// 4) drain rate has increased since the last expansion
//
func (slice *plasmaSlice) canExpandWriters(needed int) bool {

	return slice.enableWriterTuning &&
		slice.numWriters < slice.maxNumWriters &&
		needed > slice.numWriters
}

//
// Reduce the writer when
// 1) enableWriterTuning is enabled
// 2) numWriters is greater than 1
// 3) numWriters needed is fewer than numWriters
//
func (slice *plasmaSlice) canReduceWriters(needed int) bool {

	return slice.enableWriterTuning &&
		slice.numWriters > 1 &&
		needed < slice.numWriters
}

//
// Update the sample based on the stats collected in last flush
// Drain rate and mutation rate is measured based on the
// number of incoming and written keys.   It does not include
// the size of the key.
//
func (slice *plasmaSlice) updateSample(elapsed int64, needLog bool) {

	slice.writerLock.Lock()
	defer slice.writerLock.Unlock()

	drainTime := float64(atomic.LoadInt64(&slice.drainTime))
	mutations := float64(atomic.LoadInt64(&slice.numItems))

	// Update the drain rate.
	drainRate := float64(0)
	if drainTime > 0 {
		// drain rate = num of items written per writer per second
		drainRate = mutations / drainTime * float64(slice.snapInterval)
	}
	drainRatePerWriter := drainRate / float64(slice.numWriters)
	slice.drainRate.Update(drainRatePerWriter)

	// Update mutation rate.
	mutationRate := mutations / float64(elapsed) * float64(slice.snapInterval)
	slice.mutationRate.Update(mutationRate)

	// reset stats
	atomic.StoreInt64(&slice.drainTime, 0)
	atomic.StoreInt64(&slice.numItems, 0)

	// periodic logging
	if needLog {
		logging.Infof("plasmaSlice %v:%v numWriter %v standby writer %v token %v numExpand %v numReduce %v",
			slice.idxInstId, slice.idxPartnId, slice.numWriters, len(slice.cmdCh)-slice.numWriters, slice.token.num(),
			slice.numExpand, slice.numReduce)

		slice.logSample(slice.numWriters)

		slice.numExpand = 0
		slice.numReduce = 0
	}
}

//
// Check if it is time to adjust the writer
//
func (slice *plasmaSlice) shouldAdjustWriter() bool {

	if !slice.enableWriterTuning {
		return false
	}

	now := time.Now().UnixNano()
	if now-slice.lastCheckTime > int64(slice.adjustInterval) {
		slice.lastCheckTime = now
		return true
	}

	return false
}

//
// Mutation rate is always calculated using adjust interval (100ms), adjusted based on memory utilization.
// Short interval for mutation rate alllows more responsiveness. Drain rate is calculated at 1s interval to
// reduce variation.   Therefore, fluctation in mutation rate is more likely to cause writers to expand/reduce
// than fluctation in drain rate. The implementation attempts to make allocate/de-allocate writers efficiently
// to faciliate constant expansion/reduction of writers.
//
func (slice *plasmaSlice) numWritersNeeded() int {

	mutationRate := slice.adjustedMeanMutationRate()
	drainRate := slice.adjustedMeanDrainRate()

	// If drain rate is 0, there is no expansion.
	if drainRate > 0 {
		needed := int(math.Ceil(mutationRate / drainRate))

		if needed == 0 {
			needed = 1
		}

		if needed > slice.maxNumWriters {
			needed = slice.maxNumWriters
		}

		return needed
	}

	// return 1 if there is no mutation
	if mutationRate <= 0 {
		return 1
	}

	// If drain rate is 0 but mutation rate is not 0, then return current numWriters
	return slice.numWriters
}

//
// Run sampler every second

func (slice *plasmaSlice) runSampler() {

	if !slice.enableWriterTuning {
		return
	}

	lastTime := time.Now()
	lastLogTime := lastTime

	ticker := time.NewTicker(time.Duration(slice.samplingInterval))
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			needLog := time.Now().Sub(lastLogTime).Nanoseconds() > int64(time.Minute)
			slice.updateSample(time.Now().Sub(lastTime).Nanoseconds(), needLog)
			lastTime = time.Now()
			if needLog {
				lastLogTime = lastTime
			}
		case <-slice.samplerStopCh:
			return
		}
	}
}

type windowFunc func(sample *common.Sample, count int) float64

//
// Get mean drain rate adjusted based on memory uasge
//
func (slice *plasmaSlice) adjustedMeanDrainRate() float64 {

	return slice.adjustedMeanDrainRateWithInterval(uint64(time.Second))
}

func (slice *plasmaSlice) adjustedMeanDrainRateWithInterval(interval uint64) float64 {

	window := func(sample *common.Sample, count int) float64 { return sample.WindowMean(count) }
	return slice.computeAdjustedAggregate(window, slice.drainRate, interval)
}

//
// Get std dev drain rate adjusted based on memory uasge
//
func (slice *plasmaSlice) adjustedStdDevDrainRate() float64 {

	window := func(sample *common.Sample, count int) float64 { return sample.WindowStdDev(count) }
	return slice.computeAdjustedAggregate(window, slice.drainRate, uint64(time.Second))
}

//
// Get mean mutation rate adjusted based on memory uasge
//
func (slice *plasmaSlice) adjustedMeanMutationRate() float64 {

	window := func(sample *common.Sample, count int) float64 { return sample.WindowMean(count) }
	return slice.computeAdjustedAggregate(window, slice.mutationRate, slice.adjustInterval)
}

//
// Get std dev mutation rate adjusted based on memory uasge
//
func (slice *plasmaSlice) adjustedStdDevMutationRate() float64 {

	window := func(sample *common.Sample, count int) float64 { return sample.WindowStdDev(count) }
	return slice.computeAdjustedAggregate(window, slice.mutationRate, slice.adjustInterval)
}

func (slice *plasmaSlice) computeAdjustedAggregate(window windowFunc, sample *common.Sample, interval uint64) float64 {

	count := int(interval / slice.samplingInterval)

	if float64(slice.memoryAvail()) < float64(slice.memoryLimit())*0.20 && slice.memoryAvail() > 0 {
		count = count * int(slice.memoryLimit()/slice.memoryAvail())
		if count > int(slice.samplingWindow/slice.samplingInterval) {
			count = int(slice.samplingWindow / slice.samplingInterval)
		}
	}

	return window(sample, count)
}

//
// get memory limit
//
func (slice *plasmaSlice) memoryLimit() float64 {

	//return float64(slice.indexerStats.memoryQuota.Value())
	return float64(getMemTotal())
}

//
// get available memory left
//
func (slice *plasmaSlice) memoryAvail() float64 {

	//return float64(slice.indexerStats.memoryQuota.Value()) - float64(slice.indexerStats.memoryUsed.Value())
	return float64(getMemFree())
}

//
// get memory used
//
func (slice *plasmaSlice) memoryUsed() float64 {

	//return float64(slice.indexerStats.memoryUsed.Value())
	return slice.memoryLimit() - slice.memoryAvail()
}

//
// memory full
//
func (slice *plasmaSlice) memoryFull() bool {

	return (float64(slice.memoryAvail()) < float64(slice.memoryLimit())*0.05)
}

//
// minimum memory  (10M)
//
func (slice *plasmaSlice) minimumMemory() bool {

	return (float64(slice.memoryAvail()) <= float64(20*1024*1024))
}

////////////////////////////////////////////////////////////
// Writer Tokens
////////////////////////////////////////////////////////////

var freeWriters tokens

func init() {
	freeWriters.tokens = make(map[common.IndexInstId]*token)
}

type token struct {
	value int64
}

func (t *token) num() int64 {
	return atomic.LoadInt64(&t.value)
}

func (t *token) increment(increment int) {

	atomic.AddInt64(&t.value, int64(increment))
}

func (t *token) decrement(decrement int, force bool) int {

	for {
		if count := atomic.LoadInt64(&t.value); count > 0 || force {
			d := int64(decrement)

			if !force {
				if int64(decrement) > count {
					d = count
				}
			}

			if atomic.CompareAndSwapInt64(&t.value, count, count-d) {
				return int(d)
			}
		} else {
			break
		}
	}

	return 0
}

type tokens struct {
	mutex  sync.RWMutex
	tokens map[common.IndexInstId]*token
}

func registerFreeWriters(instId common.IndexInstId, count int) *token {

	freeWriters.mutex.Lock()
	defer freeWriters.mutex.Unlock()

	if _, ok := freeWriters.tokens[instId]; !ok {
		freeWriters.tokens[instId] = &token{value: int64(count)}
	}
	return freeWriters.tokens[instId]
}

func deleteFreeWriters(instId common.IndexInstId) {
	freeWriters.mutex.Lock()
	defer freeWriters.mutex.Unlock()
	delete(freeWriters.tokens, instId)
}

func getCompressionRatio(pStats plasma.Stats) float64 {
	marshalledData := (float64)(pStats.PageBytesMarshalled)
	compressedData := (float64)(pStats.PageBytesCompressed)
	if compressedData > 0 {
		return (marshalledData / compressedData)
	}
	return 1
}
