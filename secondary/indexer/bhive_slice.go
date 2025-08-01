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
	bc "github.com/couchbase/bhive/common"
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
	NumKVStore                    = 10
	MaxBatchSize                  = 256
	SNAPSHOT_META_VERSION_BHIVE_1 = 1
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
	readersReserve

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
	topNScan           int

	// doc Seq no
	docSeqno uint64

	//
	// rebalance
	shardIds []common.ShardId

	// set to true if the slice is created as a part of rebalance
	// This flag will be cleared at the end of rebalance
	rebalRunning uint32

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

	// rollback
	lastRollbackTs *common.TsVbuuid

	persistFullVector bool

	// error
	fatalDbErr error // TODO
}

type bhiveSnapshotInfo struct {
	Ts        *common.TsVbuuid
	Committed bool
	Count     int64

	mRP, bRP *bhive.RecoveryPoint

	IndexStats map[string]interface{}
	Version    int
	InstId     common.IndexInstId
	PartnId    common.PartitionId

	DocSeqno uint64
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

	chkpointCb bhive.RecoveryPointCallback
	chkpointCh chan bool

	refCount int32
}

////////////////////////////////////////////////
// BhiveSlice
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
	slice.maxNumWriters = NumKVStore // num writers must match kvstore
	slice.topNScan = sysconf["bhive.topNScan"].Int()
	updateBhiveConfig(slice.sysconf)

	numReaders := sysconf["bhive.numReaders"].Int()
	slice.readers = make(chan *bhive.Reader, numReaders)

	// if slice.idxDefn.IsVectorIndex {
	slice.InitReadersReserve(numReaders)

	// stats
	slice.idxStats = idxStats
	slice.indexerMemoryQuota = memQuota

	if len(shardIds) > 0 {
		slice.SetRebalRunning()

		logging.Infof("bhiveSlice::NewBhiveSlice Id: %v IndexInstId: %v PartitionId: %v "+
			"using shardIds: %v, rebalance running: %v", sliceId, idxInstId, partitionId,
			shardIds, slice.IsRebalRunning())

		slice.shardIds = append(slice.shardIds, shardIds[0], shardIds[1])
	}

	// initialize main and back stores
	if err := slice.initStores(isInitialBuild, cancelCh); err != nil {
		// Index is unusable. Remove the data files and reinit
		if err == errStorageCorrupted || err == errStoragePathNotFound {
			logging.Errorf("bhiveSlice:NewBhiveSlice Id %v IndexInstId %v PartitionId %v isNew %v"+
				"fatal error occured: %v", sliceId, idxInstId, partitionId, isNew, err)
		}
		if isNew {
			destroySlice_Bhive(storage_dir, path)
		}
		return nil, err
	}

	if isInitialBuild {
		atomic.StoreInt32(&slice.isInitialBuild, 1)
	}

	// intiialize and start the writers
	slice.setupWriters()

	slice.UpdateConfig(sysconf)

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

	slice.shardIds = nil // Reset the slice and read the actuals from the store
	slice.shardIds = append(slice.shardIds, common.ShardId(slice.mainstore.GetShardId()))
	slice.shardIds = append(slice.shardIds, common.ShardId(slice.backstore.GetShardId()))

	return slice, nil
}

// BackupCorruptedSlice_Bhive - save corrupted instance to a backup location
func BackupCorruptedSlice_Bhive(
	storageDir, prefix string,
	rename func(string) (string, error),
	clean func(string),
) error {
	return backupCorruptedSlice_Bhive(storageDir, prefix, rename, clean)
}

// DestroySlice_Bhive - destroy corrupt or deleted bhive instance
func DestroySlice_Bhive(storageDir string, path string) error {
	return destroySlice_Bhive(storageDir, path)
}

// GetEmptyShardInfo_Bhive - get list of empty bhive shards
func GetEmptyShardInfo_Bhive() ([]common.ShardId, error) {
	bhiveShards, err := bhive.GetCurrentEmptyShardsInfo()
	var gsiShards []common.ShardId
	for _, shard := range bhiveShards {
		gsiShards = append(gsiShards, common.ShardId(shard))
	}
	return gsiShards, err
}

// DestroyShard_Bhive - destroy bhive shard usually called to delete empty bhive shards
func DestroyShard_Bhive(shardId common.ShardId) error {
	return bhive.DestroyShardID(plasma.ShardId(shardId))
}

// RecoveryDone_Bhive - called after indexer is finished with recovery
func RecoveryDone_Bhive() {
	bhive.RecoveryDone()
}

// GetShardCompatVersion_Bhive - get bhive shard compatibility version. changing them across versions
// will prevent older versions of indexers to use DCP rebalance over file based rebalance
func GetShardCompatVersion_Bhive() int {
	return bhive.ShardCompatVersion
}

func createBhiveSliceDir(storageDir string, path string, isNew bool) error {

	_, err := iowrap.Os_Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			iowrap.Os_MkdirAll(path, 0777)
			return nil
		}
	} else if isNew {
		// if we expect a new instance but there is residual file, destroy old data.
		err = DestroySlice_Bhive(storageDir, path)
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

	cfg.EnableKeyPrefixMode = false
	cfg.EnableUpdateStatusForSet = false

	cfg.CentroidIDSize = int(reflect.TypeOf(i).Size())
	cfg.KeyPrefixSize = uint64(cfg.CentroidIDSize)
	cfg.NumKVStore = NumKVStore
	cfg.MaxBatchSize = MaxBatchSize // TODO: Make it configurable (for testing)

	cfg.Parallelism = 1
	cfg.Dimension = slice.idxDefn.VectorMeta.Dimension

	cfg.EfNumNeighbors = slice.sysconf["bhive.vanama.efNumNeighbors"].Int()
	cfg.EfConstruction = slice.sysconf["bhive.vanama.efConstruction"].Int()
	cfg.VanamaBuildQuota = slice.sysconf["bhive.vanama.buildQuota"].Int()
	cfg.FilterThreshold = float32(slice.sysconf["bhive.vanama.filterThreshold"].Float64())
	cfg.NumCompactor = slice.sysconf["bhive.numCompactor"].Int()
	cfg.UseVanama = slice.sysconf["bhive.useVanama"].Bool()
	cfg.UseDistEncoded = slice.sysconf["bhive.useResidual"].Bool()
	cfg.NumFlushBuffer = slice.sysconf["bhive.numFlushBuffer"].Int()
	cfg.MinNumFlushBuffer = slice.sysconf["bhive.minNumFlushBuffer"].Int()
	cfg.MaxNumFlushBuffer = slice.sysconf["bhive.maxNumFlushBuffer"].Int()
	cfg.NumEvictor = slice.sysconf["bhive.numEvictor"].Int()
	cfg.NumInitBuilder = slice.sysconf["bhive.numInitBuilder"].Int()
	cfg.NumBuilder = slice.sysconf["bhive.numBuilder"].Int()
	if cfg.NumBuilder == 0 {
		cfg.NumBuilder = 1
	}
	cfg.EvictThreshold = slice.sysconf["bhive.evictThreshold"].Int()
	cfg.CompressionType = slice.sysconf["bhive.compressType"].String()
	cfg.CompressionLevel = slice.sysconf["bhive.compressLevel"].Int()

	cfg.EnableRollbackFilterMerge = slice.sysconf["bhive.EnableRollbackFilterMerge"].Bool()
	cfg.EnableRollbackFilterTrim = slice.sysconf["bhive.EnableRollbackFilterTrim"].Bool()
	cfg.EnableRollbackFilterPrune = slice.sysconf["bhive.EnableRollbackFilterPrune"].Bool()

	cfg.EnableCompactFullMarshal = slice.sysconf["bhive.enableCompactFullMarshal"].Bool()
	cfg.PurgeThresholdMax = float32(slice.sysconf["bhive.purgeThresholdMax"].Float64())
	cfg.PurgeThresholdMin = float32(slice.sysconf["bhive.purgeThresholdMin"].Float64())
	cfg.PurgeThresholdReader = float32(slice.sysconf["bhive.purgeThresholdReader"].Float64())
	cfg.PurgeThresholdRecords = uint32(slice.sysconf["bhive.purgeThresholdRecords"].Int())
	cfg.AutoMVCCCompaction = slice.sysconf["bhive.compactor.enable"].Bool()
	cfg.AutoCompactionDiskLimit = float32(slice.sysconf["bhive.compactor.diskLimit"].Int()) / 100
	cfg.AutoCompactionInterval = time.Duration(slice.sysconf["bhive.compactor.interval"].Int()) * time.Minute
	cfg.AutoCompactionRateLimit = slice.sysconf["bhive.compactor.rateLimit"].Int()
	cfg.AutoCompactionBatchLimit = slice.sysconf["bhive.compactor.batchLimit"].Int()

	cfg.LSDFragmentationRatio = slice.sysconf["bhive.MagmaLSDFragmentationPercent"].Float64() / 100.0
	cfg.MaxOpenFiles = uint64(slice.sysconf["bhive.MagmaMaxOpenFiles"].Int())

	cfg.AutoLSSCleaning = slice.sysconf["bhive.enableAutoLSSCleaning"].Bool()
	cfg.LSSCleanerInterval = time.Duration(slice.sysconf["bhive.LSSCleanerInterval"].Int()) * time.Millisecond
	cfg.LSSCleanerThreshold = float32(slice.sysconf["bhive.LSSCleanerThreshold"].Float64())
	cfg.LSSCleanerMinSize = int64(slice.sysconf["bhive.LSSCleanerMinSize"].Int())
	cfg.LSSReclaimBlockSize = int64(slice.sysconf["bhive.LSSReclaimBlockSize"].Int())

	cfg.NumWriters = slice.maxNumWriters

	cfg.PersistFullVector = slice.sysconf["bhive.persistFullVector"].Bool() && slice.idxDefn.VectorMeta.PersistFullVector

	cfg.RecoveryFullDataReplay = slice.sysconf["bhive.RecoveryFullDataReplay"].Bool()
	cfg.RecoveryDump = slice.sysconf["bhive.RecoveryDump"].Bool()

	cfg.AutoBackupDiskLimit = float32(slice.sysconf["bhive.backupCorruptedDiskLimit"].Int()) / 100
	cfg.AutoBackupShard = slice.sysconf["bhive.backupCorruptedShard"].Bool()

	cfg.EnableBreakPad = slice.sysconf["bhive.enableBreakPad"].Bool()
	// cbcollect collects only from crash directory inside crash (ns_server:indexer_breakpad_minidump_dir)
	cfg.MiniDumpDir = common.SystemConfig["indexer.diagnostics_dir"].String()

	// shard transfer
	loadClientCopyConfig(&cfg.CopyConfig, slice.sysconf, false) // bhive santizes config on update

	logging.Infof("bhiveSlice:setupConfig efNumNeighbors %v efConstruction %v buildQuota %v numCompactor %v topN %v PersistFullVector: %v",
		cfg.EfNumNeighbors, cfg.EfConstruction, cfg.VanamaBuildQuota, cfg.NumCompactor, slice.topNScan, cfg.PersistFullVector)

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

	cfg.LSDFragmentationRatio = slice.sysconf["bhive.MagmaLSDFragmentationPercent"].Float64() / 100
	cfg.MaxOpenFiles = uint64(slice.sysconf["bhive.MagmaMaxOpenFiles"].Int())

	cfg.AutoBackupDiskLimit = float32(slice.sysconf["bhive.backupCorruptedDiskLimit"].Int()) / 100
	cfg.AutoBackupShard = slice.sysconf["bhive.backupCorruptedShard"].Bool()

	cfg.EnableBreakPad = slice.sysconf["bhive.enableBreakPad"].Bool()
	cfg.MiniDumpDir = common.SystemConfig["indexer.diagnostics_dir"].String()

	cfg.NumWriters = slice.maxNumWriters

	// shard transfer
	loadClientCopyConfig(&cfg.CopyConfig, slice.sysconf, false)

	return cfg
}

func backupCorruptedSlice_Bhive(
	storageDir, prefix string,
	rename func(string) (string, error),
	clean func(string),
) error {
	err := bhive.BackupCorruptedInstance(storageDir, prefix, rename, clean)
	if err != nil {
		return err
	}
	return destroySlice_Bhive(storageDir, prefix)
}

func (slice *bhiveSlice) initStores(isInitialBuild bool, cancelCh chan bool) error {
	var err error

	mCfg := slice.setupMainstoreConfig()
	bCfg := slice.setupBackstoreConfig()

	mCfg.File = filepath.Join(slice.path, "mainIndex")
	bCfg.File = filepath.Join(slice.path, "docIndex")

	enableShardAffinity := slice.sysconf.GetDeploymentAwareShardAffinity() || common.IsServerlessDeployment()

	if enableShardAffinity && len(slice.shardIds) > 0 && slice.IsRebalRunning() {
		mCfg.UseShardId = plasma.ShardId(slice.shardIds[0])
		bCfg.UseShardId = plasma.ShardId(slice.shardIds[1])
	}

	var wg sync.WaitGroup
	var mErr, bErr error
	t0 := time.Now()

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

		slice.backstore, bErr = bhive.New(
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

	if !slice.newBorn {

		logging.Infof("bhiveSlice::doRecovery SliceId %v IndexInstId %v PartitionId %v Recovering from recovery point ..",
			slice.id, slice.idxInstId, slice.idxPartnId)
		err = slice.doRecovery(isInitialBuild)
		dur := time.Since(t0)
		if err == nil {
			slice.idxStats.diskSnapLoadDuration.Set(int64(dur / time.Millisecond))
			logging.Infof("bhiveSlice::doRecovery SliceId %v IndexInstId %v PartitionId %v Warmup took %v",
				slice.id, slice.idxInstId, slice.idxPartnId, dur)
		} else {
			return err
		}
	}

	slice.persistFullVector = mCfg.PersistFullVector

	return err
}

func (mdb *bhiveSlice) doRecovery(initBuild bool) error {
	snaps, err := mdb.GetSnapshots()
	if err != nil {
		return err
	}

	if len(snaps) == 0 {
		logging.Infof("bhiveSlice::doRecovery SliceId %v IndexInstId %v PartitionId %v Unable to find recovery point. Resetting store ..",
			mdb.id, mdb.idxInstId, mdb.idxPartnId)
		if err := mdb.resetStores(initBuild); err != nil {
			return err
		}
	} else {
		err := mdb.restore(snaps[0])
		return err
	}

	return nil
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

// IsDirty returns true if there has been any change in
// in the slice storage after last in-mem/persistent snapshot
//
// flushActive will be true if there are going to be any
// messages in the cmdCh of slice after flush is done.
// It will be cleared during snapshot generation as the
// cmdCh would be empty at the time of snapshot generation
func (mdb *bhiveSlice) IsDirty() bool {
	flushActive := atomic.LoadUint32(&mdb.flushActive)
	if flushActive == 0 { // No flush happening
		return false
	}

	// Flush in progress - wait till all commands on cmdCh
	// are processed
	mdb.waitPersist()
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

func updateBhiveConfig(cfg common.Config) {
	updateMemTunerConfigs(cfg)
	bhive.MTunerOvershootRatio = cfg["plasma.memtuner.overshootRatio"].Float64()
}

func (mdb *bhiveSlice) UpdateConfig(cfg common.Config) {

	mdb.confLock.Lock()
	defer mdb.confLock.Unlock()

	mdb.sysconf = cfg

	updateBhiveConfig(cfg)

	logLevel := cfg["settings.log_level"].String()
	if bc.GetLogLevel() != bc.Level(logLevel) {
		bc.SetLogLevel(bc.Level(logLevel))
		logging.Verbosef("Set bhive log level to %v", logLevel)
	}

	mdb.maxRollbacks = cfg["settings.plasma.recovery.max_rollbacks"].Int()
	mdb.maxDiskSnaps = cfg["recovery.max_disksnaps"].Int()
	mdb.topNScan = cfg["bhive.topNScan"].Int()

	mCfg := mdb.setupMainstoreConfig()
	bCfg := mdb.setupBackstoreConfig()

	mdb.mainstore.UpdateConfig(&mCfg)
	mdb.backstore.UpdateConfig(&bCfg)
}

////////////////////////////////////////////////
// Flusher API
////////////////////////////////////////////////

func (mdb *bhiveSlice) getNextDocSeqno() uint64 {
	return atomic.AddUint64(&mdb.docSeqno, 1)
}

func (mdb *bhiveSlice) Insert(key []byte, docid []byte, includeColumn []byte,
	vectors [][]float32, centroidPos []int32, meta *MutationMeta) error {

	if mdb.CheckCmdChStopped() {
		return mdb.fatalDbErr
	}

	op := opUpdate
	if meta.firstSnap {
		op = opInsert
	}

	mut := &indexMutation{
		op:            op,
		key:           key,
		docid:         docid,
		includeColumn: includeColumn,
		vecs:          vectors,
		centroidPos:   centroidPos,
		meta:          meta,
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
	numWriter := slice.maxNumWriters // numWriters must match numKVStores
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
				nmut = mdb.insert(icmd.key, icmd.docid, icmd.includeColumn, workerId, icmd.op == opInsert, icmd.vecs)
				elapsed = time.Since(start)
				mdb.totalFlushTime += elapsed

			case opDelete:
				start = time.Now()
				nmut = mdb.delete(icmd.docid, workerId)
				elapsed = time.Since(start)
				mdb.totalFlushTime += elapsed

			default:
				logging.Errorf("bhiveSlice::handleCommandsWorker \n\tSliceId %v IndexInstId %v PartitionId %v Received "+
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

func (slice *bhiveSlice) freeAllWriters() {
	// Stop all command workers
	for _, stopCh := range slice.stopCh {
		stopCh <- true
		<-stopCh
	}
}

////////////////////////////////////////////////
// Mutation
////////////////////////////////////////////////

func (mdb *bhiveSlice) insert(key []byte, docid []byte, includeColumn []byte, workerId int, init bool, vecs [][]float32) int {

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
			nmut = mdb.insertVectorIndex(key, docid, includeColumn, workerId, init, vecs)
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
		nmut, _, _ = mdb.deleteVectorIndex(docid, nil, nil, workerId)
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
func (mdb *bhiveSlice) insertVectorIndex(key []byte, docid []byte, includeColumn []byte,
	workerId int, init bool, vecs [][]float32) (nmut int) {

	start := time.Now()
	defer func() {
		mdb.idxStats.Timings.stKVSet.Put(time.Now().Sub(start))
	}()

	if len(vecs) != 1 || vecs[0] == nil {
		err := fmt.Errorf("Fatal - Expected only one non-nil vector for docId: %s, instance: %v. "+
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
	metalen := mdb.codeSize + len(includeColumn)
	mdb.quantizedCodeBuf[workerId] = resizeQuantizedCodeBuf(mdb.quantizedCodeBuf[workerId], 1, metalen, true)

	// compute centroidId and quantized code
	_, centroidId, err := mdb.getQuantizedCodeForVector(vec, mdb.codeSize, mdb.quantizedCodeBuf[workerId])
	if err != nil {
		logging.Errorf("bhiveSlice::insertVectorIndex Slice Id %v IndexInstId %v PartitionId %v "+
			"Skipping docid:%s  due to error in computing centroidId and quantized code.  Error: %v",
			mdb.Id, mdb.idxInstId, mdb.idxPartnId, logging.TagStrUD(docid), err)
		atomic.AddInt32(&mdb.numKeysSkipped, 1)
		panic(err) // [VECTOR_TODO]: Having panics will help catch bugs. Remove panics after code stabilizes
	}

	// First "codeSize" of quantizedCodeBuf will be the quantized code.
	// Re-use that buffer to stitch include columns and quantized code together
	// This will help avoid new memory allocations
	var bhiveMeta []byte
	if len(includeColumn) > 0 {
		copy(mdb.quantizedCodeBuf[workerId][mdb.codeSize:metalen], includeColumn)
		bhiveMeta = mdb.quantizedCodeBuf[workerId][:metalen]
	} else {
		bhiveMeta = mdb.quantizedCodeBuf[workerId][:mdb.codeSize]
	}

	if vec != nil {

		var docSeqno uint64

		// remove old record
		if !init {
			if n, changed, seqno := mdb.deleteVectorIndex(docid, vec, bhiveMeta, workerId); !changed {
				return 0
			} else if n == 1 { // exist
				docSeqno = seqno
			} else { // does not exist
				docSeqno = mdb.getNextDocSeqno()
			}
		} else {
			docSeqno = mdb.getNextDocSeqno()
		}

		mdb.mainWriters[workerId].Begin()
		defer mdb.mainWriters[workerId].End()

		mdb.backWriters[workerId].Begin()
		defer mdb.backWriters[workerId].End()

		var cid [8]byte
		binary.LittleEndian.PutUint64(cid[:], uint64(centroidId))

		var v []byte
		if mdb.persistFullVector {
			// insert full vector into main index
			v = ((bhive.Vector)(vec)).Bytes()
		}

		err = mdb.mainWriters[workerId].Insert(docSeqno, cid[:], docid, bhiveMeta, v)
		if err != nil {
			logging.Errorf("bhiveSlice:insertVectorIndex.  Error during insert main index: msg=%v", err.Error())
		}
		mainIndexEntrySz := int64(len(docid) + 8 + len(bhiveMeta) + len(v))
		if err == nil {
			mdb.idxStats.rawDataSize.Add(mainIndexEntrySz)
			addKeySizeStat(mdb.idxStats, int(mainIndexEntrySz))
			atomic.AddInt64(&mdb.insert_bytes, mainIndexEntrySz)
		}

		// insert into back index
		backIndexEntrySz := 8 + sha256.Size + sha256.Size + 8 // docSeqno + sha(vector) + sha(scalar)+ centroidId
		var buffer [8 + sha256.Size + sha256.Size + 8]byte
		buf := buffer[:]

		offset := 0
		binary.LittleEndian.PutUint64(buf[offset:offset+8], docSeqno)

		offset += 8
		copy(buf[offset:offset+sha256.Size], common.ComputeSHA256ForFloat32Array(vec))

		offset += sha256.Size
		copy(buf[offset:offset+sha256.Size], common.ComputeSHA256ForByteArray(bhiveMeta))

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
func (mdb *bhiveSlice) deleteVectorIndex(docid []byte, vector []float32, fields []byte, workerId int) (nmut int, changed bool, docSeqno uint64) {

	// Delete entry from back and main index if present
	mdb.backWriters[workerId].Begin()
	defer mdb.backWriters[workerId].End()

	itemFound := false
	backEntry, err := mdb.backWriters[workerId].GetLocal(docid)

	if err == nil && len(backEntry) > 0 {
		itemFound = true

		shaVec := common.ComputeSHA256ForFloat32Array(vector)
		shaField := common.ComputeSHA256ForByteArray(fields)

		offset := 0
		docSeqno = binary.LittleEndian.Uint64(backEntry[offset:8])

		offset += 8
		backShaVec := backEntry[offset : offset+sha256.Size]

		offset += sha256.Size
		backShaField := backEntry[offset : offset+sha256.Size]

		offset += sha256.Size
		centroidId := backEntry[offset:]

		// backEntry points to shared magma work context buffer which can get reused
		// It is safer to copy the centroid
		var cid [8]byte
		binary.LittleEndian.PutUint64(cid[:], binary.LittleEndian.Uint64(centroidId))

		// back entry has not changed
		if bytes.Equal(shaVec, backShaVec) && bytes.Equal(shaField, backShaField) {
			return 0, false, docSeqno
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

		err = mdb.mainWriters[workerId].Delete(docSeqno, cid[:], docid)
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
		return 1, true, docSeqno
	} else {
		//nothing deleted
		return 0, true, docSeqno
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

func (mdb *bhiveSlice) createVectorFuncCtx() *bhive.VectorFuncCtx {
	distance := func(v1 []float32, v2 []float32, dist []float32) error {
		return mdb.codebook.ComputeDistance(v1, v2, dist)
	}

	decode := func(n int, q []byte, v []float32) error {
		return mdb.codebook.DecodeVectors(n, q, v)
	}

	sz := func() (int, error) {
		return mdb.codebook.CodeSize()
	}

	coarseSz := func() (int, error) {
		return mdb.codebook.CoarseSize()
	}

	distEncoded := func(q []float32, n int, codes []byte, dist []float32, cid int64) error {
		return mdb.codebook.ComputeDistanceEncoded(q, n, codes, dist, nil, cid)
	}

	useDistEncoded := func() bool {
		return mdb.idxDefn.VectorMeta.Quantizer.Type == common.SQ &&
			mdb.codebook.MetricType() == codebook.METRIC_L2
	}

	dimension := func() int {
		return mdb.codebook.Dimension()
	}

	ctx := &bhive.VectorFuncCtx{
		Distance:        distance,
		Decode:          decode,
		CodeSize:        sz,
		CoarseSize:      coarseSz,
		DistanceEncoded: distEncoded,
		UseDistEncoded:  useDistEncoded,
		Dimension:       dimension,
	}

	return ctx
}

func (mdb *bhiveSlice) SetNlist(nlist int) {
	mdb.nlist = nlist
}

func (mdb *bhiveSlice) GetNlist() int {
	if mdb.idxDefn.IsVectorIndex && mdb.codebook.IsTrained() {
		nlist := mdb.codebook.NumCentroids()
		mdb.nlist = nlist
		return nlist
	}
	return 0
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
	mdb.mainstore.SetVectorFuncCtx(mdb.createVectorFuncCtx())

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

	// reset codebook mem stat
	mdb.idxStats.codebookSize.Set(0)

	if mdb.codebook != nil {
		err := mdb.codebook.Close()
		mdb.codebook = nil
		if err != nil {
			logging.Errorf("bhiveSlice::ResetCodebook Error observed while closing codebook for instId: %v, partnId: %v",
				mdb.IndexInstId(), mdb.IndexPartnId())
			return err
		}
		logging.Infof("bhiveSlice::ResetCodebook closed codebook for instId: %v, partnId: %v", mdb.IndexInstId(), mdb.IndexPartnId())
	}
	return nil
}

func (mdb *bhiveSlice) InitCodebookFromSerialized(content []byte) error {

	codebook, err := vector.RecoverCodebook(content, string(mdb.idxDefn.VectorMeta.Quantizer.Type))
	if err != nil {
		logging.Errorf("bhiveSlice::InitCodebookFromSerialized: Error observed while recovering codebook, err: %v", err)
		return errCodebookCorrupted
	}

	mdb.codebook = codebook
	mdb.codeSize, err = mdb.codebook.CodeSize()
	if err != nil {
		mdb.ResetCodebook()
		return err
	}
	mdb.mainstore.SetVectorFuncCtx(mdb.createVectorFuncCtx())

	mdb.idxStats.codebookSize.Set(mdb.codebook.Size())

	mdb.initQuantizedCodeBuf()
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

	// Update codebook mem stat
	mdb.idxStats.codebookSize.Set(mdb.codebook.Size())

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
	numWriters := mdb.numWriters
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
		logging.Warnf("bhiveSlice::recoverCodebook error observed while recovering from codebookPath: %v, err: %v", newFilePath, err)
		return errCodebookPathNotFound
	}

	// Codebook path exists. Recover codebook from disk
	content, err := iowrap.Ioutil_ReadFile(newFilePath)
	if err != nil {
		logging.Errorf("bhiveSlice::recoverCodebook: Error observed while reading from disk for path: %v, err: %v", newFilePath, err)
		return errCodebookCorrupted
	}

	logging.Infof("bhiveSlice::recoverCodebook: reading from disk is successful for path: %v", newFilePath)

	codebook, err := vector.RecoverCodebook(content, string(mdb.idxDefn.VectorMeta.Quantizer.Type))
	if err != nil {
		logging.Errorf("bhiveSlice::recoverCodebook: Error observed while deserializing codebook at path: %v, err: %v", newFilePath, err)
		return errCodebookCorrupted
	}

	mdb.codebook = codebook
	mdb.codeSize, err = mdb.codebook.CodeSize()
	if err != nil {
		mdb.ResetCodebook() // Ignore error for now
		return err
	}
	mdb.mainstore.SetVectorFuncCtx(mdb.createVectorFuncCtx())

	mdb.idxStats.codebookSize.Set(mdb.codebook.Size())

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
	return mdb.shardIds
}

func (mdb *bhiveSlice) IsRebalRunning() bool {
	return atomic.LoadUint32(&mdb.rebalRunning) == 1
}

func (mdb *bhiveSlice) SetRebalRunning() {
	atomic.StoreUint32(&mdb.rebalRunning, 1)
}

func (mdb *bhiveSlice) ClearRebalRunning() {
	if mdb.IsRebalRunning() {
		logging.Infof("BhiveSlice::ClearRebalRunning Clearing rebalance running for instId: %v, partnId: %v", mdb.idxInstId, mdb.idxPartnId)
	}
	atomic.StoreUint32(&mdb.rebalRunning, 0)
}

func (mdb *bhiveSlice) IsPersistanceActive() bool {
	return mdb.isPersistorRunning()
}

// //////////////////////////////////////////////////////////
// recovery
// //////////////////////////////////////////////////////////

func (mdb *bhiveSlice) RecoveryDone() {
	bhive.RecoveryDone()
}

func (mdb *bhiveSlice) BuildDone(idxInstId common.IndexInstId, callb BuildDoneCallback) {
	count := mdb.mainstore.ItemCount()
	logging.Infof("bhiveSlice: BuildDone.  Item count %v", count)

	atomic.StoreInt32(&mdb.isInitialBuild, 0)

	go mdb.buildGraph(idxInstId, callb)
}

func (mdb *bhiveSlice) buildGraph(idxInstId common.IndexInstId, callb BuildDoneCallback) {

	donech := make(chan bool)

	go func() {
		isNotClosed := mdb.CheckAndIncrRef()

		if isNotClosed {
			defer mdb.DecrRef() // decrement ref after graph building is done

			mdb.mainstore.BuildGraph()
			close(donech)
		} else { // Slice is closed
			logging.Infof("bhiveSlice::buildGraph Returning as slice is closed for instId: %v, partnId: %v",
				mdb.IndexInstId(), mdb.IndexPartnId())
			close(donech)
			return
		}
	}()

	select {
	case <-donech:
		//send message to the indexer
		resp := &MsgBhiveGraphReady{
			idxDefnId:  mdb.idxDefnId,
			idxInstId:  idxInstId, //use proxy instId instead of realInstId for correct metadata update
			idxPartnId: mdb.idxPartnId,
		}
		callb(resp)
	}
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
		ctx.r = nil
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

func (mdb *bhiveSlice) GetSnapshots() ([]SnapshotInfo, error) {
	var mRPs, bRPs []*bhive.RecoveryPoint
	var minRP, maxRP []byte

	getRPs := func(rpts []*bhive.RecoveryPoint) []*bhive.RecoveryPoint {
		var newRpts []*bhive.RecoveryPoint
		for _, rp := range rpts {
			if mdb.cmpRPMeta(rp.GetMeta(), minRP) < 0 {
				continue
			}

			if mdb.cmpRPMeta(rp.GetMeta(), maxRP) > 0 {
				break
			}

			newRpts = append(newRpts, rp)
		}

		return newRpts
	}

	// Find out the common recovery points between mainIndex and backIndex
	mRPs = mdb.mainstore.GetRecoveryPoints()
	if len(mRPs) > 0 {
		minRP = mRPs[0].GetMeta()
		maxRP = mRPs[len(mRPs)-1].GetMeta()
	} else {
		return nil, nil
	}

	bRPs = mdb.backstore.GetRecoveryPoints()
	if len(bRPs) > 0 {
		if mdb.cmpRPMeta(bRPs[0].GetMeta(), minRP) > 0 {
			minRP = bRPs[0].GetMeta()
		}

		if mdb.cmpRPMeta(bRPs[len(bRPs)-1].GetMeta(), maxRP) < 0 {
			maxRP = bRPs[len(bRPs)-1].GetMeta()
		}
	}

	bRPs = getRPs(bRPs)
	mRPs = getRPs(mRPs)

	if len(mRPs) != len(bRPs) {
		return nil, nil
	}

	var infos []SnapshotInfo
	for i := len(mRPs) - 1; i >= 0; i-- {
		info, err := mdb.getRPSnapInfo(mRPs[i])
		if err != nil {
			return nil, err
		}

		info.bRP = bRPs[i]
		infos = append(infos, info)
	}

	return infos, nil
}

// comparing wall clock time in RP meta
func (mdb *bhiveSlice) cmpRPMeta(a, b []byte) int {
	av := binary.BigEndian.Uint64(a[:8])
	bv := binary.BigEndian.Uint64(b[:8])
	return int(av - bv)
}

func (mdb *bhiveSlice) OpenSnapshot(info SnapshotInfo, logOncePerBucket *sync.Once) (Snapshot, error) {
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
		committed:  snapInfo.IsCommitted(),
		codec:      mdb.mainstore.GetCodec(),
	}
	s.Open()

	// increment slice ref count
	if !s.slice.CheckAndIncrRef() {
		return nil, common.ErrSliceClosed
	}

	// increment snapCount
	mdb.snapCount++
	s.id = mdb.snapCount
	s.slice.idxStats.numOpenSnapshots.Add(1)

	var err error
	if s.MainSnap, err = mdb.mainstore.NewSnapshot(); err != nil {
		s.slice.DecrRef()
		mdb.snapCount--
		s.slice.idxStats.numOpenSnapshots.Add(-1)
		return nil, err
	}
	s.info.Count = int64(mdb.mainstore.ItemCount())

	if s.BackSnap, err = mdb.backstore.NewSnapshot(); err != nil {
		s.slice.DecrRef()
		mdb.snapCount--
		s.slice.idxStats.numOpenSnapshots.Add(-1)
		s.MainSnap.Close()
		return nil, err
	}

	if s.committed {
		mdb.doPersistSnapshot(s)
	}

	if info.IsCommitted() {
		logging.Infof("bhiveSlice::OpenSnapshot SliceId %v IndexInstId %v PartitionId %v Creating New "+
			"Snapshot %v", mdb.id, mdb.idxInstId, mdb.idxPartnId, snapInfo)
	}

	mdb.setCommittedCount()

	return s, nil
}

func (mdb *bhiveSlice) doPersistSnapshot(s *bhiveSnapshot) {
	if mdb.CheckCmdChStopped() {
		return
	}

	snapshotStats := make(map[string]interface{})
	snapshotStats[SNAP_STATS_KEY_SIZES] = getKeySizesStats(mdb.idxStats)
	snapshotStats[SNAP_STATS_KEY_SIZES_SINCE] = mdb.idxStats.keySizeStatsSince.Value()
	snapshotStats[SNAP_STATS_RAW_DATA_SIZE] = mdb.idxStats.rawDataSize.Value()
	snapshotStats[SNAP_STATS_BACKSTORE_RAW_DATA_SIZE] = mdb.idxStats.backstoreRawDataSize.Value()
	s.info.IndexStats = snapshotStats

	mdb.persistorLock.Lock()

	// waitgroup to notify completion of bhive magma and lss vindex checkpoints
	var chkpWg sync.WaitGroup

	s.chkpointCh = make(chan bool)
	// a)do not resume mutations until checkpoint callback is completed.
	// This is needed to ensure both vindex(full vector) and pindex (quantized)
	// workers has processed all the mutations for the snapshot.
	// b)lss checkpoint is an memory operation, actual persistence happens lazily
	// c)magma checkpoint currently persists memtables to disk.
	s.chkpointCb = func(b *bhive.Bhive) error {
		chkpWg.Done()
		return nil
	}

	// mainIndex
	chkpWg.Add(1) // lss
	if mdb.persistFullVector {
		chkpWg.Add(1) // magma
	}

	// docIndex
	chkpWg.Add(1) // magma

	s.MainSnap.Open() // close in CreateRecoveryPoint
	s.BackSnap.Open() // close in CreateRecoveryPoint

	if !mdb.isPersistorActive {
		mdb.isPersistorActive = true
		go mdb.persistSnapshot(s)
	} else {
		logging.Infof("bhiveSlice Slice Id %v, IndexInstId %v, PartitionId %v EnQueuing SnapshotId %v ondisk"+
			" snapshot. A snapshot writer is in progress.", mdb.id, mdb.idxInstId, mdb.idxPartnId, s.id)

		mdb.closeQueuedSnapNoLock()
		mdb.persistorQueue = s
	}

	// for an enqueued snapshot, we should release the lock as persistSnapshot
	// reacquires the lock to dequeue.
	mdb.persistorLock.Unlock()

	select {
	case <-s.chkpointCh:
	default:
		chkpWg.Wait()
	}
}

func (mdb *bhiveSlice) persistSnapshot(s *bhiveSnapshot) {
	if mdb.CheckCmdChStopped() {
		close(s.chkpointCh)

		mdb.persistorLock.Lock()
		defer mdb.persistorLock.Unlock()

		mdb.isPersistorActive = false
		return
	}

	logging.Infof("bhiveSlice Slice Id %v, IndexInstId %v, PartitionId %v SnapshotId %v DocSeqno %v "+
		"Creating recovery point ...", mdb.id, mdb.idxInstId, mdb.idxPartnId, s.id, atomic.LoadUint64(&mdb.docSeqno))
	t0 := time.Now()

	s.info.Version = SNAPSHOT_META_VERSION_BHIVE_1
	s.info.InstId = mdb.idxInstId
	s.info.PartnId = mdb.idxPartnId
	s.info.DocSeqno = atomic.LoadUint64(&mdb.docSeqno)

	meta, err := json.Marshal(s.info)
	common.CrashOnError(err)
	timeHdr := make([]byte, 8)
	binary.BigEndian.PutUint64(timeHdr, uint64(time.Now().UnixNano()))
	meta = append(timeHdr, meta...)

	var mErr, bErr error

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		// use plasmaPersistenceMutex to serialize persistence for co-existence
		plasmaPersistenceMutex.Lock()
		defer plasmaPersistenceMutex.Unlock()

		mErr = mdb.mainstore.CreateRecoveryPoint2(s.MainSnap, meta, s.chkpointCb)
		if mErr != nil {
			logging.Errorf("bhiveSlice Slice Id %v, IndexInstId %v, PartitionId %v SnapshotId %v: "+
				"Failed to create mainstore recovery point: %v",
				mdb.id, mdb.idxInstId, mdb.idxPartnId, s.id, mErr)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		// use plasmaPersistenceMutex to serialize persistence for co-existence
		plasmaPersistenceMutex.Lock()
		defer plasmaPersistenceMutex.Unlock()

		bErr = mdb.backstore.CreateRecoveryPoint2(s.BackSnap, meta, s.chkpointCb)
		if bErr != nil {
			logging.Errorf("bhiveSlice Slice Id %v, IndexInstId %v, PartitionId %v SnapshotId %v: "+
				"Failed to create backstore recovery point: %v",
				mdb.id, mdb.idxInstId, mdb.idxPartnId, s.id, bErr)
		}
	}()

	wg.Wait()
	close(s.chkpointCh)

	dur := time.Since(t0)
	if mErr == nil && bErr == nil {
		logging.Infof("bhiveSlice Slice Id %v, IndexInstId %v, PartitionId %v SnapshotId %v "+
			"Created recovery point (took %v)", mdb.id, mdb.idxInstId, mdb.idxPartnId, s.id, dur)
	}

	mdb.idxStats.diskSnapStoreDuration.Set(int64(dur / time.Millisecond))

	// In case there is an error creating one of recovery
	// points, the successful one has to be cleaned up.
	mdb.removeNotCommonRecoveryPoints()

	mdb.cleanupOldRecoveryPoints(s.info)

	mdb.persistorLock.Lock()
	defer mdb.persistorLock.Unlock()

	queuedS := mdb.persistorQueue
	if !mdb.stopPersistor && queuedS != nil {
		mdb.persistorQueue = nil
		go mdb.persistSnapshot(queuedS)
		return
	}
	if queuedS != nil {
		mdb.closeQueuedSnapNoLock()
	}

	mdb.stopPersistor = false
	mdb.isPersistorActive = false
}

// Find rps that are present in only one of mainstore and
// backstore and remove them.
func (mdb *bhiveSlice) removeNotCommonRecoveryPoints() {
	mRPs := mdb.mainstore.GetRecoveryPoints()
	bRPs := mdb.backstore.GetRecoveryPoints()

	for _, rp := range mdb.setDifferenceRPs(mRPs, bRPs) {
		mdb.mainstore.RemoveRecoveryPoint(rp)
	}

	for _, rp := range mdb.setDifferenceRPs(bRPs, mRPs) {
		mdb.backstore.RemoveRecoveryPoint(rp)
	}
}

// Find xRPs - yRPs: rps that are in xRPs, but not in yRPs.
func (mdb *bhiveSlice) setDifferenceRPs(xRPs, yRPs []*bhive.RecoveryPoint) []*bhive.RecoveryPoint {
	var onlyInX []*bhive.RecoveryPoint

	for _, xRP := range xRPs {
		isInY := false
		for _, yRP := range yRPs {
			if cmpRPMeta(xRP.GetMeta(), yRP.GetMeta()) == 0 {
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

// cleanupOldRecoveryPoints deletes old disk snapshots.
func (mdb *bhiveSlice) cleanupOldRecoveryPoints(sinfo *bhiveSnapshotInfo) {

	var seqTs Timestamp

	if !sinfo.IsOSOSnap() {

		seqTs = NewTimestamp(mdb.numVbuckets)
		for i := 0; i < MAX_GETSEQS_RETRIES; i++ {

			seqnos, err := common.BucketMinSeqnos(mdb.clusterAddr, "default", mdb.idxDefn.Bucket, false)
			if err != nil {
				logging.Errorf("bhiveSlice Slice Id %v, IndexInstId %v, PartitionId %v "+
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

	// Cleanup old mainstore recovery points
	mRPs := mdb.mainstore.GetRecoveryPoints()
	numDiskSnapshots := len(mRPs)

	if len(mRPs) > maxRollbacks {
		for i := 0; i < len(mRPs)-maxRollbacks; i++ {
			snapInfo, err := mdb.getRPSnapInfo(mRPs[i])
			if err != nil {
				logging.Errorf("bhiveSlice Slice Id %v, IndexInstId %v, PartitionId %v "+
					"Skipped recovery point cleanup. err %v",
					mdb.id, mdb.idxInstId, mdb.idxPartnId, err)
				continue
			}
			snapTsVbuuid := snapInfo.Timestamp()
			snapTs := getSeqTsFromTsVbuuid(snapTsVbuuid)

			if (seqTs.GreaterThanEqual(snapTs) && //min cluster seqno is greater than snap ts
				mdb.lastRollbackTs == nil) || //last rollback was successful
				len(mRPs)-i > maxDiskSnaps { //num RPs is more than max disk snapshots
				logging.Infof("bhiveSlice Slice Id %v, IndexInstId %v, PartitionId %v "+
					"Cleanup mainstore recovery point %v. num RPs %v.", mdb.id, mdb.idxInstId,
					mdb.idxPartnId, snapInfo, len(mRPs)-i)
				if err := mdb.mainstore.RemoveRecoveryPoint(mRPs[i]); err != nil {
					logging.Errorf("bhiveSlice Slice Id %v, IndexInstId %v, PartitionId %v "+
						"Cleanup mainstore recovery point %v. error %v.", mdb.id, mdb.idxInstId,
						mdb.idxPartnId, snapInfo, err)
				}
				numDiskSnapshots--
			} else {
				logging.Infof("bhiveSlice Slice Id %v, IndexInstId %v, PartitionId %v "+
					"Skipped mainstore recovery point cleanup. num RPs %v ",
					mdb.id, mdb.idxInstId, mdb.idxPartnId, len(mRPs)-i)
				break
			}
		}
	}
	mdb.idxStats.numDiskSnapshots.Set(int64(numDiskSnapshots))

	// Cleanup old backstore recovery points
	bRPs := mdb.backstore.GetRecoveryPoints()
	if len(bRPs) > maxRollbacks {
		for i := 0; i < len(bRPs)-maxRollbacks; i++ {
			snapInfo, err := mdb.getRPSnapInfo(bRPs[i])
			if err != nil {
				logging.Errorf("bhiveSlice Slice Id %v, IndexInstId %v, PartitionId %v "+
					"Skipped recovery point cleanup. err %v",
					mdb.id, mdb.idxInstId, mdb.idxPartnId, err)
				continue
			}
			snapTsVbuuid := snapInfo.Timestamp()
			snapTs := getSeqTsFromTsVbuuid(snapTsVbuuid)
			if (seqTs.GreaterThanEqual(snapTs) && //min cluster seqno is greater than snap ts
				mdb.lastRollbackTs == nil) || //last rollback was successful
				len(bRPs)-i > maxDiskSnaps { //num RPs is more than max disk snapshots
				logging.Infof("bhiveSlice Slice Id %v, IndexInstId %v, PartitionId %v "+
					"Cleanup backstore recovery point %v. num RPs %v.", mdb.id, mdb.idxInstId,
					mdb.idxPartnId, snapInfo, len(bRPs)-i)
				if err := mdb.backstore.RemoveRecoveryPoint(bRPs[i]); err != nil {
					logging.Errorf("bhiveSlice Slice Id %v, IndexInstId %v, PartitionId %v "+
						"Cleanup backstore recovery point %v. err %v", mdb.id, mdb.idxInstId,
						mdb.idxPartnId, snapInfo, err)
				}
			} else {
				logging.Infof("bhiveSlice Slice Id %v, IndexInstId %v, PartitionId %v "+
					"Skipped backstore recovery point cleanup. num RPs %v ",
					mdb.id, mdb.idxInstId, mdb.idxPartnId, len(bRPs)-i)
				break
			}
		}
	}
}

func (mdb *bhiveSlice) getRPSnapInfo(rp *bhive.RecoveryPoint) (*bhiveSnapshotInfo, error) {

	info := &bhiveSnapshotInfo{
		mRP:   rp,
		Count: int64(rp.ItemsCount()),
	}

	var err error
	var snapInfo bhiveSnapshotInfo
	if err = json.Unmarshal(info.mRP.GetMeta()[8:], &snapInfo); err != nil {
		return nil, fmt.Errorf("Unable to decode snapshot info from meta. err %v", err)
	}
	info.Ts = snapInfo.Ts
	info.IndexStats = snapInfo.IndexStats
	info.Version = snapInfo.Version
	info.InstId = snapInfo.InstId
	info.PartnId = snapInfo.PartnId
	info.DocSeqno = snapInfo.DocSeqno
	return info, nil
}

func (mdb *bhiveSlice) closeQueuedSnapNoLock() {
	deQueuedS := mdb.persistorQueue
	if deQueuedS != nil {
		deQueuedS.MainSnap.Close()
		deQueuedS.BackSnap.Close()
		close(deQueuedS.chkpointCh)
		logging.Infof("bhiveSlice Slice Id %v, IndexInstId %v, PartitionId %v DeQueuing SnapshotId %v ondisk"+
			" snapshot.", mdb.id, mdb.idxInstId, mdb.idxPartnId, deQueuedS.id)
	}
	mdb.persistorQueue = nil
}

// Wait for persistence snapshot to finish.
// Note the indexer storage manager is a singleton which
// process one command at at time. So if this function is
// called, then storage mgr will not create another persistent
// snapshot in parallel.
func (mdb *bhiveSlice) waitForPersistorThread() {
	persistorActive := func() bool {
		mdb.persistorLock.Lock()
		defer mdb.persistorLock.Unlock()
		if mdb.isPersistorActive {
			mdb.stopPersistor = true
		}
		return mdb.isPersistorActive
	}()

	for persistorActive {
		time.Sleep(time.Second)
		persistorActive = mdb.isPersistorRunning()
	}
}

func (mdb *bhiveSlice) isPersistorRunning() bool {
	mdb.persistorLock.Lock()
	defer mdb.persistorLock.Unlock()

	return mdb.isPersistorActive
}

// //////////////////////////////////////////////////////////
// rollback
// //////////////////////////////////////////////////////////

func (mdb *bhiveSlice) Rollback(s SnapshotInfo) error {
	mdb.waitPersist()
	mdb.waitForPersistorThread()

	qc := atomic.LoadInt64(&mdb.qCount)
	if qc > 0 {
		common.CrashOnError(fmt.Errorf("Slice Invariant Violation - rollback with pending mutations"))
	}

	// Block all scan requests if mdb.idxDefn.IsVectorIndex
	numReaders := cap(mdb.readers)
	mdb.ReserveReaders(numReaders, nil)
	defer mdb.ReleaseReaders(numReaders)

	var readers []*bhive.Reader
	for i := 0; i < cap(mdb.readers); i++ {
		readers = append(readers, <-mdb.readers)
	}

	err := mdb.restore(s)
	for i := 0; i < cap(mdb.readers); i++ {
		mdb.readers <- readers[i]
	}

	return err
}

func (mdb *bhiveSlice) RollbackToZero(initialBuild bool) error {
	mdb.waitPersist()
	mdb.waitForPersistorThread()

	if err := mdb.resetStores(initialBuild); err != nil {
		return err
	}

	mdb.lastRollbackTs = nil
	if initialBuild {
		atomic.StoreInt32(&mdb.isInitialBuild, 1)
	}

	// During rollback to zero, initialise the quantizedCodeBuf. Rest of the metadata is still valid
	if mdb.idxDefn.IsVectorIndex {
		mdb.initQuantizedCodeBuf()
	}

	return nil
}

func (mdb *bhiveSlice) LastRollbackTs() *common.TsVbuuid {
	return mdb.lastRollbackTs
}

func (mdb *bhiveSlice) SetLastRollbackTs(ts *common.TsVbuuid) {
	mdb.lastRollbackTs = ts
}

func (mdb *bhiveSlice) restore(o SnapshotInfo) error {
	var wg sync.WaitGroup
	var mErr, bErr error
	info := o.(*bhiveSnapshotInfo)
	wg.Add(1)
	go func() {
		defer wg.Done()
		var s bhive.Snapshot
		if s, mErr = mdb.mainstore.Rollback(info.mRP); mErr == nil {
			s.Close()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		var s bhive.Snapshot
		if s, bErr = mdb.backstore.Rollback(info.bRP); bErr == nil {
			s.Close()
		}
	}()

	wg.Wait()

	if mErr != nil || bErr != nil {
		return fmt.Errorf("Rollback error %v %v", mErr, bErr)
	}

	// Update stats available in snapshot info
	mdb.updateStatsFromSnapshotMeta(o)
	atomic.StoreUint64(&mdb.docSeqno, info.DocSeqno)

	logging.Infof("bhiveSlice::Rollback Slice Id %v IndexInstId %v PartitionId %v DocSeqno %v",
		mdb.Id, mdb.idxInstId, mdb.idxPartnId, atomic.LoadUint64(&mdb.docSeqno))

	return nil
}

// updateStatsFromSnapshotMeta updates the slice stats from those available in SnapshotInfo.
func (mdb *bhiveSlice) updateStatsFromSnapshotMeta(o SnapshotInfo) {

	// Update stats *if* available in snapshot info
	// In case of upgrade, older snapshots will not have stats
	// in which case, do not update index stats
	// Older snapshots may have only subset of stats persisted
	// For stats not available through persistence, set default values
	stats := o.Stats()
	if stats != nil {
		keySizes := stats[SNAP_STATS_KEY_SIZES].([]interface{})

		if keySizes != nil && len(keySizes) == 6 {
			mdb.idxStats.numKeySize64.Set(safeGetInt64(keySizes[0]))
			mdb.idxStats.numKeySize256.Set(safeGetInt64(keySizes[1]))
			mdb.idxStats.numKeySize1K.Set(safeGetInt64(keySizes[2]))
			mdb.idxStats.numKeySize4K.Set(safeGetInt64(keySizes[3]))
			mdb.idxStats.numKeySize100K.Set(safeGetInt64(keySizes[4]))
			mdb.idxStats.numKeySizeGt100K.Set(safeGetInt64(keySizes[5]))
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

func (mdb *bhiveSlice) resetReaders() {
	for i := 0; i < cap(mdb.readers); i++ {
		<-mdb.readers
	}
}

func (slice *bhiveSlice) resetBuffers() {
	slice.stopWriters(0)

	slice.cmdCh = slice.cmdCh[:0]
	slice.stopCh = slice.stopCh[:0]

	slice.mainWriters = slice.mainWriters[:0]
	slice.backWriters = slice.backWriters[:0]
}

func (mdb *bhiveSlice) resetStores(initBuild bool) error {
	// Clear all readers if mdb.idxDefn.IsVectorIndex
	mdb.ReserveReaders(cap(mdb.readers), nil)
	defer mdb.ReleaseReaders(cap(mdb.readers))

	mdb.resetReaders()

	numWriters := mdb.numWriters
	mdb.freeAllWriters()
	mdb.resetBuffers()

	mdb.mainstore.Close()
	mdb.backstore.Close()

	// codebook is not destroyed on rollback to zero for faster to serve scans.
	// on data drift user is expected to drop and rebuild.
	if err := bhive.DestroyInstance(mdb.storageDir, mdb.path); err != nil {
		return err
	}

	mdb.newBorn = true
	if err := mdb.initStores(initBuild, nil); err != nil {
		return err
	}

	mdb.startWriters(numWriters)
	mdb.setCommittedCount()

	mdb.resetStats()
	return nil
}

func (mdb *bhiveSlice) cleanupWritersOnClose() {
	mdb.freeAllWriters()
}

func (mdb *bhiveSlice) resetStats() {

	mdb.idxStats.itemsCount.Set(0)

	resetKeySizeStats(mdb.idxStats)
	// Slice is rolling back to zero, but there is no need to update keySizeStatsSince

	mdb.idxStats.backstoreRawDataSize.Set(0)
	mdb.idxStats.rawDataSize.Set(0)

	mdb.idxStats.lastDiskBytes.Set(0)
	mdb.idxStats.lastNumItemsFlushed.Set(0)
	mdb.idxStats.lastNumDocsIndexed.Set(0)
	mdb.idxStats.lastNumFlushQueued.Set(0)
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
	var cacheHits, cacheMiss, rcacheHits, rcacheMiss int64
	var combinedIndexSz int64

	mStats := mdb.mainstore.GetStats()
	numRecsMem += int64(mStats.RecInMem)
	numRecsDisk += int64(mStats.RecOnDisk)
	cacheHits += int64(mStats.CacheHits)
	cacheMiss += int64(mStats.CacheMisses)
	rcacheHits += int64(mStats.RCacheHits)
	rcacheMiss += int64(mStats.RCacheMisses)
	combinedIndexSz += int64(mStats.MemUsedIndex)

	sts.MemUsed = int64(mStats.MemUsed) // MemUsed include buffer usage
	sts.InsertBytes = int64(mStats.NWriteBytes)
	sts.GetBytes = int64(mStats.NReadBytes)
	sts.DiskSize = int64(mStats.TotalDiskUsage)
	sts.DataSizeOnDisk = int64(mStats.TotalDataSize)
	sts.LogSpace = int64(mStats.TotalDiskUsage)
	sts.DataSize = int64(float32(mStats.TotalDataSize) * mStats.CompressionRatio)

	sts.GraphBuildProgress = mStats.GraphBuildProgress

	bStats := mdb.backstore.GetStats()
	docidCount = int64(bStats.ItemCount)
	combinedIndexSz += int64(bStats.MemUsedIndex)

	sts.MemUsed += int64(bStats.MemUsed) // MemUsed include buffer usage
	sts.InsertBytes += int64(bStats.NWriteBytes)
	sts.GetBytes += int64(bStats.NReadBytes)
	sts.DiskSize += int64(bStats.TotalDiskUsage)
	sts.DataSizeOnDisk += int64(bStats.TotalDataSize)
	sts.LogSpace += int64(bStats.TotalDiskUsage)
	sts.DataSize += int64(float32(bStats.TotalDataSize) * bStats.CompressionRatio)

	mainStoreStatsLoggingEnabled := false
	backStoreStatsLoggingEnabled := false

	if mStats.StatsLoggingEnabled || (consumerFilter == statsMgmt.AllStatsFilter) {
		mainStoreStatsLoggingEnabled = true
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
	}

	if bStats.StatsLoggingEnabled || (consumerFilter == statsMgmt.AllStatsFilter) {
		backStoreStatsLoggingEnabled = true
		if mainStoreStatsLoggingEnabled {
			internalData = append(internalData, fmt.Sprintf(",\n\"BackStore\":\n%s", bStats))
		} else {
			internalData = append(internalData, fmt.Sprintf("{\n\"BackStore\":\n%s", bStats))
		}

		statsMap2 := make(map[string]interface{})
		if err := json.Unmarshal([]byte(bStats.String()), &statsMap2); err == nil {
			internalDataMap["BackStore"] = statsMap2
		} else {
			logging.Errorf("bhiveSlice::Statistics unable to unmarshal backstore stats for"+
				" IndexInstId %v, PartitionId %v, IndexDefnId %v SliceId %v err: %v",
				mdb.idxInstId, mdb.idxPartnId, mdb.idxDefnId, mdb.id, err)
			internalDataMap["BackStore"] = fmt.Sprintf("%v", bStats)
		}
	}

	if mainStoreStatsLoggingEnabled || backStoreStatsLoggingEnabled {
		internalData = append(internalData, "}\n")
	}

	sts.InternalData = internalData
	sts.InternalDataMap = internalDataMap
	sts.LoggingDisabled = (mainStoreStatsLoggingEnabled == false) && (backStoreStatsLoggingEnabled == false)

	mdb.idxStats.docidCount.Set(docidCount)
	mdb.idxStats.residentPercent.Set(common.ComputePercent(numRecsMem, numRecsDisk))
	mdb.idxStats.cacheHitPercent.Set(common.ComputePercent(cacheHits, cacheMiss))
	mdb.idxStats.rCacheHitPercent.Set(common.ComputePercent(rcacheHits, rcacheMiss))
	mdb.idxStats.combinedResidentPercent.Set(common.ComputePercentFloat(numRecsMem, numRecsDisk))
	mdb.idxStats.cacheHits.Set(cacheHits)
	mdb.idxStats.cacheMisses.Set(cacheMiss)
	mdb.idxStats.numRecsInMem.Set(numRecsMem)
	mdb.idxStats.numRecsOnDisk.Set(numRecsDisk)
	mdb.idxStats.bsNumRecsInMem.Set(0)  // back index does not contribute to resident ratio
	mdb.idxStats.bsNumRecsOnDisk.Set(0) // back index does not contribute to resident ratio
	mdb.idxStats.combinedMemSzIndex.Set(combinedIndexSz)

	return sts, nil
}

func (mdb *bhiveSlice) handleN1QLStorageStatistics() (StorageStatistics, error) {
	var sts StorageStatistics

	mstats := mdb.mainstore.GetPreparedStats()
	internalData := fmt.Sprintf("{\n\"MainStore\":\n"+
		"{\n"+
		"\"items_count\":%d,\n"+
		"\"avg_item_size\":%.5f,\n"+
		"\"graph_resident_ratio\":%.5f,\n"+
		"\"graph_hit_ratio\":%.2f,\n"+
		"\"num_vec_ops_per_cell\":%d,\n"+
		"\"norm_graph_disk_size\":%.5f,\n"+
		"\"norm_full_vector_size\":%.5f\n}\n}",
		mstats.ItemCount,
		mstats.AvgItemSz,
		mstats.GraphRR,
		mstats.GraphHitRatio,
		mstats.NumVectorOp,
		mstats.NormGraphDiskSz,
		mstats.NormFullVectorSz)

	sts.InternalData = []string{internalData}

	internalDataMap := make(map[string]interface{})
	internalDataMap["items_count"] = mstats.ItemCount
	internalDataMap["avg_item_size"] = mstats.AvgItemSz
	internalDataMap["graph_resident_ratio"] = mstats.GraphRR
	internalDataMap["graph_hit_ratio"] = mstats.GraphHitRatio
	internalDataMap["num_vec_ops_per_cell"] = mstats.NumVectorOp
	internalDataMap["norm_graph_disk_size"] = mstats.NormGraphDiskSz
	internalDataMap["norm_full_vector_size"] = mstats.NormFullVectorSz

	sts.InternalDataMap = make(map[string]interface{})
	sts.InternalDataMap["MainStore"] = internalDataMap
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
	ss.MemSzIndex = val.MemSzIndex
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
			logging.Infof("bhiveSlice::ShardStatistics ShardInfo is not available for shard: %v", bsAlternateShardId)
			return nil
		}
		ss.MemSz += val.MemSz
		ss.MemSzIndex += val.MemSzIndex
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

func (mdb *bhiveSlice) String() string {

	str := fmt.Sprintf("SliceId: %v ", mdb.id)
	str += fmt.Sprintf("File: %v ", mdb.path)
	str += fmt.Sprintf("Index: %v ", mdb.idxInstId)
	str += fmt.Sprintf("Partition: %v ", mdb.idxPartnId)

	return str

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

	// Stop consumer of cmdCh - this will return after the current in process
	// mutation completes and consumer loop is terminated
	mdb.cleanupWritersOnClose()

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
		mdb.resetReaders()
		mdb.resetBuffers()
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
	mdb.lock.Lock()
	defer mdb.lock.Unlock()

	if mdb.refCount > 0 {
		openSnaps := mdb.idxStats.numOpenSnapshots.Value()
		logging.Infof("bhiveSlice::Destroy Soft deleted Slice Id %v, IndexInstId %v, PartitionId %v "+
			"IndexDefnId %v RefCount %v NumOpenSnapshots %v", mdb.id, mdb.idxInstId, mdb.idxPartnId,
			mdb.idxDefnId, mdb.refCount, openSnaps)
		mdb.isSoftDeleted = true
	} else {
		mdb.isDeleted = true
		tryDeleteBhiveSlice(mdb)
	}
}

func tryDeleteBhiveSlice(mdb *bhiveSlice) {

	//cleanup the disk directory
	if err := destroySlice_Bhive(mdb.storageDir, mdb.path); err != nil {
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

	mdb.waitForPersistorThread()
	mdb.mainstore.Close()
	mdb.backstore.Close()
	mdb.ResetCodebook() // Ignore error for now
}

func destroySlice_Bhive(storageDir string, path string) error {
	if err := bhive.DestroyInstance(storageDir, path); err != nil {
		return err
	}

	// remove directory created in newBhiveSlice()
	return iowrap.Os_RemoveAll(path)
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
	return s.MainSnap.Count(), nil
}

func (s *bhiveSnapshot) StatCountTotal() (uint64, error) {
	c := s.slice.GetCommittedCount()
	return c, nil
}

func (s *bhiveSnapshot) Iterate(ctx IndexReaderContext, centroidId IndexKey, queryKey IndexKey, limit int64,
	queryTopNScan int, callb EntryCallback, fincb FinishCallback, inlineFilterCb InlineFilterCallback) error {

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

	topNScan := queryTopNScan
	if topNScan == 0 {
		topNScan = s.slice.topNScan
	}

	iter, err := reader.r.NewKeyPrefixIterator()
	if err != nil {
		return err
	}

	defer iter.Close()

	//call fincb before iterator close. This allows caller to do
	//any final actions before iterator resources get freed up.
	if fincb != nil {
		defer fincb()
	}

	if inlineFilterCb != nil {
		iter.SetInlineFilterCallback(inlineFilterCb)
	}

	// Capture the time taken to initialize new iterator
	s.slice.idxStats.Timings.stNewIterator.Put(time.Since(t0))

	// [VECTOR_TODO]: Add more timing stats

	if queryKey != nil {
		q := ([]float32)(bhive.BytesToVec(queryKey.Bytes()))
		err = iter.FindNearest2(s.MainSnap, bhive.CentroidID(centroidId.Bytes()), q, topNScan, int(limit))
		if err != nil {
			return err
		}
	} else {
		err = iter.Execute(s.MainSnap, bhive.CentroidID(centroidId.Bytes()))
		if err != nil {
			return err
		}
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

func (s *bhiveSnapshot) Range(ctx IndexReaderContext, low IndexKey, high IndexKey,
	incl Inclusion, callb EntryCallback, fincb FinishCallback) error {
	//if low.CompareIndexKey(high) != 0 || incl != Both {
	//	panic(fmt.Errorf("bhiveSnapshot::Range low: %v and high: %v should be same for Range on bhive snapshot with inclusion: %v being Both", low, high, incl))
	//}
	return s.Iterate(ctx, low, high, 0, 0, callb, fincb, nil)
}

func (s *bhiveSnapshot) Range2(ctx IndexReaderContext, low, high IndexKey,
	inclusion Inclusion, limit int64, queryTopNScan int, callb EntryCallback, fincb FinishCallback,
	inlineFilterCb InlineFilterCallback) error { // Supported only for BHIVE storage engine
	return s.Iterate(ctx, low, high, limit, queryTopNScan, callb, fincb, inlineFilterCb)
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

	err := s.Range(ctx, low, high, inclusion, callb, nil)
	return count, err
}

func (s *bhiveSnapshot) Lookup(ctx IndexReaderContext, centroidId IndexKey,
	callb EntryCallback, fincb FinishCallback) error {
	return s.Iterate(ctx, centroidId, nil, 0, 0, callb, fincb, nil)
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
		if err = s.Lookup(ctx, k, callb, nil); err != nil {
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

	err := s.Lookup(ctx, key, callb, nil)
	return count != 0, err
}

// VECTOR_TODO: Add support for multi scan count. Till then panic
func (s *bhiveSnapshot) MultiScanCount(ctx IndexReaderContext, low, high IndexKey, inclusion Inclusion,
	scan Scan, distinct bool, stopch StopChannel) (uint64, error) {
	panic("bhiveSnapshot::MultiScanCount - Currently not supported")
}

// VECTOR_TODO: All can be implemented using KeyIterator by scanning the entire storage
func (s *bhiveSnapshot) All(IndexReaderContext, EntryCallback, FinishCallback) error {
	panic("bhiveSnapshot::All - Currently not supported")
}

func (s *bhiveSnapshot) DecodeMeta(meta []byte) (uint64, uint64, []byte) {
	storeId, recordId, actualMeta := s.codec.DecodeMeta(nil, meta)
	return uint64(storeId), uint64(recordId), actualMeta
}

func (s *bhiveSnapshot) FetchValue(ctx IndexReaderContext, storeId uint64, recordId uint64, cid []byte, buf []byte) ([]byte, error) {

	// [VECTOR_TODO]: Add timings stats for FetchValue
	reader := ctx.(*bhiveReaderCtx)
	reader.r.Begin()
	defer reader.r.End()

	mainSnap := s.MainSnap
	err := reader.r.FetchValue(bhive.StoreId(storeId), bhive.RecordId(recordId), mainSnap, buf)

	return buf, err
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
