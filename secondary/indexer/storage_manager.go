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
	"encoding/gob"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	forestdb "github.com/couchbase/indexing/secondary/fdb"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/stubs/nitro/plasma"
)

var (
	ErrIndexRollback            = errors.New("Indexer rollback")
	ErrIndexRollbackOrBootstrap = errors.New("Indexer rollback or warmup")
)

type KeyspaceIdInstList map[string][]common.IndexInstId
type StreamKeyspaceIdInstList map[common.StreamId]KeyspaceIdInstList

type KeyspaceIdInstsPerWorker map[string][][]common.IndexInstId
type StreamKeyspaceIdInstsPerWorker map[common.StreamId]KeyspaceIdInstsPerWorker

//StorageManager manages the snapshots for the indexes and responsible for storing
//indexer metadata in a config database

const INST_MAP_KEY_NAME = "IndexInstMap"
const MIN_QUOTA_THRESH_PERCENT = int64(10)

type StorageManager interface {
}

type storageMgr struct {
	supvCmdch  MsgChannel //supervisor sends commands on this channel
	supvRespch MsgChannel //channel to send any async message to supervisor
	wrkrCh     MsgChannel // Channel to listen all messages from worker

	snapshotReqCh []MsgChannel // Channel to listen for snapshot requests from scan coordinator

	snapshotNotifych []chan IndexSnapshot

	indexInstMap  IndexInstMapHolder
	indexPartnMap IndexPartnMapHolder

	bucketNameNumVBucketsMapHolder common.BucketNameNumVBucketsMapHolder

	streamKeyspaceIdInstList       StreamKeyspaceIdInstListHolder
	streamKeyspaceIdInstsPerWorker StreamKeyspaceIdInstsPerWorkerHolder

	// Latest readable index snapshot for each index instance
	indexSnapMap IndexSnapMapHolder
	// List of waiters waiting for a snapshot to be created with expected
	// atleast-timestamp
	waitersMap SnapshotWaitersMapHolder

	dbfile *forestdb.File
	meta   *forestdb.KVStore // handle for index meta

	config common.Config

	stats IndexerStatsHolder

	muSnap sync.Mutex //lock to protect updates to snapMap and waitersMap

	statsLock sync.Mutex

	lastFlushDone int64

	stm *ShardTransferManager

	// List of shards that are currently in rebalance tranfser phase
	// A shard is added to the list when transfer is initiated and
	// cleared when transfer is done
	shardsInTransfer map[common.ShardId][]chan bool

	// used to send a signal to the quota distributor
	// - sending true in the channel will cause the go routine to stop
	// - sending false in the channel will force a memory quota distribution
	quotaDistCh chan bool

	lastCodebokMemLogTime uint64

	// used to phase out the minQuotaThreshold over time
	plasmaLastCreateTime, bhiveLastCreateTime time.Time
}

type snapshotWaiter struct {
	wch       chan interface{}
	ts        *common.TsVbuuid
	cons      common.Consistency
	idxInstId common.IndexInstId
	expired   time.Time
}

type PartnSnapMap map[common.PartitionId]PartitionSnapshot

func newSnapshotWaiter(idxId common.IndexInstId, ts *common.TsVbuuid,
	cons common.Consistency,
	ch chan interface{}, expired time.Time) *snapshotWaiter {

	return &snapshotWaiter{
		ts:        ts,
		cons:      cons,
		wch:       ch,
		idxInstId: idxId,
		expired:   expired,
	}
}

func (w *snapshotWaiter) Notify(is IndexSnapshot) {
	w.wch <- is
}

func (w *snapshotWaiter) Error(err error) {
	w.wch <- err
}

// NewStorageManager returns an instance of storageMgr or err message
// It listens on supvCmdch for command and every command is followed
// by a synchronous response of the supvCmdch.
// Any async response to supervisor is sent to supvRespch.
// If supvCmdch get closed, storageMgr will shut itself down.
func NewStorageManager(supvCmdch MsgChannel, supvRespch MsgChannel,
	indexPartnMap IndexPartnMap, config common.Config, snapshotNotifych []chan IndexSnapshot,
	snapshotReqCh []MsgChannel, stats *IndexerStats) (StorageManager, Message) {

	//Init the storageMgr struct
	s := &storageMgr{
		supvCmdch:        supvCmdch,
		supvRespch:       supvRespch,
		snapshotNotifych: snapshotNotifych,
		snapshotReqCh:    snapshotReqCh,
		config:           config,

		wrkrCh:           make(chan Message, 100),
		shardsInTransfer: make(map[common.ShardId][]chan bool),

		quotaDistCh: make(chan bool),
	}
	s.indexInstMap.Init()
	s.indexPartnMap.Init()
	s.indexSnapMap.Init()
	s.waitersMap.Init()
	s.stats.Set(stats)

	s.streamKeyspaceIdInstList.Init()
	s.streamKeyspaceIdInstsPerWorker.Init()

	s.bucketNameNumVBucketsMapHolder.Init()

	//if manager is not enabled, create meta file
	if config["enableManager"].Bool() == false {
		fdbconfig := forestdb.DefaultConfig()
		kvconfig := forestdb.DefaultKVStoreConfig()
		var err error

		if s.dbfile, err = forestdb.Open("meta", fdbconfig); err != nil {
			return nil, &MsgError{err: Error{cause: err}}
		}

		// Make use of default kvstore provided by forestdb
		if s.meta, err = s.dbfile.OpenKVStore("default", kvconfig); err != nil {
			return nil, &MsgError{err: Error{cause: err}}
		}
	}

	s.stm = NewShardTransferManager(s.config, s.wrkrCh)

	for i := 0; i < len(s.snapshotReqCh); i++ {
		go s.listenSnapshotReqs(i)
	}

	//start Storage Manager loop which listens to commands from its supervisor
	go s.run()

	if config["plasma.UseQuotaTuner"].Bool() {
		plasma.RunMemQuotaTuner(
			s.quotaDistCh,
			s.getStorageQuota,
			s.getStorageTunerConfig,
			s.getStorageTunerStats,
		)
	}

	return s, &MsgSuccess{}

}

// run starts the storage manager loop which listens to messages
// from its supervisor(indexer)
func (s *storageMgr) run() {

	logging.Infof("storageMgr::run starting...")
	//main Storage Manager loop
loop:
	for {
		select {

		case cmd, ok := <-s.supvCmdch:
			if ok {
				if cmd.GetMsgType() == STORAGE_MGR_SHUTDOWN {
					logging.Infof("StorageManager::run Shutting Down")
					for i := 0; i < len(s.snapshotNotifych); i++ {
						close(s.snapshotNotifych[i])
					}
					if s.stm != nil {
						s.stm.ProcessCommand(cmd) // Shutdown storage manager cmdCh
					}

					s.quotaDistCh <- true
					<-s.quotaDistCh

					s.supvCmdch <- &MsgSuccess{}
					break loop
				}
				s.handleSupvervisorCommands(cmd)
			} else {
				//supervisor channel closed. exit
				break loop
			}

		case cmd, ok := <-s.wrkrCh:
			if ok {
				s.handleWorkerCommands(cmd)
			} else {
				break loop
			}
		}
	}
}

func respWithErr(cmd Message, err error) {
	// respWithErr can get blocking if the sender is not reading from the respCh before storageMgr
	// sends a message to respCh. use this go-routine to not block storageMgr run loop. the main
	// idea is to avoid any deadlock situation where storageMgr is blocked on respCh but the sender
	// has also send another message on supervisor channel, which gets blocked until storageMgr
	// responds to its command channel for current message, before reading from respCh

	go func(cmd Message, err error) { // pass params to put them on stack rather than escaping to heap
		logging.Errorf("%v", err)
		errMsg := &MsgError{
			err: Error{
				code:     ERROR_INDEXER_INTERNAL_ERROR,
				severity: FATAL,
				cause:    err,
			},
		}
		switch cmd.GetMsgType() {
		case START_SHARD_TRANSFER:
			msg := cmd.(*MsgStartShardTransfer)
			respCh := msg.GetRespCh()
			respCh <- errMsg
		case START_SHARD_RESTORE:
			msg := cmd.(*MsgStartShardRestore)
			respCh := msg.GetRespCh()
			respCh <- errMsg
		case START_PEER_SERVER,
			STOP_PEER_SERVER:
			msg := cmd.(*MsgPeerServerCommand)
			respCh := msg.GetRespCh()
			respCh <- err
		case LOCK_SHARDS,
			UNLOCK_SHARDS, RESTORE_AND_UNLOCK_LOCKED_SHARDS:
			msg := cmd.(*MsgLockUnlockShards)
			respCh := msg.GetRespCh()
			shardIDs := msg.GetShardIds()
			errMap := make(map[common.ShardId]error)
			for _, shardID := range shardIDs {
				errMap[shardID] = err
			}
			respCh <- errMap
		default:
			// only message types which expect an error in resp get a reply. for all others we log error
		}
	}(cmd, err)
}

func (s *storageMgr) handleSupvervisorCommands(cmd Message) {

	switch cmd.GetMsgType() {

	case MUT_MGR_FLUSH_DONE:
		s.handleCreateSnapshot(cmd)

	case INDEXER_ROLLBACK:
		s.handleRollback(cmd)

	case UPDATE_INDEX_INSTANCE_MAP:
		s.handleUpdateIndexInstMap(cmd)

	case UPDATE_INDEX_PARTITION_MAP:
		s.handleUpdateIndexPartnMap(cmd)

	case UPDATE_KEYSPACE_STATS_MAP:
		s.handleUpdateKeyspaceStatsMap(cmd)

	case STORAGE_INDEX_SNAP_REQUEST:
		s.handleGetIndexSnapshot(cmd)

	case STORAGE_INDEX_STORAGE_STATS:
		s.handleGetIndexStorageStats(cmd)

	case STORAGE_INDEX_COMPACT:
		s.handleIndexCompaction(cmd)

	case STORAGE_STATS:
		s.handleStats(cmd)

	case SHARD_STORAGE_STATS:
		s.handleShardStorageStats(cmd)

	case STORAGE_INDEX_MERGE_SNAPSHOT:
		s.handleIndexMergeSnapshot(cmd)

	case STORAGE_INDEX_PRUNE_SNAPSHOT:
		s.handleIndexPruneSnapshot(cmd)

	case STORAGE_UPDATE_SNAP_MAP:
		s.handleUpdateIndexSnapMapForIndex(cmd)

	case INDEXER_ACTIVE:
		s.handleRecoveryDone()

	case CONFIG_SETTINGS_UPDATE:
		s.handleConfigUpdate(cmd)

	case UPDATE_NUMVBUCKETS:
		s.handleUpdateNumVBuckets(cmd)

	case START_SHARD_TRANSFER:
		s.handleShardTransfer(cmd)

	case SHARD_TRANSFER_CLEANUP,
		SHARD_TRANSFER_STAGING_CLEANUP,
		START_SHARD_RESTORE,
		DESTROY_LOCAL_SHARD,
		MONITOR_SLICE_STATUS,
		RESTORE_SHARD_DONE,
		START_PEER_SERVER,
		STOP_PEER_SERVER:
		if s.stm != nil {
			s.stm.ProcessCommand(cmd)
		} else {
			respWithErr(
				cmd,
				fmt.Errorf("StorageMgr::handleSupervisorCommands ShardTransferManager Not Initialized during msg %v execution",
					cmd),
			)
		}
		s.supvCmdch <- &MsgSuccess{}

	case DESTROY_EMPTY_SHARD:

		// Note: The response to indexer has to be sent after processing "handleDestroyEmptyShards"
		// Otherwise, indexer main loop can get unblocked and a new index creation might start
		// while plasma has returned shard info. This can lead to a race where storage manager
		// is trying to delete a shard on which indexer is trying placing a new shard
		s.handleDestroyEmptyShards()
		s.supvCmdch <- &MsgSuccess{}

	case LOCK_SHARDS:
		s.SetRebalanceRunning(cmd)

		if s.stm != nil {
			s.stm.ProcessCommand(cmd)
		} else {
			respWithErr(
				cmd,
				fmt.Errorf("StorageMgr::handleSupervisorCommands ShardTransferManager Not Initialized during msg %v execution",
					cmd),
			)
		}
		s.supvCmdch <- &MsgSuccess{}

	case UNLOCK_SHARDS,
		RESTORE_AND_UNLOCK_LOCKED_SHARDS:

		// At this point, rebalance is either completed or a particular group
		// of shard movement is complete. Clear rebalance running flag for
		// corresponding slices
		s.ClearRebalanceRunning(cmd)
		if s.stm != nil {
			s.stm.ProcessCommand(cmd)
		} else {
			respWithErr(
				cmd,
				fmt.Errorf("StorageMgr::handleSupervisorCommands ShardTransferManager Not Initialized during msg %v execution",
					cmd),
			)
		}
		s.supvCmdch <- &MsgSuccess{}

	case PERSISTANCE_STATUS:
		s.supvCmdch <- &MsgSuccess{}

		s.handlePersistanceStatus(cmd)

	case INDEXER_SECURITY_CHANGE:
		if s.stm != nil {
			s.stm.ProcessCommand(cmd)
		}
		s.supvCmdch <- &MsgSuccess{}

	case POPULATE_SHARD_TYPE:
		s.handlePopulateShardType(cmd)
		s.supvCmdch <- &MsgSuccess{}

	case CLEAR_SHARD_TYPE:
		s.handleClearShardType(cmd)
		s.supvCmdch <- &MsgSuccess{}

	case BHIVE_BUILD_GRAPH:
		s.handleBuildBhiveGraph(cmd)

	case TIMESTAMPED_COUNT_STATS:
		s.handleGetTimestampedItemsCount(cmd)

	}

}

func (s *storageMgr) handleWorkerCommands(cmd Message) {
	switch cmd.GetMsgType() {
	case SHARD_TRANSFER_RESPONSE:

		shardIds := cmd.(*MsgShardTransferResp).GetShardIds()
		respCh := cmd.(*MsgShardTransferResp).GetRespCh()
		for _, shardId := range shardIds {
			delete(s.shardsInTransfer, shardId)
		}
		logging.Infof("StorageMgr::ShardTransferResponse Clearing book-keeping for shardIds: %v", shardIds)
		respCh <- cmd

	case CODEBOOK_TRANSFER_RESPONSE:

		respCh := cmd.(*MsgCodebookTransferResp).GetRespCh()
		shardIds := cmd.(*MsgCodebookTransferResp).GetShardIds()
		codebookPaths := cmd.(*MsgCodebookTransferResp).GetCodebookPaths()

		var codebookNames []string
		for _, codebookPath := range codebookPaths {
			codebookNames = append(codebookNames, filepath.Base(codebookPath))
		}
		logging.Infof("StorageMgr::ShardTransferResponse Received Response for shardIds: %v, codebookNames: %v",
			shardIds, codebookNames)
		respCh <- cmd
	}
}

// handleCreateSnapshot will create the necessary snapshots
// after flush has completed
func (s *storageMgr) handleCreateSnapshot(cmd Message) {

	logging.Tracef("StorageMgr::handleCreateSnapshot %v", cmd)

	msgFlushDone := cmd.(*MsgMutMgrFlushDone)

	keyspaceId := msgFlushDone.GetKeyspaceId()
	tsVbuuid := msgFlushDone.GetTS()
	streamId := msgFlushDone.GetStreamId()
	flushWasAborted := msgFlushDone.GetAborted()
	hasAllSB := msgFlushDone.HasAllSB()

	snapType := tsVbuuid.GetSnapType()
	tsVbuuid.Crc64 = common.HashVbuuid(tsVbuuid.Vbuuids)

	//if snapType is FORCE_COMMIT_MERGE, sync response
	//will be sent after snasphot creation
	if snapType != common.FORCE_COMMIT_MERGE {
		s.supvCmdch <- &MsgSuccess{}
	}

	streamKeyspaceIdInstList := s.streamKeyspaceIdInstList.Get()
	instIdList := streamKeyspaceIdInstList[streamId][keyspaceId]

	if len(instIdList) == 0 {
		logging.Infof("storageMgr::handleCreateSnapshot Skipping snapshot creation as instIdList "+
			"is zero for streamId: %v, keyspaceId: %v", streamId, keyspaceId)
		if snapType == common.FORCE_COMMIT_MERGE {
			s.supvCmdch <- &MsgSuccess{} // send response as storage manager is not going to create snapshot
		}
		return
	}

	streamKeyspaceIdInstsPerWorker := s.streamKeyspaceIdInstsPerWorker.Get()
	instsPerWorker := streamKeyspaceIdInstsPerWorker[streamId][keyspaceId]
	// The num_snapshot_workers config has changed. Re-adjust the
	// streamKeyspaceIdInstsPerWorker map according to new snapshot workers
	numSnapshotWorkers := s.getNumSnapshotWorkers()
	if len(instsPerWorker) != numSnapshotWorkers {
		func() {
			s.muSnap.Lock()
			defer s.muSnap.Unlock()

			newStreamKeyspaceIdInstsPerWorker := getStreamKeyspaceIdInstsPerWorker(streamKeyspaceIdInstList, numSnapshotWorkers)
			s.streamKeyspaceIdInstsPerWorker.Set(newStreamKeyspaceIdInstsPerWorker)
			instsPerWorker = newStreamKeyspaceIdInstsPerWorker[streamId][keyspaceId]
			logging.Infof("StorageMgr::handleCreateSnapshot Re-adjusting the streamKeyspaceIdInstsPerWorker map to %v workers. "+
				"StreamId: %v, keyspaceId: %v", numSnapshotWorkers, streamId, keyspaceId)
		}()
	}

	s.muSnap.Lock()

	if snapType == common.NO_SNAP || snapType == common.NO_SNAP_OSO {
		logging.Debugf("StorageMgr::handleCreateSnapshot Skip Snapshot For %v "+
			"%v SnapType %v", streamId, keyspaceId, snapType)

		indexInstMap := s.indexInstMap.Get()
		indexPartnMap := s.indexPartnMap.Get()

		go s.flushDone(streamId, keyspaceId, indexInstMap, indexPartnMap,
			instIdList, tsVbuuid, flushWasAborted, hasAllSB)

		s.muSnap.Unlock()
		return
	}

	//pass copy of maps to worker
	indexInstMap := s.indexInstMap.Get()
	indexPartnMap := s.indexPartnMap.Get()
	indexSnapMap := s.indexSnapMap.Get()
	tsVbuuid_copy := tsVbuuid.Copy()
	stats := s.stats.Get()

	s.muSnap.Unlock()

	s.assertOnNonAlignedDiskCommit(streamId, keyspaceId, tsVbuuid_copy)

	var logOncePerBucket sync.Once
	if snapType == common.FORCE_COMMIT_MERGE {
		//response is sent on supvCmdch in case of FORCE_COMMIT_MERGE
		s.createSnapshotWorker(streamId, keyspaceId, tsVbuuid_copy, indexSnapMap,
			indexInstMap, indexPartnMap, instIdList, instsPerWorker, stats,
			flushWasAborted, hasAllSB, s.supvCmdch, &logOncePerBucket)
	} else {
		go s.createSnapshotWorker(streamId, keyspaceId, tsVbuuid_copy, indexSnapMap,
			indexInstMap, indexPartnMap, instIdList, instsPerWorker, stats,
			flushWasAborted, hasAllSB, s.supvRespch, &logOncePerBucket)
	}

}

func (s *storageMgr) createSnapshotWorker(streamId common.StreamId, keyspaceId string,
	tsVbuuid *common.TsVbuuid, indexSnapMap IndexSnapMap,
	indexInstMap common.IndexInstMap, indexPartnMap IndexPartnMap,
	instIdList []common.IndexInstId, instsPerWorker [][]common.IndexInstId,
	stats *IndexerStats, flushWasAborted bool, hasAllSB bool, respch MsgChannel,
	logOncePerBucket *sync.Once) {

	startTime := time.Now().UnixNano()
	var needsCommit bool
	var forceCommit bool
	snapType := tsVbuuid.GetSnapType()
	if snapType == common.DISK_SNAP ||
		snapType == common.DISK_SNAP_OSO {
		needsCommit = true
	} else if snapType == common.FORCE_COMMIT || snapType == common.FORCE_COMMIT_MERGE {
		forceCommit = true
	}

	var wg sync.WaitGroup
	wg.Add(len(instIdList))
	for _, instListPerWorker := range instsPerWorker {
		go func(instList []common.IndexInstId) {
			for _, idxInstId := range instList {
				s.createSnapshotForIndex(streamId, keyspaceId, indexInstMap,
					indexPartnMap, indexSnapMap, idxInstId, tsVbuuid,
					stats, hasAllSB, flushWasAborted, needsCommit, forceCommit,
					&wg, startTime, logOncePerBucket)
			}
		}(instListPerWorker)
	}

	wg.Wait()

	keyspaceStats := s.stats.GetKeyspaceStats(streamId, keyspaceId)
	end := time.Now().UnixNano()
	if keyspaceStats != nil {
		if keyspaceStats.lastSnapDone.Value() == 0 {
			keyspaceStats.lastSnapDone.Set(end)
		}
		keyspaceStats.snapLatDist.Add(end - keyspaceStats.lastSnapDone.Value())
		keyspaceStats.lastSnapDone.Set(end)
	}

	s.lastFlushDone = end

	respch <- &MsgMutMgrFlushDone{mType: STORAGE_SNAP_DONE,
		streamId:   streamId,
		keyspaceId: keyspaceId,
		ts:         tsVbuuid,
		aborted:    flushWasAborted}

}

func (s *storageMgr) createSnapshotForIndex(streamId common.StreamId,
	keyspaceId string, indexInstMap common.IndexInstMap,
	indexPartnMap IndexPartnMap, indexSnapMap IndexSnapMap,
	idxInstId common.IndexInstId, tsVbuuid *common.TsVbuuid, stats *IndexerStats,
	hasAllSB bool, flushWasAborted bool, needsCommit bool,
	forceCommit bool, wg *sync.WaitGroup, startTime int64,
	logOncePerBucket *sync.Once) {

	idxInst := indexInstMap[idxInstId]
	//process only if index belongs to the flushed keyspaceId and stream
	if idxInst.Defn.KeyspaceId(idxInst.Stream) != keyspaceId ||
		idxInst.Stream != streamId ||
		idxInst.State == common.INDEX_STATE_DELETED {
		wg.Done()
		return
	}

	idxStats := stats.indexes[idxInst.InstId]
	snapC := indexSnapMap[idxInstId]
	snapC.Lock()
	lastIndexSnap := CloneIndexSnapshot(snapC.snap)
	defer DestroyIndexSnapshot(lastIndexSnap)
	snapC.Unlock()

	// Signal the wait group first before destroying the snapshot
	// inorder to avoid the cost of destroying the snapshot in the
	// snapshot generation code path
	defer wg.Done()

	// List of snapshots for reading current timestamp
	var isSnapCreated bool = true

	partnSnaps := make(map[common.PartitionId]PartitionSnapshot)
	hasNewSnapshot := false

	partnMap := indexPartnMap[idxInstId]
	//for all partitions managed by this indexer
	for _, partnInst := range partnMap {
		partnId := partnInst.Defn.GetPartitionId()

		var lastPartnSnap PartitionSnapshot

		if lastIndexSnap != nil && len(lastIndexSnap.Partitions()) != 0 {
			lastPartnSnap = lastIndexSnap.Partitions()[partnId]
		}
		sc := partnInst.Sc

		sliceSnaps := make(map[SliceId]SliceSnapshot)
		//create snapshot for all the slices
		for _, slice := range sc.GetAllSlices() {

			if flushWasAborted {
				slice.IsDirty()
				return
			}

			//if TK has seen all Stream Begins after stream restart,
			//the MTR after rollback can be considered successful.
			//All snapshots become eligible to retry for next rollback.
			if hasAllSB {
				slice.SetLastRollbackTs(nil)
			}

			var latestSnapshot Snapshot
			if lastPartnSnap != nil {
				lastSliceSnap := lastPartnSnap.Slices()[slice.Id()]
				latestSnapshot = lastSliceSnap.Snapshot()
			}

			//if flush timestamp is greater than last
			//snapshot timestamp, create a new snapshot
			var snapTs Timestamp
			if latestSnapshot != nil {
				snapTsVbuuid := latestSnapshot.Timestamp()
				snapTs = Timestamp(snapTsVbuuid.Seqnos)
			} else {
				bucketNameNumVBucketsMap := s.bucketNameNumVBucketsMapHolder.Get()
				numVBuckets := bucketNameNumVBucketsMap[idxInst.Defn.Bucket]
				snapTs = NewTimestamp(numVBuckets)
			}

			// Get Seqnos from TsVbuuid
			ts := Timestamp(tsVbuuid.Seqnos)

			//if flush is active for an instance and the flush TS is
			// greater than the last snapshot TS and slice has some changes.
			// Skip only in-memory snapshot in case of unchanged data.
			if latestSnapshot == nil ||
				((slice.IsDirty() || needsCommit) && ts.GreaterThan(snapTs)) ||
				forceCommit {

				newTsVbuuid := tsVbuuid
				var err error
				var info SnapshotInfo
				var newSnapshot Snapshot

				logging.Tracef("StorageMgr::handleCreateSnapshot Creating New Snapshot "+
					"Index: %v PartitionId: %v SliceId: %v Commit: %v Force: %v", idxInstId,
					partnId, slice.Id(), needsCommit, forceCommit)

				if forceCommit {
					needsCommit = forceCommit
				}

				slice.FlushDone()

				snapCreateStart := time.Now()
				if info, err = slice.NewSnapshot(newTsVbuuid, needsCommit); err != nil {
					isSnapCreated = false
					if err != common.ErrSliceClosed {
						logging.Errorf("handleCreateSnapshot::handleCreateSnapshot Error "+
							"Creating new snapshot Slice Index: %v Slice: %v. Skipped. Error %v", idxInstId,
							slice.Id(), err)
						common.CrashOnError(err)
					}
					continue
				}
				snapCreateDur := time.Since(snapCreateStart)

				hasNewSnapshot = true

				snapOpenStart := time.Now()
				if newSnapshot, err = slice.OpenSnapshot(info, logOncePerBucket); err != nil {
					isSnapCreated = false
					if err != common.ErrSliceClosed {
						logging.Errorf("StorageMgr::handleCreateSnapshot Error Creating Snapshot "+
							"for Index: %v Slice: %v. Skipped. Error %v", idxInstId,
							slice.Id(), err)
						common.CrashOnError(err)
					}
					continue
				}
				snapOpenDur := time.Since(snapOpenStart)

				if needsCommit {
					logging.Infof("StorageMgr::handleCreateSnapshot Added New Snapshot Index: %v "+
						"PartitionId: %v SliceId: %v Crc64: %v (%v) SnapType %v SnapAligned %v "+
						"SnapCreateDur %v SnapOpenDur %v", idxInstId, partnId, slice.Id(),
						tsVbuuid.Crc64, info, tsVbuuid.GetSnapType(), tsVbuuid.IsSnapAligned(),
						snapCreateDur, snapOpenDur)
				}
				ss := &sliceSnapshot{
					id:   slice.Id(),
					snap: newSnapshot,
				}
				sliceSnaps[slice.Id()] = ss
			} else {
				// Increment reference
				latestSnapshot.Open()
				ss := &sliceSnapshot{
					id:   slice.Id(),
					snap: latestSnapshot,
				}
				sliceSnaps[slice.Id()] = ss

				if logging.IsEnabled(logging.Debug) {
					logging.Debugf("StorageMgr::handleCreateSnapshot Skipped Creating New Snapshot for Index %v "+
						"PartitionId %v SliceId %v. No New Mutations. IsDirty %v", idxInstId, partnId, slice.Id(), slice.IsDirty())
					logging.Debugf("StorageMgr::handleCreateSnapshot SnapTs %v FlushTs %v", snapTs, ts)
				}
				continue
			}
		}

		ps := &partitionSnapshot{
			id:     partnId,
			slices: sliceSnaps,
		}
		partnSnaps[partnId] = ps
	}

	if hasNewSnapshot {
		idxStats.numSnapshots.Add(1)
		if needsCommit {
			idxStats.numCommits.Add(1)
		}
	}

	is := &indexSnapshot{
		instId: idxInstId,
		ts:     tsVbuuid,
		partns: partnSnaps,
	}

	if isSnapCreated {
		s.updateSnapMapAndNotify(is, idxStats)
	} else {
		DestroyIndexSnapshot(is)
	}
	s.updateSnapIntervalStat(idxStats, startTime)
}

func (s *storageMgr) flushDone(streamId common.StreamId, keyspaceId string,
	indexInstMap common.IndexInstMap, indexPartnMap IndexPartnMap,
	instIdList []common.IndexInstId, tsVbuuid *common.TsVbuuid,
	flushWasAborted bool, hasAllSB bool) {

	isInitial := func() bool {
		if streamId == common.INIT_STREAM {
			return true
		}

		// TODO (Collections): It is not optimal to iterate over
		// entire instIdList when there are large number of indexes on
		// a node in MAINT_STREAM. This logic needs to be optimised further
		// when writer tuning is enabled
		for _, instId := range instIdList {
			inst, ok := indexInstMap[instId]
			if ok && inst.Stream == streamId &&
				inst.Defn.KeyspaceId(inst.Stream) == keyspaceId &&
				(inst.State == common.INDEX_STATE_INITIAL || inst.State == common.INDEX_STATE_CATCHUP) {
				return true
			}
		}

		return false
	}

	checkInterval := func() int64 {

		if isInitial() {
			return int64(time.Minute)
		}

		return math.MaxInt64
	}

	if common.GetStorageMode() == common.PLASMA {

		if s.config["plasma.writer.tuning.enable"].Bool() &&
			time.Now().UnixNano()-s.lastFlushDone > checkInterval() {

			var wg sync.WaitGroup

			for _, idxInstId := range instIdList {
				wg.Add(1)
				go func(idxInstId common.IndexInstId) {
					defer wg.Done()

					idxInst := indexInstMap[idxInstId]
					if idxInst.Defn.KeyspaceId(idxInst.Stream) == keyspaceId &&
						idxInst.Stream == streamId &&
						idxInst.State != common.INDEX_STATE_DELETED {

						partnMap := indexPartnMap[idxInstId]
						for _, partnInst := range partnMap {
							for _, slice := range partnInst.Sc.GetAllSlices() {
								slice.FlushDone()
							}
						}
					}
				}(idxInstId)
			}

			wg.Wait()
			s.lastFlushDone = time.Now().UnixNano()
		}
	}

	//if TK has seen all Stream Begins after stream restart,
	//the MTR after rollback can be considered successful.
	//All snapshots become eligible to retry for next rollback.
	if hasAllSB {
		for idxInstId, partnMap := range indexPartnMap {
			idxInst := indexInstMap[idxInstId]
			if idxInst.Defn.KeyspaceId(idxInst.Stream) == keyspaceId &&
				idxInst.Stream == streamId &&
				idxInst.State != common.INDEX_STATE_DELETED {
				for _, partnInst := range partnMap {
					for _, slice := range partnInst.Sc.GetAllSlices() {
						slice.SetLastRollbackTs(nil)
					}
				}
			}
		}
	}

	s.supvRespch <- &MsgMutMgrFlushDone{
		mType:      STORAGE_SNAP_DONE,
		streamId:   streamId,
		keyspaceId: keyspaceId,
		ts:         tsVbuuid,
		aborted:    flushWasAborted}
}

func (s *storageMgr) updateSnapIntervalStat(idxStats *IndexStats, startTime int64) {

	// Compute avgTsInterval
	last := idxStats.lastTsTime.Value()
	curr := int64(time.Now().UnixNano())
	avg := idxStats.avgTsInterval.Value()

	avg = common.ComputeAvg(avg, last, curr)
	if avg != 0 {
		idxStats.avgTsInterval.Set(avg)
		idxStats.sinceLastSnapshot.Set(curr - last)
	}
	idxStats.snapGenLatDist.Add(curr - startTime)
	idxStats.lastTsTime.Set(curr)

	idxStats.updateAllPartitionStats(
		func(idxStats *IndexStats) {

			// Compute avgTsItemsCount
			last = idxStats.lastNumFlushQueued.Value()
			curr = idxStats.numDocsFlushQueued.Value()
			avg = idxStats.avgTsItemsCount.Value()

			avg = common.ComputeAvg(avg, last, curr)
			idxStats.avgTsItemsCount.Set(avg)
			idxStats.lastNumFlushQueued.Set(curr)
		})
}

// Update index-snapshot map whenever a snapshot is created for an index
func (s *storageMgr) updateSnapMapAndNotify(is IndexSnapshot, idxStats *IndexStats) {

	var snapC *IndexSnapshotContainer
	var ok, updated bool
	indexSnapMap := s.indexSnapMap.Get()
	if snapC, ok = indexSnapMap[is.IndexInstId()]; !ok {
		func() {
			s.muSnap.Lock()
			defer s.muSnap.Unlock()
			snapC, updated = s.initSnapshotContainerForInst(is.IndexInstId(), is)
		}()
	}
	if snapC == nil {
		DestroyIndexSnapshot(is)
		logging.Infof("StorageMgr::updateSnapMapAndNotify - Destroying the last snapshot for index: %v", is.IndexInstId())
		return
	}

	if updated == false {
		snapC.Lock()
		if snapC.deleted {
			// Index is deleted and the snapshot is already destroyed. Skip destroying old snapshot
			// Destroy current snapshot
			DestroyIndexSnapshot(is)
			logging.Infof("StorageMgr::updateSnapMapAndNotify - Destroying the last known snapshot for index: %v "+
				"as index is deleted", is.IndexInstId())
			snapC.Unlock()
			return
		} else { // Destroy old index snapshot and update new index snapshot
			DestroyIndexSnapshot(snapC.snap)
			snapC.snap = is
		}
		snapC.Unlock()
	}

	// notify a new snapshot through channel
	// the channel receiver needs to destroy snapshot when done
	s.notifySnapshotCreation(is)

	var waitersContainer *SnapshotWaitersContainer
	waiterMap := s.waitersMap.Get()
	if waitersContainer, ok = waiterMap[is.IndexInstId()]; !ok {
		waitersContainer = s.initSnapshotWaitersForInst(is.IndexInstId())
	}

	if waitersContainer == nil {
		return
	}

	waitersContainer.Lock()
	defer waitersContainer.Unlock()
	waiters := waitersContainer.waiters

	var numReplies int64
	t := time.Now()
	// Also notify any waiters for snapshots creation
	var newWaiters []*snapshotWaiter
	for _, w := range waiters {
		// Clean up expired requests from queue
		if !w.expired.IsZero() && t.After(w.expired) {
			snapTs := is.Timestamp()
			logSnapInfoAtTimeout(snapTs, w.ts, is.IndexInstId(), "updateSnapMapAndNotify", idxStats.lastTsTime.Value())
			w.Error(common.ErrScanTimedOut)
			idxStats.numSnapshotWaiters.Add(-1)
			continue
		}

		if isSnapshotConsistent(is, w.cons, w.ts) {
			w.Notify(CloneIndexSnapshot(is))
			numReplies++
			idxStats.numSnapshotWaiters.Add(-1)
			continue
		}
		newWaiters = append(newWaiters, w)
	}
	waitersContainer.waiters = newWaiters
	idxStats.numLastSnapshotReply.Set(numReplies)
}

func (sm *storageMgr) getSortedPartnInst(partnMap PartitionInstMap) partitionInstList {

	if len(partnMap) == 0 {
		return partitionInstList(nil)
	}

	result := make(partitionInstList, 0, len(partnMap))
	for _, partnInst := range partnMap {
		result = append(result, partnInst)
	}

	sort.Sort(result)
	return result
}

// handleRollback will rollback to given timestamp
func (sm *storageMgr) handleRollback(cmd Message) {

	sm.supvCmdch <- &MsgSuccess{}

	// During rollback, some of the snapshot stats get reset
	// or updated by slice. Therefore, serialise rollback and
	// retrieving stats from slice to avoid any inconsistency
	// in stats
	sm.statsLock.Lock()
	defer sm.statsLock.Unlock()

	streamId := cmd.(*MsgRollback).GetStreamId()
	rollbackTs := cmd.(*MsgRollback).GetRollbackTs()
	keyspaceId := cmd.(*MsgRollback).GetKeyspaceId()
	sessionId := cmd.(*MsgRollback).GetSessionId()

	logging.Infof("StorageMgr::handleRollback %v %v rollbackTs %v", streamId, keyspaceId, rollbackTs)

	var err error
	var restartTs *common.TsVbuuid
	var rollbackToZero bool

	indexInstMap := sm.indexInstMap.Get()
	indexPartnMap := sm.indexPartnMap.Get()
	//for every index managed by this indexer
	for idxInstId, partnMap := range indexPartnMap {
		idxInst := indexInstMap[idxInstId]

		//if this keyspace in stream needs to be rolled back
		if idxInst.Defn.KeyspaceId(idxInst.Stream) == keyspaceId &&
			idxInst.Stream == streamId &&
			idxInst.State != common.INDEX_STATE_DELETED {

			restartTs, err = sm.rollbackIndex(streamId,
				keyspaceId, rollbackTs, idxInstId, partnMap, restartTs, idxInst.State)

			if err != nil {
				sm.supvRespch <- &MsgRollbackDone{streamId: streamId,
					keyspaceId: keyspaceId,
					err:        err,
					sessionId:  sessionId}
				return
			}

			if restartTs == nil {

				err = sm.rollbackAllToZero(streamId, keyspaceId)
				if err != nil {
					sm.supvRespch <- &MsgRollbackDone{streamId: streamId,
						keyspaceId: keyspaceId,
						err:        err,
						sessionId:  sessionId}
					return
				}
				rollbackToZero = true
				break
			}
		}
	}

	go func() {
		// Notify all scan waiters for indexes in this keyspaceId
		// and stream with error
		stats := sm.stats.Get()
		waitersMap := sm.waitersMap.Get()
		for idxInstId, wc := range waitersMap {
			idxInst := sm.indexInstMap.Get()[idxInstId]
			idxStats := stats.indexes[idxInst.InstId]
			if idxInst.Defn.KeyspaceId(idxInst.Stream) == keyspaceId &&
				idxInst.Stream == streamId {
				wc.Lock()
				for _, w := range wc.waiters {
					w.Error(ErrIndexRollback)
					if idxStats != nil {
						idxStats.numSnapshotWaiters.Add(-1)
					}
				}
				wc.waiters = nil
				wc.Unlock()
			}
		}
	}()

	sm.updateIndexSnapMap(sm.indexPartnMap.Get(), streamId, keyspaceId)

	keyspaceStats := sm.stats.GetKeyspaceStats(streamId, keyspaceId)
	if keyspaceStats != nil {
		keyspaceStats.numRollbacks.Add(1)
		if rollbackToZero {
			keyspaceStats.numRollbacksToZero.Add(1)
		}
	}

	if restartTs != nil {
		//for pre 7.0 index snapshots, the manifestUID needs to be set to epoch
		restartTs.SetEpochManifestUIDIfEmpty()
		restartTs = sm.validateRestartTsVbuuid(keyspaceId, restartTs)
	}

	sm.supvRespch <- &MsgRollbackDone{streamId: streamId,
		keyspaceId: keyspaceId,
		restartTs:  restartTs,
		sessionId:  sessionId,
	}
}

func (sm *storageMgr) rollbackIndex(streamId common.StreamId, keyspaceId string,
	rollbackTs *common.TsVbuuid, idxInstId common.IndexInstId,
	partnMap PartitionInstMap, minRestartTs *common.TsVbuuid,
	instState common.IndexState) (*common.TsVbuuid, error) {

	var restartTs *common.TsVbuuid
	var err error

	markAsUsed := true

	//for all partitions managed by this indexer
	partnInstList := sm.getSortedPartnInst(partnMap)
	for _, partnInst := range partnInstList {
		partnId := partnInst.Defn.GetPartitionId()
		sc := partnInst.Sc

		for _, slice := range sc.GetAllSlices() {
			snapInfo := sm.findRollbackSnapshot(slice, rollbackTs)

			restartTs, err = sm.rollbackToSnapshot(idxInstId, partnId,
				slice, snapInfo, markAsUsed, instState)

			if err != nil {
				return nil, err
			}

			if restartTs == nil {
				return nil, nil
			}

			if minRestartTs == nil {
				minRestartTs = restartTs
			} else {
				// compute the minimum timestamp for each vbucket and use that
				// for restarting the streams
				minRestartTs = common.ComputeMinTs(minRestartTs, restartTs)
			}
		}
	}
	return minRestartTs, nil
}

func (sm *storageMgr) findRollbackSnapshot(slice Slice,
	rollbackTs *common.TsVbuuid) SnapshotInfo {

	infos, err := slice.GetSnapshots()
	if err != nil {
		panic("Unable read snapinfo -" + err.Error())
	}
	s := NewSnapshotInfoContainer(infos)

	//DCP doesn't allow using incomplete OSO snapshots
	//for stream restart
	for _, si := range s.List() {
		if si.IsOSOSnap() {
			return nil
		}
	}

	//It is better to try with all available disk snapshots on DCP
	//rollback. There can be cases where vbucket replica is behind indexer.
	//When such a replica gets promoted to active, it may ask to rollback
	//as indexer is asking to start using snapshot/seqno/vbuuid it is not aware of.
	var snapInfo SnapshotInfo
	lastRollbackTs := slice.LastRollbackTs()
	latestSnapInfo := s.GetLatest()

	if latestSnapInfo == nil || lastRollbackTs == nil {
		logging.Infof("StorageMgr::handleRollback %v latestSnapInfo %v "+
			"lastRollbackTs %v. Use latest snapshot.", slice.IndexInstId(), latestSnapInfo,
			lastRollbackTs)
		snapInfo = latestSnapInfo
	} else {
		slist := s.List()
		for i, si := range slist {
			if lastRollbackTs.Equal(si.Timestamp()) {
				//if there are more snapshots, use the next one
				if len(slist) >= i+2 {
					snapInfo = slist[i+1]
					logging.Infof("StorageMgr::handleRollback %v Discarding Already Used "+
						"Snapshot %v. Using Next snapshot %v", slice.IndexInstId(), si, snapInfo)
				} else {
					logging.Infof("StorageMgr::handleRollback %v Unable to find a snapshot "+
						"older than last used Snapshot %v. Use nil snapshot.", slice.IndexInstId(),
						latestSnapInfo)
					snapInfo = nil
				}
				break
			} else {
				//if lastRollbackTs is set(i.e. MTR after rollback wasn't completely successful)
				//use only snapshots lower than lastRollbackTs
				logging.Infof("StorageMgr::handleRollback %v Discarding Snapshot %v. Need older "+
					"than last used snapshot %v.", slice.IndexInstId(), si, lastRollbackTs)
			}
		}
	}

	return snapInfo

}

func (sm *storageMgr) rollbackToSnapshot(idxInstId common.IndexInstId,
	partnId common.PartitionId, slice Slice, snapInfo SnapshotInfo,
	markAsUsed bool, instState common.IndexState) (*common.TsVbuuid, error) {

	isInitialBuild := func() bool {
		return instState == common.INDEX_STATE_INITIAL || instState == common.INDEX_STATE_CATCHUP ||
			instState == common.INDEX_STATE_CREATED || instState == common.INDEX_STATE_READY
	}

	var restartTs *common.TsVbuuid
	if snapInfo != nil {
		err := slice.Rollback(snapInfo)
		if err == nil {
			logging.Infof("StorageMgr::handleRollback Rollback Index: %v "+
				"PartitionId: %v SliceId: %v To Snapshot %v ", idxInstId, partnId,
				slice.Id(), snapInfo)
			restartTs = snapInfo.Timestamp().Copy()
			if markAsUsed {
				slice.SetLastRollbackTs(restartTs.Copy())
			}
		} else {
			//send error response back
			return nil, err
		}

	} else {
		if sm.stm != nil { // Only for shard rebalancer
			sm.waitForTransferCompletion(slice.GetShardIds())
		}
		//if there is no snapshot available, rollback to zero
		err := slice.RollbackToZero(isInitialBuild())
		if err == nil {
			logging.Infof("StorageMgr::handleRollback Rollback Index: %v "+
				"PartitionId: %v SliceId: %v To Zero ", idxInstId, partnId,
				slice.Id())
			//once rollback to zero has happened, set response ts to nil
			//to represent the initial state of storage
			restartTs = nil
			slice.SetLastRollbackTs(nil)
		} else {
			//send error response back
			return nil, err
		}
	}
	return restartTs, nil
}

func (sm *storageMgr) rollbackAllToZero(streamId common.StreamId,
	keyspaceId string) error {

	logging.Infof("StorageMgr::rollbackAllToZero %v %v", streamId, keyspaceId)

	indexPartnMap := sm.indexPartnMap.Get()
	indexInstMap := sm.indexInstMap.Get()
	for idxInstId, partnMap := range indexPartnMap {
		idxInst := indexInstMap[idxInstId]

		//if this keyspace in stream needs to be rolled back
		if idxInst.Defn.KeyspaceId(idxInst.Stream) == keyspaceId &&
			idxInst.Stream == streamId &&
			idxInst.State != common.INDEX_STATE_DELETED {

			partnInstList := sm.getSortedPartnInst(partnMap)
			for _, partnInst := range partnInstList {
				partnId := partnInst.Defn.GetPartitionId()
				sc := partnInst.Sc

				for _, slice := range sc.GetAllSlices() {
					_, err := sm.rollbackToSnapshot(idxInstId, partnId,
						slice, nil, false, idxInst.State)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func (sm *storageMgr) validateRestartTsVbuuid(keyspaceId string,
	restartTs *common.TsVbuuid) *common.TsVbuuid {

	clusterAddr := sm.config["clusterAddr"].String()
	bucket, _, _ := SplitKeyspaceId(keyspaceId)

	for i := 0; i < MAX_GETSEQS_RETRIES; i++ {

		flog, err := common.BucketFailoverLog(clusterAddr, DEFAULT_POOL, bucket)

		if err != nil {
			logging.Warnf("StorageMgr::validateRestartTsVbuuid Bucket %v. "+
				"Error fetching failover log %v. Retrying(%v).", bucket, err, i+1)
			time.Sleep(time.Second)
			continue
		} else {
			//for each seqnum find the lowest recorded vbuuid in failover log
			//this safeguards in cases memcached loses a vbuuid that was sent
			//to indexer. Note that this cannot help in case memcached loses
			//both mutation and vbuuid.

			for i, seq := range restartTs.Seqnos {
				lowest, err := flog.LowestVbuuid(i, seq)
				if err == nil && lowest != 0 &&
					lowest != restartTs.Vbuuids[i] {
					logging.Infof("StorageMgr::validateRestartTsVbuuid Updating Bucket %v "+
						"Vb %v Seqno %v Vbuuid From %v To %v. Flog %v", bucket, i, seq,
						restartTs.Vbuuids[i], lowest, flog[i])
					restartTs.Vbuuids[i] = lowest
				}
			}
			break
		}
	}
	return restartTs
}

// The caller of this method should acquire muSnap Lock
func (s *storageMgr) initSnapshotContainerForInst(instId common.IndexInstId,
	is IndexSnapshot) (*IndexSnapshotContainer, bool) {
	indexInstMap := s.indexInstMap.Get()
	if inst, ok := indexInstMap[instId]; !ok || inst.State == common.INDEX_STATE_DELETED {
		return nil, false
	} else {
		indexSnapMap := s.indexSnapMap.Get()
		if sc, ok := indexSnapMap[instId]; ok {
			return sc, false
		}
		var snap IndexSnapshot
		bucket := inst.Defn.Bucket
		if is == nil {
			bucketNameNumVBucketsMap := s.bucketNameNumVBucketsMapHolder.Get()
			numVBuckets := bucketNameNumVBucketsMap[bucket]
			ts := common.NewTsVbuuid(bucket, numVBuckets)
			snap = &indexSnapshot{
				instId: instId,
				ts:     ts, // nil snapshot should have ZERO Crc64 :)
				epoch:  true,
			}
		} else {
			snap = is
		}
		indexSnapMap = s.indexSnapMap.Clone()
		sc := &IndexSnapshotContainer{snap: snap}
		indexSnapMap[instId] = sc
		s.indexSnapMap.Set(indexSnapMap)
		return sc, true
	}
}

func (s *storageMgr) initSnapshotWaitersForInst(instId common.IndexInstId) *SnapshotWaitersContainer {
	s.muSnap.Lock()
	defer s.muSnap.Unlock()

	indexInstMap := s.indexInstMap.Get()
	if inst, ok := indexInstMap[instId]; !ok || inst.State == common.INDEX_STATE_DELETED {
		return nil
	}
	waitersMap := s.waitersMap.Get()
	var waiterContainer *SnapshotWaitersContainer
	var ok bool

	if waiterContainer, ok = waitersMap[instId]; !ok {
		waitersMap = s.waitersMap.Clone()
		waiterContainer = &SnapshotWaitersContainer{}
		waitersMap[instId] = waiterContainer
		s.waitersMap.Set(waitersMap)
	}
	return waiterContainer
}

func (s *storageMgr) addNilSnapshot(idxInstId common.IndexInstId, bucket string,
	snapC *IndexSnapshotContainer) {
	indexSnapMap := s.indexSnapMap.Get()
	if _, ok := indexSnapMap[idxInstId]; !ok {
		indexSnapMap := s.indexSnapMap.Clone()
		bucketNameNumVBucketsMap := s.bucketNameNumVBucketsMapHolder.Get()
		numVBuckets := bucketNameNumVBucketsMap[bucket]
		ts := common.NewTsVbuuid(bucket, numVBuckets)
		snap := &indexSnapshot{
			instId: idxInstId,
			ts:     ts, // nil snapshot should have ZERO Crc64 :)
			epoch:  true,
		}

		if snapC == nil {
			logging.Infof("StorageMgr::updateIndexSnapMapForIndex, New IndexSnapshotContainer is being created "+
				"for indexInst: %v", idxInstId)
			snapC = &IndexSnapshotContainer{snap: snap}
		} else {
			snapC.Lock()
			snapC.snap = snap
			snapC.Unlock()
		}
		indexSnapMap[idxInstId] = snapC
		s.indexSnapMap.Set(indexSnapMap)
		s.notifySnapshotCreation(snap)
	}
}

func (s *storageMgr) notifySnapshotDeletion(instId common.IndexInstId) {
	defer func() {
		if r := recover(); r != nil {
			logging.Errorf("storageMgr::notifySnapshot %v", r)
		}
	}()

	snap := &indexSnapshot{
		instId: instId,
		ts:     nil, // signal deletion with nil timestamp
	}
	index := uint64(instId) % uint64(len(s.snapshotNotifych))
	s.snapshotNotifych[int(index)] <- snap
}

func (s *storageMgr) notifySnapshotCreation(is IndexSnapshot) {
	defer func() {
		if r := recover(); r != nil {
			logging.Errorf("storageMgr::notifySnapshot %v", r)
		}
	}()

	index := uint64(is.IndexInstId()) % uint64(len(s.snapshotNotifych))
	s.snapshotNotifych[index] <- CloneIndexSnapshot(is)
}

func (s *storageMgr) handleUpdateNumVBuckets(cmd Message) {
	logging.Tracef("StorageMgr::handleUpdateNumVBuckets %v", cmd)

	req := cmd.(*MsgUpdateNumVbuckets)
	bucketNameNumVBucketsMap := req.GetBucketNameNumVBucketsMap()

	s.bucketNameNumVBucketsMapHolder.Set(bucketNameNumVBucketsMap)

	s.supvCmdch <- &MsgSuccess{}
}

func (s *storageMgr) handleUpdateIndexInstMap(cmd Message) {

	logging.Tracef("StorageMgr::handleUpdateIndexInstMap %v", cmd)
	req := cmd.(*MsgUpdateInstMap)
	indexInstMap := req.GetIndexInstMap()
	copyIndexInstMap := common.CopyIndexInstMap(indexInstMap)
	s.stats.Set(req.GetStatsObject())
	oldIndexInstMap := s.indexInstMap.Get()
	s.indexInstMap.Set(copyIndexInstMap)

	s.muSnap.Lock()
	defer s.muSnap.Unlock()

	indexInstMap = s.indexInstMap.Get()
	waitersMap := s.waitersMap.Clone()
	indexSnapMap := s.indexSnapMap.Clone()

	streamKeyspaceIdInstList := getStreamKeyspaceIdInstListFromInstMap(indexInstMap)
	s.streamKeyspaceIdInstList.Set(streamKeyspaceIdInstList)

	streamKeyspaceIdInstsPerWorker := getStreamKeyspaceIdInstsPerWorker(streamKeyspaceIdInstList, s.getNumSnapshotWorkers())
	s.streamKeyspaceIdInstsPerWorker.Set(streamKeyspaceIdInstsPerWorker)

	// Initialize waitersContainer for newly created instances
	for instId, inst := range indexInstMap {
		if _, ok := waitersMap[instId]; !ok && inst.State != common.INDEX_STATE_DELETED {
			waitersMap[instId] = &SnapshotWaitersContainer{}
		}
	}

	// Remove all snapshot waiters for indexes that do not exist anymore
	for id, wc := range waitersMap {
		if inst, ok := indexInstMap[id]; !ok || inst.State == common.INDEX_STATE_DELETED {
			wc.Lock()
			for _, w := range wc.waiters {
				w.Error(common.ErrIndexNotFound)
			}
			wc.waiters = nil
			delete(waitersMap, id)
			wc.Unlock()
		}
	}

	// Cleanup all invalid index's snapshots
	for idxInstId, snapC := range indexSnapMap {
		if inst, ok := indexInstMap[idxInstId]; !ok || inst.State == common.INDEX_STATE_DELETED {
			snapC.Lock()
			is := snapC.snap
			DestroyIndexSnapshot(is)
			delete(indexSnapMap, idxInstId)
			//set sc.deleted to true to indicate to concurrent readers
			//that this snap container should no longer be used
			snapC.deleted = true

			s.notifySnapshotDeletion(idxInstId)
			snapC.Unlock()
		}
	}

	s.indexSnapMap.Set(indexSnapMap)
	// Add 0 items index snapshots for newly added indexes
	for idxInstId, inst := range indexInstMap {
		if inst.State != common.INDEX_STATE_DELETED {
			s.addNilSnapshot(idxInstId, inst.Defn.Bucket, nil)
		}
	}

	// notify inst state change to ACTIVE
	s.notifyBuildDone(oldIndexInstMap)

	s.notifyIndexCreate(oldIndexInstMap)

	//if manager is not enable, store the updated InstMap in
	//meta file
	if s.config["enableManager"].Bool() == false {

		instMap := indexInstMap

		for id, inst := range instMap {
			inst.Pc = nil
			instMap[id] = inst
		}

		//store indexInstMap in metadata store
		var instBytes bytes.Buffer
		var err error

		enc := gob.NewEncoder(&instBytes)
		err = enc.Encode(instMap)
		if err != nil {
			logging.Errorf("StorageMgr::handleUpdateIndexInstMap \n\t Error Marshalling "+
				"IndexInstMap %v. Err %v", instMap, err)
		}

		if err = s.meta.SetKV([]byte(INST_MAP_KEY_NAME), instBytes.Bytes()); err != nil {
			logging.Errorf("StorageMgr::handleUpdateIndexInstMap \n\tError "+
				"Storing IndexInstMap %v", err)
		}

		s.dbfile.Commit(forestdb.COMMIT_MANUAL_WAL_FLUSH)
	}

	s.supvCmdch <- &MsgSuccess{}
}

func (s *storageMgr) notifyIndexCreate(oldIndexInstMap common.IndexInstMap) {

	findNewInsts := func() ([]common.IndexInst, int) {
		var numBhives int
		var result []common.IndexInst

		for id, newInst := range s.indexInstMap.Get() {
			oldInst, ok := oldIndexInstMap[id]
			if (!ok && newInst.State != common.INDEX_STATE_DELETED) ||
				(ok && oldInst.State == common.INDEX_STATE_INITIAL) {
				result = append(result, newInst)
			}

			if newInst.Defn.VectorMeta != nil && newInst.Defn.VectorMeta.IsBhive {
				numBhives++
			}
		}

		return result, numBhives
	}

	newInsts, numBhives := findNewInsts()
	if numBhives > 0 && len(newInsts) > 0 {
		// track create time and trigger quota redistribution
		// only if there are bhive indexes

		for _, inst := range newInsts {
			if inst.Defn.VectorMeta != nil && inst.Defn.VectorMeta.IsBhive {
				s.bhiveLastCreateTime = time.Now()
			} else {
				s.plasmaLastCreateTime = time.Now()
			}
		}

		s.quotaDistCh <- false
		<-s.quotaDistCh
	}
}

func (s *storageMgr) notifyBuildDone(oldIndexInstMap common.IndexInstMap) {

	newActiveInst := func() common.IndexInstMap {
		result := make(common.IndexInstMap)

		for id, newInst := range s.indexInstMap.Get() {
			oldInst, ok := oldIndexInstMap[id]
			if newInst.State == common.INDEX_STATE_ACTIVE &&
				(!ok ||
					oldInst.State == common.INDEX_STATE_INITIAL ||
					oldInst.State == common.INDEX_STATE_CATCHUP) {
				result[id] = newInst
			}
		}

		return result
	}

	indexPartnMap := s.indexPartnMap.Get()
	for idxInstId, _ := range newActiveInst() {
		partnMap := indexPartnMap[idxInstId]

		for _, partnInst := range partnMap {
			slices := partnInst.Sc.GetAllSlices()
			for _, slice := range slices {
				slice.BuildDone(idxInstId, s.buildDoneCallback)
			}
		}
	}
}

type BuildDoneCallback func(Message)

func (s *storageMgr) buildDoneCallback(msg Message) {
	//forward the message to the supervisor
	s.supvRespch <- msg
}

func (s *storageMgr) handleUpdateIndexPartnMap(cmd Message) {

	logging.Tracef("StorageMgr::handleUpdateIndexPartnMap %v", cmd)
	indexPartnMap := cmd.(*MsgUpdatePartnMap).GetIndexPartnMap()
	copyIndexPartnMap := CopyIndexPartnMap(indexPartnMap)
	s.indexPartnMap.Set(copyIndexPartnMap)

	s.supvCmdch <- &MsgSuccess{}
}

// handleUpdateKeyspaceStatsMap atomically swaps in the pointer to a new KeyspaceStatsMap.
func (s *storageMgr) handleUpdateKeyspaceStatsMap(cmd Message) {
	logging.Tracef("StorageMgr::handleUpdateKeyspaceStatsMap %v", cmd)
	req := cmd.(*MsgUpdateKeyspaceStatsMap)
	stats := s.stats.Get()
	if stats != nil {
		stats.keyspaceStatsMap.Set(req.GetStatsObject())
	}

	s.supvCmdch <- &MsgSuccess{}
}

// Process req for providing an index snapshot for index scan.
// The request contains atleast-timestamp and the storage
// manager will reply with a index snapshot soon after a
// snapshot meeting requested criteria is available.
// The requester will block wait until the response is
// available.
func (s *storageMgr) handleGetIndexSnapshot(cmd Message) {
	s.supvCmdch <- &MsgSuccess{}
	instId := cmd.(*MsgIndexSnapRequest).GetIndexId()
	index := uint64(instId) % uint64(len(s.snapshotReqCh))
	s.snapshotReqCh[int(index)] <- cmd
}

func (s *storageMgr) listenSnapshotReqs(index int) {
	for cmd := range s.snapshotReqCh[index] {
		func() {
			req := cmd.(*MsgIndexSnapRequest)
			inst, found := s.indexInstMap.Get()[req.GetIndexId()]
			if !found || inst.State == common.INDEX_STATE_DELETED {
				req.respch <- common.ErrIndexNotFound
				return
			}

			stats := s.stats.Get()
			idxStats := stats.indexes[req.GetIndexId()]

			// Return snapshot immediately if a matching snapshot exists already
			// Else add into waiters list so that next snapshot creation event
			// can notify the requester when a snapshot with matching timestamp
			// is available.
			snapC := s.indexSnapMap.Get()[req.GetIndexId()]
			if snapC == nil {
				func() {
					s.muSnap.Lock()
					defer s.muSnap.Unlock()
					snapC, _ = s.initSnapshotContainerForInst(req.GetIndexId(), nil)
				}()
				if snapC == nil {
					req.respch <- common.ErrIndexNotFound
					return
				}
			}

			snapC.Lock()
			//snapC.deleted indicates that the snapshot container belongs to a deleted
			//index and it should no longer be used.
			if snapC.deleted {
				req.respch <- common.ErrIndexNotFound
				snapC.Unlock()
				return
			}
			if isSnapshotConsistent(snapC.snap, req.GetConsistency(), req.GetTS()) {
				req.respch <- CloneIndexSnapshot(snapC.snap)
				snapC.Unlock()
				return
			}
			snapC.Unlock()

			waitersMap := s.waitersMap.Get()

			var waitersContainer *SnapshotWaitersContainer
			var ok bool
			if waitersContainer, ok = waitersMap[req.GetIndexId()]; !ok {
				waitersContainer = s.initSnapshotWaitersForInst(req.GetIndexId())
			}

			if waitersContainer == nil {
				req.respch <- common.ErrIndexNotFound
				return
			}

			w := newSnapshotWaiter(
				req.GetIndexId(), req.GetTS(), req.GetConsistency(),
				req.GetReplyChannel(), req.GetExpiredTime())

			if idxStats != nil {
				idxStats.numSnapshotWaiters.Add(1)
			}

			waitersContainer.Lock()
			defer waitersContainer.Unlock()
			waitersContainer.waiters = append(waitersContainer.waiters, w)
		}()
	}
}

func (s *storageMgr) handleGetIndexStorageStats(cmd Message) {
	s.supvCmdch <- &MsgSuccess{}
	go func() { // Process storage stats asyncronously
		s.statsLock.Lock()
		defer s.statsLock.Unlock()

		req := cmd.(*MsgIndexStorageStats)
		replych := req.GetReplyChannel()
		spec := req.GetStatsSpec()
		stats := s.getIndexStorageStats(spec)
		replych <- stats
	}()
}

func (s *storageMgr) handleStats(cmd Message) {
	s.supvCmdch <- &MsgSuccess{}

	go func() {
		s.statsLock.Lock()
		defer s.statsLock.Unlock()

		req := cmd.(*MsgStatsRequest)
		replych := req.GetReplyChannel()
		storageStats := s.getIndexStorageStats(nil)

		//node level stats
		var numStorageInstances int64
		var totalDataSize, totalDiskSize, totalRecsInMem, totalRecsOnDisk,
			totalrawDataSize, totalCodebookMemUsage int64
		var avgMutationRate, avgDrainRate, avgDiskBps, unitsUsage int64

		stats := s.stats.Get()
		indexInstMap := s.indexInstMap.Get()
		for _, st := range storageStats {
			inst := indexInstMap[st.InstId]
			if inst.State == common.INDEX_STATE_DELETED {
				continue
			}

			numStorageInstances++

			idxStats := stats.GetPartitionStats(st.InstId, st.PartnId)
			// TODO(sarath): Investigate the reason for inconsistent stats map
			// This nil check is a workaround to avoid indexer crashes for now.
			if idxStats != nil {
				idxStats.dataSize.Set(st.Stats.DataSize)
				idxStats.dataSizeOnDisk.Set(st.Stats.DataSizeOnDisk)
				idxStats.logSpaceOnDisk.Set(st.Stats.LogSpace)
				idxStats.diskSize.Set(st.Stats.DiskSize)
				idxStats.memUsed.Set(st.Stats.MemUsed)
				if common.GetStorageMode() != common.MOI {
					if common.GetStorageMode() == common.PLASMA {
						idxStats.fragPercent.Set(int64(st.getPlasmaFragmentation()))
					} else {
						idxStats.fragPercent.Set(int64(st.GetFragmentation()))
					}
				}

				idxStats.getBytes.Set(st.Stats.GetBytes)
				idxStats.insertBytes.Set(st.Stats.InsertBytes)
				idxStats.deleteBytes.Set(st.Stats.DeleteBytes)

				idxStats.graphBuildProgress.Set(int64(st.Stats.GraphBuildProgress))

				// compute mutation rate
				now := time.Now().UnixNano()
				elapsed := float64(now-idxStats.lastMutateGatherTime.Value()) / float64(time.Second)
				if elapsed > 60 {
					numDocsIndexed := idxStats.numDocsIndexed.Value()
					mutationRate := float64(numDocsIndexed-idxStats.lastNumDocsIndexed.Value()) / elapsed
					idxStats.avgMutationRate.Set(int64((mutationRate + float64(idxStats.avgMutationRate.Value())) / 2))
					idxStats.lastNumDocsIndexed.Set(numDocsIndexed)

					numItemsFlushed := idxStats.numItemsFlushed.Value()
					drainRate := float64(numItemsFlushed-idxStats.lastNumItemsFlushed.Value()) / elapsed
					idxStats.avgDrainRate.Set(int64((drainRate + float64(idxStats.avgDrainRate.Value())) / 2))
					idxStats.lastNumItemsFlushed.Set(numItemsFlushed)

					diskBytes := idxStats.getBytes.Value() + idxStats.insertBytes.Value() + idxStats.deleteBytes.Value()
					diskBps := float64(diskBytes-idxStats.lastDiskBytes.Value()) / elapsed
					idxStats.avgDiskBps.Set(int64((diskBps + float64(idxStats.avgDiskBps.Value())) / 2))
					idxStats.lastDiskBytes.Set(diskBytes)

					logging.Debugf("StorageManager.handleStats: partition %v DiskBps %v avgDiskBps %v drain rate %v",
						st.PartnId, diskBps, idxStats.avgDiskBps.Value(), idxStats.avgDrainRate.Value())

					idxStats.lastMutateGatherTime.Set(now)
				}

				//compute node level stats
				totalDataSize += st.Stats.DataSize
				totalDiskSize += st.Stats.DiskSize
				totalRecsInMem += idxStats.numRecsInMem.Value()
				totalRecsInMem += idxStats.bsNumRecsInMem.Value()
				totalRecsOnDisk += idxStats.numRecsOnDisk.Value()
				totalRecsOnDisk += idxStats.bsNumRecsOnDisk.Value()
				avgMutationRate += idxStats.avgMutationRate.Value()
				avgDrainRate += idxStats.avgDrainRate.Value()
				avgDiskBps += idxStats.avgDiskBps.Value()
				unitsUsage += idxStats.avgUnitsUsage.Value()
				totalrawDataSize += idxStats.rawDataSize.Value()
				if idxStats.isVectorIndex {
					totalCodebookMemUsage += idxStats.codebookSize.Value()
				}
			}
		}

		stats.totalDataSize.Set(totalDataSize)
		stats.totalDiskSize.Set(totalDiskSize)
		stats.numStorageInstances.Set(numStorageInstances)
		stats.avgMutationRate.Set(avgMutationRate)
		stats.avgDrainRate.Set(avgDrainRate)
		stats.avgDiskBps.Set(avgDiskBps)
		stats.unitsUsedActual.Set(unitsUsage)
		stats.totalRawDataSize.Set(totalrawDataSize)
		stats.totalCodebookMemUsage.Set(totalCodebookMemUsage)

		if numStorageInstances > 0 {

			stats.avgResidentPercent.Set(common.ComputePercent(totalRecsInMem, totalRecsOnDisk))
		} else {
			stats.avgResidentPercent.Set(0)
		}

		// get and set bucket level stats

		if common.IsServerlessDeployment() {
			s.setBucketStats()
		}

		replych <- true
	}()
}

func (s *storageMgr) handleShardStorageStats(cmd Message) {
	s.supvCmdch <- &MsgSuccess{}

	go func() { // Process shard storage stats asyncronously
		s.statsLock.Lock()
		defer s.statsLock.Unlock()

		req := cmd.(*MsgShardStatsRequest)
		replych := req.GetReplyChannel()
		stats := s.getShardStats()
		replych <- stats
	}()
}

func (s *storageMgr) setBucketStats() {
	// TODO: Fix this for non-serveless if required.
	if !common.IsServerlessDeployment() {
		return
	}

	stats := s.stats.Get()

	indexInstMap := s.indexInstMap.Get()
	indexPartnMap := s.indexPartnMap.Get()

	done := make(map[string]bool)

	for idxInstId, partnMap := range indexPartnMap {

		var inst common.IndexInst
		var ok bool
		if inst, ok = indexInstMap[idxInstId]; !ok {
			continue
		}

		if inst.State == common.INDEX_STATE_DELETED {
			continue
		}

		if _, ok := done[inst.Defn.Bucket]; ok {
			continue
		}

		bstats := stats.GetBucketStats(inst.Defn.Bucket)
		if bstats == nil {
			done[inst.Defn.Bucket] = true
			continue
		}

		for _, partnInst := range partnMap {
			if _, ok := done[inst.Defn.Bucket]; ok {
				break
			}

			slices := partnInst.Sc.GetAllSlices()
			for _, slice := range slices {
				if _, ok := done[inst.Defn.Bucket]; ok {
					break
				}

				sz, err := slice.GetTenantDiskSize()
				if err != nil {
					logging.Errorf("StorageManager getBucketStats error (%v) in "+
						"GetTenantDiskSize for bucket %v", err, inst.Defn.Bucket)
				}

				bstats.diskUsed.Set(sz)
				done[inst.Defn.Bucket] = true
				break
			}
		}
	}
}

func (s *storageMgr) getIndexStorageStats(spec *statsSpec) []IndexStorageStats {
	var stats []IndexStorageStats
	var err error
	var sts StorageStatistics

	doPrepare := true

	instIDMap := make(map[common.IndexInstId]bool)
	if spec != nil && spec.indexSpec != nil && len(spec.indexSpec.GetInstances()) > 0 {
		insts := spec.indexSpec.GetInstances()
		for _, instId := range insts {
			instIDMap[instId] = true
		}
	}

	var consumerFilter uint64
	if spec != nil {
		consumerFilter = spec.consumerFilter
	}

	var numIndexes int64
	gStats := s.stats.Get()

	indexInstMap := s.indexInstMap.Get()
	indexPartnMap := s.indexPartnMap.Get()
	for idxInstId, partnMap := range indexPartnMap {

		// If list of instances are specified in the request and the current
		// instance does not match the instance specified in request, do not
		// process storage statistics for that instance
		if len(instIDMap) > 0 {
			if _, ok := instIDMap[idxInstId]; !ok {
				continue
			}
		}

		inst, ok := indexInstMap[idxInstId]
		//skip deleted indexes
		if !ok || inst.State == common.INDEX_STATE_DELETED {
			continue
		}

		numIndexes++

		for _, partnInst := range partnMap {
			var internalData []string
			internalDataMap := make(map[string]interface{})
			var dataSz, dataSzOnDisk, logSpace, diskSz, memUsed, extraSnapDataSize int64
			var getBytes, insertBytes, deleteBytes int64
			var nslices int64
			var needUpgrade = false
			var hasStats = false
			var loggingDisabled = true
			var lastResetTime int64
			var graphBuildProgress float32

			slices := partnInst.Sc.GetAllSlices()
			nslices += int64(len(slices))
			for i, slice := range slices {

				// Increment the ref count before gathering stats. This is to ensure that
				// the instance is not deleted in the middle of gathering stats.
				if !slice.CheckAndIncrRef() {
					continue
				}

				// Prepare stats once
				if doPrepare {
					slice.PrepareStats()
					doPrepare = false
				}

				sts, err = slice.Statistics(consumerFilter)
				slice.DecrRef()

				if err != nil {
					break
				}
				lastResetTime = max(lastResetTime, sts.LastResetTime)

				dataSz += sts.DataSize
				dataSzOnDisk += sts.DataSizeOnDisk
				memUsed += sts.MemUsed
				logSpace += sts.LogSpace
				diskSz += sts.DiskSize
				getBytes += sts.GetBytes
				insertBytes += sts.InsertBytes
				deleteBytes += sts.DeleteBytes
				extraSnapDataSize += sts.ExtraSnapDataSize
				internalData = append(internalData, sts.InternalData...)
				if sts.InternalDataMap != nil && len(sts.InternalDataMap) != 0 {
					internalDataMap[fmt.Sprintf("slice_%d", i)] = sts.InternalDataMap
				}
				// Even if one slice has stats, loggingDisabled will be set to false
				loggingDisabled = loggingDisabled && sts.LoggingDisabled
				needUpgrade = needUpgrade || sts.NeedUpgrade

				graphBuildProgress = max(graphBuildProgress, sts.GraphBuildProgress)

				hasStats = true
			}

			if hasStats && err == nil {
				stat := IndexStorageStats{
					InstId:        idxInstId,
					PartnId:       partnInst.Defn.GetPartitionId(),
					Name:          inst.Defn.Name,
					Bucket:        inst.Defn.Bucket,
					Scope:         inst.Defn.Scope,
					Collection:    inst.Defn.Collection,
					LastResetTime: lastResetTime,
					Stats: StorageStatistics{
						DataSize:           dataSz,
						DataSizeOnDisk:     dataSzOnDisk,
						LogSpace:           logSpace,
						DiskSize:           diskSz,
						MemUsed:            memUsed,
						GetBytes:           getBytes,
						InsertBytes:        insertBytes,
						DeleteBytes:        deleteBytes,
						ExtraSnapDataSize:  extraSnapDataSize,
						NeedUpgrade:        needUpgrade,
						InternalData:       internalData,
						InternalDataMap:    internalDataMap,
						LoggingDisabled:    loggingDisabled,
						GraphBuildProgress: graphBuildProgress,
					},
				}

				stats = append(stats, stat)
			}
		}
	}
	gStats.numIndexes.Set(numIndexes)

	return stats
}

func (s *storageMgr) getShardStats() map[string]*common.ShardStats {

	doPrepare := true
	indexInstMap := s.indexInstMap.Get()
	indexPartnMap := s.indexPartnMap.Get()

	out := make(map[string]*common.ShardStats)

	populateShardStats := func(skipPrimaryIndex bool) {
		for idxInstId, partnMap := range indexPartnMap {
			inst, ok := indexInstMap[idxInstId]

			//skip deleted indexes
			if !ok || inst.State == common.INDEX_STATE_DELETED {
				continue
			}

			if inst.Defn.IsPrimary && skipPrimaryIndex {
				continue
			}

			// TODO: Index may skip deleted instances but shard may still
			// be maintaining the stats for the index. Need to filter the stats
			// of the deleted instances in the shard
			for _, partnInst := range partnMap {
				slices := partnInst.Sc.GetAllSlices()

				for _, slice := range slices {

					// Make sure that the slice is not deleted while stats are being gathered
					if !slice.CheckAndIncrRef() {
						continue
					}

					if doPrepare {
						slice.PrepareStats()
						doPrepare = false
					}

					partnId := partnInst.Defn.GetPartitionId()
					alternateShardId := slice.GetAlternateShardId(partnId)
					if len(alternateShardId) == 0 {
						slice.DecrRef()
						continue
					}

					if _, ok := out[alternateShardId]; ok {
						slice.DecrRef()
						continue
					}

					shardStats := slice.ShardStatistics(partnId)
					if shardStats == nil {
						logging.Fatalf("storageMgr::getShardStats Expected non-nil shard stats for alternateId: %v, slice: %v, partnId: %v",
							alternateShardId, slice.IndexInstId(), partnId)
						slice.DecrRef()
						continue
					}
					out[shardStats.AlternateShardId] = shardStats
					slice.DecrRef()

				}
			}
		}
	}

	// Shard stats are accumulated for both mainstore and backstore. As primary
	// indexes are not aware of backstore, skip processing primary indexes in
	// first iteration. As there can exist a shard with only primary indexes,
	// iterate the instance list again to accommodate such shards
	populateShardStats(true)
	populateShardStats(false)

	return out
}

func (s *storageMgr) handleRecoveryDone() {
	s.supvCmdch <- &MsgSuccess{}

	if common.GetStorageMode() == common.PLASMA {
		RecoveryDone_Plasma()
		RecoveryDone_Bhive()
	}
}

func (s *storageMgr) handleConfigUpdate(cmd Message) {
	oldConfig := s.config

	cfgUpdate := cmd.(*MsgConfigUpdate)
	newConfig := cfgUpdate.GetConfig()
	s.config = newConfig

	if (newConfig["settings.memory_quota"].Uint64() !=
		oldConfig["settings.memory_quota"].Uint64()) ||
		(newConfig["settings.percentage_memory_quota"].Uint64() !=
			oldConfig["settings.percentage_memory_quota"].Uint64()) {

		// memory quota setting has changed, need to redistribute
		s.quotaDistCh <- false
		<-s.quotaDistCh
	}

	isShardAffinityEnabled := common.CanMaintanShardAffinity(s.config)
	if isShardAffinityEnabled && s.stm == nil {
		s.stm = NewShardTransferManager(s.config, s.wrkrCh)
	}

	if s.stm != nil { // Pass on the config change to shard tranfser manager
		s.stm.ProcessCommand(cmd)
	}

	s.supvCmdch <- &MsgSuccess{}
}

func (s *storageMgr) handleIndexMergeSnapshot(cmd Message) {
	req := cmd.(*MsgIndexMergeSnapshot)
	srcInstId := req.GetSourceInstId()
	tgtInstId := req.GetTargetInstId()
	partitions := req.GetPartitions()

	var source, target IndexSnapshot
	indexSnapMap := s.indexSnapMap.Get()

	validateSnapshots := func() bool {
		sourceC, ok := indexSnapMap[srcInstId]
		if !ok {
			s.supvCmdch <- &MsgSuccess{}
			return false
		}
		sourceC.Lock()
		defer sourceC.Unlock()

		source = sourceC.snap

		targetC, ok := indexSnapMap[tgtInstId]
		if !ok {
			// increment source snapshot refcount
			target = s.deepCloneIndexSnapshot(source, false, nil)

		} else {
			targetC.Lock()
			defer targetC.Unlock()

			target = targetC.snap
			// Make sure that the source timestamp is greater than or equal to the target timestamp.
			// This comparison will only cover the seqno and vbuuids.
			//
			// Note that even if the index instance has 0 mutation or no new mutation, storage
			// manager will always create a new indexSnapshot with the current timestamp during
			// snapshot.
			//
			// But there is a chance that merge happens before snapshot.  In this case, source
			// could have a higher snapshot than target:
			// 1) source is merged to MAINT stream from INIT stream
			// 2) after (1), there is no flush/snapshot before merge partition happens
			//
			// Here, we just have to make sure that the source has a timestamp at least as high
			// as the target to detect potential data loss.   The merged snapshot will use the
			// target timestamp.    Since target timestamp cannot be higher than source snapshot,
			// there is no risk of data loss.
			//

			/*if !source.Timestamp().EqualOrGreater(target.Timestamp(), false) {
				s.supvCmdch <- &MsgError{
					err: Error{code: ERROR_STORAGE_MGR_MERGE_SNAPSHOT_FAIL,
						severity: FATAL,
						category: STORAGE_MGR,
						cause: fmt.Errorf("Timestamp mismatch between snapshot\n target %v\n source %v\n",
							target.Timestamp(), source.Timestamp())}}
				return false
			}*/

			// source will not have partition snapshot if there is no mutation in bucket.  Skip validation check.
			// If bucket has at least 1 mutation, then source will have partition snapshot.
			if len(source.Partitions()) != 0 {
				// make sure that the source snapshot has all the required partitions
				count := 0
				for _, partnId := range partitions {
					for _, sp := range source.Partitions() {
						if partnId == sp.PartitionId() {
							count++
							break
						}
					}
				}
				if count != len(partitions) || count != len(source.Partitions()) {
					s.supvCmdch <- &MsgError{
						err: Error{code: ERROR_STORAGE_MGR_MERGE_SNAPSHOT_FAIL,
							severity: FATAL,
							category: STORAGE_MGR,
							cause: fmt.Errorf("Source snapshot %v does not have all the required partitions %v",
								srcInstId, partitions)}}
					return false
				}

				// make sure there is no overlapping partition between source and target snapshot
				for _, sp := range source.Partitions() {

					found := false
					for _, tp := range target.Partitions() {
						if tp.PartitionId() == sp.PartitionId() {
							found = true
							break
						}
					}

					if found {
						s.supvCmdch <- &MsgError{
							err: Error{code: ERROR_STORAGE_MGR_MERGE_SNAPSHOT_FAIL,
								severity: FATAL,
								category: STORAGE_MGR,
								cause: fmt.Errorf("Duplicate partition %v found between source %v and target %v",
									sp.PartitionId(), srcInstId, tgtInstId)}}
						return false
					}
				}
			} else {
				logging.Infof("skip validation in merge partitions %v between inst %v and %v", partitions, srcInstId, tgtInstId)
			}

			// Deep clone a new snapshot by copying internal maps + increment target snapshot refcount.
			// The target snapshot could be being used (e.g. under scan).  Increment the snapshot refcount
			// ensure that the snapshot will not get reclaimed.
			target = s.deepCloneIndexSnapshot(target, false, nil)
			if len(partitions) != 0 {
				// Increment source snaphsot refcount (only for copied partitions).  Those snapshots will
				// be copied over to the target snapshot.  Note that the source snapshot can have different
				// refcount than the target snapshot, since the source snapshot may not be used for scanning.
				// But it should be safe to copy from source to target, even if ref count is different.
				source = s.deepCloneIndexSnapshot(source, true, partitions)

				// move the partition in source snapshot to target snapshot
				for _, snap := range source.Partitions() {
					target.Partitions()[snap.PartitionId()] = snap
				}
			}
		}
		return true
	}()

	if !validateSnapshots {
		return
	}

	// decrement source snapshot refcount
	// Do not decrement source snapshot refcount.   When the proxy instance is deleted, storage manager will be notified
	// of the new instance state.   Storage manager will then decrement the ref count at that time.
	//DestroyIndexSnapshot(s.indexSnapMap[srcInstId])

	stats := s.stats.Get()
	idxStats := stats.indexes[tgtInstId]

	// update the target with new snapshot.  This will also decrement target old snapshot refcount.
	s.updateSnapMapAndNotify(target, idxStats)

	s.supvCmdch <- &MsgSuccess{}
}

func (s *storageMgr) handleIndexPruneSnapshot(cmd Message) {
	req := cmd.(*MsgIndexPruneSnapshot)
	instId := req.GetInstId()
	partitions := req.GetPartitions()

	snapC, ok := s.indexSnapMap.Get()[instId]
	if !ok {
		s.supvCmdch <- &MsgSuccess{}
		return
	}
	snapC.Lock()
	snapshot := snapC.snap

	// find the partitions that we want to keep
	kept := make([]common.PartitionId, 0, len(snapshot.Partitions()))
	for _, sp := range snapshot.Partitions() {

		found := false
		for _, partnId := range partitions {
			if partnId == sp.PartitionId() {
				found = true
				break
			}
		}

		if !found {
			kept = append(kept, sp.PartitionId())
		}
	}

	// Increment the snapshot refcount for the partition/slice that we want to keep.
	newSnapshot := s.deepCloneIndexSnapshot(snapshot, true, kept)

	stats := s.stats.Get()
	idxStats := stats.indexes[instId]
	snapC.Unlock()

	s.updateSnapMapAndNotify(newSnapshot, idxStats)

	s.supvCmdch <- &MsgSuccess{}
}

// deepCloneIndexSnapshot makes a clone of a partitioned-index snapshot, but optionally clones only
// a subset of the partition snapshots. It also increments the reference count (i.e. opens) all the
// slices of all the snapshot partitions that do get cloned.
//
// is -- the index shapshot to clone
// doPrune -- false clones ALL partitions and IGNORES the keepPartnIds[] arg. true clones only the
//
//	subset of partitions listed in the keepPartnIds[] arg.
//
// keepPartnIds[] -- used ONLY if doPrune == true, this gives the set of partitions whose snapshots
//
//	are to be cloned, which MAY BE EMPTY OR NIL to indicate pruning away of ALL partitions is
//	desired, in which case none of the partition snapshots are cloned. (This case can occur when a
//	prune is done of all partitions currently in the real instance while there is also an
//	outstanding proxy to be merged into the real instance. Even though all existing partns are
//	moving out, other partns are moving in, so we do a prune of all partitions in the real instance
//	instead of a drop of the index.)
func (s *storageMgr) deepCloneIndexSnapshot(is IndexSnapshot, doPrune bool, keepPartnIds []common.PartitionId) IndexSnapshot {

	snap := is.(*indexSnapshot)

	clone := &indexSnapshot{
		instId: snap.instId,
		ts:     snap.ts.Copy(),
		partns: make(map[common.PartitionId]PartitionSnapshot),
	}

	// For each partition snapshot...
	for partnId, partnSnap := range snap.Partitions() {

		// Determine if we need to clone this partition snapshot
		doClone := false
		if !doPrune {
			doClone = true
		} else {
			for _, keepPartnId := range keepPartnIds {
				if partnId == keepPartnId {
					doClone = true
					break
				}
			}
		}

		if doClone {
			ps := &partitionSnapshot{
				id:     partnId,
				slices: make(map[SliceId]SliceSnapshot),
			}

			for sliceId, sliceSnap := range partnSnap.Slices() {

				// increment ref count of each slice snapshot
				sliceSnap.Snapshot().Open()
				ps.slices[sliceId] = &sliceSnapshot{
					id:   sliceSnap.SliceId(),
					snap: sliceSnap.Snapshot(),
				}
			}

			clone.partns[partnId] = ps
		}
	}

	return clone
}

func (s *storageMgr) handleIndexCompaction(cmd Message) {
	s.supvCmdch <- &MsgSuccess{}
	req := cmd.(*MsgIndexCompact)
	errch := req.GetErrorChannel()
	abortTime := req.GetAbortTime()
	minFrag := req.GetMinFrag()
	var slices []Slice

	inst, ok := s.indexInstMap.Get()[req.GetInstId()]
	stats := s.stats.Get()
	if !ok || inst.State == common.INDEX_STATE_DELETED {
		errch <- common.ErrIndexNotFound
		return
	}

	partnMap, _ := s.indexPartnMap.Get()[req.GetInstId()]
	idxStats := stats.indexes[req.GetInstId()]
	idxStats.numCompactions.Add(1)

	// Increment rc for slices
	for _, partnInst := range partnMap {
		// non-partitioned index has partitionId 0
		if partnInst.Defn.GetPartitionId() == req.GetPartitionId() {
			for _, slice := range partnInst.Sc.GetAllSlices() {
				if !slice.CheckAndIncrRef() {
					continue
				}
				slices = append(slices, slice)
			}
		}
	}

	// Perform file compaction without blocking storage manager main loop
	go func() {
		for _, slice := range slices {
			err := slice.Compact(abortTime, minFrag)
			slice.DecrRef()
			if err != nil {
				errch <- err
				return
			}
		}

		errch <- nil
	}()
}

// Used for forestdb and memdb slices.
func (s *storageMgr) openSnapshot(idxInstId common.IndexInstId, partnInst PartitionInst,
	partnSnapMap PartnSnapMap) (PartnSnapMap, *common.TsVbuuid, error) {

	pid := partnInst.Defn.GetPartitionId()
	sc := partnInst.Sc

	//there is only one slice for now
	slice := sc.GetSliceById(0)
	infos, err := slice.GetSnapshots()
	// TODO: Proper error handling if possible
	if err != nil {
		panic("Unable to read snapinfo -" + err.Error())
	}

	snapInfoContainer := NewSnapshotInfoContainer(infos)
	allSnapShots := snapInfoContainer.List()

	snapFound := false
	usableSnapFound := false
	var tsVbuuid *common.TsVbuuid
	for _, snapInfo := range allSnapShots {
		snapFound = true
		logging.Infof("StorageMgr::openSnapshot IndexInst:%v Partition:%v Attempting to open snapshot (%v)",
			idxInstId, pid, snapInfo)
		usableSnapshot, err := slice.OpenSnapshot(snapInfo, nil)
		if err != nil {
			if err == errStorageCorrupted {
				// Slice has already cleaned up the snapshot files. Try with older snapshot.
				// Note: plasma and forestdb never return errStorageCorrupted for OpenSnapshot.
				// So, we continue only in case of MOI.
				continue
			} else {
				panic("Unable to open snapshot -" + err.Error())
			}
		}
		ss := &sliceSnapshot{
			id:   SliceId(0),
			snap: usableSnapshot,
		}

		tsVbuuid = snapInfo.Timestamp()

		sid := SliceId(0)

		ps := &partitionSnapshot{
			id:     pid,
			slices: map[SliceId]SliceSnapshot{sid: ss},
		}

		partnSnapMap[pid] = ps
		usableSnapFound = true
		break
	}

	if !snapFound {
		logging.Infof("StorageMgr::openSnapshot IndexInst:%v Partition:%v No Snapshot Found.",
			idxInstId, pid)
		partnSnapMap = nil
		return partnSnapMap, tsVbuuid, nil
	}

	if !usableSnapFound {
		logging.Infof("StorageMgr::openSnapshot IndexInst:%v Partition:%v No Usable Snapshot Found.",
			idxInstId, pid)
		return partnSnapMap, nil, errStorageCorrupted
	}

	return partnSnapMap, tsVbuuid, nil
}

// Update index-snapshot map using index partition map
// This function should be called only during initialization
// of storage manager and during rollback.
// FIXME: Current implementation makes major assumption that
// single slice is supported.
func (s *storageMgr) updateIndexSnapMap(indexPartnMap IndexPartnMap,
	streamId common.StreamId, keyspaceId string) {

	s.muSnap.Lock()
	defer s.muSnap.Unlock()

	for idxInstId, partnMap := range indexPartnMap {
		idxInst := s.indexInstMap.Get()[idxInstId]
		s.updateIndexSnapMapForIndex(idxInstId, idxInst, partnMap, streamId, keyspaceId)
	}
}

// Caller of updateIndexSnapMapForIndex should ensure
// locking and subsequent unlocking of muSnap
func (s *storageMgr) updateIndexSnapMapForIndex(idxInstId common.IndexInstId, idxInst common.IndexInst,
	partnMap PartitionInstMap, streamId common.StreamId, keyspaceId string) {

	needRestart := false
	//if keyspace and stream have been provided
	if keyspaceId != "" && streamId != common.ALL_STREAMS {
		//skip the index if either keyspaceId or stream don't match
		if idxInst.Defn.KeyspaceId(idxInst.Stream) != keyspaceId || idxInst.Stream != streamId {
			return
		}
		//skip deleted indexes
		if idxInst.State == common.INDEX_STATE_DELETED {
			return
		}
	}

	// safety check
	_, indexExists := s.indexInstMap.Get()[idxInstId]
	if !indexExists {
		_, indexExists = s.indexPartnMap.Get()[idxInstId]
	}

	if !indexExists {
		logging.Warnf("StorageMgr::updateIndexSnapMapForIndex skipping snapshot creation for index inst %v as it does not exist or is deleted", idxInstId)
		return
	}

	partitionIDs, _ := idxInst.Pc.GetAllPartitionIds()
	logging.Infof("StorageMgr::updateIndexSnapMapForIndex IndexInst %v Partitions %v",
		idxInstId, partitionIDs)

	indexSnapMap := s.indexSnapMap.Clone()
	snapC := indexSnapMap[idxInstId]
	if snapC != nil {
		snapC.Lock()
		if !snapC.deleted { // Destroy only if snapshot is not already deleted
			DestroyIndexSnapshot(snapC.snap)
		}
		delete(indexSnapMap, idxInstId)
		s.indexSnapMap.Set(indexSnapMap)
		snapC.Unlock()
		s.notifySnapshotDeletion(idxInstId)
	}

	var tsVbuuid *common.TsVbuuid
	var err error
	partnSnapMap := make(PartnSnapMap)

	for _, partnInst := range partnMap {
		partnSnapMap, tsVbuuid, err = s.openSnapshot(idxInstId, partnInst, partnSnapMap)
		if err != nil {
			if err == errStorageCorrupted {
				needRestart = true
			} else {
				panic("Unable to open snapshot -" + err.Error())
			}
		}

		if partnSnapMap == nil {
			break
		}

		//if OSO snapshot, rollback all partitions to 0
		if tsVbuuid != nil && tsVbuuid.GetSnapType() == common.DISK_SNAP_OSO {
			for _, partnInst := range partnMap {
				partnId := partnInst.Defn.GetPartitionId()
				sc := partnInst.Sc

				for _, slice := range sc.GetAllSlices() {
					_, err := s.rollbackToSnapshot(idxInstId, partnId,
						slice, nil, false, idxInst.State)
					if err != nil {
						panic("Unable to rollback to 0 - " + err.Error())
					}
				}
			}
			partnSnapMap = nil
			break
		}
	}

	bucket, _, _ := SplitKeyspaceId(keyspaceId)
	if len(partnSnapMap) != 0 {
		is := &indexSnapshot{
			instId: idxInstId,
			ts:     tsVbuuid,
			partns: partnSnapMap,
		}
		indexSnapMap = s.indexSnapMap.Clone()
		if snapC == nil {
			snapC = &IndexSnapshotContainer{snap: is}
		} else {
			snapC.Lock()
			snapC.snap = is
			snapC.Unlock()
		}

		indexSnapMap[idxInstId] = snapC
		s.indexSnapMap.Set(indexSnapMap)
		s.notifySnapshotCreation(is)
	} else {
		logging.Infof("StorageMgr::updateIndexSnapMapForIndex IndexInst %v Adding Nil Snapshot.",
			idxInstId)
		s.addNilSnapshot(idxInstId, bucket, snapC)
	}

	if needRestart {
		os.Exit(1)
	}
}

func (s *storageMgr) handleUpdateIndexSnapMapForIndex(cmd Message) {

	req := cmd.(*MsgUpdateSnapMap)
	idxInstId := req.GetInstId()
	idxInst := req.GetInst()
	partnMap := req.GetPartnMap()
	streamId := req.GetStreamId()
	keyspaceId := req.GetKeyspaceId()
	replyCh := req.GetReplyChannel()

	f := func() {
		s.muSnap.Lock()
		s.updateIndexSnapMapForIndex(idxInstId, idxInst, partnMap, streamId, keyspaceId)
		s.muSnap.Unlock()
		if replyCh != nil {
			replyCh <- true
		}
	}

	if replyCh != nil && (common.GetStorageMode() == common.MOI) {
		// make updateIndexSnapMapForIndex async for MOI during bootstrap phase
		// because it tries to load index from disk which can take lot of time (sometimes in 10s of mins)
		// this will cause indexer to block and introduce failures such as rebalance failure.
		// replyChan is only set during bootstrap phase.
		go f()
	} else {
		f()
	}
	s.supvCmdch <- &MsgSuccess{}
}

func getStreamKeyspaceIdInstListFromInstMap(indexInstMap common.IndexInstMap) StreamKeyspaceIdInstList {
	out := make(StreamKeyspaceIdInstList)
	for instId, inst := range indexInstMap {
		stream := inst.Stream
		keyspaceId := inst.Defn.KeyspaceId(inst.Stream)
		if _, ok := out[stream]; !ok {
			out[stream] = make(KeyspaceIdInstList)
		}
		out[stream][keyspaceId] = append(out[stream][keyspaceId], instId)
	}
	return out
}

func getStreamKeyspaceIdInstsPerWorker(streamKeyspaceIdInstList StreamKeyspaceIdInstList, numSnapshotWorkers int) StreamKeyspaceIdInstsPerWorker {
	out := make(StreamKeyspaceIdInstsPerWorker)
	for streamId, keyspaceIdInstList := range streamKeyspaceIdInstList {
		out[streamId] = make(KeyspaceIdInstsPerWorker)
		for keyspaceId, instList := range keyspaceIdInstList {
			out[streamId][keyspaceId] = make([][]common.IndexInstId, numSnapshotWorkers)
			//for every index managed by this indexer
			for i, idxInstId := range instList {
				index := i % numSnapshotWorkers
				out[streamId][keyspaceId][index] = append(out[streamId][keyspaceId][index], idxInstId)
			}
		}
	}
	return out
}

func copyIndexSnapMap(inMap IndexSnapMap) IndexSnapMap {

	outMap := make(IndexSnapMap)
	for k, v := range inMap {
		outMap[k] = v
	}
	return outMap

}

func destroyIndexSnapMap(ism IndexSnapMap) {

	for _, v := range ism {
		v.Lock()
		if !v.deleted { // Destroy only if index is not already deleted
			DestroyIndexSnapshot(v.snap)
		}
		v.Unlock()
	}

}

func (s *IndexStorageStats) getPlasmaFragmentation() float64 {
	var fragPercent float64

	var wastedSpace int64
	if s.Stats.DataSizeOnDisk != 0 && s.Stats.LogSpace > s.Stats.DataSizeOnDisk {
		wastedSpace = s.Stats.LogSpace - s.Stats.DataSizeOnDisk
	}

	if s.Stats.LogSpace > 0 {
		fragPercent = float64(wastedSpace) * 100 / float64(s.Stats.LogSpace)
	}

	return fragPercent
}

func (s *storageMgr) getNumSnapshotWorkers() int {
	numSnapshotWorkers := s.config["numSnapshotWorkers"].Int()
	if numSnapshotWorkers < 1 {
		//Since indexer supports upto 10000 indexes in a cluster as of 7.0
		numSnapshotWorkers = 10000
	}
	return numSnapshotWorkers
}

func (s *storageMgr) assertOnNonAlignedDiskCommit(streamId common.StreamId,
	keyspaceId string, tsVbuuid *common.TsVbuuid) {

	snapType := tsVbuuid.GetSnapType()
	// For INIT_STREAM, disk snapshot need not be snap aligned
	// Hence, the assertion is only for MAINT_STREAM
	// From 7.x all initial index builds happen in INIT_STREAM. So,
	// there is no need to check for index state from 7.0 as we are
	// doing this assertion check only for MAINT_STREAM
	if (streamId == common.MAINT_STREAM) &&
		(snapType == common.DISK_SNAP ||
			snapType == common.FORCE_COMMIT ||
			snapType == common.FORCE_COMMIT_MERGE) && (tsVbuuid.CheckSnapAligned() == false) {

		logging.Fatalf("StorageMgr::handleCreateSnapshot Disk commit timestamp is not snapshot aligned. "+
			"Stream: %v, KeyspaceId: %v, tsVbuuid: %v", streamId, keyspaceId, tsVbuuid)

	}
}

func (s *storageMgr) handleShardTransfer(cmd Message) {
	// TODO: Add a configurable setting to enable or disable disk snapshotting
	// of shards that are in transfer

	msg := cmd.(*MsgStartShardTransfer)
	shardIds := msg.GetShardIds()

	storageMgrCancelCh := make(chan bool)
	storageMgrRespCh := make(chan bool)
	msg.storageMgrCancelCh = storageMgrCancelCh
	msg.storageMgrRespCh = storageMgrRespCh

	for _, shardId := range shardIds {
		s.shardsInTransfer[shardId] = []chan bool{storageMgrCancelCh, storageMgrRespCh}
	}
	logging.Infof("StorageMgr::handleShardTransfer Updated book-keeping for shardIds: %v", shardIds)
	if s.stm != nil {
		s.stm.ProcessCommand(cmd)
	} else {
		respWithErr(
			cmd,
			fmt.Errorf("StorageMgr::handleShardTransfer ShardTransferManager Not Initialized during msg %v execution",
				cmd),
		)
	}
	s.supvCmdch <- &MsgSuccess{}
}

func (s *storageMgr) handlePopulateShardType(cmd Message) {

	msg := cmd.(*MsgPopulateShardType)
	ttid := msg.GetTransferId()
	shardIds := msg.GetShardIds()
	doneCh := msg.GetDoneCh()

	defer close(doneCh)

	logging.Infof("StorageMgr::handlePopulateShardType For ttid:%v, adding the Shard type for shardIds:%v",
		ttid, shardIds)
	if s.stm != nil {
		s.stm.ProcessCommand(cmd)
	} else {
		respWithErr(
			cmd,
			fmt.Errorf("StorageMgr::handlePopulateShardType ShardTransferManager Not Initialized during msg %v execution",
				cmd),
		)
	}
}

func (s *storageMgr) handleClearShardType(cmd Message) {

	msg := cmd.(*MsgClearShardType)
	shardIds := msg.GetShardIds()
	doneCh := msg.GetDoneCh()

	defer close(doneCh)

	logging.Infof("StorageMgr::handleClearShardType Clearing the Shard type for shardIds:%v",
		shardIds)
	if s.stm != nil {
		s.stm.ProcessCommand(cmd)
	} else {
		respWithErr(
			cmd,
			fmt.Errorf("StorageMgr::handleClearShardType ShardTransferManager Not Initialized during msg %v execution",
				cmd),
		)
	}
}

func (s *storageMgr) ClearRebalanceRunning(cmd Message) {

	start := time.Now()
	defer logging.Infof("StorageMgr::ClearRebalanceRunning Done with clearing rebalance flags for all slices. elapsed: %v", time.Since(start))

	shardIdMap := make(map[common.ShardId]bool)
	switch cmd.GetMsgType() {
	case UNLOCK_SHARDS:
		shardIds := cmd.(*MsgLockUnlockShards).GetShardIds()
		for _, shardId := range shardIds {
			shardIdMap[shardId] = true
		}
	}

	logging.Infof("StorageMgr::ClearRebalanceRunning Clearing rebalance flags for all slices with shardIds: %v", shardIdMap)

	s.muSnap.Lock()
	defer s.muSnap.Unlock()

	indexPartnMap := s.indexPartnMap.Get()

	//for all partitions managed by this indexer
	for _, partnInstMap := range indexPartnMap {

		for _, partnInst := range partnInstMap {
			sc := partnInst.Sc

			if len(shardIdMap) > 0 {
				// Clear rebalance running only for the slices whose shards are getting unlocked
				for _, slice := range sc.GetAllSlices() {
					sliceShards := slice.GetShardIds()

					for _, sliceShard := range sliceShards {
						if _, ok := shardIdMap[sliceShard]; ok {
							// If any shard of the slice is getting unlocked,
							// consider rebalance done for the slice
							slice.ClearRebalRunning()
						}
					}
				}
			} else {
				// Reset rebalRunning for all the slices as this call is due to
				// RESTORE_AND_UNLOCK_LOCKED_SHARDS which happens at the end of
				// rebalance
				for _, slice := range sc.GetAllSlices() {
					slice.ClearRebalRunning()
				}
			}
		}
	}
}

func (s *storageMgr) SetRebalanceRunning(cmd Message) {

	start := time.Now()
	defer logging.Infof("StorageMgr::SetRebalanceRunning Done with setting rebalance flags for all slices. elapsed: %v", time.Since(start))

	shardIdMap := make(map[common.ShardId]bool)
	shardIds := cmd.(*MsgLockUnlockShards).GetShardIds()
	for _, shardId := range shardIds {
		shardIdMap[shardId] = true
	}

	logging.Infof("StorageMgr::SetRebalanceRunning Setting rebalance flags for all slices with shardIds: %v", shardIdMap)

	s.muSnap.Lock()
	defer s.muSnap.Unlock()

	indexPartnMap := s.indexPartnMap.Get()

	//for all partitions managed by this indexer
	for _, partnInstMap := range indexPartnMap {

		for _, partnInst := range partnInstMap {
			sc := partnInst.Sc

			if len(shardIdMap) > 0 {
				// Set rebalance running only for the slices whose shards are getting locked
				for _, slice := range sc.GetAllSlices() {
					sliceShards := slice.GetShardIds()

					for _, sliceShard := range sliceShards {
						if _, ok := shardIdMap[sliceShard]; ok {
							// If any shard of the slice is getting unlocked,
							// consider rebalance done for the slice
							slice.SetRebalRunning()
						}
					}
				}
			}
		}
	}
}

// If rebalance transfer is in progress, then this method
// will cancel rebalance transfer and waits for plasma to finish
// processing. Also, the shard transefer book-keeping is updated
//
// If no tranfer is in progress, then this method is a no-op
func (sm *storageMgr) waitForTransferCompletion(shardIds []common.ShardId) {
	transferInProgress := false
	var cancelCh, respCh chan bool
	for _, shardId := range shardIds {
		if val, ok := sm.shardsInTransfer[shardId]; ok {
			transferInProgress = true
			cancelCh = val[0]
			respCh = val[1]
			break
		}
	}

	// Shards are not being transferred. Return
	if !transferInProgress {
		logging.Infof("StorageMgr::waitForTransferCompletion Transer is not in progress for shardIds: %v", shardIds)
		return
	}

	logging.Infof("StorageMgr::waitForTransferCompletion Initiating transfer cancel for shardIds: %v", shardIds)

	close(cancelCh) // Cancel the transfer
	<-respCh

	logging.Infof("StorageMgr::waitForTransferCompletion Done with transfer cancel for shardIds: %v", shardIds)

	// At this point, transfer is complete. Clear the book-keeping
	for _, shardId := range shardIds {
		delete(sm.shardsInTransfer, shardId)
	}
}

func (sm *storageMgr) handleDestroyEmptyShards() {
	sm.muSnap.Lock()
	defer sm.muSnap.Unlock()

	emptyShards, err := GetEmptyShardInfo_Plasma()
	if err != nil {
		logging.Errorf("StorageMgr::handleDestroyEmptyShards Error observed while retrieving empty shardInfo, err: %v", err)
	} else {
		logging.Infof("StorageMgr::handleDestroyEmptyShards destroying empty shards: %v", emptyShards)
		for _, shardId := range emptyShards {
			err := DestroyShard_Plasma(shardId)
			if err != nil {
				logging.Errorf("StorageMgr::handleDestroyEmptyShards Error observed while destroying shard: %v, err: %v", shardId, err)
			}
		}
	}

	emptyBhiveShards, err := GetEmptyShardInfo_Bhive()
	if err != nil {
		logging.Errorf("StorageMgr::handleDestroyEmptyShards Error observed while retrieving empty Bhive shardInfo, err: %v", err)
	} else {
		logging.Infof("StorageMgr::handleDestroyEmptyShards destroying empty bhive shards: %v", emptyBhiveShards)
		for _, shardId := range emptyBhiveShards {
			err := DestroyShard_Bhive(shardId)
			if err != nil {
				logging.Errorf("StorageMgr::handleDestroyEmptyShards Error observed while destroying bhive shard: %v, err: %v", shardId, err)
			}
		}
	}
}

func (sm *storageMgr) handlePersistanceStatus(msg Message) {
	sm.muSnap.Lock()
	indexInstMap := sm.indexInstMap.Get()
	indexPartnMap := sm.indexPartnMap.Get()
	sm.muSnap.Unlock()

	go func() {
		respCh := msg.(*MsgPersistanceStatus).GetRespCh()
		for instId, inst := range indexInstMap {
			if inst.State != common.INDEX_STATE_ACTIVE {
				continue
			}

			partnMap, ok := indexPartnMap[instId]
			if !ok {
				continue
			}

			for _, partnInst := range partnMap {
				sc := partnInst.Sc

				for _, slice := range sc.GetAllSlices() {
					if slice.IsCleanupDone() == false && slice.IsPersistanceActive() {
						respCh <- true
						return
					}
				}
			}
		}
		respCh <- false
	}()
}

// Used by storage to periodically get the quota
func (s *storageMgr) getStorageQuota() int64 {
	indexerQuota := int64(s.config.GetIndexerMemoryQuota())

	//subtract the codebook memory usage from the total quota
	//as codebook is fully memory resident and that memory cannot
	//be allocated to storage.
	stats := s.stats.Get()
	if stats != nil {
		codebookMemUsage := stats.totalCodebookMemUsage.Value()
		if codebookMemUsage >= indexerQuota/2 {
			//if codebook mem usage is more than 50% of quota,
			//log a warning.
			now := uint64(time.Now().UnixNano())
			sinceLastLog := now - s.lastCodebokMemLogTime
			if sinceLastLog > uint64(300*time.Second) {
				logging.Warnf("StorageMgr:runMemoryQuotaDistributor: High Codebook Memory "+
					"Usage Found %d. Quota %d.", codebookMemUsage, indexerQuota)
				s.lastCodebokMemLogTime = uint64(time.Now().UnixNano())
			}
		}

		if codebookMemUsage >= indexerQuota {
			//If codebook memory usage is more than quota,
			//set quota as 0. This action is not done for developer
			//use cases with lower quota allocation(512MB or lower).
			if indexerQuota >= 512*1024*1024 {
				indexerQuota = 0
			}
		} else {
			indexerQuota = indexerQuota - codebookMemUsage
		}
	}

	//Codebook memusage can keep changing due to index creation/
	//deletion. If indexerQuota is different than the last time,
	//storage will force an update conservatively.

	// Storage takes 90% of the quota
	storageQuota := int64(float64(indexerQuota) * PLASMA_MEMQUOTA_FRAC)

	return storageQuota
}

func (s *storageMgr) getStorageTunerConfig() plasma.MemTunerConfig {
	return plasma.MakeMemTunerConfig(
		int64(s.config["bhive.quotaSplitPercent"].Int()),
		int64(s.config["bhive.minQuotaThreshold"].Int()),
		int64(s.config["bhive.minQuotaDecayDur"].Int()),
		int64(s.config["bhive.quotaMaxShiftPercent"].Int()),
	)
}

func (s *storageMgr) getStorageTunerStats() plasma.MemTunerDistStats {

	// numBhives
	var numBhives int64
	bhiveInstIds := make(map[common.IndexInstId]bool)
	for instId, inst := range s.indexInstMap.Get() {
		if inst.Defn.VectorMeta != nil && inst.Defn.VectorMeta.IsBhive {
			numBhives++
			bhiveInstIds[instId] = true
		}
	}

	// build_progress
	pLowestBP := int64(100)
	bLowestBP := int64(100)
	for instId, iSts := range s.stats.Get().indexes {
		bp := iSts.buildProgress.Value()
		if _, ok := bhiveInstIds[instId]; ok {
			bLowestBP = min(bLowestBP, bp)
		} else {
			pLowestBP = min(pLowestBP, bp)
		}
	}
	if pLowestBP < 0 {
		pLowestBP = 0
	}
	if bLowestBP < 0 {
		bLowestBP = 0
	}

	return plasma.MakeMemTunerDistStats(
		numBhives,
		pLowestBP,
		bLowestBP,
		s.plasmaLastCreateTime,
		s.bhiveLastCreateTime,
	)
}

func (sm *storageMgr) handleBuildBhiveGraph(cmd Message) {
	sm.supvCmdch <- &MsgSuccess{}

	msg := cmd.(*MsgBuildBhiveGraph)
	instId := msg.GetInstId()
	bhiveGraphStatus := msg.GetBhiveGraphStatus()

	indexInstMap := sm.indexInstMap.Get()
	indexPartnMap := sm.indexPartnMap.Get()

	inst, ok := indexInstMap[instId]
	//skip deleted indexes
	if !ok || inst.State == common.INDEX_STATE_DELETED {
		logging.Infof("StorageMgr::handleBhiveBuildGraph instId %v not found or deleted", instId)
		return
	}

	partnMap, ok := indexPartnMap[instId]
	if !ok {
		logging.Infof("StorageMgr::handleBhiveBuildGraph No partitions found for instId %v", instId)
		return
	}

	for partnId, status := range bhiveGraphStatus {

		if !status {
			partnInst, ok := partnMap[partnId]
			if !ok {
				logging.Infof("StorageMgr::handleBhiveBuildGraph No partition info for instId %v partn %v", instId, partnId)
				continue
			}
			sc := partnInst.Sc

			for _, slice := range sc.GetAllSlices() {
				//BuildDone builds the graph if missing
				slice.BuildDone(instId, sm.buildDoneCallback)
			}
		}
	}
}

// These structures are used for validating items_count across multiple replicas
// to identify if an index is corrupt or not
type IndexInfo struct {
	// The fully qualified name of the index (<bucket_name>:<scope_name>:<coll_name>:<index_name>)
	IndexName string `json:"indexName"`

	DefnId        uint64 `json:"defnId"`        // Index definition ID
	InstId        uint64 `json:"instId"`        // Index instanceId
	ReplicaID     int    `json:"replicaId"`     // replica ID of the index
	PartitionID   int    `json:"partitionId"`   // partition ID of the index
	Bucket        string `json:"bucket"`        // bucket to which the index belogs
	IsArrayIndex  bool   `json:"isArrayIndex"`  // Some validations happen only for non-array indexes
	ItemsCount    uint64 `json:"itemsCount"`    // total number of items in the snapshot at the recorded timestamp
	NumPartitions int    `json:"numPartitions"` // maximum number of partitions defined for this index

	timestamp []uint64 // Used only for internal processing - not exported
	nodeId    string   // ID of the node on which the replica partition exists - Used only for internal processing

}

type TimestampedCounts struct {
	// Timestamp (seqnos) when ItemsCount was recorded from the snapshot. All indexes
	// sharing the same timestamp will be grouped together to reduce payload in REST requests

	Timestamp []uint64     `json:"timestamp"`
	NodeId    string       `json:"nodeId"`  // address of the node on which the index exists
	Indexes   []*IndexInfo `json:"indexes"` // List of all the indexes that share the same timestamp
}

func (s *storageMgr) handleGetTimestampedItemsCount(cmd Message) {

	s.supvCmdch <- &MsgSuccess{}

	respCh := cmd.(*MsgTimestampedCountReq).GetRespCh()
	doLog := cmd.(*MsgTimestampedCountReq).GetDoLog()

	// Disable for forestDB. See the comment at CountTotal() invocation
	// in this method for more details
	if common.GetStorageMode() == common.FORESTDB {
		respCh <- nil
		return
	}

	nodeId := s.config["clusterAddr"].String()
	var indexSnapMap IndexSnapMap
	var indexInstMap common.IndexInstMap

	func() {
		s.muSnap.Lock()
		defer s.muSnap.Unlock()

		indexSnapMap = s.indexSnapMap.Get()
		indexInstMap = s.indexInstMap.Get()
	}()

	getTimestampedKey := func(timestamp []uint64) string {
		var str strings.Builder
		for i := range timestamp {
			fmt.Fprintf(&str, "%v,", timestamp[i])
		}
		return str.String()
	}

	go func() {

		// serialise on statsLock so that rollback and snapshot access
		// do not happen simultaneously
		s.statsLock.Lock()
		defer s.statsLock.Unlock()

		timestampedCountsMap := make(map[string]*TimestampedCounts)

		for instId, snapC := range indexSnapMap {

			func() {
				snapC.Lock()
				defer snapC.Unlock()

				indexInst, ok := indexInstMap[instId]
				if !ok ||
					snapC.deleted || // If snap container is deleted, it means index is deleted. Skip the index
					indexInst.Stream != common.MAINT_STREAM || // Process only MAINT_STREAM indexes
					indexInst.State != common.INDEX_STATE_ACTIVE || // process only active indexes
					indexInst.RState != common.REBAL_ACTIVE { // Skip indexes in rebalance
					if doLog || logging.IsEnabled(logging.Verbose) {
						logging.Infof("storageMgr::handleGetTimestampedItemsCount Skip processing inst: %v, partn: %v due to one of the following being true. "+
							"snapC.deleted: %v, state: %v, rstate: %v, stream: %v, arrayIndex: %v",
							snapC.deleted, indexInst.State, indexInst.Stream, indexInst.Defn.IsArrayIndex, indexInst.RState)
					}
					return
				}

				// Since we want to get fully qualified name, use INIT_STREAM so that the keyspace has bucket:scope:collection
				// included in it
				indexName := fmt.Sprintf("%v:%v", indexInst.Defn.KeyspaceId(common.INIT_STREAM), indexInst.Defn.Name)
				replicaID := indexInst.ReplicaId

				partnSnaps := snapC.snap.Partitions()
				for partnId, partnSnap := range partnSnaps {

					sc := partnSnap.Slices()
					for _, sliceSnap := range sc {
						timestamp := sliceSnap.Snapshot().Timestamp().Seqnos
						// Use CountTotal() instead of StatCountTotal()
						// StatCountTotal() will read from slice.committedCount which gets set
						// after a snapshot is created. Since this loop runs async to shapshotting loop,
						// it is possible that the timestamp is read from one snapshot while StatCountTotal()
						// is read from another snapshot.
						//
						// For memdb, plasma, bhive - This method directly reads from snapshot and it is an O(1)
						// operation. For ForestDB, the check is skipped as FDB will iterate over the entire index
						count, err := sliceSnap.Snapshot().CountTotal(nil, nil)
						if err != nil {
							logging.Warnf("storageMgr::handleGetTimestampedItemsCount error observed while retrieving snapshot count "+
								"for instId: %v, partnId: %v. Skipping this index from further processing", indexInst.InstId, partnId)
							continue
						}

						key := getTimestampedKey(timestamp)
						if _, ok := timestampedCountsMap[key]; !ok {
							timestampedCountsMap[key] = &TimestampedCounts{Timestamp: timestamp, NodeId: nodeId}
						}

						indexInfo := &IndexInfo{
							IndexName:    indexName,
							DefnId:       uint64(indexInst.Defn.DefnId),
							InstId:       uint64(indexInst.InstId),
							ReplicaID:    replicaID,
							PartitionID:  int(partnId),
							Bucket:       indexInst.Defn.Bucket,
							IsArrayIndex: indexInst.Defn.IsArrayIndex,
							ItemsCount:   count,
						}

						if common.IsPartitioned(indexInst.Defn.PartitionScheme) {
							indexInfo.NumPartitions = indexInst.Pc.GetNumPartitions()
						} else {
							indexInfo.NumPartitions = 1
						}
						timestampedCountsMap[key].Indexes = append(timestampedCountsMap[key].Indexes, indexInfo)

					}
				}
			}()
		}

		var out []*TimestampedCounts
		for _, tsCounts := range timestampedCountsMap {
			out = append(out, tsCounts)
		}

		respCh <- out
	}()
}
