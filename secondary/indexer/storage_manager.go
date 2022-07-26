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
	"encoding/gob"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	forestdb "github.com/couchbase/indexing/secondary/fdb"
	"github.com/couchbase/indexing/secondary/logging"
)

var (
	ErrIndexRollback            = errors.New("Indexer rollback")
	ErrIndexRollbackOrBootstrap = errors.New("Indexer rollback or warmup")
)

//StorageManager manages the snapshots for the indexes and responsible for storing
//indexer metadata in a config database

const INST_MAP_KEY_NAME = "IndexInstMap"

type StorageManager interface {
}

type storageMgr struct {
	supvCmdch  MsgChannel //supervisor sends commands on this channel
	supvRespch MsgChannel //channel to send any async message to supervisor

	snapshotNotifych chan IndexSnapshot

	indexInstMap  common.IndexInstMap
	indexPartnMap IndexPartnMap

	// Latest readable index snapshot for each index instance
	indexSnapMap map[common.IndexInstId]IndexSnapshot
	// List of waiters waiting for a snapshot to be created with expected
	// atleast-timestamp
	waitersMap map[common.IndexInstId][]*snapshotWaiter

	dbfile *forestdb.File
	meta   *forestdb.KVStore // handle for index meta

	config common.Config

	stats IndexerStatsHolder

	muSnap sync.Mutex //lock to protect snapMap and waitersMap

	lastFlushDone int64
}

type IndexSnapMap map[common.IndexInstId]IndexSnapshot

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

//NewStorageManager returns an instance of storageMgr or err message
//It listens on supvCmdch for command and every command is followed
//by a synchronous response of the supvCmdch.
//Any async response to supervisor is sent to supvRespch.
//If supvCmdch get closed, storageMgr will shut itself down.
func NewStorageManager(supvCmdch MsgChannel, supvRespch MsgChannel,
	indexPartnMap IndexPartnMap, config common.Config, snapshotNotifych chan IndexSnapshot) (
	StorageManager, Message) {

	//Init the storageMgr struct
	s := &storageMgr{
		supvCmdch:        supvCmdch,
		supvRespch:       supvRespch,
		snapshotNotifych: snapshotNotifych,
		indexSnapMap:     make(map[common.IndexInstId]IndexSnapshot),
		waitersMap:       make(map[common.IndexInstId][]*snapshotWaiter),
		config:           config,
	}

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

	//start Storage Manager loop which listens to commands from its supervisor
	go s.run()

	return s, &MsgSuccess{}

}

//run starts the storage manager loop which listens to messages
//from its supervisor(indexer)
func (s *storageMgr) run() {

	//main Storage Manager loop
loop:
	for {
		select {

		case cmd, ok := <-s.supvCmdch:
			if ok {
				if cmd.GetMsgType() == STORAGE_MGR_SHUTDOWN {
					logging.Infof("StorageManager::run Shutting Down")
					close(s.snapshotNotifych)
					s.supvCmdch <- &MsgSuccess{}
					break loop
				}
				s.handleSupvervisorCommands(cmd)
			} else {
				//supervisor channel closed. exit
				break loop
			}

		}
	}
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

	case STORAGE_INDEX_SNAP_REQUEST:
		s.handleGetIndexSnapshot(cmd)

	case STORAGE_INDEX_STORAGE_STATS:
		s.handleGetIndexStorageStats(cmd)

	case STORAGE_INDEX_COMPACT:
		s.handleIndexCompaction(cmd)

	case STORAGE_STATS:
		s.handleStats(cmd)

	case STORAGE_INDEX_MERGE_SNAPSHOT:
		s.handleIndexMergeSnapshot(cmd)

	case STORAGE_INDEX_PRUNE_SNAPSHOT:
		s.handleIndexPruneSnapshot(cmd)

	case STORAGE_UPDATE_SNAP_MAP:
		s.handleUpdateIndexSnapMapForIndex(cmd)

	}
}

//handleCreateSnapshot will create the necessary snapshots
//after flush has completed
func (s *storageMgr) handleCreateSnapshot(cmd Message) {

	s.supvCmdch <- &MsgSuccess{}

	logging.Tracef("StorageMgr::handleCreateSnapshot %v", cmd)

	msgFlushDone := cmd.(*MsgMutMgrFlushDone)

	bucket := msgFlushDone.GetBucket()
	tsVbuuid := msgFlushDone.GetTS()
	streamId := msgFlushDone.GetStreamId()
	flushWasAborted := msgFlushDone.GetAborted()
	hasAllSB := msgFlushDone.HasAllSB()

	numVbuckets := s.config["numVbuckets"].Int()
	snapType := tsVbuuid.GetSnapType()
	tsVbuuid.Crc64 = common.HashVbuuid(tsVbuuid.Vbuuids)

	if snapType == common.NO_SNAP {
		logging.Debugf("StorageMgr::handleCreateSnapshot Skip Snapshot For %v "+
			"%v SnapType %v", streamId, bucket, snapType)

		s.muSnap.Lock()
		defer s.muSnap.Unlock()

		indexInstMap := common.CopyIndexInstMap(s.indexInstMap)
		indexPartnMap := CopyIndexPartnMap(s.indexPartnMap)

		go s.flushDone(streamId, bucket, indexInstMap, indexPartnMap,
			tsVbuuid, flushWasAborted, hasAllSB)

		return
	}

	s.muSnap.Lock()
	defer s.muSnap.Unlock()

	//pass copy of maps to worker
	indexSnapMap := copyIndexSnapMap(s.indexSnapMap)
	indexInstMap := common.CopyIndexInstMap(s.indexInstMap)
	indexPartnMap := CopyIndexPartnMap(s.indexPartnMap)
	stats := s.stats.Get()

	go s.createSnapshotWorker(streamId, bucket, tsVbuuid, indexSnapMap,
		numVbuckets, indexInstMap, indexPartnMap, stats, flushWasAborted, hasAllSB)

}

func (s *storageMgr) createSnapshotWorker(streamId common.StreamId, bucket string,
	tsVbuuid *common.TsVbuuid, indexSnapMap IndexSnapMap, numVbuckets int,
	indexInstMap common.IndexInstMap, indexPartnMap IndexPartnMap, stats *IndexerStats,
	flushWasAborted bool, hasAllSB bool) {

	defer destroyIndexSnapMap(indexSnapMap)

	var needsCommit bool
	var forceCommit bool
	snapType := tsVbuuid.GetSnapType()
	if snapType == common.DISK_SNAP {
		needsCommit = true
	} else if snapType == common.FORCE_COMMIT {
		forceCommit = true
	}

	var wg sync.WaitGroup
	//for every index managed by this indexer
	for idxInstId, partnMap := range indexPartnMap {
		// Create snapshots for all indexes in parallel
		wg.Add(1)
		go func(idxInstId common.IndexInstId, partnMap PartitionInstMap) {
			defer wg.Done()

			idxInst := indexInstMap[idxInstId]
			idxStats := stats.indexes[idxInst.InstId]
			lastIndexSnap := indexSnapMap[idxInstId]
			//if index belongs to the flushed bucket and stream
			if idxInst.Defn.Bucket == bucket &&
				idxInst.Stream == streamId &&
				idxInst.State != common.INDEX_STATE_DELETED {

				// List of snapshots for reading current timestamp
				var isSnapCreated bool = true

				partnSnaps := make(map[common.PartitionId]PartitionSnapshot)
				hasNewSnapshot := false

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
						snapTs := NewTimestamp(numVbuckets)
						if latestSnapshot != nil {
							snapTsVbuuid := latestSnapshot.Timestamp()
							snapTs = getSeqTsFromTsVbuuid(snapTsVbuuid)
						}

						ts := getSeqTsFromTsVbuuid(tsVbuuid)

						//if the flush TS is greater than the last snapshot TS
						//and slice has some changes. Skip only in-memory snapshot
						//in case of unchanged data.
						if latestSnapshot == nil || (ts.GreaterThan(snapTs) &&
							(slice.IsDirty() || needsCommit)) || forceCommit {

							newTsVbuuid := tsVbuuid.Copy()
							var err error
							var info SnapshotInfo
							var newSnapshot Snapshot

							logging.Tracef("StorageMgr::handleCreateSnapshot Creating New Snapshot "+
								"Index: %v PartitionId: %v SliceId: %v Commit: %v Force: %v", idxInstId, partnId, slice.Id(), needsCommit, forceCommit)

							if forceCommit {
								needsCommit = forceCommit
							}

							slice.FlushDone()

							snapCreateStart := time.Now()
							if info, err = slice.NewSnapshot(newTsVbuuid, needsCommit); err != nil {
								logging.Errorf("handleCreateSnapshot::handleCreateSnapshot Error "+
									"Creating new snapshot Slice Index: %v Slice: %v. Skipped. Error %v", idxInstId,
									slice.Id(), err)
								isSnapCreated = false
								common.CrashOnError(err)
								continue
							}
							snapCreateDur := time.Since(snapCreateStart)

							hasNewSnapshot = true

							snapOpenStart := time.Now()
							if newSnapshot, err = slice.OpenSnapshot(info); err != nil {
								logging.Errorf("StorageMgr::handleCreateSnapshot Error Creating Snapshot "+
									"for Index: %v Slice: %v. Skipped. Error %v", idxInstId,
									slice.Id(), err)
								isSnapCreated = false
								common.CrashOnError(err)
								continue
							}
							snapOpenDur := time.Since(snapOpenStart)

							if needsCommit {
								logging.Infof("StorageMgr::handleCreateSnapshot Added New Snapshot Index: %v "+
									"PartitionId: %v SliceId: %v Crc64: %v (%v) SnapCreateDur %v SnapOpenDur %v", idxInstId, partnId, slice.Id(), tsVbuuid.Crc64, info, snapCreateDur, snapOpenDur)
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
							logging.Debugf("StorageMgr::handleCreateSnapshot Skipped Creating New Snapshot for Index %v "+
								"PartitionId %v SliceId %v. No New Mutations. IsDirty %v", idxInstId, partnId, slice.Id(), slice.IsDirty())
							logging.Debugf("StorageMgr::handleCreateSnapshot SnapTs %v FlushTs %v", snapTs, ts)
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
					ts:     tsVbuuid.Copy(),
					partns: partnSnaps,
				}

				if isSnapCreated {
					s.updateSnapMapAndNotify(is, idxStats)
				} else {
					DestroyIndexSnapshot(is)
				}
				s.updateSnapIntervalStat(idxStats)

			}
		}(idxInstId, partnMap)
	}

	wg.Wait()
	s.lastFlushDone = time.Now().UnixNano()

	s.supvRespch <- &MsgMutMgrFlushDone{mType: STORAGE_SNAP_DONE,
		streamId: streamId,
		bucket:   bucket,
		ts:       tsVbuuid,
		aborted:  flushWasAborted}

}

func (s *storageMgr) flushDone(streamId common.StreamId, bucket string,
	indexInstMap common.IndexInstMap, indexPartnMap IndexPartnMap,
	tsVbuuid *common.TsVbuuid, flushWasAborted bool, hasAllSB bool) {

	isInitial := func() bool {
		if streamId == common.INIT_STREAM {
			return true
		}

		for _, inst := range indexInstMap {
			if inst.Stream == streamId &&
				inst.Defn.Bucket == bucket &&
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

		if time.Now().UnixNano()-s.lastFlushDone > checkInterval() &&
			s.config["plasma.writer.tuning.enable"].Bool() {

			var wg sync.WaitGroup

			for idxInstId, partnMap := range indexPartnMap {
				wg.Add(1)
				go func(idxInstId common.IndexInstId, partnMap PartitionInstMap) {
					defer wg.Done()

					idxInst := indexInstMap[idxInstId]
					if idxInst.Defn.Bucket == bucket &&
						idxInst.Stream == streamId &&
						idxInst.State != common.INDEX_STATE_DELETED {

						for _, partnInst := range partnMap {
							for _, slice := range partnInst.Sc.GetAllSlices() {
								slice.FlushDone()
							}
						}
					}
				}(idxInstId, partnMap)
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
			if idxInst.Defn.Bucket == bucket &&
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
		mType:    STORAGE_SNAP_DONE,
		streamId: streamId,
		bucket:   bucket,
		ts:       tsVbuuid,
		aborted:  flushWasAborted}
}

func (s *storageMgr) updateSnapIntervalStat(idxStats *IndexStats) {

	// Compute avgTsInterval
	last := idxStats.lastTsTime.Value()
	curr := int64(time.Now().UnixNano())
	avg := idxStats.avgTsInterval.Value()

	avg = common.ComputeAvg(avg, last, curr)
	if avg != 0 {
		idxStats.avgTsInterval.Set(avg)
		idxStats.sinceLastSnapshot.Set(curr - last)
	}
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

	s.muSnap.Lock()
	defer s.muSnap.Unlock()

	DestroyIndexSnapshot(s.indexSnapMap[is.IndexInstId()])
	s.indexSnapMap[is.IndexInstId()] = is

	// notify a new snapshot through channel
	// the channel receiver needs to destroy snapshot when done
	s.notifySnapshotCreation(is)

	var numReplies int64
	t := time.Now()
	// Also notify any waiters for snapshots creation
	var newWaiters []*snapshotWaiter
	waiters := s.waitersMap[is.IndexInstId()]
	for _, w := range waiters {
		// Clean up expired requests from queue
		if !w.expired.IsZero() && t.After(w.expired) {
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
	s.waitersMap[is.IndexInstId()] = newWaiters
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

//handleRollback will rollback to given timestamp
func (sm *storageMgr) handleRollback(cmd Message) {

	sm.supvCmdch <- &MsgSuccess{}

	streamId := cmd.(*MsgRollback).GetStreamId()
	rollbackTs := cmd.(*MsgRollback).GetRollbackTs()
	bucket := cmd.(*MsgRollback).GetBucket()
	sessionId := cmd.(*MsgRollback).GetSessionId()

	logging.Infof("StorageMgr::handleRollback rollbackTs is %v", rollbackTs)

	var err error
	var restartTs *common.TsVbuuid

	//for every index managed by this indexer
	for idxInstId, partnMap := range sm.indexPartnMap {
		idxInst := sm.indexInstMap[idxInstId]

		//if this bucket in stream needs to be rolled back
		if idxInst.Defn.Bucket == bucket &&
			idxInst.Stream == streamId &&
			idxInst.State != common.INDEX_STATE_DELETED {

			restartTs, err = sm.rollbackIndex(streamId,
				bucket, rollbackTs, idxInstId, partnMap, restartTs)

			if err != nil {
				sm.supvRespch <- &MsgRollbackDone{streamId: streamId,
					bucket:    bucket,
					err:       err,
					sessionId: sessionId}
				return
			}

			if restartTs == nil {
				err = sm.rollbackAllToZero(streamId, bucket)
				if err != nil {
					sm.supvRespch <- &MsgRollbackDone{streamId: streamId,
						bucket:    bucket,
						err:       err,
						sessionId: sessionId}
					return
				}
				break
			}
		}
	}

	go func() {
		sm.muSnap.Lock()
		defer sm.muSnap.Unlock()
		// Notify all scan waiters for indexes in this bucket
		// and stream with error
		stats := sm.stats.Get()
		for idxInstId, waiters := range sm.waitersMap {
			idxInst := sm.indexInstMap[idxInstId]
			idxStats := stats.indexes[idxInst.InstId]
			if idxInst.Defn.Bucket == bucket &&
				idxInst.Stream == streamId {
				for _, w := range waiters {
					w.Error(ErrIndexRollback)
					if idxStats != nil {
						idxStats.numSnapshotWaiters.Add(-1)
					}
				}

				delete(sm.waitersMap, idxInstId)
			}
		}
	}()

	sm.updateIndexSnapMap(sm.indexPartnMap, streamId, bucket)

	stats := sm.stats.Get()
	if bStats, ok := stats.buckets[bucket]; ok {
		bStats.numRollbacks.Add(1)
	}

	if restartTs != nil {
		restartTs = sm.validateRestartTsVbuuid(bucket, restartTs)
	}

	sm.supvRespch <- &MsgRollbackDone{streamId: streamId,
		bucket:    bucket,
		restartTs: restartTs,
		sessionId: sessionId,
	}
}

func (sm *storageMgr) rollbackIndex(streamId common.StreamId, bucket string,
	rollbackTs *common.TsVbuuid, idxInstId common.IndexInstId,
	partnMap PartitionInstMap, minRestartTs *common.TsVbuuid) (*common.TsVbuuid, error) {

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
				slice, snapInfo, markAsUsed)

			if err != nil {
				return nil, err
			}

			if restartTs == nil {
				return nil, nil
			}

			//if restartTs is lower than the minimum, use that
			if !restartTs.AsRecentTs(minRestartTs) {
				minRestartTs = restartTs
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
						"older than last used Snapshot %v. Use nil snapshot.", slice.IndexInstId(), latestSnapInfo)
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
	markAsUsed bool) (*common.TsVbuuid, error) {

	var restartTs *common.TsVbuuid
	if snapInfo != nil {
		err := slice.Rollback(snapInfo)
		if err == nil {
			logging.Infof("StorageMgr::handleRollback Rollback Index: %v "+
				"PartitionId: %v SliceId: %v To Snapshot %v ", idxInstId, partnId,
				slice.Id(), snapInfo)
			restartTs = snapInfo.Timestamp()
			if markAsUsed {
				slice.SetLastRollbackTs(restartTs)
			}
		} else {
			//send error response back
			return nil, err
		}

	} else {
		//if there is no snapshot available, rollback to zero
		err := slice.RollbackToZero()
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
	bucket string) error {

	logging.Infof("StorageMgr::rollbackAllToZero %v %v", streamId, bucket)

	for idxInstId, partnMap := range sm.indexPartnMap {
		idxInst := sm.indexInstMap[idxInstId]

		//if this bucket in stream needs to be rolled back
		if idxInst.Defn.Bucket == bucket &&
			idxInst.Stream == streamId &&
			idxInst.State != common.INDEX_STATE_DELETED {

			partnInstList := sm.getSortedPartnInst(partnMap)
			for _, partnInst := range partnInstList {
				partnId := partnInst.Defn.GetPartitionId()
				sc := partnInst.Sc

				for _, slice := range sc.GetAllSlices() {
					_, err := sm.rollbackToSnapshot(idxInstId, partnId,
						slice, nil, false)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func (sm *storageMgr) validateRestartTsVbuuid(bucket string,
	restartTs *common.TsVbuuid) *common.TsVbuuid {

	clusterAddr := sm.config["clusterAddr"].String()
	numVbuckets := sm.config["numVbuckets"].Int()

	for i := 0; i < MAX_GETSEQS_RETRIES; i++ {

		flog, err := common.BucketFailoverLog(clusterAddr, DEFAULT_POOL,
			bucket, numVbuckets)

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

func (s *storageMgr) addNilSnapshot(idxInstId common.IndexInstId, bucket string) {
	if _, ok := s.indexSnapMap[idxInstId]; !ok {
		ts := common.NewTsVbuuid(bucket, s.config["numVbuckets"].Int())
		snap := &indexSnapshot{
			instId: idxInstId,
			ts:     ts, // nil snapshot should have ZERO Crc64 :)
			epoch:  true,
		}
		s.indexSnapMap[idxInstId] = snap
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
	s.snapshotNotifych <- snap
}

func (s *storageMgr) notifySnapshotCreation(is IndexSnapshot) {
	defer func() {
		if r := recover(); r != nil {
			logging.Errorf("storageMgr::notifySnapshot %v", r)
		}
	}()

	s.snapshotNotifych <- CloneIndexSnapshot(is)
}

func (s *storageMgr) handleUpdateIndexInstMap(cmd Message) {

	logging.Tracef("StorageMgr::handleUpdateIndexInstMap %v", cmd)
	req := cmd.(*MsgUpdateInstMap)
	indexInstMap := req.GetIndexInstMap()
	s.stats.Set(req.GetStatsObject())
	s.indexInstMap = common.CopyIndexInstMap(indexInstMap)

	s.muSnap.Lock()
	defer s.muSnap.Unlock()

	// Remove all snapshot waiters for indexes that do not exist anymore
	for id, ws := range s.waitersMap {
		if inst, ok := s.indexInstMap[id]; !ok ||
			inst.State == common.INDEX_STATE_DELETED {
			for _, w := range ws {
				w.Error(common.ErrIndexNotFound)
			}
			delete(s.waitersMap, id)
		}
	}

	// Cleanup all invalid index's snapshots
	for idxInstId, is := range s.indexSnapMap {
		if inst, ok := s.indexInstMap[idxInstId]; !ok ||
			inst.State == common.INDEX_STATE_DELETED {
			DestroyIndexSnapshot(is)
			delete(s.indexSnapMap, idxInstId)
			s.notifySnapshotDeletion(idxInstId)
		}
	}

	// Add 0 items index snapshots for newly added indexes
	for idxInstId, inst := range s.indexInstMap {
		s.addNilSnapshot(idxInstId, inst.Defn.Bucket)
	}

	//if manager is not enable, store the updated InstMap in
	//meta file
	if s.config["enableManager"].Bool() == false {

		instMap := common.CopyIndexInstMap(s.indexInstMap)

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

func (s *storageMgr) handleUpdateIndexPartnMap(cmd Message) {

	logging.Tracef("StorageMgr::handleUpdateIndexPartnMap %v", cmd)
	indexPartnMap := cmd.(*MsgUpdatePartnMap).GetIndexPartnMap()
	s.indexPartnMap = CopyIndexPartnMap(indexPartnMap)

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

	req := cmd.(*MsgIndexSnapRequest)
	inst, found := s.indexInstMap[req.GetIndexId()]
	if !found || inst.State == common.INDEX_STATE_DELETED {
		req.respch <- common.ErrIndexNotFound
		return
	}

	stats := s.stats.Get()
	idxStats := stats.indexes[req.GetIndexId()]

	s.muSnap.Lock()
	defer s.muSnap.Unlock()

	// Return snapshot immediately if a matching snapshot exists already
	// Else add into waiters list so that next snapshot creation event
	// can notify the requester when a snapshot with matching timestamp
	// is available.
	is := s.indexSnapMap[req.GetIndexId()]
	if is != nil && isSnapshotConsistent(is, req.GetConsistency(), req.GetTS()) {
		req.respch <- CloneIndexSnapshot(is)
		return
	}

	if idxStats != nil {
		idxStats.numSnapshotWaiters.Add(1)
	}

	w := newSnapshotWaiter(
		req.GetIndexId(), req.GetTS(), req.GetConsistency(),
		req.GetReplyChannel(), req.GetExpiredTime())

	if ws, ok := s.waitersMap[req.GetIndexId()]; ok {
		s.waitersMap[req.idxInstId] = append(ws, w)
	} else {
		s.waitersMap[req.idxInstId] = []*snapshotWaiter{w}
	}
}

func (s *storageMgr) handleGetIndexStorageStats(cmd Message) {
	s.supvCmdch <- &MsgSuccess{}
	req := cmd.(*MsgIndexStorageStats)
	replych := req.GetReplyChannel()
	stats := s.getIndexStorageStats()
	replych <- stats
}

func (s *storageMgr) handleStats(cmd Message) {
	s.supvCmdch <- &MsgSuccess{}

	req := cmd.(*MsgStatsRequest)
	replych := req.GetReplyChannel()
	storageStats := s.getIndexStorageStats()

	stats := s.stats.Get()
	for _, st := range storageStats {
		inst := s.indexInstMap[st.InstId]
		if inst.State == common.INDEX_STATE_DELETED {
			continue
		}

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
		}
	}

	replych <- true
}

func (s *storageMgr) getIndexStorageStats() []IndexStorageStats {
	var stats []IndexStorageStats
	var err error
	var sts StorageStatistics

	for idxInstId, partnMap := range s.indexPartnMap {

		inst, ok := s.indexInstMap[idxInstId]
		//skip deleted indexes
		if !ok || inst.State == common.INDEX_STATE_DELETED {
			continue
		}

		for _, partnInst := range partnMap {
			var internalData []string
			var dataSz, dataSzOnDisk, logSpace, diskSz, memUsed, extraSnapDataSize int64
			var getBytes, insertBytes, deleteBytes int64
			var nslices int64
			var needUpgrade = false

			slices := partnInst.Sc.GetAllSlices()
			nslices += int64(len(slices))
			for _, slice := range slices {
				sts, err = slice.Statistics()
				if err != nil {
					break
				}

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

				needUpgrade = needUpgrade || sts.NeedUpgrade
			}

			if err == nil {
				stat := IndexStorageStats{
					InstId:  idxInstId,
					PartnId: partnInst.Defn.GetPartitionId(),
					Name:    inst.Defn.Name,
					Bucket:  inst.Defn.Bucket,
					Stats: StorageStatistics{
						DataSize:          dataSz,
						DataSizeOnDisk:    dataSzOnDisk,
						LogSpace:          logSpace,
						DiskSize:          diskSz,
						MemUsed:           memUsed,
						GetBytes:          getBytes,
						InsertBytes:       insertBytes,
						DeleteBytes:       deleteBytes,
						ExtraSnapDataSize: extraSnapDataSize,
						NeedUpgrade:       needUpgrade,
						InternalData:      internalData,
					},
				}

				stats = append(stats, stat)
			}
		}
	}

	return stats
}

func (s *storageMgr) handleIndexMergeSnapshot(cmd Message) {
	req := cmd.(*MsgIndexMergeSnapshot)
	srcInstId := req.GetSourceInstId()
	tgtInstId := req.GetTargetInstId()
	partitions := req.GetPartitions()

	s.muSnap.Lock()

	source, ok := s.indexSnapMap[srcInstId]
	if !ok {
		s.muSnap.Unlock()
		s.supvCmdch <- &MsgSuccess{}
		return
	}

	target, ok := s.indexSnapMap[tgtInstId]
	if !ok {
		// increment source snapshot refcount
		target = s.deepCloneIndexSnapshot(source, nil)

	} else {

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
		if !source.Timestamp().EqualOrGreater(target.Timestamp()) {
			s.muSnap.Unlock()
			s.supvCmdch <- &MsgError{
				err: Error{code: ERROR_STORAGE_MGR_MERGE_SNAPSHOT_FAIL,
					severity: FATAL,
					category: STORAGE_MGR,
					cause: fmt.Errorf("Timestamp mismatch between snapshot\n target %v\n source %v\n",
						target.Timestamp(), source.Timestamp())}}
			return
		}

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
				s.muSnap.Unlock()
				s.supvCmdch <- &MsgError{
					err: Error{code: ERROR_STORAGE_MGR_MERGE_SNAPSHOT_FAIL,
						severity: FATAL,
						category: STORAGE_MGR,
						cause: fmt.Errorf("Source snapshot %v does not have all the required partitions %v",
							srcInstId, partitions)}}
				return
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
					s.muSnap.Unlock()
					s.supvCmdch <- &MsgError{
						err: Error{code: ERROR_STORAGE_MGR_MERGE_SNAPSHOT_FAIL,
							severity: FATAL,
							category: STORAGE_MGR,
							cause: fmt.Errorf("Duplicate partition %v found between source %v and target %v",
								sp.PartitionId(), srcInstId, tgtInstId)}}
					return
				}
			}
		} else {
			logging.Infof("skip validation in merge partitions %v between inst %v and %v", partitions, srcInstId, tgtInstId)
		}

		// Deep clone a new snapshot by copying internal maps + increment target snapshot refcount.
		// The target snapshot could be being used (e.g. under scan).  Increment the snapshot refcount
		// ensure that the snapshot will not get reclaimed.
		target = s.deepCloneIndexSnapshot(target, nil)
		if len(partitions) != 0 {
			// Increment source snaphsot refcount (only for copied partitions).  Those snapshots will
			// be copied over to the target snapshot.  Note that the source snapshot can have different
			// refcount than the target snapshot, since the source snapshot may not be used for scanning.
			// But it should be safe to copy from source to target, even if ref count is different.
			source = s.deepCloneIndexSnapshot(source, partitions)

			// move the partition in source snapshot to target snapshot
			for _, snap := range source.Partitions() {
				target.Partitions()[snap.PartitionId()] = snap
			}
		}
	}

	// decrement source snapshot refcount
	// Do not decrement source snapshot refcount.   When the proxy instance is deleted, storage manager will be notified
	// of the new instance state.   Storage manager will then decrement the ref count at that time.
	//DestroyIndexSnapshot(s.indexSnapMap[srcInstId])

	stats := s.stats.Get()
	idxStats := stats.indexes[tgtInstId]

	s.muSnap.Unlock()
	// update the target with new snapshot.  This will also decrement target old snapshot refcount.
	s.updateSnapMapAndNotify(target, idxStats)

	s.supvCmdch <- &MsgSuccess{}
}

func (s *storageMgr) handleIndexPruneSnapshot(cmd Message) {
	req := cmd.(*MsgIndexPruneSnapshot)
	instId := req.GetInstId()
	partitions := req.GetPartitions()

	s.muSnap.Lock()

	snapshot, ok := s.indexSnapMap[instId]
	if !ok {
		s.muSnap.Unlock()
		s.supvCmdch <- &MsgSuccess{}
		return
	}

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
	newSnapshot := s.deepCloneIndexSnapshot(snapshot, kept)

	stats := s.stats.Get()
	idxStats := stats.indexes[instId]

	s.muSnap.Unlock()
	s.updateSnapMapAndNotify(newSnapshot, idxStats)

	s.supvCmdch <- &MsgSuccess{}
}

func (s *storageMgr) deepCloneIndexSnapshot(is IndexSnapshot, partnIds []common.PartitionId) IndexSnapshot {

	snap := is.(*indexSnapshot)

	clone := &indexSnapshot{
		instId: snap.instId,
		ts:     snap.ts.Copy(),
		partns: make(map[common.PartitionId]PartitionSnapshot),
	}

	for partnId, partnSnap := range snap.Partitions() {

		toClone := len(partnIds) == 0
		for _, partnId2 := range partnIds {
			if partnId == partnId2 {
				toClone = true
				break
			}
		}

		if toClone {
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

	inst, ok := s.indexInstMap[req.GetInstId()]
	stats := s.stats.Get()
	if !ok || inst.State == common.INDEX_STATE_DELETED {
		errch <- common.ErrIndexNotFound
		return
	}

	partnMap, _ := s.indexPartnMap[req.GetInstId()]
	idxStats := stats.indexes[req.GetInstId()]
	idxStats.numCompactions.Add(1)

	// Increment rc for slices
	for _, partnInst := range partnMap {
		// non-partitioned index has partitionId 0
		if partnInst.Defn.GetPartitionId() == req.GetPartitionId() {
			for _, slice := range partnInst.Sc.GetAllSlices() {
				slice.IncrRef()
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
		usableSnapshot, err := slice.OpenSnapshot(snapInfo)
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
	streamId common.StreamId, bucket string) {

	s.muSnap.Lock()
	defer s.muSnap.Unlock()

	for idxInstId, partnMap := range indexPartnMap {
		idxInst := s.indexInstMap[idxInstId]
		s.updateIndexSnapMapForIndex(idxInstId, idxInst, partnMap, streamId, bucket)
	}
}

// Caller of updateIndexSnapMapForIndex should ensure
// locking and subsequent unlocking of muSnap
func (s *storageMgr) updateIndexSnapMapForIndex(idxInstId common.IndexInstId, idxInst common.IndexInst,
	partnMap PartitionInstMap, streamId common.StreamId, bucket string) {

	partitionIDs, _ := idxInst.Pc.GetAllPartitionIds()
	logging.Infof("StorageMgr::updateIndexSnapMapForIndex IndexInst %v Partitions %v",
		idxInstId, partitionIDs)

	needRestart := false
	//if bucket and stream have been provided
	if bucket != "" && streamId != common.ALL_STREAMS {
		//skip the index if either bucket or stream don't match
		if idxInst.Defn.Bucket != bucket || idxInst.Stream != streamId {
			return
		}
		//skip deleted indexes
		if idxInst.State == common.INDEX_STATE_DELETED {
			return
		}
	}

	DestroyIndexSnapshot(s.indexSnapMap[idxInstId])
	delete(s.indexSnapMap, idxInstId)
	s.notifySnapshotDeletion(idxInstId)

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
	}

	if len(partnSnapMap) != 0 {
		is := &indexSnapshot{
			instId: idxInstId,
			ts:     tsVbuuid,
			partns: partnSnapMap,
		}
		s.indexSnapMap[idxInstId] = is
		s.notifySnapshotCreation(is)
	} else {
		logging.Infof("StorageMgr::updateIndexSnapMapForIndex IndexInst %v Adding Nil Snapshot.",
			idxInstId)
		s.addNilSnapshot(idxInstId, bucket)
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
	bucket := req.GetBucket()

	s.muSnap.Lock()
	s.updateIndexSnapMapForIndex(idxInstId, idxInst, partnMap, streamId, bucket)
	s.muSnap.Unlock()

	s.supvCmdch <- &MsgSuccess{}
}

func copyIndexSnapMap(inMap IndexSnapMap) IndexSnapMap {

	outMap := make(IndexSnapMap)
	for k, v := range inMap {
		outMap[k] = CloneIndexSnapshot(v)
	}
	return outMap

}

func destroyIndexSnapMap(ism IndexSnapMap) {

	for _, v := range ism {
		DestroyIndexSnapshot(v)
	}

}

func (s IndexStorageStats) getPlasmaFragmentation() float64 {
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
