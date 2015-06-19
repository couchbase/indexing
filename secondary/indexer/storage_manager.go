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
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/fdb"
	"github.com/couchbase/indexing/secondary/logging"
	"sync"
	"time"
)

var (
	ErrIndexRollback = errors.New("Indexer rollback")
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
}

type IndexSnapMap map[common.IndexInstId]IndexSnapshot

type snapshotWaiter struct {
	wch       chan interface{}
	ts        *common.TsVbuuid
	cons      common.Consistency
	idxInstId common.IndexInstId
}

func newSnapshotWaiter(idxId common.IndexInstId, ts *common.TsVbuuid,
	cons common.Consistency,
	ch chan interface{}) *snapshotWaiter {

	return &snapshotWaiter{
		ts:        ts,
		cons:      cons,
		wch:       ch,
		idxInstId: idxId,
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

	s.updateIndexSnapMap(indexPartnMap, common.ALL_STREAMS, "")

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

	numVbuckets := s.config["numVbuckets"].Int()
	snapType := tsVbuuid.GetSnapType()
	tsVbuuid.Crc64 = common.HashVbuuid(tsVbuuid.Vbuuids)

	if snapType == common.NO_SNAP {
		logging.Debugf("StorageMgr::handleCreateSnapshot Skip Snapshot For %v "+
			"%v SnapType %v", streamId, bucket, snapType)

		s.supvRespch <- &MsgMutMgrFlushDone{mType: STORAGE_SNAP_DONE,
			streamId: streamId,
			bucket:   bucket,
			ts:       tsVbuuid}
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
		numVbuckets, indexInstMap, indexPartnMap, stats)

}

func (s *storageMgr) createSnapshotWorker(streamId common.StreamId, bucket string,
	tsVbuuid *common.TsVbuuid, indexSnapMap IndexSnapMap, numVbuckets int,
	indexInstMap common.IndexInstMap, indexPartnMap IndexPartnMap, stats *IndexerStats) {

	defer destroyIndexSnapMap(indexSnapMap)

	var needsCommit bool
	snapType := tsVbuuid.GetSnapType()
	if snapType == common.DISK_SNAP {
		needsCommit = true
	}

	//for every index managed by this indexer
	for idxInstId, partnMap := range indexPartnMap {
		idxInst := indexInstMap[idxInstId]
		idxStats := stats.indexes[idxInst.InstId]

		//if index belongs to the flushed bucket and stream
		if idxInst.Defn.Bucket == bucket &&
			idxInst.Stream == streamId &&
			idxInst.State != common.INDEX_STATE_DELETED {

			// List of snapshots for reading current timestamp
			var isSnapCreated bool = true

			lastIndexSnap := indexSnapMap[idxInstId]

			partnSnaps := make(map[common.PartitionId]PartitionSnapshot)
			//for all partitions managed by this indexer
			for partnId, partnInst := range partnMap {
				var lastPartnSnap PartitionSnapshot

				if lastIndexSnap != nil {
					lastPartnSnap = lastIndexSnap.Partitions()[partnId]
				}
				sc := partnInst.Sc

				sliceSnaps := make(map[SliceId]SliceSnapshot)
				//create snapshot for all the slices
				for _, slice := range sc.GetAllSlices() {
					var latestSnapshot Snapshot
					if lastIndexSnap.Partitions() != nil {
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
					//TODO Is it better to have a IsDirty() in Slice interface
					//rather than comparing the last snapshot?
					if latestSnapshot == nil || ts.GreaterThan(snapTs) {
						//commit the outstanding data

						logging.Tracef("StorageMgr::handleCreateSnapshot \n\tCommit Data Index: "+
							"%v PartitionId: %v SliceId: %v", idxInstId, partnId, slice.Id())

						newTsVbuuid := tsVbuuid.Copy()
						var err error
						var info SnapshotInfo
						var newSnapshot Snapshot

						logging.Tracef("StorageMgr::handleCreateSnapshot \n\tCreating New Snapshot "+
							"Index: %v PartitionId: %v SliceId: %v Commit: %v", idxInstId, partnId, slice.Id(), needsCommit)

						if info, err = slice.NewSnapshot(newTsVbuuid, needsCommit); err != nil {
							logging.Errorf("handleCreateSnapshot::handleCreateSnapshot \n\tError "+
								"Creating new snapshot Slice Index: %v Slice: %v. Skipped. Error %v", idxInstId,
								slice.Id(), err)
							isSnapCreated = false
							common.CrashOnError(err)
							continue
						}

						idxStats := stats.indexes[idxInstId]
						idxStats.numSnapshots.Add(1)
						if needsCommit {
							idxStats.numCommits.Add(1)
						}

						if newSnapshot, err = slice.OpenSnapshot(info); err != nil {
							logging.Errorf("StorageMgr::handleCreateSnapshot \n\tError Creating Snapshot "+
								"for Index: %v Slice: %v. Skipped. Error %v", idxInstId,
								slice.Id(), err)
							isSnapCreated = false
							common.CrashOnError(err)
							continue
						}

						logging.Infof("StorageMgr::handleCreateSnapshot \n\tAdded New Snapshot Index: %v "+
							"PartitionId: %v SliceId: %v (%v)", idxInstId, partnId, slice.Id(), info)

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
						logging.Warnf("StorageMgr::handleCreateSnapshot \n\tSkipped Creating New Snapshot for Index %v "+
							"PartitionId %v SliceId %v. No New Mutations.", idxInstId, partnId, slice.Id())
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
			s.updateSnapIntervalStat(idxStats)
		}
	}

	s.supvRespch <- &MsgMutMgrFlushDone{mType: STORAGE_SNAP_DONE,
		streamId: streamId,
		bucket:   bucket,
		ts:       tsVbuuid}

}

func (s *storageMgr) updateSnapIntervalStat(idxStats *IndexStats) {
	last := idxStats.lastTsTime.Value()
	curr := int64(time.Now().UnixNano())
	avg := idxStats.avgTsInterval.Value()
	interval := curr - last
	if avg == 0 {
		avg = interval
	}

	if last != 0 {
		idxStats.avgTsInterval.Set((interval + avg) / 2)
		idxStats.sinceLastSnapshot.Set(interval)
	}
	idxStats.lastTsTime.Set(curr)
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

	// Also notify any waiters for snapshots creation
	var newWaiters []*snapshotWaiter
	waiters := s.waitersMap[is.IndexInstId()]
	for _, w := range waiters {
		if isSnapshotConsistent(is, w.cons, w.ts) {
			w.Notify(CloneIndexSnapshot(is))
			continue
		}
		newWaiters = append(newWaiters, w)
	}
	s.waitersMap[is.IndexInstId()] = newWaiters
	idxStats.numSnapshotWaiters.Set(int64(len(newWaiters)))
}

//handleRollback will rollback to given timestamp
func (sm *storageMgr) handleRollback(cmd Message) {

	streamId := cmd.(*MsgRollback).GetStreamId()
	rollbackTs := cmd.(*MsgRollback).GetRollbackTs()
	bucket := cmd.(*MsgRollback).GetBucket()

	numVbuckets := sm.config["numVbuckets"].Int()

	var respTs *common.TsVbuuid

	//for every index managed by this indexer
	for idxInstId, partnMap := range sm.indexPartnMap {
		idxInst := sm.indexInstMap[idxInstId]

		//if this bucket in stream needs to be rolled back
		if idxInst.Defn.Bucket == bucket &&
			idxInst.Stream == streamId &&
			idxInst.State != common.INDEX_STATE_DELETED {

			//for all partitions managed by this indexer
			for partnId, partnInst := range partnMap {
				sc := partnInst.Sc

				//rollback all slices
				for _, slice := range sc.GetAllSlices() {
					infos, err := slice.GetSnapshots()
					// TODO: Proper error handling if possible
					if err != nil {
						panic("Unable read snapinfo -" + err.Error())
					}
					s := NewSnapshotInfoContainer(infos)
					snapInfo := s.GetOlderThanTS(rollbackTs)
					if snapInfo != nil {
						err := slice.Rollback(snapInfo)
						if err == nil {
							logging.Infof("StorageMgr::handleRollback \n\t Rollback Index: %v "+
								"PartitionId: %v SliceId: %v To Snapshot %v ", idxInstId, partnId,
								slice.Id(), snapInfo)
							respTs = snapInfo.Timestamp()
						} else {
							//send error response back
							//TODO handle the case where some of the slices fail to rollback
							sm.supvCmdch <- &MsgError{err: Error{code: ERROR_STORAGE_MGR_ROLLBACK_FAIL,
								severity: FATAL,
								category: STORAGE_MGR,
								cause:    err}}
							return
						}

					} else {
						//if there is no snapshot available, rollback to zero
						err := slice.RollbackToZero()
						if err == nil {
							logging.Infof("StorageMgr::handleRollback \n\t Rollback Index: %v "+
								"PartitionId: %v SliceId: %v To Zero ", idxInstId, partnId,
								slice.Id())
							respTs = common.NewTsVbuuid(bucket, numVbuckets)
						} else {
							//send error response back
							//TODO handle the case where some of the slices fail to rollback
							sm.supvCmdch <- &MsgError{err: Error{code: ERROR_STORAGE_MGR_ROLLBACK_FAIL,
								severity: FATAL,
								category: STORAGE_MGR,
								cause:    err}}
							return
						}
					}

				}
			}
		}
	}

	go func() {
		sm.muSnap.Lock()
		defer sm.muSnap.Unlock()
		// Notify all scan waiters for indexes in this bucket
		// and stream with error
		for idxInstId, waiters := range sm.waitersMap {
			idxInst := sm.indexInstMap[idxInstId]
			if idxInst.Defn.Bucket == bucket &&
				idxInst.Stream == streamId {
				for _, w := range waiters {
					w.Error(ErrIndexRollback)
				}
			}
		}
	}()

	sm.updateIndexSnapMap(sm.indexPartnMap, streamId, bucket)

	sm.supvCmdch <- &MsgRollback{streamId: streamId,
		bucket:     bucket,
		rollbackTs: respTs}
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
				w.Error(ErrIndexNotFound)
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
		req.respch <- ErrIndexNotFound
		return
	}

	s.muSnap.Lock()
	defer s.muSnap.Unlock()

	// Return snapshot immediately if a matching snapshot exists already
	// Otherwise add into waiters list so that next snapshot creation event
	// can notify the requester when a snapshot with matching timestamp
	// is available.
	is := s.indexSnapMap[req.GetIndexId()]
	if is != nil && isSnapshotConsistent(is, req.GetConsistency(), req.GetTS()) {
		req.respch <- CloneIndexSnapshot(is)
		return
	}
	w := newSnapshotWaiter(
		req.GetIndexId(), req.GetTS(), req.GetConsistency(),
		req.GetReplyChannel())
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

		idxStats := stats.indexes[st.InstId]
		// TODO(sarath): Investigate the reason for inconsistent stats map
		// This nil check is a workaround to avoid indexer crashes for now.
		if idxStats != nil {
			idxStats.diskSize.Set(st.Stats.DiskSize)
			idxStats.dataSize.Set(st.Stats.DataSize)
			idxStats.fragPercent.Set(st.Stats.Fragmentation)
			idxStats.getBytes.Set(st.Stats.GetBytes)
			idxStats.insertBytes.Set(st.Stats.InsertBytes)
			idxStats.deleteBytes.Set(st.Stats.DeleteBytes)
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

		var dataSz, diskSz int64
		var getBytes, insertBytes, deleteBytes, fragPercent int64
		var nslices int64
	loop:
		for _, partnInst := range partnMap {
			slices := partnInst.Sc.GetAllSlices()
			nslices += int64(len(slices))
			for _, slice := range slices {
				sts, err = slice.Statistics()
				if err != nil {
					break loop
				}

				dataSz += sts.DataSize
				diskSz += sts.DiskSize
				getBytes += sts.GetBytes
				insertBytes += sts.InsertBytes
				deleteBytes += sts.DeleteBytes
				fragPercent += sts.Fragmentation
			}
		}

		if err == nil {
			stat := IndexStorageStats{
				InstId: idxInstId,
				Stats: StorageStatistics{
					DataSize:      dataSz,
					DiskSize:      diskSz,
					GetBytes:      getBytes,
					InsertBytes:   insertBytes,
					DeleteBytes:   deleteBytes,
					Fragmentation: fragPercent / nslices,
				},
			}

			stats = append(stats, stat)
		}
	}

	return stats
}

func (s *storageMgr) handleIndexCompaction(cmd Message) {
	s.supvCmdch <- &MsgSuccess{}
	req := cmd.(*MsgIndexCompact)
	errch := req.GetErrorChannel()
	var slices []Slice

	inst, ok := s.indexInstMap[req.GetInstId()]
	stats := s.stats.Get()
	if !ok || inst.State == common.INDEX_STATE_DELETED {
		errch <- ErrIndexNotFound
		return
	}

	partnMap, _ := s.indexPartnMap[req.GetInstId()]
	idxStats := stats.indexes[req.GetInstId()]
	idxStats.numCompactions.Add(1)

	// Increment rc for slices
	for _, partnInst := range partnMap {
		for _, slice := range partnInst.Sc.GetAllSlices() {
			slice.IncrRef()
			slices = append(slices, slice)
		}
	}

	// Perform file compaction without blocking storage manager main loop
	go func() {
		for _, slice := range slices {
			err := slice.Compact()
			slice.DecrRef()
			if err != nil {
				errch <- err
				return
			}
		}

		errch <- nil
	}()
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

	var tsVbuuid *common.TsVbuuid
	for idxInstId, partnMap := range indexPartnMap {

		//if bucket and stream have been provided
		if bucket != "" && streamId != common.ALL_STREAMS {
			idxInst := s.indexInstMap[idxInstId]
			//skip the index if either bucket or stream don't match
			if idxInst.Defn.Bucket != bucket || idxInst.Stream != streamId {
				continue
			}
			//skip deleted indexes
			if idxInst.State == common.INDEX_STATE_DELETED {
				continue
			}
		}

		//there is only one partition for now
		partnInst := partnMap[0]
		sc := partnInst.Sc

		//there is only one slice for now
		slice := sc.GetSliceById(0)
		infos, err := slice.GetSnapshots()
		// TODO: Proper error handling if possible
		if err != nil {
			panic("Unable to read snapinfo -" + err.Error())
		}

		DestroyIndexSnapshot(s.indexSnapMap[idxInstId])
		delete(s.indexSnapMap, idxInstId)
		s.notifySnapshotDeletion(idxInstId)

		snapInfoContainer := NewSnapshotInfoContainer(infos)
		latestSnapshotInfo := snapInfoContainer.GetLatest()

		if latestSnapshotInfo != nil {
			logging.Infof("StorageMgr::updateIndexSnapMap IndexInst:%v Attempting to open snapshot (%v)",
				idxInstId, latestSnapshotInfo)
			latestSnapshot, err := slice.OpenSnapshot(latestSnapshotInfo)
			if err != nil {
				panic("Unable to open snapshot -" + err.Error())
			}
			ss := &sliceSnapshot{
				id:   SliceId(0),
				snap: latestSnapshot,
			}

			tsVbuuid = latestSnapshotInfo.Timestamp()

			sid := SliceId(0)
			pid := common.PartitionId(0)

			ps := &partitionSnapshot{
				id:     pid,
				slices: map[SliceId]SliceSnapshot{sid: ss},
			}

			is := &indexSnapshot{
				instId: idxInstId,
				ts:     tsVbuuid,
				partns: map[common.PartitionId]PartitionSnapshot{pid: ps},
			}
			s.indexSnapMap[idxInstId] = is
			s.notifySnapshotCreation(is)
		} else {
			s.addNilSnapshot(idxInstId, bucket)
		}
	}
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
