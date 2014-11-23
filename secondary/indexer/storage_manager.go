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
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbaselabs/goforestdb"
)

//StorageManager manages the snapshots for the indexes and responsible for storing
//indexer metadata in a config database
//TODO - Add config database storage

const INST_MAP_KEY_NAME = "IndexInstMap"

type StorageManager interface {
}

type storageMgr struct {
	supvCmdch  MsgChannel //supervisor sends commands on this channel
	supvRespch MsgChannel //channel to send any async message to supervisor

	indexInstMap  common.IndexInstMap
	indexPartnMap IndexPartnMap

	// Latest readable timestamps for each index instance
	tsMap map[common.IndexInstId]*common.TsVbuuid
	// List of waiters waiting for a snapshot to be created with expected
	// atleast-timestamp
	waitersMap map[common.IndexInstId][]*snapshotWaiter

	dbfile *forestdb.File
	meta   *forestdb.KVStore // handle for index meta
}

type snapshotWaiter struct {
	wch       chan interface{}
	ts        *common.TsVbuuid
	idxInstId common.IndexInstId
}

func newSnapshotWaiter(idxId common.IndexInstId, ts *common.TsVbuuid,
	ch chan interface{}) *snapshotWaiter {

	return &snapshotWaiter{
		ts:        ts,
		wch:       ch,
		idxInstId: idxId,
	}
}

func (w *snapshotWaiter) Notify(ts *common.TsVbuuid) {
	w.wch <- ts
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
	indexPartnMap IndexPartnMap) (
	StorageManager, Message) {

	//Init the storageMgr struct
	s := &storageMgr{
		supvCmdch:  supvCmdch,
		supvRespch: supvRespch,
		tsMap:      make(map[common.IndexInstId]*common.TsVbuuid),
		waitersMap: make(map[common.IndexInstId][]*snapshotWaiter),
	}

	config := forestdb.DefaultConfig()
	kvconfig := forestdb.DefaultKVStoreConfig()
	var err error

	if s.dbfile, err = forestdb.Open("meta", config); err != nil {
		return nil, &MsgError{err: Error{cause: err}}
	}

	// Make use of default kvstore provided by forestdb
	if s.meta, err = s.dbfile.OpenKVStore("default", kvconfig); err != nil {
		return nil, &MsgError{err: Error{cause: err}}
	}

	if len(indexPartnMap) != 0 {
		s.initTsMap(indexPartnMap)
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
					common.Infof("StorageManager::run Shutting Down")
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

	case STORAGE_TS_REQUEST:
		s.handleGetTSForIndex(cmd)
	}
}

//handleCreateSnapshot will create the necessary snapshots
//after flush has completed
func (s *storageMgr) handleCreateSnapshot(cmd Message) {

	common.Debugf("StorageMgr::handleCreateSnapshot %v", cmd)

	bucket := cmd.(*MsgMutMgrFlushDone).GetBucket()
	tsVbuuid := cmd.(*MsgMutMgrFlushDone).GetTS()

	//for every index managed by this indexer
	for idxInstId, partnMap := range s.indexPartnMap {
		idxInst := s.indexInstMap[idxInstId]

		//if index belongs to the flushed bucket
		if idxInst.Defn.Bucket == bucket {

			//for all partitions managed by this indexer
			for partnId, partnInst := range partnMap {
				sc := partnInst.Sc

				//create snapshot for all the slices
				for _, slice := range sc.GetAllSlices() {

					//if flush timestamp is greater than last
					//snapshot timestamp, create a new snapshot

					snapContainer := slice.GetSnapshotContainer()

					latestSnapshot := snapContainer.GetLatestSnapshot()

					snapTs := NewTimestamp()
					if latestSnapshot != nil {
						snapTsVbuuid := latestSnapshot.Timestamp()
						snapTs = getStabilityTSFromTsVbuuid(snapTsVbuuid)
					}

					ts := getStabilityTSFromTsVbuuid(tsVbuuid)

					//if the flush TS is greater than the last snapshot TS
					//TODO Is it better to have a IsDirty() in Slice interface
					//rather than comparing the last snapshot?
					if latestSnapshot == nil || ts.GreaterThan(snapTs) {
						//commit the outstanding data

						common.Debugf("StorageMgr::handleCreateSnapshot \n\tCommit Data Index: "+
							"%v PartitionId: %v SliceId: %v", idxInstId, partnId, slice.Id())

						newTsVbuuid := tsVbuuid.Copy()
						slice.SetTimestamp(newTsVbuuid)

						if err := slice.Commit(); err != nil {

							common.Errorf("handleCreateSnapshot::handleCreateSnapshot \n\tError "+
								"Commiting Slice Index: %v Slice: %v. Skipped. Error %v", idxInstId,
								slice.Id(), err)
							continue
						}

						common.Debugf("StorageMgr::handleCreateSnapshot \n\tCreating New Snapshot "+
							"Index: %v PartitionId: %v SliceId: %v", idxInstId, partnId, slice.Id())

						//create snapshot for slice
						if newSnapshot, err := slice.Snapshot(); err == nil {

							if snapContainer.Len() > MAX_SNAPSHOTS_PER_INDEX {
								serr := snapContainer.RemoveOldest()
								if serr != nil {
									common.Debugf("StorageMgr::handleCreateSnapshot \n\tRemoved Oldest Snapshot, "+
										"Container Len %v", snapContainer.Len())
								}
							}
							newSnapshot.Open()
							snapContainer.Add(newSnapshot)
							common.Debugf("StorageMgr::handleCreateSnapshot \n\tAdded New Snapshot Index: %v "+
								"PartitionId: %v SliceId: %v", idxInstId, partnId, slice.Id())

						} else {
							common.Errorf("StorageMgr::handleCreateSnapshot \n\tError Creating Snapshot "+
								"for Index: %v Slice: %v. Skipped. Error %v", idxInstId,
								slice.Id(), err)
						}
					} else {
						common.Debugf("StorageMgr::handleCreateSnapshot \n\tSkipped Creating New Snapshot for Index %v "+
							"PartitionId %v SliceId %v. No New Mutations.", idxInstId, partnId, slice.Id())
					}
				}
			}
		}

		// Update index-timestamp map whenever a snapshot is created for an index
		// Also notify any waiters for snapshots creation
		s.tsMap[idxInstId] = tsVbuuid
		var newWaiters []*snapshotWaiter
		for _, w := range s.waitersMap[idxInstId] {
			if w.ts == nil || tsVbuuid.AsRecent(w.ts) {
				w.Notify(tsVbuuid)
			} else {
				newWaiters = append(newWaiters, w)
			}
		}

		s.waitersMap[idxInstId] = newWaiters
	}

	s.supvCmdch <- &MsgSuccess{}

}

//handleRollback will rollback to given timestamp
func (sm *storageMgr) handleRollback(cmd Message) {

	streamId := cmd.(*MsgRollback).GetStreamId()
	rollbackTs := cmd.(*MsgRollback).GetRollbackTs()

	respTs := make(map[string]*common.TsVbuuid)

	//for every index managed by this indexer
	for idxInstId, partnMap := range sm.indexPartnMap {
		idxInst := sm.indexInstMap[idxInstId]

		//if this bucket needs to be rolled back
		if ts, ok := rollbackTs[idxInst.Defn.Bucket]; ok {

			//for all partitions managed by this indexer
			for partnId, partnInst := range partnMap {
				sc := partnInst.Sc

				//rollback all slices
				for _, slice := range sc.GetAllSlices() {
					s := slice.GetSnapshotContainer()
					snap := s.GetSnapshotOlderThanTS(ts)
					if snap != nil {
						err := slice.Rollback(snap)
						if err == nil {
							//discard all the snapshots recent than the TS rolled back to
							s.RemoveRecentThanTS(snap.Timestamp())
							common.Debugf("StorageMgr::handleRollback \n\t Rollback Index: %v "+
								"PartitionId: %v SliceId: %v To Snapshot %v ", idxInstId, partnId,
								slice.Id(), snap)
							respTs[idxInst.Defn.Bucket] = snap.Timestamp()
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
							//discard all snapshots in container
							s.RemoveAll()
							common.Debugf("StorageMgr::handleRollback \n\t Rollback Index: %v "+
								"PartitionId: %v SliceId: %v To Zero ", idxInstId, partnId,
								slice.Id())
							respTs[idxInst.Defn.Bucket] = common.NewTsVbuuid(idxInst.Defn.Bucket, int(NUM_VBUCKETS))
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
	sm.supvCmdch <- &MsgRollback{streamId: streamId,
		rollbackTs: respTs}
}

func (s *storageMgr) handleUpdateIndexInstMap(cmd Message) {

	common.Infof("StorageMgr::handleUpdateIndexInstMap %v", cmd)
	indexInstMap := cmd.(*MsgUpdateInstMap).GetIndexInstMap()
	s.indexInstMap = common.CopyIndexInstMap(indexInstMap)

	// Remove all snapshot waiters for indexes that do not exist anymore
	for id, ws := range s.waitersMap {
		if _, ok := s.indexInstMap[id]; !ok {
			for _, w := range ws {
				w.Error(ErrIndexNotFound)
			}
			delete(s.waitersMap, id)
		}
	}

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
		common.Errorf("StorageMgr::handleUpdateIndexInstMap \n\t Error Marshalling "+
			"IndexInstMap %v. Err %v", instMap, err)
	}

	if err = s.meta.SetKV([]byte(INST_MAP_KEY_NAME), instBytes.Bytes()); err != nil {
		common.Errorf("StorageMgr::handleUpdateIndexInstMap \n\tError "+
			"Storing IndexInstMap %v", err)
	}

	s.dbfile.Commit(forestdb.COMMIT_MANUAL_WAL_FLUSH)

	s.supvCmdch <- &MsgSuccess{}
}

func (s *storageMgr) handleUpdateIndexPartnMap(cmd Message) {

	common.Infof("StorageMgr::handleUpdateIndexPartnMap %v", cmd)
	indexPartnMap := cmd.(*MsgUpdatePartnMap).GetIndexPartnMap()
	s.indexPartnMap = CopyIndexPartnMap(indexPartnMap)

	s.supvCmdch <- &MsgSuccess{}
}

// Process req for providing a timestamp for index scan.
// The request contains atleast-timestamp and the storage
// manager will return consistent timestamp soon after
// a snapshot meeting requested criteria is available.
// The requester will block wait until the response is
// available.
func (s *storageMgr) handleGetTSForIndex(cmd Message) {
	s.supvCmdch <- &MsgSuccess{}

	req := cmd.(*MsgTSRequest)
	_, found := s.indexInstMap[req.GetIndexId()]
	if !found {
		req.respch <- ErrIndexNotFound
		return
	}

	// Return timestamp immediately if a matching snapshot exists already
	// Otherwise add into waiters list so that next snapshot creation event
	// can notify the requester with matching timestamp.
	ts := s.tsMap[req.GetIndexId()]
	// - If atleast-ts is nil and no timestamp is available, send nil ts
	// - If atleast-ts is not-nil and no timestamp is available, wait until
	// it is available.
	if req.GetTS() == nil || ts.AsRecent(req.GetTS()) {
		req.respch <- ts
	} else {
		w := newSnapshotWaiter(req.GetIndexId(), req.GetTS(), req.GetReplyChannel())
		ws, exists := s.waitersMap[req.GetIndexId()]
		if exists {
			s.waitersMap[req.idxInstId] = append(ws, w)
		} else {
			s.waitersMap[req.idxInstId] = []*snapshotWaiter{w}
		}
	}
}

//Init the TS Map from already available snapshot information.
//This function is used in indexer restart to recover on disk indexes.
func (s *storageMgr) initTsMap(indexPartnMap IndexPartnMap) {

	for idxInstId, partnMap := range indexPartnMap {

		//there is only one partition for now
		partnInst := partnMap[0]
		sc := partnInst.Sc

		//there is only one slice for now
		slice := sc.GetSliceById(0)

		s.tsMap[idxInstId] = slice.Timestamp()
	}
}
