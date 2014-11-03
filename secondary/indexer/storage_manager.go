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
	"github.com/couchbase/indexing/secondary/common"
)

//StorageManager manages the snapshots for the indexes and responsible for storing
//indexer metadata in a config database
//TODO - Add config database storage

type StorageManager interface {
}

type storageMgr struct {
	supvCmdch  MsgChannel //supervisor sends commands on this channel
	supvRespch MsgChannel //channel to send any async message to supervisor

	indexInstMap  common.IndexInstMap
	indexPartnMap IndexPartnMap
}

//NewStorageManager returns an instance of storageMgr or err message
//It listens on supvCmdch for command and every command is followed
//by a synchronous response of the supvCmdch.
//Any async response to supervisor is sent to supvRespch.
//If supvCmdch get closed, storageMgr will shut itself down.
func NewStorageManager(supvCmdch MsgChannel, supvRespch MsgChannel) (
	StorageManager, Message) {

	//Init the storageMgr struct
	s := &storageMgr{
		supvCmdch:  supvCmdch,
		supvRespch: supvRespch,
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

						if err := slice.Commit(); err != nil {

							common.Errorf("handleCreateSnapshot::handleCreateSnapshot \n\tError "+
								"Commiting Slice Index: %v Slice: %v. Skipped. Error %v", idxInstId,
								slice.Id(), err)
							continue
						}

						common.Debugf("StorageMgr::handleCreateSnapshot \n\tCreating New Snapshot "+
							"Index: %v PartitionId: %v SliceId: %v", idxInstId, partnId, slice.Id())

						newTsVbuuid := tsVbuuid.Copy()
						slice.SetTimestamp(newTsVbuuid)

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

	s.supvCmdch <- &MsgSuccess{}
}

func (s *storageMgr) handleUpdateIndexPartnMap(cmd Message) {

	common.Infof("StorageMgr::handleUpdateIndexPartnMap %v", cmd)
	indexPartnMap := cmd.(*MsgUpdatePartnMap).GetIndexPartnMap()
	s.indexPartnMap = CopyIndexPartnMap(indexPartnMap)

	s.supvCmdch <- &MsgSuccess{}
}
