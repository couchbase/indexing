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
	"log"
)

//StorageManager manages the snapshots for the indexes and responsible for storing
//indexer metadata in a config database
//TODO - Add config database storage

type StorageManager interface {
}

const MAX_SNAPSHOTS_PER_INDEX = 100

type storageMgr struct {
	supvCmdch  MsgChannel //supervisor sends commands on this channel
	supvRespch MsgChannel //channel to send any async message to supervisor

	indexInstMap  common.IndexInstMap
	indexPartnMap common.IndexPartnMap
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

	return s, nil

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

	case UPDATE_INDEX_INSTANCE_MAP:
		s.handleUpdateIndexInstMap(cmd)

	case UPDATE_INDEX_PARTITION_MAP:
		s.handleUpdateIndexPartnMap(cmd)

	}

}

//handleCreateSnapshot will create the necessary snapshots
//after flush has completed
func (s *storageMgr) handleCreateSnapshot(cmd Message) {

	streamId := cmd.(*MsgMutMgrFlushDone).GetStreamId()
	bucket := cmd.(*MsgMutMgrFlushDone).GetBucket()
	ts := cmd.(*MsgMutMgrFlushDone).GetTS()

	//for every index managed by this indexer
	for idxInstId, partnMap := range s.indexPartnMap {
		idxInst := s.indexInstMap[idxInstId]

		//if index belongs to the flushed bucket
		if idxInst.Defn.Bucket == bucket {

			//for all partitions managed by this indexer
			for _, partnInst := range partnMap {
				sc := partnInst.Sc

				//create snapshot for all the slices
				for _, slice := range sc.GetAllSlices() {

					//if flush timestamp is greater than last
					//snapshot timestamp, create a new snapshot

					snapContainer := slice.GetSnapshotContainer()

					latestSnapshot := snapContainer.GetLatestSnapshot()

					//if the flush TS is greater than the last snapshot TS
					//TODO Is it better to have a IsDirty() in Slice interface
					//rather than comparing the last snapshot?
					if ts.GreaterThan(latestSnapshot.Timestamp()) {
						//commit the outstanding data

						if err := slice.Commit(); err != nil {

							log.Printf("handleCreateSnapshot: Error Commiting Slice "+
								"for index %v slice %v. Skipped. Error %v", idxInstId,
								slice.Id(), err)
							continue
						}

						//create snapshot for slice
						if newSnapshot, err := slice.Snapshot(); err == nil {

							if snapContainer.Len() > MAX_SNAPSHOTS_PER_INDEX {
								snapContainer.RemoveOldest()
							}
							newSnapshot.SetTimestamp(ts)
							snapContainer.Add(newSnapshot)

						} else {
							log.Printf("handleCreateSnapshot: Error Creating Snapshot "+
								"for index %v slice %v. Skipped. Error %v", idxInstId,
								slice.Id(), err)
						}
					}
				}
			}
		}
	}

	s.supvCmdch <- &MsgSuccess{}

}

func (s *storageMgr) handleUpdateIndexInstMap(cmd Message) {

	s.indexInstMap = cmd.(*MsgUpdateInstMap).GetIndexInstMap()

}

func (s *storageMgr) handleUpdateIndexPartnMap(cmd Message) {

	s.indexPartnMap = cmd.(*MsgUpdatePartnMap).GetIndexPartnMap()

}
