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
	"log"
)

//Timekeeper manages the Stability Timestamp Generation and also
//keeps track of the HWTimestamp for each bucket
type Timekeeper interface {
}

type BucketHWTMap map[string]Timestamp
type BucketSyncCountMap map[string]uint64
type BucketNewTSReqdMap map[string]bool

const SYNC_COUNT_TS_TRIGGER = 20

type timekeeper struct {
	supvCmdch  MsgChannel //supervisor sends commands on this channel
	supvRespch MsgChannel //channel to send any async message to supervisor

	streamBucketHWTMap       map[StreamId]*BucketHWTMap
	streamBucketSyncCountMap map[StreamId]*BucketSyncCountMap
	streamBucketNewTSReqdMap map[StreamId]*BucketNewTSReqdMap
}

//NewTimekeeper returns an instance of timekeeper or err message.
//It listens on supvCmdch for command and every command is followed
//by a synchronous response of the supvCmdch.
//Any async response to supervisor is sent to supvRespch.
//If supvCmdch get closed, storageMgr will shut itself down.
func NewTimekeeper(supvCmdch MsgChannel, supvRespch MsgChannel) (
	Timekeeper, Message) {

	//Init the timekeeper struct
	tk := &timekeeper{
		supvCmdch:                supvCmdch,
		supvRespch:               supvRespch,
		streamBucketHWTMap:       make(map[StreamId]*BucketHWTMap),
		streamBucketSyncCountMap: make(map[StreamId]*BucketSyncCountMap),
		streamBucketNewTSReqdMap: make(map[StreamId]*BucketNewTSReqdMap),
	}

	//start timekeeper loop which listens to commands from its supervisor
	go tk.run()

	return tk, nil

}

//run starts the timekeeper loop which listens to messages
//from it supervisor(indexer)
func (tk *timekeeper) run() {

	//main timekeeper loop
loop:
	for {
		select {

		case cmd, ok := <-tk.supvCmdch:
			if ok {
				if cmd.GetMsgType() == TK_SHUTDOWN {
					break loop
				}
				tk.handleSupvervisorCommands(cmd)
			} else {
				//supervisor channel closed. exit
				break loop
			}

		}
	}
}

func (tk *timekeeper) handleSupvervisorCommands(cmd Message) {

	switch cmd.GetMsgType() {

	case STREAM_READER_SYNC:
		tk.handleSync(cmd)

	case TK_STREAM_START:
		tk.handleStreamStart(cmd)

	case TK_STREAM_SHOP:
		tk.handleStreamStop(cmd)

	}

}

func (tk *timekeeper) handleSync(cmd Message) {

	streamId := cmd.(*MsgStream).GetStreamId()
	meta := cmd.(*MsgStream).GetMutationMeta()

	var bucketHWTMap *BucketHWTMap
	var ok bool

	if bucketHWTMap, ok = tk.streamBucketHWTMap[streamId]; !ok {
		log.Println("Timekeeper: Got STREAM_READER_SYNC for unknown stream", streamId)
		tk.supvCmdch <- &MsgError{mType: ERROR,
			err: Error{code: ERROR_TK_UNKNOWN_STREAM,
				severity: FATAL,
				category: TIMEKEEPER}}
		return
	}

	bucketSyncCountMap := tk.streamBucketSyncCountMap[streamId]
	bucketNewTSReqd := tk.streamBucketNewTSReqdMap[streamId]

	//update HWT for this bucket
	var ts Timestamp
	if ts, ok := (*bucketHWTMap)[meta.bucket]; ok {
		//if seqno has incremented, update it
		if meta.seqno > ts[meta.vbucket] {
			(*bucketNewTSReqd)[meta.bucket] = true
			ts[meta.vbucket] = meta.seqno
		}
	} else {
		//allocate a new timestamp for this bucket
		(*bucketHWTMap)[meta.bucket] = NewTimestamp()
		(*bucketNewTSReqd)[meta.bucket] = false
	}

	//update sync count for this bucket
	if syncCount, ok := (*bucketSyncCountMap)[meta.bucket]; ok {
		syncCount++
		if syncCount > SYNC_COUNT_TS_TRIGGER &&
			(*bucketNewTSReqd)[meta.bucket] == true {
			//generate new stability timestamp
			go tk.generateNewStabilityTS(ts, meta.bucket, streamId)
			(*bucketSyncCountMap)[meta.bucket] = 0
		} else {
			(*bucketSyncCountMap)[meta.bucket] = syncCount
		}
	} else {
		//add a new counter for this bucket
		(*bucketSyncCountMap)[meta.bucket] = 1
	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleStreamStart(cmd Message) {

	streamId := cmd.(*MsgTKStreamUpdate).GetStreamId()

	bucketHWTMap := make(BucketHWTMap)
	tk.streamBucketHWTMap[streamId] = &bucketHWTMap

	bucketSyncCountMap := make(BucketSyncCountMap)
	tk.streamBucketSyncCountMap[streamId] = &bucketSyncCountMap

	bucketNewTSReqdMap := make(BucketNewTSReqdMap)
	tk.streamBucketNewTSReqdMap[streamId] = &bucketNewTSReqdMap

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleStreamStop(cmd Message) {

	streamId := cmd.(*MsgTKStreamUpdate).GetStreamId()

	delete(tk.streamBucketHWTMap, streamId)
	delete(tk.streamBucketSyncCountMap, streamId)
	delete(tk.streamBucketNewTSReqdMap, streamId)

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) generateNewStabilityTS(ts Timestamp, bucket string,
	streamId StreamId) {

	tk.supvRespch <- &MsgTKStabilityTS{ts: ts,
		bucket:   bucket,
		streamId: streamId}
}
