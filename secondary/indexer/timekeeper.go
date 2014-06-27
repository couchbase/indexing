// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

//TODO Does timekeeper need to take into account all the indexes for a bucket getting dropped?
//Right now it assumes for such a case there will be no SYNC message for that bucket, but doesn't
//clean up its internal maps

package indexer

import (
	"container/list"
	"log"
)

//Timekeeper manages the Stability Timestamp Generation and also
//keeps track of the HWTimestamp for each bucket
type Timekeeper interface {
}

type BucketHWTMap map[string]Timestamp
type BucketSyncCountMap map[string]uint64
type BucketNewTSReqdMap map[string]bool

type BucketTSListMap map[string]*list.List
type BucketFlushInProgressMap map[string]bool

const SYNC_COUNT_TS_TRIGGER = 100

type timekeeper struct {
	supvCmdch  MsgChannel //supervisor sends commands on this channel
	supvRespch MsgChannel //channel to send any async message to supervisor

	streamBucketHWTMap       map[StreamId]*BucketHWTMap
	streamBucketSyncCountMap map[StreamId]*BucketSyncCountMap
	streamBucketNewTSReqdMap map[StreamId]*BucketNewTSReqdMap

	streamBucketTSListMap          map[StreamId]*BucketTSListMap
	streamBucketFlushInProgressMap map[StreamId]*BucketFlushInProgressMap
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
		supvCmdch:                      supvCmdch,
		supvRespch:                     supvRespch,
		streamBucketHWTMap:             make(map[StreamId]*BucketHWTMap),
		streamBucketSyncCountMap:       make(map[StreamId]*BucketSyncCountMap),
		streamBucketNewTSReqdMap:       make(map[StreamId]*BucketNewTSReqdMap),
		streamBucketTSListMap:          make(map[StreamId]*BucketTSListMap),
		streamBucketFlushInProgressMap: make(map[StreamId]*BucketFlushInProgressMap),
	}

	//start timekeeper loop which listens to commands from its supervisor
	go tk.run()

	return tk, &MsgSuccess{}

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

	case TK_STREAM_STOP:
		tk.handleStreamStop(cmd)

	case MUT_MGR_FLUSH_DONE:
		tk.handleFlushDone(cmd)

	default:
		log.Printf("Timekeeper: Received Unknown Command %v", cmd)

	}

}

func (tk *timekeeper) handleSync(cmd Message) {

	log.Printf("Timekeeper: Received Stream Reader Sync %v", cmd)

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
	bucketFlushInProgressMap := tk.streamBucketFlushInProgressMap[streamId]
	bucketTSListMap := tk.streamBucketTSListMap[streamId]

	//update HWT for this bucket
	var ts Timestamp
	if ts, ok = (*bucketHWTMap)[meta.bucket]; ok {
		//if seqno has incremented, update it
		if meta.seqno > ts[meta.vbucket] {
			(*bucketNewTSReqd)[meta.bucket] = true
			ts[meta.vbucket] = meta.seqno
		}
	} else {
		//allocate a new timestamp for this bucket
		(*bucketHWTMap)[meta.bucket] = NewTimestamp()
		(*bucketNewTSReqd)[meta.bucket] = false
		(*bucketTSListMap)[meta.bucket] = list.New()
		(*bucketFlushInProgressMap)[meta.bucket] = false

	}

	//update sync count for this bucket
	if syncCount, ok := (*bucketSyncCountMap)[meta.bucket]; ok {
		syncCount++
		if syncCount >= SYNC_COUNT_TS_TRIGGER &&
			(*bucketNewTSReqd)[meta.bucket] == true {
			//generate new stability timestamp
			log.Printf("Timekeeper: Generating new Stability TS %v for Bucket %v "+
				"Stream %v. SyncCount is %v", ts, meta.bucket, streamId, syncCount)

			tsList := (*bucketTSListMap)[meta.bucket]

			//if there is no flush already in progress for this bucket and no pending TS in list,
			//send new TS
			if (*bucketFlushInProgressMap)[meta.bucket] == false && tsList.Len() == 0 {
				(*bucketFlushInProgressMap)[meta.bucket] = true
				go tk.sendNewStabilityTS(ts, meta.bucket, streamId)
			} else {
				//store the ts in list
				tsList.PushBack(ts)
			}
			(*bucketSyncCountMap)[meta.bucket] = 0
			(*bucketNewTSReqd)[meta.bucket] = false
		} else {
			log.Printf("Timekeeper: Updating Sync Count for Bucket %v "+
				"Stream %v. SyncCount %v.", meta.bucket, streamId, syncCount)
			//update only if its less than trigger count, otherwise it makes no
			//difference. On long running systems, syncCount may overflow otherwise
			if syncCount < SYNC_COUNT_TS_TRIGGER {
				(*bucketSyncCountMap)[meta.bucket] = syncCount
			}
		}
	} else {
		//add a new counter for this bucket
		log.Printf("Timekeeper: Adding new Sync Count for Bucket %v "+
			"Stream %v. SyncCount %v.", meta.bucket, streamId, syncCount)
		(*bucketSyncCountMap)[meta.bucket] = 1
	}

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleStreamStart(cmd Message) {

	log.Printf("Timekeeper: Received Stream Start %v", cmd)

	streamId := cmd.(*MsgTKStreamUpdate).GetStreamId()

	bucketHWTMap := make(BucketHWTMap)
	tk.streamBucketHWTMap[streamId] = &bucketHWTMap

	bucketSyncCountMap := make(BucketSyncCountMap)
	tk.streamBucketSyncCountMap[streamId] = &bucketSyncCountMap

	bucketNewTSReqdMap := make(BucketNewTSReqdMap)
	tk.streamBucketNewTSReqdMap[streamId] = &bucketNewTSReqdMap

	bucketTSListMap := make(BucketTSListMap)
	tk.streamBucketTSListMap[streamId] = &bucketTSListMap

	bucketFlushInProgressMap := make(BucketFlushInProgressMap)
	tk.streamBucketFlushInProgressMap[streamId] = &bucketFlushInProgressMap

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleStreamStop(cmd Message) {

	log.Printf("Timekeeper: Received Stream Stop %v", cmd)

	streamId := cmd.(*MsgTKStreamUpdate).GetStreamId()

	delete(tk.streamBucketHWTMap, streamId)
	delete(tk.streamBucketSyncCountMap, streamId)
	delete(tk.streamBucketNewTSReqdMap, streamId)
	delete(tk.streamBucketTSListMap, streamId)
	delete(tk.streamBucketFlushInProgressMap, streamId)

	tk.supvCmdch <- &MsgSuccess{}
}

func (tk *timekeeper) handleFlushDone(cmd Message) {

	log.Printf("Timekeeper: Received Flush Done %v", cmd)

	streamId := cmd.(*MsgMutMgrFlushDone).GetStreamId()
	bucket := cmd.(*MsgMutMgrFlushDone).GetBucket()

	//update internal map to reflect flush is done
	bucketFlushInProgressMap := tk.streamBucketFlushInProgressMap[streamId]
	(*bucketFlushInProgressMap)[bucket] = false

	//if there are pending TS for this bucket, send New TS
	bucketTSListMap := tk.streamBucketTSListMap[streamId]
	tsList := (*bucketTSListMap)[bucket]
	if tsList.Len() > 0 {
		e := tsList.Front()
		ts := e.Value.(Timestamp)
		(*bucketFlushInProgressMap)[bucket] = true
		go tk.sendNewStabilityTS(ts, bucket, streamId)
	}

	tk.supvCmdch <- &MsgSuccess{}

}

func (tk *timekeeper) sendNewStabilityTS(ts Timestamp, bucket string,
	streamId StreamId) {

	tk.supvRespch <- &MsgTKStabilityTS{ts: ts,
		bucket:   bucket,
		streamId: streamId}
}
