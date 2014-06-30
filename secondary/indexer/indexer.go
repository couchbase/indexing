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

type IndexerState int16
type IndexerId uint64

const (
	INIT IndexerState = iota
	ACTIVE
	RECOVERY
)

type Indexer interface {
	Shutdown() Message
}

type StreamStatus map[StreamId]bool

//TODO move this to config
var NUM_VBUCKETS uint16
var PROJECTOR_ADMIN_PORT_ENDPOINT string
var StreamAddrMap StreamAddressMap

type indexer struct {
	id    IndexerId
	state IndexerState //state of the indexer

	indexInstMap  common.IndexInstMap //map of indexInstId to IndexInst
	indexPartnMap IndexPartnMap       //map of indexInstId to PartitionInst

	streamStatus StreamStatus //map of streamId to status

	wrkrRecvCh         MsgChannel //channel to receive messages from workers
	internalRecvCh     MsgChannel //buffered channel to queue worker requests
	adminRecvCh        MsgChannel //channel to receive admin messages
	shutdownInitCh     MsgChannel //internal shutdown channel for indexer
	shutdownCompleteCh MsgChannel //indicate shutdown completion

	mutMgrCmdCh         MsgChannel //channel to send commands to mutation manager
	storageMgrCmdCh     MsgChannel //channel to send commands to storage manager
	tkCmdCh             MsgChannel //channel to send commands to timekeeper
	adminMgrCmdCh       MsgChannel //channel to send commands to admin port manager
	clustMgrSenderCmdCh MsgChannel //channel to send messages to index coordinator
	kvSenderCmdCh       MsgChannel //channel to send messages to kv sender
	cbqBridgeCmdCh      MsgChannel //channel to send message to cbq sender

	mutMgrExitCh MsgChannel //channel to indicate mutation manager exited

	tk             Timekeeper      //handle to timekeeper
	storageMgr     StorageManager  //handle to storage manager
	mutMgr         MutationManager //handle to mutation manager
	adminMgr       AdminManager    //handle to admin port manager
	clustMgrSender ClustMgrSender  //handle to ClustMgrSender
	kvSender       KVSender        //handle to KVSender
	cbqBridge      CbqBridge       //handle to CbqBridge

}

func NewIndexer(numVbuckets uint16) (Indexer, Message) {

	idx := &indexer{
		wrkrRecvCh:         make(MsgChannel),
		internalRecvCh:     make(MsgChannel, WORKER_MSG_QUEUE_LEN),
		adminRecvCh:        make(MsgChannel),
		shutdownInitCh:     make(MsgChannel),
		shutdownCompleteCh: make(MsgChannel),

		mutMgrCmdCh:         make(MsgChannel),
		storageMgrCmdCh:     make(MsgChannel),
		tkCmdCh:             make(MsgChannel),
		adminMgrCmdCh:       make(MsgChannel),
		clustMgrSenderCmdCh: make(MsgChannel),
		kvSenderCmdCh:       make(MsgChannel),
		cbqBridgeCmdCh:      make(MsgChannel),

		mutMgrExitCh: make(MsgChannel),

		indexInstMap:  make(common.IndexInstMap),
		indexPartnMap: make(IndexPartnMap),
	}

	idx.state = INIT
	log.Printf("Indexer: Status INIT")

	//assume indexerId 1 for now
	idx.id = 1

	if numVbuckets > 0 {
		NUM_VBUCKETS = numVbuckets
	} else {
		NUM_VBUCKETS = MAX_NUM_VBUCKETS
	}

	log.Printf("Indexer: Starting with Vbuckets %v", NUM_VBUCKETS)

	//init the stream address map
	StreamAddrMap = make(StreamAddressMap)
	//TODO move this to config
	StreamAddrMap[MAINT_STREAM] = common.Endpoint(INDEXER_DATA_PORT_ENDPOINT)

	//init stream status
	idx.streamStatus = make(StreamStatus)
	idx.streamStatus[MAINT_STREAM] = false

	var res Message
	idx.clustMgrSender, res = NewClustMgrSender(idx.clustMgrSenderCmdCh, idx.wrkrRecvCh)
	if res.GetMsgType() != SUCCESS {
		log.Println("Indexer: ClusterMgrSender Init Error", res)
		return nil, res
	}

	//read persisted indexer state
	if err := idx.initFromPersistedState(); err != nil {
		//log error and exit
	}

	//Register with Index Coordinator
	if err := idx.registerWithCoordinator(); err != nil {
		//log error and exit
	}

	//sync topology
	if err := idx.syncTopologyWithCoordinator(); err != nil {
		//log error and exit
	}

	//Start Storage Manager
	idx.storageMgr, res = NewStorageManager(idx.storageMgrCmdCh, idx.wrkrRecvCh)
	if res.GetMsgType() != SUCCESS {
		log.Println("Indexer: Storage Manager Init Error", res)
		return nil, res
	}

	//Recover Persisted Snapshots
	idx.recoverPersistedSnapshots()

	//Start Timekeeper
	idx.tk, res = NewTimekeeper(idx.tkCmdCh, idx.wrkrRecvCh)
	if res.GetMsgType() != SUCCESS {
		log.Println("Indexer: Timekeeper Init Error", res)
		return nil, res
	}

	//Start KV Sender
	idx.kvSender, res = NewKVSender(idx.kvSenderCmdCh, idx.wrkrRecvCh, numVbuckets)
	if res.GetMsgType() != SUCCESS {
		log.Println("Indexer: KVSender Init Error", res)
		return nil, res
	}

	//Start Admin port listener
	idx.adminMgr, res = NewAdminManager(idx.adminMgrCmdCh, idx.adminRecvCh)
	if res.GetMsgType() != SUCCESS {
		log.Println("Indexer: Admin Manager Init Error", res)
		return nil, res
	}

	//Start Mutation Manager
	idx.mutMgr, res = NewMutationManager(idx.mutMgrCmdCh, idx.wrkrRecvCh,
		numVbuckets)
	if res.GetMsgType() != SUCCESS {
		log.Println("Indexer: Mutation Manager Init Error", res)
		return nil, res
	}

	//Start CbqBridge
	idx.mutMgr, res = NewCbqBridge(idx.cbqBridgeCmdCh, idx.adminRecvCh)
	if res.GetMsgType() != SUCCESS {
		log.Println("Indexer: CbqBridge Init Error", res)
		return nil, res
	}

	idx.state = ACTIVE
	log.Println("Indexer: Status ACTIVE")

	//start the main indexer loop
	idx.run()

	return idx, &MsgSuccess{}

}

func (idx *indexer) registerWithCoordinator() error {

	//get the IndexerId from persistence and send it to Index Coordinator

	//if there is no IndexerId, send an empty one. Coordinator will assign
	//a new IndexerId in that case and treat this as a fresh node.
	return nil

}

func (idx *indexer) initFromPersistedState() error {

	//read indexer state and local state context
	return nil

}

func (idx *indexer) syncTopologyWithCoordinator() error {

	//get the latest topology from coordinator
	return nil
}

func (idx *indexer) recoverPersistedSnapshots() error {

	//recover persisted snapshots from disk
	return nil

}

//run starts the main loop for the indexer
func (idx *indexer) run() {

	go idx.listenWorkerMsgs()

	for {

		select {

		case msg, ok := <-idx.internalRecvCh:
			if ok {
				idx.handleWorkerMsgs(msg)
			}

		case msg, ok := <-idx.adminRecvCh:
			if ok {
				idx.handleAdminMsgs(msg)
			}

		case <-idx.shutdownInitCh:
			//send shutdown to all workers

			idx.shutdownWorkers()
			//close the shutdown complete channel to indicate
			//all workers are shutdown
			close(idx.shutdownCompleteCh)
			return

		}

	}

}

func (idx *indexer) listenWorkerMsgs() {

	//listen to worker messages
	for {

		select {

		case msg, ok := <-idx.wrkrRecvCh:
			if ok {
				//handle high priority messages
				switch msg.GetMsgType() {
				case ERROR:
					err := msg.(*MsgError).GetError()
					if err.code == ERROR_MUT_MGR_PANIC {
						close(idx.mutMgrExitCh)
					}
				}
				idx.internalRecvCh <- msg
			}

		case <-idx.shutdownInitCh:
			//exit the loop
			return
		}
	}

}

func (idx *indexer) handleWorkerMsgs(msg Message) {

	switch msg.GetMsgType() {

	case STREAM_READER_SYNC:
		//fwd the message to timekeeper
		idx.tkCmdCh <- msg
		<-idx.tkCmdCh

	case STREAM_READER_STREAM_BEGIN:
		//ignore for now

	case STREAM_READER_STREAM_END:
		//TODO

	case STREAM_READER_STREAM_DROP_DATA:
		//TODO

	case TK_STABILITY_TIMESTAMP:
		//send TS to Mutation Manager
		ts := msg.(*MsgTKStabilityTS).GetTimestamp()
		bucket := msg.(*MsgTKStabilityTS).GetBucket()
		streamId := msg.(*MsgTKStabilityTS).GetStreamId()

		idx.mutMgrCmdCh <- &MsgMutMgrFlushMutationQueue{
			mType:    MUT_MGR_PERSIST_MUTATION_QUEUE,
			bucket:   bucket,
			ts:       ts,
			streamId: streamId}

		<-idx.mutMgrCmdCh

	case MUT_MGR_FLUSH_DONE:

		log.Printf("Indexer: Received Flush Done From Mutation Mgr %v", msg)

		//fwd the message to storage manager
		idx.storageMgrCmdCh <- msg
		<-idx.storageMgrCmdCh

		//fwd the messate to timekeeper
		idx.tkCmdCh <- msg
		<-idx.tkCmdCh

	default:
		log.Printf("Indexer: Received Unknown Message from Worker %v", msg)
	}

}

func (idx *indexer) handleAdminMsgs(msg Message) {

	switch msg.GetMsgType() {

	case INDEXER_CREATE_INDEX_DDL:

		idx.handleCreateIndex(msg)

	case INDEXER_DROP_INDEX_DDL:

		idx.handleDropIndex(msg)

	default:

		log.Printf("Indexer: Received Unknown Admin Message %v", msg)

	}

}

//TODO handle panic, otherwise main loop will get shutdown
func (idx *indexer) handleCreateIndex(msg Message) {

	indexInst := msg.(*MsgCreateIndex).GetIndexInst()

	log.Printf("Indexer: Received CreateIndex for Index %v", indexInst)
	idx.indexInstMap[indexInst.InstId] = indexInst

	//initialize partitionInstMap for this index
	partnInstMap := make(PartitionInstMap)

	//get all partitions for this index
	partnDefnList := indexInst.Pc.GetAllPartitions()

	for i, partnDefn := range partnDefnList {
		partnInst := PartitionInst{Defn: partnDefn,
			Sc: NewHashedSliceContainer()}

		log.Printf("Indexer: Initialized Partition %v for Index %v", partnInst, indexInst.InstId)

		//add a single slice per partition for now
		if slice, err := NewForestDBSlice(indexInst.Defn.Name, 0,
			indexInst.Defn.DefnId, indexInst.InstId); err == nil {
			partnInst.Sc.AddSlice(0, slice)
			log.Printf("Indexer: Initialized Slice %v for Index %v", slice, indexInst.InstId)

			partnInstMap[common.PartitionId(i)] = partnInst
		} else {
			log.Printf("handleCreateIndex: Error creating slice %v. Abort.", err)
			return
		}
	}

	//init index partition map for this index
	idx.indexPartnMap[indexInst.InstId] = partnInstMap

	//update index map in storage manager
	msgUpdateIndexInstMap := &MsgUpdateInstMap{indexInstMap: idx.indexInstMap}

	idx.storageMgrCmdCh <- msgUpdateIndexInstMap
	if resp, ok := <-idx.storageMgrCmdCh; ok {

		if resp.GetMsgType() != SUCCESS {
			log.Printf("handleCreateIndex: Error received from Storage Manager"+
				"processing Msg %v Err %v. Aborted.", msgUpdateIndexInstMap, resp)
			return
		}
	} else {
		log.Printf("handleCreateIndex: Error communicating with Storage Manager"+
			"processing Msg %v Err %v. Aborted.", msgUpdateIndexInstMap, resp)
		return
	}

	msgUpdateIndexPartnMap := &MsgUpdatePartnMap{indexPartnMap: idx.indexPartnMap}

	idx.storageMgrCmdCh <- msgUpdateIndexPartnMap
	if resp, ok := <-idx.storageMgrCmdCh; ok {

		if resp.GetMsgType() != SUCCESS {
			log.Printf("handleCreateIndex: Error received from Storage Manager"+
				"processing Msg %v Err %v. Aborted.", msgUpdateIndexPartnMap, resp)
			return
		}
	} else {
		log.Printf("handleCreateIndex: Error communicating with Storage Manager"+
			"processing Msg %v Err %v. Aborted.", msgUpdateIndexPartnMap, resp)
		return
	}

	//send msg to timekeeper if stream is not started yet
	var tkCmd Message
	if status, _ := idx.streamStatus[MAINT_STREAM]; !status {
		var indexInstList []common.IndexInst
		indexInstList = append(indexInstList, indexInst)

		tkCmd = &MsgTKStreamUpdate{mType: TK_STREAM_START,
			streamId:      MAINT_STREAM,
			indexInstList: indexInstList}

		idx.tkCmdCh <- tkCmd
		if resp, ok := <-idx.tkCmdCh; ok {

			if resp.GetMsgType() != SUCCESS {
				log.Printf("handleCreateIndex: Error received from Timekeeper"+
					"processing Msg %v Err %v. Aborted.", tkCmd, resp)
				return
			}
		} else {
			log.Printf("handleCreateIndex: Error communicating with Timekeeper"+
				"processing Msg %v Err %v. Aborted.", tkCmd, resp)
			return
		}
	}

	//if this is first index, start the mutation stream
	var mutMgrCmd Message
	var indexList []common.IndexInst
	indexList = append(indexList, indexInst)

	if status, _ := idx.streamStatus[MAINT_STREAM]; !status {
		mutMgrCmd = &MsgMutMgrStreamUpdate{mType: MUT_MGR_OPEN_STREAM,
			streamId:  MAINT_STREAM,
			indexList: indexList}
	} else {
		mutMgrCmd = &MsgMutMgrStreamUpdate{mType: MUT_MGR_ADD_INDEX_LIST_TO_STREAM,
			streamId:  MAINT_STREAM,
			indexList: indexList}
	}

	//send message to mutation manager
	idx.mutMgrCmdCh <- mutMgrCmd
	if resp, ok := <-idx.mutMgrCmdCh; ok {

		if resp.GetMsgType() != SUCCESS {
			log.Printf("handleCreateIndex: Error received from Mutation Mgr "+
				"processing Msg %v Err %v. Aborted.", mutMgrCmd, resp)
			return
		}
	} else {
		log.Printf("handleCreateIndex: Error communicating with Mutation Mgr "+
			"processing Msg %v Err %v. Aborted.", mutMgrCmd, resp)
		return
	}

	//update index map in mutation manager

	idx.mutMgrCmdCh <- msgUpdateIndexInstMap
	if resp, ok := <-idx.mutMgrCmdCh; ok {

		if resp.GetMsgType() != SUCCESS {
			log.Printf("handleCreateIndex: Error received from Mutation Manager"+
				"processing Msg %v Err %v. Aborted.", msgUpdateIndexInstMap, resp)
			return
		}
	} else {
		log.Printf("handleCreateIndex: Error communicating with Mutation Manager"+
			"processing Msg %v Err %v. Aborted.", msgUpdateIndexInstMap, resp)
		return
	}

	idx.mutMgrCmdCh <- msgUpdateIndexPartnMap
	if resp, ok := <-idx.mutMgrCmdCh; ok {

		if resp.GetMsgType() != SUCCESS {
			log.Printf("handleCreateIndex: Error received from Mutation Manager"+
				"processing Msg %v Err %v. Aborted.", msgUpdateIndexPartnMap, resp)
			return
		}
	} else {
		log.Printf("handleCreateIndex: Error communicating with Mutation Manager"+
			"processing Msg %v Err %v. Aborted.", msgUpdateIndexPartnMap, resp)
		return
	}

	if status, _ := idx.streamStatus[MAINT_STREAM]; !status {
		idx.streamStatus[MAINT_STREAM] = true
	}

	//send the msg to KV sender

	idx.kvSenderCmdCh <- msg

	if resp, ok := <-idx.kvSenderCmdCh; ok {

		if resp.GetMsgType() != SUCCESS {
			log.Printf("handleCreateIndex: Error received from KV Sender"+
				"processing Msg %v Err %v. Aborted.", msg, resp)
			return
		}
	} else {
		log.Printf("handleCreateIndex: Error communicating with KV Sender"+
			"processing Msg %v Err %v. Aborted.", msg, resp)
		return
	}

}

//TODO handle panic, otherwise main loop will get shutdown
func (idx *indexer) handleDropIndex(msg Message) {

	indexInstId := msg.(*MsgDropIndex).GetIndexInstId()
	log.Printf("Indexer received DropIndex for Index %v", indexInstId)

	//send the msg to KV
	idx.kvSenderCmdCh <- msg

	if resp, ok := <-idx.kvSenderCmdCh; ok {

		if resp.GetMsgType() != SUCCESS {
			log.Printf("handleDropIndex: Error received from KV Sender"+
				"processing Msg %v Err %v.", msg, resp)
		}
	} else {
		log.Printf("handleDropIndex: Error communicating with KV Sender"+
			"processing Msg %v Err %v.", msg, resp)
	}

	//update internal maps
	delete(idx.indexInstMap, indexInstId)
	delete(idx.indexPartnMap, indexInstId)

	//update index map in storage manager
	msgUpdateIndexInstMap := &MsgUpdateInstMap{indexInstMap: idx.indexInstMap}

	idx.storageMgrCmdCh <- msgUpdateIndexInstMap
	if resp, ok := <-idx.storageMgrCmdCh; ok {

		if resp.GetMsgType() != SUCCESS {
			log.Printf("handleDropIndex: Error received from Storage Manager"+
				"processing Msg %v Err %v.", msgUpdateIndexInstMap, resp)
		}
	} else {
		log.Printf("handleDropIndex: Error communicating with Storage Manager"+
			"processing Msg %v Err %v.", msgUpdateIndexInstMap, resp)
	}

	msgUpdateIndexPartnMap := &MsgUpdatePartnMap{indexPartnMap: idx.indexPartnMap}

	idx.storageMgrCmdCh <- msgUpdateIndexPartnMap
	if resp, ok := <-idx.storageMgrCmdCh; ok {

		if resp.GetMsgType() != SUCCESS {
			log.Printf("handleDropIndex: Error received from Storage Manager"+
				"processing Msg %v Err %v.", msgUpdateIndexPartnMap, resp)
		}
	} else {
		log.Printf("handleDropIndex: Error communicating with Storage Manager"+
			"processing Msg %v Err %v.", msgUpdateIndexPartnMap, resp)
	}

	//send msg to timekeeper to stop stream if this was the last index
	var tkCmd Message
	if len(idx.indexInstMap) == 0 {
		tkCmd = &MsgTKStreamUpdate{mType: TK_STREAM_STOP,
			streamId: MAINT_STREAM}

		idx.tkCmdCh <- tkCmd
		if resp, ok := <-idx.tkCmdCh; ok {

			if resp.GetMsgType() != SUCCESS {
				log.Printf("handleDropIndex: Error received from Timekeeper"+
					"processing Msg %v Err %v.", tkCmd, resp)
			}
		} else {
			log.Printf("handleDropIndex: Error communicating with Timekeeper"+
				"processing Msg %v Err %v.", tkCmd, resp)
		}
	}

	//if this is the last index, stop the mutation stream
	var mutMgrCmd Message
	if len(idx.indexInstMap) == 0 {
		mutMgrCmd = &MsgMutMgrStreamUpdate{mType: MUT_MGR_CLOSE_STREAM,
			streamId: MAINT_STREAM}
	} else {
		mutMgrCmd = &MsgMutMgrStreamUpdate{mType: MUT_MGR_REMOVE_INDEX_LIST_FROM_STREAM,
			streamId: MAINT_STREAM}
	}

	//send message to mutation manager
	idx.mutMgrCmdCh <- mutMgrCmd
	if resp, ok := <-idx.mutMgrCmdCh; ok {

		if resp.GetMsgType() != SUCCESS {
			log.Printf("handleDropIndex: Error received from Mutation Mgr "+
				"processing Msg %v Err %v.", mutMgrCmd, resp)
		}
	} else {
		log.Printf("handleDropIndex: Error communicating with Mutation Mgr "+
			"processing Msg %v Err %v.", mutMgrCmd, resp)
	}

	//update index map in mutation manager

	idx.mutMgrCmdCh <- msgUpdateIndexInstMap
	if resp, ok := <-idx.mutMgrCmdCh; ok {

		if resp.GetMsgType() != SUCCESS {
			log.Printf("handleDropIndex: Error received from Mutation Manager"+
				"processing Msg %v Err %v.", msgUpdateIndexInstMap, resp)
		}
	} else {
		log.Printf("handleDropIndex: Error communicating with Mutation Manager"+
			"processing Msg %v Err %v.", msgUpdateIndexInstMap, resp)
	}

	idx.mutMgrCmdCh <- msgUpdateIndexPartnMap
	if resp, ok := <-idx.mutMgrCmdCh; ok {

		if resp.GetMsgType() != SUCCESS {
			log.Printf("handleDropIndex: Error received from Mutation Manager"+
				"processing Msg %v Err %v.", msgUpdateIndexPartnMap, resp)
		}
	} else {
		log.Printf("handleDropIndex: Error communicating with Mutation Manager"+
			"processing Msg %v Err %v.", msgUpdateIndexPartnMap, resp)
	}

	if len(idx.indexInstMap) == 0 {
		idx.streamStatus[MAINT_STREAM] = false
	}

}

func (idx *indexer) shutdownWorkers() {

	//shutdown mutation manager
	idx.mutMgrCmdCh <- &MsgGeneral{mType: MUT_MGR_SHUTDOWN}
	<-idx.mutMgrCmdCh

	//shutdown storage manager
	idx.storageMgrCmdCh <- &MsgGeneral{mType: STORAGE_MGR_SHUTDOWN}
	<-idx.storageMgrCmdCh

	//shutdown timekeeper
	idx.tkCmdCh <- &MsgGeneral{mType: TK_SHUTDOWN}
	<-idx.tkCmdCh

	//shutdown admin manager
	idx.adminMgrCmdCh <- &MsgGeneral{mType: ADMIN_MGR_SHUTDOWN}
	<-idx.adminMgrCmdCh

	//shutdown cluster manager
	idx.clustMgrSenderCmdCh <- &MsgGeneral{mType: CLUST_MGR_SENDER_SHUTDOWN}
	<-idx.clustMgrSenderCmdCh

	//shutdown kv sender
	idx.kvSenderCmdCh <- &MsgGeneral{mType: KV_SENDER_SHUTDOWN}
	<-idx.kvSenderCmdCh
}

func (idx *indexer) Shutdown() Message {

	log.Printf("Indexer: Shutting Down")
	//close the internal shutdown channel
	close(idx.shutdownInitCh)
	<-idx.shutdownCompleteCh
	log.Printf("Indexer: Shutdown Complete")
	return nil
}
