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
	"errors"
	"github.com/couchbase/indexing/secondary/common"
	"net"
	"strconv"
	"time"
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

type StreamStatus map[common.StreamId]bool

//TODO move this to config
var NUM_VBUCKETS uint16
var PROJECTOR_ADMIN_PORT_ENDPOINT string
var StreamAddrMap StreamAddressMap

type BucketIndexCountMap map[string]int

type indexer struct {
	id    IndexerId
	state IndexerState //state of the indexer

	indexInstMap  common.IndexInstMap //map of indexInstId to IndexInst
	indexPartnMap IndexPartnMap       //map of indexInstId to PartitionInst

	streamStatus StreamStatus //stream status map

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
	scanCoordCmdCh      MsgChannel //chhannel to send messages to scan coordinator

	mutMgrExitCh MsgChannel //channel to indicate mutation manager exited

	tk             Timekeeper      //handle to timekeeper
	storageMgr     StorageManager  //handle to storage manager
	mutMgr         MutationManager //handle to mutation manager
	adminMgr       AdminManager    //handle to admin port manager
	clustMgrSender ClustMgrSender  //handle to ClustMgrSender
	kvSender       KVSender        //handle to KVSender
	cbqBridge      CbqBridge       //handle to CbqBridge
	scanCoord      ScanCoordinator //handle to ScanCoordinator

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
		scanCoordCmdCh:      make(MsgChannel),

		mutMgrExitCh: make(MsgChannel),

		indexInstMap:  make(common.IndexInstMap),
		indexPartnMap: make(IndexPartnMap),

		streamStatus: make(StreamStatus),
	}

	idx.state = INIT
	common.Infof("Indexer::NewIndexer Status INIT")

	//assume indexerId 1 for now
	idx.id = 1

	if numVbuckets > 0 {
		NUM_VBUCKETS = numVbuckets
	} else {
		NUM_VBUCKETS = MAX_NUM_VBUCKETS
	}

	common.Infof("Indexer::NewIndexer Starting with Vbuckets %v", NUM_VBUCKETS)

	idx.initStreamAddressMap()

	var res Message
	idx.clustMgrSender, res = NewClustMgrSender(idx.clustMgrSenderCmdCh, idx.wrkrRecvCh)
	if res.GetMsgType() != MSG_SUCCESS {
		common.Errorf("Indexer::NewIndexer ClusterMgrSender Init Error", res)
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
	if res.GetMsgType() != MSG_SUCCESS {
		common.Errorf("Indexer::NewIndexer Storage Manager Init Error", res)
		return nil, res
	}

	//Recover Persisted Snapshots
	idx.recoverPersistedSnapshots()

	//Start Timekeeper
	idx.tk, res = NewTimekeeper(idx.tkCmdCh, idx.wrkrRecvCh)
	if res.GetMsgType() != MSG_SUCCESS {
		common.Errorf("Indexer::NewIndexer Timekeeper Init Error", res)
		return nil, res
	}

	//Start KV Sender
	idx.kvSender, res = NewKVSender(idx.kvSenderCmdCh, idx.wrkrRecvCh, numVbuckets)
	if res.GetMsgType() != MSG_SUCCESS {
		common.Errorf("Indexer::NewIndexer KVSender Init Error", res)
		return nil, res
	}

	//Start Admin port listener
	idx.adminMgr, res = NewAdminManager(idx.adminMgrCmdCh, idx.adminRecvCh)
	if res.GetMsgType() != MSG_SUCCESS {
		common.Errorf("Indexer::NewIndexer Admin Manager Init Error", res)
		return nil, res
	}

	//Start Mutation Manager
	idx.mutMgr, res = NewMutationManager(idx.mutMgrCmdCh, idx.wrkrRecvCh,
		numVbuckets)
	if res.GetMsgType() != MSG_SUCCESS {
		common.Errorf("Indexer::NewIndexer Mutation Manager Init Error", res)
		return nil, res
	}

	//Start Scan Coordinator
	idx.scanCoord, res = NewScanCoordinator(idx.scanCoordCmdCh, idx.wrkrRecvCh)
	if res.GetMsgType() != MSG_SUCCESS {
		common.Errorf("Indexer::NewIndexer Scan Coordinator Init Error", res)
		return nil, res
	}

	//Start CbqBridge
	idx.cbqBridge, res = NewCbqBridge(idx.cbqBridgeCmdCh, idx.adminRecvCh)
	if res.GetMsgType() != MSG_SUCCESS {
		common.Errorf("Indexer::NewIndexer CbqBridge Init Error", res)
		return nil, res
	}

	idx.state = ACTIVE
	common.Infof("Indexer::NewIndexer Status ACTIVE")

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
				case MSG_ERROR:
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
		common.Debugf("Indexer::handleWorkerMsgs Received Stream Begin "+
			"From Mutation Mgr %v", msg)

	case STREAM_READER_STREAM_END:
		//TODO
		common.Debugf("Indexer::handleWorkerMsgs Received Stream End "+
			"From Mutation Mgr %v", msg)

	case STREAM_READER_STREAM_DROP_DATA:
		//TODO
		common.Debugf("Indexer::handleWorkerMsgs Received Drop Data "+
			"From Mutation Mgr %v", msg)

	case STREAM_READER_SNAPSHOT_MARKER:
		//fwd the message to timekeeper
		idx.tkCmdCh <- msg
		<-idx.tkCmdCh

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

		common.Debugf("Indexer::handleWorkerMsgs Received Flush Done "+
			"From Mutation Mgr %v", msg)

		//fwd the message to storage manager
		idx.storageMgrCmdCh <- msg
		<-idx.storageMgrCmdCh

		//fwd the message to timekeeper
		idx.tkCmdCh <- msg
		<-idx.tkCmdCh

	case TK_INIT_BUILD_DONE:
		idx.handleInitialBuildDone(msg)

	case TK_MERGE_STREAM:
		idx.handleMergeStream(msg)

	default:
		common.Errorf("Indexer::handleWorkerMsgs Unknown Message %v", msg)
	}

}

func (idx *indexer) handleAdminMsgs(msg Message) {

	switch msg.GetMsgType() {

	case INDEXER_CREATE_INDEX_DDL:

		idx.handleCreateIndex(msg)

	case INDEXER_DROP_INDEX_DDL:

		idx.handleDropIndex(msg)

	case SCAN_COORD_SCAN_INDEX:

		idx.handleScanIndex(msg)

	default:

		common.Errorf("Indexer::handleAdminMsgs Unknown Message %v", msg)

	}

}

//TODO handle panic, otherwise main loop will get shutdown
func (idx *indexer) handleCreateIndex(msg Message) {

	indexInst := msg.(*MsgCreateIndex).GetIndexInst()
	respCh := msg.(*MsgCreateIndex).GetResponseChannel()

	common.Infof("Indexer::handleCreateIndex %v", indexInst)

	//check if this is duplicate index instance
	if ok := idx.checkDuplicateIndex(indexInst, respCh); !ok {
		return
	}

	//check if Initial Build is already running for this index's bucket
	if ok := idx.checkDuplicateInitialBuildRequest(indexInst, respCh); !ok {
		return
	}

	//allocate partition/slice
	var partnInstMap PartitionInstMap
	var ok bool
	if partnInstMap, ok = idx.initPartnInstance(indexInst, respCh); !ok {
		return
	}

	//if there is already an index for this bucket in MAINT_STREAM,
	//add this index to INIT_STREAM
	if idx.checkBucketExistsInStream(indexInst.Defn.Bucket, common.MAINT_STREAM) {
		indexInst.Stream = common.INIT_STREAM
	} else {
		indexInst.Stream = common.MAINT_STREAM
	}

	//get current timestamp from KV and set it as Initial Build Timestamp
	var clusterAddr string
	if host, _, err := net.SplitHostPort(PROJECTOR_ADMIN_PORT_ENDPOINT); err == nil {
		clusterAddr = host + ":" + KVPORT
	}

	buildTs := idx.getCurrentKVTs(clusterAddr, indexInst.Defn.Bucket)

	//if initial build TS is zero, set index state to active and add it to
	//MAINT_STREAM directly
	initialBuildReqd := true
	if buildTs.IsZeroTs() {
		//set index state
		indexInst.State = common.INDEX_STATE_ACTIVE
		indexInst.Stream = common.MAINT_STREAM
		initialBuildReqd = false
	} else {
		indexInst.State = common.INDEX_STATE_INITIAL
	}

	common.Debugf("Indexer::handleCreateIndex \n\tAdded Index: %v to Stream: %v State: %v",
		indexInst.InstId, indexInst.Stream, indexInst.State)

	//update index maps with this index
	idx.indexInstMap[indexInst.InstId] = indexInst
	idx.indexPartnMap[indexInst.InstId] = partnInstMap

	msgUpdateIndexInstMap := &MsgUpdateInstMap{indexInstMap: idx.indexInstMap}
	msgUpdateIndexPartnMap := &MsgUpdatePartnMap{indexPartnMap: idx.indexPartnMap}

	//update index map in storage manager
	if ok := idx.updateWorkerIndexMap(msgUpdateIndexInstMap, msgUpdateIndexPartnMap, idx.storageMgrCmdCh,
		"StorageMgr", respCh); !ok {
		return
	}

	//update index map in mutation manager
	if ok := idx.updateWorkerIndexMap(msgUpdateIndexInstMap, msgUpdateIndexPartnMap, idx.mutMgrCmdCh,
		"MutationMgr", respCh); !ok {
		return
	}

	//update index map in scan coordinator
	if ok := idx.updateWorkerIndexMap(msgUpdateIndexInstMap, msgUpdateIndexPartnMap, idx.scanCoordCmdCh,
		"ScanCoordinator", respCh); !ok {
		return
	}

	//send Stream Update to workers
	if ok := idx.sendStreamUpdateForCreateIndex(indexInst, buildTs, respCh); !ok {
		return
	}

	//if initial build is not being done, send success response,
	//otherwise success response will be sent when initial build gets done
	if !initialBuildReqd {
		respCh <- &MsgSuccess{}
	}

}

//TODO handle panic, otherwise main loop will get shutdown
func (idx *indexer) handleDropIndex(msg Message) {

	indexInstId := msg.(*MsgDropIndex).GetIndexInstId()
	respCh := msg.(*MsgDropIndex).GetResponseChannel()

	common.Debugf("Indexer::handleDropIndex - IndexInstId %v", indexInstId)

	var indexInst common.IndexInst
	var ok bool
	if indexInst, ok = idx.indexInstMap[indexInstId]; !ok {

		common.Errorf("Indexer::handleDropIndex Unknown IndexInstId", indexInstId)

		respCh <- &MsgError{
			err: Error{code: ERROR_INDEXER_UNKNOWN_INDEX,
				severity: FATAL,
				cause:    errors.New("Index Unknown"),
				category: INDEXER}}
	}

	//update internal maps
	delete(idx.indexInstMap, indexInstId)
	delete(idx.indexPartnMap, indexInstId)

	//send Stream update to workers
	if ok := idx.sendStreamUpdateForDropIndex(indexInst, respCh); !ok {
		return
	}

	msgUpdateIndexInstMap := &MsgUpdateInstMap{indexInstMap: idx.indexInstMap}
	msgUpdateIndexPartnMap := &MsgUpdatePartnMap{indexPartnMap: idx.indexPartnMap}

	//update index map in storage manager
	if ok := idx.updateWorkerIndexMap(msgUpdateIndexInstMap, msgUpdateIndexPartnMap, idx.storageMgrCmdCh,
		"StorageMgr", respCh); !ok {
		return
	}

	//update index map in mutation manager
	if ok := idx.updateWorkerIndexMap(msgUpdateIndexInstMap, msgUpdateIndexPartnMap, idx.mutMgrCmdCh,
		"MutationMgr", respCh); !ok {
		return
	}

	//update index map in scan coordinator
	if ok := idx.updateWorkerIndexMap(msgUpdateIndexInstMap, msgUpdateIndexPartnMap, idx.scanCoordCmdCh,
		"ScanCoordinator", respCh); !ok {
		return
	}

	respCh <- &MsgSuccess{}
}

func (idx *indexer) handleScanIndex(msg Message) {

	idxInstId := msg.(*MsgScanIndex).GetIndexInstId()
	common.Debugf("Indexer received ScanIndex for Index %v", idxInstId)

	//fwd the message to Scan Coordinator
	idx.scanCoordCmdCh <- msg

	if resp, ok := <-idx.scanCoordCmdCh; ok {

		if resp.GetMsgType() != MSG_SUCCESS {
			common.Errorf("Indexer::handleScanIndex Error received from Scan Coordinator "+
				"processing Msg %v Err %v. Aborted.", msg, resp)
			return
		}
	} else {
		common.Errorf("Indexer::handleScanIndex Error communicating with Scan Coordinator "+
			"processing Msg %v Err %v. Aborted.", msg, resp)
		return
	}

}

func (idx *indexer) shutdownWorkers() {

	//shutdown mutation manager
	idx.mutMgrCmdCh <- &MsgGeneral{mType: MUT_MGR_SHUTDOWN}
	<-idx.mutMgrCmdCh

	//shutdown scan coordinator
	idx.scanCoordCmdCh <- &MsgGeneral{mType: SCAN_COORD_SHUTDOWN}
	<-idx.scanCoordCmdCh

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

	common.Infof("Indexer::Shutdown -  Shutting Down")
	//close the internal shutdown channel
	close(idx.shutdownInitCh)
	<-idx.shutdownCompleteCh
	common.Infof("Indexer:Shutdown - Shutdown Complete")
	return nil
}

func (idx *indexer) sendStreamUpdateForCreateIndex(indexInst common.IndexInst,
	buildTs Timestamp, respCh MsgChannel) bool {

	var cmd Message
	var indexList []common.IndexInst
	indexList = append(indexList, indexInst)

	//if stream is already running, add index
	//to stream else open new stream
	newStream := true
	if status, ok := idx.streamStatus[indexInst.Stream]; ok && status {
		newStream = false
	}

	if newStream {
		cmd = &MsgStreamUpdate{mType: OPEN_STREAM,
			streamId:  indexInst.Stream,
			indexList: indexList,
			buildTs:   buildTs,
			respCh:    respCh}
	} else {
		cmd = &MsgStreamUpdate{mType: ADD_INDEX_LIST_TO_STREAM,
			streamId:  indexInst.Stream,
			indexList: indexList,
			buildTs:   buildTs,
			respCh:    respCh}
	}

	//send stream update to timekeeper
	if ok := idx.sendStreamUpdateToWorker(cmd, idx.tkCmdCh, "Timekeeper", respCh); !ok {
		return false
	}

	//send stream update to mutation manager
	if ok := idx.sendStreamUpdateToWorker(cmd, idx.mutMgrCmdCh, "MutationMgr", respCh); !ok {
		return false
	}

	//send stream update to kv sender
	if ok := idx.sendStreamUpdateToWorker(cmd, idx.kvSenderCmdCh, "KVSender", respCh); !ok {
		return false
	}

	//For INIT_STREAM, add index is added to MAINT_STREAM in Catchup State,
	//so mutations for this index are already in queue to allow convergence with INIT_STREAM.
	if indexInst.Stream == common.INIT_STREAM {
		//add indexes to MAINT_STREAM
		cmd := &MsgStreamUpdate{mType: ADD_INDEX_LIST_TO_STREAM,
			streamId:  common.MAINT_STREAM,
			indexList: indexList}

		//send stream update to mutation manager
		if ok := idx.sendStreamUpdateToWorker(cmd, idx.mutMgrCmdCh, "MutationMgr", respCh); !ok {
			return false
		}

		//send stream update to kv sender
		if ok := idx.sendStreamUpdateToWorker(cmd, idx.kvSenderCmdCh, "KVSender", respCh); !ok {
			return false
		}
	}

	idx.streamStatus[indexInst.Stream] = true

	return true

}

func (idx *indexer) sendStreamUpdateToWorker(cmd Message, workerCmdCh MsgChannel,
	workerStr string, respCh MsgChannel) bool {

	//send message to worker
	workerCmdCh <- cmd
	if resp, ok := <-workerCmdCh; ok {

		if resp.GetMsgType() != MSG_SUCCESS {
			common.Errorf("Indexer::sendStreamUpdateToWorker - Error received from %v "+
				"processing Msg %v Err %v. Aborted.", workerStr, cmd, resp)

			if respCh != nil {
				respCh <- &MsgError{
					err: Error{code: ERROR_INDEXER_INTERNAL_ERROR,
						severity: FATAL,
						cause:    errors.New("Indexer Internal Error"),
						category: INDEXER}}
				return false
			}
		}
	} else {
		common.Errorf("Indexer::sendStreamUpdateToWorker - Error communicating with %v "+
			"processing Msg %v Err %v. Aborted.", workerStr, cmd, resp)

		if respCh != nil {
			respCh <- &MsgError{
				err: Error{code: ERROR_INDEXER_INTERNAL_ERROR,
					severity: FATAL,
					cause:    errors.New("Indexer Internal Error"),
					category: INDEXER}}
		}
		return false
	}
	return true
}

func (idx *indexer) sendStreamUpdateForDropIndex(indexInst common.IndexInst,
	respCh MsgChannel) bool {

	var cmd Message
	var indexList []common.IndexInst
	indexList = append(indexList, indexInst)

	cmd = &MsgStreamUpdate{mType: REMOVE_INDEX_LIST_FROM_STREAM,
		streamId:  indexInst.Stream,
		indexList: indexList}

	//send stream update to kv sender
	if ok := idx.sendStreamUpdateToWorker(cmd, idx.kvSenderCmdCh, "KVSender", respCh); !ok {
		return false
	}

	//send stream update to timekeeper
	if ok := idx.sendStreamUpdateToWorker(cmd, idx.tkCmdCh, "Timekeeper", respCh); !ok {
		return false
	}

	//send stream update to mutation manager
	if ok := idx.sendStreamUpdateToWorker(cmd, idx.mutMgrCmdCh, "MutationMgr", respCh); !ok {
		return false
	}

	//if there are no more indexes in the stream, generate CLOSE_STREAM
	if idx.checkStreamEmpty(indexInst.Stream) {
		cmd = &MsgStreamUpdate{mType: CLOSE_STREAM,
			streamId: indexInst.Stream}
		idx.streamStatus[indexInst.Stream] = false

		//send stream update to timekeeper
		if ok := idx.sendStreamUpdateToWorker(cmd, idx.tkCmdCh, "Timekeeper", respCh); !ok {
			return false
		}

		//send stream update to mutation manager
		if ok := idx.sendStreamUpdateToWorker(cmd, idx.mutMgrCmdCh, "MutationMgr", respCh); !ok {
			return false
		}
	}

	return true

}

func (idx *indexer) initPartnInstance(indexInst common.IndexInst,
	respCh MsgChannel) (PartitionInstMap, bool) {

	//initialize partitionInstMap for this index
	partnInstMap := make(PartitionInstMap)

	//get all partitions for this index
	partnDefnList := indexInst.Pc.GetAllPartitions()

	for i, partnDefn := range partnDefnList {
		//TODO: Ignore partitions which do not belong to this
		//indexer node(based on the endpoints)
		partnInst := PartitionInst{Defn: partnDefn,
			Sc: NewHashedSliceContainer()}

		common.Infof("Indexer::initPartnInstance Initialized Partition: \n\t Index: %v Partition: %v",
			indexInst.InstId, partnInst)

		//add a single slice per partition for now
		if slice, err := NewForestDBSlice(indexInst.Defn.Bucket+"_"+indexInst.Defn.Name,
			0, indexInst.Defn.DefnId, indexInst.InstId); err == nil {
			partnInst.Sc.AddSlice(0, slice)
			common.Infof("Indexer::initPartnInstance Initialized Slice: \n\t Index: %v Slice: %v",
				indexInst.InstId, slice)

			partnInstMap[common.PartitionId(i)] = partnInst
		} else {
			common.Errorf("Indexer::initPartnInstance Error creating slice %v. Abort.",
				err)

			respCh <- &MsgError{
				err: Error{code: ERROR_INDEXER_INTERNAL_ERROR,
					severity: FATAL,
					cause:    errors.New("Indexer Internal Error"),
					category: INDEXER}}
			return nil, false
		}
	}

	return partnInstMap, true
}

func (idx *indexer) updateWorkerIndexMap(msgUpdateIndexInstMap Message,
	msgUpdateIndexPartnMap Message, workerCmdCh MsgChannel, workerStr string,
	respCh MsgChannel) bool {

	if ok := idx.sendUpdatedIndexMapToWorker(msgUpdateIndexInstMap, msgUpdateIndexPartnMap,
		workerCmdCh, workerStr); !ok {

		if respCh != nil {
			respCh <- &MsgError{
				err: Error{code: ERROR_INDEXER_INTERNAL_ERROR,
					severity: FATAL,
					cause:    errors.New("Indexer Internal Error"),
					category: INDEXER}}
		}
		return false
	}

	return true
}

func (idx *indexer) sendUpdatedIndexMapToWorker(msgUpdateIndexInstMap Message,
	msgUpdateIndexPartnMap Message, workerCmdCh chan Message, workerStr string) bool {

	if msgUpdateIndexInstMap != nil {
		workerCmdCh <- msgUpdateIndexInstMap

		if resp, ok := <-workerCmdCh; ok {

			if resp.GetMsgType() != MSG_SUCCESS {
				common.Errorf("Indexer::sendUpdatedIndexMapToWorker - Error received from %v processing "+
					"Msg %v Err %v. Aborted.", workerStr, msgUpdateIndexInstMap, resp)
				return false
			}
		} else {
			common.Errorf("Indexer::sendUpdatedIndexMapToWorker - Error communicating with %v "+
				"processing Msg %v Err %v. Aborted.", workerStr, msgUpdateIndexInstMap, resp)
			return false
		}
	}

	if msgUpdateIndexPartnMap != nil {
		workerCmdCh <- msgUpdateIndexPartnMap
		if resp, ok := <-workerCmdCh; ok {

			if resp.GetMsgType() != MSG_SUCCESS {
				common.Errorf("Indexer::sendUpdatedIndexMapToWorker - Error received from %v processing "+
					"Msg %v Err %v. Aborted.", workerStr, msgUpdateIndexPartnMap, resp)
				return false
			}
		} else {
			common.Errorf("Indexer::sendUpdatedIndexMapToWorker - Error communicating with %v "+
				"processing Msg %v Err %v. Aborted.", workerStr, msgUpdateIndexPartnMap, resp)
			return false
		}
	}

	return true

}

func (idx *indexer) initStreamAddressMap() {

	//init the stream address map
	StreamAddrMap = make(StreamAddressMap)

	if _, port, err := net.SplitHostPort(INDEXER_MAINT_DATA_PORT_ENDPOINT); err == nil {
		StreamAddrMap[common.MAINT_STREAM] = common.Endpoint(":" + port)
	} else {
		common.Errorf("Indexer::initStreamAddressMap Unable to find address for Maint Port. "+
			"INDEXER_MAINT_DATA_PORT_ENDPOINT not set properly. Err %v", err)
	}

	if _, port, err := net.SplitHostPort(INDEXER_INIT_DATA_PORT_ENDPOINT); err == nil {
		StreamAddrMap[common.INIT_STREAM] = common.Endpoint(":" + port)
	} else {
		common.Errorf("Indexer:initStreamAddressMap Unable to find address for Init Port. "+
			"INDEXER_INIT_DATA_PORT_ENDPOINT not set properly. Err %v", err)
	}
}

//checkDuplicateIndex checks if an index with the given indexInstId
// or name already exists
func (idx *indexer) checkDuplicateIndex(indexInst common.IndexInst,
	respCh MsgChannel) bool {

	//if the indexInstId already exists, return error
	if index, ok := idx.indexInstMap[indexInst.InstId]; ok {
		common.Errorf("Indexer::checkDuplicateIndex Duplicate Index Instance. "+
			"IndexInstId: %v, Index: %v", indexInst.InstId, index)

		respCh <- &MsgError{
			err: Error{code: ERROR_INDEX_ALREADY_EXISTS,
				severity: FATAL,
				cause:    errors.New("Duplicate Index Instance"),
				category: INDEXER}}
		return false
	}

	//if the index name already exists for the same bucket,
	//return error
	for _, index := range idx.indexInstMap {

		if index.Defn.Name == indexInst.Defn.Name &&
			index.Defn.Bucket == indexInst.Defn.Bucket {

			common.Errorf("Indexer::checkDuplicateIndex Duplicate Index Name. "+
				"Name: %v, Duplicate Index: %v", indexInst.Defn.Name, index)

			respCh <- &MsgError{
				err: Error{code: ERROR_INDEX_ALREADY_EXISTS,
					severity: FATAL,
					cause:    errors.New("Duplicate Index Name"),
					category: INDEXER}}
			return false
		}

	}
	return true
}

//checkDuplicateInitialBuildRequest check if INIT_STREAM is already running for the
//bucket on the given index
func (idx *indexer) checkDuplicateInitialBuildRequest(indexInst common.IndexInst,
	respCh MsgChannel) bool {

	//if initial build is already running for some other index on this bucket,
	//cannot start another one
	for _, index := range idx.indexInstMap {

		if (index.State == common.INDEX_STATE_INITIAL ||
			index.State == common.INDEX_STATE_CATCHUP) &&
			indexInst.Defn.Bucket == index.Defn.Bucket {

			errStr := "Index Build Already In Progress. Multiple Initial " +
				"Builds On A Bucket Are Not Supported By Indexer."

			common.Errorf(errStr)
			respCh <- &MsgError{
				err: Error{code: ERROR_INDEX_BUILD_IN_PROGRESS,
					severity: FATAL,
					cause:    errors.New(errStr),
					category: INDEXER}}
			return false
		}
	}

	return true
}

//TODO If this function gets error before its finished, the state
//can be inconsistent. This needs to be fixed.
func (idx *indexer) handleInitialBuildDone(msg Message) {

	bucket := msg.(*MsgTKInitBuildDone).GetBucket()
	streamId := msg.(*MsgTKInitBuildDone).GetStreamId()
	respCh := msg.(*MsgTKInitBuildDone).GetResponseChannel()

	common.Debugf("Indexer::handleInitialBuildDone Bucket: %v Stream: %v", bucket, streamId)

	//MAINT_STREAM should already be running for this bucket,
	//as first index gets added to MAINT_STREAM always
	if idx.checkBucketExistsInStream(bucket, common.MAINT_STREAM) == false {
		common.Errorf("Indexer::handleInitialBuildDone MAINT_STREAM not enabled for Bucket: %v. "+
			"Cannot Process Initial Build Done.", bucket)
		if respCh != nil {
			respCh <- &MsgError{
				err: Error{code: ERROR_INDEXER_INTERNAL_ERROR,
					severity: FATAL,
					cause:    errors.New("Indexer Internal Error"),
					category: INDEXER}}
		}
		return
	}

	//get the list of indexes for this bucket and stream in INITIAL state
	var indexList []common.IndexInst
	for _, index := range idx.indexInstMap {
		if index.Defn.Bucket == bucket && index.Stream == streamId &&
			index.State == common.INDEX_STATE_INITIAL {
			//index in INIT_STREAM move to Catchup state
			if streamId == common.INIT_STREAM {
				index.State = common.INDEX_STATE_CATCHUP
			} else {
				index.State = common.INDEX_STATE_ACTIVE
			}
			indexList = append(indexList, index)
		}
	}

	//For MAINT_STREAM, only need to notify Timekeeper to update its maps
	if streamId == common.MAINT_STREAM {
		cmd := &MsgStreamUpdate{mType: REMOVE_INDEX_LIST_FROM_STREAM,
			streamId:  common.MAINT_STREAM,
			indexList: indexList}

		//send stream update to timekeeper
		if ok := idx.sendStreamUpdateToWorker(cmd, idx.tkCmdCh, "Timekeeper", respCh); !ok {
			return
		}
	}

	//For INIT_STREAM, nothing needs to be done. Once Catchup gets done,
	//stream update will happen.

	//update the IndexInstMap
	for _, index := range indexList {
		idx.indexInstMap[index.InstId] = index
	}

	//send updated maps to all workers
	msgUpdateIndexInstMap := &MsgUpdateInstMap{indexInstMap: idx.indexInstMap}

	//update index map in storage manager
	if ok := idx.updateWorkerIndexMap(msgUpdateIndexInstMap, nil, idx.storageMgrCmdCh,
		"StorageMgr", respCh); !ok {
		return
	}

	//update index map in mutation manager
	if ok := idx.updateWorkerIndexMap(msgUpdateIndexInstMap, nil, idx.mutMgrCmdCh,
		"MutationMgr", respCh); !ok {
		return
	}

	//update index map in scan coordinator
	if ok := idx.updateWorkerIndexMap(msgUpdateIndexInstMap, nil, idx.scanCoordCmdCh,
		"ScanCoordinator", respCh); !ok {
		return
	}

	//send success to response channel
	respCh <- &MsgSuccess{}
}

//TODO If this function gets error before its finished, the state
//can be inconsistent. This needs to be fixed.
func (idx *indexer) handleMergeStream(msg Message) {

	bucket := msg.(*MsgTKMergeStream).GetBucket()
	streamId := msg.(*MsgTKMergeStream).GetStreamId()

	common.Debugf("Indexer::handleMergeStream Bucket: %v Stream: %v", bucket, streamId)

	//For now, only INIT_STREAM can be merged into MAINT_STREAM
	if streamId != common.INIT_STREAM {
		common.Errorf("Indexer::handleMergeStream \n\tOnly INIT_STREAM can be merged "+
			"to MAINT_STREAM. Found Stream: %v.", streamId)
		return
	}

	//MAINT_STREAM should already be running for this bucket,
	//as first index gets added to MAINT_STREAM always
	if idx.checkBucketExistsInStream(bucket, common.MAINT_STREAM) == false {
		common.Errorf("Indexer::handleMergeStream \n\tMAINT_STREAM not enabled for Bucket: %v ."+
			"Cannot Process Merge Stream", bucket)
		return
	}

	//get the list of indexes for this bucket in CATCHUP state
	var indexList []common.IndexInst
	for _, index := range idx.indexInstMap {
		if index.Defn.Bucket == bucket && index.Stream == streamId &&
			index.State == common.INDEX_STATE_CATCHUP {

			index.State = common.INDEX_STATE_ACTIVE
			index.Stream = common.MAINT_STREAM
			indexList = append(indexList, index)
		}
	}

	//remove indexes from INIT_STREAM
	cmd := &MsgStreamUpdate{mType: REMOVE_INDEX_LIST_FROM_STREAM,
		streamId:  common.INIT_STREAM,
		indexList: indexList}

	//send stream update to kv sender
	if ok := idx.sendStreamUpdateToWorker(cmd, idx.kvSenderCmdCh, "KVSender", nil); !ok {
		return
	}

	//send stream update to timekeeper
	if ok := idx.sendStreamUpdateToWorker(cmd, idx.tkCmdCh, "Timekeeper", nil); !ok {
		return
	}

	//send stream update to mutation manager
	if ok := idx.sendStreamUpdateToWorker(cmd, idx.mutMgrCmdCh, "MutationMgr", nil); !ok {
		return
	}

	//update the IndexInstMap
	for _, index := range indexList {
		idx.indexInstMap[index.InstId] = index
	}

	if idx.checkStreamEmpty(streamId) {
		cmd = &MsgStreamUpdate{mType: CLOSE_STREAM,
			streamId: common.INIT_STREAM}

		//send stream update to timekeeper
		if ok := idx.sendStreamUpdateToWorker(cmd, idx.tkCmdCh, "Timekeeper", nil); !ok {
			return
		}

		//send stream update to mutation manager
		if ok := idx.sendStreamUpdateToWorker(cmd, idx.mutMgrCmdCh, "MutationMgr", nil); !ok {
			return
		}

		idx.streamStatus[common.INIT_STREAM] = false
	}

	//send updated maps to all workers
	msgUpdateIndexInstMap := &MsgUpdateInstMap{indexInstMap: idx.indexInstMap}

	//update index map in storage manager
	if ok := idx.updateWorkerIndexMap(msgUpdateIndexInstMap, nil, idx.storageMgrCmdCh,
		"StorageMgr", nil); !ok {
		return
	}

	//update index map in mutation manager
	if ok := idx.updateWorkerIndexMap(msgUpdateIndexInstMap, nil, idx.mutMgrCmdCh,
		"MutationMgr", nil); !ok {
		return
	}

	//update index map in scan coordinator
	if ok := idx.updateWorkerIndexMap(msgUpdateIndexInstMap, nil, idx.scanCoordCmdCh,
		"ScanCoordinator", nil); !ok {
		return
	}

	//enable flush for this bucket in MAINT_STREAM
	idx.tkCmdCh <- &MsgTKEnableFlush{streamId: common.MAINT_STREAM,
		bucket: bucket}
	<-idx.tkCmdCh

	common.Debugf("Indexer::handleMergeStream Merge Done Bucket: %v Stream: %v",
		bucket, streamId)
}

func (idx *indexer) getCurrentKVTs(cluster, bucket string) Timestamp {

	ts := NewTimestamp()

	start := time.Now()
	if b, err := common.ConnectBucket(cluster, "default", bucket); err == nil {
		//get all the vb seqnum
		stats := b.GetStats("vbucket-seqno")

		//for all nodes in cluster
		for _, nodestat := range stats {
			//for all vbuckets
			for i := 1; i <= int(NUM_VBUCKETS); i++ {
				vbkey := "vb_" + strconv.Itoa(i) + ":high_seqno"
				if highseqno, ok := nodestat[vbkey]; ok {
					if s, err := strconv.Atoi(highseqno); err == nil {
						ts[i] = Seqno(s)
					}
				}
			}
		}
		elapsed := time.Since(start)
		common.Debugf("Indexer::getCurrentKVTs Time Taken %v", elapsed)
		return ts

	} else {
		common.Errorf("Indexer::getCurrentKVTs Error Connecting to KV Cluster %v", err)
		return nil
	}

}

//checkBucketExistsInStream returns true if there is no index in the given stream
//which belongs to the given bucket, else false
func (idx *indexer) checkBucketExistsInStream(bucket string, streamId common.StreamId) bool {

	//check if any index of the given bucket is in the Stream
	for _, index := range idx.indexInstMap {

		if index.Defn.Bucket == bucket && index.Stream == streamId {
			return true
		}
	}

	return false

}

//checkStreamEmpty return true if there is no index currently in the
//give stream, else false
func (idx *indexer) checkStreamEmpty(streamId common.StreamId) bool {

	for _, index := range idx.indexInstMap {
		if index.Stream == streamId {
			common.Tracef("Indexer::checkStreamEmpty Found Index %v Stream %v",
				index.InstId, streamId)
			return false
		}
	}
	common.Tracef("Indexer::checkStreamEmpty Stream %v Empty", streamId)

	return true

}
