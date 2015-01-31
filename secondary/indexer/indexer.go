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
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbaselabs/goforestdb"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

type Indexer interface {
	Shutdown() Message
}

var StreamAddrMap StreamAddressMap
var StreamTopicName map[common.StreamId]string

type BucketIndexCountMap map[string]int
type BucketFlushInProgressMap map[string]bool
type BucketObserveFlushDoneMap map[string]MsgChannel
type BucketRequestStopCh map[string]StopChannel
type BucketRollbackTs map[string]*common.TsVbuuid

// Errors
var (
	ErrFatalComm                = errors.New("Fatal Internal Communication Error")
	ErrInconsistentState        = errors.New("Inconsistent Internal State")
	ErrKVRollbackForInitRequest = errors.New("KV Rollback Received For Initial Build Request")
	ErrMaintStreamMissingBucket = errors.New("Bucket Missing in Maint Stream")
	ErrInvalidStream            = errors.New("Invalid Stream")
	ErrIndexerInRecovery        = errors.New("Indexer In Recovery")
	ErrKVConnect                = errors.New("Error Connecting KV")
	ErrUnknownBucket            = errors.New("Unknown Bucket")
)

type indexer struct {
	id string

	indexInstMap  common.IndexInstMap //map of indexInstId to IndexInst
	indexPartnMap IndexPartnMap       //map of indexInstId to PartitionInst

	streamBucketStatus map[common.StreamId]BucketStatus

	streamBucketFlushInProgress  map[common.StreamId]BucketFlushInProgressMap
	streamBucketObserveFlushDone map[common.StreamId]BucketObserveFlushDoneMap

	streamBucketRequestStopCh map[common.StreamId]BucketRequestStopCh
	streamBucketRollbackTs    map[common.StreamId]BucketRollbackTs

	bucketsInCatchup map[string]bool

	//TODO Remove this once cbq bridge support goes away
	bucketCreateClientChMap map[string]MsgChannel

	wrkrRecvCh         MsgChannel //channel to receive messages from workers
	internalRecvCh     MsgChannel //buffered channel to queue worker requests
	adminRecvCh        MsgChannel //channel to receive admin messages
	shutdownInitCh     MsgChannel //internal shutdown channel for indexer
	shutdownCompleteCh MsgChannel //indicate shutdown completion

	mutMgrCmdCh        MsgChannel //channel to send commands to mutation manager
	storageMgrCmdCh    MsgChannel //channel to send commands to storage manager
	tkCmdCh            MsgChannel //channel to send commands to timekeeper
	adminMgrCmdCh      MsgChannel //channel to send commands to admin port manager
	compactMgrCmdCh    MsgChannel //channel to send commands to compaction manager
	clustMgrAgentCmdCh MsgChannel //channel to send messages to index coordinator
	kvSenderCmdCh      MsgChannel //channel to send messages to kv sender
	cbqBridgeCmdCh     MsgChannel //channel to send message to cbq sender
	settingsMgrCmdCh   MsgChannel
	statsMgrCmdCh      MsgChannel
	scanCoordCmdCh     MsgChannel //chhannel to send messages to scan coordinator

	mutMgrExitCh MsgChannel //channel to indicate mutation manager exited

	tk            Timekeeper        //handle to timekeeper
	storageMgr    StorageManager    //handle to storage manager
	compactMgr    CompactionManager //handle to compaction manager
	mutMgr        MutationManager   //handle to mutation manager
	adminMgr      AdminManager      //handle to admin port manager
	clustMgrAgent ClustMgrAgent     //handle to ClustMgrAgent
	kvSender      KVSender          //handle to KVSender
	cbqBridge     CbqBridge         //handle to CbqBridge
	settingsMgr   settingsManager
	statsMgr      statsManager
	scanCoord     ScanCoordinator //handle to ScanCoordinator
	config        common.Config

	kvlock sync.Mutex //fine-grain lock for KVSender

	enableManager bool
	needsRestart  bool
}

func NewIndexer(config common.Config) (Indexer, Message) {

	idx := &indexer{
		wrkrRecvCh:         make(MsgChannel),
		internalRecvCh:     make(MsgChannel, WORKER_MSG_QUEUE_LEN),
		adminRecvCh:        make(MsgChannel, WORKER_MSG_QUEUE_LEN),
		shutdownInitCh:     make(MsgChannel),
		shutdownCompleteCh: make(MsgChannel),

		mutMgrCmdCh:        make(MsgChannel),
		storageMgrCmdCh:    make(MsgChannel),
		tkCmdCh:            make(MsgChannel),
		adminMgrCmdCh:      make(MsgChannel),
		compactMgrCmdCh:    make(MsgChannel),
		clustMgrAgentCmdCh: make(MsgChannel),
		kvSenderCmdCh:      make(MsgChannel),
		cbqBridgeCmdCh:     make(MsgChannel),
		settingsMgrCmdCh:   make(MsgChannel),
		statsMgrCmdCh:      make(MsgChannel),
		scanCoordCmdCh:     make(MsgChannel),

		mutMgrExitCh: make(MsgChannel),

		indexInstMap:  make(common.IndexInstMap),
		indexPartnMap: make(IndexPartnMap),

		streamBucketStatus:           make(map[common.StreamId]BucketStatus),
		streamBucketFlushInProgress:  make(map[common.StreamId]BucketFlushInProgressMap),
		streamBucketObserveFlushDone: make(map[common.StreamId]BucketObserveFlushDoneMap),
		streamBucketRequestStopCh:    make(map[common.StreamId]BucketRequestStopCh),
		streamBucketRollbackTs:       make(map[common.StreamId]BucketRollbackTs),
		bucketsInCatchup:             make(map[string]bool),
		bucketCreateClientChMap:      make(map[string]MsgChannel),
		config:                       config,
	}

	common.Infof("Indexer::NewIndexer Status INIT")

	common.Infof("Indexer::NewIndexer Starting with Vbuckets %v", idx.config["numVbuckets"].Int())

	var res Message
	idx.settingsMgr, idx.config, res = NewSettingsManager(idx.settingsMgrCmdCh, idx.wrkrRecvCh, config)
	if res.GetMsgType() != MSG_SUCCESS {
		common.Errorf("Indexer::NewIndexer settingsMgr Init Error", res)
		return nil, res
	}

	idx.initStreamAddressMap()
	idx.initStreamFlushMap()

	//Start Mutation Manager
	idx.mutMgr, res = NewMutationManager(idx.mutMgrCmdCh, idx.wrkrRecvCh, idx.config)
	if res.GetMsgType() != MSG_SUCCESS {
		common.Errorf("Indexer::NewIndexer Mutation Manager Init Error", res)
		return nil, res
	}

	//Start KV Sender
	idx.kvSender, res = NewKVSender(idx.kvSenderCmdCh, idx.wrkrRecvCh, idx.config)
	if res.GetMsgType() != MSG_SUCCESS {
		common.Errorf("Indexer::NewIndexer KVSender Init Error", res)
		return nil, res
	}

	//Start Timekeeper
	idx.tk, res = NewTimekeeper(idx.tkCmdCh, idx.wrkrRecvCh, idx.config)
	if res.GetMsgType() != MSG_SUCCESS {
		common.Errorf("Indexer::NewIndexer Timekeeper Init Error", res)
		return nil, res
	}

	//Start Scan Coordinator
	idx.scanCoord, res = NewScanCoordinator(idx.scanCoordCmdCh, idx.wrkrRecvCh, idx.config)
	if res.GetMsgType() != MSG_SUCCESS {
		common.Errorf("Indexer::NewIndexer Scan Coordinator Init Error", res)
		return nil, res
	}

	idx.enableManager = idx.config["enableManager"].Bool()

	if idx.enableManager {
		idx.clustMgrAgent, res = NewClustMgrAgent(idx.clustMgrAgentCmdCh, idx.adminRecvCh, config)
		if res.GetMsgType() != MSG_SUCCESS {
			common.Errorf("Indexer::NewIndexer ClusterMgrAgent Init Error", res)
			return nil, res
		}
	}

	//read persisted indexer state
	if err := idx.bootstrap(); err != nil {
		common.Fatalf("Indexer::Unable to Bootstrap Indexer from Persisted Metadata.")
		return nil, &MsgError{err: Error{cause: err}}
	}

	idx.statsMgr, res = NewStatsManager(idx.statsMgrCmdCh, idx.wrkrRecvCh, config)
	if res.GetMsgType() != MSG_SUCCESS {
		common.Errorf("Indexer::NewIndexer statsMgr Init Error", res)
		return nil, res
	}

	if !idx.enableManager {
		//Start CbqBridge
		idx.cbqBridge, res = NewCbqBridge(idx.cbqBridgeCmdCh, idx.adminRecvCh, idx.indexInstMap, idx.config)
		if res.GetMsgType() != MSG_SUCCESS {
			common.Errorf("Indexer::NewIndexer CbqBridge Init Error", res)
			return nil, res
		}
	}

	//Register with Index Coordinator
	if err := idx.registerWithCoordinator(); err != nil {
		//log error and exit
	}

	//sync topology
	if err := idx.syncTopologyWithCoordinator(); err != nil {
		//log error and exit
	}

	//Start Admin port listener
	idx.adminMgr, res = NewAdminManager(idx.adminMgrCmdCh, idx.adminRecvCh)
	if res.GetMsgType() != MSG_SUCCESS {
		common.Errorf("Indexer::NewIndexer Admin Manager Init Error", res)
		return nil, res
	}

	common.Infof("Indexer::NewIndexer Status ACTIVE")

	idx.compactMgr, res = NewCompactionManager(idx.compactMgrCmdCh, idx.wrkrRecvCh, idx.config)
	if res.GetMsgType() != MSG_SUCCESS {
		common.Errorf("Indexer::NewCompactionmanager Init Error", res)
		return nil, res
	}

	// Setup http server
	go func() {
		addr := net.JoinHostPort("", idx.config["httpPort"].String())
		if err := http.ListenAndServe(addr, nil); err != nil {
			common.Errorf("indexer:: Error Starting Http Server: %v", err)
			common.CrashOnError(err)
		}
	}()

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

		//fwd the message to timekeeper
		idx.tkCmdCh <- msg
		<-idx.tkCmdCh

	case STREAM_READER_STREAM_END:

		//fwd the message to timekeeper
		idx.tkCmdCh <- msg
		<-idx.tkCmdCh

	case STREAM_READER_STREAM_DROP_DATA:
		//TODO
		common.Debugf("Indexer::handleWorkerMsgs Received Drop Data "+
			"From Mutation Mgr %v", msg)

	case STREAM_READER_SNAPSHOT_MARKER:
		//fwd the message to timekeeper
		idx.tkCmdCh <- msg
		<-idx.tkCmdCh

	case STREAM_READER_CONN_ERROR:

		//fwd the message to timekeeper
		idx.tkCmdCh <- msg
		<-idx.tkCmdCh

	case TK_STABILITY_TIMESTAMP:
		//send TS to Mutation Manager
		ts := msg.(*MsgTKStabilityTS).GetTimestamp()
		bucket := msg.(*MsgTKStabilityTS).GetBucket()
		streamId := msg.(*MsgTKStabilityTS).GetStreamId()

		idx.streamBucketFlushInProgress[streamId][bucket] = true

		idx.mutMgrCmdCh <- &MsgMutMgrFlushMutationQueue{
			mType:    MUT_MGR_PERSIST_MUTATION_QUEUE,
			bucket:   bucket,
			ts:       ts,
			streamId: streamId}

		<-idx.mutMgrCmdCh

	case MUT_MGR_ABORT_PERSIST:

		idx.mutMgrCmdCh <- msg
		<-idx.mutMgrCmdCh

	case MUT_MGR_FLUSH_DONE, MUT_MGR_ABORT_DONE:

		bucket := msg.(*MsgMutMgrFlushDone).GetBucket()
		streamId := msg.(*MsgMutMgrFlushDone).GetStreamId()

		//fwd the message to storage manager
		idx.storageMgrCmdCh <- msg
		<-idx.storageMgrCmdCh

		idx.streamBucketFlushInProgress[streamId][bucket] = false

		//if there is any observer for flush done, notify
		idx.notifyFlushObserver(msg)

		//fwd the message to timekeeper
		idx.tkCmdCh <- msg
		<-idx.tkCmdCh

	case TK_INIT_BUILD_DONE:
		idx.handleInitialBuildDone(msg)

	case TK_MERGE_STREAM:
		idx.handleMergeStream(msg)

	case INDEXER_PREPARE_RECOVERY:
		idx.handlePrepareRecovery(msg)

	case INDEXER_INITIATE_RECOVERY:
		idx.handleInitRecovery(msg)

	case STORAGE_INDEX_SNAP_REQUEST,
		STORAGE_INDEX_STORAGE_STATS,
		STORAGE_INDEX_COMPACT:
		idx.storageMgrCmdCh <- msg
		<-idx.storageMgrCmdCh

	case INDEXER_ROLLBACK:
		idx.handleRollback(msg)

	case CONFIG_SETTINGS_UPDATE:
		cfgUpdate := msg.(*MsgConfigUpdate)
		newConfig := cfgUpdate.GetConfig()
		if newConfig["settings.memory_quota"].Uint64() !=
			idx.config["settings.memory_quota"].Uint64() {
			idx.needsRestart = true
		}
		idx.config = newConfig
		idx.compactMgrCmdCh <- msg
		<-idx.compactMgrCmdCh
		idx.tkCmdCh <- msg
		<-idx.tkCmdCh

	case INDEXER_PREPARE_DONE:
		idx.handlePrepareDone(msg)

	case INDEXER_RECOVERY_DONE:
		idx.handleRecoveryDone(msg)

	case KV_STREAM_REPAIR:
		idx.handleKVStreamRepair(msg)

	case TK_INIT_BUILD_DONE_ACK:
		idx.handleInitBuildDoneAck(msg)

	case TK_MERGE_STREAM_ACK:
		idx.handleMergeStreamAck(msg)

	case STREAM_REQUEST_DONE:
		idx.handleStreamRequestDone(msg)

	case KV_SENDER_RESTART_VBUCKETS:

		//fwd the message to kv_sender
		idx.sendMsgToKVSender(msg)

	case STORAGE_STATS:
		idx.storageMgrCmdCh <- msg
		<-idx.storageMgrCmdCh

	case SCAN_STATS:
		idx.scanCoordCmdCh <- msg
		<-idx.scanCoordCmdCh

	case INDEX_PROGRESS_STATS:
		idx.tkCmdCh <- msg
		<-idx.tkCmdCh

	case INDEXER_BUCKET_NOT_FOUND:
		idx.handleBucketNotFound(msg)

	case INDEXER_STATS:
		idx.handleStats(msg)

	case MSG_ERROR:
		//crash for all errors by default
		common.Fatalf("Indexer::handleWorkerMsgs Fatal Error On Worker Channel %+v", msg)
		err := msg.(*MsgError).GetError()
		common.CrashOnError(err.cause)

	default:
		common.Errorf("Indexer::handleWorkerMsgs Unknown Message %+v", msg)
		common.CrashOnError(errors.New("Unknown Msg On Worker Channel"))
	}

}

func (idx *indexer) handleAdminMsgs(msg Message) {

	switch msg.GetMsgType() {

	case CLUST_MGR_CREATE_INDEX_DDL,
		CBQ_CREATE_INDEX_DDL:

		idx.handleCreateIndex(msg)

	case CLUST_MGR_BUILD_INDEX_DDL:
		idx.handleBuildIndex(msg)

	case CLUST_MGR_DROP_INDEX_DDL,
		CBQ_DROP_INDEX_DDL:

		idx.handleDropIndex(msg)

	case MSG_ERROR:

		common.Fatalf("Indexer::handleAdminMsgs Fatal Error On Admin Channel %+v", msg)
		err := msg.(*MsgError).GetError()
		common.CrashOnError(err.cause)

	default:
		common.Errorf("Indexer::handleAdminMsgs Unknown Message %+v", msg)
		common.CrashOnError(errors.New("Unknown Msg On Admin Channel"))

	}

}

//TODO handle panic, otherwise main loop will get shutdown
func (idx *indexer) handleCreateIndex(msg Message) {

	indexInst := msg.(*MsgCreateIndex).GetIndexInst()
	clientCh := msg.(*MsgCreateIndex).GetResponseChannel()

	common.Infof("Indexer::handleCreateIndex %v", indexInst)

	if !ValidateBucket(idx.config["clusterAddr"].String(), indexInst.Defn.Bucket) {
		common.Errorf("Indexer::handleCreateIndex \n\t Bucket %v Not Found")

		if clientCh != nil {
			clientCh <- &MsgError{
				err: Error{code: ERROR_INDEXER_UNKNOWN_BUCKET,
					severity: FATAL,
					cause:    ErrUnknownBucket,
					category: INDEXER}}

		}
		return
	}

	if idx.streamBucketStatus[common.INIT_STREAM][indexInst.Defn.Bucket] == STREAM_RECOVERY ||
		idx.streamBucketStatus[common.MAINT_STREAM][indexInst.Defn.Bucket] == STREAM_RECOVERY {
		common.Errorf("Indexer::handleCreateIndex \n\tCannot Process Create Index " +
			"In Recovery Mode.")

		if clientCh != nil {
			clientCh <- &MsgError{
				err: Error{code: ERROR_INDEXER_IN_RECOVERY,
					severity: FATAL,
					cause:    ErrIndexerInRecovery,
					category: INDEXER}}

		}
		return
	}

	//check if this is duplicate index instance
	if ok := idx.checkDuplicateIndex(indexInst, clientCh); !ok {
		return
	}

	//allocate partition/slice
	var partnInstMap PartitionInstMap
	var err error
	if partnInstMap, err = idx.initPartnInstance(indexInst, clientCh); err != nil {
		return
	}

	//update index maps with this index
	idx.indexInstMap[indexInst.InstId] = indexInst
	idx.indexPartnMap[indexInst.InstId] = partnInstMap

	msgUpdateIndexInstMap := &MsgUpdateInstMap{indexInstMap: idx.indexInstMap}
	msgUpdateIndexPartnMap := &MsgUpdatePartnMap{indexPartnMap: idx.indexPartnMap}

	if err := idx.distributeIndexMapsToWorkers(msgUpdateIndexInstMap, msgUpdateIndexPartnMap); err != nil {
		if clientCh != nil {
			clientCh <- &MsgError{
				err: Error{code: ERROR_INDEXER_INTERNAL_ERROR,
					severity: FATAL,
					cause:    err,
					category: INDEXER}}
		}
		common.CrashOnError(err)
	}

	if idx.enableManager {
		clientCh <- &MsgSuccess{}
	} else {
		//for cbq bridge, simulate build index
		idx.handleBuildIndex(&MsgBuildIndex{indexInstList: []common.IndexInstId{indexInst.InstId},
			respCh: clientCh})
	}

}

func (idx *indexer) handleBuildIndex(msg Message) {

	instIdList := msg.(*MsgBuildIndex).GetIndexList()
	clientCh := msg.(*MsgBuildIndex).GetRespCh()

	common.Infof("Indexer::handleBuildIndex %v", instIdList)

	if len(instIdList) == 0 {
		common.Warnf("Indexer::handleBuildIndex Nothing To Build")
		if clientCh != nil {
			clientCh <- &MsgSuccess{}
		}
	}

	bucketIndexList := idx.groupIndexListByBucket(instIdList)

	initialBuildReqd := true
	for bucket, instIdList := range bucketIndexList {

		if ok := idx.checkValidIndexInst(bucket, instIdList, clientCh); !ok {
			common.Errorf("Indexer::handleBuildIndex \n\tInvalid Index List "+
				"Bucket %v. IndexList %v", bucket, instIdList)
			if idx.enableManager {
				delete(bucketIndexList, bucket)
				continue
			} else {
				return
			}
		}

		if !idx.checkBucketExists(bucket, instIdList, clientCh) {
			common.Errorf("Indexer::handleBuildIndex \n\tCannot Process Build Index."+
				"Unknown Bucket %v.", bucket)
			if idx.enableManager {
				delete(bucketIndexList, bucket)
				continue
			} else {
				return
			}
		}

		if ok := idx.checkBucketInRecovery(bucket, instIdList, clientCh); ok {
			common.Errorf("Indexer::handleBuildIndex \n\tCannot Process Build Index "+
				"In Recovery Mode. Bucket %v. IndexList %v", bucket, instIdList)
			if idx.enableManager {
				delete(bucketIndexList, bucket)
				continue
			} else {
				return
			}
		}

		//check if Initial Build is already running for this index's bucket
		if ok := idx.checkDuplicateInitialBuildRequest(bucket, instIdList, clientCh); !ok {
			common.Errorf("Indexer::handleBuildIndex \n\tBuild Already In"+
				"Progress. Bucket %v.", bucket)
			if idx.enableManager {
				delete(bucketIndexList, bucket)
				continue
			} else {
				return
			}
		}

		//get current timestamp from KV and set it as Initial Build Timestamp
		buildTs, err := GetCurrentKVTs(idx.config["clusterAddr"].String(),
			bucket,
			idx.config["numVbuckets"].Int())

		if err != nil {
			errStr := fmt.Sprintf("Error Connecting KV %v Err %v",
				idx.config["clusterAddr"].String(), err)
			common.Errorf("Indexer::handleBuildIndex %v", errStr)
			if idx.enableManager {
				idx.bulkUpdateError(instIdList, errStr)
				if err := idx.updateMetaInfoForIndexList(instIdList, false, false, true); err != nil {
					common.CrashOnError(err)
				}
				delete(bucketIndexList, bucket)
				continue
			} else if clientCh != nil {
				clientCh <- &MsgError{
					err: Error{code: ERROR_INDEXER_IN_RECOVERY,
						severity: FATAL,
						cause:    errors.New(errStr),
						category: INDEXER}}
				return
			}
		}

		//if there is already an index for this bucket in MAINT_STREAM,
		//add this index to INIT_STREAM
		var buildStream common.StreamId
		if idx.checkBucketExistsInStream(bucket, common.MAINT_STREAM) {
			buildStream = common.INIT_STREAM
		} else {
			buildStream = common.MAINT_STREAM
		}

		idx.bulkUpdateStream(instIdList, buildStream)

		//if initial build TS is zero and index belongs to MAINT_STREAM
		//initial build is not required.
		var buildState common.IndexState
		if buildTs.IsZeroTs() && buildStream == common.MAINT_STREAM {
			buildState = common.INDEX_STATE_ACTIVE
			initialBuildReqd = false
		} else {
			buildState = common.INDEX_STATE_INITIAL
		}

		idx.bulkUpdateState(instIdList, buildState)

		common.Debugf("Indexer::handleBuildIndex \n\tAdded Index: %v to Stream: %v State: %v",
			instIdList, buildStream, buildState)

		msgUpdateIndexInstMap := &MsgUpdateInstMap{indexInstMap: idx.indexInstMap}

		if err := idx.distributeIndexMapsToWorkers(msgUpdateIndexInstMap, nil); err != nil {
			if clientCh != nil {
				clientCh <- &MsgError{
					err: Error{code: ERROR_INDEXER_INTERNAL_ERROR,
						severity: FATAL,
						cause:    err,
						category: INDEXER}}
			}
			common.CrashOnError(err)
		}

		//send Stream Update to workers
		idx.sendStreamUpdateForBuildIndex(instIdList, buildStream, bucket, buildTs, clientCh)

		if _, ok := idx.streamBucketStatus[buildStream][bucket]; !ok {
			idx.streamBucketStatus[buildStream] = make(BucketStatus)
		}
		idx.streamBucketStatus[buildStream][bucket] = STREAM_ACTIVE

		//store updated state and streamId in meta store
		if idx.enableManager {
			if err := idx.updateMetaInfoForIndexList(instIdList, true, true, false); err != nil {
				common.CrashOnError(err)
			}
		} else {

			//if initial build is not being done, send success response,
			//otherwise success response will be sent when initial build gets done
			if !initialBuildReqd {
				clientCh <- &MsgSuccess{}
			} else {
				idx.bucketCreateClientChMap[bucket] = clientCh
			}
			return
		}
	}

	clientCh <- &MsgSuccess{}

}

//TODO handle panic, otherwise main loop will get shutdown
func (idx *indexer) handleDropIndex(msg Message) {

	indexInstId := msg.(*MsgDropIndex).GetIndexInstId()
	clientCh := msg.(*MsgDropIndex).GetResponseChannel()

	common.Debugf("Indexer::handleDropIndex - IndexInstId %v", indexInstId)

	var indexInst common.IndexInst
	var ok bool
	if indexInst, ok = idx.indexInstMap[indexInstId]; !ok {

		errStr := fmt.Sprintf("Unknown Index Instance %v", indexInstId)
		common.Errorf("Indexer::handleDropIndex %v", errStr)

		if clientCh != nil {
			clientCh <- &MsgError{
				err: Error{code: ERROR_INDEXER_UNKNOWN_INDEX,
					severity: FATAL,
					cause:    errors.New(errStr),
					category: INDEXER}}
		}
		return
	}

	//if the index state is Created/Ready/Deleted, only data cleanup is
	//required. No stream updates are required.
	if indexInst.State == common.INDEX_STATE_CREATED ||
		indexInst.State == common.INDEX_STATE_READY ||
		indexInst.State == common.INDEX_STATE_DELETED {

		idx.cleanupIndexData(indexInst, clientCh)
		common.Debugf("Indexer::handleDropIndex Cleanup Successful for "+
			"Index Data %v", indexInst)
		clientCh <- &MsgSuccess{}
		return
	}

	//Drop is a two step process. First set the index state as DELETED.
	//Then all the workers are notified about this state change. If this
	//step is successful, no mutation/scan request for the index will be processed.
	//Second step, is the actual cleanup of index instance from internal maps
	//and purging of physical slice files.

	indexInst.State = common.INDEX_STATE_DELETED
	idx.indexInstMap[indexInst.InstId] = indexInst

	msgUpdateIndexInstMap := &MsgUpdateInstMap{indexInstMap: idx.indexInstMap}

	if err := idx.distributeIndexMapsToWorkers(msgUpdateIndexInstMap, nil); err != nil {
		clientCh <- &MsgError{
			err: Error{code: ERROR_INDEXER_INTERNAL_ERROR,
				severity: FATAL,
				cause:    err,
				category: INDEXER}}
		common.CrashOnError(err)
	}

	if idx.streamBucketStatus[common.MAINT_STREAM][indexInst.Defn.Bucket] == STREAM_RECOVERY ||
		idx.streamBucketStatus[common.INIT_STREAM][indexInst.Defn.Bucket] == STREAM_RECOVERY {

		common.Errorf("Indexer::handleDropIndex Cannot Process Drop Index " +
			"In Recovery Mode.")

		if clientCh != nil {
			clientCh <- &MsgError{
				err: Error{code: ERROR_INDEXER_IN_RECOVERY,
					severity: FATAL,
					cause:    ErrIndexerInRecovery,
					category: INDEXER}}

		}
		return
	}

	//check if there is already a drop request waiting on this bucket
	if ok := idx.checkDuplicateDropRequest(indexInst, clientCh); ok {
		return
	}

	//if there is a flush in progress for this index's bucket and stream
	//wait for the flush to finish before drop
	streamId := indexInst.Stream
	bucket := indexInst.Defn.Bucket

	if ok, _ := idx.streamBucketFlushInProgress[streamId][bucket]; ok {
		notifyCh := make(MsgChannel)
		idx.streamBucketObserveFlushDone[streamId][bucket] = notifyCh
		go idx.processDropAfterFlushDone(indexInst, notifyCh, clientCh)
	} else {
		idx.cleanupIndex(indexInst, clientCh)
	}

}

func (idx *indexer) handleRollback(msg Message) {

	bucket := msg.(*MsgRollback).GetBucket()
	streamId := msg.(*MsgRollback).GetStreamId()
	rollbackTs := msg.(*MsgRollback).GetRollbackTs()

	common.Debugf("Indexer::handleRollback StreamId %v Bucket %v",
		streamId, bucket)

	if _, ok := idx.streamBucketRollbackTs[streamId]; ok {
		idx.streamBucketRollbackTs[streamId][bucket] = rollbackTs
	} else {
		bucketRollbackTs := make(BucketRollbackTs)
		bucketRollbackTs[bucket] = rollbackTs
		idx.streamBucketRollbackTs[streamId] = bucketRollbackTs
	}

	idx.streamBucketStatus[streamId][bucket] = STREAM_RECOVERY

	idx.stopBucketStream(streamId, bucket)

}

func (idx *indexer) handlePrepareRecovery(msg Message) {

	streamId := msg.(*MsgRecovery).GetStreamId()
	bucket := msg.(*MsgRecovery).GetBucket()

	common.Debugf("Indexer::handlePrepareRecovery StreamId %v Bucket %v",
		streamId, bucket)

	idx.stopBucketStream(streamId, bucket)

}

func (idx *indexer) handlePrepareDone(msg Message) {

	bucket := msg.(*MsgRecovery).GetBucket()
	streamId := msg.(*MsgRecovery).GetStreamId()

	common.Debugf("Indexer::handlePrepareDone StreamId %v Bucket %v",
		streamId, bucket)

	delete(idx.streamBucketRequestStopCh[streamId], bucket)

	//if there is a rollbackTs, process rollback
	if ts, ok := idx.streamBucketRollbackTs[streamId][bucket]; ok && ts != nil {
		restartTs, err := idx.processRollback(streamId, bucket, ts)
		if err != nil {
			common.CrashOnError(err)
		}
		idx.startBucketStream(streamId, bucket, restartTs)
	} else {
		//fwd the msg to timekeeper
		idx.tkCmdCh <- msg
		<-idx.tkCmdCh
	}

}

func (idx *indexer) handleInitRecovery(msg Message) {

	streamId := msg.(*MsgRecovery).GetStreamId()
	bucket := msg.(*MsgRecovery).GetBucket()
	restartTs := msg.(*MsgRecovery).GetRestartTs()

	common.Debugf("Indexer::handleInitRecovery StreamId %v Bucket %v",
		streamId, bucket)

	idx.startBucketStream(streamId, bucket, restartTs)

}

func (idx *indexer) handleRecoveryDone(msg Message) {

	bucket := msg.(*MsgRecovery).GetBucket()
	streamId := msg.(*MsgRecovery).GetStreamId()

	common.Debugf("Indexer::handleRecoveryDone StreamId %v Bucket %v",
		streamId, bucket)

	delete(idx.streamBucketRequestStopCh[streamId], bucket)
	delete(idx.streamBucketRollbackTs[streamId], bucket)

	//change status to Active
	idx.streamBucketStatus[streamId][bucket] = STREAM_ACTIVE

}

func (idx *indexer) handleKVStreamRepair(msg Message) {

	bucket := msg.(*MsgKVStreamRepair).GetBucket()
	streamId := msg.(*MsgKVStreamRepair).GetStreamId()
	restartTs := msg.(*MsgKVStreamRepair).GetRestartTs()

	//if there is already a repair in progress for this bucket stream
	//ignore the request
	if idx.checkStreamRequestPending(streamId, bucket) == false {
		common.Debugf("Indexer::handleKVStreamRepair Initiate Stream Repair %v Bucket %v", streamId, bucket)
		idx.startBucketStream(streamId, bucket, restartTs)
	} else {
		common.Debugf("Indexer::handleKVStreamRepair Ignore Stream Repair Request for Stream "+
			"%v Bucket %v. Request In Progress.", streamId, bucket)
	}

}

func (idx *indexer) handleInitBuildDoneAck(msg Message) {

	streamId := msg.(*MsgTKInitBuildDone).GetStreamId()
	bucket := msg.(*MsgTKInitBuildDone).GetBucket()

	common.Debugf("Indexer::handleInitBuildDoneAck StreamId %v Bucket %v",
		streamId, bucket)

	switch streamId {

	case common.INIT_STREAM:

		delete(idx.streamBucketRequestStopCh[streamId], bucket)

		//send the ack to timekeeper
		idx.tkCmdCh <- msg
		<-idx.tkCmdCh

	default:
		common.Debugf("Indexer::handleInitBuildDoneAck Unexpected Initial Build Ack Done "+
			"Received for Stream %v Bucket %v", streamId, bucket)
		common.CrashOnError(errors.New("Unexpected Initial Build Ack Done"))
	}

}

func (idx *indexer) handleMergeStreamAck(msg Message) {

	streamId := msg.(*MsgTKMergeStream).GetStreamId()
	bucket := msg.(*MsgTKMergeStream).GetBucket()

	common.Debugf("Indexer::handleMergeStreamAck StreamId %v Bucket %v",
		streamId, bucket)

	switch streamId {

	case common.INIT_STREAM:
		delete(idx.streamBucketRequestStopCh[streamId], bucket)

		idx.streamBucketStatus[streamId][bucket] = STREAM_INACTIVE

		//enable flush for this bucket in MAINT_STREAM
		//TODO shall this be moved to timekeeper now?
		idx.tkCmdCh <- &MsgTKToggleFlush{mType: TK_ENABLE_FLUSH,
			streamId: common.MAINT_STREAM,
			bucket:   bucket}
		<-idx.tkCmdCh

		//send the ack to timekeeper
		idx.tkCmdCh <- msg
		<-idx.tkCmdCh

		//for cbq bridge, return response after merge is done and
		//index is ready to query
		if !idx.enableManager {
			if clientCh, ok := idx.bucketCreateClientChMap[bucket]; ok {
				if clientCh != nil {
					clientCh <- &MsgSuccess{}
				}
				delete(idx.bucketCreateClientChMap, bucket)
			}
		} else {
			if err := idx.updateMetaInfoForBucket(bucket, true, true, false); err != nil {
				common.CrashOnError(err)
			}
		}

	default:
		common.Debugf("Indexer::handleMergeStreamAck Unexpected Initial Build Ack Done "+
			"Received for Stream %v Bucket %v", streamId, bucket)
		common.CrashOnError(errors.New("Unexpected Merge Stream Ack"))
	}

}

func (idx *indexer) handleStreamRequestDone(msg Message) {

	streamId := msg.(*MsgStreamInfo).GetStreamId()
	bucket := msg.(*MsgStreamInfo).GetBucket()

	common.Debugf("Indexer::handleStreamRequestDone StreamId %v Bucket %v",
		streamId, bucket)

	//send the ack to timekeeper
	idx.tkCmdCh <- msg
	<-idx.tkCmdCh

	delete(idx.streamBucketRequestStopCh[streamId], bucket)
}

func (idx *indexer) handleBucketNotFound(msg Message) {

	streamId := msg.(*MsgRecovery).GetStreamId()
	bucket := msg.(*MsgRecovery).GetBucket()

	common.Debugf("Indexer::handleBucketNotFound StreamId %v Bucket %v",
		streamId, bucket)

	var instIdList []common.IndexInstId
	for _, index := range idx.indexInstMap {
		if index.Stream == streamId &&
			index.Defn.Bucket == bucket {
			instIdList = append(instIdList, index.InstId)
		}
	}

	idx.bulkUpdateState(instIdList, common.INDEX_STATE_DELETED)
	common.Debugf("Indexer::handleBucketNotFound Updated Index State to DELETED %v",
		instIdList)

	msgUpdateIndexInstMap := &MsgUpdateInstMap{indexInstMap: idx.indexInstMap}

	if err := idx.distributeIndexMapsToWorkers(msgUpdateIndexInstMap, nil); err != nil {
		common.CrashOnError(err)
	}

	if idx.enableManager {
		if err := idx.updateMetaInfoForIndexList(instIdList, true, false, false); err != nil {
			common.CrashOnError(err)
		}
	}

	//TODO cleanup streambucket internal maps

	idx.stopBucketStream(streamId, bucket)

	idx.streamBucketStatus[streamId][bucket] = STREAM_INACTIVE

}

func (idx *indexer) cleanupIndexData(indexInst common.IndexInst,
	clientCh MsgChannel) {

	indexInstId := indexInst.InstId
	idxPartnInfo := idx.indexPartnMap[indexInstId]

	//update internal maps
	delete(idx.indexInstMap, indexInstId)
	delete(idx.indexPartnMap, indexInstId)

	msgUpdateIndexInstMap := &MsgUpdateInstMap{indexInstMap: idx.indexInstMap}
	msgUpdateIndexPartnMap := &MsgUpdatePartnMap{indexPartnMap: idx.indexPartnMap}

	if err := idx.distributeIndexMapsToWorkers(msgUpdateIndexInstMap, msgUpdateIndexPartnMap); err != nil {
		clientCh <- &MsgError{
			err: Error{code: ERROR_INDEXER_INTERNAL_ERROR,
				severity: FATAL,
				cause:    err,
				category: INDEXER}}
		common.CrashOnError(err)
	}

	//for all partitions managed by this indexer
	for _, partnInst := range idxPartnInfo {
		sc := partnInst.Sc
		//close all the slices
		for _, slice := range sc.GetAllSlices() {
			slice.Close()
			//wipe the physical files
			slice.Destroy()
		}
	}

}

func (idx *indexer) cleanupIndex(indexInst common.IndexInst,
	clientCh MsgChannel) {

	idx.cleanupIndexData(indexInst, clientCh)

	//send Stream update to workers
	if ok := idx.sendStreamUpdateForDropIndex(indexInst, clientCh); !ok {
		return
	}

	clientCh <- &MsgSuccess{}
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

	if idx.enableManager {
		//shutdown cluster manager
		idx.clustMgrAgentCmdCh <- &MsgGeneral{mType: CLUST_MGR_AGENT_SHUTDOWN}
		<-idx.clustMgrAgentCmdCh
	}

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

func (idx *indexer) sendStreamUpdateForBuildIndex(instIdList []common.IndexInstId,
	buildStream common.StreamId, bucket string, buildTs Timestamp, clientCh MsgChannel) bool {

	var cmd Message
	var indexList []common.IndexInst
	for _, instId := range instIdList {
		indexInst := idx.indexInstMap[instId]
		indexList = append(indexList, indexInst)
	}

	respCh := make(MsgChannel)

	cmd = &MsgStreamUpdate{mType: OPEN_STREAM,
		streamId:  buildStream,
		bucket:    bucket,
		indexList: indexList,
		buildTs:   buildTs,
		respCh:    respCh,
		restartTs: nil}

	//send stream update to timekeeper
	if resp := idx.sendStreamUpdateToWorker(cmd, idx.tkCmdCh,
		"Timekeeper"); resp.GetMsgType() != MSG_SUCCESS {
		if clientCh != nil {
			clientCh <- resp
		}
		respErr := resp.(*MsgError).GetError()
		common.CrashOnError(respErr.cause)
	}

	//send stream update to mutation manager
	if resp := idx.sendStreamUpdateToWorker(cmd, idx.mutMgrCmdCh,
		"MutationMgr"); resp.GetMsgType() != MSG_SUCCESS {
		if clientCh != nil {
			clientCh <- resp
		}
		respErr := resp.(*MsgError).GetError()
		common.CrashOnError(respErr.cause)
	}

	stopCh := make(StopChannel)

	if _, ok := idx.streamBucketRequestStopCh[buildStream][bucket]; !ok {
		idx.streamBucketRequestStopCh[buildStream] = make(BucketRequestStopCh)
	}
	idx.streamBucketRequestStopCh[buildStream][bucket] = stopCh

	go func() {
	retryloop:
		for {
			if !ValidateBucket(idx.config["clusterAddr"].String(), bucket) {
				common.Errorf("Indexer::sendStreamUpdateForBuildIndex \n\tBucket Not Found "+
					"For Stream %v Bucket %v", buildStream, bucket)
				idx.internalRecvCh <- &MsgRecovery{mType: INDEXER_BUCKET_NOT_FOUND,
					streamId: buildStream,
					bucket:   bucket}
				break retryloop
			}
			idx.sendMsgToKVSender(cmd)

			if resp, ok := <-respCh; ok {

				switch resp.GetMsgType() {

				case MSG_SUCCESS:
					//delete stopCh to indicate this request is done
					//TODO Add a separate message for this as there is no lock now
					common.Debugf("Indexer::sendStreamUpdateForBuildIndex \n\tStream Request Success For "+
						"Stream %v Bucket %v.", buildStream, bucket)
					idx.internalRecvCh <- &MsgStreamInfo{mType: STREAM_REQUEST_DONE,
						streamId: buildStream,
						bucket:   bucket,
					}
					break retryloop

				case INDEXER_ROLLBACK:
					//an initial build request should never receive rollback message
					common.Errorf("Indexer::sendStreamUpdateForBuildIndex \n\tUnexpected Rollback from "+
						"Projector during Initial Stream Request %v", resp)
					common.CrashOnError(ErrKVRollbackForInitRequest)

				default:
					//log and retry for all other responses
					common.Errorf("Indexer::sendStreamUpdateForBuildIndex - Error from Projector %v", resp)

				}
			}
		}
	}()

	return true

}

func (idx *indexer) sendMsgToKVSender(cmd Message) {
	idx.kvlock.Lock()
	defer idx.kvlock.Unlock()

	//send stream update to kv sender
	idx.kvSenderCmdCh <- cmd
	<-idx.kvSenderCmdCh
}

func (idx *indexer) sendStreamUpdateToWorker(cmd Message, workerCmdCh MsgChannel,
	workerStr string) Message {

	//send message to worker
	workerCmdCh <- cmd
	if resp, ok := <-workerCmdCh; ok {
		if resp.GetMsgType() != MSG_SUCCESS {

			common.Errorf("Indexer::sendStreamUpdateToWorker - Error received from %v "+
				"processing Msg %v Err %v. Aborted.", workerStr, cmd, resp)

			return &MsgError{
				err: Error{code: ERROR_INDEXER_INTERNAL_ERROR,
					severity: FATAL,
					cause:    ErrInconsistentState,
					category: INDEXER}}
		}
	} else {
		common.Errorf("Indexer::sendStreamUpdateToWorker - Error communicating with %v "+
			"processing Msg %v Err %v. Aborted.", workerStr, cmd, resp)

		return &MsgError{
			err: Error{code: ERROR_INDEXER_INTERNAL_ERROR,
				severity: FATAL,
				cause:    ErrFatalComm,
				category: INDEXER}}
	}
	return &MsgSuccess{}
}

func (idx *indexer) sendStreamUpdateForDropIndex(indexInst common.IndexInst,
	clientCh MsgChannel) bool {

	var cmd Message
	var indexList []common.IndexInst
	indexList = append(indexList, indexInst)

	var indexStreamIds []common.StreamId

	//index in INIT_STREAM needs to be removed from MAINT_STREAM as well
	//if the state is CATCHUP
	switch indexInst.Stream {

	case common.MAINT_STREAM:
		indexStreamIds = append(indexStreamIds, common.MAINT_STREAM)

	case common.INIT_STREAM:
		indexStreamIds = append(indexStreamIds, common.INIT_STREAM)
		if indexInst.State == common.INDEX_STATE_CATCHUP {
			indexStreamIds = append(indexStreamIds, common.MAINT_STREAM)
		}

	default:
		common.Errorf("Indexer::sendStreamUpdateForDropIndex \n\t Unsupported StreamId %v", indexInst.Stream)
		common.CrashOnError(ErrInvalidStream)
	}

	for _, streamId := range indexStreamIds {

		respCh := make(MsgChannel)

		if idx.checkBucketExistsInStream(indexInst.Defn.Bucket, streamId) {

			cmd = &MsgStreamUpdate{mType: REMOVE_INDEX_LIST_FROM_STREAM,
				streamId:  streamId,
				indexList: indexList,
				respCh:    respCh}
		} else {
			if idx.checkLastBucketInStream(indexInst.Defn.Bucket, streamId) {
				//promote to close stream
				cmd = &MsgStreamUpdate{mType: CLOSE_STREAM,
					streamId: streamId,
					respCh:   respCh}

			} else {
				cmd = &MsgStreamUpdate{mType: REMOVE_BUCKET_FROM_STREAM,
					streamId: streamId,
					bucket:   indexInst.Defn.Bucket,
					respCh:   respCh}
			}
		}

		//send stream update to mutation manager
		if resp := idx.sendStreamUpdateToWorker(cmd, idx.mutMgrCmdCh,
			"MutationMgr"); resp.GetMsgType() != MSG_SUCCESS {
			if clientCh != nil {
				clientCh <- resp
			}
			respErr := resp.(*MsgError).GetError()
			common.CrashOnError(respErr.cause)
		}

		//send stream update to timekeeper
		if resp := idx.sendStreamUpdateToWorker(cmd, idx.tkCmdCh,
			"Timekeeper"); resp.GetMsgType() != MSG_SUCCESS {
			if clientCh != nil {
				clientCh <- resp
			}
			respErr := resp.(*MsgError).GetError()
			common.CrashOnError(respErr.cause)
		}

		go func() {
		retryloop:
			for {
				if !ValidateBucket(idx.config["clusterAddr"].String(), indexInst.Defn.Bucket) {
					common.Errorf("Indexer::sendStreamUpdateForDropIndex \n\tBucket Not Found "+
						"For Stream %v Bucket %v", streamId, indexInst.Defn.Bucket)
					idx.internalRecvCh <- &MsgRecovery{mType: INDEXER_BUCKET_NOT_FOUND,
						streamId: streamId,
						bucket:   indexInst.Defn.Bucket}
					break retryloop
				}

				idx.sendMsgToKVSender(cmd)

				if resp, ok := <-respCh; ok {

					switch resp.GetMsgType() {

					case MSG_SUCCESS:
						break retryloop

					default:
						//log and retry for all other responses
						common.Errorf("Indexer::sendStreamUpdateForDropIndex - Error from Projector %v", resp)

					}
				}
			}
		}()
	}

	return true

}

func (idx *indexer) initPartnInstance(indexInst common.IndexInst,
	respCh MsgChannel) (PartitionInstMap, error) {

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

		storage_dir := idx.config["storage_dir"].String()
		os.Mkdir(storage_dir, 0755)
		if _, e := os.Stat(storage_dir); e != nil {
			common.CrashOnError(e)
		}
		path := filepath.Join(storage_dir, IndexPath(&indexInst, SliceId(0)))
		//add a single slice per partition for now
		if slice, err := NewForestDBSlice(path,
			0, indexInst.Defn.DefnId, indexInst.InstId, idx.config); err == nil {
			partnInst.Sc.AddSlice(0, slice)
			common.Infof("Indexer::initPartnInstance Initialized Slice: \n\t Index: %v Slice: %v",
				indexInst.InstId, slice)

			partnInstMap[common.PartitionId(i)] = partnInst
		} else {
			common.Errorf("Indexer::initPartnInstance Error creating slice %v. Abort.",
				err)

			if respCh != nil {
				respCh <- &MsgError{
					err: Error{code: ERROR_INDEXER_INTERNAL_ERROR,
						severity: FATAL,
						cause:    errors.New("Indexer Internal Error"),
						category: INDEXER}}
				return nil, err
			}
		}
	}

	return partnInstMap, nil
}

func (idx *indexer) distributeIndexMapsToWorkers(msgUpdateIndexInstMap Message,
	msgUpdateIndexPartnMap Message) error {

	//update index map in storage manager
	if err := idx.sendUpdatedIndexMapToWorker(msgUpdateIndexInstMap, msgUpdateIndexPartnMap, idx.storageMgrCmdCh,
		"StorageMgr"); err != nil {
		return err
	}

	//update index map in mutation manager
	if err := idx.sendUpdatedIndexMapToWorker(msgUpdateIndexInstMap, msgUpdateIndexPartnMap, idx.mutMgrCmdCh,
		"MutationMgr"); err != nil {
		return err
	}

	//update index map in scan coordinator
	if err := idx.sendUpdatedIndexMapToWorker(msgUpdateIndexInstMap, msgUpdateIndexPartnMap, idx.scanCoordCmdCh,
		"ScanCoordinator"); err != nil {
		return err
	}

	if err := idx.sendUpdatedIndexMapToWorker(msgUpdateIndexInstMap, msgUpdateIndexPartnMap, idx.tkCmdCh,
		"Timekeeper"); err != nil {
		return err
	}
	return nil
}

func (idx *indexer) sendUpdatedIndexMapToWorker(msgUpdateIndexInstMap Message,
	msgUpdateIndexPartnMap Message, workerCmdCh chan Message, workerStr string) error {

	if msgUpdateIndexInstMap != nil {
		workerCmdCh <- msgUpdateIndexInstMap

		if resp, ok := <-workerCmdCh; ok {

			if resp.GetMsgType() == MSG_ERROR {
				common.Errorf("Indexer::sendUpdatedIndexMapToWorker - Error received from %v processing "+
					"Msg %v Err %v. Aborted.", workerStr, msgUpdateIndexInstMap, resp)
				respErr := resp.(*MsgError).GetError()
				return respErr.cause
			}
		} else {
			common.Errorf("Indexer::sendUpdatedIndexMapToWorker - Error communicating with %v "+
				"processing Msg %v. Aborted.", workerStr, msgUpdateIndexInstMap)
			return ErrFatalComm
		}
	}

	if msgUpdateIndexPartnMap != nil {
		workerCmdCh <- msgUpdateIndexPartnMap
		if resp, ok := <-workerCmdCh; ok {

			if resp.GetMsgType() == MSG_ERROR {
				common.Errorf("Indexer::sendUpdatedIndexMapToWorker - Error received from %v processing "+
					"Msg %v Err %v. Aborted.", workerStr, msgUpdateIndexPartnMap, resp)
				respErr := resp.(*MsgError).GetError()
				return respErr.cause
			}
		} else {
			common.Errorf("Indexer::sendUpdatedIndexMapToWorker - Error communicating with %v "+
				"processing Msg %v. Aborted.", workerStr, msgUpdateIndexPartnMap, resp)
			return ErrFatalComm
		}
	}

	return nil

}

func (idx *indexer) initStreamAddressMap() {
	StreamAddrMap = make(StreamAddressMap)

	port2addr := func(p string) string {
		return net.JoinHostPort("", idx.config[p].String())
	}

	StreamAddrMap[common.MAINT_STREAM] = common.Endpoint(port2addr("streamMaintPort"))
	StreamAddrMap[common.CATCHUP_STREAM] = common.Endpoint(port2addr("streamCatchupPort"))
	StreamAddrMap[common.INIT_STREAM] = common.Endpoint(port2addr("streamInitPort"))
}

func (idx *indexer) initStreamTopicName() {
	StreamTopicName = make(map[common.StreamId]string)

	StreamTopicName[common.MAINT_STREAM] = MAINT_TOPIC + "_" + idx.id
	StreamTopicName[common.CATCHUP_STREAM] = CATCHUP_TOPIC + "_" + idx.id
	StreamTopicName[common.INIT_STREAM] = INIT_TOPIC + "_" + idx.id
}

//checkDuplicateIndex checks if an index with the given indexInstId
// or name already exists
func (idx *indexer) checkDuplicateIndex(indexInst common.IndexInst,
	respCh MsgChannel) bool {

	//if the indexInstId already exists, return error
	if index, ok := idx.indexInstMap[indexInst.InstId]; ok {
		common.Errorf("Indexer::checkDuplicateIndex Duplicate Index Instance. "+
			"IndexInstId: %v, Index: %v", indexInst.InstId, index)

		if respCh != nil {
			respCh <- &MsgError{
				err: Error{code: ERROR_INDEX_ALREADY_EXISTS,
					severity: FATAL,
					cause:    errors.New("Duplicate Index Instance"),
					category: INDEXER}}
		}
		return false
	}

	//if the index name already exists for the same bucket,
	//return error
	for _, index := range idx.indexInstMap {

		if index.Defn.Name == indexInst.Defn.Name &&
			index.Defn.Bucket == indexInst.Defn.Bucket &&
			index.State != common.INDEX_STATE_DELETED {

			common.Errorf("Indexer::checkDuplicateIndex Duplicate Index Name. "+
				"Name: %v, Duplicate Index: %v", indexInst.Defn.Name, index)

			if respCh != nil {
				respCh <- &MsgError{
					err: Error{code: ERROR_INDEX_ALREADY_EXISTS,
						severity: FATAL,
						cause:    errors.New("Duplicate Index Name"),
						category: INDEXER}}
			}
			return false
		}

	}
	return true
}

//checkDuplicateInitialBuildRequest check if any other index on the given bucket
//is already building
func (idx *indexer) checkDuplicateInitialBuildRequest(bucket string,
	instIdList []common.IndexInstId, respCh MsgChannel) bool {

	//if initial build is already running for some other index on this bucket,
	//cannot start another one
	for _, index := range idx.indexInstMap {

		if ((index.State == common.INDEX_STATE_INITIAL ||
			index.State == common.INDEX_STATE_CATCHUP) &&
			index.Defn.Bucket == bucket) ||
			idx.checkStreamRequestPending(index.Stream, bucket) {

			errStr := fmt.Sprintf("Build Already In Progress. Bucket %v", bucket)
			if idx.enableManager {
				idx.bulkUpdateError(instIdList, errStr)
				if err := idx.updateMetaInfoForIndexList(instIdList, false, false, true); err != nil {
					common.CrashOnError(err)
				}
			} else if respCh != nil {
				respCh <- &MsgError{
					err: Error{code: ERROR_INDEX_BUILD_IN_PROGRESS,
						severity: FATAL,
						cause:    errors.New(errStr),
						category: INDEXER}}
			}
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

	common.Debugf("Indexer::handleInitialBuildDone Bucket: %v Stream: %v", bucket, streamId)

	//MAINT_STREAM should already be running for this bucket,
	//as first index gets added to MAINT_STREAM always
	if idx.checkBucketExistsInStream(bucket, common.MAINT_STREAM) == false {
		common.Errorf("Indexer::handleInitialBuildDone MAINT_STREAM not enabled for Bucket: %v. "+
			"Cannot Process Initial Build Done.", bucket)
		common.CrashOnError(ErrMaintStreamMissingBucket)
	}

	//get the list of indexes for this bucket and stream in INITIAL state
	var indexList []common.IndexInst
	var instIdList []common.IndexInstId
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
			instIdList = append(instIdList, index.InstId)
		}
	}

	//update the IndexInstMap
	for _, index := range indexList {
		idx.indexInstMap[index.InstId] = index
	}

	//send updated maps to all workers
	msgUpdateIndexInstMap := &MsgUpdateInstMap{indexInstMap: idx.indexInstMap}

	if err := idx.distributeIndexMapsToWorkers(msgUpdateIndexInstMap, nil); err != nil {
		common.CrashOnError(err)
	}

	//if index is already in MAINT_STREAM, nothing more needs to be done
	if streamId == common.MAINT_STREAM {

		//for cbq bridge, return response as index is ready to query
		if !idx.enableManager {
			if clientCh, ok := idx.bucketCreateClientChMap[bucket]; ok {
				if clientCh != nil {
					clientCh <- &MsgSuccess{}
				}
				delete(idx.bucketCreateClientChMap, bucket)
			}
		} else {
			if err := idx.updateMetaInfoForIndexList(instIdList, true, true, false); err != nil {
				common.CrashOnError(err)
			}
		}

		return
	}

	//Add index to MAINT_STREAM in Catchup State,
	//so mutations for this index are already in queue to
	//allow convergence with INIT_STREAM.
	respCh := make(MsgChannel)
	stopCh := make(StopChannel)

	cmd := &MsgStreamUpdate{mType: ADD_INDEX_LIST_TO_STREAM,
		streamId:  common.MAINT_STREAM,
		indexList: indexList,
		respCh:    respCh,
		stopCh:    stopCh}

	//send stream update to timekeeper
	if resp := idx.sendStreamUpdateToWorker(cmd, idx.tkCmdCh, "Timekeeper"); resp.GetMsgType() != MSG_SUCCESS {
		respErr := resp.(*MsgError).GetError()
		common.CrashOnError(respErr.cause)
	}

	//send stream update to mutation manager
	if resp := idx.sendStreamUpdateToWorker(cmd, idx.mutMgrCmdCh, "MutationMgr"); resp.GetMsgType() != MSG_SUCCESS {
		respErr := resp.(*MsgError).GetError()
		common.CrashOnError(respErr.cause)
	}

	if _, ok := idx.streamBucketRequestStopCh[streamId][bucket]; !ok {
		idx.streamBucketRequestStopCh[streamId] = make(BucketRequestStopCh)
	}
	idx.streamBucketRequestStopCh[streamId][bucket] = stopCh

	go func() {
	retryloop:
		for {
			if !ValidateBucket(idx.config["clusterAddr"].String(), bucket) {
				common.Errorf("Indexer::handleInitialBuildDone \n\tBucket Not Found "+
					"For Stream %v Bucket %v", streamId, bucket)
				break retryloop
			}
			idx.sendMsgToKVSender(cmd)

			if resp, ok := <-respCh; ok {

				switch resp.GetMsgType() {

				case MSG_SUCCESS:
					idx.internalRecvCh <- &MsgTKInitBuildDone{
						mType:    TK_INIT_BUILD_DONE_ACK,
						streamId: streamId,
						bucket:   bucket}
					break retryloop

				default:
					//log and retry for all other responses
					common.Errorf("Indexer::handleInitialBuildDone Stream %v Bucket %v \n\t"+
						"Error from Projector %v", resp)
				}
			}
		}
	}()

}

func (idx *indexer) handleMergeStream(msg Message) {

	bucket := msg.(*MsgTKMergeStream).GetBucket()
	streamId := msg.(*MsgTKMergeStream).GetStreamId()

	//MAINT_STREAM should already be running for this bucket,
	//as first index gets added to MAINT_STREAM always
	if idx.checkBucketExistsInStream(bucket, common.MAINT_STREAM) == false {
		common.Errorf("Indexer::handleMergeStream \n\tMAINT_STREAM not enabled for Bucket: %v ."+
			"Cannot Process Merge Stream", bucket)
		common.CrashOnError(ErrMaintStreamMissingBucket)
	}

	switch streamId {

	case common.INIT_STREAM:
		idx.handleMergeInitStream(msg)

	default:
		common.Errorf("Indexer::handleMergeStream \n\tOnly INIT_STREAM can be merged "+
			"to MAINT_STREAM. Found Stream: %v.", streamId)
		common.CrashOnError(ErrInvalidStream)
	}
}

//TODO If this function gets error before its finished, the state
//can be inconsistent. This needs to be fixed.
func (idx *indexer) handleMergeInitStream(msg Message) {

	bucket := msg.(*MsgTKMergeStream).GetBucket()
	streamId := msg.(*MsgTKMergeStream).GetStreamId()

	common.Debugf("Indexer::handleMergeInitStream Bucket: %v Stream: %v", bucket, streamId)

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

	//update the IndexInstMap
	for _, index := range indexList {
		idx.indexInstMap[index.InstId] = index
	}

	//send updated maps to all workers
	msgUpdateIndexInstMap := &MsgUpdateInstMap{indexInstMap: idx.indexInstMap}

	if err := idx.distributeIndexMapsToWorkers(msgUpdateIndexInstMap, nil); err != nil {
		common.CrashOnError(err)
	}

	respCh := make(MsgChannel)
	stopCh := make(StopChannel)

	//remove bucket from INIT_STREAM
	var cmd Message
	if idx.checkLastBucketInStream(bucket, streamId) {
		//promote to close stream
		cmd = &MsgStreamUpdate{mType: CLOSE_STREAM,
			streamId: streamId,
			respCh:   respCh,
			stopCh:   stopCh}
	} else {
		cmd = &MsgStreamUpdate{mType: REMOVE_BUCKET_FROM_STREAM,
			streamId: streamId,
			bucket:   bucket,
			respCh:   respCh,
			stopCh:   stopCh,
		}
	}

	//send stream update to timekeeper
	if resp := idx.sendStreamUpdateToWorker(cmd, idx.tkCmdCh, "Timekeeper"); resp.GetMsgType() != MSG_SUCCESS {
		respErr := resp.(*MsgError).GetError()
		common.CrashOnError(respErr.cause)
	}

	//send stream update to mutation manager
	if resp := idx.sendStreamUpdateToWorker(cmd, idx.mutMgrCmdCh, "MutationMgr"); resp.GetMsgType() != MSG_SUCCESS {
		respErr := resp.(*MsgError).GetError()
		common.CrashOnError(respErr.cause)
	}

	if _, ok := idx.streamBucketRequestStopCh[streamId][bucket]; !ok {
		idx.streamBucketRequestStopCh[streamId] = make(BucketRequestStopCh)
	}
	idx.streamBucketRequestStopCh[streamId][bucket] = stopCh

	go func() {
	retryloop:
		for {
			if !ValidateBucket(idx.config["clusterAddr"].String(), bucket) {
				common.Errorf("Indexer::handleMergeInitStream \n\tBucket Not Found "+
					"For Stream %v Bucket %v", streamId, bucket)
				break retryloop
			}
			idx.sendMsgToKVSender(cmd)

			if resp, ok := <-respCh; ok {

				switch resp.GetMsgType() {

				case MSG_SUCCESS:
					idx.internalRecvCh <- &MsgTKMergeStream{
						mType:    TK_MERGE_STREAM_ACK,
						streamId: streamId,
						bucket:   bucket}
					break retryloop

				default:
					//log and retry for all other responses
					common.Errorf("Indexer::handleMergeInitStream Stream %v Bucket %v \n\t"+
						"Error from Projector %v", resp)
				}
			}
		}
	}()

	common.Debugf("Indexer::handleMergeInitStream Merge Done Bucket: %v Stream: %v",
		bucket, streamId)
}

//checkBucketExistsInStream returns true if there is no index in the given stream
//which belongs to the given bucket, else false
func (idx *indexer) checkBucketExistsInStream(bucket string, streamId common.StreamId) bool {

	//check if any index of the given bucket is in the Stream
	for _, index := range idx.indexInstMap {

		if index.Defn.Bucket == bucket && index.Stream == streamId &&
			(index.State == common.INDEX_STATE_ACTIVE ||
				index.State == common.INDEX_STATE_CATCHUP ||
				index.State == common.INDEX_STATE_INITIAL) {
			return true
		}
	}

	return false

}

//checkLastBucketInStream returns true if the given bucket is the only bucket
//active in the given stream, else false
func (idx *indexer) checkLastBucketInStream(bucket string, streamId common.StreamId) bool {

	for _, index := range idx.indexInstMap {

		if index.Defn.Bucket != bucket && index.Stream == streamId &&
			(index.State == common.INDEX_STATE_ACTIVE ||
				index.State == common.INDEX_STATE_CATCHUP ||
				index.State == common.INDEX_STATE_INITIAL) {
			return false
		}
	}

	return true

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

func (idx *indexer) getIndexListForBucketAndStream(streamId common.StreamId,
	bucket string) []common.IndexInst {

	indexList := make([]common.IndexInst, 0)
	for _, idx := range idx.indexInstMap {

		if idx.Stream == streamId && idx.Defn.Bucket == bucket {

			indexList = append(indexList, idx)

		}
	}

	return indexList

}

func (idx *indexer) stopBucketStream(streamId common.StreamId, bucket string) {

	common.Debugf("Indexer::stopBucketStream Stream: %v Bucket %v", streamId, bucket)

	respCh := make(MsgChannel)
	stopCh := make(StopChannel)

	var cmd Message
	if idx.checkLastBucketInStream(bucket, streamId) {
		//promote to close stream
		cmd = &MsgStreamUpdate{mType: CLOSE_STREAM,
			streamId: streamId,
			respCh:   respCh,
			stopCh:   stopCh}
	} else {
		cmd = &MsgStreamUpdate{mType: REMOVE_BUCKET_FROM_STREAM,
			streamId: streamId,
			bucket:   bucket,
			respCh:   respCh,
			stopCh:   stopCh}
	}

	//send stream update to mutation manager
	if resp := idx.sendStreamUpdateToWorker(cmd, idx.mutMgrCmdCh,
		"MutationMgr"); resp.GetMsgType() != MSG_SUCCESS {
		respErr := resp.(*MsgError).GetError()
		common.CrashOnError(respErr.cause)
	}

	//send stream update to timekeeper
	if resp := idx.sendStreamUpdateToWorker(cmd, idx.tkCmdCh,
		"Timekeeper"); resp.GetMsgType() != MSG_SUCCESS {
		respErr := resp.(*MsgError).GetError()
		common.CrashOnError(respErr.cause)
	}

	if _, ok := idx.streamBucketRequestStopCh[streamId][bucket]; !ok {
		idx.streamBucketRequestStopCh[streamId] = make(BucketRequestStopCh)
	}
	idx.streamBucketRequestStopCh[streamId][bucket] = stopCh

	go func() {
	retryloop:
		for {

			idx.sendMsgToKVSender(cmd)

			if resp, ok := <-respCh; ok {

				switch resp.GetMsgType() {

				case MSG_SUCCESS:
					idx.internalRecvCh <- &MsgRecovery{mType: INDEXER_PREPARE_DONE,
						streamId: streamId,
						bucket:   bucket}
					break retryloop

				default:
					//log and retry for all other responses
					common.Errorf("Indexer::stopBucketStream Stream %v Bucket %v \n\t"+
						"Error from Projector %v", resp)

				}
			}
		}
	}()
}

func (idx *indexer) startBucketStream(streamId common.StreamId, bucket string,
	restartTs *common.TsVbuuid) {

	common.Debugf("Indexer::startBucketStream Stream: %v Bucket: %v RestartTS %v",
		streamId, bucket, restartTs)

	var indexList []common.IndexInst

	switch streamId {

	case common.MAINT_STREAM:

		for _, indexInst := range idx.indexInstMap {
			if indexInst.Defn.Bucket == bucket &&
				(indexInst.State == common.INDEX_STATE_ACTIVE ||
					indexInst.State == common.INDEX_STATE_CATCHUP) {
				indexList = append(indexList, indexInst)
			}
		}

	case common.INIT_STREAM:

		for _, indexInst := range idx.indexInstMap {
			if indexInst.Defn.Bucket == bucket &&
				indexInst.State == common.INDEX_STATE_INITIAL ||
				indexInst.State == common.INDEX_STATE_CATCHUP {
				indexList = append(indexList, indexInst)
			}
		}

	default:
		common.Errorf("Indexer::startBucketStream \n\t Unsupported StreamId %v", streamId)
		common.CrashOnError(ErrInvalidStream)

	}

	if len(indexList) == 0 {
		common.Debugf("Indexer::startBucketStream Nothing to Start. Stream: %v Bucket: %v",
			streamId, bucket)
		return
	}

	respCh := make(MsgChannel)
	stopCh := make(StopChannel)

	cmd := &MsgStreamUpdate{mType: OPEN_STREAM,
		streamId:  streamId,
		bucket:    bucket,
		indexList: indexList,
		restartTs: restartTs,
		respCh:    respCh,
		stopCh:    stopCh}

	//send stream update to timekeeper
	if resp := idx.sendStreamUpdateToWorker(cmd, idx.tkCmdCh,
		"Timekeeper"); resp.GetMsgType() != MSG_SUCCESS {
		respErr := resp.(*MsgError).GetError()
		common.CrashOnError(respErr.cause)
	}

	//send stream update to mutation manager
	if resp := idx.sendStreamUpdateToWorker(cmd, idx.mutMgrCmdCh,
		"MutationMgr"); resp.GetMsgType() != MSG_SUCCESS {
		respErr := resp.(*MsgError).GetError()
		common.CrashOnError(respErr.cause)
	}

	if _, ok := idx.streamBucketRequestStopCh[streamId][bucket]; !ok {
		idx.streamBucketRequestStopCh[streamId] = make(BucketRequestStopCh)
	}
	idx.streamBucketRequestStopCh[streamId][bucket] = stopCh

	idx.streamBucketStatus[streamId][bucket] = STREAM_RECOVERY

	go func() {
	retryloop:
		for {
			//validate bucket before every try
			if !ValidateBucket(idx.config["clusterAddr"].String(), bucket) {
				common.Errorf("Indexer::startBucketStream \n\tBucket Not Found "+
					"For Stream %v Bucket %v", streamId, bucket)
				idx.internalRecvCh <- &MsgRecovery{mType: INDEXER_BUCKET_NOT_FOUND,
					streamId: streamId,
					bucket:   bucket}
				break retryloop
			}

			idx.sendMsgToKVSender(cmd)

			if resp, ok := <-respCh; ok {

				switch resp.GetMsgType() {

				case MSG_SUCCESS:
					idx.internalRecvCh <- &MsgRecovery{mType: INDEXER_RECOVERY_DONE,
						streamId: streamId,
						bucket:   bucket}
					break retryloop

				case INDEXER_ROLLBACK:
					common.Infof("Indexer::startBucketStream \n\tRollback from "+
						"Projector For Stream %v Bucket %v", streamId, bucket)
					idx.internalRecvCh <- resp
					break retryloop

				default:
					//log and retry for all other responses
					common.Errorf("Indexer::startBucketStream Stream %v Bucket %v \n\t"+
						"Error from Projector %v. Retrying.", resp)
				}
			}
		}
	}()
}

func (idx *indexer) processRollback(streamId common.StreamId,
	bucket string, rollbackTs *common.TsVbuuid) (*common.TsVbuuid, error) {

	//send to storage manager to rollback
	msg := &MsgRollback{streamId: streamId,
		bucket:     bucket,
		rollbackTs: rollbackTs}

	idx.storageMgrCmdCh <- msg
	res := <-idx.storageMgrCmdCh

	if res.GetMsgType() != MSG_ERROR {
		rollbackTs := res.(*MsgRollback).GetRollbackTs()
		return rollbackTs, nil
	} else {
		common.Fatalf("Indexer::processRollback Error during Rollback %v", res)
		respErr := res.(*MsgError).GetError()
		return nil, respErr.cause
	}

}

//helper function to init streamFlush map for all streams
func (idx *indexer) initStreamFlushMap() {

	for i := 0; i < int(common.ALL_STREAMS); i++ {
		idx.streamBucketFlushInProgress[common.StreamId(i)] = make(BucketFlushInProgressMap)
		idx.streamBucketObserveFlushDone[common.StreamId(i)] = make(BucketObserveFlushDoneMap)
	}
}

func (idx *indexer) notifyFlushObserver(msg Message) {

	//if there is any observer for flush, notify
	bucket := msg.(*MsgMutMgrFlushDone).GetBucket()
	streamId := msg.(*MsgMutMgrFlushDone).GetStreamId()

	if notifyCh, ok := idx.streamBucketObserveFlushDone[streamId][bucket]; ok {
		if notifyCh != nil {
			notifyCh <- msg
			//wait for a sync response that cleanup is done.
			//notification is sent one by one as there is no lock
			<-notifyCh
		}
	}
	return
}

func (idx *indexer) processDropAfterFlushDone(indexInst common.IndexInst,
	notifyCh MsgChannel, clientCh MsgChannel) {

	select {
	case <-notifyCh:
		idx.cleanupIndex(indexInst, clientCh)
	}

	streamId := indexInst.Stream
	bucket := indexInst.Defn.Bucket
	idx.streamBucketObserveFlushDone[streamId][bucket] = nil

	//indicate done
	close(notifyCh)
}

func (idx *indexer) checkDuplicateDropRequest(indexInst common.IndexInst,
	respCh MsgChannel) bool {

	//if there is any observer for flush done for this bucket,
	//drop is already in progress
	for _, bucketObserver := range idx.streamBucketObserveFlushDone {

		if _, ok := bucketObserver[indexInst.Defn.Bucket]; ok {
			errStr := "Index Drop Already In Progress. Multiple Drop " +
				"Request On A Bucket Are Not Supported By Indexer."

			common.Errorf(errStr)
			if respCh != nil {
				respCh <- &MsgError{
					err: Error{code: ERROR_INDEX_DROP_IN_PROGRESS,
						severity: FATAL,
						cause:    errors.New(errStr),
						category: INDEXER}}

			}
			return true
		}
	}
	return false
}

func (idx *indexer) bootstrap() error {

	idx.genIndexerId()

	//set topic names based on indexer id
	idx.initStreamTopicName()

	//close any old streams with projector
	idx.closeAllStreams()

	//recover indexes from local metadata
	if err := idx.initFromPersistedState(); err != nil {
		return err
	}

	//Start Storage Manager
	var res Message
	idx.storageMgr, res = NewStorageManager(idx.storageMgrCmdCh, idx.wrkrRecvCh,
		idx.indexPartnMap, idx.config)
	if res.GetMsgType() == MSG_ERROR {
		err := res.(*MsgError).GetError()
		common.Errorf("Indexer::NewIndexer Storage Manager Init Error %v", err)
		return err.cause
	}

	//if there are no indexes, return from here
	if len(idx.indexInstMap) == 0 {
		return nil
	}
	//send updated maps
	msgUpdateIndexInstMap := &MsgUpdateInstMap{indexInstMap: idx.indexInstMap}
	msgUpdateIndexPartnMap := &MsgUpdatePartnMap{indexPartnMap: idx.indexPartnMap}

	if err := idx.distributeIndexMapsToWorkers(msgUpdateIndexInstMap, msgUpdateIndexPartnMap); err != nil {
		common.CrashOnError(err)
	}

	if ok := idx.startStreams(); !ok {
		return errors.New("Unable To Start DCP Streams")
	}

	return nil

}

func (idx *indexer) genIndexerId() {

	if idx.enableManager {

		//try to fetch IndexerId from manager
		idx.clustMgrAgentCmdCh <- &MsgClustMgrLocal{
			mType: CLUST_MGR_GET_LOCAL,
			key:   INDEXER_ID_KEY,
		}

		respMsg := <-idx.clustMgrAgentCmdCh
		resp := respMsg.(*MsgClustMgrLocal)

		val := resp.GetValue()
		err := resp.GetError()

		if err == nil {
			idx.id = val
		} else if strings.Contains(err.Error(), "key not found") {
			//if there is no IndexerId, generate and store in manager
			idx.id = strconv.Itoa(rand.Int())

			idx.clustMgrAgentCmdCh <- &MsgClustMgrLocal{
				mType: CLUST_MGR_SET_LOCAL,
				key:   INDEXER_ID_KEY,
				value: idx.id,
			}

			respMsg := <-idx.clustMgrAgentCmdCh
			resp := respMsg.(*MsgClustMgrLocal)

			errMsg := resp.GetError()
			if errMsg != nil {
				common.Errorf("Indexer::genIndexerId Unable to set IndexerId In Local"+
					"Meta Storage. Err %v", errMsg)
				common.CrashOnError(errMsg)
			}

		} else {
			common.Errorf("Indexer::genIndexerId Error Fetching IndexerId From Local"+
				"Meta Storage. Err %v", err)
			common.CrashOnError(err)
		}
	} else {
		//assume 1 without manager
		idx.id = "1"
	}

	common.Infof("Indexer Id %v", idx.id)

}

func (idx *indexer) initFromPersistedState() error {

	err := idx.recoverIndexInstMap()
	if err != nil {
		common.Errorf("Indexer::initFromPersistedState Error Recovering IndexInstMap %v", err)
		return err
	}

	common.Debugf("Indexer::initFromPersistedState Recovered IndexInstMap %v", idx.indexInstMap)

	for _, inst := range idx.indexInstMap {

		newpc := common.NewKeyPartitionContainer()

		//Add one partition for now
		partnId := common.PartitionId(0)
		addr := net.JoinHostPort("", idx.config["streamMaintPort"].String())
		endpt := []common.Endpoint{common.Endpoint(addr)}
		partnDefn := common.KeyPartitionDefn{Id: partnId,
			Endpts: endpt}
		newpc.AddPartition(partnId, partnDefn)

		inst.Pc = newpc

		//allocate partition/slice
		var partnInstMap PartitionInstMap
		var err error
		if partnInstMap, err = idx.initPartnInstance(inst, nil); err != nil {
			return err
		}

		idx.indexInstMap[inst.InstId] = inst
		idx.indexPartnMap[inst.InstId] = partnInstMap

	}

	return nil

}

func (idx *indexer) recoverIndexInstMap() error {

	if idx.enableManager {
		return idx.recoverInstMapFromManager()
	} else {
		return idx.recoverInstMapFromFile()
	}

}

func (idx *indexer) recoverInstMapFromManager() error {

	idx.clustMgrAgentCmdCh <- &MsgClustMgrTopology{}

	resp := <-idx.clustMgrAgentCmdCh

	switch resp.GetMsgType() {

	case CLUST_MGR_GET_GLOBAL_TOPOLOGY:
		idx.indexInstMap = resp.(*MsgClustMgrTopology).GetInstMap()

	case MSG_ERROR:
		err := resp.(*MsgError).GetError()
		common.CrashOnError(err.cause)

	default:
		common.CrashOnError(errors.New("Unknown Response"))
	}
	return nil
}

func (idx *indexer) recoverInstMapFromFile() error {

	var dbfile *forestdb.File
	var meta *forestdb.KVStore
	var err error

	//read indexer state and local state context
	config := forestdb.DefaultConfig()

	if dbfile, err = forestdb.Open("meta", config); err != nil {
		return err
	}
	defer dbfile.Close()

	kvconfig := forestdb.DefaultKVStoreConfig()
	// Make use of default kvstore provided by forestdb
	if meta, err = dbfile.OpenKVStore("default", kvconfig); err != nil {
		return err
	}

	defer meta.Close()

	//read the instance map
	var instBytes []byte
	instBytes, err = meta.GetKV([]byte(INST_MAP_KEY_NAME))

	//forestdb reports get in a non-existent key as an
	//error, skip that
	if err != nil && err != forestdb.RESULT_KEY_NOT_FOUND {
		return err
	}

	//if there is no instance map available, proceed with
	//normal init
	if len(instBytes) == 0 {
		return nil
	}

	decBuf := bytes.NewBuffer(instBytes)
	dec := gob.NewDecoder(decBuf)
	err = dec.Decode(&idx.indexInstMap)

	if err != nil {
		common.Errorf("Indexer::recoverInstMapFromFile Decode Error %v", err)
		return err
	}
	return nil
}

func (idx *indexer) startStreams() bool {

	restartTs := idx.makeRestartTs()

	//Start MAINT_STREAM
	idx.streamBucketStatus[common.MAINT_STREAM] = make(BucketStatus)
	for bucket, ts := range restartTs {
		idx.startBucketStream(common.MAINT_STREAM, bucket, ts)
	}

	//Start INIT_STREAM
	idx.streamBucketStatus[common.INIT_STREAM] = make(BucketStatus)
	for bucket, ts := range restartTs {
		idx.startBucketStream(common.INIT_STREAM, bucket, ts)
	}

	return true

}

func (idx *indexer) makeRestartTs() map[string]*common.TsVbuuid {

	restartTs := make(map[string]*common.TsVbuuid)

	for idxInstId, partnMap := range idx.indexPartnMap {
		idxInst := idx.indexInstMap[idxInstId]

		//there is only one partition for now
		partnInst := partnMap[0]
		sc := partnInst.Sc

		//there is only one slice for now
		slice := sc.GetSliceById(0)

		infos, err := slice.GetSnapshots()
		// TODO: Proper error handling if possible
		if err != nil {
			panic("Unable read snapinfo -" + err.Error())
		}

		s := NewSnapshotInfoContainer(infos)
		latestSnapInfo := s.GetLatest()

		//There may not be a valid snapshot info if no flush
		//happened for this index
		if latestSnapInfo != nil {
			ts := latestSnapInfo.Timestamp()
			if oldTs, ok := restartTs[idxInst.Defn.Bucket]; ok {
				if !ts.AsRecent(oldTs) {
					restartTs[idxInst.Defn.Bucket] = ts
				}
			} else {
				restartTs[idxInst.Defn.Bucket] = ts
			}
		} else {
			//set restartTs to nil for this bucket
			if _, ok := restartTs[idxInst.Defn.Bucket]; !ok {
				restartTs[idxInst.Defn.Bucket] = nil
			}
		}
	}
	return restartTs
}

func (idx *indexer) closeAllStreams() {

	respCh := make(MsgChannel)

	for i := 0; i < int(common.ALL_STREAMS); i++ {

		cmd := &MsgStreamUpdate{mType: CLOSE_STREAM,
			streamId: common.StreamId(i),
			respCh:   respCh,
		}

	retryloop:
		for {
			idx.sendMsgToKVSender(cmd)

			if resp, ok := <-respCh; ok {

				switch resp.GetMsgType() {

				case MSG_SUCCESS:
					break retryloop

				default:
					//log and retry for all other responses
					common.Errorf("Indexer::closeAllStreams Stream %v Bucket %v \n\t"+
						"Error from Projector %v", resp)
				}
			}
		}
	}
}

func (idx *indexer) checkStreamRequestPending(streamId common.StreamId, bucket string) bool {

	if bmap, ok := idx.streamBucketRequestStopCh[streamId]; ok {
		if stopCh, ok := bmap[bucket]; ok {
			if stopCh != nil {
				return true
			}
		}
	}

	return false
}

func (idx *indexer) updateMetaInfoForBucket(bucket string,
	updateState bool, updateStream bool, updateError bool) error {

	var instIdList []common.IndexInstId
	for _, inst := range idx.indexInstMap {
		if inst.Defn.Bucket == bucket {
			instIdList = append(instIdList, inst.InstId)
		}
	}

	if len(instIdList) != 0 {
		return idx.updateMetaInfoForIndexList(instIdList, updateState,
			updateStream, updateError)
	} else {
		return nil
	}

}

func (idx *indexer) updateMetaInfoForIndexList(instIdList []common.IndexInstId,
	updateState bool, updateStream bool, updateError bool) error {

	var indexList []common.IndexInst
	for _, instId := range instIdList {
		indexList = append(indexList, idx.indexInstMap[instId])
	}

	updatedFields := MetaUpdateFields{
		state:  updateState,
		stream: updateStream,
		err:    updateError,
	}

	msg := &MsgClustMgrUpdate{
		mType:         CLUST_MGR_UPDATE_TOPOLOGY_FOR_INDEX,
		indexList:     indexList,
		updatedFields: updatedFields}

	return idx.sendMsgToClusterMgr(msg)

}

func (idx *indexer) sendMsgToClusterMgr(msg Message) error {

	idx.clustMgrAgentCmdCh <- msg

	if res, ok := <-idx.clustMgrAgentCmdCh; ok {

		switch res.GetMsgType() {

		case MSG_SUCCESS:
			return nil

		case MSG_ERROR:
			common.Debugf("Indexer::sendMsgToClusterMgr Error "+
				"from Cluster Manager %v", res)
			err := res.(*MsgError).GetError()
			return err.cause

		default:
			common.Debugf("Indexer::sendMsgToClusterMgr Unknown Response "+
				"from Cluster Manager %v", res)
			common.CrashOnError(errors.New("Unknown Response"))

		}

	} else {

		common.Debugf("clustMgrAgent::sendMsgToClusterMgr Unexpected Channel Close " +
			"from Cluster Manager")
		common.CrashOnError(errors.New("Unknown Response"))

	}

	return nil
}

func (idx *indexer) bulkUpdateError(instIdList []common.IndexInstId,
	errStr string) {

	for _, instId := range instIdList {
		idxInst := idx.indexInstMap[instId]
		idxInst.Error = errStr
		idx.indexInstMap[instId] = idxInst
	}

}

func (idx *indexer) bulkUpdateState(instIdList []common.IndexInstId,
	state common.IndexState) {

	for _, instId := range instIdList {
		idxInst := idx.indexInstMap[instId]
		idxInst.State = state
		idx.indexInstMap[instId] = idxInst
	}
}

func (idx *indexer) bulkUpdateStream(instIdList []common.IndexInstId,
	stream common.StreamId) {

	for _, instId := range instIdList {
		idxInst := idx.indexInstMap[instId]
		idxInst.Stream = stream
		idx.indexInstMap[instId] = idxInst
	}
}

func (idx *indexer) checkBucketInRecovery(bucket string,
	instIdList []common.IndexInstId, clientCh MsgChannel) bool {

	if idx.streamBucketStatus[common.INIT_STREAM][bucket] == STREAM_RECOVERY ||
		idx.streamBucketStatus[common.MAINT_STREAM][bucket] == STREAM_RECOVERY {

		if idx.enableManager {
			errStr := fmt.Sprintf("Bucket %v In Recovery", bucket)
			idx.bulkUpdateError(instIdList, errStr)
			if err := idx.updateMetaInfoForIndexList(instIdList, false, false, true); err != nil {
				common.CrashOnError(err)
			}
		} else if clientCh != nil {
			clientCh <- &MsgError{
				err: Error{code: ERROR_INDEXER_IN_RECOVERY,
					severity: FATAL,
					cause:    ErrIndexerInRecovery,
					category: INDEXER}}
		}
		return true
	}
	return false
}

func (idx *indexer) checkValidIndexInst(bucket string,
	instIdList []common.IndexInstId, clientCh MsgChannel) bool {

	//validate instance list
	for _, instId := range instIdList {
		if _, ok := idx.indexInstMap[instId]; !ok {
			if idx.enableManager {
				errStr := fmt.Sprintf("Unknown Index Instance %v In Build Request", instId)
				idx.bulkUpdateError(instIdList, errStr)
				if err := idx.updateMetaInfoForIndexList(instIdList, false, false, true); err != nil {
					common.CrashOnError(err)
				}
			} else if clientCh != nil {
				clientCh <- &MsgError{
					err: Error{code: ERROR_INDEXER_UNKNOWN_INDEX,
						severity: FATAL,
						cause:    ErrIndexNotFound,
						category: INDEXER}}
			}
			return false
		}
	}
	return true
}

func (idx *indexer) groupIndexListByBucket(instIdList []common.IndexInstId) map[string][]common.IndexInstId {

	bucketInstList := make(map[string][]common.IndexInstId)
	for _, instId := range instIdList {
		indexInst := idx.indexInstMap[instId]
		if instList, ok := bucketInstList[indexInst.Defn.Bucket]; ok {
			instList = append(instList, indexInst.InstId)
			bucketInstList[indexInst.Defn.Bucket] = instList
		} else {
			var newInstList []common.IndexInstId
			newInstList = append(newInstList, indexInst.InstId)
			bucketInstList[indexInst.Defn.Bucket] = newInstList
		}
	}
	return bucketInstList

}

func (idx *indexer) checkBucketExists(bucket string,
	instIdList []common.IndexInstId, clientCh MsgChannel) bool {

	if !ValidateBucket(idx.config["clusterAddr"].String(), bucket) {
		if idx.enableManager {
			errStr := fmt.Sprintf("Unknown Bucket %v In Build Request", bucket)
			idx.bulkUpdateError(instIdList, errStr)
			if err := idx.updateMetaInfoForIndexList(instIdList, false, false, true); err != nil {
				common.CrashOnError(err)
			}
		} else if clientCh != nil {
			clientCh <- &MsgError{
				err: Error{code: ERROR_INDEXER_UNKNOWN_BUCKET,
					severity: FATAL,
					cause:    ErrUnknownBucket,
					category: INDEXER}}
		}
		return false
	}
	return true
}

func (idx *indexer) handleStats(cmd Message) {
	statsMap := make(map[string]string)
	req := cmd.(*MsgStatsRequest)
	replych := req.GetReplyChannel()
	statsMap["needs_restart"] = fmt.Sprint(idx.needsRestart)
	replych <- statsMap
}
