// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/security"
	Stats "github.com/couchbase/indexing/secondary/stats"
	"github.com/couchbase/indexing/secondary/system"
)

//MutationManager handles messages from Indexer to manage Mutation Streams
//and flush mutations from mutation queues.
type MutationManager interface {
}

//Map from keyspaceId to mutation queue
type KeyspaceIdQueueMap map[string]IndexerMutationQueue

type BucketQueueMap map[string]IndexerMutationQueue

//Map from keyspaceId to flusher stop channel
type KeyspaceIdStopChMap map[string]StopChannel

// KeyspaceId -> map of vbucket to hostname
type VBMap map[string]map[Vbucket]string

type mutationMgr struct {
	memUsed   int64 //memory used by queue
	maxMemory int64 //max memory to be used

	streamKeyspaceIdQueueMap map[common.StreamId]KeyspaceIdQueueMap
	streamIndexQueueMap      map[common.StreamId]IndexQueueMap

	streamReaderMap       map[common.StreamId]MutationStreamReader
	streamReaderCmdChMap  map[common.StreamId]MsgChannel  //Command msg channel for StreamReader
	streamReaderExitChMap map[common.StreamId]DoneChannel //Channel to indicate stream reader exited

	streamFlusherStopChMap    map[common.StreamId]KeyspaceIdStopChMap //stop channels for flusher
	streamKeyspaceIdSessionId map[common.StreamId]KeyspaceIdSessionId
	streamKeyspaceIdEnableOSO map[common.StreamId]KeyspaceIdEnableOSO

	mutMgrRecvCh   MsgChannel //Receive msg channel for Mutation Manager
	internalRecvCh MsgChannel //Buffered channel to queue worker messages
	supvCmdch      MsgChannel //supervisor sends commands on this channel
	supvRespch     MsgChannel //channel to send any message to supervisor
	streamBeginCh  MsgChannel // channel to process StreamBegin messages

	shutdownCh DoneChannel //internal channel indicating shutdown

	indexInstMap  IndexInstMapHolder
	indexPartnMap IndexPartnMapHolder

	numVbuckets uint16 //number of vbuckets

	flusherWaitGroup sync.WaitGroup

	indexerState common.IndexerState

	lock  sync.Mutex //lock to protect this structure (including map reads/writes)
	flock sync.Mutex //fine-grain lock for streamFlusherStopChMap

	config common.Config
	stats  IndexerStatsHolder

	vbMap         *VbMapHolder
	numVbsPerNode map[string]int64 // NodeUUID -> Number of active vb's on the KV node across all keyspaceIds
	cpuThrottle   *CpuThrottle     // for Autofailover CPU throttling
	enableAuth    *uint32
}

//NewMutationManager creates a new Mutation Manager which listens for commands from
//Indexer.  In case returned MutationManager is nil, Message will have the error msg.
//supvCmdch is a synchronous channel and every request on this channel is followed
//by a response on the same channel. Supervisor is expected to wait for the response
//before issuing a new request on this channel.
//supvRespch will be used by Mutation Manager to send any async error/info messages
//that may happen due to any downstream error or its own processing.
//Additionally, for Flush commands, a sync response is sent on supvCmdch to indicate
//flush has been initiated and once flush completes, another message is sent on
//supvRespch to indicate its completion or any error that may have happened.
//If supvRespch or supvCmdch is closed, mutation manager will termiate its loop.
func NewMutationManager(supvCmdch MsgChannel, supvRespch MsgChannel,
	config common.Config, cpuThrottle *CpuThrottle) (MutationManager, Message) {

	//Init the mutationMgr struct
	m := &mutationMgr{
		streamKeyspaceIdQueueMap:  make(map[common.StreamId]KeyspaceIdQueueMap),
		streamIndexQueueMap:       make(map[common.StreamId]IndexQueueMap),
		streamReaderMap:           make(map[common.StreamId]MutationStreamReader),
		streamReaderCmdChMap:      make(map[common.StreamId]MsgChannel),
		streamReaderExitChMap:     make(map[common.StreamId]DoneChannel),
		streamFlusherStopChMap:    make(map[common.StreamId]KeyspaceIdStopChMap),
		streamKeyspaceIdSessionId: make(map[common.StreamId]KeyspaceIdSessionId),
		streamKeyspaceIdEnableOSO: make(map[common.StreamId]KeyspaceIdEnableOSO),

		mutMgrRecvCh:   make(MsgChannel),
		internalRecvCh: make(MsgChannel, WORKER_MSG_QUEUE_LEN),
		streamBeginCh:  make(MsgChannel, WORKER_MSG_QUEUE_LEN),
		shutdownCh:     make(DoneChannel),
		supvCmdch:      supvCmdch,
		supvRespch:     supvRespch,
		numVbuckets:    uint16(config["numVbuckets"].Int()),
		config:         config,
		memUsed:        0,
		maxMemory:      0,
		vbMap:          &VbMapHolder{},
		numVbsPerNode:  make(map[string]int64),
		cpuThrottle:    cpuThrottle,
	}

	m.setEnableAuth()

	m.vbMap.Init()
	m.indexInstMap.Init()
	m.indexPartnMap.Init()

	err := m.checkPortAvailability()
	if err != nil {
		errMsg := &MsgError{
			err: Error{
				code:     ERROR_MUT_MGR_INTERNAL_ERROR,
				severity: FATAL,
				category: MUTATION_MANAGER,
				cause:    err,
			},
		}

		return nil, errMsg
	}

	//start Mutation Manager loop which listens to commands from its supervisor
	go m.run()

	return m, &MsgSuccess{}

}

//run starts the mutation manager loop which listens to messages
//from its workers(stream_reader and flusher) and
//supervisor(indexer)
func (m *mutationMgr) run() {

	defer m.panicHandler()

	go m.handleWorkerMsgs()
	go m.listenWorkerMsgs()
	go m.processStreamBegins()

	//main Mutation Manager loop
loop:
	for {
		select {
		case cmd, ok := <-m.supvCmdch:
			if ok {
				if cmd.GetMsgType() == MUT_MGR_SHUTDOWN {
					//shutdown and exit the mutation manager loop
					msg := m.shutdown()
					m.supvCmdch <- msg
					break loop
				} else {
					m.handleSupervisorCommands(cmd)
				}
			} else {
				logging.Fatalf("Supervisor Channel Closed Unexpectedly." +
					"Mutation Manager Shutting Itself Down.")
				m.shutdown()
				break loop
			}
		}
	}

}

func (m *mutationMgr) handleWorkerMsgs() {

	for {
		select {
		// internalRecvCh is a big buffer to bridge a fast sender (mutMgrRecvCh)
		// and a slow receiver (supvRespch).  This is used for stream management.
		// Use a separate go-routine for this so that it wouldn't block the admin
		// operation (in main go-routine).
		case msg, ok := <-m.internalRecvCh:
			if ok {
				m.handleWorkerMessage(msg)
			}
		case _, ok := <-m.shutdownCh:
			if !ok {
				//shutdown signalled. exit this loop.
				return
			}
		}
	}
}

func (m *mutationMgr) listenWorkerMsgs() {

	//listen to worker
	for {

		select {

		case msg, ok := <-m.mutMgrRecvCh:
			if ok {
				//handle high priority messages
				switch msg.GetMsgType() {
				case STREAM_READER_ERROR:
					err := msg.(*MsgStreamError).GetError()
					streamId := msg.(*MsgStreamError).GetStreamId()

					//if panic, stream reader cannot be sent any more commands
					//cleanup any blocked requests and internals structs
					if err.code == ERROR_STREAM_READER_PANIC {
						logging.Errorf("MutationMgr::listenWorkerMsgs Stream Reader "+
							"Received for StreamId: %v, Err: %v", streamId, err)

						//signal to any blocked request that stream reader has exited
						close(m.streamReaderExitChMap[streamId])
						//cleanup entry for the stream reader
						func() {
							m.lock.Lock()
							defer m.lock.Unlock()
							delete(m.streamReaderCmdChMap, streamId)
							delete(m.streamReaderMap, streamId)
							delete(m.streamReaderExitChMap, streamId)
						}()
					}
				}

				//all messages need to go to supervisor
				m.internalRecvCh <- msg
			}

		case _, ok := <-m.shutdownCh:
			if !ok {
				//shutdown signalled. exit this loop.
				return
			}
		}
	}

}

//panicHandler handles the panic from underlying stream library
func (r *mutationMgr) panicHandler() {

	//panic recovery
	if rc := recover(); rc != nil {
		var err error
		switch x := rc.(type) {
		case string:
			err = errors.New(x)
		case error:
			err = x
		default:
			err = errors.New("Unknown panic")
		}

		logging.Fatalf("MutationManager Panic Err %v", err)
		logging.Fatalf("%s", logging.StackTraceAll())

		//shutdown the mutation manager
		select {
		case <-r.shutdownCh:
			//if the shutdown channel is closed, this means shutdown was in progress
			//when panic happened, skip calling shutdown again
		default:
			r.shutdown()
		}

		//panic, propagate to supervisor
		msg := &MsgError{
			err: Error{code: ERROR_MUT_MGR_PANIC,
				severity: FATAL,
				category: MUTATION_MANAGER,
				cause:    err}}
		r.supvRespch <- msg
	}

}

//handleSupervisorCommands handles the messages from Supervisor
//Each operation acquires the mutex to make the itself atomic.
func (m *mutationMgr) handleSupervisorCommands(cmd Message) {

	switch cmd.GetMsgType() {

	case OPEN_STREAM:
		m.handleOpenStream(cmd)

	case ADD_INDEX_LIST_TO_STREAM:
		m.handleAddIndexListToStream(cmd)

	case REMOVE_INDEX_LIST_FROM_STREAM:
		m.handleRemoveIndexListFromStream(cmd)

	case REMOVE_KEYSPACE_FROM_STREAM:
		m.handleRemoveKeyspaceFromStream(cmd)

	case CLOSE_STREAM:
		m.handleCloseStream(cmd)

	case CLEANUP_STREAM:
		m.handleCleanupStream(cmd)

	case MUT_MGR_PERSIST_MUTATION_QUEUE:
		m.handlePersistMutationQueue(cmd)

	case MUT_MGR_DRAIN_MUTATION_QUEUE:
		m.handleDrainMutationQueue(cmd)

	case MUT_MGR_GET_MUTATION_QUEUE_HWT:
		m.handleGetMutationQueueHWT(cmd)

	case MUT_MGR_GET_MUTATION_QUEUE_LWT:
		m.handleGetMutationQueueLWT(cmd)

	case UPDATE_INDEX_INSTANCE_MAP:
		m.handleUpdateIndexInstMap(cmd)

	case UPDATE_INDEX_PARTITION_MAP:
		m.handleUpdateIndexPartnMap(cmd)

	case UPDATE_KEYSPACE_STATS_MAP:
		m.handleUpdateKeyspaceStatsMap(cmd)

	case MUT_MGR_ABORT_PERSIST:
		m.handleAbortPersist(cmd)

	case CONFIG_SETTINGS_UPDATE:
		m.handleConfigUpdate(cmd)

	case INDEXER_PAUSE:
		m.handleIndexerPause(cmd)

	case INDEXER_RESUME:
		m.handleIndexerResume(cmd)

	case INDEXER_SECURITY_CHANGE:
		m.handleSecurityChange(cmd)

	default:
		logging.Fatalf("MutationMgr::handleSupervisorCommands Received Unknown Command %v", cmd)
		common.CrashOnError(errors.New("Unknown Command On Supervisor Channel"))
	}
}

//handleWorkerMessage handles messages from workers
func (m *mutationMgr) handleWorkerMessage(cmd Message) {

	switch cmd.GetMsgType() {

	case STREAM_READER_STREAM_DROP_DATA,
		STREAM_READER_STREAM_END,
		STREAM_READER_ERROR,
		STREAM_READER_CONN_ERROR,
		STREAM_READER_HWT,
		STREAM_READER_SYSTEM_EVENT,
		STREAM_READER_OSO_SNAPSHOT_MARKER,
		RESET_STREAM:
		//send message to supervisor to take decision
		logging.Tracef("MutationMgr::handleWorkerMessage Received %v from worker", cmd)
		m.supvRespch <- cmd

	case STREAM_READER_STREAM_BEGIN:
		//send message to supervisor to take decision
		logging.Tracef("MutationMgr::handleWorkerMessage Received %v from worker", cmd)
		m.supvRespch <- cmd
		m.streamBeginCh <- cmd

	default:
		logging.Fatalf("MutationMgr::handleWorkerMessage Received unhandled "+
			"message from worker %v", cmd)
		common.CrashOnError(errors.New("Unknown Message On Worker Channel"))
	}

}

// This method will process stream begin messages and initializes latency
// object for the corresponding stream
func (m *mutationMgr) processStreamBegins() {
	for {
		select {
		case cmd := <-m.streamBeginCh:
			switch cmd.GetMsgType() {

			case STREAM_READER_STREAM_BEGIN:
				// Initialize latency object
				node := cmd.(*MsgStream).GetNode()
				streamId := cmd.(*MsgStream).GetStreamId()
				if node != nil {
					m.lock.Lock()
					_, streamExists := m.streamReaderMap[streamId]
					m.lock.Unlock()

					// Initialize the latency object only if the stream exists
					if streamExists {
						m.initLatencyObj(cmd)
					}
				}

			case CLEANUP_PRJ_STATS:
				streamId := cmd.(*MsgStream).GetStreamId()
				m.cleanLatencyMap(streamId)
			}
		case _, ok := <-m.shutdownCh:
			if !ok {
				//shutdown signalled. exit this loop.
				return
			}
		}
	}
}

func (m *mutationMgr) checkPortAvailability() error {
	logging.Infof("MutationMgr::checkPortAvailability for dataport servers ...")
	for streamId, srvAddr := range StreamAddrMap {
		lis, err := security.MakeListener(string(srvAddr))
		if err != nil {
			msg := fmt.Sprintf("Error in listening on network port %v for stream %v", srvAddr, streamId)
			return fmt.Errorf("%v", msg)
		}

		err = lis.Close()
		if err != nil {
			msg := fmt.Sprintf("Error in closing the listener on network port %v for stream %v", srvAddr, streamId)
			return fmt.Errorf("%v", msg)
		}
	}

	logging.Infof("MutationMgr::checkPortAvailability all dataport server ports are available.")
	return nil
}

//handleOpenStream creates a new MutationStreamReader and
//initializes it with the mutation queue to store the
//mutations in.
func (m *mutationMgr) handleOpenStream(cmd Message) {

	logging.Infof("MutationMgr::handleOpenStream %v", cmd)

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()
	indexList := cmd.(*MsgStreamUpdate).GetIndexList()
	keyspaceId := cmd.(*MsgStreamUpdate).GetKeyspaceId()
	restartTs := cmd.(*MsgStreamUpdate).GetRestartTs()
	allowMarkFirsSnap := cmd.(*MsgStreamUpdate).AllowMarkFirstSnap()
	sessionId := cmd.(*MsgStreamUpdate).GetSessionId()
	enableOSO := cmd.(*MsgStreamUpdate).EnableOSO()

	keyspaceIdFilter := make(map[string]*common.TsVbuuid)
	keyspaceIdFilter[keyspaceId] = restartTs

	m.lock.Lock()
	defer m.lock.Unlock()

	//if this stream is already open, add to existing stream
	if _, ok := m.streamReaderMap[streamId]; ok {

		respMsg := m.addIndexListToExistingStream(streamId,
			keyspaceId, indexList, keyspaceIdFilter, sessionId, enableOSO)
		m.supvCmdch <- respMsg
		return
	}

	keyspaceIdQueueMap := make(KeyspaceIdQueueMap)
	indexQueueMap := make(IndexQueueMap)
	keyspaceIdSessionId := make(KeyspaceIdSessionId)
	keyspaceIdEnableOSO := make(KeyspaceIdEnableOSO)

	//create a new mutation queue for the stream reader
	//for each keyspace, a separate queue is required
	for _, i := range indexList {
		//if there is no mutation queue for this index, allocate a new one
		if _, ok := keyspaceIdQueueMap[keyspaceId]; !ok {
			//init mutation queue
			var queue MutationQueue
			if queue = NewAtomicMutationQueue(keyspaceId, m.numVbuckets,
				&m.maxMemory, &m.memUsed, m.config); queue == nil {
				m.supvCmdch <- &MsgError{
					err: Error{code: ERROR_MUTATION_QUEUE_INIT,
						severity: FATAL,
						category: MUTATION_QUEUE}}
				return
			}

			keyspaceIdQueueMap[keyspaceId] = IndexerMutationQueue{
				queue: queue}
			keyspaceIdSessionId[keyspaceId] = sessionId
			keyspaceIdEnableOSO[keyspaceId] = enableOSO
		}
		indexQueueMap[i.InstId] = keyspaceIdQueueMap[keyspaceId]
	}
	cmdCh := make(MsgChannel)

	reader, errMsg := CreateMutationStreamReader(streamId, keyspaceIdQueueMap, keyspaceIdFilter,
		cmdCh, m.mutMgrRecvCh, getNumStreamWorkers(m.config), m.stats.Get(),
		m.config, m.indexerState, allowMarkFirsSnap, m.vbMap, keyspaceIdSessionId, keyspaceIdEnableOSO,
		m.enableAuth)

	if reader == nil {
		//send the error back on supv channel
		m.supvCmdch <- errMsg
	} else {
		//update internal structs
		m.streamReaderMap[streamId] = reader
		m.streamKeyspaceIdQueueMap[streamId] = keyspaceIdQueueMap
		m.streamIndexQueueMap[streamId] = indexQueueMap
		m.streamReaderCmdChMap[streamId] = cmdCh
		m.streamReaderExitChMap[streamId] = make(DoneChannel)
		m.streamKeyspaceIdSessionId[streamId] = keyspaceIdSessionId
		m.streamKeyspaceIdEnableOSO[streamId] = keyspaceIdEnableOSO

		func() {
			m.flock.Lock()
			defer m.flock.Unlock()
			m.streamFlusherStopChMap[streamId] = make(KeyspaceIdStopChMap)
		}()

		//send success on supv channel
		m.supvCmdch <- &MsgSuccess{}
	}

}

//handleAddIndexListToStream adds a list of indexes to an
//already running MutationStreamReader. If the list has index
//for a keyspace for which there is no mutation queue, it will
//be created.
func (m *mutationMgr) handleAddIndexListToStream(cmd Message) {

	logging.Infof("MutationMgr::handleAddIndexListToStream %v", cmd)

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()
	indexList := cmd.(*MsgStreamUpdate).GetIndexList()
	sessionId := cmd.(*MsgStreamUpdate).GetSessionId()
	keyspaceId := cmd.(*MsgStreamUpdate).GetKeyspaceId()
	enableOSO := cmd.(*MsgStreamUpdate).EnableOSO()

	m.lock.Lock()
	defer m.lock.Unlock()

	//return error if this stream is already closed
	if _, ok := m.streamReaderMap[streamId]; !ok {

		logging.Errorf("MutationMgr::handleAddIndexListToStream \n\tStream "+
			"Already Closed %v", streamId)

		m.supvCmdch <- &MsgError{
			err: Error{code: ERROR_MUT_MGR_STREAM_ALREADY_CLOSED,
				severity: NORMAL,
				category: MUTATION_MANAGER}}
		return
	}

	respMsg := m.addIndexListToExistingStream(streamId,
		keyspaceId, indexList, nil, sessionId, enableOSO)
	//send the message back on supv channel
	m.supvCmdch <- respMsg

}

func (m *mutationMgr) addIndexListToExistingStream(streamId common.StreamId,
	keyspaceId string, indexList []common.IndexInst, keyspaceIdFilter map[string]*common.TsVbuuid,
	sessionId uint64, enableOSO bool) Message {

	keyspaceIdQueueMap := m.streamKeyspaceIdQueueMap[streamId]
	indexQueueMap := m.streamIndexQueueMap[streamId]
	keyspaceIdSessionId := m.streamKeyspaceIdSessionId[streamId]
	keyspaceIdEnableOSO := m.streamKeyspaceIdEnableOSO[streamId]

	var keyspaceIdMapDirty bool
	for _, i := range indexList {
		//if there is no mutation queue for this index, allocate a new one
		if _, ok := keyspaceIdQueueMap[keyspaceId]; !ok {
			//init mutation queue
			var queue MutationQueue
			if queue = NewAtomicMutationQueue(keyspaceId, m.numVbuckets,
				&m.maxMemory, &m.memUsed, m.config); queue == nil {
				return &MsgError{
					err: Error{code: ERROR_MUTATION_QUEUE_INIT,
						severity: FATAL,
						category: MUTATION_QUEUE}}
			}

			keyspaceIdQueueMap[keyspaceId] = IndexerMutationQueue{
				queue: queue}
			keyspaceIdSessionId[keyspaceId] = sessionId
			keyspaceIdEnableOSO[keyspaceId] = enableOSO
			keyspaceIdMapDirty = true
		}
		indexQueueMap[i.InstId] = keyspaceIdQueueMap[keyspaceId]
	}

	if keyspaceIdMapDirty {
		respMsg := m.sendMsgToStreamReader(streamId,
			m.newUpdateKeyspaceIdQueuesMsg(
				keyspaceIdQueueMap,
				keyspaceIdFilter,
				keyspaceIdSessionId,
				keyspaceIdEnableOSO))

		if respMsg.GetMsgType() == MSG_SUCCESS {
			//update internal structures
			m.streamKeyspaceIdQueueMap[streamId] = keyspaceIdQueueMap
			m.streamIndexQueueMap[streamId] = indexQueueMap
			m.streamKeyspaceIdSessionId[streamId] = keyspaceIdSessionId
			m.streamKeyspaceIdEnableOSO[streamId] = keyspaceIdEnableOSO
		}
		return respMsg
	}

	return &MsgSuccess{}
}

func (m *mutationMgr) newUpdateKeyspaceIdQueuesMsg(keyspaceIdQueueMap KeyspaceIdQueueMap,
	keyspaceIdFilter map[string]*common.TsVbuuid,
	keyspaceIdSessionId KeyspaceIdSessionId,
	keyspaceIdEnableOSO KeyspaceIdEnableOSO) *MsgUpdateKeyspaceIdQueue {

	return &MsgUpdateKeyspaceIdQueue{keyspaceIdQueueMap: keyspaceIdQueueMap,
		keyspaceIdFilter:    keyspaceIdFilter,
		stats:               m.stats.Get(),
		keyspaceIdSessionId: keyspaceIdSessionId,
		keyspaceIdEnableOSO: keyspaceIdEnableOSO}
}

//handleRemoveIndexListFromStream removes a list of indexes from an
//already running MutationStreamReader. If all the indexes for a
//keyspace get deleted, its mutation queue is dropped.
func (m *mutationMgr) handleRemoveIndexListFromStream(cmd Message) {

	logging.Infof("MutationMgr::handleRemoveIndexListFromStream %v", cmd)

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()

	m.lock.Lock()
	defer m.lock.Unlock()

	//ignore if this stream is already closed. This case happens
	//when the stream is stopped during recovery and a drop comes in
	if _, ok := m.streamReaderMap[streamId]; !ok {

		logging.Errorf("MutationMgr::handleRemoveIndexListFromStream "+
			"Stream Already Closed %v", streamId)

		m.supvCmdch <- &MsgSuccess{}
		return
	}

	indexList := cmd.(*MsgStreamUpdate).GetIndexList()

	keyspaceIdQueueMap := m.streamKeyspaceIdQueueMap[streamId]
	indexQueueMap := m.streamIndexQueueMap[streamId]
	keyspaceIdSessionId := m.streamKeyspaceIdSessionId[streamId]
	keyspaceIdEnableOSO := m.streamKeyspaceIdEnableOSO[streamId]

	//delete the given indexes from map
	for _, i := range indexList {
		delete(indexQueueMap, i.InstId)
	}

	var keyspaceIdMapDirty bool
	//if all indexes for a keyspaceId have been removed, drop the mutation queue
	for b, bq := range keyspaceIdQueueMap {
		dropKeyspaceId := true
		for _, iq := range indexQueueMap {
			//bad check: if the queues match, it is still being used
			//better way is to check with keyspaceId
			if bq == iq {
				dropKeyspaceId = false
				break
			}
		}
		if dropKeyspaceId == true {
			//destroy the queue explicitly so that
			//any pending mutations in queue get freed
			mq := keyspaceIdQueueMap[b].queue
			mq.Destroy()
			delete(keyspaceIdQueueMap, b)
			delete(keyspaceIdSessionId, b)
			delete(keyspaceIdEnableOSO, b)
			keyspaceIdMapDirty = true
		}
	}

	if keyspaceIdMapDirty {
		respMsg := m.sendMsgToStreamReader(streamId,
			m.newUpdateKeyspaceIdQueuesMsg(
				keyspaceIdQueueMap,
				nil,
				keyspaceIdSessionId,
				keyspaceIdEnableOSO))

		if respMsg.GetMsgType() == MSG_SUCCESS {
			//update internal structures
			m.streamKeyspaceIdQueueMap[streamId] = keyspaceIdQueueMap
			m.streamIndexQueueMap[streamId] = indexQueueMap
			m.streamKeyspaceIdSessionId[streamId] = keyspaceIdSessionId
			m.streamKeyspaceIdEnableOSO[streamId] = keyspaceIdEnableOSO
		}

		//send the message back on supv channel
		m.supvCmdch <- respMsg
	} else {
		m.supvCmdch <- &MsgSuccess{}
	}

}

func (m *mutationMgr) handleRemoveKeyspaceFromStream(cmd Message) {

	logging.Infof("MutationMgr::handleRemoveKeyspaceFromStream %v", cmd)

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()
	keyspaceId := cmd.(*MsgStreamUpdate).GetKeyspaceId()
	abort := cmd.(*MsgStreamUpdate).AbortRecovery()

	m.lock.Lock()
	defer m.lock.Unlock()

	//return error if this stream is already closed
	if _, ok := m.streamReaderMap[streamId]; !ok {

		if abort {
			logging.Infof("MutationMgr::handleRemoveKeyspaceFromStream "+
				"Stream Already Closed %v", streamId)
			m.supvCmdch <- &MsgSuccess{}
		} else {
			logging.Errorf("MutationMgr::handleRemoveKeyspaceFromStream "+
				"Stream Already Closed %v", streamId)
			m.supvCmdch <- &MsgError{
				err: Error{code: ERROR_MUT_MGR_STREAM_ALREADY_CLOSED,
					severity: NORMAL,
					category: MUTATION_MANAGER}}
		}

		return
	}

	keyspaceIdQueueMap := m.streamKeyspaceIdQueueMap[streamId]
	indexQueueMap := m.streamIndexQueueMap[streamId]
	keyspaceIdSessionId := m.streamKeyspaceIdSessionId[streamId]
	keyspaceIdEnableOSO := m.streamKeyspaceIdEnableOSO[streamId]

	var keyspaceIdMapDirty bool

	indexInstMap := m.indexInstMap.Get()
	//delete all indexes for given keyspaceId from map
	for _, inst := range indexInstMap {
		if inst.Defn.KeyspaceId(streamId) == keyspaceId {
			keyspaceIdMapDirty = true
			delete(indexQueueMap, inst.InstId)
		}
	}

	if _, ok := keyspaceIdQueueMap[keyspaceId]; ok {
		keyspaceIdMapDirty = true
		//destroy the queue explicitly so that
		//any pending mutations in queue get freed
		mq := keyspaceIdQueueMap[keyspaceId].queue
		mq.Destroy()
		delete(keyspaceIdQueueMap, keyspaceId)
		delete(keyspaceIdSessionId, keyspaceId)
		delete(keyspaceIdEnableOSO, keyspaceId)
	}

	if len(keyspaceIdQueueMap) == 0 {
		m.sendMsgToStreamReader(streamId,
			&MsgGeneral{mType: STREAM_READER_SHUTDOWN})

		m.cleanupStream(streamId)

		m.supvCmdch <- &MsgSuccess{}
	} else if keyspaceIdMapDirty {
		respMsg := m.sendMsgToStreamReader(streamId,
			m.newUpdateKeyspaceIdQueuesMsg(
				keyspaceIdQueueMap,
				nil,
				keyspaceIdSessionId,
				keyspaceIdEnableOSO))
		if respMsg.GetMsgType() == MSG_SUCCESS {
			//update internal structures
			m.streamKeyspaceIdQueueMap[streamId] = keyspaceIdQueueMap
			m.streamIndexQueueMap[streamId] = indexQueueMap
			m.streamKeyspaceIdSessionId[streamId] = keyspaceIdSessionId
			m.streamKeyspaceIdEnableOSO[streamId] = keyspaceIdEnableOSO
		}

		//send the message back on supv channel
		m.supvCmdch <- respMsg
	} else {
		m.supvCmdch <- &MsgSuccess{}
	}

}

//handleCloseStream closes MutationStreamReader for the specified stream.
func (m *mutationMgr) handleCloseStream(cmd Message) {

	logging.Infof("MutationMgr::handleCloseStream %v", cmd)

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()

	m.lock.Lock()
	defer m.lock.Unlock()

	//return error if this stream is already closed
	if _, ok := m.streamReaderMap[streamId]; !ok {

		logging.Errorf("MutationMgr::handleCloseStream Stream "+
			"Already Closed %v", streamId)

		m.supvCmdch <- &MsgError{
			err: Error{code: ERROR_MUT_MGR_STREAM_ALREADY_CLOSED,
				severity: NORMAL,
				category: MUTATION_MANAGER}}
		return
	}

	respMsg := m.sendMsgToStreamReader(streamId,
		&MsgGeneral{mType: STREAM_READER_SHUTDOWN})

	if respMsg.GetMsgType() == MSG_SUCCESS {
		//update internal data structures
		m.cleanupStream(streamId)
	}

	//send the message back on supv channel
	m.supvCmdch <- respMsg

}

//handleCleanupStream cleans up an already closed stream.
//This handles the case when a MutationStreamReader closes
//abruptly. This method can be used to clean up internal
//mutation manager structures.
func (m *mutationMgr) handleCleanupStream(cmd Message) {

	logging.Infof("MutationMgr::handleCleanupStream %v", cmd)

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()

	m.lock.Lock()
	defer m.lock.Unlock()

	//clean up internal structs for this stream
	m.cleanupStream(streamId)

	//TODO Send response to supervisor
}

//shutdown shuts down all stream readers and flushers
//This call doesn't return till shutdown is complete.
func (m *mutationMgr) shutdown() Message {

	logging.Infof("MutationMgr::shutdown Shutting Down")

	//signal shutdown
	close(m.shutdownCh)

	var uncleanShutdown bool

	donech := make(DoneChannel)
	go func(donech DoneChannel) {
		m.lock.Lock()
		defer m.lock.Unlock()
		//send shutdown message to all stream readers
		for streamId := range m.streamReaderCmdChMap {

			respMsg := m.sendMsgToStreamReader(streamId,
				&MsgGeneral{mType: STREAM_READER_SHUTDOWN})

			if respMsg.GetMsgType() == MSG_SUCCESS {
				//update internal data structures
				m.cleanupStream(streamId)
			} else {
				uncleanShutdown = true
			}

		}
		donech <- true
	}(donech)

	//wait for all readers to shutdown
	<-donech
	logging.Infof("MutationMgr:shutdown All Stream Readers Shutdown")

	//stop flushers
	go func() {
		m.flock.Lock()
		defer m.flock.Unlock()

		//send stop signal to all flushers
		for _, keyspaceIdStopChMap := range m.streamFlusherStopChMap {
			for _, stopch := range keyspaceIdStopChMap {
				close(stopch)
			}
		}
	}()

	//wait for all flushers to finish before returning
	m.flusherWaitGroup.Wait()
	logging.Infof("MutationMgr::shutdown All Flushers Shutdown")

	//return error in case of unclean shutdown
	if uncleanShutdown {
		logging.Errorf("MutationMgr::shutdown Unclean Shutdown")
		return &MsgError{
			err: Error{code: ERROR_MUT_MGR_UNCLEAN_SHUTDOWN,
				severity: NORMAL,
				category: MUTATION_MANAGER}}
	} else {
		logging.Infof("MutationMgr::shutdown Shutdown Complete")
		return &MsgSuccess{}
	}

}

//sendMsgToStreamReader sends the provided message to the stream reader
//and sends the response back. In case the stream reader panics during the
//communication, error is captured and returned back.
func (m *mutationMgr) sendMsgToStreamReader(streamId common.StreamId, msg Message) Message {

	//use select to send message to stream reader,
	//in case stream reader has exited, a message on streamReaderExitCh
	//will unblock this call
	select {

	case m.streamReaderCmdChMap[streamId] <- msg:

	case <-m.streamReaderExitChMap[streamId]:
		logging.Fatalf("MutationMgr::sendMsgToStreamReader Unexpected Stream Reader Exit")
		return &MsgError{
			err: Error{code: ERROR_STREAM_READER_PANIC,
				severity: FATAL,
				category: MUTATION_MANAGER}}
	}

	//use select to wait for stream reader response.
	//incase stream reader has exited, a message on streamReaderExitCh
	//will unblock this call
	select {

	case respMsg, ok := <-m.streamReaderCmdChMap[streamId]:
		if ok {
			return respMsg
		} else {
			logging.Fatalf("MutationMgr:sendMsgToStreamReader Internal Error. Unexpected" +
				"Close for Stream Reader Command Channel")
			return &MsgError{
				err: Error{code: ERROR_MUT_MGR_INTERNAL_ERROR,
					severity: FATAL,
					category: MUTATION_MANAGER}}
		}

	case <-m.streamReaderExitChMap[streamId]:
		logging.Fatalf("MutationMgr::sendMsgToStreamReader Unexpected Stream Reader Exit")
		return &MsgError{
			err: Error{code: ERROR_STREAM_READER_PANIC,
				severity: FATAL,
				category: MUTATION_MANAGER}}
	}

}

//cleanupStream cleans up internal structs for the given stream
func (m *mutationMgr) cleanupStream(streamId common.StreamId) {

	//cleanup internal maps for this stream
	delete(m.streamReaderMap, streamId)
	delete(m.streamKeyspaceIdQueueMap, streamId)
	delete(m.streamIndexQueueMap, streamId)
	delete(m.streamReaderCmdChMap, streamId)
	delete(m.streamReaderExitChMap, streamId)
	delete(m.streamKeyspaceIdSessionId, streamId)
	delete(m.streamKeyspaceIdEnableOSO, streamId)

	// Send a message to clean-up latency map
	m.streamBeginCh <- &MsgStream{mType: CLEANUP_PRJ_STATS, streamId: streamId}

	m.flock.Lock()
	defer m.flock.Unlock()
	delete(m.streamFlusherStopChMap, streamId)

}

//handlePersistMutationQueue handles persist queue message from
//Indexer. Success is sent on the supervisor Cmd channel
//if the flush can be processed. Once the queue gets persisted,
//status is sent on the supervisor Response channel.
func (m *mutationMgr) handlePersistMutationQueue(cmd Message) {

	logging.Tracef("MutationMgr::handlePersistMutationQueue %v", cmd)

	keyspaceId := cmd.(*MsgMutMgrFlushMutationQueue).GetKeyspaceId()
	streamId := cmd.(*MsgMutMgrFlushMutationQueue).GetStreamId()
	ts := cmd.(*MsgMutMgrFlushMutationQueue).GetTimestamp()
	changeVec := cmd.(*MsgMutMgrFlushMutationQueue).GetChangeVector()
	countVec := cmd.(*MsgMutMgrFlushMutationQueue).GetCountVector()
	hasAllSB := cmd.(*MsgMutMgrFlushMutationQueue).HasAllSB()

	m.lock.Lock()
	defer m.lock.Unlock()

	q := m.streamKeyspaceIdQueueMap[streamId][keyspaceId]
	stats := m.stats.Get()
	go m.persistMutationQueue(q, streamId, keyspaceId, ts, changeVec, countVec, stats, hasAllSB)
	m.supvCmdch <- &MsgSuccess{}
}

//persistMutationQueue implements the actual persist for the queue
func (m *mutationMgr) persistMutationQueue(q IndexerMutationQueue,
	streamId common.StreamId, keyspaceId string, ts *common.TsVbuuid,
	changeVec []bool, countVec []uint64, stats *IndexerStats, hasAllSB bool) {

	m.flock.Lock()
	defer m.flock.Unlock()

	stopch := make(StopChannel)
	m.streamFlusherStopChMap[streamId][keyspaceId] = stopch
	m.flusherWaitGroup.Add(1)

	go func(config common.Config) {
		defer m.flusherWaitGroup.Done()

		start := time.Now().UnixNano()

		// If Autofailover is enabled, do any needed CPU throttling
		cpuThrottleDelayMs := m.cpuThrottle.GetActiveThrottleDelayMs()
		if cpuThrottleDelayMs > 0 {
			time.Sleep(time.Duration(cpuThrottleDelayMs) * time.Millisecond)
		}

		flusher := NewFlusher(config, stats)
		sts := Timestamp(ts.Seqnos)
		msgch := flusher.PersistUptoTS(q.queue, streamId, keyspaceId,
			m.indexInstMap.Get(), m.indexPartnMap.Get(), sts, changeVec, countVec, stopch)
		//wait for flusher to finish
		msg := <-msgch

		//update map and free lock before blocking on the supv channel
		func() {
			m.flock.Lock()
			defer m.flock.Unlock()

			//delete the stop channel from the map
			delete(m.streamFlusherStopChMap[streamId], keyspaceId)
		}()

		stats.memoryUsedQueue.Set(atomic.LoadInt64(&m.memUsed))

		//send the response to supervisor
		if msg.GetMsgType() == MSG_SUCCESS {
			m.supvRespch <- &MsgMutMgrFlushDone{mType: MUT_MGR_FLUSH_DONE,
				streamId:   streamId,
				keyspaceId: keyspaceId,
				ts:         ts,
				hasAllSB:   hasAllSB}
		} else {
			m.supvRespch <- &MsgMutMgrFlushDone{mType: MUT_MGR_FLUSH_DONE,
				streamId:   streamId,
				keyspaceId: keyspaceId,
				ts:         ts,
				aborted:    true}
		}
		keyspaceStats := m.stats.GetKeyspaceStats(streamId, keyspaceId)
		if keyspaceStats != nil {
			keyspaceStats.flushLatDist.Add(time.Now().UnixNano() - start)
		}
	}(m.config)
}

//handleDrainMutationQueue handles drain queue message from
//supervisor. Success is sent on the supervisor Cmd channel
//if the flush can be processed. Once the queue gets drained,
//status is sent on the supervisor Response channel.
func (m *mutationMgr) handleDrainMutationQueue(cmd Message) {

	logging.Tracef("MutationMgr::handleDrainMutationQueue %v", cmd)

	keyspaceId := cmd.(*MsgMutMgrFlushMutationQueue).GetKeyspaceId()
	streamId := cmd.(*MsgMutMgrFlushMutationQueue).GetStreamId()
	ts := cmd.(*MsgMutMgrFlushMutationQueue).GetTimestamp()
	changeVec := cmd.(*MsgMutMgrFlushMutationQueue).GetChangeVector()

	m.lock.Lock()
	defer m.lock.Unlock()

	q := m.streamKeyspaceIdQueueMap[streamId][keyspaceId]
	stats := m.stats.Get()
	go m.drainMutationQueue(q, streamId, keyspaceId, ts, changeVec, stats)
	m.supvCmdch <- &MsgSuccess{}
}

//drainMutationQueue implements the actual drain for the queue
func (m *mutationMgr) drainMutationQueue(q IndexerMutationQueue,
	streamId common.StreamId, keyspaceId string, ts *common.TsVbuuid,
	changeVec []bool, stats *IndexerStats) {

	m.flock.Lock()
	defer m.flock.Unlock()

	stopch := make(StopChannel)
	m.streamFlusherStopChMap[streamId][keyspaceId] = stopch
	m.flusherWaitGroup.Add(1)

	go func(config common.Config) {
		defer m.flusherWaitGroup.Done()

		flusher := NewFlusher(config, stats)
		sts := Timestamp(ts.Seqnos)
		msgch := flusher.DrainUptoTS(q.queue, streamId, keyspaceId,
			sts, changeVec, stopch)
		//wait for flusher to finish
		msg := <-msgch

		//update map and free lock before blocking on the supv channel
		func() {
			m.flock.Lock()
			defer m.flock.Unlock()

			//delete the stop channel from the map
			delete(m.streamFlusherStopChMap[streamId], keyspaceId)
		}()

		//send the response to supervisor
		m.supvRespch <- msg
	}(m.config)

}

func (m *mutationMgr) handleAbortPersist(cmd Message) {

	logging.Infof("MutationMgr::handleAbortPersist %v", cmd)

	keyspaceId := cmd.(*MsgMutMgrFlushMutationQueue).GetKeyspaceId()
	streamId := cmd.(*MsgMutMgrFlushMutationQueue).GetStreamId()

	go func() {
		m.flock.Lock()
		defer m.flock.Unlock()

		//abort the flush for given stream and keyspaceId, if its in progress
		if keyspaceIdStopChMap, ok := m.streamFlusherStopChMap[streamId]; ok {
			if stopch, ok := keyspaceIdStopChMap[keyspaceId]; ok {
				if stopch != nil {
					close(stopch)
				}
			}
		}
		m.supvRespch <- &MsgMutMgrFlushDone{mType: MUT_MGR_ABORT_DONE,
			keyspaceId: keyspaceId,
			streamId:   streamId}
	}()

	m.supvCmdch <- &MsgSuccess{}

}

//handleGetMutationQueueHWT calculates HWT for a mutation queue
//for a given stream and keyspaceId
func (m *mutationMgr) handleGetMutationQueueHWT(cmd Message) {

	logging.Tracef("MutationMgr::handleGetMutationQueueHWT %v", cmd)

	keyspaceId := cmd.(*MsgMutMgrGetTimestamp).GetKeyspaceId()
	streamId := cmd.(*MsgMutMgrGetTimestamp).GetStreamId()

	m.lock.Lock()
	defer m.lock.Unlock()

	q := m.streamKeyspaceIdQueueMap[streamId][keyspaceId]
	stats := m.stats.Get()
	go func(config common.Config) {
		flusher := NewFlusher(config, stats)
		ts := flusher.GetQueueHWT(q.queue)
		m.supvCmdch <- &MsgTimestamp{ts: ts}
	}(m.config)
}

//handleGetMutationQueueLWT calculates LWT for a mutation queue
//for a given stream and keyspaceId
func (m *mutationMgr) handleGetMutationQueueLWT(cmd Message) {

	logging.Tracef("MutationMgr::handleGetMutationQueueLWT %v", cmd)
	keyspaceId := cmd.(*MsgMutMgrGetTimestamp).GetKeyspaceId()
	streamId := cmd.(*MsgMutMgrGetTimestamp).GetStreamId()

	m.lock.Lock()
	defer m.lock.Unlock()

	q := m.streamKeyspaceIdQueueMap[streamId][keyspaceId]
	stats := m.stats.Get()
	go func(config common.Config) {
		flusher := NewFlusher(config, stats)
		ts := flusher.GetQueueLWT(q.queue)
		m.supvCmdch <- &MsgTimestamp{ts: ts}
	}(m.config)
}

//handleUpdateIndexInstMap updates the indexInstMap
func (m *mutationMgr) handleUpdateIndexInstMap(cmd Message) {

	req := cmd.(*MsgUpdateInstMap)
	updatedInsts := req.GetUpdatedInsts()
	if len(updatedInsts) > 0 {
		logging.Infof("MutationMgr::handleUpdateIndexInstMap, updated instances: %v", updatedInsts)
	}
	deletedInstIds := req.GetDeletedInstIds()
	if len(deletedInstIds) > 0 {
		logging.Infof("MutationMgr::handleUpdateIndexInstMap, deleted instance ids: %v", deletedInstIds)
	}
	logging.Tracef("MutationMgr::handleUpdateIndexInstMap %v", cmd)

	indexInstMap := req.GetIndexInstMap()
	copyIndexInstMap := common.CopyIndexInstMap2(indexInstMap)
	m.indexInstMap.Set(copyIndexInstMap)
	m.stats.Set(req.GetStatsObject())

	m.supvCmdch <- &MsgSuccess{}

}

//handleUpdateIndexPartnMap updates the indexPartnMap
func (m *mutationMgr) handleUpdateIndexPartnMap(cmd Message) {

	req := cmd.(*MsgUpdatePartnMap)
	updatedPartnMap := req.GetUpdatedPartnMap()
	if len(updatedPartnMap) > 0 {
		logging.Infof("MutationMgr::handleUpdateIndexPartnMap, updated paritionMap: %v", updatedPartnMap)
	}
	deletedInstIds := req.GetDeletedInstIds()
	if len(deletedInstIds) > 0 {
		logging.Infof("MutationMgr::handleUpdateIndexPartnMap, deleted instance ids: %v", deletedInstIds)
	}
	logging.Tracef("MutationMgr::handleUpdateIndexPartnMap %v", cmd)

	indexPartnMap := cmd.(*MsgUpdatePartnMap).GetIndexPartnMap()
	copyIndexPartnMap := CopyIndexPartnMap(indexPartnMap)
	m.indexPartnMap.Set(copyIndexPartnMap)

	m.supvCmdch <- &MsgSuccess{}

}

// handleUpdateKeyspaceStatsMap atomically swaps in the pointer to a new KeyspaceStatsMap and forwards
// it on to the stream readers. Any failure messages from stream readers are returned to supervisor.
func (m *mutationMgr) handleUpdateKeyspaceStatsMap(cmd Message) {
	logging.Tracef("MutationMgr::handleUpdateKeyspaceStatsMap %v", cmd)
	var msg Message

	// Local stats pointer update
	req := cmd.(*MsgUpdateKeyspaceStatsMap)
	stats := m.stats.Get()
	if stats != nil {
		stats.keyspaceStatsMap.Set(req.GetStatsObject())

		// Forward to stream readers
		m.lock.Lock()
		defer m.lock.Unlock()
		for streamId := range m.streamReaderMap {
			respMsg := m.sendMsgToStreamReader(streamId, cmd)
			if respMsg.GetMsgType() == MSG_SUCCESS {
				logging.Infof("MutationMgr::handleUpdateKeyspaceStatsMap Stream %v Succceed", streamId)
			} else {
				logging.Errorf("MutationMgr::handleUpdateKeyspaceStatsMap Fatal Error %v %v", streamId,
					respMsg.(*MsgError).GetError())
				msg = respMsg
			}
		}
	}

	if msg == nil {
		msg = &MsgSuccess{}
	}

	m.supvCmdch <- msg
}

func (m *mutationMgr) initLatencyObj(cmd Message) {
	stats := m.stats.Get()
	if stats == nil {
		return
	}

	newPrjLatencyMap := stats.prjLatencyMap.Clone()
	update := false

	node := cmd.(*MsgStream).GetNode()
	streamId := cmd.(*MsgStream).GetStreamId()
	meta := cmd.(*MsgStream).GetMutationMeta()

	vb := meta.vbucket
	keyspaceId := meta.keyspaceId
	vbMap := m.vbMap.Clone()

	perStreamCurrNode := fmt.Sprintf("%v/%s", streamId, node)
	perStreamKeyspaceId := fmt.Sprintf("%v/%v", streamId, keyspaceId)

	if _, ok := vbMap[perStreamKeyspaceId]; !ok {
		vbMap[perStreamKeyspaceId] = make(map[Vbucket]string)
	}

	// Check if vb belonged to a different node before
	if perStreamPrevNode, ok := vbMap[perStreamKeyspaceId][vb]; ok &&
		perStreamPrevNode != perStreamCurrNode {
		// vb belonged to a different node before
		m.numVbsPerNode[perStreamPrevNode]--
		if m.numVbsPerNode[perStreamPrevNode] == 0 {
			delete(m.numVbsPerNode, perStreamPrevNode)
			delete(newPrjLatencyMap, perStreamPrevNode)
			update = true
		}
	}

	if _, ok := m.numVbsPerNode[perStreamCurrNode]; !ok {
		// Initialize latency object
		avg := &Stats.Int64Val{}
		avg.Init()
		newPrjLatencyMap[perStreamCurrNode] = avg
		m.numVbsPerNode[perStreamCurrNode] = 1
		update = true
	} else {
		m.numVbsPerNode[perStreamCurrNode]++
	}

	vbMap[perStreamKeyspaceId][vb] = perStreamCurrNode
	m.vbMap.Set(vbMap)

	// Update prjLatencyMap only if there is a change
	if update {
		stats.prjLatencyMap.Set(newPrjLatencyMap)
	}
}

// Cleans all latency objects corresponding to the stream
func (m *mutationMgr) cleanLatencyMap(streamId common.StreamId) {
	stats := m.stats.Get()
	if stats == nil {
		return
	}

	newPrjLatencyMap := stats.prjLatencyMap.Clone()

	streamStr := fmt.Sprintf("%v", streamId)
	for k := range newPrjLatencyMap {
		subStrs := strings.Split(k, "/")
		if len(subStrs) > 0 && subStrs[0] == streamStr {
			delete(newPrjLatencyMap, k)
		}
	}
	stats.prjLatencyMap.Set(newPrjLatencyMap)
}

func (m *mutationMgr) handleConfigUpdate(cmd Message) {
	cfgUpdate := cmd.(*MsgConfigUpdate)
	m.config = cfgUpdate.GetConfig()

	m.setMaxMemoryFromQuota()
	m.setEnableAuth()
	m.setMinVbQueueLength()

	m.supvCmdch <- &MsgSuccess{}
}

//Calculate mutation queue length from memory quota
func (m *mutationMgr) setMaxMemoryFromQuota() {

	memQuota := m.config.GetIndexerMemoryQuota(system.UpdateSysMemObject("MutationMgr::setMaxMemoryFromQuota"))
	fracQueueMem := getMutationQueueMemFrac(m.config)

	maxMem := int64(fracQueueMem * float64(memQuota))
	maxMemHard := int64(m.config["mutation_manager.maxQueueMem"].Uint64())
	if maxMem > maxMemHard {
		maxMem = maxMemHard
	}

	atomic.StoreInt64(&m.maxMemory, maxMem)
	stats := m.stats.Get()
	if stats != nil {
		stats.memoryQuotaQueue.Set(atomic.LoadInt64(&maxMem))
	}

	logging.Infof("MutationMgr::MaxQueueMemoryQuota %v", maxMem)
}

func (m *mutationMgr) setMinVbQueueLength() {

	minVbQueueLen := m.config["settings.minVbQueueLength"].Uint64()
	logging.Infof("MutationManager::setMinVbQueueLength %v", minVbQueueLen)
	for _, keyspaceIdQueueMap := range m.streamKeyspaceIdQueueMap {

		for _, queue := range keyspaceIdQueueMap {
			queue.queue.SetMinVbQueueLength(minVbQueueLen)
		}

	}
}

func (m *mutationMgr) handleIndexerPause(cmd Message) {

	m.lock.Lock()
	defer m.lock.Unlock()

	m.indexerState = common.INDEXER_PAUSED

	for streamId := range m.streamReaderMap {

		respMsg := m.sendMsgToStreamReader(streamId, cmd)

		if respMsg.GetMsgType() == MSG_SUCCESS {
			logging.Infof("MutationMgr::handleIndexerPause Stream "+
				"%v Paused", streamId)
		} else {
			err := respMsg.(*MsgError).GetError()
			logging.Errorf("MutationMgr::handleIndexerPause Fatal Error "+
				"Pausing Stream %v %v", streamId, err)
			common.CrashOnError(err.cause)
		}

	}
	m.supvCmdch <- &MsgSuccess{}

}

func (m *mutationMgr) handleIndexerResume(cmd Message) {

	m.lock.Lock()
	defer m.lock.Unlock()

	m.indexerState = common.INDEXER_ACTIVE

	m.supvCmdch <- &MsgSuccess{}
}

func (m *mutationMgr) handleSecurityChange(cmd Message) {

	m.lock.Lock()
	defer m.lock.Unlock()

	var msg Message

	for streamId := range m.streamReaderMap {

		respMsg := m.sendMsgToStreamReader(streamId, cmd)

		if respMsg.GetMsgType() == MSG_SUCCESS {
			logging.Infof("MutationMgr::handleSecurityChange Stream %v Succceed", streamId)
		} else {
			logging.Errorf("MutationMgr::handleSecurityChange Fatal Error %v %v", streamId,
				respMsg.(*MsgError).GetError())
			msg = respMsg
		}
	}

	if msg == nil {
		msg = &MsgSuccess{}
	}

	m.supvCmdch <- msg
}

func (m *mutationMgr) setEnableAuth() {
	m.lock.Lock()
	defer m.lock.Unlock()

	enableAuth := uint32(0)

	if m.enableAuth == nil {
		m.enableAuth = &enableAuth
	}

	enableAuthVal, ok := m.config["dataport.enableAuth"]
	if ok {
		if enableAuthVal.Bool() {
			atomic.StoreUint32(m.enableAuth, 1)
		} else {
			atomic.StoreUint32(m.enableAuth, 0)
		}

		logging.Infof("mutationMgr::setEnableAuth: enableAuth set to %v", atomic.LoadUint32(m.enableAuth))
	} else {
		logging.Warnf("mutationMgr::setEnableAuth: missing indexer.dataport.enableAuth in config")
	}
}

func CopyKeyspaceIdQueueMap(inMap KeyspaceIdQueueMap) KeyspaceIdQueueMap {

	outMap := make(KeyspaceIdQueueMap)
	for k, v := range inMap {
		outMap[k] = v
	}
	return outMap
}

func getMutationQueueMemFrac(config common.Config) float64 {

	if common.GetStorageMode() == common.FORESTDB {
		return config["mutation_manager.fdb.fracMutationQueueMem"].Float64()
	} else {
		return config["mutation_manager.moi.fracMutationQueueMem"].Float64()
	}

}

func getNumStreamWorkers(config common.Config) int {

	if common.GetStorageMode() == common.FORESTDB {
		return config["stream_reader.fdb.numWorkers"].Int()
	} else {
		return config["stream_reader.moi.numWorkers"].Int()
	}

}

type VbMapHolder struct {
	ptr *unsafe.Pointer
}

func (v *VbMapHolder) Init() {
	v.ptr = new(unsafe.Pointer)
}

func (v *VbMapHolder) Set(vbMap VBMap) {
	atomic.StorePointer(v.ptr, unsafe.Pointer(&vbMap))
}

func (v *VbMapHolder) Get() VBMap {
	if ptr := atomic.LoadPointer(v.ptr); ptr != nil {
		return *(*VBMap)(ptr)
	} else {
		return make(VBMap)
	}
}

func (v *VbMapHolder) Clone() VBMap {
	if ptr := atomic.LoadPointer(v.ptr); ptr != nil {
		currMap := *(*VBMap)(ptr)
		clone := make(VBMap)
		for k, v := range currMap {
			vbHostMap := make(map[Vbucket]string)
			for vb, host := range v {
				vbHostMap[vb] = host
			}
			clone[k] = vbHostMap
		}
		return clone
	} else {
		return make(VBMap)
	}
}
