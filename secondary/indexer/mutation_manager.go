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

	"errors"
	"sync"
)

//MutationManager handles messages from Indexer to manage Mutation Streams
//and flush mutations from mutation queues.
type MutationManager interface {
}

//Map from bucket name to mutation queue
type BucketQueueMap map[string]IndexerMutationQueue

//Map from bucket name to flusher stop channel
type BucketStopChMap map[string]StopChannel

type mutationMgr struct {
	streamBucketQueueMap map[common.StreamId]BucketQueueMap
	streamIndexQueueMap  map[common.StreamId]IndexQueueMap

	streamReaderMap       map[common.StreamId]MutationStreamReader
	streamReaderCmdChMap  map[common.StreamId]MsgChannel  //Command msg channel for StreamReader
	streamReaderExitChMap map[common.StreamId]DoneChannel //Channel to indicate stream reader exited

	streamFlusherStopChMap map[common.StreamId]BucketStopChMap //stop channels for flusher

	mutMgrRecvCh   MsgChannel //Receive msg channel for Mutation Manager
	internalRecvCh MsgChannel //Buffered channel to queue worker messages
	supvCmdch      MsgChannel //supervisor sends commands on this channel
	supvRespch     MsgChannel //channel to send any message to supervisor

	shutdownCh DoneChannel //internal channel indicating shutdown

	indexInstMap  common.IndexInstMap
	indexPartnMap IndexPartnMap

	numVbuckets uint16 //number of vbuckets

	flusher          Flusher //handle to flusher
	flusherWaitGroup sync.WaitGroup

	lock  sync.Mutex //lock to protect this structure
	flock sync.Mutex //fine-grain lock for streamFlusherStopChMap
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
	numVbuckets uint16) (MutationManager, Message) {

	//Init the mutationMgr struct
	m := &mutationMgr{
		streamBucketQueueMap:   make(map[common.StreamId]BucketQueueMap),
		streamIndexQueueMap:    make(map[common.StreamId]IndexQueueMap),
		streamReaderMap:        make(map[common.StreamId]MutationStreamReader),
		streamReaderCmdChMap:   make(map[common.StreamId]MsgChannel),
		streamReaderExitChMap:  make(map[common.StreamId]DoneChannel),
		streamFlusherStopChMap: make(map[common.StreamId]BucketStopChMap),
		mutMgrRecvCh:           make(MsgChannel),
		internalRecvCh:         make(MsgChannel, WORKER_MSG_QUEUE_LEN),
		shutdownCh:             make(DoneChannel),
		supvCmdch:              supvCmdch,
		supvRespch:             supvRespch,
		numVbuckets:            numVbuckets,
		flusher:                NewFlusher(),
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

	go m.listenWorkerMsgs()

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
				common.Errorf("Supervisor Channel Closed Unexpectedly." +
					"Mutation Manager Shutting Itself Down.")
				m.shutdown()
				break loop
			}
		case msg, _ := <-m.internalRecvCh:
			m.handleWorkerMessage(msg)
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
						common.Errorf("MutationMgr::listenWorkerMsgs \n\tStream Reader "+
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

	default:
		common.Errorf("MutationMgr::handleSupervisorCommands \n\tReceived Unknown Command %v", cmd)
		m.supvCmdch <- &MsgError{
			err: Error{code: ERROR_MUT_MGR_UNKNOWN_COMMAND,
				severity: NORMAL,
				category: MUTATION_MANAGER}}
	}
}

//handleWorkerMessage handles messages from workers
func (m *mutationMgr) handleWorkerMessage(cmd Message) {

	switch cmd.GetMsgType() {

	case STREAM_READER_STREAM_DROP_DATA,
		STREAM_READER_STREAM_BEGIN,
		STREAM_READER_STREAM_END,
		STREAM_READER_ERROR,
		STREAM_READER_SYNC:
		//send message to supervisor to take decision
		common.Tracef("MutationMgr::handleWorkerMessage \n\tReceived %v from worker", cmd)
		m.supvRespch <- cmd

	default:
		common.Errorf("MutationMgr::handleWorkerMessage \n\tReceived unhandled message from worker %v", cmd)
	}

}

//handleOpenStream creates a new MutationStreamReader and
//initializes it with the mutation queue to store the
//mutations in.
func (m *mutationMgr) handleOpenStream(cmd Message) {

	common.Infof("MutationMgr::handleOpenStream %v", cmd)

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()

	m.lock.Lock()
	defer m.lock.Unlock()

	//return error if this stream is already open
	if _, ok := m.streamReaderMap[streamId]; ok {

		common.Errorf("MutationMgr::handleOpenStream \n\tStream Already Open %v", streamId)

		m.supvCmdch <- &MsgError{
			err: Error{code: ERROR_MUT_MGR_STREAM_ALREADY_OPEN,
				severity: NORMAL,
				category: MUTATION_MANAGER}}
		return
	}

	indexList := cmd.(*MsgStreamUpdate).GetIndexList()

	bucketQueueMap := make(BucketQueueMap)
	indexQueueMap := make(IndexQueueMap)

	//create a new mutation queue for the stream reader
	//for each bucket, a separate queue is required
	for _, i := range indexList {
		//if there is no mutation queue for this index, allocate a new one
		if _, ok := bucketQueueMap[i.Defn.Bucket]; !ok {
			//init mutation queue
			var queue MutationQueue
			if queue = NewAtomicMutationQueue(m.numVbuckets); queue == nil {
				m.supvCmdch <- &MsgError{
					err: Error{code: ERROR_MUTATION_QUEUE_INIT,
						severity: FATAL,
						category: MUTATION_QUEUE}}
				return
			}

			//init slab manager
			var slabMgr SlabManager
			var errMsg Message
			if slabMgr, errMsg = NewSlabManager(DEFAULT_START_CHUNK_SIZE,
				DEFAULT_SLAB_SIZE, DEFAULT_MAX_SLAB_MEMORY); slabMgr == nil {
				m.supvCmdch <- errMsg
				return
			}

			bucketQueueMap[i.Defn.Bucket] = IndexerMutationQueue{
				queue:   queue,
				slabMgr: slabMgr}
		}
		indexQueueMap[i.InstId] = bucketQueueMap[i.Defn.Bucket]
	}
	cmdCh := make(MsgChannel)

	reader, errMsg := CreateMutationStreamReader(streamId, bucketQueueMap,
		cmdCh, m.mutMgrRecvCh, DEFAULT_NUM_STREAM_READER_WORKERS)

	if reader == nil {
		//send the error back on supv channel
		m.supvCmdch <- errMsg
	} else {
		//update internal structs
		m.streamReaderMap[streamId] = reader
		m.streamBucketQueueMap[streamId] = bucketQueueMap
		m.streamIndexQueueMap[streamId] = indexQueueMap
		m.streamReaderCmdChMap[streamId] = cmdCh
		m.streamReaderExitChMap[streamId] = make(DoneChannel)

		func() {
			m.flock.Lock()
			defer m.flock.Unlock()
			m.streamFlusherStopChMap[streamId] = make(BucketStopChMap)
		}()

		//send success on supv channel
		m.supvCmdch <- &MsgSuccess{}
	}

}

//handleAddIndexListToStream adds a list of indexes to an
//already running MutationStreamReader. If the list has index
//for a bucket for which there is no mutation queue, it will
//be created.
func (m *mutationMgr) handleAddIndexListToStream(cmd Message) {

	common.Infof("MutationMgr::handleAddIndexListToStream %v", cmd)

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()

	m.lock.Lock()
	defer m.lock.Unlock()

	//return error if this stream is already closed
	if _, ok := m.streamReaderMap[streamId]; !ok {

		common.Errorf("MutationMgr::handleAddIndexListToStream \n\tStream "+
			"Already Closed %v", streamId)

		m.supvCmdch <- &MsgError{
			err: Error{code: ERROR_MUT_MGR_STREAM_ALREADY_CLOSED,
				severity: NORMAL,
				category: MUTATION_MANAGER}}
		return
	}

	indexList := cmd.(*MsgStreamUpdate).GetIndexList()

	bucketQueueMap := m.streamBucketQueueMap[streamId]
	indexQueueMap := m.streamIndexQueueMap[streamId]

	var bucketMapDirty bool
	for _, i := range indexList {
		//if there is no mutation queue for this index, allocate a new one
		if _, ok := bucketQueueMap[i.Defn.Bucket]; !ok {
			//init mutation queue
			var queue MutationQueue
			if queue = NewAtomicMutationQueue(m.numVbuckets); queue == nil {
				m.supvCmdch <- &MsgError{
					err: Error{code: ERROR_MUTATION_QUEUE_INIT,
						severity: FATAL,
						category: MUTATION_QUEUE}}
				return
			}

			//init slab manager
			var slabMgr SlabManager
			var errMsg Message
			if slabMgr, errMsg = NewSlabManager(DEFAULT_START_CHUNK_SIZE,
				DEFAULT_SLAB_SIZE, DEFAULT_MAX_SLAB_MEMORY); slabMgr == nil {
				m.supvCmdch <- errMsg
				return
			}

			bucketQueueMap[i.Defn.Bucket] = IndexerMutationQueue{
				queue:   queue,
				slabMgr: slabMgr}
			bucketMapDirty = true
		}
		indexQueueMap[i.InstId] = bucketQueueMap[i.Defn.Bucket]
	}

	if bucketMapDirty {
		respMsg := m.sendMsgToStreamReader(streamId,
			&MsgUpdateBucketQueue{bucketQueueMap: bucketQueueMap})

		if respMsg.GetMsgType() == MSG_SUCCESS {
			//update internal structures
			m.streamBucketQueueMap[streamId] = bucketQueueMap
			m.streamIndexQueueMap[streamId] = indexQueueMap
		}

		//send the message back on supv channel
		m.supvCmdch <- respMsg
	} else {
		m.supvCmdch <- &MsgSuccess{}
	}

}

//handleRemoveIndexListFromStream removes a list of indexes from an
//already running MutationStreamReader. If all the indexes for a
//bucket get deleted, its mutation queue is dropped.
func (m *mutationMgr) handleRemoveIndexListFromStream(cmd Message) {

	common.Infof("MutationMgr::handleRemoveIndexListFromStream %v", cmd)

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()

	m.lock.Lock()
	defer m.lock.Unlock()

	//return error if this stream is already closed
	if _, ok := m.streamReaderMap[streamId]; !ok {

		common.Errorf("MutationMgr::handleRemoveIndexListFromStream "+
			"\n\tStream Already Closed %v", streamId)

		m.supvCmdch <- &MsgError{
			err: Error{code: ERROR_MUT_MGR_STREAM_ALREADY_CLOSED,
				severity: NORMAL,
				category: MUTATION_MANAGER}}
		return
	}

	indexList := cmd.(*MsgStreamUpdate).GetIndexList()

	bucketQueueMap := m.streamBucketQueueMap[streamId]
	indexQueueMap := m.streamIndexQueueMap[streamId]

	//delete the given indexes from map
	for _, i := range indexList {
		delete(indexQueueMap, i.InstId)
	}

	var bucketMapDirty bool
	//if all indexes for a bucket have been removed, drop the mutation queue
	for b, bq := range bucketQueueMap {
		dropBucket := true
		for _, iq := range indexQueueMap {
			//bad check: if the queues match, it is still being used
			//better way is to check with bucket
			if bq == iq {
				dropBucket = false
				break
			}
		}
		if dropBucket == true {
			delete(bucketQueueMap, b)
			bucketMapDirty = true
		}
	}

	if bucketMapDirty {
		respMsg := m.sendMsgToStreamReader(streamId,
			&MsgUpdateBucketQueue{bucketQueueMap: bucketQueueMap})

		if respMsg.GetMsgType() == MSG_SUCCESS {
			//update internal structures
			m.streamBucketQueueMap[streamId] = bucketQueueMap
			m.streamIndexQueueMap[streamId] = indexQueueMap
		}

		//send the message back on supv channel
		m.supvCmdch <- respMsg
	} else {
		m.supvCmdch <- &MsgSuccess{}
	}

}

//handleCloseStream closes MutationStreamReader for the specified stream.
func (m *mutationMgr) handleCloseStream(cmd Message) {

	common.Infof("MutationMgr::handleCloseStream %v", cmd)

	streamId := cmd.(*MsgStreamUpdate).GetStreamId()

	m.lock.Lock()
	defer m.lock.Unlock()

	//return error if this stream is already closed
	if _, ok := m.streamReaderMap[streamId]; !ok {

		common.Errorf("MutationMgr::handleCloseStream \n\tStream "+
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

	common.Infof("MutationMgr::handleCleanupStream %v", cmd)

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

	common.Infof("MutationMgr::shutdown Shutting Down")

	//signal shutdown
	close(m.shutdownCh)

	var uncleanShutdown bool

	donech := make(DoneChannel)
	go func(donech DoneChannel) {
		m.lock.Lock()
		defer m.lock.Unlock()
		//send shutdown message to all stream readers
		for streamId, _ := range m.streamReaderCmdChMap {

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
	common.Infof("MutationMgr:shutdown All Stream Readers Shutdown")

	//stop flushers
	go func() {
		m.flock.Lock()
		defer m.flock.Unlock()

		//send stop signal to all flushers
		for _, bucketStopChMap := range m.streamFlusherStopChMap {
			for _, stopch := range bucketStopChMap {
				close(stopch)
			}
		}
	}()

	//wait for all flushers to finish before returning
	m.flusherWaitGroup.Wait()
	common.Infof("MutationMgr::shutdown All Flushers Shutdown")

	//return error in case of unclean shutdown
	if uncleanShutdown {
		common.Errorf("MutationMgr::shutdown Unclean Shutdown")
		return &MsgError{
			err: Error{code: ERROR_MUT_MGR_UNCLEAN_SHUTDOWN,
				severity: NORMAL,
				category: MUTATION_MANAGER}}
	} else {
		common.Infof("MutationMgr::shutdown Shutdown Complete")
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
		common.Fatalf("MutationMgr::sendMsgToStreamReader \n\tUnexpected Stream Reader Exit")
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
			common.Fatalf("MutationMgr:sendMsgToStreamReader \n\tInternal Error. Unexpected" +
				"Close for Stream Reader Command Channel")
			return &MsgError{
				err: Error{code: ERROR_MUT_MGR_INTERNAL_ERROR,
					severity: FATAL,
					category: MUTATION_MANAGER}}
		}

	case <-m.streamReaderExitChMap[streamId]:
		common.Fatalf("MutationMgr::sendMsgToStreamReader \n\tUnexpected Stream Reader Exit")
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
	delete(m.streamBucketQueueMap, streamId)
	delete(m.streamIndexQueueMap, streamId)
	delete(m.streamReaderCmdChMap, streamId)
	delete(m.streamReaderExitChMap, streamId)

	m.flock.Lock()
	defer m.flock.Unlock()
	delete(m.streamFlusherStopChMap, streamId)

}

//handlePersistMutationQueue handles persist queue message from
//Indexer. Success is sent on the supervisor Cmd channel
//if the flush can be processed. Once the queue gets persisted,
//status is sent on the supervisor Response channel.
func (m *mutationMgr) handlePersistMutationQueue(cmd Message) {

	common.Infof("MutationMgr::handlePersistMutationQueue %v", cmd)

	bucket := cmd.(*MsgMutMgrFlushMutationQueue).GetBucket()
	streamId := cmd.(*MsgMutMgrFlushMutationQueue).GetStreamId()
	ts := cmd.(*MsgMutMgrFlushMutationQueue).GetTimestamp()

	m.lock.Lock()
	defer m.lock.Unlock()

	q := m.streamBucketQueueMap[streamId][bucket]
	go m.persistMutationQueue(q, streamId, bucket, ts)
	m.supvCmdch <- &MsgSuccess{}

}

//persistMutationQueue implements the actual persist for the queue
func (m *mutationMgr) persistMutationQueue(q IndexerMutationQueue,
	streamId common.StreamId, bucket string, ts Timestamp) {

	m.flock.Lock()
	defer m.flock.Unlock()

	stopch := make(StopChannel)
	m.streamFlusherStopChMap[streamId][bucket] = stopch
	m.flusherWaitGroup.Add(1)

	go func() {
		defer m.flusherWaitGroup.Done()

		msgch := m.flusher.PersistUptoTS(q.queue,
			streamId, m.indexInstMap, m.indexPartnMap, ts, stopch)
		//wait for flusher to finish
		msg := <-msgch

		//update map and free lock before blocking on the supv channel
		func() {
			m.flock.Lock()
			defer m.flock.Unlock()

			//delete the stop channel from the map
			delete(m.streamFlusherStopChMap[streamId], bucket)
		}()

		//send the response to supervisor
		if msg.GetMsgType() == MSG_SUCCESS {
			m.supvRespch <- &MsgMutMgrFlushDone{streamId: streamId,
				bucket: bucket,
				ts:     ts}
		} else {
			m.supvRespch <- msg
		}
	}()

}

//handleDrainMutationQueue handles drain queue message from
//supervisor. Success is sent on the supervisor Cmd channel
//if the flush can be processed. Once the queue gets drained,
//status is sent on the supervisor Response channel.
func (m *mutationMgr) handleDrainMutationQueue(cmd Message) {

	common.Infof("MutationMgr::handleDrainMutationQueue %v", cmd)

	bucket := cmd.(*MsgMutMgrFlushMutationQueue).GetBucket()
	streamId := cmd.(*MsgMutMgrFlushMutationQueue).GetStreamId()
	ts := cmd.(*MsgMutMgrFlushMutationQueue).GetTimestamp()

	m.lock.Lock()
	defer m.lock.Unlock()

	q := m.streamBucketQueueMap[streamId][bucket]
	go m.drainMutationQueue(q, streamId, bucket, ts)
	m.supvCmdch <- &MsgSuccess{}
}

//drainMutationQueue implements the actual drain for the queue
func (m *mutationMgr) drainMutationQueue(q IndexerMutationQueue,
	streamId common.StreamId, bucket string, ts Timestamp) {

	m.flock.Lock()
	defer m.flock.Unlock()

	stopch := make(StopChannel)
	m.streamFlusherStopChMap[streamId][bucket] = stopch
	m.flusherWaitGroup.Add(1)

	go func() {
		defer m.flusherWaitGroup.Done()

		msgch := m.flusher.DrainUptoTS(q.queue, streamId,
			ts, stopch)
		//wait for flusher to finish
		msg := <-msgch

		//update map and free lock before blocking on the supv channel
		func() {
			m.flock.Lock()
			defer m.flock.Unlock()

			//delete the stop channel from the map
			delete(m.streamFlusherStopChMap[streamId], bucket)
		}()

		//send the response to supervisor
		m.supvRespch <- msg
	}()

}

//handleGetMutationQueueHWT calculates HWT for a mutation queue
//for a given stream and bucket
func (m *mutationMgr) handleGetMutationQueueHWT(cmd Message) {

	common.Infof("MutationMgr::handleGetMutationQueueHWT %v", cmd)

	bucket := cmd.(*MsgMutMgrGetTimestamp).GetBucket()
	streamId := cmd.(*MsgMutMgrGetTimestamp).GetStreamId()

	m.lock.Lock()
	defer m.lock.Unlock()

	q := m.streamBucketQueueMap[streamId][bucket]

	go func() {
		ts := m.flusher.GetQueueHWT(q.queue)
		m.supvCmdch <- &MsgTimestamp{ts: ts}
	}()
}

//handleGetMutationQueueLWT calculates LWT for a mutation queue
//for a given stream and bucket
func (m *mutationMgr) handleGetMutationQueueLWT(cmd Message) {

	common.Infof("MutationMgr::handleGetMutationQueueLWT %v", cmd)
	bucket := cmd.(*MsgMutMgrGetTimestamp).GetBucket()
	streamId := cmd.(*MsgMutMgrGetTimestamp).GetStreamId()

	m.lock.Lock()
	defer m.lock.Unlock()

	q := m.streamBucketQueueMap[streamId][bucket]

	go func() {
		ts := m.flusher.GetQueueLWT(q.queue)
		m.supvCmdch <- &MsgTimestamp{ts: ts}
	}()
}

//handleUpdateIndexInstMap updates the indexInstMap
func (m *mutationMgr) handleUpdateIndexInstMap(cmd Message) {

	common.Infof("MutationMgr::handleUpdateIndexInstMap %v", cmd)

	m.lock.Lock()
	defer m.lock.Unlock()

	m.indexInstMap = cmd.(*MsgUpdateInstMap).GetIndexInstMap()

	m.supvCmdch <- &MsgSuccess{}

}

//handleUpdateIndexPartnMap updates the indexPartnMap
func (m *mutationMgr) handleUpdateIndexPartnMap(cmd Message) {

	common.Infof("MutationMgr::handleUpdateIndexPartnMap %v", cmd)

	m.lock.Lock()
	defer m.lock.Unlock()

	m.indexPartnMap = cmd.(*MsgUpdatePartnMap).GetIndexPartnMap()

	m.supvCmdch <- &MsgSuccess{}

}
