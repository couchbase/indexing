// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package indexer

import (
	"errors"
	"log"

	"github.com/couchbase/indexing/secondary/common"
)

//MutationStreamReader reads a MutationStream and stores the incoming mutations
//in mutation queue. This is the only component writing to a mutation queue.
type MutationStreamReader interface {
}

type mutationStreamReader struct {
	streamId StreamId
	stream   *MutationStream //handle to the MutationStream

	streamMutch  MutationChannel //channel for mutations sent by MutationStream
	streamRespch MsgChannel      //MutationStream side-band channel

	supvCmdch  MsgChannel //supervisor sends commands on this channel
	supvRespch MsgChannel //channel to send any message to supervisor

	shutdownCh DoneChannel //internal channel indicating shutdown

	numWorkers int // number of workers to process mutation stream

	workerch     []MutationChannel //buffered channel for each worker
	workerStopCh []StopChannel     //stop channels of workers

	indexQueueMap IndexQueueMap //indexId to mutation queue map

}

const MAX_WORKER_BUFFER = 1000

//CreateMutationStreamReader creates a new mutation stream and starts
//a reader to listen and process the mutations.
//In case returned MutationStreamReader is nil, Message will have the error msg.
//supvCmdch is a synchronous channel and every request on this channel is followed
//by a response on the same channel. Supervisor is expected to wait for the response
//before issuing a new request on this channel.
//supvRespch will be used by Stream Reader to send any async error/info messages
//that may happen due to any downstream error or its own processing.
func CreateMutationStreamReader(streamId StreamId, indexQueueMap IndexQueueMap,
	supvCmdch MsgChannel, supvRespch MsgChannel, numWorkers int) (
	MutationStreamReader, Message) {

	//start a new mutation stream
	streamMutch := make(MutationChannel)
	streamRespch := make(MsgChannel)
	stream, err := NewMutationStream(StreamAddrMap[streamId], streamMutch, streamRespch)
	if err != nil {
		//return stream init error
		msgErr := &MsgError{mType: ERROR,
			err: Error{code: ERROR_STREAM_INIT,
				severity: FATAL,
				category: STREAM_READER,
				cause:    err}}
		return nil, msgErr
	}

	//init the reader
	r := &mutationStreamReader{streamId: streamId,
		stream:        stream,
		streamMutch:   streamMutch,
		streamRespch:  streamRespch,
		supvCmdch:     supvCmdch,
		supvRespch:    supvRespch,
		numWorkers:    numWorkers,
		workerch:      make([]MutationChannel, numWorkers),
		workerStopCh:  make([]StopChannel, numWorkers),
		shutdownCh:    make(DoneChannel),
		indexQueueMap: indexQueueMap,
	}

	//start the main reader loop
	go r.run()

	//init worker buffers
	for w := 0; w < r.numWorkers; w++ {
		r.workerch[w] = make(MutationChannel, MAX_WORKER_BUFFER)
	}

	//start stream workers
	r.startWorkers()

	return r, nil
}

//Shutdown shuts down the mutation stream and all workers.
//This call doesn't return till shutdown is complete.
func (r *mutationStreamReader) shutdown() Message {

	close(r.shutdownCh)

	//close the mutation stream
	r.stream.shutdown()

	//stop all workers
	r.stopWorkers()

	return &MsgSuccess{}
}

//run starts the stream reader loop which listens to message from
//mutation stream and the supervisor
func (r *mutationStreamReader) run() {

	//panic handler
	defer r.panicHandler()

loop:
	for {
		select {

		case mut, ok := <-r.streamMutch:
			if ok {
				//TODO: Use Slab Manager here to allocate memory for mutation
				//Mutation Stream should be free to reuse the underlying memory for mutation
				r.handleMutation(mut)
			} else {
				//stream library has closed this channel indicating unexpected stream closure
				//send the message to supervisor
				msgErr := &MsgStreamError{streamId: r.streamId,
					err: Error{code: ERROR_STREAM_READER_STREAM_SHUTDOWN,
						severity: FATAL,
						category: STREAM_READER}}
				r.supvRespch <- msgErr

			}

		case msg, ok := <-r.streamRespch:
			if ok {
				r.handleStreamError(msg)
			} else {
				//stream library has closed this channel indicating unexpected stream closure
				//send the message to supervisor
				msgErr := &MsgStreamError{streamId: r.streamId,
					err: Error{code: ERROR_STREAM_READER_STREAM_SHUTDOWN,
						severity: FATAL,
						category: STREAM_READER}}
				r.supvRespch <- msgErr

			}

		case cmd, ok := <-r.supvCmdch:
			if ok {
				msg := r.handleSupervisorCommands(cmd)
				r.supvCmdch <- msg
				if cmd.GetMsgType() == STREAM_READER_SHUTDOWN {
					break loop
				}
			} else {
				//supervisor channel closed. Shutdown stream reader.
				log.Println("Supervisor Channel Closed Unexpectedly."+
					"Stream Reader for Stream %v Shutting Itself Down.", r.streamId)
				r.shutdown()
				return
			}
		}
	}

}

//handleMutation processes a single mutation based on the command type
//A mutation is put in a worker queue and control message is sent to supervisor
func (r *mutationStreamReader) handleMutation(mut *common.Mutation) {

	//based on the type of command take appropriate action
	switch mut.Command {

	case Upsert, Deletion, UpsertDeletion, Sync:

		//place mutation in the right worker's queue
		r.workerch[mut.Vbucket%r.numWorkers] <- mut

	case DropData:
		//send message to supervisor to take decision
		msg := &MsgStream{mType: STREAM_READER_STREAM_DROP_DATA,
			streamId: r.streamId,
			mutation: mut}
		r.supvRespch <- msg

	case StreamBegin:
		//send message to supervisor to take decision
		msg := &MsgStream{mType: STREAM_READER_STREAM_BEGIN,
			streamId: r.streamId,
			mutation: mut}
		r.supvRespch <- msg

	case StreamEnd:
		//send message to supervisor to take decision
		msg := &MsgStream{mType: STREAM_READER_STREAM_END,
			streamId: r.streamId,
			mutation: mut}
		r.supvRespch <- msg
	}

}

//startMutationStreamWorker is the worker which processes mutation in a worker queue
func (r *mutationStreamReader) startMutationStreamWorker(workerId int, stopch StopChannel) {

	for {
		select {
		case mut := <-r.chworkers[workerId]:
			r.handleSingleMutation(mut)
		case <-stopch:
			return
		}
	}

}

//handleSingleMutation enqueues mutation in the mutation queue
func (r *mutationStreamReader) handleSingleMutation(mut *common.Mutation) {

	//based on the index, enqueue the mutation in the right queue
	if q, ok := r.indexQueueMap[mut.IndexId]; ok {
		q.queue.Enqueue(mut, mut.Vbucket)

	} else {
		log.Println("MutationStreamReader got mutation for unknown index %v", mut.IndexId)
	}

}

//handleStreamError handles the error messages from MutationStream
func (r *mutationStreamReader) handleStreamError(msg Message) {

	//TODO Handle errors other than panic from MutationStream here
}

//handleSupervisorCommands handles the messages from Supervisor
func (r *mutationStreamReader) handleSupervisorCommands(cmd Message) Message {

	switch cmd.GetMsgType() {

	case STREAM_READER_UPDATE_QUEUE_MAP:
		//stop all workers
		r.stopWorkers()

		//store new indexQueueMap
		r.indexQueueMap = cmd.(*MsgUpdateIndexQueue).GetIndexQueueMap()

		//start all workers again
		r.startWorkers()

		return &MsgSuccess{}

	case STREAM_READER_SHUTDOWN:
		return r.shutdown()

	default:
		return &MsgError{mType: ERROR,
			err: Error{code: ERROR_STREAM_READER_UNKNOWN_COMMAND,
				severity: NORMAL,
				category: STREAM_READER}}

	}
}

//panicHandler handles the panic from underlying stream library
func (r *mutationStreamReader) panicHandler() {

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

		//shutdown the stream reader
		select {
		case <-r.shutdownCh:
			//if the shutdown channel is closed, this means shutdown was in progress
			//when panic happened, skip calling shutdown again
		default:
			r.shutdown()
		}

		//panic from stream library, propagate to supervisor
		msg := &MsgStreamError{streamId: r.streamId,
			err: Error{code: ERROR_STREAM_READER_PANIC,
				severity: FATAL,
				category: STREAM_READER,
				cause:    err}}
		r.supvRespch <- msg
	}

}

//startWorkers starts all stream workers and passes
//a StopChannel to each worker
func (r *mutationStreamReader) startWorkers() {
	//start worker goroutines to process incoming mutation concurrently
	for w := 0; w < r.numWorkers; w++ {
		go r.startMutationStreamWorker(w, r.workerStopCh[w])
	}
}

//stopWorkers stops all stream workers. This call doesn't return till
//all workers are stopped
func (r *mutationStreamReader) stopWorkers() {

	//stop all workers
	for ch := range r.workerStopCh {
		ch <- true
	}
}
