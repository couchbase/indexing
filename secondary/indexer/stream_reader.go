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
	"github.com/couchbase/indexing/secondary/protobuf"
)

//MutationStreamReader reads a MutationStream and stores the incoming mutations
//in mutation queue. This is the only component writing to a mutation queue.
type MutationStreamReader interface {
	Shutdown()
}

type mutationStreamReader struct {
	streamId StreamId
	stream   *MutationStream //handle to the MutationStream

	streamMutch  chan []*protobuf.VbKeyVersions //channel for mutations sent by MutationStream
	streamRespch chan interface{}               //MutationStream side-band channel

	supvCmdch  MsgChannel //supervisor sends commands on this channel
	supvRespch MsgChannel //channel to send any message to supervisor

	numWorkers int // number of workers to process mutation stream

	workerch     []MutationChannel //buffered channel for each worker
	workerStopCh []StopChannel     //stop channels of workers

	bucketQueueMap BucketQueueMap //indexId to mutation queue map

}

const MAX_WORKER_BUFFER = 1000

//CreateMutationStreamReader creates a new mutation stream and starts
//a reader to listen and process the mutations.
//In case returned MutationStreamReader is nil, Message will have the error msg.
func CreateMutationStreamReader(streamId StreamId, bucketQueueMap BucketQueueMap,
	supvCmdch MsgChannel, supvRespch MsgChannel, numWorkers int) (
	MutationStreamReader, Message) {

	//start a new mutation stream
	streamMutch := make(chan []*protobuf.VbKeyVersions)
	streamRespch := make(chan interface{})
	stream, err := NewMutationStream(string(StreamAddrMap[streamId]), streamMutch, streamRespch)
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
		stream:         stream,
		streamMutch:    streamMutch,
		streamRespch:   streamRespch,
		supvCmdch:      supvCmdch,
		supvRespch:     supvRespch,
		numWorkers:     numWorkers,
		workerch:       make([]MutationChannel, numWorkers),
		workerStopCh:   make([]StopChannel, numWorkers),
		bucketQueueMap: bucketQueueMap,
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
func (r *mutationStreamReader) Shutdown() {

	//close the mutation stream
	r.stream.Close()

	//stop all workers
	r.stopWorkers()
}

//run starts the stream reader loop which listens to message from
//mutation stream and the supervisor
func (r *mutationStreamReader) run() {

	//panic handler
	defer r.panicHandler()

	for {
		select {

		case vbKeyVer, ok := <-r.streamMutch:
			if ok {
				r.handleVbKeyVersions(vbKeyVer)
			} else {
				//stream library has closed this channel indicating unexpected stream closure
				//send the message to supervisor
				msgErr := &MsgError{mType: ERROR,
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
				msgErr := &MsgError{mType: ERROR,
					err: Error{code: ERROR_STREAM_READER_STREAM_SHUTDOWN,
						severity: FATAL,
						category: STREAM_READER}}
				r.supvRespch <- msgErr
			}

		case cmd, ok := <-r.supvCmdch:
			if ok {
				//handle commands from supervisor
				msg := r.handleSupervisorCommands(cmd)
				r.supvCmdch <- msg
			} else {
				//supervisor channel closed. Shutdown stream reader.
				r.Shutdown()
				return

			}
		}
	}

}

func (r *mutationStreamReader) handleVbKeyVersions(vbKeyVers []*protobuf.VbKeyVersions) {

	for _, vb := range vbKeyVers {

		r.handleKeyVersions(vb.GetBucketname(), Vbucket(vb.GetVbucket()),
			Vbuuid(vb.GetVbuuid()), vb.GetKvs())

	}

}

func (r *mutationStreamReader) handleKeyVersions(bucket string, vbucket Vbucket, vbuuid Vbuuid,
	kvs []*protobuf.KeyVersions) {

	for _, kv := range kvs {

		r.handleSingleKeyVersion(bucket, vbucket, vbuuid, kv)
	}

}

//handleSingleKeyVersion processes a single mutation based on the command type
//A mutation is put in a worker queue and control message is sent to supervisor
func (r *mutationStreamReader) handleSingleKeyVersion(bucket string, vbucket Vbucket, vbuuid Vbuuid,
	kv *protobuf.KeyVersions) {

	meta := &MutationMeta{}
	meta.bucket = bucket
	meta.vbucket = vbucket
	meta.vbuuid = vbuuid
	meta.seqno = Seqno(kv.GetSeqno())

	var mut *MutationKeys

	for i, cmd := range kv.GetCommands() {

		//based on the type of command take appropriate action
		switch byte(cmd) {

		//case protobuf.Command_Upsert, protobuf.Command_Deletion, protobuf.Command_UpsertDeletion:
		case common.Upsert, common.Deletion, common.UpsertDeletion:

			//allocate new mutation first time
			if mut == nil {
				//TODO use free list here to reuse the struct and reduce garbage
				mut := &MutationKeys{}
				mut.meta = meta
				mut.docid = kv.GetDocid()
			}

			//copy the mutation data so underlying stream library can reuse the
			//KeyVersions structs
			mut.uuids = append(mut.uuids, common.IndexInstId(kv.GetUuids()[i]))
			mut.keys = append(mut.keys, [][]byte{kv.GetKeys()[i]})
			mut.oldkeys = append(mut.oldkeys, [][]byte{kv.GetOldkeys()[i]})
			mut.commands = append(mut.commands,
				byte(kv.GetCommands()[i]))

		case common.Sync:
			msg := &MsgStream{mType: STREAM_READER_SYNC,
				streamId: r.streamId,
				meta:     meta}
			r.supvRespch <- msg

		case common.DropData:
			//send message to supervisor to take decision
			msg := &MsgStream{mType: STREAM_READER_STREAM_DROP_DATA,
				streamId: r.streamId,
				meta:     meta}
			r.supvRespch <- msg

		case common.StreamBegin:
			//send message to supervisor to take decision
			msg := &MsgStream{mType: STREAM_READER_STREAM_BEGIN,
				streamId: r.streamId,
				meta:     meta}
			r.supvRespch <- msg

		case common.StreamEnd:
			//send message to supervisor to take decision
			msg := &MsgStream{mType: STREAM_READER_STREAM_END,
				streamId: r.streamId,
				meta:     meta}
			r.supvRespch <- msg
		}
	}

	//place secKey in the right worker's queue
	r.workerch[int(vbucket)%r.numWorkers] <- mut

}

//startMutationStreamWorker is the worker which processes mutation in a worker queue
func (r *mutationStreamReader) startMutationStreamWorker(workerId int, stopch StopChannel) {

	for {
		select {
		case mut := <-r.workerch[workerId]:
			r.handleSingleMutation(mut)
		case <-stopch:
			return
		}
	}

}

//handleSingleMutation enqueues mutation in the mutation queue
func (r *mutationStreamReader) handleSingleMutation(mut *MutationKeys) {

	//based on the index, enqueue the mutation in the right queue
	if q, ok := r.bucketQueueMap[mut.meta.bucket]; ok {
		q.queue.Enqueue(mut, mut.meta.vbucket)

	} else {
		log.Println("MutationStreamReader got mutation for unknown bucket", mut)
	}

}

//handleStreamError handles the error messages from MutationStream
func (r *mutationStreamReader) handleStreamError(msg interface{}) {

	var msgErr *MsgError
	switch msg.(type) {

	case ShutdownDaemon:
		msgErr = &MsgError{mType: ERROR,
			err: Error{code: ERROR_STREAM_READER_STREAM_SHUTDOWN,
				severity: FATAL,
				category: STREAM_READER}}

		//TODO send more information upstream for RepairStream
	case RestartVbuckets:
		msgErr = &MsgError{mType: ERROR,
			err: Error{code: ERROR_STREAM_READER_RESTART_VBUCKETS,
				severity: FATAL,
				category: STREAM_READER}}

	default:
		msgErr = &MsgError{mType: ERROR,
			err: Error{code: ERROR_STREAM_READER_UNKNOWN_ERROR,
				severity: FATAL,
				category: STREAM_READER}}
	}
	r.supvRespch <- msgErr
}

//handleSupervisorCommands handles the messages from Supervisor
func (r *mutationStreamReader) handleSupervisorCommands(cmd Message) Message {

	switch cmd.GetMsgType() {

	case STREAM_READER_UPDATE_QUEUE_MAP:
		//stop all workers
		r.stopWorkers()

		//store new bucketQueueMap
		r.bucketQueueMap = cmd.(*MsgUpdateBucketQueue).GetBucketQueueMap()

		//start all workers again
		r.startWorkers()

		return &MsgSuccess{}

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
	for _, ch := range r.workerStopCh {
		ch <- true
	}
}
