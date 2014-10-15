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

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/dataport"
	"github.com/couchbase/indexing/secondary/protobuf"
)

//MutationStreamReader reads a Dataport and stores the incoming mutations
//in mutation queue. This is the only component writing to a mutation queue.
type MutationStreamReader interface {
	Shutdown()
}

var mutationCount uint64

type mutationStreamReader struct {
	stream   *dataport.Server //handle to the Dataport server
	streamId common.StreamId

	streamMutch chan interface{} //Dataport channel

	supvCmdch  MsgChannel //supervisor sends commands on this channel
	supvRespch MsgChannel //channel to send any message to supervisor

	numWorkers int // number of workers to process mutation stream

	workerch     []MutationChannel //buffered channel for each worker
	workerStopCh []StopChannel     //stop channels of workers

	bucketQueueMap BucketQueueMap //indexId to mutation queue map

}

//CreateMutationStreamReader creates a new mutation stream and starts
//a reader to listen and process the mutations.
//In case returned MutationStreamReader is nil, Message will have the error msg.
func CreateMutationStreamReader(streamId common.StreamId, bucketQueueMap BucketQueueMap,
	supvCmdch MsgChannel, supvRespch MsgChannel, numWorkers int) (
	MutationStreamReader, Message) {

	//start a new mutation stream
	streamMutch := make(chan interface{})
	stream, err := dataport.NewServer(string(StreamAddrMap[streamId]), streamMutch)
	if err != nil {
		//return stream init error
		common.Errorf("MutationStreamReader: Error returned from NewServer."+
			"StreamId: %v, Err: %v", streamId, err)

		msgErr := &MsgError{
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
		supvCmdch:      supvCmdch,
		supvRespch:     supvRespch,
		numWorkers:     numWorkers,
		workerch:       make([]MutationChannel, numWorkers),
		workerStopCh:   make([]StopChannel, numWorkers),
		bucketQueueMap: CopyBucketQueueMap(bucketQueueMap),
	}

	//start the main reader loop
	go r.run()

	//init worker buffers
	for w := 0; w < r.numWorkers; w++ {
		r.workerch[w] = make(MutationChannel, MAX_STREAM_READER_WORKER_BUFFER)
		r.workerStopCh[w] = make(StopChannel)
	}

	//start stream workers
	r.startWorkers()

	return r, &MsgSuccess{}
}

//Shutdown shuts down the mutation stream and all workers.
//This call doesn't return till shutdown is complete.
func (r *mutationStreamReader) Shutdown() {

	common.Infof("MutationStreamReader:Shutdown StreamReader %v", r.streamId)

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

		case msg, ok := <-r.streamMutch:

			if ok {
				switch msg.(type) {
				case []*protobuf.VbKeyVersions:
					vbKeyVer := msg.([]*protobuf.VbKeyVersions)
					r.handleVbKeyVersions(vbKeyVer)

				default:
					r.handleStreamInfoMsg(msg)
				}

			} else {
				//stream library has closed this channel indicating
				//unexpected stream closure send the message to supervisor
				common.Errorf("MutationStreamReader::run Unexpected Mutation "+
					"Channel Close for Stream %v", r.streamId)
				msgErr := &MsgError{
					err: Error{code: ERROR_STREAM_READER_STREAM_SHUTDOWN,
						severity: FATAL,
						category: STREAM_READER}}
				r.supvRespch <- msgErr
			}

		case cmd, ok := <-r.supvCmdch:
			if ok {
				//handle commands from supervisor
				if cmd.GetMsgType() == STREAM_READER_SHUTDOWN {
					//shutdown and exit the stream reader loop
					r.Shutdown()
					r.supvCmdch <- &MsgSuccess{}
					return
				}
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

	common.Tracef("MutationStreamReader::handleSingleKeyVersion received KeyVersions %v", kv)

	for i, cmd := range kv.GetCommands() {

		//based on the type of command take appropriate action
		switch byte(cmd) {

		//case protobuf.Command_Upsert, protobuf.Command_Deletion, protobuf.Command_UpsertDeletion:
		case common.Upsert, common.Deletion, common.UpsertDeletion:

			mutationCount++
			if (mutationCount%10000 == 0) || mutationCount == 1 {
				common.Infof("MutationStreamReader:: MutationCount %v", mutationCount)
			}
			//allocate new mutation first time
			if mut == nil {
				//TODO use free list here to reuse the struct and reduce garbage
				mut = &MutationKeys{}
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
			mut.partnkeys = append(mut.partnkeys, kv.GetKeys()[i])

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

		case common.Snapshot:
			//get snapshot information from message
			typ, start, end := kv.Snapshot()

			snapshot := &MutationSnapshot{
				snapType: typ,
				start:    start,
				end:      end}

			//send message to supervisor to take decision
			msg := &MsgStream{mType: STREAM_READER_SNAPSHOT_MARKER,
				streamId: r.streamId,
				meta:     meta,
				snapshot: snapshot}

			r.supvRespch <- msg
		}
	}

	//place secKey in the right worker's queue
	if mut != nil {
		r.workerch[int(vbucket)%r.numWorkers] <- mut
	}

}

//startMutationStreamWorker is the worker which processes mutation in a worker queue
func (r *mutationStreamReader) startMutationStreamWorker(workerId int, stopch StopChannel) {

	common.Infof("MutationStreamReader::startMutationStreamWorker Stream Worker %v "+
		"Started for Stream %v.", workerId, r.streamId)

	for {
		select {
		case mut := <-r.workerch[workerId]:
			r.handleSingleMutation(mut)
		case <-stopch:
			common.Infof("MutationStreamReader::startMutationStreamWorker Stream Worker %v "+
				"Stopped for Stream %v", workerId, r.streamId)
			return
		}
	}

}

//handleSingleMutation enqueues mutation in the mutation queue
func (r *mutationStreamReader) handleSingleMutation(mut *MutationKeys) {

	common.Tracef("MutationStreamReader::handleSingleMutation received mutation %v", mut)

	//based on the index, enqueue the mutation in the right queue
	if q, ok := r.bucketQueueMap[mut.meta.bucket]; ok {
		q.queue.Enqueue(mut, mut.meta.vbucket)

	} else {
		common.Errorf("MutationStreamReader::handleSingleMutation got mutation for "+
			"unknown bucket. Skipped  %v", mut)
	}

}

//handleStreamInfoMsg handles the error messages from Dataport
func (r *mutationStreamReader) handleStreamInfoMsg(msg interface{}) {

	var supvMsg Message

	switch msg.(type) {

	case dataport.ConnectionError:
		common.Debugf("MutationStreamReader::handleStreamInfoMsg \n\tReceived ConnectionError "+
			"from Client for Stream %v.", r.streamId)

		//send a separate message for each bucket
		for bucket, vbList := range msg.(dataport.ConnectionError) {
			supvMsg = &MsgStreamInfo{mType: STREAM_READER_CONN_ERROR,
				streamId: r.streamId,
				bucket:   bucket,
				vbList:   copyVbList(vbList),
			}
			r.supvRespch <- supvMsg
		}

	default:
		common.Errorf("MutationStreamReader::handleStreamError \n\tReceived Unknown Message "+
			"from Client for Stream %v.", r.streamId)
		supvMsg = &MsgError{
			err: Error{code: ERROR_STREAM_READER_UNKNOWN_ERROR,
				severity: FATAL,
				category: STREAM_READER}}
		r.supvRespch <- supvMsg
	}
}

//handleSupervisorCommands handles the messages from Supervisor
func (r *mutationStreamReader) handleSupervisorCommands(cmd Message) Message {

	switch cmd.GetMsgType() {

	case STREAM_READER_UPDATE_QUEUE_MAP:

		common.Debugf("MutationStreamReader::handleSupervisorCommands %v", cmd)
		//stop all workers
		r.stopWorkers()

		//copy and store new bucketQueueMap
		bucketQueueMap := cmd.(*MsgUpdateBucketQueue).GetBucketQueueMap()
		r.bucketQueueMap = CopyBucketQueueMap(bucketQueueMap)

		//start all workers again
		r.startWorkers()

		return &MsgSuccess{}

	default:
		common.Errorf("MutationStreamReader::handleSupervisorCommands \n\tReceived Unknown Command %v", cmd)
		return &MsgError{
			err: Error{code: ERROR_STREAM_READER_UNKNOWN_COMMAND,
				severity: NORMAL,
				category: STREAM_READER}}

	}
}

//panicHandler handles the panic from underlying stream library
func (r *mutationStreamReader) panicHandler() {

	//panic recovery
	if rc := recover(); rc != nil {
		common.Fatalf("MutationStreamReader::panicHandler \n\tReceived Panic for Stream %v", r.streamId)
		//TODO Log the stack trace here
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

	common.Debugf("MutationStreamReader::startWorkers Starting All Stream Workers")

	//start worker goroutines to process incoming mutation concurrently
	for w := 0; w < r.numWorkers; w++ {
		go r.startMutationStreamWorker(w, r.workerStopCh[w])
	}
}

//stopWorkers stops all stream workers. This call doesn't return till
//all workers are stopped
func (r *mutationStreamReader) stopWorkers() {

	common.Debugf("MutationStreamReader::stopWorkers Stopping All Stream Workers")

	//stop all workers
	for _, ch := range r.workerStopCh {
		ch <- true
	}
}

//helper function to copy vbList
func copyVbList(vbList []uint16) []Vbucket {

	c := make([]Vbucket, len(vbList))

	for i, vb := range vbList {
		c[i] = Vbucket(vb)
	}

	return c
}
