//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package indexer

import (
	"sync"

	"github.com/couchbase/indexing/secondary/common"
)

//Flusher is the only component which does read/dequeue from a MutationQueue.
//As MutationQueue has a restriction of only single reader and writer per vbucket,
//flusher should not be invoked concurrently for a single MutationQueue.
type Flusher interface {

	//FlushQueueUptoStabilityTimestamp will flush the mutation queue upto
	//Timestamp provided. Can be stopped anytime by closing StopChannel.
	//Sends SUCCESS on the MsgChannel when its done flushing till timestamp.
	//Any error condition is reported back on the MsgChannel.
	//Caller can wait on MsgChannel after closing StopChannel
	//to get notified about shutdown completion.
	FlushQueueUptoTimestamp(q MutationQueue, streamId StreamId, sliceMap SliceMap,
		ts Timestamp, stopch StopChannel) MsgChannel

	//FlushQueueUptoTimestampWithoutPersistence will flush the mutation
	//queue upto Timestamp provided without actually persisting it.
	//Can be stopped anytime by closing the StopChannel.
	//Sends SUCCESS on the MsgChannel when its done flushing till timestamp.
	//Any error condition is reported back on the MsgChannel.
	//Caller can wait on MsgChannel after closing StopChannel
	//to get notified about shutdown completion.
	FlushQueueUptoTimestampWithoutPersistence(q MutationQueue, streamId StreamId,
		sliceMap SliceMap, ts Timestamp, stopch StopChannel) MsgChannel

	//FlushQueue will keep flushing the mutation queue till caller closes
	//the stop channel.Can be stopped anytime by closing the StopChannel.
	//Any error condition is reported back on the MsgChannel.
	//Caller can wait on MsgChannel after closing StopChannel to get
	//notified about shutdown completion.
	FlushQueue(q MutationQueue, streamId StreamId, sliceMap SliceMap,
		stopch StopChannel) MsgChannel

	//FlushQueue will keep flushing the mutation queue till caller closes
	//the stop channel without actually persisting the mutations.
	//Can be stopped anytime by closing the StopChannel.
	//Any error condition is reported back on the MsgChannel.
	//Caller can wait on MsgChannel after closing StopChannel to get
	//notified about shutdown completion.
	FlushQueueWithoutPersistence(q MutationQueue, streamId StreamId,
		sliceMap SliceMap, stopch StopChannel) MsgChannel

	//IsTimestampGreaterThanQueueLWT checks if each Vbucket in the Queue
	//has mutation with Seqno lower than the corresponding Seqno present
	//in the specified timestamp.
	IsQueueLWTLowerThanTimestamp(q MutationQueue, ts Timestamp) bool
}

type flusher struct {
}

//NewFlusher returns new instance of flusher
func NewFlusher() *flusher {
	return &flusher{}
}

//FlushQueueUptoStabilityTimestamp will flush the mutation queue upto the
//Timestamp provided.  This function will be used when:
//1. Flushing Maintenance Queue
//2. Flushing Maintenance Catchup Queue
//3. Flushing Backfill Queue
//
//Can be stopped anytime by closing StopChannel.
//Sends SUCCESS on the MsgChannel when its done flushing till timestamp.
//Any error condition is reported back on the MsgChannel.
//Caller can wait on MsgChannel after closing StopChannel to get notified
//about shutdown completion.
func (f *flusher) FlushQueueUptoTimestamp(q MutationQueue, streamId StreamId,
	sliceMap SliceMap, ts Timestamp, stopch StopChannel) MsgChannel {

	msgch := make(MsgChannel)
	go f.flushQueue(q, streamId, sliceMap, ts, true, stopch, msgch)
	return msgch
}

//FlushQueueUptoTimestampWithoutPersistence will flush the mutation queue
//upto the Timestamp provided without actually persisting it.
//Can be stopped anytime by closing the StopChannel.
//Sends SUCCESS on the MsgChannel when its done flushing till timestamp.
//Any error condition is reported back on the MsgChannel.
//Caller can wait on MsgChannel after closing StopChannel to get notified
//about shutdown completion.
func (f *flusher) FlushQueueUptoTimestampWithoutPersistence(q MutationQueue,
	streamId StreamId, sliceMap SliceMap, ts Timestamp, stopch StopChannel) MsgChannel {

	msgch := make(MsgChannel)
	go f.flushQueue(q, streamId, sliceMap, ts, false, stopch, msgch)
	return msgch
}

//FlushQueue will keep flushing the mutation queue till caller closes
//the stop channel.  This function will be used when:
//1. Flushing Backfill Catchup Queue
//
//Can be stopped anytime by closing the StopChannel.
//Any error condition is reported back on the MsgChannel.
//Caller can wait on MsgChannel after closing StopChannel to get notified
//about shutdown completion.
func (f *flusher) FlushQueue(q MutationQueue, streamId StreamId, sliceMap SliceMap,
	stopch StopChannel) MsgChannel {

	msgch := make(MsgChannel)
	go f.flushQueue(q, streamId, sliceMap, nil, true, stopch, msgch)
	return msgch
}

//FlushQueue will keep flushing the mutation queue till caller closes
//the stop channel without actually persisting the mutations
//Can be stopped anytime by closing the StopChannel.
//Any error condition is reported back on the MsgChannel.
//Caller can wait on MsgChannel after closing StopChannel to get notified
//about shutdown completion.
func (f *flusher) FlushQueueWithoutPersistence(q MutationQueue, streamId StreamId,
	sliceMap SliceMap, stopch StopChannel) MsgChannel {

	msgch := make(MsgChannel)
	go f.flushQueue(q, streamId, sliceMap, nil, false, stopch, msgch)
	return msgch
}

//flushQueue starts and waits for actual workers to flush the mutation queue.
//This function will close the done channel once all workers have finished.
//It also listens on the stop channel and will stop all workers if stop signal is received.
func (f *flusher) flushQueue(q MutationQueue, streamId StreamId, sliceMap SliceMap,
	ts Timestamp, persist bool, stopch StopChannel, msgch MsgChannel) {

	var wg sync.WaitGroup
	var i uint16

	numVbuckets := q.GetNumVbuckets()

	//create stop channel for each worker, to propagate the stop signal
	workerStopChannels := make([]StopChannel, numVbuckets)

	//create msg channel for workers to provide messages
	workerMsgCh := make(MsgChannel)

	for i = 0; i < numVbuckets; i++ {
		wg.Add(1)
		if ts == nil {
			go f.flushSingleVbucket(q, streamId, sliceMap, i, persist,
				workerStopChannels[i], workerMsgCh, wg)
		} else {
			go f.flushSingleVbucketUptoSeqno(q, streamId, sliceMap, i, ts[i],
				persist, workerStopChannels[i], workerMsgCh, wg)
		}
	}

	allWorkersDoneCh := make(DoneChannel)

	//wait for all workers to finish
	go func() {
		wg.Wait()
		close(allWorkersDoneCh) //send signal on channel to indicate all workers have finished
	}()

	//wait for upstream to signal stop or for all workers to signal done
	//or workers to send any message
	select {
	case <-stopch:
		//stop all workers
		for ch := range workerStopChannels {
			close(ch)
		}
		//wait for all workers to stop
		<-allWorkersDoneCh

		//wait for notification of all workers finishing
	case <-allWorkersDoneCh:

		//handle any message from workers
	case m, ok := <-workerMsgCh:
		if ok {
			//TODO identify the messages and handle
			//For now, just relay back the message
			msgch <- m
		}
		return
	}

	msgch <- &MsgSuccess{}
}

//flushSingleVbucket is the actual implementation which flushes the given queue
//for a single vbucket till stop signal
func (f *flusher) flushSingleVbucket(q MutationQueue, streamId StreamId,
	sliceMap SliceMap, vbucket uint16, persist bool, stopch StopChannel,
	workerMsgCh MsgChannel, wg sync.WaitGroup) {

	defer wg.Done()

	mutch, qstopch, err := q.Dequeue(vbucket)
	if err != nil {
		//TODO
	}

	ok := true
	var mut *common.Mutation

	//Process till supervisor asks to stop on the channel
	for ok {
		select {
		case mut, ok = <-mutch:
			if ok {
				if !persist {
					//No persistence is required. Just skip this mutation.
					continue
				}
				f.flushSingleMutation(mut, streamId, sliceMap, workerMsgCh)
			}
		case <-stopch:
			qstopch <- true
			return
		}
	}
}

//flushSingleVbucket is the actual implementation which flushes the given queue
//for a single vbucket till the given seqno or till the stop signal(whichever is earlier)
func (f *flusher) flushSingleVbucketUptoSeqno(q MutationQueue, streamId StreamId,
	sliceMap SliceMap, vbucket uint16, seqno uint64, persist bool, stopch StopChannel,
	workerMsgCh MsgChannel, wg sync.WaitGroup) {

	defer wg.Done()

	mutch, err := q.DequeueUptoSeqno(vbucket, seqno)
	if err != nil {
		//TODO
	}

	ok := true
	var mut *common.Mutation

	//Read till the channel is closed by queue indicating it has sent all the
	//sequence numbers requested
	for ok {
		select {
		case mut, ok = <-mutch:
			if ok {
				if !persist {
					//No persistence is required. Just skip this mutation.
					continue
				}
				f.flushSingleMutation(mut, streamId, sliceMap, workerMsgCh)
			}
		}
	}
}

//flushSingleMutation talks to persistence layer to store the mutations
//Any error from persistence layer is sent back on workerMsgCh
func (f *flusher) flushSingleMutation(mut *common.Mutation, streamId StreamId,
	sliceMap SliceMap, workerMsgCh MsgChannel) {

	//based on type of mutation message, take appropriate decision
	//if stream is MAINT_STREAM or MAINT_CATCHUP_STREAM, ignore mutations for indexes
	//in INIT, BUILD state
	//if stream is BACKFILL_STREAM, keep doing "commit" at regular intervals
	//send to persistence layer

}

//IsTimestampGreaterThanQueueLWT checks if each Vbucket in the Queue has
//mutation with Seqno lower than the corresponding Seqno present in the
//specified timestamp.
func (f *flusher) IsQueueLWTLowerThanTimestamp(q MutationQueue, ts Timestamp) bool {

	//each individual vbucket seqno should be lower than or equal to timestamp seqno
	for i, t := range ts {
		mut := q.PeekHead(uint16(i))
		if mut.Seqno > t {
			return false
		}
	}
	return true

}
