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
	"github.com/couchbase/indexing/secondary/common"
	"log"
	"sync"
)

//Flusher is the only component which does read/dequeue from a MutationQueue.
//As MutationQueue has a restriction of only single reader and writer per vbucket,
//flusher should not be invoked concurrently for a single MutationQueue.
type Flusher interface {

	//PersistUptoTS will flush the mutation queue upto Timestamp provided.
	//Can be stopped anytime by closing StopChannel.
	//Sends SUCCESS on the MsgChannel when its done flushing till TS.
	//Any error condition is reported back on the MsgChannel.
	//Caller can wait on MsgChannel after closing StopChannel
	//to get notified about shutdown completion.
	PersistUptoTS(q MutationQueue, streamId StreamId, indexInstMap common.IndexInstMap,
		indexPartnMap common.IndexPartnMap, ts Timestamp, stopch StopChannel) MsgChannel

	//DrainUptoTS will flush the mutation queue upto Timestamp
	//provided without actually persisting it.
	//Can be stopped anytime by closing the StopChannel.
	//Sends SUCCESS on the MsgChannel when its done flushing till timestamp.
	//Any error condition is reported back on the MsgChannel.
	//Caller can wait on MsgChannel after closing StopChannel
	//to get notified about shutdown completion.
	DrainUptoTS(q MutationQueue, streamId StreamId, ts Timestamp,
		stopch StopChannel) MsgChannel

	//Persist will keep flushing the mutation queue till caller closes
	//the stop channel.Can be stopped anytime by closing the StopChannel.
	//Any error condition is reported back on the MsgChannel.
	//Caller can wait on MsgChannel after closing StopChannel to get
	//notified about shutdown completion.
	Persist(q MutationQueue, streamId StreamId, indexInstMap common.IndexInstMap,
		indexPartnMap common.IndexPartnMap, stopch StopChannel) MsgChannel

	//Drain will keep flushing the mutation queue till caller closes
	//the stop channel without actually persisting the mutations.
	//Can be stopped anytime by closing the StopChannel.
	//Any error condition is reported back on the MsgChannel.
	//Caller can wait on MsgChannel after closing StopChannel to get
	//notified about shutdown completion.
	Drain(q MutationQueue, streamId StreamId, stopch StopChannel) MsgChannel

	//IsTimestampGreaterThanQueueLWT checks if each Vbucket in the Queue
	//has mutation with Seqno lower than the corresponding Seqno present
	//in the specified timestamp.
	IsQueueLWTLowerThanTimestamp(q MutationQueue, ts Timestamp) bool

	//GetQueueLWT returns the lowest seqno for each vbucket in the queue
	GetQueueLWT(q MutationQueue) Timestamp

	//GetQueueHWT returns the highest seqno for each vbucket in the queue
	GetQueueHWT(q MutationQueue) Timestamp
}

type flusher struct {
	indexInstMap  common.IndexInstMap
	indexPartnMap common.IndexPartnMap
}

//NewFlusher returns new instance of flusher
func NewFlusher() *flusher {
	return &flusher{}
}

//PersistUptoTS will flush the mutation queue upto the
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
func (f *flusher) PersistUptoTS(q MutationQueue, streamId StreamId,
	indexInstMap common.IndexInstMap, indexPartnMap common.IndexPartnMap,
	ts Timestamp, stopch StopChannel) MsgChannel {

	f.indexInstMap = indexInstMap
	f.indexPartnMap = indexPartnMap

	msgch := make(MsgChannel)
	go f.flushQueue(q, streamId, ts, true, stopch, msgch)
	return msgch
}

//DrainUptoTS will flush the mutation queue upto the Timestamp
//provided without actually persisting it.
//Can be stopped anytime by closing the StopChannel.
//Sends SUCCESS on the MsgChannel when its done flushing till timestamp.
//Any error condition is reported back on the MsgChannel.
//Caller can wait on MsgChannel after closing StopChannel to get notified
//about shutdown completion.
func (f *flusher) DrainUptoTS(q MutationQueue, streamId StreamId,
	ts Timestamp, stopch StopChannel) MsgChannel {

	msgch := make(MsgChannel)
	go f.flushQueue(q, streamId, ts, false, stopch, msgch)
	return msgch
}

//Persist will keep flushing the mutation queue till caller closes
//the stop channel.  This function will be used when:
//1. Flushing Backfill Catchup Queue
//
//Can be stopped anytime by closing the StopChannel.
//Any error condition is reported back on the MsgChannel.
//Caller can wait on MsgChannel after closing StopChannel to get notified
//about shutdown completion.
func (f *flusher) Persist(q MutationQueue, streamId StreamId,
	indexInstMap common.IndexInstMap, indexPartnMap common.IndexPartnMap,
	stopch StopChannel) MsgChannel {

	f.indexInstMap = indexInstMap
	f.indexPartnMap = indexPartnMap

	msgch := make(MsgChannel)
	go f.flushQueue(q, streamId, nil, true, stopch, msgch)
	return msgch
}

//Drain will keep flushing the mutation queue till caller closes
//the stop channel without actually persisting the mutations
//Can be stopped anytime by closing the StopChannel.
//Any error condition is reported back on the MsgChannel.
//Caller can wait on MsgChannel after closing StopChannel to get notified
//about shutdown completion.
func (f *flusher) Drain(q MutationQueue, streamId StreamId,
	stopch StopChannel) MsgChannel {

	msgch := make(MsgChannel)
	go f.flushQueue(q, streamId, nil, false, stopch, msgch)
	return msgch
}

//flushQueue starts and waits for actual workers to flush the mutation queue.
//This function will close the done channel once all workers have finished.
//It also listens on the stop channel and will stop all workers if stop signal is received.
func (f *flusher) flushQueue(q MutationQueue, streamId StreamId,
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
			go f.flushSingleVbucket(q, streamId, Vbucket(i),
				persist, workerStopChannels[i], workerMsgCh, wg)
		} else {
			go f.flushSingleVbucketUptoSeqno(q, streamId, Vbucket(i),
				ts[i], persist, workerStopChannels[i], workerMsgCh, wg)
		}
	}

	allWorkersDoneCh := make(DoneChannel)

	//wait for all workers to finish
	go func() {
		wg.Wait()
		//send signal on channel to indicate all workers have finished
		close(allWorkersDoneCh)
	}()

	//wait for upstream to signal stop or for all workers to signal done
	//or workers to send any message
	select {
	case <-stopch:
		//stop all workers
		for _, ch := range workerStopChannels {
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
	vbucket Vbucket, persist bool, stopch StopChannel,
	workerMsgCh MsgChannel, wg sync.WaitGroup) {

	defer wg.Done()

	mutch, qstopch, err := q.Dequeue(vbucket)
	if err != nil {
		//TODO
	}

	ok := true
	var mut *MutationKeys

	//Process till supervisor asks to stop on the channel
	for ok {
		select {
		case mut, ok = <-mutch:
			if ok {
				if !persist {
					//No persistence is required. Just skip this mutation.
					continue
				}
				f.flushSingleMutation(mut, streamId)
			}
		case <-stopch:
			qstopch <- true
			return
		}
	}
	workerMsgCh <- &MsgSuccess{}
}

//flushSingleVbucket is the actual implementation which flushes the given queue
//for a single vbucket till the given seqno or till the stop signal(whichever is earlier)
func (f *flusher) flushSingleVbucketUptoSeqno(q MutationQueue, streamId StreamId,
	vbucket Vbucket, seqno Seqno, persist bool, stopch StopChannel,
	workerMsgCh MsgChannel, wg sync.WaitGroup) {

	defer wg.Done()

	mutch, err := q.DequeueUptoSeqno(vbucket, seqno)
	if err != nil {
		//TODO
	}

	ok := true
	var mut *MutationKeys

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
				f.flushSingleMutation(mut, streamId)
			}
		}
	}
	workerMsgCh <- &MsgSuccess{}
}

//flushSingleMutation talks to persistence layer to store the mutations
//Any error from persistence layer is sent back on workerMsgCh
func (f *flusher) flushSingleMutation(mut *MutationKeys, streamId StreamId) {

	switch streamId {

	case MAINT_STREAM:
		f.flushMaintStreamMutation(mut)

	case MAINT_CATCHUP_STREAM:
		f.flushMaintCatchupStreamMutation(mut)

	case BACKFILL_STREAM:
		f.flushBackfillStreamMutation(mut)

	case BACKFILL_CATCHUP_STREAM:
		f.flushBackfillCatchupStreamMutation(mut)

	default:
		log.Println("flushSingleMutation: Invalid StreamId received", streamId)
	}
}

func (f *flusher) flushMaintStreamMutation(mut *MutationKeys) {

	var processedUpserts []common.IndexInstId
	for i, cmd := range mut.commands {

		switch cmd {

		case common.Upsert:
			processedUpserts = append(processedUpserts, mut.uuids[i])

			f.processUpsert(mut, i)

		case common.Deletion:
			f.processDelete(mut, i)

		case common.UpsertDeletion:

			var skipUpsertDeletion bool
			//if Upsert has been processed for this IndexInstId,
			//skip processing UpsertDeletion
			for _, id := range processedUpserts {
				if id == mut.uuids[i] {
					skipUpsertDeletion = true
				}
			}

			if skipUpsertDeletion {
				continue
			} else {
				f.processDelete(mut, i)
			}
		}
	}
}

func (f *flusher) processUpsert(mut *MutationKeys, i int) {

	var key Key
	var value Value
	var err error

	if key, err = NewKey(mut.keys[i], mut.docid); err != nil {

		log.Printf("flushMaintStreamMutation: Error Generating Key"+
			"From Mutation %v. Skipped. Error : %v", mut.keys[i], err)
		continue
	}

	if value, err = NewValue(mut.keys[i], mut.docid, mut.meta.vbucket,
		mut.meta.seqno); err != nil {

		log.Printf("flushMaintStreamMutation: Error Generating Value"+
			"From Mutation %v. Skipped. Error : %v", mut.keys[i], err)
		continue
	}

	var idxInst common.IndexInst
	var ok bool

	if idxInst, ok = f.indexInstMap[mut.uuids[i]]; !ok {
		log.Printf("flushMaintStreamMutation: Unknown Index Instance Id %v."+
			"Skipped Mutation Key %v", mut.uuids[i], mut.keys[i])
		continue
	}

	partnId := idxInst.Pc.GetPartitionIdByPartitionKey(mut.partnkeys[i])

	var partnInstMap common.PartitionInstMap
	if partnInstMap, ok = f.indexPartnMap[mut.uuids[i]]; !ok {
		log.Printf("flushMaintStreamMutation: Missing Partition Instance Map"+
			"for Index Inst Id %v. Skipped Mutation Key %v", mut.uuids[i], mut.keys[i])
		continue
	}

	if partnInst := partnInstMap[partnId]; ok {
		slice := partnInst.Sc.GetSliceByIndexKey(common.IndexKey(mut.keys[i]))
		if err := slice.Insert(key, value); err != nil {
			log.Printf("flushMaintStreamMutation: Error Inserting Key %v "+
				"Value %v in Slice %v", key, value, slice.Id())
		}
	} else {
		log.Printf("flushMaintStreamMutation: Partition Instance not found "+
			"for Id %v", partnId)
	}

}

func (f *flusher) processDelete(mut *MutationKeys, i int) {
	var idxInst common.IndexInst
	var ok bool

	if idxInst, ok = f.indexInstMap[mut.uuids[i]]; !ok {
		log.Printf("flushMaintStreamMutation: Unknown Index Instance Id %v."+
			"Skipped Mutation Key %v", mut.uuids[i], mut.keys[i])
		continue
	}

	partnId := idxInst.Pc.GetPartitionIdByPartitionKey(mut.partnkeys[i])

	var partnInstMap common.PartitionInstMap
	if partnInstMap, ok = f.indexPartnMap[mut.uuids[i]]; !ok {
		log.Printf("flushMaintStreamMutation: Missing Partition Instance Map"+
			"for Index Inst Id %v. Skipped Mutation Key %v", mut.uuids[i], mut.keys[i])
		continue
	}

	if partnInst := partnInstMap[partnId]; ok {
		slice := partnInst.Sc.GetSliceByIndexKey(common.IndexKey(mut.keys[i]))
		if err := slice.Delete(mut.docid); err != nil {
			log.Printf("flushMaintStreamMutation: Error Deleting DocId %v "+
				"from Slice %v", mut.docid, slice.Id())
		}
	} else {
		log.Printf("flushMaintStreamMutation: Partition Instance not found "+
			"for Id %v", partnId)
	}
}

func (f *flusher) flushMaintCatchupStreamMutation(mut *MutationKeys) {
	//TODO
}

func (f *flusher) flushBackfillStreamMutation(mut *MutationKeys) {
	//TODO
}

func (f *flusher) flushBackfillCatchupStreamMutation(mut *MutationKeys) {
	//TODO
}

//IsTimestampGreaterThanQueueLWT checks if each Vbucket in the Queue has
//mutation with Seqno lower than the corresponding Seqno present in the
//specified timestamp.
func (f *flusher) IsQueueLWTLowerThanTimestamp(q MutationQueue, ts Timestamp) bool {

	//each individual vbucket seqno should be lower than or equal to timestamp seqno
	for i, t := range ts {
		mut := q.PeekHead(Vbucket(i))
		if mut.meta.seqno > t {
			return false
		}
	}
	return true

}

//GetQueueLWT returns the lowest seqno for each vbucket in the queue
func (f *flusher) GetQueueLWT(q MutationQueue) Timestamp {

	var ts Timestamp
	var i uint16
	for i = 0; i < q.GetNumVbuckets(); i++ {
		if mut := q.PeekHead(Vbucket(i)); mut != nil {
			ts[i] = mut.meta.seqno
		} else {
			ts[i] = 0
		}
	}
	return ts
}

//GetQueueHWT returns the highest seqno for each vbucket in the queue
func (f *flusher) GetQueueHWT(q MutationQueue) Timestamp {

	var ts Timestamp
	var i uint16
	for i = 0; i < q.GetNumVbuckets(); i++ {
		if mut := q.PeekTail(Vbucket(i)); mut != nil {
			ts[i] = mut.meta.seqno
		} else {
			ts[i] = 0
		}
	}
	return ts
}
