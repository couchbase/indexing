//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package indexer

import (
	"fmt"
	"sync"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
)

// Flusher is the only component which does read/dequeue from a MutationQueue.
// As MutationQueue has a restriction of only single reader and writer per vbucket,
// flusher should not be invoked concurrently for a single MutationQueue.
type Flusher interface {

	//PersistUptoTS will flush the mutation queue upto Timestamp provided.
	//Can be stopped anytime by closing StopChannel.
	//Sends SUCCESS on the MsgChannel when its done flushing till TS.
	//Any error condition is reported back on the MsgChannel.
	//Caller can wait on MsgChannel after closing StopChannel
	//to get notified about shutdown completion.
	PersistUptoTS(q MutationQueue, streamId common.StreamId, keyspaceId string, indexInstMap common.IndexInstMap,
		indexPartnMap IndexPartnMap, ts Timestamp, changeVec []bool, stopch StopChannel) MsgChannel

	//DrainUptoTS will flush the mutation queue upto Timestamp
	//provided without actually persisting it.
	//Can be stopped anytime by closing the StopChannel.
	//Sends SUCCESS on the MsgChannel when its done flushing till timestamp.
	//Any error condition is reported back on the MsgChannel.
	//Caller can wait on MsgChannel after closing StopChannel
	//to get notified about shutdown completion.
	DrainUptoTS(q MutationQueue, streamId common.StreamId, keyspaceId string, ts Timestamp,
		changeVec []bool, stopch StopChannel) MsgChannel

	//Persist will keep flushing the mutation queue till caller closes
	//the stop channel.Can be stopped anytime by closing the StopChannel.
	//Any error condition is reported back on the MsgChannel.
	//Caller can wait on MsgChannel after closing StopChannel to get
	//notified about shutdown completion.
	Persist(q MutationQueue, streamId common.StreamId, keyspaceId string, indexInstMap common.IndexInstMap,
		indexPartnMap IndexPartnMap, stopch StopChannel) MsgChannel

	//Drain will keep flushing the mutation queue till caller closes
	//the stop channel without actually persisting the mutations.
	//Can be stopped anytime by closing the StopChannel.
	//Any error condition is reported back on the MsgChannel.
	//Caller can wait on MsgChannel after closing StopChannel to get
	//notified about shutdown completion.
	Drain(q MutationQueue, streamId common.StreamId, keyspaceId string, stopch StopChannel) MsgChannel

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
	// Lock is needed to give mutation_manager exclusive access to maps on soft deletes
	// after the soft deletion indexer can close and destroy the slice so no mutations
	// shuold be posted to slice
	sync.RWMutex
	indexInstMap  common.IndexInstMap
	indexPartnMap IndexPartnMap

	config common.Config
	stats  *IndexerStats
}

// NewFlusher returns new instance of flusher
func NewFlusher(config common.Config, stats *IndexerStats) *flusher {
	return &flusher{config: config, stats: stats}
}

func (f *flusher) GetInst(instId common.IndexInstId) (common.IndexInst, bool) {
	f.RLock()
	defer f.RUnlock()

	instMap := f.indexInstMap
	inst, ok := instMap[instId]
	return inst, ok
}

func (f *flusher) GetPartn(instId common.IndexInstId) (PartitionInstMap, bool) {
	f.RLock()
	defer f.RUnlock()

	partnMap := f.indexPartnMap
	pm, ok := partnMap[instId]
	return pm, ok
}

func (f *flusher) UpdateMaps(indexInstMap common.IndexInstMap, indexPartnMap IndexPartnMap) {
	f.Lock()
	defer f.Unlock()

	if indexInstMap != nil {
		f.indexInstMap = indexInstMap
	}

	if indexPartnMap != nil {
		f.indexPartnMap = indexPartnMap
	}
}

// PersistUptoTS will flush the mutation queue upto the
// Timestamp provided.  This function will be used when:
// 1. Flushing Maintenance Queue
// 2. Flushing Maintenance Catchup Queue
// 3. Flushing Backfill Queue
//
// Can be stopped anytime by closing StopChannel.
// Sends SUCCESS on the MsgChannel when its done flushing till timestamp.
// Any error condition is reported back on the MsgChannel.
// Caller can wait on MsgChannel after closing StopChannel to get notified
// about shutdown completion.
func (f *flusher) PersistUptoTS(q MutationQueue, streamId common.StreamId,
	keyspaceId string, indexInstMap common.IndexInstMap, indexPartnMap IndexPartnMap,
	ts Timestamp, changeVec []bool, countVec []uint64, stopch StopChannel) MsgChannel {

	logging.Verbosef("Flusher::PersistUptoTS %v %v Timestamp: %v PartnMap %v",
		streamId, keyspaceId, ts, indexPartnMap)

	f.indexInstMap = indexInstMap
	f.indexPartnMap = indexPartnMap

	msgch := make(MsgChannel)
	go f.flushQueue(q, streamId, keyspaceId, ts, changeVec, countVec, true, stopch, msgch)
	return msgch
}

// DrainUptoTS will flush the mutation queue upto the Timestamp
// provided without actually persisting it.
// Can be stopped anytime by closing the StopChannel.
// Sends SUCCESS on the MsgChannel when its done flushing till timestamp.
// Any error condition is reported back on the MsgChannel.
// Caller can wait on MsgChannel after closing StopChannel to get notified
// about shutdown completion.
func (f *flusher) DrainUptoTS(q MutationQueue, streamId common.StreamId,
	keyspaceId string, ts Timestamp, changeVec []bool, stopch StopChannel) MsgChannel {

	logging.Verbosef("Flusher::DrainUptoTS %v %v Timestamp: %v",
		streamId, keyspaceId, ts)

	msgch := make(MsgChannel)
	go f.flushQueue(q, streamId, keyspaceId, ts, changeVec, nil, false, stopch, msgch)
	return msgch
}

// Persist will keep flushing the mutation queue till caller closes
// the stop channel.  This function will be used when:
// 1. Flushing Backfill Catchup Queue
//
// Can be stopped anytime by closing the StopChannel.
// Any error condition is reported back on the MsgChannel.
// Caller can wait on MsgChannel after closing StopChannel to get notified
// about shutdown completion.
func (f *flusher) Persist(q MutationQueue, streamId common.StreamId,
	keyspaceId string, indexInstMap common.IndexInstMap, indexPartnMap IndexPartnMap,
	stopch StopChannel) MsgChannel {

	logging.Verbosef("Flusher::Persist %v %v", streamId, keyspaceId)

	f.indexInstMap = common.CopyIndexInstMap(indexInstMap)
	f.indexPartnMap = CopyIndexPartnMap(indexPartnMap)

	msgch := make(MsgChannel)
	go f.flushQueue(q, streamId, keyspaceId, nil, nil, nil, true, stopch, msgch)
	return msgch
}

// Drain will keep flushing the mutation queue till caller closes
// the stop channel without actually persisting the mutations
// Can be stopped anytime by closing the StopChannel.
// Any error condition is reported back on the MsgChannel.
// Caller can wait on MsgChannel after closing StopChannel to get notified
// about shutdown completion.
func (f *flusher) Drain(q MutationQueue, streamId common.StreamId,
	keyspaceId string, stopch StopChannel) MsgChannel {

	logging.Verbosef("Flusher::Drain %v %v", streamId, keyspaceId)

	msgch := make(MsgChannel)
	go f.flushQueue(q, streamId, keyspaceId, nil, nil, nil, false, stopch, msgch)
	return msgch
}

// flushQueue starts and waits for actual workers to flush the mutation queue.
// This function will close the done channel once all workers have finished.
// It also listens on the stop channel and will stop all workers if stop signal is received.
func (f *flusher) flushQueue(q MutationQueue, streamId common.StreamId, keyspaceId string,
	ts Timestamp, changeVec []bool, countVec []uint64, persist bool, stopch StopChannel, msgch MsgChannel) {

	var wg sync.WaitGroup
	var i uint16

	numVbuckets := q.GetNumVbuckets()

	//create stop channel for each worker, to propagate the stop signal
	var workerStopChannels []StopChannel

	//create msg channel for workers to provide messages
	workerMsgCh := make(MsgChannel, numVbuckets)

	for i = 0; i < numVbuckets; i++ {
		if ts == nil {
			wg.Add(1)
			stopch := make(StopChannel)
			workerStopChannels = append(workerStopChannels, stopch)
			go f.flushSingleVbucket(q, streamId, keyspaceId, Vbucket(i),
				persist, stopch, workerMsgCh, &wg)
		} else {
			if changeVec[i] {
				wg.Add(1)
				stopch := make(StopChannel)
				workerStopChannels = append(workerStopChannels, stopch)
				if countVec != nil && countVec[i] != 0 {
					go f.flushSingleVbucketN(q, streamId, keyspaceId, Vbucket(i),
						countVec[i], persist, stopch, workerMsgCh, &wg)
				} else {
					go f.flushSingleVbucketUptoSeqno(q, streamId, keyspaceId, Vbucket(i),
						ts[i], persist, stopch, workerMsgCh, &wg)
				}
			}
		}
	}

	allWorkersDoneCh := make(DoneChannel)

	//wait for all workers to finish
	go func() {
		logging.Tracef("Flusher::flushQueue Waiting for workers to finish Stream %v", streamId)
		wg.Wait()
		//send signal on channel to indicate all workers have finished
		logging.Tracef("Flusher::flushQueue All workers finished for Stream %v", streamId)
		close(allWorkersDoneCh)
	}()

	workerAborted := false
	//wait for upstream to signal stop or for all workers to signal done
	//or workers to send any message
	select {
	case <-stopch:
		logging.Debugf("Flusher::flushQueue Stopping All Workers")
		//stop all workers
		for _, ch := range workerStopChannels {
			close(ch)
		}
		//wait for all workers to stop
		<-allWorkersDoneCh
		logging.Debugf("Flusher::flushQueue Stopped All Workers")

		//wait for notification of all workers finishing
	case <-allWorkersDoneCh:

		//handle any message from workers
	case <-workerMsgCh:
		workerAborted = true
		<-allWorkersDoneCh
	}

	if workerAborted {
		msgch <- &MsgError{}
	} else {
		msgch <- &MsgSuccess{}
	}
}

// flushSingleVbucket is the actual implementation which flushes the given queue
// for a single vbucket till stop signal.
// (Currently not used.)
func (f *flusher) flushSingleVbucket(q MutationQueue, streamId common.StreamId,
	keyspaceId string, vbucket Vbucket, persist bool, stopch StopChannel,
	workerMsgCh MsgChannel, wg *sync.WaitGroup) {

	defer wg.Done()

	logging.Tracef("Flusher::flushSingleVbucket Started worker to flush vbucket: "+
		"%v for stream: %v", vbucket, streamId)

	mutch, qstopch, err := q.Dequeue(vbucket)
	if err != nil {
		//TODO
	}

	ok := true
	var mut *MutationKeys
	keyspaceStats := f.stats.GetKeyspaceStats(streamId, keyspaceId)

	var flushCount int64
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
				mut.Free()
				flushCount++
			}
		case <-stopch:
			if keyspaceStats != nil {
				keyspaceStats.mutationQueueSize.Add(0 - flushCount)
			}
			qstopch <- true
			return
		}
	}
	if keyspaceStats != nil {
		keyspaceStats.mutationQueueSize.Add(0 - flushCount)
	}
}

// flushSingleVbucketUptoSeqno is the actual implementation which flushes the given queue
// for a single vbucket till the given seqno or till the stop signal(whichever is earlier)
func (f *flusher) flushSingleVbucketUptoSeqno(q MutationQueue, streamId common.StreamId,
	keyspaceId string, vbucket Vbucket, seqno uint64, persist bool, stopch StopChannel,
	workerMsgCh MsgChannel, wg *sync.WaitGroup) {

	defer wg.Done()

	logging.LazyTrace(func() string {
		return fmt.Sprintf("Flusher::flushSingleVbucketUptoSeqno Started worker to flush vbucket: "+
			"%v till Seqno: %v for Stream: %v", vbucket, seqno, streamId)
	})

	mutch, errch, err := q.DequeueUptoSeqno(vbucket, seqno)
	if err != nil {
		//TODO
	}

	ok := true
	var mut *MutationKeys
	keyspaceStats := f.stats.GetKeyspaceStats(streamId, keyspaceId)

	//Read till the channel is closed by queue indicating it has sent all the
	//sequence numbers requested
	var flushCount int64
	for ok {
		select {
		case mut, ok = <-mutch:
			if ok {
				if !persist {
					//No persistence is required. Just skip this mutation.
					continue
				}
				f.flushSingleMutation(mut, streamId)
				mut.Free()
				flushCount++
			}
		case <-errch:
			if keyspaceStats != nil {
				keyspaceStats.mutationQueueSize.Add(0 - flushCount)
			}
			workerMsgCh <- &MsgError{}
			return

		}
	}
	if keyspaceStats != nil {
		keyspaceStats.mutationQueueSize.Add(0 - flushCount)
	}
}

// flushSingleVbucketN is the actual implementation which flushes the given queue
// for a single vbucket for the specified count or till the stop signal(whichever is earlier)
func (f *flusher) flushSingleVbucketN(q MutationQueue, streamId common.StreamId,
	keyspaceId string, vbucket Vbucket, count uint64, persist bool, stopch StopChannel,
	workerMsgCh MsgChannel, wg *sync.WaitGroup) {

	defer wg.Done()

	logging.LazyTrace(func() string {
		return fmt.Sprintf("Flusher::flushSingleVbucketUptoSeqno Started worker to flush vbucket: "+
			"%v Count: %v for Stream: %v KeyspaceId: %v", vbucket, count, streamId, keyspaceId)
	})

	mutch, errch, err := q.DequeueN(vbucket, count)
	if err != nil {
		//TODO
	}

	ok := true
	var mut *MutationKeys
	keyspaceStats := f.stats.GetKeyspaceStats(streamId, keyspaceId)

	var flushCount int64
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
				mut.Free()
				flushCount++
			}
		case <-errch:
			if keyspaceStats != nil {
				keyspaceStats.mutationQueueSize.Add(0 - flushCount)
			}
			workerMsgCh <- &MsgError{}
			return

		}
	}
	if keyspaceStats != nil {
		keyspaceStats.mutationQueueSize.Add(0 - flushCount)
	}
}

// flushSingleMutation talks to persistence layer to store the mutations
// Any error from persistence layer is sent back on workerMsgCh
func (f *flusher) flushSingleMutation(mut *MutationKeys, streamId common.StreamId) {

	switch streamId {

	case common.MAINT_STREAM, common.INIT_STREAM, common.CATCHUP_STREAM:
		f.flush(mut, streamId)

	default:
		logging.Errorf("Flusher::flushSingleMutation Invalid StreamId: %v", streamId)
	}
}

func (f *flusher) flush(mutk *MutationKeys, streamId common.StreamId) {

	logging.LazyTrace(func() string {
		return fmt.Sprintf("Flusher::flush Flushing Stream %v Mutations %v", streamId, logging.TagUD(mutk))
	})

	processedUpserts := make(map[common.IndexInstId]bool)
	for _, mut := range mutk.mut {

		if mut.command == common.Filler {
			continue
		}

		var idxInst common.IndexInst
		var ok bool
		if idxInst, ok = f.GetInst(mut.uuid); !ok {
			logging.LazyTrace(func() string {
				return fmt.Sprintf("Flusher::flush Unknown Index Instance Id %v. "+
					"Skipped Mutation Key %v", mut.uuid, logging.TagUD(mut.key))
			})
			continue
		}

		//Skip this mutation if the index doesn't belong to the stream being flushed
		if streamId != idxInst.Stream && streamId != common.CATCHUP_STREAM {
			logging.LazyTrace(func() string {
				return fmt.Sprintf("Flusher::flush Found Mutation For IndexId: %v Stream: %v In "+
					"Stream: %v. Skipped Mutation Key %v", idxInst.InstId, idxInst.Stream,
					streamId, logging.TagUD(mut.key))
			})
			continue
		}

		//Skip mutations for indexes in DELETED state. This may happen if complete
		//couldn't happen when processing drop index.
		if idxInst.State == common.INDEX_STATE_DELETED {
			logging.LazyTrace(func() string {
				return fmt.Sprintf("Flusher::flush Found Mutation For IndexId: %v In "+
					"DELETED State. Skipped Mutation Key %v", idxInst.InstId, logging.TagUD(mut.key))
			})
			continue
		}

		immutable := idxInst.Defn.Immutable

		switch mut.command {

		case common.Upsert:
			processedUpserts[mut.uuid] = true

			f.processUpsert(mut, mutk.docid, mutk.meta)

		case common.Deletion:
			f.processDelete(mut, mutk.docid, mutk.meta)

		case common.UpsertDeletion:

			//skip UpsertDeletion if index has immutable partition
			if immutable {
				continue
			}

			var skipUpsertDeletion bool
			//if Upsert has been processed for this IndexInstId,
			//skip processing UpsertDeletion
			if _, ok := processedUpserts[mut.uuid]; ok {
				skipUpsertDeletion = true
			}

			if skipUpsertDeletion {
				continue
			} else {
				f.processUpsertDelete(mut, mutk.docid, mutk.meta)
			}

		default:
			logging.Errorf("Flusher::flush Unknown mutation type received. Skipped %v",
				logging.TagUD(mut.key))
		}
	}
}

func (f *flusher) processUpsert(mut *Mutation, docid []byte, meta *MutationMeta) {

	idxInst, valid := f.GetInst(mut.uuid)
	if !valid {
		return
	}

	partnId := idxInst.Pc.GetPartitionIdByPartitionKey(mut.partnkey)

	var partnInstMap PartitionInstMap
	var ok bool
	if partnInstMap, ok = f.GetPartn(mut.uuid); !ok {
		logging.Errorf("Flusher::processUpsert Missing Partition Instance Map"+
			"for IndexInstId: %v. Skipped Mutation Key: %v", mut.uuid, logging.TagUD(mut.key))
		return
	}

	if partnInst, ok := partnInstMap[partnId]; ok {
		slice := partnInst.Sc.GetSliceByIndexKey(mut.key)
		if err := slice.Insert(mut.key, docid, mut.vectors, meta); err != nil {
			logging.Errorf("Flusher::processUpsert Error indexing Key: %s "+
				"docid: %s in Slice: %v. Error: %v. Skipped.",
				logging.TagUD(mut.key), logging.TagStrUD(docid), slice.Id(), err)

			if err2 := slice.Delete(docid, meta); err2 != nil {
				logging.Errorf("Flusher::processUpsert Error removing entry due to error %v Key: %s "+
					"docid: %s in Slice: %v. Error: %v", err, logging.TagUD(mut.key), logging.TagStrUD(docid), slice.Id(), err2)
			}
		}
	} else {
		logging.LazyDebug(func() string {
			return fmt.Sprintf("Flusher::processUpsert Partition Instance not found "+
				"for instId: %v, Id: %v Skipped Mutation Key: %v", mut.uuid, partnId, logging.TagUD(mut.key))
		})
	}

}

func (f *flusher) processDelete(mut *Mutation, docid []byte, meta *MutationMeta) {

	var partnInstMap PartitionInstMap
	var indexInst common.IndexInst
	var ok bool
	if partnInstMap, ok = f.GetPartn(mut.uuid); !ok {
		logging.Errorf("Flusher:processDelete Missing Partition Instance Map"+
			"for IndexInstId: %v. Skipped Mutation Key: %v", mut.uuid, logging.TagUD(mut.key))
		return
	}

	if indexInst, ok = f.GetInst(mut.uuid); !ok {
		logging.Errorf("Flusher:processDelete Missing in Index Instance Map"+
			"for IndexInstId: %v. Skipped Mutation Key: %v", mut.uuid, logging.TagUD(mut.key))
		return
	}

	isPartnKeyDocId := indexInst.Defn.IsPartnKeyDocId

	// Process delete only for the partition as partnKey is always available
	// if partnKey is based on meta().id or its variant like meta(self).id
	if isPartnKeyDocId && len(mut.partnkey) > 0 {
		partnId := indexInst.Pc.GetPartitionIdByPartitionKey(mut.partnkey)

		if partnInst, ok := partnInstMap[partnId]; ok {
			slice := partnInst.Sc.GetSliceByIndexKey(mut.key)
			if err := slice.Delete(docid, meta); err != nil {
				logging.Errorf("Flusher::processDelete Error Deleting DocId: %v "+
					"from Slice: %v", logging.TagStrUD(docid), slice.Id())
			}
		} else {
			logging.LazyDebug(func() string {
				return fmt.Sprintf("Flusher::processDelete Partition Instance not found "+
					"for inst: %v, partnId: %v Skipped Mutation Key: %v", mut.uuid, partnId, logging.TagUD(mut.key))
			})
		}
	} else {
		for _, partnInst := range partnInstMap {
			slice := partnInst.Sc.GetSliceByIndexKey(mut.key)
			if err := slice.Delete(docid, meta); err != nil {
				logging.Errorf("Flusher::processDelete Error Deleting DocId: %v "+
					"from Slice: %v", logging.TagStrUD(docid), slice.Id())
			}
		}
	}
}

func (f *flusher) processUpsertDelete(mut *Mutation, docid []byte, meta *MutationMeta) {

	idxInst, _ := f.GetInst(mut.uuid)
	if !common.IsPartitioned(idxInst.Defn.PartitionScheme) || len(mut.partnkey) == 0 {
		// Delete from all partitions if partition key is empty or it is a non partitioned index
		f.processDelete(mut, docid, meta)
		return
	}

	var partnInstMap PartitionInstMap
	var ok bool
	if partnInstMap, ok = f.GetPartn(mut.uuid); !ok {
		logging.Errorf("Flusher:processDelete Missing Partition Instance Map"+
			"for IndexInstId: %v. Skipped Mutation Key: %v", mut.uuid, logging.TagUD(mut.key))
		return
	}

	partnId := idxInst.Pc.GetPartitionIdByPartitionKey(mut.partnkey)
	if partnInst, ok := partnInstMap[partnId]; ok {
		slice := partnInst.Sc.GetSliceByIndexKey(mut.key)
		if err := slice.Delete(docid, meta); err != nil {
			logging.Errorf("Flusher::processDelete Error Deleting DocId: %v "+
				"from Slice: %v", logging.TagStrUD(docid), slice.Id())
		}
	}
}

// IsTimestampGreaterThanQueueLWT checks if each Vbucket in the Queue has
// mutation with Seqno lower than the corresponding Seqno present in the
// specified timestamp.
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

// GetQueueLWT returns the lowest seqno for each vbucket in the queue
func (f *flusher) GetQueueLWT(q MutationQueue) Timestamp {

	ts := NewTimestamp(int(q.GetNumVbuckets()))
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

// GetQueueHWT returns the highest seqno for each vbucket in the queue
func (f *flusher) GetQueueHWT(q MutationQueue) Timestamp {

	ts := NewTimestamp(int(q.GetNumVbuckets()))
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
