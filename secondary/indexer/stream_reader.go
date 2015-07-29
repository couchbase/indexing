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
	"fmt"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/dataport"
	"github.com/couchbase/indexing/secondary/logging"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/data"
	"sync"
)

//MutationStreamReader reads a Dataport and stores the incoming mutations
//in mutation queue. This is the only component writing to a mutation queue.
type MutationStreamReader interface {
	Shutdown()
}

const DEFAULT_SYNC_TIMEOUT = 40

type mutationStreamReader struct {
	mutationCount uint64
	snapStart     uint64
	snapEnd       uint64

	stream   *dataport.Server //handle to the Dataport server
	streamId common.StreamId

	streamMutch chan interface{} //Dataport channel

	supvCmdch  MsgChannel //supervisor sends commands on this channel
	supvRespch MsgChannel //channel to send any message to supervisor

	numWorkers int // number of workers to process mutation stream

	workerch      []MutationChannel //buffered channel for each worker
	workerStopCh  []StopChannel     //stop channels of workers
	workerWaitGrp sync.WaitGroup

	syncStopCh StopChannel
	syncLock   sync.Mutex

	bucketQueueMap BucketQueueMap //indexId to mutation queue map

	bucketFilterMap   map[string]*common.TsVbuuid
	bucketPrevSnapMap map[string]*common.TsVbuuid
	bucketSyncDue     map[string]bool

	//local variables
	skipMutation bool
	evalFilter   bool
	snapType     uint32

	killch chan bool // kill chan for the main loop

	stats IndexerStatsHolder
}

//CreateMutationStreamReader creates a new mutation stream and starts
//a reader to listen and process the mutations.
//In case returned MutationStreamReader is nil, Message will have the error msg.
func CreateMutationStreamReader(streamId common.StreamId, bucketQueueMap BucketQueueMap,
	bucketFilter map[string]*common.TsVbuuid, supvCmdch MsgChannel, supvRespch MsgChannel,
	numWorkers int, stats *IndexerStats) (MutationStreamReader, Message) {

	//start a new mutation stream
	streamMutch := make(chan interface{}, DATAPORT_MUTATION_BUFFER)
	config := common.SystemConfig.SectionConfig(
		"indexer.dataport.", true /*trim*/)
	stream, err := dataport.NewServer(
		string(StreamAddrMap[streamId]),
		common.SystemConfig["maxVbuckets"].Int(),
		config, streamMutch)
	if err != nil {
		//return stream init error
		logging.Errorf("MutationStreamReader: Error returned from NewServer."+
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
		stream:            stream,
		streamMutch:       streamMutch,
		supvCmdch:         supvCmdch,
		supvRespch:        supvRespch,
		numWorkers:        numWorkers,
		workerch:          make([]MutationChannel, numWorkers),
		workerStopCh:      make([]StopChannel, numWorkers),
		syncStopCh:        make(StopChannel),
		bucketQueueMap:    CopyBucketQueueMap(bucketQueueMap),
		bucketFilterMap:   make(map[string]*common.TsVbuuid),
		bucketPrevSnapMap: make(map[string]*common.TsVbuuid),
		bucketSyncDue:     make(map[string]bool),
		killch:            make(chan bool),
	}

	r.stats.Set(stats)
	r.initBucketFilter(bucketFilter)

	//start the main reader loop
	go r.run()
	go r.listenSupvCmd()

	go r.syncWorker()

	//init worker buffers
	for w := 0; w < r.numWorkers; w++ {
		r.workerch[w] = make(MutationChannel, MAX_STREAM_READER_WORKER_BUFFER)
	}

	//start stream workers
	r.startWorkers()

	return r, &MsgSuccess{}
}

//Shutdown shuts down the mutation stream and all workers.
//This call doesn't return till shutdown is complete.
func (r *mutationStreamReader) Shutdown() {

	logging.Infof("MutationStreamReader:Shutdown StreamReader %v", r.streamId)

	//stop sync worker
	close(r.syncStopCh)

	//close the mutation stream
	r.stream.Close()

	//stop all workers
	r.stopWorkers()

}

//run starts the stream reader loop which listens to message from
//mutation stream
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
				logging.Errorf("MutationStreamReader::run Unexpected Mutation "+
					"Channel Close for Stream %v", r.streamId)
				msgErr := &MsgError{
					err: Error{code: ERROR_STREAM_READER_STREAM_SHUTDOWN,
						severity: FATAL,
						category: STREAM_READER}}
				r.supvRespch <- msgErr
			}

		case <-r.killch:
			return
		}
	}

}

//run starts the stream reader loop which listens to message from
//the supervisor
func (r *mutationStreamReader) listenSupvCmd() {

	//panic handler
	defer r.panicHandler()

	for {
		cmd, ok := <-r.supvCmdch
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
			close(r.killch)
			r.Shutdown()
			return

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

	meta := NewMutationMeta()
	meta.bucket = bucket
	meta.vbucket = vbucket
	meta.vbuuid = vbuuid
	meta.seqno = Seqno(kv.GetSeqno())

	defer meta.Free()

	var mutk *MutationKeys
	r.skipMutation = false
	r.evalFilter = true

	logging.LazyTrace(func() string {
		return fmt.Sprintf("MutationStreamReader::handleSingleKeyVersion received KeyVersions %v", kv)
	})

	for i, cmd := range kv.GetCommands() {

		//based on the type of command take appropriate action
		switch byte(cmd) {

		//case protobuf.Command_Upsert, protobuf.Command_Deletion, protobuf.Command_UpsertDeletion:
		case common.Upsert, common.Deletion, common.UpsertDeletion:

			//As there can multiple keys in a KeyVersion for a mutation,
			//filter needs to be evaluated and set only once.
			if r.evalFilter {
				r.evalFilter = false
				//check the bucket filter to see if this mutation can be processed
				//valid mutation will increment seqno of the filter
				if !r.checkAndSetBucketFilter(meta) {
					r.skipMutation = true
				}
			}

			if r.skipMutation {
				continue
			}

			r.logReaderStat()

			//allocate new mutation first time
			if mutk == nil {
				//TODO use free list here to reuse the struct and reduce garbage
				mutk = NewMutationKeys()
				mutk.meta = meta.Clone()
				mutk.docid = kv.GetDocid()
				mutk.mut = mutk.mut[:0]
			}

			mut := NewMutation()
			mut.uuid = common.IndexInstId(kv.GetUuids()[i])
			mut.key = kv.GetKeys()[i]
			mut.command = byte(kv.GetCommands()[i])

			mutk.mut = append(mutk.mut, mut)

		case common.DropData:
			//send message to supervisor to take decision
			msg := &MsgStream{mType: STREAM_READER_STREAM_DROP_DATA,
				streamId: r.streamId,
				meta:     meta.Clone()}
			r.supvRespch <- msg

		case common.StreamBegin:

			r.updateVbuuidInFilter(meta)

			//send message to supervisor to take decision
			msg := &MsgStream{mType: STREAM_READER_STREAM_BEGIN,
				streamId: r.streamId,
				meta:     meta.Clone()}
			r.supvRespch <- msg

		case common.StreamEnd:
			//send message to supervisor to take decision
			msg := &MsgStream{mType: STREAM_READER_STREAM_END,
				streamId: r.streamId,
				meta:     meta.Clone()}
			r.supvRespch <- msg

		case common.Snapshot:
			//get snapshot information from message
			r.snapType, r.snapStart, r.snapEnd = kv.Snapshot()

			// Snapshot marker can be processed only if
			// they belong to ondisk type or inmemory type.
			if r.snapType&(0x1|0x2) != 0 {
				r.updateSnapInFilter(meta, r.snapStart, r.snapEnd)
			}

		}
	}

	//place secKey in the right worker's queue
	if mutk != nil {
		r.workerch[int(vbucket)%r.numWorkers] <- mutk
	}

}

//startMutationStreamWorker is the worker which processes mutation in a worker queue
func (r *mutationStreamReader) startMutationStreamWorker(workerId int, stopch StopChannel) {

	logging.Debugf("MutationStreamReader::startMutationStreamWorker Stream Worker %v "+
		"Started for Stream %v.", workerId, r.streamId)

	defer r.workerWaitGrp.Done()

	var mut *MutationKeys

	for {
		select {
		case mut = <-r.workerch[workerId]:
			r.handleSingleMutation(mut, stopch)
		case <-stopch:
			logging.Debugf("MutationStreamReader::startMutationStreamWorker Stream Worker %v "+
				"Stopped for Stream %v", workerId, r.streamId)
			return
		}
	}

}

//handleSingleMutation enqueues mutation in the mutation queue
func (r *mutationStreamReader) handleSingleMutation(mut *MutationKeys, stopch StopChannel) {

	logging.LazyTrace(func() string {
		return fmt.Sprintf("MutationStreamReader::handleSingleMutation received mutation %v", mut)
	})

	//based on the index, enqueue the mutation in the right queue
	if q, ok := r.bucketQueueMap[mut.meta.bucket]; ok {
		q.queue.Enqueue(mut, mut.meta.vbucket, stopch)

		stats := r.stats.Get()
		if rstats, ok := stats.buckets[mut.meta.bucket]; ok {
			rstats.mutationQueueSize.Add(1)
			rstats.numMutationsQueued.Add(1)
		}

	} else {
		logging.Errorf("MutationStreamReader::handleSingleMutation got mutation for "+
			"unknown bucket. Skipped  %v", mut)
	}

}

//handleStreamInfoMsg handles the error messages from Dataport
func (r *mutationStreamReader) handleStreamInfoMsg(msg interface{}) {

	var supvMsg Message

	switch msg.(type) {

	case dataport.ConnectionError:
		logging.Debugf("MutationStreamReader::handleStreamInfoMsg \n\tReceived ConnectionError "+
			"from Client for Stream %v %v.", r.streamId, msg.(dataport.ConnectionError))

		//send a separate message for each bucket. If the ConnError is with empty vblist,
		//the message is ignored.
		connErr := msg.(dataport.ConnectionError)
		if len(connErr) != 0 {
			for bucket, vbList := range connErr {
				supvMsg = &MsgStreamInfo{mType: STREAM_READER_CONN_ERROR,
					streamId: r.streamId,
					bucket:   bucket,
					vbList:   copyVbList(vbList),
				}
				r.supvRespch <- supvMsg
			}
		} else {
			supvMsg = &MsgStreamInfo{mType: STREAM_READER_CONN_ERROR,
				streamId: r.streamId,
				bucket:   "",
				vbList:   []Vbucket(nil),
			}
			r.supvRespch <- supvMsg
		}

	default:
		logging.Errorf("MutationStreamReader::handleStreamError \n\tReceived Unknown Message "+
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

		logging.Debugf("MutationStreamReader::handleSupervisorCommands %v", cmd)
		//stop all workers
		r.stopWorkers()

		//copy and store new bucketQueueMap
		req := cmd.(*MsgUpdateBucketQueue)
		bucketQueueMap := req.GetBucketQueueMap()
		r.bucketQueueMap = CopyBucketQueueMap(bucketQueueMap)
		r.stats.Set(req.GetStatsObject())

		bucketFilter := req.GetBucketFilter()
		r.initBucketFilter(bucketFilter)

		//start all workers again
		r.startWorkers()

		return &MsgSuccess{}

	default:
		logging.Errorf("MutationStreamReader::handleSupervisorCommands Received Unknown Command %v", cmd)
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
		logging.Fatalf("MutationStreamReader::panicHandler Received Panic for Stream %v", r.streamId)
		var err error
		switch x := rc.(type) {
		case string:
			err = errors.New(x)
		case error:
			err = x
		default:
			err = errors.New("Unknown panic")
		}

		logging.Fatalf("StreamReader Panic Err %v", err)
		logging.Fatalf("%s", logging.StackTrace())

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

	logging.Debugf("MutationStreamReader::startWorkers Starting All Stream Workers")

	//start worker goroutines to process incoming mutation concurrently
	for w := 0; w < r.numWorkers; w++ {
		r.workerWaitGrp.Add(1)
		r.workerStopCh[w] = make(StopChannel)
		go r.startMutationStreamWorker(w, r.workerStopCh[w])
	}
}

//stopWorkers stops all stream workers. This call doesn't return till
//all workers are stopped
func (r *mutationStreamReader) stopWorkers() {

	logging.Debugf("MutationStreamReader::stopWorkers Stopping All Stream Workers")

	//stop all workers
	for _, ch := range r.workerStopCh {
		close(ch)
	}

	//wait for all workers to finish
	r.workerWaitGrp.Wait()
}

//initBucketFilter initializes the bucket filter
func (r *mutationStreamReader) initBucketFilter(bucketFilter map[string]*common.TsVbuuid) {

	r.syncLock.Lock()
	defer r.syncLock.Unlock()

	//allocate a new filter for the buckets which don't
	//have a filter yet
	for b, q := range r.bucketQueueMap {
		if _, ok := r.bucketFilterMap[b]; !ok {
			logging.Tracef("MutationStreamReader::initBucketFilter Added new filter "+
				"for Bucket %v Stream %v", b, r.streamId)

			//if there is non-nil filter, use that. otherwise use a zero filter.
			if filter, ok := bucketFilter[b]; ok && filter != nil {
				r.bucketFilterMap[b] = filter.Copy()
				r.bucketPrevSnapMap[b] = filter.Copy()
				//reset vbuuids to 0 in filter. mutations for a vbucket are
				//only processed after streambegin is received, which will set
				//the vbuuid again.
				for i := 0; i < len(filter.Vbuuids); i++ {
					r.bucketFilterMap[b].Vbuuids[i] = 0
				}
			} else {
				r.bucketFilterMap[b] = common.NewTsVbuuid(b, int(q.queue.GetNumVbuckets()))
				r.bucketPrevSnapMap[b] = common.NewTsVbuuid(b, int(q.queue.GetNumVbuckets()))
			}

			r.bucketSyncDue[b] = false
		}
	}

	//remove the bucket filters for which bucket doesn't exist anymore
	for b, _ := range r.bucketFilterMap {
		if _, ok := r.bucketQueueMap[b]; !ok {
			logging.Tracef("MutationStreamReader::initBucketFilter Deleted filter "+
				"for Bucket %v Stream %v", b, r.streamId)
			delete(r.bucketFilterMap, b)
			delete(r.bucketPrevSnapMap, b)
			delete(r.bucketSyncDue, b)
		}
	}

}

//setBucketFilter sets the bucket filter based on seqno/vbuuid of mutation.
//filter is set when stream begin is received.
func (r *mutationStreamReader) setBucketFilter(meta *MutationMeta) {

	r.syncLock.Lock()
	defer r.syncLock.Unlock()

	if filter, ok := r.bucketFilterMap[meta.bucket]; ok {
		filter.Seqnos[meta.vbucket] = uint64(meta.seqno)
		filter.Vbuuids[meta.vbucket] = uint64(meta.vbuuid)
		logging.Tracef("MutationStreamReader::setBucketFilter Vbucket %v "+
			"Seqno %v Bucket %v Stream %v", meta.vbucket, meta.seqno, meta.bucket, r.streamId)
	} else {
		logging.Errorf("MutationStreamReader::setBucketFilter Missing bucket "+
			"%v in Filter for Stream %v", meta.bucket, r.streamId)
	}

}

//checkAndSetBucketFilter checks if mutation can be processed
//based on the current filter. Filter is also updated with new
//seqno/vbuuid if mutations can be processed.
func (r *mutationStreamReader) checkAndSetBucketFilter(meta *MutationMeta) bool {

	r.syncLock.Lock()
	defer r.syncLock.Unlock()

	if filter, ok := r.bucketFilterMap[meta.bucket]; ok {
		//the filter only checks if seqno of incoming mutation is greater than
		//the existing filter. Also there should be a valid StreamBegin(vbuuid)
		//for the vbucket. The vbuuid check is only to ensure that after stream
		//restart for a bucket, mutations get processed only after StreamBegin.
		//There can be residual mutations in projector endpoint queue after
		//a bucket gets deleted from stream in case of multiple buckets.
		//The vbuuid doesn't get reset after StreamEnd/StreamBegin. The
		//filter can be extended for that check if required.
		if uint64(meta.seqno) > filter.Seqnos[meta.vbucket] &&
			filter.Vbuuids[meta.vbucket] != 0 {
			filter.Seqnos[meta.vbucket] = uint64(meta.seqno)
			filter.Vbuuids[meta.vbucket] = uint64(meta.vbuuid)
			r.bucketSyncDue[meta.bucket] = true
			return true
		} else {
			logging.Tracef("MutationStreamReader::checkAndSetBucketFilter Skipped "+
				"Mutation %v for Bucket %v Stream %v. Current Filter %v", meta,
				meta.bucket, r.streamId, filter.Seqnos[meta.vbucket])
			return false
		}
	} else {
		logging.Errorf("MutationStreamReader::checkAndSetBucketFilter Missing"+
			"bucket %v in Filter for Stream %v", meta.bucket, r.streamId)
		return false
	}
}

//updates snapshot information in bucket filter
func (r *mutationStreamReader) updateSnapInFilter(meta *MutationMeta,
	snapStart uint64, snapEnd uint64) {

	r.syncLock.Lock()
	defer r.syncLock.Unlock()

	if filter, ok := r.bucketFilterMap[meta.bucket]; ok {
		if snapEnd > filter.Snapshots[meta.vbucket][1] {
			//store the existing snap marker in prevSnap map
			prevSnap := r.bucketPrevSnapMap[meta.bucket]
			prevSnap.Snapshots[meta.vbucket][0] = filter.Snapshots[meta.vbucket][0]
			prevSnap.Snapshots[meta.vbucket][1] = filter.Snapshots[meta.vbucket][1]
			prevSnap.Vbuuids[meta.vbucket] = filter.Vbuuids[meta.vbucket]

			filter.Snapshots[meta.vbucket][0] = snapStart
			filter.Snapshots[meta.vbucket][1] = snapEnd
		} else {
			logging.Errorf("MutationStreamReader::updateSnapInFilter Skipped "+
				"Snapshot %v-%v for vb %v %v %v. Current Filter %v", snapStart,
				snapEnd, meta.vbucket, meta.bucket, r.streamId,
				filter.Snapshots[meta.vbucket][1])
		}
	} else {
		logging.Errorf("MutationStreamReader::updateSnapInFilter Missing"+
			"bucket %v in Filter for Stream %v", meta.bucket, r.streamId)
	}

}

//updates vbuuid information in bucket filter
func (r *mutationStreamReader) updateVbuuidInFilter(meta *MutationMeta) {

	r.syncLock.Lock()
	defer r.syncLock.Unlock()

	if filter, ok := r.bucketFilterMap[meta.bucket]; ok {
		filter.Vbuuids[meta.vbucket] = uint64(meta.vbuuid)
	} else {
		logging.Errorf("MutationStreamReader::updateVbuuidInFilter Missing"+
			"bucket %v vb %v vbuuid %v in Filter for Stream %v", meta.bucket,
			meta.vbucket, meta.vbuuid, r.streamId)
	}

}
func (r *mutationStreamReader) syncWorker() {

	ticker := time.NewTicker(time.Millisecond * DEFAULT_SYNC_TIMEOUT)

	for {
		select {
		case <-ticker.C:
			r.maybeSendSync()
		case <-r.syncStopCh:
			return
		}
	}
}

//send a sync message if its due
func (r *mutationStreamReader) maybeSendSync() {

	r.syncLock.Lock()
	defer r.syncLock.Unlock()

	for bucket, syncDue := range r.bucketSyncDue {
		if syncDue {
			hwt := common.NewTsVbuuidCached(bucket, len(r.bucketFilterMap[bucket].Seqnos))
			hwt.CopyFrom(r.bucketFilterMap[bucket])
			prevSnap := common.NewTsVbuuidCached(bucket, len(r.bucketFilterMap[bucket].Seqnos))
			prevSnap.CopyFrom(r.bucketPrevSnapMap[bucket])
			r.bucketSyncDue[bucket] = false
			go func(hwt *common.TsVbuuid, prevSnap *common.TsVbuuid, bucket string) {
				r.supvRespch <- &MsgBucketHWT{mType: STREAM_READER_HWT,
					streamId: r.streamId,
					bucket:   bucket,
					ts:       hwt,
					prevSnap: prevSnap}
			}(hwt, prevSnap, bucket)
		}
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

func (r *mutationStreamReader) logReaderStat() {

	r.mutationCount++
	if (r.mutationCount%10000 == 0) || r.mutationCount == 1 {
		logging.Infof("logReaderStat:: %v "+
			"MutationCount %v", r.streamId, r.mutationCount)
	}

}
