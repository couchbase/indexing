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
	"github.com/couchbase/indexing/secondary/platform"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/data"
	"sync"
)

//MutationStreamReader reads a Dataport and stores the incoming mutations
//in mutation queue. This is the only component writing to a mutation queue.
type MutationStreamReader interface {
	Shutdown()
}

type mutationStreamReader struct {
	mutationCount     platform.AlignedUint64
	syncBatchInterval uint64 //batch interval for sync message

	stream   *dataport.Server //handle to the Dataport server
	streamId common.StreamId

	streamMutch chan interface{} //Dataport channel

	supvCmdch  MsgChannel //supervisor sends commands on this channel
	supvRespch MsgChannel //channel to send any message to supervisor

	syncStopCh StopChannel

	bucketQueueMap BucketQueueMap //indexId to mutation queue map

	killch chan bool // kill chan for the main loop

	stats IndexerStatsHolder

	indexerState common.IndexerState
	stateLock    sync.Mutex

	queueMapLock sync.RWMutex
	stopch       StopChannel

	numWorkers int // number of workers to process mutation stream

	streamWorkers []*streamWorker

	config common.Config
}

//CreateMutationStreamReader creates a new mutation stream and starts
//a reader to listen and process the mutations.
//In case returned MutationStreamReader is nil, Message will have the error msg.
func CreateMutationStreamReader(streamId common.StreamId, bucketQueueMap BucketQueueMap,
	bucketFilter map[string]*common.TsVbuuid, supvCmdch MsgChannel, supvRespch MsgChannel,
	numWorkers int, stats *IndexerStats, config common.Config, is common.IndexerState) (MutationStreamReader, Message) {

	//start a new mutation stream
	streamMutch := make(chan interface{}, getMutationBufferSize(config))
	dpconf := config.SectionConfig(
		"dataport.", true /*trim*/)
	stream, err := dataport.NewServer(
		string(StreamAddrMap[streamId]),
		common.SystemConfig["maxVbuckets"].Int(),
		dpconf, streamMutch)
	if err != nil {
		//return stream init error
		logging.Fatalf("MutationStreamReader: Error returned from NewServer."+
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
		syncStopCh:        make(StopChannel),
		bucketQueueMap:    CopyBucketQueueMap(bucketQueueMap),
		killch:            make(chan bool),
		syncBatchInterval: getSyncBatchInterval(config),
		stopch:            make(StopChannel),
		streamWorkers:     make([]*streamWorker, numWorkers),
		numWorkers:        numWorkers,
		config:            config,
	}

	for i := 0; i < numWorkers; i++ {
		r.streamWorkers[i] = newStreamWorker(streamId, numWorkers, i, config, r, bucketFilter)
		go r.streamWorkers[i].start()
	}

	r.stats.Set(stats)

	r.indexerState = is

	//start the main reader loop
	go r.run()
	go r.listenSupvCmd()

	go r.syncWorker()

	return r, &MsgSuccess{}
}

//Shutdown shuts down the mutation stream and all workers.
//This call doesn't return till shutdown is complete.
func (r *mutationStreamReader) Shutdown() {

	logging.Infof("MutationStreamReader:Shutdown StreamReader %v", r.streamId)

	close(r.killch)

	//TODO check if the order of close is important
	for i := 0; i < r.numWorkers; i++ {
		close(r.streamWorkers[i].workerStopCh)
	}

	//stop sync worker
	close(r.syncStopCh)

	close(r.stopch)

	//close the mutation stream
	r.stream.Close()

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
				logging.Fatalf("MutationStreamReader::run Unexpected Mutation "+
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
			r.Shutdown()
			return

		}
	}
}

func (r *mutationStreamReader) handleVbKeyVersions(vbKeyVers []*protobuf.VbKeyVersions) {

	for _, vb := range vbKeyVers {
		r.streamWorkers[int(vb.GetVbucket())%r.numWorkers].workerch <- vb
	}

}

//handleStreamInfoMsg handles the error messages from Dataport
func (r *mutationStreamReader) handleStreamInfoMsg(msg interface{}) {

	var supvMsg Message

	switch msg.(type) {

	case dataport.ConnectionError:
		logging.Infof("MutationStreamReader::handleStreamInfoMsg \n\tReceived ConnectionError "+
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
		logging.Fatalf("MutationStreamReader::handleStreamError \n\tReceived Unknown Message "+
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

		logging.Infof("MutationStreamReader::handleSupervisorCommands %v", cmd)

		close(r.stopch)

		r.queueMapLock.Lock()
		defer r.queueMapLock.Unlock()

		//copy and store new bucketQueueMap
		req := cmd.(*MsgUpdateBucketQueue)
		bucketQueueMap := req.GetBucketQueueMap()
		r.bucketQueueMap = CopyBucketQueueMap(bucketQueueMap)
		r.stats.Set(req.GetStatsObject())

		bucketFilter := req.GetBucketFilter()
		for i := 0; i < r.numWorkers; i++ {
			r.streamWorkers[i].initBucketFilter(bucketFilter)
		}

		r.stopch = make(StopChannel)

		return &MsgSuccess{}

	case INDEXER_PAUSE:
		logging.Infof("MutationStreamReader::handleIndexerPause")
		r.setIndexerState(common.INDEXER_PAUSED)
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

func (r *mutationStreamReader) syncWorker() {

	ticker := time.NewTicker(time.Millisecond * time.Duration(r.syncBatchInterval))

	for {
		select {
		case <-ticker.C:
			r.maybeSendSync()
		case <-r.syncStopCh:
			ticker.Stop()
			return
		}
	}
}

//send a sync message if its due
func (r *mutationStreamReader) maybeSendSync() {

	hwt := make(map[string]*common.TsVbuuid)
	prevSnap := make(map[string]*common.TsVbuuid)
	numVbuckets := r.config["numVbuckets"].Int()

	r.queueMapLock.RLock()
	for bucket, _ := range r.bucketQueueMap {
		hwt[bucket] = common.NewTsVbuuidCached(bucket, numVbuckets)
		prevSnap[bucket] = common.NewTsVbuuidCached(bucket, numVbuckets)
	}

	for bucket, _ := range hwt {
		needSync := false
		for i := 0; i < r.numWorkers; i++ {

			r.streamWorkers[i].lock.Lock()

			if r.streamWorkers[i].bucketSyncDue[bucket] {
				needSync = true
			}

			for vb := 0; vb < numVbuckets; vb++ {
				if vb%r.numWorkers == i {
					hwt[bucket].Seqnos[vb] = r.streamWorkers[i].bucketFilter[bucket].Seqnos[vb]
					hwt[bucket].Vbuuids[vb] = r.streamWorkers[i].bucketFilter[bucket].Vbuuids[vb]
					hwt[bucket].Snapshots[vb] = r.streamWorkers[i].bucketFilter[bucket].Snapshots[vb]
					prevSnap[bucket].Seqnos[vb] = r.streamWorkers[i].bucketPrevSnapMap[bucket].Seqnos[vb]
					prevSnap[bucket].Vbuuids[vb] = r.streamWorkers[i].bucketPrevSnapMap[bucket].Vbuuids[vb]
					prevSnap[bucket].Snapshots[vb] = r.streamWorkers[i].bucketPrevSnapMap[bucket].Snapshots[vb]
				}
			}
			r.streamWorkers[i].bucketSyncDue[bucket] = false
			r.streamWorkers[i].lock.Unlock()
		}
		if needSync {
			r.supvRespch <- &MsgBucketHWT{mType: STREAM_READER_HWT,
				streamId: r.streamId,
				bucket:   bucket,
				ts:       hwt[bucket],
				prevSnap: prevSnap[bucket]}
		}
	}

	r.queueMapLock.RUnlock()

}

func (r *mutationStreamReader) logReaderStat() {

	platform.AddUint64(&r.mutationCount, 1)
	c := platform.LoadUint64(&r.mutationCount)
	if (c%10000 == 0) || c == 1 {
		logging.Infof("logReaderStat:: %v "+
			"MutationCount %v", r.streamId, c)
	}

}

func (r *mutationStreamReader) getIndexerState() common.IndexerState {
	r.stateLock.Lock()
	defer r.stateLock.Unlock()
	return r.indexerState
}

func (r *mutationStreamReader) setIndexerState(is common.IndexerState) {
	r.stateLock.Lock()
	defer r.stateLock.Unlock()
	r.indexerState = is
}

//Stream Worker

type streamWorker struct {
	workerch     chan *protobuf.VbKeyVersions //buffered channel for each worker
	workerStopCh StopChannel                  //stop channels of workers
	bucketFilter map[string]*common.TsVbuuid

	bucketPrevSnapMap map[string]*common.TsVbuuid
	bucketSyncDue     map[string]bool

	lock sync.RWMutex

	//local variables
	skipMutation bool
	evalFilter   bool
	snapType     uint32
	snapStart    uint64
	snapEnd      uint64

	workerId int
	streamId common.StreamId

	reader *mutationStreamReader
}

func newStreamWorker(streamId common.StreamId, numWorkers int, workerId int, config common.Config,
	reader *mutationStreamReader, bucketFilter map[string]*common.TsVbuuid) *streamWorker {

	w := &streamWorker{streamId: streamId,
		workerId:          workerId,
		workerch:          make(chan *protobuf.VbKeyVersions, getWorkerBufferSize(config)/uint64(numWorkers)),
		workerStopCh:      make(StopChannel),
		bucketFilter:      make(map[string]*common.TsVbuuid),
		bucketPrevSnapMap: make(map[string]*common.TsVbuuid),
		bucketSyncDue:     make(map[string]bool),
		reader:            reader,
	}
	w.initBucketFilter(bucketFilter)
	return w

}

func (w *streamWorker) start() {

	defer w.reader.panicHandler()

	for {
		select {

		case vb := <-w.workerch:
			w.handleKeyVersions(vb.GetBucketname(), Vbucket(vb.GetVbucket()),
				Vbuuid(vb.GetVbuuid()), vb.GetKvs())

		case <-w.workerStopCh:
			return

		}

	}

}

func (w *streamWorker) handleKeyVersions(bucket string, vbucket Vbucket, vbuuid Vbuuid,
	kvs []*protobuf.KeyVersions) {

	for _, kv := range kvs {
		w.handleSingleKeyVersion(bucket, vbucket, vbuuid, kv)
	}

}

//handleSingleKeyVersion processes a single mutation based on the command type
//A mutation is put in a worker queue and control message is sent to supervisor
func (w *streamWorker) handleSingleKeyVersion(bucket string, vbucket Vbucket, vbuuid Vbuuid,
	kv *protobuf.KeyVersions) {

	meta := NewMutationMeta()
	meta.bucket = bucket
	meta.vbucket = vbucket
	meta.vbuuid = vbuuid
	meta.seqno = Seqno(kv.GetSeqno())

	defer meta.Free()

	var mutk *MutationKeys
	w.skipMutation = false
	w.evalFilter = true

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
			if w.evalFilter {
				w.evalFilter = false
				//check the bucket filter to see if this mutation can be processed
				//valid mutation will increment seqno of the filter
				if !w.checkAndSetBucketFilter(meta) {
					w.skipMutation = true
				}
			}

			if w.skipMutation {
				continue
			}

			w.reader.logReaderStat()

			if w.reader.getIndexerState() != common.INDEXER_ACTIVE {
				if mutk != nil {
					mutk.Free()
					mutk = nil
				}
				continue
			}

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
				streamId: w.streamId,
				meta:     meta.Clone()}
			w.reader.supvRespch <- msg

		case common.StreamBegin:

			w.updateVbuuidInFilter(meta)

			//send message to supervisor to take decision
			msg := &MsgStream{mType: STREAM_READER_STREAM_BEGIN,
				streamId: w.streamId,
				meta:     meta.Clone()}
			w.reader.supvRespch <- msg

		case common.StreamEnd:
			//send message to supervisor to take decision
			msg := &MsgStream{mType: STREAM_READER_STREAM_END,
				streamId: w.streamId,
				meta:     meta.Clone()}
			w.reader.supvRespch <- msg

		case common.Snapshot:
			//get snapshot information from message
			w.snapType, w.snapStart, w.snapEnd = kv.Snapshot()

			// Snapshot marker can be processed only if
			// they belong to ondisk type or inmemory type.
			if w.snapType&(0x1|0x2) != 0 {
				w.updateSnapInFilter(meta, w.snapStart, w.snapEnd)
			}

		}
	}

	//place secKey in the right worker's queue
	if mutk != nil {
		w.handleSingleMutation(mutk, w.reader.stopch)
	}

}

//handleSingleMutation enqueues mutation in the mutation queue
func (w *streamWorker) handleSingleMutation(mut *MutationKeys, stopch StopChannel) {

	logging.LazyTrace(func() string {
		return fmt.Sprintf("MutationStreamReader::handleSingleMutation received mutation %v", mut)
	})

	w.reader.queueMapLock.RLock()
	defer w.reader.queueMapLock.RUnlock()

	//based on the index, enqueue the mutation in the right queue
	if q, ok := w.reader.bucketQueueMap[mut.meta.bucket]; ok {
		q.queue.Enqueue(mut, mut.meta.vbucket, stopch)

		stats := w.reader.stats.Get()
		if rstats, ok := stats.buckets[mut.meta.bucket]; ok {
			rstats.mutationQueueSize.Add(1)
			rstats.numMutationsQueued.Add(1)
		}

	} else {
		logging.Warnf("MutationStreamReader::handleSingleMutation got mutation for "+
			"unknown bucket. Skipped  %v", mut)
	}

}

//initBucketFilter initializes the bucket filter
func (w *streamWorker) initBucketFilter(bucketFilter map[string]*common.TsVbuuid) {

	w.lock.Lock()
	defer w.lock.Unlock()

	//allocate a new filter for the buckets which don't
	//have a filter yet
	for b, q := range w.reader.bucketQueueMap {
		if _, ok := w.bucketFilter[b]; !ok {
			logging.Debugf("MutationStreamReader::initBucketFilter Added new filter "+
				"for Bucket %v Stream %v", b, w.streamId)

			//if there is non-nil filter, use that. otherwise use a zero filter.
			if filter, ok := bucketFilter[b]; ok && filter != nil {
				w.bucketFilter[b] = filter.Copy()
				w.bucketPrevSnapMap[b] = filter.Copy()
				//reset vbuuids to 0 in filter. mutations for a vbucket are
				//only processed after streambegin is received, which will set
				//the vbuuid again.
				for i := 0; i < len(filter.Vbuuids); i++ {
					w.bucketFilter[b].Vbuuids[i] = 0
				}
			} else {
				w.bucketFilter[b] = common.NewTsVbuuid(b, int(q.queue.GetNumVbuckets()))
				w.bucketPrevSnapMap[b] = common.NewTsVbuuid(b, int(q.queue.GetNumVbuckets()))
			}

			w.bucketSyncDue[b] = false
		}
	}

	//remove the bucket filters for which bucket doesn't exist anymore
	for b, _ := range w.bucketFilter {
		if _, ok := w.reader.bucketQueueMap[b]; !ok {
			logging.Debugf("MutationStreamReader::initBucketFilter Deleted filter "+
				"for Bucket %v Stream %v", b, w.streamId)
			delete(w.bucketFilter, b)
			delete(w.bucketPrevSnapMap, b)
			delete(w.bucketSyncDue, b)
		}
	}

}

//setBucketFilter sets the bucket filter based on seqno/vbuuid of mutation.
//filter is set when stream begin is received.
func (w *streamWorker) setBucketFilter(meta *MutationMeta) {

	w.lock.Lock()
	defer w.lock.Unlock()

	if filter, ok := w.bucketFilter[meta.bucket]; ok {
		filter.Seqnos[meta.vbucket] = uint64(meta.seqno)
		filter.Vbuuids[meta.vbucket] = uint64(meta.vbuuid)
		logging.Tracef("MutationStreamReader::setBucketFilter Vbucket %v "+
			"Seqno %v Bucket %v Stream %v", meta.vbucket, meta.seqno, meta.bucket, w.streamId)
	} else {
		logging.Errorf("MutationStreamReader::setBucketFilter Missing bucket "+
			"%v in Filter for Stream %v", meta.bucket, w.streamId)
	}

}

//checkAndSetBucketFilter checks if mutation can be processed
//based on the current filter. Filter is also updated with new
//seqno/vbuuid if mutations can be processed.
func (w *streamWorker) checkAndSetBucketFilter(meta *MutationMeta) bool {

	w.lock.Lock()
	defer w.lock.Unlock()

	if filter, ok := w.bucketFilter[meta.bucket]; ok {

		if uint64(meta.seqno) < filter.Snapshots[meta.vbucket][0] ||
			uint64(meta.seqno) > filter.Snapshots[meta.vbucket][1] {

			logging.Warnf("MutationStreamReader::checkAndSetBucketFilter Out-of-bound Seqno. "+
				"Snapshot %v-%v for vb %v %v %v. New seqno %v vbuuid %v.  Current Seqno %v vbuuid %v",
				filter.Snapshots[meta.vbucket][0], filter.Snapshots[meta.vbucket][1],
				meta.vbucket, meta.bucket, w.streamId,
				uint64(meta.seqno), uint64(meta.vbuuid),
				filter.Seqnos[meta.vbucket], filter.Vbuuids[meta.vbucket])
		}

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
			w.bucketSyncDue[meta.bucket] = true
			return true
		} else {
			logging.Tracef("MutationStreamReader::checkAndSetBucketFilter Skipped "+
				"Mutation %v for Bucket %v Stream %v. Current Filter %v", meta,
				meta.bucket, w.streamId, filter.Seqnos[meta.vbucket])
			return false
		}

	} else {
		logging.Errorf("MutationStreamReader::checkAndSetBucketFilter Missing"+
			"bucket %v in Filter for Stream %v", meta.bucket, w.streamId)
		return false
	}
}

//updates snapshot information in bucket filter
func (w *streamWorker) updateSnapInFilter(meta *MutationMeta,
	snapStart uint64, snapEnd uint64) {

	w.lock.Lock()
	defer w.lock.Unlock()

	if snapEnd < snapStart {
		logging.Errorf("MutationStreamReader::updateSnapInFilter Bad Snapshot Received "+
			"for %v %v %v %v-%v", meta.bucket, meta.vbucket, w.streamId, snapStart, snapEnd)
	}

	if snapEnd-snapStart > 50000 {
		logging.Errorf("MutationStreamReader::updateSnapInFilter Huge Snapshot Received "+
			"for %v %v %v %v-%v", meta.bucket, meta.vbucket, w.streamId, snapStart, snapEnd)
	}

	if filter, ok := w.bucketFilter[meta.bucket]; ok {
		if snapEnd > filter.Snapshots[meta.vbucket][1] &&
			filter.Vbuuids[meta.vbucket] != 0 {

			//store the existing snap marker in prevSnap map
			prevSnap := w.bucketPrevSnapMap[meta.bucket]
			prevSnap.Snapshots[meta.vbucket][0] = filter.Snapshots[meta.vbucket][0]
			prevSnap.Snapshots[meta.vbucket][1] = filter.Snapshots[meta.vbucket][1]
			prevSnap.Vbuuids[meta.vbucket] = filter.Vbuuids[meta.vbucket]

			filter.Snapshots[meta.vbucket][0] = snapStart
			filter.Snapshots[meta.vbucket][1] = snapEnd

			logging.Debugf("MutationStreamReader::updateSnapInFilter "+
				"bucket %v Stream %v vb %v Snapshot %v-%v Prev Snapshot %v-%v Prev Snapshot vbuuid %v",
				meta.bucket, w.streamId, meta.vbucket, snapStart, snapEnd, prevSnap.Snapshots[meta.vbucket][0],
				prevSnap.Snapshots[meta.vbucket][1], prevSnap.Vbuuids[meta.vbucket])

		} else {
			logging.Warnf("MutationStreamReader::updateSnapInFilter Skipped "+
				"Snapshot %v-%v for vb %v %v %v. Current Filter %v vbuuid %v", snapStart,
				snapEnd, meta.vbucket, meta.bucket, w.streamId,
				filter.Snapshots[meta.vbucket][1], filter.Vbuuids[meta.vbucket])
		}
	} else {
		logging.Errorf("MutationStreamReader::updateSnapInFilter Missing"+
			"bucket %v in Filter for Stream %v", meta.bucket, w.streamId)
	}

}

//updates vbuuid information in bucket filter
func (w *streamWorker) updateVbuuidInFilter(meta *MutationMeta) {

	w.lock.Lock()
	defer w.lock.Unlock()

	if filter, ok := w.bucketFilter[meta.bucket]; ok {
		filter.Vbuuids[meta.vbucket] = uint64(meta.vbuuid)
	} else {
		logging.Errorf("MutationStreamReader::updateVbuuidInFilter Missing"+
			"bucket %v vb %v vbuuid %v in Filter for Stream %v", meta.bucket,
			meta.vbucket, meta.vbuuid, w.streamId)
	}

}

//helper functions

func copyVbList(vbList []uint16) []Vbucket {

	c := make([]Vbucket, len(vbList))

	for i, vb := range vbList {
		c[i] = Vbucket(vb)
	}

	return c
}

func getSyncBatchInterval(config common.Config) uint64 {

	if common.GetStorageMode() == common.MOI {
		return config["stream_reader.moi.syncBatchInterval"].Uint64()
	} else {
		return config["stream_reader.fdb.syncBatchInterval"].Uint64()
	}

}

func getMutationBufferSize(config common.Config) uint64 {

	if common.GetStorageMode() == common.MOI {
		return config["stream_reader.moi.mutationBuffer"].Uint64()
	} else {
		return config["stream_reader.fdb.mutationBuffer"].Uint64()
	}

}

func getWorkerBufferSize(config common.Config) uint64 {

	if common.GetStorageMode() == common.MOI {
		return config["stream_reader.moi.workerBuffer"].Uint64()
	} else {
		return config["stream_reader.fdb.workerBuffer"].Uint64()
	}

}
