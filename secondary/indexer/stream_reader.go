// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

import (
	"bytes"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"sync"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/dataport"
	"github.com/couchbase/indexing/secondary/logging"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/data"
	Stats "github.com/couchbase/indexing/secondary/stats"
)

var transactionMutationPrefix = []byte("_txn:")

//MutationStreamReader reads a Dataport and stores the incoming mutations
//in mutation queue. This is the only component writing to a mutation queue.
type MutationStreamReader interface {
	Shutdown()
}

type mutationStreamReader struct {
	mutationCount     uint64
	syncBatchInterval uint64 //batch interval for sync message

	stream   *dataport.Server //handle to the Dataport server
	streamId common.StreamId

	streamMutch chan interface{} //Dataport channel

	supvCmdch  MsgChannel //supervisor sends commands on this channel
	supvRespch MsgChannel //channel to send any message to supervisor

	syncStopCh StopChannel
	syncSendCh chan bool

	keyspaceIdQueueMap KeyspaceIdQueueMap //keyspaceId to mutation queue map

	keyspaceIdEnableOSO KeyspaceIdEnableOSO

	killch chan bool // kill chan for the main loop

	stats IndexerStatsHolder

	indexerState common.IndexerState
	stateLock    sync.RWMutex

	queueMapLock sync.RWMutex
	stopch       StopChannel

	numWorkers int // number of workers to process mutation stream

	streamWorkers []*streamWorker

	config common.Config
}

//CreateMutationStreamReader creates a new mutation stream and starts
//a reader to listen and process the mutations.
//In case returned MutationStreamReader is nil, Message will have the error msg.
func CreateMutationStreamReader(streamId common.StreamId, keyspaceIdQueueMap KeyspaceIdQueueMap,
	keyspaceIdFilter map[string]*common.TsVbuuid, supvCmdch MsgChannel, supvRespch MsgChannel,
	numWorkers int, stats *IndexerStats, config common.Config, is common.IndexerState,
	allowMarkFirstSnap bool, vbMap *VbMapHolder, keyspaceIdSessionId KeyspaceIdSessionId,
	keyspaceIdEnableOSO KeyspaceIdEnableOSO) (
	MutationStreamReader, Message) {

	//start a new mutation stream
	streamMutch := make(chan interface{}, getMutationBufferSize(config))
	dpconf := config.SectionConfig(
		"dataport.", true /*trim*/)

	dpconf = overrideDataportConf(dpconf)
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
		stream:              stream,
		streamMutch:         streamMutch,
		supvCmdch:           supvCmdch,
		supvRespch:          supvRespch,
		syncStopCh:          make(StopChannel),
		syncSendCh:          make(chan bool),
		keyspaceIdQueueMap:  CopyKeyspaceIdQueueMap(keyspaceIdQueueMap),
		keyspaceIdEnableOSO: make(KeyspaceIdEnableOSO),
		killch:              make(chan bool),
		syncBatchInterval:   getSyncBatchInterval(config),
		stopch:              make(StopChannel),
		streamWorkers:       make([]*streamWorker, numWorkers),
		numWorkers:          numWorkers,
		config:              config,
	}

	for ks, enable := range keyspaceIdEnableOSO {
		r.keyspaceIdEnableOSO[ks] = enable
	}

	r.stats.Set(stats)

	logging.Infof("MutationStreamReader: Setting Stream Workers %v %v", r.streamId, numWorkers)

	for i := 0; i < numWorkers; i++ {
		r.streamWorkers[i] = newStreamWorker(streamId, numWorkers, i, config, r,
			keyspaceIdFilter, allowMarkFirstSnap, vbMap, keyspaceIdSessionId, keyspaceIdEnableOSO)
		go r.streamWorkers[i].start()
	}

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

	for i := 0; i < r.numWorkers; i++ {
		close(r.streamWorkers[i].workerStopCh)
	}

	//stop sync worker
	close(r.syncStopCh)

	select {
	case r.syncSendCh <- true:
	default:
	}

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

					// Invoke syncWorker as there are mutations
					select {
					case r.syncSendCh <- true:
					default:
					}

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

		//send a separate message for each keyspaceId. If the ConnError is with empty vblist,
		//the message is ignored.
		connErr := msg.(dataport.ConnectionError)
		if len(connErr) != 0 {
			for keyspaceId, vbList := range connErr {
				supvMsg = &MsgStreamInfo{mType: STREAM_READER_CONN_ERROR,
					streamId:   r.streamId,
					keyspaceId: keyspaceId,
					vbList:     copyVbList(vbList),
				}
				r.supvRespch <- supvMsg
			}
		} else {
			supvMsg = &MsgStreamInfo{mType: STREAM_READER_CONN_ERROR,
				streamId:   r.streamId,
				keyspaceId: "",
				vbList:     []Vbucket(nil),
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

		//copy and store new keyspaceIdQueueMap
		req := cmd.(*MsgUpdateKeyspaceIdQueue)
		keyspaceIdQueueMap := req.GetKeyspaceIdQueueMap()
		r.keyspaceIdQueueMap = CopyKeyspaceIdQueueMap(keyspaceIdQueueMap)
		r.stats.Set(req.GetStatsObject())

		keyspaceIdFilter := req.GetKeyspaceIdFilter()
		keyspaceIdSessionId := req.GetKeyspaceIdSessionId()
		keyspaceIdEnableOSO := req.GetKeyspaceIdEnableOSO()

		for ks, enable := range keyspaceIdEnableOSO {
			r.keyspaceIdEnableOSO[ks] = enable
		}

		for i := 0; i < r.numWorkers; i++ {
			r.streamWorkers[i].initKeyspaceIdFilter(keyspaceIdFilter,
				keyspaceIdSessionId, keyspaceIdEnableOSO)
		}

		r.stopch = make(StopChannel)

		return &MsgSuccess{}

	case INDEXER_PAUSE:
		logging.Infof("MutationStreamReader::handleIndexerPause")
		r.setIndexerState(common.INDEXER_PAUSED)
		return &MsgSuccess{}

	case INDEXER_SECURITY_CHANGE:
		logging.Infof("MutationStreamReader::handleSecurityChange")
		if err := r.stream.ResetConnections(); err != nil {
			idxErr := Error{
				code:     ERROR_INDEXER_INTERNAL_ERROR,
				severity: FATAL,
				cause:    err,
				category: INDEXER,
			}
			return &MsgError{err: idxErr}
		}
		return &MsgSuccess{}

	case UPDATE_KEYSPACE_STATS_MAP:
		logging.Infof("MutationStreamReader::handleUpdateKeyspaceStatsMap")
		req := cmd.(*MsgUpdateKeyspaceStatsMap)
		stats := r.stats.Get()
		if stats != nil {
			stats.keyspaceStatsMap.Set(req.GetStatsObject())
		}
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

	fastpath := true
	inactivityTick := 0

	// If there are no mutations, then this go-routine will not be invoked
	// After the last mutataion, this method is invoked every "syncBatchInterval"
	// milliseconds until fastpath becomes false.
	for {
		select {
		case <-r.syncSendCh:
			ticker := time.NewTicker(time.Duration(r.syncBatchInterval) * time.Millisecond)
			for {
				select {
				case <-ticker.C:
					if r.maybeSendSync(fastpath) {
						inactivityTick = 0
						fastpath = true
					} else {
						if inactivityTick > int(r.syncBatchInterval)*r.numWorkers {
							fastpath = false
							break
						} else {
							inactivityTick++
						}
					}
				case <-r.syncStopCh:
					ticker.Stop()
					return
				}

				if !fastpath {
					ticker.Stop()
					break // break the inner for loop and want for new mutations
				}
			}
		case <-r.syncStopCh:
			return
		}
	}
}

//send a sync message if its due
func (r *mutationStreamReader) maybeSendSync(fastpath bool) bool {

	if !fastpath {
		if !r.checkAnySyncDue() {
			return false
		}
	}

	hwt := make(map[string]*common.TsVbuuid)
	prevSnap := make(map[string]*common.TsVbuuid)
	numVbuckets := r.config["numVbuckets"].Int()

	var hwtOSOMap map[string]*common.TsVbuuid
	var hwtOSO *common.TsVbuuid

	sent := false

	r.queueMapLock.RLock()
	for keyspaceId := range r.keyspaceIdQueueMap {
		//actual TS uses bucket as keyspaceId
		hwt[keyspaceId] = common.NewTsVbuuidCached(GetBucketFromKeyspaceId(keyspaceId), numVbuckets)
		prevSnap[keyspaceId] = common.NewTsVbuuidCached(GetBucketFromKeyspaceId(keyspaceId), numVbuckets)
		if r.keyspaceIdEnableOSO[keyspaceId] {
			if hwtOSOMap == nil {
				hwtOSOMap = make(map[string]*common.TsVbuuid)
			}
			hwtOSOMap[keyspaceId] = common.NewTsVbuuidCached(GetBucketFromKeyspaceId(keyspaceId), numVbuckets)
		}
	}

	nWrkr := r.numWorkers
	for keyspaceId := range hwt {
		needSync := false
		hasOSO := false
		enableOSO := false
		sessionId := uint64(0)
		for i := 0; i < nWrkr; i++ {

			r.streamWorkers[i].lock.Lock()

			if r.streamWorkers[i].keyspaceIdSyncDue[keyspaceId] {
				needSync = true
			}
			if r.streamWorkers[i].keyspaceIdEnableOSO[keyspaceId] {
				enableOSO = true
			}
			//all workers have same sessionId, it is ok to overwrite
			sessionId = r.streamWorkers[i].keyspaceIdSessionId[keyspaceId]

			loopCnt := 0
			vb := 0
			for {
				vb = loopCnt*nWrkr + i
				if vb < numVbuckets {

					filter := r.streamWorkers[i].keyspaceIdFilter[keyspaceId]
					filterOSO := r.streamWorkers[i].keyspaceIdFilterOSO[keyspaceId]

					//if OSO snapshot
					if enableOSO &&
						filterOSO.Snapshots[vb][0] == 1 {
						hwtOSOMap[keyspaceId].Seqnos[vb] = filterOSO.Seqnos[vb]
						hwtOSOMap[keyspaceId].Vbuuids[vb] = filterOSO.Vbuuids[vb]
						hwtOSOMap[keyspaceId].Snapshots[vb][0] = filterOSO.Snapshots[vb][0]
						hwtOSOMap[keyspaceId].Snapshots[vb][1] = filterOSO.Snapshots[vb][1]
						hasOSO = true
					}

					//regular snapshot
					hwt[keyspaceId].Seqnos[vb] = filter.Seqnos[vb]
					hwt[keyspaceId].Vbuuids[vb] = filter.Vbuuids[vb]
					hwt[keyspaceId].Snapshots[vb] = filter.Snapshots[vb]

					prevSnapMap := r.streamWorkers[i].keyspaceIdPrevSnapMap[keyspaceId]
					prevSnap[keyspaceId].Seqnos[vb] = prevSnapMap.Seqnos[vb]
					prevSnap[keyspaceId].Vbuuids[vb] = prevSnapMap.Vbuuids[vb]
					prevSnap[keyspaceId].Snapshots[vb][0] = prevSnapMap.Snapshots[vb][0]
					prevSnap[keyspaceId].Snapshots[vb][1] = prevSnapMap.Snapshots[vb][1]

					if !hwt[keyspaceId].DisableAlign {
						hwt[keyspaceId].DisableAlign = filter.DisableAlign
					}
				} else {
					break
				}
				loopCnt++
			}
			r.streamWorkers[i].keyspaceIdSyncDue[keyspaceId] = false
			r.streamWorkers[i].keyspaceIdFilter[keyspaceId].DisableAlign = false
			r.streamWorkers[i].lock.Unlock()
		}
		if hasOSO && hwtOSOMap != nil {
			hwtOSO = hwtOSOMap[keyspaceId]
		}
		if needSync {
			r.supvRespch <- &MsgKeyspaceHWT{mType: STREAM_READER_HWT,
				streamId:   r.streamId,
				keyspaceId: keyspaceId,
				hwt:        hwt[keyspaceId],
				hwtOSO:     hwtOSO,
				prevSnap:   prevSnap[keyspaceId],
				sessionId:  sessionId,
			}
			sent = true
		}
	}

	r.queueMapLock.RUnlock()
	return sent

}

//check if any worker has sync due
func (r *mutationStreamReader) checkAnySyncDue() bool {

	syncDue := false
	r.queueMapLock.RLock()
loop:
	for keyspaceId := range r.keyspaceIdQueueMap {
		for i := 0; i < r.numWorkers; i++ {
			r.streamWorkers[i].lock.Lock()
			if r.streamWorkers[i].keyspaceIdSyncDue[keyspaceId] {
				syncDue = true
			}
			r.streamWorkers[i].lock.Unlock()
			if syncDue {
				break loop
			}
		}
	}
	r.queueMapLock.RUnlock()
	return syncDue
}

func (r *mutationStreamReader) logReaderStat() {

	atomic.AddUint64(&r.mutationCount, 1)
	c := atomic.LoadUint64(&r.mutationCount)
	if (c%10000 == 0) || c == 1 {
		logging.Debugf("logReaderStat:: %v "+
			"MutationCount %v", r.streamId, c)
	}

}

func (r *mutationStreamReader) getIndexerState() common.IndexerState {
	r.stateLock.RLock()
	defer r.stateLock.RUnlock()
	return r.indexerState
}

func (r *mutationStreamReader) setIndexerState(is common.IndexerState) {
	r.stateLock.Lock()
	defer r.stateLock.Unlock()
	r.indexerState = is
}

//Stream Worker
type firstSnapFlag []bool

type streamWorker struct {
	workerch            chan *protobuf.VbKeyVersions //buffered channel for each worker
	workerStopCh        StopChannel                  //stop channels of workers
	keyspaceIdFilter    map[string]*common.TsVbuuid
	keyspaceIdFilterOSO map[string]*common.TsVbuuid

	keyspaceIdSessionId    KeyspaceIdSessionId
	keyspaceIdEnableOSO    KeyspaceIdEnableOSO
	keyspaceIdOSOException map[string]bool

	keyspaceIdPrevSnapMap map[string]*common.TsVbuuid
	keyspaceIdSyncDue     map[string]bool

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

	meta *MutationMeta

	markFirstSnap       bool
	keyspaceIdFirstSnap map[string]firstSnapFlag

	vbMap *VbMapHolder
}

func newStreamWorker(streamId common.StreamId, numWorkers int, workerId int, config common.Config,
	reader *mutationStreamReader, keyspaceIdFilter map[string]*common.TsVbuuid, allowMarkFirstSnap bool,
	vbMap *VbMapHolder, keyspaceIdSessionId KeyspaceIdSessionId, keyspaceIdEnableOSO KeyspaceIdEnableOSO) *streamWorker {

	w := &streamWorker{streamId: streamId,
		workerId:               workerId,
		workerch:               make(chan *protobuf.VbKeyVersions, getWorkerBufferSize(config)/uint64(numWorkers)),
		workerStopCh:           make(StopChannel),
		keyspaceIdFilter:       make(map[string]*common.TsVbuuid),
		keyspaceIdFilterOSO:    make(map[string]*common.TsVbuuid),
		keyspaceIdPrevSnapMap:  make(map[string]*common.TsVbuuid),
		keyspaceIdSyncDue:      make(map[string]bool),
		reader:                 reader,
		keyspaceIdFirstSnap:    make(map[string]firstSnapFlag),
		vbMap:                  vbMap,
		keyspaceIdSessionId:    make(KeyspaceIdSessionId),
		keyspaceIdEnableOSO:    make(KeyspaceIdEnableOSO),
		keyspaceIdOSOException: make(map[string]bool),
	}

	w.meta = &MutationMeta{}

	if allowMarkFirstSnap {
		w.markFirstSnap = getMarkFirstSnap(config)
	}

	w.initKeyspaceIdFilter(keyspaceIdFilter, keyspaceIdSessionId, keyspaceIdEnableOSO)
	return w

}

func (w *streamWorker) start() {

	defer w.reader.panicHandler()

	for {
		select {

		case vb := <-w.workerch:
			w.handleKeyVersions(vb.GetKeyspaceId(), Vbucket(vb.GetVbucket()),
				Vbuuid(vb.GetVbuuid()), vb.GetOpaque2(), vb.GetKvs(),
				common.ProjectorVersion(vb.GetProjVer()))

		case <-w.workerStopCh:
			return

		}

	}

}

func (w *streamWorker) handleKeyVersions(keyspaceId string, vbucket Vbucket, vbuuid Vbuuid,
	opaque uint64, kvs []*protobuf.KeyVersions, projVer common.ProjectorVersion) {

	for _, kv := range kvs {
		w.handleSingleKeyVersion(keyspaceId, vbucket, vbuuid, opaque, kv, projVer)

		if kv.GetPrjMovingAvg() > 0 {
			avg := w.getLatencyObj(keyspaceId, vbucket)
			if avg != nil {
				avg.Set(kv.GetPrjMovingAvg())
			}
		}
	}

}

//handleSingleKeyVersion processes a single mutation based on the command type
//A mutation is put in a worker queue and control message is sent to supervisor
func (w *streamWorker) handleSingleKeyVersion(keyspaceId string, vbucket Vbucket, vbuuid Vbuuid,
	opaque uint64, kv *protobuf.KeyVersions, projVer common.ProjectorVersion) {

	meta := w.meta

	meta.keyspaceId = keyspaceId
	meta.vbucket = vbucket
	meta.vbuuid = vbuuid
	meta.seqno = kv.GetSeqno()
	meta.projVer = projVer
	meta.opaque = opaque

	defer meta.Reset()

	var mutk *MutationKeys
	w.skipMutation = false
	w.evalFilter = true

	logging.LazyTrace(func() string {
		return fmt.Sprintf("MutationStreamReader::handleSingleKeyVersion received KeyVersions %v", logging.TagUD(kv))
	})

	allocateFillerMutation := func(meta *MutationMeta, docid []byte) *MutationKeys {
		mutk = NewMutationKeys()
		mutk.meta = meta.Clone()
		mutk.docid = docid
		mutk.mut = mutk.mut[:0]

		mut := NewMutation()
		mut.command = common.Filler
		mutk.mut = append(mutk.mut, mut)
		return mutk
	}

	state := w.reader.getIndexerState()

	for i, cmd := range kv.GetCommands() {

		//based on the type of command take appropriate action
		switch byte(cmd) {

		//case protobuf.Command_Upsert, protobuf.Command_Deletion, protobuf.Command_UpsertDeletion:
		case common.Upsert, common.Deletion, common.UpsertDeletion:

			//As there can multiple keys in a KeyVersion for a mutation,
			//filter needs to be evaluated and set only once.
			if w.evalFilter {
				w.evalFilter = false
				//check the keyspaceId filter to see if this mutation can be processed
				//valid mutation will increment seqno of the filter
				w.skipMutation, meta.firstSnap = w.checkAndSetKeyspaceIdFilter(meta)
			}

			if w.skipMutation {
				continue
			}

			w.reader.logReaderStat()

			if state != common.INDEXER_ACTIVE {
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
			mut.key = append(mut.key, kv.GetKeys()[i]...)
			mut.command = byte(kv.GetCommands()[i])

			// For backward compatibilty, projector may not send partnkey pre-5.1.
			if len(kv.GetPartnkeys()) != 0 && len(kv.GetPartnkeys()[i]) != 0 {
				mut.partnkey = append(mut.partnkey, kv.GetPartnkeys()[i]...)
			}

			mutk.mut = append(mutk.mut, mut)

		case common.DropData:
			//send message to supervisor to take decision
			msg := &MsgStream{mType: STREAM_READER_STREAM_DROP_DATA,
				streamId: w.streamId,
				meta:     meta.Clone()}
			w.reader.supvRespch <- msg

		case common.StreamBegin:

			status := common.STREAM_SUCCESS
			code := byte(0)

			len := len(kv.GetKeys()[i])
			if len >= 1 {
				status = common.StreamStatus(kv.GetKeys()[i][0])
			}
			if len >= 2 {
				code = kv.GetKeys()[i][1]
			}

			if status == common.STREAM_SUCCESS {
				w.updateVbuuidInFilter(meta)
			}

			//send message to supervisor to take decision
			msg := &MsgStream{
				mType:    STREAM_READER_STREAM_BEGIN,
				streamId: w.streamId,
				node:     kv.GetDocid(), // For projector versions prior to 6.5, docid would be "nil"
				meta:     meta.Clone(),
				status:   status,
				errCode:  code,
			}
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

		case common.UpdateSeqno, common.SeqnoAdvanced:

			//TODO Collections remove these logs after integration testing
			if byte(cmd) == common.UpdateSeqno && w.streamId == common.INIT_STREAM && !bytes.HasPrefix(kv.GetDocid(), transactionMutationPrefix) {

				logging.Fatalf("MutationStreamReader::handleSingleKeyVersion %v %v Unexpected UpdateSeqno "+
					"%v %v", w.streamId, keyspaceId, meta, kv.GetDocid())
			}

			/* Disable the log message till further investigation (MB-39494)
			if byte(cmd) == common.SeqnoAdvanced && w.streamId == common.MAINT_STREAM {

				logging.Fatalf("MutationStreamReader::handleSingleKeyVersion %v %v Unexpected SeqnoAdvanced "+
					"%v %v", w.streamId, keyspaceId, meta, kv.GetDocid())
			}
			*/

			skipMutation, _ := w.checkAndSetKeyspaceIdFilter(meta)

			//allocate an empty mutation for flusher
			//TODO Collections can be done only for snapshot boundary
			if !skipMutation {
				mutk = allocateFillerMutation(meta, kv.GetDocid())
			}

		case common.CollectionCreate, common.CollectionDrop,
			common.CollectionFlush, common.CollectionChanged,
			common.ScopeCreate, common.ScopeDrop:

			manifestuid := string(kv.GetKeys()[i])
			scopeId := string(kv.GetOldkeys()[i])
			collectionId := string(kv.GetPartnkeys()[i])

			w.processDcpSystemEvent(meta, byte(cmd),
				manifestuid, scopeId, collectionId)

			skipMutation, _ := w.checkAndSetKeyspaceIdFilter(meta)

			//allocate a filler mutation for flusher
			//TODO can be done only for snapshot boundary
			if !skipMutation {
				mutk = allocateFillerMutation(meta, kv.GetDocid())
			}

		case common.OSOSnapshotStart, common.OSOSnapshotEnd:
			w.updateOSOMarkerInFilter(meta, byte(cmd))
			w.processDcpOSOMarker(meta, byte(cmd))
		}
	}

	//place secKey in the right worker's queue
	if mutk != nil {
		mutk.size = mutk.Size()
		w.handleSingleMutation(mutk, w.reader.stopch)
	}

}

//handleSingleMutation enqueues mutation in the mutation queue
func (w *streamWorker) handleSingleMutation(mut *MutationKeys, stopch StopChannel) {

	logging.LazyTrace(func() string {
		return fmt.Sprintf("MutationStreamReader::handleSingleMutation received mutation %v", logging.TagUD(mut))
	})

	w.reader.queueMapLock.RLock()
	defer w.reader.queueMapLock.RUnlock()

	//based on the index, enqueue the mutation in the right queue
	if q, ok := w.reader.keyspaceIdQueueMap[mut.meta.keyspaceId]; ok {
		q.queue.Enqueue(mut, mut.meta.vbucket, stopch)

		keyspaceStats := w.reader.stats.GetKeyspaceStats(w.streamId, mut.meta.keyspaceId)
		if keyspaceStats != nil {
			keyspaceStats.mutationQueueSize.Add(1)
			keyspaceStats.numMutationsQueued.Add(1)
		}

	} else {
		logging.Warnf("MutationStreamReader::handleSingleMutation got mutation for "+
			"unknown keyspaceId:. Skipped  %v", logging.TagUD(mut))
	}
}

//initKeyspaceIdFilter initializes the keyspaceId filter
func (w *streamWorker) initKeyspaceIdFilter(keyspaceIdFilter map[string]*common.TsVbuuid,
	keyspaceIdSessionId KeyspaceIdSessionId, keyspaceIdEnableOSO KeyspaceIdEnableOSO) {

	w.lock.Lock()
	defer w.lock.Unlock()

	//allocate a new filter for the keyspaceIds which don't
	//have a filter yet
	for b, q := range w.reader.keyspaceIdQueueMap {
		if _, ok := w.keyspaceIdFilter[b]; !ok {
			logging.Debugf("MutationStreamReader::initKeyspaceIdFilter Added new filter "+
				"for KeyspaceId %v Stream %v", b, w.streamId)

			//if there is non-nil filter, use that. otherwise use a zero filter.
			if filter, ok := keyspaceIdFilter[b]; ok && filter != nil {
				w.keyspaceIdFilter[b] = filter.Copy()
				w.keyspaceIdPrevSnapMap[b] = filter.Copy()
				//reset vbuuids to 0 in filter. mutations for a vbucket are
				//only processed after streambegin is received, which will set
				//the vbuuid again.
				for i := 0; i < len(filter.Vbuuids); i++ {
					w.keyspaceIdFilter[b].Vbuuids[i] = 0
				}
			} else {
				//actual TS uses bucket as keyspaceId
				w.keyspaceIdFilter[b] = common.NewTsVbuuid(GetBucketFromKeyspaceId(b), int(q.queue.GetNumVbuckets()))
				w.keyspaceIdPrevSnapMap[b] = common.NewTsVbuuid(GetBucketFromKeyspaceId(b), int(q.queue.GetNumVbuckets()))
			}

			w.keyspaceIdSyncDue[b] = false
			w.keyspaceIdFirstSnap[b] = make(firstSnapFlag, int(q.queue.GetNumVbuckets()))
			w.keyspaceIdSessionId[b] = keyspaceIdSessionId[b]
			w.keyspaceIdEnableOSO[b] = keyspaceIdEnableOSO[b]
			if keyspaceIdEnableOSO[b] {
				w.keyspaceIdFilterOSO[b] = common.NewTsVbuuid(GetBucketFromKeyspaceId(b), int(q.queue.GetNumVbuckets()))
			}
			w.keyspaceIdOSOException[b] = false

			//reset stat for bucket
			keyspaceStats := w.reader.stats.GetKeyspaceStats(w.streamId, b)
			if keyspaceStats != nil {
				keyspaceStats.mutationQueueSize.Set(0)
			}
		}
	}

	//remove the keyspaceId filters for which keyspace doesn't exist anymore
	for b := range w.keyspaceIdFilter {
		if _, ok := w.reader.keyspaceIdQueueMap[b]; !ok {
			logging.Debugf("MutationStreamReader::initKeyspaceIdFilter Deleted filter "+
				"for KeyspaceId %v Stream %v", b, w.streamId)
			delete(w.keyspaceIdFilter, b)
			delete(w.keyspaceIdPrevSnapMap, b)
			delete(w.keyspaceIdSyncDue, b)
			delete(w.keyspaceIdFirstSnap, b)
			delete(w.keyspaceIdSessionId, b)
			delete(w.keyspaceIdFilterOSO, b)
			delete(w.keyspaceIdEnableOSO, b)
			delete(w.keyspaceIdOSOException, b)
		}
	}

}

//setKeyspaceIdFilter sets the keyspaceId filter based on seqno/vbuuid of mutation.
//filter is set when stream begin is received.
func (w *streamWorker) setKeyspaceIdFilter(meta *MutationMeta) {

	w.lock.Lock()
	defer w.lock.Unlock()

	if filter, ok := w.keyspaceIdFilter[meta.keyspaceId]; ok {
		filter.Seqnos[meta.vbucket] = meta.seqno
		filter.Vbuuids[meta.vbucket] = uint64(meta.vbuuid)
		logging.Tracef("MutationStreamReader::setKeyspaceIdFilter Vbucket %v "+
			"Seqno %v KeyspaceId %v Stream %v", meta.vbucket, meta.seqno, meta.keyspaceId, w.streamId)
	} else {
		logging.Errorf("MutationStreamReader::setKeyspaceIdFilter Missing keyspaceId "+
			"%v in Filter for Stream %v", meta.keyspaceId, w.streamId)
	}

}

//checkAndSetKeyspaceIdFilter checks if mutation can be processed
//based on the current filter. Filter is also updated with new
//seqno/vbuuid if mutations can be processed.
//Returns {skip, firstSnap} to indicate if mutations needs to be "skipped" and
//if it belongs for first DCP snapshot.
func (w *streamWorker) checkAndSetKeyspaceIdFilter(meta *MutationMeta) (bool, bool) {

	w.lock.Lock()
	defer w.lock.Unlock()

	if w.keyspaceIdOSOException[meta.keyspaceId] {
		return true /* skip */, false /* firstSnap */
	}
	if enable, ok := w.keyspaceIdEnableOSO[meta.keyspaceId]; ok && enable {
		filterOSO, _ := w.keyspaceIdFilterOSO[meta.keyspaceId]
		//if OSO snapshot is being processed
		if filterOSO.Snapshots[meta.vbucket][0] == 1 &&
			filterOSO.Snapshots[meta.vbucket][1] == 0 {
			return w.checkAndSetKeyspaceIdFilterOSO(meta)
		}
	}
	return w.checkAndSetKeyspaceIdFilterDefault(meta)
}

func (w *streamWorker) checkAndSetKeyspaceIdFilterDefault(meta *MutationMeta) (bool, bool) {

	if filter, ok := w.keyspaceIdFilter[meta.keyspaceId]; ok {

		//validate sessionId. allow opaque==0 for backward compat
		if meta.opaque != 0 &&
			meta.opaque != w.keyspaceIdSessionId[meta.keyspaceId] {
			//skip the mutation
			return true, false
		}

		if meta.seqno < filter.Snapshots[meta.vbucket][0] ||
			meta.seqno > filter.Snapshots[meta.vbucket][1] {

			logging.Debugf("MutationStreamReader::checkAndSetKeyspaceIdFilter Out-of-bound Seqno. "+
				"Snapshot %v-%v for vb %v %v %v. New seqno %v vbuuid %v.  Current Seqno %v vbuuid %v",
				filter.Snapshots[meta.vbucket][0], filter.Snapshots[meta.vbucket][1],
				meta.vbucket, meta.keyspaceId, w.streamId,
				meta.seqno, uint64(meta.vbuuid),
				filter.Seqnos[meta.vbucket], filter.Vbuuids[meta.vbucket])

			w.keyspaceIdFirstSnap[meta.keyspaceId][meta.vbucket] = false //for safety
		}

		//the filter only checks if seqno of incoming mutation is greater than
		//the existing filter. Also there should be a valid StreamBegin(vbuuid)
		//for the vbucket. The vbuuid check is only to ensure that after stream
		//restart for a keyspace, mutations get processed only after StreamBegin.
		//There can be residual mutations in projector endpoint queue after
		//a keyspace gets deleted from stream in case of multiple keyspaceIds.
		//The vbuuid doesn't get reset after StreamEnd/StreamBegin. The
		//filter can be extended for that check if required.
		if meta.seqno > filter.Seqnos[meta.vbucket] &&
			filter.Vbuuids[meta.vbucket] != 0 {
			filter.Seqnos[meta.vbucket] = meta.seqno
			filter.Vbuuids[meta.vbucket] = uint64(meta.vbuuid)
			w.keyspaceIdSyncDue[meta.keyspaceId] = true
			return false, w.keyspaceIdFirstSnap[meta.keyspaceId][meta.vbucket]
		} else {
			logging.Tracef("MutationStreamReader::checkAndSetKeyspaceIdFilter Skipped "+
				"Mutation %v for KeyspaceId %v Stream %v. Current Filter %v", meta,
				meta.keyspaceId, w.streamId, filter.Seqnos[meta.vbucket])
			return true, false
		}

	} else {
		logging.Debugf("MutationStreamReader::checkAndSetKeyspaceIdFilter Missing"+
			"keyspaceId %v in Filter for Stream %v", meta.keyspaceId, w.streamId)
		return true, false
	}
}

func (w *streamWorker) checkAndSetKeyspaceIdFilterOSO(meta *MutationMeta) (bool, bool) {

	if filter, ok := w.keyspaceIdFilter[meta.keyspaceId]; ok {

		filterOSO, _ := w.keyspaceIdFilterOSO[meta.keyspaceId]

		//validate sessionId. allow opaque==0 for backward compat
		if meta.opaque != 0 &&
			meta.opaque != w.keyspaceIdSessionId[meta.keyspaceId] {
			//skip the mutation
			return true, false
		}

		if filter.Vbuuids[meta.vbucket] == 0 {
			logging.Warnf("MutationStreamReader::checkAndSetKeyspaceIdFilterOSO Skipped "+
				"Mutation %v for KeyspaceId %v Stream %v. Vbuuid %v", meta,
				meta.keyspaceId, w.streamId, filter.Vbuuids[meta.vbucket])
			return true, false
		}

		//Vbuuid is used to store count of mutations in OSO filter
		filterOSO.Vbuuids[meta.vbucket]++

		//Seqnos is used to store the highest seqno in OSO filter
		if meta.seqno > filterOSO.Seqnos[meta.vbucket] {
			filterOSO.Seqnos[meta.vbucket] = meta.seqno
		}

		filter.Vbuuids[meta.vbucket] = uint64(meta.vbuuid)

		w.keyspaceIdSyncDue[meta.keyspaceId] = true
		return false, w.keyspaceIdFirstSnap[meta.keyspaceId][meta.vbucket]

	} else {
		logging.Debugf("MutationStreamReader::checkAndSetKeyspaceIdFilterOSO Missing"+
			"keyspaceId %v in Filter for Stream %v", meta.keyspaceId, w.streamId)
		return true, false
	}
}

func (w *streamWorker) updateOSOMarkerInFilter(meta *MutationMeta, eventType byte) {

	w.lock.Lock()
	defer w.lock.Unlock()

	if filter, ok := w.keyspaceIdFilter[meta.keyspaceId]; ok {

		filterOSO := w.keyspaceIdFilterOSO[meta.keyspaceId]

		//validate sessionId. allow opaque==0 for backward compat
		if meta.opaque != 0 &&
			meta.opaque != w.keyspaceIdSessionId[meta.keyspaceId] {
			return
		}

		resetStream := func() {
			logging.Infof("MutationStreamReader::updateOSOMarkerInFilter %v %v."+
				" Resetting Stream.", w.streamId, meta.keyspaceId)
			w.keyspaceIdOSOException[meta.keyspaceId] = true
			w.reader.supvRespch <- &MsgStreamUpdate{
				mType:      RESET_STREAM,
				streamId:   w.streamId,
				keyspaceId: meta.keyspaceId,
				sessionId:  w.keyspaceIdSessionId[meta.keyspaceId],
			}
		}

		enableOSO := w.keyspaceIdEnableOSO[meta.keyspaceId]
		if !enableOSO {
			logging.Errorf("MutationStreamReader::updateOSOMarkerInFilter %v %v "+
				"Received OSO Marker for Vbucket %v. OSO is disabled.", w.streamId,
				meta.keyspaceId, meta.vbucket)
			resetStream()
			return
		}

		if eventType == common.OSOSnapshotStart {
			//if filter already has a snapshot
			if filter.Snapshots[meta.vbucket][1] != 0 {
				logging.Infof("MutationStreamReader::updateOSOMarkerInFilter %v %v "+
					"Received OSO Start For Vbucket %v after DCP snapshot %v-%v Seqno %v", w.streamId,
					meta.keyspaceId, meta.vbucket, filter.Snapshots[meta.vbucket][0],
					filter.Snapshots[meta.vbucket][1], filter.Seqnos[meta.vbucket])
				resetStream()
				return
			}

			logging.Infof("MutationStreamReader::updateOSOMarkerInFilter %v %v "+
				"Received OSO Start For Vbucket %v. Seqno %v. "+
				"Count %v. OSO Start %v. OSO End %v.", w.streamId,
				meta.keyspaceId, meta.vbucket, filterOSO.Seqnos[meta.vbucket],
				filterOSO.Vbuuids[meta.vbucket], filterOSO.Snapshots[meta.vbucket][0],
				filterOSO.Snapshots[meta.vbucket][1])

			if filterOSO.Snapshots[meta.vbucket][0] == 1 &&
				filterOSO.Snapshots[meta.vbucket][1] == 0 {
				logging.Errorf("MutationStreamReader::updateOSOMarkerInFilter %v %v "+
					"Received OSO Start For Vbucket %v without previous OSO End. Seqno %v. "+
					"Count %v. OSO Start %v. OSO End %v.", w.streamId,
					meta.keyspaceId, meta.vbucket, filterOSO.Seqnos[meta.vbucket],
					filterOSO.Vbuuids[meta.vbucket], filterOSO.Snapshots[meta.vbucket][0],
					filterOSO.Snapshots[meta.vbucket][1])
				resetStream()
				return

			} else {
				filterOSO.Snapshots[meta.vbucket][0] = 1 //snapshot[0] stores OSO Start
				filterOSO.Snapshots[meta.vbucket][1] = 0 //snapshot[1] stores OSO End
			}

			if w.markFirstSnap &&
				filter.Snapshots[meta.vbucket][1] == 0 &&
				filter.Seqnos[meta.vbucket] == 0 &&
				filterOSO.Seqnos[meta.vbucket] == 0 {
				w.keyspaceIdFirstSnap[meta.keyspaceId][meta.vbucket] = true
			} else {
				w.keyspaceIdFirstSnap[meta.keyspaceId][meta.vbucket] = false
			}
		} else if eventType == common.OSOSnapshotEnd {
			logging.Infof("MutationStreamReader::updateOSOMarkerInFilter %v %v "+
				"Received OSO End For Vbucket %v. Seqno %v. "+
				"Count %v. OSO Start %v. OSO End %v.", w.streamId,
				meta.keyspaceId, meta.vbucket, filterOSO.Seqnos[meta.vbucket],
				filterOSO.Vbuuids[meta.vbucket], filterOSO.Snapshots[meta.vbucket][0],
				filterOSO.Snapshots[meta.vbucket][1])
			filterOSO.Snapshots[meta.vbucket][1] = 1 //snapshot[1] stores OSO End
			w.keyspaceIdFirstSnap[meta.keyspaceId][meta.vbucket] = false
			w.keyspaceIdSyncDue[meta.keyspaceId] = true
		}
	} else {
		logging.Debugf("MutationStreamReader::updateOSOMarkerInFilter Missing"+
			"keyspaceId %v in Filter for Stream %v", meta.keyspaceId, w.streamId)
	}

}

//updates snapshot information in keyspaceId filter
func (w *streamWorker) updateSnapInFilter(meta *MutationMeta,
	snapStart uint64, snapEnd uint64) {

	w.lock.Lock()
	defer w.lock.Unlock()

	if snapEnd < snapStart {
		logging.Errorf("MutationStreamReader::updateSnapInFilter Bad Snapshot Received "+
			"for %v %v %v %v-%v", meta.keyspaceId, meta.vbucket, w.streamId, snapStart, snapEnd)
	}

	if snapEnd-snapStart > 50000 {
		logging.Infof("MutationStreamReader::updateSnapInFilter Huge Snapshot Received "+
			"for %v %v %v %v-%v", meta.keyspaceId, meta.vbucket, w.streamId, snapStart, snapEnd)
	}
	resetStream := func() {
		logging.Infof("MutationStreamReader::updateSnapInFilter %v %v."+
			" Resetting Stream.", w.streamId, meta.keyspaceId)
		w.keyspaceIdOSOException[meta.keyspaceId] = true
		w.reader.supvRespch <- &MsgStreamUpdate{
			mType:      RESET_STREAM,
			streamId:   w.streamId,
			keyspaceId: meta.keyspaceId,
			sessionId:  w.keyspaceIdSessionId[meta.keyspaceId],
		}
	}

	if filter, ok := w.keyspaceIdFilter[meta.keyspaceId]; ok {

		//validate sessionId. allow opaque==0 for backward compat
		if meta.opaque != 0 &&
			meta.opaque != w.keyspaceIdSessionId[meta.keyspaceId] {
			return
		}

		//if current snapshot start from 0 and the filter doesn't have any snapshot
		if w.markFirstSnap && snapStart == 0 && filter.Snapshots[meta.vbucket][1] == 0 {
			w.keyspaceIdFirstSnap[meta.keyspaceId][meta.vbucket] = true
		} else {
			w.keyspaceIdFirstSnap[meta.keyspaceId][meta.vbucket] = false
		}

		if snapEnd > filter.Snapshots[meta.vbucket][1] &&
			filter.Vbuuids[meta.vbucket] != 0 {

			enableOSO := w.keyspaceIdEnableOSO[meta.keyspaceId]
			if enableOSO {

				filterOSO := w.keyspaceIdFilter[meta.keyspaceId]

				//first regular snapshot after OSO
				if filter.Snapshots[meta.vbucket][0] == 0 &&
					filter.Snapshots[meta.vbucket][1] == 0 &&
					filterOSO.Snapshots[meta.vbucket][0] == 1 {

					if filterOSO.Snapshots[meta.vbucket][1] != 1 {
						logging.Errorf("MutationStreamReader::updateSnapInFilter %v %v "+
							"Received Snapshot For Vbucket %v without OSO End. Seqno %v. "+
							"Count %v. OSO Start %v. OSO End %v. Snapshot %v-%v.", w.streamId,
							meta.keyspaceId, meta.vbucket, filterOSO.Seqnos[meta.vbucket],
							filterOSO.Vbuuids[meta.vbucket], filterOSO.Snapshots[meta.vbucket][0],
							filterOSO.Snapshots[meta.vbucket][1], snapStart, snapEnd)
						resetStream()
					}

					if snapStart < filterOSO.Seqnos[meta.vbucket] {

						logging.Errorf("MutationStreamReader::updateSnapInFilter %v %v "+
							"Received Snapshot For Vbucket %v lower than last seqno. Seqno %v. "+
							"Count %v. OSO Start %v. OSO End %v. Snapshot %v-%v.", w.streamId,
							meta.keyspaceId, meta.vbucket, filterOSO.Seqnos[meta.vbucket],
							filterOSO.Vbuuids[meta.vbucket], filterOSO.Snapshots[meta.vbucket][0],
							filterOSO.Snapshots[meta.vbucket][1], snapStart, snapEnd)
						resetStream()
					}

					prevSnap := w.keyspaceIdPrevSnapMap[meta.keyspaceId]
					prevSnap.Snapshots[meta.vbucket][0] = filterOSO.Snapshots[meta.vbucket][0]
					prevSnap.Snapshots[meta.vbucket][1] = filterOSO.Snapshots[meta.vbucket][1]
					prevSnap.Seqnos[meta.vbucket] = filterOSO.Seqnos[meta.vbucket]

					//filterOSO Vbuuid represents the count, filter has the actual vbuuid
					prevSnap.Vbuuids[meta.vbucket] = filter.Vbuuids[meta.vbucket]

					filter.Snapshots[meta.vbucket][0] = snapStart
					filter.Snapshots[meta.vbucket][1] = snapEnd
					return
				}
			}

			//store the existing snap marker in prevSnap map
			prevSnap := w.keyspaceIdPrevSnapMap[meta.keyspaceId]
			prevSnap.Snapshots[meta.vbucket][0] = filter.Snapshots[meta.vbucket][0]
			prevSnap.Snapshots[meta.vbucket][1] = filter.Snapshots[meta.vbucket][1]
			prevSnap.Vbuuids[meta.vbucket] = filter.Vbuuids[meta.vbucket]
			prevSnap.Seqnos[meta.vbucket] = filter.Seqnos[meta.vbucket]

			if prevSnap.Snapshots[meta.vbucket][1] != filter.Seqnos[meta.vbucket] {
				logging.Warnf("MutationStreamReader::updateSnapInFilter "+
					"KeyspaceId %v Stream %v vb %v Partial Snapshot %v-%v Seqno %v vbuuid %v."+
					"Next Snapshot %v-%v.", meta.keyspaceId,
					w.streamId, meta.vbucket, prevSnap.Snapshots[meta.vbucket][0],
					prevSnap.Snapshots[meta.vbucket][1], filter.Seqnos[meta.vbucket],
					prevSnap.Vbuuids[meta.vbucket], filter.Snapshots[meta.vbucket][0],
					filter.Snapshots[meta.vbucket][1])

				//subsequent complete snapshots can overwrite the partial snapshot
				//information. Store it separately for sync message.
				filter.DisableAlign = true
			}

			filter.Snapshots[meta.vbucket][0] = snapStart
			filter.Snapshots[meta.vbucket][1] = snapEnd

			logging.Debugf("MutationStreamReader::updateSnapInFilter "+
				"keyspaceId %v Stream %v vb %v Snapshot %v-%v Prev Snapshot %v-%v Prev Snapshot vbuuid %v",
				meta.keyspaceId, w.streamId, meta.vbucket, snapStart, snapEnd, prevSnap.Snapshots[meta.vbucket][0],
				prevSnap.Snapshots[meta.vbucket][1], prevSnap.Vbuuids[meta.vbucket])

		} else {
			logging.Warnf("MutationStreamReader::updateSnapInFilter Skipped "+
				"Snapshot %v-%v for vb %v %v %v. Current Filter %v vbuuid %v", snapStart,
				snapEnd, meta.vbucket, meta.keyspaceId, w.streamId,
				filter.Snapshots[meta.vbucket][1], filter.Vbuuids[meta.vbucket])
		}
	} else {
		logging.Debugf("MutationStreamReader::updateSnapInFilter Missing"+
			"keyspaceId %v in Filter for Stream %v", meta.keyspaceId, w.streamId)
	}

}

//updates vbuuid information in keyspaceId filter
func (w *streamWorker) updateVbuuidInFilter(meta *MutationMeta) {

	w.lock.Lock()
	defer w.lock.Unlock()

	if meta.vbuuid == 0 {
		logging.Fatalf("MutationStreamReader::updateVbuuidInFilter Received vbuuid %v "+
			"keyspaceId %v vb %v Stream %v. This vbucket will not be processed!!!", meta.vbuuid,
			meta.keyspaceId, meta.vbucket, w.streamId)
	}

	if filter, ok := w.keyspaceIdFilter[meta.keyspaceId]; ok {
		//validate sessionId. allow opaque==0 for backward compat
		if meta.opaque != 0 &&
			meta.opaque != w.keyspaceIdSessionId[meta.keyspaceId] {
			return
		}

		filter.Vbuuids[meta.vbucket] = uint64(meta.vbuuid)
	} else {
		logging.Errorf("MutationStreamReader::updateVbuuidInFilter Missing"+
			"keyspaceId %v vb %v vbuuid %v in Filter for Stream %v", meta.keyspaceId,
			meta.vbucket, meta.vbuuid, w.streamId)
	}

}

func (w *streamWorker) processDcpOSOMarker(meta *MutationMeta, eventType byte) {

	msg := &MsgStream{
		mType:     STREAM_READER_OSO_SNAPSHOT_MARKER,
		streamId:  w.streamId,
		meta:      meta.Clone(),
		eventType: eventType,
	}
	w.reader.supvRespch <- msg

}

func (w *streamWorker) processDcpSystemEvent(meta *MutationMeta,
	eventType byte, manifestuid, scopeId, collectionId string) {

	//send message to supervisor to take decision
	msg := &MsgStream{
		mType:        STREAM_READER_SYSTEM_EVENT,
		streamId:     w.streamId,
		meta:         meta.Clone(),
		eventType:    eventType,
		manifestuid:  manifestuid,
		scopeId:      scopeId,
		collectionId: collectionId,
	}
	w.reader.supvRespch <- msg

}

func (w *streamWorker) getLatencyObj(keyspaceId string, vbucket Vbucket) *Stats.Int64Val {
	perStreamKeyspaceId := fmt.Sprintf("%v/%v", w.streamId, keyspaceId)

	vbMap := w.vbMap.Get()
	if vbMap != nil {
		if vbToHostMap, ok := vbMap[perStreamKeyspaceId]; ok && vbToHostMap != nil {
			if host, ok := vbToHostMap[vbucket]; ok {
				stats := w.reader.stats.Get()
				latencyMap := stats.prjLatencyMap.Get()
				if latencyMap != nil {
					if avg, ok := latencyMap[host]; ok {
						return avg.(*Stats.Int64Val)
					}
				}
			}
		}
	}
	return nil
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

	if common.GetStorageMode() == common.FORESTDB {
		return config["stream_reader.fdb.syncBatchInterval"].Uint64()
	} else {
		return config["stream_reader.moi.syncBatchInterval"].Uint64()
	}

}

func getMutationBufferSize(config common.Config) uint64 {

	if common.GetStorageMode() == common.FORESTDB {
		return config["stream_reader.fdb.mutationBuffer"].Uint64()
	} else if common.GetStorageMode() == common.PLASMA {
		return config["stream_reader.plasma.mutationBuffer"].Uint64()
	} else {
		return config["stream_reader.moi.mutationBuffer"].Uint64()
	}

}

func getWorkerBufferSize(config common.Config) uint64 {

	if common.GetStorageMode() == common.FORESTDB {
		return config["stream_reader.fdb.workerBuffer"].Uint64()
	} else if common.GetStorageMode() == common.PLASMA {
		return config["stream_reader.plasma.workerBuffer"].Uint64()
	} else {
		return config["stream_reader.moi.workerBuffer"].Uint64()
	}
}

func overrideDataportConf(dpconf common.Config) common.Config {

	if common.GetStorageMode() == common.PLASMA {
		chSize := dpconf["plasma.dataChanSize"].Int()
		dpconf.SetValue("dataChanSize", chSize)
	}

	return dpconf

}

func getMarkFirstSnap(config common.Config) bool {

	return config["stream_reader.markFirstSnap"].Bool()

}
