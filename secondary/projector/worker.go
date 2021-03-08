// worker concurrency model:
//
//                           NewVbucketWorker()
//                                   |_________________
//                                   |                 |
//                                (spawn)           (spawn)
//                                   |                 |        *---> endpoint
//        AddEngines() --*           |                 |        |
//                       |--------> genServer     *-> run ------*---> endpoint
//       ResetConfig() --*                        |             |
//                       |                        |             *---> endpoint
//     DeleteEngines() --*                      Event()
//                       |
//     GetStatistics() --*
//                       |
//             Close() --*

package projector

import (
	"bytes"
	"fmt"
	"strconv"
	"sync"

	qexpr "github.com/couchbase/query/expression"
	qvalue "github.com/couchbase/query/value"

	mcd "github.com/couchbase/indexing/secondary/dcp/transport"

	mc "github.com/couchbase/indexing/secondary/dcp/transport/client"

	"os"
	"sync/atomic"

	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/stats"
)

var transactionMutationPrefix = []byte("_txn:")

// VbucketWorker is immutable structure defined for each vbucket.
type VbucketWorker struct {
	id         int
	feed       *Feed
	cluster    string
	topic      string
	bucket     string
	keyspaceId string
	opaque     uint16
	config     c.Config
	vbuckets   *VbucketMapHolder
	// evaluators and subscribers
	engines   *CollectionsEngineMapHolder // CollectionId -> instanceId -> engine
	endpoints *EndpointMapHolder          // Endpoint address -> RouterEndpoint

	mutex sync.Mutex // Mutex protecting the endpoint updates from genServer() and run() routines
	// server channels
	sbch   chan []interface{}
	datach chan []interface{}

	genServerStopCh chan bool
	genServerFinCh  chan bool
	runFinCh        chan bool
	// config params
	logPrefix   string
	mutChanSize int
	opaque2     uint64 //client opaque
	osoSnapshot bool

	encodeBuf []byte
	stats     *WorkerStats
}

type WorkerStats struct {
	closed       stats.BoolVal
	datach       chan []interface{}
	outgoingMut  stats.Uint64Val // Number of mutations consumed from this worker
	updateSeqno  stats.Uint64Val // Number of updateSeqno messages sent by this worker
	txnSystemMut stats.Uint64Val // Number of mutations skipped for transactions
}

func (stats *WorkerStats) Init() {
	stats.closed.Init()
	stats.outgoingMut.Init()
	stats.updateSeqno.Init()
	stats.txnSystemMut.Init()
}

func (stats *WorkerStats) IsClosed() bool {
	return stats.closed.Value()
}

// NewVbucketWorker creates a new routine to handle this vbucket stream.
func NewVbucketWorker(
	id int, feed *Feed, bucket, keyspaceId string,
	opaque uint16, config c.Config, opaque2 uint64) *VbucketWorker {

	mutChanSize := config["mutationChanSize"].Int()
	encodeBufSize := config["encodeBufSize"].Int()

	worker := &VbucketWorker{
		id:              id,
		feed:            feed,
		cluster:         feed.cluster,
		topic:           feed.topic,
		bucket:          bucket,
		keyspaceId:      keyspaceId,
		opaque:          opaque,
		config:          config,
		vbuckets:        &VbucketMapHolder{},
		engines:         &CollectionsEngineMapHolder{},
		endpoints:       &EndpointMapHolder{},
		sbch:            make(chan []interface{}, mutChanSize),
		datach:          make(chan []interface{}, mutChanSize),
		genServerStopCh: make(chan bool),
		genServerFinCh:  make(chan bool),
		runFinCh:        make(chan bool),
		encodeBuf:       make([]byte, 0, encodeBufSize),
		stats:           &WorkerStats{},
		opaque2:         opaque2,
	}
	worker.stats.Init()
	worker.stats.datach = worker.datach
	worker.osoSnapshot = feed.osoSnapshot[keyspaceId]
	fmsg := "WRKR[%v<-%v<-%v #%v]"
	worker.logPrefix = fmt.Sprintf(fmsg, id, keyspaceId, feed.cluster, feed.topic)
	worker.mutChanSize = mutChanSize

	go worker.genServer(worker.sbch)
	go worker.run(worker.datach)
	return worker
}

// commands to server
const (
	vwCmdEvent byte = iota + 1
	vwCmdSyncPulse
	vwCmdGetVbuckets
	vwCmdAddEngines
	vwCmdDelEngines
	vwCmdGetStats
	vwCmdResetConfig
	vwCmdClose
)

// Event will post an DcpEvent, asychronous call.
func (worker *VbucketWorker) Event(m *mc.DcpEvent) error {
	cmd := []interface{}{vwCmdEvent, m}
	return c.FailsafeOpAsync(worker.datach, cmd, worker.genServerStopCh)
}

// SyncPulse will trigger worker to generate a sync pulse for all its
// vbuckets, asychronous call.
func (worker *VbucketWorker) SyncPulse() error {
	cmd := []interface{}{vwCmdSyncPulse}
	return c.FailsafeOpAsync(worker.datach, cmd, worker.genServerStopCh)
}

// GetVbuckets will return the list of active vbuckets managed by this
// workers.
func (worker *VbucketWorker) GetVbuckets() (map[uint16]*Vbucket, error) {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{vwCmdGetVbuckets, respch}
	resp, err := c.FailsafeOp(worker.sbch, respch, cmd, worker.genServerStopCh)
	if err != nil {
		return nil, err
	}
	return resp[0].(map[uint16]*Vbucket), nil
}

// AddEngines update active set of engines and endpoints, synchronous call.
func (worker *VbucketWorker) AddEngines(
	opaque uint16,
	engines map[uint64]*Engine,
	endpoints map[string]c.RouterEndpoint) (map[uint16]uint64, error) {

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{vwCmdAddEngines, opaque, engines, endpoints, respch}
	resp, err := c.FailsafeOp(worker.sbch, respch, cmd, worker.genServerStopCh)
	if err != nil {
		return nil, err
	}
	return resp[0].(map[uint16]uint64), nil
}

// DeleteEngines delete engines and update endpoints
// synchronous call.
func (worker *VbucketWorker) DeleteEngines(
	opaque uint16, engines []uint64, collectionIds []uint32) error {

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{vwCmdDelEngines, opaque, engines, collectionIds, respch}
	_, err := c.FailsafeOp(worker.sbch, respch, cmd, worker.genServerStopCh)
	return err
}

// ResetConfig for worker-routine, synchronous call.
func (worker *VbucketWorker) ResetConfig(config c.Config) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{vwCmdResetConfig, config, respch}
	_, err := c.FailsafeOp(worker.sbch, respch, cmd, worker.genServerStopCh)
	return err
}

// GetStatistics for worker vbucket, synchronous call.
func (worker *VbucketWorker) GetStatistics() (map[string]interface{}, error) {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{vwCmdGetStats, respch}
	resp, err := c.FailsafeOp(worker.sbch, respch, cmd, worker.genServerStopCh)
	if err != nil {
		return nil, err
	}
	return resp[0].(map[string]interface{}), nil
}

// Close worker-routine, synchronous call.
func (worker *VbucketWorker) Close() error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{vwCmdClose, respch}
	_, err := c.FailsafeOp(worker.sbch, respch, cmd, worker.genServerStopCh)
	return err
}

// routine handles data path for a single worker handling one
// or more vbuckets.
func (worker *VbucketWorker) run(datach chan []interface{}) {
	logPrefix := worker.logPrefix
	logging.Infof("%v run() started ...", logPrefix)

	defer func() { // panic safe
		if r := recover(); r != nil {
			fmsg := "%v ##%x run() crashed: %v\n"
			logging.Fatalf(fmsg, logPrefix, worker.opaque, r)
			logging.Errorf("%v", logging.StackTrace())
		}
		engines := worker.engines.Get()
		vbuckets := worker.vbuckets.Get()
		// call out a STREAM-END for active vbuckets.
		for _, v := range vbuckets {
			if data := v.makeStreamEndData(engines); data != nil {
				worker.broadcast2Endpoints(data)
			} else {
				fmsg := "%v ##%x StreamEnd NOT PUBLISHED vb %v\n"
				logging.Errorf(fmsg, logPrefix, worker.opaque, v.vbno)
			}
		}
		// In case run() exits first, close genServerFinCh to terminate
		// genServer()
		close(worker.genServerFinCh)
		worker.stats.closed.Set(true)
		logging.Infof("%v ##%x ##%v run()... stopped\n", logPrefix,
			worker.opaque, worker.opaque2)
	}()

loop:
	for {
		select {
		case msg := <-datach:
			cmd := msg[0].(byte)
			switch cmd {
			case vwCmdEvent:
				worker.stats.outgoingMut.Add(1)
				m := msg[1].(*mc.DcpEvent)
				v := worker.handleEvent(m)
				if v == nil {
					fmsg := "%v ##%x nil vbucket %v for %v"
					logging.Errorf(fmsg, logPrefix, m.Opaque, m.VBucket, m.Opcode)

				} else if m.Opaque != v.opaque {
					fmsg := "%v ##%x mismatch with vbucket, vb:%v. ##%x %v"
					logging.Fatalf(fmsg, logPrefix, m.Opaque, v.vbno, v.opaque, m.Opcode)
					//workaround for MB-30327. this state should never happen.
					os.Exit(1)
				}

			case vwCmdSyncPulse:
				engines := worker.engines.Get()
				vbuckets := worker.vbuckets.Get()

				for _, v := range vbuckets {
					if data := v.makeSyncData(engines); data != nil {
						atomic.AddUint64(&v.syncCount, 1)
						fmsg := "%v ##%x sync count %v\n"
						logging.Tracef(fmsg, v.logPrefix, v.opaque, v.syncCount)
						worker.broadcast2Endpoints(data)

					} else {
						fmsg := "%v ##%x Sync NOT PUBLISHED for %v\n"
						logging.Errorf(fmsg, logPrefix, worker.opaque, v.vbno)
					}
				}
			}
		case <-worker.runFinCh:
			break loop
		}
	}
}

func (worker *VbucketWorker) genServer(sbch chan []interface{}) {
	logPrefix := worker.logPrefix
	logging.Infof("%v genServer() started ...", logPrefix)

	defer func() { // panic safe
		if r := recover(); r != nil {
			fmsg := "%v ##%x genServer() crashed: %v\n"
			logging.Fatalf(fmsg, logPrefix, worker.opaque, r)
			logging.Errorf("%v", logging.StackTrace())
		}

		// Close genServerStopCh so that all new incoming messages
		// will return
		close(worker.genServerStopCh)
		close(worker.runFinCh)
		logging.Infof("%v ##%x ##%v genServer()... stopped\n", logPrefix,
			worker.opaque, worker.opaque2)
	}()

loop:
	for {
		select {
		case msg := <-sbch:
			if breakloop := worker.handleCommand(msg); breakloop {
				break loop
			}

		case <-worker.genServerFinCh:
			break loop
		}
	}
}

func (worker *VbucketWorker) handleCommand(msg []interface{}) bool {
	cmd := msg[0].(byte)
	switch cmd {
	case vwCmdGetVbuckets:

		vbuckets := CloneVbucketMap(worker.vbuckets.Get())
		respch := msg[1].(chan []interface{})
		respch <- []interface{}{vbuckets}

	case vwCmdAddEngines:
		engines := make(map[uint32]EngineMap)
		opaque := msg[1].(uint16)
		if msg[2] != nil {
			fmsg := "%v ##%x AddEngine %v\n"
			for uuid, engine := range msg[2].(map[uint64]*Engine) {
				cid := getCidAsUint32(engine.GetCollectionID())
				if _, ok := engines[cid]; !ok {
					engines[cid] = make(EngineMap)
				}
				engines[cid][uuid] = engine
				logging.Tracef(fmsg, worker.logPrefix, opaque, uuid)
			}
			worker.engines.Set(engines)
			worker.printCtrl(worker.engines.Get())
		}
		if msg[3] != nil {
			func() {
				worker.mutex.Lock()
				defer worker.mutex.Unlock()

				endpoints := msg[3].(map[string]c.RouterEndpoint)
				eps := worker.updateEndpoints(opaque, endpoints)
				worker.endpoints.Set(eps)
			}()
			worker.printCtrl(worker.endpoints.Get())
		}
		cseqnos := make(map[uint16]uint64)
		vbuckets := worker.vbuckets.Get()
		for _, v := range vbuckets {
			cseqnos[v.vbno] = atomic.LoadUint64(&v.seqno)
		}
		respch := msg[4].(chan []interface{})
		respch <- []interface{}{cseqnos}

	case vwCmdDelEngines:
		opaque := msg[1].(uint16)
		fmsg := "%v ##%x vwCmdDeleteEngines\n"
		logging.Tracef(fmsg, worker.logPrefix, opaque)
		engineKeys := msg[2].([]uint64)
		collectionIds := msg[3].([]uint32)
		fmsg = "%v ##%x DelEngine %v\n"
		engines := CloneEngines(worker.engines.Get())
		for i, uuid := range engineKeys {
			cid := collectionIds[i]
			delete(engines[cid], uuid)
			if len(engines[cid]) == 0 {
				delete(engines, cid)
			}
			logging.Tracef(fmsg, worker.logPrefix, opaque, uuid)
		}
		worker.engines.Set(engines)

		fmsg = "%v ##%x deleted engines %v\n"
		logging.Tracef(fmsg, worker.logPrefix, opaque, engineKeys)
		respch := msg[4].(chan []interface{})
		respch <- []interface{}{nil}

	case vwCmdGetStats:
		logging.Tracef("%v vwCmdStatistics\n", worker.logPrefix)
		stats := make(map[string]interface{})
		vbuckets := worker.vbuckets.Get()
		for vbno, v := range vbuckets {
			stats[strconv.Itoa(int(vbno))] = map[string]interface{}{
				"syncs":     float64(atomic.LoadUint64(&v.syncCount)),
				"snapshots": float64(atomic.LoadUint64(&v.sshotCount)),
				"mutations": float64(atomic.LoadUint64(&v.mutationCount)),
			}
		}
		respch := msg[1].(chan []interface{})
		respch <- []interface{}{stats}

	case vwCmdResetConfig:
		_, respch := msg[1].(c.Config), msg[2].(chan []interface{})
		respch <- []interface{}{nil}

	case vwCmdClose:
		logging.Infof("%v ##%x closed\n", worker.logPrefix, worker.opaque)
		respch := msg[1].(chan []interface{})
		respch <- []interface{}{nil}
		return true
	}
	return false
}

// only endpoints that host engines defined on this vbucket.
func (worker *VbucketWorker) updateEndpoints(
	opaque uint16,
	eps map[string]c.RouterEndpoint) map[string]c.RouterEndpoint {

	engines := worker.engines.Get()
	endpoints := make(map[string]c.RouterEndpoint)
	for _, enginesPerColl := range engines {
		for _, engine := range enginesPerColl {
			for _, raddr := range engine.Endpoints() {
				if _, ok := eps[raddr]; !ok {
					fmsg := "%v ##%x endpoint %v not found\n"
					logging.Errorf(fmsg, worker.logPrefix, opaque, raddr)
					continue
				}
				fmsg := "%v ##%x UpdateEndpoint %v\n"
				logging.Tracef(fmsg, worker.logPrefix, opaque, raddr)
				endpoints[raddr] = eps[raddr]
			}
		}
	}
	return endpoints
}

var traceMutFormat = "%v ##%x DcpEvent %v:%v <<%v>>\n"

func (worker *VbucketWorker) handleEvent(m *mc.DcpEvent) *Vbucket {

	defer func() {
		if r := recover(); r != nil {
			logging.Fatalf("VbucketWorker.handleEvent key = %v value = %v", logging.TagStrUD(m.Key), logging.TagStrUD(m.Value))
			panic(r)
		}
	}()

	vbuckets := worker.vbuckets.Get()
	vbno := m.VBucket
	v, vbok := vbuckets[vbno]
	logPrefix := worker.logPrefix
	allEngines := worker.engines.Get()

	logging.LazyTrace(func() string {
		return fmt.Sprintf(traceMutFormat, logPrefix, m.Opaque, m.Seqno, m.Opcode, logging.TagUD(m.Key))
	})

	switch m.Opcode {
	case mcd.DCP_STREAMREQ: // broadcast StreamBegin

		if m.Status == mcd.SUCCESS {
			if vbok {
				fmsg := "%v ##%x duplicate OpStreamRequest: %v\n"
				arg1 := logging.TagUD(m)
				logging.Errorf(fmsg, logPrefix, m.Opaque, arg1)
				return v
			}
		}
		// opens up the path
		cluster, topic, bucket := worker.cluster, worker.topic, worker.bucket
		keyspaceId := worker.keyspaceId
		config, opaque, vbuuid := worker.config, m.Opaque, m.VBuuid
		v = NewVbucket(cluster, topic, bucket, keyspaceId, opaque, vbno, vbuuid,
			m.Seqno, config, worker.opaque2, worker.osoSnapshot)

		if m.Status == mcd.SUCCESS {
			vbs := CloneVbucketMap(vbuckets)
			vbs[vbno] = v
			worker.vbuckets.Set(vbs)
		}

		if data := v.makeStreamBeginData(allEngines,
			byte(v.mcStatus2StreamStatus(m.Status)), byte(m.Status)); data != nil {
			worker.broadcast2Endpoints(data)
		} else {
			fmsg := "%v ##%x StreamBeginData NOT PUBLISHED for vbucket %v\n"
			logging.Errorf(fmsg, logPrefix, m.Opaque, vbno)
		}
		return v

	case mcd.DCP_SNAPSHOT: // broadcast Snapshot
		if !vbok {
			fmsg := "%v ##%x vbucket %v not started\n"
			logging.Errorf(fmsg, logPrefix, m.Opaque, vbno)
			return v
		}
		if data := v.makeSnapshotData(m, allEngines); data != nil {
			worker.broadcast2Endpoints(data)
			atomic.AddUint64(&v.sshotCount, 1)
		} else {
			fmsg := "%v ##%x Snapshot NOT PUBLISHED for vbucket %v\n"
			logging.Errorf(fmsg, logPrefix, m.Opaque, vbno)
		}
		return v

	case mcd.DCP_MUTATION, mcd.DCP_DELETION, mcd.DCP_EXPIRATION:
		if !vbok {
			fmsg := "%v ##%x vbucket %v not started\n"
			logging.Errorf(fmsg, logPrefix, m.Opaque, m.VBucket)
			return v
		}
		atomic.AddUint64(&v.mutationCount, 1)
		atomic.StoreUint64(&v.seqno, m.Seqno)

		processMutation := func(collEngines EngineMap) {
			// prepare a data for each endpoint.
			dataForEndpoints := make(map[string]interface{})
			// for each engine distribute transformations to endpoints.

			var nvalue qvalue.Value
			if m.IsJSON() {
				nvalue = qvalue.NewParsedValueWithOptions(m.Value, true, true)
			} else {
				nvalue = qvalue.NewBinaryValue(m.Value)
			}

			context := qexpr.NewIndexContext()
			docval := qvalue.NewAnnotatedValue(nvalue)
			for _, engine := range collEngines {
				// Slices in KeyVersions struct are updated for all the indexes
				// belonging to this keyspace. Hence, pre-allocate the memory for
				// slices with number of indexes instead of expanding the slice
				// due to lack of size. This helps to reduce the re-allocs and
				// therefore reduces the garbage generated.
				newBuf, err := engine.TransformRoute(
					v.vbuuid, m, dataForEndpoints, worker.encodeBuf, docval, context,
					len(collEngines), worker.opaque2, worker.osoSnapshot,
				)
				if err != nil {
					fmsg := "%v ##%x TransformRoute: %v for index %v docid %s\n"
					logging.Errorf(fmsg, logPrefix, m.Opaque, err, engine.GetIndexName(),
						logging.TagStrUD(m.Key))
				}
				// TODO: Shrink the buffer periodically or as needed
				if cap(newBuf) > cap(worker.encodeBuf) {
					worker.encodeBuf = newBuf[:0]
				}
			}
			endpoints := worker.endpoints.Get()
			// send data to corresponding endpoint.
			for raddr, data := range dataForEndpoints {
				if endpoint, ok := endpoints[raddr]; ok {
					// FIXME: without the coordinator doing shared topic
					// management, we will allow the feed to block.
					// Otherwise, send might fail due to ErrorChannelFull
					// or ErrorClosed
					if err := endpoint.Send(data); err != nil {
						fmsg := "%v ##%x endpoint(%q).Send() failed: %v"
						logging.Debugf(fmsg, logPrefix, worker.opaque, raddr, err)
						func() {
							worker.mutex.Lock()
							defer worker.mutex.Unlock()

							endpoint.Close()
							eps := CloneEndpoints(worker.endpoints.Get())
							if _, ok := eps[raddr]; ok {
								delete(eps, raddr)
								worker.endpoints.Set(eps)
							}
						}()
					}
				}
			}

		}

		isTxn := (m.Opcode == mcd.DCP_MUTATION) && !m.IsJSON() && m.HasXATTR() && bytes.HasPrefix(m.Key, transactionMutationPrefix)
		if isTxn {
			worker.stats.txnSystemMut.Add(1)
		}

		// If the mutation belongs to a collection other than the
		// ones that are being processed at worker, send UpdateSeqno
		// message to indexer
		// The else case should get executed only incase of MAINT_STREAM
		if collEngines, ok := allEngines[m.CollectionID]; ok && !isTxn {
			processMutation(collEngines)
		} else {
			// Generate updateSeqno message and propagate it to indexer
			worker.stats.updateSeqno.Add(1)
			if data := v.makeUpdateSeqnoData(m, allEngines); data != nil {
				worker.broadcast2Endpoints(data)
			} else {
				fmsg := "%v ##%x SYSTEM_EVENT: %v NOT PUBLISHED for vbucket %v\n"
				logging.Errorf(fmsg, logPrefix, m.Opaque, m, vbno)
			}
		}

	case mcd.DCP_SYSTEM_EVENT:
		if !vbok {
			fmsg := "%v ##%x vbucket %v not started. Received system event\n"
			logging.Errorf(fmsg, logPrefix, m.Opaque, vbno)
			return v
		}
		atomic.StoreUint64(&v.seqno, m.Seqno) // update seqno for system event
		if data := v.makeSystemEventData(m, allEngines); data != nil {
			worker.broadcast2Endpoints(data)
		} else {
			fmsg := "%v ##%x SYSTEM_EVENT: %v NOT PUBLISHED for vbucket %v\n"
			logging.Errorf(fmsg, logPrefix, m.Opaque, m, vbno)
		}

	case mcd.DCP_SEQNO_ADVANCED:
		if !vbok {
			fmsg := "%v ##%x vbucket %v not started. Received SeqnoAdvanced event\n"
			logging.Errorf(fmsg, logPrefix, m.Opaque, vbno)
			return v
		}
		atomic.StoreUint64(&v.seqno, m.Seqno) // update seqno for seqno advanced
		if data := v.makeSeqnoAdvancedEvent(m, allEngines); data != nil {
			worker.broadcast2Endpoints(data)
		} else {
			fmsg := "%v ##%x SEQNO_ADVANCED: %v NOT PUBLISHED for vbucket %v\n"
			logging.Errorf(fmsg, logPrefix, m.Opaque, m, vbno)
		}

	case mcd.DCP_OSO_SNAPSHOT:
		if !vbok {
			fmsg := "%v ##%x vbucket %v not started. Received OSOSnapshot event\n"
			logging.Errorf(fmsg, logPrefix, m.Opaque, vbno)
			return v
		}
		if data := v.makeOSOSnapshotEvent(m, allEngines); data != nil {
			worker.broadcast2Endpoints(data)
		} else {
			fmsg := "%v ##%x OSO_SNAPSHOT: %v NOT PUBLISHED for vbucket %v\n"
			logging.Errorf(fmsg, logPrefix, m.Opaque, m, vbno)
		}

	case mcd.DCP_STREAMEND:
		if !vbok {
			fmsg := "%v ##%x vbucket %v not started. Received StreamEnd\n"
			logging.Errorf(fmsg, logPrefix, m.Opaque, vbno)
			return v
		}
		if data := v.makeStreamEndData(allEngines); data != nil {
			worker.broadcast2Endpoints(data)
		} else {
			fmsg := "%v ##%x StreamEnd NOT PUBLISHED vb %v\n"
			logging.Errorf(fmsg, logPrefix, worker.opaque, v.vbno)
		}
		vbs := CloneVbucketMap(worker.vbuckets.Get())
		delete(vbs, vbno)
		worker.vbuckets.Set(vbs)
	}
	return v
}

// send to all endpoints.
func (worker *VbucketWorker) broadcast2Endpoints(data interface{}) {
	endpoints := worker.endpoints.Get()

	for raddr, endpoint := range endpoints {
		// FIXME: without the coordinator doing shared topic
		// management, we will allow the feed to block.
		// Otherwise, send might fail due to ErrorChannelFull
		// or ErrorClosed
		if err := endpoint.Send(data); err != nil {
			fmsg := "%v ##%x endpoint(%q).Send() failed: %v"
			logging.Debugf(fmsg, worker.logPrefix, worker.opaque, raddr, err)

			func() {
				worker.mutex.Lock()
				defer worker.mutex.Unlock()

				endpoint.Close()
				eps := CloneEndpoints(worker.endpoints.Get())
				if _, ok := eps[raddr]; ok {
					delete(eps, raddr)
					worker.endpoints.Set(eps)
				}
			}()
		}
	}
}

func (worker *VbucketWorker) printCtrl(v interface{}) {
	switch val := v.(type) {
	case map[string]c.RouterEndpoint:
		for raddr := range val {
			fmsg := "%v ##%x knows endpoint %v\n"
			logging.Tracef(fmsg, worker.logPrefix, worker.opaque, raddr)
		}
	case map[uint32]EngineMap:
		for cid := range val {
			for uuid := range val[cid] {
				fmsg := "%v ##%x cid %v knows engine %v\n"
				logging.Tracef(fmsg, worker.logPrefix, worker.opaque, cid, uuid)
			}
		}
	}
}
