// data-path concurrency model:
//                                                     Mutations (dcp_feed)
//               back-channel                              |
//     feed <---------------------*   NewKVData()          |
//               StreamRequest    |     |____________      |
//               StreamEnd        |     |            |     |        *---> worker
//                                |   (spawn)    (spawn)   |        |
//                                |     |            |     |        *---> worker
//                                |     |            |     V        |
//        AddEngines() --*-----> genServer         runScatter-------*---> worker
//                       |
//     DeleteEngines() --*
//                       |
//     GetStatistics() --*
//                       |
//             Close() --*
//

package projector

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/stats"

	mcd "github.com/couchbase/indexing/secondary/dcp/transport"

	mc "github.com/couchbase/indexing/secondary/dcp/transport/client"

	c "github.com/couchbase/indexing/secondary/common"

	protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
)

// KVData captures an instance of data-path for single kv-node
// from upstream connection.
type KVData struct {
	feed         *Feed
	topic        string // immutable
	bucket       string // immutable
	keyspaceId   string // immutable
	collectionId string // immutable. Takes empty value for MAINT_STREAM
	opaque       uint16
	workers      []*VbucketWorker
	config       c.Config
	async        bool

	// evaluators and subscribers
	engines   map[uint64]*Engine
	endpoints map[string]c.RouterEndpoint
	// server channels
	sbch        chan []interface{}
	finch       chan bool
	stopScatter uint32

	reqTs      *protobuf.TsVbuuid
	reqTsMutex *sync.RWMutex // Mutex protecting updates to reqTs

	// Closing genServerStopCh will stop all incoming requests to the control path
	genServerStopCh       chan bool
	genServerFinCh        chan bool
	runScatterFinCh       chan bool
	runScatterDoneCh      chan bool
	runScatterDone        bool
	stopScatterFromFeedCh chan bool
	// misc.
	logPrefix string
	// statistics
	stats     *KvdataStats
	wrkrStats []interface{}
	uuid      uint64 // immutable
	kvaddr    string
	opaque2   uint64 //client opaque
}

type KvdataStats struct {
	closed        stats.BoolVal
	eventCount    stats.Uint64Val
	reqCount      stats.Uint64Val
	endCount      stats.Uint64Val
	snapStat      stats.Average
	upsertCount   stats.Uint64Val
	deleteCount   stats.Uint64Val
	exprCount     stats.Uint64Val
	ainstCount    stats.Uint64Val
	dinstCount    stats.Uint64Val
	tsCount       stats.Uint64Val
	mutch         <-chan *mc.DcpEvent
	vbseqnos      []stats.Uint64Val
	kvdata        *KVData  // Handle to KVData
	vbseqnos_copy []uint64 // Cached version of vbseqnos. Used only by stats_manager

	// Collection specific
	collectionCreate  stats.Uint64Val
	collectionDrop    stats.Uint64Val
	collectionFlush   stats.Uint64Val
	scopeCreate       stats.Uint64Val
	scopeDrop         stats.Uint64Val
	collectionChanged stats.Uint64Val
	seqnoAdvanced     stats.Uint64Val
	osoSnapshotStart  stats.Uint64Val
	osoSnapshotEnd    stats.Uint64Val
}

func (kvstats *KvdataStats) Init(numVbuckets int, kvdata *KVData) {
	kvstats.closed.Init()
	kvstats.eventCount.Init()
	kvstats.reqCount.Init()
	kvstats.endCount.Init()
	kvstats.snapStat.Init()
	kvstats.upsertCount.Init()
	kvstats.deleteCount.Init()
	kvstats.exprCount.Init()
	kvstats.ainstCount.Init()
	kvstats.dinstCount.Init()
	kvstats.tsCount.Init()
	kvstats.vbseqnos = make([]stats.Uint64Val, numVbuckets)
	for i, _ := range kvstats.vbseqnos {
		kvstats.vbseqnos[i].Init()
	}
	kvstats.kvdata = kvdata
	kvstats.vbseqnos_copy = make([]uint64, numVbuckets)

	kvstats.collectionCreate.Init()
	kvstats.collectionDrop.Init()
	kvstats.collectionFlush.Init()
	kvstats.scopeCreate.Init()
	kvstats.scopeDrop.Init()
	kvstats.collectionChanged.Init()
	kvstats.seqnoAdvanced.Init()
	kvstats.osoSnapshotStart.Init()
	kvstats.osoSnapshotEnd.Init()
}

func (kvstats *KvdataStats) IsClosed() bool {
	return kvstats.closed.Value()
}

func (stats *KvdataStats) String() (string, string) {
	var stitems [24]string
	var vbseqnos string
	var numDocsProcessed, numDocsPending uint64

	stitems[0] = `"eventCount":` + strconv.FormatUint(stats.eventCount.Value(), 10)
	stitems[1] = `"reqCount":` + strconv.FormatUint(stats.reqCount.Value(), 10)
	stitems[2] = `"endCount":` + strconv.FormatUint(stats.endCount.Value(), 10)
	stitems[3] = `"snapStat.samples":` + strconv.FormatInt(stats.snapStat.Count(), 10)
	stitems[4] = `"snapStat.min":` + strconv.FormatInt(stats.snapStat.Min(), 10)
	stitems[5] = `"snapStat.max":` + strconv.FormatInt(stats.snapStat.Max(), 10)
	stitems[6] = `"snapStat.avg":` + strconv.FormatInt(stats.snapStat.Mean(), 10)
	stitems[7] = `"upsertCount":` + strconv.FormatUint(stats.upsertCount.Value(), 10)
	stitems[8] = `"deleteCount":` + strconv.FormatUint(stats.deleteCount.Value(), 10)
	stitems[9] = `"exprCount":` + strconv.FormatUint(stats.exprCount.Value(), 10)
	stitems[10] = `"ainstCount":` + strconv.FormatUint(stats.ainstCount.Value(), 10)
	stitems[11] = `"dinstCount":` + strconv.FormatUint(stats.dinstCount.Value(), 10)
	stitems[12] = `"tsCount":` + strconv.FormatUint(stats.tsCount.Value(), 10)
	stitems[13] = `"mutChLen":` + strconv.FormatUint((uint64)(len(stats.mutch)), 10)

	stitems[14] = `"collectionCreate":` + strconv.FormatUint(stats.collectionCreate.Value(), 10)
	stitems[15] = `"collectionDrop":` + strconv.FormatUint(stats.collectionDrop.Value(), 10)
	stitems[16] = `"collectionFlush":` + strconv.FormatUint(stats.collectionFlush.Value(), 10)
	stitems[17] = `"scopeCreate":` + strconv.FormatUint(stats.scopeCreate.Value(), 10)
	stitems[18] = `"scopeDrop":` + strconv.FormatUint(stats.scopeDrop.Value(), 10)
	stitems[19] = `"collectionChanged":` + strconv.FormatUint(stats.collectionChanged.Value(), 10)
	stitems[20] = `"seqnoAdvanced":` + strconv.FormatUint(stats.seqnoAdvanced.Value(), 10)
	stitems[21] = `"osoSnapshotStart":` + strconv.FormatUint(stats.osoSnapshotStart.Value(), 10)
	stitems[22] = `"osoSnapshotEnd":` + strconv.FormatUint(stats.osoSnapshotEnd.Value(), 10)

	// A copy of vbseqnos is made so that numDocsProcessed can be consistent
	// with the sum of logged vbseqnos. Also, it helps to compute the numDocsPending
	// as stats.vbseqnos can move ahead of retrieved bucket seqnos (from getKVTs())
	// at the time of computation of numDocsPending
	for i, v := range stats.vbseqnos {
		stats.vbseqnos_copy[i] = v.Value()
		vbseqnos += fmt.Sprintf("%v ", stats.vbseqnos_copy[i])
		numDocsProcessed += stats.vbseqnos_copy[i]
	}

	stitems[23] = `"numDocsProcessed":` + strconv.FormatUint(numDocsProcessed, 10)
	statjson := strings.Join(stitems[:], ",")

	cluster := stats.kvdata.config["clusterAddr"].String()
	// Get seqnos only for the vbuckets owned by the KV on this node
	seqnos, err := getKVTs(stats.kvdata.bucket, cluster, stats.kvdata.kvaddr, stats.kvdata.collectionId)

	// numDocsPending is logged only when there is no error in retrieving the bucket seqnos
	if err == nil {
		for i, v := range stats.vbseqnos_copy {
			if seqnos[i] > v { // During a KV rollback, 'seqnos[i]' can become less than 'v'
				numDocsPending += seqnos[i] - v
			}
		}
		statjson = fmt.Sprintf("%v,\"numDocsPending\":%v", statjson, strconv.FormatUint(numDocsPending, 10))
	} else {
		fmsg := "KVDT[<-%v<-%v #%v] ##%x"
		key := fmt.Sprintf(fmsg, stats.kvdata.bucket, stats.kvdata.feed.cluster, stats.kvdata.topic, stats.kvdata.opaque)
		logging.Errorf("%v Unable to retrieve bucket sequence numbers, err: %v", key, err)
	}

	statsStr := fmt.Sprintf("{%v}", statjson)
	return statsStr, vbseqnos
}

func (stats *KvdataStats) Map() (map[string]interface{}, []uint64) {
	var stmap = make(map[string]interface{})
	var numDocsProcessed, numDocsPending uint64
	var vbseqnos = make([]uint64, 0, 1024)

	stmap["eventCount"] = strconv.FormatUint(stats.eventCount.Value(), 10)
	stmap["reqCount"] = strconv.FormatUint(stats.reqCount.Value(), 10)
	stmap["endCount"] = strconv.FormatUint(stats.endCount.Value(), 10)
	stmap["snapStat.samples"] = strconv.FormatInt(stats.snapStat.Count(), 10)
	stmap["snapStat.min"] = strconv.FormatInt(stats.snapStat.Min(), 10)
	stmap["snapStat.max"] = strconv.FormatInt(stats.snapStat.Max(), 10)
	stmap["snapStat.avg"] = strconv.FormatInt(stats.snapStat.Mean(), 10)
	stmap["upsertCount"] = strconv.FormatUint(stats.upsertCount.Value(), 10)
	stmap["deleteCount"] = strconv.FormatUint(stats.deleteCount.Value(), 10)
	stmap["exprCount"] = strconv.FormatUint(stats.exprCount.Value(), 10)
	stmap["ainstCount"] = strconv.FormatUint(stats.ainstCount.Value(), 10)
	stmap["dinstCount"] = strconv.FormatUint(stats.dinstCount.Value(), 10)
	stmap["tsCount"] = strconv.FormatUint(stats.tsCount.Value(), 10)
	stmap["mutChLen"] = strconv.FormatUint((uint64)(len(stats.mutch)), 10)

	stmap["collectionCreate"] = strconv.FormatUint(stats.collectionCreate.Value(), 10)
	stmap["collectionDrop"] = strconv.FormatUint(stats.collectionDrop.Value(), 10)
	stmap["collectionFlush"] = strconv.FormatUint(stats.collectionFlush.Value(), 10)
	stmap["scopeCreate"] = strconv.FormatUint(stats.scopeCreate.Value(), 10)
	stmap["scopeDrop"] = strconv.FormatUint(stats.scopeDrop.Value(), 10)
	stmap["collectionChanged"] = strconv.FormatUint(stats.collectionChanged.Value(), 10)
	stmap["seqnoAdvanced"] = strconv.FormatUint(stats.seqnoAdvanced.Value(), 10)
	stmap["osoSnapshotStart"] = strconv.FormatUint(stats.osoSnapshotStart.Value(), 10)
	stmap["osoSnapshotEnd"] = strconv.FormatUint(stats.osoSnapshotEnd.Value(), 10)

	// A copy of vbseqnos is made so that numDocsProcessed can be consistent
	// with the sum of logged vbseqnos. Also, it helps to compute the numDocsPending
	// as stats.vbseqnos can move ahead of retrieved bucket seqnos (from getKVTs())
	// at the time of computation of numDocsPending
	for i, v := range stats.vbseqnos {
		stats.vbseqnos_copy[i] = v.Value()
		vbseqnos = append(vbseqnos, stats.vbseqnos_copy[i])
		numDocsProcessed += stats.vbseqnos_copy[i]
	}

	stmap["numDocsProcessed"] = strconv.FormatUint(numDocsProcessed, 10)

	cluster := stats.kvdata.config["clusterAddr"].String()
	// Get seqnos only for the vbuckets owned by the KV on this node
	seqnos, err := getKVTs(stats.kvdata.bucket, cluster, stats.kvdata.kvaddr, stats.kvdata.collectionId)

	// numDocsPending is logged only when there is no error in retrieving the bucket seqnos
	if err == nil {
		for i, v := range stats.vbseqnos_copy {
			if seqnos[i] > v { // During a KV rollback, 'seqnos[i]' can become less than 'v'
				numDocsPending += seqnos[i] - v
			}
		}
		stmap["numDocsPending"] = strconv.FormatUint(numDocsPending, 10)
	} else {
		fmsg := "KVDT[<-%v<-%v #%v] ##%x"
		key := fmt.Sprintf(fmsg, stats.kvdata.bucket, stats.kvdata.feed.cluster, stats.kvdata.topic, stats.kvdata.opaque)
		logging.Errorf("%v Unable to retrieve bucket sequence numbers, err: %v", key, err)
	}

	return stmap, vbseqnos
}

// NewKVData create a new data-path instance.
func NewKVData(
	feed *Feed,
	bucket, keyspaceId string,
	collectionId string,
	opaque uint16,
	reqTs *protobuf.TsVbuuid,
	engines map[uint64]*Engine,
	endpoints map[string]c.RouterEndpoint,
	mutch <-chan *mc.DcpEvent,
	kvaddr string,
	config c.Config,
	async bool,
	opaque2 uint64,
	collectionsAware bool,
	vbucketWorkers int) (*KVData, error) {

	kvdata := &KVData{
		feed:         feed,
		opaque:       opaque,
		topic:        feed.topic,
		bucket:       bucket,
		keyspaceId:   keyspaceId,
		collectionId: collectionId,
		config:       config,
		engines:      make(map[uint64]*Engine),
		endpoints:    make(map[string]c.RouterEndpoint),
		// 16 is enough, there can't be more than that many out-standing
		// control calls on this feed.
		sbch: make(chan []interface{}, 16),

		reqTsMutex:       &sync.RWMutex{},
		genServerStopCh:  make(chan bool),
		genServerFinCh:   make(chan bool),
		runScatterFinCh:  make(chan bool),
		runScatterDoneCh: make(chan bool),

		stopScatterFromFeedCh: make(chan bool),

		stats:   &KvdataStats{},
		kvaddr:  kvaddr,
		async:   async,
		opaque2: opaque2,
	}

	uuid, err := common.NewUUID()
	if err != nil {
		logging.Errorf("%v ##%x common.NewUUID() failed: %v", kvdata.logPrefix, kvdata.opaque, err)
		return nil, err
	}
	kvdata.uuid = uuid.Uint64()

	// TODO: Replace this with cinfo.
	numVbuckets, err := common.GetNumVBuckets(config["clusterAddr"].String(), bucket)
	if err != nil {
		logging.Errorf("%v ##%x common.GetNumVBuckets(%v) failed: %v", kvdata.logPrefix, kvdata.opaque, bucket, err)
		return nil, err
	}

	kvdata.stats.Init(numVbuckets, kvdata)
	kvdata.stats.mutch = mutch

	fmsg := "KVDT[<-%v<-%v #%v]"
	kvdata.logPrefix = fmt.Sprintf(fmsg, keyspaceId, feed.cluster, feed.topic)
	for uuid, engine := range engines {
		kvdata.engines[uuid] = engine
	}
	for raddr, endpoint := range endpoints {
		kvdata.endpoints[raddr] = endpoint
	}

	// start workers
	kvdata.workers = kvdata.spawnWorkers(feed, bucket, keyspaceId, config, opaque,
		opaque2, collectionsAware, vbucketWorkers)
	// Gather stats pointers from all workers
	kvdata.updateWorkerStats()

	kvdata.reqTs = reqTs
	go kvdata.genServer()
	go kvdata.runScatter(mutch)
	logging.Infof("%v ##%x started, uuid: %v ...\n", kvdata.logPrefix, opaque, kvdata.uuid)
	return kvdata, nil
}

// commands to server
const (
	kvCmdAddEngines byte = iota + 1
	kvCmdDelEngines
	kvCmdTs
	kvCmdGetStats
	kvCmdResetConfig
	kvCmdClose
)

// AddEngines and endpoints, synchronous call.
func (kvdata *KVData) AddEngines(
	opaque uint16,
	engines map[uint64]*Engine,
	endpoints map[string]c.RouterEndpoint) (map[uint16]uint64, error) {

	// copy them to local map and then pass down the reference.
	eps := make(map[string]c.RouterEndpoint)
	for k, v := range endpoints {
		eps[k] = v
	}

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{kvCmdAddEngines, opaque, engines, eps, respch}
	resp, err := c.FailsafeOp(kvdata.sbch, respch, cmd, kvdata.genServerStopCh)
	if err = c.OpError(err, resp, 1); err != nil {
		return nil, err
	}
	return resp[0].(map[uint16]uint64), nil
}

// DeleteEngines synchronous call.
func (kvdata *KVData) DeleteEngines(opaque uint16, engineKeys []uint64, collectionIds []uint32) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{kvCmdDelEngines, opaque, engineKeys, collectionIds, respch}
	_, err := c.FailsafeOp(kvdata.sbch, respch, cmd, kvdata.genServerStopCh)
	return err
}

// UpdateTs with new set of {vbno,seqno}, synchronous call.
func (kvdata *KVData) UpdateTs(opaque uint16, ts *protobuf.TsVbuuid) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{kvCmdTs, opaque, ts, respch}
	_, err := c.FailsafeOp(kvdata.sbch, respch, cmd, kvdata.genServerStopCh)
	return err
}

// GetStatistics from kv data path, synchronous call.
func (kvdata *KVData) GetStatistics() map[string]interface{} {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{kvCmdGetStats, respch}
	resp, _ := c.FailsafeOp(kvdata.sbch, respch, cmd, kvdata.genServerStopCh)
	return resp[0].(map[string]interface{})
}

// ResetConfig for kvdata.
func (kvdata *KVData) ResetConfig(config c.Config) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{kvCmdResetConfig, config, respch}
	_, err := c.FailsafeOp(kvdata.sbch, respch, cmd, kvdata.genServerStopCh)
	return err
}

// Close kvdata kv data path, synchronous call.
func (kvdata *KVData) Close() error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{kvCmdClose, respch}
	_, err := c.FailsafeOp(kvdata.sbch, respch, cmd, kvdata.genServerStopCh)
	return err
}

// Stop scattering the mutations to workers.
func (kvdata *KVData) StopScatter() {
	atomic.StoreUint32(&kvdata.stopScatter, 1)
}

func (kvdata *KVData) StopScatterFromFeed() {
	atomic.StoreUint32(&kvdata.stopScatter, 1)
	close(kvdata.stopScatterFromFeedCh)
}

func (kvdata *KVData) GetKVStats() map[string]interface{} {
	if kvdata.stats.IsClosed() {
		return nil
	}
	fmsg := "KVDT[<-%v<-%v #%v] ##%x"
	key := fmt.Sprintf(fmsg, kvdata.keyspaceId, kvdata.feed.cluster, kvdata.topic, kvdata.opaque)
	kvstat := make(map[string]interface{}, 0)
	kvstat[key] = kvdata.stats
	return kvstat
}

func (kvdata *KVData) GetWorkerStats() map[string][]interface{} {
	if kvdata.stats.IsClosed() {
		return nil
	}
	fmsg := "WRKR[<-%v<-%v #%v] ##%x"
	key := fmt.Sprintf(fmsg, kvdata.keyspaceId, kvdata.feed.cluster, kvdata.topic, kvdata.opaque)
	wrkrstat := make(map[string][]interface{}, 0)
	wrkrstat[key] = kvdata.wrkrStats
	return wrkrstat
}

func (kvdata *KVData) updateWorkerStats() {
	kvdata.wrkrStats = make([]interface{}, 0, len(kvdata.workers))
	for _, wrkr := range kvdata.workers {
		kvdata.wrkrStats = append(kvdata.wrkrStats, wrkr.stats)
	}
}

// go-routine handles data path.
func (kvdata *KVData) runScatter(mutch <-chan *mc.DcpEvent) {

	defer func() {
		if r := recover(); r != nil {
			fmsg := "%v ##%x runScatter() crashed: %v\n"
			logging.Errorf(fmsg, kvdata.logPrefix, kvdata.opaque, r)
			logging.Errorf("%s", logging.StackTrace())
		}

		close(kvdata.runScatterDoneCh)
		// Close genServerFinCh to terminate genServer() incase runScatter() exits first
		close(kvdata.genServerFinCh)

		logging.Infof("%v ##%x runScatter() ... stopped\n", kvdata.logPrefix, kvdata.opaque)
	}()

loop:
	for {
		select {
		case <-kvdata.stopScatterFromFeedCh:
			logging.Warnf("%v ##%x exiting runScatter as scatter is stopped from feed", kvdata.logPrefix, kvdata.opaque)
			break loop
		default:
			select {
			case m, ok := <-mutch:
				if ok == false || atomic.LoadUint32(&kvdata.stopScatter) == 1 { // upstream has closed
					break loop
				}
				kvdata.stats.eventCount.Add(1)
				seqno, err := kvdata.scatterMutation(m)
				if err != nil {
					fmsg := "%v ##%x Error during scatter mutation while posting: %v, err: %v"
					logging.Errorf(fmsg, kvdata.logPrefix, kvdata.opaque, m.Opcode, err)
					break loop
				}

				// For these two events, seqno. would be zero. Setting seqno. to "0"
				// can lead to incorrect computation of numDocsProcessed and
				// numDocsPending stats. For StreamEnd, seqno. will be zero - Since
				// projector will not be processing mutations, set seqno. to "0" for
				// the vbucket
				if m.Opcode != mcd.DCP_SNAPSHOT && m.Opcode != mcd.DCP_OSO_SNAPSHOT {
					kvdata.stats.vbseqnos[m.VBucket].Set(uint64(seqno))
				}

			// Incase genServer() terminates first, it will close runScatterFinCh to
			// terminate runScatter() go-routine
			case <-kvdata.runScatterFinCh:
				break loop
			}
		}
	}
}

func (kvdata *KVData) genServer() {

	defer func() {
		if r := recover(); r != nil {
			fmsg := "%v ##%x genServer() crashed: %v\n"
			logging.Errorf(fmsg, kvdata.logPrefix, kvdata.opaque, r)
			logging.Errorf("%s", logging.StackTrace())
		}

		if !kvdata.runScatterDone {
			kvdata.StopScatter()
			// shutdown workers
			for _, worker := range kvdata.workers {
				worker.Close()
			}
			close(kvdata.runScatterFinCh)
			<-kvdata.runScatterDoneCh
			kvdata.workers = nil
		}
		// Close genServerStopCh so that any syncronous control message waiting
		// for response will return
		close(kvdata.genServerStopCh)

		kvdata.publishStreamEnd()

		kvdata.feed.PostFinKVdata(kvdata.keyspaceId, kvdata.uuid)

		//Update closed in stats object and log the stats before exiting
		kvdata.stats.closed.Set(true)
		kvdata.logStats()
		logging.Infof("%v ##%x genServer()... stopped\n", kvdata.logPrefix, kvdata.opaque)
	}()

loop:
	for {
		select {
		case msg := <-kvdata.sbch:
			if breakloop := kvdata.handleCommand(msg); breakloop {
				break loop
			}

		// Incase runScatter() terminates first, it will close genServerFinCh
		// to terminate genServer()
		case <-kvdata.genServerFinCh:
			break loop
		}
	}
}

func (kvdata *KVData) handleCommand(msg []interface{}) bool {
	cmd := msg[0].(byte)
	switch cmd {
	case kvCmdAddEngines:
		opaque := msg[1].(uint16)
		respch := msg[4].(chan []interface{})
		if msg[2] != nil { // collect engines
			for uuid, engine := range msg[2].(map[uint64]*Engine) {
				if _, ok := kvdata.engines[uuid]; !ok {
					fmsg := "%v ##%x new engine added %v"
					logging.Infof(fmsg, kvdata.logPrefix, opaque, uuid)
				}
				kvdata.engines[uuid] = engine
			}
		}
		if msg[3] != nil { // collect endpoints
			rv := msg[3].(map[string]c.RouterEndpoint)
			for raddr, endp := range rv {
				fmsg := "%v ##%x updated endpoint %q"
				logging.Infof(fmsg, kvdata.logPrefix, opaque, raddr)
				kvdata.endpoints[raddr] = endp
			}
		}
		curSeqnos := make(map[uint16]uint64)
		if kvdata.engines != nil || kvdata.endpoints != nil {
			engns, endpts := kvdata.engines, kvdata.endpoints
			for _, worker := range kvdata.workers {
				cseqnos, err := worker.AddEngines(opaque, engns, endpts)
				if err != nil {
					panic(err)
				}
				for vbno, cseqno := range cseqnos {
					curSeqnos[vbno] = cseqno
				}
			}
		}
		kvdata.stats.ainstCount.Add(1)
		respch <- []interface{}{curSeqnos, nil}

	case kvCmdDelEngines:
		opaque := msg[1].(uint16)
		engineKeys := msg[2].([]uint64)
		collectionIds := msg[3].([]uint32)
		respch := msg[4].(chan []interface{})
		for _, worker := range kvdata.workers {
			err := worker.DeleteEngines(opaque, engineKeys, collectionIds)
			if err != nil {
				panic(err)
			}
		}
		for _, engineKey := range engineKeys {
			delete(kvdata.engines, engineKey)
			fmsg := "%v ##%x deleted engine %q"
			logging.Infof(fmsg, kvdata.logPrefix, opaque, engineKey)
		}
		kvdata.stats.dinstCount.Add(1)
		respch <- []interface{}{nil}

	case kvCmdTs:
		_ /*opaque*/ = msg[1].(uint16)
		kvdata.reqTsMutex.Lock()
		kvdata.reqTs = kvdata.reqTs.Union(msg[2].(*protobuf.TsVbuuid))
		kvdata.reqTsMutex.Unlock()
		respch := msg[3].(chan []interface{})
		kvdata.stats.tsCount.Add(1)
		respch <- []interface{}{nil}

	case kvCmdGetStats:
		respch := msg[1].(chan []interface{})
		stats := kvdata.newStats()
		stats.Set("events", float64(kvdata.stats.eventCount.Value()))
		stats.Set("addInsts", float64(kvdata.stats.ainstCount.Value()))
		stats.Set("delInsts", float64(kvdata.stats.dinstCount.Value()))
		stats.Set("tsCount", float64(kvdata.stats.tsCount.Value()))
		statVbuckets := make(map[string]interface{})
		for _, worker := range kvdata.workers {
			if stats, err := worker.GetStatistics(); err != nil {
				panic(err)
			} else {
				for vbno_s, stat := range stats {
					statVbuckets[vbno_s] = stat
				}
			}
		}
		stats.Set("vbuckets", statVbuckets)
		respch <- []interface{}{map[string]interface{}(stats)}

	case kvCmdResetConfig:
		config, respch := msg[1].(c.Config), msg[2].(chan []interface{})
		for _, worker := range kvdata.workers {
			if err := worker.ResetConfig(config); err != nil {
				panic(err)
			}
		}
		kvdata.config = kvdata.config.Override(config)
		respch <- []interface{}{nil}

	case kvCmdClose:
		// Stop scattering all mutations
		kvdata.StopScatter()
		for _, worker := range kvdata.workers {
			worker.Close()
		}
		close(kvdata.runScatterFinCh)
		<-kvdata.runScatterDoneCh
		kvdata.runScatterDone = true
		kvdata.workers = nil
		respch := msg[1].(chan []interface{})
		respch <- []interface{}{nil}
		return true
	}
	return false
}

func (kvdata *KVData) scatterMutation(m *mc.DcpEvent) (seqno uint64, err error) {

	vbno := m.VBucket
	worker := kvdata.workers[int(vbno)%len(kvdata.workers)]

	switch m.Opcode {
	case mcd.DCP_STREAMREQ:
		if m.Status == mcd.ROLLBACK {
			fmsg := "%v ##%x StreamRequest ROLLBACK: %v\n"
			arg1 := logging.TagUD(m)
			logging.Infof(fmsg, kvdata.logPrefix, m.Opaque, arg1)

			if kvdata.async {
				if err = worker.Event(m); err != nil {
					return
				}
			}

		} else if m.Status == mcd.UNKNOWN_COLLECTION || m.Status == mcd.UNKNOWN_SCOPE {
			fmsg := "%v ##%x StreamRequest %v: %v\n"
			arg1 := logging.TagUD(m)
			logging.Infof(fmsg, kvdata.logPrefix, m.Opaque, m.Status, arg1)

			if kvdata.async {
				if err = worker.Event(m); err != nil {
					return
				}
			}
		} else if m.Status != mcd.SUCCESS {
			fmsg := "%v ##%x StreamRequest %s: %v\n"
			arg1 := logging.TagUD(m)
			logging.Errorf(fmsg, kvdata.logPrefix, m.Opaque, m.Status, arg1)

			// Propagate the error to downstream so that indexer can take corrective
			// action immediately
			if err = worker.Event(m); err != nil {
				return
			}

		} else if m.VBuuid, _, err = m.FailoverLog.Latest(); err != nil {
			return

		} else {
			fmsg := "%v ##%x StreamRequest: %v\n"
			arg1 := logging.TagUD(m)
			logging.Tracef(fmsg, kvdata.logPrefix, m.Opaque, arg1)

			kvdata.reqTsMutex.RLock()
			m.Seqno, _ = kvdata.reqTs.SeqnoFor(vbno)
			kvdata.reqTsMutex.RUnlock()

			if err = worker.Event(m); err != nil {
				return
			}
			seqno = m.Seqno
		}
		kvdata.stats.reqCount.Add(1)
		kvdata.feed.PostStreamRequest(kvdata.keyspaceId, m, kvdata.uuid)

	case mcd.DCP_STREAMEND:
		if m.Status != mcd.SUCCESS {
			fmsg := "%v ##%x StreamEnd %s: %v\n"
			arg1 := logging.TagUD(m)
			logging.Errorf(fmsg, kvdata.logPrefix, m.Opaque, arg1)

		} else {
			fmsg := "%v ##%x StreamEnd: %v\n"
			arg1 := logging.TagUD(m)
			logging.Tracef(fmsg, kvdata.logPrefix, m.Opaque, arg1)
			if err = worker.Event(m); err != nil {
				return
			}
		}
		kvdata.stats.endCount.Add(1)
		kvdata.feed.PostStreamEnd(kvdata.keyspaceId, m, kvdata.uuid)

	case mcd.DCP_SNAPSHOT:
		if err = worker.Event(m); err != nil {
			return
		}
		snapwindow := int64(m.SnapendSeq - m.SnapstartSeq + 1)
		if snapwindow > 50000 {
			fmsg := "%v ##%x snapshot window is %v\n"
			logging.Warnf(fmsg, kvdata.logPrefix, m.Opaque, snapwindow)
		}
		kvdata.stats.snapStat.Add(snapwindow)

	case mcd.DCP_MUTATION, mcd.DCP_DELETION, mcd.DCP_EXPIRATION:
		seqno = m.Seqno
		if err = worker.Event(m); err != nil {
			return
		}
		switch m.Opcode {
		case mcd.DCP_MUTATION:
			kvdata.stats.upsertCount.Add(1)
		case mcd.DCP_DELETION:
			kvdata.stats.deleteCount.Add(1)
		case mcd.DCP_EXPIRATION:
			kvdata.stats.exprCount.Add(1)
		}

	case mcd.DCP_SYSTEM_EVENT: // Propagate system events to workers
		fmsg := "%v ##%x SystemEvent: %v\n"
		logging.Tracef(fmsg, kvdata.logPrefix, m.Opaque, m)
		seqno = m.Seqno
		if err = worker.Event(m); err != nil {
			return
		}
		switch m.EventType {
		case mcd.COLLECTION_CREATE:
			kvdata.stats.collectionCreate.Add(1)
		case mcd.COLLECTION_DROP:
			kvdata.stats.collectionDrop.Add(1)
		case mcd.COLLECTION_FLUSH:
			kvdata.stats.collectionFlush.Add(1)
		case mcd.SCOPE_CREATE:
			kvdata.stats.scopeCreate.Add(1)
		case mcd.SCOPE_DROP:
			kvdata.stats.scopeDrop.Add(1)
		case mcd.COLLECTION_CHANGED:
			kvdata.stats.collectionChanged.Add(1)
		}

	case mcd.DCP_SEQNO_ADVANCED: // Propagate SeqnoAdvancedEvent to workers
		fmsg := "%v ##%x SeqnoAdvanced event: %v\n"
		logging.Tracef(fmsg, kvdata.logPrefix, m.Opaque, m)
		seqno = m.Seqno
		if err = worker.Event(m); err != nil {
			return
		}
		kvdata.stats.seqnoAdvanced.Add(1)

	case mcd.DCP_OSO_SNAPSHOT: // Propagate OsoSnapshotEvent to workers

		fmsg := "%v ##%x Received OSO Snapshot event: %v for vbucket: %v\n"
		logging.Infof(fmsg, kvdata.logPrefix, m.Opaque, m.EventType, vbno)
		if err = worker.Event(m); err != nil {
			return
		}
		switch m.EventType {
		case mcd.OSO_SNAPSHOT_START:
			kvdata.stats.osoSnapshotStart.Add(1)
		case mcd.OSO_SNAPSHOT_END:
			kvdata.stats.osoSnapshotEnd.Add(1)
		}
	}
	return
}

func (kvdata *KVData) spawnWorkers(
	feed *Feed, bucket, keyspaceId string, config c.Config,
	opaque uint16, opaque2 uint64, collectionsAware bool,
	vbucketWorkers int) []*VbucketWorker {

	var nworkers int

	//use config if vbucketWorkers is not specified
	if vbucketWorkers > 0 {
		nworkers = vbucketWorkers
	} else {
		nworkers = config["vbucketWorkers"].Int()
	}

	workers := make([]*VbucketWorker, nworkers)
	for i := 0; i < nworkers; i++ {
		workers[i] = NewVbucketWorker(i, feed, bucket,
			keyspaceId, opaque, config, opaque2, collectionsAware)
	}
	return workers
}

func (kvdata *KVData) publishStreamEnd() {
	for _, worker := range kvdata.workers {
		vbuckets, err := worker.GetVbuckets()
		if err != nil {
			fmsg := "Error in worker.GetVbuckets(): %v"
			logging.Errorf(fmsg, kvdata.logPrefix, err)
		}
		for _, v := range vbuckets {
			m := &mc.DcpEvent{
				Opcode:  mcd.DCP_STREAMEND,
				Status:  mcd.SUCCESS,
				VBucket: v.vbno,
				Opaque:  v.opaque,
			}
			kvdata.feed.PostStreamEnd(kvdata.keyspaceId, m, kvdata.uuid)
		}
	}
}

func (kvdata *KVData) logStats() {
	stats, vbseqnos := kvdata.stats.String()
	fmsg := "KVDT[<-%v<-%v #%v] ##%x"
	key := fmt.Sprintf(fmsg, kvdata.keyspaceId, kvdata.feed.cluster, kvdata.topic, kvdata.opaque)
	logging.Infof("%v stats: %v", key, stats)
	logging.Infof("%v vbseqnos: [%v]", key, vbseqnos)
}

func (kvdata *KVData) newStats() c.Statistics {
	statVbuckets := make(map[string]interface{})
	m := map[string]interface{}{
		"events":   float64(0),   // no. of mutations events received
		"addInsts": float64(0),   // no. of addInstances received
		"delInsts": float64(0),   // no. of delInsts received
		"tsCount":  float64(0),   // no. of updateTs received
		"vbuckets": statVbuckets, // per vbucket statistics
	}
	stats, _ := c.NewStatistics(m)
	return stats
}

// This method will not block for more than 5 seconds. As stats_manager
// logger thread calls this routine periodically, it is important that
// this routine does not block forever.
// If there is any connection issue with memcached, the go-routine spawned
// to get the bucket seqnos will eventually return as the connection to
// memcached times-out
func getKVTs(bucket, cluster, kvaddr, cid string) ([]uint64, error) {
	respch := make(chan []interface{})

	go func() {
		// Retrieves bucketSeqnos if cid is "". Otherwise, retrieves
		// corresponding collection seqnos
		seqnos, err := SeqnosLocal(cluster, "default", bucket, cid, kvaddr)
		respch <- []interface{}{seqnos, err}
	}()

	select {
	case <-time.After(time.Duration(5 * time.Second)):
		go func() {
			//read the response to ensure termination
			<-respch
		}()
		return nil, errors.New("Timeout in retrieving seqnos")
	case resp := <-respch:
		if resp[1] != nil {
			return nil, resp[1].(error)
		} else {
			switch (resp[0]).(type) {
			case []uint64:
				return resp[0].([]uint64), nil
			default:
				return nil, errors.New("Unexpected type returned while retrieving bucket seqnos")
			}
		}
	}
}
