// data-path concurrency model:
//
//               back-channel
//     feed <---------------------*   NewKVData()
//                StreamRequest   |     |            *---> worker
//                    StreamEnd   |   (spawn)        |
//                                |     |            *---> worker
//                                |     |            |
//        AddEngines() --*-----> runScatter ---------*---> worker
//                       |
//     DeleteEngines() --*
//                       |
//     GetStatistics() --*
//                       |
//             Close() --*

package projector

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
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
	feed    *Feed
	topic   string // immutable
	bucket  string // immutable
	opaque  uint16
	workers []*VbucketWorker
	config  c.Config
	async   bool

	// evaluators and subscribers
	engines   map[uint64]*Engine
	endpoints map[string]c.RouterEndpoint
	// server channels
	sbch  chan []interface{}
	finch chan bool
	// misc.
	syncTimeout time.Duration // in milliseconds
	logPrefix   string
	// statistics
	stats     *KvdataStats
	wrkrStats []interface{}
	heartBeat <-chan time.Time
	uuid      uint64 // immutable
	kvaddr    string
	opaque2   uint64 //client opaque

	stopScatter           uint32
	stopScatterFromFeedCh chan bool
}

type KvdataStats struct {
	closed        stats.BoolVal
	hbCount       stats.Uint64Val
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
}

func (kvstats *KvdataStats) Init(numVbuckets int, kvdata *KVData) {
	kvstats.closed.Init()
	kvstats.hbCount.Init()
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
}

func (kvstats *KvdataStats) IsClosed() bool {
	return kvstats.closed.Value()
}

func (stats *KvdataStats) String() (string, string) {
	var stitems [16]string
	var vbseqnos string
	var numDocsProcessed, numDocsPending uint64

	stitems[0] = `"hbCount":` + strconv.FormatUint(stats.hbCount.Value(), 10)
	stitems[1] = `"eventCount":` + strconv.FormatUint(stats.eventCount.Value(), 10)
	stitems[2] = `"reqCount":` + strconv.FormatUint(stats.reqCount.Value(), 10)
	stitems[3] = `"endCount":` + strconv.FormatUint(stats.endCount.Value(), 10)
	stitems[4] = `"snapStat.samples":` + strconv.FormatInt(stats.snapStat.Count(), 10)
	stitems[5] = `"snapStat.min":` + strconv.FormatInt(stats.snapStat.Min(), 10)
	stitems[6] = `"snapStat.max":` + strconv.FormatInt(stats.snapStat.Max(), 10)
	stitems[7] = `"snapStat.avg":` + strconv.FormatInt(stats.snapStat.Mean(), 10)
	stitems[8] = `"upsertCount":` + strconv.FormatUint(stats.upsertCount.Value(), 10)
	stitems[9] = `"deleteCount":` + strconv.FormatUint(stats.deleteCount.Value(), 10)
	stitems[10] = `"exprCount":` + strconv.FormatUint(stats.exprCount.Value(), 10)
	stitems[11] = `"ainstCount":` + strconv.FormatUint(stats.ainstCount.Value(), 10)
	stitems[12] = `"dinstCount":` + strconv.FormatUint(stats.dinstCount.Value(), 10)
	stitems[13] = `"tsCount":` + strconv.FormatUint(stats.tsCount.Value(), 10)
	stitems[14] = `"mutChLen":` + strconv.FormatUint((uint64)(len(stats.mutch)), 10)

	// A copy of vbseqnos is made so that numDocsProcessed can be consistent
	// with the sum of logged vbseqnos. Also, it helps to compute the numDocsPending
	// as stats.vbseqnos can move ahead of retrieved bucket seqnos (from getKVTs())
	// at the time of computation of numDocsPending
	for i, v := range stats.vbseqnos {
		stats.vbseqnos_copy[i] = v.Value()
		vbseqnos += fmt.Sprintf("%v ", stats.vbseqnos_copy[i])
		numDocsProcessed += stats.vbseqnos_copy[i]
	}

	stitems[15] = `"numDocsProcessed":` + strconv.FormatUint(numDocsProcessed, 10)
	statjson := strings.Join(stitems[:], ",")

	cluster := stats.kvdata.config["clusterAddr"].String()
	// Get seqnos only for the vbuckets owned by the KV on this node
	seqnos, err := getKVTs(stats.kvdata.bucket, cluster, stats.kvdata.kvaddr)

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

// NewKVData create a new data-path instance.
func NewKVData(
	feed *Feed, bucket string,
	opaque uint16,
	reqTs *protobuf.TsVbuuid,
	engines map[uint64]*Engine,
	endpoints map[string]c.RouterEndpoint,
	mutch <-chan *mc.DcpEvent,
	kvaddr string,
	config c.Config,
	async bool,
	opaque2 uint64) (*KVData, error) {

	kvdata := &KVData{
		feed:      feed,
		opaque:    opaque,
		topic:     feed.topic,
		bucket:    bucket,
		config:    config,
		engines:   make(map[uint64]*Engine),
		endpoints: make(map[string]c.RouterEndpoint),
		// 16 is enough, there can't be more than that many out-standing
		// control calls on this feed.
		sbch:                  make(chan []interface{}, 16),
		finch:                 make(chan bool),
		stopScatterFromFeedCh: make(chan bool),
		stats:                 &KvdataStats{},
		kvaddr:                kvaddr,
		async:                 async,
		opaque2:               opaque2,
	}

	uuid, err := common.NewUUID()
	if err != nil {
		logging.Errorf("%v ##%x common.NewUUID() failed: %v", kvdata.logPrefix, kvdata.opaque, err)
		return nil, err
	}
	kvdata.uuid = uuid.Uint64()

	numVbuckets := config["maxVbuckets"].Int()

	kvdata.stats.Init(numVbuckets, kvdata)
	kvdata.stats.mutch = mutch

	fmsg := "KVDT[<-%v<-%v #%v]"
	kvdata.logPrefix = fmt.Sprintf(fmsg, bucket, feed.cluster, feed.topic)
	kvdata.syncTimeout = time.Duration(config["syncTimeout"].Int())
	kvdata.syncTimeout *= time.Millisecond
	for uuid, engine := range engines {
		kvdata.engines[uuid] = engine
	}
	for raddr, endpoint := range endpoints {
		kvdata.endpoints[raddr] = endpoint
	}

	// start workers
	kvdata.workers = kvdata.spawnWorkers(feed, bucket, config, opaque, opaque2)
	// Gather stats pointers from all workers
	kvdata.updateWorkerStats()

	go kvdata.runScatter(reqTs, mutch)
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
	kvCmdReloadHeartBeat
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
	resp, err := c.FailsafeOp(kvdata.sbch, respch, cmd, kvdata.finch)
	if err = c.OpError(err, resp, 1); err != nil {
		return nil, err
	}
	return resp[0].(map[uint16]uint64), nil
}

// DeleteEngines synchronous call.
func (kvdata *KVData) DeleteEngines(opaque uint16, engineKeys []uint64) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{kvCmdDelEngines, opaque, engineKeys, respch}
	_, err := c.FailsafeOp(kvdata.sbch, respch, cmd, kvdata.finch)
	return err
}

// UpdateTs with new set of {vbno,seqno}, synchronous call.
func (kvdata *KVData) UpdateTs(opaque uint16, ts *protobuf.TsVbuuid) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{kvCmdTs, opaque, ts, respch}
	_, err := c.FailsafeOp(kvdata.sbch, respch, cmd, kvdata.finch)
	return err
}

// GetStatistics from kv data path, synchronous call.
func (kvdata *KVData) GetStatistics() map[string]interface{} {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{kvCmdGetStats, respch}
	resp, _ := c.FailsafeOp(kvdata.sbch, respch, cmd, kvdata.finch)
	return resp[0].(map[string]interface{})
}

// ResetConfig for kvdata.
func (kvdata *KVData) ResetConfig(config c.Config) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{kvCmdResetConfig, config, respch}
	_, err := c.FailsafeOp(kvdata.sbch, respch, cmd, kvdata.finch)
	return err
}

// ReloadHeartbeat for kvdata.
func (kvdata *KVData) ReloadHeartbeat() error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{kvCmdReloadHeartBeat, respch}
	_, err := c.FailsafeOp(kvdata.sbch, respch, cmd, kvdata.finch)
	return err
}

// Close kvdata kv data path, synchronous call.
func (kvdata *KVData) Close() error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{kvCmdClose, respch}
	_, err := c.FailsafeOp(kvdata.sbch, respch, cmd, kvdata.finch)
	return err
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
	key := fmt.Sprintf(fmsg, kvdata.bucket, kvdata.feed.cluster, kvdata.topic, kvdata.opaque)
	kvstat := make(map[string]interface{}, 0)
	kvstat[key] = kvdata.stats
	return kvstat
}

func (kvdata *KVData) GetWorkerStats() map[string][]interface{} {
	if kvdata.stats.IsClosed() {
		return nil
	}
	fmsg := "WRKR[<-%v<-%v #%v] ##%x"
	key := fmt.Sprintf(fmsg, kvdata.bucket, kvdata.feed.cluster, kvdata.topic, kvdata.opaque)
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
func (kvdata *KVData) runScatter(
	ts *protobuf.TsVbuuid, mutch <-chan *mc.DcpEvent) {

	defer func() {
		if r := recover(); r != nil {
			fmsg := "%v ##%x runScatter() crashed: %v\n"
			logging.Errorf(fmsg, kvdata.logPrefix, kvdata.opaque, r)
			logging.Errorf("%s", logging.StackTrace())
		}
		kvdata.publishStreamEnd()
		// shutdown workers
		for _, worker := range kvdata.workers {
			worker.Close()
		}
		kvdata.workers = nil
		kvdata.feed.PostFinKVdata(kvdata.bucket, kvdata.uuid)
		close(kvdata.finch)
		//Update closed in stats object and log the stats before exiting
		kvdata.stats.closed.Set(true)
		kvdata.logStats()
		logging.Infof("%v ##%x ... stopped\n", kvdata.logPrefix, kvdata.opaque)
	}()

	kvdata.heartBeat = time.After(kvdata.syncTimeout)
	fmsg := "%v ##%x heartbeat (%v) loaded ...\n"
	logging.Infof(fmsg, kvdata.logPrefix, kvdata.opaque, kvdata.syncTimeout)

loop:
	for {
		// Prioritize control channel over other channels
		select {
		case msg := <-kvdata.sbch:
			if breakloop := kvdata.handleCommand(msg, ts); breakloop {
				break loop
			}
		case <-kvdata.stopScatterFromFeedCh:
			break loop
		default:
		}

		select {
		case m, ok := <-mutch:
			if ok == false || atomic.LoadUint32(&kvdata.stopScatter) == 1 { // upstream has closed
				break loop
			}
			kvdata.stats.eventCount.Add(1)
			seqno, _ := kvdata.scatterMutation(m, ts)
			kvdata.stats.vbseqnos[m.VBucket].Set(uint64(seqno))

		case <-kvdata.heartBeat:
			kvdata.heartBeat = nil
			kvdata.stats.hbCount.Add(1)

			// propogate the sync-pulse via separate routine so that
			// the data-path is not blocked.
			go func() {
				// during cleanup, as long as the vbucket-routines are
				// shutdown this routine will eventually exit.
				for _, worker := range kvdata.workers {
					worker.SyncPulse()
				}
				if err := kvdata.ReloadHeartbeat(); err != nil {
					fmsg := "%v ##%x ReloadHeartbeat(): %v\n"
					logging.Errorf(fmsg, kvdata.logPrefix, kvdata.opaque, err)
				}
			}()

		case msg := <-kvdata.sbch:
			if breakloop := kvdata.handleCommand(msg, ts); breakloop {
				break loop
			}

		case <-kvdata.stopScatterFromFeedCh:
			break loop
		}
	}
}

func (kvdata *KVData) handleCommand(msg []interface{}, ts *protobuf.TsVbuuid) bool {
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
		respch := msg[3].(chan []interface{})
		for _, worker := range kvdata.workers {
			err := worker.DeleteEngines(opaque, engineKeys)
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
		ts = ts.Union(msg[2].(*protobuf.TsVbuuid))
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
		if cv, ok := config["syncTimeout"]; ok {
			kvdata.syncTimeout = time.Duration(cv.Int())
			kvdata.syncTimeout *= time.Millisecond
			logging.Infof(
				"%v ##%x heart-beat settings reloaded: %v\n",
				kvdata.logPrefix, kvdata.opaque, kvdata.syncTimeout)
		}
		if kvdata.heartBeat != nil {
			kvdata.heartBeat = time.After(kvdata.syncTimeout)
		}
		for _, worker := range kvdata.workers {
			if err := worker.ResetConfig(config); err != nil {
				panic(err)
			}
		}
		kvdata.config = kvdata.config.Override(config)
		respch <- []interface{}{nil}

	case kvCmdReloadHeartBeat:
		respch := msg[1].(chan []interface{})
		kvdata.heartBeat = time.After(kvdata.syncTimeout)
		respch <- []interface{}{nil}

	case kvCmdClose:
		for _, worker := range kvdata.workers {
			worker.Close()
		}
		kvdata.workers = nil
		respch := msg[1].(chan []interface{})
		respch <- []interface{}{nil}
		return true
	}
	return false
}

func (kvdata *KVData) scatterMutation(
	m *mc.DcpEvent, ts *protobuf.TsVbuuid) (seqno uint64, err error) {

	vbno := m.VBucket
	worker := kvdata.workers[int(vbno)%len(kvdata.workers)]

	switch m.Opcode {
	case mcd.DCP_STREAMREQ:
		if m.Status == mcd.ROLLBACK {
			fmsg := "%v ##%x StreamRequest ROLLBACK: %v\n"
			arg1 := logging.TagUD(m)
			logging.Infof(fmsg, kvdata.logPrefix, m.Opaque, arg1)

			if kvdata.async {
				if err := worker.Event(m); err != nil {
					panic(err)
				}
			}

		} else if m.Status != mcd.SUCCESS {
			fmsg := "%v ##%x StreamRequest %s: %v\n"
			arg1 := logging.TagUD(m)
			logging.Errorf(fmsg, kvdata.logPrefix, m.Opaque, m.Status, arg1)

		} else if m.VBuuid, _, err = m.FailoverLog.Latest(); err != nil {
			panic(err)

		} else {
			fmsg := "%v ##%x StreamRequest: %v\n"
			arg1 := logging.TagUD(m)
			logging.Tracef(fmsg, kvdata.logPrefix, m.Opaque, arg1)
			m.Seqno, _ = ts.SeqnoFor(vbno)
			if err := worker.Event(m); err != nil {
				panic(err)
			}
			seqno = m.Seqno
		}
		kvdata.stats.reqCount.Add(1)
		kvdata.feed.PostStreamRequest(kvdata.bucket, m, kvdata.uuid)

	case mcd.DCP_STREAMEND:
		if m.Status != mcd.SUCCESS {
			fmsg := "%v ##%x StreamEnd %s: %v\n"
			arg1 := logging.TagUD(m)
			logging.Errorf(fmsg, kvdata.logPrefix, m.Opaque, arg1)

		} else {
			fmsg := "%v ##%x StreamEnd: %v\n"
			arg1 := logging.TagUD(m)
			logging.Tracef(fmsg, kvdata.logPrefix, m.Opaque, arg1)
			if err := worker.Event(m); err != nil {
				panic(err)
			}
		}
		kvdata.stats.endCount.Add(1)
		kvdata.feed.PostStreamEnd(kvdata.bucket, m, kvdata.uuid)

	case mcd.DCP_SNAPSHOT:
		if worker.Event(m) != nil {
			panic(err)
		}
		snapwindow := int64(m.SnapendSeq - m.SnapstartSeq + 1)
		if snapwindow > 50000 {
			fmsg := "%v ##%x snapshot window is %v\n"
			logging.Warnf(fmsg, kvdata.logPrefix, m.Opaque, snapwindow)
		}
		kvdata.stats.snapStat.Add(snapwindow)

	case mcd.DCP_MUTATION, mcd.DCP_DELETION, mcd.DCP_EXPIRATION:
		seqno = m.Seqno
		if err := worker.Event(m); err != nil {
			panic(err)
		}
		switch m.Opcode {
		case mcd.DCP_MUTATION:
			kvdata.stats.upsertCount.Add(1)
		case mcd.DCP_DELETION:
			kvdata.stats.deleteCount.Add(1)
		case mcd.DCP_EXPIRATION:
			kvdata.stats.exprCount.Add(1)
		}
	}
	return
}

func (kvdata *KVData) spawnWorkers(
	feed *Feed, bucket string, config c.Config,
	opaque uint16, opaque2 uint64) []*VbucketWorker {

	nworkers := config["vbucketWorkers"].Int()
	workers := make([]*VbucketWorker, nworkers)
	for i := 0; i < nworkers; i++ {
		workers[i] = NewVbucketWorker(i, feed, bucket, opaque, config, opaque2)
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
			kvdata.feed.PostStreamEnd(kvdata.bucket, m, kvdata.uuid)
		}
	}
}

func (kvdata *KVData) logStats() {
	stats, vbseqnos := kvdata.stats.String()
	fmsg := "KVDT[<-%v<-%v #%v] ##%x"
	key := fmt.Sprintf(fmsg, kvdata.bucket, kvdata.feed.cluster, kvdata.topic, kvdata.opaque)
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
func getKVTs(bucket, cluster, kvaddr string) ([]uint64, error) {
	respch := make(chan []interface{})

	go func() {
		seqnos, err := BucketSeqnosLocal(cluster, "default", bucket, kvaddr)
		respch <- []interface{}{seqnos, err}
	}()

	select {
	case <-time.After(time.Duration(5 * time.Second)):
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
