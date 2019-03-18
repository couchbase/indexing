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

import "fmt"
import "time"
import "strconv"
import "strings"

import "github.com/couchbase/indexing/secondary/logging"
import "github.com/couchbase/indexing/secondary/stats"
import mcd "github.com/couchbase/indexing/secondary/dcp/transport"
import mc "github.com/couchbase/indexing/secondary/dcp/transport/client"
import c "github.com/couchbase/indexing/secondary/common"
import protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"

// KVData captures an instance of data-path for single kv-node
// from upstream connection.
type KVData struct {
	feed    *Feed
	topic   string // immutable
	bucket  string // immutable
	opaque  uint16
	workers []*VbucketWorker
	config  c.Config
	// evaluators and subscribers
	engines   map[uint64]*Engine
	endpoints map[string]c.RouterEndpoint
	// server channels
	sbch  chan []interface{}
	finch chan bool
	// misc.
	syncTimeout time.Duration // in milliseconds
	kvstatTick  time.Duration // in milliseconds
	logPrefix   string
	// statistics
	stats *KvdataStats
}

type KvdataStats struct {
	hbCount     stats.Uint64Val
	eventCount  stats.Uint64Val
	reqCount    stats.Uint64Val
	endCount    stats.Uint64Val
	snapStat    stats.Average
	upsertCount stats.Uint64Val
	deleteCount stats.Uint64Val
	exprCount   stats.Uint64Val
	ainstCount  stats.Uint64Val
	dinstCount  stats.Uint64Val
	tsCount     stats.Uint64Val
	mutchLen    stats.Uint64Val
	vbseqnos    []stats.Uint64Val
}

func (kvstats *KvdataStats) Init(numVbuckets int) {
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
	kvstats.mutchLen.Init()
	kvstats.vbseqnos = make([]stats.Uint64Val, numVbuckets)
	for i, _ := range kvstats.vbseqnos {
		kvstats.vbseqnos[i].Init()
	}
}

// NewKVData create a new data-path instance.
func NewKVData(
	feed *Feed, bucket string,
	opaque uint16,
	reqTs *protobuf.TsVbuuid,
	engines map[uint64]*Engine,
	endpoints map[string]c.RouterEndpoint,
	mutch <-chan *mc.DcpEvent,
	config c.Config) *KVData {

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
		sbch:  make(chan []interface{}, 16),
		finch: make(chan bool),
		stats: &KvdataStats{},
	}
	numVbuckets := config["maxVbuckets"].Int()
	kvdata.stats.Init(numVbuckets)
	fmsg := "KVDT[<-%v<-%v #%v]"
	kvdata.logPrefix = fmt.Sprintf(fmsg, bucket, feed.cluster, feed.topic)
	kvdata.syncTimeout = time.Duration(config["syncTimeout"].Int())
	kvdata.syncTimeout *= time.Millisecond
	kvdata.kvstatTick = time.Duration(config["kvstatTick"].Int())
	kvdata.kvstatTick *= time.Millisecond
	for uuid, engine := range engines {
		kvdata.engines[uuid] = engine
	}
	for raddr, endpoint := range endpoints {
		kvdata.endpoints[raddr] = endpoint
	}
	// start workers
	kvdata.workers = kvdata.spawnWorkers(feed, bucket, config, opaque)
	go kvdata.runScatter(reqTs, mutch)
	logging.Infof("%v ##%x started ...\n", kvdata.logPrefix, opaque)
	return kvdata
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
		kvdata.feed.PostFinKVdata(kvdata.bucket)
		close(kvdata.finch)
		logging.Infof("%v ##%x ... stopped\n", kvdata.logPrefix, kvdata.opaque)
	}()

	vbseqnos := make([]uint64, 1024)

	// stats
	statSince := time.Now()
	var stitems [16]string
	logstats := func() {
		stitems[0] = `"topic":"` + kvdata.topic + `"`
		stitems[1] = `"bucket":"` + kvdata.bucket + `"`
		stitems[2] = `"hbCount":` + strconv.Itoa(int(kvdata.stats.hbCount.Value()))
		stitems[3] = `"eventCount":` + strconv.Itoa(int(kvdata.stats.eventCount.Value()))
		stitems[4] = `"reqCount":` + strconv.Itoa(int(kvdata.stats.reqCount.Value()))
		stitems[5] = `"endCount":` + strconv.Itoa(int(kvdata.stats.endCount.Value()))
		stitems[6] = `"snapStat.samples":` + strconv.Itoa(int(kvdata.stats.snapStat.Count()))
		stitems[7] = `"snapStat.min":` + strconv.Itoa(int(kvdata.stats.snapStat.Min()))
		stitems[8] = `"snapStat.max":` + strconv.Itoa(int(kvdata.stats.snapStat.Max()))
		stitems[9] = `"snapStat.avg":` + strconv.Itoa(int(kvdata.stats.snapStat.Mean()))
		stitems[10] = `"upsertCount":` + strconv.Itoa(int(kvdata.stats.upsertCount.Value()))
		stitems[11] = `"deleteCount":` + strconv.Itoa(int(kvdata.stats.deleteCount.Value()))
		stitems[12] = `"exprCount":` + strconv.Itoa(int(kvdata.stats.exprCount.Value()))
		stitems[13] = `"ainstCount":` + strconv.Itoa(int(kvdata.stats.ainstCount.Value()))
		stitems[14] = `"dinstCount":` + strconv.Itoa(int(kvdata.stats.dinstCount.Value()))
		stitems[15] = `"tsCount":` + strconv.Itoa(int(kvdata.stats.tsCount.Value()))
		statjson := strings.Join(stitems[:], ",")
		fmsg := "%v ##%x stats {%v}\n"
		logging.Infof(fmsg, kvdata.logPrefix, kvdata.opaque, statjson)
		fmsg = "%v ##%x vbseqnos %v\n"
		logging.Infof(fmsg, kvdata.logPrefix, kvdata.opaque, vbseqnos)
	}

	heartBeat := time.After(kvdata.syncTimeout)
	fmsg := "%v ##%x heartbeat (%v) loaded ...\n"
	logging.Infof(fmsg, kvdata.logPrefix, kvdata.opaque, kvdata.syncTimeout)

loop:
	for {
		select {
		case m, ok := <-mutch:
			if ok == false { // upstream has closed
				break loop
			}
			kvdata.stats.eventCount.Add(1)
			kvdata.stats.mutchLen.Set(uint64(len(mutch)))
			seqno, _ := kvdata.scatterMutation(m, ts)
			kvdata.stats.vbseqnos[m.VBucket].Set(uint64(seqno))

		case <-heartBeat:
			heartBeat = nil
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

			// log stats ?
			if time.Since(statSince) > kvdata.kvstatTick {
				logstats()
				statSince = time.Now()
			}

		case msg := <-kvdata.sbch:
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
				if cv, ok := config["syncTimeout"]; ok && heartBeat != nil {
					kvdata.syncTimeout = time.Duration(cv.Int())
					kvdata.syncTimeout *= time.Millisecond
					logging.Infof(
						"%v ##%x heart-beat settings reloaded: %v\n",
						kvdata.logPrefix, kvdata.opaque, kvdata.syncTimeout)
					heartBeat = time.After(kvdata.syncTimeout)
				}
				if cv, ok := config["kvstatTick"]; ok {
					kvdata.kvstatTick = time.Duration(cv.Int())
					kvdata.kvstatTick *= time.Millisecond
					logging.Infof(
						"%v ##%x kvstat-tick settings reloaded: %v\n",
						kvdata.logPrefix, kvdata.opaque, kvdata.kvstatTick)
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
				heartBeat = time.After(kvdata.syncTimeout)
				respch <- []interface{}{nil}

			case kvCmdClose:
				for _, worker := range kvdata.workers {
					worker.Close()
				}
				kvdata.workers = nil
				respch := msg[1].(chan []interface{})
				respch <- []interface{}{nil}
				break loop
			}
		}
	}
	logstats()
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
		kvdata.feed.PostStreamRequest(kvdata.bucket, m)

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
		kvdata.feed.PostStreamEnd(kvdata.bucket, m)

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
	feed *Feed,
	bucket string, config c.Config, opaque uint16) []*VbucketWorker {

	nworkers := config["vbucketWorkers"].Int()
	workers := make([]*VbucketWorker, nworkers)
	for i := 0; i < nworkers; i++ {
		workers[i] = NewVbucketWorker(i, feed, bucket, opaque, config)
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
			kvdata.feed.PostStreamEnd(kvdata.bucket, m)
		}
	}
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
