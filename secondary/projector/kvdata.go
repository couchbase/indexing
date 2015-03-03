// data-path concurrency model:
//
//               back-channel
//     feed <---------------------*   NewKVData()
//                StreamRequest   |     |            *---> vbucket
//                    StreamEnd   |   (spawn)        |
//                                |     |            *---> vbucket
//                                |     |            |
//        AddEngines() --*-----> runScatter ---------*---> vbucket
//                       |
//     DeleteEngines() --*
//                       |
//     GetStatistics() --*
//                       |
//             Close() --*

package projector

import "fmt"
import "strconv"

import "github.com/couchbase/indexing/secondary/logging"
import mcd "github.com/couchbase/indexing/secondary/dcp/transport"
import mc "github.com/couchbase/indexing/secondary/dcp/transport/client"
import c "github.com/couchbase/indexing/secondary/common"
import protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"

// KVData captures an instance of data-path for single kv-node
// from upstream connection.
type KVData struct {
	feed   *Feed
	topic  string // immutable
	bucket string // immutable
	opaque uint16
	vrs    map[uint16]*VbucketRoutine
	config c.Config
	// evaluators and subscribers
	engines   map[uint64]*Engine
	endpoints map[string]c.RouterEndpoint
	// server channels
	sbch  chan []interface{}
	finch chan bool
	// misc.
	logPrefix string
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
		topic:     feed.topic,
		bucket:    bucket,
		vrs:       make(map[uint16]*VbucketRoutine),
		config:    config,
		engines:   make(map[uint64]*Engine),
		endpoints: make(map[string]c.RouterEndpoint),
		// 16 is enough, there can't be more than that many out-standing
		// control calls on this feed.
		sbch:      make(chan []interface{}, 16),
		finch:     make(chan bool),
		logPrefix: fmt.Sprintf("KVDT[<-%v<-%v #%v]", bucket, feed.cluster, feed.topic),
	}
	for uuid, engine := range engines {
		kvdata.engines[uuid] = engine
	}
	for raddr, endpoint := range endpoints {
		kvdata.endpoints[raddr] = endpoint
	}
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
	kvCmdSetConfig
	kvCmdClose
)

// AddEngines and endpoints, synchronous call.
func (kvdata *KVData) AddEngines(
	opaque uint16,
	engines map[uint64]*Engine,
	endpoints map[string]c.RouterEndpoint) error {

	// copy them to local map and then pass down the reference.
	eps := make(map[string]c.RouterEndpoint)
	for k, v := range endpoints {
		eps[k] = v
	}

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{kvCmdAddEngines, opaque, engines, eps, respch}
	_, err := c.FailsafeOp(kvdata.sbch, respch, cmd, kvdata.finch)
	return err
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

// SetConfig for kvdata.
func (kvdata *KVData) SetConfig(config c.Config) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{kvCmdSetConfig, config, respch}
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

	// NOTE: panic will bubble up from vbucket-routine to kvdata.
	defer func() {
		if r := recover(); r != nil {
			fmsg := "%v ##%x runScatter() crashed: %v\n"
			logging.Errorf(fmsg, kvdata.logPrefix, kvdata.opaque, r)
			logging.Errorf("%s", logging.StackTrace())
		}
		kvdata.publishStreamEnd()
		kvdata.feed.PostFinKVdata(kvdata.bucket)
		close(kvdata.finch)
		logging.Infof("%v ##%x ... stopped\n", kvdata.logPrefix, kvdata.opaque)
	}()

	// stats
	eventCount, addCount, delCount := int64(0), int64(0), int64(0)
	tsCount := int64(0)

loop:
	for {
		select {
		case m, ok := <-mutch:
			if ok == false { // upstream has closed
				break loop
			}
			kvdata.scatterMutation(m, ts)
			eventCount++

			// all vbuckets have ended for this stream, exit kvdata.
			// FIXME : For now don't cleanup the bucket because of this.
			//if len(kvdata.vrs) == 0 {
			//    break loop
			//}

		case msg := <-kvdata.sbch:
			cmd := msg[0].(byte)
			switch cmd {
			case kvCmdAddEngines:
				opaque := msg[1].(uint16)
				respch := msg[4].(chan []interface{})
				if msg[2] != nil {
					for uuid, engine := range msg[2].(map[uint64]*Engine) {
						if _, ok := kvdata.engines[uuid]; !ok {
							fmsg := "%v ##%x new engine added %v"
							logging.Infof(fmsg, kvdata.logPrefix, opaque, uuid)
						}
						kvdata.engines[uuid] = engine
					}
				}
				if msg[3] != nil {
					rv := msg[3].(map[string]c.RouterEndpoint)
					for raddr, endp := range rv {
						fmsg := "%v ##%x updated endpoint %q"
						logging.Infof(fmsg, kvdata.logPrefix, opaque, raddr)
						kvdata.endpoints[raddr] = endp
					}
				}
				if kvdata.engines != nil || kvdata.endpoints != nil {
					engines, endpoints := kvdata.engines, kvdata.endpoints
					for _, vr := range kvdata.vrs {
						err := vr.AddEngines(opaque, engines, endpoints)
						if err != nil {
							panic(err)
						}
					}
				}
				addCount++
				respch <- []interface{}{nil}

			case kvCmdDelEngines:
				opaque := msg[1].(uint16)
				engineKeys := msg[2].([]uint64)
				respch := msg[3].(chan []interface{})
				for _, vr := range kvdata.vrs {
					if err := vr.DeleteEngines(opaque, engineKeys); err != nil {
						panic(err)
					}
				}
				for _, engineKey := range engineKeys {
					delete(kvdata.engines, engineKey)
					fmsg := "%v ##%x deleted engine %q"
					logging.Infof(fmsg, kvdata.logPrefix, opaque, engineKey)
				}
				delCount++
				respch <- []interface{}{nil}

			case kvCmdTs:
				_ /*opaque*/ = msg[1].(uint16)
				ts = ts.Union(msg[2].(*protobuf.TsVbuuid))
				respch := msg[3].(chan []interface{})
				tsCount++
				respch <- []interface{}{nil}

			case kvCmdGetStats:
				respch := msg[1].(chan []interface{})
				stats := kvdata.newStats()
				stats.Set("events", float64(eventCount))
				stats.Set("addInsts", float64(addCount))
				stats.Set("delInsts", float64(delCount))
				stats.Set("tsCount", float64(tsCount))
				statVbuckets := make(map[string]interface{})
				for i, vr := range kvdata.vrs {
					stats, err := vr.GetStatistics()
					if err != nil {
						panic(err)
					}
					statVbuckets[strconv.Itoa(int(i))] = stats
				}
				stats.Set("vbuckets", statVbuckets)
				respch <- []interface{}{map[string]interface{}(stats)}

			case kvCmdSetConfig:
				config, respch := msg[1].(c.Config), msg[2].(chan []interface{})
				kvdata.config = config
				for _, vr := range kvdata.vrs {
					if err := vr.SetConfig(config); err != nil {
						panic(err)
					}
				}
				respch <- []interface{}{nil}

			case kvCmdClose:
				respch := msg[1].(chan []interface{})
				respch <- []interface{}{nil}
				break loop
			}
		}
	}
}

func (kvdata *KVData) scatterMutation(
	m *mc.DcpEvent, ts *protobuf.TsVbuuid) (err error) {

	vbno := m.VBucket

	switch m.Opcode {
	case mcd.DCP_STREAMREQ:
		if m.Status == mcd.ROLLBACK {
			fmsg := "%v ##%x StreamRequest ROLLBACK: %v\n"
			logging.Infof(fmsg, kvdata.logPrefix, m.Opaque, m)

		} else if m.Status != mcd.SUCCESS {
			fmsg := "%v ##%x StreamRequest %s: %v\n"
			logging.Errorf(fmsg, kvdata.logPrefix, m.Opaque, m.Status, m)

		} else if _, ok := kvdata.vrs[vbno]; ok {
			fmsg := "%v ##%x duplicate OpStreamRequest: %v\n"
			logging.Errorf(fmsg, kvdata.logPrefix, m.Opaque, m)

		} else if m.VBuuid, _, err = m.FailoverLog.Latest(); err != nil {
			panic(err)

		} else {
			fmsg := "%v ##%x StreamRequest: %v\n"
			logging.Tracef(fmsg, kvdata.logPrefix, m.Opaque, m)
			topic, bucket := kvdata.topic, kvdata.bucket
			m.Seqno, _ = ts.SeqnoFor(vbno)
			cluster, config := kvdata.feed.cluster, kvdata.config
			vr := NewVbucketRoutine(
				cluster, topic, bucket,
				m.Opaque, vbno, m.VBuuid, m.Seqno, config)
			if vr.AddEngines(0xFFFF, kvdata.engines, kvdata.endpoints) != nil {
				panic(err)
			}
			if vr.Event(m) != nil {
				panic(err)
			}
			kvdata.vrs[vbno] = vr
		}
		kvdata.feed.PostStreamRequest(kvdata.bucket, m)

	case mcd.DCP_STREAMEND:
		if vr, ok := kvdata.vrs[vbno]; !ok {
			fmsg := "%v ##%x duplicate OpStreamEnd: %v\n"
			logging.Errorf(fmsg, kvdata.logPrefix, m.Opaque, m)

		} else if m.Status != mcd.SUCCESS {
			fmsg := "%v ##%x StreamEnd %s: %v\n"
			logging.Errorf(fmsg, kvdata.logPrefix, m.Opaque, m)

		} else {
			fmsg := "%v ##%x StreamEnd: %v\n"
			logging.Tracef(fmsg, kvdata.logPrefix, m.Opaque, m)
			if vr.Event(m) != nil {
				panic(err)
			}
			delete(kvdata.vrs, vbno)
		}
		kvdata.feed.PostStreamEnd(kvdata.bucket, m)

	case mcd.DCP_SNAPSHOT:
		if vr, ok := kvdata.vrs[vbno]; ok && (vr.Event(m) != nil) {
			panic(err)
		} else if !ok {
			fmsg := "%v ##%x unknown vbucket: %v\n"
			logging.Fatalf(fmsg, kvdata.logPrefix, m.Opaque, m)
		}

	case mcd.DCP_MUTATION, mcd.DCP_DELETION, mcd.DCP_EXPIRATION:
		if vr, ok := kvdata.vrs[vbno]; ok && (vr.Event(m) != nil) {
			panic(err)
		} else if !ok {
			fmsg := "%v ##%x unknown vbucket: %v\n"
			logging.Fatalf(fmsg, kvdata.logPrefix, m.Opaque, m)
		}
	}
	return
}

func (kvdata *KVData) publishStreamEnd() error {
	for _, vr := range kvdata.vrs {
		m := &mc.DcpEvent{
			Opcode:  mcd.DCP_STREAMEND,
			Status:  mcd.SUCCESS,
			VBucket: vr.vbno,
			Opaque:  vr.opaque,
		}
		kvdata.feed.PostStreamEnd(kvdata.bucket, m)
		vr.Event(m)
	}
	return nil
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
