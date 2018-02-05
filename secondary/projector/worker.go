// worker concurrency model:
//
//                           NewVbucketWorker()
//                                   |
//                                   |               *---> endpoint
//                                (spawn)            |
//                                   |               *---> endpoint
//             Event() --*           |               |
//                       |--------> run -------------*---> endpoint
//        AddEngines() --*
//                       |
//       ResetConfig() --*
//                       |
//     DeleteEngines() --*
//                       |
//     GetStatistics() --*
//                       |
//             Close() --*

package projector

import "fmt"
import "strconv"

import mcd "github.com/couchbase/indexing/secondary/dcp/transport"
import mc "github.com/couchbase/indexing/secondary/dcp/transport/client"
import c "github.com/couchbase/indexing/secondary/common"
import "github.com/couchbase/indexing/secondary/logging"

// VbucketWorker is immutable structure defined for each vbucket.
type VbucketWorker struct {
	id       int
	feed     *Feed
	cluster  string
	topic    string
	bucket   string
	opaque   uint16
	config   c.Config
	vbuckets map[uint16]*Vbucket
	// evaluators and subscribers
	engines   map[uint64]*Engine
	endpoints map[string]c.RouterEndpoint
	// server channels
	reqch chan []interface{}
	finch chan bool
	// config params
	logPrefix   string
	mutChanSize int

	encodeBuf []byte
}

// NewVbucketWorker creates a new routine to handle this vbucket stream.
func NewVbucketWorker(
	id int, feed *Feed, bucket string,
	opaque uint16, config c.Config) *VbucketWorker {

	mutChanSize := config["mutationChanSize"].Int()
	encodeBufSize := config["encodeBufSize"].Int()

	worker := &VbucketWorker{
		id:        id,
		feed:      feed,
		cluster:   feed.cluster,
		topic:     feed.topic,
		bucket:    bucket,
		opaque:    opaque,
		config:    config,
		vbuckets:  make(map[uint16]*Vbucket),
		engines:   make(map[uint64]*Engine),
		endpoints: make(map[string]c.RouterEndpoint),
		reqch:     make(chan []interface{}, mutChanSize),
		finch:     make(chan bool),
		encodeBuf: make([]byte, 0, encodeBufSize),
	}
	fmsg := "WRKR[%v<-%v<-%v #%v]"
	worker.logPrefix = fmt.Sprintf(fmsg, id, bucket, feed.cluster, feed.topic)
	worker.mutChanSize = mutChanSize
	go worker.run(worker.reqch)
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
	return c.FailsafeOpAsync(worker.reqch, cmd, worker.finch)
}

// SyncPulse will trigger worker to generate a sync pulse for all its
// vbuckets, asychronous call.
func (worker *VbucketWorker) SyncPulse() error {
	cmd := []interface{}{vwCmdSyncPulse}
	return c.FailsafeOpAsync(worker.reqch, cmd, worker.finch)
}

// GetVbuckets will return the list of active vbuckets managed by this
// workers.
func (worker *VbucketWorker) GetVbuckets() ([]*Vbucket, error) {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{vwCmdGetVbuckets, respch}
	resp, err := c.FailsafeOp(worker.reqch, respch, cmd, worker.finch)
	if err != nil {
		return nil, err
	}
	return resp[0].([]*Vbucket), nil
}

// AddEngines update active set of engines and endpoints, synchronous call.
func (worker *VbucketWorker) AddEngines(
	opaque uint16,
	engines map[uint64]*Engine,
	endpoints map[string]c.RouterEndpoint) (map[uint16]uint64, error) {

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{vwCmdAddEngines, opaque, engines, endpoints, respch}
	resp, err := c.FailsafeOp(worker.reqch, respch, cmd, worker.finch)
	if err != nil {
		return nil, err
	}
	return resp[0].(map[uint16]uint64), nil
}

// DeleteEngines delete engines and update endpoints
// synchronous call.
func (worker *VbucketWorker) DeleteEngines(
	opaque uint16, engines []uint64) error {

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{vwCmdDelEngines, opaque, engines, respch}
	_, err := c.FailsafeOp(worker.reqch, respch, cmd, worker.finch)
	return err
}

// ResetConfig for worker-routine, synchronous call.
func (worker *VbucketWorker) ResetConfig(config c.Config) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{vwCmdResetConfig, config, respch}
	_, err := c.FailsafeOp(worker.reqch, respch, cmd, worker.finch)
	return err
}

// GetStatistics for worker vbucket, synchronous call.
func (worker *VbucketWorker) GetStatistics() (map[string]interface{}, error) {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{vwCmdGetStats, respch}
	resp, err := c.FailsafeOp(worker.reqch, respch, cmd, worker.finch)
	if err != nil {
		return nil, err
	}
	return resp[0].(map[string]interface{}), nil
}

// Close worker-routine, synchronous call.
func (worker *VbucketWorker) Close() error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{vwCmdClose, respch}
	_, err := c.FailsafeOp(worker.reqch, respch, cmd, worker.finch)
	return err
}

// routine handles data path for a single worker handling one
// or more vbuckets.
func (worker *VbucketWorker) run(reqch chan []interface{}) {
	logPrefix := worker.logPrefix
	logging.Infof("%v started ...", logPrefix)

	defer func() { // panic safe
		if r := recover(); r != nil {
			fmsg := "%v ##%x run() crashed: %v\n"
			logging.Fatalf(fmsg, logPrefix, worker.opaque, r)
			logging.Errorf("%v", logging.StackTrace())
		}
		// call out a STREAM-END for active vbuckets.
		for _, v := range worker.vbuckets {
			if data := v.makeStreamEndData(worker.engines); data != nil {
				worker.broadcast2Endpoints(data)
			} else {
				fmsg := "%v ##%x StreamEnd NOT PUBLISHED vb %v\n"
				logging.Errorf(fmsg, logPrefix, worker.opaque, v.vbno)
			}
		}
		close(worker.finch)
		logging.Infof("%v ##%x ... stopped\n", logPrefix, worker.opaque)
	}()

loop:
	for {
		select {
		case msg := <-reqch:
			cmd := msg[0].(byte)
			switch cmd {
			case vwCmdSyncPulse:
				for _, v := range worker.vbuckets {
					if data := v.makeSyncData(worker.engines); data != nil {
						v.syncCount++
						fmsg := "%v ##%x sync count %v\n"
						logging.Tracef(fmsg, v.logPrefix, v.opaque, v.syncCount)
						worker.broadcast2Endpoints(data)

					} else {
						fmsg := "%v ##%x Sync NOT PUBLISHED for %v\n"
						logging.Errorf(fmsg, logPrefix, worker.opaque, v.vbno)
					}
				}

			case vwCmdGetVbuckets:
				vbuckets := make([]*Vbucket, 0, len(worker.vbuckets))
				for _, v := range worker.vbuckets {
					vbuckets = append(vbuckets, v)
				}
				respch := msg[1].(chan []interface{})
				respch <- []interface{}{vbuckets}

			case vwCmdAddEngines:
				worker.engines = make(map[uint64]*Engine)
				opaque := msg[1].(uint16)
				if msg[2] != nil {
					fmsg := "%v ##%x AddEngine %v\n"
					for uuid, engine := range msg[2].(map[uint64]*Engine) {
						worker.engines[uuid] = engine
						logging.Tracef(fmsg, logPrefix, opaque, uuid)
					}
					worker.printCtrl(worker.engines)
				}
				if msg[3] != nil {
					endpoints := msg[3].(map[string]c.RouterEndpoint)
					worker.endpoints = worker.updateEndpoints(opaque, endpoints)
					worker.printCtrl(worker.endpoints)
				}
				cseqnos := make(map[uint16]uint64)
				for _, v := range worker.vbuckets {
					cseqnos[v.vbno] = v.seqno
				}
				respch := msg[4].(chan []interface{})
				respch <- []interface{}{cseqnos}

			case vwCmdDelEngines:
				opaque := msg[1].(uint16)
				fmsg := "%v ##%x vwCmdDeleteEngines\n"
				logging.Tracef(fmsg, logPrefix, opaque)
				engineKeys := msg[2].([]uint64)
				fmsg = "%v ##%x DelEngine %v\n"
				for _, uuid := range engineKeys {
					delete(worker.engines, uuid)
					logging.Tracef(fmsg, logPrefix, opaque, uuid)
				}
				fmsg = "%v ##%x deleted engines %v\n"
				logging.Tracef(fmsg, logPrefix, opaque, engineKeys)
				respch := msg[3].(chan []interface{})
				respch <- []interface{}{nil}

			case vwCmdGetStats:
				logging.Tracef("%v vwCmdStatistics\n", worker.logPrefix)
				stats := make(map[string]interface{})
				for vbno, v := range worker.vbuckets {
					stats[strconv.Itoa(int(vbno))] = map[string]interface{}{
						"syncs":     float64(v.syncCount),
						"snapshots": float64(v.sshotCount),
						"mutations": float64(v.mutationCount),
					}
				}
				respch := msg[1].(chan []interface{})
				respch <- []interface{}{stats}

			case vwCmdResetConfig:
				_, respch := msg[1].(c.Config), msg[2].(chan []interface{})
				respch <- []interface{}{nil}

			case vwCmdEvent:
				m := msg[1].(*mc.DcpEvent)
				v := worker.handleEvent(m)
				if v == nil {
					fmsg := "%v ##%x nil vbucket %v for %v"
					logging.Fatalf(fmsg, logPrefix, m.Opaque, m.VBucket, m.Opcode)

				} else if m.Opcode == mcd.DCP_STREAMEND {
					delete(worker.vbuckets, v.vbno)

				} else if m.Opaque != v.opaque {
					fmsg := "%v ##%x mismatch with vbucket.##%x %v"
					logging.Fatalf(fmsg, logPrefix, m.Opaque, v.opaque, m.Opcode)
				}

			case vwCmdClose:
				logging.Infof("%v ##%x closed\n", logPrefix, worker.opaque)
				respch := msg[1].(chan []interface{})
				respch <- []interface{}{nil}
				break loop
			}
		}
	}
}

// only endpoints that host engines defined on this vbucket.
func (worker *VbucketWorker) updateEndpoints(
	opaque uint16,
	eps map[string]c.RouterEndpoint) map[string]c.RouterEndpoint {

	endpoints := make(map[string]c.RouterEndpoint)
	for _, engine := range worker.engines {
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
	return endpoints
}

var traceMutFormat = "%v ##%x DcpEvent %v:%v <<%v>>\n"

func (worker *VbucketWorker) handleEvent(m *mc.DcpEvent) *Vbucket {
	vbno := m.VBucket
	v, vbok := worker.vbuckets[vbno]
	logPrefix := worker.logPrefix

	logging.LazyTrace(func() string {
		return fmt.Sprintf(traceMutFormat, logPrefix, m.Opaque, m.Seqno, m.Opcode, logging.TagUD(m.Key))
	})

	switch m.Opcode {
	case mcd.DCP_STREAMREQ: // broadcast StreamBegin
		if vbok {
			fmsg := "%v ##%x duplicate OpStreamRequest: %v\n"
			arg1 := logging.TagUD(m)
			logging.Errorf(fmsg, logPrefix, m.Opaque, arg1)
			return v
		}
		// opens up the path
		cluster, topic, bucket := worker.cluster, worker.topic, worker.bucket
		config, opaque, vbuuid := worker.config, m.Opaque, m.VBuuid
		v = NewVbucket(
			cluster, topic, bucket, opaque, vbno, vbuuid, m.Seqno, config)
		worker.vbuckets[vbno] = v
		if data := v.makeStreamBeginData(worker.engines); data != nil {
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
		if data := v.makeSnapshotData(m, worker.engines); data != nil {
			worker.broadcast2Endpoints(data)
			v.sshotCount++
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
		v.mutationCount++
		v.seqno = m.Seqno // sequence number gets updated only here
		// prepare a data for each endpoint.
		dataForEndpoints := make(map[string]interface{})
		// for each engine distribute transformations to endpoints.
		fmsg := "%v ##%x TransformRoute: %v\n"
		for _, engine := range worker.engines {
			newBuf, err := engine.TransformRoute(v.vbuuid, m, dataForEndpoints, worker.encodeBuf)
			if err != nil {
				logging.Errorf(fmsg, logPrefix, m.Opaque, err)
			}
			// TODO: Shrink the buffer periodically or as needed
			if cap(newBuf) > cap(worker.encodeBuf) {
				worker.encodeBuf = newBuf[:0]
			}
		}
		// send data to corresponding endpoint.
		for raddr, data := range dataForEndpoints {
			if endpoint, ok := worker.endpoints[raddr]; ok {
				// FIXME: without the coordinator doing shared topic
				// management, we will allow the feed to block.
				// Otherwise, send might fail due to ErrorChannelFull
				// or ErrorClosed
				if err := endpoint.Send(data); err != nil {
					fmsg := "%v ##%x endpoint(%q).Send() failed: %v"
					logging.Debugf(fmsg, logPrefix, worker.opaque, raddr, err)
					endpoint.Close()
					delete(worker.endpoints, raddr)
				}
			}
		}

	case mcd.DCP_STREAMEND:
		if vbok {
			if data := v.makeStreamEndData(worker.engines); data != nil {
				worker.broadcast2Endpoints(data)
			} else {
				fmsg := "%v ##%x StreamEnd NOT PUBLISHED vb %v\n"
				logging.Errorf(fmsg, logPrefix, worker.opaque, v.vbno)
			}
			delete(worker.vbuckets, vbno)
		}
	}
	return v
}

// send to all endpoints.
func (worker *VbucketWorker) broadcast2Endpoints(data interface{}) {
	for raddr, endpoint := range worker.endpoints {
		// FIXME: without the coordinator doing shared topic
		// management, we will allow the feed to block.
		// Otherwise, send might fail due to ErrorChannelFull
		// or ErrorClosed
		if err := endpoint.Send(data); err != nil {
			fmsg := "%v ##%x endpoint(%q).Send() failed: %v"
			logging.Debugf(fmsg, worker.logPrefix, worker.opaque, raddr, err)
			endpoint.Close()
			delete(worker.endpoints, raddr)
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
	case map[uint64]*Engine:
		for uuid := range val {
			fmsg := "%v ##%x knows engine %v\n"
			logging.Tracef(fmsg, worker.logPrefix, worker.opaque, uuid)
		}
	}
}
