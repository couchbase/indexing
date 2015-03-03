// vbucket concurrency model:
//
//                           NewVbucketRoutine()
//                                   |
//                                   |               *---> endpoint
//                                (spawn)            |
//                                   |               *---> endpoint
//             Event() --*           |               |
//                       |--------> run -------------*---> endpoint
//        AddEngines() --*
//                       |
//     DeleteEngines() --*
//                       |
//     GetStatistics() --*

package projector

import "fmt"
import "time"

import mcd "github.com/couchbase/indexing/secondary/dcp/transport"
import mc "github.com/couchbase/indexing/secondary/dcp/transport/client"
import c "github.com/couchbase/indexing/secondary/common"
import "github.com/couchbase/indexing/secondary/logging"

// VbucketRoutine is immutable structure defined for each vbucket.
type VbucketRoutine struct {
	bucket    string // immutable
	opaque    uint16
	vbno      uint16 // immutable
	vbuuid    uint64 // immutable
	engines   map[uint64]*Engine
	endpoints map[string]c.RouterEndpoint
	// gen-server
	reqch chan []interface{}
	finch chan bool
	// config params
	mutChanSize int
	syncTimeout time.Duration // in milliseconds
	logPrefix   string
}

// NewVbucketRoutine creates a new routine to handle this vbucket stream.
func NewVbucketRoutine(
	cluster, topic, bucket string,
	opaque, vbno uint16,
	vbuuid, startSeqno uint64, config c.Config) *VbucketRoutine {

	mutChanSize := config["mutationChanSize"].Int()

	vr := &VbucketRoutine{
		bucket:    bucket,
		opaque:    opaque,
		vbno:      vbno,
		vbuuid:    vbuuid,
		engines:   make(map[uint64]*Engine),
		endpoints: make(map[string]c.RouterEndpoint),
		reqch:     make(chan []interface{}, mutChanSize),
		finch:     make(chan bool),
	}
	vr.logPrefix = fmt.Sprintf("VBRT[<-%v<-%v<-%v #%v]", vbno, bucket, cluster, topic)
	vr.mutChanSize = mutChanSize
	vr.syncTimeout = time.Duration(config["vbucketSyncTimeout"].Int())
	vr.syncTimeout *= time.Millisecond

	go vr.run(vr.reqch, startSeqno)
	logging.Infof("%v ##%x started ...\n", vr.logPrefix, opaque)
	return vr
}

const (
	vrCmdEvent byte = iota + 1
	vrCmdAddEngines
	vrCmdDeleteEngines
	vrCmdGetStatistics
	vrCmdSetConfig
	vrCmdClose
)

// Event will post an DcpEvent, asychronous call.
func (vr *VbucketRoutine) Event(m *mc.DcpEvent) error {
	cmd := []interface{}{vrCmdEvent, m}
	return c.FailsafeOpAsync(vr.reqch, cmd, vr.finch)
}

// AddEngines update active set of engines and endpoints
// synchronous call.
func (vr *VbucketRoutine) AddEngines(
	opaque uint16,
	engines map[uint64]*Engine,
	endpoints map[string]c.RouterEndpoint) error {

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{vrCmdAddEngines, opaque, engines, endpoints, respch}
	_, err := c.FailsafeOp(vr.reqch, respch, cmd, vr.finch)
	return err
}

// DeleteEngines delete engines and update endpoints
// synchronous call.
func (vr *VbucketRoutine) DeleteEngines(opaque uint16, engines []uint64) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{vrCmdDeleteEngines, opaque, engines, respch}
	_, err := c.FailsafeOp(vr.reqch, respch, cmd, vr.finch)
	return err
}

// GetStatistics for vr vbucket
// synchronous call.
func (vr *VbucketRoutine) GetStatistics() (map[string]interface{}, error) {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{vrCmdGetStatistics, respch}
	resp, err := c.FailsafeOp(vr.reqch, respch, cmd, vr.finch)
	if err != nil {
		return nil, err
	}
	return resp[0].(map[string]interface{}), nil
}

// SetConfig for vbucket-routine.
func (vr *VbucketRoutine) SetConfig(config c.Config) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{vrCmdSetConfig, config, respch}
	_, err := c.FailsafeOp(vr.reqch, respch, cmd, vr.finch)
	return err
}

// Close vbucket-routine, synchronous call.
func (vr *VbucketRoutine) Close() error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{vrCmdClose, respch}
	_, err := c.FailsafeOp(vr.reqch, respch, cmd, vr.finch)
	return err
}

// routine handles data path for a single vbucket.
func (vr *VbucketRoutine) run(reqch chan []interface{}, seqno uint64) {
	defer func() { // panic safe
		if r := recover(); r != nil {
			fmsg := "%v ##%x run() crashed: %v\n"
			logging.Fatalf(fmsg, vr.logPrefix, vr.opaque, r)
			logging.Errorf("%v", logging.StackTrace())
		}

		if data := vr.makeStreamEndData(seqno); data == nil {
			fmsg := "%v ##%x StreamEnd NOT PUBLISHED\n"
			logging.Errorf(fmsg, vr.logPrefix, vr.opaque)

		} else { // publish stream-end
			logging.Infof("%v ##%x StreamEnd\n", vr.logPrefix, vr.opaque)
			vr.broadcast2Endpoints(data)
		}

		close(vr.finch)
		logging.Infof("%v ##%x ... stopped\n", vr.logPrefix, vr.opaque)
	}()

	var heartBeat <-chan time.Time // for Sync message

	stats := vr.newStats()
	addEngineCount := stats.Get("addInsts").(float64)
	delEngineCount := stats.Get("delInsts").(float64)
	syncCount := stats.Get("syncs").(float64)
	sshotCount := stats.Get("snapshots").(float64)
	mutationCount := stats.Get("mutations").(float64)

loop:
	for {
		select {
		case msg := <-reqch:
			cmd := msg[0].(byte)
			switch cmd {
			case vrCmdAddEngines:
				vr.engines = make(map[uint64]*Engine)
				opaque := msg[1].(uint16)
				fmsg := "%v ##%x vrCmdAddEngines\n"
				logging.Tracef(fmsg, vr.logPrefix, opaque)
				if msg[2] != nil {
					fmsg := "%v ##%x AddEngine %v\n"
					for uuid, engine := range msg[2].(map[uint64]*Engine) {
						vr.engines[uuid] = engine
						logging.Tracef(fmsg, vr.logPrefix, opaque, uuid)
					}
					vr.printCtrl(vr.engines)
				}

				if msg[3] != nil {
					endpoints := msg[3].(map[string]c.RouterEndpoint)
					vr.endpoints = vr.updateEndpoints(opaque, endpoints)
					vr.printCtrl(vr.endpoints)
				}
				respch := msg[4].(chan []interface{})
				respch <- []interface{}{nil}
				addEngineCount++

			case vrCmdDeleteEngines:
				opaque := msg[1].(uint16)
				fmsg := "%v ##%x vrCmdDeleteEngines\n"
				logging.Tracef(fmsg, vr.logPrefix, opaque)
				engineKeys := msg[2].([]uint64)
				fmsg = "%v ##%x DelEngine %v\n"
				for _, uuid := range engineKeys {
					delete(vr.engines, uuid)
					logging.Tracef(fmsg, vr.logPrefix, opaque, uuid)
				}
				fmsg = "%v ##%x deleted engines %v\n"
				logging.Tracef(fmsg, vr.logPrefix, opaque, engineKeys)
				respch := msg[3].(chan []interface{})
				respch <- []interface{}{nil}
				delEngineCount++

			case vrCmdGetStatistics:
				logging.Tracef("%v vrCmdStatistics\n", vr.logPrefix)
				respch := msg[1].(chan []interface{})
				stats.Set("addInsts", addEngineCount)
				stats.Set("delInsts", delEngineCount)
				stats.Set("syncs", syncCount)
				stats.Set("snapshots", sshotCount)
				stats.Set("mutations", mutationCount)
				respch <- []interface{}{stats.ToMap()}

			case vrCmdSetConfig:
				config, respch := msg[1].(c.Config), msg[2].(chan []interface{})
				vr.syncTimeout = time.Duration(config["vbucketSyncTimeout"].Int())
				vr.syncTimeout *= time.Millisecond
				// re-initialize the heart beat only if it is already started.
				if heartBeat != nil {
					infomsg := "%v ##%x heart-beat reloaded: %v\n"
					logging.Infof(infomsg, vr.logPrefix, vr.opaque, vr.syncTimeout)
					heartBeat = time.Tick(vr.syncTimeout)
				}
				respch <- []interface{}{nil}

			case vrCmdEvent:
				m := msg[1].(*mc.DcpEvent)
				if m.Opaque != vr.opaque {
					fmsg := "%v ##%x mismatch with vr.##%x %v"
					logging.Fatalf(fmsg, vr.logPrefix, m.Opaque, vr.opaque, m.Opcode)
				}
				if m.Opcode == mcd.DCP_STREAMREQ { // opens up the path
					heartBeat = time.Tick(vr.syncTimeout)
					fmsg := "%v ##%x heartbeat (%v) loaded ...\n"
					logging.Infof(fmsg, vr.logPrefix, m.Opaque, vr.syncTimeout)
				}

				// count statistics
				seqno = vr.handleEvent(m, seqno)
				switch m.Opcode {
				case mcd.DCP_SNAPSHOT:
					sshotCount++
				case mcd.DCP_MUTATION, mcd.DCP_DELETION, mcd.DCP_EXPIRATION:
					mutationCount++
				case mcd.DCP_STREAMEND:
					break loop
				}

			case vrCmdClose:
				logging.Debugf("%v ##%x closed\n", vr.logPrefix, vr.opaque)
				break loop
			}

		case <-heartBeat:
			if data := vr.makeSyncData(seqno); data != nil {
				syncCount++
				fmsg := "%v ##%x sync count %v\n"
				logging.Tracef(fmsg, vr.logPrefix, vr.opaque, syncCount)
				vr.broadcast2Endpoints(data)

			} else {
				fmsg := "%v ##%x Sync NOT PUBLISHED\n"
				logging.Errorf(fmsg, vr.logPrefix, vr.opaque)
			}
		}
	}
}

// only endpoints that host engines defined on this vbucket.
func (vr *VbucketRoutine) updateEndpoints(
	opaque uint16,
	eps map[string]c.RouterEndpoint) map[string]c.RouterEndpoint {

	endpoints := make(map[string]c.RouterEndpoint)
	for _, engine := range vr.engines {
		for _, raddr := range engine.Endpoints() {
			if _, ok := eps[raddr]; !ok {
				fmsg := "%v ##%x endpoint %v not found\n"
				logging.Errorf(fmsg, vr.logPrefix, opaque, raddr)
			}
			logging.Tracef("%v UpdateEndpoint %v to %v\n", vr.logPrefix, raddr, engine)
			endpoints[raddr] = eps[raddr]
		}
	}
	return endpoints
}

var ssFormat = "%v ##%x received snapshot %v %v (type %x)\n"
var traceMutFormat = "%v ##%x DcpEvent %v:%v <<%v>>\n"

func (vr *VbucketRoutine) handleEvent(m *mc.DcpEvent, seqno uint64) uint64 {
	logging.Tracef(
		traceMutFormat,
		vr.logPrefix, m.Opaque, m.Seqno, m.Opcode, string(m.Key))

	switch m.Opcode {
	case mcd.DCP_STREAMREQ: // broadcast StreamBegin
		if data := vr.makeStreamBeginData(seqno); data != nil {
			vr.broadcast2Endpoints(data)
		} else {
			fmsg := "%v ##%x StreamBeginData NOT PUBLISHED\n"
			logging.Errorf(fmsg, vr.logPrefix, m.Opaque)
		}

	case mcd.DCP_SNAPSHOT: // broadcast Snapshot
		typ, start, end := m.SnapshotType, m.SnapstartSeq, m.SnapendSeq
		logging.Infof(ssFormat, vr.logPrefix, m.Opaque, start, end, typ)
		if data := vr.makeSnapshotData(m, seqno); data != nil {
			vr.broadcast2Endpoints(data)
		} else {
			fmsg := "%v ##%x Snapshot NOT PUBLISHED\n"
			logging.Errorf(fmsg, vr.logPrefix, m.Opaque)
		}

	case mcd.DCP_MUTATION, mcd.DCP_DELETION, mcd.DCP_EXPIRATION:
		seqno = m.Seqno // sequence number gets incremented only here
		// prepare a data for each endpoint.
		dataForEndpoints := make(map[string]interface{})
		// for each engine distribute transformations to endpoints.
		fmsg := "%v ##%x TransformRoute: %v\n"
		for _, engine := range vr.engines {
			err := engine.TransformRoute(vr.vbuuid, m, dataForEndpoints)
			if err != nil {
				logging.Errorf(fmsg, vr.logPrefix, m.Opaque, err)
				continue
			}
		}
		// send data to corresponding endpoint.
		for raddr, data := range dataForEndpoints {
			if endpoint, ok := vr.endpoints[raddr]; ok {
				// FIXME: without the coordinator doing shared topic
				// management, we will allow the feed to block.
				// Otherwise, send might fail due to ErrorChannelFull
				// or ErrorClosed
				if err := endpoint.Send(data); err != nil {
					msg := "%v ##%x endpoint(%q).Send() failed: %v"
					logging.Infof(msg, vr.logPrefix, m.Opaque, raddr, err)
					endpoint.Close()
					delete(vr.endpoints, raddr)
				}
			}
		}
	}
	return seqno
}

// send to all endpoints.
func (vr *VbucketRoutine) broadcast2Endpoints(data interface{}) {
	for raddr, endpoint := range vr.endpoints {
		// FIXME: without the coordinator doing shared topic
		// management, we will allow the feed to block.
		// Otherwise, send might fail due to ErrorChannelFull
		// or ErrorClosed
		if err := endpoint.Send(data); err != nil {
			msg := "%v ##%x endpoint(%q).Send() failed: %v"
			logging.Infof(msg, vr.logPrefix, vr.opaque, raddr, err)
			endpoint.Close()
			delete(vr.endpoints, raddr)
		}
	}
}

func (vr *VbucketRoutine) makeStreamBeginData(seqno uint64) interface{} {
	defer func() {
		if r := recover(); r != nil {
			fmsg := "%v ##%x stream-begin crashed: %v\n"
			logging.Fatalf(fmsg, vr.logPrefix, vr.opaque, r)
			logging.Errorf("%s", logging.StackTrace())
		}
	}()

	if len(vr.engines) == 0 {
		return nil
	}
	// using the first engine that is capable of it.
	for _, engine := range vr.engines {
		data := engine.StreamBeginData(vr.vbno, vr.vbuuid, seqno)
		if data != nil {
			return data
		}
	}
	return nil
}

func (vr *VbucketRoutine) makeSyncData(seqno uint64) (data interface{}) {
	defer func() {
		if r := recover(); r != nil {
			fmsg := "%v ##%x sync crashed: %v\n"
			logging.Fatalf(fmsg, vr.logPrefix, vr.opaque, r)
			logging.Errorf("%s", logging.StackTrace())
		}
	}()

	if len(vr.engines) == 0 {
		return
	}
	// using the first engine that is capable of it.
	for _, engine := range vr.engines {
		data := engine.SyncData(vr.vbno, vr.vbuuid, seqno)
		if data != nil {
			return data
		}
	}
	return
}

func (vr *VbucketRoutine) makeSnapshotData(
	m *mc.DcpEvent, seqno uint64) interface{} {

	defer func() {
		if r := recover(); r != nil {
			fmsg := "%v ##%x snapshot crashed: %v\n"
			logging.Fatalf(fmsg, vr.logPrefix, vr.opaque, r)
			logging.Errorf("%s", logging.StackTrace())
		}
	}()

	if len(vr.engines) == 0 {
		return nil
	}
	// using the first engine that is capable of it.
	for _, engine := range vr.engines {
		data := engine.SnapshotData(m, vr.vbno, vr.vbuuid, seqno)
		if data != nil {
			return data
		}
	}
	return nil
}

func (vr *VbucketRoutine) makeStreamEndData(seqno uint64) interface{} {
	defer func() {
		if r := recover(); r != nil {
			fmsg := "%v stream-end crashed: %v\n"
			logging.Fatalf(fmsg, vr.logPrefix, vr.opaque, r)
			logging.Errorf("%s", logging.StackTrace())
		}
	}()

	if len(vr.engines) == 0 {
		return nil
	}

	// using the first engine that is capable of it.
	for _, engine := range vr.engines {
		data := engine.StreamEndData(vr.vbno, vr.vbuuid, seqno)
		if data != nil {
			return data
		}
	}
	return nil
}

func (vr *VbucketRoutine) newStats() c.Statistics {
	m := map[string]interface{}{
		"addInsts":  float64(0), // no. of update-engine commands
		"delInsts":  float64(0), // no. of delete-engine commands
		"syncs":     float64(0), // no. of Sync message generated
		"snapshots": float64(0), // no. of Begin
		"mutations": float64(0), // no. of Upsert, Delete
	}
	stats, _ := c.NewStatistics(m)
	return stats
}

func (vr *VbucketRoutine) printCtrl(v interface{}) {
	switch val := v.(type) {
	case map[string]c.RouterEndpoint:
		for raddr := range val {
			fmsg := "%v ##%x knows endpoint %v\n"
			logging.Tracef(fmsg, vr.logPrefix, vr.opaque, raddr)
		}
	case map[uint64]*Engine:
		for uuid := range val {
			fmsg := "%v ##%x knows engine %v\n"
			logging.Tracef(fmsg, vr.logPrefix, vr.opaque, uuid)
		}
	}
}
