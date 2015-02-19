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
	vbno uint16, vbuuid, startSeqno uint64, config c.Config) *VbucketRoutine {

	mutChanSize := config["mutationChanSize"].Int()

	vr := &VbucketRoutine{
		bucket:    bucket,
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
	logging.Infof("%v started ...\n", vr.logPrefix)
	return vr
}

const (
	vrCmdEvent byte = iota + 1
	vrCmdAddEngines
	vrCmdDeleteEngines
	vrCmdGetStatistics
	vrCmdSetConfig
)

// Event will post an DcpEvent, asychronous call.
func (vr *VbucketRoutine) Event(m *mc.DcpEvent) error {
	cmd := []interface{}{vrCmdEvent, m}
	return c.FailsafeOpAsync(vr.reqch, cmd, vr.finch)
}

// AddEngines update active set of engines and endpoints
// synchronous call.
func (vr *VbucketRoutine) AddEngines(
	engines map[uint64]*Engine,
	endpoints map[string]c.RouterEndpoint) error {

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{vrCmdAddEngines, engines, endpoints, respch}
	_, err := c.FailsafeOp(vr.reqch, respch, cmd, vr.finch)
	return err
}

// DeleteEngines delete engines and update endpoints
// synchronous call.
func (vr *VbucketRoutine) DeleteEngines(engines []uint64) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{vrCmdDeleteEngines, engines, respch}
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

// routine handles data path for a single vbucket.
func (vr *VbucketRoutine) run(reqch chan []interface{}, seqno uint64) {
	defer func() { // panic safe
		if r := recover(); r != nil {
			logging.Errorf("%v run() crashed: %v\n", vr.logPrefix, r)
			logging.Errorf("%v", logging.StackTrace())
		}

		if data := vr.makeStreamEndData(seqno); data == nil {
			logging.Errorf("%v StreamEnd NOT PUBLISHED\n", vr.logPrefix)

		} else { // publish stream-end
			logging.Debugf("%v StreamEnd for vbucket %v\n", vr.logPrefix, vr.vbno)
			vr.broadcast2Endpoints(data)
		}

		close(vr.finch)
		logging.Infof("%v ... stopped\n", vr.logPrefix)
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
				logging.Tracef("%v vrCmdAddEngines\n", vr.logPrefix)
				vr.engines = make(map[uint64]*Engine)
				if msg[1] != nil {
					for uuid, engine := range msg[1].(map[uint64]*Engine) {
						vr.engines[uuid] = engine
						logging.Tracef("%v AddEngine %v\n", vr.logPrefix, uuid)
					}
					vr.printCtrl(vr.engines)
				}

				if msg[2] != nil {
					endpoints := msg[2].(map[string]c.RouterEndpoint)
					vr.endpoints = vr.updateEndpoints(endpoints)
					vr.printCtrl(vr.endpoints)
				}
				respch := msg[3].(chan []interface{})
				respch <- []interface{}{nil}
				addEngineCount++

			case vrCmdDeleteEngines:
				logging.Tracef("%v vrCmdDeleteEngines\n", vr.logPrefix)
				engineKeys := msg[1].([]uint64)
				for _, uuid := range engineKeys {
					delete(vr.engines, uuid)
					logging.Tracef("%v DelEngine %v\n", vr.logPrefix, uuid)
				}

				logging.Tracef("%v deleted engines %v\n", engineKeys)
				respch := msg[2].(chan []interface{})
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
					infomsg := "%v heart-beat reloaded: %v\n"
					logging.Infof(infomsg, vr.logPrefix, vr.syncTimeout)
					heartBeat = time.Tick(vr.syncTimeout)
				}
				respch <- []interface{}{nil}

			case vrCmdEvent:
				m := msg[1].(*mc.DcpEvent)
				if m.Opcode == mcd.DCP_STREAMREQ { // opens up the path
					heartBeat = time.Tick(vr.syncTimeout)
					format := "%v heartbeat (%v) loaded ...\n"
					logging.Tracef(format, vr.logPrefix, vr.syncTimeout)
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
			}

		case <-heartBeat:
			if data := vr.makeSyncData(seqno); data != nil {
				syncCount++
				logging.Tracef("%v Sync count %v\n", vr.logPrefix, syncCount)
				vr.broadcast2Endpoints(data)

			} else {
				logging.Errorf("%v Sync NOT PUBLISHED\n", vr.logPrefix)
			}
		}
	}
}

// only endpoints that host engines defined on this vbucket.
func (vr *VbucketRoutine) updateEndpoints(
	eps map[string]c.RouterEndpoint) map[string]c.RouterEndpoint {

	endpoints := make(map[string]c.RouterEndpoint)
	for _, engine := range vr.engines {
		for _, raddr := range engine.Endpoints() {
			if _, ok := eps[raddr]; !ok {
				format := "%v endpoint %v not found\n"
				logging.Errorf(format, vr.logPrefix, raddr)
			}
			logging.Tracef("%v UpdateEndpoint %v to %v\n", vr.logPrefix, raddr, engine)
			endpoints[raddr] = eps[raddr]
		}
	}
	return endpoints
}

var ssFormat = "%v received snapshot %v %v (type %x)\n"
var traceMutFormat = "%v DcpEvent %v:%v <<%v>>\n"

func (vr *VbucketRoutine) handleEvent(m *mc.DcpEvent, seqno uint64) uint64 {
	logging.Tracef(traceMutFormat, vr.logPrefix, m.Seqno, m.Opcode, string(m.Key))

	switch m.Opcode {
	case mcd.DCP_STREAMREQ: // broadcast StreamBegin
		if data := vr.makeStreamBeginData(seqno); data != nil {
			vr.broadcast2Endpoints(data)
		} else {
			logging.Errorf("%v StreamBeginData NOT PUBLISHED\n", vr.logPrefix)
		}

	case mcd.DCP_SNAPSHOT: // broadcast Snapshot
		typ, start, end := m.SnapshotType, m.SnapstartSeq, m.SnapendSeq
		logging.Debugf(ssFormat, vr.logPrefix, start, end, typ)
		if data := vr.makeSnapshotData(m, seqno); data != nil {
			vr.broadcast2Endpoints(data)
		} else {
			logging.Errorf("%v Snapshot NOT PUBLISHED\n", vr.logPrefix)
		}

	case mcd.DCP_MUTATION, mcd.DCP_DELETION, mcd.DCP_EXPIRATION:
		// sequence number gets incremented only here.
		seqno = m.Seqno
		// prepare a data for each endpoint.
		dataForEndpoints := make(map[string]interface{})
		// for each engine distribute transformations to endpoints.
		for _, engine := range vr.engines {
			err := engine.TransformRoute(vr.vbuuid, m, dataForEndpoints)
			if err != nil {
				logging.Errorf("%v TransformRoute %v\n", vr.logPrefix, err)
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
					msg := "%v endpoint(%q).Send() failed: %v"
					logging.Errorf(msg, vr.logPrefix, raddr, err)
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
			msg := "%v endpoint(%q).Send() failed: %v"
			logging.Errorf(msg, vr.logPrefix, raddr, err)
			endpoint.Close()
			delete(vr.endpoints, raddr)
		}
	}
}

func (vr *VbucketRoutine) makeStreamBeginData(seqno uint64) interface{} {
	defer func() {
		if r := recover(); r != nil {
			logging.Errorf("%v stream-begin crashed: %v\n", vr.logPrefix, r)
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
			logging.Errorf("%v sync crashed: %v\n", vr.logPrefix, r)
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
			logging.Errorf("%v snapshot crashed: %v\n", vr.logPrefix, r)
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
			logging.Errorf("%v stream-end crashed: %v\n", vr.logPrefix, r)
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
			logging.Tracef("%v knows endpoint %v\n", vr.logPrefix, raddr)
		}
	case map[uint64]*Engine:
		for uuid := range val {
			logging.Tracef("%v knows engine %v\n", vr.logPrefix, uuid)
		}
	}
}
