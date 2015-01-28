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
import "runtime/debug"

import mcd "github.com/couchbase/indexing/secondary/dcp/transport"
import mc "github.com/couchbase/indexing/secondary/dcp/transport/client"
import c "github.com/couchbase/indexing/secondary/common"

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
	c.Infof("%v started ...\n", vr.logPrefix)
	return vr
}

const (
	vrCmdEvent byte = iota + 1
	vrCmdAddEngines
	vrCmdDeleteEngines
	vrCmdGetStatistics
)

// Event will post an UprEvent, asychronous call.
func (vr *VbucketRoutine) Event(m *mc.UprEvent) error {
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
func (vr *VbucketRoutine) GetStatistics() map[string]interface{} {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{vrCmdGetStatistics, respch}
	resp, _ := c.FailsafeOp(vr.reqch, respch, cmd, vr.finch)
	return resp[0].(map[string]interface{})
}

// routine handles data path for a single vbucket.
func (vr *VbucketRoutine) run(reqch chan []interface{}, seqno uint64) {
	defer func() { // panic safe
		if r := recover(); r != nil {
			c.Errorf("%v run() crashed: %v\n", vr.logPrefix, r)
			c.StackTrace(string(debug.Stack()))
		}

		if data := vr.makeStreamEndData(seqno); data == nil {
			c.Errorf("%v StreamEnd NOT PUBLISHED\n", vr.logPrefix)

		} else { // publish stream-end
			c.Debugf("%v StreamEnd for vbucket %v\n", vr.logPrefix, vr.vbno)
			vr.broadcast2Endpoints(data)
		}

		close(vr.finch)
		c.Infof("%v ... stopped\n", vr.logPrefix)
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
				c.Tracef("%v vrCmdAddEngines\n", vr.logPrefix)
				vr.engines = make(map[uint64]*Engine)
				if msg[1] != nil {
					for uuid, engine := range msg[1].(map[uint64]*Engine) {
						vr.engines[uuid] = engine
						c.Tracef("%v AddEngine %v\n", vr.logPrefix, uuid)
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
				c.Tracef("%v vrCmdDeleteEngines\n", vr.logPrefix)
				engineKeys := msg[1].([]uint64)
				for _, uuid := range engineKeys {
					delete(vr.engines, uuid)
					c.Tracef("%v DelEngine %v\n", vr.logPrefix, uuid)
				}

				c.Tracef("%v deleted engines %v\n", engineKeys)
				respch := msg[2].(chan []interface{})
				respch <- []interface{}{nil}
				delEngineCount++

			case vrCmdGetStatistics:
				c.Tracef("%v vrCmdStatistics\n", vr.logPrefix)
				respch := msg[1].(chan []interface{})
				stats.Set("addInsts", addEngineCount)
				stats.Set("delInsts", delEngineCount)
				stats.Set("syncs", syncCount)
				stats.Set("snapshots", sshotCount)
				stats.Set("mutations", mutationCount)
				respch <- []interface{}{stats.ToMap()}

			case vrCmdEvent:
				m := msg[1].(*mc.UprEvent)
				if m.Opcode == mcd.UPR_STREAMREQ { // opens up the path
					heartBeat = time.Tick(vr.syncTimeout)
					format := "%v heartbeat (%v) loaded ...\n"
					c.Tracef(format, vr.logPrefix, vr.syncTimeout)
				}

				// count statistics
				seqno = vr.handleEvent(m, seqno)
				switch m.Opcode {
				case mcd.UPR_SNAPSHOT:
					sshotCount++
				case mcd.UPR_MUTATION, mcd.UPR_DELETION, mcd.UPR_EXPIRATION:
					mutationCount++
				case mcd.UPR_STREAMEND:
					break loop
				}
			}

		case <-heartBeat:
			if data := vr.makeSyncData(seqno); data != nil {
				syncCount++
				c.Tracef("%v Sync count %v\n", vr.logPrefix, syncCount)
				vr.broadcast2Endpoints(data)

			} else {
				c.Errorf("%v Sync NOT PUBLISHED\n", vr.logPrefix)
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
				c.Errorf(format, vr.logPrefix, raddr)
			}
			c.Tracef("%v UpdateEndpoint %v to %v\n", vr.logPrefix, raddr, engine)
			endpoints[raddr] = eps[raddr]
		}
	}
	return endpoints
}

var ssFormat = "%v received snapshot %v %v (type %x)\n"
var traceMutFormat = "%v UprEvent %v:%v <<%v>>\n"

func (vr *VbucketRoutine) handleEvent(m *mc.UprEvent, seqno uint64) uint64 {
	c.Tracef(traceMutFormat, vr.logPrefix, m.Seqno, m.Opcode, string(m.Key))

	switch m.Opcode {
	case mcd.UPR_STREAMREQ: // broadcast StreamBegin
		if data := vr.makeStreamBeginData(seqno); data != nil {
			vr.broadcast2Endpoints(data)
		} else {
			c.Errorf("%v StreamBeginData NOT PUBLISHED\n", vr.logPrefix)
		}

	case mcd.UPR_SNAPSHOT: // broadcast Snapshot
		typ, start, end := m.SnapshotType, m.SnapstartSeq, m.SnapendSeq
		c.Debugf(ssFormat, vr.logPrefix, start, end, typ)
		if data := vr.makeSnapshotData(m, seqno); data != nil {
			vr.broadcast2Endpoints(data)
		} else {
			c.Errorf("%v Snapshot NOT PUBLISHED\n", vr.logPrefix)
		}

	case mcd.UPR_MUTATION, mcd.UPR_DELETION, mcd.UPR_EXPIRATION:
		// sequence number gets incremented only here.
		seqno = m.Seqno
		// prepare a data for each endpoint.
		dataForEndpoints := make(map[string]interface{})
		// for each engine distribute transformations to endpoints.
		for _, engine := range vr.engines {
			err := engine.TransformRoute(vr.vbuuid, m, dataForEndpoints)
			if err != nil {
				c.Errorf("%v TransformRoute %v\n", vr.logPrefix, err)
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
					c.Errorf(msg, vr.logPrefix, raddr, err)
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
			c.Errorf(msg, vr.logPrefix, raddr, err)
			endpoint.Close()
			delete(vr.endpoints, raddr)
		}
	}
}

func (vr *VbucketRoutine) makeStreamBeginData(seqno uint64) interface{} {
	defer func() {
		if r := recover(); r != nil {
			c.Errorf("%v stream-begin crashed: %v\n", vr.logPrefix, r)
			c.StackTrace(string(debug.Stack()))
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
			c.Errorf("%v sync crashed: %v\n", vr.logPrefix, r)
			c.StackTrace(string(debug.Stack()))
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
	m *mc.UprEvent, seqno uint64) interface{} {

	defer func() {
		if r := recover(); r != nil {
			c.Errorf("%v snapshot crashed: %v\n", vr.logPrefix, r)
			c.StackTrace(string(debug.Stack()))
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
			c.Errorf("%v stream-end crashed: %v\n", vr.logPrefix, r)
			c.StackTrace(string(debug.Stack()))
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
			c.Tracef("%v knows endpoint %v\n", vr.logPrefix, raddr)
		}
	case map[uint64]*Engine:
		for uuid := range val {
			c.Tracef("%v knows engine %v\n", vr.logPrefix, uuid)
		}
	}
}
