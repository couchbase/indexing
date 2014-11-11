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

import mcd "github.com/couchbase/gomemcached"
import mc "github.com/couchbase/gomemcached/client"
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
	syncTimeout time.Duration
	logPrefix   string
}

// NewVbucketRoutine creates a new routine to handle this vbucket stream.
func NewVbucketRoutine(
	topic, bucket, kvaddr string,
	vbno uint16, vbuuid, startSeqno uint64, config c.Config) *VbucketRoutine {

	mutChanSize := config["projector.mutationChanSize"].Int()
	syncTimeout := time.Duration(config["projector.vbucketSyncTimeout"].Int())

	vr := &VbucketRoutine{
		bucket:    bucket,
		vbno:      vbno,
		vbuuid:    vbuuid,
		engines:   make(map[uint64]*Engine),
		endpoints: make(map[string]c.RouterEndpoint),
		reqch:     make(chan []interface{}, mutChanSize),
		finch:     make(chan bool),
	}
	vr.logPrefix = fmt.Sprintf("[%v->%v->%v->%v]", topic, bucket, kvaddr, vbno)
	vr.mutChanSize = mutChanSize
	vr.syncTimeout = syncTimeout

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
			vr.sendToEndpoints(data)
		}

		close(vr.finch)
		c.Infof("%v ... stopped\n", vr.logPrefix)
	}()

	var heartBeat <-chan time.Time // for Sync message

	stats := vr.newStats()
	addEngineCount := stats.Get("addEngines").(float64)
	delEngineCount := stats.Get("delEngines").(float64)
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
				c.Debugf("%v vrCmdAddEngines\n", vr.logPrefix)
				vr.engines = make(map[uint64]*Engine)
				if msg[1] != nil {
					for uuid, engine := range msg[1].(map[uint64]*Engine) {
						vr.engines[uuid] = engine
						c.Debugf("%v AddEngine %v\n", vr.logPrefix, uuid)
					}
					vr.printCtrl(vr.engines)
				}

				if msg[2] != nil {
					vr.updateEndpoints(msg[2].(map[string]c.RouterEndpoint))
					vr.printCtrl(vr.endpoints)
				}
				respch := msg[3].(chan []interface{})
				respch <- []interface{}{nil}
				addEngineCount++

			case vrCmdDeleteEngines:
				c.Debugf("%v vrCmdDeleteEngines\n", vr.logPrefix)
				engineKeys := msg[1].([]uint64)
				for _, uuid := range engineKeys {
					delete(vr.engines, uuid)
					c.Debugf("%v DelEngine %v\n", vr.logPrefix, uuid)
				}

				c.Debugf("%v deleted engines %v\n", engineKeys)
				respch := msg[2].(chan []interface{})
				respch <- []interface{}{nil}
				delEngineCount++

			case vrCmdGetStatistics:
				c.Debugf("%v vrCmdStatistics\n", vr.logPrefix)
				respch := msg[1].(chan []interface{})
				stats.Set("addEngines", addEngineCount)
				stats.Set("delEngines", delEngineCount)
				stats.Set("syncs", syncCount)
				stats.Set("snapshots", sshotCount)
				stats.Set("mutations", mutationCount)
				respch <- []interface{}{stats.ToMap()}

			case vrCmdEvent:
				m := msg[1].(*mc.UprEvent)
				if m.Opcode == mcd.UPR_STREAMREQ { // opens up the path
					heartBeat = time.Tick(vr.syncTimeout * time.Millisecond)
					format := "%v heartbeat (%v) loaded ...\n"
					c.Debugf(format, vr.logPrefix, vr.syncTimeout)
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
				vr.sendToEndpoints(data)

			} else {
				c.Errorf("%v Sync NOT PUBLISHED\n", vr.logPrefix)
			}
		}
	}
}

// only endpoints that host engines defined on this vbucket.
func (vr *VbucketRoutine) updateEndpoints(eps map[string]c.RouterEndpoint) {
	vr.endpoints = make(map[string]c.RouterEndpoint)
	for _, engine := range vr.engines {
		for _, raddr := range engine.Endpoints() {
			if _, ok := eps[raddr]; !ok {
				msg := "%v endpoint %v not found\n"
				c.Errorf(msg, vr.logPrefix, raddr)
			}
			c.Debugf("%v UpdateEndpoints %v\n", vr.logPrefix, raddr)
			vr.endpoints[raddr] = eps[raddr]
		}
	}
}

var ssMsg = "%v received snapshot %v %v (type %x)\n"
var traceMutMsg = "%v UprEvent %v:%v <<%v>>\n"

func (vr *VbucketRoutine) handleEvent(m *mc.UprEvent, seqno uint64) uint64 {
	c.Tracef(traceMutMsg, vr.logPrefix, m.Seqno, m.Opcode, m.Key)

	switch m.Opcode {
	case mcd.UPR_STREAMREQ: // broadcast StreamBegin
		if data := vr.makeStreamBeginData(seqno); data != nil {
			vr.sendToEndpoints(data)
		} else {
			c.Errorf("%v StreamBeginData NOT PUBLISHED\n", vr.logPrefix)
		}

	case mcd.UPR_SNAPSHOT: // broadcast Snapshot
		typ, start, end := m.SnapshotType, m.SnapstartSeq, m.SnapendSeq
		c.Debugf(ssMsg, vr.logPrefix, typ, start, end)
		if data := vr.makeSnapshotData(m, seqno); data != nil {
			vr.sendToEndpoints(data)
		} else {
			c.Errorf("%v Snapshot NOT PUBLISHED\n", vr.logPrefix)
		}

	case mcd.UPR_MUTATION, mcd.UPR_DELETION, mcd.UPR_EXPIRATION:
		// sequence number gets incremented only here.
		seqno = m.Seqno
		// prepare a data for each endpoint.
		dataForEndpoints := make(map[string]interface{})
		for raddr := range vr.endpoints {
			dataForEndpoints[raddr] = nil
		}
		// for each engine distribute transformations to endpoints.
		for _, engine := range vr.engines {
			edata, err := engine.TransformRoute(vr.vbuuid, m)
			if err != nil {
				c.Errorf("%v TransformRoute %v\n", vr.logPrefix, err)
				continue
			}
			// send data to corresponding endpoint.
			for raddr, data := range edata {
				// send might fail, we don't care
				vr.endpoints[raddr].Send(data)
			}
		}
	}
	return seqno
}

// send to all endpoints.
func (vr *VbucketRoutine) sendToEndpoints(data interface{}) {
	for _, endpoint := range vr.endpoints {
		// send might fail, we don't care
		endpoint.Send(data)
	}
}

func (vr *VbucketRoutine) printCtrl(v interface{}) {
	switch val := v.(type) {
	case map[string]c.RouterEndpoint:
		for raddr := range val {
			c.Debugf("%v knows endpoint %v\n", vr.logPrefix, raddr)
		}
	case map[uint64]*Engine:
		for uuid := range val {
			c.Debugf("%v knows engine %v\n", vr.logPrefix, uuid)
		}
	}
}

func (vr *VbucketRoutine) newStats() c.Statistics {
	m := map[string]interface{}{
		"addEngines": float64(0), // no. of update-engine commands
		"delEngines": float64(0), // no. of delete-engine commands
		"syncs":      float64(0), // no. of Sync message generated
		"snapshots":  float64(0), // no. of Begin
		"mutations":  float64(0), // no. of Upsert, Delete
	}
	stats, _ := c.NewStatistics(m)
	return stats
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
