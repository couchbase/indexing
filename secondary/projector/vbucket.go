// concurrency model:
//
//                           NewVbucketRoutine()
//                                   |
//                                   |               *---> endpoint
//                                (spawn)            |
//                                   |               *---> endpoint
//             Event() --*           |               |
//                       |--------> run -------------*---> endpoint
//     UpdateEngines() --*
//                       |
//     DeleteEngines() --*
//                       |
//             Close() --*

package projector

import (
	"fmt"
	mc "github.com/couchbase/gomemcached/client"
	c "github.com/couchbase/indexing/secondary/common"
	"time"
)

// VbucketRoutine is immutable structure defined for each vbucket.
type VbucketRoutine struct {
	kvfeed *KVFeed // immutable
	bucket string  // immutable
	vbno   uint16  // immutable
	vbuuid uint64  // immutable
	// gen-server
	reqch chan []interface{}
	finch chan bool
	// misc.
	logPrefix string
	stats     c.Statistics
}

// NewVbucketRoutine creates a new routine to handle this vbucket stream.
func NewVbucketRoutine(kvfeed *KVFeed, bucket string, vbno uint16, vbuuid uint64) *VbucketRoutine {
	vr := &VbucketRoutine{
		kvfeed: kvfeed,
		bucket: bucket,
		vbno:   vbno,
		vbuuid: vbuuid,
		reqch:  make(chan []interface{}, c.MutationChannelSize),
		finch:  make(chan bool),
	}
	vr.logPrefix = vr.getLogPrefix(kvfeed, vbno)
	vr.stats = vr.newStats()

	go vr.run(vr.reqch, nil, nil)
	c.Infof("%v ... started\n", vr.logPrefix)
	return vr
}

func (vr *VbucketRoutine) getLogPrefix(kvfeed *KVFeed, vbno uint16) string {
	bfeed := kvfeed.bfeed
	feed := bfeed.feed
	return fmt.Sprintf("[vb %v:%v:%v]", feed.topic, bfeed.bucketn, vbno)
}

const (
	vrCmdEvent byte = iota + 1
	vrCmdUpdateEngines
	vrCmdDeleteEngines
	vrCmdGetStatistics
	vrCmdClose
)

// Event will post an UprEvent, asychronous call.
func (vr *VbucketRoutine) Event(m *mc.UprEvent) error {
	if m == nil {
		return ErrorArgument
	}
	var respch chan []interface{}
	cmd := []interface{}{vrCmdEvent, m}
	_, err := c.FailsafeOp(vr.reqch, respch, cmd, vr.finch)
	return err
}

// UpdateEngines update active set of engines and endpoints, synchronous call.
func (vr *VbucketRoutine) UpdateEngines(endpoints map[string]*Endpoint, engines map[uint64]*Engine) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{vrCmdUpdateEngines, endpoints, engines, respch}
	_, err := c.FailsafeOp(vr.reqch, respch, cmd, vr.finch)
	return err
}

// DeleteEngines delete engines and update endpoints, synchronous call.
func (vr *VbucketRoutine) DeleteEngines(endpoints map[string]*Endpoint, engines []uint64) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{vrCmdDeleteEngines, endpoints, engines, respch}
	_, err := c.FailsafeOp(vr.reqch, respch, cmd, vr.finch)
	return err
}

// GetStatistics for this vbucket.
func (vr *VbucketRoutine) GetStatistics() map[string]interface{} {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{vrCmdGetStatistics, respch}
	resp, _ := c.FailsafeOp(vr.reqch, respch, cmd, vr.finch)
	return resp[0].(map[string]interface{})
}

// Close this vbucket routine and free its resources, synchronous call.
func (vr *VbucketRoutine) Close() error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{vrCmdClose, respch}
	resp, err := c.FailsafeOp(vr.reqch, respch, cmd, vr.finch) // synchronous call
	return c.OpError(err, resp, 0)
}

// routine handles data path for a single vbucket, never panics.
// TODO: statistics on data path must be fast.
func (vr *VbucketRoutine) run(reqch chan []interface{}, endpoints map[string]*Endpoint, engines map[uint64]*Engine) {
	var seqno uint64
	var heartBeat <-chan time.Time

	stats := vr.stats
	uEngineCount := stats.Get("uEngines").(float64)
	dEngineCount := stats.Get("dEngines").(float64)
	beginCount := stats.Get("begins").(float64)
	mutationCount := stats.Get("mutations").(float64)
	syncCount := stats.Get("syncs").(float64)

loop:
	for {
		select {
		case msg := <-reqch:
			cmd := msg[0].(byte)
			switch cmd {
			case vrCmdUpdateEngines:
				endpoints = msg[1].(map[string]*Endpoint)
				engines = msg[2].(map[uint64]*Engine)
				respch := msg[3].(chan []interface{})
				respch <- []interface{}{nil}
				uEngineCount++
				vr.traceCtrlPath(endpoints, engines)

			case vrCmdDeleteEngines:
				endpoints = msg[1].(map[string]*Endpoint)
				for _, uuid := range msg[2].([]uint64) {
					delete(engines, uuid)
				}
				respch := msg[3].(chan []interface{})
				respch <- []interface{}{nil}
				dEngineCount++
				vr.traceCtrlPath(endpoints, engines)

			case vrCmdGetStatistics:
				respch := msg[1].(chan []interface{})
				stats.Set("uEngines", uEngineCount)
				stats.Set("dEngines", dEngineCount)
				stats.Set("begins", beginCount)
				stats.Set("mutations", mutationCount)
				stats.Set("syncs", syncCount)
				respch <- []interface{}{stats.ToMap()}

			case vrCmdEvent:
				m := msg[1].(*mc.UprEvent)
				seqno = m.Seqno
				// broadcast StreamBegin
				switch m.Opcode {
				case mc.UprStreamRequest:
					vr.sendToEndpoints(endpoints, func() *c.KeyVersions {
						kv := c.NewKeyVersions(seqno, m.Key, 1)
						kv.AddStreamBegin()
						return kv
					})
					tickTs := c.VbucketSyncTimeout * time.Millisecond
					heartBeat = time.Tick(tickTs)
					beginCount++
					break // breaks out of select{}
				}

				// prepare a KeyVersions for each endpoint.
				kvForEndpoints := make(map[string]*c.KeyVersions)
				for raddr := range endpoints {
					kv := c.NewKeyVersions(seqno, m.Key, len(engines))
					kvForEndpoints[raddr] = kv
				}
				// for each engine populate endpoint KeyVersions.
				for _, engine := range engines {
					engine.AddToEndpoints(m, kvForEndpoints)
				}
				// send kv to corresponding endpoint
				for raddr, kv := range kvForEndpoints {
					if kv.Length() == 0 {
						continue
					}
					// send might fail, we don't care
					endpoints[raddr].Send(vr.bucket, vr.vbno, vr.vbuuid, kv)
				}
				mutationCount++

			case vrCmdClose:
				respch := msg[1].(chan []interface{})
				vr.doClose(seqno, endpoints)
				respch <- []interface{}{nil}
				break loop
			}

		case <-heartBeat:
			if endpoints != nil {
				vr.sendToEndpoints(endpoints, func() *c.KeyVersions {
					kv := c.NewKeyVersions(seqno, nil, 1)
					kv.AddSync()
					return kv
				})
				syncCount++
			}
		}
	}
}

// close this vbucket routine
func (vr *VbucketRoutine) doClose(seqno uint64, endpoints map[string]*Endpoint) {
	vr.sendToEndpoints(endpoints, func() *c.KeyVersions {
		kv := c.NewKeyVersions(seqno, nil, 1)
		kv.AddStreamEnd()
		return kv
	})
	close(vr.finch)
	c.Infof("%v ... stopped\n", vr.logPrefix)
}

// send to all endpoints
func (vr *VbucketRoutine) sendToEndpoints(endpoints map[string]*Endpoint, fn func() *c.KeyVersions) {
	for _, endpoint := range endpoints {
		kv := fn()
		// send might fail, we don't care
		endpoint.Send(vr.bucket, vr.vbno, vr.vbuuid, kv)
	}
}

func (vr *VbucketRoutine) traceCtrlPath(endpoints map[string]*Endpoint, engines map[uint64]*Engine) {
	for raddr := range endpoints {
		c.Tracef("%v, knows enpdoint %q\n", vr.logPrefix, raddr)
	}
	for uuid := range engines {
		c.Tracef("%v, knows engine %q\n", vr.logPrefix, uuid)
	}
}
