// concurrency model:
//
//                                           V0 V1    Vn
//                                            ^  ^ ..  ^
//                                            |  |     |
//                                      Begin |  |     |
//                                   Mutation |  |     |
//                                   Deletion |  |     |
//                                       Sync |  |     |
//                      NewKVFeed()       End |  |     |
//                           |  |             |  |     |
//                          (spawn)         MutationEvent
//                           |  |             |
//                           |  *----------- runScatter()
//                           |                ^     ^
//                           |                |     |
//    RequestFeed() -----*-> genServer()--- sbch    *-- vbucket stream
//             |         |      ^                   |
//  <--failTs,kvTs       |      |                   *-- vbucket stream
//                       |      |                   |
//    CloseFeed() -------*      |                   *-- vbucket stream
//                              |                   |
//                              *------------> couchbase-client
//
// Notes:
//
// - new kv-feed spawns a gen-server routine for control path and
//   gather-scatter routine for data path.
// - RequestFeed can start, restart or shutdown one or more vbuckets.
// - for a successful RequestFeed,
//   - failover-timestamp, restart-timestamp must contain timestamp for
//     "active vbuckets".
//   - if request is to shutdown vbuckets, failover-timetamp and
//     restart-timetamp will be empty.
//   - StreamBegin and StreamEnd events are gauranteed by couchbase-client.
// - for idle vbuckets periodic Sync events will be published downstream.
// - KVFeed will be closed, notifying downstream component with,
//   - nil, when downstream component does CloseFeed()
//   - ErrorClientExited, when upstream closes the mutation channel
//   - ErrorShiftingVbucket, when vbuckets are shifting

package projector

import (
	"errors"
	"fmt"
	c "github.com/couchbase/indexing/secondary/common"
)

// error codes

// ErrorVBmap
var ErrorVBmap = errors.New("kvfeed.vbmap")

// ErrorClientExited
var ErrorClientExited = errors.New("kvfeed.clientExited")

// ErrorShiftingVbucket
var ErrorShiftingVbucket = errors.New("kvfeed.shiftingVbucket")

// KVFeed is per bucket, per node feed for a subset of vbuckets
type KVFeed struct {
	// immutable fields
	bfeed   *BucketFeed
	kvaddr  string // node address
	pooln   string
	bucketn string
	bucket  BucketAccess
	feeder  KVFeeder
	// data path
	vbuckets map[uint16]*activeVbucket // mutable
	// gen-server
	reqch chan []interface{}
	sbch  chan []interface{}
	finch chan bool
	// misc.
	logPrefix string
	stats     *c.ComponentStat
}

type activeVbucket struct {
	bucket string
	vbno   uint16
	vbuuid uint64
	seqno  uint64
	vr     *VbucketRoutine
}

type sbkvUpdateEngines []interface{}
type sbkvDeleteEngines []interface{}
type sbkvGetStatistics []interface{}

// NewKVFeed create a new feed from `kvaddr` node for a single bucket. Uses
// couchbase client API to identify the subset of vbuckets mapped to this
// node.
//
// if error, KVFeed is not started
// - error returned by couchbase client
func NewKVFeed(bfeed *BucketFeed, kvaddr, pooln, bucketn string) (*KVFeed, error) {
	feed := bfeed.getFeed()
	logPrefix := fmt.Sprintf("[kvfeed %v:%v:%v]", feed.topic, bucketn, kvaddr)

	p := feed.getProjector()
	bucket, err := p.getBucket(kvaddr, pooln, bucketn)
	if err != nil {
		c.Errorf("%v getBucket(): %v\n", logPrefix, err)
		return nil, err
	}
	feeder, err := bucket.OpenKVFeed(kvaddr)
	if err != nil {
		c.Errorf("%v OpenKVFeed(): %v\n", logPrefix, err)
		return nil, err
	}
	kvfeed := &KVFeed{
		bfeed:   bfeed,
		kvaddr:  kvaddr,
		pooln:   pooln,
		bucketn: bucketn,
		bucket:  bucket,
		feeder:  feeder.(KVFeeder),
		// data-path
		vbuckets: make(map[uint16]*activeVbucket),
		// gen-server
		reqch: make(chan []interface{}, c.GenserverChannelSize),
		sbch:  make(chan []interface{}, c.GenserverChannelSize),
		finch: make(chan bool),
		// misc.
		logPrefix: logPrefix,
	}
	kvfeed.stats = kvfeed.newStats()

	go kvfeed.genServer(kvfeed.reqch, kvfeed.sbch)
	go kvfeed.runScatter(kvfeed.sbch)
	c.Infof("%v started ...\n", kvfeed.logPrefix)
	return kvfeed, nil
}

// APIs to gen-server
const (
	kvfCmdRequestFeed byte = iota + 1
	kvfCmdGetStatistics
	kvfCmdCloseFeed
)

// RequestFeed synchronous call.
//
// returns failover-timetamp and kv-timestamp.
// - ErrorInvalidRequest if request is malformed.
// - error returned by couchbase client.
// - error if KVFeed is already closed.
func (kvfeed *KVFeed) RequestFeed(
	req RequestReader,
	endpoints map[string]*Endpoint,
	engines map[uint64]*Engine) (failoverTs, kvTs *c.Timestamp, err error) {

	if req == nil {
		return nil, nil, ErrorArgument
	}
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{kvfCmdRequestFeed, req, endpoints, engines, respch}
	resp, err := c.FailsafeOp(kvfeed.reqch, respch, cmd, kvfeed.finch)
	if err = c.OpError(err, resp, 2); err != nil {
		return nil, nil, err
	}
	failoverTs, kvTs = resp[0].(*c.Timestamp), resp[1].(*c.Timestamp)
	return failoverTs, kvTs, nil
}

// UpdateEngines synchronous call.
func (kvfeed *KVFeed) UpdateEngines(endpoints map[string]*Endpoint, engines map[uint64]*Engine) error {
	kvfeed.sendSideband(sbkvUpdateEngines{endpoints, engines}, kvfeed.sbch)
	return nil
}

// DeleteEngines synchronous call.
func (kvfeed *KVFeed) DeleteEngines(endpoints map[string]*Endpoint, engines []uint64) error {
	kvfeed.sendSideband(sbkvDeleteEngines{endpoints, engines}, kvfeed.sbch)
	return nil
}

// GetStatistics will recursively get statistics for kv-feed and its
// underlying workers.
func (kvfeed *KVFeed) GetStatistics() map[string]interface{} {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{kvfCmdGetStatistics, respch}
	resp, _ := c.FailsafeOp(kvfeed.reqch, respch, cmd, kvfeed.finch)
	return resp[1].(map[string]interface{})
}

// CloseFeed synchronous call.
func (kvfeed *KVFeed) CloseFeed() error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{kvfCmdCloseFeed, respch}
	resp, err := c.FailsafeOp(kvfeed.reqch, respch, cmd, kvfeed.finch)
	return c.OpError(err, resp, 0)
}

// routine handles control path.
func (kvfeed *KVFeed) genServer(reqch chan []interface{}, sbch chan []interface{}) {
	defer func() { // panic safe
		if r := recover(); r != nil {
			c.Errorf("%v paniced: %v !\n", kvfeed.logPrefix, r)
			kvfeed.doClose()
		}
		close(sbch)
	}()

loop:
	for {
		msg := <-reqch
		switch msg[0].(byte) {
		case kvfCmdRequestFeed:
			req := msg[1].(RequestReader)
			endpoints := msg[2].(map[string]*Endpoint)
			engines := msg[3].(map[uint64]*Engine)
			respch := msg[4].(chan []interface{})
			kvfeed.sendSideband(sbkvUpdateEngines{endpoints, engines}, sbch)
			failTs, kvTs, err := kvfeed.requestFeed(req, endpoints, engines)
			respch <- []interface{}{failTs, kvTs, err}

		case kvfCmdGetStatistics:
			respch := msg[1].(chan []interface{})
			resp := kvfeed.sendSideband(sbkvGetStatistics{}, sbch)
			for name, val := range resp[0].(map[string]interface{}) {
				kvfeed.stats.Set("/"+name, val)
			}
			for vbno, v := range kvfeed.vbuckets {
				s := fmt.Sprintf("%v", vbno)
				kvfeed.stats.Set("/"+s, v.vr.GetStatistics())
			}
			respch <- []interface{}{kvfeed.stats.ToMap()}

		case kvfCmdCloseFeed:
			respch := msg[1].(chan []interface{})
			respch <- []interface{}{kvfeed.doClose()}
			break loop
		}
	}
}

// start, restart or shutdown streams
func (kvfeed *KVFeed) requestFeed(
	req RequestReader,
	endpoints map[string]*Endpoint,
	engines map[uint64]*Engine) (failTs, kvTs *c.Timestamp, err error) {

	prefix := kvfeed.logPrefix

	// fetch restart-timestamp from request
	feeder := kvfeed.feeder
	ts := req.RestartTimestamp(kvfeed.bucketn)
	if ts == nil {
		c.Errorf("%v restartTimestamp is empty\n", prefix)
		return nil, nil, c.ErrorInvalidRequest
	}
	// fetch list of vbuckets mapped on this connection
	m, err := kvfeed.bucket.GetVBmap([]string{kvfeed.kvaddr})
	if err != nil {
		c.Errorf("%v bucket.GetVBmap() %v \n", prefix, err)
		return nil, nil, err
	}
	vbnos := m[kvfeed.kvaddr]
	if vbnos == nil {
		return nil, nil, ErrorVBmap
	}

	// execute the request
	ts = ts.SelectByVbuckets(vbnos)
	if req.IsStart() { // start
		if failTs, kvTs, err = feeder.StartVbStreams(ts); err != nil {
			c.Errorf("%v feeder.StartVbStreams() %v", prefix, err)
		}
	} else if req.IsRestart() { // restart implies a shutdown and start
		if err = feeder.EndVbStreams(ts); err == nil {
			if failTs, kvTs, err = feeder.StartVbStreams(ts); err != nil {
				c.Errorf("%v feeder.StartVbStreams() %v", prefix, err)
			}
		} else {
			c.Errorf("%v feeder.EndVbStreams() %v", prefix, err)
		}
	} else if req.IsShutdown() { // shutdown
		if err = feeder.EndVbStreams(ts); err != nil {
			c.Errorf("%v feeder.EndVbStreams() %v", prefix, err)
		}
		failTs, kvTs = ts, ts
	} else {
		err = c.ErrorInvalidRequest
		c.Errorf("%v %v", prefix, err)
	}
	return
}

// execute close.
func (kvfeed *KVFeed) doClose() error {
	defer func() {
		if r := recover(); r != nil {
			c.Errorf("%v doClose() paniced: %v !\n", kvfeed.logPrefix, r)
		}
	}()

	// close vbucket routines
	for _, v := range kvfeed.vbuckets {
		v.vr.Close()
	}
	kvfeed.vbuckets = nil
	// close upstream
	kvfeed.feeder.CloseKVFeed()
	close(kvfeed.finch)
	c.Infof("%v ... stopped\n", kvfeed.logPrefix)
	return nil
}

// synchronous call to update runScatter via side-band channel.
func (kvfeed *KVFeed) sendSideband(info interface{}, sbch chan []interface{}) []interface{} {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{info, respch}
	resp, _ := c.FailsafeOp(sbch, respch, cmd, kvfeed.finch)
	return resp
}

// routine handles data path, never panics.
func (kvfeed *KVFeed) runScatter(sbch chan []interface{}) {
	mutch := kvfeed.feeder.GetChannel()
	var endpoints map[string]*Endpoint
	var engines map[uint64]*Engine

	mutations := 0
loop:
	for {
		select {
		case m, ok := <-mutch: // mutation from upstream
			if ok == false {
				kvfeed.CloseFeed()
				break loop
			}
			kvfeed.scatterMutation(m, endpoints, engines)
			mutations++

		case msg, ok := <-sbch:
			if ok == false {
				break loop
			}
			info := msg[0]
			respch := msg[1].(chan []interface{})
			switch vals := info.(type) {
			case sbkvUpdateEngines:
				endpoints = vals[1].(map[string]*Endpoint)
				engines = vals[2].(map[uint64]*Engine)
				for _, v := range kvfeed.vbuckets {
					v.vr.UpdateEngines(endpoints, engines)
				}

			case sbkvDeleteEngines:
				endpoints = vals[1].(map[string]*Endpoint)
				engineKeys := vals[2].([]uint64)
				for _, v := range kvfeed.vbuckets {
					v.vr.DeleteEngines(endpoints, engineKeys)
				}
				for _, engineKey := range engineKeys {
					delete(engines, engineKey)
				}
			case sbkvGetStatistics:
				stats := map[string]interface{}{
					"mutations": mutations,
				}
				respch <- []interface{}{stats}
			}
			respch <- []interface{}{nil}

		}
	}
}

// scatterMutation to vbuckets.
func (kvfeed *KVFeed) scatterMutation(m *MutationEvent, endpoints map[string]*Endpoint, engines map[uint64]*Engine) {
	vbno := m.Vbucket

	switch m.Opcode {
	case OpStreamBegin:
		if _, ok := kvfeed.vbuckets[vbno]; ok {
			fmtstr := "%v, duplicate OpStreamBegin for %v\n"
			c.Errorf(fmtstr, kvfeed.logPrefix, m.Vbucket)
		} else {
			vr := NewVbucketRoutine(kvfeed, kvfeed.bucketn, vbno, m.Vbuuid)
			vr.UpdateEngines(endpoints, engines)
			kvfeed.vbuckets[vbno] = &activeVbucket{
				bucket: kvfeed.bucketn,
				vbno:   vbno,
				vbuuid: m.Vbuuid,
				seqno:  m.Seqno,
				vr:     vr,
			}
			vr.Event(m)
		}

	case OpStreamEnd:
		if v, ok := kvfeed.vbuckets[vbno]; !ok {
			fmtstr := "%v, duplicate OpStreamEnd for %v\n"
			c.Errorf(fmtstr, kvfeed.logPrefix, m.Vbucket)
		} else {
			v.vr.Close()
			delete(kvfeed.vbuckets, vbno)
		}

	case OpMutation, OpDeletion:
		if v, ok := kvfeed.vbuckets[vbno]; ok {
			if v.vbuuid != m.Vbuuid {
				fmtstr := "%v, vbuuid mismatch for vbucket %v\n"
				c.Errorf(fmtstr, kvfeed.logPrefix, m.Vbucket)
				v.vr.Close()
				delete(kvfeed.vbuckets, vbno)
			} else {
				v.vr.Event(m)
				v.vbuuid, v.seqno = m.Vbuuid, m.Seqno
			}
		}
	}
	return
}
