// concurrency model:
//
//                                           V0 V1 V2
//                                            ^  ^  ^
//                                            |  |  |
//                                      Begin |  |  |
//                                   Mutation |  |  |
//                                   Deletion |  |  |
//                                       Sync |  |  |
//                      NewKVFeed()       End |  |  |
//                           |  |             |  |  |
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
	"fmt"
	c "github.com/couchbase/indexing/secondary/common"
	"log"
)

// error codes

// ErrorVBmap
var ErrorVBmap = fmt.Errorf("errorVBmap")

// ErrorClientExited
var ErrorClientExited = fmt.Errorf("errorClientExited")

// ErrorShiftingVbucket
var ErrorShiftingVbucket = fmt.Errorf("errorShiftingVbucket")

// KVFeed is per bucket, per node feed for a subset of vbuckets
type KVFeed struct {
	// immutable fields
	bfeed   *BucketFeed
	kvaddr  string // node address
	pooln   string
	bucketn string
	bucket  BucketAccess
	feeder  KVFeeder
	// gen-server
	reqch     chan []interface{}
	finch     chan bool
	logPrefix string
	// data path
	vbuckets map[uint16]*activeVbucket // mutable
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

// NewKVFeed create a new feed from `kvaddr` node for a single bucket. Uses
// couchbase client API to identify the subset of vbuckets mapped to this
// node.
//
// if error, KVFeed is not started
// - error returned by couchbase client
func NewKVFeed(bfeed *BucketFeed, kvaddr, pooln, bucketn string) (*KVFeed, error) {
	p := bfeed.getFeed().getProjector()
	bucket, err := p.getBucket(kvaddr, pooln, bucketn)
	if err != nil {
		return nil, err
	}
	feeder, err := bucket.OpenKVFeed(kvaddr)
	if err != nil {
		return nil, err
	}
	kvfeed := &KVFeed{
		bfeed:   bfeed,
		kvaddr:  kvaddr,
		pooln:   pooln,
		bucketn: bucketn,
		bucket:  bucket,
		feeder:  feeder.(KVFeeder),
		// gen-server
		reqch:    make(chan []interface{}, c.GenserverChannelSize),
		finch:    make(chan bool),
		vbuckets: make(map[uint16]*activeVbucket),
	}
	kvfeed.logPrefix = kvfeed.getLogPrefix(bfeed, kvaddr)

	sbch := make(chan []interface{}, c.GenserverChannelSize)
	go kvfeed.genServer(kvfeed.reqch, sbch)
	go kvfeed.runScatter(sbch)
	log.Printf("%v, started ...\n", kvfeed.logPrefix)
	return kvfeed, nil
}

func (kvfeed *KVFeed) getLogPrefix(bfeed *BucketFeed, kvaddr string) string {
	feed := bfeed.feed
	return fmt.Sprintf("kvfeed-feed %v:%v:%v", feed.topic, bfeed.bucketn, kvaddr)
}

// APIs to gen-server
const (
	kvfCmdRequestFeed byte = iota + 1
	kvfCmdUpdateEngines
	kvfCmdDeleteEngines
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
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{kvfCmdUpdateEngines, endpoints, engines, respch}
	resp, err := c.FailsafeOp(kvfeed.reqch, respch, cmd, kvfeed.finch)
	return c.OpError(err, resp, 0)
}

// DeleteEngines synchronous call.
func (kvfeed *KVFeed) DeleteEngines(endpoints map[string]*Endpoint, engines []uint64) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{kvfCmdDeleteEngines, endpoints, engines, respch}
	resp, err := c.FailsafeOp(kvfeed.reqch, respch, cmd, kvfeed.finch)
	return c.OpError(err, resp, 0)
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
			log.Printf("%v, gen-server paniced: %v\n", kvfeed.logPrefix, r)
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

		case kvfCmdUpdateEngines:
			endpoints := msg[1].(map[string]*Endpoint)
			engines := msg[2].(map[string]*Engine)
			respch := msg[3].(chan []interface{})
			kvfeed.sendSideband(sbkvUpdateEngines{endpoints, engines}, sbch)
			respch <- []interface{}{nil}

		case kvfCmdDeleteEngines:
			endpoints := msg[1].(map[string]*Endpoint)
			engines := msg[2].([]uint64)
			respch := msg[3].(chan []interface{})
			kvfeed.sendSideband(sbkvDeleteEngines{endpoints, engines}, sbch)
			respch <- []interface{}{nil}

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

	// fetch restart-timestamp from request
	feeder := kvfeed.feeder
	ts := req.RestartTimestamp(kvfeed.bucketn)
	if ts == nil {
		log.Printf("%v, error restartTimestamp is empty\n", kvfeed.logPrefix)
		return nil, nil, c.ErrorInvalidRequest
	}
	// fetch list of vbuckets mapped on this connection
	m, err := kvfeed.bucket.GetVBmap([]string{kvfeed.kvaddr})
	if err != nil {
		return nil, nil, err
	}
	vbnos := m[kvfeed.kvaddr]
	if vbnos == nil {
		return nil, nil, ErrorVBmap
	}

	// execute the request
	ts = ts.SelectByVbuckets(vbnos)
	if req.IsStart() { // start
		failTs, kvTs, err = feeder.StartVbStreams(ts)
	} else if req.IsRestart() { // restart implies a shutdown and start
		if err = feeder.EndVbStreams(ts); err == nil {
			failTs, kvTs, err = kvfeed.feeder.StartVbStreams(ts)
		}
	} else if req.IsShutdown() { // shutdown
		err = feeder.EndVbStreams(ts)
		failTs, kvTs = ts, ts
	} else {
		err = c.ErrorInvalidRequest
	}
	return
}

// execute close.
func (kvfeed *KVFeed) doClose() error {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("%v, doClose() paniced: %q\n", kvfeed.logPrefix, r)
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
	log.Printf("%v, ... closed\n", kvfeed.logPrefix)
	return nil
}

// synchronous call to update runScatter via side-band channel.
func (kvfeed *KVFeed) sendSideband(info interface{}, sbch chan []interface{}) {
	respch := make(chan []interface{}, 1)
	c.FailsafeOp(sbch, respch, []interface{}{info, respch}, kvfeed.finch)
}

// routine handles data path, never panics.
func (kvfeed *KVFeed) runScatter(sbch chan []interface{}) {
	mutch := kvfeed.feeder.GetChannel()
	var endpoints map[string]*Endpoint
	var engines map[uint64]*Engine

loop:
	for {
		select {
		case m, ok := <-mutch: // mutation from upstream
			if ok == false {
				kvfeed.CloseFeed()
				break loop
			}
			kvfeed.scatterMutation(m, endpoints, engines)

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
			fmtstr := "%v, error duplicate OpStreamBegin for %v\n"
			log.Printf(fmtstr, kvfeed.logPrefix, m.Vbucket)
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
			fmtstr := "%v, error duplicate OpStreamEnd for %v\n"
			log.Printf(fmtstr, kvfeed.logPrefix, m.Vbucket)
		} else {
			v.vr.Close()
			delete(kvfeed.vbuckets, vbno)
		}

	case OpMutation, OpDeletion:
		if v, ok := kvfeed.vbuckets[vbno]; ok {
			if v.vbuuid != m.Vbuuid {
				fmtstr := "%v, error vbuuid mismatch for vbucket %v\n"
				log.Printf(fmtstr, kvfeed.logPrefix, m.Vbucket)
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
