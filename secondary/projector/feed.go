// Feed is the central program around which adminport, bucket_feed, kvfeed,
// engines and endpoint algorithms are organized.

package projector

import (
	"errors"
	"fmt"
	c "github.com/couchbase/indexing/secondary/common"
	"log"
	"sort"
)

// error codes

// ErrorInvalidBucket
var ErrorInvalidBucket = errors.New("feed.invalidBucket")

// ErrorRequestNotSubscriber
var ErrorRequestNotSubscriber = errors.New("feed.requestNotSubscriber")

// ErrorStartingEndpoint
var ErrorStartingEndpoint = errors.New("feed.startingEndpoint")

// Feed is mutation stream - for maintenance, initial-load, catchup etc...
type Feed struct {
	projector *Projector // immutable
	topic     string     // immutable
	// manage upstream
	bfeeds map[string]*BucketFeed // immutable and indexed by bucket name
	// manage downstream
	endpoints map[string]*Endpoint // mutable, indexed by remote-address
	engines   map[uint64]*Engine   // immutable, indexed by uuid
	// timestamp feedback
	failoverTimestamps map[string]*c.Timestamp // indexed by bucket name
	kvTimestamps       map[string]*c.Timestamp // indexed by bucket name
	// gen-server
	reqch     chan []interface{}
	finch     chan bool
	logPrefix string
}

// NewFeed creates a new instance of mutation stream for specified topic. Spawns
// a routine for gen-server
//
// if error, Feed is not created.
// - error returned by couchbase client
func NewFeed(p *Projector, topic string, request RequestReader) (*Feed, error) {
	var feed *Feed

	pools := request.GetPools()
	buckets := request.GetBuckets()

	feed = &Feed{
		projector:          p,
		topic:              topic,
		bfeeds:             make(map[string]*BucketFeed),
		failoverTimestamps: make(map[string]*c.Timestamp),
		kvTimestamps:       make(map[string]*c.Timestamp),
		endpoints:          make(map[string]*Endpoint),
		engines:            make(map[uint64]*Engine),
		reqch:              make(chan []interface{}, c.GenserverChannelSize),
		finch:              make(chan bool),
		logPrefix:          fmt.Sprintf("feed %v", topic),
	}
	// fresh start of a mutation stream.
	for i, bucket := range buckets {
		// bucket-feed
		kvaddrs := p.getKVNodes()
		bfeed, err := NewBucketFeed(feed, kvaddrs, pools[i], bucket)
		if err != nil {
			feed.doClose()
			return nil, err
		}
		// initialse empty Timestamps objects for return values.
		feed.failoverTimestamps[bucket] = c.NewTimestamp(bucket, c.MaxVbuckets)
		feed.kvTimestamps[bucket] = c.NewTimestamp(bucket, c.MaxVbuckets)
		feed.bfeeds[bucket] = bfeed
	}
	go feed.genServer(feed.reqch)
	log.Printf("%v, initialized ...\n", feed.logPrefix)
	return feed, nil
}

func (feed *Feed) getProjector() *Projector {
	return feed.projector
}

// gen-server API commands
const (
	fCmdRequestFeed byte = iota + 1
	fCmdUpdateFeed
	fCmdUpdateEngines
	fCmdDeleteEngines
	fCmdRepairEndpoints
	fCmdCloseFeed
)

// RequestFeed to start a new mutation stream, synchronous call.
//
// if error is returned then upstream instances of BucketFeed and KVFeed are
// shutdown and application must retry the request or fall-back.
// - ErrorInvalidRequest if request is malformed.
// - error returned by couchbase client.
// - error if KVFeed is already closed.
func (feed *Feed) RequestFeed(request RequestReader) error {
	if request == nil {
		return ErrorArgument
	} else if _, ok := request.(Subscriber); ok == false {
		return ErrorRequestNotSubscriber
	}

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdRequestFeed, request, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return c.OpError(err, resp, 0)
}

// UpdateFeed will start / restart / shutdown upstream vbuckets and update
// downstream engines and endpoints, synchronous call.
//
// returns failover-timetamp and kv-timestamp (restart seqno. after honoring
// rollback)
// - ErrorInvalidRequest if request is malformed.
// - error returned by couchbase client.
// - error if Feed is already closed.
func (feed *Feed) UpdateFeed(request RequestReader) error {
	if request == nil {
		return ErrorArgument
	} else if _, ok := request.(Subscriber); ok == false {
		return ErrorRequestNotSubscriber
	}
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdUpdateFeed, request, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return c.OpError(err, resp, 0)
}

// UpdateEngines will update active engines for this Feed. This happens when
// routing algorithm is affected or topology changes for one or more entities,
// synchronous call.
//
// - error if Feed is already closed.
func (feed *Feed) UpdateEngines(request Subscriber) error {
	if request == nil {
		return ErrorArgument
	}
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdUpdateEngines, request, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return c.OpError(err, resp, 0)
}

// DeleteEngines from active set of engines for this Feed. This happens when
// one or more entities are deleted, synchronous call.
//
// - error if Feed is already closed.
func (feed *Feed) DeleteEngines(request Subscriber) error {
	if request == nil {
		return ErrorArgument
	}
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdDeleteEngines, request, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return c.OpError(err, resp, 0)
}

// RepairEndpoints will restart downstream endpoints if it is already dead,
// synchronous call.
func (feed *Feed) RepairEndpoints() error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdRepairEndpoints, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return c.OpError(err, resp, 0)
}

// CloseFeed will shutdown this feed and upstream and downstream instances,
// synchronous call.
func (feed *Feed) CloseFeed() error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdCloseFeed, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return c.OpError(err, resp, 0)
}

func (feed *Feed) genServer(reqch chan []interface{}) {
	defer func() { // panic safe
		if r := recover(); r != nil {
			log.Printf("Feed:genServer() crashed `%v`\n", r)
			feed.doClose()
		}
	}()

loop:
	for {
		msg := <-reqch
		switch msg[0].(byte) {
		case fCmdRequestFeed:
			req, respch := msg[1].(RequestReader), msg[2].(chan []interface{})
			respch <- []interface{}{feed.requestFeed(req)}

		case fCmdUpdateFeed:
			req, respch := msg[1].(RequestReader), msg[2].(chan []interface{})
			respch <- []interface{}{feed.updateFeed(req)}

		case fCmdUpdateEngines:
			req, respch := msg[1].(Subscriber), msg[2].(chan []interface{})
			respch <- []interface{}{feed.updateEngines(req)}

		case fCmdDeleteEngines:
			req, respch := msg[1].(Subscriber), msg[2].(chan []interface{})
			respch <- []interface{}{feed.deleteEngines(req)}

		case fCmdRepairEndpoints:
			respch := msg[1].(chan []interface{})
			respch <- []interface{}{feed.repairEndpoints()}

		case fCmdCloseFeed:
			respch := msg[1].(chan []interface{})
			respch <- []interface{}{feed.doClose()}
			break loop
		}
	}
}

// start a new feed.
func (feed *Feed) requestFeed(req RequestReader) (err error) {
	engines := make(map[uint64]*Engine)
	endpoints := make(map[string]*Endpoint)
	endpoints, engines, err =
		feed.buildEngines(req.(Subscriber), endpoints, engines)
	if err != nil {
		return err
	}

	// order of bucket, restartTimestamp, failoverTimestamp and kvTimestamp
	// are preserved
	for bucket, emap := range bucketWiseEngines(engines) {
		bfeed := feed.bfeeds[bucket]
		if bfeed == nil {
			return ErrorInvalidBucket
		}
		failTs, kvTs, err := bfeed.RequestFeed(req, endpoints, emap)
		if err != nil {
			return err
		}
		// aggregate failover-timestamps, kv-timestamps for all buckets
		failTs = feed.failoverTimestamps[bucket].Union(failTs)
		kvTs = feed.kvTimestamps[bucket].Union(kvTs)
		feed.failoverTimestamps[bucket] = failTs
		feed.kvTimestamps[bucket] = kvTs
		sort.Sort(feed.failoverTimestamps[bucket])
		sort.Sort(feed.kvTimestamps[bucket])
	}
	feed.resetEngines(endpoints, engines)
	log.Printf("%v, started ...\n", feed.logPrefix)
	return nil
}

// start, restart, shutdown vbuckets in an active feed and/or update
// downstream engines and endpoints.
func (feed *Feed) updateFeed(req RequestReader) (err error) {
	engines, endpoints := feed.engines, feed.endpoints
	endpoints, engines, err =
		feed.buildEngines(req.(Subscriber), endpoints, engines)
	if err != nil {
		return err
	}

	// order of bucket, restartTimestamp, failoverTimestamp and kvTimestamp
	// are preserved
	for bucket, emap := range bucketWiseEngines(engines) {
		bfeed := feed.bfeeds[bucket]
		if bfeed == nil {
			return ErrorInvalidBucket
		}
		failTs, kvTs, err := bfeed.UpdateFeed(req, endpoints, emap)
		if err != nil {
			return err
		}
		// update failover-timestamps, kv-timestamps
		if req.IsShutdown() {
			failTs = feed.failoverTimestamps[bucket].FilterByVbuckets(failTs.Vbnos)
			kvTs = feed.kvTimestamps[bucket].FilterByVbuckets(kvTs.Vbnos)
		} else {
			failTs = feed.failoverTimestamps[bucket].Union(failTs)
			kvTs = feed.kvTimestamps[bucket].Union(kvTs)
		}
		feed.failoverTimestamps[bucket] = failTs
		feed.kvTimestamps[bucket] = kvTs
		sort.Sort(feed.failoverTimestamps[bucket])
		sort.Sort(feed.kvTimestamps[bucket])
	}
	feed.resetEngines(endpoints, engines)
	log.Printf("%v, updated ...\n", feed.logPrefix)
	return nil
}

// index topology has changed, update it.
func (feed *Feed) updateEngines(req Subscriber) (err error) {
	endpoints, engines, err :=
		feed.buildEngines(req, feed.endpoints, feed.engines)
	if err != nil {
		return err
	}

	for bucket, emap := range bucketWiseEngines(engines) {
		if bfeed, ok := feed.bfeeds[bucket]; ok {
			if err = bfeed.UpdateEngines(endpoints, emap); err != nil {
				return
			}
		} else {
			return c.ErrorInvalidRequest
		}
	}
	feed.resetEngines(endpoints, engines)
	return
}

// index is deleted, delete all of its downstream
func (feed *Feed) deleteEngines(req Subscriber) (err error) {
	endpoints, engines, err :=
		feed.buildEngines(req, feed.endpoints, feed.engines)
	if err != nil {
		return err
	}

	for bucket, emap := range bucketWiseEngines(engines) {
		if bfeed, ok := feed.bfeeds[bucket]; ok {
			uuids := make([]uint64, 0, len(emap))
			for uuid := range emap {
				uuids = append(uuids, uuid)
			}
			if err = bfeed.DeleteEngines(endpoints, uuids); err != nil {
				return
			}
		} else {
			return c.ErrorInvalidRequest
		}
	}
	feed.resetEngines(endpoints, engines)
	return
}

// repair endpoints, restart engines and update bucket-feed
func (feed *Feed) repairEndpoints() (err error) {

	startEndpoint := func(raddr string, coord bool) (*Endpoint, error) {
		// ignore error while starting endpoint
		endpoint, err := NewEndpoint(feed, raddr, c.ConnsPerEndpoint, coord)
		if err != nil {
			log.Printf("error starting endpoint %q: %v", raddr, err)
			return nil, ErrorStartingEndpoint
		}
		// send the vbmap to the new endpoint.
		emap := map[string]*Endpoint{raddr: endpoint}
		for _, kvTs := range feed.kvTimestamps {
			if err = feed.sendVbmap(emap, kvTs); err != nil {
				return nil, err
			}
		}
		return endpoint, nil
	}

	// repair endpoints
	endpoints := make(map[string]*Endpoint)
	for raddr, endpoint := range feed.endpoints {
		if !endpoint.Ping() {
			if endpoint, err = startEndpoint(raddr, endpoint.coord); err != nil {
				return err
			}
		}
		if endpoint != nil {
			endpoints[raddr] = endpoint
		}
	}
	// new set of engines
	engines := make(map[uint64]*Engine)
	for uuid, engine := range feed.engines {
		engine = NewEngine(feed, uuid, engine.evaluator, engine.router)
		engines[uuid] = engine
	}
	// update Engines with BucketFeed
	for bucket, emap := range bucketWiseEngines(engines) {
		if bfeed, ok := feed.bfeeds[bucket]; ok {
			if err = bfeed.UpdateEngines(endpoints, emap); err != nil {
				return err
			}
		} else {
			return c.ErrorInvalidRequest
		}
	}
	feed.resetEngines(endpoints, nil)
	return nil
}

func (feed *Feed) doClose() (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Feed:doClose() paniced %q\n", feed.topic)
		}
	}()

	for _, bfeed := range feed.bfeeds { // shutdown upstream
		bfeed.CloseFeed()
	}
	for _, endpoint := range feed.endpoints { // shutdown downstream
		endpoint.Close()
	}
	// shutdown
	close(feed.finch)
	feed.bfeeds, feed.endpoints, feed.engines = nil, nil, nil
	log.Printf("%v, ... closed\n", feed.logPrefix)
	return
}

func (feed *Feed) resetEngines(endpoints map[string]*Endpoint, engines map[uint64]*Engine) {
	newendpoints := make(map[string]*Endpoint)
	for raddr, endpoint := range endpoints {
		if _, ok := feed.endpoints[raddr]; !ok {
			log.Printf("%v, endpoint %q added\n", feed.logPrefix, raddr)
			newendpoints[raddr] = endpoint
		} else {
			delete(feed.endpoints, raddr)
		}
	}
	for raddr := range feed.endpoints {
		log.Printf("%v, endpoint %q, deleted\n", feed.logPrefix, raddr)
	}
	feed.endpoints = newendpoints

	if engines != nil {
		newengines := make(map[uint64]*Engine)
		for uuid, engine := range engines {
			if _, ok := feed.engines[uuid]; !ok {
				log.Printf("%v, engine %v added\n", feed.logPrefix, uuid)
			} else {
				log.Printf("%v, engine %v updated\n", feed.logPrefix, uuid)
				delete(feed.engines, uuid)
			}
			newengines[uuid] = engine
		}
		for uuid := range feed.engines {
			log.Printf("%v, engine %v, deleted\n", feed.logPrefix, uuid)
		}
		feed.engines = newengines
	}
}

// build per bucket uuid->engine map using Subscriber interface. `engines` and
// `endpoints` are updated inplace.
func (feed *Feed) buildEngines(
	subscriber Subscriber,
	endpoints map[string]*Endpoint,
	engines map[uint64]*Engine) (map[string]*Endpoint, map[uint64]*Engine, error) {

	evaluators, routers, err := validateSubscriber(subscriber)
	if err != nil {
		return endpoints, engines, err
	}
	if endpoints, err = feed.buildEndpoints(routers, endpoints); err != nil {
		return endpoints, engines, err
	}

	// Rebuild new set of engines
	newengines := make(map[uint64]*Engine)
	for uuid, evaluator := range evaluators {
		engine := NewEngine(feed, uuid, evaluator, routers[uuid])
		newengines[uuid] = engine
	}
	return endpoints, engines, nil
}

// organize engines based on buckets, engine is associated with one bucket.
func bucketWiseEngines(engines map[uint64]*Engine) map[string]map[uint64]*Engine {
	bengines := make(map[string]map[uint64]*Engine)
	for uuid, engine := range engines {
		bucket := engine.evaluator.Bucket()
		emap, ok := bengines[bucket]
		if !ok {
			emap = make(map[uint64]*Engine)
			bengines[bucket] = emap
		}
		emap[uuid] = engine
	}
	return bengines
}

// start endpoints for listed topology, an endpoint is not started if it is
// already active (ping-ok).
// btw, we don't close endpoints, instead we let go of them.
func (feed *Feed) buildEndpoints(
	routers map[uint64]c.Router,
	endpoints map[string]*Endpoint) (map[string]*Endpoint, error) {

	var err error

	startEndpoint := func(raddr string, coord bool) (*Endpoint, error) {
		// ignore error while starting endpoint
		endpoint, err := NewEndpoint(feed, raddr, c.ConnsPerEndpoint, coord)
		if err != nil {
			fmtString := "%v, error starting endpoint %q: %v"
			log.Printf(fmtString, feed.logPrefix, raddr, err)
			return nil, ErrorStartingEndpoint
		}
		// send the vbmap to the new endpoint.
		emap := map[string]*Endpoint{raddr: endpoint}
		for _, kvTs := range feed.kvTimestamps {
			if err = feed.sendVbmap(emap, kvTs); err != nil {
				return nil, err
			}
		}
		return endpoint, nil
	}
	for _, router := range routers {
		for _, raddr := range router.UuidEndpoints() {
			if endpoint, ok := endpoints[raddr]; (!ok) || (!endpoint.Ping()) {
				if endpoint, err = startEndpoint(raddr, false); err != nil {
					return nil, err
				} else if endpoint != nil {
					endpoints[raddr] = endpoint
				}
			}
		}
		// endpoint for coordinator
		coord := router.CoordinatorEndpoint()
		if endpoint, ok := endpoints[coord]; (!ok) || (!endpoint.Ping()) {
			if endpoint, err = startEndpoint(coord, true); err != nil {
				return nil, err
			} else if endpoint != nil {
				endpoints[coord] = endpoint
			}
		}
	}
	return endpoints, nil
}

func validateSubscriber(subscriber Subscriber) (map[uint64]c.Evaluator, map[uint64]c.Router, error) {
	evaluators, err := subscriber.GetEvaluators()
	if err != nil {
		return nil, nil, err
	}
	routers, err := subscriber.GetRouters()
	if err != nil {
		return nil, nil, err
	}
	if len(evaluators) != len(routers) {
		return nil, nil, ErrorInconsistentFeed
	}
	for uuid := range evaluators {
		if _, ok := routers[uuid]; ok == false {
			return nil, nil, ErrorInconsistentFeed
		}
	}
	return evaluators, routers, nil
}

// sendVbmap for current set of vbuckets.
func (feed *Feed) sendVbmap(endpoints map[string]*Endpoint, kvTs *c.Timestamp) error {
	vbmap := &c.VbConnectionMap{
		Bucket:   kvTs.Bucket,
		Vbuckets: make([]uint16, 0),
		Vbuuids:  make([]uint64, 0),
	}
	for i, vbno := range kvTs.Vbnos {
		vbmap.Vbuckets = append(vbmap.Vbuckets, vbno)
		vbmap.Vbuuids = append(vbmap.Vbuuids, kvTs.Vbuuids[i])
	}
	// send to each endpoints.
	for raddr, endpoint := range endpoints {
		if err := endpoint.SendVbmap(vbmap); err != nil {
			fmtString := "%v, error sending vbmap to endpoint %q: %v"
			log.Printf(fmtString, feed.logPrefix, raddr, err)
			return err
		}
	}
	return nil
}
