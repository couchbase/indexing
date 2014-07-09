// Feed is the central program around which adminport, bucket_feed, kvfeed,
// engines and endpoint algorithms are organized.

package projector

import (
	"errors"
	"fmt"
	c "github.com/couchbase/indexing/secondary/common"
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
	bfeeds map[string]*BucketFeed
	// manage downstream
	endpoints map[string]*Endpoint
	engines   map[uint64]*Engine
	// timestamp feedback
	failoverTimestamps map[string]*c.Timestamp // indexed by bucket name
	kvTimestamps       map[string]*c.Timestamp // indexed by bucket name
	// gen-server
	reqch chan []interface{}
	finch chan bool
	// misc.
	logPrefix string
	stats     *c.ComponentStat
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
		logPrefix:          fmt.Sprintf("[feed %q]", topic),
	}
	feed.stats = feed.newStats()

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
	c.Infof("%v activated ...\n", feed.logPrefix)
	return feed, nil
}

func (feed *Feed) getProjector() *Projector {
	return feed.projector
}

// gen-server API commands
const (
	fCmdRequestFeed byte = iota + 1
	fCmdUpdateFeed
	fCmdAddEngines
	fCmdUpdateEngines
	fCmdDeleteEngines
	fCmdRepairEndpoints
	fCmdGetStatistics
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

// AddEngines will add new engines for this Feed, synchronous call.
//
// - error if Feed is already closed.
func (feed *Feed) AddEngines(request Subscriber) error {
	if request == nil {
		return ErrorArgument
	}
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdAddEngines, request, respch}
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

// GetStatistics will recursively get statistics for feed and its underlying
// workers.
func (feed *Feed) GetStatistics() map[string]interface{} {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdGetStatistics, respch}
	resp, _ := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return resp[1].(map[string]interface{})
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
			c.Errorf("%v ... paniced %v !\n", feed.logPrefix, r)
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

		case fCmdAddEngines:
			req, respch := msg[1].(Subscriber), msg[2].(chan []interface{})
			respch <- []interface{}{feed.addEngines(req)}

		case fCmdUpdateEngines:
			req, respch := msg[1].(Subscriber), msg[2].(chan []interface{})
			respch <- []interface{}{feed.updateEngines(req)}

		case fCmdDeleteEngines:
			req, respch := msg[1].(Subscriber), msg[2].(chan []interface{})
			respch <- []interface{}{feed.deleteEngines(req)}

		case fCmdRepairEndpoints:
			respch := msg[1].(chan []interface{})
			respch <- []interface{}{feed.repairEndpoints()}

		case fCmdGetStatistics:
			respch := msg[1].(chan []interface{})
			respch <- []interface{}{feed.getStatistics()}

		case fCmdCloseFeed:
			respch := msg[1].(chan []interface{})
			respch <- []interface{}{feed.doClose()}
			break loop
		}
	}
}

// start a new feed.
func (feed *Feed) requestFeed(req RequestReader) (err error) {
	var engines map[uint64]*Engine

	endpoints := make(map[string]*Endpoint)
	subscr := req.(Subscriber)
	evaluators, routers, err := feed.validateSubscriber(subscr)
	if err != nil {
		return err
	}

	if endpoints, err = feed.buildEndpoints(routers, endpoints); err != nil {
		return err
	}
	if engines, err = feed.buildEngines(evaluators, routers); err != nil {
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
	if len(engines) == 0 {
		c.Warnf("%v empty engines !\n", feed.logPrefix)
	} else {
		c.Infof("%v started ...\n", feed.logPrefix)
	}
	feed.endpoints, feed.engines = endpoints, engines
	return nil
}

// start, restart, shutdown vbuckets in an active feed and/or update
// downstream engines and endpoints.
func (feed *Feed) updateFeed(req RequestReader) (err error) {
	var endpoints map[string]*Endpoint
	var engines map[uint64]*Engine

	subscr := req.(Subscriber)
	evaluators, routers, err := feed.validateSubscriber(subscr)
	if err != nil {
		return err
	}

	if endpoints, err = feed.buildEndpoints(routers, feed.endpoints); err != nil {
		return err
	}
	if engines, err = feed.buildEngines(evaluators, routers); err != nil {
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
	feed.endpoints, feed.engines = endpoints, engines
	c.Infof("%v update ... done\n", feed.logPrefix)
	return nil
}

// index topology has changed, update it.
func (feed *Feed) addEngines(subscr Subscriber) (err error) {
	var endpoints map[string]*Endpoint
	var engines map[uint64]*Engine

	evaluators, routers, err := feed.validateSubscriber(subscr)
	if err != nil {
		return err
	}

	if endpoints, err = feed.buildEndpoints(routers, feed.endpoints); err != nil {
		return err
	}
	for raddr, endpoint := range feed.endpoints {
		endpoints[raddr] = endpoint
	}
	if engines, err = feed.buildEngines(evaluators, routers); err != nil {
		return err
	}
	for uuid, engine := range feed.engines {
		engines[uuid] = engine
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
	feed.endpoints, feed.engines = endpoints, engines
	c.Infof("%v add engines ... done\n", feed.logPrefix)
	return
}

// index topology has changed, update it.
func (feed *Feed) updateEngines(subscr Subscriber) (err error) {
	var endpoints map[string]*Endpoint
	var engines map[uint64]*Engine

	evaluators, routers, err := feed.validateSubscriber(subscr)
	if err != nil {
		return err
	}

	if endpoints, err = feed.buildEndpoints(routers, feed.endpoints); err != nil {
		return err
	}
	if engines, err = feed.buildEngines(evaluators, routers); err != nil {
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
	feed.endpoints, feed.engines = endpoints, engines
	c.Infof("%v update engines ... done\n", feed.logPrefix)
	return
}

// index is deleted, delete all of its downstream
func (feed *Feed) deleteEngines(subscr Subscriber) (err error) {
	var engines map[uint64]*Engine

	evaluators, routers, err := feed.validateSubscriber(subscr)
	if err != nil {
		return err
	}

	// we don't delete endpoints, since it might be shared with other engines.
	// endpoint routine will commit harakiri if no engines are sending them
	// data.

	if engines, err = feed.buildEngines(evaluators, routers); err != nil {
		return err
	}

	for bucket, emap := range bucketWiseEngines(engines) {
		if bfeed, ok := feed.bfeeds[bucket]; ok {
			uuids := make([]uint64, 0, len(emap))
			for uuid := range emap {
				uuids = append(uuids, uuid)
			}
			if err = bfeed.DeleteEngines(feed.endpoints, uuids); err != nil {
				return
			}
		} else {
			return c.ErrorInvalidRequest
		}
	}
	for uuid := range engines {
		c.Infof("%v engine %v deleted ...\n", feed.logPrefix, uuid)
		delete(feed.engines, uuid)
	}
	c.Infof("%v delete engines ... done\n", feed.logPrefix)
	return
}

// repair endpoints, restart engines and update bucket-feed
func (feed *Feed) repairEndpoints() (err error) {
	// repair endpoints
	endpoints := make(map[string]*Endpoint)
	for raddr, endpoint := range feed.endpoints {
		if !endpoint.Ping() {
			c.Infof("%v restarting endpoint %q ...\n", feed.logPrefix, raddr)
			endpoint, err = feed.startEndpoint(raddr, endpoint.coord)
			if err != nil {
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
	feed.endpoints, feed.engines = endpoints, engines
	c.Infof("%v repair endpoints ... done\n", feed.logPrefix)
	return nil
}

func (feed *Feed) getStatistics() map[string]interface{} {
	bfeeds, _ := c.NewComponentStat(feed.stats.Get("/feeds"))
	raddrs, _ := c.NewComponentStat(feed.stats.Get("/raddrs"))
	feed.stats.Set("/engines", feed.engineNames())
	feed.stats.Set("/endpoints", feed.endpointNames())
	for bucketn, bfeed := range feed.bfeeds {
		bfeeds.Set("/"+bucketn, bfeed.GetStatistics())
	}
	for raddr, endpoint := range feed.endpoints {
		raddrs.Set("/"+raddr, endpoint.GetStatistics())
	}
	return feed.stats.ToMap()
}

func (feed *Feed) doClose() (err error) {
	defer func() {
		if r := recover(); r != nil {
			c.Errorf("%v doClose() paniced: %v !\n", feed.topic, r)
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
	c.Infof("%v ... stopped\n", feed.logPrefix)
	return
}

// build per bucket uuid->engine map using Subscriber interface. `engines` and
// `endpoints` are updated inplace.
func (feed *Feed) buildEngines(
	evaluators map[uint64]c.Evaluator,
	routers map[uint64]c.Router) (map[uint64]*Engine, error) {

	// Rebuild new set of engines
	newengines := make(map[uint64]*Engine)
	for uuid, evaluator := range evaluators {
		engine := NewEngine(feed, uuid, evaluator, routers[uuid])
		newengines[uuid] = engine
	}
	return newengines, nil
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

	for _, router := range routers {
		for _, raddr := range router.UuidEndpoints() {
			if endpoint, ok := endpoints[raddr]; (!ok) || (!endpoint.Ping()) {
				endpoint, err = feed.startEndpoint(raddr, false)
				if err != nil {
					return nil, err
				} else if endpoint != nil {
					endpoints[raddr] = endpoint
				}
			}
		}
		// endpoint for coordinator
		coord := router.CoordinatorEndpoint()
		if coord != "" {
			if endpoint, ok := endpoints[coord]; (!ok) || (!endpoint.Ping()) {
				endpoint, err = feed.startEndpoint(coord, true)
				if err != nil {
					return nil, err
				} else if endpoint != nil {
					endpoints[coord] = endpoint
				}
			}
		}
	}
	return endpoints, nil
}

func (feed *Feed) startEndpoint(raddr string, coord bool) (endpoint *Endpoint, err error) {
	// ignore error while starting endpoint
	endpoint, err = NewEndpoint(feed, raddr, c.ConnsPerEndpoint, coord)
	if err != nil {
		return nil, err
	}
	// TODO: send vbmap to the new endpoint.
	// for _, kvTs := range feed.kvTimestamps {
	//     vbmap := feed.vbTs2Vbmap(kvTs)
	//     if err = endpoint.SendVbmap(vbmap); err != nil {
	//         return nil, err
	//     }
	// }
	return endpoint, nil
}

func (feed *Feed) validateSubscriber(subscriber Subscriber) (map[uint64]c.Evaluator, map[uint64]c.Router, error) {
	evaluators, err := subscriber.GetEvaluators()
	if err != nil {
		return nil, nil, err
	}
	routers, err := subscriber.GetRouters()
	if err != nil {
		return nil, nil, err
	}
	if len(evaluators) != len(routers) {
		err = ErrorInconsistentFeed
		c.Errorf("%v error %v, len() mismatch", feed.logPrefix, err)
		return nil, nil, err
	}
	for uuid := range evaluators {
		if _, ok := routers[uuid]; ok == false {
			err = ErrorInconsistentFeed
			c.Errorf("%v error %v, uuid mismatch", feed.logPrefix, err)
			return nil, nil, err
		}
	}
	return evaluators, routers, nil
}

// vbTs2Vbmap construct VbConnectionMap from Vbucket Timestamp.
func (feed *Feed) vbTs2Vbmap(kvTs *c.Timestamp) *c.VbConnectionMap {
	vbmap := &c.VbConnectionMap{
		Bucket:   kvTs.Bucket,
		Vbuckets: make([]uint16, 0),
		Vbuuids:  make([]uint64, 0),
	}
	for i, vbno := range kvTs.Vbnos {
		vbmap.Vbuckets = append(vbmap.Vbuckets, vbno)
		vbmap.Vbuuids = append(vbmap.Vbuuids, kvTs.Vbuuids[i])
	}
	return vbmap
}

func (feed *Feed) engineNames() []string {
	names := make([]string, 0, len(feed.engines))
	for uuid := range feed.engines {
		names = append(names, fmt.Sprintf("%v", uuid))
	}
	return names
}

func (feed *Feed) endpointNames() []string {
	raddrs := make([]string, 0, len(feed.endpoints))
	for raddr := range feed.endpoints {
		raddrs = append(raddrs, raddr)
	}
	return raddrs
}
