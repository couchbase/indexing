package projector

import "errors"
import "fmt"
import "time"
import "runtime/debug"

import mcd "github.com/couchbase/gomemcached"
import mc "github.com/couchbase/gomemcached/client"
import c "github.com/couchbase/indexing/secondary/common"
import "github.com/couchbase/indexing/secondary/protobuf"
import "github.com/couchbaselabs/goprotobuf/proto"

// error codes

// ErrorInvalidBucket
var ErrorInvalidBucket = errors.New("feed.invalidBucket")

// ErrorInvalidVbucketBranch
var ErrorInvalidVbucketBranch = errors.New("feed.invalidVbucketBranch")

// ErrorInvalidVbucket
var ErrorInvalidVbucket = errors.New("feed.invalidVbucket")

// ErrorInconsistentFeed
var ErrorInconsistentFeed = errors.New("feed.inconsistentFeed")

// ErrorResponseTimeout
var ErrorResponseTimeout = errors.New("feed.responseTimeout")

// Feed is mutation stream - for maintenance, initial-load, catchup etc...
type Feed struct {
	cluster      string   // immutable
	topic        string   // immutable
	endpointType string   // immutable
	kvaddrs      []string // immutable

	// upstream
	reqTss  map[string]*protobuf.TsVbuuid // bucket -> TsVbuuid
	rollTss map[string]*protobuf.TsVbuuid // bucket -> TsVbuuid
	feeders map[string]BucketFeeder       // bucket -> BucketFeeder{}
	// downstream
	kvdata    map[string]map[string]*KVData // bucket -> kvaddr -> kvdata
	engines   map[string]map[uint64]*Engine // bucket -> uuid -> engine
	endpoints map[string]c.RouterEndpoint
	// genServer channel
	reqch  chan []interface{}
	backch chan []interface{}
	finch  chan bool

	// config params
	maxVbuckets int
	reqTimeout  time.Duration
	endTimeout  time.Duration
	epFactory   c.RouterEndpointFactory
	config      c.Config
	logPrefix   string
}

// NewFeed creates a new topic feed.
// `config` contains following keys.
//    name:        human readable name for this feed.
//    maxVbuckets: configured number vbuckets per bucket.
//    clusterAddr: KV cluster address <host:port>.
//    kvAddrs:     list of kvnodes to watch for mutations.
//    feedWaitStreamReqTimeout: wait for a response to StreamRequest
//    feedWaitStreamEndTimeout: wait for a response to StreamEnd
//    feedChanSize: channel size for feed's control path and back path
//    routerEndpointFactory: endpoint factory
func NewFeed(topic string, config c.Config) *Feed {
	epf := config["routerEndpointFactory"].Value.(c.RouterEndpointFactory)
	chsize := config["feedChanSize"].Int()
	feed := &Feed{
		cluster: config["clusterAddr"].String(),
		topic:   topic,
		kvaddrs: config["kvAddrs"].Strings(),

		// upstream
		reqTss:  make(map[string]*protobuf.TsVbuuid),
		rollTss: make(map[string]*protobuf.TsVbuuid),
		feeders: make(map[string]BucketFeeder),
		// downstream
		kvdata:    make(map[string]map[string]*KVData),
		engines:   make(map[string]map[uint64]*Engine),
		endpoints: make(map[string]c.RouterEndpoint),
		// genServer channel
		reqch:  make(chan []interface{}, chsize),
		backch: make(chan []interface{}, chsize),
		finch:  make(chan bool),

		maxVbuckets: config["maxVbuckets"].Int(),
		reqTimeout:  time.Duration(config["feedWaitStreamReqTimeout"].Int()),
		endTimeout:  time.Duration(config["feedWaitStreamEndTimeout"].Int()),
		epFactory:   epf,
		config:      config,
	}
	feed.logPrefix = fmt.Sprintf("[%v->%v]", config["name"].String(), topic)

	go feed.genServer()
	c.Infof("%v started ...\n", feed.logPrefix)
	return feed
}

const (
	fCmdStart byte = iota + 1
	fCmdRestartVbuckets
	fCmdShutdownVbuckets
	fCmdAddBuckets
	fCmdDelBuckets
	fCmdAddInstances
	fCmdDelInstances
	fCmdRepairEndpoints
	fCmdShutdown
	fCmdGetStatistics
)

// MutationTopic will start the feed.
// Synchronous call.
func (feed *Feed) MutationTopic(
	req *protobuf.MutationTopicRequest) (*protobuf.TopicResponse, error) {

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdStart, req, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return resp[0].(*protobuf.TopicResponse), c.OpError(err, resp, 1)
}

// RestartVbuckets will restart upstream vbuckets for specified buckets.
// Synchronous call.
func (feed *Feed) RestartVbuckets(
	req *protobuf.RestartVbucketsRequest) (*protobuf.TopicResponse, error) {

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdRestartVbuckets, req, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return resp[0].(*protobuf.TopicResponse), c.OpError(err, resp, 1)
}

// ShutdownVbuckets will shutdown streams for
// specified buckets.
// Synchronous call.
func (feed *Feed) ShutdownVbuckets(req *protobuf.ShutdownVbucketsRequest) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdShutdownVbuckets, req, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return c.OpError(err, resp, 0)
}

// AddBuckets will remove buckets and all its upstream
// and downstream elements, except endpoints.
// Synchronous call.
func (feed *Feed) AddBuckets(
	req *protobuf.AddBucketsRequest) (*protobuf.TopicResponse, error) {

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdAddBuckets, req, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return resp[0].(*protobuf.TopicResponse), c.OpError(err, resp, 1)
}

// DelBuckets will remove buckets and all its upstream
// and downstream elements, except endpoints.
// Synchronous call.
func (feed *Feed) DelBuckets(req *protobuf.DelBucketsRequest) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdDelBuckets, req, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return c.OpError(err, resp, 0)
}

// AddInstances will restart specified endpoint-address if
// it is not active already.
// Synchronous call.
func (feed *Feed) AddInstances(req *protobuf.AddInstancesRequest) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdAddInstances, req, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return c.OpError(err, resp, 0)
}

// DelInstances will restart specified endpoint-address if
// it is not active already.
// Synchronous call.
func (feed *Feed) DelInstances(req *protobuf.DelInstancesRequest) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdDelInstances, req, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return c.OpError(err, resp, 0)
}

// RepairEndpoints will restart specified endpoint-address if
// it is not active already.
// Synchronous call.
func (feed *Feed) RepairEndpoints(req *protobuf.RepairEndpointsRequest) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdRepairEndpoints, req, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return c.OpError(err, resp, 0)
}

// GetStatistics for this feed.
// Synchronous call.
func (feed *Feed) GetStatistics() c.Statistics {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdGetStatistics, respch}
	resp, _ := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return resp[0].(c.Statistics)
}

// Shutdown feed, its upstream connection with kv and downstream endpoints.
// Synchronous call.
func (feed *Feed) Shutdown() error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdShutdown, respch}
	_, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return err
}

type controlStreamRequest struct {
	bucket string
	kvaddr string
	opaque uint16
	status mcd.Status
	vbno   uint16
	vbuuid uint64
	seqno  uint64
}

// PostStreamRequest feedback from data-path.
// Asynchronous call.
func (feed *Feed) PostStreamRequest(bucket, kvaddr string, m *mc.UprEvent) {
	var respch chan []interface{}
	cmd := &controlStreamRequest{
		bucket: bucket,
		kvaddr: kvaddr,
		opaque: m.Opaque,
		status: m.Status,
		vbno:   m.VBucket,
		vbuuid: m.VBuuid,
		seqno:  m.Seqno,
	}
	c.FailsafeOp(feed.backch, respch, []interface{}{cmd}, feed.finch)
}

type controlStreamEnd struct {
	bucket string
	kvaddr string
	opaque uint16
	status mcd.Status
	vbno   uint16
}

// PostStreamEnd feedback from data-path.
// Asynchronous call.
func (feed *Feed) PostStreamEnd(bucket, kvaddr string, m *mc.UprEvent) {
	var respch chan []interface{}
	cmd := &controlStreamEnd{
		bucket: bucket,
		kvaddr: kvaddr,
		opaque: m.Opaque,
		status: m.Status,
		vbno:   m.VBucket,
	}
	c.FailsafeOp(feed.backch, respch, []interface{}{cmd}, feed.finch)
}

type controlFinKVData struct {
	bucket string
	kvaddr string
}

// PostFinKVdata feedback from data-path.
// Asynchronous call.
func (feed *Feed) PostFinKVdata(bucket, kvaddr string) {
	var respch chan []interface{}
	cmd := &controlFinKVData{bucket: bucket, kvaddr: kvaddr}
	c.FailsafeOp(feed.backch, respch, []interface{}{cmd}, feed.finch)
}

func (feed *Feed) genServer() {
	defer func() { // panic safe
		if r := recover(); r != nil {
			c.Errorf("%v gen-server crashed: %v\n", feed.logPrefix, r)
			c.StackTrace(string(debug.Stack()))
			feed.shutdown()
		}
	}()

	var msg []interface{}

	timeout := time.Tick(1000 * time.Millisecond)
	ctrlMsg := "%v control channel has %v messages"

loop:
	for {
		select {
		case msg = <-feed.reqch:
			if feed.handleCommand(msg) {
				break loop
			}

		case msg = <-feed.backch:
			if v, ok := msg[0].(*controlStreamEnd); ok {
				c.Infof("%v back channel flushed %T %v", feed.logPrefix, v, v)
				reqTs := feed.reqTss[v.bucket]
				reqTs = reqTs.FilterByVbuckets([]uint16{v.vbno})
				feed.reqTss[v.bucket] = reqTs

				rollTs := feed.rollTss[v.bucket]
				rollTs = rollTs.FilterByVbuckets([]uint16{v.vbno})
				feed.rollTss[v.bucket] = rollTs

			} else if v, ok := msg[0].(*controlFinKVData); ok {
				reqTs, ok := feed.reqTss[v.bucket]
				if ok && reqTs.Len() == 0 { // bucket is done
					format := "%v self deleting bucket %v"
					c.Infof(format, feed.logPrefix, v.bucket)

					feed.feeders[v.bucket].CloseFeed()
					// cleanup data structures.
					delete(feed.reqTss, v.bucket)  // :SideEffect:
					delete(feed.rollTss, v.bucket) // :SideEffect:
					delete(feed.feeders, v.bucket) // :SideEffect:
					delete(feed.kvdata, v.bucket)  // :SideEffect:
					delete(feed.engines, v.bucket) // :SideEffect:

				} else if ok { // only a single kvnode for this bucket is done
					format := "%v self deleting kvdata {%v, %v}"
					c.Infof(format, feed.logPrefix, v.bucket, v.kvaddr)
					delete(feed.kvdata[v.bucket], v.kvaddr)
				}

			} else {
				c.Errorf("%v back channel flushed %T %v", feed.logPrefix, v, v)
			}

		case <-timeout:
			c.Debugf(ctrlMsg, feed.logPrefix, len(feed.backch))
		}
	}
}

func (feed *Feed) handleCommand(msg []interface{}) (exit bool) {
	exit = false

	switch cmd := msg[0].(byte); cmd {
	case fCmdStart:
		req := msg[1].(*protobuf.MutationTopicRequest)
		respch := msg[2].(chan []interface{})
		err := feed.start(req)
		response := feed.topicResponse()
		respch <- []interface{}{response, err}

	case fCmdRestartVbuckets:
		req := msg[1].(*protobuf.RestartVbucketsRequest)
		respch := msg[2].(chan []interface{})
		err := feed.restartVbuckets(req)
		response := feed.topicResponse()
		respch <- []interface{}{response, err}

	case fCmdShutdownVbuckets:
		req := msg[1].(*protobuf.ShutdownVbucketsRequest)
		respch := msg[2].(chan []interface{})
		respch <- []interface{}{feed.shutdownVbuckets(req)}

	case fCmdAddBuckets:
		req := msg[1].(*protobuf.AddBucketsRequest)
		respch := msg[2].(chan []interface{})
		err := feed.addBuckets(req)
		response := feed.topicResponse()
		respch <- []interface{}{response, err}

	case fCmdDelBuckets:
		req := msg[1].(*protobuf.DelBucketsRequest)
		respch := msg[2].(chan []interface{})
		respch <- []interface{}{feed.delBuckets(req)}

	case fCmdAddInstances:
		req := msg[1].(*protobuf.AddInstancesRequest)
		respch := msg[2].(chan []interface{})
		respch <- []interface{}{feed.addInstances(req)}

	case fCmdDelInstances:
		req := msg[1].(*protobuf.DelInstancesRequest)
		respch := msg[2].(chan []interface{})
		respch <- []interface{}{feed.delInstances(req)}

	case fCmdRepairEndpoints:
		req := msg[1].(*protobuf.RepairEndpointsRequest)
		respch := msg[2].(chan []interface{})
		respch <- []interface{}{feed.repairEndpoints(req)}

	case fCmdGetStatistics:
		respch := msg[1].(chan []interface{})
		respch <- []interface{}{feed.getStatistics()}

	case fCmdShutdown:
		respch := msg[1].(chan []interface{})
		respch <- []interface{}{feed.shutdown()}
		exit = true

	}
	return exit
}

// start a new feed.
// - return ErrorInconsistentFeed for malformed feed request
// - return ErrorInvalidVbucketBranch for malformed vbuuid.
// - return go-couchbase failures.
// - return ErrorResponseTimeout if feedback is not completed within timeout.
func (feed *Feed) start(req *protobuf.MutationTopicRequest) error {
	feed.endpointType = req.GetEndpointType()

	// update engines and endpoints
	if err := feed.processSubscribers(req); err != nil { // :SideEffect:
		return err
	}
	// iterate request-timestamp for each bucket.
	opaque := newOpaque()
	for _, reqTs := range req.GetReqTimestamps() {
		pooln, bucketn := reqTs.GetBucket(), reqTs.GetBucket()
		// start upstream
		feeder, err := feed.bucketFeed(opaque, false, true, reqTs)
		if err != nil {
			return err
		}
		// open data-path
		m := feed.startDataPath(bucketn, feeder, reqTs)
		// wait ....
		vbnos := c.Vbno32to16(reqTs.GetVbnos())
		rollTs, err := feed.waitStreamRequests(opaque, pooln, bucketn, vbnos)
		if err != nil {
			return err
		}
		c.Infof("%v stream-request completed with %v, for vbnos %v #%x\n",
			feed.logPrefix, rollTs, vbnos, opaque)
		feed.reqTss[bucketn] = reqTs   // :SideEffect:
		feed.rollTss[bucketn] = rollTs // :SideEffect:
		feed.feeders[bucketn] = feeder // :SideEffect:
		feed.kvdata[bucketn] = m       // :SideEffect:
	}
	return nil
}

// a subset of upstreams are restarted.
// - return ErrorInvalidBucket if bucket is not added.
// - return ErrorInvalidVbucketBranch for malformed vbuuid.
// - return go-couchbase failures.
// - return ErrorResponseTimeout if feedback is not completed within timeout.
func (feed *Feed) restartVbuckets(req *protobuf.RestartVbucketsRequest) error {
	// iterate request-timestamp for each bucket.
	opaque := newOpaque()
	for _, restartTs := range req.GetRestartTimestamps() {
		pooln, bucketn := restartTs.GetPool(), restartTs.GetBucket()
		reqTs, ok1 := feed.reqTss[bucketn]
		kvdata, ok2 := feed.kvdata[bucketn]
		if !ok1 || !ok2 {
			msg := "%v restartVbuckets() invalid bucket %v\n"
			c.Errorf(msg, feed.logPrefix, bucketn)
			return ErrorInvalidBucket
		}
		// first shutdown upstream
		_, err := feed.bucketFeed(opaque, true, false, restartTs)
		if err != nil {
			return err
		}
		// wait for stream to shutdown ...
		vbnos := c.Vbno32to16(restartTs.GetVbnos())
		if err := feed.waitStreamEnds(opaque, bucketn, vbnos); err != nil {
			return err
		}

		for _, kvaddr := range feed.kvaddrs { // update with new start-sequence
			kvdata[kvaddr].UpdateTs(restartTs)
		}

		// then restart the upstream
		_, err = feed.bucketFeed(opaque, false, true, restartTs)
		if err != nil {
			return err
		}
		// wait for stream to start ...
		rollTs, err := feed.waitStreamRequests(opaque, pooln, bucketn, vbnos)
		if err != nil {
			return err
		}
		c.Infof("%v stream-request completed with %v, for vbnos %v #%x\n",
			feed.logPrefix, rollTs, vbnos, opaque)
		// update vbnos that are shutdown
		feed.reqTss[bucketn] = reqTs.Union(restartTs) // :SideEffect:
		feed.rollTss[bucketn] = rollTs                // :SideEffect:
	}
	return nil
}

// a subset of upstreams are closed.
// - return ErrorInvalidBucket if bucket is not added.
// - return ErrorInvalidVbucketBranch for malformed vbuuid.
// - return go-couchbase failures.
// - return ErrorResponseTimeout if feedback is not completed within timeout.
func (feed *Feed) shutdownVbuckets(
	req *protobuf.ShutdownVbucketsRequest) (err error) {
	// iterate request-timestamp for each bucket.
	opaque := newOpaque()
	for _, shutTs := range req.GetShutdownTimestamps() {
		bucketn := shutTs.GetBucket()
		reqTs, ok := feed.reqTss[bucketn]
		if !ok {
			return ErrorInvalidBucket
		}
		// shutdown upstream
		_, err := feed.bucketFeed(opaque, true, false, shutTs)
		if err != nil {
			return err
		}
		// wait ...
		vbnos := c.Vbno32to16(shutTs.GetVbnos())
		err = feed.waitStreamEnds(opaque, bucketn, vbnos)
		if err != nil {
			return err
		}
		c.Infof("%v stream-end completed for bucket %v, vbnos %v #%x\n",
			feed.logPrefix, bucketn, vbnos, opaque)
		// forget vbnos that are shutdown
		feed.reqTss[bucketn] = reqTs.FilterByVbuckets(vbnos) // :SideEffect:
	}
	return nil
}

// upstreams are added for buckets data-path opened and
// vbucket-routines started.
// - return ErrorInconsistentFeed for malformed feed request
// - return ErrorInvalidVbucketBranch for malformed vbuuid.
// - return go-couchbase failures.
// - return ErrorResponseTimeout if feedback is not completed within timeout.
func (feed *Feed) addBuckets(req *protobuf.AddBucketsRequest) error {
	// update engines and endpoints
	if err := feed.processSubscribers(req); err != nil { // :SideEffect:
		return err
	}

	// iterate request-timestamp for each bucket.
	opaque := newOpaque()
	for _, reqTs := range req.GetReqTimestamps() {
		pooln, bucketn := reqTs.GetPool(), reqTs.GetBucket()
		// start upstream
		feeder, err := feed.bucketFeed(opaque, false, true, reqTs)
		if err != nil {
			return err
		}
		// open data-path
		m := feed.startDataPath(bucketn, feeder, reqTs)
		// wait ....
		vbnos := c.Vbno32to16(reqTs.GetVbnos())
		rollTs, err := feed.waitStreamRequests(opaque, pooln, bucketn, vbnos)
		if err != nil {
			return err
		}
		c.Infof("%v stream-request completed with %v, for vbnos %v #%x\n",
			feed.logPrefix, rollTs, vbnos, opaque)
		feed.reqTss[bucketn] = reqTs   // :SideEffect:
		feed.rollTss[bucketn] = rollTs // :SideEffect:
		feed.feeders[bucketn] = feeder // :SideEffect:
		feed.kvdata[bucketn] = m       // :SideEffect:
	}
	return nil
}

// upstreams are closed for buckets, data-path is closed for downstream,
// vbucket-routines exits on StreamEnd
// - return ErrorInvalidBucket if bucket is not added.
// - return ErrorInvalidVbucketBranch for malformed vbuuid.
// - return go-couchbase failures.
// - return ErrorResponseTimeout if feedback is not completed within timeout.
func (feed *Feed) delBuckets(req *protobuf.DelBucketsRequest) error {
	opaque := newOpaque()
	for _, bucketn := range req.GetBuckets() {
		if _, ok := feed.kvdata[bucketn]; !ok {
			feed.errorf("no bucket", bucketn, nil)
			return ErrorInvalidBucket
		}
		// stop upstream
		_, err := feed.bucketFeed(opaque, true, false, feed.reqTss[bucketn])
		if err != nil {
			return err
		}
		// wait ...
		vbnos := c.Vbno32to16(feed.reqTss[bucketn].GetVbnos())
		err = feed.waitStreamEnds(opaque, bucketn, vbnos)
		if err != nil {
			return err
		}
		c.Infof("%v stream-end completed for bucket %v, vbnos %v #%x\n",
			feed.logPrefix, bucketn, vbnos, opaque)
		// close data-path
		for _, kvdata := range feed.kvdata[bucketn] {
			kvdata.Close()
		}

		feed.feeders[bucketn].CloseFeed()

		// cleanup data structures.
		delete(feed.reqTss, bucketn)  // :SideEffect:
		delete(feed.rollTss, bucketn) // :SideEffect:
		delete(feed.feeders, bucketn) // :SideEffect:
		delete(feed.kvdata, bucketn)  // :SideEffect:
		delete(feed.engines, bucketn) // :SideEffect:
	}
	return nil
}

// only data-path shall be updated.
// TODO: update ?
// - return ErrorInconsistentFeed for malformed feed request
func (feed *Feed) addInstances(req *protobuf.AddInstancesRequest) error {
	// update engines and endpoints
	if err := feed.processSubscribers(req); err != nil { // :SideEffect:
		return err
	}
	// post to kv data-path
	for bucketn, engines := range feed.engines {
		for _, kvdata := range feed.kvdata[bucketn] {
			kvdata.AddEngines(engines, feed.endpoints)
		}
	}
	return nil
}

// only data-path shall be updated.
// - if it is the last instance defined on the bucket, then
//   use delBuckets() API to delete the bucket.
func (feed *Feed) delInstances(req *protobuf.DelInstancesRequest) error {
	// reconstruct instance uuids bucket-wise.
	instanceIds := req.GetInstanceIds()
	bucknIds := make(map[string][]uint64)           // bucket -> []instance
	fengines := make(map[string]map[uint64]*Engine) // bucket-> uuid-> instance
	for bucketn, engines := range feed.engines {
		uuids := make([]uint64, 0)
		m := make(map[uint64]*Engine)
		for uuid, engine := range engines {
			if c.HasUint64(uuid, instanceIds) {
				uuids = append(uuids, uuid)
			} else {
				m[uuid] = engine
			}
		}
		bucknIds[bucketn] = uuids
		fengines[bucketn] = m
	}
	// posted post to kv data-path.
	for bucketn, uuids := range bucknIds {
		for _, kvdata := range feed.kvdata[bucketn] {
			kvdata.DeleteEngines(uuids)
		}
	}
	feed.engines = fengines // :SideEffect:
	return nil
}

// endpoints are independent.
func (feed *Feed) repairEndpoints(req *protobuf.RepairEndpointsRequest) error {
	for _, raddr := range req.GetEndpoints() {
		endpoint, ok := feed.endpoints[raddr]
		if (!ok) || (!endpoint.Ping()) {
			// ignore error while starting endpoint
			topic, typ := feed.topic, feed.endpointType
			endpoint, err := feed.epFactory(topic, typ, raddr)
			if err != nil {
				return err
			} else if endpoint != nil {
				feed.endpoints[raddr] = endpoint // :SideEffect:
			}
		}
	}

	// posted to each kv data-path
	for bucketn, kvdatas := range feed.kvdata {
		for _, kvdata := range kvdatas {
			// though only endpoints have been updated
			kvdata.AddEngines(feed.engines[bucketn], feed.endpoints)
		}
	}
	return nil
}

func (feed *Feed) getStatistics() map[string]interface{} {
	stats, _ := c.NewStatistics(nil)
	stats.Set("engines", feed.engineNames())
	for bucketn, kvnodes := range feed.kvdata {
		bstats, _ := c.NewStatistics(nil)
		for kvaddr, kv := range kvnodes {
			bstats.Set("node-"+kvaddr, kv.GetStatistics())
		}
		stats.Set("bucket-"+bucketn, bstats)
	}
	endStats, _ := c.NewStatistics(nil)
	for raddr, endpoint := range feed.endpoints {
		endStats.Set(raddr, endpoint.GetStatistics())
	}
	stats.Set("endpoint", endStats)
	return map[string]interface{}(stats)
}

func (feed *Feed) shutdown() error {
	defer func() {
		if r := recover(); r != nil {
			c.Errorf("%v shutdown() crashed: %v\n", feed.logPrefix, r)
			c.StackTrace(string(debug.Stack()))
		}
	}()

	// close upstream
	for _, feeder := range feed.feeders {
		feeder.CloseFeed()
	}
	// close data-path
	for _, xs := range feed.kvdata {
		for _, x := range xs {
			x.Close()
		}
	}
	// close downstream
	for _, endpoint := range feed.endpoints {
		endpoint.Close()
	}
	// cleanup
	close(feed.finch)
	c.Infof("%v ... stopped\n", feed.logPrefix)
	return nil
}

// start a feed for a bucket with a set of kvfeeder,
// based on vbmap and failover-logs.
// - return go-couchbase failures.
// - return ErrorInvalidVbucketBranch for malformed vbuuid.
func (feed *Feed) bucketFeed(
	opaque uint16,
	stop, start bool,
	reqTs *protobuf.TsVbuuid) (BucketFeeder, error) {

	pooln, bucketn := reqTs.GetPool(), reqTs.GetBucket()
	vbnos, vbuuids, err := feed.bucketDetails(pooln, bucketn)
	if err != nil {
		return nil, err
	}
	if start {
		// if streams need to be started, make sure
		// that branch histories are the same.
		if reqTs.VerifyBranch(vbnos, vbuuids) == false {
			feed.errorf("VerifyBranch()", bucketn, vbuuids)
			return nil, ErrorInvalidVbucketBranch
		}
	}

	reqTs = reqTs.SelectByVbuckets(vbnos) // filter vbuckets

	feeder, ok := feed.feeders[bucketn]
	if !ok { // the feed is being started for the first time
		bucket, err := c.ConnectBucket(feed.cluster, pooln, bucketn)
		if err != nil {
			feed.errorf("ConnectBucket()", bucketn, err)
			return nil, err
		}
		feeder, err = OpenBucketFeed(bucket)
		if err != nil {
			feed.errorf("OpenBucketFeed()", bucketn, err)
			return nil, err
		}
	}

	if stop {
		feed.infof("stop-timestamp", bucketn, reqTs)
		if err = feeder.EndVbStreams(opaque, reqTs); err != nil {
			feed.errorf("EndVbStreams()", bucketn, err)
			return nil, err
		}
	}

	if start {
		feed.infof("start-timestamp", bucketn, reqTs)
		if err = feeder.StartVbStreams(opaque, reqTs); err != nil {
			feed.errorf("StartVbStreams()", bucketn, err)
			return nil, err
		}
	}
	return feeder, nil
}

// - return go-couchbase failures.
func (feed *Feed) bucketDetails(pooln, bucketn string) ([]uint16, []uint64, error) {
	bucket, err := c.ConnectBucket(feed.cluster, pooln, bucketn)
	if err != nil {
		feed.errorf("ConnectBucket()", bucketn, err)
		return nil, nil, err
	}
	defer bucket.Close()

	// refresh vbmap before gathering vbucket-numbers hosted
	// by set of feed.kvaddrs.
	if err = bucket.Refresh(); err != nil {
		feed.errorf("bucket.Refresh()", bucketn, err)
		return nil, nil, err
	}
	m, err := bucket.GetVBmap(feed.kvaddrs)
	if err != nil {
		feed.errorf("bucket.GetVBmap()", bucketn, err)
		return nil, nil, err
	}
	vbnos := make([]uint16, 0, feed.maxVbuckets/10)
	for _, ns := range m {
		vbnos = append(vbnos, ns...)
	}

	// failover-logs
	flogs, err := bucket.GetFailoverLogs(vbnos)
	if err != nil {
		feed.errorf("bucket.GetFailoverLogs()", bucketn, err)
		return nil, nil, err
	}
	vbuuids := make([]uint64, len(vbnos))
	for i, vbno := range vbnos {
		flog := flogs[vbno]
		if len(flog) < 1 {
			feed.errorf("bucket.FailoverLog empty", bucketn, nil)
			return nil, nil, ErrorInvalidVbucket
		}
		vbuuids[i] = flog[len(flog)-1][0]
	}

	return vbnos, vbuuids, nil
}

// start data-path each kvaddr
func (feed *Feed) startDataPath(
	bucketn string, feeder BucketFeeder, reqTs *protobuf.TsVbuuid) map[string]*KVData {

	mutch := feeder.GetChannel()
	m := make(map[string]*KVData) // kvaddr -> kvdata
	for _, kvaddr := range feed.kvaddrs {
		// pass engines & endpoints to kvdata.
		kvdata := NewKVData(
			feed, bucketn, kvaddr, reqTs,
			feed.engines[bucketn], feed.endpoints, mutch)
		m[kvaddr] = kvdata
	}
	return m
}

// - return ErrorInconsistentFeed for malformed feed request
func (feed *Feed) processSubscribers(req Subscriber) error {
	evaluators, routers, err := feed.subscribers(req)
	if err != nil {
		return err
	}

	// start fresh set of all endpoints from routers.
	if err = feed.startEndpoints(routers); err != nil {
		return err
	}
	// update feed engines.
	for uuid, evaluator := range evaluators {
		bucketn := evaluator.Bucket()
		m, ok := feed.engines[bucketn]
		if !ok {
			m = make(map[uint64]*Engine)
		}
		engine := NewEngine(uuid, evaluator, routers[uuid])
		c.Infof("%v new engine %v created ...\n", feed.logPrefix, uuid)
		m[uuid] = engine
		feed.engines[bucketn] = m
	}
	return nil
}

// feed.endpoints is updated with freshly started endpoint,
// if an endpoint is already present and active it is
// reused.
func (feed *Feed) startEndpoints(routers map[uint64]c.Router) error {
	for _, router := range routers {
		for _, raddr := range router.Endpoints() {
			endpoint, ok := feed.endpoints[raddr]
			if (!ok) || (!endpoint.Ping()) {
				// ignore error while starting endpoint
				topic, typ := feed.topic, feed.endpointType
				endpoint, err := feed.epFactory(topic, typ, raddr)
				if err != nil {
					return err
				} else if endpoint != nil {
					feed.endpoints[raddr] = endpoint
				}
			}
		}
	}
	return nil
}

// - return ErrorInconsistentFeed for malformed feeds.
func (feed *Feed) subscribers(
	req Subscriber) (map[uint64]c.Evaluator, map[uint64]c.Router, error) {

	evaluators, err := req.GetEvaluators()
	if err != nil {
		return nil, nil, ErrorInconsistentFeed
	}
	routers, err := req.GetRouters()
	if err != nil {
		return nil, nil, ErrorInconsistentFeed
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

func (feed *Feed) engineNames() []string {
	names := make([]string, 0, len(feed.engines))
	for uuid := range feed.engines {
		names = append(names, fmt.Sprintf("%v", uuid))
	}
	return names
}

// wait for kvdata to post StreamRequest.
// - return ErrorResponseTimeout if feedback is not completed within timeout
func (feed *Feed) waitStreamRequests(
	opaque uint16,
	pooln, bucketn string, vbnos []uint16) (*protobuf.TsVbuuid, error) {

	rollTs := protobuf.NewTsVbuuid(pooln, bucketn, feed.maxVbuckets)

	if len(vbnos) == 0 {
		return rollTs, nil
	}

	timeout := time.After(feed.reqTimeout * time.Millisecond)

	err := feed.waitOnFeedback(timeout, func(msg interface{}) string {
		if val, ok := msg.(*controlStreamRequest); ok {
			if val.bucket == bucketn && val.opaque == opaque {
				if val.status == mcd.ROLLBACK {
					rollTs.Append(val.vbno, val.seqno, val.vbuuid, 0, 0)
				}
				vbnos = c.RemoveUint16(val.vbno, vbnos)
				if len(vbnos) == 0 {
					return "done"
				}
				return "ok"
			}
		}
		return "skip"
	})
	return rollTs, err
}

// wait for kvdata to post StreamEnd.
// - return ErrorResponseTimeout if feedback is not completed within timeout
func (feed *Feed) waitStreamEnds(
	opaque uint16, bucketn string, vbnos []uint16) error {

	if len(vbnos) == 0 {
		return nil
	}
	timeout := time.After(feed.endTimeout * time.Millisecond)
	err := feed.waitOnFeedback(timeout, func(msg interface{}) string {
		if val, ok := msg.(*controlStreamEnd); ok {
			if val.bucket == bucketn && val.opaque == opaque {
				vbnos = c.RemoveUint16(val.vbno, vbnos)
				if len(vbnos) == 0 {
					return "done"
				}
				return "ok"
			}
		}
		return "skip"
	})
	return err
}

// block feed until feedback posted back from kvdata.
// - return ErrorResponseTimeout if feedback is not completed within timeout
func (feed *Feed) waitOnFeedback(
	timeout <-chan time.Time, callb func(msg interface{}) string) (err error) {

	msgs := make([][]interface{}, 0)
loop:
	for {
		select {
		case msg := <-feed.backch:
			c.Infof("%v back channel %T %v", feed.logPrefix, msg[0], msg[0])
			switch callb(msg[0]) {
			case "skip":
				msgs = append(msgs, msg)
			case "done":
				break loop
			case "ok":
			}

		case <-timeout:
			err = ErrorResponseTimeout
			c.Errorf("%v feedback timeout %v\n", feed.logPrefix, err)
			break loop
		}
	}
	for _, msg := range msgs {
		feed.backch <- []interface{}{msg}
	}
	return
}

// compose topic-response for caller
func (feed *Feed) topicResponse() *protobuf.TopicResponse {
	uuids := make([]uint64, 0)
	for _, engines := range feed.engines {
		for uuid := range engines {
			uuids = append(uuids, uuid)
		}
	}
	xs := make([]*protobuf.TsVbuuid, 0, len(feed.reqTss))
	for _, ts := range feed.reqTss {
		xs = append(xs, ts)
	}
	ys := make([]*protobuf.TsVbuuid, 0, len(feed.rollTss))
	for _, ts := range feed.rollTss {
		if !ts.IsEmpty() {
			ys = append(ys, ts)
		}
	}
	return &protobuf.TopicResponse{
		Topic:              proto.String(feed.topic),
		InstanceIds:        uuids,
		ReqTimestamps:      xs,
		RollbackTimestamps: ys,
	}
}

// generate a new 16 bit opaque value set as MSB.
func newOpaque() uint16 {
	// bit 26 ... 42 from UnixNano().
	return uint16((uint64(time.Now().UnixNano()) >> 26) & 0xFFFF)
}

//---- local function

func (feed *Feed) errorf(prefix, bucketn string, val interface{}) {
	c.Errorf("%v %v for %q: %v\n", feed.logPrefix, prefix, bucketn, val)
}

func (feed *Feed) debugf(prefix, bucketn string, val interface{}) {
	c.Debugf("%v %v for %q: %v\n", feed.logPrefix, prefix, bucketn, val)
}

func (feed *Feed) infof(prefix, bucketn string, val interface{}) {
	c.Infof("%v %v for %q: %v\n", feed.logPrefix, prefix, bucketn, val)
}
