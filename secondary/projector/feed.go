package projector

import "errors"
import "fmt"
import "time"
import "runtime/debug"

import mcd "github.com/couchbase/indexing/secondary/dcp/transport"
import mc "github.com/couchbase/indexing/secondary/dcp/transport/client"
import c "github.com/couchbase/indexing/secondary/common"
import protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
import "github.com/couchbaselabs/goprotobuf/proto"

// error codes

// ErrorInvalidBucket
var ErrorInvalidBucket = errors.New("feed.invalidBucket")

// ErrorInvalidKVaddrs
var ErrorInvalidKVaddrs = errors.New("feed.invalidKVaddrs")

// ErrorInvalidVbucketBranch
var ErrorInvalidVbucketBranch = errors.New("feed.invalidVbucketBranch")

// ErrorInvalidVbucket
var ErrorInvalidVbucket = errors.New("feed.invalidVbucket")

// ErrorInconsistentFeed
var ErrorInconsistentFeed = errors.New("feed.inconsistentFeed")

// ErrorFeeder
var ErrorFeeder = errors.New("feed.feeder")

// ErrorNotMyVbucket
var ErrorNotMyVbucket = errors.New("feed.notMyVbucket")

// ErrorStreamRequest
var ErrorStreamRequest = errors.New("feed.streamRequest")

// ErrorStreamEnd
var ErrorStreamEnd = errors.New("feed.streamEnd")

// ErrorResponseTimeout is sent when projector does not recieve
// expected control message like StreamBegin (when stream is started)
// and StreamEnd (when stream is closed).
var ErrorResponseTimeout = errors.New("feed.responseTimeout")

// Feed is mutation stream - for maintenance, initial-load, catchup etc...
type Feed struct {
	cluster      string   // immutable
	topic        string   // immutable
	endpointType string   // immutable
	kvaddrs      []string // immutable

	// upstream
	// reqTs, book-keeping on outstanding request posted to feeder.
	// vbucket entry from this timestamp is deleted only when a SUCCESS,
	// ROLLBACK or ERROR response is received from feeder.
	reqTss map[string]*protobuf.TsVbuuid // bucket -> TsVbuuid
	// actTs, once StreamBegin SUCCESS response is got back from UPR,
	// vbucket entry is moved here.
	actTss map[string]*protobuf.TsVbuuid // bucket -> TsVbuuid
	// rollTs, when StreamBegin ROLLBACK response is got back from UPR,
	// vbucket entry is moved here.
	rollTss map[string]*protobuf.TsVbuuid // bucket -> TsVbuuid

	feeders map[string]BucketFeeder // bucket -> BucketFeeder{}
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
//    mutationChanSize: channel size of projector's data path routine
//    vbucketSyncTimeout: timeout, in ms, for sending periodic Sync messages
//    routerEndpointFactory: endpoint factory
func NewFeed(topic string, config c.Config) (*Feed, error) {
	epf := config["routerEndpointFactory"].Value.(c.RouterEndpointFactory)
	chsize := config["feedChanSize"].Int()
	feed := &Feed{
		cluster: config["clusterAddr"].String(),
		topic:   topic,
		kvaddrs: config["kvAddrs"].Strings(),

		// upstream
		reqTss:  make(map[string]*protobuf.TsVbuuid),
		actTss:  make(map[string]*protobuf.TsVbuuid),
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

	if len(feed.kvaddrs) == 0 {
		kvaddr, err := c.GetColocatedHost(feed.cluster)
		if err != nil {
			c.Errorf("%v error getting colocated host: %v", feed.logPrefix, err)
			return nil, ErrorInvalidKVaddrs
		}
		feed.kvaddrs = []string{kvaddr}
		c.Infof("%v kvaddrs %v\n", feed.logPrefix, feed.kvaddrs)
	}

	go feed.genServer()
	c.Infof("%v started ...\n", feed.logPrefix)
	return feed, nil
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
	fCmdGetTopicResponse
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

// GetTopicResponse for this feed.
// Synchronous call.
func (feed *Feed) GetTopicResponse() *protobuf.TopicResponse {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdGetTopicResponse, respch}
	resp, _ := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return resp[0].(*protobuf.TopicResponse)
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
			if v, ok := msg[0].(*controlStreamRequest); ok {
				reqTs, ok := feed.reqTss[v.bucket]
				seqno, vbuuid, sStart, sEnd, err := reqTs.Get(v.vbno)
				if err != nil {
					c.Errorf("%v unexpected %T for %v", feed.logPrefix, v, v)

				} else if ok {
					c.Infof("%v back channel flushed %T %v", feed.logPrefix, v, v)
					reqTs = reqTs.FilterByVbuckets([]uint16{v.vbno})
					feed.reqTss[v.bucket] = reqTs

					if v.status == mcd.ROLLBACK {
						rollTs := feed.rollTss[v.bucket]
						rollTs.Append(v.vbno, seqno, vbuuid, sStart, sEnd)

					} else if v.status == mcd.SUCCESS {
						actTs := feed.actTss[v.bucket]
						actTs.Append(v.vbno, seqno, vbuuid, sStart, sEnd)
					}
				}

			} else if v, ok := msg[0].(*controlStreamEnd); ok {
				c.Infof("%v back channel flushed %T %v", feed.logPrefix, v, v)
				reqTs := feed.reqTss[v.bucket]
				reqTs = reqTs.FilterByVbuckets([]uint16{v.vbno})
				feed.reqTss[v.bucket] = reqTs

				actTs := feed.actTss[v.bucket]
				actTs = actTs.FilterByVbuckets([]uint16{v.vbno})
				feed.actTss[v.bucket] = actTs

				rollTs := feed.rollTss[v.bucket]
				rollTs = rollTs.FilterByVbuckets([]uint16{v.vbno})
				feed.rollTss[v.bucket] = rollTs

			} else if v, ok := msg[0].(*controlFinKVData); ok {
				actTs, ok := feed.actTss[v.bucket]
				if ok && actTs.Len() == 0 { // bucket is done
					prefix := feed.logPrefix
					c.Infof("%v self deleting bucket %v", prefix, v.bucket)
					feed.cleanupBucket(v.bucket)

				} else if ok { // only a single kvnode for this bucket is done
					format := "%v self deleting kvdata {%v, %v}"
					c.Infof(format, feed.logPrefix, v.bucket, v.kvaddr)
					delete(feed.kvdata[v.bucket], v.kvaddr)
				}

			} else {
				c.Errorf("%v back channel flushed %T %v", feed.logPrefix, v, v)
			}

		case <-timeout:
			// TODO: should this be ERROR ?
			if len(feed.backch) > 0 {
				c.Debugf(ctrlMsg, feed.logPrefix, len(feed.backch))
			}
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

	case fCmdGetTopicResponse:
		respch := msg[1].(chan []interface{})
		respch <- []interface{}{feed.topicResponse()}

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
// - return ErrorFeeder if upstream connection has failures.
// - return ErrorNotMyVbucket due to rebalances and failures.
// - return ErrorStreamRequest if StreamRequest failed for some reason
// - return ErrorResponseTimeout if feedback is not completed within timeout.
func (feed *Feed) start(req *protobuf.MutationTopicRequest) (err error) {
	feed.endpointType = req.GetEndpointType()

	// update engines and endpoints
	if err = feed.processSubscribers(req); err != nil { // :SideEffect:
		return err
	}
	// iterate request-timestamp for each bucket.
	opaque := newOpaque()
	for _, ts := range req.GetReqTimestamps() {
		pooln, bucketn := ts.GetPool(), ts.GetBucket()
		actTs, ok := feed.actTss[bucketn]
		if ok { // don't re-request for already active vbuckets
			ts = ts.FilterByVbuckets(c.Vbno32to16(actTs.GetVbnos()))
		}
		rollTs, ok := feed.rollTss[bucketn]
		if ok { // forget previous rollback for the current set of vbuckets
			rollTs = rollTs.FilterByVbuckets(c.Vbno32to16(ts.GetVbnos()))
		}
		reqTs, ok := feed.reqTss[bucketn]
		// book-keeping of out-standing request, vbuckets that have
		// out-standing request will be ignored.
		if ok {
			ts = ts.FilterByVbuckets(c.Vbno32to16(reqTs.GetVbnos()))
		}
		reqTs = ts.Union(reqTs)
		// start upstream
		feeder, e := feed.bucketFeed(opaque, false, true, ts)
		if e != nil { // all feed errors are fatal, skip this bucket.
			feed.cleanupBucket(bucketn)
			err = ErrorFeeder
			continue
		}
		feed.feeders[bucketn] = feeder // :SideEffect:
		// open data-path, if not already open.
		m := feed.startDataPath(bucketn, feeder, ts)
		feed.kvdata[bucketn] = m // :SideEffect:
		// wait for stream to start ...
		r, f, a, e := feed.waitStreamRequests(opaque, pooln, bucketn, ts)
		feed.rollTss[bucketn] = rollTs.Union(r) // :SideEffect:
		feed.actTss[bucketn] = actTs.Union(a)   // :SideEffect:
		// forget vbuckets for which a response is already received.
		reqTs = reqTs.FilterByVbuckets(c.Vbno32to16(r.GetVbnos()))
		reqTs = reqTs.FilterByVbuckets(c.Vbno32to16(a.GetVbnos()))
		reqTs = reqTs.FilterByVbuckets(c.Vbno32to16(f.GetVbnos()))
		feed.reqTss[bucketn] = reqTs // :SideEffect:
		if e != nil {
			err = e
		}
		c.Infof("%v stream-request rollback: %v, success: vbnos %v #%x\n",
			feed.logPrefix, rollTs, actTs, opaque)
	}
	return err
}

// a subset of upstreams are restarted.
// - return ErrorInvalidBucket if bucket is not added.
// - return ErrorInvalidVbucketBranch for malformed vbuuid.
// - return ErrorFeeder if upstream connection has failures.
// - return ErrorNotMyVbucket due to rebalances and failures.
// - return ErrorStreamRequest if StreamRequest failed for some reason
// - return ErrorResponseTimeout if feedback is not completed within timeout.
func (feed *Feed) restartVbuckets(
	req *protobuf.RestartVbucketsRequest) (err error) {

	// iterate request-timestamp for each bucket.
	opaque := newOpaque()
	for _, ts := range req.GetRestartTimestamps() {
		pooln, bucketn := ts.GetPool(), ts.GetBucket()
		actTs, ok := feed.actTss[bucketn]
		if ok { // don't re-request for already active vbuckets
			ts = ts.FilterByVbuckets(c.Vbno32to16(actTs.GetVbnos()))
		}
		rollTs, ok := feed.rollTss[bucketn]
		if ok { // forget previous rollback for the current set of vbuckets
			rollTs = rollTs.FilterByVbuckets(c.Vbno32to16(ts.GetVbnos()))
		}
		reqTs, ok := feed.reqTss[bucketn]
		// book-keeping of out-standing request, vbuckets that have
		// out-standing request will be ignored.
		if ok {
			ts = ts.FilterByVbuckets(c.Vbno32to16(reqTs.GetVbnos()))
		}
		reqTs = ts.Union(ts)
		// if bucket already present update kvdata first.
		if _, ok := feed.kvdata[bucketn]; ok {
			for _, kvaddr := range feed.kvaddrs { // send seqno. to datapath
				feed.kvdata[bucketn][kvaddr].UpdateTs(ts)
			}
		}
		// (re)start the upstream
		feeder, e := feed.bucketFeed(opaque, false, true, ts)
		if e != nil { // all feed errors are fatal, skip this bucket.
			feed.cleanupBucket(bucketn)
			err = ErrorFeeder
			continue
		}
		feed.feeders[bucketn] = feeder // :SideEffect:
		// open data-path, if not already open.
		if _, ok := feed.kvdata[bucketn]; !ok {
			m := feed.startDataPath(bucketn, feeder, ts)
			feed.kvdata[bucketn] = m // :SideEffect:
		}
		// wait stream to start ...
		r, f, a, e := feed.waitStreamRequests(opaque, pooln, bucketn, ts)
		feed.rollTss[bucketn] = rollTs.Union(r) // :SideEffect:
		feed.actTss[bucketn] = actTs.Union(a)   // :SideEffect:
		// forget vbuckets for which a response is already received.
		reqTs = reqTs.FilterByVbuckets(c.Vbno32to16(r.GetVbnos()))
		reqTs = reqTs.FilterByVbuckets(c.Vbno32to16(a.GetVbnos()))
		reqTs = reqTs.FilterByVbuckets(c.Vbno32to16(f.GetVbnos()))
		feed.reqTss[bucketn] = reqTs // :SideEffect:
		if e != nil {
			err = e
		}
		c.Infof("%v stream-request rollback: %v, success: vbnos %v #%x\n",
			feed.logPrefix, rollTs, ts, opaque)
	}
	return err
}

// a subset of upstreams are closed.
// - return ErrorInvalidBucket if bucket is not added.
// - return ErrorInvalidVbucketBranch for malformed vbuuid.
// - return ErrorFeeder if upstream connection has failures.
// - return ErrorNotMyVbucket due to rebalances and failures.
// - return ErrorStreamEnd if StreamEnd failed for some reason
// - return ErrorResponseTimeout if feedback is not completed within timeout.
func (feed *Feed) shutdownVbuckets(
	req *protobuf.ShutdownVbucketsRequest) (err error) {
	// iterate request-timestamp for each bucket.
	opaque := newOpaque()
	for _, ts := range req.GetShutdownTimestamps() {
		bucketn := ts.GetBucket()
		actTs, ok1 := feed.actTss[bucketn]
		rollTs, ok2 := feed.rollTss[bucketn]
		reqTs, ok3 := feed.reqTss[bucketn]
		if !ok1 || !ok2 || !ok3 {
			msg := "%v shutdownVbuckets() invalid bucket %v\n"
			c.Errorf(msg, feed.logPrefix, bucketn)
			err = ErrorInvalidBucket
			continue
		}
		// shutdown upstream
		_, e := feed.bucketFeed(opaque, true, false, ts)
		if e != nil {
			feed.cleanupBucket(bucketn)
			err = e
			continue
		}
		endTs, _, e := feed.waitStreamEnds(opaque, bucketn, ts)
		vbnos := c.Vbno32to16(endTs.GetVbnos())
		// forget vbnos that are shutdown
		feed.actTss[bucketn] = actTs.FilterByVbuckets(vbnos)   // :SideEffect:
		feed.reqTss[bucketn] = reqTs.FilterByVbuckets(vbnos)   // :SideEffect:
		feed.rollTss[bucketn] = rollTs.FilterByVbuckets(vbnos) // :SideEffect:
		if e != nil {
			err = e
		}
		c.Infof("%v stream-end completed for bucket %v, vbnos %v #%x\n",
			feed.logPrefix, bucketn, vbnos, opaque)
	}
	return err
}

// upstreams are added for buckets data-path opened and
// vbucket-routines started.
// - return ErrorInconsistentFeed for malformed feed request
// - return ErrorInvalidVbucketBranch for malformed vbuuid.
// - return ErrorFeeder if upstream connection has failures.
// - return ErrorNotMyVbucket due to rebalances and failures.
// - return ErrorStreamRequest if StreamRequest failed for some reason
// - return ErrorResponseTimeout if feedback is not completed within timeout.
func (feed *Feed) addBuckets(req *protobuf.AddBucketsRequest) (err error) {
	// update engines and endpoints
	if err = feed.processSubscribers(req); err != nil { // :SideEffect:
		return err
	}

	// iterate request-timestamp for each bucket.
	opaque := newOpaque()
	for _, ts := range req.GetReqTimestamps() {
		pooln, bucketn := ts.GetPool(), ts.GetBucket()
		actTs, ok := feed.actTss[bucketn]
		if ok { // don't re-request for already active vbuckets
			ts.FilterByVbuckets(c.Vbno32to16(actTs.GetVbnos()))
		}
		rollTs, ok := feed.rollTss[bucketn]
		if ok { // foget previous rollback for the current set of buckets
			rollTs = rollTs.FilterByVbuckets(c.Vbno32to16(ts.GetVbnos()))
		}
		reqTs, ok := feed.reqTss[bucketn]
		// book-keeping of out-standing request, vbuckets that have
		// out-standing request will be ignored.
		if ok {
			ts = ts.FilterByVbuckets(c.Vbno32to16(reqTs.GetVbnos()))
		}
		reqTs = ts.Union(ts)
		// start upstream
		feeder, e := feed.bucketFeed(opaque, false, true, ts)
		if e != nil { // all feed errors are fatal, skip this bucket.
			feed.cleanupBucket(bucketn)
			err = ErrorFeeder
			continue
		}
		feed.feeders[bucketn] = feeder // :SideEffect:
		// open data-path, if not already open.
		m := feed.startDataPath(bucketn, feeder, ts)
		feed.kvdata[bucketn] = m // :SideEffect:
		// wait for stream to start ...
		r, f, a, e := feed.waitStreamRequests(opaque, pooln, bucketn, ts)
		feed.rollTss[bucketn] = rollTs.Union(r) // :SideEffect:
		feed.actTss[bucketn] = actTs.Union(a)   // :SideEffect
		// forget vbucket for which a response is already received.
		reqTs = reqTs.FilterByVbuckets(c.Vbno32to16(r.GetVbnos()))
		reqTs = reqTs.FilterByVbuckets(c.Vbno32to16(a.GetVbnos()))
		reqTs = reqTs.FilterByVbuckets(c.Vbno32to16(f.GetVbnos()))
		feed.reqTss[bucketn] = reqTs // :SideEffect:
		if e != nil {
			err = e
		}
		c.Infof("%v stream-request rollback: %v, success: vbnos %v #%x\n",
			feed.logPrefix, rollTs, actTs, opaque)
	}
	return err
}

// upstreams are closed for buckets, data-path is closed for downstream,
// vbucket-routines exits on StreamEnd
func (feed *Feed) delBuckets(req *protobuf.DelBucketsRequest) error {
	for _, bucketn := range req.GetBuckets() {
		feed.cleanupBucket(bucketn)
	}
	return nil
}

// only data-path shall be updated.
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
// * if it is the last instance defined on the bucket, then
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

// shutdown upstream, data-path and remove data-structure for this bucket.
func (feed *Feed) cleanupBucket(bucketn string) {
	delete(feed.engines, bucketn) // :SideEffect:
	delete(feed.reqTss, bucketn)  // :SideEffect:
	delete(feed.actTss, bucketn)  // :SideEffect:
	delete(feed.rollTss, bucketn) // :SideEffect:
	// close upstream
	feeder, ok := feed.feeders[bucketn]
	if ok {
		feeder.CloseFeed()
	}
	delete(feed.feeders, bucketn) // :SideEffect:
	// cleanup data structures.
	kvdatas, ok := feed.kvdata[bucketn]
	if ok {
		for _, kvdata := range kvdatas {
			kvdata.Close()
		}
	}
	delete(feed.kvdata, bucketn) // :SideEffect:
}

// start a feed for a bucket with a set of kvfeeder,
// based on vbmap and failover-logs.
func (feed *Feed) bucketFeed(
	opaque uint16,
	stop, start bool,
	reqTs *protobuf.TsVbuuid) (feeder BucketFeeder, err error) {

	pooln, bucketn := reqTs.GetPool(), reqTs.GetBucket()

	defer func() {
		if err != nil && feeder != nil {
			feed.infof("closing feed for", bucketn, nil)
			feeder.CloseFeed()
			feeder = nil
		}
	}()

	vbnos, vbuuids, err := feed.bucketDetails(pooln, bucketn)
	if err != nil {
		return nil, err
	}

	// if streams need to be started, make sure that branch
	// histories are the same.
	if start {
		if reqTs.VerifyBranch(vbnos, vbuuids) == false {
			feed.errorf("VerifyBranch()", bucketn, vbuuids)
			return nil, ErrorInvalidVbucketBranch
		}
	}

	reqTs = reqTs.SelectByVbuckets(vbnos) // only vbuckets relevant to kvaddrs.

	var ok bool

	feeder, ok = feed.feeders[bucketn]
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

	// stop and start are mutually exclusive
	if stop {
		feed.infof("stop-timestamp", bucketn, reqTs)
		if err = feeder.EndVbStreams(opaque, reqTs); err != nil {
			feed.errorf("EndVbStreams()", bucketn, err)
			return feeder, err
		}

	} else if start {
		feed.infof("start-timestamp", bucketn, reqTs)
		if err = feeder.StartVbStreams(opaque, reqTs); err != nil {
			feed.errorf("StartVbStreams()", bucketn, err)
			return feeder, err
		}
	}
	return feeder, nil
}

// - return dcp-client failures.
func (feed *Feed) bucketDetails(
	pooln, bucketn string) ([]uint16, []uint64, error) {

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
	bucketn string, feeder BucketFeeder,
	ts *protobuf.TsVbuuid) map[string]*KVData {

	mutch := feeder.GetChannel()
	m, ok := feed.kvdata[bucketn]
	if !ok {
		m = make(map[string]*KVData) // kvaddr -> kvdata
	}
	for _, kvaddr := range feed.kvaddrs {
		// spawn only when it is not already active
		if kvdata, ok := m[kvaddr]; ok {
			kvdata.UpdateTs(ts)

		} else {
			// pass engines & endpoints to kvdata.
			engs, ends := feed.engines[bucketn], feed.endpoints
			kvdata := NewKVData(feed, bucketn, kvaddr, ts, engs, ends, mutch)
			m[kvaddr] = kvdata
		}
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
		feed.engines[bucketn] = m // :SideEffect:
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
// - return ErrorNotMyVbucket if vbucket has migrated.
// - return ErrorStreamEnd for failed stream-end request.
func (feed *Feed) waitStreamRequests(
	opaque uint16,
	pooln, bucketn string,
	ts *protobuf.TsVbuuid) (rollTs, failTs, actTs *protobuf.TsVbuuid, err error) {

	vbnos := c.Vbno32to16(ts.GetVbnos())
	rollTs = protobuf.NewTsVbuuid(ts.GetPool(), ts.GetBucket(), len(vbnos))
	failTs = protobuf.NewTsVbuuid(ts.GetPool(), ts.GetBucket(), len(vbnos))
	actTs = protobuf.NewTsVbuuid(ts.GetPool(), ts.GetBucket(), len(vbnos))
	if len(vbnos) == 0 {
		return rollTs, failTs, actTs, nil
	}

	timeout := time.After(feed.reqTimeout * time.Millisecond)
	err1 := feed.waitOnFeedback(timeout, func(msg interface{}) string {
		if val, ok := msg.(*controlStreamRequest); ok && val.bucket == bucketn && val.opaque == opaque &&
			ts.Contains(val.vbno) {

			if val.status == mcd.SUCCESS {
				actTs.Append(val.vbno, val.seqno, val.vbuuid, 0, 0)
			} else if val.status == mcd.ROLLBACK {
				rollTs.Append(val.vbno, val.seqno, val.vbuuid, 0, 0)
			} else if val.status == mcd.NOT_MY_VBUCKET {
				failTs.Append(val.vbno, val.seqno, val.vbuuid, 0, 0)
				err = ErrorNotMyVbucket
			} else {
				failTs.Append(val.vbno, val.seqno, val.vbuuid, 0, 0)
				err = ErrorStreamRequest
			}
			vbnos = c.RemoveUint16(val.vbno, vbnos)
			if len(vbnos) == 0 {
				return "done"
			}
			return "ok"
		}
		return "skip"
	})
	if err == nil {
		err = err1
	}
	return rollTs, failTs, actTs, err
}

// wait for kvdata to post StreamEnd.
// - return ErrorResponseTimeout if feedback is not completed within timeout.
// - return ErrorNotMyVbucket if vbucket has migrated.
// - return ErrorStreamEnd for failed stream-end request.
func (feed *Feed) waitStreamEnds(
	opaque uint16,
	bucketn string,
	ts *protobuf.TsVbuuid) (endTs, failTs *protobuf.TsVbuuid, err error) {

	vbnos := c.Vbno32to16(ts.GetVbnos())
	endTs = protobuf.NewTsVbuuid(ts.GetPool(), ts.GetBucket(), len(vbnos))
	failTs = protobuf.NewTsVbuuid(ts.GetPool(), ts.GetBucket(), len(vbnos))
	if len(vbnos) == 0 {
		return endTs, failTs, nil
	}

	timeout := time.After(feed.endTimeout * time.Millisecond)
	err1 := feed.waitOnFeedback(timeout, func(msg interface{}) string {
		if val, ok := msg.(*controlStreamEnd); ok && val.bucket == bucketn && val.opaque == opaque &&
			ts.Contains(val.vbno) {

			if val.status == mcd.SUCCESS {
				endTs.Append(val.vbno, 0 /*seqno*/, 0 /*vbuuid*/, 0, 0)
			} else if val.status == mcd.NOT_MY_VBUCKET {
				failTs.Append(val.vbno, 0 /*seqno*/, 0 /*vbuuid*/, 0, 0)
				err = ErrorNotMyVbucket
			} else {
				failTs.Append(val.vbno, 0 /*seqno*/, 0 /*vbuuid*/, 0, 0)
				err = ErrorStreamEnd
			}
			vbnos = c.RemoveUint16(val.vbno, vbnos)
			if len(vbnos) == 0 {
				return "done"
			}
			return "ok"
		}
		return "skip"
	})
	if err == nil {
		err = err1
	}
	return endTs, failTs, err
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
	// re-populate in the same order.
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
	xs := make([]*protobuf.TsVbuuid, 0, len(feed.actTss))
	for _, ts := range feed.actTss {
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
		ActiveTimestamps:   xs,
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
