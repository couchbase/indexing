package projector

import "fmt"
import "time"

import "github.com/couchbase/indexing/secondary/logging"
import "github.com/couchbase/indexing/secondary/dcp"
import mcd "github.com/couchbase/indexing/secondary/dcp/transport"
import mc "github.com/couchbase/indexing/secondary/dcp/transport/client"
import c "github.com/couchbase/indexing/secondary/common"
import protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
import projC "github.com/couchbase/indexing/secondary/projector/client"
import "github.com/couchbaselabs/goprotobuf/proto"

// Feed is mutation stream - for maintenance, initial-load, catchup etc...
type Feed struct {
	cluster      string // immutable
	topic        string // immutable
	endpointType string // immutable

	// upstream
	// reqTs, book-keeping on outstanding request posted to feeder.
	// vbucket entry from this timestamp is deleted only when a SUCCESS,
	// ROLLBACK or ERROR response is received from feeder.
	reqTss map[string]*protobuf.TsVbuuid // bucket -> TsVbuuid
	// actTs, once StreamBegin SUCCESS response is got back from DCP,
	// vbucket entry is moved here.
	actTss map[string]*protobuf.TsVbuuid // bucket -> TsVbuuid
	// rollTs, when StreamBegin ROLLBACK response is got back from DCP,
	// vbucket entry is moved here.
	rollTss map[string]*protobuf.TsVbuuid // bucket -> TsVbuuid

	feeders map[string]BucketFeeder // bucket -> BucketFeeder{}
	// downstream
	kvdata    map[string]*KVData            // bucket -> kvdata
	engines   map[string]map[uint64]*Engine // bucket -> uuid -> engine
	endpoints map[string]c.RouterEndpoint
	// genServer channel
	reqch  chan []interface{}
	backch chan []interface{}
	finch  chan bool
	// book-keeping for stale-ness
	stale int

	// config params
	reqTimeout time.Duration
	endTimeout time.Duration
	epFactory  c.RouterEndpointFactory
	config     c.Config
	logPrefix  string
}

// NewFeed creates a new topic feed.
// `config` contains following keys.
//    clusterAddr: KV cluster address <host:port>.
//    feedWaitStreamReqTimeout: wait for a response to StreamRequest
//    feedWaitStreamEndTimeout: wait for a response to StreamEnd
//    feedChanSize: channel size for feed's control path and back path
//    mutationChanSize: channel size of projector's data path routine
//    vbucketSyncTimeout: timeout, in ms, for sending periodic Sync messages
//    routerEndpointFactory: endpoint factory
func NewFeed(topic string, config c.Config, opaque uint16) (*Feed, error) {
	epf := config["routerEndpointFactory"].Value.(c.RouterEndpointFactory)
	chsize := config["feedChanSize"].Int()
	feed := &Feed{
		cluster: config["clusterAddr"].String(),
		topic:   topic,

		// upstream
		reqTss:  make(map[string]*protobuf.TsVbuuid),
		actTss:  make(map[string]*protobuf.TsVbuuid),
		rollTss: make(map[string]*protobuf.TsVbuuid),
		feeders: make(map[string]BucketFeeder),
		// downstream
		kvdata:    make(map[string]*KVData),
		engines:   make(map[string]map[uint64]*Engine),
		endpoints: make(map[string]c.RouterEndpoint),
		// genServer channel
		reqch:  make(chan []interface{}, chsize),
		backch: make(chan []interface{}, chsize),
		finch:  make(chan bool),

		reqTimeout: time.Duration(config["feedWaitStreamReqTimeout"].Int()),
		endTimeout: time.Duration(config["feedWaitStreamEndTimeout"].Int()),
		epFactory:  epf,
		config:     config,
	}
	feed.logPrefix = fmt.Sprintf("FEED[<=>%v(%v)]", topic, feed.cluster)

	go feed.genServer()
	logging.Infof("%v feed started ...\n", feed.logPrefix)
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
	fCmdStaleCheck
	fCmdShutdown
	fCmdGetTopicResponse
	fCmdGetStatistics
	fCmdResetConfig
)

// ResetConfig for this feed.
func (feed *Feed) ResetConfig(config c.Config) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdResetConfig, config, respch}
	_, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return err
}

// MutationTopic will start the feed.
// Synchronous call.
func (feed *Feed) MutationTopic(
	req *protobuf.MutationTopicRequest, opaque uint16) (*protobuf.TopicResponse, error) {

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdStart, req, opaque, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return resp[0].(*protobuf.TopicResponse), c.OpError(err, resp, 1)
}

// RestartVbuckets will restart upstream vbuckets for specified buckets.
// Synchronous call.
func (feed *Feed) RestartVbuckets(
	req *protobuf.RestartVbucketsRequest, opaque uint16) (*protobuf.TopicResponse, error) {

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdRestartVbuckets, req, opaque, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return resp[0].(*protobuf.TopicResponse), c.OpError(err, resp, 1)
}

// ShutdownVbuckets will shutdown streams for
// specified buckets.
// Synchronous call.
func (feed *Feed) ShutdownVbuckets(
	req *protobuf.ShutdownVbucketsRequest, opaque uint16) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdShutdownVbuckets, req, opaque, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return c.OpError(err, resp, 0)
}

// AddBuckets will remove buckets and all its upstream
// and downstream elements, except endpoints.
// Synchronous call.
func (feed *Feed) AddBuckets(
	req *protobuf.AddBucketsRequest,
	opaque uint16) (*protobuf.TopicResponse, error) {

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdAddBuckets, req, opaque, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return resp[0].(*protobuf.TopicResponse), c.OpError(err, resp, 1)
}

// DelBuckets will remove buckets and all its upstream
// and downstream elements, except endpoints.
// Synchronous call.
func (feed *Feed) DelBuckets(
	req *protobuf.DelBucketsRequest, opaque uint16) error {

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdDelBuckets, req, opaque, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return c.OpError(err, resp, 0)
}

// AddInstances will restart specified endpoint-address if
// it is not active already.
// Synchronous call.
func (feed *Feed) AddInstances(
	req *protobuf.AddInstancesRequest, opaque uint16) error {

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdAddInstances, req, opaque, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return c.OpError(err, resp, 0)
}

// DelInstances will restart specified endpoint-address if
// it is not active already.
// Synchronous call.
func (feed *Feed) DelInstances(
	req *protobuf.DelInstancesRequest, opaque uint16) error {

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdDelInstances, req, opaque, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return c.OpError(err, resp, 0)
}

// RepairEndpoints will restart specified endpoint-address if
// it is not active already.
// Synchronous call.
func (feed *Feed) RepairEndpoints(
	req *protobuf.RepairEndpointsRequest, opaque uint16) error {

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdRepairEndpoints, req, opaque, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return c.OpError(err, resp, 0)
}

// StaleCheck will check for feed sanity and return "exit" if feed
// has was already stale and still stale.
// Synchronous call.
func (feed *Feed) StaleCheck() (string, error) {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdStaleCheck, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	if err != nil {
		return "exit", err
	}
	return resp[0].(string), nil
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
func (feed *Feed) Shutdown(opaque uint16) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdShutdown, opaque, respch}
	_, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return err
}

type controlStreamRequest struct {
	bucket string
	opaque uint16
	status mcd.Status
	vbno   uint16
	vbuuid uint64
	seqno  uint64 // also doubles as rollback-seqno
}

func (v *controlStreamRequest) Repr() string {
	return fmt.Sprintf("{controlStreamRequest, %v, %s, %d, %x, %d, ##%x}",
		v.status, v.bucket, v.vbno, v.vbuuid, v.seqno, v.opaque)
}

// PostStreamRequest feedback from data-path.
// Asynchronous call.
func (feed *Feed) PostStreamRequest(bucket string, m *mc.DcpEvent) {
	var respch chan []interface{}
	cmd := &controlStreamRequest{
		bucket: bucket,
		opaque: m.Opaque,
		status: m.Status,
		vbno:   m.VBucket,
		vbuuid: m.VBuuid,
		seqno:  m.Seqno, // can also be roll-back seqno, based on status
	}
	fmsg := "%v ##%x backch %T %v\n"
	logging.Infof(fmsg, feed.logPrefix, m.Opaque, cmd, cmd.Repr())
	err := c.FailsafeOpNoblock(feed.backch, []interface{}{cmd}, feed.finch)
	if err == c.ErrorChannelFull {
		fmsg := "%v ##%x backch blocked on PostStreamRequest\n"
		logging.Errorf(fmsg, feed.logPrefix, m.Opaque)
		// block now !!
		c.FailsafeOp(feed.backch, respch, []interface{}{cmd}, feed.finch)
	}
}

type controlStreamEnd struct {
	bucket string
	opaque uint16
	status mcd.Status
	vbno   uint16
}

func (v *controlStreamEnd) Repr() string {
	return fmt.Sprintf("{controlStreamEnd, %v, %s, %d, %x}",
		v.status, v.bucket, v.vbno, v.opaque)
}

// PostStreamEnd feedback from data-path.
// Asynchronous call.
func (feed *Feed) PostStreamEnd(bucket string, m *mc.DcpEvent) {
	var respch chan []interface{}
	cmd := &controlStreamEnd{
		bucket: bucket,
		opaque: m.Opaque,
		status: m.Status,
		vbno:   m.VBucket,
	}
	fmsg := "%v ##%x backch %T %v\n"
	logging.Infof(fmsg, feed.logPrefix, m.Opaque, cmd, cmd.Repr())
	err := c.FailsafeOpNoblock(feed.backch, []interface{}{cmd}, feed.finch)
	if err == c.ErrorChannelFull {
		fmsg := "%v ##%x backch blocked on PostStreamEnd\n"
		logging.Errorf(fmsg, feed.logPrefix, m.Opaque)
		// block now !!
		c.FailsafeOp(feed.backch, respch, []interface{}{cmd}, feed.finch)
	}
}

type controlFinKVData struct {
	bucket string
}

func (v *controlFinKVData) Repr() string {
	return fmt.Sprintf("{controlFinKVData, %s}", v.bucket)
}

// PostFinKVdata feedback from data-path.
// Asynchronous call.
func (feed *Feed) PostFinKVdata(bucket string) {
	var respch chan []interface{}
	cmd := &controlFinKVData{bucket: bucket}
	fmsg := "%v backch %T %v\n"
	logging.Infof(fmsg, feed.logPrefix, cmd, cmd.Repr())
	err := c.FailsafeOpNoblock(feed.backch, []interface{}{cmd}, feed.finch)
	if err == c.ErrorChannelFull {
		fmsg := "%v backch blocked on PostFinKVdata\n"
		logging.Errorf(fmsg, feed.logPrefix)
		// block now !!
		c.FailsafeOp(feed.backch, respch, []interface{}{cmd}, feed.finch)
	}
}

func (feed *Feed) genServer() {
	defer func() { // panic safe
		if r := recover(); r != nil {
			logging.Errorf("%v feed gen-server crashed: %v\n", feed.logPrefix, r)
			logging.Errorf("%s", logging.StackTrace())
			feed.shutdown(0xFFFF)
		}
	}()

	var msg []interface{}

	timeout := time.Tick(1000 * time.Millisecond)
	ctrlMsg := "%v control channel has %v messages"

loop:
	for {
		select {
		case msg = <-feed.reqch:
			switch feed.handleCommand(msg) {
			case "ok":
				feed.stale = 0

			case "stale":
				if feed.stale == 1 { // already gone stale.
					logging.Warnf("%v feed collect stale...", feed.logPrefix)
					break loop
				}
				logging.Warnf("%v feed mark stale...", feed.logPrefix)
				feed.stale++

			case "exit":
				break loop
			}

		case msg = <-feed.backch:
			prefix := feed.logPrefix
			if cmd, ok := msg[0].(*controlStreamRequest); ok {
				reqTs, ok := feed.reqTss[cmd.bucket]
				seqno, vbuuid, sStart, sEnd, err := reqTs.Get(cmd.vbno)
				if err != nil {
					fmsg := "%v ##%x backch flush %T: %v\n"
					logging.Fatalf(fmsg, prefix, cmd.opaque, cmd, err)

				} else if ok {
					reqTs = reqTs.FilterByVbuckets([]uint16{cmd.vbno})
					feed.reqTss[cmd.bucket] = reqTs

					if cmd.status == mcd.ROLLBACK {
						fmsg := "%v ##%x backch flush rollback %T: %v\n"
						logging.Infof(fmsg, prefix, cmd, cmd.opaque, cmd.Repr())
						rollTs := feed.rollTss[cmd.bucket]
						rollTs.Append(cmd.vbno, cmd.seqno, vbuuid, sStart, sEnd)

					} else if cmd.status == mcd.SUCCESS {
						fmsg := "%v ##%x backch flush success %T: %v\n"
						logging.Infof(fmsg, prefix, cmd, cmd.opaque, cmd.Repr())
						actTs := feed.actTss[cmd.bucket]
						actTs.Append(cmd.vbno, seqno, vbuuid, sStart, sEnd)

					} else {
						fmsg := "%v ##%x backch flush error %T: %v\n"
						logging.Errorf(fmsg, prefix, cmd, cmd.opaque, cmd.Repr())
					}
				}

			} else if cmd, ok := msg[0].(*controlStreamEnd); ok {
				fmsg := "%v ##%x backch flush %T: %v\n"
				logging.Infof(fmsg, prefix, cmd.opaque, cmd, cmd.Repr())
				reqTs := feed.reqTss[cmd.bucket]
				reqTs = reqTs.FilterByVbuckets([]uint16{cmd.vbno})
				feed.reqTss[cmd.bucket] = reqTs

				actTs := feed.actTss[cmd.bucket]
				actTs = actTs.FilterByVbuckets([]uint16{cmd.vbno})
				feed.actTss[cmd.bucket] = actTs

				rollTs := feed.rollTss[cmd.bucket]
				rollTs = rollTs.FilterByVbuckets([]uint16{cmd.vbno})
				feed.rollTss[cmd.bucket] = rollTs

			} else if cmd, ok := msg[0].(*controlFinKVData); ok {
				fmsg := "%v backch flush %T -- %v\n"
				logging.Infof(fmsg, prefix, cmd, cmd.Repr())
				actTs, ok := feed.actTss[cmd.bucket]
				if ok && actTs != nil && actTs.Len() == 0 { // bucket is done
					logging.Infof("%v self deleting bucket\n", prefix)
					feed.cleanupBucket(cmd.bucket, false)

				} else if actTs != nil && actTs.Len() == 0 {
					fmsg := "%v FinKVData before StreamEnds %v\n"
					logging.Fatalf(fmsg, prefix, actTs)

				} else {
					fmsg := "%v FinKVData can't find bucket %q\n"
					logging.Fatalf(fmsg, prefix, cmd.bucket)
				}

			} else {
				logging.Fatalf("%v backch flush %T: %v\n", prefix, msg[0], msg[0])
			}

		case <-timeout:
			if len(feed.backch) > 0 { // can happend during rebalance.
				logging.Infof(ctrlMsg, feed.logPrefix, len(feed.backch))
			}
		}
	}
	timeout = nil
}

// "ok"    - command handled.
// "stale" - feed has gone stale.
// "exit"  - feed was already stale, so exit feed.
func (feed *Feed) handleCommand(msg []interface{}) (status string) {
	status = "ok"

	switch cmd := msg[0].(byte); cmd {
	case fCmdStart:
		req := msg[1].(*protobuf.MutationTopicRequest)
		opaque, respch := msg[2].(uint16), msg[3].(chan []interface{})
		err := feed.start(req, opaque)
		response := feed.topicResponse()
		respch <- []interface{}{response, err}

	case fCmdRestartVbuckets:
		req := msg[1].(*protobuf.RestartVbucketsRequest)
		opaque, respch := msg[2].(uint16), msg[3].(chan []interface{})
		err := feed.restartVbuckets(req, opaque)
		response := feed.topicResponse()
		respch <- []interface{}{response, err}

	case fCmdShutdownVbuckets:
		req := msg[1].(*protobuf.ShutdownVbucketsRequest)
		opaque, respch := msg[2].(uint16), msg[3].(chan []interface{})
		respch <- []interface{}{feed.shutdownVbuckets(req, opaque)}

	case fCmdAddBuckets:
		req := msg[1].(*protobuf.AddBucketsRequest)
		opaque, respch := msg[2].(uint16), msg[3].(chan []interface{})
		err := feed.addBuckets(req, opaque)
		response := feed.topicResponse()
		respch <- []interface{}{response, err}

	case fCmdDelBuckets:
		req := msg[1].(*protobuf.DelBucketsRequest)
		opaque, respch := msg[2].(uint16), msg[3].(chan []interface{})
		respch <- []interface{}{feed.delBuckets(req, opaque)}

	case fCmdAddInstances:
		req := msg[1].(*protobuf.AddInstancesRequest)
		opaque, respch := msg[2].(uint16), msg[3].(chan []interface{})
		respch <- []interface{}{feed.addInstances(req, opaque)}

	case fCmdDelInstances:
		req := msg[1].(*protobuf.DelInstancesRequest)
		opaque, respch := msg[2].(uint16), msg[3].(chan []interface{})
		respch <- []interface{}{feed.delInstances(req, opaque)}

	case fCmdRepairEndpoints:
		req := msg[1].(*protobuf.RepairEndpointsRequest)
		opaque, respch := msg[2].(uint16), msg[3].(chan []interface{})
		respch <- []interface{}{feed.repairEndpoints(req, opaque)}

	case fCmdStaleCheck:
		respch := msg[1].(chan []interface{})
		what := feed.staleCheck()
		status = what
		respch <- []interface{}{what}

	case fCmdGetTopicResponse:
		respch := msg[1].(chan []interface{})
		respch <- []interface{}{feed.topicResponse()}

	case fCmdGetStatistics:
		respch := msg[1].(chan []interface{})
		respch <- []interface{}{feed.getStatistics()}

	case fCmdResetConfig:
		config, respch := msg[1].(c.Config), msg[2].(chan []interface{})
		feed.resetConfig(config)
		respch <- []interface{}{nil}

	case fCmdShutdown:
		opaque := msg[1].(uint16)
		respch := msg[2].(chan []interface{})
		respch <- []interface{}{feed.shutdown(opaque)}
		status = "exit"

	}
	return status
}

// start a new feed.
// - return ErrorInconsistentFeed for malformed feed request
// - return ErrorInvalidVbucketBranch for malformed vbuuid.
// - return ErrorFeeder if upstream connection has failures.
// - return ErrorNotMyVbucket due to rebalances and failures.
// - return ErrorStreamRequest if StreamRequest failed for some reason
// - return ErrorResponseTimeout if feedback is not completed within timeout.
func (feed *Feed) start(
	req *protobuf.MutationTopicRequest, opaque uint16) (err error) {

	feed.endpointType = req.GetEndpointType()

	// update engines and endpoints
	if err = feed.processSubscribers(opaque, req); err != nil { // :SideEffect:
		return err
	}
	for _, ts := range req.GetReqTimestamps() {
		pooln, bucketn := ts.GetPool(), ts.GetBucket()
		vbnos, e := feed.getLocalVbuckets(pooln, bucketn, opaque)
		if e != nil {
			err = e
			feed.cleanupBucket(bucketn, false)
			continue
		}
		ts := ts.SelectByVbuckets(vbnos) // take only local vbuckets

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
		// open or acquire the upstream feeder object.
		feeder, e := feed.openFeeder(opaque, pooln, bucketn)
		if e != nil {
			err = e
			feed.cleanupBucket(bucketn, false)
			continue
		}
		feed.feeders[bucketn] = feeder // :SideEffect:
		// open data-path, if not already open.
		kvdata := feed.startDataPath(bucketn, feeder, opaque, ts)
		engines, _ := feed.engines[bucketn]
		kvdata.AddEngines(opaque, engines, feed.endpoints)
		feed.kvdata[bucketn] = kvdata // :SideEffect:
		// start upstream, after filtering out vbuckets.
		e = feed.bucketFeed(opaque, false, true, ts, feeder)
		if e != nil { // all feed errors are fatal, skip this bucket.
			err = e
			feed.cleanupBucket(bucketn, false)
			continue
		}
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
			fmsg := "%v ##%x stream-request: %v\n"
			logging.Errorf(fmsg, feed.logPrefix, opaque, err)
		}
		fmsg := "%v ##%x stream-request %s, rollback: %v, success: vbnos %v\n"
		logging.Infof(
			fmsg,
			feed.logPrefix, opaque, bucketn,
			feed.rollTss[bucketn].GetVbnos(),
			feed.actTss[bucketn].GetVbnos())
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
	req *protobuf.RestartVbucketsRequest, opaque uint16) (err error) {

	// FIXME: restart-vbuckets implies a repair Endpoint.
	raddrs := feed.endpointRaddrs()
	rpReq := protobuf.NewRepairEndpointsRequest(feed.topic, raddrs)
	feed.repairEndpoints(rpReq, opaque)

	for _, ts := range req.GetRestartTimestamps() {
		pooln, bucketn := ts.GetPool(), ts.GetBucket()
		vbnos, e := feed.getLocalVbuckets(pooln, bucketn, opaque)
		if e != nil {
			err = e
			feed.cleanupBucket(bucketn, false)
			continue
		}
		ts := ts.SelectByVbuckets(vbnos)

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
		// open or acquire the upstream feeder object.
		feeder, e := feed.openFeeder(opaque, pooln, bucketn)
		if e != nil {
			err = e
			feed.cleanupBucket(bucketn, false)
			continue
		}
		feed.feeders[bucketn] = feeder // :SideEffect:
		// open data-path, if not already open.
		kvdata := feed.startDataPath(bucketn, feeder, opaque, ts)
		feed.kvdata[bucketn] = kvdata // :SideEffect:
		// (re)start the upstream, after filtering out remote vbuckets.
		e = feed.bucketFeed(opaque, false, true, ts, feeder)
		if e != nil { // all feed errors are fatal, skip this bucket.
			err = e
			feed.cleanupBucket(bucketn, false)
			continue
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
			fmsg := "%v ##%x stream-request: %v\n"
			logging.Errorf(fmsg, feed.logPrefix, opaque, err)
		}
		fmsg := "%v ##%x stream-request %s, rollback: %v, success: vbnos %v\n"
		logging.Infof(
			fmsg,
			feed.logPrefix, opaque, bucketn,
			feed.rollTss[bucketn].GetVbnos(),
			feed.actTss[bucketn].GetVbnos())
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
	req *protobuf.ShutdownVbucketsRequest, opaque uint16) (err error) {

	// iterate request-timestamp for each bucket.
	for _, ts := range req.GetShutdownTimestamps() {
		pooln, bucketn := ts.GetPool(), ts.GetBucket()
		vbnos, e := feed.getLocalVbuckets(pooln, bucketn, opaque)
		if e != nil {
			err = e
			//FIXME: in case of shutdown we are not cleaning the bucket !
			//wait for the code to settle-down and remove this.
			//feed.cleanupBucket(bucketn, false)
			continue
		}
		ts := ts.SelectByVbuckets(vbnos)

		actTs, ok1 := feed.actTss[bucketn]
		rollTs, ok2 := feed.rollTss[bucketn]
		reqTs, ok3 := feed.reqTss[bucketn]
		if !ok1 || !ok2 || !ok3 {
			fmsg := "%v ##%x shutdownVbuckets() invalid-bucket %v\n"
			logging.Errorf(fmsg, feed.logPrefix, opaque, bucketn)
			err = projC.ErrorInvalidBucket
			continue
		}
		// only shutdown active-streams.
		if ok1 && actTs != nil {
			ts = ts.FilterByVbuckets(c.Vbno32to16(actTs.GetVbnos()))
		}
		feeder, ok := feed.feeders[bucketn]
		if !ok {
			fmsg := "%v ##%x shutdownVbuckets() invalid-feeder %v\n"
			logging.Errorf(fmsg, feed.logPrefix, opaque, bucketn)
			err = projC.ErrorInvalidBucket
			continue
		}
		// shutdown upstream
		e = feed.bucketFeed(opaque, true, false, ts, feeder)
		if e != nil {
			err = e
			//FIXME: in case of shutdown we are not cleaning the bucket !
			//wait for the code to settle-down and remove this.
			//feed.cleanupBucket(bucketn, false)
			continue
		}
		endTs, _, e := feed.waitStreamEnds(opaque, bucketn, ts)
		vbnos = c.Vbno32to16(endTs.GetVbnos())
		// forget vbnos that are shutdown
		feed.actTss[bucketn] = actTs.FilterByVbuckets(vbnos)   // :SideEffect:
		feed.reqTss[bucketn] = reqTs.FilterByVbuckets(vbnos)   // :SideEffect:
		feed.rollTss[bucketn] = rollTs.FilterByVbuckets(vbnos) // :SideEffect:
		if e != nil {
			err = e
			fmsg := "%v ##%x stream-end: %v\n"
			logging.Errorf(fmsg, feed.logPrefix, opaque, err)
		}
		fmsg := "%v ##%x stream-end completed for bucket %v, vbnos %v\n"
		logging.Infof(fmsg, feed.logPrefix, opaque, bucketn, vbnos)
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
func (feed *Feed) addBuckets(
	req *protobuf.AddBucketsRequest, opaque uint16) (err error) {

	// update engines and endpoints
	if err = feed.processSubscribers(opaque, req); err != nil { // :SideEffect:
		return err
	}

	for _, ts := range req.GetReqTimestamps() {
		pooln, bucketn := ts.GetPool(), ts.GetBucket()
		vbnos, e := feed.getLocalVbuckets(pooln, bucketn, opaque)
		if e != nil {
			err = e
			feed.cleanupBucket(bucketn, false)
			continue
		}
		ts := ts.SelectByVbuckets(vbnos)

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
		// open or acquire the upstream feeder object.
		feeder, e := feed.openFeeder(opaque, pooln, bucketn)
		if e != nil {
			err = e
			feed.cleanupBucket(bucketn, false)
			continue
		}
		feed.feeders[bucketn] = feeder // :SideEffect:
		// open data-path, if not already open.
		kvdata := feed.startDataPath(bucketn, feeder, opaque, ts)
		engines, _ := feed.engines[bucketn]
		kvdata.AddEngines(opaque, engines, feed.endpoints)
		feed.kvdata[bucketn] = kvdata // :SideEffect:
		// start upstream
		e = feed.bucketFeed(opaque, false, true, ts, feeder)
		if e != nil { // all feed errors are fatal, skip this bucket.
			err = e
			feed.cleanupBucket(bucketn, false)
			continue
		}
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
			fmsg := "%v ##%x stream-request: %v\n"
			logging.Errorf(fmsg, feed.logPrefix, opaque, err)
		}
		fmsg := "%v ##%x stream-request %s, rollback: %v, success: vbnos %v\n"
		logging.Infof(
			fmsg, feed.logPrefix, opaque, bucketn,
			feed.rollTss[bucketn].GetVbnos(),
			feed.actTss[bucketn].GetVbnos())
	}
	return err
}

// upstreams are closed for buckets, data-path is closed for downstream,
// vbucket-routines exits on StreamEnd
func (feed *Feed) delBuckets(
	req *protobuf.DelBucketsRequest, opaque uint16) error {

	for _, bucketn := range req.GetBuckets() {
		feed.cleanupBucket(bucketn, true)
	}
	return nil
}

// only data-path shall be updated.
// - return ErrorInconsistentFeed for malformed feed request
func (feed *Feed) addInstances(
	req *protobuf.AddInstancesRequest, opaque uint16) error {

	// update engines and endpoints
	if err := feed.processSubscribers(opaque, req); err != nil { // :SideEffect:
		return err
	}
	var err error
	// post to kv data-path
	for bucketn, engines := range feed.engines {
		if _, ok := feed.kvdata[bucketn]; ok {
			feed.kvdata[bucketn].AddEngines(opaque, engines, feed.endpoints)
		} else {
			fmsg := "%v ##%x addInstances() invalid-bucket %q\n"
			logging.Errorf(fmsg, feed.logPrefix, opaque, bucketn)
			err = projC.ErrorInvalidBucket
		}
	}
	return err
}

// only data-path shall be updated.
// * if it is the last instance defined on the bucket, then
//   use delBuckets() API to delete the bucket.
func (feed *Feed) delInstances(
	req *protobuf.DelInstancesRequest, opaque uint16) error {

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
	var err error
	// posted post to kv data-path.
	for bucketn, uuids := range bucknIds {
		if _, ok := feed.kvdata[bucketn]; ok {
			feed.kvdata[bucketn].DeleteEngines(opaque, uuids)
		} else {
			fmsg := "%v ##%x delInstances() invalid-bucket %q"
			logging.Errorf(fmsg, feed.logPrefix, opaque, bucketn)
			err = projC.ErrorInvalidBucket
		}
	}
	feed.engines = fengines // :SideEffect:
	return err
}

// endpoints are independent.
func (feed *Feed) repairEndpoints(
	req *protobuf.RepairEndpointsRequest, opaque uint16) (err error) {

	prefix := feed.logPrefix
	for _, raddr := range req.GetEndpoints() {
		logging.Infof("%v ##%x trying to repair %q\n", prefix, opaque, raddr)
		raddr1, endpoint, e := feed.getEndpoint(raddr, opaque)
		if e != nil {
			err = e
			continue

		} else if (endpoint == nil) || !endpoint.Ping() {
			topic, typ := feed.topic, feed.endpointType
			config := feed.config.SectionConfig("dataport.", true /*trim*/)
			endpoint, e = feed.epFactory(topic, typ, raddr, config)
			if e != nil {
				fmsg := "%v ##%x endpoint-factory %q: %v\n"
				logging.Errorf(fmsg, prefix, opaque, raddr1, e)
				err = e
				continue
			}
			fmsg := "%v ##%x endpoint %q restarted\n"
			logging.Infof(fmsg, prefix, opaque, raddr)

		} else {
			fmsg := "%v ##%x endpoint %q active ...\n"
			logging.Infof(fmsg, prefix, opaque, raddr)
		}
		// FIXME: hack to make both node-name available from
		// endpoints table.
		feed.endpoints[raddr] = endpoint  // :SideEffect:
		feed.endpoints[raddr1] = endpoint // :SideEffect:
	}

	// posted to each kv data-path
	for bucketn, kvdata := range feed.kvdata {
		// though only endpoints have been updated
		kvdata.AddEngines(opaque, feed.engines[bucketn], feed.endpoints)
	}
	return nil
}

// return,
// "ok", feed is active.
// "stale", feed is stale.
func (feed *Feed) staleCheck() string {
	raddrs := []string{}
	for raddr, endpoint := range feed.endpoints {
		if endpoint.Ping() {
			return "ok" // feed active
		}
		raddrs = append(raddrs, raddr)
	}
	return "stale"
}

func (feed *Feed) getStatistics() c.Statistics {
	stats, _ := c.NewStatistics(nil)
	stats.Set("topic", feed.topic)
	stats.Set("engines", feed.engineNames())
	for bucketn, kvdata := range feed.kvdata {
		stats.Set("bucket-"+bucketn, kvdata.GetStatistics())
	}
	endStats, _ := c.NewStatistics(nil)
	for raddr, endpoint := range feed.endpoints {
		endStats.Set(raddr, endpoint.GetStatistics())
	}
	stats.Set("endpoints", endStats)
	return stats
}

func (feed *Feed) resetConfig(config c.Config) {
	if cv, ok := config["feedWaitStreamReqTimeout"]; ok {
		feed.reqTimeout = time.Duration(cv.Int())
	}
	if cv, ok := config["feedWaitStreamEndTimeout"]; ok {
		feed.endTimeout = time.Duration(cv.Int())
	}
	// pass the configuration to active kvdata
	for _, kvdata := range feed.kvdata {
		kvdata.ResetConfig(config)
	}
	// pass the configuration to active endpoints
	econf := config.SectionConfig("dataport.", true /*trim*/)
	for _, endpoint := range feed.endpoints {
		endpoint.ResetConfig(econf)
	}
	feed.config = feed.config.Override(config)
}

func (feed *Feed) shutdown(opaque uint16) error {
	defer func() {
		if r := recover(); r != nil {
			fmsg := "%v ##%x shutdown() crashed: %v\n"
			logging.Errorf(fmsg, feed.logPrefix, opaque, r)
			logging.Errorf("%s", logging.StackTrace())
		}
	}()

	// close upstream
	for _, feeder := range feed.feeders {
		feeder.CloseFeed()
	}
	// close data-path
	for bucketn, kvdata := range feed.kvdata {
		kvdata.Close()
		delete(feed.kvdata, bucketn) // :SideEffect:
	}
	// close downstream
	for _, endpoint := range feed.endpoints {
		endpoint.Close()
	}
	// cleanup
	close(feed.finch)
	logging.Infof("%v ##%x feed ... stopped\n", feed.logPrefix, opaque)
	return nil
}

// shutdown upstream, data-path and remove data-structure for this bucket.
func (feed *Feed) cleanupBucket(bucketn string, enginesOk bool) {
	if enginesOk {
		delete(feed.engines, bucketn) // :SideEffect:
	}
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
	if kvdata, ok := feed.kvdata[bucketn]; ok {
		kvdata.Close()
	}
	delete(feed.kvdata, bucketn) // :SideEffect:
}

func (feed *Feed) openFeeder(
	opaque uint16, pooln, bucketn string) (BucketFeeder, error) {

	feeder, ok := feed.feeders[bucketn]
	if ok {
		return feeder, nil
	}
	bucket, err := feed.connectBucket(feed.cluster, pooln, bucketn)
	if err != nil {
		return nil, projC.ErrorFeeder
	}

	uuid, err := c.NewUUID()
	if err != nil {
		fmsg := "%v ##%x c.NewUUID(): %v"
		logging.Errorf(fmsg, feed.logPrefix, opaque, err)
		return nil, err
	}
	name := newDCPConnectionName(bucket.Name, feed.topic, uuid.Uint64())
	dcpConfig := map[string]interface{}{
		"genChanSize":  feed.config["dcp.genChanSize"].Int(),
		"dataChanSize": feed.config["dcp.dataChanSize"].Int(),
	}
	feeder, err = OpenBucketFeed(name, bucket, dcpConfig)
	if err != nil {
		fmsg := "%v ##%x OpenBucketFeed(%q): %v"
		logging.Errorf(fmsg, feed.logPrefix, opaque, bucketn, err)
		return nil, projC.ErrorFeeder
	}
	return feeder, nil
}

// start a feed for a bucket with a set of kvfeeder,
// based on vbmap and failover-logs.
func (feed *Feed) bucketFeed(
	opaque uint16, stop, start bool,
	reqTs *protobuf.TsVbuuid, feeder BucketFeeder) error {

	pooln, bucketn := reqTs.GetPool(), reqTs.GetBucket()

	vbnos := c.Vbno32to16(reqTs.GetVbnos())
	_ /*vbuuids*/, err := feed.bucketDetails(pooln, bucketn, opaque, vbnos)
	if err != nil {
		return projC.ErrorFeeder
	}

	// stop and start are mutually exclusive
	if stop {
		fmsg := "%v ##%x stop-timestamp %v\n"
		logging.Infof(fmsg, feed.logPrefix, opaque, reqTs.Repr())
		if err = feeder.EndVbStreams(opaque, reqTs); err != nil {
			fmsg := "%v ##%x EndVbStreams(%q): %v"
			logging.Errorf(fmsg, feed.logPrefix, opaque, bucketn, err)
			return projC.ErrorFeeder
		}

	} else if start {
		fmsg := "%v ##%x start-timestamp %v\n"
		logging.Infof(fmsg, feed.logPrefix, opaque, reqTs.Repr())
		if err = feeder.StartVbStreams(opaque, reqTs); err != nil {
			fmsg := "%v ##%x StartVbStreams(%q): %v"
			logging.Errorf(fmsg, feed.logPrefix, opaque, bucketn, err)
			return projC.ErrorFeeder
		}
	}
	return nil
}

// - return dcp-client failures.
func (feed *Feed) bucketDetails(
	pooln, bucketn string, opaque uint16, vbnos []uint16) ([]uint64, error) {

	bucket, err := feed.connectBucket(feed.cluster, pooln, bucketn)
	if err != nil {
		return nil, err
	}
	defer bucket.Close()

	// failover-logs
	dcpConfig := map[string]interface{}{
		"genChanSize":  feed.config["dcp.genChanSize"].Int(),
		"dataChanSize": feed.config["dcp.dataChanSize"].Int(),
	}
	flogs, err := bucket.GetFailoverLogs(vbnos, dcpConfig)
	if err != nil {
		fmsg := "%v ##%x GetFailoverLogs(%q): %v"
		logging.Errorf(fmsg, feed.logPrefix, opaque, bucketn, err)
		return nil, err
	}
	vbuuids := make([]uint64, len(vbnos))
	for i, vbno := range vbnos {
		flog := flogs[vbno]
		if len(flog) < 1 {
			fmsg := "%v ##%x %q.FailoverLog() empty"
			logging.Errorf(fmsg, feed.logPrefix, opaque, bucketn)
			return nil, projC.ErrorInvalidVbucket
		}
		latestVbuuid, _, err := flog.Latest()
		if err != nil {
			fmsg := "%v ##%x %q.FailoverLog invalid log"
			logging.Errorf(fmsg, feed.logPrefix, opaque, bucketn)
			return nil, err
		}
		vbuuids[i] = latestVbuuid
	}

	return vbuuids, nil
}

func (feed *Feed) getLocalVbuckets(
	pooln, bucketn string, opaque uint16) ([]uint16, error) {

	prefix := feed.logPrefix
	// gather vbnos based on colocation policy.
	var cinfo *c.ClusterInfoCache
	url, err := c.ClusterAuthUrl(feed.config["clusterAddr"].String())
	if err == nil {
		cinfo, err = c.NewClusterInfoCache(url, pooln)
	}
	if err != nil {
		fmsg := "%v ##%x ClusterInfoCache(`%v`): %v\n"
		logging.Errorf(fmsg, prefix, opaque, bucketn, err)
		return nil, projC.ErrorClusterInfo
	}
	if err := cinfo.Fetch(); err != nil {
		fmsg := "%v ##%x cinfo.Fetch(`%v`): %v\n"
		logging.Errorf(fmsg, prefix, opaque, bucketn, err)
		return nil, projC.ErrorClusterInfo
	}
	nodeID := cinfo.GetCurrentNode()
	vbnos32, err := cinfo.GetVBuckets(nodeID, bucketn)
	if err != nil {
		fmsg := "%v ##%x cinfo.GetVBuckets(%d, `%v`): %v\n"
		logging.Errorf(fmsg, prefix, opaque, nodeID, bucketn, err)
		return nil, projC.ErrorClusterInfo
	}
	vbnos := c.Vbno32to16(vbnos32)
	fmsg := "%v ##%x vbmap {%v,%v} - %v\n"
	logging.Infof(fmsg, prefix, opaque, pooln, bucketn, vbnos)
	return vbnos, nil
}

// start data-path each kvaddr
func (feed *Feed) startDataPath(
	bucketn string, feeder BucketFeeder,
	opaque uint16,
	ts *protobuf.TsVbuuid) *KVData {

	mutch := feeder.GetChannel()
	kvdata, ok := feed.kvdata[bucketn]
	if ok {
		kvdata.UpdateTs(opaque, ts)

	} else { // pass engines & endpoints to kvdata.
		engs, ends := feed.engines[bucketn], feed.endpoints
		kvdata = NewKVData(
			feed, bucketn, opaque, ts, engs, ends, mutch, feed.config)
	}
	return kvdata
}

// - return ErrorInconsistentFeed for malformed feed request
func (feed *Feed) processSubscribers(opaque uint16, req Subscriber) error {
	evaluators, routers, err := feed.subscribers(opaque, req)
	if err != nil {
		return err
	}

	// start fresh set of all endpoints from routers.
	if err = feed.startEndpoints(opaque, routers); err != nil {
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
		m[uuid] = engine
		feed.engines[bucketn] = m // :SideEffect:
	}
	return nil
}

// feed.endpoints is updated with freshly started endpoint,
// if an endpoint is already present and active it is
// reused.
func (feed *Feed) startEndpoints(
	opaque uint16, routers map[uint64]c.Router) (err error) {

	prefix := feed.logPrefix
	for _, router := range routers {
		for _, raddr := range router.Endpoints() {
			raddr1, endpoint, e := feed.getEndpoint(raddr, opaque)
			if e != nil {
				err = e
				continue

			} else if endpoint == nil || !endpoint.Ping() {
				topic, typ := feed.topic, feed.endpointType
				config := feed.config.SectionConfig("dataport.", true /*trim*/)
				endpoint, e = feed.epFactory(topic, typ, raddr, config)
				if e != nil {
					fmsg := "%v ##%x endpoint-factory %q: %v\n"
					logging.Errorf(fmsg, prefix, opaque, raddr1, e)
					err = e
					continue
				}
				fmsg := "%v ##%x endpoint %q started\n"
				logging.Infof(fmsg, prefix, opaque, raddr)

			} else {
				fmsg := "%v ##%x endpoint %q active ...\n"
				logging.Infof(fmsg, prefix, opaque, raddr)
			}
			// FIXME: hack to make both node-name available from
			// endpoints table.
			feed.endpoints[raddr] = endpoint  // :SideEffect:
			feed.endpoints[raddr1] = endpoint // :SideEffect:
		}
	}
	return nil
}

func (feed *Feed) getEndpoint(
	raddr string, opaque uint16) (string, c.RouterEndpoint, error) {

	prefix := feed.logPrefix
	_, eqRaddr, err := c.EquivalentIP(raddr, feed.endpointRaddrs())
	if err != nil {
		fmsg := "%v ##%x EquivalentIP() for %q: %v"
		logging.Errorf(fmsg, prefix, opaque, raddr, err)
		return raddr, nil, err

	} else if raddr != eqRaddr {
		fmsg := "%v ##%x endpoint %q takenas %q ..."
		logging.Warnf(fmsg, prefix, opaque, raddr, eqRaddr)
		raddr = eqRaddr
	}
	endpoint, ok := feed.endpoints[raddr]
	if ok {
		return raddr, endpoint, nil
	}
	return raddr, nil, nil
}

// - return ErrorInconsistentFeed for malformed feeds.
func (feed *Feed) subscribers(
	opaque uint16,
	req Subscriber) (map[uint64]c.Evaluator, map[uint64]c.Router, error) {

	evaluators, err := req.GetEvaluators()
	if err != nil {
		fmsg := "%v ##%x malformed evaluators: %v\n"
		logging.Fatalf(fmsg, feed.logPrefix, opaque, err)
		return nil, nil, projC.ErrorInconsistentFeed
	}
	routers, err := req.GetRouters()
	if err != nil {
		fmsg := "%v ##%x malformed routers: %v\n"
		logging.Fatalf(fmsg, feed.logPrefix, opaque, err)
		return nil, nil, projC.ErrorInconsistentFeed
	}

	if len(evaluators) != len(routers) {
		fmsg := "%v ##%x malformed evaluators/routers\n"
		logging.Fatalf(fmsg, feed.logPrefix, opaque)
		return nil, nil, projC.ErrorInconsistentFeed
	}
	fmsg := "%v ##%x uuid mismatch: %v\n"
	for uuid := range evaluators {
		if _, ok := routers[uuid]; ok == false {
			logging.Fatalf(fmsg, feed.logPrefix, opaque, uuid)
			return nil, nil, projC.ErrorInconsistentFeed
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

func (feed *Feed) endpointRaddrs() []string {
	raddrs := make([]string, 0, len(feed.endpoints))
	for raddr := range feed.endpoints {
		raddrs = append(raddrs, raddr)
	}
	return raddrs
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
	err1 := feed.waitOnFeedback(timeout, opaque, func(msg interface{}) string {
		if val, ok := msg.(*controlStreamRequest); ok && val.bucket == bucketn && val.opaque == opaque &&
			ts.Contains(val.vbno) {

			if val.status == mcd.SUCCESS {
				actTs.Append(val.vbno, val.seqno, val.vbuuid, 0, 0)
			} else if val.status == mcd.ROLLBACK {
				rollTs.Append(val.vbno, val.seqno, val.vbuuid, 0, 0)
			} else if val.status == mcd.NOT_MY_VBUCKET {
				failTs.Append(val.vbno, val.seqno, val.vbuuid, 0, 0)
				err = projC.ErrorNotMyVbucket
			} else {
				failTs.Append(val.vbno, val.seqno, val.vbuuid, 0, 0)
				err = projC.ErrorStreamRequest
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
	err1 := feed.waitOnFeedback(timeout, opaque, func(msg interface{}) string {
		if val, ok := msg.(*controlStreamEnd); ok && val.bucket == bucketn && val.opaque == opaque &&
			ts.Contains(val.vbno) {

			if val.status == mcd.SUCCESS {
				endTs.Append(val.vbno, 0 /*seqno*/, 0 /*vbuuid*/, 0, 0)
			} else if val.status == mcd.NOT_MY_VBUCKET {
				failTs.Append(val.vbno, 0 /*seqno*/, 0 /*vbuuid*/, 0, 0)
				err = projC.ErrorNotMyVbucket
			} else {
				failTs.Append(val.vbno, 0 /*seqno*/, 0 /*vbuuid*/, 0, 0)
				err = projC.ErrorStreamEnd
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
	timeout <-chan time.Time,
	opaque uint16, callb func(msg interface{}) string) (err error) {

	msgs := make([][]interface{}, 0)
loop:
	for {
		select {
		case msg := <-feed.backch:
			switch callb(msg[0]) {
			case "skip":
				msgs = append(msgs, msg)
			case "done":
				break loop
			case "ok":
			}

		case <-timeout:
			logging.Errorf("%v ##%x dcp-timeout\n", feed.logPrefix, opaque)
			err = projC.ErrorResponseTimeout
			break loop
		}
	}
	// re-populate in the same order.
	if len(msgs) > 0 {
		fmsg := "%v ##%x re-populating back-channel with %d messages"
		logging.Infof(fmsg, feed.logPrefix, len(msgs))
	}
	for _, msg := range msgs {
		feed.backch <- msg
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
		if ts != nil {
			xs = append(xs, ts)
		}
	}
	ys := make([]*protobuf.TsVbuuid, 0, len(feed.rollTss))
	for _, ts := range feed.rollTss {
		if ts != nil && !ts.IsEmpty() {
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

// generate a unique opaque identifier.
func newDCPConnectionName(bucketn, topic string, uuid uint64) string {
	return fmt.Sprintf("proj-%s-%s-%v", bucketn, topic, uuid)
}

//---- local function

// connectBucket will instantiate a couchbase-bucket instance with cluster.
// caller's responsibility to close the bucket.
func (feed *Feed) connectBucket(
	cluster, pooln, bucketn string) (*couchbase.Bucket, error) {

	ah := &c.CbAuthHandler{Hostport: cluster, Bucket: bucketn}
	couch, err := couchbase.ConnectWithAuth("http://"+cluster, ah)
	if err != nil {
		fmsg := "%v connectBucket(`%v`): %v"
		logging.Errorf(fmsg, feed.logPrefix, bucketn, err)
		return nil, projC.ErrorDCPConnection
	}
	pool, err := couch.GetPool(pooln)
	if err != nil {
		fmsg := "%v GetPool(`%v`): %v"
		logging.Errorf(fmsg, feed.logPrefix, pooln, err)
		return nil, projC.ErrorDCPPool
	}
	bucket, err := pool.GetBucket(bucketn)
	if err != nil {
		fmsg := "%v GetBucket(`%v`): %v"
		logging.Errorf(fmsg, feed.logPrefix, bucketn, err)
		return nil, projC.ErrorDCPBucket
	}
	return bucket, nil
}

func FeedConfigParams() []string {
	paramNames := []string{
		"clusterAddr",
		"feedChanSize",
		"feedWaitStreamEndTimeout",
		"feedWaitStreamReqTimeout",
		"mutationChanSize",
		"routerEndpointFactory",
		"vbucketSyncTimeout",
		// dcp configuration
		"dcp.dataChanSize",
		"dcp.genChanSize",
		// dataport
		"dataport.remoteBlock",
		"dataport.keyChanSize",
		"dataport.bufferSize",
		"dataport.bufferTimeout",
		"dataport.harakiriTimeout",
		"dataport.maxPayload"}
	return paramNames
}
