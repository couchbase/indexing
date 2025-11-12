package projector

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	c "github.com/couchbase/indexing/secondary/common"
	couchbase "github.com/couchbase/indexing/secondary/dcp"
	mcd "github.com/couchbase/indexing/secondary/dcp/transport"
	mc "github.com/couchbase/indexing/secondary/dcp/transport/client"
	"github.com/couchbase/indexing/secondary/logging"
	projC "github.com/couchbase/indexing/secondary/projector/client"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
	"github.com/golang/protobuf/proto"
)

var errNilTimestamp = errors.New("Feed's book-keeping timestamp (actTss/rollTss/reqTss) is nil")
var errInconsistentTimestampMapping = errors.New("Not all keyspaceID's are present in all of feed's book-keeping timestamps")

// NOTE1:
// https://github.com/couchbase/indexing/commit/
//         9a2ea0f0ffaf9f103ace6ffe54e253001b28b9a8
// above commit mandates that back-channel should be sufficiently large to
// consume all DCP response for STREAM_REQ posted for 1 or more bucket. Here
// is the way it plays out,
//
//   * once a stream is opened DCP will start batch pushing data for that
//     stream.
//   * while data for each stream is pushed to vbucket-routines the control
//     messages, STREAMREQ & SNAPSHOT, are sent to back-channel.
//   * by the time the last vbucket is StreamRequested and corresponding
//     response and snapshot message is back-channeled, there would have
//     been a lot of data pushed to vbucket-routines.
//   * data will eventually drained to downstream endpoints and its
//     connection, but back-channel cannot be blocked until all STREAMREQ
//     is posted to DCP server. In simple terms,
//     THIS IS IMPORTANT: NEVER REDUCE THE BACK-CHANNEL SIZE TO < MAXVBUCKETS

// Feed is mutation stream - for maintenance, initial-load, catchup etc...
type Feed struct {
	cluster      string               // immutable
	version      protobuf.FeedVersion // immutable
	pooln        string               // immutable
	topic        string               // immutable
	opaque       uint16               // opaque that created this feed.
	endpointType string               // immutable
	projector    *Projector
	async        bool // immutable

	// upstream
	// reqTs, book-keeping on outstanding request posted to feeder.
	// vbucket entry from this timestamp is deleted only when a SUCCESS,
	// ROLLBACK or ERROR response is received from feeder.
	reqTss map[string]*protobuf.TsVbuuid // keyspaceId -> TsVbuuid
	// actTs, once StreamBegin SUCCESS response is got back from DCP,
	// vbucket entry is moved here.
	actTss map[string]*protobuf.TsVbuuid // keyspaceId -> TsVbuuid
	// rollTs, when StreamBegin ROLLBACK response is got back from DCP,
	// vbucket entry is moved here.
	rollTss map[string]*protobuf.TsVbuuid // keyspaceId -> TsVbuuid

	feeders map[string]BucketFeeder // keyspaceId -> BucketFeeder{}
	// downstream
	kvdata    map[string]*KVData            // keyspaceId -> kvdata
	engines   map[string]map[uint64]*Engine // keyspaceId -> uuid -> engine
	endpoints map[string]c.RouterEndpoint
	// genServer channel
	reqch  chan []interface{}
	backch chan []interface{}
	finch  chan bool
	// book-keeping for stale-ness
	stale int

	// Address with which bucket seqnos are retrieved from KV
	kvaddr string

	// Collections
	collectionsAware bool
	osoSnapshot      map[string]bool //keyspaceId -> osoSnapshot

	//client override
	numDcpConnections uint32

	// config params
	reqTimeout time.Duration
	endTimeout time.Duration
	epFactory  c.RouterEndpointFactory
	config     c.Config
	logPrefix  string
}

// NewFeed creates a new topic feed.
// `config` contains following keys.
//
//	clusterAddr: KV cluster address <host:port>.
//	feedWaitStreamReqTimeout: wait for a response to StreamRequest
//	feedWaitStreamEndTimeout: wait for a response to StreamEnd
//	feedChanSize: channel size for feed's control path and back path
//	mutationChanSize: channel size of projector's data path routine
//	syncTimeout: timeout, in ms, for sending periodic Sync messages
//	routerEndpointFactory: endpoint factory
func NewFeed(
	pooln, topic string,
	projector *Projector,
	config c.Config, opaque uint16,
	async bool) (*Feed, error) {

	epf := config["routerEndpointFactory"].Value.(c.RouterEndpointFactory)
	chsize := config["feedChanSize"].Int()
	backchsize := config["backChanSize"].Int()
	feed := &Feed{
		cluster:   config["clusterAddr"].String(),
		pooln:     pooln,
		topic:     topic,
		opaque:    opaque,
		projector: projector,
		async:     async,

		osoSnapshot: make(map[string]bool),

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
		backch: make(chan []interface{}, backchsize),
		finch:  make(chan bool),

		reqTimeout: time.Duration(config["feedWaitStreamReqTimeout"].Int()),
		endTimeout: time.Duration(config["feedWaitStreamEndTimeout"].Int()),
		epFactory:  epf,
		config:     config,
	}
	feed.logPrefix = fmt.Sprintf("FEED[<=>%v(%v)]", topic, feed.cluster)

	go feed.genServer()
	logging.Infof("%v ##%x feed started ...\n", feed.logPrefix, opaque)
	return feed, nil
}

// GetOpaque return the opaque id that created this feed.
func (feed *Feed) GetOpaque() uint16 {
	return feed.opaque
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
	fCmdGetStats
	fCmdResetConfig
	fCmdDeleteEndpoint
	fCmdPing
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
	err = c.OpError(err, resp, 1)
	if err != nil {
		return &protobuf.TopicResponse{Topic: proto.String(feed.topic)}, err
	}
	return resp[0].(*protobuf.TopicResponse), nil
}

// RestartVbuckets will restart upstream vbuckets for specified buckets.
// Synchronous call.
func (feed *Feed) RestartVbuckets(
	req *protobuf.RestartVbucketsRequest, opaque uint16) (*protobuf.TopicResponse, error) {

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdRestartVbuckets, req, opaque, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	err = c.OpError(err, resp, 1)
	if err != nil {
		return &protobuf.TopicResponse{Topic: proto.String(feed.topic)}, err
	}
	return resp[0].(*protobuf.TopicResponse), nil
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
	err = c.OpError(err, resp, 1)
	if err != nil {
		return &protobuf.TopicResponse{Topic: proto.String(feed.topic)}, err
	}
	return resp[0].(*protobuf.TopicResponse), nil
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
	req *protobuf.AddInstancesRequest,
	opaque uint16) (*protobuf.TimestampResponse, error) {

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdAddInstances, req, opaque, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	err = c.OpError(err, resp, 1)
	if err != nil {
		return &protobuf.TimestampResponse{Topic: proto.String(feed.topic)}, err
	}
	return resp[0].(*protobuf.TimestampResponse), nil
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
func (feed *Feed) StaleCheck(staleTimeout int) (string, error) {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdStaleCheck, respch}

	// modified `FailsafeOp()` to implement a nuke-switch
	// for indefinitely blocked feeds.
	tm := time.After(time.Duration(staleTimeout) * time.Millisecond)
	select {
	case feed.reqch <- cmd:
		select {
		case resp := <-respch:
			return resp[0].(string), nil
		case <-feed.finch:
			return "exit", c.ErrorClosed
			// NOTE: don't use the nuke-timeout if feed's gen-server has
			// accepted to service the request. Reason being,
			// a. staleCheck() will try to ping endpoint() for its health, and
			//    if endpoint is stuck with downstream, it might trigger the
			//    the following timeout.
			//case <-tm:
			//    logging.Fatalf("%v StaleCheck() timesout !!", feed.logPrefix)
			//    feed.shutdown(feed.opaque)
			//    return "exit", c.ErrorClosed
		}
	case <-feed.finch:
		return "exit", c.ErrorClosed
	case <-tm:
		logging.Fatalf("%v StaleCheck() timesout !!", feed.logPrefix)
		feed.shutdown(feed.opaque)
		return "exit", c.ErrorClosed
	}
	return "exit", c.ErrorClosed
}

// GetTopicResponse for this feed.
// Synchronous call.
func (feed *Feed) GetTopicResponse() *protobuf.TopicResponse {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdGetTopicResponse, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	if resp != nil && err == nil {
		return resp[0].(*protobuf.TopicResponse)
	}
	return nil
}

// GetStatistics for this feed.
// Synchronous call.
func (feed *Feed) GetStatistics() c.Statistics {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdGetStatistics, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	if resp != nil && err == nil {
		return resp[0].(c.Statistics)
	}
	return nil
}

func (feed *Feed) getFeedStats() *FeedStats {

	feedStats := &FeedStats{}
	feedStats.Init()
	// For this feed, iterate through all buckets
	for bucket, kvdata := range feed.kvdata {
		keyspaceIdStats := &KeyspaceIdStats{}
		keyspaceIdStats.topic = feed.topic
		keyspaceIdStats.keyspaceId = kvdata.keyspaceId
		keyspaceIdStats.opaque = kvdata.opaque

		if feeder := feed.feeders[bucket]; feeder != nil {
			keyspaceIdStats.dcpStats = feeder.GetStats()
			keyspaceIdStats.dcpLogPrefix = strings.Join([]string{
				"DCPT",
				keyspaceIdStats.topic,
				keyspaceIdStats.keyspaceId,
				strconv.FormatUint(feeder.GetStreamUuid(), 10),
				fmt.Sprintf("##%x", keyspaceIdStats.opaque),
			}, ":")
		}

		// For this bucket, get kvstats
		keyspaceIdStats.kvstats = kvdata.GetKVStats()

		// For this bucket, get workerStats
		keyspaceIdStats.wrkrStats = kvdata.GetWorkerStats()

		// For this bucket, get evaluator stats
		engines := feed.engines[bucket]
		keyspaceIdStats.evaluatorStats = make(map[string]interface{})
		for _, engine := range engines {
			indexname := engine.GetIndexName()
			bucketname := engine.Bucket()
			scopename := engine.Scope()
			collectionname := engine.Collection()
			var key string
			if scopename == "" && collectionname == "" {
				key = fmt.Sprintf("%v:%v", bucketname, indexname)
			} else {
				key = fmt.Sprintf("%v:%v:%v:%v", bucketname, scopename, collectionname, indexname)
			}
			keyspaceIdStats.evaluatorStats[key] = engine.GetEvaluatorStats()
		}
		// Update feed stats for this bucket
		feedStats.keyspaceIdStats[bucket] = keyspaceIdStats
	}

	for _, value := range feed.endpoints {
		// For each feed, there exists only one end point. Therefore,
		// this loop is iterated only once
		feedStats.endpStats = value.GetStats()
	}

	return feedStats
}

// Return pointers to the stats objects for this feed.
// Synchronous call.
func (feed *Feed) GetStats() *FeedStats {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdGetStats, respch}
	resp, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	if resp != nil && err == nil {
		return resp[0].(*FeedStats)
	}
	return nil
}

// Shutdown feed, its upstream connection with kv and downstream endpoints.
// Synchronous call.
func (feed *Feed) Shutdown(opaque uint16) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdShutdown, opaque, respch}
	_, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return err
}

// DeleteEndpoint will delete the specified endpoint address
// from feed.
func (feed *Feed) DeleteEndpoint(raddr string) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdDeleteEndpoint, raddr, respch}
	_, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return err
}

// Ping whether the feed is active or not.
func (feed *Feed) Ping() error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{fCmdPing, respch}
	_, err := c.FailsafeOp(feed.reqch, respch, cmd, feed.finch)
	return err
}

type controlStreamRequest struct {
	keyspaceId string
	opaque     uint16
	status     mcd.Status
	vbno       uint16
	vbuuid     uint64
	seqno      uint64 // also doubles as rollback-seqno
	uuid       uint64 // UUID of the kvdata that initiated this mesasge transfer
}

func (v *controlStreamRequest) Repr() string {
	return fmt.Sprintf("{controlStreamRequest, %v, %s, %d, %x, %d, ##%x, %v}",
		v.status, v.keyspaceId, v.vbno, v.vbuuid, v.seqno, v.opaque, v.uuid)
}

// PostStreamRequest feedback from data-path.
// Asynchronous call.
func (feed *Feed) PostStreamRequest(keyspaceId string, m *mc.DcpEvent, kvdataUUID uint64) {
	var respch chan []interface{}
	cmd := &controlStreamRequest{
		keyspaceId: keyspaceId,
		opaque:     m.Opaque,
		status:     m.Status,
		vbno:       m.VBucket,
		vbuuid:     m.VBuuid,
		seqno:      m.Seqno, // can also be roll-back seqno, based on status
		uuid:       kvdataUUID,
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
	keyspaceId string
	opaque     uint16
	status     mcd.Status
	vbno       uint16
	uuid       uint64 // UUID of the kvdata that initiated this mesasge transfer
}

func (v *controlStreamEnd) Repr() string {
	return fmt.Sprintf("{controlStreamEnd, %v, %s, %d, ##%x, %v}",
		v.status, v.keyspaceId, v.vbno, v.opaque, v.uuid)
}

// PostStreamEnd feedback from data-path.
// Asynchronous call.
func (feed *Feed) PostStreamEnd(keyspaceId string, m *mc.DcpEvent, kvdataUUID uint64) {
	var respch chan []interface{}
	cmd := &controlStreamEnd{
		keyspaceId: keyspaceId,
		opaque:     m.Opaque,
		status:     m.Status,
		vbno:       m.VBucket,
		uuid:       kvdataUUID,
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
	keyspaceId string
	uuid       uint64 // UUID of the kvdata that initiated this mesasge transfer
}

func (v *controlFinKVData) Repr() string {
	return fmt.Sprintf("{controlFinKVData, %s, %v}", v.keyspaceId, v.uuid)
}

// PostFinKVdata feedback from data-path.
// Asynchronous call.
func (feed *Feed) PostFinKVdata(keyspaceId string, kvdataUUID uint64) {
	var respch chan []interface{}
	cmd := &controlFinKVData{keyspaceId: keyspaceId, uuid: kvdataUUID}
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
			feed.shutdown(feed.opaque)
		}
		feed.projector.DelFeed(feed.topic)
	}()

	var msg []interface{}

	timeout := time.NewTicker(1000 * time.Millisecond)
	ctrlMsg := "%v ##%x control channel has %v messages"
	prefix := feed.logPrefix
	defer func() {
		timeout.Stop()
	}()

	handleBackChMsgs := func(msg []interface{}) {
		if cmd, ok := msg[0].(*controlStreamRequest); ok {
			// This check is required to avoid race in the following scenario:
			//
			// 1. Indexer sends fCmdStart, feed opens connections with DCP upstream
			//    and starts sending DCP_STREAMREQ messages for each of the vb's
			// 2. Due to a slow memcached, the streamreq message for one vb timesout
			// 3. DCPP sends stream begin messages for successful DCP_STREAMREQ
			//    messages and kvdata puts them in feed.backch
			// 4. Due to the timeout error while starting dcp streams, feed.cleanupKeyspace()
			//    is invoked
			// 5. This would clean-up the KVData instance and indexer sends MTR again
			// 6. In this MTR, the stream begin messages from earlier MTR would not be
			//    processed (in feed.waitStreamRequests()) as the opaque values differ
			//    and these stream begin messages will be read here
			//
			// As uuid's would be different for different instances of kvdata, we compare the
			// uuid's before actually trying to process the STREAM_BEGIN messages. If there is
			// a mismatch, we ignore the message

			if kvdata, ok := feed.kvdata[cmd.keyspaceId]; ok {
				if cmd.uuid != kvdata.uuid {
					logging.Infof("%v The kvdata instance: %v for keyspace: '%v' is already cleaned up."+
						" Current kvdata instance is: %v. Ignoring controlStreamRequest for vb:%v",
						feed.logPrefix, cmd.uuid, cmd.keyspaceId, kvdata.uuid, cmd.vbno)
					return
				}
			} else { // KVData instance does not exist. Ignore the message
				logging.Infof("%v The kvdata instance for keyspace: '%v' does not exist."+
					" Ignoring controlStreamRequest for vb:%v", feed.logPrefix, cmd.keyspaceId, cmd.vbno)
				return
			}

			var reqTs *protobuf.TsVbuuid
			if reqTs, ok = feed.reqTss[cmd.keyspaceId]; !ok {
				fmsg := "%v ##%x ignoring backch message %T: %v\n"
				logging.Warnf(fmsg, prefix, cmd.opaque, cmd, cmd.Repr())
				return
			}

			seqno, _, sStart, sEnd, _, err := reqTs.Get(cmd.vbno)
			if err != nil {
				fmsg := "%v ##%x backch flush %v: %v\n"
				logging.Errorf(fmsg, prefix, cmd.opaque, cmd, err)
			}
			if ok && reqTs != nil {
				reqTs = reqTs.FilterByVbuckets([]uint16{cmd.vbno})
				feed.reqTss[cmd.keyspaceId] = reqTs
			}
			if cmd.status == mcd.ROLLBACK {
				fmsg := "%v ##%x backch flush rollback %T: %v\n"
				logging.Infof(fmsg, prefix, cmd.opaque, cmd, cmd.Repr())
				rollTs, ok := feed.rollTss[cmd.keyspaceId]
				if ok {
					rollTs = rollTs.Append(
						cmd.vbno, cmd.seqno, cmd.vbuuid, sStart, sEnd, "")
					feed.rollTss[cmd.keyspaceId] = rollTs
				}

			} else if cmd.status == mcd.SUCCESS {
				fmsg := "%v ##%x backch flush success %T: %v\n"
				logging.Infof(fmsg, prefix, cmd.opaque, cmd, cmd.Repr())
				actTs, ok := feed.actTss[cmd.keyspaceId]
				if ok {
					actTs = actTs.Append(
						cmd.vbno, seqno, cmd.vbuuid, sStart, sEnd, "")
					feed.actTss[cmd.keyspaceId] = actTs
				}

			} else {
				fmsg := "%v ##%x backch flush error %T: %v\n"
				logging.Errorf(fmsg, prefix, cmd.opaque, cmd, cmd.Repr())
			}

		} else if cmd, ok := msg[0].(*controlStreamEnd); ok {
			// This check is required to avoid race in the following scenario:
			// 1. When feed.cleanupKeyspace() is triggerred, it invokes feeder.closeFeed()
			// 2. feeder.CloseFeed() would publish STREAMEND messages
			// 3. If kvdata reads the STREAMEND messages before closing, it would publish
			//    the STREAMEND messages to feed.backch
			// 4. After feed.cleanupKeyspace() exits, feed processes messages on reqch and backch
			// 5. If an fCmdStart from indexer arrives on feed's reqch and it is processed before
			//    messages on backch, then at the time STREAMEND messages from backch are processed
			//    the book-keeping for vbuckets goes out of sync with KV engine
			//
			// As uuid's would be different for different instances of kvdata, we compare the
			// uuid's before actually trying to process the STREAM_END messages. If there is
			// a mismatch, we ignore the message
			if kvdata, ok := feed.kvdata[cmd.keyspaceId]; ok {
				if cmd.uuid != kvdata.uuid {
					logging.Warnf("%v The kvdata instance: %v for keyspace: '%v' is already cleaned up."+
						" Current kvdata instance is: %v. Ignoring controlStreamEnd for vb:%v",
						feed.logPrefix, cmd.uuid, cmd.keyspaceId, kvdata.uuid, cmd.vbno)
					return
				}
			} else { // KVData instance does not exist. Ignore the message
				logging.Infof("%v The kvdata instance for keyspace: '%v' does not exist."+
					" Ignoring controlStreamEnd for vb:%v", feed.logPrefix, cmd.keyspaceId, cmd.vbno)
				return
			}

			fmsg := "%v ##%x backch flush %T: %v\n"
			logging.Infof(fmsg, prefix, cmd.opaque, cmd, cmd.Repr())
			reqTs, ok := feed.reqTss[cmd.keyspaceId]
			if ok {
				reqTs = reqTs.FilterByVbuckets([]uint16{cmd.vbno})
				feed.reqTss[cmd.keyspaceId] = reqTs
			}
			actTs, ok := feed.actTss[cmd.keyspaceId]
			if ok {
				actTs = actTs.FilterByVbuckets([]uint16{cmd.vbno})
				feed.actTss[cmd.keyspaceId] = actTs
			}
			rollTs, ok := feed.rollTss[cmd.keyspaceId]
			if ok {
				rollTs = rollTs.FilterByVbuckets([]uint16{cmd.vbno})
				feed.rollTss[cmd.keyspaceId] = rollTs
			}

		} else if cmd, ok := msg[0].(*controlFinKVData); ok {
			// This check is required to avoid race in the following scenario:
			// 1. When feed.cleanupKeyspace() is triggerred, it invokes kvdata.Close()
			// 2. kvdata.Close() will stop the genServer() and posts response back to feed.
			//    The defer() block execution in kvdata's genServer() is yet to happen
			// 3. feed.cleanupKeyspace() will go-ahead delete the kvdata entry for the bucket
			// 4. feed now waits on reqch and backch for messages
			// 5. fCmdStart from indexer arrives on feed and feed initializes kvdata for the bucket
			// 6. The defer() block from kvdata that got closed starts it's execution and it posts
			//    controlFinKVData message
			// 7. This message would cleanup the bucket again which got initialized with fCmdStart
			//
			// As uuid's would be different for different instances of kvdata, we compare the
			// uuid's before actually trying to process the controlFinKVData message. If there is
			// a mismatch, we ignore the message
			if kvdata, ok := feed.kvdata[cmd.keyspaceId]; ok {
				if cmd.uuid != kvdata.uuid {
					logging.Warnf("%v The kvdata instance: %v for keyspace: '%v' is already cleaned up."+
						" Current kvdata instance is: %v. Ignoring controlFinKVData",
						feed.logPrefix, cmd.uuid, cmd.keyspaceId, kvdata.uuid)
					return
				}
			} else { // KVData instance does not exist. Ignore the message
				logging.Infof("%v The kvdata instance for keyspace: %v does not exist."+
					" Ignoring controlFinKVData message", feed.logPrefix, cmd.keyspaceId)
				return
			}

			fmsg := "%v ##%x backch flush %T -- %v\n"
			logging.Infof(fmsg, prefix, feed.opaque, cmd, cmd.Repr())
			_, ok := feed.actTss[cmd.keyspaceId]
			if ok == false {
				// Note: bucket could have gone because of a downstream
				// delBucket() request.
				fmsg := "%v ##%x FinKVData can't find keyspace %q\n"
				logging.Warnf(fmsg, prefix, feed.opaque, cmd.keyspaceId)
			}
			fmsg = "%v ##%x self deleting keyspace\n"
			logging.Infof(fmsg, prefix, feed.opaque)
			feed.cleanupKeyspace(cmd.keyspaceId, false)

		} else {
			fmsg := "%v ##%x backch flush %T: %v\n"
			logging.Fatalf(fmsg, prefix, feed.opaque, msg[0], msg[0])
		}
	}

loop:
	for {
		// Flush backch messages before processing request
		// While processng a backCh message, we compare the UUID of the current
		// KVData instance with that of the UUID in the message. If there is a
		// mismatch, we ignore the message in backCh. This makes it safer to
		// flush the backCh messages before processing a request
		for len(feed.backch) > 0 {
			select {
			case msg = <-feed.backch:
				handleBackChMsgs(msg)
			default:
			}
		}

		select {
		case msg = <-feed.reqch:
			switch feed.handleCommand(msg) {
			case "ok":
				feed.stale = 0
			case "exit":
				break loop
			}

		case msg = <-feed.backch:
			handleBackChMsgs(msg)
		case <-timeout.C:
			if len(feed.backch) > 0 { // can happend during rebalance.
				logging.Warnf(ctrlMsg, prefix, feed.opaque, len(feed.backch))
			}
		}
	}
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
		var response *protobuf.TopicResponse
		if err != nil { // feed error takes priority in response
			response, _ = feed.topicResponse()
		} else {
			response, err = feed.topicResponse()
		}
		respch <- []interface{}{response, err}

	case fCmdRestartVbuckets:
		req := msg[1].(*protobuf.RestartVbucketsRequest)
		opaque, respch := msg[2].(uint16), msg[3].(chan []interface{})
		err := feed.restartVbuckets(req, opaque)
		var response *protobuf.TopicResponse
		if err != nil { // feed error takes priority in response
			response, _ = feed.topicResponse()
		} else {
			response, err = feed.topicResponse()
		}
		respch <- []interface{}{response, err}

	case fCmdShutdownVbuckets:
		req := msg[1].(*protobuf.ShutdownVbucketsRequest)
		opaque, respch := msg[2].(uint16), msg[3].(chan []interface{})
		respch <- []interface{}{feed.shutdownVbuckets(req, opaque)}

	case fCmdAddBuckets:
		req := msg[1].(*protobuf.AddBucketsRequest)
		opaque, respch := msg[2].(uint16), msg[3].(chan []interface{})
		err := feed.addBuckets(req, opaque)
		var response *protobuf.TopicResponse
		if err != nil { // feed error takes priority in response
			response, _ = feed.topicResponse()
		} else {
			response, err = feed.topicResponse()
		}
		respch <- []interface{}{response, err}

	case fCmdDelBuckets:
		req := msg[1].(*protobuf.DelBucketsRequest)
		opaque, respch := msg[2].(uint16), msg[3].(chan []interface{})
		err := feed.delBuckets(req, opaque)
		if len(feed.kvdata) == 0 {
			status = "exit"
			fmsg := "%v no more keyspaces left, closing the feed ..."
			logging.Warnf(fmsg, feed.logPrefix)
			feed.shutdown(feed.opaque)
		}
		respch <- []interface{}{err}

	case fCmdAddInstances:
		req := msg[1].(*protobuf.AddInstancesRequest)
		opaque, respch := msg[2].(uint16), msg[3].(chan []interface{})
		resp, err := feed.addInstances(req, opaque)
		respch <- []interface{}{resp, err}

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
		status = feed.staleCheck()
		if status == "stale" && feed.stale == 1 { // already gone stale.
			status = "exit"
			feed.shutdown(feed.opaque)
			logging.Warnf("%v feed collect stale...", feed.logPrefix)
		} else if status == "stale" {
			logging.Warnf("%v feed mark stale...", feed.logPrefix)
			feed.stale++
		}
		respch <- []interface{}{status}

	case fCmdGetTopicResponse:
		respch := msg[1].(chan []interface{})
		response, _ := feed.topicResponse()
		respch <- []interface{}{response}

	case fCmdGetStatistics:
		respch := msg[1].(chan []interface{})
		respch <- []interface{}{feed.getStatistics()}

	case fCmdGetStats:
		feedStats := feed.getFeedStats()
		respch := msg[1].(chan []interface{})
		respch <- []interface{}{feedStats}

	case fCmdResetConfig:
		config, respch := msg[1].(c.Config), msg[2].(chan []interface{})
		feed.resetConfig(config)
		respch <- []interface{}{nil}

	case fCmdShutdown:
		opaque := msg[1].(uint16)
		respch := msg[2].(chan []interface{})
		respch <- []interface{}{feed.shutdown(opaque)}
		status = "exit"

	case fCmdDeleteEndpoint:
		raddr := msg[1].(string)
		respch := msg[2].(chan []interface{})
		endpoint, ok := feed.endpoints[raddr]
		if ok && !endpoint.Ping() { // delete endpoint only if not alive.
			delete(feed.endpoints, raddr)
			logging.Infof("%v endpoint %v deleted\n", feed.logPrefix, raddr)
		}
		// If there are no more endpoints, shutdown the feed.
		// Note that this feed might still be referred by the applications
		// book-keeping entries. It is upto the application to detect
		// that this feed has closed and clean up itself.
		if len(feed.endpoints) == 0 {
			fmsg := "%v no endpoint left automatic shutdown\n"
			logging.Infof(fmsg, feed.logPrefix)
			respch <- []interface{}{feed.shutdown(feed.opaque /*opaque*/)}
			status = "exit"
		}

	case fCmdPing:
		respch := msg[1].(chan []interface{})
		respch <- []interface{}{true}
	}
	return status
}

// start a new feed.
//   - return ErrorInconsistentFeed for malformed feed request
//   - return ErrorInvalidVbucketBranch for malformed vbuuid.
//   - return ErrorFeeder if upstream connection has failures.
//   - return ErrorNotMyVbucket due to rebalances and failures.
//   - return ErrorStreamRequest if StreamRequest failed for some reason
//   - return ErrorResponseTimeout if feedback is not completed within timeout
//   - return ErrorInvalidKeyspaceIdMap if 1:1 mapping between bucket to keyspaceId
//     is invalid
func (feed *Feed) start(
	req *protobuf.MutationTopicRequest, opaque uint16) (err error) {

	feed.endpointType = req.GetEndpointType()
	opaque2 := req.GetOpaque2()
	vbucketWorkers := req.GetNumVbWorkers()

	keyspaceIdMap, err := req.GetKeyspaceIdMap()
	if err != nil {
		return err
	}

	feed.collectionsAware = req.GetCollectionAware()

	needsAuth := req.GetNeedsAuth()

	//client override
	feed.numDcpConnections = req.GetNumDcpConns()

	// update engines and endpoints
	if _, err = feed.processSubscribers(opaque, req, keyspaceIdMap, needsAuth); err != nil { // :SideEffect:
		return err
	}
	for _, ts := range req.GetReqTimestamps() {
		pooln, bucketn := ts.GetPool(), ts.GetBucket()
		keyspaceId := keyspaceIdMap[bucketn]

		feed.osoSnapshot[keyspaceId] = req.GetOsoSnapshot()

		vbnos, e := feed.getLocalVbuckets(pooln, bucketn, opaque)
		if e != nil {
			err = e
			feed.cleanupKeyspace(keyspaceId, false)
			continue
		}
		ts := ts.SelectByVbuckets(vbnos) // take only local vbuckets

		actTs, ok := feed.actTss[keyspaceId]
		if ok { // don't re-request for already active vbuckets
			ts = ts.FilterByVbuckets(c.Vbno32to16(actTs.GetVbnos()))
		}
		rollTs, ok := feed.rollTss[keyspaceId]
		if ok { // forget previous rollback for the current set of vbuckets
			rollTs = rollTs.FilterByVbuckets(c.Vbno32to16(ts.GetVbnos()))
		}
		reqTs, ok := feed.reqTss[keyspaceId]
		// book-keeping of out-standing request, vbuckets that have
		// out-standing request will be ignored.
		if ok {
			ts = ts.FilterByVbuckets(c.Vbno32to16(reqTs.GetVbnos()))
		}

		reqTs = ts.Union(reqTs)
		// open or acquire the upstream feeder object.
		feeder, e := feed.openFeeder(opaque, pooln, bucketn, keyspaceId)
		if e != nil {
			err = e
			feed.cleanupKeyspace(keyspaceId, false)
			continue
		}
		feed.feeders[keyspaceId] = feeder // :SideEffect:

		// open data-path, if not already open.
		cid := getCollectionIdFromReqTs(ts)
		kvdata, e := feed.startDataPath(bucketn, keyspaceId, cid, feeder,
			opaque, ts, opaque2, int(vbucketWorkers))
		if e != nil {
			err = e
			feed.cleanupKeyspace(keyspaceId, false)
			continue
		}

		engines, _ := feed.engines[keyspaceId]
		kvdata.AddEngines(opaque, engines, feed.endpoints)
		feed.kvdata[keyspaceId] = kvdata // :SideEffect:
		// start upstream, after filtering out vbuckets.

		// Ensure feedStats are logged if bukcetFeed takes time
		topic := req.GetTopic()
		feedStats := feed.getFeedStats()
		feed.projector.UpdateStats(topic, nil, feedStats)

		e, _ = feed.bucketFeed(opaque, false, true, ts, feeder)
		if e != nil { // all feed errors are fatal, skip this bucket.
			err = e
			feed.cleanupKeyspace(keyspaceId, false)
			continue
		}
		if !feed.async {
			// wait for stream to start ...
			r, f, a, e := feed.waitStreamRequests(opaque, pooln, keyspaceId, ts)
			feed.rollTss[keyspaceId] = rollTs.Union(r) // :SideEffect:
			feed.actTss[keyspaceId] = actTs.Union(a)   // :SideEffect:
			// forget vbuckets for which a response is already received.
			reqTs = reqTs.FilterByVbuckets(c.Vbno32to16(r.GetVbnos()))
			reqTs = reqTs.FilterByVbuckets(c.Vbno32to16(a.GetVbnos()))
			reqTs = reqTs.FilterByVbuckets(c.Vbno32to16(f.GetVbnos()))
			feed.reqTss[keyspaceId] = reqTs // :SideEffect:
			if e != nil {
				err = e
				logging.Errorf(
					"%v ##%x stream-request (err: %v) rollback: %v; vbnos: %v\n",
					feed.logPrefix, opaque, err,
					feed.rollTss[keyspaceId].GetVbnos(),
					feed.actTss[keyspaceId].GetVbnos())
			} else {
				logging.Infof(
					"%v ##%x stream-request (success) rollback: %v; vbnos: %v\n",
					feed.logPrefix, opaque,
					feed.rollTss[keyspaceId].GetVbnos(),
					feed.actTss[keyspaceId].GetVbnos())
			}
		} else {
			feed.reqTss[keyspaceId] = reqTs // :SideEffect:
			if _, ok := feed.rollTss[keyspaceId]; !ok {
				feed.rollTss[keyspaceId] = protobuf.NewTsVbuuid(ts.GetPool(), ts.GetBucket(), len(vbnos)) // :SideEffect:
			}
			if _, ok := feed.actTss[keyspaceId]; !ok {
				feed.actTss[keyspaceId] = protobuf.NewTsVbuuid(ts.GetPool(), ts.GetBucket(), len(vbnos)) // :SideEffect:
			}
		}
	}
	return err
}

// a subset of upstreams are restarted.
//   - return ErrorInvalidBucket if bucket is not added.
//   - return ErrorInvalidVbucketBranch for malformed vbuuid.
//   - return ErrorFeeder if upstream connection has failures.
//   - return ErrorNotMyVbucket due to rebalances and failures.
//   - return ErrorStreamRequest if StreamRequest failed for some reason
//   - return ErrorResponseTimeout if feedback is not completed within timeout
//   - return ErrorInvalidKeyspaceIdMap if 1:1 mapping between bucket to keyspaceId
//     is invalid
func (feed *Feed) restartVbuckets(
	req *protobuf.RestartVbucketsRequest, opaque uint16) (err error) {

	keyspaceIdMap, err := req.GetKeyspaceIdMap()
	if err != nil {
		return err
	}

	needsAuth := req.GetNeedsAuth()

	// FIXME: restart-vbuckets implies a repair Endpoint.
	raddrs := feed.endpointRaddrs()
	rpReq := protobuf.NewRepairEndpointsRequest(feed.topic, raddrs, needsAuth)
	if err := feed.repairEndpoints(rpReq, opaque); err != nil {
		return err
	}

	opaque2 := req.GetOpaque2()

	for _, ts := range req.GetRestartTimestamps() {
		pooln, bucketn := ts.GetPool(), ts.GetBucket()
		keyspaceId := keyspaceIdMap[bucketn]

		vbnos, e := feed.getLocalVbuckets(pooln, bucketn, opaque)
		if e != nil {
			err = e
			feed.cleanupKeyspace(keyspaceId, false)
			continue
		}
		ts := ts.SelectByVbuckets(vbnos)

		actTs, ok1 := feed.actTss[keyspaceId]
		rollTs, ok2 := feed.rollTss[keyspaceId]
		reqTs, ok3 := feed.reqTss[keyspaceId]
		engines, ok4 := feed.engines[keyspaceId]

		if !ok1 || !ok2 || !ok3 || !ok4 || len(engines) == 0 {
			fmsg := "%v ##%x restartVbuckets() invalid-keyspace %v\n"
			logging.Errorf(fmsg, feed.logPrefix, opaque, keyspaceId)
			err = projC.ErrorInvalidBucket
			continue
		}
		if ok1 { // don't re-request for already active vbuckets
			ts = ts.FilterByVbuckets(c.Vbno32to16(actTs.GetVbnos()))
		}
		if ok2 { // forget previous rollback for the current set of vbuckets
			rollTs = rollTs.FilterByVbuckets(c.Vbno32to16(ts.GetVbnos()))
		}
		// book-keeping of out-standing request, vbuckets that have
		// out-standing request will be ignored.
		if ok3 {
			ts = ts.FilterByVbuckets(c.Vbno32to16(reqTs.GetVbnos()))
		}
		reqTs = ts.Union(reqTs)

		// open or acquire the upstream feeder object.
		feeder, e := feed.openFeeder(opaque, pooln, bucketn, keyspaceId)
		if e != nil {
			err = e
			feed.cleanupKeyspace(keyspaceId, false)
			continue
		}
		feed.feeders[keyspaceId] = feeder // :SideEffect:
		cid := getCollectionIdFromReqTs(ts)
		// open data-path, if not already open.
		kvdata, e := feed.startDataPath(bucketn, keyspaceId, cid, feeder,
			opaque, ts, opaque2, 0 /*use default*/)
		if e != nil { // all feed errors are fatal, skip this bucket.
			err = e
			feed.cleanupKeyspace(keyspaceId, false)
			continue
		}

		feed.kvdata[keyspaceId] = kvdata // :SideEffect:

		// Ensure feedStats are logged if bukcetFeed takes time
		topic := req.GetTopic()
		feedStats := feed.getFeedStats()
		feed.projector.UpdateStats(topic, nil, feedStats)

		// (re)start the upstream, after filtering out remote vbuckets.
		e, _ = feed.bucketFeed(opaque, false, true, ts, feeder)
		if e != nil { // all feed errors are fatal, skip this bucket.
			err = e
			feed.cleanupKeyspace(keyspaceId, false)
			continue
		}
		if !feed.async {
			// wait stream to start ...
			r, f, a, e := feed.waitStreamRequests(opaque, pooln, keyspaceId, ts)
			feed.rollTss[keyspaceId] = rollTs.Union(r) // :SideEffect:
			feed.actTss[keyspaceId] = actTs.Union(a)   // :SideEffect:
			// forget vbuckets for which a response is already received.
			reqTs = reqTs.FilterByVbuckets(c.Vbno32to16(r.GetVbnos()))
			reqTs = reqTs.FilterByVbuckets(c.Vbno32to16(a.GetVbnos()))
			reqTs = reqTs.FilterByVbuckets(c.Vbno32to16(f.GetVbnos()))
			feed.reqTss[keyspaceId] = reqTs // :SideEffect:
			if e != nil {
				err = e
				logging.Errorf(
					"%v ##%x stream-request (err: %v) rollback: %v; vbnos: %v\n",
					feed.logPrefix, opaque, err,
					feed.rollTss[keyspaceId].GetVbnos(),
					feed.actTss[keyspaceId].GetVbnos())
			} else {
				logging.Infof(
					"%v ##%x stream-request (success) rollback: %v; vbnos: %v\n",
					feed.logPrefix, opaque,
					feed.rollTss[keyspaceId].GetVbnos(),
					feed.actTss[keyspaceId].GetVbnos())
			}
		} else {
			feed.reqTss[keyspaceId] = feed.reqTss[keyspaceId].Union(reqTs) // :SideEffect:
		}
	}
	return err
}

// a subset of upstreams are closed.
//   - return ErrorInvalidBucket if bucket is not added.
//   - return ErrorInvalidVbucketBranch for malformed vbuuid.
//   - return ErrorFeeder if upstream connection has failures.
//   - return ErrorNotMyVbucket due to rebalances and failures.
//   - return ErrorStreamEnd if StreamEnd failed for some reason
//   - return ErrorResponseTimeout if feedback is not completed within timeout
//   - return ErrorInvalidKeyspaceIdMap if 1:1 mapping between bucket to keyspaceId
//     is invalid
func (feed *Feed) shutdownVbuckets(
	req *protobuf.ShutdownVbucketsRequest, opaque uint16) (err error) {

	keyspaceIdMap, err := req.GetKeyspaceIdMap()
	if err != nil {
		return err
	}

	// iterate request-timestamp for each bucket.
	for _, ts := range req.GetShutdownTimestamps() {
		pooln, bucketn := ts.GetPool(), ts.GetBucket()
		keyspaceId := keyspaceIdMap[bucketn]

		vbnos, e := feed.getLocalVbuckets(pooln, bucketn, opaque)
		if e != nil {
			err = e
			//FIXME: in case of shutdown we are not cleaning the bucket !
			//wait for the code to settle-down and remove this.
			//feed.cleanupKeyspace(keyspaceId, false)
			continue
		}
		ts := ts.SelectByVbuckets(vbnos)

		actTs, ok1 := feed.actTss[keyspaceId]
		rollTs, ok2 := feed.rollTss[keyspaceId]
		reqTs, ok3 := feed.reqTss[keyspaceId]
		if !ok1 || !ok2 || !ok3 {
			fmsg := "%v ##%x shutdownVbuckets() invalid-bucket %v\n"
			logging.Errorf(fmsg, feed.logPrefix, opaque, bucketn)
			err = projC.ErrorInvalidBucket
			continue
		}
		// only shutdown active-streams.
		if ok1 && actTs != nil {
			ts = ts.SelectByVbuckets(c.Vbno32to16(actTs.GetVbnos()))
		}
		feeder, ok := feed.feeders[keyspaceId]
		if !ok {
			fmsg := "%v ##%x shutdownVbuckets() invalid-feeder %v\n"
			logging.Errorf(fmsg, feed.logPrefix, opaque, keyspaceId)
			err = projC.ErrorInvalidBucket
			continue
		}
		// shutdown upstream
		cleanup := false
		e, cleanup = feed.bucketFeed(opaque, true, false, ts, feeder)
		if e != nil {
			err = e
			if cleanup {
				feed.cleanupKeyspace(keyspaceId, false)
			}
			continue
		}
		if !feed.async {
			endTs, _, e := feed.waitStreamEnds(opaque, keyspaceId, ts)
			vbnos = c.Vbno32to16(endTs.GetVbnos())
			// forget vbnos that are shutdown
			feed.actTss[keyspaceId] = actTs.FilterByVbuckets(vbnos)   // :SideEffect:
			feed.reqTss[keyspaceId] = reqTs.FilterByVbuckets(vbnos)   // :SideEffect:
			feed.rollTss[keyspaceId] = rollTs.FilterByVbuckets(vbnos) // :SideEffect:
			if e != nil {
				err = e
				logging.Errorf(
					"%v ##%x stream-end (err: %v) vbnos: %v\n",
					feed.logPrefix, opaque, err, vbnos)
			} else {
				logging.Infof(
					"%v ##%x stream-end (success) vbnos: %v\n",
					feed.logPrefix, opaque, vbnos)
			}
		} else {
			// in async mode, do not clear out the book keeping.
			// let stream end clear the book keeping. This ensures
			// another stream request cannot be made for a vb while
			// its close stream request is in progress.
		}
	}
	return err
}

// upstreams are added for buckets data-path opened and
// vbucket-routines started.
//   - return ErrorInconsistentFeed for malformed feed request
//   - return ErrorInvalidVbucketBranch for malformed vbuuid.
//   - return ErrorFeeder if upstream connection has failures.
//   - return ErrorNotMyVbucket due to rebalances and failures.
//   - return ErrorStreamRequest if StreamRequest failed for some reason
//   - return ErrorResponseTimeout if feedback is not completed within timeout
//   - return ErrorInvalidKeyspaceIdMap if 1:1 mapping between bucket to keyspaceId
//     is invalid
func (feed *Feed) addBuckets(
	req *protobuf.AddBucketsRequest, opaque uint16) (err error) {

	keyspaceIdMap, err := req.GetKeyspaceIdMap()
	if err != nil {
		return err
	}

	needsAuth := req.GetNeedsAuth()

	// update engines and endpoints
	if _, err = feed.processSubscribers(opaque, req, keyspaceIdMap, needsAuth); err != nil { // :SideEffect:
		return err
	}

	opaque2 := req.GetOpaque2()

	for _, ts := range req.GetReqTimestamps() {
		pooln, bucketn := ts.GetPool(), ts.GetBucket()
		keyspaceId := keyspaceIdMap[bucketn]

		vbnos, e := feed.getLocalVbuckets(pooln, bucketn, opaque)
		if e != nil {
			err = e
			feed.cleanupKeyspace(keyspaceId, false)
			continue
		}
		ts := ts.SelectByVbuckets(vbnos)

		actTs, ok := feed.actTss[keyspaceId]
		if ok { // don't re-request for already active vbuckets
			ts.FilterByVbuckets(c.Vbno32to16(actTs.GetVbnos()))
		}
		rollTs, ok := feed.rollTss[keyspaceId]
		if ok { // foget previous rollback for the current set of buckets
			rollTs = rollTs.FilterByVbuckets(c.Vbno32to16(ts.GetVbnos()))
		}
		reqTs, ok := feed.reqTss[keyspaceId]
		// book-keeping of out-standing request, vbuckets that have
		// out-standing request will be ignored.
		if ok {
			ts = ts.FilterByVbuckets(c.Vbno32to16(reqTs.GetVbnos()))
		}
		reqTs = ts.Union(ts)
		// open or acquire the upstream feeder object.
		feeder, e := feed.openFeeder(opaque, pooln, bucketn, keyspaceId)
		if e != nil {
			err = e
			feed.cleanupKeyspace(keyspaceId, false)
			continue
		}
		feed.feeders[keyspaceId] = feeder // :SideEffect:

		cid := getCollectionIdFromReqTs(ts)
		// open data-path, if not already open.
		kvdata, e := feed.startDataPath(bucketn, keyspaceId, cid, feeder,
			opaque, ts, opaque2, 0 /*use default*/)
		if e != nil { // all feed errors are fatal, skip this bucket.
			err = e
			feed.cleanupKeyspace(keyspaceId, false)
			continue
		}

		engines, _ := feed.engines[keyspaceId]
		kvdata.AddEngines(opaque, engines, feed.endpoints)
		feed.kvdata[keyspaceId] = kvdata // :SideEffect:
		// start upstream
		e, _ = feed.bucketFeed(opaque, false, true, ts, feeder)
		if e != nil { // all feed errors are fatal, skip this bucket.
			err = e
			feed.cleanupKeyspace(keyspaceId, false)
			continue
		}
		if !feed.async {
			// wait for stream to start ...
			r, f, a, e := feed.waitStreamRequests(opaque, pooln, keyspaceId, ts)
			feed.rollTss[keyspaceId] = rollTs.Union(r) // :SideEffect:
			feed.actTss[keyspaceId] = actTs.Union(a)   // :SideEffect
			// forget vbucket for which a response is already received.
			reqTs = reqTs.FilterByVbuckets(c.Vbno32to16(r.GetVbnos()))
			reqTs = reqTs.FilterByVbuckets(c.Vbno32to16(a.GetVbnos()))
			reqTs = reqTs.FilterByVbuckets(c.Vbno32to16(f.GetVbnos()))
			feed.reqTss[keyspaceId] = reqTs // :SideEffect:
			if e != nil {
				err = e
				logging.Errorf(
					"%v ##%x stream-request (err: %v) rollback: %v; vbnos: %v\n",
					feed.logPrefix, opaque, err,
					feed.rollTss[keyspaceId].GetVbnos(),
					feed.actTss[keyspaceId].GetVbnos())
			} else {
				logging.Infof(
					"%v ##%x stream-request (success) rollback: %v; vbnos: %v\n",
					feed.logPrefix, opaque,
					feed.rollTss[keyspaceId].GetVbnos(),
					feed.actTss[keyspaceId].GetVbnos())
			}
		} else {
			feed.reqTss[keyspaceId] = reqTs // :SideEffect:
		}
	}
	return err
}

// upstreams are closed for buckets, data-path is closed for downstream,
// vbucket-routines exits on StreamEnd
//   - return ErrorInvalidKeyspaceIdMap if 1:1 mapping between bucket to keyspaceId
//     is invalid
func (feed *Feed) delBuckets(
	req *protobuf.DelBucketsRequest, opaque uint16) error {

	keyspaceIdMap, err := req.GetKeyspaceIdMap()
	if err != nil {
		return err
	}

	for _, keyspaceId := range keyspaceIdMap {
		feed.cleanupKeyspace(keyspaceId, true)
	}
	return nil
}

// only data-path shall be updated.
//   - return ErrorInconsistentFeed for malformed feed request
//   - return ErrorInvalidKeyspaceIdMap if 1:1 mapping between bucket to keyspaceId
//     is invalid
func (feed *Feed) addInstances(
	req *protobuf.AddInstancesRequest,
	opaque uint16) (*protobuf.TimestampResponse, error) {

	keyspaceIdMap, err := req.GetKeyspaceIdMap()
	if err != nil {
		return nil, err
	}

	tsResp := &protobuf.TimestampResponse{
		Topic:             proto.String(feed.topic),
		CurrentTimestamps: make([]*protobuf.TsVbuuid, 0, 4),
		KeyspaceIds:       make([]string, 0),
	}
	errResp := &protobuf.TimestampResponse{Topic: proto.String(feed.topic)}

	needsAuth := req.GetNeedsAuth()

	buckets, err := feed.processSubscribers(opaque, req, keyspaceIdMap, needsAuth) // :SideEffect:
	if err != nil {
		return errResp, err
	}

	// post to kv data-path for buckets from new subscribers
	// do not touch the kv data-path for buckets that are not part of addInstances
	for bucketn, _ := range buckets {
		keyspaceId := keyspaceIdMap[bucketn]
		engines := feed.engines[keyspaceId]

		if kvdata, ok := feed.kvdata[keyspaceId]; ok {
			curSeqnos, err := kvdata.AddEngines(opaque, engines, feed.endpoints)
			if err != nil {
				return errResp, err
			}
			tsResp = tsResp.AddCurrentTimestamp(feed.pooln, bucketn, curSeqnos)
			tsResp.AddKeyspaceId(keyspaceId)

		} else {
			fmsg := "%v ##%x addInstances() invalid-keyspace %q\n"
			logging.Errorf(fmsg, feed.logPrefix, opaque, keyspaceIdMap[bucketn])
			err = projC.ErrorInvalidBucket
		}
	}

	return tsResp, err
}

// only data-path shall be updated.
//   - if it is the last instance defined on the bucket, then
//     use delBuckets() API to delete the bucket.
func (feed *Feed) delInstances(
	req *protobuf.DelInstancesRequest, opaque uint16) error {

	instanceIds := req.GetInstanceIds()
	keyspaceInsts := make(map[string][]uint64)      // keyspaceId -> []instance
	collectionIds := make(map[string][]uint32)      // keyspaceId -> []collectionId
	fengines := make(map[string]map[uint64]*Engine) // keyspaceId-> uuid-> instance

	filterEngines := func(keyspaceId string, engines map[uint64]*Engine) {
		uuids := make([]uint64, 0)
		m := make(map[uint64]*Engine)
		cids := make([]uint32, 0)
		for uuid, engine := range engines {
			if c.HasUint64(uuid, instanceIds) {
				uuids = append(uuids, uuid)
				cids = append(cids, getCidAsUint32(engine.GetCollectionID()))
			} else {
				m[uuid] = engine
			}
		}
		keyspaceInsts[keyspaceId] = uuids
		collectionIds[keyspaceId] = cids
		fengines[keyspaceId] = m
	}

	reqKeyspaceId := req.GetKeyspaceId()
	if reqKeyspaceId == "" { // Iterate though all keyspaces of the feed as reqKeyspaceId is empty
		for keyspaceId, engines := range feed.engines {
			filterEngines(keyspaceId, engines)
		}
	} else {
		if engines, ok := feed.engines[reqKeyspaceId]; ok {
			filterEngines(reqKeyspaceId, engines)
		}
	}

	var err error
	// posted post to kv data-path.
	for keyspaceId, uuids := range keyspaceInsts {
		if _, ok := feed.kvdata[keyspaceId]; ok {
			feed.kvdata[keyspaceId].DeleteEngines(opaque, uuids, collectionIds[keyspaceId])
		} else {
			fmsg := "%v ##%x delInstances() invalid-keyspace %q"
			logging.Errorf(fmsg, feed.logPrefix, opaque, keyspaceId)
			err = projC.ErrorInvalidBucket
		}
	}
	if reqKeyspaceId == "" { // Update filtered engines across all keyspaces
		feed.engines = fengines // :SideEffect:
	} else { // Update filtered engines only for the keyspace specified in request
		feed.engines[reqKeyspaceId] = fengines[reqKeyspaceId] // :SideEffect:
	}
	return err
}

// endpoints are independent.
func (feed *Feed) repairEndpoints(
	req *protobuf.RepairEndpointsRequest, opaque uint16) (err error) {

	needsAuth := req.GetNeedsAuth()

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
			config.Set("syncTimeout", feed.config["syncTimeout"])
			endpoint, e = feed.epFactory(topic, typ, raddr, config, needsAuth)
			if e != nil {
				fmsg := "%v ##%x endpoint-factory %q: %v\n"
				logging.Errorf(fmsg, prefix, opaque, raddr1, e)
				err = e
				continue
			}
			go feed.watchEndpoint(raddr, endpoint)
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
	for keyspaceId, kvdata := range feed.kvdata {
		// though only endpoints have been updated
		kvdata.AddEngines(opaque, feed.engines[keyspaceId], feed.endpoints)
	}
	//return nil
	return err
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
	for keyspaceId, kvdata := range feed.kvdata {
		stats.Set("keyspace-"+keyspaceId, kvdata.GetStatistics())
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
	econf.Set("syncTimeout", config["syncTimeout"])
	for _, endpoint := range feed.endpoints {
		endpoint.ResetConfig(econf)
	}
	feed.config = feed.config.Override(config)
}

func (feed *Feed) shutdown(opaque uint16) error {
	recovery := func() {
		if r := recover(); r != nil {
			fmsg := "%v ##%x shutdown() crashed: %v\n"
			logging.Errorf(fmsg, feed.logPrefix, opaque, r)
			logging.Errorf("%s", logging.StackTrace())
		}
	}
	defer recovery()

	// close upstream
	for _, feeder := range feed.feeders {
		func() { defer recovery(); feeder.CloseFeed() }()
	}
	// close data-path
	for keyspaceId, kvdata := range feed.kvdata {
		func() { defer recovery(); kvdata.Close() }()
		delete(feed.kvdata, keyspaceId) // :SideEffect:
	}
	// close downstream
	for _, endpoint := range feed.endpoints {
		func() { defer recovery(); endpoint.Close() }()
	}
	// cleanup
	close(feed.finch)

	// Update projector stats to remove the stats belonging to this feed
	// As finch is closed, GetStats() would return nil value and UpdateStats()
	// would delete the feed related stats from feed stats
	feed.projector.UpdateStats(feed.topic, feed, nil)

	logging.Infof("%v ##%x feed ... stopped\n", feed.logPrefix, feed.opaque)
	return nil
}

// shutdown upstream, data-path and remove data-structure for this keyspace.
func (feed *Feed) cleanupKeyspace(keyspaceId string, enginesOk bool) {
	if enginesOk {
		delete(feed.engines, keyspaceId) // :SideEffect:
	}
	delete(feed.reqTss, keyspaceId)  // :SideEffect:
	delete(feed.actTss, keyspaceId)  // :SideEffect:
	delete(feed.rollTss, keyspaceId) // :SideEffect:
	// close upstream
	feeder, ok := feed.feeders[keyspaceId]
	if ok {
		if kvdata, ok := feed.kvdata[keyspaceId]; ok {
			kvdata.StopScatterFromFeed()
		}
		// drain the .C channel until it gets closed or if this feed
		// happends to get closed.
		go func(C <-chan *mc.DcpEvent, finch chan bool) {
			// List of all snapshot messages (per vbucket) that are drained in this go-routine
			snapshotMsgs := make(map[uint16][][2]uint64)
			// List of all seqnoAdvanced messages (per vbucket) that are drained in this go-routine
			seqnoAdvancedMsgs := make(map[uint16][]uint64)
			// List of all systemEvent messages (per vbucket) that are drained in this go-routine
			systemEventMsgs := make(map[uint16][]uint64)

			defer func() {
				for vb, seqnoAdvancedList := range seqnoAdvancedMsgs {
					fmsg := "%v ##%x SeqnoAdvanced for vb: %v, keyspaceId: '%v' is drained in clean-up path. List of Seqnos drained: %v"
					logging.Warnf(fmsg, feed.logPrefix, feed.opaque, vb, keyspaceId, seqnoAdvancedList)
				}

				for vb, systemEventSeqnosList := range systemEventMsgs {
					fmsg := "%v ##%x SystemEvent for vb: %v, keyspaceId: '%v' is drained in clean-up path. List of Seqnos drained: %v"
					logging.Warnf(fmsg, feed.logPrefix, feed.opaque, vb, keyspaceId, systemEventSeqnosList)
				}

				for vb, snapshotMsgList := range snapshotMsgs {
					fmsg := "%v ##%x Snapshot for vb: %v, keyspaceId: '%v' is drained in clean-up path. List of Snapshots drained: %v"
					logging.Warnf(fmsg, feed.logPrefix, feed.opaque, vb, keyspaceId, snapshotMsgList)
				}
			}()

			for {
				select {
				case m, ok := <-C:
					if ok == false {
						return
					}
					switch m.Opcode {
					case mcd.DCP_STREAMREQ:
						fmsg := "%v ##%x DCP_STREAMREQ for vb:%d, keyspaceId: '%v' is drained in clean-up path"
						logging.Errorf(fmsg, feed.logPrefix, m.Opaque, m.VBucket, keyspaceId)
					case mcd.DCP_SEQNO_ADVANCED:
						seqnoAdvancedMsgs[m.VBucket] = append(seqnoAdvancedMsgs[m.VBucket], m.Seqno)
					case mcd.DCP_SYSTEM_EVENT:
						systemEventMsgs[m.VBucket] = append(systemEventMsgs[m.VBucket], m.Seqno)
					case mcd.DCP_SNAPSHOT:
						ss := [2]uint64{m.SnapstartSeq, m.SnapendSeq}
						snapshotMsgs[m.VBucket] = append(snapshotMsgs[m.VBucket], ss)
					}
				case <-finch:
					return
				}
			}
		}(feeder.GetChannel(), feed.finch)
		feeder.CloseFeed()
	}
	delete(feed.feeders, keyspaceId) // :SideEffect:
	// cleanup data structures.
	if kvdata, ok := feed.kvdata[keyspaceId]; ok {
		kvdata.Close()
	}
	delete(feed.kvdata, keyspaceId) // :SideEffect:
	delete(feed.osoSnapshot, keyspaceId)

	fmsg := "%v ##%x keyspace %v removed ..."
	logging.Infof(fmsg, feed.logPrefix, feed.opaque, keyspaceId)
}

func (feed *Feed) openFeeder(
	opaque uint16, pooln, bucketn, keyspaceId string) (BucketFeeder, error) {

	feeder, ok := feed.feeders[keyspaceId]
	if ok {
		return feeder, nil
	}
	bucket, err := feed.connectBucket(feed.cluster, pooln, bucketn, opaque)
	if err != nil {
		return nil, projC.ErrorFeeder
	}

	uuid, err := c.NewUUID()
	if err != nil {
		fmsg := "%v ##%x c.NewUUID(): %v"
		logging.Errorf(fmsg, feed.logPrefix, opaque, err)
		return nil, err
	}
	name := newDCPConnectionName2(keyspaceId, feed.topic, opaque, uuid.Uint64())

	//override with the client specified numDcpConnections if requested
	var numConnections int
	if feed.numDcpConnections > 0 {
		numConnections = int(feed.numDcpConnections)
	} else {
		numConnections = feed.config["dcp.numConnections"].Int()
	}

	dcpConfig := map[string]interface{}{
		"genChanSize":      feed.config["dcp.genChanSize"].Int(),
		"dataChanSize":     feed.config["dcp.dataChanSize"].Int(),
		"numConnections":   numConnections,
		"latencyTick":      feed.config["dcp.latencyTick"].Int(),
		"activeVbOnly":     feed.config["dcp.activeVbOnly"].Bool(),
		"collectionsAware": feed.collectionsAware,
		"osoSnapshot":      feed.osoSnapshot[keyspaceId],
	}

	if common.IsServerlessDeployment() {
		dcpConfig["useMutationQueue"] = feed.config["dcp.serverless.useMutationQueue"].Bool()
	} else {
		dcpConfig["useMutationQueue"] = feed.config["dcp.useMutationQueue"].Bool()
	}
	dcpConfig["mutation_queue.control_data_path_separation"] = feed.config["dcp.mutation_queue.control_data_path_separation"].Bool()
	dcpConfig["mutation_queue.connection_buffer_size"] = feed.config["dcp.mutation_queue.connection_buffer_size"].Int()
	dcpConfig["connection_buffer_size"] = feed.config["dcp.connection_buffer_size"].Int()

	kvaddr, err := feed.getLocalKVAddrs(pooln, bucketn, opaque)
	if err != nil {
		return nil, err
	} else {
		feed.kvaddr = kvaddr
	}

	kvaddrs := []string{kvaddr}
	feeder, err = OpenBucketFeed(
		name, bucket, opaque,
		kvaddrs, dcpConfig,
	)
	if err != nil {
		fmsg := "%v ##%x OpenBucketFeed(%q): %v"
		logging.Errorf(fmsg, feed.logPrefix, opaque, keyspaceId, err)
		return nil, projC.ErrorFeeder
	}
	return feeder, nil
}

// start a feed for a bucket with a set of kvfeeder,
// based on vbmap and failover-logs.
func (feed *Feed) bucketFeed(
	opaque uint16, stop, start bool,
	reqTs *protobuf.TsVbuuid, feeder BucketFeeder) (error, bool) {

	pooln, bucketn := reqTs.GetPool(), reqTs.GetBucket()

	/* The bucketDetails call will retrieve failover logs from KV.

	When there is a bucket flush, indexer would send a restart
	vbuckets request to projector. If the stream request happens
	while the vbuckets are in deleted state, KV would send
	NOT_MY_VBUCKET as response to stream request.

	In the absence of bucketDetails() call, this NOT_MY_VBUCKET
	status comes in data path and it is not propagated back to
	indexer. Therefore, indexer waits for `streamRepairWaitTime`
	to repair the stream. This can cause inconsistent scan results
	till the time stream is repaired.

	Having bucketDetails() here would ensure that the NOT_MY_VBUCKETS
	status (seen while retrieving failover logs) is returned as error
	to restartVBuckets request and indexer retries again without wait */
	vbnos := c.Vbno32to16(reqTs.GetVbnos())
	_ /*vbuuids*/, err := feed.bucketDetails(pooln, bucketn, opaque, vbnos)
	if err != nil {
		return projC.ErrorFeeder, true
	}

	// stop and start are mutually exclusive
	if stop {
		cleanup := false
		fmsg := "%v ##%x stop-timestamp %v\n"
		logging.Infof(fmsg, feed.logPrefix, opaque, reqTs.Repr())
		if err, cleanup = feeder.EndVbStreams(opaque, reqTs); err != nil {
			fmsg := "%v ##%x EndVbStreams(%q): %v"
			logging.Errorf(fmsg, feed.logPrefix, opaque, bucketn, err)
			return projC.ErrorFeeder, cleanup
		}

	} else if start {
		fmsg := "%v ##%x start-timestamp %v\n"
		logging.Infof(fmsg, feed.logPrefix, opaque, reqTs.Repr())
		if err = feeder.StartVbStreams(opaque, reqTs); err != nil {
			fmsg := "%v ##%x StartVbStreams(%q): %v"
			logging.Errorf(fmsg, feed.logPrefix, opaque, bucketn, err)
			return projC.ErrorFeeder, true /* Always clean-up keyspace if an error is seen during startVbStreams */
		}
	}
	return nil, false
}

// - return dcp-client failures.
func (feed *Feed) bucketDetails(
	pooln, bucketn string, opaque uint16, vbnos []uint16) ([]uint64, error) {

	bucket, err := feed.connectBucket(feed.cluster, pooln, bucketn, opaque)
	if err != nil {
		return nil, err
	}
	defer bucket.Close()

	// failover-logs
	dcpConfig := map[string]interface{}{
		"genChanSize":    feed.config["dcp.genChanSize"].Int(),
		"dataChanSize":   feed.config["dcp.dataChanSize"].Int(),
		"numConnections": feed.config["dcp.numConnections"].Int(),
	}

	uuid := common.GetUUID(feed.logPrefix, feed.opaque)
	flogs, err := bucket.GetFailoverLogs(opaque, vbnos, uuid, dcpConfig)
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

func (feed *Feed) getLocalKVAddrs(
	pooln, bucketn string, opaque uint16) (kvaddr string, err error) {
	prefix := feed.logPrefix

	feed.projector.cinfoProviderLock.RLock()
	defer feed.projector.cinfoProviderLock.RUnlock()

	ninfo, err := feed.projector.cinfoProvider.GetNodesInfoProvider()
	if err != nil {
		fmsg := "%v ##%x GetNodesInfoProvider: %v\n"
		logging.Errorf(fmsg, prefix, opaque, err)
		return "", projC.ErrorClusterInfo
	}

	ninfo.RLock()
	kvaddr, err = ninfo.GetLocalServiceAddress("kv", false)
	ninfo.RUnlock()

	if err != nil {
		fmsg := "%v ##%x cinfo.GetLocalServiceAddress(`kv`, false): %v\n"
		logging.Errorf(fmsg, prefix, opaque, err)

		// Force fetch cluster info cache incase it was not syncronized properly,
		// so that next call to this method can succeed
		feed.projector.cinfoProvider.ForceFetch()

		return "", projC.ErrorClusterInfo
	}
	return kvaddr, nil
}

func (feed *Feed) getLocalVbuckets(pooln, bucketn string, opaque uint16) ([]uint16, error) {

	prefix := feed.logPrefix
	cluster := feed.cluster

	binfo, err := common.NewBucketInfo(cluster, pooln, bucketn)
	if err != nil {
		fmsg := "%v ##%x NewBucketInfo(`%v`, `%v`, `%v`): %v\n"
		logging.Errorf(fmsg, prefix, opaque, cluster, pooln, bucketn, err)
		return nil, projC.ErrorClusterInfo
	}

	vbnos, err := binfo.GetLocalVBuckets(bucketn)
	if err != nil {
		fmsg := "%v ##%x cinfo.GetLocalVBuckets(`%v`): %v\n"
		logging.Errorf(fmsg, prefix, opaque, bucketn, err)
		return nil, projC.ErrorClusterInfo
	}

	fmsg := "%v ##%x vbmap {%v,%v} - %v\n"
	logging.Infof(fmsg, feed.logPrefix, opaque, pooln, bucketn, vbnos)

	// vbnos is nil if KV does not own any vbuckets for this Bucket.
	// it can be when MCD crashes and all the vbuckets data for ephemeral
	// bucket is lost and active vBuckets will not be restored automatically
	// on recovery
	// return empty array & not nil as output of this function is passed to
	// SelectByVBuckets and it will select everything when vbnos is nil.
	if len(vbnos) == 0 {
		vbnos = make([]uint16, 0)
	}

	return vbnos, nil
}

// start data-path each kvaddr
func (feed *Feed) startDataPath(
	bucketn, keyspaceId string,
	collectionId string,
	feeder BucketFeeder,
	opaque uint16,
	ts *protobuf.TsVbuuid,
	opaque2 uint64,
	vbucketWorkers int) (*KVData, error) {
	var err error
	mutch := feeder.GetChannel()
	kvdata, ok := feed.kvdata[keyspaceId]
	if ok {
		kvdata.UpdateTs(opaque, ts)

	} else { // pass engines & endpoints to kvdata.
		engs, ends := feed.engines[keyspaceId], feed.endpoints
		kvdata, err = NewKVData(
			feed, bucketn, keyspaceId, collectionId, opaque, ts, engs, ends, mutch,
			feed.kvaddr, feed.config, feed.async, opaque2, feed.collectionsAware,
			vbucketWorkers)
	}
	return kvdata, err
}

// - return ErrorInconsistentFeed for malformed feed request
func (feed *Feed) processSubscribers(opaque uint16, req Subscriber,
	keyspaceIdMap map[string]string, needsAuth bool) (map[string]bool, error) {
	evaluators, routers, err := feed.subscribers(opaque, req)
	if err != nil {
		return nil, err
	}

	// start fresh set of all endpoints from routers.
	if err = feed.startEndpoints(opaque, routers, needsAuth); err != nil {
		return nil, err
	}
	// update feed engines.
	buckets := make(map[string]bool, 0)
	for uuid, evaluator := range evaluators {
		bucketn := evaluator.Bucket()
		// Get the bucket to keyspaceId mapping
		keyspaceId := keyspaceIdMap[bucketn]

		if _, ok := buckets[bucketn]; !ok {
			buckets[bucketn] = true
		}

		m, ok := feed.engines[keyspaceId]
		if !ok {
			m = make(map[uint64]*Engine)
		}
		engine := NewEngine(uuid, evaluator, routers[uuid])
		// For each engine, add logPrefix and instId in the stats
		evalStats := engine.GetEvaluatorStats()
		switch (evalStats).(type) {
		case *protobuf.IndexEvaluatorStats:
			sts := evalStats.(*protobuf.IndexEvaluatorStats)
			sts.InstId = c.IndexInstId(uuid) // uuid is same as index instId
			sts.Topic = feed.topic
			sts.KeyspaceId = keyspaceId
		default:
			logging.Errorf("%v ##%x Invalid evaluator stats type: %T for index inst: %v",
				feed.logPrefix, feed.opaque, evalStats, uuid)
		}
		m[uuid] = engine
		feed.engines[keyspaceId] = m // :SideEffect:
	}
	return buckets, nil
}

// feed.endpoints is updated with freshly started endpoint,
// if an endpoint is already present and active it is
// reused.
func (feed *Feed) startEndpoints(
	opaque uint16, routers map[uint64]c.Router, needsAuth bool) (err error) {

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
				config.Set("syncTimeout", feed.config["syncTimeout"])
				endpoint, e = feed.epFactory(topic, typ, raddr, config, needsAuth)
				if e != nil {
					fmsg := "%v ##%x endpoint-factory %q: %v\n"
					logging.Errorf(fmsg, prefix, opaque, raddr1, e)
					err = e
					continue
				}
				go feed.watchEndpoint(raddr, endpoint)
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
	//return nil
	return err
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
		fmsg := "%v ##%x mismatch in evaluators/routers\n"
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
	pooln, keyspaceId string,
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
		if val, ok := msg.(*controlStreamRequest); ok && val.keyspaceId == keyspaceId &&
			val.opaque == opaque && ts.Contains(val.vbno) {

			if val.status == mcd.SUCCESS {
				actTs.Append(val.vbno, val.seqno, val.vbuuid, 0, 0, "")
			} else if val.status == mcd.ROLLBACK {
				rollTs.Append(val.vbno, val.seqno, val.vbuuid, 0, 0, "")
			} else if val.status == mcd.NOT_MY_VBUCKET {
				failTs.Append(val.vbno, val.seqno, val.vbuuid, 0, 0, "")
				err = projC.ErrorNotMyVbucket
			} else {
				failTs.Append(val.vbno, val.seqno, val.vbuuid, 0, 0, "")
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
	keyspaceId string,
	ts *protobuf.TsVbuuid) (endTs, failTs *protobuf.TsVbuuid, err error) {

	vbnos := c.Vbno32to16(ts.GetVbnos())
	endTs = protobuf.NewTsVbuuid(ts.GetPool(), ts.GetBucket(), len(vbnos))
	failTs = protobuf.NewTsVbuuid(ts.GetPool(), ts.GetBucket(), len(vbnos))
	if len(vbnos) == 0 {
		return endTs, failTs, nil
	}

	timeout := time.After(feed.endTimeout * time.Millisecond)
	err1 := feed.waitOnFeedback(timeout, opaque, func(msg interface{}) string {
		// The opaque value comparision is not required here as StreamEnd
		// messages will carry the same opaque value used with StreamRequest
		// (The opaque value of waitStreamEnds corresponds to the opaque value
		// of the request issued by indexer)
		if val, ok := msg.(*controlStreamEnd); ok && val.keyspaceId == keyspaceId &&
			ts.Contains(val.vbno) {

			if val.status == mcd.SUCCESS {
				endTs.Append(val.vbno, 0 /*seqno*/, 0 /*vbuuid*/, 0, 0, "")
			} else if val.status == mcd.NOT_MY_VBUCKET {
				failTs.Append(val.vbno, 0 /*seqno*/, 0 /*vbuuid*/, 0, 0, "")
				err = projC.ErrorNotMyVbucket
			} else {
				failTs.Append(val.vbno, 0 /*seqno*/, 0 /*vbuuid*/, 0, 0, "")
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
		logging.Infof(fmsg, feed.logPrefix, opaque, len(msgs))
	}
	for _, msg := range msgs {
		feed.backch <- msg
	}
	return
}

// compose topic-response for caller
func (feed *Feed) topicResponse() (*protobuf.TopicResponse, error) {
	uuids := make([]uint64, 0)
	for _, engines := range feed.engines {
		for uuid := range engines {
			uuids = append(uuids, uuid)
		}
	}

	var err error
	// One of the invariants in the feed's code is
	// len(feed.actTss) == len(feed.rollTss) == len(feed.reqTss)
	// and a keyspaceId will be present in all these timestamps i.e.
	// there should not be a case where it is present in one timestamp
	// and not present in another
	if len(feed.actTss) != len(feed.rollTss) ||
		len(feed.rollTss) != len(feed.reqTss) {
		logging.Fatalf("%v Inconsistent book-keeping in feed. feed.actTss: %s, feed.rollTss: %s, feed.reqTss: %s",
			feed.logPrefix, getTssAsStr(feed.actTss), getTssAsStr(feed.rollTss), getTssAsStr(feed.reqTss))
		err = errInconsistentTimestampMapping
	}

	xs := make([]*protobuf.TsVbuuid, 0, len(feed.actTss))
	ys := make([]*protobuf.TsVbuuid, 0, len(feed.rollTss))
	zs := make([]*protobuf.TsVbuuid, 0, len(feed.reqTss))

	keyspaceIds := make([]string, 0)
	for keyspaceId, actTs := range feed.actTss {
		if actTs != nil {
			xs = append(xs, actTs.Clone())
			keyspaceIds = append(keyspaceIds, keyspaceId)
		} else {
			// Ideally, this condition should never be executed
			logging.Fatalf("%v ##%x actTss is nil for keyspaceId: %v, feed.actTss: %s",
				feed.logPrefix, feed.opaque, keyspaceId, getTssAsStr(feed.actTss))
			err = errNilTimestamp
		}

		if rollTs, ok := feed.rollTss[keyspaceId]; !ok {
			// Ideally, this condition should never be executed
			logging.Fatalf("%v ##%x keyspaceId: %v present in actTss but not in feed.rollTss. feed.actTss: %v, feed.rollTss: %v",
				feed.logPrefix, feed.opaque, keyspaceId, getTssAsStr(feed.actTss), getTssAsStr(feed.rollTss))
			err = errInconsistentTimestampMapping
		} else if rollTs == nil {
			// Ideally, this condition should never be executed
			logging.Fatalf("%v ##%x rollTs is nil for keyspaceId: %v, feed.actTss: %s, feed.rollTss: %s",
				feed.logPrefix, feed.opaque, keyspaceId,
				getTssAsStr(feed.actTss), getTssAsStr(feed.rollTss))
			err = errNilTimestamp
		} else {
			ys = append(ys, rollTs.Clone())
		}

		if reqTs, ok := feed.reqTss[keyspaceId]; !ok {
			// Ideally, this condition should never be executed
			logging.Fatalf("%v ##%x keyspaceId: %v present in actTss but not in feed.reqTss. feed.actTss: %s, feed.reqTss: %s",
				feed.logPrefix, feed.opaque, keyspaceId, getTssAsStr(feed.actTss), getTssAsStr(feed.reqTss))
			err = errInconsistentTimestampMapping
		} else if reqTs == nil {
			// Ideally, this condition should never be executed
			logging.Fatalf("%v ##%x reqTs is nil for keyspaceId: %v, feed.actTss: %s, feed.reqTss: %s",
				feed.logPrefix, feed.opaque, keyspaceId, getTssAsStr(feed.actTss), getTssAsStr(feed.reqTss))
			err = errNilTimestamp
		} else {
			zs = append(zs, reqTs.Clone())
		}
	}

	topicResp := &protobuf.TopicResponse{
		Topic:              proto.String(feed.topic),
		InstanceIds:        uuids,
		ActiveTimestamps:   xs,
		RollbackTimestamps: ys,
		PendingTimestamps:  zs,
		KeyspaceIds:        keyspaceIds,
	}
	return topicResp, err
}

func getTssAsStr(ts map[string]*protobuf.TsVbuuid) string {
	tsList := make([]string, 0)
	if ts != nil {
		for key, value := range ts {
			tsList = append(tsList, fmt.Sprintf("%v:%v,", key, value))
		}
	}
	return strings.Join(tsList, ",")
}

// generate a unique opaque identifier.
// NOTE: be careful while changing the DCP name, it might affect other
// parts of the system. ref: https://issues.couchbase.com/browse/MB-14300
func newDCPConnectionName(keyspaceId, topic string, uuid uint64) couchbase.DcpFeedName {
	return couchbase.NewDcpFeedName(fmt.Sprintf("%v%v-%s-%v", mc.DcpFeedNameCompPrefix, keyspaceId, topic, uuid))
}

func newDCPConnectionName2(keyspaceId, topic string,
	opaque uint16, uuid uint64) *mc.DcpFeedname2 {

	return mc.NewDcpFeedName2(topic, keyspaceId, opaque, uuid, 0)
}

//---- endpoint watcher

func (feed *Feed) watchEndpoint(raddr string, endpoint c.RouterEndpoint) {
	RegisterEndpoint(raddr, endpoint)
	err := endpoint.WaitForExit() // <-- will block until endpoint exits.
	logging.Infof("%v endpoint exited: %v", feed.logPrefix, err)
	UnregisterEndpoint(raddr)
	if err := feed.DeleteEndpoint(raddr); err != nil && err != c.ErrorClosed {
		fmsg := "%v failed DeleteEndpoint(): %v"
		logging.Errorf(fmsg, feed.logPrefix, err)
	}
}

//---- local function

// connectBucket will instantiate a couchbase-bucket instance with cluster.
// caller's responsibility to close the bucket.
func (feed *Feed) connectBucket(
	cluster, pooln, bucketn string, opaque uint16) (*couchbase.Bucket, error) {

	ah := &c.CbAuthHandler{Hostport: cluster, Bucket: bucketn}
	couch, err := couchbase.ConnectWithAuth("http://"+cluster, ah)
	if err != nil {
		fmsg := "%v ##%x connectBucket(`%v`): %v"
		logging.Errorf(fmsg, feed.logPrefix, opaque, bucketn, err)
		return nil, projC.ErrorDCPConnection
	}
	pool, err := couch.GetPoolWithBucket(pooln, bucketn)
	if err != nil {
		fmsg := "%v ##%x GetPoolWithBucket(`%v`): %v"
		logging.Errorf(fmsg, feed.logPrefix, opaque, pooln, err)
		return nil, projC.ErrorDCPPool
	}
	bucket, err := pool.GetBucket(bucketn)
	if err != nil {
		fmsg := "%v ##%x GetBucket(`%v`): %v"
		logging.Errorf(fmsg, feed.logPrefix, opaque, bucketn, err)
		return nil, projC.ErrorDCPBucket
	}
	return bucket, nil
}

func getCollectionIdFromReqTs(reqTs *protobuf.TsVbuuid) string {
	cids := reqTs.GetCollectionIDs()
	if len(cids) == 0 {
		return ""
	}
	// All vb's are expected to have the same collectionID for a collection
	// Return the CID of first vb
	return cids[0]
}

func getCidAsUint32(collId string) uint32 {
	if collId == "" {
		return 0
	}
	cid, err := strconv.ParseUint(collId, 16, 32)
	if err != nil {
		// Since collectionID is read from cluster info cache, it
		// is always expected to be a hexadecimal string.
		// If it is not hexadecimal string, then crash on error
		logging.Fatalf("Error while decoding collectionID: %s, err: %v", collId, err)
		c.CrashOnError(err)
	}
	return (uint32)(cid)
}

// FeedConfigParams return the list of configuration params
// supported by a feed.
func FeedConfigParams() []string {
	paramNames := []string{
		"clusterAddr",
		"feedChanSize",
		"backChanSize",
		"vbucketWorkers",
		"feedWaitStreamEndTimeout",
		"feedWaitStreamReqTimeout",
		"mutationChanSize",
		"encodeBufSize",
		"encodeBufResizeInterval",
		"routerEndpointFactory",
		"syncTimeout",
		// dcp configuration
		"dcp.dataChanSize",
		"dcp.genChanSize",
		"dcp.numConnections",
		"dcp.latencyTick",
		"dcp.activeVbOnly",
		"dcp.useMutationQueue",
		"dcp.serverless.useMutationQueue",
		"dcp.connection_buffer_size",
		"dcp.mutation_queue.connection_buffer_size",
		"dcp.mutation_queue.control_data_path_separation",
		// dataport
		"dataport.remoteBlock",
		"dataport.keyChanSize",
		"dataport.bufferSize",
		"dataport.bufferTimeout",
		"dataport.harakiriTimeout",
		"dataport.maxPayload"}
	return paramNames
}
