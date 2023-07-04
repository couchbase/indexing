// Client for projector's adminport.
//
// Client APIs:
//   - start a new feed for one or more buckets with one or more instances.
//   - restart one or more {bucket,vbuckets}.
//   - shutdown one or more {bucket,buckets}.
//   - add one or more buckets to existing feed.
//   - del one or more buckets from an existing feed.
//   - add one or more instances to existing feed.
//   - del one or more instances from an existing feed.
//   - repair one or more endpoints for an existing feed, to restart
//     an endpoint client that experienced transient connection problems.
//
// what is an instance ?
//   An instance is an abstraction implementing Evaluator{} and Router{}
//   interface, the primary function is to transform KV documents to custom
//   data and route them to one or more endpoints.
//
// General notes on client APIs:
//   - if returned error value is nil or empty-string, then the call is
//     considered as SUCCESS.
//   - since APIs accept request-timestamps for more than one bucket, and
//     it is designed to continue with next request-timestamp even in case
//     of an error, there can be multiple errors and only the last error
//     is return back to the caller.
//   - if an expected bucket is missing in TopicResponse:activeTimestamps,
//     it means it is shutdown and all its data structures are cleaned-up
//     due to upstream errors.
//   - while adding a bucket in MutationTopicRequest(), RestartVbuckets(),
//     AddBuckets(), atleast one valid instance must be defined for each
//     bucket.
//   - to delete the last instance for a bucket, use DelBucket() to delete
//     the bucket itself, because projector does not encourage a bucket
//     with ZERO instance.
//
// Idempotent retry MutationTopicRequest(), RestartVbuckets(), AddBuckets():
//   - Before attempting a retry, caller should cross-check with cluster
//     manager (eg. ns_server) for,
//     * bucket's sanity
//     * latest VBMap
//   - Caller should check that union of activeTimestamps from all
//     projectors should contain full set of vbuckets and then post a
//     SUCCESS to dataport-receiver.
//   - It is okay to pass the full set of vbuckets in requestTimestamps
//     for each projector, projector will filter out relevant
//     vbuckets that are co-located and further filter out active-vbuckets
//     and outstanding requests, before posting a StreamRequest for
//     vbuckets.
//   - Dataport-receiver shall cross check its active list of vbuckets
//     with activeTimestamps from all projectors.
//
// Idempotent retry RepairEndpoints():
//   - Caller should book-keep following information via a monitor routine.
//     * ControlSuccess, for vbuckets that have successfully completed
//       StreamRequest.
//     * ConnectionError
//     * StreamBegin
//     * StreamEnd
//   - In case of ConnectionError, StreamEnd, absence of StreamBegin or
//     absence of Sync message for a period of time, monitor-routine shall
//     post RepairEndpoints to projector hosting the vbucket.

package client

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/couchbase/indexing/secondary/logging"

	apclient "github.com/couchbase/indexing/secondary/adminport/client"
	apcommon "github.com/couchbase/indexing/secondary/adminport/common"

	c "github.com/couchbase/indexing/secondary/common"

	protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
	"github.com/golang/protobuf/proto"
)

// error codes

// ErrorTopicExist
var ErrorTopicExist = errors.New("projector.topicExist")

// ErrorTopicMissing
var ErrorTopicMissing = errors.New("projector.topicMissing")

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

// ErrorDCPConnection
var ErrorDCPConnection = errors.New("feed.dcpConnection")

// ErrorDCPPool
var ErrorDCPPool = errors.New("feed.dcpPool")

// ErrorDCPBucket
var ErrorDCPBucket = errors.New("feed.dcpBucket")

// ErrorClusterInfo
var ErrorClusterInfo = errors.New("feed.clusterInfo")

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

// Client connects with a projector's adminport to
// issues request and get back response.
type Client struct {
	adminport string
	ap        apcommon.Client
	// config
	retryInterval int
	maxRetries    int
	expBackoff    int
}

// NewClient connect with projector identified by `adminport`.
// - `retryInterval` is specified in milliseconds.
//   if retryInterval is ZERO, API will not perform retry.
// - if `maxRetries` is ZERO, will perform indefinite retry.
func NewClient(adminport string, config c.Config) (*Client, error) {
	retryInterval := config["retryInterval"].Int()
	maxRetries := config["maxRetries"].Int()
	expBackoff := config["exponentialBackoff"].Int()

	urlPrefix := config["urlPrefix"].String()
	ap, err := apclient.NewHTTPClient(adminport, urlPrefix)
	if err != nil {
		return nil, err
	}

	client := &Client{
		adminport:     adminport,
		ap:            ap,
		retryInterval: retryInterval,
		maxRetries:    maxRetries,
		expBackoff:    expBackoff,
	}
	return client, nil
}

// GetVbmap from projector, for a set of kvnodes.
// - return http errors for transport related failures.
// - return couchbase SDK error if any.
func (client *Client) GetVbmap(
	pooln, bucketn string, kvaddrs []string) (*protobuf.VbmapResponse, error) {

	req := &protobuf.VbmapRequest{
		Pool:    proto.String(pooln),
		Bucket:  proto.String(bucketn),
		Kvaddrs: kvaddrs,
	}
	res := &protobuf.VbmapResponse{}
	err := client.withRetry(
		func() error {
			err := client.ap.Request(req, res)
			if err != nil {
				return err
			} else if protoerr := res.GetErr(); protoerr != nil {
				return fmt.Errorf(protoerr.GetError())
			}
			return err // nil
		})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// GetFailoverLogs from projector, for a set vbuckets.
// - return http errors for transport related failures.
// - return couchbase SDK error if any.
func (client *Client) GetFailoverLogs(
	pooln, bucketn string,
	vbnos []uint32) (*protobuf.FailoverLogResponse, error) {

	req := &protobuf.FailoverLogRequest{
		Pool:   proto.String(pooln),
		Bucket: proto.String(bucketn),
		Vbnos:  vbnos,
	}
	res := &protobuf.FailoverLogResponse{}
	err := client.withRetry(
		func() error {
			err := client.ap.Request(req, res)
			if err != nil {
				return err
			} else if protoerr := res.GetErr(); protoerr != nil {
				return fmt.Errorf(protoerr.GetError())
			}
			return err // nil
		})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// InitialTopicRequest topic from a kvnode, for an initial set
// of instances. Initial topic will always start vbucket
// streams from seqno number ZERO using the latest-vbuuid.
//
// Idempotent API.
// - return TopicResponse that contain current set of
//   active-timestamps and rollback-timestamps reflected from
//   projector, even in case of error.
//
// Possible errors returned,
// - http errors for transport related failures.
// - ErrorInvalidKVaddrs if projector unable to find colocated host.
// - ErrorInconsistentFeed for malformed feed request.
// - ErrorInvalidVbucketBranch for malformed vbuuid.
// - ErrorFeeder if upstream connection has failures.
//      upstream connection is closed for the bucket, the bucket
//      needs to be newly added.
// - ErrorNotMyVbucket due to rebalances and failures.
// - ErrorStreamRequest if StreamRequest failed for some reason
// - ErrorResponseTimeout if request is not completed within timeout.
//
// * except of ErrorFeeder, projector feed will book-keep oustanding
//   request for vbuckets and active vbuckets. Caller should observe
//   mutation feed for StreamBegin and retry until all vbuckets are
//   started.
// * active-timestamps returned in TopicResponse contain entries
//   only for successfully started {buckets,vbuckets}.
// * rollback-timestamps contain vbucket entries that need rollback.
func (client *Client) InitialTopicRequest(
	topic, pooln, endpointType string,
	instances []*protobuf.Instance,
	async bool,
	opaque2 uint64,
	collectionAware bool,
	enableOSO bool,
	needsAuth bool,
	numVBuckets int,
	numVbWorkers uint32,
	numDcpConns uint32) (*protobuf.TopicResponse, error) {

	buckets := make(map[string]bool, 0)
	for _, instance := range instances {
		buckets[instance.GetBucket()] = true
	}

	req := protobuf.NewMutationTopicRequest(topic, endpointType,
		instances, async, opaque2, collectionAware, enableOSO,
		needsAuth, numVbWorkers, numDcpConns)
	for bucketn := range buckets {
		ts, err := client.InitialRestartTimestamp(pooln, bucketn, numVBuckets)
		if err != nil {
			return nil, err
		}
		req.Append(ts)
	}
	res := &protobuf.TopicResponse{}
	err := client.withRetry(
		func() error {
			err := client.ap.Request(req, res)
			if err != nil {
				return err
			} else if protoerr := res.GetErr(); protoerr != nil {
				return fmt.Errorf(protoerr.GetError())
			}
			return err // nil
		})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// MutationTopicRequest topic from a kvnode, with initial set
// of instances.
//
// Idempotent API.
// - return TopicResponse that contain current set of
//   active-timestamps and rollback-timestamps reflected from
//   projector, even in case of error.
// - Since the API is idempotent, it can be called repeatedly until
//   all requested vbuckets are started and returns SUCCESS to caller.
//
// Possible errors returned,
// - http errors for transport related failures.
// - ErrorInvalidKVaddrs if projector unable to find colocated host.
// - ErrorInconsistentFeed for malformed feed request.
// - ErrorInvalidVbucketBranch for malformed vbuuid.
// - ErrorFeeder if upstream connection has failures.
//      upstream connection is closed for the bucket, the bucket
//      needs to be newly added.
// - ErrorNotMyVbucket due to rebalances and failures.
// - ErrorStreamRequest if StreamRequest failed for some reason
// - ErrorResponseTimeout if request is not completed within timeout.
//
// * except of ErrorFeeder, projector feed will book-keep oustanding
//   request for vbuckets and active vbuckets. Caller should observe
//   mutation feed for StreamBegin and retry until all vbuckets are
//   started.
// * active-timestamps returned in TopicResponse response contain
//   entries only for successfully started {bucket,vbuckets}.
// * rollback-timestamp contains vbucket entries that need rollback.
func (client *Client) MutationTopicRequest(
	topic, endpointType string,
	reqTimestamps []*protobuf.TsVbuuid,
	instances []*protobuf.Instance,
	async bool,
	opaque2 uint64,
	keyspaceIds []string,
	collectionAware bool,
	enableOSO bool,
	needsAuth bool,
	numVbWorkers uint32,
	numDcpConns uint32) (*protobuf.TopicResponse, error) {

	req := protobuf.NewMutationTopicRequest(topic,
		endpointType, instances, async, opaque2, collectionAware,
		enableOSO, needsAuth, numVbWorkers, numDcpConns)
	req.ReqTimestamps = reqTimestamps
	req.KeyspaceIds = keyspaceIds

	res := &protobuf.TopicResponse{}
	err := client.withRetry(
		func() error {
			err := client.ap.Request(req, res)
			if err != nil {
				return err
			} else if protoerr := res.GetErr(); protoerr != nil {
				return fmt.Errorf(protoerr.GetError())
			}
			return err // nil
		})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// RestartVbuckets for one or more {bucket, vbuckets}. If a vbucket
// is already active or if there is an outstanding StreamRequset
// for a vbucket, then that vbucket is ignored.
//
// Idempotent API.
// - return TopicResponse that contain current set of
//   active-timestamps and rollback-timestamps reflected from
//   projector, even in case of error.
// - Since the API is idempotent, it can be called repeatedly until
//   all requested vbuckets are started and returns SUCCESS to caller.
//
// Possible errors returned,
// - http errors for transport related failures.
// - ErrorTopicMissing if feed is not started.
// - ErrorInvalidBucket if bucket is not added.
// - ErrorInvalidVbucketBranch for malformed vbuuid.
// - ErrorFeeder if upstream connection has failures.
//      upstream connection is closed for the bucket, the bucket
//      needs to be newly added.
// - ErrorNotMyVbucket due to rebalances and failures.
// - ErrorStreamRequest if StreamRequest failed for some reason
// - ErrorStreamEnd if StreamEnd failed for some reason
// - ErrorResponseTimeout if request is not completed within timeout.
//
// * if vbucket is already active and to force restart a vbucket
//   stream, use ShutdownVbuckets().
// * except of ErrorFeeder, projector feed will book-keep oustanding
//   request for vbuckets and active vbuckets. Caller should observe
//   mutation feed for StreamBegin and retry until all vbuckets are
//   started.
// * active-timestamps returned in TopicResponse response contain
//   entries only for successfully started {bucket,vbuckets}.
// * rollback-timestamp contains vbucket entries that need rollback.
func (client *Client) RestartVbuckets(
	topic string, opaque2 uint64,
	restartTimestamps []*protobuf.TsVbuuid,
	keyspaceIds []string, needsAuth bool) (*protobuf.TopicResponse, error) {

	req := protobuf.NewRestartVbucketsRequest(topic, opaque2, needsAuth)
	for _, restartTs := range restartTimestamps {
		req.Append(restartTs)
	}
	req.KeyspaceIds = keyspaceIds

	res := &protobuf.TopicResponse{}
	err := client.withRetry(
		func() error {
			err := client.ap.Request(req, res)
			if err != nil {
				return err
			} else if protoerr := res.GetErr(); protoerr != nil {
				return fmt.Errorf(protoerr.GetError())
			}
			return err // nil
		})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// ShutdownVbuckets for one or more {bucket, vbuckets}.
//
// Idempotent API
// - return TopicResponse that contain current set of
//   active-timestamps, after shutting down all vbuckets or partial set
//   of vbuckets.
// - Since the API is idempotent, it can be called repeatedly until
//   all requested vbuckets have ended and returns SUCCESS to caller.
//
// Possible errors returned,
// - errors for transport related failures.
// - ErrorTopicMissing if feed is not started.
// - ErrorInvalidBucket if bucket is not added.
// - ErrorInvalidVbucketBranch for malformed vbuuid.
// - ErrorFeeder if upstream connection has failures.
//      upstream connection is closed for the bucket, the bucket
//      needs to be newly added.
// - ErrorResponseTimeout if request is not completed within timeout.
//
// * except of ErrorFeeder, projector feed will book-keep oustanding
//   request for vbuckets and active vbuckets. Caller should observe
//   mutation feed for StreamBegin and retry until all vbuckets are
//   started.
// * active-timestamps returned in TopicResponse response contain
//   entries only for successfully started {bucket,vbuckets}.
// * rollback-timestamp contains vbucket entries that need rollback.
func (client *Client) ShutdownVbuckets(
	topic string, shutdownTimestamps []*protobuf.TsVbuuid,
	keyspaceIds []string) error {

	req := protobuf.NewShutdownVbucketsRequest(topic)
	for _, shutTs := range shutdownTimestamps {
		req.Append(shutTs)
	}
	req.KeyspaceIds = keyspaceIds
	res := &protobuf.Error{}
	err := client.withRetry(
		func() error {
			err := client.ap.Request(req, res)
			if err != nil {
				return err
			} else if s := res.GetError(); s != "" {
				return fmt.Errorf(s)
			}
			return err // nil
		})
	if err != nil {
		return err
	}
	return nil
}

// AddBuckets will add one or more buckets to an active-feed.
//
// Idempotent API.
// - return TopicResponse that contain current set of
//   active-timestamps and rollback-timestamps reflected from
//   projector, even in case of error.
// - Since the API is idempotent, it can be called repeatedly until
//   all requested vbuckets are started and returns SUCCESS to caller.
//
// Possible errors returned,
// - http errors for transport related failures.
// - ErrorTopicMissing if feed is not started.
// - ErrorInconsistentFeed for malformed feed request
// - ErrorInvalidVbucketBranch for malformed vbuuid.
// - ErrorFeeder if upstream connection has failures.
//      upstream connection is closed for the bucket, the bucket needs to be
//      newly added.
// - ErrorNotMyVbucket due to rebalances and failures.
// - ErrorStreamRequest if StreamRequest failed for some reason
// - ErrorResponseTimeout if request is not completed within timeout.
//
// * except of ErrorFeeder, projector feed will book-keep oustanding
//   request for vbuckets and active vbuckets. Caller should observe
//   mutation feed for StreamBegin and retry until all vbuckets are
//   started.
// * active-timestamps returned in TopicResponse response contain
//   entries only for successfully started {bucket,vbuckets}.
// * rollback-timestamp contains vbucket entries that need rollback.
func (client *Client) AddBuckets(
	topic string, reqTimestamps []*protobuf.TsVbuuid,
	instances []*protobuf.Instance,
	keyspaceIds []string, needsAuth bool) (*protobuf.TopicResponse, error) {

	req := protobuf.NewAddBucketsRequest(topic, instances, needsAuth)
	req.ReqTimestamps = reqTimestamps
	req.KeyspaceIds = keyspaceIds
	res := &protobuf.TopicResponse{}
	err := client.withRetry(
		func() error {
			err := client.ap.Request(req, res)
			if err != nil {
				return err
			} else if protoerr := res.GetErr(); protoerr != nil {
				return fmt.Errorf(protoerr.GetError())
			}
			return err // nil
		})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// DelBuckets will delete one or more buckets, and all of its instances,
// from a feed. Idempotent API.
//
// Possible errors returned,
// - http errors for transport related failures.
// - ErrorTopicMissing if feed is not started.
func (client *Client) DelBuckets(topic string,
	buckets []string,
	keyspaceIds []string) error {
	req := protobuf.NewDelBucketsRequest(topic, buckets, keyspaceIds)
	req.KeyspaceIds = keyspaceIds
	res := &protobuf.Error{}
	err := client.withRetry(
		func() error {
			err := client.ap.Request(req, res)
			if err != nil {
				return err
			} else if s := res.GetError(); s != "" {
				return fmt.Errorf(s)
			}
			return err // nil
		})
	if err != nil {
		return err
	}
	return nil
}

// AddInstances will add one or more instances to one or more
// buckets. Idempotent API, provided ErrorInconsistentFeed is
// addressed.
//
// Possible errors returned,
// - http errors for transport related failures.
// - ErrorTopicMissing if feed is not started.
// - ErrorInconsistentFeed for malformed feed request.
func (client *Client) AddInstances(
	topic string,
	instances []*protobuf.Instance,
	keyspaceId string, needsAuth bool) (*protobuf.TimestampResponse, error) {

	req := protobuf.NewAddInstancesRequest(topic, instances, needsAuth)
	req.KeyspaceId = proto.String(keyspaceId)
	res := &protobuf.TimestampResponse{}
	err := client.withRetry(
		func() error {
			err := client.ap.Request(req, res)
			if err != nil {
				return err
			} else if protoerr := res.GetErr(); protoerr != nil {
				return fmt.Errorf(protoerr.GetError())
			}
			return err // nil
		})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// DelInstances will delete one or more instances from one or more buckets.
// If the deleted instance is the last instance for bucket, then caller
// should have used DelBuckets() to delete the bucket. Projector does not
// encourage a bucket with ZERO instance. Idempotent API.
//
// Possible errors returned,
// - http errors for transport related failures.
// - ErrorTopicMissing if feed is not started.
func (client *Client) DelInstances(topic string,
	uuids []uint64,
	keyspaceId string) error {
	req := protobuf.NewDelInstancesRequest(topic, uuids)
	req.KeyspaceId = proto.String(keyspaceId)
	res := &protobuf.Error{}
	err := client.withRetry(
		func() error {
			err := client.ap.Request(req, res)
			if err != nil {
				return err
			} else if s := res.GetError(); s != "" {
				return fmt.Errorf(s)
			}
			return err // nil
		})
	if err != nil {
		return err
	}
	return nil
}

// RepairEndpoints will restart endpoints. Idempotent API.
//
// - return http errors for transport related failures.
// - return ErrorTopicMissing if feed is not started.
func (client *Client) RepairEndpoints(
	topic string, endpoints []string, needsAuth bool) error {

	req := protobuf.NewRepairEndpointsRequest(topic, endpoints, needsAuth)
	res := &protobuf.Error{}
	err := client.withRetry(
		func() error {
			err := client.ap.Request(req, res)
			if err != nil {
				return err
			} else if s := res.GetError(); s != "" {
				return fmt.Errorf(s)
			}
			return err // nil
		})
	if err != nil {
		return err
	}
	return nil
}

// ShutdownTopic will stop the feed for topic. Idempotent API.
//
// - return http errors for transport related failures.
// - return ErrorTopicMissing if feed is not started.
func (client *Client) ShutdownTopic(topic string) error {
	req := protobuf.NewShutdownTopicRequest(topic)
	res := &protobuf.Error{}
	err := client.withRetry(
		func() error {
			err := client.ap.Request(req, res)
			if err != nil {
				return err
			} else if s := res.GetError(); s != "" {
				return fmt.Errorf(s)
			}
			return err // nil
		})
	if err != nil {
		return err
	}
	return nil
}

// InitialRestartTimestamp will compose the initial set of timestamp
// for a subset of vbuckets in `bucket`.
// - return http errors for transport related failures.
func (client *Client) InitialRestartTimestamp(
	pooln, bucketn string, numVBuckets int) (*protobuf.TsVbuuid, error) {

	// get vbucket map.
	vbmap, err := client.GetVbmap(pooln, bucketn, nil)
	if err != nil {
		return nil, err
	}
	// get failover logs for vbuckets
	pflogs, err := client.GetFailoverLogs(pooln, bucketn, vbmap.AllVbuckets32())
	if err != nil {
		return nil, err
	}
	vbnos := vbmap.AllVbuckets16()
	flogs := pflogs.ToFailoverLog(vbnos)

	ts := protobuf.NewTsVbuuid(pooln, bucketn, numVBuckets)
	return ts.InitialRestartTs(flogs), nil
}

func (client *Client) withRetry(fn func() error) (err error) {
	interval := client.retryInterval
	maxRetries := client.maxRetries
	for {
		err = fn()
		if err == nil {
			return err
		} else if strings.Contains(err.Error(), "connection refused") == false {
			return err
		} else if interval <= 0 { // No retry
			return err
		}
		if maxRetries > 0 { // applicable only if greater than ZERO
			maxRetries--
			if maxRetries == 0 { // maxRetry expired
				return err
			}
		}
		logging.Debugf("Retrying %q after %v mS\n", client.adminport, interval)
		time.Sleep(time.Duration(interval) * time.Millisecond)
		if client.expBackoff > 0 {
			interval *= client.expBackoff
		}
	}
}

func (client *Client) String() string {
	return client.adminport
}
