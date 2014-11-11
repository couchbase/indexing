package client

import "fmt"
import "time"
import "strings"

import ap "github.com/couchbase/indexing/secondary/adminport"
import c "github.com/couchbase/indexing/secondary/common"
import "github.com/couchbase/indexing/secondary/protobuf"
import "github.com/couchbaselabs/goprotobuf/proto"

// Client connects with a projector's adminport to
// issues request and get back response.
type Client struct {
	adminport string
	ap        ap.Client
	// config
	maxVbuckets   int
	retryInterval int
	maxRetries    int
	expBackoff    int
}

// NewClient connect with projector identified by `adminport`.
// - `retryInterval` is specified in milliseconds.
//   if retryInterval is ZERO, API will not perform retry.
// - if `maxRetries` is ZERO, will perform indefinite retry.
func NewClient(adminport string, config c.Config) *Client {
	retryInterval := config["projector.client.retryInterval"].Int()
	maxRetries := config["projector.client.maxRetries"].Int()
	expBackoff := config["projector.client.exponentialBackoff"].Int()

	urlPrefix := config["projector.adminport.urlPrefix"].String()
	ap := ap.NewHTTPClient(adminport, urlPrefix)
	client := &Client{
		adminport:     adminport,
		ap:            ap,
		maxVbuckets:   config["maxVbuckets"].Int(),
		retryInterval: retryInterval,
		maxRetries:    maxRetries,
		expBackoff:    expBackoff,
	}
	return client
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
// Idempotent API.
//
// - return http errors for transport related failures.
// - return ErrorTopicExist if feed is already started.
// - return ErrorInconsistentFeed for malformed feed request.
// - return ErrorInvalidVbucketBranch for malformed vbuuid.
// - return ErrorFeeder if upstream connection has failures.
//      upstream connection is closed for the bucket, the bucket needs to be
//      newly added.
// - return ErrorNotMyVbucket due to rebalances and failures.
// - return ErrorStreamRequest if StreamRequest failed for some reason
// - return ErrorResponseTimeout if request is not completed within timeout.
//
// * except of ErrorFeeder, projector feed will book-keep oustanding request,
//   active vbuckets. Caller should observe mutation feed for
//   StreamBegin and retry until all vbuckets are started.
// * active-timestamps returned in TopicResponse response contain
//   entries only for successfully started {buckets,vbuckets}
// * rollback-timestamp contains vbucket entries that need rollback.
func (client *Client) InitialTopicRequest(
	topic, pooln, kvaddr, endpointType string,
	instances []*protobuf.Instance) (*protobuf.TopicResponse, error) {

	buckets := make(map[string]bool, 0)
	for _, instance := range instances {
		buckets[instance.GetBucket()] = true
	}

	req := protobuf.NewMutationTopicRequest(topic, endpointType, instances)
	kvaddrs := []string{kvaddr}
	for bucketn := range buckets {
		ts, err := client.InitialRestartTimestamp(pooln, bucketn, kvaddrs)
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

// MutationTopicRequest topic from a kvnode, for an initial set of
// instances. Idempotent API.
//
// - return http errors for transport related failures.
// - return ErrorTopicExist if feed is already started.
// - return ErrorInconsistentFeed for malformed feed request.
// - return ErrorInvalidVbucketBranch for malformed vbuuid.
// - return ErrorFeeder if upstream connection has failures.
//      upstream connection is closed for the bucket, the bucket needs to be
//      newly added.
// - return ErrorNotMyVbucket due to rebalances and failures.
// - return ErrorStreamRequest if StreamRequest failed for some reason
// - return ErrorResponseTimeout if request is not completed within timeout.
//
// * except of ErrorFeeder, projector feed will book-keep oustanding request,
//   active vbuckets. Caller should observe mutation feed for
//   StreamBegin and retry until all vbuckets are started.
// * active-timestamps returned in TopicResponse response contain
//   entries only for successfully started {bucket,vbuckets}.
// * rollback-timestamp contains vbucket entries that need rollback.
func (client *Client) MutationTopicRequest(
	topic, endpointType string,
	reqTimestamps []*protobuf.TsVbuuid,
	instances []*protobuf.Instance) (*protobuf.TopicResponse, error) {

	req := protobuf.NewMutationTopicRequest(topic, endpointType, instances)
	req.ReqTimestamps = reqTimestamps
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

// RestartVbuckets for topic. Idempotent API, though it is
// advised that the caller check with cluster manager for,
//   * bucket's sanity
//   * latest VBMap
//   * StreamEnd / StreamBegin message from dataport server.
// before repeating this call.
//
// - return http errors for transport related failures.
// - return ErrorTopicMissing if feed is not started.
// - return ErrorInvalidBucket if bucket is not added.
// - return ErrorInvalidVbucketBranch for malformed vbuuid.
// - return ErrorFeeder if upstream connection has failures.
//      upstream connection is closed for the bucket, the bucket needs to be
//      newly added.
// - return ErrorNotMyVbucket due to rebalances and failures.
// - return ErrorStreamRequest if StreamRequest failed for some reason
// - return ErrorStreamEnd if StreamEnd failed for some reason
// - return ErrorResponseTimeout if request is not completed within timeout.
//
// * if vbucket is already active and to force restart a vbucket stream,
//   use ShutdownVbuckets().
// * except of ErrorFeeder, projector feed will book-keep oustanding request,
//   active vbuckets. Caller should observe mutation feed for
//   StreamBegin and retry until all vbuckets are started.
// * active-timestamps returned in TopicResponse response contain
//   entries only for successfully started {bucket,vbuckets}.
// * rollback-timestamp contains vbucket entries that need rollback.
func (client *Client) RestartVbuckets(
	topic string,
	restartTimestamps []*protobuf.TsVbuuid) (*protobuf.TopicResponse, error) {

	req := protobuf.NewRestartVbucketsRequest(topic)
	for _, restartTs := range restartTimestamps {
		req.Append(restartTs)
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

// ShutdownVbuckets for topic. Idempotent API, though it is
// advised that the caller check with cluster manager for,
//   * bucket's sanity
//   * latest VBMap
//   * StreamEnd / StreamBegin message from dataport server.
// before repeating this call.
//
// - return http errors for transport related failures.
// - return ErrorTopicMissing if feed is not started.
// - return ErrorInvalidBucket if bucket is not added.
// - return ErrorInvalidVbucketBranch for malformed vbuuid.
// - return ErrorFeeder if upstream connection has failures.
//      upstream connection is closed for the bucket, the bucket needs to be
//      newly added.
// - return ErrorResponseTimeout if request is not completed within timeout.
//
// * except of ErrorFeeder, projector feed will book-keep oustanding request,
//   active vbuckets. Caller should observe mutation feed for
//   StreamEnd and retry until all vbuckets are started.
// * active-timestamps returned in TopicResponse response contain
//   entries only for successfully started {bucket,vbuckets}.
// * rollback-timestamp contains vbucket entries that need rollback.
func (client *Client) ShutdownVbuckets(
	topic string, shutdownTimestamps []*protobuf.TsVbuuid) error {

	req := protobuf.NewShutdownVbucketsRequest(topic)
	for _, shutTs := range shutdownTimestamps {
		req.Append(shutTs)
	}
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

// AddBuckets will add buckets and its instances to a topic.
// Idempotent API, though it is advised that the caller check with
// cluster manager for,
//   * bucket's sanity
//   * latest VBMap
//   * StreamEnd / StreamBegin message from dataport server.
// before repeating this call.
//
// - return http errors for transport related failures.
// - return ErrorTopicMissing if feed is not started.
// - return ErrorInconsistentFeed for malformed feed request
// - return ErrorInvalidVbucketBranch for malformed vbuuid.
// - return ErrorFeeder if upstream connection has failures.
//      upstream connection is closed for the bucket, the bucket needs to be
//      newly added.
// - return ErrorResponseTimeout if request is not completed within timeout.
//
// * except of ErrorFeeder, projector feed will book-keep oustanding request,
//   active vbuckets. Caller should observe mutation feed for
//   StreamBegin and retry until all vbuckets are started.
// * active-timestamps returned in TopicResponse response contain
//   entries only for successfully started {bucket,vbuckets}.
// * rollback-timestamp contains vbucket entries that need rollback.
func (client *Client) AddBuckets(
	topic string, reqTimestamps []*protobuf.TsVbuuid,
	instances []*protobuf.Instance) (*protobuf.TopicResponse, error) {

	req := protobuf.NewAddBucketsRequest(topic, instances)
	req.ReqTimestamps = reqTimestamps
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

// DelBuckets will del buckets and all its instances from a topic.
// Idempotent API
// - return http errors for transport related failures.
// - return ErrorTopicMissing if feed is not started.
func (client *Client) DelBuckets(topic string, buckets []string) error {
	req := protobuf.NewDelBucketsRequest(topic, buckets)
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
// - return http errors for transport related failures.
// - return ErrorTopicMissing if feed is not started.
// - return ErrorInconsistentFeed for malformed feed request.
func (client *Client) AddInstances(
	topic string, instances []*protobuf.Instance) error {

	req := protobuf.NewAddInstancesRequest(topic, instances)
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

// DelInstances will del buckets and all its instances from a topic.
// Idempotent API.
//
// - return http errors for transport related failures.
// - return ErrorTopicMissing if feed is not started.
func (client *Client) DelInstances(topic string, uuids []uint64) error {
	req := protobuf.NewDelInstancesRequest(topic, uuids)
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
	topic string, endpoints []string) error {

	req := protobuf.NewRepairEndpointsRequest(topic, endpoints)
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
// for a subset of vbuckets (hosted by `kvaddrs`) in `bucket`.
// - return http errors for transport related failures.
func (client *Client) InitialRestartTimestamp(
	pooln, bucketn string, kvaddrs []string) (*protobuf.TsVbuuid, error) {

	// get vbuckets hosted by `kvaddr`
	vbmap, err := client.GetVbmap(pooln, bucketn, kvaddrs)
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

	ts := protobuf.NewTsVbuuid(pooln, bucketn, client.maxVbuckets)
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
		c.Debugf("Retrying %q after %v mS\n", client.adminport, interval)
		time.Sleep(time.Duration(interval) * time.Millisecond)
		if client.expBackoff > 0 {
			interval *= client.expBackoff
		}
	}
}
