// Nomenclature:
//
// Adminport
// - entry point for all access to projector.
//
// Feed
// - a feed aggregates all mutations from a subset of kv-node for all buckets.
// - the list of kv-nodes and buckets are provided while starting the feed and
//   cannot be changed there after.
//
// Uuid
// - to uniquely identify the index or similar entity requesting document
//   evaluation and routing for mutation stream.
//
// BucketFeed
// - per bucket collection of KVFeed for a subset of vbuckets.
//
// KVFeed
// - per bucket, per kv-node collection of VbStreams for a subset of vbuckets.
// - gathers UprEvent from client and post them to vbucket routines.
//
// Vbucket-routine
// - projector scales with vbucket, that is, for every vbucket a go-routine is
//   spawned.
//
// VbStream
// - stream of mutations from a single vbucket.
//
// failoverTimestamp for each vbucket,
// - latest vbuuid and its high-sequence-number based on failover-log for each
//   vbucket.
// - caller (coordinator/indexer) should make sure that failoverTimestamp is
//   consistent with its original calculations.
//
// kvTimestamp for each vbucket,
// - specifies the vbuuid of the master that is going to stream the mutations.
// - specifies the actual start of the sequence number, like for instance
//   after a rollback.
// - starting sequence number must be less than or equal to sequence number
//   specified in failoverTimestamp.
// - caller (coordinator/indexer) should make sure that kvTimestamp is
//   consistent with requested restartTimestamp.
//
// restartTimestamp for each vbucket,
// - vbuuid must be same as the vbuuid found in kvTimestamp.
// - sequence number must be less than that of failoverTimestamp but greater
//   than that of kvTimestamp
// - computed by the caller (coordinator/indexer)
//
// list of active vbuckets:
// - KVFeed maintains a list of active vbuckets.
// - a vbucket is marked active, corresponding vbucket-routine is started,
//   when KVFeed sees StreamBegin message from upstream.
// - a vbucket is marked as inactive when KVFeed sees StreamEnd message from
//   upstream for that vbucket and corresponding vbucket routine is killed.
// - during normal operation StreamBegin and StreamEnd messages will be
//   generate by couchbase-client.
// - when KVFeed detects that its upstream connection is lost, it will
//   generate StreamEnd message for the subset of vbuckets mapped to that
//   connection.

package projector

import (
	"errors"
	"fmt"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbaselabs/go-couchbase"
)

// error codes

// ErrorInconsistentFeed
var ErrorInconsistentFeed = errors.New("projector.inconsistentFeed")

// ErrorTopicExist
var ErrorTopicExist = errors.New("projector.topicExist")

// ErrorTopicMissing
var ErrorTopicMissing = errors.New("projector.topicMissing")

// ErrorArgument
var ErrorArgument = errors.New("projector.argument")

// RequestReader interface abstract mutation stream requests
type RequestReader interface {
	//IsStart returns true if the request is to start vbucket streams.
	IsStart() bool

	//IsRestart returns true if the request is to restart vbucket streams.
	IsRestart() bool

	//IsShutdown returns true if the request is to shutdown vbucket streams.
	IsShutdown() bool

	//IsUpdateSubscription returns true if an index (aka entities) need to be
	//updated.
	IsUpdateSubscription() bool

	//IsDeleteSubscription returns true if an index (aka entities) need to be
	//deleted.
	IsDeleteSubscription() bool

	// GetTopic will return the name of this mutation stream.
	GetTopic() string

	// GetPools returns a list of pool, one for each bucket listed by
	// GetBuckets()
	GetPools() []string

	// GetBuckets will return a list of buckets relevant for this mutation feed.
	GetBuckets() []string

	// RestartTimestamp specifies the a list of vbuckets, its corresponding
	// vbuuid and sequence no, for specified bucket.
	RestartTimestamp(bucket string) *c.Timestamp
}

// Subscriber interface abstracts evaluators and routers that are implemented
// by mutation-stream requests and subscription requests.
// TODO: Subscriber interface name does not describe the intention adequately.
type Subscriber interface {
	// GetEvaluators will return a map of uuid to Evaluator interface.
	GetEvaluators() (map[uint64]c.Evaluator, error)

	// GetRouters will return a map of uuid to Router interface.
	GetRouters() (map[uint64]c.Router, error)
}

// Projector data structure, a projector is connected to one or more upstream
// kv-nodes.
// TODO: support elastic set of kvnodes, right now they are immutable set.
type Projector struct {
	cluster   string                       // cluster address to connect
	kvaddrs   []string                     // immutable set of kv-nodes to connect with
	adminport string                       // <host:port> for projector's admin-port
	topics    map[string]*Feed             // active topics, mutable dictionary
	buckets   map[string]*couchbase.Bucket // bucket instances
	// gen-server
	reqch chan []interface{}
	finch chan bool
	// statistics
	logPrefix string
	stats     *c.ComponentStat
}

// NewProjector creates a news projector instance and starts a corresponding
// adminport.
func NewProjector(cluster string, kvaddrs []string, adminport string) *Projector {
	p := &Projector{
		cluster:   cluster,
		kvaddrs:   kvaddrs,
		adminport: adminport,
		topics:    make(map[string]*Feed),
		buckets:   make(map[string]*couchbase.Bucket),
		reqch:     make(chan []interface{}),
		finch:     make(chan bool),
		logPrefix: fmt.Sprintf("[projector %s]", adminport),
	}
	go mainAdminPort(adminport, p)
	go p.genServer(p.reqch)
	c.Infof("%v started ...\n", p.logPrefix)
	p.stats = p.newStats()
	p.stats.Set("/kvaddrs", kvaddrs)
	return p
}

func (p *Projector) getBucket(pooln, bucketn string) (*couchbase.Bucket, error) {
	bucket, ok := p.buckets[bucketn]
	if !ok {
		return c.ConnectBucket(p.cluster, pooln, bucketn)
	}
	return bucket, nil
}

func (p *Projector) getKVNodes() []string {
	return p.kvaddrs
}

// gen-server commands
const (
	pCmdGetFeed byte = iota + 1
	pCmdAddFeed
	pCmdDelFeed
	pCmdListTopics
	pCmdGetStatistics
	pCmdClose
)

// GetFeed get feed instance for `topic`.
func (p *Projector) GetFeed(topic string) (*Feed, error) {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{pCmdGetFeed, topic, respch}
	resp, err := c.FailsafeOp(p.reqch, respch, cmd, p.finch)
	if err = c.OpError(err, resp, 1); err != nil {
		return nil, err
	}
	return resp[0].(*Feed), nil
}

// AddFeed save `feed` for `topic` for this projector.
func (p *Projector) AddFeed(topic string, feed *Feed) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{pCmdAddFeed, topic, feed, respch}
	resp, err := c.FailsafeOp(p.reqch, respch, cmd, p.finch)
	return c.OpError(err, resp, 0)
}

// DelFeed delete feed for `topic`.
func (p *Projector) DelFeed(topic string) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{pCmdDelFeed, topic, respch}
	resp, err := c.FailsafeOp(p.reqch, respch, cmd, p.finch)
	return c.OpError(err, resp, 0)
}

// ListTopics all topics as array of string.
func (p *Projector) ListTopics() ([]string, error) {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{pCmdListTopics, respch}
	resp, err := c.FailsafeOp(p.reqch, respch, cmd, p.finch)
	err = c.OpError(err, resp, 1)
	return resp[0].([]string), err
}

// GetStatistics will get all or subset of statistics from projector.
func (p *Projector) GetStatistics() map[string]interface{} {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{pCmdGetStatistics, respch}
	resp, _ := c.FailsafeOp(p.reqch, respch, cmd, p.finch)
	return resp[1].(map[string]interface{})
}

// Close this projector.
func (p *Projector) Close() error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{pCmdClose, respch}
	resp, err := c.FailsafeOp(p.reqch, respch, cmd, p.finch)
	return c.OpError(err, resp, 0)
}

func (p *Projector) genServer(reqch chan []interface{}) {
loop:
	for {
		msg := <-reqch
		switch msg[0].(byte) {
		case pCmdListTopics:
			respch := msg[1].(chan []interface{})
			respch <- []interface{}{p.listTopics(), nil}

		case pCmdGetStatistics:
			respch := msg[1].(chan []interface{})
			respch <- []interface{}{p.getStatistics()}

		case pCmdGetFeed:
			respch := msg[2].(chan []interface{})
			feed, err := p.getFeed(msg[1].(string))
			respch <- []interface{}{feed, err}

		case pCmdAddFeed:
			respch := msg[3].(chan []interface{})
			respch <- []interface{}{p.addFeed(msg[1].(string), msg[2].(*Feed))}

		case pCmdDelFeed:
			respch := msg[2].(chan []interface{})
			respch <- []interface{}{p.delFeed(msg[1].(string))}

		case pCmdClose:
			p.doClose()
			break loop
		}
	}
}

func (p *Projector) listTopics() []string {
	topics := make([]string, 0, len(p.topics))
	for topic := range p.topics {
		topics = append(topics, topic)
	}
	return topics
}

func (p *Projector) getStatistics() map[string]interface{} {
	feeds, _ := c.NewComponentStat(p.stats.Get("/feeds"))
	for topic, feed := range p.topics {
		feeds.Set("/"+topic, feed.GetStatistics())
	}
	return p.stats.ToMap()
}

func (p *Projector) getFeed(topic string) (*Feed, error) {
	if feed, ok := p.topics[topic]; ok {
		return feed, nil
	}
	return nil, ErrorTopicMissing
}

func (p *Projector) addFeed(topic string, feed *Feed) (err error) {
	if _, ok := p.topics[topic]; ok {
		return ErrorTopicExist
	}
	c.Infof("%v %q feed added ...", p.logPrefix, topic)
	p.topics[topic] = feed
	return
}

func (p *Projector) delFeed(topic string) (err error) {
	if _, ok := p.topics[topic]; ok == false {
		return ErrorTopicMissing
	}
	delete(p.topics, topic)
	c.Infof("%v ... %q feed deleted", p.logPrefix, topic)
	return
}

func (p *Projector) doClose() error {
	for _, feed := range p.topics {
		feed.CloseFeed()
	}
	for _, bucket := range p.buckets {
		bucket.Close()
	}
	close(p.finch)
	c.Infof("%v ... stopped.\n", p.logPrefix)
	return nil
}
