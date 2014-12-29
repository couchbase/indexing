package projector

import "fmt"
import "sync"
import "strings"

import ap "github.com/couchbase/indexing/secondary/adminport"
import c "github.com/couchbase/indexing/secondary/common"
import projC "github.com/couchbase/indexing/secondary/projector/client"
import protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
import "github.com/couchbaselabs/goprotobuf/proto"

// Projector data structure, a projector is connected to
// one or more upstream kv-nodes. Works in tandem with
// projector's adminport.
type Projector struct {
	mu     sync.RWMutex
	admind ap.Server        // admin-port server
	topics map[string]*Feed // active topics

	// config params
	name        string // human readable name of the projector
	clusterAddr string // kv cluster's address to connect
	adminport   string // projector listens on this adminport
	maxvbs      int
	config      c.Config // full configuration information.
	logPrefix   string
}

// NewProjector creates a news projector instance and
// starts a corresponding adminport.
func NewProjector(maxvbs int, config c.Config) *Projector {
	p := &Projector{
		name:        config["name"].String(),
		clusterAddr: config["clusterAddr"].String(),
		topics:      make(map[string]*Feed),
		maxvbs:      maxvbs,
		adminport:   config["adminport.listenAddr"].String(),
		config:      config,
	}
	cluster := p.clusterAddr
	if !strings.HasPrefix(p.clusterAddr, "http://") {
		cluster = "http://" + cluster
	}
	p.logPrefix = fmt.Sprintf("PROJ[%s]", p.adminport)

	apConfig := config.SectionConfig("adminport.", true)
	apConfig = apConfig.SetValue("name", "PRAM")
	reqch := make(chan ap.Request)
	p.admind = ap.NewHTTPServer(apConfig, reqch)

	go p.mainAdminPort(reqch)
	c.Infof("%v started ...\n", p.logPrefix)
	return p
}

// GetFeed object for `topic`.
// - return ErrorTopicMissing if topic is not started.
func (p *Projector) GetFeed(topic string) (*Feed, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if feed, ok := p.topics[topic]; ok {
		return feed, nil
	}
	return nil, projC.ErrorTopicMissing
}

// AddFeed object for `topic`.
// - return ErrorTopicExist if topic is duplicate.
func (p *Projector) AddFeed(topic string, feed *Feed) (err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.topics[topic]; ok {
		return projC.ErrorTopicExist
	}
	p.topics[topic] = feed
	c.Infof("%v %q feed added ...\n", p.logPrefix, topic)
	return
}

// DelFeed object for `topic`.
// - return ErrorTopicMissing if topic is not started.
func (p *Projector) DelFeed(topic string) (err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.topics[topic]; ok == false {
		return projC.ErrorTopicMissing
	}
	delete(p.topics, topic)
	c.Infof("%v ... %q feed deleted\n", p.logPrefix, topic)
	return
}

//---- handler for admin-port request

// - return couchbase SDK error if any.
func (p *Projector) doVbmapRequest(
	request *protobuf.VbmapRequest) ap.MessageMarshaller {

	c.Tracef("%v doVbmapRequest\n", p.logPrefix)
	response := &protobuf.VbmapResponse{}

	pooln := request.GetPool()
	bucketn := request.GetBucket()
	kvaddrs := request.GetKvaddrs()

	// get vbmap from bucket connection.
	bucket, err := c.ConnectBucket(p.clusterAddr, pooln, bucketn)
	if err != nil {
		c.Errorf("%v for bucket %q, %v\n", p.logPrefix, bucketn, err)
		response.Err = protobuf.NewError(err)
		return response
	}
	defer bucket.Close()

	bucket.Refresh()
	m, err := bucket.GetVBmap(kvaddrs)
	if err != nil {
		c.Errorf("%v for bucket %q, %v\n", p.logPrefix, bucketn, err)
		response.Err = protobuf.NewError(err)
		return response
	}

	// compose response
	response.Kvaddrs = make([]string, 0, len(kvaddrs))
	response.Kvvbnos = make([]*protobuf.Vbuckets, 0, len(kvaddrs))
	for kvaddr, vbnos := range m {
		response.Kvaddrs = append(response.Kvaddrs, kvaddr)
		response.Kvvbnos = append(
			response.Kvvbnos, &protobuf.Vbuckets{Vbnos: c.Vbno16to32(vbnos)})
	}
	return response
}

// - return couchbase SDK error if any.
func (p *Projector) doFailoverLog(
	request *protobuf.FailoverLogRequest) ap.MessageMarshaller {

	c.Tracef("%v doFailoverLog\n", p.logPrefix)
	response := &protobuf.FailoverLogResponse{}

	pooln := request.GetPool()
	bucketn := request.GetBucket()
	vbuckets := request.GetVbnos()

	bucket, err := c.ConnectBucket(p.clusterAddr, pooln, bucketn)
	if err != nil {
		c.Errorf("%v %s, %v\n", p.logPrefix, bucketn, err)
		response.Err = protobuf.NewError(err)
		return response
	}
	defer bucket.Close()

	protoFlogs := make([]*protobuf.FailoverLog, 0, len(vbuckets))
	vbnos := c.Vbno32to16(vbuckets)
	if flogs, err := bucket.GetFailoverLogs(vbnos); err == nil {
		for vbno, flog := range flogs {
			vbuuids := make([]uint64, 0, len(flog))
			seqnos := make([]uint64, 0, len(flog))
			for _, x := range flog {
				vbuuids = append(vbuuids, x[0])
				seqnos = append(seqnos, x[1])
			}
			protoFlog := &protobuf.FailoverLog{
				Vbno:    proto.Uint32(uint32(vbno)),
				Vbuuids: vbuuids,
				Seqnos:  seqnos,
			}
			protoFlogs = append(protoFlogs, protoFlog)
		}
	} else {
		c.Errorf("%v %s.GetFailoverLogs() %v\n", p.logPrefix, bucketn, err)
		response.Err = protobuf.NewError(err)
		return response
	}
	response.Logs = protoFlogs
	return response
}

// - return ErrorInvalidKVaddrs for malformed vbuuid.
// - return ErrorInconsistentFeed for malformed feed request.
// - return ErrorInvalidVbucketBranch for malformed vbuuid.
// - return dcp-client failures.
// - return ErrorResponseTimeout if request is not completed within timeout.
func (p *Projector) doMutationTopic(
	request *protobuf.MutationTopicRequest) ap.MessageMarshaller {

	c.Tracef("%v doMutationTopic()\n", p.logPrefix)
	topic := request.GetTopic()

	config, _ := c.NewConfig(map[string]interface{}{})
	config.SetValue("maxVbuckets", p.maxvbs)
	config.Set("clusterAddr", p.config["clusterAddr"])
	config.Set("feedWaitStreamReqTimeout", p.config["feedWaitStreamReqTimeout"])
	config.Set("feedWaitStreamEndTimeout", p.config["feedWaitStreamEndTimeout"])
	config.Set("feedChanSize", p.config["feedChanSize"])
	config.Set("mutationChanSize", p.config["mutationChanSize"])
	config.Set("vbucketSyncTimeout", p.config["vbucketSyncTimeout"])
	config.Set("routerEndpointFactory", p.config["routerEndpointFactory"])

	var err error

	feed, _ := p.GetFeed(topic)
	if feed == nil {
		feed, err = NewFeed(topic, config)
		if err != nil {
			return (&protobuf.TopicResponse{}).SetErr(err)
		}
	}
	response, err := feed.MutationTopic(request)
	if err != nil {
		response.SetErr(err)
	}
	p.AddFeed(topic, feed)
	return response
}

// - return ErrorTopicMissing if feed is not started.
// - return ErrorInvalidBucket if bucket is not added.
// - return ErrorInvalidVbucketBranch for malformed vbuuid.
// - return dcp-client failures.
// - return ErrorResponseTimeout if request is not completed within timeout.
func (p *Projector) doRestartVbuckets(
	request *protobuf.RestartVbucketsRequest) ap.MessageMarshaller {

	c.Tracef("%v doRestartVbuckets()\n", p.logPrefix)
	topic := request.GetTopic()

	feed, err := p.GetFeed(topic) // only existing feed
	if err != nil {
		c.Errorf("%v %v\n", p.logPrefix, err)
		response := &protobuf.TopicResponse{}
		if err != projC.ErrorTopicMissing {
			response = feed.GetTopicResponse()
		}
		return response.SetErr(err)
	}

	response, err := feed.RestartVbuckets(request)
	if err == nil {
		return response
	}
	return response.SetErr(err)
}

// - return ErrorTopicMissing if feed is not started.
// - return ErrorInvalidBucket if bucket is not added.
// - return ErrorInvalidVbucketBranch for malformed vbuuid.
// - return dcp-client failures.
// - return ErrorResponseTimeout if request is not completed within timeout.
func (p *Projector) doShutdownVbuckets(
	request *protobuf.ShutdownVbucketsRequest) ap.MessageMarshaller {

	c.Tracef("%v doShutdownVbuckets()\n", p.logPrefix)
	topic := request.GetTopic()

	feed, err := p.GetFeed(topic) // only existing feed
	if err != nil {
		c.Errorf("%v %v\n", p.logPrefix, err)
		return protobuf.NewError(err)
	}

	err = feed.ShutdownVbuckets(request)
	return protobuf.NewError(err)
}

// - return ErrorTopicMissing if feed is not started.
// - return ErrorInconsistentFeed for malformed feed request
// - return ErrorInvalidVbucketBranch for malformed vbuuid.
// - return dcp-client failures.
// - return ErrorResponseTimeout if request is not completed within timeout.
func (p *Projector) doAddBuckets(
	request *protobuf.AddBucketsRequest) ap.MessageMarshaller {

	c.Tracef("%v doAddBuckets()\n", p.logPrefix)
	topic := request.GetTopic()

	feed, err := p.GetFeed(topic) // only existing feed
	if err != nil {
		c.Errorf("%v %v\n", p.logPrefix, err)
		response := &protobuf.TopicResponse{}
		if err != projC.ErrorTopicMissing {
			response = feed.GetTopicResponse()
		}
		return response.SetErr(err)
	}

	response, err := feed.AddBuckets(request)
	if err == nil {
		return response
	}
	return response.SetErr(err)
}

// - return ErrorTopicMissing if feed is not started.
// - return ErrorInvalidBucket if bucket is not added.
// - return ErrorInvalidVbucketBranch for malformed vbuuid.
// - return dcp-client failures.
// - return ErrorResponseTimeout if request is not completed within timeout.
func (p *Projector) doDelBuckets(
	request *protobuf.DelBucketsRequest) ap.MessageMarshaller {

	c.Tracef("%v doDelBuckets()\n", p.logPrefix)
	topic := request.GetTopic()

	feed, err := p.GetFeed(topic) // only existing feed
	if err != nil {
		c.Errorf("%v %v\n", p.logPrefix, err)
		return protobuf.NewError(err)
	}

	err = feed.DelBuckets(request)
	return protobuf.NewError(err)
}

// - return ErrorTopicMissing if feed is not started.
// - return ErrorInconsistentFeed for malformed feed request
// - otherwise, error is empty string.
func (p *Projector) doAddInstances(
	request *protobuf.AddInstancesRequest) ap.MessageMarshaller {

	c.Tracef("%v doAddInstances()\n", p.logPrefix)
	topic := request.GetTopic()

	feed, err := p.GetFeed(topic) // only existing feed
	if err != nil {
		c.Errorf("%v %v\n", p.logPrefix, err)
		return protobuf.NewError(err)
	}

	err = feed.AddInstances(request)
	return protobuf.NewError(err)
}

// - return ErrorTopicMissing if feed is not started.
// - otherwise, error is empty string.
func (p *Projector) doDelInstances(
	request *protobuf.DelInstancesRequest) ap.MessageMarshaller {

	c.Tracef("%v doDelInstances()\n", p.logPrefix)
	topic := request.GetTopic()

	feed, err := p.GetFeed(topic) // only existing feed
	if err != nil {
		c.Errorf("%v %v\n", p.logPrefix, err)
		return protobuf.NewError(err)
	}

	err = feed.DelInstances(request)
	return protobuf.NewError(err)
}

// - return ErrorTopicMissing if feed is not started.
// - otherwise, error is empty string.
func (p *Projector) doRepairEndpoints(
	request *protobuf.RepairEndpointsRequest) ap.MessageMarshaller {

	c.Tracef("%v doRepairEndpoints()\n", p.logPrefix)
	topic := request.GetTopic()

	feed, err := p.GetFeed(topic) // only existing feed
	if err != nil {
		c.Errorf("%v %v\n", p.logPrefix, err)
		return protobuf.NewError(err)
	}

	err = feed.RepairEndpoints(request)
	return protobuf.NewError(err)
}

// - return ErrorTopicMissing if feed is not started.
// - otherwise, error is empty string.
func (p *Projector) doShutdownTopic(
	request *protobuf.ShutdownTopicRequest) ap.MessageMarshaller {

	c.Tracef("%v doShutdownTopic()\n", p.logPrefix)
	topic := request.GetTopic()

	feed, err := p.GetFeed(topic) // only existing feed
	if err != nil {
		c.Errorf("%v %v\n", p.logPrefix, err)
		return protobuf.NewError(err)
	}

	p.DelFeed(topic)
	feed.Shutdown()
	return protobuf.NewError(err)
}

func (p *Projector) doStatistics(request c.Statistics) ap.MessageMarshaller {
	c.Tracef("%v doStatistics()\n", p.logPrefix)

	m := map[string]interface{}{
		"clusterAddr": p.clusterAddr,
		"adminport":   p.adminport,
		"topics":      p.listTopics(),
	}
	stats, _ := c.NewStatistics(m)

	feeds, _ := c.NewStatistics(nil)
	for topic, feed := range p.topics {
		feeds.Set(topic, feed.GetStatistics())
	}
	stats.Set("feeds", feeds)
	stats.Set("adminport", p.admind.GetStatistics())
	return stats
}

// return list of active topics
func (p *Projector) listTopics() []string {
	topics := make([]string, 0, len(p.topics))
	for topic := range p.topics {
		topics = append(topics, topic)
	}
	return topics
}
