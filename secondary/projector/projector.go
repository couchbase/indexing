package projector

import "fmt"
import "sync"
import "io"
import "net/http"
import "strings"
import "encoding/json"

import ap "github.com/couchbase/indexing/secondary/adminport"
import c "github.com/couchbase/indexing/secondary/common"
import projC "github.com/couchbase/indexing/secondary/projector/client"
import protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
import "github.com/couchbaselabs/goprotobuf/proto"
import "github.com/couchbase/indexing/secondary/logging"

// Projector data structure, a projector is connected to
// one or more upstream kv-nodes. Works in tandem with
// projector's adminport.
type Projector struct {
	admind ap.Server // admin-port server
	// lock protected fields.
	mu     sync.RWMutex
	topics map[string]*Feed // active topics
	config c.Config         // full configuration information.
	// config params
	name        string // human readable name of the projector
	clusterAddr string // kv cluster's address to connect
	adminport   string // projector listens on this adminport
	maxvbs      int
	logPrefix   string
}

// NewProjector creates a news projector instance and
// starts a corresponding adminport.
func NewProjector(maxvbs int, config c.Config) *Projector {
	p := &Projector{
		topics: make(map[string]*Feed),
		maxvbs: maxvbs,
		config: config,
	}

	// Setup dynamic configuration propagation
	config, err := c.GetSettingsConfig(config)
	c.CrashOnError(err)

	p.SetConfig(config)
	callb := func(cfg c.Config) {
		p.SetConfig(cfg)
	}
	c.SetupSettingsNotifier(callb, make(chan struct{}))

	cluster := p.clusterAddr
	if !strings.HasPrefix(p.clusterAddr, "http://") {
		cluster = "http://" + cluster
	}
	p.logPrefix = fmt.Sprintf("PROJ[%s]", p.adminport)

	apConfig := p.config.SectionConfig("projector.adminport.", true)
	apConfig.SetValue("name", "PRAM")
	reqch := make(chan ap.Request)
	p.admind = ap.NewHTTPServer(apConfig, reqch)

	go p.mainAdminPort(reqch)
	go p.watcherDameon(p.config["projector.watchInterval"].Int())
	logging.Infof("%v started ...\n", p.logPrefix)
	return p
}

// GetConfig returns the config object from projector.
func (p *Projector) GetConfig() c.Config {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.config
}

// SetConfig accepts a full-set or subset of global configuration
// and updates projector related fields.
func (p *Projector) SetConfig(config c.Config) {
	p.mu.Lock()
	defer p.mu.Unlock()

	ef := p.config["projector.routerEndpointFactory"]
	p.config = p.config.Override(config)

	pconf := p.config.SectionConfig("projector.", true /*trim*/)
	p.name = pconf["name"].String()
	p.clusterAddr = pconf["clusterAddr"].String()
	p.adminport = pconf["adminport.listenAddr"].String()
	p.config["projector.routerEndpointFactory"] = ef // IMPORTANT: skip override

	// update loglevel
	level := p.config["projector.settings.log_level"].String()
	logging.SetLogLevel(logging.Level(level))
	override := p.config["projector.settings.log_override"].String()
	logging.AddOverride(override)
}

// GetFeedConfig from current configuration settings.
func (p *Projector) GetFeedConfig(topic string) c.Config {
	p.mu.Lock()
	defer p.mu.Unlock()

	config, _ := c.NewConfig(map[string]interface{}{})
	pconf := p.config.SectionConfig("projector.", true /*trim*/)
	config.SetValue("maxVbuckets", p.maxvbs)
	config.Set("clusterAddr", pconf["clusterAddr"])
	config.Set("feedWaitStreamReqTimeout", pconf["feedWaitStreamReqTimeout"])
	config.Set("feedWaitStreamEndTimeout", pconf["feedWaitStreamEndTimeout"])
	config.Set("feedChanSize", pconf["feedChanSize"])
	config.Set("mutationChanSize", pconf["mutationChanSize"])
	config.Set("vbucketSyncTimeout", pconf["vbucketSyncTimeout"])
	config.Set("routerEndpointFactory", pconf["routerEndpointFactory"])
	return config
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

// GetFeeds return a list of all feeds.
func (p *Projector) GetFeeds() []*Feed {
	p.mu.RLock()
	defer p.mu.RUnlock()

	feeds := make([]*Feed, 0)
	for _, feed := range p.topics {
		feeds = append(feeds, feed)
	}
	return feeds
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
	logging.Infof("%v %q feed added ...\n", p.logPrefix, topic)
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
	logging.Infof("%v ... %q feed deleted\n", p.logPrefix, topic)
	return
}

//---- handler for admin-port request

// - return couchbase SDK error if any.
func (p *Projector) doVbmapRequest(
	request *protobuf.VbmapRequest) ap.MessageMarshaller {

	logging.Tracef("%v doVbmapRequest\n", p.logPrefix)
	response := &protobuf.VbmapResponse{}

	pooln := request.GetPool()
	bucketn := request.GetBucket()
	kvaddrs := request.GetKvaddrs()

	// get vbmap from bucket connection.
	bucket, err := c.ConnectBucket(p.clusterAddr, pooln, bucketn)
	if err != nil {
		logging.Errorf("%v for bucket %q, %v\n", p.logPrefix, bucketn, err)
		response.Err = protobuf.NewError(err)
		return response
	}
	defer bucket.Close()

	bucket.Refresh()
	m, err := bucket.GetVBmap(kvaddrs)
	if err != nil {
		logging.Errorf("%v for bucket %q, %v\n", p.logPrefix, bucketn, err)
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

	logging.Tracef("%v doFailoverLog\n", p.logPrefix)
	response := &protobuf.FailoverLogResponse{}

	pooln := request.GetPool()
	bucketn := request.GetBucket()
	vbuckets := request.GetVbnos()

	bucket, err := c.ConnectBucket(p.clusterAddr, pooln, bucketn)
	if err != nil {
		logging.Errorf("%v %s, %v\n", p.logPrefix, bucketn, err)
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
		logging.Errorf("%v %s.GetFailoverLogs() %v\n", p.logPrefix, bucketn, err)
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

	logging.Tracef("%v doMutationTopic()\n", p.logPrefix)
	topic := request.GetTopic()

	var err error
	feed, _ := p.GetFeed(topic)
	if feed == nil {
		config := p.GetFeedConfig(topic)
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

	logging.Tracef("%v doRestartVbuckets()\n", p.logPrefix)
	topic := request.GetTopic()

	feed, err := p.GetFeed(topic) // only existing feed
	if err != nil {
		logging.Errorf("%v %v\n", p.logPrefix, err)
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

	logging.Tracef("%v doShutdownVbuckets()\n", p.logPrefix)
	topic := request.GetTopic()

	feed, err := p.GetFeed(topic) // only existing feed
	if err != nil {
		logging.Errorf("%v %v\n", p.logPrefix, err)
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

	logging.Tracef("%v doAddBuckets()\n", p.logPrefix)
	topic := request.GetTopic()

	feed, err := p.GetFeed(topic) // only existing feed
	if err != nil {
		logging.Errorf("%v %v\n", p.logPrefix, err)
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

	logging.Tracef("%v doDelBuckets()\n", p.logPrefix)
	topic := request.GetTopic()

	feed, err := p.GetFeed(topic) // only existing feed
	if err != nil {
		logging.Errorf("%v %v\n", p.logPrefix, err)
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

	logging.Tracef("%v doAddInstances()\n", p.logPrefix)
	topic := request.GetTopic()

	feed, err := p.GetFeed(topic) // only existing feed
	if err != nil {
		logging.Errorf("%v %v\n", p.logPrefix, err)
		return protobuf.NewError(err)
	}

	err = feed.AddInstances(request)
	return protobuf.NewError(err)
}

// - return ErrorTopicMissing if feed is not started.
// - otherwise, error is empty string.
func (p *Projector) doDelInstances(
	request *protobuf.DelInstancesRequest) ap.MessageMarshaller {

	logging.Tracef("%v doDelInstances()\n", p.logPrefix)
	topic := request.GetTopic()

	feed, err := p.GetFeed(topic) // only existing feed
	if err != nil {
		logging.Errorf("%v %v\n", p.logPrefix, err)
		return protobuf.NewError(err)
	}

	err = feed.DelInstances(request)
	return protobuf.NewError(err)
}

// - return ErrorTopicMissing if feed is not started.
// - otherwise, error is empty string.
func (p *Projector) doRepairEndpoints(
	request *protobuf.RepairEndpointsRequest) ap.MessageMarshaller {

	logging.Tracef("%v doRepairEndpoints()\n", p.logPrefix)
	topic := request.GetTopic()

	feed, err := p.GetFeed(topic) // only existing feed
	if err != nil {
		logging.Errorf("%v %v\n", p.logPrefix, err)
		return protobuf.NewError(err)
	}

	err = feed.RepairEndpoints(request)
	return protobuf.NewError(err)
}

// - return ErrorTopicMissing if feed is not started.
// - otherwise, error is empty string.
func (p *Projector) doShutdownTopic(
	request *protobuf.ShutdownTopicRequest) ap.MessageMarshaller {

	logging.Tracef("%v doShutdownTopic()\n", p.logPrefix)
	topic := request.GetTopic()

	feed, err := p.GetFeed(topic) // only existing feed
	if err != nil {
		logging.Errorf("%v topic %v: %v\n", p.logPrefix, topic, err)
		return protobuf.NewError(err)
	}

	p.DelFeed(topic)
	feed.Shutdown()
	return protobuf.NewError(err)
}

func (p *Projector) doStatistics() interface{} {
	logging.Tracef("%v doStatistics()\n", p.logPrefix)

	m := map[string]interface{}{
		"clusterAddr": p.clusterAddr,
		"adminport":   p.adminport,
	}
	stats, _ := c.NewStatistics(m)

	feeds, _ := c.NewStatistics(nil)
	for topic, feed := range p.topics {
		feeds.Set(topic, feed.GetStatistics())
	}
	stats.Set("feeds", feeds)
	return map[string]interface{}(stats)
}

//--------------
// http handlers
//--------------

// handle projector statistics
func (p *Projector) handleStats(w http.ResponseWriter, r *http.Request) {
	logging.Infof("%s Request %q\n", p.logPrefix, r.URL.Path)

	contentType := r.Header.Get("Content-Type")
	isJSON := strings.Contains(contentType, "application/json")

	stats := p.doStatistics().(map[string]interface{})
	if isJSON {
		data, err := json.Marshal(stats)
		if err != nil {
			logging.Errorf("%v encoding statistics: %v\n", p.logPrefix, err)
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		fmt.Fprintf(w, "%s", string(data))
		return
	}
	fmt.Fprintf(w, "%s", c.Statistics(stats).Lines())
}

// handle settings
func (p *Projector) handleSettings(w http.ResponseWriter, r *http.Request) {
	logging.Infof("%s Request %q %q\n", p.logPrefix, r.Method, r.URL.Path)
	switch r.Method {
	case "GET":
		header := w.Header()
		header["Content-Type"] = []string{"application/json"}
		fmt.Fprintf(w, "%s", string(p.GetConfig().Json()))

	case "POST":
		dataIn := make([]byte, r.ContentLength)
		// read settings
		if err := requestRead(r.Body, dataIn); err != nil {
			logging.Errorf("%v handleSettings() POST: %v\n", p.logPrefix, err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// parse settings
		newConfig := make(map[string]interface{})
		if err := json.Unmarshal(dataIn, &newConfig); err != nil {
			logging.Errorf("%v handleSettings() json decoding: %v\n", p.logPrefix, err)
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		// update projector settings
		logging.Infof("%v updating projector config ...\n", p.logPrefix)
		config, _ := c.NewConfig(newConfig)
		p.SetConfig(config)
		// update feed settings
		for _, feed := range p.GetFeeds() {
			feed.SetConfig(p.GetConfig())
		}

	default:
		http.Error(w, "only GET POST supported", http.StatusMethodNotAllowed)
	}
}

// return list of active topics
func (p *Projector) listTopics() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	topics := make([]string, 0, len(p.topics))
	for topic := range p.topics {
		topics = append(topics, topic)
	}
	return topics
}

func requestRead(r io.Reader, data []byte) (err error) {
	var c int

	n, start := len(data), 0
	for n > 0 && err == nil {
		// Per http://golang.org/pkg/io/#Reader, it is valid for Read to
		// return EOF with non-zero number of bytes at the end of the
		// input stream
		c, err = r.Read(data[start:])
		n -= c
		start += c
	}
	if n == 0 {
		return nil
	}
	return err
}
