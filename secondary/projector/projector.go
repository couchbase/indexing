package projector

import (
	"encoding/json"
	"expvar"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	apcommon "github.com/couchbase/indexing/secondary/adminport/common"
	apserver "github.com/couchbase/indexing/secondary/adminport/server"
	"github.com/couchbase/indexing/secondary/audit"
	"github.com/couchbase/indexing/secondary/common"
	c "github.com/couchbase/indexing/secondary/common"
	mc "github.com/couchbase/indexing/secondary/dcp/transport/client"
	"github.com/couchbase/indexing/secondary/logging"
	projC "github.com/couchbase/indexing/secondary/projector/client"
	"github.com/couchbase/indexing/secondary/projector/memThrottler"
	"github.com/couchbase/indexing/secondary/projector/memmanager"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
	"github.com/couchbase/indexing/secondary/security"
	"github.com/golang/protobuf/proto"
)

// UUID of the node on which the projector process executes
var nodeUUID string

const MAX_CINFO_CACHES_RETRIES = 100

// Projector data structure, a projector is connected to
// one or more upstream kv-nodes. Works in tandem with
// projector's adminport.
type Projector struct {
	admind apcommon.Server // admin-port server
	// lock protected fields.
	rw             sync.RWMutex
	topics         map[string]*Feed // active topics
	topicSerialize map[string]*sync.Mutex
	config         c.Config // full configuration information.
	// immutable config params
	name        string // human readable name of the projector
	clusterAddr string // kv cluster's address to connect
	pooln       string // kv pool-name
	adminport   string // projector listens on this adminport
	maxvbs      int
	cpuProfFd   *os.File
	logPrefix   string

	cinfoClient *c.ClusterInfoClient

	certFile             string
	keyFile              string
	reqch                chan apcommon.Request
	enableSecurityChange chan bool
	//Statistics
	stats       *ProjectorStats
	statsMgr    *statsManager
	statsCmdCh  chan []interface{}
	statsStopCh chan bool
	statsMutex  sync.RWMutex
}

// NewProjector creates a news projector instance and
// starts a corresponding adminport.
func NewProjector(maxvbs int, config c.Config, certFile string, keyFile string) *Projector {
	p := &Projector{
		topics:               make(map[string]*Feed),
		topicSerialize:       make(map[string]*sync.Mutex),
		maxvbs:               maxvbs,
		pooln:                "default", // TODO: should this be configurable ?
		certFile:             certFile,
		keyFile:              keyFile,
		enableSecurityChange: make(chan bool),
		statsCmdCh:           make(chan []interface{}, 1),
		statsStopCh:          make(chan bool, 1),
	}

	// Setup dynamic configuration propagation
	config, err := c.GetSettingsConfig(config)
	c.CrashOnError(err)

	pconfig := config.SectionConfig("projector.", true /*trim*/)
	p.name = pconfig["name"].String()
	p.clusterAddr = pconfig["clusterAddr"].String()
	p.adminport = pconfig["adminport.listenAddr"].String()
	ef := config["projector.routerEndpointFactory"]
	config["projector.routerEndpointFactory"] = ef

	go common.WatchClusterVersionChanges(p.clusterAddr, int64(common.INDEXER_71_VERSION))

	// Start cluster info client
	cic, err := c.NewClusterInfoClient(p.clusterAddr, "default", config)
	c.CrashOnError(err)
	p.cinfoClient = cic
	p.cinfoClient.SetUserAgent("projector")

	cinfo := cic.GetClusterInfoCache()
	cinfo.SetRetryInterval(4 * time.Second)

	systemStatsCollectionInterval := int64(config["projector.systemStatsCollectionInterval"].Int())
	memmanager.Init(systemStatsCollectionInterval) // Initialize memory manager

	p.stats = NewProjectorStats()
	p.statsMgr = NewStatsManager(p.statsCmdCh, p.statsStopCh, config)
	p.UpdateStatsMgr(p.stats.Clone())

	p.config = config
	p.ResetConfig(config)

	// Initialize nodeUUID
	if nodeUUID, err = p.getNodeUUID(); err != nil {
		c.CrashOnError(fmt.Errorf("Failed to get node UUID from ClusterInfoCache: %v", err))
	}

	p.logPrefix = fmt.Sprintf("PROJ[%s]", p.adminport)

	encryptLocalHost := config["security.encryption.encryptLocalhost"].Bool()
	if err := p.initSecurityContext(encryptLocalHost); err != nil {
		c.CrashOnError(fmt.Errorf("Fail to initialize security context: %v", err))
	}

	cluster := p.clusterAddr
	if !strings.HasPrefix(p.clusterAddr, "http://") {
		cluster = "http://" + cluster
	}

	p.setupHTTP()
	expvar.Publish("projector", expvar.Func(p.doStatistics))

	// projector is now ready to take security change
	close(p.enableSecurityChange)

	// set GOGC percent
	gogc := pconfig["gogc"].Int()
	oldGogc := debug.SetGCPercent(gogc)
	fmsg := "%v changing GOGC percentage from %v to %v\n"
	logging.Infof(fmsg, p.logPrefix, oldGogc, gogc)

	watchInterval := config["projector.watchInterval"].Int()
	staleTimeout := config["projector.staleTimeout"].Int()
	go c.MemstatLogger(int64(config["projector.memstatTick"].Int()))
	go p.watcherDameon(watchInterval, staleTimeout)

	callb := func(cfg c.Config) {
		logging.Infof("%v settings notifier from metakv\n", p.logPrefix)
		cfg.LogConfig(p.logPrefix)
		p.ResetConfig(cfg)
		p.ResetFeedConfig()
	}
	c.SetupSettingsNotifier(callb, make(chan struct{}))

	logging.Infof("%v started ...\n", p.logPrefix)
	return p
}

func (p *Projector) setupHTTP() {
	fn := func(r int, e error) (err error) {
		apConfig := p.config.SectionConfig("projector.adminport.", true)
		apConfig.SetValue("name", "PRAM")

		p.reqch = make(chan apcommon.Request)
		p.admind, err = apserver.NewHTTPServer(apConfig, p.reqch)
		return err
	}
	helper := c.NewRetryHelper(10, time.Second, 1, fn)
	if err := helper.Run(); err != nil {
		c.CrashOnError(fmt.Errorf("Fail to restart https server on security change. Error %v", err))
	}

	go p.mainAdminPort(p.reqch)
}

// GetConfig returns the config object from projector.
func (p *Projector) GetConfig() c.Config {
	p.rw.Lock()
	defer p.rw.Unlock()
	return p.config.Clone()
}

// ResetConfig accepts a full-set or subset of global configuration
// and updates projector related fields.
func (p *Projector) ResetConfig(config c.Config) {
	p.rw.Lock()
	defer p.rw.Unlock()

	// reset configuration.
	if cv, ok := config["projector.settings.log_level"]; ok {
		logging.SetLogLevel(logging.Level(cv.String()))
	}
	if cv, ok := config["projector.maxCpuPercent"]; ok {
		logging.Infof("Projector CPU set at %v", cv.Int())
		c.SetNumCPUs(cv.Int())
	}
	if cv, ok := config["projector.gogc"]; ok {
		gogc := cv.Int()
		oldGogc := debug.SetGCPercent(gogc)
		memmanager.SetDefaultGCPercent(gogc)
		fmsg := "%v changing GOGC percentage from %v to %v\n"
		logging.Infof(fmsg, p.logPrefix, oldGogc, gogc)
	}
	if cv, ok := config["projector.memstatTick"]; ok {
		c.Memstatch <- int64(cv.Int())
	}
	if cv, ok := config["projector.statsLogDumpInterval"]; ok {
		value := cv.Int()
		p.statsCmdCh <- []interface{}{STATS_LOG_INTERVAL_UPDATE, value}
	}
	if cv, ok := config["projector.vbseqnosLogInterval"]; ok {
		value := cv.Int()
		p.statsCmdCh <- []interface{}{VBSEQNOS_LOG_INTERVAL_UPDATE, value}
	}
	if cv, ok := config["projector.evalStatLoggingThreshold"]; ok {
		value := cv.Int()
		p.statsCmdCh <- []interface{}{EVAL_STAT_LOGGING_THRESHOLD, value}
	}

	if cv, ok := config["projector.systemStatsCollectionInterval"]; ok {
		memmanager.SetStatsCollectionInterval(int64(cv.Int()))
	}

	if cv, ok := config["projector.usedMemThreshold"]; ok {
		memmanager.SetUsedMemThreshold(cv.Float64())
	}

	if cv, ok := config["projector.rssThreshold"]; ok {
		memmanager.SetRSSThreshold(cv.Float64())
	}

	if cv, ok := config["projector.forceGCOnThreshold"]; ok {
		memmanager.SetForceGCOnThreshold(cv.Bool())
	}

	if cv, ok := config["projector.relaxGCThreshold"]; ok {
		memmanager.SetRelaxGCThreshold(cv.Float64())
	}

	if cv, ok := config["projector.memThrottle"]; ok {
		memThrottler.SetMemThrottle(cv.Bool())
	}

	if cv, ok := config["projector.memThrottle.init_build.start_level"]; ok {
		memThrottler.SetInitBuildThrottleStartLevel(cv.Int())
	}

	if cv, ok := config["projector.memThrottle.incr_build.start_level"]; ok {
		memThrottler.SetIncrBuildThrottleStartLevel(cv.Int())
	}

	if cv, ok := config["projector.maintStreamMemThrottle"]; ok {
		memThrottler.SetMaintStreamMemThrottle(cv.Bool())
	}

	p.config = p.config.Override(config)

	// CPU-profiling
	cpuProfile, ok := config["projector.cpuProfile"]
	if ok && cpuProfile.Bool() && p.cpuProfFd == nil {
		cpuProfDir, ok := config["projector.cpuProfDir"]
		fname := "projector_cpu.pprof"
		if ok && cpuProfDir.String() != "" {
			fname = filepath.Join(cpuProfDir.String(), fname)
		}
		logging.Infof("%v cpu profiling => %q\n", p.logPrefix, fname)
		p.cpuProfFd = p.startCPUProfile(fname)

	} else if ok && !cpuProfile.Bool() {
		if p.cpuProfFd != nil {
			pprof.StopCPUProfile()
			logging.Infof("%v cpu profiling stopped\n", p.logPrefix)
		}
		p.cpuProfFd = nil

	} else if ok {
		logging.Warnf("%v cpu profiling already active !!\n", p.logPrefix)
	}

	// MEM-profiling
	memProfile, ok := config["projector.memProfile"]
	if ok && memProfile.Bool() {
		memProfDir, ok := config["projector.memProfDir"]
		fname := "projector_mem.pprof"
		if ok && memProfDir.String() != "" {
			fname = filepath.Join(memProfDir.String(), fname)
		}
		if p.takeMEMProfile(fname) {
			logging.Infof("%v mem profile => %q\n", p.logPrefix, fname)
		}
	}

	if cv, ok := config["projector.memcachedTimeout"]; ok {
		mc.SetDcpMemcachedTimeout(uint32(cv.Int()))
		logging.Infof("%v memcachedTimeout set to %v\n", p.logPrefix, uint32(cv.Int()))
	}

	logging.Infof("%v\n", c.LogRuntime())
}

// ResetConfig accepts a full-set or subset of global configuration
// and updates projector related fields.
func (p *Projector) ResetFeedConfig() {
	config := p.GetFeedConfig()

	// update feed settings
	for _, feed := range p.GetFeeds() {
		if err := feed.ResetConfig(config); err != nil {
			fmsg := "%v feed(`%v`).ResetConfig: %v"
			logging.Errorf(fmsg, p.logPrefix, feed.topic, err)
		}
	}
}

// GetFeedConfig from current configuration settings.
func (p *Projector) GetFeedConfig() c.Config {
	p.rw.Lock()
	defer p.rw.Unlock()

	config, _ := c.NewConfig(map[string]interface{}{})
	config["clusterAddr"] = p.config["clusterAddr"] // copy by value.
	config["maxVbuckets"] = p.config["maxVbuckets"]
	pconfig := p.config.SectionConfig("projector.", true /*trim*/)
	for _, key := range FeedConfigParams() {
		config.Set(key, pconfig[key])
	}

	return config
}

// GetFeed object for `topic`.
// - return ErrorTopicMissing if topic is not started.
func (p *Projector) GetFeed(topic string) (*Feed, error) {
	getfeed := func() (*Feed, bool) {
		p.rw.RLock()
		defer p.rw.RUnlock()
		feed, ok := p.topics[topic]
		return feed, ok
	}

	if feed, ok := getfeed(); ok {
		if err := feed.Ping(); err != nil {
			return nil, err
		}
		return feed, nil
	}
	return nil, projC.ErrorTopicMissing
}

// GetFeeds return a list of all feeds.
func (p *Projector) GetFeeds() []*Feed {
	p.rw.RLock()
	defer p.rw.RUnlock()

	feeds := make([]*Feed, 0)
	for _, feed := range p.topics {
		feeds = append(feeds, feed)
	}
	return feeds
}

// AddFeed object for `topic`.
// - return ErrorTopicExist if topic is duplicate.
func (p *Projector) AddFeed(topic string, feed *Feed) (err error) {
	p.rw.Lock()
	defer p.rw.Unlock()

	if _, ok := p.topics[topic]; ok {
		return projC.ErrorTopicExist
	}
	p.topics[topic] = feed
	opaque := feed.GetOpaque()
	logging.Infof("%v ##%x feed %q added ...\n", p.logPrefix, opaque, topic)
	return
}

// DelFeed object for `topic`.
// - return ErrorTopicMissing if topic is not started.
func (p *Projector) DelFeed(topic string) (err error) {
	p.rw.Lock()
	defer p.rw.Unlock()

	feed, ok := p.topics[topic]
	if ok == false {
		return projC.ErrorTopicMissing
	}
	delete(p.topics, topic)
	opaque := feed.GetOpaque()
	logging.Infof("%v ##%x ... feed %q deleted\n", p.logPrefix, opaque, topic)

	go func() { // GC
		now := time.Now()
		runtime.GC()
		fmsg := "%v ##%x GC() took %v\n"
		logging.Infof(fmsg, p.logPrefix, opaque, time.Since(now))
	}()
	return
}

func (p *Projector) UpdateStats(topic string, feed *Feed) {
	feedStats := feed.GetStats()

	p.statsMutex.Lock()
	if feedStats != nil {
		p.stats.feedStats[topic] = feedStats
	} else {
		// Feed is closed
		delete(p.stats.feedStats, topic)
	}

	clonedStats := p.stats.Clone()
	p.statsMutex.Unlock()

	p.UpdateStatsMgr(clonedStats)

}

func (p *Projector) UpdateStatsMgr(clone *ProjectorStats) {
	msg := []interface{}{UPDATE_STATS_MAP, clone}
	p.statsCmdCh <- msg
}

//---- handler for admin-port request

// - return couchbase SDK error if any.
func (p *Projector) doVbmapRequest(
	request *protobuf.VbmapRequest, opaque uint16) apcommon.MessageMarshaller {

	response := &protobuf.VbmapResponse{}

	pooln := request.GetPool()
	bucketn := request.GetBucket()
	kvaddrs := request.GetKvaddrs()

	// log this request.
	prefix := p.logPrefix
	fmsg := "%v ##%x doVbmapRequest() {%q, %q, %v}\n"
	logging.Infof(fmsg, prefix, pooln, bucketn, kvaddrs, opaque)
	defer logging.Infof("%v ##%x doVbmapRequest() returns ...\n", prefix, opaque)

	// get vbmap from bucket connection.
	bucket, err := c.ConnectBucket(p.clusterAddr, pooln, bucketn)
	if err != nil {
		logging.Errorf("%v ##%x ConnectBucket(): %v\n", prefix, opaque, err)
		response.Err = protobuf.NewError(err)
		return response
	}
	defer bucket.Close()

	err = bucket.Refresh()
	if err != nil {
		logging.Errorf("%v ##%x doVbMapRequest error during bucket.Refresh(), bucket: %v, err:%v\n", prefix, opaque, bucket.Name, err)
		response.Err = protobuf.NewError(err)
		return response
	}

	m, err := bucket.GetVBmap(kvaddrs)
	if err != nil {
		logging.Errorf("%v ##%x GetVBmap(): %v\n", prefix, opaque, err)
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
	request *protobuf.FailoverLogRequest, opaque uint16) apcommon.MessageMarshaller {

	response := &protobuf.FailoverLogResponse{}

	pooln := request.GetPool()
	bucketn := request.GetBucket()
	vbuckets := request.GetVbnos()

	// log this request.
	prefix := p.logPrefix
	fmsg := "%v ##%x doFailoverLog() {%q, %q, %v}\n"
	logging.Infof(fmsg, prefix, opaque, pooln, bucketn, vbuckets)
	defer logging.Infof("%v ##%x doFailoverLog() returns ...\n", prefix, opaque)

	bucket, err := c.ConnectBucket(p.clusterAddr, pooln, bucketn)
	if err != nil {
		logging.Errorf("%v ##%x ConnectBucket(): %v\n", prefix, opaque, err)
		response.Err = protobuf.NewError(err)
		return response
	}
	defer bucket.Close()

	config := p.GetConfig()
	protoFlogs := make([]*protobuf.FailoverLog, 0, len(vbuckets))
	vbnos := c.Vbno32to16(vbuckets)
	dcpConfig := map[string]interface{}{
		"genChanSize":    config["projector.dcp.genChanSize"].Int(),
		"dataChanSize":   config["projector.dcp.dataChanSize"].Int(),
		"numConnections": config["projector.dcp.numConnections"].Int(),
	}

	uuid := common.GetUUID(prefix, opaque)
	flogs, err := bucket.GetFailoverLogs(opaque, vbnos, uuid, dcpConfig)
	if err == nil {
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
		logging.Errorf("%v ##%x GetFailoverLogs(): %v\n", prefix, opaque, err)
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
	request *protobuf.MutationTopicRequest,
	opaque uint16) apcommon.MessageMarshaller {

	topic := request.GetTopic()

	// log this request.
	prefix := p.logPrefix
	logging.Infof("%v ##%x doMutationTopic() %q\n", prefix, opaque, topic)
	defer logging.Infof("%v ##%x doMutationTopic() returns ...\n", prefix, opaque)

	var err error
	feed, _ := p.acquireFeed(topic)
	defer p.releaseFeed(topic)
	if feed == nil {
		config := p.GetFeedConfig()
		feed, err = NewFeed(p.pooln, topic, p, config, opaque, request.GetAsync())
		if err != nil {
			fmsg := "%v ##%x unable to create feed %v\n"
			logging.Errorf(fmsg, prefix, opaque, topic)
			return (&protobuf.TopicResponse{}).SetErr(err)
		}
	}
	response, err := feed.MutationTopic(request, opaque)
	if err != nil {
		response.SetErr(err)
	}
	p.UpdateStats(topic, feed)
	p.AddFeed(topic, feed)
	return response
}

// - return ErrorTopicMissing if feed is not started.
// - return ErrorInvalidBucket if bucket is not added.
// - return ErrorInvalidVbucketBranch for malformed vbuuid.
// - return dcp-client failures.
// - return ErrorResponseTimeout if request is not completed within timeout.
func (p *Projector) doRestartVbuckets(
	request *protobuf.RestartVbucketsRequest,
	opaque uint16) apcommon.MessageMarshaller {

	topic := request.GetTopic()

	// log this request.
	prefix := p.logPrefix
	logging.Infof("%v ##%x doRestartVbuckets() %q\n", prefix, opaque, topic)
	defer logging.Infof("%v ##%x doRestartVbuckets() returns ...\n", prefix, opaque)

	feed, err := p.acquireFeed(topic)
	defer p.releaseFeed(topic)
	if err != nil {
		logging.Errorf("%v ##%x acquireFeed(): %v\n", prefix, opaque, err)
		response := &protobuf.TopicResponse{}
		if err != projC.ErrorTopicMissing {
			response = feed.GetTopicResponse()
		}
		return response.SetErr(err)
	}

	response, err := feed.RestartVbuckets(request, opaque)
	if err == nil {
		p.UpdateStats(topic, feed)
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
	request *protobuf.ShutdownVbucketsRequest,
	opaque uint16) apcommon.MessageMarshaller {

	topic := request.GetTopic()

	// log this request.
	prefix := p.logPrefix
	logging.Infof("%v ##%x doShutdownVbuckets() %q\n", prefix, opaque, topic)
	defer logging.Infof("%v ##%x doShutdownVbuckets() returns ...\n", prefix, opaque)

	feed, err := p.acquireFeed(topic)
	defer p.releaseFeed(topic)
	if err != nil {
		logging.Errorf("%v ##%x acquireFeed(): %v\n", prefix, opaque, err)
		return protobuf.NewError(err)
	}

	err = feed.ShutdownVbuckets(request, opaque)
	p.UpdateStats(topic, feed)
	return protobuf.NewError(err)
}

// - return ErrorTopicMissing if feed is not started.
// - return ErrorInconsistentFeed for malformed feed request
// - return ErrorInvalidVbucketBranch for malformed vbuuid.
// - return dcp-client failures.
// - return ErrorResponseTimeout if request is not completed within timeout.
func (p *Projector) doAddBuckets(
	request *protobuf.AddBucketsRequest, opaque uint16) apcommon.MessageMarshaller {

	topic := request.GetTopic()

	// log this request.
	prefix := p.logPrefix
	logging.Infof("%v ##%x doAddBuckets() %q\n", prefix, opaque, topic)
	defer logging.Infof("%v ##%x doAddBuckets() returns ...\n", prefix, opaque)

	feed, err := p.acquireFeed(topic)
	defer p.releaseFeed(topic)
	if err != nil {
		logging.Errorf("%v ##%x acquireFeed(): %v\n", prefix, opaque, err)
		response := &protobuf.TopicResponse{}
		if err != projC.ErrorTopicMissing {
			response = feed.GetTopicResponse()
		}
		return response.SetErr(err)
	}

	response, err := feed.AddBuckets(request, opaque)
	if err == nil {
		p.UpdateStats(topic, feed)
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
	request *protobuf.DelBucketsRequest, opaque uint16) apcommon.MessageMarshaller {

	topic := request.GetTopic()

	// log this request.
	prefix := p.logPrefix
	logging.Infof("%v ##%x doDelBuckets() %q\n", prefix, opaque, topic)
	defer logging.Infof("%v ##%x doDelBuckets() returns ...\n", prefix, opaque)

	feed, err := p.acquireFeed(topic)
	defer p.releaseFeed(topic)
	if err != nil {
		logging.Errorf("%v ##%x acquireFeed(): %v\n", prefix, opaque, err)
		return protobuf.NewError(err)
	}

	err = feed.DelBuckets(request, opaque)
	p.UpdateStats(topic, feed)
	return protobuf.NewError(err)
}

// - return ErrorTopicMissing if feed is not started.
// - return ErrorInconsistentFeed for malformed feed request
// - otherwise, error is empty string.
func (p *Projector) doAddInstances(
	request *protobuf.AddInstancesRequest, opaque uint16) apcommon.MessageMarshaller {

	topic := request.GetTopic()

	// log this request.
	prefix := p.logPrefix
	logging.Infof("%v ##%x doAddInstances() %q\n", prefix, opaque, topic)
	defer logging.Infof("%v ##%x doAddInstances() returns ...\n", prefix, opaque)

	feed, err := p.acquireFeed(topic)
	defer p.releaseFeed(topic)
	if err != nil {
		logging.Errorf("%v ##%x acquireFeed(): %v\n", prefix, opaque, err)
		return protobuf.NewError(err)
	}

	response, err := feed.AddInstances(request, opaque)
	if err != nil {
		response.SetErr(err)
	}
	p.UpdateStats(topic, feed)
	return response
}

// - return ErrorTopicMissing if feed is not started.
// - otherwise, error is empty string.
func (p *Projector) doDelInstances(
	request *protobuf.DelInstancesRequest, opaque uint16) apcommon.MessageMarshaller {

	topic := request.GetTopic()

	// log this request.
	prefix := p.logPrefix
	logging.Infof("%v ##%x doDelInstances() %q\n", prefix, opaque, topic)
	defer logging.Infof("%v ##%x doDelInstances() returns ...\n", prefix, opaque)

	feed, err := p.acquireFeed(topic)
	defer p.releaseFeed(topic)
	if err != nil {
		logging.Errorf("%v ##%x acquireFeed(): %v\n", prefix, opaque, err)
		return protobuf.NewError(err)
	}

	err = feed.DelInstances(request, opaque)
	p.UpdateStats(topic, feed)
	return protobuf.NewError(err)
}

// - return ErrorTopicMissing if feed is not started.
// - otherwise, error is empty string.
func (p *Projector) doRepairEndpoints(
	request *protobuf.RepairEndpointsRequest,
	opaque uint16) apcommon.MessageMarshaller {

	topic := request.GetTopic()

	// log this request.
	prefix := p.logPrefix
	logging.Infof("%v ##%x doRepairEndpoints() %q\n", prefix, opaque, topic)
	defer logging.Infof("%v ##%x doRepairEndpoints() returns ...\n", prefix, opaque)

	feed, err := p.acquireFeed(topic)
	defer p.releaseFeed(topic)
	if err != nil {
		logging.Errorf("%v ##%x acquireFeed(): %v\n", prefix, opaque, err)
		return protobuf.NewError(err)
	}

	err = feed.RepairEndpoints(request, opaque)
	p.UpdateStats(topic, feed)
	return protobuf.NewError(err)
}

// - return ErrorTopicMissing if feed is not started.
// - otherwise, error is empty string.
func (p *Projector) doShutdownTopic(
	request *protobuf.ShutdownTopicRequest,
	opaque uint16) apcommon.MessageMarshaller {

	topic := request.GetTopic()

	// log this request.
	prefix := p.logPrefix
	logging.Infof("%v ##%x doShutdownTopic() %q\n", prefix, opaque, topic)
	defer logging.Infof("%v ##%x doShutdownTopic() returns ...\n", prefix, opaque)

	feed, err := p.acquireFeed(topic)
	defer p.releaseFeed(topic)
	if err != nil {
		logging.Errorf("%v ##%x acquireFeed(): %v\n", p.logPrefix, opaque, err)
		return protobuf.NewError(err)
	}

	p.DelFeed(topic)
	err = feed.Shutdown(opaque)
	if err == nil {
		p.statsMutex.Lock()
		delete(p.stats.feedStats, topic)
		clonedStats := p.stats.Clone()
		p.statsMutex.Unlock()

		p.UpdateStatsMgr(clonedStats)
	}
	return protobuf.NewError(err)
}

func (p *Projector) doStatistics() interface{} {
	logging.Infof("%v doStatistics()\n", p.logPrefix)
	defer logging.Infof("%v doStatistics() returns ...\n", p.logPrefix)

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

func validateAuth(w http.ResponseWriter, r *http.Request) bool {
	_, valid, err := c.IsAuthValid(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error() + "\n"))
	} else if valid == false {
		audit.Audit(common.AUDIT_UNAUTHORIZED, r, "projector::validateAuth", "")
		w.WriteHeader(http.StatusUnauthorized)
		w.Write(common.HTTP_STATUS_UNAUTHORIZED)
	}
	return valid
}

// handle projector statistics
func (p *Projector) handleStats(w http.ResponseWriter, r *http.Request) {
	valid := validateAuth(w, r)
	if !valid {
		return
	}

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
	valid := validateAuth(w, r)
	if !valid {
		return
	}

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
			fmsg := "%v handleSettings() json decoding: %v\n"
			logging.Errorf(fmsg, p.logPrefix, err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// update projector settings
		logging.Infof("%v updating projector config ...\n", p.logPrefix)
		config, _ := c.NewConfig(newConfig)
		config.LogConfig(p.logPrefix)
		p.ResetConfig(config)
		p.ResetFeedConfig()

	default:
		http.Error(w, "only GET POST supported", http.StatusMethodNotAllowed)
	}
}

// handle internal version request.
func (p *Projector) handleInternalVersion(w http.ResponseWriter, r *http.Request) {
	valid := validateAuth(w, r)
	if !valid {
		return
	}

	logging.Verbosef("%s Request %q\n", p.logPrefix, r.URL.Path)

	data, err := c.GetMarshalledInternalVersion()
	if err != nil {
		logging.Debugf("Projector:handleInternalVersion error %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	logging.Debugf("Projector:handleInternalVersion data %s", data)

	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

//----------------
// local functions
//----------------

// start cpu profiling.
func (p *Projector) startCPUProfile(filename string) *os.File {
	if filename == "" {
		fmsg := "%v empty cpu profile filename\n"
		logging.Errorf(fmsg, p.logPrefix, filename)
		return nil
	}
	fd, err := os.Create(filename)
	if err != nil {
		logging.Errorf("%v unable to create %q: %v\n", p.logPrefix, filename, err)
	}
	pprof.StartCPUProfile(fd)
	return fd
}

func (p *Projector) takeMEMProfile(filename string) bool {
	if filename == "" {
		fmsg := "%v empty mem profile filename\n"
		logging.Errorf(fmsg, p.logPrefix, filename)
		return false
	}
	fd, err := os.Create(filename)
	if err != nil {
		logging.Errorf("%v unable to create %q: %v\n", p.logPrefix, filename, err)
		return false
	}
	pprof.WriteHeapProfile(fd)
	defer fd.Close()
	return true
}

// return list of active topics
func (p *Projector) listTopics() []string {
	p.rw.Lock()
	defer p.rw.Unlock()
	topics := make([]string, 0, len(p.topics))
	for topic := range p.topics {
		topics = append(topics, topic)
	}
	return topics
}

func (p *Projector) acquireFeed(topic string) (*Feed, error) {
	p.rw.Lock()
	mu, ok := p.topicSerialize[topic]
	if !ok {
		mu = new(sync.Mutex)
	}
	p.topicSerialize[topic] = mu
	p.rw.Unlock()

	mu.Lock() // every acquireFeed is accompanied by releaseFeed. lock always!

	feed, err := p.GetFeed(topic)
	if err != nil {
		p.DelFeed(topic)
		return nil, projC.ErrorTopicMissing
	}
	return feed, nil
}

func (p *Projector) releaseFeed(topic string) {
	p.rw.RLock()
	mu := p.topicSerialize[topic]
	p.rw.RUnlock()
	mu.Unlock()
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

func (p *Projector) initSecurityContext(encryptLocalHost bool) error {

	logger := func(err error) { c.Console(p.clusterAddr, err.Error()) }
	if err := security.InitSecurityContext(logger, p.clusterAddr, p.certFile, p.keyFile, encryptLocalHost); err != nil {
		return err
	}

	fn := func(refreshCert bool, refreshEncrypt bool) error {
		select {
		case <-p.enableSecurityChange:
		default:
			logging.Infof("Receive security change during indexer bootstrap.  Restarting projector ...")
			os.Exit(1)
		}

		if err := refreshSecurityContextOnTopology(p.clusterAddr); err != nil {
			return err
		}

		// restart HTTPS server
		p.admind.Stop()
		time.Sleep(500 * time.Millisecond)
		p.setupHTTP()

		return nil
	}

	security.RegisterCallback("projector", fn)

	if err := refreshSecurityContextOnTopology(p.clusterAddr); err != nil {
		return err
	}

	return nil
}

func refreshSecurityContextOnTopology(clusterAddr string) error {

	fn := func(r int, e error) error {
		var cinfo *c.ClusterInfoCache
		url, err := c.ClusterAuthUrl(clusterAddr)
		if err != nil {
			return err
		}

		cinfo, err = c.NewClusterInfoCache(url, "default")
		cinfo.SetUserAgent("projector::refreshSecurityContextOnTopology")
		if err != nil {
			return err
		}

		cinfo.Lock()
		defer cinfo.Unlock()

		if err := cinfo.Fetch(); err != nil {
			return err
		}

		security.SetEncryptPortMapping(cinfo.EncryptPortMapping())

		return nil
	}

	helper := c.NewRetryHelper(10, time.Second, 1, fn)
	return helper.Run()
}

func (p *Projector) getNodeUUID() (string, error) {
	var nodeUUID string
	prefix := p.logPrefix
	fn := func(r int, err error) error {
		cinfo := p.cinfoClient.GetClusterInfoCache()
		cinfo.RLock()
		defer cinfo.RUnlock()

		if nodeUUID = cinfo.GetLocalNodeUUID(); nodeUUID == "" {
			// Force fetch cluster info cache so that
			// next attempt might succeed
			cinfo.RUnlock()
			cinfo.FetchWithLock()
			cinfo.RLock()

			fmsg := "%v cinfo.GetLocalNodeUUID(), nodeUUID empty\n"
			logging.Errorf(fmsg, prefix)
			return fmt.Errorf(fmsg, prefix)
		}
		return nil
	}
	rh := c.NewRetryHelper(MAX_CINFO_CACHES_RETRIES, time.Second, 1, fn)
	err := rh.Run()
	return nodeUUID, err
}

func GetNodeUUID() string {
	return nodeUUID
}
