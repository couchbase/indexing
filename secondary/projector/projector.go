package projector

import (
	"encoding/json"
	"expvar"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/cbauth"
	apcommon "github.com/couchbase/indexing/secondary/adminport/common"
	apserver "github.com/couchbase/indexing/secondary/adminport/server"
	"github.com/couchbase/indexing/secondary/audit"
	"github.com/couchbase/indexing/secondary/common"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/common/cbauthutil"
	mc "github.com/couchbase/indexing/secondary/dcp/transport/client"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/logging/systemevent"
	projC "github.com/couchbase/indexing/secondary/projector/client"
	"github.com/couchbase/indexing/secondary/projector/memThrottler"
	"github.com/couchbase/indexing/secondary/projector/memmanager"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
	"github.com/couchbase/indexing/secondary/security"
	"github.com/couchbase/indexing/secondary/system"
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

	cinfoProvider     common.ClusterInfoProvider
	cinfoProviderLock sync.RWMutex

	srvrCertFile         string
	srvrKeyFile          string
	caFile               string
	clientCertFile       string
	clientKeyFile        string
	reqch                chan apcommon.Request
	enableSecurityChange chan bool
	//Statistics
	stats       *ProjectorStats
	statsMgr    *statsManager
	statsCmdCh  chan []interface{}
	statsStopCh chan bool
	statsMutex  sync.RWMutex

	// Can be either of system or container's limit. When user
	// changes projector.maxCpuPercent and it is greater than
	// this cpuLimit, projector would ignore the settings change
	// and log an error (on console and in logs)
	cpuLimit int32
}

// NewProjector creates a news projector instance and
// starts a corresponding adminport.
func NewProjector(config c.Config,
	srvrCertFile, srvrKeyFile, caFile,
	clientCertFile, clientKeyFile string,
) *Projector {
	p := &Projector{
		topics:               make(map[string]*Feed),
		topicSerialize:       make(map[string]*sync.Mutex),
		pooln:                "default", // TODO: should this be configurable ?
		srvrCertFile:         srvrCertFile,
		srvrKeyFile:          srvrKeyFile,
		caFile:               caFile,
		clientCertFile:       clientCertFile,
		clientKeyFile:        clientKeyFile,
		enableSecurityChange: make(chan bool),
		statsCmdCh:           make(chan []interface{}, 1),
		statsStopCh:          make(chan bool, 1),
	}

	sysStats, err := initSystemStatsHandler()
	c.CrashOnError(err)

	p.updateMaxCpuPercent(sysStats, config)

	// Setup dynamic configuration propagation
	config, err = c.GetSettingsConfig(config)
	c.CrashOnError(err)

	pconfig := config.SectionConfig("projector.", true /*trim*/)
	p.name = pconfig["name"].String()
	p.clusterAddr = pconfig["clusterAddr"].String()
	p.adminport = pconfig["adminport.listenAddr"].String()
	ef := config["projector.routerEndpointFactory"]
	config["projector.routerEndpointFactory"] = ef

	// Initialize SystemEventLogger
	err = systemevent.InitSystemEventLogger(p.clusterAddr)
	if err != nil {
		common.CrashOnError(err)
	}

	// Start cluster info client
	useClusterInfoLite := config["projector.use_cinfo_lite"].Bool()
	cip, err := c.NewClusterInfoProvider(useClusterInfoLite, p.clusterAddr, "default",
		"projector", config.SectionConfig("projector.", true))
	if err != nil {
		logging.Errorf("Unable to get new ClusterInfoProvider err: %v, use_cinfo_lite: %v", err, useClusterInfoLite)
		c.CrashOnError(err)
	}
	cip.SetRetryInterval(4)
	p.cinfoProvider = cip
	logging.Infof("Started new ClusterInfoProvider use_cinfo_lite: %v", useClusterInfoLite)

	go PollForDeletedBucketsV2(p.clusterAddr, c.DEFAULT_POOL, config)
	go StartMonitoringIndexerNodes(p.clusterAddr, c.DEFAULT_POOL)

	systemStatsCollectionInterval := int64(config["projector.systemStatsCollectionInterval"].Int())
	memmanager.Init(systemStatsCollectionInterval, sysStats) // Initialize memory manager

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
	err = func() error {
		e := p.initSecurityContext(encryptLocalHost)
		if e != nil {
			return fmt.Errorf("Fail to initialize security context: %v", e)
		}

		e = cbauthutil.RegisterConfigRefreshCallback()
		if e != nil {
			return fmt.Errorf("Fail to register config refresh callback: %v", e)
		}

		e = p.registerSecurityCallback()
		if e != nil {
			return fmt.Errorf("Fail to register security callback: %v", e)
		}

		e = common.RefreshSecurityContextOnTopology(p.clusterAddr)
		if e != nil {
			return fmt.Errorf("Fail to refresh security context: %v", e)
		}

		go common.MonitorServiceForPortChanges(p.clusterAddr)

		return nil
	}()
	if err != nil {
		c.CrashOnError(err)
	}

	// Initialize auditing
	err = audit.InitAuditService(p.clusterAddr)
	if err != nil {
		common.CrashOnError(err)
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
	go p.watcherDameon(watchInterval, staleTimeout)

	callb := func(newConfig c.Config) {
		logging.Infof("%v settings notifier from metakv\n", p.logPrefix)
		newConfig.LogConfig(p.logPrefix)

		oldConfig := p.config.Clone()

		p.ResetConfig(newConfig)
		p.ResetFeedConfig()

		diffOld, diffNew := oldConfig.SectionConfig("projector.",
			false).Diff(newConfig.SectionConfig("projector.", false))
		if len(diffOld) != 0 {
			se := systemevent.NewSettingsChangeEvent(
				"NewProjector:callb",
				diffOld.Map(), diffNew.Map())
			eventID := systemevent.EVENTID_PROJECTOR_SETTINGS_CHANGE
			systemevent.InfoEvent("Projector", eventID, se)
		}

		if ucl, ok := newConfig["projector.use_cinfo_lite"]; ok {
			newUseCInfoLite := ucl.Bool()
			oldUseCInfoLite := oldConfig["projector.use_cinfo_lite"].Bool()
			if oldUseCInfoLite != newUseCInfoLite {
				logging.Infof("Updating ClusterInfoProvider in projector")

				cip, err := c.NewClusterInfoProvider(newUseCInfoLite, p.clusterAddr, "default",
					"projector", p.config.SectionConfig("projector.", true))
				if err != nil {
					logging.Errorf("%v Unable to update ClusterInfoProvider in Projector err: %v, use_cinfo_lite: old %v new %v",
						p.logPrefix, err, oldUseCInfoLite, newUseCInfoLite)
					c.CrashOnError(err)
				}
				cip.SetRetryInterval(4)

				p.cinfoProviderLock.Lock()
				oldPtr := p.cinfoProvider
				p.cinfoProvider = cip
				p.cinfoProviderLock.Unlock()

				logging.Infof("%v Updated ClusterInfoProvider in Projector use_cinfo_lite: old %v new %v", p.logPrefix,
					oldUseCInfoLite, newUseCInfoLite)
				oldPtr.Close()
			}
		}
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
		p.admind, err = apserver.NewHTTPServer(apConfig, p.reqch, p.cinfoProvider, &p.cinfoProviderLock)
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
		val := cv.Int()
		cpuLimit := atomic.LoadInt32(&p.cpuLimit)
		if val > int(cpuLimit) {
			logging.Warnf("Projector maxCpuPercent (%v) is greater than the available CPU for this system/container (%v). "+
				"Setting Projector CPU to: %v percent", val, cpuLimit, cpuLimit)
			common.Console(p.clusterAddr, "Projector maxCpuPercent (%v) is greater than the available CPU for this system/container (%v). "+
				"Setting Projector CPU to: %v percent", val, cpuLimit, cpuLimit)
			val = int(cpuLimit)
		}
		logging.Infof("Projector CPU set at %v", val)
		c.SetNumCPUs(val)
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

	// CInfo Lite
	if fa, ok := config["projector.cinfo_lite.force_after"]; ok {
		common.SetCICLMgrTimeDiffToForceFetch(fa.Uint32())
	}

	if nrs, ok := config["projector.cinfo_lite.notifier_restart_sleep"]; ok {
		common.SetCICLMgrSleepTimeOnNotifierRestart(nrs.Uint32())
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

// If feedStats are provided, this will not wait for feed.reqch to process fCmdGetStats
func (p *Projector) UpdateStats(topic string, feed *Feed, feedStats *FeedStats) {
	if feedStats == nil {
		feedStats = feed.GetStats()
	}

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

	p.UpdateStats(topic, feed, nil)
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
		p.UpdateStats(topic, feed, nil)
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
	p.UpdateStats(topic, feed, nil)
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
		p.UpdateStats(topic, feed, nil)
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
	p.UpdateStats(topic, feed, nil)
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
	p.UpdateStats(topic, feed, nil)
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
	p.UpdateStats(topic, feed, nil)
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
	p.UpdateStats(topic, feed, nil)
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

func validateAuth(w http.ResponseWriter, r *http.Request) (cbauth.Creds, bool) {
	creds, valid, err := c.IsAuthValid(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error() + "\n"))
	} else if valid == false {
		audit.Audit(common.AUDIT_UNAUTHORIZED, r, "projector::validateAuth", "")
		w.WriteHeader(http.StatusUnauthorized)
		w.Write(common.HTTP_STATUS_UNAUTHORIZED)
	}
	return creds, valid
}

// handle projector statistics
func (p *Projector) handleStats(w http.ResponseWriter, r *http.Request) {
	creds, valid := validateAuth(w, r)
	if !valid {
		return
	} else if creds != nil {
		allowed, err := creds.IsAllowed("cluster.admin.internal.index!read")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		} else if !allowed {
			logging.Verbosef("projector::handleStats not enough permissions")
			w.WriteHeader(http.StatusForbidden)
			w.Write(common.HTTP_STATUS_FORBIDDEN)
			return
		}
	}
	//TODO: Filter bucket level feed stats

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
	creds, valid := validateAuth(w, r)
	if !valid {
		return
	} else if creds != nil {
		allowed, err := creds.IsAllowed("cluster.admin.internal.index!write")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		} else if !allowed {
			logging.Verbosef("projector::handleSettings not enough permissions")
			w.WriteHeader(http.StatusForbidden)
			w.Write(common.HTTP_STATUS_FORBIDDEN)
			return
		}
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
	creds, valid := validateAuth(w, r)
	if !valid {
		return
	} else if creds != nil {
		allowed, err := creds.IsAllowed("cluster.admin.internal.index!read")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		} else if !allowed {
			logging.Verbosef("projector::handleInternalVersion not enough permissions")
			w.WriteHeader(http.StatusForbidden)
			w.Write(common.HTTP_STATUS_FORBIDDEN)
			return
		}
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
	if err := security.InitSecurityContext(logger, p.clusterAddr, p.srvrCertFile, p.srvrKeyFile,
		p.caFile, p.clientCertFile, p.clientKeyFile, encryptLocalHost); err != nil {
		return err
	}
	return nil
}

func (p *Projector) registerSecurityCallback() error {
	security.WaitForSecurityCtxInit()

	fn := func(_, _, refreshEncrypt bool) error {
		select {
		case <-p.enableSecurityChange:
		default:
			logging.Infof("Receive security change during indexer bootstrap.  Restarting projector ...")
			os.Exit(1)
		}

		if err := common.RefreshSecurityContextOnTopology(p.clusterAddr); err != nil {
			return err
		}

		if refreshEncrypt {
			// restart HTTPS server
			p.admind.CloseReqch()
			p.admind.Stop()
			time.Sleep(500 * time.Millisecond)
			p.setupHTTP()
		}

		return nil
	}

	security.RegisterCallback("projector", fn)

	return nil
}

// refreshSecurityContextOnTopology - DEPRECATED. use common.RefreshSecurityContextOnTopology
func refreshSecurityContextOnTopology(clusterAddr string) error {
	return c.RefreshSecurityContextOnTopology(clusterAddr)
}

func (p *Projector) getNodeUUID() (string, error) {
	var nodeUUID string
	prefix := p.logPrefix
	fn := func(r int, err error) error {
		p.cinfoProviderLock.RLock()
		defer p.cinfoProviderLock.RUnlock()

		ninfo, err := p.cinfoProvider.GetNodesInfoProvider()
		if err != nil {
			fmsg := "%v cinfoProvider.GetNodesInfoProvider failed with err: %v"
			logging.Errorf(fmsg, prefix, err)
			return fmt.Errorf(fmsg, prefix, err)
		}

		ninfo.RLock()
		nodeUUID = ninfo.GetLocalNodeUUID()
		ninfo.RUnlock()

		if nodeUUID == "" {
			// Force fetch cluster info cache so that
			// next attempt might succeed
			p.cinfoProvider.ForceFetch()

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

func (p *Projector) updateMaxCpuPercent(stats *system.SystemStats, config c.Config) {
	cgroupInfo := stats.GetControlGroupInfo()

	if cgroupInfo.Supported == common.SIGAR_CGROUP_SUPPORTED {
		atomic.StoreInt32(&p.cpuLimit, int32(cgroupInfo.NumCpuPrc))
		logging.Infof("Projector::updateMaxCPUPercent, setting cpuLimit of the system to: %v "+
			"(number of cores available to this container * 100)", atomic.LoadInt32(&p.cpuLimit))

		cpuPercent := int(math.Max(400.0, float64(p.cpuLimit)*0.25))
		config.SetValue("projector.maxCpuPercent", cpuPercent)

		logging.Infof("Projector::updateMaxCpuPercent: Updating projector maxCpuPercent to: %v "+
			"as cores availble for this container are: %v", cpuPercent, atomic.LoadInt32(&p.cpuLimit))
	} else {
		logging.Infof("Projector::updateMaxCpuPercent: Sigar CGroupInfo not supported")

		atomic.StoreInt32(&p.cpuLimit, int32(runtime.NumCPU()*100))
		logging.Infof("Projector::updateMaxCPUPercent, setting cpuLimit of the system to: %v "+
			"(number of cores in the system * 100)", atomic.LoadInt32(&p.cpuLimit))
	}
}

func initSystemStatsHandler() (*system.SystemStats, error) {
	var stats *system.SystemStats
	var err error
	fn := func(r int, err error) error {
		// open sigar for stats
		stats, err = system.NewSystemStats()
		if err != nil {
			logging.Errorf("initSystemStatsHandler: Fail to start system stat collector. Err=%v", err)
			return err
		}
		return nil
	}

	rh := common.NewRetryHelper(int(common.SIGAR_INIT_RETRIES), time.Second*3, 1, fn)
	err = rh.Run()
	return stats, err
}

var gIndexerMonitor IndexerMonitor

type IndexerMonitor struct {
	sync.Mutex
	prevIndexerRaddrs   map[string]bool
	registeredEndpoints map[string]c.RouterEndpoint
}

func StartMonitoringIndexerNodes(clusterAddr string, pool string) {
	gIndexerMonitor = IndexerMonitor{
		prevIndexerRaddrs:   make(map[string]bool),
		registeredEndpoints: make(map[string]c.RouterEndpoint),
	}

	go gIndexerMonitor.monitorIndexerNodes(clusterAddr, pool)
}

func RegisterEndpoint(raddr string, endpoint c.RouterEndpoint) {
	gIndexerMonitor.Lock()
	defer gIndexerMonitor.Unlock()
	gIndexerMonitor.registeredEndpoints[raddr] = endpoint
	logging.Infof("Projector::RegisterEndpoint, registered endpoint for node: %v", raddr)
}

func UnregisterEndpoint(raddr string) {
	gIndexerMonitor.Lock()
	defer gIndexerMonitor.Unlock()
	delete(gIndexerMonitor.registeredEndpoints, raddr)
	logging.Infof("Projector::UnregisterEndpoint, unregistered endpoint for node: %v", raddr)
}

func CloseAndUnregisterEndpoint(raddr string) {
	gIndexerMonitor.Lock()
	defer gIndexerMonitor.Unlock()
	endpoint, ok := gIndexerMonitor.registeredEndpoints[raddr]
	if !ok {
		logging.Infof("Projector::CloseAndUnregisterEndpoint, endpoint not found for node: %v", raddr)
		return
	}
	endpoint.ConnClose()
	delete(gIndexerMonitor.registeredEndpoints, raddr)
	logging.Infof("Projector::CloseAndUnregisterEndpoint, closed and unregistered endpoint for node: %v", raddr)
}

func (gim *IndexerMonitor) monitorIndexerNodes(clusterAddr string, pool string) {

	defer func() {
		if r := recover(); r != nil {
			logging.Errorf("Projector::monitorIndexerNodes crashed: %v\n", r)
			go gim.monitorIndexerNodes(clusterAddr, pool)
		}
	}()

	selfRestart := func() {
		time.Sleep(5000 * time.Millisecond)
		go gim.monitorIndexerNodes(clusterAddr, pool)
	}

	url, err := common.ClusterAuthUrl(clusterAddr)
	if err != nil {
		logging.Errorf("Projector::monitorIndexerNodes, error observed while retrieving ClusterAuthUrl, err : %v", err)
		selfRestart()
		return
	}

	scn, err := common.NewServicesChangeNotifier(url, pool, "monitorIndexerNodes")
	if err != nil {
		logging.Errorf("Projector::monitorIndexerNodes, error observed while initializing ServicesChangeNotifier, err: %v", err)
		selfRestart()
		return
	}
	defer scn.Close()

	cinfo, err := common.NewClusterInfoCache(url, pool)
	if err != nil {
		logging.Errorf("Projector::monitorIndexerNodes, error observed during the initilization of clusterInfoCache, err : %v", err)
		selfRestart()
		return
	}
	cinfo.SetUserAgent("monitorIndexerNodes")

	changeInIndexerNodes := func(prev map[string]bool, curr map[string]bool) bool {
		if len(prev) != len(curr) {
			return true
		} else {
			for raddr, _ := range curr {
				if _, ok := prev[raddr]; !ok {
					return true
				}
			}
		}
		return false
	}

	closeInactiveIndexerEndpoints := func(currActiveIndexerRaddrs map[string]bool) {
		for raddr, _ := range gim.prevIndexerRaddrs {
			if _, ok := currActiveIndexerRaddrs[raddr]; !ok {
				CloseAndUnregisterEndpoint(raddr)
			}
		}
	}

	ch := scn.GetNotifyCh()
	for {
		select {
		case notif, ok := <-ch:
			if !ok {
				selfRestart()
				return
			}

			// Process only PoolChangeNotification as any change to
			// ClusterMembership is reflected only in PoolChangeNotification
			if notif.Type != common.PoolChangeNotification {
				continue
			}

			if err := cinfo.FetchNodesAndSvsInfoWithLock(); err != nil {
				logging.Errorf("Projector::monitorIndexerNodes, error observed while Updating cluster info cache, err: %v", err)
				selfRestart()
				return
			}

			currActiveIndexerRaddrs := cinfo.GetActiveIndexerNodesWithPorts(common.INDEX_DATA_MAINT, common.INDEX_DATA_INIT)
			logging.Verbosef("Projector::monitorIndexerNodes, currActiveIndexerRaddrs: %v prevIndexerRaddrs: %v", currActiveIndexerRaddrs, gim.prevIndexerRaddrs)
			if changeInIndexerNodes(gim.prevIndexerRaddrs, currActiveIndexerRaddrs) {
				closeInactiveIndexerEndpoints(currActiveIndexerRaddrs)
				gim.prevIndexerRaddrs = currActiveIndexerRaddrs
			}
		}
	}
}
