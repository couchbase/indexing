package projector

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/dataport"
	memcached "github.com/couchbase/indexing/secondary/dcp/transport/client"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/projector/memThrottler"
	"github.com/couchbase/indexing/secondary/projector/memmanager"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
	"github.com/couchbase/logstats/logstats"
)

const (
	defaultEvalStatLoggingThreshold = 200
	defaultStatsLogDumpInterval     = 60
	defaultEvalStatsLogInterval     = 300
	defaultVbseqnosLogInterval      = 5 * defaultStatsLogDumpInterval
)

const (
	UPDATE_STATS_MAP byte = iota + 1
	STATS_LOG_INTERVAL_UPDATE
	VBSEQNOS_LOG_INTERVAL_UPDATE
	EVAL_STAT_LOGGING_THRESHOLD
)

type KeyspaceIdStats struct {
	keyspaceId string
	topic      string
	opaque     uint16

	// Key -> Log prefix of DCP Feed
	// Value -> Pointer to stats object of DCP feed
	dcpStats     map[string]interface{}
	dcpLogPrefix string // logPrefix for dedupLogger

	// Key -> Log prefix of KVData
	// Value -> Pointer to stats object KVData
	kvstats map[string]interface{}

	// Key -> Log prefix for Workers
	// Value -> Slice containing pointer to stats object of all workers
	wrkrStats map[string][]interface{}

	// Key -> <bucketname>:<indexname>
	// Value -> Pointer to stats object for index evaluator
	evaluatorStats map[string]interface{}
}

func (ks *KeyspaceIdStats) Init() {
	ks.dcpStats = make(map[string]interface{}, 0)
	ks.kvstats = make(map[string]interface{}, 0)
	ks.wrkrStats = make(map[string][]interface{}, 0)
	ks.evaluatorStats = make(map[string]interface{})
}

func (ks *KeyspaceIdStats) clone() *KeyspaceIdStats {
	cks := &KeyspaceIdStats{}
	cks.Init()
	cks.topic = ks.topic
	cks.keyspaceId = ks.keyspaceId
	cks.opaque = ks.opaque
	for key, value := range ks.dcpStats {
		if value != nil {
			cks.dcpStats[key] = value
		}
	}
	cks.dcpLogPrefix = ks.dcpLogPrefix

	for key, value := range ks.kvstats {
		if value != nil {
			cks.kvstats[key] = value
		}
	}

	for key, value := range ks.wrkrStats {
		if value != nil {
			wrkrstat := make([]interface{}, 0)
			for _, stat := range value {
				wrkrstat = append(wrkrstat, stat)
			}
			cks.wrkrStats[key] = wrkrstat
		}
	}

	for key, value := range ks.evaluatorStats {
		if value != nil {
			cks.evaluatorStats[key] = value
		}
	}
	return cks
}

type FeedStats struct {
	keyspaceIdStats map[string]*KeyspaceIdStats
	endpStats       map[string]interface{}

	// For other topic level stats
}

func (fs *FeedStats) Init() {
	fs.keyspaceIdStats = make(map[string]*KeyspaceIdStats, 0)
	fs.endpStats = make(map[string]interface{}, 0)
}

func (fs *FeedStats) clone() *FeedStats {
	cfs := &FeedStats{}
	cfs.Init()
	for key, value := range fs.keyspaceIdStats {
		if value != nil {
			cfs.keyspaceIdStats[key] = value.clone()
		}
	}

	for key, value := range fs.endpStats {
		if value != nil {
			cfs.endpStats[key] = value
		}
	}

	return cfs
}

type ProjectorStats struct {
	feedStats map[string]*FeedStats

	statsLogTime        int64 // Time when component stats were logged
	lastVbseqnosLogTime int64 // Time when last vbseqnos were logged
	evalStatsLogTime    int64 // Time when last eval stats were logged
}

func NewProjectorStats() *ProjectorStats {
	ps := &ProjectorStats{}
	ps.Init()
	ps.lastVbseqnosLogTime = time.Now().UnixNano()
	ps.evalStatsLogTime = time.Now().UnixNano()
	ps.statsLogTime = time.Now().UnixNano()

	return ps
}

func (ps *ProjectorStats) Init() {
	ps.feedStats = make(map[string]*FeedStats, 0)
}

func (ps *ProjectorStats) Clone() *ProjectorStats {
	cps := &ProjectorStats{}
	cps.Init()
	for topic, feedStat := range ps.feedStats {
		clonedFeedStat := feedStat.clone()
		cps.feedStats[topic] = clonedFeedStat
	}

	cps.lastVbseqnosLogTime = ps.lastVbseqnosLogTime
	cps.evalStatsLogTime = ps.evalStatsLogTime
	cps.statsLogTime = ps.statsLogTime
	return cps
}

type ProjectorStatsHolder struct {
	ptr unsafe.Pointer
}

func (p *ProjectorStatsHolder) Get() *ProjectorStats {
	return (*ProjectorStats)(atomic.LoadPointer(&p.ptr))
}

func (p *ProjectorStatsHolder) Set(s *ProjectorStats) {
	atomic.StorePointer(&p.ptr, unsafe.Pointer(s))
}

type statsManager struct {
	stats                    ProjectorStatsHolder
	cmdCh                    chan []interface{}
	stopCh                   chan bool
	stopLogger               int32
	statsLogDumpInterval     int64
	vbseqnosLogInterval      int64
	evalStatsLogInterval     int64
	evalStatLoggingThreshold int64
	lastStatTime             time.Time
	config                   common.ConfigHolder
	statLogger               logstats.LogStats
}

func NewStatsManager(cmdCh chan []interface{}, stopCh chan bool, config common.Config) *statsManager {
	sm := &statsManager{
		cmdCh:        cmdCh,
		stopCh:       stopCh,
		lastStatTime: time.Unix(0, 0),
	}

	if val, ok := config["projector.statsLogDumpInterval"]; ok {
		atomic.StoreInt64(&sm.statsLogDumpInterval, int64(val.Int()))
	} else {
		// Use default value of 60 seconds
		atomic.StoreInt64(&sm.statsLogDumpInterval, int64(defaultStatsLogDumpInterval))
	}

	si := atomic.LoadInt64(&sm.statsLogDumpInterval)
	if val, ok := config["projector.vbseqnosLogIntervalMultiplier"]; ok {
		atomic.StoreInt64(&sm.vbseqnosLogInterval, int64(val.Int())*si)
	} else {
		// Use default value of 5 * statsLogDumpInterval seconds
		atomic.StoreInt64(&sm.vbseqnosLogInterval, int64(defaultVbseqnosLogInterval))
	}

	if val, ok := config["projector.evalStatLoggingThreshold"]; ok {
		atomic.StoreInt64(&sm.evalStatLoggingThreshold, int64(val.Int()))
	} else {
		// Use default value of 60 seconds
		atomic.StoreInt64(&sm.evalStatLoggingThreshold, int64(defaultEvalStatLoggingThreshold))
	}

	// Use default value of 300 seconds
	atomic.StoreInt64(&sm.evalStatsLogInterval, int64(defaultEvalStatsLogInterval))

	logging.Infof("StatsManager: Stats logging interval set to: %v seconds", atomic.LoadInt64(&sm.statsLogDumpInterval))
	logging.Infof("StatsManager: vbseqnos logging interval set to: %v seconds", atomic.LoadInt64(&sm.vbseqnosLogInterval))
	logging.Infof("StatsManager: eval stats logging interval set to: %v seconds", atomic.LoadInt64(&sm.evalStatsLogInterval))
	logging.Infof("StatsManager: eval stats logging threshold set to: %v microseconds", atomic.LoadInt64(&sm.evalStatLoggingThreshold))

	sm.config.Store(config)

	err := sm.setupLogStatsLogger()
	if err != nil {
		logging.Fatalf("StatsManager: failed to setup stats logger with err %v", err)
		common.CrashOnError(err)
	}

	if sm.statLogger != nil {
		logstats.SetGlobalStatLogger(sm.statLogger)
		go common.MemstatLogger2(sm.statLogger, int64(config["projector.memstatTick"].Int()))
	} else {
		go common.MemstatLogger(int64(config["projector.memstatTick"].Int()))
	}

	go sm.run()
	go sm.logger()

	return sm
}

func (sm *statsManager) run() {
	for {
		select {
		case msg := <-sm.cmdCh:
			switch msg[0].(byte) {
			case UPDATE_STATS_MAP:
				ps := msg[1].(*ProjectorStats)
				sm.stats.Set(ps)
			case STATS_LOG_INTERVAL_UPDATE:
				val := msg[1].(int)
				atomic.StoreInt64(&sm.statsLogDumpInterval, int64(val))
			case VBSEQNOS_LOG_INTERVAL_UPDATE:
				val := msg[1].(int)
				atomic.StoreInt64(&sm.vbseqnosLogInterval, int64(val)*atomic.LoadInt64(&sm.statsLogDumpInterval))
			case EVAL_STAT_LOGGING_THRESHOLD:
				val := msg[1].(int)
				atomic.StoreInt64(&sm.evalStatLoggingThreshold, int64(val))
			}
		case <-sm.stopCh:
			atomic.StoreInt32(&sm.stopLogger, 1)
			return
		}
	}
}

func (sm *statsManager) logger() {
	for {
		// Get the projector stats
		ps := sm.stats.Get()
		if ps != nil {
			now := time.Now().UnixNano()
			var logStats, logVbsenos, logEvalStats bool

			// Check if projector component stats should be logged in this iteration
			if (now - ps.statsLogTime) > atomic.LoadInt64(&sm.statsLogDumpInterval)*1e9 {
				logStats = true
				ps.statsLogTime = now
			}
			// Check if vbseqnos should be logged in this iteration
			if (now - ps.lastVbseqnosLogTime) > atomic.LoadInt64(&sm.vbseqnosLogInterval)*1e9 {
				logVbsenos = true
				ps.lastVbseqnosLogTime = now
			}
			// Check if eval stats should be logged in this iteration
			if (now - ps.evalStatsLogTime) > atomic.LoadInt64(&sm.evalStatsLogInterval)*1e9 {
				logEvalStats = true
				ps.evalStatsLogTime = now
			}

			// For each topic
			for _, feedStats := range ps.feedStats {
				// For each bucket
				for _, keyspaceIdStats := range feedStats.keyspaceIdStats {
					kvdataClosed := false
					if logStats {
						sm.doLogDcpStats(keyspaceIdStats.dcpStats, keyspaceIdStats.dcpLogPrefix)

						kvdataClosed = sm.doLogKvStats(keyspaceIdStats.kvstats, logVbsenos)

						sm.doLogWrkrStats(keyspaceIdStats.wrkrStats)
					}

					// Log eval stats for every evalStatsLogInterval
					if logEvalStats && !kvdataClosed {
						sm.doLogEvaluatorStats(
							keyspaceIdStats.evaluatorStats, // evalStatsMap
							keyspaceIdStats.keyspaceId,     // keyspaceId
							keyspaceIdStats.topic,          // topic
							keyspaceIdStats.opaque,         // opaque
						)
					}
				}

				if logStats {
					sm.doLogEndpStats(feedStats.endpStats)
				}
			}
			sm.doLogProjectorStats()
		}
		if atomic.LoadInt32(&sm.stopLogger) == 1 {
			return
		}
		time.Sleep(time.Second * time.Duration(atomic.LoadInt64(&sm.statsLogDumpInterval)))
	}
}

func (sm *statsManager) doLogDcpStats(dcpStats map[string]interface{}, logPrefix string) {
	dcpStatsMap := make(map[string]interface{}, len(dcpStats))

	for key, value := range dcpStats {
		switch val := value.(type) {
		case *memcached.DcpStats:
			if !val.IsClosed() {
				stats, ltcStats := val.Map()
				dcpStatsMap[val.StreamNo] = map[string]interface{}{
					"stats": stats,
					"ltc":   ltcStats,
				}
			} else {
				logging.Tracef("%v closed", key)
			}
		default:
			logging.Errorf("Unknown Dcp stats type for %v", key)
			continue
		}
	}

	if len(dcpStatsMap) > 0 {
		if sm.statLogger != nil {
			sm.statLogger.Write(logPrefix, dcpStatsMap)
		} else {
			dcpStatsStr, err := json.Marshal(dcpStatsMap)
			if err != nil {
				logging.Errorf("%v marshal failure err - %v", logPrefix, err)
				return
			}
			logging.Infof("%v stats: %v", logPrefix, string(dcpStatsStr))
		}
	}
}

func (sm *statsManager) doLogKvStats(kvstats map[string]interface{}, logVbSeqNos bool) bool {
	kvdataClosed := false
	for key, value := range kvstats {
		switch val := (value).(type) {
		case *KvdataStats:
			if !val.IsClosed() {
				stats, vbseqnos := val.Map()
				if logVbSeqNos {
					stats["vbseqnos"] = vbseqnos
				}

				if sm.statLogger != nil {
					sm.statLogger.Write(key, stats)
				} else {
					var kvStatsStr, err = json.Marshal(stats)
					if err != nil {
						logging.Errorf("%v marshal failure err - %v", key, err)
						return false
					}
					logging.Infof("%v stats: %v", key, string(kvStatsStr))
				}
			} else {
				kvdataClosed = true
				logging.Tracef("%v closed", key)
			}
		default:
			logging.Errorf("Unknown Kvdata stats type for %v", key)
			continue
		}
	}
	return kvdataClosed
}

func (sm *statsManager) doLogWrkrStats(wrkrStats map[string][]interface{}) {
	for key, value := range wrkrStats {
		// Get the type of any worker
		switch val := (value[0]).(type) {
		case *WorkerStats:
			if !val.IsClosed() {
				if sm.statLogger != nil {
					sm.statLogger.Write(key, AccumulateJson(value))
				} else {
					logging.Infof("%v stats: %v", key, Accmulate(value))
				}
			}
		default:
			logging.Errorf("Unknown worker stats type for %v", key)
			continue
		}
	}
}

func (sm *statsManager) doLogEvaluatorStats(evalStatsMap map[string]interface{},
	keyspaceId, topic string, opaque uint16,
) {
	evalStatLoggingThreshold := atomic.LoadInt64(&sm.evalStatLoggingThreshold) * 1000

	// As of this commit, only IndexEvaluatorStats are supported
	logPrefix := getEvalStatsLogPRefix(topic, keyspaceId, opaque)
	var evalStats = make(map[string]interface{}, len(evalStatsMap))
	var skippedStr string

	for key, value := range evalStatsMap {
		switch val := (value).(type) {
		case *protobuf.IndexEvaluatorStats:
			var evalStatsKeyMap = make(map[string]interface{}, 0)

			avg := val.MovingAvg()

			if avg > evalStatLoggingThreshold {
				evalStatsKeyMap["avgLatency"] = avg
			}

			errSkip := val.GetAndResetErrorSkip()
			errSkipAll := val.GetErrorSkipAll()
			if errSkipAll > 0 {
				evalStatsKeyMap["skipCount"] = errSkipAll
			}
			vectorErrs := val.GetVectorErrs()
			for k, v := range vectorErrs {
				evalStatsKeyMap[k] = v
			}

			evalStats[key] = evalStatsKeyMap

			if errSkip != 0 {
				if len(skippedStr) == 0 {
					skippedStr = fmt.Sprintf("In last %v, projector skipped "+
						"evaluating some documents due to errors. Please see the "+
						"projector.log for details. Skipped document counts for "+
						"following indexes are:\n",
						time.Duration(atomic.LoadInt64(&sm.evalStatsLogInterval)*1e9))
				}
				skippedStr += fmt.Sprintf("\"%v\":%v,", key, errSkip)
			}
		default:
			logging.Errorf("%v Unknown type for evaluator stats", logPrefix)
			continue
		}
	}
	if len(evalStats) > 0 {
		if sm.statLogger != nil {
			sm.statLogger.Write(logPrefix, evalStats)
		} else {
			var evalStatsBytes, err = json.Marshal(evalStats)
			if err != nil {
				logging.Errorf("%v marshal failure err - %v", logPrefix, err)
				return
			}
			logging.Infof("%v stats: %v", logPrefix, string(evalStatsBytes))
		}
	}
	if len(skippedStr) != 0 {
		// Some mutations were skipped by the index evaluator.
		// Report it in the console logs.
		skippedStr = skippedStr[0 : len(skippedStr)-1]
		cfg := sm.config.Load()
		clusterAddr, ok := cfg["projector.clusterAddr"]
		if ok {
			common.Console(clusterAddr.String(), skippedStr)
		}
	}
}

func (sm *statsManager) doLogEndpStats(endpStats map[string]interface{}) {
	// Log the endpoint stats for this feed
	for key, value := range endpStats {
		switch val := value.(type) {
		case *dataport.EndpointStats:
			if !val.IsClosed() {
				if sm.statLogger != nil {
					sm.statLogger.Write(key, val.Map())
				} else {
					logging.Infof("%v stats: %v", key, val.String())
				}
			} else {
				logging.Tracef("%v closed", key)
			}
		default:
			logging.Errorf("Unknown Endpoint stats type for %v", key)
			continue
		}
	}
}

func (sm *statsManager) doLogProjectorStats() {
	if sm.statLogger != nil {
		sm.statLogger.Write("projector", map[string]interface{}{
			"cpu_util":       strconv.FormatFloat(memmanager.GetCpuPercent(), 'f', 5, 64),
			"mem_rss":        memmanager.GetRSS(),
			"mem_free":       memmanager.GetMemFree(),
			"mem_total":      memmanager.GetMemTotal(),
			"gc_percent":     memmanager.GetGCPercent(),
			"throttle_level": uint64(memThrottler.GetThrottleLevel()),
		})
	} else {
		logging.Infof("Projector stats: {\"cpu_utilisation_rate\":%v, \"memory_rss\":%v, \"memory_free\":%v,\"memory_total\":%v,\"gc_percent\":%v, \"throttle_level\":%v}",
			memmanager.GetCpuPercent(),
			memmanager.GetRSS(),
			memmanager.GetMemFree(),
			memmanager.GetMemTotal(),
			memmanager.GetGCPercent(),
			memThrottler.GetThrottleLevel(),
		)
	}
}

func Accmulate(wrkr []interface{}) string {
	var dataChLen, outgoingMut, updateSeqno, txnSystemMut uint64
	for _, stats := range wrkr {
		wrkrStat := stats.(*WorkerStats)
		dataChLen += (uint64)(len(wrkrStat.datach))
		outgoingMut += wrkrStat.outgoingMut.Value()
		updateSeqno += wrkrStat.updateSeqno.Value()
		txnSystemMut += wrkrStat.txnSystemMut.Value()
	}
	return fmt.Sprintf(
		"{\"datachLen\":%v,\"outgoingMut\":%v,\"updateSeqno\":%v,\"txnSystemMut\":%v}", dataChLen, outgoingMut, updateSeqno, txnSystemMut)
}

func (sm *statsManager) setupLogStatsLogger() error {
	config := sm.config.Load()

	logDir := config["projector.log_dir"].String()
	if len(logDir) == 0 {
		return nil
	}

	filename := config["projector.statsLogFname"].String()

	filefullpath := filepath.Join(logDir, filename)

	numfiles := config["projector.statsLogFcount"].Int()
	sizelimit := config["projector.statsLogFsize"].Int()

	var err error

	sm.statLogger, err = logstats.NewDedupeLogStats(
		filefullpath,              // fileName
		sizelimit,                 // sizeLimit
		numfiles,                  // numFiles
		common.STAT_LOG_TS_FORMAT, // tsFormat
	)

	return err
}

func AccumulateJson(wrkr []interface{}) map[string]interface{} {
	var dataChLen, outgoingMut, updateSeqno, txnSystemMut uint64
	for _, stats := range wrkr {
		wrkrStat := stats.(*WorkerStats)
		dataChLen += (uint64)(len(wrkrStat.datach))
		outgoingMut += wrkrStat.outgoingMut.Value()
		updateSeqno += wrkrStat.updateSeqno.Value()
		txnSystemMut += wrkrStat.txnSystemMut.Value()
	}
	return map[string]interface{}{
		"dataChLen":   dataChLen,
		"outMut":      outgoingMut,
		"updateSeqno": updateSeqno,
		"txnSysMut":   txnSystemMut,
	}
}
