package projector

import (
	"fmt"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/dataport"
	memcached "github.com/couchbase/indexing/secondary/dcp/transport/client"
	"github.com/couchbase/indexing/secondary/logging"
)

const (
	UPDATE_STATS_MAP byte = iota + 1
	STATS_LOG_INTERVAL_UPDATE
	VBSEQNOS_LOG_INTERVAL_UPDATE
)

type BucketStats struct {
	dcpStats  map[string]interface{}
	kvstats   map[string]interface{}
	wrkrStats map[string][]interface{}

	// Time when last vbseqnos were logged
	lastVbseqnosLogTime int64
}

func (bs *BucketStats) Init() {
	bs.dcpStats = make(map[string]interface{}, 0)
	bs.kvstats = make(map[string]interface{}, 0)
	bs.wrkrStats = make(map[string][]interface{}, 0)

	bs.lastVbseqnosLogTime = time.Now().UnixNano()
}

func (bs *BucketStats) clone() *BucketStats {
	cbs := &BucketStats{}
	cbs.Init()
	for key, value := range bs.dcpStats {
		if value != nil {
			cbs.dcpStats[key] = value
		}
	}

	for key, value := range bs.kvstats {
		if value != nil {
			cbs.kvstats[key] = value
		}
	}

	for key, value := range bs.wrkrStats {
		if value != nil {
			wrkrstat := make([]interface{}, 0)
			for _, stat := range value {
				wrkrstat = append(wrkrstat, stat)
			}
			cbs.wrkrStats[key] = wrkrstat
		}
	}
	return cbs
}

type FeedStats struct {
	bucketStats map[string]*BucketStats
	endpStats   map[string]interface{}

	// For other topic level stats
}

func (fs *FeedStats) Init() {
	fs.bucketStats = make(map[string]*BucketStats, 0)
	fs.endpStats = make(map[string]interface{}, 0)
}

func (fs *FeedStats) clone() *FeedStats {
	cfs := &FeedStats{}
	cfs.Init()
	for key, value := range fs.bucketStats {
		if value != nil {
			cfs.bucketStats[key] = value.clone()
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

	// For other projector level stats
}

func NewProjectorStats() *ProjectorStats {
	ps := &ProjectorStats{}
	ps.Init()
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
	return cps
}

type ProjectorStatsHolder struct {
	ptr unsafe.Pointer
}

func (p ProjectorStatsHolder) Get() *ProjectorStats {
	return (*ProjectorStats)(atomic.LoadPointer(&p.ptr))
}

func (p *ProjectorStatsHolder) Set(s *ProjectorStats) {
	atomic.StorePointer(&p.ptr, unsafe.Pointer(s))
}

type statsManager struct {
	stats                ProjectorStatsHolder
	cmdCh                chan []interface{}
	stopCh               chan bool
	stopLogger           int32
	statsLogDumpInterval int64
	vbseqnosLogInterval  int64
	lastStatTime         time.Time
	config               common.ConfigHolder
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
		atomic.StoreInt64(&sm.statsLogDumpInterval, int64(60))
	}

	if val, ok := config["projector.vbseqnosLogInterval"]; ok {
		atomic.StoreInt64(&sm.vbseqnosLogInterval, int64(val.Int()))
	} else {
		// Use default value of 300 seconds
		atomic.StoreInt64(&sm.vbseqnosLogInterval, int64(300))
	}

	logging.Infof("StatsManager: Stats logging interval set to: %v seconds", time.Duration(sm.statsLogDumpInterval).Seconds())
	logging.Infof("StatsManager: vbseqnos logging interval set to: %v seconds", time.Duration(sm.vbseqnosLogInterval).Seconds())

	sm.config.Store(config)
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
				atomic.StoreInt64(&sm.vbseqnosLogInterval, int64(val))
			}
		case <-sm.stopCh:
			atomic.StoreInt32(&sm.stopLogger, 1)
			return
		}
	}
}

func (sm *statsManager) logger() {
	for {
		//Get the projector stats
		ps := sm.stats.Get()
		if ps != nil {
			// For each topic
			for _, feedStats := range ps.feedStats {
				// For each bucket
				for _, bucketStats := range feedStats.bucketStats {
					for key, value := range bucketStats.dcpStats {
						switch value.(type) {
						case *memcached.DcpStats:
							val := value.(*memcached.DcpStats)
							if !val.IsClosed() {
								stats, latency := val.String()
								logging.Infof("%v dcp latency stats %v", key, latency)
								logging.Infof("%v stats: %v", key, stats)
							} else {
								logging.Tracef("%v closed", key)
							}
						default:
							logging.Errorf("Unknown Dcp stats type for %v", key)
							continue
						}
					}

					for key, value := range bucketStats.kvstats {
						switch (value).(type) {
						case *KvdataStats:
							val := (value).(*KvdataStats)
							if !val.IsClosed() {
								stats, vbseqnos := val.String()
								logging.Infof("%v stats: %v", key, stats)
								// Log vbseqno's for every "vbseqnosLogInterval" seconds
								now := time.Now().UnixNano()
								if (now - bucketStats.lastVbseqnosLogTime) > atomic.LoadInt64(&sm.vbseqnosLogInterval)*1e9 {
									logging.Infof("%v vbseqnos: [%v]", key, vbseqnos)
									bucketStats.lastVbseqnosLogTime = now
								}
							} else {
								logging.Tracef("%v closed", key)
							}
						default:
							logging.Errorf("Unknown Kvdata stats type for %v", key)
							continue
						}
					}

					for key, value := range bucketStats.wrkrStats {
						// Get the type of any worker
						switch (value[0]).(type) {
						case *WorkerStats:
							logging.Infof("%v stats: %v", key, Accmulate(value))
						default:
							logging.Errorf("Unknown worker stats type for %v", key)
							continue
						}
					}
				}

				// Log the endpoint stats for this feed
				for key, value := range feedStats.endpStats {
					switch value.(type) {
					case *dataport.EndpointStats:
						val := value.(*dataport.EndpointStats)
						if !val.IsClosed() {
							logging.Infof("%v stats: %v", key, val.String())
						} else {
							logging.Tracef("%v closed", key)
						}
					default:
						logging.Errorf("Unknown Endpoint stats type for %v", key)
						continue
					}
				}
			}
		}
		if atomic.LoadInt32(&sm.stopLogger) == 1 {
			return
		}
		time.Sleep(time.Second * time.Duration(atomic.LoadInt64(&sm.statsLogDumpInterval)))
	}
}

func Accmulate(wrkr []interface{}) string {
	var dataChLen, outgoingMut uint64
	for _, stats := range wrkr {
		wrkrStat := stats.(*WorkerStats)
		dataChLen += wrkrStat.datachLen.Value()
		outgoingMut += wrkrStat.outgoingMut.Value()
	}
	return fmt.Sprintf(
		"{\"datachLen\":%v,\"outgoingMut\":%v}", dataChLen, outgoingMut)
}
