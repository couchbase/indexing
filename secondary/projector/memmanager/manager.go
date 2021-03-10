package memmanager

import (
	"math"
	"runtime"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/system"
)

var defaultUsedMemThreshold = 0.5
var defaultRSSThreshold = 0.1

var memMgr *MemManager // Global variable. Initialized by projector

type MemManager struct {
	rss        uint64
	memTotal   uint64
	memFree    uint64
	cpuPercent uint64

	ms *runtime.MemStats

	// recentSamples holds last 4 samples (20 sec) worth of projector RSS
	// olderSamples holds last 12 samples (60 seconds) worth of projector RSS
	//
	// When a new sample is read,
	// (a) the sample is updated in each of these variables
	// (b) the shorter term average (SMA) is compared with longer term
	//     average (LMA)
	// (c) If SMA > LMA, then the projector RSS is increasing - In this case,
	//     memmanager will read memstats and decide to freeOSMemory if
	//     the difference is greater than a threshold
	// (d) If SMA is < LMA, then the projector RSS is decreasing - NoOp for
	//     memmanager
	recentSamples *common.Sample
	olderSamples  *common.Sample

	// Config related
	statsCollectionInterval int64
	usedMemThreshold        uint64
	rssThreshold            uint64

	stats *system.SystemStats
}

func Init(statsCollectionInterval int64) error {
	memMgr = &MemManager{
		ms: &runtime.MemStats{},
	}
	memMgr.recentSamples = common.NewSample(4)
	memMgr.olderSamples = common.NewSample(12)

	// open sigar for stats
	stats, err := system.NewSystemStats()
	if err != nil {
		logging.Errorf("Fail to start system stat collector. Err=%v", err)
		return err
	}
	memMgr.stats = stats

	// skip the first one
	memMgr.ProcessCpuPercent()
	memMgr.ProcessRSS()
	memMgr.ProcessFreeMem()
	memMgr.ProcessTotalMem()

	SetStatsCollectionInterval(statsCollectionInterval)
	SetUsedMemThreshold(defaultUsedMemThreshold)
	SetRSSThreshold(defaultRSSThreshold)

	// start stats collection
	go memMgr.runStatsCollection()
	go memMgr.monitorMemUsage()

	return nil
}

func (memMgr *MemManager) monitorMemUsage() {
	logging.Infof("MemManager::memManager started...")

	// Run ticker at a granularity of 1 second
	// but manage memory every "systemStatsCollectionPeriod" duration
	ticker := time.NewTicker(time.Second * 1)
	defer ticker.Stop()

	var lastCollectionTime time.Time

	for range ticker.C {
		statsCollectionInterval := GetStatsCollectionInterval()
		if time.Since(lastCollectionTime) > time.Duration(statsCollectionInterval*int64(time.Second))-1 {
			if rssBef, freeMemBef, needsgc := memMgr.needsGC(); needsgc {
				start := time.Now()
				debug.FreeOSMemory()
				elapsed := time.Since(start)

				// Refresh the RSS and FreeMem stats after GC
				memMgr.ProcessRSS()
				memMgr.ProcessFreeMem()

				// Get the updated stats
				rssAfter := GetRSS()
				freeMemAfter := GetMemFree()
				logging.Infof("MemManager::monitorMemUsage ForceGC Done. MemRSS -  Before: %v, after: %v. FreeMem - Before: %v, after: %v. Time Taken %v",
					rssBef, rssAfter, freeMemBef, freeMemAfter, elapsed)

				// TODO: Based on the memory statistics, enabling throttling
			}

			lastCollectionTime = time.Now()
		}
	}
}

func (memMgr *MemManager) needsGC() (uint64, uint64, bool) {
	memFree := GetMemFree()
	memTotal := GetMemTotal()
	memRSS := GetRSS()
	memMgr.recentSamples.Update(float64(memRSS))
	memMgr.olderSamples.Update(float64(memRSS))

	if memMgr.thresholdExceeded(memRSS, memFree, memTotal) {
		// Compute HeapIdle and HeapReleased and decide the need for
		// force GC based on HeapIdle and HeapReleased

		recentAvg := memMgr.recentSamples.Mean()
		olderAvg := memMgr.olderSamples.Mean()

		// On an average, there is a 10% increase in projector RSS in the last 4 samples
		// as compared to last 12 samples. Read memstats and forceGC if required
		if recentAvg > 1.1*olderAvg {

			runtime.ReadMemStats(memMgr.ms)
			heapIdle := memMgr.ms.HeapIdle
			heapReleased := memMgr.ms.HeapReleased
			heapAlloc := memMgr.ms.HeapAlloc

			// heapIdle is atleast 50% greater than heapReleased and
			// heapIdle-heapReleased i.e. the memory that can be released
			// to OS contributes atleast 30% to heapAlloc
			needsgc := heapIdle > uint64(1.5*float64(heapReleased)) && (heapIdle-heapReleased) > uint64(0.3*float64(heapAlloc))
			return memRSS, memFree, needsgc
		}
	}

	return memRSS, memFree, false
}

func (memMgr *MemManager) thresholdExceeded(memRSS, memFree, memTotal uint64) bool {
	usedMemThreshold := GetUsedMemThreshold()
	// At the configured RSS threshold, projector starts to
	// slow-down the DCP feeds. If the memory does not come
	// under control even after slowing down DCP feed's, only
	// then make an attempt to force a GC

	// TODO: Implement the slowing down of DCP feed part
	rssThreshold := GetRSSThreshold() + 0.06
	memUsed := memTotal - memFree

	return memUsed > uint64(usedMemThreshold*float64(memTotal)) && memRSS > uint64(rssThreshold*float64(memTotal))
}

func GetStatsCollectionInterval() int64 {
	return atomic.LoadInt64(&memMgr.statsCollectionInterval)
}

func SetStatsCollectionInterval(period int64) {
	atomic.StoreInt64(&memMgr.statsCollectionInterval, period)
	logging.Infof("MemManager::SetStatsCollectionInterval Updating stats collection interval to: %v seconds", period)
}

func GetUsedMemThreshold() float64 {
	val := atomic.LoadUint64(&memMgr.usedMemThreshold)
	return math.Float64frombits(val)
}

func SetUsedMemThreshold(f float64) {
	val := math.Float64bits(f)
	atomic.StoreUint64(&memMgr.usedMemThreshold, val)
	logging.Infof("MemManager::SetUsedMemThreshold Updating free mem threshold to: %v", f)
}

func GetRSSThreshold() float64 {
	val := atomic.LoadUint64(&memMgr.rssThreshold)
	return math.Float64frombits(val)
}

func SetRSSThreshold(f float64) {
	val := math.Float64bits(f)
	atomic.StoreUint64(&memMgr.rssThreshold, val)
	logging.Infof("MemManager::SetStatsCollectionInterval Updating RSS threshold to: %v", f)
}
