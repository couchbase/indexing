package memmanager

import (
	"math"
	"runtime"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/projector/memThrottler"
	"github.com/couchbase/indexing/secondary/system"
)

var defaultUsedMemThreshold = 0.5
var defaultRSSThreshold = 0.1
var defaultRelaxGCThreshold = 0.01

// The configured value is updated based on projector.gogc config
// Defaulted at 100%
var configuredGCPercent uint64 = 100

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

	// For adjusting GC percent when RSS is too low
	currGCPercent      uint64
	lastGCAdjustedTime time.Time

	// Config related
	statsCollectionInterval int64
	usedMemThreshold        uint64
	rssThreshold            uint64
	relaxGCThreshold        uint64

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
	SetRelaxGCThreshold(defaultRelaxGCThreshold)

	memMgr.currGCPercent = configuredGCPercent

	memThrottler.Init()

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
		var currRSS, currFreeMem uint64

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

				currRSS, currFreeMem = rssAfter, freeMemAfter
			} else {
				currRSS, currFreeMem = rssBef, freeMemBef
			}

			throttleLevel := computeThrottleLevel(currRSS, currFreeMem, memMgr.memTotal)
			if throttleLevel > memThrottler.THROTTLE_LEVEL_10 {
				throttleLevel = memThrottler.THROTTLE_LEVEL_10
			}
			memThrottler.SetThrottleLevel(throttleLevel)

			lastCollectionTime = time.Now()

			memMgr.adjustGCPercent()
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

// This method will relax GC in those cases where the projector RSS
// is very less consistently and GC has been running frequently due
// to the garbage generated.
//
// This method is very pessimistic when adjusting GC percent. It will
// increase GC percent only when the maximum value of RSS observed
// was less than thresholds during the last 60 seconds. Even if a
// single spike in RSS with RSS >2% is observed, this method would
// re-adjust the GC to the default value of 100%
func (memMgr *MemManager) adjustGCPercent() {
	// Get max value from olderSamples
	max := memMgr.olderSamples.Max()
	lowThreshold := GetRelaxGCThreshold()

	lowThrMem := math.Max(200*1024*1024, lowThreshold*float64(memMgr.memTotal))
	highThrMem := math.Max(400*1024*1024, 2*lowThreshold*float64(memMgr.memTotal))

	gcConfigured := atomic.LoadUint64(&configuredGCPercent)

	// Tuning of GC percent is disabled. Reset to default and return
	if lowThreshold == 0 {
		if atomic.LoadUint64(&memMgr.currGCPercent) != gcConfigured {
			debug.SetGCPercent(int(gcConfigured))
			atomic.StoreUint64(&memMgr.currGCPercent, uint64(gcConfigured))
			logging.Infof("MemMgr::adjustGCPercent Setting projector GC percent at: %v", gcConfigured)
		}
		return
	}

	gcPercent := GetGCPercent() // Current GC percent

	// This check happens every 60 seconds
	if (time.Since(memMgr.lastGCAdjustedTime) > 60*time.Second) && (max < lowThrMem) {
		switch {
		case max < (lowThrMem / 4):
			gcPercent = 4 * gcConfigured
		case max < (lowThrMem / 2):
			gcPercent = 3 * gcConfigured
		case max < lowThrMem:
			gcPercent = 2 * gcConfigured
		}
		memMgr.lastGCAdjustedTime = time.Now()
	} else if max > highThrMem { // This check happens every 5 sec
		gcPercent = gcConfigured // Reset to configured value
	}

	if atomic.LoadUint64(&memMgr.currGCPercent) != uint64(gcPercent) {
		debug.SetGCPercent(int(gcPercent))
		atomic.StoreUint64(&memMgr.currGCPercent, uint64(gcPercent))
		logging.Infof("MemMgr::adjustGCPercent Setting projector GC percent at: %v", gcPercent)
	}
}

func computeThrottleLevel(memRSS, memFree, memTotal uint64) int {
	// RSS after which projector starts to throttle - With default
	// settings, this would be when projector RSS exceeds 10% of memTotal
	memUsed := memTotal - memFree
	usedMemThreshold := GetUsedMemThreshold()
	if memUsed < uint64(usedMemThreshold*float64(memTotal)) {
		return memThrottler.THROTTLE_NONE
	}
	baseThrottleRSS := uint64(GetRSSThreshold() * float64(memTotal))
	rssDiffPercentage := 0.02 // Throttle level will be adjusted for every 2% change in RSS

	if memRSS > baseThrottleRSS {
		level := int(float64(memRSS-baseThrottleRSS)/(rssDiffPercentage*float64(memTotal))) + 1
		return level
	}
	return memThrottler.THROTTLE_NONE
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
	logging.Infof("MemManager::SetRSSThreshold Updating RSS threshold to: %v", f)
}

func SetRelaxGCThreshold(f float64) {
	val := math.Float64bits(f)
	atomic.StoreUint64(&memMgr.relaxGCThreshold, val)
	logging.Infof("MemManager::SetRelaxGCThreshold Updating Relax GC threshold to: %v", f)
}

func GetRelaxGCThreshold() float64 {
	val := atomic.LoadUint64(&memMgr.relaxGCThreshold)
	return math.Float64frombits(val)
}

func GetGCPercent() uint64 {
	return atomic.LoadUint64(&memMgr.currGCPercent)
}

func SetDefaultGCPercent(gc int) {
	atomic.StoreUint64(&configuredGCPercent, uint64(gc))
}
