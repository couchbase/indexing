// @copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package indexer

import (
	"os"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/system"

	"github.com/couchbase/indexing/secondary/vector/codebook"
)

const MAX_THROTTLE_ADJUST_MS float64 = 5.0 // max msec to adjust throttleDelayMs by at one time
const MAX_THROTTLE_DELAY_MS int64 = 10     // max msec to delay an action due to throttling
// (prevents runaway throttling)
const NUM_CPU_STATS int = 2 // number of past CPU stats to keep

// CpuThrottle is a class to provide a CPU throttling delay to other components. When throttling
// is enabled it periodically collects CPU stats, computes the most recent CPU usage, and adjusts
// the throttle delay. Other components call GetActiveThrottleDelayMs to get the most current delay.
// This will be 0, and CPU usage collection will not be running, if throttling is disabled.
// This class is originally implemented for the Autofailover feature but is completely generic.
// Indexer creates one singleton child instance of this class.
type CpuThrottle struct {
	cpuTarget          float64Holder  // [0.50, 1.00] CPU use target when throttling is enabled
	cpuThrottling      unsafe.Pointer // *int32 treated as a boolean; is throttling enabled?
	stopCh             chan struct{}  // close to stop the runThrottling goroutine
	throttleDelayMs    unsafe.Pointer // *int64 ms to delay an action when CPU throttling is enabled
	throttleStateMutex sync.Mutex     // sync throttling enabled/disabled state changes, incl stopCh

	// getCurrentCpuUsageStd's circular buffer of past CPU stats and index of next one to (re)use
	cpuStatsStd    [NUM_CPU_STATS]*system.SigarCpuT // CPU ticks in categories since sigar start
	cpuStatsStdIdx int                              // next index into cpuStatsStd

	// getCurrentCpuUsageCgroup's circular buffer of past CPU stats and index of next one to (re)use
	cpuStatsCgroup    [NUM_CPU_STATS]*cgroupCpuStats // CPU usage stats when cgroup is supported
	cpuStatsCgroupIdx int                            // next index into cpuStatsCgroup
}

// cgroupCpuStats holds CPU usage stats in the case where cgroups are supported.
type cgroupCpuStats struct {
	usageUsec     uint64 // microsecs of CPU usage over used cores since sigar start
	timeUnixMicro int64  // approximate Unix epoch microsecond timestamp of usageUsec retrieval
}

// NewCpuThrottle is the constructor for the CpuThrottle class. It returns an instance with
// throttling currently disabled.
func NewCpuThrottle(cpuTarget float64) *CpuThrottle {
	cpuThrottle := CpuThrottle{}

	cpuThrottle.cpuTarget.SetFloat64(1.00) // create valid ptr before SetCpuTarget reads it
	cpuThrottle.SetCpuTarget(cpuTarget)

	var cpuThrottling int32 = 0 // disabled initially
	cpuThrottle.cpuThrottling = (unsafe.Pointer)(&cpuThrottling)

	var throttleDelayMs int64 = 0
	cpuThrottle.throttleDelayMs = (unsafe.Pointer)(&throttleDelayMs)

	return &cpuThrottle
}

// float64Holder is used to hold float64s under an atomic pointer, since the sync/atomic library
// does not provide native Load/Store methods for float64.
type float64Holder struct {
	ptr unsafe.Pointer
}

func (this *float64Holder) GetFloat64() float64 {
	return *(*float64)(atomic.LoadPointer(&this.ptr))
}
func (this *float64Holder) SetFloat64(value float64) {
	atomic.StorePointer(&this.ptr, unsafe.Pointer(&value))
}

// getCpuThrottling returns whether CPU throttling is currently enabled.
func (this *CpuThrottle) getCpuThrottling() bool {
	return 0 != (int32)(atomic.LoadInt32((*int32)(this.cpuThrottling)))
}

// SetCpuThrottling sets whether CPU throttling is currently enabled. It also starts throttling if
// changing from disabled to enabled and stops it if changing from enabled to disabled. Throttling
// is off if cpuTarget == 1.00 even if cpuThrottling was passed in as true.
func (this *CpuThrottle) SetCpuThrottling(cpuThrottling bool) {
	this.throttleStateMutex.Lock()
	defer this.throttleStateMutex.Unlock()

	priorCpuThrottling := this.getCpuThrottling()
	if this.getCpuTarget() == 1.00 { // throttling is really off if cpuTarget is 1.00
		cpuThrottling = false
	}
	var cpuThrottlingInt32 int32 = 0 // convert cpuThrottling bool to int32
	if cpuThrottling {
		cpuThrottlingInt32 = 1
	}
	atomic.StoreInt32((*int32)(this.cpuThrottling), cpuThrottlingInt32)

	// Start/stop the runThrottling goroutine if the enabled/disabled state changed
	if !priorCpuThrottling && cpuThrottling { // start
		this.stopCh = make(chan struct{})
		go this.runThrottling(this.stopCh)
	} else if priorCpuThrottling && !cpuThrottling { // stop
		close(this.stopCh)
		// Reset vector throttle delay when throttling is disabled
		this.resetVectorThrottleDelay()
	}
}

// getCpuTarget returns the current cpuTarget value, which throttling aims to
// prevent CPU usage from rising above.
func (this *CpuThrottle) getCpuTarget() float64 {
	return this.cpuTarget.GetFloat64()
}

// SetCpuTarget sets the CPU usage target in [0.50, 1.00] for throttling. Any value outside this
// range is ignored.
func (this *CpuThrottle) SetCpuTarget(cpuTarget float64) {
	const method_SetCpuTarget = "CpuThrottle::SetCpuTarget:" // for logging

	if cpuTarget < 0.50 || cpuTarget > 1.00 {
		logging.Errorf("%v Invalid cpuTarget: %v ignored", method_SetCpuTarget, cpuTarget)
		return
	}
	cpuTargetOld := this.getCpuTarget()
	if cpuTarget != cpuTargetOld {
		logging.Infof("%v New cpuTarget: %v", method_SetCpuTarget, cpuTarget)
		this.cpuTarget.SetFloat64(cpuTarget)
	}
}

// GetActiveThrottleDelayMs returns the number of milliseconds of throttling delay currently active.
// If throttling is disabled this will always return 0, even though the value currently stored in
// this object may be different, because throttling is not active in that case. (getThrottleDelayMs
// gets the currently stored value, regardless of whether throttling is active.)
func (this *CpuThrottle) GetActiveThrottleDelayMs() int64 {
	if !this.getCpuThrottling() {
		return 0
	}
	return this.getThrottleDelayMs()
}

// getThrottleDelayMs returns the current raw throttling delay in number of milliseconds.
// GetActiveThrottleDelayMs returns the active delay, which will always be 0 if throttling is
// disabled even though the raw value returned by this function may be non-0.
func (this *CpuThrottle) getThrottleDelayMs() int64 {
	return (int64)(atomic.LoadInt64((*int64)(this.throttleDelayMs)))
}

// setThrottleDelayMs sets the current raw throttling delay in number of milliseconds.
// GetActiveThrottleDelayMs returns the active delay, which will always be 0 if throttling is
// disabled even though the raw value set by this function may be non-0.
func (this *CpuThrottle) setThrottleDelayMs(throttleDelayMs int64) {
	atomic.StoreInt64((*int64)(this.throttleDelayMs), throttleDelayMs)
}

// runThrottling runs as a goroutine when throttling is enabled. It periodically gets CPU usage
// stats and adjusts this.throttleDelayMs. When throttling is disabled this routine is not running.
// stopCh is passed in so this routine does not access this.stopCh which gets changed by restarts.
func (this *CpuThrottle) runThrottling(stopCh chan struct{}) {
	const method string = "CpuThrottle::runThrottling:" // for logging

	this.setThrottleDelayMs(0) // always start with 0 delay
	// Reset vector throttle delay when throttling starts
	this.resetVectorThrottleDelay()
	logging.Infof("%v Starting. cpuTarget: %v, throttleDelayMs: %v", method,
		this.getCpuTarget(), this.getThrottleDelayMs())

	// Start a sigar process for CPU stats
	os.Unsetenv("COUCHBASE_CPU_COUNT") // so sigar will return cgroups num_cpu_prc in cgroups case
	systemStats, err := system.NewSystemStats()
	if err != nil {
		logging.Infof("%v Failed to start: NewSystemStats returned error: %v", method, err)
		return
	}
	defer systemStats.Close()

	// Adjustment main loop. New CPU usage counters are only flushed into sigar visibility every 1 s
	// (from empirical experiment) so there is no sense adjusting the throttle more often than that.
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-stopCh:
			// Clear out cpuStatsStd and cpuStatsCgroup so a later restart does not use old garbage
			for idx := 0; idx < NUM_CPU_STATS; idx++ {
				this.cpuStatsStd[idx] = nil
				this.cpuStatsCgroup[idx] = nil
			}
			logging.Infof("%v Shutting down.", method)
			return

		case <-ticker.C:
			this.adjustThrottleDelay(systemStats)
		}
	}
}

// adjustThrottleDelay perform a single increase/decrease of this.throttleDelayMs based on current
// CPU usage vs this.cpuTarget.
func (this *CpuThrottle) adjustThrottleDelay(systemStats *system.SystemStats) {
	const method string = "CpuThrottle::adjustThrottleDelay:" // for logging

	currCpu := this.getCurrentCpuUsage(systemStats)
	if currCpu == 0.0 { // CPU stats did not update in the current check window
		return
	}
	cpuTarget := this.getCpuTarget()
	normalizer := 1.00 - cpuTarget // for linearly interpolating throttle adjustment amount
	diff := currCpu - cpuTarget    // current distance +/- from the target
	if diff < -normalizer {        // undershoots interpolation range; (overshoot can't occur)
		diff = -normalizer // cap diff at the bottom of the interpolation range
	}
	var adjustmentMs float64
	if normalizer != 0.0 {
		adjustmentMs = MAX_THROTTLE_ADJUST_MS * diff / normalizer
	} else if diff < 0.0 { // cpuTarget == 1.0, diff must be <= 0.0; avoid divide-by-zero "normalization"
		adjustmentMs = -MAX_THROTTLE_ADJUST_MS
	}
	var rounder float64 // to round instead of truncate adjustments
	if adjustmentMs >= 0 {
		rounder = 0.5
	} else {
		rounder = -0.5
	}
	throttleDelayMs := this.getThrottleDelayMs()
	newThrottleDelayMs := throttleDelayMs + (int64)(adjustmentMs+rounder)
	if newThrottleDelayMs > MAX_THROTTLE_DELAY_MS {
		newThrottleDelayMs = MAX_THROTTLE_DELAY_MS
	} else if newThrottleDelayMs < 0 {
		newThrottleDelayMs = 0
	}
	this.setThrottleDelayMs(newThrottleDelayMs)

	// Also adjust vector throttle delay to 1/10th of regular throttle delay, using microseconds for better granularity
	currentThrottleDelay := codebook.GetThrottleDelay()
	// Convert to microseconds, divide by 10, preserving granularity
	vectorThrottleDelayUs := (newThrottleDelayMs * 1000) / 10
	if currentThrottleDelay != vectorThrottleDelayUs {
		codebook.SetThrottleDelay(vectorThrottleDelayUs)
	}

	if newThrottleDelayMs != throttleDelayMs {
		logging.Infof("%v Adjusted throttle. cpuTarget: %v, currCpu: %v,"+
			" throttleDelayMs (new, old, change): (%v, %v, %v)",
			method, cpuTarget, currCpu,
			newThrottleDelayMs, throttleDelayMs, (newThrottleDelayMs - throttleDelayMs))
	} else if newThrottleDelayMs == MAX_THROTTLE_DELAY_MS {
		logging.Warnf("%v throttleDelayMs is at maximum %v. cpuTarget: %v, currCpu: %v",
			method, MAX_THROTTLE_DELAY_MS, cpuTarget, currCpu)
	}
}

// getCurrentCpuUsage gets the latest CPU usage stats from sigar, diffs them with the oldest stats,
// and returns the result as a value in range [0.0, 1.0] regardless of number of cores. If cgroups
// (Linux control groups) are supported, the stats will be from the newer cgroups implementation in
// sigar, else they will be from the older tick-based implementation ("Std" for "standard").
func (this *CpuThrottle) getCurrentCpuUsage(systemStats *system.SystemStats) float64 {
	cgroupsSupported, cpuUse := this.getCurrentCpuUsageCgroup(systemStats)
	if !cgroupsSupported {
		cpuUse = this.getCurrentCpuUsageStd(systemStats)
	}
	return cpuUse
}

// getCurrentCpuUsageStd gets the latest CPU usage stats from sigar when cgroups are not supported,
// diffs them with the oldest stats, and returns the result as a value in range [0.0, 1.0]
// regardless of number of cores. The fields counted as CPU "in use" are Sys + User + Nice + Irq +
// SoftIrq. (This is different from the sigar_cpu_perc_calculate function's perc.combined
// calculation, whose semantics are unclear.)
func (this *CpuThrottle) getCurrentCpuUsageStd(systemStats *system.SystemStats) float64 {
	const _getCurrentCpuUsageStd string = "CpuThrottle::getCurrentCpuUsageStd:"

	// Get new stats and update the circular stats buffer
	cpuStatsNew, err := systemStats.SigarCpuGet()
	if err != nil {
		logging.Infof("%v SigarCpuGet returned error: %v", _getCurrentCpuUsageStd, err)
		return 0.0
	}
	cpuStatsOld := this.cpuStatsStd[this.cpuStatsStdIdx] // oldest stats in circular buffer
	this.cpuStatsStd[this.cpuStatsStdIdx] = cpuStatsNew
	this.cpuStatsStdIdx = (this.cpuStatsStdIdx + 1) % NUM_CPU_STATS

	if cpuStatsOld == nil { // have not wrapped around yet
		return 0.0
	}

	// Calculate current CPU usage. Total field might not always agree with sum of the individual
	// counters as they are flushed from the OS to sigar visibility every 1 sec but one at a time
	// spread over a period of about 50 ms (from empirical experiments), so sum things ourselves.
	// IGNORE Stolen time as this is for other VMs and would inject garbage into the calculation.
	cpuUseNew := cpuStatsNew.Sys + cpuStatsNew.User + cpuStatsNew.Nice +
		cpuStatsNew.Irq + cpuStatsNew.SoftIrq
	cpuUseOld := cpuStatsOld.Sys + cpuStatsOld.User + cpuStatsOld.Nice +
		cpuStatsOld.Irq + cpuStatsOld.SoftIrq
	cpuTotalNew := cpuUseNew + cpuStatsNew.Wait + cpuStatsNew.Idle
	cpuTotalOld := cpuUseOld + cpuStatsOld.Wait + cpuStatsOld.Idle
	deltaTime := cpuTotalNew - cpuTotalOld
	if deltaTime == 0 {
		return 0.0
	}
	return float64(cpuUseNew-cpuUseOld) / float64(deltaTime)
}

// getCurrentCpuUsageCgroup gets the latest CPU usage stats from sigar when cgroups are supported,
// diffs them with the oldest stats, and returns the result as a value in range [0.0, 1.0]
// regardless of number of cores. The bool return value is true if cgroups are supported, false
// otherwise. If false, the float64 result is garbage and the caller should switch to
// getCurrentCpuUsageStd instead.
func (this *CpuThrottle) getCurrentCpuUsageCgroup(systemStats *system.SystemStats) (bool, float64) {
	cgroupInfo := systemStats.GetControlGroupInfo()
	if cgroupInfo.Supported != common.SIGAR_CGROUP_SUPPORTED {
		return false, 0.0
	}
	timeNewUsec := (time.Now().UnixNano() + 500) / 1000 // rounded to nearest microsecond

	cpuStatsNew := &cgroupCpuStats{
		usageUsec:     cgroupInfo.UsageUsec,
		timeUnixMicro: timeNewUsec,
	}
	cpuStatsOld := this.cpuStatsCgroup[this.cpuStatsCgroupIdx] // oldest stats in circular buffer
	this.cpuStatsCgroup[this.cpuStatsCgroupIdx] = cpuStatsNew
	this.cpuStatsCgroupIdx = (this.cpuStatsCgroupIdx + 1) % NUM_CPU_STATS

	if cpuStatsOld == nil || // have not wrapped around yet
		cgroupInfo.NumCpuPrc <= 0 { // bad CPU percentage; should not occur
		return true, 0.0
	}

	// Calculate current CPU usage
	maxPotentialUsage := float64(cpuStatsNew.timeUnixMicro-cpuStatsOld.timeUnixMicro) *
		(float64(cgroupInfo.NumCpuPrc) / 100.0)
	if maxPotentialUsage <= 0.0 { // could be 0.0 if no time elapsed or < 0.0 if clock changed
		return true, 0.0
	}

	actualUsage := float64(cpuStatsNew.usageUsec - cpuStatsOld.usageUsec)
	cpuUseNew := actualUsage / maxPotentialUsage
	if cpuUseNew > 1.0 { // can occur due to time rounding/slop and if cgroupInfo.NumCpuPrc changes
		cpuUseNew = 1.0
	}
	if cpuUseNew < 0.0 { // for safety; should not occur
		cpuUseNew = 0.0
	}
	return true, cpuUseNew
}

// resetVectorThrottleDelay resets the vector throttle delay to zero.
// This should be called when throttling is disabled to remove throttling delay.
func (this *CpuThrottle) resetVectorThrottleDelay() {
	codebook.SetThrottleDelay(0)
	logging.Infof("CpuThrottle::resetVectorThrottleDelay: Reset vector throttle delay to 0")
}
