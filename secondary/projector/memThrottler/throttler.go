package memThrottler

import (
	"sync/atomic"
	"time"

	"github.com/couchbase/indexing/secondary/logging"
)

// More number of throttle levels allow for smoother control over RSS
const (
	THROTTLE_NONE = iota
	THROTTLE_LEVEL_1
	THROTTLE_LEVEL_2
	THROTTLE_LEVEL_3
	THROTTLE_LEVEL_4
	THROTTLE_LEVEL_5
	THROTTLE_LEVEL_6
	THROTTLE_LEVEL_7
	THROTTLE_LEVEL_8
	THROTTLE_LEVEL_9
	THROTTLE_LEVEL_10
)

const (
	SLOWDOWN_LEVEL_1  = 100    // 100 microseconds
	SLOWDOWN_LEVEL_2  = 300    // 300 microseconds
	SLOWDOWN_LEVEL_3  = 500    // 500 microseconds
	SLOWDOWN_LEVEL_4  = 1000   // 1 millisecond
	SLOWDOWN_LEVEL_5  = 3000   // 3 milliseconds
	SLOWDOWN_LEVEL_6  = 5000   // 5 milliseconds
	SLOWDOWN_LEVEL_7  = 10000  // 10 milliseconds
	SLOWDOWN_LEVEL_8  = 30000  // 30 milliseconds
	SLOWDOWN_LEVEL_9  = 50000  // 50 milliseconds
	SLOWDOWN_LEVEL_10 = 100000 // 100 milliseconds
)

type MemThrottler struct {
	isMemThrottlingEnabled            int32 // 0 -> Disable, 1 (default) -> Enable mem throttling
	isMaintStreamMemThrottlingEnabled int32 // 0 -> Disable, 1 (default) -> Enable throttling of MAINT_STREAM

	throttleLevel int32
}

var memThrottler *MemThrottler

func Init() {
	memThrottler = &MemThrottler{
		isMemThrottlingEnabled:            1,
		isMaintStreamMemThrottlingEnabled: 1,
		throttleLevel:                     THROTTLE_NONE,
	}
}

func DoThrottle(isMaintStream bool) {
	if memThrottler == nil || IsMemThrottlingEnabled() == 0 {
		return
	} else if isMaintStream && IsMaintStreamMemThrottlingEnabled() == 0 {
		return
	}

	tl := GetThrottleLevel()

	if isMaintStream == false { // For all INIT streams
		switch tl {
		case THROTTLE_NONE:
			return
		case THROTTLE_LEVEL_1:
			time.Sleep(SLOWDOWN_LEVEL_1 * time.Microsecond)
		case THROTTLE_LEVEL_2:
			time.Sleep(SLOWDOWN_LEVEL_2 * time.Microsecond)
		case THROTTLE_LEVEL_3:
			time.Sleep(SLOWDOWN_LEVEL_3 * time.Microsecond)
		case THROTTLE_LEVEL_4:
			time.Sleep(SLOWDOWN_LEVEL_4 * time.Microsecond)
		case THROTTLE_LEVEL_5:
			time.Sleep(SLOWDOWN_LEVEL_5 * time.Microsecond)
		case THROTTLE_LEVEL_6:
			time.Sleep(SLOWDOWN_LEVEL_6 * time.Microsecond)
		case THROTTLE_LEVEL_7:
			time.Sleep(SLOWDOWN_LEVEL_7 * time.Microsecond)
		case THROTTLE_LEVEL_8:
			time.Sleep(SLOWDOWN_LEVEL_8 * time.Microsecond)
		case THROTTLE_LEVEL_9:
			time.Sleep(SLOWDOWN_LEVEL_9 * time.Microsecond)
		default:
			time.Sleep(SLOWDOWN_LEVEL_10 * time.Microsecond)
			return
		}
	} else {
		// As MAINT_STREAM operatios at bucket level, it processes
		// many mutations which may not require accountable memory
		// allocations. Hence, throttling for MAINT_STREAM is done
		// at a different rate as compared to INIT_STREAM.
		switch tl {
		case THROTTLE_NONE, THROTTLE_LEVEL_1, THROTTLE_LEVEL_2, THROTTLE_LEVEL_3:
			return
		case THROTTLE_LEVEL_4:
			time.Sleep(SLOWDOWN_LEVEL_1 * time.Microsecond)
		case THROTTLE_LEVEL_5:
			time.Sleep(SLOWDOWN_LEVEL_2 * time.Microsecond)
		case THROTTLE_LEVEL_6:
			time.Sleep(SLOWDOWN_LEVEL_3 * time.Microsecond)
		case THROTTLE_LEVEL_7:
			time.Sleep(SLOWDOWN_LEVEL_4 * time.Microsecond)
		case THROTTLE_LEVEL_8:
			time.Sleep(SLOWDOWN_LEVEL_5 * time.Microsecond)
		case THROTTLE_LEVEL_9:
			time.Sleep(SLOWDOWN_LEVEL_6 * time.Microsecond)
		default:
			time.Sleep(SLOWDOWN_LEVEL_7 * time.Microsecond)
			return
		}
	}
}

func GetThrottleLevel() int32 {
	return atomic.LoadInt32(&memThrottler.throttleLevel)
}

func SetThrottleLevel(tl int) {
	currLevel := GetThrottleLevel()
	if currLevel != int32(tl) {
		atomic.StoreInt32(&memThrottler.throttleLevel, int32(tl))
		logging.Infof("MemThrottler::SetThrottleLevel Throttling level set to: %v", tl)
	}
}

func IsMemThrottlingEnabled() int32 {
	return atomic.LoadInt32(&memThrottler.isMemThrottlingEnabled)
}

func SetMemThrottle(val bool) {
	if val {
		atomic.StoreInt32(&memThrottler.isMemThrottlingEnabled, 1)
		logging.Infof("MemManager::SetMemThrottle Enable memory throttling")
	} else {
		atomic.StoreInt32(&memThrottler.isMemThrottlingEnabled, 0)
		logging.Infof("MemManager::SetMemThrottle Disabling memory throttling")
	}
}

func SetMaintStreamMemThrottle(val bool) {
	if val {
		atomic.StoreInt32(&memThrottler.isMaintStreamMemThrottlingEnabled, 1)
		logging.Infof("MemManager::SetMaintStreamMemThrottle Enabling memory throttling for MAINT_STREAM")
	} else {
		if val {
			atomic.StoreInt32(&memThrottler.isMaintStreamMemThrottlingEnabled, 0)
			logging.Infof("MemManager::SetMaintStreamMemThrottle Disabling memory throttling for MAINT_STREAM")
		}
	}
}

func IsMaintStreamMemThrottlingEnabled() int32 {
	return atomic.LoadInt32(&memThrottler.isMaintStreamMemThrottlingEnabled)
}
