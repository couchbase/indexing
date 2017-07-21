package indexer

import (
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/system"
	"math"
	"sync/atomic"
	"time"
)

//////////////////////////////////////////////////////////////
// Global Variable
//////////////////////////////////////////////////////////////

var cpuPercent uint64

//////////////////////////////////////////////////////////////
// Concrete Type/Struct
//////////////////////////////////////////////////////////////

type cpuCollector struct {
	stats *system.SystemStats
}

//////////////////////////////////////////////////////////////
// Cpu Collector
//////////////////////////////////////////////////////////////

//
// Start Cpu collection
//
func StartCpuCollector() error {

	collector := &cpuCollector{}

	// open sigar for stats
	stats, err := system.NewSystemStats()
	if err != nil {
		logging.Errorf("Fail to start cpu stat collector. Err=%v", err)
		return err
	}
	collector.stats = stats

	// skip the first one
	collector.stats.ProcessCpuPercent()

	// start stats collection
	go collector.runCollectCpu()

	return nil
}

//
// Gather Cpu
//
func (c *cpuCollector) runCollectCpu() {

	//ticker := time.NewTicker(time.Second * 60)
	ticker := time.NewTicker(time.Second * 1)
	defer ticker.Stop()

	count := 0

	for range ticker.C {
		pid, percent, err := c.stats.ProcessCpuPercent()
		if err != nil {
			logging.Debugf("Fail to get cpu percentage. Err=%v", err)
			continue
		}
		count++

		if count > 10 {
			logging.Infof("cpuCollector: cpu percent %v for pid %v", percent, pid)
			count = 0
		}

		updateCpuPercent(percent)
	}
}

//////////////////////////////////////////////////////////////
// Global Function
//////////////////////////////////////////////////////////////

func updateCpuPercent(percent float64) {

	atomic.StoreUint64(&cpuPercent, math.Float64bits(percent))
}

func getCpuPercent() float64 {

	bits := atomic.LoadUint64(&cpuPercent)
	return math.Float64frombits(bits)
}
