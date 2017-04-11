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

var cpuPercentAvg uint64

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

	for range ticker.C {
		percent, err := c.stats.ProcessCpuPercent()
		if err != nil {
			logging.Debugf("Fail to get CPU. Err=%v", err)
			continue
		}

		updateCpuPercentAverage(percent)
	}
}

//////////////////////////////////////////////////////////////
// Global Function
//////////////////////////////////////////////////////////////

func updateCpuPercentAverage(percent float64) {

	newAvg := (getCpuPercentAverage() + percent) / 2
	atomic.StoreUint64(&cpuPercentAvg, math.Float64bits(newAvg))
}

func getCpuPercentAverage() float64 {

	bits := atomic.LoadUint64(&cpuPercentAvg)
	return math.Float64frombits(bits)
}
