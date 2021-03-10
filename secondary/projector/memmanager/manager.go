package memmanager

import (
	"sync/atomic"

	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/system"
)

var memMgr *MemManager // Global variable. Initialized by projector

type MemManager struct {
	rss        uint64
	memTotal   uint64
	memFree    uint64
	cpuPercent uint64

	statsCollectionInterval int64

	stats *system.SystemStats
}

func Init(statsCollectionInterval int64) error {
	memMgr = &MemManager{}

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

	// start stats collection
	go memMgr.runStatsCollection()

	return nil
}

func SetStatsCollectionInterval(period int64) {
	atomic.StoreInt64(&memMgr.statsCollectionInterval, period)
	logging.Infof("MemManager::SetStatsCollectionInterval Updating stats collection interval to: %v seconds", period)
}
