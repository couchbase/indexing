package memmanager

import (
	"math"
	"sync/atomic"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
)

//
// Gather system stats
//
func (mgr *MemManager) runStatsCollection() {

	// Run ticker at a granularity of 1 second but collect
	// stats at a granularity defined by "period"
	ticker := time.NewTicker(time.Second * 1)
	defer ticker.Stop()

	var lastCollectionTime time.Time

	count := 0

	for range ticker.C {
		statsCollectionInterval := GetStatsCollectionInterval()
		if time.Since(lastCollectionTime) > time.Duration(statsCollectionInterval*int64(time.Second))-1 {
			pid, cpu, err := mgr.stats.ProcessCpuPercent()
			if err != nil {
				logging.Debugf("Fail to get cpu percentage. Err=%v", err)
				continue
			}
			mgr.updateCpuPercent(cpu)

			_, rss, err := mgr.stats.ProcessRSS()
			if err != nil {
				logging.Debugf("Fail to get RSS. Err=%v", err)
				continue
			}
			mgr.updateRSS(rss)

			total, free, cgroupValues, err := mgr.stats.GetTotalAndFreeMem(true, common.SIGAR_CGROUP_SUPPORTED)
			if err != nil {
				logging.Debugf("Fail to get total and free mem. Err=%v", err)
				continue
			}

			mgr.updateMemTotal(total)
			mgr.updateMemFree(free)

			count++
			if count > 10 {
				logging.Debugf("cpuCollector: cpu percent %v for pid %v", cpu, pid)
				logging.Debugf("cpuCollector: RSS %v for pid %v", rss, pid)
				if cgroupValues {
					logging.Debugf("cpuCollector[cGroup]: memory total %v", total)
					logging.Debugf("cpuCollector[cGroup]: memory free %v", free)
				} else {
					logging.Debugf("cpuCollector[system]: memory total %v", total)
					logging.Debugf("cpuCollector[system]: memory free %v", free)
				}
				count = 0
			}
			lastCollectionTime = time.Now()
		}
	}
}

//////////////////////////////////////////////////////////////
// Global Function
//////////////////////////////////////////////////////////////

func (mgr *MemManager) updateCpuPercent(cpu float64) {

	atomic.StoreUint64(&mgr.cpuPercent, math.Float64bits(cpu))
}

func GetCpuPercent() float64 {
	bits := atomic.LoadUint64(&memMgr.cpuPercent)
	return math.Float64frombits(bits)
}

func (mgr *MemManager) updateRSS(mem uint64) {

	atomic.StoreUint64(&mgr.rss, mem)
}

func GetRSS() uint64 {

	return atomic.LoadUint64(&memMgr.rss)
}

func (mgr *MemManager) updateMemTotal(mem uint64) {

	atomic.StoreUint64(&mgr.memTotal, mem)
}

func GetMemTotal() uint64 {

	return atomic.LoadUint64(&memMgr.memTotal)
}

func (mgr *MemManager) updateMemFree(mem uint64) {

	atomic.StoreUint64(&mgr.memFree, mem)
}

func GetMemFree() uint64 {
	return atomic.LoadUint64(&memMgr.memFree)
}

func (mgr *MemManager) ProcessCpuPercent() error {
	_, cpu, err := mgr.stats.ProcessCpuPercent()
	if err != nil {
		logging.Debugf("Fail to get cpu percentage. Err=%v", err)
		return err
	}
	mgr.updateCpuPercent(cpu)
	return nil
}

func (mgr *MemManager) ProcessRSS() error {
	_, rss, err := mgr.stats.ProcessRSS()
	if err != nil {
		logging.Debugf("Fail to get RSS. Err=%v", err)
		return err
	}
	mgr.updateRSS(rss)
	return nil
}

func (mgr *MemManager) ProcessTotalAndFreeMem() error {
	total, free, _, err := mgr.stats.GetTotalAndFreeMem(true, common.SIGAR_CGROUP_SUPPORTED)
	if err != nil {
		logging.Debugf("Fail to get RSS. Err=%v", err)
		return err
	}
	mgr.updateMemTotal(total)
	mgr.updateMemFree(free)
	return nil
}

func (mgr *MemManager) ProcessFreeMem() error {
	free, err := mgr.stats.SystemActualFreeMem()
	if err != nil {
		logging.Debugf("Fail to get free memory. Err=%v", err)
		return err
	}
	mgr.updateMemFree(free)
	return nil
}

func (mgr *MemManager) ProcessTotalMem() error {
	total, free, _, err := memMgr.stats.GetTotalAndFreeMem(true, common.SIGAR_CGROUP_SUPPORTED)
	if err != nil {
		logging.Debugf("Fail to get total and free memory. Err=%v", err)
		return err
	}
	memMgr.updateMemTotal(total)
	memMgr.updateMemFree(free)
	return nil
}
