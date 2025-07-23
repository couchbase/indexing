//go:build !community
// +build !community

package plasma

import (
	ee "github.com/couchbase/plasma"
	"time"
)

var Diag = &ee.Diag

type MemTunerConfig = ee.MemTunerConfig
type MemTunerDistStats = ee.MemTunerDistStats

func SetMemoryQuota(sz int64, force bool) {
	ee.SetMemoryQuota2(sz, force)
}

func GetMandatoryQuota() (int64, int64) {
	return ee.GetMandatoryQuota()
}

func GetWorkingSetSize() int64 {
	return ee.GetWorkingSetSize()
}

func SetLogReclaimBlockSize(sz int64) {
	ee.SetLogReclaimBlockSize(sz)
}

func MemoryInUse() int64 {
	return ee.MemoryInUse() + ee.GetFlushBufferMemoryAllocated()
}

func GolangMemoryInUse() int64 {
	return ee.GetFlushBufferMemoryAllocated()
}

func TenantQuotaNeeded() int64 {
	return ee.TenantQuotaMandatory()
}

func MakeMemTunerConfig(qsp, mqt, mqdd, qmsp int64) MemTunerConfig {
	return MemTunerConfig{
		QuotaSplitPercent:    qsp,
		MinQuotaThreshold:    mqt,
		MinQuotaDecayDur:     mqdd,
		QuotaMaxShiftPercent: qmsp,
	}
}

func MakeMemTunerDistStats(nb, plbp, blbp int64, plct, blct time.Time) MemTunerDistStats {
	return MemTunerDistStats{
		NumBhives:       nb,
		PLowestBP:       plbp,
		BLowestBP:       blbp,
		PLastCreateTime: plct,
		BLastCreateTime: blct,
	}
}

func RunMemQuotaTuner(
	quotaDistCh chan bool,
	getAssignedQuota func() int64,
	getConfig func() MemTunerConfig,
	getDistStats func() MemTunerDistStats,
) {
	ee.RunMemQuotaTuner(quotaDistCh, getAssignedQuota, getConfig, getDistStats)
}
