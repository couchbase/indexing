//go:build !community
// +build !community

package plasma

import (
	ee "github.com/couchbase/plasma"
)

var Diag = &ee.Diag

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
	return ee.MemoryInUse()
}

func TenantQuotaNeeded() int64 {
	return ee.TenantQuotaMandatory()
}
