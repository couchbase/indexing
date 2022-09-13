// +build !community

package plasma

import (
	ee "github.com/couchbase/plasma"
)

var Diag = &ee.Diag

func SetMemoryQuota(sz int64) {
	ee.SetMemoryQuota(sz)
}

func SetLogReclaimBlockSize(sz int64) {
	ee.SetLogReclaimBlockSize(sz)
}

func MemoryInUse() int64 {
	return ee.MemoryInUse()
}

func TenantQuotaNeeded() int64 {
	return ee.TenantQuotaNeeded()
}
