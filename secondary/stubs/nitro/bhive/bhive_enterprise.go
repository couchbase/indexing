//go:build !community
// +build !community

package bhive

import (
	ee "github.com/couchbase/bhive"
)

var Diag = &ee.Diag

func SetMemoryQuota(sz int64) {
	ee.SetMemoryQuota(uint64(sz))
}

func GetMandatoryQuota() (int64, int64) {
	return ee.GetMandatoryQuota()
}

func GetWorkingSetSize() int64 {
	return ee.GetWorkingSetSize()
}

// MemoryInUse includes BufferMemoryInUse
func MemoryInUse() int64 {
	return int64(ee.GetMemoryUsage())
}

func BufferMemoryInUse() int64 {
	return int64(ee.GetBufferMemoryUsage())
}
