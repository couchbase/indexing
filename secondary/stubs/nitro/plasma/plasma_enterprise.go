// +build enterprise

package plasma

import (
	ee "github.com/couchbase/nitro/plasma"
)

var Diag = &ee.Diag

func UsePlasma() bool {
	return true
}

func SetMemoryQuota(sz int64) {
	ee.SetMemoryQuota(sz)
}

func SetLogReclaimBlockSize(sz int64) {
	ee.SetLogReclaimBlockSize(sz)
}

func MemoryInUse() int64 {
	return ee.MemoryInUse()
}
