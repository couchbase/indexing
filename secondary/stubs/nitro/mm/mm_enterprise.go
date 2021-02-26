// +build !community

package mm

import (
	ee "github.com/couchbase/nitro/mm"
)

var Malloc = ee.Malloc
var Free = ee.Free

var Debug = &ee.Debug

func FreeOSMemory() {
	ee.FreeOSMemory()
}

func Size() uint64 {
	return ee.Size()
}

func AllocSize() uint64 {
	return ee.AllocSize()
}

func Stats() string {
	return ee.Stats()
}

func GetAllocStats() (uint64, uint64) {
	return ee.GetAllocStats()
}
