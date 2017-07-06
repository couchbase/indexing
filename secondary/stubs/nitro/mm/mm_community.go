// +build !enterprise

package mm

import "github.com/couchbase/indexing/secondary/memdb/skiplist"

var Malloc skiplist.MallocFn
var Free skiplist.FreeFn

var Debug *bool = &[]bool{false}[0]

func FreeOSMemory() {
}

func Size() uint64 {
	return 0
}

func Stats() string {
	return ""
}
