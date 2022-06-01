// +build community

package mm

import (
	"errors"

	"github.com/couchbase/indexing/secondary/memdb/skiplist"
)

var ErrJemallocProfilingNotSupported = errors.New("jemalloc profiling not supported")

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

func StatsJson() string {
	return "{}"
}

func ProfActivate() error {
	return ErrJemallocProfilingNotSupported
}

func ProfDeactivate() error {
	return ErrJemallocProfilingNotSupported
}

func ProfDump(_ string) error {
	return ErrJemallocProfilingNotSupported
}
