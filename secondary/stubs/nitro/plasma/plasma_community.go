// +build !enterprise

package plasma

import (
	"fmt"
	"net/http"
)

type StubType int

var Diag StubType

func UsePlasma() bool {
	return false
}

func SetMemoryQuota(_ int64) {
}

func SetLogReclaimBlockSize(_ int64) {
}

func MemoryInUse() int64 {
	return 0
}

func (d *StubType) HandleHttp(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "not implemented")
}
