//go:build community
// +build community

package plasma

import (
	"fmt"
	"net/http"
)

type StubType int

var Diag StubType

func SetMemoryQuota(_ int64, _ bool) {
}

func GetMandatoryQuota() (int64, int64) {
	return 0, 0
}

func GetWorkingSetSize() int64 {
	return 0
}

func SetLogReclaimBlockSize(_ int64) {
}

func MemoryInUse() int64 {
	return 0
}

func GolangMemoryInUse() int64 {
	return 0
}

func TenantQuotaNeeded() int64 {
	return 0
}

func (d *StubType) HandleHttp(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "not implemented")
}
