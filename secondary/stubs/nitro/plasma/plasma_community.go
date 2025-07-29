//go:build community
// +build community

package plasma

import (
	"fmt"
	"net/http"
	"time"
)

type StubType int

var Diag StubType

type MemTunerConfig = bool
type MemTunerDistStats = bool

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

func MakeMemTunerConfig(_, _, _, _ int64) MemTunerConfig {
	return false
}

func MakeMemTunerDistStats(_, _, _ int64, _, _ time.Time) MemTunerDistStats {
	return false
}

func RunMemQuotaTuner(
	_ chan bool,
	_ func() int64,
	_ func() MemTunerConfig,
	_ func() MemTunerDistStats,
) {
}

func (d *StubType) HandleHttp(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "not implemented")
}
