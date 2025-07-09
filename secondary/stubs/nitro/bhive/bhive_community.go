//go:build community
// +build community

package bhive

import "net/http"
import "fmt"

type StubType int

var Diag StubType

func SetMemoryQuota(_ int64) {
}

func GetMandatoryQuota() (int64, int64) {
	return 0, 0
}

func GetWorkingSetSize() int64 {
	return 0
}

func (d *StubType) HandleHttp(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "not implemented")
}

func MemoryInUse() int64 {
	return 0
}

func GolangMemoryInUse() int64 {
	return 0
}
