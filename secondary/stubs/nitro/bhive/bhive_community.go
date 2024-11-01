// +build community

package bhive

import "net/http"
import "fmt"

type StubType int

var Diag StubType

func SetMemoryQuota(_ int64) {
}

func (d *StubType) HandleHttp(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "not implemented")
}
