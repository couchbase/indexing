// +build !community

package bhive

import (
	ee "github.com/couchbase/bhive"
)

var Diag = &ee.Diag

func SetMemoryQuota(sz int64) {
	ee.SetMemoryQuota(uint64(sz))
}
