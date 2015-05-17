package stats

import (
	"fmt"
	"github.com/couchbase/indexing/secondary/platform"
)

type Int64Val struct {
	val *platform.AlignedInt64
}

func (v *Int64Val) Init() {
	v.val = new(platform.AlignedInt64)
}

func (v *Int64Val) Add(delta int64) {
	platform.AddInt64(v.val, delta)
}

func (v *Int64Val) Set(nv int64) {
	platform.StoreInt64(v.val, nv)
}

func (v Int64Val) MarshalJSON() ([]byte, error) {
	value := platform.LoadInt64(v.val)
	return []byte(fmt.Sprint(value)), nil
}

func (v Int64Val) Value() int64 {
	value := platform.LoadInt64(v.val)
	return value
}

type BoolVal struct {
	val *int32
}

func (v *BoolVal) Init() {
	v.val = new(int32)
}

func (v *BoolVal) Set(nv bool) {
	var x int32

	if nv {
		x = 1
	} else {
		x = 0
	}

	platform.StoreInt32(v.val, x)
}

func (v BoolVal) MarshalJSON() ([]byte, error) {
	value := platform.LoadInt32(v.val)
	if value == 1 {
		return []byte("true"), nil
	}

	return []byte("false"), nil
}

func (v BoolVal) Value() bool {
	value := platform.LoadInt32(v.val)
	return value == 1
}
