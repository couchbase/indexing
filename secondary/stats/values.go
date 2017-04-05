package stats

import (
	"fmt"
	"sync/atomic"
)

type Int64Val struct {
	val *int64
}

func (v *Int64Val) Init() {
	v.val = new(int64)
}

func (v *Int64Val) Add(delta int64) {
	atomic.AddInt64(v.val, delta)
}

func (v *Int64Val) Set(nv int64) {
	atomic.StoreInt64(v.val, nv)
}

func (v Int64Val) MarshalJSON() ([]byte, error) {
	value := atomic.LoadInt64(v.val)
	return []byte(fmt.Sprint(value)), nil
}

func (v Int64Val) Value() int64 {
	value := atomic.LoadInt64(v.val)
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

	atomic.StoreInt32(v.val, x)
}

func (v BoolVal) MarshalJSON() ([]byte, error) {
	value := atomic.LoadInt32(v.val)
	if value == 1 {
		return []byte("true"), nil
	}

	return []byte("false"), nil
}

func (v BoolVal) Value() bool {
	value := atomic.LoadInt32(v.val)
	return value == 1
}
