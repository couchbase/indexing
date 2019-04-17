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

func (v *Int64Val) CAS(old, new int64) bool {
	return atomic.CompareAndSwapInt64(v.val, old, new)
}

func (v Int64Val) Value() int64 {
	value := atomic.LoadInt64(v.val)
	return value
}

type Uint64Val struct {
	val *uint64
}

func (v *Uint64Val) Init() {
	v.val = new(uint64)
}

func (v *Uint64Val) Add(delta uint64) {
	atomic.AddUint64(v.val, delta)
}

func (v *Uint64Val) Set(nv uint64) {
	atomic.StoreUint64(v.val, nv)
}

func (v Uint64Val) MarshalJSON() ([]byte, error) {
	value := atomic.LoadUint64(v.val)
	return []byte(fmt.Sprint(value)), nil
}

func (v Uint64Val) Value() uint64 {
	value := atomic.LoadUint64(v.val)
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

type TimeVal struct {
	val *int64
}

func (v *TimeVal) Init() {
	v.val = new(int64)
}

func (v *TimeVal) Set(nv int64) {
	atomic.StoreInt64(v.val, nv)
}

func (v TimeVal) MarshalJSON() ([]byte, error) {
	return []byte(v.Value()), nil
}

func (v TimeVal) Value() string {
	value := atomic.LoadInt64(v.val)
	return fmt.Sprint(value)
}
