package stats

import (
	"fmt"
	"sync/atomic"
	"unsafe"
)

// BEGIN FILTERS ===================================================================================
// List of filters for stats values
const (
	AllStatsFilter    = 0x1
	PlannerFilter     = 0x2
	IndexStatusFilter = 0x4
	// RebalancerFilter = 0x8 // no longer used
	GSIClientFilter        = 0x10
	N1QLStorageStatsFilter = 0x20 // only used for storage stats
	SummaryFilter          = 0x40
	SmartBatchingFilter    = 0x80
)

// END FILTERS =====================================================================================

// TODO: ns_server to provide a list of stats that will be
// categorised as ExternalStats

type Int64Val struct {
	val    *int64
	bitmap uint64 // bitmap to decide stat filter
}

func (v *Int64Val) Init() {
	v.val = new(int64)

	// Set the default value of filter bitmap to AllStatsFilter
	v.bitmap = AllStatsFilter
}

func (v *Int64Val) Add(delta int64) {
	atomic.AddInt64(v.val, delta)
}

func (v *Int64Val) Set(nv int64) {
	atomic.StoreInt64(v.val, nv)
}

func (v *Int64Val) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprint(atomic.LoadInt64(v.val))), nil
}

func (v *Int64Val) CAS(old, new int64) bool {
	return atomic.CompareAndSwapInt64(v.val, old, new)
}

func (v *Int64Val) Value() int64 {
	return atomic.LoadInt64(v.val)
}

func (v *Int64Val) GetValue() interface{} {
	return atomic.LoadInt64(v.val)
}

func (v *Int64Val) AddFilter(filter uint64) {
	v.bitmap |= filter
}

func (v *Int64Val) Map(bitmap uint64) bool {
	return (v.bitmap & bitmap) != 0
}

type Uint64Val struct {
	val    *uint64
	bitmap uint64 // bitmap to decide stat filter
}

func (v *Uint64Val) Init() {
	v.val = new(uint64)

	// Set the default value of filter bitmap to AllStatsFilter
	v.bitmap = AllStatsFilter
}

func (v *Uint64Val) Add(delta uint64) {
	atomic.AddUint64(v.val, delta)
}

func (v *Uint64Val) Set(nv uint64) {
	atomic.StoreUint64(v.val, nv)
}

func (v *Uint64Val) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprint(atomic.LoadUint64(v.val))), nil
}

func (v *Uint64Val) Value() uint64 {
	return atomic.LoadUint64(v.val)
}

func (v *Uint64Val) GetValue() interface{} {
	return atomic.LoadUint64(v.val)
}

func (v *Uint64Val) AddFilter(filter uint64) {
	v.bitmap |= filter
}

func (v *Uint64Val) Map(bitmap uint64) bool {
	return (v.bitmap & bitmap) != 0
}

type BoolVal struct {
	val    *int32
	bitmap uint64 // bitmap to decide stat filter
}

func (v *BoolVal) Init() {
	v.val = new(int32)

	// Set the default value of filter bitmap to AllStatsFilter
	v.bitmap = AllStatsFilter
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

func (v *BoolVal) MarshalJSON() ([]byte, error) {
	value := atomic.LoadInt32(v.val)
	if value == 1 {
		return []byte("true"), nil
	}
	return []byte("false"), nil
}

func (v *BoolVal) Value() bool {
	return 1 == atomic.LoadInt32(v.val)
}

func (v *BoolVal) GetValue() interface{} {
	return 1 == atomic.LoadInt32(v.val)
}

func (v *BoolVal) AddFilter(filter uint64) {
	v.bitmap |= filter
}

func (v *BoolVal) Map(bitmap uint64) bool {
	return (v.bitmap & bitmap) != 0
}

type TimeVal struct {
	val    *int64
	bitmap uint64 // bitmap to decide stat filter
}

func (v *TimeVal) Init() {
	v.val = new(int64)

	// Set the default value of filter bitmap to AllStatsFilter
	v.bitmap = AllStatsFilter
}

func (v *TimeVal) Set(nv int64) {
	atomic.StoreInt64(v.val, nv)
}

func (v *TimeVal) MarshalJSON() ([]byte, error) {
	return []byte(v.Value()), nil
}

func (v *TimeVal) Value() string {
	return fmt.Sprint(atomic.LoadInt64(v.val))
}

func (v *TimeVal) GetValue() interface{} {
	return fmt.Sprint(atomic.LoadInt64(v.val))
}

func (v *TimeVal) AddFilter(filter uint64) {
	v.bitmap |= filter
}

func (v *TimeVal) Map(bitmap uint64) bool {
	return (v.bitmap & bitmap) != 0
}

type StringVal struct {
	val    unsafe.Pointer
	bitmap uint64 // bitmap to decide stat filter
}

func (v *StringVal) Set(s *string) {
	atomic.StorePointer(&v.val, unsafe.Pointer(s))
}

func (v *StringVal) Get() string {
	if v.val == nil {
		return ""
	}

	s := (*string)(atomic.LoadPointer(&v.val))
	return *s
}

func (v *StringVal) AddFilter(filter uint64) {
	v.bitmap |= filter
}

func (v *StringVal) Map(bitmap uint64) bool {
	return (v.bitmap & bitmap) != 0
}

func (v *StringVal) Init() {
	v.val = nil

	// Set the default value of filter bitmap to AllStatsFilter
	v.bitmap = AllStatsFilter
}

func (v *StringVal) GetValue() interface{} {
	return string(v.Get())
}

// Val struct can hold pointer to any type of value
type MapVal struct {
	val    map[string]interface{}
	bitmap uint64 // bitmap to decide stat filter
}

func (v *MapVal) Map(bitmap uint64) bool {
	return (v.bitmap & bitmap) != 0
}

func (v *MapVal) Init() {
	// Set the default value of filter bitmap to AllStatsFilter
	v.bitmap = AllStatsFilter
}

func (v *MapVal) Set(m map[string]interface{}) {
	v.val = m
}

func (v *MapVal) GetValue() interface{} {
	return v.val
}

func (v *MapVal) AddFilter(filter uint64) {
	v.bitmap |= filter
}

// ------------------------------------------------------------------------
// StatVal interface exposes common functionality defined by all types of
// values defined here.
// ------------------------------------------------------------------------
type StatVal interface {

	// Adds a filter to the stat value.
	AddFilter(uint64)

	// Returns true if the stat value filter contains input filter.
	Map(uint64) bool

	// Returns the value of the of the stat.
	GetValue() interface{}
}
