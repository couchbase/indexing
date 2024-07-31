package stats

import (
	"fmt"
	"math"
	"sync/atomic"
	"time"
)

type Histogram struct {
	buckets    []int64
	vals       []int64
	humanizeFn func(int64) string
	bitmap     uint64
	total      uint64
}

func (h *Histogram) Init(buckets []int64, humanizeFn func(int64) string) {
	l := len(buckets)
	h.buckets = make([]int64, l+2)
	copy(h.buckets[1:l+1], buckets)
	h.buckets[0] = math.MinInt64
	h.buckets[l+1] = math.MaxInt64
	h.vals = make([]int64, l+1)

	if humanizeFn == nil {
		humanizeFn = func(v int64) string { return fmt.Sprint(v) }
	}

	h.humanizeFn = humanizeFn

	h.bitmap = AllStatsFilter

	atomic.StoreUint64(&h.total, 0)
}

func (h *Histogram) InitLatency(buckets []int64, humanizeFn func(int64) string) {
	l := len(buckets)
	h.buckets = make([]int64, l+2)
	for i := 1; i <= l; i++ {
		h.buckets[i] = buckets[i-1] * int64(time.Millisecond)
	}
	h.buckets[0] = math.MinInt64
	h.buckets[l+1] = math.MaxInt64
	h.vals = make([]int64, l+1)

	if humanizeFn == nil {
		humanizeFn = func(v int64) string { return fmt.Sprint(v) }
	}

	h.humanizeFn = humanizeFn

	h.bitmap = AllStatsFilter

	atomic.StoreUint64(&h.total, 0)
}

func (h *Histogram) Add(val int64) {
	i := h.findBucket(val)
	atomic.AddInt64(&h.vals[i], 1)
	atomic.AddUint64(&h.total, uint64(val))
}

func (h *Histogram) Merge(src Histogram) {
	if len(h.vals) != len(src.vals) {
		return
	}

	if len(h.buckets) != len(src.buckets) {
		return
	}

	for i, bucket := range src.buckets {
		if h.buckets[i] != bucket {
			return
		}
	}

	for i, val := range src.vals {
		h.vals[i] += val
	}
}

func (h *Histogram) findBucket(val int64) int {
	for i := 1; i < len(h.buckets); i++ {
		start := h.buckets[i-1]

		end := h.buckets[i]

		if val > start && val <= end {
			return i - 1
		}
	}

	return 0
}

func (h *Histogram) String() string {
	s := "\""
	l := len(h.vals)
	for i := 0; i < l; i++ {

		low := h.humanizeFn(h.buckets[i])
		hi := h.humanizeFn(h.buckets[i+1])

		s += fmt.Sprintf("(%s-%s)=%d", low, hi, h.vals[i])
		if i != l-1 {
			s += ", "
		}
	}
	s += "\""
	return s
}

func (h *Histogram) MarshalJSON() ([]byte, error) {
	return []byte(h.String()), nil
}

func (h *Histogram) AddFilter(bitmap uint64) {
	h.bitmap |= bitmap
}

func (h *Histogram) Map(bitmap uint64) bool {
	return (h.bitmap & bitmap) != 0
}

func (h *Histogram) GetValue() interface{} {
	out := make(map[string]interface{})
	maxLen := len(h.humanizeFn(h.buckets[len(h.buckets)-2])) //used to decide number of leading zeroes to pad. Padding zeroes opens the possibility of sorting on keys
	for i := 0; i < len(h.buckets)-1; i++ {

		low := h.humanizeFn(h.buckets[i])
		hi := h.humanizeFn(h.buckets[i+1])

		var key string
		if h.buckets[i] == math.MinInt64 {
			key = fmt.Sprintf("(-Inf-%0*v)", maxLen, hi)
		} else if h.buckets[i+1] == math.MaxInt64 {
			key = fmt.Sprintf("(%0*v-Inf)", maxLen, low)
		} else {
			key = fmt.Sprintf("(%0*v-%0*v)", maxLen, low, maxLen, hi)
		}
		out[key] = h.vals[i]
	}
	return out
}

func (h *Histogram) GetTotal() uint64 {
	return atomic.LoadUint64(&h.total)
}
