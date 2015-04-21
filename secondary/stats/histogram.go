package stats

import "sync/atomic"
import "math"
import "fmt"

type Histogram struct {
	buckets    []int64
	vals       []int64
	humanizeFn func(int64) string
}

func (h *Histogram) Init(buckets []int64, humanizeFn func(int64) string) {
	l := len(buckets)
	h.buckets = make([]int64, l+2)
	copy(h.buckets[1:l], buckets)
	h.buckets[0] = math.MinInt64
	h.buckets[l] = math.MaxInt64
	h.vals = make([]int64, l)

	if humanizeFn == nil {
		humanizeFn = func(v int64) string { return fmt.Sprint(v) }
	}

	h.humanizeFn = humanizeFn
}

func (h *Histogram) Add(val int64) {
	i := h.findBucket(val)
	atomic.AddInt64(&h.vals[i], 1)
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

func (h Histogram) String() string {
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

func (h Histogram) MarshalJSON() ([]byte, error) {
	return []byte(h.String()), nil
}
