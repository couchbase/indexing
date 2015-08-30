package memcached

import "math"
import "fmt"

// Average maintains the average and variance of a stream
// of numbers in a space-efficient manner.
type Average struct {
	count int64
	sum   int64
	sumsq float64
	min   int64
	max   int64
}

// Add a sample to counting average.
func (av *Average) Add(sample int64) {
	if sample == 0 {
		return
	}
	av.count++
	av.sum += sample
	av.sumsq += float64(sample) * float64(sample)
	if sample < av.min {
		av.min = sample
	}
	if sample > av.max {
		av.max = sample
	}
}

func (av *Average) String() string {
	fmsg := `{"samples": %v, "min": %v, "max": %v, "mean": %v, ` +
		`"variance": %v}`
	return fmt.Sprintf(fmsg, av.count, av.min, av.max, av.Mean(), av.Variance())
}

// GetCount return the number of samples counted so far.
func (av *Average) Count() int64 {
	return av.count
}

// Mean return the sum of all samples by number of samples so far.
func (av *Average) Mean() float64 {
	return float64(av.sum) / float64(av.count)
}

// GetTotal return the sum of all samples so far.
func (av *Average) Sum() float64 {
	return float64(av.sum)
}

// Variance return the variance of all samples so far.
func (av *Average) Variance() float64 {
	a := av.Mean()
	return av.sumsq/float64(av.count) - a*a
}

// GetStdDev return the standard-deviation of all samples so far.
func (av *Average) Sd() float64 {
	return math.Sqrt(av.Variance())
}
