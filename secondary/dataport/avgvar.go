package dataport

import "math"

// Average maintains the average and variance of a stream
// of numbers in a space-efficient manner.
type Average struct {
	min   int64
	max   int64
	count int64
	sum   int64
	sumsq int64
}

// Add a sample to counting average.
func (av *Average) Add(sample int64) {
	av.count++
	av.sum += sample
	av.sumsq += sample * sample
	if sample < av.min {
		av.min = sample
	}
	if sample > av.max {
		av.max = sample
	}
}

// Count return the number of samples counted so far.
func (av *Average) Count() int64 {
	return av.count
}

// Min return the minimum value of sample.
func (av *Average) Min() int64 {
	return av.min
}

// Max return the maximum value of sample.
func (av *Average) Max() int64 {
	return av.max
}

// Mean return the sum of all samples by number of samples so far.
func (av *Average) Mean() int64 {
	return int64(float64(av.sum) / float64(av.count))
}

// GetTotal return the sum of all samples so far.
func (av *Average) Sum() int64 {
	return av.sum
}

// Variance return the variance of all samples so far.
func (av *Average) Variance() int64 {
	a := av.Mean()
	return int64(float64(av.sumsq)/float64(av.count)) - a*a
}

// GetStdDev return the standard-deviation of all samples so far.
func (av *Average) Sd() int64 {
	return int64(math.Sqrt(float64(av.Variance())))
}
