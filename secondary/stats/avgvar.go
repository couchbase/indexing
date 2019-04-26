package stats

import "math"
import "fmt"

// Average maintains the average and variance of a stream
// of numbers in a space-efficient manner.
type Average struct {
	min       Int64Val
	max       Int64Val
	count     Int64Val
	sum       Int64Val
	sumsq     Int64Val
	prevCount Int64Val
	prevSum   Int64Val
	prevSMA   Int64Val
}

func (av *Average) Init() {
	av.min.Init()
	av.min.Set(math.MaxInt64)
	av.max.Init()
	av.count.Init()
	av.sum.Init()
	av.sumsq.Init()
	av.prevCount.Init()
	av.prevSum.Init()
	av.prevSMA.Init()
}

// Add a sample to counting average.
func (av *Average) Add(sample int64) {
	av.count.Add(1)
	av.sum.Add(sample)
	av.sumsq.Add(sample * sample)
	for {
		min := av.min.Value()
		if sample < min {
			if av.min.CAS(min, sample) {
				break
			}
		} else {
			break
		}
	}
	for {
		max := av.max.Value()
		if sample > max {
			if av.max.CAS(max, sample) {
				break
			}
		} else {
			break
		}
	}
}

// Count return the number of samples counted so far.
func (av *Average) Count() int64 {
	return av.count.Value()
}

// Min return the minimum value of sample.
func (av *Average) Min() int64 {
	return av.min.Value()
}

// Max return the maximum value of sample.
func (av *Average) Max() int64 {
	return av.max.Value()
}

// Mean return the sum of all samples by number of samples so far.
func (av *Average) Mean() int64 {
	if av.count.Value() > 0 {
		return int64(float64(av.sum.Value()) / float64(av.count.Value()))
	}
	return 0
}

// GetTotal return the sum of all samples so far.
func (av *Average) Sum() int64 {
	return av.sum.Value()
}

// Variance return the variance of all samples so far.
func (av *Average) Variance() int64 {
	if av.count.Value() > 0 {
		a := av.Mean()
		return int64(float64(av.sumsq.Value())/float64(av.count.Value())) - a*a
	}
	return 0
}

// GetStdDev return the standard-deviation of all samples so far.
func (av *Average) Sd() int64 {
	return int64(math.Sqrt(float64(av.Variance())))
}

// Compute the simple moving average value.
// Multiple threads can call this method for the same Average object
// E.g., Endpoint when exiting, stats_manger.go periodically
func (av *Average) MovingAvg() int64 {
	for {
		count := av.count.Value()
		prevCount := av.prevCount.Value()
		newSum := av.sum.Value()
		prevSum := av.prevSum.Value()
		prevSMA := av.prevSMA.Value()

		var windowAvg int64
		if count > prevCount {
			windowAvg = (newSum - prevSum) / (count - prevCount)
		}
		newAvg := (prevSMA + windowAvg) / 2
		if av.prevSMA.CAS(prevSMA, newAvg) {
			av.prevCount.Set(count)
			av.prevSum.Set(newSum)
			return newAvg
		}
		// On CAS failure attempt a retry. As of this commit, It is okay to
		// retry as there are at max only two threads contending for the same
		// object i.e. logger thread in stats_manger.go (once every
		// statsLogDumpInterval seconds) and either of genServer thread in
		// dcp_feed.go or run() thread in endpoint.go
	}
}

func (av *Average) MarshallJSON() string {
	samples := av.count.Value()
	min := av.Min()
	max := av.Max()
	mean := av.Mean()
	variance := av.Variance()
	movingAvg := av.MovingAvg()
	return fmt.Sprintf("{\"samples\": %v, \"min\": %v, \"max\": %v, \"mean\": %v, \"variance\": %v,\"movingAvg\":%v}",
		samples, min, max, mean, variance, movingAvg)
}
