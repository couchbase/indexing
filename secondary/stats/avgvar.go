package stats

import (
	"fmt"
	"math"
)

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
	SMA       Int64Val
}

func (av *Average) Init() {
	av.min.Init()
	av.min.Set(math.MaxInt64)
	av.max.Init()
	av.max.Set(math.MinInt64)
	av.count.Init()
	av.sum.Init()
	av.sumsq.Init()
	av.prevCount.Init()
	av.prevSum.Init()
	av.SMA.Init()
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

// Compute and return moving average value
func (av *Average) MovingAvg() int64 {
	count := av.count.Value()
	prevCount := av.prevCount.Value()
	newSum := av.sum.Value()
	prevSum := av.prevSum.Value()
	prevSMA := av.SMA.Value()

	if count-prevCount > 0 {
		newAvg := (newSum - prevSum) / (count - prevCount)
		// For the first movingAvg computation, prevCount would be zero
		if prevCount > 0 {
			newAvg = (prevSMA + newAvg) / 2
		}

		if av.SMA.CAS(prevSMA, newAvg) {
			av.prevCount.Set(count)
			av.prevSum.Set(newSum)
			return newAvg
		}
		// During contention, return the SMA computed by the thread
		// that succeeded in updating the SMA value.
		// As of this commit, there can be at max two threads contending
		// i.e. logger thread in stats_manager.go and either of
		// (i) genServer routine while exiting in dcp_feed.go
		// (ii) run() routine while exiting in endpoint.go
	}
	return av.SMA.Value()
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

func (av *Average) Json() map[string]interface{} {
	var ltMap = make(map[string]interface{})
	ltMap["samples"] = av.count.Value()
	ltMap["min"] = av.Min()
	ltMap["max"] = av.Max()
	ltMap["mean"] = av.Mean()
	ltMap["var"] = av.Variance()
	ltMap["movAvg"] = av.MovingAvg()
	return ltMap
}
