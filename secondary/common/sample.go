// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package common

import (
	"math"
)

type Sample struct {
	size  int
	count int

	buf  []float64
	head int // pos of head of buf
	free int // pos of head of free list
}

//
// Constructor
//
func NewSample(size int) *Sample {

	s := &Sample{
		size: size,
		buf:  make([]float64, size),
	}
	return s
}

//
// Add a new value to the sample.
//
func (s *Sample) Update(v float64) {

	if s.count == s.size {
		s.pop()
	}

	next := s.free + 1
	if next >= s.size {
		next = 0
	}

	s.buf[s.free] = v
	s.free = next
	s.count++
}

//
// Free the oldest sample
//
func (s *Sample) pop() {

	if s.count > 0 {
		next := s.head + 1
		if next >= s.size {
			next = 0
		}

		s.head = next
		s.count--
	}
}

// Calculate Max
func (s *Sample) Max() float64 {

	var max float64
	for i := 0; i < s.count; i++ {
		if s.buf[i] > max {
			max = s.buf[i]
		}
	}
	return max
}

//
// Calcuate Mean
//
func (s *Sample) Mean() float64 {

	return s.WindowMean(s.count)
}

//
// Calcuate Mean of a custom window size
//
func (s *Sample) WindowMean(count int) float64 {

	if s.count == 0 {
		return 0
	}

	if count > s.count {
		count = s.count
	}

	total := float64(0)

	tail := s.free - 1
	for i := 0; i < count; i++ {
		if tail < 0 {
			tail = s.size - 1
		}

		total += s.buf[tail]
		tail--
	}

	return total / float64(count)
}

//
// Calcuate Std Dev
//
func (s *Sample) StdDev() float64 {

	return s.WindowStdDev(s.count)
}

//
// Calcuate Std Dev on a custom window size
//
func (s *Sample) WindowStdDev(count int) float64 {

	if s.count == 0 {
		return 0
	}

	if count > s.count {
		count = s.count
	}

	mean := s.WindowMean(count)
	variance := float64(0)

	tail := s.free - 1
	for i := 0; i < count; i++ {
		if tail < 0 {
			tail = s.size - 1
		}

		v := s.buf[tail] - mean
		variance += v * v
		tail--
	}

	variance = variance / float64(count)
	return math.Sqrt(variance)
}

//
// Return last value
//
func (s *Sample) Last() float64 {

	if s.count == 0 {
		return 0
	}

	tail := s.free - 1
	if tail < 0 {
		tail = s.size - 1
	}

	return s.buf[tail]
}

//
// Return number of samples
//
func (s *Sample) Count() int {
	return s.count
}
