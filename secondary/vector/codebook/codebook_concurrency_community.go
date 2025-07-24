//go:build community
// +build community

// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package codebook

// SetConcurrency sets the global maximum number of concurrent codebook operations
func SetConcurrency(maxConcurrent int64) {
	//no-op
}

// GetConcurrency returns the current global maximum number of concurrent operations
func GetConcurrency() int64 {
	//no-op
	return 0
}

// SetTrainingConcurrency sets the maximum number of concurrent training operations
// 1 is the minimum training allowed.
func SetTrainingConcurrency(maxConcurrent int) {
	//no-op
}

// GetTrainingConcurrency returns the current global maximum number of concurrent trainings.
func GetTrainingConcurrency() int {
	//no-op
	return 0
}

// SetThrottleDelay sets the delay in microseconds to throttle the concurrency
func SetThrottleDelay(delayUs int64) {
	//no-op
}

// GetThrottleDelay returns the delay in microseconds to throttle the concurrency
func GetThrottleDelay() int64 {
	//no-op
	return 0
}

// SetOMPThreadLimit sets the thread limit for OpenMP regions.
func SetOMPThreadLimit(maxThreads int) {
	//no-op
}
