// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

// +build windows

package memdb

func GetDefaultNumFD() int {
	// window - default 512 file descriptor per process
	return 512
}

func GetIOConcurrency(concurrency float64) int {

	var limit int

	if concurrency > 1 {
		limit = int(concurrency)
	} else if concurrency <= 0 {
		limit = int(float64(GetDefaultNumFD()) * defaultIOConcurrency)
	} else {
		limit = int(float64(GetDefaultNumFD()) * concurrency)
	}

	// Throttle IO concurrency based on golang thread limit.  Unless we provide a way to increase the number
	// of threads per golang process, this will set a hard limit of IO concurrency at ~7K.   There is a way
	// to increase threads per golang process to allow higher IO concurrency, but we should not do that unless
	// we have test system stability under such a high thread count.
	numThreads := int(float64(limit) * threadFDMultiplier)
	if numThreads > maxThreadLimit {
		limit = defaultThreadLimit
	}

	return limit
}
