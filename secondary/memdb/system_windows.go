// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

//go:build windows
// +build windows

package memdb

import "os"

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

///
// Dir operations
//

// O_DIRECTORY is not supported : file handle invalid
func Dir_Sync(dir string, perm os.FileMode) error {
	return nil
}
