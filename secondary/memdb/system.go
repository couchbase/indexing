// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

// +build !windows

package memdb

import (
	"fmt"
	"math"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
)

func GetDefaultNumFD() int {
	// linux - default 1024 file descriptor per process
	return 1024
}

func GetIOConcurrency(concurrency float64) int {

	var limit int

	if concurrency > 1 {
		limit = int(concurrency)

	} else if concurrency <= 0 {
		limit = int(float64(GetDefaultNumFD()) * defaultIOConcurrency)

	} else {

		var fdLimit syscall.Rlimit
		if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &fdLimit); err != nil {
			limit = int(float64(GetDefaultNumFD()) * defaultIOConcurrency)
		} else {
			fmt.Println(fmt.Sprintf("Maximum number of file descriptors = %v", fdLimit.Max))
			limit = int(float64(fdLimit.Max) * concurrency)
		}
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

func GetMaxProc() (int, error) {

	cmd := exec.Command("ulimit", "-u")

	output, err := cmd.Output()
	if err != nil {
		fmt.Println(fmt.Sprintf("Fail to get stdout: %v", err.Error()))
		return math.MaxInt32, err
	}

	n, err := strconv.Atoi(strings.Trim(string(output), "\n"))
	if err != nil {
		fmt.Println(fmt.Sprintf("Fail to convert to str: %v", err.Error()))
		return math.MaxInt32, err
	}

	return n, nil
}
