// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

//go:build linux

//nolint:all
package platform

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"

	"github.com/couchbase/indexing/secondary/logging"
)

var thpDisableNotSupported = errors.New("THP disable not supported")

const (
	prSetTHPDisable = 41
	prGetTHPDisable = 42
)

func int8ToStr(arr []int8) string {
	b := make([]byte, 0, len(arr))
	for _, v := range arr {
		if v == 0x00 { // Stop at the null terminator
			break
		}
		b = append(b, byte(v))
	}
	return string(b)
}

func parseKernelVersion(release string) (major, minor int, err error) {
	// Clean up suffixes (e.g., "5.15.0-104-generic" -> "5.15.0", "6.6+" -> "6.6")
	base := strings.SplitN(release, "-", 2)[0]
	base = strings.SplitN(base, "+", 2)[0]

	// Split by the dot
	parts := strings.Split(base, ".")
	if len(parts) < 2 {
		return 0, 0, fmt.Errorf("unexpected release format: %s", release)
	}

	// Convert Major
	major, err = strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, fmt.Errorf("failed to parse major version '%s': %w", parts[0], err)
	}

	// Convert Minor
	minor, err = strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, fmt.Errorf("failed to parse minor version '%s': %w", parts[1], err)
	}

	return major, minor, nil
}

func getKernelVersion() (int, int) {
	var uts syscall.Utsname

	if err := syscall.Uname(&uts); err != nil {
		logging.Warnf("failed to read kernel version with err %v", err)
		return 0, 0
	}

	releaseStr := int8ToStr(uts.Release[:])

	if len(releaseStr) == 0 {
		logging.Warnf("failed to convert kernel release to string")
		return 0, 0
	}

	logging.Infof("Detected kernel - %v", releaseStr)

	var majorVer, minorVer, err = parseKernelVersion(releaseStr)
	if err != nil {
		logging.Warnf("failed to parse kernel version with err %v", err)
		return 0, 0
	}

	return majorVer, minorVer
}

// EnsureTHPDisabled disables Transparent Huge Pages for the process.
//
// On the first invocation it sets PR_SET_THP_DISABLE on the calling OS thread
// and re-execs the process with the same arguments. Because exec(2) preserves
// TIF_NOHUGEPAGE across the execve boundary, the re-exec'd process's initial
// thread starts with THP disabled. All OS threads subsequently created by the
// Go runtime inherit the flag via clone(2), so every goroutine thread starts
// with THP disabled from the very beginning.
//
// On the second invocation (the re-exec'd child) PR_GET_THP_DISABLE returns 1,
// so the function returns nil immediately and the process continues normally.
//
// If re-exec succeeds, this function never returns. If any step fails, it
// returns an error and the caller should log it; the process continues running
// with THP only partially disabled on the main thread.
func EnsureTHPDisabled() error {
	if majorVer, minorVer := getKernelVersion(); !(majorVer*1_000+minorVer >= 3*1_000+15) {
		return fmt.Errorf("%w as kernel version %v.%v is lesser than 3.15",
			thpDisableNotSupported, majorVer, minorVer)
	}

	////////// if THP already disabled then return
	r, _, _ := syscall.RawSyscall6(syscall.SYS_PRCTL, prGetTHPDisable, 0, 0, 0, 0, 0)
	if r == 1 {
		return nil
	}

	////////// restart already attempted. if THP is still not disabled, could be due to kernel/system
	if os.Getenv("CB_THP_DISABLED") == "1" {
		return fmt.Errorf("recreated with THP disable set but still got THP enabled from kernel. maybe %w",
			thpDisableNotSupported)
	}

	////////// set THP disabled for current thread. all threads inherit this property
	_, _, errno := syscall.RawSyscall6(syscall.SYS_PRCTL, prSetTHPDisable, 1, 0, 0, 0, 0)
	if errno != 0 {
		return fmt.Errorf("prctl PR_SET_THP_DISABLE: %w", errno)
	}

	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("THP re-exec: resolve executable: %w", err)
	}

	////////// prevent frequent restarts if kernel/system issues block THP disable
	envv := os.Environ()
	envv = append(envv, "CB_THP_DISABLED=1")

	////////// syscall.Exec triggers linux execve(2) which replaces the current binary with the new
	// binary. the new binary inherits the THP disabled and hence fresh indexer starts with no THP
	logging.Infof("Restarting with THP disabled...")
	if err := syscall.Exec(exe, os.Args, envv); err != nil {
		return fmt.Errorf("THP re-exec: %w", err)
	}

	return nil // unreachable
}
