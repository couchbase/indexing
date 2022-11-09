//go:build !2ici_test
// +build !2ici_test

package testcode

import "github.com/couchbase/indexing/secondary/common"

func CancelOrPanicAtTag(cfg common.Config, tag string, cancel chan struct{}) {
	// Note: This function is a no-op for non-CI builds. See cancel_panic_ci.go
	// for the implementation for CI builds
}
