//go:build !2ici_test
// +build !2ici_test

package testcode

import (
	"github.com/couchbase/indexing/secondary/common"
)

func TestActionAtTag(cfg common.Config, tag TestActionTag) error {
	// Note: This function is a no-op for non-CI builds. See test_action_ci.go
	// for the implementation for CI builds
	return nil
}

func IgnoreAlternateShardIds(cfg common.Config, defn *common.IndexDefn) {
	// No-op
}

func CorruptIndex(cfg common.Config, index *common.IndexInst) {
	// No-op
}
