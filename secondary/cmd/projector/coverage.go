//go:build go_coverage

package main

import (
	"github.com/couchbase/gocoverage"
	"github.com/couchbase/indexing/secondary/logging"
)

func init() {
	gocoverage.SetLogger(func(format string, args ...any) {
		logging.Infof("CodeCov "+format, args...)
	})
}
