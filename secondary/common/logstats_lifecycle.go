package common

import (
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/logstats/logstats"
)

// ForceRotateStatsLog seals the current stats log file and opens a new one.
// Caller must update the key state in getKey before calling this so the new
// file is written with the correct key (or no key).
func ForceRotateStatsLog() {
	logger := logstats.GetGlobalStatLogger()
	if logger == nil {
		return
	}
	if err := logger.ForceRotate(); err != nil {
		logging.Errorf("ForceRotateStatsLog: %v", err)
	}
}
