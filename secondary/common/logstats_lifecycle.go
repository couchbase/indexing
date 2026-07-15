package common

import (
	"os"
	"path/filepath"

	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/logstats/logstats"
)

// CleanupStaleStatsLogTempFiles removes stale .tmp files left in logDir by a
// previous crashed reencryption or decryption.
func CleanupStaleStatsLogTempFiles(logDir, baseName string) {
	pattern := filepath.Join(logDir, baseName+".*.tmp")
	stale, _ := filepath.Glob(pattern)
	for _, path := range stale {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			logging.Debugf("CleanupStaleStatsLogTempFiles: remove %q: %v", path, err)
		}
	}
}

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
