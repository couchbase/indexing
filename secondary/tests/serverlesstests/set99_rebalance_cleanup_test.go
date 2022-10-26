package serverlesstests

import (
	"testing"
)

// This test removes the SHARD_REBALANCE_DIR from file system
func TestRebalanceStorageDirCleanup(t *testing.T) {
	cleanupStorageDir(t)
}
