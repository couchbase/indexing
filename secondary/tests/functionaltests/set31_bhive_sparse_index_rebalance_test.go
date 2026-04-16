package functionaltests

import (
	"testing"
)

var sparseBhiveRebalanceConfig = sparseRebalanceConfig{
	createStmtPrefix:   "CREATE VECTOR INDEX",
	indexerQuota:       BHIVE_INDEX_INDEXER_QUOTA,
	indexActiveTimeout: bhiveIndexActiveTimeout,
	dedicated:          "idxSparseBhive_dedicated_partn",
	shared:             "idxSparseBhive_shared_partn",
	dedicatedReplica:   "idxSparseBhive_dedicated_replica",
	sharedReplica:      "idxSparseBhive_shared_replica",
}

func TestBhiveSparseIndexDCPRebalance(t *testing.T) {
	runSparseIndexDCPRebalance(t, sparseBhiveRebalanceConfig)
}

func TestBhiveSparseIndexShardRebalance(t *testing.T) {
	runSparseIndexShardRebalance(t, sparseBhiveRebalanceConfig)
}
