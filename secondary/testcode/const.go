package testcode

type TestActionTag int

const (
	MASTER_SHARDTOKEN_SCHEDULEACK TestActionTag = iota + 1
	SOURCE_SHARDTOKEN_AFTER_TRANSFER
	DEST_SHARDTOKEN_AFTER_RESTORE
	DEST_SHARDTOKEN_DURING_DEFERRED_INDEX_RECOVERY
	DEST_SHARDTOKEN_DURING_NON_DEFERRED_INDEX_RECOVERY
	DEST_SHARDTOKEN_DURING_INDEX_BUILD
	MASTER_SHARDTOKEN_BEFORE_DROP_ON_SOURCE
	MASTER_SHARDTOKEN_AFTER_DROP_ON_SOURCE
	MASTER_SHARDTOKEN_ALL_TOKENS_PROCESSED
	DEST_INDEXER_BEFORE_INDEX_RECOVERY
	DEST_INDEXER_AFTER_INDEX_RECOVERY
	LIFECYCLE_MANAGER_CREATE_INDEX
)

type TestAction int

const (
	NONE                TestAction = iota // No action to be taken
	INDEXER_PANIC                         // Panic indexer at the tag
	REBALANCE_CANCEL                      // Cancel rebalance at the tag
	EXEC_N1QL_STATEMENT                   // Execute N1QL statement at the tag
	SLEEP                                 // Sleep at the tag
	INJECT_ERROR                          // raise custom error at tag
)

func isMasterTag(tag TestActionTag) bool {
	switch tag {
	case MASTER_SHARDTOKEN_SCHEDULEACK,
		MASTER_SHARDTOKEN_BEFORE_DROP_ON_SOURCE,
		MASTER_SHARDTOKEN_AFTER_DROP_ON_SOURCE,
		MASTER_SHARDTOKEN_ALL_TOKENS_PROCESSED:
		return true
	}
	return false
}
