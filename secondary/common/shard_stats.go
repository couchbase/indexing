package common

type ShardStats struct {
	// UUID of the shard
	ShardId ShardId

	// AlternateShardId of the shard
	AlternateShardId string

	// In-memory consumption by the shard
	MemSz         int64
	MemSzIndex    int64
	BufMemUsed    int64
	LSSBufMemUsed int64

	// Total data size of the shard
	LSSDataSize int64

	// Total number of items in the shard
	ItemsCount int64

	// For computing resident percent of the shard
	CachedRecords int64
	TotalRecords  int64
	Instances     map[string]bool
}

func NewShardStats(alternateShardId string) *ShardStats {
	return &ShardStats{
		AlternateShardId: alternateShardId,
		Instances:        make(map[string]bool),
	}
}
