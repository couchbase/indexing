package projector

import (
	c "github.com/couchbase/indexing/secondary/common"
)

func (p *Projector) newStats() c.Statistics {
	topics, _ := p.ListTopics()
	statFeeds := make(map[string]interface{})
	m := map[string]interface{}{
		"kvaddrs":   p.kvaddrs, // list of kv-nodes attached to this projector
		"topics":    topics,    // list of active topics
		"adminport": nil,       // adminport stats for projector
		"feeds":     statFeeds, // per feed statistics
	}
	stats, _ := c.NewStatistics(m)
	return stats
}

func (feed *Feed) newStats() c.Statistics {
	statBfeeds := make(map[string]interface{})
	statEndpoints := make(map[string]interface{})
	m := map[string]interface{}{
		"engines":   []string{},    // list of engine uuids
		"endpoints": statEndpoints, // per endpoint-statistics
		"bfeeds":    statBfeeds,    // per bucket-feed statistics
	}
	stats, _ := c.NewStatistics(m)
	return stats
}

func (bfeed *BucketFeed) newStats() c.Statistics {
	statKVfeeds := make(map[string]interface{})
	m := map[string]interface{}{
		"kvfeeds": statKVfeeds, // per kvfeed statistics
	}
	stats, _ := c.NewStatistics(m)
	return stats
}

func (kvfeed *KVFeed) newStats() c.Statistics {
	statVbuckets := make(map[string]interface{})
	m := map[string]interface{}{
		"events":   float64(0),   // no. of mutations events received
		"vbuckets": statVbuckets, // per vbucket statistics
	}
	stats, _ := c.NewStatistics(m)
	return stats
}

func (vr *VbucketRoutine) newStats() c.Statistics {
	m := map[string]interface{}{
		"uEngines":  float64(0), // no. of update-engine commands
		"dEngines":  float64(0), // no. of delete-engine commands
		"begins":    float64(0), // no. of Begin
		"snapshots": float64(0), // no. of Begin
		"mutations": float64(0), // no. of Upsert, Delete
		"syncs":     float64(0), // no. of Sync message generated
	}
	stats, _ := c.NewStatistics(m)
	return stats
}

func (endpoint *Endpoint) newStats() c.Statistics {
	m := map[string]interface{}{
		"vbmaps":    float64(0), // no. of vbmaps
		"mutations": float64(0), // no. of Upsert, Delete, Begin, End, Sync
		"flushes":   float64(0), // no. of batches flushed
	}
	stats, _ := c.NewStatistics(m)
	return stats
}
