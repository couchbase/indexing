package projector

import (
	c "github.com/couchbase/indexing/secondary/common"
)

func (p *Projector) newStats() *c.ComponentStat {
	topics, _ := p.ListTopics()
	statFeeds := make(map[string]interface{})
	m := map[string]interface{}{
		"kvaddrs":   p.kvaddrs, // list of kv-nodes attached to this projector
		"topics":    topics,    // list of active topics
		"feeds":     statFeeds, // per feed statistics
		"adminport": nil,       // adminport stats for projector
	}
	statsp, _ := c.NewComponentStat(m)
	return statsp
}

func (feed *Feed) newStats() *c.ComponentStat {
	statBfeeds := make(map[string]interface{})
	statEndpoints := make(map[string]interface{})
	m := map[string]interface{}{
		"engines":   []string{},    // list of engine uuids
		"endpoints": []string{},    // list of endpoint remote addrs
		"bfeeds":    statBfeeds,    // per bucket-feed statistics
		"raddrs":    statEndpoints, // per endpoint-statistics
	}
	statsp, _ := c.NewComponentStat(m)
	return statsp
}

func (bfeed *BucketFeed) newStats() *c.ComponentStat {
	statKVfeeds := make(map[string]interface{})
	m := map[string]interface{}{
		"kvfeeds": statKVfeeds, // per kvfeed statistics
	}
	statsp, _ := c.NewComponentStat(m)
	return statsp
}

func (kvfeed *KVFeed) newStats() *c.ComponentStat {
	statVbuckets := make(map[string]interface{})
	m := map[string]interface{}{
		"mutations": float64(0),   // no. of mutations received
		"vbuckets":  statVbuckets, // per vbucket statistics
	}
	statsp, _ := c.NewComponentStat(m)
	return statsp
}

func (vr *VbucketRoutine) newStats() *c.ComponentStat {
	m := map[string]interface{}{
		"uEngines":  float64(0), // no. of update-engine commands
		"dEngines":  float64(0), // no. of delete-engine commands
		"mutations": float64(0), // no. of Upsert, Delete
		"begins":    float64(0), // no. of Begin
		"syncs":     float64(0), // no. of Sync message generated
	}
	statsp, _ := c.NewComponentStat(m)
	return statsp
}

func (endpoint *Endpoint) newStats() *c.ComponentStat {
	m := map[string]interface{}{
		"vbmaps":    float64(0), // no. of vbmaps
		"mutations": float64(0), // no. of Upsert, Delete, Begin, End, Sync
		"flushes":   float64(0), // no. of batches flushed
	}
	statsp, _ := c.NewComponentStat(m)
	return statsp
}
