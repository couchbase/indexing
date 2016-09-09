package projector

import "time"

import c "github.com/couchbase/indexing/secondary/common"
import protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
import "github.com/golang/protobuf/proto"

// watch for,
// 1. stale feeds and shut them down.
// 2. crashed routines and cleanup feeds.
func (p *Projector) watcherDameon(watchInterval, staleTimeout int) {
	watchTick := time.NewTicker(time.Duration(watchInterval) * time.Millisecond)
	defer func() {
		watchTick.Stop()
	}()

	for {
		<-watchTick.C
		topics := p.listTopics()
		for _, topic := range topics {
			feed, err := p.GetFeed(topic)
			if err != nil {
				continue
			}
			status, err := feed.StaleCheck(staleTimeout)
			if status == "exit" && err != c.ErrorClosed {
				req := &protobuf.ShutdownTopicRequest{
					Topic: proto.String(topic),
				}
				p.doShutdownTopic(req, 0xFFFE)
			}
		}
	}
}
