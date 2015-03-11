package projector

import "time"

import protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
import "code.google.com/p/goprotobuf/proto"

// watch for,
// 1. stale feeds and shut them down.
// 2. crashed routines and cleanup feeds.
func (p *Projector) watcherDameon(tick int) {
	watchTick := time.Tick(time.Duration(tick) * time.Millisecond)
	for {
		<-watchTick
		topics := p.listTopics()
		for _, topic := range topics {
			feed, err := p.GetFeed(topic)
			if err != nil {
				continue
			}
			status, err := feed.StaleCheck()
			if status == "exit" || err != nil {
				req := &protobuf.ShutdownTopicRequest{
					Topic: proto.String(topic),
				}
				p.doShutdownTopic(req, 0xFFFE)
			}
		}
	}
}
