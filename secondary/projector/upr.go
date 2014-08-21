// To be moved to go-couchbase.

package projector

import (
	"fmt"
	"time"

	mc "github.com/couchbase/gomemcached/client"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/protobuf"
	"github.com/couchbaselabs/go-couchbase"
)

// FailoverLog for a single vbucket.
type FailoverLog [][2]uint64

const (
	// OpStreamBegin for a new vbucket stream on an UPR connection.
	OpStreamBegin byte = iota + 1
	// OpMutation message on an UPR connection.
	OpMutation
	// OpDeletion message on an UPR connection.
	OpDeletion
	// OpStreamEnd for a closing vbucket stream on an UPR connection.
	OpStreamEnd
)

// BucketAccess interface manage a subset of vbucket streams with mutiple KV
// nodes. To be implemented by couchbase.Bucket type.
type BucketAccess interface {
	// Refresh bucket meta information like vbmap
	Refresh() error

	// GetVBmap returns a map of `kvaddr` to list of vbuckets hosted in a kv
	// node.
	GetVBmap(kvaddrs []string) (map[string][]uint16, error)

	// FailoverLog fetch the failover log for specified vbucket
	GetFailoverLogs(vbuckets []uint16) (flogs couchbase.FailoverLog, err error)

	// Close this bucket.
	Close()
}

// KVFeeder interface from a BucketAccess object.
type KVFeeder interface {
	// GetChannel return a mutation channel.
	GetChannel() (mutch <-chan *mc.UprEvent)

	// StartVbStreams starts new vbucket streams on this feed.
	// Return failover-timestamp and kv-timestamp for the newly started
	// vbucket streams.
	StartVbStreams(couchbase.FailoverLog, *protobuf.TsVbuuid) (failoverTs, kvTs *protobuf.TsVbuuid, err error)

	// EndVbStreams ends an existing vbucket stream from this feed.
	EndVbStreams(endTs *protobuf.TsVbuuid) (err error)

	// CloseKVFeed ends all active streams on this feed and free its resources.
	CloseKVFeed() (err error)
}

// concrete type implementing KVFeeder
type kvUpr struct {
	bucket  *couchbase.Bucket
	kvaddr  string
	uprFeed *couchbase.UprFeed
	kvfeed  *KVFeed
}

// OpenKVFeed opens feed with `kvaddr` for a subset of vbucket, specified by
// `restartTs`. Implementer will compute the failoverTs and actual restartTs,
// and return back the same to projector.
func OpenKVFeed(b *couchbase.Bucket, kvaddr string, kvfeed *KVFeed) (kvf KVFeeder, err error) {
	kv := &kvUpr{b, kvaddr, nil, kvfeed}
	name := fmt.Sprintf("%v", time.Now().UnixNano())
	kv.uprFeed, err = b.StartUprFeed(name, uint32(0))
	if err != nil {
		return nil, err
	}
	return kv, nil
}

func (kv *kvUpr) GetChannel() (mutch <-chan *mc.UprEvent) {
	return kv.uprFeed.C
}

func (kv *kvUpr) StartVbStreams(
	flogs couchbase.FailoverLog,
	restartTs *protobuf.TsVbuuid) (failoverTs, kvTs *protobuf.TsVbuuid, err error) {

	failoverTs = restartTs.ComputeFailoverTs(flogs)

	for i, vbno := range c.Vbno32to16(restartTs.Vbnos) {
		snapshots := restartTs.Snapshots
		flags, vbuuid := uint32(0), restartTs.Vbuuids[i]
		start, end := restartTs.Seqnos[i], uint64(0xFFFFFFFFFFFFFFFF)
		snapStart, snapEnd := snapshots[i].GetStart(), snapshots[i].GetEnd()
		err = kv.uprFeed.UprRequestStream(
			vbno, flags, vbuuid, start, end, snapStart, snapEnd)
		if err != nil {
			c.Errorf("%v %v", kv.kvfeed.logPrefix, err)
			return nil, nil, err
		}
	}
	return failoverTs, restartTs, nil
}

func (kv *kvUpr) EndVbStreams(endTs *protobuf.TsVbuuid) (err error) {
	return
}

func (kv *kvUpr) CloseKVFeed() (err error) {
	return nil
}
