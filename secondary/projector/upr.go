// To be moved to go-couchbase.

package projector

import (
	"fmt"
	mc "github.com/couchbase/gomemcached/client"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbaselabs/go-couchbase"
	"time"
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
	StartVbStreams(restartTs *c.Timestamp) (failoverTs, kvTs *c.Timestamp, err error)

	// EndVbStreams ends an existing vbucket stream from this feed.
	EndVbStreams(endTs *c.Timestamp) (err error)

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

func (kv *kvUpr) StartVbStreams(restartTs *c.Timestamp) (failoverTs, kvTs *c.Timestamp, err error) {
	for _, vbno := range restartTs.Vbnos {
		err = kv.uprFeed.UprRequestStream(vbno, 0, 0, 0, 0xFFFFFFFFFFFFFFFF, 0, 0)
		if err != nil {
			c.Errorf("%v %v", kv.kvfeed.logPrefix, err)
			return nil, nil, err
		}
	}
	if failoverTs, err = computeFailoverTs(kv.bucket, restartTs); err != nil {
		return nil, nil, err
	}
	return failoverTs, restartTs, nil
}

func (kv *kvUpr) EndVbStreams(endTs *c.Timestamp) (err error) {
	return
}

func (kv *kvUpr) CloseKVFeed() (err error) {
	return nil
}

func computeFailoverTs(
	bucket *couchbase.Bucket,
	restartTs *c.Timestamp) (failoverTs *c.Timestamp, err error) {

	failoverTs = c.NewTimestamp(restartTs.Bucket, cap(restartTs.Vbnos))
	flogs, err := bucket.GetFailoverLogs(restartTs.Vbnos)
	if err != nil {
		return nil, err
	}
	for vbno, flog := range flogs {
		x := flog[len(flog)-1]
		failoverTs.Vbnos = append(failoverTs.Vbnos, vbno)
		failoverTs.Vbuuids = append(failoverTs.Vbuuids, x[0])
		failoverTs.Seqnos = append(failoverTs.Seqnos, x[1])
	}
	return failoverTs, nil
}
