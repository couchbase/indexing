// To be moved to go-couchbase.

package projector

import (
	c "github.com/couchbase/indexing/secondary/common"
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

// MutationEvent objects
type MutationEvent struct {
	Opcode   byte
	Vbucket  uint16
	Vbuuid   uint64
	Seqno    uint64
	Key      []byte
	Value    []byte
	OldValue []byte
}

// BucketAccess interface manage a subset of vbucket streams with mutiple KV
// nodes. To be implemented by couchbase.Bucket type.
type BucketAccess interface {
	// GetVBmap returns a map of `kvaddr` to list of vbuckets hosted in a kv
	// node.
	GetVBmap(kvaddrs []string) (map[string][]uint16, error)

	// FailoverLog fetch the failover log for specified vbucket
	GetFailoverLog(vbucket uint16) (flog [][2]uint64, err error)

	// OpenKVFeed opens feed with `kvaddr` for a subset of vbucket, specified by
	// `restartTs`. Implementer will compute the failoverTs and actual
	// restartTs, and return back the same to projector.
	OpenKVFeed(kvaddr string) (kvfeed interface{}, err error)
	// TODO: OpenKVFeed(kvaddr string) (kvfeed KVFeeder, err error)

	// Close this bucket.
	Close()
}

// KVFeeder interface from a BucketAccess object.
type KVFeeder interface {
	// GetChannel return a mutation channel and error (sideband channel) on
	// which upstream events will be published
	GetChannel() (mutch <-chan *MutationEvent)

	// StartVbStreams starts new vbucket streams on this feed.
	// Return failover-timestamp and kv-timestamp for the newly started
	// vbucket streams.
	StartVbStreams(restartTs *c.Timestamp) (failoverTs, kvTs *c.Timestamp, err error)

	// EndVbStreams ends an existing vbucket stream from this feed.
	EndVbStreams(endTs *c.Timestamp) (err error)

	// CloseKVFeed ends all active streams on this feed and free its resources.
	CloseKVFeed() (err error)
}
