// To be moved to go-couchbase.

package projector

import "fmt"
import "time"

import mc "github.com/couchbase/gomemcached/client"
import c "github.com/couchbase/indexing/secondary/common"
import "github.com/couchbase/indexing/secondary/protobuf"
import "github.com/couchbaselabs/go-couchbase"

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

// BucketFeeder interface from a BucketAccess object.
type BucketFeeder interface {
	// GetChannel return a mutation channel.
	GetChannel() (mutch <-chan *mc.UprEvent)

	// StartVbStreams starts a set of vbucket streams on this feed.
	StartVbStreams(opaque uint16, ts *protobuf.TsVbuuid) error

	// EndVbStreams ends an existing vbucket stream from this feed.
	EndVbStreams(opaque uint16, endTs *protobuf.TsVbuuid) (err error)

	// CloseFeed ends all active streams on this feed and free its resources.
	CloseFeed() (err error)
}

// concrete type implementing BucketFeeder
type bucketUpr struct {
	uprFeed *couchbase.UprFeed
	bucket  *couchbase.Bucket
}

// OpenBucketFeed opens feed for bucket.
func OpenBucketFeed(b *couchbase.Bucket) (feeder BucketFeeder, err error) {
	name := fmt.Sprintf("%v", time.Now().UnixNano())
	bupr := &bucketUpr{bucket: b}
	if bupr.uprFeed, err = b.StartUprFeed(name, uint32(0)); err != nil {
		return nil, err
	}
	return bupr, nil
}

// GetChannel implements Feeder{} interface.
func (bupr *bucketUpr) GetChannel() (mutch <-chan *mc.UprEvent) {
	return bupr.uprFeed.C
}

// StartVbStreams implements Feeder{} interface.
func (bupr *bucketUpr) StartVbStreams(
	opaque uint16, ts *protobuf.TsVbuuid) error {

	for i, vbno := range c.Vbno32to16(ts.Vbnos) {
		snapshots := ts.Snapshots
		flags, vbuuid := uint32(0), ts.Vbuuids[i]
		start, end := ts.Seqnos[i], uint64(0xFFFFFFFFFFFFFFFF)
		snapStart, snapEnd := snapshots[i].GetStart(), snapshots[i].GetEnd()
		err := bupr.uprFeed.UprRequestStream(
			vbno, opaque, flags, vbuuid, start, end, snapStart, snapEnd)
		if err != nil {
			return err
		}
	}
	return nil
}

// EndVbStreams implements Feeder{} interface.
func (bupr *bucketUpr) EndVbStreams(
	opaque uint16, ts *protobuf.TsVbuuid) error {

	for _, vbno := range c.Vbno32to16(ts.GetVbnos()) {
		if err := bupr.uprFeed.UprCloseStream(vbno, opaque); err != nil {
			return err
		}
	}
	return nil
}

// CloseFeed implements Feeder{} interface.
func (bupr *bucketUpr) CloseFeed() error {
	bupr.uprFeed.Close()
	bupr.bucket.Close()
	return nil
}
