// To be moved to dcp-client

package projector

import "time"

import mc "github.com/couchbase/indexing/secondary/dcp/transport/client"
import c "github.com/couchbase/indexing/secondary/common"
import protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
import "github.com/couchbase/indexing/secondary/dcp"

// BucketAccess interface manage a subset of vbucket streams with mutiple KV
// nodes. To be implemented by couchbase.Bucket type.
type BucketAccess interface {
	// Refresh bucket meta information like vbmap
	Refresh() error

	// GetVBmap returns a map of `kvaddr` to list of vbuckets hosted in a kv
	// node.
	GetVBmap(kvaddrs []string) (map[string][]uint16, error)

	// FailoverLog fetch the failover log for specified vbucket
	GetFailoverLogs(
		opaque uint16,
		vbuckets []uint16,
		config map[string]interface{}) (couchbase.FailoverLog, error)

	// Close this bucket.
	Close()
}

// BucketFeeder interface from a BucketAccess object.
type BucketFeeder interface {
	// GetChannel return a mutation channel.
	GetChannel() (mutch <-chan *mc.DcpEvent)

	// StartVbStreams starts a set of vbucket streams on this feed.
	// returns list of vbuckets for which StreamRequest is successfully
	// posted.
	StartVbStreams(opaque uint16, ts *protobuf.TsVbuuid) error

	// EndVbStreams ends an existing vbucket stream from this feed.
	EndVbStreams(opaque uint16, endTs *protobuf.TsVbuuid) error

	// CloseFeed ends all active streams on this feed and free its resources.
	CloseFeed() (err error)
}

// concrete type implementing BucketFeeder
type bucketDcp struct {
	dcpFeed *couchbase.DcpFeed
	bucket  *couchbase.Bucket
}

// OpenBucketFeed opens feed for bucket.
func OpenBucketFeed(
	feedname couchbase.DcpFeedName,
	b *couchbase.Bucket,
	opaque uint16,
	kvaddrs []string,
	config map[string]interface{}) (feeder BucketFeeder, err error) {

	bdcp := &bucketDcp{bucket: b}
	bdcp.dcpFeed, err =
		b.StartDcpFeedOver(feedname, uint32(0), kvaddrs, opaque, config)
	if err != nil {
		return nil, err
	}
	return bdcp, nil
}

// GetChannel implements Feeder{} interface.
func (bdcp *bucketDcp) GetChannel() (mutch <-chan *mc.DcpEvent) {
	return bdcp.dcpFeed.C
}

// StartVbStreams implements Feeder{} interface.
func (bdcp *bucketDcp) StartVbStreams(
	opaque uint16, reqTs *protobuf.TsVbuuid) error {

	var err error

	if bdcp.bucket != nil {
		bdcp.bucket.Refresh()
	}
	vbnos := c.Vbno32to16(reqTs.GetVbnos())
	vbuuids, seqnos := reqTs.GetVbuuids(), reqTs.GetSeqnos()
	for i, vbno := range vbnos {
		snapshots := reqTs.GetSnapshots()
		flags, vbuuid := uint32(0), vbuuids[i]
		start, end := seqnos[i], uint64(0xFFFFFFFFFFFFFFFF)
		snapStart, snapEnd := snapshots[i].GetStart(), snapshots[i].GetEnd()
		e := bdcp.dcpFeed.DcpRequestStream(
			vbno, opaque, flags, vbuuid, start, end, snapStart, snapEnd)
		if e != nil {
			err = e
		}
		// FIXME/TODO: the below sleep avoid back-to-back dispatch of
		// StreamRequest to DCP, which seem to cause some problems.
		time.Sleep(time.Millisecond)
	}
	return err
}

// EndVbStreams implements Feeder{} interface.
func (bdcp *bucketDcp) EndVbStreams(
	opaque uint16, ts *protobuf.TsVbuuid) (err error) {

	if bdcp.bucket != nil {
		bdcp.bucket.Refresh()
	}
	vbnos := c.Vbno32to16(ts.GetVbnos())
	for _, vbno := range vbnos {
		if e := bdcp.dcpFeed.DcpCloseStream(vbno, opaque); e != nil {
			err = e
		}
	}
	return err
}

// CloseFeed implements Feeder{} interface.
func (bdcp *bucketDcp) CloseFeed() error {
	bdcp.dcpFeed.Close()
	bdcp.bucket.Close()
	return nil
}
