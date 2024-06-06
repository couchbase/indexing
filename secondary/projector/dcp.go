// To be moved to dcp-client

package projector

import (
	"time"

	"github.com/couchbase/indexing/secondary/logging"

	c "github.com/couchbase/indexing/secondary/common"
	couchbase "github.com/couchbase/indexing/secondary/dcp"
	mc "github.com/couchbase/indexing/secondary/dcp/transport/client"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
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
	EndVbStreams(opaque uint16, endTs *protobuf.TsVbuuid) (error, bool)

	// CloseFeed ends all active streams on this feed and free its resources.
	CloseFeed() (err error)

	// GetStats retrieves the pointer to stats objects from all DCP feeds
	// along with the bucket to which the DCP feeds belong to
	GetStats() map[string]interface{}

	GetStreamUuid() uint64
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
	config map[string]interface{},
	streamUuid uint64) (feeder BucketFeeder, err error) {

	bdcp := &bucketDcp{bucket: b}
	flags := uint32(0x0)
	bdcp.dcpFeed, err =
		b.StartDcpFeedOver(
			feedname, uint32(0), flags,
			kvaddrs, opaque, config, streamUuid,
		)
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
		if err := bdcp.bucket.Refresh(); err != nil {
			logging.Errorf("Error during bucket.Refresh() while starting vbstreams for bucket: %v, err: %v", bdcp.bucket.Name, err)
			return err
		}
	}
	scopeId := reqTs.GetScopeID()
	collectionIds := reqTs.GetCollectionIDs()
	manifestUIDs := reqTs.GetManifestUIDs()

	vbnos := c.Vbno32to16(reqTs.GetVbnos())
	vbuuids, seqnos := reqTs.GetVbuuids(), reqTs.GetSeqnos()
	for i, vbno := range vbnos {
		snapshots := reqTs.GetSnapshots()
		flags, vbuuid := uint32(0), vbuuids[i]
		start, end := seqnos[i], uint64(0xFFFFFFFFFFFFFFFF)
		snapStart, snapEnd := snapshots[i].GetStart(), snapshots[i].GetEnd()

		mid := ""
		if len(manifestUIDs) > 0 {
			mid = manifestUIDs[i]
		}

		e := bdcp.dcpFeed.DcpRequestStream(
			vbno, opaque, flags, vbuuid, start, end, snapStart, snapEnd,
			mid, scopeId, collectionIds)
		if e != nil {
			err = e
			// In case of an error, a clean-up will be triggerred after all the
			// streams are opened. Fail-fast is an optimization that will
			// prevent this unnecessary opening and closing of streams
			return err
		}
		// FIXME/TODO: the below sleep avoid back-to-back dispatch of
		// StreamRequest to DCP, which seem to cause some problems.
		time.Sleep(time.Millisecond)
	}
	return err
}

// EndVbStreams implements Feeder{} interface.
func (bdcp *bucketDcp) EndVbStreams(
	opaque uint16, ts *protobuf.TsVbuuid) (err error, cleanup bool) {

	if bdcp.bucket != nil {
		if err := bdcp.bucket.Refresh(); err != nil {
			logging.Errorf("Error during bucket.Refresh() while stopping vbstreams for bucket: %v, err: %v", bdcp.bucket.Name, err)
			return err, true
		}
	}
	vbnos := c.Vbno32to16(ts.GetVbnos())
	for _, vbno := range vbnos {
		if e := bdcp.dcpFeed.DcpCloseStream(vbno, opaque); e != nil {
			err = e
		}
	}
	return err, false
}

// CloseFeed implements Feeder{} interface.
func (bdcp *bucketDcp) CloseFeed() error {
	bdcp.dcpFeed.Close()
	bdcp.bucket.Close()
	return nil
}

// GetStats() implements Feeder{} interface.
func (bdcp *bucketDcp) GetStats() map[string]interface{} {
	return bdcp.dcpFeed.GetStats()
}

func (bdcp *bucketDcp) GetStreamUuid() uint64 {
	return bdcp.dcpFeed.StreamUuid
}
