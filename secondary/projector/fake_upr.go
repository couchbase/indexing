package projector

import (
	couchbase "github.com/couchbase/indexing/secondary/dcp"
	mc "github.com/couchbase/indexing/secondary/dcp/transport/client"
	protobuf "github.com/couchbase/indexing/secondary/protobuf/projector"
)

// FakeBucket fot unit testing.
type FakeBucket struct {
	bucket  string
	vbmap   map[string][]uint16
	flogs   couchbase.FailoverLog
	C       chan *mc.DcpEvent
	streams map[uint16]*FakeStream
}

// FakeStream fot unit testing.
type FakeStream struct {
	seqno  uint64
	vbuuid uint64
	killch chan bool
}

// NewFakeBuckets returns a reference to new FakeBucket.
func NewFakeBuckets(buckets []string) map[string]*FakeBucket {
	fakebuckets := make(map[string]*FakeBucket)
	for _, bucket := range buckets {
		fakebuckets[bucket] = &FakeBucket{
			bucket:  bucket,
			vbmap:   make(map[string][]uint16),
			flogs:   make(couchbase.FailoverLog),
			C:       make(chan *mc.DcpEvent, 10000),
			streams: make(map[uint16]*FakeStream),
		}
	}
	return fakebuckets
}

// BucketAccess interface

// GetVBmap is method receiver for BucketAccess interface
func (b *FakeBucket) GetVBmap(kvaddrs []string) (map[string][]uint16, error) {
	m := make(map[string][]uint16)
	for kvaddr, vbnos := range b.vbmap {
		m[kvaddr] = vbnos
	}
	return m, nil
}

// GetFailoverLogs is method receiver for BucketAccess interface
func (b *FakeBucket) GetFailoverLogs(
	opaque uint16,
	vbnos []uint16,
	conf map[string]interface{}) (couchbase.FailoverLog, error) {

	return b.flogs, nil
}

// OpenKVFeed is method receiver for BucketAccess interface
func (b *FakeBucket) OpenKVFeed(kvaddr string) (BucketFeeder, error) {
	return b, nil
}

// Close is method receiver for BucketAccess interface
func (b *FakeBucket) Close(kvaddr string) {
	close(b.C)
}

// SetVbmap fake initialization method.
func (b *FakeBucket) SetVbmap(kvaddr string, vbnos []uint16) {
	b.vbmap[kvaddr] = vbnos
}

// SetFailoverLog fake initialization method.
func (b *FakeBucket) SetFailoverLog(vbno uint16, flog [][2]uint64) {
	b.flogs[vbno] = flog
}

// BucketFeeder interface

// GetChannel is method receiver for BucketFeeder interface
func (b *FakeBucket) GetChannel() <-chan *mc.DcpEvent {
	return b.C
}

// StartVbStreams is method receiver for BucketFeeder interface
func (b *FakeBucket) StartVbStreams(
	opaque uint16, ts *protobuf.TsVbuuid) (err error) {

	return err
}

// EndVbStreams is method receiver for BucketFeeder interface
func (b *FakeBucket) EndVbStreams(
	opaque uint16, ts *protobuf.TsVbuuid) (err error, cleanup bool) {

	return
}

// CloseFeed is method receiver for BucketFeeder interface
func (b *FakeBucket) CloseFeed() (err error) {
	return
}

func (s *FakeStream) run(mutch chan *mc.DcpEvent) {
	// TODO: generate mutation events
}

// GetStats is method receiver for BucketFeeder interface
func (b *FakeBucket) GetStats() map[string]interface{} {
	return nil
}

func (b *FakeBucket) GetStreamUuid() uint64 {
	return 0
}
