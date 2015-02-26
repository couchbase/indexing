package couchbase

import (
	"errors"
	"sync"

	"github.com/couchbase/indexing/secondary/dcp/transport"
	"github.com/couchbase/indexing/secondary/dcp/transport/client"
	"github.com/couchbase/indexing/secondary/logging"
)

// ErrorInvalidVbucket
var ErrorInvalidVbucket = errors.New("dcp.invalidVbucket")

// ErrorConnection
var ErrorConnection = errors.New("dcp.connection")

// ErrorFailoverLog
var ErrorFailoverLog = errors.New("dcp.failoverLog")

// ErrorInvalidBucket
var ErrorInvalidBucket = errors.New("dcp.invalidBucket")

// ErrorInvalidFeed
var ErrorInvalidFeed = errors.New("dcp.invalidFeed")

// ErrorClosed
var ErrorClosed = errors.New("dcp.closed")

// GetFailoverLogs, get the failover logs for a set of vbucket ids
func (b *Bucket) GetFailoverLogs(vBuckets []uint16) (FailoverLog, error) {
	// map vbids to their corresponding hosts
	vbHostList := make(map[string][]uint16)
	vbm := b.VBServerMap()
	for _, vb := range vBuckets {
		if l := len(vbm.VBucketMap); int(vb) >= l {
			logging.Errorf("error invalid vbucket id %d >= %d\n", vb, l)
			return nil, ErrorInvalidVbucket
		}

		masterID := vbm.VBucketMap[vb][0]
		master := b.getMasterNode(masterID)
		if master == "" {
			logging.Errorf("error master node not found for vbucket %d\n", vb)
			return nil, ErrorInvalidVbucket
		}

		vbList := vbHostList[master]
		if vbList == nil {
			vbList = make([]uint16, 0)
		}
		vbList = append(vbList, vb)
		vbHostList[master] = vbList
	}

	failoverLogMap := make(FailoverLog)
	for _, serverConn := range b.getConnPools() {
		vbList := vbHostList[serverConn.host]
		if vbList == nil {
			continue
		}

		mc, err := serverConn.Get()
		if err != nil {
			logging.Errorf("in serverConn.Get() for vblist %v: %v\n", vbList, err)
			return nil, ErrorConnection
		}
		mc.Hijack()

		failoverlogs, err := mc.DcpGetFailoverLog(vbList)
		serverConn.Return(mc)
		if err != nil {
			format := "error getting failover log for host %s: %v\n"
			logging.Errorf(format, serverConn.host, err)
			return nil, ErrorFailoverLog
		}
		for vb, log := range failoverlogs {
			failoverLogMap[vb] = *log
		}
	}
	return failoverLogMap, nil
}

// DcpFeed from a single connection
type FeedInfo struct {
	dcpFeed *memcached.DcpFeed // DCP feed handle
	host    string             // hostname
	healthy bool
	mu      sync.Mutex
}

type FailoverLog map[uint16]memcached.FailoverLog

// A DcpFeed streams mutation events from a bucket.
//
// Events from the bucket can be read from the channel 'C'.
// Remember to call Close() on it when you're done, unless
// its channel has closed itself already.
type DcpFeed struct {
	C <-chan *memcached.DcpEvent

	bucket    *Bucket
	nodeFeeds map[string]*FeedInfo     // The DCP feeds of the individual nodes
	output    chan *memcached.DcpEvent // Same as C but writeably-typed
	name      string                   // name of this DCP feed
	sequence  uint32                   // sequence number for this feed
	// gen-server
	reqch  chan []interface{}
	finch  chan bool
	wgroup sync.WaitGroup
}

// StartDcpFeed creates and starts a new Dcp feed.
// No data will be sent on the channel unless vbuckets streams
// are requested.
func (b *Bucket) StartDcpFeed(name string, sequence uint32) (*DcpFeed, error) {
	return b.StartDcpFeedOver(name, sequence, nil)
}

// StartDcpFeed creates and starts a new Dcp feed.
// No data will be sent on the channel unless vbuckets streams
// are requested. Connections will be made only to specified
// kvnodes `kvaddrs`, to connect will all kvnodes hosting the bucket,
// pass `kvaddrs` as nil
func (b *Bucket) StartDcpFeedOver(
	name string, sequence uint32, kvaddrs []string) (*DcpFeed, error) {

	feed := &DcpFeed{
		bucket:    b,
		output:    make(chan *memcached.DcpEvent, 10), // TODO: no magic num.
		nodeFeeds: make(map[string]*FeedInfo),
		name:      name,
		sequence:  sequence,
		reqch:     make(chan []interface{}, 16), // TODO: no magic num.
		finch:     make(chan bool),
	}
	feed.C = feed.output
	err := feed.connectToNodes(kvaddrs)
	if err != nil {
		logging.Errorf("error cannot connect to bucket %v\n", err)
		return nil, ErrorInvalidBucket
	}
	go feed.genServer(feed.reqch)
	return feed, nil
}

const (
	ufCmdRequestStream byte = iota + 1
	ufCmdCloseStream
	ufCmdClose
)

// DcpRequestStream starts a stream for a vb on a feed
// and immediately returns, it is upto the channel listener
// to detect StreamBegin.
// Synchronous call.
func (feed *DcpFeed) DcpRequestStream(vb uint16, opaque uint16, flags uint32,
	vbuuid, startSequence, endSequence, snapStart, snapEnd uint64) error {

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{
		ufCmdRequestStream, vb, opaque, flags, vbuuid, startSequence,
		endSequence, snapStart, snapEnd, respch}
	resp, err := failsafeOp(feed.reqch, respch, cmd, feed.finch)
	return opError(err, resp, 0)
}

// DcpCloseStream closes a stream for a vb on a feed
// and immediately returns, it is upto the channel listener
// to detect StreamEnd.
func (feed *DcpFeed) DcpCloseStream(vb, opaqueMSB uint16) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{ufCmdCloseStream, vb, opaqueMSB, respch}
	resp, err := failsafeOp(feed.reqch, respch, cmd, feed.finch)
	return opError(err, resp, 0)
}

// Close DcpFeed. Synchronous call.
func (feed *DcpFeed) Close() error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{ufCmdClose, respch}
	resp, err := failsafeOp(feed.reqch, respch, cmd, feed.finch)
	return opError(err, resp, 0)
}

func (feed *DcpFeed) genServer(reqch chan []interface{}) {
	defer func() { // panic safe
		if r := recover(); r != nil {
			logging.Errorf("error DcpFeed for %v crashed: %v\n", feed.bucket, r)
			logging.Errorf("%s", logging.StackTrace())
		}
	}()

loop:
	for {
		select {
		case msg := <-reqch:
			cmd := msg[0].(byte)
			switch cmd {
			case ufCmdRequestStream:
				vb, opaque := msg[1].(uint16), msg[2].(uint16)
				flags, vbuuid := msg[3].(uint32), msg[4].(uint64)
				startSeq, endSeq := msg[5].(uint64), msg[6].(uint64)
				snapStart, snapEnd := msg[7].(uint64), msg[8].(uint64)
				err := feed.dcpRequestStream(
					vb, opaque, flags, vbuuid, startSeq, endSeq,
					snapStart, snapEnd)
				respch := msg[9].(chan []interface{})
				respch <- []interface{}{err}

			case ufCmdCloseStream:
				vb, opaqueMSB := msg[1].(uint16), msg[2].(uint16)
				err := feed.dcpCloseStream(vb, opaqueMSB)
				respch := msg[3].(chan []interface{})
				respch <- []interface{}{err}

			case ufCmdClose:
				respch := msg[1].(chan []interface{})
				respch <- []interface{}{nil}
				break loop
			}
		}
	}

	close(feed.finch)
	feed.wgroup.Wait()
	feed.nodeFeeds = nil
	close(feed.output)
}

func (feed *DcpFeed) connectToNodes(kvaddrs []string) error {
	kvcache := make(map[string]bool)
	m, err := feed.bucket.GetVBmap(kvaddrs)
	if err != nil {
		return err
	}
	for kvaddr := range m {
		kvcache[kvaddr] = true
	}

	for _, serverConn := range feed.bucket.getConnPools() {
		if _, ok := kvcache[serverConn.host]; !ok {
			continue
		}
		feedInfo := feed.nodeFeeds[serverConn.host]
		if feedInfo != nil && feedInfo.isHealthy() == true {
			continue
		}

		var name string
		if feed.name == "" {
			name = "DefaultDcpClient"
		} else {
			name = feed.name
		}
		singleFeed, err := serverConn.StartDcpFeed(name, feed.sequence)
		if err != nil {
			format := "dcp-client: Error connecting to dcp feed of %s: %v"
			logging.Errorf(format, serverConn.host, err)
			for _, f := range feed.nodeFeeds {
				f.dcpFeed.Close()
			}
			return ErrorInvalidFeed
		}
		// add the node to the connection map
		feedInfo = &FeedInfo{
			dcpFeed: singleFeed,
			healthy: true,
			host:    serverConn.host,
		}
		feed.nodeFeeds[serverConn.host] = feedInfo
		feed.wgroup.Add(1)
		go feed.forwardDcpEvents(feedInfo, feed.finch)
	}
	return nil
}

func (feed *DcpFeed) dcpRequestStream(vb uint16, opaque uint16, flags uint32,
	vbuuid, startSequence, endSequence, snapStart, snapEnd uint64) error {

	vbm := feed.bucket.VBServerMap()
	if l := len(vbm.VBucketMap); int(vb) >= l {
		logging.Errorf("error invalid vbucket id %d >= %d\n", vb, l)
		return ErrorInvalidVbucket
	}

	masterID := vbm.VBucketMap[vb][0]
	master := feed.bucket.getMasterNode(masterID)
	if master == "" {
		logging.Errorf("error master node not found for vbucket %d\n", vb)
		return ErrorInvalidVbucket
	}
	logging.Infof("Posting DCP_REQUEST to %v\n", master)
	singleFeed, ok := feed.nodeFeeds[master]
	if !ok {
		logging.Errorf("error DcpFeed for host %q (vb:%d) not found", master, vb)
		return ErrorInvalidFeed
	}
	if err := singleFeed.dcpFeed.DcpRequestStream(vb, opaque, flags,
		vbuuid, startSequence, endSequence, snapStart, snapEnd); err != nil {
		return err
	}
	return nil
}

func (feed *DcpFeed) dcpCloseStream(vb, opaqueMSB uint16) error {
	vbm := feed.bucket.VBServerMap()
	if l := len(vbm.VBucketMap); int(vb) >= l {
		logging.Errorf("error invalid vbucket id %d >= %d\n", vb, l)
		return ErrorInvalidVbucket
	}

	masterID := vbm.VBucketMap[vb][0]
	master := feed.bucket.getMasterNode(masterID)
	if master == "" {
		logging.Errorf("error master node not found for vbucket %d\n", vb)
		return ErrorInvalidVbucket
	}
	singleFeed, ok := feed.nodeFeeds[master]
	if !ok {
		logging.Errorf("error DcpFeed for host %q (vb:%d) not found", master, vb)
		return ErrorInvalidFeed
	}
	if err := singleFeed.dcpFeed.CloseStream(vb, opaqueMSB); err != nil {
		return err
	}
	return nil
}

// go routine
func (feed *DcpFeed) forwardDcpEvents(nodeFeed *FeedInfo, finch chan bool) {
	singleFeed := nodeFeed.dcpFeed
loop:
	for {
		select {
		case event, ok := <-singleFeed.C:
			if !ok {
				if singleFeed.Error != nil {
					format := "dcp-client: Dcp feed from %s failed: %v"
					logging.Errorf(format, nodeFeed.host, singleFeed.Error)
				}
				break loop
			}
			feed.output <- event
			if event.Status == transport.NOT_MY_VBUCKET {
				logging.Warnf("Got a not my vbucket error !! ")
				if err := feed.bucket.Refresh(); err != nil {
					logging.Errorf("error unable to refresh bucket : %v", err)
					break loop
				}
			}

		case <-finch:
			break loop
		}
	}

	feed.wgroup.Done()
	go feed.Close()
	nodeFeed.dcpFeed.Close()

	nodeFeed.mu.Lock()
	defer nodeFeed.mu.Unlock()
	nodeFeed.healthy = false
}

func (nodeFeed *FeedInfo) isHealthy() bool {
	nodeFeed.mu.Lock()
	defer nodeFeed.mu.Unlock()
	return nodeFeed.healthy
}

// failsafeOp can be used by gen-server implementors to avoid infinitely
// blocked API calls.
func failsafeOp(
	reqch, respch chan []interface{},
	cmd []interface{},
	finch chan bool) ([]interface{}, error) {

	select {
	case reqch <- cmd:
		if respch != nil {
			select {
			case resp := <-respch:
				return resp, nil
			case <-finch:
				return nil, ErrorClosed
			}
		}
	case <-finch:
		return nil, ErrorClosed
	}
	return nil, nil
}

// opError suppliments FailsafeOp used by gen-servers.
func opError(err error, vals []interface{}, idx int) error {
	if err != nil {
		return err
	} else if vals[idx] == nil {
		return nil
	}
	return vals[idx].(error)
}
