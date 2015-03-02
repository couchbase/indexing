package couchbase

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/couchbase/indexing/secondary/dcp/transport/client"
	"github.com/couchbase/indexing/secondary/logging"
)

// ErrorInvalidVbucket
var ErrorInvalidVbucket = errors.New("dcp.invalidVbucket")

// ErrorFailoverLog
var ErrorFailoverLog = errors.New("dcp.failoverLog")

// ErrorInvalidBucket
var ErrorInvalidBucket = errors.New("dcp.invalidBucket")

// ErrorClosed
var ErrorClosed = errors.New("dcp.closed")

// FailoverLog for list of vbuckets.
type FailoverLog map[uint16]memcached.FailoverLog

// GetFailoverLogs, get the failover logs for a set of vbucket ids
func (b *Bucket) GetFailoverLogs(
	vBuckets []uint16, config map[string]interface{}) (FailoverLog, error) {
	// map vbids to their corresponding hosts
	vbHostList := make(map[string][]uint16)
	vbm := b.VBServerMap()
	for _, vb := range vBuckets {
		if l := len(vbm.VBucketMap); int(vb) >= l {
			logging.Errorf("DCP[] invalid vbucket id %d >= %d", vb, l)
			return nil, ErrorInvalidVbucket
		}

		masterID := vbm.VBucketMap[vb][0]
		master := b.getMasterNode(masterID)
		if master == "" {
			logging.Errorf("DCP[] master node not found for vbucket %d", vb)
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

		name := fmt.Sprintf("getfailoverlog-%s-%v", b.Name, time.Now().UnixNano())
		singleFeed, err := serverConn.StartDcpFeed(name, 0, nil, config)
		if err != nil {
			return nil, err
		}
		defer singleFeed.Close()

		failoverlogs, err := singleFeed.DcpGetFailoverLog(vbList)
		if err != nil {
			return nil, err
		}
		for vb, log := range failoverlogs {
			failoverLogMap[vb] = *log
		}
	}
	return failoverLogMap, nil
}

// DcpFeed streams mutation events from a bucket.
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
	reqch     chan []interface{}
	finch     chan bool
	logPrefix string
}

// StartDcpFeed creates and starts a new Dcp feed.
// No data will be sent on the channel unless vbuckets streams
// are requested.
func (b *Bucket) StartDcpFeed(
	name string, sequence uint32,
	config map[string]interface{}) (*DcpFeed, error) {

	return b.StartDcpFeedOver(name, sequence, nil, config)
}

// StartDcpFeed creates and starts a new Dcp feed.
// No data will be sent on the channel unless vbuckets streams
// are requested. Connections will be made only to specified
// kvnodes `kvaddrs`, to connect will all kvnodes hosting the bucket,
// pass `kvaddrs` as nil
func (b *Bucket) StartDcpFeedOver(
	name string,
	sequence uint32,
	kvaddrs []string,
	config map[string]interface{}) (*DcpFeed, error) {

	genChanSize := config["genChanSize"].(int)
	dataChanSize := config["dataChanSize"].(int)
	feed := &DcpFeed{
		bucket:    b,
		nodeFeeds: make(map[string]*FeedInfo),
		output:    make(chan *memcached.DcpEvent, dataChanSize),
		name:      name,
		sequence:  sequence,
		reqch:     make(chan []interface{}, genChanSize),
		finch:     make(chan bool),
		logPrefix: fmt.Sprintf("DCP[%v]", name),
	}
	feed.C = feed.output
	if feed.connectToNodes(kvaddrs, config) != nil {
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
func (feed *DcpFeed) DcpRequestStream(
	vb uint16, opaque uint16, flags uint32,
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
		close(feed.finch)
		if r := recover(); r != nil {
			logging.Errorf("%v crashed: %v\n", feed.logPrefix, r)
			logging.Errorf("%s", logging.StackTrace())
		}
		for _, nodeFeed := range feed.nodeFeeds {
			nodeFeed.dcpFeed.Close()
		}
		feed.nodeFeeds = nil
		close(feed.output)
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
}

func (feed *DcpFeed) connectToNodes(
	kvaddrs []string, config map[string]interface{}) error {

	prefix := feed.logPrefix
	kvcache := make(map[string]bool)
	m, err := feed.bucket.GetVBmap(kvaddrs)
	if err != nil {
		logging.Fatalf("%v GetVBmap(%v) failed: %v\n", prefix, kvaddrs, err)
		return memcached.ErrorInvalidFeed
	}
	for kvaddr := range m {
		kvcache[kvaddr] = true
	}

	for _, serverConn := range feed.bucket.getConnPools() {
		if _, ok := kvcache[serverConn.host]; !ok {
			continue
		}
		nodeFeed := feed.nodeFeeds[serverConn.host]
		if nodeFeed != nil {
			nodeFeed.dcpFeed.Close()
			// and continue to spawn a new one ...
		}

		var name string
		if feed.name == "" {
			name = "DefaultDcpClient"
		} else {
			name = feed.name
		}
		singleFeed, err := serverConn.StartDcpFeed(
			name, feed.sequence, feed.output, config)
		if err != nil {
			for _, nodeFeed := range feed.nodeFeeds {
				nodeFeed.dcpFeed.Close()
			}
			return memcached.ErrorInvalidFeed
		}
		// add the node to the connection map
		feedInfo := &FeedInfo{
			dcpFeed: singleFeed,
			host:    serverConn.host,
		}
		feed.nodeFeeds[serverConn.host] = feedInfo
	}
	return nil
}

func (feed *DcpFeed) dcpRequestStream(
	vb uint16, opaque uint16, flags uint32,
	vbuuid, startSequence, endSequence, snapStart, snapEnd uint64) error {

	prefix := feed.logPrefix
	vbm := feed.bucket.VBServerMap()
	if l := len(vbm.VBucketMap); int(vb) >= l {
		logging.Errorf("%v invalid vbucket id %d >= %d\n", prefix, vb, l)
		return ErrorInvalidVbucket
	}

	masterID := vbm.VBucketMap[vb][0]
	master := feed.bucket.getMasterNode(masterID)
	if master == "" {
		logging.Errorf("%v notFound master node for vbucket %d\n", prefix, vb)
		return ErrorInvalidVbucket
	}
	singleFeed, ok := feed.nodeFeeds[master]
	if !ok {
		logging.Errorf("%v notFound DcpFeed host: %q vb:%d\n", prefix, master, vb)
		return memcached.ErrorInvalidFeed
	}
	err := singleFeed.dcpFeed.DcpRequestStream(
		vb, opaque, flags, vbuuid, startSequence, endSequence,
		snapStart, snapEnd)
	if err != nil {
		return err
	}
	return nil
}

func (feed *DcpFeed) dcpCloseStream(vb, opaqueMSB uint16) error {
	prefix := feed.logPrefix
	vbm := feed.bucket.VBServerMap()
	if l := len(vbm.VBucketMap); int(vb) >= l {
		logging.Errorf("%v invalid vbucket id %d >= %d\n", prefix, vb, l)
		return ErrorInvalidVbucket
	}

	masterID := vbm.VBucketMap[vb][0]
	master := feed.bucket.getMasterNode(masterID)
	if master == "" {
		logging.Errorf("%v notFound master node for vbucket %d\n", prefix, vb)
		return ErrorInvalidVbucket
	}
	singleFeed, ok := feed.nodeFeeds[master]
	if !ok {
		logging.Errorf("%v notFound DcpFeed host: %q vb:%d", prefix, master, vb)
		return memcached.ErrorInvalidFeed
	}
	if err := singleFeed.dcpFeed.CloseStream(vb, opaqueMSB); err != nil {
		return err
	}
	return nil
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

// FeedInfo is dcp-feed from a single connection
type FeedInfo struct {
	dcpFeed *memcached.DcpFeed // DCP feed handle
	host    string             // hostname
	mu      sync.Mutex         // protects the following field.
}
