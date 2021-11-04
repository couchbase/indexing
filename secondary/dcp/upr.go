package couchbase

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/couchbase/indexing/secondary/dcp/transport"
	memcached "github.com/couchbase/indexing/secondary/dcp/transport/client"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/security"
)

// ErrorInvalidVbucket
var ErrorInvalidVbucket = errors.New("dcp.invalidVbucket")

// ErrorFailoverLog
var ErrorFailoverLog = errors.New("dcp.failoverLog")

// ErrorInvalidBucket
var ErrorInvalidBucket = errors.New("dcp.invalidBucket")

// ErrorInconsistentDcpStats
var ErrorInconsistentDcpStats = errors.New("dcp.insconsistentDcpStats")

// ErrorTimeoutDcpStats
var ErrorTimeoutDcpStats = errors.New("dcp.timeoutDcpStats")

// ErrorClosed
var ErrorClosed = errors.New("dcp.closed")

const DCP_ADD_STREAM_ACTIVE_VB_ONLY = uint32(0x10) // 16

// FailoverLog for list of vbuckets.
type FailoverLog map[uint16]memcached.FailoverLog

// Make a valid DCP feed name. These always begin with secidx:
type DcpFeedName string

func NewDcpFeedName(name string) DcpFeedName {
	return DcpFeedName(memcached.DcpFeedNamePrefix + name)
}

// GetFailoverLogs get the failover logs for a set of vbucket ids
func (b *Bucket) GetFailoverLogs(
	opaque uint16,
	vBuckets []uint16, uuid uint64,
	config map[string]interface{}) (FailoverLog, error) {
	// map vbids to their corresponding hosts
	vbHostList := make(map[string][]uint16)
	vbm := b.VBServerMap()
	for _, vb := range vBuckets {
		if l := len(vbm.VBucketMap); int(vb) >= l {
			fmsg := "DCPF[] ##%x invalid vbucket id %d >= %d"
			logging.Errorf(fmsg, opaque, vb, l)
			return nil, ErrorInvalidVbucket
		}

		masterID := vbm.VBucketMap[vb][0]
		master := b.getMasterNode(masterID)
		if master == "" {
			fmsg := "DCP[] ##%x master node not found for vbucket %d"
			logging.Errorf(fmsg, opaque, vb)
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

		name := NewDcpFeedName(fmt.Sprintf("getfailoverlog-%s-%v", b.Name, uuid))
		flags := uint32(0x0)
		singleFeed, err := serverConn.StartDcpFeed(
			name, 0, flags, nil, opaque, nil, config)
		if err != nil {
			return nil, err
		}
		defer singleFeed.Close()

		failoverlogs, err := singleFeed.DcpGetFailoverLog(opaque, vbList)
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
	kvaddrs   []string
	nodeFeeds map[string][]*FeedInfo // The DCP feeds of the individual nodes
	vbfeedm   map[uint16]*FeedInfo
	output    chan *memcached.DcpEvent // Same as C but writeably-typed
	name      DcpFeedName              // name of this DCP feed
	sequence  uint32                   // sequence number for this feed
	opaque    uint16
	flags     uint32
	// gen-server
	reqch     chan []interface{}
	finch     chan bool
	logPrefix string
	config    map[string]interface{}
	// config
	numConnections int
	activeVbOnly   bool
}

// StartDcpFeed creates and starts a new Dcp feed.
// No data will be sent on the channel unless vbuckets streams
// are requested.
func (b *Bucket) StartDcpFeed(
	name DcpFeedName, sequence, flags uint32,
	opaque uint16,
	config map[string]interface{}) (*DcpFeed, error) {

	return b.StartDcpFeedOver(name, sequence, flags, nil, opaque, config)
}

// StartDcpFeedOver creates and starts a new Dcp feed.
// No data will be sent on the channel unless vbuckets streams
// are requested. Connections will be made only to specified
// kvnodes `kvaddrs`, to connect will all kvnodes hosting the bucket,
// pass `kvaddrs` as nil
//
// configuration parameters,
//      "genChanSize", buffer channel size for control path.
//      "dataChanSize", buffer channel size for data path.
//      "numConnections", number of connections with DCP for local vbuckets.
func (b *Bucket) StartDcpFeedOver(
	name DcpFeedName,
	sequence, flags uint32,
	kvaddrs []string,
	opaque uint16,
	config map[string]interface{}) (*DcpFeed, error) {

	genChanSize := config["genChanSize"].(int)
	dataChanSize := config["dataChanSize"].(int)
	feed := &DcpFeed{
		bucket:    b,
		kvaddrs:   kvaddrs,
		nodeFeeds: make(map[string][]*FeedInfo),
		output:    make(chan *memcached.DcpEvent, dataChanSize),
		name:      name,
		sequence:  sequence,
		opaque:    opaque,
		flags:     flags,
		reqch:     make(chan []interface{}, genChanSize),
		finch:     make(chan bool),
		config:    copyconfig(config),
		logPrefix: fmt.Sprintf("DCP[%v]", name),
	}
	feed.numConnections = config["numConnections"].(int)
	feed.activeVbOnly = config["activeVbOnly"].(bool)

	feed.C = feed.output
	if err := feed.connectToNodes(kvaddrs, opaque, flags, config); err != nil {
		logging.Errorf("%v ##%x Bucket::StartDcpFeedOver : error %v in connectToNodes",
			feed.logPrefix, opaque, err)
		return nil, ErrorInvalidBucket
	}
	go feed.genServer(feed.reqch, opaque)
	return feed, nil
}

const (
	ufCmdRequestStream byte = iota + 1
	ufCmdCloseStream
	ufCmdGetSeqnos
	ufCmdGetStats
	ufCmdClose
)

// DcpRequestStream starts a stream for a vb on a feed
// and immediately returns, it is upto the channel listener
// to detect StreamBegin.
// Synchronous call.
func (feed *DcpFeed) DcpRequestStream(
	vb uint16, opaque uint16, flags uint32,
	vbuuid, startSequence, endSequence, snapStart, snapEnd uint64,
	manifestUID, scopeId string, collectionIds []string) error {

	// only request active vbucket
	if feed.activeVbOnly {
		flags = flags | DCP_ADD_STREAM_ACTIVE_VB_ONLY
	}

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{
		ufCmdRequestStream, vb, opaque, flags, vbuuid, startSequence,
		endSequence, snapStart, snapEnd,
		manifestUID, scopeId, collectionIds,
		respch}
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

// DcpFeedName returns feed name
func (feed *DcpFeed) DcpFeedName() string {
	return string(feed.name)
}

// DcpGetSeqnos return the list of seqno for vbuckets,
// synchronous call.
func (feed *DcpFeed) DcpGetSeqnos() (map[uint16]uint64, error) {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{ufCmdGetSeqnos, respch}
	resp, err := failsafeOp(feed.reqch, respch, cmd, feed.finch)
	if err = opError(err, resp, 1); err != nil {
		return nil, err
	}
	return resp[0].(map[uint16]uint64), nil
}

// Close DcpFeed. Synchronous call.
func (feed *DcpFeed) Close() error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{ufCmdClose, respch}
	resp, err := failsafeOp(feed.reqch, respch, cmd, feed.finch)
	return opError(err, resp, 0)
}

func (feed *DcpFeed) GetStats() map[string]interface{} {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{ufCmdGetStats, respch}
	resp, err := failsafeOp(feed.reqch, respch, cmd, feed.finch)
	if err == nil {
		return resp[0].(map[string]interface{})
	}
	return nil
}

func (feed *DcpFeed) genServer(reqch chan []interface{}, opaque uint16) {
	closeNodeFeeds := func() {
		for _, nodeFeeds := range feed.nodeFeeds {
			for _, singleFeed := range nodeFeeds {
				if singleFeed != nil {
					singleFeed.dcpFeed.Close()
				}
			}
		}
		feed.nodeFeeds = nil
	}

	defer func() { // panic safe
		close(feed.finch)
		if r := recover(); r != nil {
			logging.Errorf("%v ##%x crashed: %v\n", feed.logPrefix, opaque, r)
			logging.Errorf("%s", logging.StackTrace())
		}
		closeNodeFeeds()
		close(feed.output)
	}()

loop:
	for {
		select {
		case msg := <-reqch:
			switch msg[0].(type) {
			case byte:
				cmd := msg[0].(byte)
				switch cmd {
				case ufCmdRequestStream:
					vb, opaque := msg[1].(uint16), msg[2].(uint16)
					flags, vbuuid := msg[3].(uint32), msg[4].(uint64)
					startSeq, endSeq := msg[5].(uint64), msg[6].(uint64)
					snapStart, snapEnd := msg[7].(uint64), msg[8].(uint64)

					manifestUID := msg[9].(string)
					scopeId := msg[10].(string)
					collectionIds := msg[11].([]string)

					err := feed.dcpRequestStream(
						vb, opaque, flags, vbuuid, startSeq, endSeq,
						snapStart, snapEnd,
						manifestUID, scopeId, collectionIds)
					respch := msg[12].(chan []interface{})
					respch <- []interface{}{err}

				case ufCmdCloseStream:
					vb, opaqueMSB := msg[1].(uint16), msg[2].(uint16)
					err := feed.dcpCloseStream(vb, opaqueMSB)
					respch := msg[3].(chan []interface{})
					respch <- []interface{}{err}

				case ufCmdGetSeqnos:
					respch := msg[1].(chan []interface{})
					seqnos, err := feed.dcpGetSeqnos()
					respch <- []interface{}{seqnos, err}

				case ufCmdGetStats:
					respch := msg[1].(chan []interface{})
					dcpStats := feed.getStats()
					respch <- []interface{}{dcpStats, nil}

				case ufCmdClose:
					closeNodeFeeds()
					respch := msg[1].(chan []interface{})
					respch <- []interface{}{nil}
					break loop
				}
			case transport.CommandCode:
				cmd := msg[0].(transport.CommandCode)
				switch cmd {
				case transport.DCP_STREAMEND:
					feed.cleanupVb(msg)
				default:
					logging.Fatalf("%v DcpFeed::genServer Should not receive a message other than DCP_STREAMEND, msg: %v", feed.logPrefix, msg)
					break loop
				}
			}
		}
	}
}

func (feed *DcpFeed) connectToNodes(
	kvaddrs []string, opaque uint16, flags uint32, config map[string]interface{}) error {

	prefix := feed.logPrefix
	kvcache := make(map[string]bool)
	m, err := feed.bucket.GetVBmap(kvaddrs)
	if err != nil {
		fmsg := "%v ##%x GetVBmap(%v) failed: %v\n"
		logging.Errorf(fmsg, prefix, opaque, kvaddrs, err)
		return memcached.ErrorInvalidFeed
	}
	for kvaddr := range m {
		kvcache[kvaddr] = true
	}
	for _, serverConn := range feed.bucket.getConnPools() {
		if _, ok := kvcache[serverConn.host]; !ok {
			continue
		}
		nodeFeeds, ok := feed.nodeFeeds[serverConn.host]
		if ok {
			for _, singleFeed := range nodeFeeds {
				singleFeed.dcpFeed.Close()
			}
		}
		nodeFeeds = make([]*FeedInfo, 0)
		// and continue to spawn a new one ...

		var name DcpFeedName
		if feed.name == "" {
			name = NewDcpFeedName("DefaultDcpClient")
		} else {
			name = feed.name
		}
		for i := 0; i < feed.numConnections; i++ {
			feedname := DcpFeedName(fmt.Sprintf("%v/%d", name, i))
			singleFeed, err := serverConn.StartDcpFeed(
				feedname, feed.sequence, flags, feed.output, opaque, feed.reqch, config)
			if err != nil {
				for _, singleFeed := range nodeFeeds {
					singleFeed.dcpFeed.Close()
				}
				fmsg := "%v ##%x DcpFeed::connectToNodes StartDcpFeed failed for %v with err %v\n"
				logging.Errorf(fmsg, prefix, opaque, feedname, err)
				return memcached.ErrorInvalidFeed
			}
			// add the node to the connection map
			feedInfo := &FeedInfo{
				vbnos:   make([]uint16, 0),
				dcpFeed: singleFeed,
				host:    serverConn.host,
			}
			nodeFeeds = append(nodeFeeds, feedInfo)
		}
		feed.nodeFeeds[serverConn.host] = nodeFeeds
	}
	return nil
}

func (feed *DcpFeed) reConnectToNodes(
	opaque uint16, flags uint32, config map[string]interface{}) {

	for _, serverConn := range feed.bucket.getConnPools() {
		nodeFeeds, ok := feed.nodeFeeds[serverConn.host]
		if !ok {
			continue
		}

		for i, nodeFeed := range nodeFeeds {
			if nodeFeed != nil {
				continue
			}

			var name DcpFeedName
			if feed.name == "" {
				name = NewDcpFeedName("DefaultDcpClient")
			} else {
				name = feed.name
			}
			feedname := DcpFeedName(fmt.Sprintf("%v/%d", name, i))
			singleFeed, err := serverConn.StartDcpFeed(
				feedname, feed.sequence, flags, feed.output, opaque, feed.reqch, config)
			if err != nil {
				fmsg := "%v ##%x DcpFeed::reConnectToNodes StartDcpFeed failed for %v with err %v\n"
				logging.Errorf(fmsg, feed.logPrefix, opaque, feedname, err)
				continue
			}
			// add the node to the connection map
			feedInfo := &FeedInfo{
				vbnos:   make([]uint16, 0),
				dcpFeed: singleFeed,
				host:    serverConn.host,
			}
			nodeFeeds[i] = feedInfo
		}
	}
}

func (feed *DcpFeed) dcpRequestStream(
	vb uint16, opaque uint16, flags uint32,
	vbuuid, startSequence, endSequence, snapStart, snapEnd uint64,
	manifestUID, scopeId string, collectionIds []string) error {

	prefix := feed.logPrefix
	vbm := feed.bucket.VBServerMap()
	if l := len(vbm.VBucketMap); int(vb) >= l {
		fmsg := "%v ##%x invalid vbucket id %d >= %d\n"
		logging.Errorf(fmsg, prefix, opaque, vb, l)
		return ErrorInvalidVbucket
	}

	masterID := vbm.VBucketMap[vb][0]
	master := feed.bucket.getMasterNode(masterID)
	if master == "" {
		fmsg := "%v ##%x notFound master node for vbucket %d\n"
		logging.Errorf(fmsg, prefix, opaque, vb)
		return ErrorInvalidVbucket
	} else if len(feed.nodeFeeds[master]) == 0 {
		fmsg := "%v ##%x len(feed.nodeFeeds[master]) is \"0\"." +
			" Master node for vb:%d is %v\n"
		logging.Errorf(fmsg, prefix, opaque, vb, master)
		return ErrorInvalidVbucket
	}

	feed.reConnectToNodes(feed.opaque, feed.flags, feed.config)

	var err error

	for i := 0; i < len(feed.nodeFeeds[master]); i++ {
		singleFeed, ok := addtofeed(feed.nodeFeeds[master])
		if !ok {
			fmsg := "%v ##%x notFound DcpFeed host: %q vb:%d\n"
			logging.Errorf(fmsg, prefix, opaque, master, vb)
			return memcached.ErrorInvalidFeed
		}
		err = singleFeed.dcpFeed.DcpRequestStream(
			vb, opaque, flags, vbuuid, startSequence, endSequence,
			snapStart, snapEnd,
			manifestUID, scopeId, collectionIds)
		if err != nil {
			fmsg := "%v ##%x DcpFeed %v failed, trying next, err: %v"
			logging.Errorf(fmsg, prefix, opaque, singleFeed.dcpFeed.Name(), err)
			feed.nodeFeeds[master] = purgeFeed(feed.nodeFeeds[master], singleFeed)
			continue
		}
		singleFeed.vbnos = append(singleFeed.vbnos, vb)
		break
	}
	return err
}

func (feed *DcpFeed) dcpCloseStream(vb, opaqueMSB uint16) error {
	prefix := feed.logPrefix
	vbm := feed.bucket.VBServerMap()
	if l := len(vbm.VBucketMap); int(vb) >= l {
		fmsg := "%v ##%x invalid vbucket id %d >= %d\n"
		logging.Errorf(fmsg, prefix, opaqueMSB, vb, l)
		return ErrorInvalidVbucket
	}

	masterID := vbm.VBucketMap[vb][0]
	master := feed.bucket.getMasterNode(masterID)
	if master == "" {
		fmsg := "%v ##%x notFound master node for vbucket %d\n"
		logging.Errorf(fmsg, prefix, opaqueMSB, vb)
		return ErrorInvalidVbucket
	}
	singleFeed, err := feed.getSingleFeed(master, vb, prefix, opaqueMSB)
	if err != nil {
		return err
	}
	if err := singleFeed.dcpFeed.CloseStream(vb, opaqueMSB); err != nil {
		return err
	}
	return nil
}

func (feed *DcpFeed) getSingleFeed(master string, vb uint16, prefix string, opaqueMSB uint16) (*FeedInfo, error) {
	singleFeed, ok := removefromfeed(feed.nodeFeeds[master], vb)

	if !ok {
		// In case where a node gets added with localhost address first
		// and then ns_server changes the IP address to a public/private
		// IP, then the feed.nodeFeeds book-keeping becomes inconsistent.
		//
		// E.g., when a node is added with 127.0.0.1 as IP address and
		// streams are opened, the key to nodeFeeds would be 127.0.0.1
		// When ns_server changes the IP address, then feed.bucket.getMasterNode
		// would return the changed IP address. This new IP address will not
		// be a part of nodeFeeds book-keeping and this call will fail.
		//
		// Currently, shutdownVBuckets code path does not handle errors. So,
		// the memcached.ErrorInvalidFeed error gets silently ignored and
		// the vbucket will never be shutdown. This will lead to indexer endup
		// in a repair loop. To fix this, we check if the master node's IP address
		// is same as the local IP address. If yes, we go ahead and shutdown the
		// stream by using the local kvaddress
		if islocalIP, err := isLocalIP(master); err != nil {
			logging.Errorf("%v ##%x err: %v observed while retrieving local IP "+
				"for master: %v", prefix, opaqueMSB, master)
			return nil, memcached.ErrorInvalidFeed
		} else if islocalIP && len(feed.kvaddrs) == 1 {
			fmsg := "%v ##%x notFound DcpFeed host: %q vb:%d, trying with kvaddrs: %v"
			logging.Warnf(fmsg, prefix, opaqueMSB, master, vb, feed.kvaddrs[0])
			// Trying with local address. kvaddrs[0] is the local kv address
			singleFeed, ok = removefromfeed(feed.nodeFeeds[feed.kvaddrs[0]], vb)
			if !ok {
				fmsg := "%v ##%x notFound DcpFeed host: %q vb:%d with kvaddrs: %v"
				logging.Errorf(fmsg, prefix, opaqueMSB, master, vb, feed.kvaddrs[0])
				return nil, memcached.ErrorInvalidFeed
			}
		} else {
			fmsg := "%v ##%x notFound DcpFeed host: %q vb:%d, kvaddrs: %v"
			logging.Errorf(fmsg, prefix, opaqueMSB, master, vb, feed.kvaddrs)
			return nil, memcached.ErrorInvalidFeed
		}
	}
	return singleFeed, nil
}

func isLocalIP(hostport string) (bool, error) {

	host, _, err := net.SplitHostPort(hostport)
	if err != nil {
		return false, err
	}

	localHost, err := getLocalIP()
	if err != nil {
		return false, err
	}

	inputHost := net.ParseIP(host)

	if localHost.Equal(inputHost) {
		return true, nil
	}
	return false, nil
}

// getLocalIP returns the first external IP configured for the first
// interface connected to this node. This may be IPv4 or IPv6.
// (This method is an exact implementation of GetLocalIP in common/util.go)
// (Making a copy of it here to avoid package dependency cycle.)
func getLocalIP() (net.IP, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for _, iface := range interfaces {
		if (iface.Flags & net.FlagUp) == 0 {
			continue // interface down
		}
		if (iface.Flags & net.FlagLoopback) != 0 {
			continue // loopback interface
		}
		addrs, err := iface.Addrs()
		if err != nil {
			return nil, err
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip != nil && !ip.IsLoopback() {
				if security.IsIpv6() {
					if ip = ip.To16(); ip != nil {
						return ip, nil
					}
				} else {
					if ip = ip.To4(); ip != nil {
						return ip, nil
					}
				}
			}
		}
	}
	return nil, errors.New("cannot find local IP address")
}

func (feed *DcpFeed) dcpGetSeqnos() (map[uint16]uint64, error) {
	count := len(feed.nodeFeeds)
	ch := make(chan []interface{}, count)
	for _, nodeFeeds := range feed.nodeFeeds {
		for _, singleFeed := range nodeFeeds {
			if singleFeed == nil {
				continue
			}
			go func() {
				nodeTs, err := singleFeed.dcpFeed.DcpGetSeqnos()
				ch <- []interface{}{nodeTs, err}
			}()
			break
		}
	}

	prefix := feed.logPrefix
	seqnos := make(map[uint16]uint64)
	timeout := time.After(3 * time.Second)
	for count > 0 {
		select {
		case <-timeout:
			fmsg := "%v stats-seqno timed-out %s waiting for stats"
			logging.Errorf(fmsg, prefix, timeout)
			return nil, ErrorTimeoutDcpStats
		case result := <-ch:
			nodeTs := result[0].(map[uint16]uint64)
			if result[1] != nil {
				return nil, result[1].(error)
			}
			for vbno, seqno := range nodeTs {
				if prev, ok := seqnos[vbno]; !ok || prev < seqno {
					seqnos[vbno] = seqno
				}
			}
		}
		count--
	}
	return seqnos, nil
}

func (feed *DcpFeed) getStats() map[string]interface{} {
	dcpStats := make(map[string]interface{}, 0)
	for _, nodeFeeds := range feed.nodeFeeds {
		for _, singleFeed := range nodeFeeds {
			if singleFeed == nil {
				continue
			}
			if stats := singleFeed.dcpFeed.GetStats(); stats != nil {
				key := fmt.Sprintf("DCPT[%v] ##%x", singleFeed.dcpFeed.Name(), singleFeed.dcpFeed.Opaque())
				dcpStats[key] = singleFeed.dcpFeed.GetStats()
			}
		}
	}
	return dcpStats
}

func addtofeed(nodeFeeds []*FeedInfo) (*FeedInfo, bool) {
	if len(nodeFeeds) == 0 {
		return nil, false
	}
	var feedinfo *FeedInfo
	for _, fi := range nodeFeeds {
		if fi == nil {
			continue
		} else if feedinfo == nil {
			feedinfo = fi
		} else if len(fi.vbnos) < len(feedinfo.vbnos) {
			feedinfo = fi
		}
	}
	// It is possible that all the nodeFeeds available are nil
	// and therefore feedinfo can be nil
	return feedinfo, (feedinfo != nil)
}

func removefromfeed(nodeFeeds []*FeedInfo, forvb uint16) (*FeedInfo, bool) {
	if len(nodeFeeds) == 0 {
		return nil, false
	}
	for _, singleFeed := range nodeFeeds {
		if singleFeed == nil {
			continue
		}
		for i, vbno := range singleFeed.vbnos {
			if vbno == forvb {
				copy(singleFeed.vbnos[i:], singleFeed.vbnos[i+1:])
				n := len(singleFeed.vbnos) - 1
				singleFeed.vbnos = singleFeed.vbnos[:n]
				return singleFeed, true
			}
		}
	}
	return nil, false
}

func purgeFeed(nodeFeeds []*FeedInfo, singleFeed *FeedInfo) []*FeedInfo {
	name := singleFeed.dcpFeed.Name()
	for i, nodeFeed := range nodeFeeds {
		if nodeFeed != nil && nodeFeed.dcpFeed.Name() == name {
			nodeFeed.dcpFeed.Close()
			nodeFeeds[i] = nil
			return nodeFeeds
		}
	}
	return nodeFeeds
}

func (feed *DcpFeed) cleanupVb(msg []interface{}) {
	dcpFeed := msg[1].(*memcached.DcpFeed)
	forvb := msg[2].(uint16)
	// Delete the vb corresponding to the node feed
	found := false
outerloop:
	for _, nodeFeeds := range feed.nodeFeeds {
		for _, singleFeed := range nodeFeeds {
			if singleFeed != nil && singleFeed.dcpFeed.Name() == dcpFeed.Name() {
				_, found = removefromfeed(nodeFeeds, forvb)
				break outerloop
			}
		}
	}
	if !found {
		logging.Warnf("%v DcpFeed::genServer Could not find vb:%v for feed:%v in book-keeping", feed.logPrefix, forvb, dcpFeed.Name())
	}
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

// FeedInfo is dcp-feed from a single connection.
type FeedInfo struct {
	vbnos   []uint16
	dcpFeed *memcached.DcpFeed // DCP feed handle
	host    string             // hostname
	mu      sync.Mutex         // protects the following field.
}

func copyconfig(config map[string]interface{}) map[string]interface{} {
	nconfig := make(map[string]interface{})
	for k, v := range config {
		nconfig[k] = v
	}
	return nconfig
}
