// go implementation of dcp client.
// See https://github.com/couchbaselabs/cbupr/blob/master/transport-spec.md

package memcached

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/indexing/secondary/common/collections"
	"github.com/couchbase/indexing/secondary/dcp/transport"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/projector/memThrottler"
	"github.com/couchbase/indexing/secondary/stats"
	"github.com/couchbase/logstats/logstats"
)

const dcpMutationExtraLen = 16
const bufferAckThreshold = 0.2
const opaqueOpen = 0xBEAF0001
const opaqueFailover = 0xDEADBEEF
const opaqueGetseqno = 0xDEADBEEF
const openConnFlag = uint32(0x1)
const includeXATTR = uint32(0x4)
const dcpJSON = uint8(0x1)
const dcpXATTR = uint8(0x4)
const bufferAckPeriod = 20

// Length of extras when DCP seqno message is received
const dcpSeqnoAdvExtrasLen = 8

// Length of extras when DCP oso snapshot message is received
const osoSnapshotExtrasLen = 4

// error codes
var ErrorInvalidLog = errors.New("couchbase.errorInvalidLog")

// ErrorConnection
var ErrorConnection = errors.New("dcp.connection")

// ErrorInvalidFeed
var ErrorInvalidFeed = errors.New("dcp.invalidFeed")

// ErrorEnableCollections
var ErrorEnableCollections = errors.New("dcp.EnableCollections")
var ErrorCollectionsNotEnabled = errors.New("dcp.ErrorCollectionsNotEnabled")

var DcpFeedNamePrefix = "secidx:"
var DcpFeedNameCompPrefix = "proj-"
var DcpFeedPrefix = DcpFeedNamePrefix + DcpFeedNameCompPrefix

// 0-2ms, 2-10ms, 10-100ms, 100-500ms, 500ms-1s, 1-10s, 10-30s, 30s-Inf
var projBlockedDur = []int64{0, 2, 10, 100, 500, 1000, 10000, 30000}
var blockdThresholdDur = time.Duration(projBlockedDur[len(projBlockedDur)-1]) * time.Millisecond

// DcpFeed represents an DCP feed. A feed contains a connection to a single
// host and multiple vBuckets
type DcpFeed struct {
	conn        *Client // connection to DCP producer
	name        *DcpFeedname2
	opaque      uint16
	outch       chan<- *DcpEvent      // Exported channel for receiving DCP events
	vbstreams   map[uint16]*DcpStream // vb->stream mapping
	vbstreamsMu sync.RWMutex

	// genserver
	reqch     chan []interface{}
	supvch    chan []interface{}
	finch     chan bool
	logPrefix string
	// Collections
	collectionsAware bool
	osoSnapshot      bool
	isIncrBuild      bool // Set to true for Incremental builds (only from 7.0 cluster version)
	// stats
	toAckBytes         uint32    // bytes client has read
	maxAckBytes        uint32    // Max buffer control ack bytes
	lastAckTime        time.Time // last time when BufferAck was sent
	stats              *DcpStats // Stats for dcp client
	done               uint32
	enableReadDeadline int32 // 0 => Read deadline is disabled in doReceive, 1 => enabled

	// Book-keeping for verifying sequence order.
	// TODO: This introduces a map lookup in mutation path. Need to anlayse perf implication.
	seqOrders map[uint16]transport.SeqOrderState // vb ==> state maintained for checking seq order

	truncName string

	mutationQueue             *AtomicMutationQueue
	useAtomicMutationQueue    bool
	controlDataPathSeparation bool
	closeMutQueue             chan bool
	dequeueDoneCh             chan bool // DequeueMutations will close this channel upon exit

	mu sync.Mutex // Avoid concurrent setting of connection deadline.

	syncRespCh chan []interface{}
	syncRespMu sync.Mutex
}

// NewDcpFeed creates a new DCP Feed.
func NewDcpFeed(
	mc *Client, name *DcpFeedname2, outch chan<- *DcpEvent,
	opaque uint16,
	supvch chan []interface{},
	config map[string]interface{}) (*DcpFeed, error) {

	genChanSize := config["genChanSize"].(int)
	dataChanSize := config["dataChanSize"].(int)
	feed := &DcpFeed{
		name:      name,
		outch:     outch,
		opaque:    opaque,
		vbstreams: make(map[uint16]*DcpStream),
		reqch:     make(chan []interface{}, genChanSize),
		supvch:    supvch,
		finch:     make(chan bool),
		// TODO: would be nice to add host-addr as part of prefix.
		logPrefix:     name.StreamLogPrefix(),
		stats:         &DcpStats{StreamNo: fmt.Sprintf("%v", name.StreamId)},
		seqOrders:     make(map[uint16]transport.SeqOrderState),
		closeMutQueue: make(chan bool, 1),
		dequeueDoneCh: make(chan bool),
	}

	feed.mutationQueue = NewAtomicMutationQueue()
	feed.truncName = name.StreamLogPrefix()
	if val, ok := config["useMutationQueue"]; ok {
		feed.useAtomicMutationQueue = val.(bool)
	}
	if val, ok := config["mutation_queue.control_data_path_separation"]; ok {
		feed.controlDataPathSeparation = val.(bool)
	}

	newFeedName, err := truncFeedName(name)
	if err != nil {
		logging.Infof("%v ##%x NewDcpFeed error truncating feed name %v",
			feed.logPrefix, opaque, name.String())
		return nil, err
	}

	if newFeedName != name.StreamLogPrefix() {
		logging.Infof("%v ##%x NewDcpFeed using new feed name %v for feed %v",
			feed.logPrefix, opaque, newFeedName, name.StreamLogPrefix())
		feed.truncName = newFeedName
	}

	mc.Hijack()
	feed.conn = mc
	rcvch := make(chan []interface{}, dataChanSize)

	feed.stats.Init()
	feed.stats.rcvch = rcvch
	feed.stats.mutationQueue = feed.mutationQueue
	feed.lastAckTime = time.Now()

	if _, ok := config["collectionsAware"]; ok {
		feed.collectionsAware = config["collectionsAware"].(bool)
		feed.isIncrBuild = feed.collectionsAware
	}

	if _, ok := config["osoSnapshot"]; ok {
		feed.osoSnapshot = config["osoSnapshot"].(bool)
	}

	if feed.useAtomicMutationQueue {
		go feed.DequeueMutations(rcvch, feed.finch)
	}
	go feed.genServer(opaque, feed.reqch, feed.finch, rcvch, config)
	go feed.doReceive(rcvch, feed.finch, mc)
	logging.Infof("%v ##%x feed started. useMutationQueue: %v, controlDataPathSeparation: %v ...",
		feed.logPrefix, opaque, feed.useAtomicMutationQueue, feed.controlDataPathSeparation)
	return feed, nil
}

func (feed *DcpFeed) Name() string {
	return feed.name.StreamLogPrefix()
}

func (feed *DcpFeed) Opaque() uint16 {
	return feed.opaque
}

func (feed *DcpFeed) GetStats() interface{} {
	if atomic.LoadUint32(&feed.done) == 0 {
		return interface{}(feed.stats)
	}
	return nil
}

// DcpOpen to connect with a DCP producer.
// Name: name of te DCP connection
// sequence: sequence number for the connection
// bufsize: max size of the application
func (feed *DcpFeed) DcpOpen(
	name string, sequence, flags, bufsize uint32, opaque uint16) error {

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{
		dfCmdOpen, name, sequence, flags, bufsize, opaque, respch,
	}
	resp, err := failsafeOp(feed.reqch, respch, cmd, feed.finch)
	return opError(err, resp, 0)
}

// DcpGetFailoverLog for given list of vbuckets.
func (feed *DcpFeed) DcpGetFailoverLog(
	opaque uint16, vblist []uint16) (map[uint16]*FailoverLog, error) {

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{dfCmdGetFailoverlog, opaque, vblist, respch}
	resp, err := failsafeOp(feed.reqch, respch, cmd, feed.finch)
	if err = opError(err, resp, 1); err != nil {
		return nil, err
	}
	return resp[0].(map[uint16]*FailoverLog), err
}

// DcpGetSeqnos for vbuckets hosted by this node.
func (feed *DcpFeed) DcpGetSeqnos() (map[uint16]uint64, error) {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{dfCmdGetSeqnos, respch}
	resp, err := failsafeOp(feed.reqch, respch, cmd, feed.finch)
	if err = opError(err, resp, 1); err != nil {
		return nil, err
	}
	return resp[0].(map[uint16]uint64), nil
}

// DcpRequestStream for a single vbucket.
func (feed *DcpFeed) DcpRequestStream(vbno, opaqueMSB uint16, flags uint32,
	vuuid, startSequence, endSequence, snapStart, snapEnd uint64,
	manifestUID, scopeId string, collectionIds []string) error {

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{
		dfCmdRequestStream, vbno, opaqueMSB, flags, vuuid,
		startSequence, endSequence, snapStart, snapEnd,
		manifestUID, scopeId, collectionIds,
		respch}
	resp, err := failsafeOp(feed.reqch, respch, cmd, feed.finch)
	return opError(err, resp, 0)
}

// CloseStream for specified vbucket.
func (feed *DcpFeed) CloseStream(vbno, opaqueMSB uint16) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{dfCmdCloseStream, vbno, opaqueMSB, respch}
	resp, err := failsafeOp(feed.reqch, respch, cmd, feed.finch)
	return opError(err, resp, 0)
}

// Close this DcpFeed.
func (feed *DcpFeed) Close() error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{dfCmdClose, respch}
	_, err := failsafeOp(feed.reqch, respch, cmd, feed.finch)
	return err
}

const (
	dfCmdOpen byte = iota + 1
	dfCmdGetFailoverlog
	dfCmdGetSeqnos
	dfCmdRequestStream
	dfCmdCloseStream
	dfCmdClose
)

// Intermediate go-routine to dequeue mutations and writes to rcvch
// If there are no mutations in the queue, then Dequeue would block
// until mutations arrive
func (feed *DcpFeed) DequeueMutations(rcvch chan []interface{}, abortCh chan bool) {

	defer func() {
		close(rcvch)
		close(feed.dequeueDoneCh)

		if feed.useAtomicMutationQueue && feed.controlDataPathSeparation {
			syncRespCh := feed.getSyncRespCh()
			if syncRespCh != nil {
				close(syncRespCh)
			}
		}
	}()

	for {
		pkt, bytes := feed.mutationQueue.Dequeue(abortCh, feed.closeMutQueue)
		if pkt != nil {

			// if control and data path separation is present, handlePacket in DequeueMutations()
			// otherwise, handlePacket in genServer()
			if feed.controlDataPathSeparation {
				switch pkt.Opcode {
				case transport.DCP_OPEN, transport.HELO,
					transport.DCP_CONTROL:
					respCh := feed.getSyncRespCh()
					respCh <- []interface{}{pkt, bytes}
					continue
				}

				switch feed.handlePacket(pkt, bytes) {
				case "exit":
					feed.sendStreamEnd(feed.outch)
					return
				}
			} else {
				rcvch <- []interface{}{pkt, bytes}
			}

			sendAck := false
			switch pkt.Opcode {
			case transport.DCP_MUTATION,
				transport.DCP_DELETION, transport.DCP_EXPIRATION,
				transport.DCP_STREAMEND, transport.DCP_SNAPSHOT,
				transport.DCP_SYSTEM_EVENT, transport.DCP_SEQNO_ADVANCED,
				transport.DCP_OSO_SNAPSHOT:
				sendAck = true
			default:
				sendAck = false

			}
			if err := feed.sendBufferAck(sendAck, uint32(bytes)); err != nil {
				return // Exit
			}
		} else {
			return // Exit
		}
	}
}

func (feed *DcpFeed) genServer(
	opaque uint16, reqch chan []interface{}, finch chan bool,
	rcvch chan []interface{},
	config map[string]interface{}) {

	defer func() { // panic safe
		if r := recover(); r != nil {
			logging.Errorf("%v ##%x crashed: %v\n", feed.logPrefix, opaque, r)
			logging.Errorf("%s", logging.StackTrace())
			feed.sendStreamEnd(feed.outch)
		}
		close(feed.finch)
		// Wait for DequeueMutations() to exit before closing connection
		// as DequeueMutations() will sendAck on connection
		if feed.useAtomicMutationQueue {
			<-feed.dequeueDoneCh
		}

		feed.conn.Close()
		feed.conn = nil
		atomic.StoreUint32(&feed.done, 1)
		//Update closed in stats object and log the stats before exiting
		feed.stats.Closed.Set(true)
		feed.logStats()
		logging.Infof("%v ##%x ... stopped\n", feed.logPrefix, opaque)
	}()

	prefix := feed.logPrefix
	latencyTick := int64(60 * 1000) // in milli-seconds
	if val, ok := config["latencyTick"]; ok && val != nil {
		latencyTick = int64(val.(int)) // in milli-seconds
	}
	latencyTm := time.NewTicker(time.Duration(latencyTick) * time.Millisecond)
	defer func() {
		latencyTm.Stop()
	}()

loop:
	for {
		select {
		case <-latencyTm.C:
			if feed.stats.TotalSpurious.Value() > 0 {
				fmsg := "%v detected spurious messages for inactive streams\n"
				logging.Fatalf(fmsg, feed.logPrefix)
			}

		case msg := <-reqch:
			cmd := msg[0].(byte)
			switch cmd {
			case dfCmdOpen:
				name, sequence := msg[1].(string), msg[2].(uint32)
				flags := msg[3].(uint32)
				bufsize, opaque := msg[4].(uint32), msg[5].(uint16)
				respch := msg[6].(chan []interface{})
				err := feed.doDcpOpen(
					name, sequence, flags, bufsize, opaque, rcvch)
				respch <- []interface{}{err}

			case dfCmdGetFailoverlog:
				opaque := msg[1].(uint16)
				vblist, respch := msg[2].([]uint16), msg[3].(chan []interface{})
				if feed.lenVbStreams() > 0 {
					fmsg := "%v %##x active streams in doDcpGetFailoverLog"
					logging.Errorf(fmsg, prefix, opaque)
					respch <- []interface{}{nil, ErrorInvalidFeed}
				}
				flog, err := feed.doDcpGetFailoverLog(opaque, vblist, rcvch)
				respch <- []interface{}{flog, err}

			case dfCmdGetSeqnos:
				respch := msg[1].(chan []interface{})
				seqnos, err := feed.doDcpGetSeqnos(rcvch)
				respch <- []interface{}{seqnos, err}

			case dfCmdRequestStream:
				vbno, opaqueMSB := msg[1].(uint16), msg[2].(uint16)
				flags, vuuid := msg[3].(uint32), msg[4].(uint64)
				startSequence, endSequence := msg[5].(uint64), msg[6].(uint64)
				snapStart, snapEnd := msg[7].(uint64), msg[8].(uint64)

				manifestUID := msg[9].(string)
				scopeId := msg[10].(string)
				collectionIds := msg[11].([]string)

				err := feed.doDcpRequestStream(
					vbno, opaqueMSB, flags, vuuid,
					startSequence, endSequence, snapStart, snapEnd,
					manifestUID, scopeId, collectionIds)

				respch := msg[12].(chan []interface{})
				respch <- []interface{}{err}

			case dfCmdCloseStream:
				vbno, opaqueMSB := msg[1].(uint16), msg[2].(uint16)
				respch := msg[3].(chan []interface{})
				err := feed.doDcpCloseStream(vbno, opaqueMSB)
				respch <- []interface{}{err}

			case dfCmdClose:
				if feed.useAtomicMutationQueue && feed.controlDataPathSeparation {
					feed.closeMutQueue <- true
					<-feed.dequeueDoneCh // wait for dequeueMutations to exit before sending streamEnds
				}
				feed.sendStreamEnd(feed.outch)
				respch := msg[1].(chan []interface{})
				respch <- []interface{}{nil}
				break loop
			}

		case resp, ok := <-rcvch:
			if !ok {
				feed.sendStreamEnd(feed.outch)
				break loop
			}
			pkt, bytes := resp[0].(*transport.MCRequest), resp[1].(int)
			switch feed.handlePacket(pkt, bytes) {
			case "exit":
				feed.sendStreamEnd(feed.outch)
				break loop
			}
		}
	}
}

func (feed *DcpFeed) isClosed() bool {
	select {
	case <-feed.finch:
		return true
	default:
	}
	return false
}

func (feed *DcpFeed) handlePacket(
	pkt *transport.MCRequest, bytes int) string {

	var event *DcpEvent
	feed.stats.TotalBytes.Add(uint64(bytes))
	res := &transport.MCResponse{
		Opcode: pkt.Opcode,
		Cas:    pkt.Cas,
		Opaque: pkt.Opaque,
		Status: transport.Status(pkt.VBucket),
		Extras: pkt.Extras,
		Key:    pkt.Key,
		Body:   pkt.Body,
	}
	vb := vbOpaque(pkt.Opaque)

	sendAck := false
	prefix := feed.logPrefix
	stream, _ := feed.getFromVbStreams(vb)
	if stream == nil {
		feed.stats.TotalSpurious.Add(1)
		// log first 10000 spurious messages
		logok := feed.stats.TotalSpurious.Value() < 10000
		// then log 1 spurious message for every 1000.
		logok = logok || (feed.stats.TotalSpurious.Value()%1000) == 1
		if logok {
			fmsg := "%v spurious %v for %d: %#v\n"
			arg1 := logging.TagUD(pkt)
			logging.Fatalf(fmsg, prefix, pkt.Opcode, vb, arg1)
		}
		return "ok" // yeah it not _my_ mistake...
	}

	defer func() { feed.stats.Dcplatency.Add(computeLatency(stream)) }()

	stream.LastSeen = time.Now().UnixNano()
	switch pkt.Opcode {
	case transport.DCP_STREAMREQ:
		event = newDcpEvent(pkt, stream)
		feed.handleStreamRequest(res, vb, stream, event)
		feed.stats.TotalStreamReq.Add(1)

		if !feed.osoSnapshot {
			feed.seqOrders[vb] = transport.NewSeqOrderState()
		}

	case transport.DCP_MUTATION, transport.DCP_DELETION,
		transport.DCP_EXPIRATION:
		event = newDcpEvent(pkt, stream)
		stream.Seqno = event.Seqno
		feed.stats.TotalMutation.Add(1)
		sendAck = true

		feed.checkSeqOrder(event, vb, pkt.Opcode)

	case transport.DCP_STREAMEND:
		event = newDcpEvent(pkt, stream)
		sendAck = true
		feed.deleteFromVbStreams(vb)
		feed.supvch <- []interface{}{transport.DCP_STREAMEND, feed, vb}
		fmsg := "%v ##%x DCP_STREAMEND for vb %d\n"
		logging.Debugf(fmsg, prefix, stream.AppOpaque, vb)
		feed.stats.TotalStreamEnd.Add(1)

		if !feed.osoSnapshot {
			if s, ok := feed.seqOrders[vb]; ok && s != nil && s.GetErrCount() != 0 {
				logging.Fatalf("%v error count for sequence number ordering is %v", prefix, s.GetErrCount())
			}
			feed.seqOrders[vb] = nil
		}

	case transport.DCP_SNAPSHOT:
		event = newDcpEvent(pkt, stream)
		event.SnapstartSeq = binary.BigEndian.Uint64(pkt.Extras[0:8])
		event.SnapendSeq = binary.BigEndian.Uint64(pkt.Extras[8:16])
		event.SnapshotType = binary.BigEndian.Uint32(pkt.Extras[16:20])
		stream.Snapstart = event.SnapstartSeq
		stream.Snapend = event.SnapendSeq
		feed.stats.TotalSnapShot.Add(1)
		sendAck = true
		if (stream.Snapend - stream.Snapstart) > 50000 {
			fmsg := "%v ##%x DCP_SNAPSHOT for vb %d snapshot {%v,%v}\n"
			logging.Infof(fmsg, prefix, stream.AppOpaque, vb, stream.Snapstart, stream.Snapend)
		}
		fmsg := "%v ##%x DCP_SNAPSHOT for vb %d\n"
		logging.Debugf(fmsg, prefix, stream.AppOpaque, vb)

		feed.checkSnapOrder(event, vb, pkt.Opcode)

	case transport.DCP_FLUSH:
		event = newDcpEvent(pkt, stream) // special processing ?

	case transport.DCP_CLOSESTREAM:
		event = newDcpEvent(pkt, stream)
		if event.Opaque != stream.CloseOpaque {
			fmsg := "%v ##%x DCP_CLOSESTREAM mismatch in opaque %v != %v\n"
			logging.Fatalf(
				fmsg, prefix, stream.AppOpaque, event.Opaque, stream.CloseOpaque)
		}
		event.Opcode = transport.DCP_STREAMEND // opcode re-write !!
		event.Opaque = stream.AppOpaque        // opaque re-write !!
		feed.deleteFromVbStreams(vb)
		fmsg := "%v ##%x DCP_CLOSESTREAM for vb %d\n"
		logging.Debugf(fmsg, prefix, stream.AppOpaque, vb)
		feed.stats.TotalCloseStream.Add(1)
		if !feed.osoSnapshot {
			feed.seqOrders[vb] = nil
		}

	case transport.DCP_CONTROL, transport.DCP_BUFFERACK:
		if res.Status != transport.SUCCESS {
			fmsg := "%v ##%x opcode %v received status %v\n"
			logging.Errorf(fmsg, prefix, stream.AppOpaque, pkt.Opcode, res.Status)
		}

	case transport.DCP_ADDSTREAM:
		fmsg := "%v ##%x opcode DCP_ADDSTREAM not implemented\n"
		logging.Fatalf(fmsg, prefix, stream.AppOpaque)

	case transport.DCP_SYSTEM_EVENT:
		event = newDcpEvent(pkt, stream)
		feed.handleSystemEvent(pkt, event, stream)
		stream.Seqno = event.Seqno
		sendAck = true
		fmsg := "%v ##%x DCP_SYSTEM_EVENT for vb %d, eventType: %v, manifestUID: %s, scopeId: %s, collectionId: %x\n"
		logging.Debugf(fmsg, prefix, stream.AppOpaque, vb, event.EventType, event.ManifestUID, event.ScopeID, event.CollectionID)

		feed.checkSeqOrder(event, vb, pkt.Opcode)

	case transport.DCP_SEQNO_ADVANCED:
		event = newDcpEvent(pkt, stream)
		feed.stats.SeqnoAdvanced.Add(1)
		sendAck = true

		if len(pkt.Extras) == dcpSeqnoAdvExtrasLen {
			event.Seqno = binary.BigEndian.Uint64(pkt.Extras)
			feed.checkSeqOrder(event, vb, pkt.Opcode)
		} else {
			fmsg := "%v ##%x DCP_SEQNO_ADVANCED for vb %d. Expected extras len: %v, received: %v\n"
			logging.Fatalf(fmsg, prefix, stream.AppOpaque, vb, dcpSeqnoAdvExtrasLen, len(pkt.Extras))
		}

	case transport.DCP_OSO_SNAPSHOT:
		event = newDcpEvent(pkt, stream)
		sendAck = true

		if len(pkt.Extras) == osoSnapshotExtrasLen {
			eventType := binary.BigEndian.Uint32(pkt.Extras)
			if eventType == 0x01 {
				event.EventType = transport.OSO_SNAPSHOT_START
				feed.stats.OsoSnapshotStart.Add(1)
			} else if eventType == 0x02 {
				event.EventType = transport.OSO_SNAPSHOT_END
				feed.stats.OsoSnapshotEnd.Add(1)
			} else {
				fmsg := "%v ##%x DCP_OSO_SNAPSHOT for vb %d. Expected extras value: 0x01 (or) 0x02, received: %v\n"
				logging.Fatalf(fmsg, prefix, stream.AppOpaque, vb, pkt.Extras)
			}
			fmsg := "%v ##%x DCP_OSO_SNAPSHOT for vb %d, eventType: %v\n"
			logging.Debugf(fmsg, prefix, stream.AppOpaque, vb, event.EventType)
		} else {
			fmsg := "%v ##%x DCP_OSO_SNAPSHOT for vb %d. Expected extras len: %v, received: %v\n"
			logging.Fatalf(fmsg, prefix, stream.AppOpaque, vb, osoSnapshotExtrasLen, len(pkt.Extras))
		}
	default:
		fmsg := "%v opcode %v not known for vbucket %d\n"
		logging.Warnf(fmsg, prefix, pkt.Opcode, vb)
	}

	if event != nil {
		feed.outch <- event
	}
	// When using atomic mutation queue, feed.DequeueMutations() takes care
	// of sending the buffer-ack as it read the data from KV while the mutations
	// in rcvch are waiting to get processed by downstream - there by achieving
	// pipelined parallelism
	if !feed.useAtomicMutationQueue {
		if err := feed.sendBufferAck(sendAck, uint32(bytes)); err != nil {
			return "exit"
		}
	}
	return "ok"
}

func (feed *DcpFeed) checkSeqOrder(event *DcpEvent, vb uint16, opcode transport.CommandCode) {
	if !feed.osoSnapshot {
		if s, ok := feed.seqOrders[vb]; ok && s != nil {
			if !s.ProcessSeqno(event.Seqno) {
				logging.Fatalf("%v seq order violation for vb = %v, seq = %v, opcode = %v, "+
					"orderState = %v, event = %v", feed.logPrefix, vb, event.Seqno, opcode,
					s.GetInfo(), event.GetDebugInfo())
			}
		}
	}
}

func (feed *DcpFeed) checkSnapOrder(event *DcpEvent, vb uint16, opcode transport.CommandCode) {
	if !feed.osoSnapshot {
		if s, ok := feed.seqOrders[vb]; ok && s != nil {
			if snapInfo, correctSnapOrder := s.ProcessSnapshot(event.SnapstartSeq, event.SnapendSeq); !correctSnapOrder {
				logging.Fatalf("%v ##%x seq order violation for snapshot message for vb = %v, opcode = %v, "+
					"orderState = %v, event = %v", feed.logPrefix, feed.opaque, vb, opcode,
					snapInfo, event.GetDebugInfo())
			}
		}
	}
}

func (feed *DcpFeed) handleSystemEvent(pkt *transport.MCRequest, dcpEvent *DcpEvent, stream *DcpStream) {
	extras := pkt.Extras
	dcpEvent.Seqno = binary.BigEndian.Uint64(extras[0:8])

	uid := binary.BigEndian.Uint64(pkt.Body[0:8]) //8 byte Manifest UID
	uidstr := strconv.FormatUint(uid, 16)         //convert to base 16 encoded string
	dcpEvent.ManifestUID = []byte(uidstr)

	systemEventType := transport.CollectionEvent(binary.BigEndian.Uint32(extras[8:12]))
	dcpEvent.EventType = systemEventType
	version := uint8(extras[12])

	switch systemEventType {

	case transport.COLLECTION_CREATE:
		sid := binary.BigEndian.Uint32(pkt.Body[8:12]) //4 byte ScopeID
		sidstr := strconv.FormatUint(uint64(sid), 16)  //convert to base 16 encoded string
		dcpEvent.ScopeID = []byte(sidstr)

		dcpEvent.CollectionID = binary.BigEndian.Uint32(pkt.Body[12:16])
		if version == 1 { // Capture max ttl value of the collection if version is "1"
			dcpEvent.MaxTTL = binary.BigEndian.Uint32(pkt.Body[16:20])
		}
		feed.stats.CollectionCreate.Add(1)

	case transport.COLLECTION_DROP, transport.COLLECTION_FLUSH:
		sid := binary.BigEndian.Uint32(pkt.Body[8:12]) //4 byte ScopeID
		sidstr := strconv.FormatUint(uint64(sid), 16)  //convert to base 16 encoded string
		dcpEvent.ScopeID = []byte(sidstr)
		dcpEvent.CollectionID = binary.BigEndian.Uint32(pkt.Body[12:16])
		if systemEventType == transport.COLLECTION_DROP {
			feed.stats.CollectionDrop.Add(1)
		} else {
			feed.stats.CollectionFlush.Add(1)
		}

	case transport.SCOPE_CREATE, transport.SCOPE_DROP:
		sid := binary.BigEndian.Uint32(pkt.Body[8:12]) //4 byte ScopeID
		sidstr := strconv.FormatUint(uint64(sid), 16)  //convert to base 16 encoded string
		dcpEvent.ScopeID = []byte(sidstr)

		if systemEventType == transport.SCOPE_CREATE {
			feed.stats.ScopeCreate.Add(1)
		} else {
			feed.stats.ScopeDrop.Add(1)
		}

	case transport.COLLECTION_CHANGED:
		dcpEvent.CollectionID = binary.BigEndian.Uint32(pkt.Body[8:12])
		feed.stats.CollectionChanged.Add(1)

	default:
		logging.Fatalf("%v ##%x Unknown system event type: %v", feed.logPrefix, stream.AppOpaque, systemEventType)
	}
}

// Transmit request with mutex by setting read/write deadline
func (feed *DcpFeed) TransmitWithDeadline(rq *transport.MCRequest) error {
	feed.mu.Lock()
	defer feed.mu.Unlock()

	feed.conn.SetMcdConnectionDeadline()
	defer feed.conn.ResetMcdConnectionDeadline()

	return feed.conn.Transmit(rq)
}

// Transmit request with mutex by setting write deadline
func (feed *DcpFeed) TransmitWithWriteDeadline(rq *transport.MCRequest) error {
	feed.mu.Lock()
	defer feed.mu.Unlock()

	feed.conn.SetMcdConnectionWriteDeadline()
	defer feed.conn.ResetMcdConnectionWriteDeadline()

	return feed.conn.Transmit(rq)
}

func (feed *DcpFeed) doDcpGetFailoverLog(
	opaque uint16,
	vblist []uint16,
	rcvch chan []interface{}) (map[uint16]*FailoverLog, error) {

	// Enable read deadline at function exit
	// As this method is called only once (i.e. at the start of DCP feed),
	// it is ok to set the value of readDeadline to "1" and not reset it later
	defer func() {
		atomic.StoreInt32(&feed.enableReadDeadline, 1)
		feed.conn.SetMcdMutationReadDeadline()
	}()

	rq := &transport.MCRequest{
		Opcode: transport.DCP_FAILOVERLOG,
		Opaque: opaqueFailover,
	}
	failoverLogs := make(map[uint16]*FailoverLog)

	for _, vBucket := range vblist {
		rq.VBucket = vBucket

		var msg []interface{}
		err1 := func() error {

			if err := feed.TransmitWithDeadline(rq); err != nil {
				fmsg := "%v ##%x doDcpGetFailoverLog.Transmit(): %v"
				logging.Errorf(fmsg, feed.logPrefix, opaque, err)
				return err
			}
			feed.stats.LastMsgSend.Set(time.Now().UnixNano())

			var ok bool
			msg, ok = <-rcvch
			if !ok {
				fmsg := "%v ##%x doDcpGetFailoverLog.rcvch closed"
				logging.Errorf(fmsg, feed.logPrefix, opaque)
				return ErrorConnection
			}

			return nil
		}()

		if err1 != nil {
			return nil, err1
		}

		pkt := msg[0].(*transport.MCRequest)
		req := &transport.MCResponse{
			Opcode: pkt.Opcode,
			Cas:    pkt.Cas,
			Opaque: pkt.Opaque,
			Status: transport.Status(pkt.VBucket),
			Extras: pkt.Extras,
			Key:    pkt.Key,
			Body:   pkt.Body,
		}
		if req.Opcode != transport.DCP_FAILOVERLOG {
			fmsg := "%v ##%x for failover log request unexpected #opcode %v"
			logging.Errorf(fmsg, feed.logPrefix, opaque, req.Opcode)
			return nil, ErrorInvalidFeed

		} else if req.Status != transport.SUCCESS {
			fmsg := "%v ##%x for failover log request unexpected #status %v"
			logging.Errorf(fmsg, feed.logPrefix, opaque, req.Status)
			return nil, ErrorInvalidFeed
		}
		flog, err := parseFailoverLog(req.Body)
		if err != nil {
			fmsg := "%v ##%x parse failover logs for vb %d"
			logging.Errorf(fmsg, feed.logPrefix, opaque, vBucket)
			return nil, ErrorInvalidFeed
		}
		failoverLogs[vBucket] = flog
	}
	return failoverLogs, nil
}

func (feed *DcpFeed) doDcpGetSeqnos(
	rcvch chan []interface{}) (map[uint16]uint64, error) {

	// Enable read deadline at function exit
	// As this method is called only once (i.e. at the start of DCP feed),
	// it is ok to set the value of readDeadline to "1" and not reset it later
	defer func() {
		atomic.StoreInt32(&feed.enableReadDeadline, 1)
		feed.conn.SetMcdMutationReadDeadline()
	}()

	rq := &transport.MCRequest{
		Opcode: transport.DCP_GET_SEQNO,
		Opaque: opaqueGetseqno,
	}

	rq.Extras = make([]byte, 4)
	binary.BigEndian.PutUint32(rq.Extras, 1) // Only active vbuckets

	if err := feed.TransmitWithDeadline(rq); err != nil {
		fmsg := "%v ##%x doDcpGetSeqnos.Transmit(): %v"
		logging.Errorf(fmsg, feed.logPrefix, rq.Opaque, err)
		return nil, err
	}

	feed.stats.LastMsgSend.Set(time.Now().UnixNano())
	msg, ok := <-rcvch
	if !ok {
		fmsg := "%v ##%x doDcpGetSeqnos.rcvch closed"
		logging.Errorf(fmsg, feed.logPrefix, rq.Opaque)
		return nil, ErrorConnection
	}
	pkt := msg[0].(*transport.MCRequest)
	req := &transport.MCResponse{
		Opcode: pkt.Opcode,
		Cas:    pkt.Cas,
		Opaque: pkt.Opaque,
		Status: transport.Status(pkt.VBucket),
		Extras: pkt.Extras,
		Key:    pkt.Key,
		Body:   pkt.Body,
	}
	if req.Opcode != transport.DCP_GET_SEQNO {
		fmsg := "%v ##%x for get-seqno request unexpected #opcode %v"
		logging.Errorf(fmsg, feed.logPrefix, req.Opaque, req.Opcode)
		return nil, ErrorInvalidFeed

	} else if req.Status != transport.SUCCESS {
		fmsg := "%v ##%x for get-seqno request unexpected #status %v"
		logging.Errorf(fmsg, feed.logPrefix, req.Opaque, req.Status)
		return nil, ErrorInvalidFeed
	}
	seqnos, err := parseGetSeqnos(req.Body)
	if err != nil {
		fmsg := "%v ##%x parsing get-seqnos: %v"
		logging.Errorf(fmsg, feed.logPrefix, req.Opaque, err)
		return nil, ErrorInvalidFeed
	}
	return seqnos, nil
}

func (feed *DcpFeed) addToSyncRespCh(reqCh chan []interface{}) {
	feed.syncRespMu.Lock()
	defer feed.syncRespMu.Unlock()

	if feed.syncRespCh != nil {
		panic("DcpFeed::addToSyncRespCh - Expected nil channel before adding new channel. Found non-nil channel")
	}
	feed.syncRespCh = reqCh
}

func (feed *DcpFeed) getSyncRespCh() chan []interface{} {
	feed.syncRespMu.Lock()
	defer feed.syncRespMu.Unlock()

	return feed.syncRespCh
}

func (feed *DcpFeed) resetSyncRespCh() {
	if !(feed.useAtomicMutationQueue && feed.controlDataPathSeparation) {
		return
	}

	feed.syncRespMu.Lock()
	defer feed.syncRespMu.Unlock()

	if feed.syncRespCh == nil {
		panic("DcpFeed::resetSyncRespCh - Expected non-nil channel. Found nil channel")
	}
	feed.syncRespCh = nil
}

// When atomic mutation queue is used, the response to sync requests come via
// DequeueMutations. DequeueMutations() will write to the channel returned
// by this method
func (feed *DcpFeed) initSyncRespCh(rcvch chan []interface{}) chan []interface{} {
	if feed.useAtomicMutationQueue && feed.controlDataPathSeparation {
		respRcvCh := make(chan []interface{}, 1)
		feed.addToSyncRespCh(respRcvCh)
		return respRcvCh
	}
	return rcvch
}

func (feed *DcpFeed) doDcpOpen(
	name string, sequence, flags, bufsize uint32,
	opaque uint16,
	rcvch chan []interface{}) error {

	prefix := feed.logPrefix

	// Enable read deadline at function exit
	// As this method is called only once (i.e. at the start of DCP feed),
	// it is ok to set the value of readDeadline to "1" and not reset it later
	defer func() {
		atomic.StoreInt32(&feed.enableReadDeadline, 1)
		feed.conn.SetMcdMutationReadDeadline()
	}()

	if feed.collectionsAware {
		if err := feed.enableCollections(rcvch); err != nil {
			return err
		}
	}

	respRcvCh := feed.initSyncRespCh(rcvch)
	rq := &transport.MCRequest{
		Opcode: transport.DCP_OPEN,
		Key:    []byte(feed.truncName),
		Opaque: opaqueOpen,
	}
	rq.Extras = make([]byte, 8)
	flags = flags | openConnFlag | includeXATTR
	binary.BigEndian.PutUint32(rq.Extras[:4], sequence)
	binary.BigEndian.PutUint32(rq.Extras[4:], flags) // we are consumer

	err := feed.TransmitWithDeadline(rq)
	if err != nil {
		fmsg := "%v ##%x doDcpOpen.DCP_OPEN.Transmit(): %v"
		logging.Errorf(fmsg, prefix, opaque, err)
		return err
	}

	feed.stats.LastMsgSend.Set(time.Now().UnixNano())
	msg, ok := <-respRcvCh
	if !ok {
		logging.Errorf("%v ##%x doDcpOpen.rcvch closed", prefix, opaque)
		return ErrorConnection
	}
	pkt := msg[0].(*transport.MCRequest)
	req := &transport.MCResponse{
		Opcode: pkt.Opcode,
		Cas:    pkt.Cas,
		Opaque: pkt.Opaque,
		Status: transport.Status(pkt.VBucket),
		Extras: pkt.Extras,
		Key:    pkt.Key,
		Body:   pkt.Body,
	}
	if req.Opcode != transport.DCP_OPEN {
		logging.Errorf("%v ##%x unexpected #%v", prefix, opaque, req.Opcode)
		return ErrorConnection
	} else if rq.Opaque != req.Opaque {
		fmsg := "%v ##%x opaque mismatch, %v != %v"
		logging.Errorf(fmsg, prefix, opaque, req.Opaque, req.Opaque)
		return ErrorConnection
	} else if req.Status != transport.SUCCESS {
		fmsg := "%v ##%x doDcpOpen response status %v"
		logging.Errorf(fmsg, prefix, opaque, req.Status)
		return ErrorConnection
	}
	feed.resetSyncRespCh()

	// send a DCP control message to set the window size for
	// this connection
	if bufsize > 0 {
		logging.Infof("%v##%x using bufSize: %v", prefix, opaque, bufsize)

		respRcvCh := feed.initSyncRespCh(rcvch)
		rq := &transport.MCRequest{
			Opcode: transport.DCP_CONTROL,
			Key:    []byte("connection_buffer_size"),
			Body:   []byte(strconv.Itoa(int(bufsize))),
		}

		if err := feed.TransmitWithDeadline(rq); err != nil {
			fmsg := "%v ##%x doDcpOpen.DCP_CONTROL.Transmit(connection_buffer_size): %v"
			logging.Errorf(fmsg, prefix, opaque, err)
			return err
		}

		feed.stats.LastMsgSend.Set(time.Now().UnixNano())
		msg, ok := <-respRcvCh
		if !ok {
			fmsg := "%v ##%x doDcpOpen.DCP_CONTROL.rcvch (connection_buffer_size) closed"
			logging.Errorf(fmsg, prefix, opaque)
			return ErrorConnection
		}
		pkt := msg[0].(*transport.MCRequest)
		req := &transport.MCResponse{
			Opcode: pkt.Opcode,
			Cas:    pkt.Cas,
			Opaque: pkt.Opaque,
			Status: transport.Status(pkt.VBucket),
			Extras: pkt.Extras,
			Key:    pkt.Key,
			Body:   pkt.Body,
		}
		if req.Opcode != transport.DCP_CONTROL {
			fmsg := "%v ##%x DCP_CONTROL (connection_buffer_size) != #%v"
			logging.Errorf(fmsg, prefix, opaque, req.Opcode)
			return ErrorConnection
		} else if req.Status != transport.SUCCESS {
			fmsg := "%v ##%x doDcpOpen (connection_buffer_size) response status %v"
			logging.Errorf(fmsg, prefix, opaque, req.Status)
			return ErrorConnection
		}

		feed.maxAckBytes = uint32(bufferAckThreshold * float32(bufsize))
		feed.resetSyncRespCh()
	}

	// send a DCP control message to enable_noop
	if true /*enable_noop*/ {

		respRcvCh := feed.initSyncRespCh(rcvch)
		rq := &transport.MCRequest{
			Opcode: transport.DCP_CONTROL,
			Key:    []byte("enable_noop"),
			Body:   []byte("true"),
		}

		if err := feed.TransmitWithDeadline(rq); err != nil {
			fmsg := "%v ##%x doDcpOpen.DCP_CONTROL.Transmit(enable_noop): %v"
			logging.Errorf(fmsg, prefix, opaque, err)
			return err
		}
		feed.stats.LastMsgSend.Set(time.Now().UnixNano())
		logging.Infof("%v ##%x sending enable_noop", prefix, opaque)
		msg, ok := <-respRcvCh
		if !ok {
			fmsg := "%v ##%x doDcpOpen.DCP_CONTROL.rcvch (enable_noop) closed"
			logging.Errorf(fmsg, prefix, opaque)
			return ErrorConnection
		}
		pkt := msg[0].(*transport.MCRequest)
		opcode, status := pkt.Opcode, transport.Status(pkt.VBucket)
		if opcode != transport.DCP_CONTROL {
			fmsg := "%v ##%x DCP_CONTROL (enable_noop) != #%v"
			logging.Errorf(fmsg, prefix, opaque, opcode)
			return ErrorConnection
		} else if status != transport.SUCCESS {
			fmsg := "%v ##%x doDcpOpen (enable_noop) response status %v"
			logging.Errorf(fmsg, prefix, opaque, status)
			return ErrorConnection
		}
		logging.Infof("%v ##%x received enable_noop response", prefix, opaque)
		feed.resetSyncRespCh()
	}

	// send a DCP control message to set_noop_interval
	if true /*set_noop_interval*/ {
		respRcvCh := feed.initSyncRespCh(rcvch)
		rq := &transport.MCRequest{
			Opcode: transport.DCP_CONTROL,
			Key:    []byte("set_noop_interval"),
			Body:   []byte("20"),
		}

		if err := feed.TransmitWithDeadline(rq); err != nil {
			fmsg := "%v ##%x doDcpOpen.Transmit(set_noop_interval): %v"
			logging.Errorf(fmsg, prefix, opaque, err)
			return err
		}

		feed.stats.LastMsgSend.Set(time.Now().UnixNano())
		logging.Infof("%v ##%x sending set_noop_interval", prefix, opaque)
		msg, ok := <-respRcvCh
		if !ok {
			fmsg := "%v ##%x doDcpOpen.rcvch (set_noop_interval) closed"
			logging.Errorf(fmsg, prefix, opaque)
			return ErrorConnection
		}
		pkt := msg[0].(*transport.MCRequest)
		opcode, status := pkt.Opcode, transport.Status(pkt.VBucket)
		if opcode != transport.DCP_CONTROL {
			fmsg := "%v ##%x DCP_CONTROL (set_noop_interval) != #%v"
			logging.Errorf(fmsg, prefix, opaque, opcode)
			return ErrorConnection
		} else if status != transport.SUCCESS {
			fmsg := "%v ##%x doDcpOpen (set_noop_interval) response status %v"
			logging.Errorf(fmsg, prefix, opaque, status)
			return ErrorConnection
		}
		fmsg := "%v ##%x received response for set_noop_interval"
		logging.Infof(fmsg, prefix, opaque)
		feed.resetSyncRespCh()
	}

	if feed.osoSnapshot {
		if err := feed.enableOSOSnapshot(rcvch); err != nil {
			return err
		}
	}

	return nil
}

func (feed *DcpFeed) doDcpRequestStream(
	vbno, opaqueMSB uint16, flags uint32,
	vuuid, startSequence, endSequence, snapStart, snapEnd uint64,
	manifestUID, scopeId string, collectionIds []string) error {

	rq := &transport.MCRequest{
		Opcode:  transport.DCP_STREAMREQ,
		VBucket: vbno,
		Opaque:  composeOpaque(vbno, opaqueMSB),
	}
	rq.Extras = make([]byte, 48) // #Extras
	binary.BigEndian.PutUint32(rq.Extras[:4], flags)
	binary.BigEndian.PutUint32(rq.Extras[4:8], uint32(0))
	binary.BigEndian.PutUint64(rq.Extras[8:16], startSequence)
	binary.BigEndian.PutUint64(rq.Extras[16:24], endSequence)
	binary.BigEndian.PutUint64(rq.Extras[24:32], vuuid)
	binary.BigEndian.PutUint64(rq.Extras[32:40], snapStart)
	binary.BigEndian.PutUint64(rq.Extras[40:48], snapEnd)

	prefix := feed.logPrefix

	requestValue := &StreamRequestValue{}

	if feed.collectionsAware {
		if scopeId != "" || len(collectionIds) > 0 {
			requestValue.ManifestUID = manifestUID
			requestValue.ScopeID = scopeId
			requestValue.CollectionIDs = collectionIds
			feed.isIncrBuild = feed.isIncrBuild && false
		} else {
			// ScopeId being empty and no collectionId specified will
			// open the stream for entire bucket. For such a
			// scenario, only manifestUID is required to be specified
			// in request body.
			feed.isIncrBuild = feed.isIncrBuild && true
			requestValue.ManifestUID = manifestUID
		}
		body, _ := json.Marshal(requestValue)
		rq.Body = body
	}

	stream := &DcpStream{
		AppOpaque:        opaqueMSB,
		Vbucket:          vbno,
		Vbuuid:           vuuid,
		StartSeq:         startSequence,
		EndSeq:           endSequence,
		CollectionsAware: feed.collectionsAware,
		RequestValue:     requestValue,
	}
	feed.addToVbStreams(vbno, stream)

	// Here, timeout can occur due to slow memcached. The error handling
	// in the projector feed takes care of cleanning up of the connections.
	// After closing connections, pressure on memcached may get eased, and
	// retry (from indexer side) may succeed.
	if err := feed.TransmitWithWriteDeadline(rq); err != nil {
		fmsg := "%v ##%x doDcpRequestStream.Transmit(): for vb:%v %v"
		logging.Errorf(fmsg, prefix, opaqueMSB, vbno, err)
		feed.deleteFromVbStreams(vbno)
		return err
	}

	feed.stats.LastMsgSend.Set(time.Now().UnixNano())
	return nil
}

func (feed *DcpFeed) doDcpCloseStream(vbno, opaqueMSB uint16) error {
	prefix := feed.logPrefix
	stream, ok := feed.getFromVbStreams(vbno)
	if !ok || stream == nil {
		fmsg := "%v ##%x stream for vb %d is not active"
		logging.Warnf(fmsg, prefix, opaqueMSB, vbno)
		return nil // TODO: should we return error here ?
	}
	stream.CloseOpaque = opaqueMSB
	rq := &transport.MCRequest{
		Opcode:  transport.DCP_CLOSESTREAM,
		VBucket: vbno,
		Opaque:  composeOpaque(vbno, opaqueMSB),
	}

	// In case of DCP_CLOSESTREAM, feed.conn.Transmit won't have any
	// network timeout. This is called in restartVBuckets workflow
	// on indexer side. In this case, indexer does not retry on error.
	// For now, let's just report slow/hung operation on indexer.
	if err := feed.conn.Transmit(rq); err != nil {
		fmsg := "%v ##%x (##%x) doDcpCloseStream.Transmit(): %v"
		logging.Errorf(fmsg, prefix, opaqueMSB, stream.AppOpaque, err)
		return err
	}

	feed.stats.LastMsgSend.Set(time.Now().UnixNano())
	return nil
}

func (feed *DcpFeed) enableCollections(rcvch chan []interface{}) error {
	prefix := feed.logPrefix
	opaque := feed.opaque

	respRcvCh := feed.initSyncRespCh(rcvch)

	rq := &transport.MCRequest{
		Opcode: transport.HELO,
		Key:    []byte(feed.truncName),
		Body:   []byte{0x00, transport.FEATURE_COLLECTIONS},
	}

	if err := feed.TransmitWithDeadline(rq); err != nil {
		fmsg := "%v ##%x doDcpOpen.Transmit DCP_HELO (feature_collections): %v"
		logging.Errorf(fmsg, prefix, opaque, err)
		return err
	}

	feed.stats.LastMsgSend.Set(time.Now().UnixNano())
	logging.Infof("%v ##%x sending DCP_HELO (feature_collections)", prefix, opaque)
	msg, ok := <-respRcvCh
	if !ok {
		fmsg := "%v ##%x doDcpOpen.rcvch (feature_collections) closed"
		logging.Errorf(fmsg, prefix, opaque)
		return ErrorConnection
	}
	feed.stats.LastMsgRecv.Set(time.Now().UnixNano())

	pkt := msg[0].(*transport.MCRequest)
	opcode, body := pkt.Opcode, pkt.Body
	if opcode != transport.HELO {
		fmsg := "%v ##%x DCP_HELO (feature_collections) opcode = %v. Expecting opcode = 0x1f"
		logging.Errorf(fmsg, prefix, opaque, opcode)
		return ErrorEnableCollections
	} else if (len(body) != 2) || (body[0] != 0x00 && body[1] != transport.FEATURE_COLLECTIONS) {
		fmsg := "%v ##%x DCP_HELO (feature_collections) body = %v. Expecting body = 0x0012"
		logging.Errorf(fmsg, prefix, opaque, body)
		return ErrorCollectionsNotEnabled
	}
	feed.resetSyncRespCh()

	fmsg := "%v ##%x received response for DCP_HELO (feature_collections)"
	logging.Infof(fmsg, prefix, opaque)
	return nil
}

func (feed *DcpFeed) enableOSOSnapshot(rcvch chan []interface{}) error {
	prefix := feed.logPrefix
	opaque := feed.opaque

	respRcvCh := feed.initSyncRespCh(rcvch)
	rq := &transport.MCRequest{
		Opcode: transport.DCP_CONTROL,
		Key:    []byte("enable_out_of_order_snapshots"),
		Body:   []byte("true_with_seqno_advanced"),
	}

	if err := feed.TransmitWithDeadline(rq); err != nil {
		fmsg := "%v ##%x doDcpOpen.Transmit DCP_CONTROL (enable_out_of_order_snapshots): %v"
		logging.Errorf(fmsg, prefix, opaque, err)
		return err
	}

	feed.stats.LastMsgSend.Set(time.Now().UnixNano())
	logging.Infof("%v ##%x sending DCP_CONTROL (enable_out_of_order_snapshots)", prefix, opaque)
	msg, ok := <-respRcvCh
	if !ok {
		fmsg := "%v ##%x doDcpOpen.rcvch (enable_out_of_order_snapshots) closed"
		logging.Errorf(fmsg, prefix, opaque)
		return ErrorConnection
	}
	feed.stats.LastMsgRecv.Set(time.Now().UnixNano())

	pkt := msg[0].(*transport.MCRequest)
	opcode, status := pkt.Opcode, transport.Status(pkt.VBucket)
	if opcode != transport.DCP_CONTROL {
		fmsg := "%v ##%x DCP_CONTROL (enable_out_of_order_snapshots) != #%v"
		logging.Errorf(fmsg, prefix, opaque, opcode)
		return ErrorConnection
	} else if status != transport.SUCCESS {
		fmsg := "%v ##%x doDcpOpen (enable_out_of_order_snapshots) response status %v"
		logging.Errorf(fmsg, prefix, opaque, status)
		return ErrorConnection
	}

	feed.resetSyncRespCh()
	fmsg := "%v ##%x received response for DCP_CONTROL (enable_out_of_order_snapshots)"
	logging.Infof(fmsg, prefix, opaque)
	return nil
}

// generate stream end responses for all active vb streams
func (feed *DcpFeed) sendStreamEnd(outch chan<- *DcpEvent) {
	feed.vbstreamsMu.Lock()
	defer feed.vbstreamsMu.Unlock()

	if feed.vbstreams != nil {
		for vb, stream := range feed.vbstreams {
			feed.supvch <- []interface{}{transport.DCP_STREAMEND, feed, vb}
			dcpEvent := &DcpEvent{
				VBucket: vb,
				VBuuid:  stream.Vbuuid,
				Opcode:  transport.DCP_STREAMEND,
				Opaque:  stream.AppOpaque,
				Ctime:   time.Now().UnixNano(),
			}
			outch <- dcpEvent
		}
		feed.vbstreams = nil
	}
}

func (feed *DcpFeed) handleStreamRequest(
	res *transport.MCResponse, vb uint16, stream *DcpStream, event *DcpEvent) {

	prefix := feed.logPrefix
	switch {
	case res.Status == transport.ROLLBACK && len(res.Body) != 8:
		event.Status, event.Seqno = res.Status, 0
		fmsg := "%v ##%x STREAMREQ(%v) invalid rollback: %v\n"
		arg1 := logging.TagUD(res.Body)
		logging.Errorf(fmsg, prefix, stream.AppOpaque, vb, arg1)
		feed.deleteFromVbStreams(vb)

	case res.Status == transport.ROLLBACK:
		rollback := binary.BigEndian.Uint64(res.Body)
		event.Status, event.Seqno = res.Status, rollback
		fmsg := "%v ##%x STREAMREQ(%v) with rollback %d\n"
		logging.Warnf(fmsg, prefix, stream.AppOpaque, vb, rollback)
		feed.deleteFromVbStreams(vb)

	case res.Status == transport.SUCCESS:
		event.Status, event.Seqno = res.Status, stream.StartSeq
		flog, err := parseFailoverLog(res.Body[:])
		if err != nil {
			fmsg := "%v ##%x STREAMREQ(%v) parseFailoverLog: %v\n"
			logging.Errorf(fmsg, prefix, stream.AppOpaque, vb, err)
		}
		event.FailoverLog = flog
		stream.connected = true
		fmsg := "%v ##%x STREAMREQ(%d) successful\n"
		logging.Debugf(fmsg, prefix, stream.AppOpaque, vb)
	case (res.Status == transport.UNKNOWN_COLLECTION) ||
		(res.Status == transport.MANIFEST_AHEAD) ||
		(res.Status == transport.UNKNOWN_SCOPE) ||
		(res.Status == transport.EINVAL): // EINVAL can be returned when the StreamRequestValue is invalid
		event.Status = res.Status
		event.VBucket = vb
		fmsg := "%v ##%x STREAMREQ(%v) with status: %v, stream request value: %+v\n"
		logging.Errorf(fmsg, prefix, stream.AppOpaque, vb, res.Status, stream.RequestValue)
		feed.deleteFromVbStreams(vb)
	default:
		event.Status = res.Status
		event.VBucket = vb
		fmsg := "%v ##%x STREAMREQ(%v) unexpected status: %v\n"
		logging.Errorf(fmsg, prefix, stream.AppOpaque, vb, res.Status)
		feed.deleteFromVbStreams(vb)
	}
	return
}

// Send buffer ack
func (feed *DcpFeed) sendBufferAck(sendAck bool, bytes uint32) error {
	prefix := feed.logPrefix
	if sendAck {
		var err1 error
		totalBytes := feed.toAckBytes + bytes

		// Disable periodic buffer-ack when atomic mutation queue is enabled
		if totalBytes >= feed.maxAckBytes ||
			(!feed.useAtomicMutationQueue && time.Since(feed.lastAckTime).Seconds() > bufferAckPeriod) {
			bufferAck := &transport.MCRequest{
				Opcode: transport.DCP_BUFFERACK,
			}
			bufferAck.Extras = make([]byte, 4)
			binary.BigEndian.PutUint32(bufferAck.Extras[:4], uint32(totalBytes))
			feed.stats.TotalBufferAckSent.Add(1)

			func() {
				// Timeout here will unblock genServer() thread after some time.
				if err := feed.TransmitWithWriteDeadline(bufferAck); err != nil {
					logging.Errorf("%v buffer-ack Transmit(): %v, lastAckTime: %v", prefix, err, feed.lastAckTime.UnixNano())
					err1 = err
				} else {
					// Reset the counters only on a successful BufferAck
					feed.toAckBytes = 0
					feed.stats.ToAckBytes.Set(0)
					feed.lastAckTime = time.Now()
					feed.stats.LastMsgSend.Set(feed.lastAckTime.UnixNano())
					feed.stats.LastAckTime.Set(feed.lastAckTime.UnixNano())
					logging.Tracef("%v buffer-ack %v, lastAckTime: %v\n", prefix, totalBytes, feed.lastAckTime.UnixNano())
				}
			}()

		} else {
			feed.toAckBytes += bytes
			feed.stats.ToAckBytes.Set(uint64(feed.toAckBytes))
		}
		return err1
	}
	return nil
}

func composeOpaque(vbno, opaqueMSB uint16) uint32 {
	return (uint32(opaqueMSB) << 16) | uint32(vbno)
}

func appOpaque(opq32 uint32) uint16 {
	return uint16((opq32 & 0xFFFF0000) >> 16)
}

func vbOpaque(opq32 uint32) uint16 {
	return uint16(opq32 & 0xFFFF)
}

type StreamRequestValue struct {
	ManifestUID   string   `json:"uid,omitempty"`
	CollectionIDs []string `json:"collections,omitempty"`
	ScopeID       string   `json:"scope,omitempty"`
	StreamID      string   `json:"sid,omitempty"`
}

// DcpStream is per stream data structure over an DCP Connection.
type DcpStream struct {
	AppOpaque        uint16
	CloseOpaque      uint16
	Vbucket          uint16 // Vbucket id
	Vbuuid           uint64 // vbucket uuid
	Seqno            uint64
	StartSeq         uint64 // start sequence number
	EndSeq           uint64 // end sequence number
	Snapstart        uint64
	Snapend          uint64
	LastSeen         int64 // UnixNano value of last seen
	connected        bool
	CollectionsAware bool
	RequestValue     *StreamRequestValue
}

// DcpEvent memcached events for DCP streams.
type DcpEvent struct {
	Opcode     transport.CommandCode // Type of event
	Status     transport.Status      // Response status
	Datatype   uint8                 // Datatype per binary protocol
	VBucket    uint16                // VBucket this event applies to
	Opaque     uint16                // 16 MSB of opaque
	VBuuid     uint64                // This field is set by downstream
	Key, Value []byte                // Item key/value
	OldValue   []byte                // TODO: TBD: old document value
	Cas        uint64                // CAS value of the item
	// meta fields
	Seqno uint64 // seqno. of the mutation, doubles as rollback-seqno
	// https://issues.couchbase.com/browse/MB-15333,
	RevSeqno uint64
	Flags    uint32 // Item flags
	Expiry   uint32 // Item expiration time
	LockTime uint32
	Nru      byte
	// snapshots
	SnapstartSeq uint64 // start sequence number of this snapshot
	SnapendSeq   uint64 // End sequence number of the snapshot
	SnapshotType uint32 // 0: disk 1: memory
	// Collection
	ManifestUID  []byte // For DCP_SYSTEM_EVENT
	ScopeID      []byte // For DCP_SYSTEM_EVENT
	CollectionID uint32 // For DCP_SYSTEM_EVENT, Mutations

	EventType transport.CollectionEvent // For DCP_SYSTEM_EVENT, DCP_OSO_SNAPSHOT types
	// For DCP_SYSTEM_EVENT, MaxTTL is maximum ttl value of the collection
	// As of v7.0, this value is not propagated down-stream (i.e. to indexer)
	MaxTTL uint32
	// failoverlog
	FailoverLog *FailoverLog // Failover log containing vvuid and sequnce number
	Error       error        // Error value in case of a failure
	// stats
	Ctime int64
	// extended attributes
	RawXATTR    map[string][]byte
	ParsedXATTR map[string]interface{}
}

func newDcpEvent(rq *transport.MCRequest, stream *DcpStream) (event *DcpEvent) {
	defer func() {
		if r := recover(); r != nil {
			// Error parsing XATTR, Request body might be malformed
			arg1 := logging.TagStrUD(rq.Key)
			logging.Errorf("Panic: Error parsing RawXATTR for %s: %v", arg1, r)
			event.Value = make([]byte, 0)
			event.Datatype &= ^(dcpXATTR | dcpJSON)
		}
	}()

	event = &DcpEvent{
		Cas:      rq.Cas,
		Datatype: rq.Datatype,
		Opcode:   rq.Opcode,
		VBucket:  stream.Vbucket,
		VBuuid:   stream.Vbuuid,
		Ctime:    time.Now().UnixNano(),
	}

	mutKey := rq.Key
	// For a collection aware stream, dcp mutation, deletion, expiration
	// messages will carry collectionId as a part of docid (i.e. mutation key).
	// Extract collection id from mutation key using LEB128 decode method
	if stream.CollectionsAware {
		switch event.Opcode {
		case transport.DCP_MUTATION, transport.DCP_DELETION, transport.DCP_EXPIRATION:
			mutKey, event.CollectionID = collections.LEB128Dec(rq.Key)
		}
	}
	event.Key = make([]byte, len(mutKey))
	copy(event.Key, mutKey)

	// 16 LSBits are used by client library to encode vbucket number.
	// 16 MSBits are left for application to multiplex on opaque value.
	event.Opaque = appOpaque(rq.Opaque)

	if len(rq.Extras) >= tapMutationExtraLen {
		event.Seqno = binary.BigEndian.Uint64(rq.Extras[:8])
		switch event.Opcode {
		case transport.DCP_MUTATION:
			event.RevSeqno = binary.BigEndian.Uint64(rq.Extras[8:])
			event.Flags = binary.BigEndian.Uint32(rq.Extras[16:])
			event.Expiry = binary.BigEndian.Uint32(rq.Extras[20:])
			event.LockTime = binary.BigEndian.Uint32(rq.Extras[24:])
			event.Nru = rq.Extras[30]

		case transport.DCP_DELETION:
			event.RevSeqno = binary.BigEndian.Uint64(rq.Extras[8:])

		case transport.DCP_EXPIRATION:
			event.RevSeqno = binary.BigEndian.Uint64(rq.Extras[8:])
		}

	} else if len(rq.Extras) >= tapMutationExtraLen &&
		event.Opcode == transport.DCP_SNAPSHOT {

		event.SnapstartSeq = binary.BigEndian.Uint64(rq.Extras[:8])
		event.SnapendSeq = binary.BigEndian.Uint64(rq.Extras[8:16])
		event.SnapshotType = binary.BigEndian.Uint32(rq.Extras[16:20])

	}

	if (event.Opcode == transport.DCP_MUTATION ||
		event.Opcode == transport.DCP_DELETION) && event.HasXATTR() {
		xattrLen := int(binary.BigEndian.Uint32(rq.Body))
		xattrData := rq.Body[4 : 4+xattrLen]
		event.RawXATTR = make(map[string][]byte)
		for len(xattrData) > 0 {
			pairLen := binary.BigEndian.Uint32(xattrData[0:])
			xattrData = xattrData[4:]
			binaryPair := xattrData[:pairLen-1]
			xattrData = xattrData[pairLen:]
			kvPair := bytes.Split(binaryPair, []byte{0x00})
			event.RawXATTR[string(kvPair[0])] = kvPair[1]
		}
		event.Value = make([]byte, len(rq.Body)-(4+xattrLen))
		copy(event.Value, rq.Body[4+xattrLen:])
	} else {
		event.Value = make([]byte, len(rq.Body))
		copy(event.Value, rq.Body)
	}

	return event
}

func (event *DcpEvent) IsJSON() bool {
	return (event.Datatype & dcpJSON) != 0
}

func (event *DcpEvent) TreatAsJSON() {
	event.Datatype |= dcpJSON
}

func (event *DcpEvent) HasXATTR() bool {
	return (event.Datatype & dcpXATTR) != 0
}

func (event *DcpEvent) String() string {
	name := transport.CommandNames[event.Opcode]
	if name == "" {
		name = fmt.Sprintf("#%d", event.Opcode)
	}
	return name
}

func (event *DcpEvent) GetDebugInfo() string {
	fmtStr := "Opcode %v, Status %v, Datatype %v, VBucket %v, Opaque %v, VBuuid %v, "
	fmtStr += "Key %v, Cas %v, Seqno %v, RevSeqno %v, Flags %v, "
	fmtStr += "Expiry %v, LockTime %v, Nru %v, SnapstartSeq %v, SnapendSeq %v, "
	fmtStr += "SnapshotType %v, FailoverLog %v, Error %v, Ctime %v"

	return fmt.Sprintf(fmtStr, event.Opcode, event.Status, event.Datatype, event.VBucket,
		event.Opaque, event.VBuuid, logging.TagStrUD(event.Key), event.Cas, event.Seqno,
		event.RevSeqno, event.Flags, event.Expiry, event.LockTime, event.Nru,
		event.SnapstartSeq, event.SnapendSeq, event.SnapshotType, event.FailoverLog,
		event.Error, event.Ctime)
}

// DcpStats on mutations/snapshots/buff-acks.
// DcpStats on mutations/snapshots/buff-acks.
type DcpStats struct {
	Closed             stats.BoolVal
	TotalBufferAckSent stats.Uint64Val
	TotalBytes         stats.Uint64Val
	TotalMutation      stats.Uint64Val
	TotalSnapShot      stats.Uint64Val
	TotalStreamReq     stats.Uint64Val
	TotalCloseStream   stats.Uint64Val
	TotalStreamEnd     stats.Uint64Val
	TotalSpurious      stats.Uint64Val
	ToAckBytes         stats.Uint64Val

	// Last memcached communication times
	LastAckTime  stats.Int64Val
	LastNoopSend stats.Int64Val
	LastNoopRecv stats.Int64Val
	LastMsgSend  stats.Int64Val
	LastMsgRecv  stats.Int64Val

	// Collections related
	CollectionCreate  stats.Uint64Val
	CollectionDrop    stats.Uint64Val
	CollectionFlush   stats.Uint64Val
	ScopeCreate       stats.Uint64Val
	ScopeDrop         stats.Uint64Val
	CollectionChanged stats.Uint64Val
	SeqnoAdvanced     stats.Uint64Val
	OsoSnapshotStart  stats.Uint64Val
	OsoSnapshotEnd    stats.Uint64Val

	BlockedDurHist stats.Histogram

	rcvch      chan []interface{}
	Dcplatency stats.Average
	// This stat help to determine the drain rate of dcp feed
	IncomingMsg stats.Uint64Val

	mutationQueue *AtomicMutationQueue

	// immutable
	StreamNo string // stream no. 0/1/2...
}

func (dcpStats *DcpStats) Init() {
	dcpStats.Closed.Init()
	dcpStats.TotalBufferAckSent.Init()
	dcpStats.TotalBytes.Init()
	dcpStats.TotalMutation.Init()
	dcpStats.TotalSnapShot.Init()
	dcpStats.TotalStreamReq.Init()
	dcpStats.TotalCloseStream.Init()
	dcpStats.TotalStreamEnd.Init()
	dcpStats.TotalSpurious.Init()
	dcpStats.ToAckBytes.Init()
	dcpStats.LastAckTime.Init()
	dcpStats.LastNoopSend.Init()
	dcpStats.LastNoopRecv.Init()
	dcpStats.LastMsgSend.Init()
	dcpStats.LastMsgRecv.Init()
	dcpStats.Dcplatency.Init()
	dcpStats.IncomingMsg.Init()

	// Collections related stats
	dcpStats.CollectionCreate.Init()
	dcpStats.CollectionDrop.Init()
	dcpStats.CollectionFlush.Init()
	dcpStats.ScopeCreate.Init()
	dcpStats.ScopeDrop.Init()
	dcpStats.CollectionChanged.Init()
	dcpStats.SeqnoAdvanced.Init()
	dcpStats.OsoSnapshotStart.Init()
	dcpStats.OsoSnapshotEnd.Init()

	dcpStats.BlockedDurHist.InitLatency(projBlockedDur, func(v int64) string { return fmt.Sprintf("%vms", v/int64(time.Millisecond)) })
}

func (stats *DcpStats) IsClosed() bool {
	return stats.Closed.Value()
}

func (stats *DcpStats) String() (string, string) {
	now := time.Now()
	getTimeDur := func(t int64) time.Duration {
		if t == 0 {
			return time.Duration(0 * time.Second)
		}
		return now.Sub(time.Unix(0, t))
	}

	var stitems [30]string
	stitems[0] = `"bytes":` + strconv.FormatUint(stats.TotalBytes.Value(), 10)
	stitems[1] = `"bufferacks":` + strconv.FormatUint(stats.TotalBufferAckSent.Value(), 10)
	stitems[2] = `"toAckBytes":` + strconv.FormatUint(stats.ToAckBytes.Value(), 10)
	stitems[3] = `"streamreqs":` + strconv.FormatUint(stats.TotalStreamReq.Value(), 10)
	stitems[4] = `"snapshots":` + strconv.FormatUint(stats.TotalSnapShot.Value(), 10)
	stitems[5] = `"mutations":` + strconv.FormatUint(stats.TotalMutation.Value(), 10)

	stitems[6] = `"collectionCreate":` + strconv.FormatUint(stats.CollectionCreate.Value(), 10)
	stitems[7] = `"collectionDrop":` + strconv.FormatUint(stats.CollectionDrop.Value(), 10)
	stitems[8] = `"collectionFlush":` + strconv.FormatUint(stats.CollectionFlush.Value(), 10)
	stitems[9] = `"scopeCreate":` + strconv.FormatUint(stats.ScopeCreate.Value(), 10)
	stitems[10] = `"scopeDrop":` + strconv.FormatUint(stats.ScopeDrop.Value(), 10)
	stitems[11] = `"collectionChanged":` + strconv.FormatUint(stats.CollectionChanged.Value(), 10)
	stitems[12] = `"seqnoAdvanced":` + strconv.FormatUint(stats.SeqnoAdvanced.Value(), 10)
	stitems[13] = `"osoSnapshotStart":` + strconv.FormatUint(stats.OsoSnapshotStart.Value(), 10)
	stitems[14] = `"osoSnapshotEnd":` + strconv.FormatUint(stats.OsoSnapshotEnd.Value(), 10)

	stitems[15] = `"streamends":` + strconv.FormatUint(stats.TotalStreamEnd.Value(), 10)
	stitems[16] = `"closestreams":` + strconv.FormatUint(stats.TotalCloseStream.Value(), 10)
	stitems[17] = `"lastAckTime":` + getTimeDur(stats.LastAckTime.Value()).String()
	stitems[18] = `"lastNoopSend":` + getTimeDur(stats.LastNoopSend.Value()).String()
	stitems[19] = `"lastNoopRecv":` + getTimeDur(stats.LastNoopRecv.Value()).String()
	stitems[20] = `"lastMsgSend":` + getTimeDur(stats.LastMsgSend.Value()).String()
	stitems[21] = `"lastMsgRecv":` + getTimeDur(stats.LastMsgRecv.Value()).String()
	stitems[22] = `"rcvchLen":` + strconv.FormatUint((uint64)(len(stats.rcvch)), 10)
	stitems[23] = `"incomingMsg":` + strconv.FormatUint(stats.IncomingMsg.Value(), 10)
	stitems[24] = `"queuedItems":` + strconv.FormatUint(uint64(stats.mutationQueue.GetItems()), 10)
	stitems[25] = `"queueSize":` + strconv.FormatUint(uint64(stats.mutationQueue.GetSize()), 10)
	stitems[26] = `"totalEnq":` + strconv.FormatUint(uint64(stats.mutationQueue.GetTotalEnq()), 10)
	stitems[27] = `"totalDeq":` + strconv.FormatUint(uint64(stats.mutationQueue.GetTotalDeq()), 10)
	stitems[28] = `"totalBlockedDur":` + strconv.FormatUint(stats.BlockedDurHist.GetTotal(), 10)
	stitems[29] = `"projBlockedHist":` + stats.BlockedDurHist.String()
	statjson := strings.Join(stitems[:], ",")

	statsStr := fmt.Sprintf("{%v}", statjson)
	latencyStr := stats.Dcplatency.MarshallJSON()
	return statsStr, latencyStr
}

func (stats *DcpStats) Map() (map[string]interface{}, map[string]interface{}) {
	var statMap = make(map[string]interface{}, 30)

	statMap["bytes"] = stats.TotalBytes.Value()
	statMap["bufferacks"] = stats.TotalBufferAckSent.Value()
	statMap["toAckBytes"] = stats.ToAckBytes.Value()
	statMap["streamreqs"] = stats.TotalStreamReq.Value()
	statMap["snapshots"] = stats.TotalSnapShot.Value()
	statMap["mutations"] = stats.TotalMutation.Value()

	statMap["collectionCreate"] = stats.CollectionCreate.Value()
	statMap["collectionDrop"] = stats.CollectionDrop.Value()
	statMap["collectionFlush"] = stats.CollectionFlush.Value()
	statMap["scopeCreate"] = stats.ScopeCreate.Value()
	statMap["scopeDrop"] = stats.ScopeDrop.Value()
	statMap["collectionChanged"] = stats.CollectionChanged.Value()
	statMap["seqnoAdvanced"] = stats.SeqnoAdvanced.Value()
	statMap["osoSnapshotStart"] = stats.OsoSnapshotStart.Value()
	statMap["osoSnapshotEnd"] = stats.OsoSnapshotEnd.Value()

	statMap["streamends"] = stats.TotalStreamEnd.Value()
	statMap["closestreams"] = stats.TotalCloseStream.Value()
	if stats.LastAckTime.Value() != 0 {
		statMap["lastAckTime"] = logstats.NewTimestamp(time.Unix(0, stats.LastAckTime.Value()))
	}
	if stats.LastNoopSend.Value() != 0 {
		statMap["lastNoopSend"] = logstats.NewTimestamp(time.Unix(0, stats.LastNoopSend.Value()))
	}
	if stats.LastNoopRecv.Value() != 0 {
		statMap["lastNoopRecv"] = logstats.NewTimestamp(time.Unix(0, stats.LastNoopRecv.Value()))
	}
	if stats.LastMsgSend.Value() != 0 {
		statMap["lastMsgSend"] = logstats.NewTimestamp(time.Unix(0, stats.LastMsgSend.Value()))
	}
	if stats.LastMsgRecv.Value() != 0 {
		statMap["lastMsgRecv"] = logstats.NewTimestamp(time.Unix(0, stats.LastMsgRecv.Value()))
	}
	statMap["rcvchLen"] = (uint64)(len(stats.rcvch))
	statMap["incomingMsg"] = stats.IncomingMsg.Value()
	statMap["queuedItems"] = uint64(stats.mutationQueue.GetItems())
	statMap["queueSize"] = uint64(stats.mutationQueue.GetSize())
	statMap["totalEnq"] = uint64(stats.mutationQueue.GetTotalEnq())
	statMap["totalDeq"] = uint64(stats.mutationQueue.GetTotalDeq())

	statMap["totBlckd"] = stats.BlockedDurHist.GetTotal()
	statMap["projBlckdHist"] = stats.BlockedDurHist.GetValue()

	return statMap, stats.Dcplatency.Json()
}

func (feed *DcpFeed) logStats() {
	var statLogger = logstats.GetGlobalStatLogger()

	var statsMap, latencyMap = feed.stats.Map()
	var feedMap = map[string]interface{}{}
	feedMap[feed.stats.StreamNo] = map[string]interface{}{
		"stats": statsMap,
		"ltc":   latencyMap,
	}

	if statLogger == nil {
		var feedMapBytes, err = json.Marshal(feedMap)
		if err != nil {
			logging.Errorf("%v marshal failed with err - %v", feed.Name(), err)
		} else {
			logging.Infof("%v stats: %v", feed.Name(), string(feedMapBytes))
		}
	} else {
		statLogger.Write(feed.name.String(), feedMap)
	}
}

// FailoverLog containing vvuid and sequnce number
type FailoverLog [][2]uint64

// Latest will return the recent vbuuid and its high-seqno.
func (flogp *FailoverLog) Latest() (vbuuid, seqno uint64, err error) {
	if flogp != nil {
		flog := *flogp
		latest := flog[0]
		return latest[0], latest[1], nil
	}
	return vbuuid, seqno, ErrorInvalidLog
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
				return nil, ErrorConnection
			}
		}
	case <-finch:
		return nil, ErrorConnection
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

// parse failover log fields from response body.
func parseFailoverLog(body []byte) (*FailoverLog, error) {
	if len(body)%16 != 0 {
		fmsg := "invalid body length %v, in failover-log\n"
		err := fmt.Errorf(fmsg, len(body))
		return nil, err
	}
	log := make(FailoverLog, len(body)/16)
	for i, j := 0, 0; i < len(body); i += 16 {
		vuuid := binary.BigEndian.Uint64(body[i : i+8])
		seqno := binary.BigEndian.Uint64(body[i+8 : i+16])
		log[j] = [2]uint64{vuuid, seqno}
		j++
	}
	return &log, nil
}

// parse vbno,seqno from response body for get-seqnos.
func parseGetSeqnos(body []byte) (map[uint16]uint64, error) {
	if len(body)%10 != 0 {
		fmsg := "invalid body length %v, in get-seqnos\n"
		err := fmt.Errorf(fmsg, len(body))
		return nil, err
	}
	seqnos := make(map[uint16]uint64)
	for i := 0; i < len(body); i += 10 {
		vbno := binary.BigEndian.Uint16(body[i : i+2])
		seqno := binary.BigEndian.Uint64(body[i+2 : i+10])
		seqnos[vbno] = seqno
	}
	return seqnos, nil
}

func computeLatency(stream *DcpStream) int64 {
	now := time.Now().UnixNano()
	strm_seqno := stream.Seqno
	if stream.Snapend == 0 || strm_seqno == stream.Snapend {
		return 0
	}
	delta := now - stream.LastSeen
	stream.LastSeen = now
	return delta
}

// receive loop
func (feed *DcpFeed) doReceive(
	rcvch chan []interface{}, finch chan bool, conn *Client) {

	defer func() {
		if !feed.useAtomicMutationQueue {
			close(rcvch)
		} else {
			feed.closeMutQueue <- true // write to this channel so that DequeueMutations will exit
		}
	}()

	var headerBuf [transport.HDR_LEN]byte
	var duration time.Duration
	var start time.Time
	var blocked bool

	epoc := time.Now()

	var bytes int
	var err error
	// Cached version of feed.readDeadline
	// Used inorder to prevent atomic access to feed.readDeadline in every iteration of "loop"
	var enableReadDeadline bool
loop:
	for {
		pkt := transport.MCRequest{} // always a new instance.

		// Enable read deadline only when doDcpOpen or doDcpGetFailoverLog or doDcpGetDeqnos
		// finish execution as these methods require a different read deadline
		if !enableReadDeadline {
			enableReadDeadline = (atomic.LoadInt32(&feed.enableReadDeadline) == 1)
			bytes, err = pkt.Receive(conn.conn, headerBuf[:])
		} else {
			// In cases where projector misses TCP notificatons like connection closure,
			// the dcp_feed would continuously wait for data on the connection resulting
			// in index build getting stuck.
			// In such scenarios, the read deadline would terminate the blocking read
			// calls thereby unblocking the dcp_feed. This is only a safe-guard measure
			// to handle highly unlikely scenarios like missing TCP notifications. In
			// those cases, an "i/o timeout" error will be returned to the reader.
			conn.SetMcdMutationReadDeadline()
			bytes, err = pkt.Receive(conn.conn, headerBuf[:])
			// Resetting the read deadline here is unnecessary because doReceive() is the
			// only thread reading data from conn after read deadline is enabled.
			// If reset does not happen here, then the next SetMcdMutationReadDeadline
			// would update the deadline
			// conn.ResetMcdMutationReadDeadline()
		}

		if err != nil && err == io.EOF {
			logging.Infof("%v EOF received\n", feed.logPrefix)
			break loop

		} else if feed.isClosed() {
			logging.Infof("%v doReceive(): connection closed\n", feed.logPrefix)
			break loop

		} else if err != nil {
			logging.Errorf("%v doReceive(): %v\n", feed.logPrefix, err)
			break loop
		}

		now := time.Now().UnixNano()
		feed.stats.LastMsgRecv.Set(now)

		// Immediately respond to NOOP and listen for next message.
		// NOOPs are not accounted for buffer-ack.
		if pkt.Opcode == transport.DCP_NOOP {
			feed.stats.LastNoopRecv.Set(now)
			noop := &transport.MCResponse{
				Opcode: transport.DCP_NOOP, Opaque: pkt.Opaque,
			}

			func() {
				// Timeout here will unblock doReceive() thread after some time.
				feed.mu.Lock()
				defer feed.mu.Unlock()

				conn.SetMcdConnectionDeadline()
				defer conn.ResetMcdConnectionDeadline()

				if err := conn.TransmitResponse(noop); err != nil {
					logging.Errorf("%v NOOP.Transmit(): %v", feed.logPrefix, err)
				} else {
					now = time.Now().UnixNano()
					feed.stats.LastNoopSend.Set(now)
					feed.stats.LastMsgSend.Set(now)
					fmsg := "%v responded to NOOP ok ...\n"
					logging.Tracef(fmsg, feed.logPrefix)
				}
			}()

			continue loop
		}

		logging.LazyTrace(func() string {
			return fmt.Sprintf("%v packet received %#v", feed.logPrefix, logging.TagUD(pkt))
		})

		// If the projector RSS is beyond rssThreshod * maxSystemMemory (by default 10% of RSS)
		// and the used memory in the system is greater than usedMemThreshold * maxSystemMemory
		// (by default 50% of RSS), then the DCP feed will reduce the rate at which it consumes
		// mutations from KV by sleeping for some time if required
		//
		// This is done to make sure that the downstream would eventually consume all the
		// mutations and the projector process RSS will eventually come down. Once the RSS
		// comes below 10% of total system memory, then the feed would not throttle.
		if feed.collectionsAware {
			memThrottler.DoThrottle(feed.isIncrBuild)
		}

		if len(rcvch) == cap(rcvch) {
			start, blocked = time.Now(), true
		}

		if feed.useAtomicMutationQueue {
			select {
			case <-finch:
				break loop
			default:
				feed.mutationQueue.Enqueue(&pkt, bytes)
				feed.stats.IncomingMsg.Add(1)
			}
		} else {
			select {
			case rcvch <- []interface{}{&pkt, bytes}:
				feed.stats.IncomingMsg.Add(1)
			case <-finch:
				break loop
			}
		}
		if blocked {
			blockedTs := time.Since(start)
			duration += blockedTs
			blocked = false

			feed.stats.BlockedDurHist.Add(int64(blockedTs))

			if blockedTs > blockdThresholdDur {
				percent := float64(duration) / float64(time.Since(epoc))
				fmsg := "%v DCP-socket -> projector blocked %v (%f%%)"
				logging.Infof(fmsg, feed.logPrefix, blockedTs, percent)
			}
		}
	}
}

func (feed *DcpFeed) addToVbStreams(vbno uint16, stream *DcpStream) {
	feed.vbstreamsMu.Lock()
	defer feed.vbstreamsMu.Unlock()

	feed.vbstreams[vbno] = stream
}

func (feed *DcpFeed) getFromVbStreams(vbno uint16) (*DcpStream, bool) {
	feed.vbstreamsMu.RLock()
	defer feed.vbstreamsMu.RUnlock()

	stream, ok := feed.vbstreams[vbno]
	return stream, ok
}

func (feed *DcpFeed) deleteFromVbStreams(vbno uint16) {
	feed.vbstreamsMu.Lock()
	defer feed.vbstreamsMu.Unlock()

	delete(feed.vbstreams, vbno)
}

func (feed *DcpFeed) lenVbStreams() int {
	feed.vbstreamsMu.RLock()
	defer feed.vbstreamsMu.RUnlock()

	return len(feed.vbstreams)
}

// Truncate Feed Name:
// The input name should follow following format.
//
//	DcpFeedPrefix<keyspaceId>-<topic>-<uuid>/<connectionNumber>
//
// truncFeedName removes the keyspaceId from the feed name.
func truncFeedName(name *DcpFeedname2) (string, error) {
	if !strings.HasPrefix(name.StreamLogPrefix(), DcpFeedPrefix) {
		return name.StreamLogPrefix(), nil
	}

	return strings.Join([]string{
		DcpFeedPrefix, name.Topic,
		strconv.FormatUint(name.StreamUuid, 10),
	}, "-"), nil
}

type DcpFeedname2 struct {
	Topic      string
	Keyspace   string
	Opaque     uint16
	StreamUuid uint64
	StreamId   int
}

func NewDcpFeedName2(topic, keyspace string,
	opaque uint16, streamUuid uint64, streamId int) *DcpFeedname2 {

	return &DcpFeedname2{
		topic,
		keyspace,
		opaque,
		streamUuid,
		streamId,
	}

}

func (dfn *DcpFeedname2) String() string {
	var elements = make([]string, 0, 5)
	elements = append(elements, "DCPT")
	if len(dfn.Topic) != 0 {
		elements = append(elements, dfn.Topic)
	}
	if len(dfn.Keyspace) != 0 {
		elements = append(elements, dfn.Keyspace)
	}
	if dfn.StreamUuid != 0 {
		elements = append(elements, strconv.FormatUint(dfn.StreamUuid, 10))
	}
	if dfn.Opaque != 0 {
		elements = append(elements, fmt.Sprintf("##%x", dfn.Opaque))
	}
	return strings.Join(elements, ":")
}

func (dfn *DcpFeedname2) StreamLogPrefix() string {
	return strings.Join([]string{
		dfn.String(),
		strconv.Itoa(dfn.StreamId),
	}, ":")
}

func (dfn *DcpFeedname2) Clone() *DcpFeedname2 {
	return &DcpFeedname2{
		dfn.Topic,
		dfn.Keyspace,
		dfn.Opaque,
		dfn.StreamUuid,
		dfn.StreamId,
	}
}
