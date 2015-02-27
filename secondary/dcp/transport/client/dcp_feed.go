// go implementation of dcp client.
// See https://github.com/couchbaselabs/cbupr/blob/master/transport-spec.md

package memcached

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/couchbase/indexing/secondary/dcp/transport"
	"github.com/couchbase/indexing/secondary/logging"
	"io"
	"strconv"
	"time"
)

const dcpMutationExtraLen = 16
const bufferAckThreshold = 0.2
const opaqueOpen = 0xBEAF0001
const opaqueFailover = 0xDEADBEEF

// error codes
var ErrorInvalidLog = errors.New("couchbase.errorInvalidLog")

// ErrorConnection
var ErrorConnection = errors.New("dcp.connection")

// ErrorInvalidFeed
var ErrorInvalidFeed = errors.New("dcp.invalidFeed")

// DcpFeed represents an DCP feed. A feed contains a connection to a single
// host and multiple vBuckets
type DcpFeed struct {
	conn      *Client               // connection to DCP producer
	outch     chan<- *DcpEvent      // Exported channel for receiving DCP events
	vbstreams map[uint16]*DcpStream // vb->stream mapping
	// genserver
	reqch     chan []interface{}
	finch     chan bool
	logPrefix string
	// stats
	toAckBytes  uint32   // bytes client has read
	maxAckBytes uint32   // Max buffer control ack bytes
	stats       DcpStats // Stats for dcp client
}

// NewDcpFeed creates a new DCP Feed.
func NewDcpFeed(
	mc *Client, name string, outch chan<- *DcpEvent,
	config map[string]interface{}) (*DcpFeed, error) {

	genChanSize := config["genChanSize"].(int)
	dataChanSize := config["dataChanSize"].(int)
	feed := &DcpFeed{
		outch:     outch,
		vbstreams: make(map[uint16]*DcpStream),
		reqch:     make(chan []interface{}, genChanSize),
		finch:     make(chan bool),
		// TODO: would be nice to add host-addr as part of prefix.
		logPrefix: fmt.Sprintf("DCPT[%s]", name),
	}

	mc.Hijack()
	feed.conn = mc
	rcvch := make(chan []interface{}, dataChanSize)
	go feed.genServer(feed.reqch, feed.finch, rcvch)
	go feed.doReceive(rcvch)
	logging.Infof("%v feed started ...", feed.logPrefix)
	return feed, nil
}

// DcpOpen to connect with a DCP producer.
// Name: name of te DCP connection
// sequence: sequence number for the connection
// bufsize: max size of the application
func (feed *DcpFeed) DcpOpen(name string, sequence, bufsize uint32) error {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{dfCmdOpen, name, sequence, bufsize, respch}
	resp, err := failsafeOp(feed.reqch, respch, cmd, feed.finch)
	return opError(err, resp, 0)
}

// DcpGetFailoverLog for given list of vbuckets.
func (feed *DcpFeed) DcpGetFailoverLog(
	vblist []uint16) (map[uint16]*FailoverLog, error) {

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{dfCmdGetFailoverlog, vblist, respch}
	resp, err := failsafeOp(feed.reqch, respch, cmd, feed.finch)
	err = opError(err, resp, 1)
	return resp[0].(map[uint16]*FailoverLog), err
}

// DcpRequestStream for a single vbucket.
func (feed *DcpFeed) DcpRequestStream(vbno, opaqueMSB uint16, flags uint32,
	vuuid, startSequence, endSequence, snapStart, snapEnd uint64) error {

	respch := make(chan []interface{}, 1)
	cmd := []interface{}{
		dfCmdRequestStream, vbno, opaqueMSB, flags, vuuid,
		startSequence, endSequence, snapStart, snapEnd, respch}
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
	dfCmdRequestStream
	dfCmdCloseStream
	dfCmdClose
)

func (feed *DcpFeed) genServer(
	reqch chan []interface{}, finch chan bool, rcvch chan []interface{}) {

	defer func() { // panic safe
		if r := recover(); r != nil {
			logging.Errorf("%v crashed: %v\n", feed.logPrefix, r)
			logging.Errorf("%s", logging.StackTrace())
		}
		close(feed.finch)
		logging.Infof("%v ... stopped\n", feed.logPrefix)
	}()

	prefix := feed.logPrefix
	inactivityTick := time.Tick(1 * 60 * time.Second) // 1 minute

loop:
	for {
		select {
		case <-inactivityTick:
			now := time.Now().UnixNano()
			for _, stream := range feed.vbstreams {
				delta := (now - stream.LastSeen) / 1000000000 // in Seconds
				if stream.LastSeen != 0 && delta > 10 /*seconds*/ {
					fmsg := "%v ##%x event for vb %v lastSeen %vSec before\n"
					logging.Warnf(
						fmsg, prefix, stream.AppOpaque, stream.Vbucket, delta)
				}
			}

		case msg := <-reqch:
			cmd := msg[0].(byte)
			switch cmd {
			case dfCmdOpen:
				name, sequence := msg[1].(string), msg[2].(uint32)
				bufsize, respch := msg[3].(uint32), msg[4].(chan []interface{})
				err := feed.doDcpOpen(name, sequence, bufsize, rcvch)
				respch <- []interface{}{err}

			case dfCmdGetFailoverlog:
				vblist, respch := msg[1].([]uint16), msg[2].(chan []interface{})
				if len(feed.vbstreams) > 0 {
					fmsg := "%v active streams in doDcpGetFailoverLog"
					logging.Errorf(fmsg, prefix)
					respch <- []interface{}{nil, ErrorInvalidFeed}
				}
				flog, err := feed.doDcpGetFailoverLog(vblist, rcvch)
				respch <- []interface{}{flog, err}

			case dfCmdRequestStream:
				vbno, opaqueMSB := msg[1].(uint16), msg[2].(uint16)
				flags, vuuid := msg[3].(uint32), msg[4].(uint64)
				startSequence, endSequence := msg[5].(uint64), msg[6].(uint64)
				snapStart, snapEnd := msg[7].(uint64), msg[8].(uint64)
				respch := msg[9].(chan []interface{})
				err := feed.doDcpRequestStream(
					vbno, opaqueMSB, flags, vuuid,
					startSequence, endSequence, snapStart, snapEnd)
				respch <- []interface{}{err}

			case dfCmdCloseStream:
				vbno, opaqueMSB := msg[1].(uint16), msg[2].(uint16)
				respch := msg[9].(chan []interface{})
				err := feed.doDcpCloseStream(vbno, opaqueMSB)
				respch <- []interface{}{err}

			case dfCmdClose:
				feed.sendStreamEnd(feed.outch)
				feed.conn.Close()
				feed.conn = nil
				break loop
			}

		case resp, ok := <-rcvch:
			if !ok {
				break loop
			}
			pkt, bytes := resp[0].(*transport.MCRequest), resp[1].(int)
			switch feed.handlePacket(pkt, bytes) {
			case "exit":
				break loop
			}
		}
	}
}

func (feed *DcpFeed) handlePacket(
	pkt *transport.MCRequest, bytes int) string {

	var event *DcpEvent
	feed.stats.TotalBytes += uint64(bytes)
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
	stream := feed.vbstreams[vb]
	if stream == nil {
		fmsg := "%v spurious %v for %d: %#v\n"
		logging.Fatalf(fmsg, prefix, pkt.Opcode, vb, pkt)
		return "ok" // yeah it not _my_ mistake...
	}
	stream.LastSeen = time.Now().UnixNano()
	switch pkt.Opcode {
	case transport.DCP_STREAMREQ:
		event = newDcpEvent(pkt, stream)
		feed.handleStreamRequest(res, vb, stream, event)

	case transport.DCP_MUTATION, transport.DCP_DELETION,
		transport.DCP_EXPIRATION:
		event = newDcpEvent(pkt, stream)
		feed.stats.TotalMutation++
		sendAck = true

	case transport.DCP_STREAMEND:
		event = newDcpEvent(pkt, stream)
		sendAck = true
		delete(feed.vbstreams, vb)
		fmsg := "%v ##%x DCP_STREAMEND for vb %d\n"
		logging.Infof(fmsg, prefix, stream.AppOpaque, vb)

	case transport.DCP_SNAPSHOT:
		event = newDcpEvent(pkt, stream)
		event.SnapstartSeq = binary.BigEndian.Uint64(pkt.Extras[0:8])
		event.SnapendSeq = binary.BigEndian.Uint64(pkt.Extras[8:16])
		event.SnapshotType = binary.BigEndian.Uint32(pkt.Extras[16:20])
		feed.stats.TotalSnapShot++
		sendAck = true
		fmsg := "%v ##%x DCP_SNAPSHOT for vb %d\n"
		logging.Infof(fmsg, prefix, stream.AppOpaque, vb)

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
		sendAck = true
		delete(feed.vbstreams, vb)
		fmsg := "%v ##%x DCP_CLOSESTREAM for vb %d\n"
		logging.Infof(fmsg, prefix, stream.AppOpaque, vb)

	case transport.DCP_CONTROL, transport.DCP_BUFFERACK:
		if res.Status != transport.SUCCESS {
			fmsg := "%v ##%x opcode %v received status %v\n"
			logging.Errorf(fmsg, prefix, stream.AppOpaque, pkt.Opcode, res.Status)
		}

	case transport.DCP_NOOP:
		noop := &transport.MCRequest{Opcode: transport.DCP_NOOP}
		if err := feed.conn.Transmit(noop); err != nil { // send a NOOP back
			logging.Errorf("%v NOOP.Transmit(): %v", prefix, err)
		}

	case transport.DCP_ADDSTREAM:
		fmsg := "%v ##%x opcode DCP_ADDSTREAM not implemented\n"
		logging.Fatalf(fmsg, prefix, stream.AppOpaque)

	default:
		fmsg := "%v opcode %v not known for vbucket %d\n"
		logging.Warnf(fmsg, prefix, pkt.Opcode, vb)
	}

	if event != nil {
		feed.outch <- event
	}
	l := len(feed.vbstreams)
	if event.Opcode == transport.DCP_CLOSESTREAM && l == 0 {
		fmsg := "%v last DCP_CLOSESTREAM received exiting"
		logging.Infof(fmsg, prefix)
		return "exit"
	}
	feed.sendBufferAck(sendAck, uint32(bytes))
	return "ok"
}

func (feed *DcpFeed) doDcpGetFailoverLog(
	vblist []uint16,
	rcvch chan []interface{}) (map[uint16]*FailoverLog, error) {

	rq := &transport.MCRequest{
		Opcode: transport.DCP_FAILOVERLOG,
		Opaque: opaqueFailover,
	}
	failoverLogs := make(map[uint16]*FailoverLog)
	for _, vBucket := range vblist {
		rq.VBucket = vBucket
		if err := feed.conn.Transmit(rq); err != nil {
			fmsg := "%v doDcpGetFailoverLog.Transmit(): %v"
			logging.Errorf(fmsg, feed.logPrefix, err)
			return nil, err
		}
		msg, ok := <-rcvch
		if !ok {
			fmsg := "%v doDcpGetFailoverLog.rcvch closed"
			logging.Errorf(fmsg, feed.logPrefix)
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
		if req.Opcode != transport.DCP_FAILOVERLOG || req.Status != transport.SUCCESS {
			fmsg := "%v unexpected #opcode %v"
			logging.Errorf(fmsg, feed.logPrefix, req.Opcode)
			return nil, ErrorInvalidFeed
		}
		flog, err := parseFailoverLog(req.Body)
		if err != nil {
			fmsg := "%v parse failover logs for vb %d"
			logging.Errorf(fmsg, feed.logPrefix, vBucket)
			return nil, ErrorInvalidFeed
		}
		failoverLogs[vBucket] = flog
	}
	return failoverLogs, nil
}

func (feed *DcpFeed) doDcpOpen(
	name string, sequence, bufsize uint32, rcvch chan []interface{}) error {

	rq := &transport.MCRequest{
		Opcode: transport.DCP_OPEN,
		Key:    []byte(name),
		Opaque: opaqueOpen,
	}
	rq.Extras = make([]byte, 8)
	binary.BigEndian.PutUint32(rq.Extras[:4], sequence)
	binary.BigEndian.PutUint32(rq.Extras[4:], 1) // we are consumer

	prefix := feed.logPrefix
	if err := feed.conn.Transmit(rq); err != nil {
		return err
	}
	msg, ok := <-rcvch
	if !ok {
		logging.Errorf("%v doDcpOpen.rcvch closed", prefix)
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
		logging.Errorf("%v unexpected #%v", prefix, req.Opcode)
		return ErrorConnection
	} else if rq.Opaque != req.Opaque {
		fmsg := "%v opaque mismatch, %v != %v"
		logging.Errorf(fmsg, prefix, req.Opaque, req.Opaque)
		return ErrorConnection
	} else if req.Status != transport.SUCCESS {
		fmsg := "%v doDcpOpen response status %v"
		logging.Errorf(fmsg, prefix, req.Status)
		return ErrorConnection
	}

	// send a DCP control message to set the window size for
	// this connection
	if bufsize > 0 {
		rq := &transport.MCRequest{
			Opcode: transport.DCP_CONTROL,
			Key:    []byte("connection_buffer_size"),
			Body:   []byte(strconv.Itoa(int(bufsize))),
		}
		if err := feed.conn.Transmit(rq); err != nil {
			fmsg := "%v doDcpOpen.DCP_CONTROL.Transmit(): %v"
			logging.Errorf(fmsg, prefix, err)
			return err
		}
		msg, ok := <-rcvch
		if !ok {
			logging.Errorf("%v doDcpOpen.DCP_CONTROL.rcvch closed", prefix)
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
			logging.Errorf("%v DCP_CONTROL != #%v", prefix, req.Opcode)
			return ErrorConnection
		} else if req.Status != transport.SUCCESS {
			fmsg := "%v doDcpOpen response status %v"
			logging.Errorf(fmsg, prefix, req.Status)
			return ErrorConnection
		}
		feed.maxAckBytes = uint32(bufferAckThreshold * float32(bufsize))
	}
	return nil
}

func (feed *DcpFeed) doDcpRequestStream(
	vbno, opaqueMSB uint16, flags uint32,
	vuuid, startSequence, endSequence, snapStart, snapEnd uint64) error {

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
	if err := feed.conn.Transmit(rq); err != nil {
		fmsg := "%v ##%x doDcpRequestStream.Transmit(): %v"
		logging.Errorf(fmsg, prefix, opaqueMSB, err)
		return err
	}
	stream := &DcpStream{
		AppOpaque: opaqueMSB,
		Vbucket:   vbno,
		Vbuuid:    vuuid,
		StartSeq:  startSequence,
		EndSeq:    endSequence,
	}
	feed.vbstreams[vbno] = stream
	return nil
}

func (feed *DcpFeed) doDcpCloseStream(vbno, opaqueMSB uint16) error {
	stream, ok := feed.vbstreams[vbno]
	stream.CloseOpaque = opaqueMSB
	prefix := feed.logPrefix
	if !ok || stream == nil {
		fmsg := "%v ##%x stream for vb %d is not active"
		logging.Errorf(fmsg, prefix, stream.AppOpaque, vbno)
		return ErrorConnection
	}
	rq := &transport.MCRequest{
		Opcode:  transport.DCP_CLOSESTREAM,
		VBucket: vbno,
		Opaque:  composeOpaque(vbno, opaqueMSB),
	}
	if err := feed.conn.Transmit(rq); err != nil {
		fmsg := "%v ##%x doDcpCloseStream.Transmit(): %v"
		logging.Errorf(fmsg, prefix, stream.AppOpaque, err)
		return err
	}
	return nil
}

// generate stream end responses for all active vb streams
func (feed *DcpFeed) sendStreamEnd(outch chan<- *DcpEvent) {
	for vb, stream := range feed.vbstreams {
		dcpEvent := &DcpEvent{
			VBucket: vb,
			VBuuid:  stream.Vbuuid,
			Opcode:  transport.DCP_STREAMEND,
		}
		outch <- dcpEvent
	}
}

func (feed *DcpFeed) handleStreamRequest(
	res *transport.MCResponse, vb uint16, stream *DcpStream, event *DcpEvent) {

	prefix := feed.logPrefix
	switch {
	case res.Status == transport.ROLLBACK && len(res.Extras) != 8:
		event.Status, event.Seqno = res.Status, 0
		fmsg := "%v ##%x STREAMREQ(%v) invalid rollback: %v\n"
		logging.Errorf(fmsg, prefix, stream.AppOpaque, vb, res.Extras)
		delete(feed.vbstreams, vb)

	case res.Status == transport.ROLLBACK:
		rollback := binary.BigEndian.Uint64(res.Extras)
		event.Status, event.Seqno = res.Status, rollback
		fmsg := "%v ##%x STREAMREQ(%v) with rollback %d\n"
		logging.Warnf(fmsg, prefix, stream.AppOpaque, vb, rollback)
		delete(feed.vbstreams, vb)

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
		logging.Infof(fmsg, prefix, stream.AppOpaque, vb)

	default:
		event.Status = res.Status
		event.VBucket = vb
		fmsg := "%v ##%x STREAMREQ(%v) unexpected status: %v\n"
		logging.Errorf(fmsg, prefix, stream.AppOpaque, vb, res.Status)
		delete(feed.vbstreams, vb)
	}
	return
}

// Send buffer ack
func (feed *DcpFeed) sendBufferAck(sendAck bool, bytes uint32) {
	prefix := feed.logPrefix
	if sendAck {
		totalBytes := feed.toAckBytes + bytes
		if totalBytes > feed.maxAckBytes {
			feed.toAckBytes = 0
			bufferAck := &transport.MCRequest{
				Opcode: transport.DCP_BUFFERACK,
			}
			bufferAck.Extras = make([]byte, 4)
			binary.BigEndian.PutUint32(bufferAck.Extras[:4], uint32(totalBytes))
			feed.stats.TotalBufferAckSent++
			logging.Infof("%v buffer-ack %v\n", prefix, totalBytes)
		}
		feed.toAckBytes += bytes
	}
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

// DcpStream is per stream data structure over an DCP Connection.
type DcpStream struct {
	AppOpaque   uint16
	CloseOpaque uint16
	Vbucket     uint16 // Vbucket id
	Vbuuid      uint64 // vbucket uuid
	StartSeq    uint64 // start sequence number
	EndSeq      uint64 // end sequence number
	LastSeen    int64  // UnixNano value of last seen
	connected   bool
}

// DcpEvent memcached events for DCP streams.
type DcpEvent struct {
	Opcode     transport.CommandCode // Type of event
	Status     transport.Status      // Response status
	VBucket    uint16                // VBucket this event applies to
	Opaque     uint16                // 16 MSB of opaque
	VBuuid     uint64                // This field is set by downstream
	Flags      uint32                // Item flags
	Expiry     uint32                // Item expiration time
	Key, Value []byte                // Item key/value
	OldValue   []byte                // TODO: TBD: old document value
	Cas        uint64                // CAS value of the item
	// sequence number of the mutation, also doubles as rollback-seqno.
	Seqno        uint64
	SnapstartSeq uint64       // start sequence number of this snapshot
	SnapendSeq   uint64       // End sequence number of the snapshot
	SnapshotType uint32       // 0: disk 1: memory
	FailoverLog  *FailoverLog // Failover log containing vvuid and sequnce number
	Error        error        // Error value in case of a failure
}

func newDcpEvent(rq *transport.MCRequest, stream *DcpStream) *DcpEvent {
	event := &DcpEvent{
		Opcode:  rq.Opcode,
		VBucket: stream.Vbucket,
		VBuuid:  stream.Vbuuid,
		Key:     rq.Key,
		Value:   rq.Body,
		Cas:     rq.Cas,
	}
	// 16 LSBits are used by client library to encode vbucket number.
	// 16 MSBits are left for application to multiplex on opaque value.
	event.Opaque = appOpaque(rq.Opaque)

	if len(rq.Extras) >= tapMutationExtraLen {
		event.Seqno = binary.BigEndian.Uint64(rq.Extras[:8])
	}

	if len(rq.Extras) >= tapMutationExtraLen &&
		event.Opcode == transport.DCP_MUTATION ||
		event.Opcode == transport.DCP_DELETION ||
		event.Opcode == transport.DCP_EXPIRATION {

		event.Flags = binary.BigEndian.Uint32(rq.Extras[8:])
		event.Expiry = binary.BigEndian.Uint32(rq.Extras[12:])

	} else if len(rq.Extras) >= tapMutationExtraLen &&
		event.Opcode == transport.DCP_SNAPSHOT {

		event.SnapstartSeq = binary.BigEndian.Uint64(rq.Extras[:8])
		event.SnapendSeq = binary.BigEndian.Uint64(rq.Extras[8:16])
		event.SnapshotType = binary.BigEndian.Uint32(rq.Extras[16:20])
	}

	return event
}

func (event *DcpEvent) String() string {
	name := transport.CommandNames[event.Opcode]
	if name == "" {
		name = fmt.Sprintf("#%d", event.Opcode)
	}
	return name
}

// DcpStats on mutations/snapshots/buff-acks.
type DcpStats struct {
	TotalBytes         uint64
	TotalMutation      uint64
	TotalBufferAckSent uint64
	TotalSnapShot      uint64
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
		err := fmt.Errorf("invalid body length %v, in failover-log", len(body))
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

// receive loop
func (feed *DcpFeed) doReceive(rcvch chan []interface{}) {
	defer close(rcvch)

	var headerBuf [transport.HDR_LEN]byte

	for {
		pkt := transport.MCRequest{} // always a new instance.
		bytes, err := pkt.Receive(feed.conn.conn, headerBuf[:])
		if err != nil && err == io.EOF {
			logging.Infof("%v EOF received\n", feed.logPrefix)
			break

		} else if err != nil {
			logging.Errorf("%v doReceive(): %v\n", feed.logPrefix, err)
			break
		}
		logging.Tracef("%v packet received %#v", feed.logPrefix, pkt)
		rcvch <- []interface{}{&pkt, bytes}
	}
}
