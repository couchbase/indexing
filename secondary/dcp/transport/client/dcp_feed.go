// go implementation of dcp client.
// See https://github.com/couchbaselabs/cbupr/blob/master/transport-spec.md

package memcached

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/couchbase/indexing/secondary/dcp/transport"
	"github.com/couchbase/indexing/secondary/logging"
	"strconv"
	"sync"
	"time"
)

const dcpMutationExtraLen = 16
const bufferAckThreshold = 0.2
const opaqueOpen = 0xBEAF0001
const opaqueFailover = 0xDEADBEEF

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

// DcpStream is per stream data structure over an DCP Connection.
type DcpStream struct {
	Vbucket   uint16 // Vbucket id
	Vbuuid    uint64 // vbucket uuid
	StartSeq  uint64 // start sequence number
	EndSeq    uint64 // end sequence number
	LastSeen  int64  // UnixNano value of last seen
	connected bool
}

// DcpFeed represents an DCP feed. A feed contains a connection to a single
// host and multiple vBuckets
type DcpFeed struct {
	mu          sync.RWMutex
	C           <-chan *DcpEvent          // Exported channel for receiving DCP events
	vbstreams   map[uint16]*DcpStream     // vb->stream mapping
	closer      chan bool                 // closer
	conn        *Client                   // connection to DCP producer
	Error       error                     // error
	bytesRead   uint64                    // total bytes read on this connection
	toAckBytes  uint32                    // bytes client has read
	maxAckBytes uint32                    // Max buffer control ack bytes
	stats       DcpStats                  // Stats for dcp client
	transmitCh  chan *transport.MCRequest // transmit command channel
	transmitCl  chan bool                 //  closer channel for transmit go-routine
}

type DcpStats struct {
	TotalBytes         uint64
	TotalMutation      uint64
	TotalBufferAckSent uint64
	TotalSnapShot      uint64
}

// FailoverLog containing vvuid and sequnce number
type FailoverLog [][2]uint64

// error codes
var ErrorInvalidLog = errors.New("couchbase.errorInvalidLog")

func (flogp *FailoverLog) Latest() (vbuuid, seqno uint64, err error) {
	if flogp != nil {
		flog := *flogp
		latest := flog[0]
		return latest[0], latest[1], nil
	}
	return vbuuid, seqno, ErrorInvalidLog
}

func makeDcpEvent(rq transport.MCRequest, stream *DcpStream) *DcpEvent {
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

func sendCommands(mc *Client, ch chan *transport.MCRequest, closer chan bool) {
loop:
	for {
		select {
		case command := <-ch:
			if err := mc.Transmit(command); err != nil {
				logging.Warnf("Failed to transmit command %s. Error %s\n", command.Opcode.String(), err.Error())
				break loop
			}

		case <-closer:
			logging.Warnf("Exiting send command go routine ...")
			break loop
		}
	}
}

// NewDcpFeed creates a new DCP Feed.
// TODO: Describe side-effects on bucket instance and its connection pool.
func (mc *Client) NewDcpFeed() (*DcpFeed, error) {

	feed := &DcpFeed{
		conn:       mc,
		closer:     make(chan bool),
		vbstreams:  make(map[uint16]*DcpStream),
		transmitCh: make(chan *transport.MCRequest),
		transmitCl: make(chan bool),
	}

	go sendCommands(mc, feed.transmitCh, feed.transmitCl)
	return feed, nil
}

func doDcpOpen(mc *Client, name string, sequence uint32) error {

	rq := &transport.MCRequest{
		Opcode: transport.DCP_OPEN,
		Key:    []byte(name),
		Opaque: opaqueOpen,
	}

	rq.Extras = make([]byte, 8)
	binary.BigEndian.PutUint32(rq.Extras[:4], sequence)

	binary.BigEndian.PutUint32(rq.Extras[4:], 1) // we are consumer

	if err := mc.Transmit(rq); err != nil {
		return err
	}

	if res, err := mc.Receive(); err != nil {
		return err
	} else if res.Opcode != transport.DCP_OPEN {
		return fmt.Errorf("unexpected #opcode %v", res.Opcode)
	} else if rq.Opaque != res.Opaque {
		return fmt.Errorf("opaque mismatch, %v over %v", res.Opaque, res.Opaque)
	} else if res.Status != transport.SUCCESS {
		return fmt.Errorf("error %v", res.Status)
	}

	return nil
}

// DcpOpen to connect with a DCP producer.
// Name: name of te DCP connection
// sequence: sequence number for the connection
// bufsize: max size of the application
func (feed *DcpFeed) DcpOpen(name string, sequence uint32, bufSize uint32) error {
	mc := feed.conn

	if err := doDcpOpen(mc, name, sequence); err != nil {
		return err
	}

	// send a DCP control message to set the window size for the this connection
	if bufSize > 0 {
		rq := &transport.MCRequest{
			Opcode: transport.DCP_CONTROL,
			Key:    []byte("connection_buffer_size"),
			Body:   []byte(strconv.Itoa(int(bufSize))),
		}
		feed.transmitCh <- rq
		feed.maxAckBytes = uint32(bufferAckThreshold * float32(bufSize))
	}

	return nil
}

// DcpGetFailoverLog for given list of vbuckets.
func (mc *Client) DcpGetFailoverLog(
	vb []uint16) (map[uint16]*FailoverLog, error) {

	rq := &transport.MCRequest{
		Opcode: transport.DCP_FAILOVERLOG,
		Opaque: opaqueFailover,
	}

	if err := doDcpOpen(mc, "FailoverLog", 0); err != nil {
		return nil, fmt.Errorf("DCP_OPEN Failed %s", err.Error())
	}

	failoverLogs := make(map[uint16]*FailoverLog)
	for _, vBucket := range vb {
		rq.VBucket = vBucket
		if err := mc.Transmit(rq); err != nil {
			return nil, err
		}
		res, err := mc.Receive()

		if err != nil {
			return nil, fmt.Errorf("failed to receive %s", err.Error())
		} else if res.Opcode != transport.DCP_FAILOVERLOG || res.Status != transport.SUCCESS {
			return nil, fmt.Errorf("unexpected #opcode %v", res.Opcode)
		}

		flog, err := parseFailoverLog(res.Body)
		if err != nil {
			return nil, fmt.Errorf("unable to parse failover logs for vb %d", vb)
		}
		failoverLogs[vBucket] = flog
	}

	return failoverLogs, nil
}

// DcpRequestStream for a single vbucket.
func (feed *DcpFeed) DcpRequestStream(vbno, opaqueMSB uint16, flags uint32,
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

	feed.mu.Lock()
	defer feed.mu.Unlock()
	feed.transmitCh <- rq
	stream := &DcpStream{
		Vbucket:  vbno,
		Vbuuid:   vuuid,
		StartSeq: startSequence,
		EndSeq:   endSequence,
	}
	feed.vbstreams[vbno] = stream
	return nil
}

// CloseStream for specified vbucket.
func (feed *DcpFeed) CloseStream(vbno, opaqueMSB uint16) error {
	feed.mu.Lock()
	defer feed.mu.Unlock()

	if feed.vbstreams[vbno] == nil {
		return fmt.Errorf("Stream for vb %d has not been requested", vbno)
	}
	closeStream := &transport.MCRequest{
		Opcode:  transport.DCP_CLOSESTREAM,
		VBucket: vbno,
		Opaque:  composeOpaque(vbno, opaqueMSB),
	}
	feed.transmitCh <- closeStream
	return nil
}

// StartFeed to start the upper feed.
func (feed *DcpFeed) StartFeed() error {
	ch := make(chan *DcpEvent, 10000)
	feed.C = ch
	go feed.runFeed(ch)
	return nil
}

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

func handleStreamRequest(
	res *transport.MCResponse,
) (transport.Status, uint64, *FailoverLog, error) {

	var rollback uint64
	var err error

	switch {
	case res.Status == transport.ROLLBACK && len(res.Extras) != 8:
		err = fmt.Errorf("invalid rollback %v\n", res.Extras)
		return res.Status, 0, nil, err

	case res.Status == transport.ROLLBACK:
		rollback = binary.BigEndian.Uint64(res.Extras)
		logging.Warnf("Rollback %v for vb %v\n", rollback, res.Opaque)
		return res.Status, rollback, nil, nil

	case res.Status != transport.SUCCESS:
		err = fmt.Errorf("unexpected status %v, for %v", res.Status, res.Opaque)
		return res.Status, 0, nil, err
	}

	flog, err := parseFailoverLog(res.Body[:])
	return res.Status, rollback, flog, err
}

// generate stream end responses for all active vb streams
func (feed *DcpFeed) doStreamClose(ch chan *DcpEvent) {
	feed.mu.RLock()
	for vb, stream := range feed.vbstreams {
		dcpEvent := &DcpEvent{
			VBucket: vb,
			VBuuid:  stream.Vbuuid,
			Opcode:  transport.DCP_STREAMEND,
		}
		ch <- dcpEvent
	}
	feed.mu.RUnlock()
}

func (feed *DcpFeed) runFeed(ch chan *DcpEvent) {
	defer close(ch)
	var headerBuf [transport.HDR_LEN]byte
	var pkt transport.MCRequest
	var event *DcpEvent

	mc := feed.conn.Hijack()
	dcpStats := &feed.stats

loop:
	for {
		sendAck := false
		bytes, err := pkt.Receive(mc, headerBuf[:])
		if err != nil {
			logging.Warnf("Error in receive %s\n", err.Error())
			feed.Error = err
			// send all the stream close messages to the client
			feed.doStreamClose(ch)
			break loop
		} else {
			event = nil
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
			dcpStats.TotalBytes = uint64(bytes)

			feed.mu.RLock()
			stream := feed.vbstreams[vb]
			feed.mu.RUnlock()

			switch pkt.Opcode {
			case transport.DCP_STREAMREQ:
				if stream == nil {
					logging.Warnf("Stream not found for vb %d: %#v\n", vb, pkt)
					break loop
				}
				status, rb, flog, err := handleStreamRequest(res)
				if status == transport.ROLLBACK {
					event = makeDcpEvent(pkt, stream)
					event.Status = status
					event.Seqno = rb
					// rollback stream
					logging.Warnf("DCP_STREAMREQ with rollback %d for vb %d Failed: %v\n", rb, vb, err)
					// delete the stream from the vbmap for the feed
					feed.mu.Lock()
					delete(feed.vbstreams, vb)
					feed.mu.Unlock()

				} else if status == transport.SUCCESS {
					event = makeDcpEvent(pkt, stream)
					event.Status = status
					event.Seqno = stream.StartSeq
					event.FailoverLog = flog
					stream.connected = true
					logging.Warnf("DCP_STREAMREQ for vb %d successful\n", vb)

				} else if err != nil {
					logging.Warnf("DCP_STREAMREQ for vbucket %d erro %s\n", vb, err.Error())
					event = &DcpEvent{
						Opcode:  transport.DCP_STREAMREQ,
						Status:  status,
						VBucket: vb,
						Error:   err,
					}
					// delete the stream
					feed.mu.Lock()
					delete(feed.vbstreams, vb)
					feed.mu.Unlock()
				}

			case transport.DCP_MUTATION,
				transport.DCP_DELETION,
				transport.DCP_EXPIRATION:
				if stream == nil {
					logging.Warnf("Stream not found for vb %d: %#v\n", vb, pkt)
					break loop
				}
				event = makeDcpEvent(pkt, stream)
				dcpStats.TotalMutation++
				sendAck = true

			case transport.DCP_STREAMEND:
				if stream == nil {
					logging.Warnf("Stream not found for vb %d: %#v\n", vb, pkt)
					break loop
				}
				//stream has ended
				event = makeDcpEvent(pkt, stream)
				logging.Warnf("Stream Ended for vb %d\n", vb)
				sendAck = true

				feed.mu.Lock()
				delete(feed.vbstreams, vb)
				feed.mu.Unlock()

			case transport.DCP_SNAPSHOT:
				if stream == nil {
					logging.Warnf("Stream not found for vb %d: %#v\n", vb, pkt)
					break loop
				}
				// snapshot marker
				event = makeDcpEvent(pkt, stream)
				event.SnapstartSeq = binary.BigEndian.Uint64(pkt.Extras[0:8])
				event.SnapendSeq = binary.BigEndian.Uint64(pkt.Extras[8:16])
				event.SnapshotType = binary.BigEndian.Uint32(pkt.Extras[16:20])
				dcpStats.TotalSnapShot++
				sendAck = true

			case transport.DCP_FLUSH:
				if stream == nil {
					logging.Warnf("Stream not found for vb %d: %#v\n", vb, pkt)
					break loop
				}
				// special processing for flush ?
				event = makeDcpEvent(pkt, stream)

			case transport.DCP_CLOSESTREAM:
				if stream == nil {
					logging.Warnf("Stream not found for vb %d: %#v\n", vb, pkt)
					break loop
				}
				event = makeDcpEvent(pkt, stream)
				event.Opcode = transport.DCP_STREAMEND // opcode re-write !!
				logging.Warnf("Stream Closed for vb %d StreamEnd simulated\n", vb)
				sendAck = true

				feed.mu.Lock()
				delete(feed.vbstreams, vb)
				feed.mu.Unlock()

			case transport.DCP_ADDSTREAM:
				logging.Warnf("Opcode %v not implemented\n", pkt.Opcode)

			case transport.DCP_CONTROL, transport.DCP_BUFFERACK:
				if res.Status != transport.SUCCESS {
					logging.Warnf("Opcode %v received status %d\n", pkt.Opcode.String(), res.Status)
				}

			case transport.DCP_NOOP:
				// send a NOOP back
				noop := &transport.MCRequest{
					Opcode: transport.DCP_NOOP,
				}
				feed.transmitCh <- noop

			default:
				logging.Warnf("Received an unknown response for vbucket %d\n", vb)
			}

			// debug logging for DCP hiccups
			if event != nil && stream != nil {
				now := time.Now().UnixNano()
				if event.Opcode != transport.DCP_SNAPSHOT ||
					event.Opcode != transport.DCP_STREAMREQ {

					delta := (now - stream.LastSeen) / 1000000
					if stream.LastSeen != 0 && delta > 3000 {
						msg := "Warning: DCP event %v for vb %v after %v mS\n"
						logging.Warnf(msg, event.Opcode, stream.Vbucket, delta)
					}
				}
				stream.LastSeen = now
			}
		}

		if event != nil {
			select {
			case ch <- event:
			case <-feed.closer:
				break loop
			}

			feed.mu.RLock()
			l := len(feed.vbstreams)
			feed.mu.RUnlock()

			if event.Opcode == transport.DCP_CLOSESTREAM && l == 0 {
				logging.Warnf("No more streams")
				break loop
			}
		}

		needToSend, sendSize := feed.SendBufferAck(sendAck, uint32(bytes))
		if needToSend {
			bufferAck := &transport.MCRequest{
				Opcode: transport.DCP_BUFFERACK,
			}
			bufferAck.Extras = make([]byte, 4)
			logging.Warnf("Buffer-ack %v\n", sendSize)
			binary.BigEndian.PutUint32(bufferAck.Extras[:4], uint32(sendSize))
			feed.transmitCh <- bufferAck
			dcpStats.TotalBufferAckSent++
		}
	}

	feed.transmitCl <- true
}

// Send buffer ack
func (feed *DcpFeed) SendBufferAck(sendAck bool, bytes uint32) (bool, uint32) {
	if sendAck {
		totalBytes := feed.toAckBytes + bytes
		if totalBytes > feed.maxAckBytes {
			feed.toAckBytes = 0
			return true, totalBytes
		}
		feed.toAckBytes += bytes
	}
	return false, 0
}

func (feed *DcpFeed) GetDcpStats() *DcpStats {
	return &feed.stats
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

// Close this DcpFeed.
func (feed *DcpFeed) Close() {
	feed.mu.Lock()
	defer feed.mu.Unlock()

	if feed.conn != nil {
		close(feed.closer)
		feed.conn.Close()
		feed.conn = nil
	}
}
