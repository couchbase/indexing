// On the wire transport for mutation packet.
//  { uint32(packetlen), uint16(flags), []byte(mutation) }
//
// - `flags` used for specifying encoding format, compression etc.
// - packetlen == len(mutation)

package indexer

import (
	"encoding/binary"
	"fmt"
	c "github.com/couchbase/indexing/secondary/common"
	"log"
)

// error codes

// ErrorDuplicateClient
var ErrorDuplicateClient = fmt.Errorf("errorDuplicateClient")

// ErrorStreamPacketRead
var ErrorStreamPacketRead = fmt.Errorf("errorStreamPacketRead")

// ErrorStreamPacketWrite
var ErrorStreamPacketWrite = fmt.Errorf("errorStreamPacketWrite")

// ErrorStreamPacketOverflow
var ErrorStreamPacketOverflow = fmt.Errorf("errorStreamPacketOverflow")

// ErrorStreamdWorkerKilled
var ErrorStreamdWorkerKilled = fmt.Errorf("errorStreamdWorkerKilled")

// ErrorStreamdExit
var ErrorStreamdExit = fmt.Errorf("errorStreamdExit")

// ErrorStreamcEmptyKeys
var ErrorStreamcEmptyKeys = fmt.Errorf("errorStreamcEmptyKeys")

// ErrorMissingPayload
var ErrorMissingPayload = fmt.Errorf("errorMissingPlayload")

// ErrorTransportVersion
var ErrorTransportVersion = fmt.Errorf("errorTransportVersion")

// ErrorDuplicateStreamBegin
var ErrorDuplicateStreamBegin = fmt.Errorf("errorDuplicateStreamBegin")

// packet field offset and size in bytes
const (
	pktLenOffset  int = 0
	pktLenSize    int = 4
	pktFlagOffset int = pktLenOffset + pktLenSize
	pktFlagSize   int = 2
	pktDataOffset int = pktFlagOffset + pktFlagSize
)

// StreamTransportPacket to send and receive mutation packets between router
// and indexer.
type StreamTransportPacket struct {
	flags StreamTransportFlag
	buf   []byte
}

type transporter interface { // facilitates unit testing
	Read(b []byte) (n int, err error)
	Write(b []byte) (n int, err error)
}

// NewStreamTransportPacket creates a new StreamTransportPacket and return its
// reference. Typically application should call this once and reuse it while
// sending or receiving a sequence of packets, so that same buffer can be
// reused.
//
// maxlen, maximum size of internal buffer used to marshal and unmarshal
//         packets.
// flags,  specifying encoding and compression.
func NewStreamTransportPacket(maxlen int, flags StreamTransportFlag) *StreamTransportPacket {
	return &StreamTransportPacket{
		flags: flags,
		buf:   make([]byte, maxlen),
	}
}

// Send payload to the other end using sufficient encoding and compression.
func (pkt *StreamTransportPacket) Send(conn transporter, payload interface{}) (err error) {
	var data []byte
	var n int

	// encoding
	if data, err = pkt.encode(payload); err != nil {
		return
	}
	// compression
	if data, err = pkt.compress(data); err != nil {
		return
	}
	// transport framing
	l := pktLenSize + pktFlagSize + len(data)
	if maxLen := c.MaxStreamDataLen; l > maxLen {
		log.Printf("sending packet length %v is > %v\n", l, maxLen)
		err = ErrorStreamPacketOverflow
		return
	}

	a, b := pktLenOffset, pktLenOffset+pktLenSize
	binary.BigEndian.PutUint32(pkt.buf[a:b], uint32(len(data)))
	a, b = pktFlagOffset, pktFlagOffset+pktFlagSize
	binary.BigEndian.PutUint16(pkt.buf[a:b], uint16(pkt.flags))
	//fmt.Println("write", pkt.buf[:pktDataOffset])
	if n, err = conn.Write(pkt.buf[:pktDataOffset]); err == nil {
		//fmt.Println("write", data)
		if n, err = conn.Write(data); err == nil && n != len(data) {
			log.Printf("stream packet wrote only %v bytes for data\n", n)
			err = ErrorStreamPacketWrite
		}
	} else if n != pktDataOffset {
		log.Printf("stream packet wrote only %v bytes for header\n", n)
		err = ErrorStreamPacketWrite
	}
	return
}

func fullRead(conn transporter, buf []byte) error {
	size, start := 0, 0
	for size < len(buf) {
		n, err := conn.Read(buf[start:])
		if err != nil {
			return err
		}
		size += n
		start += n
	}
	return nil
}

// Receive payload from remote, decode, decompress the payload and return the
// payload
func (pkt *StreamTransportPacket) Receive(conn transporter) (payload interface{}, err error) {
	var data []byte

	// transport de-framing
	//fmt.Println("read", pkt.buf[:pktDataOffset])
	if err = fullRead(conn, pkt.buf[:pktDataOffset]); err != nil {
		return
	}
	a, b := pktLenOffset, pktLenOffset+pktLenSize
	pktlen := binary.BigEndian.Uint32(pkt.buf[a:b])
	a, b = pktFlagOffset, pktFlagOffset+pktFlagSize
	pkt.flags = StreamTransportFlag(binary.BigEndian.Uint16(pkt.buf[a:b]))
	if maxLen := uint32(c.MaxStreamDataLen); pktlen > maxLen {
		log.Printf("receiving packet length %v > %v\n", maxLen, pktlen)
		err = ErrorStreamPacketOverflow
		return
	}
	//fmt.Println("read", pkt.buf[:pktlen])
	if err = fullRead(conn, pkt.buf[:pktlen]); err != nil {
		return
	}
	data = pkt.buf[:pktlen]
	// de-compression
	if data, err = pkt.decompress(data); err != nil {
		return
	}
	// decoding
	if payload, err = pkt.decode(data); err != nil {
		return
	}
	return
}

// encode payload to array of bytes.
func (pkt *StreamTransportPacket) encode(payload interface{}) (data []byte, err error) {
	switch pkt.flags.GetEncoding() {
	case encodingProtobuf:
		data, err = protobufEncode(payload)
	}
	return
}

// decode array of bytes back to payload.
func (pkt *StreamTransportPacket) decode(data []byte) (payload interface{}, err error) {
	switch pkt.flags.GetEncoding() {
	case encodingProtobuf:
		payload, err = protobufDecode(data)
	}
	return
}

// compress array of bytes.
func (pkt *StreamTransportPacket) compress(big []byte) (small []byte, err error) {
	switch pkt.flags.GetCompression() {
	case compressionNone:
		small = big
	}
	return
}

// decompress array of bytes.
func (pkt *StreamTransportPacket) decompress(small []byte) (big []byte, err error) {
	switch pkt.flags.GetCompression() {
	case compressionNone:
		big = small
	}
	return
}
