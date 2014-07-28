// On the wire transport for mutation packet.
//  { uint32(packetlen), uint16(flags), []byte(mutation) }
//
// - `flags` used for specifying encoding format, compression etc.
// - packetlen == len(mutation)

package dataport

import (
	"encoding/binary"
	"errors"

	c "github.com/couchbase/indexing/secondary/common"
)

// error codes

// ErrorDuplicateClient
var ErrorDuplicateClient = errors.New("dataport.duplicateClient")

// ErrorPacketWrite
var ErrorPacketWrite = errors.New("dataport.packetWrite")

// ErrorPacketOverflow
var ErrorPacketOverflow = errors.New("dataport.packetOverflow")

// ErrorWorkerKilled
var ErrorWorkerKilled = errors.New("dataport.workerKilled")

// ErrorDaemonExit
var ErrorDaemonExit = errors.New("dataport.daemonExit")

// ErrorClientEmptyKeys
var ErrorClientEmptyKeys = errors.New("dataport.clientEmptyKeys")

// ErrorMissingPayload
var ErrorMissingPayload = errors.New("dataport.missingPlayload")

// ErrorTransportVersion
var ErrorTransportVersion = errors.New("dataport.transportVersion")

// ErrorDuplicateStreamBegin
var ErrorDuplicateStreamBegin = errors.New("dataport.duplicateStreamBegin")

// packet field offset and size in bytes
const (
	pktLenOffset  int = 0
	pktLenSize    int = 4
	pktFlagOffset int = pktLenOffset + pktLenSize
	pktFlagSize   int = 2
	pktDataOffset int = pktFlagOffset + pktFlagSize
)

// TransportPacket to send and receive mutation packets between router
// and downstream client.
type TransportPacket struct {
	flags TransportFlag
	buf   []byte
}

type transporter interface { // facilitates unit testing
	Read(b []byte) (n int, err error)
	Write(b []byte) (n int, err error)
}

// NewTransportPacket creates a new TransportPacket and return its
// reference. Typically application should call this once and reuse it while
// sending or receiving a sequence of packets, so that same buffer can be
// reused.
//
// maxlen, maximum size of internal buffer used to marshal and unmarshal
//         packets.
// flags,  specifying encoding and compression.
func NewTransportPacket(maxlen int, flags TransportFlag) *TransportPacket {
	return &TransportPacket{
		flags: flags,
		buf:   make([]byte, maxlen),
	}
}

// Send payload to the other end using sufficient encoding and compression.
func (pkt *TransportPacket) Send(conn transporter, payload interface{}) (err error) {
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
	if maxLen := c.MaxDataportPayload; l > maxLen {
		c.Errorf("sending packet length %v is > %v\n", l, maxLen)
		err = ErrorPacketOverflow
		return
	}

	a, b := pktLenOffset, pktLenOffset+pktLenSize
	binary.BigEndian.PutUint32(pkt.buf[a:b], uint32(len(data)))
	a, b = pktFlagOffset, pktFlagOffset+pktFlagSize
	binary.BigEndian.PutUint16(pkt.buf[a:b], uint16(pkt.flags))
	if n, err = conn.Write(pkt.buf[:pktDataOffset]); err == nil {
		if n, err = conn.Write(data); err == nil && n != len(data) {
			c.Errorf("dataport wrote only %v bytes for data\n", n)
			err = ErrorPacketWrite
		}
	} else if n != pktDataOffset {
		c.Errorf("dataport wrote only %v bytes for header\n", n)
		err = ErrorPacketWrite
	}
	return
}

// Receive payload from remote, decode, decompress the payload and return the
// payload
func (pkt *TransportPacket) Receive(conn transporter) (payload interface{}, err error) {
	var data []byte

	// transport de-framing
	if err = fullRead(conn, pkt.buf[:pktDataOffset]); err != nil {
		return
	}
	a, b := pktLenOffset, pktLenOffset+pktLenSize
	pktlen := binary.BigEndian.Uint32(pkt.buf[a:b])
	a, b = pktFlagOffset, pktFlagOffset+pktFlagSize
	pkt.flags = TransportFlag(binary.BigEndian.Uint16(pkt.buf[a:b]))
	if maxLen := uint32(c.MaxDataportPayload); pktlen > maxLen {
		c.Errorf("receiving packet length %v > %v\n", maxLen, pktlen)
		err = ErrorPacketOverflow
		return
	}
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
func (pkt *TransportPacket) encode(payload interface{}) (data []byte, err error) {
	switch pkt.flags.GetEncoding() {
	case encodingProtobuf:
		data, err = protobufEncode(payload)
	}
	return
}

// decode array of bytes back to payload.
func (pkt *TransportPacket) decode(data []byte) (payload interface{}, err error) {
	switch pkt.flags.GetEncoding() {
	case encodingProtobuf:
		payload, err = protobufDecode(data)
	}
	return
}

// compress array of bytes.
func (pkt *TransportPacket) compress(big []byte) (small []byte, err error) {
	switch pkt.flags.GetCompression() {
	case compressionNone:
		small = big
	}
	return
}

// decompress array of bytes.
func (pkt *TransportPacket) decompress(small []byte) (big []byte, err error) {
	switch pkt.flags.GetCompression() {
	case compressionNone:
		big = small
	}
	return
}

// read len(buf) bytes from `conn`.
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
