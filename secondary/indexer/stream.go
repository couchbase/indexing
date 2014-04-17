// On the wire transport library a mutation packet.
//  { uint32(packetlen), uint16(flags), []byte(mutation) }
//
// * `flags` used for specifying encoding format, compression etc.
// * packetlen == len(mutation)

package indexer

import (
	"encoding/binary"
	"fmt"
)

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
	flags streamTransportFlag
	buf   []byte
}

type transporter interface { // facilitates unit testing
	Read(b []byte) (n int, err error)
	Write(b []byte) (n int, err error)
}

// NewStreamTransportPacket creates a new StreamTransportPacket and return its
// reference. Typically application should call this once and reuse it for while
// sending or receiving a sequence of packets, so that same buffer can be
// reused.
//
// maxlen, maximum size of internal buffer used to marshal and unmarshal
//         packets.
// flags,  specifying encoding and compression.
func NewStreamTransportPacket(maxlen int, flags streamTransportFlag) *StreamTransportPacket {
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
	if maxLen := GetMaxStreamDataLen(); l > maxLen {
		err = fmt.Errorf("packet length is greater than %v", maxLen)
		return
	}

	a, b := pktLenOffset, pktLenOffset+pktLenSize
	binary.BigEndian.PutUint32(pkt.buf[a:b], uint32(len(data)))
	a, b = pktFlagOffset, pktFlagOffset+pktFlagSize
	binary.BigEndian.PutUint16(pkt.buf[a:b], uint16(pkt.flags))
	if n, err = conn.Write(pkt.buf[:pktDataOffset]); err == nil {
		if n, err = conn.Write(data); err == nil && n != len(data) {
			err = fmt.Errorf("stream packet wrote only %v bytes for data", n)
		}
	} else if n != pktDataOffset {
		err = fmt.Errorf("stream packet wrote only %v bytes for header", n)
	}
	return
}

// Receive payload from remote, decode, decompress the payload and return the
// payload
func (pkt *StreamTransportPacket) Receive(conn transporter) (payload interface{}, err error) {
	var data []byte
	var n int

	// transport de-framing
	if n, err = conn.Read(pkt.buf[:pktDataOffset]); err != nil {
		return
	} else if n != pktDataOffset {
		err = fmt.Errorf("read only %v bytes for packet header", n)
		return
	}
	a, b := pktLenOffset, pktLenOffset+pktLenSize
	pktlen := binary.BigEndian.Uint32(pkt.buf[a:b])
	a, b = pktFlagOffset, pktFlagOffset+pktFlagSize
	pkt.flags = streamTransportFlag(binary.BigEndian.Uint16(pkt.buf[a:b]))
	if maxLen := uint32(GetMaxStreamDataLen()); pktlen > maxLen {
		err = fmt.Errorf("packet length is greater than %v", maxLen)
		return
	}
	if n, err = conn.Read(pkt.buf[:pktlen]); err != nil {
		return
	} else if n != int(pktlen) {
		err = fmt.Errorf("read only %v bytes for packet data", n)
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
	switch pkt.flags.getEncoding() {
	case encodingProtobuf:
		data, err = protobufEncode(payload)
	}
	return
}

// decode array of bytes back to payload.
func (pkt *StreamTransportPacket) decode(data []byte) (payload interface{}, err error) {
	switch pkt.flags.getEncoding() {
	case encodingProtobuf:
		payload, err = protobufDecode(data)
	}
	return
}

// compress array of bytes.
func (pkt *StreamTransportPacket) compress(big []byte) (small []byte, err error) {
	small = big
	switch pkt.flags.getCompression() {
	}
	return
}

// decompress array of bytes.
func (pkt *StreamTransportPacket) decompress(small []byte) (big []byte, err error) {
	big = small
	switch pkt.flags.getCompression() {
	}
	return
}

// TODO: move this to configuration provider
func GetMaxStreamDataLen() int {
	return 10 * 1024
}

// TODO: move this to configuration provider
func GetMaxVbuckets() uint16 {
	return 4 // TODO: Make it 1024
}
