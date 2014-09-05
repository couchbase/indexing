// On the wire transport for custom packets packet.
//
//      { uint32(packetlen), uint16(flags), []byte(mutation) }
//
//      where, packetlen == len(mutation)
//
// `flags` used for specifying encoding format, compression etc.
package transport

import (
	"encoding/binary"
	"errors"
	"net"

	c "github.com/couchbase/indexing/secondary/common"
)

// error codes

// ErrorPacketWrite is error writing packet on the wire.
var ErrorPacketWrite = errors.New("transport.packetWrite")

// ErrorPacketOverflow is input packet overflows maximum configured packet size.
var ErrorPacketOverflow = errors.New("transport.packetOverflow")

// ErrorEncoderUnknown for unknown encoder.
var ErrorEncoderUnknown = errors.New("transport.encoderUnknown")

// ErrorDecoderUnknown for unknown decoder.
var ErrorDecoderUnknown = errors.New("transport.decoderUnknown")

// packet field offset and size in bytes
const (
	pktLenOffset  int = 0
	pktLenSize    int = 4
	pktFlagOffset int = pktLenOffset + pktLenSize
	pktFlagSize   int = 2
	pktDataOffset int = pktFlagOffset + pktFlagSize
)

type transporter interface { // facilitates unit testing
	Read(b []byte) (n int, err error)
	Write(b []byte) (n int, err error)
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
}

// TransportPacket to send and receive mutation packets between router
// and downstream client.
type TransportPacket struct {
	flags    TransportFlag
	buf      []byte
	encoders map[byte]Encoder
	decoders map[byte]Decoder
}

type Encoder func(payload interface{}) (data []byte, err error)

type Decoder func(data []byte) (payload interface{}, err error)

// NewTransportPacket creates a new TransportPacket and return its
// reference. Typically application should call this once and reuse it while
// sending or receiving a sequence of packets, so that same buffer can be
// reused.
//
// maxlen, maximum size of internal buffer used to marshal and unmarshal
//         packets.
// flags,  specifying encoding and compression.
func NewTransportPacket(maxlen int, flags TransportFlag) *TransportPacket {
	pkt := &TransportPacket{
		flags:    flags,
		buf:      make([]byte, maxlen),
		encoders: make(map[byte]Encoder),
		decoders: make(map[byte]Decoder),
	}
	pkt.encoders[EncodingNone] = nil
	pkt.decoders[EncodingNone] = nil
	return pkt
}

// SetEncoder callback function for `type`.
func (pkt *TransportPacket) SetEncoder(typ byte, callb Encoder) *TransportPacket {
	pkt.encoders[typ] = callb
	return pkt
}

// SetDecoder callback function for `type`.
func (pkt *TransportPacket) SetDecoder(typ byte, callb Decoder) *TransportPacket {
	pkt.decoders[typ] = callb
	return pkt
}

// Send payload to the other end using sufficient encoding and compression.
func (pkt *TransportPacket) Send(conn transporter, payload interface{}) (err error) {
	var data []byte
	var n int

	// encode
	if data, err = pkt.encode(payload); err != nil {
		return
	}
	// compress
	if data, err = pkt.compress(data); err != nil {
		return
	}
	// transport framing
	l := pktLenSize + pktFlagSize + len(data)
	if maxLen := len(pkt.buf); l > maxLen {
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
			c.Errorf("transport wrote only %v bytes for data\n", n)
			err = ErrorPacketWrite
		}
		laddr, raddr := conn.LocalAddr(), conn.RemoteAddr()
		c.Tracef("wrote %v bytes on connection %v->%v", len(data), laddr, raddr)

	} else if n != pktDataOffset {
		c.Errorf("transport wrote only %v bytes for header\n", n)
		err = ErrorPacketWrite
	}
	return
}

// Receive payload from remote, decode, decompress the payload and return the
// payload.
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
	if maxLen := uint32(len(pkt.buf)); pktlen > maxLen {
		c.Errorf("receiving packet length %v > %v\n", maxLen, pktlen)
		err = ErrorPacketOverflow
		return
	}
	if err = fullRead(conn, pkt.buf[:pktlen]); err != nil {
		return
	}

	data = pkt.buf[:pktlen]
	laddr, raddr := conn.LocalAddr(), conn.RemoteAddr()
	c.Tracef("read %v bytes on connection %v<-%v", len(data), laddr, raddr)

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

// encode payload to array of bytes, if callback was specified `nil` for a
// valid type then return `payload` as `data`.
func (pkt *TransportPacket) encode(payload interface{}) (data []byte, err error) {
	typ := pkt.flags.GetEncoding()
	if callb, ok := pkt.encoders[typ]; ok {
		return callb(payload)
	} else if callb == nil {
		return payload.([]byte), nil
	}
	return nil, ErrorEncoderUnknown
}

// decode array of bytes back to payload, if callback was specified `nil` for
// a valid type then return `data` as `payload`.
func (pkt *TransportPacket) decode(data []byte) (payload interface{}, err error) {
	typ := pkt.flags.GetEncoding()
	if callb, ok := pkt.decoders[typ]; ok {
		return callb(data)
	} else if callb == nil {
		return data, nil
	}
	return nil, ErrorDecoderUnknown
}

// compress array of bytes.
func (pkt *TransportPacket) compress(big []byte) (small []byte, err error) {
	switch pkt.flags.GetCompression() {
	case CompressionNone:
		small = big
	}
	return
}

// decompress array of bytes.
func (pkt *TransportPacket) decompress(small []byte) (big []byte, err error) {
	switch pkt.flags.GetCompression() {
	case CompressionNone:
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
