package transport

import "io"
import "encoding/binary"
import "github.com/couchbase/indexing/secondary/logging"

func Send(conn transporter, buf []byte, flags TransportFlag, payload []byte, addChksm bool) (err error) {
	var n int

	// transport framing
	l := pktLenSize + pktFlagSize
	if maxLen := len(buf); l > maxLen {
		logging.Errorf("sending packet length %v > %v\n", l, maxLen)
		err = ErrorPacketOverflow
		return
	}

	a, b := pktLenOffset, pktLenOffset+pktLenSize
	binary.BigEndian.PutUint32(buf[a:b], uint32(len(payload)))

	if payload != nil && addChksm {
		chksm := computeChecksum(buf[a:b])
		flags = flags.SetChecksum(chksm)
	}

	a, b = pktFlagOffset, pktFlagOffset+pktFlagSize
	binary.BigEndian.PutUint16(buf[a:b], uint16(flags))
	if n, err = conn.Write(buf[:pktDataOffset]); err == nil {
		if n, err = conn.Write(payload); err == nil && n != len(payload) {
			logging.Errorf("transport wrote only %v bytes for payload\n", n)
			err = ErrorPacketWrite
		}
		laddr, raddr := conn.LocalAddr(), conn.RemoteAddr()
		logging.Tracef("wrote %v bytes on connection %v->%v", len(payload), laddr, raddr)

	} else if n != pktDataOffset {
		logging.Errorf("transport wrote only %v bytes for header\n", n)
		err = ErrorPacketWrite
	}
	return
}

func SendResponseEnd(conn transporter) error {
	buf := make([]byte, pktLenSize+pktFlagSize)
	// Special 0 byte payload and flag to indicate end of response
	return Send(conn, buf, 0, nil, false)
}

func Receive(conn transporter, buf []byte) (flags TransportFlag, payload []byte, err error) {
	// transport de-framing
	bufHeader := safeBufSlice(buf, pktDataOffset)
	if err = fullRead(conn, bufHeader); err != nil {
		if err == io.EOF {
			logging.Tracef("receiving packet: %v\n", err)
		} else {
			logging.Errorf("receiving packet: %v\n", err)
		}
		return
	}
	a, b := pktLenOffset, pktLenOffset+pktLenSize
	pktlen := binary.BigEndian.Uint32(bufHeader[a:b])

	bufLen := bufHeader[a:b]

	a, b = pktFlagOffset, pktFlagOffset+pktFlagSize
	flags = TransportFlag(binary.BigEndian.Uint16(bufHeader[a:b]))

	pktChksm := flags.GetChecksum()

	if uint8(pktChksm) != 0 {
		chksm := computeChecksum(bufLen)
		if chksm != pktChksm {
			logging.Errorf("checksum mismatch: expected %v got %v", pktChksm, chksm)
			logging.Errorf("packet header %#v, flags %#v", bufLen, bufHeader[a:b])
			err = ErrorChecksumMismatch
			return
		}
	}

	bufPkt := safeBufSlice(buf, int(pktlen))
	if err = fullRead(conn, bufPkt); err != nil {
		if err == io.EOF {
			logging.Tracef("receiving packet: %v\n", err)
		} else {
			logging.Errorf("receiving packet: %v\n", err)
		}
		return
	}

	return flags, bufPkt, err
}

func safeBufSlice(b []byte, l int) []byte {
	if cap(b) >= l {
		return b[:l]
	}

	return make([]byte, l)
}

//checksum is 7bits
func computeChecksum(pktLen []byte) byte {

	var checksum byte

	//use last 2 bytes from pktLen to
	//compute checksum
	checksum |= pktLen[3] & 0x0F
	checksum <<= 4
	checksum |= pktLen[2] & 0x0F
	checksum &= 0x7F

	return checksum
}
