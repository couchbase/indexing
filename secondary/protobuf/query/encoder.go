package protoQuery

import "github.com/couchbase/indexing/secondary/transport"
import "net"

const (
	LenOffset  int = 0
	LenSize    int = 4
	FlagOffset int = LenOffset + LenSize
	FlagSize   int = 2
	DataOffset int = FlagOffset + FlagSize
)

func EncodeAndWrite(conn net.Conn, buf []byte, r interface{}) (err error) {
	var data []byte
	data, err = ProtobufEncodeInBuf(r, buf[transport.MaxSendBufSize:][:0])
	if err != nil {
		return
	}
	flags := transport.TransportFlag(0).SetProtobuf()
	err = transport.Send(conn, buf, flags, data, false)
	return
}
