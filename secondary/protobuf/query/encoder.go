package protobuf

import "io"
import "fmt"
import "encoding/binary"
import "github.com/couchbase/indexing/secondary/transport"

const (
	LenOffset  int = 0
	LenSize    int = 4
	FlagOffset int = LenOffset + LenSize
	FlagSize   int = 2
	DataOffset int = FlagOffset + FlagSize
)

func EncodeAndWrite(w io.Writer, buf []byte, r interface{}) (err error) {
	var n int
	data, err := ProtobufEncode(r)
	l := LenSize + FlagSize + len(data)
	if maxLen := len(buf); l > maxLen {
		err = fmt.Errorf("buffer full")
		return
	}

	flags := transport.TransportFlag(0).SetProtobuf()

	a, b := LenOffset, LenOffset+LenSize
	binary.BigEndian.PutUint32(buf[a:b], uint32(len(data)))
	a, b = FlagOffset, FlagOffset+FlagSize
	binary.BigEndian.PutUint16(buf[a:b], uint16(flags))
	if n, err = w.Write(buf[:DataOffset]); err == nil {
		if n, err = w.Write(data); err == nil && n != len(data) {
			err = fmt.Errorf("paritial write of %d bytes", n)
		}

	} else if n != DataOffset {
		err = fmt.Errorf("paritial write of %d bytes for header", n)
	}

	return
}
