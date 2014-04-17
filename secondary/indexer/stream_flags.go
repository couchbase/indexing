// flags:
//           +---------------+---------------+
//       byte|       0       |       1       |
//           +---------------+---------------+
//       bits|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
//           +-------+-------+---------------+
//          0| COMP. |  ENC. | undefined     |
//           +-------+-------+---------------+

package indexer

const ( // types of encoding over the wire.
	encodingProtobuf byte = 0
)

const ( // types of compression over the wire.
	compressionSnappy byte = 0
	compressionGzip        = 1
	compressionBzip2       = 2
)

type streamTransportFlag uint16

func (flags streamTransportFlag) getCompression() byte {
	return byte(flags & streamTransportFlag(0x000F))
}

func (flags streamTransportFlag) setSnappy() streamTransportFlag {
	return (flags & streamTransportFlag(0xFFF0)) | streamTransportFlag(compressionSnappy)
}

func (flags streamTransportFlag) setGzip() streamTransportFlag {
	return (flags & streamTransportFlag(0xFFF0)) | streamTransportFlag(compressionGzip)
}

func (flags streamTransportFlag) setBzip2() streamTransportFlag {
	return (flags & streamTransportFlag(0xFFF0)) | streamTransportFlag(compressionBzip2)
}

func (flags streamTransportFlag) getEncoding() byte {
	return byte(flags & streamTransportFlag(0x00F0))
}
func (flags streamTransportFlag) setProtobuf() streamTransportFlag {
	return (flags & streamTransportFlag(0xFF0F)) | streamTransportFlag(encodingProtobuf)
}
