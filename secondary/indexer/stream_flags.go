// flags:
//           +---------------+---------------+
//       byte|       0       |       1       |
//           +---------------+---------------+
//       bits|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
//           +-------+-------+---------------+  COMP. - Compression
//          0| COMP. |  ENC. | undefined     |  ENC.  - Encoding
//           +-------+-------+---------------+

package indexer

const ( // types of encoding over the wire.
	encodingProtobuf byte = 0
)

const ( // types of compression over the wire.
	compressionNone   byte = 0
	compressionSnappy      = 1
	compressionGzip        = 2
	compressionBzip2       = 3
)

// StreamTransportFlag tell packet encoding and compression formats.
type StreamTransportFlag uint16

// GetCompression returns the compression bits from flags
func (flags StreamTransportFlag) GetCompression() byte {
	return byte(flags & StreamTransportFlag(0x000F))
}

// SetSnappy will set packet compression to snappy
func (flags StreamTransportFlag) SetSnappy() StreamTransportFlag {
	return (flags & StreamTransportFlag(0xFFF0)) | StreamTransportFlag(compressionSnappy)
}

// SetGzip will set packet compression to Gzip
func (flags StreamTransportFlag) SetGzip() StreamTransportFlag {
	return (flags & StreamTransportFlag(0xFFF0)) | StreamTransportFlag(compressionGzip)
}

// SetBzip2 will set packet compression to bzip2
func (flags StreamTransportFlag) SetBzip2() StreamTransportFlag {
	return (flags & StreamTransportFlag(0xFFF0)) | StreamTransportFlag(compressionBzip2)
}

// GetEncoding will get the encoding bits from flags
func (flags StreamTransportFlag) GetEncoding() byte {
	return byte(flags & StreamTransportFlag(0x00F0))
}

// SetProtobuf will set packet encoding to protobuf
func (flags StreamTransportFlag) SetProtobuf() StreamTransportFlag {
	return (flags & StreamTransportFlag(0xFF0F)) | StreamTransportFlag(encodingProtobuf)
}
