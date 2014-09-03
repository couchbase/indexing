// flags:
//           +---------------+---------------+
//       byte|       0       |       1       |
//           +---------------+---------------+
//       bits|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
//           +-------+-------+---------------+  COMP. - Compression
//          0| COMP. |  ENC. | undefined     |  ENC.  - Encoding
//           +-------+-------+---------------+
package transport

const ( // types of encoding over the wire.
	EncodingNone     byte = 0x00
	EncodingProtobuf byte = 0x10
)

const ( // types of compression over the wire.
	CompressionNone   byte = 0
	CompressionSnappy      = 1
	CompressionGzip        = 2
	CompressionBzip2       = 3
)

// TransportFlag tell packet encoding and compression formats.
type TransportFlag uint16

// GetCompression returns the compression bits from flags
func (flags TransportFlag) GetCompression() byte {
	return byte(flags & TransportFlag(0x000F))
}

// SetSnappy will set packet compression to snappy
func (flags TransportFlag) SetSnappy() TransportFlag {
	return (flags & TransportFlag(0xFFF0)) | TransportFlag(CompressionSnappy)
}

// SetGzip will set packet compression to Gzip
func (flags TransportFlag) SetGzip() TransportFlag {
	return (flags & TransportFlag(0xFFF0)) | TransportFlag(CompressionGzip)
}

// SetBzip2 will set packet compression to bzip2
func (flags TransportFlag) SetBzip2() TransportFlag {
	return (flags & TransportFlag(0xFFF0)) | TransportFlag(CompressionBzip2)
}

// GetEncoding will get the encoding bits from flags
func (flags TransportFlag) GetEncoding() byte {
	return byte(flags & TransportFlag(0x00F0))
}

// SetProtobuf will set packet encoding to protobuf
func (flags TransportFlag) SetProtobuf() TransportFlag {
	return (flags & TransportFlag(0xFF0F)) | TransportFlag(EncodingProtobuf)
}
