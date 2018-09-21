package common

import (
	"errors"
	"github.com/couchbase/indexing/secondary/collatejson"
	"github.com/couchbase/indexing/secondary/logging"
	qvalue "github.com/couchbase/query/value"
)

var (
	ErrDecodeScanResult = errors.New("Error during decoding scan results")
	ErrSliceTooSmall    = errors.New("Scan entries slice is too small")
)

const (
	SECKEY_TMPBUF_MULTIPLIER = 6
	SECKEY_BUFSIZE           = 16 * 1024
)

var codec *collatejson.Codec

func init() {
	codec = collatejson.NewCodec(16)
}

// --------------------------
// ScanResultKey
// --------------------------
type ScanResultKey struct {
	Skey       SecondaryKey
	Skeycjson  []byte
	DataEncFmt DataEncodingFormat
}

func (k ScanResultKey) Get(buffer *[]byte,
	maxTempBufSize uint64) ([]qvalue.Value, error, *[]byte) {

	var err error
	var vals []qvalue.Value
	var retbuf *[]byte

	if k.DataEncFmt == DATA_ENC_COLLATEJSON {
		vals, err, retbuf = retryDecode(k.Skeycjson, buffer, maxTempBufSize)
		if err != nil {
			logging.Errorf("Error %v in DecodeN1QLValues", err)
			return nil, ErrDecodeScanResult, nil
		}

		// In case of empty array, missing literal can appear
		vals = FixMissingLiteral(vals)
	} else if k.DataEncFmt == DATA_ENC_JSON {
		vals = make([]qvalue.Value, len(k.Skey))
		for j := 0; j < len(k.Skey); j++ {
			if s, ok := k.Skey[j].(string); ok && collatejson.MissingLiteral.Equal(s) {
				vals[j] = qvalue.NewMissingValue()
			} else {
				vals[j] = qvalue.NewValue(k.Skey[j])
			}
		}
	} else {
		return nil, ErrUnexpectedDataEncFmt, nil
	}

	return vals, nil, retbuf
}

func (k ScanResultKey) GetRaw() (interface{}, error) {
	if k.DataEncFmt == DATA_ENC_COLLATEJSON {
		return k.Skeycjson, nil
	} else if k.DataEncFmt == DATA_ENC_JSON {
		return k.Skey, nil
	} else {
		return nil, ErrUnexpectedDataEncFmt
	}
}

// --------------------------
// ScanResultEntries
// --------------------------
type ScanResultEntries struct {
	Skeys      []ScanResultKey
	DataEncFmt DataEncodingFormat
	Ctr        int
	Length     int
}

func NewScanResultEntries(dataEncFmt DataEncodingFormat) *ScanResultEntries {
	s := &ScanResultEntries{
		DataEncFmt: dataEncFmt,
	}
	return s
}

func (s *ScanResultEntries) Make(length int) {
	s.Length = length
	s.Skeys = make([]ScanResultKey, length)
}

func (s *ScanResultEntries) Append(skey interface{}) (*ScanResultEntries, error) {
	if s.Ctr >= s.Length {
		skey := ScanResultKey{DataEncFmt: s.DataEncFmt}
		s.Skeys = append(s.Skeys, skey)
		s.Length = len(s.Skeys)
	}

	s.Skeys[s.Ctr].DataEncFmt = s.DataEncFmt
	if s.DataEncFmt == DATA_ENC_COLLATEJSON {
		var sk []byte
		if skey != nil {
			sk = skey.([]byte)
		}
		s.Skeys[s.Ctr].Skeycjson = sk
	} else if s.DataEncFmt == DATA_ENC_JSON {
		var sk SecondaryKey
		if skey != nil {
			sk = skey.(SecondaryKey)
		}
		s.Skeys[s.Ctr].Skey = sk
	} else {
		return nil, ErrUnexpectedDataEncFmt
	}

	s.Ctr++
	return s, nil
}

func (s *ScanResultEntries) Get(buf *[]byte,
	maxTempBufSize uint64) ([]qvalue.Values, error, *[]byte) {

	var retBuf *[]byte
	result := make([]qvalue.Values, len(s.Skeys))
	for i := 0; i < len(s.Skeys); i++ {
		r, err, rb := s.Skeys[i].Get(buf, maxTempBufSize)
		if err != nil {
			return result, err, nil
		}
		if rb != nil {
			buf = rb
			retBuf = rb
		}
		result[i] = r
	}
	return result, nil, retBuf
}

func (s *ScanResultEntries) Getkth(buf *[]byte, k int,
	maxTempBufSize uint64) (qvalue.Values, error, *[]byte) {

	return s.Skeys[k].Get(buf, maxTempBufSize)
}

func (s *ScanResultEntries) GetLength() int {
	if s == nil {
		return 0
	}
	return len(s.Skeys)
}

func (s *ScanResultEntries) GetkthKey(k int) ScanResultKey {
	return s.Skeys[k]
}

// --------------------------
// Utility functions
// --------------------------
func FixMissingLiteral(skey []qvalue.Value) []qvalue.Value {
	vals := make([]qvalue.Value, len(skey))
	for i := 0; i < len(skey); i++ {
		if skey[i].Type() == qvalue.STRING {
			as := skey[i].Actual()
			s := as.(string)
			if collatejson.MissingLiteral.Equal(s) {
				vals[i] = qvalue.NewMissingValue()
			} else {
				vals[i] = skey[i]
			}
		} else {
			vals[i] = skey[i]
		}
	}
	return vals
}

func retryDecode(key []byte, buf *[]byte,
	maxTempBufSize uint64) ([]qvalue.Value, error, *[]byte) {

	var bufRealloc bool
	var retBuf *[]byte

	buffer := *buf
	buffer = buffer[:0]

	if len(key)*SECKEY_TMPBUF_MULTIPLIER > cap(buffer) {
		buffer = make([]byte, 0, len(key)*SECKEY_TMPBUF_MULTIPLIER)
		bufRealloc = true
	}

	for {
		vals, err := codec.DecodeN1QLValues(key, buffer)
		if err == nil {
			if bufRealloc {
				retBuf = &buffer
			}
			return vals, nil, retBuf
		} else if err == collatejson.ErrorOutputLen {
			bufsz := cap(buffer)
			if bufsz > int(maxTempBufSize) {
				logging.Errorf("Buffer size %v insufficient. Giving up.", bufsz)
				return nil, err, nil
			}
			logging.Warnf("Buffer size %v insufficient. Retrying with larger size %v.", bufsz, bufsz*2)
			buffer = make([]byte, 0, bufsz*2)
			bufRealloc = true
		} else {
			return nil, err, nil
		}
	}
}
