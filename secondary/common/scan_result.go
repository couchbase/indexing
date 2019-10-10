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
	SECKEY_TEMP_BUF_PADDING = 32
	TEMP_BUF_SIZE           = 4096
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

// This function returns decoded n1ql value, error (if any) and retbuf.
// retbuf is nil, if temporary buffer passed as input to Get() was sufficient
// in size. If the input buffer was insufficient, new buffer is allocated
// and returned to the caller - for reuse.
func (k ScanResultKey) Get(buffer *[]byte) ([]qvalue.Value, error, *[]byte) {

	var err error
	var vals []qvalue.Value
	var retbuf *[]byte

	if k.DataEncFmt == DATA_ENC_COLLATEJSON {
		buf := *buffer
		buf = buf[:0]
		if len(k.Skeycjson)+SECKEY_TEMP_BUF_PADDING > cap(buf) {
			buf = make([]byte, 0, len(k.Skeycjson)+SECKEY_TEMP_BUF_PADDING)
			retbuf = &buf
		}

		vals, err = codec.DecodeN1QLValues(k.Skeycjson, buf)
		if err != nil {
			if err == collatejson.ErrorOutputLen {
				buf = make([]byte, 0, len(k.Skeycjson)*3)
				retbuf = &buf
				vals, err = codec.DecodeN1QLValues(k.Skeycjson, buf)
			}
			if err != nil {
				logging.Errorf("Error %v in DecodeN1QLValues", err)
				return nil, ErrDecodeScanResult, nil
			}
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
}

func NewScanResultEntries(dataEncFmt DataEncodingFormat) *ScanResultEntries {
	s := &ScanResultEntries{
		DataEncFmt: dataEncFmt,
	}
	return s
}

func (s *ScanResultEntries) Make(length int) {
	s.Skeys = make([]ScanResultKey, length)
}

func (s *ScanResultEntries) Append(skey interface{}) (*ScanResultEntries, error) {
	if s.Ctr >= len(s.Skeys) {
		skey := ScanResultKey{DataEncFmt: s.DataEncFmt}
		s.Skeys = append(s.Skeys, skey)
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

func (s *ScanResultEntries) Get(buf *[]byte) ([]qvalue.Values, error, *[]byte) {

	var retBuf *[]byte
	result := make([]qvalue.Values, s.Ctr)
	for i := 0; i < s.Ctr; i++ {
		r, err, rb := s.Skeys[i].Get(buf)
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

func (s *ScanResultEntries) Getkth(buf *[]byte, k int) (qvalue.Values, error, *[]byte) {
	if k >= s.Ctr || k >= len(s.Skeys) {
		return nil, ErrSliceTooSmall, nil
	}

	return s.Skeys[k].Get(buf)
}

func (s *ScanResultEntries) GetLength() int {
	if s == nil {
		return 0
	}
	return s.Ctr
}

func (s *ScanResultEntries) GetkthKey(k int) (ScanResultKey, error) {
	if k >= s.Ctr || k >= len(s.Skeys) {
		return ScanResultKey{}, ErrSliceTooSmall
	}

	return s.Skeys[k], nil
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
