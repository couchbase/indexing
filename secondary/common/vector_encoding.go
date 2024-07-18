package common

import (
	"fmt"
	"strconv"

	"github.com/couchbase/indexing/secondary/collatejson"
	"github.com/couchbase/indexing/secondary/logging"
	qvalue "github.com/couchbase/query/value"
)

var DUMMY_CENTROID string = "-0000001"

func GenDummyCentroidId() string {
	return DUMMY_CENTROID
}

func GetCentroidIdStr(centroidId int64) string {
	return fmt.Sprintf("%08x", centroidId)
}

func EncodedCentroidId(centroidId int64, encodeBuf []byte) ([]byte, error) {
	centroidStr := GetCentroidIdStr(centroidId)
	codec := collatejson.NewCodec(16)
	encoded, err := codec.EncodeN1QLValue(qvalue.NewValue(centroidStr), encodeBuf)
	return encoded, err
}

func EncodedCentroidIds(centroidIds []int64, encodeBuf []byte) ([]byte, error) {
	centroidBytes := make([]interface{}, len(centroidIds))
	for i := range centroidIds {
		centroidBytes[i] = fmt.Sprintf("%08x", centroidIds[i])
	}
	codec := collatejson.NewCodec(16)
	encoded, err := codec.EncodeN1QLValue(qvalue.NewValue(centroidBytes), encodeBuf)
	return encoded, err
}

func DecodeCentroidId(code []byte, buf []byte) (int64, []byte, error) {
	var err error

	switch code[0] {
	case collatejson.TypeString:
		var val qvalue.Value
		var result int64
		val, err = codec.DecodeN1QLValue(code, buf)
		if err != nil {
			if err == collatejson.ErrorOutputLen {
				buf = make([]byte, 0, len(code)*3)
				val, err = codec.DecodeN1QLValue(code, buf)
			}
			if err != nil {
				logging.Errorf("Error %v in DecodeCentroidId", err)
				return 0, buf, ErrDecodeScanResult
			}
		}
		result, err = strconv.ParseInt(val.Actual().(string), 16, 64)
		return result, buf, err

	case collatejson.TypeArray:
		// [VECTOR_TODO]: Add support for array of centroidIds
	default:
		panic("Invalid type")
	}
	return -1, buf, nil
}
