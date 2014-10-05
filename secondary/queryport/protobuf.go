// Protobuf encoding scheme for payload

package queryport

import (
	"errors"

	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/protobuf"
	"github.com/couchbaselabs/goprotobuf/proto"
)

// ErrorTransportVersion
var ErrorTransportVersion = errors.New("dataport.transportVersion")

// ErrorMissingPayload
var ErrorMissingPayload = errors.New("dataport.missingPlayload")

// protobufEncode encode payload message into protobuf array of bytes. Return
// `data` can be transported to the other end and decoded back to Payload
// message.
func protobufEncode(payload interface{}) (data []byte, err error) {
	pl := &protobuf.QueryPayload{
		Version: proto.Uint32(uint32(ProtobufVersion())),
	}
	switch val := payload.(type) {
	// request
	case *protobuf.StatisticsRequest:
		pl.StatisticsRequest = val

	case *protobuf.ScanRequest:
		pl.ScanRequest = val

	case *protobuf.ScanAllRequest:
		pl.ScanAllRequest = val

	case *protobuf.EndStreamRequest:
		pl.EndStream = val

	// response
	case *protobuf.StatisticsResponse:
		pl.Statistics = val

	case *protobuf.ResponseStream:
		pl.Stream = val

	case *protobuf.StreamEndResponse:
		pl.StreamEnd = val

	default:
		return nil, ErrorMissingPayload
	}

	data, err = proto.Marshal(pl)
	return
}

// protobufDecode complements protobufEncode() API. `data` returned by encode
// is converted back to protobuf message structure.
func protobufDecode(data []byte) (value interface{}, err error) {
	pl := &protobuf.QueryPayload{}
	if err = proto.Unmarshal(data, pl); err != nil {
		return nil, err
	}
	currVer := ProtobufVersion()
	if ver := byte(pl.GetVersion()); ver == currVer {
		// do nothing
	} else if ver > currVer {
		return nil, ErrorTransportVersion
	} else {
		pl = protoMsgConvertor[ver](pl)
	}

	// request
	if val := pl.GetStatisticsRequest(); val != nil {
		return val, nil
	} else if val := pl.GetScanRequest(); val != nil {
		return val, nil
	} else if val := pl.GetScanAllRequest(); val != nil {
		return val, nil
	} else if val := pl.GetEndStream(); val != nil {
		return val, nil
		// response
	} else if val := pl.GetStatistics(); val != nil {
		return val, nil
	} else if val := pl.GetStream(); val != nil {
		return val, nil
	} else if val := pl.GetStreamEnd(); val != nil {
		return val, nil
	}
	return nil, ErrorMissingPayload
}

// ProtobufVersion return version of protobuf schema used in packet transport.
func ProtobufVersion() byte {
	return (c.ProtobufDataPathMajorNum << 4) | c.ProtobufDataPathMinorNum
}

var protoMsgConvertor = map[byte]func(*protobuf.QueryPayload) *protobuf.QueryPayload{}
