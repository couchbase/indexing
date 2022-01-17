// Protobuf encoding scheme for payload

package protoQuery

import (
	"errors"

	c "github.com/couchbase/indexing/secondary/common"
	"github.com/golang/protobuf/proto"
)

// ErrorTransportVersion
var ErrorTransportVersion = errors.New("queryport.transportVersion")

// ErrorMissingPayload
var ErrorMissingPayload = errors.New("queryport.missingPlayload")

// ProtobufEncode encode payload message into protobuf array of bytes. Return
// `data` can be transported to the other end and decoded back to Payload
// message.
func ProtobufEncode(payload interface{}) (data []byte, err error) {
	return ProtobufEncodeInBuf(payload, nil)
}

func ProtobufEncodeInBuf(payload interface{}, buf []byte) (data []byte, err error) {
	pl := &QueryPayload{Version: proto.Uint32(uint32(ProtobufVersion()))}
	switch val := payload.(type) {
	// request
	case *StatisticsRequest:
		pl.StatisticsRequest = val

	case *CountRequest:
		pl.CountRequest = val

	case *ScanRequest:
		pl.ScanRequest = val

	case *ScanAllRequest:
		pl.ScanAllRequest = val

	case *EndStreamRequest:
		pl.EndStream = val

	case *AuthRequest:
		pl.AuthRequest = val

	// response
	case *StatisticsResponse:
		pl.Statistics = val

	case *CountResponse:
		pl.CountResponse = val

	case *ResponseStream:
		pl.Stream = val

	case *StreamEndResponse:
		pl.StreamEnd = val

	case *HeloRequest:
		pl.HeloRequest = val

	case *HeloResponse:
		pl.HeloResponse = val

	case *AuthResponse:
		pl.AuthResponse = val

	default:
		return nil, ErrorMissingPayload
	}

	p := proto.NewBuffer(buf)
	err = p.Marshal(pl)
	data = p.Bytes()
	return
}

// ProtobufDecode complements ProtobufEncode() API. `data` returned by encode
// is converted back to protobuf message structure.
func ProtobufDecode(data []byte) (value interface{}, err error) {
	pl := &QueryPayload{}
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
	} else if val := pl.GetCountRequest(); val != nil {
		return val, nil
	} else if val := pl.GetScanRequest(); val != nil {
		return val, nil
	} else if val := pl.GetScanAllRequest(); val != nil {
		return val, nil
	} else if val := pl.GetEndStream(); val != nil {
		return val, nil
	} else if val := pl.GetAuthRequest(); val != nil {
		return val, nil
		// response
	} else if val := pl.GetStatistics(); val != nil {
		return val, nil
	} else if val := pl.GetStream(); val != nil {
		return val, nil
	} else if val := pl.GetCountResponse(); val != nil {
		return val, nil
	} else if val := pl.GetStreamEnd(); val != nil {
		return val, nil
	} else if val := pl.GetHeloRequest(); val != nil {
		return val, nil
	} else if val := pl.GetHeloResponse(); val != nil {
		return val, nil
	} else if val := pl.GetAuthResponse(); val != nil {
		return val, nil
	}
	return nil, ErrorMissingPayload
}

// ProtobufVersion return version of protobuf schema used in packet transport.
func ProtobufVersion() byte {
	return (c.ProtobufDataPathMajorNum << 4) | c.ProtobufDataPathMinorNum
}

var protoMsgConvertor = map[byte]func(*QueryPayload) *QueryPayload{}
