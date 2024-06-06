package indexer

import (
	"encoding/binary"
	"fmt"

	"github.com/couchbase/indexing/secondary/collatejson"
	qvalue "github.com/couchbase/query/value"
)

// [VECTOR_TODO]: Since indexer does not know the position of vector, array has to be
// exploded and then later joined after replacing the actual centroidId. This might not
// be an optimal approach. Also, since the contents of the key changes, a new copy of
// the key has to be generated. With fixed length encoding, this can potentially be
// avoided where the position of the centroid can be replaced without making an extra
// copy of the key
//
// Possible approaches to investigate would be to send the vectorPos from projector to
// indexer at the time of encoding. Indexer can use this information to directly replace
// the centroidId in the incoming key. This requires fixed length encoding of centroidIds
func replaceDummyCentroidId(key []byte, vectorPos int, centroidId int64, buf []byte) ([]byte, error) {

	// Step-1: Encode the centroidId
	codec := collatejson.NewCodec(16)
	encodedCentroidIdBuf, err := codec.EncodeN1QLValue(qvalue.NewValue(centroidId), buf)
	if err != nil {
		return nil, err
	}
	encodedCentroidId := make([]byte, len(encodedCentroidIdBuf))
	copy(encodedCentroidId, encodedCentroidIdBuf)

	// Step-2: Decode the incoming key using "ExplodeArray4"
	buf = buf[:0] // reset buffer
	decodedValues, err := codec.ExplodeArray4(key, buf[:0])
	if err != nil {
		return nil, err
	}

	if len(decodedValues) < vectorPos {
		err := fmt.Errorf("The number of entries after exploding is less than vectorPos. "+
			"len(decodedValues): %v, vectorPos: %v", len(decodedValues), vectorPos)
		return nil, err
	}

	decodedValues[vectorPos] = encodedCentroidId

	// Step-3: Join array and return the new secondary key
	buf = buf[:0] // reset buffer
	newKeyBuf, err := codec.JoinArray(decodedValues, buf)
	if err != nil {
		return nil, err
	}

	// Step-4: Generate new key
	newKey := make([]byte, len(newKeyBuf))
	copy(newKey, newKeyBuf)
	return newKey, nil
}

func encodeQuantizedCodes(codes [][]byte, codeSize int) []byte {
	buf := make([]byte, (len(codes)*codeSize)+4)
	for i := range codes {
		copy(buf[i*codeSize:], codes[i])
	}

	binary.LittleEndian.PutUint32(buf[len(codes)*codeSize:], uint32(len(codes)))
	return buf
}
