package common

import (
	"testing"
)

func TestVectorEncoding(t *testing.T) {
	//dummy := GenDummyCentroidId()
	encodeBuf := make([]byte, 0, 100)
	val, err := EncodedCentroidId(1234, encodeBuf[:0])
	if err != nil {
		t.Fatalf("Error observed during encoding, err: %v", err)
	}

	decVal, _, err := DecodeCentroidId(val, encodeBuf[:])
	if err != nil {
		t.Fatalf("Error observed during decoding, err: %v", err)
	}
	if decVal != 1234 {
		t.Fatalf("Encoded and decoded values are not same. EncodedVal: %v, decodedVal: %v", 1234, decVal)
	}

	val, err = EncodedCentroidIds([]int64{1234, -1, 4567}, encodeBuf[:0])
	if err != nil {
		t.Fatalf("Error observed during encoding, err: %v", err)
	}

	DecodeCentroidId(val, encodeBuf[:]) // Does not work now - until we add support for array of vectors
}
