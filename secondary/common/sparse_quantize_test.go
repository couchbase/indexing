// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package common

import (
	"math"
	"testing"
)

// mkConcise is defined in sparse_truncate_test.go (same package).

func TestEncodeQuantizedSparse_HeaderAndSize(t *testing.T) {
	in := mkConcise([]uint32{3, 9, 21}, []float32{1.0, 0.5, 2.0})
	enc, err := EncodeQuantizedSparse(in, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	q := QuantizedSparseVector(enc)
	if q.NNZ() != 3 {
		t.Fatalf("NNZ: want 3 got %d", q.NNZ())
	}
	if got, want := len(enc), quantSparseHeaderSize+3*3; got != want {
		t.Fatalf("len: want %d got %d", want, got)
	}
	if q.Size() != len(enc) {
		t.Fatalf("Size %d != len %d", q.Size(), len(enc))
	}
	// max value 2.0 -> scale 2.0/255
	wantScale := float32(2.0 / 255.0)
	if math.Abs(float64(q.Scale()-wantScale)) > 1e-9 {
		t.Fatalf("scale: want %v got %v", wantScale, q.Scale())
	}
	// indices preserved, ascending
	for i, want := range []uint16{3, 9, 21} {
		if q.IndexAt(i) != want {
			t.Fatalf("idx[%d]: want %d got %d", i, want, q.IndexAt(i))
		}
	}
	// peak weight maps to code 255
	if q.CodeAt(2) != 255 {
		t.Fatalf("peak code: want 255 got %d", q.CodeAt(2))
	}
}

func TestQuantizedSparse_Roundtrip(t *testing.T) {
	in := mkConcise(
		[]uint32{1, 4, 9, 100, 65535},
		[]float32{0.10, 4.00, 0.55, 2.25, 1.00},
	)
	enc, err := EncodeQuantizedSparse(in, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out := make([]float32, 1+2*5)
	n, err := DecodeQuantizedSparse(QuantizedSparseVector(enc), out)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if n != 1+2*5 {
		t.Fatalf("decoded len: want %d got %d", 1+2*5, n)
	}

	// indices must round-trip exactly
	inIdx := ConciseSparseVector(in).Indices()
	outIdx := ConciseSparseVector(out).Indices()
	for i := range inIdx {
		if inIdx[i] != outIdx[i] {
			t.Fatalf("idx[%d]: want %d got %d", i, inIdx[i], outIdx[i])
		}
	}

	// values within one quantization step (scale = 4.0/255 ~= 0.0157)
	scale := QuantizedSparseVector(enc).Scale()
	inVal := ConciseSparseVector(in).Values()
	outVal := ConciseSparseVector(out).Values()
	for i := range inVal {
		if d := math.Abs(float64(inVal[i] - outVal[i])); d > float64(scale) {
			t.Fatalf("val[%d]: |%v-%v|=%v exceeds step %v", i, inVal[i], outVal[i], d, scale)
		}
	}
}

func TestQuantizedSparse_RelativeErrorOnHeavyWeights(t *testing.T) {
	// Heavy weights (the ones that dominate the inner product) should have
	// small relative error after uint8 quantization.
	in := mkConcise([]uint32{1, 2, 3}, []float32{5.0, 4.0, 3.5})
	enc, _ := EncodeQuantizedSparse(in, nil)
	out := make([]float32, 1+2*3)
	DecodeQuantizedSparse(QuantizedSparseVector(enc), out)
	inVal := ConciseSparseVector(in).Values()
	outVal := ConciseSparseVector(out).Values()
	for i := range inVal {
		rel := math.Abs(float64(inVal[i]-outVal[i])) / float64(inVal[i])
		if rel > 0.01 { // < 1% for weights near the peak
			t.Fatalf("val[%d] rel err %.4f too high", i, rel)
		}
	}
}

func TestEncodeQuantizedSparse_BufferReuse(t *testing.T) {
	in := mkConcise([]uint32{1, 2}, []float32{1.0, 0.5})
	buf := make([]byte, quantSparseHeaderSize+3*2)
	enc, err := EncodeQuantizedSparse(in, buf)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if &enc[0] != &buf[0] {
		t.Fatalf("expected buffer reuse (no realloc)")
	}
}

func TestEncodeQuantizedSparse_GrowsBuffer(t *testing.T) {
	in := mkConcise([]uint32{1, 2, 3, 4}, []float32{1, 2, 3, 4})
	enc, err := EncodeQuantizedSparse(in, []byte{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(enc) != quantSparseHeaderSize+3*4 {
		t.Fatalf("len: want %d got %d", quantSparseHeaderSize+3*4, len(enc))
	}
}

func TestEncodeQuantizedSparse_AllZeroValues(t *testing.T) {
	in := mkConcise([]uint32{1, 2}, []float32{0, 0})
	enc, err := EncodeQuantizedSparse(in, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	q := QuantizedSparseVector(enc)
	if q.Scale() != 0 {
		t.Fatalf("scale: want 0 got %v", q.Scale())
	}
	if q.CodeAt(0) != 0 || q.CodeAt(1) != 0 {
		t.Fatalf("codes: want 0,0 got %d,%d", q.CodeAt(0), q.CodeAt(1))
	}
}

func TestEncodeQuantizedSparse_Errors(t *testing.T) {
	if _, err := EncodeQuantizedSparse(nil, nil); err != ErrQuantSparseEmpty {
		t.Fatalf("nil: want ErrQuantSparseEmpty got %v", err)
	}
	// claims 5 nonzeros but slice is too short
	bad := []float32{5, 1, 2}
	if _, err := EncodeQuantizedSparse(bad, nil); err != ErrQuantSparseMalformed {
		t.Fatalf("malformed: want ErrQuantSparseMalformed got %v", err)
	}
}

func TestDecodeQuantizedSparse_Errors(t *testing.T) {
	if _, err := DecodeQuantizedSparse(QuantizedSparseVector{0, 0}, nil); err != ErrQuantSparseTruncated {
		t.Fatalf("short: want ErrQuantSparseTruncated got %v", err)
	}
	in := mkConcise([]uint32{1, 2}, []float32{1, 1})
	enc, _ := EncodeQuantizedSparse(in, nil)
	small := make([]float32, 2) // need 1+2*2=5
	if _, err := DecodeQuantizedSparse(QuantizedSparseVector(enc), small); err != ErrQuantSparseOutBuf {
		t.Fatalf("small out: want ErrQuantSparseOutBuf got %v", err)
	}
}
