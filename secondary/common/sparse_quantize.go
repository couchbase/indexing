// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package common

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// Scalar-quantized sparse vector storage format.
//
// A concise sparse vector [N, idx1..idxN, val1..valN] stored as float32 takes
// (1+2N)*4 bytes. Term weights (values) are scalar-quantized to uint8 with a
// per-vector scale, and dimension ids are stored as uint16 (vocab <= 65535),
// cutting storage to 6+3N bytes (~62% smaller at N=200).
//
// Layout (little-endian):
//
//	[0:4]        scale    float32   value_i ~= code_i * scale
//	[4:6]        N        uint16    nonzero count
//	[6:6+2N]     indices  uint16xN  dimension ids, ascending
//	[6+2N:6+3N]  codes    uint8xN   quantized weights (unsigned)
//
// Alignment: scale sits at offset 0 (4-aligned) and indices at offset 6
// (2-aligned), so a future zero-copy reinterpret of the index region as
// []uint16 stays valid if the buffer base is aligned. Until then, accessors
// read elements via binary.LittleEndian, which is alignment-agnostic.
//
// Values are assumed non-negative (SPLADE log(1+ReLU)); the format does not
// carry a sign bit.
const (
	quantSparseHeaderSize = 6     // scale(4) + N(2)
	quantSparseMaxCode    = 255.0 // uint8 full-scale
)

var (
	ErrQuantSparseEmpty     = errors.New("empty concise sparse vector")
	ErrQuantSparseMalformed = errors.New("malformed concise sparse vector")
	ErrQuantSparseTruncated = errors.New("quantized sparse vector truncated")
	ErrQuantSparseOutBuf    = errors.New("output buffer too small")
)

// QuantizedSparseVector is a zero-copy view over a packed quantized sparse
// vector. Accessor bounds are the caller's responsibility on the hot path;
// use NNZ()/Size() to validate the backing slice once before iterating.
type QuantizedSparseVector []byte

// Scale returns the per-vector dequantization scale.
func (q QuantizedSparseVector) Scale() float32 {
	return math.Float32frombits(binary.LittleEndian.Uint32(q[0:4]))
}

// NNZ returns the stored nonzero count. Returns 0 for a too-short buffer.
func (q QuantizedSparseVector) NNZ() int {
	if len(q) < quantSparseHeaderSize {
		return 0
	}
	return int(binary.LittleEndian.Uint16(q[4:6]))
}

// Size returns the total byte length implied by NNZ.
func (q QuantizedSparseVector) Size() int {
	return quantSparseHeaderSize + 3*q.NNZ()
}

// IndexAt returns the i-th dimension id. Caller must ensure 0 <= i < NNZ.
func (q QuantizedSparseVector) IndexAt(i int) uint16 {
	off := quantSparseHeaderSize + 2*i
	return binary.LittleEndian.Uint16(q[off : off+2])
}

// CodeAt returns the i-th quantized weight code. Caller must ensure
// 0 <= i < NNZ.
func (q QuantizedSparseVector) CodeAt(i int) uint8 {
	return q[quantSparseHeaderSize+2*q.NNZ()+i]
}

// EncodeQuantizedSparse packs a concise sparse vector into the quantized
// storage format. Values must be non-negative and indices must fit in uint16
// (<= 65535), ascending. The per-vector scale is max(value)/255. buf is reused
// when it has capacity; the (possibly reallocated) buffer is returned.
func EncodeQuantizedSparse(concise []float32, buf []byte) ([]byte, error) {
	if len(concise) == 0 {
		return nil, ErrQuantSparseEmpty
	}
	nnz := int(concise[0])
	if nnz < 0 || len(concise) < 1+2*nnz {
		return nil, ErrQuantSparseMalformed
	}
	if nnz > math.MaxUint16 {
		return nil, fmt.Errorf("sparse nnz %d exceeds uint16 limit", nnz)
	}

	indices := concise[1 : 1+nnz]
	values := concise[1+nnz : 1+2*nnz]

	// Per-vector scale from the peak weight. A zero/empty-mass vector yields
	// scale 0 and all-zero codes, which decode back to zeros.
	var maxv float32
	for _, v := range values {
		if v > maxv {
			maxv = v
		}
	}
	var scale float32
	if maxv > 0 {
		scale = maxv / quantSparseMaxCode
	}

	outLen := quantSparseHeaderSize + 3*nnz
	if cap(buf) < outLen {
		buf = make([]byte, outLen)
	} else {
		buf = buf[:outLen]
	}

	binary.LittleEndian.PutUint32(buf[0:4], math.Float32bits(scale))
	binary.LittleEndian.PutUint16(buf[4:6], uint16(nnz))

	idxOff := quantSparseHeaderSize
	codeOff := quantSparseHeaderSize + 2*nnz
	for i := 0; i < nnz; i++ {
		idx := indices[i]
		if idx < 0 || idx > math.MaxUint16 {
			return nil, fmt.Errorf("sparse index %v exceeds uint16 limit", idx)
		}
		binary.LittleEndian.PutUint16(buf[idxOff+2*i:], uint16(idx))

		var code uint8
		if scale > 0 {
			// Round half up; values are non-negative. Clamp defends against
			// FP drift pushing the peak slightly past 255.
			c := int(values[i]/scale + 0.5)
			if c < 0 {
				c = 0
			} else if c > 255 {
				c = 255
			}
			code = uint8(c)
		}
		buf[codeOff+i] = code
	}
	return buf, nil
}

// DecodeQuantizedSparse reconstructs an approximate concise float32 vector
// from the quantized format into out, returning the number of float32 elements
// written (1+2N). out must have length >= 1+2*NNZ. Primarily for testing and
// debugging; the scan path reads the quantized buffer in place instead.
func DecodeQuantizedSparse(q QuantizedSparseVector, out []float32) (int, error) {
	if len(q) < quantSparseHeaderSize {
		return 0, ErrQuantSparseTruncated
	}
	nnz := q.NNZ()
	if len(q) < q.Size() {
		return 0, ErrQuantSparseTruncated
	}
	need := 1 + 2*nnz
	if len(out) < need {
		return 0, ErrQuantSparseOutBuf
	}
	scale := q.Scale()
	out[0] = float32(nnz)
	for i := 0; i < nnz; i++ {
		out[1+i] = float32(q.IndexAt(i))
		out[1+nnz+i] = scale * float32(q.CodeAt(i))
	}
	return need, nil
}
