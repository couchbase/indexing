//go:build !community

package common

import "github.com/couchbase/bhive"

const BhiveSentinalCellID = bhive.SentinelCellID

//go:inline
func DequantizeSparseWire(buf []byte) []float32 {
	return bhive.DequantizeSparseWire(buf)
}

//go:inline
func QuantizedWireSize(buf []byte) int {
	return bhive.QuantizedWireSize(buf)
}
