//  Copyright (c) 2013 Couchbase, Inc.

// sorting interface for byte-slice.

package common

import "bytes"

// ByteSlices to implement Sort interface.
type ByteSlices [][]byte

func (b ByteSlices) Len() int {
	return len(b)
}

func (b ByteSlices) Less(i, j int) bool {
	if bytes.Compare(b[i], b[j]) > 0 {
		return false
	}
	return true
}

func (b ByteSlices) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}
