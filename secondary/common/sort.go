//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

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
