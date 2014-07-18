//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not
//  use this file except in compliance with the License. You may obtain a copy
//  of the License at http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//  License for the specific language governing permissions and limitations
//  under the License.

// sorting interface for byte-slice.

package collatejson

import "bytes"

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
