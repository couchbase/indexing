//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package common

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"net"
	"os"
	"sync/atomic"
	"time"
)

// Globals
var counter uint64 = 0
var salt uint32 = makeSalt()

// Generate an almost unique integer. Quality reduces after 2^32 calls.
func NewID() uint64 {
	token := atomic.AddUint64(&counter, 1)
	id := uint64(salt)<<32 + token
	return id
}

// Generate almost unique 32-bit stamp for this process
func makeSalt() uint32 {
	var buf bytes.Buffer
	ifaces, err := net.Interfaces()
	if err == nil {
		for _, i := range ifaces {
			buf.Write(i.HardwareAddr)
		}
	}
	b64 := make([]byte, 8)
	binary.LittleEndian.PutUint64(b64, uint64(time.Now().UnixNano()))
	buf.Write(b64)
	binary.LittleEndian.PutUint64(b64, uint64(os.Getpid()))
	buf.Write(b64)
	return crc32.ChecksumIEEE(buf.Bytes())
}
