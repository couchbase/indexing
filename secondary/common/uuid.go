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
	"crypto/rand"
	"encoding/binary"
	"io"
	"strconv"
)

type UUID []byte

func NewUUID() (UUID, error) {
	uuid := make([]byte, 8)
	n, err := io.ReadFull(rand.Reader, uuid)
	if n != len(uuid) || err != nil {
		return UUID(nil), err
	}
	return UUID(uuid), nil
}

func (u UUID) Uint64() uint64 {
	return binary.LittleEndian.Uint64(([]byte)(u))
}

func (u UUID) Str() string {
	var buf bytes.Buffer
	for i := 0; i < len(u); i++ {
		if i > 0 {
			buf.WriteString(":")
		}
		buf.WriteString(strconv.FormatUint(uint64(u[i]), 16))
	}
	return buf.String()
}
