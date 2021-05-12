//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package common

import (
	"bytes"
	crypt "crypto/rand"
	"encoding/binary"
	"io"
	rnd "math/rand"
	"strconv"
)

type UUID []byte

func NewUUID() (UUID, error) {
	uuid := make([]byte, 8)
	n, err := io.ReadFull(crypt.Reader, uuid)
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

func SeedProcess() {
	uuid, err := NewUUID()
	if err != nil {
		seed := uuid.Uint64()
		rnd.Seed(int64(seed))
	}
}
