//go:build community
// +build community

// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package n1ql

import (
	"fmt"
	"os"
)

func newBackfillCryptWriter(_ *os.File, _ string, _ string,
	_ func([]byte) []byte, _ uint32, _ bool) (backfillCryptWriter, error) {

	return nil, fmt.Errorf("encryption not supported in community edition")
}

func newBackfillCryptReader(_ *os.File, _ func([]byte) []byte,
	_ []byte, _ uint32, _ bool) (backfillCryptReader, error) {

	return nil, fmt.Errorf("encryption not supported in community edition")
}

func isDecryptionError(_ error) bool {
	return false
}
