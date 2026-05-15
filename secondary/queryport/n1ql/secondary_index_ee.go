//go:build !community
// +build !community

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

	"github.com/couchbase/gocbcrypto"
)

func newBackfillCryptWriter(file *os.File, cipher string, encryptionKeyId string,
	getKeyById func([]byte) []byte, bufSz uint32, aligned bool) (backfillCryptWriter, error) {

	var ctx gocbcrypto.EncryptionContext
	var err error

	switch cipher {
	case "AES-256-GCM":
		ctx, err = gocbcrypto.NewAESGCM256ContextWithOpenSSL(
			[]byte(encryptionKeyId), getKeyById([]byte(encryptionKeyId)), scanKDFLabelCtx, 0)
	default:
		return nil, fmt.Errorf("invalid cipher:%v", cipher)
	}
	if err != nil {
		return nil, err
	}

	return gocbcrypto.NewCryptFileWriter(file, ctx, bufSz, aligned, nil)
}

func newBackfillCryptReader(file *os.File, getKeyById func([]byte) []byte,
	label []byte, bufSz uint32, aligned bool) (backfillCryptReader, error) {

	return gocbcrypto.NewCryptFileReaderWithLabel(file, getKeyById, label, bufSz, aligned, nil)
}

func isDecryptionError(err error) bool {
	return gocbcrypto.IsDecryptionError(err)
}
