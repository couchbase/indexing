// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

// EncryptionCtx is a local interface satisfied by gocbcrypto.EncryptionContext
// (enterprise) or nil (community / plaintext mode). Callers only need KeyID for
// tracking which key is in use, all crypto operations go through the helpers in
// encryption_enterprise.go / encryption_community.go.
type EncryptionCtx interface {
	KeyID() []byte
}

// CryptFileWriter is a local interface satisfied by gocbcrypto.CryptFileWriter
// (enterprise). Used for encrypted codebook persistence.
type CryptFileWriter interface {
	WriteHeader() (int, error)
	Flush() error
	EncryptAndWriteBlock([]byte) (int, error)
	Reset()
}
