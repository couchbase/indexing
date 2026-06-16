// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package common

import (
	"context"
	"fmt"

	"github.com/couchbase/cbauth"
)

// EaRKeyProvider supplies cluster-wide EncrKeysInfo to an EaRKeyCache when
// the cache misses. Implementations may block.
//
// Production wires this to cbauth via CbauthEaRKeyProvider. Tests inject
// fakes so the cache can be unit-tested without a real cbauth backend.
type EaRKeyProvider interface {
	FetchKeys(ctx context.Context, kdt KeyDataType) (*EncrKeysInfo, error)
}

// CbauthEaRKeyProvider is the default EaRKeyProvider. It tries the non-blocking
// cbauth.GetEncryptionKeys first; if that fails (keys not yet available) it falls
// back to cbauth.GetEncryptionKeysBlocking to wait until they are.
type CbauthEaRKeyProvider struct{}

func (CbauthEaRKeyProvider) FetchKeys(ctx context.Context, kdt KeyDataType) (*EncrKeysInfo, error) {
	if info, err := cbauth.GetEncryptionKeys(kdt); err == nil {
		return info, nil
	}

	info, err := cbauth.GetEncryptionKeysBlocking(ctx, kdt)
	if err != nil {
		return nil, fmt.Errorf("EaR key fetch failed with err: %w", err)
	}
	return info, nil
}
