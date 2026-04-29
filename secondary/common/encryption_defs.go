// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package common

import (
	"errors"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/cbauth/cbauthimpl"
)

type KeyID = string

// KeyDataType represents the scope of an encryption key. For indexer we use
// bucket-level keys (TypeName="bucket", BucketUUID set), but the structure is
// generic to match cbauth expectations.
type KeyDataType = cbauth.KeyDataType

// EaRKey represents a single encryption-at-rest key (DEK) with its cipher and
// encoded key material.
type EaRKey = cbauthimpl.EaRKey

// EncrKeysInfo aggregates active and available keys for a given KeyDataType.
type EncrKeysInfo = cbauth.EncrKeysInfo

// ShardKeyBundle is [[]KeyID] used per shard KeyIDs lists the key identifiers in use,
// and StagingDir points to the copied key files in the transfer staging area.
type ShardKeyBundle = []KeyID

var ErrNoCallbackDefined = errors.New("no encryption store callback set")

type MetaEncryptCallbacks struct {
	getActiveKey      func() (*EaRKey, error)
	getKeyCipherByID  func(KeyID) (*EaRKey, error)
	getEncryptionKeys func() (*EncrKeysInfo, error)
	setInuseKey       func(KeyID) error
}

func (mec *MetaEncryptCallbacks) GetActiveKeyCipher() (*EaRKey, error) {
	if mec.getActiveKey != nil {
		return mec.getActiveKey()
	}
	return nil, ErrNoCallbackDefined
}

func (mec *MetaEncryptCallbacks) GetKeyCipherByID(keyID KeyID) (*EaRKey, error) {
	if mec.getKeyCipherByID != nil {
		return mec.getKeyCipherByID(keyID)
	}
	return nil, ErrNoCallbackDefined
}

func (mec *MetaEncryptCallbacks) GetEncryptionKeys() (*EncrKeysInfo, error) {
	if mec.getEncryptionKeys != nil {
		return mec.getEncryptionKeys()
	}
	return nil, ErrNoCallbackDefined
}

func (mec *MetaEncryptCallbacks) SetInuseKeys(keyID KeyID) error {
	if mec.setInuseKey != nil {
		return mec.setInuseKey(keyID)
	}
	return ErrNoCallbackDefined
}

func NewMetaEncryptionCallbacks(
	getActiveKey func() (*EaRKey, error),
	getKeyCipherByID func(KeyID) (*EaRKey, error),
	getEncryptionKeys func() (*EncrKeysInfo, error),
	setInuseKey func(KeyID) error,
) *MetaEncryptCallbacks {

	return &MetaEncryptCallbacks{
		getActiveKey:      getActiveKey,
		getKeyCipherByID:  getKeyCipherByID,
		getEncryptionKeys: getEncryptionKeys,
		setInuseKey:       setInuseKey,
	}
}
