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
	"errors"
	"fmt"
	"sync"
)

// ErrKeyNotFound is returned by *OrFetch lookups when the keyID is not in
// the cache and the provider's fresh snapshot also does not contain it.
var ErrKeyNotFound = errors.New("ear key not found")

func cloneEarKeys(keys []EaRKey) []EaRKey {
	keysCopy := make([]EaRKey, len(keys))
	copy(keysCopy, keys)
	for _, newKey := range keysCopy {
		keyData := make([]byte, len(newKey.Key))
		copy(keyData, newKey.Key)
		newKey.Key = keyData
	}
	return keysCopy
}

// EaRKeyCache is a goroutine-safe, self-healing cache of cbauth-issued
// encryption keys, indexed by KeyDataType and keyId.
//
// The cache does NOT call cbauth directly; it goes through an injected
// EaRKeyProvider. The cache also does NOT register cbauth callbacks,
// track in-use keys, panic on miss, or know about buckets — those are
// per-component policies that live in the EncryptionMgr.
type EaRKeyCache struct {
	provider EaRKeyProvider

	mu sync.RWMutex

	// kdtInfo holds the latest EncrKeysInfo snapshot for each KDT.
	kdtInfo map[KeyDataType]*EncrKeysInfo

	// kdtKeyByID[kdt][keyID] -> EaRKey for that KDT.
	kdtKeyByID map[KeyDataType]map[string]EaRKey

	// keyIDToKDT maps a keyID to the KDT it belongs to, for cross-KDT lookup.
	// Last writer wins if the same keyID ever appears under multiple KDTs
	// (cbauth's contract: keyIDs are globally unique).
	keyIDToKDT map[string]KeyDataType
}

// NewEaRKeyCache constructs a cache backed by provider. provider must be
// non-nil; pass CbauthEaRKeyProvider{} in production or a fake in tests.
func NewEaRKeyCache(provider EaRKeyProvider) *EaRKeyCache {
	if provider == nil {
		panic("NewEaRKeyCache: provider must not be nil")
	}
	return &EaRKeyCache{
		provider:   provider,
		kdtInfo:    make(map[KeyDataType]*EncrKeysInfo),
		kdtKeyByID: make(map[KeyDataType]map[string]EaRKey),
		keyIDToKDT: make(map[string]KeyDataType),
	}
}

// SetClusterKeys replaces the cached snapshot for kdt with info.
// info must be non-nil; passing nil is a no-op (callers should Delete instead).
// A defensive copy of info is stored.
func (c *EaRKeyCache) SetClusterKeys(kdt KeyDataType, info *EncrKeysInfo) {
	if info == nil {
		return
	}

	// Make a defensive copy before acquiring the lock.
	infoCopy := *info
	keysCopy := cloneEarKeys(info.Keys)
	infoCopy.Keys = keysCopy

	c.mu.Lock()
	defer c.mu.Unlock()

	// Remove reverse-index entries that pointed to this KDT but are no longer
	// present in the new snapshot.
	if oldInfo, ok := c.kdtInfo[kdt]; ok {
		for _, k := range oldInfo.Keys {
			if existingKDT, found := c.keyIDToKDT[k.Id]; found && existingKDT == kdt {
				delete(c.keyIDToKDT, k.Id)
			}
		}
	}

	// Store new snapshot.
	c.kdtInfo[kdt] = &infoCopy

	// Rebuild per-KDT key map and upsert reverse-index entries.
	byID := make(map[string]EaRKey, len(keysCopy))
	for _, k := range keysCopy {
		byID[k.Id] = k
		c.keyIDToKDT[k.Id] = kdt
	}
	c.kdtKeyByID[kdt] = byID
}

// GetClusterKeys returns the cached EncrKeysInfo for kdt and true,
// or zero value and false on miss.
func (c *EaRKeyCache) GetClusterKeys(kdt KeyDataType) (EncrKeysInfo, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	info, ok := c.kdtInfo[kdt]
	if !ok {
		return EncrKeysInfo{}, false
	}
	return *info, true
}

// GetActiveKey returns the EaRKey matching info.ActiveKeyId for kdt.
//
//	bool = true  : kdt is cached
//	bool = false : kdt has no cached info (true miss)
//
// When kdt is cached but ActiveKeyId == "" or no key in Keys matches
// ActiveKeyId, returns (EaRKey{}, true). Callers interpret an empty Id
// as "encryption disabled for this KDT".
func (c *EaRKeyCache) GetActiveKey(kdt KeyDataType) (EaRKey, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	info, ok := c.kdtInfo[kdt]
	if !ok {
		return EaRKey{}, false
	}
	if info.ActiveKeyId == "" {
		return EaRKey{}, true
	}
	byID, ok := c.kdtKeyByID[kdt]
	if !ok {
		return EaRKey{}, true
	}
	k := byID[info.ActiveKeyId]
	return k, true
}

// GetKeyByID looks up keyID scoped to a single kdt. Returns (EaRKey{}, false)
// on miss.
func (c *EaRKeyCache) GetKeyByID(kdt KeyDataType, keyID string) (EaRKey, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	byID, ok := c.kdtKeyByID[kdt]
	if !ok {
		return EaRKey{}, false
	}
	k, ok := byID[keyID]
	return k, ok
}

// FindKeyByID searches every cached KDT for keyID using the reverse index.
// Returns the EaRKey, the KDT it was found under, and true on hit.
// Used by indexer's cross-KDT fallback in getKeyCipherById.
func (c *EaRKeyCache) FindKeyByID(keyID string) (EaRKey, KeyDataType, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	kdt, ok := c.keyIDToKDT[keyID]
	if !ok {
		return EaRKey{}, KeyDataType{}, false
	}
	byID, ok := c.kdtKeyByID[kdt]
	if !ok {
		return EaRKey{}, KeyDataType{}, false
	}
	k, ok := byID[keyID]
	if !ok {
		return EaRKey{}, KeyDataType{}, false
	}
	return k, kdt, true
}

// Delete drops any cached snapshot and per-KDT key map for kdt, and removes
// the corresponding entries from the reverse index.
func (c *EaRKeyCache) Delete(kdt KeyDataType) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if oldInfo, ok := c.kdtInfo[kdt]; ok {
		for _, k := range oldInfo.Keys {
			if existingKDT, found := c.keyIDToKDT[k.Id]; found && existingKDT == kdt {
				delete(c.keyIDToKDT, k.Id)
			}
		}
	}
	delete(c.kdtInfo, kdt)
	delete(c.kdtKeyByID, kdt)
}

// Refresh unconditionally fetches fresh keys for kdt from the provider and
// replaces the cached snapshot. Use this when cbauth signals that keys have
// changed (refreshKeysCallback, dropKeysCallback) so the cache is never left
// holding stale data.
func (c *EaRKeyCache) Refresh(ctx context.Context, kdt KeyDataType) error {
	info, err := c.provider.FetchKeys(ctx, kdt)
	if err != nil {
		return fmt.Errorf("EaRKeyCache: Refresh(%v): %w", kdt, err)
	}
	c.SetClusterKeys(kdt, info)
	return nil
}

// GetActiveKeyOrFetch returns the active key for kdt from the cache; on miss
// it calls the provider, stores the fresh snapshot, and retries.
//
// Returns (EaRKey{}, nil) when the cluster has encryption disabled for kdt
// (ActiveKeyId == ""). Provider errors bubble up unchanged.
func (c *EaRKeyCache) GetActiveKeyOrFetch(ctx context.Context, kdt KeyDataType) (EaRKey, error) {
	if k, ok := c.GetActiveKey(kdt); ok {
		return k, nil
	}
	info, err := c.provider.FetchKeys(ctx, kdt)
	if err != nil {
		return EaRKey{}, fmt.Errorf("EaRKeyCache: provider FetchKeys(%v): %w", kdt, err)
	}
	c.SetClusterKeys(kdt, info)
	k, _ := c.GetActiveKey(kdt)
	return k, nil
}

// GetKeyByIDOrFetch returns the EaRKey for keyID under kdt from the cache;
// on miss it calls the provider, stores the fresh snapshot, and retries.
// Returns ErrKeyNotFound if the key is still absent after the refresh.
// Provider errors bubble up unchanged.
//
// Empty keyID is a caller-side concern (it means "plaintext"); the cache
// returns ErrKeyNotFound for it.
func (c *EaRKeyCache) GetKeyByIDOrFetch(ctx context.Context, kdt KeyDataType, keyID string) (EaRKey, error) {
	if keyID == "" {
		return EaRKey{}, ErrKeyNotFound
	}
	if k, ok := c.GetKeyByID(kdt, keyID); ok {
		return k, nil
	}
	info, err := c.provider.FetchKeys(ctx, kdt)
	if err != nil {
		return EaRKey{}, fmt.Errorf("EaRKeyCache: provider FetchKeys(%v): %w", kdt, err)
	}
	c.SetClusterKeys(kdt, info)
	if k, ok := c.GetKeyByID(kdt, keyID); ok {
		return k, nil
	}
	return EaRKey{}, ErrKeyNotFound
}
