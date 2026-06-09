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
	"sync"
	"testing"
)

// fakeProvider is a test-only EaRKeyProvider.
type fakeProvider struct {
	mu        sync.Mutex
	snapshots map[KeyDataType]*EncrKeysInfo
	err       error
	callCount int
	// blockCh, when non-nil, blocks FetchKeys until the channel is closed.
	blockCh chan struct{}
}

func newFakeProvider() *fakeProvider {
	return &fakeProvider{snapshots: make(map[KeyDataType]*EncrKeysInfo)}
}

func (f *fakeProvider) FetchKeys(ctx context.Context, kdt KeyDataType) (*EncrKeysInfo, error) {
	if f.blockCh != nil {
		select {
		case <-f.blockCh:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.callCount++
	if f.err != nil {
		return nil, f.err
	}
	info, ok := f.snapshots[kdt]
	if !ok {
		return &EncrKeysInfo{}, nil
	}
	return info, nil
}

func (f *fakeProvider) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.callCount
}

var kdtLog = KeyDataType{TypeName: "log"}
var kdtBucket = KeyDataType{TypeName: "bucket", BucketUUID: "u1"}

func makeKey(id, cipher string) EaRKey {
	return EaRKey{Id: id, Cipher: cipher, Key: []byte(id)}
}

func makeInfo(activeID string, keys ...EaRKey) *EncrKeysInfo {
	return &EncrKeysInfo{ActiveKeyId: activeID, Keys: keys}
}

// --- pure cache tests ---

func TestNewEaRKeyCache_NilProviderPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for nil provider")
		}
	}()
	NewEaRKeyCache(nil)
}

func TestSetAndGetClusterKeys(t *testing.T) {
	c := NewEaRKeyCache(newFakeProvider())
	k1 := makeKey("k1", "AES-256-GCM")
	info := makeInfo("k1", k1)
	c.SetClusterKeys(kdtLog, info)

	// Mutate the input after Set — should not affect the cache.
	info.ActiveKeyId = "mutated"
	info.Keys[0].Cipher = "mutated"

	got, ok := c.GetClusterKeys(kdtLog)
	if !ok {
		t.Fatal("expected cache hit")
	}
	if got.ActiveKeyId != "k1" {
		t.Errorf("defensive copy failed: got ActiveKeyId=%v", got.ActiveKeyId)
	}
	if got.Keys[0].Cipher != "AES-256-GCM" {
		t.Errorf("defensive copy failed: got Cipher=%v", got.Keys[0].Cipher)
	}
}

func TestGetActiveKey_HappyPath(t *testing.T) {
	c := NewEaRKeyCache(newFakeProvider())
	k1 := makeKey("k1", "AES-256-GCM")
	c.SetClusterKeys(kdtLog, makeInfo("k1", k1))

	got, ok := c.GetActiveKey(kdtLog)
	if !ok {
		t.Fatal("expected cached=true")
	}
	if got.Id != "k1" {
		t.Errorf("expected k1, got %v", got.Id)
	}
}

func TestGetActiveKey_EncryptionDisabled(t *testing.T) {
	c := NewEaRKeyCache(newFakeProvider())
	c.SetClusterKeys(kdtLog, makeInfo("", makeKey("k1", "AES-256-GCM")))

	got, ok := c.GetActiveKey(kdtLog)
	if !ok {
		t.Fatal("expected cached=true")
	}
	if got.Id != "" {
		t.Errorf("expected empty EaRKey, got %v", got.Id)
	}
}

func TestGetActiveKey_Miss(t *testing.T) {
	c := NewEaRKeyCache(newFakeProvider())

	_, ok := c.GetActiveKey(kdtLog)
	if ok {
		t.Fatal("expected cached=false for unknown KDT")
	}
}

func TestGetActiveKey_ActiveKeyMissingFromList(t *testing.T) {
	c := NewEaRKeyCache(newFakeProvider())
	// ActiveKeyId set but absent from Keys slice.
	c.SetClusterKeys(kdtLog, makeInfo("nonexistent"))

	got, ok := c.GetActiveKey(kdtLog)
	if !ok {
		t.Fatal("expected cached=true")
	}
	if got.Id != "" {
		t.Errorf("expected zero EaRKey, got %v", got.Id)
	}
}

func TestGetKeyByID_HitAndMiss(t *testing.T) {
	c := NewEaRKeyCache(newFakeProvider())
	k1 := makeKey("k1", "AES-256-GCM")
	c.SetClusterKeys(kdtLog, makeInfo("k1", k1))

	got, ok := c.GetKeyByID(kdtLog, "k1")
	if !ok || got.Id != "k1" {
		t.Errorf("expected hit for k1, ok=%v got=%v", ok, got.Id)
	}
	_, ok = c.GetKeyByID(kdtLog, "missing")
	if ok {
		t.Error("expected miss for unknown keyID")
	}
}

func TestFindKeyByID_CrossKDT(t *testing.T) {
	c := NewEaRKeyCache(newFakeProvider())
	kLog := makeKey("kL", "AES-256-GCM")
	kBucket := makeKey("kB", "AES-256-GCM")
	c.SetClusterKeys(kdtLog, makeInfo("kL", kLog))
	c.SetClusterKeys(kdtBucket, makeInfo("kB", kBucket))

	got, gotKDT, ok := c.FindKeyByID("kL")
	if !ok || got.Id != "kL" || gotKDT != kdtLog {
		t.Errorf("cross-KDT lookup failed: ok=%v id=%v kdt=%v", ok, got.Id, gotKDT)
	}

	got, gotKDT, ok = c.FindKeyByID("kB")
	if !ok || got.Id != "kB" || gotKDT != kdtBucket {
		t.Errorf("cross-KDT lookup failed: ok=%v id=%v kdt=%v", ok, got.Id, gotKDT)
	}
}

func TestSetClusterKeys_RemovesStaleReverseIndex(t *testing.T) {
	c := NewEaRKeyCache(newFakeProvider())
	k1 := makeKey("k1", "AES-256-GCM")
	k2 := makeKey("k2", "AES-256-GCM")
	c.SetClusterKeys(kdtLog, makeInfo("k1", k1))

	// Replace with a new snapshot that has only k2.
	c.SetClusterKeys(kdtLog, makeInfo("k2", k2))

	_, _, ok := c.FindKeyByID("k1")
	if ok {
		t.Error("stale reverse-index entry k1 should have been removed")
	}
	_, _, ok = c.FindKeyByID("k2")
	if !ok {
		t.Error("new key k2 should be in reverse index")
	}
}

func TestDelete(t *testing.T) {
	c := NewEaRKeyCache(newFakeProvider())
	k1 := makeKey("k1", "AES-256-GCM")
	c.SetClusterKeys(kdtLog, makeInfo("k1", k1))
	c.Delete(kdtLog)

	_, ok := c.GetClusterKeys(kdtLog)
	if ok {
		t.Error("expected miss after Delete")
	}
	_, _, ok = c.FindKeyByID("k1")
	if ok {
		t.Error("reverse-index should be cleared after Delete")
	}
}

func TestConcurrentReadersOneWriter(t *testing.T) {
	c := NewEaRKeyCache(newFakeProvider())
	k1 := makeKey("k1", "AES-256-GCM")
	c.SetClusterKeys(kdtLog, makeInfo("k1", k1))

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.GetKeyByID(kdtLog, "k1")
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			c.SetClusterKeys(kdtLog, makeInfo("k1", k1))
		}
	}()
	wg.Wait()
}

// --- Refresh tests ---

func TestRefresh_PopulatesCache(t *testing.T) {
	fp := newFakeProvider()
	k1 := makeKey("k1", "AES-256-GCM")
	fp.snapshots[kdtLog] = makeInfo("k1", k1)
	c := NewEaRKeyCache(fp)

	if err := c.Refresh(context.Background(), kdtLog); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if fp.calls() != 1 {
		t.Errorf("expected 1 provider call, got %v", fp.calls())
	}
	got, ok := c.GetClusterKeys(kdtLog)
	if !ok || got.ActiveKeyId != "k1" {
		t.Errorf("expected cached k1, got ok=%v id=%v", ok, got.ActiveKeyId)
	}
}

func TestRefresh_ReplacesStaleSnapshot(t *testing.T) {
	fp := newFakeProvider()
	k1 := makeKey("k1", "AES-256-GCM")
	k2 := makeKey("k2", "AES-256-GCM")
	c := NewEaRKeyCache(fp)

	// Seed with k1.
	fp.snapshots[kdtLog] = makeInfo("k1", k1)
	if err := c.Refresh(context.Background(), kdtLog); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// Provider now returns k2 (key rotation).
	fp.snapshots[kdtLog] = makeInfo("k2", k2)
	if err := c.Refresh(context.Background(), kdtLog); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if fp.calls() != 2 {
		t.Errorf("expected 2 provider calls, got %v", fp.calls())
	}
	active, ok := c.GetActiveKey(kdtLog)
	if !ok || active.Id != "k2" {
		t.Errorf("expected active key k2 after refresh, got ok=%v id=%v", ok, active.Id)
	}
	// k1 should no longer be in the reverse index.
	if _, _, found := c.FindKeyByID("k1"); found {
		t.Error("stale k1 should have been removed from reverse index")
	}
}

func TestRefresh_ProviderError(t *testing.T) {
	sentinel := errors.New("provider down")
	fp := newFakeProvider()
	fp.err = sentinel
	c := NewEaRKeyCache(fp)

	err := c.Refresh(context.Background(), kdtLog)
	if err == nil || !errors.Is(err, sentinel) {
		t.Errorf("expected wrapped sentinel, got %v", err)
	}
	// Cache must remain unpopulated on error.
	if _, ok := c.GetClusterKeys(kdtLog); ok {
		t.Error("cache should be empty after failed Refresh")
	}
}

// --- fetch-on-miss tests ---

func TestGetKeyByIDOrFetch_CacheHit_NoProviderCall(t *testing.T) {
	fp := newFakeProvider()
	c := NewEaRKeyCache(fp)
	k1 := makeKey("k1", "AES-256-GCM")
	c.SetClusterKeys(kdtLog, makeInfo("k1", k1))

	got, err := c.GetKeyByIDOrFetch(context.Background(), kdtLog, "k1")
	if err != nil || got.Id != "k1" {
		t.Errorf("unexpected result: err=%v id=%v", err, got.Id)
	}
	if fp.calls() != 0 {
		t.Errorf("expected 0 provider calls, got %v", fp.calls())
	}
}

func TestGetKeyByIDOrFetch_MissThenProviderSucceeds(t *testing.T) {
	fp := newFakeProvider()
	k1 := makeKey("k1", "AES-256-GCM")
	fp.snapshots[kdtLog] = makeInfo("k1", k1)
	c := NewEaRKeyCache(fp)

	got, err := c.GetKeyByIDOrFetch(context.Background(), kdtLog, "k1")
	if err != nil || got.Id != "k1" {
		t.Errorf("unexpected result: err=%v id=%v", err, got.Id)
	}
	if fp.calls() != 1 {
		t.Errorf("expected 1 provider call, got %v", fp.calls())
	}
	// Second call should hit the cache.
	_, _ = c.GetKeyByIDOrFetch(context.Background(), kdtLog, "k1")
	if fp.calls() != 1 {
		t.Errorf("expected still 1 provider call after cache populated, got %v", fp.calls())
	}
}

func TestGetKeyByIDOrFetch_MissProviderReturnsEmpty(t *testing.T) {
	fp := newFakeProvider()
	// No snapshot for kdtLog → provider returns empty EncrKeysInfo.
	c := NewEaRKeyCache(fp)

	_, err := c.GetKeyByIDOrFetch(context.Background(), kdtLog, "k1")
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestGetKeyByIDOrFetch_ProviderError_PropagatesWrapped(t *testing.T) {
	sentinel := errors.New("cbauth down")
	fp := newFakeProvider()
	fp.err = sentinel
	c := NewEaRKeyCache(fp)

	_, err := c.GetKeyByIDOrFetch(context.Background(), kdtLog, "k1")
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("expected wrapped sentinel, got %v", err)
	}
}

func TestGetKeyByIDOrFetch_EmptyKeyID_ReturnsErrKeyNotFound_NoProviderCall(t *testing.T) {
	fp := newFakeProvider()
	c := NewEaRKeyCache(fp)

	_, err := c.GetKeyByIDOrFetch(context.Background(), kdtLog, "")
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("expected ErrKeyNotFound for empty keyID, got %v", err)
	}
	if fp.calls() != 0 {
		t.Errorf("expected 0 provider calls for empty keyID, got %v", fp.calls())
	}
}

func TestGetActiveKeyOrFetch_CacheHit_NoProviderCall(t *testing.T) {
	fp := newFakeProvider()
	k1 := makeKey("k1", "AES-256-GCM")
	c := NewEaRKeyCache(fp)
	c.SetClusterKeys(kdtLog, makeInfo("k1", k1))

	got, err := c.GetActiveKeyOrFetch(context.Background(), kdtLog)
	if err != nil || got.Id != "k1" {
		t.Errorf("unexpected result: err=%v id=%v", err, got.Id)
	}
	if fp.calls() != 0 {
		t.Errorf("expected 0 provider calls, got %v", fp.calls())
	}
}

func TestGetActiveKeyOrFetch_MissThenProviderSucceeds(t *testing.T) {
	fp := newFakeProvider()
	k1 := makeKey("k1", "AES-256-GCM")
	fp.snapshots[kdtLog] = makeInfo("k1", k1)
	c := NewEaRKeyCache(fp)

	got, err := c.GetActiveKeyOrFetch(context.Background(), kdtLog)
	if err != nil || got.Id != "k1" {
		t.Errorf("unexpected result: err=%v id=%v", err, got.Id)
	}
	if fp.calls() != 1 {
		t.Errorf("expected 1 provider call, got %v", fp.calls())
	}
}

func TestGetActiveKeyOrFetch_EncryptionDisabledFromProvider(t *testing.T) {
	fp := newFakeProvider()
	// ActiveKeyId == "" means encryption is disabled.
	fp.snapshots[kdtLog] = makeInfo("", makeKey("k1", "AES-256-GCM"))
	c := NewEaRKeyCache(fp)

	got, err := c.GetActiveKeyOrFetch(context.Background(), kdtLog)
	if err != nil {
		t.Errorf("expected nil error for disabled encryption, got %v", err)
	}
	if got.Id != "" {
		t.Errorf("expected zero EaRKey, got %v", got.Id)
	}
}

func TestGetActiveKeyOrFetch_ProviderError(t *testing.T) {
	sentinel := errors.New("cbauth timeout")
	fp := newFakeProvider()
	fp.err = sentinel
	c := NewEaRKeyCache(fp)

	_, err := c.GetActiveKeyOrFetch(context.Background(), kdtLog)
	if !errors.Is(err, sentinel) {
		t.Errorf("expected wrapped sentinel, got %v", err)
	}
}

func TestOrFetch_ContextCancellation(t *testing.T) {
	fp := newFakeProvider()
	fp.blockCh = make(chan struct{})
	c := NewEaRKeyCache(fp)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := c.GetKeyByIDOrFetch(ctx, kdtLog, "k1")
		errCh <- err
	}()

	cancel()
	err := <-errCh
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestOrFetch_Stampede_Idempotent(t *testing.T) {
	fp := newFakeProvider()
	k1 := makeKey("k1", "AES-256-GCM")
	fp.snapshots[kdtLog] = makeInfo("k1", k1)
	c := NewEaRKeyCache(fp)

	var wg sync.WaitGroup
	errs := make(chan error, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := c.GetKeyByIDOrFetch(context.Background(), kdtLog, "k1")
			if err != nil {
				errs <- err
				return
			}
			if got.Id != "k1" {
				errs <- errors.New("wrong key id")
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("goroutine error: %v", err)
	}
}
