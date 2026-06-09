// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package projector

import (
	"errors"
	"sync"
	"testing"
	"time"

	c "github.com/couchbase/indexing/secondary/common"
)

func makeEaRKey(id, cipher string) c.EaRKey {
	return c.EaRKey{Id: id, Cipher: cipher, Key: []byte(id)}
}

func makeEncrKeysInfo(activeID string, keys ...c.EaRKey) *c.EncrKeysInfo {
	return &c.EncrKeysInfo{ActiveKeyId: activeID, Keys: keys}
}

func testConfig() c.Config {
	return c.Config{}
}

// newEncryptionMgrNoCbauth builds the manager with a pre-seeded cache and
// no cbauth callback registration. Used by tests only.
func newEncryptionMgrNoCbauth(config c.Config, seed *c.EncrKeysInfo) *EncryptionMgr {
	seedCh := make(chan struct{})
	close(seedCh) // no async seed needed in tests; unblock run() immediately
	e := &EncryptionMgr{
		config:    config,
		cache:     c.NewEaRKeyCache(c.CbauthEaRKeyProvider{}),
		inUseKeys: make(map[string]struct{}),
		workCh:    make(chan workItem, workChCapacity),
		stopCh:    make(chan struct{}),
		seedCh:    seedCh,
	}
	if seed != nil {
		e.cache.SetClusterKeys(LogdataKDT, seed)
	}
	go e.run()
	return e
}

// TestSetInUseAndDrop verifies SetInUseKeys and DropInUseKeys membership.
func TestSetInUseAndDrop(t *testing.T) {
	e := newEncryptionMgrNoCbauth(testConfig(), nil)
	defer e.Close()

	e.SetInUseKeys("k1")
	e.SetInUseKeys("k2")

	keys, err := e.getInUseKeysCallback(LogdataKDT)
	if err == nil {
		t.Error("expected ErrEncrMgrNotReady before MarkReady")
	}

	e.MarkReady()
	keys, err = e.getInUseKeysCallback(LogdataKDT)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	keySet := make(map[string]bool)
	for _, k := range keys {
		keySet[k] = true
	}
	if !keySet["k1"] || !keySet["k2"] {
		t.Errorf("expected k1 and k2 in keys, got %v", keys)
	}

	e.DropInUseKeys([]string{"k1"})
	keys, _ = e.getInUseKeysCallback(LogdataKDT)
	for _, k := range keys {
		if k == "k1" {
			t.Error("k1 should have been removed")
		}
	}
}

// TestGetInUseKeysCallback_NotReady verifies the not-ready guard.
func TestGetInUseKeysCallback_NotReady(t *testing.T) {
	e := newEncryptionMgrNoCbauth(testConfig(), nil)
	defer e.Close()

	_, err := e.getInUseKeysCallback(LogdataKDT)
	if !errors.Is(err, ErrEncrMgrNotReady) {
		t.Errorf("expected ErrEncrMgrNotReady, got %v", err)
	}
}

// TestGetInUseKeysCallback_AfterReady verifies keys are returned after MarkReady.
func TestGetInUseKeysCallback_AfterReady(t *testing.T) {
	e := newEncryptionMgrNoCbauth(testConfig(), nil)
	defer e.Close()

	e.SetInUseKeys("k1")
	e.SetInUseKeys("") // empty key (plaintext mode)
	e.MarkReady()

	keys, err := e.getInUseKeysCallback(LogdataKDT)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	found := make(map[string]bool)
	for _, k := range keys {
		found[k] = true
	}
	if !found["k1"] || !found[""] {
		t.Errorf("expected k1 and empty string, got %v", keys)
	}
}

// TestGetKeyCipherById_CacheHit verifies the cache is consulted.
func TestGetKeyCipherById_CacheHit(t *testing.T) {
	k1 := makeEaRKey("k1", "AES-256-GCM")
	seed := makeEncrKeysInfo("k1", k1)
	e := newEncryptionMgrNoCbauth(testConfig(), seed)
	defer e.Close()

	keyBytes, cipher := e.GetKeyCipherById("k1")
	if cipher != "AES-256-GCM" {
		t.Errorf("expected AES-256-GCM, got %v", cipher)
	}
	if string(keyBytes) != "k1" {
		t.Errorf("expected key bytes 'k1', got %v", string(keyBytes))
	}
}

// TestGetKeyCipherById_EmptyID verifies empty ID returns plaintext sentinel.
func TestGetKeyCipherById_EmptyID(t *testing.T) {
	e := newEncryptionMgrNoCbauth(testConfig(), nil)
	defer e.Close()

	keyBytes, cipher := e.GetKeyCipherById("")
	if cipher != CipherNameNone {
		t.Errorf("expected %v, got %v", CipherNameNone, cipher)
	}
	if len(keyBytes) != 0 {
		t.Errorf("expected empty key bytes, got %v", keyBytes)
	}
}

// TestGetActiveKeyIdCipher_EncryptionDisabled verifies disabled encryption returns empty.
func TestGetActiveKeyIdCipher_EncryptionDisabled(t *testing.T) {
	seed := makeEncrKeysInfo("") // ActiveKeyId == "" means disabled
	e := newEncryptionMgrNoCbauth(testConfig(), seed)
	defer e.Close()

	_, id, cipher := e.GetActiveKeyIdCipher()
	if id != "" || cipher != CipherNameNone {
		t.Errorf("expected empty id and CipherNameNone, got id=%v cipher=%v", id, cipher)
	}
}

// TestEnqueueUpdate_AllBufferedInWorkCh verifies that multiple updates with different
// key IDs are all buffered in workCh and processed in arrival order.
func TestEnqueueUpdate_AllBufferedInWorkCh(t *testing.T) {
	e := newEncryptionMgrNoCbauth(testConfig(), nil)
	defer e.Close()

	var mu sync.Mutex
	var processed []string

	e.SetStatsHooks(StatsHooks{
		UpdateActiveKey: func(key c.EaRKey) error {
			mu.Lock()
			processed = append(processed, key.Id)
			mu.Unlock()
			return nil
		},
	})

	kA := makeEaRKey("kA", "AES-256-GCM")
	kB := makeEaRKey("kB", "AES-256-GCM")
	kC := makeEaRKey("kC", "AES-256-GCM")

	e.enqueueUpdate(workItem{typ: workItemUpdate, activeKey: kA})
	e.enqueueUpdate(workItem{typ: workItemUpdate, activeKey: kB})
	e.enqueueUpdate(workItem{typ: workItemUpdate, activeKey: kC})

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	got := append([]string{}, processed...)
	mu.Unlock()

	if len(got) != 3 {
		t.Fatalf("expected 3 updates processed, got %v: %v", len(got), got)
	}
	if got[0] != "kA" || got[1] != "kB" || got[2] != "kC" {
		t.Errorf("expected [kA kB kC], got %v", got)
	}
}

// TestWorkerDispatch_Update verifies the update hook fires with the right key.
func TestWorkerDispatch_Update(t *testing.T) {
	e := newEncryptionMgrNoCbauth(testConfig(), nil)
	defer e.Close()

	done := make(chan c.EaRKey, 1)
	e.SetStatsHooks(StatsHooks{
		UpdateActiveKey: func(key c.EaRKey) error {
			done <- key
			return nil
		},
	})

	k1 := makeEaRKey("k1", "AES-256-GCM")
	e.enqueueUpdate(workItem{typ: workItemUpdate, activeKey: k1})

	select {
	case got := <-done:
		if got.Id != "k1" {
			t.Errorf("expected k1, got %v", got.Id)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout waiting for update hook")
	}
}

// TestWorkerDispatch_Drop_HookSuccess_RemovesInUse verifies a successful drop
// removes the key from in-use set.
func TestWorkerDispatch_Drop_HookSuccess_RemovesInUse(t *testing.T) {
	e := newEncryptionMgrNoCbauth(testConfig(), makeEncrKeysInfo("k2", makeEaRKey("k2", "AES-256-GCM")))
	defer e.Close()

	e.SetInUseKeys("k1")
	e.MarkReady()

	done := make(chan struct{}, 1)
	e.SetStatsHooks(StatsHooks{
		DropKey: func(active c.EaRKey, drop []string) error {
			done <- struct{}{}
			return nil
		},
	})

	k2 := makeEaRKey("k2", "AES-256-GCM")
	e.enqueueDrop(workItem{typ: workItemDrop, activeKey: k2, dropKeyIDs: []string{"k1"}})

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout waiting for drop hook")
	}
	time.Sleep(20 * time.Millisecond) // let advance() run

	e.MarkReady()
	keys, _ := e.getInUseKeysCallback(LogdataKDT)
	for _, k := range keys {
		if k == "k1" {
			t.Error("k1 should have been removed from in-use set")
		}
	}
}

// TestWorkerDispatch_Drop_HookFailure_KeepsInUse verifies a failed drop
// keeps the key in the in-use set.
func TestWorkerDispatch_Drop_HookFailure_KeepsInUse(t *testing.T) {
	e := newEncryptionMgrNoCbauth(testConfig(), makeEncrKeysInfo("k2", makeEaRKey("k2", "AES-256-GCM")))
	defer e.Close()

	e.SetInUseKeys("k1")
	e.MarkReady()

	done := make(chan struct{}, 1)
	e.SetStatsHooks(StatsHooks{
		DropKey: func(active c.EaRKey, drop []string) error {
			done <- struct{}{}
			return errors.New("drop failed")
		},
	})

	k2 := makeEaRKey("k2", "AES-256-GCM")
	e.enqueueDrop(workItem{typ: workItemDrop, activeKey: k2, dropKeyIDs: []string{"k1"}})

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout waiting for drop hook")
	}
	time.Sleep(20 * time.Millisecond)

	keys, _ := e.getInUseKeysCallback(LogdataKDT)
	found := false
	for _, k := range keys {
		if k == "k1" {
			found = true
		}
	}
	if !found {
		t.Error("k1 should still be in in-use set after failed drop")
	}
}
