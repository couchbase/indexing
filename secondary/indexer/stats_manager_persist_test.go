//go:build !community
// +build !community

// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package indexer

import (
	"slices"
	"testing"

	"github.com/couchbase/indexing/secondary/common"
)

const persistTestRounds = 100

const persistTestPlaintext = "plaintext"

// newPersistTestManager returns a stats manager whose stats file is on key k.
// It has no stats log, so a drop rewrites only the stats file.
func newPersistTestManager(t *testing.T, ks *rhcTestKeyStore, k EaRKey) (*statsManager, *FlatFileStatsPersister) {
	t.Helper()
	cb := ks.callbacks()
	fp := NewFlatFilePersister(t.TempDir(), 0, "stats", "stats_new")
	s := &statsManager{supvCmdch: make(MsgChannel, 1), statsPersister: fp}
	s.config.Store(common.Config{
		"log_dir":       common.ConfigValue{Value: ""},
		"statsLogFname": common.ConfigValue{Value: ""},
	})
	s.encCallbacks = StatsEncryptionCallbacks{
		getKeyCipherById: cb.getKeyCipherById,
		setInUseKeys:     cb.setInUseKeys,
	}

	respCh := make(chan error, 1)
	s.handleEncryptionUpdateKey(&MsgEncryptionUpdateKey{keyDataType: LogdataKDT, earkey: k, respCh: respCh})
	<-s.supvCmdch
	if err := <-respCh; err != nil {
		t.Fatalf("UpdateKey err:%v", err)
	}
	if err := fp.PersistStats(map[string]interface{}{"probe": int64(0)}); err != nil {
		t.Fatalf("PersistStats err:%v", err)
	}
	return s, fp
}

// persistDuringDrop starts a persist and a log key drop at the same time.
func persistDuringDrop(t *testing.T, s *statsManager, fp *FlatFileStatsPersister,
	active EaRKey, dropKeyIds []string) {
	t.Helper()
	start := make(chan struct{})
	persistErr := make(chan error, 1)
	go func() {
		<-start
		persistErr <- fp.PersistStats(map[string]interface{}{"probe": int64(1)})
	}()

	dropErr := make(chan error, 1)
	close(start)
	s.handleEncryptionDropKey(&MsgEncryptionDropKey{keyDataType: LogdataKDT, activeEarkey: active,
		dropKeyIds: dropKeyIds, respCh: dropErr})
	<-s.supvCmdch
	if err := <-dropErr; err != nil {
		t.Fatalf("drop of %v with active key %q err:%v", dropKeyIds, active.Id, err)
	}
	if err := <-persistErr; err != nil {
		t.Fatalf("PersistStats during drop of %v err:%v", dropKeyIds, err)
	}
}

// persistedStatsKey returns the key the stats file is encrypted with, or
// persistTestPlaintext. It fails if the file does not read back.
func persistedStatsKey(t *testing.T, fp *FlatFileStatsPersister, ks *rhcTestKeyStore) string {
	t.Helper()
	cb := ks.callbacks()
	reader := NewFlatFilePersister(fp.statsDir, 0, "stats", "stats_new")
	if _, err := reader.ReadPersistedStats(&StatsEncryptionCallbacks{getKeyCipherById: cb.getKeyCipherById}); err != nil {
		t.Fatalf("ReadPersistedStats err:%v", err)
	}
	encrypted, err := IsFileEncrypted(fp.filePath)
	if err != nil {
		t.Fatalf("IsFileEncrypted err:%v", err)
	}
	if !encrypted {
		return persistTestPlaintext
	}
	var keyID string
	if _, err = ReadEncryptedFile(fp.filePath, func(id []byte) []byte {
		keyID = string(id)
		key, _ := cb.getKeyCipherById(keyID)
		return key
	}, statsKDFLabelCtx, nil); err != nil {
		t.Fatalf("ReadEncryptedFile err:%v", err)
	}
	return keyID
}

func persistedStatsInUse(t *testing.T, s *statsManager) []string {
	t.Helper()
	respCh := make(chan map[KeyDataType][]string, 1)
	s.handleGetInUseKeys(&MsgEncryptionGetInuseKeys{keyDataType: LogdataKDT, respMapCh: respCh})
	<-s.supvCmdch
	return (<-respCh)[LogdataKDT]
}

func checkPersistedStatsKey(t *testing.T, s *statsManager, fp *FlatFileStatsPersister,
	ks *rhcTestKeyStore, round int, want string) {
	t.Helper()
	if got := persistedStatsKey(t, fp, ks); got != want {
		t.Fatalf("round %d: stats file key: got %v, want %v", round, got, want)
	}
	var wantInUse []string
	if want != persistTestPlaintext {
		wantInUse = []string{want}
	}
	if got := persistedStatsInUse(t, s); !slices.Equal(got, wantInUse) {
		t.Fatalf("round %d: in-use keys: got %v, want %v", round, got, wantInUse)
	}
}

// MB-74842: a persist overlapping a key rotation must not leave the stats file
// on the dropped key, corrupt it, or fail either side.
func TestPersistStatsDuringKeyRotation(t *testing.T) {
	k1, k2 := newRHCTestKey(t, "key1"), newRHCTestKey(t, "key2")
	ks := newRHCTestKeyStore(k1, k2)
	s, fp := newPersistTestManager(t, ks, k1)

	cur, next := k1, k2
	for i := 0; i < persistTestRounds; i++ {
		persistDuringDrop(t, s, fp, next, []string{cur.Id})
		checkPersistedStatsKey(t, s, fp, ks, i, next.Id)
		cur, next = next, cur
	}
}

// MB-74842: a persist overlapping a disable or enable must leave the stats file
// plaintext after the disable and encrypted after the enable.
func TestPersistStatsDuringEncryptionToggle(t *testing.T) {
	k1 := newRHCTestKey(t, "key1")
	ks := newRHCTestKeyStore(k1)
	s, fp := newPersistTestManager(t, ks, k1)
	off := EaRKey{Cipher: CipherNameNone}

	for i := 0; i < persistTestRounds; i++ {
		if i%2 == 0 {
			persistDuringDrop(t, s, fp, off, []string{k1.Id})
			checkPersistedStatsKey(t, s, fp, ks, i, persistTestPlaintext)
		} else {
			persistDuringDrop(t, s, fp, k1, []string{""})
			checkPersistedStatsKey(t, s, fp, ks, i, k1.Id)
		}
	}
}
