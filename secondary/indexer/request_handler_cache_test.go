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
	"crypto/rand"
	"encoding/json"
	"errors"
	"os"
	"path"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/iowrap"
	"github.com/couchbase/indexing/secondary/manager"
)

const rhcTestTimeout = 10 * time.Second

var rhcTestHostKey = Host2key("127.0.0.1:9102")

// rhcTestKeyStore stands in for EncryptionMgr: it serves key lookups and records the last key
// id the cache reported as in use.
type rhcTestKeyStore struct {
	mu        sync.Mutex
	keys      map[string]EaRKey
	lastInUse *string
}

func newRHCTestKeyStore(keys ...EaRKey) *rhcTestKeyStore {
	ks := &rhcTestKeyStore{keys: make(map[string]EaRKey)}
	for _, k := range keys {
		ks.keys[k.Id] = k
	}
	return ks
}

func newRHCTestKey(t *testing.T, id string) EaRKey {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("rand.Read err:%v", err)
	}
	return EaRKey{Id: id, Cipher: CipherNameAES256GCM, Key: key}
}

func (ks *rhcTestKeyStore) add(k EaRKey) {
	ks.mu.Lock()
	defer ks.mu.Unlock()
	ks.keys[k.Id] = k
}

func (ks *rhcTestKeyStore) remove(id string) {
	ks.mu.Lock()
	defer ks.mu.Unlock()
	delete(ks.keys, id)
}

// reportedInUse returns the last key id passed to setInUseKeys, and false if it was never called.
func (ks *rhcTestKeyStore) reportedInUse() (string, bool) {
	ks.mu.Lock()
	defer ks.mu.Unlock()
	if ks.lastInUse == nil {
		return "", false
	}
	return *ks.lastInUse, true
}

func (ks *rhcTestKeyStore) callbacks() CacheEncryptionCallbacks {
	return CacheEncryptionCallbacks{
		// Mirrors EncryptionMgr.getKeyCipherById, which returns an empty key and the None
		// cipher for an unknown key id.
		getKeyCipherById: func(keyId string) ([]byte, string) {
			ks.mu.Lock()
			defer ks.mu.Unlock()
			k, ok := ks.keys[keyId]
			if !ok {
				return []byte{}, CipherNameNone
			}
			return k.Key, k.Cipher
		},
		setInUseKeys: func(_ KeyDataType, keyId string) {
			ks.mu.Lock()
			defer ks.mu.Unlock()
			ks.lastInUse = &keyId
		},
	}
}

// startRHC opens a cache on dir and waits for its boot load from disk to finish. The returned
// stop shuts it down and also runs at test cleanup, so a failed test does not leak runPersistor.
func startRHC(t *testing.T, dir string, ks *rhcTestKeyStore) (*requestHandlerCache, func()) {
	t.Helper()
	rhc := NewRequestHandlerCache(dir)
	rhc.SetEncryptionCallbacks(ks.callbacks())
	var once sync.Once
	stop := func() { once.Do(rhc.Shutdown) }
	t.Cleanup(stop)
	flushRHC(t, rhc)
	return rhc, stop
}

// flushRHC waits until runPersistor has processed everything queued before it and returns the
// key ids the cache reports as in use. workCh is FIFO with a single consumer, and runPersistor
// loads the disk caches before it reads workCh, so the reply also means the boot load and every
// earlier write, key update and drop are done.
func flushRHC(t *testing.T, rhc *requestHandlerCache) []string {
	t.Helper()
	respCh := make(chan map[KeyDataType][]string, 1)
	rhc.workCh <- &workChEntry_getInUseKeys{kdt: LogdataKDT, respCh: respCh}
	select {
	case inUse := <-respCh:
		return inUse[LogdataKDT]
	case <-time.After(rhcTestTimeout):
		t.Fatalf("runPersistor did not drain workCh within %v", rhcTestTimeout)
	}
	return nil
}

// updateRHCKey queues a key update in order with the caller's other work.
// HandleEncryptionUpdateKey sends from a goroutine, so its order against later writes is not fixed.
func updateRHCKey(t *testing.T, rhc *requestHandlerCache, key EaRKey) {
	t.Helper()
	rhc.workCh <- &workChEntry_encryptUpdate{earkey: key, kdt: LogdataKDT}
	flushRHC(t, rhc)
}

func dropRHCKeys(t *testing.T, rhc *requestHandlerCache, activeKey EaRKey, dropKeyIds ...string) error {
	t.Helper()
	select {
	case err := <-rhc.HandleEncryptionDropKey(activeKey, dropKeyIds, LogdataKDT):
		return err
	case <-time.After(rhcTestTimeout):
		t.Fatalf("HandleEncryptionDropKey did not complete within %v", rhcTestTimeout)
	}
	return nil
}

// cacheRHCEntries caches one meta and one stats entry tagged with indexerId and waits for both
// to reach disk. ts must increase across calls, as older metadata is not cached.
func cacheRHCEntries(t *testing.T, rhc *requestHandlerCache, indexerId string, ts int64) {
	t.Helper()
	rhc.CacheLocalIndexMetadata(rhcTestHostKey,
		&manager.LocalIndexMetadata{IndexerId: indexerId, Timestamp: ts})
	stats := common.Statistics{"indexerId": indexerId}
	rhc.CacheStats(rhcTestHostKey, &stats)
	flushRHC(t, rhc)
}

// checkRHCLoaded fails unless rhc holds the meta and stats entries tagged with indexerId.
func checkRHCLoaded(t *testing.T, rhc *requestHandlerCache, indexerId string) {
	t.Helper()
	if meta := rhc.GetLocalIndexMetadataFromCache(rhcTestHostKey); meta == nil ||
		meta.IndexerId != indexerId {
		t.Errorf("meta cache: got %+v, want IndexerId %v", meta, indexerId)
	}
	if stats := rhc.GetStatsFromCache(rhcTestHostKey); stats == nil ||
		(*stats)["indexerId"] != indexerId {
		t.Errorf("stats cache: got %v, want indexerId %v", stats, indexerId)
	}
}

func checkRHCNotLoaded(t *testing.T, rhc *requestHandlerCache) {
	t.Helper()
	if meta := rhc.GetLocalIndexMetadataFromCache(rhcTestHostKey); meta != nil {
		t.Errorf("meta cache: got %+v, want nil", meta)
	}
	if stats := rhc.GetStatsFromCache(rhcTestHostKey); stats != nil {
		t.Errorf("stats cache: got %v, want nil", stats)
	}
}

type rhcTestFile struct {
	path     string
	kdfLabel []byte
}

func rhcTestFiles(dir string) []rhcTestFile {
	return []rhcTestFile{
		{path.Join(dir, "meta", rhcTestHostKey), cacheMetaKDFLabel},
		{path.Join(dir, "stats", rhcTestHostKey), cacheStatsKDFLabel},
	}
}

// checkRHCFilesSealedWith fails unless both cache files are encrypted and decrypt with key alone.
func checkRHCFilesSealedWith(t *testing.T, dir string, key EaRKey) {
	t.Helper()
	onlyKey := func(keyId []byte) []byte {
		if string(keyId) == key.Id {
			return key.Key
		}
		return nil
	}
	for _, f := range rhcTestFiles(dir) {
		if enc, err := IsFileEncrypted(f.path); err != nil || !enc {
			t.Errorf("%v: encrypted=%v err:%v, want encrypted", f.path, enc, err)
			continue
		}
		if _, err := ReadEncryptedFile(f.path, onlyKey, f.kdfLabel, nil); err != nil {
			t.Errorf("%v: not readable with key %v alone, err:%v", f.path, key.Id, err)
		}
	}
}

func checkRHCFilesPlaintext(t *testing.T, dir string) {
	t.Helper()
	for _, f := range rhcTestFiles(dir) {
		if enc, err := IsFileEncrypted(f.path); err != nil || enc {
			t.Errorf("%v: encrypted=%v err:%v, want plaintext", f.path, enc, err)
			continue
		}
		content, err := iowrap.Ioutil_ReadFile(f.path)
		if err != nil || !json.Valid(content) {
			t.Errorf("%v: want plaintext JSON, err:%v", f.path, err)
		}
	}
}

func checkRHCInUse(t *testing.T, rhc *requestHandlerCache, want ...string) {
	t.Helper()
	if got := flushRHC(t, rhc); !slices.Equal(got, want) {
		t.Errorf("in-use keys: got %v, want %v", got, want)
	}
}

// MB-74418: readFile passed a nil KDF label to ReadEncryptedFile, which gocbcrypto rejects, so
// no encrypted cache file ever loaded at boot.
func TestRequestHandlerCacheEncryptedLoadsAtBoot(t *testing.T) {
	dir := t.TempDir()
	k1 := newRHCTestKey(t, "log-dek-1")
	ks := newRHCTestKeyStore(k1)

	rhc, stop := startRHC(t, dir, ks)
	updateRHCKey(t, rhc, k1)
	cacheRHCEntries(t, rhc, "idx-1", 1)
	checkRHCFilesSealedWith(t, dir, k1)
	stop()

	rhc, _ = startRHC(t, dir, ks)
	checkRHCLoaded(t, rhc, "idx-1")

	// Boot restores the write contexts from the files, so writes made before the next key
	// update stay encrypted.
	checkRHCInUse(t, rhc, k1.Id)
	cacheRHCEntries(t, rhc, "idx-2", 2)
	checkRHCFilesSealedWith(t, dir, k1)
}

func TestRequestHandlerCachePlaintextLoadsAtBoot(t *testing.T) {
	dir := t.TempDir()
	ks := newRHCTestKeyStore()

	rhc, stop := startRHC(t, dir, ks)
	cacheRHCEntries(t, rhc, "idx-1", 1)
	checkRHCFilesPlaintext(t, dir)
	stop()

	rhc, _ = startRHC(t, dir, ks)
	checkRHCLoaded(t, rhc, "idx-1")
	checkRHCInUse(t, rhc)
}

// A file whose key is unavailable is skipped at boot, but the cache must still become ready and
// the file must stay on disk so it loads once the key is back.
func TestRequestHandlerCacheKeyUnavailableAtBoot(t *testing.T) {
	dir := t.TempDir()
	k1 := newRHCTestKey(t, "log-dek-1")
	ks := newRHCTestKeyStore(k1)

	rhc, stop := startRHC(t, dir, ks)
	updateRHCKey(t, rhc, k1)
	cacheRHCEntries(t, rhc, "idx-1", 1)
	stop()

	ks.remove(k1.Id)
	rhc, stop = startRHC(t, dir, ks)
	checkRHCNotLoaded(t, rhc)
	if _, ready := rhc.GetAllCachedLocalIndexMetadata(); !ready {
		t.Errorf("metaCacheReady: got false, want true")
	}
	rhc.statsMutex.RLock()
	statsReady := rhc.statsCacheReady
	rhc.statsMutex.RUnlock()
	if !statsReady {
		t.Errorf("statsCacheReady: got false, want true")
	}
	checkRHCFilesSealedWith(t, dir, k1)
	stop()

	ks.add(k1)
	rhc, _ = startRHC(t, dir, ks)
	checkRHCLoaded(t, rhc, "idx-1")
}

func TestRequestHandlerCacheReadFileWithoutKeyLookup(t *testing.T) {
	dir := t.TempDir()
	k1 := newRHCTestKey(t, "log-dek-1")

	rhc, stop := startRHC(t, dir, newRHCTestKeyStore(k1))
	updateRHCKey(t, rhc, k1)
	cacheRHCEntries(t, rhc, "idx-1", 1)
	stop()

	noLookup := &requestHandlerCache{}
	f := rhcTestFiles(dir)[0]
	if _, _, err := noLookup.readFile(f.path, f.kdfLabel); !errors.Is(err, errCacheEncryptedNoKeyLookup) {
		t.Errorf("readFile err:%v, want %v", err, errCacheEncryptedNoKeyLookup)
	}
}

// Drop scenario 1: turning encryption on encrypts the existing plaintext files.
func TestRequestHandlerCacheEnableEncryption(t *testing.T) {
	dir := t.TempDir()
	k1 := newRHCTestKey(t, "log-dek-1")
	ks := newRHCTestKeyStore(k1)

	rhc, stop := startRHC(t, dir, ks)
	cacheRHCEntries(t, rhc, "idx-1", 1)
	checkRHCFilesPlaintext(t, dir)

	// ns_server drops the null DEK ("") to retire unencrypted data.
	updateRHCKey(t, rhc, k1)
	if err := dropRHCKeys(t, rhc, k1, ""); err != nil {
		t.Fatalf("drop null DEK err:%v", err)
	}
	checkRHCFilesSealedWith(t, dir, k1)
	checkRHCInUse(t, rhc, k1.Id)
	if got, ok := ks.reportedInUse(); !ok || got != k1.Id {
		t.Errorf("setInUseKeys: got %q (called=%v), want %q", got, ok, k1.Id)
	}
	stop()

	rhc, _ = startRHC(t, dir, ks)
	checkRHCLoaded(t, rhc, "idx-1")
}

// Drop scenario 3: rotating the key re-encrypts the files so the old key can be dropped.
func TestRequestHandlerCacheRotateKey(t *testing.T) {
	dir := t.TempDir()
	k1 := newRHCTestKey(t, "log-dek-1")
	k2 := newRHCTestKey(t, "log-dek-2")
	ks := newRHCTestKeyStore(k1, k2)

	rhc, stop := startRHC(t, dir, ks)
	updateRHCKey(t, rhc, k1)
	cacheRHCEntries(t, rhc, "idx-1", 1)

	// A key update only switches the write context, the files stay on k1 until the drop.
	updateRHCKey(t, rhc, k2)
	checkRHCFilesSealedWith(t, dir, k1)

	if err := dropRHCKeys(t, rhc, k2, k1.Id); err != nil {
		t.Fatalf("drop %v err:%v", k1.Id, err)
	}
	checkRHCFilesSealedWith(t, dir, k2)
	checkRHCInUse(t, rhc, k2.Id)
	stop()

	ks.remove(k1.Id)
	rhc, _ = startRHC(t, dir, ks)
	checkRHCLoaded(t, rhc, "idx-1")
	checkRHCInUse(t, rhc, k2.Id)
}

// Drop scenario 2: turning encryption off decrypts the files back to plaintext.
func TestRequestHandlerCacheDisableEncryption(t *testing.T) {
	dir := t.TempDir()
	k1 := newRHCTestKey(t, "log-dek-1")
	ks := newRHCTestKeyStore(k1)

	rhc, stop := startRHC(t, dir, ks)
	updateRHCKey(t, rhc, k1)
	cacheRHCEntries(t, rhc, "idx-1", 1)

	// EncryptionMgr sends the None cipher when encryption is turned off. The write contexts are
	// kept until the drop, otherwise the drop would skip decrypting the files.
	updateRHCKey(t, rhc, EaRKey{Cipher: CipherNameNone})
	checkRHCInUse(t, rhc, k1.Id)
	checkRHCFilesSealedWith(t, dir, k1)

	if err := dropRHCKeys(t, rhc, EaRKey{Cipher: CipherNameNone}, k1.Id); err != nil {
		t.Fatalf("drop %v err:%v", k1.Id, err)
	}
	checkRHCFilesPlaintext(t, dir)
	checkRHCInUse(t, rhc)

	// Once plaintext, new writes stay plaintext and a repeated drop is a no-op.
	cacheRHCEntries(t, rhc, "idx-2", 2)
	checkRHCFilesPlaintext(t, dir)
	if err := dropRHCKeys(t, rhc, EaRKey{Cipher: CipherNameNone}, k1.Id); err != nil {
		t.Fatalf("repeated drop %v err:%v", k1.Id, err)
	}
	checkRHCFilesPlaintext(t, dir)
	stop()

	ks.remove(k1.Id)
	rhc, _ = startRHC(t, dir, ks)
	checkRHCLoaded(t, rhc, "idx-2")
}

// A leftover .tmp file from an interrupted write is neither encrypted nor loaded at boot.
func TestRequestHandlerCacheTempFilesIgnored(t *testing.T) {
	dir := t.TempDir()
	k1 := newRHCTestKey(t, "log-dek-1")
	ks := newRHCTestKeyStore(k1)

	rhc, stop := startRHC(t, dir, ks)
	cacheRHCEntries(t, rhc, "idx-1", 1)

	// Valid metadata, so it would load under its file name if boot did not skip it. It belongs
	// to another host so no write in this test reuses its path.
	tempKey := Host2key("127.0.0.1:9999") + TEMP_FILE_SUFFIX
	temp := path.Join(dir, "meta", tempKey)
	content, err := json.Marshal(&manager.LocalIndexMetadata{IndexerId: "idx-tmp", Timestamp: 1})
	if err != nil {
		t.Fatalf("json.Marshal err:%v", err)
	}
	if err = os.WriteFile(temp, content, cacheFileMode); err != nil {
		t.Fatalf("WriteFile %v err:%v", temp, err)
	}

	updateRHCKey(t, rhc, k1)
	if err = dropRHCKeys(t, rhc, k1, ""); err != nil {
		t.Fatalf("drop null DEK err:%v", err)
	}
	checkRHCFilesSealedWith(t, dir, k1)
	if enc, err := IsFileEncrypted(temp); err != nil || enc {
		t.Errorf("%v: encrypted=%v err:%v, want left as plaintext", temp, enc, err)
	}
	stop()

	rhc, _ = startRHC(t, dir, ks)
	checkRHCLoaded(t, rhc, "idx-1")
	if meta := rhc.GetLocalIndexMetadataFromCache(tempKey); meta != nil {
		t.Errorf("temp file loaded into meta cache: %+v", meta)
	}
}
