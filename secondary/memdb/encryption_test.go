package memdb

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/couchbase/gocbcrypto"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/stretchr/testify/assert"
)

// verifies that the dir op guard can be used to cancel long-running operations.
func testDirOpGuardWithCancel(t *testing.T, conf Config) {
	guard := newDirOpGuard()
	ctx := context.Background()
	snapDir := "snapdir"

	// cancellable operation
	rencryptOp, _ := guard.TryAcquire(snapDir, ctx)
	assert.NotNil(t, rencryptOp)

	go func() {
		defer guard.Release(rencryptOp)
		for {
			select {
			case <-rencryptOp.cancelCtx.Done():
				rencryptOp.done()
				return
			default:
				time.Sleep(time.Second) // slow encryption
			}
		}
	}()

	// non-cancellable operation
	cleanupOp := guard.Acquire(snapDir, ctx)
	assert.NotNil(t, cleanupOp)
	guard.Release(cleanupOp)
	assert.Equal(t, 0, len(guard.operations))
}

// verifies that the dir op guard can be used to preempt concurrent operations.
func testDirOpGuardWithMultiplePremption(t *testing.T, cfg Config) {
	guard := newDirOpGuard()
	ctx := context.Background()
	snapDir := "snapdir"

	rencryptOp, _ := guard.TryAcquire(snapDir, ctx)
	assert.NotNil(t, rencryptOp)

	var preempted atomic.Bool
	go func() {
		select {
		case <-rencryptOp.cancelCtx.Done():
			rencryptOp.done()
		}
		time.Sleep(50 * time.Millisecond)
		preempted.Store(true)
		guard.Release(rencryptOp)
	}()

	time.Sleep(10 * time.Millisecond)

	// multiple preemption attempts
	var wg sync.WaitGroup
	const numPreempters = 5

	for i := 0; i < numPreempters; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			cleanupOp := guard.Acquire(snapDir, ctx)
			assert.NotNil(t, cleanupOp)
			time.Sleep(10 * time.Millisecond)
			guard.Release(cleanupOp)
		}(i)
	}

	wg.Wait()
	assert.True(t, preempted.Load())
	assert.Equal(t, 0, len(guard.operations))
}

// tests disk usage between encrypted and unencrypted MemDB persistence.
// and checks encryption does not produce unexpected divergence in disk size
func testEncryptedVsUnencryptedDiskSize(t *testing.T, testConf Config) {
	defer ValidateNoMemLeaks()

	var itmDataSz uint64
	callb := func(itm *ItemEntry) {
		atomic.AddUint64(&itmDataSz, uint64(ItemSize(unsafe.Pointer(itm.Item()))))
	}

	fn := func(dir string, testConf Config) (int64, int64) {
		os.RemoveAll(dir)
		var wg sync.WaitGroup

		testConf.Path = dir
		db, err := NewWithEncryptionConfig(testConf, nil)
		assert.NoError(t, err)
		n := (1000000 / runtime.GOMAXPROCS(0)) * runtime.GOMAXPROCS(0)
		t0 := time.Now()
		for i := 0; i < runtime.GOMAXPROCS(0); i++ {
			wg.Add(1)
			w := db.NewWriter()
			go doInsertSafe(w, &wg, n/runtime.GOMAXPROCS(0), true)
		}
		wg.Wait()
		fmt.Printf("Inserting %v items took %v\n", n, time.Since(t0))

		snap, _ := db.NewSnapshot()
		t.Log("snap item count", snap.count)
		snap.Open()
		keyId, cipher, _ := db.RegisterSnapshotKeyId(dir)
		assert.NoError(t, db.PreparePersistence(dir, snap, keyId, cipher))
		assert.NoError(t, db.StoreToDisk(dir, snap, runtime.GOMAXPROCS(0), keyId, cipher, callb))
		dataSz1 := itmDataSz
		itmDataSz = 0
		snap.Close()

		du, _ := common.DiskUsage(testConf.Path)
		assert.GreaterOrEqual(t, db.aggrStoreStats().Memory, du)
		db.Close()

		db, err = NewWithEncryptionConfig(testConf, []string{dir})
		assert.NoError(t, err)
		defer db.Close()

		snap2, err := db.LoadFromDisk(dir, runtime.GOMAXPROCS(0), callb)
		assert.NoError(t, err)
		defer snap2.Close()
		assert.Equal(t, snap.count, snap2.count)
		dataSz2 := itmDataSz
		itmDataSz = 0
		assert.Equal(t, dataSz1, dataSz2)
		du2, _ := common.DiskUsage(testConf.Path)
		assert.Equal(t, du, du2)

		t.Logf("data size:%v disk size:%v", dataSz1, du)
		return snap.count, du
	}

	itemCount1, du1 := fn("db.dump", testConf)

	// disable encryption
	testConf.GetKeyById = nil
	itemCount2, du2 := fn("db.dump", testConf)

	// compare
	assert.Equal(t, itemCount1, itemCount2)
	t.Log(du1, du2)
}

// tests disk usage between encrypted and unencrypted MemDB persistence with delta files.
func testEncryptedVsUnencryptedDiskSizeWithDeltaFiles(t *testing.T, testConf Config) {
	defer ValidateNoMemLeaks()

	testConf.UseDeltaInterleaving()

	// get disk usage
	getDiskUsage := func(dir string, testConf Config) (int64, int64) {
		os.RemoveAll(dir)
		var wg sync.WaitGroup

		testConf.Path = dir
		db, err := NewWithEncryptionConfig(testConf, nil)
		assert.NoError(t, err)
		n := 1000000

		// insert n items
		wg.Add(1)
		w := db.NewWriter()
		doInsertSafe(w, &wg, n, false)
		wg.Wait()

		snap, _ := db.NewSnapshot()
		// delete n items. This adds dead items to the writer gc list but does not delete them from the index.
		wg.Add(1)
		doDeleteSafe(w, &wg, n)
		wg.Wait()

		snap.Open()
		keyId, cipher, _ := db.RegisterSnapshotKeyId(dir)
		assert.NoError(t, db.PreparePersistence(dir, snap, keyId, cipher))
		snap.Close() // this updates snapshot gc list

		// This triggers GC which flushes the gc list and removes the dead items from the index
		assert.NoError(t, db.StoreToDisk(dir, snap, runtime.GOMAXPROCS(0), keyId, cipher, nil))
		du, _ := common.DiskUsage(testConf.Path)
		db.Close()

		db, err = NewWithEncryptionConfig(testConf, []string{dir})
		assert.NoError(t, err)
		defer db.Close()

		snap2, err := db.LoadFromDisk(dir, runtime.GOMAXPROCS(0), nil)
		assert.NoError(t, err)
		defer snap2.Close()
		assert.Equal(t, snap.count, snap2.count)

		du2, _ := common.DiskUsage(testConf.Path)
		assert.Equal(t, du, du2)
		return snap.count, du
	}

	// with encryption
	itmCnt1, du1 := getDiskUsage("db.dump", testConf)

	// disable encryption
	testConf.GetKeyById = nil
	itmCnt2, du2 := getDiskUsage("db.dump", testConf)

	// compare
	t.Log(du1, du2)
	if float64(du1) > float64(du2)*1.10 {
		t.Errorf("du1 (%d) is more than 10%% greater than du2 (%d)", du1, du2)
	}
	assert.Equal(t, itmCnt1, itmCnt2)
}

// verifies, active key IDs extracted from each snapshot and unique before and after recovery.
func testEncryptionGetActiveKeyIds(t *testing.T, testConf Config) {
	defer ValidateNoMemLeaks()

	os.RemoveAll("db.dump")
	var wg sync.WaitGroup

	testConf.Path = "db.dump"
	db, err := NewWithEncryptionConfig(testConf, nil)
	assert.NoError(t, err)
	n := 1000000
	t0 := time.Now()
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		wg.Add(1)
		w := db.NewWriter()
		go doInsertSafe(w, &wg, n/runtime.GOMAXPROCS(0), true)
	}
	wg.Wait()
	fmt.Printf("Inserting %v items took %v\n", n, time.Since(t0))

	for i := 0; i < 2; i++ {
		snap, _ := db.NewSnapshot()
		snapPath := filepath.Join(db.Path, fmt.Sprintf("snap:%v", i))
		snap.Open()
		keyId, cipher, _ := db.RegisterSnapshotKeyId(snapPath)
		if err = db.PreparePersistence(snapPath, snap, keyId, cipher); err == nil {
			err = db.StoreToDisk(snapPath, snap, 8, keyId, cipher, nil)
		}
		if err != nil {
			t.Error(err)
		}
		snap.Close()
	}

	if t.Failed() {
		db.Close()
		return
	}

	var preRecoveryKeyIds = make([][]byte, 0)
	var snapDirs []string
	for i := 0; i < 2; i++ {
		snapPath := filepath.Join(db.Path, fmt.Sprintf("snap:%v", i))
		snapDirs = append(snapDirs, snapPath)
		keyIds, err := db.getActiveKeyIdsFromSnapshot(snapPath)
		assert.NoError(t, err)
		// Store only unique keyIds in preRecoveryKeyIds
		for _, kid := range keyIds {
			unique := true
			for _, existing := range preRecoveryKeyIds {
				if bytes.Equal(existing, kid) {
					unique = false
					break
				}
			}
			if unique {
				preRecoveryKeyIds = append(preRecoveryKeyIds, kid)
			}
		}
	}

	if len(preRecoveryKeyIds) != 1 {
		t.Log(preRecoveryKeyIds)
		t.Error("expected 1 key id, got", len(preRecoveryKeyIds))
	}

	db.Close()

	db, err = NewWithEncryptionConfig(testConf, snapDirs)
	assert.NoError(t, err)
	defer db.Close()

	for i := 0; i < 2; i++ {
		snapPath := filepath.Join(db.Path, fmt.Sprintf("snap:%v", i))
		keyIds, err2 := db.getActiveKeyIdsFromSnapshot(snapPath)
		assert.NoError(t, err2)
		for _, kid := range keyIds {
			found := false
			for _, preKid := range preRecoveryKeyIds {
				if bytes.Equal(kid, preKid) {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("unexpected key id found after recovery: %v", kid)
			}
		}
	}
}

// verifies active key IDs from delta files
func testEncryptionGetActiveKeyIdsWithDeltaFiles(t *testing.T, conf Config) {
	os.RemoveAll("db.dump")
	conf.Path = "db.dump"
	conf.UseDeltaInterleaving()
	db, err := NewWithEncryptionConfig(conf, nil)
	assert.NoError(t, err)

	var writers []*Writer
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		writers = append(writers, db.NewWriter())
	}

	n := 1000000
	chunk := n / runtime.GOMAXPROCS(0)
	version := 0

	doMutate := func() *Snapshot {
		var wg sync.WaitGroup
		version++
		for i := 0; i < runtime.GOMAXPROCS(0); i++ {
			wg.Add(1)
			start := i * chunk
			end := start + chunk
			go doUpdate(db, &wg, writers[i], start, end, version)
		}
		wg.Wait()

		snap, _ := db.NewSnapshot()
		return snap
	}

	var snap, snapw *Snapshot
	for x := 0; x < 2; x++ {
		if snap != nil {
			snap.Close()
		}
		snap = doMutate()
	}

	waiter := make(chan bool)
	var wg2 sync.WaitGroup
	wg2.Add(1)
	go func() {
		defer wg2.Done()

		for x := 0; x < 10; x++ {
			if snapw != nil {
				snapw.Close()
			}

			snapw = doMutate()
			if x == 0 {
				close(waiter)
			}
		}

		snap.Close()
		count := db.gcsnapshots.GetStats().NodeCount

		for count > 5 {
			time.Sleep(time.Second)
			count = db.gcsnapshots.GetStats().NodeCount
		}
	}()

	callb := func(itm *ItemEntry) {
		<-waiter
	}

	snapPath := filepath.Join(db.Path, fmt.Sprintf("snap"))
	snap.Open()
	keyId, cipher, _ := db.RegisterSnapshotKeyId(snapPath)
	if err := db.PreparePersistence(snapPath, snap, keyId, cipher); err != nil {
		t.Errorf("Error while preparing %v", err)
		return
	}

	err = db.StoreToDisk(snapPath, snap, 8, keyId, cipher, callb)
	assert.NoError(t, err)

	wg2.Wait()
	snapw.Close()

	keyIds, err := db.getActiveKeyIdsFromSnapshot(snapPath)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(keyIds))

	db.Close()

	if t.Failed() {
		return
	}

	db, err = NewWithEncryptionConfig(conf, []string{snapPath})
	assert.NoError(t, err)
	defer db.Close()

	// Verify that the key ids are the same after recovery
	keyIds2, err2 := db.getActiveKeyIdsFromSnapshot(snapPath)
	assert.NoError(t, err2)
	assert.ElementsMatch(t, keyIds, keyIds2)
}

// tests with different snapshots.
func testEncryptionGetActiveKeyIdsManySnapshots(t *testing.T, testConf Config) {
	defer ValidateNoMemLeaks()

	os.RemoveAll("db.dump")

	testConf.Path = "db.dump"
	db, err := NewWithEncryptionConfig(testConf, nil)
	assert.NoError(t, err)
	defer db.Close()

	n := 100000
	var wg sync.WaitGroup
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		wg.Add(1)
		w := db.NewWriter()
		go doInsertSafe(w, &wg, n/runtime.GOMAXPROCS(0), true)
	}
	wg.Wait()

	// Get the initial key ID used for encryption
	initialKeyId, _ := db.GetCurrentKeyId()
	assert.NotNil(t, initialKeyId, "Initial key ID should not be nil")

	// Create multiple snapshots with the same key
	var snapPaths []string
	numSnaps := 3
	for i := 0; i < numSnaps; i++ {
		snap, err := db.NewSnapshot()
		assert.NoError(t, err)

		snapPath := filepath.Join(db.Path, fmt.Sprintf("snap:%v", i))
		snapPaths = append(snapPaths, snapPath)

		snap.Open()
		keyId, cipher, _ := db.RegisterSnapshotKeyId(snapPath)
		err = db.PreparePersistence(snapPath, snap, keyId, cipher)
		assert.NoError(t, err)

		err = db.StoreToDisk(snapPath, snap, 8, keyId, cipher, nil)
		assert.NoError(t, err)

		snap.Close()
	}

	if t.Failed() {
		return
	}

	t.Run("NonExistentSnapshot", func(t *testing.T) {
		invalidPaths := []string{filepath.Join(db.Path, "nonexistent")}
		_, _, err := db.getActiveKeyIdsFromSnapshots(invalidPaths)
		assert.Error(t, err)
	})

	t.Run("MixValidAndInvalid", func(t *testing.T) {
		mixedPaths := append([]string{snapPaths[0]}, filepath.Join(db.Path, "invalid"))
		_, _, err := db.getActiveKeyIdsFromSnapshots(mixedPaths)
		assert.Error(t, err)
	})

	t.Run("DuplicateSnapshots", func(t *testing.T) {
		duplicatePaths := append(snapPaths, snapPaths[0], snapPaths[1])
		keyIds, _, err := db.getActiveKeyIdsFromSnapshots(duplicatePaths)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(keyIds))
		assert.NotEmpty(t, keyIds[0])
	})

	// Create a new key and rotate one snapshot
	t.Run("SnapshotsWithDifferentKeys", func(t *testing.T) {
		newKeyId := []byte("new-test-key-id-12345678")
		masterKey, _, _ := db.GetKeyById(newKeyId)
		err := db.SetCurrentEncryptionKey(masterKey, newKeyId, gocbcrypto.CipherNameAES256GCM)
		assert.NoError(t, err)

		// Rotate the first snapshot to use the new key
		err = db.DropKeyIdsFromSnapshot([][]byte{initialKeyId}, snapPaths[0])
		assert.NoError(t, err)

		// Now we should have 2 different keys across all snapshots
		keyIds, _, err := db.getActiveKeyIdsFromSnapshots(snapPaths)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(keyIds))

		// Verify both keys are present
		foundInitial := false
		foundNew := false
		for _, kid := range keyIds {
			if bytes.Equal(kid, initialKeyId) {
				foundInitial = true
			}
			if bytes.Equal(kid, newKeyId) {
				foundNew = true
			}
		}
		assert.True(t, foundInitial, "Initial key should be present in unrotated snapshots")
		assert.True(t, foundNew, "New key should be present in rotated snapshot")

		// Test with only rotated snapshot
		keyIds, _, err = db.getActiveKeyIdsFromSnapshots([]string{snapPaths[0]})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(keyIds), "Rotated snapshot should have 1 key")
		assert.NotEmpty(t, keyIds[0])
		assert.True(t, bytes.Equal(newKeyId, keyIds[0]), "Should be the new key")

		// Test with only unrotated snapshots
		keyIds, _, err = db.getActiveKeyIdsFromSnapshots(snapPaths[1:])
		assert.NoError(t, err)
		assert.Equal(t, 1, len(keyIds), "Unrotated snapshots should have 1 key")
		assert.NotEmpty(t, keyIds[0])
		assert.True(t, bytes.Equal(initialKeyId, keyIds[0]), "Should be the initial key")
	})

	t.Run("SnapshotsWithNoKeys", func(t *testing.T) {
		err := db.SetCurrentEncryptionKey(nil, nil, gocbcrypto.CipherNameNone)
		assert.NoError(t, err)

		for i := range snapPaths {
			keyIds, err := db.getActiveKeyIdsFromSnapshot(snapPaths[i])
			assert.NoError(t, err)
			err = db.DropKeyIdsFromSnapshot(keyIds, snapPaths[i])
			assert.NoError(t, err)
		}

		keyIds, _, err := db.getActiveKeyIdsFromSnapshots(snapPaths)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(keyIds))
		assert.Equal(t, 0, len(keyIds[0]))
	})
}

// test disabling encryption and then getting active key ids from encrypted and unencrypted snapshots.
func testEncryptionGetActiveKeyIdsEncryptedUnencryptedSnapshot(t *testing.T, testConf Config) {
	defer ValidateNoMemLeaks()

	os.RemoveAll("db.dump")

	testConf.Path = "db.dump"
	db, err := NewWithEncryptionConfig(testConf, nil)
	assert.NoError(t, err)
	defer db.Close()

	n := 100000
	var wg sync.WaitGroup
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		wg.Add(1)
		w := db.NewWriter()
		go doInsertSafe(w, &wg, n/runtime.GOMAXPROCS(0), true)
	}
	wg.Wait()

	var snapPaths []string
	snap, err := db.NewSnapshot()
	assert.NoError(t, err)

	defer snap.Close()

	snapPath := filepath.Join(db.Path, fmt.Sprintf("snap:%v", 0))
	snapPaths = append(snapPaths, snapPath)

	keyId, cipher, _ := db.RegisterSnapshotKeyId(snapPath)
	snap.Open()
	err = db.PreparePersistence(snapPath, snap, keyId, cipher)
	assert.NoError(t, err)

	err = db.StoreToDisk(snapPath, snap, 8, keyId, cipher, nil)
	assert.NoError(t, err)

	err = db.SetCurrentEncryptionKey(nil, nil, gocbcrypto.CipherNameNone)
	assert.NoError(t, err)

	snapPath2 := filepath.Join(db.Path, fmt.Sprintf("snap:%v", 1))
	snapPaths = append(snapPaths, snapPath2)

	keyId, cipher, _ = db.RegisterSnapshotKeyId(snapPath2)
	snap.Open()
	err = db.PreparePersistence(snapPath2, snap, keyId, cipher)
	assert.NoError(t, err)

	err = db.StoreToDisk(snapPath2, snap, 8, keyId, cipher, nil)
	assert.NoError(t, err)

	snap.Close()

	keyIds := db.GetActiveKeyIdList()
	assert.Equal(t, 2, len(keyIds))

	cached, _ := db.GetEncryptionStatsCached()
	assert.Equal(t, StatusPartEncrypted, cached.Status)
}

// verfies that concurrent snapshot removal and get active key ids operations can be performed safely.
func testEncryptionGetActiveKeyIdsWithConcurrentRemoveSnapshot(t *testing.T, testConf Config) {
	defer ValidateNoMemLeaks()

	os.RemoveAll("db.dump")
	var wg sync.WaitGroup

	testConf.Path = "db.dump"
	db, err := NewWithEncryptionConfig(testConf, nil)
	assert.NoError(t, err)
	defer db.Close()

	n := 1000000
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		wg.Add(1)
		w := db.NewWriter()
		go doInsertSafe(w, &wg, n/runtime.GOMAXPROCS(0), true)
	}
	wg.Wait()

	for i := 0; i < 2; i++ {
		snap, _ := db.NewSnapshot()
		snapPath := filepath.Join(db.Path, fmt.Sprintf("snap:%v", i))
		snap.Open()
		keyId, cipher, _ := db.RegisterSnapshotKeyId(snapPath)
		err = db.PreparePersistence(snapPath, snap, keyId, cipher)
		assert.NoError(t, err)
		err = db.StoreToDisk(snapPath, snap, 8, keyId, cipher, nil)
		assert.NoError(t, err)
		snap.Close()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 2; i++ {
			snapPath := filepath.Join(db.Path, fmt.Sprintf("snap:%v", i))
			_, err := db.getActiveKeyIdsFromSnapshot(snapPath)
			t.Log(err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 2; i++ {
			snapPath := filepath.Join(db.Path, fmt.Sprintf("snap:%v", i))
			assert.NoError(t, db.RemoveSnapshot(snapPath))
		}
	}()

	wg.Wait()

	for i := 0; i < 2; i++ {
		snapPath := filepath.Join(db.Path, fmt.Sprintf("snap:%v", i))
		_, err := os.Stat(snapPath)
		assert.True(t, os.IsNotExist(err))
	}
}

// verifies key ids dropped from a snapshot and then re-encrypted
func testEncryptionDropKeyIdsFromSnapshot(t *testing.T, conf Config) {
	os.RemoveAll("db.dump")
	conf.Path = "db.dump"
	conf.UseDeltaInterleaving()
	db, err := NewWithEncryptionConfig(conf, nil)
	assert.NoError(t, err)

	var wg sync.WaitGroup
	n := 1000000

	// insert n items
	wg.Add(1)
	w := db.NewWriter()
	doInsertSafe(w, &wg, n, false)
	wg.Wait()

	snap, _ := db.NewSnapshot()
	// delete n items. This adds dead items to the writer gc list but does not delete them from the index.
	wg.Add(1)
	doDeleteSafe(w, &wg, n)
	wg.Wait()

	snap.Open()
	keyId, cipher, _ := db.RegisterSnapshotKeyId(conf.Path)
	assert.NoError(t, db.PreparePersistence(conf.Path, snap, keyId, cipher))
	snap.Close() // this updates snapshot gc list

	// This triggers GC which flushes the gc list and removes the dead items from the index
	assert.NoError(t, db.StoreToDisk(conf.Path, snap, runtime.GOMAXPROCS(0), keyId, cipher, nil))

	// get key ids from snapshot before dropping
	keyIds, err := db.getActiveKeyIdsFromSnapshot(conf.Path)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(keyIds))

	// rotate key ids from snapshot

	oldId := db.encKeyId
	oldKey := db.GetEncryptionKeyById(oldId)

	keyIds = make([][]byte, 0)
	bs := make([]byte, len(db.encKeyId))
	copy(bs, db.encKeyId)
	keyIds = append(keyIds, bs)
	t.Log("oldkeyId", oldId)

	newKey := make([]byte, gocbcrypto.AES_256_GCM_KEY_SZ)
	r := rand.New(rand.NewSource(0))
	r.Read(newKey)

	newId := make([]byte, gocbcrypto.AES_256_GCM_KEY_SZ)
	r.Read(newId)
	t.Log("newkeyId", newId)

	// set new key id and cipher
	conf.GetKeyById = func(id []byte) ([]byte, []byte, string) {
		if bytes.Equal(id, oldId) {
			return oldKey, id, gocbcrypto.CipherNameAES256GCM
		}

		if bytes.Equal(id, newId) {
			return newKey, id, gocbcrypto.CipherNameAES256GCM
		}

		return nil, nil, gocbcrypto.CipherNameNone
	}

	// set new key id and cipher
	assert.NoError(t, db.SetCurrentEncryptionKey(newKey, newId, gocbcrypto.CipherNameAES256GCM))
	assert.NoError(t, db.DropKeyIdsFromSnapshot(keyIds, conf.Path))
	t.Log(db.encSts.String())
	assert.Greater(t, db.encSts.NumFilesRotated, uint64(0))
	assert.Equal(t, uint64(0), db.encSts.NumFilesErrEncrypt+
		db.encSts.NumFilesErrRencrypt+db.encSts.NumFilesErrDecrypt)

	// get key ids from snapshot after dropping
	keyIds, err = db.getActiveKeyIdsFromSnapshot(conf.Path)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(keyIds))

	// verify that only the new key id is present
	for _, id := range keyIds {
		assert.True(t, bytes.Equal(id, newId))
	}

	db.Close()

	if t.Failed() {
		return
	}

	db, err = NewWithEncryptionConfig(conf, []string{conf.Path})
	assert.NoError(t, err)
	defer db.Close()

	// get key ids from snapshot after re-encrypting
	keyIds2, err2 := db.getActiveKeyIdsFromSnapshot(conf.Path)
	assert.NoError(t, err2)
	assert.Equal(t, len(keyIds), len(keyIds2))
	assert.ElementsMatch(t, keyIds, keyIds2)
}

// verifies that all key ids can be dropped from a snapshot when encryption is disabled
func testEncryptionDropAllKeyIdsFromSnapshot(t *testing.T, conf Config) {
	os.RemoveAll("db.dump")
	conf.Path = "db.dump"
	conf.UseDeltaInterleaving()
	db, err := NewWithEncryptionConfig(conf, nil)
	assert.NoError(t, err)

	var wg sync.WaitGroup
	n := 1000000

	// insert n items
	wg.Add(1)
	w := db.NewWriter()
	doInsertSafe(w, &wg, n, false)
	wg.Wait()

	snap, _ := db.NewSnapshot()
	// delete n items. This adds dead items to the writer gc list but does not delete them from the index.
	wg.Add(1)
	doDeleteSafe(w, &wg, n)
	wg.Wait()

	snap.Open()
	keyId, cipher, _ := db.RegisterSnapshotKeyId(conf.Path)
	assert.NoError(t, db.PreparePersistence(conf.Path, snap, keyId, cipher))
	assert.NoError(t, db.StoreToDisk(conf.Path, snap, runtime.GOMAXPROCS(0), keyId, cipher, nil))
	snap.Close()

	// get key ids from snapshot before dropping
	keyIds, err := db.getActiveKeyIdsFromSnapshot(conf.Path)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(keyIds))

	// disable encryption
	assert.NoError(t, db.SetCurrentEncryptionKey(nil, []byte{}, gocbcrypto.CipherNameNone))

	// set new key id and cipher
	assert.NoError(t, db.DropKeyIdsFromSnapshot(keyIds, conf.Path))
	t.Log(db.encSts.String())

	assert.Greater(t, db.encSts.NumFilesRotated, uint64(0))
	assert.Equal(t, uint64(0), db.encSts.NumFilesErrEncrypt+
		db.encSts.NumFilesErrRencrypt+db.encSts.NumFilesErrDecrypt)

	// empty keyId from snapshot after dropping
	keyIds, err = db.getActiveKeyIdsFromSnapshot(conf.Path)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(keyIds))
	assert.ElementsMatch(t, []byte{}, keyIds[0])

	sts, err := db.GetEncryptionStatsCached()
	assert.NoError(t, err)
	assert.Equal(t, StatusNotEncrypted, sts.Status)
	t.Log(sts.String())

	db.Close()

	if t.Failed() {
		return
	}

	db, err = NewWithEncryptionConfig(conf, []string{conf.Path})
	assert.NoError(t, err)
	defer db.Close()

	// get key ids from snapshot after re-encrypting
	keyIds2, err2 := db.getActiveKeyIdsFromSnapshot(conf.Path)
	assert.NoError(t, err2)
	assert.Equal(t, len(keyIds), len(keyIds2))
	assert.ElementsMatch(t, keyIds, keyIds2)
}

// verifies that concurrent snapshot cleanup and key id rotation operations can be performed safely.
// The snapshot cleanup operation should be able to preempt the key id rotation operation.
func testEncryptionDropKeyIdsFromSnapshotWithConcurrentRemoveSnapshot(t *testing.T, conf Config) {
	os.RemoveAll("db.dump")
	conf.Path = "db.dump"
	conf.UseDeltaInterleaving()
	db, err := NewWithEncryptionConfig(conf, nil)
	assert.NoError(t, err)
	defer db.Close()

	var wg sync.WaitGroup
	n := 1000000

	// insert n items
	wg.Add(1)
	w := db.NewWriter()
	doInsertSafe(w, &wg, n, false)
	wg.Wait()

	snap, _ := db.NewSnapshot()
	// delete n items. This adds dead items to the writer gc list but does not delete them from the index.
	wg.Add(1)
	doDeleteSafe(w, &wg, n)
	wg.Wait()

	snap.Open()
	keyId, cipher, _ := db.RegisterSnapshotKeyId(conf.Path)
	assert.NoError(t, db.PreparePersistence(conf.Path, snap, keyId, cipher))
	snap.Close() // this updates snapshot gc list

	// This triggers GC which flushes the gc list and removes the dead items from the index
	assert.NoError(t, db.StoreToDisk(conf.Path, snap, runtime.GOMAXPROCS(0), keyId, cipher, nil))

	// rotate key ids from snapshot

	oldId := db.encKeyId
	oldKey := db.GetEncryptionKeyById(oldId)

	keyIds := make([][]byte, 0)
	bs := make([]byte, len(db.encKeyId))
	copy(bs, db.encKeyId)
	keyIds = append(keyIds, bs)
	t.Log("oldkeyId", oldId)

	newKey := make([]byte, gocbcrypto.AES_256_GCM_KEY_SZ)
	r := rand.New(rand.NewSource(0))
	r.Read(newKey)

	newId := make([]byte, gocbcrypto.AES_256_GCM_KEY_SZ)
	r.Read(newId)
	t.Log("newkeyId", newId)

	// set new key id and cipher
	conf.GetKeyById = func(id []byte) ([]byte, []byte, string) {
		if bytes.Equal(id, oldId) {
			return oldKey, id, gocbcrypto.CipherNameAES256GCM
		}

		if bytes.Equal(id, newId) {
			return newKey, id, gocbcrypto.CipherNameAES256GCM
		}

		return nil, nil, ""
	}

	// set new key id and cipher
	assert.NoError(t, db.SetCurrentEncryptionKey(newKey, newId, gocbcrypto.CipherNameAES256GCM))

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := db.DropKeyIdsFromSnapshot(keyIds, conf.Path)
		t.Log(err)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, db.RemoveSnapshot(conf.Path))
	}()

	wg.Wait()

	for i := 0; i < 2; i++ {
		snapPath := filepath.Join(db.Path, fmt.Sprintf("snap:%v", i))
		_, err := os.Stat(snapPath)
		assert.True(t, os.IsNotExist(err))
	}
}

func testEncryptionDropKeyIdsConcurrent(t *testing.T, conf Config) {
	snapDir := "db.dump"
	os.RemoveAll(snapDir)
	conf.Path = snapDir
	conf.UseDeltaInterleaving()

	db, err := NewWithEncryptionConfig(conf, nil)
	assert.NoError(t, err)
	defer db.Close()

	var wg sync.WaitGroup
	n := 1000000

	wg.Add(1)
	w := db.NewWriter()
	doInsertSafe(w, &wg, n, false)
	wg.Wait()

	snap, _ := db.NewSnapshot()
	wg.Add(1)
	doDeleteSafe(w, &wg, n)
	wg.Wait()

	snap.Open()
	keyId, cipher, _ := db.RegisterSnapshotKeyId(snapDir)
	assert.NoError(t, db.PreparePersistence(snapDir, snap, keyId, cipher))
	snap.Close()

	assert.NoError(t, db.StoreToDisk(snapDir, snap, runtime.GOMAXPROCS(0), keyId, cipher, nil))

	currId := db.encKeyId
	currKey := db.GetEncryptionKeyById(currId)

	keyIds := make([][]byte, 0)
	bs := make([]byte, len(currId))
	copy(bs, currId)
	keyIds = append(keyIds, bs)

	// generate new keys for simultaneous key rotation operations

	newKey1 := make([]byte, gocbcrypto.AES_256_GCM_KEY_SZ)
	r := rand.New(rand.NewSource(0))
	r.Read(newKey1)

	newId1 := make([]byte, gocbcrypto.AES_256_GCM_KEY_SZ)
	r.Read(newId1)

	newKey2 := make([]byte, gocbcrypto.AES_256_GCM_KEY_SZ)
	r2 := rand.New(rand.NewSource(0))
	r2.Read(newKey2)

	newId2 := make([]byte, gocbcrypto.AES_256_GCM_KEY_SZ)
	r.Read(newId2)

	// set new key ids and cipher
	conf.GetKeyById = func(id []byte) ([]byte, []byte, string) {
		if bytes.Equal(id, currId) {
			return currKey, id, gocbcrypto.CipherNameAES256GCM
		}

		if bytes.Equal(id, newId1) {
			return newKey1, id, gocbcrypto.CipherNameAES256GCM
		}

		if bytes.Equal(id, newId2) {
			return newKey2, id, gocbcrypto.CipherNameAES256GCM
		}

		return nil, nil, gocbcrypto.CipherNameNone
	}
	db.SetEncryption(conf.GetKeyById, 0)

	// set new key id and cipher

	var err1, err2 error
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, db.SetCurrentEncryptionKey(newKey1, newId1, gocbcrypto.CipherNameAES256GCM))
		err1 = db.DropKeyIdsFromSnapshot(keyIds, snapDir)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, db.SetCurrentEncryptionKey(newKey2, newId2, gocbcrypto.CipherNameAES256GCM))
		err2 = db.DropKeyIdsFromSnapshot([][]byte{newId1}, snapDir)
	}()

	wg.Wait()

	// caller should retry on concurrent drop operation
	if err1 != nil {
		assert.ErrorIs(t, err1, ErrSnapshotBusy)
	}
	if err2 != nil {
		assert.ErrorIs(t, err2, ErrSnapshotBusy)
	}
}

// TestEncryptionDropKeyIdsConcurrentManyInstances creates 5 MemDB instances,
// each with 3 snapshots, and concurrently:
// 1. Adds a new encryption key to all instances
// 2. Drops the old key from all 15 snapshots (5 instances × 3 snapshots each)
// 3. Verifies that encryption stats show no pending operations
func testEncryptionDropKeyIdsConcurrentManyInstances(t *testing.T, testConf Config) {
	defer ValidateNoMemLeaks()

	const (
		numInstances = 5
		numSnapshots = 3
		numItems     = 10000
	)

	// Create temporary directories
	baseDirs := make([]string, 0, numInstances)
	defer func() {
		for _, dir := range baseDirs {
			os.RemoveAll(dir)
		}
	}()

	instances := make([]*MemDB, 0, numInstances)

	// Step 1: Create instances and snapshots with initial encryption key
	oldKey := make([]byte, 32)
	rand.Read(oldKey)
	oldKeyID := []byte("oldkey-" + fmt.Sprintf("%d", rand.Int63()))

	for i := 0; i < numInstances; i++ {
		dir := fmt.Sprintf("testdb-instance-%d", i)
		baseDirs = append(baseDirs, dir)
		os.RemoveAll(dir)

		conf := testConf
		conf.Path = dir

		// Create instance with encryption
		db, err := NewWithEncryptionConfig(conf, nil)
		assert.NoError(t, err, "failed to create instance %d", i)

		// Set initial encryption key
		err = db.SetCurrentEncryptionKey(oldKey, oldKeyID, gocbcrypto.CipherNameAES256GCM)
		assert.NoError(t, err, "failed to set encryption key for instance %d", i)

		// Insert data
		var wg sync.WaitGroup
		w := db.NewWriter()
		wg.Add(1)
		go doInsertSafe(w, &wg, numItems, false)
		wg.Wait()

		// Create snapshots with data
		for j := 0; j < numSnapshots; j++ {
			snap, _ := db.NewSnapshot()
			snap.Open()

			// Persist snapshot
			snapDir := filepath.Join(dir, fmt.Sprintf("snap-%d", j))
			keyID, cipher, _ := db.RegisterSnapshotKeyId(snapDir)
			os.MkdirAll(snapDir, 0755)

			err := db.PreparePersistence(snapDir, snap, keyID, cipher)
			assert.NoError(t, err, "failed to prepare persistence for instance %d snap %d", i, j)

			err = db.StoreToDisk(snapDir, snap, runtime.GOMAXPROCS(0), keyID, cipher, nil)
			assert.NoError(t, err, "failed to store snapshot to disk for instance %d snap %d", i, j)

			snap.Close()
		}

		instances = append(instances, db)
	}

	// Step 2: Concurrently add new encryption key to all instances
	newKey := make([]byte, 32)
	rand.Read(newKey)
	newKeyID := []byte("newkey-" + fmt.Sprintf("%d", rand.Int63()))

	var keyRotationWg sync.WaitGroup
	for i, db := range instances {
		keyRotationWg.Add(1)
		go func(idx int, instance *MemDB) {
			defer keyRotationWg.Done()
			err := instance.SetCurrentEncryptionKey(newKey, newKeyID, gocbcrypto.CipherNameAES256GCM)
			assert.NoError(t, err, "failed to rotate key for instance %d", idx)
		}(i, db)
	}
	keyRotationWg.Wait()

	// Step 3: Concurrently drop old keys from all snapshots (5 instances × 3 snapshots each)
	var dropKeyWg sync.WaitGroup
	for i := 0; i < numInstances; i++ {
		for j := 0; j < numSnapshots; j++ {
			dropKeyWg.Add(1)
			go func(instIdx int, snapIdx int) {
				defer dropKeyWg.Done()

				snapDir := filepath.Join(baseDirs[instIdx], fmt.Sprintf("snap-%d", snapIdx))
				err := instances[instIdx].DropKeyIdsFromSnapshot([][]byte{oldKeyID}, snapDir)
				assert.NoError(t, err, "failed to drop key from instance %d snapshot %d", instIdx, snapIdx)
			}(i, j)
		}
	}
	dropKeyWg.Wait()

	// Step 4: Verify encryption stats across all instances
	for i, db := range instances {
		// Get encryption stats
		stats, err := db.GetEncryptionStatsFromDisk()
		assert.NoError(t, err, "failed to get encryption stats for instance %d", i)

		// Verify status and active keys
		assert.Equal(t, "encrypted", stats.Status, "instance %d should be encrypted", i)
		assert.Equal(t, uint32(1), stats.NumActiveKeys, "instance %d should have 1 active key after full rotation", i)

		// Verify no pending operations
		assert.Zero(t, stats.numFilesPendingEncrypt, "instance %d should have no pending encrypt operations", i)
		assert.Zero(t, stats.numFilesPendingDecrypt, "instance %d should have no pending decrypt operations", i)
		assert.Zero(t, stats.numFilesPendingRencrypt, "instance %d should have no pending rencrypt operations", i)

		// Verify through cached stats as well
		cachedStats, err := db.GetEncryptionStatsCached()
		assert.NoError(t, err, "failed to get cached encryption stats for instance %d", i)

		assert.Equal(t, "encrypted", cachedStats.Status, "instance %d cached status should be encrypted", i)
		assert.Equal(t, uint32(1), cachedStats.NumActiveKeys, "instance %d cached should have 1 active key", i)
		assert.Zero(t, cachedStats.numFilesPendingEncrypt, "instance %d cached should have no pending encrypt", i)
		assert.Zero(t, cachedStats.numFilesPendingDecrypt, "instance %d cached should have no pending decrypt", i)
		assert.Zero(t, cachedStats.numFilesPendingRencrypt, "instance %d cached should have no pending rencrypt", i)

		// Verify active keys are only the new key
		activeKeys := db.GetActiveKeyIdList()
		assert.Len(t, activeKeys, 1, "instance %d should have exactly 1 active key", i)
		assert.Equal(t, newKeyID, activeKeys[0], "instance %d should have new key as active", i)

		db.Close()
	}

	t.Logf("Successfully completed concurrent multi-instance key rotation: %d instances with key rotation",
		numInstances)
}

func testEncryptionDropKeyIdsCorruptSnapshot(t *testing.T, conf Config) {
	snapDir := "db.dump"
	os.RemoveAll(snapDir)
	conf.Path = snapDir
	conf.UseDeltaInterleaving()

	db, err := NewWithEncryptionConfig(conf, nil)
	assert.NoError(t, err)
	defer db.Close()

	var wg sync.WaitGroup
	n := 1000000

	wg.Add(1)
	w := db.NewWriter()
	doInsertSafe(w, &wg, n, false)
	wg.Wait()

	snap, _ := db.NewSnapshot()
	wg.Add(1)
	doDeleteSafe(w, &wg, n)
	wg.Wait()

	snap.Open()
	keyId, cipher, _ := db.RegisterSnapshotKeyId(snapDir)
	assert.NoError(t, db.PreparePersistence(snapDir, snap, keyId, cipher))
	snap.Close()

	assert.NoError(t, db.StoreToDisk(snapDir, snap, runtime.GOMAXPROCS(0), keyId, cipher, nil))

	currId := db.encKeyId
	currKey := db.GetEncryptionKeyById(currId)

	keyIds := make([][]byte, 0)
	bs := make([]byte, len(currId))
	copy(bs, currId)
	keyIds = append(keyIds, bs)

	// generate new keys for simultaneous key rotation operations
	newKey1 := make([]byte, gocbcrypto.AES_256_GCM_KEY_SZ)
	r := rand.New(rand.NewSource(0))
	r.Read(newKey1)

	newId1 := make([]byte, gocbcrypto.AES_256_GCM_KEY_SZ)
	r.Read(newId1)

	// set new key ids and cipher
	conf.GetKeyById = func(id []byte) ([]byte, []byte, string) {
		if bytes.Equal(id, currId) {
			return currKey, id, gocbcrypto.CipherNameAES256GCM
		}
		if bytes.Equal(id, newId1) {
			return newKey1, id, gocbcrypto.CipherNameAES256GCM
		}
		return nil, nil, gocbcrypto.CipherNameNone
	}
	db.SetEncryption(conf.GetKeyById, 0)

	// trigger rotation
	assert.NoError(t, db.SetCurrentEncryptionKey(newKey1, newId1, gocbcrypto.CipherNameAES256GCM))

	// corrupt
	shard0 := filepath.Join(snapDir, "data", "shard-0")
	cwr, err := os.OpenFile(shard0, os.O_WRONLY, 0755)
	assert.NoError(t, err)
	off := gocbcrypto.FILEHDR_SZ + 4
	cwr.WriteAt([]byte("corrupt"), int64(off))
	cwr.Close()

	err = db.DropKeyIdsFromSnapshot(keyIds, snapDir)
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "fatal"))
	assert.NoError(t, db.RemoveSnapshot(snapDir))
}

// verifies that the key rotation janitor can be used to cleanup temporary and backup files.
func testEncryptionCleanupDropKeyFiles(t *testing.T, conf Config) {
	snapDir := "db.dump"
	os.RemoveAll(snapDir)
	conf.Path = snapDir
	conf.UseDeltaInterleaving()
	db, err := NewWithEncryptionConfig(conf, nil)
	assert.NoError(t, err)

	var wg sync.WaitGroup
	n := 1000000

	// insert n items
	wg.Add(1)
	w := db.NewWriter()
	doInsertSafe(w, &wg, n, false)
	wg.Wait()

	snap, _ := db.NewSnapshot()
	// delete n items. This adds dead items to the writer gc list but does not delete them from the index.
	wg.Add(1)
	doDeleteSafe(w, &wg, n)
	wg.Wait()

	snap.Open()
	keyId, cipher, _ := db.RegisterSnapshotKeyId(conf.Path)
	assert.NoError(t, db.PreparePersistence(conf.Path, snap, keyId, cipher))
	snap.Close() // this updates snapshot gc list

	// This triggers GC which flushes the gc list and removes the dead items from the index
	assert.NoError(t, db.StoreToDisk(conf.Path, snap, runtime.GOMAXPROCS(0), keyId, cipher, nil))

	// Helper to create test encrypted files
	createEncFile := func(path string, content []byte) error {
		keyId, cipher := db.GetCurrentKeyId()
		ctx, err := db.NewEncryptionContext(keyId, cipher)
		if err != nil {
			return err
		}
		return gocbcrypto.WriteFile(path, content, 0644, ctx, nil)
	}

	// Test Case 1: Temp file should be cleaned up
	tmpFile1 := filepath.Join(snapDir, "data", "shard-0"+rencrypt_tmp_ext)
	assert.NoError(t, createEncFile(tmpFile1, []byte("temp data 1")))

	// Test Case 2: Backup file with original exists - backup should be removed
	origFile1 := filepath.Join(snapDir, "data", "shard-2")
	bakFile1 := origFile1 + encrypt_bak_ext
	assert.NoError(t, createEncFile(bakFile1, []byte("temp data 1")))
	assert.FileExists(t, origFile1)
	assert.FileExists(t, bakFile1)

	// Test Case 3: Backup file without original - backup should be restored
	origFile2 := filepath.Join(snapDir, "data", "shard-3")
	bakFile2 := origFile2 + encrypt_bak_ext
	assert.NoError(t, os.Rename(origFile2, bakFile2))

	db.Close()

	// Run janitor cleanup
	db, err = NewWithEncryptionConfig(conf, []string{snapDir})
	assert.NoError(t, err)
	defer db.Close()

	snap2, err := db.LoadFromDisk(snapDir, runtime.GOMAXPROCS(0), nil)
	assert.NoError(t, err)
	defer snap2.Close()

	// Verify Case 1: Temp files should be removed
	assert.NoFileExists(t, tmpFile1)

	// Verify Case 2: Backup removed when original exists
	assert.FileExists(t, origFile1)
	assert.NoFileExists(t, bakFile1)

	// Verify Case 3: Backup restored to original
	assert.FileExists(t, origFile2)
	assert.NoFileExists(t, bakFile2)
}

// verifies that if a snapshot is left in a failed state, it is cleaned up from
// snapKeyIds
func testEncryptionCleanupStaleSnapshotFromSnapKeys(t *testing.T, conf Config) {
	snapDir := "db.dump"
	os.RemoveAll(snapDir)
	conf.Path = snapDir
	conf.UseDeltaInterleaving()

	db, err := NewWithEncryptionConfig(conf, nil)
	assert.NoError(t, err)
	defer db.Close()

	var wg sync.WaitGroup
	n := 10000

	wg.Add(1)
	w := db.NewWriter()
	doInsertSafe(w, &wg, n, false)
	wg.Wait()

	snap, _ := db.NewSnapshot()
	defer snap.Close()
	keyId, cipher, _ := db.RegisterSnapshotKeyId(snapDir)

	wg.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				t.Logf("(expected) recovered from panic: %v", r)
				db.DeregisterSnapshotKeyId(snapDir, keyId)

			}
			wg.Done()
		}()
		snap.Open()
		assert.NoError(t, db.PreparePersistence(snapDir, snap, keyId, cipher))
		assert.NoError(t, db.StoreToDisk(snapDir, nil, runtime.GOMAXPROCS(0), keyId, cipher, nil))
	}()

	wg.Wait()
	assert.Equal(t, 0, len(db.snapKeyIds), "snapshot has failed and should be cleaned up from snapKeyIds")
}

// tests encryption of an empty index (no items inserted)
func testEncryptionEmptyIndex(t *testing.T, conf Config) {
	defer ValidateNoMemLeaks()

	snapDir := "db.dump.empty"
	os.RemoveAll(snapDir)
	defer os.RemoveAll(snapDir)

	conf.Path = snapDir
	db, err := NewWithEncryptionConfig(conf, nil)
	assert.NoError(t, err)

	// Verify encryption is enabled
	keyId, _ := db.GetCurrentKeyId()
	assert.NotNil(t, keyId, "Encryption should be enabled")

	// Create a snapshot without inserting any items
	snap, err := db.NewSnapshot()
	assert.NoError(t, err)

	snapPath := filepath.Join(db.Path, "snap:empty")
	snap.Open()
	keyId, cipher, _ := db.RegisterSnapshotKeyId(snapPath)
	err = db.PreparePersistence(snapPath, snap, keyId, cipher)
	assert.NoError(t, err)

	err = db.StoreToDisk(snapPath, snap, runtime.GOMAXPROCS(0), keyId, cipher, nil)
	assert.NoError(t, err)
	snap.Close()

	_, err = os.Stat(snapPath)
	assert.NoError(t, err, "Snapshot directory should exist")

	keyIds, err := db.getActiveKeyIdsFromSnapshot(snapPath)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(keyIds), "Should have one key ID")

	// Close and reopen database
	db.Close()

	db, err = NewWithEncryptionConfig(conf, []string{snapPath})
	assert.NoError(t, err)

	// Load the empty snapshot
	snap2, err := db.LoadFromDisk(snapPath, runtime.GOMAXPROCS(0), nil)
	assert.NoError(t, err)
	assert.NotNil(t, snap2)
	assert.Equal(t, int64(0), snap2.count, "Loaded snapshot should have 0 items")
	snap2.Close()

	// Verify key IDs are still accessible after recovery
	keyIds2, err := db.getActiveKeyIdsFromSnapshot(snapPath)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(keyIds2))
	assert.True(t, bytes.Equal(keyId, keyIds2[0]), "Key ID should match after recovery")

	// Test key rotation on empty snapshot
	newKeyId := []byte("new-empty-test-key-123456")
	masterKey, _, _ := db.GetKeyById(newKeyId)
	err = db.SetCurrentEncryptionKey(masterKey, newKeyId, gocbcrypto.CipherNameAES256GCM)
	assert.NoError(t, err)

	// Rotate the empty snapshot to use new key
	err = db.DropKeyIdsFromSnapshot([][]byte{keyId}, snapPath)
	assert.NoError(t, err)

	// Verify key rotation worked
	keyIds3, err := db.getActiveKeyIdsFromSnapshot(snapPath)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(keyIds3), "Should still have one key ID after rotation")
	assert.True(t, bytes.Equal(newKeyId, keyIds3[0]), "Key ID should be the new key")

	// Test disabling encryption on empty snapshot
	err = db.SetCurrentEncryptionKey(nil, nil, gocbcrypto.CipherNameNone)
	assert.NoError(t, err)

	sts, err := db.GetEncryptionStatsFromDisk()
	assert.NoError(t, err)
	t.Logf("Encryption stats after rotation: %+v", sts)
	assert.Equal(t, uint32(1), sts.NumActiveKeys)
	assert.NotZero(t, sts.numFilesPendingDecrypt)

	// Decrypt the snapshot
	err = db.DropKeyIdsFromSnapshot([][]byte{newKeyId}, snapPath)
	assert.NoError(t, err)

	// Verify snapshot is now unencrypted
	keyIds4, err := db.getActiveKeyIdsFromSnapshot(snapPath)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(keyIds4), "Should have one entry (empty key)")
	assert.Equal(t, 0, len(keyIds4[0]), "Key ID should be empty for unencrypted snapshot")

	sts, err = db.GetEncryptionStatsFromDisk()
	assert.NoError(t, err)
	t.Logf("Encryption stats after rotation: %+v", sts)
	assert.Equal(t, uint32(0), sts.NumActiveKeys)
	assert.NotZero(t, sts.NumFilesRotated)

	db.Close()

	db, err = NewWithEncryptionConfig(conf, []string{snapPath})
	assert.NoError(t, err)
	defer db.Close()

	// Verify we can still load the unencrypted empty snapshot
	snap3, err := db.LoadFromDisk(snapPath, runtime.GOMAXPROCS(0), nil)
	assert.NoError(t, err)
	snap3.Close()

	t.Log("Empty index encryption test completed successfully")
}

// tests encryption stats with various scenarios
// PendingDecrypt: files that are encrypted but not decrypted
// PendingRencrypt: files that are encrypted but need to be re-encrypted
// Rotation: files that are rotated
func testEncryptionStats(t *testing.T, conf Config) {

	t.Run("PendingEncryptWithDropEmptyKeyId", func(t *testing.T) {
		snapDir := "db.dump.encrypt"
		os.RemoveAll(snapDir)
		defer os.RemoveAll(snapDir)

		// Step 1: Create snapshot WITHOUT encryption
		conf.Path = snapDir
		conf.UseDeltaInterleaving()
		conf.GetKeyById = nil // disable encryption
		db, err := NewWithEncryptionConfig(conf, nil)
		assert.NoError(t, err)

		var wg sync.WaitGroup
		n := 100000

		// Insert items
		wg.Add(1)
		w := db.NewWriter()
		doInsertSafe(w, &wg, n, false)
		wg.Wait()

		snap, _ := db.NewSnapshot()
		snap.Open()
		keyId, cipher, _ := db.RegisterSnapshotKeyId(conf.Path)
		assert.NoError(t, db.PreparePersistence(conf.Path, snap, keyId, cipher))
		snap.Close()

		// Persist without encryption
		assert.NoError(t, db.StoreToDisk(conf.Path, snap, runtime.GOMAXPROCS(0), keyId, cipher, nil))
		snap.Close()
		db.Close()

		// Step 2: Reload with encryption enabled
		encKey := make([]byte, gocbcrypto.AES_256_GCM_KEY_SZ)
		encId := make([]byte, gocbcrypto.AES_256_GCM_KEY_SZ)
		r := rand.New(rand.NewSource(0))
		r.Read(encKey)
		r.Read(encId)

		conf.GetKeyById = func(id []byte) ([]byte, []byte, string) {
			if bytes.Equal(id, encId) {
				return encKey, encId, gocbcrypto.CipherNameAES256GCM
			}
			return nil, nil, gocbcrypto.CipherNameNone
		}

		db, err = NewWithEncryptionConfig(conf, []string{snapDir})
		assert.NoError(t, err)

		// Set encryption key
		assert.NoError(t, db.SetCurrentEncryptionKey(encKey, encId, gocbcrypto.CipherNameAES256GCM))

		// Load unencrypted snapshot
		snap2, err := db.LoadFromDisk(snapDir, runtime.GOMAXPROCS(0), nil)
		assert.NoError(t, err)
		snap2.Close()

		// Check encryption stats - all files should be pending encryption
		sts, err := db.GetEncryptionStatsFromDisk()
		assert.NoError(t, err)
		t.Logf("Encryption stats: %+v", sts)

		// Verify stats
		assert.Equal(t, uint32(1), sts.NumActiveKeys)
		assert.Equal(t, "not_encrypted", sts.Status)
		assert.Greater(t, sts.numFiles, uint64(0))
		assert.Equal(t, sts.numFiles, sts.numFilesPendingEncrypt)
		assert.Equal(t, uint64(0), sts.numFilesPendingRencrypt)

		cached, err := db.GetEncryptionStatsCached()
		assert.NoError(t, err)
		t.Logf("Encryption stats cached: %+v", cached)
		assert.Equal(t, uint32(1), cached.NumActiveKeys)
		assert.Equal(t, "not_encrypted", cached.Status)

		// Now encrypt the files by dropping the unencrypted key
		keyIds := [][]byte{[]byte{}} // empty keyId represents unencrypted files
		assert.NoError(t, db.DropKeyIdsFromSnapshot(keyIds, conf.Path))

		sts, err = db.GetEncryptionStatsFromDisk()
		assert.NoError(t, err)
		t.Logf("Encryption stats after rotation: %+v", sts)

		assert.Equal(t, uint32(1), sts.NumActiveKeys)
		assert.Equal(t, "encrypted", sts.Status)
		assert.Greater(t, sts.numFiles, uint64(0))
		assert.Equal(t, uint64(0), sts.numFilesPendingEncrypt)
		assert.Equal(t, uint64(0), sts.numFilesPendingRencrypt)
		assert.Greater(t, sts.NumFilesRotated, uint64(0), "Files should have been rotated")
		assert.Equal(t, uint64(0), sts.NumFilesErrEncrypt+sts.NumFilesErrDecrypt+sts.NumFilesErrRencrypt)

		cached, err = db.GetEncryptionStatsCached()
		assert.NoError(t, err)
		assert.Equal(t, uint32(1), cached.NumActiveKeys)
		assert.Greater(t, cached.NumFilesRotated, uint64(0), "Files should have been rotated")
		assert.Equal(t, "encrypted", cached.Status)
		assert.Equal(t, uint64(0), cached.NumFilesErrEncrypt+cached.NumFilesErrDecrypt+cached.NumFilesErrRencrypt)

		db.Close()

		db, err = NewWithEncryptionConfig(conf, []string{snapDir})
		assert.NoError(t, err)
		defer db.Close()

		snap2, err = db.LoadFromDisk(snapDir, runtime.GOMAXPROCS(0), nil)
		assert.NoError(t, err)
		snap2.Close()

		cached, err = db.GetEncryptionStatsCached()
		assert.NoError(t, err)
		assert.Equal(t, uint32(1), cached.NumActiveKeys)
		assert.Equal(t, "encrypted", cached.Status)
	})

	t.Run("PendingDecryptWithSetCurrentEncryptionKeyToEmpty", func(t *testing.T) {
		snapDir := "db.dump.decrypt"
		os.RemoveAll(snapDir)
		defer os.RemoveAll(snapDir)

		// Step 1: Create snapshot WITH encryption
		conf.Path = snapDir
		conf.UseDeltaInterleaving()

		encKey := make([]byte, gocbcrypto.AES_256_GCM_KEY_SZ)
		encId := make([]byte, gocbcrypto.AES_256_GCM_KEY_SZ)
		r := rand.New(rand.NewSource(0))
		r.Read(encKey)
		r.Read(encId)

		conf.GetKeyById = func(id []byte) ([]byte, []byte, string) {
			if bytes.Equal(id, encId) {
				return encKey, encId, gocbcrypto.CipherNameAES256GCM
			}
			return nil, nil, gocbcrypto.CipherNameNone
		}

		db, err := NewWithEncryptionConfig(conf, nil)
		assert.NoError(t, err)
		assert.NoError(t, db.SetCurrentEncryptionKey(encKey, encId, gocbcrypto.CipherNameAES256GCM))

		var wg sync.WaitGroup
		n := 100000

		// Insert items
		wg.Add(1)
		w := db.NewWriter()
		doInsertSafe(w, &wg, n, false)
		wg.Wait()

		snap, _ := db.NewSnapshot()
		snap.Open()
		keyId, cipher, _ := db.RegisterSnapshotKeyId(conf.Path)
		assert.NoError(t, db.PreparePersistence(conf.Path, snap, keyId, cipher))
		snap.Close()

		// Persist with encryption
		assert.NoError(t, db.StoreToDisk(conf.Path, snap, runtime.GOMAXPROCS(0), keyId, cipher, nil))
		db.Close()

		// Step 2: Reload with encryption disabled
		conf.GetKeyById = func(id []byte) ([]byte, []byte, string) {
			// Still provide old key for reading
			if bytes.Equal(id, encId) {
				return encKey, encId, gocbcrypto.CipherNameAES256GCM
			}
			return nil, nil, gocbcrypto.CipherNameNone
		}

		db, err = NewWithEncryptionConfig(conf, []string{snapDir})
		assert.NoError(t, err)
		defer db.Close()

		// Disable encryption by setting empty key
		assert.NoError(t, db.SetCurrentEncryptionKey(nil, nil, gocbcrypto.CipherNameNone))

		// Load encrypted snapshot with encryption disabled
		snap2, err := db.LoadFromDisk(snapDir, runtime.GOMAXPROCS(0), nil)
		assert.NoError(t, err)
		defer snap2.Close()

		// Check encryption stats - all files should be pending decryption
		sts, err := db.GetEncryptionStatsFromDisk()
		assert.NoError(t, err)
		t.Logf("Encryption stats: %+v", sts)

		// Verify stats
		assert.Equal(t, uint32(1), sts.NumActiveKeys)
		assert.Equal(t, "encrypted", sts.Status)
		assert.Greater(t, sts.numFiles, uint64(0))
		assert.Equal(t, uint64(0), sts.numFilesPendingEncrypt)
		assert.Equal(t, sts.numFiles, sts.numFilesPendingDecrypt, "All files should be pending decryption")
		assert.Equal(t, uint64(0), sts.numFilesPendingRencrypt)
		assert.Equal(t, uint64(0), sts.NumFilesRotated)

		cached, err := db.GetEncryptionStatsCached()
		assert.NoError(t, err)
		assert.Equal(t, uint32(1), cached.NumActiveKeys)
		assert.Equal(t, uint64(0), cached.NumFilesRotated)
		assert.Equal(t, "encrypted", cached.Status)

		// Now decrypt the files by dropping the encrypted key
		keyIds := [][]byte{encId}
		assert.NoError(t, db.DropKeyIdsFromSnapshot(keyIds, conf.Path))

		// Check stats after decryption
		sts, err = db.GetEncryptionStatsFromDisk()
		assert.NoError(t, err)
		t.Logf("Encryption stats after rotation: %+v", sts)

		assert.Equal(t, uint32(0), sts.NumActiveKeys, "Should have no active key (empty)")
		assert.Equal(t, "not_encrypted", sts.Status)
		assert.Greater(t, sts.numFiles, uint64(0))
		assert.Equal(t, uint64(0), sts.numFilesPendingEncrypt)
		assert.Equal(t, uint64(0), sts.numFilesPendingDecrypt, "No files should be pending decryption after rotation")
		assert.Equal(t, uint64(0), sts.numFilesPendingRencrypt)
		assert.Equal(t, sts.numFiles, sts.NumFilesRotated, "Files should have been rotated")
		assert.Equal(t, uint64(0), sts.NumFilesErrEncrypt+sts.NumFilesErrDecrypt+sts.NumFilesErrRencrypt)

		cached, err = db.GetEncryptionStatsCached()
		assert.NoError(t, err)
		assert.Equal(t, uint32(0), cached.NumActiveKeys)
		assert.Equal(t, "not_encrypted", cached.Status)
		assert.Equal(t, sts.NumFilesRotated, cached.NumFilesRotated)
		assert.Equal(t, uint64(0), cached.NumFilesErrEncrypt+cached.NumFilesErrDecrypt+cached.NumFilesErrRencrypt)
	})

	t.Run("PendingRencryptWithMultipleSnapshots", func(t *testing.T) {
		// Step 1: Create snapshot with mixed encrypted/unencrypted files
		conf.Path = "db.dump.mixed"
		os.RemoveAll(conf.Path)
		defer os.RemoveAll(conf.Path)
		conf.UseDeltaInterleaving()

		encKey1 := make([]byte, gocbcrypto.AES_256_GCM_KEY_SZ)
		encId1 := make([]byte, gocbcrypto.AES_256_GCM_KEY_SZ)
		r := rand.New(rand.NewSource(0))
		r.Read(encKey1)
		r.Read(encId1)

		conf.GetKeyById = func(id []byte) ([]byte, []byte, string) {
			if bytes.Equal(id, encId1) {
				return encKey1, encId1, gocbcrypto.CipherNameAES256GCM
			}
			return nil, nil, gocbcrypto.CipherNameNone
		}

		// Create DB with encryption enabled
		db, err := NewWithEncryptionConfig(conf, nil)
		assert.NoError(t, err)
		assert.NoError(t, db.SetCurrentEncryptionKey(encKey1, encId1, gocbcrypto.CipherNameAES256GCM))

		var wg sync.WaitGroup
		n := 50000

		// Insert items with encryption
		wg.Add(1)
		w := db.NewWriter()
		doInsertSafe(w, &wg, n, false)
		wg.Wait()

		snap, _ := db.NewSnapshot()
		snap.Open()
		snapDir1 := filepath.Join(conf.Path, "snap1")
		keyId, cipher, _ := db.RegisterSnapshotKeyId(snapDir1)
		assert.NoError(t, db.PreparePersistence(snapDir1, snap, keyId, cipher))
		snap.Close()
		assert.NoError(t, db.StoreToDisk(snapDir1, snap, runtime.GOMAXPROCS(0), keyId, cipher, nil))

		// Disable encryption temporarily
		db.SetCurrentEncryptionKey(nil, nil, gocbcrypto.CipherNameNone)

		// Insert more items without encryption
		wg.Add(1)
		doInsertSafe(w, &wg, n, false)
		wg.Wait()

		snap2, _ := db.NewSnapshot()
		snap2.Open()
		snapDir2 := filepath.Join(conf.Path, "snap2")
		keyId2, cipher2, _ := db.RegisterSnapshotKeyId(snapDir2)
		assert.NoError(t, db.PreparePersistence(snapDir2, snap2, keyId2, cipher2))
		snap2.Close()
		assert.NoError(t, db.StoreToDisk(snapDir2, snap2, runtime.GOMAXPROCS(0), keyId2, cipher2, nil))

		db.Close()

		// Step 2: Reload with a new encryption key
		encKey2 := make([]byte, gocbcrypto.AES_256_GCM_KEY_SZ)
		encId2 := make([]byte, gocbcrypto.AES_256_GCM_KEY_SZ)
		r.Read(encKey2)
		r.Read(encId2)

		conf.GetKeyById = func(id []byte) ([]byte, []byte, string) {
			if bytes.Equal(id, encId1) {
				return encKey1, encId1, gocbcrypto.CipherNameAES256GCM
			}
			if bytes.Equal(id, encId2) {
				return encKey2, encId2, gocbcrypto.CipherNameAES256GCM
			}
			if id == nil {
				return encKey1, encId1, gocbcrypto.CipherNameAES256GCM
			}
			return nil, nil, gocbcrypto.CipherNameNone
		}

		db, err = NewWithEncryptionConfig(conf, []string{snapDir1, snapDir2})
		assert.NoError(t, err)
		defer db.Close()

		// Set new encryption key
		assert.NoError(t, db.SetCurrentEncryptionKey(encKey2, encId2, gocbcrypto.CipherNameAES256GCM))

		// Load old snapshot
		snap3, err := db.LoadFromDisk(snapDir1, runtime.GOMAXPROCS(0), nil)
		assert.NoError(t, err)
		defer snap3.Close()

		// Check encryption stats - should have pending encrypt (unencrypted files)
		// and pending re-encrypt (files with old key)
		sts, err := db.GetEncryptionStatsFromDisk()
		assert.NoError(t, err)
		t.Logf("Encryption stats: %+v", sts)

		// Verify stats
		assert.Equal(t, uint32(2), sts.NumActiveKeys)
		assert.Equal(t, "partially_encrypted", sts.Status)
		assert.Greater(t, sts.numFiles, uint64(0))
		assert.Equal(t, sts.numFiles/2, sts.numFilesPendingRencrypt, "snapDir1 should be pending re-encryption")
		assert.Equal(t, sts.numFiles/2, sts.numFilesPendingEncrypt, "snapDir2 is not encrypted")

		cached, err := db.GetEncryptionStatsCached()
		assert.NoError(t, err)
		assert.Equal(t, uint32(2), cached.NumActiveKeys)
		assert.Equal(t, "partially_encrypted", cached.Status)

		// include empty keyId for full encryption
		var dropKeys [][]byte
		dropKeys = append(dropKeys, encId1)
		dropKeys = append(dropKeys, []byte{})

		assert.NoError(t, db.DropKeyIdsFromSnapshot(dropKeys, snapDir1))
		assert.NoError(t, db.DropKeyIdsFromSnapshot(dropKeys, snapDir2))
		sts, err = db.GetEncryptionStatsFromDisk()
		assert.NoError(t, err)
		t.Logf("Encryption stats after rotation: %+v", sts)

		if t.Failed() {
			return
		}

		// both snapshots should be encrypted with latest key
		assert.Equal(t, uint32(1), sts.NumActiveKeys)
		assert.Equal(t, "encrypted", sts.Status)
		assert.Greater(t, sts.numFiles, uint64(0))
		assert.Equal(t, sts.NumFilesRotated, sts.numFiles)
		assert.Equal(t, uint64(0), sts.NumFilesErrEncrypt+sts.NumFilesErrRencrypt)
		assert.Equal(t, uint64(0), sts.numFilesPendingEncrypt+sts.numFilesPendingRencrypt)

		cached, err = db.GetEncryptionStatsCached()
		assert.NoError(t, err)
		assert.Equal(t, "encrypted", cached.Status)
		assert.Equal(t, uint32(1), cached.NumActiveKeys)
		assert.Equal(t, sts.NumFilesRotated, cached.NumFilesRotated)
		assert.Equal(t, uint64(0), cached.NumFilesErrEncrypt+cached.NumFilesErrRencrypt)
	})

	t.Run("PartialRotationMixedOldAndNewKeysInSnapshot", func(t *testing.T) {
		snapRoot := "db.dump.partial.rotation"
		os.RemoveAll(snapRoot)
		defer os.RemoveAll(snapRoot)

		conf.Path = snapRoot
		conf.UseDeltaInterleaving()

		oldKey := make([]byte, gocbcrypto.AES_256_GCM_KEY_SZ)
		oldKeyID := make([]byte, gocbcrypto.AES_256_GCM_KEY_SZ)
		newKey := make([]byte, gocbcrypto.AES_256_GCM_KEY_SZ)
		newKeyID := make([]byte, gocbcrypto.AES_256_GCM_KEY_SZ)

		r := rand.New(rand.NewSource(1))
		r.Read(oldKey)
		r.Read(oldKeyID)
		r.Read(newKey)
		r.Read(newKeyID)

		conf.GetKeyById = func(id []byte) ([]byte, []byte, string) {
			if bytes.Equal(id, oldKeyID) {
				return oldKey, oldKeyID, gocbcrypto.CipherNameAES256GCM
			}
			if bytes.Equal(id, newKeyID) {
				return newKey, newKeyID, gocbcrypto.CipherNameAES256GCM
			}
			if id == nil {
				return oldKey, oldKeyID, gocbcrypto.CipherNameAES256GCM
			}
			return nil, nil, gocbcrypto.CipherNameNone
		}

		db, err := NewWithEncryptionConfig(conf, nil)
		assert.NoError(t, err)
		defer db.Close()
		assert.NoError(t, db.SetCurrentEncryptionKey(oldKey, oldKeyID, gocbcrypto.CipherNameAES256GCM))

		var wg sync.WaitGroup
		n := 50000
		wg.Add(1)
		w := db.NewWriter()
		doInsertSafe(w, &wg, n, false)
		wg.Wait()

		snap, _ := db.NewSnapshot()
		snap.Open()
		snapDir := filepath.Join(conf.Path, "snap-partial")
		keyID, cipher, _ := db.RegisterSnapshotKeyId(snapDir)
		assert.NoError(t, db.PreparePersistence(snapDir, snap, keyID, cipher))
		snap.Close()
		assert.NoError(t, db.StoreToDisk(snapDir, snap, runtime.GOMAXPROCS(0), keyID, cipher, nil))

		assert.NoError(t, db.SetCurrentEncryptionKey(newKey, newKeyID, gocbcrypto.CipherNameAES256GCM))

		var encryptedFiles []string
		err = filepath.WalkDir(snapDir, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			encrypted, er := gocbcrypto.IsFileEncrypted(path)
			if er != nil {
				return er
			}
			if encrypted {
				encryptedFiles = append(encryptedFiles, path)
			}
			return nil
		})
		assert.NoError(t, err)
		if len(encryptedFiles) == 0 {
			t.Fatalf("expected encrypted files in snapshot %s", snapDir)
		}

		rotatedFile := encryptedFiles[0]
		copyFile := rotatedFile + ".oldcopy"
		contents, err := os.ReadFile(rotatedFile)
		assert.NoError(t, err)
		assert.NoError(t, os.WriteFile(copyFile, contents, 0644))

		visitor := &keyRotationVisitor{db: db}
		assert.NoError(t, visitor.rotateSingleFile(context.Background(), rotatedFile, newKeyID, gocbcrypto.CipherNameAES256GCM))

		activeKeys, err := db.getActiveKeyIdsFromSnapshot(snapDir)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(activeKeys), "snapshot should have mixed old and new keys after partial rotation")

		sts, err := db.GetEncryptionStatsFromDisk()
		assert.NoError(t, err)
		assert.Equal(t, "encrypted", sts.Status)
		assert.Equal(t, uint32(2), sts.NumActiveKeys)
		assert.Greater(t, sts.numFilesPendingRencrypt, uint64(0), "some files should still be pending re-encryption")

		cached, err := db.GetEncryptionStatsCached()
		assert.NoError(t, err)
		assert.Equal(t, "encrypted", cached.Status)
		assert.Equal(t, uint32(2), cached.NumActiveKeys)

		assert.ElementsMatch(t, [][]byte{oldKeyID, newKeyID}, activeKeys)

	})
}

func testEncryptionUnsupportedCipher(t *testing.T, conf Config) {
	snapDir := "db.dump"
	os.RemoveAll(snapDir)
	conf.Path = snapDir
	conf.UseDeltaInterleaving()

	db, err := NewWithEncryptionConfig(conf, nil)
	assert.NoError(t, err)

	var wg sync.WaitGroup
	n := 1000000

	wg.Add(1)
	w := db.NewWriter()
	doInsertSafe(w, &wg, n, false)
	wg.Wait()

	snap, _ := db.NewSnapshot()
	wg.Add(1)
	doDeleteSafe(w, &wg, n)
	wg.Wait()

	snap.Open()
	keyId, cipher, _ := db.RegisterSnapshotKeyId(snapDir)
	assert.NoError(t, db.PreparePersistence(snapDir, snap, keyId, cipher))
	assert.NoError(t, db.StoreToDisk(snapDir, snap, runtime.GOMAXPROCS(0), keyId, cipher, nil))
	snap.Close()
	db.Close()

	// unsupported cipher
	conf.GetKeyById = func(id []byte) ([]byte, []byte, string) {
		return nil, nil, ""
	}

	db, err = NewWithEncryptionConfig(conf, []string{snapDir})
	assert.Error(t, err)
	assert.Nil(t, db)
}

func testEncryptionLoadSnapshotDecryptionError(t *testing.T, conf Config) {
	t.Skip("skipping due to MB-70620")

	snapDir := "db.dump"
	os.RemoveAll(snapDir)
	conf.Path = snapDir
	conf.UseDeltaInterleaving()

	db, err := NewWithEncryptionConfig(conf, nil)
	assert.NoError(t, err)

	var wg sync.WaitGroup
	n := 1000000

	wg.Add(1)
	w := db.NewWriter()
	doInsertSafe(w, &wg, n, false)
	wg.Wait()

	snap, _ := db.NewSnapshot()
	wg.Add(1)
	doDeleteSafe(w, &wg, n)
	wg.Wait()

	snap.Open()
	keyId, cipher, _ := db.RegisterSnapshotKeyId(snapDir)
	assert.NoError(t, db.PreparePersistence(snapDir, snap, keyId, cipher))
	assert.NoError(t, db.StoreToDisk(snapDir, snap, runtime.GOMAXPROCS(0), keyId, cipher, nil))
	snap.Close()

	db.Close()

	// corrupt
	shard0 := filepath.Join(snapDir, "data", "shard-0")
	cwr, err := os.OpenFile(shard0, os.O_WRONLY, 0755)
	assert.NoError(t, err)
	off := gocbcrypto.FILEHDR_SZ + 4
	cwr.WriteAt([]byte("corrupt"), int64(off))
	cwr.Close()

	db, err = NewWithEncryptionConfig(conf, nil)
	assert.NoError(t, err)
	defer db.Close()

	// simulate decryption error
	db.Config.GetKeyById = func(id []byte) ([]byte, []byte, string) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		randomKey := make([]byte, 32)
		_, _ = r.Read(randomKey)
		return randomKey, id, gocbcrypto.CipherNameAES256GCM
	}

	snap, err = db.LoadFromDisk(snapDir, runtime.GOMAXPROCS(0), nil)
	assert.Error(t, err)
	assert.NoError(t, db.RemoveSnapshot(snapDir))
}

func TestDirOpGuardWithCancel(t *testing.T) {
	runTest(t, "TestDirOpGuardWithCancel", testDirOpGuardWithCancel, "encryption")
}

func TestDirOpGuardWithMultiplePremption(t *testing.T) {
	runTest(t, "TestDirOpGuardWitMultiplePremption", testDirOpGuardWithMultiplePremption, "encryption")
}

func TestEncryptionLoadStoreDisk(t *testing.T) {
	runTest(t, "TestEncryptionLoadStoreDisk", testLoadStoreDisk, "encryption")
}

func TestEncryptionConcurrentLoadCloseFragmentation(t *testing.T) {
	runTest(t, "TestEncryptionConcurrentLoadCloseFragmentation", testConcurrentLoadCloseFragmentationInt, "encryption")
}

func TestEncryptionStoreDiskShutdown(t *testing.T) {
	runTest(t, "TestEncryptionStoreDiskShutdown", testStoreDiskShutdown, "encryption")
}

func TestEncryptionLoadDeltaStoreDisk(t *testing.T) {
	runTest(t, "testEncryptionLoadDeltaStoreDisk", testLoadDeltaStoreDisk, "encryption")
}

func TestEncryptionDiskCorruption(t *testing.T) {
	runTest(t, "TestEncryptionDiskCorruption", testDiskCorruption, "encryption")
}

func TestEncryptedVsUnencryptedDiskSize(t *testing.T) {
	runTest(t, "TestEncryptedVsUnencryptedDiskSize", testEncryptedVsUnencryptedDiskSize, "encryption")
}

func TestEncryptedVsUnencryptedDiskSizeWithDeltaFiles(t *testing.T) {
	runTest(t, "TestEncryptedVsUnencryptedDiskSizeWithDeltaFiles", testEncryptedVsUnencryptedDiskSizeWithDeltaFiles, "encryption")
}

func TestEncryptionGetActiveKeyIds(t *testing.T) {
	runTest(t, "TestEncryptionGetActiveKeyIds", testEncryptionGetActiveKeyIds, "encryption")
}

func TestEncryptionGetActiveKeyIdsWithDeltaFiles(t *testing.T) {
	runTest(t, "TestEncryptionGetActiveKeyIdsWithDeltaFiles", testEncryptionGetActiveKeyIdsWithDeltaFiles, "encryption")
}

func TestEncryptionGetActiveKeyIdsManySnapshots(t *testing.T) {
	runTest(t, "TestEncryptionGetActiveKeyIdsManySnapshots", testEncryptionGetActiveKeyIdsManySnapshots, "encryption")
}

func TestEncryptionGetActiveKeyIdsEncryptedUnencryptedSnapshot(t *testing.T) {
	runTest(t, "TestEncryptionGetActiveKeyIdsEncryptedUnencryptedSnapshot", testEncryptionGetActiveKeyIdsEncryptedUnencryptedSnapshot, "encryption")
}

func TestEncryptionGetActiveKeyIdsWithConcurrentRemoveSnapshot(t *testing.T) {
	runTest(t, "TestEncryptionGetActiveKeyIdsWithConcurrentRemoveSnapshot", testEncryptionGetActiveKeyIdsWithConcurrentRemoveSnapshot, "encryption")
}

func TestEncryptionDropKeyIdsFromSnapshot(t *testing.T) {
	runTest(t, "TestEncryptionDropKeyIdsFromSnapshot", testEncryptionDropKeyIdsFromSnapshot, "encryption")
}

func TestEncryptionDropAllKeyIdsFromSnapshot(t *testing.T) {
	runTest(t, "TestEncryptionDropAllKeyIdsFromSnapshot", testEncryptionDropAllKeyIdsFromSnapshot, "encryption")
}

func TestEncryptionDropKeyIdsFromSnapshotWithConcurrentRemoveSnapshot(t *testing.T) {
	runTest(t, "TestEncryptionDropKeyIdsFromSnapshotWithConcurrentRemoveSnapshot", testEncryptionDropKeyIdsFromSnapshotWithConcurrentRemoveSnapshot, "encryption")
}

func TestEncryptionDropKeyIdsConcurrent(t *testing.T) {
	runTest(t, "TestEncryptionDropKeyIdsConcurrent", testEncryptionDropKeyIdsConcurrent, "encryption")
}

func TestEncryptionDropKeyIdsConcurrentManyInstances(t *testing.T) {
	runTest(t, "TestEncryptionDropKeyIdsConcurrentManyInstances", testEncryptionDropKeyIdsConcurrentManyInstances, "encryption")
}

func TestEncryptionDropKeyIdsCorruptSnapshot(t *testing.T) {
	runTest(t, "TestEncryptionDropKeyIdsCorruptSnapshot", testEncryptionDropKeyIdsCorruptSnapshot, "encryption")
}

func TestEncryptionCleanupDropKeyFiles(t *testing.T) {
	runTest(t, "TestEncryptionCleanupDropKeyFiles", testEncryptionCleanupDropKeyFiles, "encryption")
}

func TestEncryptionStaleSnapshotCleanup(t *testing.T) {
	runTest(t, "TestEncryptionStaleSnapshotCleanup", testEncryptionCleanupStaleSnapshotFromSnapKeys, "encryption")
}

func TestEncryptionStats(t *testing.T) {
	runTest(t, "TestEncryptionStats", testEncryptionStats, "encryption")
}

func TestEncryptionEmptyIndex(t *testing.T) {
	runTest(t, "TestEncryptionEmptyIndex", testEncryptionEmptyIndex, "encryption")
}

func TestEncryptionUnsupportedCipher(t *testing.T) {
	runTest(t, "TestEncryptionUnsupportedCipher", testEncryptionUnsupportedCipher, "encryption")
}

func TestEncryptionLoadFromDiskDecryptionError(t *testing.T) {
	runTest(t, "TestEncryptionLoadFromDiskDecryptionError", testEncryptionLoadSnapshotDecryptionError, "encryption")
}
