// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package memdb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/couchbase/gocbcrypto"
	"github.com/couchbase/indexing/secondary/iowrap"
	"github.com/couchbase/indexing/secondary/logging"
)

const (
	dataFilePrefix = "shard-"

	// extension for encrypted files
	rencrypt_tmp_ext = ".aes.tmp"
	encrypt_bak_ext  = ".bak"

	// encryption status for stats
	StatusEncrypted     = "encrypted"
	StatusNotEncrypted  = "not_encrypted"
	StatusPartEncrypted = "partially_encrypted"
)

var (
	unencryptedFiles = []string{"checksums.json", "files.json", "nitro.json"}

	// KDF label/context for memdb encryption
	KDFLabelCtx = []byte("indexing/memdb")

	// use empty slice for unencrypted files
	NullKeyId = []byte{}
)

func isDataFile(fpath string) bool {
	return strings.HasPrefix(filepath.Base(fpath), dataFilePrefix)
}

type RotationType int

const (
	NoRotation RotationType = iota + 1
	Encrypt
	Decrypt
	Reencrypt
)

func rotTypString(rt RotationType) string {
	switch rt {
	case NoRotation:
		return "NoRotation"
	case Encrypt:
		return "Encrypt"
	case Decrypt:
		return "Decrypt"
	case Reencrypt:
		return "Reencrypt"
	default:
		return ""
	}
}

// masterKey and keyId are in a pair
type GetKeyByIdCb func(keyId []byte) (masterKey []byte, outKeyId []byte, cipher string)

// statistics
type EncryptionStats struct {
	// gauge
	NumActiveKeys uint32 `json:"keys"`              // includes current key even if no snapshots are present
	Status        string `json:"encryption_status"` // partial/encrypted/not_encrypted

	// counter
	NumFilesRotated     uint64 `json:"files_rotated"`
	NumBytesRotated     uint64 `json:"bytes_rotated"`
	NumFilesErrRencrypt uint64 `json:"file_rencrypt_errors"`
	NumFilesErrEncrypt  uint64 `json:"file_encrypt_errors"`
	NumFilesErrDecrypt  uint64 `json:"file_decrypt_errors"`

	// gauge, internal stats used for test validation
	numFiles                uint64
	numFilesPendingRencrypt uint64
	numFilesPendingEncrypt  uint64
	numFilesPendingDecrypt  uint64
}

func (e *EncryptionStats) String() string {
	if e != nil {
		if bs, err := json.MarshalIndent(e, "", " "); err == nil && len(bs) > 0 {
			return string(bs)
		}
	}
	return "{}"
}

// keyIdVisitor collects unique key IDs from encrypted files
type keyIdVisitor struct {
	db         *MemDB
	keyId      []byte // current keyId
	keyIds     [][]byte
	allowEmpty bool // allow reporting of empty keyId (unencrypted)
	EncryptionStats
}

// keyRotationVisitor re-encrypts files for DropKeys
type keyRotationVisitor struct {
	db             *MemDB
	dropKeyIds     [][]byte
	dstKeyId       []byte
	cipher         string
	candidateFiles []string
	EncryptionStats
}

// cleanup stale files from failed DropKeys operations
type keyRotationJanitor struct {
	candidates []string
}

// visitor for file encryption operations.
type encryptedFileVisitor interface {
	visit(ctx context.Context, path string) error
	process(ctx context.Context) error
}

// dirOpGuard coordinates directory operations with cancellation.
// It ensures only one operation runs per directory, while allowing cleanup
// to preempt long-running operations like encryption.
// e.g. key rotation is preemptible by snapshot cleanup
type dirOpGuard struct {
	mu         sync.RWMutex
	operations map[string]*dirOpCtx // dir path -> op
}

type dirOpCtx struct {
	dir string
	// cancelCtx is cancelled to preempt the operation; doneCtx is closed when the operation is done
	cancelCtx, doneCtx context.Context
	// cancel is called by preempting operation; done is called by current operation when it finishes
	cancel, done context.CancelFunc
}

func (m *MemDB) NewEncryptionContext(keyId []byte, cipher string) (gocbcrypto.EncryptionContext, error) {
	if cipher == gocbcrypto.CipherNameAES256GCM {
		if len(keyId) == 0 {
			return nil, gocbcrypto.ErrInvalidArgs
		}
		return gocbcrypto.NewAESGCM256ContextWithOpenSSL(keyId, m.GetEncryptionKeyById(keyId), KDFLabelCtx, 0)
	}

	return nil, gocbcrypto.ErrCipherUnsupported
}

// snapshotDirs are used to preload active snapshot key IDs
// so keys are not deleted before snapshots are loaded.
func (m *MemDB) initEncryption(snapDirs []string) (err error) {
	m.dirGuard = newDirOpGuard()
	m.encCtx, m.cancelCtx = context.WithCancel(context.Background())

	defer func() {
		if err != nil && m.cancelCtx != nil {
			m.cancelCtx()
			m.cancelCtx = nil
		}
	}()

	if err = m.restoreCurrentEncryptionKey(); err != nil {
		return err
	}

	if err2 := m.cleanupStaleDropKeyFilesFromSnapshot(); err2 != nil {
		logging.Errorf("MemDB::%v cleanupStaleDropKeyFilesFromSnapshot error:%v", m.Path, err2)
	}

	// do not fail initialization if there is an error. memDbSlice openSnapshot error handling
	// retries LoadFromDisk from successive snapshots on error
	if _, snapKeyIds, _ := m.getActiveKeyIdsFromSnapshots(snapDirs); len(snapKeyIds) > 0 {
		m.snapKeyIds = snapKeyIds
	}

	return nil
}

// current key id is not persisted. recover it using callback.
func (m *MemDB) restoreCurrentEncryptionKey() error {
	if m.GetKeyById != nil {
		key, keyId, cipher := m.GetKeyById(nil)
		if err := m.SetCurrentEncryptionKey(key, keyId, cipher); err != nil {
			return err
		}
	} else {
		m.encKeyId = NullKeyId
	}

	return nil
}

func (m *MemDB) stopEncryption() {
	if m.cancelCtx != nil {
		m.cancelCtx()
		m.cancelCtx = nil
	}
}

// unencrypted -> encrypted
func (m *MemDB) encryptFileByItem(ctx context.Context, src, dst string, keyId []byte, cipher string) (uint64, error) {
	if ctx == nil || len(src) == 0 || len(dst) == 0 {
		return 0, gocbcrypto.ErrInvalidArgs
	}

	r, err := m.newFileReader(m.fileType, version, src)
	if err != nil {
		return 0, fmt.Errorf("new filereader: %w", err)
	}

	err = r.Open(src)
	if err != nil {
		return 0, fmt.Errorf("open filereader: %w", err)
	}

	defer func() {
		if err2 := r.Close(); err2 != nil {
			logging.Warnf("close filereader %v failed: %v", src, err2)
		}
	}()

	d, err := m.newFileWriter(m.fileType, dst, keyId, cipher)
	if err != nil {
		return 0, fmt.Errorf("new filewriter: %w", err)
	}

	err = d.Open()
	if err != nil {
		return 0, fmt.Errorf("open filewriter: %w", err)
	}

	defer func() {
		if err != nil {
			if err2 := d.Close(true); err2 != nil {
				logging.Warnf("close filewriter %v failed: %v", dst, err2)
			}
		}
	}()

	var bytesWritten uint64

	for {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}

		itm, err := r.ReadItem()
		if err != nil {
			return 0, fmt.Errorf("read item: %w", err)
		}

		// Terminator
		if itm == nil {
			break
		}

		if err = d.WriteItem(itm); err != nil {
			m.freeItem(itm)
			return 0, fmt.Errorf("write item: %w", err)
		}

		bytesWritten += uint64(len(itm.Bytes()))
		m.freeItem(itm)
	}

	if err = d.FlushAndClose(true); err != nil {
		return 0, fmt.Errorf("flush and close writer: %w", err)
	}

	// re-opens and writes the Terminator
	if err = d.Close(true); err != nil {
		return 0, fmt.Errorf("close writer: %w", err)
	}

	return bytesWritten, nil
}

//////////
// apis
//////////

// sets the encryption key ID and cipher for new files.
// To enable encryption: pass keyId and gocbcrypto.CipherNameAES256GCM.
// To disable encryption: pass nil keyId and gocbcrypto.CipherNameNone.
// key : can be nil when called during recovery
// (assumption: keyId is never recycled)
func (m *MemDB) SetCurrentEncryptionKey(key []byte, keyId []byte, cipher string) error {
	m.encMu.Lock()
	defer m.encMu.Unlock()

	switch cipher {
	case gocbcrypto.CipherNameAES256GCM:
		if len(keyId) > 0 && len(keyId) <= 36 && len(key) == gocbcrypto.AES_256_GCM_KEY_SZ {
			m.encKeyId = make([]byte, len(keyId))
			copy(m.encKeyId, keyId)
		} else {
			return fmt.Errorf("%w key length:%v keyId length :%v",
				gocbcrypto.ErrInvalidArgs, len(key), len(keyId))
		}

	case gocbcrypto.CipherNameNone:
		if len(keyId) == 0 {
			m.encKeyId = NullKeyId
		} else {
			return gocbcrypto.ErrInvalidArgs
		}
	default:
		return gocbcrypto.ErrCipherUnsupported
	}

	return nil
}

// returns the master key for the given key ID.
// if the key ID is nil, it returns the current encryption key.
func (m *MemDB) GetEncryptionKeyById(keyID []byte) []byte {
	if m.GetKeyById != nil {
		key, _, _ := m.GetKeyById(keyID)
		return key
	}

	return nil
}

// returns a copy of current encryption key ID and cipher
func (m *MemDB) GetCurrentKeyId() ([]byte, string) {
	m.encMu.RLock()
	defer m.encMu.RUnlock()

	if len(m.encKeyId) > 0 {
		keyId := make([]byte, len(m.encKeyId))
		copy(keyId, m.encKeyId)
		return keyId, gocbcrypto.CipherNameAES256GCM
	}

	return nil, gocbcrypto.CipherNameNone
}

// registers current keyId for a snapshot. It is called during snapshot creation
// and key rotation to keep track of active keys for snapshots.
func (m *MemDB) RegisterSnapshotKeyId(snapDir string) (keyId []byte, cipher string, exists bool) {
	m.encMu.Lock()
	defer m.encMu.Unlock()

	if len(m.encKeyId) > 0 {
		keyId = make([]byte, len(m.encKeyId))
		copy(keyId, m.encKeyId)
		cipher = gocbcrypto.CipherNameAES256GCM
	} else {
		keyId = m.encKeyId // empty slice
		cipher = gocbcrypto.CipherNameNone
	}

	// register only once per snapshot
	if exists = keyIdExists(m.snapKeyIds[snapDir], keyId); !exists {
		m.snapKeyIds[snapDir] = append(m.snapKeyIds[snapDir], keyId)
	}

	return
}

func (m *MemDB) DeregisterSnapshotKeyId(snapDir string, keyId []byte) {
	m.encMu.Lock()
	defer m.encMu.Unlock()

	if keyIds, ok := m.snapKeyIds[snapDir]; ok {
		if rem := removeKeyIdFromList(keyIds, keyId); len(rem) == 0 {
			delete(m.snapKeyIds, snapDir)
		} else {
			m.snapKeyIds[snapDir] = rem
		}
	}
}

func (m *MemDB) DeregisterSnapshot(snapDir string) {
	m.encMu.Lock()
	defer m.encMu.Unlock()

	delete(m.snapKeyIds, snapDir)
}

func (m *MemDB) GetActiveKeyIdList() [][]byte {
	m.encMu.RLock()
	defer m.encMu.RUnlock()

	var result [][]byte
	for _, keyIds := range m.snapKeyIds {
		for _, kid := range keyIds {
			result = appendUniqueKeyId(result, kid)
		}
	}

	// include current encryption key if not present
	result = appendUniqueKeyId(result, m.encKeyId)
	return result
}

// called only during startup
// result will have an empty keyId if snapshot is not encrypted
func (m *MemDB) getActiveKeyIdsFromSnapshots(snapDirs []string) ([][]byte, map[string][][]byte, error) {
	var firstErr error
	result := make([][]byte, 0)
	snapKeyIds := make(map[string][][]byte)

	for i := range snapDirs {
		if keyIds, err := m.getActiveKeyIdsFromSnapshot(snapDirs[i]); err == nil {
			for _, keyId := range keyIds {
				result = appendUniqueKeyId(result, keyId)
			}
			snapKeyIds[snapDirs[i]] = keyIds
		} else if firstErr == nil { // continue
			firstErr = err
		}
	}

	return result, snapKeyIds, firstErr
}

// returns unique encryption key IDs currently in use by snapshot.
// If a snapshot is not encrypted, it returns an empty list.
func (m *MemDB) getActiveKeyIdsFromSnapshot(snapDir string) ([][]byte, error) {
	m.encMu.RLock()
	if keyIds, ok := m.snapKeyIds[snapDir]; ok {
		res := deepCopyKeyIds(keyIds)
		m.encMu.RUnlock()
		return res, nil
	}
	m.encMu.RUnlock()

	g, err := m.dirGuard.TryAcquire(snapDir, m.encCtx)
	if err != nil {
		return nil, err // snapshot is being cleaned up or rotated
	}
	defer m.dirGuard.Release(g)

	v := &keyIdVisitor{
		db:         m,
		allowEmpty: true,
	}
	if err := m.walkEncryptedFiles(snapDir, g.cancelCtx, v); err != nil {
		return nil, err
	}
	return v.keyIds, nil
}

func (v *keyIdVisitor) visit(ctx context.Context, path string) error {
	var keyId []byte

	defer func() {
		atomic.AddUint64(&v.numFiles, 1)
	}()

	// skip temporary and backup files
	if v.skip(path) {
		return nil
	}

	if ok, err := gocbcrypto.IsFileEncrypted(path); err != nil {
		return err
	} else if ok {
		keyId, err = ReadFileKeyId(path, v.db.GetEncryptionKeyById)
		if err != nil {
			return err
		}
	}

	//  include all encrypted and unencrypted files
	if len(keyId) > 0 || v.allowEmpty {
		v.keyIds = appendUniqueKeyId(v.keyIds, keyId)
	}

	// collect stats
	if len(keyId) == 0 && len(v.keyId) > 0 {
		atomic.AddUint64(&v.numFilesPendingEncrypt, 1)
	} else if len(keyId) > 0 && len(v.keyId) == 0 {
		atomic.AddUint64(&v.numFilesPendingDecrypt, 1)
	} else if len(keyId) > 0 && !bytes.Equal(keyId, v.keyId) {
		atomic.AddUint64(&v.numFilesPendingRencrypt, 1)
	}

	return nil
}

func (v *keyIdVisitor) process(ctx context.Context) error {
	return nil
}

func (v *keyIdVisitor) skip(fpath string) bool {
	return len(fpath) > 0 && (strings.HasSuffix(fpath, rencrypt_tmp_ext) ||
		strings.HasSuffix(fpath, encrypt_bak_ext) ||
		strings.HasSuffix(fpath, ".tmp"))
}

// re-encrypts all files which have the specified key IDs with current
// encryption key. The api is blocking and is called asynchrously by a groutine.
// It can be cancelled by concurrent snapshot cleanup operation or instance close
func (m *MemDB) DropKeyIdsFromSnapshot(keyIds [][]byte, snapDir string) error {
	if len(keyIds) == 0 {
		return ErrInvalid
	}

	if keyIdList, err := m.getActiveKeyIdsFromSnapshot(snapDir); err == nil {
		hasDropKey := false
		for _, keyId := range keyIds {
			if keyIdExists(keyIdList, keyId) {
				hasDropKey = true
				break
			}
		}
		if !hasDropKey {
			return nil
		}
	}

	// cleanup could be in progress
	g, err := m.dirGuard.TryAcquire(snapDir, m.encCtx)
	if err != nil {
		return err
	}
	defer m.dirGuard.Release(g)

	// add current key
	keyId, cipher, exists := m.RegisterSnapshotKeyId(snapDir)

	r := &keyRotationVisitor{
		db:             m,
		dropKeyIds:     keyIds,
		candidateFiles: make([]string, 0),
		dstKeyId:       keyId,
		cipher:         cipher,
	}

	if err = m.walkEncryptedFiles(snapDir, g.cancelCtx, r); err != nil {
		if r.NumFilesRotated == 0 && !exists {
			m.DeregisterSnapshotKeyId(snapDir, keyId) // remove current key if no files were encrypted with it
		}
		return err
	}

	for _, dropKey := range keyIds {
		m.DeregisterSnapshotKeyId(snapDir, dropKey)
	}
	return nil
}

func (v *keyRotationVisitor) visit(ctx context.Context, fpath string) error {
	var fkeyId []byte

	// clean up stale rotation files from previous attempts (if janitor fails cleanup)
	if _, _, err := handleStaleRotationFile(fpath); err != nil {
		return err
	}

	if ok, err := gocbcrypto.IsFileEncrypted(fpath); err != nil {
		return fmt.Errorf("keyRotationVisitor %s: %w", fpath, err)
	} else if ok {
		if fkeyId, err = ReadFileKeyId(fpath, v.db.GetEncryptionKeyById); err != nil {
			return err
		}
	}

	for _, dropKey := range v.dropKeyIds {
		// candidate can be unencrypted file with empty keyId
		if bytes.Equal(dropKey, fkeyId) {
			v.candidateFiles = append(v.candidateFiles, fpath)
			return nil
		}
	}

	return nil
}

func (v *keyRotationVisitor) getRotationType(fpath string, tgtKeyId []byte) (rt RotationType, err error) {
	encrypted, err := gocbcrypto.IsFileEncrypted(fpath)
	if err != nil {
		return NoRotation, fmt.Errorf("keyRotationVisitor %s: %w", fpath, err)
	}

	if encrypted {
		// file is encrypted, decrypt it
		if len(tgtKeyId) == 0 {
			rt = Decrypt
			return
		}
		rt = Reencrypt
		return
	}

	// file is unencrypted, encrypt it
	if len(tgtKeyId) > 0 {
		rt = Encrypt
		return
	}

	// file is unencrypted and encryption disabled, do nothing
	rt = NoRotation
	return
}

func (v *keyRotationVisitor) process(ctx context.Context) error {
	var err error
	var wg sync.WaitGroup
	var errMu sync.Mutex

	if ctx == nil {
		return ErrInvalid
	}

	for _, file := range v.candidateFiles {
		wg.Add(1)
		if gDropKeySem.Acquire(ctx, 1) != nil {
			wg.Done()
			break // context cancelled, stop scheduling more rotations
		}
		if !gWriteBarrier.get() {
			gDropKeySem.Release(1)
			wg.Done()
			break // write barrier unavailable
		}
		go func(file string) {
			defer wg.Done()
			defer gDropKeySem.Release(1)
			defer gWriteBarrier.release()

			if er := v.rotateSingleFile(ctx, file, v.dstKeyId, v.cipher); er != nil {
				errMu.Lock()
				defer errMu.Unlock()

				// override last error
				if v.isFatalError(er) {
					err = fmt.Errorf("%w fatal", er)
					return
				}

				// continue rotation, error could be transient
				if err == nil {
					err = er
				}
			}
		}(file)
	}

	wg.Wait()

	atomic.AddUint64(&v.db.encSts.NumFilesRotated, v.NumFilesRotated)
	atomic.AddUint64(&v.db.encSts.NumBytesRotated, v.NumBytesRotated)
	atomic.AddUint64(&v.db.encSts.NumFilesErrDecrypt, v.NumFilesErrDecrypt)
	atomic.AddUint64(&v.db.encSts.NumFilesErrEncrypt, v.NumFilesErrEncrypt)
	atomic.AddUint64(&v.db.encSts.NumFilesErrRencrypt, v.NumFilesErrRencrypt)

	if err == nil {
		err = ctx.Err()
	}
	return err
}

// rotateSingleFile re-encrypts a file with a new encryption key
// rencryption does not require to update checksums.json as checksum is computed on unencrypted data.
func (v *keyRotationVisitor) rotateSingleFile(ctx context.Context, file string, keyId []byte, cipher string) error {
	var bytesWritten uint64

	tmpDst := file + rencrypt_tmp_ext
	backup := file + encrypt_bak_ext

	if err := iowrap.Os_Remove(tmpDst); err != nil && !os.IsNotExist(err) {
		logging.Errorf("MemDB::rotateSingleFile remove tmp %v failed: %v", tmpDst, err)
		return err
	}

	if err := iowrap.Os_Remove(backup); err != nil && !os.IsNotExist(err) {
		logging.Errorf("MemDB::rotateSingleFile remove backup %v failed: %v", backup, err)
		return err
	}

	defer func() {
		if err := iowrap.Os_Remove(backup); err != nil && !os.IsNotExist(err) {
			logging.Warnf("MemDB::rotateSingleFile cleanup backup %v failed: %v", backup, err)
		}
		if err := iowrap.Os_Remove(tmpDst); err != nil && !os.IsNotExist(err) {
			logging.Warnf("MemDB::rotateSingleFile cleanup tmp %v failed: %v", tmpDst, err)
		}
		if syncErr := Dir_Sync(filepath.Dir(file), 0o755); syncErr != nil {
			logging.Warnf("MemDB::rotateSingleFile dir sync for %v failed: %v", file, syncErr)
		}
	}()

	rotType, err := v.getRotationType(file, keyId)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			logging.Errorf("MemDB::rotateSingleFile %v rotation:%v error:%v",
				file, rotTypString(rotType), err)
		}
	}()

	var encryptCtx gocbcrypto.EncryptionContext

	switch rotType {
	case NoRotation:
		return nil
	case Reencrypt:
		encryptCtx, err = v.db.NewEncryptionContext(keyId, cipher)
		if err != nil {
			atomic.AddUint64(&v.NumFilesErrRencrypt, 1)
			return err
		}

		// encryption by block should be faster than by item
		bytesWritten, err = gocbcrypto.ReencryptFileByChunk(ctx, file, tmpDst,
			encryptCtx, v.db.GetEncryptionKeyById, KDFLabelCtx, iowrap.CountDiskFailures)

		if err != nil && !errors.Is(err, context.Canceled) {
			atomic.AddUint64(&v.NumFilesErrRencrypt, 1)
		}

	case Decrypt:
		bytesWritten, err = gocbcrypto.DecryptFileByChunk(ctx, file, tmpDst,
			v.db.GetEncryptionKeyById, KDFLabelCtx, iowrap.CountDiskFailures)

		if err != nil && !errors.Is(err, context.Canceled) {
			atomic.AddUint64(&v.NumFilesErrDecrypt, 1)
		}

	case Encrypt:
		if isDataFile(file) {
			bytesWritten, err = v.db.encryptFileByItem(ctx, file, tmpDst, keyId, cipher)
		} else {
			// manifest
			var bs []byte
			if bs, err = iowrap.Os_ReadFile(file); err == nil {
				if encryptCtx, err = v.db.NewEncryptionContext(keyId, cipher); err == nil {
					err = gocbcrypto.WriteFile(tmpDst, bs, FilePermMode, encryptCtx, iowrap.CountDiskFailures)
				}
			}
		}

		if err != nil && !errors.Is(err, context.Canceled) {
			atomic.AddUint64(&v.NumFilesErrEncrypt, 1)
		}

	default:
		return ErrInvalidRotationType
	}

	if err != nil {
		return err
	}

	// Rename original file to backup
	if err = iowrap.Os_Rename(file, backup); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	// Rename re-encrypted/encrypted/decrypted file to target
	if err = iowrap.Os_Rename(tmpDst, file); err != nil {
		// Attempt failed, restore original file
		if restoreErr := iowrap.Os_Rename(backup, file); restoreErr != nil {
			// restore has failed. snapshot is in an inconsistent state.
			// a) We hope it is resolved in next rotation as it may be due to a transient error.
			// b) in case there is a prior restart, checksum will fail
			// For now, we just log an error
			if !os.IsNotExist(restoreErr) {
				err = fmt.Errorf("%v:%v file:%v original err:%v",
					ErrKeyRotationRestore, restoreErr, file, err)
			}
		}

		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	atomic.AddUint64(&v.NumFilesRotated, 1)
	atomic.AddUint64(&v.NumBytesRotated, bytesWritten)
	return nil
}

// decryption error
func (v *keyRotationVisitor) isFatalError(err error) bool {
	return gocbcrypto.IsDecryptionError(err)
}

func (m *MemDB) RemoveSnapshot(snapDir string) error {
	g := m.dirGuard.Acquire(snapDir, m.encCtx)
	if g == nil {
		return ErrInvalid
	}
	defer m.dirGuard.Release(g)

	err := iowrap.Os_RemoveAll(snapDir)
	if err != nil {
		return err
	}

	m.DeregisterSnapshot(snapDir)
	return nil
}

// cleans up temporary and backup files after a crash during key rotation.
// It should be called on startup before any key rotation operations start.
func (m *MemDB) cleanupStaleDropKeyFilesFromSnapshot() error {
	if _, err := iowrap.Os_Stat(m.Path); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	v := &keyRotationJanitor{
		candidates: make([]string, 0),
	}
	// cannot be cancelled as it is called only during bootstrap
	return m.walkEncryptedFiles(m.Path, m.encCtx, v)
}

func (v *keyRotationJanitor) visit(ctx context.Context, file string) error {
	// accept only temporary and backup files
	if strings.HasSuffix(file, rencrypt_tmp_ext) ||
		strings.HasSuffix(file, encrypt_bak_ext) {
		v.candidates = append(v.candidates, file)
	}
	return nil
}

func (v *keyRotationJanitor) process(ctx context.Context) error {
	var err error
	var cleanup, restored int

	defer func() {
		if err != nil || (cleanup+restored) > 0 {
			logging.Infof("MemDB::keyRotationJanitor cleanup_files:%v restored_files:%v files:%v error:%v",
				cleanup, restored, len(v.candidates), err)
		}
	}()

	for _, f := range v.candidates {
		var er error
		restored, cleanup, er = handleStaleRotationFile(f)
		if er != nil && !os.IsNotExist(er) {
			if err == nil {
				err = er
			}
			logging.Errorf("MemDB::keyRotationJanitor %v error %v", f, er)
		}
	}

	return err
}

// restores the original file from a backup if needed, or removes stale backup/temp files
// from failed rotation attempt due to crash
func handleStaleRotationFile(f string) (restored, cleanedUp int, err error) {
	if strings.HasSuffix(f, encrypt_bak_ext) {
		orig := strings.TrimSuffix(f, encrypt_bak_ext)
		if _, er := iowrap.Os_Stat(orig); er != nil {
			if os.IsNotExist(er) {
				if er = iowrap.Os_Rename(f, orig); er == nil {
					restored++
				}
			} else {
				if er = iowrap.Os_Remove(f); er == nil {
					cleanedUp++
				}
			}
		} else {
			er := iowrap.Os_Remove(f)
			if er == nil {
				cleanedUp++
			}
		}
	} else if strings.HasSuffix(f, rencrypt_tmp_ext) {
		if er := iowrap.Os_Remove(f); er == nil {
			cleanedUp++
		}
	}
	return
}

// returns encryption statistics for all snapshots.
// Rotation statistics (NumFilesRotated, NumBytesRotated, error counters) are
// always included and updated incrementally during key rotation operations.
func (m *MemDB) GetEncryptionStatsCached() (EncryptionStats, error) {
	m.encMu.RLock()
	defer m.encMu.RUnlock()

	var kids, snapkeyIds [][]byte
	var emptyKey bool
	for _, v := range m.snapKeyIds {
		for _, kid := range v {
			if len(kid) == 0 {
				emptyKey = true
			}
			kids = appendUniqueKeyId(kids, kid)
		}
	}

	// include current encryption key if present (non-empty)
	snapkeyIds = kids
	if len(m.encKeyId) > 0 {
		kids = appendUniqueKeyId(kids, m.encKeyId)
	}

	numKeys := ^uint32(0)
	if len(kids) <= math.MaxUint32 {
		numKeys = uint32(len(kids))
	}
	if emptyKey {
		numKeys-- // do not count unencrypted state as a key
	}

	status := StatusEncrypted
	if len(snapkeyIds) == 0 || (len(snapkeyIds) == 1 && emptyKey) {
		status = StatusNotEncrypted
	}

	if len(m.encKeyId) > 0 {
		if len(snapkeyIds) > 1 && emptyKey {
			status = StatusPartEncrypted
		}
	} else {
		if len(snapkeyIds) > 1 {
			status = StatusPartEncrypted
		}
	}

	var encSts EncryptionStats
	if m.encSts != nil {
		encSts = *m.encSts
	}

	atomic.StoreUint32(&encSts.NumActiveKeys, numKeys)
	encSts.Status = status

	return encSts, nil
}

// returns encryption statistics by scanning snapshot files on disk.
// It is called by tests only for validation
func (m *MemDB) GetEncryptionStatsFromDisk() (EncryptionStats, error) {
	v := &keyIdVisitor{
		db:         m,
		allowEmpty: false,
	}

	// needed to derive rencryption stats
	if keyId, _ := m.GetCurrentKeyId(); len(keyId) > 0 {
		v.keyId = keyId
		v.keyIds = append(v.keyIds, keyId)
	}

	if err := m.walkEncryptedFiles(m.Path, m.encCtx, v); err != nil {
		return EncryptionStats{}, err
	}

	v.Status = StatusEncrypted
	if len(v.keyId) > 0 {
		if pending := v.numFilesPendingRencrypt + v.numFilesPendingEncrypt; pending > 0 {
			if v.numFilesPendingRencrypt == 0 {
				v.Status = StatusNotEncrypted // offline upgrade/recovery or encryption enabled
			} else if v.numFilesPendingEncrypt > 0 {
				v.Status = StatusPartEncrypted
			}
		}
	} else {
		// encryption disabled
		if v.numFilesPendingDecrypt == 0 {
			v.Status = StatusNotEncrypted
		} else if v.numFilesPendingDecrypt < v.numFiles {
			v.Status = StatusPartEncrypted
		}
	}

	var encSts EncryptionStats
	if m.encSts != nil {
		encSts = *m.encSts
	}

	if len(v.keyIds) > math.MaxUint32 {
		encSts.NumActiveKeys = math.MaxUint32
	} else {
		encSts.NumActiveKeys = uint32(len(v.keyIds))
	}
	encSts.numFiles = v.numFiles
	encSts.numFilesPendingRencrypt = v.numFilesPendingRencrypt
	encSts.numFilesPendingEncrypt = v.numFilesPendingEncrypt
	encSts.numFilesPendingDecrypt = v.numFilesPendingDecrypt
	encSts.Status = v.Status

	return encSts, nil
}

func (m *MemDB) walkEncryptedFiles(dir string, ctx context.Context, visitor encryptedFileVisitor) error {

	// skip files which we do not want to encrypt
	skip := func(fpath string) bool {
		for _, f := range unencryptedFiles {
			if filepath.Base(fpath) == f {
				return true
			}
		}
		return false
	}

	if _, err := iowrap.Os_Stat(dir); err != nil {
		return err
	}

	if err := filepath.WalkDir(dir, func(path string, f fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Check for context cancellation to enable preemption
		select {
		case <-ctx.Done():
			return fmt.Errorf("walk cancelled: %w", ctx.Err())
		default:
		}

		if f.IsDir() || skip(f.Name()) {
			return nil
		}

		return visitor.visit(ctx, path)
	}); err != nil {
		return err
	}

	return visitor.process(ctx)
}

//////////
// utils
//////////

func ReadFileKeyId(filepath string, getKeyId func([]byte) []byte) ([]byte, error) {
	fd, err := iowrap.Os_Open(filepath)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err2 := iowrap.File_Close(fd); err2 != nil {
			logging.Warnf("MemDB::ReadFileKeyId close file %s failed: %v", filepath, err2)
		}
	}()

	rd, err := gocbcrypto.NewCryptFileReaderWithLabel(fd, getKeyId, KDFLabelCtx, gocbcrypto.ChunkSize, false, iowrap.CountDiskFailures)
	if err != nil {
		logging.Errorf("MemDB::ReadFileKeyId %s: %v", filepath, err)
		if errors.Is(err, gocbcrypto.ErrCipherKeyLookup) {
			return nil, ErrSnapshotKeyIdMissing
		}
		return nil, err
	}
	defer rd.Reset()

	return rd.GetCtx().KeyID(), nil
}

// key compare helper
func appendUniqueKeyId(result [][]byte, newId []byte) [][]byte {
	found := false
	for _, keyId := range result {
		if bytes.Equal(keyId, newId) {
			found = true
			break
		}
	}

	if !found {
		// appending empty slice just returns the original slice which is []byte(nil)
		if len(newId) == 0 {
			result = append(result, NullKeyId)
		} else {
			result = append(result, append([]byte(nil), newId...))
		}
	}

	return result
}

func keyIdExists(keyIds [][]byte, keyId []byte) bool {
	for _, existing := range keyIds {
		if bytes.Equal(existing, keyId) {
			return true
		}
	}

	return false
}

func removeKeyIdFromList(keyIds [][]byte, keyIdToDrop []byte) [][]byte {
	if len(keyIds) == 0 {
		return nil
	}

	rem := make([][]byte, 0, len(keyIds))
	for _, keyId := range keyIds {
		if bytes.Equal(keyId, keyIdToDrop) {
			continue
		}
		rem = append(rem, keyId)
	}

	return rem
}

func deepCopyKeyIds(keyIds [][]byte) [][]byte {
	if keyIds == nil {
		return nil
	}

	out := make([][]byte, len(keyIds))
	for i := range keyIds {
		if len(keyIds[i]) == 0 {
			out[i] = NullKeyId
			continue
		}
		out[i] = append([]byte(nil), keyIds[i]...)
	}

	return out
}

// ////////////////
// directory guard
// ////////////////

func newDirOpGuard() *dirOpGuard {
	return &dirOpGuard{
		operations: make(map[string]*dirOpCtx),
	}
}

// acquires exclusive lock on a directory with preemption.
// If dir is already locked, cancels the active op until owner releases the lock.
func (g *dirOpGuard) Acquire(dir string, ctx context.Context) *dirOpCtx {
	if len(dir) == 0 || ctx == nil {
		return nil
	}

	for {
		// retry on error
		if dCtx, _ := g.TryAcquire(dir, ctx); dCtx != nil {
			return dCtx
		}
		g.cancel(dir) // blocks until current op yields
	}
}

func (g *dirOpGuard) TryAcquire(dir string, ctx context.Context) (*dirOpCtx, error) {
	if len(dir) == 0 || ctx == nil {
		return nil, ErrInvalid
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if _, ok := g.operations[dir]; !ok {
		dCtx := &dirOpCtx{dir: dir}
		// derive from parent context (top->down)
		dCtx.cancelCtx, dCtx.cancel = context.WithCancel(ctx)
		// independent context
		dCtx.doneCtx, dCtx.done = context.WithCancel(context.Background())
		g.operations[dir] = dCtx
		return dCtx, nil
	}

	return nil, ErrSnapshotBusy
}

func (g *dirOpGuard) Release(dCtx *dirOpCtx) {
	if dCtx == nil {
		return
	}

	dCtx.cancel() // idempotent
	dCtx.done()

	g.mu.Lock()
	delete(g.operations, dCtx.dir)
	g.mu.Unlock()
}

func (g *dirOpGuard) cancel(dir string) {
	if len(dir) == 0 {
		return
	}

	g.mu.Lock()
	dCtx, ok := g.operations[dir]
	g.mu.Unlock()

	if ok {
		dCtx.cancel()
		<-dCtx.doneCtx.Done()
	}
}
