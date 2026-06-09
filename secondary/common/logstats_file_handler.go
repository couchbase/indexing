//go:build !community

package common

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/logstats/logstats"
	"github.com/couchbase/tools-common/couchbase/v4/cbcrypto"
)

// not a valid cbcrypto header (too small, or magic bytes mismatch).
var ErrCBCryptoHeader = errors.New("not valid cbcrypto header")

const cbcryptoHeaderSize = 80

const (
	cbcryptoIDLenOffset   = 27
	cbcryptoIDStartOffset = 28
	cbcryptoMaxIDLen      = 36
)

// encryptedStatsWriter is returned by LogStatsFileHandler when encryption is
// on. Each Write becomes one independently-sealed chunk.
type encryptedStatsWriter struct {
	f *os.File
	w *cbcrypto.CBCWriter
}

func (e *encryptedStatsWriter) Write(p []byte) (int, error) {
	if err := e.w.AppendChunk(bytes.NewReader(p)); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (e *encryptedStatsWriter) Close() error { return e.f.Close() }
func (e *encryptedStatsWriter) Sync() error  { return e.f.Sync() }

// LogStatsFileHandler implements logstats.FileHandler for encrypted stats logs.
// Open always returns a plain writer (encryption starts only the first Rotate).
// Rotate returns an encryptedStatsWriter when getKey returns a non-empty key,
// or a plain *os.File otherwise.
type LogStatsFileHandler struct {
	getKey     func() (keyID string, key []byte)
	getKeyByID func(keyID string) ([]byte, string)
}

func NewLogStatsFileHandler(
	getKey func() (keyID string, key []byte),
	getKeyByID func(keyID string) ([]byte, string),
) *LogStatsFileHandler {
	return &LogStatsFileHandler{getKey: getKey, getKeyByID: getKeyByID}
}

// DisableCompression is a no-op needed to satisfy interface contract
func (h *LogStatsFileHandler) DisableCompression() {}

func (h *LogStatsFileHandler) Open(fileName string) (logstats.SyncWriteCloser, int, error) {
	if err := os.MkdirAll(filepath.Dir(fileName), 0o755); err != nil {
		return nil, 0, err
	}

	keyID, err := GetStatsLogFileKeyID(fileName)
	if err == nil {
		// Active file is encrypted,  must continue writing encrypted.
		var keyBytes []byte
		if h.getKeyByID != nil {
			keyBytes, _ = h.getKeyByID(keyID)
		}
		if len(keyBytes) == 0 {
			return nil, 0, fmt.Errorf("LogStatsFileHandler.Open: active log encrypted with key %q but key is unavailable", keyID)
		}
		f, err := os.OpenFile(fileName, os.O_RDWR, 0o644)
		if err != nil {
			return nil, 0, err
		}
		w, err := cbcrypto.Open(f, keyBytes)
		if err != nil {
			f.Close()
			return nil, 0, fmt.Errorf("LogStatsFileHandler.Open: cbcrypto.Open: %w", err)
		}
		fi, err := f.Stat()
		if err != nil {
			f.Close()
			return nil, 0, err
		}
		return &encryptedStatsWriter{f: f, w: w}, int(fi.Size()), nil
	}
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	return f, int(fi.Size()), nil
}

// Rotate shifts backup files and opens a fresh active file. If getKey returns
// a non-empty key the new file is encrypted; otherwise it is plain text.
func (h *LogStatsFileHandler) Rotate(fileName string, numFiles int) (logstats.SyncWriteCloser, int, error) {
	keyID, key := h.getKey()

	if len(key) == 0 {
		return logstats.RotateLogFile(fileName, numFiles, false)
	}

	f, _, err := logstats.RotateLogFile(fileName, numFiles, false)
	if err != nil {
		return nil, 0, fmt.Errorf("LogStatsFileHandler.Rotate: %w", err)
	}

	w, err := cbcrypto.NewCBCWriter(f, cbcrypto.WriterOptions{
		KeyID:         keyID,
		Key:           key,
		KeyDerivation: cbcrypto.KeyBasedKDF,
		Compression:   cbcrypto.None,
	})
	if err != nil {
		f.Close()
		return nil, 0, fmt.Errorf("LogStatsFileHandler.Rotate: NewCBCWriter: %w", err)
	}
	return &encryptedStatsWriter{f: f, w: w}, 0, nil
}

func GetStatsLogFileKeyID(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	hdr := make([]byte, cbcryptoHeaderSize)
	if _, err := io.ReadFull(f, hdr); err != nil {
		return "", ErrCBCryptoHeader
	}
	if err := cbcrypto.Validate(bytes.NewReader(hdr)); err != nil {
		return "", ErrCBCryptoHeader
	}
	idLen := int(hdr[cbcryptoIDLenOffset])
	if idLen > cbcryptoMaxIDLen {
		return "", fmt.Errorf("invalid key ID length %d", idLen)
	}
	return string(hdr[cbcryptoIDStartOffset : cbcryptoIDStartOffset+idLen]), nil
}

// valid cbcrypto header.
func IsStatsLogActiveFileEncrypted(filePath string) bool {
	_, err := GetStatsLogFileKeyID(filePath)
	return err == nil
}

// Each file is written to a .tmp sibling then atomically renamed
// so a crash leaves either the old or the
// new file intact.
func ReencryptStatsLogFiles(
	logDir, baseName string,
	dropKeyIDs []string,
	newKeyID string,
	newKey []byte,
	getKeyCipher func(keyID string) ([]byte, string),
) error {
	if len(dropKeyIDs) == 0 || len(newKey) == 0 {
		return nil
	}

	dropSet := make(map[string]bool, len(dropKeyIDs))
	for _, id := range dropKeyIDs {
		dropSet[id] = true
	}

	stem := baseName
	if filepath.Ext(stem) == ".log" {
		stem = stem[:len(stem)-4]
	}
	rotated, _ := filepath.Glob(filepath.Join(logDir, stem+".log.*"))

	provider := statsLogKeyProvider(getKeyCipher)
	for _, path := range rotated {
		if strings.HasSuffix(path, ".tmp") || strings.HasSuffix(path, ".gz") {
			continue
		}
		if err := reencryptFileIfNeeded(path, dropSet, newKeyID, newKey, provider); err != nil {
			logging.Errorf("common::ReencryptStatsLogFiles %q: %v", path, err)
			return err
		}
	}
	return nil
}

// DecryptStatsLogFiles decrypts every rotated stats log file whose header key
// ID is in dropKeyIDs back to plaintext. Called when encryption is disabled.
func DecryptStatsLogFiles(
	logDir, baseName string,
	dropKeyIDs []string,
	getKeyCipher func(keyID string) ([]byte, string),
) error {
	if len(dropKeyIDs) == 0 {
		return nil
	}

	dropSet := make(map[string]bool, len(dropKeyIDs))
	for _, id := range dropKeyIDs {
		dropSet[id] = true
	}

	stem := baseName
	if filepath.Ext(stem) == ".log" {
		stem = stem[:len(stem)-4]
	}
	rotated, _ := filepath.Glob(filepath.Join(logDir, stem+".log.*"))

	provider := statsLogKeyProvider(getKeyCipher)
	for _, path := range rotated {
		if strings.HasSuffix(path, ".tmp") || strings.HasSuffix(path, ".gz") {
			continue
		}
		if err := decryptFileIfNeeded(path, dropSet, provider); err != nil {
			logging.Errorf("common::DecryptStatsLogFiles %q: %v", path, err)
			return err
		}
	}
	return nil
}

// statsLogKeyProvider wraps the getKeyCipher callback into a cbcrypto.KeyProvider.
func statsLogKeyProvider(getKeyCipher func(string) ([]byte, string)) cbcrypto.KeyProvider {
	return func(id string) ([]byte, error) {
		data, _ := getKeyCipher(id)
		if len(data) == 0 {
			return nil, fmt.Errorf("no key data for keyID %q", id)
		}
		return data, nil
	}
}

func reencryptFileIfNeeded(
	path string,
	dropSet map[string]bool,
	newKeyID string,
	newKey []byte,
	provider cbcrypto.KeyProvider,
) error {
	keyID, err := GetStatsLogFileKeyID(path)
	if err != nil {
		if !errors.Is(err, ErrCBCryptoHeader) {
			return err
		}
		// plaintext file — encrypt if "" is in the drop set
		if !dropSet[""] {
			return nil
		}
		tmp := path + ".reenc.tmp"
		if err := encryptPlaintextStatsFile(path, tmp, newKeyID, newKey); err != nil {
			os.Remove(tmp)
			return fmt.Errorf("encrypt plaintext: %w", err)
		}
		return os.Rename(tmp, path)
	}

	if !dropSet[keyID] {
		return nil
	}

	tmp := path + ".reenc.tmp"
	if err := reencryptStatsFile(path, tmp, provider, newKeyID, newKey); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("transcrypt: %w", err)
	}
	return os.Rename(tmp, path)
}

func decryptFileIfNeeded(path string, dropSet map[string]bool, provider cbcrypto.KeyProvider) error {
	keyID, err := GetStatsLogFileKeyID(path)
	if err != nil {
		if !errors.Is(err, ErrCBCryptoHeader) {
			return err
		}
		return nil
	}
	if !dropSet[keyID] {
		return nil
	}
	tmp := path + ".dec.tmp"
	if err := decryptStatsFile(path, tmp, provider); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("decrypt: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

func encryptPlaintextStatsFile(src, dst, newKeyID string, newKey []byte) error {
	srcF, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcF.Close()

	dstF, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	w, err := cbcrypto.NewCBCWriter(dstF, cbcrypto.WriterOptions{
		KeyID:         newKeyID,
		Key:           newKey,
		KeyDerivation: cbcrypto.KeyBasedKDF,
		Compression:   cbcrypto.None,
	})
	if err != nil {
		dstF.Close()
		return fmt.Errorf("NewCBCWriter: %w", err)
	}

	buf := make([]byte, 32*1024)
	for {
		n, readErr := srcF.Read(buf)
		if n > 0 {
			if err := w.AppendChunk(bytes.NewReader(buf[:n])); err != nil {
				dstF.Close()
				return fmt.Errorf("AppendChunk: %w", err)
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			dstF.Close()
			return fmt.Errorf("read: %w", readErr)
		}
	}
	return dstF.Close()
}

func reencryptStatsFile(src, dst string, provider cbcrypto.KeyProvider, newKeyID string, newKey []byte) error {
	srcF, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcF.Close()

	dstF, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	r, err := cbcrypto.NewReader(srcF, provider)
	if err != nil {
		dstF.Close()
		return fmt.Errorf("NewReader: %w", err)
	}

	w, err := cbcrypto.NewCBCWriter(dstF, cbcrypto.WriterOptions{
		KeyID:         newKeyID,
		Key:           newKey,
		KeyDerivation: cbcrypto.KeyBasedKDF,
		Compression:   cbcrypto.None,
	})
	if err != nil {
		dstF.Close()
		return fmt.Errorf("NewCBCWriter: %w", err)
	}

	buf := make([]byte, 32*1024)
	for {
		n, readErr := r.Read(buf)
		if n > 0 {
			if err := w.AppendChunk(bytes.NewReader(buf[:n])); err != nil {
				dstF.Close()
				return fmt.Errorf("AppendChunk: %w", err)
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			dstF.Close()
			return fmt.Errorf("read: %w", readErr)
		}
	}
	return dstF.Close()
}

func decryptStatsFile(src, dst string, provider cbcrypto.KeyProvider) error {
	srcF, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcF.Close()

	dstF, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	r, err := cbcrypto.NewReader(srcF, provider)
	if err != nil {
		dstF.Close()
		return fmt.Errorf("NewReader: %w", err)
	}

	if _, err := io.Copy(dstF, r); err != nil {
		dstF.Close()
		return fmt.Errorf("copy: %w", err)
	}
	return dstF.Close()
}
