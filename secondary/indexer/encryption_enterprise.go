//go:build !community
// +build !community

package indexer

import (
	"context"
	"os"

	gocbcrypto "github.com/couchbase/gocbcrypto"
)

const (
	CipherNameNone      = gocbcrypto.CipherNameNone
	CipherNameAES256GCM = gocbcrypto.CipherNameAES256GCM
)

func NewEncryptionCtx(keyId string, keyData []byte, cipher string, kdfLabel []byte) (EncryptionCtx, error) {
	switch cipher {
	case gocbcrypto.CipherNameAES256GCM:
		return gocbcrypto.NewAESGCM256ContextWithOpenSSL([]byte(keyId), keyData, kdfLabel, 0)
	case gocbcrypto.CipherNameNone:
		return nil, nil
	default:
		return nil, errUnsupportedCipher
	}
}

// IsFileEncrypted reports whether filepath was written by gocbcrypto.
func IsFileEncrypted(filepath string) (bool, error) {
	return gocbcrypto.IsFileEncrypted(filepath)
}

// ctx must be non-nil; callers should guard with encCtx != nil before calling.
func WriteEncryptedFile(filepath string, content []byte, mode os.FileMode, ctx EncryptionCtx, errCb func(error)) error {
	return gocbcrypto.WriteFile(filepath, content, mode, ctx.(gocbcrypto.EncryptionContext), errCb)
}

func ReadEncryptedFile(filepath string, getKey func([]byte) []byte, kdfLabel []byte, errCb func(error)) ([]byte, error) {
	return gocbcrypto.ReadFile(filepath, getKey, kdfLabel, errCb)
}

func DecryptFileByChunk(ctx context.Context, src, dst string, getKey func([]byte) []byte, kdfLabel []byte, errCb func(error)) (uint64, error) {
	return gocbcrypto.DecryptFileByChunk(ctx, src, dst, getKey, kdfLabel, errCb)
}

func ReencryptFileByChunk(ctx context.Context, src, dst string, newCtx EncryptionCtx, getKey func([]byte) []byte, kdfLabel []byte, errCb func(error)) (uint64, error) {
	return gocbcrypto.ReencryptFileByChunk(ctx, src, dst, newCtx.(gocbcrypto.EncryptionContext), getKey, kdfLabel, errCb)
}
