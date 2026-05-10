//go:build community
// +build community

package indexer

import (
	"context"
	"errors"
	"fmt"
	"os"
)

const (
	CipherNameNone      = "NONE"
	CipherNameAES256GCM = "AES-256-GCM"
)

var EncryptionChunkSize = uint32(32 * 1024)
var ErrCipherKeyLookup = errors.New("cipher key lookup failed")
var ErrRetryDropKey = errors.New("drop key retry")

func NewEncryptionCtx(_ string, _ []byte, _ string, _ []byte) (EncryptionCtx, error) {
	return nil, nil
}

// always returns false — community edition never writes encrypted files.
func IsFileEncrypted(_ string) (bool, error) {
	return false, nil
}

// WriteEncryptedFile is a no-op
func WriteEncryptedFile(_ string, _ []byte, _ os.FileMode, _ EncryptionCtx, _ func(error)) error {
	return nil
}

// ReadEncryptedFile is a no-op in community edition.
func ReadEncryptedFile(_ string, _ func([]byte) []byte, _ []byte, _ func(error)) ([]byte, error) {
	return nil, nil
}

// no-op in community edition.
func DecryptFileByChunk(_ context.Context, _, _ string, _ func([]byte) []byte, _ []byte, _ func(error)) (uint64, error) {
	return 0, nil
}

// no-op in community edition.
func ReencryptFileByChunk(_ context.Context, _, _ string, _ EncryptionCtx, _ func([]byte) []byte, _ []byte, _ func(error)) (uint64, error) {
	return 0, nil
}

func IsBytesEncrypted(_ []byte) bool {
	return false
}

func NewCryptFileWriter(_ *os.File, _ EncryptionCtx, _ uint32, _ bool, _ func(error)) (CryptFileWriter, error) {
	return nil, fmt.Errorf("encryption not supported in community edition")
}
