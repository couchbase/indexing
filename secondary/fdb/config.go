package forestdb

//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

//#cgo CFLAGS: -O0
//#include <libforestdb/forestdb.h>
import "C"

import (
	"unsafe"
)

import "github.com/couchbase/indexing/secondary/logging"
import "github.com/couchbase/indexing/secondary/common"

type OpenFlags uint32

const (
	OPEN_FLAG_CREATE OpenFlags = 1
	OPEN_FLAG_RDONLY OpenFlags = 2
)

type SeqTreeOpt uint8

const (
	SEQTREE_NOT_USE SeqTreeOpt = 0
	SEQTREE_USE     SeqTreeOpt = 1
)

type DurabilityOpt uint8

const (
	DRB_NONE          DurabilityOpt = 0
	DRB_ODIRECT       DurabilityOpt = 0x1
	DRB_ASYNC         DurabilityOpt = 0x2
	DRB_ODIRECT_ASYNC DurabilityOpt = 0x3
)

type CompactOpt uint8

const (
	COMPACT_MANUAL CompactOpt = 0
	COMPACT_AUTO   CompactOpt = 1
)

// ForestDB config options
type Config struct {
	config *C.fdb_config
}

func (c *Config) ChunkSize() uint16 {
	return uint16(c.config.chunksize)
}

func (c *Config) SetChunkSize(s uint16) {
	c.config.chunksize = C.uint16_t(s)
}

func (c *Config) BlockSize() uint32 {
	return uint32(c.config.blocksize)
}

func (c *Config) SetBlockSize(s uint32) {
	c.config.blocksize = C.uint32_t(s)
}

func (c *Config) BufferCacheSize() uint64 {
	return uint64(c.config.buffercache_size)
}

func (c *Config) SetBufferCacheSize(s uint64) {
	c.config.buffercache_size = C.uint64_t(s)
}

func (c *Config) WalThreshold() uint64 {
	return uint64(c.config.wal_threshold)
}

func (c *Config) SetWalThreshold(s uint64) {
	c.config.wal_threshold = C.uint64_t(s)
}

func (c *Config) WalFlushBeforeCommit() bool {
	return bool(c.config.wal_flush_before_commit)
}

func (c *Config) SetWalFlushBeforeCommit(b bool) {
	c.config.wal_flush_before_commit = C.bool(b)
}

func (c *Config) PurgingInterval() uint32 {
	return uint32(c.config.purging_interval)
}

func (c *Config) SetPurgingInterval(s uint32) {
	c.config.purging_interval = C.uint32_t(s)
}

func (c *Config) SeqTreeOpt() SeqTreeOpt {
	return SeqTreeOpt(c.config.seqtree_opt)
}

func (c *Config) SetSeqTreeOpt(o SeqTreeOpt) {
	c.config.seqtree_opt = C.fdb_seqtree_opt_t(o)
}

func (c *Config) DurabilityOpt() DurabilityOpt {
	return DurabilityOpt(c.config.durability_opt)
}

func (c *Config) SetDurabilityOpt(o DurabilityOpt) {
	c.config.durability_opt = C.fdb_durability_opt_t(o)
}

func (c *Config) OpenFlags() OpenFlags {
	return OpenFlags(c.config.flags)
}

func (c *Config) SetOpenFlags(o OpenFlags) {
	c.config.flags = C.fdb_open_flags(o)
}

func (c *Config) CompactionBufferSizeMax() uint32 {
	return uint32(c.config.compaction_buf_maxsize)
}

func (c *Config) NumKeepingHeaders() uint8 {
	return uint8(c.config.num_keeping_headers)
}

func (c *Config) BlockReuseThreshold() uint8 {
	return uint8(c.config.block_reusing_threshold)
}

func (c *Config) SetCompactionBufferSizeMax(s uint32) {
	c.config.compaction_buf_maxsize = C.uint32_t(s)
}

func (c *Config) CleanupCacheOnClose() bool {
	return bool(c.config.cleanup_cache_onclose)
}

func (c *Config) SetCleanupCacheOnClose(b bool) {
	c.config.cleanup_cache_onclose = C.bool(b)
}

func (c *Config) CompressDocumentBody() bool {
	return bool(c.config.compress_document_body)
}

func (c *Config) SetCompressDocumentBody(b bool) {
	c.config.compress_document_body = C.bool(b)
}

func (c *Config) CompactionMode() CompactOpt {
	return CompactOpt(c.config.compaction_mode)
}

func (c *Config) SetCompactionMode(o CompactOpt) {
	c.config.compaction_mode = C.fdb_compaction_mode_t(o)
}

func (c *Config) CompactionThreshold() uint8 {
	return uint8(c.config.compaction_threshold)
}

func (c *Config) SetCompactionThreshold(s uint8) {
	c.config.compaction_threshold = C.uint8_t(s)
}

func (c *Config) CompactionMinimumFilesize() uint64 {
	return uint64(c.config.compaction_minimum_filesize)
}

func (c *Config) SetCompactionMinimumFilesize(s uint64) {
	c.config.compaction_minimum_filesize = C.uint64_t(s)
}

func (c *Config) CompactorSleepDuration() uint64 {
	return uint64(c.config.compactor_sleep_duration)
}

func (c *Config) SetCompactorSleepDuration(s uint64) {
	c.config.compactor_sleep_duration = C.uint64_t(s)
}

func (c *Config) MaxWriterLockProb() uint8 {
	return uint8(c.config.max_writer_lock_prob)
}

func (c *Config) SetMaxWriterLockProb(s uint8) {
	c.config.max_writer_lock_prob = C.size_t(s)
}

func (c *Config) SetNumKeepingHeaders(s uint8) {
	c.config.num_keeping_headers = C.size_t(s)
}

func (c *Config) SetBlockReuseThreshold(s uint8) {
	c.config.block_reusing_threshold = C.size_t(s)
}

// DefaultConfig gets the default ForestDB config
func DefaultConfig() *Config {
	Log.Tracef("fdb_get_default_config call")
	config := C.fdb_get_default_config()
	config.breakpad_minidump_dir = C.CString(common.SystemConfig["indexer.diagnostics_dir"].String())
	logging.Debugf("DefaultConfig(): config.breakpad_minidump_dir %v", config.breakpad_minidump_dir)
	Log.Tracef("fdb_get_default_config ret config:%v", config)
	return &Config{
		config: &config,
	}
}

// ForestDB KVStore config options
type KVStoreConfig struct {
	config *C.fdb_kvs_config
}

func (c *KVStoreConfig) CreateIfMissing() bool {
	return bool(c.config.create_if_missing)
}

func (c *KVStoreConfig) SetCreateIfMissing(b bool) {
	c.config.create_if_missing = C.bool(b)
}

func (c *KVStoreConfig) SetCustomCompare(comparator unsafe.Pointer) {
	c.config.custom_cmp = C.fdb_custom_cmp_variable(comparator)
}

// DefaultConfig gets the default ForestDB config
func DefaultKVStoreConfig() *KVStoreConfig {
	Log.Tracef("fdb_get_default_kvs_config call")
	config := C.fdb_get_default_kvs_config()
	Log.Tracef("fdb_get_default_kvs_config ret config:%v", config)
	return &KVStoreConfig{
		config: &config,
	}
}
