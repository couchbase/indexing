// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

import (
	"github.com/couchbase/indexing/secondary/common"
)

var (
	encBufPool      *common.BytesBufPool
	arrayEncBufPool *common.BytesBufPool
)

func init() {
	// Initialize buffer pools based on key sizes
	conf := common.SystemConfig.SectionConfig("indexer.", true /*trim*/)
	initBufPools(conf)
}

//
// Initialize global buffer pools
//
func initBufPools(newCfg common.Config) {
	keySzCfg := getKeySizeConfig(newCfg)

	encBufPool = common.NewByteBufferPool(keySzCfg.maxIndexEntrySize + ENCODE_BUF_SAFE_PAD)
	arrayEncBufPool = common.NewByteBufferPool(keySzCfg.maxArrayIndexEntrySize + ENCODE_BUF_SAFE_PAD)
	secKeyBufPool = common.NewByteBufferPool(keySzCfg.maxSecKeyBufferLen + ENCODE_BUF_SAFE_PAD)
}

//
// Get keySizeConfig object from configuration settings
//
func getKeySizeConfig(cfg common.Config) keySizeConfig {

	keyCfg := keySizeConfig{}

	keyCfg.allowLargeKeys = cfg["settings.allow_large_keys"].Bool()
	if common.GetStorageMode() == common.FORESTDB {
		keyCfg.allowLargeKeys = false
	}

	if keyCfg.allowLargeKeys {
		keyCfg.maxArrayKeyLength = DEFAULT_MAX_ARRAY_KEY_SIZE
		keyCfg.maxSecKeyLen = DEFAULT_MAX_SEC_KEY_LEN
	} else {
		keyCfg.maxArrayKeyLength = cfg["settings.max_array_seckey_size"].Int()
		keyCfg.maxSecKeyLen = cfg["settings.max_seckey_size"].Int()
	}
	keyCfg.maxArrayKeyBufferLength = keyCfg.maxArrayKeyLength * 3
	keyCfg.maxArrayIndexEntrySize = keyCfg.maxArrayKeyBufferLength + MAX_DOCID_LEN + 2

	keyCfg.maxSecKeyBufferLen = keyCfg.maxSecKeyLen * 3
	keyCfg.maxIndexEntrySize = keyCfg.maxSecKeyBufferLen + MAX_DOCID_LEN + 2

	return keyCfg
}

// Return true if any of the size related config has changed
func keySizeConfigUpdated(cfg, oldCfg common.Config) bool {

	if cfg["settings.allow_large_keys"].Bool() !=
		oldCfg["settings.allow_large_keys"].Bool() {
		return true
	}

	if cfg["settings.max_array_seckey_size"].Int() !=
		oldCfg["settings.max_array_seckey_size"].Int() {
		return true
	}

	if cfg["settings.max_seckey_size"].Int() !=
		oldCfg["settings.max_seckey_size"].Int() {
		return true
	}

	return false
}
