// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

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
