// Copyright 2015-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package common

import (
	"os"
	"sync/atomic"
	"time"

	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/security"
)

const MAX_METAKV_RETRIES = 100
const SIGAR_INIT_RETRIES = 100
const SIGAR_CGROUP_SUPPORTED = 1 // SigarControlGroupInfo.Supported value if cgroups are supported

var maxMetaKVRetries = int32(MAX_METAKV_RETRIES)

const (
	IndexingMetaDir                  = "/indexing/"
	IndexingSettingsMetaDir          = IndexingMetaDir + "settings/"
	IndexingSettingsMetaPath         = IndexingSettingsMetaDir + "config"
	IndexingSettingsFeaturesMetaPath = IndexingSettingsMetaPath + "/features/"

	// Note: Do not use this path for any setting other than writing the JSON
	// {"indexer.default.enable_shard_affinity":<true|false>}'
	// Otherwise, ns_server compatability would break
	IndexingSettingsShardAffinityMetaPath = IndexingSettingsFeaturesMetaPath + "ShardAffinity"
)

func GetSettingsConfig(cfg Config) (Config, error) {
	var newConfig Config
	if security.IsToolsConfigUsed() {
		return cfg, nil
	}
	fn := func(r int, err error) error {
		newConfig = cfg.Clone()
		current, _, err := metakv.Get(IndexingSettingsMetaPath)
		if err == nil {
			if len(current) > 0 {
				newConfig.Update(current)
			}
		} else {
			logging.Errorf("GetSettingsConfig() failed: %v", err)
			return err
		}

		current, _, err = metakv.Get(IndexingSettingsShardAffinityMetaPath)
		if err == nil {
			if len(current) > 0 {
				newConfig.Update(current)
			}
		} else {
			logging.Errorf("GetSettingsConfig() failed to retrive shard affinity meta path: %v", err)
		}
		return err
	}

	rh := NewRetryHelper(int(maxMetaKVRetries), time.Second*3, 1, fn)
	err := rh.Run()
	return newConfig, err
}

func SetupSettingsNotifier(callb func(Config), cancelCh chan struct{}) {
	metaKvCb := func(kve metakv.KVEntry) error {
		if kve.Path == IndexingSettingsMetaPath || kve.Path == IndexingSettingsShardAffinityMetaPath {
			logging.Infof("New settings received on path: %v, value: \n%s", string(kve.Value))
			config := SystemConfig.FilterConfig(".settings.")
			if kve.Path == IndexingSettingsShardAffinityMetaPath {
				config = SystemConfig.Get("indexer.default.enable_shard_affinity")
			}
			config.Update(kve.Value)
			callb(config)
		}
		return nil
	}

	go func() {
		fn := func(r int, err error) error {
			if r > 0 {
				logging.Errorf("metakv notifier failed (%v)..Retrying %v", err, r)
			}
			err = metakv.RunObserveChildren(IndexingSettingsMetaDir, metaKvCb, cancelCh)
			return err
		}
		rh := NewRetryHelper(MAX_METAKV_RETRIES, time.Second, 2, fn)
		err := rh.Run()
		if err != nil {
			logging.Fatalf("Settings metakv notifier failed (%v).. Exiting", err)
			os.Exit(1)
		}
	}()
	return
}

// For standalone tests where MetaKV is not present, call this method
// to attempt MetaKV retry only once
func EnableStandaloneTest() {
	atomic.StoreInt32(&maxMetaKVRetries, 1)
}

func DisableStandaloneTest() {
	atomic.StoreInt32(&maxMetaKVRetries, int32(MAX_METAKV_RETRIES))
}
