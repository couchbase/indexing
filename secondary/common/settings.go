// Copyright (c) 2015 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package common

import (
	"os"
	"sync/atomic"
	"time"

	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/indexing/secondary/logging"
)

const MAX_METAKV_RETRIES = 100

var maxMetaKVRetries = int32(MAX_METAKV_RETRIES)

const (
	IndexingMetaDir          = "/indexing/"
	IndexingSettingsMetaDir  = IndexingMetaDir + "settings/"
	IndexingSettingsMetaPath = IndexingSettingsMetaDir + "config"
)

func GetSettingsConfig(cfg Config) (Config, error) {
	var newConfig Config
	fn := func(r int, err error) error {
		newConfig = cfg.Clone()
		current, _, err := metakv.Get(IndexingSettingsMetaPath)
		if err == nil {
			if len(current) > 0 {
				newConfig.Update(current)
			}
		} else {
			logging.Errorf("GetSettingsConfig() failed: %v", err)
		}
		return err
	}

	rh := NewRetryHelper(int(maxMetaKVRetries), time.Second*3, 1, fn)
	err := rh.Run()
	return newConfig, err
}

func SetupSettingsNotifier(callb func(Config), cancelCh chan struct{}) {
	metaKvCb := func(path string, val []byte, rev interface{}) error {
		if path == IndexingSettingsMetaPath {
			logging.Infof("New settings received: \n%s", string(val))
			config := SystemConfig.FilterConfig(".settings.")
			config.Update(val)
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
