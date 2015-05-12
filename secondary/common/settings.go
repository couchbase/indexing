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
	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/indexing/secondary/logging"
)

const (
	IndexingMetaDir          = "/indexing/"
	IndexingSettingsMetaDir  = IndexingMetaDir + "settings/"
	IndexingSettingsMetaPath = IndexingSettingsMetaDir + "config"
)

func GetSettingsConfig(cfg Config) (Config, error) {
	newConfig := cfg.Clone()
	current, _, err := metakv.Get(IndexingSettingsMetaPath)
	if err == nil {
		if len(current) > 0 {
			newConfig.Update(current)
		}
	} else {
		logging.Errorf("GetSettingsConfig() failed: %v", err)
	}
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
		for {
			err := metakv.RunObserveChildren(IndexingSettingsMetaDir, metaKvCb, cancelCh)
			if err == nil {
				return
			} else {
				logging.Errorf("Settings metakv notifier failed (%v)..Restarting", err)
			}
		}
	}()

	return
}
