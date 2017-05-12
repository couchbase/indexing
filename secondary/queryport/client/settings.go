// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package client

import (
	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"sync/atomic"
	"time"
)

type ClientSettings struct {
	numReplica    int32
	backfillLimit int32
	config        common.Config
	cancelCh      chan struct{}
}

func NewClientSettings(needRefresh bool) *ClientSettings {

	s := &ClientSettings{
		config:   nil,
		cancelCh: make(chan struct{}, 1),
	}

	if needRefresh {
		config, err := common.GetSettingsConfig(common.SystemConfig)
		if err != nil {
			logging.Errorf("ClientSettings: Fail to initialize metakv for reading latest indexer setting (%v).  Will use default indexer setting.", err)
		} else {
			s.config = config
		}
	}

	if s.config == nil {
		s.config = common.SystemConfig.Clone()
	}

	if needRefresh {
		go func() {
			fn := func(r int, err error) error {
				if r > 0 {
					logging.Errorf("ClientSettings: metakv notifier failed (%v)..Restarting %v", err, r)
				}
				err = metakv.RunObserveChildren(common.IndexingSettingsMetaDir, s.metaKVCallback, s.cancelCh)
				return err
			}
			rh := common.NewRetryHelper(200, time.Second, 2, fn)
			err := rh.Run()
			if err != nil {
				logging.Errorf("ClientSettings: metakv notifier failed even after max retries.")
			}
		}()
	}

	s.handleSettings(s.config)

	return s
}

func (s *ClientSettings) Close() {

	close(s.cancelCh)
}

func (s *ClientSettings) metaKVCallback(path string, value []byte, rev interface{}) error {

	if path == common.IndexingSettingsMetaPath {
		logging.Infof("New settings received: \n%s", string(value))

		config := s.config.Clone()
		config.Update(value)
		s.config = config

		s.handleSettings(s.config)
	}

	return nil
}

func (s *ClientSettings) handleSettings(config common.Config) {

	numReplica := int32(config["indexer.settings.num_replica"].Int())
	if numReplica >= 0 {
		atomic.StoreInt32(&s.numReplica, numReplica)
	} else {
		logging.Errorf("ClientSettings: invalid setting value for num_replica=%v", numReplica)
	}

	backfillLimit := int32(config["queryport.client.backfillLimit"].Int())
	if backfillLimit >= 0 {
		atomic.StoreInt32(&s.backfillLimit, backfillLimit)
	} else {
		logging.Errorf("ClientSettings: invalid setting value for backfillLimit=%v", backfillLimit)
	}
}

func (s *ClientSettings) NumReplica() int32 {
	return atomic.LoadInt32(&s.numReplica)
}

func (s *ClientSettings) BackfillLimit() int32 {
	return atomic.LoadInt32(&s.backfillLimit)
}
