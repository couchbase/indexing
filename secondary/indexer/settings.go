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
	"bytes"
	"errors"
	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/indexing/secondary/common"
	"io/ioutil"
	"net/http"
	"time"
)

const (
	indexerMetaDir          = "/indexer/"
	indexerSettingsMetaPath = indexerMetaDir + "settings"
	indexCompactonMetaPath  = indexerMetaDir + "triggerCompaction"
)

// Implements dynamic settings management for indexer
type settingsManager struct {
	supvCmdch       MsgChannel
	supvMsgch       MsgChannel
	config          common.Config
	cancelCh        chan struct{}
	compactionToken []byte
}

func NewSettingsManager(supvCmdch MsgChannel,
	supvMsgch MsgChannel, config common.Config) (settingsManager, Message) {
	s := settingsManager{
		supvCmdch: supvCmdch,
		supvMsgch: supvMsgch,
		config:    config,
		cancelCh:  make(chan struct{}),
	}

	http.HandleFunc("/settings", s.handleSettingsReq)
	http.HandleFunc("/triggerCompaction", s.handleCompactionTrigger)
	go func() {
		for {
			err := metakv.RunObserveChildren("/", s.metaKVCallback, s.cancelCh)
			if err == nil {
				return
			} else {
				common.Errorf("IndexerSettingsManager: metakv notifier failed (%v)..Restarting", err)
			}
		}
	}()
	return s, &MsgSuccess{}
}

func (s *settingsManager) writeOk(w http.ResponseWriter) {
	w.WriteHeader(200)
	w.Write([]byte("OK\n"))
}

func (s *settingsManager) writeError(w http.ResponseWriter, err error) {
	w.WriteHeader(400)
	w.Write([]byte(err.Error() + "\n"))
}

func (s *settingsManager) writeJson(w http.ResponseWriter, json []byte) {
	header := w.Header()
	header["Content-Type"] = []string{"application/json"}
	w.WriteHeader(200)
	w.Write(json)
	w.Write([]byte("\n"))
}

func (s *settingsManager) handleSettingsReq(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		bytes, _ := ioutil.ReadAll(r.Body)

		config := s.config.Clone()
		current, rev, err := metakv.Get(indexerSettingsMetaPath)
		if err == nil {
			if len(current) > 0 {
				config.Update(current)
			}
			err = config.Update(bytes)
		}

		if err != nil {
			s.writeError(w, err)
			return
		}

		settingsConfig := config.SectionConfig("settings.", false)
		newSettingsBytes := settingsConfig.Json()
		if err = metakv.Set(indexerSettingsMetaPath, newSettingsBytes, rev); err != nil {
			s.writeError(w, err)
			return
		}
		s.writeOk(w)
	} else if r.Method == "GET" {
		settingsConfig, err := getSettingsConfig(s.config)
		if err != nil {
			s.writeError(w, err)
			return
		}
		s.writeJson(w, settingsConfig.Json())
	} else {
		s.writeError(w, errors.New("Unsupported method"))
		return
	}
}

func (s *settingsManager) handleCompactionTrigger(w http.ResponseWriter, r *http.Request) {
	_, rev, err := metakv.Get(indexCompactonMetaPath)
	if err != nil {
		s.writeError(w, err)
		return
	}

	newToken := time.Now().String()
	if err = metakv.Set(indexCompactonMetaPath, []byte(newToken), rev); err != nil {
		s.writeError(w, err)
		return
	}

	s.writeOk(w)
}

func (s *settingsManager) run() {
loop:
	for {
		select {
		case cmd, ok := <-s.supvCmdch:
			if ok {
				if cmd.GetMsgType() == STORAGE_MGR_SHUTDOWN {
					common.Infof("SettingsManager::run Shutting Down")
					close(s.cancelCh)
					s.supvCmdch <- &MsgSuccess{}
					break loop
				}
			} else {
				break loop
			}
		}
	}
}

func (s *settingsManager) metaKVCallback(path string, value []byte, rev interface{}) error {
	if path == indexerSettingsMetaPath {
		common.Infof("New settings received: \n%s", string(value))
		config := s.config.Clone()
		config.Update(value)
		s.config = config
		s.supvMsgch <- &MsgConfigUpdate{
			cfg: s.config,
		}
	} else if path == indexCompactonMetaPath {
		currentToken := s.compactionToken
		s.compactionToken = value
		if currentToken == nil || bytes.Equal(currentToken, value) {
			return nil
		}

		common.Infof("Manual compaction trigger requested")
		replych := make(chan []IndexStorageStats)
		statReq := &MsgIndexStorageStats{respch: replych}
		s.supvMsgch <- statReq
		stats := <-replych
		// XXX: minFile size check can be applied
		go func() {
			for _, is := range stats {
				errch := make(chan error)
				compactReq := &MsgIndexCompact{
					instId: is.InstId,
					errch:  errch,
				}
				common.Infof("ManualCompaction: Compacting index instance:%v", is.InstId)
				s.supvMsgch <- compactReq
				err := <-errch
				if err == nil {
					common.Infof("ManualCompaction: Finished compacting index instance:%v", is.InstId)
				} else {
					common.Errorf("ManualCompaction: Index instance:%v Compaction failed with reason - %v", is.InstId, err)
				}
			}
		}()
	}

	return nil
}

func getSettingsConfig(cfg common.Config) (common.Config, error) {
	settingsConfig := cfg.SectionConfig("settings.", false)
	current, _, err := metakv.Get(indexerSettingsMetaPath)
	if err == nil {
		if len(current) > 0 {
			settingsConfig.Update(current)
		}
	}
	return settingsConfig, err
}
