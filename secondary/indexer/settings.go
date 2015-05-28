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
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/pipeline"
	"io/ioutil"
	"net/http"
	"time"
)

const (
	indexCompactonMetaPath = common.IndexingMetaDir + "triggerCompaction"
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
	supvMsgch MsgChannel, config common.Config) (settingsManager, common.Config, Message) {
	s := settingsManager{
		supvCmdch: supvCmdch,
		supvMsgch: supvMsgch,
		config:    config,
		cancelCh:  make(chan struct{}),
	}

	config, err := common.GetSettingsConfig(config)
	if err != nil {
		return s, nil, &MsgError{
			err: Error{
				category: INDEXER,
				cause:    err,
				severity: FATAL,
			}}
	}

	ncpu := common.SetNumCPUs(config["indexer.settings.max_cpu_percent"].Int())
	logging.Infof("Setting maxcpus = %d", ncpu)

	setBlockPoolSize(nil, config)
	setLogger(config)

	http.HandleFunc("/settings", s.handleSettingsReq)
	http.HandleFunc("/triggerCompaction", s.handleCompactionTrigger)
	go func() {
		for {
			err := metakv.RunObserveChildren("/", s.metaKVCallback, s.cancelCh)
			if err == nil {
				return
			} else {
				logging.Errorf("IndexerSettingsManager: metakv notifier failed (%v)..Restarting", err)
			}
		}
	}()

	indexerConfig := config.SectionConfig("indexer.", true)
	return s, indexerConfig, &MsgSuccess{}
}

func (s *settingsManager) writeOk(w http.ResponseWriter) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK\n"))
}

func (s *settingsManager) writeError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusBadRequest)
	w.Write([]byte(err.Error() + "\n"))
}

func (s *settingsManager) writeJson(w http.ResponseWriter, json []byte) {
	header := w.Header()
	header["Content-Type"] = []string{"application/json"}
	w.WriteHeader(200)
	w.Write(json)
	w.Write([]byte("\n"))
}

func (s *settingsManager) validateAuth(w http.ResponseWriter, r *http.Request) bool {
	valid, err := common.IsAuthValid(r, s.config["indexer.clusterAddr"].String())
	if err != nil {
		s.writeError(w, err)
	} else if valid == false {
		w.WriteHeader(401)
		w.Write([]byte("401 Unauthorized\n"))
	}
	return valid
}

func (s *settingsManager) handleSettingsReq(w http.ResponseWriter, r *http.Request) {
	if !s.validateAuth(w, r) {
		return
	}

	if r.Method == "POST" {
		bytes, _ := ioutil.ReadAll(r.Body)

		config := s.config.Clone()
		current, rev, err := metakv.Get(common.IndexingSettingsMetaPath)
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

		settingsConfig := config.FilterConfig(".settings.")
		newSettingsBytes := settingsConfig.Json()
		if err = metakv.Set(common.IndexingSettingsMetaPath, newSettingsBytes, rev); err != nil {
			s.writeError(w, err)
			return
		}
		s.writeOk(w)
	} else if r.Method == "GET" {
		settingsConfig, err := common.GetSettingsConfig(s.config)
		if err != nil {
			s.writeError(w, err)
			return
		}
		s.writeJson(w, settingsConfig.FilterConfig(".settings.").Json())
	} else {
		s.writeError(w, errors.New("Unsupported method"))
		return
	}
}

func (s *settingsManager) handleCompactionTrigger(w http.ResponseWriter, r *http.Request) {
	if !s.validateAuth(w, r) {
		return
	}
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
					logging.Infof("SettingsManager::run Shutting Down")
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
	if path == common.IndexingSettingsMetaPath {
		logging.Infof("New settings received: \n%s", string(value))
		config := s.config.Clone()
		config.Update(value)
		setBlockPoolSize(s.config, config)
		s.config = config

		ncpu := common.SetNumCPUs(config["indexer.settings.max_cpu_percent"].Int())
		logging.Infof("Setting maxcpus = %d", ncpu)

		setLogger(config)

		indexerConfig := s.config.SectionConfig("indexer.", true)
		s.supvMsgch <- &MsgConfigUpdate{
			cfg: indexerConfig,
		}
	} else if path == indexCompactonMetaPath {
		currentToken := s.compactionToken
		s.compactionToken = value
		if bytes.Equal(currentToken, value) {
			return nil
		}

		logging.Infof("Manual compaction trigger requested")
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
				logging.Infof("ManualCompaction: Compacting index instance:%v", is.InstId)
				s.supvMsgch <- compactReq
				err := <-errch
				if err == nil {
					logging.Infof("ManualCompaction: Finished compacting index instance:%v", is.InstId)
				} else {
					logging.Errorf("ManualCompaction: Index instance:%v Compaction failed with reason - %v", is.InstId, err)
				}
			}
		}()
	}

	return nil
}

func setLogger(config common.Config) {
	logLevel := config["indexer.settings.log_level"].String()
	level := logging.Level(logLevel)
	logging.Infof("Setting log level to %v", level)
	logging.SetLogLevel(level)
}

func setBlockPoolSize(o, n common.Config) {
	var oldSz, newSz int
	if o != nil {
		oldSz = o["indexer.settings.bufferPoolBlockSize"].Int()
	}

	newSz = n["indexer.settings.bufferPoolBlockSize"].Int()

	if oldSz < newSz {
		pipeline.SetupBlockPool(newSz)
		logging.Infof("Setting buffer block size to %d bytes", newSz)
	} else if oldSz > newSz {
		logging.Errorf("Setting buffer block size from %d to %d failed "+
			" - Only sizes higher than current size is allowed during runtime",
			oldSz, newSz)
	}
}
