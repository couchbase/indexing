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
	"errors"
	"github.com/couchbase/indexing/secondary/common"
	"io/ioutil"
	"net/http"
)

// Implements dynamic settings management for indexer
type settingsManager struct {
	supvCmdch MsgChannel
	supvMsgch MsgChannel
	config    common.Config
}

func NewSettingsManager(supvCmdch MsgChannel,
	supvMsgch MsgChannel, config common.Config) (settingsManager, Message) {
	s := settingsManager{
		supvCmdch: supvCmdch,
		supvMsgch: supvMsgch,
		config:    config,
	}

	http.HandleFunc("/settings", s.handleSettingsReq)
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
		err := config.Update(bytes)
		if err != nil {
			s.writeError(w, err)
			return
		}

		s.config = config
		s.supvMsgch <- &MsgConfigUpdate{
			cfg: s.config,
		}
		common.Infof("New settings received: \n%s", string(bytes))
		s.writeOk(w)

	} else if r.Method == "GET" {
		settingsConfig := s.config.SectionConfig("settings.", false)
		s.writeJson(w, settingsConfig.Json())
	} else {
		s.writeError(w, errors.New("Unsupported method"))
		return
	}
}

func (s *settingsManager) run() {
loop:
	for {
		select {
		case cmd, ok := <-s.supvCmdch:
			if ok {
				if cmd.GetMsgType() == STORAGE_MGR_SHUTDOWN {
					common.Infof("SettingsManager::run Shutting Down")
					s.supvCmdch <- &MsgSuccess{}
					break loop
				}
			} else {
				break loop
			}
		}
	}
}
