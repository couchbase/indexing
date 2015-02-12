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
	"encoding/json"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/common"
	"net/http"
	"runtime"
)

type statsManager struct {
	supvCmdch MsgChannel
	supvMsgch MsgChannel
}

func NewStatsManager(supvCmdch MsgChannel,
	supvMsgch MsgChannel, config common.Config) (statsManager, Message) {
	s := statsManager{
		supvCmdch: supvCmdch,
		supvMsgch: supvMsgch,
	}

	http.HandleFunc("/stats", s.handleStatsReq)
	http.HandleFunc("/stats/mem", s.handleMemStatsReq)
	return s, &MsgSuccess{}
}

func (s *statsManager) handleStatsReq(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" || r.Method == "GET" {
		statsMap := make(map[string]string)
		stats_list := []MsgType{STORAGE_STATS, SCAN_STATS, INDEX_PROGRESS_STATS, INDEXER_STATS}
		for _, t := range stats_list {
			ch := make(chan map[string]string)
			msg := &MsgStatsRequest{
				mType:  t,
				respch: ch,
			}

			s.supvMsgch <- msg
			for k, v := range <-ch {
				statsMap[k] = v
			}
		}

		bytes, _ := json.Marshal(statsMap)
		w.WriteHeader(200)
		w.Write(bytes)
	} else {
		w.WriteHeader(400)
		w.Write([]byte("Unsupported method"))
	}
}

func (s *statsManager) handleMemStatsReq(w http.ResponseWriter, r *http.Request) {
	stats := new(runtime.MemStats)
	if r.Method == "POST" || r.Method == "GET" {
		runtime.ReadMemStats(stats)
		bytes, _ := json.Marshal(stats)
		w.WriteHeader(200)
		w.Write(bytes)
	} else {
		w.WriteHeader(400)
		w.Write([]byte("Unsupported method"))
	}
}

func (s *statsManager) run() {
loop:
	for {
		select {
		case cmd, ok := <-s.supvCmdch:
			if ok {
				if cmd.GetMsgType() == STORAGE_MGR_SHUTDOWN {
					logging.Infof("SettingsManager::run Shutting Down")
					s.supvCmdch <- &MsgSuccess{}
					break loop
				}
			} else {
				break loop
			}
		}
	}
}
