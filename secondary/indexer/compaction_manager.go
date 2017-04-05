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
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
)

type CompactionManager interface {
}

type compactionManager struct {
	logPrefix string
	config    common.Config
	supvMsgCh MsgChannel
	supvCmdCh MsgChannel
}

type compactionDaemon struct {
	quitch       chan bool
	started      bool
	timer        *time.Timer
	msgch        MsgChannel
	config       common.ConfigHolder
	clusterAddr  string
	lastCheckDay int32
}

func (cd *compactionDaemon) Start() {
	if !cd.started {
		conf := cd.config.Load()
		dur := time.Second * time.Duration(conf["check_period"].Int())
		cd.timer = time.NewTimer(dur)
		cd.started = true
		go cd.loop()
	}
}

func (cd *compactionDaemon) Stop() {
	if cd.started {
		cd.timer.Stop()
		cd.quitch <- true
		<-cd.quitch
	}
}

func (cd *compactionDaemon) ResetConfig(c common.Config) {
	last_config := cd.config.Load()
	cd.config.Store(c)

	abort := c["abort_exceed_interval"].Bool()
	interval := c["interval"].String()

	var start_hr, start_min, end_hr, end_min int
	fmt.Sscanf(interval, "%d:%d,%d:%d", &start_hr, &start_min, &end_hr, &end_min)

	if abort && end_hr == 0 && end_min == 0 {
		// if abort specified, but no end time.
		common.Console(cd.clusterAddr, "Compaction setting misconfigured.  End time is not specified while allowing compaction to abort.")
		logging.Errorf("Compaction setting misconfigured.  End time is not specified while allowing compaction to abort.")
	} else if !abort && !(end_hr == 0 && end_min == 0) {
		// if end_time specified, but no abort.
		common.Console(cd.clusterAddr, "Compaction setting misconfigured.  End time is specified while not allowing compaction to abort.")
		logging.Errorf("Compaction setting misconfigured.  End time is specified while not allowing compaction to abort.")
	}

	// force daemon to re-check start time for the next compaction
	last_interval := last_config["interval"].String()
	var last_start_hr, last_start_min, last_end_hr, last_end_min int
	fmt.Sscanf(last_interval, "%d:%d,%d:%d", &last_start_hr, &last_start_min, &last_end_hr, &last_end_min)
	if last_start_hr != start_hr || last_start_min != start_min {
		atomic.StoreInt32(&cd.lastCheckDay, -1)
	}
}

func (cd *compactionDaemon) needsCompaction(is IndexStorageStats, config common.Config, checkTime time.Time, abortTime time.Time) bool {

	mode := strings.ToLower(config["compaction_mode"].String())
	logging.Infof("CompactionDaemon: Checking fragmentation: %s, mode : %s", is.String(), mode)

	if mode == "full" {
		// if full compaction, then
		// 1) check min_size
		// 2) check min_frag
		if uint64(is.Stats.DiskSize) > config["min_size"].Uint64() {
			if is.GetFragmentation() >= float64(config["min_frag"].Int()) {
				return true
			}
		}
	} else {

		// if circular compaction, then
		// 1) check compaction interval
		// 2) check the week of day
		interval := config["interval"].String()
		isCompactionInterval := true

		var start_hr, start_min, end_hr, end_min int
		n, err := fmt.Sscanf(interval, "%d:%d,%d:%d", &start_hr, &start_min, &end_hr, &end_min)

		if n == 4 && err == nil {

			// validate parameters
			if start_hr < 0 || start_hr > 23 {
				common.Console(cd.clusterAddr, "Compaction setting misconfigured.  Invalid start hour %v.", start_hr)
				logging.Errorf("Compaction setting misconfigured.  Invalid start hour %v.", start_hr)
				return false
			}

			if end_hr < 0 || end_hr > 23 {
				common.Console(cd.clusterAddr, "Compaction setting misconfigured.  Invalid end hour %v.", end_hr)
				logging.Errorf("Compaction setting misconfigured.  Invalid end hour %v.", end_hr)
				return false
			}

			if start_min < 0 || start_min > 59 {
				common.Console(cd.clusterAddr, "Compaction setting misconfigured.  Invalid start min %v.", start_min)
				logging.Errorf("Compaction setting misconfigured.  Invalid start min %v.", start_min)
				return false
			}

			if end_min < 0 || end_min > 59 {
				common.Console(cd.clusterAddr, "Compaction setting misconfigured.  Invalid end min %v.", end_min)
				logging.Errorf("Compaction setting misconfigured.  Invalid end min %v.", end_min)
				return false
			}

			start_min += start_hr * 60
			end_min += end_hr * 60

			// Instead of using current time, use the time when
			// compaction check starts.
			hr, min, _ := checkTime.Clock()
			min += hr * 60

			// if not yet past start time, no compaction.
			if start_min > min {
				isCompactionInterval = false
			}

			// if past start time, check end time
			if isCompactionInterval {

				// If user wants to abort past end date, check if already past end time.
				abort := config["abort_exceed_interval"].Bool()

				// if past 24 hours, then stop this run.
				if abort && time.Now().After(abortTime) {
					return false
				}

				if abort && end_min != 0 {
					now_hr, now_min, _ := time.Now().Clock()
					now_min += now_hr * 60

					// At this point, we know we have past start time.
					// If end time is next day from current time, add minutes
					// for the remaining of today.  To know if end time is next day
					// from current time, current time is larger than start time.
					if start_min > end_min && now_min > start_min {
						end_min += 24 * 60
					}

					if now_min > end_min {
						isCompactionInterval = false
					}

				} else if abort {
					// if abort specified, but no end time.
					logging.Errorf("CompactionDaemon: Compaction setting misconfigured.  " +
						"End time is not specified while allowing compaction to abort.")
				} else if end_min != 0 {
					// if end_time specified, but no abort.
					logging.Errorf("CompactionDaemon: Compaction setting misconfigured.  " +
						"End time is specified while not allowing compaction to abort.")
				}
			}
		}

		if !isCompactionInterval {
			logging.Infof("CompactionDaemon: Compaction attempt skipped since compaction interval is configured for %v", interval)
			return false
		}

		hasDaysOfWeek := false
		days := config["days_of_week"].Strings()
		today := strings.ToLower(checkTime.Weekday().String())
		for _, day := range days {
			if strings.ToLower(strings.TrimSpace(day)) == today {
				return true
			}
			hasDaysOfWeek = true
		}

		if hasDaysOfWeek {
			logging.Infof("CompactionDaemon: Compaction attempt skipped since compaction day is configured for %v", days)
		}
	}

	return false
}

func (cd *compactionDaemon) loop() {
	var stats []IndexStorageStats
	hasStartedToday := false

loop:
	for {
		select {
		case _, ok := <-cd.timer.C:

			conf := cd.config.Load()
			if common.GetStorageMode() == common.FORESTDB {

				if ok {
					replych := make(chan []IndexStorageStats)
					statReq := &MsgIndexStorageStats{respch: replych}
					cd.msgch <- statReq
					stats = <-replych

					// each compaction interval cannot go over 24 hours if specified.
					abortTime := time.Now().Add(time.Duration(24) * time.Hour)
					checkTime := time.Now()

					mode := strings.ToLower(conf["compaction_mode"].String())
					if mode == "circular" {
						// if circular compaction, run full compaction at most once a day.
						if atomic.LoadInt32(&cd.lastCheckDay) != int32(checkTime.Weekday()) {
							hasStartedToday = false
							atomic.StoreInt32(&cd.lastCheckDay, int32(checkTime.Weekday()))
						}

						if hasStartedToday {
							dur := time.Second * time.Duration(conf["check_period"].Int())
							cd.timer.Reset(dur)
							continue
						}
					}

					for _, is := range stats {
						conf = cd.config.Load() // refresh to get up-to-date settings
						needUpgrade := is.Stats.NeedUpgrade
						if needUpgrade || cd.needsCompaction(is, conf, checkTime, abortTime) {
							hasStartedToday = true

							errch := make(chan error)
							compactReq := &MsgIndexCompact{
								instId:    is.InstId,
								errch:     errch,
								abortTime: abortTime,
							}
							logging.Infof("CompactionDaemon: Compacting index instance:%v", is.InstId)
							if needUpgrade {
								common.Console(cd.clusterAddr, "Compacting index %v.%v for upgrade", is.Bucket, is.Name)
							}
							cd.msgch <- compactReq
							err := <-errch
							if err == nil {
								logging.Infof("CompactionDaemon: Finished compacting index instance:%v", is.InstId)
								if needUpgrade {
									common.Console(cd.clusterAddr, "Finished compacting index %v.%v for upgrade", is.Bucket, is.Name)
								}
							} else {
								logging.Errorf("CompactionDaemon: Index instance:%v Compaction failed with reason - %v", is.InstId, err)
								if needUpgrade {
									common.Console(cd.clusterAddr, "Compaction for index %v.%v failed with reason - %v", is.Bucket, is.Name, err)
								}
							}
						}
					}
				}
			}

			dur := time.Second * time.Duration(conf["check_period"].Int())
			cd.timer.Reset(dur)

		case <-cd.quitch:
			cd.quitch <- true
			break loop
		}
	}
}

func NewCompactionManager(supvCmdCh MsgChannel, supvMsgCh MsgChannel,
	config common.Config) (CompactionManager, Message) {
	cm := &compactionManager{
		config:    config,
		supvCmdCh: supvCmdCh,
		supvMsgCh: supvMsgCh,
		logPrefix: "CompactionManager",
	}
	go cm.run()
	return cm, &MsgSuccess{}
}

func (cm *compactionManager) run() {
	cd := cm.newCompactionDaemon()
	cd.Start()
loop:
	for {
		select {
		case cmd, ok := <-cm.supvCmdCh:
			if ok {
				if cmd.GetMsgType() == COMPACTION_MGR_SHUTDOWN {
					logging.Infof("%v: Shutting Down", cm.logPrefix)
					cm.supvCmdCh <- &MsgSuccess{}
					break loop
				} else if cmd.GetMsgType() == CONFIG_SETTINGS_UPDATE {
					logging.Infof("%v: Refreshing settings", cm.logPrefix)
					cfgUpdate := cmd.(*MsgConfigUpdate)
					fullConfig := cfgUpdate.GetConfig()
					cfg := fullConfig.SectionConfig("settings.compaction.", true)
					cd.ResetConfig(cfg)
					cm.supvCmdCh <- &MsgSuccess{}
				}
			} else {
				break loop
			}
		}
	}

	cd.Stop()
}

func (cm *compactionManager) newCompactionDaemon() *compactionDaemon {
	cfg := cm.config.SectionConfig("settings.compaction.", true)
	clusterAddr := cm.config["clusterAddr"].String()
	cd := &compactionDaemon{
		quitch:       make(chan bool),
		started:      false,
		msgch:        cm.supvMsgCh,
		clusterAddr:  clusterAddr,
		lastCheckDay: -1,
	}
	cd.config.Store(cfg)

	return cd
}
