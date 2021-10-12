// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/indexing/secondary/audit"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/logging/systemevent"
	"github.com/couchbase/indexing/secondary/pipeline"
	"github.com/couchbase/indexing/secondary/stubs/nitro/mm"
	"github.com/couchbase/indexing/secondary/stubs/nitro/plasma"

	"io/ioutil"
	"net/http"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"time"
)

const (
	indexCompactonMetaPath = common.IndexingMetaDir + "triggerCompaction"
	compactionDaysSetting  = "indexer.settings.compaction.days_of_week"
)

// Implements dynamic settings management for indexer
type settingsManager struct {
	supvCmdch       MsgChannel
	supvMsgch       MsgChannel
	config          common.Config
	cancelCh        chan struct{}
	compactionToken []byte
	indexerReady    bool
	notifyPending   bool
}

func NewSettingsManager(supvCmdch MsgChannel,
	supvMsgch MsgChannel, config common.Config) (settingsManager, common.Config, Message) {
	s := settingsManager{
		supvCmdch: supvCmdch,
		supvMsgch: supvMsgch,
		config:    config,
		cancelCh:  make(chan struct{}),
	}

	// This method will merge metakv indexer settings onto default settings.
	config, err := common.GetSettingsConfig(config)
	if err != nil {
		return s, nil, &MsgError{
			err: Error{
				category: INDEXER,
				cause:    err,
				severity: FATAL,
			}}
	}

	initGlobalSettings(nil, config)

	go func() {
		fn := func(r int, err error) error {
			if r > 0 {
				logging.Errorf("IndexerSettingsManager: metakv notifier failed (%v)..Restarting %v", err, r)
			}
			err = metakv.RunObserveChildrenV2("/", s.metaKVCallback, s.cancelCh)
			return err
		}
		rh := common.NewRetryHelper(MAX_METAKV_RETRIES, time.Second, 2, fn)
		err := rh.Run()
		if err != nil {
			logging.Fatalf("IndexerSettingsManager: metakv notifier failed even after max retries. Restarting indexer.")
			os.Exit(1)
		}
	}()

	go s.run()

	indexerConfig := config.SectionConfig("indexer.", true)
	return s, indexerConfig, &MsgSuccess{}
}

func (s *settingsManager) RegisterRestEndpoints() {
	mux := GetHTTPMux()
	mux.HandleFunc("/settings", s.handleSettingsReq)
	mux.HandleFunc("/internal/settings", s.handleInternalSettingsReq)
	mux.HandleFunc("/triggerCompaction", s.handleCompactionTrigger)
	mux.HandleFunc("/settings/runtime/freeMemory", s.handleFreeMemoryReq)
	mux.HandleFunc("/settings/runtime/forceGC", s.handleForceGCReq)
	mux.HandleFunc("/plasmaDiag", s.handlePlasmaDiag)
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

func (s *settingsManager) validateAuth(w http.ResponseWriter, r *http.Request) (cbauth.Creds, bool) {
	creds, valid, err := common.IsAuthValid(r)
	if err != nil {
		s.writeError(w, err)
	} else if valid == false {
		audit.Audit(common.AUDIT_UNAUTHORIZED, r, "SettingsManager::validateAuth", "")
		w.WriteHeader(http.StatusUnauthorized)
		w.Write(common.HTTP_STATUS_UNAUTHORIZED)
	}
	return creds, valid
}

func (s *settingsManager) handleInternalSettingsReq(w http.ResponseWriter, r *http.Request) {
	s.handleSettings(w, r, true)
}

func (s *settingsManager) handleSettingsReq(w http.ResponseWriter, r *http.Request) {
	s.handleSettings(w, r, false)
}

func (s *settingsManager) handleSettings(w http.ResponseWriter, r *http.Request, internal bool) {
	creds, ok := s.validateAuth(w, r)
	if !ok {
		return
	}

	if !common.IsAllowed(creds, []string{"cluster.settings!write"}, r, w,
		"SettingsManager::handleSettings") {
		return
	}

	if r.Method == "POST" {
		bytes, _ := ioutil.ReadAll(r.Body)

		config := s.config.FilterConfig(".settings.")
		current, rev, err := metakv.Get(common.IndexingSettingsMetaPath)
		if err == nil {
			if len(current) > 0 {
				config.Update(current)
			}

			err = validateSettings(bytes, config, internal)
			if err != nil {
				logging.Errorf("Fail to change setting.  Error: %v", err)
				s.writeError(w, err)
				return
			}

			if bytes, _, err = common.MapSettings(bytes); err != nil {
				logging.Errorf("Fail to map settings.  Error: %v", err)
				s.writeError(w, err)
				return
			}

			err = config.Update(bytes)
		}

		if err != nil {
			s.writeError(w, err)
			return
		}

		//settingsConfig := config.FilterConfig(".settings.")
		newSettingsBytes := config.Json()
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
		// handle ?internal=ok
		if query := r.URL.Query(); query != nil {
			param, ok := query["internal"]
			if ok && len(param) > 0 && param[0] == "ok" {
				s.writeJson(w, settingsConfig.Json())
				return
			}
		}
		s.writeJson(w, settingsConfig.FilterConfig(".settings.").Json())

	} else {
		s.writeError(w, errors.New("Unsupported method"))
		return
	}
}

func (s *settingsManager) handleCompactionTrigger(w http.ResponseWriter, r *http.Request) {
	creds, ok := s.validateAuth(w, r)
	if !ok {
		return
	}

	if !common.IsAllowed(creds, []string{"cluster.settings!write"}, r, w,
		"SettingsManager::handleCompactionTrigger") {
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

func (s *settingsManager) handlePlasmaDiag(w http.ResponseWriter, r *http.Request) {
	creds, ok := s.validateAuth(w, r)
	if !ok {
		return
	}

	if !common.IsAllowed(creds, []string{"cluster.settings!write"}, r, w,
		"SettingsManager::handlePlasmaDiag") {
		return
	}

	plasma.Diag.HandleHttp(w, r)
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
				s.handleSupervisorCommands(cmd)
			} else {
				break loop
			}
		}
	}
}

func (s *settingsManager) handleSupervisorCommands(cmd Message) {
	switch cmd.GetMsgType() {

	case CLUST_MGR_INDEXER_READY:
		s.handleIndexerReady()

	default:
		logging.Fatalf("SettingsManager::handleSupervisorCommands Unknown Message %+v", cmd)
		common.CrashOnError(errors.New("Unknown Msg On Supv Channel"))
	}

}

func (s *settingsManager) metaKVCallback(kve metakv.KVEntry) error {
	if !s.indexerReady && (kve.Path == common.IndexingSettingsMetaPath || kve.Path == indexCompactonMetaPath) {
		s.notifyPending = true
		logging.Infof("SettingsMgr::metaKVCallback Dropped request %v %v. Any setting change will get applied once Indexer is ready.", kve.Path, string(kve.Value))
		return nil
	}

	if kve.Path == common.IndexingSettingsMetaPath {
		err := s.applySettings(kve.Path, kve.Value, kve.Rev)
		if err != nil {
			return err
		}
	} else if kve.Path == indexCompactonMetaPath {
		currentToken := s.compactionToken
		s.compactionToken = kve.Value
		if bytes.Equal(currentToken, kve.Value) {
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

func (s *settingsManager) applySettings(path string, value []byte, rev interface{}) error {

	logging.Infof("New settings received: \n%s", string(value))

	var err error
	upgradedConfig, upgraded := tryUpgradeConfig(value)
	if upgraded {
		if err := metakv.Set(common.IndexingSettingsMetaPath, upgradedConfig, rev); err != nil {
			return err
		}
		return nil
	}

	oldConfig := s.config.Clone()

	newConfig := s.config.Clone()
	newConfig.Update(value)
	initGlobalSettings(s.config, newConfig)

	s.config = newConfig

	indexerConfig := s.config.SectionConfig("indexer.", true)
	s.supvMsgch <- &MsgConfigUpdate{
		cfg: indexerConfig,
	}

	diffOld, diffNew := oldConfig.SectionConfig("indexer.", false).Diff(
		newConfig.SectionConfig("indexer.", false))
	if len(diffOld) != 0 {
		systemevent.InfoEvent("Indexer:SettingsManager",
			systemevent.EVENTID_INDEXER_SETTINGS_CHANGE,
			map[string]interface{}{"NewSetting": diffNew.Map(),
				"OldSetting": diffOld.Map()})
	}

	return err
}

func (s *settingsManager) handleFreeMemoryReq(w http.ResponseWriter, r *http.Request) {
	creds, ok := s.validateAuth(w, r)
	if !ok {
		return
	}

	if !common.IsAllowed(creds, []string{"cluster.settings!write"}, r, w,
		"SettingsManager::handleFreeMemoryReq") {
		return
	}

	logging.Infof("Received force free memory request. Executing FreeOSMemory...")
	debug.FreeOSMemory()
	mm.FreeOSMemory()
	s.writeOk(w)
}

func (s *settingsManager) handleForceGCReq(w http.ResponseWriter, r *http.Request) {
	creds, ok := s.validateAuth(w, r)
	if !ok {
		return
	}

	if !common.IsAllowed(creds, []string{"cluster.settings!write"}, r, w,
		"SettingsManager::handleForceGCReq") {
		return
	}

	logging.Infof("Received force GC request. Executing GC...")
	runtime.GC()
	s.writeOk(w)
}

func (s *settingsManager) handleIndexerReady() {

	s.supvCmdch <- &MsgSuccess{}

	s.indexerReady = true

	if s.notifyPending {
		logging.Infof("SettingsMgr::handleIndexerReady apply pending setting changes")
		s.notifyPending = false
		config, rev, err := metakv.Get(common.IndexingSettingsMetaPath)
		if err != nil {
			logging.Errorf("SettingsMgr::handleIndexerReady Err Metakv Get for Settings %v", err)
			return
		}
		if err = s.applySettings(common.IndexingSettingsMetaPath, config, rev); err != nil {
			logging.Errorf("SettingsMgr::handleIndexerReady Err Applying Settings %v", err)
			return
		}
	}

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

func initGlobalSettings(oldCfg, newCfg common.Config) {
	setBlockPoolSize(oldCfg, newCfg)

	ncpu := common.SetNumCPUs(newCfg["indexer.settings.max_cpu_percent"].Int())
	logging.Infof("Setting maxcpus = %d", ncpu)

	setLogger(newCfg)
	useMutationSyncPool = newCfg["indexer.useMutationSyncPool"].Bool()

	newEncodeCompatMode := EncodeCompatMode(newCfg["indexer.encoding.encode_compat_mode"].Int())
	if gEncodeCompatMode != newEncodeCompatMode {
		gEncodeCompatMode = newEncodeCompatMode
		logging.Infof("Set EncodeCompatMode %v", gEncodeCompatMode)
	}
}

func validateSettings(value []byte, current common.Config, internal bool) error {
	newConfig, err := common.NewConfig(value)
	if err != nil {
		return err
	}
	if val, ok := newConfig[compactionDaysSetting]; ok {
		for _, day := range val.Strings() {
			if !isValidDay(day) {
				msg := "Index circular compaction days_of_week is case-sensitive " +
					"and must have zero or more comma-separated values of " +
					"Sunday,Monday,Tuesday,Wednesday,Thursday,Friday,Saturday"
				return errors.New(msg)
			}
		}
	}

	if val, ok := newConfig["indexer.settings.max_seckey_size"]; ok {
		if val.Int() <= 0 {
			return errors.New("Setting should be an integer greater than 0")
		}
	}

	if val, ok := newConfig["indexer.settings.max_array_seckey_size"]; ok {
		if val.Int() <= 0 {
			return errors.New("Setting should be an integer greater than 0")
		}
	}

	if val, ok := newConfig["indexer.statsPersistenceInterval"]; ok {
		if val.Uint64() < 0 {
			return errors.New("statsPersistenceInterval should be an integer 0 or greater")
		}
	}

	if val, ok := newConfig["indexer.statsPersistenceChunkSize"]; ok {
		if val.Int() <= 0 {
			return errors.New("statsPersistenceChunkSize should be an integer greater than 0")
		}
	}

	if val, ok := newConfig["indexer.vbseqnos.workers_per_reader"]; ok {
		if val.Int() <= 0 {
			return errors.New("indexer.vbseqnos.workers_per_reader Setting should be an integer greater than 0")
		}
	}

	if !internal {
		if val, ok := newConfig["indexer.settings.storage_mode"]; ok {
			if len(val.String()) != 0 {

				if currentVal, ok := current["indexer.settings.storage_mode"]; ok {
					if len(currentVal.String()) != 0 {
						if currentVal.String() == val.String() {
							return nil
						}
						return fmt.Errorf("Storage mode is already set to %v", currentVal)
					}
				}

				storageMode := strings.ToLower(val.String())
				if common.GetBuildMode() == common.ENTERPRISE {
					if storageMode == common.ForestDB {
						return errors.New("ForestDB storage mode is not supported for enterprise version")
					}
				} else {
					if storageMode == common.PlasmaDB {
						return errors.New("Plasma storage mode is not supported for community version")
					}
					if storageMode == common.MemoryOptimized || storageMode == common.MemDB {
						return errors.New("Memory optimized storage mode is not supported for community version")
					}
				}
			}
		}
	}

	// ToDo: Validate other settings
	return nil
}

func isValidDay(day string) bool {
	validDays := []string{"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"}
	for _, validDay := range validDays {
		if day == validDay {
			return true
		}
	}
	return false
}

func isValidDaysOfWeek(value []byte) bool {
	conf, _ := common.NewConfig(value)
	if val, ok := conf[compactionDaysSetting]; ok {
		for _, day := range val.Strings() {
			if !isValidDay(day) {
				return false
			}
		}
	}
	return true
}

// Try upgrading the config and fix any issues in config values
// Return true if upgraded, else false
func tryUpgradeConfig(value []byte) ([]byte, bool) {
	conf, err := common.NewConfig(value)
	if err != nil {
		logging.Errorf("tryUpgradeConfig: Failed to parse config err [%v]", err)
		return value, false
	}

	var upgradedCompactionDaysSetting, upgradedBloomSetting bool

	// Correct compaction days setting
	if val, ok := conf[compactionDaysSetting]; ok {
		if !isValidDaysOfWeek(value) {

			// Try correcting the value
			conf.SetValue(compactionDaysSetting, strings.Title(val.String()))

			if !isValidDaysOfWeek(conf.Json()) {
				// Could not correct the value

				logging.Errorf("tryUpgradeConfig: %v has invalid value %v. Setting it to empty value. "+
					"Update the setting to a valid value.",
					compactionDaysSetting, val.String())

				conf.SetValue(compactionDaysSetting, "")
			}

			upgradedCompactionDaysSetting = true
		}
	}

	// Correct plasma bloom filter setting
	if value, upgradedBloomSetting, err = common.MapSettings(conf.Json()); err != nil {
		logging.Errorf("tryUpgradeConfig: Failed to map settings err [%v]", err)
		return value, false
	}

	return value, upgradedCompactionDaysSetting || upgradedBloomSetting
}
