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
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/indexing/secondary/audit"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/logging/systemevent"
	mc "github.com/couchbase/indexing/secondary/manager/common"
	"github.com/couchbase/indexing/secondary/pipeline"
	"github.com/couchbase/indexing/secondary/stubs/nitro/mm"
	"github.com/couchbase/indexing/secondary/stubs/nitro/plasma"
	"github.com/couchbase/indexing/secondary/system"

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

// settingsManager implements dynamic settings management for indexer.
type settingsManager struct {
	supvCmdch       MsgChannel
	supvMsgch       MsgChannel
	config          common.Config
	cancelCh        chan struct{}
	compactionToken []byte
	indexerReady    bool
	notifyPending   bool
}

// NewSettingsManager is the settingsManager constructor. Indexer creates a child singleton of this.
func NewSettingsManager(supvCmdch MsgChannel,
	supvMsgch MsgChannel, config common.Config) (*settingsManager, common.Config, Message) {

	s := settingsManager{
		supvCmdch: supvCmdch,
		supvMsgch: supvMsgch,
		config:    config,
		cancelCh:  make(chan struct{}),
	}

	// Set cgroup overrides; these will be 0 if cgroups are not supported. Must be set before
	// calling GetSettingsConfig else that can result in an async callback based on the (now stale)
	// version of config that was passed to it when called, wiping out these new config settings.
	sigarMemoryMax, sigarNumCpuPrc := sigarGetMemoryMaxAndNumCpuPrc()
	const memKey = "indexer.cgroup.memory_quota"
	const cpuKey = "indexer.cgroup.max_cpu_percent"
	value := config[memKey]
	value.Value = sigarMemoryMax
	config[memKey] = value
	value = config[cpuKey]
	value.Value = sigarNumCpuPrc
	config[cpuKey] = value

	numCPU := config.GetIndexerNumCpuPrc() / 100
	numSliceWritersKey := "indexer.numSliceWriters"
	value = config[numSliceWritersKey]
	value.Value = numCPU
	config[numSliceWritersKey] = value
	logging.Infof("IndexerSettingsManager: Setting numSliceWriters to %v cgroup.max_cpu_percent: %v runtime.NumCPU: %v", numCPU, sigarNumCpuPrc, runtime.NumCPU()*100)

	// This method will merge metakv indexer settings onto default settings.
	config, err := common.GetSettingsConfig(config)
	if err != nil {
		return &s, nil, &MsgError{
			err: Error{
				category: INDEXER,
				cause:    err,
				severity: FATAL,
			}}
	}

	// Initialize the global config settings
	s.setGlobalSettings(nil, config)

	go func() {
		fn := func(r int, err error) error {
			if r > 0 {
				logging.Errorf("IndexerSettingsManager: metakv notifier failed (%v)..Restarting %v", err, r)
			}
			err = metakv.RunObserveChildren("/", s.metaKVCallback, s.cancelCh)
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
	return &s, indexerConfig, &MsgSuccess{}
}

// sigarGetMemoryMaxAndNumCpuPrc returns memory_max and num_cpu_prc from sigar when cgroups are
// supported.
//
//	memory_max is the memory quota in bytes, not accounting for GSI's exposed user override
//	  indexer.settings.memory_quota.
//	num_cpu_prc is the number of CPUs to use * 100 either from the cgroup or from ns_server's
//	  COUCHBASE_CPU_COUNT environment variable which overrides it. This value does not account for
//	  GSI's exposed user override indexer.settings.max_cpu_percent.
//
// Returns 0, 0 if cgroups are not supported or sigar fails.
func sigarGetMemoryMaxAndNumCpuPrc() (uint64, int) {
	const _sigarGetMemoryMaxAndNumCpuPrc = "settings::sigarGetMemoryMaxAndNumCpuPrc:"

	// Start a sigar process
	systemStats, err := system.NewSystemStats()
	if err != nil {
		logging.Infof("%v sigar failed to start: NewSystemStats returned error: %v",
			_sigarGetMemoryMaxAndNumCpuPrc, err)
		return 0, 0
	}
	defer systemStats.Close()

	// Get cgroup info and check if cgroups are supported
	cgroupInfo := systemStats.GetControlGroupInfo()
	if cgroupInfo.Supported != common.SIGAR_CGROUP_SUPPORTED {
		return 0, 0
	}

	// Cgroups are supported, so return its mem and CPU values. (CPU value will pick up any user
	// override from ns_server's COUCHBASE_CPU_COUNT environment variable.)
	return cgroupInfo.MemoryMax, int(cgroupInfo.NumCpuPrc)
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
	upgradedConfig, upgraded, minNumShardChanged := tryUpgradeConfig(value)
	if upgraded {
		if err := metakv.Set(common.IndexingSettingsMetaPath, upgradedConfig, rev); err != nil {
			return err
		}
		return nil
	}

	oldConfig := s.config.Clone()

	newConfig := s.config.Clone()
	newConfig.Update(value)
	s.setGlobalSettings(s.config, newConfig)

	// If the minNumShard setting has not been explicitly updated, then compute it based on max_cpu_percent
	if !minNumShardChanged {
		value := common.ConfigValue{
			Value:         uint64(math.Max(2.0, float64(runtime.GOMAXPROCS(0))*0.25)),
			Help:          "Minimum number of shard",
			DefaultVal:    uint64(math.Max(2.0, float64(runtime.GOMAXPROCS(0))*0.25)),
			Immutable:     false,
			Casesensitive: false,
		}
		newConfig["indexer.plasma.minNumShard"] = value
		logging.Infof("SettingsManager::applySettings Updating 'plasma.minNumShard' setting to: %v", value.Uint64())
	}

	s.config = newConfig

	indexerConfig := s.config.SectionConfig("indexer.", true)
	s.supvMsgch <- &MsgConfigUpdate{
		cfg: indexerConfig,
	}

	diffOld, diffNew := oldConfig.SectionConfig("indexer.", false).Diff(
		newConfig.SectionConfig("indexer.", false))
	if len(diffOld) != 0 {
		se := systemevent.NewSettingsChangeEvent("settingsManager:applySettings",
			diffOld.Map(), diffNew.Map())
		eventID := systemevent.EVENTID_INDEXER_SETTINGS_CHANGE
		systemevent.InfoEvent("Indexer", eventID, se)
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

	// enable bloomFilter for plasma back indexes
	go s.enablePageBloomFilterPlasmaBackIndex()
}

// Note: We are enabling bloom filter for all the active indexer nodes on first node addition
// In mixed mode cluster
// * If all the indexers have support for bloom filter we will have overhead on all the
// . nodes due to bloom filter and should not impact planner
// * If few indexers are having support and few indexers does not have support we are
// . assuming that the overhead is not huge and should not impact planner
func (s *settingsManager) enablePageBloomFilterPlasmaBackIndex() {
	selfRestart := func() {
		time.Sleep(5 * time.Second)
		go s.enablePageBloomFilterPlasmaBackIndex()
	}

	defer func() {
		if r := recover(); r != nil {
			logging.Errorf("settingsManager::enablePageBloomFilterPlasmaBackIndex crashed: %v\n", r)
			logging.Errorf("settingsManager::enablePageBloomFilterPlasmaBackIndex recovered from exception at %s", logging.StackTrace())
			selfRestart()
		}
	}()

	// Checks if Plasma BloomFilter is enabled for back index or not
	checkBloomFilterEnabled := func() bool {
		isPythonCaseCfgSet := s.config["indexer.settings.enable_page_bloom_filter"].Bool()
		isCamelCaseCfgSet := s.config["indexer.plasma.backIndex.enablePageBloomFilter"].Bool()

		// even if one of the entry is enabled, someone has already enabled/disabled the feature
		logging.Debugf("Indexer::enablePageBloomFilterPlasmaBackIndex setting value enable_page_bloom_filter: %v, enablePageBloomFilter:%v",
			isPythonCaseCfgSet, isCamelCaseCfgSet)
		if isPythonCaseCfgSet || isCamelCaseCfgSet {
			return true
		}
		return false
	}

	// send settings request to local http url
	postRequestFn := func() error {
		url := "/settings"

		clusterAddr := s.config["indexer.clusterAddr"].String() // "127.0.0.1:<cluster_admin_port>" (eg 8091)
		host, _, _ := net.SplitHostPort(clusterAddr)
		port := s.config["indexer.httpPort"].String()
		addr := net.JoinHostPort(host, port) // "127.0.0.1:<indexer_http_port"> (eg 9102, 9108, ...)

		logging.Debugf("Indexer::enablePageBloomFilterPlasmaBackIndex addr : %v .", addr)

		enablePageBloomFilterPlasmaBackIndex := struct {
			Settings1 bool `json:"indexer.settings.enable_page_bloom_filter"`
			Settings2 bool `json:"indexer.plasma.backIndex.enablePageBloomFilter"`
		}{true, true}

		body, err := json.Marshal(enablePageBloomFilterPlasmaBackIndex)
		if err != nil {
			logging.Errorf("Indexer::enablePageBloomFilterPlasmaBackIndex error in marshalling settings request, err: %v", err)
			return err
		}

		bodybuf := bytes.NewBuffer(body)

		resp, err := postWithAuth(addr+url, "application/json", bodybuf)
		if err != nil {
			logging.Errorf("Indexer::enablePageBloomFilterPlasmaBackIndex error in posting http request for settings change on %v, err: %v",
				addr+url, err)
			return err
		}
		ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		return nil
	}

	enabled := checkBloomFilterEnabled()
	if enabled {
		logging.Debugf("Indexer::enablePageBloomFilterPlasmaBackIndex setting is already enabled")
		return
	}

	// add random delay so that not all indexers will try together
	time.Sleep(time.Second * time.Duration(rand.Intn(30)))

	// get the metakv value
	exists, err := mc.EnablePageBloomFilterPlasmaBackIndexTokenExist()
	if err != nil {
		logging.Errorf("Indexer::enablePageBloomFilterPlasmaBackIndex error in getting feature value from metakv, err: %v ", err)
		selfRestart()
		return
	}
	if exists {
		logging.Infof("Indexer::enablePageBloomFilterPlasmaBackIndex feature is already enabled")
		return
	}
	logging.Infof("Indexer::enablePageBloomFilterPlasmaBackIndex trying to enable feature")

	err = postRequestFn()
	if err != nil {
		selfRestart()
		return
	}
	// update metakv that we are done
	if err := mc.PostEnablePageBloomFilterPlasmaBackIndexToken(); err != nil {
		logging.Errorf("Indexer::enablePageBloomFilterPlasmaBackIndex error in setting feature value in metakv. err: %v", err)
		selfRestart()
		return
	}
	logging.Infof("Indexer::enablePageBloomFilterPlasmaBackIndex done enabling feature")
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

// setGlobalSettings is used to both initialize and update global config settings.
func (s *settingsManager) setGlobalSettings(oldCfg, newCfg common.Config) {
	const _setGlobalSettings = "settingsManager::setGlobalSettings:"

	setBlockPoolSize(oldCfg, newCfg)

	// Set number of CPU cores to use to min(node, cgroup, GSI)
	ncpu := common.SetNumCPUs(newCfg.GetIndexerNumCpuPrc())
	memoryQuota := float64(newCfg.GetIndexerMemoryQuota())
	logging.Infof(
		"%v Indexer # CPU cores: %v, memory quota: %.0f bytes (%.3f KB, %.3f MB, %.3f GB)",
		_setGlobalSettings, ncpu, memoryQuota, memoryQuota/1024, memoryQuota/(1024*1024),
		memoryQuota/(1024*1024*1024))

	setLogger(newCfg)
	useMutationSyncPool = newCfg["indexer.useMutationSyncPool"].Bool()

	newEncodeCompatMode := EncodeCompatMode(newCfg["indexer.encoding.encode_compat_mode"].Int())
	if gEncodeCompatMode != newEncodeCompatMode {
		gEncodeCompatMode = newEncodeCompatMode
		logging.Infof("Set EncodeCompatMode %v", gEncodeCompatMode)
	}

	var oldT uint32
	if oldCfg != nil {
		oldT = oldCfg["indexer.cinfo_lite.force_after"].Uint32()
	}
	newT := newCfg["indexer.cinfo_lite.force_after"].Uint32()
	if oldT != newT {
		common.SetCICLMgrTimeDiffToForceFetch(newT)
	}

	var oldNotifierSleep uint32
	if oldCfg != nil {
		oldNotifierSleep = oldCfg["indexer.cinfo_lite.notifier_restart_sleep"].Uint32()
	}
	newNotifierSleep := newCfg["indexer.cinfo_lite.notifier_restart_sleep"].Uint32()
	if oldNotifierSleep != newNotifierSleep {
		common.SetCICLMgrSleepTimeOnNotifierRestart(newNotifierSleep)
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
func tryUpgradeConfig(value []byte) ([]byte, bool, bool) {
	conf, err := common.NewConfig(value)
	if err != nil {
		logging.Errorf("tryUpgradeConfig: Failed to parse config err [%v]", err)
		return value, false, false
	}

	var upgradedCompactionDaysSetting, upgradedBloomSetting bool
	var minNumShardChanged bool

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

	if _, ok := conf["indexer.plasma.minNumShard"]; ok {
		minNumShardChanged = true
	}

	// Correct plasma bloom filter setting
	if value, upgradedBloomSetting, err = common.MapSettings(conf.Json()); err != nil {
		logging.Errorf("tryUpgradeConfig: Failed to map settings err [%v]", err)
		return value, false, minNumShardChanged
	}

	return value, upgradedCompactionDaysSetting || upgradedBloomSetting, minNumShardChanged
}
