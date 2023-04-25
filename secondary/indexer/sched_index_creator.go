package indexer

import (
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/manager/client"
	mc "github.com/couchbase/indexing/secondary/manager/common"

	"container/heap"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

var SCHED_TOKEN_CHECK_INTERVAL = 2000   // Milliseconds
var SCHED_TOKEN_PROCESS_INTERVAL = 2000 // Milliseconds
var STOP_TOKEN_CLEANER_INITERVAL = 60   // Seconds
var TOKEN_MOVER_INTERVAL = 60           // Seconds
var STOP_TOKEN_RETENTION_TIME = 600     // Seconds
var SCHED_PROCESS_INIT_INTERVAL = 60    // Seconds
var KEYSPACE_CLEANER_INTERVAL = 20      // Seconds

var RETRYABLE_ERROR_BACKOFF = int64(5 * time.Second)
var NON_RETRYABLE_ERROR_BACKOFF = int64(5 * time.Second)
var NETWORK_ERROR_BACKOFF = int64(5 * time.Second)

var RANDOM_BACKOFF_START = 50 // Milliseconds
var RANDOM_BACKOFF_END = 5000 // Milliseconds

var DEFAULT_MAX_CREATION_RETRIES = 1000

/////////////////////////////////////////////////////////////////////
// Global Variables
/////////////////////////////////////////////////////////////////////

var gSchedIndexCreator *schedIndexCreator
var gSchedIndexCreatorLck sync.RWMutex // protects gSchedIndexCreator which is assigned only once

var useSecondsFromCtime bool

func init() {
	if unsafe.Sizeof(int(1)) < unsafe.Sizeof(int64(1)) {
		useSecondsFromCtime = true
	}
}

//
// Get schedIndexCreator singleton
//
func getSchedIndexCreator() *schedIndexCreator {
	gSchedIndexCreatorLck.RLock()
	defer gSchedIndexCreatorLck.RUnlock()

	return gSchedIndexCreator
}

//
// Set schedIndexCreator singleton
//
func setSchedIndexCreator(sic *schedIndexCreator) {
	gSchedIndexCreatorLck.Lock()
	gSchedIndexCreator = sic
	gSchedIndexCreatorLck.Unlock()
}

/////////////////////////////////////////////////////////////////////
// Data Types
/////////////////////////////////////////////////////////////////////

//
// Scheduled index creator (schedIndexCreator), checks up on the scheduled
// create tokens and tries to create index with the help of metadata provider.
// 1. If the index creation succeeds, scheduled create token is deleted.
// 2. If the index creation fails, the index creation will be retried, based
//    on the reason for failure.
// 3. The retry count is tracked in memory and after the retry limit is reached
//    the stop schedule create token will be posted with last error.
// 3.1. Once the stop schedule create token is posted, the index creation will
//      not be retried again (until indexer restarts). The schedule create token
//      will NOT be deleted. Failed index will continue to show on UI (in Error
//      state), until the user explicitly deletes the failed index.
// 4. Similar to DDL service manager, scheduled index creator observes mutual
//    exclusion with rebalance operation.
type schedIndexCreator struct {
	indexerId    common.IndexerId
	config       common.ConfigHolder
	oldProviders []*client.MetadataProvider
	provider     *client.MetadataProvider
	proMutex     sync.Mutex
	supvCmdch    MsgChannel //supervisor sends commands on this channel
	supvMsgch    MsgChannel //channel to send any message to supervisor
	clusterAddr  string
	settings     *ddlSettings
	killch       chan bool

	allowDDL      bool         // allow new DDL to start? false during Rebalance
	allowDDLMutex sync.RWMutex // protects allowDDL

	mon             *schedTokenMonitor
	indexQueue      *schedIndexQueue
	queueMutex      sync.Mutex
	backoff         int64 // random backoff in nanoseconds before the next request.
	stopCleaner     chan bool
	stopMover       chan bool
	stopKeyspaceMon chan bool

	cinfoProvider     common.ClusterInfoProvider
	cinfoProviderLock sync.Mutex
}

//
// scheduledIndex type holds information about one index that is scheduled
// for background creation. This information contains the (1) token and
// (2) information related to failed attempts to create this index.
// The scheduledIndexes are maintained as a heap, so the struct implements
// properties required by golang contanier/heap implementation.
//
type scheduledIndex struct {
	token *mc.ScheduleCreateToken
	state *scheduledIndexState

	// container/heap properties
	priority int
	index    int
}

//
// scheduledIndexState maintains the information about the failed attempts
// at the time of creating the index.
//
type scheduledIndexState struct {
	lastError   error // Error returned by the last creation attempt.
	retryCount  int   // Number of attempts failed by retryable errors.
	nRetryCount int   // Number of attempts failed by non retryable errors.
}

//
// Schedule token monitor checks if there are any new indexes that are
// scheduled for creation.
//
type schedTokenMonitor struct {
	creator         *schedIndexCreator
	commandListener *mc.CommandListener
	listenerDonech  chan bool
	uCloseCh        chan bool
	processed       map[string]bool
	indexerId       common.IndexerId
}

//
// schedIndexQueue is used as a priority queue where the indexes are processed
// in fist-come-first-served manner, based on the creation time of the request.
//
type schedIndexQueue []*scheduledIndex

///////////////////////////////////////////////////////////////////
// Constructor and member functions for schedIndexCreator
/////////////////////////////////////////////////////////////////////

func NewSchedIndexCreator(indexerId common.IndexerId, supvCmdch MsgChannel,
	supvMsgch MsgChannel, config common.Config) (*schedIndexCreator, Message) {

	addr := config["clusterAddr"].String()
	numReplica := int32(config["settings.num_replica"].Int())
	numPartition := int32(config["numPartitions"].Int())

	settings := &ddlSettings{
		numReplica:   numReplica,
		numPartition: numPartition,
	}

	allowPartialQuorum := config["allowPartialQuorum"].Bool()
	if allowPartialQuorum {
		atomic.StoreUint32(&settings.allowPartialQuorum, 1)
	}

	useGreedyPlanner := config["planner.useGreedyPlanner"].Bool()
	if useGreedyPlanner {
		atomic.StoreUint32(&settings.useGreedyPlanner, 1)
	}

	iq := make(schedIndexQueue, 0)
	heap.Init(&iq)

	mgr := &schedIndexCreator{
		indexerId:       indexerId,
		supvCmdch:       supvCmdch,
		supvMsgch:       supvMsgch,
		clusterAddr:     addr,
		settings:        settings,
		killch:          make(chan bool),
		allowDDL:        true,
		indexQueue:      &iq,
		stopCleaner:     make(chan bool),
		stopMover:       make(chan bool),
		stopKeyspaceMon: make(chan bool),
	}

	mgr.mon = NewSchedTokenMonitor(mgr, indexerId)

	mgr.config.Store(config)

	cip := mgr.getCinfoNoLock()

	mgr.cinfoProviderLock.Lock()
	mgr.cinfoProvider = cip
	mgr.cinfoProviderLock.Unlock()

	go mgr.run()
	go mgr.stopTokenCleaner()
	go mgr.orphanTokenMover()
	go mgr.keyspaceMonitor()

	setSchedIndexCreator(mgr)

	logging.Infof("schedIndexCreator: intialized.")
	return mgr, &MsgSuccess{}
}

func (m *schedIndexCreator) run() {

	go m.processSchedIndexes()

loop:
	for {
		select {

		case cmd, ok := <-m.supvCmdch:
			if ok {
				if cmd.GetMsgType() == ADMIN_MGR_SHUTDOWN {
					m.Close()
					m.supvCmdch <- &MsgSuccess{}
					break loop
				}

				m.handleSupervisorCommands(cmd)
			} else {
				//supervisor channel closed. exit
				break loop
			}
		}
	}
}

func (m *schedIndexCreator) handleSupervisorCommands(cmd Message) {
	switch cmd.GetMsgType() {

	case CONFIG_SETTINGS_UPDATE:
		cfgUpdate := cmd.(*MsgConfigUpdate)

		newConfig := cfgUpdate.GetConfig()
		oldConfig := m.config.Load()
		newUseCInfoLite := newConfig["use_cinfo_lite"].Bool()
		oldUseCInfoLite := oldConfig["use_cinfo_lite"].Bool()

		m.config.Store(newConfig)

		if oldUseCInfoLite != newUseCInfoLite {
			logging.Infof("schedIndexCreator:handleSupervisorCommands Updating ClusterInfoProvider in schedIndexCreator")

			cip := m.getCinfoNoLock()
			if cip == nil {
				logging.Warnf("schedIndexCreator:handleSupervisorCommands Unable to update ClusterInfoProvider in schedIndexCreator use_cinfo_lite: old %v new %v",
					oldUseCInfoLite, newUseCInfoLite)
				// Not crashing here due to below
			}

			// Setting cinfoProvider to nil as in schedIndexCreator whenever
			// cinfoProvider is used nil check if done and if nil its fetched
			// again at that point
			m.cinfoProviderLock.Lock()
			oldProvider := m.cinfoProvider
			m.cinfoProvider = cip
			m.cinfoProviderLock.Unlock()

			if cip != nil {
				logging.Infof("schedIndexCreator:handleSupervisorCommands Updated ClusterInfoProvider in schedIndexCreator use_cinfo_lite: old %v new %v",
					oldUseCInfoLite, newUseCInfoLite)
			}

			if oldProvider != nil {
				oldProvider.Close()
			}
		}

		m.settings.handleSettings(cfgUpdate.GetConfig())
		m.supvCmdch <- &MsgSuccess{}

	default:
		logging.Fatalf("schedIndexCreator::handleSupervisorCommands Unknown Message %+v", cmd)
		common.CrashOnError(errors.New("Unknown Msg On Supv Channel"))
	}
}

func (m *schedIndexCreator) stopProcessDDL() {
	func() {
		m.allowDDLMutex.Lock()
		defer m.allowDDLMutex.Unlock()

		m.allowDDL = false
	}()

	func() {
		m.proMutex.Lock()
		defer m.proMutex.Unlock()

		// At this point, old provider might be in the middle of
		// processing a DDL operation. Keep a reference of old
		// provider so that it can be closed after DDL processing is done
		m.oldProviders = append(m.oldProviders, m.provider)
		m.provider = nil
	}()

	logging.Infof("schedIndexCreator::stopProcessDDL done")
}

func (m *schedIndexCreator) canProcessDDL(allowDDLAtRebalance bool) bool {
	m.allowDDLMutex.RLock()
	defer m.allowDDLMutex.RUnlock()

	if !allowDDLAtRebalance {
		return m.allowDDL
	}

	if !m.allowDDL { // rebalance is running. Allow or reject based on the config
		cfg := m.config.Load()
		if common.IsServerlessDeployment() {
			val, ok := cfg["serverless.allowDDLDuringRebalance"]
			if ok {
				return val.Bool()
			}
		} else {
			val, ok := cfg["allowDDLDuringRebalance"]
			if ok {
				return val.Bool()
			}
		}
		return false
	}

	return true
}

func (m *schedIndexCreator) startProcessDDL() {
	func() {
		m.allowDDLMutex.Lock()
		defer m.allowDDLMutex.Unlock()

		m.allowDDL = true
	}()

	func() {
		m.proMutex.Lock()
		defer m.proMutex.Unlock()

		// At this point, old provider might be in the middle of
		// processing a DDL operation. Keep a reference of old
		// provider so that it can be closed after DDL processing is done
		m.oldProviders = append(m.oldProviders, m.provider)
		m.provider = nil
	}()

	logging.Infof("schedIndexCreator::startProcessDDL done")
}

func (m *schedIndexCreator) rebalanceDone() {
	// TODO: Need to check if provider needs to be reset.
}

func (m *schedIndexCreator) processSchedIndexes() {

	// Sleep for some time before starting.
	time.Sleep(time.Duration(SCHED_PROCESS_INIT_INTERVAL) * time.Second)

	// Check for new indexes to be created every 5 seconds
	ticker := time.NewTicker(time.Duration(SCHED_TOKEN_PROCESS_INTERVAL) * time.Millisecond)

	for {
		select {
		case <-ticker.C:

			func() {
				m.proMutex.Lock()
				defer m.proMutex.Unlock()

				// Metadata provider is reset (by setting m.provider to nil) at the start
				// and end of every rebalance. During that time, it is possible that a
				// DDL operation is on-going. Hence, close all the old metadata providers
				// after DDL operation to prevent any metadata provider leaks
				if m.oldProviders != nil {
					for _, provider := range m.oldProviders {
						if provider != nil {
							provider.Close()
						}
					}
					m.oldProviders = nil
				}
			}()

			if !m.canProcessDDL(true) {
				continue
			}

			cfg := m.config.Load()
			enabled, ok := cfg["debug.enableBackgroundIndexCreation"]
			if ok {
				if !enabled.Bool() {
					continue
				}
			}

		innerLoop:
			for {
				if !m.canProcessDDL(true) {
					break innerLoop
				}

				cfg := m.config.Load()
				enabled, ok := cfg["debug.enableBackgroundIndexCreation"]
				if ok {
					if !enabled.Bool() {
						break innerLoop
					}
				}

				index := m.popQ()
				if index == nil {
					break innerLoop
				}

				m.processIndex(index)
			}

		case <-m.killch:
			logging.Infof("schedIndexCreator: Stopping processSchedIndexes routine ...")
			return
		}
	}
}

func (m *schedIndexCreator) processIndex(index *scheduledIndex) {
	logging.Infof("schedIndexCreator: Trying to create index %v, %v", index.token.Definition.DefnId, index.token.Ctime)
	err, success := m.tryCreateIndex(index)
	if err != nil {
		retry, dropToken := m.handleError(index, err)
		if retry {
			logging.Errorf("schedIndexCreator: error(%v) while creating index %v. The operation will be retried. Current retry counts (%v,%v).",
				err, index.token.Definition.DefnId, index.state.retryCount, index.state.nRetryCount)
			m.pushQ(index)
		} else {
			// TODO: Check if we need a console log
			logging.Errorf("schedIndexCreator: error(%v) while creating index %v. The operation failed after retry counts (%v,%v).",
				err, index.token.Definition.DefnId, index.state.retryCount, index.state.nRetryCount)

			if !dropToken {
				err := mc.PostStopScheduleCreateToken(index.token.Definition.DefnId, err.Error(), time.Now().UnixNano(), index.token.IndexerId)
				if err != nil {
					logging.Errorf("schedIndexCreator: error (%v) in posting the stop schedule create token for %v",
						err, index.token.Definition.DefnId)
				}
			} else {
				err := mc.DeleteScheduleCreateToken(index.token.Definition.DefnId)
				if err != nil {
					logging.Errorf("schedIndexCreator: error (%v) in deleting the schedule create token for %v",
						err, index.token.Definition.DefnId)
				}
			}
		}
	} else {
		if success {
			logging.Infof("schedIndexCreator: successfully created index %v", index.token.Definition.DefnId)
			if !index.token.Definition.Deferred {
				logging.Infof("schedIndexCreator: index %v was created with non-deferred build. "+
					"DDL service manager will build the index", index.token.Definition.DefnId)
			}

			// TODO: Check if this doesn't error out in case of key not found.
			err := mc.DeleteScheduleCreateToken(index.token.Definition.DefnId)
			if err != nil {
				logging.Errorf("schedIndexCreator: error (%v) in deleting the schedule create token for %v",
					err, index.token.Definition.DefnId)
			}
		}
	}

}

func (m *schedIndexCreator) handleError(index *scheduledIndex, err error) (bool, bool) {
	index.state.lastError = err

	checkErr := func(knownErrs []error) bool {
		for _, e := range knownErrs {
			if strings.Contains(err.Error(), e.Error()) {
				return true
			}
		}

		return false
	}

	retryable := false
	network := false

	setBackoff := func() {
		if strings.Contains(err.Error(), common.ErrAnotherIndexCreation.Error()) {

			// TODO: The value of this backoff should be a function of
			//       network latency and number of indexer nodes.

			rand.Seed(time.Now().UnixNano())
			diff := RANDOM_BACKOFF_END - RANDOM_BACKOFF_START
			b := rand.Intn(diff) + RANDOM_BACKOFF_START
			m.backoff = int64(b * 1000 * 1000)
			return
		}

		if retryable {
			if network {
				m.backoff = NETWORK_ERROR_BACKOFF
				return
			}

			m.backoff = RETRYABLE_ERROR_BACKOFF
			return
		}

		m.backoff = NON_RETRYABLE_ERROR_BACKOFF
	}

	defer setBackoff()

	// Check for known non-retryable errors.
	if checkErr(common.NonRetryableErrorsInCreate) {
		// TODO: Fix non-retryable error retry count.
		index.state.nRetryCount++

		if checkErr(common.KeyspaceDeletedErrorsInCreate) {
			return false, true
		}
		return false, false
	}

	retryable = true
	index.state.retryCount++

	var maxRetries int
	cfg := m.config.Load()
	maxRetriesVal, ok := cfg["scheduleCreateRetries"]
	if !ok {
		maxRetries = DEFAULT_MAX_CREATION_RETRIES
	} else {
		maxRetries = maxRetriesVal.Int()
	}

	if index.state.retryCount > maxRetries {
		return false, false
	}

	// Check for known retryable error
	if checkErr(common.RetryableErrorsInCreate) {
		return true, false
	}

	isNetworkError := func() bool {
		// Because the exact error may have got embedded in the error
		// received, need to check for substring. If any of the following
		// substring is found, most likely the error is network error.
		if strings.Contains(err.Error(), io.EOF.Error()) ||
			strings.Contains(err.Error(), syscall.ECONNRESET.Error()) ||
			strings.Contains(err.Error(), syscall.EPIPE.Error()) ||
			strings.Contains(err.Error(), "i/o timeout") {

			return true
		}

		return false
	}

	if isNetworkError() {
		network = true
		return true, false
	}

	// Treat all unknown erros as retryable errors
	return true, false
}

func (m *schedIndexCreator) getMetadataProvider() (*client.MetadataProvider, error) {

	m.proMutex.Lock()
	defer m.proMutex.Unlock()

	if m.provider == nil {
		logging.Infof("schedIndexCreator: Initializing new metadata provider")
		provider, _, _, err := newMetadataProvider(m.clusterAddr, nil, m.settings, "schedIndexCreator")
		if err != nil {
			return nil, err
		}

		m.provider = provider
	}

	return m.provider, nil
}

func (m *schedIndexCreator) tryCreateIndex(index *scheduledIndex) (error, bool) {
	exists, err := mc.StopScheduleCreateTokenExist(index.token.Definition.DefnId)
	if err != nil {
		logging.Errorf("schedIndexCreator:tryCreateIndex error (%v) in getting stop schedule create token for %v",
			err, index.token.Definition.DefnId)
		return err, false
	}

	if exists {
		logging.Debugf("schedIndexCreator:tryCreateIndex stop schedule token exists for %v",
			index.token.Definition.DefnId)
		return nil, false
	}

	exists, err = mc.DeleteCommandTokenExist(index.token.Definition.DefnId)
	if err != nil {
		logging.Errorf("schedIndexCreator:tryCreateIndex error (%v) in getting delete command token for %v",
			err, index.token.Definition.DefnId)
		return err, false
	}

	if exists {
		logging.Infof("schedIndexCreator:tryCreateIndex delete command token exists for %v",
			index.token.Definition.DefnId)
		return nil, false
	}

	if m.backoff > 0 {
		logging.Debugf("schedIndexCreator:tryCreateIndex using %v backoff for index %v",
			time.Duration(m.backoff), index.token.Definition.DefnId)
		time.Sleep(time.Duration(m.backoff))

		// Reset the backoff for the next attempt
		m.backoff = 0
	}

	var provider *client.MetadataProvider
	provider, err = m.getMetadataProvider()
	if err != nil {
		logging.Errorf("schedIndexCreator:tryCreateIndex error (%v) in getting getMetadataProvider for %v",
			err, index.token.Definition.DefnId)
		return err, false
	}

	// check if ScheduleCreateToken still exists - drop index may have deleted ScheduleCreateToken
	exists, err = mc.ScheduleCreateTokenExist(index.token.Definition.DefnId)
	if err != nil {
		logging.Errorf("schedIndexCreator:tryCreateIndex error (%v) in getting schedule create command token for %v",
			err, index.token.Definition.DefnId)
		return err, false
	}

	if !exists {
		logging.Infof("schedIndexCreator:tryCreateIndex create command token does not exists for %v",
			index.token.Definition.DefnId)
		return nil, false
	}

	// Following check is an effort to check if index was created but the
	// schedule create token was not deleted.
	if provider.FindIndexIgnoreStatus(index.token.Definition.DefnId) != nil {
		logging.Infof("schedIndexCreator:tryCreateIndex index %v is already created", index.token.Definition.DefnId)
		return nil, true
	}

	// If the bucket/scope/collection was dropped after the token creation,
	// the index creation should fail due to id mismatch.
	index.token.Definition.BucketUUID = index.token.BucketUUID
	index.token.Definition.ScopeId = index.token.ScopeId
	index.token.Definition.CollectionId = index.token.CollectionId

	err = provider.CreateIndexWithDefnAndPlan(&index.token.Definition, index.token.Plan, index.token.Ctime)
	if err != nil {
		return err, false
	}

	return nil, true
}

func (m *schedIndexCreator) pushQ(item *scheduledIndex) {
	m.queueMutex.Lock()
	defer m.queueMutex.Unlock()

	heap.Push(m.indexQueue, item)
}

func (m *schedIndexCreator) popQ() *scheduledIndex {
	m.queueMutex.Lock()
	defer m.queueMutex.Unlock()

	if m.indexQueue.Len() <= 0 {
		return nil
	}

	si, ok := heap.Pop(m.indexQueue).(*scheduledIndex)
	if !ok {
		return nil
	}

	return si
}

func (m *schedIndexCreator) stopTokenCleaner() {
	// Sleep for some time before starting.
	time.Sleep(time.Duration(SCHED_PROCESS_INIT_INTERVAL) * time.Second)

	ticker := time.NewTicker(time.Duration(STOP_TOKEN_CLEANER_INITERVAL) * time.Second)
	retention := int64(STOP_TOKEN_RETENTION_TIME) * int64(time.Second)

	for {
		select {
		case <-ticker.C:
			stopTokens, err := mc.ListAllStopScheduleCreateTokens()
			if err != nil {
				logging.Errorf("schedIndexCreator:stopTokenCleaner error in getting stop schedule create tokens: %v", err)
				continue
			}

			for _, token := range stopTokens {
				exists, err := mc.ScheduleCreateTokenExist(token.DefnId)
				if err != nil {
					logging.Infof("schedIndexCreator:stopTokenCleaner error (%v) in ScheduleCreateTokenExist for %v", err, token.DefnId)
					continue
				}

				if exists {
					logging.Debugf("schedIndexCreator:stopTokenCleaner schedule create token exists for defnId %v", token.DefnId)
					continue
				}

				if (time.Now().UnixNano() - retention) > token.Ctime {
					logging.Infof("schedIndexCreator:stopTokenCleaner deleting stop create token for %v", token.DefnId)
					// TODO: Avoid deletion by all indexers.
					err := mc.DeleteStopScheduleCreateToken(token.DefnId)
					if err != nil {
						logging.Errorf("schedIndexCreator:stopTokenCleaner error (%v) in deleting stop create token for %v", err, token.DefnId)
					}
				}
			}

		case <-m.stopCleaner:
			logging.Infof("schedIndexCreator: Stoppinig stopTokenCleaner routine")
			return
		}
	}
}

//
// orphanTokenMover is used to transfer the ownership of the orphan tokens.
// Orphan tokens could get created due to node failover. orphanTokenMover is
// responsible for not letting any tokens remain orphan for a long time.
//
func (m *schedIndexCreator) orphanTokenMover() {
	// Sleep for some time before starting.
	time.Sleep(time.Duration(SCHED_PROCESS_INIT_INTERVAL) * time.Second)

	cfg := m.config.Load()
	clusterURL := cfg["clusterAddr"].String()

	ticker := time.NewTicker(time.Duration(TOKEN_MOVER_INTERVAL) * time.Second)

	for {
		select {
		case <-ticker.C:
			// TODO (Elixir): Can orphan tokens be moved during rebalance
			// Don't proceed with orphan token processing if rebalance is running.
			if !m.canProcessDDL(false) {
				continue
			}

			skipIter := false
			func() {
				m.cinfoProviderLock.Lock()
				defer m.cinfoProviderLock.Unlock()

				if m.cinfoProvider == nil {
					m.cinfoProvider = m.getCinfoNoLock()
					if m.cinfoProvider == nil {
						logging.Warnf("schedIndexCreator:orphanTokenMover nil cluster info. Skipping iteration.")
						skipIter = true
					}
				}
			}()

			if skipIter {
				continue
			}

			m.cinfoProviderLock.Lock()
			ninfo, e := m.cinfoProvider.GetNodesInfoProvider()
			m.cinfoProviderLock.Unlock()
			if e != nil {
				logging.Errorf("schedIndexCreator:orphanTokenMover GetNodesInfoProvider returned err: %v", e)
				continue
			}

			keepNodes := make(map[string]bool)
			runIter := func() bool {
				ninfo.RLock()
				defer ninfo.RUnlock()

				activeNodes := ninfo.GetActiveIndexerNodes()
				if len(activeNodes) <= 0 {
					return false
				}

				uuids := make([]string, 0, len(activeNodes))
				for _, node := range activeNodes {
					uuids = append(uuids, node.NodeUUID)
					keepNodes[node.NodeUUID] = true
				}

				sort.Strings(uuids)
				if uuids[0] == string(m.indexerId) {
					// Only indexer with smallest uuid runs the orphan token mover
					return true
				}

				return false
			}()

			if !runIter {
				logging.Debugf("schedIndexCreator:orphanTokenMover nothing to do. Skipping iteration.")
				continue
			}

			err := transferScheduleTokens(keepNodes, clusterURL)
			if err != nil {
				logging.Errorf("schedIndexCreator:orphanTokenMover error in transferScheduleTokens %v", err)
			} else {
				logging.Debugf("schedIndexCreator:orphanTokenMover iteration done.")
			}

		case <-m.stopMover:
			logging.Infof("schedIndexCreator: Stopping orphanTokenMover routine")
			return
		}
	}

}

//
// keyspaceMonitor is responsible to cleanup of schedule tokens
// which failed during creation.
//
// When indexes scheduled for creation get KeyspaceDeletedErrors, the schedule
// create token will be dropped for those indexes. But the indexes which are
// already errored (i.e. exhaused all the retries), may never get cleaned up.
//
func (m *schedIndexCreator) keyspaceMonitor() {
	// Sleep for some time before starting.
	time.Sleep(time.Duration(SCHED_PROCESS_INIT_INTERVAL) * time.Second)

	ticker := time.NewTicker(time.Duration(KEYSPACE_CLEANER_INTERVAL) * time.Second)

	for {
		select {
		case <-ticker.C:
			stopTokens, err := mc.ListAllStopScheduleCreateTokens()
			if err != nil {
				logging.Errorf("schedIndexCreator:keyspaceMonitor error in getting stop schedule create tokens: %v", err)
				continue
			}

			skipIter := false
			func() {
				m.cinfoProviderLock.Lock()
				defer m.cinfoProviderLock.Unlock()

				if m.cinfoProvider == nil {
					m.cinfoProvider = m.getCinfoNoLock()
					if m.cinfoProvider == nil {
						logging.Warnf("schedIndexCreator:keyspaceMonitor nil cluster info. Skipping iteration.")
						skipIter = true
					}
				}
			}()

			if skipIter {
				continue
			}

			buckets := make(map[string]string)
			scopes := make(map[string]string)
			collections := make(map[string]string)

			for _, stopToken := range stopTokens {
				schedToken, err := mc.GetScheduleCreateToken(stopToken.DefnId)
				if err != nil {
					logging.Infof("schedIndexCreator:keyspaceMonitor error (%v) in GetScheduleCreateToken for %v", err, stopToken.DefnId)
					continue
				}

				if schedToken == nil {
					logging.Debugf("schedIndexCreator:keyspaceMonitor GetScheduleCreateToken returned nil token for %v", err, stopToken.DefnId)
					continue
				}

				defn := schedToken.Definition

				m.cinfoProviderLock.Lock()
				collnInfo, e := m.cinfoProvider.GetCollectionInfoProvider(defn.Bucket)
				m.cinfoProviderLock.Unlock()
				if e != nil {
					logging.Errorf("schedIndexCreator:keyspaceMonitor GetCollectionInfoProvider returned err: %v", e)
					continue
				}

				scopeKey := fmt.Sprintf("%v:%v", defn.Bucket, defn.Scope)
				collectionKey := fmt.Sprintf("%v:%v:%v", defn.Bucket, defn.Scope, defn.Collection)

				var bucketUuid, scopeId, collectionId string
				var ok bool
				if bucketUuid, ok = buckets[defn.Bucket]; !ok {
					cont := func() bool {
						m.cinfoProviderLock.Lock()
						bucketUuid, err = m.cinfoProvider.GetBucketUUID(defn.Bucket)
						m.cinfoProviderLock.Unlock()
						if err != nil {
							logging.Errorf("schedIndexCreator:keyspaceMonitor GetBucketUUID returned err: %v", e)
							return true
						}
						buckets[defn.Bucket] = bucketUuid
						return false
					}()
					if cont {
						continue
					}
				}

				if scopeId, ok = scopes[scopeKey]; !ok {
					func() {
						scopeId = collnInfo.ScopeID(defn.Bucket, defn.Scope)
						scopes[scopeKey] = scopeId
					}()
				}

				if collectionId, ok = collections[collectionKey]; !ok {
					func() {
						collectionId = collnInfo.CollectionID(defn.Bucket, defn.Scope, defn.Collection)
						collections[collectionKey] = collectionId
					}()
				}

				if bucketUuid != schedToken.BucketUUID ||
					scopeId != schedToken.ScopeId ||
					collectionId != schedToken.CollectionId {

					m.cleanupTokens(defn.DefnId)
				}
			}

		case <-m.stopKeyspaceMon:
			logging.Infof("schedIndexCreator: Stopping keyspaceMonitor routine")
			return
		}
	}
}

func (m *schedIndexCreator) cleanupTokens(defnId common.IndexDefnId) {

	logging.Infof("schedIndexCreator:cleanupTokens cleaning tokens for %v", defnId)

	if err := mc.DeleteScheduleCreateToken(defnId); err != nil {
		logging.Errorf("schedIndexCreator:cleanupTokens error in DeleteScheduleCreateToken:%v:%v", defnId, err)
		return
	}

	if err := mc.DeleteStopScheduleCreateToken(defnId); err != nil {
		logging.Errorf("schedIndexCreator:cleanupTokens error in DeleteStopScheduleCreateToken:%v:%v", defnId, err)
	}
}

func (m *schedIndexCreator) getCinfoNoLock() common.ClusterInfoProvider {
	cfg := m.config.Load()
	clusterURL := cfg["clusterAddr"].String()
	useCinfolite := cfg["use_cinfo_lite"].Bool()

	cip, err := common.NewClusterInfoProvider(useCinfolite, clusterURL,
		common.DEFAULT_POOL, "schedIndexCreator", cfg)
	if err != nil {
		logging.Warnf("schedIndexCreator:getCinfoNoLock Unable to get new ClusterInfoProvider err: %v use_cinfo_lite: %v",
			err, useCinfolite)
		return nil
	}

	if cip == nil {
		logging.Warnf("schedIndexCreator:getCinfoNoLock nil cluster info client")
		return nil
	}

	return cip
}

func (m *schedIndexCreator) Close() {
	logging.Infof("schedIndexCreator: Shutting Down ...")
	close(m.killch)
	close(m.stopCleaner)
	close(m.stopMover)
	close(m.stopKeyspaceMon)
	m.mon.Close()
}

/////////////////////////////////////////////////////////////////////
// Constructor and member functions for scheduledIndex
/////////////////////////////////////////////////////////////////////

func NewScheduledIndex(token *mc.ScheduleCreateToken) *scheduledIndex {
	var priority int
	if useSecondsFromCtime {
		// get ctime at the granularity of a second
		seconds := token.Ctime / int64(1000*1000*1000)
		priority = int(seconds)
	} else {
		priority = int(token.Ctime)
	}

	return &scheduledIndex{
		token:    token,
		state:    &scheduledIndexState{},
		priority: priority,
	}
}

/////////////////////////////////////////////////////////////////////
// Constructor and member functions for schedTokenMonitor
/////////////////////////////////////////////////////////////////////

func NewSchedTokenMonitor(creator *schedIndexCreator, indexerId common.IndexerId) *schedTokenMonitor {

	lCloseCh := make(chan bool)
	listener := mc.NewCommandListener(lCloseCh, false, false, false, false, true, false)

	s := &schedTokenMonitor{
		creator:         creator,
		commandListener: listener,
		listenerDonech:  lCloseCh,
		processed:       make(map[string]bool),
		uCloseCh:        make(chan bool),
		indexerId:       indexerId,
	}

	go s.updater()

	return s
}

func (s *schedTokenMonitor) checkProcessed(key string) bool {

	if _, ok := s.processed[key]; ok {
		return true
	}

	return false
}

func (s *schedTokenMonitor) markProcessed(key string) {

	s.processed[key] = true
}

func (s *schedTokenMonitor) update() {
	createTokens := s.commandListener.GetNewScheduleCreateTokens()
	for key, token := range createTokens {
		if s.checkProcessed(key) {
			continue
		}

		if token.IndexerId != s.indexerId {
			continue
		}

		exists, err := mc.StopScheduleCreateTokenExist(token.Definition.DefnId)
		if err != nil {
			logging.Errorf("schedIndexCreator: Error in getting stop schedule create token for %v", token.Definition.DefnId)
			continue
		}

		if exists {
			logging.Debugf("schedIndexCreator: stop schedule token exists for %v", token.Definition.DefnId)
			continue
		}

		s.creator.pushQ(NewScheduledIndex(token))

		s.markProcessed(key)
	}
}

func (s *schedTokenMonitor) updater() {
	// Sleep for some time before starting.
	time.Sleep(time.Duration(SCHED_PROCESS_INIT_INTERVAL) * time.Second)

	s.commandListener.ListenTokens()

	ticker := time.NewTicker(time.Duration(SCHED_TOKEN_CHECK_INTERVAL) * time.Millisecond)

	for {
		select {
		case <-ticker.C:
			s.update()

		case <-s.uCloseCh:
			return
		}
	}
}

func (s *schedTokenMonitor) Close() {
	s.commandListener.Close()
	close(s.uCloseCh)
}

/////////////////////////////////////////////////////////////////////
// Member functions (heap implementation) for schedIndexQueue
// As the schedIndexQueue is implemented as a container/heap,
// following methods are the exposed methods and will be called
// from the golang container/heap library. Please don't call
// these methods directly.
/////////////////////////////////////////////////////////////////////

func (q schedIndexQueue) Len() int {
	return len(q)
}

func (q schedIndexQueue) Less(i, j int) bool {
	return q[i].priority < q[j].priority
}

func (q schedIndexQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].index = i
	q[j].index = j
}

func (q *schedIndexQueue) Push(x interface{}) {
	n := len(*q)
	item := x.(*scheduledIndex)
	item.index = n
	*q = append(*q, item)
}

func (q *schedIndexQueue) Pop() interface{} {
	old := *q
	n := len(old)
	if n == 0 {
		return nil
	}

	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*q = old[0 : n-1]
	return item
}
