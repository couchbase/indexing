package indexer

import (
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/manager/client"
	mc "github.com/couchbase/indexing/secondary/manager/common"

	"container/heap"
	"errors"
	"sync"
	"time"
	"unsafe"
)

var SCHED_TOKEN_CHECK_INTERVAL = 5000 // Milliseconds

/////////////////////////////////////////////////////////////////////
// Global Variables
/////////////////////////////////////////////////////////////////////

var gSchedIndexCreator *schedIndexCreator
var gSchedIndexCreatorLck sync.Mutex
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

	gSchedIndexCreatorLck.Lock()
	defer gSchedIndexCreatorLck.Unlock()

	return gSchedIndexCreator
}

/////////////////////////////////////////////////////////////////////
// Data Types
/////////////////////////////////////////////////////////////////////

//
// Scheduled index creator (schedIndexCreator), checks up on the scheduled
// create tokens and tries to create index with the help of metadata provider.
// 1. If the index creation succeeds, corresponding stop schedule create token
//    is posted and scheduled create token is deleted.
// 2. If the index creation fails, the index creation will be retried, based
//    on the reason for failure.
// 3. The retry count is tracked in memory and after the retry limit is reached
//    the stop schedule create token will be posted with last error.
// 4. Similar to DDL service manager, scheduled index creator observes mutual
//    exclusion with rebalance operation.
//
type schedIndexCreator struct {
	indexerId   common.IndexerId
	config      common.ConfigHolder
	provider    *client.MetadataProvider
	proMutex    sync.Mutex
	supvCmdch   MsgChannel //supervisor sends commands on this channel
	supvMsgch   MsgChannel //channel to send any message to supervisor
	clusterAddr string
	settings    *ddlSettings
	killch      chan bool
	allowDDL    bool
	mutex       sync.Mutex
	mon         *schedTokenMonitor
	indexQueue  *schedIndexQueue
	queueMutex  sync.Mutex
	backoff     int64 // random backoff in nanoseconds before the next request.
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

/////////////////////////////////////////////////////////////////////
// Constructor and member functions for schedIndexCreator
/////////////////////////////////////////////////////////////////////

func NewSchedIndexCreator(indexerId common.IndexerId, supvCmdch MsgChannel,
	supvMsgch MsgChannel, config common.Config) (*schedIndexCreator, Message) {

	// TODO: Make indexer start sched index creator
	addr := config["clusterAddr"].String()
	numReplica := int32(config["settings.num_replica"].Int())
	settings := &ddlSettings{numReplica: numReplica}

	iq := make(schedIndexQueue, 0)
	heap.Init(&iq)

	mgr := &schedIndexCreator{
		indexerId:   indexerId,
		supvCmdch:   supvCmdch,
		supvMsgch:   supvMsgch,
		clusterAddr: addr,
		settings:    settings,
		killch:      make(chan bool),
		allowDDL:    true,
		indexQueue:  &iq,
	}

	mgr.mon = NewSchedTokenMonitor(mgr, indexerId)

	mgr.config.Store(config)

	go mgr.run()

	gSchedIndexCreatorLck.Lock()
	defer gSchedIndexCreatorLck.Unlock()
	gSchedIndexCreator = mgr

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
					logging.Infof("schedIndexCreator: Shutting Down ...")
					close(m.killch)
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

	// TODO: Make indexer send this message
	case CONFIG_SETTINGS_UPDATE:
		cfgUpdate := cmd.(*MsgConfigUpdate)
		m.config.Store(cfgUpdate.GetConfig())
		m.settings.handleSettings(cfgUpdate.GetConfig())
		m.supvCmdch <- &MsgSuccess{}

	default:
		logging.Fatalf("schedIndexCreator::handleSupervisorCommands Unknown Message %+v", cmd)
		common.CrashOnError(errors.New("Unknown Msg On Supv Channel"))
	}
}

func (m *schedIndexCreator) stopProcessDDL() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.allowDDL = false
}

func (m *schedIndexCreator) canProcessDDL() bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.allowDDL
}

func (m *schedIndexCreator) startProcessDDL() {
	func() {
		m.mutex.Lock()
		defer m.mutex.Unlock()
		m.allowDDL = true
	}()

	func() {
		m.proMutex.Lock()
		defer m.proMutex.Unlock()
		m.provider = nil
	}()
}

func (m *schedIndexCreator) rebalanceDone() {
	// TODO: Need to check if provider needs to be reset.
}

// TODO: Integrate with rebalance start / finish

func (m *schedIndexCreator) processSchedIndexes() {

	// Check for new indexes to be created every 5 seconds
	ticker := time.NewTicker(time.Duration(SCHED_TOKEN_CHECK_INTERVAL) * time.Millisecond)

	for {
		select {
		case <-ticker.C:
			if !m.canProcessDDL() {
				continue
			}

		innerLoop:
			for {
				if !m.canProcessDDL() {
					break innerLoop
				}

				index := m.popQ()
				if index == nil {
					break innerLoop
				}

				logging.Infof("schedIndexCreator: Trying to create index %v", index.token.Definition.DefnId)
				err := m.tryCreateIndex(index)
				if err != nil {
					retry := m.handleError(index, err)
					if retry {
						logging.Errorf("schedIndexCreator: error(%v) while creating index %v. The operation will be retried. Current retry counts (%v,%v)",
							err, index.token.Definition.DefnId, index.state.retryCount, index.state.nRetryCount)
						m.pushQ(index)
					} else {
						// TODO: Check if we need a console log
						logging.Errorf("schedIndexCreator: error(%v) while creating index %v. The operation failed after retry counts (%v,%v)",
							err, index.token.Definition.DefnId, index.state.retryCount, index.state.nRetryCount)
						err := mc.PostStopScheduleCreateToken(index.token.Definition.DefnId, err.Error(), time.Now().UnixNano())
						if err != nil {
							logging.Errorf("schedIndexCreator: error (%v) in postnig the stop schedule create token for %v",
								err, index.token.Definition.DefnId)
						}
					}
				} else {
					logging.Infof("schedIndexCreator: successfully created index %v", index.token.Definition.DefnId)
					// TODO: Check if this doesn't error out in case of key not found.
					err := mc.DeleteScheduleCreateToken(index.token.Definition.DefnId)
					if err != nil {
						logging.Errorf("schedIndexCreator: error (%v) in deleting the schedule create token for %v",
							err, index.token.Definition.DefnId)
					}
				}
			}

		case <-m.killch:
			logging.Infof("schedIndexCreator: Stopping processSchedIndexes routine ...")
			return
		}
	}
}

func (m *schedIndexCreator) handleError(index *scheduledIndex, err error) bool {
	// TODO: Handle following scnearios.
	// 1. Network errors - esp in newMetadataProvider
	// 2. Ongoing DDL
	// 3. Duplicate index
	// 4. Unknown Errors
	// Set backoff based on the error

	return false
}

func (m *schedIndexCreator) getMetadataProvider() (*client.MetadataProvider, error) {

	m.proMutex.Lock()
	defer m.proMutex.Unlock()

	if m.provider == nil {
		provider, _, err := newMetadataProvider(m.clusterAddr, nil, m.settings, "schedIndexCreator")
		if err != nil {
			return nil, err
		}

		m.provider = provider
	}

	// TODO: need to refresh provider on Reabalance.

	return m.provider, nil
}

func (m *schedIndexCreator) tryCreateIndex(index *scheduledIndex) error {
	exists, err := mc.StopScheduleCreateTokenExist(index.token.Definition.DefnId)
	if err != nil {
		logging.Errorf("schedIndexCreator:tryCreateIndex Error in getting stop schedule create token for %v",
			index.token.Definition.DefnId)
		return err
	}

	if exists {
		logging.Debugf("schedIndexCreator:tryCreateIndex stop schedule token exists for %v",
			index.token.Definition.DefnId)
		return nil
	}

	if m.backoff > 0 {
		logging.Debugf("schedIndexCreator:tryCreateIndex using %v backoff for index %v",
			m.backoff, index.token.Definition.DefnId)
		time.Sleep(time.Duration(m.backoff))
	}

	var provider *client.MetadataProvider
	provider, err = m.getMetadataProvider()
	if err != nil {
		logging.Errorf("schedIndexCreator:tryCreateIndex Error in getting getMetadataProvider for %v",
			index.token.Definition.DefnId)
		return err
	}

	// TODO: Check if index already exists in metadata provider. If yes, then the
	// index was successfully created earlier, but the token deletion may
	// have failed.

	return provider.CreateIndexWithDefnAndPlan(&index.token.Definition, index.token.Plan)
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

	// TODO: This can be optimised for the first call by sending a batch of tokens.
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
