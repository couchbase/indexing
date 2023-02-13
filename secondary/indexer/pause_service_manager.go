// @copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
)

////////////////////////////////////////////////////////////////////////////////////////////////////
// PauseServiceManager class
////////////////////////////////////////////////////////////////////////////////////////////////////

// PauseMgr points to the singleton of this class, which will still be nil early in user
// ScanCoordinator's lifecycle, hence the need for atomics. It is really type *PauseServiceManager.
var PauseMgr unsafe.Pointer

// PauseServiceManager provides the implementation of the Pause-Resume-specific APIs of
// ns_server RPC Manager interface (defined in cbauth/service/interface.go).
type PauseServiceManager struct {
	genericMgr *GenericServiceManager // pointer to our parent
	httpAddr   string                 // local host:port for HTTP: "127.0.0.1:9102", 9108, ...

	config common.ConfigHolder

	// bucketStates is a map from bucket to its Pause-Resume state. The design supports concurrent
	// pauses and resumes of different buckets, although this may not be done in practice.
	bucketStates map[string]bucketStateEnum

	// bucketStatesMu protects bucketStates
	bucketStatesMu sync.RWMutex

	// tasks is the set of Pause-Resume tasks that are running, if any, mapped by taskId
	tasks map[string]*taskObj

	// tasksMu protects tasks
	tasksMu sync.RWMutex

	supvMsgch MsgChannel //channel to send msg to supervisor for normal handling (idx.wrkrRecvCh)
	supvCmdch MsgChannel //channel to receive command msg from supervisor (idx.prMgrCmdCh)

	// Track global PauseTokens
	pauseTokensById map[string]*PauseToken
	pauseTokenMapMu sync.RWMutex

	// Track Pausers
	pausersById  map[string]*Pauser
	pausersMapMu sync.Mutex

	// Track Resumers
	resumersById  map[string]*Resumer
	resumersMapMu sync.Mutex

	nodeInfo *service.NodeInfo
}

// NewPauseServiceManager is the constructor for the PauseServiceManager class.
// GenericServiceManager constructs a singleton at boot.
//
//	genericMgr - pointer to our parent
//	mux - Indexer's HTTP server
//	httpAddr - host:port of the local node for Index Service HTTP calls
func NewPauseServiceManager(genericMgr *GenericServiceManager, mux *http.ServeMux, supvCmdch,
	supvMsgch MsgChannel, httpAddr string, config common.Config, nodeInfo *service.NodeInfo,
) *PauseServiceManager {

	m := &PauseServiceManager{
		genericMgr: genericMgr,
		httpAddr:   httpAddr,

		bucketStates: make(map[string]bucketStateEnum),
		tasks:        make(map[string]*taskObj),

		supvMsgch: supvMsgch,
		supvCmdch: supvCmdch,

		pauseTokensById: make(map[string]*PauseToken),
		pausersById:     make(map[string]*Pauser),
		resumersById:    make(map[string]*Resumer),

		nodeInfo: nodeInfo,
	}
	m.config.Store(config)

	// Save the singleton
	SetPauseMgr(m)

	// Internal REST APIs
	mux.HandleFunc("/pauseMgr/FailedTask", m.RestHandleFailedTask)
	mux.HandleFunc("/pauseMgr/Pause", m.RestHandlePause)

	// Unit test REST APIs -- THESE MUST STILL DO AUTHENTICATION!!
	mux.HandleFunc("/test/Pause", m.testPause)
	mux.HandleFunc("/test/PreparePause", m.testPreparePause)
	mux.HandleFunc("/test/PrepareResume", m.testPrepareResume)
	mux.HandleFunc("/test/Resume", m.testResume)

	go m.run()

	return m
}

// Track Pauser based on pauseId. Pauser can be deleted by calling with nil Pauser. If there is already a Pauser with
// the pauseId, then an error is returned.
func (m *PauseServiceManager) setPauser(pauseId string, p *Pauser) error {
	m.pausersMapMu.Lock()
	defer m.pausersMapMu.Unlock()

	if oldPauser, ok := m.pausersById[pauseId]; ok {

		if p == nil {
			delete(m.pausersById, pauseId)
		} else {
			return fmt.Errorf("conflict: Pauser[%v] with pauseId[%v] already present!", oldPauser, pauseId)
		}

	} else {
		m.pausersById[pauseId] = p
	}

	return nil
}

// Get pauser based on pauseId. If there is no such Pauser, then the returned boolean will be false.
func (m *PauseServiceManager) getPauser(pauseId string) (*Pauser, bool) {
	m.pausersMapMu.Lock()
	defer m.pausersMapMu.Unlock()

	pauser, exists := m.pausersById[pauseId]

	return pauser, exists
}

// Track Resumer based on resumeId. Resumer can be deleted by calling with nil Resumer. If there is already a Resumer
// with the resumeId, then an error is returned.
func (m *PauseServiceManager) setResumer(resumeId string, r *Resumer) error {
	m.resumersMapMu.Lock()
	defer m.resumersMapMu.Unlock()

	if oldResumer, ok := m.resumersById[resumeId]; ok {

		if r == nil {
			delete(m.resumersById, resumeId)
		} else {
			return fmt.Errorf("conflict: Resumer[%v] with resumeId[%v] already present!", oldResumer, resumeId)
		}

	} else {
		m.resumersById[resumeId] = r
	}

	return nil
}

// Get Resumer based on resumeId. If there is no such Resumer, then the returned boolean will be false.
func (m *PauseServiceManager) getResumer(resumeId string) (*Resumer, bool) {
	m.resumersMapMu.Lock()
	defer m.resumersMapMu.Unlock()

	resumer, exists := m.resumersById[resumeId]

	return resumer, exists
}

// GetPauseMgr atomically gets the global PauseMgr pointer. When called from ScanCoordinator it may
// still be nil as ScanCoordinator is constructed long before PauseServiceManager.
func GetPauseMgr() *PauseServiceManager {
	return (*PauseServiceManager)(atomic.LoadPointer(&PauseMgr))
}

// SetPauseMgr atomically sets the global PauseMgr pointer. This is done only once, when the
// PauseServiceManager singleton is constructed.
func SetPauseMgr(pauseMgr *PauseServiceManager) {
	atomic.StorePointer(&PauseMgr, unsafe.Pointer(pauseMgr))
}

// run is a blocking thread that runs forever and listens for commands from
// parent to update internal operations
func (psm *PauseServiceManager) run() {
	for {
		select {
		case cmd, ok := <-psm.supvCmdch:
			if ok {
				if cmd.GetMsgType() == ADMIN_MGR_SHUTDOWN {
					logging.Infof("PauseServiceManager::listenForCommands Shutting Down")
					psm.supvCmdch <- &MsgSuccess{}
					return
				}
				psm.handleSupervisorCommands(cmd)
			} else {
				//supervisor channel closed. exit
				return
			}
		}
	}
}

func (psm *PauseServiceManager) handleSupervisorCommands(cmd Message) {
	switch cmd.GetMsgType() {

	case CONFIG_SETTINGS_UPDATE:
		psm.handleConfigUpdate(cmd)

	case CLUST_MGR_INDEXER_READY:
		psm.handleIndexerReady(cmd)

	default:
		logging.Fatalf("PauseServiceManager::handleSupervisorCommands Unknown Message %+v", cmd)
		common.CrashOnError(errors.New("Unknown Msg On Supv Channel"))
	}
}

func (psm *PauseServiceManager) handleConfigUpdate(cmd Message) {
	cfgUpdate := cmd.(*MsgConfigUpdate)
	psm.config.Store(cfgUpdate.GetConfig())
	psm.supvCmdch <- &MsgSuccess{}
}

func (psm *PauseServiceManager) handleIndexerReady(cmd Message) {
	psm.supvCmdch <- &MsgSuccess{}

	go psm.recoverFromCrash()
}

func (psm *PauseServiceManager) recoverFromCrash() {
	// TODO: add recovery logic here
	logging.Infof("PauseServiceManager::recoverFromCrash: crash recovery called on Pause-Resume service manager")
}

func (psm *PauseServiceManager) lockShards(shardIds []common.ShardId) error {
	respCh := make(chan map[common.ShardId]error)

	msg := &MsgLockUnlockShards{
		mType:    LOCK_SHARDS,
		shardIds: shardIds,
		respCh:   respCh,
	}

	psm.supvMsgch <- msg

	errMap := <-respCh
	errBuilder := new(strings.Builder)
	for shardId, err := range errMap {
		if err != nil {
			errBuilder.WriteString(fmt.Sprintf("Failed to lock shard %v err: %v", shardId, err))
		}
	}
	if errBuilder.Len() == 0 {
		return nil
	}

	return errors.New(errBuilder.String())
}

func (psm *PauseServiceManager) unlockShards(shardIds []common.ShardId) error {
	respCh := make(chan map[common.ShardId]error)

	msg := &MsgLockUnlockShards{
		mType:    UNLOCK_SHARDS,
		shardIds: shardIds,
		respCh:   respCh,
	}

	psm.supvMsgch <- msg

	errMap := <-respCh
	errBuilder := new(strings.Builder)
	for shardId, err := range errMap {
		if err != nil {
			errBuilder.WriteString(fmt.Sprintf("Failed to unlock shard %v err: %v", shardId, err))
		}
	}
	if errBuilder.Len() == 0 {
		return nil
	}

	return errors.New(errBuilder.String())
}

func (psm *PauseServiceManager) copyShardsWithLock(shardIds []common.ShardId, taskId, bucket, destination string, cancelCh <-chan struct{}) (map[common.ShardId]string, error) {
	err := psm.lockShards(shardIds)
	if err != nil {
		logging.Errorf("PauseServiceManager::copyShardsWithLock: locking shards failed. err -> %v for taskId %v", err, taskId)
		return nil, err
	}
	defer func() {
		err := psm.unlockShards(shardIds)
		if err != nil {
			logging.Errorf("PauseServiceManager::copyShardsWithLock: unlocking shards failed. err -> %v for taskId %v", err, taskId)
		}
	}()
	logging.Infof("PauseServiceManager::copyShardsWithLock: locked shards with id %v for taskId: %v", shardIds, taskId)

	respCh := make(chan Message)

	msg := &MsgStartShardTransfer{
		shardIds:    shardIds,
		transferId:  bucket,
		destination: destination,
		taskType:    common.PauseResumeTask,
		taskId:      taskId,
		respCh:      respCh,
		doneCh:      cancelCh, // abort transfer if msg received on done
		cancelCh:    cancelCh,
		progressCh:  nil, // TODO: handle progress reporting
	}

	psm.supvMsgch <- msg

	resp, ok := (<-respCh).(*MsgShardTransferResp)

	if !ok || resp == nil {
		// We skip unlocking of shards here. it should get called when pause gets rolled back
		err = fmt.Errorf("either response channel got closed or sent an invalid response")
		logging.Errorf("PauseServiceManager::copyShardsWithLock: %v for taskId %v", err, taskId)
		return nil, err
	}
	errMap := resp.GetErrorMap()
	errBuilder := new(strings.Builder)
	for shardId, errInCopy := range errMap {
		if errInCopy != nil {
			errBuilder.WriteString(fmt.Sprintf("Failed to copy shard %v, err: %v", shardId, errInCopy))
		}
	}
	if errBuilder.Len() != 0 {
		err = errors.New(errBuilder.String())
		logging.Errorf("PauseServiceManager::copyShardsWithLock: %v for taskId: %v", err, taskId)
		return nil, err
	}

	return resp.GetShardPaths(), nil
}

func (psm *PauseServiceManager) downloadShardsWithoutLock(
	shardPaths map[common.ShardId]string,
	taskId, bucket, origin, region string,
	cancelCh <-chan struct{},
) (map[common.ShardId]string, error) {

	logging.Infof("PauseServiceManager::downloadShardsWithoutLock: downloading shards %v from %v for taskId %v", shardPaths, taskId)
	respCh := make(chan Message)
	msg := &MsgStartShardRestore{
		taskType:    common.PauseResumeTask,
		taskId:      taskId,
		transferId:  bucket,
		shardPaths:  shardPaths,
		destination: origin,
		region:      region,
		cancelCh:    cancelCh,
		doneCh:      cancelCh,
		progressCh:  nil,
		respCh:      respCh,
	}

	psm.supvMsgch <- msg
	resp, ok := (<-respCh).(*MsgShardTransferResp)

	if !ok || resp == nil {
		err := fmt.Errorf("either response channel got closed or sent an invalid response")
		logging.Errorf("PauseServiceManager::downloadShardsWithLock: %v for taskId %v", err, taskId)
		return nil, err
	}
	errMap := resp.GetErrorMap()
	errBuilder := new(strings.Builder)
	for shardId, errInCopy := range errMap {
		if errInCopy != nil {
			errBuilder.WriteString(fmt.Sprintf("Failed to download shard %v, err: %v", shardId, errInCopy))
		}
	}
	if errBuilder.Len() != 0 {
		err := errors.New(errBuilder.String())
		logging.Errorf("PauseServiceManager::downloadShardsWithLock: %v for taskId %v", err, taskId)
		return nil, err
	}
	return resp.GetShardPaths(), nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Constants
////////////////////////////////////////////////////////////////////////////////////////////////////

// ARCHIVE_VERSION is the version of the archive format used for Pause and Resume. Later versions
// are expected to understand earlier versions but not vice versa. This version needs to be
// increased when a breaking change is made to anything that gets persisted in the archive.
//
// For future-proofing, the initial version is 100, and we expect to increment by 100. This allows
// us to easily insert new versions in the middle of the history, which could happen e.g. if a prior
// release branch diverges from the main branch due to a bug fix.
const ARCHIVE_VERSION int = 100 // increment by +100 -- see above

// FILENAME_METADATA is the name of the file to write containing the index metadata from ONE node.
const FILENAME_METADATA = "indexMetadata.json"

// FILENAME_STATS is the name of the file to write containing persisted index stats from ONE node.
const FILENAME_STATS = "indexStats.json"

// FILENAME_PAUSE_METADATA is the name of the file to write containing the PauseMetadata.
const FILENAME_PAUSE_METADATA = "pauseMetadata.json"

////////////////////////////////////////////////////////////////////////////////////////////////////
// Enums
////////////////////////////////////////////////////////////////////////////////////////////////////

// type bucketStateEnum defines the possible Pause-Resume states of a bucket (following constants).
// bucketStates map holds these. If a bucket is not in the map, it is not pausing or resuming. This
// is equivalent to state bst_NIL.
type bucketStateEnum int

const (
	// bst_NIL is the zero-value state, indicating a bucket has no Pause-Resume state.
	bst_NIL bucketStateEnum = iota

	// bst_PREPARE_PAUSE is entered when ns_server calls PreparePause. Nothing is happening yet,
	// but the state must be tracked to verify legality of state transitions.
	bst_PREPARE_PAUSE

	// bst_PAUSING state is entered when ns_server calls Pause (after PreparePause already
	// succeeded). It means Pause actions such as rejecting scans and closing DCP streams for the
	// bucket have begun, and thus canceling the Pause requires reviving these. Pause-related tasks
	// still exist.
	bst_PAUSING

	// bst_PAUSED state is entered once all Pause work is completed and Pause-related tasks have
	// been removed, but ns_server has not yet dropped the bucket. During this period ns_server may
	// decide to "cancel" the Pause, but since the tasks are gone this can't happen via CancelTask.
	// Instead we must monitor the /pools/default/buckets API for buckets in this state and revive
	// such a bucket if it gets marked as active again. This revival is not a Resume from S3 but
	// rather the same as reviving from a CancelTask during bst_PAUSING state, i.e. just reopen DCP
	// connections and stop blocking scans. Pause-related tasks do NOT exist. This state ends when
	// either the bucket is dropped or marked active.
	bst_PAUSED

	// bst_PREPARE_RESUME is entered when ns_server calls PrepareResume. Nothing is happening yet,
	// but the state must be tracked to verify legality of state transitions.
	bst_PREPARE_RESUME

	// bst_RESUMING state is entered when ns_server calls Resume (after PrepareResume already
	// succeeded). Resume work is ongoing. Resume-related tasks still exist.
	bst_RESUMING

	// bst_RESUMED state is entered once all Resume work is completed and Resume-related tasks have
	// been removed, but ns_server has not yet marked the bucket active. During this period
	// ns_server may decide to "cancel" the Resume, but since the tasks are gone this can't happen
	// via CancelTask. Instead we must monitor the /pools/default/buckets API for buckets in this
	// state and only open such a bucket for business if it gets marked as active. Resume-related
	// tasks do NOT exist. This state ends when either the bucket is marked active or is dropped.
	bst_RESUMED
)

// String converter for bucketStateEnum type.
func (this bucketStateEnum) String() string {
	switch this {
	case bst_NIL:
		return "NilBucketState"
	case bst_PREPARE_PAUSE:
		return "PreparePause"
	case bst_PAUSING:
		return "Pausing"
	case bst_PAUSED:
		return "Paused"
	case bst_PREPARE_RESUME:
		return "PrepareResume"
	case bst_RESUMING:
		return "Resuming"
	case bst_RESUMED:
		return "Resumed"
	default:
		return fmt.Sprintf("undefinedBucketStateEnum_%v", int(this))
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Type definitions
////////////////////////////////////////////////////////////////////////////////////////////////////

// this information should be considered read only during resume
// created by pause leader node
type PauseMetadata struct {
	// nodeId gives node info
	// map[nodeId]->shardIds gives information about shardIds copied from node
	// map[shardId] -> string are obj store paths where each shard is saved
	Data map[service.NodeID]map[common.ShardId]string `json:"metadata"`

	// cluster Version during data creation
	Version string `json:"version"`

	lock *sync.RWMutex
}

func NewPauseMetadata() *PauseMetadata {
	return &PauseMetadata{
		// size value should be max nodes in a subcluster
		Data: make(map[service.NodeID]map[common.ShardId]string, 2),
		lock: &sync.RWMutex{},
	}
}

func (pm *PauseMetadata) setVersionNoLock(ver string) {
	pm.Version = ver
}

func (pm *PauseMetadata) addShardPathNoLock(nodeId service.NodeID, shardId common.ShardId,
	shardPath string) {
	_, ok := pm.Data[nodeId]
	if !ok {
		// size value should be no of shards per bucket
		pm.Data[nodeId] = make(map[common.ShardId]string, 2)
	}
	pm.Data[nodeId][shardId] = shardPath
}

func (pm *PauseMetadata) addShardPaths(nodeId service.NodeID, shardPaths map[common.ShardId]string) {
	pm.lock.Lock()
	defer pm.lock.Unlock()
	pm.Data[nodeId] = shardPaths
}

// taskObj represents one task of any type in the taskType enum below. This is the GSI internal
// representation, not the GetTaskList return format.
//
// For GetTaskList response to ns_server, we convert a taskObj to a service.Task struct (shared
// with Rebalance).
type taskObj struct {
	taskMu *sync.RWMutex // protects this taskObj; pointer as taskObj may be cloned

	taskType     service.TaskType
	taskId       string             // opaque ns_server unique ID for this task
	taskStatus   service.TaskStatus // local analog of service.TaskStatus
	progress     float64            // completion progress [0.0, 1.0]
	errorMessage string             // only if a failure occurred

	bucket string // bucket name being Paused or Resumed
	// bucketUuid string // Pause: UUID of bucket; Resume: ""
	dryRun bool // is task for Resume dry run (cluster capacity check)?

	// ns_server does not differentiate between PreparePause and PrepareResume
	// this flag is to internally track if the request if for PreparePause/PrepareResume
	// when the taskType = service.TaskTypePrepared
	isPause bool

	// archivePath is path to top-level "directory" to use to write/read Pause/Resume images. For:
	//   S3 format:       "s3://<s3_bucket>/index/"
	//   Local FS format: "/absolute/path/" or "relative/path/"
	// The trailing slash is always present. For FS the original "file://" prefix has been removed.
	archivePath string
	archiveType ArchiveEnum // type of storage archive used for this task

	// master gets set to true on the master node for this task once ns_server calls Pause or
	// Resume, which it only does on the master node. At PreparePause and PrepareResume time we do
	// not yet know what node will be master.
	master bool

	// pauser is the async object executing this task iff it is a task_PAUSE, else nil
	pauser *Pauser

	// resumer is the async object executing this task iff it is a task_RESUME, else nil
	resumer *Resumer

	// ctx holds the context object that can be used for task context
	ctx context.Context
	// other packages are not supposed to call this. use Cancel/cancelNoLock
	// cancelFunc should be called only to cancel any ongoing observers and uploads
	// it is does not modify the task status or any other task related operations
	cancelFunc context.CancelFunc

	// PauseMetadata stores the metadata about the pause
	// this is only created and saved by the master
	pauseMetadata *PauseMetadata
}

// NewTaskObj is the constructor for the taskObj class. If the parameters are not valid, it will
// return (nil, error) rather than create an unsupported taskObj.
func NewTaskObj(taskType service.TaskType, taskId, bucket, remotePath string, isPause, dryRun bool,
) (*taskObj, error) {

	archiveType, archivePath, err := ArchiveInfoFromRemotePath(remotePath)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &taskObj{
		taskMu:      &sync.RWMutex{},
		taskId:      taskId,
		taskType:    taskType,
		taskStatus:  service.TaskStatusRunning,
		isPause:     isPause,
		bucket:      bucket,
		dryRun:      dryRun,
		archivePath: archivePath,
		archiveType: archiveType,
		ctx:         ctx,
		cancelFunc:  cancel,
	}, nil
}

// TaskObjSetFailed sets task.status to service.TaskStatusFailed and task.errMessage to errMsg.
// this applies lock and internally calls TaskObjSetFailedNoLock
func (this *taskObj) TaskObjSetFailed(errMsg string) {
	this.taskMu.Lock()
	this.TaskObjSetFailedNoLock(errMsg)
	this.taskMu.Unlock()
}

// TaskObjSetFailedNoLock sets the status to service.TaskStatusFailed and errorMessage to errMsg
// callers should hold the lock
func (t *taskObj) TaskObjSetFailedNoLock(errMsg string) {
	t.errorMessage = errMsg
	t.taskStatus = service.TaskStatusFailed
}

func (this *taskObj) setMasterNoLock() {
	this.master = true
	this.pauseMetadata = NewPauseMetadata()
}

func (this *taskObj) updateTaskTypeNoLock(newTaskType service.TaskType) {
	this.taskType = newTaskType
}

// taskObjToServiceTask creates the ns_server service.Task (cbauth/service/interface.go)
// representation of a taskObj.
func (this *taskObj) taskObjToServiceTask() []service.Task {
	this.taskMu.RLock()
	defer this.taskMu.RUnlock()

	tasks := make([]service.Task, 0, 2)

	nsTask := service.Task{
		Rev:          EncodeRev(0),
		ID:           this.taskId,
		Type:         this.taskType,
		Status:       this.taskStatus,
		IsCancelable: true,
		Progress:     this.progress,
		ErrorMessage: this.errorMessage,
		Extra:        make(map[string]interface{}),
	}

	// Add task parameters to service.Extra map for supportability (ns_server will ignore these)
	nsTask.Extra["bucket"] = this.bucket
	if this.hasDryRun() {
		nsTask.Extra["dryRun"] = this.dryRun
	}
	nsTask.Extra["archivePath"] = this.archivePath
	nsTask.Extra["archiveType"] = this.archiveType.String()
	nsTask.Extra["master"] = this.master

	if this.master {
		prepTask := service.Task{
			Rev:          EncodeRev(0),
			ID:           nsTask.ID,
			Type:         service.TaskTypePrepared,
			Status:       nsTask.Status,
			IsCancelable: true,
			Progress:     100,
			ErrorMessage: nsTask.ErrorMessage,
			Extra:        make(map[string]interface{}, len(nsTask.Extra)),
		}
		for key, value := range nsTask.Extra {
			prepTask.Extra[key] = value
		}
		tasks = append(tasks, prepTask)
	}
	tasks = append(tasks, nsTask)

	return tasks
}

// cancelNoLock stops any ongoing work by the task
// caller should hold lock. `Cancel` calls this function internally
func (this *taskObj) cancelNoLock() {
	if this.ctx == nil || this.cancelFunc == nil {
		logging.Warnf("taskObj::cancelNoLock: cancel called on already cancelled task %v", this.taskId)
		return
	}
	this.cancelFunc()
	this.ctx = nil
	this.cancelFunc = nil
}

// Cancel stops any ongoing work by the task
func (this *taskObj) Cancel() {
	this.taskMu.Lock()
	defer this.taskMu.Unlock()
	this.cancelNoLock()
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//
//  Implementation of Pause-Resume-specific APIs of the service.Manager interface
//  defined in cbauth/service/interface.go.
//
////////////////////////////////////////////////////////////////////////////////////////////////////

// PreparePause is an external API called by ns_server (via cbauth) on all index nodes before
// calling Pause on leader only.
//
// Phase 1 (dry run) does not exist for Pause.
//
// Phase 2. ns_server will poll progress on leader only via GetTaskList.
//
//  3. All: PreparePause - Create PreparePause tasks on all nodes.
//
//  4. Leader: Pause - Create Pause task on leader. Orchestrate Index pause.
//
//     On Pause success:
//     o Leader: Remove Pause and PreparePause tasks from ITSELF ONLY.
//     o Followers: ns_server will call CancelTask(PreparePause) on all followers, which then
//     remove their own PreparePause tasks.
//
//     On Pause failure:
//     o Leader: Change its own service.Task.Status to TaskStatusFailed and add failure info to
//     service.Task.ErrorMessage.
//     o All: ns_server will call CancelTask for Pause on leader and PreparePause on all nodes
//     and abort the Pause. Index nodes should remove their respective tasks.
//
// Method arguments
// params service.PauseParams - cbauth object providing
//   - ID: task ID
//   - Bucket: name of the bucket to be paused
//   - RemotePath: object store path
func (m *PauseServiceManager) PreparePause(params service.PauseParams) (err error) {
	const _PreparePause = "PauseServiceManager::PreparePause:"

	const args = "taskId: %v, bucket: %v, remotePath: %v"
	logging.Infof("%v Called. "+args, _PreparePause, params.ID, params.Bucket, params.RemotePath)
	defer logging.Infof("%v Returned %v. "+args, _PreparePause, err, params.ID, params.Bucket, params.RemotePath)

	// TODO: Check remotePath access?

	// Set bst_PREPARE_PAUSE state
	err = m.bucketStateSet(_PreparePause, params.Bucket, bst_NIL, bst_PREPARE_PAUSE)
	if err != nil {
		return err
	}

	// Record the task in progress
	return m.taskAddPrepare(params.ID, params.Bucket, params.RemotePath, true, false)
}

// Pause is an external API called by ns_server (via cbauth) only on the GSI master node to initiate
// a pause (formerly hibernate) of a bucket in the serverless offering.
//
//		params service.PauseParams - cbauth object providing
//	  - ID: task ID
//	  - Bucket: name of the bucket to be paused
//	  - RemotePath: object store path
func (m *PauseServiceManager) Pause(params service.PauseParams) (err error) {
	const _Pause = "PauseServiceManager::Pause:"

	const args = "taskId: %v, bucket: %v, remotePath: %v"
	logging.Infof("%v Called. "+args, _Pause, params.ID, params.Bucket, params.RemotePath)
	defer logging.Infof("%v Returned %v. "+args, _Pause, err, params.ID, params.Bucket, params.RemotePath)

	// Update the task to set this node as master
	task := m.taskSetMasterAndUpdateType(params.ID, service.TaskTypeBucketPause)
	if task == nil || !task.isPause {
		logging.Errorf("%v taskId %v (from PreparePause) not found", _Pause, params.ID)
		return service.ErrNotFound
	}

	// Set bst_PAUSING state
	err = m.bucketStateSet(_Pause, params.Bucket, bst_PREPARE_PAUSE, bst_PAUSING)
	if err != nil {
		return err
	}

	if err = m.initStartPhase(params.Bucket, params.ID, PauseTokenPause); err != nil {
		m.runPauseCleanupPhase(params.ID, task.isMaster())
		return err
	}

	// Create a Pauser object to run the master orchestration loop.

	pauser := NewPauser(m, task, m.pauseTokensById[params.ID], m.pauseDoneCallback)

	if err = m.setPauser(params.ID, pauser); err != nil {
		m.runPauseCleanupPhase(params.ID, task.isMaster())
		return err
	}

	pauser.startWorkers()

	return nil
}

func (m *PauseServiceManager) initStartPhase(bucketName, pauseId string, typ PauseTokenType) (err error) {

	err = m.genericMgr.cinfo.FetchNodesAndSvsInfoWithLock()
	if err != nil {
		logging.Errorf("PauseServiceManager::initStartPhase: Failed to fetch nodes data via cinfo: err[%v]",
			err)
		return err
	}

	var masterIP string
	masterIP, err = func() (string, error) {
		m.genericMgr.cinfo.RLock()
		defer m.genericMgr.cinfo.RUnlock()
		return m.genericMgr.cinfo.GetLocalHostname()
	}()
	if err != nil {
		logging.Errorf("PauseServiceManager::initStartPhase: Failed to get local host name via cinfo: err[%v]",
			err)
		return err
	}

	pauseToken := m.genPauseToken(masterIP, bucketName, pauseId, typ)
	logging.Infof("PauseServiceManager::initStartPhase Generated PauseToken[%v]", pauseToken)

	m.pauseTokenMapMu.Lock()
	m.pauseTokensById[pauseId] = pauseToken
	m.pauseTokenMapMu.Unlock()

	// Add to local metadata
	if err = m.registerLocalPauseToken(pauseToken); err != nil {
		return err
	}

	// Add to metaKV
	if err = m.registerPauseTokenInMetakv(pauseToken); err != nil {
		return err
	}

	// Register via /pauseMgr/Pause
	if err = m.registerGlobalPauseToken(pauseToken); err != nil {
		return err
	}

	return nil
}

// pauseDoneCallback is the Pauser.cb.done callback function.
// Upload work is interrupted based on pauseId, using cancel ctx from task in pauser.
func (m *PauseServiceManager) pauseDoneCallback(pauseId string, err error) {

	pauser, exists := m.getPauser(pauseId)
	if !exists {
		logging.Errorf("PauseServiceManager::pauseDoneCallback: Failed to find Pauser with pauseId[%v]", pauseId)
		return
	}

	// If there is an error, set it in the task, otherwise, delete task from task list.
	// TODO: Check if follower task should be handled differently.
	m.endTask(err, pauseId)

	isMaster := pauser.task.isMaster()

	if err := m.runPauseCleanupPhase(pauseId, isMaster); err != nil {
		logging.Errorf("PauseServiceManager::pauseDoneCallback: Failed to run cleanup: err[%v]", err)
		return
	}

	if err := m.setPauser(pauseId, nil); err != nil {
		logging.Errorf("PauseServiceManager::pauseDoneCallback: Failed to run cleanup: err[%v]", err)
		return
	}

	logging.Infof("PauseServiceManager::pauseDoneCallback Pause Done: isMaster %v, err: %v",
		isMaster, err)
}

func (m *PauseServiceManager) runPauseCleanupPhase(pauseId string, isMaster bool) error {

	logging.Infof("PauseServiceManager::runPauseCleanupPhase: pauseId[%v] isMaster[%v]", pauseId, isMaster)

	if isMaster {
		if err := m.cleanupPauseTokenInMetakv(pauseId); err != nil {
			logging.Errorf("PauseServiceManager::runPauseCleanupPhase: Failed to cleanup PauseToken in metkv:"+
				" err[%v]", err)
			return err
		}
	}

	_, puts, err := m.getCurrPauseTokens(pauseId)
	if err != nil {
		logging.Errorf("PauseServiceManager::runPauseCleanupPhase Error Fetching Metakv Tokens: err[%v]", err)
		return err
	}

	if len(puts) != 0 {
		if err := m.cleanupPauseUploadTokens(puts); err != nil {
			logging.Errorf("PauseServiceManager::runPauseCleanupPhase Error Cleaning Tokens: err[%v]", err)
			return err
		}
	}

	if err := m.cleanupLocalPauseToken(pauseId); err != nil {
		logging.Errorf("PauseServiceManager::runPauseCleanupPhase: Failed to cleanup PauseToken in local"+
			" meta: err[%v]", err)
		return err
	}

	return nil
}

func (m *PauseServiceManager) getCurrPauseTokens(pauseId string) (pt *PauseToken, puts map[string]*common.PauseUploadToken,
	err error) {

	metaInfo, err := metakv.ListAllChildren(PauseMetakvDir)
	if err != nil {
		return nil, nil, err
	}

	if len(metaInfo) == 0 {
		return nil, nil, nil
	}

	puts = make(map[string]*common.PauseUploadToken)

	for _, kv := range metaInfo {

		if strings.Contains(kv.Path, PauseTokenTag) {
			var mpt PauseToken
			if err = json.Unmarshal(kv.Value, &mpt); err != nil {
				return nil, nil, err
			}

			if mpt.PauseId == pauseId {
				if pt != nil {
					return nil, nil, fmt.Errorf("encountered duplicate PauseToken for pauseId[%v]"+
						" oldPT[%v] PT[%v]", mpt.PauseId, pt)
				}

				pt = &mpt
			}

		} else if strings.Contains(kv.Path, common.PauseUploadTokenTag) {
			putId, put, err := decodePauseUploadToken(kv.Path, kv.Value)
			if err != nil {
				return nil, nil, err
			}

			if put.PauseId == pauseId {
				if oldPUT, ok := puts[putId]; ok {
					return nil, nil, fmt.Errorf("encountered duplicate PauseUploadToken for"+
						" pauseId[%v] oldPUT[%v] PUT[%v] putId[%v]", put.PauseId, oldPUT, put, putId)
				}

				puts[putId] = put
			}

		} else {
			logging.Warnf("PauseServiceManager::getCurrPauseTokens Unknown Token %v. Ignored.", kv)

		}

	}

	return pt, puts, nil
}

func (m *PauseServiceManager) cleanupPauseUploadTokens(puts map[string]*common.PauseUploadToken) error {

	if puts == nil || len(puts) == 0 {
		logging.Infof("PauseServiceManager::cleanupPauseUploadTokens: No Tokens Found For Cleanup")
		return nil
	}

	for putId, put := range puts {
		logging.Infof("PauseServiceManager::cleanupPauseUploadTokens: Cleaning Up %v %v", putId, put)

		if put.MasterId == string(m.nodeInfo.NodeID) {
			if err := m.cleanupPauseUploadTokenForMaster(putId, put); err != nil {
				return err
			}
		}

		if put.FollowerId == string(m.nodeInfo.NodeID) {
			if err := m.cleanupPauseUploadTokenForFollower(putId, put); err != nil {
				return err
			}
		}
	}

	return nil
}

func (m *PauseServiceManager) cleanupPauseUploadTokenForMaster(putId string, put *common.PauseUploadToken) error {

	switch put.State {
	case common.PauseUploadTokenProcessed, common.PauseUploadTokenError:

		logging.Infof("PauseServiceManager::cleanupPauseUploadTokenForMaster: Cleanup Token %v %v", putId, put)

		if err := common.MetakvDel(PauseMetakvDir + putId); err != nil {
			logging.Errorf("PauseServiceManager::cleanupPauseUploadTokenForMaster: Unable to delete"+
				"PauseUploadToken[%v] In Meta Storage: err[%v]", put, err)
			return err
		}

	}

	return nil
}

func (m *PauseServiceManager) cleanupPauseUploadTokenForFollower(putId string, put *common.PauseUploadToken) error {

	switch put.State {

	case common.PauseUploadTokenPosted:
		// Followers just acknowledged the token, just delete the token from metakv.

		err := common.MetakvDel(PauseMetakvDir + putId)
		if err != nil {
			logging.Errorf("PauseServiceManager::cleanupPauseUploadTokenForFollower: Unable to delete[%v] in "+
				"Meta Storage: err[%v]", put, err)
			return err
		}

	case common.PauseUploadTokenInProgess:
		// Follower node might be uploading the data

		logging.Infof("PauseServiceManager::cleanupPauseUploadTokenForFollower: Initiating clean-up for"+
			" putId[%v], put[%v]", putId, put)

		pauser, exists := m.getPauser(put.PauseId)
		if !exists {
			err := fmt.Errorf("Pauser with pauseId[%v] not found", put.PauseId)
			logging.Errorf("PauseServiceManager::cleanupPauseUploadTokenForFollower: Failed to find pauser:"+
				" err[%v]", err)

			return err
		}

		// Cancel pause upload work using task ctx
		if doCancelUpload := pauser.task.cancelFunc; doCancelUpload != nil {
			doCancelUpload()
		} else {
			logging.Warnf("PauseServiceManager::cleanupPauseUploadTokenForFollower: Task already cancelled")
		}

		if err := common.MetakvDel(common.PauseMetakvDir + putId); err != nil {
			logging.Errorf("PauseServiceManager::cleanupPauseUploadTokenForFollower: Unable to delete"+
				" PauseUploadToken[%v] In Meta Storage: err[%v]", put, err)
			return err
		}

		logging.Infof("PauseServiceManager::cleanupPauseUploadTokenForFollower: Deleted putId[%v] from metakv",
			putId)

	}

	return nil
}

// PrepareResume is an external API called by ns_server (via cbauth) on all index nodes before
// calling Resume on leader only. The Resume call following PrepareResume will be given the SAME
// dryRun value. The full sequence is:
//
// Phase 1. ns_server will poll progress on leader only via GetTaskList.
//
//  1. All: PrepareResume(dryRun: true) - Create dry run PrepareResume tasks on all nodes.
//
//  2. Leader: Resume(dryRun: true) - Create dry run Resume task on leader, do dry run
//     computations (est # of addl nodes needed to fit on this cluster; may be 0)
//
//     If dry run determines Resume is possible:
//     o Leader: Remove Resume and PrepareResume tasks from ITSELF ONLY.
//     o Followers: ns_server will call CancelTask(PrepareResume) on all followers, which then
//     remove their own PrepareResume tasks.
//
//     If dry run determines Resume is not possible:
//     o Leader: Change its own service.Task.Status to TaskStatusCannotResume and optionally set
//     service.Task.Extra["additionalNodesNeeded"] = <int>
//     o All: ns_server will call CancelTask for Resume on leader and PrepareResume on all nodes
//     and abort the Resume. Index nodes should remove their respective tasks.
//
//     On dry run failure:
//     o Leader: Change its own service.Task.Status to TaskStatusFailed and add failure info to
//     service.Task.ErrorMessage.
//     o All: ns_server will call CancelTask for Resume on leader and PrepareResume on all nodes
//     and abort the Resume. Index nodes should remove their respective tasks.
//
// Phase 2. ns_server will poll progress on leader only via GetTaskList.
//
//  3. All: PrepareResume(dryRun: false) - Create real PrepareResume tasks on all nodes.
//
//  4. Leader: Resume(dryRun: false) - Create real Resume task on leader. Orchestrate Index resume.
//
//     On Resume success:
//     o Leader: Remove Resume and PrepareResume tasks from ITSELF ONLY.
//     o Followers: ns_server will call CancelTask(PrepareResume) on all followers, which then
//     remove their own PrepareResume tasks.
//
//     On Resume failure:
//     o Leader: Change its own service.Task.Status to TaskStatusFailed and add failure info to
//     service.Task.ErrorMessage.
//     o All: ns_server will call CancelTask for Resume on leader and PrepareResume on all nodes
//     and abort the Resume. Index nodes should remove their respective tasks.
//
// Method arguments
//
//		params service.ResumeParams - cbauth object providing
//	  - ID: task ID
//	  - Bucket: name of the bucket to be paused
//	  - RemotePath: object store path
//		- DryRun: if this is a dryRun of resume
func (m *PauseServiceManager) PrepareResume(params service.ResumeParams) (err error) {
	const _PrepareResume = "PauseServiceManager::PrepareResume:"

	const args = "taskId: %v, bucket: %v, remotePath: %v, dryRun: %v"
	logging.Infof("%v Called. "+args, _PrepareResume, params.ID, params.Bucket, params.RemotePath, params.DryRun)
	defer logging.Infof("%v Returned %v. "+args, _PrepareResume, err, params.ID, params.Bucket, params.RemotePath, params.DryRun)

	// TODO: Check remotePath access?

	// Set bst_PREPARE_RESUME state
	err = m.bucketStateSet(_PrepareResume, params.Bucket, bst_NIL, bst_PREPARE_RESUME)
	if err != nil {
		return err
	}

	// Record the task in progress
	return m.taskAddPrepare(params.ID, params.Bucket, params.RemotePath, false, params.DryRun)
}

// Resume is an external API called by ns_server (via cbauth) only on the GSI master node to
// initiate a resume (formerly unhibernate or rehydrate) of a bucket in the serverless offering.
//
// params service.ResumeParams - cbauth object providing
//   - ID: task ID
//   - Bucket: name of the bucket to be paused
//   - RemotePath: object store path
//   - DryRun: if this is a dryRun of resume
func (m *PauseServiceManager) Resume(params service.ResumeParams) error {
	const _Resume = "PauseServiceManager::Resume:"

	const args = "taskId: %v, bucket: %v, remotePath: %v, dryRun: %v"
	logging.Infof("%v Called. "+args, _Resume, params.ID, params.Bucket, params.RemotePath, params.DryRun)
	// Update the task to set this node as master
	task := m.taskSetMasterAndUpdateType(params.ID, service.TaskTypeBucketResume)
	if task == nil || task.isPause {
		err := service.ErrNotFound
		logging.Errorf("%v taskId %v (from PrepareResume) not found", _Resume, params.ID)
		return err
	}

	// Set bst_RESUMING state
	err := m.bucketStateSet(_Resume, params.Bucket, bst_PREPARE_RESUME, bst_RESUMING)
	if err != nil {
		logging.Errorf("%v failed to set bucket state; err: %v for task ID: %v", _Resume,
			err, params.ID)
		return err
	}

	if err := m.initStartPhase(params.Bucket, params.ID, PauseTokenResume); err != nil {
		logging.Errorf("%v couldn't start resume; err: %v for task ID: %v", _Resume, err, params.ID)
		m.runResumeCleanupPhase(params.ID, task.isMaster())
		return err
	}

	// Create a Resumer object to run the master orchestration loop.

	resumer := NewResumer(m, task, m.pauseTokensById[params.ID], m.resumeDoneCallback)

	if err := m.setResumer(params.ID, resumer); err != nil {
		logging.Errorf("%v couldn't set resume; err: %v for task ID: %v", _Resume, err, params.ID)
		m.runResumeCleanupPhase(params.ID, task.isMaster())
		return err
	}

	resumer.startWorkers()

	logging.Infof("%v started resume with task ID %v", _Resume, params.ID)
	return nil
}

// resumeDoneCallback is the Resumer.doneCb callback function.
// Download work is interrupted based on resumeId, using cancel ctx from task in resumer.
func (m *PauseServiceManager) resumeDoneCallback(resumeId string, err error) {

	resumer, exists := m.getResumer(resumeId)
	if !exists {
		logging.Errorf("PauseServiceManager::resumeDoneCallback: Failed to find Resumer with resumeId[%v]", resumeId)
		return
	}

	// If there is an error, set it in the task, otherwise, delete task from task list.
	// TODO: Check if follower task should be handled differently.
	m.endTask(err, resumeId)

	isMaster := resumer.task.isMaster()

	if err := m.runResumeCleanupPhase(resumeId, isMaster); err != nil {
		logging.Errorf("PauseServiceManager::resumeDoneCallback: Failed to run cleanup: err[%v]", err)
		return
	}

	if err := m.setResumer(resumeId, nil); err != nil {
		logging.Errorf("PauseServiceManager::resumeDoneCallback: Failed to unset Resumer: err[%v]", err)
		return
	}

	logging.Infof("PauseServiceManager::resumeDoneCallback Resume Done: isMaster %v, err: %v",
		isMaster, err)
}

func (m *PauseServiceManager) runResumeCleanupPhase(resumeId string, isMaster bool) error {

	logging.Infof("PauseServiceManager::runResumeCleanupPhase: resumeId[%v] isMaster[%v]", resumeId, isMaster)

	if isMaster {
		if err := m.cleanupPauseTokenInMetakv(resumeId); err != nil {
			logging.Errorf("PauseServiceManager::runResumeCleanupPhase: Failed to cleanup PauseToken in metkv:"+
				" err[%v]", err)
			return err
		}
	}

	// TODO: Get tokens and cleanup ResumeDownloadTokens

	if err := m.cleanupLocalPauseToken(resumeId); err != nil {
		logging.Errorf("PauseServiceManager::runResumeCleanupPhase: Failed to cleanup PauseToken in local"+
			" meta: err[%v]", err)
		return err
	}

	return nil
}

// endTask is the endpoint of pause resume
func (m *PauseServiceManager) endTask(opErr error, taskId string) *taskObj {
	var task *taskObj

	logging.Infof("PauseServiceManager::endTask: called with err %v for taskId %v", opErr, taskId)
	if opErr != nil {
		// if caller has passed an error, we don't want to delete the task from task list
		// but the task could be nil
		task = m.taskFind(taskId)
	} else {
		task = m.taskDelete(taskId)
	}
	if task == nil {
		logging.Infof("PauseServiceManager::endTask task with ID %v already cleaned up", taskId)
		return nil
	}

	if opErr != nil {
		task.errorMessage = opErr.Error()
		logging.Infof("PauseServiceManager::endTask: skipping task cleanup now for task ID: %v", taskId)
	}

	task.Cancel()
	m.bucketStateDelete(task.bucket)

	logging.Infof("PauseServiceManager::endTask stopped task %v", task)

	return task
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Delegates for GenericServiceManager RPC APIs
////////////////////////////////////////////////////////////////////////////////////////////////////

// PauseResumeGetTaskList is a delegate of GenericServiceManager.PauseResumeGetTaskList which is an external API
// called by ns_server (via cbauth). It gets the Pause-Resume task list of the current node. Since
// service.Task is a struct, the returned tasks are copies of the originals.
func (m *PauseServiceManager) PauseResumeGetTaskList() (tasks []service.Task) {
	m.tasksMu.RLock()
	defer m.tasksMu.RUnlock()
	for _, taskObj := range m.tasks {
		tasks = append(tasks, taskObj.taskObjToServiceTask()...)
	}
	return tasks
}

// PauseResumeCancelTask is a delegate of GenericServiceManager.PauseResumeCancelTask which is an
// external API called by ns_server (via cbauth). It cancels a Pause-Resume task on the current node
func (m *PauseServiceManager) PauseResumeCancelTask(id string) error {
	task := m.endTask(nil, id)
	if task != nil {
		logging.Errorf("PauseServiceManager::PauseResumeCancelTask: couldn't find a task with ID %v", id)
		return service.ErrNotFound
	}
	logging.Infof("PauseServiceManager::PauseResumeCancelTask: removed all tasks with ID %v", id)
	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// REST orchestration sender methods. These are all called by the master to instruct the followers.
////////////////////////////////////////////////////////////////////////////////////////////////////

// RestNotifyFailedTask calls REST API /pauseMgr/FailedTask to tell followers (otherIndexAddrs
// "host:port") that task has failed for the reason given in errMsg.
func (m *PauseServiceManager) RestNotifyFailedTask(otherIndexAddrs []string, task *taskObj,
	errMsg string) {
	const _RestNotifyFailedTask = "PauseServiceManager::RestNotifyFailedTask:"

	bodyStr := fmt.Sprintf("{\"errMsg\":%v}", errMsg)
	bodyBuf := bytes.NewBuffer([]byte(bodyStr))
	for _, indexAddr := range otherIndexAddrs {
		url := fmt.Sprintf("%v/pauseMgr/FailedTask?id=%v", indexAddr, task.taskId)
		go postWithAuthWrapper(_RestNotifyFailedTask, url, bodyBuf, task)
	}
}

// RestNotifyPause calls REST API /pauseMgr/Pause to tell followers (otherIndexAddrs "host:port")
// to initiate Pause work for task.
func (m *PauseServiceManager) RestNotifyPause(otherIndexAddrs []string, task *taskObj) {
	const _RestNotifyPause = "PauseServiceManager::RestNotifyPause:"

	for _, indexAddr := range otherIndexAddrs {
		url := fmt.Sprintf("%v/pauseMgr/Pause?id=%v", indexAddr, task.taskId)
		go postWithAuthWrapper(_RestNotifyPause, url, bytes.NewBuffer([]byte("{}")), task)
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// REST orchestration receiver methods. These are all invoked on followers to handle and respond to
// instructions from the master.
////////////////////////////////////////////////////////////////////////////////////////////////////

// RestHandleFailedTask handles REST API /pauseMgr/FailedTask by marking the task as failed.
func (m *PauseServiceManager) RestHandleFailedTask(w http.ResponseWriter, r *http.Request) {
	const _RestHandleFailedTask = "PauseServiceManager::RestHandleFailedTask:"

	logging.Infof("%v called", _RestHandleFailedTask)
	defer logging.Infof("%v returned", _RestHandleFailedTask)

	// Authenticate
	_, ok := doAuth(r, w, _RestHandleFailedTask)
	if !ok {
		return
	}

	// Parameters
	id := r.FormValue("id")
	errMsg := r.FormValue("errMsg")

	task := m.taskFind(id)
	if task != nil {
		// Do the work for this task
		logging.Infof("%v Marking taskId %v (taskType %v) failed. errMsg: %v", _RestHandleFailedTask,
			id, task.taskType, errMsg)
		task.TaskObjSetFailed(errMsg)
		resp := &TaskResponse{Code: RESP_SUCCESS, TaskId: id}
		rhSend(http.StatusOK, w, resp)

		return
	}

	// Task not found error reply
	errMsg2 := fmt.Sprintf("%v taskId %v not found", _RestHandleFailedTask, id)
	logging.Errorf(errMsg2)
	resp := &TaskResponse{Code: RESP_ERROR, Error: errMsg2}
	rhSend(http.StatusInternalServerError, w, resp)
}

// RestHandlePause handles REST API /pauseMgr/Pause by initiating work on the specified taskId on
// this follower node.
func (m *PauseServiceManager) RestHandlePause(w http.ResponseWriter, r *http.Request) {

	// Authenticate
	if _, ok := doAuth(r, w, "PauseServiceManager::RestHandlePause:"); !ok {
		err := fmt.Errorf("either invalid credentials or bad request")
		logging.Errorf("PauseServiceManager::RestHandlePause: Failed to authenticate pause register request,"+
			" err[%v]", err)
		return
	}

	writeError := func(w http.ResponseWriter, err error) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error() + "\n"))
	}

	writeOk := func(w http.ResponseWriter) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK\n"))
	}

	var pauseToken PauseToken
	if r.Method == "POST" {
		bytes, _ := ioutil.ReadAll(r.Body)
		if err := json.Unmarshal(bytes, &pauseToken); err != nil {
			logging.Errorf("PauseServiceManager::RestHandlePause: Failed to unmarshal pause token in request"+
				" body: err[%v] bytes[%v]", err, string(bytes))
			writeError(w, err)

			return
		}

		logging.Infof("PauseServiceManager::RestHandlePause: New Pause Token [%v]", pauseToken)

		if m.observeGlobalPauseToken(pauseToken) {
			// Pause token from rest and metakv are the same.

			var task *taskObj
			if task = m.taskFind(pauseToken.PauseId); task == nil {
				err := fmt.Errorf("failed to find task with id[%v]", pauseToken.PauseId)
				logging.Errorf("PauseServiceManager::RestHandlePause: Node[%v] not in Prepared State for pause"+
					": err[%v]", string(m.nodeInfo.NodeID), err)
				writeError(w, err)

				return
			}

			m.pauseTokenMapMu.Lock()
			m.pauseTokensById[pauseToken.PauseId] = &pauseToken
			m.pauseTokenMapMu.Unlock()

			if err := m.registerLocalPauseToken(&pauseToken); err != nil {
				logging.Errorf("PauseServiceManager::RestHandlePause: Failed to store pause token in local"+
					" meta: err[%v]", err)
				writeError(w, err)

				return
			}

			// TODO: Move task from prepared to running

			if pauseToken.Type == PauseTokenPause {

				// Move bucket to bst_PAUSING state
				if err := m.bucketStateSet("PauseServiceManager::RestHandlePause", task.bucket, bst_PREPARE_PAUSE, bst_PAUSING); err != nil {
					logging.Errorf("PauseServiceManager::RestHandlePause: Failed to change bucketState to pausing"+
						" err[%v]", err)
					writeError(w, err)
					return
				}

				pauser := NewPauser(m, task, &pauseToken, m.pauseDoneCallback)

				if err := m.setPauser(pauseToken.PauseId, pauser); err != nil {
					logging.Errorf("PauseServiceManager::RestHandlePause: Failed to set Pauser in bookkeeping"+
						" err[%v]", err)
					writeError(w, err)
					return
				}

				pauser.startWorkers()

			} else if pauseToken.Type == PauseTokenResume {

				// Move bucket to bst_RESUMING state
				if err := m.bucketStateSet("PauseServiceManager::RestHandlePause", task.bucket, bst_PREPARE_RESUME, bst_RESUMING); err != nil {
					logging.Errorf("PauseServiceManager::RestHandlePause: Failed to change bucketState to resuming"+
						" err[%v]", err)
					writeError(w, err)
					return
				}

				resumer := NewResumer(m, task, &pauseToken, m.resumeDoneCallback)

				if err := m.setResumer(pauseToken.PauseId, resumer); err != nil {
					logging.Errorf("PauseServiceManager::RestHandlePause: Failed to set Resumer in bookkeeping"+
						" err[%v]", err)
					writeError(w, err)
					return
				}

				resumer.startWorkers()

			}

			writeOk(w)
			return

		} else {
			// Timed out waiting to see the token in metaKV

			err := fmt.Errorf("pause token wait timeout")
			logging.Errorf("PauseServiceManager::RestHandlePause: Failed to observe token: err[%v]", err)
			writeError(w, err)

			return
		}

	} else {
		writeError(w, fmt.Errorf("PauseServiceManager::RestHandlePause: Unsupported method, use only POST"))
		return
	}
}

func (m *PauseServiceManager) observeGlobalPauseToken(pauseToken PauseToken) bool {

	// TODO: Make timeout configurable
	// Time in seconds to fail pause if metaKV doesn't deliver the token
	globalTokenWaitTimeout := 60 * time.Second

	checkInterval := 1 * time.Second
	var elapsed time.Duration
	var pToken PauseToken
	path := buildMetakvPathForPauseToken(pauseToken.PauseId)

	for elapsed < globalTokenWaitTimeout {

		found, err := common.MetakvGet(path, &pToken)
		if err != nil {
			logging.Errorf("PauseServiceManager::observeGlobalPauseToken Error Checking Pause Token In Metakv: err[%v] path[%v]",
				err, path)

			time.Sleep(checkInterval)
			elapsed += checkInterval

			continue
		}

		if found {
			if reflect.DeepEqual(pToken, pauseToken) {
				logging.Infof("PauseServiceManager::observeGlobalPauseToken Global And Local Pause Tokens Match: [%v]",
					pauseToken)
				return true

			} else {
				logging.Errorf("PauseServiceManager::observeGlobalPauseToken Mismatch in Global and Local Pause Token: Global[%v] Local[%v]",
					pToken, pauseToken)
				return false

			}
		}

		logging.Infof("PauseServiceManager::observeGlobalPauseToken Waiting for Global Pause Token In Metakv")

		time.Sleep(checkInterval)
		elapsed += checkInterval
	}

	logging.Errorf("PauseServiceManager::observeGlobalPauseToken Timeout Waiting for Global Pause Token In Metakv: path[%v]",
		path)

	return false

}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Handlers for unit test REST APIs (/test/methodName) -- MUST STILL DO AUTHENTICATION!!
////////////////////////////////////////////////////////////////////////////////////////////////////

// testPreparePause handles unit test REST API "/test/PreparePause" by calling the PreparePause API
// directly, which is normally called by ns_server via cbauth RPC.
func (m *PauseServiceManager) testPreparePause(w http.ResponseWriter, r *http.Request) {
	const _testPreparePause = "PauseServiceManager::testPreparePause:"

	m.testPauseOrResume(w, r, _testPreparePause, service.TaskTypePrepared, true)
}

// testPause handles unit test REST API "/test/Pause" by calling the Pause API directly, which is
// normally called by ns_server via cbauth RPC.
func (m *PauseServiceManager) testPause(w http.ResponseWriter, r *http.Request) {
	const _testPause = "PauseServiceManager::testPause:"

	m.testPauseOrResume(w, r, _testPause, service.TaskTypeBucketPause, false)
}

// testPrepareResume handles unit test REST API "/test/PrepareResume" by calling the PreparePause
// API directly, which is normally called by ns_server via cbauth RPC.
func (m *PauseServiceManager) testPrepareResume(w http.ResponseWriter, r *http.Request) {
	const _testPrepareResume = "PauseServiceManager::testPrepareResume:"

	m.testPauseOrResume(w, r, _testPrepareResume, service.TaskTypePrepared, false)
}

// testResume handles unit test REST API "/test/Resume" by calling the Resume API directly, which is
// normally called by ns_server via cbauth RPC.
func (m *PauseServiceManager) testResume(w http.ResponseWriter, r *http.Request) {
	const _testResume = "PauseServiceManager::testResume:"

	m.testPauseOrResume(w, r, _testResume, service.TaskTypeBucketResume, false)
}

// testPauseOrResume is the delegate of testPreparePause, testPause, testPrepareResume, testResume
// since their logic differs only in a few parameters and which API they call.
func (m *PauseServiceManager) testPauseOrResume(w http.ResponseWriter, r *http.Request,
	logPrefix string, taskType service.TaskType, pause bool) {

	logging.Infof("%v called", logPrefix)
	defer logging.Infof("%v returned", logPrefix)

	// Authenticate
	_, ok := doAuth(r, w, logPrefix)
	if !ok {
		return
	}

	// Parameters
	id := r.FormValue("id")
	bucket := r.FormValue("bucket")
	remotePath := r.FormValue("remotePath") // e.g. S3 bucket or filesystem path

	var dryRun bool
	dryRunStr := r.FormValue("dryRun") // PrepareResume, Resume only
	if dryRunStr == "true" {
		dryRun = true
	}

	var err error
	switch taskType {
	case service.TaskTypePrepared:
		if pause {
			err = m.PreparePause(service.PauseParams{ID: id, Bucket: bucket, RemotePath: remotePath})
		} else {
			err = m.PrepareResume(service.ResumeParams{ID: id, Bucket: bucket, RemotePath: remotePath, DryRun: dryRun})
		}
	case service.TaskTypeBucketPause:
		err = m.Pause(service.PauseParams{ID: id, Bucket: bucket, RemotePath: remotePath})
	case service.TaskTypeBucketResume:
		err = m.Resume(service.ResumeParams{ID: id, Bucket: bucket, RemotePath: remotePath, DryRun: dryRun})
	default:
		err = fmt.Errorf("%v invalid taskType %v", logPrefix, taskType)
	}
	if err == nil {
		resp := &TaskResponse{Code: RESP_SUCCESS, TaskId: id}
		rhSend(http.StatusOK, w, resp)
		return
	}
	err = fmt.Errorf("%v %v RPC returned error: %v", logPrefix, taskType, err)
	resp := &TaskResponse{Code: RESP_ERROR, Error: err.Error()}
	rhSend(http.StatusInternalServerError, w, resp)
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// General methods and functions
////////////////////////////////////////////////////////////////////////////////////////////////////

// BucketScansBlocked checks whether a bucket must not currently allow scans due to its Pause-
// Resume state. If scans are blocked, this returns the specific blocking state the bucket is in for
// logging, otherwise it returns bst_NIL. Note that bucket state bst_PREPARE_PAUSE does not block
// scans, so bst_NIL will be returned in this case, thus this method is NOT meant to look up the
// current bucket state but only to check the boolean condition of whether scans should be blocked
// and if so return the blocking state for logging in the caller. bst_RESUMED state still blocks
// scans as this state ends (reverting to bst_NIL) only when ns_server marks the bucket active.
func (m *PauseServiceManager) BucketScansBlocked(bucket string) bucketStateEnum {
	m.bucketStatesMu.RLock()
	defer m.bucketStatesMu.RUnlock()
	bucketState := m.bucketStates[bucket]
	if bucketState >= bst_PAUSING {
		return bucketState
	}
	return bst_NIL
}

// bucketStateCompareAndSwap is a helper for bucketStateSet. It sets m.bucketState to newState iff
// the bucket was in oldState, else it does nothing. It returns both the prior state and whether it
// set newState.
func (m *PauseServiceManager) bucketStateCompareAndSwap(bucket string,
	oldState, newState bucketStateEnum) (priorState bucketStateEnum, swapped bool) {

	m.bucketStatesMu.Lock()
	defer m.bucketStatesMu.Unlock()

	priorState = m.bucketStates[bucket]
	if priorState == oldState {
		m.bucketStates[bucket] = newState
		return priorState, true
	}
	return priorState, false
}

// bucketStateDelete deletes a bucket state from m.bucketState, no matter the prior state.
func (m *PauseServiceManager) bucketStateDelete(bucket string) {
	m.bucketStatesMu.Lock()
	delete(m.bucketStates, bucket)
	m.bucketStatesMu.Unlock()
}

// bucketStateSet sets m.bucketState to newState iff the bucket was in oldState, else it does
// nothing but log and return an error with caller's log prefix.
func (m *PauseServiceManager) bucketStateSet(logPrefix, bucket string,
	oldState, newState bucketStateEnum) error {

	priorState, swapped := m.bucketStateCompareAndSwap(bucket, oldState, newState)
	if !swapped {
		err := service.ErrConflict
		logging.Errorf("%v Cannot set bucket %v to Pause-Resume state %v as it already has conflicting state %v", logPrefix,
			bucket, newState, priorState)
		return err
	}
	return nil
}

// GetIndexerNodeAddresses returns a slice of "host:port" for all the current Indexer nodes
// EXCLUDING the one passed in (used to exclude the current node). This does a cinfo.FetchWithLock
// so it can be expensive. It will return a nil slice if this class has never been able to get a
// valid cinfoClient or if there are no Indexer nodes other than excludeAddr.
func (m *PauseServiceManager) GetIndexerNodeAddresses(excludeAddr string) (nodeAddrs []string) {
	const _GetIndexerNodeAddresses = "PauseCinfo::GetIndexerNodeAddresses:"

	if err := m.genericMgr.cinfo.FetchWithLock(); err != nil {
		logging.Warnf("%v Using potentially stale CIC data as FetchWithLock returned err: %v", _GetIndexerNodeAddresses, err)
	}

	m.genericMgr.cinfo.RLock()
	defer m.genericMgr.cinfo.RUnlock()
	nids := m.genericMgr.cinfo.GetNodeIdsByServiceType(common.INDEX_HTTP_SERVICE)
	for _, nid := range nids {
		nodeAddr, err := m.genericMgr.cinfo.GetServiceAddress(nid, common.INDEX_HTTP_SERVICE, true)
		if err == nil {
			if nodeAddr != excludeAddr {
				nodeAddrs = append(nodeAddrs, nodeAddr)
			}
		} else { // err != nil
			logging.Errorf("%v Skipping nid %v as GetServiceAddress returned error: %v", _GetIndexerNodeAddresses, nid, err)
		}
	}
	return nodeAddrs
}

// taskAdd adds a task to m.tasks.
func (m *PauseServiceManager) taskAdd(task *taskObj) {
	m.tasksMu.Lock()
	m.tasks[task.taskId] = task
	m.tasksMu.Unlock()
}

// taskAddPreparePause constructs and adds a Pause task to m.tasks.
func (m *PauseServiceManager) taskAddPrepare(taskId, bucket, remotePath string, isPause, dryRun bool) error {
	task, err := NewTaskObj(service.TaskTypePrepared, taskId, bucket, remotePath, isPause, dryRun)
	if err != nil {
		return err
	}
	m.taskAdd(task)
	return nil
}

// taskClone looks up a task by taskId and if found returns a pointer to a CLONE of it, else nil.
// The clone has no mutex and thus should not be shared.
func (m *PauseServiceManager) taskClone(taskId string) *taskObj {
	task := m.taskFind(taskId)
	if task != nil {
		task.taskMu.RLock()
		taskClone := *task
		task.taskMu.RUnlock()

		taskClone.taskMu = nil // don't share original mutex; nil as clone does not need locking
		return &taskClone
	}
	return nil
}

// taskDelete deletes a task by taskId if it exists. If the task existed, this method returns the
// deleted task, else nil (no-op).
func (m *PauseServiceManager) taskDelete(taskId string) *taskObj {
	m.tasksMu.Lock()
	defer m.tasksMu.Unlock()
	task := m.tasks[taskId]
	if task != nil {
		delete(m.tasks, taskId)
	}
	return task
}

// taskFind looks up a task by taskId and if found returns a pointer to it (not a copy), else nil.
func (m *PauseServiceManager) taskFind(taskId string) *taskObj {
	m.tasksMu.RLock()
	defer m.tasksMu.RUnlock()
	return m.tasks[taskId]
}

// taskSetFailed looks up a task by taskId and if found marks it as failed with the given errMsg and
// returns true, else returns false (not found).
func (m *PauseServiceManager) taskSetFailed(taskId, errMsg string) bool {
	task := m.taskFind(taskId)
	if task != nil {
		task.TaskObjSetFailed(errMsg)
		return true
	}
	return false
}

func (m *PauseServiceManager) taskSetMasterAndUpdateType(taskId string, newType service.TaskType) *taskObj {
	task := m.taskFind(taskId)
	if task != nil {
		task.taskMu.Lock()
		task.setMasterNoLock()
		task.updateTaskTypeNoLock(newType)
		task.taskMu.Unlock()
	}
	return task
}

// hasDryRun returns whether this task has the dryRun parameter.
func (this *taskObj) hasDryRun() bool {
	this.taskMu.RLock()
	defer this.taskMu.RUnlock()
	if this.taskType == service.TaskTypeBucketResume || this.taskType == service.TaskTypePrepared {
		return true
	}
	return false
}

func (task *taskObj) isMaster() bool {
	task.taskMu.RLock()
	defer task.taskMu.RUnlock()

	return task.master
}

// postWithAuthWrapper wraps postWithAuth so it can be called as a goroutine for parallel scatter.
// Errors are logged with caller's logPrefix. bodyBuf contains JSON POST body. If anything fails
// this will mark the task as failed here on the master and also notify the workers of the failure.
func postWithAuthWrapper(logPrefix string, url string, bodyBuf *bytes.Buffer, task *taskObj) {

	resp, err := postWithAuth(url, "application/json", bodyBuf)
	if err != nil {
		err = fmt.Errorf("%v postWithAuth to url: %v returned error: %v", logPrefix, url, err)
		logging.Errorf(err.Error())
		task.TaskObjSetFailed(err.Error())
		return
	}
	defer resp.Body.Close()

	bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		err = fmt.Errorf("%v ReadAll(resp.Body) from url: %v returned error: %v", logPrefix,
			url, err)
		logging.Errorf(err.Error())
		task.TaskObjSetFailed(err.Error())
		return
	}

	var taskResponse TaskResponse
	err = json.Unmarshal(bytes, &taskResponse)
	if err != nil {
		err = fmt.Errorf("%v Unmarshal from url: %v returned error: %v", logPrefix, url, err)
		logging.Errorf(err.Error())
		task.TaskObjSetFailed(err.Error())
		return
	}

	// Check if taskResponse reports an error, which would be from GSI code, not HTTP
	if taskResponse.Code == RESP_ERROR {
		err = fmt.Errorf("%v TaskResponse from url: %v reports error: %v", logPrefix,
			url, taskResponse.Error)
		logging.Errorf(err.Error())
		task.TaskObjSetFailed(err.Error())
		return
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// PauseToken
////////////////////////////////////////////////////////////////////////////////////////////////////

const PauseTokenTag = "PauseToken"
const PauseMetakvDir = common.IndexingMetaDir + "pause/"
const PauseTokenPathPrefix = PauseMetakvDir + PauseTokenTag

type PauseTokenType uint8

const (
	PauseTokenPause PauseTokenType = iota
	PauseTokenResume
)

type PauseToken struct {
	MasterId string
	MasterIP string

	BucketName string
	PauseId    string

	Type PauseTokenType

	Error string
}

func (m *PauseServiceManager) genPauseToken(masterIP, bucketName, pauseId string, typ PauseTokenType) *PauseToken {
	cfg := m.config.Load()
	return &PauseToken{
		MasterId:   cfg["nodeuuid"].String(),
		MasterIP:   masterIP,
		BucketName: bucketName,
		PauseId:    pauseId,
		Type:       typ,
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// PauseToken - Lifecycle
////////////////////////////////////////////////////////////////////////////////////////////////////

func buildKeyForLocalPauseToken(pauseId string) string {
	return fmt.Sprintf("%s_%s", PauseTokenTag, pauseId)
}

func buildMetakvPathForPauseToken(pauseId string) string {
	return fmt.Sprintf("%s_%s", PauseTokenPathPrefix, pauseId)
}

func (m *PauseServiceManager) registerLocalPauseToken(pauseToken *PauseToken) error {

	pToken, err := json.Marshal(pauseToken)
	if err != nil {
		logging.Errorf("PauseServiceManager::registerLocalPauseToken: Failed to marshal pauseToken[%v]: err[%v]",
			pauseToken, err)
		return err
	}

	respch := make(MsgChannel)
	m.supvMsgch <- &MsgClustMgrLocal{
		mType:  CLUST_MGR_SET_LOCAL,
		key:    buildKeyForLocalPauseToken(pauseToken.PauseId),
		value:  string(pToken),
		respch: respch,
	}

	respMsg := <-respch
	resp := respMsg.(*MsgClustMgrLocal)

	if err = resp.GetError(); err != nil {
		logging.Errorf("PauseServiceManager::registerLocalPauseToken: Unable to set PauseToken In Local Meta"+
			"Storage: [%v]", err)
		return err
	}

	logging.Infof("PauseServiceManager::registerLocalPauseToken: Registered Pause Token In Local Meta: [%v]",
		string(pToken))

	return nil
}

func (m *PauseServiceManager) cleanupLocalPauseToken(pauseId string) error {

	logging.Infof("PauseServiceManager::cleanupLocalPauseToken: Cleanup PauseToken[%v]", pauseId)

	key := buildKeyForLocalPauseToken(pauseId)

	respch := make(MsgChannel)
	m.supvMsgch <- &MsgClustMgrLocal{
		mType:  CLUST_MGR_DEL_LOCAL,
		key:    key,
		respch: respch,
	}

	respMsg := <-respch
	resp := respMsg.(*MsgClustMgrLocal)

	if err := resp.GetError(); err != nil {
		logging.Fatalf("PauseServiceManager::cleanupLocalPauseToken: Unable to delete Pause Token In Local"+
			"Meta Storage. Path[%v] Err[%v]", key, err)
		common.CrashOnError(err)
	}

	m.pauseTokenMapMu.Lock()
	delete(m.pauseTokensById, pauseId)
	m.pauseTokenMapMu.Unlock()

	return nil
}

func (m *PauseServiceManager) registerPauseTokenInMetakv(pauseToken *PauseToken) error {

	path := buildMetakvPathForPauseToken(pauseToken.PauseId)

	err := common.MetakvSet(path, pauseToken)
	if err != nil {
		logging.Errorf("PauseServiceManager::registerPauseTokenInMetakv: Unable to set PauseToken In Metakv Storage: Err[%v]", err)
		return err
	}

	logging.Infof("PauseServiceManager::registerPauseTokenInMetakv: Registered Global PauseToken[%v] In Metakv at path[%v]", pauseToken, path)

	return nil
}

func (m *PauseServiceManager) cleanupPauseTokenInMetakv(pauseId string) error {

	path := buildMetakvPathForPauseToken(pauseId)
	var ptoken PauseToken

	if found, err := common.MetakvGet(path, &ptoken); err != nil {
		logging.Errorf("PauseServiceManager::cleanupPauseTokenInMetakv Error Fetching Pause Token From Metakv"+
			" err[%v] path[%v]", err, path)

		return err

	} else if found {
		logging.Infof("PauseServiceManager::cleanupPauseTokenInMetakv Delete Global Pause Token %v", ptoken)

		if err := common.MetakvDel(path); err != nil {
			logging.Fatalf("PauseServiceManager::cleanupPauseTokenInMetakv Unable to delete PauseToken"+
				" from Meta Storage. %v. Err %v", ptoken, err)

			return err
		}

	}

	return nil
}

func (m *PauseServiceManager) registerGlobalPauseToken(pauseToken *PauseToken) (err error) {

	m.genericMgr.cinfo.Lock()
	defer m.genericMgr.cinfo.Unlock()

	nids := m.genericMgr.cinfo.GetNodeIdsByServiceType(common.INDEX_HTTP_SERVICE)
	if len(nids) < 1 {
		err := fmt.Errorf("got too few indexer nodes[%d]", len(nids))
		logging.Errorf("PauseServiceManager::registerGlobalPauseToken: Failed to get NodeIds, err[%v]", err)

		return err
	}

	url := "/pauseMgr/Pause"
	for _, nid := range nids {

		addr, err := m.genericMgr.cinfo.GetServiceAddress(nid, common.INDEX_HTTP_SERVICE, true)
		if err == nil {

			localaddr, err := m.genericMgr.cinfo.GetLocalServiceAddress(common.INDEX_HTTP_SERVICE, true)
			if err != nil {
				logging.Errorf("PauseServiceManager::registerGlobalPauseToken: Error Fetching Local Service"+
					" Address [%v]", err)
				return err
			}

			if addr == localaddr {
				logging.Infof("PauseServiceManager::registerGlobalPauseToken: Skip local service [%v]", addr)
				continue
			}

			body, err := json.Marshal(pauseToken)
			if err != nil {
				logging.Errorf("PauseServiceManager::registerGlobalPauseToken: Failed to marshal pause token:"+
					" err[%v] pauseToken[%v]", err, pauseToken)
				return err
			}

			bodyBuf := bytes.NewBuffer(body)
			resp, err := postWithAuth(addr+url, "application/json", bodyBuf)
			if err != nil {
				logging.Errorf("PauseServiceManager::registerGlobalPauseToken: Error registering pause token,"+
					" err[%v] addr[%v]", err, addr+url)
				return err
			}

			ioutil.ReadAll(resp.Body)
			resp.Body.Close()

		} else {
			logging.Errorf("PauseServiceManager::registerGlobalPauseToken: Failed to Fetch Service Address:"+
				" [%v]", err)
			return err
		}

		logging.Infof("PauseServiceManager::registerGlobalPauseToken: Successfully registered pause token on"+
			" [%v]", addr+url)
	}

	return nil
}

// monitorBucketForPauseResume - starts monitoring bucket endpoints for state changes to the bucket
//
// not a part of Pauser/Resumer object as it can be garbage collected while this is running
func monitorBucketForPauseResume(bucket string, isPause bool) {
	logging.Infof("Pauser::startWathcer: TODO: monitor bucket monitoring enpoint for bucket %v, under pause? %v", bucket, isPause)
}

func CancellableTaskRunnerWithContext(ctx context.Context, cancelledError error) func(func() error) error {
	return func(executor func() error) error {
		if ctx == nil {
			return cancelledError
		}
		closeCh := ctx.Done()
		return CancellableTaskRunnerWithChannel(closeCh, cancelledError)(executor)
	}
}

func CancellableTaskRunnerWithChannel(closeCh <-chan struct{}, cancelledError error) func(func() error) error {
	return func(executor func() error) error {
		select {
		case <-closeCh:
			return cancelledError
		default:
			return executor()
		}
	}
}

// generateNodeDir joins archivePath and nodeId to create a node directory used during pause resume
// nodeDir ends with filepath.Seperator(/)
func generateNodeDir(archivePath string, nodeId service.NodeID) string {
	separator := string(filepath.Separator)
	archivePath = strings.TrimSuffix(archivePath, separator)
	return filepath.Join(archivePath, fmt.Sprintf("node_%v", nodeId)) + separator
}

// generateShardPath generates a location to download shards from
// shardPath ends with filepath.Seperator(/)
func generateShardPath(nodeDir string, trimmedShardPath string) string {
	separator := string(filepath.Separator)
	trimmedShardPath = strings.TrimPrefix(trimmedShardPath, separator)
	nodeDir = strings.TrimSuffix(nodeDir, separator)
	return filepath.Join(nodeDir, trimmedShardPath) + separator
}
