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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"sync/atomic"
	"unsafe"

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

	// bucketStates is a map from bucket to its Pause-Resume state. The design supports concurrent
	// pauses and resumes of different buckets, although this may not be done in practice.
	bucketStates map[string]bucketStateEnum

	// bucketStatesMu protects bucketStates
	bucketStatesMu sync.RWMutex

	// tasks is the set of Pause-Resume tasks that are running, if any, mapped by taskId
	tasks map[string]*taskObj

	// tasksMu protects tasks
	tasksMu sync.RWMutex
}

// NewPauseServiceManager is the constructor for the PauseServiceManager class.
// GenericServiceManager constructs a singleton at boot.
//   genericMgr - pointer to our parent
//   mux - Indexer's HTTP server
//   httpAddr - host:port of the local node for Index Service HTTP calls
func NewPauseServiceManager(genericMgr *GenericServiceManager, mux *http.ServeMux,
	httpAddr string) *PauseServiceManager {

	m := &PauseServiceManager{
		genericMgr: genericMgr,
		httpAddr:   httpAddr,

		bucketStates: make(map[string]bucketStateEnum),
		tasks:        make(map[string]*taskObj),
	}

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

	return m
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

// FILENAME_VERSION is the name of the file to write containing the ARCHIVE_VERSION.
const FILENAME_VERSION = "version.json"

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

// taskEnum defines types of tasks (following task_XXX constants) and is local analog of
// ns_server service.TaskType.
type taskEnum int

const (
	task_NIL taskEnum = iota // undefined
	task_PAUSE
	task_RESUME
)

// String converter for taskEnum type.
func (this taskEnum) String() string {
	switch this {
	case task_NIL:
		return "NilTask"
	case task_PAUSE:
		return "Pause"
	case task_RESUME:
		return "Resume"
	default:
		return fmt.Sprintf("undefinedTaskEnum_%v", int(this))
	}
}

// StringNs is an alternate string converter for taskEnum that gives the value for
// service.TaskTypeXxx constants (cbauth/service/interface.go).
func (this taskEnum) StringNs() string {
	switch this {
	case task_PAUSE:
		return "task-pause" // kjc not defined in interface.go yet
	case task_RESUME:
		return "task-resume" // kjc not defined in interface.go yet
	default:
		return fmt.Sprintf("undefinedTaskEnum_%v", int(this))
	}
}

// statusEnum defines status of tasks (following status_XXX constants) and is local analog of
// ns_server service.TaskStatus.
type statusEnum int

const (
	status_RUNNING statusEnum = iota
	status_FAILED
	status_CANNOT_RESUME // Resume (unhibernate) dry run says cannot resume; no error in dry run
)

// String converter for statusEnum type.
func (this statusEnum) String() string {
	switch this {
	case status_RUNNING:
		return "Running"
	case status_FAILED:
		return "Failed"
	case status_CANNOT_RESUME:
		return "CannotResume"
	default:
		return fmt.Sprintf("undefinedStatusEnum_%v", int(this))
	}
}

// StringNs is an alternate string converter for statusEnum that gives the value for
// service.TaskStatusXxx constants (cbauth/service/interface.go).
func (this statusEnum) StringNs() string {
	switch this {
	case status_RUNNING:
		return string(service.TaskStatusRunning)
	case status_FAILED:
		return string(service.TaskStatusFailed)
	case status_CANNOT_RESUME:
		return "task-cannot-resume" // kjc not defined in interface.go yet
	default:
		return fmt.Sprintf("undefinedStatusEnum_%v", int(this))
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Type definitions
////////////////////////////////////////////////////////////////////////////////////////////////////

// taskObj represents one task of any type in the taskType enum below. This is the GSI internal
// representation, not the GetTaskList return format.
//
// For GetTaskList response to ns_server, we convert a taskObj to a service.Task struct (shared
// with Rebalance).
type taskObj struct {
	taskMu *sync.RWMutex // protects this taskObj; pointer as taskObj may be cloned

	taskType     taskEnum   // local analog of service.TaskType
	taskId       string     // opaque ns_server unique ID for this task
	taskStatus   statusEnum // local analog of service.TaskStatus
	progress     float64    // completion progress [0.0, 1.0]
	errorMessage string     // only if a failure occurred

	bucket     string // bucket name being Paused or Resumed
	bucketUuid string // Pause: UUID of bucket; Resume: ""
	dryRun     bool   // is task for Resume dry run (cluster capacity check)?

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
}

// NewTaskObj is the constructor for the taskObj class. If the parameters are not valid, it will
// return (nil, error) rather than create an unsupported taskObj.
func NewTaskObj(taskType taskEnum, taskId, bucket, bucketUuid, remotePath string, dryRun bool,
) (*taskObj, error) {

	archiveType, archivePath, err := ArchiveInfoFromRemotePath(remotePath)
	if err != nil {
		return nil, err
	}
	return &taskObj{
		taskMu:      &sync.RWMutex{},
		taskType:    taskType,
		taskId:      taskId,
		taskStatus:  status_RUNNING,
		bucket:      bucket,
		bucketUuid:  bucketUuid,
		dryRun:      dryRun,
		archivePath: archivePath,
		archiveType: archiveType,
	}, nil
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
//   3. All: PreparePause - Create PreparePause tasks on all nodes.
//
//   4. Leader: Pause - Create Pause task on leader. Orchestrate Index pause.
//
//      On Pause success:
//        o Leader: Remove Pause and PreparePause tasks from ITSELF ONLY.
//        o Followers: ns_server will call CancelTask(PreparePause) on all followers, which then
//          remove their own PreparePause tasks.
//
//      On Pause failure:
//        o Leader: Change its own service.Task.Status to TaskStatusFailed and add failure info to
//          service.Task.ErrorMessage.
//        o All: ns_server will call CancelTask for Pause on leader and PreparePause on all nodes
//          and abort the Pause. Index nodes should remove their respective tasks.
//
// Method arguments
//   taskId - opaque ns_server unique ID for this task
//   bucket - name of the bucket to operate on
//   bucketUuid - UUID of the bucket to operate on
//   remotePath - root of storage (e.g. "s3://..." or "file3://...") for the image
func (m *PauseServiceManager) PreparePause(taskId, bucket, bucketUuid, remotePath string,
) (err error) {
	const _PreparePause = "PauseServiceManager::PreparePause:"

	const args = "taskId: %v, bucket: %v, bucketUuid: %v, remotePath: %v"
	logging.Infof("%v Called. "+args, _PreparePause, taskId, bucket, bucketUuid, remotePath)
	defer logging.Infof("%v Returned %v. "+args, _PreparePause, err, taskId, bucket, bucketUuid,
		remotePath)

	// Set bst_PREPARE_PAUSE state
	err = m.bucketStateSet(_PreparePause, bucket, bst_NIL, bst_PREPARE_PAUSE)
	if err != nil {
		return err
	}

	// Record the task in progress
	return m.taskAddPause(taskId, bucket, bucketUuid, remotePath)
}

// Pause is an external API called by ns_server (via cbauth) only on the GSI master node to initiate
// a pause (formerly hibernate) of a bucket in the serverless offering.
//   taskId - opaque ns_server unique ID for this task
//   bucket - name of the bucket to operate on
//   bucketUuid - UUID of the bucket to operate on
//   remotePath - root of storage (e.g. "s3://..." or "file3://...") for the image
func (m *PauseServiceManager) Pause(taskId, bucket, bucketUuid, remotePath string) (err error) {
	const _Pause = "PauseServiceManager::Pause:"

	const args = "taskId: %v, bucket: %v, bucketUuid: %v, remotePath: %v"
	logging.Infof("%v Called. "+args, _Pause, taskId, bucket, bucketUuid, remotePath)
	defer logging.Infof("%v Returned %v. "+args, _Pause, err, taskId, bucket, bucketUuid,
		remotePath)

	// Update the task to set this node as master
	task := m.taskSetMaster(taskId)
	if task == nil {
		err = service.ErrNotFound
		logging.Errorf("%v taskId %v (from PreparePause) not found", _Pause, taskId)
		return err
	}

	// Set bst_PAUSING state
	err = m.bucketStateSet(_Pause, bucket, bst_PREPARE_PAUSE, bst_PAUSING)
	if err != nil {
		return err
	}

	// Create a Pauser object to run the master orchestration loop. It will be the only thread
	// that changes or deletes *task after this point. It will save a pointer to itself into
	// task.pauser and start its own goroutine, so we don't need to save a pointer to it here.
	RunPauser(m, task, true)
	return nil
}

// PrepareResume is an external API called by ns_server (via cbauth) on all index nodes before
// calling Resume on leader only. The Resume call following PrepareResume will be given the SAME
// dryRun value. The full sequence is:
//
// Phase 1. ns_server will poll progress on leader only via GetTaskList.
//
//   1. All: PrepareResume(dryRun: true) - Create dry run PrepareResume tasks on all nodes.
//
//   2. Leader: Resume(dryRun: true) - Create dry run Resume task on leader, do dry run
//        computations (est # of addl nodes needed to fit on this cluster; may be 0)
//
//      If dry run determines Resume is possible:
//        o Leader: Remove Resume and PrepareResume tasks from ITSELF ONLY.
//        o Followers: ns_server will call CancelTask(PrepareResume) on all followers, which then
//          remove their own PrepareResume tasks.
//
//      If dry run determines Resume is not possible:
//        o Leader: Change its own service.Task.Status to TaskStatusCannotResume and optionally set
//          service.Task.Extra["additionalNodesNeeded"] = <int>
//        o All: ns_server will call CancelTask for Resume on leader and PrepareResume on all nodes
//          and abort the Resume. Index nodes should remove their respective tasks.
//
//      On dry run failure:
//        o Leader: Change its own service.Task.Status to TaskStatusFailed and add failure info to
//          service.Task.ErrorMessage.
//        o All: ns_server will call CancelTask for Resume on leader and PrepareResume on all nodes
//          and abort the Resume. Index nodes should remove their respective tasks.
//
// Phase 2. ns_server will poll progress on leader only via GetTaskList.
//
//   3. All: PrepareResume(dryRun: false) - Create real PrepareResume tasks on all nodes.
//
//   4. Leader: Resume(dryRun: false) - Create real Resume task on leader. Orchestrate Index resume.
//
//      On Resume success:
//        o Leader: Remove Resume and PrepareResume tasks from ITSELF ONLY.
//        o Followers: ns_server will call CancelTask(PrepareResume) on all followers, which then
//          remove their own PrepareResume tasks.
//
//      On Resume failure:
//        o Leader: Change its own service.Task.Status to TaskStatusFailed and add failure info to
//          service.Task.ErrorMessage.
//        o All: ns_server will call CancelTask for Resume on leader and PrepareResume on all nodes
//          and abort the Resume. Index nodes should remove their respective tasks.
//
// Method arguments
//   taskId - opaque ns_server unique ID for this task
//   bucket - name of the bucket to operate on
//   remotePath - root of storage (e.g. "s3://..." or "file3://...") for the image
//   dryRun - whether this call is for a dry run or real Resume
func (m *PauseServiceManager) PrepareResume(taskId, bucket, remotePath string, dryRun bool,
) (err error) {
	const _PrepareResume = "PauseServiceManager::PrepareResume:"

	const args = "taskId: %v, bucket: %v, remotePath: %v, dryRun: %v"
	logging.Infof("%v Called. "+args, _PrepareResume, taskId, bucket, remotePath, dryRun)
	defer logging.Infof("%v Returned %v. "+args, _PrepareResume, err, taskId, bucket, remotePath,
		dryRun)

	// Set bst_PREPARE_RESUME state
	err = m.bucketStateSet(_PrepareResume, bucket, bst_NIL, bst_PREPARE_RESUME)
	if err != nil {
		return err
	}

	// Record the task in progress
	return m.taskAddResume(taskId, bucket, remotePath, dryRun)
}

// Resume is an external API called by ns_server (via cbauth) only on the GSI master node to
// initiate a resume (formerly unhibernate or rehydrate) of a bucket in the serverless offering.
//   taskId - opaque ns_server unique ID for this task
//   bucket - name of the bucket to operate on
//   remotePath - root of storage (e.g. "s3://..." or "file3://...") for the image
//   dryRun - whether this call is for a dry run or real Resume
func (m *PauseServiceManager) Resume(taskId, bucket, remotePath string, dryRun bool) (err error) {
	const _Resume = "PauseServiceManager::Resume:"

	const args = "taskId: %v, bucket: %v, remotePath: %v, dryRun: %v"
	logging.Infof("%v Called. "+args, _Resume, taskId, bucket, remotePath, dryRun)
	defer logging.Infof("%v Returned %v. "+args, _Resume, err, taskId, bucket, remotePath, dryRun)

	// Update the task to set this node as master
	task := m.taskSetMaster(taskId)
	if task == nil {
		err = service.ErrNotFound
		logging.Errorf("%v taskId %v (from PrepareResume) not found", _Resume, taskId)
		return err
	}

	// Set bst_RESUMING state
	err = m.bucketStateSet(_Resume, bucket, bst_PREPARE_RESUME, bst_RESUMING)
	if err != nil {
		return err
	}

	// Create a Resumer object to run the master orchestration loop. It will be the only thread
	// that changes or deletes *task after this point. It will save a pointer to itself into
	// task.resumer and start its own goroutine, so we don't need to save a pointer to it here.
	RunResumer(m, task, true)
	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Delegates for GenericServiceManager RPC APIs
////////////////////////////////////////////////////////////////////////////////////////////////////

// PauseGetTaskList is a delegate of GenericServiceManager.GetTaskList which is an external API
// called by ns_server (via cbauth). It gets the Pause-Resume task list of the current node. Since
// service.Task is a struct, the returned tasks are copies of the originals.
func (m *PauseServiceManager) PauseGetTaskList() (tasks []service.Task) {
	m.tasksMu.RLock()
	defer m.tasksMu.RUnlock()
	for _, taskObj := range m.tasks {
		tasks = append(tasks, *taskObj.taskObjToServiceTask())
	}
	return tasks
}

// PauseCancelTask is a delegate of GenericServiceManager.CancelTask which is an external API
// called by ns_server (via cbauth). It cancels a Pause-Resume task owned by the current node.
func (m *PauseServiceManager) PauseCancelTask(id string) error {
	taskClone := m.taskClone(id)
	if taskClone == nil {
		return service.ErrNotFound
	}

	switch taskClone.taskType {
	case task_PAUSE:
		return m.CancelPause(taskClone)
	case task_RESUME:
		return m.CancelResume(taskClone)
	default:
		return service.ErrNotSupported
	}
}

// CancelPause cancels a local Pause task and deletes its Pause-Resume state. If the current node
// was the master and the state had reached bst_PAUSING, this must do extra work to abort the
// Pause.
func (m *PauseServiceManager) CancelPause(taskClone *taskObj) error {
	const _CancelPause = "PauseServiceManager::CancelPause:"
	logging.Infof("%v canceling Pause taskId: %v, task: %+v", _CancelPause,
		taskClone.taskId, taskClone)

	// Delete the Pause task
	task := m.taskDelete(taskClone.taskId)
	if task == nil {
		// Something else deleted it since caller found and cloned it
		logging.Infof("%v taskId %v not found (already finished or canceled)", _CancelPause,
			taskClone.taskId)
		return service.ErrNotFound
	}

	// Master orchestrates aborting the Pause. If no node is master, the task never got past
	// prepare, so there is nothing to orchestrate.
	if task.master {
		// kjc abort Pause
	}

	// Delete the bucket state
	m.bucketStateDelete(task.bucket)
	return nil
}

// CancelResume cancels a local Resume task and deletes its Pause-Resume state. If the current node
// was the master and the state had reached bst_RESUMING, this must do extra work to abort the
// Resume.
func (m *PauseServiceManager) CancelResume(taskClone *taskObj) error {
	const _CancelResume = "PauseServiceManager::CancelResume:"
	logging.Infof("%v canceling Resume taskId: %v, task: %+v", _CancelResume,
		taskClone.taskId, taskClone)

	// Delete the Resume task
	task := m.taskDelete(taskClone.taskId)
	if task == nil {
		// Something else deleted it since caller found and cloned it
		logging.Infof("%v taskId: %v not found (already finished or canceled)", _CancelResume,
			taskClone.taskId)
		return service.ErrNotFound
	}

	// Master orchestrates aborting the Resume. If no node is master, the task never got past
	// prepare, so there is nothing to orchestrate.
	if task.master {
		// kjc abort Resume
	}

	// Delete the bucket state
	m.bucketStateDelete(task.bucket)
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
	const _RestHandlePause = "PauseServiceManager::RestHandlePause:"

	logging.Infof("%v called", _RestHandlePause)
	defer logging.Infof("%v returned", _RestHandlePause)

	// Authenticate
	_, ok := doAuth(r, w, _RestHandlePause)
	if !ok {
		return
	}

	// Parameters
	id := r.FormValue("id")

	task := m.taskFind(id)
	if task != nil {
		// Do the work for this task
		RunPauser(m, task, false)

		return
	}

	// Task not found error reply
	errMsg2 := fmt.Sprintf("%v taskId %v not found", _RestHandlePause, id)
	logging.Errorf(errMsg2)
	resp := &TaskResponse{Code: RESP_ERROR, Error: errMsg2}
	rhSend(http.StatusInternalServerError, w, resp)
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Handlers for unit test REST APIs (/test/methodName) -- MUST STILL DO AUTHENTICATION!!
////////////////////////////////////////////////////////////////////////////////////////////////////

// testPreparePause handles unit test REST API "/test/PreparePause" by calling the PreparePause API
// directly, which is normally called by ns_server via cbauth RPC.
func (m *PauseServiceManager) testPreparePause(w http.ResponseWriter, r *http.Request) {
	const _testPreparePause = "PauseServiceManager::testPreparePause:"

	m.testPauseOrResume(w, r, _testPreparePause, task_PAUSE, true)
}

// testPause handles unit test REST API "/test/Pause" by calling the Pause API directly, which is
// normally called by ns_server via cbauth RPC.
func (m *PauseServiceManager) testPause(w http.ResponseWriter, r *http.Request) {
	const _testPause = "PauseServiceManager::testPause:"

	m.testPauseOrResume(w, r, _testPause, task_PAUSE, false)
}

// testPrepareResume handles unit test REST API "/test/PrepareResume" by calling the PreparePause
// API directly, which is normally called by ns_server via cbauth RPC.
func (m *PauseServiceManager) testPrepareResume(w http.ResponseWriter, r *http.Request) {
	const _testPrepareResume = "PauseServiceManager::testPrepareResume:"

	m.testPauseOrResume(w, r, _testPrepareResume, task_RESUME, true)
}

// testResume handles unit test REST API "/test/Resume" by calling the Resume API directly, which is
// normally called by ns_server via cbauth RPC.
func (m *PauseServiceManager) testResume(w http.ResponseWriter, r *http.Request) {
	const _testResume = "PauseServiceManager::testResume:"

	m.testPauseOrResume(w, r, _testResume, task_RESUME, false)
}

// testPauseOrResume is the delegate of testPreparePause, testPause, testPrepareResume, testResume
// since their logic differs only in a few parameters and which API they call.
func (m *PauseServiceManager) testPauseOrResume(w http.ResponseWriter, r *http.Request,
	logPrefix string, taskType taskEnum, prepare bool) {

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
	bucketUuid := r.FormValue("bucketUuid") // PreparePause, Pause only
	remotePath := r.FormValue("remotePath") // e.g. S3 bucket or filesystem path

	var dryRun bool
	dryRunStr := r.FormValue("dryRun") // PrepareResume, Resume only
	if dryRunStr == "true" {
		dryRun = true
	}

	var err error
	switch taskType {
	case task_PAUSE:
		if prepare {
			err = m.PreparePause(id, bucket, bucketUuid, remotePath)
		} else {
			err = m.Pause(id, bucket, bucketUuid, remotePath)
		}
	case task_RESUME:
		if prepare {
			err = m.PrepareResume(id, bucket, remotePath, dryRun)
		} else {
			err = m.Resume(id, bucket, remotePath, dryRun)
		}
	default:
		err = fmt.Errorf("%v invalid taskType %v", logPrefix, int(taskType))
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

// taskAdd adds a task to m.tasks.
func (m *PauseServiceManager) taskAdd(task *taskObj) {
	m.tasksMu.Lock()
	m.tasks[task.taskId] = task
	m.tasksMu.Unlock()
}

// taskAddPause constructs and adds a Pause task to m.tasks.
func (m *PauseServiceManager) taskAddPause(taskId, bucket, bucketUuid, remotePath string) error {
	task, err := NewTaskObj(task_PAUSE, taskId, bucket, bucketUuid, remotePath, false)
	if err != nil {
		return err
	}
	m.taskAdd(task)
	return nil
}

// taskAddResume constructs and adds a Resume task to m.tasks.
func (m *PauseServiceManager) taskAddResume(taskId, bucket, remotePath string, dryRun bool) error {
	task, err := NewTaskObj(task_RESUME, taskId, bucket, "", remotePath, dryRun)
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

// taskSetMaster looks up a task by taskId and if found sets task.master to true and returns a
// pointer to the task object (not a copy), else returns nil.
func (m *PauseServiceManager) taskSetMaster(taskId string) *taskObj {
	task := m.taskFind(taskId)
	if task != nil {
		task.taskMu.Lock()
		task.master = true
		task.taskMu.Unlock()
	}
	return task
}

// hasBucketUuid returns whether this task has the bucketUuid parameter.
func (this *taskObj) hasBucketUuid() bool {
	this.taskMu.RLock()
	defer this.taskMu.RUnlock()
	if this.taskType == task_PAUSE {
		return true
	}
	return false
}

// hasDryRun returns whether this task has the dryRun parameter.
func (this *taskObj) hasDryRun() bool {
	this.taskMu.RLock()
	defer this.taskMu.RUnlock()
	if this.taskType == task_RESUME {
		return true
	}
	return false
}

// postWithAuthWrapper wraps postWithAuth so it can be called as a goroutine for parallel scatter.
// Errors are logged with caller's logPrefix. bodyBuf contains JSON POST body. If anything fails
// this will mark the task as failed here on the master and also notify the workers of the failure.
func postWithAuthWrapper(logPrefix string, url string, bodyBuf *bytes.Buffer, task *taskObj) {

	resp, err := postWithAuth(url, "application/json", bodyBuf)
	if err != nil {
		err = fmt.Errorf("%v postWithAuth to url: %v returned error: %v", logPrefix, url, err)
		logging.Errorf(err.Error())
		task.pauser.failPause(logPrefix, "postWithAuth", err)
		return
	}
	defer resp.Body.Close()

	bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		err = fmt.Errorf("%v ReadAll(resp.Body) from url: %v returned error: %v", logPrefix,
			url, err)
		logging.Errorf(err.Error())
		task.pauser.failPause(logPrefix, "ReadAll(resp.Body)", err)
		return
	}

	var taskResponse TaskResponse
	err = json.Unmarshal(bytes, &taskResponse)
	if err != nil {
		err = fmt.Errorf("%v Unmarshal from url: %v returned error: %v", logPrefix, url, err)
		logging.Errorf(err.Error())
		task.pauser.failPause(logPrefix, "Unmarshal", err)
		return
	}

	// Check if taskResponse reports an error, which would be from GSI code, not HTTP
	if taskResponse.Code == RESP_ERROR {
		err = fmt.Errorf("%v TaskResponse from url: %v reports error: %v", logPrefix,
			url, taskResponse.Error)
		logging.Errorf(err.Error())
		task.pauser.failPause(logPrefix, "TaskResponse", err)
		return
	}
}

// TaskObjSetFailed sets task.status to status_FAILED and task.errMessage to errMsg.
func (this *taskObj) TaskObjSetFailed(errMsg string) {
	this.taskMu.Lock()
	this.errorMessage = errMsg
	this.taskStatus = status_FAILED
	this.taskMu.Unlock()
}

// taskObjToServiceTask creates the ns_server service.Task (cbauth/service/interface.go)
// representation of a taskObj.
func (this *taskObj) taskObjToServiceTask() *service.Task {
	this.taskMu.RLock()
	defer this.taskMu.RUnlock()

	nsTask := service.Task{
		Rev:          EncodeRev(0),
		ID:           this.taskId,
		Type:         service.TaskType(this.taskType.StringNs()),
		Status:       service.TaskStatus(this.taskStatus.StringNs()),
		IsCancelable: true,
		Progress:     this.progress,
		ErrorMessage: this.errorMessage,
		Extra:        make(map[string]interface{}),
	}

	// Add task parameters to service.Extra map for supportability (ns_server will ignore these)
	nsTask.Extra["bucket"] = this.bucket
	if this.hasBucketUuid() {
		nsTask.Extra["bucketUuid"] = this.bucketUuid
	}
	if this.hasDryRun() {
		nsTask.Extra["dryRun"] = this.dryRun
	}
	nsTask.Extra["archivePath"] = this.archivePath
	nsTask.Extra["archiveType"] = this.archiveType.String()
	nsTask.Extra["master"] = this.master

	return &nsTask
}