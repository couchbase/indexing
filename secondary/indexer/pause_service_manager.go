// @copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

import (
	"fmt"
	"github.com/couchbase/cbauth/service"
	"net/http"
	"strings"
	"sync"

	"github.com/couchbase/indexing/secondary/logging"
)

////////////////////////////////////////////////////////////////////////////////////////////////////
// PauseServiceManager class
////////////////////////////////////////////////////////////////////////////////////////////////////

// pauseMgr points to the singleton of this class
var pauseMgr *PauseServiceManager

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
// genericMgr is a pointer to our parent.
// httpAddr gives the host:port of the local node for Index Service HTTP calls.
func NewPauseServiceManager(mux *http.ServeMux, genericMgr *GenericServiceManager,
	httpAddr string) *PauseServiceManager {

	m := &PauseServiceManager{
		genericMgr: genericMgr,
		httpAddr:   httpAddr,
		tasks:      make(map[string]*taskObj),
	}

	// Save the singleton
	pauseMgr = m

	// Unit test REST APIs -- THESE MUST STILL DO AUTHENTICATION!!
	mux.HandleFunc("/test/Pause", pauseMgr.testPause)
	mux.HandleFunc("/test/PreparePause", pauseMgr.testPreparePause)
	mux.HandleFunc("/test/PrepareResume", pauseMgr.testPrepareResume)
	mux.HandleFunc("/test/Resume", pauseMgr.testResume)

	return m
}

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
	case bst_PAUSING:
		return "Pausing"
	case bst_PAUSED:
		return "Paused"
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

	task_PREPARE_PAUSE
	task_PAUSE

	task_PREPARE_RESUME
	task_RESUME
)

// String converter for taskEnum type.
func (this taskEnum) String() string {
	switch this {
	case task_NIL:
		return "NilTask"
	case task_PREPARE_PAUSE:
		return "PreparePause"
	case task_PAUSE:
		return "Pause"
	case task_PREPARE_RESUME:
		return "PrepareResume"
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
	case task_PREPARE_PAUSE:
		return "task-prepare-pause" // kjc not defined in interface.go yet
	case task_PAUSE:
		return "task-pause" // kjc not defined in interface.go yet
	case task_PREPARE_RESUME:
		return "task-prepare-resume" // kjc not defined in interface.go yet
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

// archiveEnum defines types of storage archives for Pause-Resume (following archive_XXX constants)
type archiveEnum int

const (
	archive_NIL  archiveEnum = iota // undefined
	archive_AZ                      // Azure Cloud Storage
	archive_FILE                    // local filesystem
	archive_GS                      // Google Cloud Storage
	archive_S3                      // AWS S3 bucket
)

// String converter for archiveEnum type.
func (this archiveEnum) String() string {
	switch this {
	case archive_NIL:
		return "NilArchive"
	case archive_AZ:
		return "Azure"
	case archive_FILE:
		return "File"
	case archive_GS:
		return "Google"
	case archive_S3:
		return "S3"
	default:
		return fmt.Sprintf("undefinedArchiveEnum_%v", int(this))
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
	taskType     taskEnum   // local analog of service.TaskType
	taskId       string     // opaque ns_server unique ID for this task
	taskStatus   statusEnum // local analog of service.TaskStatus
	progress     float64    // completion progress [0.0, 1.0]
	errorMessage string     // only if a failure occurred

	bucket     string // bucket name being Paused or Resumed
	bucketUuid string // Pause: UUID of bucket; Resume: ""
	dryRun     bool   // is task for Resume dry run (cluster capacity check)?

	// archiveDir gives the top-level "directory" to use to write/read Pause/Resume images. For:
	//   S3 format:       "s3://<s3_bucket>/index/"
	//   Local FS format: "/absolute/path/" or "relative/path/"
	// The trailing slash is always present. For FS the original "file://" prefix has been removed.
	archiveDir  string
	archiveType archiveEnum // type of storage archive used for this task
}

// NewTaskObj is the constructor for the taskObj class. If the parameters are not valid, it will
// return (nil, error) rather than create an unsupported taskObj.
func NewTaskObj(taskType taskEnum, taskId, bucket, bucketUuid, remotePath string, dryRun bool,
) (*taskObj, error) {

	archiveType, archiveDir, err := archiveInfoFromRemotePath(remotePath)
	if err != nil {
		return nil, err
	}
	return &taskObj{
		taskType:    taskType,
		taskId:      taskId,
		taskStatus:  status_RUNNING,
		bucket:      bucket,
		bucketUuid:  bucketUuid,
		dryRun:      dryRun,
		archiveDir:  archiveDir,
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

	// kjc Test ability to access the target archive

	// Record the task in progress
	return m.taskAddPreparePause(taskId, bucket, bucketUuid, remotePath)
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

	// Set bst_PAUSING state
	priorBucketState := m.bucketStateSetIfNil(bucket, bst_PAUSING)
	if priorBucketState != bst_NIL {
		err = service.ErrConflict
		logging.Errorf("%v Cannot pause bucket %v as it already has Pause-Resume state %v", _Pause,
			bucket, priorBucketState)
		return err
	}

	// kjc implement Pause

	// Record the task in progress
	err = m.taskAddPause(taskId, bucket, bucketUuid, remotePath)
	return err
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

	// kjc Test ability to access the target archive

	// Record the task in progress
	return m.taskAddPrepareResume(taskId, bucket, remotePath, dryRun)
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

	// Set bst_RESUMING state
	priorBucketState := m.bucketStateSetIfNil(bucket, bst_RESUMING)
	if priorBucketState != bst_NIL {
		err = service.ErrConflict
		logging.Errorf("%v Cannot resume bucket %v as it already has Pause-Resume state %v", _Resume,
			bucket, priorBucketState)
		return err
	}

	// kjc implement Resume

	// Record the task in progress
	err = m.taskAddResume(taskId, bucket, remotePath, dryRun)
	return err
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
	taskClone, found := m.taskFind(id)
	if !found {
		return service.ErrNotFound
	}

	switch taskClone.taskType {
	case task_PREPARE_PAUSE:
		return m.CancelPreparePause(&taskClone)
	case task_PAUSE:
		return m.CancelPause(&taskClone)
	case task_PREPARE_RESUME:
		return m.CancelPrepareResume(&taskClone)
	case task_RESUME:
		return m.CancelResume(&taskClone)
	default:
		return service.ErrNotSupported
	}
}

// CancelPreparePause cancels a local PreparePause task.
func (m *PauseServiceManager) CancelPreparePause(taskClone *taskObj) error {
	const _CancelPreparePause = "PauseServiceManager::CancelPreparePause:"
	logging.Infof("%v canceling PreparePause taskId: %v, task: %+v", _CancelPreparePause,
		taskClone.taskId, taskClone)

	found := m.taskDelete(taskClone.taskId)
	if !found {
		// Something else deleted it since caller found and cloned it
		logging.Infof("%v taskId: %v not found (already finished or canceled)", _CancelPreparePause,
			taskClone.taskId)
		return service.ErrNotFound
	}
	return nil
}

// CancelPause cancels a local Pause task.
func (m *PauseServiceManager) CancelPause(taskClone *taskObj) error {
	return nil // kjc implement CancelPause
}

// CancelPrepareResume cancels a local PrepareResume task.
func (m *PauseServiceManager) CancelPrepareResume(taskClone *taskObj) error {
	const _CancelPrepareResume = "PauseServiceManager::CancelPrepareResume:"
	logging.Infof("%v canceling PrepareResume taskId: %v, task: %+v", _CancelPrepareResume,
		taskClone.taskId, taskClone)

	found := m.taskDelete(taskClone.taskId)
	if !found {
		// Something else deleted it since caller found and cloned it
		logging.Infof("%v taskId: %v not found (already finished or canceled)", _CancelPrepareResume,
			taskClone.taskId)
		return service.ErrNotFound
	}
	return nil
}

// CancelResume cancels a local Resume task.
func (m *PauseServiceManager) CancelResume(taskClone *taskObj) error {
	return nil // kjc implement CancelResume
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Handlers for unit test REST APIs (/test/methodName) -- MUST STILL DO AUTHENTICATION!!
////////////////////////////////////////////////////////////////////////////////////////////////////

// testPreparePause handles unit test REST API "/test/PreparePause" by calling the PreparePause API
// directly, which is normally called by ns_server via cbauth RPC.
func (m *PauseServiceManager) testPreparePause(w http.ResponseWriter, r *http.Request) {
	const _testPreparePause = "PauseServiceManager::testPreparePause:"

	m.testPauseOrResume(w, r, _testPreparePause, task_PREPARE_PAUSE)
}

// testPause handles unit test REST API "/test/Pause" by calling the Pause API directly, which is
// normally called by ns_server via cbauth RPC.
func (m *PauseServiceManager) testPause(w http.ResponseWriter, r *http.Request) {
	const _testPause = "PauseServiceManager::testPause:"

	m.testPauseOrResume(w, r, _testPause, task_PAUSE)
}

// testPrepareResume handles unit test REST API "/test/PrepareResume" by calling the PreparePause
// API directly, which is normally called by ns_server via cbauth RPC.
func (m *PauseServiceManager) testPrepareResume(w http.ResponseWriter, r *http.Request) {
	const _testPrepareResume = "PauseServiceManager::testPrepareResume:"

	m.testPauseOrResume(w, r, _testPrepareResume, task_PREPARE_RESUME)
}

// testResume handles unit test REST API "/test/Resume" by calling the Resume API directly, which is
// normally called by ns_server via cbauth RPC.
func (m *PauseServiceManager) testResume(w http.ResponseWriter, r *http.Request) {
	const _testResume = "PauseServiceManager::testResume:"

	m.testPauseOrResume(w, r, _testResume, task_RESUME)
}

// testPauseOrResume is the delegate of testPreparePause, testPause, testPrepareResume, testResume
// since their logic differs only in a few parameters and which API they call.
func (m *PauseServiceManager) testPauseOrResume(w http.ResponseWriter, r *http.Request,
	logPrefix string, taskType taskEnum) {

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
	case task_PREPARE_PAUSE:
		err = m.PreparePause(id, bucket, bucketUuid, remotePath)
	case task_PAUSE:
		err = m.Pause(id, bucket, bucketUuid, remotePath)
	case task_PREPARE_RESUME:
		err = m.PrepareResume(id, bucket, remotePath, dryRun)
	case task_RESUME:
		err = m.Resume(id, bucket, remotePath, dryRun)
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

// bucketStateSetIfNil sets m.bucketState to the state passed in iff the bucket had no state, else
// it does nothing. It returns the prior state, which is bst_NIL if it did set the new state.
func (m *PauseServiceManager) bucketStateSetIfNil(bucket string, state bucketStateEnum) (
	priorState bucketStateEnum) {

	m.bucketStatesMu.Lock()
	defer m.bucketStatesMu.Unlock()
	priorState = m.bucketStates[bucket]
	if priorState == bst_NIL {
		m.bucketStates[bucket] = state
	}
	return priorState
}

// taskAdd adds a task to m.tasks.
func (m *PauseServiceManager) taskAdd(task *taskObj) {
	m.tasksMu.Lock()
	m.tasks[task.taskId] = task
	m.tasksMu.Unlock()
}

// taskAddPreparePause constructs and adds a PreparePause task to m.tasks.
func (m *PauseServiceManager) taskAddPreparePause(taskId, bucket, bucketUuid, remotePath string,
) error {

	task, err := NewTaskObj(task_PREPARE_PAUSE, taskId, bucket, bucketUuid, remotePath, false)
	if err != nil {
		return err
	}
	m.taskAdd(task)
	return nil
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

// taskAddPrepareResume constructs and adds a PrepareResume task to m.tasks.
func (m *PauseServiceManager) taskAddPrepareResume(taskId, bucket, remotePath string,
	dryRun bool) error {

	task, err := NewTaskObj(task_PREPARE_RESUME, taskId, bucket, "", remotePath, dryRun)
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

// taskDelete deletes a task by taskId, returning true if it was found and false otherwise (no-op).
func (m *PauseServiceManager) taskDelete(taskId string) bool {
	m.tasksMu.Lock()
	defer m.tasksMu.Unlock()
	_, found := m.tasks[taskId]
	if found {
		delete(m.tasks, taskId)
	}
	return found
}

// findAndCloneTask looks up a task by taskId and if found returns a clone of it and found = true,
// else returns found = false.
func (m *PauseServiceManager) taskFind(taskId string) (taskClone taskObj, found bool) {
	m.tasksMu.RLock()
	defer m.tasksMu.RUnlock()
	task, found := m.tasks[taskId]
	if found {
		taskClone = *task
	}
	return taskClone, found
}

// archiveInfoFromRemotePath returns the archive type and directory from the remotePath from
// either ns_server or test code, or logs and returns an error if the type is missing or
// unrecognized. The type is determined by the remotePath prefix:
//   file:// - local filesystem path; used in tests
//   s3://   - AWS S3 bucket and path; used in production
// In all cases this will append a trailing "/" if one is not present. For local filesystem, the
// "file://" prefix will be removed from the returned archiveDir, so it works as a regular path:
//   "file://foo/bar" becomes relative path "foo/bar/"
//   "file:///foo/bar" becomes absolute path "/foo/bar/"
func archiveInfoFromRemotePath(remotePath string) (
	archiveType archiveEnum, archiveDir string, err error) {
	const _archiveInfoFromRemotePath = "PauseServiceManager::archiveInfoFromRemotePath:"

	const (
		PREFIX_AZ   = "az://"   // Azure Cloud Storage target
		PREFIX_FILE = "file://" // local filesystem target
		PREFIX_GS   = "gs://"   // Google Cloud Storage target
		PREFIX_S3   = "s3://"   // AWS S3 target
	)

	// Check for valid archive type
	hasPrefix := true
	if strings.HasPrefix(remotePath, PREFIX_AZ) {
		archiveType = archive_AZ
	} else if strings.HasPrefix(remotePath, PREFIX_FILE) {
		archiveType = archive_FILE
	} else if strings.HasPrefix(remotePath, PREFIX_GS) {
		archiveType = archive_GS
	} else if strings.HasPrefix(remotePath, PREFIX_S3) {
		archiveType = archive_S3
	} else { // treat as FILE, which does not require the prefix
		hasPrefix = false
		archiveType = archive_FILE
	}

	// Ensure there is more than just the archive type prefix
	if (!hasPrefix && len(remotePath) == 0) ||
		hasPrefix && ((archiveType == archive_AZ && len(remotePath) <= len(PREFIX_AZ)) ||
			(archiveType == archive_FILE && len(remotePath) <= len(PREFIX_FILE)) ||
			(archiveType == archive_GS && len(remotePath) <= len(PREFIX_GS)) ||
			(archiveType == archive_S3 && len(remotePath) <= len(PREFIX_S3))) {
		err = fmt.Errorf("%v Missing path body in remotePath '%v'", _archiveInfoFromRemotePath, remotePath)
		logging.Errorf(err.Error())
		return archive_NIL, "", err
	}

	// Ensure there is a trailing slash
	if strings.HasSuffix(remotePath, "/") {
		archiveDir = remotePath
	} else {
		archiveDir = remotePath + "/"
	}

	// For archive_FILE, strip off "file://" prefix
	if hasPrefix && archiveType == archive_FILE {
		archiveDir = strings.Replace(archiveDir, PREFIX_FILE, "", 1)
	}

	return archiveType, archiveDir, nil
}

// hasBucketUuid returns whether this task has the bucketUuid parameter.
func (this *taskObj) hasBucketUuid() bool {
	if this.taskType == task_PREPARE_PAUSE || this.taskType == task_PAUSE {
		return true
	}
	return false
}

// hasDryRun returns whether this task has the dryRun parameter.
func (this *taskObj) hasDryRun() bool {
	if this.taskType == task_PREPARE_RESUME || this.taskType == task_RESUME {
		return true
	}
	return false
}

// taskObjToServiceTask creates the ns_server service.Task (cbauth/service/interface.go)
// representation of a taskObj.
func (this *taskObj) taskObjToServiceTask() *service.Task {
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
	nsTask.Extra["archiveDir"] = this.archiveDir
	nsTask.Extra["archiveType"] = this.archiveType.String()

	return &nsTask
}
