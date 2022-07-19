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

	// tasks is the current list of Pause-Resume tasks that are running, if any
	tasks      []*taskObj
	tasksMutex sync.RWMutex // protects tasks
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
	}

	// Save the singleton
	pauseMgr = m

	// Unit test REST APIs -- THESE MUST STILL DO AUTHENTICATION!!
	mux.HandleFunc("/test/Pause", pauseMgr.testPause)
	mux.HandleFunc("/test/Resume", pauseMgr.testResume)

	return m
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Enums
////////////////////////////////////////////////////////////////////////////////////////////////////

// taskEnum defines types of tasks (following task_XXX constants)
type taskEnum int

const (
	task_NIL taskEnum = iota // undefined
	task_PAUSE
	task_RESUME
)

// archiveEnum defines types of storage archives for Pause-Resume (following archive_XXX constants)
type archiveEnum int

const (
	archive_NIL  archiveEnum = iota // undefined
	archive_FILE                    // local filesystem
	archive_S3                      // AWS S3 bucket
)

////////////////////////////////////////////////////////////////////////////////////////////////////
// Type definitions
////////////////////////////////////////////////////////////////////////////////////////////////////

// taskObj represents one Pause or Resume task
type taskObj struct {
	// archiveDir gives the top-level "directory" to use to write/read Pause/Resume images. For S3
	// this will be of the form "s3://<s3_bucket>/index/"
	archiveDir string

	archiveType archiveEnum // type of storage archive used for this task
	bucket      string      // bucket name being Paused or Resumed
	taskId      string      // opaque ns_server unique ID for this task
	taskType    taskEnum    // whether this task is Pause or Resume
}

// NewTaskObj is the constructor for the taskObj class. If the parameters are not valid, it will
// return (nil, error) rather than create an unsupported taskObj.
func NewTaskObj(taskId string, taskType taskEnum, bucket string, archiveRoot string,
) (*taskObj, error) {

	archiveType, archiveDir, err := archiveInfoFromRoot(archiveRoot)
	if err != nil {
		return nil, err
	}
	return &taskObj{
		archiveDir:  archiveDir,
		archiveType: archiveType,
		bucket:      bucket,
		taskId:      taskId,
		taskType:    taskType,
	}, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//
//  Implementation of Pause-Resume-specific APIs of the service.Manager interface
//  defined in cbauth/service/interface.go.
//
////////////////////////////////////////////////////////////////////////////////////////////////////

// Pause is an external API called by ns_server (via cbauth) only on the GSI master node to initiate
// a pause (formerly hibernate) of a bucket in the serverless offering.
//   taskId - opaque ns_server unique ID for this task
//   bucket - name of the bucket to pause
//   archiveRoot - root of cloud storage (e.g. S3 bucket) into which to write the image
func (m *PauseServiceManager) Pause(taskId string, bucket string, archiveRoot string) error {
	const _Pause = "PauseServiceManager::Pause:"

	logging.Infof("%v Called. bucket: %v, archiveRoot: %v", _Pause, bucket, archiveRoot)

	// kjc implement
	//error := m.PauseSanityChecks(bucket, archiveRoot)
	//if error != nil {
	//	return error
	//}

	// Record the task in progress
	task, err := NewTaskObj(taskId, task_PAUSE, bucket, archiveRoot)
	if err != nil {
		return err
	}
	m.tasksMutex.Lock()
	m.tasks = append(m.tasks, task)
	m.tasksMutex.Unlock()
	return nil
}

// Resume is an external API called by ns_server (via cbauth) only on the GSI master node to
// initiate a resume (formerly unhibernate or rehydrate) of a bucket in the serverless offering.
//   taskId - opaque ns_server unique ID for this task
//   bucket - name of the bucket to resume
//   archiveRoot - root of cloud storage (e.g. S3 bucket) from which to read the image
func (m *PauseServiceManager) Resume(taskId string, bucket string, archiveRoot string) error {
	const _Resume = "PauseServiceManager::Resume:"

	logging.Infof("%v Called. bucket: %v, archiveRoot: %v", _Resume, bucket, archiveRoot)

	// kjc implement
	//error := m.ResumeSanityChecks(bucket, archiveRoot)
	//if error != nil {
	//	return error
	//}

	// Record the task in progress
	task, err := NewTaskObj(taskId, task_RESUME, bucket, archiveRoot)
	if err != nil {
		return err
	}
	m.tasksMutex.Lock()
	m.tasks = append(m.tasks, task)
	m.tasksMutex.Unlock()
	return nil
}

// PauseCancelTask is a delegate of GenericServiceManager.GetTaskList which is an external API
// called by ns_server (via cbauth) to cancel an ongoing Pause or Resume.
// kjc implement and add delegation in GenericServiceManager.CancelTask

// PauseGetTaskList is a delegate of GenericServiceManager.GetTaskList which is an external API
// called by ns_server (via cbauth) to retrieve a progress update on an ongoing Pause or Resume.
// kjc implement and add delegation in GenericServiceManager.GetTaskList

////////////////////////////////////////////////////////////////////////////////////////////////////
// Handlers for unit test REST APIs (/test/methodName) -- MUST STILL DO AUTHENTICATION!!
////////////////////////////////////////////////////////////////////////////////////////////////////

// testPause handles unit test REST API "/test/Pause" by calling
// pause_service_manager.go Pause, which is normally called by ns_server via cbauth RPC.
func (m *PauseServiceManager) testPause(w http.ResponseWriter, r *http.Request) {
	const _testPause = "PauseServiceManager::testPause:"

	m.testPauseOrResume(w, r, _testPause, "Pause")
}

// testResume handles unit test REST API "/test/Resume" by calling
// pause_service_manager.go Resume, which is normally called by ns_server via cbauth RPC.
func (m *PauseServiceManager) testResume(w http.ResponseWriter, r *http.Request) {
	const _testResume = "PauseServiceManager::testResume:"

	m.testPauseOrResume(w, r, _testResume, "Resume")
}

// testPauseOrResume is the delegate of testPause and testResume, since their logic differs only in
// which API they call.
func (m *PauseServiceManager) testPauseOrResume(w http.ResponseWriter, r *http.Request,
	logPrefix string, funcName string) {

	logging.Infof("%v called", logPrefix)
	defer logging.Infof("%v returned", logPrefix)

	// Authenticate
	_, ok := doAuth(r, w, logPrefix)
	if !ok {
		return
	}

	// Required parameters
	taskId := r.FormValue("taskId")
	bucket := r.FormValue("bucket")
	archiveRoot := r.FormValue("archiveRoot") // e.g. S3 bucket or filesystem path

	var err error
	if funcName == "Pause" {
		err = m.Pause(taskId, bucket, archiveRoot)
	} else { // Resume
		err = m.Resume(taskId, bucket, archiveRoot)
	}
	if err == nil {
		resp := &TaskResponse{Code: RESP_SUCCESS, TaskId: taskId}
		rhSend(http.StatusOK, w, resp)
		return
	}
	err = fmt.Errorf("%v %v RPC returned error: %v", logPrefix, funcName, err)
	resp := &TaskResponse{Code: RESP_ERROR, Error: err.Error()}
	rhSend(http.StatusInternalServerError, w, resp)
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// General methods and functions
////////////////////////////////////////////////////////////////////////////////////////////////////

// archiveInfoFromRoot returns the archive type and archive directory from the archive root from
// either ns_server or test code, or logs and returns an error if the type is missing or
// unrecognized. The type is determined by the archiveRoot prefix:
//   file:// - local filesystem path; used in tests
//   s3://   - AWS S3 bucket and path; used in production
// In all cases this will append a trailing "/" if one is not present. For local filesystem, the
// "file://" prefix will be removed from the returned archiveDir, so it works as a regular path:
//   "file://foo/bar" becomes relative path "foo/bar/"
//   "file:///foo/bar" becomes absolute path "/foo/bar/"
func archiveInfoFromRoot(archiveRoot string) (
	archiveType archiveEnum, archiveDir string, err error) {
	const _archiveInfoFromRoot = "PauseServiceManager::archiveInfoFromRoot:"

	const PREFIX_FILE = "file://" // prefix for local filesystem target
	const PREFIX_S3 = "s3://"     // prefix for AWS S3 target

	// Check for valid archive type
	if strings.HasPrefix(archiveRoot, PREFIX_FILE) {
		archiveType = archive_FILE
	} else if strings.HasPrefix(archiveRoot, PREFIX_S3) {
		archiveType = archive_S3
	} else { // missing or unrecognized archive type
		err = fmt.Errorf("%v Missing or unrecognized archive type prefix in archiveRoot: %v", _archiveInfoFromRoot, archiveRoot)
		logging.Errorf(err.Error())
		return archive_NIL, "", err
	}

	// Ensure there is more than just the archive type prefix
	if (archiveType == archive_FILE && len(archiveRoot) <= len(PREFIX_FILE)) ||
		(archiveType == archive_S3 && len(archiveRoot) <= len(PREFIX_S3)) {
		err = fmt.Errorf("%v Missing path body in archiveRoot: %v", _archiveInfoFromRoot, archiveRoot)
		logging.Errorf(err.Error())
		return archive_NIL, "", err
	}

	// Ensure there is a trailing slash
	if strings.HasSuffix(archiveRoot, "/") {
		archiveDir = archiveRoot
	} else {
		archiveDir = archiveRoot + "/"
	}

	// For archive_FILE, strip off "file://" prefix
	if archiveType == archive_FILE {
		archiveDir = strings.Replace(archiveDir, PREFIX_FILE, "", 1)
	}

	return archiveType, archiveDir, nil
}
