// @copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

import (
	"github.com/couchbase/indexing/secondary/logging"
)

////////////////////////////////////////////////////////////////////////////////////////////////////
// Resumer class - Perform the Resume of a given bucket (similar to Rebalancer's role).
// This is used only on the master node of a task_RESUME task to do the GSI orchestration.
////////////////////////////////////////////////////////////////////////////////////////////////////

// Resumer object holds the state of Resume orchestration
type Resumer struct {
	// otherIndexAddrs is "host:port" to all the known Index Service nodes EXCLUDING this one
	otherIndexAddrs []string

	// pauseMgr is the singleton parent of this object
	pauseMgr *PauseServiceManager

	// task is the task_RESUME task we are executing (protected by task.taskMu). It lives in the
	// pauseMgr.tasks map (protected by pauseMgr.tasksMu). Only the current object should change or
	// delete task at this point, but GetTaskList and other processing may concurrently read it.
	// Thus Resumer needs to write lock task.taskMu for changes but does not need to read lock it.
	task *taskObj
}

// RunResumer creates a Resumer instance to execute the given task. It saves a pointer to itself in
// task.pauser (visible to pauseMgr parent) and launches a goroutine for the work.
//   pauseMgr - parent object (singleton)
//   task - the task_RESUME task this object will execute
//   master - true iff this node is the master
func RunResumer(pauseMgr *PauseServiceManager, task *taskObj, master bool) {
	resumer := &Resumer{
		pauseMgr: pauseMgr,
		task:     task,
	}

	task.taskMu.Lock()
	task.resumer = resumer
	task.taskMu.Unlock()

	go resumer.run(master)
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Methods
////////////////////////////////////////////////////////////////////////////////////////////////////

// failResume logs an error using the caller's logPrefix and a provided context string and aborts
// the Resume task. If there is a set of known Indexer nodes, it will also try to notify them.
func (this *Resumer) failResume(logPrefix string, context string, error error) {
	logging.Errorf("%v Aborting Resume task %v due to %v error: %v", logPrefix,
		this.task.taskId, context, error)

	// Mark the task as failed directly here on master node (avoids dependency on loopback REST)
	this.task.TaskObjSetFailed(error.Error())

	// Notify other Index nodes
	this.pauseMgr.RestNotifyFailedTask(this.otherIndexAddrs, this.task, error.Error())
}

// run is a goroutine for the main body of Resume work for this.task.
//   master - true iff this node is the master
func (this *Resumer) run(master bool) {
	const _run = "Resumer::run:"

	// Get the list of Index node host:port addresses EXCLUDING this one
	this.otherIndexAddrs = this.pauseMgr.GetIndexerNodeAddresses(this.pauseMgr.httpAddr)

	// kjc implement Resume
}
