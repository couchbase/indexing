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

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/manager"
)

////////////////////////////////////////////////////////////////////////////////////////////////////
// Pauser class - Perform the Pause of a given bucket (similar to Rebalancer's role).
// This is used only on the master node of a task_PAUSE task to do the GSI orchestration.
////////////////////////////////////////////////////////////////////////////////////////////////////

// Pauser object holds the state of Pause orchestration
type Pauser struct {
	// nodeDir is "node_<nodeId>/" for this node, where nodeId is the 32-digit hex ID from ns_server
	nodeDir string

	// otherIndexAddrs is "host:port" to all the known Index Service nodes EXCLUDING this one
	otherIndexAddrs []string

	// pauseMgr is the singleton parent of this object
	pauseMgr *PauseServiceManager

	// task is the task_PAUSE task we are executing (protected by task.taskMu). It lives in the
	// pauseMgr.tasks map (protected by pauseMgr.tasksMu). Only the current object should change or
	// delete task at this point, but GetTaskList and other processing may concurrently read it.
	// Thus Pauser needs to write lock task.taskMu for changes but does not need to read lock it.
	task *taskObj
}

// RunPauser creates a Pauser instance to execute the given task. It saves a pointer to itself in
// task.pauser (visible to pauseMgr parent) and launches a goroutine for the work.
//   pauseMgr - parent object (singleton)
//   task - the task_PAUSE task this object will execute
//   master - true iff this node is the master
func RunPauser(pauseMgr *PauseServiceManager, task *taskObj, master bool) {
	pauser := &Pauser{
		pauseMgr: pauseMgr,
		task:     task,
		nodeDir:  "node_" + string(pauseMgr.genericMgr.nodeInfo.NodeID) + "/",
	}

	task.taskMu.Lock()
	task.pauser = pauser
	task.taskMu.Unlock()

	go pauser.run(master)
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Methods
////////////////////////////////////////////////////////////////////////////////////////////////////

// restGetLocalIndexMetadataBinary calls the /getLocalndexMetadata REST API (request_handler.go) via
// self-loopback to get the index metadata for the current node and the task's bucket (tenant). This
// verifies it can be unmarshaled, but it returns a checksummed and optionally compressed byte slice
// version of the data rather than the unmarshaled object.
func (this *Pauser) restGetLocalIndexMetadataBinary(compress bool) ([]byte, error) {
	const _restGetLocalIndexMetadataBinary = "Pauser::restGetLocalIndexMetadataBinary:"

	url := fmt.Sprintf("%v/getLocalIndexMetadata?useETag=false&bucket=%v",
		this.pauseMgr.httpAddr, this.task.bucket)
	resp, err := getWithAuth(url)
	if err != nil {
		this.failPause(_restGetLocalIndexMetadataBinary, fmt.Sprintf("getWithAuth(%v)", url), err)
		return nil, err
	}
	defer resp.Body.Close()

	byteSlice, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		this.failPause(_restGetLocalIndexMetadataBinary, "ReadAll(resp.Body)", err)
		return nil, err
	}

	// Verify response can be unmarshaled
	metadata := new(manager.LocalIndexMetadata)
	err = json.Unmarshal(byteSlice, metadata)
	if err != nil {
		this.failPause(_restGetLocalIndexMetadataBinary, "Unmarshal localMeta", err)
		return nil, err
	}
	if len(metadata.IndexDefinitions) == 0 {
		return nil, nil
	}

	// Return checksummed and optionally compressed byte slice, not the unmarshaled object
	return common.ChecksumAndCompress(byteSlice, compress), nil
}

// failPause logs an error using the caller's logPrefix and a provided context string and aborts the
// Pause task. If there is a set of known Indexer nodes, it will also try to notify them.
func (this *Pauser) failPause(logPrefix string, context string, error error) {
	logging.Errorf("%v Aborting Pause task %v due to %v error: %v", logPrefix,
		this.task.taskId, context, error)

	// Mark the task as failed directly here on master node (avoids dependency on loopback REST)
	this.task.TaskObjSetFailed(error.Error())

	// Notify other Index nodes
	this.pauseMgr.RestNotifyFailedTask(this.otherIndexAddrs, this.task, error.Error())
}

// run is a goroutine for the main body of Pause work for this.task.
//   master - true iff this node is the master
func (this *Pauser) run(master bool) {
	const _run = "Pauser::run:"

	// Get the list of Index node host:port addresses EXCLUDING this one
	this.otherIndexAddrs = this.pauseMgr.GetIndexerNodeAddresses(this.pauseMgr.httpAddr)

	var byteSlice []byte
	var err error
	reader := bytes.NewReader(nil)

	/////////////////////////////////////////////
	// Work done by master only
	/////////////////////////////////////////////

	if master {
		// Write the version.json file to the archive
		byteSlice = []byte(fmt.Sprintf("{\"version\":%v}\n", ARCHIVE_VERSION))
		reader.Reset(byteSlice)
		err = Upload(this.task.archivePath, FILENAME_VERSION, reader)
		if err != nil {
			this.failPause(_run, "Upload "+FILENAME_VERSION, err)
			return
		}

		// Notify the followers to start working on this task
		this.pauseMgr.RestNotifyPause(this.otherIndexAddrs, this.task)
	} // if master

	/////////////////////////////////////////////
	// Work done by both master and followers
	/////////////////////////////////////////////

	// nodePath is the path to the node-specific archive subdirectory for the current node
	nodePath := this.task.archivePath + this.nodeDir

	// Get the index metadata from all nodes and write it as a single file to the archive
	byteSlice, err = this.restGetLocalIndexMetadataBinary(true)
	if err != nil {
		this.failPause(_run, "getLocalInstanceMetadata", err)
		return
	}
	if byteSlice == nil {
		// there are no indexes on this node for bucket. pause is a no-op
		logging.Infof("Pauser::run pause is a no-op for bucket %v-%v", this.task.bucket, this.task.bucketUuid)
		return
	}
	reader.Reset(byteSlice)
	err = Upload(nodePath, FILENAME_METADATA, reader)
	if err != nil {
		this.failPause(_run, "Upload "+FILENAME_METADATA, err)
		return
	}

	// Write the persistent stats to the archive
	byteSlice, err = this.pauseMgr.genericMgr.statsMgr.GetStatsToBePersistedBinary(true)
	if err != nil {
		this.failPause(_run, "GetStatsToBePersistedBinary", err)
		return
	}
	reader.Reset(byteSlice)
	err = Upload(nodePath, FILENAME_STATS, reader)
	if err != nil {
		this.failPause(_run, "Upload "+FILENAME_STATS, err)
		return
	}

	// kjc implement Pause
}
