// @copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/manager"
	"github.com/couchbase/indexing/secondary/planner"
	"github.com/couchbase/plasma"
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
	// this.pauseMgr.RestNotifyFailedTask(this.otherIndexAddrs, this.task, error.Error())
}

// masterGenerateResumePlan: this method downloads all the metadata, stats from archivePath and
// plans which nodes resume indexes for given bucket
func (r *Resumer) masterGenerateResumePlan() (newNodes map[service.NodeID]service.NodeID, err error) {
	// Step 1: download PauseMetadata
	logging.Infof("Resumer::masterGenerateResumePlan: downloading pause metadata from %v for resume task ID: %v", r.task.archivePath, r.task.taskId)
	ctx := r.task.ctx
	plasmaCfg := plasma.DefaultConfig()

	copier := plasma.MakeFileCopier(r.task.archivePath, "", plasmaCfg.Environment, plasmaCfg.CopyConfig)
	if copier == nil {
		err = fmt.Errorf("object store not supported")
		logging.Errorf("Resumer::masterGenerateResumePlan: %v", err)
		return
	}

	data, err := copier.DownloadBytes(ctx, fmt.Sprintf("%v%v", r.task.archivePath, FILENAME_PAUSE_METADATA))
	if err != nil {
		logging.Errorf("Resumer::masterGenerateResumePlan: failed to download pause metadata err: %v for resume task ID: %v", err, r.task.taskId)
		return
	}
	data, err = common.ChecksumAndUncompress(data)
	if err != nil {
		logging.Errorf("Resumer::masterGenerateResumePlan: failed to read valid pause metadata err: %v for resume task ID: %v", err, r.task.taskId)
		return
	}
	pauseMetadata := new(PauseMetadata)
	err = json.Unmarshal(data, pauseMetadata)
	if err != nil {
		logging.Errorf("Resumer::masterGenerateResumePlan: couldn't unmarshal pause metadata err: %v for resume task ID: %v", err, r.task.taskId)
		return
	}

	// Step 2: Download metadata and stats for all nodes in pause metadata
	indexMetadataPerNode := make(map[service.NodeID]*planner.LocalIndexMetadata)
	statsPerNode := make(map[service.NodeID]map[string]interface{})

	var dWaiter sync.WaitGroup
	var dLock sync.Mutex
	dErrCh := make(chan error, len(pauseMetadata.Data))
	for nodeId := range pauseMetadata.Data {
		dWaiter.Add(1)
		go func(nodeId service.NodeID) {
			defer dWaiter.Done()

			nodeDir := fmt.Sprintf("%vnode_%v", r.task.archivePath, nodeId)
			indexMetadata, stats, err := r.downloadNodeMetadataAndStats(nodeDir)
			if err != nil {
				err = fmt.Errorf("couldn't get metadata and stats err: %v for nodeId %v, resume task ID: %v", err, nodeId, r.task.taskId)
				logging.Errorf("Resumer::masterGenerateResumePlan: %v", err)
				dErrCh <- err
				return
			}
			dLock.Lock()
			defer dLock.Unlock()
			indexMetadataPerNode[nodeId] = indexMetadata
			statsPerNode[nodeId] = stats
		}(nodeId)
	}
	dWaiter.Wait()
	close(dErrCh)

	var errStr strings.Builder
	for err := range dErrCh {
		errStr.WriteString(err.Error() + "\n")
	}
	if errStr.Len() != 0 {
		err = errors.New(errStr.String())
		return
	}

	// Step 3: get replacement node for old paused data
	resumeNodes := make([]*planner.IndexerNode,len(pauseMetadata.Data))
	config := r.pauseMgr.config.Load()
	clusterVersion := r.pauseMgr.genericMgr.cinfo.GetClusterVersion()
	// since we don't support mixed mode for pause resume, we can use the current server version
	// as the indexer version
	indexerVersion, err := r.pauseMgr.genericMgr.cinfo.GetServerVersion(
		r.pauseMgr.genericMgr.cinfo.GetCurrentNode(),
	)
	if err != nil {
		// we should never hit this err condition as we should always be able to read current node's
		// server version. if we do, should we fail here?
		logging.Warnf("Resumer::masterGenerateResumePlan: couldn't fetch this node's server version. hit unreachable error. err: %v for taskId %v",err, r.task.taskId)
		err = nil
		// use min indexer version required for Pause-Resume
		indexerVersion = common.INDEXER_72_VERSION
	}

	for nodeId := range pauseMetadata.Data {
		// Step 3a: generate IndexerNode for planner

		idxMetadata, ok := indexMetadataPerNode[nodeId]
		if !ok {
			err = fmt.Errorf("unable to read indexMetadata for node %v", nodeId)
			logging.Errorf("Resumer::masterGenerateResumePlan: %v", err)
			return
		}
		statsPerNode, ok := statsPerNode[nodeId]
		if !ok {
			err = fmt.Errorf("unable to read stats for node %v", nodeId)
			logging.Errorf("Resumer::masterGenerateResumePlan: %v", err)
			return
		}

		// TODO: replace with planner calls to generate []IndexerNode with []IndexUsage
		logging.Infof("Resumer::masterGenerateResumePlan: metadata and stats from node %v available. Total Idx definitions: %v, Total stats: %v for task ID: %v", nodeId, len(idxMetadata.IndexDefinitions), len(statsPerNode))

		indexerNode := planner.CreateIndexerNodeWithIndexes(string(nodeId),nil,nil)
		
		// Step 3b: populate IndexerNode with metadata
		var indexerUsage []*planner.IndexUsage
		indexerUsage, err = planner.ConvertToIndexUsages(config,idxMetadata,indexerNode,nil,
			nil)
		if err != nil {
			logging.Errorf("Resumer::masterGenerateResumePlan: couldn't generate index usage. err: %v for task ID: %v", err, r.task.taskId)
			return
		}

		indexerNode.Indexes = indexerUsage

		planner.SetStatsInIndexer(indexerNode,statsPerNode,clusterVersion,indexerVersion,config)

		resumeNodes = append(resumeNodes, indexerNode)
	}
	newNodes, err = StubExecuteTenantAwarePlanForResume(config["clusterAddr"].String(),resumeNodes)

	return
}

func (r *Resumer) downloadNodeMetadataAndStats(nodeDir string) (metadata *planner.LocalIndexMetadata, stats map[string]interface{}, err error) {
	defer func() {
		if err == nil {
			logging.Infof("Resumer::downloadNodeMetadataAndStats: successfully downloaded metadata and stats from %v", nodeDir)
		}
	}()

	ctx := r.task.ctx
	plasmaCfg := plasma.DefaultConfig()

	copier := plasma.MakeFileCopier(nodeDir, "", plasmaCfg.Environment, plasmaCfg.CopyConfig)
	if copier == nil {
		err = fmt.Errorf("object store not supported")
		logging.Errorf("Resumer::downloadNodeMetadataAndStats: %v", err)
		return
	}

	url, err := copier.GetPathEncoding(fmt.Sprintf("%v%v", nodeDir, FILENAME_METADATA))
	if err != nil {
		logging.Errorf("Resumer::downloadNodeMetadataAndStats: url encoding failed %v", err)
		return
	}
	data, err := copier.DownloadBytes(ctx, url)
	if err != nil {
		logging.Errorf("Resumer::downloadNodeMetadataAndStats: failed to download metadata err: %v", err)
		return
	}
	data, err = common.ChecksumAndUncompress(data)
	if err != nil {
		logging.Errorf("Resumer::downloadNodeMetadataAndStats: invalid metadata in object store err :%v", err)
		return
	}

	// NOTE: pause uploads manager.LocalIndexMetadata which has the similar fields as planner.LocalIndexMetadata
	mgrMetadata := new(manager.LocalIndexMetadata)
	err = json.Unmarshal(data, mgrMetadata)
	if err != nil {
		logging.Errorf("Resumer::downloadnodeMetadataAndStats: failed to unmarshal index metadata err: %v", err)
		return
	}
	metadata = manager.TransformMetaToPlannerMeta(mgrMetadata)

	url, err = copier.GetPathEncoding(fmt.Sprintf("%v%v", nodeDir, FILENAME_STATS))
	if err != nil {
		logging.Errorf("Resumer::downloadNodeMetadataAndStats: url encoding failed %v", err)
		return
	}
	data, err = copier.DownloadBytes(ctx, url)
	if err != nil {
		logging.Errorf("Resumer::downloadNodeMetadataAndStats: failed to download stats err: %v", err)
		return
	}
	data, err = common.ChecksumAndUncompress(data)
	if err != nil {
		logging.Errorf("Resumer::downloadNodeMetadataAndStats: invalid stats in object store err :%v", err)
		return
	}
	err = json.Unmarshal(data, &stats)
	if err != nil {
		logging.Errorf("Resumer::downloadnodeMetadataAndStats: failed to unmarshal stats err: %v", err)
	}

	return
}

func StubExecuteTenantAwarePlanForResume(clusterUrl string, resumeNodes []*planner.IndexerNode) (map[service.NodeID]service.NodeID, error) {
	logging.Infof("Resumer::StubExecuteTenantAwarePlanForResume: TODO: call actual planner")
	return nil, nil
}

// run is a goroutine for the main body of Resume work for this.task.
//   master - true iff this node is the master
func (this *Resumer) run(master bool) {
	const _run = "Resumer::run:"

	// Get the list of Index node host:port addresses EXCLUDING this one
	this.otherIndexAddrs = this.pauseMgr.GetIndexerNodeAddresses(this.pauseMgr.httpAddr)

	// kjc implement Resume
}
