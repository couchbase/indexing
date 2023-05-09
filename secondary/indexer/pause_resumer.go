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
	"time"

	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/cbauth/service"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	l "github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/manager"
	"github.com/couchbase/indexing/secondary/manager/client"
	"github.com/couchbase/indexing/secondary/planner"
	"github.com/couchbase/plasma"
)

func newResumeDownloadToken(masterUuid, followerUuid, resumeId, bucketName, uploaderId string) (
	string, *c.ResumeDownloadToken, error) {

	rdt := &c.ResumeDownloadToken{
		MasterId:   masterUuid,
		FollowerId: followerUuid,
		ResumeId:   resumeId,
		State:      c.ResumeDownloadTokenPosted,
		BucketName: bucketName,
		UploaderId: uploaderId,
	}

	ustr, err := c.NewUUID()
	if err != nil {
		logging.Warnf("newResumeDownloadToken: Failed to generate uuid: err[%v]", err)
		return "", nil, err
	}

	rdtId := fmt.Sprintf("%s%s", c.ResumeDownloadTokenTag, ustr.Str())

	return rdtId, rdt, nil
}

func decodeResumeDownloadToken(path string, value []byte) (string, *c.ResumeDownloadToken, error) {

	rdtIdPos := strings.Index(path, c.ResumeDownloadTokenTag)
	if rdtIdPos < 0 {
		return "", nil, fmt.Errorf("ResumeDownloadTokenTag[%v] not present in metakv path[%v]",
			c.ResumeDownloadTokenTag, path)
	}

	rdtId := path[rdtIdPos:]

	rdt := &c.ResumeDownloadToken{}
	err := json.Unmarshal(value, rdt)
	if err != nil {
		logging.Errorf("decodeResumeDownloadToken: Failed to unmarshal value[%s] path[%v]: err[%v]",
			string(value), path, err)
		return "", nil, err
	}

	return rdtId, rdt, nil
}

func setResumeDownloadTokenInMetakv(rdtId string, rdt *c.ResumeDownloadToken) {

	rhCb := func(r int, err error) error {
		if r > 0 {
			logging.Warnf("setResumeDownloadTokenInMetakv::rhCb: err[%v], Retrying[%d]", err, r)
		}

		return c.MetakvSet(PauseMetakvDir+rdtId, rdt)
	}

	rh := c.NewRetryHelper(10, time.Second, 1, rhCb)
	err := rh.Run()

	if err != nil {
		logging.Fatalf("setResumeDownloadTokenInMetakv: Failed to set ResumeDownloadToken In Meta Storage:"+
			" rdtId[%v] rdt[%v] err[%v]", rdtId, rdt, err)
		c.CrashOnError(err)
	}
}

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

	// Channels used for signalling
	// Used to signal that the ResumeDownloadTokens have been published.
	waitForTokenPublish chan struct{}
	// Used to signal metakv observer to stop
	metakvCancel chan struct{}

	metakvMutex sync.RWMutex
	wg          sync.WaitGroup

	// Global token associated with this Resume task
	pauseToken *PauseToken

	// in-memory bookkeeping for observed tokens
	masterTokens, followerTokens map[string]*c.ResumeDownloadToken

	// lock protecting access to maps like masterTokens and followerTokens
	mu sync.RWMutex

	// For cleanup
	retErr      error
	cleanupOnce sync.Once
	cb          PauseResumeCallbacks

	// For progress tracking
	masterProgress, followerProgress float64Holder

	// for cleanup after resume
	shardIds []c.ShardId
}

// NewResumer creates a Resumer instance to execute the given task. It saves a pointer to itself in
// task.pauser (visible to pauseMgr parent) and launches a goroutine for the work.
//
//	pauseMgr - parent object (singleton)
//	task - the task_RESUME task this object will execute
//	pauseToken - global master PauseToken
//	doneCb - callback that initiates the cleanup phase
func NewResumer(pauseMgr *PauseServiceManager, task *taskObj, pauseToken *PauseToken,
	doneCb PauseResumeDoneCallback, progressCb PauseResumeProgressCallback) *Resumer {

	resumer := &Resumer{
		pauseMgr: pauseMgr,
		task:     task,

		waitForTokenPublish: make(chan struct{}),
		metakvCancel:        make(chan struct{}),
		pauseToken:          pauseToken,

		masterTokens:   make(map[string]*c.ResumeDownloadToken),
		followerTokens: make(map[string]*c.ResumeDownloadToken),

		cb: PauseResumeCallbacks{done: doneCb, progress: progressCb},

		masterProgress:   float64Holder{},
		followerProgress: float64Holder{},
	}
	resumer.masterProgress.SetFloat64(0.0)
	resumer.followerProgress.SetFloat64(0.0)

	task.taskMu.Lock()
	task.resumer = resumer
	task.taskMu.Unlock()

	return resumer
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Methods
////////////////////////////////////////////////////////////////////////////////////////////////////

func (r *Resumer) startWorkers() {
	// no need to observe resume during dry run
	if !r.task.dryRun {
		go r.observeResume()
	}

	if r.task.isMaster() {
		go r.initResumeAsync()
	} else {
		// if not master, no need to wait for publishing of tokens
		close(r.waitForTokenPublish)
	}
}

func (r *Resumer) initResumeAsync() {

	// Ask observe to continue
	defer close(r.waitForTokenPublish)

	rdts, err := r.masterGenerateResumePlan()
	if err != nil {
		logging.Errorf(
			"Resumer::initResumeAsync: couldn't generate plan for resume. err: %v for task ID: %v",
			err, r.task.taskId,
		)
		r.finishResume(err)
		return
	}

	if r.task.dryRun {
		// TODO: should rdts be persisted for reuse in resume without dryRun? if yes, how?
		r.finishResume(nil)
		return
	}

	if rdts == nil || len(rdts) == 0 {
		logging.Infof("Resumer::initResumeAsync: resume is a no-op for task ID: %v", r.task.taskId)
		r.finishResume(nil)
		return
	}
	r.masterTokens = rdts

	// Publish tokens to metaKV
	// will crash if cannot set in metaKV even after retries.
	r.publishResumeDownloadTokens(rdts)

	r.masterIncrProgress(10.0)

	func() {
		if r.task.ctx == nil {
			logging.Infof("Resumer::initResumeAsync: task %v cancelled. skipping to start progress collector", r.task.taskId)
			return
		}
		closeCh := r.task.ctx.Done()
		followerNodes := make(map[service.NodeID]string)
		for _, token := range r.masterTokens {
			nid, _ := r.pauseMgr.genericMgr.cinfo.GetNodeIdByUUID(token.FollowerId)
			addr, err := r.pauseMgr.genericMgr.cinfo.GetServiceAddress(nid, c.INDEX_HTTP_SERVICE, true)
			if err != nil {
				logging.Warnf("Resumer::initResumeAsync: failed to get service address for %v. err: %v for task ID %v",
					token.FollowerId, err, r.task.taskId)
			}
			followerNodes[service.NodeID(token.FollowerId)] = addr
		}
		go collectComputeAndReportProgress("Resumer::collectComputeAndReportProgress:",
			r.task.taskId, followerNodes, r.cb.progress, &r.masterProgress, 5*time.Second, &r.wg,
			closeCh)
	}()
}

func (r *Resumer) publishResumeDownloadTokens(rdts map[string]*c.ResumeDownloadToken) {
	for rdtId, rdt := range rdts {
		setResumeDownloadTokenInMetakv(rdtId, rdt)
		logging.Infof("Pauser::publishResumeDownloadTokens Published resume upload token: %v", rdtId)
	}
}

func (r *Resumer) observeResume() {
	logging.Infof("Resumer::observeResume pauseToken[%v] master[%v]", r.pauseToken, r.task.isMaster())

	<-r.waitForTokenPublish

	err := metakv.RunObserveChildren(PauseMetakvDir, r.processDownloadTokens, r.metakvCancel)
	if err != nil {
		logging.Errorf("Resumer::observeResume Exiting on metaKV observe: err[%v]", err)

		r.finishResume(err)
	}

	logging.Infof("Resumer::observeResume exiting: err[%v]", err)
}

// processDownloadTokens is metakv callback, not intended to be called otherwise
func (r *Resumer) processDownloadTokens(kve metakv.KVEntry) error {

	if kve.Path == buildMetakvPathForPauseToken(r.pauseToken.PauseId) {
		// Process PauseToken

		logging.Infof("Resumer::processDownloadTokens: PauseToken path[%v] value[%s]", kve.Path, kve.Value)

		if kve.Value == nil {
			// During cleanup, PauseToken is deleted by master and serves as a signal for
			// all observers on followers to stop.

			logging.Infof("Resumer::processDownloadTokens: PauseToken Deleted. Mark Done.")
			r.cancelMetakv()
			r.finishResume(nil)
		}

	} else if strings.Contains(kve.Path, c.ResumeDownloadTokenPathPrefix) {
		// Process ResumeDownloadTokens

		if kve.Value != nil {
			rdtId, rdt, err := decodeResumeDownloadToken(kve.Path, kve.Value)
			if err != nil {
				logging.Errorf("Resumer::processDownloadTokens: Failed to decode ResumeDownloadToken. Ignored.")
				return nil
			}

			r.processResumeDownloadToken(rdtId, rdt)

		} else {
			logging.Infof("Resumer::processDownloadTokens: Received empty or deleted ResumeDownloadToken path[%v]",
				kve.Path)

		}
	}

	return nil
}

func (r *Resumer) cancelMetakv() {
	r.metakvMutex.Lock()
	defer r.metakvMutex.Unlock()

	if r.metakvCancel != nil {
		close(r.metakvCancel)
		r.metakvCancel = nil
	}
}

func (r *Resumer) processResumeDownloadToken(rdtId string, rdt *c.ResumeDownloadToken) {
	logging.Infof("Resumer::processResumeDownloadToken rdtId[%v] rdt[%v]", rdtId, rdt)
	if !r.addToWaitGroup() {
		logging.Errorf("Resumer::processResumeDownloadToken: Failed to add to resumer waitgroup.")
		return
	}

	defer r.wg.Done()

	// TODO: Check DDL running

	// "processed" var ensures only the incoming token state gets processed by this
	// call, as metakv will call parent processDownloadTokens again for each state change.
	var processed bool

	nodeUUID := string(r.pauseMgr.nodeInfo.NodeID)

	if rdt.MasterId == nodeUUID {
		processed = r.processResumeDownloadTokenAsMaster(rdtId, rdt)
	}

	if rdt.FollowerId == nodeUUID && !processed {
		r.processResumeDownloadTokenAsFollower(rdtId, rdt)
	}
}

func (r *Resumer) addToWaitGroup() bool {
	r.metakvMutex.Lock()
	defer r.metakvMutex.Unlock()

	if r.metakvCancel != nil {
		r.wg.Add(1)
		return true
	}
	return false
}

func (r *Resumer) processResumeDownloadTokenAsMaster(rdtId string, rdt *c.ResumeDownloadToken) bool {

	logging.Infof("Resumer::processResumeDownloadTokenAsMaster: rdtId[%v] rdt[%v]", rdtId, rdt)

	if rdt.ResumeId != r.task.taskId {
		logging.Warnf("Resumer::processResumeDownloadTokenAsMaster: Found ResumeDownloadToken[%v] with Unknown "+
			"ResumeId. Expected to match local taskId[%v]", rdt, r.task.taskId)

		return true
	}

	if rdt.Error != "" {
		logging.Errorf("Resumer::processResumeDownloadTokenAsMaster: Detected PauseUploadToken[%v] in Error state."+
			" Abort.", rdt)

		r.cancelMetakv()
		go r.finishResume(errors.New(rdt.Error))

		return true
	}

	if !r.checkValidNotifyState(rdtId, rdt, "master") {
		return true
	}

	switch rdt.State {

	case c.ResumeDownloadTokenPosted:
		// Follower owns token, do nothing

		return false

	case c.ResumeDownloadTokenInProgess:
		// Follower owns token, just mark in memory maps.

		r.updateInMemToken(rdtId, rdt, "master")
		return false

	case c.ResumeDownloadTokenProcessed:
		// Master owns token

		// Follower completed work, delete token
		err := c.MetakvDel(PauseMetakvDir + rdtId)
		if err != nil {
			logging.Fatalf("Resumer::processResumeDownloadTokenAsMaster: Failed to delete ResumeDownloadToken[%v] with"+
				" rdtId[%v] In Meta Storage: err[%v]", rdt, rdtId, err)
			c.CrashOnError(err)
		}

		r.masterIncrProgress(90.0 / float64(len(r.masterTokens)))

		r.updateInMemToken(rdtId, rdt, "master")

		if r.checkAllTokensDone() {
			// All the followers completed work

			// TODO: set progress 100%

			logging.Infof("Resumer::processResumeDownloadTokenAsMaster: No Tokens Found. Mark Done.")

			r.cancelMetakv()

			go r.finishResume(nil)
		}

		return true

	default:
		return false

	}

}

func (r *Resumer) finishResume(err error) {

	if r.retErr == nil {
		r.retErr = err
	}

	r.cleanupOnce.Do(r.doFinish)
}

func (r *Resumer) doFinish() {
	logging.Infof("Resumer::doFinish Cleanup: retErr[%v]", r.retErr)

	// TODO: signal others that we are cleaning up using done channel

	r.Cleanup()
	r.wg.Wait()

	// call done callback to start the cleanup phase
	// For DryRun=true, pauseToken is nil, so use ID from task instead
	r.cb.done(r.task.taskId, r.retErr)
}

func (r *Resumer) processResumeDownloadTokenAsFollower(rdtId string, rdt *c.ResumeDownloadToken) bool {

	logging.Infof("Resumer::processResumeDownloadTokenAsFollower: rdtId[%v] rdt[%v]", rdtId, rdt)

	if rdt.ResumeId != r.task.taskId {
		logging.Warnf("Resumer::processResumeDownloadTokenAsFollower: Found ResumeDownloadToken[%v] with Unknown "+
			"PauseId. Expected to match local taskId[%v]", rdt, r.task.taskId)

		return true
	}

	if !r.checkValidNotifyState(rdtId, rdt, "follower") {
		return true
	}

	switch rdt.State {

	case c.ResumeDownloadTokenPosted:
		// Follower owns token, update in-memory and move to InProgress State

		r.updateInMemToken(rdtId, rdt, "follower")

		rdt.State = c.ResumeDownloadTokenInProgess
		setResumeDownloadTokenInMetakv(rdtId, rdt)

		return true

	case c.ResumeDownloadTokenInProgess:
		// Follower owns token, update in-memory and start pause work

		r.updateInMemToken(rdtId, rdt, "follower")

		go r.startResumeDownload(rdtId, rdt)

		return true

	case c.ResumeDownloadTokenProcessed:
		// Master owns token, just mark in memory maps

		r.updateInMemToken(rdtId, rdt, "follower")

		return false

	default:
		return false
	}
}

func (r *Resumer) startResumeDownload(rdtId string, rdt *c.ResumeDownloadToken) {
	start := time.Now()
	logging.Infof("Resumer::startResumeDownload: Begin work: rdtId[%v] rdt[%v]", rdtId, rdt)
	defer logging.Infof("Resumer::startResumeDownload: Done work: rdtId[%v] rdt[%v] took[%v]",
		rdtId, rdt, time.Since(start))

	if !r.addToWaitGroup() {
		logging.Errorf("Resumer::startResumeDownload: Failed to add to resumer waitgroup.")
		return
	}
	defer r.wg.Done()

	err := r.followerResumeBuckets(rdtId, rdt)
	if err != nil {
		rdt.Error = err.Error()
	}

	// work done, change state, master handler will pick it up and do cleanup.
	rdt.State = c.ResumeDownloadTokenProcessed
	setResumeDownloadTokenInMetakv(rdtId, rdt)
}

// Often, metaKV can send multiple notifications for the same state change (probably
// due to the eventual consistent nature of metaKV). Keep track of all state changes
// in in-memory bookkeeping and ignore the duplicate notifications
func (r *Resumer) checkValidNotifyState(rdtId string, rdt *c.ResumeDownloadToken, caller string) bool {

	// As the default state is "ResumeDownloadTokenPosted"
	// do not check for valid state changes for this state
	if rdt.State == c.ResumeDownloadTokenPosted {
		return true
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	var inMemToken *c.ResumeDownloadToken
	var ok bool

	if caller == "master" {
		inMemToken, ok = r.masterTokens[rdtId]
	} else if caller == "follower" {
		inMemToken, ok = r.followerTokens[rdtId]
	}

	if ok {
		// Token seen before, validate the state

		// < for invalid state change
		// == for duplicate notification
		if rdt.State <= inMemToken.State {
			logging.Warnf("Resumer::checkValidNotifyState Detected Invalid State Change Notification"+
				" for [%v]. rdtId[%v] Local[%v] Metakv[%v]", caller, rdtId, inMemToken.State, rdt.State)

			return false
		}
	}

	return true
}

func (r *Resumer) updateInMemToken(rdtId string, rdt *c.ResumeDownloadToken, caller string) {

	r.mu.Lock()
	defer r.mu.Unlock()

	if caller == "master" {
		r.masterTokens[rdtId] = rdt.Clone()
	} else if caller == "follower" {
		r.followerTokens[rdtId] = rdt.Clone()
	}
}

func (r *Resumer) checkAllTokensDone() bool {

	r.mu.Lock()
	defer r.mu.Unlock()

	for rdtId, rdt := range r.masterTokens {
		if rdt.State < c.ResumeDownloadTokenProcessed {
			// Either posted or processing

			logging.Infof("Resumer::checkAllTokensDone ResumeDownloadToken: rdtId[%v] is in state[%v]",
				rdtId, rdt.State)

			return false
		}
	}

	return true
}

// masterGenerateResumePlan: this method downloads all the metadata, stats from archivePath and
// plans which nodes resume indexes for given bucket
func (r *Resumer) masterGenerateResumePlan() (map[string]*c.ResumeDownloadToken, error) {
	// Step 1: download PauseMetadata
	logging.Infof("Resumer::masterGenerateResumePlan: downloading pause metadata from %v for resume task ID: %v", r.task.archivePath, r.task.taskId)
	ctx := r.task.ctx
	plasmaCfg := generatePlasmaCopierConfig(r.task)

	copier := plasma.MakeFileCopier(r.task.archivePath, "", plasmaCfg.Environment, plasmaCfg.CopyConfig)
	if copier == nil {
		err := fmt.Errorf("object store not supported")
		logging.Errorf("Resumer::masterGenerateResumePlan: %v", err)
		return nil, err
	}

	data, err := copier.DownloadBytes(ctx, fmt.Sprintf("%v%v", r.task.archivePath, FILENAME_PAUSE_METADATA))
	if err != nil {
		logging.Errorf("Resumer::masterGenerateResumePlan: failed to download pause metadata err: %v for resume task ID: %v", err, r.task.taskId)
		return nil, err
	}
	data, err = c.ChecksumAndUncompress(data)
	if err != nil {
		logging.Errorf("Resumer::masterGenerateResumePlan: failed to read valid pause metadata err: %v for resume task ID: %v", err, r.task.taskId)
		return nil, err
	}
	pauseMetadata := new(PauseMetadata)
	err = json.Unmarshal(data, pauseMetadata)
	if err != nil {
		logging.Errorf("Resumer::masterGenerateResumePlan: couldn't unmarshal pause metadata err: %v for resume task ID: %v", err, r.task.taskId)
		return nil, err
	}

	// Pause could be a no-op for indexer; if that is the case, no need to execute planner
	if pauseMetadata.Data == nil || len(pauseMetadata.Data) == 0 {
		return nil, nil
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

			nodeDir := generateNodeDir(r.task.archivePath, nodeId)
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
		return nil, err
	}

	// Step 3: get replacement node for old paused data
	resumeNodes := make([]*planner.IndexerNode, 0, len(pauseMetadata.Data))
	config := r.pauseMgr.config.Load()

	err = r.pauseMgr.genericMgr.cinfo.FetchNodesAndSvsInfoWithLock()
	if err != nil {
		logging.Errorf("Resumer::masterGenerateResumePlan: cluster info cache sync failed; err - %v",
			err)
		return nil, err
	}
	clusterVersion := r.pauseMgr.genericMgr.cinfo.GetClusterVersion()
	// since we don't support mixed mode for pause resume, we can use the current server version
	// as the indexer version
	indexerVersion, err := r.pauseMgr.genericMgr.cinfo.GetServerVersion(
		r.pauseMgr.genericMgr.cinfo.GetCurrentNode(),
	)
	if err != nil {
		// we should never hit this err condition as we should always be able to read current node's
		// server version. if we do, should we fail here?
		logging.Warnf("Resumer::masterGenerateResumePlan: couldn't fetch this node's server version. hit unreachable error. err: %v for taskId %v", err, r.task.taskId)
		err = nil
		// use min indexer version required for Pause-Resume
		indexerVersion = c.INDEXER_72_VERSION
	}
	sysConfig, err := c.GetSettingsConfig(c.SystemConfig)
	if err != nil {
		err = fmt.Errorf("Unable to get system config; err: %v", err)
		logging.Errorf("Resumer::masterGenerateResumePlan: %v", err)
		return nil, err
	}

	for nodeId := range pauseMetadata.Data {
		// Step 3a: generate IndexerNode for planner

		idxMetadata, ok := indexMetadataPerNode[nodeId]
		if !ok {
			err = fmt.Errorf("unable to read indexMetadata for node %v", nodeId)
			logging.Errorf("Resumer::masterGenerateResumePlan: %v", err)
			return nil, err
		}
		statsPerNode, ok := statsPerNode[nodeId]
		if !ok {
			err = fmt.Errorf("unable to read stats for node %v", nodeId)
			logging.Errorf("Resumer::masterGenerateResumePlan: %v", err)
			return nil, err
		}

		logging.Infof("Resumer::masterGenerateResumePlan: metadata and stats from node %v available. Total Idx definitions: %v, Total stats: %v for task ID: %v",
			nodeId, len(idxMetadata.IndexDefinitions), len(statsPerNode), r.task.taskId)

		indexerNode := planner.CreateIndexerNodeWithIndexes(string(nodeId), nil, nil)

		// Step 3b: populate IndexerNode with metadata
		var indexerUsage []*planner.IndexUsage
		indexerUsage, err = planner.ConvertToIndexUsages(config, idxMetadata, indexerNode, nil,
			nil)
		if err != nil {
			logging.Errorf("Resumer::masterGenerateResumePlan: couldn't generate index usage. err: %v for task ID: %v", err, r.task.taskId)
			return nil, err
		}

		indexerNode.Indexes = indexerUsage

		planner.SetStatsInIndexer(indexerNode, statsPerNode, clusterVersion, indexerVersion,
			sysConfig)

		resumeNodes = append(resumeNodes, indexerNode)
	}
	rdts, err := planner.ExecuteTenantAwarePlanForResume(config["clusterAddr"].String(),
		r.task.taskId, string(r.pauseMgr.genericMgr.nodeInfo.NodeID), r.task.bucket, resumeNodes)
	if err != nil {
		err = fmt.Errorf("master failed to plan resume. Reason: %v", err)
		return nil, err
	}
	for _, rdt := range rdts {
		shardPaths, ok := pauseMetadata.Data[service.NodeID(rdt.UploaderId)]
		if !ok {
			err = fmt.Errorf("no shard paths found for uploader %v", rdt.UploaderId)
			logging.Errorf("Resumer::masterGenerateResumePlan: %v for task ID: %v", err,
				r.task.taskId)
			return nil, err
		}
		rdt.ShardPaths = shardPaths
	}
	logging.Infof("Resumer::masterGenerateResumePlan create resume download tokens %v", rdts)

	return rdts, nil
}

func (r *Resumer) downloadNodeMetadataAndStats(nodeDir string) (metadata *planner.LocalIndexMetadata, stats map[string]interface{}, err error) {
	defer func() {
		if err == nil {
			logging.Infof("Resumer::downloadNodeMetadataAndStats: successfully downloaded metadata and stats from %v", nodeDir)
		}
	}()

	ctx := r.task.ctx
	plasmaCfg := generatePlasmaCopierConfig(r.task)

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
	data, err = c.ChecksumAndUncompress(data)
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
	data, err = c.ChecksumAndUncompress(data)
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

// followerResumeBuckets performs resume tasks on the called follower
// It performs the following steps:
// 1. Download metadata and stats
// 2. Download plasma shards
// 3. restore indexes in common.INDEX_STATE_RECOVERED state
// Returns: []client.IndexIdList - it a list of list of indexes to build grouped by stream state
func (r *Resumer) followerResumeBuckets(rdtId string, rdt *c.ResumeDownloadToken) error {
	nodeDir := generateNodeDir(r.task.archivePath, service.NodeID(rdt.UploaderId))

	// TODO: make all these downloads async. blocking go threads for networks downloads
	// is bad design

	metadata, stats, err := r.downloadNodeMetadataAndStats(nodeDir)
	if err != nil {
		err = fmt.Errorf("couldn't download metadata/stats uploaded by %v, err: %v",
			rdt.UploaderId, err)
		logging.Errorf("Resumer::followerResumeBucket: %v", err)
		return err
	}

	r.followerIncrProgress(10.0)

	logging.Infof(
		"Resumer::followerResumeBucket: downloaded metadata of %v indexes and stats (count=%v) for task ID %v",
		len(metadata.IndexDefinitions), len(stats), r.task.taskId,
	)

	shardIds := make([]c.ShardId, 0, len(rdt.ShardPaths))
	shardPaths := rdt.ShardPaths
	for shardId, shardPath := range shardPaths {
		shardPaths[shardId] = generateShardPath(nodeDir, shardPath)
		shardIds = append(shardIds, shardId)
	}
	r.shardIds = shardIds

	cancelCh := r.task.ctx.Done()
	_, err = r.pauseMgr.downloadShardsWithoutLock(shardPaths, r.task.taskId, r.task.bucket,
		nodeDir, "", cancelCh,
		func(incr float64) { r.followerIncrProgress(incr * 60.0 / float64(len(shardPaths))) },
	)
	if err != nil {
		err = fmt.Errorf("couldn't download plasma shards %v; err: %v for task ID: %v",
			rdt.ShardPaths, err, r.task.taskId)

		logging.Errorf("Resumer::followerResumeBucket: %v", err)
		return err
	}
	logging.Infof("Resumer::followerResumeBucket: successfully downloaded shards for task ID %v",
		r.task.taskId)

	err = r.recoverShard(rdtId, rdt, cancelCh)
	if err != nil {
		return err
	}

	r.followerIncrProgress(2.0)

	return nil
}

func (r *Resumer) recoverShard(rdtid string,
	rdt *c.ResumeDownloadToken, cancelCh <-chan struct{}) error {

	//lock the restored shard
	if err := lockShards(rdt.ShardIds, r.pauseMgr.supvMsgch, true); err != nil {
		logging.Errorf("Resumer::recoverShard:: error observed while locking shards: %v, err: %v", rdt.ShardIds, err)

		unlockShards(rdt.ShardIds, r.pauseMgr.supvMsgch)
		return err
	}

	start := time.Now()

	//Restored shard for a resumed tenant can only contain a) active instances (MAINT_STREAM/INDEX_STATE_ACTIVE) or
	//deferred indexes(NIL_STREAM/INDEX_STATE_CREATE/READY). PreparePause will ensure that tenant doesn't get
	//paused if there are indexes in any other state. As pause happens after few days of inactivity, it is
	//the expected situation.

	var buildDefnIdList client.IndexIdList

	nonDeferredInsts := make(map[c.IndexInstId]bool)
	defnIdToInstIdMap := make(map[c.IndexDefnId]c.IndexInstId)

	//set InstIds in Defn
	func(rdt *c.ResumeDownloadToken) {
		for i, _ := range rdt.IndexInsts {
			defn := &rdt.IndexInsts[i].Defn
			defn.SetCollectionDefaults()

			defn.Nodes = nil
			defn.InstId = rdt.InstIds[i]
			defn.RealInstId = rdt.RealInstIds[i]
		}
		return
	}(rdt)

	for _, inst := range rdt.IndexInsts {

		defn := inst.Defn
		defn.ShardIdsForDest = rdt.ShardIds
		if skip, err := r.postRecoverIndexReq(defn); err != nil {
			return err
		} else if skip {
			// bucket (or) scope (or) collection (or) index are dropped.
			// Continue instead of failing resume
			continue
		}

		currInst := make(map[c.IndexInstId]bool)

		// For deferred indexes, build command is not required
		if defn.Deferred &&
			(defn.InstStateAtRebal == c.INDEX_STATE_CREATED ||
				defn.InstStateAtRebal == c.INDEX_STATE_READY) {

			currInst[defn.InstId] = true

			if err := r.waitForIndexState(c.INDEX_STATE_READY, currInst, rdtid, rdt, cancelCh); err != nil {
				return err
			}

		} else {

			buildDefnIdList.DefnIds = append(buildDefnIdList.DefnIds, uint64(defn.DefnId))
			defnIdToInstIdMap[defn.DefnId] = defn.InstId

			currInst[defn.InstId] = true

			if err := r.waitForIndexState(c.INDEX_STATE_RECOVERED, currInst, rdtid, rdt, cancelCh); err != nil {
				return err
			}

		}

		r.followerIncrProgress(14.0 / float64(len(rdt.IndexInsts)))

	}

	if len(buildDefnIdList.DefnIds) > 0 {

		logging.Infof("Resumer::recoverShard Successfully posted "+
			"recoverIndexRebalance requests for defnIds: %v rdtid: %v. "+
			"Initiating build.", buildDefnIdList, rdtid)

		skipDefns, err := r.postResumeIndexesReq(buildDefnIdList)
		if err != nil {
			return err
		}

		// Do not wait for index state of skipped insts
		if len(skipDefns) > 0 {
			logging.Infof("Resumer::recoverShard Skipping state monitoring for insts: %v "+
				"as scope/collection/index is dropped", skipDefns)
			for defnId, _ := range skipDefns {
				if instId, ok := defnIdToInstIdMap[defnId]; ok {
					delete(nonDeferredInsts, instId)
				}
			}
		}

		logging.Infof("Resumer::recoverShard Waiting for index state to "+
			"become active for insts: %v", nonDeferredInsts)

		if err := r.waitForIndexState(c.INDEX_STATE_ACTIVE, nonDeferredInsts, rdtid, rdt, cancelCh); err != nil {
			return err
		}

		r.followerIncrProgress(14.0 / float64(len(buildDefnIdList.DefnIds)))
	}

	elapsed := time.Since(start).Seconds()
	l.Infof("Resumer::recoverShard Finished recovery of all indexes in rdtid: %v, elapsed(sec): %v", rdtid, elapsed)

	// Follower will call RestoreShardDone for the shardId involved in the
	// resume.
	restoreShardDone(rdt.ShardIds, r.pauseMgr.supvMsgch)

	// Unlock the shards
	unlockShards(rdt.ShardIds, r.pauseMgr.supvMsgch)

	return nil
}

func (r *Resumer) postRecoverIndexReq(indexDefn c.IndexDefn) (bool, error) {

	url := "/recoverIndexRebalance"

	resp, err := postWithHandleEOF(indexDefn, r.pauseMgr.httpAddr, url, "Resumer::postRecoverIndexReq")
	if err != nil {
		logging.Errorf("Resumer::postRecoverIndexReq Error observed when posting recover index request, "+
			"indexDefnId: %v, err: %v", indexDefn.DefnId, err)
		return false, err
	}

	response := new(IndexResponse)
	if err := convertResponse(resp, response); err != nil {
		l.Errorf("Resumer::postRecoverIndexReq Error unmarshal response for indexDefnId: %v, "+
			"url: %v, err: %v", indexDefn.DefnId, r.pauseMgr.httpAddr+url, err)
		return false, err
	}

	if response.Error != "" {
		l.Errorf("Resumer::postRecoverIndexReq Error received for indexDefnId: %v, err: %v",
			indexDefn.DefnId, response.Error)
		return false, errors.New(response.Error)
	}
	return false, nil
}

func (r *Resumer) postResumeIndexesReq(defnIdList client.IndexIdList) (map[c.IndexDefnId]bool, error) {

	url := "/resumeRecoveredIndexes"

	resp, err := postWithHandleEOF(defnIdList, r.pauseMgr.httpAddr, url, "Resumer::postResumeIndexesReq")
	if err != nil {
		logging.Errorf("Resumer::postResumeIndexesReq Error observed when posting build indexes request, "+
			"defnIdList: %v, err: %v", defnIdList.DefnIds, err)
		return nil, err
	}

	response := new(IndexResponse)
	if err := convertResponse(resp, response); err != nil {
		l.Errorf("Resumer::postResumeIndexesReq Error unmarshal response for defnIdList: %v, "+
			"url: %v, err: %v", defnIdList.DefnIds, r.pauseMgr.httpAddr+url, err)
		return nil, err
	}

	if response.Error != "" {
		skipDefns, err := unmarshalAndProcessBuildReqResponse(response.Error, defnIdList.DefnIds)
		if err != nil { // Error while unmarshalling - Return the error to caller and fail rebalance
			l.Errorf("Resumer::postResumeIndexesReq Error received for defnIdList: %v, err: %v",
				defnIdList.DefnIds, response.Error)
			return nil, errors.New(response.Error)
		} else {
			return skipDefns, nil
		}
	}
	return nil, nil
}

func (r *Resumer) waitForIndexState(expectedState c.IndexState,
	processedInsts map[c.IndexInstId]bool, rdtid string,
	rdt *c.ResumeDownloadToken, cancelCh <-chan struct{}) error {

	lastLogTime := time.Now()

	retryInterval := time.Duration(1)
	retryCount := 0
loop:
	for {

		select {
		case <-cancelCh:
			//TODO return relevant error
			l.Infof("Resumer::waitForIndexState Cancel Received")
			return ErrRebalanceCancel

		default:
			statsMgr := r.pauseMgr.genericMgr.statsMgr
			allStats := statsMgr.stats.Get()

			indexerState := allStats.indexerStateHolder.GetValue().(string)
			if indexerState == "Paused" {
				err := fmt.Errorf("Paused state detected for %v", r.pauseMgr.httpAddr)
				l.Errorf("Resumer::waitForIndexState err: %v", err)
				return err
			}

			localMeta, err := getLocalMeta(r.pauseMgr.httpAddr)
			if err != nil {
				l.Errorf("Resumer::waitForIndexState Error Fetching Local Meta %v %v", r.pauseMgr.httpAddr, err)
				retryCount++

				if retryCount > 5 {
					return err // Return after 5 unsuccessful attempts
				}
				time.Sleep(retryInterval * time.Second)
				goto loop
			}

			indexStateMap, errMap := r.getIndexStatusFromMeta(rdt, processedInsts, localMeta)
			for instId, indexState := range indexStateMap {
				err := errMap[instId]

				// At this point, the request "/recoverIndexRebalance" and/or "/resumeRecoveredIndexes"
				// are successful. This means that local metadata is updated with index instance infomration.
				// If drop were to happen in this state, drop can remove the index from topology. In that
				// case, the indexState would be INDEX_STATE_NIL. Instead of failing rebalance, skip the
				// instance from further scanity checks and continue processing
				if indexState == c.INDEX_STATE_NIL || indexState == c.INDEX_STATE_DELETED {
					logging.Warnf("Resumer::waitForIndexState, Could not get index status. "+
						"scope/collection/index are likely dropped. Skipping instId: %v, indexState: %v, "+
						"rdtid: %v", instId, indexState, rdtid)
					continue
				} else if err != "" {
					l.Errorf("Resumer::waitForIndexState Error Fetching Index Status %v %v", r.pauseMgr.httpAddr, err)
					retryCount++

					if retryCount > 5 {
						return errors.New(err) // Return after 5 unsuccessful attempts
					}
					time.Sleep(retryInterval * time.Second)
					goto loop // Retry
				}
			}

			switch expectedState {
			case c.INDEX_STATE_READY, c.INDEX_STATE_RECOVERED, c.INDEX_STATE_ACTIVE:
				// Check if all index instances have reached this state
				allReachedState := true
				for _, indexState := range indexStateMap {
					if indexState == c.INDEX_STATE_NIL || indexState == c.INDEX_STATE_DELETED {
						continue
					}
					if indexState != expectedState {
						allReachedState = false
						break
					}
				}

				if allReachedState {
					logging.Infof("Resumer::waitForIndexState: Indexes: %v reached state: %v", indexStateMap, expectedState)
					return nil
				}

				now := time.Now()
				if now.Sub(lastLogTime) > 30*time.Second {
					lastLogTime = now
					logging.Infof("Resumer::waitForIndexState: Waiting for some indexes to reach state: %v, indexes: %v", expectedState, indexStateMap)
				}
				// retry after "retryInterval" if not all indexes have reached the expectedState
			}
		}
		// reset retry count as one iteration of the loop could be completed
		// successfully without any error
		retryCount = 0

		time.Sleep(retryInterval * time.Second)
	}

	return nil
}

func (r *Resumer) getIndexStatusFromMeta(rdt *c.ResumeDownloadToken,
	processedInsts map[c.IndexInstId]bool,
	localMeta *manager.LocalIndexMetadata) (map[c.IndexInstId]c.IndexState, map[c.IndexInstId]string) {

	outStates := make(map[c.IndexInstId]c.IndexState)
	outErr := make(map[c.IndexInstId]string)
	for i, inst := range rdt.IndexInsts {

		instId := rdt.InstIds[i]
		realInstId := rdt.RealInstIds[i]

		if !isInstProcessed(instId, realInstId, processedInsts) {
			continue
		}

		topology := findTopologyByCollection(localMeta.IndexTopologies, inst.Defn.Bucket, inst.Defn.Scope, inst.Defn.Collection)
		if topology == nil {
			outStates[instId] = c.INDEX_STATE_NIL
			outErr[instId] = fmt.Sprintf("Topology Information Missing for Bucket %v Scope %v Collection %v",
				inst.Defn.Bucket, inst.Defn.Scope, inst.Defn.Collection)
			continue
		}

		state, errMsg := topology.GetStatusByInst(inst.Defn.DefnId, instId)
		if state == c.INDEX_STATE_NIL && realInstId != 0 {
			state, errMsg = topology.GetStatusByInst(inst.Defn.DefnId, realInstId)
			outStates[realInstId], outErr[realInstId] = state, errMsg
		} else {
			outStates[instId], outErr[instId] = state, errMsg
		}
	}

	return outStates, outErr
}

func (r *Resumer) cleanupNoLocks() {
	r.task.cancelNoLock()
}

func (r *Resumer) Cleanup() {
	r.task.taskMu.Lock()
	r.cleanupNoLocks()
	r.task.taskMu.Unlock()
	r.cancelMetakv()
}

func (r *Resumer) masterIncrProgress(incr float64) {
	incrProgress(&r.masterProgress, incr)
	r.cb.progress(r.task.taskId, r.masterProgress.GetFloat64(), nil)
}

func (r *Resumer) followerIncrProgress(incr float64) {
	incrProgress(&r.followerProgress, incr)
}
