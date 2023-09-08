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
	"io/ioutil"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/manager"
)

func newPauseUploadToken(masterUuid, followerUuid, pauseId, bucketName string) (string, *common.PauseUploadToken, error) {
	put := &common.PauseUploadToken{
		MasterId:   masterUuid,
		FollowerId: followerUuid,
		PauseId:    pauseId,
		State:      common.PauseUploadTokenPosted,
		BucketName: bucketName,
	}

	ustr, err := common.NewUUID()
	if err != nil {
		logging.Warnf("newPauseUploadToken: Failed to generate uuid: err[%v]", err)
		return "", nil, err
	}

	putId := fmt.Sprintf("%s%s", common.PauseUploadTokenTag, ustr.Str())

	return putId, put, nil
}

func decodePauseUploadToken(path string, value []byte) (string, *common.PauseUploadToken, error) {

	putIdPos := strings.Index(path, common.PauseUploadTokenTag)
	if putIdPos < 0 {
		return "", nil, fmt.Errorf("PauseUploadTokenTag[%v] not present in metakv path[%v]",
			common.PauseUploadTokenTag, path)
	}

	putId := path[putIdPos:]

	put := &common.PauseUploadToken{}
	err := json.Unmarshal(value, put)
	if err != nil {
		logging.Errorf("decodePauseUploadToken: Failed to unmarshal value[%s] path[%v]: err[%v]",
			string(value), path, err)
		return "", nil, err
	}

	return putId, put, nil
}

func setPauseUploadTokenInMetakv(putId string, put *common.PauseUploadToken) {

	rhCb := func(r int, err error) error {
		if r > 0 {
			logging.Warnf("setPauseUploadTokenInMetakv::rhCb: err[%v], Retrying[%d]", err, r)
		}

		return common.MetakvSet(PauseMetakvDir+putId, put)
	}

	rh := common.NewRetryHelper(10, time.Second, 1, rhCb)
	err := rh.Run()

	if err != nil {
		logging.Fatalf("setPauseUploadTokenInMetakv: Failed to set PauseUploadToken In Meta Storage:"+
			" putId[%v] put[%v] err[%v]", putId, put, err)
		common.CrashOnError(err)
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Pauser class - Perform the Pause of a given bucket (similar to Rebalancer's role).
// This is used only on the master node of a task_PAUSE task to do the GSI orchestration.
////////////////////////////////////////////////////////////////////////////////////////////////////

// Called at the end of the pause lifecycle. It takes pauseId and any error as input.
type PauseResumeDoneCallback func(string, error)
type PauseResumeProgressCallback func(string, float64, map[string]float64)

type PauseResumeCallbacks struct {
	done     PauseResumeDoneCallback
	progress PauseResumeProgressCallback
}

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

	// Channels used for signalling
	// Used to signal that the PauseUploadTokens have been published.
	waitForTokenPublish chan struct{}
	// Used to signal metakv observer to stop
	metakvCancel chan struct{}

	metakvMutex sync.RWMutex
	wg          sync.WaitGroup

	// Global token associated with this Pause task
	pauseToken *PauseToken

	// in-memory bookkeeping for observed tokens
	masterTokens, followerTokens map[string]*common.PauseUploadToken

	// lock protecting access to maps like masterTokens and followerTokens
	mu sync.RWMutex

	// For cleanup
	retErr      error
	cleanupOnce sync.Once
	cb          PauseResumeCallbacks

	// For progress tracking
	masterProgress, followerProgress float64Holder

	// for cleanup after pause
	shardIds []common.ShardId
}

// NewPauser creates a Pauser instance to execute the given task. It saves a pointer to itself in
// task.pauser (visible to pauseMgr parent) and launches a goroutine for the work.
//
//	pauseMgr - parent object (singleton)
//	task - the task_PAUSE task this object will execute
//	pauseToken - global PauseToken
//	doneCb - callback that initiates the cleanup phase
func NewPauser(pauseMgr *PauseServiceManager, task *taskObj, pauseToken *PauseToken,
	doneCb PauseResumeDoneCallback, progressCb PauseResumeProgressCallback) *Pauser {

	pauser := &Pauser{
		pauseMgr: pauseMgr,
		task:     task,
		nodeDir:  generateNodeDir(task.archivePath, pauseMgr.genericMgr.nodeInfo.NodeID),

		waitForTokenPublish: make(chan struct{}),
		metakvCancel:        make(chan struct{}),
		pauseToken:          pauseToken,

		followerTokens: make(map[string]*common.PauseUploadToken),

		cb: PauseResumeCallbacks{done: doneCb, progress: progressCb},

		masterProgress:   float64Holder{},
		followerProgress: float64Holder{},
	}
	pauser.masterProgress.SetFloat64(0.0)
	pauser.followerProgress.SetFloat64(0.0)

	task.taskMu.Lock()
	task.pauser = pauser
	task.taskMu.Unlock()

	return pauser
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Methods
////////////////////////////////////////////////////////////////////////////////////////////////////

func (p *Pauser) startWorkers() {
	go p.observePause()

	if p.task.isMaster() {
		go p.initPauseAsync()
	} else {
		// if not master, no need to wait for publishing of tokens
		close(p.waitForTokenPublish)
	}
}

func (p *Pauser) initPauseAsync() {

	// Ask observe to continue
	defer close(p.waitForTokenPublish)

	// Generate PauseUploadTokens
	puts, err := p.generatePauseUploadTokens()
	if err != nil {
		logging.Errorf("Pauser::initPauseAsync: Failed to generate PauseUploadTokens: err[%v], puts[%v]",
			err, puts)

		p.finishPause(err)
		return
	}

	// Initilize master tokens instead of waiting to observe them through metakv callback
	p.masterTokens = puts

	// Publish tokens to metaKV
	// will crash if cannot set in metaKV even after retries.
	p.publishPauseUploadTokens(puts)

	p.masterIncrProgress(5.0)

	func() {
		if p.task.ctx == nil {
			logging.Infof("Pauser::initResumeAsync: task %v cancelled. skipping to start progress collector", p.task.taskId)
			return
		}
		closeCh := p.task.ctx.Done()
		followerNodes := make(map[service.NodeID]string)
		for _, token := range p.masterTokens {
			nid, _ := p.pauseMgr.genericMgr.cinfo.GetNodeIdByUUID(token.FollowerId)
			addr, err := p.pauseMgr.genericMgr.cinfo.GetServiceAddress(nid, common.INDEX_HTTP_SERVICE, true)
			if err != nil {
				logging.Warnf("Pauser::initPauseAsync: failed to get service address for %v. err: %v for task ID %v",
					token.FollowerId, err, p.task.taskId)
			}
			followerNodes[service.NodeID(token.FollowerId)] = addr
		}
		go collectComputeAndReportProgress("Pauser::collectComputeAndReportProgress:",
			p.task.taskId, followerNodes, p.cb.progress, &p.masterProgress, 5*time.Second, &p.wg,
			closeCh)
	}()
}

func (p *Pauser) generatePauseUploadTokens() (map[string]*common.PauseUploadToken, error) {
	indexerUuids, err := p.getIndexerUuids()
	if err != nil || len(indexerUuids) < 1 {
		logging.Errorf("Pauser::generatePauseUploadTokens: Error getting indexer node UUIDs: err[%v]"+
			" indexerUuids[%v]", err, indexerUuids)
		return nil, err
	}

	puts := make(map[string]*common.PauseUploadToken)
	nodeUUID := string(p.pauseMgr.nodeInfo.NodeID)

	for _, uuid := range indexerUuids {
		rhCb := func(retryAttempt int, lastErr error) error {
			putId, put, err := newPauseUploadToken(nodeUUID, uuid, p.task.taskId, p.task.bucket)
			if err != nil {
				logging.Warnf("Pauser::generatePauseUploadTokens::rhCb: Error making new PauseUploadToken: "+
					"err[%v] retryAttempt[%d] lastErr[%v]", err, retryAttempt, lastErr)
				return err
			}

			if oldPut, ok := puts[putId]; ok {
				err = fmt.Errorf("put with putId[%s] already exists, put[%v]", putId, oldPut)
				logging.Warnf("Pauser::generatePauseUploadTokens::rhCb: PUTId Collision: "+
					"err[%v] retryAttempt[%d] lastErr[%v]", err, retryAttempt, lastErr)
				return err
			}

			puts[putId] = put

			return nil
		}

		rh := common.NewRetryHelper(10, 100*time.Millisecond, 1, rhCb)
		if err := rh.Run(); err != nil {
			logging.Errorf("Pauser::generatePauseUploadTokens: Failed to generate PauseUploadToken: err[%v]",
				err)
			return nil, err
		}
	}

	return puts, nil
}

func (p *Pauser) getIndexerUuids() (indexerUuids []string, err error) {

	p.pauseMgr.genericMgr.cinfo.Lock()
	defer p.pauseMgr.genericMgr.cinfo.Unlock()

	if err := p.pauseMgr.genericMgr.cinfo.FetchNodesAndSvsInfo(); err != nil {
		logging.Errorf("Pauser::getIndexerUuids Error Fetching Cluster Information %v", err)
		return nil, err
	}

	nids := p.pauseMgr.genericMgr.cinfo.GetNodeIdsByServiceType(common.INDEX_HTTP_SERVICE)
	url := "/nodeuuid"

	for _, nid := range nids {
		haddr, err := p.pauseMgr.genericMgr.cinfo.GetServiceAddress(nid, common.INDEX_HTTP_SERVICE, true)
		if err != nil {
			return nil, err
		}

		resp, err := getWithAuth(haddr + url)
		if err != nil {
			logging.Errorf("Pauser::getIndexerUuids Unable to Fetch Node UUID %v %v", haddr, err)
			return nil, err

		} else {
			defer resp.Body.Close()

			bytes, _ := ioutil.ReadAll(resp.Body)
			uuid := string(bytes)
			indexerUuids = append(indexerUuids, uuid)
		}
	}

	return indexerUuids, nil
}

func (p *Pauser) publishPauseUploadTokens(puts map[string]*common.PauseUploadToken) {
	for putId, put := range puts {
		setPauseUploadTokenInMetakv(putId, put)
		logging.Infof("Pauser::publishPauseUploadTokens Published pause upload token: %v", putId)
	}
}

func (p *Pauser) observePause() {
	logging.Infof("Pauser::observePause pauseToken[%v] master[%v]", p.pauseToken, p.task.isMaster())

	<-p.waitForTokenPublish

	err := metakv.RunObserveChildren(PauseMetakvDir, p.processUploadTokens, p.metakvCancel)
	if err != nil {
		logging.Errorf("Pauser::observePause Exiting on metaKV observe: err[%v]", err)

		p.finishPause(err)
	}

	logging.Infof("Pauser::observePause exiting: err[%v]", err)
}

// processUploadTokens is metakv callback, not intended to be called otherwise
func (p *Pauser) processUploadTokens(kve metakv.KVEntry) error {

	if kve.Path == buildMetakvPathForPauseToken(p.pauseToken.PauseId) {
		// Process PauseToken

		logging.Infof("Pauser::processUploadTokens: PauseToken path[%v] value[%s]", kve.Path, kve.Value)

		if kve.Value == nil {
			// During cleanup, PauseToken is deleted by master and serves as a signal for
			// all observers on followers to stop.

			logging.Infof("Pauser::processUploadTokens: PauseToken Deleted. Mark Done.")
			p.cancelMetakv()
			p.finishPause(nil)
		}

	} else if strings.Contains(kve.Path, common.PauseUploadTokenPathPrefix) {
		// Process PauseUploadTokens

		if kve.Value != nil {
			putId, put, err := decodePauseUploadToken(kve.Path, kve.Value)
			if err != nil {
				logging.Errorf("Pauser::processUploadTokens: Failed to decode PauseUploadToken. Ignored.")
				return nil
			}

			p.processPauseUploadToken(putId, put)

		} else {
			logging.Infof("Pauser::processUploadTokens: Received empty or deleted PauseUploadToken path[%v]",
				kve.Path)

		}
	}

	return nil
}

func (p *Pauser) cancelMetakv() {
	p.metakvMutex.Lock()
	defer p.metakvMutex.Unlock()

	if p.metakvCancel != nil {
		close(p.metakvCancel)
		p.metakvCancel = nil
	}
}

func (p *Pauser) processPauseUploadToken(putId string, put *common.PauseUploadToken) {
	logging.Infof("Pauser::processPauseUploadToken putId[%v] put[%v]", putId, put)
	if !p.addToWaitGroup() {
		logging.Errorf("Pauser::processPauseUploadToken: Failed to add to pauser waitgroup.")
		return
	}

	defer p.wg.Done()

	// "processed" var ensures only the incoming token state gets processed by this
	// call, as metakv will call parent processUploadTokens again for each state change.
	var processed bool

	nodeUUID := string(p.pauseMgr.nodeInfo.NodeID)

	if put.MasterId == nodeUUID {
		processed = p.processPauseUploadTokenAsMaster(putId, put)
	}

	if put.FollowerId == nodeUUID && !processed {
		p.processPauseUploadTokenAsFollower(putId, put)
	}
}

func (p *Pauser) addToWaitGroup() bool {
	p.metakvMutex.Lock()
	defer p.metakvMutex.Unlock()

	if p.metakvCancel != nil {
		p.wg.Add(1)
		return true
	}
	return false
}

func (p *Pauser) processPauseUploadTokenAsMaster(putId string, put *common.PauseUploadToken) bool {

	logging.Infof("Pauser::processPauseUploadTokenAsMaster: putId[%v] put[%v]", putId, put)

	if put.PauseId != p.task.taskId {
		logging.Warnf("Pauser::processPauseUploadTokenAsMaster: Found PauseUploadToken[%v] with Unknown "+
			"PauseId. Expected to match local taskId[%v]", put, p.task.taskId)

		return true
	}

	if put.Error != "" {
		logging.Errorf("Pauser::processPauseUploadTokenAsMaster: Detected PauseUploadToken[%v] in Error state."+
			" Abort.", put)

		p.cancelMetakv()
		go p.finishPause(errors.New(put.Error))

		return true
	}

	if !p.checkValidNotifyState(putId, put, "master") {
		return true
	}

	switch put.State {

	case common.PauseUploadTokenPosted:
		// Follower owns token, do nothing

		return false

	case common.PauseUploadTokenInProgess:
		// Follower owns token, just mark in memory maps.

		p.updateInMemToken(putId, put, "master")
		return false

	case common.PauseUploadTokenProcessed:
		// Master owns token

		shardPaths := put.ShardPaths
		if shardPaths != nil {
			p.task.pauseMetadata.addShardPaths(service.NodeID(put.FollowerId), shardPaths)
		}

		// Follower completed work, delete token
		err := common.MetakvDel(PauseMetakvDir + putId)
		if err != nil {
			logging.Fatalf("Pauser::processPauseUploadTokenAsMaster: Failed to delete PauseUploadToken[%v] with"+
				" putId[%v] In Meta Storage: err[%v]", put, putId, err)
			common.CrashOnError(err)
		}

		p.updateInMemToken(putId, put, "master")

		p.masterIncrProgress(90.0 / float64(len(p.masterTokens)))

		if p.checkAllTokensDone() {
			// All the followers completed work

			// TODO: set progress 100%

			logging.Infof("Pauser::processPauseUploadTokenAsMaster: No Tokens Found. Mark Done.")

			err := p.masterUploadPauseMetadata()
			if err == nil {
				p.masterIncrProgress(5.0)
			}

			p.cancelMetakv()

			go p.finishPause(err)
		}

		return true

	default:
		return false

	}

}

func (p *Pauser) finishPause(err error) {

	if p.retErr == nil && err != nil {
		p.retErr = err
	}

	p.cleanupOnce.Do(p.doFinish)
}

func (p *Pauser) doFinish() {
	logging.Infof("Pauser::doFinish Cleanup: retErr[%v]", p.retErr)

	// TODO: signal others that we are cleaning up using done channel

	p.Cleanup()
	p.wg.Wait()

	// call done callback to start the cleanup phase
	p.cb.done(p.task.taskId, p.retErr)
}

func (p *Pauser) processPauseUploadTokenAsFollower(putId string, put *common.PauseUploadToken) bool {

	logging.Infof("Pauser::processPauseUploadTokenAsFollower: putId[%v] put[%v]", putId, put)

	if put.PauseId != p.task.taskId {
		logging.Warnf("Pauser::processPauseUploadTokenAsFollower: Found PauseUploadToken[%v] with Unknown "+
			"PauseId. Expected to match local taskId[%v]", put, p.task.taskId)

		return true
	}

	if !p.checkValidNotifyState(putId, put, "follower") {
		return true
	}

	switch put.State {

	case common.PauseUploadTokenPosted:
		// Follower owns token, update in-memory and move to InProgress State

		p.updateInMemToken(putId, put, "follower")

		put.State = common.PauseUploadTokenInProgess
		setPauseUploadTokenInMetakv(putId, put)

		return true

	case common.PauseUploadTokenInProgess:
		// Follower owns token, update in-memory and start pause work

		p.updateInMemToken(putId, put, "follower")

		go p.startPauseUpload(putId, put)

		return true

	case common.PauseUploadTokenProcessed:
		// Master owns token, follower work is done cleanup and start watcher

		p.updateInMemToken(putId, put, "follower")

		return false

	default:
		return false
	}
}

func (p *Pauser) startPauseUpload(putId string, put *common.PauseUploadToken) {
	start := time.Now()
	logging.Infof("Pauser::startPauseUpload: Begin work: putId[%v] put[%v]", putId, put)
	defer logging.Infof("Pauser::startPauseUpload: Done work: putId[%v] put[%v] took[%v]",
		putId, put, time.Since(start))

	if !p.addToWaitGroup() {
		logging.Errorf("Pauser::startPauseUpload: Failed to add to pauser waitgroup.")
		return
	}
	defer p.wg.Done()

	shardPaths, err := p.followerUploadBucketData()
	if err != nil {
		put.Error = err.Error()
	} else if shardPaths != nil {
		put.ShardPaths = shardPaths
	}

	// work done, change state, master handler will pick it up and do cleanup.
	put.State = common.PauseUploadTokenProcessed
	setPauseUploadTokenInMetakv(putId, put)
}

// Often, metaKV can send multiple notifications for the same state change (probably
// due to the eventual consistent nature of metaKV). Keep track of all state changes
// in in-memory bookkeeping and ignore the duplicate notifications
func (p *Pauser) checkValidNotifyState(putId string, put *common.PauseUploadToken, caller string) bool {

	// As the default state is "PauseUploadTokenPosted"
	// do not check for valid state changes for this state
	if put.State == common.PauseUploadTokenPosted {
		return true
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	var inMemToken *common.PauseUploadToken
	var ok bool

	if caller == "master" {
		inMemToken, ok = p.masterTokens[putId]
	} else if caller == "follower" {
		inMemToken, ok = p.followerTokens[putId]
	}

	if ok {
		// Token seen before, validate the state

		// < for invalid state change
		// == for duplicate notification
		if put.State <= inMemToken.State {
			logging.Warnf("Pauser::checkValidNotifyState Detected Invalid State Change Notification"+
				" for [%v]. putId[%v] Local[%v] Metakv[%v]", caller, putId, inMemToken.State, put.State)

			return false
		}
	}

	return true
}

func (p *Pauser) updateInMemToken(putId string, put *common.PauseUploadToken, caller string) {

	p.mu.Lock()
	defer p.mu.Unlock()

	if caller == "master" {
		p.masterTokens[putId] = put.Clone()
	} else if caller == "follower" {
		p.followerTokens[putId] = put.Clone()
	}
}

func (p *Pauser) checkAllTokensDone() bool {

	p.mu.Lock()
	defer p.mu.Unlock()

	for putId, put := range p.masterTokens {
		if put.State < common.PauseUploadTokenProcessed {
			// Either posted or processing

			logging.Infof("Pauser::checkAllTokensDone PauseUploadToken: putId[%v] is in state[%v]",
				putId, put.State)

			return false
		}
	}

	return true
}

// restGetLocalIndexMetadataBinary calls the /getLocalndexMetadata REST API (request_handler.go) via
// self-loopback to get the index metadata for the current node and the task's bucket (tenant). This
// verifies it can be unmarshaled, but it returns a checksummed and optionally compressed byte slice
// version of the data rather than the unmarshaled object.
func (p *Pauser) restGetLocalIndexMetadataBinary(compress bool) ([]byte, *manager.LocalIndexMetadata, error) {

	metadata, bs, err := p.pauseMgr.restGetLocalIndexMetadata(p.task.bucket)
	if err != nil {
		return nil, nil, err
	}

	if len(metadata.IndexDefinitions) == 0 {
		return nil, nil, nil
	}

	// Return checksummed and optionally compressed byte slice, not the unmarshaled object
	return common.ChecksumAndCompress(bs, compress), metadata, nil
}

// masterUploadPauseMetadata is master's method to upload PauseMetadata to object store
//
// this method should be called after all the followers have finished execution so we can
// correctly upload the metadata about all the follower nodes
//
// meta about follower nodes should be gathered before calling this function
// object structure after upload:
// archivePath/
// └── pauseMetadata.json
func (p *Pauser) masterUploadPauseMetadata() error {
	metadata := p.task.pauseMetadata

	metadata.lock.Lock()
	metadata.setVersionNoLock(common.GetLocalInternalVersion().String())
	metadata.lock.Unlock()

	data, err := json.Marshal(metadata)
	if err != nil {
		logging.Errorf("Pauser::masterUploadPauseMetadata: couldn't marshal pause metadata err :%v  for task ID: %v.", err, p.task.taskId)
		return err
	}

	cfg := p.pauseMgr.config.Load()
	cfgValue, ok := cfg["pause_resume.compression"]
	var compression bool
	if !ok {
		compression = true
	} else {
		compression = cfgValue.Bool()
	}
	if !compression {
		logging.Infof("Pauser::masterUploadPauseMetadata: compression is disabled. will upload raw data")
	}

	data = common.ChecksumAndCompress(data, compression)

	ctx := p.task.ctx
	copier, cerr := MakeFileCopierForPauseResume(p.task, cfg)
	if cerr != nil {
		err = fmt.Errorf("couldn't create copier object. archive path %v is unsupported (err=%v)", p.task.archivePath, cerr)
		logging.Errorf("Pauser::masterUploadPauseMetadata: %v", err)
		return err
	}

	url, err := copier.GetPathEncoding(fmt.Sprintf("%v%v", p.task.archivePath, FILENAME_PAUSE_METADATA))
	if err != nil {
		logging.Errorf("Pauser::masterUploadPauseMetadata: url encoding failed, err: %v to %v%v for task ID: %v", err, p.task.archivePath, FILENAME_METADATA, p.task.taskId)
		return err
	}
	_, err = copier.UploadBytes(ctx, data, url)

	if err != nil {
		logging.Errorf("Pauser::masterUploadPauseMetadata: upload of pause metadata failed to path : %v%v err: %v for task ID %v", p.task.archivePath, FILENAME_PAUSE_METADATA, err, p.task.taskId)
		return err
	}
	logging.Infof("Pauser::masterUploadPauseMetadata: successful upload of pause metadata to %v%v for task ID %v", p.task.archivePath, FILENAME_PAUSE_METADATA, p.task.taskId)
	return nil
}

// followerUploadBucketData is follower's method to upload bucket related data to object store
//
// it gathers bucket's indexes and uploads:
// * Index Metadata -> FILENAME_METADATA
// * Index Stats -> FILENAME_STATS
// * Start Plasma Shard Transfer
// object store structure after upload:
// archivePath/
// └── node_<nodeId>/
//
//	├── indexMetadata.json
//	├── indexStats.json
//	└── /plasma_storage/PauseResume/<bucketName>/<shardId>/
//		└── plasma shard data
func (p *Pauser) followerUploadBucketData() (map[common.ShardId]string, error) {
	cfg := p.pauseMgr.config.Load()
	cfgValue, ok := cfg["pause_resume.compression"]
	var compression bool
	if !ok {
		compression = true
	} else {
		compression = cfgValue.Bool()
	}
	if !compression {
		logging.Infof("Pauser::followerUploadBucketData: compression is disabled. will upload raw data")
	}

	copier, cerr := MakeFileCopierForPauseResume(p.task, p.pauseMgr.config.Load())
	if cerr != nil {
		err := fmt.Errorf("couldn't create a copier object. archive path %v is unsupported (err=%v)", p.task.archivePath, cerr)
		logging.Errorf("Pauser::followerUploadBucketData: %v", err)
		return nil, err
	}
	ctx := p.task.ctx

	logging.Tracef("Pauser::followerUploadBucketData: uploading data to path %v", p.nodeDir)

	// Step 1. Gather bucket's indexes data
	byteSlice, indexMetadata, err := p.restGetLocalIndexMetadataBinary(compression)
	if err != nil {
		logging.Errorf("Pauser::followerUploadBucketData: failed to get local index metadata err :%v for task ID: %v bucket: %v", err, p.task.taskId, p.task.bucket)
		return nil, err
	}
	if byteSlice == nil {
		// there are no indexes on this node for bucket. pause is a no-op
		logging.Infof("Pauser::followerUploadBucketData: pause is a no-op for bucket %v task ID %v", p.task.bucket, p.task.taskId)

		p.followerIncrProgress(100.0)

		return nil, nil
	}

	// Step 2. Upload index metadata
	url, err := copier.GetPathEncoding(fmt.Sprintf("%v%v", p.nodeDir, FILENAME_METADATA))
	if err != nil {
		logging.Errorf("Pauser::followerUploadBucketData: url encoding failed, err: %v to %v%v for task ID: %v", err, p.nodeDir, FILENAME_METADATA, p.task.taskId)
		return nil, err
	}
	_, err = copier.UploadBytes(ctx, byteSlice, url)
	if err != nil {
		logging.Errorf("Pauser::followerUploadBucketData: metadata upload failed, err: %v to %v%v for task ID: %v", err, p.nodeDir, FILENAME_METADATA, p.task.taskId)
		return nil, err
	}

	p.followerIncrProgress(5.0)

	logging.Infof("Pauser::followerUploadBucketData: metadata successfully uploaded to %v%v for taskId %v", p.nodeDir, FILENAME_METADATA, p.task.taskId)

	// Step 3. Gather index stats
	getIndexInstanceIds := func() []common.IndexInstId {
		res := make([]common.IndexInstId, 0, len(indexMetadata.IndexDefinitions))
		for _, topology := range indexMetadata.IndexTopologies {
			for _, indexDefn := range topology.Definitions {
				res = append(res, common.IndexInstId(indexDefn.Instances[0].InstId))
			}
		}
		logging.Tracef("Pauser::followerUploadBucketData::getIndexInstanceId: index instance ids: %v for bucket %v", res, p.task.bucket)
		return res
	}

	byteSlice, err = p.pauseMgr.genericMgr.statsMgr.GetStatsForIndexesToBePersisted(getIndexInstanceIds(), compression)
	if err != nil {
		logging.Errorf("Pauser::followerUploadBucketData: couldn't get stats for indexes err: %v for task ID: %v", err, p.task.taskId)
		return nil, err
	}

	// Step 4. Upload index stats
	url, err = copier.GetPathEncoding(fmt.Sprintf("%v%v", p.nodeDir, FILENAME_STATS))
	if err != nil {
		logging.Errorf("Pauser::followerUploadBucketData: url encoding failed, err: %v to %v%v for task ID: %v", err, p.nodeDir, FILENAME_METADATA, p.task.taskId)
		return nil, err
	}
	_, err = copier.UploadBytes(ctx, byteSlice, url)
	if err != nil {
		logging.Errorf("Pauser::followerUploadBucketData: stats upload failed, err: %v to %v%v for task ID: %v", err, p.nodeDir, FILENAME_STATS, p.task.taskId)
		return nil, err
	}

	p.followerIncrProgress(5.0)

	logging.Infof("Pauser::followerUploadBucketData: stats successfully uploaded to %v%v for taskId %v", p.nodeDir, FILENAME_STATS, p.task.taskId)

	// Step 5. Initiate plasma shard transfer
	shardIds := func() []common.ShardId {
		uniqueShardIds := make(map[common.ShardId]bool)
		for _, topology := range indexMetadata.IndexTopologies {
			for _, indexDefn := range topology.Definitions {
				for _, instance := range indexDefn.Instances {
					for _, partition := range instance.Partitions {
						for _, shard := range partition.ShardIds {
							uniqueShardIds[shard] = true
						}
					}
				}
			}
		}

		shardIds := make([]common.ShardId, 0, len(uniqueShardIds))
		for shardId := range uniqueShardIds {
			shardIds = append(shardIds, shardId)
		}
		logging.Tracef("Pauser::followerUploadBucketData::getShardIds: found shard Ids %v for bucket %v", shardIds, p.task.bucket)

		return shardIds
	}()

	p.shardIds = shardIds

	// TODO: add contextWithCancel to task and reuse it here
	closeCh := p.task.ctx.Done()
	shardPaths, err := p.pauseMgr.copyShardsWithLock(shardIds, p.task.taskId, p.task.bucket,
		p.nodeDir, closeCh,
		func(incr float64) { p.followerIncrProgress(incr * 90.0 / float64(len(shardIds))) },
	)
	if err != nil {
		return nil, err
	}
	for shardId, shardPath := range shardPaths {
		shardPaths[shardId] = strings.TrimPrefix(shardPath, p.nodeDir)
	}
	return shardPaths, nil
}

// cleanupNoLocks stops any ongoing operation and starts bucket endpoint watchers
func (p *Pauser) cleanupNoLocks() {
	p.task.cancelNoLock()
}

func (p *Pauser) Cleanup() {
	p.task.taskMu.Lock()
	p.cleanupNoLocks()
	p.task.taskMu.Unlock()
	p.cancelMetakv()
}

func collectComputeAndReportProgress(method, taskId string, followerNodes map[service.NodeID]string,
	progressUpdateCallback PauseResumeProgressCallback, masterProgress *float64Holder,
	progressUpdateDuration time.Duration, wg *sync.WaitGroup, closeCh <-chan struct{}) {

	wg.Add(1)
	defer wg.Done()

	logging.Infof("%v starting progress collection and reporting for task ID %v", method, taskId)

	var totalNodes float64 = float64(len(followerNodes))
	perNodeProgress := make(map[string]float64, len(followerNodes))
	var pnpLock sync.RWMutex
	progressUpdateCh := make(chan struct {
		nodeUuid string
		progress float64
	}, len(followerNodes))

	// collect from followers
	go collectPauserResumerProgress(followerNodes, method, taskId, progressUpdateCh, closeCh,
		progressUpdateDuration)

	// gather updates
	go func() {
		for {
			select {
			case <-closeCh:
				return
			case progressUpdate, ok := <-progressUpdateCh:
				if !ok {
					if len(progressUpdateCh) != 0 {
						pnpLock.Lock()
						for progressUpdate = range progressUpdateCh {
							perNodeProgress[progressUpdate.nodeUuid] = progressUpdate.progress
						}
						pnpLock.Unlock()
					}
					return
				}
				pnpLock.Lock()
				perNodeProgress[progressUpdate.nodeUuid] = progressUpdate.progress
				pnpLock.Unlock()
			}
		}
	}()

	ticker := time.NewTicker(progressUpdateDuration)
	defer ticker.Stop()
	for {
		select {
		case <-closeCh:
			logging.Infof("%v closing progress collection and reporting for task ID %v", method,
				taskId)
			return
		case <-ticker.C:
			masterProgress := masterProgress.GetFloat64()
			pnpLock.RLock()
			for _, progress := range perNodeProgress {
				masterProgress += progress * (90.0 / totalNodes)
			}
			pnpLock.RUnlock()
			progressUpdateCallback(taskId, masterProgress, perNodeProgress)
		}
	}
}

func (p *Pauser) masterIncrProgress(incr float64) {
	incrProgress(&p.masterProgress, incr)
	p.cb.progress(p.task.taskId, p.masterProgress.GetFloat64(), nil)
}

func (p *Pauser) followerIncrProgress(incr float64) {
	incrProgress(&p.followerProgress, incr)
}

func incrProgress(progress *float64Holder, incr float64) {
	prog := progress.GetFloat64()
	if prog == 100.0 {
		// no need to perform Store operations
		return
	}
	prog += incr
	if prog > 100.0 {
		prog = 100.0
	}
	progress.SetFloat64(prog)
}

func collectPauserResumerProgress(followerNodes map[service.NodeID]string, method, taskId string,
	progressUpdateCh chan<- struct {
		nodeUuid string
		progress float64
	}, closeCh <-chan struct{},
	progressUpdateDuration time.Duration) {
	hostAddrs := make(map[service.NodeID]string, len(followerNodes))
	for nodeUuid, addr := range followerNodes {
		hostAddrs[nodeUuid] = addr
	}
	url := fmt.Sprintf("/pauseMgr/Progress?id=%v", taskId)

	// keep collector tick duration less that reporter to ensure we have updates
	collectorDuration := progressUpdateDuration - (500 * time.Millisecond)
	if collectorDuration <= 0 {
		collectorDuration = 500 * time.Millisecond
	}
	for {
		select {
		case <-closeCh:
			// only writers close channels in go
			close(progressUpdateCh)
			return
		default:
			// if node is slow to response, we could have multiple ticks lined up
			var collectWg sync.WaitGroup
			for nodeUuid, hostAddr := range hostAddrs {
				collectWg.Add(1)
				go func(nodeUuid service.NodeID, hostAddr string) {
					defer collectWg.Done()

					resp, err := getWithAuth(hostAddr + url)
					if err != nil {
						logging.Warnf("%v http request failed for %v. err: %v for task ID %v",
							method, hostAddr, err, taskId)
						return
					}
					defer resp.Body.Close()

					var progress float64
					body, err := ioutil.ReadAll(resp.Body)
					if err != nil {
						logging.Warnf("%v failed to read response body. err: %v for task ID %v",
							method, err, taskId)
						return
					}
					err = json.Unmarshal(body, &progress)
					if err != nil {
						logging.Warnf("%v couldn't unmarshal response body(%v) to float64. err: %v for task ID %v",
							method, body, err, taskId)
						return
					}

					if progress >= 100.0 {
						delete(hostAddrs, nodeUuid)
					}

					progressUpdateCh <- struct {
						nodeUuid string
						progress float64
					}{nodeUuid: string(nodeUuid), progress: progress}
				}(nodeUuid, hostAddr)
			}
			collectWg.Wait()
			// not using a ticker - if nodes are slow to respond, we could have multiple ticks
			// on channel, and batch of requests will go consecutively; instead, we want to
			// wait for collectorDuration after every batch of collection
			time.Sleep(collectorDuration)
		}
	}
}
