package indexer

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/indexing/secondary/common"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	l "github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/manager"
	"github.com/couchbase/indexing/secondary/manager/client"
)

// ShardRebalancer embeds Rebalancer struct to reduce code
// duplication across common functions
type ShardRebalancer struct {
	clusterVersion int64

	// List of transfer tokens sent by planner & as maintained by master
	transferTokens map[string]*c.TransferToken
	sourceTokens   map[string]*c.TransferToken // as maintained by source
	acceptedTokens map[string]*c.TransferToken // as maintained by destination

	// List of all tokens that have been acknowledge by both source
	// and destination nodes
	ackedTokens map[string]*c.TransferToken

	transferStats map[string]map[common.ShardId]*ShardTransferStatistics // ttid -> shardId -> stats

	// lock protecting access to maps like transferTokens, sourceTokens etc.
	mu sync.RWMutex

	rebalToken *RebalanceToken
	nodeUUID   string
	isMaster   bool // true for rebalance master node & false otherwise

	cb Callbacks // rebalance progress and rebalance done callbacks

	cancel              chan struct{} // Close to signal rebalance cancellation
	done                chan struct{} // Close to signal completion of rebalance
	waitForTokenPublish chan struct{}

	isDone int32 // Atomically updated if rebalance is done

	supvMsgch   MsgChannel
	localaddr   string
	wg          sync.WaitGroup
	cleanupOnce sync.Once
	config      c.ConfigHolder
	retErr      error

	// topologyChange is populated in Rebalance and Failover cases only, else nil
	topologyChange *service.TopologyChange

	// Metakv management
	metakvCancel chan struct{}
	metakvMutex  sync.RWMutex

	// Dropping of shards
	dropQueue  chan string     // ttids of source indexes waiting to be submitted for drop
	dropQueued map[string]bool // set of ttids already added to dropQueue, as metakv can send duplicate notifications

	runPlanner bool

	runParams *runParams // For DDL during rebalance
	statsMgr  *statsManager
}

func NewShardRebalancer(transferTokens map[string]*c.TransferToken, rebalToken *RebalanceToken,
	nodeUUID string, master bool, progress ProgressCallback, done DoneCallback,
	supvMsgch MsgChannel, localaddr string, config c.Config, topologyChange *service.TopologyChange,
	runPlanner bool, runParams *runParams, statsMgr *statsManager) *ShardRebalancer {

	clusterVersion := common.GetClusterVersion()
	l.Infof("NewShardRebalancer nodeId %v rebalToken %v master %v localaddr %v runPlanner %v runParam %v clusterVersion %v", nodeUUID,
		rebalToken, master, localaddr, runPlanner, runParams, clusterVersion)

	sr := &ShardRebalancer{
		clusterVersion: clusterVersion,

		transferTokens: transferTokens,
		rebalToken:     rebalToken,
		isMaster:       master,
		nodeUUID:       nodeUUID,
		runPlanner:     runPlanner,
		runParams:      runParams,
		statsMgr:       statsMgr,
		supvMsgch:      supvMsgch,
		localaddr:      localaddr,

		cb: Callbacks{progress, done},

		acceptedTokens: make(map[string]*c.TransferToken),
		sourceTokens:   make(map[string]*c.TransferToken),
		ackedTokens:    make(map[string]*c.TransferToken),

		cancel:              make(chan struct{}),
		done:                make(chan struct{}),
		metakvCancel:        make(chan struct{}),
		waitForTokenPublish: make(chan struct{}),

		topologyChange: topologyChange,
		transferStats:  make(map[string]map[common.ShardId]*ShardTransferStatistics),

		dropQueue:  make(chan string, 10000),
		dropQueued: make(map[string]bool),
	}

	sr.config.Store(config)

	if master {
		go sr.initRebalAsync()
	} else {
		close(sr.waitForTokenPublish)
		go sr.observeRebalance()
	}
	go sr.processDropShards()

	return sr
}

func (sr *ShardRebalancer) observeRebalance() {
	l.Infof("ShardRebalancer::observeRebalance %v master:%v", sr.rebalToken, sr.isMaster)

	<-sr.waitForTokenPublish

	err := metakv.RunObserveChildren(RebalanceMetakvDir, sr.processShardTokens, sr.metakvCancel)
	if err != nil {
		l.Infof("ShardRebalancer::observeRebalance Exiting On Metakv Error %v", err)
		sr.finishRebalance(err)
	}
	l.Infof("ShardRebalancer::observeRebalance exiting err %v", err)
}

func (sr *ShardRebalancer) initRebalAsync() {

	//short circuit
	if len(sr.transferTokens) == 0 && !sr.runPlanner {
		sr.cb.progress(1.0, sr.cancel)
		sr.finishRebalance(nil)
		return
	}

	// Launch the progress updater goroutine.
	// Computation of progress is different for ShardRebalancer and Rebalancer
	// Hence, share updateProgress code but have different implementations of
	// computeProgress
	if sr.cb.progress != nil {
		go sr.updateProgress()
	}

	if sr.runPlanner {
		cfg := sr.config.Load()
	loop:
		for {
			select {
			case <-sr.cancel:
				l.Infof("Rebalancer::initRebalAsync Cancel Received")
				return

			case <-sr.done:
				l.Infof("Rebalancer::initRebalAsync Done Received")
				return

			default:
				allWarmedup, _ := checkAllIndexersWarmedup(cfg["clusterAddr"].String())
				if !allWarmedup {
					l.Errorf("Rebalancer::initRebalAsync All Indexers Not Active. Waiting...")
					time.Sleep(5 * time.Second)
					continue
				}

				// TODO: Add planner related logic and integrate with planner

				if len(sr.transferTokens) == 0 {
					sr.transferTokens = nil
				}

				break loop
			}
		}
	}

	go sr.doRebalance()
}

// processTokens is invoked by observeRebalance() method
// processTokens invokes processShardTokens of ShardRebalancer
func (sr *ShardRebalancer) processShardTokens(kve metakv.KVEntry) error {

	if kve.Path == RebalanceTokenPath || kve.Path == MoveIndexTokenPath {
		l.Infof("ShardRebalancer::processTokens RebalanceToken %v %s", kve.Path, kve.Value)
		if kve.Value == nil {
			l.Infof("ShardRebalancer::processTokens Rebalance Token Deleted. Mark Done.")
			sr.cancelMetakv()
			sr.finishRebalance(nil)
		}
	} else if strings.Contains(kve.Path, TransferTokenTag) {
		if kve.Value != nil {
			ttid, tt, err := decodeTransferToken(kve.Path, kve.Value)
			if err != nil {
				l.Errorf("ShardRebalancer::processTokens Unable to decode transfer token. Ignored.")
				return nil
			}
			sr.processShardTransferToken(ttid, tt)
		} else {
			l.Infof("ShardRebalancer::processTokens Received empty or deleted transfer token %v", kve.Path)
		}
	}

	return nil
}

func (sr *ShardRebalancer) processShardTransferToken(ttid string, tt *c.TransferToken) {
	if !sr.addToWaitGroup() {
		return
	}

	defer sr.wg.Done()

	//TODO (7.2.0): Transfer token can be processed if there
	// are conflicts with rebalance movements. Update this logic
	// under allow DDL during rebalance part
	if ddl, err := sr.runParams.checkDDLRunning("ShardRebalancer"); ddl {
		sr.setTransferTokenError(ttid, tt, err.Error())
		return
	}

	if !tt.IsShardTransferToken() {
		err := fmt.Errorf("ShardRebalancer::processShardTransferToken Transfer token is not for transferring shard. ttid: %v, tt: %v",
			ttid, tt)
		l.Fatalf(err.Error())
		sr.setTransferTokenError(ttid, tt, err.Error())
		return
	}

	// "processed" var ensures only the incoming token state gets processed by this
	// call, as metakv will call parent processTokens again for each TT state change.
	var processed bool

	if tt.MasterId == sr.nodeUUID {
		processed = sr.processShardTransferTokenAsMaster(ttid, tt)
	}

	if (tt.SourceId == sr.nodeUUID && !processed) || (tt.ShardTransferTokenState == c.ShardTokenReady) {
		processed = sr.processShardTransferTokenAsSource(ttid, tt)
	}

	if tt.DestId == sr.nodeUUID && !processed {
		processed = sr.processShardTransferTokenAsDest(ttid, tt)
	}
}

func (sr *ShardRebalancer) processShardTransferTokenAsMaster(ttid string, tt *c.TransferToken) bool {

	if tt.RebalId != sr.rebalToken.RebalId {
		l.Warnf("ShardRebalancer::processShardTransferTokenAsMaster Found TransferToken with Unknown "+
			"RebalanceId. Local RId %v Token %v. Ignored.", sr.rebalToken.RebalId, tt)
		return true
	}

	// Finish rebalance so that rebalance_service_manager will take care
	// of initiating clean-up for other transfer tokens in the batch
	//
	// TODO: Update logic in rebalance service manager to clean-up on-going
	// transfer tokens
	if tt.Error != "" {
		l.Errorf("Rebalancer::processShardTransferTokenAsMaster Detected TransferToken in Error state %v. Abort.", tt)

		sr.cancelMetakv()
		go sr.finishRebalance(errors.New(tt.Error))
		return true
	}

	if !sr.checkValidNotifyState(ttid, tt, "master") {
		return true
	}

	switch tt.ShardTransferTokenState {

	case c.ShardTokenScheduleAck:

		sr.mu.Lock()
		defer sr.mu.Unlock()

		sr.ackedTokens[ttid] = tt.Clone()
		sr.transferTokens[ttid] = tt.Clone() // Update in-memory book-keeping with new state

		if sr.allShardTransferTokensAcked() {
			sr.initiateShardTransfer()
		}
		return true

	case c.ShardTokenTransferShard:
		// Update the in-memory state but do not process the token
		sr.updateInMemToken(ttid, tt, "master")
		return false

	case c.ShardTokenCommit:
		sr.updateInMemToken(ttid, tt, "master")

		tt.ShardTransferTokenState = c.ShardTokenDeleted
		setTransferTokenInMetakv(ttid, tt)
		return true

	case c.ShardTokenDeleted:
		err := c.MetakvDel(RebalanceMetakvDir + ttid)
		if err != nil {
			l.Fatalf("ShardRebalancer::processShardTransferTokenAsMaster Unable to set TransferToken In "+
				"Meta Storage. %v. Err %v", tt, err)
			c.CrashOnError(err)
		}

		sr.updateInMemToken(ttid, tt, "master")

		if sr.checkAllTokensDone() { // rebalance completed
			if sr.cb.progress != nil {
				sr.cb.progress(1.0, sr.cancel)
			}
			l.Infof("ShardRebalancer::processShardTransferTokenAsMaster No Tokens Found. Mark Done.")
			sr.cancelMetakv()
			go sr.finishRebalance(nil)
		} else {
			sr.initiateShardTransfer()
		}

		return true

	default:
		return false
	}

}

func (sr *ShardRebalancer) processShardTransferTokenAsSource(ttid string, tt *c.TransferToken) bool {

	if tt.RebalId != sr.rebalToken.RebalId {
		l.Warnf("ShardRebalancer::processShardTransferTokenAsSource Found TransferToken with Unknown "+
			"RebalanceId. Local RId %v Token %v. Ignored.", sr.rebalToken.RebalId, tt)
		return true
	}

	if !sr.checkValidNotifyState(ttid, tt, "source") {
		return true
	}

	switch tt.ShardTransferTokenState {

	case c.ShardTokenCreated:

		// TODO: Update in-mem book-keeping  with the list of index
		// movements during rebalance. This information will be used
		// for conflict resolution when  DDL and rebalance co-exist
		// together
		sr.updateInMemToken(ttid, tt, "source")
		tt.ShardTransferTokenState = c.ShardTokenScheduledOnSource
		setTransferTokenInMetakv(ttid, tt)

		// TODO: It is possible for indexer to crash after updating
		// the transfer token state. Include logic to clean-up rebalance
		// in such case
		return true

	case c.ShardTokenTransferShard:
		sr.updateInMemToken(ttid, tt, "source")

		go sr.startShardTransfer(ttid, tt)

		return true

	case c.ShardTokenReady:
		sr.updateInMemToken(ttid, tt, "source")

		if sr.isSiblingTokenReady(tt) {
			sourceTokenId, sourceTT := sr.getSourceTokenId(ttid, tt)
			if sourceTT.SourceId == sr.nodeUUID {
				sr.queueDropShardRequests(sourceTokenId)
			}
		}
		return true
	default:
		return false
	}
}

func (sr *ShardRebalancer) startShardTransfer(ttid string, tt *c.TransferToken) {

	respCh := make(chan Message)                            // Carries final response of shard transfer to rebalancer
	progressCh := make(chan *ShardTransferStatistics, 1000) // Carries periodic progress of shard tranfser to indexer

	msg := &MsgStartShardTransfer{
		shardIds:        tt.ShardIds,
		rebalanceId:     sr.rebalToken.RebalId,
		transferTokenId: ttid,
		destination:     tt.Destination,

		cancelCh:   sr.cancel,
		respCh:     respCh,
		progressCh: progressCh,
	}

	sr.supvMsgch <- msg

	for {
		select {
		// Incase rebalance is cancelled upstream, transfer would be
		// aborted and rebalancer would still get a message on respCh
		// with errors as sr.cancel is passed on to downstream
		case respMsg := <-respCh:

			msg := respMsg.(*MsgShardTransferResp)
			errMap := msg.GetErrorMap()
			shardPaths := msg.GetShardPaths()

			for shardId, err := range errMap {
				if err != nil {
					l.Errorf("ShardRebalancer::startShardTransfer Observed error during trasfer"+
						" for destination: %v, shardId: %v, shardPaths: %v, err: %v. Initiating transfer clean-up",
						tt.Destination, shardId, shardPaths, err)

					// Invoke clean-up for all shards even if error is observed for one shard transfer
					sr.initiateShardTransferCleanup(shardPaths, tt.Destination, ttid, tt, err)
					return
				}
			}

			// No errors are observed during shard transfer. Change the state of
			// the transfer token and update metaKV
			sr.mu.Lock()
			defer sr.mu.Unlock()

			tt.ShardTransferTokenState = c.ShardTokenRestoreShard
			tt.ShardPaths = shardPaths
			setTransferTokenInMetakv(ttid, tt)
			return

		case stats := <-progressCh:
			sr.updateTransferStatistics(ttid, stats)
			l.Infof("ShardRebalancer::startShardTranfser ShardId: %v bytesWritten: %v, totalBytes: %v, transferRate: %v",
				stats.shardId, stats.bytesWritten, stats.totalBytes, stats.transferRate)
		}
	}
}

func (sr *ShardRebalancer) updateTransferStatistics(ttid string, stats *ShardTransferStatistics) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	if _, ok := sr.transferStats[ttid]; !ok {
		sr.transferStats[ttid] = make(map[common.ShardId]*ShardTransferStatistics)
	}
	sr.transferStats[ttid][stats.shardId] = stats
}

func (sr *ShardRebalancer) initiateShardTransferCleanup(shardPaths map[common.ShardId]string,
	destination string, ttid string, tt *c.TransferToken, err error) {

	l.Infof("ShardRebalancer::initiateShardTransferCleanup Initiating clean-up for ttid: %v, "+
		"shard paths: %v, destination: %v", ttid, shardPaths, destination)

	respCh := make(chan bool)
	msg := &MsgShardTransferCleanup{
		shardPaths:      shardPaths,
		destination:     destination,
		rebalanceId:     sr.rebalToken.RebalId,
		transferTokenId: ttid,
		respCh:          respCh,
	}

	sr.supvMsgch <- msg

	<-respCh // Wait for response of clean-up

	l.Infof("ShardRebalancer::initiateShardTransferCleanup Done clean-up for ttid: %v, "+
		"shard paths: %v, destination: %v", ttid, shardPaths, destination)

	// Update error in transfer token so that rebalance master
	// will finish the rebalance and clean-up can be invoked for
	// other transfer tokens in the batch depending on their state
	sr.setTransferTokenError(ttid, tt, err.Error())

}

func (sr *ShardRebalancer) isSiblingTokenReady(tt *c.TransferToken) bool {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	if siblingToken, ok := sr.sourceTokens[tt.SiblingTokenId]; ok && siblingToken != nil {
		if tt.ShardTransferTokenState == c.ShardTokenReady &&
			siblingToken.ShardTransferTokenState == c.ShardTokenReady {
			return true
		}
	}
	return false
}

func (sr *ShardRebalancer) getSourceTokenId(ttid string, tt *c.TransferToken) (string, *c.TransferToken) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	if tt.SourceId == sr.nodeUUID {
		return ttid, tt
	} else {
		return tt.SiblingTokenId, sr.sourceTokens[tt.SiblingTokenId]
	}
}

func (sr *ShardRebalancer) queueDropShardRequests(ttid string) {
	const method string = "ShardRebalancer::queueDropShardRequests:" // for logging

	select {
	case <-sr.cancel:
		l.Warnf("ShardRebalancer::queueDropShardRequests: Cannot drop shards for ttid: %v "+
			"when rebalance being canceled.", ttid)
		return

	case <-sr.done:
		l.Warnf("ShardRebalancer::queueDropShardRequests: Cannot drop shards for ttid: %v "+
			"when rebalance is done.", ttid)
		return

	default:
		if _, ok := sr.dropQueued[ttid]; !ok {
			sr.dropQueued[ttid] = true
			sr.dropQueue <- ttid
			logging.Infof("ShardRebalancer::queueDropShardRequests: Queued ttid: %v for drop", ttid)
		} else {
			logging.Warnf("ShardRebalancer::queueDropShardRequests: Skipped ttid: %v that was previously queued for drop", ttid)
		}
	}
}

func (sr *ShardRebalancer) processShardTransferTokenAsDest(ttid string, tt *c.TransferToken) bool {

	if tt.RebalId != sr.rebalToken.RebalId {
		l.Warnf("ShardRebalancer::processShardTransferTokenAsDest Found TransferToken with Unknown "+
			"RebalanceId. Local RId %v Token %v. Ignored.", sr.rebalToken.RebalId, tt)
		return true
	}

	if !sr.checkValidNotifyState(ttid, tt, "dest") {
		return true
	}

	switch tt.ShardTransferTokenState {

	case c.ShardTokenScheduledOnSource:

		// TODO: Update in-mem book-keeping  with the list of index
		// movements during rebalance. This information will be used
		// for conflict resolution when  DDL and rebalance co-exist
		// together
		sr.updateInMemToken(ttid, tt, "dest")
		tt.ShardTransferTokenState = c.ShardTokenScheduleAck
		setTransferTokenInMetakv(ttid, tt)

		// TODO: It is possible for destination node to crash
		// after updating metakv state. Include logic to clean-up
		// rebalance in such case
		return true

	case c.ShardTokenRestoreShard:
		sr.updateInMemToken(ttid, tt, "dest")

		go sr.startRestoreShard(ttid, tt)

		return true

	case c.ShardTokenRecoverShard:
		sr.updateInMemToken(ttid, tt, "dest")

		go sr.startShardRecovery(ttid, tt)

		return true

	default:
		return false
	}
}

func (sr *ShardRebalancer) startRestoreShard(ttid string, tt *c.TransferToken) {
	respCh := make(chan Message)                            // Carries final response of shard restore to rebalancer
	progressCh := make(chan *ShardTransferStatistics, 1000) // Carries periodic progress of shard restore to indexer

	msg := &MsgStartShardRestore{
		shardPaths:      tt.ShardPaths,
		rebalanceId:     sr.rebalToken.RebalId,
		transferTokenId: ttid,
		destination:     tt.Destination,

		cancelCh:   sr.cancel,
		respCh:     respCh,
		progressCh: progressCh,
	}

	sr.supvMsgch <- msg

	for {
		select {
		case respMsg := <-respCh:

			msg := respMsg.(*MsgShardTransferResp)
			errMap := msg.GetErrorMap()
			shardPaths := msg.GetShardPaths()

			for shardId, err := range errMap {
				if err != nil {
					// If there are any errors during restore, the restored data on local file system
					// is cleaned by the destination node. The data on S3 will be cleaned by the rebalance
					// source node. Rebalance source node is the only writer (insert/delete) of data on S3
					l.Errorf("ShardRebalancer::startRestoreShard Observed error during trasfer"+
						" for destination: %v, shardId: %v, shardPaths: %v, err: %v. Initiating transfer clean-up",
						tt.Destination, shardId, shardPaths, err)

					// Invoke clean-up for all shards even if error is observed for one shard transfer
					sr.initiateLocalShardCleanup(ttid, shardPaths, tt, err)
					return
				}
			}

			// No errors are observed during shard transfer. Change the state of
			// the transfer token and update metaKV
			sr.mu.Lock()
			defer sr.mu.Unlock()

			tt.ShardTransferTokenState = c.ShardTokenRecoverShard
			setTransferTokenInMetakv(ttid, tt)
			return

		case stats := <-progressCh:
			sr.updateTransferStatistics(ttid, stats)
			l.Infof("ShardRebalancer::startRestoreShard ShardId: %v bytesWritten: %v, totalBytes: %v, transferRate: %v",
				stats.shardId, stats.bytesWritten, stats.totalBytes, stats.transferRate)
		}
	}
}

// Cleans-up the shard data from local file system
func (sr *ShardRebalancer) initiateLocalShardCleanup(ttid string, shardPaths map[common.ShardId]string,
	tt *c.TransferToken, err error) {

	l.Infof("ShardRebalancer::initiateLocalShardCleanup Initiating clean-up on local file "+
		"system for ttid: %v, shards: %v ", ttid, shardPaths)

	shardIds := make([]common.ShardId, 0)
	for shardId, _ := range shardPaths {
		shardIds = append(shardIds, shardId)
	}

	respCh := make(chan bool)

	msg := &MsgDestroyLocalShardData{
		shardIds: shardIds,
		respCh:   respCh,
	}

	sr.supvMsgch <- msg

	// Wait for response
	<-respCh

	// Update error in transfer token so that rebalance master
	// will finish the rebalance and clean-up can be invoked for
	// other transfer tokens in the batch depending on their state
	sr.setTransferTokenError(ttid, tt, err.Error())
}

func (sr *ShardRebalancer) startShardRecovery(ttid string, tt *c.TransferToken) {

	for _, inst := range tt.IndexInsts {
		if inst.Defn.Deferred {

			select {
			case <-sr.cancel:
				l.Infof("ShardRebalancer::startShardRecovery rebalance cancel received")
				return // return for now. Cleanup will take care of dropping the index instances

			case <-sr.done:
				l.Infof("ShardRebalancer::startShardRecovery rebalance done received")
				return // return for now. Cleanup will take care of dropping the index instances

			default:

				if err := sr.postCreateDeferredIndexReq(inst.Defn, inst.InstId, inst.RealInstId, ttid, tt); err != nil {
					sr.setTransferTokenError(ttid, tt, err.Error()) // Set transfer token error and return
					return
				}

				sr.destTokenToMergeOrReady(inst.InstId, inst.RealInstId, ttid, tt)
			}
		}
	}

	// All deferred indexes are created now. Create and recover non-deferred indexes
	indexInsts := tt.IndexInsts
	groupedDefns := groupInstsPerColl(indexInsts)

	for cid, defns := range groupedDefns {
		var buildDefnIdList client.IndexIdList
		// Post recover index request for all non-deferred definitions
		// of this collection. Once all indexes are recovered, they can be
		// built in a batch
		for _, defn := range defns {
			defn.Deferred = true

			select {
			case <-sr.cancel:
				l.Infof("ShardRebalancer::startShardRecovery rebalance cancel received")
				return // return for now. Cleanup will take care of dropping the index instances

			case <-sr.done:
				l.Infof("ShardRebalancer::startShardRecovery rebalance done received")
				return // return for now. Cleanup will take care of dropping the index instances

			default:
				if err := sr.postRecoverIndexReq(defn, ttid, tt); err != nil {
					// Set transfer token error and return
					sr.setTransferTokenError(ttid, tt, err.Error())
					return
				}
				// On a successful request, update book-keeping and process
				// the next definition
				buildDefnIdList.DefnIds = append(buildDefnIdList.DefnIds, uint64(defn.DefnId))

				if err := sr.waitForIndexState(c.INDEX_STATE_RECOVERED, buildDefnIdList, ttid, tt); err != nil {
					sr.setTransferTokenError(ttid, tt, err.Error())
					return
				}
			}
		}
		logging.Infof("ShardRebalancer::startShardRecovery Successfully posted "+
			"recoveryIndexRebalance requests for defnIds: %v belonging to collectionId: %v, ttid: %v. "+
			"Initiating build", buildDefnIdList.DefnIds, cid, ttid)

		select {
		case <-sr.cancel:
			l.Infof("ShardRebalancer::startShardRecovery rebalance cancel received")
			return // return for now. Cleanup will take care of dropping the index instances

		case <-sr.done:
			l.Infof("ShardRebalancer::startShardRecovery rebalance done received")
			return // return for now. Cleanup will take care of dropping the index instances

		default:
			if err := sr.postBuildIndexesReq(buildDefnIdList, ttid, tt); err != nil {
				sr.setTransferTokenError(ttid, tt, err.Error())
				return
			}

			if err := sr.waitForIndexState(c.INDEX_STATE_ACTIVE, buildDefnIdList, ttid, tt); err != nil {
				sr.setTransferTokenError(ttid, tt, err.Error())
				return
			}
		}
	}

	// Coming here means that all indexes are actively build without any error
	// Move the transfer token state to Ready
	tt.ShardTransferTokenState = c.ShardTokenReady
	setTransferTokenInMetakv(ttid, tt)
}

func groupInstsPerColl(indexInsts []common.IndexInst) map[string][]common.IndexDefn {
	out := make(map[string][]common.IndexDefn)

	for _, inst := range indexInsts {
		if inst.Defn.Deferred {
			continue // Ignore deferred indexes as they are already built
		}

		defn := inst.Defn
		defn.SetCollectionDefaults()

		defn.Deferred = true
		defn.Nodes = nil
		defn.InstId = inst.InstId
		defn.RealInstId = inst.RealInstId

		cid := defn.CollectionId
		out[cid] = append(out[cid], defn)
	}

	return out
}

// ShardRebalancer uses this endpoint only for deferred indexes
// For non-deferred indexes, ShardRebalancer uses /recoverIndexRebalance
func (sr *ShardRebalancer) postCreateDeferredIndexReq(indexDefn common.IndexDefn,
	instId, realInstId c.IndexInstId, ttid string, tt *common.TransferToken) error {

	indexDefn.SetCollectionDefaults()
	indexDefn.Nodes = nil
	indexDefn.InstId = instId
	indexDefn.RealInstId = realInstId

	url := "/createIndexRebalance"
	resp, err := postWithHandleEOF(indexDefn, sr.localaddr, url, "ShardRebalancer::postCreateIndexRebalanceReq")
	if err != nil {
		logging.Errorf("ShardRebalancer::postCreateIndexRebalanceReq Error observed when posting request, err: %v", err)
		return err
	}

	response := new(IndexResponse)
	if err := convertResponse(resp, response); err != nil {
		l.Errorf("ShardRebalancer::postCreateIndexRebalanceReq Error unmarshal response %v %v", sr.localaddr+url, err)
		return err
	}

	if response.Error != "" {
		l.Errorf("ShardRebalancer::postCreateIndexRebalanceReq Error received, err: %v", response.Error)
		return errors.New(response.Error)
	}
	return nil
}

func (sr *ShardRebalancer) postRecoverIndexReq(indexDefn common.IndexDefn, ttid string, tt *common.TransferToken) error {
	url := "/recoverIndexRebalance"

	resp, err := postWithHandleEOF(indexDefn, sr.localaddr, url, "ShardRebalancer::postRecoverIndexReq")
	if err != nil {
		logging.Errorf("ShardRebalancer::postRecoverIndexReq Error observed when posting recover index request, err: %v", err)
		return err
	}

	response := new(IndexResponse)
	if err := convertResponse(resp, response); err != nil {
		l.Errorf("ShardRebalancer::postRecoverIndexReq Error unmarshal response %v %v", sr.localaddr+url, err)
		return err
	}

	if response.Error != "" {
		l.Errorf("ShardRebalancer::postRecoverIndexReq Error received, err: %v", response.Error)
		return errors.New(response.Error)
	}
	return nil
}

func (sr *ShardRebalancer) postBuildIndexesReq(defnIdList client.IndexIdList, ttid string, tt *c.TransferToken) error {
	url := "/buildRecoveredIndexesRebalance"

	resp, err := postWithHandleEOF(defnIdList, sr.localaddr, url, "ShardRebalancer::postBuildIndexesReq")
	if err != nil {
		logging.Errorf("ShardRebalancer::postBuildIndexesReq Error observed when posting build indexes request, err: %v", err)
		return err
	}

	response := new(IndexResponse)
	if err := convertResponse(resp, response); err != nil {
		l.Errorf("ShardRebalancer::postBuildIndexesReq Error unmarshal response %v %v", sr.localaddr+url, err)
		return err
	}

	if response.Error != "" {
		l.Errorf("ShardRebalancer::postBuildIndexesReq Error received, err: %v", response.Error)
		return errors.New(response.Error)
	}
	return nil
}

func (sr *ShardRebalancer) waitForIndexState(expectedState c.IndexState, defnIds client.IndexIdList, ttid string, tt *c.TransferToken) error {

	buildStartTime := time.Now()
	cfg := sr.config.Load()
	maxRemainingBuildTime := cfg["rebalance.maxRemainingBuildTime"].Uint64()
	lastLogTime := time.Now()

	defnIdMap := make(map[uint64]bool)
	for _, defnId := range defnIds.DefnIds {
		defnIdMap[defnId] = true
	}

	retryInterval := time.Duration(1)
	retryCount := 0
loop:
	for {

		activeIndexes := make(map[c.IndexInstId]bool) // instId -> bool

		select {
		case <-sr.cancel:
			l.Infof("ShardRebalancer::waitForIndexState Cancel Received")
			break loop
		case <-sr.done:
			l.Infof("ShardRebalancer::waitForIndexState Done Received")
			break loop

		default:
			allStats := sr.statsMgr.stats.Get()

			indexerState := allStats.indexerStateHolder.GetValue().(string)
			if indexerState == "Paused" {
				err := fmt.Errorf("Paused state detected for %v", sr.localaddr)
				func() {
					sr.mu.Lock()
					defer sr.mu.Unlock()

					for ttid, tt := range sr.acceptedTokens {
						l.Errorf("ShardRebalancer::waitForIndexState Token State Changed to Error %v %v", ttid, tt)
						sr.setTransferTokenError(ttid, tt, "Indexer In Paused State")
					}
				}()
				l.Errorf("ShardRebalancer::waitForIndexState err: %v", err)
				return err
			}

			localMeta, err := getLocalMeta(sr.localaddr)
			if err != nil {
				l.Errorf("ShardRebalancer::waitForIndexState Error Fetching Local Meta %v %v", sr.localaddr, err)
				retryCount++

				if retryCount > 5 {
					return err // Return after 5 unsuccessful attempts
				}
				time.Sleep(retryInterval * time.Second)
				goto loop
			}
			retryCount = 0 // reset retryCount as err is nil

			if tt.ShardTransferTokenState != c.ShardTokenRecoverShard {
				err := fmt.Errorf("Transfer token in: %v state. Expected state: %v", tt.ShardTransferTokenState, c.ShardTokenRecoverShard)
				l.Errorf("ShardRebalancer::waitForIndexState err: %v", err)
				return err
			}

			indexStateMap, errMap := sr.getIndexStatusFromMeta(tt, defnIdMap, localMeta)
			for instId, indexState := range indexStateMap {
				err := errMap[instId]
				if err != "" {
					l.Errorf("ShardRebalancer::waitForIndexState Error Fetching Index Status %v %v", sr.localaddr, err)
					retryCount++

					if retryCount > 5 {
						return errors.New(err) // Return after 5 unsuccessful attempts
					}
					time.Sleep(retryInterval * time.Second)
					goto loop // Retry
				}
				retryCount = 0 // reset retryCount as err is nil

				if indexState == c.INDEX_STATE_NIL || indexState == c.INDEX_STATE_DELETED {
					err1 := fmt.Errorf("Could not get index status; bucket/scope/collection likely dropped."+
						" Skipping. instId: %v, indexState %v, tt %v.", instId, indexState, tt)
					logging.Errorf("ShardRebalancer::waitForIndexState, err: %v", err1)
					// Return err and fail rebalance as index definitions not found can lead to
					// violations in cluster affinity
					return err1
				}
			}

			switch expectedState {
			case common.INDEX_STATE_RECOVERED:
				// Check if all index instances have reached this state
				allReachedState := true
				for _, indexState := range indexStateMap {
					if indexState != common.INDEX_STATE_RECOVERED {
						allReachedState = false
						break
					}
				}

				if allReachedState {
					logging.Infof("ShardRebalancer::waitForIndexState: Indexes: %v reached state: %v", indexStateMap, expectedState)
					return nil
				}

				now := time.Now()
				if now.Sub(lastLogTime) > 30*time.Second {
					lastLogTime = now
					logging.Infof("ShardRebalancer::waitForIndexState: Waiting for some indexes to reach state: %v, indexes: %v", expectedState, indexStateMap)
				}
				// retry after "retryInterval" if not all indexes have reached the expectedState

			case common.INDEX_STATE_ACTIVE:

				for _, inst := range tt.IndexInsts {

					defn := inst.Defn
					// Change the RState for deferred indexes. For non-deferred
					// indexes RState will change after build is complete
					if _, ok := defnIdMap[uint64(defn.DefnId)]; !ok {
						continue // Index is a deferred index or not yet created or alredy built
					}

					defn.SetCollectionDefaults()
					defnKey := inst.RealInstId
					if inst.RealInstId == 0 {
						defnKey = inst.InstId
					}
					defnStats := allStats.indexes[defnKey] // stats for current defn
					if defnStats == nil {
						l.Infof("ShardRebalancer::waitForIndexState Missing defnStats for instId %v. Retrying...", defnKey)
						continue // Try next index definition
					}

					// Processing rate calculation and check is to ensure the destination index is not
					// far behind in mutation processing when we redirect traffic from the old source.
					// Indexes become active when they merge to MAINT_STREAM but may still be behind.
					numDocsPending := defnStats.numDocsPending.GetValue().(int64)
					numDocsQueued := defnStats.numDocsQueued.GetValue().(int64)
					numDocsProcessed := defnStats.numDocsProcessed.GetValue().(int64)

					elapsed := time.Since(buildStartTime).Seconds()
					if elapsed == 0 {
						elapsed = 1
					}
					processing_rate := float64(numDocsProcessed) / elapsed
					tot_remaining := numDocsPending + numDocsQueued
					remainingBuildTime := maxRemainingBuildTime
					if processing_rate != 0 {
						remainingBuildTime = uint64(float64(tot_remaining) / processing_rate)
					}
					if tot_remaining == 0 {
						remainingBuildTime = 0
					}

					indexState := indexStateMap[inst.InstId]

					now := time.Now()
					if now.Sub(lastLogTime) > 30*time.Second {
						lastLogTime = now
						l.Infof("ShardRebalancer::waitForIndexState Index: %v:%v:%v:%v State: %v"+
							" DocsPending: %v DocsQueued: %v DocsProcessed: %v, Rate: %v"+
							" Remaining: %v EstTime: %v Partns: %v DestAddr: %v",
							defn.Bucket, defn.Scope, defn.Collection, defn.Name, indexState,
							numDocsPending, numDocsQueued, numDocsProcessed, processing_rate,
							tot_remaining, remainingBuildTime, defn.Partitions, sr.localaddr)
					}
					if indexState == c.INDEX_STATE_ACTIVE && remainingBuildTime < maxRemainingBuildTime {
						activeIndexes[inst.InstId] = true
						sr.destTokenToMergeOrReady(inst.InstId, inst.RealInstId, ttid, tt)
						delete(defnIdMap, uint64(defn.DefnId))
					}
				}

				// If all indexes are built, defnIdMap will have no entries
				if len(defnIdMap) == 0 {
					l.Infof("ShardRebalancer::waitForIndexState All indexes: %v are active and caught up", activeIndexes)
					return nil
				}
			}
		}

		time.Sleep(retryInterval * time.Second)
	}

	return nil
}

func (sr *ShardRebalancer) destTokenToMergeOrReady(instId c.IndexInstId, realInstId c.IndexInstId, ttid string, tt *c.TransferToken) {
	// There is no proxy (no merge needed)
	if realInstId == 0 {

		respch := make(chan error)
		sr.supvMsgch <- &MsgUpdateIndexRState{
			instId: instId,
			rstate: c.REBAL_ACTIVE,
			respch: respch}
		err := <-respch
		c.CrashOnError(err)

		// metaKV state update will happen after all index instances in the shard are built
	} else {
		// TODO: Add support for partitioned indexes
	}
}

func (sr *ShardRebalancer) getIndexStatusFromMeta(tt *c.TransferToken,
	defnIdMap map[uint64]bool, localMeta *manager.LocalIndexMetadata) (map[c.IndexInstId]c.IndexState, map[c.IndexInstId]string) {

	outStates := make(map[c.IndexInstId]c.IndexState)
	outErr := make(map[c.IndexInstId]string)
	for _, inst := range tt.IndexInsts {

		if _, ok := defnIdMap[uint64(inst.Defn.DefnId)]; !ok {
			continue
		}

		topology := findTopologyByCollection(localMeta.IndexTopologies, inst.Defn.Bucket, inst.Defn.Scope, inst.Defn.Collection)
		if topology == nil {
			outStates[inst.InstId] = c.INDEX_STATE_NIL
			outErr[inst.InstId] = fmt.Sprintf("Topology Information Missing for Bucket %v Scope %v Collection %v",
				inst.Defn.Bucket, inst.Defn.Scope, inst.Defn.Collection)
			continue
		}

		state, errMsg := topology.GetStatusByInst(inst.Defn.DefnId, inst.InstId)
		if state == c.INDEX_STATE_NIL && inst.RealInstId != 0 {
			state, errMsg = topology.GetStatusByInst(inst.Defn.DefnId, inst.RealInstId)
		}
		outStates[inst.InstId], outErr[inst.InstId] = state, errMsg
	}

	return outStates, outErr
}

func (sr *ShardRebalancer) doRebalance() {

	if sr.transferTokens == nil {
		sr.cb.progress(1.0, sr.cancel)
		sr.finishRebalance(nil)
		return
	}

	// TODO: For multi-tenancy, DDL's are to be supported while
	// rebalance is in progress. Add the support for the same
	if ddl, err := sr.runParams.checkDDLRunning("ShardRebalancer"); ddl {
		sr.finishRebalance(err)
		return
	}

	select {
	case <-sr.cancel:
		l.Infof("Rebalancer::doRebalance Cancel Received. Skip Publishing Tokens.")
		return

	default:
		// Publish all transfer tokens to metaKV so that rebalance
		// source and destination nodes are aware of potential index
		// movements during rebalance
		sr.publishShardTransferTokens()
		close(sr.waitForTokenPublish)
		go sr.observeRebalance()
	}
}

func (sr *ShardRebalancer) publishShardTransferTokens() {
	for ttid, tt := range sr.transferTokens {
		setTransferTokenInMetakv(ttid, tt)
		l.Infof("ShardRebalancer::publishShardTransferTokens Published transfer token: %v", ttid)
	}
}

// Acquire "mu" before calling this method as "transferTokens"
// and "ackedTokens" are being accessed in this method
func (sr *ShardRebalancer) allShardTransferTokensAcked() bool {
	if len(sr.transferTokens) != len(sr.ackedTokens) {
		return false
	}

	for ttid, _ := range sr.transferTokens {
		if _, ok := sr.ackedTokens[ttid]; !ok {
			return false
		}
	}

	l.Infof("ShardRebalancer::allShardTransferTokensAcked All transfer tokens " +
		"moded to ScheduleAck state. Initiating transfer for futher processing")

	return true
}

func (sr *ShardRebalancer) updateInMemToken(ttid string, tt *c.TransferToken, caller string) {

	sr.mu.Lock()
	defer sr.mu.Unlock()

	if caller == "master" {
		sr.transferTokens[ttid] = tt.Clone()
	} else if caller == "source" {
		sr.sourceTokens[ttid] = tt.Clone()
	} else if caller == "dest" {
		sr.acceptedTokens[ttid] = tt.Clone()
	}
}

// tokenMap is the in-memory version of the token state as maintained
// by shard rebalancer. "tt" is the transfer token received through notification
// from metaKV.
//
// Often, metaKV can send multiple notifications for the same state change
// (probably due to the eventual consistent nature of metaKV). ShardRebalancer
// will keep a track of all state changes in its in-memory book-keeping and
// ignores the duplicate notifications
func (sr *ShardRebalancer) checkValidNotifyState(ttid string, tt *c.TransferToken, caller string) bool {

	// As the default state is "ShardTokenCreated"
	// do not check for valid state changes for this state
	if tt.ShardTransferTokenState == c.ShardTokenCreated {
		return true
	}

	sr.mu.RLock()
	defer sr.mu.RUnlock()

	var inMemToken *c.TransferToken
	var ok bool

	if caller == "master" {
		inMemToken, ok = sr.transferTokens[ttid]
	} else if caller == "source" {
		inMemToken, ok = sr.sourceTokens[ttid]
	} else if caller == "dest" {
		inMemToken, ok = sr.acceptedTokens[ttid]
	}

	if ok {
		if tt.ShardTransferTokenState <= inMemToken.ShardTransferTokenState {
			l.Warnf("ShardRebalancer::checkValidNotifyState Detected Invalid State "+
				"Change Notification for %v. Token Id %v Local State %v Metakv State %v",
				caller, ttid, inMemToken.ShardTransferTokenState, tt.ShardTransferTokenState)
			return false
		}
	}
	return true
}

// updateProgress runs in a master-node go routine to update the progress of processing
// a single transfer token
func (sr *ShardRebalancer) updateProgress() {

	if !sr.addToWaitGroup() {
		return
	}
	defer sr.wg.Done()

	l.Infof("ShardRebalancer::updateProgress goroutine started")
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			progress := sr.computeProgress()
			sr.cb.progress(progress, sr.cancel)
		case <-sr.cancel:
			l.Infof("ShardRebalancer::updateProgress Cancel Received")
			return
		case <-sr.done:
			l.Infof("ShardRebalancer::updateProgress Done Received")
			return
		}
	}
}

// Shard rebalancer's version of compute progress method
func (sr *ShardRebalancer) computeProgress() float64 {
	// TODO: Implement the computation of progress for shard rebalance
	return 0
}

func (sr *ShardRebalancer) processDropShards() {

	notifych := make(chan bool, 2)

	first := true
	cfg := sr.config.Load()
	waitTime := cfg["rebalance.drop_index.wait_time"].Int()

	for {
		select {
		case <-sr.cancel:
			l.Infof("ShardRebalancer::processDropShards Cancel Received")
			return
		case <-sr.done:
			l.Infof("ShardRebalancer::processDropShards Done Received")
			return
		case ttid := <-sr.dropQueue:

			logging.Infof("ShardRebalancer::processDropShards processing drop shards request for ttid: %v", ttid)
			if first {
				// If it is the first drop, let wait to give a chance for the target's metadata
				// being synchronized with the cbq nodes.  This is to ensure that the cbq nodes
				// can direct scan to the target nodes, before we start dropping the index in the source.
				time.Sleep(time.Duration(waitTime) * time.Second)
				first = false
			}

			sr.mu.Lock()
			tt, ok := sr.sourceTokens[ttid]
			sr.mu.Unlock()

			if !ok {
				l.Warnf("ShardRebalancer::processDropShards Cannot find token %v in sr.sourceTokens. Skip drop shards.", ttid)
				continue
			}

			if tt.ShardTransferTokenState == c.ShardTokenReady {
				if sr.addToWaitGroup() {
					notifych <- true
					go sr.dropShardsWhenIdle(ttid, tt.Clone(), notifych)
				} else {
					logging.Warnf("ShardRebalancer::processDropShards Skip processing drop shards request for ttid: %v "+
						"as rebalancer can not add to wait group", tt)
				}
			} else {
				logging.Warnf("ShardRebalancer::processDropShards Skipping drop shard request for tt: %v "+
					"as state: %v != ShardTokenReady", ttid, tt.ShardTransferTokenState.String())
			}
		}
	}
}

func (sr *ShardRebalancer) dropShardsWhenIdle(ttid string, tt *c.TransferToken, notifyCh chan bool) {
	const method = "ShardRebalancer::dropShardsWhenIdle:" // for logging
	const sleepSecs = 1                                   // seconds to sleep per loop iteration

	defer sr.wg.Done()

	defer func() {
		if notifyCh != nil {
			// Blocking wait to ensure indexes are dropped sequentially
			<-notifyCh
		}
	}()

	missingStatRetry := 0
	droppedIndexes := make(map[c.IndexInstId]bool)

loop:
	for {
	labelselect:
		select {
		case <-sr.cancel:
			l.Infof("ShardRebalancer::dropShardsWhenIdle: Cancel Received")
			break loop
		case <-sr.done:
			l.Infof("ShardRebalancer::dropShardsWhenIdle: Done Received")
			break loop

		default:

			for _, inst := range tt.IndexInsts {

				defn := &inst.Defn
				defn.SetCollectionDefaults()
				defn.InstId = inst.InstId
				defn.RealInstId = inst.RealInstId

				allStats := sr.statsMgr.stats.Get()

				defnKey := inst.RealInstId
				if inst.RealInstId == 0 {
					defnKey = inst.InstId
				}

				defnStats := allStats.indexes[defnKey] // stats for current defn
				if defnStats == nil {
					l.Infof("ShardRebalancer::dropShardsWhenIdle Missing defnStats for instId %v. Retrying...", method, defnKey)
					break
				}

				var pending, numRequests, numCompletedRequests int64
				for _, partitionId := range defn.Partitions {
					partnStats := defnStats.partitions[partitionId] // stats for this partition
					if partnStats != nil {
						numRequests = partnStats.numRequests.GetValue().(int64)
						numCompletedRequests = partnStats.numCompletedRequests.GetValue().(int64)
					} else {
						l.Infof("ShardRebalancer::dropShardsWhenIdle Missing partnStats for instId %d partition %v. Retrying...",
							method, defnKey, partitionId)
						missingStatRetry++
						if missingStatRetry > 50 {
							if sr.needRetryForDrop(ttid, tt) {
								break labelselect
							} else {
								break loop
							}
						}
						break labelselect
					}
					pending += numRequests - numCompletedRequests
				}

				if pending > 0 {
					l.Infof("ShardRebalancer::dropShardsWhenIdle Index %v:%v:%v:%v has %v pending scans",
						method, defn.Bucket, defn.Scope, defn.Collection, defn.Name, pending)
					break
				}

				url := "/dropIndex"
				resp, err := postWithHandleEOF(defn, sr.localaddr, url, method)
				if err != nil {
					l.Errorf("ShardRebalancer::dropShardsWhenIdle: Error observed when posting dropIndex request "+
						" for index: %v", defn)
					sr.setTransferTokenError(ttid, tt, err.Error())
					return
				}

				response := new(IndexResponse)
				if err := convertResponse(resp, response); err != nil {
					l.Errorf("ShardRebalancer::dropShardsWhenIdle: Error unmarshal response %v for defn: %v, err: %v",
						sr.localaddr+url, defn, err)
					sr.setTransferTokenError(ttid, tt, err.Error())
					return
				}

				if response.Code == RESP_ERROR {
					// Error from index processing code (e.g. the string from common.ErrCollectionNotFound)
					if !isMissingBSC(response.Error) {
						l.Errorf("ShardRebalancer::dropShardsWhenIdle: Error dropping index defn: %v, url: %v, err: %v",
							sr.localaddr+url, url, response.Error)
						sr.setTransferTokenError(ttid, tt, response.Error)
						return
					}
					// Ok: failed to drop source index because b/s/c was dropped. Continue to TransferTokenCommit state.
					l.Infof("ShardRebalancer::dropShardsWhenIdle: Source index already dropped due to bucket/scope/collection dropped. tt %v.", method, tt)
				}
				droppedIndexes[inst.InstId] = true
			}
		}

		allInstancesDropped := true
		for _, inst := range tt.IndexInsts {
			if _, ok := droppedIndexes[inst.InstId]; !ok {
				// Still some instances need to be dropped
				allInstancesDropped = false
				break
			}
		}

		if allInstancesDropped {
			tt.ShardTransferTokenState = c.ShardTokenCommit
			setTransferTokenInMetakv(ttid, tt)

			sr.updateInMemToken(ttid, tt, "source")
			return
		}

		time.Sleep(1 * time.Second)
	}
}

// needRetryForDrop is called after multiple unsuccessful attempts to get the stats for an index to be
// dropped. It determines whether we should keep retrying or assume it was already dropped. It is only
// assumed already to be dropped if there is no entry for it in the local metadata.
func (sr *ShardRebalancer) needRetryForDrop(ttid string, tt *c.TransferToken) bool {
	const method = "ShardRebalancer::needRetryForDrop:" // for logging

	localMeta, err := getLocalMeta(sr.localaddr)
	if err != nil {
		l.Errorf("ShardRebalancer::needRetryForDrop: Error Fetching Local Meta %v %v", sr.localaddr, err)
		return true
	}

	defnIdMap := make(map[uint64]bool)
	for _, inst := range tt.IndexInsts {
		defnIdMap[uint64(inst.Defn.DefnId)] = true
	}

	indexStateMap, errStrMap := sr.getIndexStatusFromMeta(tt, defnIdMap, localMeta)

	for instId, indexState := range indexStateMap {
		errStr := errStrMap[instId]

		if errStr != "" {
			l.Errorf("ShardRebalancer::needRetryForDrop: Error Fetching Index Status %v %v", sr.localaddr, errStr)
			return true
		}

		if indexState == c.INDEX_STATE_NIL {
			//if index cannot be found in metadata, most likely its drop has already succeeded.
			//instead of waiting indefinitely, it is better to assume success and proceed.
			l.Infof("ShardRebalancer::needRetryForDrop: Missing Metadata for %v. Assume success and abort retry", instId)
			continue
		} else {
			// Index in shard exists in metadata. Do not change the transfer token state yet
			return false
		}
	}

	// Coming here means that none of the indexes in the transfer token have
	// been found in the local meta. Hence, the indexes are likely dropped
	// Change the state of the token and proceed further
	tt.ShardTransferTokenState = c.ShardTokenCommit
	setTransferTokenInMetakv(ttid, tt)

	sr.updateInMemToken(ttid, tt, "source")

	return true
}

func (sr *ShardRebalancer) finishRebalance(err error) {
	// TODO: Add logic to clean-up transfer tokens
	sr.retErr = err
	sr.cleanupOnce.Do(sr.doFinish)
}

func (sr *ShardRebalancer) doFinish() {
	l.Infof("ShardRebalancer::doFinish Cleanup: %v", sr.retErr)

	atomic.StoreInt32(&sr.isDone, 1)
	close(sr.done)

	sr.cancelMetakv()
	sr.wg.Wait()
	sr.cb.done(sr.retErr, sr.cancel)
}

func (sr *ShardRebalancer) isFinish() bool {
	return atomic.LoadInt32(&sr.isDone) == 1
}

func (sr *ShardRebalancer) cancelMetakv() {
	sr.metakvMutex.Lock()
	defer sr.metakvMutex.Unlock()

	if sr.metakvCancel != nil {
		close(sr.metakvCancel)
		sr.metakvCancel = nil
	}
}

func (sr *ShardRebalancer) addToWaitGroup() bool {
	sr.metakvMutex.Lock()
	defer sr.metakvMutex.Unlock()

	if sr.metakvCancel != nil {
		sr.wg.Add(1)
		return true
	}
	return false
}

func (sr *ShardRebalancer) setTransferTokenError(ttid string, tt *c.TransferToken, err string) {
	tt.Error = err
	setTransferTokenInMetakv(ttid, tt)
}

func (sr *ShardRebalancer) Cancel() {
	l.Infof("ShardRebalancer::Cancel Exiting")

	sr.cancelMetakv()
	close(sr.cancel)
	sr.wg.Wait()
}

// called by master to initiate transfer of shards.
// sr.mu needs to be acquired by the caller of this method
func (sr *ShardRebalancer) initiateShardTransfer() {

	config := sr.config.Load()
	batchSize := config["rebalance.transferBatchSize"].Int()

	count := 0

	var publishedIds []string

	// TODO: This logic picks first "transferBatchSize" tokens
	// from 'transferTokens' map. In future, a more intelligent
	// algorithm is required to batch a group based on source
	// node, destination node and group them appropriately

	for ttid, tt := range sr.transferTokens {
		if tt.ShardTransferTokenState == c.ShardTokenScheduleAck {
			// Used a cloned version so that the master token list
			// will not be updated until the transfer token with
			// updated state is persisted in metaKV
			ttClone := tt.Clone()

			// Change state of transfer token to TransferTokenTransferShard
			ttClone.ShardTransferTokenState = c.ShardTokenTransferShard
			setTransferTokenInMetakv(ttid, ttClone)
			publishedIds = append(publishedIds, ttid)
			count++
			if count >= batchSize {
				break
			}
		}
	}
	l.Infof("ShardRebalancer::initiateShardTransfer Published transfer token batch: %v", publishedIds)
}

func (sr *ShardRebalancer) checkAllTokensDone() bool {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	for _, tt := range sr.transferTokens {
		if tt.ShardTransferTokenState != c.ShardTokenDeleted {
			return false
		}
	}
	return true
}
