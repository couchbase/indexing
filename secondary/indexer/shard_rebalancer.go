package indexer

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/indexing/secondary/common"
	c "github.com/couchbase/indexing/secondary/common"
	l "github.com/couchbase/indexing/secondary/logging"
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

	transferStats map[string]map[uint64]*ShardTransferStatistics // ttid -> shardId -> stats

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
		transferStats:  make(map[string]map[uint64]*ShardTransferStatistics),
	}

	sr.config.Store(config)

	if master {
		go sr.initRebalAsync()
	} else {
		close(sr.waitForTokenPublish)
		go sr.observeRebalance()
	}
	go sr.processDropShard()

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

	if tt.SourceId == sr.nodeUUID && !processed {
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

	return false
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

	default:
		l.Infof("VarunLog: In the source code in default ttid: %v", ttid)
		return false
	}

	return false
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
					sr.initiateShardTransferCleanup(shardPaths, tt.Destination, ttid)
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
		sr.transferStats[ttid] = make(map[uint64]*ShardTransferStatistics)
	}
	sr.transferStats[ttid][stats.shardId] = stats
}

func (sr *ShardRebalancer) initiateShardTransferCleanup(shardPaths map[uint64]string, destination string, ttid string) {

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
	default:
		return false
	}

	return false
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
					sr.initiateLocalShardCleanup(shardPaths)
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
func (sr *ShardRebalancer) initiateLocalShardCleanup(shardPaths map[uint64]string) {
	// TODO: Add logic to clean-up shard data from local file system
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

	sr.mu.RLock()
	defer sr.mu.RUnlock()

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

func (sr *ShardRebalancer) processDropShard() {
	// TODO: Add logic to drop shard
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
