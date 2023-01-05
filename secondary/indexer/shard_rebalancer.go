package indexer

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
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
	"github.com/couchbase/indexing/secondary/planner"
	"github.com/couchbase/indexing/secondary/testcode"
)

var ErrRebalanceCancel = errors.New("Shard rebalance cancel received")
var ErrRebalanceDone = errors.New("Shard rebalance done received")

// ShardRebalancer embeds Rebalancer struct to reduce code
// duplication across common functions
type ShardRebalancer struct {
	clusterVersion int64

	// List of transfer tokens sent by planner & as maintained by master
	transferTokens map[string]*c.TransferToken
	sourceTokens   map[string]*c.TransferToken // as maintained by source
	acceptedTokens map[string]*c.TransferToken // as maintained by destination

	// Group sibling transfer tokens. Both siblings gets published
	// in the same batch
	batchedTokens []map[string]*c.TransferToken

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

	// Maintained by master. Represents the number of transfer tokens
	// that are currently being processed after all tokens have reached
	// ShardTokenScheduleAck state
	currBatchSize int

	// For computing rebalance progress
	lastKnownProgress map[c.IndexInstId]float64

	// topologyChange is populated in Rebalance and Failover cases only, else nil
	topologyChange *service.TopologyChange

	// Metakv management
	metakvCancel chan struct{}
	metakvMutex  sync.RWMutex

	// Dropping of shards
	dropQueue  chan string     // ttids of source indexes waiting to be submitted for drop
	dropQueued map[string]bool // set of ttids already added to dropQueue, as metakv can send duplicate notifications

	destination string // Tranfser destination for rebalance

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

		batchedTokens: make([]map[string]*c.TransferToken, 0),

		cancel:              make(chan struct{}),
		done:                make(chan struct{}),
		metakvCancel:        make(chan struct{}),
		waitForTokenPublish: make(chan struct{}),

		topologyChange: topologyChange,
		transferStats:  make(map[string]map[common.ShardId]*ShardTransferStatistics),

		dropQueue:         make(chan string, 10000),
		dropQueued:        make(map[string]bool),
		lastKnownProgress: make(map[c.IndexInstId]float64),
	}

	sr.config.Store(config)

	if master {
		go sr.initRebalAsync()
	} else {
		close(sr.waitForTokenPublish)
		go sr.observeRebalance()
	}
	go sr.processDropShards()
	go sr.updateTransferProgress()

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
				l.Infof("ShardRebalancer::initRebalAsync Cancel Received")
				return

			case <-sr.done:
				l.Infof("ShardRebalancer::initRebalAsync Done Received")
				return

			default:
				allWarmedup, _ := checkAllIndexersWarmedup(cfg["clusterAddr"].String())
				if !allWarmedup {
					l.Errorf("ShardRebalancer::initRebalAsync All Indexers Not Active. Waiting...")
					time.Sleep(5 * time.Second)
					continue
				}

				var err error
				sr.transferTokens, _, err = planner.ExecuteTenantAwareRebalance(cfg["clusterAddr"].String(),
					*sr.topologyChange, sr.nodeUUID)

				// TODO: Add logic to remove duplicate indexes

				if err != nil {
					l.Errorf("ShardRebalancer::initRebalAsync Planner Error %v", err)
					go sr.finishRebalance(err)
					return
				}

				if len(sr.transferTokens) == 0 {
					sr.transferTokens = nil
				} else {
					destination, err := getDestinationFromConfig(sr.config.Load())
					if err != nil {
						l.Errorf("ShardRebalancer::initRebalAsync err: %v", err)
						go sr.finishRebalance(err)
						return
					}
					// Populate destination in transfer tokens
					for _, token := range sr.transferTokens {
						token.Destination = destination
					}
					sr.destination = destination
					l.Infof("ShardRebalancer::initRebalAsync Populated destination: %v in all transfer tokens", destination)

				}

				break loop
			}
		}
	}

	go sr.doRebalance()
}

func getDestinationFromConfig(cfg c.Config) (string, error) {
	blobStorageScheme := cfg["settings.rebalance.blob_storage_scheme"].String()
	blobStorageBucket := cfg["settings.rebalance.blob_storage_bucket"].String()
	blobStoragePrefix := cfg["settings.rebalance.blob_storage_prefix"].String()

	if blobStorageScheme != "" && !strings.HasSuffix(blobStorageBucket, "://") {
		blobStorageScheme += "://"
	}
	if blobStorageBucket != "" && !strings.HasSuffix(blobStorageBucket, "/") {
		blobStorageBucket += "/"
	}

	destination := blobStorageScheme + blobStorageBucket + blobStoragePrefix
	if len(destination) == 0 {
		return "", errors.New("Empty destination for shard rebalancer")
	}
	return destination, nil
}

// processTokens is invoked by observeRebalance() method
// processTokens invokes processShardTokens of ShardRebalancer
func (sr *ShardRebalancer) processShardTokens(kve metakv.KVEntry) error {

	if kve.Path == RebalanceTokenPath {
		l.Infof("ShardRebalancer::processShardTokens RebalanceToken %v %s", kve.Path, kve.Value)
		if kve.Value == nil {
			l.Infof("ShardRebalancer::processShardTokens Rebalance Token Deleted. Mark Done.")
			sr.cancelMetakv()
			sr.finishRebalance(nil)
		} else if kve.Value != nil {
			rToken := &RebalanceToken{}
			err := json.Unmarshal(kve.Value, rToken)
			if err != nil {
				l.Errorf("ShardRebalancer::processShardTokens Error observed while unmarshalling RebalanceToken")
			} else {
				l.Infof("ShardRebalancer::processShardTokens Rebalance Token updated with AllowDDLDuringRebalance")
				if rToken.Version >= c.AllowDDLDuringRebalance_v1 {
					sr.updateGlobalRebalancePhase(rToken)
				}
			}
		}
	} else if strings.Contains(kve.Path, TransferTokenTag) {
		if kve.Value != nil {
			ttid, tt, err := decodeTransferToken(kve.Path, kve.Value, "ShardRebalancer")
			if err != nil {
				l.Errorf("ShardRebalancer::processShardTokens Unable to decode transfer token. Ignored.")
				return nil
			}
			sr.processShardTransferToken(ttid, tt)
		} else {
			l.Infof("ShardRebalancer::processShardTokens Received empty or deleted transfer token %v", kve.Path)
		}
	}

	return nil
}

func (sr *ShardRebalancer) updateGlobalRebalancePhase(rToken *RebalanceToken) {
	if !sr.addToWaitGroup() {
		return
	}

	defer sr.wg.Done()

	sr.mu.Lock()
	defer sr.mu.Unlock()

	if rToken.RebalPhase == sr.rebalToken.RebalPhase {
		logging.Infof("ShardRebalancer::updateGlobalRebalancePhase Skipping to update RebalPhase "+
			"as token is already updated to phase: %v", rToken.RebalPhase)
		return
	}

	sr.rebalToken.Version = rToken.Version
	sr.rebalToken.RebalPhase = rToken.RebalPhase

	if sr.rebalToken.RebalPhase == common.RebalanceTransferInProgress {
		logging.Infof("ShardRebalancer::updateGlobalRebalancePhase Updating rebalance phase to %v", common.RebalanceTransferInProgress)
		msg := &MsgUpdateRebalancePhase{
			GlobalRebalancePhase: common.RebalanceTransferInProgress,
		}

		sr.supvMsgch <- msg
	}
}

func (sr *ShardRebalancer) processShardTransferToken(ttid string, tt *c.TransferToken) {
	if !sr.addToWaitGroup() {
		return
	}

	defer sr.wg.Done()

	// If DDL is allowed during rebalance, skip the ongoing DDL check.
	// Otherwise, if there is an on-going DDL, fail the rebalance.
	if !sr.canAllowDDLDuringRebalance() {
		if ddl, err := sr.runParams.checkDDLRunning("ShardRebalancer"); ddl {
			sr.setTransferTokenError(ttid, tt, err.Error())
			return
		}
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

	if (tt.SourceId == sr.nodeUUID && !processed) || (tt.ShardTransferTokenState == c.ShardTokenDropOnSource) {
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
		l.Errorf("ShardRebalancer::processShardTransferTokenAsMaster Detected TransferToken in Error state %v. Abort.", tt)

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

		////////////// Testing code - Not used in production //////////////
		testcode.TestActionAtTag(sr.config.Load(), testcode.MASTER_SHARDTOKEN_SCHEDULEACK)
		///////////////////////////////////////////////////////////////////

		if sr.allShardTransferTokensAcked() {
			sr.updateRebalancePhaseInGlobalRebalToken()
			sr.batchTransferTokens()
			sr.initiateShardTransferAsMaster()
		}
		return true

	case c.ShardTokenTransferShard,
		c.ShardTokenRestoreShard,
		c.ShardTokenRecoverShard:
		// Update the in-memory state but do not process the token
		// This will help to compute the rebalance progress
		sr.updateInMemToken(ttid, tt, "master")
		return false

	case c.ShardTokenReady:
		sr.updateInMemToken(ttid, tt, "master")

		////////////// Testing code - Not used in production //////////////
		testcode.TestActionAtTag(sr.config.Load(), testcode.MASTER_SHARDTOKEN_BEFORE_DROP_ON_SOURCE)
		///////////////////////////////////////////////////////////////////

		if tt.SiblingExists() {
			if sr.getSiblingState(tt) == c.ShardTokenReady {
				dropOnSourceTokenId, dropOnSourceToken := genShardTokenDropOnSource(tt.RebalId, ttid, tt.SiblingTokenId)
				setTransferTokenInMetakv(dropOnSourceTokenId, dropOnSourceToken)

				////////////// Testing code - Not used in production //////////////
				testcode.TestActionAtTag(sr.config.Load(), testcode.MASTER_SHARDTOKEN_AFTER_DROP_ON_SOURCE)
				///////////////////////////////////////////////////////////////////
			}
		} else {
			// If sibling does not exist, this could be replica repair or single node
			// swap rebalance.
			// For replica repair case, source node should clear the transferred data and
			// also unlock the shards. For single node swap rebalance, the indexes are
			// also expected to be dropped on source node. Therefore, post a DropOnSource
			// token in either case. Source node will act based on the transferMode of the
			// token
			dropOnSourceTokenId, dropOnSourceToken := genShardTokenDropOnSource(tt.RebalId, ttid, "")
			setTransferTokenInMetakv(dropOnSourceTokenId, dropOnSourceToken)
		}

		return true

	case c.ShardTokenDropOnSource:
		// Just update in-mem book keeping
		sr.updateInMemToken(ttid, tt, "master")
		return false

	case c.ShardTokenCommit:
		sr.updateInMemToken(ttid, tt, "master")
		siblingState := sr.getSiblingState(tt)

		if tt.SiblingExists() {
			if siblingState == c.ShardTokenCommit || siblingState == c.ShardTokenDeleted {
				dropOnSourceTokenId, dropOnSourceToken := sr.getDropOnSourceTokenAndId(ttid, tt.SiblingTokenId)
				if dropOnSourceToken != nil {
					dropOnSourceToken.ShardTransferTokenState = c.ShardTokenDeleted
					setTransferTokenInMetakv(dropOnSourceTokenId, dropOnSourceToken)
				}
			}
		} else {
			// If sibling does not exist, this could be replica repair or swap rebalance.
			// Go-ahead and delete dropOnSource token signalling completion of movements
			dropOnSourceTokenId, dropOnSourceToken := sr.getDropOnSourceTokenAndId(ttid, "")
			if dropOnSourceToken != nil {
				dropOnSourceToken.ShardTransferTokenState = c.ShardTokenDeleted
				setTransferTokenInMetakv(dropOnSourceTokenId, dropOnSourceToken)
			}
		}

		// Return false so that if master is also destination node,
		// it will update the book-keeping at lifecycle manager about
		// completion of rebalance for this bucket
		return false

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
			func() {
				sr.mu.Lock()
				defer sr.mu.Unlock()
				// Decrement the batchSize as token has finished processing
				sr.currBatchSize--

				sr.initiateShardTransferAsMaster()
			}()
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

		sr.updateInMemToken(ttid, tt, "source")
		sr.updateBucketTransferPhase(tt.IndexInsts[0].Defn.Bucket, common.RebalanceInitated)

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

	case c.ShardTokenRestoreShard:
		// Update in-mem book keeping and do not process the token
		sr.updateInMemToken(ttid, tt, "source")

		// Notify lifecycle manager that shard transfer is done.
		// This will allow lifecycle manager to unblock any drop
		// index commands that have been issued while transfer is
		// in progress
		sr.updateBucketTransferPhase(tt.IndexInsts[0].Defn.Bucket, c.RebalanceTransferDone)
		return false

	case c.ShardTokenDropOnSource:

		// For this token type, compare the sourceId of the corresponding
		// tokens with ID's as tt.TokenId or tt.SiblingTokenId.
		//
		// If either of them match, drop index instances on source node.
		// When there is replica repair and swap rebalance happening in
		// same rebalance, the swap rebalance token is queued for drop
		// and replica repair token is moved to Commit phase
		if sr.getSourceIdForTokenId(tt.SourceTokenId) == sr.nodeUUID {
			retVal := sr.checkAndQueueTokenForDrop(tt, tt.SourceTokenId, tt.SiblingTokenId)
			if retVal {
				return true
			}
		}
		if sr.getSourceIdForTokenId(tt.SiblingTokenId) == sr.nodeUUID {
			retVal := sr.checkAndQueueTokenForDrop(tt, tt.SiblingTokenId, tt.SourceTokenId)
			if retVal {
				return true
			}
		}
		return true

	case c.ShardTokenCommit:
		// If node sees a commit token, then DDL can be allowed on
		// the bucket. It is necessary for source to update its
		// book-keeping. Otherwise, source will reject prepare create
		// request thinking that the rebalance of bucket is not done.
		// During the plan phase (which happens after prepare phase),
		// planner will decide which node the index will land-on
		sr.updateInMemToken(ttid, tt, "source")
		sr.updateBucketTransferPhase(tt.IndexInsts[0].Defn.Bucket, common.RebalanceDone)

		return false // Return false as this is source node. Destination node should also process the token

	default:
		return false
	}
}

func (sr *ShardRebalancer) checkAndQueueTokenForDrop(token *c.TransferToken, sourceId, siblingId string) bool {
	sourceToken := sr.getTokenById(sourceId)
	if sourceToken != nil && sourceToken.TransferMode == common.TokenTransferModeCopy && siblingId == "" { // only replica repair case
		// Since this is replica repair, do not drop the shard data. Only cleanup the transferred data
		// and unlock the shards
		l.Infof("ShardRebalancer::processShardTransferTokenAsSource Initiating shard unlocking for token: %v", sourceId)

		unlockShards(sourceToken.ShardIds, sr.supvMsgch)
		sr.initiateShardTransferCleanup(sourceToken.ShardPaths, sourceToken.Destination, sourceId, sourceToken, nil)

		sourceToken.ShardTransferTokenState = common.ShardTokenCommit
		setTransferTokenInMetakv(sourceId, sourceToken)
		return true

	} else if sourceToken != nil && sourceToken.TransferMode == common.TokenTransferModeMove { // Single node swap rebalance (or) single node swap + replica repair in same rebalance

		l.Infof("ShardRebalancer::processShardTransferTokenAsSource Queuing token: %v for drop", sourceId)
		sr.queueDropShardRequests(sourceId)

		// Skip unlocking shards as the shards are going to be destroyed
		// Clean-up any transferred data - Currently, plasma cleans up transferred data on a successful
		// restore. This is only a safety operation from indexer. Coming to this code means that restore
		// is successful and this operation becomes a no-op
		sr.initiateShardTransferCleanup(sourceToken.ShardPaths, sourceToken.Destination, sourceId, sourceToken, nil)

		siblingToken := sr.getTokenById(siblingId)
		if siblingToken != nil && siblingToken.TransferMode == common.TokenTransferModeCopy && siblingToken.SourceId == sr.nodeUUID {
			siblingToken.ShardTransferTokenState = common.ShardTokenCommit
			setTransferTokenInMetakv(siblingId, siblingToken)
		}
		return true

	} else {
		l.Infof("ShardRebalancer::processShardTransferTokenAsSource Skipping token: %v for drop", sourceId)
	}
	return false
}

func (sr *ShardRebalancer) getTokenById(ttid string) *c.TransferToken {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	if token, ok := sr.sourceTokens[ttid]; ok && token != nil {
		return token.Clone()
	}
	return nil // Return error as the default state
}

func (sr *ShardRebalancer) startShardTransfer(ttid string, tt *c.TransferToken) {

	if !sr.addToWaitGroup() {
		return
	}
	defer sr.wg.Done()

	start := time.Now()

	// Lock shard to prevent any index instance mapping while transfer is in progress
	// Unlock of the shard happens:
	// (a) after shard transfer is successful & destination node has recovered the shard
	// (b) If any error is encountered, clean-up from indexer will unlock the shards
	err := lockShards(tt.ShardIds, sr.supvMsgch, false)
	if err != nil {
		logging.Errorf("ShardRebalancer::startShardTransfer Observed error: %v when locking shards: %v", err, tt.ShardIds)

		unlockShards(tt.ShardIds, sr.supvMsgch)
		sr.setTransferTokenError(ttid, tt, err.Error())
		return
	}

	respCh := make(chan Message)                            // Carries final response of shard transfer to rebalancer
	progressCh := make(chan *ShardTransferStatistics, 1000) // Carries periodic progress of shard tranfser to indexer

	msg := &MsgStartShardTransfer{
		shardIds:    tt.ShardIds,
		taskId:      sr.rebalToken.RebalId,
		transferId:  ttid,
		taskType:    common.RebalanceTask,
		destination: tt.Destination,

		cancelCh:   sr.cancel,
		doneCh:     sr.done,
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

			elapsed := time.Since(start).Seconds()
			l.Infof("ShardRebalancer::startShardTransfer Received response for transfer of "+
				"shards: %v, ttid: %v, elapsed(sec): %v", tt.ShardIds, ttid, elapsed)

			msg := respMsg.(*MsgShardTransferResp)
			errMap := msg.GetErrorMap()
			shardPaths := msg.GetShardPaths()

			for shardId, err := range errMap {
				if err != nil {
					l.Errorf("ShardRebalancer::startShardTransfer Observed error during trasfer"+
						" for destination: %v, shardId: %v, shardPaths: %v, err: %v. Initiating transfer clean-up",
						tt.Destination, shardId, shardPaths, err)

					unlockShards(tt.ShardIds, sr.supvMsgch)
					// Invoke clean-up for all shards even if error is observed for one shard transfer
					sr.initiateShardTransferCleanup(shardPaths, tt.Destination, ttid, tt, err)
					return
				}
			}

			////////////// Testing code - Not used in production //////////////
			testcode.TestActionAtTag(sr.config.Load(), testcode.SOURCE_SHARDTOKEN_AFTER_TRANSFER)
			///////////////////////////////////////////////////////////////////

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

	start := time.Now()
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

	elapsed := time.Since(start).Seconds()
	l.Infof("ShardRebalancer::initiateShardTransferCleanup Done clean-up for ttid: %v, "+
		"shard paths: %v, destination: %v, elapsed(sec): %v", ttid, shardPaths, destination, elapsed)

	if err != nil {
		// Update error in transfer token so that rebalance master
		// will finish the rebalance and clean-up can be invoked for
		// other transfer tokens in the batch depending on their state
		sr.setTransferTokenError(ttid, tt, err.Error())
	}

}

func (sr *ShardRebalancer) getDropOnSourceTokenAndId(ttid, siblingId string) (string, *c.TransferToken) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	for dropOnSourceTokenId, dropOnSourceToken := range sr.transferTokens {
		if dropOnSourceToken.ShardTransferTokenState == c.ShardTokenDropOnSource {
			if (dropOnSourceToken.SourceTokenId == ttid && dropOnSourceToken.SiblingTokenId == siblingId) ||
				(dropOnSourceToken.SourceTokenId == siblingId && dropOnSourceToken.SiblingTokenId == ttid) {
				return dropOnSourceTokenId, dropOnSourceToken
			}
		}
	}
	return "", nil
}

func (sr *ShardRebalancer) getSiblingState(tt *c.TransferToken) c.ShardTokenState {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	if siblingToken, ok := sr.transferTokens[tt.SiblingTokenId]; ok && siblingToken != nil {
		return siblingToken.ShardTransferTokenState
	}
	return c.ShardTokenError // Return error as the default state
}

func (sr *ShardRebalancer) getSourceIdForTokenId(ttid string) string {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	if tt, ok := sr.sourceTokens[ttid]; ok {
		return tt.SourceId
	}
	return ""
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

		sr.updateInMemToken(ttid, tt, "dest")
		sr.updateBucketTransferPhase(tt.IndexInsts[0].Defn.Bucket, common.RebalanceInitated)
		tt.ShardTransferTokenState = c.ShardTokenScheduleAck
		setTransferTokenInMetakv(ttid, tt)

		// TODO: It is possible for destination node to crash
		// after updating metakv state. Include logic to clean-up
		// rebalance in such case
		return true

	case c.ShardTokenRestoreShard:
		sr.updateInMemToken(ttid, tt, "dest")

		// Notify lifecycle manager that shard transfer is done.
		// This will allow lifecycle manager to unblock any drop
		// index commands that have been issued while transfer is
		// in progress
		sr.updateBucketTransferPhase(tt.IndexInsts[0].Defn.Bucket, c.RebalanceTransferDone)

		go sr.startShardRestore(ttid, tt)

		return true

	case c.ShardTokenRecoverShard:
		sr.updateInMemToken(ttid, tt, "dest")

		go sr.startShardRecovery(ttid, tt)

		return true

	case c.ShardTokenCommit:
		sr.updateInMemToken(ttid, tt, "dest")

		// Destination will call RestoreShardDone for the shardId involved in the
		// rebalance as coming here means that rebalance is successful for the
		// tenant. After RestoreShardDone, the shards will be unlocked
		restoreShardDone(tt.ShardIds, sr.supvMsgch)

		// Unlock the shards that are locked before initiating recovery
		unlockShards(tt.ShardIds, sr.supvMsgch)

		// If nodes sees a commit token, then DDL can be allowed on
		// the bucket
		sr.updateBucketTransferPhase(tt.IndexInsts[0].Defn.Bucket, common.RebalanceDone)

		tt.ShardTransferTokenState = c.ShardTokenDeleted
		setTransferTokenInMetakv(ttid, tt)
		return true

	default:
		return false
	}
}

func (sr *ShardRebalancer) startShardRestore(ttid string, tt *c.TransferToken) {

	if !sr.addToWaitGroup() {
		return
	}
	defer sr.wg.Done()

	start := time.Now()

	respCh := make(chan Message)                            // Carries final response of shard restore to rebalancer
	progressCh := make(chan *ShardTransferStatistics, 1000) // Carries periodic progress of shard restore to indexer

	msg := &MsgStartShardRestore{
		shardPaths:      tt.ShardPaths,
		rebalanceId:     sr.rebalToken.RebalId,
		transferTokenId: ttid,
		destination:     tt.Destination,
		instRenameMap:   tt.InstRenameMap,

		cancelCh:   sr.cancel,
		doneCh:     sr.done,
		respCh:     respCh,
		progressCh: progressCh,
	}

	sr.supvMsgch <- msg

	for {
		select {
		case respMsg := <-respCh:

			elapsed := time.Since(start).Seconds()
			l.Infof("ShardRebalancer::startRestoreShard Received response for restore of "+
				"shardIds: %v, ttid: %v, elapsed(sec): %v", tt.ShardIds, ttid, elapsed)

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
					sr.initiateLocalShardCleanup(ttid, shardPaths, tt)
					sr.setTransferTokenError(ttid, tt, err.Error())
					return
				}
			}

			////////////// Testing code - Not used in production //////////////
			testcode.TestActionAtTag(sr.config.Load(), testcode.DEST_SHARDTOKEN_AFTER_RESTORE)
			///////////////////////////////////////////////////////////////////

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
	tt *c.TransferToken) {

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

	// Wait for response. Cleanup is a best effor call
	// So, no need to process response
	<-respCh
}

func (sr *ShardRebalancer) startShardRecovery(ttid string, tt *c.TransferToken) {

	if !sr.addToWaitGroup() {
		return
	}
	defer sr.wg.Done()

	if err := lockShards(tt.ShardIds, sr.supvMsgch, true); err != nil {
		logging.Errorf("ShardRebalancer::startShardRecovery, error observed while locking shards: %v, err: %v", tt.ShardIds, err)

		unlockShards(tt.ShardIds, sr.supvMsgch)
		sr.setTransferTokenError(ttid, tt, err.Error())
		return
	}

	setErrInTransferToken := func(err error) {
		if err != ErrRebalanceCancel && err != ErrRebalanceDone {
			sr.setTransferTokenError(ttid, tt, err.Error())
		}
	}

	// During shard rebalance, all active instances are built in one
	// batch, all initial instances are built in one batch and all catchup
	// instances are built in another batch. Initial instances are those
	// instances for which a built command is posted but build is not yet
	// initialised (or) the index is being moved from source to destination
	// while the build is in progress. For such instances, as build
	// has to start from zero-seqno, they are built in a separate batch.
	// Active, catchup instances can start from non-zero seqno. and catch
	// up with KV. As catchup instances can be behind active instances, build
	// catchup instances in separate batch
	const ACTIVE_INSTS int = 0
	const INITIAL_INSTS int = 1
	const CATCHUP_INSTS int = 2

	start := time.Now()

	// All deferred indexes are created now. Create and recover non-deferred indexes
	groupedDefns := groupInstsPerColl(tt)

	populateInstsAndDefnIds := func(nonDeferredInsts []map[c.IndexInstId]bool,
		buildDefnIdList []client.IndexIdList, defn c.IndexDefn,
		defnIdToInstIdMap map[common.IndexDefnId]common.IndexInstId) ([]map[c.IndexInstId]bool, []client.IndexIdList, int) {

		currIndex := ACTIVE_INSTS
		if defn.InstStateAtRebal == common.INDEX_STATE_INITIAL {
			currIndex = INITIAL_INSTS
		} else if defn.InstStateAtRebal == common.INDEX_STATE_CATCHUP {
			currIndex = CATCHUP_INSTS
		}

		if len(nonDeferredInsts[currIndex]) == 0 {
			nonDeferredInsts[currIndex] = make(map[c.IndexInstId]bool)
		}
		if defn.RealInstId != 0 {
			nonDeferredInsts[currIndex][defn.RealInstId] = true
			defnIdToInstIdMap[defn.DefnId] = defn.RealInstId
		} else {
			nonDeferredInsts[currIndex][defn.InstId] = true
			defnIdToInstIdMap[defn.DefnId] = defn.InstId
		}

		buildDefnIdList[currIndex].DefnIds = append(buildDefnIdList[currIndex].DefnIds, uint64(defn.DefnId))

		return nonDeferredInsts, buildDefnIdList, currIndex
	}

	for cid, defns := range groupedDefns {
		buildDefnIdList := make([]client.IndexIdList, 3)
		nonDeferredInsts := make([]map[c.IndexInstId]bool, 3)
		defnIdToInstIdMap := make(map[common.IndexDefnId]common.IndexInstId)
		// Post recover index request for all non-deferred definitions
		// of this collection. Once all indexes are recovered, they can be
		// built in a batch
		for _, defn := range defns {

			// TODO: Use ShardIds for desintaion when changing the shardIds
			defn.ShardIdsForDest = tt.ShardIds
			if skip, err := sr.postRecoverIndexReq(defn, ttid, tt); err != nil {
				setErrInTransferToken(err)
				return
			} else if skip {
				// bucket (or) scope (or) collection (or) index are dropped.
				// Continue instead of failing rebalance
				continue
			}

			// For deferred indexes, build command is not required
			if defn.Deferred &&
				(defn.InstStateAtRebal == c.INDEX_STATE_CREATED ||
					defn.InstStateAtRebal == c.INDEX_STATE_READY) {

				var partnMergeWaitGroup sync.WaitGroup
				deferredInsts := make(map[common.IndexInstId]bool)
				if defn.RealInstId != 0 {
					deferredInsts[defn.RealInstId] = true
				} else {
					deferredInsts[defn.InstId] = true
				}

				////////////// Testing code - Not used in production //////////////
				testcode.TestActionAtTag(sr.config.Load(), testcode.DEST_SHARDTOKEN_DURING_DEFERRED_INDEX_RECOVERY)
				///////////////////////////////////////////////////////////////////

				if err := sr.waitForIndexState(c.INDEX_STATE_READY, deferredInsts, ttid, tt); err != nil {
					setErrInTransferToken(err)
					return
				}

				sr.destTokenToMergeOrReady(defn.InstId, defn.RealInstId, ttid, tt, partnMergeWaitGroup)

				// Wait for the deferred partition index merge to happen
				// This is essentially a no-op for non-partitioned indexes
				partnMergeWaitGroup.Wait()
			} else {
				var currIndex int
				nonDeferredInsts, buildDefnIdList, currIndex = populateInstsAndDefnIds(nonDeferredInsts, buildDefnIdList, defn, defnIdToInstIdMap)

				////////////// Testing code - Not used in production //////////////
				testcode.TestActionAtTag(sr.config.Load(), testcode.DEST_SHARDTOKEN_DURING_NON_DEFERRED_INDEX_RECOVERY)
				///////////////////////////////////////////////////////////////////

				if err := sr.waitForIndexState(c.INDEX_STATE_RECOVERED, nonDeferredInsts[currIndex], ttid, tt); err != nil {
					sr.setTransferTokenError(ttid, tt, err.Error())
					return
				}

			}

		}

		for i := range buildDefnIdList {
			if len(buildDefnIdList[i].DefnIds) > 0 {
				currInsts := "active insts"
				if i == INITIAL_INSTS {
					currInsts = "initial insts"
				} else if i == CATCHUP_INSTS {
					currInsts = "catchup insts"
				}

				logging.Infof("ShardRebalancer::startShardRecovery Successfully posted "+
					"recoverIndexRebalance requests for %v defnIds: %v belonging to collectionId: %v, ttid: %v. "+
					"Initiating build", currInsts, buildDefnIdList[i].DefnIds, cid, ttid)

				skipDefns, err := sr.postBuildIndexesReq(buildDefnIdList[i], ttid, tt)
				if err != nil {
					setErrInTransferToken(err)
					return
				}

				// Do not wait for index state of skipped insts
				if len(skipDefns) > 0 {
					logging.Infof("ShardRebalancer::startShardRecovery Skipping state monitoring for insts: %v "+
						"as scope/collection/index is dropped", skipDefns)
					for defnId, _ := range skipDefns {
						if instId, ok := defnIdToInstIdMap[defnId]; ok {
							delete(nonDeferredInsts[i], instId)
						}
					}
				}

				logging.Infof("ShardRebalancer::startShardRecovery Waiting for index state to "+
					"become active for insts: %v", nonDeferredInsts[i])

				if err := sr.waitForIndexState(c.INDEX_STATE_ACTIVE, nonDeferredInsts[i], ttid, tt); err != nil {
					setErrInTransferToken(err)
					return
				}
			}
		}
	}

	elapsed := time.Since(start).Seconds()
	l.Infof("ShardRebalancer::startShardRecovery Finished recovery of all indexes in ttid: %v, elapsed(sec): %v", ttid, elapsed)
	// Coming here means that all indexes are actively build without any error
	// Move the transfer token state to Ready
	tt.ShardTransferTokenState = c.ShardTokenReady
	setTransferTokenInMetakv(ttid, tt)
}

func groupInstsPerColl(tt *c.TransferToken) map[string][]common.IndexDefn {
	out := make(map[string][]common.IndexDefn)

	for i, inst := range tt.IndexInsts {

		defn := inst.Defn
		defn.SetCollectionDefaults()

		defn.Nodes = nil
		defn.InstId = tt.InstIds[i]
		defn.RealInstId = tt.RealInstIds[i]

		cid := defn.CollectionId
		out[cid] = append(out[cid], defn)
	}

	return out
}

func (sr *ShardRebalancer) postRecoverIndexReq(indexDefn common.IndexDefn, ttid string, tt *common.TransferToken) (bool, error) {
	select {
	case <-sr.cancel:
		l.Infof("ShardRebalancer::startShardRecovery rebalance cancel received")
		return false, ErrRebalanceCancel // return for now. Cleanup will take care of dropping the index instances

	case <-sr.done:
		l.Infof("ShardRebalancer::startShardRecovery rebalance done received")
		return false, ErrRebalanceDone // return for now. Cleanup will take care of dropping the index instances

	default:
		url := "/recoverIndexRebalance"

		resp, err := postWithHandleEOF(indexDefn, sr.localaddr, url, "ShardRebalancer::postRecoverIndexReq")
		if err != nil {
			logging.Errorf("ShardRebalancer::postRecoverIndexReq Error observed when posting recover index request, "+
				"indexDefnId: %v, err: %v", indexDefn.DefnId, err)
			return false, err
		}

		response := new(IndexResponse)
		if err := convertResponse(resp, response); err != nil {
			l.Errorf("ShardRebalancer::postRecoverIndexReq Error unmarshal response for indexDefnId: %v, "+
				"url: %v, err: %v", indexDefn.DefnId, sr.localaddr+url, err)
			return false, err
		}

		if response.Error != "" {
			if isMissingBSC(response.Error) || isIndexDeletedDuringRebal(response.Error) {
				logging.Infof("ShardRebalancer::postRecoverIndexReq scope/collection/index is "+
					"deleted during rebalance. Skipping the indexDefnId: %v from further processing. "+
					"indexDefnId: %v, Error: %v", indexDefn.DefnId, response.Error)
				return true, nil
			}
			l.Errorf("ShardRebalancer::postRecoverIndexReq Error received for indexDefnId: %v, err: %v",
				indexDefn.DefnId, response.Error)
			return false, errors.New(response.Error)
		}
	}
	return false, nil
}

func (sr *ShardRebalancer) postBuildIndexesReq(defnIdList client.IndexIdList, ttid string, tt *c.TransferToken) (map[common.IndexDefnId]bool, error) {
	select {
	case <-sr.cancel:
		l.Infof("ShardRebalancer::startShardRecovery rebalance cancel received")
		return nil, ErrRebalanceCancel // return for now. Cleanup will take care of dropping the index instances

	case <-sr.done:
		l.Infof("ShardRebalancer::startShardRecovery rebalance done received")
		return nil, ErrRebalanceDone // return for now. Cleanup will take care of dropping the index instances

	default:
		url := "/buildRecoveredIndexesRebalance"

		resp, err := postWithHandleEOF(defnIdList, sr.localaddr, url, "ShardRebalancer::postBuildIndexesReq")
		if err != nil {
			logging.Errorf("ShardRebalancer::postBuildIndexesReq Error observed when posting build indexes request, "+
				"defnIdList: %v, err: %v", defnIdList.DefnIds, err)
			return nil, err
		}

		response := new(IndexResponse)
		if err := convertResponse(resp, response); err != nil {
			l.Errorf("ShardRebalancer::postBuildIndexesReq Error unmarshal response for defnIdList: %v, "+
				"url: %v, err: %v", defnIdList.DefnIds, sr.localaddr+url, err)
			return nil, err
		}

		if response.Error != "" {
			skipDefns, err := unmarshalAndProcessBuildReqResponse(response.Error, defnIdList.DefnIds)
			if err != nil { // Error while unmarshalling - Return the error to caller and fail rebalance
				l.Errorf("ShardRebalancer::postBuildIndexesReq Error received for defnIdList: %v, err: %v",
					defnIdList.DefnIds, response.Error)
				return nil, errors.New(response.Error)
			} else {
				return skipDefns, nil
			}
		}
	}
	return nil, nil
}

func unmarshalAndProcessBuildReqResponse(errStr string, defnIdList []uint64) (map[common.IndexDefnId]bool, error) {
	errMap := make(map[common.IndexDefnId]string)

	err := json.Unmarshal([]byte(errStr), &errMap)
	if err != nil {
		logging.Errorf("ShardRebalancer::postBuildIndexesReq Unmarshal of errStr failed. "+
			"defnIdList: %v, err: %v, errStr: %v", defnIdList, err, errStr)
		return nil, err
	}

	skipDefns := make(map[common.IndexDefnId]bool)
	for defnId, buildErr := range errMap {
		if isIndexDeletedDuringRebal(buildErr) || isIndexNotFoundRebal(buildErr) {
			skipDefns[defnId] = true
		}
	}
	return skipDefns, nil
}

func (sr *ShardRebalancer) waitForIndexState(expectedState c.IndexState,
	processedInsts map[c.IndexInstId]bool, ttid string, tt *c.TransferToken) error {

	buildStartTime := time.Now()
	cfg := sr.config.Load()
	maxRemainingBuildTime := cfg["rebalance.maxRemainingBuildTime"].Uint64()
	lastLogTime := time.Now()

	// Used when there are partitioned indexes in transfer token
	// The destTokenToMergeOrReady will use this wait group to
	// to signal the completion of go-routines waiting for
	// partitioned index merge. waitForIndexState will always wait
	// for partitioned index merge to finish before proceeding futher
	var partnMergeWaitGroup sync.WaitGroup

	retryInterval := time.Duration(1)
	retryCount := 0
loop:
	for {

		activeIndexes := make(map[c.IndexInstId]bool) // instId -> bool

		select {
		case <-sr.cancel:
			l.Infof("ShardRebalancer::waitForIndexState Cancel Received")
			return ErrRebalanceCancel
		case <-sr.done:
			l.Infof("ShardRebalancer::waitForIndexState Done Received")
			return ErrRebalanceDone

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

			if tt.ShardTransferTokenState != c.ShardTokenRecoverShard {
				err := fmt.Errorf("Transfer token in: %v state. Expected state: %v", tt.ShardTransferTokenState, c.ShardTokenRecoverShard)
				l.Errorf("ShardRebalancer::waitForIndexState err: %v", err)
				return err
			}

			indexStateMap, errMap := sr.getIndexStatusFromMeta(tt, processedInsts, localMeta)

			for instId, indexState := range indexStateMap {
				err := errMap[instId]

				// At this point, the request "/recoverIndexRebalance" and/or "/buildRecoveredIndexesRebalance"
				// are successful. This means that local metadata is updated with index instance infomration.
				// If drop were to happen in this state, drop can remove the index from topology. In that
				// case, the indexState would be INDEX_STATE_NIL. Instead of failing rebalance, skip the
				// instance from further scanity checks and continue processing
				if indexState == c.INDEX_STATE_NIL || indexState == c.INDEX_STATE_DELETED {
					logging.Warnf("ShardRebalancer::waitForIndexState, Could not get index status. "+
						"scope/collection/index are likely dropped. Skipping instId: %v, indexState: %v, "+
						"ttid: %v", instId, indexState, ttid)
					continue
				} else if err != "" {
					l.Errorf("ShardRebalancer::waitForIndexState Error Fetching Index Status %v %v", sr.localaddr, err)
					retryCount++

					if retryCount > 5 {
						return errors.New(err) // Return after 5 unsuccessful attempts
					}
					time.Sleep(retryInterval * time.Second)
					goto loop // Retry
				}
			}

			switch expectedState {
			case common.INDEX_STATE_READY, common.INDEX_STATE_RECOVERED:
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

				////////////// Testing code - Not used in production //////////////
				testcode.TestActionAtTag(sr.config.Load(), testcode.DEST_SHARDTOKEN_DURING_INDEX_BUILD)
				///////////////////////////////////////////////////////////////////

				for i, inst := range tt.IndexInsts {

					defn := inst.Defn
					instId := tt.InstIds[i]
					realInstId := tt.RealInstIds[i]

					if !isInstProcessed(instId, realInstId, processedInsts) {
						continue
					}

					defn.SetCollectionDefaults()
					instKey := realInstId
					if realInstId == 0 {
						instKey = instId
					}

					indexState := indexStateMap[instKey]

					if indexState == c.INDEX_STATE_NIL || indexState == c.INDEX_STATE_DELETED {
						l.Infof("ShardRebalancer::waitForIndexState Missing defnStats for instId %v. Retrying...", instId)
						delete(processedInsts, instKey) // consider the index build done
						continue
					}

					//  for current defn. If indexState is not NIL or DELETED and defn stats are nil,
					// wait for defn stats to get populated
					defnStats := allStats.indexes[instKey]
					if defnStats == nil {
						l.Infof("ShardRebalancer::waitForIndexState Missing defnStats for instId %v. Retrying...", instKey)
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
						activeIndexes[instKey] = true
						sr.destTokenToMergeOrReady(instId, realInstId, ttid, tt, partnMergeWaitGroup)
						delete(processedInsts, instKey)
					}
				}

				// If all indexes are built, defnIdMap will have no entries
				if len(processedInsts) == 0 {
					l.Infof("ShardRebalancer::waitForIndexState All indexes: %v are active and caught up. "+
						"Waiting for pending merge to finish", activeIndexes)
					// Wait for merge of partitioned indexes to finish before returning
					partnMergeWaitGroup.Wait()
					return nil
				}
			}
		}
		// reset retry count as one iteration of the loop could be completed
		// successfully without any error
		retryCount = 0

		time.Sleep(retryInterval * time.Second)
	}

	return nil
}

func (sr *ShardRebalancer) destTokenToMergeOrReady(instId c.IndexInstId,
	realInstId c.IndexInstId, ttid string, tt *c.TransferToken, partnMergeWaitGroup sync.WaitGroup) {
	// There is no proxy (no merge needed)
	if realInstId == 0 {

		respch := make(chan error)
		sr.supvMsgch <- &MsgUpdateIndexRState{
			instId: instId,
			rstate: c.REBAL_ACTIVE,
			respch: respch}
		err := <-respch
		// If any error other than ErrIndexDeletedDuringRebal, crash indexer
		if err != nil && !isIndexDeletedDuringRebal(err.Error()) {
			c.CrashOnError(err)
		}

		// metaKV state update will happen after all index instances in the shard are built
	} else {
		// There is a proxy (merge needed). The proxy partitions need to be move
		// to the real index instance before Token can move to Ready state.
		partnMergeWaitGroup.Add(1)

		go func(ttid string, tt *c.TransferToken) {
			defer partnMergeWaitGroup.Done()

			ticker := time.NewTicker(time.Duration(20) * time.Second)

			// Create a non-blocking channel so that even if rebalance fails,
			// indexer can still push a response to the response channel with
			// out this go-routine being active
			respch := make(chan error, 1)
			sr.supvMsgch <- &MsgMergePartition{
				srcInstId:  instId,
				tgtInstId:  realInstId,
				rebalState: c.REBAL_ACTIVE,
				respCh:     respch}

			var err error
			select {
			case <-sr.cancel:
				l.Infof("ShardRebalancer::destTokenToMergeOrReady rebalance cancel Received")
				return
			case <-sr.done:
				l.Infof("ShardRebalancer::destTokenToMergeOrReady rebalance done Received")
				return
			case <-ticker.C:
				l.Infof("ShardRebalancer::destTokenToMergeOrReady waiting for partition merge "+
					"to finish for index instance: %v, realInst: %v", instId, realInstId)
			case err = <-respch:
			}
			if err != nil && !isIndexDeletedDuringRebal(err.Error()) {
				// The indexer could be in an inconsistent state.
				// So we will need to restart the indexer.
				time.Sleep(100 * time.Millisecond)
				c.CrashOnError(err)
			}

			// Since there can be multiple partitioned indexes in a single shard
			// do not update the transfer token state yet. Once all indexes move
			// to active state, ShardRebalancer will check the state of partitioned
			// indexes and move the token state to ShardTokenReady if all the
			// partitioned indexes move to RState: REBAL_MERGED
		}(ttid, tt)

	}
}

func isInstProcessed(instId, realInstId c.IndexInstId, processedInsts map[c.IndexInstId]bool) bool {

	if len(processedInsts) > 0 {
		if realInstId != 0 {
			if _, ok := processedInsts[realInstId]; !ok {
				return false
			}
		} else {
			if _, ok := processedInsts[instId]; !ok {
				return false
			}
		}
	}
	return true
}

func (sr *ShardRebalancer) getIndexStatusFromMeta(tt *c.TransferToken,
	processedInsts map[c.IndexInstId]bool, localMeta *manager.LocalIndexMetadata) (map[c.IndexInstId]c.IndexState, map[c.IndexInstId]string) {

	outStates := make(map[c.IndexInstId]c.IndexState)
	outErr := make(map[c.IndexInstId]string)
	for i, inst := range tt.IndexInsts {

		instId := tt.InstIds[i]
		realInstId := tt.RealInstIds[i]

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

func (sr *ShardRebalancer) doRebalance() {

	if sr.transferTokens == nil {
		sr.cb.progress(1.0, sr.cancel)
		sr.finishRebalance(nil)
		return
	}

	// If DDL is allowed during rebalance, skip the ongoing DDL check.
	// Otherwise, if there is an on-going DDL, fail the rebalance.
	if !sr.canAllowDDLDuringRebalance() {
		if ddl, err := sr.runParams.checkDDLRunning("ShardRebalancer"); ddl {
			sr.finishRebalance(err)
			return
		}
	}

	select {
	case <-sr.cancel:
		l.Infof("ShardRebalancer::doRebalance Cancel Received. Skip Publishing Tokens.")
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
		"moved to ScheduleAck state. Initiating transfer for futher processing")

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
func getProgressForToken(statusResp *IndexStatusResponse, ttid string, token *c.TransferToken) float64 {
	var node string
	if token.ShardTransferTokenState == c.ShardTokenTransferShard {
		node = token.SourceHost
	} else if token.ShardTransferTokenState == c.ShardTokenRestoreShard {
		node = token.DestHost
	}

	nodeLevelProgress, ok := statusResp.RebalTransferProgress[node]
	if !ok || nodeLevelProgress == nil {
		return 0.0
	}

	switch nodeLevelProgress.(type) {
	case map[string]interface{}:
		progress := nodeLevelProgress.(map[string]interface{})
		if val, ok := progress[ttid]; !ok || val == nil {
			return 0.0
		} else {
			return val.(float64)
		}
	default:
		return 0.0
	}
}

// Shard rebalancer's version of compute progress method
func (sr *ShardRebalancer) computeProgress() float64 {

	url := "/getIndexStatus?getAll=true"
	resp, err := getWithAuth(sr.localaddr + url)
	if err != nil {
		l.Errorf("ShardRebalancer::computeProgress Error getting local metadata %v %v", sr.localaddr+url, err)
		return 0
	}

	defer resp.Body.Close()
	statusResp := new(IndexStatusResponse)
	bytes, _ := ioutil.ReadAll(resp.Body)
	if err := json.Unmarshal(bytes, &statusResp); err != nil {
		l.Errorf("ShardRebalancer::computeProgress Error unmarshal response %v %v", sr.localaddr+url, err)
		return 0
	}

	// Make a shallow copy of the transfer token map
	getTransferTokenMap := func() map[string]*c.TransferToken {
		out := make(map[string]*c.TransferToken)

		sr.mu.Lock()
		defer sr.mu.Unlock()

		for ttid, token := range sr.transferTokens {
			out[ttid] = token
		}
		return out
	}

	tokens := getTransferTokenMap()
	totTokens := len(tokens)

	transferWt := 0.35
	restoreWt := 0.35
	recoverWt := 0.3

	var totalProgress float64
	for ttid, tt := range tokens {
		state := tt.ShardTransferTokenState
		// All states not tested in the if-else if are treated as 0% progress
		if state == c.ShardTokenReady || state == c.ShardTokenMerged ||
			state == c.ShardTokenCommit || state == c.ShardTokenDeleted {
			totalProgress += 100.00
		} else if state == c.ShardTokenTransferShard {
			totalProgress += transferWt * getProgressForToken(statusResp, ttid, tt)
		} else if state == c.ShardTokenRestoreShard {
			// Transfer is complete. So, add progress related to transfer
			totalProgress += transferWt*100 + restoreWt*getProgressForToken(statusResp, ttid, tt)
		} else if state == c.ShardTokenRecoverShard {
			// If index state is recovered, get build progress
			totalProgress += transferWt*100.0 + restoreWt*100.0
			totalProgress += recoverWt * sr.getBuildProgressFromStatus(statusResp, tt)
		}
	}

	progress := (totalProgress / float64(totTokens)) / 100.0
	l.Infof("ShardRebalancer::computeProgress %v", progress)

	if progress < 0.1 || math.IsNaN(progress) {
		progress = 0.1
	} else if progress == 1.0 {
		progress = 0.99
	}

	return progress
}

// getBuildProgressFromStatus is a helper for computeProgress that gets an estimate of index build progress for the
// given transfer token from the status arg.
func (sr *ShardRebalancer) getBuildProgressFromStatus(status *IndexStatusResponse, tt *c.TransferToken) float64 {

	totalProgress := 0.0
	for i, inst := range tt.IndexInsts {
		instId := tt.InstIds[i]
		realInstId := tt.RealInstIds[i] // for partitioned indexes

		// A deferred index is completely moved
		if inst.Defn.Deferred {
			totalProgress += 100
			continue
		}

		// If it is a partitioned index, it is possible that we cannot find progress from instId:
		// 1) partition is built using realInstId
		// 2) partition has already been merged to instance with realInstId
		// In either case, count will be 0 after calling getBuildProgress(instId) and it will find progress
		// using realInstId instead.
		realInstProgress, count := sr.getBuildProgress(status, instId, realInstId, inst.Defn, tt.DestId)
		if count == 0 {
			realInstProgress, count = sr.getBuildProgress(status, realInstId, realInstId, inst.Defn, tt.DestId)
		}

		if count > 0 {
			sr.lastKnownProgress[instId] = realInstProgress / float64(count)
		}

		if p, ok := sr.lastKnownProgress[instId]; ok {
			totalProgress += p
		}
	}
	return (totalProgress / float64(len(tt.IndexInsts))) / 100
}

func (sr *ShardRebalancer) getBuildProgress(status *IndexStatusResponse,
	instId, realInstId c.IndexInstId, defn c.IndexDefn, destId string) (realInstProgress float64, count int) {

	for _, idx := range status.Status {
		if idx.InstId == instId {
			// This function is called for every transfer token before it has becomes COMMITTED or DELETED.
			// The index may have not be in REAL_PENDING state but the token has not yet moved to COMMITTED/DELETED state.
			// So we need to return progress even if it is not replicating.
			// Pre-7.0 nodes will report "Replicating" instead of "Moving" so check for both.
			if idx.Status == "Moving" || idx.Status == "Replicating" || idx.NodeUUID == destId {
				progress, ok := sr.lastKnownProgress[instId]
				if !ok || idx.Progress > 0 {
					progress = idx.Progress
				}

				destNode := getDestNode(defn.Partitions[0], idx.PartitionMap)
				l.Infof("ShardRebalancer::getBuildProgress Index: %v:%v:%v:%v"+
					" Progress: %v InstId: %v RealInstId: %v Partitions: %v Destination: %v",
					defn.Bucket, defn.Scope, defn.Collection, defn.Name,
					progress, instId, realInstId, defn.Partitions, destNode)

				realInstProgress += progress
				count++
			} else if idx.Status == "Ready" {
				realInstProgress += 100
				count++
			}
		}
	}
	return realInstProgress, count
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

			if sr.addToWaitGroup() {
				notifych <- true
				go sr.dropShardsWhenIdle(ttid, tt.Clone(), notifych)
			} else {
				logging.Warnf("ShardRebalancer::processDropShards Skip processing drop shards request for ttid: %v "+
					"as rebalancer can not add to wait group", tt)
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

	droppedIndexes := make(map[c.IndexInstId]bool)
	retryInterval := time.Duration(1)
	retryCount := 0
loop:
	for {
		select {
		case <-sr.cancel:
			l.Infof("ShardRebalancer::dropShardsWhenIdle: Cancel Received")
			break loop
		case <-sr.done:
			l.Infof("ShardRebalancer::dropShardsWhenIdle: Done Received")
			break loop

		default:

			localMeta, err := getLocalMeta(sr.localaddr)
			if err != nil {
				l.Errorf("ShardRebalancer::dropShardsWhenIdle Error Fetching Local Meta %v %v", sr.localaddr, err)
				retryCount++

				if retryCount > 5 {
					return // Return after 5 unsuccessful attempts
				}
				time.Sleep(retryInterval * time.Second)
				goto loop
			}

			indexStateMap, errMap := sr.getIndexStatusFromMeta(tt, nil, localMeta)

			for i, inst := range tt.IndexInsts {

				defn := &inst.Defn
				defn.SetCollectionDefaults()
				defn.InstId = tt.InstIds[i]
				defn.RealInstId = tt.RealInstIds[i]

				allStats := sr.statsMgr.stats.Get()

				instKey := defn.RealInstId
				if defn.RealInstId == 0 {
					instKey = defn.InstId
				}

				if _, ok := droppedIndexes[instKey]; ok { // Instance is already dropped
					continue
				}

				if instState, ok := indexStateMap[instKey]; ok {
					err := errMap[instKey]
					if instState == common.INDEX_STATE_NIL || instState == common.INDEX_STATE_DELETED {
						droppedIndexes[instKey] = true // consider the index as deleted
						continue
					} else if err != "" {
						l.Errorf("ShardRebalancer::dropShardsWhenIdle Error Fetching Index Status %v %v", sr.localaddr, err)
						retryCount++

						if retryCount > 50 { // After 50 retires
							droppedIndexes[instKey] = true // Consider index drop successful and continue
							continue
						}
						time.Sleep(retryInterval * time.Second)
						goto loop // Retry
					}
				}

				defnStats := allStats.indexes[instKey] // stats for current defn
				if defnStats == nil {
					l.Infof("ShardRebalancer::dropShardsWhenIdle Missing defnStats for instId %v. Considering the index as dropped", instKey)
					continue // If index is deleted, then it is validated from localMeta
				}

				var pending, numRequests, numCompletedRequests int64
				for _, partitionId := range defn.Partitions {
					partnStats := defnStats.partitions[partitionId] // stats for this partition
					if partnStats != nil {
						numRequests = partnStats.numRequests.GetValue().(int64)
						numCompletedRequests = partnStats.numCompletedRequests.GetValue().(int64)
					} else {
						l.Infof("ShardRebalancer::dropShardsWhenIdle Missing partnStats for instId %d partition %v. Considering the inst dropped",
							instKey, partitionId)
						droppedIndexes[instKey] = true
						continue
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
					if isIndexInAsyncRecovery(response.Error) {
						// Wait for async recovery to finish
						l.Errorf("ShardRebalancer::dropShardsWhenIdle: Index is in async recovery, defn: %v, url: %v, err: %v",
							defn, url, response.Error)
						continue
					} else if !isMissingBSC(response.Error) &&
						!isIndexDeletedDuringRebal(response.Error) &&
						!isIndexNotFoundRebal(response.Error) {
						l.Errorf("ShardRebalancer::dropShardsWhenIdle: Error dropping index defn: %v, url: %v, err: %v",
							defn, url, response.Error)
						sr.setTransferTokenError(ttid, tt, response.Error)
						return
					}
					// Ok: failed to drop source index because b/s/c was dropped. Continue to TransferTokenCommit state.
					l.Infof("ShardRebalancer::dropShardsWhenIdle: Source index already dropped due to bucket/scope/collection dropped. tt %v.", method, tt)
				}
				droppedIndexes[instKey] = true
			}
		}

		allInstancesDropped := true
		for i, _ := range tt.IndexInsts {
			instKey := tt.RealInstIds[i]
			if instKey == 0 {
				instKey = tt.InstIds[i]
			}

			if _, ok := droppedIndexes[instKey]; !ok {
				// Still some instances need to be dropped
				allInstancesDropped = false
				break
			}
		}

		if allInstancesDropped {

			sr.initiateLocalShardCleanup(ttid, tt.ShardPaths, tt)

			tt.ShardTransferTokenState = c.ShardTokenCommit
			setTransferTokenInMetakv(ttid, tt)
			return
		}

		time.Sleep(1 * time.Second)
	}
}

func (sr *ShardRebalancer) finishRebalance(err error) {
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

func (sr *ShardRebalancer) cancelMetakv() bool {
	sr.metakvMutex.Lock()
	defer sr.metakvMutex.Unlock()

	if sr.metakvCancel != nil {
		close(sr.metakvCancel)
		sr.metakvCancel = nil
		return true
	}
	return false
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

	if sr.cancelMetakv() {
		close(sr.cancel)
		sr.wg.Wait()
	}
}

// This function batches a group of transfer tokens
// according to the following rules:
//
// All tokens of a bucket moving from one subcluster
// to another will be in the same batch. E.g.,
// if bucket_1 has indexes on subcluster_1:indexer_1 and
// subcluster_1:indexer_2 - There will be 2 transfer tokens
// for this bucket movement. Both these transfer tokens will
// remain in same batch.
//
// As indexer source node can drop index instances only after
// both tokens move to READY state, publishing them in different
// batches can lead to a deadlock where after finishing the first
// batch, tokens would wait for sibling to come to Ready but sibling
// would wait for initial token to move to Deleted state.
func (sr *ShardRebalancer) batchTransferTokens() {
	tokenBatched := make(map[string]bool)

	for ttid, token := range sr.transferTokens {

		if _, ok := tokenBatched[ttid]; !ok {
			tokenMap := make(map[string]*c.TransferToken)
			tokenMap[ttid] = token
			if token.SiblingExists() {
				tokenMap[token.SiblingTokenId] = sr.transferTokens[token.SiblingTokenId]
			}
			sr.batchedTokens = append(sr.batchedTokens, tokenMap)

			tokenBatched[ttid] = true
			if token.SiblingExists() {
				tokenBatched[token.SiblingTokenId] = true
			}
		}
	}
}

// called by master to initiate transfer of shards.
// sr.mu needs to be acquired by the caller of this method
func (sr *ShardRebalancer) initiateShardTransferAsMaster() {

	config := sr.config.Load()
	batchSize := config["rebalance.serverless.transferBatchSize"].Int()

	publishAllTokens := false
	if batchSize == 0 { // Disable batching and publish all tokens
		publishAllTokens = true
	}

	if !publishAllTokens && sr.currBatchSize >= batchSize {
		l.Infof("ShardRebalancer::initiateShardTransferAsMaster Returning as currBatchSize(%v) >= batchSize(%v)",
			sr.currBatchSize, batchSize)
		return
	}

	var publishedIds []string

	// TODO: Publish tokens related to replica repair.
	// Prioritise replica repair over rebalance

	for i, groupedTokens := range sr.batchedTokens {
		for ttid, tt := range groupedTokens {
			if tt.ShardTransferTokenState == c.ShardTokenScheduleAck {
				// Used a cloned version so that the master token list
				// will not be updated until the transfer token with
				// updated state is persisted in metaKV
				ttClone := tt.Clone()

				// Change state of transfer token to TransferTokenTransferShard
				ttClone.ShardTransferTokenState = c.ShardTokenTransferShard
				setTransferTokenInMetakv(ttid, ttClone)
				publishedIds = append(publishedIds, ttid)
				sr.currBatchSize++
			}
		}

		// TODO: With swap rebalance, replica repair and index movements in the list of
		// transfer tokens, the transerBatchSize becomes a soft limit i.e. if there are
		// 2 index movement tokens belonging to a bucket and 3 other swap rebalance tokens
		// all 5 will be processed in same batch. This needs to be fixed
		sr.batchedTokens[i] = nil // Deleted published tokens from batch list

		if !publishAllTokens && sr.currBatchSize >= batchSize {
			break
		}
	}
	if len(publishedIds) > 0 {
		l.Infof("ShardRebalancer::initiateShardTransferAsMaster Published transfer token batch: %v, "+
			"currBatchSize: %v", publishedIds, sr.currBatchSize)
	}
}

func (sr *ShardRebalancer) checkAllTokensDone() bool {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	for ttid, tt := range sr.transferTokens {
		if tt.ShardTransferTokenState != c.ShardTokenDeleted {
			l.Infof("ShardRebalancer::checkAllTokensDone Tranfser token: %v is in state: %v", ttid, tt.ShardTransferTokenState)
			return false
		}
	}
	return true
}

func genShardTokenDropOnSource(rebalId, sourceTokenId, siblingTokenId string) (string, *c.TransferToken) {
	ustr, _ := common.NewUUID()
	dropOnSourceTokenId := fmt.Sprintf("TransferToken%s", ustr.Str())

	dropOnSourceToken := &c.TransferToken{
		ShardTransferTokenState: c.ShardTokenDropOnSource,
		Version:                 c.MULTI_INST_SHARD_TRANSFER,
		RebalId:                 rebalId,
		SourceTokenId:           sourceTokenId,
		SiblingTokenId:          siblingTokenId,
	}

	return dropOnSourceTokenId, dropOnSourceToken
}

func (sr *ShardRebalancer) updateTransferProgress() {
	ticker := time.NewTicker(5 * time.Second)

	resetTransferStats := func() {
		indexerStats := sr.statsMgr.stats.Get()
		indexerStats.RebalanceTransferProgress.Reset()
	}

	for {
		select {
		case <-sr.cancel:
			resetTransferStats()
			return

		case <-sr.done:
			resetTransferStats()
			return

		case <-ticker.C:

			func() {
				sr.mu.RLock()
				defer sr.mu.RUnlock()

				transferProgressMap := make(map[string]interface{})
				for ttid, perTokenStats := range sr.transferStats {
					progress := 0.0
					for _, stats := range perTokenStats {
						if stats.totalBytes > 0 {
							progress += (((float64)(stats.bytesWritten) * 100.0) / ((float64)(stats.totalBytes)))
						} else {
							progress = 100.0
						}
					}
					if len(perTokenStats) == 1 { // Token contains only one shard
						transferProgressMap[ttid] = progress
					} else {
						transferProgressMap[ttid] = progress / 2.0 // Average on both shards
					}
				}

				indexerStats := sr.statsMgr.stats.Get()
				indexerStats.RebalanceTransferProgress.Set(transferProgressMap)
			}()
		}
	}
}

// This method is called on rebalance master, after all tokens are
// moved to ShardTokenScheduleAck phase. This method is called after
// acquiring the sr.mu mutex
func (sr *ShardRebalancer) updateRebalancePhaseInGlobalRebalToken() {

	// Make a clone of rebalance token
	t1 := *sr.rebalToken
	t2 := t1
	cloneRebalToken := &t2

	fn := func(r int, e error) error {
		rebalToken := cloneRebalToken
		rebalToken.Version = common.AllowDDLDuringRebalance_v1
		rebalToken.RebalPhase = common.RebalanceTransferInProgress
		err := c.MetakvSet(RebalanceTokenPath, rebalToken)
		if err != nil {
			l.Errorf("ShardRebalancer::updateRebalancePhaseInGlobalRebalToken Unable to set RebalanceToken In Metakv Storage. Err %v", err)
			return err
		}
		l.Infof("ShardRebalancer::updateRebalancePhaseInGlobalRebalToken Registered Global Rebalance Token In Metakv %v", rebalToken)
		return nil
	}

	helper := c.NewRetryHelper(10, time.Second, 1, fn)
	if err := helper.Run(); err != nil {
		logging.Errorf("ShardRebalancer::updateRebalancePhaseInGlobalRebalToken Unable to update global rebalance phase in rebalance token")
	}
	// Do not return the error - In case of error all DDL's during rebalance will be blocked, but rebalance can proceed
}

func (sr *ShardRebalancer) updateBucketTransferPhase(bucket string, tranfserPhase common.RebalancePhase) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	bucketPhase := make(map[string]common.RebalancePhase)
	bucketPhase[bucket] = tranfserPhase
	globalPh := common.RebalanceInitated
	if sr.rebalToken.RebalPhase == common.RebalanceTransferInProgress {
		globalPh = common.RebalanceTransferInProgress
	}

	msg := &MsgUpdateRebalancePhase{
		GlobalRebalancePhase: globalPh,
		BucketTransferPhase:  bucketPhase,
	}

	sr.supvMsgch <- msg
}

func (sr *ShardRebalancer) canAllowDDLDuringRebalance() bool {
	// Allow or reject DDL based on "allowDDLDuringRebalance" config
	config := sr.config.Load()
	if common.IsServerlessDeployment() {
		canAllowDDLDuringRebalance := config["serverless.allowDDLDuringRebalance"].Bool()
		if !canAllowDDLDuringRebalance {
			logging.Warnf("ShardRebalancer::canAllowDDLDuringRebalance Disallowing DDL as config: serverless.allowDDLDuringRebalance is false")
			return false
		}
	} else {
		canAllowDDLDuringRebalance := config["allowDDLDuringRebalance"].Bool()
		if !canAllowDDLDuringRebalance {
			logging.Warnf("ShardRebalancer::canAllowDDLDuringRebalance Disallowing DDL as config: allowDDLDuringRebalance is false")
			return false
		}
	}
	return true
}

// isIndexDeletedDuringRebal returns true if an index is deleted while
// bucket rebalance is in progress
func isIndexDeletedDuringRebal(errMsg string) bool {
	return errMsg == common.ErrIndexDeletedDuringRebal.Error()
}

func isIndexInAsyncRecovery(errMsg string) bool {
	return errMsg == common.ErrIndexInAsyncRecovery.Error()
}

func lockShards(shardIds []common.ShardId, supvMsgch MsgChannel, lockedForRecovery bool) error {

	respCh := make(chan map[common.ShardId]error)
	msg := &MsgLockUnlockShards{
		mType:             LOCK_SHARDS,
		shardIds:          shardIds,
		lockedForRecovery: lockedForRecovery,
		respCh:            respCh,
	}

	supvMsgch <- msg

	errMap := <-respCh
	for _, err := range errMap { // Return on the first error seen
		if err != nil {
			return err
		}
	}

	return nil
}

func unlockShards(shardIds []common.ShardId, supvMsgch MsgChannel) error {

	respCh := make(chan map[common.ShardId]error)
	msg := &MsgLockUnlockShards{
		mType:    UNLOCK_SHARDS,
		shardIds: shardIds,
		respCh:   respCh,
	}

	supvMsgch <- msg

	errMap := <-respCh
	for _, err := range errMap { // Return on the first error seen
		if err != nil {
			return err
		}
	}

	return nil
}

func restoreShardDone(shardIds []common.ShardId, supvMsgch MsgChannel) {
	respCh := make(chan bool)
	msg := &MsgRestoreShardDone{
		shardIds: shardIds,
		respCh:   respCh,
	}

	supvMsgch <- msg

	<-respCh
}
