package indexer

import (
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

	// lock protecting access to maps like transferTokens, sourceTokens etc.
	mu sync.Mutex

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

		cancel:              make(chan struct{}),
		done:                make(chan struct{}),
		metakvCancel:        make(chan struct{}),
		waitForTokenPublish: make(chan struct{}),
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
		// TODO: Add logic to publish transfer tokens to metaKV and start processing them
	}
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

func (sr *ShardRebalancer) processShardTransferToken(ttid string, tt *c.TransferToken) {
	// TODO: Add logic to process the shard transfer token
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

func (sr *ShardRebalancer) Cancel() {
	l.Infof("ShardRebalancer::Cancel Exiting")

	sr.cancelMetakv()
	close(sr.cancel)
	sr.wg.Wait()
}
