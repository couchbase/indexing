// @copyright 2016-Present Couchbase, Inc.
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
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/indexing/secondary/common"
	c "github.com/couchbase/indexing/secondary/common"
	forestdb "github.com/couchbase/indexing/secondary/fdb"
	"github.com/couchbase/indexing/secondary/logging"
	l "github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/manager"
	"github.com/couchbase/indexing/secondary/manager/client"
	mc "github.com/couchbase/indexing/secondary/manager/common"
	"github.com/couchbase/indexing/secondary/planner"
)

type DoneCallback func(err error, cancel <-chan struct{})
type ProgressCallback func(progress float64, cancel <-chan struct{})

type Callbacks struct {
	progress ProgressCallback
	done     DoneCallback
}

type Rebalancer struct {
	clusterVersion int64 // to check for down-level nodes

	// transferTokens is the master's map by [ttid] of all transfer tokens (originally from Planner or passed
	// in for move index case)
	// ##### ALERT: State may differ in certain cases between these and metakv. #####
	transferTokens map[string]*c.TransferToken

	// toBuildTTsByDestId is the master's map by [DestId][ttid] of unstarted to-build TTs; DestId is the nodeUUID
	toBuildTTsByDestId map[string]map[string]*c.TransferToken

	// buildingTTsByDestId is the master's map by [DestId][ttid] of currently building and/or
	// dropping TTs (i.e. those published but not yet completely finished); DestId is the nodeUUID.
	// We don't consider a new batch slot to open up until a previously published token is deleted,
	// as the source node's resources for an index are not freed up until it is dropped.
	buildingTTsByDestId map[string]map[string]*c.TransferToken

	// acceptedTokens is a destination node's map by [ttid] of its TTs; these never reach state TransferTokenDeleted
	acceptedTokens map[string]*c.TransferToken

	// sourceTokens is a source node's map by [ttid] of its TTs that need index drops after moving off the node
	sourceTokens map[string]*c.TransferToken

	// nodeLoads is a sortable slice of the most recent workload info for each indexer node;
	// nil for index move case
	nodeLoads NodeLoadSlice

	// nodeLoadsTime holds the last time the indexer stats in nodeLoads were gathered
	nodeLoadsTime time.Time

	dropQueue  chan string         // ttids of source indexes waiting to be submitted for drop
	dropQueued map[string]struct{} // set of ttids already added to dropQueue, as metakv can send duplicate notifications

	mu sync.RWMutex // for transferTokens, acceptedTokens, sourceTokens, toBuildTTsByDestId, buildingTTsByDestId,
	// nodeLoads, nodeLoadsTime, dropQueue, dropQueued

	rebalToken *RebalanceToken
	nodeUUID   string // this node's new-style node ID (32-digit random hex assigned by ns_server)
	master     bool   // is this the rebalance master node?

	cb Callbacks // rebalance progress and done callback functions

	cancel         chan struct{} // closed to signal rebalance canceled
	done           chan struct{} // closed to signal rebalance done (not canceled)
	dropIndexDone  chan struct{} // closed to signal rebalance is done with dropping duplicate indexes
	removeDupIndex bool          // duplicate index removal is called

	isDone int32

	metakvCancel chan struct{} // close this to end metakv.RunObserveChildren callbacks
	muCleanup    sync.RWMutex  // for metakvCancel

	supvMsgch           MsgChannel
	localaddr           string         // local indexer host:port for HTTP, e.g. "127.0.0.1:9102", 9108,...
	wg                  sync.WaitGroup // group of all currently running Rebalance goroutines
	cleanupOnce         sync.Once
	waitForTokenPublish chan struct{} // blocks observeRebalance until closed
	retErr              error
	config              c.ConfigHolder
	lastKnownProgress   map[c.IndexInstId]float64

	topologyChange *service.TopologyChange // some info passed in on topology change being done
	runPlanner     bool                    // should this rebalance run the planner?

	// globalTopology is the cluster topology from getGlobalTopology at start of rebalance; nil in
	// index move case
	globalTopology *manager.ClusterIndexMetadata

	runParam *runParams
}

// NewRebalancer creates the Rebalancer object that will run the rebalance on this node and starts
// go routines that perform it asynchronously. If this is a worker node it also launches
// an observeRebalance go routine that does the token processing for this node. If this
// is the master that will be launched later by initRebalAsync --> doRebalance.
func NewRebalancer(transferTokens map[string]*c.TransferToken, rebalToken *RebalanceToken,
	nodeUUID string, master bool, progress ProgressCallback, done DoneCallback,
	supvMsgch MsgChannel, localaddr string, config c.Config, topologyChange *service.TopologyChange,
	runPlanner bool, runParam *runParams) *Rebalancer {

	clusterVersion := common.GetClusterVersion()
	l.Infof("NewRebalancer nodeId %v rebalToken %v master %v localaddr %v runPlanner %v runParam %v clusterVersion %v", nodeUUID,
		rebalToken, master, localaddr, runPlanner, runParam, clusterVersion)

	r := &Rebalancer{
		clusterVersion: clusterVersion,

		transferTokens: transferTokens,
		rebalToken:     rebalToken,
		master:         master,
		nodeUUID:       nodeUUID,

		cb: Callbacks{progress, done},

		cancel:         make(chan struct{}),
		done:           make(chan struct{}),
		dropIndexDone:  make(chan struct{}),
		removeDupIndex: false,

		metakvCancel: make(chan struct{}),

		supvMsgch: supvMsgch,

		acceptedTokens: make(map[string]*c.TransferToken),
		sourceTokens:   make(map[string]*c.TransferToken),
		dropQueue:      make(chan string, 10000),
		dropQueued:     make(map[string]struct{}),
		localaddr:      localaddr,

		waitForTokenPublish: make(chan struct{}),
		lastKnownProgress:   make(map[c.IndexInstId]float64),

		topologyChange: topologyChange,
		runPlanner:     runPlanner,

		runParam: runParam,
	}

	r.config.Store(config)

	if master {
		go r.initRebalAsync()
	} else {
		close(r.waitForTokenPublish)
		go r.observeRebalance()
	}
	go r.processDropIndexQueue()

	return r
}

// RemoveDuplicateIndexes function gets input of hosts and index definitions to be removed from hosts,
// these are indexes that have been detected as duplicate and are being removed as part of rebalance.
// The method itself deos not do anything to identfy or detect duplicates and only drops the input set of index
// that it recieves but the name has been given as removeDuplicateIndexes to identify its purpose
// which is different than other dropIndex cleanup that rebalance process does.
// Also if there is an error while dropping one or more indexes it simply logs the error but does not fail the rebalance.
func (r *Rebalancer) RemoveDuplicateIndexes(hostIndexMap map[string]map[common.IndexDefnId]*common.IndexDefn) {
	defer close(r.dropIndexDone)
	var mu sync.Mutex
	var wg sync.WaitGroup
	const method string = "Rebalancer::RemoveDuplicateIndexes:"
	errMap := make(map[string]map[common.IndexDefnId]error)

	uniqueDefns := make(map[common.IndexDefnId]bool)
	for _, indexes := range hostIndexMap {
		for _, index := range indexes {
			// true is set if we are not able to post dropToken, in which case index is not removed
			// initially all indexes to be dropped.
			uniqueDefns[index.DefnId] = false
		}
	}

	// Place delete token for recovery and any corner cases.
	// We place the drop tokens for every identified index except for the case of rebalance being canceled/done
	// If we fail to place drop token even after 3 retries we will not drop that index to avoid any errors in dropIndex
	// causing metadata consistency and cleanup problems
	for defnId, _ := range uniqueDefns {
	loop:
		for i := 0; i < 3; i++ { // 3 retries in case of error on PostDeleteCommandToken
			select {
			case <-r.cancel:
				l.Warnf("%v Cancel Received. Skip processing drop duplicate indexes.", method)
				return
			case <-r.done:
				l.Warnf("%v Cannot drop duplicate index when rebalance is done.", method)
				return
			default:
				l.Infof("%v posting dropToken for defnid %v", method, defnId)
				if err := mc.PostDeleteCommandToken(defnId, true); err != nil {
					if i == 2 { // all retries have failed to post drop token
						uniqueDefns[defnId] = true
						l.Errorf("%v failed to post delete command token after 3 retries, for index defnId %v due to internal errors.  Error=%v.", method, defnId, err)
					} else {
						l.Warnf("%v failed to post delete command token for index defnId %v due to internal errors.  Error=%v.", method, defnId, err)
					}
					break // break select and retry PostDeleteCommandToken
				}
				break loop // break retry loop and move to next defn
			}
		}
	}

	removeIndexes := func(host string, indexes map[common.IndexDefnId]*common.IndexDefn) {
		defer wg.Done()

		for _, index := range indexes {
			select {
			case <-r.cancel:
				l.Warnf("%v Cancel Received. Skip processing drop duplicate indexes.", method)
				return
			case <-r.done:
				l.Warnf("%v Cannot drop duplicate index when rebalance is done.", method)
				return
			default:
				if uniqueDefns[index.DefnId] == true { // we were not able to post dropToken for this index.
					break // break select move to next index
				}
				if err := r.makeDropIndexRequest(index, host); err != nil {
					// we will only record the error and proceeded, not failing rebalance just because we could not delete
					// a index.
					mu.Lock()
					if _, ok := errMap[host]; !ok {
						errMap[host] = make(map[common.IndexDefnId]error)
					}
					errMap[host][index.DefnId] = err
					mu.Unlock()
				}
			}
		}
	}

	select {
	case <-r.cancel:
		l.Warnf("%v Cancel Received. Skip processing drop duplicate indexes.", method)
		return
	case <-r.done:
		l.Warnf("%v Cannot drop duplicate index when rebalance is done.", method)
		return
	default:
		for host, indexes := range hostIndexMap {
			wg.Add(1)
			go removeIndexes(host, indexes)
		}

		wg.Wait()

		for host, indexes := range errMap {
			for defnId, err := range indexes { // not really an error as we already posted drop tokens
				l.Warnf("%v encountered error while removing index on host %v, defnId %v, err %v", method, host, defnId, err)
			}
		}
	}
}

func (r *Rebalancer) makeDropIndexRequest(defn *common.IndexDefn, host string) error {
	const method string = "Rebalancer::makeDropIndexRequest:" // for logging
	req := manager.IndexRequest{Index: *defn}
	body, err := json.Marshal(&req)
	if err != nil {
		l.Errorf("%v error in marshal drop index defnId %v err %v", method, defn.DefnId, err)
		return err
	}

	bodybuf := bytes.NewBuffer(body)

	url := "/dropIndex"
	resp, err := postWithAuth(host+url, "application/json", bodybuf)
	if err != nil {
		l.Errorf("%v error in drop index on host %v, defnId %v, err %v", method, host, defn.DefnId, err)
		if err == io.EOF {
			// should not rety in case of rebalance done or cancel
			select {
			case <-r.cancel:
				return err // return original error
			case <-r.done:
				return err
			default:
				bodybuf := bytes.NewBuffer(body)
				resp, err = postWithAuth(host+url, "application/json", bodybuf)
				if err != nil {
					l.Errorf("%v error in drop index on host %v, defnId %v, err %v", method, host, defn.DefnId, err)
					return err
				}
			}
		} else {
			return err
		}
	}

	response := new(manager.IndexResponse)
	err = convertResponse(resp, response)
	if err != nil {
		l.Errorf("%v encountered error parsing response, host %v, defnId %v, err %v", method, host, defn.DefnId, err)
		return err
	}

	if response.Code == manager.RESP_ERROR {
		if strings.Contains(response.Error, forestdb.FDB_RESULT_KEY_NOT_FOUND.Error()) {
			l.Errorf("%v error dropping index, host %v defnId %v err %v. Ignored.", method, host, defn.DefnId, response.Error)
			return nil
		}
		l.Errorf("%v error dropping index, host %v, defnId %v, err %v", method, host, defn.DefnId, response.Error)
		return err
	}
	l.Infof("%v removed index defnId %v, defn %v, from host %v", method, defn.DefnId, defn, host)
	return nil
}

// initRebalAsync runs in the rebalance master as a helper go routine for NewRebalancer.
// It calls the planner if needed, then launches a separate go routine, doRebalance, to
// manage the fine-grained steps of the rebalance.
func (r *Rebalancer) initRebalAsync() {

	var hostToIndexToRemove map[string]map[common.IndexDefnId]*common.IndexDefn
	//short circuit
	if len(r.transferTokens) == 0 && !r.runPlanner {
		r.cb.progress(1.0, r.cancel)
		r.finishRebalance(nil)
		return
	}

	// Launch the progress updater goroutine
	if r.cb.progress != nil {
		go r.updateProgress()
	}

	if r.runPlanner {
		cfg := r.config.Load()
	loop:
		for {
			select {
			case <-r.cancel:
				l.Infof("Rebalancer::initRebalAsync Cancel Received")
				return

			case <-r.done:
				l.Infof("Rebalancer::initRebalAsync Done Received")
				return

			default:
				allWarmedup, _ := checkAllIndexersWarmedup(cfg["clusterAddr"].String())
				if allWarmedup {
					globalTopology, err := getGlobalTopology(r.localaddr)
					if err != nil {
						l.Errorf("Rebalancer::initRebalAsync Error Fetching Topology %v", err)
						go r.finishRebalance(err)
						return
					}
					r.globalTopology = globalTopology
					l.Infof("Rebalancer::initRebalAsync Global Topology %v", globalTopology)

					onEjectOnly := cfg["rebalance.node_eject_only"].Bool()
					optimizePlacement := cfg["settings.rebalance.redistribute_indexes"].Bool()
					disableReplicaRepair := cfg["rebalance.disable_replica_repair"].Bool()
					timeout := cfg["planner.timeout"].Int()
					threshold := cfg["planner.variationThreshold"].Float64()
					cpuProfile := cfg["planner.cpuProfile"].Bool()
					minIterPerTemp := cfg["planner.internal.minIterPerTemp"].Int()
					maxIterPerTemp := cfg["planner.internal.maxIterPerTemp"].Int()

					//user setting redistribute_indexes overrides the internal setting
					//onEjectOnly. onEjectOnly is not expected to be used in production
					//as this is not documented.
					if optimizePlacement {
						onEjectOnly = false
					} else {
						onEjectOnly = true
					}

					start := time.Now()
					r.transferTokens, hostToIndexToRemove, err = planner.ExecuteRebalance(cfg["clusterAddr"].String(), *r.topologyChange,
						r.nodeUUID, onEjectOnly, disableReplicaRepair, threshold, timeout, cpuProfile,
						minIterPerTemp, maxIterPerTemp)
					if err != nil {
						l.Errorf("Rebalancer::initRebalAsync Planner Error %v", err)
						go r.finishRebalance(err)
						return
					}
					if len(hostToIndexToRemove) > 0 {
						r.removeDupIndex = true
						go r.RemoveDuplicateIndexes(hostToIndexToRemove)
					}
					if len(r.transferTokens) == 0 {
						r.transferTokens = nil
					}

					elapsed := time.Since(start)
					l.Infof("Rebalancer::initRebalAsync Planner Time Taken %v", elapsed)
					break loop
				}
			}
			l.Errorf("Rebalancer::initRebalAsync All Indexers Not Active. Waiting...")
			time.Sleep(5 * time.Second)
		}
	}

	go r.doRebalance()
}

// Cancel cancels a currently running rebalance or failover and waits
// for its go routines to finish.
func (r *Rebalancer) Cancel() {
	l.Infof("Rebalancer::Cancel Exiting")

	r.cancelMetakv()

	close(r.cancel)
	r.wg.Wait()
}

func (r *Rebalancer) finishRebalance(err error) {
	if err == nil && r.master && r.topologyChange != nil {
		// Note that this function tansfers the ownership of only those
		// tokens, which are not owned by keep nodes. Ownership of other
		// tokens remains unchanged.
		keepNodes := make(map[string]bool)
		for _, node := range r.topologyChange.KeepNodes {
			keepNodes[string(node.NodeInfo.NodeID)] = true
		}

		// Suppress error if any. The background task to handle failover
		// will do necessary retry for transferring ownership.
		cfg := r.config.Load()
		_ = transferScheduleTokens(keepNodes, cfg["clusterAddr"].String())
	}
	r.retErr = err
	r.cleanupOnce.Do(r.doFinish)
}

func (r *Rebalancer) doFinish() {
	l.Infof("Rebalancer::doFinish Cleanup %v", r.retErr)
	atomic.StoreInt32(&r.isDone, 1)
	close(r.done)
	r.cancelMetakv()

	r.wg.Wait()
	r.cb.done(r.retErr, r.cancel)
}

func (r *Rebalancer) isFinish() bool {
	return atomic.LoadInt32(&r.isDone) == 1
}

// cancelMetakv closes the metakvCancel channel, thus terminating metakv's
// transfer token callback loop that was initiated by metakv.RunObserveChildren.
// Must be done when rebalance finishes (successful or not).
func (r *Rebalancer) cancelMetakv() {

	r.muCleanup.Lock()
	defer r.muCleanup.Unlock()

	if r.metakvCancel != nil {
		close(r.metakvCancel)
		r.metakvCancel = nil
	}

}

// addToWaitGroup adds caller to r.wg waitgroup and returns true if metakv.RunObserveChildren
// callbacks are active, else it does nothing and returns false.
func (r *Rebalancer) addToWaitGroup() bool {

	r.muCleanup.Lock()
	defer r.muCleanup.Unlock()

	if r.metakvCancel != nil {
		r.wg.Add(1)
		return true
	} else {
		return false
	}

}

// doRebalance runs in the rebalance master as a go routine helper to initRebalanceAsync
// that manages the fine-grained steps of a rebalance. It creates the transfer token
// batches and publishes the first one. (If more than one batch exists the others will
// be published later by processTokenAsMaster.) Then it launches an observeRebalance go
// routine that does the real rebalance work for the master.
func (r *Rebalancer) doRebalance() {

	if r.master && r.runPlanner && r.removeDupIndex {
		<-r.dropIndexDone // wait for duplicate index removal to finish
	}
	if r.transferTokens != nil {
		if ddl, err := r.checkDDLRunning(); ddl {
			r.finishRebalance(err)
			return
		}

		select {
		case <-r.cancel:
			l.Infof("Rebalancer::doRebalance Cancel Received. Skip Publishing Tokens.")
			return

		default:
			// Start the rebalance master work
			r.publishDeferredTokens() // won't be built so these can all go at once
			r.publishTransferTokenBatch(true)
			close(r.waitForTokenPublish)
			go r.observeRebalance()
		}
	} else {
		r.cb.progress(1.0, r.cancel)
		r.finishRebalance(nil)
		return
	}
}

// publishDeferredTokens publishes all transfer tokens that are for user-deferred index builds.
// Rebalance will only move the metadata but not build these, so they can all be published at
// once with negligible load generated, and there is no need to include them in the Smart
// Batching computations.
func (r *Rebalancer) publishDeferredTokens() {
	const method string = "Rebalancer::publishDeferredTokens:" // for logging

	r.mu.Lock()
	defer r.mu.Unlock()
	var publishedIds strings.Builder
	published := 0
	for ttid, tt := range r.transferTokens {
		if tt.IsUserDeferred() { // rebalance will not build this index
			setTransferTokenInMetakv(ttid, r.transferTokens[ttid])
			fmt.Fprintf(&publishedIds, " %v", ttid)
			published++
		}
	}
	if published > 0 {
		l.Infof("%v Published %v deferred tokens: %v", method, published, publishedIds.String())
	} else {
		l.Infof("%v No deferred tokens to publish", method)
	}
}

// mapTransferTokensByDestId populates r.toBuildTTsByDestId from r.transferTokens
// and also initializes r.buildingTTsByDestId to an empty map. r.toBuildTTsByDestId only
// includes transfer tokens that will need building (i.e. omits deferred).
func (r *Rebalancer) mapTransferTokensByDestId() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.toBuildTTsByDestId = make(map[string]map[string]*c.TransferToken)
	r.buildingTTsByDestId = make(map[string]map[string]*c.TransferToken)
	for ttid, tt := range r.transferTokens {
		if tt.IsUserDeferred() { // rebalance will not build this index
			continue
		}
		destId := tt.DestId
		destIdMap := r.toBuildTTsByDestId[destId]
		if destIdMap == nil {
			destIdMap = make(map[string]*c.TransferToken)
			r.toBuildTTsByDestId[destId] = destIdMap
		}
		destIdMap[ttid] = tt
	}
}

// numBuildingTokensLOCKED returns the number of r.buildingTTsByDestId that are still building.
// It also deletes those that are finished from this map tree.
// Caller must be holding r.mu write locked.
func (r *Rebalancer) numBuildingTokensLOCKED() (building int) {
	for _, buildingTTs := range r.buildingTTsByDestId {
		for ttid, tt := range buildingTTs {
			if tt.State == c.TransferTokenDeleted {
				delete(buildingTTs, ttid)
				continue
			}
			building++
		}
	}
	return building
}

// numToBuildTokensLOCKED returns the number of tokens still in the r.toBuildTTsByDestId map
// tree. These are the ones that will need building but have not yet been submitted for builds.
// Caller must be holding r.mu at least read locked.
func (r *Rebalancer) numToBuildTokensLOCKED() (toBuild int) {
	for _, toBuildTTs := range r.toBuildTTsByDestId {
		toBuild += len(toBuildTTs)
	}
	return toBuild
}

// impliedStreams returns a set of stream keys for the DCP init streams implied by the
// transfer tokens in ttMap.
func impliedStreams(ttMap map[string]*common.TransferToken) (impliedStreams map[string]struct{}) {
	impliedStreams = make(map[string]struct{})
	for _, tt := range ttMap {
		impliedStreams[streamKeyFromTT(tt)] = struct{}{}
	}
	return impliedStreams
}

// publishTransferTokenBatch is called by the master to publish the first or next batch of
// transfer tokens into metakv so they will start being processed. The batches are generated
// dynamically based on unfinished builds and concurrency constraints. An entire batch does
// not need to complete before an additional batch is published. This moves token references
// from r.toBuildTTsByDestId to r.buildingTTsByDestId. If there are no pending tokens left this will
// not publish anything, which is okay because caller processTokenAsMaster detects rebalance
// complete independently based on all r.TransferTokens reaching TransferTokenDeleted state.
func (r *Rebalancer) publishTransferTokenBatch(first bool) {

	if first {
		r.mapTransferTokensByDestId() // init r.toBuildTTsByDestId and r.buildingTTsByDestId
		r.initNodeLoads()
	}
	r.getNodeIndexerStats(r.config.Load()) // will only fire if first call or enough time has passed

	// Support batchSize being changed dynamically during a rebalance. It now denotes the total
	// number of builds (index moves) allowed in progress concurrently, not the number to start.
	cfg := r.config.Load()
	batchSize := cfg["rebalance.transferBatchSize"].Int() // max concurrent builds for cluster
	if batchSize == 0 {                                   // unlimited
		batchSize = math.MaxInt32
	} else if batchSize < 0 {
		batchSize = 1
	}

	r.mu.Lock()
	startTokens := r.createBatchLOCKED(batchSize)
	var publishedIds strings.Builder
	for ttid, tt := range startTokens {
		setTransferTokenInMetakv(ttid, tt)
		fmt.Fprintf(&publishedIds, " %v", ttid)
	}
	r.mu.Unlock()
	l.Infof("Rebalancer::publishTransferTokenBatch: first %v, published %v transfer tokens: %v",
		first, len(startTokens), publishedIds.String())
}

// createBatchLOCKED returns a map from ttid to token of the next set of tokens to publish. These
// will be added incrementally to the existing work, which may still be ongoing. batchSize is the
// maximum number of builds for the entire cluster, including those already running. The new
// returned set of tokens to publish will be at most batchSize minus the number already running,
// but fewer (including 0) will be returned if there are not that many to-build tokens remaining.
// Caller must be holding r.mu write locked.
func (r *Rebalancer) createBatchLOCKED(batchSize int) (toBuildTokens map[string]*c.TransferToken) {

	numBuilding := r.numBuildingTokensLOCKED() // # tokens published but not yet deleted (100% done)

	// If there are pre-7.1.0 nodes we cannot pipeline the work due to MB-48191
	if r.clusterVersion < common.INDEXER_71_VERSION && numBuilding > 0 {
		return nil
	}
	numToSelect := batchSize - numBuilding // max number of builds to start = open build "slots"
	if numToSelect <= 0 {
		return nil
	}
	numRemaining := r.numToBuildTokensLOCKED() // # tokens still to be built
	if numRemaining == 0 {
		return nil
	}

	// Only publish the next batch if there are enough open build slots for a reasonable chance
	// of stream sharing or if it's an edge case where we must publish to make any progress
	if numToSelect >= numRemaining || (batchSize >= 8 && numToSelect >= batchSize/4) ||
		(batchSize <= 7 && numToSelect >= 2) || batchSize == 1 {

		// Select a "smart" subset of pending tokens to spread work over index nodes and
		// maximize the possibility of stream sharing
		toBuildTokens = r.selectSmartToBuildTokensLOCKED(batchSize, numToSelect)
	}
	return toBuildTokens
}

// selectSmartToBuildTokensLOCKED is the core of the "Smart Batching for Rebalance" feature. It
// returns a map from ttid to token of up to numToSelect tokens still in the r.toBuildTTsByDestId
// map tree (i.e. TTs that still need to be built) selected to both spread the work around Index
// nodes and to maximize the possibility of stream sharing. There are guaranteed to be at least
// numToSelect candidate tokens. batchSize is the maximum number of builds for the entire cluster,
// including those already running, used to compute the maximum builds allowed to run per node.
// Caller must be holding r.mu write locked.
func (r *Rebalancer) selectSmartToBuildTokensLOCKED(batchSize int, numToSelect int) (selectedTokens map[string]*c.TransferToken) {
	selectedTokens = make(map[string]*c.TransferToken)

	numIndexNodes := len(r.topologyChange.KeepNodes) // total index nodes remaining
	maxPerNode := batchSize / numIndexNodes          // max concurrent builds per node
	if maxPerNode < 3 {                              // legacy transferBatchSize default (could all be on one node)
		maxPerNode = 3
	}
	if maxPerNode > batchSize {
		maxPerNode = batchSize
	}

	// Sort the nodes ascending by current load. (Note that nodeLoads also includes nodes
	// being ejected, but there will not be any transfer tokens with them as DestId.)
	sort.Sort(r.nodeLoads)

	cfg := r.config.Load()
	maxStreamsPerNode := cfg["max_parallel_collection_builds"].Int() // indexer will error if builds need more than this

	// Calculate number of tokens we could select for each node (not considering numToSelect overall limit)
	// and the sets of already-building streams for each, which we cannot reuse due to indexer limitations.
	// Also initialize the set of new streams for each node and find index of first high-load node.
	numNodes := len(r.nodeLoads)
	goalsForNodes := make([]int, numNodes) // decremented when a token is selected, as is numToSelect
	forbiddenStreamsForNodes := make([]map[string]struct{}, numNodes)
	newStreamsForNodes := make([]map[string]struct{}, numNodes)
	firstHighLoadNode := 0
	for node, nodeLoad := range r.nodeLoads {
		buildingTTsForNode := r.buildingTTsByDestId[nodeLoad.nodeUUID]
		goalsForNodes[node] = maxPerNode - len(buildingTTsForNode)
		forbiddenStreamsForNodes[node] = impliedStreams(buildingTTsForNode)
		if nodeLoad.getLoad() == 0 { // low load
			firstHighLoadNode++
		}
	}

	// Low-load nodes are at the front and we fully load them up with builds one by one
	for node := 0; node < firstHighLoadNode && numToSelect > 0; node++ {
		nodeUUID := r.nodeLoads[node].nodeUUID
		toBuildTTsForNode := r.toBuildTTsByDestId[nodeUUID]          // tokens (by ttid) available to select for node
		selectedTTsForNode := make(map[string]*common.TransferToken) // tokens (by ttid) selected for node

		for numToSelect > 0 && goalsForNodes[node] > 0 {
			ttid, tt := r.selectSmartToBuildTokenLOCKED(toBuildTTsForNode, selectedTTsForNode,
				forbiddenStreamsForNodes[node], newStreamsForNodes[node], maxStreamsPerNode)
			if tt != nil {
				numToSelect--
				goalsForNodes[node]--
				selectedTokens[ttid] = tt
				selectedTTsForNode[ttid] = tt

				newStreamsForNode := newStreamsForNodes[node]
				if newStreamsForNode == nil {
					newStreamsForNode = make(map[string]struct{})
					newStreamsForNodes[node] = newStreamsForNode
				}
				newStreamsForNode[streamKeyFromTT(tt)] = struct{}{}

				r.moveTokenToBuildingLOCKED(ttid, tt)
			} else {
				break // no legally selectable token for this node was found
			}
		} // for next selection on current node
	} // for each low-load destination node

	// High-load nodes are at the back and we assign builds to them round-robin
	var selectedTTsForNodes map[string]map[string]*common.TransferToken // tts selected per hi-load node by [DestId][ttid]
	if numToSelect > 0 {
		selectedTTsForNodes = make(map[string]map[string]*common.TransferToken)
	}
	priorNumToSelect := math.MaxInt32                       // to terminate outer loop when no more progress made
	for numToSelect > 0 && numToSelect < priorNumToSelect { // for next full round robin cycle
		priorNumToSelect = numToSelect

		// Round robin once over each high-load dest node
		for node := firstHighLoadNode; node < numNodes && numToSelect > 0 && goalsForNodes[node] > 0; node++ {
			nodeUUID := r.nodeLoads[node].nodeUUID
			ttid, tt := r.selectSmartToBuildTokenLOCKED(r.toBuildTTsByDestId[nodeUUID],
				selectedTTsForNodes[nodeUUID], forbiddenStreamsForNodes[node],
				newStreamsForNodes[node], maxStreamsPerNode)
			if tt != nil {
				numToSelect--
				goalsForNodes[node]--
				selectedTokens[ttid] = tt

				selectedTTsForNode := selectedTTsForNodes[nodeUUID]
				if selectedTTsForNode == nil {
					selectedTTsForNode = make(map[string]*common.TransferToken)
					selectedTTsForNodes[nodeUUID] = selectedTTsForNode
				}
				selectedTTsForNode[ttid] = tt

				newStreamsForNode := newStreamsForNodes[node]
				if newStreamsForNode == nil {
					newStreamsForNode = make(map[string]struct{})
					newStreamsForNodes[node] = newStreamsForNode
				}
				newStreamsForNode[streamKeyFromTT(tt)] = struct{}{}

				r.moveTokenToBuildingLOCKED(ttid, tt)
			}
		} // round robin once over each high-load dest node
	} // for next full round robin cycle

	return selectedTokens
}

// selectSmartToBuildTokenLOCKED attempts to select one token to build from those available to build
// for a node, in the following priority order (Smart Batching for Rebalance feature):
//   1. Replica repairs (stream-sharing preferred) - reduce scan load on other replicas, smoothing
//      load across nodes
//   2. Partitions of same index - share a stream and all have identical keys
//   3. Indexes on same collection - share a stream but have different keys, adding fields Projector
//      must forward
//   4. Everything else
// A selected token must satisfy the constraints that it cannot need a DCP init stream that indexer
// already has running for that node (as indexer supports only one copy of a stream at a time) nor
// push the total number of streams for that node above the maximum allowed.
//
// Return: ID and ptr to selected token iff any legally selectable token is found, else "", nil
// Arguments:
//   toBuildTTsForNode: tokens by [ttid] available to select for the node
//   selectedTTsForNode: tokens by [ttid] already selected for the node
//   forbiddenStreamsForNode: set of keys of streams already running on the node for prior builds
//   newStreamsForNode: set of keys of streams to be started on the node for already-selected tokens
//   maxStreamsPerNode: limit on number of streams the node is allowed to run concurrently
// Caller must be holding r.mu at least read locked.
func (r *Rebalancer) selectSmartToBuildTokenLOCKED(toBuildTTsForNode, selectedTTsForNode map[string]*common.TransferToken,
	forbiddenStreamsForNode, newStreamsForNode map[string]struct{}, maxStreamsPerNode int) (ttid string, tt *common.TransferToken) {

	// 1. Replica repairs (stream-sharing preferred)
	ttid, tt = r.findReplicaRepairTokenLOCKED(toBuildTTsForNode, selectedTTsForNode)
	if tt != nil && tokenOkayToBuild(tt, forbiddenStreamsForNode, newStreamsForNode, maxStreamsPerNode) {
		return ttid, tt
	}

	// 2. Partitions of same index
	ttid, tt = r.findSamePartitionLOCKED(toBuildTTsForNode, selectedTTsForNode)
	if tt != nil && tokenOkayToBuild(tt, forbiddenStreamsForNode, newStreamsForNode, maxStreamsPerNode) {
		return ttid, tt
	}

	// 3. Indexes on same collection
	ttid, tt = r.findSameCollectionLOCKED(toBuildTTsForNode, selectedTTsForNode)
	if tt != nil && tokenOkayToBuild(tt, forbiddenStreamsForNode, newStreamsForNode, maxStreamsPerNode) {
		return ttid, tt
	}

	// 4. Everything else; since no prior selection succeeded, any token selected here will
	//    require an additional stream on the node
	tt = nil
	for ttid, tt = range toBuildTTsForNode { // get any toBuild token for this node
		break
	}
	if tt != nil && tokenOkayToBuild(tt, forbiddenStreamsForNode, newStreamsForNode, maxStreamsPerNode) {
		return ttid, tt
	}

	return "", nil
}

// tokenOkayToBuild determines whether starting a build for transfer token tt is okay given the
// constraints that only one DCP init stream at a time can run for a given collection and there
// is also a cap on number of init streams allowed to run concurrently.
func tokenOkayToBuild(tt *common.TransferToken,
	forbiddenStreamsForNode, newStreamsForNode map[string]struct{}, maxStreamsPerNode int) bool {

	// Can't build if it requires a forbidden stream (one that is already running in indexer)
	streamKey := streamKeyFromTT(tt)
	if _, member := forbiddenStreamsForNode[streamKey]; member {
		return false
	}

	totalStreams := len(forbiddenStreamsForNode) + len(newStreamsForNode)
	if _, member := newStreamsForNode[streamKey]; !member { // needs a new stream
		totalStreams++
	}
	if totalStreams > maxStreamsPerNode {
		return false
	}

	return true
}

// NodeLoad holds stats about each Index node retained by a rebal, used to estimate current load.
type NodeLoad struct {
	nodeUUID string // UUID of the Index node

	// Indexer stats info from nodes
	avgResidentPercent int64  // used for Plasma
	memoryQuota        int64  // used for MOI
	memoryUsed         int64  // used for MOI
	storageMode        string // common.MemoryOptimized or common.PlasmaDB; "" if never retrieved
}

// NewNodeLoad is the constructor for the NodeLoad class. It initializes the stats to high load.
// These will be updated by any future successful stats retrievals. storageMode is unknown until
// stats are retrieved.
func NewNodeLoad(nodeUUID string) *NodeLoad {
	nodeLoad := &NodeLoad{
		nodeUUID: nodeUUID,
	}
	nodeLoad.setStatsHighLoad()
	return nodeLoad
}

// NodeLoad.getLoad returns 0 or 1 for nodes deemed to have low or high loads, respectively.
func (nl *NodeLoad) getLoad() int {
	switch nl.storageMode {
	case common.PlasmaDB:
		if nl.avgResidentPercent >= 50 {
			return 0
		}
		return 1
	case common.MemoryOptimized:
		if nl.memoryQuota > 0 && nl.memoryUsed <= nl.memoryQuota/2 {
			return 0
		}
		return 1
	default:
		return 1 // stats never successfully retrieved; safer to assume high load
	}
}

// NodeLoad.addStats sets the passed-in stats that match NodeLoad fields into those fields.
func (nl *NodeLoad) addStats(stats *common.Statistics) {
	// int64s get changed to float64s in transit by Go's default JSON unmarshal rules for
	// numeric fields that were typed as interface{}
	if value := stats.Get("avg_resident_percent"); value != nil {
		nl.avgResidentPercent = int64(value.(float64))
	}
	if value := stats.Get("memory_quota"); value != nil {
		nl.memoryQuota = int64(value.(float64))
	}
	if value := stats.Get("memory_used"); value != nil {
		nl.memoryUsed = int64(value.(float64))
	}
	if value := stats.Get("storage_mode"); value != nil {
		nl.storageMode = value.(string)
	}
}

// NodeLoad.setStatsHighLoad sets fake stats in the receiver indicating the node is highly loaded.
func (nl *NodeLoad) setStatsHighLoad() {
	nl.avgResidentPercent = 0
	nl.memoryQuota = 1
	nl.memoryUsed = 1 // used same as quota implies 100% of memory used
}

// NodeLoadSlice implements sort.Interface to enable sorting by load (which is really just
// two categories: low and high load).
type NodeLoadSlice []*NodeLoad

// NodeLoadSlice.Len is a sort.Interface method returning the number of elements in the slice.
func (nls NodeLoadSlice) Len() int {
	return len(nls)
}

// NodeLoadSlice.Less is a sort.Interface method that returns whether slice entry i should sort before entry j.
func (nls NodeLoadSlice) Less(i, j int) bool {
	return nls[i].getLoad() < nls[j].getLoad()
}

// NodeLoadSlice.Swap is a sort.Interface method that swaps two elements in the slice.
func (nls NodeLoadSlice) Swap(i, j int) {
	nls[i], nls[j] = nls[j], nls[i]
}

// initNodeLoads initializes r.nodeLoads with an entry for each Index node containing its nodeUUID
// and number of indexes determined from r.globalTopology. NO-OP if globalTopology == nil.
func (r *Rebalancer) initNodeLoads() {
	var nodeLoads NodeLoadSlice

	if r.globalTopology == nil {
		return
	}

	for _, metadata := range r.globalTopology.Metadata {
		nodeLoad := NewNodeLoad(metadata.NodeUUID)
		nodeLoads = append(nodeLoads, nodeLoad)
	}
	r.mu.Lock()
	r.nodeLoads = nodeLoads
	r.mu.Unlock()
}

// getOrCreateNodeLoadLOCKED searches r.nodeLoads for the entry for nodeUUID, returning
// it if found, else creates a new one, adds it to r.nodeLoads, and returns it.
// Caller must be holding r.mu write locked.
func (r *Rebalancer) getOrCreateNodeLoadLOCKED(nodeUUID string) (nodeLoad *NodeLoad) {
	// Find the existing entry for this node
	for _, nodeLoad = range r.nodeLoads {
		if nodeLoad.nodeUUID == nodeUUID {
			return nodeLoad
		}
	}

	// If existing node entry not found, add an entry for it
	nodeLoad = NewNodeLoad(nodeUUID)
	r.nodeLoads = append(r.nodeLoads, nodeLoad)
	return nodeLoad
}

// setNodeUUIDStatsHighLoadLOCKED sets fake stats for nodeUUID's entry in r.nodeLoads
// indicating the node is highly loaded (used when we could not get stats from the node).
// Caller must be holding r.mu write locked.
func (r *Rebalancer) setNodeUUIDStatsHighLoadLOCKED(nodeUUID string) {
	nodeLoad := r.getOrCreateNodeLoadLOCKED(nodeUUID)
	nodeLoad.setStatsHighLoad()
}

// getNodeIndexerStats gets a subset of indexer stats from each Index node via REST and
// adds this information to r.nodeLoads, which should already have an entry for every node.
// The stats are used to decide whether a node has high or low current load. The stats
// gathering is only done if enough time has passed since the last gather. Failures are
// worked around by setting fake stats indicating unreachable nodes are highly loaded.
func (r *Rebalancer) getNodeIndexerStats(config common.Config) {

	doGetStats := false // get the stats in this call?
	now := time.Now()
	r.mu.Lock() // for r.nodeLoadsTime
	if now.Sub(r.nodeLoadsTime) >= 1*time.Minute {
		r.nodeLoadsTime = now // don't try again for the wait period even if some/all fail
		doGetStats = true
	}
	r.mu.Unlock()

	if doGetStats {
		// Parallel REST call to all index nodes
		statsMap, errMap, err := common.GetIndexStats(config, "smartBatching", 3)

		r.mu.Lock() // for r.nodeLoads, including in children
		defer r.mu.Unlock()

		if err != nil {
			// Could not get stats for some nodes, so set fake stats for them showing highly loaded.
			// We used a short REST timeout so slow nodes do not drag down speed of rebalance. It is
			// reasonable to treat such nodes as highly loaded.
			if errMap != nil { // specific nodes failed
				for nodeUUID := range errMap {
					r.setNodeUUIDStatsHighLoadLOCKED(nodeUUID)
				}
			} else { // failure prevented any nodes from being called
				for _, nodeLoad := range r.nodeLoads {
					nodeLoad.setStatsHighLoad()
				}
			}
		}

		// Copy in the new stats for nodes that succeeded
		for nodeUUID, stats := range statsMap {
			nodeLoad := r.getOrCreateNodeLoadLOCKED(nodeUUID)
			nodeLoad.addStats(stats)
		}
	}
}

// moveTokenToBuildingLOCKED moves a token from the r.toBuildTTsByDestId map tree
// to the r.buildingTTsByDestId map tree.
// Caller must be holding r.mu write locked.
func (r *Rebalancer) moveTokenToBuildingLOCKED(ttid string, tt *common.TransferToken) {
	destId := tt.DestId
	buildingMap := r.buildingTTsByDestId[destId]
	if buildingMap == nil {
		buildingMap = make(map[string]*common.TransferToken)
		r.buildingTTsByDestId[destId] = buildingMap
	}
	buildingMap[ttid] = tt
	delete(r.toBuildTTsByDestId[destId], ttid)
}

// streamKeyFromTT returns a key for the init stream indexer will use for the given transfer token. This is for
// local tracking only so does not need to use just the bucket if it is in default scope and collection.
func streamKeyFromTT(tt *common.TransferToken) string {
	defn := tt.IndexInst.Defn
	return strings.Join([]string{defn.Bucket, defn.Scope, defn.Collection}, ":")
}

// findReplicaRepairTokenLOCKED searches toBuildTTsForNode for a replica repair token, preferring those that
// would share a stream with a token in selectedTTsForNode, and returns the (ttid, tt) of one if found.
// Caller must be holding r.mu at least read locked.
func (r *Rebalancer) findReplicaRepairTokenLOCKED(
	toBuildTTsForNode, selectedTTsForNode map[string]*common.TransferToken) (string, *common.TransferToken) {

	// Non-stream-sharing replica repair token and ID, if found
	var rrtt *common.TransferToken
	var rrttid string

	streamsForNode := impliedStreams(selectedTTsForNode) // streams already needed
	for ttid, tt := range toBuildTTsForNode {
		if tt.TransferMode == common.TokenTransferModeCopy { // replica repair
			if _, member := streamsForNode[streamKeyFromTT(tt)]; member { // will share an existing stream
				return ttid, tt
			} else if rrtt == nil { // remember first non-stream-sharing RR token
				rrttid = ttid
				rrtt = tt
			}
		}
	}
	return rrttid, rrtt // == "", nil if no RR token found
}

// findSamePartitionLOCKED looks for a token in toBuildTTsForNode that is another partition of a
// partitioned index that already has a token in selectedTTsForNode and returns its (ttid, tt) if found.
// Caller must be holding r.mu at least read locked.
func (r *Rebalancer) findSamePartitionLOCKED(toBuildTTsForNode, selectedTTsForNode map[string]*common.TransferToken) (string, *common.TransferToken) {

	// Get the instIds and realInstIds of all tokens in selectedTTsForNode
	instIds := make(map[common.IndexInstId]struct{})
	realInstIds := make(map[common.IndexInstId]struct{})
	for _, tt := range selectedTTsForNode {
		instIds[tt.InstId] = struct{}{}
		realInstIds[tt.RealInstId] = struct{}{}
	}

	// Search for a match in toBuildTTsForNode
	for ttid, tt := range toBuildTTsForNode {
		if _, member := realInstIds[tt.RealInstId]; member {
			return ttid, tt
		}
		if _, member := realInstIds[tt.InstId]; member {
			return ttid, tt
		}
		if _, member := instIds[tt.RealInstId]; member {
			return ttid, tt
		}
	}
	return "", nil
}

// findSameCollectionLOCKED looks for a token in toBuildTTsForNode that is for the same collection
// as an index that already has a token in selectedTTsForNode and returns its (ttid, tt) if found.
// Caller must be holding r.mu at least read locked.
func (r *Rebalancer) findSameCollectionLOCKED(toBuildTTsForNode, selectedTTsForNode map[string]*common.TransferToken) (string, *common.TransferToken) {

	// Get the collections of all index tokens in selectedTTsForNode
	collections := make(map[string]struct{})
	for _, tt := range selectedTTsForNode {
		collections[streamKeyFromTT(tt)] = struct{}{}
	}

	// Search for a match in toBuildTTsForNode
	for ttid, tt := range toBuildTTsForNode {
		if _, member := collections[streamKeyFromTT(tt)]; member {
			return ttid, tt
		}
	}
	return "", nil
}

// observeRebalance runs as a go routine on both master and worker nodes of a rebalance.
// It registers the processTokens function as a callback in metakv on the rebalance
// transfer token (TT) directory. Metakv will call the callback function on each TT and
// on each mutation of a TT until an error occurs or its stop channel is closed. These
// callbacks trigger the individual index movement steps of the rebalance.
func (r *Rebalancer) observeRebalance() {
	l.Infof("Rebalancer::observeRebalance %v master:%v", r.rebalToken, r.master)

	<-r.waitForTokenPublish

	err := metakv.RunObserveChildrenV2(RebalanceMetakvDir, r.processTokens, r.metakvCancel)
	if err != nil {
		l.Infof("Rebalancer::observeRebalance Exiting On Metakv Error %v", err)
		r.finishRebalance(err)
	}
	l.Infof("Rebalancer::observeRebalance exiting err %v", err)
}

// processTokens is the callback registered on the metakv transfer token directory for
// a rebalance and gets called by metakv for each token and each change to a token.
// This decodes the token and hands it off to processTransferToken.
func (r *Rebalancer) processTokens(kve metakv.KVEntry) error {
	const method string = "Rebalancer::processTokens:" // for logging

	if kve.Path == RebalanceTokenPath || kve.Path == MoveIndexTokenPath {
		l.Infof("%v RebalanceToken %v %v", method, kve.Path, kve.Value)
		if kve.Value == nil {
			l.Infof("%v Rebalance Token Deleted. Mark Done.", method)
			r.cancelMetakv()
			r.finishRebalance(nil)
		}
	} else if strings.Contains(kve.Path, TransferTokenTag) {
		if kve.Value != nil {
			ttid, tt, err := r.decodeTransferToken(kve.Path, kve.Value)
			if err != nil {
				l.Errorf("%v Unable to decode transfer token. Ignored.", method)
				return nil
			}
			r.processTransferToken(ttid, tt)
		} else {
			l.Infof("%v Received empty or deleted transfer token %v", method, kve.Path)
		}

		// In a cluster with down-level nodes, we cannot overlap builds/merges with drops/prunes as
		// it can trigger bug MB-48191 in those nodes which existed until version 7.1.0. This dance
		// suffers from ordering and timing interactions between token state changes, queuing drops,
		// and publishing more tokens, making it extremely hard to queue drops or publish tokens in
		// all the places in code this might need to occur to avoid a publish attempt being blocked
		// and never occurring again, triggering a rebalance hang. Thus the code block below is used
		// instead to brute-force solve this problem by attempting to queue and publish after every
		// transfer token state change. These are idempotent, so safe. It just burns a little more
		// CPU to check these so often during rebalance on a cluster with down-level nodes.
		if r.clusterVersion < common.INDEXER_71_VERSION {
			r.maybeQueueDropIndexesForDownLevelCluster()
			if r.master {
				r.publishTransferTokenBatch(false)
			}
		}
	}
	return nil
}

// processTransferTokens performs the work needed from this node, if any, for a
// transfer token. The work is split into three helpers for work specific to
// 1) the master (non-movement bookkeeping, including publishing the next token
// batch when the prior one completes), 2) source of an index move, 3) destination
// of a move.
func (r *Rebalancer) processTransferToken(ttid string, tt *c.TransferToken) {

	if !r.addToWaitGroup() {
		return
	}

	defer r.wg.Done()

	if ddl, err := r.checkDDLRunning(); ddl {
		r.setTransferTokenError(ttid, tt, err.Error())
		return
	}

	// "processed" var ensures only the incoming token state gets processed by this
	// call, as metakv will call parent processTokens again for each TT state change.
	var processed bool
	if tt.MasterId == r.nodeUUID {
		processed = r.processTokenAsMaster(ttid, tt)
	}

	if tt.SourceId == r.nodeUUID && !processed {
		processed = r.processTokenAsSource(ttid, tt)
	}

	if tt.DestId == r.nodeUUID && !processed {
		processed = r.processTokenAsDest(ttid, tt)
	}
}

// processTokenAsSource performs the work of the source node of an index move
// reflected by the transfer token, which is to queue up the source index drops
// for later processing. It handles only TT state TransferTokenReady. Returns
// true iff it is considered to have processed this token (including handling
// some error cases).
func (r *Rebalancer) processTokenAsSource(ttid string, tt *c.TransferToken) bool {
	const method string = "Rebalancer::processTokenAsSource:" // for logging

	if tt.RebalId != r.rebalToken.RebalId {
		l.Warnf("%v Found TransferToken with Unknown "+
			"RebalanceId. Local RId %v Token %v. Ignored.", method, r.rebalToken.RebalId, tt)
		return true
	}

	switch tt.State {

	case c.TransferTokenReady:
		if !r.checkValidNotifyStateSource(ttid, tt) {
			return true
		}

		r.mu.Lock()
		defer r.mu.Unlock()
		r.sourceTokens[ttid] = tt
		logging.Infof("%v Processing transfer token: %v", method, tt)

		// If there are pre-7.1.0 nodes we cannot pipeline the work due to MB-48191
		if r.clusterVersion >= common.INDEXER_71_VERSION {
			//TODO batch this rather than one per index
			r.queueDropIndexLOCKED(ttid) // this is the ONLY place drops are queued in >= 7.1.0 cluster
		}

	default:
		return false
	}
	return true
}

// processDropIndexQueue runs in a goroutine on all nodes of the rebalance.
func (r *Rebalancer) processDropIndexQueue() {

	notifych := make(chan bool, 2)

	first := true
	cfg := r.config.Load()
	waitTime := cfg["rebalance.drop_index.wait_time"].Int()

	for {
		select {
		case <-r.cancel:
			l.Infof("Rebalancer::processDropIndexQueue Cancel Received")
			return
		case <-r.done:
			l.Infof("Rebalancer::processDropIndexQueue Done Received")
			return
		case ttid := <-r.dropQueue:
			var tt c.TransferToken
			logging.Infof("Rebalancer::processDropIndexQueue processing drop index request for ttid: %v", ttid)
			if first {
				// If it is the first drop, let wait to give a chance for the target's metadata
				// being synchronized with the cbq nodes.  This is to ensure that the cbq nodes
				// can direct scan to the target nodes, before we start dropping the index in the source.
				time.Sleep(time.Duration(waitTime) * time.Second)
				first = false
			}

			r.mu.Lock()
			tt1, ok := r.sourceTokens[ttid]
			if ok {
				tt = *tt1
			}
			r.mu.Unlock()

			if !ok {
				l.Warnf("Rebalancer::processDropIndexQueue: Cannot find token %v in r.sourceTokens. Skip drop index.", ttid)
				continue
			}

			if tt.State == c.TransferTokenReady {
				if r.addToWaitGroup() {
					notifych <- true
					go r.dropIndexWhenIdle(ttid, &tt, notifych)
				} else {
					logging.Warnf("Rebalancer::processDropIndexQueue Skip processing drop index request for tt: %v as rebalancer can not add to wait group", tt)
				}
			} else {
				logging.Warnf("Rebalancer::processDropIndexQueue Skipping drop index request for tt: %v as state: %v != TransferTokenReady", tt, tt.State.String())
			}
		}
	}
}

// queueDropIndexLOCKED queues a ttid to be submitted for source index drop iff it has not already
// been queued in the past (since metakv can send duplicate notifications). The queuing mechanism
// submits these sequentially to indexer so each gets the full indexer.rebalance.httpTimeout,
// as drops can take a long time due to Projector stream bookkeeping.
// Caller must be holding r.mu write locked (for r.dropQueue, r.dropQueued).
// TODO Add a bulk drop index API to indexer so multiple drops can be sent at once as builds are.
func (r *Rebalancer) queueDropIndexLOCKED(ttid string) {
	const method string = "Rebalancer::queueDropIndexLOCKED:" // for logging

	select {
	case <-r.cancel:
		l.Warnf("%v Cannot drop index when rebalance being canceled.", method)
		return

	case <-r.done:
		l.Warnf("%v Cannot drop index when rebalance is done.", method)
		return

	default:
		indexName := r.sourceTokens[ttid].IndexInst.Defn.Name
		if _, member := r.dropQueued[ttid]; !member {
			r.dropQueued[ttid] = struct{}{}
			r.dropQueue <- ttid
			logging.Infof("%v Queued index %v for drop, ttid: %v", method, indexName, ttid)
		} else {
			logging.Warnf("%v Skipped index %v that was previously queued for drop, ttid: %v",
				method, indexName, ttid)
		}
	}
}

// maybeQueueDropIndexesForDownLevelCluster applies only to cluster levels < 7.1.0. This
// queues all the drops for source tokens on this node once all the builds for this node's currently
// received source AND dest tokens are done. This is to avoid overlapping builds/merges with drops/
// prunes to work around MB-48191. In these clusters, no new tokens are published until the prior
// batch completely finishes, which is the other half of avoiding build and drop overlaps.
func (r *Rebalancer) maybeQueueDropIndexesForDownLevelCluster() {
	const method string = "Rebalancer::maybeQueueDropIndexesForDownLevelCluster:" // for logging

	if r.clusterVersion < common.INDEXER_71_VERSION {
		r.mu.Lock()
		dropTokenIds := r.checkAllSourceIndexesReadyToDropLOCKED()
		for _, ttid := range dropTokenIds {
			r.queueDropIndexLOCKED(ttid)
		}
		r.mu.Unlock()
		logging.Infof("%v queued ttids for drop: %v", method, dropTokenIds)
	}
}

// isMissingBSC determines whether an error message is due to a bucket, scope,
// or collection not existing. These can be dropped after a TT referencing them
// was created, in which case we will set the TT forward to TransferTokenCommit
// state to abort the index move without failing the rebalance. In some cases
// the target error string will be contained inside brackets within a more
// detailed user-visible errMsg.
func isMissingBSC(errMsg string) bool {
	return errMsg == common.ErrCollectionNotFound.Error() ||
		errMsg == common.ErrScopeNotFound.Error() ||
		errMsg == common.ErrBucketNotFound.Error() ||
		strings.Contains(errMsg, "["+common.ErrCollectionNotFound.Error()+"]") ||
		strings.Contains(errMsg, "["+common.ErrScopeNotFound.Error()+"]") ||
		strings.Contains(errMsg, "["+common.ErrBucketNotFound.Error()+"]")
}

// isIndexNotFoundRebal checks whether a build error returned for rebalance is the
// special error ErrIndexNotFoundRebal indicating the key returned for this error
// is the originally submitted defnId instead of an instId because the metadata
// for defnId could not be found. This error should only be used for this purpose.
func isIndexNotFoundRebal(errMsg string) bool {
	return errMsg == common.ErrIndexNotFoundRebal.Error()
}

// dropIndexWhenIdle performs the source index drop asynchronously. This is the last real
// action of an index move during rebalance; the remainder are token bookkeeping operations.
// Changes the TT state to TransferTokenCommit when the drop is completed.
func (r *Rebalancer) dropIndexWhenIdle(ttid string, tt *c.TransferToken, notifych chan bool) {
	defer r.wg.Done()

	defer func() {
		if notifych != nil {
			// Blocking wait to ensure indexes are dropped sequentially
			<-notifych
		}
	}()

	missingStatRetry := 0
loop:
	for {
	labelselect:
		select {
		case <-r.cancel:
			l.Infof("Rebalancer::dropIndexWhenIdle Cancel Received")
			break loop
		case <-r.done:
			l.Infof("Rebalancer::dropIndexWhenIdle Done Received")
			break loop
		default:

			stats, err := getLocalStats(r.localaddr, true)
			if err != nil {
				l.Errorf("Rebalancer::dropIndexWhenIdle Error Fetching Local Stats %v %v", r.localaddr, err)
				break
			}

			statsMap := stats.ToMap()
			if statsMap == nil {
				l.Infof("Rebalancer::dropIndexWhenIdle Nil Stats. Retrying...")
				break
			}

			tt.IndexInst.Defn.SetCollectionDefaults()

			pending := float64(0)
			for _, partitionId := range tt.IndexInst.Defn.Partitions {

				defn := tt.IndexInst.Defn

				prefix := common.GetStatsPrefix(defn.Bucket, defn.Scope, defn.Collection,
					defn.Name, tt.IndexInst.ReplicaId, int(partitionId), true)

				sname_completed := common.GetIndexStatKey(prefix, "num_completed_requests")
				sname_requests := common.GetIndexStatKey(prefix, "num_requests")

				var num_completed, num_requests float64
				if _, ok := statsMap[sname_completed]; ok {
					num_completed = statsMap[sname_completed].(float64)
					num_requests = statsMap[sname_requests].(float64)
				} else {
					l.Infof("Rebalancer::dropIndexWhenIdle Missing Stats %v %v. Retrying...", sname_completed, sname_requests)
					missingStatRetry++
					if missingStatRetry > 10 {
						if r.needRetryForDrop(ttid, tt) {
							break labelselect
						} else {
							break loop
						}
					}
					break labelselect
				}
				pending += num_requests - num_completed
			}

			if pending > 0 {
				l.Infof("Rebalancer::dropIndexWhenIdle Index %v:%v:%v:%v Pending Scan %v", tt.IndexInst.Defn.Bucket, tt.IndexInst.Defn.Scope, tt.IndexInst.Defn.Collection, tt.IndexInst.Defn.Name, pending)
				break
			}

			defn := tt.IndexInst.Defn
			defn.InstId = tt.InstId
			defn.RealInstId = tt.RealInstId
			req := manager.IndexRequest{Index: defn}
			body, err := json.Marshal(&req)
			if err != nil {
				l.Errorf("Rebalancer::dropIndexWhenIdle Error marshal drop index %v", err)
				r.setTransferTokenError(ttid, tt, err.Error())
				return
			}

			bodybuf := bytes.NewBuffer(body)

			url := "/dropIndex"
			resp, err := postWithAuth(r.localaddr+url, "application/json", bodybuf)
			if err != nil {
				// Error from HTTP layer, not from index processing code
				l.Errorf("Rebalancer::dropIndexWhenIdle Error drop index on %v %v", r.localaddr+url, err)
				r.setTransferTokenError(ttid, tt, err.Error())
				return
			}

			response := new(manager.IndexResponse)
			if err := convertResponse(resp, response); err != nil {
				l.Errorf("Rebalancer::dropIndexWhenIdle Error unmarshal response %v %v", r.localaddr+url, err)
				r.setTransferTokenError(ttid, tt, err.Error())
				return
			}

			if response.Code == manager.RESP_ERROR {
				// Error from index processing code (e.g. the string from common.ErrCollectionNotFound)
				if !isMissingBSC(response.Error) {
					l.Errorf("Rebalancer::dropIndexWhenIdle Error dropping index %v %v", r.localaddr+url, response.Error)
					r.setTransferTokenError(ttid, tt, response.Error)
					return
				}
				// Ok: failed to drop source index because b/s/c was dropped. Continue to TransferTokenCommit state.
				l.Infof("Rebalancer::dropIndexWhenIdle Source index already dropped due to bucket/scope/collection dropped. tt %v.", tt)
			}
			tt.State = c.TransferTokenCommit
			setTransferTokenInMetakv(ttid, tt)

			r.mu.Lock()
			r.sourceTokens[ttid] = tt
			r.mu.Unlock()

			break loop
		}
		time.Sleep(5 * time.Second)
	}
}

// needRetryForDrop is called after multiple unsuccessful attempts to get the stats for an index to be
// dropped. It determines whether we should keep retrying or assume it was already dropped. It is only
// assumed already to be dropped if there is no entry for it in the local metadata.
func (r *Rebalancer) needRetryForDrop(ttid string, tt *c.TransferToken) bool {

	localMeta, err := getLocalMeta(r.localaddr)
	if err != nil {
		l.Errorf("Rebalancer::dropIndexWhenIdle Error Fetching Local Meta %v %v", r.localaddr, err)
		return true
	}
	indexState, errStr := getIndexStatusFromMeta(tt, localMeta)
	if errStr != "" {
		l.Errorf("Rebalancer::dropIndexWhenIdle Error Fetching Index Status %v %v", r.localaddr, errStr)
		return true
	}

	if indexState == c.INDEX_STATE_NIL {
		//if index cannot be found in metadata, most likely its drop has already succeeded.
		//instead of waiting indefinitely, it is better to assume success and proceed.
		l.Infof("Rebalancer::dropIndexWhenIdle Missing Metadata for %v. Assume success and abort retry", tt.IndexInst)
		tt.State = c.TransferTokenCommit
		setTransferTokenInMetakv(ttid, tt)

		r.mu.Lock()
		r.sourceTokens[ttid] = tt
		r.mu.Unlock()

		return false
	}

	return true
}

// processTokenAsDest performs the work of the destination node of an index
// move reflected by the transfer token. It directly handles TT states
// TransferTokenCreated and TransferTokenInitiate, and indirectly (via token
// destTokenToMergeOrReadyLOCKED call here or in buildAcceptedIndexes --> waitForIndexBuild)
// states TransferTokenInProgress and TransferTokenMerge. Returns true iff it is
// considered to have processed this token (including handling some error cases).
func (r *Rebalancer) processTokenAsDest(ttid string, tt *c.TransferToken) bool {
	const method string = "Rebalancer::processTokenAsDest:" // for logging

	if tt.RebalId != r.rebalToken.RebalId {
		l.Warnf("%v Found TransferToken with Unknown "+
			"RebalanceId. Local RId %v Token %v. Ignored.", method, r.rebalToken.RebalId, tt)
		return true
	}

	if !r.checkValidNotifyStateDest(ttid, tt) {
		return true
	}

	switch tt.State {
	case c.TransferTokenCreated:

		indexDefn := tt.IndexInst.Defn
		indexDefn.SetCollectionDefaults()

		indexDefn.Nodes = nil
		indexDefn.Deferred = true // prevent the build from happening on indexer for now; just move the metadata
		indexDefn.InstId = tt.InstId
		indexDefn.RealInstId = tt.RealInstId

		getReqBody := func() (*bytes.Buffer, bool) {

			ir := manager.IndexRequest{Index: indexDefn}
			body, err := json.Marshal(&ir)
			if err != nil {
				l.Errorf("%v Error marshal clone index %v", method, err)
				r.setTransferTokenError(ttid, tt, err.Error())
				return nil, true
			}

			bodybuf := bytes.NewBuffer(body)
			return bodybuf, false
		}

		bodybuf, isErr := getReqBody()
		if isErr {
			return true
		}

		var resp *http.Response
		var err error
		url := "/createIndexRebalance"
		resp, err = postWithAuth(r.localaddr+url, "application/json", bodybuf)
		if err != nil {
			// Error from HTTP layer, not from index processing code
			l.Errorf("%v Error register clone index on %v %v", method, r.localaddr+url, err)
			// If the error is io.EOF, then it is possible that server side
			// may have closed the connection while client is about the send the request.
			// Though this is extremely unlikely, this is observed for multiple users
			// in golang community. See: https://github.com/golang/go/issues/19943,
			// https://groups.google.com/g/golang-nuts/c/A46pBUjdgeM/m/jrn35_IxAgAJ for
			// more details
			//
			// In such a case, instead of failing the rebalance with io.EOF error, retry
			// the POST request. Two scenarios exist here:
			// (a) Server has received the request and closed the connection (Very unlikely)
			//     In this case, the request will be processed by server but client will see
			//     EOF error. Retry will fail rebalance that index definition already exists.
			// (b) Server has not received this request. Then retry will work and rebalance
			//     will not fail.
			//
			// Instead of failing rebalance with io.EOF error, we retry the request and reduce
			// probability of failure
			if strings.HasSuffix(err.Error(), ": EOF") {
				bodybuf, isErr := getReqBody()
				if isErr {
					return true
				}
				resp, err = postWithAuth(r.localaddr+url, "application/json", bodybuf)
				if err != nil {
					l.Errorf("%v Error register clone index during retry on %v %v",
						method, r.localaddr+url, err)
					r.setTransferTokenError(ttid, tt, err.Error())
					return true
				} else {
					l.Infof("%v Successful POST of createIndexRebalance during retry on %v, defnId: %v, instId: %v",
						method, r.localaddr+url, indexDefn.DefnId, indexDefn.InstId)
				}
			} else {
				r.setTransferTokenError(ttid, tt, err.Error())
				return true
			}
		}

		response := new(manager.IndexResponse)
		if err := convertResponse(resp, response); err != nil {
			l.Errorf("%v Error unmarshal response %v %v", method, r.localaddr+url, err)
			r.setTransferTokenError(ttid, tt, err.Error())
			return true
		}
		if response.Code == manager.RESP_ERROR {
			// Error from index processing code (e.g. the string from common.ErrCollectionNotFound)
			if !isMissingBSC(response.Error) {
				l.Errorf("%v Error cloning index %v %v", method, r.localaddr+url, response.Error)
				r.setTransferTokenError(ttid, tt, response.Error)
				return true
			}
			// Ok: failed to create dest index because b/s/c was dropped. Skip to TransferTokenCommit state.
			l.Infof("%v Create destination index failed due to bucket/scope/collection dropped. Skipping. tt %v.", method, tt)
			tt.State = c.TransferTokenCommit
		} else {
			tt.State = c.TransferTokenAccepted
		}
		setTransferTokenInMetakv(ttid, tt)

		r.mu.Lock()
		r.acceptedTokens[ttid] = tt
		r.mu.Unlock()

	case c.TransferTokenInitiate:

		r.mu.Lock()
		defer r.mu.Unlock()
		att, ok := r.acceptedTokens[ttid]
		if !ok {
			l.Errorf("%v Unknown TransferToken for Initiate %v %v", method, ttid, tt)
			r.setTransferTokenError(ttid, tt, "Unknown TransferToken For Initiate")
			return true
		}
		// User-deferred index will move its metadata but not be built by Rebalance, so skip TransferTokenInProgress
		if tt.IsUserDeferred() {
			r.destTokenToMergeOrReadyLOCKED(ttid, tt)
			att.State = tt.State
		} else { // non-user-deferred: Rebalance will build this index
			att.State = c.TransferTokenInProgress
			tt.State = c.TransferTokenInProgress
			setTransferTokenInMetakv(ttid, tt)
		}

		buildTokens := r.checkAllAcceptedIndexesReadyToBuildLOCKED()
		if buildTokens != nil {
			if !r.addToWaitGroup() {
				return true
			}
			go r.buildAcceptedIndexes(buildTokens)
		}

	case c.TransferTokenInProgress:
		// Nothing to do here; TT state transitions done in destTokenToMergeOrReadyLOCKED

	case c.TransferTokenMerge:
		// Nothing to do here; TT state transitions done in destTokenToMergeOrReadyLOCKED

	default:
		return false
	}
	return true
}

func (r *Rebalancer) checkValidNotifyStateDest(ttid string, tt *c.TransferToken) bool {

	r.mu.Lock()
	defer r.mu.Unlock()

	if att, ok := r.acceptedTokens[ttid]; ok {
		if tt.State <= att.State {
			l.Warnf("Rebalancer::checkValidNotifyStateDest Detected Invalid State "+
				"Change Notification. Token Id %v Local State %v Metakv State %v", ttid,
				att.State, tt.State)
			return false
		}
	}
	return true

}

func (r *Rebalancer) checkValidNotifyStateSource(ttid string, tt *c.TransferToken) bool {

	r.mu.Lock()
	defer r.mu.Unlock()

	if tto, ok := r.sourceTokens[ttid]; ok {
		if tt.State <= tto.State {
			l.Warnf("Rebalancer::checkValidNotifyStateSource Detected Invalid State "+
				"Change Notification. Token Id %v Local State %v Metakv State %v", ttid,
				tto.State, tt.State)
			return false
		}
	}
	return true

}

// checkAllAcceptedIndexesReadyToBuildLOCKED determines whether all TTs accepted so far have reached
// a state where a build request can be sent to Indexer for those that need to be built. If so,
// it returns a map from ttid to tt of those that need building, else it returns nil. This is done
// so a group of builds can be submitted in a single call to Indexer.
// Caller must be holding r.mu at least read locked.
func (r *Rebalancer) checkAllAcceptedIndexesReadyToBuildLOCKED() (buildTokens map[string]*common.TransferToken) {

	// acceptedTokens are destination-owned copies and thus should never be set to
	// TransferTokenDeleted state, so that state is not checked here
	for ttid, tt := range r.acceptedTokens {
		if tt.State != c.TransferTokenInProgress &&
			tt.State != c.TransferTokenMerge &&
			tt.State != c.TransferTokenReady &&
			tt.State != c.TransferTokenCommit {
			l.Infof("Rebalancer::checkAllAcceptedIndexesReadyToBuildLOCKED Not ready to build %v %v", ttid, tt)
			return nil
		}
		if tt.State == c.TransferTokenInProgress { // needs build
			if buildTokens == nil {
				buildTokens = make(map[string]*common.TransferToken)
			}
			buildTokens[ttid] = tt
		}
	}
	return buildTokens
}

// checkAllSourceIndexesReadyToDropLOCKED determines whether all TTs this node is the dest OR source
// for so far are done building. If so, it returns a slice of ttids of the source tokens that need
// to be queued for dropping, else it returns nil. This function is only used when the cluster
// contains pre-7.1.0 nodes, where we must avoid overlapping builds/merges and drops/prunes to work
// around MB-48191.
// Caller must be holding r.mu at least read locked.
func (r *Rebalancer) checkAllSourceIndexesReadyToDropLOCKED() (dropTokens []string) {
	// Check for any unbuilt dest tokens for this node
	for _, tt := range r.acceptedTokens {
		if tt.State < c.TransferTokenReady || tt.State == c.TransferTokenMerge {
			return nil
		}
	}

	for ttid, tt := range r.sourceTokens {
		// Check for any unbuilt source tokens for this node
		if tt.State < c.TransferTokenReady || tt.State == c.TransferTokenMerge {
			return nil
		}

		// Source tokens still needing drop
		if tt.State == c.TransferTokenReady {
			dropTokens = append(dropTokens, ttid)
		}
	}
	return dropTokens
}

// buildAcceptedIndexes runs in a go routine and submits a request to Indexer to build all indexes for
// accepted transfer tokens that are ready to start building (buildTokens). The builds occur asynchronously
// in Indexer handleBuildIndex. This function calls waitForIndexBuild to wait for them to finish.
func (r *Rebalancer) buildAcceptedIndexes(buildTokens map[string]*common.TransferToken) {
	const method string = "Rebalancer::buildAcceptedIndexes:" // for logging

	defer r.wg.Done()

	var idList client.IndexIdList // defnIds of the indexes to build
	for _, tt := range buildTokens {
		idList.DefnIds = append(idList.DefnIds, uint64(tt.IndexInst.Defn.DefnId))
	}

	if len(idList.DefnIds) == 0 {
		l.Infof("%v Nothing to build", method)
		return
	}

	response := new(manager.IndexResponse)
	url := "/buildIndexRebalance"

	getReqBody := func() (*bytes.Buffer, error) {
		ir := manager.IndexRequest{IndexIds: idList}
		body, err := json.Marshal(&ir)
		if err != nil {
			l.Errorf("%v Error marshalling index inst list: %v, err: %v", method, idList, err)
			return nil, err
		}
		bodybuf := bytes.NewBuffer(body)
		return bodybuf, nil
	}

	var bodybuf *bytes.Buffer
	var resp *http.Response
	var err error
	var errStr string

	bodybuf, err = getReqBody()
	if err != nil {
		errStr = err.Error()
		goto cleanup
	}

	resp, err = postWithAuth(r.localaddr+url, "application/json", bodybuf)
	if err != nil {
		// Error from HTTP layer, not from index processing code
		l.Errorf("%v Error register clone index on %v %v", method, r.localaddr+url, err)
		if strings.HasSuffix(err.Error(), ": EOF") {
			// Retry build again before failing rebalance
			bodybuf, err = getReqBody()
			resp, err = postWithAuth(r.localaddr+url, "application/json", bodybuf)
			if err != nil {
				l.Errorf("%v Error register clone index during retry on %v %v",
					method, r.localaddr+url, err)
				errStr = err.Error()
				goto cleanup
			} else {
				l.Infof("%v Successful POST of buildIndexRebalance during retry on %v, instIdList: %v",
					method, r.localaddr+url, idList)
			}
		} else {
			errStr = err.Error()
			goto cleanup
		}
	}

	if err := convertResponse(resp, response); err != nil {
		l.Errorf("%v Error unmarshal response %v %v", method, r.localaddr+url, err)
		errStr = err.Error()
		goto cleanup
	}
	if response.Code == manager.RESP_ERROR {
		// Error from index processing code. For rebalance this returns either ErrMarshalFailed.Error
		// or a JSON string of a marshaled map[IndexInstId]string of error messages per instance ID. The
		// keys for any entries having magic error string ErrIndexNotFoundRebal.Error are the submitted
		// defnIds instead of instIds, as the metadata could not be found for these.
		l.Errorf("%v Error cloning index %v %v", method, r.localaddr+url, response.Error)
		errStr = response.Error
		if errStr == common.ErrMarshalFailed.Error() { // no detailed error info available
			goto cleanup
		}

		// Unmarshal the detailed error info
		var errMap map[c.IndexInstId]string
		err = json.Unmarshal([]byte(errStr), &errMap)
		if err != nil {
			errStr = c.ErrUnmarshalFailed.Error()
			goto cleanup
		}

		// Deal with the individual errors
		for id, errStr := range errMap {
			if isMissingBSC(errStr) {
				// id is an instId. Move the TT for this instId to TransferTokenCommit
				r.mu.Lock()
				for ttid, tt := range buildTokens {
					if id == tt.IndexInst.InstId {
						l.Infof("%v Build destination index failed due to bucket/scope/collection dropped. Skipping. tt %v.", method, tt)
						tt.State = c.TransferTokenCommit
						setTransferTokenInMetakv(ttid, tt)
						break
					}
				}
				r.mu.Unlock()
			} else if isIndexNotFoundRebal(errStr) {
				// id is a defnId. Move all TTs for this defnId to TransferTokenCommit
				defnId := c.IndexDefnId(id)
				r.mu.Lock()
				for ttid, tt := range buildTokens {
					if defnId == tt.IndexInst.Defn.DefnId {
						l.Infof("%v Build destination index failed due to index metadata missing; bucket/scope/collection likely dropped. Skipping. tt %v.", method, tt)
						tt.State = c.TransferTokenCommit
						setTransferTokenInMetakv(ttid, tt)
					}
				}
				r.mu.Unlock()
			} else {
				goto cleanup // unrecoverable error
			}
		}
	}

	r.waitForIndexBuild(buildTokens)
	return

cleanup: // fail the rebalance; mark all accepted transfer tokens with error
	r.mu.Lock()
	for ttid, tt := range r.acceptedTokens {
		r.setTransferTokenError(ttid, tt, errStr)
	}
	r.mu.Unlock()
	return
}

// waitForIndexBuild waits for all currently running rebalance-initiated index
// builds to complete. Transfer token state changes and some processing are
// delegated to destTokenToMergeOrReadyLOCKED.
func (r *Rebalancer) waitForIndexBuild(buildTokens map[string]*common.TransferToken) {
	const method string = "Rebalancer::waitForIndexBuild:" // for logging

	buildStartTime := time.Now()
	cfg := r.config.Load()
	maxRemainingBuildTime := cfg["rebalance.maxRemainingBuildTime"].Uint64()

loop:
	for {
		select {
		case <-r.cancel:
			l.Infof("%v Cancel Received", method)
			break loop
		case <-r.done:
			l.Infof("%v Done Received", method)
			break loop
		default:

			stats, err := getLocalStats(r.localaddr, false)
			if err != nil {
				l.Errorf("%v Error Fetching Local Stats %v %v", method, r.localaddr, err)
				break
			}

			statsMap := stats.ToMap()
			if statsMap == nil {
				l.Infof("%v Nil Stats. Retrying...", method)
				break
			}

			if state, ok := statsMap["indexer_state"]; ok {
				if state == "Paused" {
					l.Errorf("%v Paused state detected for %v", method, r.localaddr)
					r.mu.Lock()
					for ttid, tt := range r.acceptedTokens {
						l.Errorf("%v Token State Changed to Error %v %v", method, ttid, tt)
						r.setTransferTokenError(ttid, tt, "Indexer In Paused State")
					}
					r.mu.Unlock()
					break
				}
			}

			localMeta, err := getLocalMeta(r.localaddr)
			if err != nil {
				l.Errorf("%v Error Fetching Local Meta %v %v", method, r.localaddr, err)
				break
			}

			r.mu.Lock()
			allTokensReady := true
			for ttid, tt := range buildTokens {
				if tt.State == c.TransferTokenReady || tt.State == c.TransferTokenCommit {
					continue
				}
				allTokensReady = false

				if tt.State == c.TransferTokenMerge {
					continue
				}

				indexState, err := getIndexStatusFromMeta(tt, localMeta)
				if indexState == c.INDEX_STATE_NIL || indexState == c.INDEX_STATE_DELETED {
					l.Infof("%v Could not get index status; bucket/scope/collection likely dropped."+
						" Skipping. indexState %v, err %v, tt %v.", method, indexState, err, tt)
					tt.State = c.TransferTokenCommit // skip forward instead of failing rebalance
					setTransferTokenInMetakv(ttid, tt)
				} else if err != "" {
					l.Errorf("%v Error Fetching Index Status %v %v", method, r.localaddr, err)
					break
				}

				defn := tt.IndexInst.Defn
				defn.SetCollectionDefaults()

				prefix := common.GetStatsPrefix(defn.Bucket, defn.Scope, defn.Collection,
					defn.Name, tt.IndexInst.ReplicaId, 0, false)

				sname_pend := common.GetIndexStatKey(prefix, "num_docs_pending")
				sname_queued := common.GetIndexStatKey(prefix, "num_docs_queued")
				sname_processed := common.GetIndexStatKey(prefix, "num_docs_processed")

				var num_pend, num_queued, num_processed float64
				if _, ok := statsMap[sname_pend]; ok {
					num_pend = statsMap[sname_pend].(float64)
					num_queued = statsMap[sname_queued].(float64)
					num_processed = statsMap[sname_processed].(float64)
				} else {
					l.Infof("%v Missing Stats %v %v. Retrying...", method, sname_queued, sname_pend)
					break
				}

				tot_remaining := num_pend + num_queued

				elapsed := time.Since(buildStartTime).Seconds()
				if elapsed == 0 {
					elapsed = 1
				}

				processing_rate := num_processed / elapsed
				remainingBuildTime := maxRemainingBuildTime
				if processing_rate != 0 {
					remainingBuildTime = uint64(tot_remaining / processing_rate)
				}

				if tot_remaining == 0 {
					remainingBuildTime = 0
				}

				l.Infof("%v Index: %v:%v:%v:%v State: %v"+
					" Pending: %v EstTime: %v Partitions: %v Destination: %v",
					method, defn.Bucket, defn.Scope, defn.Collection, defn.Name, indexState,
					tot_remaining, remainingBuildTime, defn.Partitions, r.localaddr)

				if indexState == c.INDEX_STATE_ACTIVE && remainingBuildTime < maxRemainingBuildTime {
					r.destTokenToMergeOrReadyLOCKED(ttid, tt)
				}
			}
			r.mu.Unlock()

			if allTokensReady {
				l.Infof("%v Batch Done", method)
				break loop
			}
			time.Sleep(3 * time.Second)
		}
	}
}

// destTokenToMergeOrReadyLOCKED handles dest TT transitions from TransferTokenInProgress,
// possibly through TransferTokenMerge, and then to TransferTokenReady (move
// case) or TransferTokenCommit (non-move case == replica repair; no source
// index to delete). TransferTokenMerge state is for partitioned indexes where
// a partn is being moved to a node that already has another partn of the same
// index. There cannot be two IndexDefns of the same index, so in this situation
// the moving partition (called a "proxy") gets a "fake" IndexDefn that later
// must "merge" with the "real" IndexDefn once it has completed its move.
//
// tt arg points to an entry in r.acceptedTokens map.
// Caller must be holding r.mu write locked.
func (r *Rebalancer) destTokenToMergeOrReadyLOCKED(ttid string, tt *c.TransferToken) {
	const method string = "Rebalancer::destTokenToMergeOrReadyLOCKED:" // for logging

	// There is no proxy (no merge needed)
	if tt.RealInstId == 0 {

		respch := make(chan error)
		r.supvMsgch <- &MsgUpdateIndexRState{
			instId: tt.InstId,
			rstate: c.REBAL_ACTIVE,
			respch: respch}
		err := <-respch
		c.CrashOnError(err)

		if tt.TransferMode == c.TokenTransferModeMove {
			tt.State = c.TransferTokenReady
		} else {
			tt.State = c.TransferTokenCommit // no source to delete in non-move case
		}
		setTransferTokenInMetakv(ttid, tt)
	} else {
		// There is a proxy (merge needed). The proxy partitions need to be move
		// to the real index instance before Token can move to Ready state.
		tt.State = c.TransferTokenMerge
		setTransferTokenInMetakv(ttid, tt)

		go func(ttid string, tt c.TransferToken) {

			respch := make(chan error)
			r.supvMsgch <- &MsgMergePartition{
				srcInstId:  tt.InstId,
				tgtInstId:  tt.RealInstId,
				rebalState: c.REBAL_ACTIVE,
				respCh:     respch}

			var err error
			select {
			case <-r.cancel:
				l.Infof("%v rebalancer cancel Received", method)
				return
			case <-r.done:
				l.Infof("%v rebalancer done Received", method)
				return
			case err = <-respch:
			}
			if err != nil {
				// If there is an error, rewind state to TransferTokenInProgress so cleanup
				// will be done after the intentional crash this block then produces.
				// An error condition indicates that merge has not been
				// committed yet, so we are safe to revert.
				tt.State = c.TransferTokenInProgress
				setTransferTokenInMetakv(ttid, &tt)

				// The indexer could be in an inconsistent state.
				// So we will need to restart the indexer.
				time.Sleep(100 * time.Millisecond)
				c.CrashOnError(err)
			}

			// There is no error from merging index instance. Update token in metakv.
			if tt.TransferMode == c.TokenTransferModeMove {
				tt.State = c.TransferTokenReady
			} else {
				tt.State = c.TransferTokenCommit // no source to delete in non-move case
			}
			setTransferTokenInMetakv(ttid, &tt)

			// if rebalancer is still active, then update its runtime state.
			if !r.isFinish() {
				r.mu.Lock()
				defer r.mu.Unlock()

				if newTT := r.acceptedTokens[ttid]; newTT != nil {
					newTT.State = tt.State
				}
			}

		}(ttid, *tt)
	}
}

// processTokenAsMaster performs master node TT bookkeeping for a rebalance. It
// handles transfer token states TransferTokenAccepted, TransferTokenCommit,
// TransferTokenDeleted, and NO-OP TransferTokenRefused. Returns true iff it is
// considered to have processed this token (including handling some error cases).
func (r *Rebalancer) processTokenAsMaster(ttid string, tt *c.TransferToken) bool {

	if tt.RebalId != r.rebalToken.RebalId {
		l.Warnf("Rebalancer::processTokenAsMaster Found TransferToken with Unknown "+
			"RebalanceId. Local RId %v Token %v. Ignored.", r.rebalToken.RebalId, tt)
		return true
	}

	if tt.Error != "" {
		l.Errorf("Rebalancer::processTokenAsMaster Detected TransferToken in Error state %v. Abort.", tt)

		r.cancelMetakv()
		go r.finishRebalance(errors.New(tt.Error))
		return true
	}

	switch tt.State {

	case c.TransferTokenAccepted:
		tt.State = c.TransferTokenInitiate
		setTransferTokenInMetakv(ttid, tt)

	case c.TransferTokenRefused:
		//TODO replan

	case c.TransferTokenCommit:
		tt.State = c.TransferTokenDeleted
		setTransferTokenInMetakv(ttid, tt)

		r.updateMasterTokenState(ttid, c.TransferTokenCommit)

	case c.TransferTokenDeleted:
		err := c.MetakvDel(RebalanceMetakvDir + ttid)
		if err != nil {
			l.Fatalf("Rebalancer::processTokenAsMaster Unable to set TransferToken In "+
				"Meta Storage. %v. Err %v", tt, err)
			c.CrashOnError(err)
		}

		r.updateMasterTokenState(ttid, c.TransferTokenDeleted)
		if r.checkAllTokensDone() { // rebalance completed
			if r.cb.progress != nil {
				r.cb.progress(1.0, r.cancel)
			}
			l.Infof("Rebalancer::processTokenAsMaster No Tokens Found. Mark Done.")
			r.cancelMetakv()
			go r.finishRebalance(nil)
		} else {
			r.publishTransferTokenBatch(false)
		}

	default:
		return false
	}
	return true
}

func (r *Rebalancer) setTransferTokenError(ttid string, tt *c.TransferToken, err string) {
	tt.Error = err
	setTransferTokenInMetakv(ttid, tt)

}

// updateMasterTokenState updates the state of the master versions of transfer tokens
// in the r.transferTokens map.
func (r *Rebalancer) updateMasterTokenState(ttid string, state c.TokenState) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if tt, ok := r.transferTokens[ttid]; ok {
		tt.State = state
	} else {
		return false
	}
	return true
}

// setTransferTokenInMetakv stores a transfer token in metakv with several retries before
// failure. Creation and changes to a token in metakv will trigger the next step of token
// processing via callback from metakv to LifecycleMgr set by its prior OnNewRequest call.
func setTransferTokenInMetakv(ttid string, tt *c.TransferToken) {

	fn := func(r int, err error) error {
		if r > 0 {
			l.Warnf("Rebalancer::setTransferTokenInMetakv error=%v Retrying (%d)", err, r)
		}
		err = c.MetakvSet(RebalanceMetakvDir+ttid, tt)
		return err
	}

	rh := c.NewRetryHelper(10, time.Second, 1, fn)
	err := rh.Run()

	if err != nil {
		l.Fatalf("Rebalancer::setTransferTokenInMetakv Unable to set TransferToken In "+
			"Meta Storage. %v %v. Err %v", ttid, tt, err)
		c.CrashOnError(err)
	}
}

// decodeTransferToken unmarshals a transfer token received in a callback from metakv.
func (r *Rebalancer) decodeTransferToken(path string, value []byte) (string, *c.TransferToken, error) {

	ttidpos := strings.Index(path, TransferTokenTag)
	ttid := path[ttidpos:]

	var tt c.TransferToken
	err := json.Unmarshal(value, &tt)
	if err != nil {
		l.Fatalf("Rebalancer::decodeTransferToken Failed unmarshalling value for %s: %s\n%s",
			path, err.Error(), string(value))
		return "", nil, err
	}

	l.Infof("Rebalancer::decodeTransferToken TransferToken %v %v", ttid, tt)

	return ttid, &tt, nil

}

// updateProgress runs in a master-node go routine to update the progress of processing
// a single transfer token. It is started when the TransferTokenAccepted state is processed.
func (r *Rebalancer) updateProgress() {

	if !r.addToWaitGroup() {
		return
	}
	defer r.wg.Done()

	l.Infof("Rebalancer::updateProgress goroutine started")
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			progress := r.computeProgress()
			r.cb.progress(progress, r.cancel)
		case <-r.cancel:
			l.Infof("Rebalancer::updateProgress Cancel Received")
			return
		case <-r.done:
			l.Infof("Rebalancer::updateProgress Done Received")
			return
		}
	}
}

// computeProgress is a helper for updateProgress called periodically to compute the progress of index builds.
func (r *Rebalancer) computeProgress() (progress float64) {

	url := "/getIndexStatus?getAll=true"
	resp, err := getWithAuth(r.localaddr + url)
	if err != nil {
		l.Errorf("Rebalancer::computeProgress Error getting local metadata %v %v", r.localaddr+url, err)
		return
	}

	defer resp.Body.Close()
	statusResp := new(manager.IndexStatusResponse)
	bytes, _ := ioutil.ReadAll(resp.Body)
	if err := json.Unmarshal(bytes, &statusResp); err != nil {
		l.Errorf("Rebalancer::computeProgress Error unmarshal response %v %v", r.localaddr+url, err)
		return
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	totTokens := len(r.transferTokens)

	var totalProgress float64
	for _, tt := range r.transferTokens {
		state := tt.State
		// All states not tested in the if-else if are treated as 0% progress
		if state == c.TransferTokenReady || state == c.TransferTokenMerge ||
			state == c.TransferTokenCommit || state == c.TransferTokenDeleted {
			totalProgress += 100.00
		} else if state == c.TransferTokenInProgress {
			totalProgress += r.getBuildProgressFromStatus(statusResp, tt)
		}
	}

	progress = (totalProgress / float64(totTokens)) / 100.0
	l.Infof("Rebalancer::computeProgress %v", progress)

	if progress < 0.1 || math.IsNaN(progress) {
		progress = 0.1
	} else if progress == 1.0 {
		progress = 0.99
	}
	return
}

// checkAllTokensDone returns true iff processing for all transfer tokens
// of the current rebalance is complete (i.e. the rebalance is finished).
func (r *Rebalancer) checkAllTokensDone() bool {

	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, tt := range r.transferTokens {
		if tt.State != c.TransferTokenDeleted {
			return false
		}
	}
	return true
}

func (r *Rebalancer) checkDDLRunning() (bool, error) {

	if r.runParam != nil && r.runParam.ddlRunning {
		l.Errorf("Rebalancer::doRebalance Found index build running. Cannot process rebalance.")
		fmtMsg := "indexer rebalance failure - index build is in progress for indexes: %v."
		err := errors.New(fmt.Sprintf(fmtMsg, r.runParam.ddlRunningIndexNames))
		return true, err
	}
	return false, nil
}

// getIndexStatusFromMeta gets the index state and error message, if present, for the
// index referenced by a transfer token from index metadata (topology tree). It returns
// state INDEX_STATE_NIL if the metadata is not found (e.g. if the index has been
// dropped). If an error message is returned with a state other than INDEX_STATE_NIL,
// the metadata was found and the message is from a field in the topology tree itself.
func getIndexStatusFromMeta(tt *c.TransferToken, localMeta *manager.LocalIndexMetadata) (c.IndexState, string) {

	inst := tt.IndexInst

	topology := findTopologyByCollection(localMeta.IndexTopologies, inst.Defn.Bucket, inst.Defn.Scope, inst.Defn.Collection)
	if topology == nil {
		return c.INDEX_STATE_NIL, fmt.Sprintf("Topology Information Missing for Bucket %v Scope %v Collection %v",
			inst.Defn.Bucket, inst.Defn.Scope, inst.Defn.Collection)
	}

	state, errMsg := topology.GetStatusByInst(inst.Defn.DefnId, tt.InstId)
	if state == c.INDEX_STATE_NIL && tt.RealInstId != 0 {
		return topology.GetStatusByInst(inst.Defn.DefnId, tt.RealInstId)
	}

	return state, errMsg
}

// getDestNode returns the key of the partitionMap entry whose value
// contains partitionId. This is the node hosting that partition.
func getDestNode(partitionId c.PartitionId, partitionMap map[string][]int) string {
	for node, partIds := range partitionMap {
		for _, partId := range partIds {
			if partId == int(partitionId) {
				return node
			}
		}
	}
	return "" // should not reach here
} // getDestNode

// getBuildProgressFromStatus is a helper for computeProgress that gets an estimate of index build progress for the
// given transfer token from the status arg.
func (r *Rebalancer) getBuildProgressFromStatus(status *manager.IndexStatusResponse, tt *c.TransferToken) float64 {

	instId := tt.InstId
	realInstId := tt.RealInstId // for partitioned indexes

	// If it is a partitioned index, it is possible that we cannot find progress from instId:
	// 1) partition is built using realInstId
	// 2) partition has already been merged to instance with realInstId
	// In either case, count will be 0 after calling getBuildProgress(instId) and it will find progress
	// using realInstId instead.
	realInstProgress, count := r.getBuildProgress(status, tt, instId)
	if count == 0 {
		realInstProgress, count = r.getBuildProgress(status, tt, realInstId)
	}
	if count > 0 {
		r.lastKnownProgress[instId] = realInstProgress / float64(count)
	}

	if p, ok := r.lastKnownProgress[instId]; ok {
		return p
	}
	return 0.0
} // getBuildProgressFromStatus

// getBuildProgress is a helper for getBuildProgressFromStatus that gets the progress of an inde≈ build
// for a given instId. Return value count gives the number of partitions of instId found to be building.
// If this is 0 the caller will try again with realInstId to find the progress of a partitioned index.
func (r *Rebalancer) getBuildProgress(status *manager.IndexStatusResponse, tt *c.TransferToken, instId c.IndexInstId) (
	realInstProgress float64, count int) {

	destId := tt.DestId
	defn := tt.IndexInst.Defn

	for _, idx := range status.Status {
		if idx.InstId == instId {
			// This function is called for every transfer token before it has becomes COMMITTED or DELETED.
			// The index may have not be in REAL_PENDING state but the token has not yet moved to COMMITTED/DELETED state.
			// So we need to return progress even if it is not replicating.
			// Pre-7.0 nodes will report "Replicating" instead of "Moving" so check for both.
			if idx.Status == "Moving" || idx.Status == "Replicating" || idx.NodeUUID == destId {
				progress, ok := r.lastKnownProgress[instId]
				if !ok || idx.Progress > 0 {
					progress = idx.Progress
				}

				destNode := getDestNode(defn.Partitions[0], idx.PartitionMap)
				l.Infof("Rebalancer::getBuildProgress Index: %v:%v:%v:%v"+
					" Progress: %v InstId: %v RealInstId: %v Partitions: %v Destination: %v",
					defn.Bucket, defn.Scope, defn.Collection, defn.Name,
					progress, idx.InstId, tt.RealInstId, defn.Partitions, destNode)

				realInstProgress += progress
				count++
			}
		}
	}
	return realInstProgress, count
}

//
// This function gets the indexer stats for a specific indexer host.
//
func getLocalStats(addr string, partitioned bool) (*c.Statistics, error) {

	queryStr := "/stats?async=true&consumerFilter=rebalancer"
	if partitioned {
		queryStr += "&partition=true"
	}
	resp, err := getWithAuth(addr + queryStr)
	if err != nil {
		return nil, err
	}

	stats := new(c.Statistics)
	if err := convertResponse(resp, stats); err != nil {
		return nil, err
	}

	return stats, nil
}

func getLocalMeta(addr string) (*manager.LocalIndexMetadata, error) {

	url := "/getLocalIndexMetadata?useETag=false"
	resp, err := getWithAuth(addr + url)
	if err != nil {
		l.Errorf("Rebalancer::getLocalMeta Error getting local metadata %v %v", addr+url, err)
		return nil, err
	}

	defer resp.Body.Close()
	localMeta := new(manager.LocalIndexMetadata)
	bytes, _ := ioutil.ReadAll(resp.Body)
	if err := json.Unmarshal(bytes, &localMeta); err != nil {
		l.Errorf("Rebalancer::getLocalMeta Error unmarshal response %v %v", addr+url, err)
		return nil, err
	}

	return localMeta, nil
}

//returs if all indexers are warmedup and addr of any paused indexer
func checkAllIndexersWarmedup(clusterURL string) (bool, []string) {

	var pausedAddr []string

	cinfo, err := c.FetchNewClusterInfoCache2(clusterURL, c.DEFAULT_POOL, "checkAllIndexersWarmedup")
	if err != nil {
		l.Errorf("Rebalancer::checkAllIndexersWarmedup Error Fetching Cluster Information %v", err)
		return false, nil
	}

	if err := cinfo.FetchNodesAndSvsInfo(); err != nil {
		l.Errorf("Rebalancer::checkAllIndexersWarmedup Error Fetching Nodes and serives Information %v", err)
		return false, nil
	}

	nids := cinfo.GetNodesByServiceType(c.INDEX_HTTP_SERVICE)
	url := "/stats?async=true"

	allWarmedup := true

	for _, nid := range nids {

		addr, err := cinfo.GetServiceAddress(nid, c.INDEX_HTTP_SERVICE, true)
		if err == nil {

			resp, err := getWithAuth(addr + url)
			if err != nil {
				l.Infof("Rebalancer::checkAllIndexersWarmedup Error Fetching Stats %v From %v", err, addr)
				return false, nil
			}

			stats := new(c.Statistics)
			if err := convertResponse(resp, stats); err != nil {
				l.Infof("Rebalancer::checkAllIndexersWarmedup Error Convert Response %v From %v", err, addr)
				return false, nil
			}

			statsMap := stats.ToMap()
			if statsMap == nil {
				l.Infof("Rebalancer::checkAllIndexersWarmedup Nil Stats From %v", addr)
				return false, nil
			}

			if state, ok := statsMap["indexer_state"]; ok {
				if state == "Paused" {
					l.Infof("Rebalancer::checkAllIndexersWarmedup Paused state detected for %v", addr)
					pausedAddr = append(pausedAddr, addr)
				} else if state != "Active" {
					l.Infof("Rebalancer::checkAllIndexersWarmedup Indexer %v State %v", addr, state)
					allWarmedup = false
				}
			}
		} else {
			l.Errorf("Rebalancer::checkAllIndexersWarmedup Error Fetching Service Address %v", err)
			return false, nil
		}
	}

	return allWarmedup, pausedAddr
}

//
// This function unmarshalls a response.
//
func convertResponse(r *http.Response, resp interface{}) error {
	defer r.Body.Close()

	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(r.Body); err != nil {
		return err
	}

	if err := json.Unmarshal(buf.Bytes(), resp); err != nil {
		return err
	}

	return nil
}
