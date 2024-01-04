package indexer

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/indexing/secondary/common"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/logging"
	l "github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/manager"
	"github.com/couchbase/indexing/secondary/manager/client"
	"github.com/couchbase/indexing/secondary/planner"
	"github.com/couchbase/indexing/secondary/security"
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

	// List of transfer tokens which are waiting for RState change when empty
	// node batching is enabled
	pendingReadyTokens map[string]*c.TransferToken

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
	dropIndexDone       chan struct{} // closed to signal rebalance is done with dropping duplicate indexes
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

	activeTransfers map[string]map[string]bool // key -> sourceId, value -> map of shard tokenIds in transfer

	// During scale-out operation, it is possible for multiple nodes to upload
	// shard data to same destination node. In that case, there can be more than
	// "configured" restores happening simultaneously. To limit the impact on
	// download bandwidth due to multiple restores, we use "waitQ" as a semaphore
	// to control the number of downloads
	//
	// TODO: Make the size of this runtime-adjustable
	waitQ chan bool

	// schedule version helps to decide whether to use per node transfer batch size
	// or a global transfer batch size. This setting can not be changed during an
	// ongoing rebalance. It can be changed before a rebalance
	schedulingVersion c.ShardRebalanceSchedulingVersion

	rpcCommandSyncChannel chan struct{}

	cinfo                    *c.ClusterInfoCache
	transferScheme           string
	canMaintainShardAffinity bool
	dcpRebalStarted          bool // set to true if DCP rebalance is initiated

	dcpRebr          RebalanceProvider // controlled rebalancer (DCP rebalancer)
	dcpTokens        map[string]*common.TransferToken
	dcpRebrOnce      sync.Once
	dcpRebrCloseCh   chan struct{} // channel to sync on controlled rebalancer close
	dcpRebrCloseOnce sync.Once     // sync construct to handle the closure of 'dcpRebrCloseCh'

	shardProgressRatio, dcpProgressRatio float64
	shardProgress, dcpProgress           float64Holder

	removeDupIndex bool

	// globalTopology is cluster topology from getGlobalTopology in Rebalance case only, else nil
	globalTopology *manager.ClusterIndexMetadata

	emptyNodeBatchingEnabled bool

	batchBuildReqCh chan *batchBuildReq

	droppedInstsInRebal map[c.IndexInstId]bool
}

func NewShardRebalancer(transferTokens map[string]*c.TransferToken, rebalToken *RebalanceToken,
	nodeUUID string, master bool, progress ProgressCallback, done DoneCallback,
	supvMsgch MsgChannel, localaddr string, config c.Config, topologyChange *service.TopologyChange,
	runPlanner bool, runParams *runParams, statsMgr *statsManager,
	cinfo *c.ClusterInfoCache) *ShardRebalancer {

	clusterVersion := common.GetClusterVersion()
	l.Infof("NewShardRebalancer nodeId %v rebalToken %v master %v localaddr %v runPlanner %v runParam %v clusterVersion %v", nodeUUID,
		rebalToken, master, localaddr, runPlanner, runParams, clusterVersion)

	perNodeBatchSize := config.GetDeploymentModelAwareCfg("rebalance.perNodeTransferBatchSize").Int()
	schedulingVersion := config.GetDeploymentModelAwareCfg("rebalance.scheduleVersion").String()
	transferScheme := getTransferScheme(config)
	canMaintainShardAffinity := c.CanMaintanShardAffinity(config)

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

		acceptedTokens:     make(map[string]*c.TransferToken),
		sourceTokens:       make(map[string]*c.TransferToken),
		ackedTokens:        make(map[string]*c.TransferToken),
		pendingReadyTokens: make(map[string]*c.TransferToken),

		batchedTokens: make([]map[string]*c.TransferToken, 0),

		cancel:              make(chan struct{}),
		done:                make(chan struct{}),
		dropIndexDone:       make(chan struct{}),
		metakvCancel:        make(chan struct{}),
		waitForTokenPublish: make(chan struct{}),

		topologyChange: topologyChange,
		transferStats:  make(map[string]map[common.ShardId]*ShardTransferStatistics),

		dropQueue:         make(chan string, 10000),
		dropQueued:        make(map[string]bool),
		lastKnownProgress: make(map[c.IndexInstId]float64),
		activeTransfers:   make(map[string]map[string]bool),
		waitQ:             make(chan bool, perNodeBatchSize),
		schedulingVersion: c.ShardRebalanceSchedulingVersion(schedulingVersion),

		rpcCommandSyncChannel: make(chan struct{}),

		cinfo:                    cinfo,
		transferScheme:           transferScheme,
		canMaintainShardAffinity: canMaintainShardAffinity,

		dcpRebrCloseCh: make(chan struct{}),

		batchBuildReqCh:     make(chan *batchBuildReq, 100),
		droppedInstsInRebal: make(map[c.IndexInstId]bool),
	}

	sr.shardProgress.SetFloat64(0)
	sr.dcpProgress.SetFloat64(0)

	sr.config.Store(config)

	// Clean-up any empty shards at the start of rebalance
	sr.supvMsgch <- &MsgDestroyEmptyShard{
		force: true,
	}

	if master {
		go sr.initRebalAsync()
	} else {
		close(sr.waitForTokenPublish)
		go sr.observeRebalance()
	}

	if sr.canMaintainShardAffinity {
		// start shard transfer server
		go sr.sendPeerServerCommand(START_PEER_SERVER, sr.rpcCommandSyncChannel, sr.done)
	} else {
		close(sr.rpcCommandSyncChannel)
	}

	go sr.processDropShards()
	go sr.updateTransferProgress()
	go sr.processBatchBuildReqs()

	return sr
}

func (sr *ShardRebalancer) observeRebalance() {
	l.Infof("ShardRebalancer::observeRebalance %v master:%v", sr.rebalToken, sr.isMaster)

	<-sr.waitForTokenPublish
	<-sr.rpcCommandSyncChannel

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
		if sr.cb.progress != nil {
			sr.cb.progress(sr.shardProgressRatio, sr.cancel)
		}
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

				if globalTopology, err := GetGlobalTopology(sr.localaddr); err != nil {
					l.Errorf("ShardRebalancer::initRebalAsync Error Fetching Topology %v", err)
					go sr.finishRebalance(err)
					return
				} else {
					sr.globalTopology = globalTopology
					l.Infof("ShardRebalancer::initRebalAsync Global Topology %v", globalTopology)
				}

				var err error
				var hostToIndexToRemove map[string]map[common.IndexDefnId]*common.IndexDefn

				// If shard affinity is enabled in non-serverless deployments, used normal planner.
				// Otherwise, use tenant aware planner
				if sr.canMaintainShardAffinity && !common.IsServerlessDeployment() {
					onEjectOnly := cfg["rebalance.node_eject_only"].Bool()
					optimizePlacement := cfg["settings.rebalance.redistribute_indexes"].Bool()
					disableReplicaRepair := cfg["rebalance.disable_replica_repair"].Bool()
					timeout := cfg["planner.timeout"].Int()
					threshold := cfg["planner.variationThreshold"].Float64()
					cpuProfile := cfg["planner.cpuProfile"].Bool()
					minIterPerTemp := cfg["planner.internal.minIterPerTemp"].Int()
					maxIterPerTemp := cfg["planner.internal.maxIterPerTemp"].Int()
					binSize := common.GetBinSize(cfg)

					//user setting redistribute_indexes overrides the internal setting
					//onEjectOnly. onEjectOnly is not expected to be used in production
					//as this is not documented.
					if optimizePlacement {
						onEjectOnly = false
					} else {
						onEjectOnly = true
					}

					sr.transferTokens, hostToIndexToRemove, err = planner.ExecuteRebalance(cfg["clusterAddr"].String(), *sr.topologyChange,
						sr.nodeUUID, onEjectOnly, disableReplicaRepair, threshold, timeout, cpuProfile,
						minIterPerTemp, maxIterPerTemp, binSize, true)

				} else { //
					sr.transferTokens, _, err = planner.ExecuteTenantAwareRebalance(cfg["clusterAddr"].String(),
						*sr.topologyChange, sr.nodeUUID)
				}

				if err != nil {
					l.Errorf("ShardRebalancer::initRebalAsync Planner Error %v", err)
					go sr.finishRebalance(err)
					return
				}

				if err := sr.updateAlternateShardIds(); err != nil {
					l.Errorf("ShardRebalancer::initRebalAsync Error while updating alterante shardIds. err: %v", err)
					go sr.finishRebalance(err)
					return
				}

				if len(hostToIndexToRemove) > 0 {
					sr.removeDupIndex = true
					go RemoveDuplicateIndexes(hostToIndexToRemove, sr.cancel, sr.done,
						sr.dropIndexDone, "ShardRebalancer")
				}

				if len(sr.transferTokens) == 0 {
					sr.transferTokens = nil
				} else if !sr.canMaintainShardAffinity && common.IsServerlessDeployment() {
					destination, region, err := getDestinationFromConfig(sr.config.Load())
					if err != nil {
						l.Errorf("ShardRebalancer::initRebalAsync err: %v", err)
						go sr.finishRebalance(err)
						return
					}
					// Populate destination in transfer tokens
					for _, token := range sr.transferTokens {
						token.Destination = destination
						token.Region = region
					}
					sr.destination = destination
					l.Infof("ShardRebalancer::initRebalAsync Populated destination: %v, region: %v in all transfer tokens", destination, region)

				} else if sr.canMaintainShardAffinity {
					err := sr.cinfo.FetchNodesAndSvsInfoWithLock()
					if err != nil {
						l.Warnf("ShardRebalancer::initRebalAsync failed to fetch nodes and services info with error %v. Using stale values",
							err)
					}

					// seperate out list of tokens for shard and DCP transfer
					sr.transferTokens, sr.dcpTokens = func(tokens map[string]*c.TransferToken) (
						shardTokens, dcpTokens map[string]*c.TransferToken) {

						shardTokens = make(map[string]*c.TransferToken)
						dcpTokens = make(map[string]*c.TransferToken)

						for ttid, tt := range tokens {
							if tt.IsShardTransferToken() {
								shardTokens[ttid] = tt
							} else if tt.IsDcpTransferToken() {
								dcpTokens[ttid] = tt
							} else {
								l.Warnf("ShardRebalancer::initRebalAsync got invalid transfer token %v. skipping it...",
									ttid)
							}
						}

						return shardTokens, dcpTokens
					}(sr.transferTokens)

					for tid, token := range sr.transferTokens {
						if !token.IsShardTransferToken() {
							continue
						}

						token.Destination, err = sr.genPeerDestination(token.DestId)
						if err != nil {
							l.Errorf("ShardRebalancer::initRebalAsync failed to create destination for token %v (destination %v) with error %v",
								tid, token.DestId, err)
							go sr.finishRebalance(err)
							return
						}

						l.Infof("ShardRebalancer::initRebalAsync set destination %v for token with id %v",
							token.Destination, tid)
					}
				}

				sr.enableEmptyNodeBatching()

				sum := len(sr.transferTokens) + len(sr.dcpTokens)
				if sum == 0 {
					sum = 1
				}

				sr.shardProgressRatio = float64(len(sr.transferTokens)) / float64(sum)
				sr.dcpProgressRatio = float64(len(sr.dcpTokens)) / float64(sum)

				break loop
			}
		}
	}

	go sr.doRebalance()
}

func getAlternateShardIds(ttid string, tt *c.TransferToken) ([]string, error) {

	// Step-1: Make sure that there exists <= 2 alternate shardIds
	// for all the indexes in the token
	altShardIdMap := make(map[string]bool)
	for _, inst := range tt.IndexInsts {
		for _, altShardIds := range inst.Defn.AlternateShardIds {
			for _, altShardId := range altShardIds {
				altShardIdMap[altShardId] = true
			}
		}
	}

	if len(altShardIdMap) > 2 {
		err := fmt.Errorf("ShardRebalancer::getAlternateShardIds Found more than 2 alternate shardIds for token: %v, "+
			"alternate shardIds: %v", ttid, altShardIdMap)
		return nil, err
	}

	// Step-2: Populate the slice of alternate shardIds
	result := make([]string, len(altShardIdMap))
	for _, inst := range tt.IndexInsts {
		for _, altShardIds := range inst.Defn.AlternateShardIds {
			if len(altShardIds) == len(altShardIdMap) {
				copy(result, altShardIds)
				return result, nil
			}
		}
	}
	return nil, fmt.Errorf("ShardRebalancer::getAlternateShardIds Expected %v alternateShardIds. Found none. "+
		"Token: %v, alternateShardIdMap: %v", len(altShardIdMap), ttid, altShardIdMap)
}

func (sr *ShardRebalancer) updateAlternateShardIds() error {
	// Alternate shardIds are not a part of serverless yet
	if common.IsServerlessDeployment() {
		return nil
	}

	for ttid, tt := range sr.transferTokens {
		if tt.TransferMode == c.TokenTransferModeCopy && tt.BuildSource == c.TokenBuildSourcePeer {
			alternateShardIdsInToken, err := getAlternateShardIds(ttid, tt)
			if err != nil {
				logging.Errorf("%v", err)
				return err
			}
			tt.NewAlternateShardIds = alternateShardIdsInToken
			logging.Infof("ShardRebalancer::updateAlternateShardIds Updating alternate shardIds to %v for token: %v",
				tt.NewAlternateShardIds, ttid)
		}
	}
	return nil
}

func (sr *ShardRebalancer) genPeerDestination(nodeUuid string) (string, error) {
	nid, ok := sr.cinfo.GetNodeIdByUUID(nodeUuid)
	if !ok {
		return "", errors.New("couldn't find source node in cluster info cache")
	}

	addr, err := sr.cinfo.GetServiceAddress(nid, c.INDEX_RPC_SERVICE, true)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%v%v/", sr.transferScheme, addr), nil
}

func getTransferScheme(cfg c.Config) string {
	if c.IsServerlessDeployment() {
		blobStorageScheme := cfg["settings.rebalance.blob_storage_scheme"].String()
		if blobStorageScheme != "" && !strings.HasSuffix(blobStorageScheme, "://") {
			blobStorageScheme += "://"
		}
		return blobStorageScheme
	} else if c.CanMaintanShardAffinity(cfg) {
		shardTransferProtocol := cfg["rebalance.shardTransferProtocol"].String()
		if shardTransferProtocol != "" && !strings.HasSuffix(shardTransferProtocol, "://") {
			shardTransferProtocol += "://"
		}
		return shardTransferProtocol
	}

	return ""
}

func getDestinationFromConfig(cfg c.Config) (string, string, error) {
	blobStorageScheme := getTransferScheme(cfg)
	blobStorageBucket := cfg["settings.rebalance.blob_storage_bucket"].String()
	blobStoragePrefix := cfg["settings.rebalance.blob_storage_prefix"].String()
	blobStorageRegion := cfg["settings.rebalance.blob_storage_region"].String()

	if blobStorageBucket != "" && !strings.HasSuffix(blobStorageBucket, "/") {
		blobStorageBucket += "/"
	}

	destination := blobStorageScheme + blobStorageBucket + blobStoragePrefix
	if len(destination) == 0 {
		return "", blobStorageRegion, errors.New("Empty destination for shard rebalancer")
	}
	return destination, blobStorageRegion, nil
}

// Empty node batching is enabled in the following cases:
// a. Cluster is fully upgraded to post 7.6+
// b. The config "indexer.rebalance.enableEmptyNodeBatching" is set to true
// c. The destination node on which indexes are being built is empty
// d. There is atleast one DCP token for the destination node
//
// The main aim of empty node batching is to skip the destination node from serving
// scans while DCP builds are happening as DCP builds can saturate node resources
// and scan latencies can get impacted. For shard rebalance alone, this is not a
// problem as shard rebalance relies on shard copy which does not consume much
// resources (2-4 cores based on the perf experiments).
//
// However, if there are DCP tokens involved (either due to replica repair (or) upgrade
// cases), then if a shard is moved to a destination node, indexes on the shard start
// scans while there are DCP tokens yet to be processed, then the DCP builds can saturate
// node resources and impact scan latencies. Hence, shard rebalance will enable empty node
// batching for the shard tokens only if empty node batching is enabled and DCP transfer
// tokens are involved in the movement to destination node
func (sr *ShardRebalancer) enableEmptyNodeBatching() {

	if common.IsServerlessDeployment() {
		logging.Infof("ShardRebalancer::enableEmptyNodeBatching Empty node batching is not " +
			"applicable for serverless deployment")
		return
	}

	globalClusterVer := common.GetClusterVersion()
	if globalClusterVer < common.INDEXER_76_VERSION {
		logging.Infof("ShardRebalancer::enableEmptyNodeBatching Skip empty node batching "+
			"as cluster version is: %v", globalClusterVer)
		return
	}

	emptyNodeBatchingCfg := sr.config.Load()["rebalance.enableEmptyNodeBatching"].Bool()
	if !emptyNodeBatchingCfg {
		logging.Infof("ShardRebalancer::enableEmptyNodeBatching Skip empty node batching "+
			"as it is not enabled in config. Val: %v", emptyNodeBatchingCfg)
		return
	}

	emptyNodes := func() map[string]bool {
		if sr.globalTopology == nil {
			return nil
		}

		emptyNodes := make(map[string]bool)
		for _, metadata := range sr.globalTopology.Metadata {
			if len(metadata.IndexDefinitions) == 0 {
				emptyNodes[metadata.NodeUUID] = true
			}
		}
		return emptyNodes
	}()

	if len(emptyNodes) == 0 {
		logging.Infof("ShardRebalancer::enableEmptyNodeBatching Skip empty node batching " +
			"there are no empty nodes in the cluster")
		return
	}

	if len(sr.transferTokens) == 0 {
		logging.Infof("ShardRebalancer::enableEmptyNodeBatching No shard tokens generated for rebalance")
		return
	}

	for ttid, token := range sr.transferTokens {

		if _, ok := emptyNodes[token.DestId]; ok { // Check if destination node is empty
			// Enable empty node batching
			token.IsEmptyNodeBatch = true
			logging.Infof("ShardRebalancer::enableEmptyNodeBatching Enabled empty node batching "+
				"for token: %v, destId: %v", ttid, token.DestId)
		}
	}
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

				if rToken.ActiveRebalancer < c.SHARD_REBALANCER &&
					rToken.ActiveRebalancer != sr.rebalToken.ActiveRebalancer {

					l.Infof("ShardRebalancer::processShardTokens change in ActiveRebalancer from %v to %v",
						sr.rebalToken.ActiveRebalancer, rToken.ActiveRebalancer)
					// switch to DcpRebr
					sr.processRebalancerChange(rToken)
				}
			}
		}
	} else if strings.Contains(kve.Path, ShardTokenTag) {
		if kve.Value != nil {
			ttid, tt, err := decodeTransferToken(kve.Path, kve.Value, "ShardRebalancer",
				ShardTokenTag)

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
		if !common.IsServerlessDeployment() && sr.canMaintainShardAffinity {
			// because of eventual consistecy, it can happen that we receive DCP based TT first
			// and later get update of RebalToken which starts the DCP rebr and stop shard metakv
			// watches; ignore non-shard transfer tokens in such cases
			l.Warnf("ShardRebalancer::processShardTransferToken got non-(Shard Transfer Token) %v. skipping processing in Shard Rebalancer...",
				tt)
			return
		}
		errStr := "Transfer token is not for transferring shard"
		l.Fatalf("ShardRebalancer::processShardTransferToken %v. ttid: %v, tt: %v", errStr, ttid, tt)
		sr.setTransferTokenError(ttid, tt, errStr)
		if tt.MasterId != sr.nodeUUID {
			// allow master to consume this error
			return
		}
	}

	// "processed" var ensures only the incoming token state gets processed by this
	// call, as metakv will call parent processTokens again for each TT state change.
	var processed bool

	// When empty node batching is enabled, a notification will be sent via metaKV with
	// "pendingReady" flag set to true in transfer token. Master should always process
	// ShardTokenRecoverShard state in such a case
	if tt.MasterId == sr.nodeUUID {
		processed = sr.processShardTransferTokenAsMaster(ttid, tt)
	}

	if (tt.SourceId == sr.nodeUUID && !processed) || (tt.ShardTransferTokenState == c.ShardTokenDropOnSource) {
		processed = sr.processShardTransferTokenAsSource(ttid, tt)
	}

	if (tt.DestId == sr.nodeUUID && !processed) || (tt.ShardTransferTokenState == c.ShardTokenDropOnSource) {
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

		sr.updateEmptyNodeBatching(tt.IsEmptyNodeBatch)

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
			if sr.schedulingVersion >= common.PER_NODE_TRANSFER_LIMIT {
				sr.initiateDeploymentAwarePerNodeTransferAsMaster()
			} else {
				sr.initiateShardTransferAsMaster()
			}
		}
		return true

	case c.ShardTokenTransferShard:
		// Update the in-memory state but do not process the token
		// This will help to compute the rebalance progress
		sr.updateInMemToken(ttid, tt, "master")
		return false

	case c.ShardTokenRestoreShard:
		sr.updateInMemToken(ttid, tt, "master")

		if sr.schedulingVersion >= common.PER_NODE_TRANSFER_LIMIT {
			func() {
				sr.mu.Lock()
				defer sr.mu.Unlock()

				// Upload is completed on source node. Download starts on destination node
				// Remove token from active transfers for source node and add for
				// destination node
				sr.removeTokenFromActiveTransfersLOCKED(ttid, tt.SourceId)
				sr.addTokenToActiveTransfersLOCKED(ttid, tt.DestId)

				sr.initiateDeploymentAwarePerNodeTransferAsMaster()
			}()
		}

		return false

	case c.ShardTokenRecoverShard:
		sr.updateInMemToken(ttid, tt, "master")

		if tt.IsPendingReady {
			sr.updatePendingReadyTokens(ttid, tt)
			if sr.allTokensPendingReadyOrDone() {
				sr.transitionToDcpOrEndShardRebalance()
			}
		}

		if sr.schedulingVersion >= common.PER_NODE_TRANSFER_LIMIT {
			func() {
				sr.mu.Lock()
				defer sr.mu.Unlock()

				// In some cases, rebalance master can observe only Transfer -> Recover
				// transition missing Restore. This will lead to stale book-keeping
				// for source node and further tokens may not be scheduled. Clear
				// source book-keeping to avoid such inconsistencies.

				// Incase, master has observed Transfer -> Restore -> Recover, then
				// clearing book-keeping on source is a no-op
				sr.removeTokenFromActiveTransfersLOCKED(ttid, tt.SourceId)

				// Download is completed on destination node. Decrement the active
				// transfer count for destination so that new tokens can be scheduled
				sr.removeTokenFromActiveTransfersLOCKED(ttid, tt.DestId)

				sr.initiateDeploymentAwarePerNodeTransferAsMaster()
			}()
		}
		return false

	case c.ShardTokenMerge:
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

		// return false so that source can update its book-keeping if source and master are same
		return false

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

		func() {
			sr.mu.Lock()
			defer sr.mu.Unlock()
			if sr.schedulingVersion >= common.PER_NODE_TRANSFER_LIMIT {
				// Remove token from active transfers on source and destination
				sr.removeTokenFromActiveTransfersLOCKED(ttid, tt.SourceId)
				sr.removeTokenFromActiveTransfersLOCKED(ttid, tt.DestId)
				sr.initiateDeploymentAwarePerNodeTransferAsMaster()
			} else {
				// Decrement the batchSize as token has finished processing
				sr.currBatchSize--

				sr.initiateShardTransferAsMaster()
			}
		}()

		if sr.allTokensPendingReadyOrDone() {

			////////////// Testing code - Not used in production //////////////
			testcode.TestActionAtTag(sr.config.Load(), testcode.MASTER_SHARDTOKEN_ALL_TOKENS_PROCESSED)
			///////////////////////////////////////////////////////////////////

			if sr.cb.progress != nil {
				sr.cb.progress(sr.shardProgressRatio, sr.cancel)
			}
			l.Infof("ShardRebalancer::processShardTransferTokenAsMaster No Tokens Found. Mark Done.")

			dcpRebalanceStarted := sr.isDcpRebalanceStarted()

			// If DCP rebalance is not already started, then start DCP rebalance as there can be DCP
			// tokens waiting to be processed. Once DCP rebalance finishes, shard rebalance would finish
			// via: processRebalancerChange() method (or) changes the state of pendingReady tokens to
			// shardTokenMerge would move all tokens to ShardTokenDeleted and rebalance would finish
			if dcpRebalanceStarted == false {
				sr.transitionToDcpOrEndShardRebalance()
			} else {
				// Either empty node batching is disabled (or) DCP rebalance is already started
				// Finish rebalance
				sr.cancelMetakv()
				go sr.finishRebalance(nil)
			}
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
		sr.updateInstsTransferPhase(ttid, tt, common.RebalanceInitated)
		sr.updateEmptyNodeBatching(tt.IsEmptyNodeBatch)

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

	case c.ShardTokenRestoreShard, c.ShardTokenRecoverShard:
		// Update in-mem book keeping and do not process the token
		sr.updateInMemToken(ttid, tt, "source")

		// Notify lifecycle manager that shard transfer is done.
		// This will allow lifecycle manager to unblock any drop
		// index commands that have been issued while transfer is
		// in progress
		sr.updateInstsTransferPhase(ttid, tt, c.RebalanceTransferDone)

		// in new rebalance scheme, we can have some indices being created on a shard being copied
		// via DCP rebalance. with empty node batching in mind, we should unlock copied shards
		// to avoid index creation failure when DCP rebalance tries to create the index on a shard
		// on source node
		sr.unlockShardsOnSourcePostTransfer(ttid, tt)

		return false

	case c.ShardTokenReady:
		sr.updateInMemToken(ttid, tt, "source")
		if sr.isDropOnSourceTokenPosted(ttid) {
			logging.Infof("ShardRebalancer::processShardTransferTokenAsSource Queuing indexes for drop as DropOnSource "+
				"token is posted for this tokenId: %v", ttid)
			return sr.checkAndQueueTokenForDrop(tt, ttid, tt.SiblingTokenId)
		}
		return false

	case c.ShardTokenDropOnSource:
		sr.updateInMemToken(ttid, tt, "source")

		if sr.getSourceIdForTokenId(tt.SourceTokenId) != sr.nodeUUID &&
			sr.getSourceIdForTokenId(tt.SiblingTokenId) != sr.nodeUUID {
			return false // return as the dropOnSource token was not meant for the tokens on this node
		}

		if sr.shouldProcessDropOnSource(tt) == false {
			logging.Infof("ShardRebalancer::processShardTransferTokenAsSource Skipping dropOnSource token: %v as "+
				"either the source(%v)/sibling token(%v) is not in Ready state", ttid, tt.SourceTokenId, tt.SiblingTokenId)
			return true
		}

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
		return false

	case c.ShardTokenCommit:
		// If node sees a commit token, then DDL can be allowed on
		// the bucket. It is necessary for source to update its
		// book-keeping. Otherwise, source will reject prepare create
		// request thinking that the rebalance of bucket is not done.
		// During the plan phase (which happens after prepare phase),
		// planner will decide which node the index will land-on
		sr.updateInMemToken(ttid, tt, "source")
		sr.updateInstsTransferPhase(ttid, tt, common.RebalanceDone)

		return false // Return false as this is source node. Destination node should also process the token

	default:
		return false
	}
}

func (sr *ShardRebalancer) checkAndQueueTokenForDrop(token *c.TransferToken, sourceId, siblingId string) bool {
	sourceToken := sr.getTokenById(sourceId)
	if sourceToken != nil && sourceToken.TransferMode == common.TokenTransferModeCopy && siblingId == "" { // only replica repair case
		if !sourceToken.IsEmptyNodeBatch || c.IsServerlessDeployment() {
			// Since this is replica repair, do not drop the shard data. Only cleanup the transferred data
			// and unlock the shards
			l.Infof("ShardRebalancer::checkAndQueueTokenForDrop Initiating shard unlocking for token: %v", sourceId)

			unlockShards(sourceToken.ShardIds, sr.supvMsgch)
		}
		sr.initiateShardTransferCleanup(sourceToken.ShardPaths, sourceToken.Destination, sourceToken.Region, sourceId, sourceToken, nil, false)

		sourceToken.ShardTransferTokenState = common.ShardTokenCommit
		setTransferTokenInMetakv(sourceId, sourceToken)
		return true

	} else if sourceToken != nil && sourceToken.TransferMode == common.TokenTransferModeMove { // Single node swap rebalance (or) single node swap + replica repair in same rebalance

		l.Infof("ShardRebalancer::checkAndQueueTokenForDrop Queuing token: %v for drop", sourceId)
		sr.queueDropShardRequests(sourceId)

		// Skip unlocking shards as the shards are going to be destroyed
		// Clean-up any transferred data - Currently, plasma cleans up transferred data on a successful
		// restore. This is only a safety operation from indexer. Coming to this code means that restore
		// is successful and this operation becomes a no-op
		sr.initiateShardTransferCleanup(sourceToken.ShardPaths, sourceToken.Destination, sourceToken.Region, sourceId, sourceToken, nil, false)

		siblingToken := sr.getTokenById(siblingId)
		if siblingToken != nil && siblingToken.TransferMode == common.TokenTransferModeCopy && siblingToken.SourceId == sr.nodeUUID {
			siblingToken.ShardTransferTokenState = common.ShardTokenCommit
			setTransferTokenInMetakv(siblingId, siblingToken)
		}
		return true

	} else {
		l.Infof("ShardRebalancer::checkAndQueueTokenForDrop Skipping token: %v for drop", sourceId)
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

func (sr *ShardRebalancer) getAcceptedTokenById(ttid string) *c.TransferToken {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	if token, ok := sr.acceptedTokens[ttid]; ok && token != nil {
		return token.Clone()
	}
	return nil // Return error as the default state
}

func (sr *ShardRebalancer) startShardTransfer(ttid string, tt *c.TransferToken) {

	if !sr.addToWaitGroup() {
		return
	}
	defer sr.wg.Done()

	// If rebalance transfer fails due to rollbackToZero, then transfer is attempted
	// for upto the limit specified by the config "indexer.rebalance.serverless.transferRetries"
	retryCount := 0
	maxRetries := sr.config.Load()["rebalance.serverless.transferRetries"].Int()

loop:
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
		region:      tt.Region,

		cancelCh:   sr.cancel,
		doneCh:     sr.done,
		respCh:     respCh,
		progressCh: progressCh,

		newAlternateShardIds: tt.NewAlternateShardIds,
	}

	if sr.canMaintainShardAffinity {
		msg.isPeerTransfer = true
		msg.authCallback = func(req *http.Request) error {
			return cbauth.SetRequestAuth(req)
		}
		host, _, _ := net.SplitHostPort(tt.DestHost)
		msg.tlsConfig, err = getTLSConfigWithCeftificates(host)

		if err != nil {
			l.Errorf("ShardRebalancer::startShardTransfer failed to setup TLS config for transfer token %v (host %v) with error %v",
				ttid, tt.DestHost, err)
			sr.setTransferTokenError(ttid, tt, err.Error())
			return
		}

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

			hasErr := false
			for shardId, err := range errMap {
				if err != nil {
					hasErr = true

					l.Errorf("ShardRebalancer::startShardTransfer Observed error during trasfer"+
						" for destination: %v, region: %v, shardId: %v, shardPaths: %v, err: %v. Initiating transfer clean-up",
						tt.Destination, tt.Region, shardId, shardPaths, err)

					unlockShards(tt.ShardIds, sr.supvMsgch)

					// Invoke clean-up for all shards even if error is observed for one shard transfer
					if err == ErrIndexRollback {
						if retryCount > maxRetries { // all retries exhausted and still transfer could not be completed
							sr.initiateShardTransferCleanup(shardPaths, tt.Destination, tt.Region, ttid, tt, err, false)
							sr.setTransferTokenError(ttid, tt, err.Error())
							return
						} else {
							retryCount++
							// Clean up the transferred data with nil error and retry transfer
							// If transfer could not be completed after configured attempts, then set
							// error in transfer token
							sr.initiateShardTransferCleanup(shardPaths, tt.Destination, tt.Region, ttid, tt, nil, true)
							goto loop
						}
					} else if strings.Contains(err.Error(), "context canceled") {
						continue // Do not set this error in transfer token. Look for other errors
					} else {
						sr.initiateShardTransferCleanup(shardPaths, tt.Destination, tt.Region, ttid, tt, err, false)
						sr.setTransferTokenError(ttid, tt, err.Error())
						return
					}
				}
			}

			// All errors are "context canceled" errors. Use the same to update transfer token
			if hasErr {
				for _, err := range errMap {
					if err != nil {
						sr.initiateShardTransferCleanup(shardPaths, tt.Destination, tt.Region, ttid, tt, err, false)
						sr.setTransferTokenError(ttid, tt, err.Error())
						return
					}
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
	destination, region string, ttid string, tt *c.TransferToken, err error, syncCleanup bool) {

	l.Infof("ShardRebalancer::initiateShardTransferCleanup Initiating clean-up for ttid: %v, "+
		"destination: %v, region: %v", ttid, destination, region)

	start := time.Now()
	respCh := make(chan bool)
	msg := &MsgShardTransferCleanup{
		destination:     destination,
		region:          region,
		rebalanceId:     sr.rebalToken.RebalId,
		transferTokenId: ttid,
		respCh:          respCh,
		syncCleanup:     syncCleanup,
	}

	if sr.canMaintainShardAffinity {
		msg.isPeerTransfer = true
		msg.authCallback = func(req *http.Request) error {
			return cbauth.SetRequestAuth(req)
		}
		host, _, _ := net.SplitHostPort(tt.DestHost)
		msg.tlsConfig, err = getTLSConfigWithCeftificates(host)
		if err != nil {
			l.Errorf("ShardRebalancer::initiateShardTransferCleanup failed to get TLS config for transfer token %v (host %v) with error %v",
				ttid, tt.DestHost, err)
			sr.setTransferTokenError(ttid, tt, err.Error())
			return
		}
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

func (sr *ShardRebalancer) shouldProcessDropOnSource(tt *c.TransferToken) bool {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	if tt.ShardTransferTokenState != c.ShardTokenDropOnSource {
		return false
	}

	// If token is in ready state, process the token
	// Otherwise, skip processing the dropOnSource token
	srcToken, ok := sr.sourceTokens[tt.SourceTokenId]
	if ok && srcToken.ShardTransferTokenState == c.ShardTokenReady && srcToken.SourceId == sr.nodeUUID {
		return true
	}

	// If sibling token is in ready state, process the token
	// Otherwise, skip processing the dropOnSource token
	sibToken, ok := sr.sourceTokens[tt.SiblingTokenId]
	if ok && sibToken.ShardTransferTokenState == c.ShardTokenReady && sibToken.SourceId == sr.nodeUUID {
		return true
	}

	return false
}

func (sr *ShardRebalancer) isDropOnSourceTokenPosted(ttid string) bool {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	for _, token := range sr.sourceTokens {
		if token.ShardTransferTokenState != c.ShardTokenDropOnSource {
			continue
		}

		if token.SourceTokenId == ttid || token.SiblingTokenId == ttid {
			return true
		}
	}
	return false
}

func (sr *ShardRebalancer) getSourceIdForTokenId(ttid string) string {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	if tt, ok := sr.sourceTokens[ttid]; ok {
		return tt.SourceId
	}
	return ""
}

func (sr *ShardRebalancer) getDestinationIdForTokenId(ttid string) string {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	if tt, ok := sr.acceptedTokens[ttid]; ok {
		return tt.DestId
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
		sr.updateInstsTransferPhase(ttid, tt, common.RebalanceInitated)
		sr.updateEmptyNodeBatching(tt.IsEmptyNodeBatch)

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
		sr.updateInstsTransferPhase(ttid, tt, c.RebalanceTransferDone)

		go sr.startShardRestore(ttid, tt)

		return true

	case c.ShardTokenRecoverShard:
		sr.updateInMemToken(ttid, tt, "dest")

		// If pendingReady is set to true, then recovery is already finished.
		// Skip recovering again
		if tt.IsEmptyNodeBatch == false || tt.IsPendingReady == false {
			if common.IsServerlessDeployment() {
				go sr.startShardRecovery(ttid, tt)
			} else {
				go sr.startShardRecoveryNonServerless(ttid, tt)
			}
		}

		return true

	case c.ShardTokenMerge:
		sr.updateInMemToken(ttid, tt, "dest")
		go sr.updateRStateToActive(ttid, tt)

		return true

	case c.ShardTokenDropOnSource:
		sr.updateInMemToken(ttid, tt, "dest")
		// Process write billing only for serverless deployments
		if c.IsServerlessDeployment() == false {
			return false
		}

		enableWriteBilling := func(token *c.TransferToken) {
			msg := &MsgMeteringUpdate{
				mType:   METERING_MGR_START_WRITE_BILLING,
				InstIds: make([]c.IndexInstId, 0),
				respCh:  make(chan error),
			}
			msg.InstIds = append(msg.InstIds, token.InstIds...)

			sr.supvMsgch <- msg
			<-msg.respCh
		}

		// Enable metering on destination node after DropOnSource token is posted
		// After this point shard is assumed to be moved to destination
		if sr.getDestinationIdForTokenId(tt.SourceTokenId) == sr.nodeUUID {
			srcToken := sr.getAcceptedTokenById(tt.SourceTokenId)
			enableWriteBilling(srcToken)
			return true
		}
		if sr.getDestinationIdForTokenId(tt.SiblingTokenId) == sr.nodeUUID {
			sibToken := sr.getAcceptedTokenById(tt.SiblingTokenId)
			enableWriteBilling(sibToken)
			return true
		}
		return false

	case c.ShardTokenCommit:
		sr.updateInMemToken(ttid, tt, "dest")

		// When empty node batching is enabled, then destination will restore and
		// unlock the shards as soon as recovery of all indexes is done. This is done
		// to facilitate placement of DCP indexes on the shard that is moved as a
		// part of rebalance.
		if tt.IsEmptyNodeBatch == false {
			// Destination will call RestoreShardDone for the shardId involved in the
			// rebalance as coming here means that rebalance is successful for the
			// tenant. After RestoreShardDone, the shards will be unlocked
			restoreShardDone(tt.ShardIds, sr.supvMsgch)

			// Unlock the shards that are locked before initiating recovery
			unlockShards(tt.ShardIds, sr.supvMsgch)
		}

		// If nodes sees a commit token, then DDL can be allowed on
		// the bucket
		sr.updateInstsTransferPhase(ttid, tt, common.RebalanceDone)

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
		shardPaths:    tt.ShardPaths,
		taskId:        sr.rebalToken.RebalId,
		transferId:    ttid,
		destination:   tt.Destination,
		region:        tt.Region,
		instRenameMap: tt.InstRenameMap,

		cancelCh:   sr.cancel,
		doneCh:     sr.done,
		respCh:     respCh,
		progressCh: progressCh,
	}

	if sr.canMaintainShardAffinity {
		msg.isPeerTransfer = true
		msg.authCallback = cbauth.SetRequestAuth
		var err error

		host, _, _ := net.SplitHostPort(tt.DestHost)
		msg.tlsConfig, err = getTLSConfigWithCeftificates(host)
		if err != nil {
			l.Errorf("ShardRebalancer::startShardRestore failed to get TLS config for transfer token %v (host %v) with error %v",
				ttid, tt.DestHost, err)
			sr.setTransferTokenError(ttid, tt, err.Error())
			return
		}
	}

	sr.supvMsgch <- msg

	for {
		select {
		case respMsg := <-respCh:

			elapsed := time.Since(start).Seconds()
			l.Infof("ShardRebalancer::startRestoreShard Received response for restore of "+
				"shardIds: %v, ttid: %v, elapsed(sec): %v", tt.ShardIds, ttid, elapsed)

			cleanupAndSetErr := func(shardId common.ShardId, shardPaths map[common.ShardId]string, err error) {

				// If there are any errors during restore, the restored data on local file system
				// is cleaned by the destination node. The data on S3 will be cleaned by the rebalance
				// source node. Rebalance source node is the only writer (insert/delete) of data on S3
				l.Errorf("ShardRebalancer::startRestoreShard Observed error during trasfer"+
					" for destination: %v, region: %v, shardId: %v, shardPaths: %v, err: %v. Initiating transfer clean-up",
					tt.Destination, tt.Region, shardId, shardPaths, err)

				// Invoke clean-up for all shards even if error is observed for one shard transfer
				sr.initiateLocalShardCleanup(ttid, shardPaths, tt)
				sr.setTransferTokenError(ttid, tt, err.Error())
				return

			}

			msg := respMsg.(*MsgShardTransferResp)
			errMap := msg.GetErrorMap()
			shardPaths := msg.GetShardPaths()

			hasErr := false
			for shardId, err := range errMap {
				if err != nil {
					hasErr = true

					// context canceled error can be a by-product of some other
					// errors that happened during transfer/restore. Do not
					// use "context canceled" errors as the first preference for setting
					// transfer token errors. If there is no other error type, the
					// use this error
					if strings.Contains(err.Error(), "context canceled") {
						continue
					} else {
						cleanupAndSetErr(shardId, shardPaths, err)
						return
					}
				}
			}

			if hasErr {
				// Coming here means that all errors are "context canceled" errors
				// Use the same to update transfer token
				for shardId, err := range errMap {
					if err != nil { // Use the first error
						cleanupAndSetErr(shardId, shardPaths, err)
						return
					}
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

func (sr *ShardRebalancer) createDeferredIndex(defn *c.IndexDefn,
	ttid string, tt *c.TransferToken, isDeferred, pendingCreate bool,
	recoveryListener chan c.IndexInstId) (bool, error) {

	// TODO: Use ShardIds for desintaion when changing the shardIds
	if len(defn.ShardIdsForDest) == 0 {
		defn.ShardIdsForDest = make(map[c.PartitionId][]c.ShardId)
	}
	for _, partnId := range defn.Partitions {
		if len(defn.ShardIdsForDest[partnId]) == 0 {
			defn.ShardIdsForDest[partnId] = tt.ShardIds[:len(defn.AlternateShardIds[partnId])]
		}
	}

	if !pendingCreate {

		if skip, err := sr.postRecoverIndexReq(*defn, ttid, tt); err != nil {
			return false, err
		} else if skip {
			// bucket (or) scope (or) collection (or) index are dropped.
			// Continue instead of failing rebalance
			return true, nil
		}
	} else {
		// For pendingCreate indexes, explicitly set deferred to true so that shard rebalancer
		// will only create the index metadata and DDL service manager will take care of building
		// the index on destination node after rebalance is done
		defn.Deferred = true

		if skip, err := sr.postCreateIndexReq(*defn, ttid, tt); err != nil {
			return false, err
		} else if skip {
			// bucket (or) scope (or) collection (or) index are dropped.
			// Continue instead of failing rebalance
			return true, nil
		}
	}

	// For deferred indexes and pendingCreate, build command is not required
	// wait for index state to move to Ready phase
	if isDeferred || pendingCreate {

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

		if err := sr.waitForIndexState(c.INDEX_STATE_READY, deferredInsts, ttid, tt, recoveryListener); err != nil {
			return false, err
		}

		sr.destTokenToMergeOrReady(defn.InstId, defn.RealInstId, ttid, tt, &partnMergeWaitGroup)

		// Wait for the deferred partition index merge to happen
		// This is essentially a no-op for non-partitioned indexes
		partnMergeWaitGroup.Wait()
	}
	return false, nil
}

func (sr *ShardRebalancer) startShardRecovery(ttid string, tt *c.TransferToken) {

	if !sr.addToWaitGroup() {
		return
	}
	defer sr.wg.Done()

	if tt.IsEmptyNodeBatch == false {
		select {
		case sr.waitQ <- true:
		case <-sr.cancel:
			return
		case <-sr.done:
			return
		}
	}

	logging.Infof("ShardRebalancer::startShardRecovery Starting to recover index instance for ttid: %v", ttid)

	defer func() {
		if tt.IsEmptyNodeBatch == false {
			select {
			case <-sr.waitQ:
			case <-sr.cancel:
				return
			case <-sr.done:
				return
			}
		}
		logging.Infof("ShardRebalancer::startShardRecovery Done with recovery for ttid: %v", ttid)
	}()

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

	pendingCreateDefnList := make(map[common.IndexDefnId]*c.IndexDefn, 0)

	for cid, defns := range groupedDefns {
		buildDefnIdList := make([]client.IndexIdList, 3)
		nonDeferredInsts := make([]map[c.IndexInstId]bool, 3)
		defnIdToInstIdMap := make(map[common.IndexDefnId]common.IndexInstId)
		// Post recover index request for all non-deferred definitions
		// of this collection. Once all indexes are recovered, they can be
		// built in a batch
		for _, defn := range defns {
			var partitions []common.PartitionId
			var versions []int
			for i, partnId := range defn.Partitions {
				if len(defn.ShardIdsForDest[partnId]) == 0 {
					logging.Infof("ShardRebalancer::StartShardRecovery: Skipping partition for defnId: %v, partnId: %v, all partitions: %v "+
						"for later processing", defn.DefnId, partnId, defn.Partitions)

					if _, ok := pendingCreateDefnList[defn.DefnId]; !ok {
						clone := defn.Clone()
						clone.Partitions = nil                                      // reset partitions list
						clone.Versions = nil                                        // reset versions list
						clone.ShardIdsForDest = make(map[c.PartitionId][]c.ShardId) // reset shardIds list
						clone.AlternateShardIds = make(map[c.PartitionId][]string)  // reset alternate shardIds list
						pendingCreateDefnList[defn.DefnId] = clone
					}

					pendingCreateDefnList[defn.DefnId].Partitions = append(pendingCreateDefnList[defn.DefnId].Partitions, partnId)
					pendingCreateDefnList[defn.DefnId].Versions = append(pendingCreateDefnList[defn.DefnId].Versions, defn.Versions[i])
				} else {
					partitions = append(partitions, defn.Partitions[i])
					versions = append(versions, defn.Versions[i])
				}
			}

			// All partitions are pendingCreate - They will be processed later
			if len(partitions) == 0 {
				continue
			} else {
				defn.Partitions = partitions
				defn.Versions = versions
			}

			isDeferred := defn.Deferred &&
				(defn.InstStateAtRebal == c.INDEX_STATE_CREATED ||
					defn.InstStateAtRebal == c.INDEX_STATE_READY)

			if skip, err := sr.createDeferredIndex(&defn, ttid, tt, isDeferred, false, nil); err != nil {
				setErrInTransferToken(err)
				return
			} else if skip {
				// bucket (or) scope (or) collection (or) index are dropped.
				// Continue instead of failing rebalance
				continue
			}

			// "createDeferredIndex" method will wait for index state to move to READY phase
			// for deferred indexes
			// For non-deferred indexes, wait for index state to move to RECOVERED phase
			if !isDeferred {
				var currIndex int
				nonDeferredInsts, buildDefnIdList, currIndex = populateInstsAndDefnIds(nonDeferredInsts, buildDefnIdList, defn, defnIdToInstIdMap)

				////////////// Testing code - Not used in production //////////////
				testcode.TestActionAtTag(sr.config.Load(), testcode.DEST_SHARDTOKEN_DURING_NON_DEFERRED_INDEX_RECOVERY)
				///////////////////////////////////////////////////////////////////

				if err := sr.waitForIndexState(c.INDEX_STATE_RECOVERED, nonDeferredInsts[currIndex], ttid, tt, nil); err != nil {
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

				retry := true
				lastlog := time.Now()
				for retry {
					skipDefns, retryInsts, err := sr.postBuildIndexesReq(buildDefnIdList[i], ttid, tt)
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
					retry = len(retryInsts) > 0
					if retry {
						if time.Since(lastlog) > time.Duration(60*time.Second) {
							logging.Infof("ShardRebalancer::startShardRecovery Retrying index build for insts: %v", retryInsts)
							lastlog = time.Now()
						}
						// Retry build after 5 seconds
						time.Sleep(5 * time.Second)
					}
				}

				logging.Infof("ShardRebalancer::startShardRecovery Waiting for index state to "+
					"become active for insts: %v", nonDeferredInsts[i])

				if err := sr.waitForIndexState(c.INDEX_STATE_ACTIVE, nonDeferredInsts[i], ttid, tt, nil); err != nil {
					setErrInTransferToken(err)
					return
				}
			}
		}
	}

	// Create metadata for all pendingCreate indexes on destination node
	// Do not build the indexes as this can lead to long rebalance times.
	// DDL service manager will take care of building the indexes after
	// rebalance is done
	for _, defn := range pendingCreateDefnList {
		if skip, err := sr.createDeferredIndex(defn, ttid, tt, false, true, nil); err != nil {
			setErrInTransferToken(err)
			return
		} else if skip {
			continue
		}
	}

	elapsed := time.Since(start).Seconds()
	l.Infof("ShardRebalancer::startShardRecovery Finished recovery of all indexes in ttid: %v, elapsed(sec): %v", ttid, elapsed)

	if tt.IsEmptyNodeBatch {
		tt.IsPendingReady = true
		// As recovery is complete, restore and unlock the shards. This will facilitate
		// the DCP indexes to be placed on the shard (incase of replica-repair) that is
		// just moved to destination node.

		// restore the shard as recovery is complete
		restoreShardDone(tt.ShardIds, sr.supvMsgch)

		// Unlock the shards that are locked before initiating recovery
		unlockShards(tt.ShardIds, sr.supvMsgch)

		l.Infof("ShardRebalancer::startShardRecovery Set pendingReady to true as empty node batching is enabled for token: %v", ttid)
		setTransferTokenInMetakv(ttid, tt)
	} else {
		// Coming here means that all indexes are actively build without any error
		// Move the transfer token state to Ready
		tt.ShardTransferTokenState = c.ShardTokenReady
		setTransferTokenInMetakv(ttid, tt)
	}
}

// startShardRecoveryNonServerless deviates from startShardRecovery in the
// following ways:
//
// a. Batches index builds across multiple transfer tokens. This will help
// to group indexes beloning to same collection across multiple transfer
// tokens to be built at the same time.
//
// b. Avoids waitQ throttling - startShardRecovery skips waitQ throttling
// when empty node batching is enabled and enforces it when empty node
// batching is disabled. Both are not optimal. When waitQ throttling is
// disabled, then indexer can establish INIT_STREAMs for 100's of collections
// at the same time - overloading KV with a pool of connections. When waitQ
// throttling is enabled, indexes beloning to only 2 collections will be
// recovered at a time (while indexer can potentially establish upto 10
// streams).
//
// Indexer will now enforce maxParallelCollectionBuilds for building recovered
// indexes for non-serverless deployments. Hence, waitQ throttling is disabled
//
// c. Consider only ACTIVE instances in transfer tokens. In serverless, with DDL
// during rebalance support, rebalancer has to consider the INITIAL, CATCHUP and
// ACTIVE instances. Each of these indexes are built in a separate batch so that
// the restart timestamp of all indexes sharing same state are with in 10min disk
// snapshot boundary. For non-serverless, as DDL during rebalance is not supported,
// there is no need to handle this additional complexity
func (sr *ShardRebalancer) startShardRecoveryNonServerless(ttid string, tt *c.TransferToken) {
	if !sr.addToWaitGroup() {
		return
	}
	defer sr.wg.Done()

	start := time.Now()
	logging.Infof("ShardRebalancer::startShardRecovery Starting to recover index instance for ttid: %v", ttid)

	defer func() {
		logging.Infof("ShardRebalancer::startShardRecovery Done with recovery for ttid: %v, elapsed: %v", ttid, time.Since(start))
	}()

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

	recoveryListener := make(chan c.IndexInstId, 2*len(tt.IndexInsts))

	// Group index definitions for each collection
	groupedDefns := groupInstsPerColl(tt)
	for _, defns := range groupedDefns {
		for _, defn := range defns {

			isDeferred := defn.Deferred && (defn.InstStateAtRebal == c.INDEX_STATE_CREATED ||
				defn.InstStateAtRebal == c.INDEX_STATE_READY)

			if skip, err := sr.createDeferredIndex(&defn, ttid, tt, isDeferred, false, recoveryListener); err != nil {
				setErrInTransferToken(err)
				return
			} else if skip {
				// bucket (or) scope (or) collection (or) index are dropped.
				// Continue instead of failing rebalance. Add to dropped insts
				sr.addInstToDroppedInsts(&defn)
				continue
			}

			// "createDeferredIndex" method will wait for index state to move to READY phase
			// for deferred indexes
			// For non-deferred indexes, wait for index state to move to RECOVERED phase
			if !isDeferred {
				////////////// Testing code - Not used in production //////////////
				testcode.TestActionAtTag(sr.config.Load(), testcode.DEST_SHARDTOKEN_DURING_NON_DEFERRED_INDEX_RECOVERY)
				///////////////////////////////////////////////////////////////////

				recoveryInsts := make(map[c.IndexInstId]bool)
				if defn.RealInstId != 0 {
					recoveryInsts[defn.RealInstId] = true
				} else {
					recoveryInsts[defn.InstId] = true
				}

				if err := sr.waitForIndexState(c.INDEX_STATE_RECOVERED, recoveryInsts, ttid, tt, recoveryListener); err != nil {
					sr.setTransferTokenError(ttid, tt, err.Error())
					return
				}
			}
		}
	}

	nonDeferredInsts := make(map[c.IndexInstId]bool)
	// At this point, indexes belonging to all collections are recovered
	// Post build reqs to batchBuildReqCh
	for _, defns := range groupedDefns {
		for _, defn := range defns {
			isDeferred := defn.Deferred && (defn.InstStateAtRebal == c.INDEX_STATE_CREATED ||
				defn.InstStateAtRebal == c.INDEX_STATE_READY)

			if !isDeferred {

				if defn.RealInstId != 0 {
					nonDeferredInsts[defn.RealInstId] = true
				} else {
					nonDeferredInsts[defn.InstId] = true
				}

				req := &batchBuildReq{
					instId:     defn.InstId,
					realInstId: defn.RealInstId,
					defn:       defn,
					ttid:       ttid,
					tt:         tt,
					listenerCh: recoveryListener,
				}

				sr.batchBuildReqCh <- req // queue the build request for further processing
			}
		}
	}

	// Wait for all indexes to reach ACTIVE state
	if err := sr.waitForIndexState(c.INDEX_STATE_ACTIVE, nonDeferredInsts, ttid, tt, recoveryListener); err != nil {
		setErrInTransferToken(err)
		return
	}

	if tt.IsEmptyNodeBatch {

		tt.IsPendingReady = true
		// As recovery is complete, restore and unlock the shards. This will facilitate
		// the DCP indexes to be placed on the shard (incase of replica-repair) that is
		// just moved to destination node.
		// restore the shard as recovery is complete
		restoreShardDone(tt.ShardIds, sr.supvMsgch)

		// Unlock the shards that are locked before initiating recovery
		unlockShards(tt.ShardIds, sr.supvMsgch)

		l.Infof("ShardRebalancer::startShardRecovery Set pendingReady to true as empty node batching is enabled for token: %v", ttid)
		setTransferTokenInMetakv(ttid, tt)
	} else {
		// Coming here means that all indexes are actively build without any error
		// Move the transfer token state to Ready
		tt.ShardTransferTokenState = c.ShardTokenReady
		setTransferTokenInMetakv(ttid, tt)
	}
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
		l.Infof("ShardRebalancer::postRecoverIndexReq rebalance cancel received")
		return false, ErrRebalanceCancel // return for now. Cleanup will take care of dropping the index instances

	case <-sr.done:
		l.Infof("ShardRebalancer::postRecoverIndexReq rebalance done received")
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

func (sr *ShardRebalancer) postBuildIndexesReq(defnIdList client.IndexIdList, ttid string, tt *c.TransferToken) (map[common.IndexDefnId]bool, map[common.IndexInstId]bool, error) {
	select {
	case <-sr.cancel:
		l.Infof("ShardRebalancer::postBuildIndexesReq rebalance cancel received")
		return nil, nil, ErrRebalanceCancel // return for now. Cleanup will take care of dropping the index instances

	case <-sr.done:
		l.Infof("ShardRebalancer::postBuildIndexesReq rebalance done received")
		return nil, nil, ErrRebalanceDone // return for now. Cleanup will take care of dropping the index instances

	default:
		url := "/buildRecoveredIndexesRebalance"

		resp, err := postWithHandleEOF(defnIdList, sr.localaddr, url, "ShardRebalancer::postBuildIndexesReq")
		if err != nil {
			logging.Errorf("ShardRebalancer::postBuildIndexesReq Error observed when posting build indexes request, "+
				"defnIdList: %v, err: %v", defnIdList.DefnIds, err)
			return nil, nil, err
		}

		response := new(IndexResponse)
		if err := convertResponse(resp, response); err != nil {
			l.Errorf("ShardRebalancer::postBuildIndexesReq Error unmarshal response for defnIdList: %v, "+
				"url: %v, err: %v", defnIdList.DefnIds, sr.localaddr+url, err)
			return nil, nil, err
		}

		if response.Error != "" {
			skipDefns, retryInsts, err := unmarshalAndProcessBuildReqResponse(response.Error, defnIdList.DefnIds)
			if err != nil { // Error while unmarshalling - Return the error to caller and fail rebalance
				l.Errorf("ShardRebalancer::postBuildIndexesReq Error received for defnIdList: %v, err: %v",
					defnIdList.DefnIds, response.Error)
				return nil, nil, errors.New(response.Error)
			} else {
				return skipDefns, retryInsts, nil
			}
		}
	}
	return nil, nil, nil
}

func unmarshalAndProcessBuildReqResponse(errStr string, defnIdList []uint64) (map[common.IndexDefnId]bool, map[common.IndexInstId]bool, error) {
	errMap := make(map[common.IndexDefnId]string)

	err := json.Unmarshal([]byte(errStr), &errMap)
	if err != nil {
		logging.Errorf("ShardRebalancer::postBuildIndexesReq Unmarshal of errStr failed. "+
			"defnIdList: %v, err: %v, errStr: %v", defnIdList, err, errStr)
		return nil, nil, err
	}

	skipDefns := make(map[common.IndexDefnId]bool)
	retryInsts := make(map[common.IndexInstId]bool)

	for instOrDefnId, buildErr := range errMap {
		if isIndexDeletedDuringRebal(buildErr) || isIndexNotFoundRebal(buildErr) {
			skipDefns[instOrDefnId] = true
		}

		if isBuildAlreadyInProgress(buildErr) {
			retryInsts[c.IndexInstId(instOrDefnId)] = true
		}
	}
	return skipDefns, retryInsts, nil
}

func (sr *ShardRebalancer) postCreateIndexReq(indexDefn common.IndexDefn, ttid string, tt *common.TransferToken) (bool, error) {
	select {
	case <-sr.cancel:
		l.Infof("ShardRebalancer::postCreateIndexReq rebalance cancel received")
		return false, ErrRebalanceCancel // return for now. Cleanup will take care of dropping the index instances

	case <-sr.done:
		l.Infof("ShardRebalancer::postCreateIndexReq rebalance done received")
		return false, ErrRebalanceDone // return for now. Cleanup will take care of dropping the index instances

	default:
		url := "/createIndexRebalance"

		resp, err := postWithHandleEOF(indexDefn, sr.localaddr, url, "ShardRebalancer::postCreateIndexReq")
		if err != nil {
			logging.Errorf("ShardRebalancer::postCreateIndexReq Error observed when posting recover index request, "+
				"indexDefnId: %v, err: %v", indexDefn.DefnId, err)
			return false, err
		}

		response := new(IndexResponse)
		if err := convertResponse(resp, response); err != nil {
			l.Errorf("ShardRebalancer::postCreateIndexReq Error unmarshal response for indexDefnId: %v, "+
				"url: %v, err: %v", indexDefn.DefnId, sr.localaddr+url, err)
			return false, err
		}

		if response.Error != "" {
			if isMissingBSC(response.Error) || isIndexDeletedDuringRebal(response.Error) {
				logging.Infof("ShardRebalancer::postCreateIndexReq scope/collection/index is "+
					"deleted during rebalance. Skipping the indexDefnId: %v from further processing. "+
					"indexDefnId: %v, Error: %v", indexDefn.DefnId, response.Error)
				return true, nil
			}
			l.Errorf("ShardRebalancer::postCreateIndexReq Error received for indexDefnId: %v, err: %v",
				indexDefn.DefnId, response.Error)
			return false, errors.New(response.Error)
		}
	}
	return false, nil
}

func (sr *ShardRebalancer) waitForIndexState(expectedState c.IndexState,
	processedInsts map[c.IndexInstId]bool, ttid string, tt *c.TransferToken,
	recoveryListener chan c.IndexInstId) error {

	if recoveryListener == nil {
		recoveryListener = make(chan c.IndexInstId)
	}

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

		select {
		case <-sr.cancel:
			l.Infof("ShardRebalancer::waitForIndexState Cancel Received")
			return ErrRebalanceCancel
		case <-sr.done:
			l.Infof("ShardRebalancer::waitForIndexState Done Received")
			return ErrRebalanceDone

			// If bucket/scope/collection/index are dropped during rebalance,
			// then "processBatchBuildReqs" will post the instanceId to be
			// removed from processedInsts in this channel
		case instId := <-recoveryListener:
			delete(processedInsts, instId)
			continue

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

					if indexState == c.INDEX_STATE_INITIAL ||
						indexState == c.INDEX_STATE_CATCHUP ||
						indexState == c.INDEX_STATE_ACTIVE {
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

					// defnKey is used to retrive the stats of real instance
					// (in case real instance exists)
					defnKey := realInstId
					if realInstId == 0 {
						defnKey = instId
					}

					var instKey common.IndexInstId
					var indexState c.IndexState
					if _, ok := indexStateMap[instId]; ok {
						indexState = indexStateMap[instId]
						instKey = instId
					} else {
						indexState = indexStateMap[realInstId]
						instKey = realInstId
					}

					if indexState == c.INDEX_STATE_NIL || indexState == c.INDEX_STATE_DELETED {
						l.Infof("ShardRebalancer::waitForIndexState Index state is nil or deleted for instId %v. Retrying...", instId)
						delete(processedInsts, instKey) // consider the index build done
						continue
					}

					//  for current defn. If indexState is not NIL or DELETED and defn stats are nil,
					// wait for defn stats to get populated
					defnStats := allStats.indexes[defnKey]
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

					now := time.Now()
					if now.Sub(lastLogTime) > 30*time.Second {
						lastLogTime = now
						l.Infof("ShardRebalancer::waitForIndexState Index: %v:%v:%v:%v State: %v"+
							" DocsPending: %v DocsQueued: %v DocsProcessed: %v, Rate: %v"+
							" Remaining: %v EstTime: %v Partns: %v DestAddr: %v, instId: %v, realInstId: %v",
							defn.Bucket, defn.Scope, defn.Collection, defn.Name, indexState,
							numDocsPending, numDocsQueued, numDocsProcessed, processing_rate,
							tot_remaining, remainingBuildTime, defn.Partitions, sr.localaddr,
							instId, realInstId)
					}
					if indexState == c.INDEX_STATE_ACTIVE && remainingBuildTime < maxRemainingBuildTime {
						delete(processedInsts, instKey)
						// remove realInstId irrespective of merge as the instance is processed
						delete(processedInsts, realInstId)

						if tt.IsEmptyNodeBatch {
							l.Infof("ShardRebalancer::waitForIndexState Skip changing RState for instId: %v, realInstId: %v, ttid: %v "+
								"as empty node batching is enabled for this token", instId, realInstId, ttid)
						} else {
							sr.destTokenToMergeOrReady(instId, realInstId, ttid, tt, &partnMergeWaitGroup)
							// Wait for merge of partitioned indexes (or) RState update to finish before returning
							partnMergeWaitGroup.Wait()
						}
					}
				}

				// If all indexes are built, defnIdMap will have no entries
				if len(processedInsts) == 0 {
					l.Infof("ShardRebalancer::waitForIndexState All indexes are active and "+
						"caught up for token: %v. Returning from waitForIndexState", ttid)
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
	realInstId c.IndexInstId, ttid string, tt *c.TransferToken, partnMergeWaitGroup *sync.WaitGroup) {

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

			ticker := time.NewTicker(time.Duration(5) * time.Minute)
			defer ticker.Stop()

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
			retry := true
			for retry {
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
					l.Infof("ShardRebalancer::destTokenToMergeOrReady Response reveived for merge of "+
						"index instance: %v, realInst: %v", instId, realInstId)
					retry = false
					break
				}
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

func (sr *ShardRebalancer) updateRStateToActive(ttid string, tt *c.TransferToken) {
	if !sr.addToWaitGroup() {
		return
	}

	defer sr.wg.Done()

	logging.Infof("ShardRebalancer::updateRStateToActive Updating RState of indexes in transfer token: %v", ttid)

	var partnMergeWaitGroup, wg sync.WaitGroup
	for i := range tt.IndexInsts {
		wg.Add(1)
		// Change RState in go-routines so that all indexes in the token gets a chance to merge
		// Otherwise, due to the ordering of instances in the token, merge can get stuck in deadlock.
		// E.g., token_1 [proxy_1, real_2], token_2: [proxy_2, real_1]
		// Both proxies would wait for RState of real instance to go active and the real instance would
		// never get a change to move RState active if "destTokenToMergeOrReady" is not spawned asynchronously
		// Indexer would serialize the RState transitions anyways
		go func(index int) {
			defer wg.Done()
			// merge only if instance is not dropped during rebalance
			if sr.isInstDroppedDuringRebal(tt.InstIds[index], tt.RealInstIds[index]) == false {
				sr.destTokenToMergeOrReady(tt.InstIds[index], tt.RealInstIds[index], ttid, tt, &partnMergeWaitGroup)
			} else {
				logging.Infof("ShardRebalancer::updateRStateToActive Skip updating RState for instId: %v, realInstId: %v, ttid: %v",
					tt.InstIds[index], tt.RealInstIds[index], ttid)
			}
		}(i)
	}

	// Ensures that partnMergeWaitGroup.Add(1) gets a chance inside "destTokenToMergeOrReady"
	wg.Wait()

	// Wait for actual partition merge to finish
	partnMergeWaitGroup.Wait()
	logging.Infof("ShardRebalancer::updateRStateToActive Done with partition merge for ttid: %v", ttid)

	// For a partitioned index, it is possible that real instace is in one token and proxy instance
	// is in another token. In such a case, if proxy initiates merge first, then merge fails as
	// real instance RState is not active. However, the go-routine in "destTokenToMergeOrReady"
	// would keep waiting for merge to finish. In such a case, acquiring sr.mu.Lock() before
	// changing the RState would block token with "real instance" to change RState and rebalance
	// ends up in a deadlock. As "sr.mu" is only to protect token states and shard rebalancer's
	// book-keeping, delay the locking until merge is finished
	sr.mu.Lock()
	defer sr.mu.Unlock()

	// Update state of the transfer token
	tt.ShardTransferTokenState = c.ShardTokenReady
	sr.acceptedTokens[ttid] = tt // Update accpeted tokens
	setTransferTokenInMetakv(ttid, tt)

	if sr.allDestTokensReadyLOCKED() {
		sr.updateRStateOfDCPTokens()
	}
}

func (sr *ShardRebalancer) allDestTokensReadyLOCKED() bool {
	logging.Infof("ShardRebalancer::allDestTokensReadyLOCKED Updating RState of indexes in transfer token")

	for ttid, token := range sr.acceptedTokens {
		switch token.ShardTransferTokenState {
		case c.ShardTokenCreated,
			c.ShardTokenScheduledOnSource,
			c.ShardTokenScheduleAck,
			c.ShardTokenTransferShard,
			c.ShardTokenRestoreShard,
			c.ShardTokenRecoverShard,
			c.ShardTokenMerge,
			c.ShardTokenError:
			logging.Infof("ShardRebalancer::allDestTokensReadyLOCKED Token: %v is still not ready", ttid)
			return false
		}
	}
	return true
}

func getDcpTokens() (map[string]*c.TransferToken, error) {

	metainfo, err := metakv.ListAllChildren(RebalanceMetakvDir)
	if err != nil {
		return nil, err
	}

	if len(metainfo) == 0 {
		return nil, nil
	}

	var rinfo RebalTokens
	rinfo.TT = make(map[string]*c.TransferToken)

	for _, kv := range metainfo {

		if strings.Contains(kv.Path, TransferTokenTag) {
			ttidpos := strings.Index(kv.Path, TransferTokenTag)
			ttid := kv.Path[ttidpos:]

			var tt c.TransferToken
			json.Unmarshal(kv.Value, &tt)
			rinfo.TT[ttid] = &tt
		}
	}

	return rinfo.TT, nil
}

func (sr *ShardRebalancer) updateRStateOfDCPTokens() {
	// Step-1: Get all DCP tokens from metaKV
	dcpTokens, err := getDcpTokens()
	if err != nil {
		logging.Errorf("ShardRebalancer::updateRStateOfDCPTokens Failure to get DCP tokens from metaKV, err: %v", err)
		return // Return but do not fail rebalance
	}

	if len(dcpTokens) == 0 {
		logging.Errorf("ShardRebalancer::updateRStateOfDCPTokens No DCP tokens found")
		return
	}

	// Step-2: Change Rstate of each of the DCP tokens
	for dcpTokenId, dcpToken := range dcpTokens {

		if dcpToken.IsEmptyNodeBatch == false || dcpToken.IsPendingReady == false {
			continue
		}

		if dcpToken.DestId != sr.nodeUUID {
			continue
		}

		var partnMergeWaitGroup sync.WaitGroup
		sr.destTokenToMergeOrReady(dcpToken.InstId, dcpToken.RealInstId, dcpTokenId, dcpToken, &partnMergeWaitGroup)
		partnMergeWaitGroup.Wait()

		// At this point, index is merged & RState of the index is changed.
		// Otherwise, indexer would have crashed
		// Update transfer token state
		if dcpToken.TransferMode == c.TokenTransferModeMove {
			dcpToken.State = c.TransferTokenReady
		} else {
			dcpToken.State = c.TransferTokenCommit // no source to delete in non-move case
		}
		logging.Infof("ShardRebalancer::updateRStateOfDCPTokens Changing RState of dcpToken: %v to %v", dcpTokenId, dcpToken.State)
		setTransferTokenInMetakv(dcpTokenId, dcpToken)
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

	if sr.isMaster && sr.runPlanner && sr.removeDupIndex {
		<-sr.dropIndexDone // wait for duplicate index removal to finish
	}

	if len(sr.transferTokens) == 0 {
		if sr.cb.progress != nil {
			sr.cb.progress(sr.shardProgressRatio, sr.cancel)
		}
		sr.transitionToDcpOrEndShardRebalance()
		if c.IsServerlessDeployment() {
			// need to run observer to detect ActiveRebalancer change
			return
		}
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

func (sr *ShardRebalancer) updatePendingReadyTokens(ttid string, tt *c.TransferToken) {

	sr.mu.Lock()
	defer sr.mu.Unlock()

	sr.pendingReadyTokens[ttid] = tt
}

func (sr *ShardRebalancer) allTokensPendingReadyOrDone() bool {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	for ttid, tt := range sr.transferTokens {
		if (tt.ShardTransferTokenState == c.ShardTokenRecoverShard && tt.IsPendingReady) || tt.ShardTransferTokenState == c.ShardTokenDeleted {
			continue
		} else {
			logging.Infof("ShardRebalancer::allTokensPendingReadyOrDone Returning false as token: %v "+
				"is in state: %v, pendingReady: %v", ttid, tt.ShardTransferTokenState, tt.IsPendingReady)
			return false
		}
	}
	logging.Infof("ShardRebalancer::allTokensPendingReadyOrDone All tokens are either pendingReady or Deleted")
	return true
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
	// do not check for valid state changes for this state.
	//
	// When empty node batching is enabled, a notification will be sent via metaKV with
	// "pendingReady" flag set to true in transfer token. Master should always process
	// ShardTokenRecoverShard state in such a case
	if tt.ShardTransferTokenState == c.ShardTokenCreated || (tt.ShardTransferTokenState == c.ShardTokenRecoverShard && sr.isMaster) {
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

	// set progress
	go asyncCollectProgress(&sr.shardProgress, sr.computeProgress, 5*time.Second,
		"ShardRebalancer::updateProgress", sr.cancel, sr.done)

	// report progress
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			sr.cb.progress(sr.getCurrentTotalProgress(), sr.cancel)
		case <-sr.cancel:
			l.Infof("ShardRebalancer::updateProgress Cancel Received. progress reporter stapped")
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

// computeProgress calculate progress considering Dcp Rebal tokens
func (sr *ShardRebalancer) computeProgress() float64 {
	return sr.shardProgressRatio * sr.computeShardProgress()
}

// computeShardProgress only computes the progress of the shard tokens
func (sr *ShardRebalancer) computeShardProgress() float64 {

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

	if totTokens == 0 {
		return 1.0
	}

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

	transferWt := 0.35
	restoreWt := 0.35
	recoverWt := 0.3

	var totalProgress float64
	for ttid, tt := range tokens {
		state := tt.ShardTransferTokenState
		// All states not tested in the if-else if are treated as 0% progress
		if state == c.ShardTokenReady ||
			state == c.ShardTokenCommit || state == c.ShardTokenDeleted {
			totalProgress += 100.00
		} else if state == c.ShardTokenMerge {
			totalProgress += 99.0 // 1% less as RState change is pending
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
						// If index build is scheduled but it could not proceed, then
						// ignore the error and proceed to drop the index
						if strings.Contains(err, common.ErrTransientError.Error()) {
							logging.Infof("ShardRebalancer::dropShardsWhenIdle Ignoring err: %v for inst: %v and "+
								"proceeding to drop the index", err, instKey)
							// Continue to drop the index
						} else {
							l.Errorf("ShardRebalancer::dropShardsWhenIdle Error Fetching Index Status %v, err: %v", sr.localaddr, err)
							retryCount++

							if retryCount > 50 { // After 50 retires
								droppedIndexes[instKey] = true // Consider index drop successful and continue
								continue
							}
							time.Sleep(retryInterval * time.Second)
							goto loop // Retry
						}
					}
				}

				defnStats := allStats.indexes[instKey] // stats for current defn
				if defnStats == nil {
					l.Infof("ShardRebalancer::dropShardsWhenIdle Missing defnStats for instId %v. Considering the index as dropped", instKey)
					droppedIndexes[instKey] = true
					continue // If index is deleted, then it is already validated from localMeta
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

		// reset retryCount as one iteration has been successfully completed
		retryCount = 0

		time.Sleep(1 * time.Second)
	}
}

func (sr *ShardRebalancer) finishRebalance(err error) {

	if err == nil && sr.isMaster && sr.topologyChange != nil {
		// Note that this function tansfers the ownership of only those
		// tokens, which are not owned by keep nodes. Ownership of other
		// tokens remains unchanged.
		keepNodes := make(map[string]bool)
		for _, node := range sr.topologyChange.KeepNodes {
			keepNodes[string(node.NodeInfo.NodeID)] = true
		}

		// Suppress error if any. The background task to handle failover
		// will do necessary retry for transferring ownership.
		cfg := sr.config.Load()
		_ = transferScheduleTokens(keepNodes, cfg["clusterAddr"].String())
	}
	if err != nil {
		// we cannot encounter an error on finishRebalance once we have moved to DcpRebalance
		// so if we have seen an error, we can close the DcpRebr channel as we will not be entering
		// DcpRebalance
		sr.dcpRebrCloseOnce.Do(func() {
			if sr.dcpRebrCloseCh != nil {
				close(sr.dcpRebrCloseCh)
			}
		})
	}

	sr.retErr = err
	sr.cleanupOnce.Do(sr.doFinish)
}

func (sr *ShardRebalancer) doFinish() {

	l.Infof("ShardRebalancer::doFinish cleanup started")
	atomic.StoreInt32(&sr.isDone, 1)
	close(sr.done)

	sr.cancelMetakv()
	if sr.canMaintainShardAffinity && !c.IsServerlessDeployment() {
		// can't directly use sr.done as it is closed before STOP_PEER_SERVER command
		sr.sendPeerServerCommand(STOP_PEER_SERVER, nil, nil)

		sr.dcpRebrCloseOnce.Do(func() {
			if sr.dcpRebrCloseCh != nil {
				close(sr.dcpRebrCloseCh)
			}
		})
	}

	sr.wg.Wait()

	// we want to log the last err we send to callback once all components are done processing
	l.Infof("ShardRebalancer::doFinish Cleanup: %v", sr.retErr)
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

	// we are not making any checks here if the RPC server was running or not; if the server is not
	// running then STOP_PEER_SERVER should be a no-op
	if sr.canMaintainShardAffinity && !c.IsServerlessDeployment() {
		go sr.sendPeerServerCommand(STOP_PEER_SERVER, nil, nil)
	}

	sr.mu.Lock()
	defer sr.mu.Unlock()
	if sr.dcpRebr != nil {
		sr.dcpRebr.Cancel()
	}
}

// batchTransferTokens is the delegator func which creates batches of transfer token on the
// basis of serverless mode or shard affinity
func (sr *ShardRebalancer) batchTransferTokens() {
	if c.IsServerlessDeployment() {
		sr.batchShardTransferTokensForServerless()
	} else if sr.canMaintainShardAffinity {
		if sr.schedulingVersion >= c.PER_NODE_TRANSFER_LIMIT {
			// according to current algorithm, we don't need to group tokens
			// since per group has only one token, we take care of selecting right amount of token
			// per batch in scheduling
			// if this windowSz is more than 1, we have inefficient transfers happening as currently
			// we only order tokens by copy and move; inefficiency comes in scheduling as we won't
			// publish more tokens for other nodes which were eligible for transfers
			// also if the windowSz is more than 1 then we need to ensure no move is scheduled
			// before copy of same shard which can happen today
			sr.orderTransferTokensPerNode(1)
		} else {
			windowSz := sr.config.Load().GetDeploymentModelAwareCfg("rebalance.transferBatchSize").Int()
			sr.orderCopyAndMoveTransferTokens(windowSz)
		}
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
func (sr *ShardRebalancer) batchShardTransferTokensForServerless() {
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

func (sr *ShardRebalancer) orderCopyAndMoveTransferTokens(windowSz int) {
	if windowSz == 0 {
		sr.batchedTokens = []map[string]*c.TransferToken{sr.transferTokens}
		return
	}

	cTokens, mTokens := func() (cTokens, mTokens map[string]*c.TransferToken) {
		cTokens = make(map[string]*c.TransferToken)
		mTokens = make(map[string]*c.TransferToken)

		for ttid, token := range sr.transferTokens {
			switch token.TransferMode {
			case c.TokenTransferModeCopy:
				cTokens[ttid] = token
			case c.TokenTransferModeMove:
				mTokens[ttid] = token
			}
		}

		return
	}()

	groupTokens := func(tokens map[string]*c.TransferToken, windowSz int) []map[string]*c.TransferToken {
		batch := make([]map[string]*c.TransferToken, 0, len(tokens)/windowSz)

		group := make(map[string]*c.TransferToken)
		for ttid, token := range tokens {
			group[ttid] = token
			if len(group) >= windowSz {
				batch = append(batch, group)

				group = make(map[string]*c.TransferToken)
			}
		}
		if len(group) > 0 {
			batch = append(batch, group)
		}

		return batch
	}

	// Perform copy of all shards first
	sr.batchedTokens = append(sr.batchedTokens, groupTokens(cTokens, windowSz)...)

	// To keep things simple, we don't keep copy and move tokens in the same group
	sr.batchedTokens = append(sr.batchedTokens, groupTokens(mTokens, windowSz)...)

	l.Infof("ShardRebalancer::orderCopyAndMoveTransferTokens created a batch of %v tokens with window of %v",
		len(sr.batchedTokens), windowSz)
}

func (sr *ShardRebalancer) orderTransferTokensPerNode(windowSz int) {
	// TODO: add batching by source node
	sr.orderCopyAndMoveTransferTokens(windowSz)
}

func (sr *ShardRebalancer) initiateDeploymentAwarePerNodeTransferAsMaster() {
	if c.IsServerlessDeployment() {
		sr.initiatePerNodeTransferAsMasterForServerless()
	} else {
		sr.initiatePerNodeTransferAsMaster()
	}
}

func (sr *ShardRebalancer) initiatePerNodeTransferAsMaster() {
	config := sr.config.Load()
	batchSize := config.GetDeploymentModelAwareCfg("rebalance.perNodeTransferBatchSize").Int()

	publishedIds := make(map[string]string) // Source ID to transfer token ID

	shardsInCopy := func() map[c.ShardId]bool {
		res := make(map[c.ShardId]bool)
		for _, tokenMap := range sr.activeTransfers {
			for ttid, active := range tokenMap {
				tt := sr.transferTokens[ttid]
				if active && tt != nil && tt.TransferMode == c.TokenTransferModeCopy {
					for _, shardId := range tt.ShardIds {
						res[shardId] = true
					}
				}
			}
		}
		return res
	}()

	logging.Infof("ShardRebalancer::initiatePerNodeTransferAsMaster activeTransferCount: %v, activeShardsInCopy %v, batchSize: %v",
		sr.activeTransfers, shardsInCopy, batchSize)
	for _, groupedTokens := range sr.batchedTokens {
		publish := (len(groupedTokens) != 0)
		if !publish {
			continue
		}

		// For any source node in the groupedTokens, if the active transfer
		// count is greater than the per node transfer batch size, skip that
		// batch from current transfer - Choose a different batch
		for _, tt := range groupedTokens {
			if tt.ShardTransferTokenState == c.ShardTokenScheduleAck {
				if len(sr.activeTransfers[tt.SourceId]) >= batchSize {
					publish = false
					break
				}
			}
		}

		if publish {
			for ttid, tt := range groupedTokens {
				shardInCopy := false
				for _, shardId := range tt.ShardIds {
					shardInCopy = shardInCopy || shardsInCopy[shardId]
				}
				if tt.TransferMode == c.TokenTransferModeMove && shardInCopy {
					continue
				}
				if tt.ShardTransferTokenState == c.ShardTokenScheduleAck {
					// Used a cloned version so that the master token list
					// will not be updated until the transfer token with
					// updated state is persisted in metaKV
					ttClone := tt.Clone()

					// Change state of transfer token to TransferTokenTransferShard
					ttClone.ShardTransferTokenState = c.ShardTokenTransferShard
					setTransferTokenInMetakv(ttid, ttClone)
					publishedIds[tt.SourceId] = ttid
					sr.addTokenToActiveTransfersLOCKED(ttid, tt.SourceId)

					delete(groupedTokens, ttid)
				}
			}
		}
	}

	if len(publishedIds) > 0 {
		l.Infof("ShardRebalancer::initiatePerNodeTransferAsMaster Published transfer token batch: %v, "+
			"currActiveTransfers: %v", publishedIds, sr.activeTransfers)
	}
}

func (sr *ShardRebalancer) initiatePerNodeTransferAsMasterForServerless() {
	config := sr.config.Load()
	batchSize := config.GetDeploymentModelAwareCfg("rebalance.perNodeTransferBatchSize").Int()

	publishedIds := make(map[string]string) // Source ID to transfer token ID

	logging.Infof("ShardRebalancer::initiatePerNodeTransferAsMasterForServerless activeTransferCount: %v, batchSize: %v", sr.activeTransfers, batchSize)
	for i, groupedTokens := range sr.batchedTokens {
		publish := (groupedTokens != nil)
		if !publish {
			continue
		}

		// For any source node in the groupedTokens, if the active transfer
		// count is greater than the per node transfer batch size, skip that
		// batch from current transfer - Choose a different batch
		for _, tt := range groupedTokens {
			if tt.ShardTransferTokenState == c.ShardTokenScheduleAck {
				if len(sr.activeTransfers[tt.SourceId]) >= batchSize {
					publish = false
					break
				}
			}
		}

		if publish {
			for ttid, tt := range groupedTokens {
				if tt.ShardTransferTokenState == c.ShardTokenScheduleAck {
					// Used a cloned version so that the master token list
					// will not be updated until the transfer token with
					// updated state is persisted in metaKV
					ttClone := tt.Clone()

					// Change state of transfer token to TransferTokenTransferShard
					ttClone.ShardTransferTokenState = c.ShardTokenTransferShard
					setTransferTokenInMetakv(ttid, ttClone)
					publishedIds[tt.SourceId] = ttid
					sr.addTokenToActiveTransfersLOCKED(ttid, tt.SourceId)
				}
			}

			sr.batchedTokens[i] = nil
		}
	}

	if len(publishedIds) > 0 {
		l.Infof("ShardRebalancer::initiatePerNodeTransferAsMasterForServerless Published transfer token batch: %v, "+
			"currActiveTransfers: %v", publishedIds, sr.activeTransfers)
	}
}

// called by master to initiate transfer of shards.
// sr.mu needs to be acquired by the caller of this method
func (sr *ShardRebalancer) initiateShardTransferAsMaster() {

	config := sr.config.Load()

	batchSize := config.GetDeploymentModelAwareCfg("rebalance.transferBatchSize").Int()

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
	dropOnSourceTokenId := fmt.Sprintf("ShardToken%s", ustr.Str())

	dropOnSourceToken := &c.TransferToken{
		ShardTransferTokenState: c.ShardTokenDropOnSource,
		Version:                 c.MULTI_INST_SHARD_TRANSFER,
		RebalId:                 rebalId,
		SourceTokenId:           sourceTokenId,
		SiblingTokenId:          siblingTokenId,
		BuildSource:             c.TokenBuildSourceS3,
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

func (sr *ShardRebalancer) updateInstsTransferPhase(ttid string, tt *c.TransferToken, tranfserPhase common.RebalancePhase) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	instsTransferPhase := make(map[c.IndexInstId]common.RebalancePhase)
	for i := range tt.IndexInsts {
		if tt.RealInstIds[i] != 0 {
			instsTransferPhase[tt.RealInstIds[i]] = tranfserPhase
		} else {
			instsTransferPhase[tt.InstIds[i]] = tranfserPhase
		}
	}

	// Incase of copy (replica repair cases), update the transfer phase for
	// actual instances on source node (as instances can get renamed in the
	// transfer token)
	if tt.TransferMode == c.TokenTransferModeCopy && tt.ShardTransferTokenState <= c.ShardTokenTransferShard {
		for _, instRenameMap := range tt.InstRenameMap {
			for origPath := range instRenameMap {
				splitPath := strings.Split(origPath, ".index")
				if len(splitPath) > 0 {
					// Last 2 entires of splitPath[0] would be instanceID and partnID
					newSplit := strings.Split(splitPath[0], "_")
					origInstId, _ := strconv.ParseUint(newSplit[len(newSplit)-2], 10, 64)
					instsTransferPhase[common.IndexInstId(origInstId)] = tranfserPhase
				} else {
					logging.Fatalf("ShardRebalancer::updateInstsTransferPhase Invalid path seen for token: %v, "+
						"path: %v, instRenameMap: %v", ttid, origPath, tt.InstRenameMap)
				}
			}
		}
	}

	globalPh := common.RebalanceInitated
	if sr.rebalToken.RebalPhase == common.RebalanceTransferInProgress {
		globalPh = common.RebalanceTransferInProgress
	}

	msg := &MsgUpdateRebalancePhase{
		GlobalRebalancePhase: globalPh,
		InstsTransferPhase:   instsTransferPhase,
	}

	sr.supvMsgch <- msg
}

func (sr *ShardRebalancer) updateEmptyNodeBatching(isEmptyBatch bool) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	sr.emptyNodeBatchingEnabled = sr.emptyNodeBatchingEnabled || isEmptyBatch
}

func (sr *ShardRebalancer) isEmptyNodeBatchingEnabled() bool {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	return sr.emptyNodeBatchingEnabled
}

func (sr *ShardRebalancer) moveToDcpRebr() {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	sr.dcpRebalStarted = true

	if sr.rebalToken.ActiveRebalancer != c.DCP_REBALANCER {

		temp := *sr.rebalToken
		clonedToken := temp

		clonedToken.ActiveRebalancer = c.DCP_REBALANCER

		setToken := func(attempts int, lastErr error) error {
			if lastErr != nil {
				l.Errorf("ShardRebalancer::moveToContRebr failed %v times. Last error %v.",
					attempts, lastErr)
			}

			return c.MetakvSet(RebalanceTokenPath, &clonedToken)
		}

		rh := c.NewRetryHelper(10, 10*time.Millisecond, 10, setToken)
		err := rh.Run()

		if err != nil {
			l.Errorf("ShardRebalancer::moveToContRebr failed to update metakv Token to transition to DCP rebalancer with err %v",
				err)
			if sr.canMaintainShardAffinity && !c.IsServerlessDeployment() {
				go sr.finishRebalance(err)
			}
		}
	} else {
		sr.cancelMetakv()
		go sr.finishRebalance(nil)
	}
}

func (sr *ShardRebalancer) canAllowDDLDuringRebalance() bool {
	// Allow or reject DDL based on "allowDDLDuringRebalance" config
	config := sr.config.Load()
	if common.IsServerlessDeployment() {
		canAllowDDLDuringRebalance := config["serverless.allowDDLDuringRebalance"].Bool()
		if !canAllowDDLDuringRebalance {
			logging.Verbosef("ShardRebalancer::canAllowDDLDuringRebalance Disallowing DDL as config: serverless.allowDDLDuringRebalance is false")
			return false
		}
	} else {
		canAllowDDLDuringRebalance := config["allowDDLDuringRebalance"].Bool()
		if !canAllowDDLDuringRebalance {
			logging.Verbosef("ShardRebalancer::canAllowDDLDuringRebalance Disallowing DDL as config: allowDDLDuringRebalance is false")
			return false
		}
	}
	return true
}

func (sr *ShardRebalancer) sendPeerServerCommand(cmd MsgType, syncCh chan struct{}, doneCh chan struct{}) {
	defer func() {
		if syncCh != nil {
			close(syncCh)
		}
	}()

	if doneCh == nil {
		doneCh = make(chan struct{})
		defer close(doneCh)
	}

	respCh := make(chan error)

	msg := &MsgPeerServerCommand{
		mType:       cmd,
		respCh:      respCh,
		rebalanceId: sr.rebalToken.RebalId,
	}

	monitor := func(respCh chan error, rebalId string, cmd MsgType) {
		// rebalance could be done by now so we dont have a nice way to hadle this error other than
		// crash
		err := <-respCh
		if err != nil {
			l.Errorf("ShardRebalancer::sendPeerCommand:monitor failed command %v for rebalance %v with error %v",
				cmd, rebalId, err)
			common.CrashOnError(err)
		} else {
			l.Infof("ShardRebalancer::sendPeerCommand:monitor: command %v successful for rebalance %v",
				cmd, rebalId)
		}
	}

	sr.supvMsgch <- msg
	select {
	case <-doneCh:
		l.Infof("ShardRebalancer::sendPeerServerCommand: got done on rebalancer channel. will monitor %v in go-routine",
			cmd)
		go monitor(respCh, sr.rebalToken.RebalId, cmd)
	case <-sr.cancel:
		l.Infof("ShardRebalancer::sendPeerServerCommand: got cancel on rebalancer channel. will monitor %v in go-routine",
			cmd)
		go monitor(respCh, sr.rebalToken.RebalId, cmd)
	case err := <-respCh:
		if err != nil {
			// only finish rebalance if we have an error
			logging.Errorf("ShardRebalancer::sendPeerServerCommand: failed command %v with error %v", cmd, err)
			go sr.finishRebalance(err)
		} else {
			logging.Infof("ShardRebalancer::sendPeerServerCommand: command %v successful", cmd)
		}
	}
}

// transitionToDcpOrEndShardRebalance - is the final step in a succesful shard rebalance
// the step is to either start Dcp Rebalance for on-prem or call finishRebalance
func (sr *ShardRebalancer) transitionToDcpOrEndShardRebalance() {
	if !c.IsServerlessDeployment() && sr.canMaintainShardAffinity {
		sr.moveToDcpRebr()
	} else {
		sr.dcpRebrCloseOnce.Do(func() {
			if sr.dcpRebrCloseCh != nil {
				close(sr.dcpRebrCloseCh)
			}
		})
		sr.cancelMetakv()
		go sr.finishRebalance(nil)
	}
}

// processRebalancerChange to start controlled rebalancer according to transition on
// rToken.ActiveRebalancer; only to be called on change in ActiveRebalancer
func (sr *ShardRebalancer) processRebalancerChange(rToken *RebalanceToken) {

	select {
	case <-sr.cancel:
		l.Infof("ShardRebalancer::processRebalancerChange saw cancel will skip processing rebalancer change")
		return
	default:
	}

	switch rToken.ActiveRebalancer {
	case c.DCP_REBALANCER:
		// start DCP rebalance and finish shard rebalance
		sr.startDcpRebalance(rToken)

		if sr.isMaster {

			logging.Infof("ShardRebalance::processRebalancerChange Waiting for DCP rebalance to finish")
			<-sr.dcpRebrCloseCh
			logging.Infof("ShardRebalance::processRebalancerChange Done with DCP rebalance. Moving tokens to merge state")

			func() {
				sr.mu.Lock()
				defer sr.mu.Unlock()

				if sr.retErr != nil || len(sr.transferTokens) == 0 || len(sr.pendingReadyTokens) == 0 {
					sr.cancelMetakv()
					go sr.finishRebalance(sr.retErr)
				} else {
					sr.movePendingReadyTokensToMergeStateLocked()
				}
			}()
		} else {
			// For all follower nodes, once dcpRebr is finished, they would wait for
			// any change in shard token states. Hence, no-op for follower nodes
		}

		break

	default:
		// if tomorrow we add a new rebalancer, we should record that change too although we may not
		// start that rebalancer; ideally this will be the scenario only on follower as master
		// always has latest code aware of new rebalancer too; follower does not stop shard
		// rebalancer here but rather will end when the token gets deleted;

		// can we end up in a hang here if we don't finish rebalance?
		l.Warnf("ShardRebalancer::processRebalancerChange invalid change in active rebalancer from %v to %v",
			sr.rebalToken.ActiveRebalancer, rToken.ActiveRebalancer)
	}

}

func (sr *ShardRebalancer) movePendingReadyTokensToMergeStateLocked() {
	for ttid, tt := range sr.pendingReadyTokens {

		select {
		case <-sr.cancel:
			logging.Infof("ShardRebalancer::movePendingReadyTokensToMergeStateLocked Cancel received")
			return
		case <-sr.done:
			logging.Infof("ShardRebalancer::movePendingReadyTokensToMergeStateLocked Done received")
			return

		default:

			logging.Infof("ShardRebalancer::movePendingReadyTokensToMergeStateLocked Moving token: %v, to ShardTokenMerge state", ttid)
			tt.ShardTransferTokenState = c.ShardTokenMerge
			setTransferTokenInMetakv(ttid, tt)
		}
	}
}

// startDcpRebalancer starts the DCP rebalancer in controlled mode (runPlanner: false,
// master: sr.IsMaster); or closes the dcpRebrCloseCh
func (sr *ShardRebalancer) startDcpRebalance(rToken *RebalanceToken) {
	sr.wg.Add(1)
	defer sr.wg.Done()

	sr.mu.Lock()
	defer sr.mu.Unlock()

	if sr.canMaintainShardAffinity && !c.IsServerlessDeployment() {
		tokens := (map[string]*c.TransferToken)(nil)
		if sr.isMaster {
			if len(sr.dcpTokens) == 0 {
				sr.dcpRebrCloseOnce.Do(func() {
					if sr.dcpRebrCloseCh != nil {
						close(sr.dcpRebrCloseCh)
					}
				})
				l.Infof("ShardRebalancer::startDcpRebalance exiting as there are no dcp tokens to be processed")
				return
			} else {
				// only master has DCP tokens, followers get nil
				tokens = sr.dcpTokens
			}
		}
		sr.dcpRebrOnce.Do(func() {
			sr.dcpRebr = NewRebalancer(tokens, sr.rebalToken, sr.nodeUUID, sr.isMaster,
				sr.dcpRebrProgressCallback, sr.dcpRebrDoneCallback, sr.supvMsgch, sr.localaddr,
				sr.config.Load(), sr.topologyChange, false, sr.runParams, sr.statsMgr, sr.globalTopology,
				c.SHARD_REBALANCER)

			if sr.isMaster {
				sr.dcpRebr.InitGlobalTopology(sr.globalTopology)
			}

			l.Infof("ShardRebalancer::startDcpRebalance started controlled rebalancer")
		})
	} else {
		sr.dcpRebrCloseOnce.Do(func() {
			if sr.dcpRebrCloseCh != nil {
				close(sr.dcpRebrCloseCh)
			}
		})
	}

	sr.rebalToken.ActiveRebalancer = rToken.ActiveRebalancer
}

func (sr *ShardRebalancer) dcpRebrDoneCallback(err error, cancel <-chan struct{}) {
	sr.wg.Add(1)
	defer sr.wg.Done()

	defer sr.dcpRebrCloseOnce.Do(func() {
		if sr.dcpRebrCloseCh != nil {
			close(sr.dcpRebrCloseCh)
		}
	})

	l.Infof("ShardRebalancer::dcpRebrDoneCallback controlled rebalancer exited with error %v",
		err)
	if err != nil {
		sr.retErr = err
	}
}

func (sr *ShardRebalancer) dcpRebrProgressCallback(progress float64, cancel <-chan struct{}) {
	if sr.cb.progress == nil {
		return
	}
	sr.dcpProgress.SetFloat64(progress)

	sr.cb.progress(sr.getCurrentTotalProgress(), cancel)
}

func (sr *ShardRebalancer) InitGlobalTopology(globalTopology *manager.ClusterIndexMetadata) {
	// no-op for shard rebalancer
}

func (sr *ShardRebalancer) isDcpRebalanceStarted() bool {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	return sr.dcpRebalStarted
}

// isIndexDeletedDuringRebal returns true if an index is deleted while
// bucket rebalance is in progress
func isIndexDeletedDuringRebal(errMsg string) bool {
	return errMsg == common.ErrIndexDeletedDuringRebal.Error()
}

func isIndexInAsyncRecovery(errMsg string) bool {
	return errMsg == common.ErrIndexInAsyncRecovery.Error()
}

func isBuildAlreadyInProgress(errMsg string) bool {
	return strings.Contains(errMsg, "Build Already In Progress")
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

func getTLSConfigWithCeftificates(host string) (*tls.Config, error) {
	tlsConfig, err := security.GetTLSConfigForClient(host)
	if err != nil {
		return nil, err
	} else if tlsConfig == nil {
		tlsConfig = &tls.Config{}

		err = security.SetupCertificateForClient(host, tlsConfig)
		if err != nil {
			return nil, err
		}
	}

	return tlsConfig, nil
}

func asyncCollectProgress(progHolder *float64Holder, collector func() float64, dur time.Duration,
	caller string, cancel, done <-chan struct{}) {
	collectorTicker := time.NewTicker(dur - 500*time.Millisecond)
	defer collectorTicker.Stop()

	for {
		select {
		case <-collectorTicker.C:
			progHolder.SetFloat64(collector())
		case <-cancel:
			l.Infof("%v:asyncCollectProgress cancel received. progress collector stopped", caller)
			return
		case <-done:
			l.Infof("%v:asyncCollectProgress done received. progress collector stopped", caller)
			return
		}
	}
}

func (sr *ShardRebalancer) addTokenToActiveTransfersLOCKED(ttid string, nodeId string) {
	if _, ok := sr.activeTransfers[nodeId]; !ok {
		sr.activeTransfers[nodeId] = make(map[string]bool)
	}

	sr.activeTransfers[nodeId][ttid] = true
}

func (sr *ShardRebalancer) removeTokenFromActiveTransfersLOCKED(ttid string, nodeId string) {
	if _, ok := sr.activeTransfers[nodeId]; ok {
		delete(sr.activeTransfers[nodeId], ttid)
	}
}

type batchBuildReq struct {
	instId     c.IndexInstId
	realInstId c.IndexInstId
	defn       c.IndexDefn
	ttid       string
	tt         *c.TransferToken
	listenerCh chan c.IndexInstId
}

func getKeyspaceName(defn c.IndexDefn) string {
	return fmt.Sprintf("%v/%v/%v", defn.Bucket, defn.Scope, defn.Collection)
}

func (sr *ShardRebalancer) processBatchBuildReqs() {

	pendingBulidReqs := make(map[string][]*batchBuildReq)

	// Batch index build requests for every 3 seconds
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	lastlog := time.Now()

	for {
		select {
		case <-sr.cancel:
			logging.Infof("ShardRebalancer::processBatchBuildReqs Cancel received. Exiting")
			return

		case <-sr.done:
			logging.Infof("ShardRebalancer::processBatchBuildReqs Done received. Exiting")
			return

		case req, ok := <-sr.batchBuildReqCh:
			if !ok {
				logging.Infof("ShardRebalancer::processBatchBuildReqs batchBuildReqCh is closed. Exiting")
				return
			}

			key := getKeyspaceName(req.defn)
			pendingBulidReqs[key] = append(pendingBulidReqs[key], req)

		case <-ticker.C:

			forceLog := false
			if time.Since(lastlog) > time.Duration(60*time.Second) {
				forceLog = true
				lastlog = time.Now()
			}

			for keyspace, perKeyspaceReqs := range pendingBulidReqs {

				// Delete from pendingBuildReqs. If retry has to be performed,
				// then the definition IDs will be added again to pendingBulidReqs
				delete(pendingBulidReqs, keyspace)

				var defnIdList client.IndexIdList
				for _, req := range perKeyspaceReqs {
					defnIdList.DefnIds = append(defnIdList.DefnIds, uint64(req.defn.DefnId))
				}

				if len(defnIdList.DefnIds) == 0 {
					continue
				}

				logging.Infof("ShardRebalancer::processBatchBuildReqs processing indexDefns: %v", defnIdList.DefnIds)

				skipDefns, retryInsts, err := sr.postBuildIndexesReq(defnIdList, "", nil)
				if err != nil {
					// Set error in transfer token and return
					logging.Errorf("ShardRebalancer::processBatchBuildReqs Error observed while posting build reqs for defnIds: %v", defnIdList.DefnIds)
					if err != ErrRebalanceCancel && err != ErrRebalanceDone {
						for _, req := range perKeyspaceReqs {
							sr.setTransferTokenError(req.ttid, req.tt, err.Error())
							break
						}
						sr.cancelMetakv()
						go sr.finishRebalance(err)
					}

					// If error is ErrRebalanceCancel or ErrRebalanceDone, no point in continuing further
					// Hence return
					return
				}

				if len(skipDefns) > 0 {
					logging.Infof("ShardRebalancer::processBatchBuildReqs Skipping state monitoring for insts: %v "+
						"as scope/collection/index is dropped", skipDefns)
					for _, req := range perKeyspaceReqs {
						if _, ok := skipDefns[req.defn.DefnId]; ok {
							req.listenerCh <- req.instId
							req.listenerCh <- req.realInstId
						}
					}
				}

				if len(retryInsts) > 0 {
					var queueReqs []*batchBuildReq
					if forceLog {
						logging.Infof("ShardRebalancer::processBatchBuildReqs Retrying index build for insts: %v", retryInsts)
					}

					// Indexer can pickup either proxy instId  or realInstId based on the presence of an partition
					// on the node. Since rebalancer does not know that information, queue the build request based
					// if either proxyInstId or realInstId are present in retryInst list
					for _, req := range perKeyspaceReqs {
						_, ok1 := retryInsts[req.instId]
						_, ok2 := retryInsts[req.realInstId]
						if ok1 || ok2 {
							queueReqs = append(queueReqs, req)
						}
					}

					pendingBulidReqs[keyspace] = queueReqs
				}
			}
		}
	}
}

func (sr *ShardRebalancer) addInstToDroppedInsts(defn *common.IndexDefn) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	// Add both instId and realInstId as rebalancer does not know whether indexer
	// has picked instId or realInstId for the instance. Also, if realInst is dropped
	// proxy will be dropped and vice versa
	sr.droppedInstsInRebal[defn.InstId] = true
	sr.droppedInstsInRebal[defn.RealInstId] = true
}

func (sr *ShardRebalancer) isInstDroppedDuringRebal(instId, realInstId c.IndexInstId) bool {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	_, ok1 := sr.droppedInstsInRebal[instId]
	_, ok2 := sr.droppedInstsInRebal[realInstId]

	return ok1 || ok2
}

func (sr *ShardRebalancer) unlockShardsOnSourcePostTransfer(ttid string, tt *c.TransferToken) {
	if !c.IsServerlessDeployment() && tt != nil && tt.SourceId == sr.nodeUUID &&
		tt.TransferMode == c.TokenTransferModeCopy &&
		tt.ShardTransferTokenState == c.ShardTokenRecoverShard &&
		tt.IsEmptyNodeBatch && tt.IsPendingReady {

		logging.Infof("ShardRebalancer::unlockShardsOnSourcePostTransfer unlocking shards %v as token %v has finished transfer and recovery on destination",
			tt.ShardIds, ttid)
		unlockShards(tt.ShardIds, sr.supvMsgch)
	}
}

func (sr *ShardRebalancer) getCurrentTotalProgress() float64 {
	return (sr.shardProgress.GetFloat64() * sr.shardProgressRatio) +
		(sr.dcpProgress.GetFloat64() * sr.dcpProgressRatio)
}
