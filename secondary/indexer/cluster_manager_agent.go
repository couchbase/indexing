// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indexer

import (
	"errors"
	"net"
	"sync/atomic"
	"time"

	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/common/queryutil"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/manager"
	mc "github.com/couchbase/indexing/secondary/manager/common"
)

// ClustMgrAgent provides the mechanism to talk to Index Coordinator
type ClustMgrAgent interface {
	// Used to register rest apis served by cluster manager.
	RegisterRestEndpoints()
}

type clustMgrAgent struct {
	supvCmdch    MsgChannel            //supervisor sends commands on this channel
	supvRespch   MsgChannel            //channel to send any message to supervisor
	mgr          *manager.IndexManager // handle to index manager singleton
	config       common.Config
	metaNotifier *metaNotifier
	stats        IndexerStatsHolder
	statsCount   uint64
}

// NewClustMgrAgent creates a new ClustMgrAgent. This is a singleton owned by the Indexer.
// It creates a singleton child IndexMgr object stored in the mgr field of the ClustMgrAgent.
func NewClustMgrAgent(supvCmdch MsgChannel, supvRespch MsgChannel, cfg common.Config, storageMode common.StorageMode) (
	ClustMgrAgent, Message) {

	//Init the clustMgrAgent struct
	c := &clustMgrAgent{
		supvCmdch:  supvCmdch,
		supvRespch: supvRespch,
		config:     cfg,
	}

	mgr, err := manager.NewIndexManager(cfg, storageMode)
	if err != nil {
		logging.Errorf("ClustMgrAgent::NewClustMgrAgent Error In Init %v", err)
		return nil, &MsgError{
			err: Error{code: ERROR_CLUSTER_MGR_AGENT_INIT,
				severity: FATAL,
				category: CLUSTER_MGR,
				cause:    err}}
	}

	c.mgr = mgr

	metaNotifier := NewMetaNotifier(supvRespch, cfg, c)
	if metaNotifier == nil {
		logging.Errorf("ClustMgrAgent::NewClustMgrAgent Error In Init %v", err)
		return nil, &MsgError{
			err: Error{code: ERROR_CLUSTER_MGR_AGENT_INIT,
				severity: FATAL,
				category: CLUSTER_MGR}}

	}

	mgr.RegisterNotifier(metaNotifier)

	c.metaNotifier = metaNotifier

	//start clustMgrAgent loop which listens to commands from its supervisor
	go c.run()

	//register with Index Manager for notification of metadata updates

	return c, &MsgSuccess{}

}

// RegisterRestEndpoints registers the REST APIs defined in request_handler.go.
func (c *clustMgrAgent) RegisterRestEndpoints() {
	RegisterRequestHandler(c.mgr, GetHTTPMux(), c.config)
}

// run starts the clustmgrAgent loop which listens to messages
// from it supervisor(indexer)
func (c *clustMgrAgent) run() {

	//main ClustMgrAgent loop

	defer func() {
		if rc := recover(); rc != nil {
			panic(rc)
		} else {
			// if there is a panic, close can block the panic until it finishes and close may end
			// up in a deadlock as the clusterMgrAgent loop will not be available and LifecycleMgr
			// could be waiting on a response from the clusterMgrAgent for on-going requests
			c.mgr.Close()
		}
	}()

	defer c.panicHandler()

loop:
	for {
		select {

		case cmd, ok := <-c.supvCmdch:
			if ok {
				if cmd.GetMsgType() == CLUST_MGR_AGENT_SHUTDOWN {
					logging.Infof("ClusterMgrAgent: Shutting Down")
					c.supvCmdch <- &MsgSuccess{}
					break loop
				}
				c.handleSupvervisorCommands(cmd)
			} else {
				//supervisor channel closed. exit
				break loop
			}

		}
	}
}

func (c *clustMgrAgent) handleSupvervisorCommands(cmd Message) {

	switch cmd.GetMsgType() {

	case CLUST_MGR_INDEXER_READY:
		c.handleIndexerReady(cmd)

	case CLUST_MGR_REBALANCE_RUNNING:
		c.handleRebalanceRunning(cmd)

	case CLUST_MGR_UPDATE_TOPOLOGY_FOR_INDEX:
		c.handleUpdateTopologyForIndex(cmd)

	case CLUST_MGR_RESET_INDEX_ON_UPGRADE:
		c.handleResetIndexOnUpgrade(cmd)

	case CLUST_MGR_RESET_INDEX_ON_ROLLBACK:
		c.handleResetIndexOnRollback(cmd)

	case CLUST_MGR_GET_GLOBAL_TOPOLOGY:
		c.handleGetGlobalTopology(cmd)

	case CLUST_MGR_GET_LOCAL:
		c.handleGetLocalValue(cmd)

	case CLUST_MGR_SET_LOCAL:
		c.handleSetLocalValue(cmd)

	case CLUST_MGR_DEL_LOCAL:
		c.handleDelLocalValue(cmd)

	case CLUST_MGR_GET_LOCAL_WITH_PREFIX:
		c.handleGetLocalValuesWithPrefix(cmd)

	case CLUST_MGR_DEL_KEYSPACE:
		c.handleDeleteKeyspace(cmd)

	case CLUST_MGR_CLEANUP_INDEX:
		c.handleCleanupIndex(cmd)

	case UPDATE_INDEX_INSTANCE_MAP:
		c.handleIndexMap(cmd)

	case INDEX_STATS_DONE:
		c.handleStats(cmd)

	case INDEX_STATS_BROADCAST:
		c.handleBroadcastStats(cmd)

	case INDEX_BOOTSTRAP_STATS_UPDATE:
		c.handleUpdateBootstrapStats(cmd)

	case CONFIG_SETTINGS_UPDATE:
		c.handleConfigUpdate(cmd)

	case CLUST_MGR_CLEANUP_PARTITION:
		c.handleCleanupPartition(cmd)

	case CLUST_MGR_MERGE_PARTITION:
		c.handleMergePartition(cmd)

	case INDEXER_SECURITY_CHANGE:
		c.handleSecurityChange(cmd)

	case UPDATE_REBALANCE_PHASE:
		c.handleUpdateRebalancePhase(cmd)

	case CLUST_MGR_INST_ASYNC_RECOVERY_DONE:
		c.handleInstAsyncRecoveryDone(cmd)

	default:
		logging.Errorf("ClusterMgrAgent::handleSupvervisorCommands Unknown Message %v", cmd)
	}

}

func (c *clustMgrAgent) handleUpdateTopologyForIndex(cmd Message) {

	logging.Infof("ClustMgr:handleUpdateTopologyForIndex %v", cmd)

	indexList := cmd.(*MsgClustMgrUpdate).GetIndexList()
	updatedFields := cmd.(*MsgClustMgrUpdate).GetUpdatedFields()
	syncUpdate := cmd.(*MsgClustMgrUpdate).GetIsSyncUpdate()
	respCh := cmd.(*MsgClustMgrUpdate).GetRespCh()

	for _, index := range indexList {
		updatedState := common.INDEX_STATE_NIL
		updatedStream := common.NIL_STREAM
		updatedError := ""
		updatedRState := common.REBAL_NIL
		updatedPartitions := []uint64(nil)
		updatedVersions := []int(nil)
		updatedInstVersion := -1
		updatedShardIds := make(common.PartnShardIdMap)
		partnShardMap := updatedFields.partnShardIdMap

		if updatedFields.state {
			updatedState = index.State
		}
		if updatedFields.stream {
			updatedStream = index.Stream
		}
		if updatedFields.err {
			updatedError = index.Error
		}
		if updatedFields.rstate {
			updatedRState = index.RState
		}
		if updatedFields.partitions {
			for _, partition := range index.Pc.GetAllPartitions() {
				partnId := partition.GetPartitionId()
				updatedPartitions = append(updatedPartitions, uint64(partnId))
				updatedVersions = append(updatedVersions, int(partition.GetVersion()))
				if len(partnShardMap) > 0 {
					updatedShardIds[partnId] = partnShardMap[partnId]
				}
			}
		}
		if updatedFields.version {
			updatedInstVersion = index.Version
		}

		updatedBuildTs := index.BuildTs
		updatedTrainingPhase := index.TrainingPhase

		var err error
		if syncUpdate {
			go func() {
				err = c.mgr.UpdateIndexInstanceSync(index.Defn.Bucket, index.Defn.Scope, index.Defn.Collection,
					index.Defn.DefnId, index.InstId, updatedState, updatedStream, updatedError, updatedBuildTs,
					updatedRState, updatedPartitions, updatedVersions, updatedInstVersion, updatedShardIds,
					updatedTrainingPhase)
				respCh <- err
			}()
		} else {
			err = c.mgr.UpdateIndexInstance(index.Defn.Bucket, index.Defn.Scope, index.Defn.Collection,
				index.Defn.DefnId, index.InstId, updatedState, updatedStream, updatedError, updatedBuildTs,
				updatedRState, updatedPartitions, updatedVersions, updatedInstVersion, updatedShardIds,
				updatedTrainingPhase)
		}
		common.CrashOnError(err)
	}

	c.supvCmdch <- &MsgSuccess{}

}

func (c *clustMgrAgent) handleCleanupPartition(cmd Message) {

	logging.Infof("ClustMgr:handleCleanupPartition%v", cmd)

	msg := cmd.(*MsgClustMgrCleanupPartition)
	defn := msg.GetDefn()

	defn.InstId = msg.GetInstId()
	defn.ReplicaId = msg.GetReplicaId()
	defn.Partitions = append(defn.Partitions, msg.GetPartitionId())

	if err := c.mgr.CleanupPartition(defn, msg.UpdateStatusOnly()); err != nil {
		common.CrashOnError(err)
	}

	c.supvCmdch <- &MsgSuccess{}
}

func (c *clustMgrAgent) handleMergePartition(cmd Message) {

	logging.Infof("ClustMgr:handleMergePartition%v", cmd)

	defnId := cmd.(*MsgClustMgrMergePartition).GetDefnId()
	srcInstId := cmd.(*MsgClustMgrMergePartition).GetSrcInstId()
	srcRState := cmd.(*MsgClustMgrMergePartition).GetSrcRState()
	tgtInstId := cmd.(*MsgClustMgrMergePartition).GetTgtInstId()
	tgtPartitions := cmd.(*MsgClustMgrMergePartition).GetTgtPartitions()
	tgtVersions := cmd.(*MsgClustMgrMergePartition).GetTgtVersions()
	tgtInstVersion := cmd.(*MsgClustMgrMergePartition).GetTgtInstVersion()
	respch := cmd.(*MsgClustMgrMergePartition).GetRespch()

	go func() {
		respch <- c.mgr.MergePartition(defnId, srcInstId, srcRState, tgtInstId, tgtInstVersion, tgtPartitions, tgtVersions)
	}()

	c.supvCmdch <- &MsgSuccess{}
}

func (c *clustMgrAgent) handleSecurityChange(cmd Message) {

	err := c.mgr.ResetConnections(c.metaNotifier)
	if err != nil {
		idxErr := Error{
			code:     ERROR_INDEXER_INTERNAL_ERROR,
			severity: FATAL,
			cause:    err,
			category: INDEXER,
		}
		c.supvCmdch <- &MsgError{err: idxErr}
		return
	}

	c.supvCmdch <- &MsgSuccess{}
}

func (c *clustMgrAgent) handleResetIndexOnUpgrade(cmd Message) {

	logging.Infof("ClustMgr:handleResetIndexUpgrade %v", cmd)

	index := cmd.(*MsgClustMgrResetIndexOnUpgrade).GetIndex()

	if err := c.mgr.ResetIndex(index); err != nil {
		common.CrashOnError(err)
	}

	c.supvCmdch <- &MsgSuccess{}
}

func (c *clustMgrAgent) handleResetIndexOnRollback(cmd Message) {

	logging.Infof("ClustMgr:handleResetIndexOnRollback %v", cmd)

	index := cmd.(*MsgClustMgrResetIndexOnRollback).GetIndex()
	respch := cmd.(*MsgClustMgrResetIndexOnRollback).GetRespch()

	go func() {
		respch <- c.mgr.ResetIndexOnRollback(index)
	}()

	c.supvCmdch <- &MsgSuccess{}
}

func (c *clustMgrAgent) handleIndexMap(cmd Message) {

	req := cmd.(*MsgUpdateInstMap)
	updatedInsts := req.GetUpdatedInsts()
	if len(updatedInsts) > 0 {
		logging.Infof("ClustMgr::handleIndexMap, updated instances: %v", updatedInsts)
	}
	deletedInsts := req.GetDeletedInstIds()
	if len(deletedInsts) > 0 {
		logging.Infof("ClustMgr::handleIndexMap, deleted instance id's: %v", deletedInsts)
	}
	logging.Tracef("ClustMgr:handleIndexMap %v", cmd)

	statsObj := cmd.(*MsgUpdateInstMap).GetStatsObject()
	if statsObj != nil {
		c.stats.Set(statsObj)
	}

	c.supvCmdch <- &MsgSuccess{}
}

func (c *clustMgrAgent) handleStats(cmd Message) {

	c.supvCmdch <- &MsgSuccess{}

	c.handleStatsInternal()
	atomic.AddUint64(&c.statsCount, 1)
}

func (c *clustMgrAgent) handleStatsInternal() {

	stats := c.stats.Get()
	if stats != nil {
		spec := NewStatsSpec(false, false, false, false, false, nil)
		spec.OverrideFilter("gsiClient") // Get only the stats related to GSI client
		filteredStats := stats.GetStats(spec, nil)
		if val, ok := filteredStats.(map[string]interface{}); ok {
			c.mgr.NotifyStats(val)
		} else {
			logging.Fatalf("clustMgrAgent:handleStatsInternal, Invalid type of stats, for spec: %v", spec)
		}
	}
}

func (c *clustMgrAgent) handleBroadcastStats(cmd Message) {

	c.supvCmdch <- &MsgSuccess{}
	stats := cmd.(*MsgStatsRequest).GetStats()
	if stats != nil {
		c.mgr.NotifyStats(stats)
	}
}

func (c *clustMgrAgent) handleUpdateBootstrapStats(cmd Message) {

	c.supvCmdch <- &MsgSuccess{}
	stats := cmd.(*MsgStatsRequest).GetStats()
	if stats != nil {
		c.mgr.UpdateStats(stats)
	}
}

func (c *clustMgrAgent) handleConfigUpdate(cmd Message) {

	logging.Infof("ClustMgr:handleConfigUpdate")
	c.supvCmdch <- &MsgSuccess{}

	cfgUpdate := cmd.(*MsgConfigUpdate)
	config := cfgUpdate.GetConfig()
	err := c.mgr.NotifyConfigUpdate(config)
	if err != nil {
		logging.Errorf("clusterMgrAgent: NotifyConfigUpdate(config) returned err %v", err)
	}
}

func (c *clustMgrAgent) handleGetGlobalTopology(cmd Message) {

	logging.Infof("ClustMgr:handleGetGlobalTopology %v", cmd)

	//get the latest topology from manager
	metaIter, err := c.mgr.NewIndexDefnIterator()
	if err != nil {
		common.CrashOnError(err)
	}
	defer metaIter.Close()

	indexInstMap := make(common.IndexInstMap)
	topoCache := make(map[string]map[string]map[string]*manager.IndexTopology)

	var delTokens map[common.IndexDefnId]*mc.DeleteCommandToken
	delTokens, err = mc.FetchIndexDefnToDeleteCommandTokensMap()
	if err != nil {
		logging.Warnf("ClustMgr:handleGetGlobalTopology: Error in FetchIndexDefnToDeleteCommandTokensMap %v", err)
	}

	var dropTokens map[common.IndexDefnId][]*mc.DropInstanceCommandToken
	dropTokens, err = mc.FetchIndexDefnToDropInstanceCommandTokenMap()
	if err != nil {
		logging.Warnf("ClustMgr:handleGetGlobalTopology: Error in FetchIndexDefnToDropInstanceCommandTokenMap %v", err)
	}

	for _, defn, err := metaIter.Next(); err == nil; _, defn, err = metaIter.Next() {

		var idxDefn common.IndexDefn
		idxDefn = *defn

		t := topoCache[idxDefn.Bucket][idxDefn.Scope][idxDefn.Collection]
		if t == nil {
			t, err = c.mgr.GetTopologyByCollection(idxDefn.Bucket, idxDefn.Scope, idxDefn.Collection)
			if err != nil {
				common.CrashOnError(err)
			}
			if _, ok := topoCache[idxDefn.Bucket]; !ok {
				topoCache[idxDefn.Bucket] = make(map[string]map[string]*manager.IndexTopology)
			}
			if _, ok := topoCache[idxDefn.Bucket][idxDefn.Scope]; !ok {
				topoCache[idxDefn.Bucket][idxDefn.Scope] = make(map[string]*manager.IndexTopology)
			}
			topoCache[idxDefn.Bucket][idxDefn.Scope][idxDefn.Collection] = t
		}
		if t == nil {
			logging.Warnf("ClustMgr:handleGetGlobalTopology Index Instance Not "+
				"Found For Index Definition %v. Ignored.", idxDefn)
			continue
		}

		insts := t.GetIndexInstancesByDefn(idxDefn.DefnId)

		if len(insts) == 0 {
			logging.Warnf("ClustMgr:handleGetGlobalTopology Index Instance Not "+
				"Found For Index Definition %v. Ignored.", idxDefn)
			continue
		}

		// init Desc if missing (pre-Spock indexes did not have it)
		if idxDefn.Desc == nil {
			idxDefn.Desc = make([]bool, len(idxDefn.SecExprs))
		}

		// Populate IsPartnKeyDocId on bootstrap (Pre 7.5 indexes does not have this field)
		idxDefn.IsPartnKeyDocId = queryutil.IsPartnKeyDocId(idxDefn.PartitionKeys)

		for _, inst := range insts {

			// create partitions
			partitions := make([]common.PartitionId, len(inst.Partitions))
			versions := make([]int, len(inst.Partitions))
			shardIds := make([][]common.ShardId, len(inst.Partitions))
			alternateShardIds := make(map[common.PartitionId][]string)
			for i, partn := range inst.Partitions {
				partitions[i] = common.PartitionId(partn.PartId)
				versions[i] = int(partn.Version)
				shardIds[i] = []common.ShardId(partn.ShardIds)
				alternateShardIds[common.PartitionId(partn.PartId)] = partn.AlternateShardIds
			}
			pc := c.metaNotifier.makeDefaultPartitionContainer(partitions, versions, shardIds,
				inst.NumPartitions, idxDefn.PartitionScheme, idxDefn.HashScheme)

			// Populate alterante shardIds for this instance in the index definition
			idxDefn.AlternateShardIds = alternateShardIds

			// create index instance
			idxInst := common.IndexInst{
				InstId:         common.IndexInstId(inst.InstId),
				Defn:           idxDefn,
				State:          common.IndexState(inst.State),
				Stream:         common.StreamId(inst.StreamId),
				ReplicaId:      int(inst.ReplicaId),
				Version:        int(inst.Version),
				RState:         common.RebalanceState(inst.RState),
				Scheduled:      inst.Scheduled,
				StorageMode:    inst.StorageMode,
				OldStorageMode: inst.OldStorageMode,
				Pc:             pc,
				RealInstId:     common.IndexInstId(inst.RealInstId),
				TrainingPhase:  inst.TrainingPhase,
			}

			if idxInst.State != common.INDEX_STATE_DELETED {
				var exist1, exist2 bool
				var err error

				if delTokens != nil {
					_, exist1 = delTokens[idxDefn.DefnId]
				} else {
					exist1, err = mc.DeleteCommandTokenExist(idxDefn.DefnId)
					if err != nil {
						logging.Warnf("Error when reading delete command token for defn %v", idxDefn.DefnId)
					}
				}

				if dropTokens != nil {
					dropTokenList := dropTokens[idxDefn.DefnId]
					for _, dropToken := range dropTokenList {
						if dropToken.InstId == idxDefn.InstId {
							exist2 = true
							break
						}
					}
				} else {
					exist2, err = mc.DropInstanceCommandTokenExist(idxDefn.DefnId, idxInst.InstId)
					if err != nil {
						logging.Warnf("Error when reading drop command token for index (%v, %v)", idxDefn.DefnId, idxInst.InstId)
					}
				}

				if exist1 || exist2 {
					idxInst.State = common.INDEX_STATE_DELETED
					logging.Infof("Delete token found for index (%v, %v).  Setting index state to DELETED", idxDefn.DefnId, idxInst.InstId)
				}
			}

			indexInstMap[idxInst.InstId] = idxInst
		}
	}

	c.supvCmdch <- &MsgClustMgrTopology{indexInstMap: indexInstMap}
}

func (c *clustMgrAgent) handleGetLocalValue(cmd Message) {

	key := cmd.(*MsgClustMgrLocal).GetKey()

	logging.Infof("ClustMgr:handleGetLocalValue Key %v", key)

	val, err := c.mgr.GetLocalValue(key)

	c.supvCmdch <- &MsgClustMgrLocal{
		mType: CLUST_MGR_GET_LOCAL,
		key:   key,
		value: val,
		err:   err,
	}

}

func (c *clustMgrAgent) handleSetLocalValue(cmd Message) {

	key := cmd.(*MsgClustMgrLocal).GetKey()
	val := cmd.(*MsgClustMgrLocal).GetValue()

	logging.Infof("ClustMgr:handleSetLocalValue Key %v Value %v", key, val)

	err := c.mgr.SetLocalValue(key, val)

	c.supvCmdch <- &MsgClustMgrLocal{
		mType: CLUST_MGR_SET_LOCAL,
		key:   key,
		value: val,
		err:   err,
	}

}

func (c *clustMgrAgent) handleDelLocalValue(cmd Message) {

	key := cmd.(*MsgClustMgrLocal).GetKey()

	logging.Infof("ClustMgr:handleDelLocalValue Key %v", key)

	if key == RebalanceRunning { // Indicate that rebalance is done at lifecycle manager
		c.mgr.RebalanceDone()
	}

	err := c.mgr.DeleteLocalValue(key)

	c.supvCmdch <- &MsgClustMgrLocal{
		mType: CLUST_MGR_DEL_LOCAL,
		key:   key,
		err:   err,
	}

}

func (c *clustMgrAgent) handleGetLocalValuesWithPrefix(cmd Message) {

	key := cmd.(*MsgClustMgrLocal).GetKey()

	logging.Infof("ClustMgr:handleGetLocalValuesWithPrefix Key %v", key)

	// Use key as prefix
	values, err := c.mgr.GetLocalValuesWithKeyPrefix(key)

	c.supvCmdch <- &MsgClustMgrLocal{
		mType:  CLUST_MGR_GET_LOCAL_WITH_PREFIX,
		key:    key,
		values: values,
		err:    err,
	}

}

func (c *clustMgrAgent) handleDeleteKeyspace(cmd Message) {

	logging.Infof("ClustMgr:handleDeleteKeyspace %v", cmd)

	bucket := cmd.(*MsgClustMgrUpdate).GetBucket()
	scope := cmd.(*MsgClustMgrUpdate).GetScope()
	collection := cmd.(*MsgClustMgrUpdate).GetCollection()
	streamId := cmd.(*MsgClustMgrUpdate).GetStreamId()

	var err error
	if collection == "" {
		err = c.mgr.DeleteIndexForBucket(bucket, streamId)
	} else {
		err = c.mgr.DeleteIndexForCollection(bucket, scope, collection, streamId)
	}

	common.CrashOnError(err)

	c.supvCmdch <- &MsgSuccess{}
}

func (c *clustMgrAgent) handleCleanupIndex(cmd Message) {

	logging.Infof("ClustMgr:handleCleanupIndex %v", cmd)

	index := cmd.(*MsgClustMgrUpdate).GetIndexList()[0]

	err := c.mgr.CleanupIndex(index)
	common.CrashOnError(err)

	c.supvCmdch <- &MsgSuccess{}
}

func (c *clustMgrAgent) handleIndexerReady(cmd Message) {

	logging.Infof("ClustMgr:handleIndexerReady %v", cmd)

	err := c.mgr.NotifyIndexerReady()
	common.CrashOnError(err)

	c.supvCmdch <- &MsgSuccess{}
}

func (c *clustMgrAgent) handleRebalanceRunning(cmd Message) {

	logging.Infof("ClustMgr:handleRebalanceRunning %v", cmd)

	c.mgr.RebalanceRunning()
	c.supvCmdch <- &MsgSuccess{}
}

func (c *clustMgrAgent) handleUpdateRebalancePhase(cmd Message) {
	logging.Infof("ClustMgr:handleUpdateRebalancePhase %v", cmd)

	globalRebalPhase := cmd.(*MsgUpdateRebalancePhase).GetGlobalRebalancePhase()
	bucketTransferPhase := cmd.(*MsgUpdateRebalancePhase).GetBucketTransferPhase()

	c.mgr.UpdateRebalancePhase(globalRebalPhase, bucketTransferPhase)
	c.supvCmdch <- &MsgSuccess{}
}

func (c *clustMgrAgent) handleInstAsyncRecoveryDone(cmd Message) {
	logging.Infof("ClustMgr:handleInstAsyncRecoveryDone %v", cmd)

	index := cmd.(*MsgClustMgrUpdate).GetIndexList()[0]

	err := c.mgr.NotifyInstAsyncRecoveryDone(index)
	common.CrashOnError(err)

	c.supvCmdch <- &MsgSuccess{}
}

// panicHandler handles the panic from index manager
func (c *clustMgrAgent) panicHandler() {

	//panic recovery
	if rc := recover(); rc != nil {
		var err error
		switch x := rc.(type) {
		case string:
			err = errors.New(x)
		case error:
			err = x
		default:
			err = errors.New("Unknown panic")
		}

		logging.Fatalf("ClusterMgrAgent Panic Err %v", err)
		logging.Fatalf("%s", logging.StackTrace())

		//at this point, the state of index metadata is unknown.
		//restart indexer to return to a stable state.
		//the only action supervisor can take at this point is
		//to restart as well.
		panic(rc)
	}

}

type metaNotifier struct {
	adminCh MsgChannel
	config  common.Config
	mgr     *clustMgrAgent
}

func NewMetaNotifier(adminCh MsgChannel, config common.Config, mgr *clustMgrAgent) *metaNotifier {

	if adminCh == nil {
		return nil
	}

	return &metaNotifier{
		adminCh: adminCh,
		config:  config,
		mgr:     mgr,
	}

}

func (meta *metaNotifier) OnIndexCreate(indexDefn *common.IndexDefn, instId common.IndexInstId,
	replicaId int, partitions []common.PartitionId, versions []int, numPartitions uint32, realInstId common.IndexInstId,
	reqCtx *common.MetadataRequestContext) (common.PartnShardIdMap, error) {
	const _OnIndexCreate = "clustMgrAgent::OnIndexCreate:"

	logging.Infof("%v Notification received for Create Index:"+
		" instId %v, indexDefn %+v, reqCtx %+v, partitions %v",
		_OnIndexCreate, instId, indexDefn, reqCtx, partitions)

	pc := meta.makeDefaultPartitionContainer(partitions, versions, nil, numPartitions, indexDefn.PartitionScheme, indexDefn.HashScheme)

	idxInst := common.IndexInst{InstId: instId,
		Defn:       *indexDefn,
		State:      common.INDEX_STATE_CREATED,
		Pc:         pc,
		ReplicaId:  replicaId,
		RealInstId: realInstId,
		Version:    indexDefn.InstVersion,
	}

	if idxInst.Defn.InstVersion != 0 {
		idxInst.RState = common.REBAL_PENDING
	}

	respCh := make(MsgChannel)

	meta.adminCh <- &MsgCreateIndex{mType: CLUST_MGR_CREATE_INDEX_DDL,
		indexInst: idxInst,
		respCh:    respCh,
		reqCtx:    reqCtx}

	//wait for response
	if res, ok := <-respCh; ok {

		switch res.GetMsgType() {

		case UPDATE_SHARDID_MAP:
			partnShardIdMap := (res).(*MsgUpdateShardIds).GetShardIds()
			logging.Infof("%v Success for Create Index: instId %v, partnShardIdMap: %v, indexDefn %+v",
				_OnIndexCreate, instId, partnShardIdMap, indexDefn)
			return partnShardIdMap, nil

		case MSG_ERROR:
			err := res.(*MsgError).GetError()
			logging.Errorf("%v Error for Create Index: instId %v, indexDefn %+v. Error: %+v.",
				_OnIndexCreate, instId, indexDefn, err)
			return nil, &common.IndexerError{Reason: err.String(), Code: err.convertError()}

		default:
			logging.Fatalf("%v Unknown response received for Create Index:"+
				" instId %v, indexDefn %+v. Response: %+v.",
				_OnIndexCreate, instId, indexDefn, res)
			common.CrashOnError(errors.New("Unknown Response"))

		}

	} else {
		logging.Fatalf("%v Unexpected channel close for Create Index: instId %v, indexDefn %+v",
			_OnIndexCreate, instId, indexDefn)
		common.CrashOnError(errors.New("Unknown Response"))
	}

	return nil, nil
}

func (meta *metaNotifier) OnIndexRecover(indexDefn *common.IndexDefn, instId common.IndexInstId,
	replicaId int, partitions []common.PartitionId, versions []int, numPartitions uint32, realInstId common.IndexInstId,
	reqCtx *common.MetadataRequestContext, cancelCh chan bool) error {
	const _OnIndexCreate = "clustMgrAgent::OnIndexRecover:"

	logging.Infof("clustMgrAgent::OnIndexRecover Notification received for "+
		"Recover Index: instId %v,realInstId: %v, indexDefn %+v, reqCtx %+v, partitions %v",
		instId, realInstId, indexDefn, reqCtx, partitions)

	// The shardIds in partition container will be updated after
	// index is completely recovered into slice
	pc := meta.makeDefaultPartitionContainer(partitions, versions, nil, numPartitions, indexDefn.PartitionScheme, indexDefn.HashScheme)

	idxInst := common.IndexInst{InstId: instId,
		Defn:       *indexDefn,
		State:      common.INDEX_STATE_CREATED,
		Pc:         pc,
		ReplicaId:  replicaId,
		RealInstId: realInstId,
		Version:    indexDefn.InstVersion,
	}

	if idxInst.Defn.InstVersion != 0 {
		idxInst.RState = common.REBAL_PENDING
	}

	respCh := make(MsgChannel)

	meta.adminCh <- &MsgRecoverIndex{mType: CLUST_MGR_RECOVER_INDEX,
		indexInst: idxInst,
		respCh:    respCh,
		cancelCh:  cancelCh,
		reqCtx:    reqCtx}

	//wait for response
	if res, ok := <-respCh; ok {

		switch res.GetMsgType() {

		case MSG_SUCCESS:
			logging.Infof("clustMgrAgent::OnIndexRecover: Successfully initiated "+
				"for index recovery: instId %v, indexDefn %+v", instId, indexDefn)
			return nil

		case MSG_ERROR:
			err := res.(*MsgError).GetError()
			logging.Errorf("clustMgrAgent::OnIndexRecover: Error for "+
				"index recovery: instId %v, indexDefn %+v. Error: %+v.", instId, indexDefn, err)
			return &common.IndexerError{Reason: err.String(), Code: err.convertError()}

		default:
			logging.Fatalf("clustMgrAgent::OnIndexRecover: Unknown response received "+
				"for index recovery: instId %v, indexDefn %+v. Response: %+v.",
				instId, indexDefn, res)
			common.CrashOnError(errors.New("Unknown Response"))

		}

	} else {
		logging.Fatalf("clustMgrAgent::OnIndexRecover: Unexpected channel close "+
			"for index recovery: instId %v, indexDefn %+v", instId, indexDefn)
		common.CrashOnError(errors.New("Unknown Response"))
	}

	return nil
}

func (meta *metaNotifier) OnIndexBuild(indexInstList []common.IndexInstId,
	buckets []string, isEmptyNodeBatch bool, reqCtx *common.MetadataRequestContext) map[common.IndexInstId]error {

	logging.Infof("clustMgrAgent::OnIndexBuild Notification "+
		"Received for Build Index %v %v %v", indexInstList, reqCtx, isEmptyNodeBatch)

	respCh := make(MsgChannel)

	meta.adminCh <- &MsgBuildIndex{mType: CLUST_MGR_BUILD_INDEX_DDL,
		indexInstList:    indexInstList,
		respCh:           respCh,
		bucketList:       buckets,
		reqCtx:           reqCtx,
		isEmptyNodeBatch: isEmptyNodeBatch}

	//wait for response
	if res, ok := <-respCh; ok {

		switch res.GetMsgType() {

		case CLUST_MGR_BUILD_INDEX_DDL_RESPONSE:
			errMap := res.(*MsgBuildIndexResponse).GetErrorMap()
			logging.Infof("clustMgrAgent::OnIndexBuild returns "+
				"for Build Index %v", indexInstList)
			return errMap

		case MSG_ERROR:
			logging.Errorf("clustMgrAgent::OnIndexBuild Error "+
				"for Build Index %v. Error %v.", indexInstList, res)
			err := res.(*MsgError).GetError()
			errMap := make(map[common.IndexInstId]error)
			for _, instId := range indexInstList {
				errMap[instId] = &common.IndexerError{Reason: err.String(), Code: err.convertError()}
			}

			return errMap

		default:
			logging.Fatalf("clustMgrAgent::OnIndexBuild Unknown Response "+
				"Received for Build Index %v. Response %v", indexInstList, res)
			common.CrashOnError(errors.New("Unknown Response"))

		}

	} else {

		logging.Fatalf("clustMgrAgent::OnIndexBuild Unexpected Channel Close "+
			"for Create Index %v", indexInstList)
		common.CrashOnError(errors.New("Unknown Response"))
	}

	return nil
}

func (meta *metaNotifier) OnRecoveredIndexBuild(indexInstList []common.IndexInstId,
	buckets []string, reqCtx *common.MetadataRequestContext) map[common.IndexInstId]error {

	logging.Infof("clustMgrAgent::OnRecoveredIndexBuild Notification "+
		"Received for Build Index %v %v", indexInstList, reqCtx)

	respCh := make(MsgChannel)

	meta.adminCh <- &MsgBuildIndex{mType: CLUST_MGR_BUILD_RECOVERED_INDEXES,
		indexInstList: indexInstList,
		respCh:        respCh,
		bucketList:    buckets,
		reqCtx:        reqCtx}

	//wait for response
	if res, ok := <-respCh; ok {

		switch res.GetMsgType() {

		case CLUST_MGR_BUILD_INDEX_DDL_RESPONSE:
			errMap := res.(*MsgBuildIndexResponse).GetErrorMap()
			logging.Infof("clustMgrAgent::OnRecoveredIndexBuild returns "+
				"for Build Index %v", indexInstList)
			return errMap

		case MSG_ERROR:
			logging.Errorf("clustMgrAgent::OnRecoveredIndexBuild Error "+
				"for Build Index %v. Error %v.", indexInstList, res)
			err := res.(*MsgError).GetError()
			errMap := make(map[common.IndexInstId]error)
			for _, instId := range indexInstList {
				errMap[instId] = &common.IndexerError{Reason: err.String(), Code: err.convertError()}
			}

			return errMap

		default:
			logging.Fatalf("clustMgrAgent::OnRecoveredIndexBuild Unknown Response "+
				"Received for Build Index %v. Response %v", indexInstList, res)
			common.CrashOnError(errors.New("Unknown Response"))

		}

	} else {

		logging.Fatalf("clustMgrAgent::OnRecoveredIndexBuild Unexpected Channel Close "+
			"for Create Index %v", indexInstList)
		common.CrashOnError(errors.New("Unknown Response"))
	}

	return nil
}

func (meta *metaNotifier) OnIndexDelete(instId common.IndexInstId,
	bucket string, reqCtx *common.MetadataRequestContext) error {

	logging.Infof("clustMgrAgent::OnIndexDelete Notification "+
		"Received for Drop IndexId %v %v", instId, reqCtx)

	respCh := make(MsgChannel)

	//Treat DefnId as InstId for now
	meta.adminCh <- &MsgDropIndex{mType: CLUST_MGR_DROP_INDEX_DDL,
		indexInstId: instId,
		respCh:      respCh,
		keyspaceId:  bucket,
		reqCtx:      reqCtx}

	//wait for response
	if res, ok := <-respCh; ok {

		switch res.GetMsgType() {

		case MSG_SUCCESS:
			logging.Infof("clustMgrAgent::OnIndexDelete Success "+
				"for Drop IndexId %v", instId)
			return nil

		case MSG_ERROR:
			logging.Errorf("clustMgrAgent::OnIndexDelete Error "+
				"for Drop IndexId %v. Error %v", instId, res)
			err := res.(*MsgError).GetError()
			return &common.IndexerError{Reason: err.String(), Code: err.convertError()}

		default:
			logging.Fatalf("clustMgrAgent::OnIndexDelete Unknown Response "+
				"Received for Drop IndexId %v. Response %v", instId, res)
			common.CrashOnError(errors.New("Unknown Response"))

		}

	} else {
		logging.Fatalf("clustMgrAgent::OnIndexDelete Unexpected Channel Close "+
			"for Drop IndexId %v", instId)
		common.CrashOnError(errors.New("Unknown Response"))

	}

	return nil
}

func (meta *metaNotifier) OnPartitionPrune(instId common.IndexInstId, partitions []common.PartitionId, reqCtx *common.MetadataRequestContext) error {

	logging.Infof("clustMgrAgent::OnPartitionPrune Notification "+
		"Received for Prune Partition IndexId %v %v %v", instId, partitions, reqCtx)

	respCh := make(MsgChannel)

	//Treat DefnId as InstId for now
	meta.adminCh <- &MsgClustMgrPrunePartition{
		instId:     instId,
		partitions: partitions,
		respCh:     respCh}

	//wait for response
	if res, ok := <-respCh; ok {

		switch res.GetMsgType() {

		case MSG_SUCCESS:
			logging.Infof("clustMgrAgent::OnPrunePartition Success "+
				"for IndexId %v", instId)
			return nil

		case MSG_ERROR:
			logging.Errorf("clustMgrAgent::OnPrunePartition Error "+
				"for IndexId %v. Error %v", instId, res)
			err := res.(*MsgError).GetError()
			return &common.IndexerError{Reason: err.String(), Code: err.convertError()}

		default:
			logging.Fatalf("clustMgrAgent::OnPrunePartition Unknown Response "+
				"Received for IndexId %v. Response %v", instId, res)
			common.CrashOnError(errors.New("Unknown Response"))

		}

	} else {
		logging.Fatalf("clustMgrAgent::OnPrunePartition Unexpected Channel Close "+
			"for IndexId %v", instId)
		common.CrashOnError(errors.New("Unknown Response"))
	}

	return nil
}

func (meta *metaNotifier) OnFetchStats() error {

	go meta.fetchStats()

	return nil
}

func (meta *metaNotifier) fetchStats() {

	ticker := time.NewTicker(time.Millisecond * 500)
	defer ticker.Stop()
	startTime := time.Now()

	for range ticker.C {
		if meta.mgr.stats.Get() != nil && atomic.LoadUint64(&meta.mgr.statsCount) != 0 {
			meta.mgr.handleStatsInternal()
			logging.Infof("Fetch new stats upon request by life cycle manager")
			return
		}

		if time.Now().Sub(startTime) > time.Minute {
			return
		}
	}
}

func (meta *metaNotifier) makeDefaultPartitionContainer(partitions []common.PartitionId, versions []int,
	shardIds [][]common.ShardId, numPartitions uint32,
	scheme common.PartitionScheme, hash common.HashScheme) common.PartitionContainer {

	pc := common.NewKeyPartitionContainer(int(numPartitions), scheme, hash)

	//Add one partition for now
	addr := net.JoinHostPort("", meta.config["streamMaintPort"].String())
	endpt := []common.Endpoint{common.Endpoint(addr)}

	for i, partnId := range partitions {
		if shardIds != nil {
			partnDefn := common.KeyPartitionDefn{Id: partnId, Version: versions[i], ShardIds: shardIds[i], Endpts: endpt}
			pc.AddPartition(partnId, partnDefn)
		} else {
			partnDefn := common.KeyPartitionDefn{Id: partnId, Version: versions[i], Endpts: endpt}
			pc.AddPartition(partnId, partnDefn)
		}
	}

	return pc

}
