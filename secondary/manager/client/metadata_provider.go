// Copyright 2014-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"net"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/cbauth"

	"github.com/couchbase/gometa/common"
	gometaL "github.com/couchbase/gometa/log"
	"github.com/couchbase/gometa/message"
	"github.com/couchbase/gometa/protocol"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/common/collections"
	"github.com/couchbase/indexing/secondary/common/queryutil"
	"github.com/couchbase/indexing/secondary/logging"
	mc "github.com/couchbase/indexing/secondary/manager/common"
	"github.com/couchbase/indexing/secondary/planner"
	"github.com/couchbase/indexing/secondary/security"
	"github.com/couchbase/indexing/secondary/transport"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/expression/parser"
)

// TODO:
// 1) cleanup on create/build index fails for replica

///////////////////////////////////////////////////////
// Interface
///////////////////////////////////////////////////////

type Settings interface {
	NumReplica() int32
	NumPartition() int32
	StorageMode() string
	UsePlanner() bool
	AllowPartialQuorum() bool
	AllowScheduleCreate() bool
	AllowScheduleCreateRebal() bool
	WaitForScheduledIndex() bool
	UseGreedyPlanner() bool
	MemHighThreshold() int32
	MemLowThreshold() int32
	ServerlessIndexLimit() uint32
}

///////////////////////////////////////////////////////
// Type Definition
///////////////////////////////////////////////////////

type MetadataProvider struct {
	clusterUrl         string
	providerId         string
	watchers           map[c.IndexerId]*watcher
	pendings           map[c.IndexerId]chan bool
	timeout            int64
	repo               *metadataRepo
	mutex              sync.RWMutex
	watcherCount       int
	metaNotifyCh       chan bool
	numExpectedWatcher int32
	numFailedNode      int32
	numUnhealthyNode   int32
	numAddNode         int32
	numWatcher         int32
	settings           Settings
	indexerVersion     uint64
	clusterVersion     uint64
	internalVersion    c.InternalVersion
	statsNotifyCh      chan map[c.IndexInstId]map[c.PartitionId]c.Statistics
	limitsCfg          *c.LimitsCache
}

//
// 1) Each index definition has a logical identifer (IndexDefnId).
// 2) The logical definition can have multiple instances or replica.
//    Each index instance is identified by IndexInstId.
// 3) Each instance may reside in different nodes for HA or
//    load balancing purpose.
// 4) Each instance can have different version.  Many versions can
//    co-exist in the cluster at a given time, but only one version can be
//    active (State == active) and valid (RState = active).
// 5) In steady state, there should be only one version for each instance, but
//    during rebalance, index can be moved from one node to another, with
//    multiple versions representing the same index instance being "in-transit"
//    (occupying both source and destination nodes during rebalancing).
// 6) A definition can have multiple physical identical copies, residing
//    along with each instance.  The physical copies will have the same
//    definition id as well as definition/structure.
// 7) An observer (metadataRepo) can only determine the "consistent" state of
//    metadata with a full participation.  Full participation means that the obsever
//    see the local metadata state of each indexer node.
// 8) At full participation, if an index definiton does not have any instance, the
//    index definition is considered as deleted.    The side effect is an index
//    could be implicitly dropped if it loses all its replica.
// 9) For partitioned index, each index instance will be distributed across many
//    nodes.  An index instance is well-formed if the observer can account for
//    all the partitions for the instance.
// 10) For partitioned index, each partition will have its own version.  Each
//     partition will be rebalanced separately.
//
type metadataRepo struct {
	provider    *MetadataProvider
	definitions map[c.IndexDefnId]*c.IndexDefn
	instances   map[c.IndexDefnId]map[c.IndexInstId]map[c.PartitionId]map[uint64]*mc.IndexInstDistribution
	indices     map[c.IndexDefnId]*IndexMetadata
	topology    map[c.IndexerId]map[c.IndexDefnId]bool
	version     uint64
	mutex       sync.RWMutex
	notifiers   map[c.IndexDefnId]*event
}

type watcher struct {
	provider     *MetadataProvider
	leaderAddr   string
	factory      protocol.MsgFactory
	pendings     map[common.Txnid]protocol.LogEntryMsg
	killch       chan bool
	alivech      chan bool
	pingch       chan bool
	mutex        sync.Mutex
	timerKillCh  chan bool
	isClosed     bool
	serviceMap   *ServiceMap
	lastSeenTxid common.Txnid
	authHost     string
	aParams      *authParams
	clusterUrl   string

	incomingReqs chan *protocol.RequestHandle
	pendingReqs  map[uint64]*protocol.RequestHandle // key : request id
	loggedReqs   map[common.Txnid]*protocol.RequestHandle

	clientStats IndexStats2Holder // local cached complete version showing all buckets and indexes that exist
	storeStats  bool
}

// With partitioning, index instance is distributed among indexer nodes.
// IndexMetadata can contain instances that do not have all the partitions,
// even though each instance is eventually consistent if there is no network
// partition.
type IndexMetadata struct {
	Definition       *c.IndexDefn
	Instances        []*InstanceDefn
	InstsInRebalance []*InstanceDefn
	State            c.IndexState
	Error            string
	Scheduled        bool
	ScheduleFailed   bool
	Stats            map[string]interface{}
}

type InstanceDefn struct {
	DefnId        c.IndexDefnId
	InstId        c.IndexInstId
	State         c.IndexState
	Error         string
	IndexerId     map[c.PartitionId]c.IndexerId
	Versions      map[c.PartitionId]uint64
	RState        uint32
	ReplicaId     uint64
	StorageMode   string
	NumPartitions uint32
}

type event struct {
	defnId   c.IndexDefnId
	status   []c.IndexState
	topology map[int]map[c.PartitionId]c.IndexerId
	notifyCh chan error
	all      bool // all instance should satisfy event
}

type IndexerStatus struct {
	Adminport string
	Connected bool
}

type watcherCallback func(string, c.IndexerId, c.IndexerId)

type authParams struct {
	user string
	pass string
}

var REQUEST_CHANNEL_COUNT = 1000

var VALID_PARAM_NAMES = []string{"nodes", "defer_build", "retain_deleted_xattr",
	"num_partition", "num_replica", "docKeySize", "secKeySize", "arrSize", "numDoc", "residentRatio"}

var ErrWaitScheduleTimeout = fmt.Errorf("Timeout in checking for schedule create token.")

func (im *IndexMetadata) Clone() *IndexMetadata {
	return &IndexMetadata{
		Definition:       im.Definition,
		Instances:        im.Instances,
		InstsInRebalance: im.InstsInRebalance,
		Stats:            im.Stats,
		// Immutable fields
		State:          im.State,
		Error:          im.Error,
		Scheduled:      im.Scheduled,
		ScheduleFailed: im.ScheduleFailed,
	}
}

///////////////////////////////////////////////////////
// Public function : MetadataProvider
///////////////////////////////////////////////////////

func NewMetadataProvider(cluster string, providerId string, changeCh chan bool, statsCh chan map[c.IndexInstId]map[c.PartitionId]c.Statistics,
	settings Settings) (s *MetadataProvider, err error) {

	s = new(MetadataProvider)
	s.clusterUrl = cluster
	s.watchers = make(map[c.IndexerId]*watcher)
	s.pendings = make(map[c.IndexerId]chan bool)
	s.repo = newMetadataRepo(s)
	s.timeout = int64(time.Second) * 120
	s.metaNotifyCh = changeCh
	s.statsNotifyCh = statsCh
	s.settings = settings

	s.providerId = providerId
	if err != nil {
		return nil, err
	}
	logging.Debugf("MetadataProvider.NewMetadataProvider(): MetadataProvider follower ID %s", s.providerId)

	cinfo, err := c.FetchNewClusterInfoCache2(cluster, c.DEFAULT_POOL, "NewMetaDataProvider")
	if err != nil {
		return nil, err
	}
	cinfo.FetchNodesAndSvsInfoWithLock()
	s.clusterVersion = cinfo.GetClusterVersion()
	s.internalVersion, _ = c.GetInternalClusterVersion(c.NodesInfoProvider(cinfo), true)

	lc, err1 := c.NewLimitsCache()
	if err1 != nil {
		return nil, err1
	}

	s.limitsCfg = lc

	return s, nil
}

func (o *MetadataProvider) SetTimeout(timeout int64) {
	o.timeout = timeout
}

func (o *MetadataProvider) SetClusterStatus(numExpectedWatcher int, numFailedNode int, numUnhealthyNode int, numAddNode int) {

	if (numExpectedWatcher > -1 && int32(numExpectedWatcher) != atomic.LoadInt32(&o.numExpectedWatcher)) ||
		(numFailedNode > -1 && int32(numFailedNode) != atomic.LoadInt32(&o.numFailedNode)) ||
		(numUnhealthyNode > -1 && int32(numUnhealthyNode) != atomic.LoadInt32(&o.numUnhealthyNode)) ||
		(numAddNode > -1 && int32(numAddNode) != atomic.LoadInt32(&o.numAddNode)) {
		logging.Infof("MetadataProvider.SetClusterStatus(): healthy nodes %v failed node %v unhealthy node %v add node %v",
			numExpectedWatcher, numFailedNode, numUnhealthyNode, numAddNode)
	}

	if numExpectedWatcher != -1 {
		atomic.StoreInt32(&o.numExpectedWatcher, int32(numExpectedWatcher))
	}

	if numFailedNode != -1 {
		atomic.StoreInt32(&o.numFailedNode, int32(numFailedNode))
	}

	if numUnhealthyNode != -1 {
		atomic.StoreInt32(&o.numUnhealthyNode, int32(numUnhealthyNode))
	}

	if numAddNode != -1 {
		atomic.StoreInt32(&o.numAddNode, int32(numAddNode))
	}
}

func (o *MetadataProvider) GetMetadataVersion() uint64 {

	return o.repo.getVersion()
}

func (o *MetadataProvider) IncrementMetadataVersion() {

	o.repo.incrementVersion()
}

func (o *MetadataProvider) WatchMetadata(indexAdminPort string, callback watcherCallback, numExpectedWatcher int) c.IndexerId {

	o.mutex.Lock()
	defer o.mutex.Unlock()

	logging.Infof("MetadataProvider.WatchMetadata(): indexer %v", indexAdminPort)

	atomic.StoreInt32(&o.numExpectedWatcher, int32(numExpectedWatcher))

	for _, watcher := range o.watchers {
		if watcher.getAdminAddr() == indexAdminPort {
			return watcher.getIndexerId()
		}
	}

	// start a watcher to the indexer admin
	watcher, readych := o.startWatcher(indexAdminPort)

	// Wait for indexer to connect for a prime number of ms to prevent retry Timers in watcherServer
	// from all being aligned on harmonics of 1 sec if the network is partitioned. (This used to
	// foreground wait for 1,000 ms which led to "thundering herd" retries.)
	success, _ := watcher.waitForReady(readych, 971, nil)
	if success {
		// if successfully connected, retrieve indexerId
		success, _ = watcher.notifyReady(indexAdminPort, 0, nil)
		if success {
			logging.Infof("WatchMetadata(): successfully reach indexer at %v.", indexAdminPort)
			// watcher succesfully initialized, add it to MetadataProvider
			o.addWatcherNoLock(watcher, c.INDEXER_ID_NIL)
			return watcher.getIndexerId()

		} else {
			// watcher is ready, but no able to read indexerId
			readych = nil
		}
	}

	logging.Infof("WatchMetadata(): unable to reach indexer at %v. Retry in background.", indexAdminPort)

	// watcher is not connected to indexer or fail to get indexer id,
	// create a temporary index id
	o.watcherCount = o.watcherCount + 1
	tempIndexerId := c.IndexerId(fmt.Sprintf("%v_Indexer_Id_%d", indexAdminPort, o.watcherCount))
	killch := make(chan bool, 1)
	o.pendings[tempIndexerId] = killch

	// retry it in the background.  Return a temporary indexerId for book-keeping.
	go o.retryHelper(watcher, readych, indexAdminPort, tempIndexerId, killch, callback)
	return tempIndexerId
}

func (o *MetadataProvider) UnwatchMetadata(indexerId c.IndexerId, numExpectedWatcher int) {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	logging.Infof("UnwatchMetadata(): indexer %v", indexerId)
	defer logging.Infof("UnwatchMetadata(): finish for indexer %v", indexerId)

	atomic.StoreInt32(&o.numExpectedWatcher, int32(numExpectedWatcher))

	watcher, ok := o.watchers[indexerId]
	if !ok {
		killch, ok := o.pendings[indexerId]
		if ok {
			delete(o.pendings, indexerId)
			// notify retryHelper to terminate.  This is for
			// watcher that is still waiting to complete
			// handshake with indexer.
			killch <- true
		}
		return
	}

	delete(o.watchers, indexerId)
	if watcher != nil {
		// must update the number of watcher before cleanupIndices
		atomic.StoreInt32(&o.numWatcher, int32(len(o.watchers)))
		watcher.close()
		watcher.cleanupIndices(o.repo)
	}

	// increment version when unwatch metadata
	o.repo.incrementVersion()
}

//
// Since this function holds the lock, it ensure that
// neither WatchMetadata or UnwatchMetadata is being called.
// It also ensure safety of calling CheckIndexerStatusNoLock.
//
func (o *MetadataProvider) CheckIndexerStatus() []IndexerStatus {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	return o.CheckIndexerStatusNoLock()
}

//
// It is important the caller of this function holds a lock to ensure
// this function is mutual exclusive.
//
func (o *MetadataProvider) CheckIndexerStatusNoLock() []IndexerStatus {

	status := make([]IndexerStatus, len(o.watchers))
	i := 0
	for _, watcher := range o.watchers {
		status[i].Adminport = watcher.leaderAddr
		status[i].Connected = watcher.isAliveNoLock()
		logging.Infof("MetadataProvider.CheckIndexerStatus(): adminport=%v connected=%v", status[i].Adminport, status[i].Connected)
		i++
	}

	return status
}

// scope, collection params are ignored for now
func (o *MetadataProvider) CreateIndexWithPlan(
	name, bucket, scope, collection, using, exprType, whereExpr string,
	secExprs []string, desc []bool, indexMissingLeadingKey, isPrimary bool,
	scheme c.PartitionScheme, partitionKeys []string,
	plan map[string]interface{}) (c.IndexDefnId, error, bool) {

	// FindIndexByName will only return valid index
	if o.findIndexByName(name, bucket, scope, collection) != nil {
		return c.IndexDefnId(0), errors.New(fmt.Sprintf("Index %s already exists.", name)), false
	}

	// Create index definition
	idxDefn, err, retry := o.PrepareIndexDefn(name, bucket, scope, collection,
		using, exprType, whereExpr, secExprs, desc, indexMissingLeadingKey,
		isPrimary, scheme, partitionKeys, plan)
	if err != nil {
		return c.IndexDefnId(0), err, retry
	}

	clusterVersion := o.GetClusterVersion()
	if c.IsServerlessDeployment() {
		serverlessIndexLimit := o.settings.ServerlessIndexLimit()
		numIndexes := o.GetNumIndexesPerBucket(bucket)
		if numIndexes >= serverlessIndexLimit {
			errMsg := fmt.Sprintf("%v Limit : %v", c.ErrIndexBucketLimitReached.Error(), serverlessIndexLimit)
			return c.IndexDefnId(0), errors.New(errMsg), false
		}
	}

	enforceLimits := false
	if clusterVersion >= c.INDEXER_71_VERSION {
		enforceLimits, err = o.limitsCfg.EnforceLimits()
		if err != nil {
			return c.IndexDefnId(0), err, false
		}
	}
	if enforceLimits {
		scopeLimit, err := o.GetIndexScopeLimit(bucket, scope)
		if err != nil {
			return c.IndexDefnId(0), err, false
		}

		if scopeLimit != collections.NUM_INDEXES_NIL {
			numIndexes := o.GetNumIndexesPerScope(bucket, scope)
			newIndexes := (idxDefn.NumReplica + 1) * idxDefn.NumPartitions
			if numIndexes+newIndexes > scopeLimit {
				errMsg := fmt.Sprintf("%v Limit : %v", c.ErrIndexScopeLimitReached.Error(), scopeLimit)
				return c.IndexDefnId(0), errors.New(errMsg), false
			}
		}
	}

	if c.IsServerlessDeployment() {
		if err := c.CheckIngressLockdown(bucket); err != nil {
			index := fmt.Sprintf("%v:%v:%v:%v", bucket, scope, collection, name)
			errMsg := fmt.Sprintf("Fails to create index (%v) due to error %v", index, err)
			logging.Errorf("%v", errMsg)
			if err.Error() == c.ErrNoIngress.Error() {
				return c.IndexDefnId(0), errors.New(c.ErrDiskLimitReached.Error()), false
			}

			return c.IndexDefnId(0), errors.New(err.Error()), false
		}
	}

	if clusterVersion < c.INDEXER_70_VERSION {
		if collection != c.DEFAULT_COLLECTION || scope != c.DEFAULT_SCOPE {
			err := errors.New("Fails to create index.  Creation of an index on non-default collection" +
				"is enabled only after cluster is fully upgraded and there is no failed node.")
			return c.IndexDefnId(0), err, true
		}
	}

	if clusterVersion < c.INDEXER_55_VERSION || (!o.settings.UsePlanner()) {
		if err := o.createIndex(idxDefn, plan); err != nil {
			return c.IndexDefnId(0), err, false
		}
	} else {
		scheduleOnFailure := o.settings.AllowScheduleCreate()
		if err := o.recoverableCreateIndex(idxDefn, plan, scheduleOnFailure, false, time.Now().UnixNano(), false); err != nil {
			return c.IndexDefnId(0), err, false
		}
	}

	return idxDefn.DefnId, nil, false
}

func (o *MetadataProvider) GetNumIndexesPerScope(bucket, scope string) uint32 {
	o.mutex.RLock()
	defer o.mutex.RUnlock()
	var numIndexes uint32 = 0
	for _, metadata := range o.repo.indices {
		if metadata.Definition.Bucket == bucket && metadata.Definition.Scope == scope {
			if len(metadata.Instances) != 0 {
				numIndexes = numIndexes + metadata.Instances[0].NumPartitions*(metadata.Definition.NumReplica+1)
			}
		}
	}
	return numIndexes
}

func (o *MetadataProvider) GetIndexScopeLimit(bucket, scope string) (uint32, error) {
	clusterURL, err := c.ClusterAuthUrl(o.clusterUrl)
	if err != nil {
		return 0, err
	}
	cinfo, err := c.NewClusterInfoCache(clusterURL, "default")
	if err != nil {
		return 0, err
	}
	cinfo.Lock()
	defer cinfo.Unlock()
	cinfo.SetUserAgent("Client:Metadata_provider:GetIndexScopeLimit")
	return cinfo.GetIndexScopeLimit(bucket, scope)
}

func (o *MetadataProvider) GetNumIndexesPerBucket(bucket string) uint32 {
	o.mutex.RLock()
	defer o.mutex.RUnlock()
	var numIndexes uint32 = 0
	for _, metadata := range o.repo.indices {
		if metadata.Definition.Bucket == bucket {
			numIndexes = numIndexes + 1
		}
	}
	return numIndexes
}

//
// This function makes a call to create index using new protocol (vulcan).
//
func (o *MetadataProvider) makePrepareIndexRequest(defnId c.IndexDefnId, name string,
	bucket, scope, collection string, nodes []string, partitionScheme c.PartitionScheme,
	numReplica int, checkDuplicateIndex bool, ctime int64) (map[c.IndexerId]int, error, bool, bool) {

	// do a preliminary check
	watchers, err, _ := o.findWatchersWithRetry(nodes, numReplica, c.IsPartitioned(partitionScheme), false)
	if err != nil {
		return nil, err, false, false
	}

	if len(nodes) == 0 {
		// Get the full list of healthy watcher.  Unhealthy watcher could be unwatched.
		watchers = o.getAllAvailWatchers()
	}

	request := &PrepareCreateRequest{
		Op:          PREPARE,
		DefnId:      defnId,
		RequesterId: o.providerId,
		Timeout:     int64(time.Duration(3) * time.Minute),
		Ctime:       ctime,
	}

	if checkDuplicateIndex {
		request.Bucket = bucket
		request.Name = name
		request.Scope = scope
		request.Collection = collection
	}

	requestMsg, err := MarshallPrepareCreateRequest(request)
	if err != nil {
		return nil, err, false, false
	}

	var wg *sync.WaitGroup = new(sync.WaitGroup)
	var accept uint32
	var schedule uint32
	var duplicate uint32
	var rebalance uint32

	watcherMap := make(map[c.IndexerId]int)

	// Send prepare command to healthy watchers.   During network partitioning, if there are 2 concurrent create requests
	// from 2 different cbq nodes:
	// 1) If both cbq nodes see overlapping indexers, then one of them will succeed.
	// 2) If the cbq nodes see a non-overlapping subset of indexers, then both will succeed.   But for each cbq node, the
	//    planner will only use its subset of nodes for planning.
	//
	key := fmt.Sprintf("%d", defnId)
	for _, w := range watchers {

		wg.Add(1)
		watcherMap[w.getIndexerId()] = 1

		go func(w *watcher) {

			logging.Infof("send prepare create request to watcher %v", w.getAdminAddr())

			defer wg.Done()

			// if there is a network partitioning between the metadata provider and indexer, makeRequest would not return until timeout.
			content, err := w.makeRequest(OPCODE_PREPARE_CREATE_INDEX, key, requestMsg)
			if err != nil {
				logging.Errorf("Fail to process prepare request on %v. Error: %v", w.getAdminAddr(), err)
				return
			}

			response, err := UnmarshallPrepareCreateResponse(content)
			if err != nil {
				logging.Errorf("Fail to process prepare request on %v. Error: %v", w.getAdminAddr(), err)
				return
			}

			if response != nil && response.Accept {
				logging.Infof("Indexer %v accept prepare request. Index (%v, %v, %v, %v)",
					w.getAdminAddr(), bucket, scope, collection, name)

				atomic.AddUint32(&accept, 1)
				return
			}

			if response != nil {
				logging.Infof("Indexer %v rejected request for index (%v, %v, %v, %v) with reason %v",
					w.getAdminAddr(), bucket, scope, collection, name, response.Msg)

				if response.Msg == RespAnotherIndexCreation {
					atomic.AddUint32(&schedule, 1)
					return
				}

				if response.Msg == RespDuplicateIndex {
					atomic.AddUint32(&duplicate, 1)
					return
				}

				if response.Msg == RespRebalanceRunning {
					atomic.AddUint32(&rebalance, 1)
					return
				}
			}
		}(w)
	}

	wg.Wait()

	if accept < uint32(len(watcherMap)) {
		if atomic.LoadUint32(&duplicate) > 0 {
			return watcherMap, c.ErrDuplicateIndex, false, false
		}

		rebal := atomic.LoadUint32(&rebalance)
		sched := atomic.LoadUint32(&schedule)

		if (sched + rebal + accept) < uint32(len(watcherMap)) {
			return watcherMap, c.ErrNetworkPartition, false, false
		}

		if rebal > 0 {
			return watcherMap, c.ErrRebalanceRunning, true, true
		}

		return watcherMap, c.ErrAnotherIndexCreation, true, false
	}

	return watcherMap, nil, false, false
}

//
// This function clean up prepare index request
//
func (o *MetadataProvider) cancelPrepareIndexRequest(defnId c.IndexDefnId, watcherMap map[c.IndexerId]int) {

	request := &PrepareCreateRequest{
		Op:          CANCEL_PREPARE,
		DefnId:      defnId,
		RequesterId: o.providerId,
	}

	content, err := MarshallPrepareCreateRequest(request)
	if err != nil {
		logging.Errorf("Fail to cancel prepare index creation on indexerId. Error: %v", err)
		return
	}

	key := fmt.Sprintf("%d", defnId)
	for indexerId, _ := range watcherMap {

		go func(indexerId c.IndexerId) {

			watcher, err := o.findWatcherByIndexerId(indexerId)
			if err != nil {
				logging.Errorf("Fail to cancel prepare index creation.  Cannot find watcher for indexerId %v. Error: %v", indexerId, err)
				return
			}

			logging.Infof("send cancel create request to watcher %v", watcher.getAdminAddr())

			_, err = watcher.makeRequest(OPCODE_PREPARE_CREATE_INDEX, key, content)
			if err != nil {
				logging.Errorf("Fail to cancel prepare index creation on %v. Error: %v", watcher.getAdminAddr(), err)
				return
			}
		}(indexerId)
	}
}

//
// This function makes a call to create index using new protocol (vulcan).
//
func (o *MetadataProvider) makeCommitIndexRequest(op CommitCreateRequestOp, idxDefn *c.IndexDefn, requestId uint64,
	definitions map[c.IndexerId][]c.IndexDefn, watcherMap map[c.IndexerId]int, asyncCreate bool) error {

	request := &CommitCreateRequest{
		Op:          op,
		DefnId:      idxDefn.DefnId,
		RequesterId: o.providerId,
		Definitions: definitions,
		RequestId:   requestId,
		AsyncCreate: asyncCreate,
	}

	requestMsg, err := MarshallCommitCreateRequest(request)
	if err != nil {
		return fmt.Errorf("Unable to send commit request.  Reason: %v", err)
	}

	var mutex sync.Mutex
	var cond *sync.Cond = sync.NewCond(&mutex)
	var accept bool
	var count int32

	errorMap := make(map[string]bool)

	key := fmt.Sprintf("%d", idxDefn.DefnId)
	for indexerId, _ := range watcherMap {

		w, err := o.findWatcherByIndexerId(indexerId)
		if err != nil {
			logging.Errorf("Fail to cancel prepare index creation.  Cannot find watcher for indexerId %v", indexerId)
			continue
		}

		atomic.AddInt32(&count, 1)

		go func(w *watcher) {
			defer func() {
				atomic.AddInt32(&count, -1)

				cond.L.Lock()
				defer cond.L.Unlock()
				cond.Signal()
			}()

			logging.Infof("send commit create request to watcher %v defnID %v", w.getAdminAddr(), idxDefn.DefnId)

			// if there is a network partitioning between the metadata provider and indexer,
			// makeRequest would not return until timeout.
			content, err := w.makeRequest(OPCODE_COMMIT_CREATE_INDEX, key, requestMsg)
			if err != nil {
				logging.Errorf("Encountered error during create index.  Error: %v", err)
				mutex.Lock()
				errorMap[err.Error()] = true
				mutex.Unlock()
			}

			response, err := UnmarshallCommitCreateResponse(content)
			if err != nil {
				logging.Errorf("Encountered error during create index.  Error: %v", err)
			}

			mutex.Lock()
			if response != nil {
				accept = response.Accept || accept
			}
			mutex.Unlock()
		}(w)
	}

	// wait for result
	var success bool
	for {
		cond.L.Lock()
		cond.Wait()
		success = accept
		cond.L.Unlock()

		if success {
			break
		}

		if atomic.LoadInt32(&count) == 0 {
			break
		}
	}

	var createErr error
	if len(errorMap) != 0 {
		var errStr string
		for errStr2, _ := range errorMap {
			errStr += errStr2 + "\n"
		}
		createErr = errors.New(errStr)
	}

	//result is ready
	if success {
		if createErr != nil {
			return fmt.Errorf("Encountered transient error.  Index creation will be retried in background.  Error: %v", createErr)
		}
		return nil
	}

	// asyncCreate operation will retry the operation again. Hence, no
	// need to check for the presence of CreateCommandToken if asyncCreate
	// is true and createErr != nil
	if !asyncCreate || createErr == nil {
		// As metaKV is eventually consistent, it might take some time for
		// the createToken to propagate to all indexer nodes. Hence, check
		// periodically for upto 10 seconds.
		ticker := time.NewTicker(1 * time.Second)
		retryCount := 0
	loop:
		for {
			select {
			case <-ticker.C:
				retryCount++
				exist, _ := mc.CreateCommandTokenExist(idxDefn.DefnId)
				if exist {
					if createErr != nil {
						return fmt.Errorf("Encountered transient error.  Index creation will be retried in background.  Error: %v", createErr)
					}
					return nil
				}

				if retryCount > 10 {
					break loop
				}
			}
		}
	}

	if createErr == nil {
		errStr := "Fail to create index due to rebalancing, another concurrent request, network partition, or node failed. " +
			"The operation may have succeed.  If not, please retry the operation at later time."
		createErr = errors.New(errStr)
	}

	return createErr
}

func (o *MetadataProvider) verifyDuplicateScheduleToken(idxDefn *c.IndexDefn) error {
	tokens, err := mc.ListAllScheduleCreateTokens()
	if err != nil {
		logging.Errorf("verifyDuplicateScheduleToken:: error %v in ListAllScheduleCreateTokens", err)
		return err
	}

	for _, token := range tokens {
		defn := token.Definition
		if defn.Bucket == idxDefn.Bucket &&
			defn.Scope == idxDefn.Scope &&
			defn.Collection == idxDefn.Collection &&
			defn.Name == idxDefn.Name {

			exists, err := mc.StopScheduleCreateTokenExist(defn.DefnId)
			if err != nil {
				logging.Errorf("verifyDuplicateScheduleToken:: error %v in StopScheduleCreateTokenExist", err)
				return err
			}

			if exists {
				continue
			}

			return fmt.Errorf("%v DefnId = %v.", c.ErrDuplicateCreateToken.Error(), defn.DefnId)
		}
	}

	return nil
}

func (o *MetadataProvider) scheduleIndexCreation(idxDefn *c.IndexDefn,
	plan map[string]interface{}) error {

	err := o.verifyDuplicateScheduleToken(idxDefn)
	if err != nil {
		return err
	}

	//
	// choose one indexer and let it post the token
	//
	nodes := make(map[string]*watcher)
	watchers := o.getAllWatchers()
	for _, w := range watchers {
		if w.serviceMap.ExcludeNode == "in" ||
			w.serviceMap.ExcludeNode == "inout" {

			continue
		}

		nodes[strings.ToLower(w.getNodeAddr())] = w
	}

	if len(nodes) <= 0 {
		return fmt.Errorf("No candidate indexer node found for posting schedule create token "+
			"for index (%v, %v, %v, %v)", idxDefn.Bucket, idxDefn.Scope, idxDefn.Collection, idxDefn.Name)
	}

	//
	// validate availability of input nodes, if any.
	//
	for _, n := range idxDefn.Nodes {
		_, ok := nodes[strings.ToLower(n)]
		if !ok {
			return fmt.Errorf("Indexer node (%v) not found. The node may be failed or "+
				"under rebalance or network partitioned from query process.", n)
		}
	}

	var indexer *watcher

	if len(idxDefn.Nodes) != 0 {
		n := idxDefn.Nodes[0]
		for addr, node := range nodes {
			if addr == n {
				indexer = node
				break
			}
		}
	}

	if indexer == nil {
		if len(idxDefn.Nodes) != 0 {
			logging.Warnf("Cannot find the candidate indexer node to post schedule create token, "+
				"which belongs in the user-specified list of nodes, for index (%v, %v, %v, %v)",
				idxDefn.Bucket, idxDefn.Scope, idxDefn.Collection, idxDefn.Name)
		}

		for _, node := range nodes {
			indexer = node
			break
		}
	}

	if indexer == nil {
		return fmt.Errorf("No candidate indexer node found for posting schedule create token.")
	}

	return o.makeScheduleCreateRequest(idxDefn, plan, indexer)
}

func (o *MetadataProvider) makeScheduleCreateRequest(idxDefn *c.IndexDefn,
	plan map[string]interface{}, indexer *watcher) error {

	addr := indexer.getHttpAddr()
	url := addr + "/postScheduleCreateRequest"

	req := &ScheduleCreateRequest{
		Definition: *idxDefn,
		Plan:       plan,
		IndexerId:  indexer.getIndexerId(),
	}

	buf, err := json.Marshal(req)
	if err != nil {
		return err
	}

	bytesBuf := bytes.NewBuffer(buf)
	params := &security.RequestParams{Timeout: time.Duration(10) * time.Second}

	var resp *http.Response
	resp, err = security.PostWithAuth(url, "application/json", bytesBuf, params)
	if err != nil {
		logging.Errorf("MetadataProvider::makeScheduleCreateRequest: error in PostWithAuth: %v, for index (%v, %v, %v, %v)",
			err, idxDefn.Bucket, idxDefn.Scope, idxDefn.Collection, idxDefn.Name)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		ioutil.ReadAll(resp.Body)
	} else {
		logging.Errorf("MetadataProvider::makeScheduleCreateRequest: unexpected http status: %v, for index (%v, %v, %v, %v)",
			resp.StatusCode, idxDefn.Bucket, idxDefn.Scope, idxDefn.Collection, idxDefn.Name)

		var msg interface{}
		buf := new(bytes.Buffer)
		if _, err := buf.ReadFrom(resp.Body); err != nil {
			logging.Errorf("MetadataProvider::makeScheduleCreateRequest: error in reading response body: %v, for index (%v, %v, %v, %v)",
				err, idxDefn.Bucket, idxDefn.Scope, idxDefn.Collection, idxDefn.Name)
			return err
		}

		if err := json.Unmarshal(buf.Bytes(), &msg); err != nil {
			logging.Errorf("MetadataProvider::makeScheduleCreateRequest: error in unmarshalling response body: %v, for index (%v, %v, %v, %v)",
				err, idxDefn.Bucket, idxDefn.Scope, idxDefn.Collection, idxDefn.Name)
			return err
		}

		return fmt.Errorf("Error in posting schedule create request %v", msg)
	}

	err = o.waitForScheduleCreateToken(idxDefn.DefnId)
	if err != nil {
		return err
	}

	logging.Infof("Indexer %v has posted schedule create token for index (%v, %v, %v, %v, %v)",
		indexer.getIndexerId(), idxDefn.Bucket, idxDefn.Scope, idxDefn.Collection, idxDefn.Name,
		idxDefn.DefnId)

	return nil
}

func (o *MetadataProvider) waitForScheduleCreateToken(defnId c.IndexDefnId) error {
	tries := 20
	for i := 0; i < tries; i++ {
		exists, err := mc.ScheduleCreateTokenExist(defnId)
		if err != nil || !exists {
			time.Sleep(3 * time.Second)
			continue
		}

		return nil
	}

	return ErrWaitScheduleTimeout
}

func (o *MetadataProvider) waitForScheduledIndex(idxDefn *c.IndexDefn) error {

	//
	// Wait until one of the following events are observed.
	// 1. StopScheduleCreateToken is posted. Last error for token will be returned.
	// 2. DeleteCommandToken is posted. No error returned.
	// 3. Index metadata is created with state dependent on Deferred flag.
	//
	// Note that the DeleteCommandToken cleanup is delayed by 24 hours.
	//

	var states []c.IndexState
	if idxDefn.Deferred {
		states = []c.IndexState{c.INDEX_STATE_READY, c.INDEX_STATE_CATCHUP, c.INDEX_STATE_ACTIVE, c.INDEX_STATE_DELETED}
	} else {
		states = []c.IndexState{c.INDEX_STATE_ACTIVE, c.INDEX_STATE_DELETED}
	}

	tok, err := mc.GetScheduleCreateToken(idxDefn.DefnId)
	if err != nil {
		logging.Errorf("MetadataProvider:waitForScheduledIndex error in GetScheduleCreateToken for %v", idxDefn.DefnId)
		return err
	}

	//
	// By this time, the schedule create token has been observed at least once
	// in waitForScheduleCreateToken. If the token is not present, index creation
	// or deletion may have already happened. So, it is safe to return from here.
	//
	if tok == nil {
		logging.Infof("MetadataProvider:waitForScheduledIndex schedule create token does not exits for %v", idxDefn.DefnId)
		return nil
	}

	// Use nil topolgy as at this point the topology is unknown. This can lead
	// to false notification in case of restart of unrelated indexer process.
	e := &event{defnId: idxDefn.DefnId, status: states, notifyCh: make(chan error, 1), topology: nil, all: true}
	if !o.repo.registerEvent(e) {
		// This means that the event has already occurred.
		return nil
	}

	errIndexDel := errors.New("Index is deleted. DeleteCommandToken posted.")

	startTm := time.Now()

	cinfo, err := c.FetchNewClusterInfoCache2(o.clusterUrl, c.DEFAULT_POOL, "waitForScheduledIndex")
	if err != nil {
		return err
	}
	cinfo.SetMaxRetries(5) // avoid flooding query.log with retry messages if bucket was dropped

	checkValidKeyspace := func() (bool, error) {
		//
		// Keyspace validation happens before posting schedule create token.
		// Here, the purpose of keyspace validation is only to check for
		// continued keyspace existence. If it no longer exists, waitForScheduledIndex
		// can terminate.
		//

		// Fetch bucket info in the ClusterInfoCache
		err := cinfo.FetchForBucket(idxDefn.Bucket, true, true, false, false)
		if err != nil {
			return false, err
		}

		bucketUUID := cinfo.GetBucketUUID(idxDefn.Bucket)
		if bucketUUID != tok.BucketUUID {
			return false, nil
		}

		scopeId, collId := cinfo.GetScopeAndCollectionID(idxDefn.Bucket, idxDefn.Scope, idxDefn.Collection)
		if scopeId != tok.ScopeId || collId != tok.CollectionId {
			return false, nil
		}

		return true, nil
	}

	checkForTokens := func() {
		t := time.NewTicker(3 * time.Second)
		defer t.Stop()

		count := 0
		for {
			select {

			case <-t.C:
				count++
				// Check for worst case scenario timeout.
				if time.Now().Sub(startTm) > time.Duration(4*time.Hour) {
					timeoutErr := errors.New("Index creation timed out. The operation may" +
						"have suceeded in the background. Please check for the index state.")
					o.repo.cancelEvent(idxDefn.DefnId, timeoutErr)
					return
				}

				exists, err := mc.DeleteCommandTokenExist(idxDefn.DefnId)
				if err != nil {
					o.repo.cancelEvent(idxDefn.DefnId, err)
					return
				}

				if exists {
					o.repo.cancelEvent(idxDefn.DefnId, errIndexDel)
					return
				}

				token, err1 := mc.GetStopScheduleCreateToken(idxDefn.DefnId)
				if err1 != nil {
					o.repo.cancelEvent(idxDefn.DefnId, err1)
					return
				}

				if token != nil {
					o.repo.cancelEvent(idxDefn.DefnId, fmt.Errorf("%v", token.Reason))
					return
				}

				// check for valid keyspace every 30 seconds.
				if count%10 == 0 {
					valid, err := checkValidKeyspace()
					if err != nil {
						o.repo.cancelEvent(idxDefn.DefnId, err)
						return
					}

					if !valid {
						o.repo.cancelEvent(idxDefn.DefnId, nil)
						return
					}
				}
			}
		}
	}

	go checkForTokens()

	// Wait for the notification
	err, _ = <-e.notifyCh
	if err != nil {
		if err.Error() == errIndexDel.Error() {
			return nil
		}

		return err
	}

	return nil
}

// This should be called for the defn, that is not yet persisted in index metadata.
// This function doesn't trigger the index build, even if the deferred flag is false.
func (o *MetadataProvider) CreateIndexWithDefnAndPlan(idxDefn *c.IndexDefn,
	plan map[string]interface{}, ctime int64) error {

	// TODO: Check for existing idxDefn.DefnId.
	return o.recoverableCreateIndex(idxDefn, plan, false, true, ctime, true)
}

//
// This function create index using new protocol (vulcan).
//
func (o *MetadataProvider) recoverableCreateIndex(idxDefn *c.IndexDefn,
	plan map[string]interface{}, scheduleOnFailure bool, asyncCreate bool, ctime int64,
	allowLostReplica bool) error {

	//
	// Prepare Phase.  This is to seek full quorum from all the indexers.
	//
	// This operation will fail if
	// 1) Any indexer is unreachable
	// 2) Any indexer is serving another create index request
	//
	// Once the full quorum is achieved, the indexer will not accept any other create index request until:
	// 1) This create index request has completed
	// 2) This create index request has been explicity canceled
	// 3) Indexer has timed out
	//
	// Note: Even in case of plan having with "nodes" clause, a full quorum will be sought.
	//       This can be disabled with the help of AllowPartialQuorum setting.

	//
	// Validate idxDefn.Nodes before starting with the index creation
	//
	if len(idxDefn.Nodes) != 0 {
		// If specified, validate if all the input nodes are available.
		valid, err := o.validateNodes(idxDefn.Nodes)
		if !valid {
			return err
		}
		if err != nil {
			return err
		}
	}

	useNodes := ([]string)(nil)
	if o.settings.AllowPartialQuorum() {
		if len(idxDefn.Nodes) != 0 {
			logging.Infof("As per the setting, allowing partial quorum for creation "+
				"of the index name=%v, bucket=%v, scope=%v, collection=%v",
				idxDefn.Name, idxDefn.Bucket, idxDefn.Scope, idxDefn.Collection)
		}
		useNodes = idxDefn.Nodes
	}

	actualNumReplica := idxDefn.NumReplica
	if allowLostReplica && !c.IsServerlessDeployment() {
		numNodes := len(o.getAllAvailWatchers())
		if numNodes < int(idxDefn.NumReplica)+1 {
			logging.Infof("Allow index creation with lost replica for defn %v. Num replicas %v, num nodes %v",
				idxDefn.DefnId, idxDefn.NumReplica, numNodes)
			idxDefn.NumReplica = uint32(numNodes - 1)
		}
	}

	watcherMap, err, canSchedule, rebalRunning := o.makePrepareIndexRequest(idxDefn.DefnId, idxDefn.Name,
		idxDefn.Bucket, idxDefn.Scope, idxDefn.Collection, useNodes,
		idxDefn.PartitionScheme, int(idxDefn.NumReplica), true, ctime)

	if err != nil {
		o.cancelPrepareIndexRequest(idxDefn.DefnId, watcherMap)
		msg := fmt.Sprintf("Index creation for index %v, bucket %v, scope %v, collection %v"+
			" cannot start. Reason: %v.", idxDefn.Name, idxDefn.Bucket, idxDefn.Scope, idxDefn.Collection, err)

		sched := false

		clusterVersion := o.GetClusterVersion()
		if scheduleOnFailure && canSchedule && clusterVersion >= c.INDEXER_70_VERSION {
			// Check if background creation is allowed or needded.
			sched = true
		}

		if rebalRunning && !o.settings.AllowScheduleCreateRebal() {
			// Check if background creation is allowed during rebalance
			sched = false
		}

		if sched {
			scheduleErr := o.scheduleIndexCreation(idxDefn, plan)
			if scheduleErr == nil {
				wait := true
				if rebalRunning {
					wait = false
					if c.IsServerlessDeployment() {
						wait = true
					}
				}

				if o.settings.WaitForScheduledIndex() && wait {
					err := o.waitForScheduledIndex(idxDefn)
					if err != nil {
						logging.Errorf("Error in waitForScheduledIndex %v", err)
						return err
					}

					return nil
				}

				message := "The index is scheduled for background creation. " + msg
				if rebalRunning {
					message = message + " The index will be created in the background after the ongoing rebalance."
				}
				logging.Warnf("%v", message)

				if c.IsServerlessDeployment() {
					return c.ErrServerBusy
				}

				return fmt.Errorf("%v", message)
			} else {
				if scheduleErr.Error() == ErrWaitScheduleTimeout.Error() {
					msg += fmt.Sprintf(" Scheduling of index creation was attempted. Please check for index status later.")
				} else {
					msg += fmt.Sprintf(" Could not schedule index creation in the background. Reason: %v", scheduleErr)
					logging.Errorf("%v", msg)
				}
			}
		}

		logging.Errorf("Fail to create index: %v, msg: %v", err, msg)
		return fmt.Errorf("%v", msg)
	}

	//
	// Plan Phase.
	// The planner will use nodes that metadta provider sees for planning.  All inactive_failed, inactive_new and unhealthy
	// nodes will be excluded from planning.    If the user provides a specific node list, those nodes will be used.
	//

	var layout map[int]map[c.IndexerId][]c.PartitionId
	var definitions map[c.IndexerId][]c.IndexDefn

	enforceLimits, err := o.limitsCfg.EnforceLimits()
	if err != nil {
		return err
	}
	if o.GetClusterVersion() < c.INDEXER_71_VERSION {
		enforceLimits = false
	}

	if c.IsServerlessDeployment() {
		if err := c.CheckIngressLockdown(idxDefn.Bucket); err != nil {
			index := fmt.Sprintf("%v:%v:%v:%v", idxDefn.Bucket, idxDefn.Scope, idxDefn.Collection, idxDefn.Name)
			errMsg := fmt.Sprintf("Fails to create index (%v) due to error %v", index, err)
			logging.Errorf("%v", errMsg)
			if err.Error() == c.ErrNoIngress.Error() {
				return c.ErrDiskLimitReached
			}

			return err
		}
	}

	if o.canSkipPlanner(watcherMap, idxDefn) && !enforceLimits && !c.IsServerlessDeployment() {
		logging.Infof("Skipping planner for creation of the index %v:%v:%v:%v", idxDefn.Bucket,
			idxDefn.Scope, idxDefn.Collection, idxDefn.Name)
		layout, definitions, err = o.getIndexLayoutWithoutPlanner(watcherMap, idxDefn, allowLostReplica, actualNumReplica)
		if err != nil {
			logging.Errorf("Fail to create index: %v", err)
			return err
		}
	} else {
		layout, definitions, err = o.plan(idxDefn, plan, watcherMap, allowLostReplica, actualNumReplica, enforceLimits)

		if err != nil && (c.IsServerlessDeployment() || (strings.Contains(err.Error(), "Index already exist") || strings.Contains(err.Error(), c.ErrIndexScopeLimitReached.Error()) || strings.Contains(err.Error(), c.ErrIndexBucketLimitReached.Error()))) {
			o.cancelPrepareIndexRequest(idxDefn.DefnId, watcherMap)
			return err
		}
	}

	if err != nil {
		logging.Errorf("Encounter planner error for index.  Use round robin strategy for planning. Error: %v: Index:%v, %v, %v, %v",
			err, idxDefn.Bucket, idxDefn.Scope, idxDefn.Collection, idxDefn.Name)
		layout, definitions, err = o.getIndexLayoutWithoutPlanner(watcherMap, idxDefn, allowLostReplica, actualNumReplica)
		if err != nil {
			logging.Errorf("Fail to create index: %v", err)
			return err
		}
	}

	//
	// Commit Phase.  Metadata Provider will send a commit request to at least one indexer.  If at least one indexer
	// responds with success, then it means there won't be another concurrent create index request.    Even though
	// the metadata provider may not have full quorum, this create index request can roll forward.
	//
	// The first indexer that responds with success will create a token so that it can roll forward even if this
	// metadata provider has died.  Other indexer will observe the token and proceed with the request.
	//
	err = o.makeCommitIndexRequest(NEW_INDEX, idxDefn, 0, definitions, watcherMap, asyncCreate)
	if err != nil {
		logging.Errorf("Fail to create index: %v", err)
		return err
	}

	//
	// Wait for response
	//
	topologyMap := make(map[int]map[c.PartitionId]c.IndexerId)
	for replicaId, indexerPartitionMap := range layout {
		if _, ok := topologyMap[replicaId]; !ok {
			topologyMap[replicaId] = make(map[c.PartitionId]c.IndexerId)
		}

		for indexerId, partitions := range indexerPartitionMap {
			for _, partition := range partitions {
				topologyMap[replicaId][partition] = indexerId
			}
		}
	}

	var states []c.IndexState
	if !idxDefn.Deferred {
		states = []c.IndexState{c.INDEX_STATE_ACTIVE, c.INDEX_STATE_DELETED}
	} else {
		states = []c.IndexState{c.INDEX_STATE_READY, c.INDEX_STATE_CATCHUP, c.INDEX_STATE_ACTIVE, c.INDEX_STATE_DELETED}
	}

	err = o.repo.waitForAllEvent(idxDefn.DefnId, states, topologyMap)
	if err != nil {
		return fmt.Errorf("%v\n", err)
	}

	return nil
}

func (o *MetadataProvider) canSkipPlanner(watcherMap map[c.IndexerId]int,
	idxDefn *c.IndexDefn) bool {

	//for serverless deployment always run the planner
	if c.IsServerlessDeployment() {
		return false
	}

	// The planner can be skipped if planner is always going to yield the same
	// output for any kind of current index layout or any type of load generated
	// by the application. This holds true when
	// 1. There is only one node in the cluster.
	// 2. User has specified only one node as input
	// 3. The index is NOT partitioned and
	//    3.1. There are exact same number of nodes as that of instances
	//    3.2. The user has specified exact same number of nodes in the
	//         input as that of instances.

	if len(watcherMap) == 1 {
		return true
	}

	if len(idxDefn.Nodes) == 1 {
		return true
	}

	// If index is partitioned and more than 1 node is present in the cluster,
	// then always use planner as the planner will yield an optimal partition
	// placement.
	if c.IsPartitioned(idxDefn.PartitionScheme) {
		return false
	}

	if len(watcherMap) == idxDefn.GetNumReplica()+1 {
		return true
	}

	if len(idxDefn.Nodes) == idxDefn.GetNumReplica()+1 {
		return true
	}

	return false
}

func (o *MetadataProvider) getIndexLayoutWithoutPlanner(watcherMap map[c.IndexerId]int,
	idxDefn *c.IndexDefn, allowLostReplica bool, actualNumReplica uint32) (map[int]map[c.IndexerId][]c.PartitionId, map[c.IndexerId][]c.IndexDefn, error) {

	var watchers []*watcher
	if len(idxDefn.Nodes) == 0 {
		watchers = make([]*watcher, 0, len(watcherMap))
		for indexerId, _ := range watcherMap {
			watcher, err := o.findWatcherByIndexerId(indexerId)
			if err != nil {
				return nil, nil, err
			}

			watchers = append(watchers, watcher)
		}
	} else {
		names := make(map[string]*watcher)

		o.mutex.Lock()
		for _, watcher := range o.watchers {
			if watcher.serviceMap.ExcludeNode != "in" &&
				watcher.serviceMap.ExcludeNode != "inout" {
				nodeAddr := strings.ToLower(watcher.getNodeAddr())
				encryptedNodeAddr, _ := security.EncryptPortInAddr(nodeAddr)
				if !security.EncryptionEnabled() {
					names[nodeAddr] = watcher
				} else if security.DisableNonSSLPort() {
					names[encryptedNodeAddr] = watcher
				} else {
					names[nodeAddr] = watcher
					names[encryptedNodeAddr] = watcher
				}
			}
		}

		o.mutex.Unlock()

		for _, node := range idxDefn.Nodes {
			var watcher *watcher
			var ok bool
			if watcher, ok = names[strings.ToLower(node)]; !ok {
				fmtMsg := "Indexer node (%v) not found. The node may be failed or " +
					"under rebalance or network partitioned from query process."

				return nil, nil, errors.New(fmt.Sprintf(fmtMsg, node))
			} else {
				watchers = append(watchers, watcher)
			}
		}
	}

	layout := o.createLayoutWithRoundRobin(idxDefn, watchers)
	definitions, err := o.getDefinitionsFromLayout(layout, idxDefn, allowLostReplica, actualNumReplica)
	if err != nil {
		return nil, nil, err
	}

	return layout, definitions, nil
}

//
// This fuction returns list of index definitions required by commit phase
// given the index layout generated by round robin index placement.
//
func (o *MetadataProvider) getDefinitionsFromLayout(layout map[int]map[c.IndexerId][]c.PartitionId,
	defn *c.IndexDefn, allowLostReplica bool, actualNumReplica uint32) (map[c.IndexerId][]c.IndexDefn, error) {

	definitions := make(map[c.IndexerId][]c.IndexDefn)
	for replicaId, indexerPartitionMap := range layout {
		instId, err := c.NewIndexInstId()
		if err != nil {
			return nil, fmt.Errorf("Internal Error = %v.", err)
		}

		for indexerId, partitions := range indexerPartitionMap {
			temp := *defn

			temp.InstId = instId
			temp.ReplicaId = replicaId
			temp.Partitions = partitions
			temp.Versions = make([]int, len(partitions))

			if allowLostReplica && actualNumReplica > temp.NumReplica {
				temp.NumReplica = actualNumReplica
			}

			definitions[indexerId] = append(definitions[indexerId], temp)
		}
	}

	return definitions, nil
}

func (o *MetadataProvider) validateNodes(nodes []string) (bool, error) {
	availableNodes := make(map[string]bool)

	watchers := o.getAllAvailWatchers()
	for _, watcher := range watchers {
		nodeAddr := strings.ToLower(watcher.getNodeAddr())
		if !security.EncryptionEnabled() {
			availableNodes[nodeAddr] = true
		} else if security.DisableNonSSLPort() {
			encryptedNodeAddr, err := security.EncryptPortInAddr(nodeAddr)
			if err != nil {
				return false, err
			}
			availableNodes[encryptedNodeAddr] = true
		} else {
			availableNodes[nodeAddr] = true
			encryptedNodeAddr, err := security.EncryptPortInAddr(nodeAddr)
			if err != nil {
				return false, err
			}
			availableNodes[encryptedNodeAddr] = true
		}
	}

	for _, node := range nodes {
		_, ok := availableNodes[strings.ToLower(node)]
		if !ok {
			fmtMsg := "Indexer node (%v) not found. The node may be failed or " +
				"under rebalance or network partitioned from query process."
			return false, errors.New(fmt.Sprintf(fmtMsg, node))
		}
	}

	return true, nil
}

//
// This function builds the index layout using round robin.
//
func (o *MetadataProvider) createLayoutWithRoundRobin(idxDefn *c.IndexDefn,
	watchers []*watcher) map[int]map[c.IndexerId][]c.PartitionId {

	layout := make(map[int]map[c.IndexerId][]c.PartitionId)

	//
	// Functions to shuffle nodes and node lists
	//
	shuffleNodes := func(indexerIds []c.IndexerId) []c.IndexerId {
		num := len(indexerIds)
		result := make([]c.IndexerId, num)

		for _, indexerId := range indexerIds {
			found := false
			for !found {
				n := rand.Intn(num)
				if result[n] == c.INDEXER_ID_NIL {
					result[n] = indexerId
					found = true
				}
			}
		}
		return result
	}

	shuffleNodeList := func(input [][]c.IndexerId) [][]c.IndexerId {

		// shuffle the list of lists
		num := len(input)
		result := make([][]c.IndexerId, num)
		for _, nodes := range input {
			found := false
			for !found {
				n := rand.Intn(num)
				if result[n] == nil {
					result[n] = nodes
					found = true
				}
			}
		}

		// shuffle individual lists
		final := make([][]c.IndexerId, num)
		for i, nodes := range result {
			final[i] = shuffleNodes(nodes)
		}

		return final
	}

	//
	// Function to add partition to the layout.
	//
	addPartition := func(replicaId int, indexerId c.IndexerId, partnId c.PartitionId) {
		if _, ok := layout[replicaId]; !ok {
			layout[replicaId] = make(map[c.IndexerId][]c.PartitionId)
		}

		if _, ok := layout[replicaId][indexerId]; !ok {
			layout[replicaId][indexerId] = make([]c.PartitionId, 0)
		}

		layout[replicaId][indexerId] = append(layout[replicaId][indexerId], partnId)
	}

	//
	// Generate a list of node lists based on server group
	//
	serverGroupIdxMap := make(map[string]int)
	i := 0
	for _, w := range watchers {
		sg := w.getServerGroup()
		if _, ok := serverGroupIdxMap[sg]; !ok {
			serverGroupIdxMap[sg] = i
			i++
		}
	}

	maxNodesInServerGroup := 0

	nodeList := make([][]c.IndexerId, len(serverGroupIdxMap))
	for _, w := range watchers {
		sg := w.getServerGroup()
		idx := serverGroupIdxMap[sg]
		nodeList[idx] = append(nodeList[idx], w.getIndexerId())
		if maxNodesInServerGroup < len(nodeList[idx]) {
			maxNodesInServerGroup = len(nodeList[idx])
		}
	}

	//
	// Function to flatten the node list ordered by server group
	//
	flattenNodeList := func(input [][]c.IndexerId) []c.IndexerId {
		output := make([]c.IndexerId, 0)
		for i := 0; i < maxNodesInServerGroup; i++ {
			for j := 0; j < len(input); j++ {
				if i < len(input[j]) {
					output = append(output, input[j][i])
				}
			}
		}

		return output
	}

	numInstances := int(idxDefn.NumReplica) + 1

	//
	// Place the index partitions in ordered manner where partition ids will be
	// placed in increasing order and the partition ids of all replicas are
	// grouped together.
	//
	// Example for 3 partitions, num instances 3:
	// (p0, r0), (p0, r1), (p0, r2), (p1, r0), (p1, r1), (p1, r2), (p2, r0), (p2, r1), (p2, r2)
	//
	placeAllReplicas := func(partnId c.PartitionId) {
		nodeList = shuffleNodeList(nodeList)
		nodes := flattenNodeList(nodeList)
		idx := 0
		for replicaId := 0; replicaId < numInstances; replicaId++ {
			addPartition(replicaId, nodes[idx], partnId)
			idx++
		}
	}

	if c.IsPartitioned(idxDefn.PartitionScheme) {
		for partnId := 1; partnId <= int(idxDefn.NumPartitions); partnId++ {
			placeAllReplicas(c.PartitionId(partnId))
		}
	} else {
		placeAllReplicas(c.NON_PARTITION_ID)
	}

	return layout
}

//
// This function create index using old protocol (spock).
//
func (o *MetadataProvider) createIndex(idxDefn *c.IndexDefn, plan map[string]interface{}) error {

	logging.Infof("Using old protocol for create index")

	// For non-partitioned index, this will return nodes with fewest indexes.  The number of nodes match the number of replica.
	// For partitioned index, it return all healthy nodes.
	watchers, err, _ := o.findWatchersWithRetry(idxDefn.Nodes, int(idxDefn.NumReplica), c.IsPartitioned(idxDefn.PartitionScheme), true)
	if err != nil {
		return err
	}

	if len(idxDefn.Nodes) != 0 && len(watchers) != len(idxDefn.Nodes) {
		return errors.New(fmt.Sprintf("Fails to create index.  Some indexer node is not available for create index.  Indexers=%v.", idxDefn.Nodes))
	}

	if len(watchers) < int(idxDefn.NumReplica)+1 {
		return errors.New(fmt.Sprintf("Fails to create index.  Cannot find enough indexer node for replica.  numReplica=%v.", idxDefn.NumReplica))
	}

	layout := o.createLayoutWithRoundRobin(idxDefn, watchers)

	return o.makeCreateIndexRequest(idxDefn, layout)
}

//
// This function makes a call to create index using old protocol (spock).
//
func (o *MetadataProvider) makeCreateIndexRequest(idxDefn *c.IndexDefn, layout map[int]map[c.IndexerId][]c.PartitionId) error {

	defnID := idxDefn.DefnId
	wait := !idxDefn.Deferred
	scheduled := true

	if c.IsPartitioned(idxDefn.PartitionScheme) && idxDefn.NumReplica > 0 {
		scheduled = false
	}

	errMap := make(map[string]bool)
	topologyMap := make(map[int]map[c.PartitionId]c.IndexerId)

	for replicaId, partitionMap := range layout {

		// create index id
		var err error
		idxDefn.InstId, err = c.NewIndexInstId()
		if err != nil {
			return err
		}
		idxDefn.ReplicaId = replicaId

		for indexerId, partitions := range partitionMap {

			idxDefn.Partitions = partitions
			idxDefn.Versions = make([]int, len(partitions))

			if err := o.SendCreateIndexRequest(indexerId, idxDefn, scheduled); err != nil {
				errMap[err.Error()] = true
			}

			if _, ok := topologyMap[replicaId]; !ok {
				topologyMap[replicaId] = make(map[c.PartitionId]c.IndexerId)
			}

			for _, partnId := range partitions {
				topologyMap[replicaId][partnId] = indexerId
			}
		}
	}

	if len(errMap) != 0 {
		errStr := ""
		for msg, _ := range errMap {
			errStr += msg + "\n"
		}

		if len(errStr) != 0 {
			return errors.New(fmt.Sprintf("Encountered errors during create index.  Error=%s.", errStr))
		}
	}

	// build partitioned index with replica
	if c.IsPartitioned(idxDefn.PartitionScheme) && idxDefn.NumReplica > 0 && wait {

		// place token for index build
		if err := mc.PostBuildCommandToken(defnID); err != nil {
			logging.Errorf("Index is created, but fail to Build Index due to internal errors.  Error=%v", err)
			return errors.New("Index is created, bu fail to Build Index due to internal errors.  Please use build index statement.")
		}

		list := BuildIndexIdList([]c.IndexDefnId{defnID})
		content, err := MarshallIndexIdList(list)
		if err != nil {
			logging.Errorf("Encountered unexpected error during build index.  Index build will be retried in background. Error=%v", err)
			return errors.New("Encountered unexpected error.  Index build will be retried in background.")
		}

		hasError := false
		sent := make(map[c.IndexerId]bool)
		for _, partitionMap := range layout {
			for indexerId, _ := range partitionMap {
				if _, ok := sent[indexerId]; !ok {
					sent[indexerId] = true

					watcher, err := o.findAliveWatcherByIndexerId(indexerId)
					if err != nil {
						logging.Errorf("Cannot reach indexer node.  Index build will be retried in background once network connection is re-established.")
						hasError = true
						continue
					}

					_, err = watcher.makeRequest(OPCODE_BUILD_INDEX, "Index Build", content)
					if err != nil {
						logging.Errorf("Encountered unexpected error during build index.  Index build will be retried in background. Error=%v", err)
						hasError = true
					}
				}
			}
		}

		if hasError {
			return errors.New("Encountered unexpected error.  Index build will be retried in background.")
		}
	}

	//
	// Wait for response
	//

	if wait {
		var errStr string
		err := o.repo.waitForEvent(defnID, []c.IndexState{c.INDEX_STATE_ACTIVE, c.INDEX_STATE_DELETED}, topologyMap)
		if err != nil {
			errStr += err.Error() + "\n"
		}

		if len(errStr) != 0 {
			return errors.New(errStr)
		}
	}

	return nil
}

//
// This function send a create index request
//
func (o *MetadataProvider) SendCreateIndexRequest(indexerId c.IndexerId, idxDefn *c.IndexDefn, scheduled bool) error {

	watcher, err := o.findWatcherByIndexerId(indexerId)
	if err != nil {
		return errors.New("Fail to create index.  Internal Error: Cannot locate indexer nodes")
	}

	content, err := c.MarshallIndexDefn(idxDefn)
	if err != nil {
		return fmt.Errorf("Fail to send create index request.  Error=%v", err)
	}

	key := fmt.Sprintf("%d", idxDefn.DefnId)
	if scheduled {
		if _, err := watcher.makeRequest(OPCODE_CREATE_INDEX, key, content); err != nil {
			return err
		}
	} else {
		if _, err := watcher.makeRequest(OPCODE_CREATE_INDEX_DEFER_BUILD, key, content); err != nil {
			return err
		}
	}

	return nil
}

//
// Create Index Defnition from DDL
//
func (o *MetadataProvider) PrepareIndexDefn(
	name, bucket, scope, collection, using, exprType, whereExpr string,
	secExprs []string, desc []bool, indexMissingLeadingKey, isPrimary bool,
	partitionScheme c.PartitionScheme, partitionKeys []string,
	plan map[string]interface{}) (*c.IndexDefn, error, bool) {

	//
	// Validation
	//

	if err := o.validateParamNames(plan); err != nil {
		return nil, err, false
	}

	//
	// Parse WITH CLAUSE
	//

	var immutable bool = false
	var deferred bool = false
	var nodes []string = nil
	var numReplica int = 0
	var numPartition int = 0
	var retainDeletedXATTR = false
	var numDoc uint64 = 0
	var secKeySize uint64 = 0
	var docKeySize uint64 = 0
	var arrSize uint64 = 0
	var residentRatio float64 = 0

	version := o.GetIndexerVersion()
	clusterVersion := o.GetClusterVersion()

	if plan != nil {
		logging.Debugf("MetadataProvider:CreateIndexWithPlan(): plan %v version %v", plan, version)

		var err error
		var retry bool

		nodes, err, retry = o.getNodesParam(plan)
		if err != nil {
			return nil, err, retry
		}

		deferred, err, retry = o.getDeferredParam(plan)
		if err != nil {
			return nil, err, retry
		}

		xattrExprs := make([]string, 0)
		xattrExprs = append(xattrExprs, secExprs...)
		if len(whereExpr) > 0 {
			xattrExprs = append(xattrExprs, whereExpr)
		}
		xattrExprs = append(xattrExprs, partitionKeys...)
		isXATTRIndex, XATTRNames, err := queryutil.GetXATTRNames(xattrExprs)
		if err != nil {
			return nil, err, retry
		}

		if isXATTRIndex {
			if XATTRNames[0] == "$document" {
				return nil,
					errors.New("Fails to create index.  Cannot index on Virtual Extended Attributes."),
					false
			}
			if clusterVersion < c.INDEXER_55_VERSION {
				return nil,
					errors.New("Fails to create index.  Extended Attributes are enabled only after cluster is fully upgraded and there is no failed node."),
					false
			}
		}

		retainDeletedXATTR, err, retry = o.getXATTRParam(plan)
		if err != nil {
			return nil, err, retry
		}

		if retainDeletedXATTR && !isXATTRIndex {
			return nil,
				errors.New("Fails to create index.  retain_deleted_xattr can be used only if extended attributes are indexed."),
				false
		}

		if indexType, ok := plan["index_type"].(string); ok {
			if c.IsValidIndexType(indexType) {
				using = indexType
			} else {
				return nil,
					errors.New("Fails to create index.  Invalid index_type parameter value specified."),
					false
			}
		}

		if len(partitionKeys) != 0 {
			if clusterVersion < c.INDEXER_55_VERSION {
				return nil,
					errors.New("Fails to create index.  Partitioned index is enabled only after cluster is fully upgraded and there is no failed node."),
					false
			}
		}

		err = o.validatePartitionKeys(partitionScheme, partitionKeys, secExprs, isPrimary)
		if err != nil {
			return nil, err, false
		}

		numPartition, err, retry = o.getNumPartitionParam(partitionScheme, plan, version)
		if err != nil {
			return nil, err, retry
		}

		immutable, err, retry = o.getImmutableParam(partitionScheme, plan, whereExpr)
		if err != nil {
			return nil, err, retry
		}

		numReplica, err, retry = o.getReplicaParam(plan, version)
		if err != nil {
			return nil, err, retry
		}

		if numReplica != 0 && len(nodes) != 0 {
			if numReplica > len(nodes)-1 {
				errStr := fmt.Sprintf("Fails to create index.  There are more replica than specified node list (%v).  ", nodes)
				errStr += "Please check the parameter 'num_replica' or setting 'indexer.settings.num_replica'."
				return nil, errors.New(errStr), false
			}
		}

		if !c.IsPartitioned(partitionScheme) && numReplica == 0 && len(nodes) != 0 {
			numReplica = len(nodes) - 1
		}

		numDoc, err, retry = o.getNumDocParam(plan)
		if err != nil {
			return nil, err, retry
		}

		docKeySize, err, retry = o.getDocKeySizeParam(plan)
		if err != nil {
			return nil, err, retry
		}

		secKeySize, err, retry = o.getSecKeySizeParam(plan)
		if err != nil {
			return nil, err, retry
		}

		arrSize, err, retry = o.getArrSizeParam(plan)
		if err != nil {
			return nil, err, retry
		}

		residentRatio, err, retry = o.getResidentRatioParam(plan)
		if err != nil {
			return nil, err, retry
		}
	}

	logging.Debugf("MetadataProvider:CreateIndex(): deferred_build %v nodes %v", deferred, nodes)

	//
	// Array index related information
	//
	isArrayIndex := false
	isArrayFlattened := false
	arrayExprCount := 0
	skipFlattenExprsTillPos := 0
	isArrayDistinct := false
	for pos, exp := range secExprs {
		// As `secExprs` in flattened array index are exploded,
		// skip some `secExprs`
		if isArrayIndex && isArrayFlattened && pos < skipFlattenExprsTillPos {
			continue
		}

		isArray, isDistinct, isFlatten, err := queryutil.IsArrayExpression(exp)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Fails to create index.  Error in parsing expression %v : %v", exp, err)), false
		}
		if isArray == true {
			isArrayIndex = isArray
			isArrayFlattened = isFlatten
			isArrayDistinct = isDistinct
			arrayExprCount++
		}
		if isArray && isFlatten {
			numFlattenKeys, err := queryutil.NumFlattenKeys(exp)
			if err != nil {
				return nil, errors.New(fmt.Sprintf("Fails to create index.  Error while retrieving flatten keys in expression %v : %v ", exp, err)), false
			}
			skipFlattenExprsTillPos = pos + numFlattenKeys
		}
	}

	if arrayExprCount > 1 {
		return nil, errors.New("Fails to create index.  Multiple expressions with ALL are found. Only one array expression is supported per index."), false
	}

	if isArrayIndex && isArrayFlattened && (version < c.INDEXER_71_VERSION || clusterVersion < c.INDEXER_71_VERSION) {
		return nil,
			errors.New("Fail to create index with flatten array. This option is available only after all nodes in the cluster are atleast running on server 7.1 version"),
			false
	}

	// arr_items_count counter is supported only on MOI and Plasma for ALL array indexes created after
	// all nodes in cluster are version 7.1 or above.
	hasArrItemsCount := false
	if isArrayIndex && c.IndexType(using) != c.ForestDB && isArrayDistinct == false &&
		version >= c.INDEXER_71_VERSION && clusterVersion >= c.INDEXER_71_VERSION {
		hasArrItemsCount = true
	}

	//
	// Ascending/Descending key
	//

	if o.isDecending(desc) && (version < c.INDEXER_50_VERSION || clusterVersion < c.INDEXER_50_VERSION) {
		return nil,
			errors.New("Fail to create index with descending order. This option is enabled after cluster is fully upgraded and there is no failed node."),
			false
	}

	if desc != nil && len(secExprs) != len(desc) {
		return nil, errors.New("Fail to create index.  Collation order is required for all expressions in the index."), false
	}

	//
	// Missing key
	//

	if indexMissingLeadingKey && clusterVersion < c.INDEXER_72_VERSION {
		cinfo, err := c.FetchNewClusterInfoCache2(o.clusterUrl, c.DEFAULT_POOL, "NewMetaDataProvider")
		if err != nil {
			return nil,
				errors.New("Fail to create index with missing attribute. Error while Fetching Cluster Info Cache. Cluster must be atleast 7.1.2 for using Missing Keyword"),
				false
		}

		err = cinfo.FetchNodesAndSvsInfo()
		if err != nil {
			return nil,
				errors.New("Fail to create index with missing attribute. Error while Fetching Nodes and Services Info. Cluster must be atleast 7.1.2 for using Missing Keyword"),
				false
		}

		internalVersion, err := c.GetInternalClusterVersion(c.NodesInfoProvider(cinfo), false)
		if err != nil {
			return nil,
				errors.New("Fail to create index with missing attribute. Error while finding internal version of all nodes. Cluster must be atleast 7.1.2 for using Missing Keyword"),
				false
		}

		if internalVersion.LessThan(c.InternalVersion(c.MIN_VER_MISSING_LEADING_KEY)) {
			return nil,
				errors.New("Fail to create index with missing attribute. This option is enabled after cluster is fully upgraded to atleast 7.1.2 and there is no failed node."),
				false
		}
	}

	//
	// Create Index Definition
	//

	defnID, err := c.NewIndexDefnId()
	if err != nil {
		return nil,
			errors.New(fmt.Sprintf("Fails to create index. Internal Error: Fail to create uuid for index definition.")),
			false
	}

	idxDefn := &c.IndexDefn{
		DefnId:                 defnID,
		Name:                   name,
		Using:                  c.IndexType(using),
		Bucket:                 bucket,
		IsPrimary:              isPrimary,
		SecExprs:               secExprs,
		Desc:                   desc,
		ExprType:               c.ExprType(exprType),
		PartitionScheme:        partitionScheme,
		PartitionKeys:          partitionKeys,
		WhereExpr:              whereExpr,
		Deferred:               deferred,
		Nodes:                  nodes,
		Immutable:              immutable,
		IsArrayIndex:           isArrayIndex,
		IsArrayFlattened:       isArrayFlattened,
		NumReplica:             uint32(numReplica),
		HashScheme:             c.CRC32,
		NumPartitions:          uint32(numPartition),
		RetainDeletedXATTR:     retainDeletedXATTR,
		NumDoc:                 numDoc,
		SecKeySize:             secKeySize,
		DocKeySize:             docKeySize,
		ArrSize:                arrSize,
		ResidentRatio:          residentRatio,
		Scope:                  scope,
		Collection:             collection,
		HasArrItemsCount:       hasArrItemsCount,
		IndexMissingLeadingKey: indexMissingLeadingKey,
	}

	idxDefn.NumReplica2.InitializeCounter(idxDefn.NumReplica)

	return idxDefn, nil, false
}

func (o *MetadataProvider) prepareIndexSpec(defn *c.IndexDefn) *planner.IndexSpec {

	var spec planner.IndexSpec
	spec.DefnId = defn.DefnId
	spec.Name = defn.Name
	spec.Bucket = defn.Bucket
	spec.Scope = defn.Scope
	spec.Collection = defn.Collection
	spec.IsPrimary = defn.IsPrimary
	spec.SecExprs = defn.SecExprs
	spec.WhereExpr = defn.WhereExpr
	spec.Deferred = defn.Deferred
	spec.Immutable = defn.Immutable
	spec.IsArrayIndex = defn.IsArrayIndex
	spec.Desc = defn.Desc
	spec.IndexMissingLeadingKey = defn.IndexMissingLeadingKey
	spec.NumPartition = uint64(defn.NumPartitions)
	spec.PartitionScheme = string(defn.PartitionScheme)
	spec.HashScheme = uint64(defn.HashScheme)
	spec.PartitionKeys = defn.PartitionKeys
	spec.Replica = uint64(defn.NumReplica) + 1
	spec.RetainDeletedXATTR = defn.RetainDeletedXATTR
	spec.ExprType = string(defn.ExprType)

	spec.NumDoc = defn.NumDoc
	spec.DocKeySize = defn.DocKeySize
	spec.SecKeySize = defn.SecKeySize
	spec.ArrKeySize = defn.SecKeySize
	spec.ArrSize = defn.ArrSize
	spec.ResidentRatio = defn.ResidentRatio
	spec.MutationRate = 0
	spec.ScanRate = 0

	// Get the storage mode from setting.  This is ONLY used for sizing purpose.  The actual
	// storage mode of the index will be determined when indexer receives create index request.
	// 1) if cluster storage mode is plasma, use plasma sizing.
	// 2) if cluster storage mode is moi, use moi sizing.
	// 3) if cluster storage mode is forestdb, then ignore sizing input.
	//    - During upgrade from forestdb to plasma, sizing will be ignored.
	// 4) if cluster storage mode is not available, then ignore sizing input.
	spec.Using = o.settings.StorageMode()

	return &spec
}

func (o *MetadataProvider) prepareNodeList(nodeList []string, watcherMap map[c.IndexerId]int) ([]string, error) {

	nodes := nodeList

	if len(nodeList) == 0 {
		// If user does not specify a node list, then get the node list where we have acquired locks.
		nodes = make([]string, 0, len(watcherMap))
		for indexerId, _ := range watcherMap {
			watcher, err := o.findWatcherByIndexerId(indexerId)
			if err != nil {
				return nil, errors.New("Fail to invoke planner.  Some of the indexers may be down or network partitioned from query process.")
			}
			nodeAddr, _, _, _ := security.EncryptPortFromAddr(watcher.getNodeAddr())
			nodes = append(nodes, strings.ToLower(nodeAddr))
		}
	}

	return nodes, nil
}

//
// Verify watchers matching the given node list
//
func (o *MetadataProvider) verifyNodeList(nodeList []string, watcherMap map[c.IndexerId]int) (bool, error) {

	if len(nodeList) != len(watcherMap) {
		return false, nil
	}

	// If user does not specify a node list, then get the node list where we have acquired locks.
	for indexerId, _ := range watcherMap {
		watcher, err := o.findWatcherByIndexerId(indexerId)
		if err != nil {
			return false, err
		}

		found := false
		for _, nodeAddr := range nodeList {
			if strings.ToLower(nodeAddr) == strings.ToLower(watcher.getNodeAddr()) {
				found = true
				break
			}
		}

		if !found {
			return false, nil
		}
	}

	return true, nil
}

func (o *MetadataProvider) plan(defn *c.IndexDefn, plan map[string]interface{}, watcherMap map[c.IndexerId]int,
	allowLostReplica bool, actualNumReplica uint32, enforceLimits bool) (map[int]map[c.IndexerId][]c.PartitionId, map[c.IndexerId][]c.IndexDefn, error) {

	spec := o.prepareIndexSpec(defn)
	nodes, err := o.prepareNodeList(defn.Nodes, watcherMap)
	if err != nil {
		return nil, nil, err
	}

	useGreedyPlanner := o.settings.UseGreedyPlanner()
	serverlessIndexLimit := o.settings.ServerlessIndexLimit()

	var solution *planner.Solution

	if c.IsServerlessDeployment() {
		solution, err = planner.ExecutePlan2(o.clusterUrl, spec, nodes, serverlessIndexLimit)
		if err != nil {
			return nil, nil, err
		}
	} else {
		solution, err = planner.ExecutePlan(o.clusterUrl, []*planner.IndexSpec{spec}, nodes,
			len(defn.Nodes) != 0, useGreedyPlanner, enforceLimits)
		if err != nil {
			return nil, nil, err
		}
	}

	layout := make(map[int]map[c.IndexerId][]c.PartitionId)
	for _, indexer := range solution.Placement {
		for _, index := range indexer.Indexes {
			if index.DefnId == defn.DefnId {
				if _, ok := layout[index.Instance.ReplicaId]; !ok {
					layout[index.Instance.ReplicaId] = make(map[c.IndexerId][]c.PartitionId)
				}
				layout[index.Instance.ReplicaId][c.IndexerId(indexer.IndexerId)] =
					append(layout[index.Instance.ReplicaId][c.IndexerId(indexer.IndexerId)], index.PartnId)
			}
		}
	}

	var definitions map[c.IndexerId][]c.IndexDefn
	definitions, err = o.getDefinitionsFromLayout(layout, defn, allowLostReplica, actualNumReplica)
	if err != nil {
		return nil, nil, err
	}

	return layout, definitions, nil
}

func (o *MetadataProvider) replicaRepair(defn *c.IndexDefn, numReplica c.Counter, increment int, plan map[string]interface{},
	watcherMap map[c.IndexerId]int) (map[int]map[c.IndexerId][]c.PartitionId, map[c.IndexerId][]c.IndexDefn, error) {

	curCount, _ := numReplica.Value()
	totalCount := int(curCount) + increment

	// prepareNodeList prepares the list of all nodes in the cluster.
	var nodes []string
	var err error
	nodes, err = o.prepareNodeList(nil, watcherMap)
	if err != nil {
		return nil, nil, err
	}

	var useNodes []string
	useNodes, err, _ = o.getNodesParam(plan)
	if err != nil {
		return nil, nil, err
	}

	if len(useNodes) != 0 {
		// Use planner to find the nodes hosting existing set of replicas
		// Input node list "useNodes" should contain all the nodes currently
		// hosting the existing replica, along with new nodes for replica
		// placement. Otherwise, the operation will fail.
		var currNodes []string
		currNodes, err = planner.FindIndexReplicaNodes(o.clusterUrl, nodes, defn.DefnId)
		if err != nil {
			return nil, nil, err
		}

		validateUseNodesList := func(useNodes, currNodes []string) error {
			for _, currNode := range currNodes {
				found := false
				for _, useNode := range useNodes {
					un := strings.ToLower(useNode)
					eun, _ := security.EncryptPortInAddr(useNode)
					cn := strings.ToLower(currNode)
					if un == cn || eun == cn {
						found = true
						break
					}
				}
				if !found {
					errMsg := "Please provide the list of nodes currently hosting"
					errMsg += " all the existing index replicas, along with the list"
					errMsg += " of new nodes to be used for replica placement."
					return errors.New(errMsg)
				}
			}
			return nil
		}

		if err = validateUseNodesList(useNodes, currNodes); err != nil {
			return nil, nil, err
		}
	} else {
		useNodes = nodes
	}
	enforceLimits := false
	if o.GetClusterVersion() >= c.INDEXER_71_VERSION {
		enforceLimits, err = o.limitsCfg.EnforceLimits()
		if err != nil {
			return nil, nil, err
		}
	}

	// Use the planner to find out where to place the replica.
	// If planner cannot read from the given list of nodes, it will return error.
	// In case of input plan has list of nodes to be used, pass the list along
	// for planner to place the replicas on those specific nodes.
	var solution *planner.Solution
	solution, err = planner.ExecuteReplicaRepair(o.clusterUrl, defn.DefnId, increment, useNodes, false, enforceLimits)
	if err != nil {
		return nil, nil, err
	}

	// make sure the number of indexer in the computed plan matches the number of nodes
	if len(useNodes) != len(solution.Placement) {
		err := fmt.Errorf("Cluster has failed nodes, undergo network partition, or unable to determine indexer node status.")
		return nil, nil, err
	}

	// build the layout and index definitions
	definitions := make(map[c.IndexerId][]c.IndexDefn)
	layout := make(map[int]map[c.IndexerId][]c.PartitionId)
	for _, indexer := range solution.Placement {
		for _, index := range indexer.Indexes {
			if index.DefnId == defn.DefnId {
				if _, ok := layout[index.Instance.ReplicaId]; !ok {
					layout[index.Instance.ReplicaId] = make(map[c.IndexerId][]c.PartitionId)
				}

				if _, ok := layout[index.Instance.ReplicaId][c.IndexerId(indexer.IndexerId)]; !ok {
					temp := *defn
					temp.NumReplica2 = numReplica
					temp.NumReplica2.Increment(uint32(increment))
					temp.InstId = index.Instance.InstId
					temp.ReplicaId = index.Instance.ReplicaId
					temp.NumPartitions = uint32(index.Instance.Pc.GetNumPartitions())

					definitions[c.IndexerId(indexer.IndexerId)] = append(definitions[c.IndexerId(indexer.IndexerId)], temp)
				}

				layout[index.Instance.ReplicaId][c.IndexerId(indexer.IndexerId)] =
					append(layout[index.Instance.ReplicaId][c.IndexerId(indexer.IndexerId)], index.PartnId)
			}
		}
	}

	// Make sure that planner has all the replica
	if len(layout) != int(totalCount+1) {
		logging.Errorf("Fail to allocate the requested number of replica. len(layout) %v totalCount+1 (%v).", len(layout), totalCount)
		return nil, nil, fmt.Errorf("Fail to allocate the requested number of replica.")
	}

	// update the partitions and versions for the definitions
	for replicaId, indexerPartitionMap := range layout {
		for indexerId, partitions := range indexerPartitionMap {
			defns := definitions[indexerId]
			for i, _ := range defns {
				if defns[i].ReplicaId == replicaId {
					defns[i].Partitions = partitions
					defns[i].Versions = make([]int, len(partitions))
				}
			}
		}
	}

	return layout, definitions, nil
}

func (o *MetadataProvider) replicaDrop(defn *c.IndexDefn, numReplica c.Counter, decrement int, numPartition int, dropReplicaId int, plan map[string]interface{},
	watcherMap map[c.IndexerId]int) ([]c.IndexInstId, []int, error) {

	nodes, err := o.prepareNodeList(nil, watcherMap)
	if err != nil {
		return nil, nil, err
	}

	solution, replicaIds, err := planner.ExecuteReplicaDrop(o.clusterUrl, defn.DefnId, nodes, numPartition, decrement, dropReplicaId)
	if err != nil {
		return nil, nil, err
	}

	instIds := []c.IndexInstId(nil)
	for _, replicaId := range replicaIds {
		found := false
		for _, indexer := range solution.Placement {
			for _, index := range indexer.Indexes {
				if index.DefnId == defn.DefnId {
					if index.Instance.ReplicaId == replicaId {
						instIds = append(instIds, index.InstId)
						found = true
						break
					}
				}
			}

			if found {
				break
			}
		}
	}

	return instIds, replicaIds, nil
}

func (o *MetadataProvider) isDecending(desc []bool) bool {

	hasDecending := false
	for _, flag := range desc {
		hasDecending = hasDecending || flag
	}

	return hasDecending
}

func (o *MetadataProvider) validateParamNames(plan map[string]interface{}) error {

	for param, _ := range plan {
		found := false
		for _, valid := range VALID_PARAM_NAMES {
			if param == valid {
				found = true
				break
			}
		}

		if !found {
			errStr := fmt.Sprintf("Invalid parameters in with-clause: '%v'. Valid parameters are ", param)
			for i, valid := range VALID_PARAM_NAMES {
				if i == 0 {
					errStr = fmt.Sprintf("%v '%v'", errStr, valid)
				} else {
					errStr = fmt.Sprintf("%v, '%v'", errStr, valid)
				}
			}
			return errors.New(errStr)
		}
	}

	return nil
}

func (o *MetadataProvider) getNodesParam(plan map[string]interface{}) ([]string, error, bool) {

	var nodes []string = nil

	//ignore "nodes" param for serverless deployement
	if c.IsServerlessDeployment() {
		return nil, nil, false
	}

	ns, ok := plan["nodes"].([]interface{})
	if ok {
		for _, nse := range ns {
			n, ok := nse.(string)
			if ok {
				nodes = append(nodes, n)
			} else {
				return nil, errors.New(fmt.Sprintf("Fails to create index.  Node '%v' is not valid", plan["nodes"])), false
			}
		}
	} else {
		n, ok := plan["nodes"].(string)
		if ok {
			nodes = []string{n}
		} else if _, ok := plan["nodes"]; ok {
			return nil, errors.New(fmt.Sprintf("Fails to create index.  Node '%v' is not valid", plan["nodes"])), false
		}
	}

	if len(nodes) != 0 {
		nodeSet := make(map[string]bool)
		for _, node := range nodes {
			if _, ok := nodeSet[node]; ok {
				return nil, errors.New(fmt.Sprintf("Fails to create index.  Node '%v' contain duplicate node ID", plan["nodes"])), false
			}
			nodeSet[node] = true
		}
	}

	return nodes, nil, true
}

func (o *MetadataProvider) getImmutableParam(partitionScheme c.PartitionScheme,
	plan map[string]interface{}, whereExpr string) (bool, error, bool) {

	// for partitioned index, by default, it is immutable, unless it is a partial index
	immutable := c.IsPartitioned(partitionScheme)
	if len(whereExpr) > 0 {
		immutable = false
	}

	return immutable, nil, false
}

func (o *MetadataProvider) getXATTRParam(plan map[string]interface{}) (bool, error, bool) {

	xattr := false

	xattr2, ok := plan["retain_deleted_xattr"].(bool)
	if !ok {
		xattr_str, ok := plan["retain_deleted_xattr"].(string)
		if ok {
			var err error
			xattr2, err = strconv.ParseBool(xattr_str)
			if err != nil {
				return false, errors.New("Fails to create index.  Parameter retain_deleted_xattr must be a boolean value of (true or false)."), false
			}
			xattr = xattr2

		} else if _, ok := plan["retain_deleted_xattr"]; ok {
			return false, errors.New("Fails to create index.  Parameter retain_deleted_xattr must be a boolean value of (true or false)."), false
		}
	} else {
		xattr = xattr2
	}

	return xattr, nil, false
}

func (o *MetadataProvider) getDeferredParam(plan map[string]interface{}) (bool, error, bool) {

	deferred := false

	deferred2, ok := plan["defer_build"].(bool)
	if !ok {
		deferred_str, ok := plan["defer_build"].(string)
		if ok {
			var err error
			deferred2, err = strconv.ParseBool(deferred_str)
			if err != nil {
				return false, errors.New("Fails to create index.  Parameter defer_build must be a boolean value of (true or false)."), false
			}
			deferred = deferred2

		} else if _, ok := plan["defer_build"]; ok {
			return false, errors.New("Fails to create index.  Parameter defer_build must be a boolean value of (true or false)."), false
		}
	} else {
		deferred = deferred2
	}

	return deferred, nil, false
}

func (o *MetadataProvider) validatePartitionKeys(partitionScheme c.PartitionScheme, partitionKeys []string, secKeys []string, isPrimary bool) error {

	if partitionScheme != c.SINGLE && partitionScheme != c.KEY {
		return errors.New(fmt.Sprintf("Fails to create index.  Partition Scheme %v is not allowed.", partitionScheme))
	}

	if partitionScheme == c.SINGLE && len(partitionKeys) != 0 {
		return errors.New(fmt.Sprintf("Fails to create index.  Cannot support partition keys for non-partitioned index."))
	}

	if partitionScheme == c.SINGLE {
		return nil
	}

	if partitionScheme == c.KEY && len(partitionKeys) == 0 {
		return errors.New(fmt.Sprintf("Fails to create index.  Must specify partition keys for partitioned index."))
	}

	secExprs := make(expression.Expressions, 0, len(secKeys))
	for _, key := range secKeys {
		expr, err := parser.Parse(key)
		if err != nil {
			return errors.New(fmt.Sprintf("Fails to create index.  Invalid index key %v.", key))
		}
		secExprs = append(secExprs, expr)
	}

	partnExprs := make(expression.Expressions, 0, len(partitionKeys))
	for _, key := range partitionKeys {

		expr, err := parser.Parse(key)
		if err != nil {
			return errors.New(fmt.Sprintf("Fails to create index.  Invalid partition key %v.", key))
		}
		partnExprs = append(partnExprs, expr)
	}

	/*
		id := expression.NewField(expression.NewMeta(), expression.NewFieldName("id", false))
		idself := expression.NewField(expression.NewMeta(expression.NewIdentifier("self")), expression.NewFieldName("id", false))
		for i, partnExpr := range partnExprs {
			found := false

			if !isPrimary {
				for _, secExpr := range secExprs {
					if partnExpr.EquivalentTo(secExpr) || partnExpr.Depends(id) || partnExpr.Depends(idself) {
						found = true
						break
					}
				}
			} else if partnExpr.DependsOn(id) || partnExpr.DependsOn(idself) {
				found = true
			}

			if !found {
				return errors.New(fmt.Sprintf("Fails to create index. Partition key '%v' is not an index key.", partitionKeys[i]))
			}
		}
	*/

	for i, partnExpr := range partnExprs {

		for j := i + 1; j < len(partnExprs); j++ {
			if partnExpr.EquivalentTo(partnExprs[j]) {
				return errors.New(fmt.Sprintf("Fails to create index. Do not allow duplicate partition key '%v'.", partitionKeys[i]))
			}
		}

		if isArray, _, _ := partnExpr.IsArrayIndexKey(); isArray {
			return errors.New(fmt.Sprintf("Fails to create index. Partition key '%v' cannot be an array expression.", partitionKeys[i]))
		}
	}

	return nil
}

func (o *MetadataProvider) getNumPartitionParam(scheme c.PartitionScheme, plan map[string]interface{}, version uint64) (int, error, bool) {

	if scheme == c.SINGLE {
		return 1, nil, false
	}

	numPartition := int(o.settings.NumPartition())

	//Use config setting for numPartition for serverless,
	//user override is not allowed
	if c.IsServerlessDeployment() {
		return numPartition, nil, false
	}

	numPartition2, ok := plan["num_partition"].(float64)
	if !ok {
		numPartition_str, ok := plan["num_partition"].(string)
		if ok {
			var err error
			numPartition3, err := strconv.ParseInt(numPartition_str, 10, 64)
			if err != nil {
				return 0, errors.New("Fails to create index.  Parameter num_partition must be a integer value."), false
			}
			numPartition = int(numPartition3)

		} else if _, ok := plan["num_partition"]; ok {
			return 0, errors.New("Fails to create index.  Parameter num_partition must be a integer value."), false
		}
	} else {
		numPartition = int(numPartition2)
	}

	if numPartition <= 0 {
		return 0, errors.New("Fails to create index.  Parameter num_partition must be a positive value."), false
	}

	return numPartition, nil, false
}

func (o *MetadataProvider) getReplicaParam(plan map[string]interface{}, version uint64) (int, error, bool) {

	numReplica := int(0)
	if version >= c.INDEXER_50_VERSION {
		numReplica = int(o.settings.NumReplica())
	}

	//use num_replica from config for serverless,
	//user override is not allowed
	if c.IsServerlessDeployment() {
		return numReplica, nil, false
	}

	numReplica2, ok := plan["num_replica"].(int64)
	if !ok {
		numReplica2, ok := plan["num_replica"].(float64)
		if !ok {
			numReplica_str, ok := plan["num_replica"].(string)
			if ok {
				var err error
				numReplica2, err := strconv.ParseInt(numReplica_str, 10, 64)
				if err != nil {
					return 0, errors.New("Parameter num_replica must be a integer value."), false
				}
				numReplica = int(numReplica2)

			} else if v, ok := plan["num_replica"]; ok {
				return 0, fmt.Errorf("Parameter num_replica must be a integer value (%v).", reflect.TypeOf(v)), false
			}
		} else {
			numReplica = int(numReplica2)
		}
	} else {
		numReplica = int(numReplica2)
	}

	if numReplica > 0 && version < c.INDEXER_50_VERSION {
		return 0, errors.New("Replica is not supported.  This option is enabled after cluster is fully upgraded and there is no failed node."), false
	}

	if numReplica < 0 {
		return 0, errors.New("Parameter num_replica must be a positive value."), false
	}

	return numReplica, nil, false
}

func (o *MetadataProvider) getReplicaParam2(plan map[string]interface{}, version uint64) (int, error, bool) {

	numReplica := int(-1)

	numReplica2, ok := plan["num_replica"].(int64)
	if !ok {
		numReplica2, ok := plan["num_replica"].(float64)
		if !ok {
			numReplica_str, ok := plan["num_replica"].(string)
			if ok {
				var err error
				numReplica2, err := strconv.ParseInt(numReplica_str, 10, 64)
				if err != nil {
					return 0, errors.New("Parameter num_replica must be a integer value."), false
				}
				numReplica = int(numReplica2)
				if numReplica < 0 {
					return 0, errors.New("Parameter num_replica must be a positive value."), false
				}

			} else if v, ok := plan["num_replica"]; ok {
				return 0, fmt.Errorf("Parameter num_replica must be a integer value (%v).", reflect.TypeOf(v)), false
			}
		} else {
			numReplica = int(numReplica2)
			if numReplica < 0 {
				return 0, errors.New("Parameter num_replica must be a positive value."), false
			}
		}
	} else {
		numReplica = int(numReplica2)
		if numReplica < 0 {
			return 0, errors.New("Parameter num_replica must be a positive value."), false
		}
	}

	if numReplica > -1 && version < c.INDEXER_65_VERSION {
		return 0, errors.New("Replica is not supported.  This option is enabled after cluster is fully upgraded and there is no failed node."), false
	}

	return numReplica, nil, false
}

func (o *MetadataProvider) getReplicaIdParam(plan map[string]interface{}, version uint64) (int, error, bool) {

	replicaId := int(-1)

	replicaId2, ok := plan["replicaId"].(int64)
	if !ok {
		replicaId2, ok := plan["replicaId"].(float64)
		if !ok {
			replicaId_str, ok := plan["replicaId"].(string)
			if ok {
				var err error
				replicaId2, err := strconv.ParseInt(replicaId_str, 10, 64)
				if err != nil {
					return 0, errors.New("Parameter replicaId must be a integer value."), false
				}
				replicaId = int(replicaId2)
				if replicaId < 0 {
					return 0, errors.New("Parameter replicaId must be a positive value."), false
				}

			} else if v, ok := plan["replicaId"]; ok {
				return 0, fmt.Errorf("Parameter replicaId must be a integer value (%v).", reflect.TypeOf(v)), false
			}
		} else {
			replicaId = int(replicaId2)
			if replicaId < 0 {
				return 0, errors.New("Parameter replicaId must be a positive value."), false
			}
		}
	} else {
		replicaId = int(replicaId2)
		if replicaId < 0 {
			return 0, errors.New("Parameter replicaId must be a positive value."), false
		}
	}

	if replicaId > -1 && version < c.INDEXER_65_VERSION {
		return 0, errors.New("ReplicaId is not supported.  This option is enabled after cluster is fully upgraded and there is no failed node."), false
	}

	return replicaId, nil, false
}

func (o *MetadataProvider) getDocKeySizeParam(plan map[string]interface{}) (uint64, error, bool) {

	docKeySize := uint64(0)

	docKeySize2, ok := plan["docKeySize"].(float64)
	if !ok {
		docKeySize_str, ok := plan["docKeySize"].(string)
		if ok {
			var err error
			docKeySize3, err := strconv.ParseInt(docKeySize_str, 10, 64)
			if err != nil {
				return 0, errors.New("Fails to create index.  Parameter docKeySize must be a integer value."), false
			}
			docKeySize = uint64(docKeySize3)

		} else if _, ok := plan["docKeySize"]; ok {
			return 0, errors.New("Fails to create index.  Parameter docKeySize must be a integer value."), false
		}
	} else {
		docKeySize = uint64(docKeySize2)
	}

	if docKeySize < 0 {
		return 0, errors.New("Fails to create index.  Parameter docKeySize must be a positive value."), false
	}

	return docKeySize, nil, false
}

func (o *MetadataProvider) getSecKeySizeParam(plan map[string]interface{}) (uint64, error, bool) {

	secKeySize := uint64(0)

	secKeySize2, ok := plan["secKeySize"].(float64)
	if !ok {
		secKeySize_str, ok := plan["secKeySize"].(string)
		if ok {
			var err error
			secKeySize3, err := strconv.ParseInt(secKeySize_str, 10, 64)
			if err != nil {
				return 0, errors.New("Fails to create index.  Parameter secKeySize must be a integer value."), false
			}
			secKeySize = uint64(secKeySize3)

		} else if _, ok := plan["secKeySize"]; ok {
			return 0, errors.New("Fails to create index.  Parameter secKeySize must be a integer value."), false
		}
	} else {
		secKeySize = uint64(secKeySize2)
	}

	if secKeySize < 0 {
		return 0, errors.New("Fails to create index.  Parameter secKeySize must be a positive value."), false
	}

	return secKeySize, nil, false
}

func (o *MetadataProvider) getArrSizeParam(plan map[string]interface{}) (uint64, error, bool) {

	arrSize := uint64(0)

	arrSize2, ok := plan["arrSize"].(float64)
	if !ok {
		arrSize_str, ok := plan["arrSize"].(string)
		if ok {
			var err error
			arrSize3, err := strconv.ParseInt(arrSize_str, 10, 64)
			if err != nil {
				return 0, errors.New("Fails to create index.  Parameter arrSize must be a integer value."), false
			}
			arrSize = uint64(arrSize3)

		} else if _, ok := plan["arrSize"]; ok {
			return 0, errors.New("Fails to create index.  Parameter arrSize must be a integer value."), false
		}
	} else {
		arrSize = uint64(arrSize2)
	}

	if arrSize < 0 {
		return 0, errors.New("Fails to create index.  Parameter arrSize must be a positive value."), false
	}

	return arrSize, nil, false
}

func (o *MetadataProvider) getNumDocParam(plan map[string]interface{}) (uint64, error, bool) {

	numDoc := uint64(0)

	numDoc2, ok := plan["numDoc"].(float64)
	if !ok {
		numDoc_str, ok := plan["numDoc"].(string)
		if ok {
			var err error
			numDoc3, err := strconv.ParseInt(numDoc_str, 10, 64)
			if err != nil {
				return 0, errors.New("Fails to create index.  Parameter numDoc must be a integer value."), false
			}
			numDoc = uint64(numDoc3)

		} else if _, ok := plan["numDoc"]; ok {
			return 0, errors.New("Fails to create index.  Parameter numDoc must be a integer value."), false
		}
	} else {
		numDoc = uint64(numDoc2)
	}

	if numDoc < 0 {
		return 0, errors.New("Fails to create index.  Parameter numDoc must be a positive value."), false
	}

	return numDoc, nil, false
}

func (o *MetadataProvider) getResidentRatioParam(plan map[string]interface{}) (float64, error, bool) {

	residentRatio := float64(100)

	residentRatio2, ok := plan["residentRatio"].(float64)
	if !ok {
		residentRatio_str, ok := plan["residentRatio"].(string)
		if ok {
			var err error
			residentRatio3, err := strconv.ParseFloat(residentRatio_str, 64)
			if err != nil {
				return 0, errors.New("Fails to create index.  Parameter residentRatio must be a float value."), false
			}
			residentRatio = residentRatio3

		} else if _, ok := plan["residentRatio"]; ok {
			return 0, errors.New("Fails to create index.  Parameter residentRatio must be a float value."), false
		}
	} else {
		residentRatio = residentRatio2
	}

	if residentRatio < 0 {
		return 0, errors.New("Fails to create index.  Parameter residentRatio must be a positive value."), false
	}

	return residentRatio, nil, false
}

func (o *MetadataProvider) findWatchersWithRetry(nodes []string, numReplica int, partitioned bool, legacy bool) ([]*watcher, error, bool) {

	var watchers []*watcher
	count := 0

RETRY1:
	errCode := 0
	if len(nodes) == 0 {
		if partitioned {
			// partitioned
			watchers := o.getAllAvailWatchers()
			if len(watchers) >= numReplica+1 {
				return watchers, nil, false
			}

			if len(watchers) == 0 {
				errCode = 1
			} else {
				errCode = 3
			}

		} else {
			// non-partitioned
			watcher, numWatcher := o.findNextAvailWatcher(watchers, true)
			if watcher == nil {
				watcher, numWatcher = o.findNextAvailWatcher(watchers, false)
			}
			if watcher == nil {
				if len(watchers) < numReplica+1 {
					watchers = nil
					if numWatcher == 0 {
						errCode = 1
					} else {
						errCode = 3
					}
				}
			} else {
				watchers = append(watchers, watcher)
				if !legacy || len(watchers) < numReplica+1 {
					goto RETRY1
				}
			}
		}
	} else {
		for _, node := range nodes {
			watcher := o.findWatcherByNodeAddr(node)
			if watcher == nil {
				watchers = nil
				errCode = 2
				break
			} else {
				watchers = append(watchers, watcher)
			}
		}
	}

	if errCode != 0 && count < 50 && !o.AllWatchersAlive() {
		logging.Debugf("MetadataProvider:findWatcherWithRetry(): cannot find available watcher. Retrying ...")
		time.Sleep(time.Duration(500) * time.Millisecond)
		count++
		goto RETRY1
	}

	if errCode == 1 {
		stmt1 := fmt.Sprintf("%v", c.ErrIndexerNotAvailable.Error())
		stmt2 := "Index Service can be in bootstrap, recovery, or non-reachable."
		stmt3 := "Please retry the operation at a later time."
		return nil, errors.New(fmt.Sprintf("%s %s %s", stmt1, stmt2, stmt3)), false

	} else if errCode == 2 {
		stmt1 := "Fails to create index.  Nodes %s does not exist or is not running"
		return nil, errors.New(fmt.Sprintf(stmt1, nodes)), true

	} else if errCode == 3 {
		stmt1 := fmt.Sprintf("%v count of %v. ", c.ErrNotEnoughIndexers.Error(), numReplica)
		stmt2 := stmt1 + "Some indexer nodes may be marked as excluded."
		return nil, errors.New(stmt2), true
	}

	return watchers, nil, false
}

func (o *MetadataProvider) deleteScheduleTokens(defnID c.IndexDefnId) (bool, bool, error) {
	var schedToken, stopToken bool
	var err error
	if schedToken, err = mc.ScheduleCreateTokenExist(defnID); err != nil {
		return schedToken, stopToken, fmt.Errorf("ScheduleCreateTokenExist:%v:%v", defnID, err)
	}

	if stopToken, err = mc.StopScheduleCreateTokenExist(defnID); err != nil {
		return schedToken, stopToken, fmt.Errorf("ScheduleCreateTokenExist:%v:%v", defnID, err)
	}

	// Best effort deletion - if the token is not present, the operation will not fail.
	if err = mc.DeleteScheduleCreateToken(defnID); err != nil {
		return schedToken, stopToken, fmt.Errorf("DeleteScheduleCreateToken:%v:%v", defnID, err)
	}

	if err = mc.DeleteStopScheduleCreateToken(defnID); err != nil {
		return schedToken, stopToken, fmt.Errorf("DeleteStopScheduleCreateToken:%v:%v", defnID, err)
	}

	return schedToken, stopToken, nil
}

func (o *MetadataProvider) DropIndex(defnID c.IndexDefnId) error {

	// place token for recovery.  Even if the index does not exist, the delete token will
	// be cleaned up during rebalance.  By placing the delete token, it will make sure that the
	// outstanding create token will be deleted.
	if err := mc.PostDeleteCommandToken(defnID, false); err != nil {
		return errors.New(fmt.Sprintf("Fail to Drop Index due to internal errors.  Error=%v.", err))
	}

	var schedToken, stopToken bool
	var tokenErr error
	if schedToken, stopToken, tokenErr = o.deleteScheduleTokens(defnID); tokenErr != nil {
		return errors.New(fmt.Sprintf("Fail to Drop Index due to internal errors. "+
			"Cleanup may happen in the background.  Error=%v.", tokenErr))
	}

	// find index -- this method will not return the index if the index is in DELETED
	// status (but defn exists).
	meta := o.findIndex(defnID)
	if meta == nil {
		// If both schedule create token and stop schedule create token exist,
		// and the metadata is not present, it is safe to return success for
		// drop index just after dropping the tokens.
		if schedToken && stopToken {
			return nil
		}

		// If only schedule create token exists, and stop schedule token or
		// the metadata doesn't exists, then there is a possible race condition.
		// If the scheduled index creator finds the delete command token before
		// it tries to create the index, it will not create the index. But if
		// the scheduled index creator creates the index, then lifecycle manager
		// will drop the index. At this point, there is no easy way to know this.
		if schedToken && !stopToken {
			return errors.New("The index was scheduled for background creation." +
				"The cleanup will happen in the background.")
		}

		return errors.New("Index does not exist.")
	}

	// find watcher -- This method does not check index status (return the watcher even
	// if index is in created status). This return an error if  watcher (holding the index)
	// is dropped asynchronously (concurrent unwatchMetadata).
	watchers, err := o.findWatchersByDefnIdIgnoreStatus(defnID)
	if err != nil {
		return errors.New(fmt.Sprintf("Cannot locate cluster node hosting Index %s.", meta.Definition.Name))
	}

	// Make a request to drop the index, the index may be dropped in parallel before this MetadataProvider
	// is aware of it.  (e.g. bucket flush).  The server side will have to check for this condition.
	// If the index is already deleted, indexer WILL NOT return an error.
	key := fmt.Sprintf("%d", defnID)
	errMap := make(map[string]bool)
	for _, watcher := range watchers {
		_, err = watcher.makeRequest(OPCODE_DROP_INDEX, key, []byte(""))
		if err != nil {
			errMap[err.Error()] = true
		}
	}

	if len(errMap) != 0 {
		errStr := ""
		for msg, _ := range errMap {
			errStr += msg + "\n"
		}

		if len(errStr) != 0 {
			msg := fmt.Sprintf("Fail to drop index on some indexer nodes.  Error=%s.  ", errStr)
			msg += "If cluster or indexer is currently unavailable, the operation will automatically retry after cluster is back to normal."
			return errors.New(msg)
		}
	}

	return nil
}

func (o *MetadataProvider) BuildIndexes(defnIDs []c.IndexDefnId) error {

	watcherIndexMap := make(map[c.IndexerId][]c.IndexDefnId)
	watcherNodeMap := make(map[c.IndexerId]string)
	defnList := ([]c.IndexDefnId)(nil)
	buckets := make(map[string]bool)

	for _, id := range defnIDs {

		// Has the index been deleted?
		found, err := mc.DeleteCommandTokenExist(id)
		if err != nil {
			return errors.New(fmt.Sprintf("Fail to Build Index due to internal errors.  Error=%v.", err))
		}
		if found {
			logging.Warnf("Index %v have been deleted. Skip build index.", id)
			continue
		}

		// Has the index been built?
		found, err = mc.BuildCommandTokenExist(id)
		if err != nil {
			return errors.New(fmt.Sprintf("Fail to Build Index due to internal errors.  Error=%v.", err))
		}
		if found {
			logging.Warnf("Index %v has already built. Skip build index.", id)
			continue
		}

		// find index -- this method will not return deleted index. This will only return an instance
		// that is valid.
		//
		// If meta not found, it could mean index is deleted or there is a race condition so
		// index metadata has not yet reached cbq. In this case, we will still send the index
		// over to indexer for it to decide whether build is necessary.
		if meta := o.findIndex(id); meta != nil {

			checkState := func(state c.IndexState) bool {
				if state != c.INDEX_STATE_READY && state != c.INDEX_STATE_CREATED {
					if state == c.INDEX_STATE_INITIAL || state == c.INDEX_STATE_CATCHUP {
						logging.Warnf("Index %v is being built .", meta.Definition.Name)
					} else if state == c.INDEX_STATE_ACTIVE {
						logging.Warnf("Index %v has already built .", meta.Definition.Name)
					} else {
						logging.Warnf("Index %v have been deleted.", meta.Definition.Name)
					}
					return false
				}
				return true
			}

			// metadata exist, check if we should build the index by checking index state.
			if !checkState(meta.State) {
				continue
			}

			buckets[meta.Definition.Bucket] = true
		}

		// find watcher -- This method does not check index status (return the watcher even
		// if index is in created status). So this return an error if  watcher is dropped
		// asynchronously (some parallel go-routine unwatchMetadata).
		watchers, err := o.findWatchersByDefnIdIgnoreStatus(id)
		if err != nil {
			return errors.New(fmt.Sprintf("Fail to Build Index due to internal errors.  Error=%v.", err))
		}

		// There is at least one watcher (one indexer node)
		defnList = append(defnList, id)

		for _, watcher := range watchers {
			indexerId := watcher.getIndexerId()
			var found bool
			for _, tid := range watcherIndexMap[indexerId] {
				if tid == id {
					found = true
					break
				}
			}

			if !found {
				watcherIndexMap[indexerId] = append(watcherIndexMap[indexerId], id)
				watcherNodeMap[indexerId] = watcher.getNodeAddr()
			}
		}
	}

	if c.IsServerlessDeployment() {
		for bucket, _ := range buckets {
			if err := c.CheckIngressLockdown(bucket); err != nil {
				errMsg := fmt.Sprintf("Fail to Build index due to error %v for %v", err, defnIDs)
				logging.Errorf("%v", errMsg)
				if err.Error() == c.ErrNoIngress.Error() {
					return c.ErrDiskLimitReached
				}

				return err
			}
		}
	}

	// place token for recovery.
	for _, id := range defnList {
		if err := mc.PostBuildCommandToken(id); err != nil {
			return errors.New(fmt.Sprintf("Fail to Build Index due to internal errors.  Error=%v.", err))
		}
	}

	// send request
	errMap := make(map[string]bool)
	for indexerId, idList := range watcherIndexMap {
		if err := o.SendBuildIndexRequest(indexerId, idList, watcherNodeMap[indexerId]); err != nil {
			errMap[err.Error()] = true
		}
	}

	if len(errMap) != 0 {
		errStr := ""
		for msg, _ := range errMap {
			errStr += msg + "\n"
		}
		return errors.New(errStr)
	}

	return nil
}

func (o *MetadataProvider) SendBuildIndexRequest(indexerId c.IndexerId, idList []c.IndexDefnId, addr string) error {

	watcher, err := o.findAliveWatcherByIndexerId(indexerId)
	if err != nil {
		return fmt.Errorf("Cannot reach node %v.  Index build will be retried in background once network connection is re-established.", addr)
	}

	list := BuildIndexIdList(idList)

	content, err := MarshallIndexIdList(list)
	if err != nil {
		return err
	}

	_, err = watcher.makeRequest(OPCODE_BUILD_INDEX, "Index Build", content)
	if err != nil {
		return err
	}

	return nil
}

func (o *MetadataProvider) BroadcastAlterReplicaCountRequest(defn *c.IndexDefn) error {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	content, err := c.MarshallIndexDefn(defn)
	if err != nil {
		return err
	}

	for _, watcher := range o.watchers {
		_, err = watcher.makeRequest(OPCODE_UPDATE_REPLICA_COUNT, "Alter Replica Count", content)
		if err != nil {
			return err
		}
	}

	return nil
}

func (o *MetadataProvider) SendAlterReplicaCountRequest(indexerId c.IndexerId, defn *c.IndexDefn, addr string) error {

	watcher, err := o.findAliveWatcherByIndexerId(indexerId)
	if err != nil {
		return fmt.Errorf("Cannot reach node %v.  Alter replica count will be retried in background once network connection is re-established.", addr)
	}

	content, err := c.MarshallIndexDefn(defn)
	if err != nil {
		return err
	}

	_, err = watcher.makeRequest(OPCODE_UPDATE_REPLICA_COUNT, "Alter Replica Count", content)
	if err != nil {
		return err
	}

	return nil
}

func (o *MetadataProvider) SendGetReplicaCountRequest(indexerId c.IndexerId, defnId c.IndexDefnId) (*c.Counter, error) {

	watcher, err := o.findAliveWatcherByIndexerId(indexerId)
	if err != nil {
		return nil, fmt.Errorf("Cannot reach node.")
	}

	content, err := json.Marshal(defnId)
	if err != nil {
		return nil, err
	}

	result, err := watcher.makeRequest(OPCODE_GET_REPLICA_COUNT, "Get Index Num Replica", content)
	if err != nil {
		return nil, err
	}

	numReplica, err := c.UnmarshallCounter(result)
	if err != nil {
		return nil, err
	}

	return numReplica, nil
}

func (o *MetadataProvider) SendCheckTokenRequest(indexerId c.IndexerId, defnId c.IndexDefnId, flag uint32) (bool, error) {

	watcher, err := o.findAliveWatcherByIndexerId(indexerId)
	if err != nil {
		return false, fmt.Errorf("Cannot reach node.")
	}

	check := CheckToken{DefnId: defnId, Flag: flag}
	content, err := MarshallCheckToken(&check)
	if err != nil {
		return false, err
	}

	result, err := watcher.makeRequest(OPCODE_CHECK_TOKEN_EXIST, "Get Token Exist", content)
	if err != nil {
		return false, err
	}

	var exist bool
	if err := json.Unmarshal(result, &exist); err != nil {
		return false, err
	}

	return exist, nil
}

func (o *MetadataProvider) ListIndex() ([]*IndexMetadata, uint64) {

	indices, version := o.repo.listDefnWithValidInst()
	result := make([]*IndexMetadata, 0, len(indices))

	for _, meta := range indices {
		if o.isValidIndexFromActiveIndexer(meta) {
			result = append(result, meta)
		}
	}

	return result, version
}

//
// Find an index with at least one valid instance.  Note that the instance may not be well-formed.
//
func (o *MetadataProvider) findIndex(id c.IndexDefnId) *IndexMetadata {

	indices, _ := o.repo.listDefnWithValidInst()
	if meta, ok := indices[id]; ok {
		if o.isValidIndexFromActiveIndexer(meta) {
			return meta
		}
	}

	return nil
}

func (o *MetadataProvider) FindServiceForIndexer(id c.IndexerId) (adminport string, queryport string, httpport string, err error) {

	watcher, err := o.findWatcherByIndexerId(id)
	if err != nil {
		return "", "", "", errors.New(fmt.Sprintf("Cannot locate cluster node."))
	}

	return watcher.getAdminAddr(), watcher.getScanAddr(), watcher.getHttpAddr(), nil
}

func (o *MetadataProvider) UpdateServiceAddrForIndexer(id c.IndexerId, adminport string, refreshServiceMap bool) error {

	watcher, err := o.findWatcherByIndexerId(id)
	if err != nil {
		return errors.New(fmt.Sprintf("Cannot locate cluster node."))
	}

	if refreshServiceMap {
		watcher.refreshServiceMap()
	}
	return watcher.updateServiceMap(adminport)
}

func (o *MetadataProvider) findIndexByName(name, bucket, scope, collection string) *IndexMetadata {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	indices, _ := o.repo.listDefnWithValidInst()
	for _, meta := range indices {
		if meta.Definition.Name == name &&
			meta.Definition.Bucket == bucket &&
			meta.Definition.Scope == scope &&
			meta.Definition.Collection == collection {
			// will not hold lock on metadataRepo
			if o.isValidIndexFromActiveIndexerNoLock(meta) {
				return meta
			}
		}
	}

	return nil
}

//
// Get the list of nodes from a healthy cluster.  This function depends on ns-server
// to provide cluster info, and since cluster info is eventual consistent, this
// function cannot always return immedidate cluster status.  This function can only
// provides a snapshot of healthy cluster nodes at a point in time.
//
func (o *MetadataProvider) getNodesInHealthyCluster() ([]string, error) {

	// Lock down metadata provider while checking cluster.  This will block any watchMetadata()
	// or unwatchMetadata(). This lock ensures the state of metadata provider does not change between
	// checkProviderHealthNoLock() and getAllWatcherNodeAddrNoLock().
	o.mutex.Lock()
	defer o.mutex.Unlock()

	// Check cluster health
	// 1) All watchers are alive (have live connections)
	// 2) The number of watchers match the number of active nodes
	// 3) There is no failed node
	// 4) There is no unhealthy node
	// 5) Watcher is not in the middle of synchronization with indexer (timeout at 100ms)
	healthy, err := o.checkProviderHealthNoLock()
	if err != nil {
		return nil, err
	}

	if !healthy {
		return nil, errors.New("Cluster has failed nodes, undergo network partition, or unable to determine indexer node status.")
	}

	// Get the list of nodes in the healthy cluster
	nodes := o.getAllWatcherNodeAddrNoLock()

	return nodes, nil
}

//
// The caller must acquire locks on indexer before calling this method. This ensures that there is
// no concurrent create/alter index running in parallel.
//
func (o *MetadataProvider) getNumReplica(defnId c.IndexDefnId, name, bucket, scope, collection string,
	watcherMap map[c.IndexerId]int) (*c.Counter, error) {

	// Get num replica from each watcher/indexer.  Each indexer will lookup numReplica
	// 1) create token
	// 2) local index definition
	// Metadata provider will merge the num replica (convergent counter) from each indexer to determine the latest value.
	//
	var numReplica c.Counter
	for indexerId, _ := range watcherMap {
		numReplica2, err := o.SendGetReplicaCountRequest(indexerId, defnId)
		if err != nil {
			logging.Errorf("Cannot retrieve num replica from index %v from indexer %v", defnId, indexerId)
			return nil, err
		}

		if numReplica2 != nil && numReplica2.IsValid() {
			result, merged, err := numReplica.MergeWith(*numReplica2)
			if err != nil {
				logging.Errorf("Cannot merge counter for index definition %v from indexer %v", defnId, indexerId)
				return nil, err
			}
			if merged {
				numReplica = result
			}
		}
	}

	if !numReplica.IsValid() {
		return nil, fmt.Errorf("Cannot determine number of replica for index (%v, %v, %v, %v)", name, bucket, scope, collection)
	}

	return &numReplica, nil
}

// When plan (with clause) mentions a specific set of nodes to be used for
// AlterReplicaCount, all the nodes - including the new nodes and the nodes
// hosting the exising replicas - should be specified in the input plan.
// Move index replica also has the same expectation.
func (o *MetadataProvider) AlterReplicaCount(action string, defnId c.IndexDefnId, plan map[string]interface{}) error {

	// Support for 6.5 and onwards
	clusterVersion := o.GetClusterVersion()
	if clusterVersion < c.INDEXER_65_VERSION {
		return errors.New("Alter index requires version 6.5 or higher")
	}

	// Verify if the cluster is in a healthy state.  Retrieve the node list from healthy cluster.
	nodeList, err := o.getNodesInHealthyCluster()
	if err != nil {
		return fmt.Errorf("Fail to alter index: %v", err)
	}

	// Find the index metadata.   We don't need the index metadata to be latest.  We just need
	// the definition for acquiring exclusive lock from indexer.
	idxMeta := o.findIndex(defnId)
	if idxMeta == nil {
		return fmt.Errorf("Index %s does not exist.", defnId)
	}

	dropReplicaId := -1
	count := -1

	if action == "replica_count" {
		var err error
		count, err, _ = o.getReplicaParam2(plan, clusterVersion)
		if err != nil {
			return fmt.Errorf("Fail to alter index: %v", err)
		}

		if count == -1 {
			return fmt.Errorf("Missing argument num_replica")
		}
	}

	if action == "drop_replica" {
		var err error
		dropReplicaId, err, _ = o.getReplicaIdParam(plan, clusterVersion)
		if err != nil {
			return fmt.Errorf("Fail to alter index: %v", err)
		}

		if dropReplicaId == -1 {
			return fmt.Errorf("Missing argument replicaId")
		}
	}

	//
	// Prepare phase.  This is to seek full quorum from all the indexers by acquiring locks.
	//
	// This operation will fail if
	// 1) Any indexer is unreachable
	// 2) Any indexer is serving another create/alter index request
	//
	// Once the full quorum is achieved, the indexer will not accept any other create/alter index request until:
	// 1) This create/alter index request has completed
	// 2) This create/alter index request has been explicity canceled
	// 3) Indexer has timed out
	//
	defn := *idxMeta.Definition
	numPartition := idxMeta.numPartitions()
	watcherMap, err, _, _ := o.makePrepareIndexRequest(defn.DefnId, defn.Name, defn.Bucket,
		defn.Scope, defn.Collection, nil, defn.PartitionScheme, count, false, 0)

	if err != nil {
		o.cancelPrepareIndexRequest(defn.DefnId, watcherMap)
		return fmt.Errorf("Fail to alter index: %v", err)
	}

	// We have a snapshot of node list from a healthy cluster.
	// Let see if the watcher map matches the node list.
	valid, err := o.verifyNodeList(nodeList, watcherMap)
	if err != nil {
		o.cancelPrepareIndexRequest(defn.DefnId, watcherMap)
		return fmt.Errorf("Fail to alter index: %v", err)
	}
	if !valid {
		o.cancelPrepareIndexRequest(defn.DefnId, watcherMap)
		return fmt.Errorf("Cluster has failed nodes, undergo network partition, or unable to determine indexer node status.")
	}

	// Fetch the numReplica
	numReplica, err := o.getNumReplica(defn.DefnId, defn.Name, defn.Bucket, defn.Scope, defn.Collection, watcherMap)
	if err != nil {
		o.cancelPrepareIndexRequest(defn.DefnId, watcherMap)
		return fmt.Errorf("Fail to alter index: %v", err)
	}
	curCount, _ := numReplica.Value()

	logging.Infof("alter replica count.  Current num replica %v new replica count %v", curCount, count)

	// drop replica
	if dropReplicaId != -1 {
		if err := o.removeReplica(&defn, watcherMap, *numReplica, 1, numPartition, dropReplicaId, (map[string]interface{})(nil)); err != nil {
			return fmt.Errorf("Fail to alter index: %v", err)
		}
		return nil
	}

	// Alter replica_count.   Check if we already have the desired number of replica
	if int(curCount) == count {
		o.cancelPrepareIndexRequest(defn.DefnId, watcherMap)
		return fmt.Errorf("Index already has %v number of replica.", curCount)
	}

	if int(curCount) < count {
		// add replica
		// TODO Elixir: Check for ingress lockdown for serverless.
		if err := o.addReplica(&defn, watcherMap, *numReplica, count-int(curCount), plan); err != nil {
			return fmt.Errorf("Fail to alter index: %v", err)
		}

	} else {
		// remove replica
		if err := o.removeReplica(&defn, watcherMap, *numReplica, int(curCount)-count, numPartition, dropReplicaId, (map[string]interface{})(nil)); err != nil {
			return fmt.Errorf("Fail to alter index: %v", err)
		}
	}

	return nil
}

//
// This function adds replica count of an index.
//
func (o *MetadataProvider) addReplica(idxDefn *c.IndexDefn, watcherMap map[c.IndexerId]int, numReplica c.Counter,
	increment int, plan map[string]interface{}) error {

	// Check for any create or drop token
	for indexerId, _ := range watcherMap {
		exist, err := o.SendCheckTokenRequest(indexerId, idxDefn.DefnId, DROP_INDEX_TOKEN)
		if err != nil {
			o.cancelPrepareIndexRequest(idxDefn.DefnId, watcherMap)
			logging.Errorf("Fail to alter index: %v", err)
			return err
		}
		if exist {
			o.cancelPrepareIndexRequest(idxDefn.DefnId, watcherMap)
			err := fmt.Errorf("Cannot alter index while the index is in the process of being dropped.")
			logging.Errorf("Fail to alter index: %v", err)
			return err
		}
	}

	//
	// Plan Phase.
	// The planner will use nodes that metadta provider sees for planning.  All inactive_failed, inactive_new and unhealthy
	// nodes will be excluded from planning.    If the user provides a specific node list, those nodes will be used.
	//
	_, definitions, err := o.replicaRepair(idxDefn, numReplica, increment, plan, watcherMap)
	if err != nil {
		o.cancelPrepareIndexRequest(idxDefn.DefnId, watcherMap)
		return err
	}

	//
	// Commit Phase.  Metadata Provider will send a commit request to at least one indexer.  If at least one indexer
	// responds with success, then it means there won't be another concurrent create/alter index request.    Even though
	// the metadata provider may not have full quorum, this create/alter index request can roll forward.
	//
	// The first indexer that responds with success will create a token so that it can roll forward even if this
	// metadata provider has died.  Other indexer will observe the token and proceed with the request.
	//
	requestId, err := c.NewIndexInstId()
	if err != nil {
		o.cancelPrepareIndexRequest(idxDefn.DefnId, watcherMap)
		return err
	}
	err = o.makeCommitIndexRequest(ADD_REPLICA, idxDefn, uint64(requestId), definitions, watcherMap, false)
	if err != nil {
		logging.Errorf("Fail to alter index: %v", err)
		return err
	}

	logging.Infof("Commit add replica on index %v with requestId %v", idxDefn.DefnId, requestId)

	return nil
}

//
// This function removes replica count of an index.
//
func (o *MetadataProvider) removeReplica(idxDefn *c.IndexDefn, watcherMap map[c.IndexerId]int, numReplica c.Counter, decrement int,
	numPartition int, dropReplicaId int, plan map[string]interface{}) error {

	//
	// Plan Phase.
	// The planner will use nodes that metadta provider sees for planning.  All inactive_failed, inactive_new and unhealthy
	// nodes will be excluded from planning.    If the user provides a specific node list, those nodes will be used.
	//
	instIds, replicaIds, err := o.replicaDrop(idxDefn, numReplica, decrement, numPartition, dropReplicaId, plan, watcherMap)
	if err != nil {
		o.cancelPrepareIndexRequest(idxDefn.DefnId, watcherMap)
		logging.Errorf("Fail to drop index replica: %v", err)
		return err
	}

	definitions := make([]c.IndexDefn, 0, len(instIds))
	for i, instId := range instIds {
		temp := *idxDefn
		temp.InstId = instId
		temp.ReplicaId = replicaIds[i]
		temp.NumReplica2 = numReplica
		temp.NumReplica2.Decrement(uint32(decrement))
		definitions = append(definitions, temp)
	}

	if len(definitions) == 0 {
		temp := *idxDefn
		temp.InstId = c.IndexInstId(0)
		temp.ReplicaId = int(math.MaxInt64)
		temp.NumReplica2 = numReplica
		temp.NumReplica2.Decrement(uint32(decrement))
		definitions = append(definitions, temp)
	}

	definitionsMap := make(map[c.IndexerId][]c.IndexDefn)
	for indexerId, _ := range watcherMap {
		definitionsMap[indexerId] = definitions
	}

	//
	// Commit Phase.  Metadata Provider will send a commit request to at least one indexer.  If at least one indexer
	// responds with success, then it means there won't be another concurrent create/alter index request.    Even though
	// the metadata provider may not have full quorum, this create/alter index request can roll forward.
	//
	// The first indexer that responds with success will create a token so that it can roll forward even if this
	// metadata provider has died.  Other indexer will observe the token and proceed with the request.
	//
	err = o.makeCommitIndexRequest(DROP_REPLICA, idxDefn, 0, definitionsMap, watcherMap, false)
	if err != nil {
		logging.Errorf("Fail to alter index: %v", err)
		return err
	}

	logging.Infof("Commit drop replica on index %v replicaIds %v", idxDefn.DefnId, replicaIds)

	return nil
}

func (o *MetadataProvider) Close() {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	logging.Infof("MetadataProvider is terminated. Cleaning up ...")

	for _, watcher := range o.watchers {
		watcher.close()
	}

	for indexerId, killch := range o.pendings {
		logging.Infof("MetadataProvider.Close(): removing pending indexerId %v", indexerId)
		delete(o.pendings, indexerId)

		// notify retryHelper to terminate.  This is for
		// watcher that is still waiting to complete
		// handshake with indexer.
		killch <- true
	}
}

//
// Since this function holds the lock, it ensure that
// neither WatchMetadata or UnwatchMetadata is being called.
// It also ensure safety of calling CheckIndexerStatusNoLock.
//
func (o *MetadataProvider) AllWatchersAlive() bool {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	// This only check watchers are running and being responsive (connected).
	// See more comment on allWatchersRunningNoLock()
	return o.AllWatchersAliveNoLock()
}

//
// Find out if a watcher is alive
//
func (o *MetadataProvider) IsWatcherAlive(nodeUUID string) bool {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	for _, watcher := range o.watchers {
		if nodeUUID == watcher.getNodeUUID() {
			return watcher.isAliveNoLock()
		}
	}

	return false
}

//
// The caller of this function must hold lock to ensure
// mutual exclusiveness.  The lock is used to prevent
// concurrent WatchMetadata/UnwatchMetadata being called,
// as well as to protect CheckIndexerStatusNoLock.
//
func (o *MetadataProvider) AllWatchersAliveNoLock() bool {

	if !o.allWatchersRunningNoLock() {
		return false
	}

	if len(o.pendings) != 0 {
		return false
	}

	statuses := o.CheckIndexerStatusNoLock()
	for _, status := range statuses {
		if !status.Connected {
			return false
		}
	}

	return true
}

//
// Are all watchers running?   If numExpctedWatcher does
// not match numWatcher, it could mean cluster is under
// topology change or current process is under bootstrap.
//
func (o *MetadataProvider) allWatchersRunningNoLock() bool {

	// This only check watchers have started successfully.
	// The watcher may not be connected (alive).
	// numExpectedWatcher = active node (known ports, unknown ports, unhealthy)
	// This does not include failed over node or new node (not yet rebalanced in).
	expected := atomic.LoadInt32(&o.numExpectedWatcher)
	actual := atomic.LoadInt32(&o.numWatcher)

	return expected == actual
}

//
// Get number of watchers
//
func (o *MetadataProvider) getNumWatchers() int32 {
	return atomic.LoadInt32(&o.numWatcher)
}

//
// Get the storage mode
//
func (o *MetadataProvider) GetStorageMode() c.StorageMode {

	o.mutex.Lock()
	defer o.mutex.Unlock()

	storageMode := c.StorageMode(c.NOT_SET)
	initialized := false

	for _, watcher := range o.watchers {

		if !initialized {
			storageMode = watcher.getStorageMode()
			initialized = true
			continue
		}

		if storageMode != watcher.getStorageMode() {
			return c.NOT_SET
		}
	}

	return storageMode
}

//
// Get the Indexer Version
//
func (o *MetadataProvider) GetIndexerVersion() uint64 {

	latestVersion := atomic.LoadUint64(&o.indexerVersion)
	if latestVersion < c.INDEXER_CUR_VERSION {
		return latestVersion
	}

	return c.INDEXER_CUR_VERSION
}

//
// Get the Cluster Version
//
func (o *MetadataProvider) GetClusterVersion() uint64 {

	clusterVersion := atomic.LoadUint64(&o.clusterVersion)
	if clusterVersion < c.INDEXER_CUR_VERSION {
		return clusterVersion
	}

	return c.INDEXER_CUR_VERSION
}

//
// Refresh the indexer version.  This will look at both
// metakv and indexers to figure out the latest version.
// This function still be 0 if (1) there are failed nodes and,
// (2) during upgrade to 5.0.
//
func (o *MetadataProvider) RefreshIndexerVersion() uint64 {

	// Find the version from metakv.  If token not found or error, fromMetakv is 0.
	fromMetakv, metakvErr := mc.GetIndexerVersionToken()

	// Any failed node?
	numFailedNode := atomic.LoadInt32(&o.numFailedNode)

	// Any unhealith node?
	numUnhealthyNode := atomic.LoadInt32(&o.numUnhealthyNode)

	// Any add node?
	numAddNode := atomic.LoadInt32(&o.numAddNode)

	// Find the version from active watchers.  This value is non-zero if
	// metadata provider has connected to all watchers and there are no
	// failed nodes and unhealthy nodes in the cluster.  Note that some
	// watchers could be disconnected when this method is called, but metadata provider
	// would have gotten the indexer version during initialization.
	fromWatcher := uint64(math.MaxUint64)
	clusterVersion := uint64(math.MaxUint64)
	func() {
		o.mutex.RLock()
		defer o.mutex.RUnlock()

		if o.allWatchersRunningNoLock() && numFailedNode == 0 && numUnhealthyNode == 0 && numAddNode == 0 {
			for _, watcher := range o.watchers {
				logging.Debugf("Watcher Version %v from %v", watcher.getIndexerVersion(), watcher.getNodeAddr())
				if watcher.getIndexerVersion() < fromWatcher {
					fromWatcher = watcher.getIndexerVersion()
				}
				if watcher.getClusterVersion() < clusterVersion {
					clusterVersion = watcher.getClusterVersion()
				}
			}
		} else {
			fromWatcher = 0
			clusterVersion = 0
		}

		logging.Verbosef("Indexer Version from metakv %v. Indexer Version from watchers %v.  Current version %v.",
			fromMetakv, fromWatcher, atomic.LoadUint64(&o.indexerVersion))
		logging.Verbosef("Num Watcher %v. Expected Watcher %v. Failed Node %v. Unhealthy Node %v.  Add Node %v. Cluster version %v.",
			atomic.LoadInt32(&o.numWatcher), atomic.LoadInt32(&o.numExpectedWatcher), numFailedNode, numUnhealthyNode, numAddNode,
			clusterVersion)
	}()

	latestVersion := atomic.LoadUint64(&o.indexerVersion)

	// If metakv has a higher version, it means that some other nodes have seen indexers converged to a higher
	// version, so this is the latest version.
	if fromMetakv > latestVersion && metakvErr == nil {
		latestVersion = fromMetakv
	}

	// If watchers have a higher version, then it means that this node has seen all indexers converged to a higher
	// version, so this is the latest version.
	if fromWatcher > latestVersion {
		latestVersion = fromWatcher
	}

	// make sure that the latest version is not higher than the software version for the current process
	if latestVersion > c.INDEXER_CUR_VERSION {
		latestVersion = c.INDEXER_CUR_VERSION
	}

	// update metakv
	if latestVersion > fromMetakv {
		if err := mc.PostIndexerVersionToken(latestVersion); err != nil {
			logging.Errorf("MetadataProvider: fail to post indexer version. Error = %s", err)
		} else {
			logging.Infof("MetadataProvider: Posting indexer version to metakv. Version=%v", latestVersion)
		}
	}

	// update the latest version
	if latestVersion > atomic.LoadUint64(&o.indexerVersion) {
		logging.Infof("MetadataProvider: Updating indexer version to %v", latestVersion)
		atomic.StoreUint64(&o.indexerVersion, latestVersion)
	}

	// update cluster version
	if clusterVersion > atomic.LoadUint64(&o.clusterVersion) {
		logging.Infof("MetadataProvider: Updating cluster version to %v", clusterVersion)
		atomic.StoreUint64(&o.clusterVersion, clusterVersion)
	}

	return latestVersion
}

///////////////////////////////////////////////////////
// private function : MetadataProvider
///////////////////////////////////////////////////////

// A watcher is active only when it is ready to accept request.  This
// means initial synchronization phase is done with the indexer.  This does not
// mean the watcher is connected to the indexer at the moment when this
// call is made.  This is not a network liveness check on the indexer.
// But this can check if UnwatchMetadata has been called on this indexer.
func (o *MetadataProvider) isActiveWatcherNoLock(indexerId c.IndexerId) bool {

	for _, watcher := range o.watchers {
		if watcher.getIndexerId() == indexerId {
			return true
		}
	}

	return false
}

func (o *MetadataProvider) retryHelper(watcher *watcher, readych chan bool, indexAdminPort string,
	tempIndexerId c.IndexerId, killch chan bool, callback watcherCallback) {

	if readych != nil {
		// if watcher is not ready, let's wait.
		if _, killed := watcher.waitForReady(readych, 0, killch); killed {
			watcher.cleanupIndices(o.repo)
			return
		}
	}

	// get the indexerId
	if _, killed := watcher.notifyReady(indexAdminPort, -1, killch); killed {
		watcher.close()
		watcher.cleanupIndices(o.repo)
		return
	}

	// add the watcher
	killed := func() bool {
		o.mutex.Lock()
		defer o.mutex.Unlock()

		// make sure watcher is still active. Unwatch metadata could have
		// been called just after watcher.notifyReady has finished.
		if _, ok := o.pendings[tempIndexerId]; !ok {
			logging.Infof("MetadataProvider.retryHelper(): Removing pending watcher on Close() or UnwatchMetadata().  IndexerId %v", tempIndexerId)
			watcher.close()
			watcher.cleanupIndices(o.repo)
			return true
		}

		o.addWatcherNoLock(watcher, tempIndexerId)
		return false
	}()

	if !killed {
		logging.Infof("WatchMetadata(): Successfully connected to indexer at %v after retry.", indexAdminPort)

		indexerId := watcher.getIndexerId()
		if callback != nil {
			callback(indexAdminPort, indexerId, tempIndexerId)
		}
	}
}

func (o *MetadataProvider) addWatcherNoLock(watcher *watcher, tempIndexerId c.IndexerId) {

	delete(o.pendings, tempIndexerId)

	indexerId := watcher.getIndexerId()
	oldWatcher, ok := o.watchers[indexerId]
	if ok {
		// there is an old watcher with an matching indexerId.  Close it ...
		oldWatcher.close()
	}
	o.watchers[indexerId] = watcher

	// increment version whenever a watcher is registered
	o.repo.incrementVersion()

	// remember the number of watcher
	atomic.StoreInt32(&o.numWatcher, int32(len(o.watchers)))
}

func (o *MetadataProvider) startWatcher(addr string) (*watcher, chan bool) {

	s := newWatcher(o, addr)
	readych := make(chan bool)

	// TODO: call Close() to cleanup the state upon retry by the MetadataProvider server
	go protocol.RunWatcherServerWithRequest2(
		s.leaderAddr,
		s,
		s,
		s.factory,
		s.killch,
		readych,
		s.alivech,
		s.pingch,
		s.ClientAuth,
	)

	return s, readych
}

//
// This function returns the index regardless of its state or well-formed (all partitions).
// This function will not return the index if it does not have any valid instance or partition.
// In other words, this function will return the index if it has at least one non-DELETED
// instance with Active RState.
//
func (o *MetadataProvider) FindIndexIgnoreStatus(id c.IndexDefnId) *IndexMetadata {

	indices, _ := o.repo.listAllDefn()
	if meta, ok := indices[id]; ok {
		return meta
	}

	return nil
}

//
// This function returns the index regardless of its state or well-formed (all partitions).
// This function will not return the index if it does not have any valid instance or partition.
// In other words, this function will return the index if it has at least one non-DELETED
// instance with Active RState.
//
func (o *MetadataProvider) FindIndexInstanceIgnoreStatus(id c.IndexDefnId, instId c.IndexInstId) *IndexMetadata {

	indices, _ := o.repo.listAllDefn()
	if meta, ok := indices[id]; ok {
		for _, inst := range meta.Instances {
			if inst.InstId == instId {
				return meta
			}
		}

		for _, inst := range meta.InstsInRebalance {
			if inst.InstId == instId {
				return meta
			}
		}
	}

	return nil
}

func (o *MetadataProvider) getAllWatchers() []*watcher {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	result := make([]*watcher, len(o.watchers))
	count := 0
	for _, watcher := range o.watchers {
		result[count] = watcher
		count++
	}
	return result
}

func (o *MetadataProvider) getAllAvailWatchers() []*watcher {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	result := make([]*watcher, 0, len(o.watchers))
	for _, watcher := range o.watchers {
		if watcher.serviceMap.ExcludeNode != "in" &&
			watcher.serviceMap.ExcludeNode != "inout" {
			result = append(result, watcher)
		}
	}
	return result
}

func (o *MetadataProvider) getAllWatcherNodeAddrNoLock() []string {

	result := make([]string, 0, len(o.watchers))
	for _, watcher := range o.watchers {
		result = append(result, watcher.getNodeAddr())
	}
	return result
}

func (o *MetadataProvider) findNextAvailWatcher(excludes []*watcher, checkServerGroup bool) (*watcher, int) {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	var minCount = math.MaxUint16
	var nextWatcher *watcher = nil

	for _, watcher := range o.watchers {
		found := false
		for _, exclude := range excludes {
			if watcher == exclude {
				found = true
			} else if checkServerGroup && watcher.getServerGroup() == exclude.getServerGroup() {
				found = true
			}
		}

		if watcher.serviceMap.ExcludeNode == "in" ||
			watcher.serviceMap.ExcludeNode == "inout" {
			found = true
		}

		if !found {
			count := o.repo.getValidDefnCount(watcher.getIndexerId())
			if count <= minCount {
				minCount = count
				nextWatcher = watcher
			}
		}
	}

	return nextWatcher, len(o.watchers)
}

func (o *MetadataProvider) findAliveWatchersByDefnIdIgnoreStatus(defnId c.IndexDefnId) ([]*watcher, error, bool) {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	if !o.AllWatchersAliveNoLock() {
		return nil, errors.New("Some indexer nodes are not reachable.  Cannot process request."), false
	}

	var result []*watcher
	for _, watcher := range o.watchers {
		if o.repo.hasDefnIgnoreStatus(watcher.getIndexerId(), defnId) {
			result = append(result, watcher)
		}
	}

	if len(result) != 0 {
		return result, nil, true
	}

	return nil, errors.New("Cannot find indexer node with index."), true
}

func (o *MetadataProvider) findWatchersByDefnIdIgnoreStatus(defnId c.IndexDefnId) ([]*watcher, error) {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	var result []*watcher
	for _, watcher := range o.watchers {
		if o.repo.hasDefnIgnoreStatus(watcher.getIndexerId(), defnId) {
			result = append(result, watcher)
		}
	}

	return result, nil
}

func (o *MetadataProvider) findWatcherByIndexerId(id c.IndexerId) (*watcher, error) {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	for indexerId, watcher := range o.watchers {
		if indexerId == id {
			return watcher, nil
		}
	}

	return nil, errors.New(fmt.Sprintf("Cannot find watcher with IndexerId %v", id))
}

func (o *MetadataProvider) findAliveWatcherByIndexerId(id c.IndexerId) (*watcher, error) {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	for indexerId, watcher := range o.watchers {
		if indexerId == id && watcher.isAliveNoLock() {
			return watcher, nil
		}
	}

	return nil, errors.New(fmt.Sprintf("Cannot find alive watcher with IndexerId %v", id))
}

func (o *MetadataProvider) findWatcherByNodeUUID(nodeUUID string) (*watcher, error) {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	for _, watcher := range o.watchers {
		if nodeUUID == watcher.getNodeUUID() {
			return watcher, nil
		}
	}

	return nil, errors.New(fmt.Sprintf("Cannot find watcher with nodeUUID %v", nodeUUID))
}

func (o *MetadataProvider) findWatcherByNodeAddr(nodeAddr string) *watcher {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	for _, watcher := range o.watchers {
		watcherNodeAddr := strings.ToLower(watcher.getNodeAddr())
		encryptedwatcherNodeAddr, _ := security.EncryptPortInAddr(watcherNodeAddr)
		if !security.EncryptionEnabled() {
			if watcherNodeAddr == strings.ToLower(nodeAddr) {
				return watcher
			}
		} else if security.DisableNonSSLPort() {
			if encryptedwatcherNodeAddr == strings.ToLower(nodeAddr) {
				return watcher
			}
		} else {
			if watcherNodeAddr == strings.ToLower(nodeAddr) ||
				encryptedwatcherNodeAddr == strings.ToLower(nodeAddr) {
				return watcher
			}
		}
	}

	return nil
}

//
// This function returns true if all partitons belong active watcher (watcher has
// not been unwatched).
//
func (o *MetadataProvider) allPartitionsFromActiveIndexerNoLock(inst *InstanceDefn) bool {

	for _, indexerId := range inst.IndexerId {
		if !o.isActiveWatcherNoLock(indexerId) {
			return false
		}
	}

	return true
}

//
// This function returns true as long as there is a valid index instance
// belong to an active indexer/watcher (watcher has not been unwatched).
//
func (o *MetadataProvider) isValidIndexFromActiveIndexer(meta *IndexMetadata) bool {
	o.mutex.RLock()
	defer o.mutex.RUnlock()

	return o.isValidIndexFromActiveIndexerNoLock(meta)
}

// Return index metadata if
// 1) Index defn has valid status
// 2) At least one index instance has valid status
// 3) Partitions in the valid index instance has not been unwatched
func (o *MetadataProvider) isValidIndexFromActiveIndexerNoLock(meta *IndexMetadata) bool {

	if !isValidIndex(meta) {
		return false
	}

	for _, inst := range meta.Instances {
		if isValidIndexInst(inst) && o.allPartitionsFromActiveIndexerNoLock(inst) {
			return true
		}
	}

	for _, inst := range meta.InstsInRebalance {
		if isValidIndexInst(inst) && o.allPartitionsFromActiveIndexerNoLock(inst) {
			return true
		}
	}

	return false
}

//
// This function notifies metadata provider and its caller that new version of
// metadata is available.
//
func (o *MetadataProvider) needRefresh() {

	if o.metaNotifyCh != nil {
		select {
		case o.metaNotifyCh <- true:
		default:
		}
	}
}

//
// This function notifies metadata provider and its caller that new version of
// metadata is available.
//
func (o *MetadataProvider) refreshStats(stats map[c.IndexInstId]map[c.PartitionId]c.Statistics) {

	if o.statsNotifyCh != nil {
		select {
		case o.statsNotifyCh <- stats:
		default:
		}
	}
}

//
// Refresh cluster info. Check for failed and unhealthy node.
// This function depends on ns-server for getting cluster info.
// Since cluster info is eventual consistent, this does not
// necessarily reflect immediate cluster status.
//
func (o *MetadataProvider) checkClusterHealth() (bool, error) {

	cinfo, err := c.FetchNewClusterInfoCache(o.clusterUrl, c.DEFAULT_POOL, "checkClusterHealth")
	if err != nil {
		return false, err
	}

	activeNodes := len(cinfo.GetActiveIndexerNodes()) // all active indexer node (known ports + unknown ports + unhealthy node)
	failedNodes := len(cinfo.GetFailedIndexerNodes()) // failover indexer node
	newNodes := len(cinfo.GetNewIndexerNodes())       // new indexer node
	unhealthyNodes := 0                               // active, unhealthy indexer node with known ports

	for _, node := range cinfo.GetNodeIdsByServiceType("indexAdmin") {
		status, err := cinfo.GetNodeStatus(node)
		if err != nil {
			return false, err
		}

		if status != "healthy" {
			unhealthyNodes++
		}
	}

	o.SetClusterStatus(activeNodes, failedNodes, unhealthyNodes, newNodes)

	return o.isClusterHealthy(), nil
}

//
// 1) Check cluster health (see checkClusterHealth)
// 2) Check if number of watchers matching number of active nodes
// 3) Check if all watchers are ready to receive requests
//    - connected to indexer
//    - not in the middle of synchronization with indexer
//
func (o *MetadataProvider) checkProviderHealthNoLock() (bool, error) {

	healthy, err := o.checkClusterHealth()
	if err != nil {
		return false, err
	}

	if !healthy {
		return false, nil
	}

	// Check if all indexers are available
	// This will check if there is network partition.
	return o.AllWatchersAliveNoLock(), nil
}

//
// This function checks if cluster is healthy
// 1) no failed node
// 2) no unhealthy node
//
func (o *MetadataProvider) isClusterHealthy() bool {
	return atomic.LoadInt32(&o.numFailedNode) == 0 &&
		atomic.LoadInt32(&o.numUnhealthyNode) == 0
}

func (o *MetadataProvider) getUsageThresholdForPlanner() *planner.UsageThreshold {

	usageThreshold := &planner.UsageThreshold{
		MemHighThreshold: o.settings.MemHighThreshold(),
		MemLowThreshold:  o.settings.MemHighThreshold(),
	}
	return usageThreshold
}

//
// This function returns true as long as there is a
// valid index instance for this index definition.
//
func isValidIndex(meta *IndexMetadata) bool {

	if meta.Definition == nil {
		return false
	}

	if meta.State == c.INDEX_STATE_NIL ||
		meta.State == c.INDEX_STATE_CREATED ||
		meta.State == c.INDEX_STATE_DELETED ||
		meta.State == c.INDEX_STATE_ERROR {
		return false
	}

	for _, inst := range meta.Instances {
		if isValidIndexInst(inst) {
			return true
		}
	}

	for _, inst := range meta.InstsInRebalance {
		if isValidIndexInst(inst) {
			return true
		}
	}

	return false
}

//
// This function returns true if it is a valid index instance.
//
func isValidIndexInst(inst *InstanceDefn) bool {

	// RState for InstanceDefn is always ACTIVE -- so no need to check
	return inst.State != c.INDEX_STATE_NIL && inst.State != c.INDEX_STATE_CREATED &&
		inst.State != c.INDEX_STATE_DELETED && inst.State != c.INDEX_STATE_ERROR
}

//
// This function return true if the index instance has all the partitions
//
func isWellFormed(defn *c.IndexDefn, inst *InstanceDefn) bool {

	if !c.IsPartitioned(defn.PartitionScheme) {
		for partnId, _ := range inst.IndexerId {
			if partnId != c.NON_PARTITION_ID {
				return false
			}
		}
	}

	return len(inst.IndexerId) == int(inst.NumPartitions)
}

///////////////////////////////////////////////////////
// private function : metadataRepo
///////////////////////////////////////////////////////

func newMetadataRepo(provider *MetadataProvider) *metadataRepo {

	return &metadataRepo{
		definitions: make(map[c.IndexDefnId]*c.IndexDefn),
		instances:   make(map[c.IndexDefnId]map[c.IndexInstId]map[c.PartitionId]map[uint64]*mc.IndexInstDistribution),
		indices:     make(map[c.IndexDefnId]*IndexMetadata),
		topology:    make(map[c.IndexerId]map[c.IndexDefnId]bool),
		version:     uint64(0),
		provider:    provider,
		notifiers:   make(map[c.IndexDefnId]*event),
	}
}

func (r *metadataRepo) RLock() {
	r.mutex.RLock()
}

func (r *metadataRepo) RUnlock() {
	r.mutex.RUnlock()
}

func (r *metadataRepo) listAllDefn() (map[c.IndexDefnId]*IndexMetadata, uint64) {

	r.mutex.RLock()
	defer r.mutex.RUnlock()

	result := make(map[c.IndexDefnId]*IndexMetadata)
	for id, meta := range r.indices {
		if len(meta.Instances) != 0 || len(meta.InstsInRebalance) != 0 {

			insts := make([]*InstanceDefn, len(meta.Instances))
			copy(insts, meta.Instances)

			instsInRebalance := make([]*InstanceDefn, len(meta.InstsInRebalance))
			copy(instsInRebalance, meta.InstsInRebalance)

			defn := *meta.Definition
			tmp := &IndexMetadata{
				Definition:       &defn,
				State:            meta.State,
				Error:            meta.Error,
				Instances:        insts,
				InstsInRebalance: instsInRebalance,
				Stats:            meta.Stats,
			}

			result[id] = tmp
		}
	}

	return result, r.getVersion()
}

func (r *metadataRepo) listDefnWithValidInst() (map[c.IndexDefnId]*IndexMetadata, uint64) {

	r.mutex.RLock()
	defer r.mutex.RUnlock()

	return r.listDefnWithValidInstNoLock()
}

func (r *metadataRepo) listDefnWithValidInstNoLock() (map[c.IndexDefnId]*IndexMetadata, uint64) {

	result := make(map[c.IndexDefnId]*IndexMetadata)
	for id, meta := range r.indices {
		if isValidIndex(meta) {
			var insts []*InstanceDefn
			for _, inst := range meta.Instances {
				if isValidIndexInst(inst) {
					insts = append(insts, inst)
				}
			}

			var instsInRebalance []*InstanceDefn
			for _, inst := range meta.InstsInRebalance {
				if isValidIndexInst(inst) {
					instsInRebalance = append(instsInRebalance, inst)
				}
			}

			defn := *meta.Definition
			tmp := &IndexMetadata{
				Definition:       &defn,
				State:            meta.State,
				Error:            meta.Error,
				Instances:        insts,
				InstsInRebalance: instsInRebalance,
				Stats:            meta.Stats,
			}

			result[id] = tmp
		}
	}

	return result, r.getVersion()
}

func (r *metadataRepo) addDefn(defn *c.IndexDefn) {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	logging.Debugf("metadataRepo.addDefn %v", defn.DefnId)

	// A definition can have mutliple physical copies.  If
	// we have seen a copy already, then it is not necessary
	// to add another copy again.
	if cached, ok := r.definitions[defn.DefnId]; !ok {
		r.definitions[defn.DefnId] = defn
		r.indices[defn.DefnId] = r.makeIndexMetadata(defn)

		r.updateIndexMetadataNoLock(defn.DefnId)
		r.incrementVersion()
	} else {
		if result, changed, err := cached.NumReplica2.MergeWith(defn.NumReplica2); err == nil {
			if changed {
				cached.NumReplica2 = result
				r.incrementVersion()
			}
		}
	}
}

//
// This function returns the an index instance which is an ensemble of different index partitions.
// Each index partition has the highest version with active RState, and each one can be residing on
// different indexer node.  This function will not check if the index instance has all the partitions.
//
func (r *metadataRepo) findLatestActiveIndexInstNoLock(defnId c.IndexDefnId) []*mc.IndexInstDistribution {

	var result []*mc.IndexInstDistribution

	instsByInstId := r.instances[defnId]
	for _, instsByPartitionId := range instsByInstId {
		var latest *mc.IndexInstDistribution

		for partId, instsByVersion := range instsByPartitionId {

			var chosen *mc.IndexInstDistribution
			var chosenVersion uint64
			for version, inst := range instsByVersion {

				// Do not filter out CREATED index, even though it is a "transient"
				// index state.  A created index can be promoted to a READY index by
				// indexer upon bootstrap.  An index in CREATED state will be filtered out
				// in ListIndex() for scanning.
				// For DELETED index, it needs to be filtered out since/we don't want it to
				// "pollute" IndexMetadata.State.
				//
				if c.IndexState(inst.State) != c.INDEX_STATE_NIL &&
					//c.IndexState(inst.State) != c.INDEX_STATE_CREATED &&
					c.IndexState(inst.State) != c.INDEX_STATE_DELETED &&
					c.IndexState(inst.State) != c.INDEX_STATE_ERROR &&
					inst.RState == uint32(c.REBAL_ACTIVE) { // valid

					if chosen == nil || version > chosenVersion { // latest
						chosen = inst
						chosenVersion = version
					}
				}
			}

			if chosen != nil {
				latest = r.mergeSingleIndexPartition(latest, chosen, partId)
			}
		}

		if latest != nil {
			result = append(result, latest)
		}
	}

	logging.Debugf("defnId %v has %v recent and active instances", defnId, len(result))
	return result
}

//
// This function returns the an index instance which is an ensemble of different index partitions.
// Each index partition has the highest version with the specific RState. Each partition can be residing on
// different indexer node.   This function will not check if all the indexes have all the partitions.
//
func (r *metadataRepo) findIndexInstNoLock(defnId c.IndexDefnId, instId c.IndexInstId, activeInst *InstanceDefn, rstate uint32) *mc.IndexInstDistribution {

	var result *mc.IndexInstDistribution

	if instsByInstId := r.instances[defnId]; len(instsByInstId) > 0 {

		instsByPartitionId := instsByInstId[instId]

		for partId, instsByVersion := range instsByPartitionId {

			var chosen *mc.IndexInstDistribution
			var chosenVersion uint64
			for partnVersion, inst := range instsByVersion {

				if c.IndexState(inst.State) != c.INDEX_STATE_NIL &&
					c.IndexState(inst.State) != c.INDEX_STATE_CREATED &&
					c.IndexState(inst.State) != c.INDEX_STATE_DELETED &&
					c.IndexState(inst.State) != c.INDEX_STATE_ERROR &&
					inst.RState == rstate {

					var activeVersion uint64
					if activeInst != nil && activeInst.Versions[c.PartitionId(partId)] != 0 {
						activeVersion = activeInst.Versions[c.PartitionId(partId)]
					}

					if chosen == nil || (partnVersion > activeVersion && partnVersion > chosenVersion) {
						chosen = inst
						chosenVersion = partnVersion
					}
				}
			}

			if chosen != nil {
				result = r.mergeSingleIndexPartition(result, chosen, partId)
			}
		}
	}

	return result
}

//
// This function return if an indexer contains at least one partition of the given index instance.
//
func (r *metadataRepo) hasIndexerContainingPartition(indexerId c.IndexerId, inst *InstanceDefn) bool {

	if inst != nil {
		for _, id := range inst.IndexerId {
			if indexerId == id {
				return true
			}
		}
	}

	return false
}

//
// This function merges multiple index instance per partition.
//
func (r *metadataRepo) mergeSingleIndexPartition(to *mc.IndexInstDistribution, from *mc.IndexInstDistribution,
	partId c.PartitionId) *mc.IndexInstDistribution {

	// This is just for safety check.  REBAL_MERGED index should have DELETED index state.
	if from.RState == uint32(c.REBAL_MERGED) {
		return to
	}

	if to == nil {
		temp := *from
		to = &temp
		to.Partitions = nil

	} else {

		// Find the lowest state among the partitions.  For example, if one partition is active but another is intiial,
		// the index state remains initial.
		if from.State < to.State {
			to.State = from.State
		}
		if from.Error != to.Error {
			to.Error += " " + from.Error
		}

		if from.RState == uint32(c.REBAL_PENDING) {
			to.RState = uint32(c.REBAL_PENDING)
		}
	}

	// merge partition
	for _, partition := range from.Partitions {
		if partition.PartId == uint64(partId) {
			to.Partitions = append(to.Partitions, partition)
			break
		}
	}

	return to
}

// Only Consider instance with Active RState.  This function will not check if the index
// is valid.
func (r *metadataRepo) hasDefnIgnoreStatus(indexerId c.IndexerId, defnId c.IndexDefnId) bool {

	r.mutex.RLock()
	defer r.mutex.RUnlock()

	meta, ok := r.indices[defnId]
	if ok && meta != nil {
		for _, inst := range meta.Instances {
			if r.hasIndexerContainingPartition(indexerId, inst) {
				return true
			}
		}
	}

	return false
}

// Only Consider instance with Active RState and full partitions
func (r *metadataRepo) hasWellFormedInstMatchingStatusNoLock(defnId c.IndexDefnId, status []c.IndexState) bool {

	if meta, ok := r.indices[defnId]; ok && meta != nil && meta.Definition != nil && len(meta.Instances) != 0 {
		for _, s := range status {
			for _, inst := range meta.Instances {
				if inst.State == s && isWellFormed(meta.Definition, inst) {
					return true
				}
			}
		}
	}
	return false
}

// Only Consider instance with Active RState and full partitions
// all argument if true will ensure every instance for this defnId (usually replicas)
// matches one of the status from status argument.
// when all = false function retains the behaviour of hasWellFormedInstMatchingStatusNoLock that is
// at least one instance should match the allowed statuses.
func (r *metadataRepo) hasWellFormedAllInstMatchingStatusNoLock(defnId c.IndexDefnId, status []c.IndexState, all bool) bool {

	if meta, ok := r.indices[defnId]; ok && meta != nil && meta.Definition != nil && len(meta.Instances) != 0 {
		for _, inst := range meta.Instances {
			instMatches := false
			for _, s := range status {
				if inst.State == s && isWellFormed(meta.Definition, inst) {
					if !all {
						return true // at least one of the instance matches allowed status, no need to check further  - old behaviour
					}
					instMatches = true // this instance matches one of the allowed statuses, continue checking other instances - new behaviour
					break
				}
			}
			// this instance is not in any allowed status,
			// if all=false then continue with remaining instances - old behaviour;
			// if all = true && instMatches = false we are done here else continue checking remaining instances -- new behaviour.
			if all && instMatches == false {
				return false
			}
		}
		// if all = true and we reached here means all instances have matched some allowed status
		// if all was false and we reached here then no instance matched allowed status
		if all {
			return true
		}
	}
	return false
}

// Only Consider instance with Active RState
func (r *metadataRepo) getDefnErrorIgnoreStatusNoLock(defnId c.IndexDefnId) error {

	meta, ok := r.indices[defnId]
	if ok && meta != nil && len(meta.Instances) != 0 {
		var errStr string
		for _, inst := range meta.Instances {
			if len(inst.Error) != 0 {
				errStr += inst.Error + "\n"
			}
		}

		if len(errStr) != 0 {
			return errors.New(errStr)
		}
	}

	return nil
}

func (r *metadataRepo) getValidDefnCount(indexerId c.IndexerId) int {

	r.mutex.RLock()
	defer r.mutex.RUnlock()

	count := 0

	for _, meta := range r.indices {
		if isValidIndex(meta) {
			for _, inst := range meta.Instances {
				if isValidIndexInst(inst) && r.hasIndexerContainingPartition(indexerId, inst) {
					count++
					break
				}
			}
			for _, inst := range meta.InstsInRebalance {
				if isValidIndexInst(inst) && r.hasIndexerContainingPartition(indexerId, inst) {
					count++
					break
				}
			}
		}
	}

	return count
}

func (r *metadataRepo) removeInstForIndexerNoLock(indexerId c.IndexerId, bucket, scope, collection string) {

	isBucketScopeCollEmpty := func() bool {
		return (len(bucket) == 0 && len(scope) == 0 && len(collection) == 0)
	}

	checkDefn := func(defn *c.IndexDefn) bool {
		return (defn != nil && defn.Bucket == bucket && defn.Scope == scope && defn.Collection == collection)
	}

	for defnId, instsByDefnId := range r.instances {
		defn := r.definitions[defnId]

		// Do not process indexes belonging to a different topology
		if (isBucketScopeCollEmpty() || checkDefn(defn)) == false {
			continue
		}
		for instId, instsByInstId := range instsByDefnId {

			for partnId, instsByPartitionId := range instsByInstId {

				for version, instByVersion := range instsByPartitionId {

					instIndexerId := instByVersion.FindIndexerId()

					if instIndexerId == string(indexerId) {
						delete(r.instances[defnId][instId][partnId], version)
						logging.Debugf("remove index for indexerId : defnId %v instId %v indexerId %v", defnId, instId, instIndexerId)
					}
				}
				if len(r.instances[defnId][instId][partnId]) == 0 {
					delete(r.instances[defnId][instId], partnId)
				}
			}
			if len(r.instances[defnId][instId]) == 0 {
				delete(r.instances[defnId], instId)
			}
		}
		if len(r.instances[defnId]) == 0 {
			delete(r.instances, defnId)
		}
	}
}

// Removing an index with no index instance:
// 1) All the index instances have been deleted.
// 2) If indexer is partitioned away from metadata provider (unhealthy indexer), the correpsonding instance will be removed.
//    If all instances are removed, the defn will be removed.  The index will be materialized again when those indexers are
//    reconnected to metadata provider (through watchMetadata).
//   - If indexer is temporalily disconnected from metadata provider (e.g. indexer crash), index will not be removed.
//   - An indexer under heavy load (max out cpu) will exhibit symptoms of network partition
// 3) If indexer node has failed over or rebalanced out of the cluster, the corresponding instance will be removed.
//    If all instances are removed, the defn will be removed.
//
func (r *metadataRepo) cleanupOrphanDefnNoLock(indexerId c.IndexerId, bucket, scope, collection string) {

	deleteDefn := ([]c.IndexDefnId)(nil)

	isBucketScopeCollEmpty := func() bool {
		return (len(bucket) == 0 && len(scope) == 0 && len(collection) == 0)
	}

	checkDefn := func(defn *c.IndexDefn) bool {
		return (defn != nil && defn.Bucket == bucket && defn.Scope == scope && defn.Collection == collection)
	}

	for defnId, _ := range r.topology[indexerId] {
		if defn, ok := r.definitions[defnId]; ok {
			if isBucketScopeCollEmpty() || checkDefn(defn) {

				if len(r.instances[defnId]) == 0 {
					deleteDefn = append(deleteDefn, defnId)
				}
			}
		} else {
			logging.Verbosef("Find orphan index %v in topology but watcher has not received corresponding definition", defnId)
		}
	}

	for _, defnId := range deleteDefn {
		logging.Verbosef("removing orphan defn with no instance %v", defnId)
		delete(r.definitions, defnId)
		delete(r.instances, defnId)
		delete(r.indices, defnId)
		delete(r.topology[indexerId], defnId)

		// Notify any outstanding subscribers that are waiting for
		// a notification on this definition
		if event, ok := r.notifiers[defnId]; ok {
			delete(r.notifiers, defnId)
			close(event.notifyCh)
			logging.Infof("Notifying outstanding waiters due to orphan definition removal. DefnId: %v", defnId)
		}
	}

	if len(r.topology[indexerId]) == 0 {
		delete(r.topology, indexerId)
	}
}

func (r *metadataRepo) cleanupIndicesForIndexer(indexerId c.IndexerId) {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.removeInstForIndexerNoLock(indexerId, "", "", "")
	r.cleanupOrphanDefnNoLock(indexerId, "", "", "")

	for defnId, _ := range r.indices {
		logging.Debugf("update topology during cleanup: defn %v", defnId)
		r.updateIndexMetadataNoLock(defnId)
	}
}

func (r *metadataRepo) updateTopology(topology *mc.IndexTopology, indexerId c.IndexerId) {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	if len(indexerId) == 0 {
		indexerId = c.IndexerId(topology.FindIndexerId())
	}

	// IndexerId is known when
	// 1) Ater watcher has successfully re-connected to the indexer.  The indexerId
	//    is kept with watcher until UnwatchMetadata().    Therefore, even if watcher
	//    is re-connected (re-sync) with the indexer, watcher knows the indexerId during resync.
	// 2) IndexTopology (per bucket) is not empty.  Each index inst contains the indexerId.
	//
	// IndexerId is not known when
	// 1) When the watcher is first syncronized with the indexer (WatchMetadata) AND IndexTopology is emtpy.
	//    Even after sync is done (but service map is not yet refreshed), there is a small window that indexerId
	//    is not known when processing Commit message from indexer.   But there is no residual metadata to clean up
	//    during WatchMetadata (previous UnwatchMetadata haved removed metadata for same indexerId).
	//
	if len(indexerId) != 0 {
		r.removeInstForIndexerNoLock(indexerId, topology.Bucket, topology.Scope, topology.Collection)
	}

	logging.Debugf("new topology change from %v", indexerId)

	for _, defnRef := range topology.Definitions {
		defnId := c.IndexDefnId(defnRef.DefnId)

		if _, ok := r.topology[indexerId]; !ok {
			r.topology[indexerId] = make(map[c.IndexDefnId]bool)
		}

		r.topology[indexerId][defnId] = true

		for _, instRef := range defnRef.Instances {
			if _, ok := r.instances[defnId]; !ok {
				r.instances[defnId] = make(map[c.IndexInstId]map[c.PartitionId]map[uint64]*mc.IndexInstDistribution)
			}

			if _, ok := r.instances[defnId][c.IndexInstId(instRef.InstId)]; !ok {
				r.instances[defnId][c.IndexInstId(instRef.InstId)] = make(map[c.PartitionId]map[uint64]*mc.IndexInstDistribution)
			}

			for k, partnRef := range instRef.Partitions {
				// for backward compatiblity on non-partitioned index (pre-5.5.)
				if partnRef.PartId == 0 && partnRef.Version == 0 && instRef.Version != partnRef.Version {
					instRef.Partitions[k].Version = instRef.Version
					partnRef.Version = instRef.Version
				}

				if _, ok := r.instances[defnId][c.IndexInstId(instRef.InstId)][c.PartitionId(partnRef.PartId)]; !ok {
					r.instances[defnId][c.IndexInstId(instRef.InstId)][c.PartitionId(partnRef.PartId)] = make(map[uint64]*mc.IndexInstDistribution)
				}

				// r.Instances has all the index instances and partitions regardless of its state and version
				temp := instRef
				r.instances[defnId][c.IndexInstId(instRef.InstId)][c.PartitionId(partnRef.PartId)][partnRef.Version] = &temp
			}
		}

		logging.Debugf("update Topology: defn %v", defnId)
		r.updateIndexMetadataNoLock(defnId)
	}

	if len(indexerId) != 0 {
		r.cleanupOrphanDefnNoLock(indexerId, topology.Bucket, topology.Scope, topology.Collection)
	}

	r.incrementVersion()
}

func (r *metadataRepo) incrementVersion() {

	atomic.AddUint64(&r.version, 1)
}

func (r *metadataRepo) getVersion() uint64 {

	return atomic.LoadUint64(&r.version)
}

func (r *metadataRepo) unmarshallAndAddDefn(content []byte) error {

	defn, err := c.UnmarshallIndexDefn(content)
	if err != nil {
		return err
	}

	defn.SetCollectionDefaults()
	r.addDefn(defn)
	return nil
}

func (r *metadataRepo) unmarshallAndUpdateTopology(content []byte, indexerId c.IndexerId) error {

	topology, err := unmarshallIndexTopology(content)
	if err != nil {
		return err
	}

	// If indexer is old and query is new, scope/collection name will be empty
	// Populate it with defaults
	clusterVersion := r.provider.GetClusterVersion()
	if clusterVersion < c.INDEXER_70_VERSION {
		// In mixed mode, topology from old indexer nodes have empty scope/collection name.
		// Populate it with defaults
		if topology.Scope == "" {
			topology.Scope = c.DEFAULT_SCOPE
		}
		if topology.Collection == "" {
			topology.Collection = c.DEFAULT_COLLECTION
		}

		for i, _ := range topology.Definitions {
			if topology.Definitions[i].Scope == "" {
				topology.Definitions[i].Scope = c.DEFAULT_SCOPE
			}

			if topology.Definitions[i].Collection == "" {
				topology.Definitions[i].Collection = c.DEFAULT_COLLECTION
			}
		}
	}

	r.updateTopology(topology, indexerId)
	return nil
}

func (r *metadataRepo) makeIndexMetadata(defn *c.IndexDefn) *IndexMetadata {

	return &IndexMetadata{
		Definition:       defn,
		Instances:        nil,
		InstsInRebalance: nil,
		State:            c.INDEX_STATE_NIL,
		Error:            "",
		Stats:            map[string]interface{}{},
	}
}

//
// This materializes an IndexMetadata.  It can be one of the following after materialization:
// 1) A new index with no instance created yet (State=CREATED, len(instances) == 0).
// 2) A new index with one or more instances in CREATED state (State=CREATED, len(instances) != 0)
// 3) A index with one or more valid instances (State={READY|INITIAL|CATCHUP|ACTIVE}, len(instances) !=0)
// 4) A pending delete index with one or more remaining valid instances (State={READY|INITIAL|CATCHUP|ACTIVE}, len(instances) !=0)
// 5) A deleted index with no instances (State={CREATED}, len(instances) == 0)
//
// Under rebalance, indexer will make copy of instance under rebalance.  IndexMetadata will also contain copies under rebalance.
// In addition, those copies can be promoted to "active" instance if there is no correpsonding active instance.
//
func (r *metadataRepo) updateIndexMetadataNoLock(defnId c.IndexDefnId) {

	meta, ok := r.indices[defnId]
	if ok {
		r.updateInstancesInIndexMetadata(defnId, meta)
		r.updateRebalanceInstancesInIndexMetadata(defnId, meta)
	}
}

func (r *metadataRepo) updateInstancesInIndexMetadata(defnId c.IndexDefnId, meta *IndexMetadata) {

	meta.Instances = nil
	meta.Error = ""
	// initialize index state with smallest value.  If there is no instance, meta.State
	// will remain to be INDEX_STATE_CREATED, which will be filtered out in ListIndex.
	meta.State = c.INDEX_STATE_CREATED

	// Find all instacnes and partitions with a valid index state and Active RState.
	// This will exclude all DELETED instances.  Therefore, meta.Instances will be
	// not be empty if there is at least one instance/partition that is not DELETED
	// with active Rstate.
	chosens := r.findLatestActiveIndexInstNoLock(defnId)
	for _, inst := range chosens {
		idxInst := r.makeInstanceDefn(defnId, inst)

		if idxInst.State > meta.State {
			meta.State = idxInst.State
		}

		if idxInst.Error != meta.Error {
			meta.Error += idxInst.Error + "\n"
		}

		meta.Instances = append(meta.Instances, idxInst)

		logging.Debugf("new index instance %v for %v", idxInst, defnId)
	}

	logging.Debugf("update index metadata: index definition %v has %v active instances.", defnId, len(meta.Instances))
}

func (r *metadataRepo) makeInstanceDefn(defnId c.IndexDefnId, inst *mc.IndexInstDistribution) *InstanceDefn {

	idxInst := new(InstanceDefn)
	idxInst.DefnId = defnId
	idxInst.InstId = c.IndexInstId(inst.InstId)
	idxInst.State = c.IndexState(inst.State)
	idxInst.Error = inst.Error
	idxInst.RState = inst.RState
	idxInst.ReplicaId = inst.ReplicaId
	idxInst.StorageMode = inst.StorageMode
	idxInst.IndexerId = make(map[c.PartitionId]c.IndexerId)
	idxInst.Versions = make(map[c.PartitionId]uint64)
	idxInst.NumPartitions = inst.NumPartitions

	if idxInst.NumPartitions == 0 {
		idxInst.NumPartitions = uint32(len(inst.Partitions))
	}

	for _, partition := range inst.Partitions {
		for _, slice := range partition.SinglePartition.Slices {
			idxInst.IndexerId[c.PartitionId(partition.PartId)] = c.IndexerId(slice.IndexerId)
		}
		idxInst.Versions[c.PartitionId(partition.PartId)] = partition.Version
	}

	return idxInst
}

func (r *metadataRepo) copyInstanceDefn(source *InstanceDefn) *InstanceDefn {

	idxInst := new(InstanceDefn)
	idxInst.DefnId = source.DefnId
	idxInst.InstId = source.InstId
	idxInst.State = source.State
	idxInst.Error = source.Error
	idxInst.RState = source.RState
	idxInst.ReplicaId = source.ReplicaId
	idxInst.StorageMode = source.StorageMode
	idxInst.IndexerId = make(map[c.PartitionId]c.IndexerId)
	idxInst.NumPartitions = source.NumPartitions

	for partnId, indexerId := range source.IndexerId {
		idxInst.IndexerId[partnId] = indexerId
	}

	for partnId, version := range source.Versions {
		idxInst.Versions[partnId] = version
	}

	return idxInst
}

//
// This function finds if there is any instance of the given index being under rebalance.
// 1) The instance must have a greater version than an active instance.
// 2) If there is no active instance, it must have a version greater than 0.
// 3) If there are multiple versions under rebalance, the highest version is chosen.
// 4) The highest version active instance can be promoted to active if there is no active instance.
//
func (r *metadataRepo) updateRebalanceInstancesInIndexMetadata(defnId c.IndexDefnId, meta *IndexMetadata) {

	meta.InstsInRebalance = nil

	instsByInstId := r.instances[defnId]
	for instId, _ := range instsByInstId {

		var moreRecentInst *mc.IndexInstDistribution
		var activeInst *InstanceDefn

		for _, current := range meta.Instances {
			if current.InstId == c.IndexInstId(instId) {
				activeInst = current
				break
			}
		}

		// Find an instance-in-rebalance that is more recent than the active instance.  If there is no active instance,
		// find the latest instance-in-rebalance (instance-in-rebalance must have a version > 0).
		if activeInst != nil {
			moreRecentInst = r.findIndexInstNoLock(defnId, instId, activeInst, uint32(c.REBAL_PENDING))
		} else {
			// index in rebalacnce must have version > 0
			moreRecentInst = r.findIndexInstNoLock(defnId, instId, nil, uint32(c.REBAL_PENDING))
		}

		// Add this instance to "future topology" if it does not have another instance with
		// active Rstate or it is more recent than the active Rstate instance.
		if moreRecentInst != nil {

			idxInst := r.makeInstanceDefn(defnId, moreRecentInst)
			meta.InstsInRebalance = append(meta.InstsInRebalance, idxInst)

			// If there are two copies of the same instnace, the rebalancer
			// ensures that the index state of the new copy is ACTIVE before the
			// index state of the old copy is deleted.   Therefore, if we see
			// there is a copy with RState=PENDING, but there is no copy with
			// RState=ACTIVE, it means:
			// 1) the active RState copy has been deleted, but metadataprovider
			//    coud not see the update from the new copy yet.
			// 2) the indexer with the old copy is partitioned away from cbq when
			//    cbq is bootstrap.   In this case, we do not know for sure
			//    what is the state of the old copy.
			// 3) During bootstrap, different watchers are instanitated at different
			//    time.   The watcher with the new copy may be initiated before the
			//    indexer with old copy.  Even though, the metadata will be eventually
			//    consistent when all watchers are alive, but metadata can be temporarily
			//    inconsistent.
			//
			// We want to promote the index state for (1), but not the other 2 cases.
			//
			if activeInst == nil {

				// Promote if all watchers are running/synchronzied with indexers.  If this function
				// returns true, it means the cluster is not under topology change nor process under
				// bootstrap.   Note that once watcher is synchronized, it keeps a copy of the metadata
				// in memory until being unwatched.
				if r.provider.allWatchersRunningNoLock() {
					logging.Debugf("update update metadata: promote instance state %v %v", defnId, idxInst.InstId)
					idxInst.State = c.INDEX_STATE_ACTIVE
				}

				if idxInst.State > meta.State {
					meta.State = idxInst.State
				}
			}
		}
	}

	logging.Debugf("update update metadata: index definition %v has %v instances under rebalance.", defnId, len(meta.InstsInRebalance))
}

func (r *metadataRepo) resolveIndexStats(indexerId c.IndexerId, stats c.Statistics) map[c.IndexInstId]map[c.PartitionId]c.Statistics {

	r.mutex.RLock()
	defer r.mutex.RUnlock()

	if len(indexerId) == 0 {
		return (map[c.IndexInstId]map[c.PartitionId]c.Statistics)(nil)
	}

	result := make(map[c.IndexInstId]map[c.PartitionId]c.Statistics)

	// if the index is being rebalanced, this will only look at the active instance with the highest instance version.
	for _, meta := range r.indices {
		for _, inst := range meta.Instances {

			prefix := c.GetStatsPrefix(meta.Definition.Bucket, meta.Definition.Scope,
				meta.Definition.Collection, meta.Definition.Name, int(inst.ReplicaId), 0, false)

			for statName, statVal := range stats {
				if strings.HasPrefix(statName, prefix) {
					key := strings.TrimPrefix(statName, prefix)
					for partitionId, indexerId2 := range inst.IndexerId {
						if indexerId == indexerId2 {
							if _, ok := result[inst.InstId]; !ok {
								result[inst.InstId] = make(map[c.PartitionId]c.Statistics)
							}
							if _, ok := result[inst.InstId][partitionId]; !ok {
								result[inst.InstId][partitionId] = make(c.Statistics)
							}
							result[inst.InstId][partitionId].Set(key, statVal)
						}
					}
				}
			}
		}
	}

	return result
}

// resolveIndexStats2 extracts statistics of the requested indexerId from stats (the Stats field of an IndexStats2 obj).
func (r *metadataRepo) resolveIndexStats2(indexerId c.IndexerId, stats map[string]*DedupedIndexStats) map[c.IndexInstId]map[c.PartitionId]c.Statistics {

	r.mutex.RLock()
	defer r.mutex.RUnlock()

	if len(indexerId) == 0 {
		return (map[c.IndexInstId]map[c.PartitionId]c.Statistics)(nil)
	}

	result := make(map[c.IndexInstId]map[c.PartitionId]c.Statistics)

	// if the index is being rebalanced, this will only look at the active instance with the highest instance version.
	for _, meta := range r.indices {
		for _, inst := range meta.Instances {

			// Get the fully qualified index name
			indexName := c.GetStatsPrefix(meta.Definition.Bucket, meta.Definition.Scope,
				meta.Definition.Collection, meta.Definition.Name, int(inst.ReplicaId), 0, false)
			indexName = indexName[0 : len(indexName)-1] // strip off trailing colon

			if dedupedIndexStats, ok := stats[meta.Definition.Bucket]; !ok {
				return result
			} else {
				if perIndexStats, exists := dedupedIndexStats.Indexes[indexName]; exists {
					for partitionId, indexerId2 := range inst.IndexerId {
						if indexerId == indexerId2 {
							if _, ok := result[inst.InstId]; !ok {
								result[inst.InstId] = make(map[c.PartitionId]c.Statistics)
							}
							if _, ok := result[inst.InstId][partitionId]; !ok {
								result[inst.InstId][partitionId] = make(c.Statistics)
							}
							result[inst.InstId][partitionId].Set("num_docs_pending", interface{}(dedupedIndexStats.NumDocsPending))
							result[inst.InstId][partitionId].Set("num_docs_queued", interface{}(dedupedIndexStats.NumDocsQueued))
							result[inst.InstId][partitionId].Set("last_rollback_time", interface{}(dedupedIndexStats.LastRollbackTime))
							result[inst.InstId][partitionId].Set("progress_stat_time", interface{}(dedupedIndexStats.ProgressStatTime))
						}
					}
					if perIndexStats != nil {
						if val, ok := meta.Stats["last_known_scan_time"]; ok {
							if val.(float64) < dedupedIndexStats.Indexes[indexName].LastScanTime {
								meta.Stats["last_known_scan_time"] = dedupedIndexStats.Indexes[indexName].LastScanTime
							}
						} else {
							meta.Stats["last_known_scan_time"] = dedupedIndexStats.Indexes[indexName].LastScanTime
						}
					}
				}
			}
		}
	}

	return result
}

// wait for one of the instance to satisfy the event
func (r *metadataRepo) waitForEvent(defnId c.IndexDefnId, status []c.IndexState, topology map[int]map[c.PartitionId]c.IndexerId) error {

	event := &event{defnId: defnId, status: status, notifyCh: make(chan error, 1), topology: topology}
	if r.registerEvent(event) {
		logging.Debugf("metadataRepo.waitForEvent(): wait event : id %v status %v", event.defnId, event.status)
		err, ok := <-event.notifyCh
		if ok && err != nil {
			logging.Debugf("metadataRepo.waitForEvent(): wait arrives : id %v status %v", event.defnId, event.status)
			return err
		}
	}
	return nil
}

// wait for all instances to satisfy the event.
func (r *metadataRepo) waitForAllEvent(defnId c.IndexDefnId, status []c.IndexState, topology map[int]map[c.PartitionId]c.IndexerId) error {

	event := &event{defnId: defnId, status: status, notifyCh: make(chan error, 1), topology: topology, all: true}
	if r.registerEvent(event) {
		logging.Debugf("metadataRepo.waitForAllEvent(): wait event : id %v status %v", event.defnId, event.status)
		err, ok := <-event.notifyCh
		if ok && err != nil {
			logging.Debugf("metadataRepo.waitForAllEvent(): wait arrives : id %v status %v", event.defnId, event.status)
			return err
		}
	}
	return nil
}

func (r *metadataRepo) registerEvent(event *event) bool {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	if !r.hasWellFormedAllInstMatchingStatusNoLock(event.defnId, event.status, event.all) {
		logging.Debugf("metadataRepo.registerEvent(): add event : id %v status %v", event.defnId, event.status)
		r.notifiers[event.defnId] = event
		return true
	}

	logging.Debugf("metadataRepo.registerEvent(): found event existed: id %v status %v", event.defnId, event.status)
	return false
}

func (r *metadataRepo) notifyEvent() {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	for defnId, event := range r.notifiers {
		if r.hasWellFormedAllInstMatchingStatusNoLock(defnId, event.status, event.all) {
			delete(r.notifiers, defnId)
			close(event.notifyCh)
		} else if err := r.getDefnErrorIgnoreStatusNoLock(defnId); err != nil {
			delete(r.notifiers, defnId)
			event.notifyCh <- err
			close(event.notifyCh)
		}
	}
}

func (r *metadataRepo) notifyIndexerClose(indexerId c.IndexerId) {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	for defnId, event := range r.notifiers {
		for replicaId, partns := range event.topology {

			found := false
			for _, indexerId2 := range partns {
				if indexerId2 == indexerId {
					found = true
					break
				}
			}

			if found {
				delete(event.topology, replicaId)
			}
		}

		if len(event.topology) == 0 {
			delete(r.notifiers, defnId)
			event.notifyCh <- errors.New("Terminate Request due to client termination")
			close(event.notifyCh)
		}
	}
}

func (r *metadataRepo) cancelEvent(defnId c.IndexDefnId, err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	event, ok := r.notifiers[defnId]
	if !ok {
		return
	}

	delete(r.notifiers, defnId)

	if err != nil {
		event.notifyCh <- err
	} else {
		close(event.notifyCh)
	}
}

///////////////////////////////////////////////////////
// private function : IndexMetadata
///////////////////////////////////////////////////////

func (m *IndexMetadata) numPartitions() int {

	if len(m.Instances) > 0 {
		return int(m.Instances[0].NumPartitions)
	}

	return -1
}

func (m *IndexMetadata) numInstances() int {

	return len(m.Instances)
}

///////////////////////////////////////////////////////
// private function : Watcher
///////////////////////////////////////////////////////

func newWatcher(o *MetadataProvider, addr string) *watcher {
	s := new(watcher)

	s.provider = o
	s.leaderAddr = addr
	s.killch = make(chan bool, 1) // make it buffered to unblock sender
	s.alivech = make(chan bool, 1)
	s.pingch = make(chan bool, 1)
	s.factory = message.NewConcreteMsgFactory()
	s.pendings = make(map[common.Txnid]protocol.LogEntryMsg)
	s.incomingReqs = make(chan *protocol.RequestHandle, REQUEST_CHANNEL_COUNT)
	s.pendingReqs = make(map[uint64]*protocol.RequestHandle)
	s.loggedReqs = make(map[common.Txnid]*protocol.RequestHandle)
	s.isClosed = false
	s.lastSeenTxid = common.Txnid(0)
	s.storeStats = (o.statsNotifyCh != nil)
	s.clusterUrl = o.clusterUrl
	s.setAuthHost()
	s.setAuthParams()

	return s
}

// Need to get HTTP port for leader address. Cannot use getHttpAddr as the
// service map may not be available.
func (w *watcher) setAuthHost() error {
	if len(w.authHost) != 0 {
		return nil
	}

	clusterUrl, err := c.ClusterAuthUrl(w.clusterUrl)
	if err != nil {
		logging.Errorf("watcher:setAuthHost error in ClusterAuthUrl %v for %v", err, w.leaderAddr)
		return err
	}

	cinfo, err := c.NewClusterInfoCache(clusterUrl, c.DEFAULT_POOL)
	if err != nil {
		logging.Errorf("watcher:setAuthHost error in NewClusterInfoCache %v for %v", err, w.leaderAddr)
		return err
	}

	cinfo.Lock()
	defer cinfo.Unlock()

	cinfo.SetUserAgent(fmt.Sprintf("watcher:%v", w.leaderAddr))

	err = cinfo.FetchNodesAndSvsInfo()
	if err != nil {
		logging.Errorf("watcher:setAuthHost error in FetchNodesAndSvsInfo %v for %v", err, w.leaderAddr)
		return err
	}

	var authHost string
	authHost, err = cinfo.TranslatePort(w.leaderAddr, c.INDEX_ADMIN_SERVICE, c.INDEX_HTTP_SERVICE)
	if err != nil {
		logging.Errorf("watcher:setAuthHost error in TranslatePort %v for %v", err, w.leaderAddr)
		return err
	}

	w.authHost = authHost
	return nil
}

func (w *watcher) setAuthParams() error {

	if w.aParams == nil || len(w.aParams.user) == 0 || len(w.aParams.pass) == 0 {
		user, pass, err := cbauth.GetHTTPServiceAuth(w.authHost)
		if err != nil {
			logging.Errorf("watcher:ClientAuth cbauth.GetHTTPServiceAuth returns error %v", err)
			return err
		}

		w.aParams = &authParams{
			user: user,
			pass: pass,
		}
	}

	return nil
}

func (w *watcher) ClientAuth(pipe *common.PeerPipe) error {

	raddr := pipe.GetAddr()

	clusterVer := c.GetClusterVersion()
	if clusterVer < int64(w.provider.GetClusterVersion()) {
		clusterVer = int64(w.provider.GetClusterVersion())
	}
	intVer := c.GetInternalVersion()
	if intVer.LessThan(w.provider.internalVersion) {
		intVer = w.provider.internalVersion
	}

	if clusterVer == 0 || intVer.Equals("") {
		logging.Warnf("watcher:ClientAuth cluster Ver (%v)/Internal Version (%v) not yet initialised", clusterVer, intVer)
	}

	if clusterVer < c.INDEXER_71_VERSION && intVer.LessThan(c.MIN_VER_SRV_AUTH) {
		logging.Infof("watcher:ClientAuth skipping auth because of cluster "+
			"version %v, internal version %v for %v", clusterVer, intVer, raddr)
		return nil
	}

	if err := w.setAuthHost(); err != nil {
		return err
	}

	if err := w.setAuthParams(); err != nil {
		return err
	}

	authReq := &AuthRequest{
		User: w.aParams.user,
		Pass: w.aParams.pass,
	}

	var err error

	var content []byte
	if content, err = MarshallAuthRequest(authReq); err != nil {
		logging.Errorf("watcher:ClientAuth MarshallAuthRequest error %v for %v", err, raddr)
		return err
	}

	uuid, err := c.NewUUID()
	if err != nil {
		logging.Errorf("watcher:ClientAuth NewUUID error %v for %v", err, raddr)
		return err
	}
	id := uuid.Uint64()

	request := w.factory.CreateRequest(id, uint32(OPCODE_AUTH_REQUEST), "auth request", content)
	pipe.Send(request)

	// Receive the response.
	reqch := pipe.ReceiveChannel()
	req, ok := <-reqch
	if !ok {
		return common.NewError(common.SERVER_ERROR, "watcher:ClientAuth: channel closed. Terminate")
	}

	if req.Name() != "Response" {
		return common.NewError(common.PROTOCOL_ERROR,
			"watcher:ClientAuth: Expect message Response, Receive message "+req.Name())
	}

	resp := req.(protocol.ResponseMsg)
	if errStr := resp.GetError(); len(errStr) != 0 {
		logging.Errorf("watcher:ClientAuth Response error %v for %v", errStr, raddr)
		return fmt.Errorf("%v", errStr)
	}

	var authResp *AuthResponse
	if authResp, err = UnmarshallAuthResponse(resp.GetContent()); err != nil {
		logging.Errorf("watcher:ClientAuth UnmarshallAuthResponse error %v for %v", err, raddr)
		return err
	}

	if authResp.Code != transport.AUTH_SUCCESS {
		logging.Errorf("watcher:ClientAuth auth failure with code %v for %v", authResp.Code, raddr)
		return fmt.Errorf("Adminport Authentication Failure")
	}

	logging.Infof("watcher:ClientAuth successful for %v", raddr)

	return nil
}

func (w *watcher) waitForReady(readych chan bool, timeout int, killch chan bool) (done bool, killed bool) {

	if killch == nil {
		killch = make(chan bool, 1)
	}

	if timeout > 0 {
		// if there is a timeout
		ticker := time.NewTicker(time.Duration(timeout) * time.Millisecond)
		defer ticker.Stop()
		select {
		case <-readych:
			return true, false
		case <-ticker.C:
			return false, false
		case <-killch:
			w.killch <- true
			return false, true
		}
	} else {
		// if there is no timeout
		select {
		case <-readych:
			return true, false
		case <-killch:
			w.killch <- true
			return false, true
		}
	}

	return true, false
}

func (w *watcher) notifyReady(addr string, retry int, killch chan bool) (done bool, killed bool) {

	if killch == nil {
		killch = make(chan bool, 1)
	}

	// start a timer if it has not restarted yet
	if w.timerKillCh == nil {
		w.startTimer()
	}

RETRY2:
	// get IndexerId from indexer
	err := w.refreshServiceMap()
	if err == nil {
		err = w.updateServiceMap(addr)
	}

	if err == nil && w.storeStats {
		clusterVersion := w.getClusterVersion()
		if clusterVersion >= c.INDEXER_70_VERSION {
			err = w.getClientStats()
		}
	}

	if err != nil && retry != 0 {
		ticker := time.NewTicker(time.Duration(500) * time.Millisecond)
		select {
		case <-killch:
			ticker.Stop()
			return false, true
		case <-ticker.C:
			// do nothing
		}
		ticker.Stop()
		retry--
		goto RETRY2
	}

	if err != nil {
		return false, false
	}

	return true, false
}

//
//  This function cannot hold lock since it waits for channel.
//  We don't want to block the watcher for potential deadlock.
//  It is important the caller of this function holds the lock
//  as to ensure this function is mutual exclusive.
//
func (w *watcher) isAliveNoLock() bool {

	for len(w.pingch) > 0 {
		<-w.pingch
	}

	for len(w.alivech) > 0 {
		<-w.alivech
	}

	w.pingch <- true

	ticker := time.NewTicker(time.Duration(100) * time.Millisecond)
	defer ticker.Stop()

	select {
	case <-w.alivech:
		return true
	case <-ticker.C:
		return false
	}

	return false
}

func (w *watcher) updateServiceMap(adminport string) error {

	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.serviceMap == nil {
		panic("Index node metadata is not initialized")
	}

	h, _, err := net.SplitHostPort(adminport)
	if err != nil {
		return err
	}

	if len(h) > 0 {
		w.serviceMap.AdminAddr = adminport

		_, p, err := net.SplitHostPort(w.serviceMap.NodeAddr)
		if err != nil {
			return err
		}
		w.serviceMap.NodeAddr = net.JoinHostPort(h, p)

		_, p, err = net.SplitHostPort(w.serviceMap.ScanAddr)
		if err != nil {
			return err
		}
		w.serviceMap.ScanAddr = net.JoinHostPort(h, p)

		_, p, err = net.SplitHostPort(w.serviceMap.HttpAddr)
		if err != nil {
			return err
		}
		w.serviceMap.HttpAddr = net.JoinHostPort(h, p)
	}

	return nil
}

func (w *watcher) updateServiceMapNoLock(indexerId c.IndexerId, serviceMap *ServiceMap) bool {

	needRefresh := false

	if w.serviceMap == nil {
		return false
	}

	if w.serviceMap.ServerGroup != serviceMap.ServerGroup {
		logging.Infof("Received new service map.  Server group=%v", serviceMap.ServerGroup)
		w.serviceMap.ServerGroup = serviceMap.ServerGroup
		needRefresh = true
	}

	if w.serviceMap.IndexerVersion != serviceMap.IndexerVersion {
		logging.Infof("Received new service map.  Indexer version=%v", serviceMap.IndexerVersion)
		w.serviceMap.IndexerVersion = serviceMap.IndexerVersion
		needRefresh = true
	}

	if w.serviceMap.NodeAddr != serviceMap.NodeAddr {
		logging.Infof("Received new service map.  Node Addr=%v", serviceMap.NodeAddr)
		w.serviceMap.NodeAddr = serviceMap.NodeAddr
		needRefresh = true
	}

	if w.serviceMap.ClusterVersion != serviceMap.ClusterVersion {
		logging.Infof("Received new service map.  Cluster version=%v", serviceMap.ClusterVersion)
		w.serviceMap.ClusterVersion = serviceMap.ClusterVersion
		needRefresh = true
	}

	if w.serviceMap.ExcludeNode != serviceMap.ExcludeNode {
		logging.Infof("Received new service map.  ExcludeNode=%v", serviceMap.ExcludeNode)
		w.serviceMap.ExcludeNode = serviceMap.ExcludeNode
		needRefresh = true
	}

	if w.serviceMap.StorageMode != serviceMap.StorageMode {
		logging.Infof("Received new service map.  StorageMode=%v", serviceMap.StorageMode)
		w.serviceMap.StorageMode = serviceMap.StorageMode
		needRefresh = true
	}

	return needRefresh
}

func (w *watcher) updateIndexStatsNoLock(indexerId c.IndexerId, indexStats *IndexStats) map[c.IndexInstId]map[c.PartitionId]c.Statistics {

	stats := (map[c.IndexInstId]map[c.PartitionId]c.Statistics)(nil)
	if indexStats != nil && len(indexStats.Stats) != 0 {
		stats = w.provider.repo.resolveIndexStats(indexerId, indexStats.Stats)
		if len(stats) == 0 {
			stats = nil
		}
	}

	return stats
}

// updateIndexStats2NoLock does two things:
//   1. Update the local w.clientStats cache with the indexStats2 that was just received from periodic broadcast iff it
//      is a complete copy. Incomplete copies are never cached. The cache is only referenced for bucket-index layouts.
//   2. Return the most recent bucket-level stats for indexerId (multiplexed to [instId][partitionId]) from indexStats2.
//
// #1 is silently skipped if the received indexStats2 is incomplete. This occurs whenever no buckets or indexes have
// been created or dropped since the prior broadcast. In this case all DedupedIndexStats.Indexes map entries of the
// indexStats2.Stats top-level map will be nil to save on payload (otherwise none of them will be nil, though they can
// be empty if there are no indexes for the corresponding buckets).
//
// #2 is done by retrieving the stats from the indexStats2 object and multiplexing them to the output format of a map
// per [instId][partitionId]. This can only be accomplished if we have a complete version of this object so that the
// Indexes map entries exist, as the keys of these maps tell what indexes exist in each bucket; otherwise we can't do
// the multiplexing for the output map. (Currently the values are just empty structs.) Thus #2 is only performed if
// either a full indexStats2 came in or we already have one in the w.clientStats cache. In the latter case we copy the
// Indexes entries from the cached copy into the incoming indexStats2 object, then use this patched-up object to extract
// the stats from, as its stats are fresh while the cached ones are stale.
//
// At client startup this function is in a race with function getClientStats to initialize the w.clientStats cache. That
// function is called only once when the client is started and requests a full set of stats from the indexer to
// initially populate the local w.clientStats cache, otherwise we would never get a complete indexStats2 object from the
// server unless something changes. If the current function receives a complete indexStats2 first, it can populate the
// cache, and then getClientStats will avoid doing so as its copy might be older and thus cause later bucket lookup
// misses in the cached copy if a bucket was added after that version.
func (w *watcher) updateIndexStats2NoLock(indexerId c.IndexerId, indexStats2 *IndexStats2) map[c.IndexInstId]map[c.PartitionId]c.Statistics {

	// Check if the received indexStats2 is incomplete, in which case try to use the cached ones instead
	useCached := false
	for _, dedupedIndexStats := range indexStats2.Stats {
		if dedupedIndexStats == nil || len(dedupedIndexStats.Indexes) == 0 {
			useCached = true
			break
		}
	}

	if useCached {
		// Check if clientStats entry exists yet. It is possible that getClientStats is yet to receive the response
		// to OPCODE_CLIENT_STATS, while the current function already got one from periodic broadcast (a corner case).
		clientStatsPtr := w.clientStats.Get()
		if clientStatsPtr == nil {
			return nil
		}

		// Copy index information from cached version to indexStats2 received from broadcast
		for bucket, dedupedIndexStats := range indexStats2.Stats {
			if dedupedIndexStats == nil || clientStatsPtr.Stats[bucket] == nil {
				logging.Errorf("watcher.updateIndexStats2NoLock: unexpected nil *DedupedIndexStats."+
					" bucket %v, dedupedIndexStats %v, clientStatsPtr.Stats[bucket] %v",
					bucket, dedupedIndexStats, clientStatsPtr.Stats[bucket])
				return nil
			}
			dedupedIndexStats.Indexes = clientStatsPtr.Stats[bucket].Indexes // map of index names for this bucket
		}
	} else {
		// Update the cached value with indexStats2
		w.clientStats.Set(indexStats2)
	}

	// We have a (possibly patched-up) complete indexStats2, so extract the stats for indexerId from it
	stats := (map[c.IndexInstId]map[c.PartitionId]c.Statistics)(nil)
	if indexStats2 != nil && len(indexStats2.Stats) != 0 {
		stats = w.provider.repo.resolveIndexStats2(indexerId, indexStats2.Stats)
		if len(stats) == 0 {
			stats = nil
		}
	}
	return stats
}

func (w *watcher) getIndexerId() c.IndexerId {

	w.mutex.Lock()
	defer w.mutex.Unlock()

	indexerId := w.getIndexerIdNoLock()
	if indexerId == c.INDEXER_ID_NIL {
		panic("Index node metadata is not initialized")
	}

	return indexerId
}

func (w *watcher) getNodeUUID() string {

	w.mutex.Lock()
	defer w.mutex.Unlock()

	return w.getNodeUUIDNoLock()
}

func (w *watcher) getIndexerIdNoLock() c.IndexerId {

	if w.serviceMap == nil {
		return c.INDEXER_ID_NIL
	}

	return c.IndexerId(w.serviceMap.IndexerId)
}

func (w *watcher) getNodeUUIDNoLock() string {

	if w.serviceMap == nil {
		panic("Index node metadata is not initialized")
	}

	return w.serviceMap.NodeUUID
}

func (w *watcher) getServerGroup() string {

	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.serviceMap == nil {
		panic("Index node metadata is not initialized")
	}

	return w.serviceMap.ServerGroup
}

func (w *watcher) getIndexerVersion() uint64 {

	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.serviceMap == nil {
		panic("Index node metadata is not initialized")
	}

	return w.serviceMap.IndexerVersion
}

func (w *watcher) getClusterVersion() uint64 {

	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.serviceMap == nil {
		panic("Index node metadata is not initialized")
	}

	return w.serviceMap.ClusterVersion
}

func (w *watcher) getStorageMode() c.StorageMode {

	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.serviceMap == nil {
		panic("Index node metadata is not initialized")
	}

	return c.StorageMode(w.serviceMap.StorageMode)
}

func (w *watcher) getNodeAddr() string {

	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.serviceMap == nil {
		panic("Index node metadata is not initialized")
	}

	return w.serviceMap.NodeAddr
}

func (w *watcher) getAdminAddr() string {

	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.serviceMap == nil {
		panic("Index node metadata is not initialized")
	}

	return w.serviceMap.AdminAddr
}

func (w *watcher) getScanAddr() string {

	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.serviceMap == nil {
		panic("Index node metadata is not initialized")
	}

	return w.serviceMap.ScanAddr
}

func (w *watcher) getHttpAddr() string {

	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.serviceMap == nil {
		panic("Index node metadata is not initialized")
	}

	return w.serviceMap.HttpAddr
}

func (w *watcher) refreshServiceMap() error {

	content, err := w.makeRequest(OPCODE_SERVICE_MAP, "Service Map", []byte(""))
	if err != nil {
		logging.Errorf("watcher.refreshServiceMap() %s", err)
		return err
	}

	srvMap, err := UnmarshallServiceMap(content)
	if err != nil {
		logging.Errorf("watcher.refreshServiceMap() %s", err)
		return err
	}

	w.mutex.Lock()
	defer w.mutex.Unlock()

	w.serviceMap = srvMap
	return nil
}

// getClientStats requests a complete set of IndexStats2 from the Indexer, including the Indexes maps children
// of the DedupedIndexStats entries of the top-level map. This function is called only at bootstrap of the
// client to initialize the w.clientStats cache. This call is in a race with the ongoing event-handling loop
// that calls updateIndexStats2NoLock, so it must guard against overwriting newer stats with older ones.
func (w *watcher) getClientStats() error {

	content, err := w.makeRequest(OPCODE_CLIENT_STATS, "Client Stats", []byte(""))
	if err != nil {
		logging.Errorf("watcher.getClientStats() %s", err)
		return err
	}

	clientStats, err := UnmarshallIndexStats2(content)
	if err != nil {
		logging.Errorf("watcher.getClientStats() %s", err)
		return err
	}

	// Avoid overwriting the cache if updateIndexStats2NoLock already received and cached a clientStats object
	w.clientStats.CAS(clientStats) // never nil
	return nil
}

func (w *watcher) cleanupIndices(repo *metadataRepo) {

	w.mutex.Lock()
	defer w.mutex.Unlock()

	// CleanupIndices may not necessarily remove all indcies
	// from the repository for this watcher, since there may
	// be new messages still in flight from gometa.   Even so,
	// repoistory will filter out any watcher that has been
	// terminated. So there is no functional issue.
	// TODO: It is actually possible to wait for gometa to
	// stop, before cleaning up the indices.
	indexerId := w.getIndexerIdNoLock()
	if indexerId != c.INDEXER_ID_NIL {
		repo.cleanupIndicesForIndexer(indexerId)
	}
}

func (w *watcher) close() {

	logging.Infof("Unwatching metadata for indexer at %v.", w.leaderAddr)

	// kill the watcherServer
	if len(w.killch) == 0 {
		w.killch <- true
	}

	// kill the timeoutChecker
	if len(w.timerKillCh) == 0 {
		w.timerKillCh <- true
	}

	w.cleanupOnClose()
}

func (w *watcher) makeRequest(opCode common.OpCode, key string, content []byte) ([]byte, error) {

	uuid, err := c.NewUUID()
	if err != nil {
		return nil, err
	}
	id := uuid.Uint64()

	request := w.factory.CreateRequest(id, uint32(opCode), key, content)

	handle := &protocol.RequestHandle{Request: request, Err: nil, StartTime: 0, Content: nil}
	handle.StartTime = time.Now().UnixNano()
	handle.CondVar = sync.NewCond(&handle.Mutex)

	handle.CondVar.L.Lock()
	defer handle.CondVar.L.Unlock()

	if w.queueRequest(handle) {
		handle.CondVar.Wait()
	}

	return handle.Content, handle.Err
}

func (w *watcher) startTimer() {

	w.timerKillCh = make(chan bool, 1)
	go w.timeoutChecker()
}

func (w *watcher) timeoutChecker() {

	timer := time.NewTicker(time.Second)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			w.cleanupOnTimeout()
		case <-w.timerKillCh:
			return
		}
	}
}

func (w *watcher) cleanupOnTimeout() {

	// Mutex for protecting the following go-routine:
	// 1) WatcherServer main processing loop
	// 2) Commit / LogProposal / Respond / Abort
	// 3) CleanupOnClose / CleaupOnError

	w.mutex.Lock()
	defer w.mutex.Unlock()

	errormsg := "Request timed out. Index server may still be processing this request. Please check the status after sometime or retry."
	current := time.Now().UnixNano()

	for key, request := range w.pendingReqs {
		if current-request.StartTime >= w.provider.timeout {
			delete(w.pendingReqs, key)
			w.signalError(request, errormsg)
		}
	}

	for key, request := range w.loggedReqs {
		if current-request.StartTime >= w.provider.timeout {
			delete(w.loggedReqs, key)
			w.signalError(request, errormsg)
		}
	}
}

func (w *watcher) cleanupOnClose() {

	// Mutex for protecting the following go-routine:
	// 1) WatcherServer main processing loop
	// 2) Commit / LogProposal / Respond / Abort
	// 3) CleanupOnTimeout / CleaupOnError

	w.mutex.Lock()
	defer w.mutex.Unlock()

	w.isClosed = true

	for len(w.incomingReqs) != 0 {
		request := <-w.incomingReqs
		w.signalError(request, "Terminate Request during cleanup")
	}

	for key, request := range w.pendingReqs {
		delete(w.pendingReqs, key)
		w.signalError(request, "Terminate Request during cleanup")
	}

	for key, request := range w.loggedReqs {
		delete(w.loggedReqs, key)
		w.signalError(request, "Terminate Request during cleanup")
	}

	w.provider.repo.notifyIndexerClose(w.getIndexerIdNoLock())
}

func (w *watcher) signalError(request *protocol.RequestHandle, errStr string) {
	request.Err = errors.New(errStr)
	request.CondVar.L.Lock()
	defer request.CondVar.L.Unlock()
	request.CondVar.Signal()
}

///////////////////////////////////////////////////////
// private function
///////////////////////////////////////////////////////

func isIndexDefnKey(key string) bool {
	return strings.Contains(key, "IndexDefinitionId/")
}

func isIndexTopologyKey(key string) bool {
	return strings.Contains(key, "IndexTopology/")
}

func isServiceMapKey(key string) bool {
	return strings.Contains(key, "ServiceMap")
}

func isIndexStats(key string) bool {
	return key == "IndexStats"
}

func isIndexStats2(key string) bool {
	return key == "IndexStats2"
}

///////////////////////////////////////////////////////
// Interface : RequestMgr
///////////////////////////////////////////////////////

func (w *watcher) queueRequest(handle *protocol.RequestHandle) bool {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.isClosed {
		handle.Err = errors.New("Connection is shutting down.  Cannot process incoming request")
		return false
	}

	w.incomingReqs <- handle
	return true
}

func (w *watcher) AddPendingRequest(handle *protocol.RequestHandle) {

	// Mutex for protecting the following go-routine:
	// 1) Commit / LogProposal / Respond / Abort
	// 2) CleanupOnClose / CleaupOnTimeout / CleanupOnError

	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.isClosed {
		w.signalError(handle, "Terminate Request during cleanup")
		return
	}

	// remember the request
	handle.StartTime = time.Now().UnixNano()
	w.pendingReqs[handle.Request.GetReqId()] = handle
}

func (w *watcher) GetRequestChannel() <-chan *protocol.RequestHandle {

	// Mutex for protecting the following go-routine:
	// 1) CleanupOnClose / CleaupOnTimeout

	w.mutex.Lock()
	defer w.mutex.Unlock()

	return (<-chan *protocol.RequestHandle)(w.incomingReqs)
}

func (w *watcher) CleanupOnError() {

	// Mutex for protecting the following go-routine:
	// 1) Commit / LogProposal / Respond / Abort
	// 2) CleanupOnClose / CleaupOnTimeout

	w.mutex.Lock()
	defer w.mutex.Unlock()

	for key, request := range w.pendingReqs {
		delete(w.pendingReqs, key)
		w.signalError(request, "Terminate Request due to server termination")
	}

	for key, request := range w.loggedReqs {
		delete(w.loggedReqs, key)
		w.signalError(request, "Terminate Request due to server termination")
	}
}

///////////////////////////////////////////////////////
// Interface : QuorumVerifier
///////////////////////////////////////////////////////

func (w *watcher) HasQuorum(count int) bool {
	return count == 1
}

///////////////////////////////////////////////////////
// Interface : ServerAction
///////////////////////////////////////////////////////

///////////////////////////////////////////////////////
// Server Action for Environment
///////////////////////////////////////////////////////

func (w *watcher) GetEnsembleSize() uint64 {
	return 1
}

func (w *watcher) GetQuorumVerifier() protocol.QuorumVerifier {
	return w
}

///////////////////////////////////////////////////////
// Server Action for Broadcast stage (normal execution)
///////////////////////////////////////////////////////

func (w *watcher) Commit(txid common.Txnid) error {

	w.mutex.Lock()
	defer w.mutex.Unlock()

	msg, ok := w.pendings[txid]
	if !ok {
		// Once the watcher is synchronized with the server, it is possible that the first message
		// is a commit.  It is OK to ignore this commit, since during server synchronization, the
		// server is responsible to send a snapshot of the repository that includes this commit txid.
		// If the server cannot send a snapshot that is as recent as this commit's txid, server
		// synchronization will fail, so it won't even come to this function.
		return nil
	}

	var indexerId c.IndexerId
	if w.serviceMap != nil {
		indexerId = c.IndexerId(w.serviceMap.IndexerId)
	}

	delete(w.pendings, txid)
	needRefresh, stats, err := w.processChange(txid, msg.GetOpCode(), msg.GetKey(), msg.GetContent(), indexerId)
	if needRefresh {
		w.provider.needRefresh()
	}
	if len(stats) != 0 {
		w.provider.refreshStats(stats)
	}

	handle, ok := w.loggedReqs[txid]
	if ok {
		delete(w.loggedReqs, txid)

		handle.CondVar.L.Lock()
		defer handle.CondVar.L.Unlock()

		handle.CondVar.Signal()
	}

	return err
}

func (w *watcher) LogProposal(p protocol.ProposalMsg) error {

	w.mutex.Lock()
	defer w.mutex.Unlock()

	msg := w.factory.CreateLogEntry(p.GetTxnid(), p.GetOpCode(), p.GetKey(), p.GetContent())
	w.pendings[common.Txnid(p.GetTxnid())] = msg

	handle, ok := w.pendingReqs[p.GetReqId()]
	if ok {
		delete(w.pendingReqs, p.GetReqId())
		w.loggedReqs[common.Txnid(p.GetTxnid())] = handle
	}

	return nil
}

func (w *watcher) Abort(fid string, reqId uint64, err string) error {
	w.respond(reqId, err, nil)
	return nil
}

func (w *watcher) Respond(fid string, reqId uint64, err string, content []byte) error {
	w.respond(reqId, err, content)
	return nil
}

func (w *watcher) respond(reqId uint64, err string, content []byte) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	handle, ok := w.pendingReqs[reqId]
	if ok {
		delete(w.pendingReqs, reqId)

		handle.CondVar.L.Lock()
		defer handle.CondVar.L.Unlock()

		if len(err) != 0 {
			handle.Err = errors.New(err)
		}

		logging.Debugf("watcher.Respond() : len(content) %d", len(content))
		handle.Content = content

		handle.CondVar.Signal()
	}
}

func (w *watcher) GetFollowerId() string {
	return w.provider.providerId
}

func (w *watcher) GetNextTxnId() common.Txnid {
	panic("Calling watcher.GetCommitedEntries() : not supported")
}

///////////////////////////////////////////////////////
// Server Action for retrieving repository state
///////////////////////////////////////////////////////

func (w *watcher) GetLastLoggedTxid() (common.Txnid, error) {
	// need to stream from txnid 0 since this is supported by TransientCommitLog
	return common.Txnid(0), nil
}

func (w *watcher) GetLastCommittedTxid() (common.Txnid, error) {
	// need to stream from txnid 0 since this is supported by TransientCommitLog
	return common.Txnid(0), nil
}

func (w *watcher) GetStatus() protocol.PeerStatus {
	return protocol.WATCHING
}

func (w *watcher) GetCurrentEpoch() (uint32, error) {
	return 0, nil
}

func (w *watcher) GetAcceptedEpoch() (uint32, error) {
	return 0, nil
}

///////////////////////////////////////////////////////
// Server Action for updating repository state
///////////////////////////////////////////////////////

func (w *watcher) NotifyNewAcceptedEpoch(epoch uint32) error {
	// no-op
	return nil
}

func (w *watcher) NotifyNewCurrentEpoch(epoch uint32) error {
	// no-op
	return nil
}

///////////////////////////////////////////////////////
// Function for discovery phase
///////////////////////////////////////////////////////

func (w *watcher) GetCommitedEntries(txid1, txid2 common.Txnid) (<-chan protocol.LogEntryMsg, <-chan error, chan<- bool, error) {
	panic("Calling watcher.GetCommitedEntries() : not supported")
}

func (w *watcher) LogAndCommit(txid common.Txnid, op uint32, key string, content []byte, toCommit bool) error {

	var indexerId c.IndexerId
	func() {
		w.mutex.Lock()
		defer w.mutex.Unlock()

		if w.serviceMap != nil {
			indexerId = c.IndexerId(w.serviceMap.IndexerId)
		}
	}()

	if _, _, err := w.processChange(txid, op, key, content, indexerId); err != nil {
		logging.Errorf("watcher.LogAndCommit(): receive error when processing log entry from server.  Error = %v", err)
	}

	return nil
}

func (w *watcher) processChange(txid common.Txnid, op uint32, key string, content []byte, indexerId c.IndexerId) (bool,
	map[c.IndexInstId]map[c.PartitionId]c.Statistics, error) {

	logging.Debugf("watcher.processChange(): key = %v txid=%v last_seen_txid=%v", key, txid, w.lastSeenTxid)
	defer logging.Debugf("watcher.processChange(): done -> key = %v", key)

	opCode := common.OpCode(op)

	switch opCode {
	case common.OPCODE_ADD, common.OPCODE_SET, common.OPCODE_BROADCAST:
		if isIndexDefnKey(key) {
			if len(content) == 0 {
				logging.Debugf("watcher.processChange(): content of key = %v is empty.", key)
			}

			_, err := extractDefnIdFromKey(key)
			if err != nil {
				return false, nil, err
			}
			if err := w.provider.repo.unmarshallAndAddDefn(content); err != nil {
				return false, nil, err
			}
			w.provider.repo.notifyEvent()

			if txid > w.lastSeenTxid {
				w.lastSeenTxid = txid
			}

			// Even though there is new index definition, do not refresh metadata until it sees
			// changes to index topology
			return false, nil, nil

		} else if isIndexTopologyKey(key) {
			if len(content) == 0 {
				logging.Debugf("watcher.processChange(): content of key = %v is empty.", key)
			}
			if err := w.provider.repo.unmarshallAndUpdateTopology(content, indexerId); err != nil {
				return false, nil, err
			}
			w.provider.repo.notifyEvent()

			if txid > w.lastSeenTxid {
				w.lastSeenTxid = txid
			}

			// return needRefersh to true
			return true, nil, nil

		} else if isServiceMapKey(key) {
			if len(content) == 0 {
				logging.Debugf("watcher.processChange(): content of key = %v is empty.", key)
			}

			serviceMap, err := UnmarshallServiceMap(content)
			if err != nil {
				return false, nil, err
			}

			needRefresh := w.updateServiceMapNoLock(indexerId, serviceMap)
			return needRefresh, nil, nil

		} else if isIndexStats2(key) {
			if !w.storeStats {
				return false, nil, nil
			}

			if len(content) == 0 {
				logging.Debugf("watcher.processChange(): content of key = %v is empty.", key)
			}

			indexStats2, err := UnmarshallIndexStats2(content)
			if err != nil {
				return false, nil, err
			}

			stats := w.updateIndexStats2NoLock(indexerId, indexStats2)
			return false, stats, nil

		} else if isIndexStats(key) {
			if len(content) == 0 {
				logging.Debugf("watcher.processChange(): content of key = %v is empty.", key)
			}

			indexStats, err := UnmarshallIndexStats(content)
			if err != nil {
				return false, nil, err
			}

			stats := w.updateIndexStatsNoLock(indexerId, indexStats)
			return false, stats, nil
		}
	case common.OPCODE_DELETE:

		if isIndexDefnKey(key) {

			_, err := extractDefnIdFromKey(key)
			if err != nil {
				return false, nil, err
			}
			w.provider.repo.notifyEvent()

			if txid > w.lastSeenTxid {
				w.lastSeenTxid = txid
			}

			return true, nil, nil
		}
	}

	return false, nil, nil
}

func extractDefnIdFromKey(key string) (c.IndexDefnId, error) {
	i := strings.Index(key, "/")
	if i != -1 && i < len(key)-1 {
		id, err := strconv.ParseUint(key[i+1:], 10, 64)
		return c.IndexDefnId(id), err
	}

	return c.IndexDefnId(0), errors.New("watcher.processChange() : cannot parse index definition id")
}

func init() {
	gometaL.Current = &logging.SystemLogger
}
