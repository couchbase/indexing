// Copyright (c) 2014 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package client

import (
	"errors"
	"fmt"
	"github.com/couchbase/gometa/common"
	gometaL "github.com/couchbase/gometa/log"
	"github.com/couchbase/gometa/message"
	"github.com/couchbase/gometa/protocol"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/common/queryutil"
	"github.com/couchbase/indexing/secondary/logging"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// TODO:
// 1) cleanup on create/build index fails for replica

///////////////////////////////////////////////////////
// Interface
///////////////////////////////////////////////////////

type Settings interface {
	NumReplica() int32
}

///////////////////////////////////////////////////////
// Type Definition
///////////////////////////////////////////////////////

type MetadataProvider struct {
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
	statsNotifyCh      chan map[c.IndexInstId]c.Statistics
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
//    is see the local metadata state of each indexer node.
// 8) At full participation, if an index definiton does not have any instance, the
//    index definition is considered as deleted.    The side effect is an index
//    could be implicitly dropped if it loses all its replica.
//
type metadataRepo struct {
	provider    *MetadataProvider
	definitions map[c.IndexDefnId]*c.IndexDefn
	instances   map[c.IndexDefnId]map[c.IndexInstId]map[uint64]*IndexInstDistribution
	indices     map[c.IndexDefnId]*IndexMetadata
	topology    map[c.IndexerId]map[c.IndexDefnId]bool
	version     uint64
	mutex       sync.RWMutex
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

	incomingReqs chan *protocol.RequestHandle
	pendingReqs  map[uint64]*protocol.RequestHandle // key : request id
	loggedReqs   map[common.Txnid]*protocol.RequestHandle

	notifiers map[c.IndexDefnId]*event
}

type IndexMetadata struct {
	Definition       *c.IndexDefn
	Instances        []*InstanceDefn
	InstsInRebalance []*InstanceDefn
	State            c.IndexState
	Error            string
}

type InstanceDefn struct {
	DefnId      c.IndexDefnId
	InstId      c.IndexInstId
	State       c.IndexState
	Error       string
	IndexerId   c.IndexerId
	Endpts      []c.Endpoint
	Version     uint64
	RState      uint32
	ReplicaId   uint64
	StorageMode string
}

type event struct {
	defnId   c.IndexDefnId
	status   []c.IndexState
	notifyCh chan error
}

type IndexerStatus struct {
	Adminport string
	Connected bool
}

type watcherCallback func(string, c.IndexerId, c.IndexerId)

var REQUEST_CHANNEL_COUNT = 1000

///////////////////////////////////////////////////////
// Public function : MetadataProvider
///////////////////////////////////////////////////////

func NewMetadataProvider(providerId string, changeCh chan bool, statsCh chan map[c.IndexInstId]c.Statistics, settings Settings) (s *MetadataProvider, err error) {

	s = new(MetadataProvider)
	s.watchers = make(map[c.IndexerId]*watcher)
	s.pendings = make(map[c.IndexerId]chan bool)
	s.repo = newMetadataRepo(s)
	s.timeout = int64(time.Second) * 120
	s.metaNotifyCh = changeCh
	s.statsNotifyCh = statsCh
	s.settings = settings

	s.providerId, err = s.getWatcherAddr(providerId)
	if err != nil {
		return nil, err
	}
	logging.Debugf("MetadataProvider.NewMetadataProvider(): MetadataProvider follower ID %s", s.providerId)

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

	// wait for indexer to connect
	success, _ := watcher.waitForReady(readych, 1000, nil)
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

func (o *MetadataProvider) CreateIndexWithPlan(
	name, bucket, using, exprType, partnExpr, whereExpr string,
	secExprs []string, desc []bool, isPrimary bool, plan map[string]interface{}) (c.IndexDefnId, error, bool) {

	// FindIndexByName will only return valid index
	if o.FindIndexByName(name, bucket) != nil {
		return c.IndexDefnId(0), errors.New(fmt.Sprintf("Index %s already exists.", name)), false
	}

	// Create index definition
	idxDefn, err, retry := o.PrepareIndexDefn(name, bucket, using, exprType, partnExpr, whereExpr, secExprs, desc, isPrimary, plan)
	if err != nil {
		return c.IndexDefnId(0), err, retry
	}

	//
	// Make Index Creation Request
	//

	watchers := ([]*watcher)(nil)
	for _, indexerId := range idxDefn.Nodes {
		watcher, err := o.findWatcherByIndexerId(c.IndexerId(indexerId))
		if err != nil {
			return c.IndexDefnId(0), errors.New("Fail to create index.  Internal Error: Cannot locate indexer nodes"), false
		}
		watchers = append(watchers, watcher)
	}

	defnID := idxDefn.DefnId
	wait := !idxDefn.Deferred

	key := fmt.Sprintf("%d", defnID)
	errMap := make(map[string]bool)
	for replicaId, watcher := range watchers {
		idxDefn.ReplicaId = replicaId

		content, err := c.MarshallIndexDefn(idxDefn)
		if err != nil {
			errMap[fmt.Sprintf("Fail to send create index request.  Error=%v", err)] = true
			continue
		}

		if _, err := watcher.makeRequest(OPCODE_CREATE_INDEX, key, content); err != nil {
			errMap[err.Error()] = true
		}
	}

	if len(errMap) != 0 {
		errStr := ""
		for msg, _ := range errMap {
			errStr += msg + "\n"
		}

		if len(errStr) != 0 {
			return c.IndexDefnId(0), errors.New(fmt.Sprintf("Encounter errors during create index.  Error=%s.", errStr)), false
		}
	}

	//
	// Wait for response
	//

	if wait {
		var errStr string
		for _, watcher := range watchers {
			err := watcher.waitForEvent(defnID, []c.IndexState{c.INDEX_STATE_ACTIVE, c.INDEX_STATE_DELETED})
			if err != nil {
				errStr += err.Error() + "\n"
			}
		}

		if len(errStr) != 0 {
			return c.IndexDefnId(0), errors.New(errStr), false
		}

		return defnID, nil, false
	}

	return defnID, nil, false
}

func (o *MetadataProvider) PrepareIndexDefn(
	name, bucket, using, exprType, partnExpr, whereExpr string,
	secExprs []string, desc []bool, isPrimary bool, plan map[string]interface{}) (*c.IndexDefn, error, bool) {

	//
	// Parse WITH CLAUSE
	//

	var immutable bool = false
	var deferred bool = false
	var wait bool = true
	var nodes []string = nil
	var numReplica int = 0

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
		wait = !deferred

		if indexType, ok := plan["index_type"].(string); ok {
			if c.IsValidIndexType(indexType) {
				using = indexType
			} else {
				return nil, errors.New("Fails to create index.  Invalid index_type parameter value specified."), false
			}
		}

		immutable, err, retry = o.getImmutableParam(plan)
		if err != nil {
			return nil, err, retry
		}

		numReplica, err, retry = o.getReplicaParam(plan, version)
		if err != nil {
			return nil, err, retry
		}

		if _, ok := plan["num_replica"]; ok {
			if len(nodes) != 0 {
				if numReplica != len(nodes)-1 {
					return nil, errors.New("Fails to create index.  Parameter num_replica should be one less than parameter nodes."), false
				}
			}
		}

		if numReplica == 0 && len(nodes) != 0 {
			numReplica = len(nodes) - 1
		}
	}

	logging.Debugf("MetadataProvider:CreateIndex(): deferred_build %v sync %v nodes %v", deferred, wait, nodes)

	//
	// Get the list of Watchers
	//

	watchers, err, retry := o.findWatchersWithRetry(nodes, numReplica)
	if err != nil {
		return nil, err, retry
	}

	if len(nodes) != 0 && len(watchers) != len(nodes) {
		return nil,
			errors.New(fmt.Sprintf("Fails to create index.  Some indexer node is not available for create index.  Indexers=%v.", nodes)),
			false
	}

	if numReplica != 0 && len(watchers) != numReplica+1 {
		return nil,
			errors.New(fmt.Sprintf("Fails to create index.  Cannot find enough indexer node for replica.  numReplica=%v.", numReplica)),
			false
	}

	// set the node list using indexerId
	nodes = make([]string, len(watchers))
	for i, watcher := range watchers {
		nodes[i] = string(watcher.getIndexerId())
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

	// Array index related information
	isArrayIndex := false
	arrayExprCount := 0
	for _, exp := range secExprs {
		isArray, _, err := queryutil.IsArrayExpression(exp)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Fails to create index.  Error in parsing expression %v : %v", exp, err)), false
		}
		if isArray == true {
			isArrayIndex = isArray
			arrayExprCount++
		}
	}

	if arrayExprCount > 1 {
		return nil, errors.New("Fails to create index.  Multiple expressions with ALL are found. Only one array expression is supported per index."), false
	}

	if o.isDecending(desc) && (version < c.INDEXER_50_VERSION || clusterVersion < c.INDEXER_50_VERSION) {
		return nil,
			errors.New("Fail to create index with descending order. This option is enabled after cluster is fully upgraded and there is no failed node."),
			false
	}

	if desc != nil && len(secExprs) != len(desc) {
		return nil, errors.New("Fail to create index.  Collation order is required for all expressions in the index."), false
	}

	idxDefn := &c.IndexDefn{
		DefnId:          defnID,
		Name:            name,
		Using:           c.IndexType(using),
		Bucket:          bucket,
		IsPrimary:       isPrimary,
		SecExprs:        secExprs,
		Desc:            desc,
		ExprType:        c.ExprType(exprType),
		PartitionScheme: c.SINGLE,
		PartitionKey:    partnExpr,
		WhereExpr:       whereExpr,
		Deferred:        deferred,
		Nodes:           nodes,
		Immutable:       immutable,
		IsArrayIndex:    isArrayIndex,
		NumReplica:      uint32(numReplica),
	}

	return idxDefn, nil, false
}

func (o *MetadataProvider) isDecending(desc []bool) bool {

	hasDecending := false
	for _, flag := range desc {
		hasDecending = hasDecending || flag
	}

	return hasDecending
}

func (o *MetadataProvider) getNodesParam(plan map[string]interface{}) ([]string, error, bool) {

	var nodes []string = nil

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

func (o *MetadataProvider) getImmutableParam(plan map[string]interface{}) (bool, error, bool) {

	immutable := false

	immutable2, ok := plan["immutable"].(bool)
	if !ok {
		immutable_str, ok := plan["immutable"].(string)
		if ok {
			var err error
			immutable2, err = strconv.ParseBool(immutable_str)
			if err != nil {
				return false, errors.New("Fails to create index.  Parameter Immutable must be a boolean value of (true or false)."), false
			}
			immutable = immutable2

		} else if _, ok := plan["immutable"]; ok {
			return false, errors.New("Fails to create index.  Parameter immutable must be a boolean value of (true or false)."), false
		}
	} else {
		immutable = immutable2
	}

	return immutable, nil, false
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

func (o *MetadataProvider) getReplicaParam(plan map[string]interface{}, version uint64) (int, error, bool) {

	numReplica := int(0)
	if version >= c.INDEXER_50_VERSION {
		numReplica = int(o.settings.NumReplica())
	}

	numReplica2, ok := plan["num_replica"].(float64)
	if !ok {
		numReplica_str, ok := plan["num_replica"].(string)
		if ok {
			var err error
			numReplica3, err := strconv.ParseInt(numReplica_str, 10, 64)
			if err != nil {
				return 0, errors.New("Fails to create index.  Parameter num_replica must be a integer value."), false
			}
			numReplica = int(numReplica3)

		} else if _, ok := plan["num_replica"]; ok {
			return 0, errors.New("Fails to create index.  Parameter num_replica must be a integer value."), false
		}
	} else {
		numReplica = int(numReplica2)
	}

	if numReplica > 0 && version < c.INDEXER_50_VERSION {
		return 0, errors.New("Fails to create index with replica.   This option is enabled after cluster is fully upgraded and there is no failed node."), false
	}

	if numReplica < 0 {
		return 0, errors.New("Fails to create index.  Parameter num_replica must be a positive value."), false
	}

	return numReplica, nil, false
}

func (o *MetadataProvider) findWatchersWithRetry(nodes []string, numReplica int) ([]*watcher, error, bool) {

	var watchers []*watcher
	count := 0

RETRY1:
	errCode := 0
	if len(nodes) == 0 {
		watcher, numWatcher := o.findNextAvailWatcher(watchers, true)
		if watcher == nil {
			watcher, numWatcher = o.findNextAvailWatcher(watchers, false)
		}
		if watcher == nil {
			watchers = nil
			if numWatcher == 0 {
				errCode = 1
			} else {
				errCode = 3
			}
		} else {
			watchers = append(watchers, watcher)

			if len(watchers) < numReplica+1 {
				goto RETRY1
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

	if errCode != 0 && count < 20 && !o.AllWatchersAlive() {
		logging.Debugf("MetadataProvider:findWatcherWithRetry(): cannot find available watcher. Retrying ...")
		time.Sleep(time.Duration(500) * time.Millisecond)
		count++
		goto RETRY1
	}

	if errCode == 1 {
		stmt1 := "Fails to create index.  There is no available index service that can process this request at this time."
		stmt2 := "Index Service can be in bootstrap, recovery, or non-reachable."
		stmt3 := "Please retry the operation at a later time."
		return nil, errors.New(fmt.Sprintf("%s %s %s", stmt1, stmt2, stmt3)), false

	} else if errCode == 2 {
		stmt1 := "Fails to create index.  Nodes %s does not exist or is not running"
		return nil, errors.New(fmt.Sprintf(stmt1, nodes)), true

	} else if errCode == 3 {
		stmt1 := "Fails to create index.  There are not enough indexer nodes to create index with replica count of %v"
		return nil, errors.New(fmt.Sprintf(stmt1, numReplica)), true
	}

	return watchers, nil, false
}

func (o *MetadataProvider) DropIndex(defnID c.IndexDefnId) error {

	// find index -- this method will not return the index if the index is in DELETED
	// status (but defn exists).
	meta := o.FindIndex(defnID)
	if meta == nil {
		return errors.New("Index does not exist.")
	}

	// find watcher -- This method does not check index status (return the watcher even
	// if index is in deleted status). This return an error if  watcher (holding the index)
	// is dropped asynchronously (concurrent unwatchMetadata).
	watchers, err := o.findWatchersByDefnIdIgnoreStatus(defnID)
	if err != nil {
		return errors.New(fmt.Sprintf("Cannot locate cluster node hosting Index %s.", meta.Definition.Name))
	}

	// place token for recovery.
	if err := PostDeleteCommandToken(defnID); err != nil {
		return errors.New(fmt.Sprintf("Fail to Drop Index due to internal errors.  Error=%v.", err))
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
			msg += "If cluster or indexer is currently unavailable, the operation will automaticaly retry after cluster is back to normal."
			return errors.New(msg)
		}
	}

	return nil
}

func (o *MetadataProvider) BuildIndexes(defnIDs []c.IndexDefnId) error {

	watcherIndexMap := make(map[c.IndexerId][]c.IndexDefnId)
	watcherNodeMap := make(map[c.IndexerId]string)
	defnList := ([]c.IndexDefnId)(nil)

	for _, id := range defnIDs {

		// find index -- this method will not return the index if the index is in DELETED
		// status (but defn exists).
		meta := o.FindIndex(id)
		if meta == nil {
			return errors.New("Cannot build index. Index Definition not found")
		}

		if len(meta.Instances) == 0 {
			return errors.New("Cannot build index. Index Definition not found or index is currently being rebalanced.")
		}

		for _, inst := range meta.Instances {
			if inst.State != c.INDEX_STATE_READY {

				if inst.State == c.INDEX_STATE_INITIAL || inst.State == c.INDEX_STATE_CATCHUP {
					return errors.New(fmt.Sprintf("Index %s is being built .", meta.Definition.Name))
				}

				if inst.State == c.INDEX_STATE_ACTIVE {
					return errors.New(fmt.Sprintf("Index %s is already built .", meta.Definition.Name))
				}

				return errors.New("Cannot build index. Index Definition not found")
			}
		}

		// find watcher -- This method does not check index status (return the watcher even
		// if index is in deleted status). So this return an error if  watcher is dropped
		// asynchronously (some parallel go-routine unwatchMetadata).
		watchers, err := o.findWatchersByDefnIdIgnoreStatus(id)
		if err != nil {
			return errors.New(fmt.Sprintf("Cannot locate cluster node hosting Index %s.", meta.Definition.Name))
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

	// place token for recovery.
	for _, id := range defnList {
		if err := PostBuildCommandToken(id); err != nil {
			return errors.New(fmt.Sprintf("Fail to Build Index due to internal errors.  Error=%v.", err))
		}
	}

	errMap := make(map[string]bool)
	for indexerId, idList := range watcherIndexMap {

		watcher, err := o.findAliveWatcherByIndexerId(indexerId)
		if err != nil {
			errFmtStr := "Cannot reach node %v.  Index build will retry in background once network connection is re-established."
			errMap[fmt.Sprintf(errFmtStr, watcherNodeMap[indexerId])] = true
			continue
		}

		list := BuildIndexIdList(idList)

		content, err := MarshallIndexIdList(list)
		if err != nil {
			errMap[err.Error()] = true
			continue
		}

		_, err = watcher.makeRequest(OPCODE_BUILD_INDEX, "Index Build", content)
		if err != nil {
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

func (o *MetadataProvider) FindIndex(id c.IndexDefnId) *IndexMetadata {

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

func (o *MetadataProvider) UpdateServiceAddrForIndexer(id c.IndexerId, adminport string) error {

	watcher, err := o.findWatcherByIndexerId(id)
	if err != nil {
		return errors.New(fmt.Sprintf("Cannot locate cluster node."))
	}

	return watcher.updateServiceMap(adminport)
}

func (o *MetadataProvider) FindIndexByName(name string, bucket string) *IndexMetadata {

	indices, _ := o.repo.listDefnWithValidInst()
	for _, meta := range indices {
		if o.isValidIndexFromActiveIndexer(meta) {
			if meta.Definition.Name == name && meta.Definition.Bucket == bucket {
				return meta
			}
		}
	}

	return nil
}

func (o *MetadataProvider) Close() {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	logging.Infof("MetadataProvider is terminated. Cleaning up ...")

	for _, watcher := range o.watchers {
		watcher.close()
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
	fromMetakv, metakvErr := GetIndexerVersionToken()

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
		if err := PostIndexerVersionToken(latestVersion); err != nil {
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
// means synchronization phase is done with the indexer.
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
	func() {
		o.mutex.Lock()
		defer o.mutex.Unlock()

		// make sure watcher is still active. Unwatch metadata could have
		// been called just after watcher.notifyReady has finished.
		if _, ok := o.pendings[tempIndexerId]; !ok {
			watcher.close()
			watcher.cleanupIndices(o.repo)
			return
		}

		o.addWatcherNoLock(watcher, tempIndexerId)
	}()

	logging.Infof("WatchMetadata(): Successfully connected to indexer at %v after retry.", indexAdminPort)

	indexerId := watcher.getIndexerId()
	if callback != nil {
		callback(indexAdminPort, indexerId, tempIndexerId)
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
	go protocol.RunWatcherServerWithRequest(
		s.leaderAddr,
		s,
		s,
		s.factory,
		s.killch,
		readych,
		s.alivech,
		s.pingch)

	return s, readych
}

func (o *MetadataProvider) FindIndexIgnoreStatus(id c.IndexDefnId) *IndexMetadata {

	indices, _ := o.repo.listAllDefn()
	if meta, ok := indices[id]; ok {
		return meta
	}

	return nil
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

	if len(result) != 0 {
		return result, nil
	}

	return nil, errors.New("Cannot find indexer node with index.")
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

func (o *MetadataProvider) getWatcherAddr(MetadataProviderId string) (string, error) {

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	if len(addrs) == 0 {
		return "", errors.New("MetadataProvider.getWatcherAddr() : No network address is available")
	}

	for _, addr := range addrs {
		switch s := addr.(type) {
		case *net.IPAddr:
			if s.IP.IsGlobalUnicast() {
				return fmt.Sprintf("%s:indexer:MetadataProvider:%s", addr.String(), MetadataProviderId), nil
			}
		case *net.IPNet:
			if s.IP.IsGlobalUnicast() {
				return fmt.Sprintf("%s:indexer:MetadataProvider:%s", addr.String(), MetadataProviderId), nil
			}
		}
	}

	return "", errors.New("Fail to find an IP address")
}

func (o *MetadataProvider) findWatcherByNodeAddr(nodeAddr string) *watcher {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	for _, watcher := range o.watchers {
		if strings.ToLower(watcher.getNodeAddr()) == strings.ToLower(nodeAddr) {
			return watcher
		}
	}

	return nil
}

//
// This function returns true as long as there is a
// valid index instance on a connected indexer/watcher.
//
func (o *MetadataProvider) isValidIndexFromActiveIndexer(meta *IndexMetadata) bool {
	o.mutex.RLock()
	defer o.mutex.RUnlock()

	if !isValidIndex(meta) {
		return false
	}

	for _, inst := range meta.Instances {
		if isValidIndexInst(inst) && o.isActiveWatcherNoLock(inst.IndexerId) {
			return true
		}
	}

	for _, inst := range meta.InstsInRebalance {
		if isValidIndexInst(inst) && o.isActiveWatcherNoLock(inst.IndexerId) {
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
func (o *MetadataProvider) refreshStats(stats map[c.IndexInstId]c.Statistics) {

	if o.statsNotifyCh != nil {
		select {
		case o.statsNotifyCh <- stats:
		default:
		}
	}
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

///////////////////////////////////////////////////////
// private function : metadataRepo
///////////////////////////////////////////////////////

func newMetadataRepo(provider *MetadataProvider) *metadataRepo {

	return &metadataRepo{
		definitions: make(map[c.IndexDefnId]*c.IndexDefn),
		instances:   make(map[c.IndexDefnId]map[c.IndexInstId]map[uint64]*IndexInstDistribution),
		indices:     make(map[c.IndexDefnId]*IndexMetadata),
		topology:    make(map[c.IndexerId]map[c.IndexDefnId]bool),
		version:     uint64(0),
		provider:    provider,
	}
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

			tmp := &IndexMetadata{
				Definition:       meta.Definition,
				State:            meta.State,
				Error:            meta.Error,
				Instances:        insts,
				InstsInRebalance: instsInRebalance,
			}

			result[id] = tmp
		}
	}

	return result, r.getVersion()
}

func (r *metadataRepo) listDefnWithValidInst() (map[c.IndexDefnId]*IndexMetadata, uint64) {

	r.mutex.RLock()
	defer r.mutex.RUnlock()

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

			tmp := &IndexMetadata{
				Definition:       meta.Definition,
				State:            meta.State,
				Error:            meta.Error,
				Instances:        insts,
				InstsInRebalance: instsInRebalance,
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
	if _, ok := r.definitions[defn.DefnId]; !ok {
		r.definitions[defn.DefnId] = defn
		r.indices[defn.DefnId] = r.makeIndexMetadata(defn)

		r.updateIndexMetadataNoLock(defn.DefnId)
		r.incrementVersion()
	}
}

func (r *metadataRepo) findRecentValidIndexInstNoLock(defnId c.IndexDefnId) []*IndexInstDistribution {

	var recent []*IndexInstDistribution

	count := 0
	instsByInstId := r.instances[defnId]
	for _, instsByVersion := range instsByInstId {
		var chosen *IndexInstDistribution
		for _, inst := range instsByVersion {

			count++

			// Do not filter out CREATED index, even though it is not a "transient"
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

				if chosen == nil || inst.Version > chosen.Version { // recent
					chosen = inst
				}
			}
		}

		if chosen != nil {
			recent = append(recent, chosen)
		}
	}

	logging.Debugf("defnId %v has (%v total instances) and (%v recent and active instances)", defnId, count, len(recent))
	return recent
}

// Only Consider instance with Active RState
func (r *metadataRepo) hasDefnIgnoreStatus(indexerId c.IndexerId, defnId c.IndexDefnId) bool {

	r.mutex.RLock()
	defer r.mutex.RUnlock()

	meta, ok := r.indices[defnId]
	if ok && meta != nil {
		for _, inst := range meta.Instances {
			if inst != nil && inst.IndexerId == indexerId {
				return true
			}
		}
	}

	return false
}

// Only Consider instance with Active RState
func (r *metadataRepo) hasDefnMatchingStatus(defnId c.IndexDefnId, status []c.IndexState) bool {

	r.mutex.RLock()
	defer r.mutex.RUnlock()

	if meta, ok := r.indices[defnId]; ok && meta != nil && len(meta.Instances) != 0 {
		for _, s := range status {
			for _, inst := range meta.Instances {
				if inst.State == s {
					return true
				}
			}
		}
	}
	return false
}

// Only Consider instance with Active RState
func (r *metadataRepo) getDefnError(defnId c.IndexDefnId) error {

	r.mutex.RLock()
	defer r.mutex.RUnlock()

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
				if isValidIndexInst(inst) && inst.IndexerId == indexerId {
					count++
					break
				}
			}
			for _, inst := range meta.InstsInRebalance {
				if isValidIndexInst(inst) && inst.IndexerId == indexerId {
					count++
					break
				}
			}
		}
	}

	return count
}

func (r *metadataRepo) removeInstForIndexerNoLock(indexerId c.IndexerId, bucket string) {

	newInstsByDefnId := make(map[c.IndexDefnId]map[c.IndexInstId]map[uint64]*IndexInstDistribution)
	for defnId, instsByDefnId := range r.instances {
		defn := r.definitions[defnId]

		newInstsByInstId := make(map[c.IndexInstId]map[uint64]*IndexInstDistribution)
		for instId, instsByInstId := range instsByDefnId {

			newInstsByVersion := make(map[uint64]*IndexInstDistribution)
			for version, instByVersion := range instsByInstId {
				instIndexerId := instByVersion.findIndexerId()
				if instIndexerId == string(indexerId) && (len(bucket) == 0 || (defn != nil && defn.Bucket == bucket)) {
					logging.Debugf("remove index for indexerId : defnId %v instId %v indexerId %v", defnId, instId, instIndexerId)
				} else {
					newInstsByVersion[version] = instByVersion
				}
			}
			if len(newInstsByVersion) != 0 {
				newInstsByInstId[instId] = newInstsByVersion
			}
		}
		if len(newInstsByInstId) != 0 {
			newInstsByDefnId[defnId] = newInstsByInstId
		}
	}

	r.instances = newInstsByDefnId
}

func (r *metadataRepo) cleanupOrphanDefnNoLock(indexerId c.IndexerId, bucket string) {

	deleteDefn := ([]c.IndexDefnId)(nil)

	for defnId, _ := range r.topology[indexerId] {
		if defn, ok := r.definitions[defnId]; ok {
			if len(bucket) == 0 || defn.Bucket == bucket {

				if len(r.instances[defnId]) == 0 {
					deleteDefn = append(deleteDefn, defnId)
				}
			}
		} else {
			logging.Verbosef("Find orphan index %v in topology but watcher has not recieved corresponding definition", defnId)
		}
	}

	for _, defnId := range deleteDefn {
		logging.Verbosef("removing orphan defn with no instance %v", defnId)
		delete(r.definitions, defnId)
		delete(r.instances, defnId)
		delete(r.indices, defnId)
		delete(r.topology[indexerId], defnId)
	}

	if len(r.topology[indexerId]) == 0 {
		delete(r.topology, indexerId)
	}
}

func (r *metadataRepo) cleanupIndicesForIndexer(indexerId c.IndexerId) {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.removeInstForIndexerNoLock(indexerId, "")
	r.cleanupOrphanDefnNoLock(indexerId, "")

	for defnId, _ := range r.indices {
		logging.Debugf("update topology during cleanup: defn %v", defnId)
		r.updateIndexMetadataNoLock(defnId)
	}
}

func (r *metadataRepo) updateTopology(topology *IndexTopology, indexerId c.IndexerId) {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	if len(indexerId) == 0 {
		indexerId = c.IndexerId(topology.findIndexerId())
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
		r.removeInstForIndexerNoLock(indexerId, topology.Bucket)
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
				r.instances[defnId] = make(map[c.IndexInstId]map[uint64]*IndexInstDistribution)
			}
			if _, ok := r.instances[defnId][c.IndexInstId(instRef.InstId)]; !ok {
				r.instances[defnId][c.IndexInstId(instRef.InstId)] = make(map[uint64]*IndexInstDistribution)
			}
			r.instances[defnId][c.IndexInstId(instRef.InstId)][instRef.Version] = &instRef
		}

		logging.Debugf("update Topology: defn %v", defnId)
		r.updateIndexMetadataNoLock(defnId)
	}

	if len(indexerId) != 0 {
		r.cleanupOrphanDefnNoLock(indexerId, topology.Bucket)
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
	r.addDefn(defn)
	return nil
}

func (r *metadataRepo) unmarshallAndUpdateTopology(content []byte, indexerId c.IndexerId) error {

	topology, err := unmarshallIndexTopology(content)
	if err != nil {
		return err
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
	}
}

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

	chosens := r.findRecentValidIndexInstNoLock(defnId)
	for _, inst := range chosens {
		idxInst := r.makeInstanceDefn(defnId, inst)

		if idxInst.State > meta.State {
			meta.State = idxInst.State
		}

		if idxInst.Error != "" {
			meta.Error += idxInst.Error + "\n"
		}

		meta.Instances = append(meta.Instances, idxInst)
	}

	logging.Debugf("update index metadata: index definition %v has %v active instances.", defnId, len(meta.Instances))
}

func (r *metadataRepo) makeInstanceDefn(defnId c.IndexDefnId, inst *IndexInstDistribution) *InstanceDefn {

	idxInst := new(InstanceDefn)
	idxInst.DefnId = defnId
	idxInst.InstId = c.IndexInstId(inst.InstId)
	idxInst.State = c.IndexState(inst.State)
	idxInst.Error = inst.Error
	idxInst.Version = inst.Version
	idxInst.RState = inst.RState
	idxInst.ReplicaId = inst.ReplicaId
	idxInst.StorageMode = inst.StorageMode

	for _, partition := range inst.Partitions {
		for _, slice := range partition.SinglePartition.Slices {
			idxInst.IndexerId = c.IndexerId(slice.IndexerId)
			break
		}
	}

	return idxInst
}

func (r *metadataRepo) updateRebalanceInstancesInIndexMetadata(defnId c.IndexDefnId, meta *IndexMetadata) {

	meta.InstsInRebalance = nil

	instsByInstId := r.instances[defnId]
	for _, instsByVersion := range instsByInstId {
		for _, inst := range instsByVersion {
			hasActiveRstate := false
			isMoreRecent := false
			for _, current := range meta.Instances {
				// meta.Instances contains instance with active RState.
				if current.InstId == c.IndexInstId(inst.InstId) {
					hasActiveRstate = true

					// only consider higher version
					if current.Version < inst.Version {
						if inst.RState == uint32(c.REBAL_PENDING) {
							isMoreRecent = true
						} else {
							logging.Warnf("Encounter an index instance with higher version than active instance: defnId %v instId %v version %v rstate %v",
								defnId, inst.InstId, inst.Version, inst.RState)
						}
					}
				}
			}

			// Add this instance to "future topology" if it does not have another instance with
			// active Rstate or it is more recent than the active Rstate instance.
			if !hasActiveRstate || isMoreRecent {

				if c.IndexState(inst.State) == c.INDEX_STATE_INITIAL ||
					c.IndexState(inst.State) == c.INDEX_STATE_CATCHUP ||
					c.IndexState(inst.State) == c.INDEX_STATE_ACTIVE {

					idxInst := r.makeInstanceDefn(defnId, inst)
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
					if !hasActiveRstate {

						// Promote if all watchers are running/synchronzied with indexers.  If this function
						// returns true, it means the cluster is not under topology change nor process under
						// bootstrap.   Note that once watcher is synchronized, it keeps a copy of the metadata
						// in memory until being unwatched.
						if r.provider.allWatchersRunningNoLock() {
							logging.Debugf("update update metadata: promote instance state %v %v", defnId, inst.InstId)
							idxInst.State = c.INDEX_STATE_ACTIVE
						}

						if idxInst.State > meta.State {
							meta.State = idxInst.State
						}
					}
				}
			}
		}
	}

	logging.Debugf("update update metadata: index definition %v has %v instances under rebalance.", defnId, len(meta.InstsInRebalance))
}

func (r *metadataRepo) resolveIndexStats(indexerId c.IndexerId, stats c.Statistics) map[c.IndexInstId]c.Statistics {

	r.mutex.RLock()
	defer r.mutex.RUnlock()

	if len(indexerId) == 0 {
		return (map[c.IndexInstId]c.Statistics)(nil)
	}

	result := make(map[c.IndexInstId]c.Statistics)

	// if the index is being rebalanced, this will only look at the active instance with the highest instance version.
	for _, meta := range r.indices {
		for _, inst := range meta.Instances {
			name := c.FormatIndexInstDisplayName(meta.Definition.Name, int(inst.ReplicaId))
			prefix := fmt.Sprintf("%s:%s:", meta.Definition.Bucket, name)

			for statName, statVal := range stats {
				if strings.HasPrefix(statName, prefix) {
					if inst.IndexerId == indexerId {
						key := strings.TrimPrefix(statName, prefix)
						if _, ok := result[inst.InstId]; !ok {
							result[inst.InstId] = make(c.Statistics)
						}
						result[inst.InstId].Set(key, statVal)
					}
				}
			}
		}
	}

	return result
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
	s.notifiers = make(map[c.IndexDefnId]*event)
	s.isClosed = false
	s.lastSeenTxid = common.Txnid(0)

	return s
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

	return needRefresh
}

func (w *watcher) updateIndexStatsNoLock(indexerId c.IndexerId, indexStats *IndexStats) map[c.IndexInstId]c.Statistics {

	stats := (map[c.IndexInstId]c.Statistics)(nil)
	if indexStats != nil && len(indexStats.Stats) != 0 {
		stats = w.provider.repo.resolveIndexStats(indexerId, indexStats.Stats)
		if len(stats) == 0 {
			stats = nil
		}
	}

	return stats
}

func (w *watcher) waitForEvent(defnId c.IndexDefnId, status []c.IndexState) error {

	event := &event{defnId: defnId, status: status, notifyCh: make(chan error, 1)}
	if w.registerEvent(event) {
		logging.Debugf("watcher.waitForEvent(): wait event : id %v status %v", event.defnId, event.status)
		err, ok := <-event.notifyCh
		if ok && err != nil {
			logging.Debugf("watcher.waitForEvent(): wait arrives : id %v status %v", event.defnId, event.status)
			return err
		}
	}
	return nil
}

func (w *watcher) registerEvent(event *event) bool {

	// by locking the watcher, it will not process any commit
	// while this function is executing
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if !w.provider.repo.hasDefnMatchingStatus(event.defnId, event.status) {
		logging.Debugf("watcher.registerEvent(): add event : id %v status %v", event.defnId, event.status)
		w.notifiers[event.defnId] = event
		return true
	}

	logging.Debugf("watcher.registerEvent(): found event existed: id %v status %v", event.defnId, event.status)
	return false
}

func (w *watcher) notifyEventNoLock() {

	for defnId, event := range w.notifiers {
		if w.provider.repo.hasDefnMatchingStatus(defnId, event.status) {
			delete(w.notifiers, defnId)
			close(event.notifyCh)
		} else if err := w.provider.repo.getDefnError(defnId); err != nil {
			delete(w.notifiers, defnId)
			event.notifyCh <- err
			close(event.notifyCh)
		}
	}
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

	for key, event := range w.notifiers {
		delete(w.notifiers, key)
		event.notifyCh <- errors.New("Terminate Request due to client termination")
		close(event.notifyCh)
	}
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
	return strings.Contains(key, "IndexStats")
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

func (w *watcher) processChange(txid common.Txnid, op uint32, key string, content []byte, indexerId c.IndexerId) (bool, map[c.IndexInstId]c.Statistics, error) {

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
			w.notifyEventNoLock()

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
			w.notifyEventNoLock()

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
			w.notifyEventNoLock()

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
