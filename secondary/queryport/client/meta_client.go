package client

import "sync"
import "fmt"
import "sort"
import "errors"
import "strings"
import "math/rand"
import "time"
import "unsafe"
import "sync/atomic"
import "encoding/json"

import "github.com/couchbase/indexing/secondary/logging"
import common "github.com/couchbase/indexing/secondary/common"
import mclient "github.com/couchbase/indexing/secondary/manager/client"

type metadataClient struct {
	cluster  string
	finch    chan bool
	mdClient *mclient.MetadataProvider
	indexers unsafe.Pointer // *indexTopology
	// config
	servicesNotifierRetryTm int
	logtick                 time.Duration
	randomWeight            float64 // value between [0, 1.0)
	equivalenceFactor       float64 // value between [0, 1.0)

	topoChangeLock sync.Mutex
	metaCh         chan bool
}

// sherlock topology management, multi-node & single-partition.
type indexTopology struct {
	version    uint64
	adminports map[string]common.IndexerId // book-keeping for cluster changes
	topology   map[common.IndexerId][]*mclient.IndexMetadata
	replicas   map[common.IndexDefnId][]common.IndexDefnId
	rw         sync.RWMutex
	loads      map[common.IndexDefnId]loadHeuristics
}

func newMetaBridgeClient(
	cluster string, config common.Config, metaCh chan bool) (c *metadataClient, err error) {

	b := &metadataClient{
		cluster: cluster,
		finch:   make(chan bool),
		metaCh:  metaCh,
	}
	b.servicesNotifierRetryTm = config["servicesNotifierRetryTm"].Int()
	b.logtick = time.Duration(config["logtick"].Int()) * time.Millisecond
	b.randomWeight = config["load.randomWeight"].Float64()
	b.equivalenceFactor = config["load.equivalenceFactor"].Float64()
	// initialize meta-data-provide.
	uuid, err := common.NewUUID()
	if err != nil {
		logging.Errorf("Could not generate UUID in common.NewUUID\n")
		return nil, err
	}
	b.mdClient, err = mclient.NewMetadataProvider(uuid.Str())
	if err != nil {
		return nil, err
	}

	if err := b.updateIndexerList(false); err != nil {
		logging.Errorf("updateIndexerList(): %v\n", err)
		b.mdClient.Close()
		return nil, err
	}

	go b.watchClusterChanges() // will also update the indexer list
	go b.logstats()
	return b, nil
}

// Sync will update the indexer list.
func (b *metadataClient) Sync() error {
	if err := b.updateIndexerList(true); err != nil {
		logging.Errorf("updateIndexerList(): %v\n", err)
		return err
	}
	return nil
}

// Refresh implement BridgeAccessor{} interface.
func (b *metadataClient) Refresh() ([]*mclient.IndexMetadata, error) {
	mindexes, version := b.mdClient.ListIndex()
	if b.hasIndexesChanged(mindexes, version) {
		currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
		b.safeupdate(currmeta.adminports, false)
	}
	return mindexes, nil
}

// Nodes implement BridgeAccessor{} interface.
func (b *metadataClient) Nodes() ([]*IndexerService, error) {
	// gather Indexer services
	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
	nodes := make(map[string]*IndexerService)
	for indexerID := range currmeta.topology {
		if indexerID != common.INDEXER_ID_NIL {
			a, q, err := b.mdClient.FindServiceForIndexer(indexerID)
			if err == nil {
				nodes[a] = &IndexerService{
					Adminport: a, Queryport: q, Status: "initial",
				}
			}
		}
	}
	// gather indexer status
	for _, indexer := range b.mdClient.CheckIndexerStatus() {
		if node, ok := nodes[indexer.Adminport]; ok && indexer.Connected {
			node.Status = "online"
		}
	}
	services := make([]*IndexerService, 0, len(nodes))
	for _, node := range nodes {
		services = append(services, node)
	}
	return services, nil
}

// GetIndexDefn implements BridgeAccessor{} interface.
func (b *metadataClient) GetIndexDefn(defnID uint64) *common.IndexDefn {
	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
	for _, indexes := range currmeta.topology {
		for _, index := range indexes {
			if defnID == uint64(index.Definition.DefnId) {
				return index.Definition
			}
		}
	}
	return nil
}

// CreateIndex implements BridgeAccessor{} interface.
func (b *metadataClient) CreateIndex(
	indexName, bucket, using, exprType, partnExpr, whereExpr string,
	secExprs []string, isPrimary bool,
	planJSON []byte) (uint64, error) {

	plan := make(map[string]interface{})
	if planJSON != nil && len(planJSON) > 0 {
		err := json.Unmarshal(planJSON, &plan)
		if err != nil {
			return 0, err
		}
	}

	refreshCnt := 0
RETRY:
	defnID, err, needRefresh := b.mdClient.CreateIndexWithPlan(
		indexName, bucket, using, exprType, partnExpr, whereExpr,
		secExprs, isPrimary, plan)

	if needRefresh && refreshCnt == 0 {
		fmsg := "GsiClient: Indexer Node List is out-of-date.  Require refresh."
		logging.Debugf(fmsg)
		if err := b.updateIndexerList(false); err != nil {
			logging.Errorf("updateIndexerList(): %v\n", err)
			return uint64(defnID), err
		}
		refreshCnt++
		goto RETRY
	}
	return uint64(defnID), err
}

// BuildIndexes implements BridgeAccessor{} interface.
func (b *metadataClient) BuildIndexes(defnIDs []uint64) error {
	_, ok := b.getNodes(defnIDs)
	if !ok {
		return ErrorIndexNotFound
	}
	ids := make([]common.IndexDefnId, len(defnIDs))
	for i, id := range defnIDs {
		ids[i] = common.IndexDefnId(id)
	}
	return b.mdClient.BuildIndexes(ids)
}

// DropIndex implements BridgeAccessor{} interface.
func (b *metadataClient) DropIndex(defnID uint64) error {
	err := b.mdClient.DropIndex(common.IndexDefnId(defnID))
	if err == nil { // cleanup index local cache.
		currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
		b.safeupdate(currmeta.adminports, true /*force*/)
	}
	return err
}

// GetScanports implements BridgeAccessor{} interface.
func (b *metadataClient) GetScanports() (queryports []string) {
	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
	queryports = make([]string, 0)
	for indexerID := range currmeta.topology {
		if indexerID != common.INDEXER_ID_NIL {
			_, queryport, err := b.mdClient.FindServiceForIndexer(indexerID)
			if err == nil {
				queryports = append(queryports, queryport)
			}
		}
	}
	logging.Debugf("Scan ports %v for all indexes", queryports)
	return queryports
}

// GetScanport implements BridgeAccessor{} interface.
func (b *metadataClient) GetScanport(
	defnID uint64, retry int, excludes map[uint64]bool) (qp string, targetDefnID uint64, ok bool) {

	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
	if rand.Float64() < b.randomWeight {
		var replicas [128]uint64
		n := 0
		for _, replicaID := range currmeta.replicas[common.IndexDefnId(defnID)] {
			replicas[n] = uint64(replicaID)
			n++
		}
		targetDefnID, ok = b.pickRandom(replicas[:n], defnID, excludes)
	} else {
		targetDefnID, ok = b.pickOptimal(defnID, excludes)
	}

	if !ok {
		return "", 0, false
	}

	_, qp, err := b.mdClient.FindServiceForIndex(common.IndexDefnId(targetDefnID))
	if err != nil {
		return "", 0, false
	}
	fmsg := "Scan port %s for index defnID %d of equivalent index defnId %d"
	logging.Debugf(fmsg, qp, targetDefnID, defnID)
	return qp, targetDefnID, true
}

// Timeit implement BridgeAccessor{} interface.
func (b *metadataClient) Timeit(defnID uint64, value float64) {
	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))

	currmeta.rw.Lock()
	defer currmeta.rw.Unlock()

	id := common.IndexDefnId(defnID)
	if load, ok := currmeta.loads[id]; !ok {
		currmeta.loads[id] = loadHeuristics{avgLoad: value}
	} else {
		// compute incremental average.
		load.avgLoad = (load.avgLoad + float64(value)) / 2.0
		currmeta.loads[id] = load
	}
}

// IsPrimary implement BridgeAccessor{} interface.
func (b *metadataClient) IsPrimary(defnID uint64) bool {
	b.Refresh()
	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
	for _, indexes := range currmeta.topology {
		for _, index := range indexes {
			if index.Definition.DefnId == common.IndexDefnId(defnID) {
				return index.Definition.IsPrimary
			}
		}
	}
	return false
}

// IndexState implement BridgeAccessor{} interface.
func (b *metadataClient) IndexState(defnID uint64) (common.IndexState, error) {
	b.Refresh()
	return b.indexState(defnID)
}

// close this bridge, to be called when a new indexer is added or
// an active indexer leaves the cluster or during system shutdown.
func (b *metadataClient) Close() {
	defer func() { recover() }() // in case async Close is called.
	b.mdClient.Close()
	close(b.finch)
}

//--------------------------------
// local functions to map replicas
//--------------------------------

// compute a map of replicas for each index in 2i.
func (b *metadataClient) computeReplicas(
	topo map[common.IndexerId][]*mclient.IndexMetadata) map[common.IndexDefnId][]common.IndexDefnId {

	replicaMap := make(map[common.IndexDefnId][]common.IndexDefnId)
	for id1, indexes1 := range topo {
		for _, index1 := range indexes1 {
			replicas := make([]common.IndexDefnId, 0)
			replicas = append(replicas, index1.Definition.DefnId) // add itself
			for id2, indexes2 := range topo {
				if id1 == id2 { // skip colocated indexes
					continue
				}
				for _, index2 := range indexes2 {
					if b.equivalentIndex(index1, index2) { // pick equivalents
						replicas = append(replicas, index2.Definition.DefnId)
					}
				}
			}
			replicaMap[index1.Definition.DefnId] = replicas // map it
		}
	}
	return replicaMap
}

// compare whether two index are equivalent.
func (b *metadataClient) equivalentIndex(
	index1, index2 *mclient.IndexMetadata) bool {

	d1, d2 := index1.Definition, index2.Definition
	if d1.Using != d1.Using ||
		d1.Bucket != d2.Bucket ||
		d1.IsPrimary != d2.IsPrimary ||
		d1.ExprType != d2.ExprType ||
		d1.PartitionScheme != d2.PartitionScheme ||
		d1.PartitionKey != d2.PartitionKey ||
		d1.WhereExpr != d2.WhereExpr {

		return false
	}

	for _, s1 := range d1.SecExprs {
		for _, s2 := range d2.SecExprs {
			if s1 != s2 {
				return false
			}
		}
	}
	return true
}

//--------------------------------------
// local functions to work with replicas
//--------------------------------------

// manage load statistics.
type loadHeuristics struct {
	avgLoad float64
}

// pick a random replica from the list.
func (b *metadataClient) pickRandom(replicas []uint64, defnID uint64, excludes map[uint64]bool) (uint64, bool) {
	var actvReplicas [128]uint64
	n := 0
	for _, replicaID := range replicas {
		state, _ := b.indexState(uint64(replicaID))
		if state == common.INDEX_STATE_ACTIVE && (excludes == nil || !excludes[uint64(replicaID)]) {
			actvReplicas[n] = uint64(replicaID)
			n++
		}
	}

	if n > 0 {
		return actvReplicas[rand.Intn(n*10)%n], true
	}

	if excludes == nil || !excludes[uint64(defnID)] {
		return defnID, true
	}

	return uint64(0), false
}

// pick an optimal replica for the index `defnID` under least load.
func (b *metadataClient) pickOptimal(defnID uint64, excludes map[uint64]bool) (uint64, bool) {
	// gather active-replicas
	var actvReplicas [128]uint64
	var loadList [128]float64
	n := 0
	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
	for _, replicaID := range currmeta.replicas[common.IndexDefnId(defnID)] {
		state, _ := b.indexState(uint64(replicaID))
		if state == common.INDEX_STATE_ACTIVE && (excludes == nil || !excludes[uint64(replicaID)]) {
			actvReplicas[n] = uint64(replicaID)
			currmeta.rw.RLock()
			load, ok := currmeta.loads[replicaID]
			currmeta.rw.RUnlock()
			if !ok {
				loadList[n] = 0.0
			} else {
				loadList[n] = load.avgLoad
			}
			n++
		}
	}
	if n == 0 { // none are active
		if excludes == nil || !excludes[uint64(defnID)] {
			return defnID, true
		}
		return uint64(0), false
	}
	// compute replica with least load.
	sort.Float64s(loadList[:n])
	leastLoad := loadList[0]

	var replicas [128]uint64
	// gather list of replicas with equivalent load
	m := 0
	for _, replicaID := range actvReplicas[:n] {
		currmeta.rw.RLock()
		load, ok := currmeta.loads[common.IndexDefnId(replicaID)]
		currmeta.rw.RUnlock()
		if !ok || (load.avgLoad*b.equivalenceFactor <= leastLoad) {
			replicas[m] = replicaID
			m++
		}
	}
	return b.pickRandom(replicas[:m], defnID, excludes)
}

//----------------
// local functions
//----------------

func (b *metadataClient) logstats() {
	tick := time.NewTicker(b.logtick)
	defer func() {
		tick.Stop()
	}()

loop:
	for {
		<-tick.C
		s := make([]string, 0, 16)
		currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
		func() {
			logging.Infof("connected with %v indexers\n", len(currmeta.topology))
			for id, replicas := range currmeta.replicas {
				logging.Infof("index %v has %v replicas\n", id, len(replicas))
			}
			currmeta.rw.RLock()
			defer currmeta.rw.RUnlock()
			for id, load := range currmeta.loads {
				s = append(s, fmt.Sprintf(`"%v": %v`, id, load.avgLoad))
			}
			logging.Infof("client load stats {%v}", strings.Join(s, ","))
		}()
		select {
		case _, ok := <-b.finch:
			if !ok {
				break loop
			}
		default:
		}
	}
}

// unprotected access to shared structures.
func (b *metadataClient) indexState(defnID uint64) (common.IndexState, error) {
	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
	for _, indexes := range currmeta.topology {
		for _, index := range indexes {
			if index.Definition.DefnId == common.IndexDefnId(defnID) {
				if index.Instances != nil && len(index.Instances) > 0 {
					state := index.Instances[0].State
					if len(index.Instances) == 0 {
						err := fmt.Errorf("no instance for %q", defnID)
						return state, err
					} else if index.Instances[0].Error != "" {
						return state, errors.New(index.Instances[0].Error)
					} else {
						return state, nil
					}
				}
				return common.INDEX_STATE_ERROR, ErrorInstanceNotFound
			}
		}
	}
	return common.INDEX_STATE_ERROR, ErrorIndexNotFound
}

// getNodes return the set of nodes hosting the specified set
// of indexes
func (b *metadataClient) getNodes(defnIDs []uint64) ([]string, bool) {
	adminports := make([]string, 0)
	for _, defnID := range defnIDs {
		adminport, ok := b.getNode(defnID)
		if !ok {
			return nil, false
		}
		adminports = append(adminports, adminport)
	}
	return adminports, true
}

// getNode hosting index with `defnID`.
func (b *metadataClient) getNode(defnID uint64) (adminport string, ok bool) {
	aport, _, err := b.mdClient.FindServiceForIndex(common.IndexDefnId(defnID))
	if err != nil {
		return "", false
	}
	return aport, true
}

// update 2i cluster information,
func (b *metadataClient) updateIndexerList(discardExisting bool) error {

	clusterURL, err := common.ClusterAuthUrl(b.cluster)
	if err != nil {
		return err
	}
	cinfo, err := common.NewClusterInfoCache(clusterURL, "default")
	if err != nil {
		return err
	}
	if err := cinfo.Fetch(); err != nil {
		return err
	}

	// UpdateIndexerList is synchronous, except for async callback from WatchMetadata() -- when indexer is
	// not responding fast enough.
	// TopoChangeLock is to protect the updates to index topology made by async callack.   Index topology contains
	// the assignment between admniport and indexerId -- which is udpated by async callback.   Metadata version cannot
	// enforce serializability of such assigment update, since it is not part of metadata tracked by metadataProvider.
	//
	// The adminport-indexerId assignment is used to figure out difference in topology so that gsiClient can call
	// WatchMetadata and UnwatchMetadata properly.   Corruption of such assignment can cause a lot of issue.
	//
	// The lock is to protect race condition in such case
	// 1) make sure that updateIndexerList() is finished before any its callback is invoked.
	//    This is to ensure async callback does not lose its work.
	// 2) make sure that the async callback is called sequentially so their changes on adminport-indexerId are accumulative.
	//    This is to ensure async callback does not lose its work.
	// 3) if there are consecutive topology changes by ns-server, make sure that async callback will not save a stale
	//    adminport-indexerId assignment by overwriting the assignment created by second topology changes.
	b.topoChangeLock.Lock()
	defer b.topoChangeLock.Unlock()

	// populate indexers' adminport and queryport
	adminports, err := getIndexerAdminports(cinfo)
	if err != nil {
		return err
	}

	fmsg := "Refreshing indexer list due to cluster changes or auto-refresh."
	logging.Infof(fmsg)
	logging.Infof("Refreshed Indexer List: %v", adminports)

	var curradmns map[string]common.IndexerId
	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
	if currmeta != nil {
		curradmns = make(map[string]common.IndexerId)
		for adminport, indexerId := range currmeta.adminports {
			curradmns[adminport] = indexerId
		}
	}

	if discardExisting {
		for _, indexerID := range curradmns {
			b.mdClient.UnwatchMetadata(indexerID)
		}
		curradmns = make(map[string]common.IndexerId)
	}

	// watch all indexers
	m := make(map[string]common.IndexerId)
	for _, adminport := range adminports { // add new indexer-nodes if any
		if indexerID, ok := curradmns[adminport]; !ok {
			// This adminport is provided by cluster manager.  Meta client will
			// honor cluster manager to treat this adminport as a healthy node.
			// If the indexer is unavail during initialization, WatchMetadata()
			// will return afer timeout. A background watcher will keep
			// retrying, since it can be tranisent partitioning error.
			// If retry eventually successful, this callback will be invoked
			// to update meta_client. The metadata client has to rely on the
			// cluster manager to send a notification if this node is detected
			// to be down, such that the metadata client can stop the
			// background watcher.
			fn := func(ad string, n_id common.IndexerId, o_id common.IndexerId) {
				// following time.Sleep() ensures that bootstrap initialization
				// completes before other async activities into metadataClient{}
				currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
				for currmeta == nil {
					time.Sleep(10 * time.Millisecond)
					currmeta = (*indexTopology)(atomic.LoadPointer(&b.indexers))
				}
				b.updateIndexer(ad, n_id, o_id)
			}

			// WatchMetadata will "unwatch" an old metadata watcher which
			// shares the same indexer Id (but the adminport may be different).
			indexerID = b.mdClient.WatchMetadata(adminport, fn)
			m[adminport] = indexerID
		} else {
			err = b.mdClient.UpdateServiceAddrForIndexer(indexerID, adminport)
			m[adminport] = indexerID
			delete(curradmns, adminport)
		}
	}
	// delete indexer-nodes that got removed from cluster.
	for _, indexerID := range curradmns {
		// check if the indexerId exists in var "m".  In case the
		// adminport changes for the same index node, there would
		// be two adminport mapping to the same indexerId, one
		// in b.adminport (old) and the other in "m" (new).  So
		// make sure not to accidently unwatch the indexer.
		found := false
		for _, id := range m {
			if indexerID == id {
				found = true
			}
		}
		if !found {
			b.mdClient.UnwatchMetadata(indexerID)
		}
	}
	b.safeupdate(m, true /*force*/)
	return err
}

func (b *metadataClient) updateIndexer(
	adminport string, newIndexerId, oldIndexerId common.IndexerId) {

	func() {
		b.topoChangeLock.Lock()
		defer b.topoChangeLock.Unlock()

		adminports := make(map[string]common.IndexerId)
		currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))
		for admnport, indexerId := range currmeta.adminports {
			adminports[admnport] = indexerId
		}

		// UpdateIndexer is a async call.  When this is invoked, currmeta.adminports may be different.
		if _, ok := adminports[adminport]; ok {
			logging.Infof(
				"Acknowledged that new indexer is registered.  Indexer = %v, id = %v",
				adminport, newIndexerId)
			adminports[adminport] = newIndexerId
			b.safeupdate(adminports, true /*force*/)
		} else {
			logging.Infof(
				"New indexer registration is skipped.  Indexer may have been rebalanced out (unwatch).  Indexer = %v, id = %v",
				adminport, newIndexerId)
		}
	}()
}

// replicas:
//		Refresh
//		deleteIndex
// loads:
//		Refresh
//		deleteIndex
// topology:
//		Refresh
//		deleteIndex
//
//		Timeit for b.loads

func (b *metadataClient) updateTopology(
	adminports map[string]common.IndexerId, force bool) *indexTopology {

	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))

	mindexes, version := b.mdClient.ListIndex()
	// detect change in indexer cluster or indexes.
	if force == false && currmeta != nil &&
		(!b.hasIndexersChanged(adminports) &&
			!b.hasIndexesChanged(mindexes, version)) {
		return currmeta
	}
	// create a new topology.
	newmeta := &indexTopology{
		version:    version,
		adminports: make(map[string]common.IndexerId),
		topology:   make(map[common.IndexerId][]*mclient.IndexMetadata),
		replicas:   make(map[common.IndexDefnId][]common.IndexDefnId),
	}
	// adminport
	for adminport, indexerID := range adminports {
		newmeta.adminports[adminport] = indexerID
		newmeta.topology[indexerID] = make([]*mclient.IndexMetadata, 0, 16)
	}
	// topology
	for _, mindex := range mindexes {
		index := *mindex
		for _, instance := range index.Instances {
			indexes, ok := newmeta.topology[instance.IndexerId]
			if !ok {
				fmsg := "indexer node %v not available"
				logging.Fatalf(fmsg, instance.IndexerId)
				continue
			}
			newmeta.topology[instance.IndexerId] = append(indexes, &index)
		}
	}
	// replicas
	newmeta.replicas = b.computeReplicas(newmeta.topology)
	// loads
	newmeta.loads = make(map[common.IndexDefnId]loadHeuristics)
	func() {
		if currmeta != nil {
			currmeta.rw.RLock()
			defer currmeta.rw.RUnlock()

			for indexId, load := range currmeta.loads {
				if _, ok := newmeta.replicas[indexId]; ok {
					newmeta.loads[indexId] = load
				}
			}
		}
	}()
	return newmeta
}

func (b *metadataClient) safeupdate(
	adminports map[string]common.IndexerId, force bool) {

	var currmeta, newmeta *indexTopology

	done := false
	for done == false {
		currmeta = (*indexTopology)(atomic.LoadPointer(&b.indexers))
		newmeta = b.updateTopology(adminports, force)
		if currmeta == nil {
			atomic.StorePointer(&b.indexers, unsafe.Pointer(newmeta))
			logging.Infof("initialized currmeta %v\n", newmeta.version)
			return
		} else if force {
			if newmeta.version < currmeta.version {
				fmsg := "skip newmeta %v <= %v\n"
				logging.Infof(fmsg, newmeta.version, currmeta.version)
				return
			}
		} else if newmeta.version <= currmeta.version {
			fmsg := "skip newmeta %v <= %v\n"
			logging.Infof(fmsg, newmeta.version, currmeta.version)
			return
		}
		oldptr := unsafe.Pointer(currmeta)
		newptr := unsafe.Pointer(newmeta)
		done = atomic.CompareAndSwapPointer(&b.indexers, oldptr, newptr)

		// metaCh should never close
		if b.metaCh != nil {
			select {
			case b.metaCh <- true:
			default:
			}
		}
	}
	fmsg := "switched currmeta from %v -> %v\n"
	logging.Infof(fmsg, currmeta.version, newmeta.version)
}

func (b *metadataClient) hasIndexersChanged(
	adminports map[string]common.IndexerId) bool {

	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))

	for adminport, indexerId := range adminports {
		x, ok := currmeta.adminports[adminport]
		if !ok || x != indexerId { // new indexer node detected.
			return true
		}
	}
	for adminport, indexerId := range currmeta.adminports {
		x, ok := adminports[adminport]
		if !ok || x != indexerId { // indexer node dropped out.
			return true
		}
	}
	return false
}

func (b *metadataClient) hasIndexesChanged(
	mindexes []*mclient.IndexMetadata, version uint64) bool {

	currmeta := (*indexTopology)(atomic.LoadPointer(&b.indexers))

	if currmeta.version < version {
		fmsg := "metadata provider version changed %v -> %v\n"
		logging.Infof(fmsg, currmeta.version, version)
		return true
	}

	for _, mindex := range mindexes {
		_, ok := currmeta.replicas[mindex.Definition.DefnId]
		if !ok { // new index detected.
			return true
		}
	}

	for _, iindexes := range currmeta.topology {
	loop:
		for _, index := range iindexes {
			for _, mindex := range mindexes {
				if mindex.Definition.DefnId == index.Definition.DefnId {
					for _, ix := range index.Instances {
						for _, iy := range mindex.Instances {
							if ix.InstId == iy.InstId && ix.State != iy.State {
								return true
							}
						}
					}
					continue loop
				}
			}
			return true
		}
	}
	return false
}

// return adminports for all known indexers.
func getIndexerAdminports(cinfo *common.ClusterInfoCache) ([]string, error) {
	iAdminports := make([]string, 0)
	for _, node := range cinfo.GetNodesByServiceType("indexAdmin") {
		status, err := cinfo.GetNodeStatus(node)
		if err != nil {
			return nil, err
		}
		logging.Verbosef("node %v status: %q", node, status)
		if status == "healthy" || status == "active" || status == "warmup" {
			adminport, err := cinfo.GetServiceAddress(node, "indexAdmin")
			if err != nil {
				return nil, err
			}
			iAdminports = append(iAdminports, adminport)
		} else {
			logging.Warnf("node %v status: %q", node, status)
		}
	}
	return iAdminports, nil
}

// FIXME/TODO: based on discussion with John-
//
//    if we cannot watch the metadata due to network partition we will
//    have an empty list of index and cannot query, in other words
//    the client will tolerate the partition and rejects scans until it
//    is healed.
//    i) alternatively, figure out a way to propagate error that happens
//       with watchClusterChanges() go-routine.
//
//    and while propating error back to the caller
//    1) we can encourage the caller to Refresh() the client hoping for
//       success, or,
//    2) Close() the client and re-create it.
//
//    side-effects of partitioning,
//    a) query cannot get indexes from the indexer node -- so n1ql has
//       to do bucket scan. It is a perf issue.
//    b) Network disconnected after watcher is up. We have the list of
//       indexes -- but we cannot query on it. N1QL should still degrade
//       to bucket scan.

func (b *metadataClient) watchClusterChanges() {
	selfRestart := func() {
		time.Sleep(time.Duration(b.servicesNotifierRetryTm) * time.Millisecond)
		go b.watchClusterChanges()
	}

	clusterURL, err := common.ClusterAuthUrl(b.cluster)
	if err != nil {
		logging.Errorf("common.ClusterAuthUrl(): %v\n", err)
		selfRestart()
		return
	}
	scn, err := common.NewServicesChangeNotifier(clusterURL, "default")
	if err != nil {
		logging.Errorf("common.NewServicesChangeNotifier(): %v\n", err)
		selfRestart()
		return
	}
	defer scn.Close()

	// For observing node services config
	ch := scn.GetNotifyCh()
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				selfRestart()
				return
			} else if err := b.updateIndexerList(false); err != nil {
				logging.Errorf("updateIndexerList(): %v\n", err)
				selfRestart()
				return
			}
		case <-b.finch:
			return
		}
	}
}
