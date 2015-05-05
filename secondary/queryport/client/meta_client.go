package client

import "sync"
import "fmt"
import "errors"
import "time"
import "encoding/json"

import "github.com/couchbase/indexing/secondary/logging"
import common "github.com/couchbase/indexing/secondary/common"
import mclient "github.com/couchbase/indexing/secondary/manager/client"

type metadataClient struct {
	cluster  string
	finch    chan bool
	mdClient *mclient.MetadataProvider
	rw       sync.RWMutex // protects all fields listed below
	// sherlock topology management, multi-node & single-partition.
	adminports map[string]common.IndexerId // book-keeping for cluster changes
	topology   map[common.IndexerId]map[common.IndexDefnId]*mclient.IndexMetadata
	// shelock load replicas.
	replicas map[common.IndexDefnId][]common.IndexDefnId
	// shelock load balancing.
	loads map[common.IndexDefnId]*loadHeuristics // index -> loadHeuristics
	// config
	servicesNotifierRetryTm int
}

func newMetaBridgeClient(
	cluster string, config common.Config) (c *metadataClient, err error) {

	b := &metadataClient{
		cluster:    cluster,
		finch:      make(chan bool),
		adminports: make(map[string]common.IndexerId),
		loads:      make(map[common.IndexDefnId]*loadHeuristics),
	}
	b.topology = make(map[common.IndexerId]map[common.IndexDefnId]*mclient.IndexMetadata)
	b.servicesNotifierRetryTm = config["servicesNotifierRetryTm"].Int()
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
		b.mdClient.Close()
		return nil, err
	}

	b.Refresh()
	go b.watchClusterChanges() // will also update the indexer list
	return b, nil
}

// Sync will update the indexer list.
func (b *metadataClient) Sync() error {
	return b.updateIndexerList(true)
}

// Refresh implement BridgeAccessor{} interface.
func (b *metadataClient) Refresh() ([]*mclient.IndexMetadata, error) {
	mindexes := b.mdClient.ListIndex()
	indexes := make([]*mclient.IndexMetadata, 0, len(mindexes))
	for _, mindex := range mindexes {
		indexes = append(indexes, mindex)
	}

	b.rw.Lock()
	defer b.rw.Unlock()

	// b.adminports is the source of truth for list of indexers.
	newtopo :=
		make(map[common.IndexerId]map[common.IndexDefnId]*mclient.IndexMetadata)
	for _, indexerID := range b.adminports {
		newtopo[indexerID] = make(map[common.IndexDefnId]*mclient.IndexMetadata)
	}
	// gather topology of each index.
	for _, index := range mindexes {
		for _, instance := range index.Instances {
			id := instance.IndexerId
			if _, ok := newtopo[id]; ok {
				newtopo[id][index.Definition.DefnId] = index
			}
		}
	}

	b.topology = newtopo
	// compute replicas
	b.replicas = b.computeReplicas()
	// remove loads for indexes that is been deleted / gone-offline.
	for defnId := range b.loads {
		if _, ok := b.replicas[defnId]; !ok {
			delete(b.loads, defnId)
		}
	}
	return indexes, nil
}

// Nodes implement BridgeAccessor{} interface.
func (b *metadataClient) Nodes() ([]*IndexerService, error) {
	b.rw.RLock()
	defer b.rw.RUnlock()

	// gather Indexer services
	nodes := make(map[string]*IndexerService)
	for indexerID := range b.topology {
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
	b.rw.RLock()
	defer b.rw.RUnlock()

	for _, indexes := range b.topology {
		for id, index := range indexes {
			if defnID == uint64(id) {
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
		b.updateIndexerList(false)
		refreshCnt++
		goto RETRY
	}

	b.Refresh() // refresh so that we too have IndexMetadata table.
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
		b.deleteIndex(defnID)
	}
	return err
}

// GetScanports implements BridgeAccessor{} interface.
func (b *metadataClient) GetScanports() (queryports []string) {
	b.rw.RLock()
	defer b.rw.RUnlock()

	queryports = make([]string, 0)
	for indexerID := range b.topology {
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
	defnID uint64,
	retry int) (queryport string, targetDefnID uint64, ok bool) {

	b.rw.RLock()
	defer b.rw.RUnlock()

	if retry == 0 {
		targetDefnID = b.pickOptimal(defnID) // index under least load
	} else {
		targetDefnID = b.roundRobin(defnID, retry)
	}
	_, queryport, err :=
		b.mdClient.FindServiceForIndex(common.IndexDefnId(targetDefnID))
	if err != nil {
		return "", 0, false
	}
	fmsg := "Scan port %s for index defnID %d of equivalent index defnId %d"
	logging.Debugf(fmsg, queryport, targetDefnID, defnID)
	return queryport, targetDefnID, true
}

// Timeit implement BridgeAccessor{} interface.
func (b *metadataClient) Timeit(defnID uint64, value float64) {
	b.rw.Lock()
	defer b.rw.Unlock()

	id := common.IndexDefnId(defnID)
	if load, ok := b.loads[id]; !ok {
		b.loads[id] = &loadHeuristics{avgLoad: value, count: 1}
	} else {
		// compute incremental average.
		avg, n := load.avgLoad, load.count
		load.avgLoad = (float64(n)*avg + float64(value)) / float64(n+1)
		load.count = n + 1
	}
}

// IndexState implement BridgeAccessor{} interface.
func (b *metadataClient) IndexState(defnID uint64) (common.IndexState, error) {
	b.Refresh()

	b.rw.RLock()
	defer b.rw.RUnlock()

	for _, indexes := range b.topology {
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
func (b *metadataClient) computeReplicas() map[common.IndexDefnId][]common.IndexDefnId {

	replicaMap := make(map[common.IndexDefnId][]common.IndexDefnId)
	for id1, indexes1 := range b.topology {
		for _, index1 := range indexes1 {
			replicas := make([]common.IndexDefnId, 0)
			replicas = append(replicas, index1.Definition.DefnId) // add itself
			for id2, indexes2 := range b.topology {
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
		d1.PartitionKey != d2.PartitionKey {

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
	count   uint64
}

// pick an optimal replica for the index `defnID` under least load.
func (b *metadataClient) pickOptimal(defnID uint64) uint64 {
	id := common.IndexDefnId(defnID)
	optimalID, currLoad := id, 0.0
	if load, ok := b.loads[id]; ok {
		currLoad = load.avgLoad
	}
	for _, replicaID := range b.replicas[id] {
		load, ok := b.loads[replicaID]
		if !ok { // no load for this replica
			return uint64(replicaID)
		}
		if currLoad == 0.0 || load.avgLoad < currLoad {
			// found an index under less load
			optimalID, currLoad = replicaID, load.avgLoad
		}
	}
	return uint64(optimalID)
}

func (b *metadataClient) roundRobin(defnID uint64, retry int) uint64 {
	id := common.IndexDefnId(defnID)
	replicas, ok := b.replicas[id]
	if l := len(replicas); ok && l > 0 {
		return uint64(replicas[retry%l])
	}
	return defnID
}

//----------------
// local functions
//----------------

func (b *metadataClient) deleteIndex(defnID uint64) {
	b.rw.Lock()
	defer b.rw.Unlock()

	id := common.IndexDefnId(defnID)
	for indexerID, indexes := range b.topology {
		delete(indexes, id)
		b.topology[indexerID] = indexes
	}
	b.replicas = b.computeReplicas()
	delete(b.loads, common.IndexDefnId(defnID))
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
// IMPORTANT: make sure to call Refresh() after calling updateIndexerList()
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
	// populate indexers' adminport and queryport
	adminports, err := getIndexerAdminports(cinfo)
	if err != nil {
		return err
	}

	fmsg := "Refreshing indexer list due to cluster changes or auto-refresh."
	logging.Infof(fmsg)
	logging.Infof("Refreshed Indexer List: %v", adminports)

	b.rw.Lock()
	defer b.rw.Unlock()

	if discardExisting {
		for _, indexerID := range b.adminports {
			b.mdClient.UnwatchMetadata(indexerID)
		}
		b.adminports = nil
	}

	// watch all indexers
	m := make(map[string]common.IndexerId)
	for _, adminport := range adminports { // add new indexer-nodes if any
		if indexerID, ok := b.adminports[adminport]; !ok {
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
				b.updateIndexer(ad, n_id, o_id)
			}

			// WatchMetadata will "unwatch" an old metadata watcher which
			// shares the same indexer Id (but the adminport may be different).
			indexerID = b.mdClient.WatchMetadata(adminport, fn)
			m[adminport] = indexerID
		} else {
			err = b.mdClient.UpdateServiceAddrForIndexer(indexerID, adminport)
			m[adminport] = indexerID
			delete(b.adminports, adminport)
		}
	}
	// delete indexer-nodes that got removed from cluster.
	for _, indexerID := range b.adminports {
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
	b.adminports = m
	return err
}

func (b *metadataClient) updateIndexer(
	adminport string, newIndexerId, oldIndexerId common.IndexerId) {

	func() {
		b.rw.Lock()
		defer b.rw.Unlock()
		logging.Infof(
			"Acknowledged that new indexer is registered.  Indexer = %v, id = %v",
			adminport, newIndexerId)
		b.adminports[adminport] = newIndexerId
	}()
	b.Refresh()
}

// return adminports for all known indexers.
func getIndexerAdminports(cinfo *common.ClusterInfoCache) ([]string, error) {
	iAdminports := make([]string, 0)
	for _, node := range cinfo.GetNodesByServiceType("indexAdmin") {
		status, err := cinfo.GetNodeStatus(node)
		common.CrashOnError(err)
		logging.Warnf("node %v status: %q", node, status)
		if status == "healthy" || status == "active" || status == "warmup" {
			adminport, err := cinfo.GetServiceAddress(node, "indexAdmin")
			if err != nil {
				return nil, err
			}
			iAdminports = append(iAdminports, adminport)
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
		b.Refresh()
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
