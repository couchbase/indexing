package client

import "sync"
import "fmt"
import "errors"
import "encoding/json"

import "github.com/couchbase/indexing/secondary/common"
import mclient "github.com/couchbase/indexing/secondary/manager/client"

type metadataClient struct {
	clusterURL string
	mdClient   *mclient.MetadataProvider
	rw         sync.RWMutex // protects all fields listed below
	// sherlock topology management, multi-node & single-partition.
	adminports map[string]common.IndexerId                   // book-keeping for cluster changes
	topology   map[common.IndexerId][]*mclient.IndexMetadata // indexerId -> indexes
	// shelock load replicas.
	replicas map[common.IndexDefnId][]common.IndexDefnId
	// shelock load balancing.
	loads map[common.IndexDefnId]*loadHeuristics // index -> loadHeuristics
}

func newMetaBridgeClient(cluster string) (c *metadataClient, err error) {
	cinfo, err :=
		common.NewClusterInfoCache(common.ClusterUrl(cluster), "default")
	if err != nil {
		return nil, err
	}
	b := &metadataClient{
		clusterURL: cluster,
		adminports: make(map[string]common.IndexerId),
		loads:      make(map[common.IndexDefnId]*loadHeuristics),
		topology:   make(map[common.IndexerId][]*mclient.IndexMetadata),
	}
	// initialize meta-data-provide.
	uuid, err := common.NewUUID()
	if err != nil {
		common.Errorf("Could not generate UUID in common.NewUUID")
		return nil, err
	}
	b.mdClient, err = mclient.NewMetadataProvider(uuid.Str())
	if err != nil {
		return nil, err
	}
	if err := b.updateIndexerList(cinfo); err != nil {
		b.mdClient.Close()
		return nil, err
	}

	b.Refresh()
	go b.watchClusterChanges(cluster)
	return b, nil
}

// Refresh implement BridgeAccessor{} interface.
func (b *metadataClient) Refresh() ([]*mclient.IndexMetadata, error) {
	b.rw.Lock()
	defer b.rw.Unlock()

	mindexes := b.mdClient.ListIndex()
	indexes := make([]*mclient.IndexMetadata, 0, len(mindexes))
	for _, mindex := range mindexes {
		indexes = append(indexes, mindex)
	}

	// gather topology of each index.
	for _, index := range indexes {
		for _, instance := range index.Instances {
			id := instance.IndexerId
			if _, ok := b.topology[id]; !ok {
				// there could be a race between Refresh() and
				// watchClusterChanges() so create an entry in
				// topology.
				b.topology[id] = make([]*mclient.IndexMetadata, 0)
			}
			b.topology[id] = append(b.topology[id], index)
		}
	}
	// compute replicas
	b.replicas = b.computeReplicas()
	return indexes, nil
}

// Nodes implement BridgeAccessor{} interface.
func (b *metadataClient) Nodes() (map[string]string, error) {
	b.rw.RLock()
	defer b.rw.RUnlock()

	nodes := make(map[string]string)
	for indexerID := range b.topology {
		if indexerID != common.INDEXER_ID_NIL {
			a, q, err := b.mdClient.FindServiceForIndexer(indexerID)
			if err == nil {
				nodes[a] = q
			}
		}
	}
	return nodes, nil
}

// CreateIndex implements BridgeAccessor{} interface.
func (b *metadataClient) CreateIndex(
	indexName, bucket, using, exprType, partnExpr, whereExpr string,
	secExprs []string, isPrimary bool,
	planJSON []byte) (common.IndexDefnId, error) {

	plan := make(map[string]interface{})
	if planJSON != nil && len(planJSON) > 0 {
		err := json.Unmarshal(planJSON, &plan)
		if err != nil {
			return common.IndexDefnId(0), err
		}
	}

	defnID, err := b.mdClient.CreateIndexWithPlan(
		indexName, bucket, using, exprType, partnExpr, whereExpr,
		secExprs, isPrimary, plan)
	b.Refresh() // refresh so that we too have IndexMetadata table.
	return defnID, err
}

// BuildIndexes implements BridgeAccessor{} interface.
func (b *metadataClient) BuildIndexes(defnIDs []common.IndexDefnId) error {
	_, ok := b.getNodes(defnIDs)
	if !ok {
		return ErrorIndexNotFound
	}
	return b.mdClient.BuildIndexes(defnIDs)
}

// DropIndex implements BridgeAccessor{} interface.
func (b *metadataClient) DropIndex(defnID common.IndexDefnId) error {
	return b.mdClient.DropIndex(defnID)
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
	common.Debugf("Scan ports %v for all indexes", queryports)
	return queryports
}

// GetScanport implements BridgeAccessor{} interface.
func (b *metadataClient) GetScanport(
	defnID common.IndexDefnId) (queryport string, ok bool) {

	b.rw.RLock()
	defer b.rw.RUnlock()

	defnID = b.pickOptimal(defnID) // defnID (aka index) under least load
	_, queryport, err := b.mdClient.FindServiceForIndex(defnID)
	if err != nil {
		return "", false
	}
	common.Debugf("Scan port %s for index %d", queryport, defnID)
	return queryport, true
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
	b.mdClient.Close()
}

//--------------------------------
// local functions to map replicas
//--------------------------------

// compute a map of replicas for each index in 2i.
func (b *metadataClient) computeReplicas() map[common.IndexDefnId][]common.IndexDefnId {
	replicaMap := make(map[common.IndexDefnId][]common.IndexDefnId, 0)
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

//--------------------------------
// local functions to map replicas
//--------------------------------

// manage load statistics.
type loadHeuristics struct {
	avgLoad float64
	count   uint64
}

// pick an optimal replica for the index `defnID` under least load.
func (b *metadataClient) pickOptimal(
	defnID common.IndexDefnId) common.IndexDefnId {

	optimalID, currLoad := defnID, 0.0
	if load, ok := b.loads[defnID]; ok {
		currLoad = load.avgLoad
	}
	for _, replicaID := range b.replicas[defnID] {
		load, ok := b.loads[replicaID]
		if !ok { // no load for this replica
			return replicaID
		}
		if currLoad == 0.0 || load.avgLoad < currLoad {
			// found an index under less load
			optimalID, currLoad = replicaID, load.avgLoad
		}
	}
	return optimalID
}

//----------------
// local functions
//----------------

// getNodes return the set of nodes hosting the specified set
// of indexes
func (b *metadataClient) getNodes(
	defnIDs []common.IndexDefnId) (adminport []string, ok bool) {

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
func (b *metadataClient) getNode(
	defnID common.IndexDefnId) (adminport string, ok bool) {

	adminport, _, err := b.mdClient.FindServiceForIndex(defnID)
	if err != nil {
		return "", false
	}
	return adminport, true
}

// update 2i cluster information
func (b *metadataClient) updateIndexerList(cinfo *common.ClusterInfoCache) error {
	if err := cinfo.Fetch(); err != nil {
		return err
	}
	// populate indexers' adminport and queryport
	adminports, err := getIndexerAdminports(cinfo)
	if err != nil {
		return err
	}

	b.rw.Lock()
	defer b.rw.Unlock()
	// watch all indexers
	m := make(map[string]common.IndexerId)
	for _, adminport := range adminports { // add new indexer-nodes if any
		if indexerID, ok := b.adminports[adminport]; !ok {
			indexerID, err = b.mdClient.WatchMetadata(adminport)
			m[adminport] = indexerID
			b.topology[indexerID] = make([]*mclient.IndexMetadata, 0)
		} else {
			m[adminport] = indexerID
			delete(b.adminports, adminport)
		}
	}
	// delete indexer-nodes that got removed from cluster.
	for _, indexerID := range b.adminports {
		b.mdClient.UnwatchMetadata(indexerID)
	}
	b.adminports = m
	return nil
}

// return adminports for all known indexers.
func getIndexerAdminports(
	cinfo *common.ClusterInfoCache) ([]string, error) {

	iAdminports := make([]string, 0)
	for _, node := range cinfo.GetNodesByServiceType("indexAdmin") {
		adminport, err := cinfo.GetServiceAddress(node, "indexAdmin")
		if err != nil {
			return nil, err
		}
		iAdminports = append(iAdminports, adminport)
	}
	return iAdminports, nil
}

func (b *metadataClient) watchClusterChanges(cluster string) {
	clusterURL := common.ClusterUrl(cluster)
	cinfo, err := common.NewClusterInfoCache(clusterURL, "default")
	if err != nil {
		err := fmt.Errorf("error NewClusterInfoCache(): %v", err)
		panic(err)
	}

	for {
		if err := cinfo.WaitAndUpdateServices(); err != nil {
			err := fmt.Errorf("error while waiting for cluster change: %v", err)
			panic(err)
		}
		if err = b.updateIndexerList(cinfo); err != nil {
			err := fmt.Errorf("error while updating indexer nodes: %v", err)
			panic(err)
		}
	}
}
